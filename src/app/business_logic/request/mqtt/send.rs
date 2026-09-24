use std::sync::Arc;
use std::time::{Duration, Instant};
use chrono::Local;
use parking_lot::{Mutex, RwLock};
use reqwest::Url;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, trace};
use crate::app::app::App;
use crate::app::business_logic::request::mqtt::client::{MqttClient, MqttEvent, MqttEventLoop, PreparedMqttRequest};
use crate::app::business_logic::request::send::{PrepareRequestError, RequestResponseError};
use crate::models::auth::auth::Auth;
use crate::models::auth::basic::BasicAuth;
use crate::models::environment::Environment;
use crate::models::protocol::mqtt::mqtt::{MqttCommand, MqttMessage, MqttMessageContent, MqttRequest, MqttVersion, QoS};
use crate::models::protocol::mqtt::payload::MqttPayload;
use crate::models::protocol::ws::ws::Sender;
use crate::models::request::Request;
use crate::models::response::{RequestResponse, ResponseContent};

impl App<'_> {
    pub fn prepare_mqtt_request(&self, request: &mut Request) -> Result<PreparedMqttRequest, PrepareRequestError> {
        trace!("Preparing MQTT request");

        let env = self.get_selected_env_as_local();

        /* PRE-REQUEST SCRIPT */

        let modified_request = self.handle_pre_request_script(request, env)?;
        let mqtt_request = modified_request.get_mqtt_request().unwrap();

        /* URL */

        let url = self.replace_env_keys_by_value(&modified_request.url);

        let url = match Url::parse(&url) {
            Ok(url) => url,
            Err(_) => return Err(PrepareRequestError::InvalidUrl)
        };

        let (use_tls, default_port) = match url.scheme() {
            "mqtt" | "tcp" => (false, 1883),
            "mqtts" | "ssl" => (true, 8883),
            _ => return Err(PrepareRequestError::InvalidMqttUrlScheme)
        };

        let host = match url.host_str() {
            Some(host) => host.to_string(),
            None => return Err(PrepareRequestError::InvalidUrl)
        };

        /* AUTH */

        let credentials = match &modified_request.auth {
            Auth::NoAuth => None,
            Auth::BasicAuth(BasicAuth { username, password }) => Some((
                self.replace_env_keys_by_value(username),
                self.replace_env_keys_by_value(password)
            )),
            Auth::BearerToken(_) | Auth::JwtToken(_) | Auth::Digest(_) => return Err(PrepareRequestError::UnsupportedMqttAuth)
        };

        /* CLIENT ID */

        let client_id = self.replace_env_keys_by_value(&mqtt_request.client_id);

        // Checked here because rumqttc panics on it, MQTT 5 allows it and lets the broker assign an ID
        if mqtt_request.version == MqttVersion::V3_1_1 && client_id.is_empty() && !mqtt_request.clean_session {
            return Err(PrepareRequestError::MqttClientIdRequired);
        }

        /* KEEP ALIVE */

        // Same, rumqttc panics on MQTT 5 keep alives under 5 seconds
        if mqtt_request.version == MqttVersion::V5 && mqtt_request.keep_alive < 5 {
            return Err(PrepareRequestError::MqttKeepAliveTooShort);
        }

        /* SUBSCRIPTIONS */

        let subscriptions = mqtt_request.subscriptions
            .iter()
            .filter(|subscription| subscription.enabled)
            .map(|subscription| (self.replace_env_keys_by_value(&subscription.topic), subscription.qos))
            .collect();

        Ok(PreparedMqttRequest {
            version: mqtt_request.version,
            host,
            port: url.port().unwrap_or(default_port),
            use_tls,
            client_id,
            clean_session: mqtt_request.clean_session,
            session_expiry_interval: mqtt_request.session_expiry_interval,
            keep_alive: mqtt_request.keep_alive,
            max_packet_size: mqtt_request.max_packet_size,
            credentials,
            subscriptions,
            connection_timeout: (modified_request.settings.timeout.as_u32() as u64).div_ceil(1000).max(1),
            accept_invalid_certs: modified_request.settings.accept_invalid_certs.as_bool(),
            accept_invalid_hostnames: modified_request.settings.accept_invalid_hostnames.as_bool(),
        })
    }
}

pub async fn send_mqtt_request(prepared_request: PreparedMqttRequest, local_request: Arc<RwLock<Request>>, env: &Option<Arc<RwLock<Environment>>>, received_response: Arc<Mutex<bool>>) -> Result<RequestResponse, RequestResponseError> {
    info!("Connecting to MQTT broker");

    let mut request = local_request.write();
    request.is_pending = true;

    let cancellation_token = request.cancellation_token.clone();
    let timeout = Duration::from_millis(request.settings.timeout.as_u32() as u64);

    let mqtt_request = request.get_mqtt_request_mut().unwrap();
    mqtt_request.is_connected = false;

    drop(request);

    let request_start = Instant::now();
    let mut connection: Option<(MqttClient, MqttEventLoop)> = None;

    let mut response = match MqttClient::new(&prepared_request) {
        Err(error) => error_response(error),
        Ok((new_client, mut new_event_loop)) => {
            let connack = tokio::select! {
                _ = cancellation_token.cancelled() => Err(String::from("CANCELED")),
                _ = tokio::time::sleep(timeout) => Err(String::from("TIMEOUT")),
                connack = wait_for_connack(&mut new_event_loop) => Ok(connack),
            };

            match connack {
                Err(status_code) => RequestResponse {
                    duration: None,
                    status_code: Some(status_code),
                    content: None,
                    cookies: None,
                    headers: vec![],
                },
                Ok(Err(error)) => error_response(error),
                Ok(Ok((code, session_present, properties))) => {
                    info!("Connected to MQTT broker");

                    let mut headers = vec![(String::from("session present"), session_present.to_string())];
                    headers.extend(properties);

                    connection = Some((new_client, new_event_loop));

                    RequestResponse {
                        duration: None,
                        status_code: Some(format!("CONNACK {code}")),
                        content: None,
                        cookies: None,
                        headers,
                    }
                }
            }
        }
    };

    response.duration = Some(format!("{:?}", request_start.elapsed()));

    trace!("MQTT connection attempt done");

    /* POST-REQUEST SCRIPT */

    let request = local_request.read();
    let (modified_response, post_request_output) = App::handle_post_request_script(&request, response, env)?;
    drop(request);

    let mut request = local_request.write();

    request.console_output.post_request_output = post_request_output;
    request.is_pending = false;
    request.cancellation_token = CancellationToken::new();

    let mqtt_request = request.get_mqtt_request_mut().unwrap();
    mqtt_request.messages = vec![];

    let Some((client, event_loop)) = connection else {
        // The messages tab is where MQTT requests are read, so the reason goes there too
        if let Some(ResponseContent::Body(reason)) = &modified_response.content {
            mqtt_request.messages.push(MqttMessage {
                timestamp: Local::now(),
                sender: Sender::Server,
                content: MqttMessageContent::Event(reason.clone()),
            });
        }

        return Ok(modified_response);
    };

    let (commands_sender, commands_receiver) = unbounded_channel();
    mqtt_request.connection = Some(commands_sender);
    mqtt_request.is_connected = true;

    drop(request);

    for (topic, qos) in prepared_request.subscriptions {
        if let Err(error) = client.subscribe(topic.clone(), qos).await {
            push_event(&local_request, format!("Could not subscribe to \"{topic}\": {error}"));
        }
    }

    tokio::spawn(connection_loop(client, event_loop, commands_receiver, local_request, received_response));

    Ok(modified_response)
}

/// Queue a message to be published on the request's broker connection, and add it to the messages
pub fn mqtt_publish(mqtt_request: &mut MqttRequest, topic: String, payload: MqttPayload, qos: QoS, retain: bool) {
    let Some(connection) = &mqtt_request.connection else {
        info!("MQTT client is not connected");
        return;
    };

    let payload_bytes = payload.to_bytes();

    // Sending a packet over the limit would close the connection, the fixed and variable headers are at most 9 bytes
    let packet_size = payload_bytes.len() + topic.len() + 9;
    if packet_size > mqtt_request.max_packet_size as usize {
        mqtt_request.messages.push(MqttMessage {
            timestamp: Local::now(),
            sender: Sender::You,
            content: MqttMessageContent::Event(format!(
                "Not published to \"{topic}\": the message is about {packet_size} bytes, over the {} bytes max packet size",
                mqtt_request.max_packet_size
            )),
        });
        return;
    }

    let command = MqttCommand::Publish {
        topic: topic.clone(),
        payload: payload_bytes,
        qos,
        retain,
    };

    if connection.send(command).is_err() {
        return;
    }

    mqtt_request.messages.push(MqttMessage {
        timestamp: Local::now(),
        sender: Sender::You,
        content: MqttMessageContent::Publish {
            topic,
            payload,
            qos,
            retain,
        },
    });
}

pub fn mqtt_disconnect(mqtt_request: &mut MqttRequest) {
    if let Some(connection) = &mqtt_request.connection {
        connection.send(MqttCommand::Disconnect).ok();
    }
}

async fn wait_for_connack(event_loop: &mut MqttEventLoop) -> Result<(String, bool, Vec<(String, String)>), String> {
    loop {
        if let MqttEvent::ConnAck { code, session_present, properties } = event_loop.poll().await? {
            return Ok((code, session_present, properties));
        }
    }
}

async fn connection_loop(client: MqttClient, mut event_loop: MqttEventLoop, mut commands_receiver: UnboundedReceiver<MqttCommand>, local_request: Arc<RwLock<Request>>, received_response: Arc<Mutex<bool>>) {
    let mut is_disconnecting = false;

    let reason = loop {
        tokio::select! {
            command = commands_receiver.recv(), if !is_disconnecting => match command {
                Some(MqttCommand::Publish { topic, payload, qos, retain }) => {
                    if let Err(error) = client.publish(topic.clone(), payload, qos, retain).await {
                        push_event(&local_request, format!("Could not publish to \"{topic}\": {error}"));
                    }
                },
                Some(MqttCommand::Disconnect) | None => {
                    is_disconnecting = true;

                    if let Err(error) = client.disconnect().await {
                        break format!("Disconnected: {error}");
                    }
                }
            },
            event = event_loop.poll() => match event {
                Ok(MqttEvent::Publish { topic, payload, qos, retain }) => {
                    let mut request = local_request.write();
                    let mqtt_request = request.get_mqtt_request_mut().unwrap();

                    mqtt_request.messages.push(MqttMessage {
                        timestamp: Local::now(),
                        sender: Sender::Server,
                        content: MqttMessageContent::Publish {
                            topic,
                            payload: MqttPayload::from_bytes(&payload),
                            qos,
                            retain,
                        },
                    });
                },
                Ok(MqttEvent::SubAck(return_codes)) => push_event(&local_request, format!("Subscription result: {return_codes}")),
                Ok(MqttEvent::Disconnect(reason)) => break reason,
                Ok(MqttEvent::Disconnected) => break String::from("Disconnected"),
                Ok(MqttEvent::ConnAck { .. } | MqttEvent::Other) => continue,
                Err(error) => {
                    error!("MQTT connection error: {}", error);
                    break format!("Connection closed: {error}");
                }
            }
        }

        *received_response.lock() = true;
    };

    push_event(&local_request, reason);

    let mut request = local_request.write();
    let mqtt_request = request.get_mqtt_request_mut().unwrap();
    mqtt_request.connection = None;
    mqtt_request.is_connected = false;

    *received_response.lock() = true;
}

fn push_event(local_request: &Arc<RwLock<Request>>, event: String) {
    let mut request = local_request.write();
    let mqtt_request = request.get_mqtt_request_mut().unwrap();

    mqtt_request.messages.push(MqttMessage {
        timestamp: Local::now(),
        sender: Sender::Server,
        content: MqttMessageContent::Event(event),
    });
}

fn error_response(error: String) -> RequestResponse {
    error!("MQTT connection error: {}", error);

    RequestResponse {
        duration: None,
        status_code: Some(error.clone()),
        content: Some(ResponseContent::Body(error)),
        cookies: None,
        headers: vec![],
    }
}
