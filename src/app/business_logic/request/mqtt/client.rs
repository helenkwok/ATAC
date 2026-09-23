//! Wraps both rumqttc versions behind one interface, so the rest of ATAC doesn't depend on the MQTT library

use crate::models::protocol::mqtt::mqtt::{MqttVersion, QoS};

pub struct PreparedMqttRequest {
    pub version: MqttVersion,
    pub host: String,
    pub port: u16,
    pub use_tls: bool,
    pub client_id: String,
    pub clean_session: bool,
    pub keep_alive: u16,
    pub max_packet_size: u32,
    pub credentials: Option<(String, String)>,
    pub subscriptions: Vec<(String, QoS)>,
}

pub enum MqttClient {
    V4(rumqttc_v4::AsyncClient, rumqttc_v4::EventLoop),
    V5(rumqttc_v5::AsyncClient, rumqttc_v5::EventLoop),
}

pub enum MqttEvent {
    ConnAck {
        code: String,
        session_present: bool,
        properties: Vec<(String, String)>,
    },
    Publish {
        topic: String,
        payload: Vec<u8>,
        qos: QoS,
        retain: bool,
    },
    SubAck(String),
    Disconnect(String),
    /// Outgoing disconnect has been written, the connection can be dropped
    Disconnected,
    Other,
}

pub enum MqttError {
    /// The broker answered a clean session connect with session_present=1, which MQTT forbids
    SessionStateMismatch,
    Other(String),
}

impl MqttClient {
    pub fn new(prepared: &PreparedMqttRequest) -> Result<MqttClient, String> {
        match prepared.version {
            MqttVersion::V3_1_1 => {
                use rumqttc_v4::{AsyncClient, MqttOptions, Transport};

                let mut builder = MqttOptions::builder(prepared.client_id.clone(), (prepared.host.clone(), prepared.port))
                    .keep_alive(prepared.keep_alive)
                    .max_packet_size(prepared.max_packet_size as usize, prepared.max_packet_size as usize)
                    .clean_session(prepared.clean_session);

                if prepared.use_tls {
                    builder = builder.transport(Transport::try_tls_with_default_config().map_err(|error| error.to_string())?);
                }

                if let Some((username, password)) = &prepared.credentials {
                    builder = builder.credentials(username.clone(), password.clone().into_bytes());
                }

                let options = builder.try_build().map_err(|error| error.to_string())?;
                let (client, event_loop) = AsyncClient::builder(options).build();

                Ok(MqttClient::V4(client, event_loop))
            }
            MqttVersion::V5 => {
                use rumqttc_v5::{AsyncClient, MqttOptions, Transport, IncomingPacketSizeLimit};

                let mut builder = MqttOptions::builder(prepared.client_id.clone(), (prepared.host.clone(), prepared.port))
                    .keep_alive(prepared.keep_alive)
                    .max_packet_size(Some(prepared.max_packet_size))
                    .incoming_packet_size_limit(IncomingPacketSizeLimit::Bytes(prepared.max_packet_size))
                    .clean_start(prepared.clean_session);

                if prepared.use_tls {
                    builder = builder.transport(Transport::try_tls_with_default_config().map_err(|error| error.to_string())?);
                }

                if let Some((username, password)) = &prepared.credentials {
                    builder = builder.credentials(username.clone(), password.clone().into_bytes());
                }

                let options = builder.try_build().map_err(|error| error.to_string())?;
                let (client, event_loop) = AsyncClient::builder(options).build();

                Ok(MqttClient::V5(client, event_loop))
            }
        }
    }

    pub async fn poll(&mut self) -> Result<MqttEvent, MqttError> {
        match self {
            MqttClient::V4(_, event_loop) => {
                use rumqttc_v4::{ConnectionError, Event, Outgoing, Packet};

                match event_loop.poll().await {
                    Ok(Event::Incoming(Packet::ConnAck(connack))) => Ok(MqttEvent::ConnAck {
                        code: format!("{:?}", connack.code),
                        session_present: connack.session_present,
                        properties: vec![],
                    }),
                    Ok(Event::Incoming(Packet::Publish(publish))) => Ok(MqttEvent::Publish {
                        topic: String::from_utf8_lossy(&publish.topic).to_string(),
                        payload: publish.payload.to_vec(),
                        qos: from_v4_qos(publish.qos),
                        retain: publish.retain,
                    }),
                    Ok(Event::Incoming(Packet::SubAck(suback))) => Ok(MqttEvent::SubAck(format!("{:?}", suback.return_codes))),
                    Ok(Event::Incoming(Packet::Disconnect)) => Ok(MqttEvent::Disconnect(String::from("Disconnected by the broker"))),
                    Ok(Event::Outgoing(Outgoing::Disconnect)) => Ok(MqttEvent::Disconnected),
                    Ok(_) => Ok(MqttEvent::Other),
                    Err(ConnectionError::SessionStateMismatch { .. }) => Err(MqttError::SessionStateMismatch),
                    Err(error) => Err(MqttError::Other(error.to_string())),
                }
            }
            MqttClient::V5(_, event_loop) => {
                use rumqttc_v5::{ConnectionError, Event, Outgoing};
                use rumqttc_v5::mqttbytes::v5::Packet;

                match event_loop.poll().await {
                    Ok(Event::Incoming(Packet::ConnAck(connack))) => Ok(MqttEvent::ConnAck {
                        code: format!("{:?}", connack.code),
                        session_present: connack.session_present,
                        properties: match &connack.properties {
                            None => vec![],
                            Some(properties) => connack_properties_to_vec(properties),
                        },
                    }),
                    Ok(Event::Incoming(Packet::Publish(publish))) => Ok(MqttEvent::Publish {
                        topic: String::from_utf8_lossy(&publish.topic).to_string(),
                        payload: publish.payload.to_vec(),
                        qos: from_v5_qos(publish.qos),
                        retain: publish.retain,
                    }),
                    Ok(Event::Incoming(Packet::SubAck(suback))) => Ok(MqttEvent::SubAck(format!("{:?}", suback.return_codes))),
                    Ok(Event::Incoming(Packet::Disconnect(disconnect))) => Ok(MqttEvent::Disconnect(format!("Disconnected by the broker: {:?}", disconnect.reason_code))),
                    Ok(Event::Outgoing(Outgoing::Disconnect)) => Ok(MqttEvent::Disconnected),
                    Ok(_) => Ok(MqttEvent::Other),
                    Err(ConnectionError::SessionStateMismatch { .. }) => Err(MqttError::SessionStateMismatch),
                    Err(error) => Err(MqttError::Other(error.to_string())),
                }
            }
        }
    }

    pub async fn subscribe(&self, topic: String, qos: QoS) -> Result<(), String> {
        match self {
            MqttClient::V4(client, _) => client.subscribe(topic, to_v4_qos(qos)).await.map_err(|error| error.to_string()),
            MqttClient::V5(client, _) => client.subscribe(topic, to_v5_qos(qos)).await.map_err(|error| error.to_string()),
        }
    }

    pub async fn publish(&self, topic: String, payload: Vec<u8>, qos: QoS, retain: bool) -> Result<(), String> {
        match self {
            MqttClient::V4(client, _) => {
                let options = rumqttc_v4::PublishOptions::new(to_v4_qos(qos)).retain(retain);
                client.publish(topic, payload, options).await.map_err(|error| error.to_string())
            },
            MqttClient::V5(client, _) => {
                let options = rumqttc_v5::PublishOptions::new(to_v5_qos(qos)).retain(retain);
                client.publish(topic, payload, options).await.map_err(|error| error.to_string())
            },
        }
    }

    pub async fn disconnect(&self) -> Result<(), String> {
        match self {
            MqttClient::V4(client, _) => client.disconnect().await.map_err(|error| error.to_string()),
            MqttClient::V5(client, _) => client.disconnect().await.map_err(|error| error.to_string()),
        }
    }
}

fn to_v4_qos(qos: QoS) -> rumqttc_v4::QoS {
    match qos {
        QoS::AtMostOnce => rumqttc_v4::QoS::AtMostOnce,
        QoS::AtLeastOnce => rumqttc_v4::QoS::AtLeastOnce,
        QoS::ExactlyOnce => rumqttc_v4::QoS::ExactlyOnce,
    }
}

fn from_v4_qos(qos: rumqttc_v4::QoS) -> QoS {
    match qos {
        rumqttc_v4::QoS::AtMostOnce => QoS::AtMostOnce,
        rumqttc_v4::QoS::AtLeastOnce => QoS::AtLeastOnce,
        rumqttc_v4::QoS::ExactlyOnce => QoS::ExactlyOnce,
    }
}

fn to_v5_qos(qos: QoS) -> rumqttc_v5::mqttbytes::QoS {
    match qos {
        QoS::AtMostOnce => rumqttc_v5::mqttbytes::QoS::AtMostOnce,
        QoS::AtLeastOnce => rumqttc_v5::mqttbytes::QoS::AtLeastOnce,
        QoS::ExactlyOnce => rumqttc_v5::mqttbytes::QoS::ExactlyOnce,
    }
}

fn from_v5_qos(qos: rumqttc_v5::mqttbytes::QoS) -> QoS {
    match qos {
        rumqttc_v5::mqttbytes::QoS::AtMostOnce => QoS::AtMostOnce,
        rumqttc_v5::mqttbytes::QoS::AtLeastOnce => QoS::AtLeastOnce,
        rumqttc_v5::mqttbytes::QoS::ExactlyOnce => QoS::ExactlyOnce,
    }
}

/// Only keeps the properties the broker actually sent
fn connack_properties_to_vec(properties: &rumqttc_v5::mqttbytes::v5::ConnAckProperties) -> Vec<(String, String)> {
    let optional_properties = [
        ("session expiry interval", properties.session_expiry_interval.map(|value| value.to_string())),
        ("receive maximum", properties.receive_max.map(|value| value.to_string())),
        ("maximum QoS", properties.max_qos.map(|value| value.to_string())),
        ("retain available", properties.retain_available.map(|value| value.to_string())),
        ("maximum packet size", properties.max_packet_size.map(|value| value.to_string())),
        ("assigned client identifier", properties.assigned_client_identifier.clone()),
        ("topic alias maximum", properties.topic_alias_max.map(|value| value.to_string())),
        ("reason string", properties.reason_string.clone()),
        ("wildcard subscription available", properties.wildcard_subscription_available.map(|value| value.to_string())),
        ("subscription identifiers available", properties.subscription_identifiers_available.map(|value| value.to_string())),
        ("shared subscription available", properties.shared_subscription_available.map(|value| value.to_string())),
        ("server keep alive", properties.server_keep_alive.map(|value| value.to_string())),
        ("response information", properties.response_information.clone()),
        ("server reference", properties.server_reference.clone()),
        ("authentication method", properties.authentication_method.clone()),
    ];

    let mut properties_vec: Vec<(String, String)> = optional_properties
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key.to_string(), value)))
        .collect();

    for (key, value) in &properties.user_properties {
        properties_vec.push((key.clone(), value.clone()));
    }

    properties_vec
}
