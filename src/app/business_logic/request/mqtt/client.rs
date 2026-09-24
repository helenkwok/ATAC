//! Wraps both rumqttc versions behind one interface, so the rest of ATAC doesn't depend on the MQTT library

use std::sync::Arc;
use std::time::Duration;
use rumqttc::tokio_rustls::rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rumqttc::tokio_rustls::rustls::client::WebPkiServerVerifier;
use rumqttc::tokio_rustls::rustls::crypto::aws_lc_rs;
use rumqttc::tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rumqttc::tokio_rustls::rustls::{CertificateError, ClientConfig, DigitallySignedStruct, Error, RootCertStore, SignatureScheme};
use rumqttc::{TlsConfiguration, Transport};
use crate::models::protocol::mqtt::mqtt::{MqttVersion, QoS};

pub struct PreparedMqttRequest {
    pub version: MqttVersion,
    pub host: String,
    pub port: u16,
    pub use_tls: bool,
    pub client_id: String,
    pub clean_session: bool,
    pub session_expiry_interval: u32,
    pub keep_alive: u16,
    pub max_packet_size: u32,
    pub credentials: Option<(String, String)>,
    pub subscriptions: Vec<(String, QoS)>,
    /// In seconds
    pub connection_timeout: u64,
    pub accept_invalid_certs: bool,
    pub accept_invalid_hostnames: bool,
}

/// Sends requests to the event loop, which owns the network connection
pub enum MqttClient {
    V4(rumqttc::AsyncClient),
    V5(rumqttc::v5::AsyncClient),
}

pub enum MqttEventLoop {
    V4(rumqttc::EventLoop),
    V5(rumqttc::v5::EventLoop),
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

/// rumqttc panics on some option values, they are checked beforehand in `prepare_mqtt_request`
impl MqttClient {
    pub fn new(prepared: &PreparedMqttRequest) -> Result<(MqttClient, MqttEventLoop), String> {
        let transport = match prepared.use_tls {
            true => Transport::tls_with_config(tls_configuration(prepared.accept_invalid_certs, prepared.accept_invalid_hostnames)?),
            false => Transport::tcp(),
        };

        match prepared.version {
            MqttVersion::V3_1_1 => {
                let mut options = rumqttc::MqttOptions::new(prepared.client_id.clone(), prepared.host.clone(), prepared.port);

                options
                    .set_transport(transport)
                    .set_keep_alive(Duration::from_secs(prepared.keep_alive as u64))
                    .set_max_packet_size(prepared.max_packet_size as usize, prepared.max_packet_size as usize)
                    .set_clean_session(prepared.clean_session);

                if let Some((username, password)) = &prepared.credentials {
                    options.set_credentials(username.clone(), password.clone());
                }

                let (client, mut event_loop) = rumqttc::AsyncClient::new(options, 10);
                event_loop.network_options.set_connection_timeout(prepared.connection_timeout);

                Ok((MqttClient::V4(client), MqttEventLoop::V4(event_loop)))
            }
            MqttVersion::V5 => {
                let mut options = rumqttc::v5::MqttOptions::new(prepared.client_id.clone(), prepared.host.clone(), prepared.port);

                options
                    .set_transport(transport)
                    .set_keep_alive(Duration::from_secs(prepared.keep_alive as u64))
                    .set_max_packet_size(Some(prepared.max_packet_size))
                    .set_clean_start(prepared.clean_session)
                    .set_connection_timeout(prepared.connection_timeout);

                // MQTT 5 sessions end on disconnect unless an expiry interval is sent.
                // Not sent without a client ID, the broker would assign one that is never reused and keep its session.
                if !prepared.clean_session && !prepared.client_id.is_empty() && prepared.session_expiry_interval > 0 {
                    options.set_session_expiry_interval(Some(prepared.session_expiry_interval));
                }

                if let Some((username, password)) = &prepared.credentials {
                    options.set_credentials(username.clone(), password.clone());
                }

                let (client, event_loop) = rumqttc::v5::AsyncClient::new(options, 10);

                Ok((MqttClient::V5(client), MqttEventLoop::V5(event_loop)))
            }
        }
    }

    pub async fn subscribe(&self, topic: String, qos: QoS) -> Result<(), String> {
        match self {
            MqttClient::V4(client) => client.subscribe(topic, to_v4_qos(qos)).await.map_err(|error| error.to_string()),
            MqttClient::V5(client) => client.subscribe(topic, to_v5_qos(qos)).await.map_err(|error| error.to_string()),
        }
    }

    pub async fn publish(&self, topic: String, payload: Vec<u8>, qos: QoS, retain: bool) -> Result<(), String> {
        match self {
            MqttClient::V4(client) => client.publish(topic, to_v4_qos(qos), retain, payload).await.map_err(|error| error.to_string()),
            MqttClient::V5(client) => client.publish(topic, to_v5_qos(qos), retain, payload).await.map_err(|error| error.to_string()),
        }
    }

    pub async fn disconnect(&self) -> Result<(), String> {
        match self {
            MqttClient::V4(client) => client.disconnect().await.map_err(|error| error.to_string()),
            MqttClient::V5(client) => client.disconnect().await.map_err(|error| error.to_string()),
        }
    }
}

impl MqttEventLoop {
    pub async fn poll(&mut self) -> Result<MqttEvent, String> {
        match self {
            MqttEventLoop::V4(event_loop) => {
                use rumqttc::{Event, Outgoing, Packet};

                match event_loop.poll().await {
                    Ok(Event::Incoming(Packet::ConnAck(connack))) => Ok(MqttEvent::ConnAck {
                        code: format!("{:?}", connack.code),
                        session_present: connack.session_present,
                        properties: vec![],
                    }),
                    Ok(Event::Incoming(Packet::Publish(publish))) => Ok(MqttEvent::Publish {
                        topic: publish.topic,
                        payload: publish.payload.to_vec(),
                        qos: from_v4_qos(publish.qos),
                        retain: publish.retain,
                    }),
                    Ok(Event::Incoming(Packet::SubAck(suback))) => Ok(MqttEvent::SubAck(format!("{:?}", suback.return_codes))),
                    Ok(Event::Incoming(Packet::Disconnect)) => Ok(MqttEvent::Disconnect(String::from("Disconnected by the broker"))),
                    Ok(Event::Outgoing(Outgoing::Disconnect)) => Ok(MqttEvent::Disconnected),
                    Ok(_) => Ok(MqttEvent::Other),
                    Err(error) => Err(error.to_string()),
                }
            }
            MqttEventLoop::V5(event_loop) => {
                use rumqttc::Outgoing;
                use rumqttc::v5::Event;
                use rumqttc::v5::mqttbytes::v5::Packet;

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
                    Err(error) => Err(error.to_string()),
                }
            }
        }
    }
}

/// rumqttc's default TLS configuration panics on platform certificates it can't load, so unreadable ones are skipped instead
fn tls_configuration(accept_invalid_certs: bool, accept_invalid_hostnames: bool) -> Result<TlsConfiguration, String> {
    let provider = Arc::new(aws_lc_rs::default_provider());

    // Not needed when every certificate is accepted, which also works without any platform certificate
    let verifier = match accept_invalid_certs {
        true => None,
        false => {
            let mut root_cert_store = RootCertStore::empty();

            for cert in rustls_native_certs::load_native_certs().certs {
                root_cert_store.add(cert).ok();
            }

            let verifier = WebPkiServerVerifier::builder_with_provider(Arc::new(root_cert_store), provider.clone())
                .build()
                .map_err(|error| error.to_string())?;

            Some(verifier)
        }
    };

    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|error| error.to_string())?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(RequestSettingsVerifier {
            verifier,
            supported_schemes: provider.signature_verification_algorithms.supported_schemes(),
            accept_invalid_hostnames,
        }))
        .with_no_client_auth();

    Ok(TlsConfiguration::Rustls(Arc::new(config)))
}

/// Applies the request "accept invalid certs" and "accept invalid hostnames" settings on top of the usual verification
#[derive(Debug)]
struct RequestSettingsVerifier {
    /// None when invalid certs are accepted
    verifier: Option<Arc<WebPkiServerVerifier>>,
    supported_schemes: Vec<SignatureScheme>,
    accept_invalid_hostnames: bool,
}

impl ServerCertVerifier for RequestSettingsVerifier {
    fn verify_server_cert(&self, end_entity: &CertificateDer<'_>, intermediates: &[CertificateDer<'_>], server_name: &ServerName<'_>, ocsp_response: &[u8], now: UnixTime) -> Result<ServerCertVerified, Error> {
        let Some(verifier) = &self.verifier else {
            return Ok(ServerCertVerified::assertion());
        };

        match verifier.verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now) {
            Err(Error::InvalidCertificate(CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. })) if self.accept_invalid_hostnames => Ok(ServerCertVerified::assertion()),
            result => result
        }
    }

    // Like reqwest, accepting invalid certs also skips the handshake signature checks, which reject e.g. X.509 v1 certificates
    fn verify_tls12_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, Error> {
        match &self.verifier {
            None => Ok(HandshakeSignatureValid::assertion()),
            Some(verifier) => verifier.verify_tls12_signature(message, cert, dss)
        }
    }

    fn verify_tls13_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, Error> {
        match &self.verifier {
            None => Ok(HandshakeSignatureValid::assertion()),
            Some(verifier) => verifier.verify_tls13_signature(message, cert, dss)
        }
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported_schemes.clone()
    }
}

fn to_v4_qos(qos: QoS) -> rumqttc::QoS {
    match qos {
        QoS::AtMostOnce => rumqttc::QoS::AtMostOnce,
        QoS::AtLeastOnce => rumqttc::QoS::AtLeastOnce,
        QoS::ExactlyOnce => rumqttc::QoS::ExactlyOnce,
    }
}

fn from_v4_qos(qos: rumqttc::QoS) -> QoS {
    match qos {
        rumqttc::QoS::AtMostOnce => QoS::AtMostOnce,
        rumqttc::QoS::AtLeastOnce => QoS::AtLeastOnce,
        rumqttc::QoS::ExactlyOnce => QoS::ExactlyOnce,
    }
}

fn to_v5_qos(qos: QoS) -> rumqttc::v5::mqttbytes::QoS {
    match qos {
        QoS::AtMostOnce => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
        QoS::AtLeastOnce => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
        QoS::ExactlyOnce => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
    }
}

fn from_v5_qos(qos: rumqttc::v5::mqttbytes::QoS) -> QoS {
    match qos {
        rumqttc::v5::mqttbytes::QoS::AtMostOnce => QoS::AtMostOnce,
        rumqttc::v5::mqttbytes::QoS::AtLeastOnce => QoS::AtLeastOnce,
        rumqttc::v5::mqttbytes::QoS::ExactlyOnce => QoS::ExactlyOnce,
    }
}

/// Only keeps the properties the broker actually sent
fn connack_properties_to_vec(properties: &rumqttc::v5::mqttbytes::v5::ConnAckProperties) -> Vec<(String, String)> {
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
