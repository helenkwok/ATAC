use chrono::{DateTime, Local};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use strum::Display;
use tokio::sync::mpsc::UnboundedSender;
use crate::app::files::config::SKIP_SAVE_REQUESTS_RESPONSE;
use crate::models::protocol::mqtt::payload::MqttPayload;
use crate::models::protocol::ws::ws::Sender;

/// Broker URL is the request URL (mqtt:// or mqtts://), username and password come from the request basic auth
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttRequest {
    #[serde(default)]
    pub version: MqttVersion,

    /// An empty client ID lets the broker assign one, only possible with a clean session
    #[serde(default)]
    pub client_id: String,

    #[serde(default = "default_clean_session")]
    pub clean_session: bool,

    /// In seconds, MQTT 5 only, how long the broker keeps the session when clean session is off
    #[serde(default = "default_session_expiry_interval")]
    pub session_expiry_interval: u32,

    /// In seconds
    #[serde(default = "default_keep_alive")]
    pub keep_alive: u16,

    /// In bytes, applies to both incoming and outgoing packets
    #[serde(default = "default_max_packet_size")]
    pub max_packet_size: u32,

    #[serde(default)]
    pub subscriptions: Vec<MqttSubscription>,

    #[serde(default)]
    pub publish: MqttPublish,

    #[serde(skip_serializing_if = "should_skip_requests_messages", default = "Vec::default")]
    pub messages: Vec<MqttMessage>,

    #[serde(skip)]
    pub payload: MqttPayload,

    /// Commands for the task owning the broker connection
    #[serde(skip)]
    pub connection: Option<UnboundedSender<MqttCommand>>,

    #[serde(skip)]
    pub is_connected: bool,
}

impl Default for MqttRequest {
    fn default() -> Self {
        MqttRequest {
            version: MqttVersion::default(),
            client_id: String::new(),
            clean_session: default_clean_session(),
            session_expiry_interval: default_session_expiry_interval(),
            keep_alive: default_keep_alive(),
            max_packet_size: default_max_packet_size(),
            subscriptions: vec![],
            publish: MqttPublish::default(),
            messages: vec![],
            payload: MqttPayload::default(),
            connection: None,
            is_connected: false,
        }
    }
}

fn default_clean_session() -> bool {
    true
}

/// Long enough to test resuming a session, without leaving never-expiring sessions on shared brokers
fn default_session_expiry_interval() -> u32 {
    3600
}

fn default_keep_alive() -> u16 {
    60
}

/// rumqttc defaults to 10 KiB, which is too small for a lot of real payloads
fn default_max_packet_size() -> u32 {
    1024 * 1024
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Display, ValueEnum, Serialize, Deserialize)]
pub enum MqttVersion {
    #[default]
    #[serde(rename = "3.1.1")]
    #[strum(to_string = "3.1.1")]
    #[clap(name = "3.1.1")]
    V3_1_1,

    #[serde(rename = "5")]
    #[strum(to_string = "5")]
    #[clap(name = "5")]
    V5,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Display, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QoS {
    #[default]
    #[strum(to_string = "QoS 0")]
    #[clap(name = "0")]
    AtMostOnce,

    #[strum(to_string = "QoS 1")]
    #[clap(name = "1")]
    AtLeastOnce,

    #[strum(to_string = "QoS 2")]
    #[clap(name = "2")]
    ExactlyOnce,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct MqttSubscription {
    pub enabled: bool,
    pub topic: String,
    #[serde(default)]
    pub qos: QoS,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct MqttPublish {
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub qos: QoS,
    #[serde(default)]
    pub retain: bool,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct MqttMessage {
    pub timestamp: DateTime<Local>,
    pub sender: Sender,
    pub content: MqttMessageContent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "content", rename_all = "lowercase")]
pub enum MqttMessageContent {
    Publish {
        topic: String,
        payload: MqttPayload,
        qos: QoS,
        /// Brokers set it on retained messages delivered on subscribe, not on live ones
        retain: bool,
    },
    /// Connection events, e.g. subscription results or disconnections
    Event(String),
}

impl Default for MqttMessageContent {
    fn default() -> Self {
        MqttMessageContent::Event(String::new())
    }
}

#[derive(Debug, Clone)]
pub enum MqttCommand {
    Publish {
        topic: String,
        payload: Vec<u8>,
        qos: QoS,
        retain: bool,
    },
    Disconnect,
}

pub fn should_skip_requests_messages(_: &Vec<MqttMessage>) -> bool {
    *SKIP_SAVE_REQUESTS_RESPONSE.get().unwrap_or(&true)
}
