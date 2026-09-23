use serde::{Deserialize, Serialize};
use strum::Display;

#[derive(Debug, Clone, PartialEq, Display, Serialize, Deserialize)]
#[serde(tag = "type", content = "content")]
#[serde(rename_all = "lowercase")]
pub enum MqttPayload {
    #[strum(to_string = "Text")]
    Text(String),

    #[strum(to_string = "Binary")]
    Binary(Box<[u8]>),
}

impl Default for MqttPayload {
    fn default() -> Self {
        MqttPayload::Text(String::new())
    }
}

impl MqttPayload {
    /// MQTT payloads are raw bytes, show them as text when they are valid UTF-8
    pub fn from_bytes(bytes: &[u8]) -> Self {
        match std::str::from_utf8(bytes) {
            Ok(text) => MqttPayload::Text(text.to_string()),
            Err(_) => MqttPayload::Binary(bytes.to_vec().into_boxed_slice()),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match &self {
            MqttPayload::Text(text) => text.as_bytes().to_vec(),
            MqttPayload::Binary(bytes) => bytes.to_vec(),
        }
    }

    pub fn to_content(&self) -> String {
        match &self {
            MqttPayload::Text(text) => text.clone(),
            MqttPayload::Binary(bytes) => format!("{:?}", bytes)
        }
    }
}

pub fn next_payload_type(payload: &MqttPayload) -> MqttPayload {
    match payload {
        MqttPayload::Text(text) => MqttPayload::Binary(text.as_bytes().to_vec().into_boxed_slice()),
        MqttPayload::Binary(binary) => MqttPayload::Text(String::from_utf8_lossy(binary).to_string()),
    }
}
