use serde::Deserialize;
use serde_json::value::RawValue;

pub const CONNECT: &str = "40";
pub const PONG: &str = "3";
#[derive(Debug)]
pub enum Packet {
    Open,
    Connected,
    Ping,
    Event { name: String, argument: Vec<u8> },
    BinaryHeader { name: String },
}

/// Only the observed Engine.IO and single-attachment Socket.IO framing is admitted.
pub fn decode(text: &str) -> Result<Packet, String> {
    if text == "2" {
        return Ok(Packet::Ping);
    }
    if let Some(body) = text.strip_prefix('0') {
        serde_json::from_str::<std::collections::BTreeMap<String, Box<RawValue>>>(body)
            .map_err(|_| "socket.io: invalid opening object")?;
        return Ok(Packet::Open);
    }
    if text.starts_with("40") {
        return Ok(Packet::Connected);
    }
    if text.starts_with("41") {
        // Observed against the real endpoint on 2026-09-16: sent immediately after the connect
        // acknowledgement when the request carried no `Origin` header.
        return Err(
            "socket.io: the server disconnected the namespace (an `origin` setting is usually required)"
                .into(),
        );
    }
    let (body, binary) = if let Some(body) = text.strip_prefix("451-") {
        (body, true)
    } else if let Some(body) = text.strip_prefix("42") {
        (body, false)
    } else {
        return Err("socket.io: unsupported text framing".into());
    };
    let parts: Vec<Box<RawValue>> =
        serde_json::from_str(body).map_err(|_| "socket.io: malformed event array")?;
    if parts.is_empty() || parts.len() > 2 {
        return Err("socket.io: expected event name and at most one argument".into());
    }
    let name: String = serde_json::from_str(parts[0].get())
        .map_err(|_| "socket.io: event name must be a string")?;
    if binary {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Placeholder {
            _placeholder: bool,
            num: u8,
        }
        let placeholder: Placeholder = serde_json::from_str(
            parts
                .get(1)
                .ok_or("socket.io: missing binary placeholder")?
                .get(),
        )
        .map_err(|_| "socket.io: unsupported binary attachment shape")?;
        if !placeholder._placeholder || placeholder.num != 0 {
            return Err("socket.io: expected exactly one binary attachment".into());
        }
        Ok(Packet::BinaryHeader { name })
    } else {
        Ok(Packet::Event {
            name,
            argument: parts
                .get(1)
                .map_or(b"null".to_vec(), |raw| raw.get().as_bytes().to_vec()),
        })
    }
}

pub fn encode_event(name: &str, argument_json: &str) -> String {
    format!(
        "42[{},{}]",
        serde_json::to_string(name).expect("event name serializes"),
        argument_json
    )
}
