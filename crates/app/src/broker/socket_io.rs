use super::Clock;
use super::transport::{Frame, ReadOutcome, Transport};
use serde::Deserialize;
use serde_json::value::RawValue;

pub const CONNECT: &str = "40";
pub const PONG: &str = "3";
const KEEPALIVE_MICROS: i64 = 30_000_000;

pub(super) struct Event {
    pub name: String,
    pub raw: Vec<u8>,
    pub receipt_micros: i64,
}

#[derive(Debug)]
pub(super) enum ReceiveError {
    Disconnected,
    Interrupted,
    Transport(String),
    Framing(String),
}

impl From<String> for ReceiveError {
    fn from(error: String) -> Self {
        Self::Framing(error)
    }
}
impl From<&str> for ReceiveError {
    fn from(error: &str) -> Self {
        Self::Framing(error.into())
    }
}
impl From<ReceiveError> for String {
    fn from(error: ReceiveError) -> Self {
        match error {
            ReceiveError::Disconnected => "socket.io: the server disconnected the namespace (an `origin` setting is usually required)".into(),
            ReceiveError::Interrupted => "socket.io: read interrupted for queued intent".into(),
            ReceiveError::Transport(error) | ReceiveError::Framing(error) => error,
        }
    }
}

#[derive(Default)]
pub(super) struct Session {
    pending: Option<String>,
    next_keepalive_micros: Option<i64>,
    pub last_received_frame_micros: i64,
}

impl Session {
    pub fn new(now: i64) -> Self {
        Self {
            last_received_frame_micros: now,
            ..Self::default()
        }
    }

    pub fn login(
        &mut self,
        transport: &mut dyn Transport,
        clock: &dyn Clock,
    ) -> Result<(), ReceiveError> {
        self.send_keepalive(transport, clock)
    }

    fn send_keepalive(
        &mut self,
        transport: &mut dyn Transport,
        clock: &dyn Clock,
    ) -> Result<(), ReceiveError> {
        transport
            .send(Frame::Text(encode_event("ps", "null")))
            .map_err(ReceiveError::Transport)?;
        self.next_keepalive_micros = Some(
            transport
                .last_send_micros()
                .unwrap_or_else(|| clock.now_micros())
                .saturating_add(KEEPALIVE_MICROS),
        );
        Ok(())
    }

    pub fn receive(
        &mut self,
        transport: &mut dyn Transport,
        clock: &dyn Clock,
        timeout_micros: i64,
    ) -> Result<Option<Event>, ReceiveError> {
        self.receive_with_poll(transport, clock, timeout_micros, false)
    }

    pub fn poll(
        &mut self,
        transport: &mut dyn Transport,
        clock: &dyn Clock,
        timeout_micros: i64,
    ) -> Result<Option<Event>, ReceiveError> {
        self.receive_with_poll(transport, clock, timeout_micros, true)
    }

    fn receive_with_poll(
        &mut self,
        transport: &mut dyn Transport,
        clock: &dyn Clock,
        timeout_micros: i64,
        poll: bool,
    ) -> Result<Option<Event>, ReceiveError> {
        let deadline = clock.now_micros().saturating_add(timeout_micros.max(0));
        loop {
            if self.pending.is_none()
                && self
                    .next_keepalive_micros
                    .is_some_and(|next| clock.now_micros() >= next)
            {
                self.send_keepalive(transport, clock)?;
            }
            let wait_until = if self.pending.is_some() {
                deadline
            } else {
                self.next_keepalive_micros
                    .map_or(deadline, |next| deadline.min(next))
            };
            let Some(frame) = (match transport
                .receive_until(wait_until, clock, poll && self.pending.is_none())
                .map_err(ReceiveError::Transport)?
            {
                ReadOutcome::Frame(frame) => frame,
                ReadOutcome::Interrupted => return Err(ReceiveError::Interrupted),
            }) else {
                if self.pending.is_none()
                    && self
                        .next_keepalive_micros
                        .is_some_and(|next| clock.now_micros() >= next)
                {
                    continue;
                }
                return Ok(None);
            };
            let receipt_micros = clock.now_micros();
            self.last_received_frame_micros = receipt_micros;
            match frame {
                Frame::Ping(bytes) => transport
                    .send(Frame::Pong(bytes))
                    .map_err(ReceiveError::Transport)?,
                Frame::Pong(_) => (),
                Frame::Close => {
                    return Err(if self.pending.is_some() {
                        ReceiveError::Framing(
                            "pocket_option: close with incomplete binary attachment".into(),
                        )
                    } else {
                        ReceiveError::Transport("pocket_option: connection closed".into())
                    });
                }
                Frame::Binary(raw) => {
                    let name = self
                        .pending
                        .take()
                        .ok_or("pocket_option: binary attachment without header")?;
                    return Ok(Some(Event {
                        name,
                        raw,
                        receipt_micros,
                    }));
                }
                Frame::Text(text) => match decode(&text)? {
                    Packet::Disconnected => return Err(ReceiveError::Disconnected),
                    Packet::Ping => transport
                        .send(Frame::Text(PONG.into()))
                        .map_err(ReceiveError::Transport)?,
                    Packet::BinaryHeader { name } => {
                        if self.pending.is_some() {
                            return Err("pocket_option: overlapping binary event headers".into());
                        }
                        self.pending = Some(name);
                    }
                    Packet::Event { name, argument } => {
                        if self.pending.is_some() {
                            return Err(
                                "pocket_option: text event interrupted binary attachment".into()
                            );
                        }
                        return Ok(Some(Event {
                            name,
                            raw: argument,
                            receipt_micros,
                        }));
                    }
                    Packet::Open => {
                        return Ok(Some(Event {
                            name: "open".into(),
                            raw: Vec::new(),
                            receipt_micros,
                        }));
                    }
                    Packet::Connected => {
                        return Ok(Some(Event {
                            name: "connected".into(),
                            raw: Vec::new(),
                            receipt_micros,
                        }));
                    }
                },
            }
            if clock.now_micros() >= deadline {
                return Ok(None);
            }
        }
    }
}
#[derive(Debug)]
pub enum Packet {
    Open,
    Connected,
    Disconnected,
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
        return Ok(Packet::Disconnected);
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
