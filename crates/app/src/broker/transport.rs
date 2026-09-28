use super::Clock;
use futures_util::{SinkExt, StreamExt};
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicI64, Ordering},
};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close,
}
pub enum ReadOutcome {
    Frame(Option<Frame>),
    Interrupted,
}
pub trait Transport: Send {
    fn send(&mut self, frame: Frame) -> Result<(), String>;
    fn receive(&mut self, timeout_micros: i64) -> Result<Option<Frame>, String>;
    fn receive_until(
        &mut self,
        deadline_micros: i64,
        clock: &dyn super::Clock,
        _poll: bool,
    ) -> Result<ReadOutcome, String> {
        self.receive(deadline_micros.saturating_sub(clock.now_micros()).max(0))
            .map(ReadOutcome::Frame)
    }
    fn last_send_micros(&self) -> Option<i64> {
        None
    }
    fn close(&mut self) -> Result<(), String>;
}
pub trait Connector: Send {
    fn connect(
        &mut self,
        url: &str,
        headers: &[(String, String)],
    ) -> Result<Box<dyn Transport>, String>;
}
pub trait Http {
    fn get_json(&mut self, url: &str, headers: &[(String, String)]) -> Result<Vec<u8>, String>;
    fn post_json(&mut self, url: &str, headers: &[(String, String)]) -> Result<Vec<u8>, String>;
}

pub fn endpoint_host(url: &str) -> Result<String, String> {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .ok_or("endpoint has no valid host".into())
}

/// The shared synchronous boundary for WebSocket and bootstrap HTTP operations.
pub struct WebSocketConnector {
    runtime: Arc<Runtime>,
    client: reqwest::Client,
}
impl WebSocketConnector {
    pub fn new() -> Result<Self, String> {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_| "cannot start broker transport runtime")?,
        );
        // The retained probes construct this client before secure WebSocket use to select Rustls's provider.
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|_| "cannot initialize secure broker transport")?;
        Ok(Self { runtime, client })
    }
    pub fn http(&self) -> ReqwestHttp {
        ReqwestHttp {
            runtime: Arc::clone(&self.runtime),
            client: self.client.clone(),
        }
    }
}
impl Connector for WebSocketConnector {
    fn connect(
        &mut self,
        url: &str,
        headers: &[(String, String)],
    ) -> Result<Box<dyn Transport>, String> {
        let host = endpoint_host(url)?;
        let mut request = url
            .into_client_request()
            .map_err(|_| format!("websocket {host}: invalid connection address"))?;
        for (name, value) in headers {
            let name = name
                .parse::<tokio_tungstenite::tungstenite::http::HeaderName>()
                .map_err(|_| format!("websocket {host}: invalid header name"))?;
            let value = value
                .parse()
                .map_err(|_| format!("websocket {host}: invalid header value"))?;
            request.headers_mut().insert(name, value);
        }
        let (socket, _) = self
            .runtime
            .block_on(async {
                tokio::time::timeout(
                    Duration::from_secs(10),
                    tokio_tungstenite::connect_async(request),
                )
                .await
            })
            .map_err(|_| format!("websocket {host}: connection timeout"))?
            .map_err(|_| format!("websocket {host}: connection failed"))?;
        Ok(Box::new(WebSocketTransport {
            runtime: Arc::clone(&self.runtime),
            socket,
            host,
        }))
    }
}
struct WebSocketTransport {
    runtime: Arc<Runtime>,
    socket: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    host: String,
}
impl Transport for WebSocketTransport {
    fn send(&mut self, frame: Frame) -> Result<(), String> {
        let message = match frame {
            Frame::Text(text) => Message::Text(text.into()),
            Frame::Binary(bytes) => Message::Binary(bytes.into()),
            Frame::Ping(bytes) => Message::Ping(bytes.into()),
            Frame::Pong(bytes) => Message::Pong(bytes.into()),
            Frame::Close => Message::Close(None),
        };
        self.runtime
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(10), self.socket.send(message)).await
            })
            .map_err(|_| format!("websocket {}: send timeout", self.host))?
            .map_err(|_| format!("websocket {}: send failed", self.host))
    }
    fn receive(&mut self, timeout_micros: i64) -> Result<Option<Frame>, String> {
        let received = self.runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_micros(timeout_micros.max(0) as u64),
                self.socket.next(),
            )
            .await
        });
        let message = match received {
            Err(_) => return Ok(None),
            Ok(None) => return Ok(Some(Frame::Close)),
            Ok(Some(result)) => {
                result.map_err(|_| format!("websocket {}: receive failed", self.host))?
            }
        };
        Ok(Some(match message {
            Message::Text(text) => Frame::Text(text.to_string()),
            Message::Binary(bytes) => Frame::Binary(bytes.to_vec()),
            Message::Ping(bytes) => Frame::Ping(bytes.to_vec()),
            Message::Pong(bytes) => Frame::Pong(bytes.to_vec()),
            Message::Close(_) => Frame::Close,
            Message::Frame(_) => {
                return Err(format!("websocket {}: unsupported raw frame", self.host));
            }
        }))
    }
    fn close(&mut self) -> Result<(), String> {
        self.send(Frame::Close)
    }
}

/// HTTP bytes are decoded by provider-owned readers, never an intermediate JSON value.
pub struct ReqwestHttp {
    runtime: Arc<Runtime>,
    client: reqwest::Client,
}
impl ReqwestHttp {
    fn request(
        &mut self,
        url: &str,
        headers: &[(String, String)],
        post: bool,
    ) -> Result<Vec<u8>, String> {
        let host = endpoint_host(url)?;
        let mut request = if post {
            self.client.post(url)
        } else {
            self.client.get(url)
        };
        for (name, value) in headers {
            request = request.header(name, value);
        }
        self.runtime.block_on(async {
            let response = request
                .send()
                .await
                .map_err(|_| format!("http {host}: request failed"))?;
            if !response.status().is_success() {
                return Err(format!(
                    "http {host}: status {}",
                    response.status().as_u16()
                ));
            }
            response
                .bytes()
                .await
                .map(|bytes| bytes.to_vec())
                .map_err(|_| format!("http {host}: response read failed"))
        })
    }
}
impl Http for ReqwestHttp {
    fn get_json(&mut self, url: &str, headers: &[(String, String)]) -> Result<Vec<u8>, String> {
        self.request(url, headers, false)
    }
    fn post_json(&mut self, url: &str, headers: &[(String, String)]) -> Result<Vec<u8>, String> {
        self.request(url, headers, true)
    }
}

/// Shared replay time and the globally ordered recorded transport scheduler.
#[derive(Clone, Default)]
pub struct ReplayClock {
    time: Arc<AtomicI64>,
    schedule: Arc<(Mutex<RecordedState>, Condvar)>,
}
impl ReplayClock {
    pub fn at(micros: i64) -> Self {
        Self {
            time: Arc::new(AtomicI64::new(micros)),
            ..Self::default()
        }
    }
    fn advance_to(&self, micros: i64) {
        self.time.fetch_max(micros, Ordering::SeqCst);
    }
}
impl super::Clock for ReplayClock {
    fn now_micros(&self) -> i64 {
        self.time.load(Ordering::SeqCst)
    }
    fn sleep(&mut self, micros: i64) {
        self.sleep_until(
            self.time
                .load(Ordering::SeqCst)
                .saturating_add(micros.max(0)),
        );
    }
    fn sleep_until(&mut self, deadline: i64) {
        self.complete();
        let mut state = self.schedule.0.lock().unwrap();
        let session = state.sessions.get(&std::thread::current().id()).cloned();
        while self.time.load(Ordering::SeqCst) < deadline && state.failure.is_none() {
            state = self.park(
                state,
                session.as_deref(),
                RecordedWait::Rate(deadline),
                None,
            );
        }
        if state.failure.is_some() {
            self.advance_to(deadline);
        }
    }
}
impl ReplayClock {
    /// Registration and release of the mutex are atomic with respect to the stall query.
    fn park<'a>(
        &self,
        mut state: std::sync::MutexGuard<'a, RecordedState>,
        session: Option<&str>,
        reason: RecordedWait,
        timeout: Option<Duration>,
    ) -> std::sync::MutexGuard<'a, RecordedState> {
        if let Some(session) = session {
            state.parked.insert(
                session.into(),
                Parked {
                    reason,
                    until: timeout.map(|duration| std::time::Instant::now() + duration),
                },
            );
        }
        state = match timeout {
            Some(timeout) => self.schedule.1.wait_timeout(state, timeout).unwrap().0,
            None => self.schedule.1.wait(state).unwrap(),
        };
        if let Some(session) = session {
            state.parked.remove(session);
        }
        state
    }
    fn notify(&self, state: &mut RecordedState) {
        // A notified worker is runnable even before it reacquires this mutex.
        state.parked.clear();
        self.schedule.1.notify_all();
    }
    pub fn complete(&self) {
        let mut state = self.schedule.0.lock().unwrap();
        if state.in_flight == Some(std::thread::current().id()) {
            state.in_flight = None;
            state.generation += 1;
            self.notify(&mut state);
        }
    }
    pub fn wake(&self, session: &str) {
        let mut state = self.schedule.0.lock().unwrap();
        *state.pending.entry(session.into()).or_default() += 1;
        state.generation += 1;
        self.notify(&mut state);
    }
    pub fn begin(&self, session: &str) {
        let mut state = self.schedule.0.lock().unwrap();
        let pending = state.pending.entry(session.into()).or_default();
        *pending = pending.saturating_sub(1);
        state.parked.remove(session);
        state.generation += 1;
        state
            .sessions
            .insert(std::thread::current().id(), session.into());
    }
    /// A session without a subscription blocks here until the owner queues an intent.
    pub fn idle(&self, session: &str) {
        let mut state = self.schedule.0.lock().unwrap();
        while state.pending.get(session).copied().unwrap_or(0) == 0 && state.failure.is_none() {
            state = self.park(state, Some(session), RecordedWait::Idle, None);
        }
    }
    /// Capture before servicing owner ingress and periodic work.
    pub fn generation(&self) -> u64 {
        self.schedule.0.lock().unwrap().generation
    }
    pub fn stalled(&self, owner_generation: u64) -> bool {
        let state = self.schedule.0.lock().unwrap();
        // Any intervening progress requires an owner pass at the new state, including
        // equal-time frames. A timeout or repeated observation is not evidence of a stall.
        state.generation == owner_generation
            && state.failure.is_none()
            && !state.frames.is_empty()
            && !state.can_progress(self.time.load(Ordering::SeqCst))
    }
    pub fn cancel(&self) {
        let mut state = self.schedule.0.lock().unwrap();
        state
            .failure
            .get_or_insert("recorded transport stopped".into());
        self.notify(&mut state);
    }
    pub fn failure(&self) -> Option<String> {
        self.schedule.0.lock().unwrap().failure.clone()
    }
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedLine {
    session: String,
    at: Option<i64>,
    frame: Option<String>,
    binary: Option<Vec<u8>>,
    expect: Option<String>,
}
#[derive(Default)]
struct RecordedState {
    frames: std::collections::VecDeque<RecordedLine>,
    writes: Vec<(String, String)>,
    requests: std::collections::BTreeMap<(String, u64), u64>,
    pocket_requests: std::collections::BTreeMap<u64, u64>,
    subscriptions: std::collections::BTreeMap<(String, String), u64>,
    in_flight: Option<std::thread::ThreadId>,
    failure: Option<String>,
    parked: std::collections::BTreeMap<String, Parked>,
    generation: u64,
    pending: std::collections::BTreeMap<String, usize>,
    sessions: std::collections::HashMap<std::thread::ThreadId, String>,
}

struct Parked {
    reason: RecordedWait,
    until: Option<std::time::Instant>,
}

#[derive(Clone, Copy)]
enum RecordedWait {
    Idle,
    Read,
    ReadUntil { deadline: i64, poll: bool },
    Write,
    Rate(i64),
}

impl RecordedState {
    fn can_progress(&self, now: i64) -> bool {
        let local_now = std::time::Instant::now();
        self.in_flight.is_some()
            || ["market", "account"]
                .iter()
                .any(|session| match self.parked.get(*session) {
                    None => true,
                    // A timed wait can expire before its worker reacquires the mutex too.
                    Some(wait)
                        if wait.until.is_some_and(|until| local_now >= until)
                            || (wait.until.is_some()
                                && self.pending.get(*session).copied().unwrap_or(0) != 0) =>
                    {
                        true
                    }
                    Some(Parked {
                        reason: RecordedWait::Rate(deadline),
                        ..
                    }) => now >= *deadline,
                    Some(Parked {
                        reason: RecordedWait::ReadUntil { deadline, poll },
                        ..
                    }) => {
                        now >= *deadline
                            || (*poll && self.pending.get(*session).copied().unwrap_or(0) != 0)
                            || self.frames.front().is_some_and(|record| {
                                record.session == *session && self.ready(record)
                            })
                    }
                    Some(Parked {
                        reason: RecordedWait::Idle,
                        ..
                    }) => self.pending.get(*session).copied().unwrap_or(0) != 0,
                    Some(Parked {
                        reason: RecordedWait::Read,
                        ..
                    }) => self
                        .frames
                        .front()
                        .is_some_and(|record| record.session == *session && self.ready(record)),
                    Some(Parked {
                        reason: RecordedWait::Write,
                        ..
                    }) => self
                        .frames
                        .front()
                        .is_some_and(|record| record.session == *session),
                })
    }
    fn ready(&self, record: &RecordedLine) -> bool {
        if record.binary.is_some() {
            return true;
        }
        record.frame.as_ref().is_some_and(|text| {
            let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
                return true;
            };
            record.session == "bootstrap"
                || response_scope(&value).is_some_and(|scope| {
                    self.subscriptions
                        .contains_key(&(record.session.clone(), scope))
                })
                || value["req_id"]
                    .as_u64()
                    .is_none_or(|id| self.requests.contains_key(&(record.session.clone(), id)))
        })
    }
}

/// Only the head line may be consumed, including equal-time lines and expected sends.
/// A consumed frame stays owned until its worker hands the result to the ordered owner.
#[derive(Clone)]
pub struct RecordedConnector {
    clock: ReplayClock,
    session: String,
    last_send_micros: Option<i64>,
}
impl RecordedConnector {
    pub fn open(path: &std::path::Path) -> Result<Self, String> {
        Self::from_jsonl(&std::fs::read_to_string(path).map_err(|error| error.to_string())?)
    }
    pub fn from_jsonl(text: &str) -> Result<Self, String> {
        let mut state = RecordedState::default();
        let mut previous = i64::MIN;
        let mut first = None;
        for (index, line) in text.lines().enumerate() {
            let record: RecordedLine = serde_json::from_str(line)
                .map_err(|error| format!("recorded log line {}: {error}", index + 1))?;
            if !matches!(record.session.as_str(), "market" | "account" | "bootstrap")
                || !matches!(
                    (&record.frame, &record.binary, record.at, &record.expect),
                    (Some(_), None, Some(_), None)
                        | (None, Some(_), Some(_), None)
                        | (None, None, _, Some(_))
                )
            {
                return Err(format!(
                    "recorded log line {}: expected session and either at/frame, at/binary or expect",
                    index + 1
                ));
            }
            if let Some(at) = record.at {
                if at < previous {
                    return Err(format!(
                        "recorded log line {}: receipts are reordered",
                        index + 1
                    ));
                }
                previous = at;
                first.get_or_insert(at);
            }
            state.frames.push_back(record);
        }
        let clock = ReplayClock::at(first.unwrap_or(0));
        *clock.schedule.0.lock().unwrap() = state;
        Ok(Self {
            clock,
            session: "market".into(),
            last_send_micros: None,
        })
    }
    pub fn session(&self, session: &str) -> Result<Self, String> {
        if !matches!(session, "market" | "account") {
            return Err("recorded connector session must be market or account".into());
        }
        Ok(Self {
            session: session.into(),
            ..self.clone()
        })
    }
    pub fn clock(&self) -> ReplayClock {
        self.clock.clone()
    }
    pub fn http(&self) -> RecordedHttp {
        RecordedHttp(self.clone())
    }
    pub fn exhausted(&self) -> bool {
        let state = self.clock.schedule.0.lock().unwrap();
        state.frames.is_empty() && state.in_flight.is_none()
    }
    pub fn writes(&self) -> Vec<(String, String)> {
        self.clock.schedule.0.lock().unwrap().writes.clone()
    }
    pub fn fail(&self, reason: String) {
        let mut state = self.clock.schedule.0.lock().unwrap();
        state.failure.get_or_insert(reason);
        self.clock.notify(&mut state);
    }
    fn send_text(&mut self, text: &str) -> Result<(), String> {
        self.clock.complete();
        let mut state = self.clock.schedule.0.lock().unwrap();
        state.parked.remove(&self.session);
        state
            .sessions
            .insert(std::thread::current().id(), self.session.clone());
        let request: Option<serde_json::Value> = serde_json::from_str(text).ok();
        let actual = request.as_ref().and_then(|v| v["req_id"].as_u64());
        let pocket_actual = pocket_open_request(text);
        // Sends with an explicit expectation wait for their line, never skipping another session.
        while state
            .frames
            .iter()
            .any(|r| r.session == self.session && r.expect.is_some())
            && (state.in_flight.is_some()
                || state
                    .frames
                    .front()
                    .is_some_and(|r| r.session != self.session))
        {
            if let Some(error) = &state.failure {
                return Err(error.clone());
            }
            state = self
                .clock
                .park(state, Some(&self.session), RecordedWait::Write, None);
        }
        if let Some(record) = state.frames.front()
            && record.session == self.session
            && let Some(expected) = &record.expect
        {
            let expected_id = serde_json::from_str::<serde_json::Value>(expected)
                .ok()
                .and_then(|v| v["req_id"].as_u64());
            let pocket_expected = pocket_open_request(expected);
            let comparable =
                if let (Some(recorded), Some(actual)) = (pocket_expected, pocket_actual) {
                    replace_pocket_ids(
                        expected,
                        &std::collections::BTreeMap::from([(recorded, actual)]),
                    )?
                } else {
                    actual.map_or_else(|| Ok(expected.clone()), |id| correlate(expected, id))?
                };
            if comparable != text {
                return Err(format!(
                    "recorded {}: write does not match expect",
                    self.session
                ));
            }
            if let (Some(recorded), Some(actual)) = (expected_id, actual) {
                state
                    .requests
                    .insert((self.session.clone(), recorded), actual);
            }
            if let (Some(recorded), Some(actual)) = (pocket_expected, pocket_actual)
                && state.pocket_requests.insert(recorded, actual).is_some()
            {
                return Err("recorded account: duplicate Pocket requestId expectation".into());
            }
            let record = state.frames.pop_front().unwrap();
            if let Some(at) = record.at {
                self.clock.advance_to(at);
            }
        } else if let (Some(request), Some(actual)) = (&request, actual) {
            // Retained provider captures omit writes. Bind the next unconsumed response identity,
            // never the most recent response type; subscriptions retain their own scope.
            let kind = request_kind(request);
            if let Some(kind) = kind {
                let recorded = state
                    .frames
                    .iter()
                    .filter(|r| r.session == self.session)
                    .filter_map(|r| r.frame.as_ref())
                    .filter_map(|r| serde_json::from_str::<serde_json::Value>(r).ok())
                    .filter(|v| v["msg_type"] == kind)
                    .filter_map(|v| v["req_id"].as_u64())
                    .find(|id| !state.requests.contains_key(&(self.session.clone(), *id)));
                if let Some(id) = recorded {
                    state.requests.insert((self.session.clone(), id), actual);
                }
            }
        }
        if let (Some(request), Some(actual)) = (&request, actual)
            && request.get("subscribe").and_then(|v| v.as_u64()) == Some(1)
            && let Some(scope) = request_scope(request)
        {
            state
                .subscriptions
                .insert((self.session.clone(), scope), actual);
        }
        state.writes.push((self.session.clone(), text.into()));
        self.last_send_micros = Some(self.clock.now_micros());
        state.generation += 1;
        self.clock.notify(&mut state);
        Ok(())
    }
    fn receive_frame(
        &mut self,
        timeout: i64,
        deadline: Option<(i64, bool)>,
    ) -> Result<ReadOutcome, String> {
        self.clock.complete();
        let mut state = self.clock.schedule.0.lock().unwrap();
        let poll = deadline.map_or(timeout <= 10_000, |(_, poll)| poll);
        loop {
            if let Some(error) = &state.failure {
                return Err(error.clone());
            }
            if deadline.is_some_and(|(until, _)| self.clock.now_micros() >= until) {
                return Ok(ReadOutcome::Frame(None));
            }
            let Some(record) = state.frames.front() else {
                if deadline.is_some() {
                    return Ok(ReadOutcome::Frame(None));
                }
                drop(self.clock.park(
                    state,
                    Some(&self.session),
                    RecordedWait::Read,
                    Some(Duration::from_micros(timeout.clamp(0, 10_000) as u64)),
                ));
                return Ok(ReadOutcome::Frame(None));
            };
            if self.session == "bootstrap" && record.session != "bootstrap" {
                return Err("recorded bootstrap: expected bootstrap response before market or account frames".into());
            }
            let ready = state.ready(record);
            if state.in_flight.is_none() && record.session == self.session && ready {
                state.parked.remove(&self.session);
                let record = state.frames.pop_front().unwrap();
                self.clock.advance_to(record.at.unwrap());
                state.in_flight = Some(std::thread::current().id());
                state.generation += 1;
                let Some(text) = record.frame else {
                    let bytes = record.binary.unwrap();
                    let bytes = match std::str::from_utf8(&bytes) {
                        Ok(text) => replace_pocket_ids(text, &state.pocket_requests)?.into_bytes(),
                        Err(_) => bytes,
                    };
                    return Ok(ReadOutcome::Frame(Some(Frame::Binary(bytes))));
                };
                let value: Option<serde_json::Value> = serde_json::from_str(&text).ok();
                let id = value.as_ref().and_then(|v| {
                    let subscription = response_scope(v)
                        .and_then(|scope| state.subscriptions.get(&(self.session.clone(), scope)))
                        .copied();
                    subscription.or_else(|| {
                        v["req_id"]
                            .as_u64()
                            .and_then(|id| state.requests.get(&(self.session.clone(), id)).copied())
                    })
                });
                let text = match id {
                    Some(id) => correlate(&text, id)?,
                    None => text,
                };
                return Ok(ReadOutcome::Frame(Some(Frame::Text(replace_pocket_ids(
                    &text,
                    &state.pocket_requests,
                )?))));
            }
            if let Some((deadline, _)) = deadline {
                if poll && state.pending.get(&self.session).copied().unwrap_or(0) != 0 {
                    return Ok(ReadOutcome::Interrupted);
                }
                state = self.clock.park(
                    state,
                    Some(&self.session),
                    RecordedWait::ReadUntil { deadline, poll },
                    None,
                );
                continue;
            }
            if timeout <= 10_000 {
                // A queued intent is runnable as soon as this ordinary poll returns.
                if state.pending.get(&self.session).copied().unwrap_or(0) == 0 {
                    drop(self.clock.park(
                        state,
                        Some(&self.session),
                        RecordedWait::Read,
                        Some(Duration::from_micros(timeout.max(0) as u64)),
                    ));
                }
                return Ok(ReadOutcome::Frame(None));
            }
            state = self
                .clock
                .park(state, Some(&self.session), RecordedWait::Read, None);
        }
    }
    #[cfg(test)]
    fn receive_text(&mut self, timeout: i64) -> Result<Option<String>, String> {
        let ReadOutcome::Frame(frame) = self.receive_frame(timeout, None)? else {
            unreachable!()
        };
        frame.map_or(Ok(None), |frame| match frame {
            Frame::Text(text) => Ok(Some(text)),
            Frame::Binary(_) => Err("recorded bootstrap: expected text frame".into()),
            _ => unreachable!(),
        })
    }
}
fn request_kind(v: &serde_json::Value) -> Option<&'static str> {
    [
        ("ticks", "tick"),
        ("proposal", "proposal"),
        ("buy", "buy"),
        ("transaction", "transaction"),
        ("proposal_open_contract", "proposal_open_contract"),
        ("portfolio", "portfolio"),
        ("statement", "statement"),
        ("balance", "balance"),
    ]
    .into_iter()
    .find_map(|(field, kind)| v.get(field).map(|_| kind))
}
fn request_scope(v: &serde_json::Value) -> Option<String> {
    match request_kind(v)? {
        "tick" => Some(format!("tick:{}", v["ticks"])),
        "proposal_open_contract" => Some(format!("contract:{}", v["contract_id"])),
        "transaction" => Some("transaction".into()),
        _ => None,
    }
}
fn response_scope(v: &serde_json::Value) -> Option<String> {
    v.get("subscription")?;
    match v["msg_type"].as_str()? {
        "tick" => Some(format!("tick:{}", v["tick"]["symbol"])),
        "proposal_open_contract" => Some(format!(
            "contract:{}",
            v["proposal_open_contract"]["contract_id"]
        )),
        "transaction" => Some("transaction".into()),
        _ => None,
    }
}

/// Replaces only the request correlation token, preserving all provider decimal tokens.
fn correlate(text: &str, id: u64) -> Result<String, String> {
    let fields: std::collections::BTreeMap<&str, &serde_json::value::RawValue> =
        serde_json::from_str(text).map_err(|_| "recorded frame: malformed JSON object")?;
    let Some(raw) = fields.get("req_id") else {
        return Ok(text.into());
    };
    let start = raw.get().as_ptr() as usize - text.as_ptr() as usize;
    let mut result = text.to_string();
    result.replace_range(start..start + raw.get().len(), &id.to_string());
    Ok(result)
}

fn pocket_open_request(text: &str) -> Option<u64> {
    let body = text.strip_prefix("42")?;
    let event: serde_json::Value = serde_json::from_str(body).ok()?;
    (event.get(0)?.as_str()? == "openOrder")
        .then(|| event.get(1)?.get("requestId")?.as_u64())
        .flatten()
}

/// Rewrites only numeric values of JSON `requestId` fields, retaining the other wire tokens.
fn replace_pocket_ids(
    text: &str,
    ids: &std::collections::BTreeMap<u64, u64>,
) -> Result<String, String> {
    if ids.is_empty() || !text.contains("\"requestId\"") {
        return Ok(text.into());
    }
    let json = text.strip_prefix("42").unwrap_or(text);
    serde_json::from_str::<serde_json::Value>(json)
        .map_err(|_| "recorded Pocket frame: malformed JSON")?;
    let mut output = String::with_capacity(text.len());
    let mut scan = 0;
    let mut copied = 0;
    while let Some(relative) = text[scan..].find("\"requestId\"") {
        let key = scan + relative;
        let mut value = key + "\"requestId\"".len();
        while text
            .as_bytes()
            .get(value)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            value += 1;
        }
        if text.as_bytes().get(value) != Some(&b':') {
            scan = value;
            continue;
        }
        value += 1;
        while text
            .as_bytes()
            .get(value)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            value += 1;
        }
        let end = text[value..]
            .find(|ch: char| !ch.is_ascii_digit())
            .map(|offset| value + offset)
            .unwrap_or(text.len());
        if let Ok(recorded) = text[value..end].parse::<u64>()
            && let Some(actual) = ids.get(&recorded)
        {
            output.push_str(&text[copied..value]);
            output.push_str(&actual.to_string());
            scan = end;
            copied = end;
            continue;
        }
        scan = end.max(value);
    }
    output.push_str(&text[copied..]);
    Ok(output)
}
impl Connector for RecordedConnector {
    fn connect(&mut self, _: &str, _: &[(String, String)]) -> Result<Box<dyn Transport>, String> {
        Ok(Box::new(self.clone()))
    }
}
impl Transport for RecordedConnector {
    fn send(&mut self, frame: Frame) -> Result<(), String> {
        match frame {
            Frame::Text(text) => self.send_text(&text),
            Frame::Close => Ok(()),
            _ => Err("recorded transport requires text frames".into()),
        }
    }
    fn receive(&mut self, timeout_micros: i64) -> Result<Option<Frame>, String> {
        let ReadOutcome::Frame(frame) = self.receive_frame(timeout_micros, None)? else {
            unreachable!()
        };
        Ok(frame)
    }
    fn receive_until(
        &mut self,
        deadline_micros: i64,
        clock: &dyn super::Clock,
        poll: bool,
    ) -> Result<ReadOutcome, String> {
        self.receive_frame(
            deadline_micros.saturating_sub(clock.now_micros()).max(0),
            Some((deadline_micros, poll)),
        )
    }
    fn last_send_micros(&self) -> Option<i64> {
        self.last_send_micros
    }
    fn close(&mut self) -> Result<(), String> {
        Ok(())
    }
}

/// Bootstrap responses share the recorded receipt clock and never resolve credentials.
pub struct RecordedHttp(RecordedConnector);
impl RecordedHttp {
    fn request(&mut self, method: &str, url: &str) -> Result<Vec<u8>, String> {
        if self
            .0
            .clock
            .schedule
            .0
            .lock()
            .unwrap()
            .frames
            .front()
            .is_some_and(|record| record.session != "bootstrap")
        {
            return Err(
                "recorded bootstrap: expected bootstrap response before market or account frames"
                    .into(),
            );
        }
        self.0.session = "bootstrap".into();
        self.0.send_text(&format!("{method} {url}"))?;
        let ReadOutcome::Frame(frame) = self.0.receive_frame(i64::MAX, None)? else {
            unreachable!()
        };
        let result = frame
            .map(|frame| match frame {
                Frame::Text(text) => text.into_bytes(),
                Frame::Binary(bytes) => bytes,
                _ => unreachable!(),
            })
            .ok_or("recorded bootstrap: response missing".into());
        self.0.clock.complete();
        result
    }
}
impl Http for RecordedHttp {
    fn get_json(&mut self, url: &str, _: &[(String, String)]) -> Result<Vec<u8>, String> {
        self.request("GET", url)
    }
    fn post_json(&mut self, url: &str, _: &[(String, String)]) -> Result<Vec<u8>, String> {
        self.request("POST", url)
    }
}

#[cfg(test)]
mod recorded_tests {
    use super::*;
    use crate::broker::Clock;

    #[test]
    fn recorded_binary_and_text_frames_keep_receipt_order() {
        let log = [
            serde_json::json!({"session":"market","at":1,"frame":"text"}),
            serde_json::json!({"session":"market","at":1,"binary":[0, 1, 255]}),
            serde_json::json!({"session":"market","at":2,"frame":"last"}),
        ]
        .iter()
        .map(|line| format!("{line}\n"))
        .collect::<String>();
        let recorded = RecordedConnector::from_jsonl(&log).unwrap();
        let mut market = recorded.session("market").unwrap();
        assert_eq!(market.receive(1).unwrap(), Some(Frame::Text("text".into())));
        assert_eq!(
            market.receive(1).unwrap(),
            Some(Frame::Binary(vec![0, 1, 255]))
        );
        assert_eq!(market.receive(1).unwrap(), Some(Frame::Text("last".into())));
        recorded.clock.complete();
        assert!(recorded.exhausted());
        for invalid in [
            r#"{"session":"market","at":1,"frame":"text","binary":[1]}"#,
            r#"{"session":"market","at":1}"#,
            r#"{"session":"market","binary":[1]}"#,
            r#"{"session":"market","at":1,"binary":[256]}"#,
        ] {
            assert!(RecordedConnector::from_jsonl(invalid).is_err());
        }
    }

    #[test]
    fn recorded_binary_and_text_frames_keep_global_session_order() {
        let log = [
            serde_json::json!({"session":"market","at":10,"binary":[1, 2]}),
            serde_json::json!({"session":"account","at":10,"frame":"account"}),
            serde_json::json!({"session":"market","at":11,"frame":"market"}),
        ]
        .iter()
        .map(|line| format!("{line}\n"))
        .collect::<String>();
        let recorded = RecordedConnector::from_jsonl(&log).unwrap();
        let mut account = recorded.session("account").unwrap();
        let mut market = recorded.session("market").unwrap();
        let thread = std::thread::spawn(move || {
            assert_eq!(
                account.receive(20_000_000).unwrap(),
                Some(Frame::Text("account".into()))
            );
            account.clock.complete();
        });
        assert_eq!(
            market.receive(20_000_000).unwrap(),
            Some(Frame::Binary(vec![1, 2]))
        );
        market.clock.complete();
        assert_eq!(
            market.receive(20_000_000).unwrap(),
            Some(Frame::Text("market".into()))
        );
        market.clock.complete();
        thread.join().unwrap();
        assert_eq!(recorded.clock().now_micros(), 11);
        assert!(recorded.exhausted());
    }

    #[test]
    fn recorded_frames_correlate_without_changing_decimal_tokens() {
        let expected = r#"{"proposal":1,"amount":10.00,"req_id":8}"#;
        let frame = r#"{"echo_req":{"req_id":8},"msg_type":"proposal","proposal":{"ask_price":10.00},"req_id":8}"#;
        let log = format!(
            "{}\n{}\n",
            serde_json::json!({"session":"account","expect":expected}),
            serde_json::json!({"session":"account","at":123,"frame":frame})
        );
        let recorded = RecordedConnector::from_jsonl(&log).unwrap();
        let mut transport = recorded.session("account").unwrap();
        transport
            .send(Frame::Text(expected.replace(":8", ":12")))
            .unwrap();
        assert_eq!(
            transport.receive(1).unwrap(),
            Some(Frame::Text(
                frame.replace(",\"req_id\":8}", ",\"req_id\":12}")
            ))
        );
        transport.clock.complete();
        assert_eq!(recorded.clock().now_micros(), 123);
        assert!(recorded.exhausted());
    }
    #[test]
    fn recorded_expect_mismatch_and_reordered_receipts_fail() {
        let mut recorded = RecordedConnector::from_jsonl(
            r#"{"session":"market","expect":"{\"ticks\":\"R_50\",\"req_id\":1}"}"#,
        )
        .unwrap();
        assert_eq!(
            recorded
                .send(Frame::Text(r#"{"ticks":"R_100","req_id":2}"#.into()))
                .unwrap_err(),
            "recorded market: write does not match expect"
        );
        assert!(RecordedConnector::from_jsonl("{\"session\":\"market\",\"at\":2,\"frame\":\"first\"}\n{\"session\":\"account\",\"at\":1,\"frame\":\"second\"}").err().unwrap().contains("receipts are reordered"));
    }
    #[test]
    fn concurrent_sessions_keep_global_order_and_recorded_request_identities() {
        let p1 = r#"{"proposal":1,"req_id":1}"#;
        let p2 = r#"{"proposal":1,"req_id":2}"#;
        let response = |id| format!(r#"{{"msg_type":"proposal","req_id":{id}}}"#);
        let log = [
            serde_json::json!({"session":"account","expect":p1}),
            serde_json::json!({"session":"market","at":10,"frame":"tick"}),
            serde_json::json!({"session":"account","at":10,"frame":response(1)}),
            serde_json::json!({"session":"account","expect":p2}),
            serde_json::json!({"session":"account","at":10,"frame":response(1)}),
            serde_json::json!({"session":"account","at":10,"frame":response(2)}),
        ]
        .iter()
        .map(|v| format!("{v}\n"))
        .collect::<String>();
        let recorded = RecordedConnector::from_jsonl(&log).unwrap();
        let mut account = recorded.session("account").unwrap();
        let mut market = recorded.session("market").unwrap();
        account
            .send(Frame::Text(p1.replace(":1}", ":11}")))
            .unwrap();
        let thread = std::thread::spawn(move || {
            assert_eq!(
                account.receive(20_000_000).unwrap(),
                Some(Frame::Text(response(11)))
            );
            account
                .send(Frame::Text(p2.replace(":2}", ":12}")))
                .unwrap();
            assert_eq!(
                account.receive(20_000_000).unwrap(),
                Some(Frame::Text(response(11)))
            );
            assert_eq!(
                account.receive(20_000_000).unwrap(),
                Some(Frame::Text(response(12)))
            );
            account.clock.complete();
        });
        assert_eq!(
            market.receive(20_000_000).unwrap(),
            Some(Frame::Text("tick".into()))
        );
        market.clock.complete();
        thread.join().unwrap();
        assert_eq!(recorded.clock().now_micros(), 10);
        assert!(recorded.exhausted());
    }
}

#[cfg(test)]
mod scheduler_regressions {
    use super::*;
    use crate::broker::Clock;
    use crate::broker::socket_io::Session;
    use std::sync::mpsc;

    fn blocked_scheduler() -> ReplayClock {
        RecordedConnector::from_jsonl(r#"{"session":"account","at":0,"frame":"{\"req_id\":1}"}"#)
            .unwrap()
            .clock()
    }

    struct CancelOnDrop<'a>(&'a ReplayClock);
    impl Drop for CancelOnDrop<'_> {
        fn drop(&mut self) {
            self.0.cancel();
        }
    }

    fn wait_for_parked(clock: &ReplayClock) {
        let limit = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if clock.schedule.0.lock().unwrap().parked.len() == 2 {
                return;
            }
            assert!(std::time::Instant::now() < limit, "workers did not park");
            std::thread::yield_now();
        }
    }

    #[test]
    fn rate_wait_with_subscribed_head_is_stalled() {
        let recorded = RecordedConnector::from_jsonl(r#"{"session":"account","at":0,"frame":"{\"msg_type\":\"transaction\",\"req_id\":1,\"subscription\":{\"id\":\"tx\"}}"}"#).unwrap();
        let mut account = recorded.session("account").unwrap();
        account
            .send_text(r#"{"transaction":1,"subscribe":1,"req_id":1}"#)
            .unwrap();
        let scheduler = recorded.clock();
        std::thread::scope(|scope| {
            let _cancel = CancelOnDrop(&scheduler);
            scope.spawn(|| scheduler.idle("market"));
            scope.spawn(|| {
                let mut clock = scheduler.clone();
                clock.begin("account");
                clock.sleep_until(3_600_000_000);
            });
            wait_for_parked(&scheduler);
            let stalled = scheduler.stalled(scheduler.generation());
            scheduler.cancel();
            assert!(
                stalled,
                "rate-waiting session cannot consume its subscribed head frame"
            );
        });
    }

    #[test]
    fn notified_workers_are_progress_before_they_reacquire_the_mutex() {
        let scheduler = blocked_scheduler();
        std::thread::scope(|scope| {
            let _cancel = CancelOnDrop(&scheduler);
            scope.spawn(|| scheduler.idle("market"));
            scope.spawn(|| scheduler.idle("account"));
            wait_for_parked(&scheduler);
            assert!(scheduler.stalled(scheduler.generation()));
            {
                let mut state = scheduler.schedule.0.lock().unwrap();
                scheduler.notify(&mut state);
                // Both workers are still unable to run: this thread retains the mutex.
                assert!(state.can_progress(0));
            }
            wait_for_parked(&scheduler);
            let generation = scheduler.generation();
            scheduler.wake("account");
            assert!(!scheduler.stalled(generation));
            assert!(!scheduler.stalled(scheduler.generation()));
            scheduler.cancel();
        });
    }

    #[test]
    fn owner_must_service_the_generation_that_is_declared_stalled() {
        let recorded = RecordedConnector::from_jsonl(
            "{\"session\":\"market\",\"at\":0,\"frame\":\"tick\"}\n{\"session\":\"market\",\"expect\":\"unrequested\"}"
        ).unwrap();
        let scheduler = recorded.clock();
        let generation = scheduler.generation();
        std::thread::scope(|scope| {
            let _cancel = CancelOnDrop(&scheduler);
            scope.spawn(|| scheduler.idle("account"));
            scope.spawn(|| {
                let mut market = recorded.session("market").unwrap();
                assert_eq!(market.receive_text(0).unwrap(), Some("tick".into()));
                // A consumed frame belongs to the worker until its owner handoff completes.
                assert!(!scheduler.stalled(scheduler.generation()));
                scheduler.complete();
                scheduler.idle("market");
            });
            wait_for_parked(&scheduler);
            assert!(!scheduler.stalled(generation));
            assert!(scheduler.stalled(scheduler.generation()));
            scheduler.cancel();
        });
    }

    #[test]
    fn rate_admission_keeps_its_deadline_when_replay_advances_before_waiting() {
        struct AdvancingClock {
            clock: ReplayClock,
            advance_on_read: std::sync::atomic::AtomicBool,
        }
        impl Clock for AdvancingClock {
            fn now_micros(&self) -> i64 {
                let now = self.clock.now_micros();
                if self.advance_on_read.swap(false, Ordering::SeqCst) {
                    let _state = self.clock.schedule.0.lock().unwrap();
                    self.clock.advance_to(now + 1_000_000);
                }
                now
            }
            fn sleep(&mut self, micros: i64) {
                self.clock.sleep(micros);
            }
            fn sleep_until(&mut self, deadline: i64) {
                self.clock.sleep_until(deadline);
            }
        }
        let scheduler = blocked_scheduler();
        let mut clock = AdvancingClock {
            clock: scheduler.clone(),
            advance_on_read: false.into(),
        };
        let mut budget = crate::broker::RateBudget::new(binary_alpha_engine::config::RateBudgets {
            trade: binary_alpha_engine::config::RateLimit {
                per_minute: 1,
                per_hour: 1,
            },
            ..Default::default()
        })
        .unwrap();
        budget.admit(crate::broker::RateGroup::Trade, &mut clock);
        {
            let _state = scheduler.schedule.0.lock().unwrap();
            scheduler.advance_to(20_000_000);
        }
        clock.advance_on_read.store(true, Ordering::SeqCst);
        std::thread::scope(|scope| {
            let _cancel = CancelOnDrop(&scheduler);
            scope.spawn(|| scheduler.idle("market"));
            let account = scope.spawn(move || {
                clock.clock.begin("account");
                budget.admit(crate::broker::RateGroup::Trade, &mut clock);
                assert_eq!(clock.now_micros(), 3_600_000_000);
            });
            wait_for_parked(&scheduler);
            let correct_deadline = {
                let mut state = scheduler.schedule.0.lock().unwrap();
                assert_eq!(scheduler.now_micros(), 21_000_000);
                let correct = matches!(
                    state.parked.get("account"),
                    Some(Parked {
                        reason: RecordedWait::Rate(3_600_000_000),
                        ..
                    })
                );
                scheduler.advance_to(3_600_000_000);
                scheduler.notify(&mut state);
                correct
            };
            if !correct_deadline {
                scheduler.cancel();
            }
            account.join().unwrap();
            scheduler.cancel();
            assert!(
                correct_deadline,
                "replay must not add the elapsed second to the rate deadline"
            );
        });
    }

    #[test]
    fn ready_head_and_elapsed_rate_wait_are_progress_in_the_locked_snapshot() {
        let scheduler = blocked_scheduler();
        std::thread::scope(|scope| {
            let _cancel = CancelOnDrop(&scheduler);
            scope.spawn(|| scheduler.idle("market"));
            scope.spawn(|| {
                let mut clock = scheduler.clone();
                clock.begin("account");
                clock.sleep_until(100);
            });
            wait_for_parked(&scheduler);
            {
                let mut state = scheduler.schedule.0.lock().unwrap();
                assert!(!state.can_progress(0));
                // Another session can advance replay time before the sleeper is notified.
                scheduler.advance_to(100);
                assert!(state.can_progress(100));
                scheduler.notify(&mut state);
            }
            scheduler.cancel();
        });
        let recorded =
            RecordedConnector::from_jsonl(r#"{"session":"market","at":0,"frame":"tick"}"#).unwrap();
        let mut state = recorded.clock.schedule.0.lock().unwrap();
        state.parked.insert(
            "market".into(),
            Parked {
                reason: RecordedWait::Read,
                until: None,
            },
        );
        state.parked.insert(
            "account".into(),
            Parked {
                reason: RecordedWait::Idle,
                until: None,
            },
        );
        assert!(state.can_progress(0));
        state.frames.front_mut().unwrap().expect = Some("unrequested".into());
        state.frames.front_mut().unwrap().frame = None;
        assert!(!state.can_progress(0));
        state.parked.get_mut("market").unwrap().until = Some(std::time::Instant::now());
        assert!(
            state.can_progress(0),
            "an expired poll is runnable before reacquiring the mutex"
        );
    }

    #[test]
    fn pocket_reads_hold_absolute_deadlines_across_market_receipts() {
        for timeout in [10_000, 40_000_000] {
            let warm = if timeout == 10_000 { 29_995_000 } else { 0 };
            let log = format!(
                "{{\"session\":\"market\",\"at\":0,\"frame\":\"seed\"}}\n\
                 {{\"session\":\"account\",\"expect\":\"42[\\\"ps\\\",null]\"}}\n\
                 {{\"session\":\"market\",\"at\":{warm},\"frame\":\"warm\"}}\n\
                 {{\"session\":\"market\",\"at\":30001000,\"frame\":\"crossing\"}}\n\
                 {{\"session\":\"account\",\"expect\":\"42[\\\"ps\\\",null]\"}}\n\
                 {{\"session\":\"account\",\"at\":30002000,\"frame\":\"42[\\\"listing\\\",{{}}]\"}}"
            );
            let recorded = RecordedConnector::from_jsonl(&log).unwrap();
            let mut market = recorded.session("market").unwrap();
            let mut account = recorded.session("account").unwrap();
            let clock = recorded.clock();
            assert_eq!(market.receive_text(0).unwrap(), Some("seed".into()));
            clock.complete();
            let mut session = Session::new(clock.now_micros());
            session.login(&mut account, &clock).unwrap();
            assert_eq!(market.receive_text(0).unwrap(), Some("warm".into()));
            clock.complete();
            let (result, done) = mpsc::channel();
            std::thread::scope(|scope| {
                let _cancel = CancelOnDrop(&clock);
                scope.spawn(|| {
                    let event = if timeout == 10_000 {
                        session.poll(&mut account, &clock, timeout)
                    } else {
                        session.receive(&mut account, &clock, timeout)
                    };
                    clock.complete();
                    result.send(event).unwrap();
                });
                let captured = 30_000_000;
                let limit = std::time::Instant::now() + Duration::from_secs(2);
                loop {
                    let state = clock.schedule.0.lock().unwrap();
                    if let Some(Parked {
                        reason: RecordedWait::ReadUntil { deadline, poll },
                        until: None,
                    }) = state.parked.get("account")
                    {
                        assert_eq!(*deadline, captured);
                        assert_eq!(*poll, timeout == 10_000);
                        break;
                    }
                    drop(state);
                    assert!(std::time::Instant::now() < limit, "read did not park");
                    std::thread::yield_now();
                }
                assert!(
                    done.recv_timeout(Duration::from_millis(25)).is_err(),
                    "host time completed a recorded read"
                );
                assert_eq!(market.receive_text(0).unwrap(), Some("crossing".into()));
                {
                    let mut state = clock.schedule.0.lock().unwrap();
                    state.in_flight = None;
                    assert!(state.can_progress(clock.now_micros()));
                    clock.notify(&mut state);
                }
                let event = done
                    .recv_timeout(Duration::from_secs(2))
                    .unwrap()
                    .unwrap()
                    .unwrap();
                assert_eq!(event.name, "listing");
            });
            assert_eq!(
                recorded
                    .writes()
                    .into_iter()
                    .filter(|(_, text)| text == "42[\"ps\",null]")
                    .count(),
                2
            );
            assert!(recorded.exhausted());
        }
    }

    #[test]
    fn pocket_read_keeps_calculated_deadline_before_registration() {
        struct Gate {
            inner: RecordedConnector,
            captured: Option<mpsc::Sender<i64>>,
            release: Option<mpsc::Receiver<()>>,
        }
        impl Transport for Gate {
            fn send(&mut self, frame: Frame) -> Result<(), String> {
                self.inner.send(frame)
            }
            fn receive(&mut self, timeout: i64) -> Result<Option<Frame>, String> {
                self.inner.receive(timeout)
            }
            fn receive_until(
                &mut self,
                deadline: i64,
                clock: &dyn Clock,
                poll: bool,
            ) -> Result<ReadOutcome, String> {
                if let Some(captured) = self.captured.take() {
                    captured.send(deadline).unwrap();
                    self.release.take().unwrap().recv().unwrap();
                }
                self.inner.receive_until(deadline, clock, poll)
            }
            fn last_send_micros(&self) -> Option<i64> {
                self.inner.last_send_micros()
            }
            fn close(&mut self) -> Result<(), String> {
                self.inner.close()
            }
        }
        let recorded = RecordedConnector::from_jsonl(
            "{\"session\":\"market\",\"at\":0,\"frame\":\"seed\"}\n\
             {\"session\":\"account\",\"expect\":\"42[\\\"ps\\\",null]\"}\n\
             {\"session\":\"market\",\"at\":30001000,\"frame\":\"crossing\"}\n\
             {\"session\":\"account\",\"expect\":\"42[\\\"ps\\\",null]\"}\n\
             {\"session\":\"account\",\"at\":30002000,\"frame\":\"42[\\\"listing\\\",{}]\"}",
        )
        .unwrap();
        let clock = recorded.clock();
        let mut market = recorded.session("market").unwrap();
        assert_eq!(market.receive_text(0).unwrap(), Some("seed".into()));
        clock.complete();
        let (captured, deadline) = mpsc::channel();
        let (release, resume) = mpsc::channel();
        let mut account = Gate {
            inner: recorded.session("account").unwrap(),
            captured: Some(captured),
            release: Some(resume),
        };
        let mut session = Session::new(0);
        session.login(&mut account, &clock).unwrap();
        std::thread::scope(|scope| {
            let _cancel = CancelOnDrop(&clock);
            let worker = scope.spawn(|| {
                let event = session.receive(&mut account, &clock, 40_000_000);
                clock.complete();
                event
            });
            assert_eq!(
                deadline.recv_timeout(Duration::from_secs(2)).unwrap(),
                30_000_000
            );
            assert_eq!(market.receive_text(0).unwrap(), Some("crossing".into()));
            clock.complete();
            release.send(()).unwrap();
            assert_eq!(worker.join().unwrap().unwrap().unwrap().name, "listing");
        });
        assert!(recorded.exhausted());
    }

    #[test]
    fn premature_pocket_write_remains_stalled() {
        let recorded = RecordedConnector::from_jsonl(
            "{\"session\":\"account\",\"expect\":\"42[\\\"ps\\\",null]\"}\n\
             {\"session\":\"account\",\"expect\":\"42[\\\"ps\\\",null]\"}",
        )
        .unwrap();
        let clock = recorded.clock();
        let mut account = recorded.session("account").unwrap();
        let mut session = Session::new(0);
        session.login(&mut account, &clock).unwrap();
        std::thread::scope(|scope| {
            let _cancel = CancelOnDrop(&clock);
            scope.spawn(|| clock.idle("market"));
            scope.spawn(|| {
                let _ = session.poll(&mut account, &clock, 10_000);
            });
            wait_for_parked(&clock);
            assert!(clock.stalled(clock.generation()));
            assert_eq!(recorded.writes().len(), 1);
            clock.cancel();
        });
    }

    #[test]
    fn queued_intent_interrupts_only_an_ordinary_pocket_poll() {
        let recorded = RecordedConnector::from_jsonl(
            "{\"session\":\"account\",\"expect\":\"42[\\\"ps\\\",null]\"}",
        )
        .unwrap();
        let clock = recorded.clock();
        std::thread::scope(|scope| {
            let _cancel = CancelOnDrop(&clock);
            scope.spawn(|| clock.idle("market"));
            let worker = scope.spawn(|| {
                let mut account = recorded.session("account").unwrap();
                account.receive_until(10_000, &clock, true).unwrap()
            });
            wait_for_parked(&clock);
            assert!(clock.stalled(clock.generation()));
            clock.wake("account");
            assert!(!clock.stalled(clock.generation()));
            assert!(matches!(worker.join().unwrap(), ReadOutcome::Interrupted));
            assert_eq!(clock.now_micros(), 0);
            clock.cancel();
        });

        let recorded = RecordedConnector::from_jsonl("").unwrap();
        let clock = recorded.clock();
        std::thread::scope(|scope| {
            let _cancel = CancelOnDrop(&clock);
            scope.spawn(|| clock.idle("market"));
            let worker = scope.spawn(|| {
                let mut account = recorded.session("account").unwrap();
                account.receive_until(10_000, &clock, true).unwrap()
            });
            assert!(matches!(worker.join().unwrap(), ReadOutcome::Frame(None)));
            assert_eq!(clock.now_micros(), 0);
            clock.cancel();
        });

        let recorded = RecordedConnector::from_jsonl(
            "{\"session\":\"account\",\"expect\":\"42[\\\"ps\\\",null]\"}",
        )
        .unwrap();
        let clock = recorded.clock();
        {
            let _state = clock.schedule.0.lock().unwrap();
            clock.advance_to(29_995_000);
        }
        std::thread::scope(|scope| {
            let _cancel = CancelOnDrop(&clock);
            scope.spawn(|| clock.idle("market"));
            let worker = scope.spawn(|| {
                let mut account = recorded.session("account").unwrap();
                account.receive_until(30_000_000, &clock, false).unwrap()
            });
            wait_for_parked(&clock);
            assert!(matches!(
                clock.schedule.0.lock().unwrap().parked.get("account"),
                Some(Parked {
                    reason: RecordedWait::ReadUntil { poll: false, .. },
                    ..
                })
            ));
            clock.wake("account");
            wait_for_parked(&clock);
            assert!(
                !worker.is_finished(),
                "queued intent interrupted a long read"
            );
            {
                let mut state = clock.schedule.0.lock().unwrap();
                clock.advance_to(30_000_000);
                clock.notify(&mut state);
            }
            assert!(matches!(worker.join().unwrap(), ReadOutcome::Frame(None)));
        });
    }

    #[test]
    fn exhausted_recorded_log_ends_absolute_reads_after_an_expected_write() {
        let recorded = RecordedConnector::from_jsonl(
            "{\"session\":\"account\",\"expect\":\"42[\\\"openOrder\\\",{}]\"}",
        )
        .unwrap();
        let clock = recorded.clock();
        let mut account = recorded.session("account").unwrap();
        account
            .send(Frame::Text("42[\"openOrder\",{}]".into()))
            .unwrap();
        assert!(recorded.exhausted());
        for deadline in [10_000, 12_000_000] {
            assert!(matches!(
                account
                    .receive_until(deadline, &clock, deadline == 10_000)
                    .unwrap(),
                ReadOutcome::Frame(None)
            ));
        }
        assert_eq!(clock.now_micros(), 0);
    }

    #[test]
    fn queued_intent_waits_for_a_pocket_binary_attachment() {
        let recorded = RecordedConnector::from_jsonl(
            "{\"session\":\"account\",\"at\":0,\"frame\":\"451-[\\\"listing\\\",{\\\"_placeholder\\\":true,\\\"num\\\":0}]\"}\n\
             {\"session\":\"market\",\"at\":0,\"frame\":\"seed\"}\n\
             {\"session\":\"account\",\"at\":0,\"binary\":[123,125]}",
        )
        .unwrap();
        let clock = recorded.clock();
        std::thread::scope(|scope| {
            let _cancel = CancelOnDrop(&clock);
            let worker = scope.spawn(|| {
                let mut account = recorded.session("account").unwrap();
                let mut session = Session::new(0);
                let event = session.poll(&mut account, &clock, 10_000);
                clock.complete();
                event
            });
            let limit = std::time::Instant::now() + Duration::from_secs(2);
            loop {
                let state = clock.schedule.0.lock().unwrap();
                if let Some(Parked {
                    reason: RecordedWait::ReadUntil { poll: false, .. },
                    ..
                }) = state.parked.get("account")
                {
                    break;
                }
                drop(state);
                assert!(std::time::Instant::now() < limit, "attachment did not park");
                std::thread::yield_now();
            }
            clock.wake("account");
            std::thread::sleep(Duration::from_millis(25));
            assert!(!worker.is_finished(), "intent interrupted the attachment");
            let mut market = recorded.session("market").unwrap();
            assert_eq!(market.receive_text(0).unwrap(), Some("seed".into()));
            clock.complete();
            let event = worker.join().unwrap().unwrap().unwrap();
            assert_eq!(event.name, "listing");
            assert_eq!(event.raw, b"{}");
        });
    }

    #[test]
    fn recorded_read_returns_empty_before_later_account_frame() {
        for deadline in [10_000, 12_000_000] {
            let log = format!(
                "{{\"session\":\"market\",\"at\":0,\"frame\":\"seed\"}}\n\
                 {{\"session\":\"market\",\"at\":{},\"frame\":\"crossing\"}}\n\
                 {{\"session\":\"account\",\"at\":{},\"frame\":\"later\"}}",
                deadline + 1,
                deadline + 2
            );
            let recorded = RecordedConnector::from_jsonl(&log).unwrap();
            let clock = recorded.clock();
            let mut market = recorded.session("market").unwrap();
            let mut account = recorded.session("account").unwrap();
            assert_eq!(market.receive_text(0).unwrap(), Some("seed".into()));
            clock.complete();
            std::thread::scope(|scope| {
                let _cancel = CancelOnDrop(&clock);
                let worker = scope.spawn(|| {
                    let outcome = account
                        .receive_until(deadline, &clock, deadline == 10_000)
                        .unwrap();
                    clock.complete();
                    outcome
                });
                let limit = std::time::Instant::now() + Duration::from_secs(2);
                loop {
                    let state = clock.schedule.0.lock().unwrap();
                    if let Some(Parked {
                        reason: RecordedWait::ReadUntil { deadline: held, .. },
                        until: None,
                    }) = state.parked.get("account")
                    {
                        assert_eq!(*held, deadline);
                        break;
                    }
                    drop(state);
                    assert!(std::time::Instant::now() < limit, "read did not park");
                    std::thread::yield_now();
                }
                std::thread::sleep(Duration::from_millis(25));
                assert!(!worker.is_finished(), "host time completed a recorded read");
                assert_eq!(market.receive_text(0).unwrap(), Some("crossing".into()));
                clock.complete();
                assert!(matches!(worker.join().unwrap(), ReadOutcome::Frame(None)));
                assert_eq!(
                    clock.schedule.0.lock().unwrap().frames.front().unwrap().at,
                    Some(deadline + 2)
                );
            });
        }
    }

    #[test]
    fn pocket_keepalive_deadline_uses_the_consumed_send_instant() {
        struct Gate {
            inner: RecordedConnector,
            consumed: Option<mpsc::Sender<i64>>,
            release: Option<mpsc::Receiver<()>>,
        }
        impl Transport for Gate {
            fn send(&mut self, frame: Frame) -> Result<(), String> {
                let ps = matches!(&frame, Frame::Text(text) if text == "42[\"ps\",null]");
                self.inner.send(frame)?;
                if ps && let Some(consumed) = self.consumed.take() {
                    consumed
                        .send(self.inner.last_send_micros().unwrap())
                        .unwrap();
                    self.release.take().unwrap().recv().unwrap();
                }
                Ok(())
            }
            fn receive(&mut self, timeout: i64) -> Result<Option<Frame>, String> {
                self.inner.receive(timeout)
            }
            fn receive_until(
                &mut self,
                deadline: i64,
                clock: &dyn Clock,
                poll: bool,
            ) -> Result<ReadOutcome, String> {
                self.inner.receive_until(deadline, clock, poll)
            }
            fn last_send_micros(&self) -> Option<i64> {
                self.inner.last_send_micros()
            }
            fn close(&mut self) -> Result<(), String> {
                self.inner.close()
            }
        }
        let recorded = RecordedConnector::from_jsonl(
            "{\"session\":\"market\",\"at\":0,\"frame\":\"seed\"}\n\
             {\"session\":\"account\",\"expect\":\"42[\\\"ps\\\",null]\"}\n\
             {\"session\":\"market\",\"at\":35000000,\"frame\":\"crossing\"}\n\
             {\"session\":\"account\",\"expect\":\"42[\\\"ps\\\",null]\"}\n\
             {\"session\":\"account\",\"at\":35000001,\"frame\":\"42[\\\"listing\\\",{}]\"}",
        )
        .unwrap();
        let clock = recorded.clock();
        let mut market = recorded.session("market").unwrap();
        assert_eq!(market.receive_text(0).unwrap(), Some("seed".into()));
        clock.complete();
        let (consumed, accepted) = mpsc::channel();
        let (release, resume) = mpsc::channel();
        let mut account = Gate {
            inner: recorded.session("account").unwrap(),
            consumed: Some(consumed),
            release: Some(resume),
        };
        std::thread::scope(|scope| {
            let _cancel = CancelOnDrop(&clock);
            let worker = scope.spawn(|| {
                let mut session = Session::new(0);
                session.login(&mut account, &clock).unwrap();
                let event = session.receive(&mut account, &clock, 20_000_000);
                clock.complete();
                event
            });
            assert_eq!(accepted.recv_timeout(Duration::from_secs(2)).unwrap(), 0);
            assert_eq!(market.receive_text(0).unwrap(), Some("crossing".into()));
            clock.complete();
            release.send(()).unwrap();
            assert_eq!(worker.join().unwrap().unwrap().unwrap().name, "listing");
        });
        assert_eq!(
            recorded
                .writes()
                .into_iter()
                .filter(|(_, text)| text == "42[\"ps\",null]")
                .count(),
            2
        );
        assert!(recorded.exhausted());
    }
}
