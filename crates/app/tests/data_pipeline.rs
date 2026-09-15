//! Goal-bearing proof of `binary-alpha data pipeline` over synthetic sources, loopback Deriv
//! and Pocket Option brokers, and a loopback Drive: selected intake, seeded backfill, private
//! archive, exact restore, crash and conflict recovery, scope denial, and schedule semantics.
//! Every broker frame, archive byte, and Drive response here is synthetic.

mod common;

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use binary_alpha_app::broker::socket_io;
use binary_alpha_app::data_pipeline::{self, Catalog};
use binary_alpha_app::fetch::HistoryCoverage;
use binary_alpha_app::{archive, verify};
use binary_alpha_engine::dataset::GenerationManifest;
use binary_alpha_engine::market::{Bar, Tick, format_event_time_micros as time_text};
use binary_alpha_engine::stream::StreamManifest;
use common::broker::FakeClock;
use common::{AssetSpec, BarRow, Scratch, bar, write_collection, write_daily_directory};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

// ----------------------------------------------------------------------------------------------
// Synthetic series shared by the archives and the loopback providers
// ----------------------------------------------------------------------------------------------

/// 2025-08-11T00:00:00Z, the first Deriv archive day.
const DAY1: i64 = 1_754_870_400;
const DAY2: i64 = DAY1 + 86_400;
/// 2025-05-19T11:15:00Z, the first Pocket bar.
const POCKET_START: i64 = 1_747_653_300;
const POCKET_OFFSET_S: i64 = 7_200;
const POCKET_SYMBOL_ID: i32 = 538;

/// The provider tick price at Unix second `t`, as exact five-decimal text.
fn deriv_price(t: i64) -> String {
    format!("1.{:05}", 10_000 + (t / 2) % 500)
}

/// One tick every two seconds in `[from, to)`.
fn deriv_ticks(from: i64, to: i64) -> Vec<(i64, String)> {
    (from..to)
        .filter(|t| t % 2 == 0)
        .map(|t| (t, deriv_price(t)))
        .collect()
}

fn units5(text: &str) -> i64 {
    binary_alpha_engine::market::parse_price_units(text, 5.try_into().unwrap()).unwrap()
}

/// The provider bar starting at Unix second `start`: open, high, low, close, volume.
fn synthetic_bar(start: i64) -> [f64; 5] {
    // Integer micro-units divided once, so every price has an exact six-decimal rendering.
    let step = (start / 5) % 100;
    let open = 1_200_000 + step * 1_000;
    let units = |value: i64| value as f64 / 1e6;
    [
        units(open),
        units(open + 500),
        units(open - 500),
        units(open + 200),
        (step % 7) as f64,
    ]
}

fn bar_rows(from: i64, to: i64) -> Vec<BarRow> {
    (from..to)
        .step_by(5)
        .map(|start| bar("AEDCNY_otc", POCKET_SYMBOL_ID, start, synthetic_bar(start)))
        .collect()
}

fn price_text(value: f64) -> String {
    let text = format!("{value:.4}");
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

// ----------------------------------------------------------------------------------------------
// Loopback brokers
// ----------------------------------------------------------------------------------------------

#[derive(Default, Clone)]
struct BrokerFaults {
    delay_ms: u64,
    /// Shift every price before this Unix second by one unit: a conflicting overlap.
    conflict_before: Option<i64>,
    /// Close the connection after this many history responses.
    drop_after_pages: Option<usize>,
    unrelated_frames: usize,
    wrong_asset: bool,
    wrong_index: bool,
    wrong_period: bool,
    off_second: bool,
    real_account: bool,
}

struct FakeBroker {
    url: String,
    requests: Arc<Mutex<Vec<String>>>,
    forbidden: Arc<Mutex<Vec<String>>>,
    faults: Arc<Mutex<BrokerFaults>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl FakeBroker {
    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
    fn forbidden(&self) -> Vec<String> {
        self.forbidden.lock().unwrap().clone()
    }
    fn set(&self, faults: BrokerFaults) {
        *self.faults.lock().unwrap() = faults;
    }
}

impl Drop for FakeBroker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

enum Kind {
    Deriv(Arc<Vec<(i64, String)>>),
    Pocket { from: i64, to: i64 },
}

fn serve_broker(kind: Kind) -> FakeBroker {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let url = match kind {
        Kind::Deriv(_) => format!("ws://{address}/"),
        Kind::Pocket { .. } => format!("ws://{address}/socket.io/?EIO=4&transport=websocket"),
    };
    let requests = Arc::new(Mutex::new(Vec::new()));
    let forbidden = Arc::new(Mutex::new(Vec::new()));
    let faults = Arc::new(Mutex::new(BrokerFaults::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let (requests_, forbidden_, faults_, stop_) = (
        Arc::clone(&requests),
        Arc::clone(&forbidden),
        Arc::clone(&faults),
        Arc::clone(&stop),
    );
    let thread = std::thread::spawn(move || {
        runtime().block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            while !stop_.load(Ordering::SeqCst) {
                let Ok(Ok((socket, _))) =
                    tokio::time::timeout(Duration::from_millis(200), listener.accept()).await
                else {
                    continue;
                };
                let Ok(mut socket) = tokio_tungstenite::accept_async(socket).await else {
                    continue;
                };
                let mut pages = 0usize;
                let mut authenticated = false;
                if let Kind::Pocket { .. } = kind {
                    let _ = socket
                        .send(Message::Text(
                            r#"0{"synthetic":true,"pingInterval":25000,"pingTimeout":20000}"#.into(),
                        ))
                        .await;
                }
                loop {
                    let received =
                        tokio::time::timeout(Duration::from_secs(10), socket.next()).await;
                    let Ok(Some(Ok(message))) = received else { break };
                    let text = match message {
                        Message::Text(text) => text.to_string(),
                        Message::Ping(bytes) => {
                            let _ = socket.send(Message::Pong(bytes)).await;
                            continue;
                        }
                        Message::Close(_) => break,
                        _ => continue,
                    };
                    let faults = faults_.lock().unwrap().clone();
                    let mut replies: Vec<Message> = Vec::new();
                    let mut drop_after = false;
                    match &kind {
                        Kind::Deriv(ticks) => {
                            let fields: BTreeMap<String, Value> = serde_json::from_str(&text).unwrap();
                            let req_id = fields["req_id"].as_u64().unwrap();
                            if fields.contains_key("ticks_history") {
                                requests_.lock().unwrap().push(text.clone());
                                pages += 1;
                                let end = fields["end"].as_str().unwrap();
                                let upper = match end {
                                    "latest" => ticks.len(),
                                    epoch => {
                                        let end: i64 = epoch.parse().unwrap();
                                        ticks.partition_point(|(t, _)| *t <= end)
                                    }
                                };
                                let lower = upper.saturating_sub(100);
                                let page = &ticks[lower..upper];
                                let times: Vec<String> = page.iter().map(|(t, _)| t.to_string()).collect();
                                let prices: Vec<String> = page
                                    .iter()
                                    .map(|(t, price)| {
                                        if faults.conflict_before.is_some_and(|before| *t < before) {
                                            format!("{:.5}", price.parse::<f64>().unwrap() + 0.00001)
                                        } else {
                                            price.clone()
                                        }
                                    })
                                    .collect();
                                replies.push(Message::Text(
                                    format!(
                                        r#"{{"msg_type":"history","req_id":{req_id},"pip_size":5,"history":{{"prices":[{}],"times":[{}]}}}}"#,
                                        prices.join(","),
                                        times.join(",")
                                    )
                                    .into(),
                                ));
                                drop_after = faults.drop_after_pages.is_some_and(|limit| pages >= limit);
                            } else {
                                forbidden_.lock().unwrap().push(text.clone());
                                replies.push(Message::Text(
                                    format!(
                                        r#"{{"msg_type":"error","req_id":{req_id},"error":{{"code":"SyntheticForbidden","message":"not part of the pipeline"}}}}"#
                                    )
                                    .into(),
                                ));
                            }
                        }
                        Kind::Pocket { from, to } => {
                            if text == "40" {
                                replies.push(Message::Text(r#"40{"synthetic":true}"#.into()));
                                continue_send(&mut socket, replies).await;
                                continue;
                            }
                            if text == "3" || text == "2" {
                                continue;
                            }
                            let socket_io::Packet::Event { name, argument } = socket_io::decode(&text).unwrap()
                            else {
                                panic!("unexpected client framing: {text}")
                            };
                            match name.as_str() {
                                "auth" => {
                                    authenticated = true;
                                    replies.push(Message::Text(r#"42["successauth",{"synthetic":true}]"#.into()));
                                    replies.push(Message::Text(
                                        format!(
                                            r#"42["successupdateBalance",{{"isDemo":{},"synthetic":true}}]"#,
                                            u8::from(!faults.real_account)
                                        )
                                        .into(),
                                    ));
                                    let mut row = vec![Value::Null; 19];
                                    row[0] = json!("synthetic");
                                    row[1] = json!("AEDCNY_otc");
                                    replies.extend(attachment("updateAssets", json!([row]).to_string()));
                                }
                                "loadHistoryPeriod" if authenticated => {
                                    let request: Value = serde_json::from_slice(&argument).unwrap();
                                    let period = request["period"].as_u64().unwrap();
                                    if period != 5 {
                                        forbidden_.lock().unwrap().push(text.clone());
                                        replies.push(Message::Text(r#"42["error","synthetic tick history is forbidden"]"#.into()));
                                    } else {
                                        requests_
                                            .lock()
                                            .unwrap()
                                            .push(String::from_utf8_lossy(&argument).into_owned());
                                        pages += 1;
                                        let anchor = request["time"].as_f64().unwrap() as i64 - POCKET_OFFSET_S;
                                        let index = request["index"].as_u64().unwrap();
                                        let last = anchor.div_euclid(5) * 5;
                                        let first = (last - 200).max(*from);
                                        let rows: Vec<Value> = (first..last.min(*to))
                                            .step_by(5)
                                            .map(|start| {
                                                let [open, high, low, close, volume] = synthetic_bar(start);
                                                let shift = if faults.conflict_before.is_some_and(|before| start < before) { 0.0001 } else { 0.0 };
                                                let mut time = (start + POCKET_OFFSET_S).to_string();
                                                if faults.off_second {
                                                    time.push_str(".5");
                                                }
                                                serde_json::from_str::<Value>(&format!(
                                                    r#"{{"symbol_id":{POCKET_SYMBOL_ID},"time":{time},"open":{},"close":{},"high":{},"low":{},"volume":{}}}"#,
                                                    price_text(open + shift),
                                                    price_text(close + shift),
                                                    price_text(high + shift),
                                                    price_text(low + shift),
                                                    volume
                                                ))
                                                .unwrap()
                                            })
                                            .collect();
                                        let payload = json!({
                                            "asset": if faults.wrong_asset { "EURUSD_otc" } else { "AEDCNY_otc" },
                                            "index": if faults.wrong_index { index + 1 } else { index },
                                            "data": rows,
                                            "period": if faults.wrong_period { 60 } else { 5 },
                                        });
                                        for _ in 0..faults.unrelated_frames {
                                            replies.extend(attachment("updateStream", r#"[["AEDCNY_otc",1789406505.1,1.82]]"#.into()));
                                        }
                                        replies.extend(attachment("loadHistoryPeriodFast", payload.to_string()));
                                        drop_after = faults.drop_after_pages.is_some_and(|limit| pages >= limit);
                                    }
                                }
                                other => {
                                    forbidden_.lock().unwrap().push(other.to_string());
                                    replies.push(Message::Text(r#"42["error","synthetic forbidden request"]"#.into()));
                                }
                            }
                        }
                    }
                    if faults.delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(faults.delay_ms)).await;
                    }
                    continue_send(&mut socket, replies).await;
                    if drop_after {
                        let _ = socket.close(None).await;
                        break;
                    }
                }
            }
        });
    });
    FakeBroker {
        url,
        requests,
        forbidden,
        faults,
        stop,
        thread: Some(thread),
    }
}

async fn continue_send<S>(socket: &mut S, replies: Vec<tokio_tungstenite::tungstenite::Message>)
where
    S: futures_util::Sink<tokio_tungstenite::tungstenite::Message> + Unpin,
{
    use futures_util::SinkExt;
    for reply in replies {
        if socket.send(reply).await.is_err() {
            break;
        }
    }
}

fn attachment(name: &str, payload: String) -> Vec<tokio_tungstenite::tungstenite::Message> {
    use tokio_tungstenite::tungstenite::Message;
    vec![
        Message::Text(format!("451-[\"{name}\",{{\"_placeholder\":true,\"num\":0}}]").into()),
        Message::Binary(payload.into_bytes().into()),
    ]
}

// ----------------------------------------------------------------------------------------------
// Loopback Drive
// ----------------------------------------------------------------------------------------------

#[derive(Default, Clone)]
struct DriveFaults {
    /// Close the connection without answering the chunk with this ordinal (per session).
    drop_upload_at_chunk: Option<usize>,
    /// Forget the session before answering this chunk: the client sees 404.
    expire_session_at_chunk: Option<usize>,
    /// Store the final chunk, then close without a reply.
    complete_without_reply: bool,
    omit_sha256: bool,
    /// Close the media download after this many bytes, once.
    drop_download_at: Option<usize>,
    unauthorized_once: bool,
}

#[derive(Clone)]
struct RemoteEntry {
    name: String,
    bytes: Vec<u8>,
    trashed: bool,
}

struct Session {
    id: String,
    name: String,
    total: usize,
    received: Vec<u8>,
    chunks: usize,
    completed: bool,
}

#[derive(Default)]
struct DriveState {
    files: BTreeMap<String, RemoteEntry>,
    sessions: BTreeMap<String, Session>,
    next: usize,
    log: Vec<String>,
    faults: DriveFaults,
}

struct FakeDrive {
    base: String,
    state: Arc<Mutex<DriveState>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl FakeDrive {
    fn set(&self, faults: DriveFaults) {
        self.state.lock().unwrap().faults = faults;
    }
    fn log(&self) -> Vec<String> {
        self.state.lock().unwrap().log.clone()
    }
    fn files(&self) -> BTreeMap<String, RemoteEntry> {
        self.state.lock().unwrap().files.clone()
    }
}

impl Drop for FakeDrive {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn serve_drive() -> FakeDrive {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let state = Arc::new(Mutex::new(DriveState::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let (state_, stop_, base_) = (Arc::clone(&state), Arc::clone(&stop), base.clone());
    let thread = std::thread::spawn(move || {
        while !stop_.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(false).unwrap();
                    let state = Arc::clone(&state_);
                    let base = base_.clone();
                    std::thread::spawn(move || handle_http(stream, &state, &base));
                }
                Err(_) => std::thread::sleep(Duration::from_millis(20)),
            }
        }
    });
    FakeDrive {
        base,
        state,
        stop,
        thread: Some(thread),
    }
}

struct Request {
    method: String,
    path: String,
    query: BTreeMap<String, String>,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

fn read_request(stream: &mut TcpStream) -> Option<Request> {
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .ok()?;
    let mut buffer = Vec::new();
    let mut byte = [0u8; 1];
    while !buffer.ends_with(b"\r\n\r\n") {
        if stream.read(&mut byte).ok()? == 0 {
            return None;
        }
        buffer.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&buffer).into_owned();
    let mut lines = head.lines();
    let request_line = lines.next()?;
    let mut parts = request_line.split(' ');
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();
    let (path, query_text) = target.split_once('?').unwrap_or((&target, ""));
    let query = query_text
        .split('&')
        .filter(|pair| !pair.is_empty())
        .filter_map(|pair| pair.split_once('='))
        .map(|(key, value)| (key.to_string(), percent_decode(value)))
        .collect();
    let headers: BTreeMap<String, String> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    let length: usize = headers
        .get("content-length")
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).ok()?;
    Some(Request {
        method,
        path: path.to_string(),
        query,
        headers,
        body,
    })
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() + 1 => {
                let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap();
                out.push(u8::from_str_radix(hex, 16).unwrap());
                index += 3;
            }
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(out).unwrap()
}

fn respond(stream: &mut TcpStream, status: u16, extra: &[(&str, &str)], body: &[u8]) {
    let reason = match status {
        200 => "OK",
        206 => "Partial Content",
        308 => "Resume Incomplete",
        401 => "Unauthorized",
        404 => "Not Found",
        409 => "Conflict",
        _ => "Error",
    };
    let mut head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in extra {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body);
    let _ = stream.flush();
}

fn file_json(id: &str, entry: &RemoteEntry, omit_sha256: bool) -> Vec<u8> {
    let mut value = json!({
        "id": id,
        "name": entry.name,
        "size": entry.bytes.len().to_string(),
        "trashed": entry.trashed,
    });
    if !omit_sha256 {
        value["sha256Checksum"] = json!(binary_alpha_engine::hex(&Sha256::digest(&entry.bytes)));
    }
    value.to_string().into_bytes()
}

fn handle_http(mut stream: TcpStream, state: &Arc<Mutex<DriveState>>, base: &str) {
    let Some(request) = read_request(&mut stream) else {
        return;
    };
    let mut state = state.lock().unwrap();
    state.log.push(format!(
        "{} {} {} {}",
        request.method,
        request.path,
        request
            .headers
            .get("content-range")
            .cloned()
            .unwrap_or_default(),
        request.headers.get("range").cloned().unwrap_or_default()
    ));
    if request.path == "/token" {
        respond(
            &mut stream,
            200,
            &[],
            br#"{"access_token":"fixture-token","expires_in":3600}"#,
        );
        return;
    }
    if state.faults.unauthorized_once {
        state.faults.unauthorized_once = false;
        respond(&mut stream, 401, &[], b"{}");
        return;
    }
    if request.headers.get("authorization").map(String::as_str) != Some("Bearer fixture-token") {
        respond(&mut stream, 401, &[], b"{}");
        return;
    }
    let faults = state.faults.clone();
    match (request.method.as_str(), request.path.as_str()) {
        ("POST", "/drive/v3/files/generateIds") => {
            let count: usize = request.query["count"].parse().unwrap();
            let ids: Vec<String> = (0..count)
                .map(|_| {
                    state.next += 1;
                    format!("fixture-id-{}", state.next)
                })
                .collect();
            respond(
                &mut stream,
                200,
                &[],
                json!({ "ids": ids }).to_string().as_bytes(),
            );
        }
        ("POST", "/upload/drive/v3/files") => {
            let metadata: Value = serde_json::from_slice(&request.body).unwrap();
            let id = metadata["id"].as_str().unwrap().to_string();
            if state.files.contains_key(&id) {
                respond(&mut stream, 409, &[], b"{}");
                return;
            }
            let total: usize = request.headers["x-upload-content-length"].parse().unwrap();
            state.next += 1;
            let token = format!("session-{}", state.next);
            state.sessions.insert(
                token.clone(),
                Session {
                    id,
                    name: metadata["name"].as_str().unwrap().to_string(),
                    total,
                    received: Vec::new(),
                    chunks: 0,
                    completed: false,
                },
            );
            respond(
                &mut stream,
                200,
                &[("Location", &format!("{base}/upload/session/{token}"))],
                b"",
            );
        }
        ("PUT", path) if path.starts_with("/upload/session/") => {
            let token = path.trim_start_matches("/upload/session/").to_string();
            let range = request
                .headers
                .get("content-range")
                .cloned()
                .unwrap_or_default();
            let Some(session) = state.sessions.get_mut(&token) else {
                respond(&mut stream, 404, &[], b"{}");
                return;
            };
            if session.completed {
                let id = session.id.clone();
                let entry = state.files[&id].clone();
                respond(
                    &mut stream,
                    200,
                    &[],
                    &file_json(&id, &entry, faults.omit_sha256),
                );
                return;
            }
            if range.starts_with("bytes */") && session.received.len() < session.total {
                if session.received.is_empty() {
                    respond(&mut stream, 308, &[], b"");
                } else {
                    let end = session.received.len() - 1;
                    respond(
                        &mut stream,
                        308,
                        &[("Range", &format!("bytes=0-{end}"))],
                        b"",
                    );
                }
                return;
            }
            session.chunks += 1;
            if faults.expire_session_at_chunk == Some(session.chunks) {
                state.faults.expire_session_at_chunk = None;
                state.sessions.remove(&token);
                respond(&mut stream, 404, &[], b"{}");
                return;
            }
            if faults.drop_upload_at_chunk == Some(session.chunks) {
                state.faults.drop_upload_at_chunk = None;
                return;
            }
            let (start, _) = range.trim_start_matches("bytes ").split_once('/').unwrap();
            let start: usize = start
                .split_once('-')
                .map_or(0, |(start, _)| start.parse().unwrap());
            if start == session.received.len() {
                session.received.extend_from_slice(&request.body);
            }
            if session.received.len() >= session.total {
                session.completed = true;
                let id = session.id.clone();
                let entry = RemoteEntry {
                    name: session.name.clone(),
                    bytes: session.received.clone(),
                    trashed: false,
                };
                state.files.insert(id.clone(), entry.clone());
                if faults.complete_without_reply {
                    state.faults.complete_without_reply = false;
                    return;
                }
                respond(
                    &mut stream,
                    200,
                    &[],
                    &file_json(&id, &entry, faults.omit_sha256),
                );
            } else {
                let end = session.received.len() - 1;
                respond(
                    &mut stream,
                    308,
                    &[("Range", &format!("bytes=0-{end}"))],
                    b"",
                );
            }
        }
        ("GET", "/drive/v3/files") => {
            let page: usize = request
                .query
                .get("pageToken")
                .and_then(|token| token.parse().ok())
                .unwrap_or(0);
            let matching: Vec<(String, RemoteEntry)> = state
                .files
                .iter()
                .filter(|(_, entry)| !entry.trashed && entry.name.contains("catalog-"))
                .map(|(id, entry)| (id.clone(), entry.clone()))
                .collect();
            let files: Vec<Value> = matching
                .iter()
                .skip(page * 2)
                .take(2)
                .map(|(id, entry)| {
                    serde_json::from_slice(&file_json(id, entry, faults.omit_sha256)).unwrap()
                })
                .collect();
            let mut body = json!({ "files": files });
            if (page + 1) * 2 < matching.len() {
                body["nextPageToken"] = json!((page + 1).to_string());
            }
            respond(&mut stream, 200, &[], body.to_string().as_bytes());
        }
        ("GET", path) if path.starts_with("/drive/v3/files/") => {
            let id = path.trim_start_matches("/drive/v3/files/");
            let Some(entry) = state.files.get(id).cloned() else {
                respond(&mut stream, 404, &[], b"{}");
                return;
            };
            if request.query.get("alt").map(String::as_str) != Some("media") {
                respond(
                    &mut stream,
                    200,
                    &[],
                    &file_json(id, &entry, faults.omit_sha256),
                );
                return;
            }
            let start: usize = request
                .headers
                .get("range")
                .and_then(|range| {
                    range
                        .trim_start_matches("bytes=")
                        .trim_end_matches('-')
                        .parse()
                        .ok()
                })
                .unwrap_or(0);
            let body = &entry.bytes[start.min(entry.bytes.len())..];
            if let Some(limit) = faults.drop_download_at
                && entry.name.starts_with("object-")
            {
                state.faults.drop_download_at = None;
                let partial = &body[..limit.min(body.len())];
                let head = format!(
                    "HTTP/1.1 {} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    if start > 0 { 206 } else { 200 },
                    body.len()
                );
                let _ = stream.write_all(head.as_bytes());
                let _ = stream.write_all(partial);
                let _ = stream.flush();
                let _ = stream.shutdown(std::net::Shutdown::Both);
                return;
            }
            respond(&mut stream, if start > 0 { 206 } else { 200 }, &[], body);
        }
        _ => respond(&mut stream, 404, &[], b"{}"),
    }
}

// ----------------------------------------------------------------------------------------------
// Configurations
// ----------------------------------------------------------------------------------------------

struct Fixture {
    scratch: Scratch,
    deriv: FakeBroker,
    pocket: FakeBroker,
    drive: FakeDrive,
    pipeline: PathBuf,
}

const DERIV_SERIES_START: i64 = DAY1 - 3_600;
const DERIV_SERIES_END: i64 = DAY2 + 7_200;
/// The seed archive's second day ends here; the gap `[DAY2+120, DAY2+180)` stays unfilled.
const DERIV_SEED_END: i64 = DAY2 + 300;
const POCKET_SEED_END: i64 = POCKET_START + 1_000;
const POCKET_SERIES_END: i64 = POCKET_START + 7_200;

fn deriv_core(endpoint: &str, overlap: u32, max_pages: u32, max_elapsed: u32) -> String {
    format!(
        r#"schema_version = 1
run_mode = "research"

[storage]
historical_data_dir = "unused"
publication_uri = "file:///unused"

[[import.sources]]
kind = "tick_parquet_daily"
path = "sources/deriv"
broker = "deriv"
role = "development"
price_scale = 5
instruments = ["EURUSD"]

[[instruments]]
broker = "deriv"
provider_symbol = "frxEURUSD"
quote_currency = "USD"
price_scale = 5
native_granularity = {{ kind = "tick" }}
candles = [{{ duration_seconds = 10, offset_seconds = 0, min_observations = 1 }}]

[[brokers]]
id = "deriv"
kind = "deriv"
public_endpoint = "{endpoint}"
bootstrap_endpoint = "http://127.0.0.1/trading/v1/options"
app_id = "SYNTHETIC"

[history]
broker = "deriv"
instruments = ["frxEURUSD"]
role = "development"
start = "2025-08-11T00:00:00Z"
end = "2025-08-13T00:00:00Z"
native_granularity = {{ kind = "tick" }}
overlap_seconds = {overlap}
max_pages = {max_pages}
max_elapsed_seconds = {max_elapsed}
"#
    )
}

fn pocket_core(
    endpoint: &str,
    account_class: &str,
    granularity: &str,
    overlap: u32,
    max_pages: u32,
    max_elapsed: u32,
) -> String {
    format!(
        r#"schema_version = 1
run_mode = "research"

[storage]
historical_data_dir = "unused"
publication_uri = "file:///unused"

[[import.sources]]
kind = "bar_parquet_collection"
path = "sources/pocket"
broker = "pocket_option"
role = "development"
manifest = "collection.json"
instruments = ["AEDCNY_otc"]

[[instruments]]
broker = "pocket_option"
provider_symbol = "AEDCNY_otc"
quote_currency = "CNY"
price_scale = 6
native_granularity = {granularity}
candles = [{{ duration_seconds = 10, offset_seconds = 0, min_observations = 1 }}]

[[brokers]]
id = "pocket_option"
kind = "pocket_option"
endpoint = "{endpoint}"
credential = "PIPELINE_SYNTHETIC_AUTH"
account_class = "{account_class}"
server_offset_minutes = 120

[history]
broker = "pocket_option"
instruments = ["AEDCNY_otc"]
role = "development"
start = "2025-05-19T11:15:00Z"
end = "2025-05-20T00:00:00Z"
native_granularity = {granularity}
overlap_seconds = {overlap}
max_pages = {max_pages}
max_elapsed_seconds = {max_elapsed}
"#
    )
}

const BAR_GRANULARITY: &str = r#"{ kind = "bar", period_seconds = 5 }"#;

fn pipeline_toml(
    local_root: &Path,
    drive_base: &str,
    jobs: &[(&str, &str)],
    governance: Option<&str>,
    max_attempts: u32,
) -> String {
    let mut text = format!(
        "schema_version = 1\nlocal_root = \"{}\"\n",
        local_root.display()
    );
    if let Some(uri) = governance {
        text.push_str(&format!("governance_manifest = \"{uri}\"\n"));
    }
    text.push_str(&format!(
        "\n[drive]\nroot_folder_id = \"fixture-root\"\nchunk_bytes = 262144\nrequest_timeout_seconds = 5\nmax_attempts = {max_attempts}\nloopback_endpoint = \"{drive_base}\"\n"
    ));
    for (id, config) in jobs {
        text.push_str(&format!(
            "\n[[jobs]]\nid = \"{id}\"\nconfig = \"{config}\"\nintake_dir = \"raw_sources/{id}\"\nevidence = \"evidence/{id}.md\"\n"
        ));
    }
    text
}

fn write_sources(scratch: &Scratch) {
    // Deriv: two archive days drawn from the provider series, with one unfilled gap on day two.
    let day = |from: i64, to: i64| -> Vec<(i64, f64)> {
        deriv_ticks(from, to)
            .into_iter()
            .map(|(t, price)| (t * 1_000_000_000, price.parse().unwrap()))
            .collect()
    };
    let day1 = day(DAY1, DAY1 + 600);
    let mut day2 = day(DAY2, DERIV_SEED_END);
    day2.retain(|(nanos, _)| {
        let seconds = nanos / 1_000_000_000;
        !(DAY2 + 120..DAY2 + 180).contains(&seconds)
    });
    write_daily_directory(
        &scratch.path("sources/deriv/EURUSD"),
        "EURUSD",
        "frxEURUSD",
        &[("2025-08-11", &day1), ("2025-08-12", &day2)],
    );
    // A neighbouring directory that a selected import never opens.
    fs::create_dir_all(scratch.path("sources/deriv/GBPUSD")).unwrap();
    fs::write(
        scratch.path("sources/deriv/GBPUSD/not-a-daily-file"),
        b"junk",
    )
    .unwrap();
    // Pocket: the selected asset and one excluded asset whose files vanish after listing.
    write_collection(
        &scratch.path("sources/pocket"),
        &[
            AssetSpec {
                asset: "AEDCNY_otc",
                expected_symbol_id: None,
                symbol_id: Some(POCKET_SYMBOL_ID),
                files: vec![bar_rows(POCKET_START, POCKET_SEED_END)],
                metadata: true,
            },
            AssetSpec {
                asset: "EXCLUDED_otc",
                expected_symbol_id: None,
                symbol_id: Some(1),
                files: vec![bar_rows(POCKET_START, POCKET_START + 50)],
                metadata: true,
            },
        ],
    );
    fs::remove_dir_all(scratch.path("sources/pocket/EXCLUDED_otc/dataset")).unwrap();
    fs::write(
        scratch.path("sources/pocket/EXCLUDED_otc/download_manifest.json"),
        b"{ not json",
    )
    .unwrap();
    fs::create_dir_all(scratch.path("evidence")).unwrap();
    for job in ["deriv", "pocket"] {
        fs::write(
            scratch.path(&format!("evidence/{job}.md")),
            format!(
                "# Synthetic source binding for {job}\n\nLoopback fixture; no operator archive.\n"
            ),
        )
        .unwrap();
    }
}

fn fixture(name: &str) -> Fixture {
    let scratch = Scratch::new(name);
    write_sources(&scratch);
    let deriv = serve_broker(Kind::Deriv(Arc::new(deriv_ticks(
        DERIV_SERIES_START,
        DERIV_SERIES_END,
    ))));
    let pocket = serve_broker(Kind::Pocket {
        from: POCKET_START,
        to: POCKET_SERIES_END,
    });
    let drive = serve_drive();
    fs::write(
        scratch.path("deriv.toml"),
        deriv_core(&deriv.url, 60, 50, 60),
    )
    .unwrap();
    fs::write(
        scratch.path("pocket.toml"),
        pocket_core(&pocket.url, "demo", BAR_GRANULARITY, 60, 50, 60),
    )
    .unwrap();
    let pipeline = scratch.path("pipeline.toml");
    fs::write(
        &pipeline,
        pipeline_toml(
            &scratch.path("producer"),
            &drive.base,
            &[("deriv", "deriv.toml"), ("pocket", "pocket.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    Fixture {
        scratch,
        deriv,
        pocket,
        drive,
        pipeline,
    }
}

// ----------------------------------------------------------------------------------------------
// Command and store helpers
// ----------------------------------------------------------------------------------------------

fn run(args: &[&str]) -> Result<String, String> {
    let output = Command::new(env!("CARGO_BIN_EXE_binary-alpha"))
        .args(args)
        .env("PIPELINE_SYNTHETIC_AUTH", "{\"synthetic\":true}")
        .output()
        .unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    if output.status.success() {
        assert!(stderr.is_empty(), "{stderr}");
        Ok(stdout)
    } else {
        assert_eq!(output.status.code(), Some(1), "{stdout}{stderr}");
        Err(format!("{stdout}{stderr}"))
    }
}

fn pipeline(command: &str, config: &Path, extra: &[&str]) -> Result<String, String> {
    let mut args = vec![
        "data",
        "pipeline",
        command,
        "--config",
        config.to_str().unwrap(),
    ];
    args.extend_from_slice(extra);
    run(&args)
}

fn field<'a>(line: &'a str, key: &str) -> &'a str {
    let rest = line
        .strip_prefix(&format!("{key} "))
        .or_else(|| line.split(&format!(" {key} ")).nth(1))
        .unwrap_or_else(|| panic!("`{key}` in `{line}`"));
    rest.split(' ').next().unwrap()
}

fn job_line<'a>(report: &'a str, job: &str) -> &'a str {
    report
        .lines()
        .find(|line| line.starts_with("pipeline ") && line.contains(&format!(" {job} ")))
        .unwrap_or_else(|| panic!("no pipeline line for {job} in {report}"))
}

fn dataset(store: &Path, generation: &str) -> GenerationManifest {
    GenerationManifest::from_json(
        &fs::read(store.join(format!("manifests/{generation}/ready.json"))).unwrap(),
    )
    .unwrap()
}

fn stream(store: &Path, generation: &str) -> StreamManifest {
    StreamManifest::from_json(
        &fs::read(store.join(format!("manifests/{generation}/ready.json"))).unwrap(),
    )
    .unwrap()
}

fn coverage(store: &Path, manifest: &GenerationManifest) -> HistoryCoverage {
    let object = manifest
        .objects
        .iter()
        .find(|object| object.path == "provenance/coverage.json")
        .unwrap();
    serde_json::from_slice(&fs::read(store.join(&object.key)).unwrap()).unwrap()
}

fn ticks(store: &Path, manifest: &GenerationManifest) -> Vec<Tick> {
    common::read_normalized_ticks(store, manifest)
}

fn bars(store: &Path, manifest: &GenerationManifest) -> Vec<Bar> {
    // An imported collection keeps its listed files as data; broker history normalizes once.
    let role = match manifest.source_kind {
        binary_alpha_engine::dataset::SourceKind::BarParquet => {
            binary_alpha_engine::dataset::ObjectRole::Source
        }
        _ => binary_alpha_engine::dataset::ObjectRole::Normalized,
    };
    let mut rows = Vec::new();
    for object in manifest.objects.iter().filter(|object| object.role == role) {
        archive::validate_bar_file_with(
            &store.join(&object.key),
            &verify::bar_expectation(manifest).unwrap(),
            |bar| {
                rows.push(bar);
                Ok(())
            },
        )
        .unwrap();
    }
    rows
}

/// The expected retained rows: the seed rows plus every provider row from `fetch_start` to
/// the cutoff, deduplicated by time.
fn expected_ticks(seed_end: i64, fetch_start: i64, cutoff: i64) -> Vec<Tick> {
    let mut rows: Vec<Tick> = deriv_ticks(DAY1, DAY1 + 600)
        .into_iter()
        .chain(
            deriv_ticks(DAY2, seed_end)
                .into_iter()
                .filter(|(t, _)| !(DAY2 + 120..DAY2 + 180).contains(t)),
        )
        .chain(deriv_ticks(fetch_start, cutoff))
        .map(|(t, price)| Tick {
            event_time_micros: t * 1_000_000,
            price_units: units5(&price),
        })
        .collect();
    rows.sort_by_key(|tick| tick.event_time_micros);
    rows.dedup();
    rows
}

fn expected_bars(seed_end: i64, fetch_start: i64, cutoff: i64) -> Vec<Bar> {
    let mut rows: Vec<Bar> = (POCKET_START..seed_end)
        .step_by(5)
        .chain(
            (fetch_start..cutoff)
                .step_by(5)
                .filter(|start| start + 5 <= cutoff),
        )
        .map(|start| {
            let [open, high, low, close, volume] = synthetic_bar(start);
            Bar {
                start_unix_s: start,
                open,
                high,
                low,
                close,
                volume,
                period_s: 5,
            }
        })
        .collect();
    rows.sort_by_key(|bar| bar.start_unix_s);
    rows.dedup_by_key(|bar| bar.start_unix_s);
    rows
}

fn read_json(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

/// Every `(close_time_micros, known_at_micros)` pair of one candle object.
fn candle_clocks(store: &Path, stream: &StreamManifest) -> Vec<(i64, i64)> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::record::RowAccessor;
    let object = stream
        .objects
        .iter()
        .find(|object| object.path != "profile.json")
        .unwrap();
    let reader = SerializedFileReader::new(File::open(store.join(&object.key)).unwrap()).unwrap();
    reader
        .get_row_iter(None)
        .unwrap()
        .map(|row| {
            let row = row.unwrap();
            (
                row.get_timestamp_micros(1).unwrap(),
                row.get_timestamp_micros(2).unwrap(),
            )
        })
        .collect()
}

// ----------------------------------------------------------------------------------------------
// Gates 1 and 2
// ----------------------------------------------------------------------------------------------

#[test]
fn pipeline_roundtrip() {
    let f = fixture("pipeline_roundtrip");
    let store = f.scratch.path("producer/store");
    let bootstrap = pipeline("bootstrap", &f.pipeline, &[]).unwrap();
    let deriv_seed = field(job_line(&bootstrap, "deriv"), "dataset").to_string();
    let pocket_seed = field(job_line(&bootstrap, "pocket"), "dataset").to_string();
    // Selected intake: only the selected trees exist beneath the intake, the projection names
    // relative roots, and the excluded asset's broken tree was never opened.
    assert!(f.scratch.path("producer/raw_sources/deriv/EURUSD").is_dir());
    assert!(!f.scratch.path("producer/raw_sources/deriv/GBPUSD").exists());
    assert!(
        !f.scratch
            .path("producer/raw_sources/pocket/EXCLUDED_otc")
            .exists()
    );
    let projection = read_json(
        &f.scratch
            .path("producer/raw_sources/pocket/collection.intake.json"),
    );
    assert_eq!(
        projection["assets"]["AEDCNY_otc"]["asset_root"],
        json!("AEDCNY_otc")
    );
    assert_eq!(
        projection["assets"]["AEDCNY_otc"]["dataset_root"],
        json!("AEDCNY_otc/dataset")
    );
    assert!(projection["assets"].get("EXCLUDED_otc").is_none());
    assert_eq!(
        projection["binary_alpha_intake"]["selected"],
        json!(["AEDCNY_otc"])
    );
    let seed_bars = bars(&store, &dataset(&store, &pocket_seed));
    assert_eq!(
        seed_bars,
        expected_bars(POCKET_SEED_END, POCKET_SEED_END, POCKET_SEED_END)
    );
    assert_eq!(
        ticks(&store, &dataset(&store, &deriv_seed)),
        expected_ticks(DERIV_SEED_END, DERIV_SEED_END, DERIV_SEED_END)
    );
    // Repeated bootstrap reuses identical generations in the one managed store.
    let again = pipeline("bootstrap", &f.pipeline, &[]).unwrap();
    assert_eq!(field(job_line(&again, "deriv"), "dataset"), deriv_seed);
    assert_eq!(field(job_line(&again, "pocket"), "dataset"), pocket_seed);
    assert!(again.contains("(already published)"), "{again}");
    assert!(
        fs::read_to_string(
            f.scratch
                .path("producer/pipeline_state/deriv/bootstrap.toml")
        )
        .unwrap()
        .contains("file://"),
        "publication stays on the local filesystem"
    );

    // First update to a pinned cutoff two pages past the seed frontier, ending inside a bar.
    let deriv_cutoff = DERIV_SEED_END + 1_200;
    let pocket_cutoff = POCKET_SEED_END + 362;
    let first = pipeline(
        "update",
        &f.pipeline,
        &["--end", &time_text(pocket_cutoff * 1_000_000)],
    )
    .unwrap_err();
    // The Deriv cutoff and the Pocket cutoff are one pinned instant; Deriv's provider has no
    // rows that late, so its job reports its tail honestly while Pocket archives cleanly.
    assert!(first.contains("pipeline update pocket "), "{first}");
    let pocket_line = job_line(&first, "pocket");
    assert_eq!(field(pocket_line, "status"), "archived");
    let pocket_first = field(pocket_line, "dataset").to_string();
    let pocket_stream = field(pocket_line, "stream").to_string();
    let bars_after = bars(&store, &dataset(&store, &pocket_first));
    let pocket_fetch_start = POCKET_SEED_END - 60;
    assert_eq!(
        bars_after,
        expected_bars(POCKET_SEED_END, pocket_fetch_start, pocket_cutoff)
    );
    assert_eq!(
        bars_after.last().unwrap().start_unix_s + 5,
        pocket_cutoff - 2,
        "incomplete final bar withheld"
    );
    let pocket_coverage = coverage(&store, &dataset(&store, &pocket_first));
    assert_eq!(
        pocket_coverage.seed.as_ref().unwrap().generation,
        pocket_seed
    );
    assert_eq!(
        pocket_coverage.verified.as_ref().unwrap().start,
        time_text(pocket_fetch_start * 1_000_000)
    );
    assert!(pocket_coverage.pages.len() >= 2, "{pocket_coverage:?}");
    let pocket_manifest = dataset(&store, &pocket_first);
    assert!(
        pocket_manifest
            .objects
            .iter()
            .any(|object| object.path == "seed/ready.json")
    );
    assert!(
        pocket_manifest
            .objects
            .iter()
            .any(|object| object.path.starts_with("seed/collection/"))
    );
    // Exact integer conversion of every provider price at the configured scale.
    for bar in &bars_after {
        let observation =
            binary_alpha_engine::stream::Observation::from_bar(bar, 6.try_into().unwrap()).unwrap();
        let binary_alpha_engine::stream::Observation::Bar(units) = observation else {
            unreachable!()
        };
        assert_eq!(units.open, (bar.open * 1e6).round() as i64);
        assert_eq!(units.close, (bar.close * 1e6).round() as i64);
    }
    // Bars are known at their end; the stream's candles record exactly that clock.
    for (close, known_at) in candle_clocks(&store, &stream(&store, &pocket_stream)) {
        assert_eq!(known_at, close);
    }
    assert!(
        f.pocket.forbidden().is_empty(),
        "{:?}",
        f.pocket.forbidden()
    );

    // Deriv over its own cutoff: the provider series covers it, so the same command archives.
    let deriv_only = f.scratch.path("deriv-only.toml");
    fs::write(
        &deriv_only,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("deriv", "deriv.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    let second = pipeline(
        "update",
        &deriv_only,
        &["--end", &time_text(deriv_cutoff * 1_000_000)],
    )
    .unwrap();
    let deriv_line = job_line(&second, "deriv");
    assert_eq!(field(deriv_line, "status"), "archived");
    let deriv_first = field(deriv_line, "dataset").to_string();
    let deriv_stream = field(deriv_line, "stream").to_string();
    // The seed's last tick lies two seconds before its end; the acquisition starts one overlap
    // before that frontier.
    let deriv_fetch_start = DERIV_SEED_END - 2 - 60;
    let rows = ticks(&store, &dataset(&store, &deriv_first));
    assert_eq!(
        rows,
        expected_ticks(DERIV_SEED_END, deriv_fetch_start, deriv_cutoff)
    );
    assert!(
        !rows
            .iter()
            .any(|tick| (DAY2 + 120..DAY2 + 180).contains(&(tick.event_time_micros / 1_000_000))),
        "inherited gap stays"
    );
    let deriv_coverage = coverage(&store, &dataset(&store, &deriv_first));
    assert_eq!(
        deriv_coverage.verified.as_ref().unwrap().start,
        time_text(deriv_fetch_start * 1_000_000)
    );
    assert_eq!(
        deriv_coverage.actual.as_ref().unwrap().first,
        time_text(DAY1 * 1_000_000)
    );
    for (close, known_at) in candle_clocks(&store, &stream(&store, &deriv_stream)) {
        assert!(known_at >= close);
    }
    assert!(f.deriv.forbidden().is_empty());
    let requests_before = f.deriv.requests().len();

    // Second acquisition with an advancing cutoff starts at the persisted frontier minus the
    // overlap and never refetches the seed.
    let deriv_cutoff_2 = deriv_cutoff + 600;
    let third = pipeline(
        "update",
        &deriv_only,
        &["--end", &time_text(deriv_cutoff_2 * 1_000_000)],
    )
    .unwrap();
    let deriv_second = field(job_line(&third, "deriv"), "dataset").to_string();
    let rows = ticks(&store, &dataset(&store, &deriv_second));
    assert_eq!(
        rows,
        expected_ticks(DERIV_SEED_END, deriv_fetch_start, deriv_cutoff_2)
    );
    let anchors: Vec<i64> = f.deriv.requests()[requests_before..]
        .iter()
        .map(|request| {
            let fields: Value = serde_json::from_str(request).unwrap();
            fields["end"].as_str().unwrap().parse().unwrap()
        })
        .collect();
    assert_eq!(anchors[0], deriv_cutoff_2);
    let frontier = deriv_cutoff - 2;
    assert!(
        anchors.iter().all(|anchor| *anchor >= frontier - 60 - 200),
        "{anchors:?}"
    );
    assert!(anchors.len() <= 5, "{anchors:?}");
    let coverage_2 = coverage(&store, &dataset(&store, &deriv_second));
    assert_eq!(
        coverage_2.verified.as_ref().unwrap().start,
        time_text(deriv_fetch_start * 1_000_000)
    );
    assert_eq!(coverage_2.seed.as_ref().unwrap().generation, deriv_seed);

    // Gate 2: portable closure into a fresh directory after the producer's inputs vanish.
    let listing = pipeline(
        "list",
        &f.pipeline,
        &["--broker", "pocket_option", "--symbol", "AEDCNY_otc"],
    )
    .unwrap();
    let catalog_line = listing
        .lines()
        .find(|line| line.contains(&format!(" dataset {pocket_first} ")))
        .unwrap_or_else(|| panic!("{listing}"));
    let catalog_id = field(catalog_line, "catalog").to_string();
    let catalog_sha = field(catalog_line, "sha256").to_string();
    let deriv_listing = pipeline(
        "list",
        &f.pipeline,
        &["--broker", "deriv", "--symbol", "frxEURUSD"],
    )
    .unwrap();
    assert_eq!(deriv_listing.lines().count(), 3, "{deriv_listing}");
    let expected_dataset_bytes =
        fs::read(store.join(format!("manifests/{pocket_first}/ready.json"))).unwrap();
    let expected_stream_bytes =
        fs::read(store.join(format!("manifests/{pocket_stream}/ready.json"))).unwrap();
    let pocket_objects = dataset(&store, &pocket_first).objects.clone();
    fs::rename(&store, f.scratch.path("producer/store.gone")).unwrap();
    fs::rename(
        f.scratch.path("producer/raw_sources"),
        f.scratch.path("producer/raw_sources.gone"),
    )
    .unwrap();
    fs::rename(f.scratch.path("sources"), f.scratch.path("sources.gone")).unwrap();
    let consumer = f.scratch.path("consumer.toml");
    fs::write(
        &consumer,
        pipeline_toml(
            &f.scratch.path("elsewhere/consumer"),
            &f.drive.base,
            &[],
            None,
            3,
        ),
    )
    .unwrap();
    let restored = pipeline(
        "restore",
        &consumer,
        &[
            "--catalog",
            &catalog_id,
            "--sha256",
            &catalog_sha,
            "--broker",
            "pocket_option",
            "--symbol",
            "AEDCNY_otc",
        ],
    )
    .unwrap();
    let dataset_uri = field(&restored, "dataset").to_string();
    let stream_uri = field(&restored, "stream").to_string();
    let consumer_store = f.scratch.path("elsewhere/consumer/store");
    assert_eq!(
        dataset_uri,
        format!(
            "file://{}/manifests/{pocket_first}/ready.json",
            consumer_store.display()
        )
    );
    assert_eq!(
        fs::read(consumer_store.join(format!("manifests/{pocket_first}/ready.json"))).unwrap(),
        expected_dataset_bytes
    );
    assert_eq!(
        fs::read(consumer_store.join(format!("manifests/{pocket_stream}/ready.json"))).unwrap(),
        expected_stream_bytes
    );
    for object in &pocket_objects {
        let bytes = fs::read(consumer_store.join(&object.key)).unwrap();
        assert_eq!(bytes.len() as u64, object.bytes);
        assert_eq!(
            binary_alpha_engine::hex(&Sha256::digest(&bytes)),
            object.sha256
        );
    }
    assert!(!f.drive.log().iter().any(|line| line.contains("EXCLUDED")));
    assert_eq!(
        bars(&consumer_store, &dataset(&consumer_store, &pocket_first)),
        bars_after
    );
    // The existing feature consumer reaches an asserted output from the restored closure, and
    // re-auditing under the same definition names the same stream generation.
    let features = f.scratch.path("features.toml");
    fs::write(
        &features,
        format!(
            r#"schema_version = 1
run_mode = "research"

[storage]
historical_data_dir = "{store}"
publication_uri = "file://{store}"

[[instruments]]
broker = "pocket_option"
provider_symbol = "AEDCNY_otc"
quote_currency = "CNY"
price_scale = 6
native_granularity = {{ kind = "bar", period_seconds = 5 }}
candles = [{{ duration_seconds = 10, offset_seconds = 0, min_observations = 1 }}]

[[features.instruments]]
role = "development"
input_manifest = "{dataset_uri}"
profile_manifest = "{stream_uri}"
streams = [{{ duration_seconds = 10, offset_seconds = 0 }}]
outputs = ["candle_direction", "range_bps"]
"#,
            store = consumer_store.display()
        ),
    )
    .unwrap();
    let built = run(&["features", "build", "--config", features.to_str().unwrap()]).unwrap();
    assert!(built.contains(" generation "), "{built}");
    let audited = run(&[
        "data",
        "audit",
        "--config",
        features.to_str().unwrap(),
        "--manifest",
        &dataset_uri,
    ])
    .unwrap();
    assert_eq!(field(&audited, "generation"), pocket_stream);
    assert!(audited.contains("(already published)"), "{audited}");
    assert!(
        run(&["data", "verify", "--manifest", &stream_uri])
            .unwrap()
            .starts_with("verified")
    );
}

// ----------------------------------------------------------------------------------------------
// Gate 3
// ----------------------------------------------------------------------------------------------

#[test]
fn pipeline_recovery() {
    let f = fixture("pipeline_recovery");
    let store = f.scratch.path("producer/store");
    let state = f.scratch.path("producer/pipeline_state");
    // One-page budgets with a fragile transport: every interruption is a real process exit.
    fs::write(
        f.scratch.path("pocket.toml"),
        pocket_core(&f.pocket.url, "demo", BAR_GRANULARITY, 60, 1, 60),
    )
    .unwrap();
    let pocket_only = f.scratch.path("pocket-only.toml");
    fs::write(
        &pocket_only,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("pocket", "pocket.toml")],
            None,
            1,
        ),
    )
    .unwrap();

    // Interrupted mid-upload during bootstrap: the next run resumes from the acknowledged
    // offset under the same file identity.
    f.drive.set(DriveFaults {
        drop_upload_at_chunk: Some(1),
        ..Default::default()
    });
    let failed = pipeline("bootstrap", &pocket_only, &[]).unwrap_err();
    assert!(failed.contains("drive upload"), "{failed}");
    let transfers = read_json(&state.join("pocket/transfers.json"));
    let open_session = transfers["files"]
        .as_object()
        .unwrap()
        .values()
        .find(|entry| entry["session"].is_string())
        .cloned();
    assert!(open_session.is_some(), "{transfers}");
    let log_before = f.drive.log().len();
    let bootstrap = pipeline("bootstrap", &pocket_only, &[]).unwrap();
    let seed = field(job_line(&bootstrap, "pocket"), "dataset").to_string();
    let resumed = f.drive.log()[log_before..]
        .iter()
        .find(|line| line.starts_with("PUT /upload/session/") && line.contains("bytes */"))
        .cloned();
    assert!(
        resumed.is_some(),
        "status query before resuming: {:?}",
        f.drive.log()
    );
    let files = f.drive.files();
    let ids: Vec<&String> = files.keys().collect();
    assert_eq!(
        ids.len(),
        files
            .values()
            .map(|entry| &entry.name)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        "no duplicate remote files"
    );

    // Repeated one-page invocations resume backward from the same cutoff and durable cursor
    // until the seed overlap is reached, archiving partial snapshots without closing the intent.
    let cutoff = POCKET_SEED_END + 362;
    let end = time_text(cutoff * 1_000_000);
    let mut statuses = Vec::new();
    let mut generations = Vec::new();
    for _ in 0..6 {
        match pipeline("update", &pocket_only, &["--end", &end]) {
            Ok(report) => {
                let line = job_line(&report, "pocket");
                statuses.push(field(line, "status").to_string());
                generations.push(field(line, "dataset").to_string());
                break;
            }
            Err(report) => {
                let line = job_line(&report, "pocket");
                statuses.push(field(line, "status").to_string());
                generations.push(field(line, "dataset").to_string());
                let pending = read_json(&state.join("pocket/progress.json"));
                assert_eq!(pending["progress"]["cutoff"], json!(end));
                assert_eq!(pending["progress"]["baseline"], json!(seed));
            }
        }
    }
    assert_eq!(
        statuses.last().map(String::as_str),
        Some("archived"),
        "{statuses:?}"
    );
    assert!(
        statuses
            .iter()
            .filter(|status| *status == "pending")
            .count()
            >= 2,
        "{statuses:?}"
    );
    assert!(!state.join("pocket/progress.json").exists());
    let final_generation = generations.last().unwrap();
    assert_eq!(
        bars(&store, &dataset(&store, final_generation)),
        expected_bars(POCKET_SEED_END, POCKET_SEED_END - 60, cutoff)
    );
    let requests: Vec<Value> = f
        .pocket
        .requests()
        .iter()
        .map(|request| serde_json::from_str(request).unwrap())
        .collect();
    let anchors: Vec<i64> = requests
        .iter()
        .map(|request| request["time"].as_f64().unwrap() as i64 - POCKET_OFFSET_S)
        .collect();
    assert_eq!(anchors[0], cutoff);
    assert!(
        anchors.windows(2).all(|pair| pair[1] < pair[0]),
        "{anchors:?}"
    );
    let receipts: Vec<PathBuf> = fs::read_dir(state.join("records"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("pocket-receipt-")
        })
        .collect();
    assert!(receipts.len() >= statuses.len(), "{receipts:?}");
    // The same cutoff again: byte-identical pages, new request receipts, the dataset reused.
    let repeat = pipeline("update", &pocket_only, &["--end", &end]).unwrap();
    assert_eq!(
        field(job_line(&repeat, "pocket"), "dataset"),
        final_generation
    );
    assert_eq!(field(job_line(&repeat, "pocket"), "status"), "archived");
    let newest = fs::read_dir(state.join("records"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("pocket-receipt-")
        })
        .max_by_key(|path| fs::metadata(path).unwrap().modified().unwrap())
        .unwrap();
    let receipt = read_json(&newest);
    assert_eq!(receipt["dataset_generation"], json!(final_generation));
    assert!(
        !receipt["requests"].as_array().unwrap().is_empty(),
        "{receipt}"
    );

    // Expired session, missing remote checksum, unrelated same-name file, and completion whose
    // reply was lost: with retries allowed again, a fresh archive of a new generation survives
    // each once within one run.
    let cutoff_2 = cutoff + 300;
    let end_2 = time_text(cutoff_2 * 1_000_000);
    fs::write(
        f.scratch.path("pocket.toml"),
        pocket_core(&f.pocket.url, "demo", BAR_GRANULARITY, 60, 50, 60),
    )
    .unwrap();
    fs::write(
        &pocket_only,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("pocket", "pocket.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    f.drive.state.lock().unwrap().files.insert(
        "unrelated-id".into(),
        RemoteEntry {
            name: format!("object-{}", "0".repeat(64)),
            bytes: b"unrelated".to_vec(),
            trashed: false,
        },
    );
    f.drive.set(DriveFaults {
        expire_session_at_chunk: Some(1),
        omit_sha256: true,
        complete_without_reply: true,
        ..Default::default()
    });
    let recovered = pipeline("update", &pocket_only, &["--end", &end_2]).unwrap();
    let generation_2 = field(job_line(&recovered, "pocket"), "dataset").to_string();
    assert_ne!(generation_2, *final_generation);
    assert!(
        f.drive
            .log()
            .iter()
            .any(|line| line.contains("alt=media")
                || line.starts_with("GET /drive/v3/files/fixture-id")),
        "readback after a missing checksum: {:?}",
        f.drive.log()
    );
    f.drive.set(DriveFaults::default());

    // Changed same-size remote content is a conflict that replaces nothing.
    let catalog_id = {
        let files = f.drive.files();
        files
            .iter()
            .find(|(_, entry)| {
                entry.name.starts_with("catalog-") && entry.name.contains(&generation_2[..16])
            })
            .map(|(id, _)| id.clone())
            .unwrap()
    };
    {
        let mut state = f.drive.state.lock().unwrap();
        let entry = state.files.get_mut(&catalog_id).unwrap();
        let mut bytes = entry.bytes.clone();
        bytes[0] ^= 0x01;
        entry.bytes = bytes;
    }
    let conflict = pipeline("update", &pocket_only, &["--end", &end_2]).unwrap_err();
    assert!(conflict.contains("no longer carries"), "{conflict}");
    {
        let mut state = f.drive.state.lock().unwrap();
        let entry = state.files.get_mut(&catalog_id).unwrap();
        let mut bytes = entry.bytes.clone();
        bytes[0] ^= 0x01;
        entry.bytes = bytes;
    }

    // Conflicting overlap from the provider stops publication and keeps the prior generation.
    f.pocket.set(BrokerFaults {
        conflict_before: Some(cutoff_2),
        ..Default::default()
    });
    let conflicting = pipeline(
        "update",
        &pocket_only,
        &["--end", &time_text((cutoff_2 + 300) * 1_000_000)],
    )
    .unwrap_err();
    assert!(
        conflicting.contains("conflicting or inconsistent reread"),
        "{conflicting}"
    );
    f.pocket.set(BrokerFaults::default());
    assert!(
        store
            .join(format!("manifests/{generation_2}/ready.json"))
            .is_file()
    );

    // Restore interrupted mid-download resumes by range; a corrupt partial is refetched; a
    // missing dependency installs no manifest.
    let listing = pipeline(
        "list",
        &pocket_only,
        &["--broker", "pocket_option", "--symbol", "AEDCNY_otc"],
    )
    .unwrap();
    let line = listing
        .lines()
        .find(|line| line.contains(&format!(" dataset {generation_2} ")))
        .unwrap();
    let (catalog, sha) = (
        field(line, "catalog").to_string(),
        field(line, "sha256").to_string(),
    );
    let consumer = f.scratch.path("consumer.toml");
    let consumer_root = f.scratch.path("consumer");
    fs::write(
        &consumer,
        pipeline_toml(&consumer_root, &f.drive.base, &[], None, 1),
    )
    .unwrap();
    let restore_args = [
        "--catalog",
        catalog.as_str(),
        "--sha256",
        sha.as_str(),
        "--broker",
        "pocket_option",
        "--symbol",
        "AEDCNY_otc",
    ];
    f.drive.set(DriveFaults {
        drop_download_at: Some(1_000),
        ..Default::default()
    });
    let interrupted = pipeline("restore", &consumer, &restore_args).unwrap_err();
    assert!(interrupted.contains("drive files.get"), "{interrupted}");
    let downloads = consumer_root.join("pipeline_state/downloads");
    let partial = fs::read_dir(&downloads)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "partial")
        })
        .expect("a partial file survives");
    let partial_len = fs::metadata(&partial).unwrap().len();
    assert!(partial_len > 0);
    let mut corrupt = fs::read(&partial).unwrap();
    corrupt[0] ^= 0xff;
    fs::write(&partial, &corrupt).unwrap();
    f.drive.set(DriveFaults::default());
    let log_before = f.drive.log().len();
    let restored = pipeline("restore", &consumer, &restore_args).unwrap();
    assert!(
        f.drive.log()[log_before..]
            .iter()
            .any(|line| line.contains(&format!("bytes={partial_len}-"))),
        "range resume: {:?}",
        &f.drive.log()[log_before..]
    );
    assert!(
        restored.starts_with("restored pocket_option:AEDCNY_otc development"),
        "{restored}"
    );
    let object_ids: Vec<String> = f
        .drive
        .files()
        .iter()
        .filter(|(_, entry)| entry.name.starts_with("object-"))
        .map(|(id, _)| id.clone())
        .collect();
    f.drive.state.lock().unwrap().files.remove(&object_ids[0]);
    let other_root = f.scratch.path("consumer-2");
    let other = f.scratch.path("consumer-2.toml");
    fs::write(
        &other,
        pipeline_toml(&other_root, &f.drive.base, &[], None, 1),
    )
    .unwrap();
    let missing = pipeline("restore", &other, &restore_args).unwrap_err();
    assert!(
        missing.contains("missing") || missing.contains("status 404"),
        "{missing}"
    );
    assert!(!other_root.join("store/manifests").exists());
}

// ----------------------------------------------------------------------------------------------
// Gate 4
// ----------------------------------------------------------------------------------------------

#[test]
fn pipeline_scope() {
    let f = fixture("pipeline_scope");
    let producer = f.scratch.path("producer");
    let write_pocket = |text: String| fs::write(f.scratch.path("pocket.toml"), text).unwrap();
    let pocket_only = f.scratch.path("pocket-only.toml");
    fs::write(
        &pocket_only,
        pipeline_toml(
            &producer,
            &f.drive.base,
            &[("pocket", "pocket.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    let deriv_only = f.scratch.path("deriv-only.toml");
    fs::write(
        &deriv_only,
        pipeline_toml(
            &producer,
            &f.drive.base,
            &[("deriv", "deriv.toml")],
            None,
            3,
        ),
    )
    .unwrap();

    // Wrong native kind: a Pocket tick job and a Deriv bar job are refused before any
    // credential or connection, as are invalid selections.
    write_pocket(pocket_core(
        &f.pocket.url,
        "demo",
        r#"{ kind = "tick" }"#,
        60,
        50,
        60,
    ));
    let refused = pipeline("bootstrap", &pocket_only, &[]).unwrap_err();
    assert!(
        refused.contains("native_granularity must be 5-second bar"),
        "{refused}"
    );
    fs::write(
        f.scratch.path("deriv.toml"),
        deriv_core(&f.deriv.url, 60, 50, 60).replace(
            r#"native_granularity = { kind = "tick" }"#,
            &format!("native_granularity = {BAR_GRANULARITY}"),
        ),
    )
    .unwrap();
    let refused = pipeline("bootstrap", &deriv_only, &[]).unwrap_err();
    assert!(
        refused.contains("native_granularity must be tick")
            || refused.contains("must name a declared"),
        "{refused}"
    );
    fs::write(
        f.scratch.path("deriv.toml"),
        deriv_core(&f.deriv.url, 60, 50, 60),
    )
    .unwrap();
    for (selection, expected) in [
        (r#"instruments = []"#, "at least one asset name"),
        (
            r#"instruments = ["AEDCNY_otc", "AEDCNY_otc"]"#,
            "listed twice",
        ),
        (r#"instruments = ["UNKNOWN_otc"]"#, "lists no asset"),
    ] {
        write_pocket(
            pocket_core(&f.pocket.url, "demo", BAR_GRANULARITY, 60, 50, 60).replacen(
                r#"instruments = ["AEDCNY_otc"]"#,
                selection,
                1,
            ),
        );
        let refused = pipeline("bootstrap", &pocket_only, &[]).unwrap_err();
        assert!(refused.contains(expected), "{selection}: {refused}");
    }
    assert!(f.pocket.requests().is_empty() && f.deriv.requests().is_empty());
    assert!(!producer.join("raw_sources/pocket/EXCLUDED_otc").exists());

    // A real bootstrap binds the demo source context; a demo/real switch, an unknown seed
    // binding, and range narrowing are all refused before a connection.
    write_pocket(pocket_core(
        &f.pocket.url,
        "demo",
        BAR_GRANULARITY,
        60,
        50,
        60,
    ));
    let bootstrap = pipeline("bootstrap", &pocket_only, &[]).unwrap();
    let seed = field(job_line(&bootstrap, "pocket"), "dataset").to_string();
    let requests = f.pocket.requests().len();
    write_pocket(pocket_core(
        &f.pocket.url,
        "real",
        BAR_GRANULARITY,
        60,
        50,
        60,
    ));
    let refused = pipeline(
        "update",
        &pocket_only,
        &["--end", &time_text((POCKET_SEED_END + 300) * 1_000_000)],
    )
    .unwrap_err();
    assert!(refused.contains("source identity"), "{refused}");
    write_pocket(pocket_core(
        &f.pocket.url,
        "demo",
        BAR_GRANULARITY,
        60,
        50,
        60,
    ));
    let receipt_path = producer.join("pipeline_state/pocket/bootstrap.json");
    let receipt = fs::read_to_string(&receipt_path).unwrap();
    fs::write(&receipt_path, receipt.replace(&seed[..8], "00000000")).unwrap();
    let refused = pipeline(
        "update",
        &pocket_only,
        &["--end", &time_text((POCKET_SEED_END + 300) * 1_000_000)],
    )
    .unwrap_err();
    assert!(
        refused.contains("seed") || refused.contains("manifest"),
        "{refused}"
    );
    fs::write(&receipt_path, &receipt).unwrap();
    let refused = pipeline(
        "update",
        &pocket_only,
        &["--end", &time_text((POCKET_SEED_END - 100) * 1_000_000)],
    )
    .unwrap_err();
    assert!(
        refused.contains("narrows the retained lineage"),
        "{refused}"
    );
    write_pocket(
        pocket_core(&f.pocket.url, "demo", BAR_GRANULARITY, 60, 50, 60).replace(
            "start = \"2025-05-19T11:15:00Z\"",
            "start = \"2025-05-19T11:20:00Z\"",
        ),
    );
    let refused = pipeline(
        "update",
        &pocket_only,
        &["--end", &time_text((POCKET_SEED_END + 300) * 1_000_000)],
    )
    .unwrap_err();
    assert!(
        refused.contains("narrows the retained lineage"),
        "{refused}"
    );
    write_pocket(pocket_core(
        &f.pocket.url,
        "demo",
        BAR_GRANULARITY,
        60,
        50,
        60,
    ));
    assert_eq!(
        f.pocket.requests().len(),
        requests,
        "no connection was opened"
    );
    assert!(seed_unchanged(&producer.join("store"), &seed));

    // Provider response mismatches fail explicitly without publishing.
    let end = time_text((POCKET_SEED_END + 300) * 1_000_000);
    for (faults, expected) in [
        (
            BrokerFaults {
                wrong_asset: true,
                ..Default::default()
            },
            "asset or index mismatch",
        ),
        (
            BrokerFaults {
                wrong_index: true,
                ..Default::default()
            },
            "asset or index mismatch",
        ),
        (
            BrokerFaults {
                wrong_period: true,
                ..Default::default()
            },
            "unsupported period 60",
        ),
        (
            BrokerFaults {
                off_second: true,
                ..Default::default()
            },
            "not a whole second",
        ),
    ] {
        f.pocket.set(faults);
        let refused = pipeline("update", &pocket_only, &["--end", &end]).unwrap_err();
        assert!(refused.contains(expected), "{refused}");
        assert!(
            seed_unchanged(&producer.join("store"), &seed),
            "nothing published"
        );
    }
    // The envelope mismatches were refused before retention; the malformed rows of the matched
    // page stay retained as a diagnostic object that no manifest names.
    assert!(
        !producer
            .join("pipeline_state/pocket/progress.json")
            .exists()
            || read_json(&producer.join("pipeline_state/pocket/progress.json"))["progress"]["pages"]
                == json!([])
    );
    f.pocket.set(BrokerFaults::default());
    assert!(f.pocket.forbidden().is_empty());

    // A finite total deadline holds even when the provider interleaves unrelated frames and
    // answers slowly: the acquisition stays pending instead of running on.
    write_pocket(pocket_core(
        &f.pocket.url,
        "demo",
        BAR_GRANULARITY,
        60,
        50,
        1,
    ));
    f.pocket.set(BrokerFaults {
        delay_ms: 1_500,
        unrelated_frames: 50,
        ..Default::default()
    });
    let started = std::time::Instant::now();
    let pending = pipeline("update", &pocket_only, &["--end", &end]).unwrap_err();
    assert_eq!(
        field(job_line(&pending, "pocket"), "status"),
        "pending",
        "{pending}"
    );
    assert!(started.elapsed() < Duration::from_secs(20));
    f.pocket.set(BrokerFaults::default());
    write_pocket(pocket_core(
        &f.pocket.url,
        "demo",
        BAR_GRANULARITY,
        60,
        50,
        60,
    ));
    let done = pipeline("update", &pocket_only, &["--end", &end]).unwrap();
    let generation = field(job_line(&done, "pocket"), "dataset").to_string();

    // Catalog pins: wrong hash, protected role, and targets outside the closure are refused
    // before anything is fetched or installed.
    let listing = pipeline(
        "list",
        &pocket_only,
        &["--broker", "pocket_option", "--symbol", "AEDCNY_otc"],
    )
    .unwrap();
    let line = listing
        .lines()
        .find(|line| line.contains(&format!(" dataset {generation} ")))
        .unwrap();
    let (catalog, sha) = (
        field(line, "catalog").to_string(),
        field(line, "sha256").to_string(),
    );
    let consumer_root = f.scratch.path("consumer");
    let consumer = f.scratch.path("consumer.toml");
    fs::write(
        &consumer,
        pipeline_toml(&consumer_root, &f.drive.base, &[], None, 3),
    )
    .unwrap();
    let wrong_hash = "0".repeat(64);
    let refused = pipeline(
        "restore",
        &consumer,
        &[
            "--catalog",
            &catalog,
            "--sha256",
            &wrong_hash,
            "--broker",
            "pocket_option",
            "--symbol",
            "AEDCNY_otc",
        ],
    )
    .unwrap_err();
    assert!(refused.contains("SHA-256"), "{refused}");
    assert!(!consumer_root.join("store/manifests").exists());
    let refused = pipeline(
        "restore",
        &consumer,
        &[
            "--catalog",
            &catalog,
            "--sha256",
            &sha,
            "--broker",
            "deriv",
            "--symbol",
            "frxEURUSD",
        ],
    )
    .unwrap_err();
    assert!(
        refused.contains("describes pocket_option:AEDCNY_otc"),
        "{refused}"
    );
    let original: Catalog = {
        let files = f.drive.files();
        Catalog::from_json(&files[&catalog].bytes).unwrap()
    };
    let tampered = |mutate: &dyn Fn(&mut Catalog)| -> (String, String) {
        let mut catalog = original.clone();
        mutate(&mut catalog);
        let mut bytes = serde_json::to_vec_pretty(&catalog).unwrap();
        bytes.push(b'\n');
        let sha = binary_alpha_engine::hex(&Sha256::digest(&bytes));
        let id = format!("tampered-{}", &sha[..8]);
        f.drive.state.lock().unwrap().files.insert(
            id.clone(),
            RemoteEntry {
                name: "catalog-tampered.json".into(),
                bytes,
                trashed: false,
            },
        );
        (id, sha)
    };
    let log_before = f.drive.log().len();
    let (id, sha_outside) = tampered(&|catalog| {
        let first = catalog.objects[0].key.clone();
        catalog.objects.retain(|object| object.key != first);
    });
    let refused = pipeline(
        "restore",
        &consumer,
        &[
            "--catalog",
            &id,
            "--sha256",
            &sha_outside,
            "--broker",
            "pocket_option",
            "--symbol",
            "AEDCNY_otc",
        ],
    )
    .unwrap_err();
    assert!(
        refused.contains("outside the pinned catalog closure"),
        "{refused}"
    );
    assert!(
        !f.drive.log()[log_before..]
            .iter()
            .any(|line| line.contains("fixture-id") && line.contains("alt=media")),
        "no object fetched: {:?}",
        &f.drive.log()[log_before..]
    );
    let (id, sha_protected) =
        tampered(&|catalog| catalog.role = binary_alpha_engine::dataset::DatasetRole::Holdout);
    let refused = pipeline(
        "restore",
        &consumer,
        &[
            "--catalog",
            &id,
            "--sha256",
            &sha_protected,
            "--broker",
            "pocket_option",
            "--symbol",
            "AEDCNY_otc",
        ],
    )
    .unwrap_err();
    assert!(
        refused.contains("ordinary generation") || refused.contains("holdout"),
        "{refused}"
    );
    assert!(!consumer_root.join("store/manifests").exists());

    // A supplied declaration denies an undeclared generation before its manifest is read, and
    // declares a holdout population that no catalog can open.
    let declaration = f.scratch.path("declaration.json");
    fs::write(
        &declaration,
        json!({
            "schema_version": 1,
            "operator": "fixture",
            "root": format!("file://{}", consumer_root.join("store").display()),
            "namespace": "fixture",
            "populations": [{
                "id": "protected",
                "role": "holdout",
                "instrument": "pocket_option:AEDCNY_otc",
                "source": "synthetic",
                "coverage": { "first_event_time": "2025-05-19T11:15:00.000000Z", "last_event_time": "2025-05-19T12:00:00.000000Z" },
                "generations": [original.dataset.generation.clone()],
                "tokens": ["fixture"]
            }]
        })
        .to_string(),
    )
    .unwrap();
    let governed = f.scratch.path("governed.toml");
    fs::write(
        &governed,
        pipeline_toml(
            &consumer_root,
            &f.drive.base,
            &[],
            Some(&format!("file://{}", declaration.display())),
            3,
        ),
    )
    .unwrap();
    let log_before = f.drive.log().len();
    let refused = pipeline(
        "restore",
        &governed,
        &[
            "--catalog",
            &catalog,
            "--sha256",
            &sha,
            "--broker",
            "pocket_option",
            "--symbol",
            "AEDCNY_otc",
        ],
    )
    .unwrap_err();
    assert!(refused.contains("holdout data is protected"), "{refused}");
    assert!(!f.drive.log()[log_before..].iter().any(|line| line.contains("fixture-id") && !line.contains(&format!("files/{catalog} "))), "denied before any target read: {:?}", &f.drive.log()[log_before..]);
    let (id, sha_undeclared) = tampered(&|_| {});
    let refused = pipeline(
        "restore",
        &governed,
        &[
            "--catalog",
            &id,
            "--sha256",
            &sha_undeclared,
            "--broker",
            "pocket_option",
            "--symbol",
            "AEDCNY_otc",
        ],
    )
    .unwrap_err();
    assert!(refused.contains("holdout data is protected"), "{refused}");
    let other_declaration = f.scratch.path("other-declaration.json");
    let mut text: Value = read_json(&declaration);
    text["populations"][0]["role"] = json!("development");
    text["populations"][0]["generations"] = json!(["a".repeat(64)]);
    fs::write(&other_declaration, text.to_string()).unwrap();
    fs::write(
        &governed,
        pipeline_toml(
            &consumer_root,
            &f.drive.base,
            &[],
            Some(&format!("file://{}", other_declaration.display())),
            3,
        ),
    )
    .unwrap();
    let refused = pipeline(
        "restore",
        &governed,
        &[
            "--catalog",
            &catalog,
            "--sha256",
            &sha,
            "--broker",
            "pocket_option",
            "--symbol",
            "AEDCNY_otc",
        ],
    )
    .unwrap_err();
    assert!(refused.contains("not declared"), "{refused}");
    assert!(!consumer_root.join("store/manifests").exists());
    // Without a declaration the ordinary consumer restores the same catalog.
    pipeline(
        "restore",
        &consumer,
        &[
            "--catalog",
            &catalog,
            "--sha256",
            &sha,
            "--broker",
            "pocket_option",
            "--symbol",
            "AEDCNY_otc",
        ],
    )
    .unwrap();
}

fn seed_unchanged(store: &Path, generation: &str) -> bool {
    let manifests = fs::read_dir(store.join("manifests")).unwrap().count();
    store
        .join(format!("manifests/{generation}/ready.json"))
        .is_file()
        && manifests == 2
}

// ----------------------------------------------------------------------------------------------
// Gate 5
// ----------------------------------------------------------------------------------------------

#[test]
fn pipeline_schedule() {
    // The in-process updater resolves the synthetic Pocket credential from this process.
    // SAFETY: set once before any other thread of this test reads the environment.
    unsafe { std::env::set_var("PIPELINE_SYNTHETIC_AUTH", "{\"synthetic\":true}") };
    let f = fixture("pipeline_schedule");
    let producer = f.scratch.path("producer");
    fs::write(
        f.scratch.path("pocket.toml"),
        pocket_core(&f.pocket.url, "demo", BAR_GRANULARITY, 60, 1, 60),
    )
    .unwrap();
    pipeline("bootstrap", &f.pipeline, &[]).unwrap();

    // One updater invocation under an injected clock pins its cutoff at that clock; an
    // interrupted acquisition keeps the cutoff on resume; a later invocation advances the
    // cutoff only after the intent closed.
    let cutoff = POCKET_SEED_END + 362;
    let mut clock = FakeClock::at(cutoff * 1_000_000);
    let pocket_only = f.scratch.path("pocket-only.toml");
    fs::write(
        &pocket_only,
        pipeline_toml(
            &producer,
            &f.drive.base,
            &[("pocket", "pocket.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    let mut out = Vec::new();
    data_pipeline::update_with(&pocket_only, None, &mut clock, &mut out).unwrap_err();
    let first = String::from_utf8(out).unwrap();
    assert_eq!(
        field(job_line(&first, "pocket"), "status"),
        "pending",
        "{first}"
    );
    assert_eq!(
        field(job_line(&first, "pocket"), "cutoff"),
        time_text(cutoff * 1_000_000)
    );
    binary_alpha_app::broker::Clock::sleep(&mut clock, 600 * 1_000_000);
    let mut out = Vec::new();
    data_pipeline::update_with(&pocket_only, None, &mut clock, &mut out).unwrap_err();
    let resumed = String::from_utf8(out).unwrap();
    assert_eq!(
        field(job_line(&resumed, "pocket"), "cutoff"),
        time_text(cutoff * 1_000_000),
        "{resumed}"
    );
    let mut out = Vec::new();
    data_pipeline::update_with(
        &pocket_only,
        Some(&time_text((cutoff + 5) * 1_000_000)),
        &mut clock,
        &mut out,
    )
    .unwrap_err();
    let conflict = String::from_utf8(out).unwrap();
    assert!(conflict.contains("conflicts"), "{conflict}");
    let mut statuses = vec![];
    for _ in 0..8 {
        let mut out = Vec::new();
        let result = data_pipeline::update_with(&pocket_only, None, &mut clock, &mut out);
        let report = String::from_utf8(out).unwrap();
        statuses.push(field(job_line(&report, "pocket"), "status").to_string());
        if result.is_ok() {
            break;
        }
    }
    assert_eq!(
        statuses.last().map(String::as_str),
        Some("archived"),
        "{statuses:?}"
    );
    // With the intent closed, the next invocation pins the advanced clock as its cutoff and,
    // under an unbounded page budget, closes in one run.
    fs::write(
        f.scratch.path("pocket.toml"),
        pocket_core(&f.pocket.url, "demo", BAR_GRANULARITY, 60, 50, 60),
    )
    .unwrap();
    let mut out = Vec::new();
    let result = data_pipeline::update_with(&pocket_only, None, &mut clock, &mut out);
    let advanced = String::from_utf8(out).unwrap();
    result.unwrap_or_else(|error| panic!("{error}\n{advanced}"));
    assert_eq!(
        field(job_line(&advanced, "pocket"), "cutoff"),
        time_text((cutoff + 600) * 1_000_000),
        "{advanced}"
    );
    assert_eq!(
        field(job_line(&advanced, "pocket"), "status"),
        "archived",
        "{advanced}"
    );

    // Partial per-source success: the Deriv provider is gone, Pocket still archives, and the
    // command fails naming the failed job.
    binary_alpha_app::broker::Clock::sleep(&mut clock, 600 * 1_000_000);
    drop(f.deriv);
    let mut out = Vec::new();
    let partial = data_pipeline::update_with(&f.pipeline, None, &mut clock, &mut out).unwrap_err();
    assert!(partial.contains("1 job(s) failed: deriv"), "{partial}");
    let report = String::from_utf8(out).unwrap();
    assert!(report.contains("pipeline job deriv failed"), "{report}");
    assert!(report.contains("pipeline update pocket "), "{report}");

    // Local writer-lock contention: a second producer is refused while the lock is held.
    let lock = File::create(producer.join("pipeline_state/writer.lock")).unwrap();
    lock.try_lock().unwrap();
    let held =
        data_pipeline::update_with(&pocket_only, None, &mut clock, &mut Vec::new()).unwrap_err();
    assert!(held.contains("another producer holds"), "{held}");
    drop(lock);

    // The shipped units declare the specified calendar and persistence, and systemd validates
    // them with fixture paths substituted.
    let ops = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../ops/systemd");
    let timer = fs::read_to_string(ops.join("binary-alpha-data-backfill@.timer")).unwrap();
    assert!(timer.contains("OnCalendar=Sat *-*-* 06:00:00 America/Chicago"));
    assert!(timer.contains("Persistent=true"));
    assert!(timer.contains("AccuracySec=1s"));
    assert!(timer.contains("WantedBy=timers.target"));
    let service = fs::read_to_string(ops.join("binary-alpha-data-backfill@.service")).unwrap();
    assert!(
        service.contains("Type=oneshot")
            && service.contains("User=%i")
            && service.contains("UMask=0077")
    );
    assert!(service.contains("ExecStart=/usr/local/bin/binary-alpha data pipeline update --config /etc/binary-alpha/data-pipeline/%i.toml"));
    assert!(!service.contains("RemainAfterExit") && !service.contains("Restart="));
    let units = f.scratch.path("units");
    fs::create_dir_all(&units).unwrap();
    fs::write(
        units.join("binary-alpha-data-backfill@.service"),
        service.replace(
            "/usr/local/bin/binary-alpha",
            env!("CARGO_BIN_EXE_binary-alpha"),
        ),
    )
    .unwrap();
    fs::write(units.join("binary-alpha-data-backfill@.timer"), &timer).unwrap();
    let verified = Command::new("systemd-analyze")
        .current_dir(&units)
        .args([
            "verify",
            "--man=no",
            "binary-alpha-data-backfill@.service",
            "binary-alpha-data-backfill@.timer",
        ])
        .output()
        .expect("systemd-analyze is installed");
    assert!(
        verified.status.success(),
        "{}",
        String::from_utf8_lossy(&verified.stderr)
    );
    let calendar = Command::new("systemd-analyze")
        .args(["calendar", "Sat *-*-* 06:00:00 America/Chicago"])
        .output()
        .unwrap();
    assert!(calendar.status.success());
    assert!(
        String::from_utf8_lossy(&calendar.stdout)
            .contains("Normalized form: Sat *-*-* 06:00:00 America/Chicago")
    );
}
