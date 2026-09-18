//! Goal-bearing proof of `binary-alpha data pipeline` over synthetic sources, loopback Deriv
//! and Pocket Option brokers, and a loopback Drive: selected imports, seeded backfill, private
//! archive, exact restore, crash and conflict recovery, scope denial, and schedule semantics.
//! Every broker frame, archive byte, and Drive response here is synthetic.

mod common;
#[path = "data_pipeline/lineage.rs"]
mod lineage;
#[path = "data_pipeline/new_instrument.rs"]
mod new_instrument;

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use binary_alpha_app::broker::socket_io;
use binary_alpha_app::data_pipeline::{self, Catalog};
use binary_alpha_app::fetch::HistoryCoverage;
use binary_alpha_app::verify;
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
const POCKET_HISTORY_PAGES_IN_FLIGHT: usize = 8;

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
    /// Keep an interrupted acquisition pending by rejecting its attempted reconnect.
    reject_auth_after_drop: bool,
    unrelated_frames: usize,
    wrong_asset: bool,
    wrong_index: bool,
    wrong_period: bool,
    off_second: bool,
    /// Reject the next authentication as a stale session, once.
    reject_auth_once: bool,
    /// Reject the synthetic environment session until the fixture renewal command replaces it.
    reject_initial_session: bool,
    /// Exact decimal prices for discovery/scale regression cases (all four OHLC fields).
    pocket_price: Option<String>,
}

struct FakeBroker {
    url: String,
    requests: Arc<Mutex<Vec<String>>>,
    forbidden: Arc<Mutex<Vec<String>>>,
    faults: Arc<Mutex<BrokerFaults>>,
    rejected_auths: Arc<AtomicUsize>,
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
    let rejected_auths = Arc::new(AtomicUsize::new(0));
    let rejected_auths_ = Arc::clone(&rejected_auths);
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
                            if fields.contains_key("active_symbols") {
                                replies.push(Message::Text(json!({
                                    "msg_type":"active_symbols", "req_id":req_id,
                                    "active_symbols":[{
                                        "underlying_symbol":"frxEURUSD",
                                        "underlying_symbol_name":"Synthetic EUR/USD",
                                        "pip_size":0.00001, "exchange_is_open":1,
                                        "is_trading_suspended":0
                                    }]
                                }).to_string().into()));
                            } else if fields.contains_key("ticks_history") {
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
                                "auth" if faults.reject_auth_once || (faults.reject_initial_session
                                    && serde_json::from_slice::<Value>(&argument).unwrap()["renewed"] != true) => {
                                    faults_.lock().unwrap().reject_auth_once = false;
                                    rejected_auths_.fetch_add(1, Ordering::SeqCst);
                                    replies.push(Message::Text(r#"42["error","synthetic session rejected"]"#.into()));
                                }
                                "auth" => {
                                    authenticated = true;
                                    replies.push(Message::Text(r#"42["successauth",{"synthetic":true}]"#.into()));
                                    replies.push(Message::Text(
                                        r#"42["successupdateBalance",{"isDemo":1,"synthetic":true}]"#.into(),
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
                                        assert_eq!(request["offset"], 200);
                                        // The real window contains 40 starts, ending at the
                                        // anchor inclusive. Off-grid cutoffs include the bar
                                        // containing the cutoff; adjacent pages overlap by one bar.
                                        let last = anchor.div_euclid(5) * 5;
                                        let first = (last - 195).max(*from);
                                        let rows: Vec<Value> = (first..=last)
                                            .step_by(5)
                                            .filter(|start| *start < *to)
                                            .map(|start| {
                                                let [open, high, low, close, volume] = synthetic_bar(start);
                                                let shift = if faults.conflict_before.is_some_and(|before| start < before) { 0.0001 } else { 0.0 };
                                                let mut time = (start + POCKET_OFFSET_S).to_string();
                                                if faults.off_second {
                                                    time.push_str(".5");
                                                }
                                                let price = |value| faults.pocket_price.clone().unwrap_or_else(|| price_text(value));
                                                serde_json::from_str::<Value>(&format!(
                                                    r#"{{"symbol_id":{POCKET_SYMBOL_ID},"time":{time},"open":{},"close":{},"high":{},"low":{},"volume":{}}}"#,
                                                    price(open + shift),
                                                    price(close + shift),
                                                    price(high + shift),
                                                    price(low + shift),
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
                        if faults.reject_auth_after_drop {
                            faults_.lock().unwrap().reject_auth_once = true;
                        }
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
        rejected_auths,
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
    /// Answer the next N object content uploads with 503; usize::MAX never clears.
    unavailable_uploads: usize,
    /// Answer the next N object upload/download requests with the given 403 reason.
    forbidden_uploads: Option<(&'static str, usize)>,
    forbidden_downloads: Option<(&'static str, usize)>,
    forbidden_begins: Option<(&'static str, usize)>,
    /// Permanently reject status queries of this recorded session path with a 403 reason.
    forbidden_session_status: Option<(String, &'static str)>,
    /// Reject every chunk of the named object's first N sessions; usize::MAX poisons all.
    forbidden_chunk_sessions: Option<(String, &'static str, usize)>,
    /// Hold the first upload until another job's report has been flushed.
    upload_gate: Option<Arc<Mutex<std::sync::mpsc::Receiver<()>>>>,
    /// Drop the next N object media requests before replying; usize::MAX never clears.
    /// When drop_download_at is also armed, first send that partial body.
    drop_download_requests: usize,
    /// Close the connection without answering the chunk with this ordinal (per session).
    drop_upload_at_chunk: Option<usize>,
    /// Forget the session before answering this chunk: the client sees 404.
    expire_session_at_chunk: Option<usize>,
    /// Store the final chunk, then close without a reply.
    complete_without_reply: bool,
    omit_sha256: bool,
    incomplete_listing: bool,
    reject_page_token_once: bool,
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
    forbidden_chunks: Option<&'static str>,
}

#[derive(Default)]
struct DriveState {
    files: BTreeMap<String, RemoteEntry>,
    sessions: BTreeMap<String, Session>,
    /// Directly installed historical fixtures count as already uploaded content.
    seeded_uploads: Vec<(String, Vec<u8>)>,
    next: usize,
    log: Vec<String>,
    media_requests: Vec<(String, Option<String>)>,
    faults: DriveFaults,
}

fn drive_error(reason: &str) -> Vec<u8> {
    json!({ "error": { "message": "Synthetic Drive error", "errors": [{ "reason": reason }] } })
        .to_string()
        .into_bytes()
}

#[derive(Default)]
struct RequestCount {
    in_flight: AtomicUsize,
    high_water: AtomicUsize,
}

impl RequestCount {
    fn enter(&self) -> InFlight<'_> {
        let count = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.high_water.fetch_max(count, Ordering::SeqCst);
        InFlight(self)
    }
}

struct InFlight<'a>(&'a RequestCount);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct DriveActivity {
    uploads: RequestCount,
    downloads: RequestCount,
}

struct FakeDrive {
    base: String,
    state: Arc<Mutex<DriveState>>,
    activity: Arc<DriveActivity>,
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
    let activity = Arc::new(DriveActivity::default());
    let activity_ = Arc::clone(&activity);
    let stop = Arc::new(AtomicBool::new(false));
    let (state_, stop_, base_) = (Arc::clone(&state), Arc::clone(&stop), base.clone());
    let thread = std::thread::spawn(move || {
        while !stop_.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(false).unwrap();
                    let state = Arc::clone(&state_);
                    let activity = Arc::clone(&activity_);
                    let base = base_.clone();
                    std::thread::spawn(move || handle_http(stream, &state, &activity, &base));
                }
                Err(_) => std::thread::sleep(Duration::from_millis(20)),
            }
        }
    });
    FakeDrive {
        base,
        state,
        activity,
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
    let url = reqwest::Url::parse(&format!("http://fixture{target}")).ok()?;
    let path = url.path().to_string();
    let query = url.query_pairs().into_owned().collect();
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
        path,
        query,
        headers,
        body,
    })
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

fn handle_http(
    mut stream: TcpStream,
    shared: &Arc<Mutex<DriveState>>,
    activity: &DriveActivity,
    base: &str,
) {
    let Some(request) = read_request(&mut stream) else {
        return;
    };
    let mut state = shared.lock().unwrap();
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
    if request.method == "DELETE" && request.path.starts_with("/drive/v3/files/") {
        let id = request.path.trim_start_matches("/drive/v3/files/");
        let existed = state.files.remove(id).is_some();
        respond(&mut stream, if existed { 204 } else { 404 }, &[], b"");
        return;
    }
    // The real service refuses a PUT without `Content-Length` with `411 Length Required`
    // (observed 2026-09-16 on an empty object), even when `Content-Range` says `bytes */0`.
    if request.method == "PUT"
        && request.path.starts_with("/upload/session/")
        && !request.headers.contains_key("content-length")
    {
        respond(&mut stream, 411, &[], b"Length Required");
        return;
    }
    let content_upload = request.method == "PUT"
        && request.path.starts_with("/upload/session/")
        && !request.body.is_empty();
    let media_download = request.method == "GET"
        && request.path.starts_with("/drive/v3/files/")
        && request.query.get("alt").map(String::as_str) == Some("media");
    if media_download {
        state.media_requests.push((
            request.path.trim_start_matches("/drive/v3/files/").into(),
            request.headers.get("range").cloned(),
        ));
    }
    if content_upload && let Some(gate) = state.faults.upload_gate.take() {
        drop(state);
        gate.lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(5))
            .expect("another job's report must flush before this upload completes");
        state = shared.lock().unwrap();
    }
    let forbidden = if content_upload {
        &mut state.faults.forbidden_uploads
    } else if media_download {
        &mut state.faults.forbidden_downloads
    } else if request.method == "POST" && request.path == "/upload/drive/v3/files" {
        &mut state.faults.forbidden_begins
    } else {
        &mut None
    };
    if let Some((reason, remaining)) = forbidden
        && *remaining > 0
    {
        *remaining -= 1;
        respond(&mut stream, 403, &[], &drive_error(reason));
        return;
    }
    // Immediate transient faults leave the four-second test budget for the 3.75s backoff.
    if content_upload
        && state
            .sessions
            .get(request.path.trim_start_matches("/upload/session/"))
            .is_some_and(|session| session.name.starts_with("object-"))
        && state.faults.unavailable_uploads > 0
    {
        if state.faults.unavailable_uploads != usize::MAX {
            state.faults.unavailable_uploads -= 1;
        }
        respond(&mut stream, 503, &[], b"{}");
        return;
    }
    if media_download
        && state
            .files
            .get(request.path.trim_start_matches("/drive/v3/files/"))
            .is_some_and(|entry| entry.name.starts_with("object-"))
        && state.faults.drop_download_at.is_none()
        && state.faults.drop_download_requests > 0
    {
        if state.faults.drop_download_requests != usize::MAX {
            state.faults.drop_download_requests -= 1;
        }
        return;
    }
    let in_flight = if content_upload {
        Some(activity.uploads.enter())
    } else if media_download {
        Some(activity.downloads.enter())
    } else {
        None
    };
    if in_flight.is_some() {
        // Model transfer latency outside the state lock so concurrent requests can overlap.
        drop(state);
        std::thread::sleep(Duration::from_millis(50));
        state = shared.lock().unwrap();
    }
    let faults = state.faults.clone();
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/drive/v3/files/generateIds") => {
            let count: usize = request.query["count"].parse().unwrap();
            if count > 1000 {
                respond(&mut stream, 400, &[], b"{}");
                return;
            }
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
            let forbidden_chunks = faults
                .forbidden_chunk_sessions
                .as_ref()
                .filter(|(name, _, count)| {
                    metadata["name"] == *name
                        && state.sessions.values().filter(|s| s.id == id).count() < *count
                })
                .map(|(_, reason, _)| *reason);
            state.next += 1;
            let token = format!("session-{}", state.next);
            state
                .log
                .push(format!("BEGIN {id} /upload/session/{token}"));
            state.sessions.insert(
                token.clone(),
                Session {
                    id,
                    name: metadata["name"].as_str().unwrap().to_string(),
                    total,
                    received: Vec::new(),
                    chunks: 0,
                    completed: false,
                    forbidden_chunks,
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
            if range.starts_with("bytes */")
                && let Some((session_path, reason)) = &faults.forbidden_session_status
                && session_path == path
            {
                let body = json!({ "error": {
                    "code": 403,
                    "message": "User rate limit exceeded.",
                    "errors": [{ "message": "User rate limit exceeded.",
                        "domain": "usageLimits", "reason": reason }],
                } });
                respond(&mut stream, 403, &[], body.to_string().as_bytes());
                return;
            }
            let Some(session) = state.sessions.get_mut(&token) else {
                respond(&mut stream, 404, &[], b"{}");
                return;
            };
            if session.completed {
                respond(&mut stream, 409, &[], &drive_error("fileIdInUse"));
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
            if let Some(reason) = session.forbidden_chunks {
                respond(&mut stream, 403, &[], &drive_error(reason));
                return;
            }
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
            assert!(request.query["fields"].contains("incompleteSearch"));
            if faults.reject_page_token_once && request.query.contains_key("pageToken") {
                state.faults.reject_page_token_once = false;
                respond(&mut stream, 400, &[], &drive_error("invalidPageToken"));
                return;
            }
            let page: usize = request
                .query
                .get("pageToken")
                .and_then(|token| token.parse().ok())
                .unwrap_or(0);
            let matching: Vec<(String, RemoteEntry)> = state
                .files
                .iter()
                .filter(|(_, entry)| {
                    !entry.trashed
                        && (!request.query["q"].contains("catalog-")
                            || entry.name.contains("catalog-"))
                })
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
            let mut body = json!({ "files": files, "incompleteSearch": faults.incomplete_listing });
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
                && limit < body.len()
            {
                // Empty markers and short objects cannot exercise a truncated download.
                // Keep the fault armed until a response can carry the requested prefix.
                state.faults.drop_download_at = None;
                let partial = &body[..limit];
                let head = format!(
                    "HTTP/1.1 {} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    if start > 0 { 206 } else { 200 },
                    body.len()
                );
                drop(state);
                stream.write_all(head.as_bytes()).unwrap();
                stream.write_all(partial).unwrap();
                stream.flush().unwrap();
                stream.shutdown(std::net::Shutdown::Write).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut buffer = [0; 1_024];
                while let Ok(read) = stream.read(&mut buffer) {
                    if read == 0 {
                        break;
                    }
                }
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

const DERIV_FX_SESSION: &str = r#"{ kind = "weekly", timezone = "UTC", open = { day = "monday", time = "00:00:00" }, close = { day = "friday", time = "20:55:00" } }"#;
fn synthetic_deriv(core: String) -> String {
    core.replace("frxEURUSD", "R_50")
        .replace(DERIV_FX_SESSION, r#"{ kind = "always" }"#)
}

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
session = {DERIV_FX_SESSION}
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
session = {{ kind = "always" }}
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
history_pages_in_flight = {POCKET_HISTORY_PAGES_IN_FLIGHT}

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
        "schema_version = 1\nlocal_root = \"{}\"\nparallel_transfers = 3\n",
        local_root.display()
    );
    if let Some(uri) = governance {
        text.push_str(&format!("governance_manifest = \"{uri}\"\n"));
    }
    if jobs.len() > 1 {
        text.push_str("parallel_jobs = 2\n");
    }
    text.push_str(&format!(
        "\n[drive]\nroot_folder_id = \"fixture-root\"\nchunk_bytes = 262144\nrequest_timeout_seconds = 5\nmax_attempts = {max_attempts}\nretry_seconds = 4\nloopback_endpoint = \"{drive_base}\"\n"
    ));
    for (id, config) in jobs {
        text.push_str(&format!(
            "\n[[jobs]]\nid = \"{id}\"\nconfig = \"{config}\"\nevidence = \"evidence/{id}.json\"\n"
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
}

/// The operator's source-binding evidence for one job: the identity of the broker context the
/// job's core configuration names, plus free-form notes.
fn write_evidence(scratch: &Scratch, job: &str, core: &str) {
    let config = binary_alpha_engine::config::Config::parse(core).unwrap();
    let history = config.history.as_ref().unwrap();
    let broker = config
        .brokers
        .iter()
        .find(|broker| broker.id() == &history.broker)
        .unwrap();
    fs::write(
        scratch.path(&format!("evidence/{job}.json")),
        json!({
            "source_identity": binary_alpha_app::broker::source_identity(broker),
            "statement": format!("Synthetic source binding for {job}: loopback fixture, no operator archive."),
        })
        .to_string(),
    )
    .unwrap();
}

fn import_config(scratch: &Scratch, job: &str, core: &str) -> PathBuf {
    let store = scratch.path("producer/store");
    let path = scratch.path(&format!("{job}-import.toml"));
    fs::write(
        &path,
        core.replace(
            "historical_data_dir = \"unused\"",
            &format!("historical_data_dir = \"{}\"", store.display()),
        )
        .replace(
            "publication_uri = \"file:///unused\"",
            &format!("publication_uri = \"file://{}\"", store.display()),
        ),
    )
    .unwrap();
    path
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
    import_config(&scratch, "deriv", &deriv_core(&deriv.url, 60, 50, 60));
    import_config(
        &scratch,
        "pocket",
        &pocket_core(&pocket.url, "demo", BAR_GRANULARITY, 60, 50, 60),
    );
    write_evidence(&scratch, "deriv", &deriv_core(&deriv.url, 60, 50, 60));
    write_evidence(
        &scratch,
        "pocket",
        &pocket_core(&pocket.url, "demo", BAR_GRANULARITY, 60, 50, 60),
    );
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
        .env_remove("PIPELINE_UNSET_AUTH")
        .env_remove("PIPELINE_UNSET_DRIVE_AUTH")
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

fn import(config: &Path) -> Result<String, String> {
    run(&["data", "import", "--config", config.to_str().unwrap()])
}

fn imported_generation<'a>(report: &'a str, instrument: &str) -> &'a str {
    let line = report
        .lines()
        .find(|line| line.starts_with(&format!("published {instrument} development generation ")))
        .unwrap_or_else(|| panic!("no import line for {instrument} in {report}"));
    field(line, "generation")
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
    if manifest.layout.is_some() {
        let records = store.parent().unwrap().join("pipeline_state/records");
        for entry in fs::read_dir(records).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "json") {
                let record = read_json(&path);
                if record["dataset_generation"] == manifest.generation {
                    return serde_json::from_value(record["coverage"].clone()).unwrap();
                }
            }
        }
        panic!("missing acquisition receipt for {}", manifest.generation);
    }
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
    let mut rows = Vec::new();
    binary_alpha_app::daily::read_generation(
        &binary_alpha_app::store::Store::filesystem(store),
        manifest,
        |row| {
            let binary_alpha_app::daily::MarketRow::Bar(bar) = row else {
                panic!("expected bar")
            };
            rows.push(bar);
            Ok(())
        },
    )
    .unwrap();
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

// Page overlap contributes each start once; only bars closed by the cutoff are retained.
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
                provider: (),
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

/// Read the old inline prefix followed by the append-only page log.
fn read_progress(path: &Path) -> Value {
    let mut pending = read_json(path);
    let mut pages = pending["progress"]["pages"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    if let Ok(log) = fs::read_to_string(path.with_file_name("progress.pages.jsonl")) {
        for line in log
            .split_inclusive('\n')
            .filter(|line| line.ends_with('\n'))
        {
            pages.push(serde_json::from_str(line).unwrap());
        }
    }
    pending["progress"]["pages"] = json!(pages);
    pending
}

/// Hand-build a manifest with independently authenticated coverage and object identities.
fn indexed_fixture(
    source: &Path,
    root: &Path,
    mut manifest: GenerationManifest,
    coverage: &HistoryCoverage,
) -> String {
    use binary_alpha_engine::dataset::{PriceRepresentation, generation_id, object_key};
    fs::create_dir_all(root.join("objects")).unwrap();
    for object in &manifest.objects {
        fs::copy(source.join(&object.key), root.join(&object.key)).unwrap();
    }
    let bytes = serde_json::to_vec(coverage).unwrap();
    let record = manifest
        .objects
        .iter_mut()
        .find(|object| object.path == "provenance/coverage.json")
        .unwrap();
    record.sha256 = binary_alpha_engine::hex(&Sha256::digest(&bytes));
    record.bytes = bytes.len() as u64;
    record.key = object_key(&record.sha256);
    record.crc32c = None;
    record.generation = None;
    fs::write(root.join(&record.key), bytes).unwrap();
    let scale = match manifest.price_representation {
        PriceRepresentation::IntegerUnits { scale } => Some(scale),
        _ => None,
    };
    manifest.generation = generation_id(
        &binary_alpha_engine::market::InstrumentId {
            broker: manifest.broker.clone(),
            provider_symbol: manifest.provider_symbol.clone(),
        },
        manifest.source_kind,
        manifest.role,
        scale,
        &manifest.objects,
    );
    GenerationManifest::from_json(&manifest.to_json()).unwrap();
    let path = root.join(manifest.key());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, manifest.to_json()).unwrap();
    format!("file://{}", path.display())
}

fn assert_bundle(store: &Path, manifest: &GenerationManifest, expected_pages: usize) {
    if manifest.layout.is_some() {
        let receipt = coverage(store, manifest);
        assert_eq!(receipt.pages.len(), expected_pages);
        let pages: Vec<_> = manifest
            .day_inventory
            .iter()
            .filter(|d| d.family == binary_alpha_engine::dataset::DayFamily::Pages)
            .flat_map(|d| {
                binary_alpha_app::daily::read_pages(
                    &store.join(d.object.as_ref().unwrap()),
                    &d.date,
                )
                .unwrap()
            })
            .collect();
        for p in &receipt.pages {
            let identity = p.occurrence.as_ref().unwrap();
            let actual: Vec<_> = pages
                .iter()
                .filter(|r| {
                    r.acquisition_id == identity.acquisition_id && r.ordinal == identity.ordinal
                })
                .collect();
            assert_eq!(actual.len(), 1);
            let actual = actual[0];
            assert_eq!(actual.payload_sha256, p.sha256);
            assert_eq!(actual.payload.len() as u64, p.bytes);
            assert_eq!(actual.request_token, p.anchor);
            assert_eq!(actual.rows, p.rows);
        }
        assert!(
            manifest
                .objects
                .iter()
                .filter(|o| o.role == binary_alpha_engine::dataset::ObjectRole::Source)
                .all(|o| o.path.starts_with("pages/") && o.path.ends_with(".parquet"))
        );
        return;
    }
    let source: Vec<_> = manifest
        .objects
        .iter()
        .filter(|object| object.role == binary_alpha_engine::dataset::ObjectRole::Source)
        .collect();
    assert_eq!(
        source.len(),
        1,
        "one source bundle, no per-page source objects"
    );
    assert_eq!(source[0].path, "raw/pages.bin");
    let coverage = coverage(store, manifest);
    let bundle = coverage.bundle.as_ref().unwrap();
    assert_eq!(bundle.sha256, source[0].sha256);
    assert_eq!(bundle.bytes, source[0].bytes);
    assert_eq!(coverage.pages.len(), expected_pages);
    let mut offset = 0;
    for page in &coverage.pages {
        assert_eq!(page.path, "raw/pages.bin");
        assert_eq!(page.offset, Some(offset));
        offset += page.bytes;
    }
    assert_eq!(offset, bundle.bytes);
}

/// Every `(close_time_micros, known_at_micros)` pair of one candle object.
fn candle_clocks(store: &Path, stream: &StreamManifest) -> Vec<(i64, i64)> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::record::RowAccessor;
    stream
        .objects
        .iter()
        .filter(|object| object.path != "profile.json")
        .flat_map(|object| {
            let reader =
                SerializedFileReader::new(File::open(store.join(&object.key)).unwrap()).unwrap();
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
                .collect::<Vec<_>>()
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
    let deriv_import = import(&f.scratch.path("deriv-import.toml")).unwrap();
    let pocket_import = import(&f.scratch.path("pocket-import.toml")).unwrap();
    let deriv_seed = imported_generation(&deriv_import, "deriv:frxEURUSD").to_string();
    let pocket_seed = imported_generation(&pocket_import, "pocket_option:AEDCNY_otc").to_string();
    // Only the selected assets are imported; their broken neighbours are never opened.
    assert_eq!(deriv_import.lines().count(), 1, "{deriv_import}");
    assert_eq!(pocket_import.lines().count(), 1, "{pocket_import}");
    let seed_bars = bars(&store, &dataset(&store, &pocket_seed));
    assert_eq!(
        seed_bars,
        expected_bars(POCKET_SEED_END, POCKET_SEED_END, POCKET_SEED_END)
    );
    assert_eq!(
        ticks(&store, &dataset(&store, &deriv_seed)),
        expected_ticks(DERIV_SEED_END, DERIV_SEED_END, DERIV_SEED_END)
    );
    // Repeated imports reuse identical generations in the one managed store.
    let again = import(&f.scratch.path("deriv-import.toml")).unwrap();
    assert_eq!(imported_generation(&again, "deriv:frxEURUSD"), deriv_seed);
    assert!(again.contains("(already published)"), "{again}");
    let again = import(&f.scratch.path("pocket-import.toml")).unwrap();
    assert_eq!(
        imported_generation(&again, "pocket_option:AEDCNY_otc"),
        pocket_seed
    );
    assert!(again.contains("(already published)"), "{again}");

    // An aligned cutoff exercises matched prefetch anchors. The bar starting exactly at
    // the cutoff is present on the first page but must be withheld until it closes.
    let deriv_cutoff = DERIV_SEED_END + 1_200;
    let pocket_cutoff = POCKET_SEED_END + 360;
    let first = pipeline(
        "update",
        &f.pipeline,
        &["--end", &time_text(pocket_cutoff * 1_000_000)],
    )
    .unwrap_err();
    // The pinned Pocket cutoff precedes Deriv's seed, so Deriv refuses the narrower range
    // while Pocket publishes its first descendant and catalog.
    assert!(first.contains("pipeline update pocket "), "{first}");
    assert!(
        (2..=3).contains(&f.drive.activity.uploads.high_water.load(Ordering::SeqCst)),
        "closure object uploads must overlap within the three-worker limit"
    );
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
    assert_eq!(bars_after.len(), 272);
    assert_eq!(
        bars_after.last().unwrap().start_unix_s + 5,
        pocket_cutoff,
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
    assert_eq!(pocket_coverage.pages.len(), 3, "{pocket_coverage:?}");
    for (n, page) in pocket_coverage.pages.iter().enumerate() {
        let anchor = pocket_cutoff - n as i64 * 195;
        assert_eq!(page.anchor, Some((anchor + POCKET_OFFSET_S).to_string()));
        assert_eq!(page.rows, 40);
        assert_eq!(page.first, Some(time_text((anchor - 195) * 1_000_000)));
        assert_eq!(page.last, Some(time_text(anchor * 1_000_000)));
    }
    let pocket_requests = f.pocket.requests();
    assert!(
        pocket_requests.len() <= pocket_coverage.pages.len() + POCKET_HISTORY_PAGES_IN_FLIGHT,
        "matched anchors must reuse look-ahead requests: {pocket_requests:?}"
    );
    for (n, request) in pocket_requests.iter().enumerate() {
        let request: Value = serde_json::from_str(request).unwrap();
        assert_eq!(
            request["time"].as_i64().unwrap() - POCKET_OFFSET_S,
            pocket_cutoff - n as i64 * 195
        );
    }
    let pocket_manifest = dataset(&store, &pocket_first);
    assert_bundle(&store, &pocket_manifest, 3);
    assert_archive_inventory(
        &f.drive,
        &store,
        &f.scratch.path("producer/pipeline_state/records"),
        &[field(pocket_line, "catalog")],
    );
    let root = dataset(&store, &pocket_seed);
    let lineage_object = root
        .objects
        .iter()
        .find(|o| o.path == "provenance/lineage.json")
        .unwrap();
    let imported_lineage = read_json(&store.join(&lineage_object.key));
    let collection_path = f.scratch.path("sources/pocket/collection.json");
    let collection = read_json(&collection_path);
    let entry = imported_lineage["provenance"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["object"]["path"] == "collection/collection.json")
        .unwrap();
    assert_eq!(
        entry["instrument_entry"],
        collection["assets"]["AEDCNY_otc"]
    );
    assert_eq!(entry["object"]["sha256"], common::sha256(&collection_path));
    let asset = f.scratch.path("sources/pocket/AEDCNY_otc");
    for (checkpoint, name) in [(false, "raw_pages.ndjson"), (true, "checkpoint.ndjson")] {
        assert_eq!(
            common::daily::reconstruct_import(&store, &root, checkpoint),
            fs::read(asset.join(name)).unwrap()
        );
    }
    for name in [
        "download_manifest.json",
        "dataset/manifest.json",
        "dataset/hashes.sha256",
        "dataset/reports/quality.json",
    ] {
        let entry = imported_lineage["provenance"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["object"]["path"] == name)
            .unwrap();
        assert_eq!(
            serde_json::from_value::<Vec<u8>>(entry["bytes_verbatim"].clone()).unwrap(),
            fs::read(asset.join(name)).unwrap()
        );
    }
    let lineage_object = pocket_manifest
        .objects
        .iter()
        .find(|o| o.path == "provenance/lineage.json")
        .unwrap();
    assert_eq!(
        read_json(&store.join(&lineage_object.key))["root_generation"],
        pocket_seed
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
    let deriv_requests_before = f.deriv.requests().len();
    let second = pipeline(
        "update",
        &deriv_only,
        &["--end", &time_text(deriv_cutoff * 1_000_000)],
    )
    .unwrap();
    let deriv_line = job_line(&second, "deriv");
    assert_eq!(field(deriv_line, "status"), "archived");
    assert!(
        fs::read_to_string(f.scratch.path("producer/pipeline_state/deriv/update.toml"))
            .unwrap()
            .contains("file://"),
        "publication stays on the local filesystem"
    );
    let deriv_first = field(deriv_line, "dataset").to_string();
    let deriv_stream = field(deriv_line, "stream").to_string();
    let deriv_manifest = dataset(&store, &deriv_first);
    assert_bundle(
        &store,
        &deriv_manifest,
        f.deriv.requests().len() - deriv_requests_before,
    );
    assert_archive_inventory(
        &f.drive,
        &store,
        &f.scratch.path("producer/pipeline_state/records"),
        &[field(pocket_line, "catalog"), field(deriv_line, "catalog")],
    );
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
    assert_archive_inventory(
        &f.drive,
        &store,
        &f.scratch.path("producer/pipeline_state/records"),
        &[
            field(pocket_line, "catalog"),
            field(job_line(&third, "deriv"), "catalog"),
        ],
    );
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
    assert_eq!(deriv_listing.lines().count(), 2, "{deriv_listing}");
    let expected_dataset_bytes =
        fs::read(store.join(format!("manifests/{pocket_first}/ready.json"))).unwrap();
    let expected_stream_bytes =
        fs::read(store.join(format!("manifests/{pocket_stream}/ready.json"))).unwrap();
    let pocket_objects = dataset(&store, &pocket_first).objects.clone();
    fs::rename(&store, f.scratch.path("producer/store.gone")).unwrap();
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
    let verified = run(&["data", "verify", "--manifest", &dataset_uri]).unwrap();
    assert!(
        verified.contains(&format!("generation {pocket_first} rows ")),
        "{verified}"
    );
    assert_eq!(
        dataset(&f.scratch.path("elsewhere/consumer/store"), &pocket_first).layout,
        Some(binary_alpha_engine::dataset::Layout::DailyV2)
    );
    assert!(
        (2..=3).contains(&f.drive.activity.downloads.high_water.load(Ordering::SeqCst)),
        "closure object downloads must overlap within the three-worker limit"
    );
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
session = {{ kind = "always" }}
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

    // The consumer's one step: `pull` selects the newest archived catalog of the instrument,
    // restores it into a fresh store, and a second pull reuses it without any object download.
    let listing = pipeline(
        "list",
        &f.pipeline,
        &["--broker", "pocket_option", "--symbol", "AEDCNY_otc"],
    )
    .unwrap();
    let newest = listing
        .lines()
        .max_by_key(|line| {
            line.split(" coverage ")
                .nth(1)
                .unwrap()
                .split(' ')
                .nth(1)
                .unwrap()
                .to_string()
        })
        .unwrap();
    let newest_dataset = field(newest, "dataset").to_string();
    let puller_root = f.scratch.path("puller");
    let puller = f.scratch.path("puller.toml");
    fs::write(
        &puller,
        pipeline_toml(&puller_root, &f.drive.base, &[], None, 3),
    )
    .unwrap();
    let pulled = pipeline(
        "pull",
        &puller,
        &["--broker", "pocket_option", "--symbol", "AEDCNY_otc"],
    )
    .unwrap();
    assert!(
        pulled.starts_with("restored pocket_option:AEDCNY_otc"),
        "{pulled}"
    );
    assert!(
        field(&pulled, "dataset").contains(&newest_dataset),
        "{pulled}\n{listing}"
    );
    let log_before = f.drive.log().len();
    let again = pipeline(
        "pull",
        &puller,
        &["--broker", "pocket_option", "--symbol", "AEDCNY_otc"],
    )
    .unwrap();
    assert!(again.contains("(already local)"), "{again}");
    assert!(
        !f.drive.log()[log_before..]
            .iter()
            .any(|line| line.starts_with("GET /drive/v3/files/fixture-id")
                && line.contains("alt=media")),
        "objects fetched again: {:?}",
        &f.drive.log()[log_before..]
    );
    assert!(
        run(&["data", "verify", "--manifest", field(&pulled, "dataset")])
            .unwrap()
            .starts_with("verified")
    );
}

// ----------------------------------------------------------------------------------------------
// Gate 3
// ----------------------------------------------------------------------------------------------

fn append_log_recovery() {
    let f = fixture("pipeline_append_log_recovery");
    let producer = f.scratch.path("producer");
    let store = producer.join("store");
    let state = producer.join("pipeline_state/pocket");
    let header = state.join("progress.json");
    let log = state.join("progress.pages.jsonl");
    fs::write(
        f.scratch.path("pocket.toml"),
        pocket_core(&f.pocket.url, "demo", BAR_GRANULARITY, 60, 1, 60),
    )
    .unwrap();
    let config = f.scratch.path("pocket-only.toml");
    fs::write(
        &config,
        pipeline_toml(
            &producer,
            &f.drive.base,
            &[("pocket", "pocket.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    import(&f.scratch.path("pocket-import.toml")).unwrap();
    let cutoff = POCKET_SEED_END + 362;
    let end = time_text(cutoff * 1_000_000);
    fs::create_dir_all(&state).unwrap();
    fs::write(&log, b"orphan from a completed intent\n").unwrap();
    pipeline("update", &config, &["--end", &end]).unwrap_err();
    let original_header = fs::read(&header).unwrap();
    let first_line = fs::read(&log).unwrap();
    assert_eq!(first_line.iter().filter(|byte| **byte == b'\n').count(), 1);

    // A header without a log is a valid zero-page pending intent, even though a partial
    // snapshot already exists. Refetch against its pinned seed, not that partial snapshot.
    fs::remove_file(&log).unwrap();
    let before = f.pocket.requests().len();
    pipeline("update", &config, &["--end", &end]).unwrap_err();
    let request: Value = serde_json::from_str(&f.pocket.requests()[before]).unwrap();
    assert_eq!(request["time"].as_i64().unwrap() - POCKET_OFFSET_S, cutoff);
    assert_eq!(fs::read(&header).unwrap(), original_header);
    let first_line = fs::read(&log).unwrap();
    pipeline("update", &config, &["--end", &end]).unwrap_err();
    let two_lines = fs::read(&log).unwrap();
    assert!(two_lines.starts_with(&first_line));
    assert_eq!(two_lines.iter().filter(|byte| **byte == b'\n').count(), 2);
    assert_eq!(fs::read(&header).unwrap(), original_header);
    let second_page: Value = serde_json::from_slice(&two_lines[first_line.len()..]).unwrap();
    let second_digest = second_page["sha256"].as_str().unwrap();
    assert!(store.join(format!("objects/{second_digest}")).exists());

    // Simulate process death halfway through appending page two, after retaining its bytes.
    fs::write(
        &log,
        &two_lines[..first_line.len() + (two_lines.len() - first_line.len()) / 2],
    )
    .unwrap();
    let before = f.pocket.requests().len();
    let resumed = pipeline("update", &config, &["--end", &end]).unwrap_err();
    assert!(
        resumed.contains("progress log: 1 partial line ignored"),
        "{resumed}"
    );
    let request: Value = serde_json::from_str(&f.pocket.requests()[before]).unwrap();
    assert_eq!(
        request["time"].as_i64().unwrap() - POCKET_OFFSET_S,
        cutoff - 197,
        "page with interrupted append is fetched again"
    );
    assert_eq!(fs::read(&header).unwrap(), original_header);
    let log_bytes = fs::read(&log).unwrap();
    assert!(log_bytes.starts_with(&first_line));
    assert_eq!(log_bytes.iter().filter(|byte| **byte == b'\n').count(), 2);

    // Only an incomplete final line is ignorable; a complete malformed record refuses resume.
    let before = f.pocket.requests().len();
    let mut malformed = log_bytes.clone();
    malformed.extend_from_slice(b"{broken}\n");
    fs::write(&log, &malformed).unwrap();
    let refused = pipeline("update", &config, &["--end", &end]).unwrap_err();
    assert!(refused.contains("progress.pages.jsonl line 3"), "{refused}");
    assert_eq!(f.pocket.requests().len(), before);
    assert_eq!(fs::read(&header).unwrap(), original_header);
    assert_eq!(fs::read(&log).unwrap(), malformed);
    fs::write(&log, &log_bytes).unwrap();

    // Migrate an old inline prefix while retaining a newer appended page after it.
    let mut legacy = read_json(&header);
    legacy["progress"]["pages"] = json!([serde_json::from_slice::<Value>(&first_line).unwrap()]);
    fs::write(&header, serde_json::to_vec(&legacy).unwrap()).unwrap();
    fs::write(&log, &log_bytes[first_line.len()..]).unwrap();
    let done = pipeline("update", &config, &["--end", &end]).unwrap();
    assert!(!done.contains("partial line ignored"), "{done}");
    assert!(!header.exists() && !log.exists());
    let generation = field(job_line(&done, "pocket"), "dataset");
    let manifest = dataset(&store, generation);
    assert_bundle(&store, &manifest, 3);
    let coverage = coverage(&store, &manifest);
    let anchors: Vec<_> = coverage
        .pages
        .iter()
        .map(|page| page.anchor.as_ref().unwrap().parse::<i64>().unwrap() - POCKET_OFFSET_S)
        .collect();
    assert_eq!(anchors, [cutoff, cutoff - 197, cutoff - 392]);
    let verified =
        verify::run(&format!("file://{}", store.join(manifest.key()).display())).unwrap();
    assert!(
        verified.contains(&format!(
            "generation {} rows {}",
            manifest.generation, manifest.row_count
        )),
        "{verified}"
    );
    assert_eq!(
        manifest.layout,
        Some(binary_alpha_engine::dataset::Layout::DailyV2)
    );
    assert_eq!(
        bars(&store, &manifest),
        expected_bars(POCKET_SEED_END, POCKET_SEED_END - 60, cutoff)
    );
}

/// A malformed archived index passes transport checks, but neither restore nor a later pull
/// may report success merely because both ready manifests have already been installed.
fn failed_restore_pull(f: &Fixture, catalog_id: &str) {
    legacy_fixtures::catalog(f, catalog_id);
    use binary_alpha_engine::dataset::{
        ObjectRecord, PriceRepresentation, generation_id, object_key,
    };
    use binary_alpha_engine::stream::{PROFILE_OBJECT_PATH, stream_generation_id};

    let files = f.drive.files();
    let mut catalog = Catalog::from_json(&files[catalog_id].bytes).unwrap();
    let mut dataset =
        GenerationManifest::from_json(&files[&catalog.dataset.file_id].bytes).unwrap();
    let mut stream = StreamManifest::from_json(&files[&catalog.stream.file_id].bytes).unwrap();
    let object_bytes = |record: &ObjectRecord| {
        let entry = catalog
            .objects
            .iter()
            .find(|entry| entry.key == record.key)
            .unwrap();
        files[&entry.file_id].bytes.clone()
    };
    let index_record = dataset
        .objects
        .iter_mut()
        .find(|object| object.path == "provenance/coverage.json")
        .unwrap();
    let mut index: HistoryCoverage = serde_json::from_slice(&object_bytes(index_record)).unwrap();
    let profile_record = stream
        .objects
        .iter_mut()
        .find(|object| object.path == PROFILE_OBJECT_PATH)
        .unwrap();
    let mut profile: Value = serde_json::from_slice(&object_bytes(profile_record)).unwrap();
    let page = index
        .pages
        .iter_mut()
        .filter(|page| page.path == "raw/pages.bin")
        .nth(1)
        .unwrap();
    let expected_offset = page.offset.unwrap();
    page.offset = Some(expected_offset + 1);
    let expected = format!(
        "pages do not tile the bundle raw/pages.bin: expected offset {expected_offset}, recorded Some({})",
        expected_offset + 1
    );

    // Rebind the coverage, dataset, stream profile, stream, and catalog identities so the
    // sole defect is the second page's offset. The archived bundle bytes stay untouched.
    let mut state = f.drive.state.lock().unwrap();
    let mut replace_object = |record: &mut ObjectRecord, bytes: Vec<u8>| {
        let entry = catalog
            .objects
            .iter_mut()
            .find(|entry| entry.key == record.key)
            .unwrap();
        record.sha256 = binary_alpha_engine::hex(&Sha256::digest(&bytes));
        record.bytes = bytes.len() as u64;
        record.key = object_key(&record.sha256);
        record.crc32c = None;
        record.generation = None;
        entry.key = record.key.clone();
        entry.sha256 = record.sha256.clone();
        entry.bytes = record.bytes;
        let remote = state.files.get_mut(&entry.file_id).unwrap();
        remote.name = format!("object-{}", record.sha256);
        remote.bytes = bytes;
    };
    replace_object(index_record, serde_json::to_vec(&index).unwrap());
    let scale = match dataset.price_representation {
        PriceRepresentation::IntegerUnits { scale } => Some(scale),
        _ => None,
    };
    dataset.generation = generation_id(
        &binary_alpha_engine::market::InstrumentId {
            broker: dataset.broker.clone(),
            provider_symbol: dataset.provider_symbol.clone(),
        },
        dataset.source_kind,
        dataset.role,
        scale,
        &dataset.objects,
    );
    profile["source"]["generation"] = json!(dataset.generation);
    replace_object(profile_record, serde_json::to_vec(&profile).unwrap());
    stream.source_generation = dataset.generation.clone();
    stream.generation = stream_generation_id(
        &stream.source_generation,
        &stream.definition.canonical_toml(),
    );
    GenerationManifest::from_json(&dataset.to_json()).unwrap();
    StreamManifest::from_json(&stream.to_json()).unwrap();
    for (entry, generation, key, bytes) in [
        (
            &mut catalog.dataset,
            &dataset.generation,
            dataset.key(),
            dataset.to_json(),
        ),
        (
            &mut catalog.stream,
            &stream.generation,
            stream.key(),
            stream.to_json(),
        ),
    ] {
        entry.generation = generation.clone();
        entry.key = key;
        entry.sha256 = binary_alpha_engine::hex(&Sha256::digest(&bytes));
        entry.bytes = bytes.len() as u64;
        state.files.get_mut(&entry.file_id).unwrap().bytes = bytes;
    }
    let bytes = serde_json::to_vec(&catalog).unwrap();
    Catalog::from_json(&bytes).unwrap();
    let sha = binary_alpha_engine::hex(&Sha256::digest(&bytes));
    state.files.get_mut(catalog_id).unwrap().bytes = bytes;
    drop(state);

    let consumer_root = f.scratch.path("bad-index-consumer");
    let consumer = f.scratch.path("bad-index-consumer.toml");
    fs::write(
        &consumer,
        pipeline_toml(&consumer_root, &f.drive.base, &[], None, 3),
    )
    .unwrap();
    let refused = pipeline(
        "restore",
        &consumer,
        &[
            "--catalog",
            catalog_id,
            "--sha256",
            &sha,
            "--broker",
            "pocket_option",
            "--symbol",
            "AEDCNY_otc",
        ],
    )
    .unwrap_err();
    assert!(refused.contains(&expected), "{refused}");
    let store = consumer_root.join("store");
    assert!(store.join(&catalog.dataset.key).is_file());
    assert!(store.join(&catalog.stream.key).is_file());
    let bundled = dataset
        .objects
        .iter()
        .find(|object| object.path == "raw/pages.bin")
        .unwrap();
    let bundle_entry = catalog
        .objects
        .iter()
        .find(|entry| entry.key == bundled.key)
        .unwrap();
    assert_eq!(
        fs::read(store.join(&bundled.key)).unwrap(),
        files[&bundle_entry.file_id].bytes
    );
    // A legacy catalog never wins selection while a daily catalog exists: pull restores the
    // newest verified daily catalog and never reports the refused, already installed legacy
    // manifests as local. The refused legacy dataset stays refused.
    let pulled = pipeline(
        "pull",
        &consumer,
        &["--broker", "pocket_option", "--symbol", "AEDCNY_otc"],
    )
    .unwrap();
    assert!(!pulled.contains("already local"), "{pulled}");
    let pulled_dataset = field(&pulled, "dataset");
    assert!(
        !pulled_dataset.contains(&catalog.dataset.generation),
        "{pulled}"
    );
    verify::run(pulled_dataset).unwrap();
    let legacy = verify::run(&format!(
        "file://{}",
        store.join(&catalog.dataset.key).display()
    ))
    .unwrap_err();
    assert!(legacy.contains(&expected), "{legacy}");

    // Isolate the original refusal property from the daily-preference property above.
    let mut state = f.drive.state.lock().unwrap();
    for (id, entry) in &mut state.files {
        if id != catalog_id
            && Catalog::from_json(&entry.bytes).is_ok_and(|other| {
                other.broker == catalog.broker && other.provider_symbol == catalog.provider_symbol
            })
        {
            entry.trashed = true;
        }
    }
    let candidates: Vec<_> = state
        .files
        .iter()
        .filter(|(_, entry)| {
            !entry.trashed
                && Catalog::from_json(&entry.bytes).is_ok_and(|other| {
                    other.broker == catalog.broker
                        && other.provider_symbol == catalog.provider_symbol
                })
        })
        .map(|(id, _)| id.as_str())
        .collect();
    assert_eq!(candidates, [catalog_id]);
    drop(state);
    let isolated_root = f.scratch.path("bad-index-only-consumer");
    let isolated = f.scratch.path("bad-index-only-consumer.toml");
    fs::write(
        &isolated,
        pipeline_toml(&isolated_root, &f.drive.base, &[], None, 3),
    )
    .unwrap();
    assert!(!isolated_root.join("store").exists());
    let refused = pipeline(
        "pull",
        &isolated,
        &["--broker", "pocket_option", "--symbol", "AEDCNY_otc"],
    )
    .unwrap_err();
    assert!(refused.contains(&expected), "{refused}");
    assert!(!refused.contains("already local"), "{refused}");
}

#[test]
fn pipeline_flushes_each_job_to_a_non_send_writer() {
    struct Reports(
        std::rc::Rc<std::cell::RefCell<Vec<Vec<u8>>>>,
        Option<std::sync::mpsc::Sender<()>>,
    );
    impl Write for Reports {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .borrow_mut()
                .last_mut()
                .unwrap()
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.0.borrow_mut().push(Vec::new());
            if let Some(report_flushed) = self.1.take() {
                report_flushed.send(()).unwrap();
            }
            Ok(())
        }
    }
    let f = fixture("pipeline_streamed_reports");
    import(&f.scratch.path("deriv-import.toml")).unwrap();
    let (report_flushed, wait_for_report) = std::sync::mpsc::channel();
    f.drive.set(DriveFaults {
        upload_gate: Some(Arc::new(Mutex::new(wait_for_report))),
        ..Default::default()
    });
    fs::write(
        &f.pipeline,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("first", "missing-first.toml"), ("deriv", "deriv.toml")],
            None,
            1,
        ),
    )
    .unwrap();
    let reports = std::rc::Rc::new(std::cell::RefCell::new(vec![Vec::new()]));
    let error = data_pipeline::update_with(
        &f.pipeline,
        None,
        &FakeClock::at(DERIV_SEED_END * 1_000_000),
        &mut Reports(std::rc::Rc::clone(&reports), Some(report_flushed)),
    )
    .unwrap_err();
    assert_eq!(error, "pipeline: 1 job(s) failed: first");
    let reports = reports.borrow();
    assert_eq!(reports.len(), 3, "each job must flush independently");
    assert!(reports[2].is_empty());
    let first = std::str::from_utf8(&reports[0]).unwrap();
    let second = std::str::from_utf8(&reports[1]).unwrap();
    assert!(first.contains("pipeline job first failed: "), "{first}");
    assert!(!first.contains("pipeline update deriv"), "{first}");
    assert_eq!(field(job_line(second, "deriv"), "status"), "archived");
    assert!(!second.contains("pipeline job first"), "{second}");
}

#[test]
fn pipeline_drive_forbidden_recovery() {
    for reason in [
        "userRateLimitExceeded",
        "rateLimitExceeded",
        "storageQuotaExceeded",
    ] {
        let f = fixture(&format!("pipeline_drive_{reason}"));
        import(&f.scratch.path("pocket-import.toml")).unwrap();
        let config = f.scratch.path("pocket-only.toml");
        let transient = reason != "storageQuotaExceeded";
        fs::write(
            &config,
            pipeline_toml(
                &f.scratch.path("producer"),
                &f.drive.base,
                &[("pocket", "pocket.toml")],
                None,
                if transient { 5 } else { 1 },
            )
            .replace("parallel_transfers = 3", "parallel_transfers = 1"),
        )
        .unwrap();
        let count = if transient { 4 } else { 1 };
        f.drive.set(DriveFaults {
            forbidden_uploads: Some((reason, count)),
            ..Default::default()
        });
        let end = time_text(POCKET_SEED_END * 1_000_000);
        let result = pipeline("update", &config, &["--end", &end]);
        let report = if transient {
            result.unwrap()
        } else {
            let error = result.unwrap_err();
            assert!(
                error.contains("status 403 (storageQuotaExceeded)"),
                "{error}"
            );
            assert!(
                error.contains("pipeline: 1 job(s) failed: pocket"),
                "{error}"
            );
            // One fault would have cleared on retry: failure proves this 403 was final.
            assert_eq!(
                f.drive
                    .log()
                    .iter()
                    .filter(|line| line.starts_with("PUT ") && !line.contains("bytes */"))
                    .count(),
                1
            );
            let transfers = registry_archive::registry_state(
                &f.scratch.path("producer/pipeline_state/registry"),
            );
            assert!(
                transfers["files"]
                    .as_object()
                    .unwrap()
                    .values()
                    .any(|entry| entry["session"].is_string())
            );
            f.drive.set(DriveFaults::default());
            pipeline("update", &config, &["--end", &end]).unwrap()
        };
        assert_eq!(field(job_line(&report, "pocket"), "status"), "archived");
        if transient {
            assert_eq!(
                f.drive.state.lock().unwrap().faults.forbidden_uploads,
                Some((reason, 0))
            );
        }

        // Downloads share the classification and never write an error body into the file.
        let (id, entry) = f
            .drive
            .files()
            .into_iter()
            .max_by_key(|(_, entry)| entry.bytes.len())
            .unwrap();
        let mut hasher = binary_alpha_app::store::Hasher::default();
        hasher.write_all(&entry.bytes).unwrap();
        let expected = hasher.finish();
        let settings: data_pipeline::PipelineConfig =
            toml::from_str(&fs::read_to_string(&config).unwrap()).unwrap();
        let mut drive = binary_alpha_app::drive::Drive::open(&settings.drive).unwrap();
        let partial = f.scratch.path("download.partial");
        f.drive.set(DriveFaults {
            forbidden_downloads: Some((reason, if transient { 2 } else { 1 })),
            ..Default::default()
        });
        let result = drive.download(&id, &partial, &expected);
        if transient {
            result.unwrap();
        } else {
            let error = result.unwrap_err();
            assert!(
                error.contains("status 403 (storageQuotaExceeded)"),
                "{error}"
            );
            assert!(fs::read(&partial).unwrap().is_empty());
            f.drive.set(DriveFaults::default());
            drive.download(&id, &partial, &expected).unwrap();
        }
        assert_eq!(fs::read(partial).unwrap(), entry.bytes);
    }
}

fn chunk_rate_limited_session_recovery(reason: &'static str, poison_all: bool) {
    let f = fixture(&format!("pipeline_chunk_rate_limit_{reason}_{poison_all}"));
    let imported = import(&f.scratch.path("pocket-import.toml")).unwrap();
    let store = f.scratch.path("producer/store");
    let manifest = dataset(
        &store,
        imported_generation(&imported, "pocket_option:AEDCNY_otc"),
    );
    let object = manifest.objects.iter().max_by_key(|o| o.bytes).unwrap();
    let name = format!("object-{}", object.sha256);
    let max_attempts = 3;
    let config = f.scratch.path("pocket-only.toml");
    fs::write(
        &config,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("pocket", "pocket.toml")],
            None,
            max_attempts,
        )
        .replace("parallel_transfers = 3", "parallel_transfers = 1")
        .replace("retry_seconds = 4", "retry_seconds = 30"),
    )
    .unwrap();
    f.drive.set(DriveFaults {
        forbidden_chunk_sessions: Some((
            name.clone(),
            reason,
            if poison_all { usize::MAX } else { 1 },
        )),
        ..Default::default()
    });
    let end = time_text(POCKET_SEED_END * 1_000_000);
    let started = Instant::now();
    let result = pipeline("update", &config, &["--end", &end]);
    let elapsed = started.elapsed();
    if poison_all {
        let error = result.unwrap_err();
        assert!(
            error.contains(&format!(
                "drive upload {name}: session restarted too often (last: 403 {reason})"
            )),
            "{error}"
        );
        assert!(
            error.contains("pipeline: 1 job(s) failed: pocket"),
            "{error}"
        );
    } else {
        let report = result.unwrap_or_else(|error| {
            panic!("a chunk-rate-limited first session must recover: {error}")
        });
        assert_eq!(field(job_line(&report, "pocket"), "status"), "archived");
    }
    assert!(
        elapsed < Duration::from_secs(10),
        "session restarts must finish far below the 30-second retry budget: {elapsed:?}"
    );
    assert!(
        elapsed >= Duration::from_secs(if poison_all { 3 } else { 1 }),
        "replacement sessions must wait for exponential backoff: {elapsed:?}"
    );
    let transfers_path = f.scratch.path("producer/pipeline_state/registry");
    let transfers = registry_archive::registry_state(&transfers_path);
    let entry = &transfers["files"][&object.key];
    let id = entry["file_id"].as_str().unwrap();
    assert_eq!(entry["done"], !poison_all);
    assert!(entry["session"].is_null(), "abandonment must be durable");
    {
        let state = f.drive.state.lock().unwrap();
        let sessions: Vec<_> = state.sessions.iter().filter(|(_, s)| s.id == id).collect();
        assert_eq!(
            sessions.len(),
            if poison_all { max_attempts as usize } else { 2 }
        );
        let poisoned: Vec<_> = sessions
            .iter()
            .filter(|(_, session)| session.forbidden_chunks.is_some())
            .collect();
        assert_eq!(
            poisoned.len(),
            if poison_all { max_attempts as usize } else { 1 }
        );
        for (token, session) in poisoned {
            assert_eq!(session.forbidden_chunks, Some(reason));
            assert!(!session.completed);
            assert!(session.received.is_empty());
            assert_eq!(session.chunks, 1);
            assert_eq!(
                state
                    .log
                    .iter()
                    .filter(|line| {
                        line.starts_with(&format!("PUT /upload/session/{token} bytes "))
                            && !line.contains("bytes */")
                    })
                    .count(),
                1,
                "a poisoned session must get exactly one chunk PUT: {:?}",
                state.log
            );
        }
        for (token, _) in sessions {
            assert!(
                state
                    .log
                    .contains(&format!("BEGIN {id} /upload/session/{token}")),
                "every replacement must begin under the same file id"
            );
        }
        if poison_all {
            assert!(!state.files.contains_key(id));
        }
    }
    if poison_all {
        f.drive.set(DriveFaults::default());
        let recovered = pipeline("update", &config, &["--end", &end]).unwrap();
        assert_eq!(field(job_line(&recovered, "pocket"), "status"), "archived");
    }
    let transfers = registry_archive::registry_state(&transfers_path);
    assert_eq!(transfers["files"][&object.key]["file_id"], id);
    assert_eq!(transfers["files"][&object.key]["done"], true);
    assert!(transfers["files"][&object.key]["session"].is_null());
    let state = f.drive.state.lock().unwrap();
    let sessions: Vec<_> = state.sessions.iter().filter(|(_, s)| s.id == id).collect();
    assert_eq!(
        sessions.len(),
        if poison_all {
            max_attempts as usize + 1
        } else {
            2
        }
    );
    let completed: Vec<_> = sessions.iter().filter(|(_, s)| s.completed).collect();
    assert_eq!(completed.len(), 1, "the object must be stored exactly once");
    let (token, session) = completed[0];
    assert!(session.forbidden_chunks.is_none());
    assert!(
        state
            .log
            .contains(&format!("BEGIN {id} /upload/session/{token}"))
    );
    let remote = &state.files[id];
    assert_eq!(remote.name, name);
    assert_eq!(remote.bytes.len() as u64, object.bytes);
    assert_eq!(
        binary_alpha_engine::hex(&Sha256::digest(&remote.bytes)),
        object.sha256
    );
    assert_eq!(remote.bytes, fs::read(store.join(&object.key)).unwrap());
    assert_eq!(remote.bytes, session.received);
    assert_eq!(
        state
            .files
            .values()
            .filter(|file| file.name == name)
            .count(),
        1
    );
}

#[test]
fn pipeline_drive_chunk_rate_limit_recovery() {
    for reason in ["userRateLimitExceeded", "rateLimitExceeded"] {
        chunk_rate_limited_session_recovery(reason, false);
    }
}

#[test]
fn pipeline_drive_chunk_rate_limit_exhaustion() {
    for reason in ["userRateLimitExceeded", "rateLimitExceeded"] {
        chunk_rate_limited_session_recovery(reason, true);
    }
}

fn abandoned_session_recovery(reason: &'static str, completed: bool) {
    let f = fixture(&format!("pipeline_abandoned_{reason}_{completed}"));
    import(&f.scratch.path("pocket-import.toml")).unwrap();
    let config = f.scratch.path("pocket-only.toml");
    let settings = pipeline_toml(
        &f.scratch.path("producer"),
        &f.drive.base,
        &[("pocket", "pocket.toml")],
        None,
        2,
    )
    .replace("parallel_transfers = 3", "parallel_transfers = 1");
    fs::write(
        &config,
        settings.replace("retry_seconds = 4", "retry_seconds = 1"),
    )
    .unwrap();
    f.drive.set(DriveFaults {
        unavailable_uploads: if completed { 0 } else { usize::MAX },
        complete_without_reply: completed,
        ..Default::default()
    });
    let end = time_text(POCKET_SEED_END * 1_000_000);
    let failed = pipeline("update", &config, &["--end", &end]).unwrap_err();
    assert!(
        failed.contains(if completed {
            "status 409 (fileIdInUse)"
        } else {
            "drive upload: HTTP 503"
        }),
        "{failed}"
    );
    let transfers_path = f.scratch.path("producer/pipeline_state/registry");
    let transfers = registry_archive::registry_state(&transfers_path);
    let (key, entry) = transfers["files"]
        .as_object()
        .unwrap()
        .iter()
        .find(|(_, entry)| entry["session"].is_string())
        .expect("failed upload must leave its pre-generated id and session recorded");
    let id = entry["file_id"].as_str().unwrap();
    let session_path = entry["session"]
        .as_str()
        .unwrap()
        .strip_prefix(&f.drive.base)
        .unwrap();
    assert_eq!(f.drive.files().contains_key(id), completed);
    assert_eq!(entry["done"], false);
    fs::write(
        &config,
        settings.replace("retry_seconds = 4", "retry_seconds = 30"),
    )
    .unwrap();
    f.drive.set(DriveFaults {
        forbidden_session_status: Some((session_path.to_string(), reason)),
        // Session creation still retries a genuinely rate-limited account.
        forbidden_begins: (!completed).then_some((reason, 1)),
        omit_sha256: completed,
        ..Default::default()
    });
    if completed {
        // Reconciliation must read and check content when metadata lacks a checksum.
        f.drive
            .state
            .lock()
            .unwrap()
            .files
            .get_mut(id)
            .unwrap()
            .bytes[0] ^= 1;
        let conflict = pipeline("update", &config, &["--end", &end]).unwrap_err();
        assert!(conflict.contains("nothing was replaced"), "{conflict}");
        assert_eq!(
            registry_archive::registry_state(&transfers_path)["files"][key],
            *entry
        );
        f.drive
            .state
            .lock()
            .unwrap()
            .files
            .get_mut(id)
            .unwrap()
            .bytes[0] ^= 1;
    }
    let log_before = f.drive.log().len();
    let started = Instant::now();
    let recovered = pipeline("update", &config, &["--end", &end]).unwrap_or_else(|error| {
        panic!("recorded session {reason} (completed={completed}) must recover: {error}")
    });
    // The configured retry wait is 30 seconds, so any retry sleep pushes the elapsed time
    // past this bound; the bound itself leaves room for a loaded host.
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "session recovery must finish far below its 30-second retry budget"
    );
    assert_eq!(field(job_line(&recovered, "pocket"), "status"), "archived");
    let transfers = registry_archive::registry_state(&transfers_path);
    assert_eq!(transfers["files"][key]["file_id"], id);
    assert_eq!(transfers["files"][key]["done"], true);
    assert!(transfers["files"][key]["session"].is_null());
    let log = f.drive.log();
    let resumed = &log[log_before..];
    assert_eq!(
        resumed
            .iter()
            .filter(|line| line.starts_with(&format!("PUT {session_path} bytes */")))
            .count(),
        1,
        "the abandoned session is queried exactly once: {resumed:?}"
    );
    assert!(!resumed.iter().any(
        |line| line.starts_with(&format!("PUT {session_path} bytes "))
            && !line.contains("bytes */")
    ));
    assert!(
        resumed
            .iter()
            .any(|line| line.starts_with(&format!("GET /drive/v3/files/{id} ")))
    );
    let state = f.drive.state.lock().unwrap();
    let sessions: Vec<_> = state
        .sessions
        .iter()
        .filter(|(_, session)| session.id == id)
        .collect();
    assert_eq!(
        sessions.len(),
        if completed { 1 } else { 2 },
        "new session only when the file is absent"
    );
    assert_eq!(
        sessions
            .iter()
            .filter(|(_, session)| session.completed)
            .count(),
        1,
        "the pre-generated id is uploaded exactly once"
    );
    let (token, session) = sessions
        .iter()
        .find(|(_, session)| session.completed)
        .unwrap();
    let entry = &state.files[id];
    assert_eq!(entry.bytes, session.received);
    assert_eq!(
        entry.name,
        format!(
            "object-{}",
            binary_alpha_engine::hex(&Sha256::digest(&entry.bytes))
        )
    );
    assert_eq!(
        state
            .files
            .values()
            .filter(|file| file.name == entry.name)
            .count(),
        1
    );
    if !completed {
        assert_ne!(format!("/upload/session/{token}"), session_path);
        assert!(
            resumed
                .iter()
                .any(|line| line.starts_with("POST /upload/drive/v3/files "))
        );
        assert!(
            resumed
                .iter()
                .any(|line| line.starts_with(&format!("PUT /upload/session/{token} bytes 0-")))
        );
        assert_eq!(state.faults.forbidden_begins, Some((reason, 0)));
    }
}

#[test]
fn pipeline_recovery() {
    for reason in ["userRateLimitExceeded", "rateLimitExceeded"] {
        for completed in [false, true] {
            abandoned_session_recovery(reason, completed);
        }
    }
    append_log_recovery();
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
        )
        .replace("retry_seconds = 4", "retry_seconds = 1")
        .replace("parallel_transfers = 3", "parallel_transfers = 1"),
    )
    .unwrap();

    let imported = import(&f.scratch.path("pocket-import.toml")).unwrap();
    let seed = imported_generation(&imported, "pocket_option:AEDCNY_otc").to_string();
    // Persistent 503s exhaust the time budget, leaving the session and identity durable.
    // A rerun clears that failure and survives four consecutive 503s on the same object,
    // even with max_attempts = 1 (which governs authentication and session restarts only).
    let seed_end = time_text(POCKET_SEED_END * 1_000_000);
    f.drive.set(DriveFaults {
        unavailable_uploads: usize::MAX,
        ..Default::default()
    });
    let started = Instant::now();
    let failed = pipeline("update", &pocket_only, &["--end", &seed_end]).unwrap_err();
    assert!(started.elapsed() >= Duration::from_secs(1));
    assert!(
        failed.contains("pipeline job pocket failed: drive upload: HTTP 503 after ")
            && failed.contains(" attempts over 1 s"),
        "{failed}"
    );
    let transfers = registry_archive::registry_state(&state.join("registry"));
    let open_session = transfers["files"]
        .as_object()
        .unwrap()
        .values()
        .find(|entry| entry["session"].is_string())
        .cloned();
    assert!(open_session.is_some(), "{transfers}");
    let session = open_session.unwrap()["session"]
        .as_str()
        .unwrap()
        .to_string();
    fs::write(
        &pocket_only,
        fs::read_to_string(&pocket_only)
            .unwrap()
            .replace("retry_seconds = 1", "retry_seconds = 4"),
    )
    .unwrap();
    f.drive.set(DriveFaults {
        unavailable_uploads: 4,
        ..Default::default()
    });
    let log_before = f.drive.log().len();
    let started = Instant::now();
    let first = pipeline("update", &pocket_only, &["--end", &seed_end]).unwrap();
    assert!(started.elapsed() >= Duration::from_millis(3_750));
    assert_eq!(f.drive.state.lock().unwrap().faults.unavailable_uploads, 0);
    let session_path = session.strip_prefix(&f.drive.base).unwrap();
    assert_eq!(
        f.drive.log()[log_before..]
            .iter()
            .filter(|line| line.starts_with(&format!("PUT {session_path} bytes 0-")))
            .count(),
        5,
        "four 503s then success on the same content request"
    );
    let baseline = field(job_line(&first, "pocket"), "dataset").to_string();
    assert_eq!(field(job_line(&first, "pocket"), "status"), "archived");
    assert_ne!(baseline, seed);
    assert_eq!(
        bars(&store, &dataset(&store, &baseline)),
        bars(&store, &dataset(&store, &seed))
    );
    assert_eq!(
        coverage(&store, &dataset(&store, &baseline))
            .seed
            .unwrap()
            .generation,
        seed
    );
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
    assert!(
        files
            .values()
            .any(|entry| entry.name.starts_with("catalog-"))
    );
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
    let mut resumed_anchor = cutoff;
    let pending_path = state.join("pocket/progress.json");
    let pages_path = state.join("pocket/progress.pages.jsonl");
    let mut header_bytes = None;
    for n in 0..3 {
        let requests_before = f.pocket.requests().len();
        let result = pipeline("update", &pocket_only, &["--end", &end]);
        let request: Value = serde_json::from_str(&f.pocket.requests()[requests_before]).unwrap();
        assert_eq!(
            request["time"].as_i64().unwrap() - POCKET_OFFSET_S,
            resumed_anchor
        );
        match result {
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
                let pending = read_progress(&state.join("pocket/progress.json"));
                assert_eq!(pending["progress"]["cutoff"], json!(end));
                assert_eq!(pending["progress"]["baseline"], json!(baseline));
                let pages = pending["progress"]["pages"].as_array().unwrap();
                assert_eq!(pages.len(), n + 1);
                assert_eq!(
                    fs::read_to_string(&pages_path).unwrap().lines().count(),
                    n + 1,
                    "exactly one appended line per new retained page; replay adds none"
                );
                let header = fs::read(&pending_path).unwrap();
                assert!(read_json(&pending_path)["progress"].get("pages").is_none());
                if let Some(before) = &header_bytes {
                    assert_eq!(&header, before, "progress.json is written only once");
                } else {
                    header_bytes = Some(header);
                }
                assert_eq!(pages[n]["rows"], 40);
                assert_eq!(
                    pages[n]["last"],
                    time_text((cutoff - 2 - n as i64 * 195) * 1_000_000)
                );
                resumed_anchor = binary_alpha_engine::market::parse_event_time_micros(
                    pages.last().unwrap()["first"].as_str().unwrap(),
                )
                .unwrap()
                    / 1_000_000;
                assert_eq!(resumed_anchor, cutoff - 2 - (n as i64 + 1) * 195);
            }
        }
    }
    assert_eq!(statuses, ["pending", "pending", "archived"]);
    assert!(!state.join("pocket/progress.json").exists());
    assert!(!pages_path.exists());
    let final_generation = generations.last().unwrap();
    let final_coverage = coverage(&store, &dataset(&store, final_generation));
    let acquired: Vec<_> = final_coverage.pages.iter().collect();
    assert_eq!(acquired.len(), 3);
    let mut offset = 0;
    for (page, anchor) in acquired.iter().zip([cutoff, cutoff - 197, cutoff - 392]) {
        assert_eq!(
            page.anchor.as_ref().unwrap().parse::<i64>().unwrap() - POCKET_OFFSET_S,
            anchor
        );
        assert_eq!(page.offset, None);
        offset += page.bytes;
    }
    assert_eq!(offset, acquired.iter().map(|p| p.bytes).sum::<u64>());
    assert_bundle(&store, &dataset(&store, final_generation), 3);
    assert_eq!(
        bars(&store, &dataset(&store, final_generation)),
        expected_bars(POCKET_SEED_END, POCKET_SEED_END - 60, cutoff)
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
    // Receipts record consumed pages in acquisition order, including retained pages replayed
    // after a restart. Unconsumed look-ahead requests may legitimately repeat across runs.
    let receipt = receipts
        .iter()
        .map(|path| read_json(path))
        .find(|receipt| receipt["dataset_generation"] == json!(final_generation))
        .unwrap();
    let anchors: Vec<i64> = receipt["requests"]
        .as_array()
        .unwrap()
        .iter()
        .map(|page| page["anchor"].as_str().unwrap().parse::<i64>().unwrap() - POCKET_OFFSET_S)
        .collect();
    assert_eq!(anchors, [cutoff, cutoff - 197, cutoff - 392]);
    // The same cutoff retains new response occurrences while reusing unchanged market days.
    let repeat = pipeline("update", &pocket_only, &["--end", &end]).unwrap();
    let repeated_generation = field(job_line(&repeat, "pocket"), "dataset");
    assert_ne!(repeated_generation, final_generation);
    let previous = dataset(&store, final_generation);
    let repeated = dataset(&store, repeated_generation);
    let market_keys = |m: &GenerationManifest| {
        m.objects
            .iter()
            .filter(|o| o.path.starts_with("observations/"))
            .map(|o| (&o.path, &o.key))
            .map(|(p, k)| (p.clone(), k.clone()))
            .collect::<BTreeMap<_, _>>()
    };
    assert_eq!(market_keys(&previous), market_keys(&repeated));
    assert_eq!(bars(&store, &previous), bars(&store, &repeated));
    let count_pages = |m: &GenerationManifest| {
        m.day_inventory
            .iter()
            .filter(|d| d.family == binary_alpha_engine::dataset::DayFamily::Pages)
            .map(|d| d.rows)
            .sum::<u64>()
    };
    assert!(count_pages(&repeated) > count_pages(&previous));
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
    assert_eq!(receipt["dataset_generation"], json!(repeated_generation));
    assert!(
        !receipt["requests"].as_array().unwrap().is_empty(),
        "{receipt}"
    );

    // Expired session, missing remote checksum, unrelated same-name file, and completion whose
    // reply was lost: with retries allowed again, a fresh archive of a new generation survives
    // each once; the lost completion is reconciled by session status on the next run.
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
        drop_upload_at_chunk: Some(1),
        omit_sha256: true,
        complete_without_reply: true,
        drop_download_at: Some(1),
        ..Default::default()
    });
    let log_before = f.drive.log().len();
    let lost_reply = pipeline("update", &pocket_only, &["--end", &end_2]).unwrap_err();
    assert!(
        lost_reply.contains("status 409 (fileIdInUse)"),
        "{lost_reply}"
    );
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
    assert!(
        f.drive.log()[log_before..]
            .iter()
            .any(|line| line.starts_with("GET /drive/v3/files/") && line.contains("bytes=1-")),
        "checksum readback resumes after the streamed prefix without hashing it twice: {:?}",
        &f.drive.log()[log_before..]
    );
    f.drive.set(DriveFaults::default());

    // Changed same-size remote content is a conflict that replaces nothing: first the archived
    // catalog that a repeated archive confirms through its receipt, then a market-day object
    // that the next update reuses by key. A repeated cutoff records new page occurrences, so
    // it publishes a new generation and never reuses the previous catalog itself.
    let flip = |id: &str| {
        let mut state = f.drive.state.lock().unwrap();
        let entry = state.files.get_mut(id).unwrap();
        entry.bytes[0] ^= 0x01;
        entry.bytes.clone()
    };
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
    let tampered = flip(&catalog_id);
    let conflict = pipeline("archive", &pocket_only, &[]).unwrap_err();
    assert!(conflict.contains("nothing was replaced"), "{conflict}");
    assert_eq!(f.drive.files()[&catalog_id].bytes, tampered);
    flip(&catalog_id);
    let object_id = {
        let object = dataset(&store, &generation_2)
            .objects
            .into_iter()
            .find(|object| object.path.starts_with("observations/"))
            .unwrap();
        let files = f.drive.files();
        files
            .iter()
            .find(|(_, entry)| entry.name == format!("object-{}", object.sha256))
            .map(|(id, _)| id.clone())
            .unwrap()
    };
    let tampered = flip(&object_id);
    let conflict = pipeline("update", &pocket_only, &["--end", &end_2]).unwrap_err();
    assert!(conflict.contains("nothing was replaced"), "{conflict}");
    assert_eq!(f.drive.files()[&object_id].bytes, tampered);
    flip(&object_id);
    // The interrupted operation resumes at its pinned cutoff once the remote content agrees.
    let resumed = pipeline("update", &pocket_only, &["--end", &end_2]).unwrap();
    assert_eq!(field(job_line(&resumed, "pocket"), "status"), "archived");
    assert_ne!(field(job_line(&resumed, "pocket"), "dataset"), generation_2);

    // Identifier allocation above the service limit is batched: 1500 identifiers arrive unique
    // through more than one request, and the fake refuses any single request over 1000.
    {
        let mut drive =
            binary_alpha_app::drive::Drive::open(&binary_alpha_app::drive::DriveSettings {
                root_folder_id: "fixture-root".into(),
                credential: None,
                chunk_bytes: 262_144,
                request_timeout_seconds: 5,
                max_attempts: 3,
                retry_seconds: None,
                loopback_endpoint: Some(f.drive.base.clone()),
            })
            .unwrap();
        let log_before = f.drive.log().len();
        let ids = drive.generate_ids(1_500).unwrap();
        assert_eq!(ids.len(), 1_500);
        assert_eq!(
            ids.iter().collect::<std::collections::BTreeSet<_>>().len(),
            1_500
        );
        assert!(
            f.drive.log()[log_before..]
                .iter()
                .filter(|line| line.contains("generateIds"))
                .count()
                >= 2
        );
    }

    // Transport drop after one retained page and rejected reconnect: the run fails, the page and its request receipt
    // stay durable, and the resumed run (through one token refresh) closes at the same cutoff
    // carrying that receipt.
    let cutoff_3 = cutoff_2 + 150;
    let end_3 = time_text(cutoff_3 * 1_000_000);
    // This fault requires the first response to become durable before reconnect.
    // A multi-request prefetch can hit the close while still sending its initial
    // burst, before consuming that response. Use one in-flight page for this
    // serial retention proof; the regular pipeline fixtures still exercise eight.
    let pocket_core_path = f.scratch.path("pocket.toml");
    let original_pocket_core = fs::read_to_string(&pocket_core_path).unwrap();
    let serial_pocket_core = original_pocket_core.replace(
        &format!("history_pages_in_flight = {POCKET_HISTORY_PAGES_IN_FLIGHT}"),
        "history_pages_in_flight = 1",
    );
    fs::write(&pocket_core_path, serial_pocket_core).unwrap();
    f.pocket.set(BrokerFaults {
        drop_after_pages: Some(1),
        reject_auth_after_drop: true,
        ..Default::default()
    });
    let dropped = pipeline("update", &pocket_only, &["--end", &end_3]).unwrap_err();
    assert!(dropped.contains("pipeline job pocket failed"), "{dropped}");
    let pending = read_progress(&state.join("pocket/progress.json"));
    let retained = pending["progress"]["pages"].as_array().unwrap().clone();
    assert_eq!(retained.len(), 1, "{dropped}\n{pending}");
    assert!(retained[0]["receipt_time"].is_string(), "{pending}");
    f.pocket.set(BrokerFaults::default());
    f.drive.set(DriveFaults {
        unauthorized_once: true,
        ..Default::default()
    });
    let log_before = f.drive.log().len();
    let resumed = pipeline("update", &pocket_only, &["--end", &end_3]).unwrap();
    assert_eq!(
        field(job_line(&resumed, "pocket"), "status"),
        "archived",
        "{resumed}"
    );
    assert_eq!(field(job_line(&resumed, "pocket"), "cutoff"), end_3);
    assert!(!state.join("pocket/progress.json").exists());
    assert_eq!(
        f.drive.log()[log_before..]
            .iter()
            .filter(|line| line.starts_with("POST /token"))
            .count(),
        2 + 3,
        "one caller refresh after the rejected token, plus three worker sessions: {:?}",
        &f.drive.log()[log_before..]
    );
    f.drive.set(DriveFaults::default());
    let generation_3 = field(job_line(&resumed, "pocket"), "dataset").to_string();
    assert_eq!(
        bars(&store, &dataset(&store, &generation_3)),
        expected_bars(POCKET_SEED_END, POCKET_SEED_END - 60, cutoff_3)
    );
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
    let requests = receipt["requests"].as_array().unwrap();
    assert!(requests.len() >= 2, "{receipt}");
    assert_eq!(requests[0]["sha256"], retained[0]["sha256"], "{receipt}");
    assert_eq!(
        requests[0]["receipt_time"], retained[0]["receipt_time"],
        "{receipt}"
    );
    fs::write(&pocket_core_path, original_pocket_core).unwrap();
    // Conflicting overlap from the provider stops publication and keeps the prior generation.
    f.pocket.set(BrokerFaults {
        conflict_before: Some(cutoff_2),
        ..Default::default()
    });
    let end_conflict = time_text((cutoff_2 + 300) * 1_000_000);
    let conflicting = pipeline("update", &pocket_only, &["--end", &end_conflict]).unwrap_err();
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
    // One transfer worker makes the interrupted object and retained partial deterministic.
    fs::write(
        &consumer,
        pipeline_toml(&consumer_root, &f.drive.base, &[], None, 1)
            .replace("parallel_transfers = 3", "parallel_transfers = 1")
            .replace("retry_seconds = 4", "retry_seconds = 1"),
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
        drop_download_requests: usize::MAX,
        ..Default::default()
    });
    let interrupted = pipeline("restore", &consumer, &restore_args).unwrap_err();
    assert!(
        interrupted.contains("drive files.get ")
            && interrupted.contains(": transport failure after ")
            && interrupted.contains(" attempts over 1 s: error sending request"),
        "{interrupted}"
    );
    assert!(!interrupted.contains(&f.drive.base), "{interrupted}");
    assert!(!interrupted.contains("fixture-token"), "{interrupted}");
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
    fs::write(
        &consumer,
        fs::read_to_string(&consumer)
            .unwrap()
            .replace("retry_seconds = 1", "retry_seconds = 4"),
    )
    .unwrap();
    f.drive.set(DriveFaults {
        drop_download_requests: 4,
        ..Default::default()
    });
    let log_before = f.drive.log().len();
    let started = Instant::now();
    let restored = pipeline("restore", &consumer, &restore_args).unwrap();
    assert!(started.elapsed() >= Duration::from_millis(3_750));
    assert_eq!(
        f.drive.state.lock().unwrap().faults.drop_download_requests,
        0
    );
    assert_eq!(
        f.drive.log()[log_before..]
            .iter()
            .filter(|line| line.contains(&format!("bytes={partial_len}-")))
            .count(),
        5,
        "four connection drops then a successful ranged download"
    );
    assert!(
        restored.starts_with("restored pocket_option:AEDCNY_otc development"),
        "{restored}"
    );
    // Flip a byte in the archived current bundle. Transport identity checking refuses a
    // fresh restore; then dataset verification diagnoses the exact page in those downloaded
    // bytes using an otherwise intact restored closure (without changing any recorded digest).
    let archived = dataset(&store, &generation_2);
    let bundled = archived
        .objects
        .iter()
        .find(|object| object.path.starts_with("pages/"))
        .unwrap();
    let bundle_id = f
        .drive
        .files()
        .iter()
        .find(|(_, entry)| entry.name == format!("object-{}", bundled.sha256))
        .map(|(id, _)| id.clone())
        .unwrap();
    let original_bundle = f.drive.files()[&bundle_id].bytes.clone();
    let mut corrupt_bundle = original_bundle.clone();
    corrupt_bundle[20] ^= 1;
    f.drive
        .state
        .lock()
        .unwrap()
        .files
        .get_mut(&bundle_id)
        .unwrap()
        .bytes = corrupt_bundle.clone();
    let damaged_root = f.scratch.path("damaged-consumer");
    let damaged_config = f.scratch.path("damaged-consumer.toml");
    fs::write(
        &damaged_config,
        pipeline_toml(&damaged_root, &f.drive.base, &[], None, 1),
    )
    .unwrap();
    let refused = pipeline("restore", &damaged_config, &restore_args).unwrap_err();
    assert!(refused.contains("bytes carry SHA-256"), "{refused}");
    assert!(!damaged_root.join("store/manifests").exists());
    let downloaded = fs::read(damaged_root.join(format!(
        "pipeline_state/downloads/{}.partial",
        bundled.sha256
    )))
    .unwrap();
    assert_eq!(downloaded, corrupt_bundle);
    let restored_bundle = consumer_root.join("store").join(&bundled.key);
    let semantic_fault = f.scratch.path("bad-daily-payload.parquet");
    common::daily::flip_page_payload(&restored_bundle, &semantic_fault);
    fs::copy(&semantic_fault, &restored_bundle).unwrap();
    let refused = verify::run(field(&restored, "dataset")).unwrap_err();
    // A re-encoded day file no longer carries the recorded object identity; verification
    // names the exact object before decoding. The page-level diagnosis is the reader's.
    assert!(
        refused.contains(&format!(
            "{} does not match the recorded size, generation, and checksum",
            bundled.key
        )),
        "{refused}"
    );
    let date = &bundled.path["pages/".len()..][..10];
    assert_eq!(
        binary_alpha_app::daily::read_pages(&semantic_fault, date).unwrap_err(),
        "page payload_sha256 does not match payload"
    );
    fs::write(&restored_bundle, &original_bundle).unwrap();
    f.drive
        .state
        .lock()
        .unwrap()
        .files
        .get_mut(&bundle_id)
        .unwrap()
        .bytes = original_bundle;

    let object_ids: Vec<String> = f
        .drive
        .files()
        .iter()
        .filter(|(_, entry)| entry.name.starts_with("object-"))
        .map(|(id, _)| id.clone())
        .collect();
    let removed = f
        .drive
        .state
        .lock()
        .unwrap()
        .files
        .remove(&object_ids[0])
        .unwrap();
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

    // A conflicting reread invalidates every checkpoint of the pending intent, because the
    // deferred overlap boundary of an earlier page may be implicated; the received journal
    // keeps both responses as evidence and the intent resumes at its pinned cutoff by
    // refetching once the provider's data is consistent again.
    let pending = read_progress(&state.join("pocket/progress.json"));
    assert_eq!(
        pending["progress"]["cutoff"],
        json!(end_conflict),
        "{pending}"
    );
    assert!(
        pending["progress"]["pages"].as_array().unwrap().is_empty(),
        "{pending}"
    );
    let received = fs::read_to_string(state.join("pocket/progress.received.jsonl")).unwrap();
    let received: Vec<_> = received
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|entry| entry["rows"] == 40)
        .map(|entry| {
            let page: binary_alpha_app::fetch::PageCoverage =
                serde_json::from_value(entry).unwrap();
            let bytes =
                fs::read(store.join(binary_alpha_engine::dataset::object_key(&page.sha256)))
                    .unwrap();
            assert_eq!(bytes.len() as u64, page.bytes);
            assert_eq!(
                binary_alpha_engine::hex(&Sha256::digest(&bytes)),
                page.sha256
            );
            (page, bytes)
        })
        .collect();
    assert_eq!(received.len(), 2);
    assert_ne!(received[0].0.occurrence, received[1].0.occurrence);
    let assert_diagnostics = |root: &Path, manifest: &GenerationManifest| {
        let pages = common::daily::pages(root, manifest);
        for (record, bytes) in &received {
            let identity = record.occurrence.as_ref().unwrap();
            let matched: Vec<_> = pages
                .iter()
                .filter(|p| {
                    p.acquisition_id == identity.acquisition_id && p.ordinal == identity.ordinal
                })
                .collect();
            assert_eq!(
                matched.len(),
                1,
                "one diagnostic occurrence for {identity:?}"
            );
            let page = matched[0];
            assert_eq!(page.intent, identity.intent);
            assert_eq!(&page.payload, bytes);
            assert_eq!(page.payload_sha256, record.sha256);
            assert_eq!(page.rows, record.rows);
            assert_eq!(page.request_token, record.anchor);
            let time = |text: &Option<String>| {
                text.as_deref()
                    .map(|s| binary_alpha_engine::market::parse_event_time_micros(s).unwrap())
            };
            assert_eq!(page.receipt_time_utc, time(&record.receipt_time));
            assert_eq!(page.first_event_time, time(&record.first));
            assert_eq!(page.last_event_time, time(&record.last));
            assert_eq!(
                page.disposition,
                binary_alpha_app::daily::PageDisposition::Diagnostic
            );
        }
        pages
    };
    // Its closure carries the seed object deleted remotely above: archival re-confirms every
    // reused transfer and refuses instead of reporting a cached success.
    let missing = pipeline("update", &pocket_only, &[]).unwrap_err();
    assert!(missing.contains("missing or trashed"), "{missing}");
    // The acquisition stays pinned at its cutoff until reclamation closes it after a
    // successful archive; a refused archive never discards the acquired evidence.
    let pending = read_progress(&state.join("pocket/progress.json"));
    assert_eq!(
        pending["progress"]["cutoff"],
        json!(end_conflict),
        "{pending}"
    );
    f.drive
        .state
        .lock()
        .unwrap()
        .files
        .insert(object_ids[0].clone(), removed);
    let resumed = pipeline("update", &pocket_only, &["--end", &end_conflict]).unwrap();
    assert_eq!(
        field(job_line(&resumed, "pocket"), "cutoff"),
        end_conflict,
        "{resumed}"
    );
    assert_eq!(
        field(job_line(&resumed, "pocket"), "status"),
        "archived",
        "{resumed}"
    );
    assert!(!state.join("pocket/progress.json").exists(), "{resumed}");
    assert_eq!(
        bars(
            &store,
            &dataset(&store, field(job_line(&resumed, "pocket"), "dataset"))
        ),
        expected_bars(POCKET_SEED_END, POCKET_SEED_END - 60, cutoff_2 + 300)
    );
    let line = job_line(&resumed, "pocket");
    let final_manifest = dataset(&store, field(line, "dataset"));
    let final_pages = assert_diagnostics(&store, &final_manifest);
    let fresh_root = f.scratch.path("conflict-diagnostics-restored");
    let fresh_config = f.scratch.path("conflict-diagnostics-restored.toml");
    fs::write(
        &fresh_config,
        pipeline_toml(&fresh_root, &f.drive.base, &[], None, 3),
    )
    .unwrap();
    pipeline(
        "restore",
        &fresh_config,
        &[
            "--catalog",
            field(line, "catalog"),
            "--sha256",
            field(line, "sha256"),
            "--broker",
            "pocket_option",
            "--symbol",
            "AEDCNY_otc",
        ],
    )
    .unwrap();
    let restored_store = fresh_root.join("store");
    let restored_manifest = dataset(&restored_store, &final_manifest.generation);
    assert_eq!(
        assert_diagnostics(&restored_store, &restored_manifest),
        final_pages
    );
    for day in final_manifest
        .day_inventory
        .iter()
        .filter(|d| d.family == binary_alpha_engine::dataset::DayFamily::Pages)
    {
        let key = day.object.as_ref().unwrap();
        assert_eq!(
            fs::read(restored_store.join(key)).unwrap(),
            fs::read(store.join(key)).unwrap(),
            "archived page partition {key}"
        );
    }
    failed_restore_pull(&f, field(job_line(&resumed, "pocket"), "catalog"));
}

// ----------------------------------------------------------------------------------------------
// Gate 4
// ----------------------------------------------------------------------------------------------

#[test]
fn pipeline_scope() {
    let f = fixture("pipeline_scope");
    let producer = f.scratch.path("producer");
    let invalid_retry = f.scratch.path("invalid-retry.toml");
    fs::write(
        &invalid_retry,
        fs::read_to_string(&f.pipeline)
            .unwrap()
            .replace("retry_seconds = 4", "retry_seconds = 0"),
    )
    .unwrap();
    let refused = pipeline("update", &invalid_retry, &[]).unwrap_err();
    assert!(
        refused.contains("retry_seconds must be positive"),
        "{refused}"
    );
    assert!(f.drive.log().is_empty());
    assert!(f.pocket.requests().is_empty() && f.deriv.requests().is_empty());
    // Drive session failures belong to each job's report, even when all jobs fail to open.
    let unavailable_drive = f.scratch.path("unavailable-drive.toml");
    fs::write(
        &unavailable_drive,
        fs::read_to_string(&f.pipeline).unwrap().replace(
            &format!("loopback_endpoint = \"{}\"", f.drive.base),
            "credential = \"PIPELINE_UNSET_DRIVE_AUTH\"",
        ),
    )
    .unwrap();
    let refused = pipeline("update", &unavailable_drive, &[]).unwrap_err();
    for job in ["deriv", "pocket"] {
        assert!(
            refused.contains(&format!("pipeline job {job} failed:")),
            "{refused}"
        );
    }
    assert!(
        refused.contains("2 job(s) failed: deriv, pocket"),
        "{refused}"
    );
    assert!(refused.contains("PIPELINE_UNSET_DRIVE_AUTH"), "{refused}");
    assert!(f.pocket.requests().is_empty() && f.deriv.requests().is_empty());
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

    // Wrong native kind: a Pocket tick job and a Deriv bar job are refused at update before
    // any credential or connection.
    write_pocket(pocket_core(
        &f.pocket.url,
        "demo",
        r#"{ kind = "tick" }"#,
        60,
        50,
        60,
    ));
    let refused = pipeline("update", &pocket_only, &[]).unwrap_err();
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
    let refused = pipeline("update", &deriv_only, &[]).unwrap_err();
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
        let config = import_config(
            &f.scratch,
            "pocket",
            &pocket_core(&f.pocket.url, "demo", BAR_GRANULARITY, 60, 50, 60).replacen(
                r#"instruments = ["AEDCNY_otc"]"#,
                selection,
                1,
            ),
        );
        let refused = import(&config).unwrap_err();
        assert!(refused.contains(expected), "{selection}: {refused}");
    }
    assert!(f.pocket.requests().is_empty() && f.deriv.requests().is_empty());

    // Empty-store acquisition requires an explicit calendar before any broker connection.
    let mut sessionless: toml::Value = toml::from_str(&pocket_core(
        &f.pocket.url,
        "demo",
        BAR_GRANULARITY,
        60,
        50,
        60,
    ))
    .unwrap();
    sessionless["instruments"][0]
        .as_table_mut()
        .unwrap()
        .remove("session");
    write_pocket(toml::to_string(&sessionless).unwrap());
    let refused = pipeline("update", &pocket_only, &[]).unwrap_err();
    assert!(
        refused.contains(
            "job requires an explicit [instruments.session] table; no calendar is inferred"
        ),
        "{refused}"
    );
    assert!(f.pocket.requests().is_empty() && f.deriv.requests().is_empty());
    let config = import_config(
        &f.scratch,
        "pocket",
        &pocket_core(&f.pocket.url, "demo", BAR_GRANULARITY, 60, 50, 60),
    );
    let imported = import(&config).unwrap();
    let seed = imported_generation(&imported, "pocket_option:AEDCNY_otc").to_string();
    // The evidence binds the demo source context; a demo/real switch, an inconsistent seed
    // identity, and range narrowing are all refused before a connection.
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
    let seed_path = producer.join(format!("store/manifests/{seed}/ready.json"));
    let manifest = fs::read_to_string(&seed_path).unwrap();
    fs::write(&seed_path, manifest.replace(&seed, &"0".repeat(64))).unwrap();
    let refused = pipeline(
        "update",
        &pocket_only,
        &["--end", &time_text((POCKET_SEED_END + 300) * 1_000_000)],
    )
    .unwrap_err();
    assert!(
        refused.contains("does not match the recorded inputs"),
        "{refused}"
    );
    fs::write(&seed_path, &manifest).unwrap();
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
            || read_progress(&producer.join("pipeline_state/pocket/progress.json"))["progress"]["pages"]
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

    // A valid outer manifest/object identity does not excuse an invalid bundle index.
    let original_manifest = common::legacy::dataset(
        &producer.join("store"),
        &dataset(&producer.join("store"), &generation),
    );
    let mut bad_coverage = coverage(&producer.join("store"), &original_manifest);
    bad_coverage.pages.last_mut().unwrap().bytes += 1;
    let bad_root = f.scratch.path("bad-index");
    let bad_uri = indexed_fixture(
        &producer.join("store"),
        &bad_root,
        original_manifest.clone(),
        &bad_coverage,
    );
    let refused = verify::run(&bad_uri).unwrap_err();
    assert!(
        refused.contains("pages do not tile the bundle raw/pages.bin"),
        "{refused}"
    );

    // A pre-bundle manifest still verifies from its individual source page objects. None
    // fields serialize absent, exactly as in records written by the previous binary.
    let mut legacy_manifest = original_manifest.clone();
    let mut legacy_coverage = coverage(&producer.join("store"), &legacy_manifest);
    legacy_manifest
        .objects
        .retain(|object| object.path != "raw/pages.bin");
    legacy_coverage.bundle = None;
    for page in &mut legacy_coverage.pages {
        page.offset = None;
        page.path = format!("raw/{}.json", page.sha256);
        legacy_manifest
            .objects
            .push(binary_alpha_engine::dataset::ObjectRecord {
                role: binary_alpha_engine::dataset::ObjectRole::Source,
                path: page.path.clone(),
                key: binary_alpha_engine::dataset::object_key(&page.sha256),
                bytes: page.bytes,
                sha256: page.sha256.clone(),
                crc32c: None,
                generation: None,
            });
    }
    let legacy_uri = indexed_fixture(
        &producer.join("store"),
        &f.scratch.path("legacy-pages"),
        legacy_manifest,
        &legacy_coverage,
    );
    let verified = verify::run(&legacy_uri).unwrap();
    assert!(!verified.contains("history bundles"), "{verified}");

    // Deleting every page entry cannot downgrade a bundle to opaque legacy source evidence.
    let mut missing_index = coverage(&producer.join("store"), &original_manifest);
    missing_index.pages.clear();
    let missing_uri = indexed_fixture(
        &producer.join("store"),
        &f.scratch.path("missing-index"),
        original_manifest,
        &missing_index,
    );
    assert!(
        verify::run(&missing_uri)
            .unwrap_err()
            .contains("no page entries")
    );

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
    let (id, sha_extra) = tampered(&|catalog| {
        let mut extra = catalog.objects[0].clone();
        extra.key = format!("objects/{}", "e".repeat(64));
        extra.sha256 = "e".repeat(64);
        catalog.objects.push(extra);
    });
    let refused = pipeline(
        "restore",
        &consumer,
        &[
            "--catalog",
            &id,
            "--sha256",
            &sha_extra,
            "--broker",
            "pocket_option",
            "--symbol",
            "AEDCNY_otc",
        ],
    )
    .unwrap_err();
    assert!(
        refused.contains("named by neither pinned manifest"),
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

    // A zero worker count is refused when the document loads.
    let zero = f.scratch.path("zero-workers.toml");
    fs::write(
        &zero,
        pipeline_toml(
            &producer,
            &f.drive.base,
            &[("pocket", "pocket.toml")],
            None,
            3,
        )
        .replace("local_root = ", "parallel_jobs = 0\nlocal_root = "),
    )
    .unwrap();
    let refused = pipeline("update", &zero, &["--end", &end]).unwrap_err();
    assert!(
        refused.contains("parallel_jobs must be positive"),
        "{refused}"
    );
    fs::write(
        &zero,
        pipeline_toml(
            &producer,
            &f.drive.base,
            &[("pocket", "pocket.toml")],
            None,
            3,
        )
        .replace("parallel_transfers = 3", "parallel_transfers = 0"),
    )
    .unwrap();
    let refused = pipeline("update", &zero, &["--end", &end]).unwrap_err();
    assert!(
        refused.contains("parallel_transfers must be positive"),
        "{refused}"
    );

    // Hands-off credential renewal: with the referenced variable unset, the operator's command
    // supplies the session; when the provider rejects a session, the command runs once more and
    // the connection is retried; a failing command names itself without echoing any value.
    let marker = f.scratch.path("renewals.log");
    let renew = f.scratch.path("renew.sh");
    fs::write(
        &renew,
        format!(
            "#!/bin/sh\necho renewed >> {}\nprintf '%s' '{{\"synthetic\":true}}'\n",
            marker.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&renew, fs::Permissions::from_mode(0o700)).unwrap();
    let renewing = pocket_core(&f.pocket.url, "demo", BAR_GRANULARITY, 60, 50, 60).replace(
        "credential = \"PIPELINE_SYNTHETIC_AUTH\"",
        &format!(
            "credential = \"PIPELINE_UNSET_AUTH\"\ncredential_command = [\"{}\"]",
            renew.display()
        ),
    );
    assert!(renewing.contains("credential_command"));
    write_pocket(renewing.clone());
    f.pocket.set(BrokerFaults {
        reject_auth_once: true,
        ..Default::default()
    });
    let renewed_end = time_text((POCKET_SEED_END + 900) * 1_000_000);
    let renewed = pipeline("update", &pocket_only, &["--end", &renewed_end]).unwrap();
    assert_eq!(
        field(job_line(&renewed, "pocket"), "status"),
        "archived",
        "{renewed}"
    );
    assert_eq!(fs::read_to_string(&marker).unwrap().lines().count(), 2);
    assert!(!renewed.contains("synthetic\":true"), "{renewed}");
    f.pocket.set(BrokerFaults::default());
    fs::write(&renew, "#!/bin/sh\nexit 3\n").unwrap();
    let refused = pipeline(
        "update",
        &pocket_only,
        &["--end", &time_text((POCKET_SEED_END + 1_200) * 1_000_000)],
    )
    .unwrap_err();
    assert!(
        refused.contains("credential_command") && refused.contains("exited"),
        "{refused}"
    );

    // Both parallel jobs reject the same environment session. Only the first rejection
    // rotates it; its sibling must authenticate with the cached replacement.
    let parallel = fixture("pipeline_scope_parallel_renewal");
    import(&parallel.scratch.path("pocket-import.toml")).unwrap();
    let marker = parallel.scratch.path("renewals.log");
    let renew = parallel.scratch.path("renew.sh");
    fs::write(&renew, format!(
        "#!/bin/sh\necho renewed >> '{}'\nprintf '%s' '{{\"synthetic\":true,\"renewed\":true}}'\n",
        marker.display(),
    )).unwrap();
    fs::set_permissions(&renew, fs::Permissions::from_mode(0o700)).unwrap();
    let core = pocket_core(&parallel.pocket.url, "demo", BAR_GRANULARITY, 60, 50, 60).replace(
        "credential = \"PIPELINE_SYNTHETIC_AUTH\"",
        &format!(
            "credential = \"PIPELINE_SYNTHETIC_AUTH\"\ncredential_command = [\"{}\"]",
            renew.display()
        ),
    );
    fs::write(parallel.scratch.path("pocket.toml"), &core).unwrap();
    write_evidence(&parallel.scratch, "sibling", &core);
    fs::write(
        &parallel.pipeline,
        pipeline_toml(
            &parallel.scratch.path("producer"),
            &parallel.drive.base,
            &[("pocket", "pocket.toml"), ("sibling", "pocket.toml")],
            None,
            3,
        ),
    )
    .unwrap();
    parallel.pocket.set(BrokerFaults {
        reject_initial_session: true,
        ..Default::default()
    });
    let renewed = pipeline("update", &parallel.pipeline, &["--end", &renewed_end]).unwrap();
    for job in ["pocket", "sibling"] {
        assert_eq!(
            field(job_line(&renewed, job), "status"),
            "archived",
            "{renewed}"
        );
    }
    assert_eq!(parallel.pocket.rejected_auths.load(Ordering::SeqCst), 2);
    assert_eq!(fs::read_to_string(&marker).unwrap().lines().count(), 1);
    assert!(!renewed.contains("synthetic\":true"), "{renewed}");
}

fn seed_unchanged(store: &Path, generation: &str) -> bool {
    let manifests = fs::read_dir(store.join("manifests")).unwrap().count();
    store
        .join(format!("manifests/{generation}/ready.json"))
        .is_file()
        && manifests == 1
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
    import(&f.scratch.path("deriv-import.toml")).unwrap();
    import(&f.scratch.path("pocket-import.toml")).unwrap();

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
    data_pipeline::update_with(&pocket_only, None, &clock, &mut out).unwrap_err();
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
    let pending_path = producer.join("pipeline_state/pocket/progress.json");
    let pending = read_progress(&pending_path);
    assert_eq!(pending["progress"]["pages"].as_array().unwrap().len(), 1);
    assert_eq!(pending["progress"]["pages"][0]["rows"], 40);
    assert_eq!(
        pending["progress"]["pages"][0]["first"],
        time_text((cutoff - 197) * 1_000_000)
    );
    binary_alpha_app::broker::Clock::sleep(&mut clock, 600 * 1_000_000);
    let mut out = Vec::new();
    data_pipeline::update_with(&pocket_only, None, &clock, &mut out).unwrap_err();
    let resumed = String::from_utf8(out).unwrap();
    assert_eq!(
        field(job_line(&resumed, "pocket"), "cutoff"),
        time_text(cutoff * 1_000_000),
        "{resumed}"
    );
    assert_eq!(field(job_line(&resumed, "pocket"), "status"), "pending");
    let pending = read_progress(&pending_path);
    let pages = pending["progress"]["pages"].as_array().unwrap();
    assert_eq!(pages.len(), 2);
    assert_eq!(pages[1]["rows"], 40);
    assert_eq!(pages[0]["first"], pages[1]["last"]);
    assert_eq!(pages[1]["first"], time_text((cutoff - 392) * 1_000_000));
    let mut out = Vec::new();
    data_pipeline::update_with(
        &pocket_only,
        Some(&time_text((cutoff + 5) * 1_000_000)),
        &clock,
        &mut out,
    )
    .unwrap_err();
    let conflict = String::from_utf8(out).unwrap();
    assert!(conflict.contains("conflicts"), "{conflict}");
    // The third page reaches cutoff-587, past the required seed overlap.
    let mut out = Vec::new();
    data_pipeline::update_with(&pocket_only, None, &clock, &mut out).unwrap();
    let completed = String::from_utf8(out).unwrap();
    assert_eq!(field(job_line(&completed, "pocket"), "status"), "archived");
    assert!(!pending_path.exists());
    let store = producer.join("store");
    assert_eq!(
        bars(
            &store,
            &dataset(&store, field(job_line(&completed, "pocket"), "dataset"))
        ),
        expected_bars(POCKET_SEED_END, POCKET_SEED_END - 60, cutoff)
    );
    // With the intent closed, the next invocation pins the advanced clock as its cutoff and,
    // under an unbounded page budget, closes in one run.
    fs::write(
        f.scratch.path("pocket.toml"),
        pocket_core(&f.pocket.url, "demo", BAR_GRANULARITY, 60, 50, 60),
    )
    .unwrap();
    let mut out = Vec::new();
    let result = data_pipeline::update_with(&pocket_only, None, &clock, &mut out);
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
    let partial = data_pipeline::update_with(&f.pipeline, None, &clock, &mut out).unwrap_err();
    assert!(partial.contains("1 job(s) failed: deriv"), "{partial}");
    let report = String::from_utf8(out).unwrap();
    assert!(report.contains("pipeline job deriv failed"), "{report}");
    assert!(report.contains("pipeline update pocket "), "{report}");

    // Local writer-lock contention: a second producer is refused while the lock is held.
    let lock = File::create(producer.join("pipeline_state/writer.lock")).unwrap();
    lock.try_lock().unwrap();
    let held = data_pipeline::update_with(&pocket_only, None, &clock, &mut Vec::new()).unwrap_err();
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

#[path = "data_pipeline/daily_readers.rs"]
mod daily_readers;

#[path = "common/research.rs"]
mod fixture_config;
#[path = "phase12_live_runtime/support.rs"]
mod live_support;

fn migration_table_rows(path: &Path) -> Vec<parquet::record::Row> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    SerializedFileReader::new(File::open(path).unwrap())
        .unwrap()
        .get_row_iter(None)
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[path = "data_pipeline/migrate_census.rs"]
mod migrate_census;

/// Lossless offline continuation from real import/update entry points and synthetic transports.
#[test]
fn pipeline_migration_lossless_resume_and_tamper() {
    use binary_alpha_app::daily::{self, PageOrderKind, ReceiptState};
    use binary_alpha_engine::dataset::daily::{DayFamily, DayState};
    let mut f = fixture("pipeline_migration_lossless");
    let store = f.scratch.path("producer/store");
    // Repeated midnight ticks survive the shared v1/v2 reader in their original order.
    let first = vec![(DAY1 * 1_000_000_000, deriv_price(DAY1).parse().unwrap())];
    let mut second: Vec<_> = deriv_ticks(DAY2, DERIV_SEED_END)
        .into_iter()
        .map(|(t, p)| (t * 1_000_000_000, p.parse::<f64>().unwrap()))
        .collect();
    second.insert(0, second[0]);
    write_daily_directory(
        &f.scratch.path("sources/deriv/EURUSD"),
        "EURUSD",
        "frxEURUSD",
        &[("2025-08-11", &first), ("2025-08-12", &second)],
    );
    let meta = f
        .scratch
        .path("sources/deriv/EURUSD/EURUSD_2025-08-11_ticks.meta.json");
    let mut value = read_json(&meta);
    value["complete"] = json!(false);
    fs::write(meta, value.to_string()).unwrap();
    fs::write(f.scratch.path("sources/deriv/EURUSD/EURUSD_2025-08-10_ticks.meta.json"), json!({"calendar":"UTC","date":"2025-08-10","symbol":"frxEURUSD","ticks":0,"market_closed":true}).to_string()).unwrap();
    let midnight = (POCKET_START / 86_400 + 1) * 86_400;
    f.pocket = serve_broker(Kind::Pocket {
        from: midnight - 1_000,
        to: midnight + 35,
    });
    let core = pocket_core(&f.pocket.url, "demo", BAR_GRANULARITY, 30, 50, 60);
    fs::write(f.scratch.path("pocket.toml"), &core).unwrap();
    import_config(&f.scratch, "pocket", &core);
    write_evidence(&f.scratch, "pocket", &core);
    write_collection(
        &f.scratch.path("sources/pocket"),
        &[AssetSpec {
            asset: "AEDCNY_otc",
            expected_symbol_id: None,
            symbol_id: Some(POCKET_SYMBOL_ID),
            files: vec![
                bar_rows(midnight - 10, midnight),
                bar_rows(midnight, midnight + 20),
            ],
            metadata: true,
        }],
    );
    let import_root = f.scratch.path("sources/pocket/AEDCNY_otc");
    fs::write(
        import_root.join("download_manifest.json"),
        json!({"asset":"AEDCNY_otc","server_timestamp_offset_seconds":7200}).to_string(),
    )
    .unwrap();
    let page = json!({"asset":"AEDCNY_otc","period":5,"data":[{"time":midnight-5+7200},{"time":midnight+7200}]}).to_string();
    let empty = json!({"asset":"AEDCNY_otc","period":5,"data":[]}).to_string();
    let hash = |s: &str| binary_alpha_engine::hex(&Sha256::digest(s.as_bytes()));
    let cp = |payload: &str, rows: u64, recovered: bool| {
        let mut v = json!({"target_server_s":midnight+30+7200,"index":42,"attempt":1,"rows":rows,"payload_sha256":hash(payload)});
        if recovered {
            v["recovered_from_raw"] = json!(true);
        } else {
            v["received_at"] = json!(time_text((midnight + 100) * 1_000_000));
        }
        v.to_string()
    };
    let raw_bytes = format!("{page}\r\n{empty}\n{page}");
    let cp_bytes = format!(
        "{}\n{}\r\n{}",
        cp(&empty, 0, false),
        cp(&page, 2, true),
        cp(&page, 2, false)
    );
    fs::write(import_root.join("raw_pages.ndjson"), &raw_bytes).unwrap();
    fs::write(import_root.join("checkpoint.ndjson"), &cp_bytes).unwrap();
    import(&f.scratch.path("deriv-import.toml")).unwrap();
    import(&f.scratch.path("pocket-import.toml")).unwrap();
    // Capture the actual v1 outputs before migration for independent sequence oracles.
    let mut source_outputs = BTreeMap::new();
    // Run each existing pipeline job under its own cutoff.
    for (job, end) in [
        ("pocket", midnight + 40),
        ("deriv", DERIV_SEED_END + 200),
        ("deriv", DERIV_SEED_END + 300),
    ] {
        let path = f.scratch.path(&format!("migration-{job}.toml"));
        fs::write(
            &path,
            pipeline_toml(
                &f.scratch.path("producer"),
                &f.drive.base,
                &[(job, &format!("{job}.toml"))],
                None,
                2,
            ),
        )
        .unwrap();
        let report = pipeline("update", &path, &["--end", &time_text(end * 1_000_000)]).unwrap();
        let line = job_line(&report, job);
        source_outputs.insert(
            job,
            (
                field(line, "dataset").to_string(),
                field(line, "stream").to_string(),
            ),
        );
    }
    let mapping = legacy_fixtures::freeze(&f);
    for (dataset, stream) in source_outputs.values_mut() {
        *dataset = mapping[dataset].clone();
        *stream = mapping[stream].clone();
    }
    let before_generations = binary_alpha_app::store::Store::filesystem(&store)
        .list_manifests()
        .unwrap();
    let records = f.scratch.path("producer/pipeline_state/records");
    // Replay a request receipt under another immutable record name; occurrence identity stays
    // tied to its original request/receipt timestamp. Also retain one legacy single-page alias.
    let receipt = fs::read_dir(&records)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("deriv-receipt-")
        })
        .max_by_key(|p| read_json(p)["requests"].as_array().unwrap().len())
        .unwrap();
    let receipt_value = read_json(&receipt);
    fs::write(
        records.join("legacy-deriv-receipt-replay.json"),
        serde_json::to_vec_pretty(&receipt_value).unwrap(),
    )
    .unwrap();
    let mut subset = receipt_value.clone();
    let requests = receipt_value["requests"].as_array().unwrap();
    assert!(requests.len() > 1);
    subset["requests"] = json!([requests.last().unwrap()]);
    fs::write(
        records.join("legacy-deriv-receipt-subset.json"),
        serde_json::to_vec_pretty(&subset).unwrap(),
    )
    .unwrap();
    let mut additional = receipt_value.clone();
    let mut request = additional["requests"][0].clone();
    let original_receipt = binary_alpha_engine::market::parse_event_time_micros(
        request["receipt_time"].as_str().unwrap(),
    )
    .unwrap();
    request["receipt_time"] = json!(time_text(original_receipt + 1_000_000));
    additional["requests"] = json!([request]);
    fs::write(
        records.join("legacy-deriv-receipt-distinct-time.json"),
        serde_json::to_vec_pretty(&additional).unwrap(),
    )
    .unwrap();
    let generation = receipt_value["dataset_generation"].as_str().unwrap();
    let mut legacy = dataset(&store, generation);
    let cov = coverage(&store, &legacy);
    let p = &cov.pages[0];
    let mut object = legacy
        .objects
        .iter()
        .find(|o| o.path == p.path)
        .unwrap()
        .clone();
    object.path = format!("raw/{}.json", p.sha256);
    object.key = format!("objects/{}", p.sha256);
    object.bytes = p.bytes;
    object.sha256 = p.sha256.clone();
    object.crc32c = None;
    object.generation = None;
    legacy.objects.push(object);
    let scale = match legacy.price_representation {
        binary_alpha_engine::dataset::PriceRepresentation::IntegerUnits { scale } => Some(scale),
        _ => None,
    };
    legacy.generation = binary_alpha_engine::dataset::generation_id(
        &binary_alpha_engine::market::InstrumentId {
            broker: legacy.broker.clone(),
            provider_symbol: legacy.provider_symbol.clone(),
        },
        legacy.source_kind,
        legacy.role,
        scale,
        &legacy.objects,
    );
    binary_alpha_engine::dataset::GenerationManifest::from_json(&legacy.to_json()).unwrap();
    let path = store.join(legacy.key());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, legacy.to_json()).unwrap();
    let broker_requests = (f.deriv.requests().len(), f.pocket.requests().len());
    let drive_requests = f.drive.log().len();
    let mut interrupted_report = Vec::new();
    let interrupted = data_pipeline::migrate_with(
        &f.pipeline,
        Some("pocket"),
        &|_| Err("fixture interruption after converted".into()),
        &mut interrupted_report,
    )
    .unwrap_err();
    assert_eq!(interrupted, "pipeline: 1 job(s) failed: pocket");
    let interrupted_report = String::from_utf8(interrupted_report).unwrap();
    assert!(
        interrupted_report.contains("fixture interruption"),
        "{interrupted_report}"
    );
    let state_path = f
        .scratch
        .path("producer/pipeline_state/pocket/migration.json");
    let converted = read_json(&state_path);
    assert_eq!(converted["phase"], "converted");
    let v2 = dataset(&store, converted["dataset"].as_str().unwrap());
    let page_object = v2
        .objects
        .iter()
        .find(|o| o.path.starts_with("pages/"))
        .unwrap();
    let page_path = store.join(&page_object.key);
    let original = fs::read(&page_path).unwrap();
    let mut changed = original.clone();
    changed[10] ^= 1;
    fs::write(&page_path, &changed).unwrap();
    let error = pipeline("migrate", &f.pipeline, &["--job", "pocket"]).unwrap_err();
    assert!(
        error.contains("pages/") && error.contains("SHA-256"),
        "{error}"
    );
    assert_eq!(read_json(&state_path)["phase"], "converted");
    fs::write(&page_path, original).unwrap();
    let imported = converted["imports"][0]["checkpoint"]["key"]
        .as_str()
        .unwrap();
    let cp_path = store.join(imported);
    let original = fs::read(&cp_path).unwrap();
    let mut changed = original.clone();
    changed[1] ^= 1;
    fs::write(&cp_path, changed).unwrap();
    let error = pipeline("migrate", &f.pipeline, &["--job", "pocket"]).unwrap_err();
    assert!(error.contains("checkpoint"), "{error}");
    assert_eq!(read_json(&state_path)["phase"], "converted");
    fs::write(cp_path, original).unwrap();
    // Fail only the atomic checkpoint save: verification publishes its immutable record first.
    let checkpoint_before = fs::read(&state_path).unwrap();
    let records_before = common::snapshot_tree(&records);
    let checkpoint_fault = state_path.with_extension("tmp");
    let mut interrupted_report = Vec::new();
    data_pipeline::migrate_with(
        &f.pipeline,
        Some("pocket"),
        &|job| {
            assert_eq!(job, "pocket");
            fs::create_dir(&checkpoint_fault).unwrap();
            Ok(())
        },
        &mut interrupted_report,
    )
    .unwrap_err();
    let interrupted_report = String::from_utf8(interrupted_report).unwrap();
    assert!(
        interrupted_report.contains(&format!("cannot create {}", checkpoint_fault.display())),
        "{interrupted_report}"
    );
    assert_eq!(fs::read(&state_path).unwrap(), checkpoint_before);
    assert!(read_json(&state_path)["record"].is_null());
    assert!(state_path.parent().unwrap().join("migration-work").is_dir());
    let published_records = common::snapshot_tree(&records);
    let new_records: Vec<_> = published_records
        .iter()
        .filter(|(name, _)| !records_before.contains_key(*name))
        .collect();
    assert_eq!(
        new_records.len(),
        1,
        "only the verified record is published before checkpoint failure"
    );
    let (verified_name, verified_bytes) = new_records[0];
    let proof: Value = serde_json::from_slice(verified_bytes).unwrap();
    assert_eq!(proof["phase"], "verified");
    assert_eq!(proof["v2_root"], converted["dataset"]);
    assert_eq!(proof["v2_stream"], converted["stream"]);
    assert_eq!(
        proof["equality"],
        json!({"observations":true,"pages":true,"source_files":true,"candles":true})
    );
    let objects_before_resume = common::snapshot_tree(&store.join("objects"));
    fs::remove_dir(&checkpoint_fault).unwrap();
    let resumed = pipeline("migrate", &f.pipeline, &["--job", "pocket"]).unwrap();
    assert!(resumed.contains("status verified"), "{resumed}");
    let verified = read_json(&state_path);
    assert_eq!(verified["record"], verified_name.to_str().unwrap());
    assert_eq!(verified["dataset"], converted["dataset"]);
    assert_eq!(verified["stream"], converted["stream"]);
    assert_eq!(
        common::snapshot_tree(&records),
        published_records,
        "resume preserves the exact record without duplicates"
    );
    assert_eq!(
        common::snapshot_tree(&store.join("objects")),
        objects_before_resume
    );
    assert!(!state_path.parent().unwrap().join("migration-work").exists());
    let report = pipeline("migrate", &f.pipeline, &[]).unwrap();
    assert!(report.contains("status verified"), "{report}");
    assert_eq!(
        (f.deriv.requests().len(), f.pocket.requests().len()),
        broker_requests
    );
    assert_eq!(f.drive.log().len(), drive_requests, "migrate is offline");
    let mut all_recorded = Vec::new();
    for job in ["deriv", "pocket"] {
        let state = read_json(
            &f.scratch
                .path(&format!("producer/pipeline_state/{job}/migration.json")),
        );
        assert_eq!(state["phase"], "verified");
        let record = read_json(&records.join(state["record"].as_str().unwrap()));
        all_recorded.extend(
            record["v1_generations"]
                .as_array()
                .unwrap()
                .iter()
                .chain(record["v1_streams"].as_array().unwrap())
                .map(|v| v.as_str().unwrap().to_string()),
        );
        let manifest = dataset(&store, state["dataset"].as_str().unwrap());
        let (source_dataset, source_stream) = &source_outputs[job];
        let before = dataset(&store, source_dataset);
        // Decode full rows directly: this includes every Pocket provider column and repeated ticks.
        let observations = |m: &GenerationManifest| {
            let mut objects: Vec<_> = m
                .objects
                .iter()
                .filter(|o| {
                    o.path.starts_with("normalized/") || o.path.starts_with("observations/")
                })
                .collect();
            objects.sort_by_key(|o| &o.path);
            objects
                .into_iter()
                .flat_map(|o| migration_table_rows(&store.join(&o.key)))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            observations(&manifest),
            observations(&before),
            "{job}: observation sequence"
        );
        let before_stream = stream(&store, source_stream);
        let after_stream = stream(&store, state["stream"].as_str().unwrap());
        // The fixture spans Monday/Tuesday: every bucket is inside both the Deriv
        // FX week and the OTC always session. Daily candles fill the overnight
        // feed gap while preserving every original feed-candle field.
        let mut expected_streams = before_stream.streams.clone();
        for spec in &mut expected_streams {
            let start = binary_alpha_engine::market::parse_event_time_micros(
                spec.first_open_time.as_ref().unwrap(),
            )
            .unwrap();
            let end = binary_alpha_engine::market::parse_event_time_micros(
                spec.last_close_time.as_ref().unwrap(),
            )
            .unwrap();
            spec.rows = ((end - start) / (i64::from(spec.duration_seconds) * 1_000_000)) as u64;
        }
        assert_eq!(expected_streams, after_stream.streams);
        for spec in &before_stream.streams {
            let prefix = format!(
                "candles/{}s_{}s",
                spec.duration_seconds, spec.offset_seconds
            );
            let rows = |m: &StreamManifest| {
                let mut objects: Vec<_> = m
                    .objects
                    .iter()
                    .filter(|o| {
                        o.path == format!("{prefix}.parquet")
                            || o.path.starts_with(&format!("{prefix}/"))
                    })
                    .collect();
                objects.sort_by_key(|o| &o.path);
                objects
                    .into_iter()
                    .flat_map(|o| common::read_table(&store.join(&o.key)).1)
                    .collect::<Vec<_>>()
            };
            let after_rows = rows(&after_stream);
            let feed: Vec<_> = after_rows
                .iter()
                .filter(|row| row[10] != Some(binary_alpha_engine::features::Value::Int(0)))
                .map(|row| row[..33].to_vec())
                .collect();
            assert_eq!(feed, rows(&before_stream), "{job}: feed candle sequence");
            for adjacent in after_rows.windows(2) {
                let Some(binary_alpha_engine::features::Value::Time(open)) = adjacent[0][0] else {
                    panic!("candle timestamp");
                };
                assert_eq!(
                    adjacent[1][0],
                    Some(binary_alpha_engine::features::Value::Time(
                        open + i64::from(spec.duration_seconds) * 1_000_000
                    )),
                    "{job}: continuous candle sequence"
                );
            }
        }
        let pages: Vec<_> = manifest
            .day_inventory
            .iter()
            .filter(|d| d.family == DayFamily::Pages)
            .flat_map(|d| {
                daily::read_pages(&store.join(d.object.as_ref().unwrap()), &d.date).unwrap()
            })
            .collect();
        // Source receipts establish occurrence multiplicity; aliases and reported proof flags
        // cannot certify their own completeness. Replay copies contribute the maximum per intent.
        let mut expected = BTreeMap::new();
        for entry in fs::read_dir(&records).unwrap() {
            let path = entry.unwrap().path();
            if !path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains("-receipt-")
            {
                continue;
            }
            let receipt = read_json(&path);
            if receipt["coverage"]["provider_symbol"] != before.provider_symbol.as_str() {
                continue;
            }
            let mut invocation = BTreeMap::<String, usize>::new();
            for request in receipt["requests"].as_array().unwrap() {
                let raw = fs::read(store.join(binary_alpha_engine::dataset::object_key(
                    request["sha256"].as_str().unwrap(),
                )))
                .unwrap();
                let key = json!([
                    receipt["intent"],
                    request["anchor"],
                    request["receipt_time"],
                    request["rows"],
                    raw
                ])
                .to_string();
                *invocation.entry(key).or_default() += 1;
            }
            for (key, count) in invocation {
                let total = expected.entry(key).or_insert(0);
                *total = (*total).max(count);
            }
        }
        let mut actual = BTreeMap::new();
        for page in pages
            .iter()
            .filter(|p| p.order_kind == PageOrderKind::RequestOrder)
        {
            let key = json!([
                page.intent,
                page.request_token,
                page.receipt_time_utc.map(time_text),
                page.rows,
                page.payload
            ])
            .to_string();
            *actual.entry(key).or_insert(0) += 1;
        }
        assert_eq!(actual, expected, "{job}: request bytes and multiplicity");
        verify::run(&format!("file://{}", store.join(manifest.key()).display())).unwrap();
        verify::run(&format!(
            "file://{}",
            store
                .join(binary_alpha_engine::dataset::manifest_key(
                    state["stream"].as_str().unwrap()
                ))
                .display()
        ))
        .unwrap();
        if job == "deriv" {
            let aliases: Vec<Value> =
                fs::read_to_string(records.join(state["aliases"].as_str().unwrap()))
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect();
            let receipt_name = receipt.file_name().unwrap().to_str().unwrap();
            let alias = |label: &str| aliases.iter().find(|a| a["label"] == label).unwrap();
            let occurrence = |a: &Value| (a["acquisition_id"].clone(), a["ordinal"].clone());
            assert_eq!(
                occurrence(alias(&format!("receipt:{receipt_name}/0"))),
                occurrence(alias("receipt:legacy-deriv-receipt-replay.json/0")),
            );
            assert_ne!(
                occurrence(alias(&format!("receipt:{receipt_name}/0"))),
                occurrence(alias("receipt:legacy-deriv-receipt-distinct-time.json/0")),
            );
            assert_eq!(
                occurrence(alias(&format!(
                    "receipt:{receipt_name}/{}",
                    requests.len() - 1
                ))),
                occurrence(alias("receipt:legacy-deriv-receipt-subset.json/0")),
            );
            assert_eq!(
                occurrence(alias(&format!("coverage:{}/0", legacy.generation))),
                occurrence(alias(&format!(
                    "single:{}/raw/{}.json",
                    legacy.generation, p.sha256
                ))),
            );
            assert!(
                manifest
                    .day_inventory
                    .iter()
                    .any(|d| d.family == DayFamily::Observations
                        && d.date == "2025-08-10"
                        && d.state == DayState::EmptyKnown
                        && d.object.is_none())
            );
            assert!(
                manifest
                    .day_inventory
                    .iter()
                    .any(|d| d.family == DayFamily::Observations
                        && d.date == "2025-08-11"
                        && d.state == DayState::Unknown)
            );
            let cutoff = manifest
                .day_inventory
                .iter()
                .find(|d| d.family == DayFamily::Observations && d.date == "2025-08-12")
                .unwrap();
            assert_eq!(cutoff.state, DayState::Partial);
            assert!(
                cutoff
                    .reason
                    .as_deref()
                    .unwrap()
                    .contains("cutoff day may receive later input")
            );
            assert_eq!(
                cutoff.unresolved,
                vec![binary_alpha_engine::dataset::daily::UnresolvedInterval {
                    start: time_text((DERIV_SEED_END + 300) * 1_000_000),
                    end: time_text((DAY2 + 86_400) * 1_000_000),
                }]
            );
        } else {
            let head = manifest
                .day_inventory
                .iter()
                .find(|d| {
                    d.family == DayFamily::Observations
                        && d.date == time_text((midnight - 1) * 1_000_000)[..10]
                })
                .unwrap();
            assert_eq!(head.state, DayState::Partial);
            assert_eq!(head.unresolved.len(), 1);
            assert_eq!(
                head.unresolved[0].start,
                time_text((midnight - 86_400) * 1_000_000)
            );
            assert_eq!(
                head.unresolved[0].end,
                time_text((midnight - 10) * 1_000_000)
            );
            let tail = manifest
                .day_inventory
                .iter()
                .find(|d| {
                    d.family == DayFamily::Observations
                        && d.date == time_text(midnight * 1_000_000)[..10]
                })
                .unwrap();
            assert_eq!(tail.state, DayState::Partial);
            assert_eq!(tail.unresolved.len(), 1);
            assert_eq!(
                tail.unresolved[0].start,
                time_text((midnight + 35) * 1_000_000),
                "provider shortfall before cutoff remains unresolved"
            );
            let mut pages = Vec::new();
            for day in manifest
                .day_inventory
                .iter()
                .filter(|d| d.family == DayFamily::Pages)
            {
                pages.extend(
                    daily::read_pages(&store.join(day.object.as_ref().unwrap()), &day.date)
                        .unwrap(),
                );
            }
            let mut imported: Vec<_> = pages
                .iter()
                .filter(|p| p.order_kind == PageOrderKind::SourceFileOrder)
                .collect();
            imported.sort_by_key(|p| p.ordinal);
            let aliases: Vec<Value> =
                fs::read_to_string(records.join(state["aliases"].as_str().unwrap()))
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect();
            let reconstruct = |checkpoint: bool| {
                let mut ordered = imported.clone();
                ordered.sort_by_key(|p| {
                    if checkpoint {
                        p.checkpoint_ordinal.unwrap()
                    } else {
                        p.ordinal
                    }
                });
                let mut bytes = Vec::new();
                for (ordinal, page) in ordered.into_iter().enumerate() {
                    assert_eq!(
                        if checkpoint {
                            page.checkpoint_ordinal.unwrap()
                        } else {
                            page.ordinal
                        },
                        ordinal as u64
                    );
                    let alias = aliases
                        .iter()
                        .find(|a| {
                            a["acquisition_id"] == page.acquisition_id
                                && a["ordinal"] == page.ordinal
                        })
                        .unwrap();
                    let (source, suffix, chunk) = if checkpoint {
                        (
                            &alias["checkpoint"],
                            &alias["checkpoint_suffix"],
                            page.checkpoint.as_deref().unwrap(),
                        )
                    } else {
                        (
                            &alias["source"],
                            &alias["raw_suffix"],
                            page.payload.as_slice(),
                        )
                    };
                    assert_eq!(
                        source["offset"].as_u64().unwrap(),
                        bytes.len() as u64,
                        "source offsets must reconstruct contiguous bytes"
                    );
                    bytes.extend_from_slice(chunk);
                    bytes.extend(serde_json::from_value::<Vec<u8>>(suffix.clone()).unwrap());
                }
                bytes
            };
            assert_eq!(
                reconstruct(false),
                raw_bytes.as_bytes(),
                "raw source bytes including recorded framing"
            );
            assert_eq!(
                reconstruct(true),
                cp_bytes.as_bytes(),
                "checkpoint source bytes including recorded framing"
            );
            assert_eq!(imported.len(), 3);
            assert_eq!(imported[0].checkpoint_ordinal, Some(1));
            assert_eq!(imported[1].checkpoint_ordinal, Some(0));
            assert_eq!(imported[0].receipt_state, ReceiptState::NotRecordedBySource);
            assert_eq!(
                imported[0].first_event_time,
                Some((midnight - 5) * 1_000_000)
            );
            assert_eq!(imported[0].last_event_time, Some(midnight * 1_000_000));
            assert_eq!(imported[1].rows, 0);
            assert_eq!(
                imported[1].request_anchor_utc,
                Some((midnight + 30) * 1_000_000)
            );
            assert_eq!(
                record["proofs"]["import_files"].as_array().unwrap().len(),
                2
            );
        }
        println!(
            "migration fixture {job}: peak_day_rows={} peak_day_payload_bytes={}",
            state["peak_day_rows"], state["peak_day_payload_bytes"]
        );
    }
    for generation in before_generations {
        assert!(
            all_recorded.contains(&generation),
            "missing v1 generation {generation}"
        );
    }
    assert!(all_recorded.contains(&legacy.generation));
    let record_count = fs::read_dir(&records).unwrap().count();
    let before_manifests = binary_alpha_app::store::Store::filesystem(&store)
        .list_manifests()
        .unwrap();
    let before_states: Vec<_> = ["deriv", "pocket"]
        .map(|job| {
            fs::read(
                f.scratch
                    .path(&format!("producer/pipeline_state/{job}/migration.json")),
            )
            .unwrap()
        })
        .into();
    let rerun = pipeline("migrate", &f.pipeline, &[]).unwrap();
    assert_eq!(rerun.matches("status already_verified").count(), 2);
    assert_eq!(record_count, fs::read_dir(records).unwrap().count());
    assert_eq!(
        before_manifests,
        binary_alpha_app::store::Store::filesystem(&store)
            .list_manifests()
            .unwrap()
    );
    for (job, before) in ["deriv", "pocket"].into_iter().zip(before_states) {
        assert_eq!(
            before,
            fs::read(
                f.scratch
                    .path(&format!("producer/pipeline_state/{job}/migration.json"))
            )
            .unwrap()
        );
    }
}

#[test]
fn pipeline_migration_parallel_jobs_are_deterministic_and_isolate_failures() {
    use std::sync::Condvar;

    fn copy_tree(source: &Path, destination: &Path) {
        fs::create_dir_all(destination).unwrap();
        for entry in fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let target = destination.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                fs::copy(entry.path(), target).unwrap();
            }
        }
    }

    let f = fixture("migration_parallel_jobs");
    for job in ["deriv", "pocket"] {
        lineage::legacy_import(&f.scratch.path(&format!("{job}-import.toml"))).unwrap();
    }
    let mut identities = BTreeMap::new();
    for (mode, workers) in [("serial", 1), ("parallel", 2), ("failure", 2)] {
        let root = f.scratch.path(mode);
        copy_tree(&f.scratch.path("producer/store"), &root.join("store"));
        let config = f.scratch.path(&format!("{mode}.toml"));
        let mut document: data_pipeline::PipelineConfig =
            toml::from_str(&fs::read_to_string(&f.pipeline).unwrap()).unwrap();
        document.local_root = root.clone();
        document.parallel_jobs = Some(workers);
        fs::write(&config, toml::to_string(&document).unwrap()).unwrap();
        let report = if mode == "serial" {
            pipeline("migrate", &config, &[]).unwrap()
        } else {
            let arrived = Mutex::new(0);
            let ready = Condvar::new();
            let mut report = Vec::new();
            let result = data_pipeline::migrate_with(
                &config,
                None,
                &|job| {
                    // Both conversions must finish before either hook returns. A timeout
                    // makes a serial scheduler regression fail without hanging the suite.
                    let mut count = arrived.lock().unwrap();
                    *count += 1;
                    ready.notify_all();
                    let (count, _) = ready
                        .wait_timeout_while(count, Duration::from_secs(30), |n| *n < 2)
                        .unwrap();
                    if *count != 2 {
                        return Err("fixture jobs did not overlap".into());
                    }
                    if mode == "failure" && job == "deriv" {
                        return Err("fixture conversion interruption".into());
                    }
                    Ok(())
                },
                &mut report,
            );
            if mode == "failure" {
                assert_eq!(result.unwrap_err(), "pipeline: 1 job(s) failed: deriv");
            } else {
                result.unwrap();
            }
            String::from_utf8(report).unwrap()
        };
        assert_eq!(report.lines().count(), 2, "{report}");
        for job in ["deriv", "pocket"] {
            let state = read_json(&root.join(format!("pipeline_state/{job}/migration.json")));
            let line = job_line(&report, job);
            if mode == "failure" && job == "deriv" {
                assert_eq!(state["phase"], "converted");
                assert!(state["record"].is_null());
                assert!(
                    line.contains("failed: fixture conversion interruption"),
                    "{line}"
                );
            } else {
                assert_eq!(state["phase"], "verified");
                assert_eq!(field(line, "status"), "verified");
                let record: binary_alpha_app::lineage::MigrationRecord =
                    serde_json::from_value(read_json(
                        &root
                            .join("pipeline_state/records")
                            .join(state["record"].as_str().unwrap()),
                    ))
                    .unwrap();
                assert!(record.verified());
            }
            let generations = (state["dataset"].clone(), state["stream"].clone());
            if mode == "serial" {
                identities.insert(job, generations);
            } else {
                assert_eq!(generations, identities[job], "{mode} {job}");
            }
        }
        if mode == "failure" {
            let resumed = pipeline("migrate", &config, &[]).unwrap();
            assert_eq!(field(job_line(&resumed, "deriv"), "status"), "verified");
            assert_eq!(
                field(job_line(&resumed, "pocket"), "status"),
                "already_verified"
            );
        }
    }
    assert!(f.deriv.requests().is_empty());
    assert!(f.pocket.requests().is_empty());
    assert!(f.drive.log().is_empty());
}

#[test]
fn pipeline_migration_import_only_and_writer_lock() {
    use binary_alpha_engine::dataset::daily::{DayFamily, DayState};
    let f = fixture("migration_import_only");
    let store = f.scratch.path("producer/store");
    let meta = f
        .scratch
        .path("sources/deriv/EURUSD/EURUSD_2025-08-11_ticks.meta.json");
    let mut clipped = read_json(&meta);
    clipped["market_closed"] = json!(false);
    clipped["clipped_by_retention"] = json!(true);
    fs::write(meta, clipped.to_string()).unwrap();
    let cutoff_meta = f
        .scratch
        .path("sources/deriv/EURUSD/EURUSD_2025-08-12_ticks.meta.json");
    let mut gap = read_json(&cutoff_meta);
    gap["complete"] = json!(false);
    fs::write(cutoff_meta, gap.to_string()).unwrap();
    lineage::legacy_import(&f.scratch.path("deriv-import.toml")).unwrap();
    let lock = File::create(f.scratch.path("producer/pipeline_state/writer.lock"));
    // The pipeline state directory is normally first created by the command.
    if lock.is_err() {
        fs::create_dir_all(f.scratch.path("producer/pipeline_state")).unwrap();
    }
    let lock = File::create(f.scratch.path("producer/pipeline_state/writer.lock")).unwrap();
    lock.lock().unwrap();
    let denied = pipeline("migrate", &f.pipeline, &["--job", "deriv"]).unwrap_err();
    assert!(denied.contains("another producer"), "{denied}");
    drop(lock);
    let report = pipeline("migrate", &f.pipeline, &["--job", "deriv"]).unwrap();
    assert!(report.contains("pages 0"), "{report}");
    assert!(report.contains("candles_equal not_applicable"), "{report}");
    let state = read_json(
        &f.scratch
            .path("producer/pipeline_state/deriv/migration.json"),
    );
    let record: binary_alpha_app::lineage::MigrationRecord = serde_json::from_value(read_json(
        &f.scratch
            .path("producer/pipeline_state/records")
            .join(state["record"].as_str().unwrap()),
    ))
    .unwrap();
    assert!(record.verified());
    assert!(record.mapping.streams().is_empty());
    assert_eq!(record.equality.candles, None);
    let m = dataset(&store, state["dataset"].as_str().unwrap());
    assert!(
        m.day_inventory
            .iter()
            .any(|d| d.family == DayFamily::Observations
                && d.date == "2025-08-11"
                && d.state == DayState::Unknown)
    );
    let cutoff_day = m
        .day_inventory
        .iter()
        .find(|d| d.family == DayFamily::Observations && d.date == "2025-08-12")
        .unwrap();
    assert_eq!(cutoff_day.state, DayState::Unknown);
    assert!(
        cutoff_day
            .reason
            .as_ref()
            .unwrap()
            .contains("complete=false")
    );
    assert_eq!(cutoff_day.unresolved[0].start, time_text(DAY2 * 1_000_000));
    assert_eq!(
        cutoff_day.unresolved[0].end,
        time_text((DAY2 + 86_400) * 1_000_000)
    );
    assert!(f.deriv.requests().is_empty());
    assert!(f.pocket.requests().is_empty());
    assert!(f.drive.log().is_empty());
}

#[test]
fn pipeline_migration_carried_legacy_occurrences() {
    migration_carried_legacy_occurrences(false);
}

#[test]
fn pipeline_migration_legacy_requests_with_retained_anchor() {
    migration_carried_legacy_occurrences(true);
}

fn migration_carried_legacy_occurrences(retain_anchor: bool) {
    use binary_alpha_app::daily::{self, ReceiptState};
    use binary_alpha_engine::dataset::daily::DayFamily;
    use binary_alpha_engine::dataset::{PriceRepresentation, generation_id, object_key};
    use binary_alpha_engine::market::InstrumentId;
    let f = fixture(if retain_anchor {
        "migration_legacy_with_anchor"
    } else {
        "migration_carried_legacy"
    });
    import(&f.scratch.path("deriv-import.toml")).unwrap();
    let config = f.scratch.path("legacy-only.toml");
    fs::write(
        &config,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("deriv", "deriv.toml")],
            None,
            2,
        ),
    )
    .unwrap();
    let report = pipeline(
        "update",
        &config,
        &["--end", &time_text((DERIV_SEED_END + 200) * 1_000_000)],
    )
    .unwrap();
    let mapping = legacy_fixtures::freeze(&f);
    let mut report = report;
    for (a, b) in mapping {
        report = report.replace(&a, &b);
    }
    let store = f.scratch.path("producer/store");
    let mut original = dataset(&store, field(job_line(&report, "deriv"), "dataset"));
    let mut cov = coverage(&store, &original);
    let mut legacy = cov.pages[0].clone();
    let hash = legacy.sha256.clone();
    let mut single = original
        .objects
        .iter()
        .find(|o| o.path == legacy.path)
        .unwrap()
        .clone();
    single.path = format!("raw/{hash}.json");
    single.key = object_key(&hash);
    single.sha256 = hash.clone();
    single.bytes = legacy.bytes;
    single.crc32c = None;
    single.generation = None;
    legacy.path = single.path.clone();
    legacy.offset = None;
    legacy.receipt_time = None;
    if !retain_anchor {
        legacy.anchor = None;
    }
    original.objects.push(single);
    // Two original requests can have identical bytes and absent receipt metadata. Both
    // occurrences are carried in order into a later generation and must remain exactly two.
    cov.pages.splice(0..0, [legacy.clone(), legacy]);
    let mut generations = Vec::new();
    for extra in [0, 1] {
        let mut manifest = original.clone();
        cov.requested.end = time_text((DERIV_SEED_END + 200 + extra) * 1_000_000);
        let bytes = serde_json::to_vec(&cov).unwrap();
        let object = manifest
            .objects
            .iter_mut()
            .find(|o| o.path == "provenance/coverage.json")
            .unwrap();
        object.sha256 = binary_alpha_engine::hex(&Sha256::digest(&bytes));
        object.bytes = bytes.len() as u64;
        object.key = object_key(&object.sha256);
        object.crc32c = None;
        object.generation = None;
        fs::write(store.join(&object.key), bytes).unwrap();
        manifest.generation = generation_id(
            &InstrumentId {
                broker: manifest.broker.clone(),
                provider_symbol: manifest.provider_symbol.clone(),
            },
            manifest.source_kind,
            manifest.role,
            match manifest.price_representation {
                PriceRepresentation::IntegerUnits { scale } => Some(scale),
                _ => None,
            },
            &manifest.objects,
        );
        GenerationManifest::from_json(&manifest.to_json()).unwrap();
        let path = store.join(manifest.key());
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, manifest.to_json()).unwrap();
        generations.push(manifest.generation);
    }
    pipeline("migrate", &config, &[]).unwrap();
    let state = read_json(
        &f.scratch
            .path("producer/pipeline_state/deriv/migration.json"),
    );
    let manifest = dataset(&store, state["dataset"].as_str().unwrap());
    let pages: Vec<_> = manifest
        .day_inventory
        .iter()
        .filter(|d| d.family == DayFamily::Pages)
        .flat_map(|d| daily::read_pages(&store.join(d.object.as_ref().unwrap()), &d.date).unwrap())
        .collect();
    let raw = fs::read(store.join(object_key(&hash))).unwrap();
    let expected = (
        raw,
        ReceiptState::AbsentInLegacyRecord,
        None,
        cov.pages[0].anchor.clone(),
    );
    assert_eq!(
        pages
            .iter()
            .filter(|p| p.payload_sha256 == hash
                && p.receipt_state == ReceiptState::AbsentInLegacyRecord)
            .map(|p| (
                p.payload.clone(),
                p.receipt_state,
                p.receipt_time_utc,
                p.request_token.clone()
            ))
            .collect::<Vec<_>>(),
        vec![expected.clone(), expected]
    );
    let aliases: Vec<Value> = fs::read_to_string(
        f.scratch
            .path("producer/pipeline_state/records")
            .join(state["aliases"].as_str().unwrap()),
    )
    .unwrap()
    .lines()
    .map(|line| serde_json::from_str(line).unwrap())
    .collect();
    let mut legacy_targets = Vec::new();
    for ordinal in [0, 1] {
        let targets: Vec<_> = generations
            .iter()
            .map(|g| {
                let a = aliases
                    .iter()
                    .find(|a| a["label"] == format!("coverage:{g}/{ordinal}"))
                    .unwrap();
                (a["acquisition_id"].clone(), a["ordinal"].clone())
            })
            .collect();
        assert_eq!(
            targets[0], targets[1],
            "carried prefix keeps occurrence identity"
        );
        let row = pages
            .iter()
            .find(|p| json!(p.acquisition_id) == targets[0].0 && json!(p.ordinal) == targets[0].1)
            .unwrap();
        assert_eq!(
            row.payload,
            fs::read(store.join(object_key(&hash))).unwrap()
        );
        assert_eq!(row.receipt_state, ReceiptState::AbsentInLegacyRecord);
        assert_eq!(row.receipt_time_utc, None);
        assert_eq!(
            row.request_token,
            if retain_anchor {
                cov.pages[0].anchor.clone()
            } else {
                None
            }
        );
        legacy_targets.push(targets[0].clone());
    }
    assert_ne!(
        legacy_targets[0], legacy_targets[1],
        "distinct legacy requests retain multiplicity"
    );
    for row in pages
        .iter()
        .filter(|p| p.payload_sha256 == hash && p.receipt_state == ReceiptState::Recorded)
    {
        assert!(!legacy_targets.contains(&(json!(row.acquisition_id), json!(row.ordinal))));
    }
    assert!(
        pages
            .iter()
            .any(|p| p.payload_sha256 == hash && p.receipt_state == ReceiptState::Recorded),
        "independent modern request using the same payload stays separate"
    );
}

#[test]
fn pipeline_migration_pending_receipt_is_diagnostic() {
    use binary_alpha_app::daily::{self, PageDisposition};
    use binary_alpha_engine::dataset::daily::DayFamily;
    let f = fixture("migration_pending");
    import(&f.scratch.path("deriv-import.toml")).unwrap();
    let core = deriv_core(&f.deriv.url, 60, 1, 60);
    fs::write(f.scratch.path("deriv.toml"), &core).unwrap();
    write_evidence(&f.scratch, "deriv", &core);
    let config = f.scratch.path("pending-only.toml");
    fs::write(
        &config,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("deriv", "deriv.toml")],
            None,
            2,
        ),
    )
    .unwrap();
    let pending = pipeline(
        "update",
        &config,
        &["--end", &time_text((DERIV_SEED_END + 1000) * 1_000_000)],
    )
    .unwrap_err();
    assert!(pending.contains("status pending"), "{pending}");
    legacy_fixtures::freeze(&f);
    let header = f
        .scratch
        .path("producer/pipeline_state/deriv/progress.json");
    let before = fs::read(&header).unwrap();
    let requests = f.deriv.requests().len();
    // A sibling finishes first and publishes a Pending copy whose valid job id contains
    // the receipt marker. That copy must not enter the next job's immutable source census:
    // otherwise serial and parallel schedules could produce different lineage identities.
    let mut document: data_pipeline::PipelineConfig =
        toml::from_str(&fs::read_to_string(&config).unwrap()).unwrap();
    document.parallel_jobs = Some(1);
    document.jobs.insert(
        0,
        data_pipeline::Job {
            id: "first-receipt-job".into(),
            config: "deriv.toml".into(),
            evidence: "evidence/deriv.json".into(),
        },
    );
    fs::write(&config, toml::to_string(&document).unwrap()).unwrap();
    pipeline("migrate", &config, &[]).unwrap();
    assert_eq!(f.deriv.requests().len(), requests);
    assert_eq!(fs::read(header).unwrap(), before);
    let state = read_json(
        &f.scratch
            .path("producer/pipeline_state/deriv/migration.json"),
    );
    let store = f.scratch.path("producer/store");
    let m = dataset(&store, state["dataset"].as_str().unwrap());
    let lineage_object = m
        .objects
        .iter()
        .find(|o| o.path == "provenance/lineage.json")
        .unwrap();
    let lineage = read_json(&store.join(&lineage_object.key));
    let records = lineage["records"].as_array().unwrap();
    assert!(records.iter().any(|r| r["kind"] == "pending"));
    assert!(
        !records.iter().any(|r| {
            r["kind"] == "operation_receipt"
                && r["name"].as_str().unwrap().contains("-migration-pending-")
        }),
        "{records:?}"
    );
    let mut pages = Vec::new();
    for day in m
        .day_inventory
        .iter()
        .filter(|d| d.family == DayFamily::Pages)
    {
        pages.extend(
            daily::read_pages(&store.join(day.object.as_ref().unwrap()), &day.date).unwrap(),
        );
    }
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].disposition, PageDisposition::Diagnostic);
    assert!(pages[0].acquisition_id.contains("-receipt-"));
    assert!(pages[0].intent.is_some());
}

#[test]
fn pipeline_migration_checkpoint_only_import_is_unresolved() {
    let f = fixture("review_checkpoint_only");
    let root = f.scratch.path("sources/pocket/AEDCNY_otc");
    let report = lineage::legacy_import(&f.scratch.path("pocket-import.toml")).unwrap();
    let store = f.scratch.path("producer/store");
    let mut manifest = dataset(
        &store,
        imported_generation(&report, "pocket_option:AEDCNY_otc"),
    );
    fs::remove_dir_all(store.join(manifest.key()).parent().unwrap()).unwrap();
    manifest
        .objects
        .retain(|o| o.path != "raw_pages.ndjson" && o.path != "checkpoint.ndjson");
    fs::write(root.join("checkpoint.ndjson"), json!({"payload_sha256":"0".repeat(64),"target_server_s":POCKET_START+7200,"rows":1,"recovered_from_raw":true}).to_string()+"\n").unwrap();
    manifest.objects.push(common::daily::object(
        &store,
        "checkpoint.ndjson",
        binary_alpha_engine::dataset::ObjectRole::Provenance,
        &root.join("checkpoint.ndjson"),
    ));
    migration_publish_v1(&store, &mut manifest);
    let result = pipeline("migrate", &f.pipeline, &["--job", "pocket"]);
    let error = result.expect_err("checkpoint entry must not disappear");
    assert!(
        error.contains("checkpoint.ndjson has no raw_pages.ndjson"),
        "{error}"
    );
    assert!(
        !f.scratch
            .path("producer/pipeline_state/pocket/migration.json")
            .exists()
    );
}

fn migration_publish_v1(store: &Path, manifest: &mut GenerationManifest) {
    use binary_alpha_engine::dataset::{PriceRepresentation, generation_id};
    use binary_alpha_engine::market::InstrumentId;
    manifest.generation = generation_id(
        &InstrumentId {
            broker: manifest.broker.clone(),
            provider_symbol: manifest.provider_symbol.clone(),
        },
        manifest.source_kind,
        manifest.role,
        match manifest.price_representation {
            PriceRepresentation::IntegerUnits { scale } => Some(scale),
            _ => None,
        },
        &manifest.objects,
    );
    GenerationManifest::from_json(&manifest.to_json()).unwrap();
    let path = store.join(manifest.key());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, manifest.to_json()).unwrap();
}

#[test]
fn pipeline_migration_coverage_bounds_must_match_payload() {
    for rows_mismatch in [false, true] {
        migration_coverage_payload_mismatch(rows_mismatch);
    }
}

fn migration_coverage_payload_mismatch(rows_mismatch: bool) {
    use binary_alpha_engine::dataset::object_key;
    let f = fixture("review_coverage_day");
    import(&f.scratch.path("deriv-import.toml")).unwrap();
    let config = f.scratch.path("only.toml");
    fs::write(
        &config,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("deriv", "deriv.toml")],
            None,
            2,
        ),
    )
    .unwrap();
    let report = pipeline(
        "update",
        &config,
        &["--end", &time_text((DERIV_SEED_END + 200) * 1_000_000)],
    )
    .unwrap();
    let mapping = legacy_fixtures::freeze(&f);
    let mut report = report;
    for (a, b) in mapping {
        report = report.replace(&a, &b);
    }
    let store = f.scratch.path("producer/store");
    let mut m = dataset(&store, field(job_line(&report, "deriv"), "dataset"));
    let mut c = coverage(&store, &m);
    let old_generation = m.generation.clone();
    // Old legacy coverage can be internally self-consistent while disagreeing with provider bytes.
    for p in &mut c.pages {
        p.receipt_time = None;
        if rows_mismatch {
            p.rows += 1;
        } else {
            for t in [&mut p.first, &mut p.last].into_iter().flatten() {
                *t = time_text(
                    binary_alpha_engine::market::parse_event_time_micros(t).unwrap()
                        + 86_400_000_000,
                );
            }
        }
    }
    let bytes = serde_json::to_vec(&c).unwrap();
    let o = m
        .objects
        .iter_mut()
        .find(|o| o.path == "provenance/coverage.json")
        .unwrap();
    o.sha256 = binary_alpha_engine::hex(&Sha256::digest(&bytes));
    o.bytes = bytes.len() as u64;
    o.key = object_key(&o.sha256);
    o.crc32c = None;
    o.generation = None;
    fs::write(store.join(&o.key), bytes).unwrap();
    // Remove modern receipt/stream evidence to model a legacy-only generation.
    let local = binary_alpha_app::store::Store::filesystem(&store);
    for g in local.list_manifests().unwrap() {
        let path = store.join(binary_alpha_engine::dataset::manifest_key(&g));
        let v = read_json(&path);
        if v["kind"] == "instrument_stream" || g == old_generation {
            fs::remove_file(path).unwrap();
        }
    }
    for entry in fs::read_dir(f.scratch.path("producer/pipeline_state/records")).unwrap() {
        let p = entry.unwrap().path();
        if p.file_name()
            .unwrap()
            .to_string_lossy()
            .contains("-receipt-")
        {
            fs::remove_file(p).unwrap();
        }
    }
    migration_publish_v1(&store, &mut m);
    let error = pipeline("migrate", &config, &[])
        .expect_err("provider bounds must be validated before partitioning");
    assert!(
        error.contains("coverage:") && error.contains("payload rows/event bounds mismatch"),
        "{error}"
    );
    assert!(
        !f.scratch
            .path("producer/pipeline_state/deriv/migration.json")
            .exists()
    );
}

#[test]
fn pipeline_migration_receipt_without_coverage_preserves_request() {
    for case in [
        "bound",
        "unbound",
        "ambiguous",
        "conflicting",
        "unrelated",
        "late",
    ] {
        migration_receipt_without_coverage(case);
    }
}

fn migration_receipt_without_coverage(case: &str) {
    let f = fixture("review_receipt_no_coverage");
    import(&f.scratch.path("deriv-import.toml")).unwrap();
    let config = f.scratch.path("only.toml");
    fs::write(
        &config,
        pipeline_toml(
            &f.scratch.path("producer"),
            &f.drive.base,
            &[("deriv", "deriv.toml")],
            None,
            2,
        ),
    )
    .unwrap();
    pipeline(
        "update",
        &config,
        &["--end", &time_text((DERIV_SEED_END + 200) * 1_000_000)],
    )
    .unwrap();
    legacy_fixtures::freeze(&f);
    let records = f.scratch.path("producer/pipeline_state/records");
    let receipt_path = fs::read_dir(&records)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("deriv-receipt-")
        })
        .unwrap();
    let mut v = read_json(&receipt_path);
    let mut req = v["requests"][0].clone();
    let received =
        binary_alpha_engine::market::parse_event_time_micros(req["receipt_time"].as_str().unwrap())
            .unwrap();
    req["receipt_time"] = json!(time_text(received + 1_000_000));
    v["requests"] = json!([req.clone()]);
    v["coverage"] = Value::Null;
    v["dataset_generation"] = Value::Null;
    v["stream_generation"] = Value::Null;
    v["catalog"] = Value::Null;
    v["status"] = json!("no_data");
    let name = "deriv-receipt-without-coverage.json";
    if matches!(case, "unbound" | "ambiguous" | "conflicting" | "unrelated") {
        let mut intent = read_json(&records.join(v["intent"].as_str().unwrap()));
        match case {
            "unbound" => intent["seeds"] = json!([]),
            "conflicting" => intent["seeds"][0]["source_identity"] = json!("another-context"),
            "ambiguous" => {
                let mut other = intent["seeds"][0].clone();
                other["provider_symbol"] = json!("OTHER");
                intent["seeds"].as_array_mut().unwrap().push(other);
            }
            "unrelated" => intent["seeds"][0]["provider_symbol"] = json!("OTHER"),
            _ => unreachable!(),
        }
        let name = "fixture-null-coverage-intent.json";
        fs::write(records.join(name), intent.to_string()).unwrap();
        v["intent"] = json!(name);
    }
    if case == "late" {
        let mut report = Vec::new();
        let stopped = data_pipeline::migrate_with(
            &config,
            Some("deriv"),
            &|_| Err("stop after converted".into()),
            &mut report,
        )
        .unwrap_err();
        assert_eq!(stopped, "pipeline: 1 job(s) failed: deriv");
        assert!(
            String::from_utf8(report)
                .unwrap()
                .contains("stop after converted")
        );
    }
    fs::write(records.join(name), v.to_string()).unwrap();
    let result = pipeline("migrate", &config, &[]);
    if matches!(case, "unbound" | "ambiguous" | "conflicting" | "late") {
        let error = result.expect_err("unaccounted receipt must prevent verified");
        let expected = if case == "late" {
            "missing source alias receipt:deriv-receipt-without-coverage.json/0"
        } else if case == "unbound" {
            "intent has no source binding"
        } else if case == "conflicting" {
            "receipt source identity mismatch"
        } else {
            "intent binds multiple sources"
        };
        assert!(error.contains(expected), "{case}: {error}");
        return;
    }
    result.unwrap();
    let state = read_json(
        &f.scratch
            .path("producer/pipeline_state/deriv/migration.json"),
    );
    let aliases = fs::read_to_string(records.join(state["aliases"].as_str().unwrap())).unwrap();
    let alias = aliases
        .lines()
        .map(|s| serde_json::from_str::<Value>(s).unwrap())
        .find(|v| v["label"] == format!("receipt:{name}/0"));
    if case == "unrelated" {
        assert!(
            alias.is_none(),
            "another instrument's request must not enter this migration"
        );
        return;
    }
    let alias = alias.expect("receipt with null coverage must retain its request");
    let store = f.scratch.path("producer/store");
    let m = dataset(&store, state["dataset"].as_str().unwrap());
    let pages: Vec<_> = m
        .day_inventory
        .iter()
        .filter(|d| d.family == binary_alpha_engine::dataset::daily::DayFamily::Pages)
        .flat_map(|d| {
            binary_alpha_app::daily::read_pages(&store.join(d.object.as_ref().unwrap()), &d.date)
                .unwrap()
        })
        .collect();
    let row = pages
        .iter()
        .find(|p| {
            json!(p.acquisition_id) == alias["acquisition_id"]
                && json!(p.ordinal) == alias["ordinal"]
        })
        .unwrap();
    assert_eq!(
        row.payload,
        fs::read(store.join(binary_alpha_engine::dataset::object_key(
            req["sha256"].as_str().unwrap()
        )))
        .unwrap()
    );
    assert_eq!(row.receipt_time_utc, Some(received + 1_000_000));
    assert_eq!(row.intent.as_deref(), v["intent"].as_str());
    assert_eq!(json!(row.request_token), req["anchor"]);
    assert_eq!(json!(row.rows), req["rows"]);
}

#[test]
fn pipeline_migration_pending_bounds_must_match_payload() {
    for rows_mismatch in [false, true] {
        let f = fixture("migration_pending_bounds");
        import(&f.scratch.path("deriv-import.toml")).unwrap();
        let core = deriv_core(&f.deriv.url, 60, 1, 60);
        fs::write(f.scratch.path("deriv.toml"), &core).unwrap();
        write_evidence(&f.scratch, "deriv", &core);
        let config = f.scratch.path("pending-only.toml");
        fs::write(
            &config,
            pipeline_toml(
                &f.scratch.path("producer"),
                &f.drive.base,
                &[("deriv", "deriv.toml")],
                None,
                2,
            ),
        )
        .unwrap();
        let error = pipeline(
            "update",
            &config,
            &["--end", &time_text((DERIV_SEED_END + 1000) * 1_000_000)],
        )
        .unwrap_err();
        assert!(error.contains("status pending"), "{error}");
        legacy_fixtures::freeze(&f);
        // Retain only the pending evidence, so a modern receipt cannot supply the bounds.
        let records = f.scratch.path("producer/pipeline_state/records");
        for entry in fs::read_dir(&records).unwrap() {
            let path = entry.unwrap().path();
            if path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains("-receipt-")
            {
                fs::remove_file(path).unwrap();
            }
        }
        let log = f
            .scratch
            .path("producer/pipeline_state/deriv/progress.pages.jsonl");
        let original = fs::read_to_string(&log).unwrap();
        let mut pages: Vec<Value> = original
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        for page in &mut pages {
            page["receipt_time"] = Value::Null;
            if rows_mismatch {
                page["rows"] = json!(page["rows"].as_u64().unwrap() + 1);
            } else {
                for key in ["first", "last"] {
                    page[key] = json!(time_text(
                        binary_alpha_engine::market::parse_event_time_micros(
                            page[key].as_str().unwrap()
                        )
                        .unwrap()
                            + 86_400_000_000
                    ));
                }
            }
        }
        let bytes = pages.iter().map(|p| format!("{p}\n")).collect::<String>();
        fs::write(&log, &bytes).unwrap();
        let error = pipeline("migrate", &config, &[])
            .expect_err("pending payload metadata must be validated before partitioning");
        assert!(
            error.contains("pending:") && error.contains("payload rows/event bounds mismatch"),
            "{error}"
        );
        assert_eq!(
            fs::read_to_string(log).unwrap(),
            bytes,
            "unresolved pending evidence is retained"
        );
        assert!(
            !f.scratch
                .path("producer/pipeline_state/deriv/migration.json")
                .exists()
        );
    }
}
#[path = "data_pipeline/daily_review_fixes.rs"]
mod daily_review_fixes;
#[path = "data_pipeline/registry_archive.rs"]
mod registry_archive;
#[path = "data_pipeline/retire.rs"]
mod retire;

#[path = "data_pipeline/archive_records.rs"]
mod archive_records;
#[path = "data_pipeline/daily_end_to_end.rs"]
mod daily_end_to_end;

#[path = "data_pipeline/legacy_fixtures.rs"]
mod legacy_fixtures;

fn assert_archive_inventory(
    drive: &FakeDrive,
    store: &Path,
    records: &Path,
    newest_catalogs: &[&str],
) {
    use std::collections::BTreeSet;
    let files = drive.files();
    // Derive archived instruments from the producer's immutable receipts and local manifests,
    // never from the catalog whose completeness is under test.
    let local_records = common::snapshot_tree(records);
    let manifests: Vec<_> = binary_alpha_app::store::Store::filesystem(store)
        .list_manifests()
        .unwrap()
        .into_iter()
        .map(|g| {
            let key = binary_alpha_engine::dataset::manifest_key(&g);
            (g, key.clone(), read_json(&store.join(key)))
        })
        .collect();
    let mut instruments = BTreeSet::new();
    let mut jobs = BTreeSet::new();
    let assert_remote = |label: &str, bytes: &[u8], length: u64, digest: &str| {
        assert_eq!(bytes.len() as u64, length, "local identity {label}");
        assert_eq!(
            binary_alpha_engine::hex(&Sha256::digest(bytes)),
            digest,
            "local identity {label}"
        );
        assert!(
            files
                .values()
                .any(|entry| !entry.trashed && entry.bytes == bytes),
            "producer closure missing from Drive: {label} ({digest})"
        );
    };
    for (name, bytes) in &local_records {
        let name = name.to_str().unwrap();
        if let Some((job, suffix)) = name.split_once("-catalog-") {
            let receipt: data_pipeline::CatalogReceipt = serde_json::from_slice(bytes).unwrap();
            let remote = &files[&receipt.file_id];
            assert!(!remote.trashed);
            assert_eq!(remote.bytes.len() as u64, receipt.bytes);
            assert_eq!(
                binary_alpha_engine::hex(&Sha256::digest(&remote.bytes)),
                receipt.sha256
            );
            jobs.insert(job.to_string());
            let matches: Vec<_> = manifests
                .iter()
                .filter(|(g, _, _)| suffix.starts_with(&g[..16]))
                .collect();
            assert_eq!(
                matches.len(),
                1,
                "local catalog receipt must bind one dataset"
            );
            instruments.insert(matches[0].2["instrument"].as_str().unwrap().to_string());
        }
    }
    assert!(!instruments.is_empty());
    for (_, key, manifest) in &manifests {
        if !instruments.contains(manifest["instrument"].as_str().unwrap()) {
            continue;
        }
        let bytes = fs::read(store.join(key)).unwrap();
        assert_remote(
            key,
            &bytes,
            bytes.len() as u64,
            &binary_alpha_engine::hex(&Sha256::digest(&bytes)),
        );
        for object in manifest["objects"].as_array().unwrap() {
            let key = object["key"].as_str().unwrap();
            assert_remote(
                key,
                &fs::read(store.join(key)).unwrap(),
                object["bytes"].as_u64().unwrap(),
                object["sha256"].as_str().unwrap(),
            );
        }
    }
    for (name, bytes) in &local_records {
        let value: Value = serde_json::from_slice(bytes).unwrap();
        // A catalog cannot archive its own receipt. Every older receipt is cumulative
        // evidence, selected here from local records and the command's newest catalog IDs.
        let receipt_job = name
            .to_str()
            .unwrap()
            .split_once("-catalog-")
            .map(|(job, _)| job);
        if receipt_job.is_some()
            && value["file_id"]
                .as_str()
                .is_some_and(|id| newest_catalogs.contains(&id))
        {
            continue;
        }
        let owner = if let Some(job) = receipt_job.or_else(|| value["job"].as_str()) {
            Some(job.to_string())
        } else {
            value["intent"]
                .as_str()
                .and_then(|intent| local_records.get(Path::new(intent)))
                .map(|bytes| {
                    serde_json::from_slice::<Value>(bytes).unwrap()["job"]
                        .as_str()
                        .unwrap()
                        .to_string()
                })
        };
        if owner.is_some_and(|job| jobs.contains(&job)) {
            assert_remote(
                name.to_str().unwrap(),
                bytes,
                bytes.len() as u64,
                &binary_alpha_engine::hex(&Sha256::digest(bytes)),
            );
        }
    }
    let mut expected = BTreeSet::new();
    for (id, file) in &files {
        if let Ok(catalog) = Catalog::from_json(&file.bytes) {
            expected.insert(id.clone());
            expected.extend(
                catalog
                    .objects
                    .iter()
                    .chain(&catalog.records)
                    .map(|o| o.file_id.clone()),
            );
            expected.extend(
                [&catalog.dataset, &catalog.stream]
                    .into_iter()
                    .chain(&catalog.lineage_manifests)
                    .map(|m| m.file_id.clone()),
            );
        }
    }
    assert!(!expected.is_empty());
    assert_eq!(
        files.keys().cloned().collect::<BTreeSet<_>>(),
        expected,
        "every uploaded file is owned by a complete pinned catalog closure"
    );
}

#[path = "data_pipeline/review_retire.rs"]
mod review_retire;

#[path = "data_pipeline/review_archive.rs"]
mod review_archive;
