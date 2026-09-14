use futures_util::{SinkExt, StreamExt};
use std::rc::Rc;
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
pub trait Transport {
    fn send(&mut self, frame: Frame) -> Result<(), String>;
    fn receive(&mut self, timeout_micros: i64) -> Result<Option<Frame>, String>;
    fn close(&mut self) -> Result<(), String>;
}
pub trait Connector {
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
    runtime: Rc<Runtime>,
    client: reqwest::Client,
}
impl WebSocketConnector {
    pub fn new() -> Result<Self, String> {
        let runtime = Rc::new(
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
            runtime: Rc::clone(&self.runtime),
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
            runtime: Rc::clone(&self.runtime),
            socket,
            host,
        }))
    }
}
struct WebSocketTransport {
    runtime: Rc<Runtime>,
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
    runtime: Rc<Runtime>,
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
