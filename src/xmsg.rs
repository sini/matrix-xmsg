use crate::config::{Config, XmsgEndpoint};
use crate::error::AppError;
use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendResponse {
    pub message_id: String,
    pub session_id: Option<String>,
}

impl SendResponse {
    pub fn new(message_id: impl Into<String>, session_id: Option<String>) -> Self {
        Self {
            message_id: message_id.into(),
            session_id,
        }
    }
}

impl From<String> for SendResponse {
    fn from(message_id: String) -> Self {
        Self {
            message_id,
            session_id: Some("mock-session".to_string()),
        }
    }
}

impl From<&str> for SendResponse {
    fn from(message_id: &str) -> Self {
        Self {
            message_id: message_id.to_string(),
            session_id: Some("mock-session".to_string()),
        }
    }
}

#[async_trait]
pub trait XmsgClient: Send + Sync {
    /// Injects a message into the expert session inbox via POST /v1/sessions/{expert}/messages.
    /// Returns the assigned ULID message_id and receiving session_id if available.
    async fn send_message(
        &self,
        expert_ref: &str,
        from: &str,
        text: &str,
    ) -> Result<SendResponse, AppError>;

    /// Long-polls /v1/messages/{id}/replies until a reply arrives or timeout expires.
    async fn wait_for_reply(&self, message_id: &str, timeout_secs: u64)
        -> Result<String, AppError>;
}

pub fn create_xmsg_client(config: &Config) -> Arc<dyn XmsgClient> {
    match config.xmsg_endpoint() {
        XmsgEndpoint::Unix(path) => Arc::new(UnixXmsgClient::new(path)),
        XmsgEndpoint::Tcp(url) => Arc::new(HttpXmsgClient::new(url)),
    }
}

pub struct UnixXmsgClient {
    socket_path: PathBuf,
}

impl UnixXmsgClient {
    pub fn new(socket_path: PathBuf) -> Self {
        Self { socket_path }
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

#[async_trait]
impl XmsgClient for UnixXmsgClient {
    async fn send_message(
        &self,
        expert_ref: &str,
        from: &str,
        text: &str,
    ) -> Result<SendResponse, AppError> {
        let path_and_query = format!("/v1/sessions/{expert_ref}/messages");
        let payload = serde_json::to_vec(&serde_json::json!({
            "from": from,
            "text": text,
        }))
        .map_err(|e| AppError::Xmsg(format!("JSON serialization failed: {e}")))?;

        let (status, body) =
            unix_http_request(&self.socket_path, "POST", &path_and_query, Some(&payload)).await?;

        if !(200..300).contains(&status) {
            let body_str = String::from_utf8_lossy(&body);
            return Err(AppError::Xmsg(format!(
                "POST {path_and_query} returned {status}: {body_str}"
            )));
        }

        let json: Value = serde_json::from_slice(&body).map_err(|e| {
            AppError::Xmsg(format!(
                "Failed to parse response JSON from {path_and_query}: {e}"
            ))
        })?;

        let message_id = json["messageId"]
            .as_str()
            .or_else(|| json["message_id"].as_str())
            .ok_or_else(|| AppError::Xmsg("Missing messageId in response".to_string()))?
            .to_string();

        let session_id = json["sessionId"]
            .as_str()
            .or_else(|| json["session_id"].as_str())
            .map(|s| s.to_string());

        Ok(SendResponse {
            message_id,
            session_id,
        })
    }

    async fn wait_for_reply(
        &self,
        message_id: &str,
        timeout_secs: u64,
    ) -> Result<String, AppError> {
        let start = Instant::now();
        let timeout_dur = Duration::from_secs(timeout_secs);
        let mut after_seq: u64 = 0;

        while start.elapsed() < timeout_dur {
            let remaining_secs = timeout_dur.saturating_sub(start.elapsed()).as_secs();
            let poll_wait = remaining_secs.clamp(1, 60);

            let path_and_query =
                format!("/v1/messages/{message_id}/replies?after={after_seq}&wait={poll_wait}");

            match unix_http_request(&self.socket_path, "GET", &path_and_query, None).await {
                Ok((status, body)) if (200..300).contains(&status) => {
                    if let Ok(replies) = serde_json::from_slice::<Vec<Value>>(&body) {
                        for rep in replies {
                            if let Some(seq) = rep["seq"].as_u64() {
                                if seq > after_seq {
                                    after_seq = seq;
                                }
                            }
                            if let Some(text) =
                                rep["body"].as_str().or_else(|| rep["text"].as_str())
                            {
                                return Ok(text.to_string());
                            }
                        }
                    }
                }
                Ok((status, _)) => {
                    tracing::warn!("Polling {} returned status {}", path_and_query, status);
                }
                Err(e) => {
                    tracing::warn!("Polling {} failed: {}", path_and_query, e);
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        Err(AppError::Timeout(timeout_secs))
    }
}

pub enum HttpXmsgClient {
    Tcp { client: Client, base_url: String },
    Unix(UnixXmsgClient),
}

impl HttpXmsgClient {
    pub fn new(endpoint: String) -> Self {
        if let Some(path) = endpoint.strip_prefix("unix://") {
            Self::Unix(UnixXmsgClient::new(PathBuf::from(path)))
        } else {
            Self::Tcp {
                client: Client::new(),
                base_url: endpoint.trim_end_matches('/').to_string(),
            }
        }
    }

    pub fn new_unix(socket_path: PathBuf) -> Self {
        Self::Unix(UnixXmsgClient::new(socket_path))
    }
}

#[async_trait]
impl XmsgClient for HttpXmsgClient {
    async fn send_message(
        &self,
        expert_ref: &str,
        from: &str,
        text: &str,
    ) -> Result<SendResponse, AppError> {
        match self {
            Self::Tcp { client, base_url } => {
                let url = format!("{base_url}/v1/sessions/{expert_ref}/messages");
                let resp = client
                    .post(&url)
                    .json(&serde_json::json!({
                        "from": from,
                        "text": text,
                    }))
                    .send()
                    .await
                    .map_err(|e| AppError::Xmsg(format!("POST {url} failed: {e}")))?;

                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    return Err(AppError::Xmsg(format!(
                        "POST {url} returned {status}: {body}"
                    )));
                }

                let json: Value = resp.json().await.map_err(|e| {
                    AppError::Xmsg(format!("Failed to parse response JSON from {url}: {e}"))
                })?;

                let message_id = json["messageId"]
                    .as_str()
                    .or_else(|| json["message_id"].as_str())
                    .ok_or_else(|| AppError::Xmsg("Missing messageId in response".to_string()))?
                    .to_string();

                let session_id = json["sessionId"]
                    .as_str()
                    .or_else(|| json["session_id"].as_str())
                    .map(|s| s.to_string());

                Ok(SendResponse {
                    message_id,
                    session_id,
                })
            }
            Self::Unix(unix) => unix.send_message(expert_ref, from, text).await,
        }
    }

    async fn wait_for_reply(
        &self,
        message_id: &str,
        timeout_secs: u64,
    ) -> Result<String, AppError> {
        match self {
            Self::Tcp { client, base_url } => {
                let start = Instant::now();
                let timeout_dur = Duration::from_secs(timeout_secs);
                let mut after_seq: u64 = 0;

                while start.elapsed() < timeout_dur {
                    let remaining_secs = timeout_dur.saturating_sub(start.elapsed()).as_secs();
                    let poll_wait = remaining_secs.clamp(1, 60);

                    let url = format!(
                        "{base_url}/v1/messages/{message_id}/replies?after={after_seq}&wait={poll_wait}"
                    );

                    let resp = client.get(&url).send().await;
                    match resp {
                        Ok(r) if r.status().is_success() => {
                            if let Ok(replies) = r.json::<Vec<Value>>().await {
                                for rep in replies {
                                    if let Some(seq) = rep["seq"].as_u64() {
                                        if seq > after_seq {
                                            after_seq = seq;
                                        }
                                    }
                                    if let Some(text) =
                                        rep["body"].as_str().or_else(|| rep["text"].as_str())
                                    {
                                        return Ok(text.to_string());
                                    }
                                }
                            }
                        }
                        Ok(r) => {
                            tracing::warn!("Polling {} returned status {}", url, r.status());
                        }
                        Err(e) => {
                            tracing::warn!("Polling {} failed: {}", url, e);
                        }
                    }

                    tokio::time::sleep(Duration::from_millis(500)).await;
                }

                Err(AppError::Timeout(timeout_secs))
            }
            Self::Unix(unix) => unix.wait_for_reply(message_id, timeout_secs).await,
        }
    }
}

pub(crate) async fn unix_http_request(
    socket_path: &Path,
    method: &str,
    path_and_query: &str,
    body: Option<&[u8]>,
) -> Result<(u16, Vec<u8>), AppError> {
    let mut stream = UnixStream::connect(socket_path).await.map_err(|e| {
        AppError::Xmsg(format!(
            "Failed to connect to unix socket {}: {e}",
            socket_path.display()
        ))
    })?;

    let body_bytes = body.unwrap_or(&[]);
    let mut req_header = format!(
        "{method} {path_and_query} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Accept: application/json\r\n\
         Connection: close\r\n"
    );
    if !body_bytes.is_empty() {
        req_header.push_str(&format!(
            "Content-Type: application/json\r\n\
             Content-Length: {}\r\n",
            body_bytes.len()
        ));
    }
    req_header.push_str("\r\n");

    stream
        .write_all(req_header.as_bytes())
        .await
        .map_err(|e| AppError::Xmsg(format!("Failed to write HTTP header to unix socket: {e}")))?;

    if !body_bytes.is_empty() {
        stream.write_all(body_bytes).await.map_err(|e| {
            AppError::Xmsg(format!("Failed to write HTTP body to unix socket: {e}"))
        })?;
    }
    stream
        .flush()
        .await
        .map_err(|e| AppError::Xmsg(format!("Failed to flush unix socket: {e}")))?;

    let mut response_bytes = Vec::new();
    stream.read_to_end(&mut response_bytes).await.map_err(|e| {
        AppError::Xmsg(format!(
            "Failed to read HTTP response from unix socket: {e}"
        ))
    })?;

    let header_end = response_bytes
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| {
            AppError::Xmsg("Malformed HTTP response: missing header delimiter".to_string())
        })?;

    let headers_part = std::str::from_utf8(&response_bytes[..header_end])
        .map_err(|e| AppError::Xmsg(format!("Invalid UTF-8 in HTTP headers: {e}")))?;
    let mut lines = headers_part.lines();
    let status_line = lines
        .next()
        .ok_or_else(|| AppError::Xmsg("Empty HTTP response".to_string()))?;

    let mut parts = status_line.split_whitespace();
    let _proto = parts
        .next()
        .ok_or_else(|| AppError::Xmsg("Missing protocol in status line".to_string()))?;
    let status_code: u16 = parts.next().and_then(|s| s.parse().ok()).ok_or_else(|| {
        AppError::Xmsg(format!(
            "Invalid HTTP status code in status line: {status_line}"
        ))
    })?;

    let raw_body = &response_bytes[header_end + 4..];

    let is_chunked = lines.any(|l| {
        let lower = l.to_ascii_lowercase();
        lower.starts_with("transfer-encoding:") && lower.contains("chunked")
    });

    let body_vec = if is_chunked {
        decode_chunked(raw_body)?
    } else {
        raw_body.to_vec()
    };

    Ok((status_code, body_vec))
}

fn decode_chunked(mut data: &[u8]) -> Result<Vec<u8>, AppError> {
    let mut decoded = Vec::new();
    while !data.is_empty() {
        let pos = data.windows(2).position(|w| w == b"\r\n").ok_or_else(|| {
            AppError::Xmsg("Truncated chunk header in chunked response".to_string())
        })?;
        let size_str = std::str::from_utf8(&data[..pos])
            .map_err(|e| AppError::Xmsg(format!("Invalid UTF-8 in chunk size: {e}")))?
            .trim();
        let chunk_size = usize::from_str_radix(size_str.split(';').next().unwrap_or(""), 16)
            .map_err(|e| AppError::Xmsg(format!("Invalid chunk size '{size_str}': {e}")))?;
        data = &data[pos + 2..];
        if chunk_size == 0 {
            break;
        }
        if data.len() < chunk_size + 2 {
            return Err(AppError::Xmsg("Incomplete chunk data".to_string()));
        }
        decoded.extend_from_slice(&data[..chunk_size]);
        data = &data[chunk_size + 2..];
    }
    Ok(decoded)
}
