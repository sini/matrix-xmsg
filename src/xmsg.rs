use crate::config::Config;
use crate::error::AppError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

/// Parses the replied-to message ID from a delivery envelope or text.
///
/// Typical formats produced by xmsg:
/// - `[xmsg] reply to message_id=01M4H3NG18MMH1CWYXM6FAS6HG — message_id=...; reply with the xmsg reply tool\n\n...`
/// - `[xmsg] reply to message_id=01M4H3NG18MMH1CWYXM6FAS6HG from=... message_id=...`
/// - `reply to message_id=01M4H3NG18MMH1CWYXM6FAS6HG`
pub fn parse_in_reply_to_id(input: &str) -> Option<&str> {
    let key = "reply to message_id=";
    let start = input.find(key)? + key.len();
    let remainder = &input[start..];
    let end = remainder
        .find(|c: char| c.is_whitespace() || c == '—' || c == ';')
        .unwrap_or(remainder.len());
    let id = remainder[..end].trim();
    if id.is_empty() {
        None
    } else {
        Some(id)
    }
}

/// A delivery received through the xmsg service inbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SvcDelivery {
    pub message_id: String,
    pub from_name: String,
    pub text: String,
    pub envelope: String,
    #[serde(default)]
    pub origin: Option<Value>,
}

impl SvcDelivery {
    pub fn new(
        message_id: impl Into<String>,
        from_name: impl Into<String>,
        text: impl Into<String>,
        envelope: impl Into<String>,
        origin: Option<Value>,
    ) -> Self {
        Self {
            message_id: message_id.into(),
            from_name: from_name.into(),
            text: text.into(),
            envelope: envelope.into(),
            origin,
        }
    }
}

/// Service inbox trait for polling deliveries and acknowledging them.
#[async_trait]
pub trait SvcInbox: Send + Sync {
    /// Polls the service inbox for the next delivery.
    /// Returns Ok(Some(delivery)) on delivery, or Ok(None) on timeout.
    async fn poll(&mut self, wait_secs: u64) -> Result<Option<SvcDelivery>, AppError>;

    /// Acknowledges a delivered message after it has been successfully posted.
    async fn ack(&mut self, message_id: &str) -> Result<(), AppError>;
}

/// Live socket connection to `register.sock` acting as the service inbox.
pub struct SvcSocketInbox {
    lines: tokio::io::Lines<tokio::io::BufReader<tokio::net::unix::OwnedReadHalf>>,
    writer: tokio::net::unix::OwnedWriteHalf,
    session_id: String,
}

impl SvcSocketInbox {
    pub fn session_id(&self) -> &str {
        &self.session_id
    }
}

#[async_trait]
impl SvcInbox for SvcSocketInbox {
    async fn poll(&mut self, wait_secs: u64) -> Result<Option<SvcDelivery>, AppError> {
        let req = serde_json::json!({
            "action": "poll",
            "waitSecs": wait_secs,
        });
        let line = format!("{req}\n");
        self.writer
            .write_all(line.as_bytes())
            .await
            .map_err(|e| AppError::Xmsg(format!("Failed to write poll request: {e}")))?;
        self.writer
            .flush()
            .await
            .map_err(|e| AppError::Xmsg(format!("Failed to flush poll request: {e}")))?;

        let resp_line = self
            .lines
            .next_line()
            .await
            .map_err(|e| AppError::Xmsg(format!("Failed to read poll response: {e}")))?
            .ok_or_else(|| {
                AppError::Xmsg("Svc socket closed while waiting for poll response".to_string())
            })?;

        let val: Value = serde_json::from_str(&resp_line).map_err(|e| {
            AppError::Xmsg(format!(
                "Invalid JSON in poll response: {e}; raw: {resp_line}"
            ))
        })?;

        if let Some(status) = val.get("status").and_then(|s| s.as_str()) {
            if status == "error" {
                let detail = val
                    .get("detail")
                    .and_then(|d| d.as_str())
                    .unwrap_or("unknown error");
                return Err(AppError::Xmsg(format!("Poll error: {detail}")));
            }
        }

        let action = val.get("action").and_then(|a| a.as_str()).unwrap_or("");
        match action {
            "deliver" => {
                let message_id = val
                    .get("messageId")
                    .or_else(|| val.get("message_id"))
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        AppError::Xmsg("Missing messageId in deliver frame".to_string())
                    })?
                    .to_string();
                let from_name = val
                    .get("fromName")
                    .or_else(|| val.get("from_name"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let text = val
                    .get("text")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let envelope = val
                    .get("envelope")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let origin = val.get("origin").cloned();

                Ok(Some(SvcDelivery {
                    message_id,
                    from_name,
                    text,
                    envelope,
                    origin,
                }))
            }
            "timeout" => Ok(None),
            _ => Err(AppError::Xmsg(format!(
                "Unexpected action in poll response: {action}"
            ))),
        }
    }

    async fn ack(&mut self, message_id: &str) -> Result<(), AppError> {
        let req = serde_json::json!({
            "action": "ack",
            "messageId": message_id,
        });
        let line = format!("{req}\n");
        self.writer
            .write_all(line.as_bytes())
            .await
            .map_err(|e| AppError::Xmsg(format!("Failed to write ack request: {e}")))?;
        self.writer
            .flush()
            .await
            .map_err(|e| AppError::Xmsg(format!("Failed to flush ack request: {e}")))?;

        let resp_line = self
            .lines
            .next_line()
            .await
            .map_err(|e| AppError::Xmsg(format!("Failed to read ack response: {e}")))?
            .ok_or_else(|| {
                AppError::Xmsg("Svc socket closed while waiting for ack response".to_string())
            })?;

        let val: Value = serde_json::from_str(&resp_line).map_err(|e| {
            AppError::Xmsg(format!(
                "Invalid JSON in ack response: {e}; raw: {resp_line}"
            ))
        })?;

        if let Some(status) = val.get("status").and_then(|s| s.as_str()) {
            if status == "ok" {
                return Ok(());
            } else if status == "error" {
                let detail = val
                    .get("detail")
                    .and_then(|d| d.as_str())
                    .unwrap_or("unknown error");
                return Err(AppError::Xmsg(format!("Ack error: {detail}")));
            }
        }
        Err(AppError::Xmsg(format!(
            "Unexpected response to ack: {resp_line}"
        )))
    }
}

/// Connects to `register.sock` and registers as `svc:<service_name>`.
/// Returns the connected `SvcSocketInbox` on success.
/// If registration is refused, returns an error.
pub async fn register_svc(
    register_sock_path: &Path,
    service_name: &str,
) -> Result<SvcSocketInbox, AppError> {
    let stream = tokio::net::UnixStream::connect(register_sock_path)
        .await
        .map_err(|e| {
            AppError::Xmsg(format!(
                "Failed to connect to register socket at {}: {e}",
                register_sock_path.display()
            ))
        })?;

    let (read_half, mut write_half) = stream.into_split();
    let mut lines = tokio::io::BufReader::new(read_half).lines();

    let reg_req = serde_json::json!({
        "harness": "svc",
        "name": service_name,
        "cwd": "/",
    });
    let reg_line = format!("{reg_req}\n");
    write_half
        .write_all(reg_line.as_bytes())
        .await
        .map_err(|e| AppError::Xmsg(format!("Failed to send registration: {e}")))?;
    write_half
        .flush()
        .await
        .map_err(|e| AppError::Xmsg(format!("Failed to flush registration: {e}")))?;

    let resp_line = lines
        .next_line()
        .await
        .map_err(|e| AppError::Xmsg(format!("Failed to read registration response: {e}")))?
        .ok_or_else(|| {
            AppError::Xmsg("Register socket closed before registration response".to_string())
        })?;

    let val: Value = serde_json::from_str(&resp_line).map_err(|e| {
        AppError::Xmsg(format!(
            "Invalid JSON in registration response: {e}; raw: {resp_line}"
        ))
    })?;

    let status = val.get("status").and_then(|s| s.as_str()).unwrap_or("");
    if status != "ok" {
        let detail = val
            .get("detail")
            .and_then(|d| d.as_str())
            .unwrap_or("registration rejected");
        return Err(AppError::Xmsg(format!("Svc registration failed: {detail}")));
    }

    let session_id = val
        .get("sessionId")
        .or_else(|| val.get("session_id"))
        .and_then(|s| s.as_str())
        .unwrap_or(service_name)
        .to_string();

    Ok(SvcSocketInbox {
        lines,
        writer: write_half,
        session_id,
    })
}

/// An auto-reconnecting service inbox that wraps `register_svc`.
/// When connection is lost or registration fails, reconnects with exponential backoff
/// (default: start 1s, cap 30s).
pub struct ReconnectingSvcInbox {
    register_sock_path: PathBuf,
    service_name: String,
    current: Option<SvcSocketInbox>,
    min_backoff: Duration,
    max_backoff: Duration,
    current_backoff: Duration,
}

impl ReconnectingSvcInbox {
    pub fn new(register_sock_path: impl Into<PathBuf>, service_name: impl Into<String>) -> Self {
        let min_backoff = Duration::from_secs(1);
        Self {
            register_sock_path: register_sock_path.into(),
            service_name: service_name.into(),
            current: None,
            min_backoff,
            max_backoff: Duration::from_secs(30),
            current_backoff: min_backoff,
        }
    }

    pub fn with_backoff(mut self, min: Duration, max: Duration) -> Self {
        self.min_backoff = min;
        self.max_backoff = max;
        self.current_backoff = min;
        self
    }

    pub fn is_connected(&self) -> bool {
        self.current.is_some()
    }

    pub fn current_backoff(&self) -> Duration {
        self.current_backoff
    }
}

#[async_trait]
impl SvcInbox for ReconnectingSvcInbox {
    async fn poll(&mut self, wait_secs: u64) -> Result<Option<SvcDelivery>, AppError> {
        if self.current.is_none() {
            match register_svc(&self.register_sock_path, &self.service_name).await {
                Ok(inbox) => {
                    tracing::info!("Registered on xmsg as {}", inbox.session_id());
                    self.current = Some(inbox);
                    self.current_backoff = self.min_backoff;
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to connect/register on xmsg at {}: {e}; retrying in {:?}",
                        self.register_sock_path.display(),
                        self.current_backoff
                    );
                    tokio::time::sleep(self.current_backoff).await;
                    self.current_backoff = (self.current_backoff * 2).min(self.max_backoff);
                    return Ok(None);
                }
            }
        }

        let res = match self.current.as_mut() {
            Some(inbox) => inbox.poll(wait_secs).await,
            None => return Ok(None),
        };

        match res {
            Ok(delivery_opt) => Ok(delivery_opt),
            Err(e) => {
                tracing::warn!(
                    "xmsg registration connection lost: {e}; will reconnect with backoff"
                );
                self.current = None;
                self.current_backoff = self.min_backoff;
                Ok(None)
            }
        }
    }

    async fn ack(&mut self, message_id: &str) -> Result<(), AppError> {
        match self.current.as_mut() {
            Some(inbox) => {
                let res = inbox.ack(message_id).await;
                if let Err(ref e) = res {
                    tracing::warn!("Failed to ack delivery {message_id} on xmsg: {e}");
                    self.current = None;
                    self.current_backoff = self.min_backoff;
                }
                res
            }
            None => Err(AppError::Xmsg(format!(
                "Cannot ack delivery {message_id}: not connected to xmsg"
            ))),
        }
    }
}

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
    /// Injects a message into the expert session via agent.sock (action: send, push_replies: true).
    /// Returns the assigned ULID message_id and receiving session_id if available.
    async fn send_message(
        &self,
        expert_ref: &str,
        from: &str,
        text: &str,
    ) -> Result<SendResponse, AppError>;

    /// Sends a reply to a message via agent.sock (action: reply).
    /// Returns the assigned ULID message_id and receiving session_id if available.
    async fn reply_message(
        &self,
        _message_id: &str,
        _text: &str,
    ) -> Result<SendResponse, AppError> {
        Ok(SendResponse::new(
            "mock-reply-msg-id",
            Some("mock-session".to_string()),
        ))
    }
}

pub struct UnixXmsgClient {
    agent_sock_path: PathBuf,
}

impl UnixXmsgClient {
    pub fn new(agent_sock_path: PathBuf) -> Self {
        Self { agent_sock_path }
    }

    pub fn agent_sock_path(&self) -> &Path {
        &self.agent_sock_path
    }
}

#[async_trait]
impl XmsgClient for UnixXmsgClient {
    async fn send_message(
        &self,
        expert_ref: &str,
        _from: &str,
        text: &str,
    ) -> Result<SendResponse, AppError> {
        let stream = tokio::net::UnixStream::connect(&self.agent_sock_path)
            .await
            .map_err(|e| {
                AppError::Xmsg(format!(
                    "Failed to connect to agent socket at {}: {e}",
                    self.agent_sock_path.display()
                ))
            })?;

        let (read_half, mut write_half) = stream.into_split();
        let mut lines = tokio::io::BufReader::new(read_half).lines();

        let req = serde_json::json!({
            "action": "send",
            "ref": expert_ref,
            "text": text,
            "push_replies": true,
        });
        let line = format!("{req}\n");
        write_half.write_all(line.as_bytes()).await.map_err(|e| {
            AppError::Xmsg(format!("Failed to write send request to agent.sock: {e}"))
        })?;
        write_half.flush().await.map_err(|e| {
            AppError::Xmsg(format!("Failed to flush send request to agent.sock: {e}"))
        })?;

        let resp_line = lines
            .next_line()
            .await
            .map_err(|e| {
                AppError::Xmsg(format!("Failed to read send response from agent.sock: {e}"))
            })?
            .ok_or_else(|| {
                AppError::Xmsg("Agent socket closed before send response".to_string())
            })?;

        let val: Value = serde_json::from_str(&resp_line).map_err(|e| {
            AppError::Xmsg(format!(
                "Invalid JSON in agent.sock send response: {e}; raw: {resp_line}"
            ))
        })?;

        let status = val.get("status").and_then(|s| s.as_str()).unwrap_or("");
        if status != "ok" {
            let detail = val
                .get("detail")
                .and_then(|d| d.as_str())
                .unwrap_or("send failed");
            return Err(AppError::Xmsg(format!("agent.sock send failed: {detail}")));
        }

        let message_id = val
            .get("delivery")
            .and_then(|d| d.get("messageId").or_else(|| d.get("message_id")))
            .or_else(|| val.get("messageId"))
            .or_else(|| val.get("message_id"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AppError::Xmsg("Missing messageId in agent.sock delivery response".to_string())
            })?
            .to_string();

        let session_id = val
            .get("delivery")
            .and_then(|d| d.get("sessionId").or_else(|| d.get("session_id")))
            .or_else(|| val.get("sessionId"))
            .or_else(|| val.get("session_id"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        Ok(SendResponse {
            message_id,
            session_id,
        })
    }

    async fn reply_message(&self, message_id: &str, text: &str) -> Result<SendResponse, AppError> {
        let stream = tokio::net::UnixStream::connect(&self.agent_sock_path)
            .await
            .map_err(|e| {
                AppError::Xmsg(format!(
                    "Failed to connect to agent socket at {}: {e}",
                    self.agent_sock_path.display()
                ))
            })?;

        let (read_half, mut write_half) = stream.into_split();
        let mut lines = tokio::io::BufReader::new(read_half).lines();

        let req = serde_json::json!({
            "action": "reply",
            "messageId": message_id,
            "text": text,
        });
        let line = format!("{req}\n");
        write_half.write_all(line.as_bytes()).await.map_err(|e| {
            AppError::Xmsg(format!("Failed to write reply request to agent.sock: {e}"))
        })?;
        write_half.flush().await.map_err(|e| {
            AppError::Xmsg(format!("Failed to flush reply request to agent.sock: {e}"))
        })?;

        let resp_line = lines
            .next_line()
            .await
            .map_err(|e| {
                AppError::Xmsg(format!(
                    "Failed to read reply response from agent.sock: {e}"
                ))
            })?
            .ok_or_else(|| {
                AppError::Xmsg("Agent socket closed before reply response".to_string())
            })?;

        let val: Value = serde_json::from_str(&resp_line).map_err(|e| {
            AppError::Xmsg(format!(
                "Invalid JSON in agent.sock reply response: {e}; raw: {resp_line}"
            ))
        })?;

        let status = val.get("status").and_then(|s| s.as_str()).unwrap_or("");
        if status != "ok" {
            let detail = val
                .get("detail")
                .and_then(|d| d.as_str())
                .unwrap_or("reply failed");
            return Err(AppError::Xmsg(format!("agent.sock reply failed: {detail}")));
        }

        let reply_obj = val.get("reply");
        let pushed_message_id = reply_obj
            .and_then(|r| {
                r.get("pushedMessageId")
                    .or_else(|| r.get("pushed_message_id"))
            })
            .or_else(|| reply_obj.and_then(|r| r.get("messageId").or_else(|| r.get("message_id"))))
            .or_else(|| val.get("messageId").or_else(|| val.get("message_id")))
            .and_then(|v| v.as_str())
            .unwrap_or(message_id)
            .to_string();

        let session_id = reply_obj
            .and_then(|r| {
                r.get("sessionRef")
                    .or_else(|| r.get("replierSessionId"))
                    .or_else(|| r.get("sessionId"))
            })
            .or_else(|| val.get("sessionId"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        Ok(SendResponse {
            message_id: pushed_message_id,
            session_id,
        })
    }
}

pub fn create_xmsg_client(config: &Config) -> Arc<dyn XmsgClient> {
    Arc::new(UnixXmsgClient::new(config.xmsg_agent_socket()))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuardLine {
    pub sender: String,
    pub tier: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuardContent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tier: Option<String>,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuardEnvelope {
    pub source: String,
    pub history: Vec<GuardLine>,
    pub content: GuardContent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuardVerdict {
    pub verdict: String,
    pub reason: String,
    pub cleaned_text: String,
}

#[async_trait]
pub trait GuardClient: Send + Sync {
    async fn check_guard(
        &self,
        guard_ref: &str,
        envelope: &GuardEnvelope,
    ) -> Result<GuardVerdict, AppError>;
}

#[derive(Default, Clone)]
pub struct MockGuardClient {
    pub verdicts: Arc<Mutex<Vec<Result<GuardVerdict, String>>>>,
    pub calls: Arc<Mutex<Vec<(String, GuardEnvelope)>>>,
}

impl MockGuardClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_verdict(verdict: &str, reason: &str, cleaned_text: &str) -> Self {
        let mock = Self::default();
        mock.verdicts.lock().unwrap().push(Ok(GuardVerdict {
            verdict: verdict.to_string(),
            reason: reason.to_string(),
            cleaned_text: cleaned_text.to_string(),
        }));
        mock
    }

    pub fn with_error(err: &str) -> Self {
        let mock = Self::default();
        mock.verdicts.lock().unwrap().push(Err(err.to_string()));
        mock
    }

    pub fn add_verdict(&self, verdict: &str, reason: &str, cleaned_text: &str) {
        self.verdicts.lock().unwrap().push(Ok(GuardVerdict {
            verdict: verdict.to_string(),
            reason: reason.to_string(),
            cleaned_text: cleaned_text.to_string(),
        }));
    }

    pub fn add_error(&self, err: &str) {
        self.verdicts.lock().unwrap().push(Err(err.to_string()));
    }
}

#[async_trait]
impl GuardClient for MockGuardClient {
    async fn check_guard(
        &self,
        guard_ref: &str,
        envelope: &GuardEnvelope,
    ) -> Result<GuardVerdict, AppError> {
        self.calls
            .lock()
            .unwrap()
            .push((guard_ref.to_string(), envelope.clone()));
        let mut list = self.verdicts.lock().unwrap();
        if list.is_empty() {
            Ok(GuardVerdict {
                verdict: "allow".to_string(),
                reason: "default allow".to_string(),
                cleaned_text: envelope.content.text.clone(),
            })
        } else {
            let res = list.remove(0);
            match res {
                Ok(v) => Ok(v),
                Err(e) => Err(AppError::Xmsg(format!("Guard error: {e}"))),
            }
        }
    }
}

pub struct UnixGuardClient {
    pub agent_sock_path: PathBuf,
    pub http_sock_path: PathBuf,
    pub timeout_secs: u64,
}

impl UnixGuardClient {
    pub fn new(agent_sock_path: PathBuf, http_sock_path: PathBuf, timeout_secs: u64) -> Self {
        Self {
            agent_sock_path,
            http_sock_path,
            timeout_secs,
        }
    }
}

pub fn parse_guard_reply(reply_text: &str) -> Result<GuardVerdict, AppError> {
    let val: Value = serde_json::from_str(reply_text.trim()).map_err(|e| {
        AppError::Xmsg(format!(
            "Guard reply is not valid JSON: {e}; raw: {reply_text}"
        ))
    })?;

    let obj = val
        .as_object()
        .ok_or_else(|| AppError::Xmsg("Guard reply JSON is not an object".to_string()))?;

    if let Some(err_val) = obj.get("error") {
        return Err(AppError::Xmsg(format!("Guard returned error: {err_val}")));
    }

    let verdict = obj
        .get("verdict")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::Xmsg("Missing 'verdict' in guard reply".to_string()))?;

    if verdict != "allow" && verdict != "rewrite" && verdict != "reject" {
        return Err(AppError::Xmsg(format!(
            "Invalid verdict '{verdict}': must be 'allow', 'rewrite', or 'reject'"
        )));
    }

    let reason = obj
        .get("reason")
        .and_then(|r| r.as_str())
        .unwrap_or("")
        .to_string();

    let cleaned_text = obj
        .get("cleaned_text")
        .and_then(|c| c.as_str())
        .unwrap_or("")
        .to_string();

    Ok(GuardVerdict {
        verdict: verdict.to_string(),
        reason,
        cleaned_text,
    })
}

async fn http_get_replies(
    http_sock_path: &Path,
    message_id: &str,
    after_seq: i64,
    wait_secs: u64,
) -> Result<Vec<Value>, AppError> {
    let mut stream = tokio::net::UnixStream::connect(http_sock_path)
        .await
        .map_err(|e| {
            AppError::Xmsg(format!(
                "Failed to connect to http.sock at {}: {e}",
                http_sock_path.display()
            ))
        })?;

    let path_query =
        format!("/v1/messages/{message_id}/replies?after={after_seq}&wait={wait_secs}");
    let req = format!("GET {path_query} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| AppError::Xmsg(format!("Failed to write HTTP request: {e}")))?;
    stream
        .flush()
        .await
        .map_err(|e| AppError::Xmsg(format!("Failed to flush HTTP request: {e}")))?;

    let mut response_bytes = Vec::new();
    stream
        .read_to_end(&mut response_bytes)
        .await
        .map_err(|e| AppError::Xmsg(format!("Failed to read HTTP response: {e}")))?;

    let response_str = String::from_utf8_lossy(&response_bytes);
    let (headers, body) = match response_str.find("\r\n\r\n") {
        Some(idx) => (&response_str[..idx], &response_str[idx + 4..]),
        None => match response_str.find("\n\n") {
            Some(idx) => (&response_str[..idx], &response_str[idx + 2..]),
            None => {
                return Err(AppError::Xmsg(format!(
                    "Invalid HTTP response: header delimiter not found; raw: {response_str}"
                )));
            }
        },
    };

    let status_line = headers.lines().next().unwrap_or("");
    if !status_line.contains(" 200 ") && !status_line.ends_with(" 200") {
        return Err(AppError::Xmsg(format!(
            "HTTP error from http.sock: {status_line}; body: {body}"
        )));
    }

    let is_chunked = headers.lines().any(|l| {
        let lower = l.to_ascii_lowercase();
        lower.starts_with("transfer-encoding:") && lower.contains("chunked")
    });

    let decoded_body = if is_chunked {
        decode_chunked_body(body)?
    } else {
        body.to_string()
    };

    let val: Value = serde_json::from_str(&decoded_body).map_err(|e| {
        AppError::Xmsg(format!(
            "Failed to parse JSON replies array: {e}; body: {decoded_body}"
        ))
    })?;

    val.as_array()
        .cloned()
        .ok_or_else(|| AppError::Xmsg("Expected JSON array of replies".to_string()))
}

fn decode_chunked_body(body: &str) -> Result<String, AppError> {
    let mut result = String::new();
    let mut remaining = body;
    while !remaining.is_empty() {
        let (size_str, rest) = match remaining.split_once("\r\n") {
            Some(pair) => pair,
            None => match remaining.split_once('\n') {
                Some(pair) => pair,
                None => break,
            },
        };
        let size_str = size_str.trim();
        if size_str.is_empty() {
            break;
        }
        let chunk_size = usize::from_str_radix(size_str, 16)
            .map_err(|e| AppError::Xmsg(format!("Invalid chunk size hex '{size_str}': {e}")))?;
        if chunk_size == 0 {
            break;
        }
        if rest.len() < chunk_size {
            return Err(AppError::Xmsg("Truncated chunked HTTP body".to_string()));
        }
        result.push_str(&rest[..chunk_size]);
        let after_chunk = &rest[chunk_size..];
        remaining = after_chunk
            .strip_prefix("\r\n")
            .or_else(|| after_chunk.strip_prefix('\n'))
            .unwrap_or(after_chunk);
    }
    Ok(result)
}

#[async_trait]
impl GuardClient for UnixGuardClient {
    async fn check_guard(
        &self,
        guard_ref: &str,
        envelope: &GuardEnvelope,
    ) -> Result<GuardVerdict, AppError> {
        let stream = tokio::net::UnixStream::connect(&self.agent_sock_path)
            .await
            .map_err(|e| {
                AppError::Xmsg(format!(
                    "Failed to connect to agent socket at {}: {e}",
                    self.agent_sock_path.display()
                ))
            })?;

        let (read_half, mut write_half) = stream.into_split();
        let mut lines = tokio::io::BufReader::new(read_half).lines();

        let env_text = serde_json::to_string(envelope)
            .map_err(|e| AppError::Xmsg(format!("Failed to serialize guard envelope: {e}")))?;

        let req = serde_json::json!({
            "action": "send",
            "ref": guard_ref,
            "text": env_text,
            "push_replies": false,
        });
        let line = format!("{req}\n");
        write_half.write_all(line.as_bytes()).await.map_err(|e| {
            AppError::Xmsg(format!("Failed to write send request to agent.sock: {e}"))
        })?;
        write_half.flush().await.map_err(|e| {
            AppError::Xmsg(format!("Failed to flush send request to agent.sock: {e}"))
        })?;

        let resp_line = lines
            .next_line()
            .await
            .map_err(|e| {
                AppError::Xmsg(format!("Failed to read send response from agent.sock: {e}"))
            })?
            .ok_or_else(|| {
                AppError::Xmsg("Agent socket closed before send response".to_string())
            })?;

        let val: Value = serde_json::from_str(&resp_line).map_err(|e| {
            AppError::Xmsg(format!(
                "Invalid JSON in agent.sock send response: {e}; raw: {resp_line}"
            ))
        })?;

        let status = val.get("status").and_then(|s| s.as_str()).unwrap_or("");
        if status != "ok" {
            let detail = val
                .get("detail")
                .and_then(|d| d.as_str())
                .unwrap_or("send failed");
            return Err(AppError::Xmsg(format!(
                "agent.sock guard send failed: {detail}"
            )));
        }

        let message_id = val
            .get("delivery")
            .and_then(|d| d.get("messageId").or_else(|| d.get("message_id")))
            .or_else(|| val.get("messageId"))
            .or_else(|| val.get("message_id"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AppError::Xmsg(format!(
                    "agent.sock send response missing messageId: {resp_line}"
                ))
            })?;

        // Long-poll http.sock for replies
        let start_time = tokio::time::Instant::now();
        let total_timeout = std::time::Duration::from_secs(self.timeout_secs);
        let mut after_seq = 0i64;

        loop {
            let elapsed = start_time.elapsed();
            if elapsed >= total_timeout {
                return Err(AppError::Xmsg(format!(
                    "Guard check timed out after {}s waiting for reply to message {message_id}",
                    self.timeout_secs
                )));
            }
            let remaining = total_timeout - elapsed;
            let wait_secs = remaining.as_secs().clamp(1, 60);

            let replies =
                http_get_replies(&self.http_sock_path, message_id, after_seq, wait_secs).await?;
            if !replies.is_empty() {
                for r in &replies {
                    if let Some(s) = r.get("seq").and_then(|s| s.as_i64()) {
                        if s > after_seq {
                            after_seq = s;
                        }
                    }
                }
                let last_reply = &replies[replies.len() - 1];
                let reply_text = last_reply
                    .get("text")
                    .and_then(|t| t.as_str())
                    .ok_or_else(|| AppError::Xmsg("Missing 'text' in reply record".to_string()))?;

                return parse_guard_reply(reply_text);
            }
        }
    }
}

pub fn create_guard_client(config: &Config) -> Arc<dyn GuardClient> {
    Arc::new(UnixGuardClient::new(
        config.xmsg_agent_socket(),
        config.xmsg_http_socket(),
        config.guard_timeout_secs,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_in_reply_to_id() {
        let envelope1 = "[xmsg] reply to message_id=01M4H3NG18MMH1CWYXM6FAS6HG — message_id=01M4H7REPLY; reply with the xmsg reply tool\n\nHello";
        assert_eq!(
            parse_in_reply_to_id(envelope1),
            Some("01M4H3NG18MMH1CWYXM6FAS6HG")
        );

        let envelope2 = "[xmsg] reply to message_id=01M4H3NG18MMH1CWYXM6FAS6HG from=claude message_id=01M4H7REPLY";
        assert_eq!(
            parse_in_reply_to_id(envelope2),
            Some("01M4H3NG18MMH1CWYXM6FAS6HG")
        );

        let plain = "reply to message_id=01M4H3NG18MMH1CWYXM6FAS6HG\n\nsome text";
        assert_eq!(
            parse_in_reply_to_id(plain),
            Some("01M4H3NG18MMH1CWYXM6FAS6HG")
        );

        let none = "hello world without key";
        assert_eq!(parse_in_reply_to_id(none), None);
    }

    #[test]
    fn test_parse_guard_reply_allow() {
        let text = r#"{"verdict": "allow", "reason": "clean", "cleaned_text": "hello"}"#;
        let v = parse_guard_reply(text).unwrap();
        assert_eq!(v.verdict, "allow");
        assert_eq!(v.reason, "clean");
        assert_eq!(v.cleaned_text, "hello");
    }

    #[test]
    fn test_parse_guard_reply_rewrite() {
        let text = r#"{"verdict": "rewrite", "reason": "sanitized", "cleaned_text": "safe text"}"#;
        let v = parse_guard_reply(text).unwrap();
        assert_eq!(v.verdict, "rewrite");
        assert_eq!(v.reason, "sanitized");
        assert_eq!(v.cleaned_text, "safe text");
    }

    #[test]
    fn test_parse_guard_reply_reject() {
        let text = r#"{"verdict": "reject", "reason": "attack detected", "cleaned_text": ""}"#;
        let v = parse_guard_reply(text).unwrap();
        assert_eq!(v.verdict, "reject");
        assert_eq!(v.reason, "attack detected");
    }

    #[test]
    fn test_parse_guard_reply_error() {
        let text = r#"{"error": "model timeout"}"#;
        assert!(parse_guard_reply(text).is_err());
    }

    #[test]
    fn test_parse_guard_reply_invalid_verdict() {
        let text = r#"{"verdict": "unknown", "reason": "none"}"#;
        assert!(parse_guard_reply(text).is_err());
    }

    #[test]
    fn test_parse_guard_reply_invalid_json() {
        let text = "not json";
        assert!(parse_guard_reply(text).is_err());
    }
}
