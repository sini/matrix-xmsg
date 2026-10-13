use crate::config::Config;
use crate::error::AppError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

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

/// The ID in an xmsg sender label or address (A12): `label · id[~n]@node` for an instance,
/// `name@node` for a service, or a bare `id`. The label is the sender's own description and
/// `@node` names where it runs; only the ID is returned.
pub fn sender_id(label_or_address: &str) -> &str {
    let attested = label_or_address
        .rsplit_once(" · ")
        .map_or(label_or_address, |(_, a)| a);
    attested.rsplit_once('@').map_or(attested, |(id, _)| id)
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
    /// The message this delivery answers, when it is a pushed reply (`inReplyTo`).
    #[serde(default)]
    pub in_reply_to: Option<String>,
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
            in_reply_to: None,
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

    /// This node's name, from the registration's `ok` frame; `None` while unregistered.
    fn node(&self) -> Option<String> {
        None
    }
}

/// Live socket connection to `register.sock` acting as the service inbox.
pub struct SvcSocketInbox {
    lines: tokio::io::Lines<tokio::io::BufReader<tokio::net::unix::OwnedReadHalf>>,
    writer: tokio::net::unix::OwnedWriteHalf,
    address: String,
    node: String,
}

impl SvcSocketInbox {
    /// The registration's address, `<name>@<node>`.
    pub fn address(&self) -> &str {
        &self.address
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
                let str_field = |k: &str| val.get(k).and_then(|v| v.as_str()).map(String::from);
                let message_id = str_field("messageId").ok_or_else(|| {
                    AppError::Xmsg("Missing messageId in deliver frame".to_string())
                })?;
                Ok(Some(SvcDelivery {
                    message_id,
                    from_name: str_field("fromName").unwrap_or_default(),
                    text: str_field("text").unwrap_or_default(),
                    envelope: str_field("envelope").unwrap_or_default(),
                    origin: val.get("origin").cloned(),
                    in_reply_to: str_field("inReplyTo"),
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

    fn node(&self) -> Option<String> {
        Some(self.node.clone())
    }
}

/// Connects to `register.sock` and registers as the service `<service_name>` (a bare name, A11).
/// Returns the connected `SvcSocketInbox` on success, holding the address and node name from the
/// `ok` frame. If registration is refused, or the `ok` frame names no node, returns an error.
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

    let node = val
        .get("node")
        .and_then(|s| s.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            AppError::Xmsg(format!(
                "Svc registration ok frame names no node: {resp_line}"
            ))
        })?
        .to_string();
    let address = val
        .get("address")
        .and_then(|s| s.as_str())
        .map_or_else(|| format!("{service_name}@{node}"), String::from);

    Ok(SvcSocketInbox {
        lines,
        writer: write_half,
        address,
        node,
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
                    tracing::info!("Registered on xmsg as {}", inbox.address());
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

    fn node(&self) -> Option<String> {
        self.current.as_ref().and_then(|i| i.node())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendResponse {
    pub message_id: String,
    /// The ID the send resolved to: an instance's `id[~n]` or a service's name, read from the
    /// `address` (`id@node`) in `send`'s result. `None` for a reply.
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
    /// Sends a message to `expert_ref` via agent.sock (`{"action":"send","to","text"}`).
    /// Replies are pushed into the bot's service inbox (A10).
    /// Returns the assigned ULID message_id and the resolved receiving ID.
    async fn send_message(
        &self,
        expert_ref: &str,
        from: &str,
        text: &str,
    ) -> Result<SendResponse, AppError>;

    /// Sends a reply to a message via agent.sock (action: reply).
    /// Returns the pushed reply's message_id when there is one.
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

/// One `agent.sock` request: writes `frame`, reads one answer, and returns it when its
/// `status` is `ok`. Any other answer is an error naming the xmsg error code and detail.
async fn agent_call(agent_sock_path: &Path, what: &str, frame: Value) -> Result<Value, AppError> {
    let stream = tokio::net::UnixStream::connect(agent_sock_path)
        .await
        .map_err(|e| {
            AppError::Xmsg(format!(
                "Failed to connect to agent socket at {}: {e}",
                agent_sock_path.display()
            ))
        })?;
    let (read_half, mut write_half) = stream.into_split();
    let mut lines = tokio::io::BufReader::new(read_half).lines();

    write_half
        .write_all(format!("{frame}\n").as_bytes())
        .await
        .map_err(|e| {
            AppError::Xmsg(format!("Failed to write {what} request to agent.sock: {e}"))
        })?;
    write_half.flush().await.map_err(|e| {
        AppError::Xmsg(format!("Failed to flush {what} request to agent.sock: {e}"))
    })?;

    let resp_line = lines
        .next_line()
        .await
        .map_err(|e| {
            AppError::Xmsg(format!(
                "Failed to read {what} response from agent.sock: {e}"
            ))
        })?
        .ok_or_else(|| AppError::Xmsg(format!("Agent socket closed before {what} response")))?;

    let val: Value = serde_json::from_str(&resp_line).map_err(|e| {
        AppError::Xmsg(format!(
            "Invalid JSON in agent.sock {what} response: {e}; raw: {resp_line}"
        ))
    })?;

    if val.get("status").and_then(|s| s.as_str()) != Some("ok") {
        let code = val.get("error").and_then(|d| d.as_str()).unwrap_or("error");
        let detail = val
            .get("detail")
            .and_then(|d| d.as_str())
            .unwrap_or("no detail");
        return Err(AppError::Xmsg(format!(
            "agent.sock {what} failed ({code}): {detail}"
        )));
    }
    Ok(val)
}

/// Sends `text` to `to` on agent.sock and returns the message ID and the resolved ID.
async fn agent_send(
    agent_sock_path: &Path,
    what: &str,
    to: &str,
    text: &str,
) -> Result<SendResponse, AppError> {
    let val = agent_call(
        agent_sock_path,
        what,
        serde_json::json!({ "action": "send", "to": to, "text": text }),
    )
    .await?;
    let message_id = val
        .get("message_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            AppError::Xmsg(format!("agent.sock {what} answer has no message_id: {val}"))
        })?
        .to_string();
    let session_id = val
        .get("address")
        .and_then(|v| v.as_str())
        .map(|a| sender_id(a).to_string());
    Ok(SendResponse {
        message_id,
        session_id,
    })
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
        agent_send(&self.agent_sock_path, "send", expert_ref, text).await
    }

    async fn reply_message(&self, message_id: &str, text: &str) -> Result<SendResponse, AppError> {
        let val = agent_call(
            &self.agent_sock_path,
            "reply",
            serde_json::json!({ "action": "reply", "message_id": message_id, "text": text }),
        )
        .await?;
        let pushed_message_id = val
            .get("reply")
            .and_then(|r| r.get("pushedMessageId"))
            .and_then(|v| v.as_str())
            .unwrap_or(message_id)
            .to_string();
        Ok(SendResponse {
            message_id: pushed_message_id,
            session_id: None,
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

/// Guard requests awaiting their verdict, keyed by the guard request's message ID (AM-3, plan
/// §6 D6). The verdict arrives pushed into the bot's service inbox, and the inbox loop hands it
/// here by its `inReplyTo` (`route_guard_reply`); it never reaches the expert path.
///
/// One process-wide map. A check that times out evicts its entry: a verdict arriving after that
/// matches no entry and no forward, so the inbox loop ignores it and never posts it.
type GuardPending = tokio::sync::Mutex<HashMap<String, tokio::sync::oneshot::Sender<String>>>;

fn guard_pending() -> &'static GuardPending {
    static PENDING: OnceLock<GuardPending> = OnceLock::new();
    PENDING.get_or_init(Default::default)
}

/// Hands a pushed reply to the guard check waiting on `in_reply_to`. Returns `true` when
/// `in_reply_to` is a guard request this process sent: the delivery is a verdict, consumed here.
pub async fn route_guard_reply(in_reply_to: &str, text: &str) -> bool {
    match guard_pending().lock().await.remove(in_reply_to) {
        Some(tx) => {
            let _ = tx.send(text.to_string());
            true
        }
        None => false,
    }
}

pub struct UnixGuardClient {
    pub agent_sock_path: PathBuf,
    pub timeout_secs: u64,
    /// The bound on the guard request's `agent.sock` send. The pending map is locked for that
    /// send, and the inbox loop routes every pushed reply through the map, so this also bounds
    /// how long one guard check can hold up the inbox.
    pub send_timeout: Duration,
}

impl UnixGuardClient {
    pub fn new(agent_sock_path: PathBuf, timeout_secs: u64) -> Self {
        Self {
            agent_sock_path,
            timeout_secs,
            send_timeout: Duration::from_secs(10),
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

#[async_trait]
impl GuardClient for UnixGuardClient {
    /// Sends the envelope to `guard_ref` and waits for the verdict pushed into the inbox.
    /// An `ambiguous` route (two guards in view, A-10) fails here, naming the error code.
    async fn check_guard(
        &self,
        guard_ref: &str,
        envelope: &GuardEnvelope,
    ) -> Result<GuardVerdict, AppError> {
        let env_text = serde_json::to_string(envelope)
            .map_err(|e| AppError::Xmsg(format!("Failed to serialize guard envelope: {e}")))?;

        // The map stays locked from the send until the entry is in, so the inbox loop cannot
        // see the verdict before the request is known. The send is bounded, so neither can a
        // stalled agent.sock hold the map, and with it the inbox loop, past `send_timeout`.
        let (message_id, rx) = {
            let mut pending = guard_pending().lock().await;
            let send = agent_send(&self.agent_sock_path, "guard send", guard_ref, &env_text);
            let sent = tokio::time::timeout(self.send_timeout, send)
                .await
                .map_err(|_| {
                    AppError::Xmsg(format!(
                        "Guard send to {guard_ref} timed out after {:?}",
                        self.send_timeout
                    ))
                })
                .and_then(|r| r)
                .inspect_err(|e| tracing::error!("Guard route {guard_ref} refused: {e}"))?;
            let (tx, rx) = tokio::sync::oneshot::channel();
            pending.insert(sent.message_id.clone(), tx);
            (sent.message_id, rx)
        };

        match tokio::time::timeout(Duration::from_secs(self.timeout_secs), rx).await {
            Ok(Ok(text)) => parse_guard_reply(&text),
            _ => {
                guard_pending().lock().await.remove(&message_id);
                Err(AppError::Xmsg(format!(
                    "Guard check timed out after {}s waiting for the verdict to message {message_id}",
                    self.timeout_secs
                )))
            }
        }
    }
}

pub fn create_guard_client(config: &Config) -> Arc<dyn GuardClient> {
    Arc::new(UnixGuardClient::new(
        config.xmsg_agent_socket(),
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
    fn test_sender_id_reads_the_a12_shape() {
        let id = "2a82335a-0000-4000-8000-000000000001";
        assert_eq!(sender_id(&format!("den-f0 · {id}@sini.cortex")), id);
        assert_eq!(
            sender_id(&format!("den-f0 · {id}~2@sini.cortex")),
            format!("{id}~2")
        );
        assert_eq!(sender_id(&format!("{id}@sini.cortex")), id);
        assert_eq!(sender_id("genie-guard@sini.bitstream"), "genie-guard");
        assert_eq!(sender_id(id), id);
        // The attested part is the RIGHTMOST ` · `: a label is the sender's own text. xmsg drops
        // a label containing `·`, so this only pins the split's side.
        assert_eq!(sender_id(&format!("a · b · {id}@sini.cortex")), id);
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
