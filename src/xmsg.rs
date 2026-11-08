use crate::config::Config;
use crate::error::AppError;
pub use ::xmsg::client::ReplyRecord;
use ::xmsg::client::{
    current_uid, AgentClient, ClientError, ClientOptions, Delivery, RegisteredService,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

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

/// The full attested `<id-or-service>@<node>` (or bare `<id>` when no `@node` is present)
/// in an xmsg sender label or address (A12): strips any leading `label · ` description and
/// keeps `@<node>`.
pub fn sender_address(label_or_address: &str) -> &str {
    label_or_address
        .rsplit_once(" · ")
        .map_or(label_or_address, |(_, a)| a)
        .trim()
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

    pub fn from_deliver_frame(val: &Value) -> Option<Self> {
        let str_field = |k: &str| val.get(k).and_then(|v| v.as_str()).map(String::from);
        let message_id = str_field("messageId").or_else(|| str_field("message_id"))?;
        Some(Self {
            message_id,
            from_name: str_field("fromName")
                .or_else(|| str_field("from_name"))
                .unwrap_or_default(),
            text: str_field("text").unwrap_or_default(),
            envelope: str_field("envelope").unwrap_or_default(),
            origin: val.get("origin").cloned(),
            in_reply_to: str_field("inReplyTo").or_else(|| str_field("in_reply_to")),
        })
    }

    pub fn to_deliver_frame(&self) -> Value {
        let mut obj = serde_json::json!({
            "status": "ok",
            "action": "deliver",
            "messageId": self.message_id,
            "fromName": self.from_name,
            "text": self.text,
            "envelope": self.envelope,
        });
        if let Some(ref origin) = self.origin {
            obj["origin"] = origin.clone();
        }
        if let Some(ref in_reply_to) = self.in_reply_to {
            obj["inReplyTo"] = Value::String(in_reply_to.clone());
        }
        obj
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

    fn xmsg_client(&self) -> Option<Arc<dyn XmsgClient>> {
        None
    }
}

/// Shared test-support `SvcInbox` mock that behaves like real `xmsg` at `cca3de7` (XF6) or later:
/// - The head delivery remains in `queue` and is re-served until `ack` removes it (`poll` never pops).
/// - `in_flight` is tracked per connection as in XF6, so subsequent polls on the same connection
///   advance past in-flight unacked deliveries when `track_in_flight` is enabled.
/// - Reconnecting (`reconnect` or `new_connection`) resets the connection's `in_flight` set so any
///   unacked delivery in `queue` is redelivered from the head.
#[derive(Clone)]
pub struct MockSvcInbox {
    pub queue: Arc<Mutex<VecDeque<SvcDelivery>>>,
    pub acks: Arc<Mutex<Vec<String>>>,
    pub in_flight: Arc<Mutex<HashSet<String>>>,
    pub track_in_flight: Arc<AtomicBool>,
    pub node_name: Option<String>,
}

impl Default for MockSvcInbox {
    fn default() -> Self {
        Self::new()
    }
}

impl MockSvcInbox {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            acks: Arc::new(Mutex::new(Vec::new())),
            in_flight: Arc::new(Mutex::new(HashSet::new())),
            track_in_flight: Arc::new(AtomicBool::new(true)),
            node_name: Some("sini.test".to_string()),
        }
    }

    /// Disables XF6 per-connection in-flight tracking so `poll` re-serves the head unacked
    /// delivery on every call until `ack` removes it.
    pub fn without_in_flight(self) -> Self {
        self.track_in_flight.store(false, Ordering::SeqCst);
        self
    }

    pub fn push(&self, delivery: SvcDelivery) {
        self.queue.lock().unwrap().push_back(delivery);
    }

    pub fn push_json_frame(&self, frame: &Value) {
        if let Some(d) = SvcDelivery::from_deliver_frame(frame) {
            self.push(d);
        }
    }

    /// Opens a fresh connection view sharing `queue` and `acks` with its own empty `in_flight` set.
    pub fn new_connection(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            acks: Arc::clone(&self.acks),
            in_flight: Arc::new(Mutex::new(HashSet::new())),
            track_in_flight: Arc::clone(&self.track_in_flight),
            node_name: self.node_name.clone(),
        }
    }

    pub fn poll_now(&self) -> Option<SvcDelivery> {
        let q = self.queue.lock().unwrap();
        let mut in_flight = self.in_flight.lock().unwrap();
        let next = q
            .iter()
            .find(|d| !in_flight.contains(&d.message_id))
            .cloned();
        if let Some(ref d) = next {
            if self.track_in_flight.load(Ordering::SeqCst) {
                in_flight.insert(d.message_id.clone());
            }
        }
        next
    }

    pub fn poll_json_frame(&self) -> Value {
        match self.poll_now() {
            Some(d) => d.to_deliver_frame(),
            None => serde_json::json!({
                "status": "ok",
                "action": "timeout",
            }),
        }
    }

    pub fn ack_now(&self, message_id: &str) {
        self.acks.lock().unwrap().push(message_id.to_string());
        self.queue
            .lock()
            .unwrap()
            .retain(|d| d.message_id != message_id);
        self.in_flight.lock().unwrap().remove(message_id);
    }
}

#[async_trait]
impl SvcInbox for MockSvcInbox {
    async fn poll(&mut self, _wait_secs: u64) -> Result<Option<SvcDelivery>, AppError> {
        let next = self.poll_now();
        if next.is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(next)
    }

    async fn ack(&mut self, message_id: &str) -> Result<(), AppError> {
        self.ack_now(message_id);
        Ok(())
    }

    fn node(&self) -> Option<String> {
        self.node_name.clone()
    }
}

/// The caller's live service registration, using xmsg's shared transport.
pub struct SvcSocketInbox {
    service: RegisteredService,
    client: Arc<UnixXmsgClient>,
}

impl SvcSocketInbox {
    pub fn address(&self) -> Option<&str> {
        self.service.address()
    }

    pub fn client(&self) -> Arc<UnixXmsgClient> {
        self.client.clone()
    }
}

fn delivery_from_transport(delivery: Delivery) -> Result<SvcDelivery, AppError> {
    Ok(SvcDelivery {
        message_id: delivery.message_id().into(),
        from_name: delivery.from_name().into(),
        text: delivery.text().into(),
        envelope: delivery.envelope().into(),
        origin: Some(serde_json::to_value(delivery.origin()).map_err(|_| ClientError::Protocol)?),
        in_reply_to: delivery.in_reply_to().map(str::to_owned),
    })
}

#[async_trait]
impl SvcInbox for SvcSocketInbox {
    async fn poll(&mut self, wait_secs: u64) -> Result<Option<SvcDelivery>, AppError> {
        self.service
            .poll(wait_secs)
            .await?
            .map(delivery_from_transport)
            .transpose()
    }

    async fn ack(&mut self, message_id: &str) -> Result<(), AppError> {
        self.service.ack(message_id).await.map_err(Into::into)
    }

    fn node(&self) -> Option<String> {
        self.service.node().map(str::to_owned)
    }

    fn xmsg_client(&self) -> Option<Arc<dyn XmsgClient>> {
        Some(self.client.clone())
    }
}

/// Matrix accepts the UID-verified daemon's own node name; it never guesses one.
/// Callers can override bounds in the returned options before constructing the inbox.
pub fn client_options(config: &Config, service: &str) -> ClientOptions {
    ClientOptions {
        register_socket: config.xmsg_register_socket(),
        agent_socket: config.xmsg_agent_socket(),
        max_frame_bytes: 1024 * 1024,
        operation_timeout: Duration::from_secs(10),
        service: service.into(),
        expected_node: None,
        daemon_uid: current_uid(),
    }
}

async fn register_with_client(
    options: ClientOptions,
    client: Arc<UnixXmsgClient>,
) -> Result<SvcSocketInbox, AppError> {
    let service = RegisteredService::register(options).await?;
    *client.agent.lock().unwrap() = Some(service.agent_client());
    Ok(SvcSocketInbox { service, client })
}

pub async fn register_svc(options: ClientOptions) -> Result<SvcSocketInbox, AppError> {
    register_with_client(options, Arc::new(UnixXmsgClient::disconnected())).await
}

/// An auto-reconnecting service inbox that wraps `register_svc`.
/// When connection is lost or registration fails, reconnects with exponential backoff
/// (default: start 1s, cap 30s).
pub struct ReconnectingSvcInbox {
    options: ClientOptions,
    client: Arc<UnixXmsgClient>,
    current: Option<SvcSocketInbox>,
    min_backoff: Duration,
    max_backoff: Duration,
    current_backoff: Duration,
}

impl ReconnectingSvcInbox {
    pub fn new(options: ClientOptions) -> Self {
        let min_backoff = Duration::from_secs(1);
        Self {
            options,
            client: Arc::new(UnixXmsgClient::disconnected()),
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
            match register_with_client(self.options.clone(), self.client.clone()).await {
                Ok(inbox) => {
                    if let Some(address) = inbox.address() {
                        tracing::info!("Registered on xmsg as {address}");
                    }
                    self.current = Some(inbox);
                    self.current_backoff = self.min_backoff;
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to connect/register on xmsg at {}: {e}; retrying in {:?}",
                        self.options.register_socket.display(),
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

    fn xmsg_client(&self) -> Option<Arc<dyn XmsgClient>> {
        Some(self.client.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendResponse {
    pub message_id: String,
    pub outcome: Option<String>,
    /// The resolved address (`id[~n]@node` or `service@node`) read from `address` in `send`'s
    /// result. `None` for a reply.
    pub session_id: Option<String>,
}

impl SendResponse {
    pub fn new(message_id: impl Into<String>, session_id: Option<String>) -> Self {
        Self {
            message_id: message_id.into(),
            session_id,
            outcome: None,
        }
    }
}

impl From<String> for SendResponse {
    fn from(message_id: String) -> Self {
        Self {
            message_id,
            outcome: None,
            session_id: Some("mock-session@sini.cortex".to_string()),
        }
    }
}

impl From<&str> for SendResponse {
    fn from(message_id: &str) -> Self {
        Self {
            message_id: message_id.to_string(),
            outcome: None,
            session_id: Some("mock-session@sini.cortex".to_string()),
        }
    }
}

/// Stored is accepted success, including a leaf reply waiting to be pulled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplyOutcome {
    /// An ID is assigned; record.push_outcome still distinguishes pending delivery.
    Pushed {
        response: SendResponse,
        record: ReplyRecord,
    },
    Stored {
        record: ReplyRecord,
    },
}

impl ReplyOutcome {
    fn from_record(record: ReplyRecord) -> Self {
        match record
            .pushed_message_id
            .as_deref()
            .filter(|id| !id.is_empty())
        {
            Some(id) if record.push_outcome.as_deref() != Some("stored") => Self::Pushed {
                response: SendResponse {
                    message_id: id.into(),
                    session_id: None,
                    outcome: record.push_outcome.clone(),
                },
                record,
            },
            _ => Self::Stored { record },
        }
    }
}

#[async_trait]
pub trait XmsgClient: Send + Sync {
    /// Sends a message to `expert_ref` via agent.sock (`{"action":"send","to","text"}`).
    /// Replies are pushed into the bot's service inbox (A10).
    /// Returns the assigned ULID message_id and the resolved receiving address (`id@node`).
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
    ) -> Result<ReplyOutcome, AppError> {
        Err(AppError::Xmsg(
            "reply is not implemented by this client".into(),
        ))
    }

    /// `send_message` carrying an idempotency key (xmsg README, Idempotency): xmsg delivers
    /// one message per key and answers a repeat of it with the first send's `message_id`, with
    /// `outcome` `pending` while the first is still being delivered. A client that does not
    /// deduplicate sends unkeyed.
    async fn send_message_keyed(
        &self,
        expert_ref: &str,
        from: &str,
        text: &str,
        _idempotency_key: &str,
    ) -> Result<SendResponse, AppError> {
        self.send_message(expert_ref, from, text).await
    }

    /// `reply_message` carrying an idempotency key: a repeat returns the first reply's
    /// `pushedMessageId`. A client that does not deduplicate replies unkeyed.
    async fn reply_message_keyed(
        &self,
        message_id: &str,
        text: &str,
        _idempotency_key: &str,
    ) -> Result<ReplyOutcome, AppError> {
        self.reply_message(message_id, text).await
    }
}

/// Outbound facet of the inbox registration. It never holds a poll lock.
pub struct UnixXmsgClient {
    agent: Mutex<Option<AgentClient>>,
}

impl UnixXmsgClient {
    fn disconnected() -> Self {
        Self {
            agent: Mutex::new(None),
        }
    }

    fn agent(&self) -> Result<AgentClient, AppError> {
        self.agent
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| ClientError::Disconnected.into())
    }

    async fn send(
        &self,
        to: &str,
        text: &str,
        key: Option<&str>,
    ) -> Result<SendResponse, AppError> {
        let receipt = self.agent()?.send(to, text, key).await?;
        Ok(SendResponse {
            message_id: receipt.message_id,
            session_id: (!receipt.address.is_empty()).then_some(receipt.address),
            outcome: Some(receipt.outcome),
        })
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
        self.send(expert_ref, text, None).await
    }

    async fn send_message_keyed(
        &self,
        expert_ref: &str,
        _from: &str,
        text: &str,
        idempotency_key: &str,
    ) -> Result<SendResponse, AppError> {
        self.send(expert_ref, text, Some(idempotency_key)).await
    }

    async fn reply_message(&self, message_id: &str, text: &str) -> Result<ReplyOutcome, AppError> {
        Ok(ReplyOutcome::from_record(
            self.agent()?.reply(message_id, text, None).await?,
        ))
    }

    async fn reply_message_keyed(
        &self,
        message_id: &str,
        text: &str,
        idempotency_key: &str,
    ) -> Result<ReplyOutcome, AppError> {
        Ok(ReplyOutcome::from_record(
            self.agent()?
                .reply(message_id, text, Some(idempotency_key))
                .await?,
        ))
    }
}

pub fn create_xmsg_client<I: SvcInbox + ?Sized>(inbox: &I) -> Arc<dyn XmsgClient> {
    inbox
        .xmsg_client()
        .unwrap_or_else(|| Arc::new(UnixXmsgClient::disconnected()))
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
// Producer attestation; the consumer authorizes the transport origin before using this scope.
#[serde(rename_all = "snake_case")]
pub enum GuardReviewScope {
    All,
    UntrustedLines,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuardEnvelope {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_scope: Option<GuardReviewScope>,
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

struct GuardPendingEntry {
    guard_ref: String,
    resolved_address: Option<String>,
    tx: tokio::sync::oneshot::Sender<String>,
}

/// Guard requests awaiting their verdict, keyed by the guard request's message ID (AM-3, plan
/// §6 D6). The verdict arrives pushed into the bot's service inbox, and the inbox loop hands it
/// here by its `inReplyTo` (`route_guard_reply`); it never reaches the expert path.
///
/// One process-wide map. A check that times out evicts its entry: a verdict arriving after that
/// matches no entry and no forward, so the inbox loop ignores it and never posts it.
type GuardPending = tokio::sync::Mutex<HashMap<String, GuardPendingEntry>>;

fn guard_pending() -> &'static GuardPending {
    static PENDING: OnceLock<GuardPending> = OnceLock::new();
    PENDING.get_or_init(Default::default)
}

fn delivery_origin_session_id(delivery: &SvcDelivery) -> Option<String> {
    if let Some(ref orig) = delivery.origin {
        if let Some(id) = orig
            .get("service")
            .or_else(|| orig.get("id"))
            .and_then(|v| v.as_str())
        {
            let trimmed = id.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn delivery_origin_author_key(delivery: &SvcDelivery) -> Option<String> {
    if let Some(id) = delivery_origin_session_id(delivery) {
        if let Some(node) = delivery
            .origin
            .as_ref()
            .and_then(|o| o.get("node"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|n| !n.is_empty())
        {
            return Some(format!("{id}@{node}"));
        }
        return Some(id);
    }
    None
}

fn delivery_matches_ref(delivery: &SvcDelivery, expected_ref: &str) -> bool {
    let bound_addr = sender_address(expected_ref.trim());
    if bound_addr.is_empty() {
        return false;
    }
    if let Some(caller_key) = delivery_origin_author_key(delivery) {
        if bound_addr.contains('@') {
            return caller_key == bound_addr;
        }
        if let Some(caller_id) = delivery_origin_session_id(delivery) {
            return caller_id == bound_addr || caller_key == bound_addr;
        }
        return false;
    }
    if delivery.origin.is_none() {
        let caller_from = sender_address(delivery.from_name.trim());
        let caller_id = sender_id(delivery.from_name.trim());
        if bound_addr.contains('@') {
            return !caller_from.is_empty() && caller_from == bound_addr;
        }
        return !caller_from.is_empty() && (caller_from == bound_addr || caller_id == bound_addr);
    }
    false
}

/// Hands a pushed reply to the guard check waiting on `in_reply_to`, verifying first that
/// `delivery.origin` matches `guard_ref` (and the resolved guard address when known) before
/// removing or resolving the pending channel.
pub async fn route_guard_reply(in_reply_to: &str, delivery: &SvcDelivery, guard_ref: &str) -> bool {
    let mut pending = guard_pending().lock().await;
    let Some(entry) = pending.get(in_reply_to) else {
        return false;
    };
    let expected_ref = if !guard_ref.trim().is_empty() {
        guard_ref
    } else {
        entry.guard_ref.as_str()
    };
    let matches_ref = delivery_matches_ref(delivery, expected_ref);
    let matches_resolved = entry
        .resolved_address
        .as_deref()
        .is_none_or(|addr| delivery_matches_ref(delivery, addr));
    if !matches_ref || !matches_resolved {
        tracing::warn!(
            "Guard reply {} for request {} refused from unauthorized origin {:?} (from_name={}, expected={})",
            delivery.message_id,
            in_reply_to,
            delivery.origin,
            delivery.from_name,
            expected_ref
        );
        return false;
    }
    if let Some(entry) = pending.remove(in_reply_to) {
        let _ = entry.tx.send(delivery.text.clone());
        true
    } else {
        false
    }
}

pub struct UnixGuardClient<'a> {
    xmsg: &'a dyn XmsgClient,
    pub timeout_secs: u64,
    /// The bound on the guard request's `agent.sock` send. The pending map is locked for that
    /// send, and the inbox loop routes every pushed reply through the map, so this also bounds
    /// how long one guard check can hold up the inbox.
    pub send_timeout: Duration,
}

impl<'a> UnixGuardClient<'a> {
    pub fn new(xmsg: &'a dyn XmsgClient, timeout_secs: u64) -> Self {
        Self {
            xmsg,
            timeout_secs,
            send_timeout: Duration::from_secs(10),
        }
    }
}

pub const GUARD_REJECT_CLASSES: &[&str] = &[
    "directs_model",
    "seeks_holdings",
    "coerces_tool",
    "exfil_channel",
    "hidden_instruction",
    "guard_error",
];

pub fn is_valid_guard_reject_class(class: Option<&str>) -> bool {
    matches!(class, Some(c) if GUARD_REJECT_CLASSES.contains(&c))
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

    if verdict == "reject" {
        let class = obj.get("class").and_then(|c| c.as_str());
        if !is_valid_guard_reject_class(class) {
            tracing::warn!(
                class = ?class,
                reason = %reason,
                "genie-guard: reject without valid class ({:?}) treated as allow: {}",
                class,
                reason
            );
            return Ok(GuardVerdict {
                verdict: "allow".to_string(),
                reason,
                cleaned_text: String::new(),
            });
        }
    }

    if verdict == "rewrite" && cleaned_text.trim().is_empty() {
        return Err(AppError::Xmsg(
            "Missing or empty 'cleaned_text' in rewrite verdict".to_string(),
        ));
    }

    Ok(GuardVerdict {
        verdict: verdict.to_string(),
        reason,
        cleaned_text,
    })
}

#[async_trait]
impl GuardClient for UnixGuardClient<'_> {
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
            let send = self.xmsg.send_message(guard_ref, "", &env_text);
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
            pending.insert(
                sent.message_id.clone(),
                GuardPendingEntry {
                    guard_ref: guard_ref.to_string(),
                    resolved_address: sent.session_id.clone(),
                    tx,
                },
            );
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

pub fn create_guard_client<'a>(
    config: &Config,
    xmsg: &'a dyn XmsgClient,
) -> Arc<dyn GuardClient + 'a> {
    Arc::new(UnixGuardClient::new(xmsg, config.guard_timeout_secs))
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

        let empty = r#"{"verdict": "rewrite", "reason": "sanitized", "cleaned_text": ""}"#;
        assert!(parse_guard_reply(empty).is_err());

        let whitespace = r#"{"verdict": "rewrite", "reason": "sanitized", "cleaned_text": "   "}"#;
        assert!(parse_guard_reply(whitespace).is_err());
    }

    #[test]
    fn test_parse_guard_reply_reject() {
        for class in GUARD_REJECT_CLASSES {
            let text = serde_json::json!({
                "verdict": "reject",
                "class": class,
                "reason": "attack detected",
                "cleaned_text": ""
            })
            .to_string();
            let v = parse_guard_reply(&text).unwrap();
            assert_eq!(v.verdict, "reject", "class {class} must remain reject");
            assert_eq!(v.reason, "attack detected");
        }
    }

    #[test]
    fn oracle_gi12b_reject_without_valid_class_treated_as_allow_and_guard_error_stays_reject() {
        for bad in [
            r#"{"verdict": "reject", "reason": "status report, not a question", "cleaned_text": ""}"#,
            r#"{"verdict": "reject", "class": "not_a_question", "reason": "not a question", "cleaned_text": ""}"#,
            r#"{"verdict": "reject", "class": "", "reason": "empty class", "cleaned_text": ""}"#,
        ] {
            let v = parse_guard_reply(bad).unwrap();
            assert_eq!(
                v.verdict, "allow",
                "classless/invalid-class reject must become allow: {bad}"
            );
            assert_eq!(v.cleaned_text, "");
        }

        let guard_err = r#"{"verdict": "reject", "class": "guard_error", "reason": "guard-in output invalid: unparseable", "cleaned_text": ""}"#;
        let v_err = parse_guard_reply(guard_err).unwrap();
        assert_eq!(
            v_err.verdict, "reject",
            "guard_error class must stay reject"
        );
        assert_eq!(v_err.reason, "guard-in output invalid: unparseable");
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
