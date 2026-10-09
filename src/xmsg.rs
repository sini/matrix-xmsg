use crate::config::Config;
use crate::error::AppError;
use async_trait::async_trait;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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

/// A delivery received through the xmsg service inbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SvcDelivery {
    pub message_id: String,
    pub from_name: String,
    pub text: String,
    pub envelope: String,
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

                Ok(Some(SvcDelivery {
                    message_id,
                    from_name,
                    text,
                    envelope,
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
}

pub fn create_xmsg_client(config: &Config) -> Arc<dyn XmsgClient> {
    Arc::new(UnixXmsgClient::new(config.xmsg_agent_socket()))
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
}
