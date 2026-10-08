use crate::error::AppError;
use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;
use std::time::{Duration, Instant};

#[async_trait]
pub trait XmsgClient: Send + Sync {
    /// Injects a message into the expert session inbox via POST /v1/sessions/{expert}/messages.
    /// Returns the assigned ULID message_id.
    async fn send_message(
        &self,
        expert_ref: &str,
        from: &str,
        text: &str,
    ) -> Result<String, AppError>;

    /// Long-polls /v1/messages/{id}/replies until a reply arrives or timeout expires.
    async fn wait_for_reply(&self, message_id: &str, timeout_secs: u64)
        -> Result<String, AppError>;
}

pub struct HttpXmsgClient {
    client: Client,
    base_url: String,
}

impl HttpXmsgClient {
    pub fn new(base_url: String) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }
}

#[async_trait]
impl XmsgClient for HttpXmsgClient {
    async fn send_message(
        &self,
        expert_ref: &str,
        from: &str,
        text: &str,
    ) -> Result<String, AppError> {
        let url = format!("{}/v1/sessions/{expert_ref}/messages", self.base_url);
        let resp = self
            .client
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
            .ok_or_else(|| AppError::Xmsg("Missing messageId in response".to_string()))?;

        Ok(message_id.to_string())
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

            let url = format!(
                "{}/v1/messages/{message_id}/replies?after={after_seq}&wait={poll_wait}",
                self.base_url
            );

            let resp = self.client.get(&url).send().await;
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
}
