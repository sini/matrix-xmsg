use crate::config::Config;
use crate::context::{build_envelope, EventMessage};
use crate::error::AppError;
use crate::matrix::{is_bot_mentioned, MatrixClient};
use crate::sender_map::map_sender_mxid;
use crate::store::Store;
use crate::xmsg::XmsgClient;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BotOutcome {
    IgnoredRoom,
    IgnoredNoMention,
    IgnoredUntrustedUser,
    IgnoredSelf,
    SizeCapRefusal,
    RateLimitRefusal,
    EscalatedUserRequest,
    Replied,
    RepliedAndEscalated,
    TimedOutAndEscalated,
}

#[derive(Debug, Clone)]
pub struct IncomingMatrixEvent {
    pub room_id: String,
    pub event_id: String,
    pub sender_mxid: String,
    pub body: String,
    pub formatted_body: Option<String>,
    pub mentions: Option<Vec<String>>,
    pub timestamp_ms: i64,
    pub thread_root_id: Option<String>,
}

pub async fn handle_incoming_event(
    event: &IncomingMatrixEvent,
    room_history: &[EventMessage],
    config: &Config,
    matrix: &dyn MatrixClient,
    xmsg: &dyn XmsgClient,
    store: &Store,
) -> Result<BotOutcome, AppError> {
    // 0. Self-Sender Gate: Drop silently if sender is the bot itself (F8)
    if event.sender_mxid == config.bot_mxid {
        return Ok(BotOutcome::IgnoredSelf);
    }

    // 1. Room Allowlist Gate: Drop silently if not allowlisted
    if !config.is_room_allowlisted(&event.room_id) {
        return Ok(BotOutcome::IgnoredRoom);
    }

    // 2. Mention / Command Gate: Drop silently if not mentioned and not !escalate in a thread
    let is_thread_escalate = event.thread_root_id.is_some() && event.body.trim() == "!escalate";
    let is_mentioned = is_bot_mentioned(
        &event.body,
        event.formatted_body.as_deref(),
        event.mentions.as_deref(),
        &config.bot_mxid,
    );

    if !is_thread_escalate && !is_mentioned {
        return Ok(BotOutcome::IgnoredNoMention);
    }

    // 3. Trusted User Allowlist Gate: Drop SILENTLY if sender is not on the trusted allowlist!
    // (Prevents oracle probing of allowlist membership)
    if !config.is_user_trusted(&event.sender_mxid) {
        return Ok(BotOutcome::IgnoredUntrustedUser);
    }

    let thread_root = event.thread_root_id.as_deref().unwrap_or(&event.event_id);
    let now_secs = event.timestamp_ms / 1000;

    // 4. Size Cap Gate
    if event.body.len() > config.size_cap_bytes {
        let notice = format!(
            "Message exceeds size limit of {} bytes.",
            config.size_cap_bytes
        );
        matrix
            .send_notice(
                &event.room_id,
                Some(thread_root),
                &notice,
                Some(&event.sender_mxid),
            )
            .await?;
        return Ok(BotOutcome::SizeCapRefusal);
    }

    // 5. Rate Limit Gate
    if store
        .check_and_record_rate_limit(
            &event.sender_mxid,
            config.rate_limit_count,
            config.rate_limit_window_secs,
            now_secs,
        )
        .is_err()
    {
        let notice = "Rate limit exceeded. Please wait before asking another question.";
        matrix
            .send_notice(
                &event.room_id,
                Some(thread_root),
                notice,
                Some(&event.sender_mxid),
            )
            .await?;
        return Ok(BotOutcome::RateLimitRefusal);
    }

    // 6. Explicit User !escalate Command
    if event.body.trim() == "!escalate" {
        let dm_text = format!(
            "[matrix-xmsg] User escalation requested in room {} thread {} by {}",
            event.room_id, thread_root, event.sender_mxid
        );
        matrix.send_dm(&config.owner_mxid, &dm_text).await?;

        let notice = "A human has been notified.";
        matrix
            .send_notice(
                &event.room_id,
                Some(thread_root),
                notice,
                Some(&event.sender_mxid),
            )
            .await?;
        return Ok(BotOutcome::EscalatedUserRequest);
    }

    // 7. Context & Envelope Assembly
    let trigger_msg = EventMessage {
        event_id: event.event_id.clone(),
        sender_mxid: event.sender_mxid.clone(),
        timestamp_ms: event.timestamp_ms,
        body: event.body.clone(),
        thread_root_id: event.thread_root_id.clone(),
    };

    let envelope = build_envelope(
        &event.room_id,
        thread_root,
        &trigger_msg,
        room_history,
        &config.trusted_mxids,
        config.history_n,
        config.history_byte_cap,
    );

    // 8. Send to xmsg Expert Inbox
    let mapped_sender = map_sender_mxid(&event.sender_mxid);
    let message_id = xmsg
        .send_message(&config.expert_ref, &mapped_sender, &envelope)
        .await?;

    // Record mapping in SQLite
    store.record_thread_message(thread_root, &message_id, now_secs)?;

    // 9. Long-poll for Expert Reply
    match xmsg
        .wait_for_reply(&message_id, config.answer_timeout_secs)
        .await
    {
        Ok(reply_text) => {
            let trimmed = reply_text.trim();
            if trimmed.ends_with("[escalate]") {
                let clean_reply = trimmed.strip_suffix("[escalate]").unwrap_or(trimmed).trim();
                let dm_text = format!(
                    "[matrix-xmsg] Expert requested escalation in room {} thread {} for message {}",
                    event.room_id, thread_root, message_id
                );
                matrix.send_dm(&config.owner_mxid, &dm_text).await?;

                matrix
                    .send_notice(
                        &event.room_id,
                        Some(thread_root),
                        clean_reply,
                        Some(&event.sender_mxid),
                    )
                    .await?;
                Ok(BotOutcome::RepliedAndEscalated)
            } else {
                matrix
                    .send_notice(
                        &event.room_id,
                        Some(thread_root),
                        trimmed,
                        Some(&event.sender_mxid),
                    )
                    .await?;
                Ok(BotOutcome::Replied)
            }
        }
        Err(AppError::Timeout(_)) => {
            let dm_text = format!(
                "[matrix-xmsg] Answer timeout in room {} thread {} for message {}",
                event.room_id, thread_root, message_id
            );
            matrix.send_dm(&config.owner_mxid, &dm_text).await?;

            let notice = "The expert has not answered; a human has been notified.";
            matrix
                .send_notice(
                    &event.room_id,
                    Some(thread_root),
                    notice,
                    Some(&event.sender_mxid),
                )
                .await?;
            Ok(BotOutcome::TimedOutAndEscalated)
        }
        Err(e) => Err(e),
    }
}

/// Extracts an `IncomingMatrixEvent` from a Matrix SDK `SyncRoomMessageEvent`.
pub fn extract_incoming_event(
    event: &matrix_sdk::ruma::events::room::message::SyncRoomMessageEvent,
    room_id: &matrix_sdk::ruma::RoomId,
) -> Option<IncomingMatrixEvent> {
    use matrix_sdk::ruma::events::room::message::{MessageType, Relation, SyncRoomMessageEvent};

    if let SyncRoomMessageEvent::Original(orig) = event {
        // F3: Edits (m.replace) must not re-trigger questions
        if let Some(Relation::Replacement(_)) = &orig.content.relates_to {
            return None;
        }

        let formatted_body = match &orig.content.msgtype {
            MessageType::Text(t) => t.formatted.as_ref().map(|f| f.body.clone()),
            MessageType::Notice(n) => n.formatted.as_ref().map(|f| f.body.clone()),
            _ => None,
        };
        let mentions = orig
            .content
            .mentions
            .as_ref()
            .map(|m| m.user_ids.iter().map(|u| u.to_string()).collect::<Vec<_>>());
        let thread_root_id = match &orig.content.relates_to {
            Some(Relation::Thread(t)) => Some(t.event_id.to_string()),
            _ => None,
        };
        Some(IncomingMatrixEvent {
            room_id: room_id.to_string(),
            event_id: orig.event_id.to_string(),
            sender_mxid: orig.sender.to_string(),
            body: orig.content.body().to_string(),
            formatted_body,
            mentions,
            timestamp_ms: u64::from(orig.origin_server_ts.0) as i64,
            thread_root_id,
        })
    } else {
        None
    }
}

#[derive(Debug, Clone)]
pub struct InFlightQuestion {
    pub room_id: String,
    pub thread_root: String,
    pub sender_mxid: String,
}

#[derive(Clone, Default)]
pub struct TaskTracker {
    counter: std::sync::Arc<std::sync::atomic::AtomicU64>,
    tasks: std::sync::Arc<
        tokio::sync::Mutex<
            std::collections::HashMap<u64, (InFlightQuestion, tokio::task::AbortHandle)>,
        >,
    >,
}

impl TaskTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn track<F, Fut>(
        &self,
        question: InFlightQuestion,
        future_fn: F,
    ) -> tokio::task::JoinHandle<()>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let id = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let tasks = self.tasks.clone();
        let handle = tokio::spawn(async move {
            future_fn().await;
            tasks.lock().await.remove(&id);
        });
        self.tasks
            .lock()
            .await
            .insert(id, (question, handle.abort_handle()));
        handle
    }

    pub async fn drain_or_notify(
        &self,
        matrix: &dyn MatrixClient,
        grace_period: std::time::Duration,
    ) {
        let start = tokio::time::Instant::now();
        loop {
            {
                let guard = self.tasks.lock().await;
                if guard.is_empty() {
                    return;
                }
            }
            if start.elapsed() >= grace_period {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        // Grace period expired: abort in-flight tasks and post notice
        let remaining: Vec<(InFlightQuestion, tokio::task::AbortHandle)> = {
            let mut guard = self.tasks.lock().await;
            guard.drain().map(|(_, v)| v).collect()
        };

        for (q, abort_handle) in remaining {
            abort_handle.abort();
            let notice = "The bot is restarting; please re-ask your question in a moment.";
            let _ = matrix
                .send_notice(
                    &q.room_id,
                    Some(&q.thread_root),
                    notice,
                    Some(&q.sender_mxid),
                )
                .await;
        }
    }
}

/// Registers the Matrix SDK event handlers to dispatch room message events through the bot gating pipeline.
pub fn register_event_handlers(
    client: &matrix_sdk::Client,
    config: std::sync::Arc<Config>,
    matrix: std::sync::Arc<dyn MatrixClient>,
    xmsg: std::sync::Arc<dyn XmsgClient>,
    store: std::sync::Arc<Store>,
) -> TaskTracker {
    let startup_ts = chrono::Utc::now().timestamp_millis();
    let tracker = TaskTracker::new();
    register_event_handlers_full(
        client,
        config,
        matrix,
        xmsg,
        store,
        startup_ts,
        tracker.clone(),
    );
    tracker
}

/// Registers event handlers with an explicit startup cutoff timestamp (useful for testing backlog suppression).
pub fn register_event_handlers_with_startup_ts(
    client: &matrix_sdk::Client,
    config: std::sync::Arc<Config>,
    matrix: std::sync::Arc<dyn MatrixClient>,
    xmsg: std::sync::Arc<dyn XmsgClient>,
    store: std::sync::Arc<Store>,
    startup_ts: i64,
) -> TaskTracker {
    let tracker = TaskTracker::new();
    register_event_handlers_full(
        client,
        config,
        matrix,
        xmsg,
        store,
        startup_ts,
        tracker.clone(),
    );
    tracker
}

/// Registers event handlers with an explicit startup cutoff timestamp and task tracker.
pub fn register_event_handlers_full(
    client: &matrix_sdk::Client,
    config: std::sync::Arc<Config>,
    matrix: std::sync::Arc<dyn MatrixClient>,
    xmsg: std::sync::Arc<dyn XmsgClient>,
    store: std::sync::Arc<Store>,
    startup_ts: i64,
    tracker: TaskTracker,
) {
    matrix.set_store(store.clone());
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(16));

    client.add_event_handler(
        move |event: matrix_sdk::ruma::events::room::message::SyncRoomMessageEvent,
              room: matrix_sdk::room::Room| {
            let config = config.clone();
            let matrix = matrix.clone();
            let xmsg = xmsg.clone();
            let store = store.clone();
            let sem = semaphore.clone();
            let tracker = tracker.clone();

            async move {
                if let Some(incoming) = extract_incoming_event(&event, room.room_id()) {
                    // F1 & N4: Backlog suppression with 10s clock skew tolerance
                    if incoming.timestamp_ms < startup_ts - 10_000 {
                        tracing::debug!(
                            "Ignoring historical backlog event {} (ts {} < startup {})",
                            incoming.event_id,
                            incoming.timestamp_ms,
                            startup_ts
                        );
                        return;
                    }

                    // F8 & N7: Self-sender guard
                    if incoming.sender_mxid == config.bot_mxid {
                        return;
                    }

                    // N3 Gate 1: Room allowlist check (synchronous)
                    if !config.is_room_allowlisted(&incoming.room_id) {
                        return;
                    }

                    // N3 Gate 2: Mention / Command check (synchronous)
                    let is_thread_escalate =
                        incoming.thread_root_id.is_some() && incoming.body.trim() == "!escalate";
                    let is_mentioned = is_bot_mentioned(
                        &incoming.body,
                        incoming.formatted_body.as_deref(),
                        incoming.mentions.as_deref(),
                        &config.bot_mxid,
                    );
                    if !is_thread_escalate && !is_mentioned {
                        return;
                    }

                    // N3 Gate 3: Trusted user check (synchronous)
                    if !config.is_user_trusted(&incoming.sender_mxid) {
                        return;
                    }

                    // All synchronous gates passed! Track in-flight task and spawn pipeline.
                    let thread_root = incoming
                        .thread_root_id
                        .clone()
                        .unwrap_or_else(|| incoming.event_id.clone());

                    let question = InFlightQuestion {
                        room_id: incoming.room_id.clone(),
                        thread_root,
                        sender_mxid: incoming.sender_mxid.clone(),
                    };

                    tracker
                        .track(question, move || async move {
                            let _permit = match sem.acquire_owned().await {
                                Ok(p) => p,
                                Err(_) => return,
                            };

                            let history = matrix
                                .fetch_history(&incoming.room_id, config.history_n * 2)
                                .await
                                .unwrap_or_default();

                            if let Err(e) = handle_incoming_event(
                                &incoming,
                                &history,
                                &config,
                                matrix.as_ref(),
                                xmsg.as_ref(),
                                &store,
                            )
                            .await
                            {
                                tracing::error!(
                                    "Error handling incoming event {}: {e}",
                                    incoming.event_id
                                );
                            }
                        })
                        .await;
                }
            }
        },
    );
}

/// Runs the continuous Matrix sync loop until a shutdown signal is received.
pub async fn run_daemon_loop(
    client: &matrix_sdk::Client,
    store: std::sync::Arc<Store>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), AppError> {
    use matrix_sdk::config::SyncSettings;
    let mut sync_settings = SyncSettings::default();

    // F1 & N9: Resume from persisted sync token if present
    if let Ok(Some(saved_token)) = store.get_sync_token() {
        tracing::info!("Resuming sync from saved token: {saved_token}");
        sync_settings = sync_settings.token(saved_token);
    }

    loop {
        tokio::select! {
            _ = shutdown.recv() => {
                tracing::info!("Shutdown signal received, exiting sync loop");
                break;
            }
            res = client.sync_once(sync_settings.clone()) => {
                match res {
                    Ok(sync_resp) => {
                        sync_settings = sync_settings.token(sync_resp.next_batch.clone());
                        if let Err(e) = store.set_sync_token(&sync_resp.next_batch) {
                            tracing::error!("Failed to persist sync token: {e}");
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Sync error: {e}, retrying in 3 seconds");
                        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Runs the continuous Matrix sync loop and drains in-flight tasks upon shutdown.
pub async fn run_daemon_loop_with_drain(
    client: &matrix_sdk::Client,
    store: std::sync::Arc<Store>,
    matrix: &dyn MatrixClient,
    tracker: &TaskTracker,
    grace_period: std::time::Duration,
    shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), AppError> {
    run_daemon_loop(client, store, shutdown).await?;
    tracker.drain_or_notify(matrix, grace_period).await;
    Ok(())
}
