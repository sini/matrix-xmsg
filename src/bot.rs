use crate::config::{Admission, Config};
use crate::context::{build_envelope, ContextMode, EventMessage};
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
    TimedOutSilent,
    Declined,
    AbortedByRestart,
    ControlEmitted,
    ControlDebounced,
    IgnoredEdit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncomingReactionEvent {
    pub room_id: String,
    pub event_id: String,
    pub sender_mxid: String,
    pub relates_to_event_id: String,
    pub key: String,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlAction {
    Accept,
    Deeper,
}

impl ControlAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Deeper => "deeper",
        }
    }
}

pub fn parse_reaction_control(key: &str) -> Option<ControlAction> {
    let trimmed = key.trim();
    if trimmed.contains('✅') {
        Some(ControlAction::Accept)
    } else if trimmed.contains('🔍') || trimmed.contains('🔎') {
        Some(ControlAction::Deeper)
    } else {
        None
    }
}

/// Renders an expert reply. If the reply is JSON `{"silent": true}`, returns `None`
/// indicating the expert declined silently. If the reply is JSON carrying `confidence` (0..1),
/// `gaps[]` or `tier: 1|expert`, renders the answer, confidence line, and controls hint line.
/// Replies with `tier: expert` are labelled "expert review".
/// Plain-text replies with no JSON render exactly as pre-M5.
/// Returns Option<(rendered_text, is_escalate)>.
pub fn format_expert_reply(reply_text: &str) -> Option<(String, bool)> {
    let trimmed = reply_text.trim();

    if let Ok(val) = serde_json::from_str::<serde_json::Value>(trimmed) {
        if let Some(obj) = val.as_object() {
            if obj.get("silent").and_then(|v| v.as_bool()).unwrap_or(false) {
                return None;
            }
            if let Some(answer_val) = obj
                .get("answer")
                .or_else(|| obj.get("text"))
                .or_else(|| obj.get("body"))
                .and_then(|v| v.as_str())
            {
                let is_escalate = obj
                    .get("escalate")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                    || obj
                        .get("escalate_forced")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                    || answer_val.trim().ends_with("[escalate]");
                let clean_answer = answer_val
                    .trim()
                    .strip_suffix("[escalate]")
                    .unwrap_or(answer_val.trim())
                    .trim();

                let is_expert = obj
                    .get("tier")
                    .and_then(|t| {
                        t.as_str()
                            .map(|s| s == "expert")
                            .or_else(|| t.as_i64().map(|n| n > 1))
                    })
                    .unwrap_or(false);

                let mut meta_parts = Vec::new();
                if is_expert {
                    meta_parts.push("expert review".to_string());
                }
                if let Some(conf) = obj.get("confidence").and_then(|c| c.as_f64()) {
                    meta_parts.push(format!("confidence {:.2}", conf));
                }
                if let Some(gaps_val) = obj.get("gaps") {
                    if let Some(gaps_arr) = gaps_val.as_array() {
                        let gaps: Vec<&str> = gaps_arr.iter().filter_map(|v| v.as_str()).collect();
                        if !gaps.is_empty() {
                            meta_parts.push(format!("gaps: {}", gaps.join(", ")));
                        } else {
                            meta_parts.push("gaps: none".to_string());
                        }
                    }
                }

                let mut rendered = String::new();
                if is_expert {
                    rendered.push_str("[expert review]\n");
                }
                rendered.push_str(clean_answer);

                if !meta_parts.is_empty() {
                    rendered.push_str("\n\n");
                    rendered.push_str(&meta_parts.join(" · "));
                }

                rendered.push_str("\nControls: ✅ accept · 🔍 deeper · !deeper");

                return Some((rendered, is_escalate));
            }
        }
    }

    if trimmed.ends_with("[escalate]") {
        let clean = trimmed.strip_suffix("[escalate]").unwrap_or(trimmed).trim();
        Some((clean.to_string(), true))
    } else {
        Some((trimmed.to_string(), false))
    }
}

pub use crate::matrix::IncomingMatrixEvent;

pub async fn handle_incoming_event(
    event: &IncomingMatrixEvent,
    room_history: &[EventMessage],
    config: &Config,
    matrix: &dyn MatrixClient,
    xmsg: &dyn XmsgClient,
    store: &Store,
) -> Result<BotOutcome, AppError> {
    handle_incoming_event_with_claim(event, room_history, config, matrix, xmsg, store, None).await
}

pub async fn handle_incoming_event_with_claim(
    event: &IncomingMatrixEvent,
    room_history: &[EventMessage],
    config: &Config,
    matrix: &dyn MatrixClient,
    xmsg: &dyn XmsgClient,
    store: &Store,
    claimed: Option<&std::sync::atomic::AtomicBool>,
) -> Result<BotOutcome, AppError> {
    // 0. Self-Sender Gate: Drop silently if sender is the bot itself (F8)
    if event.sender_mxid == config.bot_mxid {
        return Ok(BotOutcome::IgnoredSelf);
    }

    // 1. Room Allowlist Gate: Drop silently if not allowlisted
    if !config.is_room_allowlisted(&event.room_id) {
        return Ok(BotOutcome::IgnoredRoom);
    }

    let (thread_root, claim_id, reaction_target_id, trigger_msg, is_addressed) =
        if let Some(ref orig_event_id) = event.replaces_event_id {
            // M10 Rule 1: Replacement content must mention the bot or be a reply to the bot
            let is_mentioned = is_bot_mentioned(
                &event.body,
                event.formatted_body.as_deref(),
                event.mentions.as_deref(),
                &config.bot_mxid,
            );
            let is_reply_to_bot = if let Some(ref in_reply_to) = event.in_reply_to_event_id {
                if !event.is_falling_back {
                    store.get_bot_message_thread(in_reply_to)?.is_some()
                } else {
                    false
                }
            } else {
                false
            };
            if !is_mentioned && !is_reply_to_bot {
                return Ok(BotOutcome::IgnoredNoMention);
            }

            // M10 Rule 3: Original event must never have been relayed
            if store.is_event_relayed(orig_event_id)? {
                return Ok(BotOutcome::IgnoredEdit);
            }

            // Fetch original event from history or Matrix API
            let orig_event = if let Some(msg) =
                room_history.iter().find(|m| m.event_id == *orig_event_id)
            {
                Some(msg.clone())
            } else {
                match matrix.fetch_event(&event.room_id, orig_event_id).await {
                    Ok(Some(msg)) => Some(msg),
                    Ok(None) => None,
                    Err(e) => {
                        tracing::debug!("Failed to fetch original event {}: {}", orig_event_id, e);
                        None
                    }
                }
            };

            let Some(orig) = orig_event else {
                tracing::debug!(
                    "Original event {} could not be read; dropping edit {}",
                    orig_event_id,
                    event.event_id
                );
                return Ok(BotOutcome::IgnoredEdit);
            };

            // M10 Rule 2: Original event must NOT have mentioned the bot
            let orig_mentioned = is_bot_mentioned(&orig.body, None, None, &config.bot_mxid);
            if orig_mentioned {
                return Ok(BotOutcome::IgnoredEdit);
            }

            // Admission: Sender of edit must match original sender
            if event.sender_mxid != orig.sender_mxid {
                return Ok(BotOutcome::IgnoredUntrustedUser);
            }

            // Admission: Sender must pass standard admission policy
            let is_trusted = config.is_user_trusted(&event.sender_mxid);
            let is_top_level = orig.thread_root_id.is_none();
            let is_asker = if let Some(ref root) = orig.thread_root_id {
                store.get_thread_asker(root)?.as_deref() == Some(&event.sender_mxid)
            } else {
                false
            };
            let is_admitted = match config.admission {
                Admission::Trusted => is_trusted,
                Admission::Public => is_trusted || is_top_level || is_asker,
            };
            if !is_admitted {
                return Ok(BotOutcome::IgnoredUntrustedUser);
            }

            let thread_root = orig
                .thread_root_id
                .as_deref()
                .unwrap_or(&orig.event_id)
                .to_string();
            let claim_id = orig.event_id.clone();
            let reaction_target_id = orig.event_id.clone();
            let trigger_msg = EventMessage {
                event_id: orig.event_id.clone(),
                sender_mxid: event.sender_mxid.clone(),
                timestamp_ms: event.timestamp_ms,
                body: event.body.clone(),
                thread_root_id: orig.thread_root_id.clone(),
            };
            (thread_root, claim_id, reaction_target_id, trigger_msg, true)
        } else {
            let is_thread_escalate =
                event.thread_root_id.is_some() && event.body.trim() == "!escalate";
            let is_thread_deeper = event.thread_root_id.is_some() && event.body.trim() == "!deeper";
            let is_mentioned = is_bot_mentioned(
                &event.body,
                event.formatted_body.as_deref(),
                event.mentions.as_deref(),
                &config.bot_mxid,
            );
            let is_reply_to_bot = if let Some(ref in_reply_to) = event.in_reply_to_event_id {
                if !event.is_falling_back {
                    store.get_bot_message_thread(in_reply_to)?.is_some()
                } else {
                    false
                }
            } else {
                false
            };
            let is_addressed =
                is_mentioned || is_reply_to_bot || is_thread_escalate || is_thread_deeper;

            let is_engaged_thread = if let Some(ref root) = event.thread_root_id {
                store.is_thread_engaged(root)?
            } else {
                false
            };

            if !is_addressed && !is_engaged_thread {
                return Ok(BotOutcome::IgnoredNoMention);
            }

            let thread_root = event
                .thread_root_id
                .as_deref()
                .unwrap_or(&event.event_id)
                .to_string();
            let now_secs = event.timestamp_ms / 1000;

            // Handle !deeper control command in thread
            if is_thread_deeper {
                let asker = store.get_thread_asker(&thread_root)?;
                let is_asker = asker.as_deref() == Some(&event.sender_mxid);
                let is_trusted = config.is_user_trusted(&event.sender_mxid);

                if !is_asker && !is_trusted {
                    return Ok(BotOutcome::IgnoredUntrustedUser);
                }

                let target_msg = store
                    .get_latest_bot_message_in_thread(&thread_root)?
                    .unwrap_or_else(|| thread_root.clone());

                let is_new = store.record_control_if_new(
                    &target_msg,
                    &event.sender_mxid,
                    "deeper",
                    now_secs,
                )?;

                if !is_new {
                    return Ok(BotOutcome::ControlDebounced);
                }

                let payload = serde_json::json!({
                    "thread_id": thread_root,
                    "control": "deeper",
                    "by": event.sender_mxid,
                })
                .to_string();

                let mapped_sender = map_sender_mxid(&event.sender_mxid);
                xmsg.send_message(&config.expert_ref, &mapped_sender, &payload)
                    .await?;

                return Ok(BotOutcome::ControlEmitted);
            }

            // Admission Gate for normal event
            let is_trusted = config.is_user_trusted(&event.sender_mxid);
            let is_top_level = event.thread_root_id.is_none();
            let is_asker = if let Some(ref root) = event.thread_root_id {
                store.get_thread_asker(root)?.as_deref() == Some(&event.sender_mxid)
            } else {
                false
            };
            let is_admitted = match config.admission {
                Admission::Trusted => is_trusted,
                Admission::Public => is_trusted || is_top_level || is_asker,
            };
            if !is_admitted {
                return Ok(BotOutcome::IgnoredUntrustedUser);
            }

            let claim_id = event.event_id.clone();
            let reaction_target_id = event.event_id.clone();
            let trigger_msg = EventMessage {
                event_id: event.event_id.clone(),
                sender_mxid: event.sender_mxid.clone(),
                timestamp_ms: event.timestamp_ms,
                body: event.body.clone(),
                thread_root_id: event.thread_root_id.clone(),
            };
            (
                thread_root,
                claim_id,
                reaction_target_id,
                trigger_msg,
                is_addressed,
            )
        };

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
                Some(&thread_root),
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
                Some(&thread_root),
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
                Some(&thread_root),
                notice,
                Some(&event.sender_mxid),
            )
            .await?;
        return Ok(BotOutcome::EscalatedUserRequest);
    }

    // 7. Context & Envelope Assembly
    let cursor_opt = store.get_thread_cursor(&thread_root)?;
    let mode = match cursor_opt {
        None => ContextMode::Bootstrap,
        Some(ref cursor) => {
            let elapsed = now_secs.saturating_sub(cursor.forwarded_at);
            if elapsed >= config.session_live_secs as i64 {
                ContextMode::Bootstrap
            } else {
                ContextMode::Delta {
                    cursor_event_id: Some(cursor.cursor_event_id.clone()),
                }
            }
        }
    };

    let envelope = build_envelope(
        &event.room_id,
        &thread_root,
        &trigger_msg,
        room_history,
        &config.trusted_mxids,
        Some(&config.owner_mxid),
        config.history_n,
        config.history_byte_cap,
        is_addressed,
        mode.clone(),
    );

    // Record relay claim in SQLite
    store.record_event_relayed(&claim_id, now_secs)?;

    // 8. Send to xmsg Expert Inbox
    let mapped_sender = map_sender_mxid(&event.sender_mxid);
    let send_resp = xmsg
        .send_message(&config.expert_ref, &mapped_sender, &envelope)
        .await?;
    let mut message_id = send_resp.message_id;
    let mut session_id = send_resp.session_id;

    let delta_message_id = message_id.clone();
    let mut rebootstrap_message_id: Option<String> = None;

    // M16: If forward was sent as Delta, check if the receiving session differs from the cursor's session.
    // If different (including pre-migration rows with no session id), immediately send one bootstrap to the same ref.
    if matches!(mode, ContextMode::Delta { .. }) {
        let cursor_sess = cursor_opt.as_ref().and_then(|c| c.session_id.as_deref());
        let new_sess = session_id.as_deref();
        let session_changed = match (cursor_sess, new_sess) {
            (Some(c), Some(n)) => c != n,
            // A missing cursor session id is treated as different, so existing rows re-bootstrap once.
            (None, _) => true,
            (Some(_), None) => true,
        };

        if session_changed {
            let bootstrap_envelope = build_envelope(
                &event.room_id,
                &thread_root,
                &trigger_msg,
                room_history,
                &config.trusted_mxids,
                Some(&config.owner_mxid),
                config.history_n,
                config.history_byte_cap,
                is_addressed,
                ContextMode::Rebootstrap {
                    supersedes: delta_message_id.clone(),
                },
            );

            let boot_resp = xmsg
                .send_message(&config.expert_ref, &mapped_sender, &bootstrap_envelope)
                .await?;

            rebootstrap_message_id = Some(boot_resp.message_id.clone());
            message_id = boot_resp.message_id;
            session_id = boot_resp.session_id.or(session_id);
        }
    }

    // Rule 1 & Rule 3 & Rule 5 & M16: Advance cursor with the new session id
    store.set_thread_cursor(
        &thread_root,
        &trigger_msg.event_id,
        now_secs,
        session_id.as_deref(),
    )?;

    let mut ack_reaction_id: Option<String> = None;
    if is_addressed {
        match matrix
            .send_reaction(&event.room_id, &reaction_target_id, "👀")
            .await
        {
            Ok(rid) => {
                ack_reaction_id = Some(rid);
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to send ack reaction for event {}: {}",
                    reaction_target_id,
                    e
                );
            }
        }
    }

    // Record mapping in SQLite: record both message ids against the thread
    store.record_thread_message(&thread_root, &delta_message_id, now_secs)?;
    if let Some(ref r_msg_id) = rebootstrap_message_id {
        store.record_thread_message(&thread_root, r_msg_id, now_secs)?;
    }
    store.record_thread_asker(&thread_root, &event.sender_mxid)?;

    // 9. Long-poll for Expert Reply
    let first_timeout = config.answer_timeout_secs.min(config.answer_deadline_secs);
    let first_res = xmsg.wait_for_reply(&message_id, first_timeout).await;

    let final_res = match first_res {
        Ok(reply) => Ok(reply),
        Err(AppError::Timeout(_)) => {
            if is_addressed {
                let dm_text = format!(
                    "[matrix-xmsg] Answer timeout in room {} thread {} for message {}",
                    event.room_id, thread_root, message_id
                );
                matrix.send_dm(&config.owner_mxid, &dm_text).await?;
            }

            let remaining = config.answer_deadline_secs.saturating_sub(first_timeout);
            if remaining > 0 {
                xmsg.wait_for_reply(&message_id, remaining).await
            } else {
                Err(AppError::Timeout(config.answer_deadline_secs))
            }
        }
        Err(e) => Err(e),
    };

    match final_res {
        Ok(reply_text) => {
            // R2: Atomic claim before posting reply
            if let Some(flag) = claimed {
                if flag
                    .compare_exchange(
                        false,
                        true,
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                    )
                    .is_err()
                {
                    return Ok(BotOutcome::AbortedByRestart);
                }
            }
            match format_expert_reply(&reply_text) {
                None => {
                    // Declined ({"silent": true})
                    if let Some(ref r_id) = ack_reaction_id {
                        if let Err(e) = matrix.redact_event(&event.room_id, r_id, None).await {
                            tracing::warn!("Failed to redact ack reaction {}: {}", r_id, e);
                        }
                    }
                    if is_addressed {
                        if let Err(e) = matrix
                            .send_reaction(&event.room_id, &reaction_target_id, "🫡")
                            .await
                        {
                            tracing::warn!(
                                "Failed to send decline reaction for event {}: {}",
                                reaction_target_id,
                                e
                            );
                        }
                    }
                    Ok(BotOutcome::Declined)
                }
                Some((rendered_reply, is_escalate)) => {
                    if is_escalate {
                        let dm_text = format!(
                            "[matrix-xmsg] Expert requested escalation in room {} thread {} for message {}",
                            event.room_id, thread_root, message_id
                        );
                        matrix.send_dm(&config.owner_mxid, &dm_text).await?;

                        let sent_ev_id = matrix
                            .send_notice(
                                &event.room_id,
                                Some(&thread_root),
                                &rendered_reply,
                                Some(&event.sender_mxid),
                            )
                            .await?;
                        let _ = store.record_bot_message(&sent_ev_id, &thread_root, now_secs);
                        Ok(BotOutcome::RepliedAndEscalated)
                    } else {
                        let sent_ev_id = matrix
                            .send_notice(
                                &event.room_id,
                                Some(&thread_root),
                                &rendered_reply,
                                Some(&event.sender_mxid),
                            )
                            .await?;
                        let _ = store.record_bot_message(&sent_ev_id, &thread_root, now_secs);
                        Ok(BotOutcome::Replied)
                    }
                }
            }
        }
        Err(AppError::Timeout(_)) => {
            if !is_addressed {
                return Ok(BotOutcome::TimedOutSilent);
            }
            // R2: Atomic claim before posting timeout notice
            if let Some(flag) = claimed {
                if flag
                    .compare_exchange(
                        false,
                        true,
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                    )
                    .is_err()
                {
                    return Ok(BotOutcome::AbortedByRestart);
                }
            }

            let notice = "The expert has not answered; a human has been notified.";
            let sent_ev_id = matrix
                .send_notice(
                    &event.room_id,
                    Some(&thread_root),
                    notice,
                    Some(&event.sender_mxid),
                )
                .await?;
            let _ = store.record_bot_message(&sent_ev_id, &thread_root, now_secs);
            Ok(BotOutcome::TimedOutAndEscalated)
        }
        Err(e) => Err(e),
    }
}

pub use crate::matrix::extract_incoming_event;

/// Handles an incoming Matrix reaction event (M5a controls).
/// Reactions ✅ (accept) and 🔍 (deeper) on a message the bot posted.
/// Only the original asker of that thread or a trusted MXID counts; anyone else is ignored silently.
/// Debounces duplicate controls from the same user on the same message.
pub async fn handle_incoming_reaction(
    reaction: &IncomingReactionEvent,
    config: &Config,
    xmsg: &dyn XmsgClient,
    store: &Store,
) -> Result<BotOutcome, AppError> {
    // 0. Self-Sender Gate: Drop silently if sender is the bot itself
    if reaction.sender_mxid == config.bot_mxid {
        return Ok(BotOutcome::IgnoredSelf);
    }

    // 1. Room Allowlist Gate: Drop silently if not allowlisted
    if !config.is_room_allowlisted(&reaction.room_id) {
        return Ok(BotOutcome::IgnoredRoom);
    }

    // 2. Control Recognition Gate: Only ✅ and 🔍 (and 🔎) are controls
    let Some(control) = parse_reaction_control(&reaction.key) else {
        return Ok(BotOutcome::IgnoredNoMention);
    };

    // 3. Message check: Must be a reaction on a message the bot posted
    let Some(thread_id) = store.get_bot_message_thread(&reaction.relates_to_event_id)? else {
        return Ok(BotOutcome::IgnoredNoMention);
    };

    // 4. Sender authorization Gate: Only the original asker of that thread or a trusted MXID counts
    let asker = store.get_thread_asker(&thread_id)?;
    let is_asker = asker.as_deref() == Some(&reaction.sender_mxid);
    let is_trusted = config.is_user_trusted(&reaction.sender_mxid);

    if !is_asker && !is_trusted {
        return Ok(BotOutcome::IgnoredUntrustedUser);
    }

    // 5. Debounce Gate: Duplicates from the same user on the same message => 1 event
    let now_secs = reaction.timestamp_ms / 1000;
    let is_new = store.record_control_if_new(
        &reaction.relates_to_event_id,
        &reaction.sender_mxid,
        control.as_str(),
        now_secs,
    )?;

    if !is_new {
        return Ok(BotOutcome::ControlDebounced);
    }

    // 6. Emit event to expert over existing xmsg path: {thread_id, control: accept|deeper, by}
    let payload = serde_json::json!({
        "thread_id": thread_id,
        "control": control.as_str(),
        "by": reaction.sender_mxid,
    })
    .to_string();

    let mapped_sender = map_sender_mxid(&reaction.sender_mxid);
    xmsg.send_message(&config.expert_ref, &mapped_sender, &payload)
        .await?;

    Ok(BotOutcome::ControlEmitted)
}

/// Extracts an `IncomingReactionEvent` from a Matrix SDK `SyncReactionEvent`.
pub fn extract_incoming_reaction(
    event: &matrix_sdk::ruma::events::reaction::SyncReactionEvent,
    room_id: &matrix_sdk::ruma::RoomId,
) -> Option<IncomingReactionEvent> {
    use matrix_sdk::ruma::events::reaction::SyncReactionEvent;

    if let SyncReactionEvent::Original(orig) = event {
        Some(IncomingReactionEvent {
            room_id: room_id.to_string(),
            event_id: orig.event_id.to_string(),
            sender_mxid: orig.sender.to_string(),
            relates_to_event_id: orig.content.relates_to.event_id.to_string(),
            key: orig.content.relates_to.key.clone(),
            timestamp_ms: u64::from(orig.origin_server_ts.0) as i64,
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
    pub claimed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl InFlightQuestion {
    pub fn new(room_id: String, thread_root: String, sender_mxid: String) -> Self {
        Self {
            room_id,
            thread_root,
            sender_mxid,
            claimed: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
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
        let mut guard = self.tasks.lock().await;
        let handle = tokio::spawn(async move {
            future_fn().await;
            tasks.lock().await.remove(&id);
        });
        guard.insert(id, (question, handle.abort_handle()));
        drop(guard);
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

        let mut notices_to_send = Vec::new();
        for (q, abort_handle) in remaining {
            abort_handle.abort();
            // R2: Atomic claim. Whichever claims first posts; the other skips.
            if q.claimed
                .compare_exchange(
                    false,
                    true,
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                )
                .is_ok()
            {
                notices_to_send.push(q);
            }
        }

        // R4: Send notices concurrently with 3s per-notice timeout, logging warnings on failure with room_id and thread_root, under 5s overall cap.
        let notice_futures = notices_to_send.into_iter().map(|q| async move {
            let notice = "The bot is restarting; please re-ask your question in a moment.";
            match tokio::time::timeout(
                std::time::Duration::from_secs(3),
                matrix.send_notice(
                    &q.room_id,
                    Some(&q.thread_root),
                    notice,
                    Some(&q.sender_mxid),
                ),
            )
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    tracing::warn!(
                        room_id = %q.room_id,
                        thread_root = %q.thread_root,
                        "Failed to send restart notice: {e}"
                    );
                }
                Err(_) => {
                    tracing::warn!(
                        room_id = %q.room_id,
                        thread_root = %q.thread_root,
                        "Timed out sending restart notice after 3s"
                    );
                }
            }
        });

        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            futures_util::future::join_all(notice_futures),
        )
        .await;
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

    let config_msg = config.clone();
    let xmsg_msg = xmsg.clone();
    let store_msg = store.clone();

    client.add_event_handler(
        move |event: matrix_sdk::ruma::events::room::message::SyncRoomMessageEvent,
              room: matrix_sdk::room::Room| {
            let config = config_msg.clone();
            let matrix = matrix.clone();
            let xmsg = xmsg_msg.clone();
            let store = store_msg.clone();
            let sem = semaphore.clone();
            let tracker = tracker.clone();

            async move {
                if let Some(incoming) = extract_incoming_event(&event, room.room_id()) {
                    // R5: Backlog suppression: primary guard is the persisted sync token.
                    // On initial token-less sync, no skew tolerance is granted: events before startup_ts are backlog.
                    // On resumed sync with token, a 10s clock skew tolerance is allowed.
                    let has_persisted_token = store.get_sync_token().ok().flatten().is_some();
                    let cutoff_ts = if has_persisted_token {
                        startup_ts - 10_000
                    } else {
                        startup_ts
                    };

                    if incoming.timestamp_ms < cutoff_ts {
                        tracing::debug!(
                            "Ignoring historical backlog event {} (ts {} < cutoff {})",
                            incoming.event_id,
                            incoming.timestamp_ms,
                            cutoff_ts
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

                    // N3 Gate 2: Mention / Command / Reply check (synchronous)
                    let is_thread_escalate =
                        incoming.thread_root_id.is_some() && incoming.body.trim() == "!escalate";
                    let is_thread_deeper =
                        incoming.thread_root_id.is_some() && incoming.body.trim() == "!deeper";
                    let is_mentioned = is_bot_mentioned(
                        &incoming.body,
                        incoming.formatted_body.as_deref(),
                        incoming.mentions.as_deref(),
                        &config.bot_mxid,
                    );
                    let is_reply_to_bot =
                        if let Some(ref in_reply_to) = incoming.in_reply_to_event_id {
                            if !incoming.is_falling_back {
                                store
                                    .get_bot_message_thread(in_reply_to)
                                    .ok()
                                    .flatten()
                                    .is_some()
                            } else {
                                false
                            }
                        } else {
                            false
                        };
                    let is_engaged_thread = if let Some(ref root) = incoming.thread_root_id {
                        store.is_thread_engaged(root).unwrap_or(false)
                    } else {
                        false
                    };
                    if !is_thread_escalate
                        && !is_thread_deeper
                        && !is_mentioned
                        && !is_reply_to_bot
                        && !is_engaged_thread
                    {
                        return;
                    }

                    // N3 Gate 3: Admission check (synchronous)
                    // For !deeper or edits, full authorization is checked in handle_incoming_event
                    let is_top_level = incoming.thread_root_id.is_none();
                    let is_trusted = config.is_user_trusted(&incoming.sender_mxid);
                    let is_admitted = match config.admission {
                        Admission::Trusted => is_trusted,
                        Admission::Public => {
                            is_trusted
                                || is_top_level
                                || incoming.replaces_event_id.is_some()
                                || incoming
                                    .thread_root_id
                                    .as_deref()
                                    .and_then(|r| store.get_thread_asker(r).ok().flatten())
                                    .as_deref()
                                    == Some(&incoming.sender_mxid)
                        }
                    };
                    if !is_thread_deeper && !is_admitted {
                        return;
                    }

                    // All synchronous gates passed! Track in-flight task and spawn pipeline.
                    let thread_root = incoming
                        .thread_root_id
                        .clone()
                        .or_else(|| incoming.replaces_event_id.clone())
                        .unwrap_or_else(|| incoming.event_id.clone());

                    let question = InFlightQuestion::new(
                        incoming.room_id.clone(),
                        thread_root,
                        incoming.sender_mxid.clone(),
                    );
                    let claimed = question.claimed.clone();

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

                            if let Err(e) = handle_incoming_event_with_claim(
                                &incoming,
                                &history,
                                &config,
                                matrix.as_ref(),
                                xmsg.as_ref(),
                                &store,
                                Some(&claimed),
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

    let config_react = config.clone();
    let xmsg_react = xmsg.clone();
    let store_react = store.clone();
    client.add_event_handler(
        move |event: matrix_sdk::ruma::events::reaction::SyncReactionEvent,
              room: matrix_sdk::room::Room| {
            let config = config_react.clone();
            let xmsg = xmsg_react.clone();
            let store = store_react.clone();
            async move {
                if let Some(reaction) = extract_incoming_reaction(&event, room.room_id()) {
                    let has_persisted_token = store.get_sync_token().ok().flatten().is_some();
                    let cutoff_ts = if has_persisted_token {
                        startup_ts - 10_000
                    } else {
                        startup_ts
                    };
                    if reaction.timestamp_ms < cutoff_ts {
                        return;
                    }
                    if let Err(e) =
                        handle_incoming_reaction(&reaction, &config, xmsg.as_ref(), store.as_ref())
                            .await
                    {
                        tracing::error!(
                            "Error handling incoming reaction {}: {e}",
                            reaction.event_id
                        );
                    }
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
