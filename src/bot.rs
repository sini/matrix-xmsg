use crate::config::{Admission, Config};
use crate::context::{
    build_envelope, build_envelope_with_rewrite, compute_sender_tier, ContextMode, EventMessage,
};
use crate::error::AppError;
use crate::matrix::{is_bot_mentioned, MatrixClient};
use crate::sender_map::map_sender_mxid;
use crate::store::{ForwardedMessageRecord, Store};
use crate::xmsg::{
    parse_in_reply_to_id, GuardClient, GuardContent, GuardEnvelope, GuardLine, SvcDelivery,
    SvcInbox, XmsgClient,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BotOutcome {
    IgnoredRoom,
    IgnoredNoMention,
    IgnoredUntrustedUser,
    IgnoredSelf,
    SizeCapRefusal,
    RateLimitRefusal,
    EscalatedUserRequest,
    Forwarded,
    Replied,
    RepliedAndEscalated,
    TimedOutAndEscalated,
    TimedOutSilent,
    Declined,
    AbortedByRestart,
    ControlEmitted,
    ControlDebounced,
    IgnoredEdit,
    IgnoredUnknownMessage,
    Resynced,
    ResyncThrottled,
    ResyncUnknown,
    OpenThreadSuccess,
    OpenThreadRefusal,
    GuardRejected,
    GuardFailed,
    Unbound,
    IgnoredReleaseNotOwner,
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
    let guard = crate::xmsg::create_guard_client(config);
    handle_incoming_event_with_guard(
        event,
        room_history,
        config,
        matrix,
        xmsg,
        guard.as_ref(),
        store,
        None,
    )
    .await
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
    let guard = crate::xmsg::create_guard_client(config);
    handle_incoming_event_with_guard(
        event,
        room_history,
        config,
        matrix,
        xmsg,
        guard.as_ref(),
        store,
        claimed,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_incoming_event_with_guard(
    event: &IncomingMatrixEvent,
    room_history: &[EventMessage],
    config: &Config,
    matrix: &dyn MatrixClient,
    xmsg: &dyn XmsgClient,
    guard: &dyn GuardClient,
    store: &Store,
    _claimed: Option<&std::sync::atomic::AtomicBool>,
) -> Result<BotOutcome, AppError> {
    // 0. Self-Sender Gate: Drop silently if sender is the bot itself (F8)
    if event.sender_mxid == config.bot_mxid {
        return Ok(BotOutcome::IgnoredSelf);
    }

    // 1. Room Allowlist Gate: Drop silently if not allowlisted
    if !config.is_room_allowlisted(&event.room_id) {
        return Ok(BotOutcome::IgnoredRoom);
    }

    // Check for !release command on solicited thread opening post
    if event.body.trim() == "!release" {
        let target_root = event
            .thread_root_id
            .as_deref()
            .or(event.in_reply_to_event_id.as_deref());
        if let Some(root_id) = target_root {
            if let Some(solicited) = store.get_solicited_thread(root_id)? {
                let is_reply_to_opening = event.in_reply_to_event_id.as_deref()
                    == Some(&solicited.root_event_id)
                    || event.thread_root_id.as_deref() == Some(&solicited.root_event_id);
                if is_reply_to_opening {
                    if event.sender_mxid == config.owner_mxid {
                        store.unbind_solicited_thread(&solicited.root_event_id)?;
                        return Ok(BotOutcome::Unbound);
                    } else {
                        return Ok(BotOutcome::IgnoredReleaseNotOwner);
                    }
                }
            }
        }
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
            let is_solicited_thread = if let Some(ref root) = event.thread_root_id {
                store.get_solicited_thread(root)?.is_some()
            } else {
                false
            };
            let is_admitted = match config.admission {
                Admission::Trusted => is_trusted,
                Admission::Public => is_trusted || is_top_level || is_asker || is_solicited_thread,
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

    // Check if this thread is a bound solicited thread (Unit M15)
    let solicited_opt = store.get_solicited_thread(&thread_root)?;
    if let Some(solicited) = solicited_opt {
        // 1. Build Guard Envelope
        let all_thread_msgs: Vec<&EventMessage> = room_history
            .iter()
            .filter(|m| m.event_id != trigger_msg.event_id)
            .filter(|m| {
                m.event_id == thread_root || m.thread_root_id.as_deref() == Some(&thread_root)
            })
            .collect();

        let history: Vec<GuardLine> = all_thread_msgs
            .iter()
            .map(|m| GuardLine {
                sender: m.sender_mxid.clone(),
                tier: compute_sender_tier(
                    &m.sender_mxid,
                    &config.trusted_mxids,
                    Some(&config.owner_mxid),
                )
                .to_string(),
                text: m.body.clone(),
            })
            .collect();

        let trigger_tier = compute_sender_tier(
            &trigger_msg.sender_mxid,
            &config.trusted_mxids,
            Some(&config.owner_mxid),
        );
        let content = GuardContent {
            sender: Some(trigger_msg.sender_mxid.clone()),
            tier: Some(trigger_tier.to_string()),
            text: trigger_msg.body.clone(),
        };

        let guard_env = GuardEnvelope {
            source: "message".to_string(),
            history,
            content,
        };

        // 2. Call Guard
        let guard_res = guard.check_guard(&config.guard_ref, &guard_env).await;

        let verdict = match guard_res {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    "Guard-in failure for line {} in thread {}: {}",
                    trigger_msg.event_id,
                    thread_root,
                    e
                );
                let attempts = store.record_guard_retry(
                    &trigger_msg.event_id,
                    &event.room_id,
                    &thread_root,
                    now_secs,
                )?;
                if attempts >= config.guard_retry_budget
                    && !store.has_guard_failure_dm_sent(&trigger_msg.event_id)?
                {
                    let permalink =
                        format!("https://matrix.to/#/{}/{}", event.room_id, thread_root);
                    let dm_text = format!(
                        "[matrix-xmsg] Guard-in failed for line in thread {}",
                        permalink
                    );
                    let _ = matrix.send_dm(&config.owner_mxid, &dm_text).await;
                    let _ = store.set_guard_failure_dm_sent(&trigger_msg.event_id);
                }
                return Ok(BotOutcome::GuardFailed);
            }
        };

        let _ = store.clear_guard_retries(&trigger_msg.event_id);

        if verdict.verdict == "reject" {
            tracing::info!(
                "Guard rejected line in thread {}: {}",
                thread_root,
                verdict.reason
            );
            return Ok(BotOutcome::GuardRejected);
        }

        // Build relay envelope: allow uses original text; rewrite uses cleaned_text marked rewritten
        let (relay_trigger, is_rewritten) = if verdict.verdict == "rewrite" {
            let mut rew = trigger_msg.clone();
            rew.body = verdict.cleaned_text;
            (rew, true)
        } else {
            (trigger_msg.clone(), false)
        };

        let relay_envelope = build_envelope_with_rewrite(
            &event.room_id,
            &thread_root,
            &relay_trigger,
            room_history,
            &config.trusted_mxids,
            Some(&config.owner_mxid),
            config.history_n,
            config.history_byte_cap,
            is_addressed,
            mode.clone(),
            is_rewritten,
        );

        // Record relay claim in SQLite
        store.record_event_relayed(&claim_id, now_secs)?;

        // Relay as reply to request_message_id
        let send_res = xmsg
            .reply_message(&solicited.request_message_id, &relay_envelope)
            .await;

        let (relayed_message_id, session_id) = match send_res {
            Ok(resp) => (resp.message_id, resp.session_id),
            Err(e) => {
                tracing::warn!(
                    "Push failure to session in room {} thread {}: {}",
                    event.room_id,
                    thread_root,
                    e
                );
                if !store.has_solicited_thread_push_failure_dm_sent(&thread_root)? {
                    let permalink =
                        format!("https://matrix.to/#/{}/{}", event.room_id, thread_root);
                    let dm_text = format!(
                        "[matrix-xmsg] Push failure to session in room {} thread {}: {}",
                        event.room_id, thread_root, permalink
                    );
                    let _ = matrix.send_dm(&config.owner_mxid, &dm_text).await;
                    let _ = store.set_solicited_thread_push_failure_dm_sent(&thread_root);
                }
                (format!("dead-session-{}", trigger_msg.event_id), None)
            }
        };

        // Advance cursor
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
                Ok(rid) => ack_reaction_id = Some(rid),
                Err(e) => tracing::warn!(
                    "Failed to send ack reaction for event {}: {}",
                    reaction_target_id,
                    e
                ),
            }
        }

        store.record_thread_message(&thread_root, &relayed_message_id, now_secs)?;
        let forward_rec = ForwardedMessageRecord {
            message_id: relayed_message_id.clone(),
            room_id: event.room_id.clone(),
            thread_root_id: thread_root.clone(),
            event_id: reaction_target_id.clone(),
            sender_mxid: event.sender_mxid.clone(),
            addressed: is_addressed,
            sent_at: now_secs,
            ack_reaction_id,
            timeout_dm_sent: false,
            reply_count: 0,
        };
        store.record_forwarded_message(&forward_rec)?;

        return Ok(BotOutcome::Forwarded);
    }

    // Record relay claim in SQLite
    store.record_event_relayed(&claim_id, now_secs)?;

    // 8. Send to xmsg Expert Inbox
    let mapped_sender = map_sender_mxid(&event.sender_mxid);
    let send_resp = xmsg
        .send_message(&config.expert_ref, &mapped_sender, &envelope)
        .await?;
    let delta_message_id = send_resp.message_id;
    let mut session_id = send_resp.session_id;
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

            rebootstrap_message_id = Some(boot_resp.message_id);
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

    // Record forwarded message for inbox reply routing and timeout tracking:
    // Both delta and rebootstrap message IDs are recorded so a reply to EITHER is attributed to the thread.
    let delta_forward_rec = ForwardedMessageRecord {
        message_id: delta_message_id.clone(),
        room_id: event.room_id.clone(),
        thread_root_id: thread_root.clone(),
        event_id: reaction_target_id.clone(),
        sender_mxid: event.sender_mxid.clone(),
        addressed: is_addressed,
        sent_at: now_secs,
        ack_reaction_id: ack_reaction_id.clone(),
        timeout_dm_sent: false,
        reply_count: 0,
    };
    store.record_forwarded_message(&delta_forward_rec)?;

    if let Some(ref r_msg_id) = rebootstrap_message_id {
        let boot_forward_rec = ForwardedMessageRecord {
            message_id: r_msg_id.clone(),
            room_id: event.room_id.clone(),
            thread_root_id: thread_root.clone(),
            event_id: reaction_target_id.clone(),
            sender_mxid: event.sender_mxid.clone(),
            addressed: is_addressed,
            sent_at: now_secs,
            ack_reaction_id,
            timeout_dm_sent: false,
            reply_count: 0,
        };
        store.record_forwarded_message(&boot_forward_rec)?;
    }

    Ok(BotOutcome::Forwarded)
}

/// Handles a delivery from the xmsg service inbox.
///
/// Looks up the forwarded message, renders the expert reply, posts to Matrix,
/// records the bot message, and acknowledges the delivery on the service inbox.
/// A delivery is acked ONLY after Matrix posting succeeds, so a failure/crash
/// re-delivers rather than losing the reply.
pub async fn handle_inbox_delivery<I: SvcInbox + ?Sized>(
    delivery: &SvcDelivery,
    config: &Config,
    matrix: &dyn MatrixClient,
    store: &Store,
    inbox: &mut I,
    now_secs: i64,
) -> Result<BotOutcome, AppError> {
    let default_xmsg = crate::xmsg::create_xmsg_client(config);
    handle_inbox_delivery_with_xmsg(
        delivery,
        config,
        matrix,
        store,
        default_xmsg.as_ref(),
        inbox,
        now_secs,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn handle_open_thread<I: SvcInbox + ?Sized>(
    delivery: &SvcDelivery,
    open_thread_val: &serde_json::Value,
    config: &Config,
    matrix: &dyn MatrixClient,
    store: &Store,
    xmsg: &dyn XmsgClient,
    inbox: &mut I,
    now_secs: i64,
) -> Result<BotOutcome, AppError> {
    // 1. Origin check: must be kind == "local"
    let is_local = delivery
        .origin
        .as_ref()
        .and_then(|o| o.get("kind"))
        .and_then(|k| k.as_str())
        == Some("local");

    if !is_local {
        let reply_json = serde_json::json!({
            "error": "unauthorized origin: open_thread requires kind: 'local'"
        })
        .to_string();
        let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
        inbox.ack(&delivery.message_id).await?;
        return Ok(BotOutcome::OpenThreadRefusal);
    }

    // 2. Room extraction and allowlist check
    let room = match open_thread_val.get("room").and_then(|r| r.as_str()) {
        Some(r) => r.to_string(),
        None => {
            let reply_json = serde_json::json!({
                "error": "missing 'room' in open_thread request"
            })
            .to_string();
            let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::OpenThreadRefusal);
        }
    };

    if !config.is_room_allowlisted(&room) {
        let reply_json = serde_json::json!({
            "error": format!("room '{room}' is not in allowlisted rooms")
        })
        .to_string();
        let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
        inbox.ack(&delivery.message_id).await?;
        return Ok(BotOutcome::OpenThreadRefusal);
    }

    // 3. Text extraction and capping
    let text = match open_thread_val.get("text").and_then(|t| t.as_str()) {
        Some(t) => t,
        None => {
            let reply_json = serde_json::json!({
                "error": "missing 'text' in open_thread request"
            })
            .to_string();
            let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::OpenThreadRefusal);
        }
    };

    let capped_text = if text.len() > config.size_cap_bytes {
        let mut end = config.size_cap_bytes;
        while end > 0 && !text.is_char_boundary(end) {
            end -= 1;
        }
        &text[..end]
    } else {
        text
    };

    let localpart = config
        .owner_mxid
        .strip_prefix('@')
        .unwrap_or(&config.owner_mxid)
        .split(':')
        .next()
        .unwrap_or(&config.owner_mxid);

    let opening_body = format!("On behalf of @{localpart}: {capped_text}");

    // 4. Post top-level message to Matrix room
    let root_event_id = matrix.send_notice(&room, None, &opening_body, None).await?;

    // 5. Record bot message and bind solicited thread
    store.record_bot_message(&root_event_id, &root_event_id, now_secs)?;
    store.bind_solicited_thread(&root_event_id, &room, &delivery.message_id, now_secs)?;

    // Record forward record for opening message so replies to request_message_id also route to thread
    let forward_rec = ForwardedMessageRecord {
        message_id: delivery.message_id.clone(),
        room_id: room.clone(),
        thread_root_id: root_event_id.clone(),
        event_id: root_event_id.clone(),
        sender_mxid: config.owner_mxid.clone(),
        addressed: true,
        sent_at: now_secs,
        ack_reaction_id: None,
        timeout_dm_sent: false,
        reply_count: 0,
    };
    store.record_forwarded_message(&forward_rec)?;

    // 6. Reply with root and permalink
    let permalink = format!("https://matrix.to/#/{room}/{root_event_id}");
    let reply_json = serde_json::json!({
        "root": root_event_id,
        "permalink": permalink,
    })
    .to_string();
    xmsg.reply_message(&delivery.message_id, &reply_json)
        .await?;

    inbox.ack(&delivery.message_id).await?;
    Ok(BotOutcome::OpenThreadSuccess)
}

pub async fn handle_inbox_delivery_with_xmsg<I: SvcInbox + ?Sized>(
    delivery: &SvcDelivery,
    config: &Config,
    matrix: &dyn MatrixClient,
    store: &Store,
    xmsg: &dyn XmsgClient,
    inbox: &mut I,
    now_secs: i64,
) -> Result<BotOutcome, AppError> {
    if let Ok(val) = serde_json::from_str::<serde_json::Value>(delivery.text.trim()) {
        if let Some(open_thread_val) = val.get("open_thread") {
            return handle_open_thread(
                delivery,
                open_thread_val,
                config,
                matrix,
                store,
                xmsg,
                inbox,
                now_secs,
            )
            .await;
        }
    }

    let is_resync = if let Ok(val) = serde_json::from_str::<serde_json::Value>(delivery.text.trim())
    {
        val.get("resync").and_then(|v| v.as_bool()).unwrap_or(false)
    } else {
        false
    };

    if is_resync {
        let orig_id = match parse_in_reply_to_id(&delivery.envelope)
            .or_else(|| parse_in_reply_to_id(&delivery.text))
        {
            Some(id) => id.to_string(),
            None => {
                let _ = xmsg
                    .reply_message(&delivery.message_id, r#"{"resync": "unknown"}"#)
                    .await;
                inbox.ack(&delivery.message_id).await?;
                return Ok(BotOutcome::ResyncUnknown);
            }
        };

        let forward = match store.get_forwarded_message(&orig_id)? {
            Some(f) => f,
            None => {
                let _ = xmsg
                    .reply_message(&delivery.message_id, r#"{"resync": "unknown"}"#)
                    .await;
                inbox.ack(&delivery.message_id).await?;
                return Ok(BotOutcome::ResyncUnknown);
            }
        };

        // Rate: at most one resync per thread per 60 s
        if let Err(retry_after) =
            store.check_and_record_thread_resync(&forward.thread_root_id, now_secs, 60)?
        {
            let reply_json = serde_json::json!({
                "resync": "throttled",
                "retry_after": retry_after
            })
            .to_string();
            let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::ResyncThrottled);
        }

        // Paged thread history fetch from root
        let thread_history = matrix
            .fetch_thread_history(&forward.room_id, &forward.thread_root_id)
            .await?;

        // Background room history
        let room_history = matrix
            .fetch_history(&forward.room_id, config.history_n)
            .await?;
        let root_idx_opt = room_history
            .iter()
            .position(|m| m.event_id == forward.thread_root_id);
        let background_msgs: Vec<EventMessage> = match root_idx_opt {
            Some(idx) => room_history[..idx]
                .iter()
                .filter(|m| m.thread_root_id.is_none())
                .cloned()
                .collect(),
            None => Vec::new(),
        };

        let transcript = crate::context::format_resync_transcript(
            &forward.room_id,
            &forward.thread_root_id,
            &background_msgs,
            &thread_history,
            &config.trusted_mxids,
            Some(&config.owner_mxid),
            config.resync_byte_cap,
        );

        let send_resp = xmsg
            .reply_message(&delivery.message_id, &transcript)
            .await?;

        // The thread's cursor (M16) then points at the newest line sent, with the asker's session id.
        let newest_line_id = thread_history
            .last()
            .map(|m| m.event_id.as_str())
            .unwrap_or(&forward.thread_root_id);

        let asker_session_id = send_resp
            .session_id
            .as_deref()
            .or((!delivery.from_name.is_empty()).then_some(delivery.from_name.as_str()));

        store.set_thread_cursor(
            &forward.thread_root_id,
            newest_line_id,
            now_secs,
            asker_session_id,
        )?;

        inbox.ack(&delivery.message_id).await?;
        return Ok(BotOutcome::Resynced);
    }

    let orig_id = match parse_in_reply_to_id(&delivery.envelope)
        .or_else(|| parse_in_reply_to_id(&delivery.text))
    {
        Some(id) => id.to_string(),
        None => {
            tracing::warn!(
                "Delivery {} has no in-reply-to message id, acking and ignoring",
                delivery.message_id
            );
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::IgnoredUnknownMessage);
        }
    };

    let forward = match store.get_forwarded_message(&orig_id)? {
        Some(f) => f,
        None => {
            tracing::warn!(
                "Delivery {} refers to unknown forward message_id {}, acking and ignoring",
                delivery.message_id,
                orig_id
            );
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::IgnoredUnknownMessage);
        }
    };

    match format_expert_reply(&delivery.text) {
        None => {
            // Declined ({"silent": true})
            if forward.reply_count == 0 {
                if let Some(ref r_id) = forward.ack_reaction_id {
                    if let Err(e) = matrix.redact_event(&forward.room_id, r_id, None).await {
                        tracing::warn!("Failed to redact ack reaction {}: {}", r_id, e);
                    }
                }
                if forward.addressed {
                    if let Err(e) = matrix
                        .send_reaction(&forward.room_id, &forward.event_id, "🫡")
                        .await
                    {
                        tracing::warn!(
                            "Failed to send decline reaction for event {}: {}",
                            forward.event_id,
                            e
                        );
                    }
                }
            }
            store.increment_reply_count(&orig_id)?;
            inbox.ack(&delivery.message_id).await?;
            Ok(BotOutcome::Declined)
        }
        Some((rendered_reply, is_escalate)) => {
            if is_escalate {
                let dm_text = format!(
                    "[matrix-xmsg] Expert requested escalation in room {} thread {} for message {}",
                    forward.room_id, forward.thread_root_id, orig_id
                );
                matrix.send_dm(&config.owner_mxid, &dm_text).await?;

                let sent_ev_id = matrix
                    .send_notice(
                        &forward.room_id,
                        Some(&forward.thread_root_id),
                        &rendered_reply,
                        Some(&forward.sender_mxid),
                    )
                    .await?;
                let _ = store.record_bot_message(&sent_ev_id, &forward.thread_root_id, now_secs);
                store.increment_reply_count(&orig_id)?;
                inbox.ack(&delivery.message_id).await?;
                Ok(BotOutcome::RepliedAndEscalated)
            } else {
                let sent_ev_id = matrix
                    .send_notice(
                        &forward.room_id,
                        Some(&forward.thread_root_id),
                        &rendered_reply,
                        Some(&forward.sender_mxid),
                    )
                    .await?;
                let _ = store.record_bot_message(&sent_ev_id, &forward.thread_root_id, now_secs);
                store.increment_reply_count(&orig_id)?;
                inbox.ack(&delivery.message_id).await?;
                Ok(BotOutcome::Replied)
            }
        }
    }
}

/// Sweeps unanswered addressed forwarded messages and sends an owner DM for any
/// that exceeded answer_timeout_secs without a reply.
/// Sets timeout_dm_sent in the store so restarts do not duplicate the notification.
pub async fn sweep_answer_timeouts(
    config: &Config,
    matrix: &dyn MatrixClient,
    store: &Store,
    now_secs: i64,
) -> Result<usize, AppError> {
    let timed_out =
        store.get_unanswered_timed_out_messages(config.answer_timeout_secs, now_secs)?;
    let count = timed_out.len();
    for msg in timed_out {
        let dm_text = format!(
            "[matrix-xmsg] Answer timeout in room {} thread {} for message {}",
            msg.room_id, msg.thread_root_id, msg.message_id
        );
        match matrix.send_dm(&config.owner_mxid, &dm_text).await {
            Ok(_) => {
                store.mark_timeout_dm_sent(&msg.message_id)?;
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to send answer timeout DM to {} for message {}: {}",
                    config.owner_mxid,
                    msg.message_id,
                    e
                );
            }
        }
    }
    Ok(count)
}

/// Runs the service inbox poll loop until shutdown signal is received.
pub async fn run_inbox_loop<I: SvcInbox>(
    mut inbox: I,
    config: std::sync::Arc<Config>,
    matrix: std::sync::Arc<dyn MatrixClient>,
    store: std::sync::Arc<Store>,
    xmsg: std::sync::Arc<dyn XmsgClient>,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), AppError> {
    loop {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        if let Err(e) =
            sweep_answer_timeouts(&config, matrix.as_ref(), store.as_ref(), now_secs).await
        {
            tracing::warn!("Failed sweeping answer timeouts: {e}");
        }

        tokio::select! {
            _ = shutdown_rx.recv() => {
                tracing::info!("Inbox loop received shutdown signal");
                break;
            }
            poll_res = inbox.poll(5) => {
                match poll_res {
                    Ok(Some(delivery)) => {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs() as i64;
                        if let Err(e) = handle_inbox_delivery_with_xmsg(
                            &delivery,
                            &config,
                            matrix.as_ref(),
                            store.as_ref(),
                            xmsg.as_ref(),
                            &mut inbox,
                            now,
                        ).await {
                            tracing::error!("Error handling inbox delivery {}: {e}", delivery.message_id);
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!("Error polling svc inbox: {e}");
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                }
            }
        }
    }
    Ok(())
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
