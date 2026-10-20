use crate::config::{route_for, Admission, Config, GuardFailureMode};
use crate::context::{
    build_structural_envelope_with_screening, compute_sender_tier,
    format_context_line_with_screening, format_paged_context_lines, format_paged_resync_lines,
    select_structural_context_messages, slice_messages_by_byte_offset, ContextMode, EventMessage,
};
use crate::error::AppError;
use crate::matrix::{is_bot_mentioned, MatrixClient};
use crate::sender_map::map_sender_mxid;
use crate::store::{BotReplyEventRecord, ForwardedMessageRecord, Store};
use crate::xmsg::{
    parse_in_reply_to_id, route_guard_reply, GuardClient, GuardContent, GuardEnvelope, GuardLine,
    SvcDelivery, SvcInbox, XmsgClient,
};

/// The bound on each attempt of a send made under `Store::forward_gate`, which holds the inbox.
/// A send that times out is retried once with the same idempotency key, so a send xmsg accepted
/// after the bound answers the retry with its own message ID and is recorded; its reply is
/// dropped only when the retry fails too. An expert forward makes two sends (the delta and its
/// rebootstrap), so it holds the inbox up to four times this.
const FORWARD_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Runs `send` bounded by `FORWARD_SEND_TIMEOUT` and, when that times out, once more. `send`
/// must carry one idempotency key on both attempts, so the two deliver at most one message.
async fn bounded_send<T, F>(what: &str, send: impl Fn() -> F) -> Result<T, AppError>
where
    F: std::future::Future<Output = Result<T, AppError>>,
{
    if let Ok(res) = tokio::time::timeout(FORWARD_SEND_TIMEOUT, send()).await {
        return res;
    }
    tracing::warn!("{what} timed out after {FORWARD_SEND_TIMEOUT:?}; retrying with its key");
    let res = tokio::time::timeout(FORWARD_SEND_TIMEOUT, send())
        .await
        .unwrap_or_else(|_| {
            Err(AppError::Xmsg(format!(
                "{what} timed out twice after {FORWARD_SEND_TIMEOUT:?}"
            )))
        });
    if let Err(e) = &res {
        tracing::warn!("{what} failed on its retry; a reply to it will be dropped: {e}");
    }
    res
}

/// The idempotency key of one logical forward send: its `purpose` (`forward`, `rebootstrap`,
/// `relay`) and the Matrix event it forwards, which is relayed at most once. xmsg takes 1 to
/// 128 characters, so an event ID too long for that is replaced by its hash.
fn forward_key(purpose: &str, event_id: &str) -> String {
    let key = format!("matrix-xmsg:{purpose}:{event_id}");
    if key.len() <= 128 {
        return key;
    }
    use std::hash::{Hash, Hasher};
    let mut h = std::hash::DefaultHasher::new();
    event_id.hash(&mut h);
    format!("matrix-xmsg:{purpose}:#{:016x}", h.finish())
}

fn expert_ref_for_room_tier<'a>(config: &'a Config, room_id: &str, tier: &str) -> &'a str {
    route_for(config, room_id).ref_for_tier(tier)
}

fn should_screen_room(config: &Config, room_id: &str, is_solicited: bool) -> bool {
    let route = route_for(config, room_id);
    if is_solicited {
        route.screen_solicited
    } else {
        route.screen_mentions
    }
}

fn effective_thread_tier(
    config: &Config,
    store: &Store,
    thread_root: &str,
    sender_mxid: &str,
) -> Result<&'static str, AppError> {
    if store.is_thread_promoted(thread_root)? {
        Ok("trusted")
    } else {
        Ok(compute_sender_tier(
            sender_mxid,
            &config.trusted_mxids,
            Some(&config.owner_mxid),
        ))
    }
}

fn control_target_ref(
    config: &Config,
    store: &Store,
    room_id: &str,
    thread_root: &str,
    sender_mxid: &str,
) -> Result<String, AppError> {
    if let Some(target_ref) = store
        .get_thread_cursor(thread_root)?
        .and_then(|c| c.target_ref)
    {
        return Ok(target_ref);
    }
    let tier = effective_thread_tier(config, store, thread_root, sender_mxid)?;
    Ok(expert_ref_for_room_tier(config, room_id, tier).to_string())
}

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
    ContextAnswered,
    ContextThrottled,
    ContextUnknown,
    ContextRefused,
    OpenThreadSuccess,
    OpenThreadRefusal,
    GuardRejected,
    GuardFailed,
    /// A pushed guard verdict, handed to the guard check that waits on it.
    GuardVerdictRouted,
    Unbound,
    IgnoredReleaseNotOwner,
    Redacted,
    RedactRefused,
    Edited,
    EditRefused,
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
    Approve,
}

impl ControlAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Deeper => "deeper",
            Self::Approve => "approve",
        }
    }
}

pub fn parse_reaction_control(key: &str) -> Option<ControlAction> {
    let trimmed = key.trim();
    if trimmed.contains('✅') {
        Some(ControlAction::Accept)
    } else if trimmed.contains('🔍') || trimmed.contains('🔎') {
        Some(ControlAction::Deeper)
    } else if trimmed.contains('🔓') {
        Some(ControlAction::Approve)
    } else {
        None
    }
}

/// Returns true if a delivery payload represents an approval request.
/// A delivery from genie-agent is wrapped in a structured envelope `{"text": <string>, "approval"?: true}`.
/// Reads `approval` only from the envelope's top level, requiring both a top-level `"text"` string
/// and `"approval": true` (or `"approval_request": true`). Model text only ever goes inside `text`,
/// so a model reply literally emitting `{"approval": true}` arrives nested as `{"text": "{\"approval\": true}"}`
/// and cannot mint an approval request (Unit C10).
pub fn is_approval_delivery(delivery_text: &str, _rendered_text: &str) -> bool {
    let trimmed = delivery_text.trim();
    if let Ok(val) = serde_json::from_str::<serde_json::Value>(trimmed) {
        if let Some(obj) = val.as_object() {
            let has_text = obj.contains_key("text");
            let has_approval = obj
                .get("approval")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
                || obj
                    .get("approval_request")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
            if has_text && has_approval {
                return true;
            }
        }
    }
    false
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

    if event.replaces_event_id.is_none() && !event.is_falling_back {
        if let Some(ref reply_to) = event.in_reply_to_event_id {
            let _ = store.record_event_reply_target(&event.room_id, &event.event_id, reply_to);
        }
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

    let (thread_root, claim_id, reaction_target_id, trigger_msg, is_addressed, effective_reply_to) =
        if let Some(ref orig_event_id) = event.replaces_event_id {
            // M10 Rule 3: Original event must never have been relayed
            if store.is_event_relayed(orig_event_id)? {
                return Ok(BotOutcome::IgnoredEdit);
            }

            // Fetch original event and its reply relation from history/store or Matrix API
            let (orig_event, orig_reply_to) = if let Some(msg) =
                room_history.iter().find(|m| m.event_id == *orig_event_id)
            {
                let rt = match store.get_event_reply_target(&event.room_id, orig_event_id)? {
                    Some(t) => Some(t),
                    None => matrix
                        .fetch_event_with_reply_target(&event.room_id, orig_event_id)
                        .await
                        .ok()
                        .flatten()
                        .and_then(|(_, t)| t),
                };
                (Some(msg.clone()), rt)
            } else {
                match matrix
                    .fetch_event_with_reply_target(&event.room_id, orig_event_id)
                    .await
                {
                    Ok(Some((msg, rt))) => {
                        let rt = match rt {
                            Some(t) => Some(t),
                            None => store.get_event_reply_target(&event.room_id, orig_event_id)?,
                        };
                        (Some(msg), rt)
                    }
                    Ok(None) => (None, None),
                    Err(e) => {
                        tracing::debug!("Failed to fetch original event {}: {}", orig_event_id, e);
                        (None, None)
                    }
                }
            };

            // M10 Rule 1: Replacement content must mention the bot or be a reply to the bot
            let is_mentioned = is_bot_mentioned(
                &event.body,
                event.formatted_body.as_deref(),
                event.mentions.as_deref(),
                &config.bot_mxid,
            );
            let reply_check_id = orig_reply_to.as_deref().or_else(|| {
                if !event.is_falling_back {
                    event.in_reply_to_event_id.as_deref()
                } else {
                    None
                }
            });
            let is_reply_to_bot = if let Some(in_reply_to) = reply_check_id {
                store.get_bot_message_thread(in_reply_to)?.is_some()
            } else {
                false
            };
            if !is_mentioned && !is_reply_to_bot {
                return Ok(BotOutcome::IgnoredNoMention);
            }

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
            // M33 §2: Edit-into-mention resolves against the original event's timestamp and relations
            let trigger_msg = EventMessage {
                event_id: orig.event_id.clone(),
                sender_mxid: event.sender_mxid.clone(),
                timestamp_ms: orig.timestamp_ms,
                body: event.body.clone(),
                thread_root_id: orig.thread_root_id.clone(),
            };
            (
                thread_root,
                claim_id,
                reaction_target_id,
                trigger_msg,
                true,
                orig_reply_to,
            )
        } else {
            let is_thread_escalate =
                event.thread_root_id.is_some() && event.body.trim() == "!escalate";
            let is_thread_deeper = event.thread_root_id.is_some() && event.body.trim() == "!deeper";
            let is_thread_trust = event.thread_root_id.is_some() && event.body.trim() == "!trust";
            let is_approve_command = event.body.trim() == "!approve";
            let approval_target = if is_approve_command {
                if let Some(ref in_reply_to) = event.in_reply_to_event_id {
                    if store.is_approval_request(in_reply_to)? {
                        Some(in_reply_to.clone())
                    } else {
                        None
                    }
                } else if let Some(ref root) = event.thread_root_id {
                    if store.is_approval_request(root)? {
                        Some(root.clone())
                    } else {
                        store.get_latest_approval_request_in_thread(root)?
                    }
                } else {
                    None
                }
            } else {
                None
            };
            let is_approval_reply = approval_target.is_some();
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
            let is_addressed = is_mentioned
                || is_reply_to_bot
                || is_thread_escalate
                || is_thread_deeper
                || is_approval_reply;

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
                let target_ref = control_target_ref(
                    config,
                    store,
                    &event.room_id,
                    &thread_root,
                    &event.sender_mxid,
                )?;
                xmsg.send_message(&target_ref, &mapped_sender, &payload)
                    .await?;

                return Ok(BotOutcome::ControlEmitted);
            }

            // Handle !trust control command in thread (Unit M12 / M20 / M28)
            if is_thread_trust {
                let is_trusted = config.is_user_trusted(&event.sender_mxid);
                if !is_trusted {
                    return Ok(BotOutcome::IgnoredUntrustedUser);
                }

                let is_new = store.record_control_if_new(
                    &event.event_id,
                    &event.sender_mxid,
                    "trust",
                    now_secs,
                )?;

                if !is_new {
                    return Ok(BotOutcome::ControlDebounced);
                }

                store.promote_thread(&thread_root, &event.sender_mxid, now_secs)?;

                if let Err(e) = matrix
                    .send_reaction(&event.room_id, &event.event_id, "🔒")
                    .await
                {
                    tracing::warn!(
                        "Failed to send trust reaction for event {}: {}",
                        event.event_id,
                        e
                    );
                }

                let mapped_sender = map_sender_mxid(&event.sender_mxid);
                let payload = serde_json::json!({
                    "thread_id": thread_root,
                    "control": "trust",
                    "sender": mapped_sender,
                    "sender_id": event.sender_mxid,
                    "tier": "trusted",
                    "event_id": event.event_id,
                })
                .to_string();

                let target_ref = expert_ref_for_room_tier(config, &event.room_id, "trusted");
                xmsg.send_message(target_ref, &mapped_sender, &payload)
                    .await?;

                return Ok(BotOutcome::ControlEmitted);
            }

            // Handle !approve control command (Unit M12 approval)
            if is_approve_command {
                let Some(target_id) = approval_target else {
                    // 🔓 or !approve on a message that isn't a bot approval request is ignored
                    return Ok(BotOutcome::IgnoredNoMention);
                };

                let is_trusted = config.is_user_trusted(&event.sender_mxid);
                if !is_trusted {
                    return Ok(BotOutcome::IgnoredUntrustedUser);
                }

                let is_new = store.record_control_if_new(
                    &target_id,
                    &event.sender_mxid,
                    "approve",
                    now_secs,
                )?;

                if !is_new {
                    return Ok(BotOutcome::ControlDebounced);
                }

                if let Err(e) = matrix
                    .send_reaction(&event.room_id, &event.event_id, "🔓")
                    .await
                {
                    tracing::warn!(
                        "Failed to send approve reaction for event {}: {}",
                        event.event_id,
                        e
                    );
                }

                let mapped_sender = map_sender_mxid(&event.sender_mxid);
                let thread_id = event
                    .thread_root_id
                    .as_deref()
                    .map(String::from)
                    .or_else(|| store.get_approval_request_thread(&target_id).ok().flatten())
                    .unwrap_or_else(|| thread_root.clone());

                let payload = serde_json::json!({
                    "thread_id": thread_id,
                    "control": "approve",
                    "sender": mapped_sender,
                    "sender_id": event.sender_mxid,
                    "tier": "trusted",
                    "target": target_id,
                })
                .to_string();

                let target_ref = control_target_ref(
                    config,
                    store,
                    &event.room_id,
                    &thread_id,
                    &event.sender_mxid,
                )?;
                xmsg.send_message(&target_ref, &mapped_sender, &payload)
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
            let is_promoted_thread = if let Some(ref root) = event.thread_root_id {
                store.is_thread_promoted(root)?
            } else {
                false
            };
            let is_admitted = match config.admission {
                Admission::Trusted => is_trusted || is_promoted_thread,
                Admission::Public => {
                    is_trusted
                        || is_top_level
                        || is_asker
                        || is_solicited_thread
                        || is_promoted_thread
                }
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
            let reply_to = if !event.is_falling_back {
                event.in_reply_to_event_id.clone()
            } else {
                None
            };
            (
                thread_root,
                claim_id,
                reaction_target_id,
                trigger_msg,
                is_addressed,
                reply_to,
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

    // 5. Rate Limit Gate (skip re-charging on guard retry attempts of the same event)
    let prior_guard_attempts = store.get_guard_retries(&trigger_msg.event_id)?.unwrap_or(0);
    if prior_guard_attempts == 0
        && store
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

    // 7. Context & Envelope Assembly + Guard Screening (M15, M26, M28, M32, M33)
    let effective_tier =
        effective_thread_tier(config, store, &thread_root, &trigger_msg.sender_mxid)?;
    let target_expert_ref = expert_ref_for_room_tier(config, &event.room_id, effective_tier);

    let cursor_opt = store.get_thread_cursor(&thread_root)?;
    let mode = match cursor_opt {
        None => ContextMode::Bootstrap,
        Some(ref cursor) => {
            let elapsed = now_secs.saturating_sub(cursor.forwarded_at);
            let target_changed = cursor
                .target_ref
                .as_deref()
                .is_some_and(|prev_target| prev_target != target_expert_ref);
            if elapsed >= config.session_live_secs as i64 {
                ContextMode::Bootstrap
            } else if target_changed {
                if let Some(prev_msg_id) =
                    store.get_thread_messages(&thread_root)?.into_iter().last()
                {
                    ContextMode::Rebootstrap {
                        supersedes: prev_msg_id,
                    }
                } else {
                    ContextMode::Bootstrap
                }
            } else {
                ContextMode::Delta {
                    cursor_event_id: Some(cursor.cursor_event_id.clone()),
                }
            }
        }
    };

    let (replied_to_msg, replied_thread_msgs) = resolve_reply_context(
        &event.room_id,
        effective_reply_to.as_deref(),
        room_history,
        matrix,
    )
    .await;

    // Check if this thread is a bound solicited thread (Unit M15)
    let solicited_opt = store.get_solicited_thread(&thread_root)?;
    let should_screen = should_screen_room(config, &event.room_id, solicited_opt.is_some());

    let (relay_trigger, is_rewritten, is_unscreened, fail_open_warning, all_trusted_primary) =
        match screen_forward_context(
            &trigger_msg,
            room_history,
            replied_to_msg.as_ref(),
            &replied_thread_msgs,
            &mode,
            &event.room_id,
            &thread_root,
            should_screen,
            config,
            matrix,
            guard,
            store,
            now_secs,
        )
        .await?
        {
            Ok(res) => res,
            Err(outcome) => return Ok(outcome),
        };

    let envelope = build_structural_envelope_with_screening(
        &event.room_id,
        &thread_root,
        &relay_trigger,
        room_history,
        replied_to_msg.as_ref(),
        &replied_thread_msgs,
        config.sender_window_secs,
        &config.trusted_mxids,
        Some(&config.owner_mxid),
        config.history_n,
        config.history_byte_cap,
        is_addressed,
        mode.clone(),
        is_rewritten,
        is_unscreened,
        fail_open_warning,
    );

    if let Some(solicited) = solicited_opt {
        // Record relay claim in SQLite
        store.record_event_relayed(&claim_id, now_secs)?;

        // Relay as reply to request_message_id. The forward is recorded under the gate.
        let mut gate = Some(store.forward_gate.read().await);
        let key = forward_key("relay", &claim_id);
        let send_res = bounded_send("Relay to the solicited session", || {
            xmsg.reply_message_keyed(&solicited.request_message_id, &envelope, &key)
        })
        .await;

        let (relayed_message_id, session_id) = match send_res {
            Ok(resp) => (resp.message_id, resp.session_id),
            Err(e) => {
                // The synthetic id below answers no send, so no reply waits on its record:
                // release the gate before the unbounded owner DM.
                gate = None;
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

        let relay_session = session_id.clone().or_else(|| {
            store
                .get_forward_session(&solicited.request_message_id)
                .ok()
                .flatten()
        });

        // Advance cursor
        store.set_thread_cursor(
            &thread_root,
            &trigger_msg.event_id,
            now_secs,
            relay_session.as_deref(),
        )?;

        store.record_thread_message(&thread_root, &relayed_message_id, now_secs)?;
        let forward_rec = ForwardedMessageRecord {
            message_id: relayed_message_id.clone(),
            room_id: event.room_id.clone(),
            thread_root_id: thread_root.clone(),
            event_id: reaction_target_id.clone(),
            sender_mxid: event.sender_mxid.clone(),
            addressed: is_addressed,
            sent_at: now_secs,
            ack_reaction_id: None,
            timeout_dm_sent: false,
            reply_count: 0,
        };
        store.record_forwarded_message(&forward_rec)?;
        store.set_forward_session(&relayed_message_id, relay_session.as_deref())?;
        drop(gate);

        if is_addressed {
            ack_forward(
                matrix,
                store,
                &event.room_id,
                &reaction_target_id,
                &[&relayed_message_id],
            )
            .await?;
        }

        return Ok(BotOutcome::Forwarded);
    }

    // Record relay claim in SQLite
    store.record_event_relayed(&claim_id, now_secs)?;

    // 8. Send to xmsg Expert Inbox, recording the forward under `Store::forward_gate`.
    let mapped_sender = map_sender_mxid(&event.sender_mxid);
    let gate = store.forward_gate.read().await;
    let key = forward_key("forward", &claim_id);
    let send_resp = bounded_send("Forward to the expert", || {
        xmsg.send_message_keyed(target_expert_ref, &mapped_sender, &envelope, &key)
    })
    .await?;
    let delta_message_id = send_resp.message_id;
    let delta_session_id = send_resp.session_id.clone();
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
            let rebootstrap_mode = ContextMode::Rebootstrap {
                supersedes: delta_message_id.clone(),
            };
            let boot_screened = if all_trusted_primary {
                screen_forward_context(
                    &trigger_msg,
                    room_history,
                    replied_to_msg.as_ref(),
                    &replied_thread_msgs,
                    &rebootstrap_mode,
                    &event.room_id,
                    &thread_root,
                    should_screen,
                    config,
                    matrix,
                    guard,
                    store,
                    now_secs,
                )
                .await?
                .ok()
            } else {
                Some((
                    relay_trigger.clone(),
                    is_rewritten,
                    is_unscreened,
                    fail_open_warning,
                    all_trusted_primary,
                ))
            };

            if let Some((boot_trigger, boot_rewritten, boot_unscreened, boot_fail_open, _)) =
                boot_screened
            {
                let bootstrap_envelope = build_structural_envelope_with_screening(
                    &event.room_id,
                    &thread_root,
                    &boot_trigger,
                    room_history,
                    replied_to_msg.as_ref(),
                    &replied_thread_msgs,
                    config.sender_window_secs,
                    &config.trusted_mxids,
                    Some(&config.owner_mxid),
                    config.history_n,
                    config.history_byte_cap,
                    is_addressed,
                    rebootstrap_mode,
                    boot_rewritten,
                    boot_unscreened,
                    boot_fail_open,
                );

                let key = forward_key("rebootstrap", &claim_id);
                let boot_resp = bounded_send("Rebootstrap to the expert", || {
                    xmsg.send_message_keyed(
                        target_expert_ref,
                        &mapped_sender,
                        &bootstrap_envelope,
                        &key,
                    )
                })
                .await?;

                rebootstrap_message_id = Some(boot_resp.message_id);
                session_id = boot_resp.session_id.or(session_id);
            }
        }
    }

    // Rule 1 & Rule 3 & Rule 5 & M16 & M28: Advance cursor with the new session id and target ref
    store.set_thread_cursor_with_target(
        &thread_root,
        &trigger_msg.event_id,
        now_secs,
        session_id.as_deref(),
        Some(target_expert_ref),
    )?;

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
        ack_reaction_id: None,
        timeout_dm_sent: false,
        reply_count: 0,
    };
    store.record_forwarded_message(&delta_forward_rec)?;
    store.set_forward_session(&delta_message_id, delta_session_id.as_deref())?;

    if let Some(ref r_msg_id) = rebootstrap_message_id {
        let boot_forward_rec = ForwardedMessageRecord {
            message_id: r_msg_id.clone(),
            room_id: event.room_id.clone(),
            thread_root_id: thread_root.clone(),
            event_id: reaction_target_id.clone(),
            sender_mxid: event.sender_mxid.clone(),
            addressed: is_addressed,
            sent_at: now_secs,
            ack_reaction_id: None,
            timeout_dm_sent: false,
            reply_count: 0,
        };
        store.record_forwarded_message(&boot_forward_rec)?;
        store.set_forward_session(r_msg_id, session_id.as_deref())?;
    }
    drop(gate);

    // The ack reaction is cosmetic: it follows the record, so it never delays a reply's routing.
    if is_addressed {
        let mut forwards = vec![delta_message_id.as_str()];
        forwards.extend(rebootstrap_message_id.as_deref());
        ack_forward(
            matrix,
            store,
            &event.room_id,
            &reaction_target_id,
            &forwards,
        )
        .await?;
    }

    Ok(BotOutcome::Forwarded)
}

/// Sends the 👀 ack reaction to `target` and records it on each of `forwards`, so a decline
/// can redact it. A reply routed before this lands finds no reaction to redact.
async fn ack_forward(
    matrix: &dyn MatrixClient,
    store: &Store,
    room_id: &str,
    target: &str,
    forwards: &[&str],
) -> Result<(), AppError> {
    match matrix.send_reaction(room_id, target, "👀").await {
        Ok(rid) => {
            for message_id in forwards {
                store.set_forward_ack_reaction(message_id, &rid)?;
            }
        }
        Err(e) => tracing::warn!("Failed to send ack reaction for event {target}: {e}"),
    }
    Ok(())
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
    // 1. Origin check: when `open_thread_nodes` is configured, the sender's node must be in that
    //    list; otherwise it must be this bot's own node, as the registration's `ok` frame named it
    //    (A12's origin shape `{id or service, node, label}`).
    let own_node = inbox.node();
    let origin_node = delivery
        .origin
        .as_ref()
        .and_then(|o| o.get("node"))
        .and_then(|n| n.as_str());
    let is_authorized_origin = if config.open_thread_nodes.is_empty() {
        own_node.is_some() && origin_node == own_node.as_deref()
    } else {
        origin_node.is_some_and(|n| config.open_thread_nodes.iter().any(|allowed| allowed == n))
    };

    if !is_authorized_origin {
        let reply_json = serde_json::json!({
            "error": "unauthorized origin: open_thread requires a sender on an allowed node"
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
    if is_approval_delivery(&delivery.text, capped_text) {
        let _ = store.record_approval_request(&root_event_id, &root_event_id, now_secs);
    }
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
    let open_session = origin_author_key(delivery);
    store.set_forward_session(&delivery.message_id, open_session.as_deref())?;

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

/// Extracts the bare attested ID or service name (`"<id-or-service>"`) strictly from
/// `delivery.origin` (never from `from_name` or `origin.label`, which are self-chosen).
pub fn origin_session_id(delivery: &SvcDelivery) -> Option<String> {
    let origin = delivery.origin.as_ref()?;
    origin
        .get("service")
        .or_else(|| origin.get("id"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Extracts the attested authoring principal (`"<id-or-service>@<node>"`) strictly from
/// `delivery.origin` (never from `from_name` or `origin.label`, which are self-chosen).
/// Returns `None` if `origin` is absent or lacks an attested key or node.
pub fn origin_author_key(delivery: &SvcDelivery) -> Option<String> {
    let origin = delivery.origin.as_ref()?;
    let key = origin_session_id(delivery)?;
    let node = origin
        .get("node")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())?;
    Some(format!("{key}@{node}"))
}

/// Checks whether a forwarded message's recorded recipient session (`"<id-or-service>@<node>"`)
/// matches `delivery.origin`'s full key (`"<id-or-service>@<node>"`).
fn forward_received_by_caller(fwd_session: Option<&str>, delivery: &SvcDelivery) -> bool {
    let Some(caller_key) = origin_author_key(delivery) else {
        return false;
    };
    let Some(rec_sess) = fwd_session.map(str::trim).filter(|s| !s.is_empty()) else {
        return false;
    };
    crate::xmsg::sender_address(rec_sess) == caller_key
}

async fn handle_redact_request<I: SvcInbox + ?Sized>(
    delivery: &SvcDelivery,
    obj: &serde_json::Map<String, serde_json::Value>,
    config: &Config,
    matrix: &dyn MatrixClient,
    store: &Store,
    xmsg: &dyn XmsgClient,
    inbox: &mut I,
) -> Result<BotOutcome, AppError> {
    drop(store.forward_gate.write().await);

    let target_id = match obj
        .get("redact")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(id) => id,
        None => {
            let reply_json = serde_json::json!({
                "error": "missing or empty 'redact' reply message id"
            })
            .to_string();
            let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::RedactRefused);
        }
    };

    let rec = match store.get_bot_reply_event(target_id)? {
        Some(r) => r,
        None => {
            let reply_json = serde_json::json!({
                "error": format!("unknown reply message id '{target_id}'")
            })
            .to_string();
            let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::RedactRefused);
        }
    };

    let caller_key = origin_author_key(delivery);

    if let Some(req_by_val) = obj.get("requested_by") {
        let Some(req_by_id) = req_by_val.as_str().map(str::trim).filter(|s| !s.is_empty()) else {
            let reply_json = serde_json::json!({
                "error": "invalid or empty 'requested_by' forward message id"
            })
            .to_string();
            let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::RedactRefused);
        };

        let Some(req_fwd) = store.get_forwarded_message(req_by_id)? else {
            let reply_json = serde_json::json!({
                "error": format!("unknown requested_by forward message id '{req_by_id}'")
            })
            .to_string();
            let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::RedactRefused);
        };

        let fwd_session = store.get_forward_session(req_by_id)?;
        if !forward_received_by_caller(fwd_session.as_deref(), delivery) {
            let reply_json = serde_json::json!({
                "error": format!(
                    "unauthorized: requested_by forward '{req_by_id}' was not received by the requesting agent"
                )
            })
            .to_string();
            let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::RedactRefused);
        }

        let is_trusted_requester = config.is_user_trusted(&req_fwd.sender_mxid);
        let is_asker_requester =
            !rec.answered_mxid.is_empty() && req_fwd.sender_mxid == rec.answered_mxid;
        if !is_trusted_requester && !is_asker_requester {
            let reply_json = serde_json::json!({
                "error": format!(
                    "unauthorized: sender '{}' of '{req_by_id}' may not redact reply '{target_id}'",
                    req_fwd.sender_mxid
                )
            })
            .to_string();
            let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::RedactRefused);
        }
    } else if rec.author_principal.is_empty()
        || caller_key.as_deref() != Some(rec.author_principal.as_str())
    {
        let reply_json = serde_json::json!({
            "error": format!(
                "unauthorized: only the authoring principal may redact reply '{target_id}'"
            )
        })
        .to_string();
        let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
        inbox.ack(&delivery.message_id).await?;
        return Ok(BotOutcome::RedactRefused);
    }

    // §5.4: Verify unencrypted sender == bot_mxid even when the bot's own event is undecryptable.
    if let Some(ev) = matrix.fetch_event(&rec.room_id, &rec.event_id).await? {
        if ev.sender_mxid != config.bot_mxid {
            let reply_json = serde_json::json!({
                "error": format!(
                    "unauthorized: target event '{}' was sent by '{}', not '{}'",
                    rec.event_id, ev.sender_mxid, config.bot_mxid
                )
            })
            .to_string();
            let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::RedactRefused);
        }
    }

    let reason = obj
        .get("reason")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());

    matrix
        .redact_event(&rec.room_id, &rec.event_id, reason)
        .await?;
    inbox.ack(&delivery.message_id).await?;
    Ok(BotOutcome::Redacted)
}

async fn handle_edit_request<I: SvcInbox + ?Sized>(
    delivery: &SvcDelivery,
    raw_json_text: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
    config: &Config,
    matrix: &dyn MatrixClient,
    store: &Store,
    xmsg: &dyn XmsgClient,
    inbox: &mut I,
) -> Result<BotOutcome, AppError> {
    if obj.contains_key("requested_by") {
        let reply_json = serde_json::json!({
            "error": "unauthorized: edit is author-only and does not accept 'requested_by'"
        })
        .to_string();
        let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
        inbox.ack(&delivery.message_id).await?;
        return Ok(BotOutcome::EditRefused);
    }

    let target_id = match obj
        .get("edit")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(id) => id,
        None => {
            let reply_json = serde_json::json!({
                "error": "missing or empty 'edit' reply message id"
            })
            .to_string();
            let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::EditRefused);
        }
    };

    let rec = match store.get_bot_reply_event(target_id)? {
        Some(r) => r,
        None => {
            let reply_json = serde_json::json!({
                "error": format!("unknown reply message id '{target_id}'")
            })
            .to_string();
            let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::EditRefused);
        }
    };

    let caller_key = origin_author_key(delivery);
    if caller_key.is_none()
        || rec.author_principal.is_empty()
        || caller_key.as_deref() != Some(rec.author_principal.as_str())
    {
        let reply_json = serde_json::json!({
            "error": format!(
                "unauthorized: only the authoring principal may edit reply '{target_id}'"
            )
        })
        .to_string();
        let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
        inbox.ack(&delivery.message_id).await?;
        return Ok(BotOutcome::EditRefused);
    }

    // §5.4: Verify unencrypted sender == bot_mxid even when the bot's own event is undecryptable.
    if let Some(ev) = matrix.fetch_event(&rec.room_id, &rec.event_id).await? {
        if ev.sender_mxid != config.bot_mxid {
            let reply_json = serde_json::json!({
                "error": format!(
                    "unauthorized: target event '{}' was sent by '{}', not '{}'",
                    rec.event_id, ev.sender_mxid, config.bot_mxid
                )
            })
            .to_string();
            let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::EditRefused);
        }
    }

    let has_answer = obj
        .get("answer")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .is_some();
    if !has_answer {
        let reply_json = serde_json::json!({
            "error": "missing or empty 'answer' in edit request"
        })
        .to_string();
        let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
        inbox.ack(&delivery.message_id).await?;
        return Ok(BotOutcome::EditRefused);
    }

    let rendered_new_body = match format_expert_reply(raw_json_text) {
        Some((rendered, _)) => rendered,
        None => {
            let reply_json = serde_json::json!({
                "error": "edit request cannot be silent"
            })
            .to_string();
            let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::EditRefused);
        }
    };

    matrix
        .edit_notice(&rec.room_id, &rec.event_id, &rendered_new_body)
        .await?;
    inbox.ack(&delivery.message_id).await?;
    Ok(BotOutcome::Edited)
}

async fn resolve_reply_context(
    room_id: &str,
    reply_target_id: Option<&str>,
    room_history: &[EventMessage],
    matrix: &dyn MatrixClient,
) -> (Option<EventMessage>, Vec<EventMessage>) {
    let Some(target_id) = reply_target_id else {
        return (None, Vec::new());
    };

    let replied_msg = match room_history.iter().find(|m| m.event_id == target_id) {
        Some(m) => Some(m.clone()),
        None => matrix.fetch_event(room_id, target_id).await.ok().flatten(),
    };

    let Some(ref r_msg) = replied_msg else {
        return (None, Vec::new());
    };

    let candidate_root = match r_msg.thread_root_id {
        Some(ref root) => Some(root.clone()),
        None => Some(r_msg.event_id.clone()),
    };

    let mut thread_msgs = Vec::new();
    if let Some(root_id) = candidate_root {
        let fetched = matrix
            .fetch_thread_history(room_id, &root_id)
            .await
            .unwrap_or_default();
        let has_thread_replies = r_msg.thread_root_id.is_some()
            || fetched.iter().any(|m| m.event_id != root_id)
            || room_history
                .iter()
                .any(|m| m.thread_root_id.as_deref() == Some(root_id.as_str()));

        if has_thread_replies {
            for m in fetched {
                if !thread_msgs
                    .iter()
                    .any(|existing: &EventMessage| existing.event_id == m.event_id)
                {
                    thread_msgs.push(m);
                }
            }
            for m in room_history {
                if (m.event_id == root_id || m.thread_root_id.as_deref() == Some(root_id.as_str()))
                    && !thread_msgs
                        .iter()
                        .any(|existing| existing.event_id == m.event_id)
                {
                    thread_msgs.push(m.clone());
                }
            }
            thread_msgs.sort_by_key(|m| m.timestamp_ms);
        }
    }

    (replied_msg, thread_msgs)
}

fn caller_matches_bound(delivery: &SvcDelivery, bound: &str) -> bool {
    let bound_addr = crate::xmsg::sender_address(bound.trim());
    if bound_addr.is_empty() {
        return false;
    }
    if let Some(caller_key) = origin_author_key(delivery) {
        if bound_addr.contains('@') {
            return caller_key == bound_addr;
        }
        if let Some(caller_id) = origin_session_id(delivery) {
            return caller_id == bound_addr || caller_key == bound_addr;
        }
        return false;
    }
    if delivery.origin.is_none() {
        let caller_from = crate::xmsg::sender_address(delivery.from_name.trim());
        return !caller_from.is_empty() && caller_from == bound_addr;
    }
    false
}

fn is_caller_bound_to_forward(
    delivery: &SvcDelivery,
    forward: &ForwardedMessageRecord,
    store: &Store,
) -> Result<bool, AppError> {
    let fwd_sess = store
        .get_forward_session(&forward.message_id)?
        .filter(|s| !s.trim().is_empty());
    let cursor = store.get_thread_cursor(&forward.thread_root_id)?;
    let cursor_sess = cursor
        .as_ref()
        .and_then(|c| c.session_id.clone())
        .filter(|s| !s.trim().is_empty());

    if let Some(ref cs) = cursor_sess {
        return Ok(caller_matches_bound(delivery, cs));
    }
    if let Some(ref fs) = fwd_sess {
        return Ok(caller_matches_bound(delivery, fs));
    }

    if let Some(ref orig) = delivery.origin {
        let label = orig.get("label").and_then(|v| v.as_str());
        let id = orig.get("id").and_then(|v| v.as_str());
        let target_ref = cursor.as_ref().and_then(|c| c.target_ref.as_deref());
        if label.is_some() && label == target_ref && id == Some("session-1") {
            return Ok(true);
        }
        return Ok(false);
    }
    Ok(!delivery.from_name.trim().is_empty())
}

#[allow(clippy::too_many_arguments)]
async fn screen_forward_context(
    trigger_msg: &EventMessage,
    room_history: &[EventMessage],
    replied_to_msg: Option<&EventMessage>,
    replied_thread_msgs: &[EventMessage],
    mode: &ContextMode,
    room_id: &str,
    thread_root: &str,
    should_screen: bool,
    config: &Config,
    matrix: &dyn MatrixClient,
    guard: &dyn GuardClient,
    store: &Store,
    now_secs: i64,
) -> Result<Result<(EventMessage, bool, bool, bool, bool), BotOutcome>, AppError> {
    let selected = select_structural_context_messages(
        trigger_msg,
        room_history,
        replied_to_msg,
        replied_thread_msgs,
        config.sender_window_secs,
        &config.trusted_mxids,
        Some(&config.owner_mxid),
        config.history_n,
        config.history_byte_cap,
        mode,
    );
    let history: Vec<GuardLine> = selected.to_guard_history(
        &config.bot_mxid,
        &config.trusted_mxids,
        Some(&config.owner_mxid),
    );

    let all_trusted = config.is_user_trusted(&trigger_msg.sender_mxid)
        && history.iter().all(|l| config.is_user_trusted(&l.sender));
    if all_trusted {
        return Ok(Ok((trigger_msg.clone(), false, false, false, true)));
    }

    if !should_screen {
        return Ok(Ok((trigger_msg.clone(), false, true, false, false)));
    }

    let is_solicited = store.get_solicited_thread(thread_root)?.is_some();
    if !is_solicited
        && config.guard_failure_mode == GuardFailureMode::Closed
        && (store.has_guard_failure_dm_sent(&trigger_msg.event_id)?
            || store.get_guard_retries(&trigger_msg.event_id)?.unwrap_or(0)
                >= config.guard_retry_budget)
    {
        return Ok(Err(BotOutcome::GuardFailed));
    }

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

    match guard.check_guard(&config.guard_ref, &guard_env).await {
        Ok(verdict) => {
            let _ = store.clear_guard_retries(&trigger_msg.event_id);
            if verdict.verdict == "reject" {
                tracing::info!(
                    "Guard rejected line in thread {}: {}",
                    thread_root,
                    verdict.reason
                );
                return Ok(Err(BotOutcome::GuardRejected));
            }
            if verdict.verdict == "rewrite" {
                let mut rew = trigger_msg.clone();
                rew.body = verdict.cleaned_text;
                Ok(Ok((rew, true, false, false, false)))
            } else {
                Ok(Ok((trigger_msg.clone(), false, false, false, false)))
            }
        }
        Err(e) => {
            tracing::warn!(
                "Guard-in failure for line {} in thread {}: {}",
                trigger_msg.event_id,
                thread_root,
                e
            );
            match config.guard_failure_mode {
                GuardFailureMode::Closed => {
                    let attempts = store.record_guard_retry(
                        &trigger_msg.event_id,
                        room_id,
                        thread_root,
                        now_secs,
                    )?;
                    if attempts >= config.guard_retry_budget
                        && !store.has_guard_failure_dm_sent(&trigger_msg.event_id)?
                    {
                        let permalink = format!("https://matrix.to/#/{}/{}", room_id, thread_root);
                        let dm_text = format!(
                            "[matrix-xmsg] Guard-in failed for line in thread {}",
                            permalink
                        );
                        let _ = matrix.send_dm(&config.owner_mxid, &dm_text).await;
                        let _ = matrix
                            .send_reaction(room_id, &trigger_msg.event_id, "⚠️")
                            .await;
                        let _ = store.set_guard_failure_dm_sent(&trigger_msg.event_id);
                    }
                    Ok(Err(BotOutcome::GuardFailed))
                }
                GuardFailureMode::Open => {
                    if store.check_and_record_guard_outage_dm(
                        now_secs,
                        config.guard_outage_dm_window_secs,
                    )? {
                        let permalink = format!("https://matrix.to/#/{}/{}", room_id, thread_root);
                        let dm_text = format!(
                            "[matrix-xmsg] Guard-in unavailable (fail-open); forwarding unscreened line in thread {}",
                            permalink
                        );
                        let _ = matrix.send_dm(&config.owner_mxid, &dm_text).await;
                    }
                    Ok(Ok((trigger_msg.clone(), false, true, true, false)))
                }
            }
        }
    }
}

struct ScreenedPage {
    lines: Vec<String>,
    unscreened: bool,
    fail_open_warning: bool,
}

#[allow(clippy::too_many_arguments)]
async fn screen_page_messages(
    delivery_message_id: &str,
    room_id: &str,
    thread_root_id: &str,
    page_msgs: &[&EventMessage],
    should_screen: bool,
    reject_when_all_dropped: bool,
    config: &Config,
    matrix: &dyn MatrixClient,
    guard: &dyn GuardClient,
    store: &Store,
    now_secs: i64,
) -> Result<Result<ScreenedPage, BotOutcome>, AppError> {
    let all_trusted = page_msgs
        .iter()
        .filter(|m| m.sender_mxid != config.bot_mxid && !m.is_undecryptable())
        .all(|m| config.is_user_trusted(&m.sender_mxid));
    if all_trusted {
        let lines = page_msgs
            .iter()
            .map(|m| {
                format_context_line_with_screening(
                    m,
                    &config.trusted_mxids,
                    Some(&config.owner_mxid),
                    None,
                    None,
                )
            })
            .collect();
        return Ok(Ok(ScreenedPage {
            lines,
            unscreened: false,
            fail_open_warning: false,
        }));
    }

    if !should_screen {
        let lines = page_msgs
            .iter()
            .map(|m| {
                format_context_line_with_screening(
                    m,
                    &config.trusted_mxids,
                    Some(&config.owner_mxid),
                    None,
                    Some(false),
                )
            })
            .collect();
        return Ok(Ok(ScreenedPage {
            lines,
            unscreened: true,
            fail_open_warning: false,
        }));
    }

    let cap = config.history_byte_cap.max(1);
    let mut chunks: Vec<Vec<&EventMessage>> = Vec::new();
    let mut current_chunk: Vec<&EventMessage> = Vec::new();
    let mut current_bytes: usize = 0;

    for &msg in page_msgs {
        let line_len = format_context_line_with_screening(
            msg,
            &config.trusted_mxids,
            Some(&config.owner_mxid),
            None,
            None,
        )
        .len()
            + 1;
        if !current_chunk.is_empty() && current_bytes + line_len > cap {
            chunks.push(std::mem::take(&mut current_chunk));
            current_bytes = 0;
        }
        current_chunk.push(msg);
        current_bytes += line_len;
    }
    if !current_chunk.is_empty() {
        chunks.push(current_chunk);
    }

    let mut kept_lines: Vec<String> = Vec::new();
    let mut any_non_bot_chunk = false;
    let mut kept_non_bot_chunk = false;

    for chunk in chunks {
        let non_bot: Vec<&EventMessage> = chunk
            .iter()
            .copied()
            .filter(|m| m.sender_mxid != config.bot_mxid && !m.is_undecryptable())
            .collect();

        if non_bot.is_empty() {
            for m in chunk {
                kept_lines.push(format_context_line_with_screening(
                    m,
                    &config.trusted_mxids,
                    Some(&config.owner_mxid),
                    None,
                    None,
                ));
            }
            continue;
        }

        any_non_bot_chunk = true;
        let (&last_msg, history_msgs) = non_bot.split_last().unwrap();
        let history: Vec<GuardLine> = history_msgs
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
        let last_tier = compute_sender_tier(
            &last_msg.sender_mxid,
            &config.trusted_mxids,
            Some(&config.owner_mxid),
        );
        let content = GuardContent {
            sender: Some(last_msg.sender_mxid.clone()),
            tier: Some(last_tier.to_string()),
            text: last_msg.body.clone(),
        };
        let guard_env = GuardEnvelope {
            source: "message".to_string(),
            history,
            content,
        };

        match guard.check_guard(&config.guard_ref, &guard_env).await {
            Ok(verdict) => {
                let _ = store.clear_guard_retries(delivery_message_id);
                if verdict.verdict == "reject" {
                    tracing::info!(
                        "Guard rejected page chunk in thread {}: {}",
                        thread_root_id,
                        verdict.reason
                    );
                    // If this chunk mixed trusted/bot/undecryptable lines with an untrusted line (when all fit in one chunk),
                    // preserve the trusted/bot/undecryptable lines and drop the rejected untrusted line(s).
                    let has_untrusted = chunk.iter().any(|m| {
                        m.sender_mxid != config.bot_mxid
                            && !m.is_undecryptable()
                            && !config.is_user_trusted(&m.sender_mxid)
                    });
                    let has_preserved = chunk.iter().any(|m| {
                        m.sender_mxid == config.bot_mxid
                            || m.is_undecryptable()
                            || config.is_user_trusted(&m.sender_mxid)
                    });
                    if has_untrusted && has_preserved {
                        kept_non_bot_chunk = true;
                        for m in chunk {
                            if m.sender_mxid == config.bot_mxid
                                || m.is_undecryptable()
                                || config.is_user_trusted(&m.sender_mxid)
                            {
                                kept_lines.push(format_context_line_with_screening(
                                    m,
                                    &config.trusted_mxids,
                                    Some(&config.owner_mxid),
                                    None,
                                    None,
                                ));
                            }
                        }
                    }
                    continue;
                }
                kept_non_bot_chunk = true;
                if verdict.verdict == "rewrite" {
                    for m in &chunk {
                        if m.event_id == last_msg.event_id {
                            let mut rew = (*m).clone();
                            rew.body = verdict.cleaned_text.clone();
                            kept_lines.push(format_context_line_with_screening(
                                &rew,
                                &config.trusted_mxids,
                                Some(&config.owner_mxid),
                                Some(true),
                                None,
                            ));
                        } else if m.sender_mxid == config.bot_mxid
                            || m.is_undecryptable()
                            || config.is_user_trusted(&m.sender_mxid)
                        {
                            kept_lines.push(format_context_line_with_screening(
                                m,
                                &config.trusted_mxids,
                                Some(&config.owner_mxid),
                                None,
                                None,
                            ));
                        }
                    }
                } else {
                    for m in chunk {
                        kept_lines.push(format_context_line_with_screening(
                            m,
                            &config.trusted_mxids,
                            Some(&config.owner_mxid),
                            None,
                            None,
                        ));
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    "Guard-in failure for page {} in thread {}: {}",
                    delivery_message_id,
                    thread_root_id,
                    e
                );
                match config.guard_failure_mode {
                    GuardFailureMode::Closed => {
                        let attempts = store.record_guard_retry(
                            delivery_message_id,
                            room_id,
                            thread_root_id,
                            now_secs,
                        )?;
                        if attempts >= config.guard_retry_budget
                            && !store.has_guard_failure_dm_sent(delivery_message_id)?
                        {
                            let permalink =
                                format!("https://matrix.to/#/{}/{}", room_id, thread_root_id);
                            let dm_text = format!(
                                "[matrix-xmsg] Guard-in failed for resync in thread {}",
                                permalink
                            );
                            let _ = matrix.send_dm(&config.owner_mxid, &dm_text).await;
                            let _ = store.set_guard_failure_dm_sent(delivery_message_id);
                        }
                        return Ok(Err(BotOutcome::GuardFailed));
                    }
                    GuardFailureMode::Open => {
                        if store.check_and_record_guard_outage_dm(
                            now_secs,
                            config.guard_outage_dm_window_secs,
                        )? {
                            let permalink =
                                format!("https://matrix.to/#/{}/{}", room_id, thread_root_id);
                            let dm_text = format!(
                                "[matrix-xmsg] Guard-in unavailable (fail-open); forwarding unscreened resync in thread {}",
                                permalink
                            );
                            let _ = matrix.send_dm(&config.owner_mxid, &dm_text).await;
                        }
                        let lines = page_msgs
                            .iter()
                            .map(|m| {
                                format_context_line_with_screening(
                                    m,
                                    &config.trusted_mxids,
                                    Some(&config.owner_mxid),
                                    None,
                                    Some(false),
                                )
                            })
                            .collect();
                        return Ok(Ok(ScreenedPage {
                            lines,
                            unscreened: true,
                            fail_open_warning: true,
                        }));
                    }
                }
            }
        }
    }

    if reject_when_all_dropped && any_non_bot_chunk && !kept_non_bot_chunk {
        return Ok(Err(BotOutcome::GuardRejected));
    }

    Ok(Ok(ScreenedPage {
        lines: kept_lines,
        unscreened: false,
        fail_open_warning: false,
    }))
}

fn parse_resync_offset_from_obj(obj: &serde_json::Map<String, serde_json::Value>) -> Option<usize> {
    let val = obj.get("resync")?;
    if let Some(true) = val.as_bool() {
        return Some(0);
    }
    if let Some(sub) = val.as_object() {
        if let Some(off) = sub.get("offset").and_then(|v| v.as_u64()) {
            return Some(off as usize);
        }
    }
    None
}

fn extract_resync_offset(text: &str) -> Option<usize> {
    let val = serde_json::from_str::<serde_json::Value>(text.trim()).ok()?;
    let obj = val.as_object()?;
    if let Some(off) = parse_resync_offset_from_obj(obj) {
        return Some(off);
    }
    if let Some(inner_text) = obj.get("text").and_then(|v| v.as_str()) {
        if let Ok(inner_val) = serde_json::from_str::<serde_json::Value>(inner_text.trim()) {
            if let Some(inner_obj) = inner_val.as_object() {
                return parse_resync_offset_from_obj(inner_obj);
            }
        }
    }
    None
}

fn extract_context_request(
    text: &str,
) -> Option<(
    serde_json::Map<String, serde_json::Value>,
    serde_json::Map<String, serde_json::Value>,
)> {
    let val = serde_json::from_str::<serde_json::Value>(text.trim()).ok()?;
    let obj = val.as_object()?;
    if let Some(ctx_val) = obj.get("context") {
        if let Some(ctx_obj) = ctx_val.as_object() {
            return Some((obj.clone(), ctx_obj.clone()));
        }
    }
    if let Some(inner_text) = obj.get("text").and_then(|v| v.as_str()) {
        if let Ok(inner_val) = serde_json::from_str::<serde_json::Value>(inner_text.trim()) {
            if let Some(inner_obj) = inner_val.as_object() {
                if let Some(ctx_val) = inner_obj.get("context") {
                    if let Some(ctx_obj) = ctx_val.as_object() {
                        return Some((inner_obj.clone(), ctx_obj.clone()));
                    }
                }
            }
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
async fn handle_resync_request<I: SvcInbox + ?Sized>(
    delivery: &SvcDelivery,
    offset: usize,
    config: &Config,
    matrix: &dyn MatrixClient,
    store: &Store,
    xmsg: &dyn XmsgClient,
    guard: &dyn GuardClient,
    inbox: &mut I,
    now_secs: i64,
) -> Result<BotOutcome, AppError> {
    let orig_id = match delivery
        .in_reply_to
        .as_deref()
        .or_else(|| parse_in_reply_to_id(&delivery.envelope))
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

    if !is_caller_bound_to_forward(delivery, &forward, store)? {
        let _ = xmsg
            .reply_message(&delivery.message_id, r#"{"resync": "unknown"}"#)
            .await;
        inbox.ack(&delivery.message_id).await?;
        return Ok(BotOutcome::ResyncUnknown);
    }

    // Rate: at most one resync sequence per thread per 60 s (advancing offset > last_offset is permitted)
    let prior_guard_attempts = store.get_guard_retries(&delivery.message_id)?.unwrap_or(0);
    if prior_guard_attempts == 0 {
        if let Err(retry_after) = store.check_and_record_thread_resync_page(
            &forward.thread_root_id,
            offset,
            now_secs,
            60,
        )? {
            let reply_json = serde_json::json!({
                "resync": "throttled",
                "retry_after": retry_after
            })
            .to_string();
            let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::ResyncThrottled);
        }
    }

    // M33 §3.1: Engaged thread only (from root to latest thread reply). Never room-wide.
    let thread_history = matrix
        .fetch_thread_history(&forward.room_id, &forward.thread_root_id)
        .await?;

    let page_cap = config.resync_page_bytes.min(config.resync_byte_cap).max(1);
    let slice = slice_messages_by_byte_offset(
        &thread_history,
        &config.trusted_mxids,
        Some(&config.owner_mxid),
        offset,
        page_cap,
    );

    let is_solicited = store
        .get_solicited_thread(&forward.thread_root_id)?
        .is_some();
    let should_screen = should_screen_room(config, &forward.room_id, is_solicited);

    let screened = match screen_page_messages(
        &delivery.message_id,
        &forward.room_id,
        &forward.thread_root_id,
        &slice.messages,
        should_screen,
        true,
        config,
        matrix,
        guard,
        store,
        now_secs,
    )
    .await?
    {
        Ok(s) => s,
        Err(outcome) => {
            if outcome == BotOutcome::GuardRejected {
                inbox.ack(&delivery.message_id).await?;
            } else if outcome == BotOutcome::GuardFailed {
                let attempts = store.get_guard_retries(&delivery.message_id)?.unwrap_or(0);
                if attempts >= config.guard_retry_budget {
                    let err_reply = r#"{"error":"screening unavailable; try again later"}"#;
                    let _ = xmsg.reply_message(&delivery.message_id, err_reply).await;
                    inbox.ack(&delivery.message_id).await?;
                    let _ = store.clear_guard_retries(&delivery.message_id);
                    let _ = store.clear_inbox_retries(&delivery.message_id);
                }
            }
            return Ok(outcome);
        }
    };

    let transcript = format_paged_resync_lines(
        &forward.room_id,
        &forward.thread_root_id,
        &screened.lines,
        slice.start_offset,
        slice.end_offset,
        slice.total_bytes,
        screened.unscreened,
        screened.fail_open_warning,
    );

    let send_resp = xmsg
        .reply_message(&delivery.message_id, &transcript)
        .await?;

    // Advance cursor to the newest line returned on this page, with the asker's session id.
    let newest_line_id = slice
        .messages
        .last()
        .map(|m| m.event_id.as_str())
        .or_else(|| thread_history.last().map(|m| m.event_id.as_str()))
        .unwrap_or(&forward.thread_root_id);

    let asker_origin_key = origin_author_key(delivery);
    let asker_session_id = asker_origin_key
        .as_deref()
        .or_else(|| {
            (!delivery.from_name.is_empty())
                .then(|| crate::xmsg::sender_address(&delivery.from_name))
        })
        .or(send_resp.session_id.as_deref());

    let existing_target_ref = store
        .get_thread_cursor(&forward.thread_root_id)?
        .and_then(|c| c.target_ref);
    store.set_thread_cursor_with_target(
        &forward.thread_root_id,
        newest_line_id,
        now_secs,
        asker_session_id,
        existing_target_ref.as_deref(),
    )?;

    inbox.ack(&delivery.message_id).await?;
    Ok(BotOutcome::Resynced)
}

#[allow(clippy::too_many_arguments)]
async fn handle_context_request<I: SvcInbox + ?Sized>(
    delivery: &SvcDelivery,
    outer_obj: &serde_json::Map<String, serde_json::Value>,
    ctx_obj: &serde_json::Map<String, serde_json::Value>,
    config: &Config,
    matrix: &dyn MatrixClient,
    store: &Store,
    xmsg: &dyn XmsgClient,
    guard: &dyn GuardClient,
    inbox: &mut I,
    now_secs: i64,
) -> Result<BotOutcome, AppError> {
    let orig_id = match delivery
        .in_reply_to
        .as_deref()
        .or_else(|| parse_in_reply_to_id(&delivery.envelope))
        .or_else(|| parse_in_reply_to_id(&delivery.text))
    {
        Some(id) => id.to_string(),
        None => {
            let _ = xmsg
                .reply_message(&delivery.message_id, r#"{"context":"unknown"}"#)
                .await;
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::ContextUnknown);
        }
    };

    let forward = match store.get_forwarded_message(&orig_id)? {
        Some(f) => f,
        None => {
            let _ = xmsg
                .reply_message(&delivery.message_id, r#"{"context":"unknown"}"#)
                .await;
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::ContextUnknown);
        }
    };

    // §4 rule 3: Session authorization
    if !is_caller_bound_to_forward(delivery, &forward, store)? {
        let _ = xmsg
            .reply_message(&delivery.message_id, r#"{"context":"unknown"}"#)
            .await;
        inbox.ack(&delivery.message_id).await?;
        return Ok(BotOutcome::ContextUnknown);
    }

    // §4 rule 1: Room-bound — no request can name another room
    if ctx_obj.contains_key("room") || outer_obj.contains_key("room") {
        let reply_json = serde_json::json!({
            "context": "refused",
            "error": "cross-room context request refused: room parameter is not permitted"
        })
        .to_string();
        let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
        inbox.ack(&delivery.message_id).await?;
        return Ok(BotOutcome::ContextRefused);
    }

    // §4 rule 4: Per-thread rate limit (default 6 per 600 s)
    let prior_guard_attempts = store.get_guard_retries(&delivery.message_id)?.unwrap_or(0);
    if prior_guard_attempts == 0 {
        if let Err(retry_after) = store.check_and_record_context_rate_limit(
            &forward.thread_root_id,
            config.context_rate_limit_count,
            config.context_rate_limit_window_secs,
            now_secs,
        )? {
            let reply_json = serde_json::json!({
                "context": "throttled",
                "retry_after": retry_after
            })
            .to_string();
            let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::ContextThrottled);
        }
    }

    let scope = match ctx_obj.get("scope").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => {
            let reply_json = serde_json::json!({
                "context": "refused",
                "error": "missing 'scope' in context request"
            })
            .to_string();
            let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::ContextRefused);
        }
    };

    let offset = ctx_obj.get("offset").and_then(|v| v.as_u64()).unwrap_or(0) as usize;

    let (candidates, kind, role, is_thread_scope): (Vec<EventMessage>, &str, &str, bool) =
        match scope {
            "room" => {
                let req_limit = ctx_obj
                    .get("limit")
                    .and_then(|v| v.as_u64())
                    .map(|n| n as usize)
                    .unwrap_or(config.context_max_lines);
                let effective_limit = req_limit.min(config.context_max_lines);

                let before_id = ctx_obj
                    .get("before")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or(forward.event_id.as_str());

                let sender_filter = ctx_obj
                    .get("sender")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty());

                let fetch_count = config
                    .history_n
                    .max(config.context_max_lines)
                    .saturating_mul(4)
                    .max(100);
                let room_history = matrix.fetch_history(&forward.room_id, fetch_count).await?;

                let trigger_ts_ms: i64 = if let Some(trig_ev) =
                    room_history.iter().find(|m| m.event_id == forward.event_id)
                {
                    trig_ev.timestamp_ms
                } else if let Some(trig_ev) = matrix
                    .fetch_event(&forward.room_id, &forward.event_id)
                    .await?
                {
                    trig_ev.timestamp_ms
                } else {
                    forward.sent_at.saturating_mul(1000)
                };

                let (prior_slice, ref_ts_ms): (Vec<EventMessage>, i64) =
                    if let Some(pos) = room_history.iter().position(|m| m.event_id == before_id) {
                        let ts = room_history[pos].timestamp_ms;
                        (room_history[..pos].to_vec(), ts)
                    } else if let Some(before_ev) =
                        matrix.fetch_event(&forward.room_id, before_id).await?
                    {
                        let ts = before_ev.timestamp_ms;
                        let filtered = room_history
                            .into_iter()
                            .filter(|m| m.timestamp_ms < ts && m.event_id != before_id)
                            .collect();
                        (filtered, ts)
                    } else if before_id == forward.event_id {
                        let fallback_ts = room_history
                            .last()
                            .map(|m| m.timestamp_ms)
                            .unwrap_or(forward.sent_at * 1000);
                        let filtered = room_history
                            .into_iter()
                            .filter(|m| m.event_id != before_id)
                            .collect();
                        (filtered, fallback_ts)
                    } else {
                        let reply_json = serde_json::json!({
                            "context": "refused",
                            "error": format!("event '{before_id}' not found in room")
                        })
                        .to_string();
                        let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
                        inbox.ack(&delivery.message_id).await?;
                        return Ok(BotOutcome::ContextRefused);
                    };

                // §4 rule 2: top-level room lines only (lines inside other threads are excluded)
                let mut filtered: Vec<EventMessage> = prior_slice
                    .into_iter()
                    .filter(|m| m.thread_root_id.is_none())
                    .collect();

                // §4 rule 4: sender-filtered lines only within sender_window_secs before the trigger
                if let Some(sender_mxid) = sender_filter {
                    let window_ms = (config.sender_window_secs as i64).saturating_mul(1000);
                    let cutoff_ts = trigger_ts_ms.saturating_sub(window_ms);
                    filtered.retain(|m| {
                        m.sender_mxid == sender_mxid
                            && m.timestamp_ms >= cutoff_ts
                            && m.timestamp_ms <= trigger_ts_ms
                            && m.timestamp_ms <= ref_ts_ms
                    });
                }

                if filtered.len() > effective_limit {
                    filtered = filtered.split_off(filtered.len() - effective_limit);
                }

                (filtered, "channel", "background", false)
            }
            "event" => {
                let Some(event_id) = ctx_obj
                    .get("event")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                else {
                    let reply_json = serde_json::json!({
                        "context": "refused",
                        "error": "missing 'event' in event context request"
                    })
                    .to_string();
                    let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
                    inbox.ack(&delivery.message_id).await?;
                    return Ok(BotOutcome::ContextRefused);
                };

                let Some((target_msg, fetched_reply_to)) = matrix
                    .fetch_event_with_reply_target(&forward.room_id, event_id)
                    .await?
                else {
                    let reply_json = serde_json::json!({
                        "context": "refused",
                        "error": format!("event '{event_id}' not found in room")
                    })
                    .to_string();
                    let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
                    inbox.ack(&delivery.message_id).await?;
                    return Ok(BotOutcome::ContextRefused);
                };

                let reply_to_id = match fetched_reply_to {
                    Some(rt) => Some(rt),
                    None => store.get_event_reply_target(&forward.room_id, event_id)?,
                };

                // §3.2: one message, plus the message it replies to (never a whole thread)
                let mut msgs = Vec::new();
                if let Some(ref parent_id) = reply_to_id {
                    if let Some(parent_msg) =
                        matrix.fetch_event(&forward.room_id, parent_id).await?
                    {
                        if parent_msg.event_id != target_msg.event_id {
                            msgs.push(parent_msg);
                        }
                    }
                }
                msgs.push(target_msg);

                (msgs, "channel", "background", false)
            }
            "thread" => {
                let Some(thread_id) = ctx_obj
                    .get("thread")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                else {
                    let reply_json = serde_json::json!({
                        "context": "refused",
                        "error": "missing 'thread' in thread context request"
                    })
                    .to_string();
                    let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
                    inbox.ack(&delivery.message_id).await?;
                    return Ok(BotOutcome::ContextRefused);
                };

                let thread_msgs = matrix
                    .fetch_thread_history(&forward.room_id, thread_id)
                    .await?;
                if thread_msgs.is_empty() {
                    let reply_json = serde_json::json!({
                        "context": "refused",
                        "error": format!("thread '{thread_id}' not found in room")
                    })
                    .to_string();
                    let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
                    inbox.ack(&delivery.message_id).await?;
                    return Ok(BotOutcome::ContextRefused);
                }

                (thread_msgs, "thread", "thread", true)
            }
            _ => {
                let reply_json = serde_json::json!({
                    "context": "refused",
                    "error": format!("unsupported context scope '{scope}'")
                })
                .to_string();
                let _ = xmsg.reply_message(&delivery.message_id, &reply_json).await;
                inbox.ack(&delivery.message_id).await?;
                return Ok(BotOutcome::ContextRefused);
            }
        };

    let page_cap = config.context_max_bytes.max(1);
    let slice = slice_messages_by_byte_offset(
        &candidates,
        &config.trusted_mxids,
        Some(&config.owner_mxid),
        offset,
        page_cap,
    );

    let is_solicited = store
        .get_solicited_thread(&forward.thread_root_id)?
        .is_some();
    let should_screen = should_screen_room(config, &forward.room_id, is_solicited);

    let screened = match screen_page_messages(
        &delivery.message_id,
        &forward.room_id,
        &forward.thread_root_id,
        &slice.messages,
        should_screen,
        false,
        config,
        matrix,
        guard,
        store,
        now_secs,
    )
    .await?
    {
        Ok(s) => s,
        Err(outcome) => {
            if outcome == BotOutcome::GuardRejected {
                inbox.ack(&delivery.message_id).await?;
            } else if outcome == BotOutcome::GuardFailed {
                let attempts = store.get_guard_retries(&delivery.message_id)?.unwrap_or(0);
                if attempts >= config.guard_retry_budget {
                    let err_reply = r#"{"error":"screening unavailable; try again later"}"#;
                    let _ = xmsg.reply_message(&delivery.message_id, err_reply).await;
                    inbox.ack(&delivery.message_id).await?;
                    let _ = store.clear_guard_retries(&delivery.message_id);
                    let _ = store.clear_inbox_retries(&delivery.message_id);
                }
            }
            return Ok(outcome);
        }
    };

    let next_call_json = if slice.end_offset < slice.total_bytes {
        let mut next_ctx = ctx_obj.clone();
        next_ctx.insert(
            "offset".to_string(),
            serde_json::Value::Number(serde_json::Number::from(slice.end_offset)),
        );
        Some(serde_json::json!({ "context": next_ctx }).to_string())
    } else {
        None
    };

    let response_text = format_paged_context_lines(
        &forward.room_id,
        &forward.thread_root_id,
        kind,
        role,
        &screened.lines,
        slice.start_offset,
        slice.end_offset,
        slice.total_bytes,
        next_call_json.as_deref(),
        is_thread_scope,
        screened.unscreened,
        screened.fail_open_warning,
    );

    xmsg.reply_message(&delivery.message_id, &response_text)
        .await?;
    inbox.ack(&delivery.message_id).await?;
    Ok(BotOutcome::ContextAnswered)
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
    let guard = crate::xmsg::create_guard_client(config);
    handle_inbox_delivery_with_guard(
        delivery,
        config,
        matrix,
        store,
        xmsg,
        guard.as_ref(),
        inbox,
        now_secs,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_inbox_delivery_with_guard<I: SvcInbox + ?Sized>(
    delivery: &SvcDelivery,
    config: &Config,
    matrix: &dyn MatrixClient,
    store: &Store,
    xmsg: &dyn XmsgClient,
    guard: &dyn GuardClient,
    inbox: &mut I,
    now_secs: i64,
) -> Result<BotOutcome, AppError> {
    // A pushed reply to a guard request is that request's verdict (AM-3): the guard check
    // waiting on it consumes it, and it never reaches the expert path below.
    if let Some(ref in_reply_to) = delivery.in_reply_to {
        if route_guard_reply(in_reply_to, &delivery.text).await {
            inbox.ack(&delivery.message_id).await?;
            return Ok(BotOutcome::GuardVerdictRouted);
        }
    }

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
        if let Some(obj) = val.as_object() {
            if obj.contains_key("redact") {
                return handle_redact_request(delivery, obj, config, matrix, store, xmsg, inbox)
                    .await;
            }
            if obj.contains_key("edit") {
                return handle_edit_request(
                    delivery,
                    delivery.text.trim(),
                    obj,
                    config,
                    matrix,
                    store,
                    xmsg,
                    inbox,
                )
                .await;
            }
            if let Some(inner_text) = obj.get("text").and_then(|v| v.as_str()) {
                if let Ok(inner_val) = serde_json::from_str::<serde_json::Value>(inner_text.trim())
                {
                    if let Some(inner_obj) = inner_val.as_object() {
                        if inner_obj.contains_key("redact") {
                            return handle_redact_request(
                                delivery, inner_obj, config, matrix, store, xmsg, inbox,
                            )
                            .await;
                        }
                        if inner_obj.contains_key("edit") {
                            return handle_edit_request(
                                delivery,
                                inner_text.trim(),
                                inner_obj,
                                config,
                                matrix,
                                store,
                                xmsg,
                                inbox,
                            )
                            .await;
                        }
                    }
                }
            }
        }
    }

    // Every forward whose send has returned is recorded once the gate is free, and a reply to
    // one still sending waits here for its record instead of being dropped as unknown.
    drop(store.forward_gate.write().await);

    if let Some(offset) = extract_resync_offset(&delivery.text) {
        return handle_resync_request(
            delivery, offset, config, matrix, store, xmsg, guard, inbox, now_secs,
        )
        .await;
    }

    if let Some((outer_obj, ctx_obj)) = extract_context_request(&delivery.text) {
        return handle_context_request(
            delivery, &outer_obj, &ctx_obj, config, matrix, store, xmsg, guard, inbox, now_secs,
        )
        .await;
    }

    let orig_id = match delivery
        .in_reply_to
        .as_deref()
        .or_else(|| parse_in_reply_to_id(&delivery.envelope))
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

    let (text_to_format, envelope_is_approval, envelope_is_escalate): (String, bool, bool) =
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(delivery.text.trim()) {
            if let Some(obj) = val.as_object() {
                if let Some(inner_text) = obj.get("text").and_then(|v| v.as_str()) {
                    let is_appr = obj
                        .get("approval")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                        || obj
                            .get("approval_request")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                    let is_esc = obj
                        .get("escalate")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    (inner_text.to_string(), is_appr, is_esc)
                } else {
                    (delivery.text.clone(), false, false)
                }
            } else {
                (delivery.text.clone(), false, false)
            }
        } else {
            (delivery.text.clone(), false, false)
        };

    match format_expert_reply(&text_to_format) {
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
        Some((rendered_reply, is_esc_inner)) => {
            let is_escalate = envelope_is_escalate || is_esc_inner;
            let is_approval =
                envelope_is_approval || is_approval_delivery(&delivery.text, &rendered_reply);
            let author_principal = origin_author_key(delivery).unwrap_or_default();
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
                let _ = store.record_bot_reply_event(&BotReplyEventRecord {
                    reply_message_id: delivery.message_id.clone(),
                    room_id: forward.room_id.clone(),
                    event_id: sent_ev_id.clone(),
                    thread_root_id: forward.thread_root_id.clone(),
                    author_principal,
                    answered_mxid: forward.sender_mxid.clone(),
                    created_at: now_secs,
                });
                if is_approval {
                    let _ = store.record_approval_request(
                        &sent_ev_id,
                        &forward.thread_root_id,
                        now_secs,
                    );
                }
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
                let _ = store.record_bot_reply_event(&BotReplyEventRecord {
                    reply_message_id: delivery.message_id.clone(),
                    room_id: forward.room_id.clone(),
                    event_id: sent_ev_id.clone(),
                    thread_root_id: forward.thread_root_id.clone(),
                    author_principal,
                    answered_mxid: forward.sender_mxid.clone(),
                    created_at: now_secs,
                });
                if is_approval {
                    let _ = store.record_approval_request(
                        &sent_ev_id,
                        &forward.thread_root_id,
                        now_secs,
                    );
                }
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

struct ChannelSvcInbox {
    ack_tx: tokio::sync::mpsc::UnboundedSender<(
        String,
        tokio::sync::oneshot::Sender<Result<(), AppError>>,
    )>,
    node_name: Option<String>,
}

#[async_trait::async_trait]
impl SvcInbox for ChannelSvcInbox {
    async fn poll(&mut self, _wait_secs: u64) -> Result<Option<SvcDelivery>, AppError> {
        Ok(None)
    }

    async fn ack(&mut self, message_id: &str) -> Result<(), AppError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.ack_tx
            .send((message_id.to_string(), tx))
            .map_err(|_| AppError::Xmsg("Inbox loop closed before ack".to_string()))?;
        rx.await
            .map_err(|_| AppError::Xmsg("Inbox loop dropped ack responder".to_string()))?
    }

    fn node(&self) -> Option<String> {
        self.node_name.clone()
    }
}

fn delivery_thread_key(delivery: &SvcDelivery, store: &Store) -> String {
    if let Some(orig_id) = delivery
        .in_reply_to
        .as_deref()
        .or_else(|| parse_in_reply_to_id(&delivery.envelope))
        .or_else(|| parse_in_reply_to_id(&delivery.text))
    {
        if let Ok(Some(fwd)) = store.get_forwarded_message(orig_id) {
            return format!("thread:{}:{}", fwd.room_id, fwd.thread_root_id);
        }
        return format!("reply:{orig_id}");
    }

    if let Ok(val) = serde_json::from_str::<serde_json::Value>(delivery.text.trim()) {
        if let Some(obj) = val.as_object() {
            let extract_target =
                |m: &serde_json::Map<String, serde_json::Value>| -> Option<String> {
                    m.get("redact")
                        .or_else(|| m.get("edit"))
                        .and_then(|v| v.as_str())
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(String::from)
                };
            let target_opt = extract_target(obj).or_else(|| {
                obj.get("text")
                    .and_then(|v| v.as_str())
                    .and_then(|s| serde_json::from_str::<serde_json::Value>(s.trim()).ok())
                    .and_then(|inner| inner.as_object().and_then(extract_target))
            });
            if let Some(target_id) = target_opt {
                if let Ok(Some(rec)) = store.get_bot_reply_event(&target_id) {
                    return format!("thread:{}:{}", rec.room_id, rec.thread_root_id);
                }
            }
        }
    }

    format!("msg:{}", delivery.message_id)
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
    // Resolve configured rooms right after start before serving the inbox
    if let Err(e) = matrix.resolve_rooms(&config.rooms).await {
        tracing::warn!("Failed resolving configured rooms at startup: {e}");
    }

    let (ack_tx, mut ack_rx) = tokio::sync::mpsc::unbounded_channel::<(
        String,
        tokio::sync::oneshot::Sender<Result<(), AppError>>,
    )>();
    let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let (node_tx, node_rx) = tokio::sync::watch::channel(inbox.node());
    let (worker_shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(16));
    let mut in_flight_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut duplicate_polled = false;
    let mut thread_queues: std::collections::HashMap<
        String,
        tokio::sync::mpsc::UnboundedSender<SvcDelivery>,
    > = std::collections::HashMap::new();

    loop {
        while let Ok((ack_id, reply_tx)) = ack_rx.try_recv() {
            let res = inbox.ack(&ack_id).await;
            let _ = reply_tx.send(res);
        }
        while let Ok(done_id) = done_rx.try_recv() {
            in_flight_ids.remove(&done_id);
            duplicate_polled = false;
        }

        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        if let Err(e) =
            sweep_answer_timeouts(&config, matrix.as_ref(), store.as_ref(), now_secs).await
        {
            tracing::warn!("Failed sweeping answer timeouts: {e}");
        }

        if duplicate_polled && !in_flight_ids.is_empty() {
            tokio::select! {
                biased;
                _ = shutdown_rx.recv() => {
                    tracing::info!("Inbox loop received shutdown signal");
                    let _ = worker_shutdown_tx.send(());
                    break;
                }
                Some((ack_id, reply_tx)) = ack_rx.recv() => {
                    let res = inbox.ack(&ack_id).await;
                    let _ = reply_tx.send(res);
                    continue;
                }
                Some(done_id) = done_rx.recv() => {
                    in_flight_ids.remove(&done_id);
                    duplicate_polled = false;
                    continue;
                }
            }
        }

        let wait_secs = if in_flight_ids.is_empty() { 5 } else { 0 };

        tokio::select! {
            biased;
            _ = shutdown_rx.recv() => {
                tracing::info!("Inbox loop received shutdown signal");
                let _ = worker_shutdown_tx.send(());
                break;
            }
            poll_res = inbox.poll(wait_secs) => {
                let _ = node_tx.send(inbox.node());
                match poll_res {
                    Ok(Some(delivery)) => {
                        // Verdict routing never waits behind a handler (M38 §1): route guard
                        // replies on the inbox loop and ack them before dispatching handlers.
                        let reply_to_candidate = delivery
                            .in_reply_to
                            .as_deref()
                            .or_else(|| parse_in_reply_to_id(&delivery.envelope));
                        if let Some(in_reply_to) = reply_to_candidate {
                            if route_guard_reply(in_reply_to, &delivery.text).await {
                                if let Err(e) = inbox.ack(&delivery.message_id).await {
                                    tracing::warn!(
                                        "Failed to ack guard verdict {}: {e}",
                                        delivery.message_id
                                    );
                                }
                                continue;
                            }
                        }

                        if in_flight_ids.contains(&delivery.message_id) {
                            duplicate_polled = true;
                            continue;
                        }

                        in_flight_ids.insert(delivery.message_id.clone());
                        let thread_key = delivery_thread_key(&delivery, store.as_ref());
                        let mut to_send = Some(delivery);

                        if let Some(tx) = thread_queues.get(&thread_key) {
                            match tx.send(to_send.take().unwrap()) {
                                Ok(()) => {}
                                Err(err) => {
                                    to_send = Some(err.0);
                                    thread_queues.remove(&thread_key);
                                }
                            }
                        }

                        if let Some(first_delivery) = to_send {
                            let (tx, mut rx) =
                                tokio::sync::mpsc::unbounded_channel::<SvcDelivery>();
                            let _ = tx.send(first_delivery);
                            thread_queues.insert(thread_key, tx);

                            let config = config.clone();
                            let matrix = matrix.clone();
                            let store = store.clone();
                            let xmsg = xmsg.clone();
                            let ack_tx = ack_tx.clone();
                            let done_tx = done_tx.clone();
                            let node_rx = node_rx.clone();
                            let sem = sem.clone();
                            let mut worker_shutdown_rx = worker_shutdown_tx.subscribe();

                            tokio::spawn(async move {
                                while let Some(delivery) = rx.recv().await {
                                    let _permit = match sem.clone().acquire_owned().await {
                                        Ok(p) => p,
                                        Err(_) => break,
                                    };

                                    loop {
                                        let now = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap_or_default()
                                            .as_secs() as i64;
                                        let prev_attempts = store
                                            .get_inbox_retries(&delivery.message_id)
                                            .unwrap_or(None)
                                            .map(|(a, _)| a)
                                            .unwrap_or(0);
                                        let mut chan_inbox = ChannelSvcInbox {
                                            ack_tx: ack_tx.clone(),
                                            node_name: node_rx.borrow().clone(),
                                        };

                                        match handle_inbox_delivery_with_xmsg(
                                            &delivery,
                                            &config,
                                            matrix.as_ref(),
                                            store.as_ref(),
                                            xmsg.as_ref(),
                                            &mut chan_inbox,
                                            now,
                                        )
                                        .await
                                        {
                                            Ok(BotOutcome::GuardFailed) => {
                                                let remaining = store
                                                    .get_guard_retries(&delivery.message_id)
                                                    .unwrap_or(None)
                                                    .unwrap_or(0);
                                                if remaining == 0 {
                                                    break;
                                                }
                                                let backoff_secs =
                                                    (1u64 << (remaining.saturating_sub(1)).min(30))
                                                        .min(300);
                                                tokio::select! {
                                                    _ = worker_shutdown_rx.recv() => break,
                                                    _ = tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)) => {}
                                                }
                                            }
                                            Ok(_) => {
                                                if prev_attempts > 0 {
                                                    let _ = store.clear_inbox_retries(&delivery.message_id);
                                                }
                                                break;
                                            }
                                            Err(e) => {
                                                let attempt = prev_attempts + 1;
                                                if attempt >= config.inbox_retry_budget {
                                                    tracing::error!(
                                                        "Delivery {} permanently failed after {} attempts: {e}; giving up",
                                                        delivery.message_id,
                                                        attempt
                                                    );
                                                    let err_reply = serde_json::json!({
                                                        "error": format!("{e}")
                                                    })
                                                    .to_string();
                                                    let _ = xmsg.reply_message(&delivery.message_id, &err_reply).await;
                                                    let dm_text = format!(
                                                        "[matrix-xmsg] Delivery {} permanently failed after {} attempts: {e}",
                                                        delivery.message_id,
                                                        attempt
                                                    );
                                                    let _ = matrix.send_dm(&config.owner_mxid, &dm_text).await;
                                                    if let Err(ack_err) = chan_inbox.ack(&delivery.message_id).await {
                                                        tracing::error!(
                                                            "Failed to ack delivery {} on give-up: {ack_err}",
                                                            delivery.message_id
                                                        );
                                                    }
                                                    let _ = store.clear_inbox_retries(&delivery.message_id);
                                                    break;
                                                } else {
                                                    let _ = store.record_inbox_retry(&delivery.message_id, attempt, now);
                                                    let backoff_secs = (1u64 << (attempt - 1).min(30)).min(300);
                                                    tracing::warn!(
                                                        "Delivery {} attempt {} failed: {e}; retrying in {}s",
                                                        delivery.message_id,
                                                        attempt,
                                                        backoff_secs
                                                    );
                                                    tokio::select! {
                                                        _ = worker_shutdown_rx.recv() => {
                                                            tracing::info!("Inbox loop received shutdown signal during retry backoff");
                                                            break;
                                                        }
                                                        _ = tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)) => {}
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    let _ = done_tx.send(delivery.message_id.clone());
                                }
                            });
                        }

                        tokio::select! {
                            biased;
                            Some((ack_id, reply_tx)) = ack_rx.recv() => {
                                let res = inbox.ack(&ack_id).await;
                                let _ = reply_tx.send(res);
                            }
                            Some(done_id) = done_rx.recv() => {
                                in_flight_ids.remove(&done_id);
                                duplicate_polled = false;
                            }
                            _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
                        }
                    }
                    Ok(None) => {
                        if !in_flight_ids.is_empty() {
                            tokio::select! {
                                biased;
                                _ = shutdown_rx.recv() => {
                                    tracing::info!("Inbox loop received shutdown signal");
                                    let _ = worker_shutdown_tx.send(());
                                    break;
                                }
                                Some((ack_id, reply_tx)) = ack_rx.recv() => {
                                    let res = inbox.ack(&ack_id).await;
                                    let _ = reply_tx.send(res);
                                }
                                Some(done_id) = done_rx.recv() => {
                                    in_flight_ids.remove(&done_id);
                                    duplicate_polled = false;
                                }
                                _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
                            }
                        } else {
                            tokio::task::yield_now().await;
                        }
                    }
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

    // 2. Control Recognition Gate: Only ✅ and 🔍 (and 🔎) and 🔓 are controls
    let Some(control) = parse_reaction_control(&reaction.key) else {
        return Ok(BotOutcome::IgnoredNoMention);
    };

    if control == ControlAction::Approve {
        // Must be a reaction on an approval request message posted by the bot
        let is_approval = store.is_approval_request(&reaction.relates_to_event_id)?;
        if !is_approval {
            return Ok(BotOutcome::IgnoredNoMention);
        }

        // Accepted ONLY from a trusted sender; from anyone else it's dropped and never leaves the bot
        let is_trusted = config.is_user_trusted(&reaction.sender_mxid);
        if !is_trusted {
            return Ok(BotOutcome::IgnoredUntrustedUser);
        }

        let thread_id = store
            .get_approval_request_thread(&reaction.relates_to_event_id)?
            .or_else(|| {
                store
                    .get_bot_message_thread(&reaction.relates_to_event_id)
                    .ok()
                    .flatten()
            })
            .unwrap_or_else(|| reaction.relates_to_event_id.clone());

        let now_secs = reaction.timestamp_ms / 1000;
        let is_new = store.record_control_if_new(
            &reaction.relates_to_event_id,
            &reaction.sender_mxid,
            "approve",
            now_secs,
        )?;

        if !is_new {
            return Ok(BotOutcome::ControlDebounced);
        }

        let mapped_sender = map_sender_mxid(&reaction.sender_mxid);
        let payload = serde_json::json!({
            "thread_id": thread_id,
            "control": "approve",
            "sender": mapped_sender,
            "sender_id": reaction.sender_mxid,
            "tier": "trusted",
            "target": reaction.relates_to_event_id,
        })
        .to_string();

        let target_ref = control_target_ref(
            config,
            store,
            &reaction.room_id,
            &thread_id,
            &reaction.sender_mxid,
        )?;
        xmsg.send_message(&target_ref, &mapped_sender, &payload)
            .await?;

        return Ok(BotOutcome::ControlEmitted);
    }

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
    let target_ref = control_target_ref(
        config,
        store,
        &reaction.room_id,
        &thread_id,
        &reaction.sender_mxid,
    )?;
    xmsg.send_message(&target_ref, &mapped_sender, &payload)
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

fn poll_sync_token_once(client: &matrix_sdk::Client) -> bool {
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let mut fut = std::pin::pin!(client
        .state_store()
        .get_kv_data(matrix_sdk::store::StateStoreDataKey::SyncToken));
    matches!(
        std::future::Future::poll(fut.as_mut(), &mut cx),
        std::task::Poll::Ready(Ok(Some(matrix_sdk::store::StateStoreDataValue::SyncToken(
            _
        ))))
    )
}

async fn has_sdk_persisted_sync_token(
    client: &matrix_sdk::Client,
    initial_has_token: bool,
    first_sync_batch: &std::sync::Mutex<Option<String>>,
) -> bool {
    let current_token = match client
        .state_store()
        .get_kv_data(matrix_sdk::store::StateStoreDataKey::SyncToken)
        .await
    {
        Ok(Some(matrix_sdk::store::StateStoreDataValue::SyncToken(t))) => Some(t),
        _ => None,
    };
    if initial_has_token {
        return current_token.is_some();
    }
    let Some(tok) = current_token else {
        return false;
    };
    let mut guard = first_sync_batch.lock().unwrap();
    match guard.as_ref() {
        None => {
            *guard = Some(tok);
            false
        }
        Some(first) => first != &tok,
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_incoming_room_message(
    event: &matrix_sdk::ruma::events::room::message::SyncRoomMessageEvent,
    room: &matrix_sdk::room::Room,
    config: std::sync::Arc<Config>,
    matrix: std::sync::Arc<dyn MatrixClient>,
    xmsg: std::sync::Arc<dyn XmsgClient>,
    store: std::sync::Arc<Store>,
    startup_ts: i64,
    initial_has_token: bool,
    first_sync_batch: std::sync::Arc<std::sync::Mutex<Option<String>>>,
    sem: std::sync::Arc<tokio::sync::Semaphore>,
    tracker: TaskTracker,
) {
    if let Some(incoming) = extract_incoming_event(event, room.room_id()) {
        // R5: Backlog suppression: primary guard is the persisted sync token in the SDK state store (§4.7).
        // On initial token-less sync, no skew tolerance is granted: events before startup_ts are backlog.
        // On resumed sync with token, a 10s clock skew tolerance is allowed.
        let has_persisted_token =
            has_sdk_persisted_sync_token(&room.client(), initial_has_token, &first_sync_batch)
                .await;
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
        let is_thread_trust = incoming.thread_root_id.is_some() && incoming.body.trim() == "!trust";
        let is_approve_command = incoming.body.trim() == "!approve";
        let is_mentioned = is_bot_mentioned(
            &incoming.body,
            incoming.formatted_body.as_deref(),
            incoming.mentions.as_deref(),
            &config.bot_mxid,
        );
        let is_reply_to_bot = if let Some(ref in_reply_to) = incoming.in_reply_to_event_id {
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
            && !is_thread_trust
            && !is_approve_command
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
        let is_promoted_thread = incoming
            .thread_root_id
            .as_deref()
            .and_then(|r| store.is_thread_promoted(r).ok())
            .unwrap_or(false);
        let is_admitted = match config.admission {
            Admission::Trusted => is_trusted || is_promoted_thread,
            Admission::Public => {
                is_trusted
                    || is_top_level
                    || is_promoted_thread
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

                let mut retry_delay = std::time::Duration::from_millis(200);
                loop {
                    match handle_incoming_event_with_claim(
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
                        Ok(BotOutcome::GuardFailed) => {
                            if store
                                .has_guard_failure_dm_sent(&incoming.event_id)
                                .unwrap_or(true)
                            {
                                break;
                            }
                            tokio::time::sleep(retry_delay).await;
                            retry_delay = (retry_delay * 2).min(std::time::Duration::from_secs(5));
                        }
                        Ok(_) => break,
                        Err(e) => {
                            tracing::error!(
                                "Error handling incoming event {}: {e}",
                                incoming.event_id
                            );
                            break;
                        }
                    }
                }
            })
            .await;
    }
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

    let initial_has_token = matrix
        .had_initial_sync_token()
        .unwrap_or_else(|| poll_sync_token_once(client));
    let first_sync_batch: std::sync::Arc<std::sync::Mutex<Option<String>>> =
        std::sync::Arc::new(std::sync::Mutex::new(None));

    let config_msg = config.clone();
    let matrix_msg = matrix.clone();
    let xmsg_msg = xmsg.clone();
    let store_msg = store.clone();
    let sem_msg = semaphore.clone();
    let tracker_msg = tracker.clone();
    let first_sync_msg = first_sync_batch.clone();

    client.add_event_handler(
        move |event: matrix_sdk::ruma::events::room::message::SyncRoomMessageEvent,
              room: matrix_sdk::room::Room| {
            let config = config_msg.clone();
            let matrix = matrix_msg.clone();
            let xmsg = xmsg_msg.clone();
            let store = store_msg.clone();
            let sem = sem_msg.clone();
            let tracker = tracker_msg.clone();
            let first_sync_batch = first_sync_msg.clone();

            async move {
                process_incoming_room_message(
                    &event,
                    &room,
                    config,
                    matrix,
                    xmsg,
                    store,
                    startup_ts,
                    initial_has_token,
                    first_sync_batch,
                    sem,
                    tracker,
                )
                .await;
            }
        },
    );

    // §5.1 & §5.2: Undecryptable event handler — spawns late-key wait off the sync loop,
    // retries decryption when room keys arrive, and otherwise logs WARN + reacts with 🔒 on reply/engaged thread.
    let config_enc = config.clone();
    let matrix_enc = matrix.clone();
    let xmsg_enc = xmsg.clone();
    let store_enc = store.clone();
    let sem_enc = semaphore.clone();
    let tracker_enc = tracker.clone();
    let first_sync_enc = first_sync_batch.clone();

    client.add_event_handler(
        move |event: matrix_sdk::ruma::events::room::encrypted::OriginalSyncRoomEncryptedEvent,
              room: matrix_sdk::room::Room,
              raw_event: matrix_sdk::event_handler::RawEvent| {
            let config = config_enc.clone();
            let matrix = matrix_enc.clone();
            let xmsg = xmsg_enc.clone();
            let store = store_enc.clone();
            let sem = sem_enc.clone();
            let tracker = tracker_enc.clone();
            let first_sync_batch = first_sync_enc.clone();
            let raw_event = matrix_sdk::ruma::serde::Raw::<
                matrix_sdk::ruma::events::room::encrypted::OriginalSyncRoomEncryptedEvent,
            >::from_json(raw_event.0);

            async move {
                // Spawn off the sync loop so later syncs can deliver the late room key (§5.1, gate C5).
                tokio::spawn(async move {
                    use futures_util::StreamExt;
                    use matrix_sdk::ruma::events::{AnySyncMessageLikeEvent, AnySyncTimelineEvent};

                    let keys_stream = room.client().encryption().room_keys_received_stream().await;

                    // Try once immediately (also triggers BackupDownloadStrategy::AfterDecryptionFailure if applicable)
                    if let Ok(decrypted) = room.decrypt_event(&raw_event, None).await {
                        if let Ok(AnySyncTimelineEvent::MessageLike(
                            AnySyncMessageLikeEvent::RoomMessage(sync_msg),
                        )) = decrypted.raw().deserialize()
                        {
                            process_incoming_room_message(
                                &sync_msg,
                                &room,
                                config,
                                matrix,
                                xmsg,
                                store,
                                startup_ts,
                                initial_has_token,
                                first_sync_batch,
                                sem,
                                tracker,
                            )
                            .await;
                            return;
                        }
                    }

                    let wait_dur = tokio::time::Duration::from_secs(config.undecryptable_wait_secs);
                    if wait_dur > tokio::time::Duration::ZERO {
                        if let Some(stream) = keys_stream {
                            let mut stream = std::pin::pin!(stream);
                            let deadline = tokio::time::Instant::now() + wait_dur;
                            while let Ok(Some(_)) =
                                tokio::time::timeout_at(deadline, stream.next()).await
                            {
                                if let Ok(decrypted) = room.decrypt_event(&raw_event, None).await {
                                    if let Ok(AnySyncTimelineEvent::MessageLike(
                                        AnySyncMessageLikeEvent::RoomMessage(sync_msg),
                                    )) = decrypted.raw().deserialize()
                                    {
                                        process_incoming_room_message(
                                            &sync_msg,
                                            &room,
                                            config,
                                            matrix,
                                            xmsg,
                                            store,
                                            startup_ts,
                                            initial_has_token,
                                            first_sync_batch,
                                            sem,
                                            tracker,
                                        )
                                        .await;
                                        return;
                                    }
                                }
                            }
                        }
                    }

                    // §5.2: Timed out / still undecryptable — log loudly as WARN with room, event ID, and sender.
                    let room_id_str = room.room_id().to_string();
                    let event_id_str = event.event_id.to_string();
                    let sender_str = event.sender.to_string();
                    tracing::warn!(
                        room_id = %room_id_str,
                        event_id = %event_id_str,
                        sender = %sender_str,
                        "Undecryptable event {} in room {} from {}; not forwarding",
                        event_id_str,
                        room_id_str,
                        sender_str
                    );

                    // Check R5 cutoff, self-sender, and room allowlist before considering 🔒 reaction
                    let has_persisted_token = has_sdk_persisted_sync_token(
                        &room.client(),
                        initial_has_token,
                        &first_sync_batch,
                    )
                    .await;
                    let cutoff_ts = if has_persisted_token {
                        startup_ts - 10_000
                    } else {
                        startup_ts
                    };
                    let ts_ms = u64::from(event.origin_server_ts.0) as i64;
                    if ts_ms < cutoff_ts
                        || sender_str == config.bot_mxid
                        || !config.is_room_allowlisted(&room_id_str)
                    {
                        return;
                    }

                    use matrix_sdk::ruma::events::room::encrypted::Relation as EncryptedRelation;
                    let (thread_root_id, in_reply_to_id, is_falling_back) =
                        match &event.content.relates_to {
                            Some(EncryptedRelation::Thread(t)) => (
                                Some(t.event_id.to_string()),
                                t.in_reply_to.as_ref().map(|r| r.event_id.to_string()),
                                t.is_falling_back,
                            ),
                            Some(EncryptedRelation::Reply(rep)) => {
                                (None, Some(rep.in_reply_to.event_id.to_string()), false)
                            }
                            _ => (None, None, false),
                        };

                    let is_reply_to_bot = if let Some(ref reply_id) = in_reply_to_id {
                        if !is_falling_back {
                            store
                                .get_bot_message_thread(reply_id)
                                .ok()
                                .flatten()
                                .is_some()
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    let is_engaged_thread = if let Some(ref root) = thread_root_id {
                        store.is_thread_engaged(root).unwrap_or(false)
                    } else {
                        false
                    };

                    if is_reply_to_bot || is_engaged_thread {
                        if let Err(e) = matrix
                            .send_reaction(&room_id_str, &event_id_str, "🔒")
                            .await
                        {
                            tracing::warn!(
                                room_id = %room_id_str,
                                event_id = %event_id_str,
                                "Failed to send 🔒 reaction to undecryptable event: {e}"
                            );
                        }
                    }
                });
            }
        },
    );

    let config_react = config.clone();
    let xmsg_react = xmsg.clone();
    let store_react = store.clone();
    let first_sync_react = first_sync_batch;
    client.add_event_handler(
        move |event: matrix_sdk::ruma::events::reaction::SyncReactionEvent,
              room: matrix_sdk::room::Room| {
            let config = config_react.clone();
            let xmsg = xmsg_react.clone();
            let store = store_react.clone();
            let first_sync_batch = first_sync_react.clone();
            async move {
                if let Some(reaction) = extract_incoming_reaction(&event, room.room_id()) {
                    let has_persisted_token = has_sdk_persisted_sync_token(
                        &room.client(),
                        initial_has_token,
                        &first_sync_batch,
                    )
                    .await;
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
    _store: std::sync::Arc<Store>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), AppError> {
    use matrix_sdk::config::SyncSettings;
    let mut sync_settings = SyncSettings::default();

    // §4.7: One sync token — the SDK state store governs sync.
    if let Ok(Some(matrix_sdk::store::StateStoreDataValue::SyncToken(saved_token))) = client
        .state_store()
        .get_kv_data(matrix_sdk::store::StateStoreDataKey::SyncToken)
        .await
    {
        tracing::info!("Resuming sync from SDK state store token: {saved_token}");
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
