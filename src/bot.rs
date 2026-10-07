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
