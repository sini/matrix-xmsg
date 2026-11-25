#![allow(dead_code)]
use matrix_xmsg::xmsg::{ReplyOutcome, ReplyRecord, SendResponse};

pub fn pushed(response: SendResponse) -> ReplyOutcome {
    let record = ReplyRecord {
        seq: 0,
        message_id: "fixture-request".into(),
        created_at: 1,
        replier_session_id: "matrix-xmsg".into(),
        replier: None,
        text: "fixture reply".into(),
        attachments: Vec::new(),
        push_outcome: response.outcome.clone().or_else(|| Some("queued".into())),
        push_error: None,
        pushed_message_id: Some(response.message_id.clone()),
    };
    ReplyOutcome::Pushed { response, record }
}
