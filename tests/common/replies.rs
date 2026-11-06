#![allow(dead_code)]
use matrix_xmsg::xmsg::{ReplyOutcome, ReplyRecord, SendResponse};

pub fn pushed(response: SendResponse) -> ReplyOutcome {
    let record = ReplyRecord {
        seq: 0,
        message_id: "fixture-request".into(),
        created_at: 1,
        replier_session_id: "matrix-xmsg".into(),
        text: "fixture reply".into(),
        push_outcome: response.outcome.clone().or_else(|| Some("queued".into())),
        pushed_message_id: Some(response.message_id.clone()),
    };
    ReplyOutcome::Pushed { response, record }
}
