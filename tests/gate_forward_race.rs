//! An expert reply pushed the moment its forward is sent is posted, never dropped as a reply to
//! an unknown forward.
//!
//! The mock xmsg hands the inbox the reply to each send before the send returns, and the mock
//! Matrix reaction takes a round-trip, so the reply can be routed anywhere in the window from
//! the send to the ack reaction. Both run on one thread, so the interleaving is the same on
//! every run: the reply task runs at each yield, and it either finishes or waits on the
//! forward gate.

use async_trait::async_trait;
use matrix_xmsg::bot::{
    handle_inbox_delivery_with_xmsg, handle_incoming_event_with_guard, BotOutcome,
    IncomingMatrixEvent,
};
use matrix_xmsg::config::{Admission, Config};
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::{MatrixClient, MockMatrixClient};
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{MockGuardClient, SendResponse, SvcDelivery, SvcInbox, XmsgClient};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

const ROOM: &str = "!support:example.org";
const REPLY: &str = "pong from expert";

/// Enough yields for the reply task to run to completion when nothing holds it.
async fn round_trip() {
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
}

fn config() -> Config {
    Config {
        homeserver_url: "https://matrix.example.org".to_string(),
        bot_mxid: "@genie:example.org".to_string(),
        access_token_file: PathBuf::from("/nonexistent/token"),
        rooms: vec![ROOM.to_string()],
        trusted_mxids: vec!["@alice:example.org".to_string()],
        owner_mxid: "@owner:example.org".to_string(),
        admission: Admission::Trusted,
        xmsg_socket: PathBuf::from("/nonexistent/xmsg"),
        expert_ref: "claude".to_string(),
        history_n: 5,
        history_byte_cap: 1024,
        rate_limit_count: 10,
        rate_limit_window_secs: 60,
        size_cap_bytes: 1024,
        answer_timeout_secs: 10,
        session_live_secs: 3600,
        resync_byte_cap: 65536,
        guard_ref: "genie-guard".to_string(),
        guard_timeout_secs: 30,
        guard_retry_budget: 3,
        inbox_retry_budget: 10,
        db_path: PathBuf::from(":memory:"),
    }
}

/// `MockMatrixClient` whose reaction is a round-trip.
#[derive(Default)]
struct SlowReactions(MockMatrixClient);

#[async_trait]
impl MatrixClient for SlowReactions {
    async fn send_notice(
        &self,
        room_id: &str,
        thread_root_id: Option<&str>,
        body: &str,
        mention_user: Option<&str>,
    ) -> Result<String, AppError> {
        self.0
            .send_notice(room_id, thread_root_id, body, mention_user)
            .await
    }
    async fn send_dm(&self, user_id: &str, body: &str) -> Result<String, AppError> {
        self.0.send_dm(user_id, body).await
    }
    async fn fetch_history(
        &self,
        room_id: &str,
        limit: usize,
    ) -> Result<Vec<matrix_xmsg::context::EventMessage>, AppError> {
        self.0.fetch_history(room_id, limit).await
    }
    async fn send_reaction(
        &self,
        room_id: &str,
        event_id: &str,
        key: &str,
    ) -> Result<String, AppError> {
        round_trip().await;
        self.0.send_reaction(room_id, event_id, key).await
    }
    async fn redact_event(
        &self,
        room_id: &str,
        event_id: &str,
        reason: Option<&str>,
    ) -> Result<(), AppError> {
        self.0.redact_event(room_id, event_id, reason).await
    }
}

#[derive(Default)]
struct AckInbox(Vec<String>);

#[async_trait]
impl SvcInbox for AckInbox {
    async fn poll(&mut self, _wait_secs: u64) -> Result<Option<SvcDelivery>, AppError> {
        Ok(None)
    }
    async fn ack(&mut self, message_id: &str) -> Result<(), AppError> {
        self.0.push(message_id.to_string());
        Ok(())
    }
}

type Routed = JoinHandle<Result<(BotOutcome, Vec<String>), AppError>>;

/// An xmsg whose expert answers every send at once: the reply is in the inbox, and being
/// routed, before the send returns.
struct InstantExpert {
    config: Config,
    matrix: Arc<SlowReactions>,
    store: Arc<Store>,
    routed: Mutex<Vec<Routed>>,
}

impl InstantExpert {
    async fn answer_at_once(&self, message_id: String) -> Result<SendResponse, AppError> {
        let (config, matrix, store) =
            (self.config.clone(), self.matrix.clone(), self.store.clone());
        let mut reply = SvcDelivery::new(
            format!("{message_id}-REPLY"),
            "expert · sess@sini.test",
            REPLY,
            "",
            None,
        );
        reply.in_reply_to = Some(message_id.clone());
        self.routed.lock().unwrap().push(tokio::spawn(async move {
            let mut inbox = AckInbox::default();
            let outcome = handle_inbox_delivery_with_xmsg(
                &reply,
                &config,
                matrix.as_ref(),
                &store,
                &NoXmsg,
                &mut inbox,
                2000,
            )
            .await?;
            Ok((outcome, inbox.0))
        }));
        round_trip().await;
        Ok(SendResponse::new(message_id, Some("sess".to_string())))
    }
}

#[async_trait]
impl XmsgClient for InstantExpert {
    async fn send_message(
        &self,
        _expert_ref: &str,
        _from: &str,
        _text: &str,
    ) -> Result<SendResponse, AppError> {
        self.answer_at_once("01RACEFORWARD0001".to_string()).await
    }
    async fn reply_message(
        &self,
        _message_id: &str,
        _text: &str,
    ) -> Result<SendResponse, AppError> {
        self.answer_at_once("01RACERELAY00001".to_string()).await
    }
}

/// The reply path's xmsg, which a plain expert reply never calls.
struct NoXmsg;

#[async_trait]
impl XmsgClient for NoXmsg {
    async fn send_message(&self, _: &str, _: &str, _: &str) -> Result<SendResponse, AppError> {
        Err(AppError::Xmsg("unexpected send".to_string()))
    }
}

fn mention(root: &str) -> IncomingMatrixEvent {
    IncomingMatrixEvent {
        room_id: ROOM.to_string(),
        event_id: "$ev_1".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie ping".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 1_000_000,
        thread_root_id: Some(root.to_string()),
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    }
}

/// Forwards one mention in `root` and returns the instant reply's outcome and acks.
async fn forward_and_route(root: &str, store: Arc<Store>) -> (Arc<SlowReactions>, BotOutcome) {
    let config = config();
    let matrix = Arc::new(SlowReactions::default());
    let xmsg = InstantExpert {
        config: config.clone(),
        matrix: matrix.clone(),
        store: store.clone(),
        routed: Mutex::new(Vec::new()),
    };
    let outcome = handle_incoming_event_with_guard(
        &mention(root),
        &[],
        &config,
        matrix.as_ref(),
        &xmsg,
        &MockGuardClient::new(),
        &store,
        None,
    )
    .await
    .expect("forward");
    assert_eq!(outcome, BotOutcome::Forwarded);

    let mut routed = std::mem::take(&mut *xmsg.routed.lock().unwrap());
    assert_eq!(routed.len(), 1, "one send, one instant reply");
    let (reply_outcome, acks) = routed.pop().unwrap().await.unwrap().expect("reply routing");
    assert_eq!(
        reply_outcome,
        BotOutcome::Replied,
        "the instant reply was not posted"
    );
    assert_eq!(acks.len(), 1, "the reply is acked once, after its post");
    (matrix, reply_outcome)
}

fn posted_replies(matrix: &SlowReactions, root: &str) -> usize {
    let notices = matrix.0.sent_notices.lock().unwrap();
    notices
        .iter()
        .filter(|n| n.thread_root_id.as_deref() == Some(root) && n.body.contains(REPLY))
        .count()
}

// An expert reply delivered before the forward's send has returned is posted to the thread
// exactly once. Mutants: record the forward after the ack reaction, or route without taking
// the forward gate => RED (IgnoredUnknownMessage).
#[tokio::test]
async fn an_instant_expert_reply_is_posted_once() {
    let store = Arc::new(Store::new_in_memory().unwrap());
    let (matrix, _) = forward_and_route("$root_expert", store.clone()).await;
    assert_eq!(posted_replies(&matrix, "$root_expert"), 1);

    // The reaction, sent after the record, is still recorded on the forward.
    let forward = store
        .get_forwarded_message("01RACEFORWARD0001")
        .unwrap()
        .unwrap();
    assert!(
        forward.ack_reaction_id.is_some(),
        "ack reaction not recorded"
    );
}

// The same for a relay into a solicited thread, whose forward is the pushed reply to the
// session's request. Mutant: record the relayed forward after the reaction => RED.
#[tokio::test]
async fn an_instant_reply_to_a_relay_is_posted_once() {
    let store = Arc::new(Store::new_in_memory().unwrap());
    store
        .bind_solicited_thread("$root_solicited", ROOM, "01RACEREQUEST0001", 900)
        .unwrap();
    let (matrix, _) = forward_and_route("$root_solicited", store.clone()).await;
    assert_eq!(posted_replies(&matrix, "$root_solicited"), 1);
    assert!(
        store
            .get_forwarded_message("01RACERELAY00001")
            .unwrap()
            .is_some(),
        "the forward is not the relay's"
    );
}
