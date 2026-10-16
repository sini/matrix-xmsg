use async_trait::async_trait;
use matrix_xmsg::bot::{handle_incoming_event, sweep_answer_timeouts, IncomingMatrixEvent};
use matrix_xmsg::config::Config;
use matrix_xmsg::error::AppError;
use matrix_xmsg::matrix::MockMatrixClient;
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{SendResponse, XmsgClient};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct MockXmsg {
    sent: Arc<Mutex<Vec<(String, String, String)>>>,
    counter: AtomicUsize,
}

#[async_trait]
impl XmsgClient for MockXmsg {
    async fn send_message(
        &self,
        expert_ref: &str,
        from: &str,
        text: &str,
    ) -> Result<SendResponse, AppError> {
        self.sent.lock().unwrap().push((
            expert_ref.to_string(),
            from.to_string(),
            text.to_string(),
        ));
        let idx = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(SendResponse::new(
            format!("01M29MSG{idx:06}"),
            Some(format!("sess:{expert_ref}")),
        ))
    }

    async fn reply_message(
        &self,
        _message_id: &str,
        _text: &str,
    ) -> Result<SendResponse, AppError> {
        let idx = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(SendResponse::new(
            format!("01M29REPLY{idx:06}"),
            Some("sess:reply".to_string()),
        ))
    }
}

/// Oracle 5 (M29):
/// Default `answer_timeout_secs` is 600s.
/// An unanswered addressed forward at `t + 599` sends 0 owner DMs;
/// at `t + 600` sends 1 owner DM.
/// Mutant: default 300s sends 1 owner DM at `t + 599` => RED.
#[tokio::test]
async fn oracle_5_default_answer_timeout_600s_boundary_and_mutant() {
    let toml_str = r#"
        homeserver_url = "https://matrix.example.org"
        bot_mxid = "@genie:example.org"
        access_token_file = "/tmp/token"
        rooms = ["!support:example.org"]
        trusted_mxids = ["@alice:example.org"]
        owner_mxid = "@owner:example.org"
        screen_mentions = false
        xmsg_socket = "/tmp/xmsg"
    "#;
    let config: Config = toml::from_str(toml_str).expect("default config must parse");
    assert_eq!(
        config.answer_timeout_secs, 600,
        "default answer_timeout_secs must be 600"
    );

    let xmsg = MockXmsg::default();
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();

    // Forward an addressed question at t = 1000s (timestamp_ms = 1_000_000)
    let t0 = 1000i64;
    let event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_m29_ask".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie Question testing 600s default answer timeout".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: t0 * 1000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    handle_incoming_event(&event, &[], &config, &matrix, &xmsg, &store)
        .await
        .expect("handle_incoming_event must succeed");

    // At t0 + 599 (599s elapsed < 600s): 0 owner DMs
    let swept_599 = sweep_answer_timeouts(&config, &matrix, &store, t0 + 599)
        .await
        .unwrap();
    assert_eq!(swept_599, 0, "at t+599s, 0 messages must time out");
    assert_eq!(
        matrix.sent_dms.lock().unwrap().len(),
        0,
        "at t+599s, 0 owner DMs must be sent"
    );

    // At t0 + 600 (600s elapsed == 600s): 1 owner DM
    let swept_600 = sweep_answer_timeouts(&config, &matrix, &store, t0 + 600)
        .await
        .unwrap();
    assert_eq!(swept_600, 1, "at t+600s, 1 message must time out");
    {
        let dms = matrix.sent_dms.lock().unwrap();
        assert_eq!(dms.len(), 1, "at t+600s, 1 owner DM must be sent");
        assert_eq!(dms[0].user_id, "@owner:example.org");
        assert!(dms[0].body.contains("Answer timeout"));
    }

    // Mutant check: if default were still 300, then at t0 + 599 it would have sent 1 owner DM (RED against 0 expected).
    let mut mutant_config = config.clone();
    mutant_config.answer_timeout_secs = 300;
    let mutant_matrix = MockMatrixClient::default();
    let mutant_store = Store::new_in_memory().unwrap();
    handle_incoming_event(&event, &[], &mutant_config, &mutant_matrix, &xmsg, &mutant_store)
        .await
        .unwrap();
    let mutant_swept_599 =
        sweep_answer_timeouts(&mutant_config, &mutant_matrix, &mutant_store, t0 + 599)
            .await
            .unwrap();
    assert_eq!(
        mutant_swept_599, 1,
        "mutant with 300s default fires prematurely at t+599s"
    );
    assert_eq!(mutant_matrix.sent_dms.lock().unwrap().len(), 1);
}
