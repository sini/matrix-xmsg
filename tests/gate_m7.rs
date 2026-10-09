use matrix_xmsg::bot::{
    handle_inbox_delivery, handle_incoming_event, BotOutcome, IncomingMatrixEvent,
};
use matrix_xmsg::config::{Admission, Config};
use matrix_xmsg::matrix::MockMatrixClient;
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::{create_xmsg_client, register_svc, SvcInbox, UnixXmsgClient};
use serde_json::Value;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::Mutex;

pub struct MockXmsgSockets {
    pub dir: PathBuf,
    pub recorded_sends: Arc<Mutex<Vec<Value>>>,
    pub recorded_acks: Arc<Mutex<Vec<String>>>,
    pub queued_deliveries: Arc<Mutex<VecDeque<Value>>>,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl MockXmsgSockets {
    pub async fn start(dir: &Path) -> Self {
        let register_sock_path = dir.join("register.sock");
        let agent_sock_path = dir.join("agent.sock");
        let _ = std::fs::remove_file(&register_sock_path);
        let _ = std::fs::remove_file(&agent_sock_path);

        let reg_listener = UnixListener::bind(&register_sock_path).expect("bind register.sock");
        let agent_listener = UnixListener::bind(&agent_sock_path).expect("bind agent.sock");

        let recorded_sends = Arc::new(Mutex::new(Vec::new()));
        let recorded_acks = Arc::new(Mutex::new(Vec::new()));
        let queued_deliveries = Arc::new(Mutex::new(VecDeque::new()));

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let sends_clone = Arc::clone(&recorded_sends);
        let acks_clone = Arc::clone(&recorded_acks);
        let deliveries_clone = Arc::clone(&queued_deliveries);

        tokio::spawn(async move {
            tokio::select! {
                _ = &mut shutdown_rx => {}
                _ = async {
                    let agent_task = {
                        let sends = Arc::clone(&sends_clone);
                        tokio::spawn(async move {
                            while let Ok((stream, _)) = agent_listener.accept().await {
                                let (r, mut w) = stream.into_split();
                                let mut lines = tokio::io::BufReader::new(r).lines();
                                let sends = Arc::clone(&sends);
                                tokio::spawn(async move {
                                    while let Ok(Some(line)) = lines.next_line().await {
                                        if let Ok(val) = serde_json::from_str::<Value>(&line) {
                                            sends.lock().await.push(val.clone());
                                            let action = val.get("action").and_then(|a| a.as_str()).unwrap_or("");
                                            if action == "send" {
                                                let resp = serde_json::json!({
                                                    "status": "ok",
                                                    "message_id": "01M7MSGTEST0000000000000000",
                                                });
                                                let _ = w.write_all(format!("{resp}\n").as_bytes()).await;
                                                let _ = w.flush().await;
                                            }
                                        }
                                    }
                                });
                            }
                        })
                    };

                    let reg_task = {
                        let acks = Arc::clone(&acks_clone);
                        let deliveries = Arc::clone(&deliveries_clone);
                        tokio::spawn(async move {
                            while let Ok((stream, _)) = reg_listener.accept().await {
                                let (r, mut w) = stream.into_split();
                                let mut lines = tokio::io::BufReader::new(r).lines();
                                let acks = Arc::clone(&acks);
                                let deliveries = Arc::clone(&deliveries);
                                tokio::spawn(async move {
                                    // First line is register
                                    if let Ok(Some(line)) = lines.next_line().await {
                                        if let Ok(val) = serde_json::from_str::<Value>(&line) {
                                            if val.get("harness").and_then(|h| h.as_str()) == Some("svc") {
                                                let resp = serde_json::json!({
                                                    "status": "ok",
                                                    "sessionId": "matrix-xmsg",
                                                });
                                                let _ = w.write_all(format!("{resp}\n").as_bytes()).await;
                                                let _ = w.flush().await;
                                            }
                                        }
                                    }

                                    // Next lines are poll / ack
                                    while let Ok(Some(line)) = lines.next_line().await {
                                        if let Ok(val) = serde_json::from_str::<Value>(&line) {
                                            let action = val.get("action").and_then(|a| a.as_str()).unwrap_or("");
                                            if action == "poll" {
                                                let delivery = deliveries.lock().await.pop_front();
                                                let resp = match delivery {
                                                    Some(d) => d,
                                                    None => serde_json::json!({
                                                        "status": "ok",
                                                        "action": "timeout",
                                                    }),
                                                };
                                                let _ = w.write_all(format!("{resp}\n").as_bytes()).await;
                                                let _ = w.flush().await;
                                            } else if action == "ack" {
                                                if let Some(msg_id) = val.get("messageId").and_then(|m| m.as_str()) {
                                                    acks.lock().await.push(msg_id.to_string());
                                                }
                                                let resp = serde_json::json!({
                                                    "status": "ok",
                                                });
                                                let _ = w.write_all(format!("{resp}\n").as_bytes()).await;
                                                let _ = w.flush().await;
                                            }
                                        }
                                    }
                                });
                            }
                        })
                    };

                    let _ = tokio::join!(agent_task, reg_task);
                } => {}
            }
        });

        Self {
            dir: dir.to_path_buf(),
            recorded_sends,
            recorded_acks,
            queued_deliveries,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    pub async fn queue_delivery(&self, delivery: Value) {
        self.queued_deliveries.lock().await.push_back(delivery);
    }
}

impl Drop for MockXmsgSockets {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = std::fs::remove_file(self.dir.join("register.sock"));
        let _ = std::fs::remove_file(self.dir.join("agent.sock"));
    }
}

fn test_config() -> Config {
    Config {
        homeserver_url: "https://matrix.example.org".to_string(),
        bot_mxid: "@genie:example.org".to_string(),
        access_token_file: PathBuf::from("/nonexistent/token"),
        rooms: vec!["!support:example.org".to_string()],
        trusted_mxids: vec![
            "@trusted:example.org".to_string(),
            "@alice:example.org".to_string(),
        ],
        owner_mxid: "@owner:example.org".to_string(),
        admission: Admission::Trusted,
        xmsg_socket: PathBuf::from("/run/user/1000/xmsg"),
        expert_ref: "claude".to_string(),
        history_n: 5,
        history_byte_cap: 1024,
        rate_limit_count: 5,
        rate_limit_window_secs: 60,
        size_cap_bytes: 2048,
        answer_timeout_secs: 10,
        session_live_secs: 3600,
        db_path: PathBuf::from(":memory:"),
    }
}

// ---------------------------------------------------------------------------
// Oracle 1:
// With xmsgSocket set, a send reaches the agent.sock stub with expected
// action, ref, text, and push_replies=true.
// Mutant: fall back or send without push_replies => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_1_send_reaches_socket_stub() {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockXmsgSockets::start(tmp.path()).await;

    let mut config = test_config();
    config.xmsg_socket = tmp.path().to_path_buf();

    let client = create_xmsg_client(&config);
    let resp = client
        .send_message("claude", "matrix_alice", "hello unix expert")
        .await
        .expect("send_message over unix socket must succeed");

    assert_eq!(resp.message_id, "01M7MSGTEST0000000000000000");

    let sends = server.recorded_sends.lock().await.clone();
    assert_eq!(
        sends.len(),
        1,
        "Expected exactly 1 request to agent.sock stub"
    );
    let req = &sends[0];
    assert_eq!(req["action"], "send");
    assert_eq!(req["ref"], "claude");
    assert_eq!(req["text"], "hello unix expert");
    assert_eq!(req["push_replies"], true);
}

// ---------------------------------------------------------------------------
// Oracle 2:
// The reply is delivered via svc inbox on register.sock, bot posts it to Matrix,
// and acks the message id on register.sock.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_2_reply_delivered_via_svc_inbox_and_bot_posts_reply() {
    let tmp = tempfile::tempdir().unwrap();
    let server = MockXmsgSockets::start(tmp.path()).await;

    let mut config = test_config();
    config.xmsg_socket = tmp.path().to_path_buf();

    let xmsg = create_xmsg_client(&config);
    let matrix = MockMatrixClient::default();
    let store = Store::new_in_memory().unwrap();

    let event = IncomingMatrixEvent {
        room_id: "!support:example.org".to_string(),
        event_id: "$ev_ask".to_string(),
        sender_mxid: "@alice:example.org".to_string(),
        body: "@genie How does Unix domain socket communication work?".to_string(),
        formatted_body: None,
        mentions: Some(vec!["@genie:example.org".to_string()]),
        timestamp_ms: 1000000,
        thread_root_id: None,
        replaces_event_id: None,
        in_reply_to_event_id: None,
        is_falling_back: false,
    };

    let outcome = handle_incoming_event(&event, &[], &config, &matrix, xmsg.as_ref(), &store)
        .await
        .expect("handle_incoming_event should succeed");

    assert_eq!(outcome, BotOutcome::Forwarded);

    // Queue delivery into register.sock
    server
        .queue_delivery(serde_json::json!({
            "status": "ok",
            "action": "deliver",
            "messageId": "01M7REPLYID",
            "fromName": "claude",
            "text": "Expert answer received over unix domain socket",
            "envelope": "[xmsg] reply to message_id=01M7MSGTEST0000000000000000 — message_id=01M7REPLYID\nExpert answer received over unix domain socket"
        }))
        .await;

    // Connect to register.sock as svc:matrix-xmsg
    let mut inbox = register_svc(&config.xmsg_register_socket(), "matrix-xmsg")
        .await
        .expect("register_svc must succeed");

    let delivery = inbox
        .poll(1)
        .await
        .expect("poll must succeed")
        .expect("must receive delivery");

    assert_eq!(delivery.message_id, "01M7REPLYID");

    handle_inbox_delivery(&delivery, &config, &matrix, &store, &mut inbox, 1000)
        .await
        .expect("handle_inbox_delivery must succeed");

    {
        let sent = matrix.sent_notices.lock().unwrap();
        assert_eq!(sent.len(), 1, "Bot must post exactly one notice reply");
        assert_eq!(sent[0].room_id, "!support:example.org");
        assert!(
            sent[0]
                .body
                .contains("Expert answer received over unix domain socket"),
            "Posted notice must contain the expert reply body, got: {}",
            sent[0].body
        );
    }

    let acks = server.recorded_acks.lock().await.clone();
    assert_eq!(acks, vec!["01M7REPLYID".to_string()]);
}

// ---------------------------------------------------------------------------
// Oracle 3:
// Module eval: a socket with DynamicUser => an assertion failure.
// Valid non-dynamic user configuration passes.
// Mutant: drop the assertion => RED.
// ---------------------------------------------------------------------------
#[test]
fn oracle_3_module_eval_assertions() {
    let which_nix = Command::new("which")
        .arg("nix")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);

    if !which_nix {
        eprintln!(
            "Nix binary not found in PATH; skipping nix-eval test (handled in CI flake check)"
        );
        return;
    }

    let nix_expr = r#"
    let
      pkgs = import <nixpkgs> {};
      lib = pkgs.lib;
      baseModule = {
        options = {
          assertions = lib.mkOption {
            type = lib.types.listOf lib.types.attrs;
            default = [];
          };
          users.users = lib.mkOption {
            type = lib.types.attrs;
            default = {};
          };
          users.groups = lib.mkOption {
            type = lib.types.attrs;
            default = {};
          };
          systemd.services = lib.mkOption {
            type = lib.types.attrs;
            default = {};
          };
        };
      };
      eval = serviceCfg: (lib.evalModules {
        modules = [
          baseModule
          (import ./nix/module.nix { self = { packages.${pkgs.system}.default = pkgs.hello; }; })
          {
            services.matrix-xmsg = {
              enable = true;
              homeserverUrl = "http://localhost:8008";
              botMxid = "@bot:local";
              accessTokenFile = /dev/null;
              rooms = [ "!room:local" ];
              ownerMxid = "@owner:local";
            } // serviceCfg;
          }
        ];
      });
      failedMsgs = eval: map (a: a.message) (builtins.filter (a: !a.assertion) eval.config.assertions);
    in {
      dynamicUserWithSocket = failedMsgs (eval { xmsgSocket = "/run/xmsg"; dynamicUser = true; });
      validSocket = failedMsgs (eval { xmsgSocket = "/run/xmsg"; dynamicUser = false; });
    }
    "#;

    let output = Command::new("nix")
        .args(["eval", "--impure", "--json", "--expr", nix_expr])
        .output()
        .expect("Failed to execute nix eval");

    assert!(
        output.status.success(),
        "nix eval failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let val: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("Failed to parse json");

    let dyn_user_msgs = val["dynamicUserWithSocket"]
        .as_array()
        .expect("dynamicUserWithSocket array");
    assert!(
        dyn_user_msgs.iter().any(|m| m
            .as_str()
            .unwrap_or("")
            .contains("when xmsgSocket is configured, dynamicUser must be false")),
        "Expected dynamicUser assertion failure when dynamicUser=true, got: {:?}",
        dyn_user_msgs
    );

    let valid_socket = val["validSocket"].as_array().expect("validSocket array");
    assert!(
        valid_socket.is_empty(),
        "Expected valid socket config to pass with no failed assertions, got: {:?}",
        valid_socket
    );
}

// ---------------------------------------------------------------------------
// Oracle 4:
// Unix domain socket configuration verifies paths for register.sock and agent.sock.
// ---------------------------------------------------------------------------
#[test]
fn oracle_4_unix_socket_paths_configured() {
    let mut config = test_config();
    config.xmsg_socket = PathBuf::from("/run/user/1000/xmsg");

    assert_eq!(
        config.xmsg_register_socket(),
        PathBuf::from("/run/user/1000/xmsg/register.sock")
    );
    assert_eq!(
        config.xmsg_agent_socket(),
        PathBuf::from("/run/user/1000/xmsg/agent.sock")
    );

    let client = UnixXmsgClient::new(config.xmsg_agent_socket());
    assert_eq!(
        client.agent_sock_path(),
        Path::new("/run/user/1000/xmsg/agent.sock")
    );
}
