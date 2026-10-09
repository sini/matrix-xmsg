use matrix_xmsg::bot::{handle_incoming_event, BotOutcome, IncomingMatrixEvent};
use matrix_xmsg::config::{Admission, Config, XmsgEndpoint};
use matrix_xmsg::matrix::MockMatrixClient;
use matrix_xmsg::store::Store;
use matrix_xmsg::xmsg::create_xmsg_client;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub struct RecordedRequest {
    pub method: String,
    pub path: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

pub struct StubUnixServer {
    pub socket_path: PathBuf,
    pub requests: Arc<Mutex<Vec<RecordedRequest>>>,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl StubUnixServer {
    pub async fn start(socket_path: &Path) -> Self {
        let _ = std::fs::remove_file(socket_path);
        let listener = UnixListener::bind(socket_path).unwrap_or_else(|e| {
            panic!(
                "Failed to bind unix socket at {}: {e}",
                socket_path.display()
            )
        });

        let requests = Arc::new(Mutex::new(Vec::new()));
        let reqs_clone = Arc::clone(&requests);
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        break;
                    }
                    accept_res = listener.accept() => {
                        match accept_res {
                            Ok((mut stream, _)) => {
                                let reqs = Arc::clone(&reqs_clone);
                                tokio::spawn(async move {
                                    let mut buf = Vec::new();
                                    let mut temp_buf = [0u8; 1024];

                                    // Read until end of headers (\r\n\r\n)
                                    let header_end = loop {
                                        let n = match stream.read(&mut temp_buf).await {
                                            Ok(0) => return,
                                            Ok(n) => n,
                                            Err(_) => return,
                                        };
                                        buf.extend_from_slice(&temp_buf[..n]);
                                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                            break pos;
                                        }
                                    };

                                    let header_str = match std::str::from_utf8(&buf[..header_end]) {
                                        Ok(s) => s,
                                        Err(_) => return,
                                    };

                                    let mut lines = header_str.lines();
                                    let req_line = match lines.next() {
                                        Some(l) => l,
                                        None => return,
                                    };
                                    let mut parts = req_line.split_whitespace();
                                    let method = parts.next().unwrap_or("").to_string();
                                    let path = parts.next().unwrap_or("").to_string();

                                    let mut headers = HashMap::new();
                                    let mut content_length = 0usize;
                                    for line in lines {
                                        if let Some((k, v)) = line.split_once(':') {
                                            let key = k.trim().to_lowercase();
                                            let val = v.trim().to_string();
                                            if key == "content-length" {
                                                content_length = val.parse().unwrap_or(0);
                                            }
                                            headers.insert(key, val);
                                        }
                                    }

                                    let mut body = buf[header_end + 4..].to_vec();
                                    while body.len() < content_length {
                                        let n = match stream.read(&mut temp_buf).await {
                                            Ok(0) => break,
                                            Ok(n) => n,
                                            Err(_) => break,
                                        };
                                        body.extend_from_slice(&temp_buf[..n]);
                                    }

                                    reqs.lock().await.push(RecordedRequest {
                                        method: method.clone(),
                                        path: path.clone(),
                                        headers,
                                        body,
                                    });

                                    // Respond
                                    if method == "POST" && path.starts_with("/v1/sessions/") {
                                        let resp_body = r#"{"messageId":"01M7MSGTEST0000000000000000"}"#;
                                        let resp = format!(
                                            "HTTP/1.1 200 OK\r\n\
                                             Content-Type: application/json\r\n\
                                             Content-Length: {}\r\n\
                                             Connection: close\r\n\
                                             \r\n\
                                             {}",
                                            resp_body.len(),
                                            resp_body
                                        );
                                        let _ = stream.write_all(resp.as_bytes()).await;
                                        let _ = stream.flush().await;
                                    } else if method == "GET" && path.starts_with("/v1/messages/") {
                                        let resp_body = r#"[{"seq":1,"body":"Expert answer received over unix domain socket"}]"#;
                                        let resp = format!(
                                            "HTTP/1.1 200 OK\r\n\
                                             Content-Type: application/json\r\n\
                                             Content-Length: {}\r\n\
                                             Connection: close\r\n\
                                             \r\n\
                                             {}",
                                            resp_body.len(),
                                            resp_body
                                        );
                                        let _ = stream.write_all(resp.as_bytes()).await;
                                        let _ = stream.flush().await;
                                    } else {
                                        let resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                                        let _ = stream.write_all(resp.as_bytes()).await;
                                        let _ = stream.flush().await;
                                    }
                                });
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });

        Self {
            socket_path: socket_path.to_path_buf(),
            requests,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    pub async fn recorded_requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().await.clone()
    }
}

impl Drop for StubUnixServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = std::fs::remove_file(&self.socket_path);
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
        xmsg_url: "http://127.0.0.1:1".to_string(), // Unreachable TCP port ensures fallback fails
        xmsg_socket: None,
        expert_ref: "claude".to_string(),
        history_n: 5,
        history_byte_cap: 1024,
        rate_limit_count: 5,
        rate_limit_window_secs: 60,
        size_cap_bytes: 2048,
        answer_timeout_secs: 10,
        answer_deadline_secs: 3600,
        session_live_secs: 3600,
        db_path: PathBuf::from(":memory:"),
    }
}

// ---------------------------------------------------------------------------
// Oracle 1:
// With xmsgSocket set, a send reaches the socket stub with expected path,
// method and body.
// Mutant: fall back to TCP => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_1_send_reaches_socket_stub() {
    let tmp = tempfile::tempdir().unwrap();
    let socket_path = tmp.path().join("xmsg.sock");
    let server = StubUnixServer::start(&socket_path).await;

    let mut config = test_config();
    config.xmsg_socket = Some(socket_path.clone());

    let client = create_xmsg_client(&config);
    let msg_id = client
        .send_message("claude", "matrix_alice", "hello unix expert")
        .await
        .expect("send_message over unix socket must succeed");

    assert_eq!(msg_id, "01M7MSGTEST0000000000000000");

    let reqs = server.recorded_requests().await;
    assert_eq!(reqs.len(), 1, "Expected exactly 1 request to unix stub");
    let req = &reqs[0];
    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/v1/sessions/claude/messages");
    let body_json: serde_json::Value = serde_json::from_slice(&req.body).expect("Valid JSON body");
    assert_eq!(body_json["from"], "matrix_alice");
    assert_eq!(body_json["text"], "hello unix expert");
}

// ---------------------------------------------------------------------------
// Oracle 2:
// The reply long-poll works over the socket (the stub returns a reply,
// and the bot posts it).
// Mutant: break the unix connector for GET => RED.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_2_reply_long_poll_and_bot_posts_reply() {
    let tmp = tempfile::tempdir().unwrap();
    let socket_path = tmp.path().join("xmsg.sock");
    let server = StubUnixServer::start(&socket_path).await;

    let mut config = test_config();
    config.xmsg_socket = Some(socket_path.clone());

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

    assert_eq!(outcome, BotOutcome::Replied);

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

    let reqs = server.recorded_requests().await;
    assert!(reqs.len() >= 2, "Expected at least POST and GET requests");
    assert_eq!(reqs[0].method, "POST");
    assert_eq!(reqs[0].path, "/v1/sessions/claude/messages");
    assert_eq!(reqs[1].method, "GET");
    assert!(
        reqs[1]
            .path
            .starts_with("/v1/messages/01M7MSGTEST0000000000000000/replies"),
        "GET request path was: {}",
        reqs[1].path
    );
}

// ---------------------------------------------------------------------------
// Oracle 3:
// Module eval: both xmsgUrl and xmsgSocket set => an assertion failure;
// a socket with DynamicUser => an assertion failure.
// Mutant: drop the assertion => RED.
// ---------------------------------------------------------------------------
#[test]
fn oracle_3_module_eval_assertions() {
    // When running inside a Nix build sandbox, nix binary is not present in PATH.
    // In that environment, the check is guaranteed by ci/tests/m7_module.nix
    // evaluated under nix flake check ./ci.
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
      both = failedMsgs (eval { xmsgUrl = "http://127.0.0.1:7787"; xmsgSocket = "/run/xmsg.sock"; dynamicUser = false; });
      dynamicUserWithSocket = failedMsgs (eval { xmsgSocket = "/run/xmsg.sock"; dynamicUser = true; });
      validSocket = failedMsgs (eval { xmsgSocket = "/run/xmsg.sock"; dynamicUser = false; });
      validUrl = failedMsgs (eval { xmsgUrl = "http://127.0.0.1:7787"; dynamicUser = true; });
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

    let both_msgs = val["both"].as_array().expect("both array");
    assert!(
        both_msgs.iter().any(|m| m
            .as_str()
            .unwrap_or("")
            .contains("exactly one of services.matrix-xmsg.xmsgUrl or services.matrix-xmsg.xmsgSocket must be set")),
        "Expected exactly one assertion failure when both xmsgUrl and xmsgSocket are set, got: {:?}",
        both_msgs
    );

    let dyn_user_msgs = val["dynamicUserWithSocket"]
        .as_array()
        .expect("dynamicUserWithSocket array");
    assert!(
        dyn_user_msgs.iter().any(|m| m
            .as_str()
            .unwrap_or("")
            .contains("when xmsgSocket is configured, dynamicUser must be false")),
        "Expected dynamicUser assertion failure when xmsgSocket is set with dynamicUser, got: {:?}",
        dyn_user_msgs
    );

    let valid_socket = val["validSocket"].as_array().expect("validSocket array");
    assert!(
        valid_socket.is_empty(),
        "Expected valid socket config to pass with no failed assertions, got: {:?}",
        valid_socket
    );

    let valid_url = val["validUrl"].as_array().expect("validUrl array");
    assert!(
        valid_url.is_empty(),
        "Expected valid url config to pass with no failed assertions, got: {:?}",
        valid_url
    );
}

// ---------------------------------------------------------------------------
// Oracle 4:
// The TCP path is unchanged (the existing tests stay green).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn oracle_4_tcp_path_unchanged() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/sessions/claude/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "messageId": "01M7TCPMESSAGEID"
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(wiremock::matchers::path_regex(r"^/v1/messages/.*/replies"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {
                "seq": 1,
                "body": "TCP response"
            }
        ])))
        .mount(&mock_server)
        .await;

    let mut config = test_config();
    config.xmsg_url = mock_server.uri();
    config.xmsg_socket = None;

    assert_eq!(config.xmsg_endpoint(), XmsgEndpoint::Tcp(mock_server.uri()));

    let client = create_xmsg_client(&config);
    let msg_id = client
        .send_message("claude", "matrix_alice", "hello tcp")
        .await
        .expect("send_message over TCP must succeed");
    assert_eq!(msg_id, "01M7TCPMESSAGEID");

    let reply = client
        .wait_for_reply("01M7TCPMESSAGEID", 5)
        .await
        .expect("wait_for_reply over TCP must succeed");
    assert_eq!(reply, "TCP response");
}
