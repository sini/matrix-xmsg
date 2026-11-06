#![allow(dead_code)]
use matrix_xmsg::xmsg::{register_svc, SvcSocketInbox};
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use xmsg::client::{current_uid, ClientOptions};

pub fn options(register: impl AsRef<Path>, service: &str) -> ClientOptions {
    ClientOptions {
        register_socket: register.as_ref().into(),
        agent_socket: register.as_ref().with_file_name("agent.sock"),
        max_frame_bytes: 1024 * 1024,
        operation_timeout: Duration::from_secs(2),
        service: service.into(),
        expected_node: None,
        daemon_uid: current_uid(),
    }
}

/// A live registration for tests whose external boundary is just agent.sock.
pub async fn register_for_agent(agent: impl AsRef<Path>) -> SvcSocketInbox {
    let register = agent.as_ref().with_file_name("fixture-register.sock");
    let listener = tokio::net::UnixListener::bind(&register).unwrap();
    tokio::spawn(async move {
        let mut stream = BufReader::new(listener.accept().await.unwrap().0);
        let mut line = String::new();
        stream.read_line(&mut line).await.unwrap();
        let frame: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(frame["harness"], "svc");
        assert_eq!(frame["name"], "matrix-xmsg");
        stream.get_mut().write_all(b"{\"status\":\"ok\",\"node\":\"sini.test\",\"address\":\"matrix-xmsg@sini.test\"}\n").await.unwrap();
        loop {
            line.clear();
            if stream.read_line(&mut line).await.unwrap() == 0 {
                break;
            }
        }
    });
    let mut config = options(register, "matrix-xmsg");
    config.agent_socket = agent.as_ref().into();
    register_svc(config).await.unwrap()
}
