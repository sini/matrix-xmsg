use crate::error::AppError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

fn default_xmsg_socket() -> PathBuf {
    PathBuf::from("/run/user/1000/xmsg")
}

fn default_expert_ref() -> String {
    "claude".to_string()
}

fn default_history_n() -> usize {
    30
}

fn default_history_byte_cap() -> usize {
    12 * 1024
}

fn default_rate_limit_count() -> usize {
    10
}

fn default_rate_limit_window_secs() -> u64 {
    600
}

fn default_size_cap_bytes() -> usize {
    4096
}

fn default_answer_timeout_secs() -> u64 {
    300
}

fn default_session_live_secs() -> u64 {
    3600
}

fn default_db_path() -> PathBuf {
    PathBuf::from("/var/lib/matrix-xmsg/matrix-xmsg.db")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Admission {
    #[default]
    Trusted,
    Public,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub homeserver_url: String,
    pub bot_mxid: String,
    pub access_token_file: PathBuf,
    pub rooms: Vec<String>,
    pub trusted_mxids: Vec<String>,
    pub owner_mxid: String,

    #[serde(default)]
    pub admission: Admission,

    #[serde(default = "default_xmsg_socket")]
    pub xmsg_socket: PathBuf,

    #[serde(default = "default_expert_ref")]
    pub expert_ref: String,

    #[serde(default = "default_history_n")]
    pub history_n: usize,

    #[serde(default = "default_history_byte_cap")]
    pub history_byte_cap: usize,

    #[serde(default = "default_rate_limit_count")]
    pub rate_limit_count: usize,

    #[serde(default = "default_rate_limit_window_secs")]
    pub rate_limit_window_secs: u64,

    #[serde(default = "default_size_cap_bytes")]
    pub size_cap_bytes: usize,

    #[serde(default = "default_answer_timeout_secs")]
    pub answer_timeout_secs: u64,

    #[serde(default = "default_session_live_secs")]
    pub session_live_secs: u64,

    #[serde(default = "default_db_path")]
    pub db_path: PathBuf,
}

impl Config {
    pub fn from_file(path: &Path) -> Result<Self, AppError> {
        let content = fs::read_to_string(path).map_err(|e| {
            AppError::Config(format!(
                "Failed to read config file at {}: {e}",
                path.display()
            ))
        })?;
        toml::from_str(&content)
            .map_err(|e| AppError::Config(format!("Failed to parse TOML config: {e}")))
    }

    pub fn load_access_token(&self) -> Result<String, AppError> {
        let token = fs::read_to_string(&self.access_token_file).map_err(|e| {
            AppError::Config(format!(
                "Failed to read token from {}: {e}",
                self.access_token_file.display()
            ))
        })?;
        let trimmed = token.trim();
        if trimmed.is_empty() {
            return Err(AppError::Config("Access token file is empty".to_string()));
        }
        Ok(trimmed.to_string())
    }

    pub fn is_room_allowlisted(&self, room_id_or_alias: &str) -> bool {
        self.rooms.iter().any(|r| r == room_id_or_alias)
    }

    pub fn is_user_trusted(&self, user_mxid: &str) -> bool {
        self.owner_mxid == user_mxid || self.trusted_mxids.iter().any(|u| u == user_mxid)
    }

    pub fn xmsg_register_socket(&self) -> PathBuf {
        self.xmsg_socket.join("register.sock")
    }

    pub fn xmsg_agent_socket(&self) -> PathBuf {
        self.xmsg_socket.join("agent.sock")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_admission_default_trusted() {
        let toml_str = r#"
            homeserver_url = "https://matrix.example.org"
            bot_mxid = "@genie:example.org"
            access_token_file = "/tmp/token"
            rooms = ["!room:example.org"]
            trusted_mxids = ["@alice:example.org"]
            owner_mxid = "@owner:example.org"
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.admission, Admission::Trusted);
    }

    #[test]
    fn test_admission_public() {
        let toml_str = r#"
            homeserver_url = "https://matrix.example.org"
            bot_mxid = "@genie:example.org"
            access_token_file = "/tmp/token"
            rooms = ["!room:example.org"]
            trusted_mxids = []
            owner_mxid = "@owner:example.org"
            admission = "public"
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.admission, Admission::Public);
    }

    #[test]
    fn test_admission_explicit_trusted() {
        let toml_str = r#"
            homeserver_url = "https://matrix.example.org"
            bot_mxid = "@genie:example.org"
            access_token_file = "/tmp/token"
            rooms = ["!room:example.org"]
            trusted_mxids = []
            owner_mxid = "@owner:example.org"
            admission = "trusted"
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.admission, Admission::Trusted);
    }

    #[test]
    fn test_xmsg_endpoint_tcp_default() {
        let toml_str = r#"
            homeserver_url = "https://matrix.example.org"
            bot_mxid = "@genie:example.org"
            access_token_file = "/tmp/token"
            rooms = ["!room:example.org"]
            trusted_mxids = []
            owner_mxid = "@owner:example.org"
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.xmsg_socket, PathBuf::from("/run/user/1000/xmsg"));
        assert_eq!(
            cfg.xmsg_register_socket(),
            PathBuf::from("/run/user/1000/xmsg/register.sock")
        );
        assert_eq!(
            cfg.xmsg_agent_socket(),
            PathBuf::from("/run/user/1000/xmsg/agent.sock")
        );
    }

    #[test]
    fn test_xmsg_socket_custom() {
        let toml_str = r#"
            homeserver_url = "https://matrix.example.org"
            bot_mxid = "@genie:example.org"
            access_token_file = "/tmp/token"
            rooms = ["!room:example.org"]
            trusted_mxids = []
            owner_mxid = "@owner:example.org"
            xmsg_socket = "/tmp/test-xmsg"
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.xmsg_socket, PathBuf::from("/tmp/test-xmsg"));
        assert_eq!(
            cfg.xmsg_register_socket(),
            PathBuf::from("/tmp/test-xmsg/register.sock")
        );
        assert_eq!(
            cfg.xmsg_agent_socket(),
            PathBuf::from("/tmp/test-xmsg/agent.sock")
        );
    }

    #[test]
    fn test_session_live_secs_default() {
        let toml_str = r#"
            homeserver_url = "https://matrix.example.org"
            bot_mxid = "@genie:example.org"
            access_token_file = "/tmp/token"
            rooms = ["!room:example.org"]
            trusted_mxids = []
            owner_mxid = "@owner:example.org"
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.session_live_secs, 3600);
    }
}
