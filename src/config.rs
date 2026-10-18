use crate::error::AppError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[cfg(target_os = "macos")]
fn darwin_user_temp_dir() -> Option<PathBuf> {
    use std::ffi::{CStr, OsStr};
    use std::os::unix::ffi::OsStrExt;
    const CS_DARWIN_USER_TEMP_DIR: i32 = 65537;
    const PATH_MAX: usize = 1024;
    extern "C" {
        fn confstr(name: i32, buf: *mut std::ffi::c_char, len: usize) -> usize;
    }
    let mut buf = vec![0 as std::ffi::c_char; PATH_MAX];
    let n = unsafe { confstr(CS_DARWIN_USER_TEMP_DIR, buf.as_mut_ptr(), buf.len()) };
    if n == 0 || n > buf.len() {
        return None;
    }
    let s = unsafe { CStr::from_ptr(buf.as_ptr()) };
    Some(PathBuf::from(OsStr::from_bytes(s.to_bytes())))
}

pub fn socket_dir_for_env(xdg_var: Option<&str>) -> Result<PathBuf, AppError> {
    if let Some(val) = xdg_var.filter(|s| !s.trim().is_empty()) {
        return Ok(PathBuf::from(val).join("xmsg"));
    }
    #[cfg(target_os = "macos")]
    if let Some(dir) = darwin_user_temp_dir() {
        return Ok(dir.join("xmsg"));
    }
    Err(AppError::Config(
        "Neither `xmsg_socket` in config nor `XDG_RUNTIME_DIR` in environment is set; refusing to guess an xmsg socket directory".to_string(),
    ))
}

fn default_xmsg_socket() -> PathBuf {
    PathBuf::new()
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

fn default_resync_byte_cap() -> usize {
    65536
}

fn default_resync_page_bytes() -> usize {
    8192
}

fn default_context_max_lines() -> usize {
    20
}

fn default_context_max_bytes() -> usize {
    8192
}

fn default_sender_window_secs() -> u64 {
    900
}

fn default_context_rate_limit_count() -> usize {
    6
}

fn default_context_rate_limit_window_secs() -> u64 {
    600
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
    600
}

fn default_session_live_secs() -> u64 {
    3600
}

fn default_guard_ref() -> String {
    "genie-guard".to_string()
}

fn default_guard_timeout_secs() -> u64 {
    150
}

fn default_guard_retry_budget() -> usize {
    3
}

fn default_screen_mentions() -> bool {
    true
}

fn default_guard_outage_dm_window_secs() -> u64 {
    3600
}

fn default_inbox_retry_budget() -> usize {
    10
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum GuardFailureMode {
    #[default]
    Closed,
    Open,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpertRefs {
    pub public: String,
    pub trusted: String,
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

    #[serde(default)]
    pub expert_refs: Option<ExpertRefs>,

    #[serde(default)]
    pub open_thread_nodes: Vec<String>,

    #[serde(default = "default_guard_ref")]
    pub guard_ref: String,

    #[serde(default = "default_guard_timeout_secs")]
    pub guard_timeout_secs: u64,

    #[serde(default = "default_guard_retry_budget")]
    pub guard_retry_budget: usize,

    #[serde(default = "default_screen_mentions")]
    pub screen_mentions: bool,

    #[serde(default)]
    pub guard_failure_mode: GuardFailureMode,

    #[serde(default = "default_guard_outage_dm_window_secs")]
    pub guard_outage_dm_window_secs: u64,

    #[serde(default = "default_inbox_retry_budget")]
    pub inbox_retry_budget: usize,

    #[serde(default = "default_history_n")]
    pub history_n: usize,

    #[serde(default = "default_history_byte_cap")]
    pub history_byte_cap: usize,

    #[serde(default = "default_resync_byte_cap")]
    pub resync_byte_cap: usize,

    #[serde(default = "default_resync_page_bytes")]
    pub resync_page_bytes: usize,

    #[serde(default = "default_context_max_lines")]
    pub context_max_lines: usize,

    #[serde(default = "default_context_max_bytes")]
    pub context_max_bytes: usize,

    #[serde(default = "default_sender_window_secs")]
    pub sender_window_secs: u64,

    #[serde(default = "default_context_rate_limit_count")]
    pub context_rate_limit_count: usize,

    #[serde(default = "default_context_rate_limit_window_secs")]
    pub context_rate_limit_window_secs: u64,

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
    pub fn from_toml_str_with_env(content: &str, xdg_var: Option<&str>) -> Result<Self, AppError> {
        let mut cfg: Self = toml::from_str(content)
            .map_err(|e| AppError::Config(format!("Failed to parse TOML config: {e}")))?;
        if cfg.xmsg_socket.as_os_str().is_empty() {
            cfg.xmsg_socket = socket_dir_for_env(xdg_var)?;
        }
        if let Some(ref refs) = cfg.expert_refs {
            if refs.public.trim().is_empty() || refs.trusted.trim().is_empty() {
                return Err(AppError::Config(
                    "`expert_refs.public` and `expert_refs.trusted` must both be non-empty"
                        .to_string(),
                ));
            }
        }
        Ok(cfg)
    }

    pub fn from_toml_str(content: &str) -> Result<Self, AppError> {
        let xdg = std::env::var("XDG_RUNTIME_DIR").ok();
        Self::from_toml_str_with_env(content, xdg.as_deref())
    }

    pub fn from_file(path: &Path) -> Result<Self, AppError> {
        let content = fs::read_to_string(path).map_err(|e| {
            AppError::Config(format!(
                "Failed to read config file at {}: {e}",
                path.display()
            ))
        })?;
        Self::from_toml_str(&content)
    }

    pub fn expert_ref_for_tier(&self, tier: &str) -> &str {
        if let Some(ref refs) = self.expert_refs {
            match tier {
                "trusted" => &refs.trusted,
                _ => &refs.public,
            }
        } else {
            &self.expert_ref
        }
    }

    pub fn load_access_token(&self) -> Result<String, AppError> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let meta = fs::metadata(&self.access_token_file).map_err(|e| {
                AppError::Config(format!(
                    "Failed to read token from {}: {e}",
                    self.access_token_file.display()
                ))
            })?;
            let mode = meta.permissions().mode() & 0o777;
            // systemd LoadCredential mounts /run/credentials/<unit> (0550) and writes
            // the credential file inside it with mode 0440.
            let is_systemd_cred =
                mode == 0o440 && self.access_token_file.starts_with("/run/credentials/");
            if mode & 0o077 != 0 && !is_systemd_cred {
                return Err(AppError::Config(format!(
                    "Access token file {} has mode {:04o}; refusing to start because it is group- or world-readable (expected 0600 or 0400)",
                    self.access_token_file.display(),
                    mode
                )));
            }
        }
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

    pub fn xmsg_http_socket(&self) -> PathBuf {
        self.xmsg_socket.join("http.sock")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guard_defaults() {
        let toml_str = r#"
            homeserver_url = "https://matrix.example.org"
            bot_mxid = "@genie:example.org"
            access_token_file = "/tmp/token"
            rooms = ["!room:example.org"]
            trusted_mxids = []
            owner_mxid = "@owner:example.org"
        "#;
        let cfg = Config::from_toml_str_with_env(toml_str, Some("/run/user/1001")).unwrap();
        assert_eq!(cfg.guard_ref, "genie-guard");
        assert_eq!(cfg.guard_timeout_secs, 150);
        assert_eq!(cfg.guard_retry_budget, 3);
        assert!(cfg.screen_mentions);
        assert_eq!(cfg.guard_failure_mode, GuardFailureMode::Closed);
        assert_eq!(cfg.guard_outage_dm_window_secs, 3600);
        assert_eq!(
            cfg.xmsg_http_socket(),
            PathBuf::from("/run/user/1001/xmsg/http.sock")
        );
    }

    #[test]
    fn test_guard_m26_explicit_options() {
        let toml_str = r#"
            homeserver_url = "https://matrix.example.org"
            bot_mxid = "@genie:example.org"
            access_token_file = "/tmp/token"
            rooms = ["!room:example.org"]
            trusted_mxids = []
            owner_mxid = "@owner:example.org"
            screen_mentions = false
            guard_failure_mode = "open"
            guard_outage_dm_window_secs = 600
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        assert!(!cfg.screen_mentions);
        assert_eq!(cfg.guard_failure_mode, GuardFailureMode::Open);
        assert_eq!(cfg.guard_outage_dm_window_secs, 600);
    }

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
        let cfg = Config::from_toml_str_with_env(toml_str, Some("/run/user/1001")).unwrap();
        let expected_dir = PathBuf::from("/run/user/1001/xmsg");
        assert_eq!(cfg.xmsg_socket, expected_dir);
        assert_eq!(
            cfg.xmsg_register_socket(),
            expected_dir.join("register.sock")
        );
        assert_eq!(cfg.xmsg_agent_socket(), expected_dir.join("agent.sock"));
    }

    #[test]
    fn test_socket_dir_for_env() {
        assert_eq!(
            socket_dir_for_env(Some("/run/user/1001")).unwrap(),
            PathBuf::from("/run/user/1001/xmsg")
        );
        #[cfg(not(target_os = "macos"))]
        {
            let err = socket_dir_for_env(None).unwrap_err().to_string();
            assert!(err.contains("xmsg_socket"), "{err}");
            assert!(err.contains("XDG_RUNTIME_DIR"), "{err}");
        }
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
    fn test_answer_timeout_secs_default_m29() {
        let toml_str = r#"
            homeserver_url = "https://matrix.example.org"
            bot_mxid = "@genie:example.org"
            access_token_file = "/tmp/token"
            rooms = ["!room:example.org"]
            trusted_mxids = []
            owner_mxid = "@owner:example.org"
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.answer_timeout_secs, 600);
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

    #[test]
    fn test_resync_byte_cap_default() {
        let toml_str = r#"
            homeserver_url = "https://matrix.example.org"
            bot_mxid = "@genie:example.org"
            access_token_file = "/tmp/token"
            rooms = ["!room:example.org"]
            trusted_mxids = []
            owner_mxid = "@owner:example.org"
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.resync_byte_cap, 65536);
    }

    #[test]
    fn test_inbox_retry_budget_default() {
        let toml_str = r#"
            homeserver_url = "https://matrix.example.org"
            bot_mxid = "@genie:example.org"
            access_token_file = "/tmp/token"
            rooms = ["!room:example.org"]
            trusted_mxids = []
            owner_mxid = "@owner:example.org"
        "#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.inbox_retry_budget, 10);
    }

    #[cfg(unix)]
    #[test]
    fn test_load_access_token_enforces_mode_0600_or_0400() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let token_path = dir.path().join("matrix-token");
        fs::write(&token_path, "syt_secret_token_123\n").unwrap();

        let toml_str = format!(
            r#"
            homeserver_url = "https://matrix.example.org"
            bot_mxid = "@genie:example.org"
            access_token_file = "{}"
            rooms = ["!room:example.org"]
            trusted_mxids = []
            owner_mxid = "@owner:example.org"
        "#,
            token_path.display()
        );
        let cfg: Config = toml::from_str(&toml_str).unwrap();

        // 0644 (group- and world-readable) is refused naming the file and its mode
        fs::set_permissions(&token_path, fs::Permissions::from_mode(0o644)).unwrap();
        let err_644 = cfg.load_access_token().unwrap_err().to_string();
        assert!(
            err_644.contains(&token_path.display().to_string()),
            "{err_644}"
        );
        assert!(err_644.contains("0644"), "{err_644}");

        // 0640 (group-readable) is refused naming the file and its mode
        fs::set_permissions(&token_path, fs::Permissions::from_mode(0o640)).unwrap();
        let err_640 = cfg.load_access_token().unwrap_err().to_string();
        assert!(
            err_640.contains(&token_path.display().to_string()),
            "{err_640}"
        );
        assert!(err_640.contains("0640"), "{err_640}");

        // 0604 (world-readable) is refused naming the file and its mode
        fs::set_permissions(&token_path, fs::Permissions::from_mode(0o604)).unwrap();
        let err_604 = cfg.load_access_token().unwrap_err().to_string();
        assert!(
            err_604.contains(&token_path.display().to_string()),
            "{err_604}"
        );
        assert!(err_604.contains("0604"), "{err_604}");

        // 0600 succeeds
        fs::set_permissions(&token_path, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(cfg.load_access_token().unwrap(), "syt_secret_token_123");

        // 0400 succeeds
        fs::set_permissions(&token_path, fs::Permissions::from_mode(0o400)).unwrap();
        assert_eq!(cfg.load_access_token().unwrap(), "syt_secret_token_123");
    }
}
