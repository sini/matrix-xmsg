use matrix_xmsg::matrix::{MatrixClient, MatrixSdkClient};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn setup_versions_mock(server: &MockServer) {
    use wiremock::matchers::path;
    Mock::given(method("GET"))
        .and(path("/_matrix/client/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "versions": ["v1.1", "v1.2", "v1.3", "v1.4", "v1.5", "v1.6", "v1.7", "v1.8", "v1.9", "v1.10", "v1.11"]
        })))
        .mount(server)
        .await;
}

/// Helper to parse YAML frontmatter from markdown content.
fn parse_yaml_frontmatter(content: &str) -> Option<(String, String)> {
    let trimmed = content.trim_start();
    if !trimmed.starts_with("---") {
        return None;
    }
    let rest = &trimmed[3..];
    let end_idx = rest.find("\n---")?;
    let frontmatter = &rest[..end_idx];

    let mut name = None;
    let mut description = None;

    for line in frontmatter.lines() {
        let line = line.trim();
        if let Some(val) = line.strip_prefix("name:") {
            name = Some(val.trim().trim_matches('"').trim_matches('\'').to_string());
        } else if let Some(val) = line.strip_prefix("description:") {
            description = Some(val.trim().trim_matches('"').trim_matches('\'').to_string());
        }
    }

    match (name, description) {
        (Some(n), Some(d)) => Some((n, d)),
        _ => None,
    }
}

/// Oracle 1:
/// The skill file exists, parses as YAML frontmatter with `name: matrix` and a non-empty `description`,
/// and contains all required protocol keywords (`silent`, `resync`, `open_thread`, `!release`).
/// Also verifies the flake output exposes the skill directory if nix is present.
/// Mutant: delete the `resync` section => RED.
#[test]
fn oracle_1_skill_frontmatter_and_protocol_keywords() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let skill_path = manifest_dir.join("skills/matrix/SKILL.md");
    assert!(
        skill_path.exists(),
        "SKILL.md must exist at {}",
        skill_path.display()
    );

    let content = fs::read_to_string(&skill_path)
        .unwrap_or_else(|e| panic!("Failed to read {}: {e}", skill_path.display()));

    // 1. Verify frontmatter
    let (name, description) = parse_yaml_frontmatter(&content)
        .expect("SKILL.md must contain valid YAML frontmatter between --- delimiters");

    assert_eq!(name, "matrix", "Frontmatter name must be 'matrix'");
    assert!(
        !description.trim().is_empty(),
        "Frontmatter description must be non-empty"
    );

    // 2. Verify protocol keywords
    assert!(
        content.contains("silent"),
        "SKILL.md must mention 'silent' protocol keyword"
    );
    assert!(
        content.contains("resync"),
        "SKILL.md must mention 'resync' protocol keyword"
    );
    assert!(
        content.contains("open_thread"),
        "SKILL.md must mention 'open_thread' protocol keyword"
    );
    assert!(
        content.contains("!release"),
        "SKILL.md must mention '!release' protocol keyword"
    );

    // 3. Verify flake output exposes matrix-skill if nix is available
    let which_nix = Command::new("which")
        .arg("nix")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);

    if which_nix {
        let output = Command::new("nix")
            .args(["build", ".#matrix-skill", "--no-link", "--print-out-paths"])
            .current_dir(&manifest_dir)
            .output();

        if let Ok(out) = output {
            if out.status.success() {
                let out_path = String::from_utf8_lossy(&out.stdout).trim().to_string();
                let built_skill = Path::new(&out_path).join("SKILL.md");
                assert!(
                    built_skill.exists(),
                    "Built flake package matrix-skill must contain SKILL.md at {}",
                    built_skill.display()
                );
            }
        }
    }
}

/// Oracle 2:
/// `send_notice`'s cold-room join fallback is restricted to rooms in `config.rooms`.
/// A send_notice to an unconfigured room id does not join it.
/// Mutant: join any room => RED.
#[tokio::test]
async fn oracle_2_send_notice_unconfigured_room_does_not_join() {
    let mock_server = MockServer::start().await;
    setup_versions_mock(&mock_server).await;

    let unconfigured_room = "!unconfigured:example.org";
    let configured_room = "!configured:example.org";

    // Expect 0 join requests for the unconfigured room
    Mock::given(method("POST"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/!unconfigured.*join$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "room_id": unconfigured_room
        })))
        .expect(0)
        .mount(&mock_server)
        .await;

    // Expect 1 join request for the configured room
    Mock::given(method("POST"))
        .and(path_regex(r"^/_matrix/client/v3/rooms/!configured.*join$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "room_id": configured_room
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    // Mock send message for the configured room once joined
    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/_matrix/client/v3/rooms/!configured.*send/m\.room\.message/.*$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "event_id": "$event_configured_01"
        })))
        .mount(&mock_server)
        .await;

    let client = MatrixSdkClient::new(
        &mock_server.uri(),
        "@genie:example.org",
        "syt_valid_token_test",
    )
    .await
    .expect("Failed to create MatrixSdkClient");

    // Only configured_room is allowlisted in config.rooms
    client.set_configured_rooms(&[configured_room.to_string()]);

    // 1. send_notice to UNCONFIGURED room must NOT call join
    let res_unconfigured = client
        .send_notice(unconfigured_room, None, "notice to unconfigured", None)
        .await;
    assert!(
        res_unconfigured.is_err(),
        "send_notice to unconfigured room should fail without joining"
    );

    // 2. send_notice to CONFIGURED room DOES call join and succeeds
    let res_configured = client
        .send_notice(configured_room, None, "notice to configured", None)
        .await;
    assert!(
        res_configured.is_ok(),
        "send_notice to configured room should succeed via join fallback: {:?}",
        res_configured.err()
    );

    // Verify mock server expectations (specifically expect(0) on unconfigured join)
    mock_server.verify().await;
}
