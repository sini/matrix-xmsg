use std::path::PathBuf;
use std::process::Command;

pub fn xmsg_bin(required: &str) -> Option<PathBuf> {
    let mandatory = std::env::var_os(required).is_some();
    let Some(path) = std::env::var_os("XMSG_BIN") else {
        assert!(
            !mandatory,
            "XMSG_BIN missing in mandatory real-daemon check"
        );
        return None;
    };
    let binary = PathBuf::from(path);
    assert!(binary.is_file(), "pinned xmsg executable missing");
    match std::env::var("MA_XMSG_REVISION") {
        Ok(revision) => {
            let output = Command::new(&binary)
                .arg("--version")
                .output()
                .expect("pinned xmsg version");
            assert!(output.status.success(), "pinned xmsg version failed");
            assert_eq!(
                String::from_utf8(output.stdout).unwrap().trim(),
                format!("xmsg 0.1.0+{revision}"),
                "wrong daemon revision"
            );
        }
        Err(_) => assert!(
            !mandatory,
            "MA_XMSG_REVISION missing in mandatory real-daemon check"
        ),
    }
    Some(binary)
}
