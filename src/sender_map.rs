/// Maps a Matrix user ID (e.g. `@alice:example.org`) to an ASCII sender name
/// conforming to xmsg HTTP sender invariants.
///
/// Invariants:
/// - Never contains ':' or '/'
/// - Strictly ASCII printable characters (0x20..=0x7E)
/// - Maximum length capped at 64 characters
pub fn map_sender_mxid(mxid: &str) -> String {
    let stripped = mxid.strip_prefix('@').unwrap_or(mxid);

    // Replace ':' with " at "
    let at_replaced = stripped.replace(':', " at ");

    // Filter to ASCII printable characters only, replacing '/' or remaining ':' with '-'
    let mut sanitized = String::with_capacity(at_replaced.len() + 7);
    sanitized.push_str("matrix ");

    for c in at_replaced.chars() {
        if c == '/' || c == ':' {
            sanitized.push('-');
        } else if (0x20..=0x7E).contains(&(c as u32)) {
            sanitized.push(c);
        }
        // non-ASCII or control characters are dropped
    }

    // Cap at 64 characters
    let capped: String = sanitized.chars().take(64).collect();

    if capped == "matrix " || capped.is_empty() {
        "matrix anonymous".to_string()
    } else {
        capped
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_standard_mxid_mapping() {
        assert_eq!(
            map_sender_mxid("@alice:example.org"),
            "matrix alice at example.org"
        );
        assert_eq!(
            map_sender_mxid("@bob:matrix.org"),
            "matrix bob at matrix.org"
        );
    }

    #[test]
    fn test_scrubs_slashes_and_colons() {
        let mapped = map_sender_mxid("@alice:sub/path:example.org");
        assert!(!mapped.contains(':'));
        assert!(!mapped.contains('/'));
        assert_eq!(mapped, "matrix alice at sub-path at example.org");
    }

    #[test]
    fn test_drops_non_ascii_and_caps_at_64() {
        let long_mxid = format!("@user_{}:example.org", "🦀".repeat(100));
        let mapped = map_sender_mxid(&long_mxid);
        assert!(mapped.len() <= 64);
        assert!(!mapped.contains('🦀'));
        assert_eq!(mapped, "matrix user_ at example.org");

        let very_long = format!("@verylongusername_{}:example.org", "a".repeat(100));
        let mapped_long = map_sender_mxid(&very_long);
        assert_eq!(mapped_long.len(), 64);
        assert!(!mapped_long.contains(':'));
        assert!(!mapped_long.contains('/'));
    }

    proptest! {
        #[test]
        fn prop_sender_mapping_invariants(input in ".*") {
            let mapped = map_sender_mxid(&input);
            prop_assert!(mapped.len() <= 64, "length {} > 64", mapped.len());
            prop_assert!(!mapped.contains(':'), "contains ':' in {}", mapped);
            prop_assert!(!mapped.contains('/'), "contains '/' in {}", mapped);
            for b in mapped.bytes() {
                prop_assert!((0x20..=0x7E).contains(&b), "non-printable ASCII byte 0x{:02x}", b);
            }
        }
    }
}
