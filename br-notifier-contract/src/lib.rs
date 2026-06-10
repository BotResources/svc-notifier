//! Published language for `svc-notifier` — the contract producers use to request
//! a notification delivery. Owned by the receiver; versioned independently of the
//! service. See the repository README for the full intake semantics.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// JetStream subject on which [`DeliverNotification`] commands are published.
pub const DELIVER_SUBJECT: &str = "notifier.cmd.notification.deliver.v1";

/// The v1 deliver command: one message fans out to one notification per recipient.
/// Delivery is idempotent on `(source_event_id, recipient_id)` — the first command
/// wins, duplicates are silently skipped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeliverNotification {
    pub source_event_id: Uuid,
    pub recipient_ids: Vec<Uuid>,
    pub template: String,
    pub payload: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link: Option<RelativeLink>,
}

/// A link a frontend can render on a notification — restricted to same-domain
/// relative URLs, validated at construction and on deserialization (fail closed).
///
/// Accepted: a path rooted at `/` (query string and fragment allowed).
/// Rejected: everything else — absolute URLs and schemes (`https:`, `javascript:`, …),
/// scheme-relative `//host`, backslashes, whitespace, control characters, empty input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RelativeLink(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RelativeLinkError {
    #[error("relative_link_empty")]
    Empty,
    #[error("relative_link_forbidden_character")]
    ForbiddenCharacter,
    #[error("relative_link_backslash")]
    Backslash,
    #[error("relative_link_not_rooted")]
    NotRooted,
    #[error("relative_link_scheme_relative")]
    SchemeRelative,
}

impl RelativeLink {
    pub fn parse(candidate: impl Into<String>) -> Result<Self, RelativeLinkError> {
        let candidate = candidate.into();
        if candidate.is_empty() {
            return Err(RelativeLinkError::Empty);
        }
        if candidate
            .chars()
            .any(|c| c.is_control() || c.is_whitespace())
        {
            return Err(RelativeLinkError::ForbiddenCharacter);
        }
        if candidate.contains('\\') {
            return Err(RelativeLinkError::Backslash);
        }
        if !candidate.starts_with('/') {
            return Err(RelativeLinkError::NotRooted);
        }
        if candidate.starts_with("//") {
            return Err(RelativeLinkError::SchemeRelative);
        }
        Ok(Self(candidate))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for RelativeLink {
    type Error = RelativeLinkError;

    fn try_from(candidate: String) -> Result<Self, Self::Error> {
        Self::parse(candidate)
    }
}

impl From<RelativeLink> for String {
    fn from(link: RelativeLink) -> Self {
        link.0
    }
}

impl AsRef<str> for RelativeLink {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RelativeLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::uuid;

    #[test]
    fn relative_link_accepts_same_domain_relative_urls() {
        for candidate in [
            "/",
            "/inbox",
            "/inbox?x=1#f",
            "/a/b/c",
            "/path:with/colon",
            "/items/123?sort=asc&dir=desc#row-4",
            "/caf%C3%A9",
            "/café",
        ] {
            let link = RelativeLink::parse(candidate)
                .unwrap_or_else(|e| panic!("rejected {candidate:?}: {e}"));
            assert_eq!(link.as_str(), candidate);
        }
    }

    #[test]
    fn relative_link_rejects_unsafe_candidates() {
        use RelativeLinkError::*;
        for (candidate, expected) in [
            ("", Empty),
            ("https://evil.com", NotRooted),
            ("http://evil.com", NotRooted),
            ("javascript:alert(1)", NotRooted),
            ("JavaScript:alert(1)", NotRooted),
            ("mailto:x@evil.com", NotRooted),
            ("data:text/html;base64,AAAA", NotRooted),
            ("inbox", NotRooted),
            ("./inbox", NotRooted),
            ("../inbox", NotRooted),
            ("//evil.com", SchemeRelative),
            ("/\\evil.com", Backslash),
            ("\\\\evil.com", Backslash),
            ("/x\\y", Backslash),
            ("   ", ForbiddenCharacter),
            (" /inbox", ForbiddenCharacter),
            ("/inbox ", ForbiddenCharacter),
            ("/x y", ForbiddenCharacter),
            ("/x\ty", ForbiddenCharacter),
            ("/x\ny", ForbiddenCharacter),
            ("/x\ry", ForbiddenCharacter),
            ("/x\u{0}", ForbiddenCharacter),
            ("/x\u{7f}", ForbiddenCharacter),
            ("/x\u{2028}y", ForbiddenCharacter),
        ] {
            assert_eq!(
                RelativeLink::parse(candidate),
                Err(expected),
                "candidate: {candidate:?}"
            );
        }
    }

    #[test]
    fn deserialization_is_fail_closed() {
        for unsafe_link in ["https://evil.com", "//evil.com", "javascript:alert(1)", ""] {
            assert!(
                serde_json::from_value::<RelativeLink>(json!(unsafe_link)).is_err(),
                "deserialized {unsafe_link:?}"
            );
        }
        let command_with_unsafe_link = json!({
            "source_event_id": "0196a000-0000-7000-8000-000000000001",
            "recipient_ids": ["0196a000-0000-7000-8000-000000000002"],
            "template": "meeting_scheduled",
            "payload": {},
            "link": "javascript:alert(1)",
        });
        assert!(serde_json::from_value::<DeliverNotification>(command_with_unsafe_link).is_err());
    }

    #[test]
    fn wire_format_is_pinned_and_round_trips() {
        let command = DeliverNotification {
            source_event_id: uuid!("0196a000-0000-7000-8000-000000000001"),
            recipient_ids: vec![
                uuid!("0196a000-0000-7000-8000-000000000002"),
                uuid!("0196a000-0000-7000-8000-000000000003"),
            ],
            template: "meeting_scheduled".to_string(),
            payload: json!({"meeting_id": "m-1", "starts_at": "2026-06-10T12:00:00Z"}),
            link: Some(RelativeLink::parse("/meetings/m-1").unwrap()),
        };
        let expected = json!({
            "source_event_id": "0196a000-0000-7000-8000-000000000001",
            "recipient_ids": [
                "0196a000-0000-7000-8000-000000000002",
                "0196a000-0000-7000-8000-000000000003",
            ],
            "template": "meeting_scheduled",
            "payload": {"meeting_id": "m-1", "starts_at": "2026-06-10T12:00:00Z"},
            "link": "/meetings/m-1",
        });
        assert_eq!(serde_json::to_value(&command).unwrap(), expected);
        let round_tripped: DeliverNotification = serde_json::from_value(expected).unwrap();
        assert_eq!(round_tripped, command);
    }

    #[test]
    fn link_is_optional_on_the_wire() {
        let without_link = json!({
            "source_event_id": "0196a000-0000-7000-8000-000000000001",
            "recipient_ids": ["0196a000-0000-7000-8000-000000000002"],
            "template": "meeting_scheduled",
            "payload": {},
        });
        let command: DeliverNotification = serde_json::from_value(without_link).unwrap();
        assert_eq!(command.link, None);
        let serialized = serde_json::to_value(&command).unwrap();
        assert!(serialized.get("link").is_none());
    }

    #[test]
    fn subject_follows_the_command_convention() {
        let convention = regex::Regex::new(r"^[a-z-]+\.cmd\.[a-z-]+\.[a-z-]+\.v[0-9]+$").unwrap();
        assert!(convention.is_match(DELIVER_SUBJECT), "{DELIVER_SUBJECT}");
    }
}
