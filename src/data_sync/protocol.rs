use std::time::Duration;

use chrono::Utc;
use rand::Rng;
use url::Url;

use crate::model::DataSyncEnvelope;
use crate::options::FbOptions;
use crate::store::{PatchResult, SnapshotStore};

use super::{StatusTracker, SyncStatus};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ApplyResult {
    Full,
    Patch,
    Ignored,
    VersionConflict,
}

pub(super) fn apply_message(
    store: &SnapshotStore,
    status: &StatusTracker,
    payload: &[u8],
) -> ApplyResult {
    let envelope = match serde_json::from_slice::<DataSyncEnvelope>(payload) {
        Ok(envelope) => envelope,
        Err(error) => {
            log::debug!("discarding malformed FeatBit WebSocket message: {error}");
            return ApplyResult::Ignored;
        }
    };
    if envelope.message_type != "data-sync" {
        return ApplyResult::Ignored;
    }

    let result = match envelope.data.event_type.as_str() {
        "full" => {
            store.populate(&envelope.data);
            ApplyResult::Full
        }
        "patch" => match store.patch(&envelope.data) {
            PatchResult::Changed | PatchResult::Unchanged => ApplyResult::Patch,
            PatchResult::VersionConflict => ApplyResult::VersionConflict,
        },
        event_type => {
            log::debug!("ignoring unknown FeatBit data-sync event type {event_type:?}");
            return ApplyResult::Ignored;
        }
    };
    if result == ApplyResult::VersionConflict {
        status.set(SyncStatus::Stale);
        return result;
    }
    log::debug!(
        "applied FeatBit {} data-sync at version {}",
        envelope.data.event_type,
        store.version()
    );
    status.set(SyncStatus::Ready);
    result
}

pub(super) fn streaming_url(options: &FbOptions) -> Result<Url, String> {
    let token = connection_token(&options.env_secret)
        .ok_or_else(|| "environment secret cannot form a connection token".to_owned())?;
    let mut url = options.streaming_url.clone();
    let base_path = url.path().trim_end_matches('/');
    url.set_path(&format!("{base_path}/streaming"));
    url.set_query(None);
    url.set_fragment(None);
    url.query_pairs_mut()
        .append_pair("type", "server")
        .append_pair("token", &token);
    Ok(url)
}

pub(super) fn connection_token(secret: &str) -> Option<String> {
    let trimmed = secret.trim_end_matches('=');
    if trimmed.len() < 3 || !trimmed.is_ascii() {
        return None;
    }

    let start = rand::rng().random_range(2..trimmed.len());
    let start_number = u64::try_from(start).ok()?;
    let timestamp = Utc::now().timestamp_millis().max(0);
    let timestamp_text = timestamp.to_string();
    let prefix = trimmed.get(..start)?;
    let suffix = trimmed.get(start..)?;

    Some(format!(
        "{}{}{prefix}{}{suffix}",
        encode_number(start_number, 3),
        encode_number(u64::try_from(timestamp_text.len()).ok()?, 2),
        encode_number(timestamp.cast_unsigned(), timestamp_text.len())
    ))
}

pub(super) fn encode_number(number: u64, length: usize) -> String {
    const MAP: [char; 10] = ['Q', 'B', 'W', 'S', 'P', 'H', 'D', 'X', 'Z', 'U'];
    let padded = format!("{number:0length$}");
    let mut digits: Vec<char> = padded.chars().rev().take(length).collect();
    digits.reverse();
    digits
        .into_iter()
        .filter_map(|digit| digit.to_digit(10))
        .filter_map(|digit| MAP.get(digit as usize).copied())
        .collect()
}

pub(super) fn reconnect_delay(options: &FbOptions, attempt: usize) -> Duration {
    let length = options.reconnect_delays.len();
    if length == 0 {
        return Duration::from_secs(1);
    }
    let index = attempt % length;
    let base = options
        .reconnect_delays
        .get(index)
        .copied()
        .unwrap_or(Duration::from_secs(1));
    if base.is_zero() {
        return base;
    }
    let jitter_percent = rand::rng().random_range(80..=120);
    base.checked_mul(jitter_percent)
        .and_then(|duration| duration.checked_div(100))
        .unwrap_or(base)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::reconnect_delay;
    use crate::options::FbOptions;

    #[test]
    fn reconnect_delay_cycles_configured_series_and_stays_within_jitter_bounds() {
        let options = FbOptions::builder("abc")
            .offline(true)
            .reconnect_delays([
                Duration::ZERO,
                Duration::from_secs(1),
                Duration::from_secs(2),
            ])
            .build()
            .expect("test options should be valid");

        assert_eq!(reconnect_delay(&options, 0), Duration::ZERO);
        assert_eq!(reconnect_delay(&options, 3), Duration::ZERO);
        for attempt in [1, 4] {
            let delay = reconnect_delay(&options, attempt);
            assert!(delay >= Duration::from_millis(800));
            assert!(delay <= Duration::from_millis(1_200));
        }
        for attempt in [2, 5] {
            let delay = reconnect_delay(&options, attempt);
            assert!(delay >= Duration::from_millis(1_600));
            assert!(delay <= Duration::from_millis(2_400));
        }
    }
}
