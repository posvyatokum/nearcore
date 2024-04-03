use chrono::{DateTime, Utc};
use near_primitives_core::types::ProtocolVersion;
use std::collections::btree_map::BTreeMap;

/// Defines the point in time after which validators are expected to vote on the
/// new protocol version.
#[derive(Clone, Debug, PartialEq)]
pub struct ProtocolUpgradeVotingSchedule {
    timestamp_to_version: BTreeMap<DateTime<Utc>, ProtocolVersion>,
}

impl Default for ProtocolUpgradeVotingSchedule {
    fn default() -> Self {
        Self { timestamp_to_version: BTreeMap::new() }
    }
}

impl ProtocolUpgradeVotingSchedule {
    /// Return the latest announced protocol version.
    /// Protocol version is announced if Utc::now() >= timestamp of version voting.
    /// If all timestamps from voting schedule are in the future, returns None.
    pub fn get_latest_announced_version(&self) -> Option<ProtocolVersion> {
        let now = Utc::now();

        // We need to return a value corresponding to the latest timestamp that is less or equal than `now`.
        // If there is no such value, no protocol voting took place yet, so we should return None.
        self.timestamp_to_version
            .range((std::ops::Bound::Unbounded, std::ops::Bound::Included(&now))) // all values <= now
            .next_back() // latest (timestamp, value) pair
            .map(|(_key, value)| value.clone())
    }

    /*
    pub fn is_in_future(&self) -> bool {
        chrono::Utc::now() < self.timestamp
    }

    pub fn timestamp(&self) -> i64 {
        self.timestamp.timestamp()
    }

    /// This method creates an instance of the ProtocolUpgradeVotingSchedule.
    ///
    /// It will first check if the NEAR_TESTS_IMMEDIATE_PROTOCOL_UPGRADE is
    /// set in the environment and if so return the immediate upgrade schedule.
    /// This should only be used in tests, in particular in tests the in some
    /// way test neard upgrades.
    ///
    /// Otherwise it will parse the given string and return the corresponding
    /// upgrade schedule.
    pub fn from_env_or_str(s: &str) -> Result<Self, ParseError> {
        let immediate_upgrade = env::var("NEAR_TESTS_IMMEDIATE_PROTOCOL_UPGRADE");
        if let Ok(_) = immediate_upgrade {
            tracing::warn!("Setting immediate protocol upgrade. This is fine in tests but should be avoided otherwise");
            return Ok(Self::default());
        }

        Ok(Self {
            timestamp: DateTime::<Utc>::from_naive_utc_and_offset(
                NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")?,
                Utc,
            ),
        })
    }
     */
}

pub(crate) fn get_protocol_version_internal(
    // Protocol version that will be used in the next epoch.
    next_epoch_protocol_version: ProtocolVersion,
    // Latest protocol version supported by this client.
    client_protocol_version: ProtocolVersion,
    // Point in time when voting for client_protocol_version version is expected
    // to start.  Use `Default::default()` to start voting immediately.
    voting_schedule: &ProtocolUpgradeVotingSchedule,
) -> ProtocolVersion {
    if next_epoch_protocol_version >= client_protocol_version {
        client_protocol_version
    } else if let Some(protocol_version) = voting_schedule.get_latest_announced_version() {
        protocol_version
    } else {
        // Don't announce support for new protocol version yet.
        next_epoch_protocol_version
    }
}

/*
#[cfg(test)]
mod tests {
    use super::*;

    // The tests call `get_protocol_version_internal()` with the following parameters:
    // No schedule:                  (X-2,X), (X,X), (X+2,X)
    // Before the scheduled upgrade: (X-2,X), (X,X), (X+2,X)
    // After the scheduled upgrade:  (X-2,X), (X,X), (X+2,X)

    #[test]
    fn test_no_upgrade_schedule() {
        // As no protocol upgrade voting schedule is set, always return the version supported by the client.

        let client_protocol_version = 100;
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version - 2,
                client_protocol_version,
                Default::default(),
            )
        );
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version,
                client_protocol_version,
                Default::default()
            )
        );
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version + 2,
                client_protocol_version,
                Default::default(),
            )
        );
    }

    #[test]
    fn test_none_upgrade_schedule() {
        // As no protocol upgrade voting schedule is set, always return the version supported by the client.

        let client_protocol_version = 100;

        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version - 2,
                client_protocol_version,
                Default::default(),
            )
        );
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version,
                client_protocol_version,
                Default::default(),
            )
        );
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version + 2,
                client_protocol_version,
                Default::default(),
            )
        );
    }

    #[test]
    fn test_before_scheduled_time() {
        let client_protocol_version = 100;
        let schedule =
            ProtocolUpgradeVotingSchedule::from_env_or_str("2050-01-01 00:00:00").unwrap();

        // The client supports a newer version than the version of the next epoch.
        // Upgrade voting will start in the far future, therefore don't announce the newest supported version.
        let next_epoch_protocol_version = client_protocol_version - 2;
        assert_eq!(
            next_epoch_protocol_version,
            get_protocol_version_internal(
                next_epoch_protocol_version,
                client_protocol_version,
                schedule,
            )
        );

        // An upgrade happened before the scheduled time.
        let next_epoch_protocol_version = client_protocol_version;
        assert_eq!(
            next_epoch_protocol_version,
            get_protocol_version_internal(
                next_epoch_protocol_version,
                client_protocol_version,
                schedule,
            )
        );

        // Several upgrades happened before the scheduled time. Announce only the currently supported protocol version.
        let next_epoch_protocol_version = client_protocol_version + 2;
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                next_epoch_protocol_version,
                client_protocol_version,
                schedule,
            )
        );
    }

    #[test]
    fn test_after_scheduled_time() {
        let client_protocol_version = 100;
        let schedule =
            ProtocolUpgradeVotingSchedule::from_env_or_str("1900-01-01 00:00:00").unwrap();

        // Regardless of the protocol version of the next epoch, return the version supported by the client.
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version - 2,
                client_protocol_version,
                schedule,
            )
        );
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version,
                client_protocol_version,
                schedule,
            )
        );
        assert_eq!(
            client_protocol_version,
            get_protocol_version_internal(
                client_protocol_version + 2,
                client_protocol_version,
                schedule,
            )
        );
    }

    #[test]
    fn test_parse() {
        assert!(ProtocolUpgradeVotingSchedule::from_env_or_str("2001-02-03 23:59:59").is_ok());
        assert!(ProtocolUpgradeVotingSchedule::from_env_or_str("123").is_err());
    }

    #[test]
    fn test_is_in_future() {
        assert!(ProtocolUpgradeVotingSchedule::from_env_or_str("2999-02-03 23:59:59")
            .unwrap()
            .is_in_future());
        assert!(!ProtocolUpgradeVotingSchedule::from_env_or_str("1999-02-03 23:59:59")
            .unwrap()
            .is_in_future());
    }

    #[test]
    fn test_env_overwrite() {
        // The immediate protocol upgrade needs to be set for this test to pass in
        // the release branch where the protocol upgrade date is set.
        std::env::set_var("NEAR_TESTS_IMMEDIATE_PROTOCOL_UPGRADE", "1");

        assert_eq!(
            ProtocolUpgradeVotingSchedule::from_env_or_str("2999-02-03 23:59:59").unwrap(),
            ProtocolUpgradeVotingSchedule::default()
        );
    }
}
*/
