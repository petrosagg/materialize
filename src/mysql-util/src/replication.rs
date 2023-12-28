// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use mysql_async::prelude::Queryable;
use mysql_async::Conn;

use crate::MySqlError;

/// Query a MySQL System Variable
pub async fn query_sys_var(conn: &mut Conn, name: &str) -> Result<String, MySqlError> {
    let value: String = conn
        .query_first(format!("SELECT @@{}", name))
        .await?
        .unwrap();
    Ok(value)
}

/// Verify a MySQL System Variable matches the expected value
async fn verify_sys_setting(
    conn: &mut Conn,
    setting: &str,
    expected: &str,
) -> Result<(), MySqlError> {
    match query_sys_var(conn, setting).await?.as_str() {
        actual if actual == expected => Ok(()),
        actual => Err(MySqlError::InvalidSystemSetting {
            setting: setting.to_string(),
            expected: expected.to_string(),
            actual: actual.to_string(),
        }),
    }
}

pub async fn ensure_full_row_binlog_format(conn: &mut Conn) -> Result<(), MySqlError> {
    verify_sys_setting(conn, "log_bin", "1").await?;
    verify_sys_setting(conn, "binlog_format", "ROW").await?;
    verify_sys_setting(conn, "binlog_row_image", "FULL").await?;
    Ok(())
}

pub async fn ensure_gtid_consistency(conn: &mut Conn) -> Result<(), MySqlError> {
    verify_sys_setting(conn, "gtid_mode", "ON").await?;
    verify_sys_setting(conn, "enforce_gtid_consistency", "ON").await?;
    Ok(())
}

// TODO: This only applies if connecting to a replica, to ensure that the replica preserves
// all transactions in the order they were committed on the primary which implies
// monotonically increasing transaction ids
pub async fn ensure_replication_commit_order(conn: &mut Conn) -> Result<(), MySqlError> {
    // This system variable was renamed between MySQL 5.7 and 8.0
    match (
        verify_sys_setting(conn, "replica_preserve_commit_order", "1").await,
        verify_sys_setting(conn, "slave_preserve_commit_order", "1").await,
    ) {
        (Ok(_), Ok(_)) => Ok(()),
        (Err(_), Ok(())) => Ok(()),
        (Ok(()), Err(_)) => Ok(()),
        (Err(e), Err(_)) => Err(e),
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GtidInterval {
    pub start: u64,
    pub end: Option<u64>,
}

/// Represents a MySQL GTID with a single Source-ID and a list of intervals
/// e.g. `3E11FA47-71CA-11E1-9E33-C80AA9429562:1-3:4:5-9`
/// If this represents a single GTID point e.g. `3E11FA47-71CA-11E1-9E33-C80AA9429562:5`
/// it will contain just one interval with one start and no end
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Gtid {
    pub uuid: uuid::Uuid,
    pub intervals: Vec<GtidInterval>,
}

/// A representation of a MySQL GTID set (returned from the `gtid_executed` & `gtid_purged` system variables)
/// e.g. `2174B383-5441-11E8-B90A-C80AA9429562:1-3, 24DA167-0C0C-11E8-8442-00059A3C7B00:1-19`
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GtidSet {
    pub gtids: Vec<Gtid>,
}

impl Gtid {
    pub fn earliest_transaction_id(&self) -> u64 {
        self.intervals
            .first()
            .unwrap_or_else(|| {
                panic!(
                    "expected at least one interval in gtid: {:?}",
                    self.intervals
                )
            })
            .start
    }

    pub fn latest_transaction_id(&self) -> u64 {
        // NOTE: Use the last interval; there might be discrete intervals if they have not been compacted yet
        // but interval values are guaranteed to be monotonically increasing as long as replica_preserve_commit_order=1
        // which is why we only care about the highest value
        let last_interval = self.intervals.last().unwrap_or_else(|| {
            panic!(
                "expected at least one interval in gtid: {:?}",
                self.intervals
            )
        });
        // If this interval is a range use the end value; if not we can use the start value
        last_interval.end.unwrap_or(last_interval.start)
    }
}

impl FromStr for GtidInterval {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let vals: Vec<u64> = s
            .split("-")
            .map(|num| num.parse::<u64>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid interval in gtid: {}: {}", s, e),
                )
            })?;
        if vals.len() == 1 {
            Ok(Self {
                start: vals[0],
                end: None,
            })
        } else if vals.len() == 2 {
            Ok(Self {
                start: vals[0],
                end: Some(vals[1]),
            })
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid gtid interval: {}", s),
            ))
        }
    }
}

impl FromStr for Gtid {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (uuid, intervals) = s.split_once(':').ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, format!("invalid gtid: {}", s))
        })?;
        let uuid = Uuid::parse_str(uuid).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid uuid in gtid: {}: {}", s, e),
            )
        })?;
        let intervals: Vec<GtidInterval> = intervals
            .split(":")
            .map(|interval_str| GtidInterval::from_str(interval_str))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { uuid, intervals })
    }
}

impl FromStr for GtidSet {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let gtids: Vec<Gtid> = s
            .split(", ")
            .map(|gtid_str| Gtid::from_str(gtid_str))
            .collect::<Result<Vec<_>, _>>()?;
        // Confirm that no uuid is repeated among the set of gtids
        // This should be guaranteed by MySQL if this value was retrieved from `gtid_executed` or `gtid_purged``,
        // but we check here just in case
        let mut uuids = std::collections::HashSet::new();
        for gtid in &gtids {
            if !uuids.insert(&gtid.uuid) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("duplicate uuid in gtid set: {}", gtid.uuid),
                ));
            }
        }
        Ok(Self { gtids })
    }
}
