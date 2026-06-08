pub mod command;
pub mod db;
pub mod entry;
pub mod error;
pub mod executor;
pub mod stats;
pub mod storage;

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use rustkv_protocol::resp::{RespFrame, RespValue};

    use crate::command::Command;
    use crate::db::{to_json_string, Database, ShardedDatabase};
    use crate::error::KvError;
    use crate::executor::execute_command;
    use crate::stats::ServerStats;
    use crate::storage::StorageEngine;

    fn command_frame(args: &[&'static [u8]]) -> RespFrame<'static> {
        RespFrame::Array(args.iter().map(|arg| RespFrame::BulkString(arg)).collect())
    }

    #[test]
    fn database_set_get_exists_del_and_flushdb_work() {
        let mut db = Database::new();

        db.set(String::from("alpha"), b"one".to_vec(), None)
            .unwrap();
        db.set(String::from("beta"), b"two".to_vec(), None).unwrap();

        assert_eq!(db.len(), 2);
        assert_eq!(db.get("alpha").unwrap(), Some(b"one".to_vec()));
        assert!(db.exists("beta").unwrap());
        assert_eq!(db.del(&[String::from("alpha")]).unwrap(), 1);
        assert_eq!(db.get("alpha").unwrap(), None);
        assert_eq!(db.len(), 1);

        db.flushdb().unwrap();
        assert!(db.is_empty());
    }

    #[test]
    fn database_lazy_deletes_expired_keys_on_get_exists_and_ttl() {
        let mut db = Database::new();

        db.set(
            String::from("temp"),
            b"value".to_vec(),
            Some(Duration::from_millis(10)),
        )
        .unwrap();

        std::thread::sleep(Duration::from_millis(25));

        assert_eq!(db.get("temp").unwrap(), None);
        assert_eq!(db.len(), 0);

        db.set(
            String::from("temp"),
            b"value".to_vec(),
            Some(Duration::from_millis(10)),
        )
        .unwrap();
        std::thread::sleep(Duration::from_millis(25));

        assert!(!db.exists("temp").unwrap());
        assert_eq!(db.len(), 0);

        db.set(
            String::from("temp"),
            b"value".to_vec(),
            Some(Duration::from_millis(10)),
        )
        .unwrap();
        std::thread::sleep(Duration::from_millis(25));

        assert_eq!(db.ttl("temp").unwrap(), -2);
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn database_expire_ttl_keys_and_remove_expired_work() {
        let mut db = Database::new();

        db.set(String::from("forever"), b"v".to_vec(), None)
            .unwrap();
        db.set(String::from("soon"), b"v".to_vec(), None).unwrap();

        assert_eq!(db.ttl("missing").unwrap(), -2);
        assert_eq!(db.ttl("forever").unwrap(), -1);
        assert!(db.expire(String::from("soon"), 1).unwrap());

        let ttl = db.ttl("soon").unwrap();
        assert!((0..=1).contains(&ttl));

        assert_eq!(
            db.keys().unwrap(),
            vec![String::from("forever"), String::from("soon")]
        );

        db.set(
            String::from("expired"),
            b"v".to_vec(),
            Some(Duration::from_millis(10)),
        )
        .unwrap();
        std::thread::sleep(Duration::from_millis(25));

        assert_eq!(db.remove_expired(), 1);
        assert!(!db.exists("expired").unwrap());
    }

    #[test]
    fn command_parser_handles_arrays_case_insensitively() {
        let resp = RespFrame::Array(vec![
            RespFrame::BulkString(b"set"),
            RespFrame::BulkString(b"name"),
            RespFrame::BulkString(b"alice"),
            RespFrame::BulkString(b"EX"),
            RespFrame::BulkString(b"5"),
        ]);

        let command = Command::from_resp(resp).unwrap();

        assert_eq!(
            command,
            Command::Set {
                key: String::from("name"),
                value: b"alice".to_vec(),
                expire: Some(Duration::from_secs(5)),
            }
        );

        let resp = RespFrame::Array(vec![
            RespFrame::BulkString(b"DeL"),
            RespFrame::BulkString(b"a"),
            RespFrame::BulkString(b"b"),
        ]);

        assert_eq!(
            Command::from_resp(resp).unwrap(),
            Command::Del {
                keys: vec![String::from("a"), String::from("b")]
            }
        );
    }

    #[test]
    fn command_parser_rejects_invalid_command_shapes() {
        assert!(Command::from_resp(RespFrame::Integer(1)).is_err());

        let resp = RespFrame::Array(vec![RespFrame::BulkString(b"GET")]);
        assert!(Command::from_resp(resp).is_err());

        let resp = RespFrame::Array(vec![RespFrame::BulkString(b"NOPE")]);
        assert_eq!(
            Command::from_resp(resp).unwrap(),
            Command::Unknown(String::from("NOPE"))
        );
    }

    #[test]
    fn command_parser_handles_core_commands() {
        assert_eq!(
            Command::from_resp(RespFrame::Array(vec![RespFrame::BulkString(b"PING")])).unwrap(),
            Command::Ping
        );
        assert_eq!(
            Command::from_resp(RespFrame::Array(vec![
                RespFrame::BulkString(b"GET"),
                RespFrame::BulkString(b"k")
            ]))
            .unwrap(),
            Command::Get {
                key: String::from("k")
            }
        );
        assert_eq!(
            Command::from_resp(RespFrame::Array(vec![
                RespFrame::BulkString(b"EXPIRE"),
                RespFrame::BulkString(b"k"),
                RespFrame::BulkString(b"10")
            ]))
            .unwrap(),
            Command::Expire {
                key: String::from("k"),
                seconds: 10
            }
        );
        assert_eq!(
            Command::from_resp(RespFrame::Array(vec![
                RespFrame::BulkString(b"TTL"),
                RespFrame::BulkString(b"k")
            ]))
            .unwrap(),
            Command::Ttl {
                key: String::from("k")
            }
        );
        assert_eq!(
            Command::from_resp(RespFrame::Array(vec![
                RespFrame::BulkString(b"SET"),
                RespFrame::BulkString(b"k"),
                RespFrame::BulkString(b"v"),
                RespFrame::BulkString(b"PXAT"),
                RespFrame::BulkString(b"123456789")
            ]))
            .unwrap(),
            Command::SetPxAt {
                key: String::from("k"),
                value: b"v".to_vec(),
                timestamp_ms: 123456789
            }
        );
        assert_eq!(
            Command::from_resp(RespFrame::Array(vec![
                RespFrame::BulkString(b"EXPIREAT"),
                RespFrame::BulkString(b"k"),
                RespFrame::BulkString(b"123456789")
            ]))
            .unwrap(),
            Command::ExpireAt {
                key: String::from("k"),
                timestamp_ms: 123456789
            }
        );
    }

    #[test]
    fn command_from_resp_parses_ping_set_get_del_expire_and_ttl() {
        assert_eq!(
            Command::from_resp(command_frame(&[b"PING"])).unwrap(),
            Command::Ping
        );

        assert_eq!(
            Command::from_resp(command_frame(&[b"SET", b"course", b"rust"])).unwrap(),
            Command::Set {
                key: String::from("course"),
                value: b"rust".to_vec(),
                expire: None,
            }
        );

        assert_eq!(
            Command::from_resp(command_frame(&[b"GET", b"course"])).unwrap(),
            Command::Get {
                key: String::from("course")
            }
        );

        assert_eq!(
            Command::from_resp(command_frame(&[b"DEL", b"course", b"token"])).unwrap(),
            Command::Del {
                keys: vec![String::from("course"), String::from("token")]
            }
        );

        assert_eq!(
            Command::from_resp(command_frame(&[b"EXPIRE", b"token", b"30"])).unwrap(),
            Command::Expire {
                key: String::from("token"),
                seconds: 30,
            }
        );

        assert_eq!(
            Command::from_resp(command_frame(&[b"TTL", b"token"])).unwrap(),
            Command::Ttl {
                key: String::from("token")
            }
        );
    }

    #[test]
    fn command_from_resp_returns_invalid_command_for_wrong_arity() {
        let error = Command::from_resp(command_frame(&[b"GET"])).unwrap_err();

        match error {
            KvError::InvalidCommand(message) => {
                assert!(
                    message.contains("GET expects"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected InvalidCommand, got {other:?}"),
        }

        let error = Command::from_resp(command_frame(&[b"SET", b"only_key"])).unwrap_err();

        match error {
            KvError::InvalidCommand(message) => {
                assert!(
                    message.contains("SET expects"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected InvalidCommand, got {other:?}"),
        }
    }

    #[test]
    fn database_set_get_del_exists_keys_expire_ttl_lazy_delete_and_flushdb() {
        let mut db = Database::new();

        db.set(String::from("b"), b"two".to_vec(), None).unwrap();
        db.set(String::from("a"), b"one".to_vec(), None).unwrap();
        db.set(String::from("temp"), b"gone".to_vec(), None)
            .unwrap();

        assert_eq!(db.get("a").unwrap(), Some(b"one".to_vec()));
        assert!(db.exists("b").unwrap());
        assert_eq!(
            db.keys().unwrap(),
            vec![String::from("a"), String::from("b"), String::from("temp")]
        );

        assert_eq!(db.del(&[String::from("b")]).unwrap(), 1);
        assert!(!db.exists("b").unwrap());

        assert!(db.expire(String::from("temp"), 1).unwrap());
        assert!(matches!(db.ttl("temp").unwrap(), 0..=1));

        db.set(
            String::from("lazy"),
            b"value".to_vec(),
            Some(Duration::from_millis(10)),
        )
        .unwrap();
        std::thread::sleep(Duration::from_millis(25));

        assert_eq!(db.get("lazy").unwrap(), None);
        assert!(!db.exists("lazy").unwrap());
        assert_eq!(db.ttl("lazy").unwrap(), -2);

        db.flushdb().unwrap();
        assert!(db.is_empty());
    }

    #[tokio::test]
    async fn sharded_database_routes_keys_internally() {
        let db = ShardedDatabase::new(4);

        assert_eq!(db.shard_count(), 4);

        db.set(String::from("name"), b"rust".to_vec(), None)
            .await
            .result
            .unwrap();
        db.set(String::from("course"), b"kv".to_vec(), None)
            .await
            .result
            .unwrap();

        assert_eq!(db.len(), 2);
        assert_eq!(db.get("name").await.result.unwrap(), Some(b"rust".to_vec()));
        assert_eq!(
            db.keys().await.unwrap(),
            vec![String::from("course"), String::from("name")]
        );

        let removed = db
            .del(&[String::from("name"), String::from("missing")])
            .await
            .result
            .unwrap();

        assert_eq!(removed, 1);
        assert_eq!(db.len(), 1);
    }

    #[tokio::test]
    async fn sharded_database_snapshot_exports_only_live_entries() {
        let db = ShardedDatabase::new(4);

        db.set(String::from("alive"), b"value".to_vec(), None)
            .await
            .result
            .unwrap();
        db.set(
            String::from("expired"),
            b"gone".to_vec(),
            Some(Duration::from_millis(10)),
        )
        .await
        .result
        .unwrap();

        tokio::time::sleep(Duration::from_millis(25)).await;

        let snapshot = db.snapshot_entries().await;

        assert_eq!(snapshot.expired_count, 1);
        assert_eq!(snapshot.key_count, 1);
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].key, "alive");
        assert_eq!(snapshot.entries[0].value, b"value".to_vec());
        assert_eq!(snapshot.entries[0].expire_at_ms, None);
        assert_eq!(db.len(), 1);
    }

    #[tokio::test]
    async fn sharded_database_snapshot_converts_ttl_to_unix_ms() {
        let db = ShardedDatabase::new(4);

        db.set(
            String::from("ttl"),
            b"value".to_vec(),
            Some(Duration::from_secs(5)),
        )
        .await
        .result
        .unwrap();

        let before_snapshot_ms = unix_now_ms();
        let snapshot = db.snapshot_entries().await;
        let after_snapshot_ms = unix_now_ms();

        assert_eq!(snapshot.entries.len(), 1);
        let expire_at_ms = snapshot.entries[0]
            .expire_at_ms
            .expect("TTL key should be exported with an absolute expiry");

        assert!(expire_at_ms >= before_snapshot_ms);
        assert!(expire_at_ms <= after_snapshot_ms + 5_000);
    }

    #[test]
    fn stats_serializes_as_a_snapshot() {
        let stats = ServerStats::new();
        stats.incr_total_commands();
        stats.incr_get_count();
        stats.set_key_count(3);

        let json = to_json_string(&stats).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(value["total_commands"], 1);
        assert_eq!(value["get_count"], 1);
        assert_eq!(value["key_count"], 3);
        assert_eq!(value["role"], "standalone");
        assert_eq!(value["aof_enabled"], false);
        assert_eq!(value["addr"], "unknown");
        assert!(value["server_version"]
            .as_str()
            .is_some_and(|text| !text.is_empty()));
        assert!(value["uptime_seconds"].as_u64().is_some());
        assert!(value["memory_estimate_bytes"].as_u64().is_some());
        assert!(value["max_frame_size"].as_u64().is_some());
    }

    #[tokio::test]
    async fn executor_runs_commands_and_updates_stats() {
        let db = ShardedDatabase::default();
        let stats = ServerStats::new();

        let response = execute_command(
            Command::Set {
                key: String::from("lang"),
                value: b"rust".to_vec(),
                expire: None,
            },
            &db,
            &stats,
        )
        .await;

        assert_eq!(response, RespValue::SimpleString(String::from("OK")));

        let response = execute_command(
            Command::Get {
                key: String::from("lang"),
            },
            &db,
            &stats,
        )
        .await;

        assert_eq!(response, RespValue::BulkString(b"rust".to_vec()));

        let response = execute_command(Command::Info, &db, &stats).await;
        let RespValue::BulkString(bytes) = response else {
            panic!("INFO must return a bulk string");
        };

        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(value["server_version"]
            .as_str()
            .is_some_and(|text| !text.is_empty()));
        assert_eq!(value["role"], "standalone");
        assert_eq!(value["aof_enabled"], false);
        assert_eq!(value["addr"], "unknown");
        assert_eq!(value["max_frame_size"], 0);
        assert_eq!(value["total_commands"], 2);
        assert_eq!(value["set_count"], 1);
        assert_eq!(value["get_count"], 1);
        assert_eq!(value["key_count"], 1);
        assert!(value["memory_estimate_bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0));
    }

    #[tokio::test]
    async fn info_command_does_not_increment_total_commands() {
        let db = ShardedDatabase::default();
        let stats = ServerStats::new();

        let response = execute_command(Command::Info, &db, &stats).await;
        assert!(matches!(response, RespValue::BulkString(_)));

        let response = execute_command(Command::Info, &db, &stats).await;
        let RespValue::BulkString(bytes) = response else {
            panic!("INFO must return a bulk string");
        };

        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["total_commands"], 0);
    }

    fn unix_now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}
