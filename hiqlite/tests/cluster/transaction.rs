use crate::execute_query::TestData;
use crate::log;
use chrono::Utc;
use hiqlite::macros::params;
use hiqlite::{Client, Error, Param};
use std::time::Duration;
use tokio::time;

pub async fn test_transactions(
    client_1: &Client,
    client_2: &Client,
    client_3: &Client,
) -> Result<(), Error> {
    // we re-use the test table from the simple insert / query tests here

    log("Inserting rows with a transaction");

    let sql = "INSERT INTO test VALUES ($1, $2, $3)";
    let now = Utc::now().timestamp();
    let results = client_1
        .txn([
            (sql, params!(11, now, "Transaction Data id 11")),
            (sql, params!(12, now, "Transaction Data id 12")),
            (sql, params!(13, now, None::<Option<String>>)),
        ])
        // The first result returned is for the whole transaction commit
        .await?;
    assert_eq!(results.iter().len(), 3);

    for res in results {
        // each result in the returned vector is for
        // the single queries in the exact same order
        assert!(res.is_ok());
    }

    log("Making sure transaction data exists for client 1");
    let select = "SELECT * FROM test WHERE id >= $1";
    let data: Vec<TestData> = client_1.query_map(select, params!(11)).await?;
    assert_eq!(data.len(), 3);

    assert_eq!(data[0].id, 11);
    assert_eq!(data[0].ts, now);
    assert_eq!(
        data[0].description.as_deref(),
        Some("Transaction Data id 11")
    );

    assert_eq!(data[1].id, 12);
    assert_eq!(data[1].ts, now);
    assert_eq!(
        data[1].description.as_deref(),
        Some("Transaction Data id 12")
    );

    assert_eq!(data[2].id, 13);
    assert_eq!(data[2].ts, now);
    assert_eq!(data[2].description, None);

    log("Making sure transaction data exists for client 2");
    time::sleep(Duration::from_millis(10)).await;

    let data: Vec<TestData> = client_2.query_map(select, params!(11)).await?;
    assert_eq!(data.len(), 3);

    assert_eq!(data[0].id, 11);
    assert_eq!(data[0].ts, now);
    assert_eq!(
        data[0].description.as_deref(),
        Some("Transaction Data id 11")
    );

    assert_eq!(data[1].id, 12);
    assert_eq!(data[1].ts, now);
    assert_eq!(
        data[1].description.as_deref(),
        Some("Transaction Data id 12")
    );

    assert_eq!(data[2].id, 13);
    assert_eq!(data[2].ts, now);
    assert_eq!(data[2].description, None);

    log("Making sure transaction data exists for client 3");
    let data: Vec<TestData> = client_3.query_map(select, params!(11)).await?;
    assert_eq!(data.len(), 3);

    assert_eq!(data[0].id, 11);
    assert_eq!(data[0].ts, now);
    assert_eq!(
        data[0].description.as_deref(),
        Some("Transaction Data id 11")
    );

    assert_eq!(data[1].id, 12);
    assert_eq!(data[1].ts, now);
    assert_eq!(
        data[1].description.as_deref(),
        Some("Transaction Data id 12")
    );

    assert_eq!(data[2].id, 13);
    assert_eq!(data[2].ts, now);
    assert_eq!(data[2].description, None);

    log("Inserting rows with a raft-serialized timestamp transaction");
    let timestamp_txn = client_1
        .txn_with_raft_serialized_timestamp([
            (
                sql,
                params!(
                    91,
                    Param::raft_serialized_unix_ms(),
                    "Raft Serialized Timestamp Data id 91"
                ),
            ),
            (
                sql,
                params!(
                    92,
                    Param::raft_serialized_unix_ms(),
                    "Raft Serialized Timestamp Data id 92"
                ),
            ),
        ])
        .await?;
    let timestamp = timestamp_txn.timestamp;
    let timestamp_results = timestamp_txn.result?;
    assert_eq!(timestamp_results.len(), 2);
    for res in timestamp_results {
        assert!(res.is_ok());
    }
    assert!(timestamp.unix_ms > 0);
    assert!(timestamp.raft_term > 0);
    assert!(timestamp.raft_log_index > 0);

    let data: Vec<TestData> = client_2
        .query_map(
            "SELECT * FROM test WHERE id IN (91, 92) ORDER BY id",
            params!(),
        )
        .await?;
    assert_eq!(data.len(), 2);
    assert_eq!(data[0].id, 91);
    assert_eq!(data[0].ts, timestamp.unix_ms);
    assert_eq!(
        data[0].description.as_deref(),
        Some("Raft Serialized Timestamp Data id 91")
    );
    assert_eq!(data[1].id, 92);
    assert_eq!(data[1].ts, timestamp.unix_ms);
    assert_eq!(
        data[1].description.as_deref(),
        Some("Raft Serialized Timestamp Data id 92")
    );

    let err = client_1
        .txn([(
            sql,
            params!(
                93,
                Param::raft_serialized_unix_ms(),
                "Invalid Raft Serialized Timestamp Data id 93"
            ),
        )])
        .await
        .expect_err("raft-serialized timestamp params must fail in a regular transaction");
    let err = format!("{err:?}");
    assert!(err.contains("txn_with_raft_serialized_timestamp"));

    let failed_timestamp_txn = client_1
        .txn_with_raft_serialized_timestamp([(
            sql,
            params!(
                91,
                Param::raft_serialized_unix_ms(),
                "Failed Raft Serialized Timestamp Data id 91"
            ),
        )])
        .await?;
    assert!(failed_timestamp_txn.timestamp.unix_ms > 0);
    assert!(failed_timestamp_txn.timestamp.raft_term > 0);
    assert!(failed_timestamp_txn.timestamp.raft_log_index > timestamp.raft_log_index);
    assert!(failed_timestamp_txn.result.is_err());

    let deleted = client_1
        .execute("DELETE FROM test WHERE id IN (91, 92)", params!())
        .await?;
    assert_eq!(deleted, 2);

    Ok(())
}
