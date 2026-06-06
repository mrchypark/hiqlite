use crate::execute_query::TestData;
use crate::start::SECRET_API;
use crate::{Cache, check, log, start};
use chrono::Utc;
use hiqlite::macros::params;
use hiqlite::{Client, Error, Lock, Param};
use std::time::Duration;
use tokio::{task, time};

pub async fn test_remote_only_client() -> Result<(), Error> {
    log("Make sure remote clients work fine with any member node, even if none leader");

    let nodes = start::nodes()
        .into_iter()
        .map(|n| n.addr_api)
        .collect::<Vec<_>>();

    let client_1 =
        Client::remote(nodes.clone(), false, false, SECRET_API.to_string(), false).await?;
    check_client(&client_1, 1).await?;

    let client_2 = Client::remote(nodes, false, false, SECRET_API.to_string(), false).await?;
    check_client(&client_2, 2).await?;

    log("Make sure remote clients can skip leader discovery with a single API endpoint");
    let single_endpoint_nodes = vec![start::nodes()[0].addr_api.clone()];
    let single_endpoint_client = Client::remote(
        single_endpoint_nodes,
        false,
        false,
        SECRET_API.to_string(),
        true,
    )
    .await?;
    check_client(&single_endpoint_client, 3).await?;

    log("Test Listen / Notify with remote clients");
    let msg = TestData {
        id: 12345,
        ts: Utc::now().timestamp(),
        description: Some("Some Message".to_string()),
    };
    client_1.notify(&msg).await?;

    let res = client_1.listen::<TestData>().await?;
    assert_eq!(res, msg);
    let res = client_2.listen::<TestData>().await?;
    assert_eq!(res, msg);

    Ok(())
}

async fn check_client(client: &Client, id: u64) -> Result<(), Error> {
    check::is_client_db_healthy(&client, Some(id)).await?;

    log(format!("Test remote client {} database", id));

    // single execute / query
    let data = TestData {
        id: 1337,
        ts: Utc::now().timestamp(),
        description: Some("My Remote Row".to_string()),
    };
    let rows_affected = client
        .execute(
            "INSERT INTO test VALUES ($1, $2, $3)",
            params!(data.id, data.ts, data.description.clone()),
        )
        .await?;
    assert_eq!(rows_affected, 1);

    let res: TestData = client
        .query_map_one("SELECT * FROM test WHERE id = $1", params!(data.id))
        .await?;
    assert_eq!(res.id, data.id);
    assert_eq!(res.ts, data.ts);
    assert_eq!(res.description, data.description);

    let rows_affected = client
        .execute("DELETE FROM test WHERE id = $1", params!(data.id))
        .await?;
    assert_eq!(rows_affected, 1);

    let res: Option<TestData> = client
        .query_map_one("SELECT * FROM test WHERE id = $1", params!(data.id))
        .await
        .ok();
    assert!(res.is_none());

    // transaction

    let sql = "INSERT INTO test VALUES ($1, $2, $3)";
    let now = Utc::now().timestamp();
    let results = client
        .txn([
            (sql, params!(1001, now, "Transaction Data id 1001")),
            (sql, params!(1002, now, "Transaction Data id 1002")),
        ])
        // The first result returned is for the whole transaction commit
        .await?;
    assert_eq!(results.iter().len(), 2);

    for res in results {
        // each result in the returned vector is for
        // the single queries in the exact same order
        assert!(res.is_ok());
    }

    let data: Vec<TestData> = client
        .query_map("SELECT * FROM test WHERE id >= $1", params!(1001))
        .await?;
    assert_eq!(data.len(), 2);

    assert_eq!(data[0].id, 1001);
    assert_eq!(data[0].ts, now);
    assert_eq!(
        data[0].description.as_deref(),
        Some("Transaction Data id 1001")
    );

    assert_eq!(data[1].id, 1002);
    assert_eq!(data[1].ts, now);
    assert_eq!(
        data[1].description.as_deref(),
        Some("Transaction Data id 1002")
    );

    let remote_id = 2000 + id as i64;
    let timestamp_txn = client
        .txn_with_raft_serialized_timestamp([(
            sql,
            params!(
                remote_id,
                Param::raft_serialized_unix_ms(),
                format!("Remote Raft Serialized Timestamp Data id {remote_id}")
            ),
        )])
        .await?;
    let timestamp = timestamp_txn.timestamp;
    let results = timestamp_txn.result?;
    assert_eq!(results.len(), 1);
    assert!(results[0].is_ok());
    assert!(timestamp.unix_ms > 0);
    assert!(timestamp.raft_log_index > 0);

    let data: TestData = client
        .query_map_one("SELECT * FROM test WHERE id = $1", params!(remote_id))
        .await?;
    assert_eq!(data.id, remote_id);
    assert_eq!(data.ts, timestamp.unix_ms);
    assert_eq!(
        data.description.as_deref(),
        Some(format!("Remote Raft Serialized Timestamp Data id {remote_id}").as_str())
    );

    let failed_timestamp_txn = client
        .txn_with_raft_serialized_timestamp([(
            sql,
            params!(
                remote_id,
                Param::raft_serialized_unix_ms(),
                format!("Failed Remote Raft Serialized Timestamp Data id {remote_id}")
            ),
        )])
        .await?;
    assert!(failed_timestamp_txn.timestamp.unix_ms > 0);
    assert!(failed_timestamp_txn.timestamp.raft_log_index > timestamp.raft_log_index);
    assert!(failed_timestamp_txn.result.is_err());

    let rows_affected = client
        .execute("DELETE FROM test WHERE id = $1", params!(remote_id))
        .await?;
    assert_eq!(rows_affected, 1);

    // batch
    let results = client
        .batch(
            r#"
        DELETE FROM test WHERE id = 1001;
        DELETE FROM test WHERE id = 1002;
        "#,
        )
        .await?;

    for res in results {
        let rows_affected = res?;
        assert_eq!(rows_affected, 1);
    }

    let data: Vec<TestData> = client
        .query_map("SELECT * FROM test WHERE id >= $1", params!(1001))
        .await?;
    assert_eq!(data.len(), 0);

    log(format!("Test remote client {} cache", id));
    let key = "remote_key";
    let value = "remote Value";
    client
        .put(Cache::One, key, &value.to_string(), None)
        .await?;

    let v: String = client.get(Cache::One, key).await?.unwrap();
    assert_eq!(&v, value);

    client.delete(Cache::One, key).await?;

    let v: Option<String> = client.get(Cache::One, key).await?;
    assert!(v.is_none());

    log(format!("Test remote client {} locks", id));
    let lock = client.lock("remote").await?;

    let cl = client.clone();
    let handle = task::spawn(async move {
        let lock = cl.lock("remote").await?;
        Ok::<Lock, Error>(lock)
    });

    time::sleep(Duration::from_millis(100)).await;
    assert!(!handle.is_finished());

    drop(lock);
    let _lock_handle = handle.await??;

    Ok(())
}
