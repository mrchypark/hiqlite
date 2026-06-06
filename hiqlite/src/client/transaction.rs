use crate::client::stream::{ClientStreamReq, ClientTransactionPayload};
use crate::network::api::ApiStreamResponsePayload;
use crate::store::state_machine::sqlite::state_machine::{
    Query, QueryWrite, RaftSerializedTimestamp, RaftSerializedTimestampTransaction,
};
use crate::{Client, Error, Params, Response};
use std::borrow::Cow;
use tokio::sync::oneshot;

impl Client {
    /// Takes multiple queries and executes all of them in a single transaction.
    ///
    /// The transaction will be rolled back if any query returns an error.
    ///
    /// ```rust, notest
    /// let sql = "INSERT INTO test (id, num, description) VALUES ($1, $2, $3)";
    /// let res = client
    ///     .txn([
    ///         (sql, params!("id2", 345, "my description for 2. row")),
    ///         (sql, params!("id3", 678, "my description for 3. row")),
    ///         (sql, params!("id4", 999, "my description for 4. row")),
    ///     ])
    ///     .await;
    ///
    /// // From a transaction, you get one result and many smaller ones.
    /// // The first result is for the transaction commit itself
    /// assert!(res.is_ok());
    ///
    /// // The inner value is a Vec<Result<_>> contain a result for each single execute in the
    /// // exact same order as they were provided.
    /// for inner_res in res? {
    ///     let rows_affected = inner_res?;
    ///     assert_eq!(rows_affected, 1);
    /// }
    /// ```
    pub async fn txn<C, Q>(&self, sql: Q) -> Result<Vec<Result<usize, Error>>, Error>
    where
        Q: IntoIterator<Item = (C, Params)>,
        C: Into<Cow<'static, str>>,
    {
        let queries: Vec<Query> = sql
            .into_iter()
            .map(|(q, params)| Query {
                sql: q.into(),
                params,
            })
            .collect();

        match self.txn_execute(queries.clone()).await {
            Ok(res) => Ok(res),
            Err(err) => {
                if self
                    .was_leader_update_error(&err, &self.inner.leader_db, &self.inner.tx_client_db)
                    .await
                {
                    self.txn_execute(queries).await
                } else {
                    Err(err)
                }
            }
        }
    }

    #[inline(always)]
    pub(crate) async fn txn_execute(
        &self,
        queries: Vec<Query>,
    ) -> Result<Vec<Result<usize, Error>>, Error> {
        if let Some(state) = self.is_leader_db_with_state().await {
            let res = state
                .raft_db
                .raft
                .client_write(QueryWrite::Transaction(queries))
                .await?;
            let resp: Response = res.data;
            match resp {
                Response::Transaction(res) => res,
                _ => unreachable!(),
            }
        } else {
            let (ack, rx) = oneshot::channel();
            self.inner
                .tx_client_db
                .send_async(ClientStreamReq::Transaction(ClientTransactionPayload {
                    request_id: self.new_request_id(),
                    queries,
                    ack,
                }))
                .await
                .map_err(|err| Error::Error(err.to_string().into()))?;
            let res = rx
                .await
                .expect("To always receive an answer from Client Stream Manager")?;
            match res {
                ApiStreamResponsePayload::Transaction(res) => res,
                _ => unreachable!(),
            }
        }
    }

    /// Takes multiple queries and executes all of them in a single Raft write transaction.
    ///
    /// The returned [`RaftSerializedTimestampTransaction`] contains the SQL transaction result
    /// and a wall-clock Unix millisecond sample captured at Hiqlite's write-admission boundary
    /// and serialized into the Raft command. Every replica replays the same serialized value, and
    /// the SQL transaction can bind it via
    /// [`Param::raft_serialized_unix_ms`](crate::Param::raft_serialized_unix_ms). The timestamp is
    /// returned even when the SQL transaction itself rolls back with an error after the Raft write
    /// has been admitted and applied.
    ///
    /// This timestamp is not a monotonic cluster time, is not safe against clock skew or
    /// wall-clock rollback, and is not derived from the Raft log index. The returned Raft term
    /// and log index identify the applied log entry; they do not make the timestamp monotonic.
    pub async fn txn_with_raft_serialized_timestamp<C, Q>(
        &self,
        sql: Q,
    ) -> Result<RaftSerializedTimestampTransaction, Error>
    where
        Q: IntoIterator<Item = (C, Params)>,
        C: Into<Cow<'static, str>>,
    {
        let queries: Vec<Query> = sql
            .into_iter()
            .map(|(q, params)| Query {
                sql: q.into(),
                params,
            })
            .collect();

        match self
            .txn_with_raft_serialized_timestamp_execute(queries.clone())
            .await
        {
            Ok(res) => Ok(res),
            Err(err) => {
                if self
                    .was_leader_update_error(&err, &self.inner.leader_db, &self.inner.tx_client_db)
                    .await
                {
                    self.txn_with_raft_serialized_timestamp_execute(queries)
                        .await
                } else {
                    Err(err)
                }
            }
        }
    }

    #[inline(always)]
    pub(crate) async fn txn_with_raft_serialized_timestamp_execute(
        &self,
        queries: Vec<Query>,
    ) -> Result<RaftSerializedTimestampTransaction, Error> {
        if let Some(state) = self.is_leader_db_with_state().await {
            let res = state
                .raft_db
                .raft
                .client_write(QueryWrite::TransactionWithRaftSerializedTimestamp {
                    queries,
                    unix_ms: RaftSerializedTimestamp::now_unix_ms(),
                })
                .await?;
            let resp: Response = res.data;
            match resp {
                Response::TransactionWithRaftSerializedTimestamp(res) => Ok(res),
                _ => unreachable!(),
            }
        } else {
            let (ack, rx) = oneshot::channel();
            self.inner
                .tx_client_db
                .send_async(ClientStreamReq::TransactionWithRaftSerializedTimestamp(
                    ClientTransactionPayload {
                        request_id: self.new_request_id(),
                        queries,
                        ack,
                    },
                ))
                .await
                .map_err(|err| Error::Error(err.to_string().into()))?;
            let res = rx
                .await
                .expect("To always receive an answer from Client Stream Manager")?;
            match res {
                ApiStreamResponsePayload::TransactionWithRaftSerializedTimestamp(res) => res,
                _ => unreachable!(),
            }
        }
    }
}
