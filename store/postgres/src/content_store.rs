//! Postgres-backed [`ContentStore`]: subgraph files in `public.ipfs_content` in the primary shard,
//! so every node that shares the primary can read what was deployed through any of them.

use async_trait::async_trait;
use diesel::sql_query;
use diesel::sql_types::{Binary, Text};
use diesel_async::RunQueryDsl;
use graph::bytes::Bytes;
use graph::ipfs::builtin::ContentStore;

use crate::ConnectionPool;

pub struct PgContentStore {
    pool: ConnectionPool,
}

impl PgContentStore {
    pub fn new(pool: ConnectionPool) -> Self {
        Self { pool }
    }
}

#[derive(diesel::QueryableByName)]
struct Content {
    #[diesel(sql_type = Binary)]
    content: Vec<u8>,
}

#[async_trait]
impl ContentStore for PgContentStore {
    async fn get(&self, cid: &str) -> anyhow::Result<Option<Bytes>> {
        let mut conn = self.pool.get().await?;
        let row = sql_query("select content from public.ipfs_content where cid = $1")
            .bind::<Text, _>(cid)
            .get_result::<Content>(&mut conn)
            .await;
        match row {
            Ok(row) => Ok(Some(Bytes::from(row.content))),
            Err(diesel::result::Error::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn put(&self, cid: &str, data: &[u8]) -> anyhow::Result<()> {
        let mut conn = self.pool.get().await?;
        sql_query(
            "insert into public.ipfs_content (cid, content) values ($1, $2) \
             on conflict (cid) do nothing",
        )
        .bind::<Text, _>(cid)
        .bind::<Binary, _>(data)
        .execute(&mut conn)
        .await?;
        Ok(())
    }
}
