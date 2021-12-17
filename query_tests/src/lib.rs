use async_trait::async_trait;
use server::Db;

struct DbScenario {
    pub db: Db,
}

#[async_trait]
trait DbSetup: Send + Sync {
    async fn make(&self) -> Vec<DbScenario>;
}

struct OneDeleteSimpleExprOneChunkDeleteAll;
#[async_trait]
impl DbSetup for OneDeleteSimpleExprOneChunkDeleteAll {
    async fn make(&self) -> Vec<DbScenario> {
        alpha().await
    }
}

struct OneDeleteSimpleExprOneChunk;
#[async_trait]
impl DbSetup for OneDeleteSimpleExprOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        alpha().await
    }
}

struct NoDeleteOneChunk;
#[async_trait]
impl DbSetup for NoDeleteOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        alpha().await
    }
}

struct OneDeleteMultiExprsOneChunk;
#[async_trait]
impl DbSetup for OneDeleteMultiExprsOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        alpha().await
    }
}

struct TwoDeletesMultiExprsOneChunk;
#[async_trait]
impl DbSetup for TwoDeletesMultiExprsOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        alpha().await
    }
}

struct ThreeDeleteThreeChunks;
#[async_trait]
impl DbSetup for ThreeDeleteThreeChunks {
    async fn make(&self) -> Vec<DbScenario> {
        alpha().await
    }
}

async fn alpha() -> Vec<DbScenario> {
    let mut scenarios = vec![];
    scenarios.push(beta().await);
    scenarios
}

async fn beta() -> DbScenario {
    todo!()
}
