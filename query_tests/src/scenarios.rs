use async_trait::async_trait;
use server::Db;
use std::sync::Arc;

#[derive(Debug)]
pub struct OneDeleteSimpleExprOneChunkDeleteAll;
#[async_trait]
impl DbSetup for OneDeleteSimpleExprOneChunkDeleteAll {
    async fn make(&self) -> Vec<DbScenario> {
        alpha().await
    }
}

#[derive(Debug)]
pub struct OneDeleteSimpleExprOneChunk;
#[async_trait]
impl DbSetup for OneDeleteSimpleExprOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        alpha().await
    }
}

#[derive(Debug)]
pub struct NoDeleteOneChunk;
#[async_trait]
impl DbSetup for NoDeleteOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        alpha().await
    }
}

#[derive(Debug)]
pub struct OneDeleteMultiExprsOneChunk;
#[async_trait]
impl DbSetup for OneDeleteMultiExprsOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        alpha().await
    }
}

#[derive(Debug)]
pub struct TwoDeletesMultiExprsOneChunk;
#[async_trait]
impl DbSetup for TwoDeletesMultiExprsOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        alpha().await
    }
}

#[derive(Debug)]
pub struct ThreeDeleteThreeChunks;
#[async_trait]
impl DbSetup for ThreeDeleteThreeChunks {
    async fn make(&self) -> Vec<DbScenario> {
        beta().await;
        beta().await;
        beta().await;
        beta().await;
        beta().await;
        beta().await;

        gamma().await;

        vec![]
    }
}

pub async fn alpha() -> Vec<DbScenario> {
    let mut scenarios = vec![];
    scenarios.push(alpha2().await);
    scenarios
}

pub async fn alpha2() -> DbScenario {
    todo!()
}

pub async fn beta() -> DbScenario {
    todo!()
}

pub async fn gamma() -> Vec<DbScenario> {
    vec![]
}

#[derive(Debug)]
pub struct DbScenario {
    pub db: Arc<Db>,
}

#[async_trait]
pub trait DbSetup: Send + Sync {
    async fn make(&self) -> Vec<DbScenario>;
}
