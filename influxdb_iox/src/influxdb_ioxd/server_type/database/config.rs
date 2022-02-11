use generated_types::influxdata::iox::management;

use async_trait::async_trait;
use data_types::server_id::ServerId;
use generated_types::google::FieldViolation;
use server::{
    config::{ConfigProvider, StdError},
    rules::{PersistedDatabaseRules, ProvidedDatabaseRules},
};
use snafu::{OptionExt, ResultExt, Snafu};
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("error fetching server config: {}", source))]
    FetchBytes { source: std::io::Error },

    #[snafu(display("error decoding server config: {}", source))]
    Decode { source: serde_json::Error },

    #[snafu(display("invalid server config: {}", source))]
    Invalid { source: FieldViolation },

    #[snafu(display("rules not found in config file"))]
    RulesMissingConfigFile,

    #[snafu(display("config is immutable"))]
    ImmutableConfig,
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A loader for [`ServerConfigFile`]
#[derive(Debug)]
pub struct ServerConfigFile {
    path: String,
}

impl ServerConfigFile {
    pub fn new(path: String) -> Self {
        Self { path }
    }

    async fn load(&self) -> Result<Vec<PersistedDatabaseRules>> {
        let bytes = tokio::fs::read(&self.path).await.context(FetchBytesSnafu)?;

        let proto: management::v1::ServerConfigFile =
            serde_json::from_slice(bytes.as_slice()).context(DecodeSnafu)?;

        proto
            .databases
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()
            .context(InvalidSnafu)
    }
}

#[async_trait]
impl ConfigProvider for ServerConfigFile {
    async fn fetch_server_config(
        &self,
        _server_id: ServerId,
    ) -> Result<Vec<(String, Uuid)>, StdError> {
        let databases = self.load().await?;

        let mapping = databases
            .into_iter()
            .map(|x| (x.db_name().to_string(), x.uuid()))
            .collect();

        Ok(mapping)
    }

    async fn store_server_config(
        &self,
        _server_id: ServerId,
        _config: &[(String, Uuid)],
    ) -> Result<(), StdError> {
        Err(Error::ImmutableConfig.into())
    }

    async fn fetch_rules(&self, uuid: Uuid) -> Result<ProvidedDatabaseRules, StdError> {
        // We load the file each time to pick up changes
        let databases = self.load().await?;

        let databases = databases
            .into_iter()
            .find(|d| d.uuid() == uuid)
            .map(|d| d.into_inner().1)
            .context(RulesMissingConfigFileSnafu)?;

        Ok(databases)
    }

    async fn store_rules(
        &self,
        _uuid: Uuid,
        _rules: &ProvidedDatabaseRules,
    ) -> Result<(), StdError> {
        Err(Error::ImmutableConfig.into())
    }
}
