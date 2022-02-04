//! This module implements the `catalog topic` CLI subcommand

use futures::{stream::FuturesUnordered, StreamExt};
use thiserror::Error;

use crate::clap_blocks::catalog_dsn::CatalogDsnConfig;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Error connecting to IOx: {0}")]
    ConnectionError(#[from] influxdb_iox_client::connection::Error),

    #[error("Error updating catalog: {0}")]
    UpdateCatalogError(#[from] iox_catalog::interface::Error),

    #[error("Client error: {0}")]
    ClientError(#[from] influxdb_iox_client::error::Error),
}

/// Manage IOx chunks
#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    command: Command,
}

/// Create or update a topic
#[derive(Debug, clap::Parser)]
struct Update {
    #[clap(flatten)]
    catalog_dsn: CatalogDsnConfig,

    /// The name(s) of the topic(s) to update
    topic_names: Vec<String>,
}

/// All possible subcommands for topic
#[derive(Debug, clap::Parser)]
enum Command {
    Update(Update),
}

pub async fn command(config: Config) -> Result<(), Error> {
    match config.command {
        Command::Update(update) => {
            // for each topic name given, update the catalog, await and collect the response, print
            // the IDs and return on error
            let catalog = update.catalog_dsn.get_catalog("cli").await?;
            let topics_repo = catalog.kafka_topics();
            update
                .topic_names
                .iter()
                .map(|n| async move { topics_repo.create_or_get(n).await })
                .collect::<FuturesUnordered<_>>()
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?
                .iter()
                .for_each(|t| {
                    println!("{}", t.id);
                });
            Ok(())
        }
    }
}
