use std::time::Duration;

use structopt::StructOpt;

use influxdb_iox_client::{connection::Connection, management::generated_types::ChunkStorage};

pub type Result<T, E = String> = std::result::Result<T, E>;
use crate::error::StringifyError;

const MAX_OPERATION_WAIT_SECS: u64 = 10;

/// Ensure that all chunks have been loaded into the read buffer (rather than parquet)
#[derive(Debug, StructOpt)]
pub struct LoadReadBuffer {
    /// The database name for which to load
    db: String,
}

impl LoadReadBuffer {
    pub async fn execute(&self, connection: Connection) -> Result<()> {
        println!(
            "Ensuring all chunks are loaded into read buffer for {}",
            self.db
        );

        let mut client = influxdb_iox_client::management::Client::new(connection.clone());

        let chunks = client
            .list_chunks(&self.db)
            .await
            .context("Listing chunks")?;

        //println!("{} Available chunks: {:#?}", chunks.len(), chunks);
        let mut jobs = vec![];

        println!("Checking {} Available chunks", chunks.len());
        for chunk in chunks {
            let chunk_name = format!(
                "Chunk({}:{}:{})",
                chunk.table_name, chunk.partition_key, "ID"
            );
            print!("{} ", chunk_name);
            let storage = ChunkStorage::from_i32(chunk.storage);

            match storage {
                Some(ChunkStorage::ObjectStoreOnly) => {
                    print!("Loading from ObjectStoreOnly");
                    let load = client
                        .load_partition_chunk(
                            &self.db,
                            chunk.table_name,
                            chunk.partition_key,
                            chunk.id,
                        )
                        .await
                        .context("Loading partition chunk")?;
                    println!("Started operation: {}", load.operation.name);
                    jobs.push(load);
                    //println!("Loaded chunk: {:#?}", load);
                }
                Some(ChunkStorage::ReadBufferAndObjectStore) => {
                    println!("Chunk in desired state");
                }
                Some(other) => {
                    println!("WARN: skipping invalid state: {:#?}", other);
                }
                None => {
                    println!("WARN: skipping chunk unknown state {:#?}", storage);
                }
            };
        }

        if !jobs.is_empty() {
            let mut counter = 0;
            let mut operation_client = influxdb_iox_client::operations::Client::new(connection);
            print!("Waiting for {} jobs to complete", jobs.len());
            for job in jobs {
                let id = job.operation.id();
                let timeout = Duration::from_secs(MAX_OPERATION_WAIT_SECS);
                operation_client.wait_operation(id, Some(timeout))
                .await
                .context(&format!("waiting for operation to complete:{:#?}", job))?;
                print!(".");
                if (counter % 10) == 0 {
                    print!("{}", id);
                }
                counter += 1
            }
            println!(" Done");
        }
        Ok(())
    }
}
