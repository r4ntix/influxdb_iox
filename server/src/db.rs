//! This module contains the main IOx Database object which has the
//! instances of the mutable buffer, read buffer, and object store

use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use ::lifecycle::select_persistable_chunks;
use async_trait::async_trait;
use parking_lot::{Mutex, RwLock};
use rand_distr::{Distribution, Poisson};
use snafu::{ensure, OptionExt, ResultExt, Snafu};

pub use ::lifecycle::{LifecycleChunk, LockableChunk, LockablePartition};
use data_types::{
    chunk_metadata::{ChunkId, ChunkLifecycleAction, ChunkOrder, ChunkSummary},
    database_rules::DatabaseRules,
    delete_predicate::DeletePredicate,
    job::Job,
    partition_metadata::{PartitionSummary, TableSummary},
    server_id::ServerId,
};
use datafusion::catalog::{catalog::CatalogProvider, schema::SchemaProvider};
use dml::{DmlDelete, DmlMeta, DmlOperation, DmlWrite};
use internal_types::mailbox::Mailbox;
use iox_object_store::IoxObjectStore;
use mutable_batch::payload::PartitionWrite;
use mutable_buffer::{ChunkMetrics as MutableBufferChunkMetrics, MBChunk};
use observability_deps::tracing::{debug, error, info, warn};
use parquet_catalog::{
    cleanup::{delete_files as delete_parquet_files, get_unreferenced_parquet_files},
    core::PreservedCatalog,
    interface::{CatalogParquetInfo, CheckpointData, ChunkAddrWithoutDatabase},
    prune::prune_history as prune_catalog_transaction_history,
};
use persistence_windows::{checkpoint::ReplayPlan, persistence_windows::PersistenceWindows};
use predicate::predicate::Predicate;
use query::{
    exec::{ExecutionContextProvider, Executor, ExecutorType, IOxExecutionContext},
    QueryDatabase,
};
use schema::selection::Selection;
use schema::Schema;
use time::{Time, TimeProvider};
use trace::ctx::SpanContext;
use tracker::TaskTracker;
use write_buffer::core::WriteBufferReading;

use crate::JobRegistry;

use read_buffer::RBChunk;

#[derive(Debug)]
pub struct Db {
    catalog_access: Arc<QueryCatalogAccess>,
}

#[derive(Debug)]
struct QueryCatalogAccess {
    user_tables: Arc<DbSchemaProvider>,
}

#[derive(Debug)]
struct DbSchemaProvider {
    chunk_access: Arc<ChunkAccess>,
}

#[derive(Debug)]
struct ChunkAccess {
    catalog: Arc<Catalog>,
}

#[derive(Debug)]
struct Catalog {
    tables: Arc<Table>,
}

#[derive(Debug)]
struct Table {
    /// key is partition key
    partitions: Arc<Partition>,
}

#[derive(Debug)]
struct Partition {
    chunks: Arc<ChunkCollection>,
}

#[derive(Debug)]
struct ChunkCollection {
    chunk: Arc<CatalogChunk>,
}

#[derive(Debug)]
struct CatalogChunk {
    stage: Arc<ChunkStage>,
}

#[derive(Debug)]
enum ChunkStage {
    Persisted { read_buffer: Arc<RBChunk> },
}
