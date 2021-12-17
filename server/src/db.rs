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
use crate::db::catalog::table::Table;
use crate::db::catalog::metrics::CatalogMetrics;

 pub mod catalog;

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Snafu)]
pub enum Error {
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum DmlError {
}

#[derive(Debug)]
pub struct Db {
    // 50 ms
//    jobs: JobRegistry,

    // 800 ms
    catalog_access: QueryCatalogAccess,
}

#[derive(Debug)]
pub(crate) struct QueryCatalogAccess {
    user_tables: DbSchemaProvider,
}

#[derive(Debug)]
struct DbSchemaProvider {
    chunk_access: ChunkAccess,
}

#[derive(Debug)]
struct ChunkAccess {
    catalog: Catalog,

}

#[derive(Debug)]
pub struct Catalog {
    db_name: Arc<str>,

    /// key is table name
    ///
    /// TODO: Remove this unnecessary additional layer of locking
    tables: RwLock<HashMap<Arc<str>, Table>>,

}
