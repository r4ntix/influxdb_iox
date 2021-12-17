use std::sync::Arc;

use snafu::Snafu;

use data_types::{
    chunk_metadata::{
        ChunkAddr, ChunkColumnSummary, ChunkId, ChunkLifecycleAction, ChunkOrder, ChunkStorage,
        ChunkSummary, DetailedChunkSummary,
    },
    delete_predicate::DeletePredicate,
    partition_metadata::TableSummary,
};
use internal_types::access::AccessRecorder;
use mutable_buffer::{snapshot::ChunkSnapshot as MBChunkSnapshot, MBChunk};
use observability_deps::tracing::debug;
use parquet_file::chunk::ParquetChunk;
use read_buffer::RBChunk;
use schema::Schema;
use tracker::{TaskRegistration, TaskTracker};

use crate::db::catalog::metrics::StorageRecorder;
use parking_lot::Mutex;
use time::{Time, TimeProvider};

#[derive(Debug, Snafu)]
pub enum Error {}
pub type Result<T, E = Error> = std::result::Result<T, E>;


#[derive(Debug)]
pub struct ChunkMetrics {
    /// Chunk storage metrics
    pub(super) chunk_storage: StorageRecorder,

    /// Chunk row count metrics
    pub(super) row_count: StorageRecorder,

    /// Catalog memory metrics
    pub(super) memory_metrics: StorageRecorder,
}
