//! The catalog representation of a Partition

use super::chunk::{CatalogChunk, Error as ChunkError};
use crate::db::catalog::metrics::PartitionMetrics;
use data_types::{
    chunk_metadata::{ChunkAddr, ChunkId, ChunkLifecycleAction, ChunkOrder, ChunkSummary},
    delete_predicate::DeletePredicate,
    partition_metadata::{PartitionAddr, PartitionSummary},
};
use hashbrown::HashMap;
use observability_deps::tracing::info;
use persistence_windows::{
    min_max_sequence::OptionalMinMaxSequence, persistence_windows::PersistenceWindows,
};
use schema::Schema;
use snafu::{OptionExt, Snafu};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Display,
    ops::RangeInclusive,
    sync::Arc,
};
use time::{Time, TimeProvider};
use tracker::RwLock;

/// Provides ordered iteration of a collection of chunks
#[derive(Debug)]
struct ChunkCollection {
    /// The chunks that make up this partition, indexed by order and id.
    ///
    /// This is the order that chunks should be iterated and locks acquired
    a: CatalogChunk
}

#[derive(Debug)]
pub struct Partition {
    chunks: ChunkCollection,
}
