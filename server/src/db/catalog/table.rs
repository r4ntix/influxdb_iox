use std::{ops::Deref, result::Result, sync::Arc};

use hashbrown::HashMap;

use data_types::partition_metadata::{PartitionAddr, PartitionSummary};
use schema::{
    builder::SchemaBuilder,
    merge::{Error as SchemaMergerError, SchemaMerger},
    Schema,
};
use time::TimeProvider;
use tracker::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::db::catalog::metrics::TableMetrics;

use super::partition::Partition;
