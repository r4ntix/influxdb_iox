use async_trait::async_trait;
use data_types::{delete_predicate::DeletePredicate, DatabaseName};
use futures::stream::{FuturesUnordered, TryStreamExt};
use hashbrown::HashMap;
use influxdb_line_protocol::parse_lines;
use mutable_batch::MutableBatch;
use mutable_batch_lp::LinesConverter;
use observability_deps::tracing::*;
use thiserror::Error;
use time::{SystemProvider, TimeProvider};
use trace::ctx::SpanContext;

use super::{DmlError, DmlHandler};

/// An error raised by the [`Partitioner`] handler.
#[derive(Debug, Error)]
pub enum PartitionError {
    /// Failure to parse line protocol in the request.
    #[error("error parsing line {line_idx}: {source}")]
    LineParse {
        /// The 1-indexed line number that caused the error.
        line_idx: usize,
        /// The underlying parser error.
        source: influxdb_line_protocol::Error,
    },

    /// The line failed to apply to the batch of writes for the partition.
    #[error("error batching line {line_idx} into write: {source}")]
    LineBatchWrite {
        /// The 1-indexed line number that caused the error.
        line_idx: usize,
        /// The underlying batch builder error.
        source: mutable_batch::writer::Error,
    },

    /// The inner DML handler returned an error.
    #[error(transparent)]
    Inner(Box<DmlError>),
}

/// A decorator of `T`, tagging it with the partition key derived from it.
#[derive(Debug, PartialEq, Clone)]
pub struct Partitioned<T> {
    key: String,
    payload: T,
}

impl<T> Partitioned<T> {
    /// Wrap `payload` with a partition `key`.
    pub fn new(key: String, payload: T) -> Self {
        Self { key, payload }
    }

    /// Get a reference to the partition payload.
    pub fn payload(&self) -> &T {
        &self.payload
    }

    /// Unwrap `Self` returning the inner payload `T` and the partition key.
    pub fn into_parts(self) -> (String, T) {
        (self.key, self.payload)
    }
}

/// A [`DmlHandler`] implementation that splits line-protocol strings into
/// partitioned [`MutableBatch`] instances by date. Deletes pass through
/// unmodified.
///
/// Each partition is passed through to the inner DML handler (or chain of
/// handlers) concurrently, aborting if an error occurs. This may allow a
/// partial write to be observable down-stream of the [`Partitioner`] if at
/// least one partitioned write succeeds and at least one partitioned write
/// fails. When a partial write occurs, the handler returns an error describing
/// the failure.
#[derive(Debug)]
pub struct Partitioner<D, T = SystemProvider> {
    time_provider: T,
    inner: D,
}

impl<D> Partitioner<D> {
    /// Initialise a new [`Partitioner`] passing partitioned writes to `inner`.
    pub fn new(inner: D) -> Self {
        Self {
            time_provider: SystemProvider::default(),
            inner,
        }
    }
}

#[async_trait]
impl<D, T> DmlHandler for Partitioner<D, T>
where
    D: DmlHandler<WriteInput = Partitioned<HashMap<String, MutableBatch>>>,
    T: TimeProvider,
{
    type WriteError = PartitionError;
    type DeleteError = D::DeleteError;

    type WriteInput = String;

    /// Parse the input line-protocol string and emit partitioned batches to the
    /// next handler.
    async fn write(
        &self,
        namespace: DatabaseName<'static>,
        writes: Self::WriteInput,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::WriteError> {
        // The timestamp value applied to writes that do not specify a
        // timestamp.
        let default_time = self.time_provider.now().timestamp_nanos();

        // A collection of LineConverter instances keyed by partition (ymd date)
        let mut partitions: HashMap<_, LinesConverter> = HashMap::default();

        // Collate the individual LP lines into partitions.
        for (i, line) in parse_lines(&writes).enumerate() {
            let line = line.map_err(|e| PartitionError::LineParse {
                line_idx: i + 1, // 1-based
                source: e,
            })?;

            // Derive the partition key (the date).
            let timestamp = line.timestamp.unwrap_or(default_time);
            let partition_key = time::Time::from_timestamp_nanos(timestamp)
                .date_time()
                .date();

            // Push the write into the batch builder for the partition.
            partitions
                .entry(partition_key)
                .or_insert(LinesConverter::new(default_time))
                .write_parsed_line(line)
                .map_err(|e| PartitionError::LineBatchWrite {
                    line_idx: i + 1,
                    source: e,
                })?;
        }

        // Finalise the LineConverter in each partition to produce a set of
        // per-table MutableBatch, and dispatch all individual partitions into
        // the next handler in the request pipeline.
        partitions
            .into_iter()
            .map(|(key, batch)| {
                let (batch, stats) = batch.finish().expect("unexpected empty batch");
                let p = Partitioned {
                    key: key.format("%Y-%m-%d").to_string(),
                    payload: batch,
                };

                let namespace = namespace.clone();
                let span_ctx = span_ctx.clone();
                async move {
                    self.inner
                        .write(namespace, p, span_ctx)
                        .await
                        .map(|_| stats)
                }
            })
            .collect::<FuturesUnordered<_>>()
            .try_for_each(|stats| async move {
                trace!(
                    lines = stats.num_lines,
                    fields = stats.num_fields,
                    "partitioned write complete"
                );
                Ok(())
            })
            .await
            .map_err(|e| PartitionError::Inner(Box::new(e.into())))
    }

    /// Pass the delete request through unmodified to the next handler.
    async fn delete<'a>(
        &self,
        namespace: DatabaseName<'static>,
        table_name: impl Into<String> + Send + Sync + 'a,
        predicate: DeletePredicate,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::DeleteError> {
        self.inner
            .delete(namespace, table_name, predicate, span_ctx)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use lazy_static::lazy_static;
    use time::Time;

    use crate::dml_handlers::mock::{MockDmlHandler, MockDmlHandlerCall};

    use super::*;

    lazy_static! {
        /// A static default time to use in tests (1971-05-02 UTC).
        static ref DEFAULT_TIME: Time = Time::from_timestamp_nanos(42000000000000000);
    }

    // Generate a test case that partitions "lp" and calls a mock inner DML
    // handler, which returns the values specified in "inner_write_returns".
    //
    // Assert the partition-to-table mapping in "want_writes" and assert the
    // handler write() return value in "want_handler_ret".
    macro_rules! test_write {
        (
            $name:ident,
            lp = $lp:expr,
            inner_write_returns = $inner_write_returns:expr,
            want_writes = [$($want_writes:tt)*], // "partition key" => ["mapped", "tables"] or [unchecked] to skip assert
            want_handler_ret = $($want_handler_ret:tt)+
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_write_ $name>]() {
                    use pretty_assertions::assert_eq;
                    let default_time = time::MockProvider::new(*DEFAULT_TIME);

                    let inner = Arc::new(MockDmlHandler::default().with_write_return($inner_write_returns));
                    let partitioner = Partitioner {
                        time_provider: default_time,
                        inner: Arc::clone(&inner),
                    };
                    let ns = DatabaseName::new("bananas").expect("valid db name");

                    let writes = $lp.to_string();
                    let handler_ret = partitioner.write(ns.clone(), writes, None).await;
                    assert_matches!(handler_ret, $($want_handler_ret)+);

                    // Collect writes into a <partition_key, table_names> map.
                    let calls = inner.calls().into_iter().map(|v| match v {
                        MockDmlHandlerCall::Write { namespace, batches, .. } => {
                            assert_eq!(namespace, *ns);

                            // Extract the table names for comparison
                            let mut tables = batches
                                .payload
                                .keys()
                                .cloned()
                                .collect::<Vec<String>>();

                            tables.sort();

                            (batches.key.clone(), tables)
                        },
                        MockDmlHandlerCall::Delete { .. } => unreachable!("mock should not observe deletes"),
                    })
                    .collect::<HashMap<String, _>>();

                    test_write!(@assert_writes, calls, $($want_writes)*);
                }
            }
        };

        // Generate a NOP that doesn't assert the writes if "unchecked" is
        // specified.
        //
        // This is useful for tests that cause non-deterministic partial writes.
        (@assert_writes, $got:ident, unchecked) => { let _x = $got; };

        // Generate a block of code that validates tokens in the form of:
        //
        //      key => ["table", "names"]
        //
        // Matches the partition key / tables names observed by the mock.
        (@assert_writes, $got:ident, $($partition_key:expr => $want_tables:expr, )*) => {
            // Construct the desired writes, keyed by partition key
            #[allow(unused_mut)]
            let mut want_writes: HashMap<String, _> = Default::default();
            $(
                let mut want: Vec<String> = $want_tables.into_iter().map(|t| t.to_string()).collect();
                want.sort();
                want_writes.insert($partition_key.to_string(), want);
            )*

            assert_eq!(want_writes, $got);
        };
    }

    test_write!(
        single_partition_ok,
        lp = "\
            bananas,tag1=A,tag2=B val=42i 1\n\
            platanos,tag1=A,tag2=B value=42i 2\n\
            another,tag1=A,tag2=B value=42i 3\n\
            bananas,tag1=A,tag2=B val=42i 2\n\
            table,tag1=A,tag2=B val=42i 1\n\
        ",
        inner_write_returns = [Ok(())],
        want_writes = [
            // Attempted write recorded by the mock
            "1970-01-01" => ["bananas", "platanos", "another", "table"],
        ],
        want_handler_ret = Ok(())
    );

    test_write!(
        single_partition_err,
        lp = "\
            bananas,tag1=A,tag2=B val=42i 1\n\
            platanos,tag1=A,tag2=B value=42i 2\n\
            another,tag1=A,tag2=B value=42i 3\n\
            bananas,tag1=A,tag2=B val=42i 2\n\
            table,tag1=A,tag2=B val=42i 1\n\
        ",
        inner_write_returns = [Err(DmlError::DatabaseNotFound("missing".to_owned()))],
        want_writes = [
            // Attempted write recorded by the mock
            "1970-01-01" => ["bananas", "platanos", "another", "table"],
        ],
        want_handler_ret = Err(PartitionError::Inner(e)) => {
            assert_matches!(*e, DmlError::DatabaseNotFound(_));
        }
    );

    test_write!(
        multiple_partitions_ok,
        lp = "\
            bananas,tag1=A,tag2=B val=42i 1\n\
            platanos,tag1=A,tag2=B value=42i 1465839830100400200\n\
            another,tag1=A,tag2=B value=42i 1465839830100400200\n\
            bananas,tag1=A,tag2=B val=42i 2\n\
            table,tag1=A,tag2=B val=42i 1644347270670952000\n\
        ",
        inner_write_returns = [Ok(()), Ok(()), Ok(())],
        want_writes = [
            "1970-01-01" => ["bananas"],
            "2016-06-13" => ["platanos", "another"],
            "2022-02-08" => ["table"],
        ],
        want_handler_ret = Ok(())
    );

    test_write!(
        multiple_partitions_total_err,
        lp = "\
            bananas,tag1=A,tag2=B val=42i 1\n\
            platanos,tag1=A,tag2=B value=42i 1465839830100400200\n\
            another,tag1=A,tag2=B value=42i 1465839830100400200\n\
            bananas,tag1=A,tag2=B val=42i 2\n\
            table,tag1=A,tag2=B val=42i 1644347270670952000\n\
        ",
        inner_write_returns = [
            Err(DmlError::DatabaseNotFound("missing".to_owned())),
            Err(DmlError::DatabaseNotFound("missing".to_owned())),
            Err(DmlError::DatabaseNotFound("missing".to_owned())),
        ],
        want_writes = [unchecked],
        want_handler_ret = Err(PartitionError::Inner(e)) => {
            assert_matches!(*e, DmlError::DatabaseNotFound(_));
        }
    );

    test_write!(
        multiple_partitions_partial_err,
        lp = "\
            bananas,tag1=A,tag2=B val=42i 1\n\
            platanos,tag1=A,tag2=B value=42i 1465839830100400200\n\
            another,tag1=A,tag2=B value=42i 1465839830100400200\n\
            bananas,tag1=A,tag2=B val=42i 2\n\
            table,tag1=A,tag2=B val=42i 1644347270670952000\n\
        ",
        inner_write_returns = [
            Err(DmlError::DatabaseNotFound("missing".to_owned())),
            Ok(()),
            Ok(()),
        ],
        want_writes = [unchecked],
        want_handler_ret = Err(PartitionError::Inner(e)) => {
            assert_matches!(*e, DmlError::DatabaseNotFound(_));
        }
    );

    test_write!(
        no_specified_timestamp,
        lp = "\
            bananas,tag1=A,tag2=B val=42i\n\
            platanos,tag1=A,tag2=B value=42i\n\
            another,tag1=A,tag2=B value=42i\n\
            bananas,tag1=A,tag2=B val=42i\n\
            table,tag1=A,tag2=B val=42i\n\
        ",
        inner_write_returns = [Ok(())],
        want_writes = [
            "1971-05-02" => ["bananas", "platanos", "another", "table"],
        ],
        want_handler_ret = Ok(())
    );

    test_write!(
        single_partition_conflicting_schema,
        lp = "\
            bananas,tag1=A,tag2=B val=42i\n\
            bananas,tag1=A,tag2=B val=42.0\n\
        ",
        inner_write_returns = [],
        want_writes = [],
        want_handler_ret = Err(PartitionError::LineBatchWrite{line_idx, source}) => {
            assert_eq!(line_idx, 2);
            assert_matches!(source, mutable_batch::writer::Error::TypeMismatch{..});
        }
    );

    test_write!(
        invalid_lp,
        lp = "platanos is a word that means bananas",
        inner_write_returns = [],
        want_writes = [],
        want_handler_ret = Err(PartitionError::LineParse { .. })
    );

    // Writes destined for different partitions wind up in different
    // MutableBatch instances, and therefore conflicting schemas are not
    // observed in one batch, and the conflicts pass through this handler
    // without raising an error.
    //
    // These conflicting writes will identified and validated once they pass
    // through the schema validator handler (which is responsible for this kind
    // of enforcement).
    test_write!(
        multiple_partition_conflicting_schema,
        lp = "\
            bananas,tag1=A,tag2=B val=42i 1\n\
            platanos,tag1=A,tag2=B val=42.0 1644347270670952000\n\
        ",
        inner_write_returns = [Ok(()), Ok(())],
        want_writes = [
            "1970-01-01" => ["bananas"],
            "2022-02-08" => ["platanos"],
        ],
        want_handler_ret = Ok(())
    );

    // This handler does not treat an empty write as an error, though another
    // handler in the chain that depends on a non-empty write may.
    test_write!(
        empty_write,
        lp = "",
        inner_write_returns = [],
        want_writes = [],
        want_handler_ret = Ok(())
    );
}
