//! gRPC service implementations for `ingester`.

use std::sync::Arc;
use crate::handler::IngestHandler;

/// This type is responsible for managing all gRPC services exposed by
/// `ingester`.
#[derive(Debug, Default)]
pub struct GrpcDelegate<I: IngestHandler> {
    ingest_handler: Arc<I>
}

impl<I: IngestHandler> GrpcDelegate<I> {
    /// Initialise a new [`GrpcDelegate`] passing valid requests to the
    /// specified `ingest_handler`.
    pub fn new(ingest_handler: Arc<I>) -> Self {
        Self{
            ingest_handler,
        }
    }
}
