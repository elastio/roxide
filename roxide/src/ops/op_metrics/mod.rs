//! There are metrics for tracking how the various operations are performing.  They are updated
//! from the various implementations in the `ops` module, but they're all defined in one place in
//! this module.
//!
//! We report a good assortment of metrics at the op level, and at the item level (meaning each key
//! or pair processed within a single vectorized op).
//!
//! The other modules in the `ops` module actually implement these ops, but they all use
//! `instrument_cf_op` or `instrument_async_cf_op` (or the `db` equivalents) to start the op and
//! create a reporter.  the `ColumnFamilyOperationMetricsReporter` (or `Database` equivalent) is
//! passed to that closure, and the op implementations use it to report the performance and
//! behavior of their op.
//!
//! The submodules `cf` and `db` contain the metric reporting logic for column family and database
//! level operations, respectively.
mod cf;
mod db;

pub(super) use cf::{instrument_async_cf_op, instrument_cf_op, ColumnFamilyOperation};
pub(super) use db::{instrument_async_db_op, instrument_db_op, run_blocking_db_task};

// Need to make this a higher visibility because the `Transaction` class uses it to report Commit
// and Rollback op metrics
pub(crate) use db::{instrument_tx_op, DatabaseOperation};

/// The type of `Future` which is returned by all of our async RocksDB operations.  This complex
/// type is due to the fact that we wrap the `BlockingOpFuture` returned by `assuri-iopool` into
/// another type in order to instrument the future with `cheburashka` for metrics and log context.
pub(super) type AsyncOpFuture<T, E = crate::Error> =
    cheburashka::logging::futures::Instrumented<crate::future::BlockingOpFuture<T, E>>;
