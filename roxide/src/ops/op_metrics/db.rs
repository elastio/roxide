//! Contains database-level operation metrics.
//!
//! We don't have the same amount of detail on database metrics, as they tend not to be on a
//! performance critical path.
use super::AsyncDbOpFuture;
use crate::labels::DatabaseOperationLabels;
use crate::logging::LoggingContext;
use crate::metrics;
use crate::{db::DbLike, error::ErrorPostprocessor};
use crate::{Error, Result};
use cheburashka::{logging::prelude::*, metrics::*};
use lazy_static::lazy_static;
use strum_macros::{Display, IntoStaticStr};

#[derive(Copy, Clone, Display, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum DatabaseOperation {
    Checkpoint,
    BeginTrans,
    AsyncBeginTrans,
    Write,
    Commit,
    Rollback,
    Flush,
    GetLiveFiles,
}

lazy_static! {
    static ref DB_OP_TOTAL_METRIC: metrics::DatabaseOperationIntCounter =
        DatabaseOperationLabels::register_int_counter(
            "rocksdb_db_ops_total",
            "The number of rocksdb database operations performed",
        )
        .unwrap();
    static ref DB_OP_RUNNING_METRIC: metrics::DatabaseOperationIntGauge =
        DatabaseOperationLabels::register_int_gauge(
            "rocksdb_db_ops_running",
            "The number of rocksdb database operations currently running",
        )
        .unwrap();
    static ref DB_OP_FAILED_METRIC: metrics::DatabaseOperationIntCounter =
        DatabaseOperationLabels::register_int_counter(
            "rocksdb_db_ops_failed",
            "The number of rocksdb database operations failed",
        )
        .unwrap();
    static ref DB_OP_DURATION_METRIC: metrics::DatabaseOperationHistogram =
        DatabaseOperationLabels::register_histogram(
            "rocksdb_db_op_duration_seconds",
            "The duration in seconds of a database operation",
            buckets::io_latency_seconds(None)
        )
        .unwrap();
}

fn db_metrics(
    labels: &DatabaseOperationLabels<'_>,
) -> Result<(IntCounter, IntGauge, IntCounter, Histogram)> {
    Ok((
        DB_OP_TOTAL_METRIC.apply_labels(labels)?,
        DB_OP_RUNNING_METRIC.apply_labels(labels)?,
        DB_OP_FAILED_METRIC.apply_labels(labels)?,
        DB_OP_DURATION_METRIC.apply_labels(labels)?,
    ))
}

/// Instruments a database operation which is performed on a transaction, by updating metrics and
/// setting the appropriate logging context.
pub(crate) fn instrument_tx_op<R, F: FnOnce() -> Result<R>>(
    tx: &crate::tx::unsync::Transaction,
    op: DatabaseOperation,
    func: F,
) -> Result<R> {
    let op_name: &'static str = op.into();
    let labels = DatabaseOperationLabels::new_from_tx(tx, op_name);

    instrument_db_op_internal(labels, || func().map_err(|e| tx.postprocess_error(e)))
}

/// Instruments a database operation, by updating metrics and setting the appropriate logging
/// context.
pub(crate) fn instrument_db_op<R, F: FnOnce() -> Result<R>>(
    db: &impl DbLike,
    op: DatabaseOperation,
    func: F,
) -> Result<R> {
    let op_name: &'static str = op.into();

    let labels = DatabaseOperationLabels::new(db, op_name);
    instrument_db_op_internal(labels, || func().map_err(|e| db.postprocess_error(e)))
}

/// Instruments a database operation, by updating metrics and setting the appropriate logging
/// context.
pub(crate) fn instrument_async_db_op<
    R,
    F: FnOnce(DatabaseOperationLabels) -> crate::future::BlockingOpFuture<R>,
>(
    db: &impl DbLike,
    op: DatabaseOperation,
    func: F,
) -> AsyncDbOpFuture<R> {
    let op_name: &'static str = op.into();

    let labels = DatabaseOperationLabels::new(db, op_name);
    let db = db.clone();

    // It's not convenient to instrument the `Future` returned by `func` because that would
    // require a much more complicated type signature and adding a custom `Future`
    // implementation.  We can't use `async` here because our operations are exposed as traits
    // and traits can't use `async`.  `async-trait` is a workaround but it defeats
    // monomorphisatiion and inlining so it comes at too high a performance cost.
    //
    // Instead we have a hand-rolled `Future` impl in `AsyncDbOpFuture` that ensures
    // post-processing happens on DB ops
    AsyncDbOpFuture::wrap_op_future(db, labels.clone().in_async_context(move || func(labels)))
}

/// Runs a blocking DB task on the thread pool.  Any database op that uses `instrument_async_db_op`
/// for instrumentation and needs to do some blocking work must use this method, and not the
/// general-purpose `elasyncio::run_blocking_task`, otherwise the database op metrics related
/// to failure counts will be absent, and errors will not be properly logged.
pub(crate) fn run_blocking_db_task<F, T>(
    labels: &DatabaseOperationLabels,
    func: F,
) -> crate::future::BlockingOpFuture<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    let (total, running, failed, duration) = db_metrics(labels).unwrap();
    elasyncio::run_blocking_task(move || {
        total.inc();
        running.inc();
        let result = duration.observe_closure_duration(|| func());
        running.dec();

        match &result {
            Ok(_) => {}

            Err(err) => {
                failed.inc();
                report_error(err);
            }
        }

        result
    })
}

fn instrument_db_op_internal<R, F: FnOnce() -> Result<R>>(
    labels: DatabaseOperationLabels,
    func: F,
) -> Result<R> {
    labels.in_context(|| {
        let (total, running, failed, duration) = db_metrics(&labels)?;

        total.inc();
        running.inc();
        let result = duration.observe_closure_duration(|| func());
        running.dec();

        match &result {
            Err(err) => {
                failed.inc();
                report_error(err);
            }

            Ok(_) => {}
        }

        result
    })
}

#[allow(clippy::cognitive_complexity)] // the complexity is low; clippy is seeing the macro expansion
fn report_error(err: &Error) {
    match err {
        Error::RocksDbConflict { .. } => {
            // This looks like how a commit operation fails when there is a conflict between
            // transactions on a database using optimistic locking.
            //
            // Usually this happens in a retriable transaction so don't blast the error log with
            // it.  If this causes some higher-level operation to fail completely, that'll be the
            // time to log it as an error
            warn!(target: "rocksdb_db_op", "Busy error from RocksDB which usually indicates a conflict between transactions");
        }
        Error::RocksDbError { status, backtrace } => {
            error!(target: "rocksdb_db_op", status = %status, %backtrace, "RocksDB database error");
        }
        Error::DatabaseError { message, backtrace } => {
            error!(target: "rocksdb_db_op", message = %message, %backtrace, "RocksDB database error message");
        }
        other_error => {
            error!(target: "rocksdb_db_op", other_error = log_error(other_error), "Error during RocksDB operation");
        }
    };
}
