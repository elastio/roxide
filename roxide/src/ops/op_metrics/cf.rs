//! Contains the implementation of `ColumnFamilyOperationMetricsReporter` used to report metrics on
//! a column family operation
use super::AsyncOpFuture;
use crate::db::ColumnFamilyLike;
use crate::labels::ColumnFamilyOperationLabels;
use crate::logging::LoggingContext;
use crate::metrics;
use crate::{Error, Result};
use cheburashka::logging::{futures::Instrument, prelude::*};
use cheburashka::metrics::*;
use lazy_static::lazy_static;
use strum::{Display, IntoStaticStr};

#[derive(Copy, Clone, Display, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum ColumnFamilyOperation {
    Get,
    MultiGet,
    AsyncGet,
    GetForUpdate,
    MultiGetForUpdate,
    AsyncGetForUpdate,
    CompactRange,
    Delete,
    MultiDelete,
    AsyncDelete,
    DeleteRange,
    MultiDeleteRange,
    AsyncDeleteRange,
    GetProperty,
    IterateAll,
    IterateRange,
    IteratePrefix,
    AsyncIterateAll,
    AsyncIterateRange,
    AsyncIteratePrefix,
    KeyExists,
    KeysExist,
    AsyncKeysExist,
    Merge,
    MultiMerge,
    AsyncMerge,
    Put,
    MultiPut,
    AsyncPut,

    /// In some cases an async op is implemented by calling the sync version of that same op.  It
    /// would be confusing to count that twice, once as the async verion and once as the sync
    /// version.  So this operation is used in those cases.
    AsyncWrapper,
}

const CF_OP_TOTAL_METRIC_NAME: &str = "rocksdb_cf_ops_total";
const CF_OP_RUNNING_METRIC_NAME: &str = "rocksdb_cf_ops_running";
const CF_OP_FAILED_METRIC_NAME: &str = "rocksdb_cf_ops_failed";
const CF_OP_DURATION_METRIC_NAME: &str = "rocksdb_cf_op_duration_seconds";

const CF_OP_ITEM_TOTAL_METRIC_NAME: &str = "rocksdb_cf_op_items_total";
const CF_OP_ITEM_FAILED_METRIC_NAME: &str = "rocksdb_cf_op_items_failed";
const CF_OP_ITEM_DURATION_METRIC_NAME: &str = "rocksdb_cf_op_item_duration_seconds";

const CF_OP_VECTOR_LEN_METRIC_NAME: &str = "rocksdb_cf_op_items_per_op";
const CF_OP_KEY_LEN_METRIC_NAME: &str = "rocksdb_cf_op_key_len_bytes";
const CF_OP_VALUE_LEN_METRIC_NAME: &str = "rocksdb_cf_op_value_len_bytes";

lazy_static! {
    static ref CF_OP_TOTAL_METRIC: metrics::ColumnFamilyOperationIntCounter = ColumnFamilyOperationLabels::register_int_counter(
        CF_OP_TOTAL_METRIC_NAME,
        "The number of rocksdb column family operations performed",
    )
    .unwrap();
    static ref CF_OP_RUNNING_METRIC: metrics::ColumnFamilyOperationIntGauge = ColumnFamilyOperationLabels::register_int_gauge(
        CF_OP_RUNNING_METRIC_NAME,
        "The number of rocksdb column family operations currently running",
    )
    .unwrap();
    static ref CF_OP_FAILED_METRIC: metrics::ColumnFamilyOperationIntCounter = ColumnFamilyOperationLabels::register_int_counter(
        CF_OP_FAILED_METRIC_NAME,
        "The number of rocksdb column family operations failed",
    )
    .unwrap();
    static ref CF_OP_DURATION_METRIC: metrics::ColumnFamilyOperationHistogram = ColumnFamilyOperationLabels::register_histogram(
        CF_OP_DURATION_METRIC_NAME,
        "The duration in seconds of a column family operation",
        buckets::io_latency_seconds(None)
    )
    .unwrap();

    static ref CF_OP_ITEM_TOTAL_METRIC: metrics::ColumnFamilyOperationIntCounter = ColumnFamilyOperationLabels::register_int_counter(
        CF_OP_ITEM_TOTAL_METRIC_NAME,
        "The total number of items in this column family which have been processed (vectorized ops can cause this to be substantially more than the corresponding op count metric)",
    )
    .unwrap();
    static ref CF_OP_ITEM_FAILED_METRIC: metrics::ColumnFamilyOperationIntCounter = ColumnFamilyOperationLabels::register_int_counter(
        CF_OP_ITEM_FAILED_METRIC_NAME,
        "The total number of items in this column family which have failed to process (vectorized ops can cause this to be substantially more than the corresponding op count metric)",
    )
    .unwrap();
    static ref CF_OP_ITEM_DURATION_METRIC: metrics::ColumnFamilyOperationHistogram = ColumnFamilyOperationLabels::register_histogram(
        CF_OP_ITEM_DURATION_METRIC_NAME,
        "The duration in seconds taken to process a single item in a potentially multi-item operation",
        buckets::io_latency_seconds(None)
    )
    .unwrap();

    static ref CF_OP_VECTOR_LEN_METRIC: metrics::ColumnFamilyOperationHistogram = ColumnFamilyOperationLabels::register_histogram(
        CF_OP_VECTOR_LEN_METRIC_NAME,
        "The number of items in a vector op (like multi_get, async_put,etc)",
        buckets::custom_exponential(1., 2., 2_000.)
    )
    .unwrap();
    static ref CF_OP_KEY_LEN_METRIC: metrics::ColumnFamilyOperationHistogram = ColumnFamilyOperationLabels::register_histogram(
        CF_OP_KEY_LEN_METRIC_NAME,
        "The size in bytes of the keys in this column family",
        buckets::byte_size(4_096) // hard to imagine any key anywhere near this big
    )
    .unwrap();
    static ref CF_OP_VALUE_LEN_METRIC: metrics::ColumnFamilyOperationHistogram = ColumnFamilyOperationLabels::register_histogram(
        CF_OP_VALUE_LEN_METRIC_NAME,
        "The size in bytes of the values in this column family",
        buckets::byte_size(65_536) // This would be a very big value
    )
    .unwrap();
}

/// Instruments a column family operation, by updating metrics and setting the appropriate logging
/// context.
pub(crate) fn instrument_cf_op<
    CF: ColumnFamilyLike,
    R,
    F: FnOnce(&ColumnFamilyOperationMetricsReporter<CF>) -> Result<R>,
>(
    cf: &CF,
    op: ColumnFamilyOperation,
    func: F,
) -> Result<R> {
    let op_name: &'static str = op.into();

    let labels = ColumnFamilyOperationLabels::new(cf, op_name);
    labels.in_context(|| {
        let reporter = ColumnFamilyOperationMetricsReporter::new(cf, labels.clone());

        reporter.run_op(func)
    })
}

/// Instruments a column family operation, by updating metrics and setting the appropriate logging
/// context.
pub(crate) fn instrument_async_cf_op<
    CF: ColumnFamilyLike,
    R,
    F: FnOnce(ColumnFamilyOperationMetricsReporter<CF>) -> crate::future::BlockingOpFuture<R>,
>(
    cf: &CF,
    op: ColumnFamilyOperation,
    func: F,
) -> AsyncOpFuture<R> {
    let op_name: &'static str = op.into();

    let labels = ColumnFamilyOperationLabels::new(cf, op_name);
    labels.in_context(|| {
        let reporter = ColumnFamilyOperationMetricsReporter::new(cf, labels.clone());

        reporter.run_async_op(func).in_current_span()
    })
}

/// Passed to each column family op for which metrics are being recorded, allowing the op implementation to
/// report detailed op-specific metrics information
pub(crate) struct ColumnFamilyOperationMetricsReporter<CF: ColumnFamilyLike> {
    op_name: &'static str,
    cf: CF,
    op_total: IntCounter,
    op_running: IntGauge,
    op_failed: LocalIntCounter,
    op_duration: LocalHistogram,
    op_item_total: LocalIntCounter,
    op_item_failed: LocalIntCounter,
    op_item_duration: LocalHistogram,
    op_vector_len: LocalHistogram,
    op_key_len: LocalHistogram,
    op_value_len: LocalHistogram,
}

impl<CF: ColumnFamilyLike> ColumnFamilyOperationMetricsReporter<CF> {
    pub(super) fn new(cf: &CF, labels: ColumnFamilyOperationLabels<'_>) -> Self {
        // Get labeled local versions of all the relevant metrics.  This is a very low cost
        // operation that doesn't perform any atomic operations or otherwise lock.
        // If any of these metrics end up not being modified, then the corresopnding `flush`
        // operation does nothing.

        ColumnFamilyOperationMetricsReporter {
            op_name: labels.op_name,
            // This may seem excessive but in order to support instrumenting async operations we
            // can't have any non-static lifetimes in this struct.  The performance implications
            // are minimal since cloning a CF is just an Arc clone.
            cf: cf.clone(),

            // Total and running should not be local since they are always updated, and in the case
            // of running we want the running count updates to be pushed to the global metrics
            // state immediately so there's no performance gain in using local
            op_total: CF_OP_TOTAL_METRIC.apply_labels(&labels).unwrap(),
            op_running: CF_OP_RUNNING_METRIC.apply_labels(&labels).unwrap(),

            // All other metrics are local
            op_failed: CF_OP_FAILED_METRIC.apply_labels(&labels).unwrap().local(),
            op_duration: CF_OP_DURATION_METRIC.apply_labels(&labels).unwrap().local(),
            op_item_total: CF_OP_ITEM_TOTAL_METRIC
                .apply_labels(&labels)
                .unwrap()
                .local(),
            op_item_failed: CF_OP_ITEM_FAILED_METRIC
                .apply_labels(&labels)
                .unwrap()
                .local(),
            op_item_duration: CF_OP_ITEM_DURATION_METRIC
                .apply_labels(&labels)
                .unwrap()
                .local(),
            op_vector_len: CF_OP_VECTOR_LEN_METRIC
                .apply_labels(&labels)
                .unwrap()
                .local(),
            op_key_len: CF_OP_KEY_LEN_METRIC.apply_labels(&labels).unwrap().local(),
            op_value_len: CF_OP_VALUE_LEN_METRIC
                .apply_labels(&labels)
                .unwrap()
                .local(),
        }
    }

    pub(super) fn run_op<R, F: FnOnce(&ColumnFamilyOperationMetricsReporter<CF>) -> Result<R>>(
        self,
        func: F,
    ) -> Result<R> {
        self.op_total.inc();
        self.op_running.inc();

        let result = self
            .op_duration
            .observe_closure_duration(|| func(&self))
            .map_err(|e| self.cf.postprocess_error(e));

        self.op_running.dec();

        match &result {
            Err(err) => {
                self.report_error(err);
            }

            Ok(_) => {}
        }

        self.flush();

        result
    }

    /// Runs an asyncronous operation, consuming this reporter and passing it to the operation
    /// closure.
    ///
    /// Within that closure, it's assumed that the async operation is in fact a blocking operation
    /// which will be run on the elasyncio threadpool, and that our method
    /// `Self::run_blocking_task` is how that task is run.  We rely on that method to update the
    /// performance metrics for async operations.
    pub(super) fn run_async_op<
        R,
        F: FnOnce(ColumnFamilyOperationMetricsReporter<CF>) -> crate::future::BlockingOpFuture<R>,
    >(
        self,
        func: F,
    ) -> crate::future::BlockingOpFuture<R> {
        // It's not convenient to instrument the `Future` returned by `func` because that would
        // require a much more complicated type signature and adding a custom `Future`
        // implementation.  We can't use `async` here because our operations are exposed as traits
        // and traits can't use `async`.  `async-trait` is a workaround but it defeats
        // monomorphisatiion and inlining so it comes at too high a performance cost.
        func(self)
    }

    /// Runs a given closure as a blocking task, returning a future that will reflect the status of
    /// the task.  This works the same as `elasyncio::run_blocking_task()`, except it ensures
    /// the metrics for this task are captured properly.
    pub fn run_blocking_task<F, T>(self, func: F) -> crate::future::BlockingOpFuture<T>
    where
        F: FnOnce(&ColumnFamilyOperationMetricsReporter<CF>) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        // even though this is async, it's run as a blocking task, which means we can use the same
        // approach as in run_op
        elasyncio::run_blocking_task(move || self.run_op(func))
    }

    /// Instruments the processing of a single item in a vector op.  Records the runtime, operand
    /// lenths, and result in metrics.
    pub fn processing_item<R, F: FnOnce() -> Result<R>>(
        &self,
        key_len: impl Into<Option<usize>>,
        value_len: impl Into<Option<usize>>,
        func: F,
    ) -> Result<R> {
        self.op_item_total.inc();

        if let Some(key_len) = key_len.into() {
            self.op_key_len.observe(key_len as f64);
        }

        if let Some(value_len) = value_len.into() {
            self.op_value_len.observe(value_len as f64);
        }

        let result = self.op_item_duration.observe_closure_duration(func);

        if result.is_err() {
            self.op_item_failed.inc();
        }

        result
    }

    /// Instruments item processing when the RocksDB operation itself is vectorized and operates on
    /// the entire batch at once.  In this case there will not be per-item timing information.
    pub fn processing_batch<R, F: FnOnce() -> Result<R>>(
        &self,
        batch_size: usize,
        func: F,
    ) -> Result<R> {
        self.op_item_total.inc_by(batch_size as u64);

        let result = func();

        if result.is_err() {
            self.op_item_failed.inc_by(batch_size as u64);
        }

        result
    }

    /// Update the global metrics counters with the changes made here
    fn flush(self) {
        // Use the item counter to find out how many items were in this op, and update the vector
        // len histogram before we flush everything else
        self.op_vector_len.observe(self.op_item_total.get() as f64);

        self.op_failed.flush();
        self.op_duration.flush();
        self.op_item_total.flush();
        self.op_item_failed.flush();
        self.op_item_duration.flush();
        self.op_vector_len.flush();
        self.op_key_len.flush();
        self.op_value_len.flush();
    }

    #[allow(clippy::cognitive_complexity)] // the complexity is low; clippy is seeing the macro expansion
    fn report_error(&self, err: &Error) {
        self.op_failed.inc();
        match err {
            Error::RocksDbError { status, backtrace } => {
                error!(target: "rocksdb_cf_op", op_name = self.op_name, cf_name = %self.cf.name(), %status, %backtrace, "RocksDB database error");
            }
            Error::RocksDbDeadlock { deadlock_paths, .. } => {
                // This is usually something that happens in the context of a retriable
                // transaction, so it's an expected error.  Log that at the warning level to avoid
                // spooking whoever views the log output
                warn!(target: "rocksdb_cf_op", op_name = self.op_name, cf_name = %self.cf.name(), deadlock_paths = %format!("{:#?}", deadlock_paths),
                    "Deadlock in RocksDB transaction"
                );
            }
            Error::RocksDbLockTimeout { .. } => {
                // As with deadlocks, this is usually a sign of a transaction conflict.  If
                // deadlock detection is enabled, normally conflicts result in deadlocks and fail
                // with `RocksDbDeadlock`, however for pessimistic locking databases without
                // deadlock detection, or in cases where the lock times out but there is no
                // deadlock, this error can happen also.  It's also retriable and therefore should
                // not be logged at the error level
                warn!(target: "rocksdb_cf_op", op_name = self.op_name, cf_name = %self.cf.name(),
                    "Lock timeout in RocksDB transaction"
                );
            }
            Error::DatabaseError { message, backtrace } => {
                error!(target: "rocksdb_cf_op", op_name = self.op_name, cf_name = %self.cf.name(), %message, %backtrace, "RocksDB database error message");
            }
            other_error => {
                error!(target: "rocksdb_cf_op", op_name = self.op_name, cf_name = %self.cf.name(), error = log_error(other_error), "Error during RocksDB operation");
            }
        }
    }
}
