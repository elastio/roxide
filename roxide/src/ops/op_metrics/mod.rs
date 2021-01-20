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

use {
    pin_project::pin_project,
    std::{future::Future, pin::Pin, task::Context, task::Poll},
};

pub(super) use cf::{instrument_async_cf_op, instrument_cf_op, ColumnFamilyOperation};
pub(super) use db::{instrument_async_db_op, instrument_db_op, run_blocking_db_task};

// Need to make this a higher visibility because the `Transaction` class uses it to report Commit
// and Rollback op metrics
pub(crate) use db::{instrument_tx_op, DatabaseOperation};

/// The type of `Future` which is returned by all of our async RocksDB operations.  This complex
/// type is due to the fact that we wrap the `BlockingOpFuture` returned by `elasyncio` into
/// another type in order to instrument the future with `cheburashka` for metrics and log context.
pub(super) type AsyncOpFuture<T, E = crate::Error> =
    cheburashka::logging::futures::Instrumented<crate::future::BlockingOpFuture<T, E>>;

/// Ugly workaround to ensure that database-level async ops also invoke their error postprocessor
/// on error.
///
/// For all CF-level ops, and synchronous DB-level ops, there's code in the [`cf`] and [`db`]
/// modules, respectively, which ensures that if any ops fail with an error, that error is passed
/// to the DB- or CF-level `ErrorPostprocessor` before being returned to the caller.  For
/// architectural reasons relating to the fact that we require a concrete `Future` type instead of
/// using `async-trait`, this isn't so easy for DB-level async operations.
///
/// Thus this future, which wraps the `AsyncOpFuture` which we normally return, and adds error
/// postprocessing.
#[pin_project]
pub struct AsyncDbOpFuture<T> {
    #[pin]
    inner: AsyncOpFuture<T, crate::Error>,
    #[pin]
    error_postprocessor: Box<dyn crate::error::ErrorPostprocessor>,
}

impl<T> AsyncDbOpFuture<T> {
    pub(super) fn wrap_op_future<P: crate::error::ErrorPostprocessor>(
        error_postprocessor: P,
        inner: AsyncOpFuture<T, crate::Error>,
    ) -> Self {
        Self {
            inner,
            error_postprocessor: Box::new(error_postprocessor),
        }
    }
}

impl<T> Future for AsyncDbOpFuture<T> {
    type Output = Result<T, crate::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.project();
        let inner = me.inner;
        let error_postprocessor = me.error_postprocessor;

        inner.poll(cx).map(|result: Result<T, crate::Error>| {
            // If this failed make sure the error postprocessor is invoked
            result.map_err(|e| error_postprocessor.postprocess_error(e))
        })
    }
}
