//! Starts a new transaction, for database implementations that support transactions.
//!
//! In addition, a higher-level helper struct `RetryableTransaction` automatically retries
//! transactions if they fail due to a conflict.

use super::op_metrics;
use crate::db::DbLike;
use crate::db::{
    opt_txdb::{OptimisticTransactionDb, OptimisticTransactionOptions},
    txdb::{TransactionDb, TransactionOptions},
};
use crate::db_options::WriteOptions;
use crate::ffi;
use crate::handle::{RocksObject, RocksObjectDefault};
use crate::labels::DatabaseLabels;
use crate::metrics;
use crate::ops::RocksOpBase;
use crate::tx::{sync, unsync};
use crate::{Error, Result};
use async_trait::async_trait;
use cheburashka::logging::{prelude::*, Instrument};
use cheburashka::metrics::*;
use lazy_static::lazy_static;
use std::ptr;
use std::ptr::NonNull;
use std::{future::Future, time::Duration};

const RETRYABLE_TX_TOTAL_METRIC_NAME: &str = "rocksdb_retryable_tx_total";
const RETRYABLE_TX_RUNNING_METRIC_NAME: &str = "rocksdb_retryable_tx_running";
const RETRYABLE_TX_SLEEPING_METRIC_NAME: &str = "rocksdb_retryable_tx_sleeping";
const RETRYABLE_TX_FAILED_METRIC_NAME: &str = "rocksdb_retryable_tx_failed";
const RETRYABLE_TX_RETRIES_METRIC_NAME: &str = "rocksdb_retryable_tx_retries";
const RETRYABLE_TX_ATTEMPTS_METRIC_NAME: &str = "rocksdb_retryable_tx_attempts";
const RETRYABLE_TX_DELAY_METRIC_NAME: &str = "rocksdb_retryable_tx_delay";

lazy_static! {
    static ref RETRYABLE_TX_TOTAL_METRIC: metrics::DatabaseIntCounter = DatabaseLabels::register_int_counter(
        RETRYABLE_TX_TOTAL_METRIC_NAME,
        "The total number of retryable transactions which were initiated",
    )
    .unwrap();
    static ref RETRYABLE_TX_RUNNING_METRIC: metrics::DatabaseIntGauge = DatabaseLabels::register_int_gauge(
        RETRYABLE_TX_RUNNING_METRIC_NAME,
        "The number of retryable transactions which are outstanding",
    )
    .unwrap();
    static ref RETRYABLE_TX_SLEEPING_METRIC: metrics::DatabaseIntGauge = DatabaseLabels::register_int_gauge(
        RETRYABLE_TX_SLEEPING_METRIC_NAME,
        "The subset of rocksdb_retryable_tx_running which are currently sleeping after a retryable error and before their next attempt",
    )
    .unwrap();
    static ref RETRYABLE_TX_FAILED_METRIC: metrics::DatabaseIntCounter = DatabaseLabels::register_int_counter(
        RETRYABLE_TX_FAILED_METRIC_NAME,
        "The number of retryable transactions which failed with a (non-retryable) error",
    )
    .unwrap();
    static ref RETRYABLE_TX_RETRIES_METRIC: metrics::DatabaseIntCounter = DatabaseLabels::register_int_counter(
        RETRYABLE_TX_RETRIES_METRIC_NAME,
        "The number of times a retryable transaction has been retried due to a retryable error",
    )
    .unwrap();

    static ref RETRYABLE_TX_ATTEMPTS_METRIC: metrics::DatabaseIntCounter = DatabaseLabels::register_int_counter(
        RETRYABLE_TX_ATTEMPTS_METRIC_NAME,
        "The total number of attempts made to complete a retryable transaction, including the initial attempt and all retries",
    )
    .unwrap();
    static ref RETRYABLE_TX_DELAY_METRIC: metrics::DatabaseHistogram = DatabaseLabels::register_histogram(
        RETRYABLE_TX_DELAY_METRIC_NAME,
        "The total amount of time delay (in seconds) added to retryable transactions due to retryable errors which eventually succeeded",
        buckets::custom_exponential(0.001, 5.0, 30.0)
    )
    .unwrap();
}

/// Provides the `begin_trans` operation to start an isolated transaction on the database.
///
/// Note that all databases supporting `BeginTrans` must be `Send` and `Sync` so that multiple
/// transactions can be performed on separate threads
#[async_trait]
pub trait BeginTrans: RocksOpBase + DbLike {
    /// The type of the native FFI struct that the RocksDB C API expectes for the transaction
    /// options
    type TransactionOptionsNativeType;

    /// The type of the Rust options struct that holds the transaction options
    type TransactionOptionsType: RocksObjectDefault<Self::TransactionOptionsNativeType>;

    /// Raw version of `begin_trans`, operating on unsafe C bindings.  Not intended for use by external
    /// crates.
    ///
    ///
    /// # Safety
    ///
    /// Never call this function; it's intended for internal use by the `roxide`
    /// implementation.
    unsafe fn raw_begin_trans(
        &self,
        write_options: NonNull<ffi::rocksdb_writeoptions_t>,
        tx_options: NonNull<Self::TransactionOptionsNativeType>,
    ) -> Result<unsync::Transaction>;

    /// Starts a new transaction, with is isolated from changes made in the underlying database and
    /// other transactions.
    ///
    /// Note that, unlike database types, transactions are not `Sync` so they cannot be used from
    /// multiple threads simultaneously.
    fn begin_trans(
        &self,
        write_options: impl Into<Option<WriteOptions>>,
        tx_options: impl Into<Option<Self::TransactionOptionsType>>,
    ) -> Result<unsync::Transaction> {
        op_metrics::instrument_db_op(self, op_metrics::DatabaseOperation::BeginTrans, move || {
            let write_options = WriteOptions::from_option(write_options.into());
            let tx_options = Self::TransactionOptionsType::from_option(tx_options.into());

            unsafe { self.raw_begin_trans(write_options.rocks_ptr(), tx_options.rocks_ptr()) }
        })
    }

    /// Similar to [`Self::begin_trans`] but the transaction is properly configured for use as a
    /// retryable transaction.
    ///
    /// Callers should never use this directly.  If you require retryable transaction semantics,
    /// you must use [`Self::with_trans_retryable`].
    fn begin_trans_retryable(
        &self,
        write_options: impl Into<Option<WriteOptions>>,
        tx_options: impl Into<Option<Self::TransactionOptionsType>>,
    ) -> Result<unsync::Transaction> {
        // This only requires a separate implementation in TransactionDB
        self.begin_trans(write_options, tx_options)
    }

    /// Asynchronously creates a new transaction in a worker thread.
    ///
    /// # Notes
    ///
    /// Because this operation happens in another thread, and in general is used for async
    /// operations, this version produces a `sync::Transaction`, which is the thread-safe wrapper
    /// around `unsync::Transaction`.
    fn async_begin_trans(
        &self,
        write_options: impl Into<Option<WriteOptions>>,
        tx_options: impl Into<Option<Self::TransactionOptionsType>>,
    ) -> op_metrics::AsyncDbOpFuture<sync::Transaction>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
        Self::TransactionOptionsType: Send + 'static,
        Self::TransactionOptionsNativeType: 'static,
        Self: Sync + Clone + 'static,
    {
        op_metrics::instrument_async_db_op(
            self,
            op_metrics::DatabaseOperation::AsyncBeginTrans,
            move |labels| {
                let me = (*self).clone();
                let write_options = WriteOptions::from_option(write_options.into());
                let tx_options = Self::TransactionOptionsType::from_option(tx_options.into());

                op_metrics::run_blocking_db_task(&labels, move || {
                    let tx = unsafe {
                        me.raw_begin_trans(write_options.rocks_ptr(), tx_options.rocks_ptr())?
                    };

                    Ok(tx.into_sync())
                })
            },
        )
    }

    /// Similar to [`Self::async_begin_trans`] but the transaction is properly configured for use as a
    /// retryable transaction.
    ///
    /// Callers should never use this directly.  If you require retryable transaction semantics,
    /// you must use [`Self::async_with_trans_retryable`].
    fn async_begin_trans_retryable(
        &self,
        write_options: impl Into<Option<WriteOptions>>,
        tx_options: impl Into<Option<Self::TransactionOptionsType>>,
    ) -> op_metrics::AsyncDbOpFuture<sync::Transaction>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
        Self::TransactionOptionsType: Send + 'static,
        Self::TransactionOptionsNativeType: 'static,
        Self: Sync + Clone + 'static,
    {
        // This only requires a separate implementation in TransactionDB
        self.async_begin_trans(write_options, tx_options)
    }

    /// Perform a complete transaction by invoking a lambda, automatically rolling back and
    /// re-trying if the transaction deadlocks or times out.
    ///
    /// There's a lot of complexity here to unpack.
    ///
    /// In RocksDB transactions, it's quite common to have two parallel transactions that interfere
    /// with one another, by both attempting to modify the same record.  What happens when they do
    /// this depends on whether pessimistic locking (`TransactionDB`) or optimistic locking
    /// (`OptimisticTransactionDb`) is being used, but in either case the result is that one or
    /// both transactions fail.
    ///
    /// When that happens, in most cases the correct behavior is to re-run the transaction again,
    /// with fresh data, so that hopefully one of the two conflicting transactions completes, and
    /// thus the other one can also complete if it's re-run with the latest version of the data.
    ///
    /// That's not very convenient to code for though, which is why this method exists.  It runs an
    /// arbitrary closure that is not `FnOnce` as is often the case for situations like this, but
    /// `FnMut`.  That's an important distinction in that it means the closure can be invoked
    /// multiple times.  That makes the implementation of the closure a bit messier, as it will
    /// require more cloning of inputs that would otherwise not need to be cloned, however that's a
    /// small price to pay for getting this transaction retry capability more or less for free.
    ///
    /// # Commit and Rollback
    ///
    /// The closure is passed the [`crate::tx::unsync::Transaction`] by reference, which means that
    /// the `commit` and `rollback` methods cannot be called (they consume the transaction).
    /// Instead, the closure controls the disposition of the transaction by its return value.  If
    /// the closure returns `Ok`, then the transaction will be committed.  If it returns `Err`, the
    /// transaction will be rolled back, and depending on the result of
    /// [`crate::error::TransactionRetryError::retry_transaction`] the operation may or may not be
    /// attempted again with a new transaction.
    ///
    /// # Write and Transaction options
    ///
    /// Neither `WriteOptions` nor the DB-specific transaction options types are `Clone`, which
    /// means they cannot be used with retryable transactions.  The default write and transaction
    /// options will be used instead.
    ///
    /// # Error Type
    ///
    /// The closure must produce a `Result`, with any error type so long as it's possible to test
    /// if that error came from RocksDB.  Therefore the error type needs to implement the
    /// [`crate::error::TransactionRetryError`] trait.  The [`crate::Error`] type in this crate already
    /// implements that trait correctly, but if the error type returned by `func` is some other
    /// error type which can potentially contain a [`crate::Error`], then that error type must
    /// implement the trait as well.
    ///
    /// # Idempotency
    ///
    /// Make sure that the closure `func` will produce the same results if it's invoked once, or
    /// 100 times.  In the event that a tranaction fails due to a deadlock or other conflict, a new
    /// transaction will be created and the closure invoked again.  If that closure modifies any
    /// state outside of transaction, this can result in incorrect behavior.
    fn with_trans_retryable<
        R,
        E: std::error::Error
            + crate::error::TransactionRetryError
            + From<crate::error::Error>
            + 'static,
        F: FnMut(&unsync::Transaction) -> Result<R, E>,
    >(
        &self,
        func: F,
    ) -> Result<R, E> {
        let tx = RetryableTransaction::new(self);
        tx.exec(func)
    }

    /// Asynchronous version of [`Self::with_trans_retryable`].  See that function for detailed
    /// documentation.
    ///
    /// # Differences in Async Version
    ///
    /// Some differences are inevitable due to the complication of async execution.
    ///
    /// First, `func` takes a `sync::Transaction`, not `unsync::Transaction`.  This is required
    /// since async code can run on different threads.
    ///
    /// Second, to make the async closure more ergonomic the `Transaction` instance passed to the
    /// closure is owned, there the async code can clone it and with that clone could in theory
    /// call `commit` or `rollback`.  However to do that is an error, as the commit/rollback
    /// behavior of the async version is the same as the sync version documented in
    /// [`Self::with_trans_retryable`].
    async fn async_with_trans_retryable<
        R: Send,
        E: std::error::Error
            + crate::error::TransactionRetryError
            + From<crate::error::Error>
            + Sync
            + Send
            + 'static,
        Func: FnMut(sync::Transaction) -> Fut + Send,
        Fut: Future<Output = Result<R, E>> + Send,
    >(
        &self,
        func: Func,
    ) -> Result<R, E>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
        Self::TransactionOptionsType: Send + 'static,
        Self::TransactionOptionsNativeType: 'static,
        Self: Sync + Clone + 'static,
    {
        let tx = RetryableTransaction::new(self);
        tx.async_exec(func).await
    }
}

#[async_trait::async_trait]
impl BeginTrans for TransactionDb {
    type TransactionOptionsNativeType = ffi::rocksdb_transaction_options_t;
    type TransactionOptionsType = TransactionOptions;

    unsafe fn raw_begin_trans(
        &self,
        write_options: NonNull<ffi::rocksdb_writeoptions_t>,
        tx_options: NonNull<Self::TransactionOptionsNativeType>,
    ) -> Result<unsync::Transaction> {
        let handle = self.handle();
        let tx_ptr = ffi::rocksdb_transaction_begin(
            handle.as_ptr(),
            write_options.as_ptr(),
            tx_options.as_ptr(),
            ptr::null_mut(),
        );

        if let Some(tx_ptr) = ptr::NonNull::new(tx_ptr) {
            Ok(unsync::Transaction::new(self, tx_ptr))
        } else {
            Err(Error::other_error("begin transaction failed"))
        }
    }

    fn begin_trans_retryable(
        &self,
        write_options: impl Into<Option<WriteOptions>>,
        tx_options: impl Into<Option<Self::TransactionOptionsType>>,
    ) -> Result<unsync::Transaction> {
        // With pessimistic locking, deadlock detection MUST be enabled otherwise parallel
        // conflicting transactions are likely not to ever succeed
        let mut tx_options = tx_options.into().unwrap_or_default();
        tx_options.deadlock_detect(true);

        self.begin_trans(write_options, tx_options)
    }

    fn async_begin_trans_retryable(
        &self,
        write_options: impl Into<Option<WriteOptions>>,
        tx_options: impl Into<Option<Self::TransactionOptionsType>>,
    ) -> op_metrics::AsyncDbOpFuture<sync::Transaction>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
        Self::TransactionOptionsType: Send + 'static,
        Self::TransactionOptionsNativeType: 'static,
        Self: Sync + Clone + 'static,
    {
        // With pessimistic locking, deadlock detection MUST be enabled otherwise parallel
        // conflicting transactions are likely not to ever succeed
        let mut tx_options = tx_options.into().unwrap_or_default();
        tx_options.deadlock_detect(true);

        self.async_begin_trans(write_options, tx_options)
    }
}

#[async_trait::async_trait]
impl BeginTrans for OptimisticTransactionDb {
    type TransactionOptionsNativeType = ffi::rocksdb_optimistictransaction_options_t;
    type TransactionOptionsType = OptimisticTransactionOptions;

    unsafe fn raw_begin_trans(
        &self,
        write_options: NonNull<ffi::rocksdb_writeoptions_t>,
        tx_options: NonNull<Self::TransactionOptionsNativeType>,
    ) -> Result<unsync::Transaction> {
        let handle = self.handle();
        let tx_ptr = ffi::rocksdb_optimistictransaction_begin(
            handle.as_ptr(),
            write_options.as_ptr(),
            tx_options.as_ptr(),
            ptr::null_mut(),
        );

        if let Some(tx_ptr) = ptr::NonNull::new(tx_ptr) {
            Ok(unsync::Transaction::new(self, tx_ptr))
        } else {
            Err(Error::other_error("begin transaction failed"))
        }
    }
}

/// Encapsulates the logic which will repeatedly invoke some arbitrary closure, each time in the
/// context of a new transaction, retrying if the closure fails with a RocksDB error which
/// indicates some kind of conflict with another transaction.  The conflict depends on what kind of
/// locking is used and how the database is configured, but broadly they can be:
///
/// * Timeout getting the lock on a key
/// * Deadlock detected (if deadlock detection is enabled)
/// * A concurrent modification in another transaction was detected
///
/// Regardless of what kind of conflict it is, the solution is to abort the transaction, pause for
/// some period, and retry.
///
/// This struct implements that internally.  It's not used directly; see [`BeginTrans`]
/// for both sync and async versions of `with_trans_retryable`.
struct RetryableTransaction<'a, DBT> {
    db: &'a DBT,
    retry_delays: try_again::BackoffIter,
    total_delay: Duration,
    attempt: usize,
    running: IntGauge,
    sleeping: IntGauge,
    failed: IntCounter,
    retries: IntCounter,
    attempts: IntCounter,
    delay: Histogram,
}

impl<'a, DBT: DbLike + BeginTrans> RetryableTransaction<'a, DBT> {
    fn new(db: &'a DBT) -> Self {
        let policy = build_trans_retry_policy();
        let retry_delays = policy.backoffs();

        let labels = DatabaseLabels::from(db);
        let total = RETRYABLE_TX_TOTAL_METRIC.apply_labels(&labels).unwrap();
        let running = RETRYABLE_TX_RUNNING_METRIC.apply_labels(&labels).unwrap();
        let sleeping = RETRYABLE_TX_SLEEPING_METRIC.apply_labels(&labels).unwrap();
        let failed = RETRYABLE_TX_FAILED_METRIC.apply_labels(&labels).unwrap();
        let retries = RETRYABLE_TX_RETRIES_METRIC.apply_labels(&labels).unwrap();
        let attempts = RETRYABLE_TX_ATTEMPTS_METRIC.apply_labels(&labels).unwrap();
        let delay = RETRYABLE_TX_DELAY_METRIC.apply_labels(&labels).unwrap();

        // We assume that when this instance is created, it's the start of a retryable transaction,
        // so just increment `total` now
        total.inc();

        // To ensure this is always decremented in all possible failure and success cases, this
        // gauge is decremented in `drop`
        running.inc();

        Self {
            db,
            retry_delays,
            total_delay: Duration::default(),
            attempt: 0,
            running,
            sleeping,
            failed,
            retries,
            attempts,
            delay,
        }
    }

    /// Synchronous version which invokes a closure with a transaction.
    fn exec<
        R,
        E: std::error::Error
            + crate::error::TransactionRetryError
            + From<crate::error::Error>
            + 'static,
        F: FnMut(&unsync::Transaction) -> Result<R, E>,
    >(
        mut self,
        mut func: F,
    ) -> Result<R, E> {
        loop {
            self.attempt += 1;
            self.attempts.inc();
            if self.attempt > 1 {
                self.retries.inc();
            }

            let tx = self.db.begin_trans_retryable(None, None)?;
            let span = debug_span!("retryable transaction", attempt = self.attempt);
            let _e = span.enter();

            match func(&tx) {
                Ok(ok) => {
                    // If this is a pessimistic locking database, then the fact that execution made
                    // it this far means the transaction didn't deadlock or conflict and commit
                    // will succeed absent some I/O or other error.
                    //
                    // If this is an optimistic locking database, it's at commit time that we find
                    // out if there are conflicts or not
                    debug!(
                        self.attempt,
                        ?self.total_delay,
                        "Closure completed successfully; attempting to commit"
                    );

                    match tx.commit() {
                        Ok(()) => {
                            // The transaction is committed so there is definitely no error
                            debug!(self.attempt, ?self.total_delay, "Transaction committed successfully");

                            self.delay.observe(self.total_delay.as_secs_f64());
                            return Ok(ok);
                        }
                        Err(e) => {
                            // It's possible a retryable error happened here if this is an
                            // optimistic locking database
                            self.handle_commit_error(e)?;

                            // If `handle_commit_error` returned success it means we should retry now
                            continue;
                        }
                    }
                }
                Err(e) => {
                    // If this is a pessimistic locking database, we expect that deadlocks or
                    // conflicts will be reported as they happen in the transaction implementation,
                    // meaning they'll be reported here.
                    //
                    // For an optimistic locking database the errors will be at commit time.
                    self.handle_closure_error(tx, e)?;

                    // If `handle_closure_error` returned success it means we should retry now
                    continue;
                }
            }
        }
    }

    /// Async version which invokes a closure with a transaction.
    async fn async_exec<
        R,
        E: std::error::Error
            + crate::error::TransactionRetryError
            + From<crate::error::Error>
            + Sync
            + Send
            + 'static,
        Fut: Future<Output = Result<R, E>> + Send,
        F: FnMut(sync::Transaction) -> Fut,
    >(
        mut self,
        mut func: F,
    ) -> Result<R, E>
    where
        <DBT as RocksOpBase>::HandleType: Sync + Send,
        DBT::TransactionOptionsType: Send + 'static,
        DBT::TransactionOptionsNativeType: 'static,
        DBT: Sync + Clone + 'static,
    {
        loop {
            self.attempt += 1;
            self.attempts.inc();
            if self.attempt > 1 {
                self.retries.inc();
            }

            let tx = self.db.async_begin_trans_retryable(None, None).await?;
            let span = debug_span!("retryable transaction", attempt = self.attempt);

            match func(tx.clone()).instrument(span).await {
                Ok(ok) => {
                    debug!(
                        self.attempt,
                        ?self.total_delay,
                        "Closure completed successfully; attempting to commit"
                    );

                    match tx.async_commit().await {
                        Ok(()) => {
                            // The transaction is committed so there is definitely no error
                            debug!(self.attempt, ?self.total_delay, "Transaction committed successfully");

                            self.delay.observe(self.total_delay.as_secs_f64());
                            return Ok(ok);
                        }
                        Err(e) => {
                            // It's possible a retryable error happened here if this is an
                            // optimistic locking database
                            self.async_handle_commit_error(e).await?;

                            // If `handle_commit_error` returned success it means we should retry now
                            continue;
                        }
                    }
                }
                Err(e) => {
                    // If this is a pessimistic locking database, we expect that deadlocks or
                    // conflicts will be reported as they happen in the transaction implementation,
                    // meaning they'll be reported here.
                    //
                    // For an optimistic locking database the errors will be at commit time.
                    self.async_handle_closure_error(tx, e).await?;

                    // If `handle_closure_error` returned success it means we should retry now
                    continue;
                }
            }
        }
    }

    /// Roll back a transaction in response to an error, properly handling the case when the
    /// transaction rollback itself fails.
    fn rollback_tx<E: std::error::Error + crate::error::TransactionRetryError + 'static>(
        &self,
        tx: unsync::Transaction,
        e: &E,
    ) {
        // If the rollback itself fails, log the error, but we will still proceed with retrying the
        // transaction again even if the rollback fails.  In any case we know the transaction
        // wasn't committed so the transaction will roll back anyway once the handle goes out of
        // scope and is deleted from Rocks.
        if let Err(rocks_e) = tx.rollback() {
            error!(
                tx_err = log_error(e),
                rollback_err = log_error(&rocks_e),
                self.attempt,
                ?self.total_delay,
                "Error rolling back transaction (rollback_err) which is required due to an error in the transaction (tx_err)"
            );
        }
    }

    /// Async version of [`Self::rollback_tx`]
    async fn async_rollback_tx<
        E: std::error::Error + crate::error::TransactionRetryError + Sync + Send + 'static,
    >(
        &self,
        tx: sync::Transaction,
        e: &E,
    ) {
        // If the rollback itself fails, log the error, but we will still proceed with retrying the
        // transaction again even if the rollback fails.  In any case we know the transaction
        // wasn't committed so the transaction will roll back anyway once the handle goes out of
        // scope and is deleted from Rocks.
        if let Err(rocks_e) = tx.async_rollback().await {
            error!(
                tx_err = log_error(e),
                rollback_err = log_error(&rocks_e),
                self.attempt,
                ?self.total_delay,
                "Error rolling back transaction (rollback_err) which is required due to an error in the transaction (tx_err)"
            );
        }
    }

    /// Handle an error reported by the caller's closure that implemented the transaction.
    ///
    /// In this case there is still a `Transaction` object which needs to be rolled back, and then
    /// the retry logic will be activated if this particular error is retryable.
    fn handle_closure_error<
        E: std::error::Error + crate::error::TransactionRetryError + 'static,
    >(
        &mut self,
        tx: unsync::Transaction,
        e: E,
    ) -> Result<(), E> {
        // It's possible a retryable error happened here if this is an
        // optimistic locking database
        self.rollback_tx(tx, &e);

        self.handle_error(e)
    }

    /// Async version of [`Self::handle_closure_error`]
    async fn async_handle_closure_error<
        E: std::error::Error + crate::error::TransactionRetryError + Sync + Send + 'static,
    >(
        &mut self,
        tx: sync::Transaction,
        e: E,
    ) -> Result<(), E> {
        // It's possible a retryable error happened here if this is an
        // optimistic locking database
        self.async_rollback_tx(tx, &e).await;

        self.async_handle_error(e).await
    }

    /// Handle an error raised when attempting to commit the RocksDB transaction, after the closure
    /// has run successfully.
    ///
    /// In this case there is no transaction to rollback since `commit` consumes the transaction,
    /// but otherwise the error handling and retry logic are the same as `handle_closure_error`
    fn handle_commit_error(&mut self, e: crate::Error) -> Result<()> {
        self.handle_error(e)
    }

    /// Async version of [`Self::handle_commit_error`]
    async fn async_handle_commit_error(&mut self, e: crate::Error) -> Result<()> {
        self.async_handle_error(e).await
    }

    /// Report that a transaction has failed due to an error.
    ///
    /// If the error is retryable, and if the retry count is not exceeded, this will sleep for the
    /// delay interval, and return `Ok`.  It also takes care of updating all metrics and logging.
    ///
    /// If the error is not retryable or the retry count is exceeded, this logs the error, and
    /// returns the error back to the caller in the `Err` variant of the result.
    fn handle_error<E: std::error::Error + crate::error::TransactionRetryError + 'static>(
        &mut self,
        e: E,
    ) -> Result<(), E> {
        if e.retry_transaction() {
            if let Some(sleep) = self.retry_delays.next() {
                debug!(
                    err = log_error(&e),
                    self.attempt,
                    ?sleep,
                    ?self.total_delay,
                    "Retryable error in transaction; will delay and then retry"
                );
                self.total_delay += sleep;
                self.sleeping.inc();
                std::thread::sleep(sleep);
                self.sleeping.dec();

                Ok(())
            } else {
                error!(
                    err = log_error(&e),
                    self.attempt,
                    ?self.total_delay,
                    "Transaction retry limit exceeded; failing"
                );
                self.failed.inc();
                Err(e)
            }
        } else {
            // this is a non-retryable error that should be reported up the stack
            error!(
                err = log_error(&e),
                self.attempt,
                ?self.total_delay,
                "Non-retryable error in transaction; failing"
            );
            self.failed.inc();
            Err(e)
        }
    }

    /// Async version of [`Self::handle_error`]
    async fn async_handle_error<
        E: std::error::Error + crate::error::TransactionRetryError + Sync + Send + 'static,
    >(
        &mut self,
        e: E,
    ) -> Result<(), E> {
        if e.retry_transaction() {
            if let Some(sleep) = self.retry_delays.next() {
                debug!(
                    err = log_error(&e),
                    self.attempt,
                    ?sleep,
                    ?self.total_delay,
                    "Retryable error in transaction; will delay and then retry"
                );
                self.total_delay += sleep;
                self.sleeping.inc();
                tokio::time::sleep(sleep).await;
                self.sleeping.dec();

                Ok(())
            } else {
                error!(
                    err = log_error(&e),
                    self.attempt,
                    ?self.total_delay,
                    "Transaction retry limit exceeded; failing"
                );
                self.failed.inc();
                Err(e)
            }
        } else {
            // this is a non-retryable error that should be reported up the stack
            error!(
                err = log_error(&e),
                self.attempt,
                ?self.total_delay,
                "Non-retryable error in transaction; failing"
            );
            self.failed.inc();
            Err(e)
        }
    }
}

impl<'a, DBT> Drop for RetryableTransaction<'a, DBT> {
    fn drop(&mut self) {
        // This ensures that the `running` metric is updated no matter how the transaction ends up
        // being dropped.
        self.running.dec();
    }
}

/// Construct the `try_again` policy that controls the number of transaction retry attempts and the
/// duration of the sleeps between them.
///
/// TODO: Does this need to be configurable?  `try-again` is originally designed for retrying calls
/// to external systems, so this use case is a bit different.  The reason for using `try-again`
/// here is to re-use the exponential backoff logic because that de-correlates the multiple
/// parallel conflicting operations that cause deadlocks in the first place, so it's unlikely that
/// it needs to be tuned.  In my testing even a simple `sleep` driven by `rand` was sufficient, the
/// reason I'm using `try-again` here is for consistency, because it's already well tested and
/// understood, and if in the future this does need to be made configurable it'll be relatively
/// easier to do so.
fn build_trans_retry_policy() -> try_again::Policy {
    try_again::Policy::load_from_config(
        "retryable transaction",
        try_again::config::RetryPolicyConfig {
            backoff: try_again::Backoff::default(),
            jitter: true,

            // we don't want a lot of delay initially; a few ms is usually enough
            delay: 0.001,

            // in cases of pathological contention throttling makes sense to relieve the pressure,
            // but only up to a certain point.  jitter is enabled so even once the retry logic has
            // reached the max delay, the actual delay will be variable.  that SHOULD be enough to
            // let many parallel conflicting transactions eventually succeed.
            //
            // note that this value is a reasonable default determined based on experimentation
            // with the conflict tests in this module, which represent a pathological extreme level
            // of conflict that is not likely to be encountered in normal workloads.  it's possible
            // this max delay is too high for more realistic workloads, however in testing the
            // conflict tests do not reliably converge on success with a lower max delay than this,
            // if jitter is enabled.  Disabling jitter results in more reliable convergence for
            // pessimistic locking cases, but in optimistic locking cases jitter performs better.
            max_delay: Some(2.0),

            // This should never be reached, but if there's some pathological edge case I want this to be
            // low enough that it fails and doesn't just run for usize::MAX.
            //
            // In a worst case scenario, with N parallel operations, assuming they all conflict
            // with eachother, and deadlock detection is enabled, then `max_retries` should be at
            // least N in order to guarantee all operations will eventually succeed.  If optimistic
            // locking is enabled then this needs to be much larger because with optimistic locking
            // there is no guarantee that any of the transactions will commit successfully.
            //
            // The number of parallel transactions isn't limited anywhere in our code, but as a
            // practical matter we don't expect to see thousands of parallel transactions at a
            // time.
            max_retries: 100,
        },
    )
}

/// The tests for this module are a bit complicated.
///
/// Tests for `begin_trans` and the async version are in a submodule `begin_trans`.
///
/// Tests for `with_trans_retryable` are in a submodule `retryable`, with a submodule for each test
/// case.  The `conflict` test case is particularly complicated because it has a sync and async
/// version, with pieces factored out into separate helper functions to avoid code duplication.
#[cfg(test)]
mod test {
    use super::*;
    use crate::db::{ColumnFamilyLike, DbLike};
    use crate::ops::*;
    use crate::test;
    use crate::test::TempDbPath;
    use rand::prelude::*;

    /// Tests exercise `begin_trans` in the various database types, sync and async
    mod begin_trans {
        use super::*;

        #[test]
        fn txdb() -> Result<()> {
            let path = TempDbPath::new();
            let db = TransactionDb::open(&path, None)?;
            let _cf = db.get_cf("default").unwrap();

            let _trans = db.begin_trans(None, None)?;

            Ok(())
        }

        #[test]
        fn opt_txdb() -> Result<()> {
            let path = TempDbPath::new();
            let db = OptimisticTransactionDb::open(&path, None)?;
            let _cf = db.get_cf("default").unwrap();

            let _trans = db.begin_trans(None, None)?;

            Ok(())
        }

        #[tokio::test]
        async fn txdb_async() -> Result<()> {
            let path = TempDbPath::new();
            let db = TransactionDb::open(&path, None)?;
            let cf = db.get_cf("default").unwrap();

            let tx = db.async_begin_trans(None, None).await?;
            test::fill_db_deterministic(&tx, &cf, 42, 1_000)?;

            let keys = test::random_keys_deterministic(42, 1_000);

            let pairs = tx.async_get(&cf, keys.clone(), None).await?;

            assert_eq!(
                keys,
                pairs
                    .into_iter()
                    .map(|(key, _value)| key)
                    .collect::<Vec<Vec<u8>>>()
            );

            tx.async_commit().await?;

            Ok(())
        }
    }

    /// Tests to exercise `with_trans_retryable`
    mod retryable {
        use super::*;

        /// Number of keys to touch in transactions that test the retry logic after a deadlock or
        /// conflict.
        ///
        /// The higher this number, the more likely a conflict between transactions.
        const KEYS_PER_CONFLICTING_TX: usize = 1024;

        /// The number of parallel threads, each of which touches the same `KEYS_PER_CONFLICTING_TX`
        /// keys but in a random order.
        ///
        /// Obviously the more threads there are, the more potential for conflict there is, and also
        /// the higher the number of retries needed before all threads are able to commit their changes
        /// successfully.
        const PARALLEL_CONFLICTING_THREADS: usize = 32;

        /// Test the case where multiple parallel retryable transactions all cause conflicts and
        /// must be retried
        mod conflict {
            use super::*;

            /// Using `TransactionDB` (and therefore pessimistic locking), perform an operation that is
            /// likely to cause deadlocks when run in multiple threads, and verify that the transaction
            /// retry logic keeps retrying until the operation succeeds.
            #[test]
            fn txdb() -> Result<()> {
                let path = TempDbPath::new();
                let db = TransactionDb::open(&path, None)?;
                conflict_impl(db)
            }

            /// Async equivalent of txdb_with_trans_retryable_conflicts
            #[tokio::test]
            async fn txdb_async() -> Result<()> {
                let path = TempDbPath::new();
                let db = TransactionDb::open(&path, None)?;
                conflict_impl_async(db).await
            }

            /// Same test as above but with `OptimisticTransactionDb` and therefore optimistic locking.
            ///
            /// This kind of locking is a poor choice for cases where conflicts between transactions are
            /// likely, because those conflicts won't be detected until commit time by which point all
            /// conflicting transactions have to restart.  In pathological cases it's possible that none of
            /// the transactions make any progress.
            #[test]
            fn opt_txdb() -> Result<()> {
                let path = TempDbPath::new();
                let db = OptimisticTransactionDb::open(&path, None)?;
                conflict_impl(db)
            }

            /// Async equivalent of txdb_with_trans_retryable_conflicts
            #[tokio::test]
            async fn opt_txdb_async() -> Result<()> {
                let path = TempDbPath::new();
                let db = OptimisticTransactionDb::open(&path, None)?;
                conflict_impl_async(db).await
            }

            /// Generic implementation of the test case for synchonous retryable transaction functionality
            /// in the presence of conflicts.
            ///
            /// This is generic over the actual database type, as long as it supports transactions.
            fn conflict_impl(db: impl DbLike + BeginTrans) -> Result<()> {
                let cf = db.get_cf("default").unwrap();
                let keys = generate_test_keys();

                cheburashka::logging::with_test_logging(test_log_filter(), move || {
                    let mut rand = rand::thread_rng();
                    let threads = (0..PARALLEL_CONFLICTING_THREADS)
                        .map(|_| {
                            // In each thread process the same keys, but with a different ordering.  This
                            // virtually guarantees a conflict
                            let mut keys = keys.clone();
                            keys.shuffle(&mut rand);
                            let db = db.clone();
                            let cf = cf.clone();

                            std::thread::spawn(move || {
                                cheburashka::logging::with_test_logging(
                                    test_log_filter(),
                                    move || {
                                        debug!("In worker thread");
                                        db.with_trans_retryable(move |tx| {
                                            insert_or_increment(&tx, &cf, &keys)
                                        })
                                    },
                                )
                            })
                        })
                        .collect::<Vec<_>>();

                    for thread in threads {
                        debug!("Joining worker thread");
                        thread.join().unwrap()?;
                    }

                    validate_test_keys(&db, keys)?;

                    validate_test_metrics(&db);

                    Ok(())
                })
            }

            /// Async version of `conflig_impl`
            #[allow(clippy::async_yields_async)] // The `async` block yielding a future is needed here to satisfy the borrow checker
            async fn conflict_impl_async<DBT: DbLike + BeginTrans>(db: DBT) -> Result<()>
            where
                <DBT as RocksOpBase>::HandleType: Sync + Send,
                DBT::TransactionOptionsType: Send + 'static,
                DBT::TransactionOptionsNativeType: 'static,
                DBT: Sync + Clone + 'static,
            {
                let cf = db.get_cf("default").unwrap();
                let keys = generate_test_keys();

                cheburashka::logging::with_test_logging_async(test_log_filter(), async move {
                    let mut rand = rand::thread_rng();
                    let tasks = (0..PARALLEL_CONFLICTING_THREADS).map(|_| {
                        // In each thread process the same keys, but with a different ordering.  This
                        // virtually guarantees a conflict
                        let mut keys = keys.clone();
                        keys.shuffle(&mut rand);
                        let db = db.clone();
                        let cf = cf.clone();

                        tokio::spawn(async move {
                            cheburashka::logging::with_test_logging_async(
                                test_log_filter(),
                                async move {
                                    debug!("In worker thread");
                                    db.async_with_trans_retryable(move |tx| {
                                        let cf = cf.clone();
                                        let keys = keys.clone();

                                        tx.async_with_tx(move |tx| {
                                            insert_or_increment(tx, &cf, &keys)
                                        })
                                    })
                                    .await
                                },
                            )
                            .await
                        })
                    });

                    futures::future::try_join_all(tasks)
                        .await?
                        .into_iter()
                        .collect::<Result<Vec<_>>>()?;

                    validate_test_keys(&db, keys)?;

                    validate_test_metrics(&db);

                    Ok(())
                })
                .await
            }

            fn generate_test_keys() -> Vec<u64> {
                (0u64..KEYS_PER_CONFLICTING_TX as u64).into_iter().collect()
            }

            fn validate_test_keys<DBT: DbLike + BeginTrans>(
                db: &DBT,
                keys: Vec<u64>,
            ) -> Result<()> {
                let cf = db.get_cf("default").unwrap();

                // If everything went well, each of the keys will have been incremented once by each of
                // the threads, so each key should now have the value `PARALLEL_CONFLICTING_THREADS`
                for key in keys {
                    let value = db.get(&cf, key.to_le_bytes(), None)?;
                    let value = value.expect("key doesn't have any value at all!");
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(value.as_ref());

                    let value = u64::from_le_bytes(bytes);

                    assert_eq!(
                        value, PARALLEL_CONFLICTING_THREADS as u64,
                        "The value for key '{}' is wrong",
                        key
                    );
                }

                Ok(())
            }

            #[allow(clippy::float_cmp)] // Testing that the sum is != 0 is the correct assertion here
            fn validate_test_metrics<DBT: DbLike + BeginTrans>(db: &DBT) {
                // Validate that the metrics got the expected values
                let labels = DatabaseLabels::from(db);
                let total = RETRYABLE_TX_TOTAL_METRIC.apply_labels(&labels).unwrap();
                let running = RETRYABLE_TX_RUNNING_METRIC.apply_labels(&labels).unwrap();
                let sleeping = RETRYABLE_TX_SLEEPING_METRIC.apply_labels(&labels).unwrap();
                let failed = RETRYABLE_TX_FAILED_METRIC.apply_labels(&labels).unwrap();
                let retries = RETRYABLE_TX_RETRIES_METRIC.apply_labels(&labels).unwrap();
                let attempts = RETRYABLE_TX_ATTEMPTS_METRIC.apply_labels(&labels).unwrap();
                let delay = RETRYABLE_TX_DELAY_METRIC.apply_labels(&labels).unwrap();

                // There should have been one retryable transaction for each test thread
                assert_eq!(PARALLEL_CONFLICTING_THREADS as u64, total.get());

                // There should be none still running
                assert_eq!(0, running.get());

                // There should be none still sleeping
                assert_eq!(0, sleeping.get());

                // None failed (this metric counts failures that were not retryable or exceeded the
                // retry count)
                assert_eq!(0, failed.get());

                // There should have been some retries since this test reliably causes conflicts.  It's
                // hard to say how many but we can definitely say more than zero
                assert_ne!(0, retries.get());

                // Attempts will be equal to the PARALLEL_CONFLICTING_THREADS count, plus however many
                // retries there were
                assert_eq!(
                    PARALLEL_CONFLICTING_THREADS as u64 + retries.get(),
                    attempts.get()
                );

                // Impossible to predict the statistical distribution of the total delay, but we can
                // say for sure that there were definitely some delays
                assert_ne!(0.0, (delay.get_sample_sum()));
            }
        }

        /// Test the case where many retryable transactions are happening but there are no
        /// conflicts
        mod no_conflicts {
            use super::*;

            #[test]
            fn txdb() -> Result<()> {
                let path = TempDbPath::new();
                let db = TransactionDb::open(&path, None)?;
                no_conflicts_impl(db)
            }

            #[test]
            fn opt_txdb() -> Result<()> {
                let path = TempDbPath::new();
                let db = OptimisticTransactionDb::open(&path, None)?;
                no_conflicts_impl(db)
            }

            /// Generic implementation of the test case for synchonous retryable transaction functionality
            /// in the presence of many parallel operations that are intersecting but not overlapping, and
            /// therefore are not conflicts
            ///
            /// This is generic over the actual database type, as long as it supports transactions.
            #[allow(clippy::float_cmp)] // Testing that the sum is != 0 is the correct assertion here
            fn no_conflicts_impl(db: impl DbLike + BeginTrans) -> Result<()> {
                let cf = db.get_cf("default").unwrap();

                let mut all_keys =
                    Vec::with_capacity(PARALLEL_CONFLICTING_THREADS * KEYS_PER_CONFLICTING_TX);
                cheburashka::logging::with_test_logging(test_log_filter(), move || {
                    let mut rand = rand::thread_rng();
                    let threads = (0..PARALLEL_CONFLICTING_THREADS)
                        .map(|thread_idx| {
                            // For this test we want each thread to have its own keys, but they should be interleaved
                            // with the keys of other threads.  So thread 1 will get key 1, 33, 65, thread 2 will get
                            // 34, 66, and so on.
                            let mut keys = (0..KEYS_PER_CONFLICTING_TX)
                                .into_iter()
                                .map(|key_idx| {
                                    (thread_idx * KEYS_PER_CONFLICTING_TX + key_idx) as u64
                                })
                                .collect::<Vec<_>>();
                            all_keys.extend_from_slice(&keys);
                            keys.shuffle(&mut rand);
                            let db = db.clone();
                            let cf = cf.clone();

                            std::thread::spawn(move || {
                                cheburashka::logging::with_test_logging(
                                    test_log_filter(),
                                    move || {
                                        debug!("In worker thread");
                                        db.with_trans_retryable(move |tx| {
                                            insert_or_increment(&tx, &cf, &keys)
                                        })
                                    },
                                )
                            })
                        })
                        .collect::<Vec<_>>();

                    for thread in threads {
                        debug!("Joining worker thread");
                        thread.join().unwrap()?;
                    }

                    // If everything went well, all of the keys used by all of the threads should all be
                    // equal to '1'
                    for key in all_keys {
                        let value = db.get(&cf, key.to_le_bytes(), None)?;
                        let value = value.expect("key doesn't have any value at all!");
                        let mut bytes = [0u8; 8];
                        bytes.copy_from_slice(value.as_ref());

                        let value = u64::from_le_bytes(bytes);

                        assert_eq!(value, 1, "The value for key '{}' is wrong", key);
                    }

                    // Validate that the metrics got the expected values
                    let labels = DatabaseLabels::from(&db);
                    let total = RETRYABLE_TX_TOTAL_METRIC.apply_labels(&labels).unwrap();
                    let running = RETRYABLE_TX_RUNNING_METRIC.apply_labels(&labels).unwrap();
                    let sleeping = RETRYABLE_TX_SLEEPING_METRIC.apply_labels(&labels).unwrap();
                    let failed = RETRYABLE_TX_FAILED_METRIC.apply_labels(&labels).unwrap();
                    let retries = RETRYABLE_TX_RETRIES_METRIC.apply_labels(&labels).unwrap();
                    let attempts = RETRYABLE_TX_ATTEMPTS_METRIC.apply_labels(&labels).unwrap();
                    let delay = RETRYABLE_TX_DELAY_METRIC.apply_labels(&labels).unwrap();

                    // There should have been one retryable transaction for each test thread
                    assert_eq!(PARALLEL_CONFLICTING_THREADS as u64, total.get());

                    // There should be none still running
                    assert_eq!(0, running.get());

                    // There should be none still sleeping
                    assert_eq!(0, sleeping.get());

                    // None failed (this metric counts failures that were not retryable or exceeded the
                    // retry count)
                    assert_eq!(0, failed.get());

                    // There should not have been any retries because there should not have been any errors
                    assert_eq!(0, retries.get());

                    // Attempts will be equal to the PARALLEL_CONFLICTING_THREADS count
                    assert_eq!(PARALLEL_CONFLICTING_THREADS as u64, attempts.get());

                    // Because there were no errors, there should have been no delays
                    assert_eq!(0.0, (delay.get_sample_sum()));

                    Ok(())
                })
            }
        }

        /// Helper for multiple `with_trans_retryable` tests.
        ///
        /// Use `get_or_update` to get the values of specified keys, and either insert a value of `1`
        /// for keys that didnt' already exist, or increment the existing value for keys that did.
        ///
        /// If this operation is performed in multiple threads with `keys` in different order, it will
        /// cause a deadlock, timeout, or conflict at commit time depending on what type of locking is
        /// being used and whether or not deadlock detection is enabled.
        fn insert_or_increment(
            tx: &crate::unsync::Transaction,
            cf: &impl ColumnFamilyLike,
            keys: &[u64],
        ) -> Result<()> {
            let existing_values = keys
                .iter()
                .map(|key| {
                    let key = key.to_le_bytes();

                    // If there is no value, make up an initial value.
                    let value = match tx.get_for_update(cf, &key, None)? {
                        None => 0u64,
                        Some(value) => {
                            let mut bytes = [0u8; 8];
                            bytes.copy_from_slice(value.as_ref());

                            u64::from_le_bytes(bytes)
                        }
                    };

                    Ok((key, value))
                })
                .collect::<Result<Vec<_>>>()?;

            // Write an updated value, adding one to the existing value
            for (key, value) in existing_values {
                let value = value + 1;

                tx.put(cf, key, value.to_le_bytes(), None)?;
            }

            // That's it.  All keys have been incremented
            Ok(())
        }
    }

    /// Some of these tests generate a large volume of warnings or errors, which spam the log on CI
    /// builds and make it really difficult to troubleshoot problems.  So this will override the
    /// default log filter used with the `ELASTIO_LOG` env var is not set, so exclude everything
    /// except actual errors
    fn test_log_filter() -> String {
        std::env::var(cheburashka::logging::ENV_VAR_NAME).unwrap_or_else(|_| "error".to_string())
    }
}
