//! Transactions in RocksDB can be either optimistic (using `OptimisticTransactionDB`) or
//! pessimistic with locks (using `TransactionDB`).  The API is the same either way.
//!
//! There are two `Transaction` implementations:
//!
//! * The `Transaction` in the `unsync` module is not safe to share between threads (aka `Sync`).
//! This is a direct wrapper of the RocksDB `Transaction` class, which also is not thread safe.
//!
//! * The `Transaction` class in the `sync` module provides another layer on top of
//! `unsync::Transaction`, which allows safe sharing of the transaction between threads.
//! Internally, it wraps the inner `Transaction` in a `Arc<Mutex<_>>` and aquires the mutex lock
//! before calling down into the inner `Transaction` object.
use crate::ffi;
use crate::handle;
use crate::labels::{self, DatabaseLabels};
use crate::metrics as rocks_metrics;
use crate::ops::{self, op_metrics};
use crate::rocks_class;
use crate::status;
use crate::Result;
use cheburashka::metrics::{self, *};
use lazy_static::lazy_static;
use std::ptr;

// Define some metrics that we will report about transactions.
// These are not quite DB metrics, and not quite CF metrics, so the framework in `op_metrics`
// doesn't apply.  These are one-offs that have to be handled separately.

lazy_static! {
    static ref DB_TX_TOTAL_METRIC: rocks_metrics::DatabaseIntCounter =
        DatabaseLabels::register_int_counter(
            "rocksdb_db_tx_count",
            "The total number of RocksDB transactions created",
        )
        .unwrap();
    static ref DB_TX_OUTSTANDING_METRIC: rocks_metrics::DatabaseIntGauge =
        DatabaseLabels::register_int_gauge(
            "rocksdb_db_tx_outstanding",
            "The total number of RocksDB transactions currently oustatnding",
        )
        .unwrap();
    static ref DB_TX_COMMITTED_METRIC: rocks_metrics::DatabaseIntCounter =
        DatabaseLabels::register_int_counter(
            "rocksdb_db_tx_committed",
            "The total number of RocksDB transactions committed",
        )
        .unwrap();
    static ref DB_TX_ROLLED_BACK_METRIC: rocks_metrics::DatabaseIntCounter =
        DatabaseLabels::register_int_counter(
            "rocksdb_db_tx_rolled_back",
            "The total number of RocksDB transactions rolled back",
        )
        .unwrap();
    static ref DB_TX_ABANDONED_METRIC: rocks_metrics::DatabaseIntCounter =
        DatabaseLabels::register_int_counter(
            "rocksdb_db_tx_abandoned",
            "The total number of RocksDB transactions abandoned",
        )
        .unwrap();
}

rocks_class!(TransactionHandle, ffi::rocksdb_transaction_t, ffi::rocksdb_transaction_destroy, @send);

cpp! {{
    #include "src/tx.h"
    #include "src/tx.cpp"
}}

// Declare in Rust the functions in `tx.cpp` which let us call the C++ version of Transaction's
// methods from Rust.  This is important because the C bindings don't give us the actual status
// code of the error, while the C++ bindings do.
extern "C" {
    fn tx_commit(tx: *mut ffi::rocksdb_transaction_t) -> status::CppStatus;
    fn tx_rollback(tx: *mut ffi::rocksdb_transaction_t) -> status::CppStatus;
}

/// This module contains the basic `Transaction` type which is not safe to use from multiple
/// threads
pub mod unsync {
    use super::*;
    use crate::db::DBLike;

    /// Provides an isolated context for database operations with transaction semantics.
    ///
    /// None of the operations are actually applied and visible outside of the transaction until
    /// `commit` is called.  If `commit` isn't called when this is dropped, or if `rollback` is
    /// called explicitly, the changes are not applied to the database and will not be seen by
    /// other readers.
    pub struct Transaction {
        inner: TransactionHandle,

        /// The `ErrorPostprocessor` implementation that this transaction should use when reporting
        /// errors.
        ///
        /// This will typically be the `DBLike` implementation that created this transaction
        parent_error_processor: Box<dyn crate::error::ErrorPostprocessor>,

        /// The handle to the database this transaction is opened on.  Since struct members are dropped
        /// in order of declaration, this ensures the transaction handle `inner` is dropped first, so
        /// that even if the last remaining handle to the database is this one, the transaction still
        /// gets dropped first
        _db: handle::AnonymousHandle,

        /// Owned copy of the database's path, for use generating metrics labels
        db_path_str: String,

        /// Owned copy of the database's ID, for use generating metrics labels
        db_id: String,

        /// Used to detect when a transaction is abandoned.  Abandoned means the transaction object
        /// is dropped without having been either committed or rolled back.  This causes the
        /// transaction to be automatically rolled back, but we track it with a separate metric
        abandoned: bool,
    }

    impl Transaction {
        pub(crate) fn new(db: &impl DBLike, tx: impl Into<TransactionHandle>) -> Self {
            let db_handle = db.db_handle();

            let tx = Transaction {
                inner: tx.into(),
                parent_error_processor: Box::new(db.clone()),
                _db: db_handle,
                db_path_str: db.path_str().to_owned(),
                db_id: db.db_id().to_owned(),
                abandoned: true,
            };

            // Update the metrics
            let labels = tx.labels();
            Self::total_metric(&labels).inc();
            Self::outstanding_metric(&labels).inc();

            tx
        }

        /// Gets the path of this transaction's database as a Rust string slice
        pub(crate) fn db_path_str(&self) -> &str {
            &self.db_path_str
        }

        /// Gets the unique ID of this transaction's database, which never changes once a database is created.
        pub(crate) fn db_id(&self) -> &str {
            &self.db_id
        }

        pub fn commit(mut self) -> Result<()> {
            let labels = self.labels();
            op_metrics::instrument_tx_op(
                &self,
                op_metrics::DatabaseOperation::Commit,
                || unsafe { tx_commit(self.inner.as_ptr()).into_result() },
            )?;

            Self::committed_metric(&labels).inc();

            self.abandoned = false;

            Ok(())
        }

        pub fn rollback(mut self) -> Result<()> {
            let labels = self.labels();
            op_metrics::instrument_tx_op(
                &self,
                op_metrics::DatabaseOperation::Rollback,
                || unsafe { tx_rollback(self.inner.as_ptr()).into_result() },
            )?;

            Self::rolled_back_metric(&labels).inc();

            self.abandoned = false;

            Ok(())
        }

        /// Convert this transaction into a `sync::Transaction`, which adds a tiny amount of
        /// overhead per operation but allows the transaction to be used by multiple threads
        pub fn into_sync(self) -> sync::Transaction {
            sync::Transaction::new(self)
        }

        fn labels(&self) -> DatabaseLabels {
            labels::get_db_metric_labels_for_tx(self)
        }

        fn total_metric(labels: &DatabaseLabels) -> metrics::IntCounter {
            DB_TX_TOTAL_METRIC.apply_labels(labels).unwrap()
        }

        fn outstanding_metric(labels: &DatabaseLabels) -> metrics::IntGauge {
            DB_TX_OUTSTANDING_METRIC.apply_labels(labels).unwrap()
        }

        fn committed_metric(labels: &DatabaseLabels) -> metrics::IntCounter {
            DB_TX_COMMITTED_METRIC.apply_labels(labels).unwrap()
        }

        fn rolled_back_metric(labels: &DatabaseLabels) -> metrics::IntCounter {
            DB_TX_ROLLED_BACK_METRIC.apply_labels(labels).unwrap()
        }

        fn abandoned_metric(labels: &DatabaseLabels) -> metrics::IntCounter {
            DB_TX_ABANDONED_METRIC.apply_labels(labels).unwrap()
        }
    }

    impl Drop for Transaction {
        fn drop(&mut self) {
            // This transaction is being dropped so it no longer counts as outstanding
            let labels = self.labels();
            Self::outstanding_metric(&labels).dec();

            // If the `abandoned` flag wasn't cleared by either `commit` or `rollback`, update the
            // `abandoned` metric, and substract this transaction from the `outstanding` count
            if self.abandoned {
                Self::abandoned_metric(&labels).inc();
            }
        }
    }

    impl handle::RocksObject<ffi::rocksdb_transaction_t> for Transaction {
        fn rocks_ptr(&self) -> ptr::NonNull<ffi::rocksdb_transaction_t> {
            self.inner.rocks_ptr()
        }
    }

    impl ops::RocksOpBase for Transaction {
        type HandleType = TransactionHandle;

        fn handle(&self) -> &Self::HandleType {
            &self.inner
        }
    }

    impl crate::error::ErrorPostprocessor for Transaction {
        fn postprocess_error(&self, err: crate::error::Error) -> crate::error::Error {
            self.parent_error_processor.postprocess_error(err)
        }
    }
}

/// Contains the thread-safe `Transaction` type
pub mod sync {
    use super::*;
    use crate::error;
    use crate::future;
    use std::ops::Deref;
    use std::sync::Arc;
    use std::sync::{Mutex, MutexGuard};

    /// A locking version of `unsync::Transaction` which can be called freely from multiple
    /// threads.
    #[derive(Clone)]
    pub struct Transaction(Arc<Mutex<unsync::Transaction>>);

    impl Transaction {
        pub(super) fn new(inner: unsync::Transaction) -> Self {
            Self(Arc::new(Mutex::new(inner)))
        }

        /// Acquires the lock and returns the inner transaction.
        ///
        /// The lock is released when the returned `MutexGuard` is dropped.
        pub(crate) fn lock(&self) -> MutexGuard<unsync::Transaction> {
            self.0.lock().unwrap()
        }

        /// Acquires the lock and passes the inner transaction to a closure.
        ///
        /// The lock is held for the duration of the closure's execution.  When the closure
        /// returns, the lock is released.
        pub(crate) fn with_tx<F: FnOnce(&unsync::Transaction) -> R, R>(&self, f: F) -> R {
            let tx = self.lock();

            f(tx.deref())
        }

        /// Acquires the lock and passes the inner transaction to a closure, on a separate IO
        /// thread.
        ///
        /// When running async code on something like a Tokio reactor, blocking to obtain locks is
        /// harmful to performance.  In such cases, this method can be used.  It will schedule a
        /// task on an IO thread to block on the lock and then call a closure with the
        /// `Transaction` object as an argument.
        pub(crate) fn async_with_tx<
            F: FnOnce(&unsync::Transaction) -> Result<R> + Send + 'static,
            R: Send + 'static,
        >(
            &self,
            f: F,
        ) -> future::BlockingOpFuture<R> {
            let clone = self.clone();

            elasyncio::run_blocking_task(move || {
                let tx = clone.lock();

                f(tx.deref())
            })
        }

        /// Consumes this object, and attempts to recover the original `unsync::Transaction`.  This
        /// can fail if there is more than one reference to the underlying transaction
        pub(crate) fn unwrap(self) -> Result<unsync::Transaction> {
            match Arc::try_unwrap(self.0) {
                Ok(mutex) => {
                    let tx = mutex.into_inner().unwrap();
                    Ok(tx)
                }

                Err(_) => {
                    // The arc has more than one strong reference so we can't possibly unwrap it
                    // right now.
                    error::TransactionStillReferenced
                        .fail()
                        .map_err(error::Error::report)
                }
            }
        }

        /// Commits the transaction.
        ///
        /// # Notes
        ///
        /// This will fail if there are any cloned copies of this transaction object still in scope
        /// in any thread.
        pub fn commit(self) -> Result<()> {
            let tx = self.unwrap()?;
            tx.commit()
        }

        /// Async version of `commit`, performs the commit operation in a worker thread.
        pub fn async_commit(self) -> future::BlockingOpFuture<()> {
            elasyncio::run_blocking_task(move || self.commit())
        }

        /// Rolls back the transaction
        ///
        /// # Notes
        ///
        /// This will fail if there are any cloned copies of this transaction object still in scope
        /// in any thread.
        pub fn rollback(self) -> Result<()> {
            let tx = self.unwrap()?;
            tx.rollback()
        }

        /// Async version of `rollback`, performs the commit operation in a worker thread.
        pub fn async_rollback(self) -> future::BlockingOpFuture<()> {
            elasyncio::run_blocking_task(move || self.rollback())
        }
    }

    impl ops::RocksOpBase for Transaction {
        /// The `HandleType` everywhere else in the code is some implementation of `RocksObject`
        /// that holds the FFI object that corresponds to some RocksDB object pointer.  In this
        /// case that abstraction doesn't quite fit, so instead we'll just expose our inner `Arc`
        /// that holds the transaction object.
        ///
        /// The main thing is that the `HandleType` should be very cheap to clone, which `Arc`
        /// certainly is.
        type HandleType = Arc<Mutex<unsync::Transaction>>;

        fn handle(&self) -> &Self::HandleType {
            &self.0
        }
    }

    impl crate::error::ErrorPostprocessor for Transaction {
        fn postprocess_error(&self, err: crate::error::Error) -> crate::error::Error {
            self.with_tx(|tx| tx.postprocess_error(err))
        }
    }
}
