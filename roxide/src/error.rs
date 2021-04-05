//! The C RocksDB bindings (and thus the Rust bindings) report errors as a string.  There's no
//! additional information about the nature of the failure, unlike the C++ API which provides a
//! pretty detailed `Status` type.  For now we'll have to live with this.

use crate::status;
use cheburashka::metrics;
use lazy_static::lazy_static;
use snafu::{Backtrace, Snafu};
use std::borrow::Cow;
use std::path::PathBuf;
use std::result::Result as StdResult;
use strum_macros::AsRefStr;

pub mod prelude {
    pub use super::*;
    pub use snafu::{ensure, Backtrace, ErrorCompat, ResultExt, Snafu};
}

pub type Result<T, E = Error> = StdResult<T, E>;

/// Most functions in this crate use the `Result` type above, but a few methods that are intended
/// to be implemented by code outside this crate are not bound to the use of `Error` and
/// instead use this type which returns a boxed `Error` impl.
pub type AnyError = Box<dyn std::error::Error + Sync + Send>;

const ERROR_COUNT_METRIC_NAME: &str = "rocksdb_error_count";

lazy_static! {
    /// Maintain a simple counter of the number of times an error is reported, with a label equal
    /// to the name of the `Error` enum variant that was reported.
    ///
    /// We use the `report()` method on `Error` to update this counter when errors are encountered.
    ///
    /// Unfortunately it's not perfect because we can't ensure that every place an error is
    /// reported will call this `report()` function, but the most important error types are covered
    static ref ERROR_COUNT_METRIC: metrics::IntCounterVec = metrics::register_int_counter_vec!(
        ERROR_COUNT_METRIC_NAME,
        "The total number of errors reported",
        &["error_type"]
    )
    .unwrap();
}

#[derive(Debug, Snafu, AsRefStr)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    /// An actual RocksDb error, reported via the RocksDb API.  Should have a status code to
    /// support programmatically testing for a specific error type
    #[snafu(display("rocksdb: {}\n{}", status, backtrace))]
    RocksDbError {
        status: status::Status,
        backtrace: Backtrace,
    },

    /// A RocksDB lock timeout error was reported.
    ///
    /// When a RocksDB error status `TimedOut`, with a subcode of `LockTimeout`, is reported, it's
    /// translated by roxide into this specific error variant.  This is a convenience because
    /// handling that specific type of rocks error is common therefore having a dedicated error
    /// variant for it makes calling code easier.
    #[snafu(display("rocksdb lock timed out"))]
    RocksDbLockTimeout { backtrace: Backtrace },

    /// A deadlock was detected in a TransactionDB.
    ///
    /// This includes additional debugging information to help identify the deadlock.
    #[snafu(display(
        "rocksdb transaction deadlocked.  Recent deadlocks for debugging purposes: \n{:#?}",
        deadlock_paths
    ))]
    RocksDbDeadlock {
        backtrace: Backtrace,
        deadlock_paths: Vec<crate::DeadlockPath>,
    },

    /// A conflict between two competiting transactions was detected in an
    /// `OptimisticTransactionDB`.
    ///
    /// This error is specific to optimistic locking.  On a `TransactionDb` the same scenario would
    /// fail with either `RocksDbLockTimeout` or `RocksDbDeadlock` depending on whether or not
    /// deadlock detection is enabled.
    #[snafu(display("rocksdb detected a conflict between two optimistic locking transactions"))]
    RocksDbConflict { backtrace: Backtrace },

    /// An error returned by the `rust-rocksdb` wrapper.
    ///
    /// This contains just a string error message, with no additional context.
    #[snafu(display("RocksDB"))]
    RustRocksDbError {
        source: rocksdb::Error,
        backtrace: Backtrace,
    },

    /// A database error in the form of a string.  Some RocksDB APIs don't produce anything more
    /// than a string error message.
    #[snafu(display("DB: {}\n{}", message, backtrace))]
    DatabaseError {
        message: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Unable to enable statistics on this database because it was initialized with statistics disabled"))]
    DbStatsDisabledError,

    #[snafu(display("The property named '{}' was not found", property_name))]
    PropertyNotFoundError { property_name: String },

    #[snafu(display(
        "The property '{}' has an incompatible type: {}",
        property_name,
        property_value
    ))]
    MismatchedPropertyTypeError {
        property_name: String,
        property_value: String,
    },

    #[snafu(display("{}", message))]
    OtherError { message: String },

    #[snafu(display("The path '{}' is not a valid UTF-8 string", path.display()))]
    PathNotValidUtf8 { path: PathBuf },

    #[snafu(display("The path '{}' contains embedded NUL bytes so it can't be passed to a C function", path.display()))]
    PathHasNullBytes {
        path: PathBuf,
        source: std::ffi::NulError,
    },

    #[snafu(display("Error creating database path '{}'", path.display()))]
    PathMkdirFailed {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("I/O pool"), context(false))]
    IoPool { source: elasyncio::IoPoolError },

    #[snafu(display("I/O"), context(false))]
    Io { source: std::io::Error },

    #[snafu(display("A callback reported an error"))]
    CallbackError { source: AnyError },

    #[snafu(display("The transaction object is still referenced by one or more other threads"))]
    TransactionStillReferenced { backtrace: Backtrace },

    #[snafu(display("Observability"), context(false))]
    Observability {
        source: cheburashka::CheburashkaError,
    },

    #[snafu(display("Invalid value '{}' for RocksDB metric '{}'", value, name))]
    InvalidMetricValue {
        name: Cow<'static, str>,
        value: String,
        source: std::num::ParseFloatError,
    },
}

impl Error {
    pub(crate) fn other_error<S: Into<String>>(msg: S) -> Self {
        Self::report(Error::OtherError {
            message: msg.into(),
        })
    }

    /// Update the error metrics to reflect the occurrence of this error
    pub(crate) fn report(error: impl Into<Self>) -> Self {
        let error = error.into();
        let error_name = error.as_ref();

        let counter = ERROR_COUNT_METRIC.with_label_values(&[error_name]);

        counter.inc();

        error
    }
}

impl TransactionRetryError for Error {
    fn retry_transaction(&self) -> bool {
        matches!(
            self,
            Self::RocksDbDeadlock { .. }
                | Self::RocksDbLockTimeout { .. }
                | Self::RocksDbConflict { .. }
        )
    }
}

/// Post-process an error reported by a RocksDB component (a database or a transaction)
///
/// In practice this translates deadlock errors into a specific deadlock error that includes a
/// list of recent deadlock paths to aid in debugging.  In the future it might be used for other
/// purposes as well.
pub trait ErrorPostprocessor: Send + 'static {
    fn postprocess_error(&self, err: Error) -> Error {
        // Default behavior is to do nothing; specific implementations may override
        err
    }
}

/// Trait implemented by error types that may contain a RocksDB error which signals a transaction
/// needs to be retried due to a deadlock, lock timeout, or lock conflict.
///
/// This is used with [`crate::BeginTrans::with_trans_retryable`] and related methods to allow
/// arbitrary error types to be used while still detecting when a RocksDB error should cause the
/// transaction to be retried
pub trait TransactionRetryError {
    /// Test to see if this error indicates the current transaction should be retried.
    ///
    /// If this returns `true`, the entire transaction will be aborted, and the steps to perform
    /// the transaction will be performed again in a new transaction.  if this returns false, the
    /// transaction fails and the error is propagated back to the caller.
    fn retry_transaction(&self) -> bool;
}

// Due to Rust rules, no other crate can implement `TransactionRetryError` on behalf of third party
// crates, so we need to implement this for Anyhow so that other code which uses anyhow can operate
// in retryable transactions.
//
// Unfortunately there's a huge down side.  The concrete roxide error type `Error` must be in the
// error chain; if some other error type implements `TransactionRetryError` but doesn't expose our
// roxide `Error` type as a source, then this won't work.  Limitations on Rust dynamic casts are to
// blame.
#[cfg(feature = "anyhow")]
impl TransactionRetryError for anyhow::Error {
    fn retry_transaction(&self) -> bool {
        // If any error in the chain wrapped by anyhow is our error type, then
        for err in self.chain() {
            if let Some(rocks_err) = err.downcast_ref::<Error>() {
                return rocks_err.retry_transaction();
            }
        }

        false
    }
}
