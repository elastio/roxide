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

const ERROR_COUNT_METRIC_NAME: &str = "roxidedb_error_count";

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
    /// An actual RocksDB error, reported via the RocksDB API.  Should have a status code to
    /// support programmatically testing for a specific error type
    #[snafu(display("rocksdb: {}\n{}", status, backtrace))]
    RocksDBError {
        status: status::Status,
        backtrace: Backtrace,
    },

    /// An error returned by the `rust-rocksdb` wrapper.
    ///
    /// This contains just a string error message, with no additional context.
    #[snafu(display("RocksDB"))]
    RustRocksDBError {
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
    DBStatsDisabledError,

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
    PathNotValidUTF8 { path: PathBuf },

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
