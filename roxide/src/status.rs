//! In the RocksDB C++ code the `Status` class represents the status of any operation.  It captures
//! not only failed/succeeded, but also possible kinds of errors and their severity.
//!
//! `Status` is not exposed via the C bindings, so most of the functions we call provide only a
//! `char*` error value, which is either `NULL` or the English text of the error.
//!
//! Sometimes though it's very important for us to know what specific error occurred.  In these
//! cases we must use the C++ API, which is more complex and error prone to call than the C API.
//! When we do that, we use the `Status` struct to bridge the gap between the C++ `Status` class
//! and Rust.

use crate::error;
use crate::Result;
use std::ffi;
use std::fmt;

cpp! {{
    #include "src/status.h"
    #include "src/status.cpp"
}}

/// The possible error codes.
///
/// Copy/pasted from the C++ code in `include/rocksdb/status.h`
#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(u8)]
pub enum Code {
    Ok = 0,
    NotFound = 1,
    Corruption = 2,
    NotSupported = 3,
    InvalidArgument = 4,
    IoError = 5,
    MergeInProgress = 6,
    Incomplete = 7,
    ShutdownInProgress = 8,
    TimedOut = 9,
    Aborted = 10,
    Busy = 11,
    Expired = 12,
    TryAgain = 13,
    CompactionTooLarge = 14,
    ColumnFamilyDropped = 15,
}

/// The possible error sub-codes.
///
/// Copy/pasted from the C++ code in `include/rocksdb/status.h`
#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(u8)]
pub enum SubCode {
    None = 0,
    MutexTimeout = 1,
    LockTimeout = 2,
    LockLimit = 3,
    NoSpace = 4,
    Deadlock = 5,
    StaleFile = 6,
    MemoryLimit = 7,
    SpaceLimit = 8,
    PathNotFound = 9,
}

/// The possible error severity levels.
///
/// Copy/pasted from the C++ code in `include/rocksdb/status.h`
#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(u8)]
pub enum Severity {
    NoError = 0,
    SoftError = 1,
    HardError = 2,
    FatalError = 3,
    UnrecoverableError = 4,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Status {
    pub(crate) code: Code,
    pub(crate) subcode: SubCode,
    pub(crate) severity: Severity,
    pub(crate) state: Option<Vec<u8>>,
}

/// Ugly hack for C++ interop.
///
/// This has the same layout as the `CppStatus` struct in `status.h`.  It's how status information
/// is passed between C++ and Rust code without an excess of hassle.
#[repr(C)]
#[derive(Debug, PartialEq, Clone)]
#[doc(hidden)] // this is only `pub` so it can be used with C++
pub struct CppStatus {
    code: Code,
    subcode: SubCode,
    severity: Severity,
    state: *const libc::c_char,
}

impl Drop for CppStatus {
    fn drop(&mut self) {
        // Need to free the `state` member
        if !self.state.is_null() {
            let state_ptr = self.state;

            unsafe {
                cpp!([state_ptr as "const char*"] {
                    delete[] state_ptr;
                });
            }
        }
    }
}

impl CppStatus {
    /// Consume this status and convert it into a Rust-native one, while ensuring that any memory
    /// allocated on the C++ heap gets freed
    pub(crate) fn into_status(self) -> Status {
        Status::new(self)
    }

    pub(crate) fn into_result(self) -> Result<()> {
        self.into_status().into_result()
    }
}

impl Status {
    pub fn code(&self) -> Code {
        self.code
    }

    pub fn subcode(&self) -> SubCode {
        self.subcode
    }

    pub fn severity(&self) -> Severity {
        self.severity
    }

    pub fn state(&self) -> Option<&Vec<u8>> {
        self.state.as_ref()
    }

    pub fn is_err(&self) -> bool {
        // The 'severity' value is bullshit.  It doesn't seem to be set on most errors.
        // So instead just test for a code value of `Ok`.  it seems that there are no cases where a
        // successful result returns a code other than OK.
        self.code != Code::Ok
    }

    pub fn is_ok(&self) -> bool {
        !self.is_err()
    }

    pub(crate) fn new(cpp_status: CppStatus) -> Self {
        let state: Option<Vec<u8>> = if cpp_status.state.is_null() {
            None
        } else {
            unsafe {
                let state_str = ffi::CStr::from_ptr(cpp_status.state);

                Some(Vec::from(state_str.to_bytes()))
            }
        };

        Status {
            code: cpp_status.code,
            subcode: cpp_status.subcode,
            severity: cpp_status.severity,
            state,
        }
    }

    /// Consumes this status and convert it into a `Result` that is either en `Err` which describes
    /// the particular error encountered, or `Ok` if this status describes success.
    pub(crate) fn into_result(self) -> Result<()> {
        if self.is_ok() {
            Ok(())
        } else if self.code == Code::TimedOut && self.subcode == SubCode::LockTimeout {
            // There's a special error variant specifically for this case to make it more
            // convenient for calling code to detect
            error::RocksDbLockTimeout
                .fail()
                .map_err(error::Error::report)
        } else {
            // Some other rocks error that doesn't indicate a lock timeout
            error::RocksDbError { status: self }
                .fail()
                .map_err(error::Error::report)
        }
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_ok() {
            write!(f, "Success")
        } else {
            // Error.  If severity is somethign other than `NoError`, which AFAIK never happens,
            // but just in case, use that
            if self.severity != Severity::NoError {
                write!(f, "{:?}: ", self.severity)?;
            }

            write!(f, "{:?}", self.code)?;

            if self.subcode != SubCode::None {
                write!(f, " ({:?})", self.subcode)?;
            }

            if let Some(ref state) = self.state {
                write!(f, " - {}", String::from_utf8_lossy(state.as_ref()))?;
            }

            Ok(())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// Generate a C++ status representing success
    fn generate_success() -> CppStatus {
        unsafe {
            cpp!([] -> CppStatus as "CppStatus" {
                return rustify_status(rocksdb::Status::OK());
            })
        }
    }

    /// Generate a C++ status representing a failure without any subcode
    fn generate_failure() -> CppStatus {
        unsafe {
            cpp!([] -> CppStatus as "CppStatus" {
                return rustify_status(rocksdb::Status::Corruption());
            })
        }
    }

    /// Generate a C++ status representing a failure with a subcode
    fn generate_failure_with_subcode() -> CppStatus {
        unsafe {
            cpp!([] -> CppStatus as "CppStatus" {
                return rustify_status(rocksdb::Status::NoSpace());
            })
        }
    }

    #[test]
    fn handles_success() {
        assert_eq!("Success", format!("{}", generate_success().into_status()));
        assert!(generate_success().into_result().is_ok());
    }

    #[test]
    fn handles_failure() {
        assert_eq!(
            "Corruption",
            format!("{}", generate_failure().into_status())
        );
        assert!(generate_failure().into_result().is_err());
    }

    #[test]
    fn handles_failure_with_subcode() {
        assert_eq!(
            "IoError (NoSpace)",
            format!("{}", generate_failure_with_subcode().into_status())
        );
        assert!(generate_failure_with_subcode().into_result().is_err());
    }
}
