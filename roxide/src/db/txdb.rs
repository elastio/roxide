//! This module implements `TransactionDb`, which is a variant of `DB` which supports two-phase
//! commit transaction semantics `Begin/Commit/Rollback`
use super::*;
use crate::rocks_class;
use cheburashka::logging::prelude::*;
use once_cell::sync::OnceCell;
use std::ptr;

rocks_class!(TransactionDbHandle, ffi::rocksdb_transactiondb_t, ffi::rocksdb_transactiondb_close, @send, @sync);

/// The options specific to a TransactionDb transaction
///
/// TODO: If we need to set any of these options, implement that
pub struct TransactionOptions {
    inner: ptr::NonNull<ffi::rocksdb_transaction_options_t>,
}

impl TransactionOptions {
    #[cfg_attr(
        not(feature = "deadlock_detection"),
        allow(unused_mut, clippy::let_and_return,)
    )]
    fn new() -> Self {
        let options = unsafe { ffi::rocksdb_transaction_options_create() };

        let mut me = TransactionOptions {
            inner: ptr::NonNull::new(options).unwrap(),
        };

        // The RocksDB default options do not enable deadlock detection, for now we're keeping it
        // that way.  Without deadlock detection, pessimistic locking transactions that do in fact
        // deadlock with another transaction will fail when their locks timeout.  The default lock
        // timeout is one second, so that's probably not unreasonable default behavior, however if
        // the lock timeout is increased this could be an undesireable behavior.
        //
        // If the `deadlock_detection` feature is enabled, the default is changed so that deadlock
        // detection is enabled by default.
        #[cfg(feature = "deadlock_detection")]
        me.deadlock_detect(true);

        me
    }

    /// Enable or disable deadlock detections for this specific transaction.
    ///
    /// The RocksDB default is to disable deadlock detection, but callers may want to
    /// enable it because it's very helpful for identifying conflicts between transactions.
    ///
    /// By enabling the `deadlock_detection` feature on this crate, deadlock detection will be
    /// enabled by default.  Otherwise it defaults to disabled.
    ///
    /// If deadlock detection is *not* enabled, and two transactions deadlock, they will both block
    /// until the lock timeout elapses, and it won't be obvious from the resulting error if this
    /// was due to contention on the lock or a deadlock.
    ///
    /// Note that there is definitely some runtime performance cost for enabling this, but in our
    /// testing it's not been a noticeably overhead.
    pub fn deadlock_detect(&mut self, deadlock_detect: bool) {
        let options = self.inner.as_ptr();

        unsafe {
            cpp!([options as "rocksdb::TransactionOptions*", deadlock_detect as "bool"] {
                options->deadlock_detect = deadlock_detect;
            })
        };
    }
}

impl Drop for TransactionOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transaction_options_destroy(self.inner.as_ptr());
        }
    }
}

impl Default for TransactionOptions {
    fn default() -> Self {
        TransactionOptions::new()
    }
}

// Technically the struct which RocksDB allocates when a `TransactionOptions` is created is not safe for
// concurrent thread access, meaning it's `Send` but not `Sync` in Rust terminology.  However, at
// the Rust level we don't expose any operations on the struct, it's just a placeholder because
// Rocks doesn't allow us to pass a NULL pointer for options.  This in this implementation it's
// safe for both `Send` and `Sync`
unsafe impl Sync for TransactionOptions {}
unsafe impl Send for TransactionOptions {}

impl handle::RocksObject<ffi::rocksdb_transaction_options_t> for TransactionOptions {
    fn rocks_ptr(&self) -> ptr::NonNull<ffi::rocksdb_transaction_options_t> {
        self.inner
    }
}

impl handle::RocksObjectDefault<ffi::rocksdb_transaction_options_t> for TransactionOptions {
    fn default_object() -> &'static Self {
        static DEFAULT_TX_OPTIONS: OnceCell<TransactionOptions> = OnceCell::new();
        DEFAULT_TX_OPTIONS.get_or_init(TransactionOptions::default)
    }
}

rocks_db_impl!(
    TransactionDb,
    TransactionDbColumnFamily,
    TransactionDbHandle,
    ffi::rocksdb_transactiondb_t
);

cpp! {{
    #include "src/lib.h"
}}

/// Information about a lock that contributed to a deadlock
#[derive(Debug, Clone)]
pub struct DeadlockInfo {
    pub txn_id: u64,
    pub cf_id: u32,
    pub exclusive: bool,
    pub waiting_key: Vec<u8>,
}

impl DeadlockInfo {
    /// Initialize from a poitner to the C++ DeadlockInfo struct
    unsafe fn from_cpp(cpp_ptr: *const libc::c_void) -> Self {
        let mut me = Self {
            txn_id: 0,
            cf_id: 0,
            exclusive: false,
            waiting_key: Vec::new(),
        };
        let me_ptr: *mut Self = &mut me;

        // This is really awkward and repetitive because of the limitations of C++ interop.  This
        // would be easier with a plain C struct.
        cpp!([cpp_ptr as "const rocksdb::DeadlockInfo*", me_ptr as "void*"] {
            auto txn_id = cpp_ptr->m_txn_id;
            auto cf_id = cpp_ptr->m_cf_id;
            auto exclusive = cpp_ptr->m_exclusive;
            auto waiting_key_ptr = cpp_ptr->m_waiting_key.c_str();
            auto waiting_key_len = cpp_ptr->m_waiting_key.size();

            rust!(DeadlockInfo_new [
                me_ptr: *mut DeadlockInfo as "void*",
                txn_id: u64 as "uint64_t",
                cf_id: u32 as "uint32_t",
                exclusive: bool as "bool",
                waiting_key_ptr: *const u8 as "const char*",
                waiting_key_len: isize as "size_t"
            ] {
                (*me_ptr).txn_id = txn_id;
                (*me_ptr).cf_id = cf_id;
                (*me_ptr).exclusive = exclusive;
                (*me_ptr).waiting_key.resize(waiting_key_len as usize, 0u8);
                std::ptr::copy_nonoverlapping(waiting_key_ptr, (*me_ptr).waiting_key.as_mut_ptr(), waiting_key_len as usize);
            });
        });

        me
    }
}

/// Diagnostic output from RocksDB describing the series of locks which led to a deadlock error
#[derive(Debug, Clone)]
pub struct DeadlockPath {
    pub path: Vec<DeadlockInfo>,
    pub limit_exceeded: bool,
    pub deadlock_time: i64,
}

impl DeadlockPath {
    /// Initialize from a pointer to the C++ DeadlockPath struct
    unsafe fn from_cpp(cpp_ptr: *const libc::c_void) -> Self {
        let mut me = Self {
            path: Vec::new(),
            limit_exceeded: false,
            deadlock_time: 0,
        };
        let me_ptr: *mut Self = &mut me;

        // This is really awkward and repetitive because of the limitations of C++ interop.  This
        // would be easier with a plain C struct.
        cpp!([cpp_ptr as "const rocksdb::DeadlockPath*", me_ptr as "void*"] {
            auto path_len = cpp_ptr->path.size();
            auto limit_exceeded = cpp_ptr->limit_exceeded;
            auto deadlock_time = cpp_ptr->deadlock_time;

            rust!(DeadlockPath_new [
                me_ptr: *mut DeadlockPath as "void*",
                path_len: isize as "size_t",
                limit_exceeded: bool as "bool",
                deadlock_time: i64 as "int64_t"
            ] {
                (*me_ptr).path.reserve(path_len as usize);
                (*me_ptr).limit_exceeded = limit_exceeded;
                (*me_ptr).deadlock_time = deadlock_time;
            });

            for (auto info : cpp_ptr->path) {
                const rocksdb::DeadlockInfo* info_ptr = &info;

                rust!(DeadlockPath_add_info [
                    me_ptr: *mut DeadlockPath as "void*",
                    info_ptr: *const libc::c_void as "const rocksdb::DeadlockInfo*"
                ] {
                    (*me_ptr).path.push(DeadlockInfo::from_cpp(info_ptr));
                });
            }
        });

        me
    }
}

impl crate::error::ErrorPostprocessor for TransactionDb {
    fn postprocess_error(&self, err: Error) -> Error {
        match err {
            Error::RocksDbError { status, backtrace }
                if status.code == crate::status::Code::Busy
                    && status.subcode == crate::status::SubCode::Deadlock =>
            {
                // This is a deadlock error which means deadlock detection was enabled which means
                // there should be a record of recent deadlocks.
                Error::RocksDbDeadlock {
                    deadlock_paths: self.get_deadlocks(),
                    backtrace,
                }
            }
            Error::RocksDbError { status, backtrace }
                if status.code == crate::status::Code::TimedOut
                    && status.subcode == crate::status::SubCode::LockTimeout =>
            {
                // A timeout waiting on a lock.  This happens if deadlocks happen and deadlock
                // detection is not enabled
                Error::RocksDbLockTimeout { backtrace }
            }
            Error::RocksDbDeadlock {
                deadlock_paths,
                backtrace,
            } => {
                // If a deadlock causes the failure of a C FFI call instead of one of the
                // operations we implement using the C++ API, then the error is reported as a
                // message, and `make_result` in `ffi_util` will detect when that string matches
                // the error message for a deadlock, and report this error.  But in that case, it
                // can't obtain the deadlock paths because it doesn't have access to the database
                // handle.  So if there are no deadlock paths, re-create this same error but with
                // the current deadlocks
                Error::RocksDbDeadlock {
                    deadlock_paths: if deadlock_paths.is_empty() {
                        self.get_deadlocks()
                    } else {
                        deadlock_paths
                    },
                    backtrace,
                }
            }
            other => {
                // This isn't an error we care about
                trace!(
                    err = log_error(&other),
                    "Error postprocessor ignoring this error"
                );
                other
            }
        }
    }
}

impl TransactionDb {
    /// If deadlock detection is enabled, and if at least one deadlock error has occurred, gets the
    /// path info for all recent deadlocks.
    pub fn get_deadlocks(&self) -> Vec<DeadlockPath> {
        use crate::ops::GetDbPtr;

        let mut deadlocks = Vec::new();
        let deadlocks_ptr: *mut Vec<DeadlockPath> = &mut deadlocks;
        let db_ptr = self.get_db_ptr();

        unsafe {
            cpp!([db_ptr as "rocksdb::DB*", deadlocks_ptr as "void*"] {
                auto txdb_ptr = static_cast<rocksdb::TransactionDB*>(db_ptr);
                auto deadlocks = txdb_ptr->GetDeadlockInfoBuffer();

                for (auto deadlock : deadlocks) {
                    const rocksdb::DeadlockPath* deadlock_ptr = &deadlock;

                    rust!(TransactionDB_get_deadlock [
                        deadlocks_ptr: *mut Vec<DeadlockPath> as "void*",
                        deadlock_ptr: *const libc::c_void as "const rocksdb::DeadlockPath*"
                    ] {
                        (*deadlocks_ptr).push(DeadlockPath::from_cpp(deadlock_ptr));
                    });
                }
            });
        }

        deadlocks
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[cfg(feature = "deadlock_detection")]
    #[test]
    fn deadlock_detection_enabled_by_default() {
        deadlock_detection_test_impl(true);
    }

    #[cfg(not(feature = "deadlock_detection"))]
    #[test]
    fn deadlock_detection_disabled_by_default() {
        deadlock_detection_test_impl(false);
    }

    fn deadlock_detection_test_impl(should_be_enabled: bool) {
        let options = TransactionOptions::default();

        let options_ptr = options.inner.as_ptr();

        let enabled = unsafe {
            cpp!([options_ptr as "rocksdb::TransactionOptions*"] -> bool as "bool" {
                return options_ptr->deadlock_detect;
            })
        };

        if should_be_enabled {
            assert!(enabled, "Deadlock detection should be enabled by default");
        } else {
            assert!(!enabled, "Deadlock detection should be disabled by default");
        }
    }
}
