//! This module implements `TransactionDB`, which is a variant of `DB` which supports two-phase
//! commit transaction semantics `Begin/Commit/Rollback`
use super::*;
use crate::rocks_class;
use once_cell::sync::OnceCell;
use std::ptr;

rocks_class!(TransactionDBHandle, ffi::rocksdb_transactiondb_t, ffi::rocksdb_transactiondb_close, @send, @sync);

/// The options specific to a TransactionDB transaction
///
/// TODO: If we need to set any of these options, implement that
pub struct TransactionOptions {
    inner: ptr::NonNull<ffi::rocksdb_transaction_options_t>,
}

impl TransactionOptions {
    fn new() -> Self {
        TransactionOptions {
            inner: unsafe { ptr::NonNull::new(ffi::rocksdb_transaction_options_create()).unwrap() },
        }
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
    TransactionDB,
    TransactionDBColumnFamily,
    TransactionDBHandle,
    ffi::rocksdb_transactiondb_t
);

impl TransactionDB {}
