//! This module implements `OptimisticTransactionDb`, which is a variation of `DB` which provides
//! the same transaction semantics as `TransactionDb`, except using an optimistic locking strategy.
use super::*;
use crate::rocks_class;
use once_cell::sync::OnceCell;

rocks_class!(OptimisticTransactionDbHandle, ffi::rocksdb_optimistictransactiondb_t, ffi::rocksdb_optimistictransactiondb_close, @send, @sync);

/// The options specific to a OptimisticTransactionDB transaction
///
/// TODO: If we need to set any of these options, implement that
pub struct OptimisticTransactionOptions {
    inner: ptr::NonNull<ffi::rocksdb_optimistictransaction_options_t>,
}

impl OptimisticTransactionOptions {
    fn new() -> Self {
        OptimisticTransactionOptions {
            inner: unsafe {
                ptr::NonNull::new(ffi::rocksdb_optimistictransaction_options_create()).unwrap()
            },
        }
    }
}

impl Drop for OptimisticTransactionOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_optimistictransaction_options_destroy(self.inner.as_ptr());
        }
    }
}

impl Default for OptimisticTransactionOptions {
    fn default() -> Self {
        OptimisticTransactionOptions::new()
    }
}

// Technically the struct which RocksDB allocates when a `OptimisticTransactionOptions` is created is not safe for
// concurrent thread access, meaning it's `Send` but not `Sync` in Rust terminology.  However, at
// the Rust level we don't expose any operations on the struct, it's just a placeholder because
// Rocks doesn't allow us to pass a NULL pointer for options.  This in this implementation it's
// safe for both `Send` and `Sync`
unsafe impl Sync for OptimisticTransactionOptions {}
unsafe impl Send for OptimisticTransactionOptions {}

impl handle::RocksObject<ffi::rocksdb_optimistictransaction_options_t>
    for OptimisticTransactionOptions
{
    fn rocks_ptr(&self) -> ptr::NonNull<ffi::rocksdb_optimistictransaction_options_t> {
        self.inner
    }
}

impl handle::RocksObjectDefault<ffi::rocksdb_optimistictransaction_options_t>
    for OptimisticTransactionOptions
{
    fn default_object() -> &'static Self {
        static DEFAULT_TX_OPTIONS: OnceCell<OptimisticTransactionOptions> = OnceCell::new();
        DEFAULT_TX_OPTIONS.get_or_init(OptimisticTransactionOptions::default)
    }
}

rocks_db_impl!(
    OptimisticTransactionDb,
    OptimisticTransactionDbColumnFamily,
    OptimisticTransactionDbHandle,
    ffi::rocksdb_optimistictransactiondb_t
);

impl OptimisticTransactionDb {
    pub(crate) fn ffi_open(
        options: *const ffi::rocksdb_options_t,
        path: *const libc::c_char,
        num_column_families: libc::c_int,
        column_family_names: *mut *const libc::c_char,
        column_family_options: *mut *const ffi::rocksdb_options_t,
        column_family_handles: *mut *mut ffi::rocksdb_column_family_handle_t,
    ) -> Result<*mut ffi::rocksdb_optimistictransactiondb_t> {
        unsafe {
            ffi_try!(ffi::rocksdb_optimistictransactiondb_open_column_families(
                options,
                path,
                num_column_families,
                column_family_names,
                column_family_options,
                column_family_handles,
            ))
        }
    }

    /// Invokes a closure passing it a pointer to the base database for this optimistic tx
    /// database.  This has to be freed when done, hence the use of a closure to guarantee that
    /// happens
    pub(crate) unsafe fn with_base_db<T, F: FnOnce(*mut ffi::rocksdb_t) -> Result<T>>(
        handle: &OptimisticTransactionDbHandle,
        func: F,
    ) -> Result<T> {
        // The optimistic transaction DB API is slightly different than either of the others.
        // The regular `DB` has a `put_cf` operation.  The `TransactionDB` does also.  But
        // `OptimisticTransactionDb` doesn't have its own separate `put_cf` operation; rather it
        // exposes a function to get the "base" `DB` and you can use that to do a `put`.  I suppose
        // this is because of the difference in implementation; `OptimisticTransactionDb` doesn't do
        // any locking or conflict detection except within the context of an explicitly created
        // transaction, so it delegates operations outside of transactions to the underlying
        // non-transactional implementation.  That's just a guess on my part.

        // TODO: it's really wasteful to incur a heap alloc every time we want to call down into the
        // base database.  If we were calling the C++ API directly we could avoid this overhead
        let base_db = ffi::rocksdb_optimistictransactiondb_get_base_db(handle.rocks_ptr().as_ptr());
        if base_db.is_null() {
            return Err(Error::other_error("Failed to get base database"));
        }

        let result = func(base_db);

        ffi::rocksdb_optimistictransactiondb_close_base_db(base_db);

        result
    }
}

impl crate::error::ErrorPostprocessor for OptimisticTransactionDb {
    fn postprocess_error(&self, err: Error) -> Error {
        match err {
            Error::RocksDbError { status, backtrace }
                if status.code == crate::status::Code::Busy
                    && status.subcode == crate::status::SubCode::None =>
            {
                // This is how commit operations fail when optimistic locking is enabled.  This
                // error would be expected to happen at commit time.
                Error::RocksDbConflict { backtrace }
            }
            other => other,
        }
    }
}
