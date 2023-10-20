//! The `Get` operation reads a single key from the database
use super::op_metrics;
use super::*;
use crate::db::{self, db::*, opt_txdb::*, txdb::*};
use crate::db_options::ReadOptions;
use crate::error;
use crate::ffi;
use crate::ffi_util;
use crate::handle::{RocksObject, RocksObjectDefault};
use crate::mem::{DbPinnableSlice, DbVector};
use crate::status;
use crate::status::{Code, CppStatus};
use crate::tx::{sync, unsync};
use crate::Result;
use std::ptr::NonNull;

cpp! {{
    #include "src/ops/get.h"
    #include "src/ops/get.cpp"
}}

// Declaration for C callable wrapper defined in `get.cpp` that wraps Transaction

extern "C" {
    fn tx_get_for_update(
        tx: *mut ffi::rocksdb_transaction_t,
        options: *const ffi::rocksdb_readoptions_t,
        cf: *const ffi::rocksdb_column_family_handle_t,
        key_ptr: *const libc::c_char,
        key_len: libc::size_t,
        value_ptr_ptr: *mut *mut libc::c_char,
        value_len_ptr: *mut libc::size_t,
    ) -> status::CppStatus;
}

/// Generalized implementation of multi-get that works with either DB or transaction pointers
#[allow(clippy::transmute_num_to_bytes)]
unsafe fn raw_multi_get_impl<K: BinaryStr>(
    cpp_db_ptr: *mut std::ffi::c_void,
    cpp_tx_ptr: *mut ffi::rocksdb_transaction_t,
    cf: NonNull<ffi::rocksdb_column_family_handle_t>,
    keys: impl IntoIterator<Item = K>,
    options: NonNull<ffi::rocksdb_readoptions_t>,
) -> Result<Vec<(K, Option<DbPinnableSlice>)>> {
    // Keed all of the keys in a contiguous vector to use MultiGet
    let keys = keys.into_iter().collect::<Vec<_>>();
    let num_keys = keys.len();
    let cf_ptr = cf.as_ptr();
    let options_ptr = options.as_ptr();

    // Make a C++ STL vector of `rocks::Slice` containing each key
    let cpp_keys =
        ffi_util::rust_slices_to_rocks_slices(keys.iter().map(|binary_str| binary_str.as_slice()));

    // prepare Rust vecs to hold the values retrieved and the status codes for each key
    let mut values = Vec::<DbPinnableSlice>::with_capacity(num_keys);
    let rust_values_ptr: *mut Vec<DbPinnableSlice> = &mut values;
    let mut statuses = Vec::<CppStatus>::with_capacity(num_keys);
    let rust_statuses_ptr: *mut Vec<CppStatus> = &mut statuses;

    // One of these should be null and one should be non-null
    assert!(cpp_db_ptr.is_null() || cpp_tx_ptr.is_null());
    assert!(!cpp_db_ptr.is_null() || !cpp_tx_ptr.is_null());

    cpp!([cpp_db_ptr as "rocksdb::DB*",
        cpp_tx_ptr as "rocksdb_transaction_t*",
        options_ptr as "rocksdb_readoptions_t*",
        cf_ptr as "rocksdb_column_family_handle_t*",
        num_keys as "size_t",
        cpp_keys as "std::vector<rocksdb::Slice>*",
        rust_values_ptr as "void*",
        rust_statuses_ptr as "void*"
        ] {
        std::vector<rocksdb::PinnableSlice> values;
        std::vector<CppStatus> statuses;

        db_or_tx_multi_get(cpp_db_ptr,
            cpp_tx_ptr,
            options_ptr,
            cf_ptr,
            num_keys,
            *cpp_keys,
            values,
            statuses
        );

        // The STL vectors `values` and `statuses` are now populated with key values and
        // `CppStatus` status values, respectively.  Fil the Rust `statuses` Vec accordingly,
        // and copy the contents of `values` into a new Rust Vec of `DbPinnableSlice`.
        auto cpp_statuses_ptr = statuses.data();
        rust!(DBMultiGet_copy_statuses [
            num_keys: usize as "size_t",
            rust_statuses_ptr: *mut Vec<CppStatus> as "void*",
            cpp_statuses_ptr: *const CppStatus as "const CppStatus*"
        ] {
            unsafe {
                let cpp_statuses = std::slice::from_raw_parts(cpp_statuses_ptr, num_keys);
                (*rust_statuses_ptr).extend_from_slice(cpp_statuses);
            }
        });

        auto cpp_values_ptr = &values;

        rust!(DBMultiGet_copy_values [
            rust_values_ptr: *mut Vec<DbPinnableSlice> as "void*",
            cpp_values_ptr: *mut std::ffi::c_void as "std::vector<rocksdb::PinnableSlice>*"
        ] {
                unsafe {
                    DbPinnableSlice::extend_vec_from_pinnable_slices(&mut (*rust_values_ptr), cpp_values_ptr);
                }
            }
        );
    });

    // No matter whether the call succeeded or failed, free the STL vector now.
    ffi_util::free_rocks_slices_vector(cpp_keys);

    // Need to combine the status codes and data results into a Result<(key, optional_value)>
    debug_assert_eq!(values.len(), statuses.len());
    debug_assert_eq!(values.len(), keys.len());

    let results: Result<Vec<(K, Option<DbPinnableSlice>)>> = keys
        .into_iter()
        .zip(values)
        .zip(statuses)
        .map(|((key, value), status)| {
            let status = status.into_status();

            // The lookup will fail with a NotFound error if the key isn't found
            if status.is_ok() {
                Ok((key, Some(value)))
            } else if status.code() == Code::NotFound {
                Ok((key, None))
            } else {
                error::RocksDbError { status }.fail()
            }
        })
        .collect();

    results
}

pub trait Get: RocksOpBase {
    /// Raw version of `get`, operating on unsafe C bindings.  Not intended for use by external
    /// crates.
    ///
    /// # Safety
    ///
    /// Never call this function; it's intended for internal use by the `roxide`
    /// implementation.
    unsafe fn raw_get(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<Option<DbVector>>;

    /// Raw version of `multi_get`, operating on unsafe C bindings.  Not intended for use by external
    /// crates.
    ///
    /// # Safety
    ///
    /// Never call this function; it's intended for internal use by the `roxide`
    /// implementation.
    unsafe fn raw_multi_get<K: BinaryStr>(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        keys: impl IntoIterator<Item = K>,
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<Vec<(K, Option<DbPinnableSlice>)>>;

    /// Gets the value for `key` in `cf`, returning a Rocks-allocated buffer containing the value
    /// if found, or `None` if not found.
    fn get(
        &self,
        cf: &impl db::ColumnFamilyLike,
        key: impl BinaryStr,
        options: impl Into<Option<ReadOptions>>,
    ) -> Result<Option<DbVector>> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::Get,
            move |reporter| {
                let options = ReadOptions::from_option(options.into());

                unsafe {
                    reporter.processing_item(key.as_slice().len(), None, || {
                        Self::raw_get(
                            self.handle(),
                            cf.rocks_ptr(),
                            key.as_slice(),
                            options.rocks_ptr(),
                        )
                    })
                }
            },
        )
    }

    /// MultiGets the the values for multiple keys in a single column family
    ///
    /// For each item in `queries`, a corresponding item is returned at the same position in the
    /// sequence.  Each returned item is a `Option<DBVector>`; if a read fails the entire call will
    /// fail. If the key isn't found the result is `None`.
    /// Otherwise it's the contents of the key.
    fn multi_get<CF: db::ColumnFamilyLike, K: BinaryStr>(
        &self,
        cf: &CF,
        keys: impl IntoIterator<Item = K>,
        options: impl Into<Option<ReadOptions>>,
    ) -> Result<Vec<(K, Option<DbPinnableSlice>)>> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::MultiGet,
            move |reporter| {
                let options = ReadOptions::from_option(options.into());

                let keys = keys.into_iter();
                let batch_size = keys.size_hint().0;

                reporter.processing_batch(batch_size, || unsafe {
                    Self::raw_multi_get(self.handle(), cf.rocks_ptr(), keys, options.rocks_ptr())
                })
            },
        )
    }

    /// Asynchronously get the values for multiple keys, returning the value or `None` if not
    /// found.
    fn async_get<CF: db::ColumnFamilyLike, K: BinaryStr + Send + 'static>(
        &self,
        cf: &CF,
        keys: impl IntoIterator<Item = K> + Send + 'static,
        options: impl Into<Option<ReadOptions>>,
    ) -> op_metrics::AsyncOpFuture<Vec<(K, Option<DbPinnableSlice>)>>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncGet,
            move |reporter| {
                let handle = self.handle().clone();
                let cf = cf.clone();
                let options = ReadOptions::from_option(options.into());

                reporter.run_blocking_task(move |reporter| {
                    let keys = keys.into_iter();
                    let batch_size = keys.size_hint().0;

                    reporter.processing_batch(batch_size, || unsafe {
                        Self::raw_multi_get(&handle, cf.rocks_ptr(), keys, options.rocks_ptr())
                    })
                })
            },
        )
    }

    /// Like `async_get`, but operates on a single scalar value.
    ///
    /// It's not clear if this is a good approach.  Is the async overhead too high for a single
    /// record?  More testing is needed.
    fn async_get_scalar<CF: db::ColumnFamilyLike, K: BinaryStr + Send + 'static>(
        &self,
        cf: &CF,
        key: K,
        options: impl Into<Option<ReadOptions>>,
    ) -> op_metrics::AsyncOpFuture<(K, Option<DbVector>)>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncGet,
            move |reporter| {
                let handle = self.handle().clone();
                let cf = cf.clone();
                let options = ReadOptions::from_option(options.into());

                reporter.run_blocking_task(move |reporter| {
                    let opt_value = unsafe {
                        reporter.processing_item(key.as_slice().len(), None, || {
                            Self::raw_get(
                                &handle,
                                cf.rocks_ptr(),
                                key.as_slice(),
                                options.rocks_ptr(),
                            )
                        })?
                    };

                    Ok((key, opt_value))
                })
            },
        )
    }
}

/// Operates semantically like `Get`, but with some additional transaction-related semantics: any
/// key used with `GetForUpdate`, whether that key existed at the time of the call or not, cannot
/// be modified in any other transaction.
pub trait GetForUpdate: RocksOpBase {
    /// Raw version of `get_for_update`, operating on unsafe C bindings.  Not intended for use by external
    /// crates.
    ///
    /// # Safety
    ///
    /// Never call this function; it's intended for internal use by the `roxide`
    /// implementation.
    unsafe fn raw_get_for_update(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<Option<DbVector>>;

    /// Gets the value for `key` in `cf`, returning a Rocks-allocated buffer containing the value
    /// if found, or `None` if not found.
    fn get_for_update(
        &self,
        cf: &impl db::ColumnFamilyLike,
        key: impl BinaryStr,
        options: impl Into<Option<ReadOptions>>,
    ) -> Result<Option<DbVector>> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::GetForUpdate,
            move |reporter| {
                let options = ReadOptions::from_option(options.into());

                unsafe {
                    reporter.processing_item(key.as_slice().len(), None, || {
                        Self::raw_get_for_update(
                            self.handle(),
                            cf.rocks_ptr(),
                            key.as_slice(),
                            options.rocks_ptr(),
                        )
                    })
                }
            },
        )
    }

    /// MultiGets the the values for multiple keys in a single column family
    ///
    /// For each item in `queries`, a corresponding item is returned at the same position in the
    /// sequence.  Each returned item is a `Option<DBVector>`; if a read fails the entire call will
    /// fail. If the key isn't found the result is `None`.
    /// Otherwise it's the contents of the key.
    fn multi_get_for_update<CF: db::ColumnFamilyLike, K: BinaryStr>(
        &self,
        cf: &CF,
        keys: impl IntoIterator<Item = K>,
        options: impl Into<Option<ReadOptions>>,
    ) -> Result<Vec<(K, Option<DbVector>)>> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::MultiGetForUpdate,
            move |reporter| {
                let options = ReadOptions::from_option(options.into());

                let keys = keys.into_iter();
                let results = keys.map(|key| unsafe {
                    reporter.processing_item(key.as_slice().len(), None, || {
                        Self::raw_get_for_update(
                            self.handle(),
                            cf.rocks_ptr(),
                            key.as_slice(),
                            options.rocks_ptr(),
                        )
                        .map(|value| (key, value))
                    })
                });

                // Contort ourselves a bit to pull out the per-item `Result` into one top-level `Result`
                // which will contain whatever the first error was if any
                let results: Result<Vec<_>> = results.collect();

                results
            },
        )
    }

    fn async_get_for_update<CF: db::ColumnFamilyLike, K: BinaryStr + Send + 'static>(
        &self,
        cf: &CF,
        keys: impl IntoIterator<Item = K> + Send + 'static,
        options: impl Into<Option<ReadOptions>>,
    ) -> op_metrics::AsyncOpFuture<Vec<(K, Option<DbVector>)>>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncGetForUpdate,
            move |reporter| {
                let handle = self.handle().clone();
                let cf = cf.clone();
                let options = ReadOptions::from_option(options.into());

                reporter.run_blocking_task(move |reporter| {
                    let keys = keys.into_iter();
                    let results = keys.map(|key| unsafe {
                        reporter.processing_item(key.as_slice().len(), None, || {
                            Self::raw_get_for_update(
                                &handle,
                                cf.rocks_ptr(),
                                key.as_slice(),
                                options.rocks_ptr(),
                            )
                            .map(|value| (key, value))
                        })
                    });

                    // Contort ourselves a bit to pull out the per-item `Result` into one top-level `Result`
                    // which will contain whatever the first error was if any
                    let results: Result<Vec<(K, Option<DbVector>)>> = results.collect();

                    results
                })
            },
        )
    }

    /// Like `async_get_for_update`, but operates on a single scalar value.
    ///
    /// It's not clear if this is a good approach.  Is the async overhead too high for a single
    /// record?  More testing is needed.
    fn async_get_for_update_scalar<CF: db::ColumnFamilyLike, K: BinaryStr + Send + 'static>(
        &self,
        cf: &CF,
        key: K,
        options: impl Into<Option<ReadOptions>>,
    ) -> op_metrics::AsyncOpFuture<(K, Option<DbVector>)>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncGetForUpdate,
            move |reporter| {
                let handle = self.handle().clone();
                let cf = cf.clone();
                let options = ReadOptions::from_option(options.into());

                reporter.run_blocking_task(move |reporter| {
                    let opt_value = unsafe {
                        reporter.processing_item(key.as_slice().len(), None, || {
                            Self::raw_get_for_update(
                                &handle,
                                cf.rocks_ptr(),
                                key.as_slice(),
                                options.rocks_ptr(),
                            )
                        })?
                    };

                    Ok((key, opt_value))
                })
            },
        )
    }
}

impl Get for Db {
    unsafe fn raw_get(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<Option<DbVector>> {
        let mut val_len: libc::size_t = 0;
        let val_ptr = ffi_try!(ffi::rocksdb_get_cf(
            handle.rocks_ptr().as_ptr(),
            options.as_ptr(),
            cf.as_ptr(),
            key.as_ptr() as *const libc::c_char,
            key.len() as libc::size_t,
            &mut val_len,
        ))?;

        if !val_ptr.is_null() {
            Ok(Some(DbVector::from_c(val_ptr as *mut u8, val_len)))
        } else {
            Ok(None)
        }
    }

    unsafe fn raw_multi_get<K: BinaryStr>(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        keys: impl IntoIterator<Item = K>,
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<Vec<(K, Option<DbPinnableSlice>)>> {
        let cpp_db_ptr = Self::raw_get_db_ptr(handle);
        raw_multi_get_impl(cpp_db_ptr, std::ptr::null_mut(), cf, keys, options)
    }
}

impl Get for TransactionDb {
    unsafe fn raw_get(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<Option<DbVector>> {
        let mut val_len: libc::size_t = 0;
        let val_ptr = ffi_try!(ffi::rocksdb_transactiondb_get_cf(
            handle.rocks_ptr().as_ptr(),
            options.as_ptr(),
            cf.as_ptr(),
            key.as_ptr() as *const libc::c_char,
            key.len() as libc::size_t,
            &mut val_len,
        ))?;

        if !val_ptr.is_null() {
            Ok(Some(DbVector::from_c(val_ptr as *mut u8, val_len)))
        } else {
            Ok(None)
        }
    }

    unsafe fn raw_multi_get<K: BinaryStr>(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        keys: impl IntoIterator<Item = K>,
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<Vec<(K, Option<DbPinnableSlice>)>> {
        let cpp_db_ptr = Self::raw_get_db_ptr(handle);
        raw_multi_get_impl(cpp_db_ptr, std::ptr::null_mut(), cf, keys, options)
    }
}

impl Get for OptimisticTransactionDb {
    unsafe fn raw_get(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<Option<DbVector>> {
        // The optimistic transaction DB API is slightly different than either of the others.
        // See the comment in `put.rs` for the details
        Self::with_base_db(handle, |base_db| {
            let mut val_len: libc::size_t = 0;
            let val_ptr = ffi_try!(ffi::rocksdb_get_cf(
                base_db,
                options.as_ptr(),
                cf.as_ptr(),
                key.as_ptr() as *const libc::c_char,
                key.len() as libc::size_t,
                &mut val_len,
            ))?;

            if !val_ptr.is_null() {
                Ok(Some(DbVector::from_c(val_ptr as *mut u8, val_len)))
            } else {
                Ok(None)
            }
        })
    }

    unsafe fn raw_multi_get<K: BinaryStr>(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        keys: impl IntoIterator<Item = K>,
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<Vec<(K, Option<DbPinnableSlice>)>> {
        let cpp_db_ptr = Self::raw_get_db_ptr(handle);
        raw_multi_get_impl(cpp_db_ptr, std::ptr::null_mut(), cf, keys, options)
    }
}

impl Get for unsync::Transaction {
    unsafe fn raw_get(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<Option<DbVector>> {
        let mut val_len: libc::size_t = 0;
        let val_ptr = ffi_try!(ffi::rocksdb_transaction_get_cf(
            handle.rocks_ptr().as_ptr(),
            options.as_ptr(),
            cf.as_ptr(),
            key.as_ptr() as *const libc::c_char,
            key.len() as libc::size_t,
            &mut val_len,
        ))?;

        if !val_ptr.is_null() {
            Ok(Some(DbVector::from_c(val_ptr as *mut u8, val_len)))
        } else {
            Ok(None)
        }
    }

    unsafe fn raw_multi_get<K: BinaryStr>(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        keys: impl IntoIterator<Item = K>,
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<Vec<(K, Option<DbPinnableSlice>)>> {
        raw_multi_get_impl(
            std::ptr::null_mut(),
            handle.rocks_ptr().as_ptr(),
            cf,
            keys,
            options,
        )
    }
}

impl GetForUpdate for unsync::Transaction {
    unsafe fn raw_get_for_update(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        key: &[u8],
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<Option<DbVector>> {
        let mut val_len: libc::size_t = 0;
        let mut val_ptr: *mut libc::c_char = std::ptr::null_mut();

        let status = tx_get_for_update(
            handle.rocks_ptr().as_ptr(),
            options.as_ptr(),
            cf.as_ptr(),
            key.as_ptr() as *const libc::c_char,
            key.len() as libc::size_t,
            &mut val_ptr,
            &mut val_len,
        )
        .into_status();

        if status.is_ok() {
            // Operation succeeded and value was found
            Ok(Some(DbVector::from_c(val_ptr as *mut u8, val_len)))
        } else if status.code() == status::Code::NotFound {
            // Operation failed but only because the value was not found
            Ok(None)
        } else {
            // Error
            status.into_result()?;
            unreachable!()
        }
    }
}

// The implementation for the thread-safe `Transaction` is just a wrapper around the
// `unsync::Transaction` implementation so it takes a rather different form.
impl Get for sync::Transaction {
    unsafe fn raw_get(
        _handle: &Self::HandleType,
        _cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        _key: &[u8],
        _options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<Option<DbVector>> {
        unreachable!()
    }

    unsafe fn raw_multi_get<K: BinaryStr>(
        _handle: &Self::HandleType,
        _cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        _keys: impl IntoIterator<Item = K>,
        _options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<Vec<(K, Option<DbPinnableSlice>)>> {
        unreachable!()
    }

    fn get(
        &self,
        cf: &impl db::ColumnFamilyLike,
        key: impl BinaryStr,
        options: impl Into<Option<ReadOptions>>,
    ) -> Result<Option<DbVector>> {
        self.with_tx(move |tx| tx.get(cf, key, options))
    }

    fn multi_get<CF: db::ColumnFamilyLike, K: BinaryStr>(
        &self,
        cf: &CF,
        keys: impl IntoIterator<Item = K>,
        options: impl Into<Option<ReadOptions>>,
    ) -> Result<Vec<(K, Option<DbPinnableSlice>)>> {
        self.with_tx(move |tx| tx.multi_get(cf, keys, options))
    }

    /// Asynchronously get the values for multiple keys, returning the value or `None` if not
    /// found.
    fn async_get<CF: db::ColumnFamilyLike, K: BinaryStr + Send + 'static>(
        &self,
        cf: &CF,
        keys: impl IntoIterator<Item = K> + Send + 'static,
        options: impl Into<Option<ReadOptions>>,
    ) -> op_metrics::AsyncOpFuture<Vec<(K, Option<DbPinnableSlice>)>>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        let options = options.into();
        let cf_clone = cf.clone();

        // the async version isn't available on `unsync::Transaction`; that's almost the entire
        // reason for having this `sync` version.  Instead, just call the sync version from a
        // worker thread
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncGet,
            move |_reporter| self.async_with_tx(move |tx| tx.multi_get(&cf_clone, keys, options)),
        )
    }

    fn async_get_scalar<CF: db::ColumnFamilyLike, K: BinaryStr + Send + 'static>(
        &self,
        cf: &CF,
        key: K,
        options: impl Into<Option<ReadOptions>>,
    ) -> op_metrics::AsyncOpFuture<(K, Option<DbVector>)>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        let options = options.into();
        let cf_clone = cf.clone();

        // the async version isn't available on `unsync::Transaction`; that's almost the entire
        // reason for having this `sync` version.  Instead, just call the sync version from a
        // worker thread
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncGet,
            move |reporter| {
                self.async_with_tx(move |tx| {
                    reporter.processing_item(key.as_slice().len(), None, || {
                        let opt_value = tx.get(&cf_clone, key.as_slice(), options)?;
                        Ok((key, opt_value))
                    })
                })
            },
        )
    }
}

impl GetForUpdate for sync::Transaction {
    unsafe fn raw_get_for_update(
        _handle: &Self::HandleType,
        _cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        _key: &[u8],
        _options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<Option<DbVector>> {
        unreachable!()
    }

    /// Gets the value for `key` in `cf`, returning a Rocks-allocated buffer containing the value
    /// if found, or `None` if not found.
    fn get_for_update(
        &self,
        cf: &impl db::ColumnFamilyLike,
        key: impl BinaryStr,
        options: impl Into<Option<ReadOptions>>,
    ) -> Result<Option<DbVector>> {
        self.with_tx(move |tx| tx.get_for_update(cf, key, options))
    }

    /// MultiGets the the values for multiple keys in a single column family
    ///
    /// For each item in `queries`, a corresponding item is returned at the same position in the
    /// sequence.  Each returned item is a `Option<DBVector>`; if a read fails the entire call will
    /// fail. If the key isn't found the result is `None`.
    /// Otherwise it's the contents of the key.
    fn multi_get_for_update<CF: db::ColumnFamilyLike, K: BinaryStr>(
        &self,
        cf: &CF,
        keys: impl IntoIterator<Item = K>,
        options: impl Into<Option<ReadOptions>>,
    ) -> Result<Vec<(K, Option<DbVector>)>> {
        self.with_tx(move |tx| tx.multi_get_for_update(cf, keys, options))
    }

    fn async_get_for_update<CF: db::ColumnFamilyLike, K: BinaryStr + Send + 'static>(
        &self,
        cf: &CF,
        keys: impl IntoIterator<Item = K> + Send + 'static,
        options: impl Into<Option<ReadOptions>>,
    ) -> op_metrics::AsyncOpFuture<Vec<(K, Option<DbVector>)>>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        let options = options.into();
        let cf_clone = cf.clone();

        // the async version isn't available on `unsync::Transaction`; that's almost the entire
        // reason for having this `sync` version.  Instead, just call the sync version from a
        // worker thread
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncGet,
            move |_reporter| {
                self.async_with_tx(move |tx| tx.multi_get_for_update(&cf_clone, keys, options))
            },
        )
    }

    fn async_get_for_update_scalar<CF: db::ColumnFamilyLike, K: BinaryStr + Send + 'static>(
        &self,
        cf: &CF,
        key: K,
        options: impl Into<Option<ReadOptions>>,
    ) -> op_metrics::AsyncOpFuture<(K, Option<DbVector>)>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        let options = options.into();
        let cf_clone = cf.clone();

        // the async version isn't available on `unsync::Transaction`; that's almost the entire
        // reason for having this `sync` version.  Instead, just call the sync version from a
        // worker thread
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncGet,
            move |reporter| {
                self.async_with_tx(move |tx| {
                    reporter.processing_item(key.as_slice().len(), None, || {
                        let opt_value = tx.get_for_update(&cf_clone, key.as_slice(), options)?;
                        Ok((key, opt_value))
                    })
                })
            },
        )
    }
}

#[cfg(test)]
mod test {
    use super::super::put::Put;
    use super::*;
    use crate::db::DbLike;
    use crate::error;
    use crate::ops::begin_tx::BeginTrans;
    use crate::test::TempDbPath;

    #[test]
    fn db_simple_get() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        assert!(db.get(&cf, "foo", None)?.is_none());
        db.put(&cf, "foo", "bar", None)?;
        assert_eq!("bar", db.get(&cf, "foo", None)?.unwrap().to_string_lossy());

        Ok(())
    }

    #[test]
    fn txdb_simple_get() -> Result<()> {
        let path = TempDbPath::new();
        let db = TransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        assert!(db.get(&cf, "foo", None)?.is_none());
        db.put(&cf, "foo", "bar", None)?;
        assert_eq!("bar", db.get(&cf, "foo", None)?.unwrap().to_string_lossy());

        Ok(())
    }

    #[test]
    fn opt_txdb_simple_get() -> Result<()> {
        let path = TempDbPath::new();
        let db = OptimisticTransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        assert!(db.get(&cf, "foo", None)?.is_none());
        db.put(&cf, "foo", "bar", None)?;
        assert_eq!("bar", db.get(&cf, "foo", None)?.unwrap().to_string_lossy());

        Ok(())
    }

    #[test]
    fn tx_simple_get() -> Result<()> {
        let path = TempDbPath::new();
        let db = TransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let tx = db.begin_trans(None, None)?;
        tx.put(&cf, "foo", "bar", None)?;
        tx.put(&cf, "baz", "boo", None)?;
        assert_eq!("bar", tx.get(&cf, "foo", None)?.unwrap().to_string_lossy());

        Ok(())
    }

    #[test]
    fn sync_tx_simple_get() -> Result<()> {
        let path = TempDbPath::new();
        let db = TransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let tx = db.begin_trans(None, None)?.into_sync();
        tx.put(&cf, "foo", "bar", None)?;
        tx.put(&cf, "baz", "boo", None)?;
        assert_eq!("bar", tx.get(&cf, "foo", None)?.unwrap().to_string_lossy());

        Ok(())
    }

    #[test]
    fn db_simple_multi_get() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let keys: Vec<_> = (0..1000).map(|n| format!("key{}", n)).collect();
        let num_keys = keys.len();

        let results = db.multi_get(&cf, &keys, None)?;
        assert_eq!(num_keys, results.len());

        // None of these keys exist yet, so expect them all to be None
        for (_key, value) in results.into_iter() {
            assert_eq!(None, value);
        }

        // Now write every even-numbered key to the database
        let put_pairs: Vec<_> = (0..1000u32)
            .filter(|n| n % 2 == 0)
            .map(|n| (format!("key{}", n), format!("value{}", n)))
            .collect();

        for (k, v) in put_pairs.iter() {
            db.put(&cf, k, v, None)?;
        }

        let results = db.multi_get(&cf, &keys, None)?;
        assert_eq!(keys.len(), results.len());

        for (idx, (_key, value)) in results.into_iter().enumerate() {
            if idx % 2 == 0 {
                assert_eq!(format!("value{}", idx), value.unwrap().to_string_lossy());
            } else {
                assert_eq!(None, value);
            }
        }

        Ok(())
    }

    #[test]
    fn tx_simple_multi_get() -> Result<()> {
        let path = TempDbPath::new();
        let db = TransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let tx = db.begin_trans(None, None)?;

        let keys: Vec<_> = (0..1000).map(|n| format!("key{}", n)).collect();
        let num_keys = keys.len();

        let results = tx.multi_get(&cf, &keys, None)?;
        assert_eq!(num_keys, results.len());

        // None of these keys exist yet, so expect them all to be None
        for (_key, value) in results.into_iter() {
            assert_eq!(None, value);
        }

        // Now write every even-numbered key to the database
        let put_pairs: Vec<_> = (0..1000u32)
            .filter(|n| n % 2 == 0)
            .map(|n| (format!("key{}", n), format!("value{}", n)))
            .collect();

        for (k, v) in put_pairs.iter() {
            tx.put(&cf, k, v, None)?;
        }

        let results = tx.multi_get(&cf, &keys, None)?;
        assert_eq!(keys.len(), results.len());

        for (idx, (_key, value)) in results.into_iter().enumerate() {
            if idx % 2 == 0 {
                assert_eq!(format!("value{}", idx), value.unwrap().to_string_lossy());
            } else {
                assert_eq!(None, value);
            }
        }

        tx.commit()?;

        let results = db.multi_get(&cf, &keys, None)?;
        assert_eq!(keys.len(), results.len());

        for (idx, (_key, value)) in results.into_iter().enumerate() {
            if idx % 2 == 0 {
                assert_eq!(format!("value{}", idx), value.unwrap().to_string_lossy());
            } else {
                assert_eq!(None, value);
            }
        }

        Ok(())
    }

    #[test]
    fn txdb_get_for_update_conflict() -> Result<()> {
        // Deliberately cause a conflict using `GetForUpdate` with a `TransactionDB`
        // `TransactionDB` uses pessimistic locking, so the conflict is reported on the conflicting
        // `get_for_update` call.
        let path = TempDbPath::new();
        let db = TransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        // Write all keys to the database
        let put_pairs: Vec<_> = (0..1000u32)
            .map(|n| (format!("key{}", n), format!("value{}", n)))
            .collect();

        for (k, v) in put_pairs.iter() {
            db.put(&cf, k, v, None)?;
        }

        // Start two transactions
        let tx1 = db.begin_trans(None, None)?;
        let tx2 = db.begin_trans(None, None)?;

        // In one transaction, get and update one of the keys
        let _value1 = tx1.get_for_update(&cf, b"key0", None)?;
        let value2 = tx2.get_for_update(&cf, b"key0", None);

        // In tx2, there is now a conflict, because the key `key0` has already been "locked" by the
        // `get_for_update` call in tx1.  `tx2` will time out waiting to acquire the lock on this
        // key.
        match value2 {
            Err(error::Error::RocksDbLockTimeout { .. }) => {
                // Expected result
                Ok(())
            }
            other => {
                panic!("Expected a lock timeout error, but got: {:?}", other);
            }
        }
    }

    /// Similar to txdb_get_for_update_conflict but uses the async scalar version which
    /// historically has had some bugs
    #[tokio::test]
    async fn txdb_async_get_for_update_conflict() -> Result<()> {
        // Deliberately cause a conflict using `GetForUpdate` with a `TransactionDB`
        // `TransactionDB` uses pessimistic locking, so the conflict is reported on the conflicting
        // `get_for_update` call.
        let path = TempDbPath::new();
        let db = TransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        // Write all keys to the database
        let put_pairs: Vec<_> = (0..1000u32)
            .map(|n| (format!("key{}", n), format!("value{}", n)))
            .collect();

        for (k, v) in put_pairs.iter() {
            db.put(&cf, k, v, None)?;
        }

        // Start two transactions
        let tx1 = db.async_begin_trans(None, None).await?;
        let tx2 = db.async_begin_trans(None, None).await?;

        // In one transaction, get and update one of the keys
        let _value1 = tx1.async_get_for_update_scalar(&cf, b"key0", None).await?;
        let value2 = tx2.async_get_for_update_scalar(&cf, b"key0", None).await;

        // In tx2, there is now a conflict, because the key `key0` has already been "locked" by the
        // `get_for_update` call in tx1.  `tx2` will time out waiting to acquire the lock on this
        // key.
        match value2 {
            Err(error::Error::RocksDbLockTimeout { .. }) => {
                // Expected result
                Ok(())
            }
            other => {
                panic!("Expected a lock timeout error, but got: {:?}", other);
            }
        }
    }

    #[test]
    fn opt_txdb_get_for_update_conflict() -> Result<()> {
        // Deliberately cause a conflict using `GetForUpdate` with a `OptimisticTransactionDb`
        // `OptimisticTransactionDb` uses optimistic locking, so the conflict is reported on the conflicting
        // `commit` call.
        let path = TempDbPath::new();
        let db = OptimisticTransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        // Write all keys to the database
        let put_pairs: Vec<_> = (0..1000u32)
            .map(|n| (format!("key{}", n), format!("value{}", n)))
            .collect();

        for (k, v) in put_pairs.iter() {
            db.put(&cf, k, v, None)?;
        }

        // Start two transactions
        let tx1 = db.begin_trans(None, None)?;
        let tx2 = db.begin_trans(None, None)?;

        // In one transaction, get and update one of the keys
        let _value1 = tx1.get_for_update(&cf, b"key0", None)?;
        let _value2 = tx2.get_for_update(&cf, b"key0", None)?;

        tx1.put(&cf, b"key0", "value0a", None)?;
        tx2.put(&cf, b"key0", "value0b", None)?;

        tx1.commit()?;

        // This commit should fail, because `key0` was already modified in tx1
        let result = tx2.commit();

        match result {
            Err(error::Error::RocksDbConflict { .. }) => {
                // Expected result
                Ok(())
            }
            other => {
                panic!("Expected a busy error, but got: {:?}", other);
            }
        }
    }

    #[tokio::test]
    async fn db_async_get() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let keys: Vec<_> = (0..1000).map(|n| format!("key{}", n)).collect();
        let num_keys = keys.len();

        let results = db.async_get(&cf, keys.clone(), None).await?;
        assert_eq!(num_keys, results.len());

        // None of these keys exist yet, so expect them all to be None
        for (_key, value) in results.into_iter() {
            assert_eq!(None, value);
        }

        // Now write every even-numbered key to the database
        let put_pairs: Vec<_> = (0..1000u32)
            .filter(|n| n % 2 == 0)
            .map(|n| (format!("key{}", n), format!("value{}", n)))
            .collect();

        db.async_put(&cf, put_pairs, None).await?;

        let results = db.async_get(&cf, keys.clone(), None).await?;
        assert_eq!(keys.len(), results.len());

        for (idx, (_key, value)) in results.into_iter().enumerate() {
            if idx % 2 == 0 {
                assert_eq!(format!("value{}", idx), value.unwrap().to_string_lossy());
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn tx_async_get_scalar() -> Result<()> {
        let path = TempDbPath::new();
        let db = TransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();
        let tx = db.async_begin_trans(None, None).await?;

        let keys: Vec<_> = (0..1000).map(|n| format!("key{}", n)).collect();

        // None of these keys exist yet, so expect them all to be None
        for key in keys.clone() {
            assert_eq!(None, tx.async_get_scalar(&cf, key, None).await?.1);
        }

        // Now write every even-numbered key to the database
        let put_pairs: Vec<_> = (0..1000u32)
            .filter(|n| n % 2 == 0)
            .map(|n| (format!("key{}", n), format!("value{}", n)))
            .collect();

        for (key, value) in put_pairs {
            tx.async_put_scalar(&cf, key.clone(), value.clone(), None)
                .await?;
            let (res_key, res_value) = tx.async_get_scalar(&cf, key.clone(), None).await?;

            assert_eq!(key, res_key);
            assert_eq!(
                Some(value),
                res_value.map(|v| String::from_utf8_lossy(v.as_slice()).to_string())
            );
        }

        Ok(())
    }
}
