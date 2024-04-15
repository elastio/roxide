//! The Iterate operation creates a `RocksIterator` (not to be confused with Rust's `Iterator`) to
//! sequentially read key/value pairs in sorted order.
use super::op_metrics;
use super::*;
use crate::db::{self, db::*, opt_txdb::*, txdb::*};
use crate::db_options::ReadOptions;
use crate::ffi;
use crate::handle::{self, RocksObject, RocksObjectDefault};
use crate::iterator::{DbRangeIterator, RocksIterator, UnboundedIterator};
use crate::tx::{sync, unsync};
use crate::{Error, Result};
use std::ptr;
use std::ptr::NonNull;

/// Internal helper method that sets the lower and upper range boundaries on the `ReadOptions`
/// struct.
///
/// The important logic here is that the keys are copied into an owned vector, then "forgotten" by
/// Rust and converted into raw pointer/length pairs.  This guarantees that Rust will not move the
/// memory associated with these keys.
///
/// It's important that this be converted back to a Rust-managed type so the memory can be freed.
fn set_option_ranges<K: BinaryStr>(
    options: &mut ReadOptions,
    key_range: impl Into<OpenKeyRange<K>>,
) -> OpenKeyRange<RawVec> {
    // Convert the keys into owned Vec<u8> buffers
    let key_range = key_range.into().to_owned();

    // Convert those owned Vec<u8> buffers into raw pointer/length tuples for use with Rocks
    let key_range = key_range.into_raw_parts();
    key_range.set_read_options_bounds(options);

    key_range
}

/// The low-level RocksDB iteration API
///
/// This is not part of the public roxide API, but it's the base upon which the public API
/// is built.
///
/// Internal implementations for all of `IterateAll`, `IterateRange`, and `IteratePrefix` are
/// present here.  Not all implementaitons will implement all of these; the default implementation
/// just panics.  Users of this crate are not expected to call any of these methods; they should
/// instead use the public versions on the corresponding `Iterate*` trait.
#[doc(hidden)]
pub trait IterateInternal: RocksOpBase {
    type InternalRangeIter;
    type InternalPrefixIter;

    /// Raw version of `iterate`, for use by the internal implementation.
    ///
    /// Not intended to be called directly.
    ///
    /// # Safety
    ///
    /// Never call this function; it's intended for internal use by the `roxide`
    /// implementation.
    unsafe fn raw_iterate(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<*mut ffi::rocksdb_iterator_t>;

    fn internal_iterate_all(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
    ) -> Result<UnboundedIterator> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::IterateAll,
            move |_reporter| {
                let options = ReadOptions::from_option(options.into());

                let iterator_ptr = unsafe {
                    Self::raw_iterate(self.handle(), cf.rocks_ptr(), options.rocks_ptr())?
                };

                if let Some(iterator_ptr) = ptr::NonNull::new(iterator_ptr) {
                    Ok(UnboundedIterator::new(
                        handle::AnonymousHandle::wrap_opaque_type(self.handle().clone()),
                        iterator_ptr,
                        options,
                    ))
                } else {
                    Err(Error::other_error("Failed to create iterator"))
                }
            },
        )
    }

    fn internal_async_iterate_all(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
    ) -> op_metrics::AsyncOpFuture<UnboundedIterator>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncIterateAll,
            move |_reporter| {
                let cf = cf.clone();
                let options = ReadOptions::from_option(options);
                let handle = self.handle().clone();

                elasyncio::run_blocking_task(move || {
                    let iterator_ptr =
                        unsafe { Self::raw_iterate(&handle, cf.rocks_ptr(), options.rocks_ptr())? };

                    if let Some(iterator_ptr) = ptr::NonNull::new(iterator_ptr) {
                        Ok(UnboundedIterator::new(
                            handle::AnonymousHandle::wrap_opaque_type(handle.clone()),
                            iterator_ptr,
                            options,
                        ))
                    } else {
                        Err(Error::other_error("Failed to create iterator"))
                    }
                })
            },
        )
    }
}

/// Iterate over all of the records in a column family.
///
/// All iterators implement [`crate::iterator::RocksIterator`].
pub trait IterateAll: RocksOpBase + IterateInternal {
    /// Creates an iterator that will iterate over the contents of the column family
    fn iterate_all(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
    ) -> Result<UnboundedIterator> {
        // This is always supported by every implementation
        self.internal_iterate_all(cf, options)
    }

    /// Async version of `iterate_all`
    fn async_iterate_all(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
    ) -> op_metrics::AsyncOpFuture<UnboundedIterator>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        self.internal_async_iterate_all(cf, options)
    }

    /// Creates an iterator that will iterate over the contents of a column family if that column
    /// family is configured with a prefix extractor.
    ///
    /// The iterator returned by this function is very subtly different than the `iterate_all`
    /// iterator, and should be used only under very specific circumstances.  Specifically, this
    /// iterator *might* or *might not* restrict itself to only those keys that have the same
    /// prefix as the key passed as `start_key`.  Callers should not rely on either one behavior
    /// or the other.  If a caller requires an iterator that is always limited to keys with a given
    /// prefix, then use the `IteratePrefix` methods.
    ///
    /// However, the problem with `IteratePrefix` is that it's not supported by RocksDB on
    /// transactions.  So requiring `IteratePrefix` means you are requiring direct database access
    /// and don't support transaction isolation in your code.
    ///
    /// If however your code can accept the possibility that this iterate might behave like an
    /// unbounded iterator or might behave like a prefix iterator, then you can use this method
    /// instead.  If called on a database object, it will produce a prefix iterator (with all of
    /// the RocksDB performance advantages that implies).  If called on a transaction object, it
    /// will produce an unbounded iterator, which doesn't know anything about your prefix hint.
    ///
    /// # Prefix hint
    ///
    /// Assuming your column family is configured at open time to use a prefix extractor, Rocks
    /// will use that extractor every time you give it a key for an iterator seek (or even Get)
    /// operation.  The parameter `start_key` should be a full key, *not* just the prefix part.
    /// Rocks internally will pass this prefix hint to the prefix extractor to determine what
    /// prefix it correponds to.
    ///
    /// # Implementation
    ///
    /// The default implementation ignores the prefix hint and calls `iterate_all`.
    /// Implementations which support prefix iterating should override this.
    ///
    /// # Significant restrictions
    ///
    /// See [`IteratePrefix`] to understand the restrictions that apply to prefix iterators.  In
    /// addition to those, because `iterate_with_prefix_hint` might or might not actually make a
    /// prefix iterator depending upon implementation, there are additional restrictions on the use
    /// of this iterator in addition to those specified in [`IteratePrefix`]:
    ///
    /// * You must not make any assumptions about the current position of the iterator when it's
    /// returned.  If you do not explicity call one of the `seek` or `from` methods, the contents
    /// of the iterator's current value are effectively randomized.
    fn iterate_with_prefix_hint(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
        start_key: impl BinaryStr,
    ) -> Result<UnboundedIterator> {
        let _ = start_key;
        self.iterate_all(cf, options)
    }

    /// Async version of `iterate_with_prefix_hint`
    fn async_iterate_with_prefix_hint(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
        start_key: impl BinaryStr + Send + 'static,
    ) -> op_metrics::AsyncOpFuture<UnboundedIterator>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        let _ = start_key;
        self.async_iterate_all(cf, options)
    }
}

/// Iterate over a pre-defined range of key values in a column family.
pub trait IterateRange: RocksOpBase + IterateInternal {
    type RangeIter: RocksIterator;

    /// When you know you are interested only in a specific range of keys, you can optimize
    /// iterator performance by specifying that range.
    ///
    /// # Usage
    ///
    /// The `start` of `key_range` specifies the first key to iterate over.  It's equivalent to
    /// calling `seek` on the returned iterator.
    ///
    /// The `end` of `key_range` specifies the key lexicographically _after_ the last key to
    /// iterate over.  So if you specify a range `(b"foo1", b"foo3")`, only keys `b"foo1"` and
    /// `b"foo2"` will be included in the range.
    fn iterate_range<K: BinaryStr>(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
        key_range: impl Into<OpenKeyRange<K>>,
    ) -> Result<Self::RangeIter>;

    /// Async version of `iterate_range`
    fn async_iterate_range<K: BinaryStr + Send + 'static>(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
        key_range: impl Into<OpenKeyRange<K>>,
    ) -> op_metrics::AsyncOpFuture<Self::RangeIter>
    where
        <Self as RocksOpBase>::HandleType: Sync,
    {
        // Default implementation required because `unsync::Transaction` can't implemetn
        // `async_iterate_range` because it's handle isn't `Sync`
        let _ = (cf, key_range, options);
        unimplemented!()
    }

    /// Creates an iterator that is limited to a specific range of values, AND that entire range
    /// falls within the same prefix as defined by the column family's prefix extractor.
    ///
    /// All of the dire warnings on [`IteratePrefix`] about being careful and you probably don't
    /// want to use this apply double here.
    fn iterate_prefix_range<K: BinaryStr>(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
        key_range: impl Into<OpenKeyRange<K>>,
    ) -> Result<Self::RangeIter>
    where
        Self: IteratePrefix,
    {
        // This is a special case of iterate range
        //
        // It somewhat inelegantly just assumes that anything that implements `IteratePrefix` will
        // accept the `prefix_same_as_start` option in its options.  That happens to be true, but
        // it feels inelegant to make that assumption.
        let options = options.into().unwrap_or_default();
        unsafe {
            ffi::rocksdb_readoptions_set_prefix_same_as_start(options.inner, true as libc::c_uchar);
        }

        self.iterate_range(cf, options, key_range)
    }

    /// Async version of `iterate_prefix_range`
    fn async_iterate_prefix_range<K: BinaryStr + Send + 'static>(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
        key_range: impl Into<OpenKeyRange<K>>,
    ) -> op_metrics::AsyncOpFuture<Self::RangeIter>
    where
        <Self as RocksOpBase>::HandleType: Sync,
    {
        let options = options.into().unwrap_or_default();
        unsafe {
            ffi::rocksdb_readoptions_set_prefix_same_as_start(options.inner, true as libc::c_uchar);
        }

        self.async_iterate_range(cf, options, key_range)
    }
}

/// Iterate over a values with the same key prefix.
///
/// Prefix iteration requires that the column family was configured with a prefix extractor.  If
/// records have a common prefix that can be used to limit the range of keys being accessed, this
/// can provide a performance optimization.
///
/// Prefix iteration is complex and fraught with peril.  Read the [RocksDB wiki docs on prefix seek][wiki]
/// and see if you still want to do prefix iteration after that.
///
/// In particular you must be aware of the following restrictions:
///
/// * Make no assumptions about the current position of the cursor when `iterate_prefix` returns.
/// Position the cursor explicitly with `from_key` or `from_key_reverse`.
/// * `from_end` and `from_start` may or may not work correctly.  The RocksDB wiki is says about
/// `SeekToLast` (which `from_end` uses) and `SeekToFirst` (which `from_start` uses):
///   > is not supported well with prefix iterating.  SeekToFirst() is only supported by some configurations.
/// * Once the iterator has reached the end of its data, you should not attempt to re-use it, even
/// if you are seeking it back to some key you've already accessed.
/// * In general, this is only useful for cases when you have some sequential lookups you need to
/// do and they all share the same prefix.  You should be very wary of using prefix iteration
/// unless you absolutely need to for performance reasons.
///
///
/// [wiki]: https://github.com/facebook/rocksdb/wiki/Prefix-Seek
pub trait IteratePrefix: RocksOpBase + IterateInternal {
    type PrefixIter: RocksIterator;

    /// If the column family `cf` has a non-null `prefix_extractor`, it means it's working in
    /// prefix mode.  That is an optimization that lets RocksDB better optimize the structures for
    /// range lookups with a common prefix.
    ///
    /// In this mode, calling `iterate_prefix` configures the iterator to become invalid when it
    /// scans past the last record with the same prefix.
    ///
    /// # Key
    ///
    /// Assuming your column family is configured at open time to use a prefix extractor, Rocks
    /// will use that extractor every time you give it a start_key for an iterator seek (or even Get)
    /// operation.  The parameter `start_key` should be a full start_key, *not* just the prefix part.
    /// Rocks internally will pass this start_key to the prefix extractor to determine what
    /// prefix it correponds to.  Attempting to seek this iterator outside of the range of keys
    /// with the same prefix leads to undefined behavior.  Don't do it.
    ///
    /// Assuming a record at `start_key` exists, the iterator will initially be positioned at that
    /// record.  If no such record exists, the iterator will be positioned at whatever start_key comes
    /// after `start_key` in the sort order while still having the same prefix.  If there is no such
    /// record the iterator is empty.
    ///
    /// Note that you are free to move this iterator to other keys, as long as they all have the
    /// same prefix as `start_key`.
    fn iterate_prefix(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
        start_key: impl BinaryStr,
    ) -> Result<Self::PrefixIter>;

    /// Async version of `iterate_prefix`
    fn async_iterate_prefix(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
        start_key: impl BinaryStr + Send + 'static,
    ) -> op_metrics::AsyncOpFuture<Self::PrefixIter>
    where
        <Self as RocksOpBase>::HandleType: Sync;
}

// All three database types support all iteration, including range and prefix
impl IterateInternal for Db {
    type InternalRangeIter = DbRangeIterator;
    type InternalPrefixIter = UnboundedIterator;

    unsafe fn raw_iterate(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<*mut ffi::rocksdb_iterator_t> {
        let iterator_ptr = ffi::rocksdb_create_iterator_cf(
            handle.as_ptr(),
            options.rocks_ptr().as_ptr(),
            cf.rocks_ptr().as_ptr(),
        );

        Ok(iterator_ptr)
    }
}

impl AutoIterateRange for Db {}

impl IterateInternal for TransactionDb {
    type InternalRangeIter = DbRangeIterator;
    type InternalPrefixIter = UnboundedIterator;

    unsafe fn raw_iterate(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<*mut ffi::rocksdb_iterator_t> {
        let iterator_ptr = ffi::rocksdb_transactiondb_create_iterator_cf(
            handle.as_ptr(),
            options.rocks_ptr().as_ptr(),
            cf.rocks_ptr().as_ptr(),
        );

        Ok(iterator_ptr)
    }
}

impl AutoIterateRange for TransactionDb {}

impl IterateInternal for OptimisticTransactionDb {
    type InternalRangeIter = DbRangeIterator;
    type InternalPrefixIter = UnboundedIterator;

    unsafe fn raw_iterate(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<*mut ffi::rocksdb_iterator_t> {
        // The optimistic transaction DB API is slightly different than either of the others.
        // See the comment in `put.rs` for the details
        Self::with_base_db(handle, |base_db| {
            Ok(ffi::rocksdb_create_iterator_cf(
                base_db,
                options.rocks_ptr().as_ptr(),
                cf.rocks_ptr().as_ptr(),
            ))
        })
    }
}

impl AutoIterateRange for OptimisticTransactionDb {}

/// Implement IterateAll for the types that support `IteratePrefix` (meaning the
/// `iterate_with_prefix_hint` functions should actually tell Rocks about the prefix).
///
/// In effect this means implementing `IterateAll` for `DB`, TransactionDB`, and
/// `OptimisticTransactionDb`
impl<
        T: IterateInternal<InternalPrefixIter = UnboundedIterator>
            + IteratePrefix<PrefixIter = UnboundedIterator>,
    > IterateAll for T
{
    // Override the default implementation to actually use prefix iterators

    fn iterate_with_prefix_hint(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
        start_key: impl BinaryStr,
    ) -> Result<UnboundedIterator> {
        self.iterate_prefix(cf, options, start_key)
    }

    /// Async version of `iterate_with_prefix_hint`
    fn async_iterate_with_prefix_hint(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
        start_key: impl BinaryStr + Send + 'static,
    ) -> op_metrics::AsyncOpFuture<UnboundedIterator>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        self.async_iterate_prefix(cf, options, start_key)
    }
}

/// Marker trait for automatic `IterateRange` implementation.
trait AutoIterateRange {}

/// Implement range iteration for DB-based impls of `IterateInternal` marked with `AutoIterateRange`.
impl<T: IterateInternal<InternalRangeIter = DbRangeIterator> + AutoIterateRange> IterateRange
    for T
{
    type RangeIter = DbRangeIterator;

    fn iterate_range<K: BinaryStr>(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
        key_range: impl Into<OpenKeyRange<K>>,
    ) -> Result<Self::RangeIter> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::IterateRange,
            move |_reporter| {
                // Putting a range bound on the iterator as actually done by setting the relevant
                // `ReadOptions` values.
                let mut options = options.into().unwrap_or_default();
                let key_range = set_option_ranges(&mut options, key_range);

                let iterator_ptr = unsafe {
                    Self::raw_iterate(self.handle(), cf.rocks_ptr(), options.rocks_ptr())?
                };

                if let Some(iterator_ptr) = ptr::NonNull::new(iterator_ptr) {
                    Ok(DbRangeIterator::new(
                        handle::AnonymousHandle::wrap_opaque_type(self.handle().clone()),
                        iterator_ptr,
                        options,
                        key_range,
                    ))
                } else {
                    // Free the range memory first
                    unsafe {
                        key_range.free();
                    }

                    Err(Error::other_error("Failed to create iterator"))
                }
            },
        )
    }

    fn async_iterate_range<K: BinaryStr + Send + 'static>(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
        key_range: impl Into<OpenKeyRange<K>>,
    ) -> op_metrics::AsyncOpFuture<Self::RangeIter>
    where
        <Self as RocksOpBase>::HandleType: Sync,
    {
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncIterateRange,
            move |_reporter| {
                let cf = cf.clone();
                // Putting a range bound on the iterator as actually done by setting the relevant
                // `ReadOptions` values.
                let mut options = options.into().unwrap_or_default();
                let key_range = set_option_ranges(&mut options, key_range);

                let handle = self.handle().clone();

                elasyncio::run_blocking_task(move || {
                    let iterator_ptr =
                        unsafe { Self::raw_iterate(&handle, cf.rocks_ptr(), options.rocks_ptr())? };

                    if let Some(iterator_ptr) = ptr::NonNull::new(iterator_ptr) {
                        Ok(DbRangeIterator::new(
                            handle::AnonymousHandle::wrap_opaque_type(handle.clone()),
                            iterator_ptr,
                            options,
                            key_range,
                        ))
                    } else {
                        // Free the range memory first
                        unsafe {
                            key_range.free();
                        }

                        Err(Error::other_error("Failed to create iterator"))
                    }
                })
            },
        )
    }
}

/// Implement IteratePrefix for all the DB implementations.  Since we know all of those DB impls
/// implement `iterate_with_prefix_hint` using native Rocks prefix iteration, we'll just call into
/// those impls.
impl<T: Sync + IterateInternal<InternalPrefixIter = UnboundedIterator>> IteratePrefix for T {
    type PrefixIter = UnboundedIterator;

    fn iterate_prefix(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
        start_key: impl BinaryStr,
    ) -> Result<Self::PrefixIter> {
        op_metrics::instrument_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::IteratePrefix,
            move |_reporter| {
                let options = options.into().unwrap_or_default();
                unsafe {
                    ffi::rocksdb_readoptions_set_prefix_same_as_start(
                        options.inner,
                        true as libc::c_uchar,
                    );
                }

                let iterator_ptr = unsafe {
                    Self::raw_iterate(self.handle(), cf.rocks_ptr(), options.rocks_ptr())?
                };

                if let Some(iterator_ptr) = ptr::NonNull::new(iterator_ptr) {
                    let mut iter = UnboundedIterator::new(
                        handle::AnonymousHandle::wrap_opaque_type(self.handle().clone()),
                        iterator_ptr,
                        ReadOptions::from_option(options),
                    );

                    // By seeking to this prefix hint start_key, we're locking the iterator into only
                    // working on keys with the same prefix as the hint start_key.
                    iter.from_key(start_key);

                    Ok(iter)
                } else {
                    Err(Error::other_error("Failed to create iterator"))
                }
            },
        )
    }

    fn async_iterate_prefix(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
        start_key: impl BinaryStr + Send + 'static,
    ) -> op_metrics::AsyncOpFuture<Self::PrefixIter>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncIteratePrefix,
            move |_reporter| {
                let cf = cf.clone();
                let options = options.into().unwrap_or_default();
                unsafe {
                    ffi::rocksdb_readoptions_set_prefix_same_as_start(
                        options.inner,
                        true as libc::c_uchar,
                    );
                }
                let handle = self.handle().clone();

                elasyncio::run_blocking_task(move || {
                    let iterator_ptr =
                        unsafe { Self::raw_iterate(&handle, cf.rocks_ptr(), options.rocks_ptr())? };

                    if let Some(iterator_ptr) = ptr::NonNull::new(iterator_ptr) {
                        let mut iter = UnboundedIterator::new(
                            handle::AnonymousHandle::wrap_opaque_type(handle),
                            iterator_ptr,
                            ReadOptions::from_option(options),
                        );

                        // By seeking to this prefix hint start_key, we're locking the iterator into only
                        // working on keys with the same prefix as the hint start_key.
                        iter.from_key(start_key);

                        Ok(iter)
                    } else {
                        Err(Error::other_error("Failed to create iterator"))
                    }
                })
            },
        )
    }
}

// The RocksDB transaction impl has the same API and takes the same options as the DB impl, but it
// doesn't properly implement prefix (https://github.com/facebook/rocksdb/issues/5100) iteration.

/// Unsync (that is not thread safe) transactions support `IterateAll` with an `UnboundedIterator`,
/// and `IterateRange` with `DbRangeIterator`.  Prefix iteration is not
/// possible, because we have no way to invoke the prefix extractor from Rust in order to implement
/// a prefix extract polyfill
impl IterateInternal for unsync::Transaction {
    // Range iterator uses a Rust polyfill implementation
    type InternalRangeIter = DbRangeIterator;

    // Prefix iteration not supported at all
    // TODO: once stabilized this should be the `!` type
    type InternalPrefixIter = ();

    unsafe fn raw_iterate(
        handle: &Self::HandleType,
        cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<*mut ffi::rocksdb_iterator_t> {
        let iterator_ptr = ffi::rocksdb_transaction_create_iterator_cf(
            handle.as_ptr(),
            options.rocks_ptr().as_ptr(),
            cf.rocks_ptr().as_ptr(),
        );

        Ok(iterator_ptr)
    }
}

impl AutoIterateRange for unsync::Transaction {}

/// The default impl of `IterateAll` is sufficient, because it defaults to using `iterate_all` for
/// iterating with a prefix hint
impl IterateAll for unsync::Transaction {}

// The implementation for the thread-safe `Transaction` is just a wrapper around the
// `unsync::Transaction` implementation, so it takes a rather different form.
impl IterateInternal for sync::Transaction {
    type InternalRangeIter = <unsync::Transaction as IterateInternal>::InternalRangeIter;
    type InternalPrefixIter = <unsync::Transaction as IterateInternal>::InternalPrefixIter;

    unsafe fn raw_iterate(
        _handle: &Self::HandleType,
        _cf: NonNull<ffi::rocksdb_column_family_handle_t>,
        _options: NonNull<ffi::rocksdb_readoptions_t>,
    ) -> Result<*mut ffi::rocksdb_iterator_t> {
        // This should never be called, because we redirect the higher-level operations to the
        // wrapped `unsync::Transaction`
        unreachable!()
    }

    fn internal_iterate_all(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
    ) -> Result<UnboundedIterator> {
        self.with_tx(move |tx| tx.internal_iterate_all(cf, options))
    }

    fn internal_async_iterate_all(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
    ) -> op_metrics::AsyncOpFuture<UnboundedIterator>
    where
        <Self as RocksOpBase>::HandleType: Sync + Send,
    {
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncWrapper,
            move |_reporter| {
                let options = options.into();
                let cf = cf.clone();
                self.async_with_tx(move |tx| tx.internal_iterate_all(&cf, options))
            },
        )
    }
}

impl IterateRange for sync::Transaction {
    type RangeIter = <unsync::Transaction as IterateRange>::RangeIter;

    fn iterate_range<K: BinaryStr>(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
        key_range: impl Into<OpenKeyRange<K>>,
    ) -> Result<Self::RangeIter> {
        self.with_tx(move |tx| tx.iterate_range(cf, options, key_range))
    }

    fn async_iterate_range<K: BinaryStr + Send + 'static>(
        &self,
        cf: &impl db::ColumnFamilyLike,
        options: impl Into<Option<ReadOptions>>,
        key_range: impl Into<OpenKeyRange<K>>,
    ) -> op_metrics::AsyncOpFuture<Self::RangeIter>
    where
        <Self as RocksOpBase>::HandleType: Sync,
    {
        op_metrics::instrument_async_cf_op(
            cf,
            op_metrics::ColumnFamilyOperation::AsyncWrapper,
            move |_reporter| {
                let cf = cf.clone();
                let options = options.into();
                let key_range = key_range.into();
                self.async_with_tx(move |tx| tx.iterate_range(&cf, options, key_range))
            },
        )
    }
}

impl IterateAll for sync::Transaction {}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::DbLike;
    use crate::test::TempDbPath;

    #[test]
    fn db_iterate_all() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let _iter = db.iterate_all(&cf, None)?;

        Ok(())
    }

    #[test]
    fn txdb_iterate_all() -> Result<()> {
        let path = TempDbPath::new();
        let db = TransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let _iter = db.iterate_all(&cf, None)?;

        Ok(())
    }

    #[test]
    fn opt_txdb_iterate_all() -> Result<()> {
        let path = TempDbPath::new();
        let db = OptimisticTransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let _iter = db.iterate_all(&cf, None)?;

        Ok(())
    }

    #[test]
    fn tx_iterate_all() -> Result<()> {
        let path = TempDbPath::new();
        let db = TransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let tx = db.begin_trans(None, None)?;

        let _iter = tx.iterate_all(&cf, None)?;

        Ok(())
    }

    #[tokio::test]
    async fn db_async_iterate_all() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let _iter = db.async_iterate_all(&cf, None).await?;

        Ok(())
    }

    #[tokio::test]
    async fn tx_async_iterate_all() -> Result<()> {
        let path = TempDbPath::new();
        let db = TransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();
        let tx = db.begin_trans(None, None)?.into_sync();

        let _iter = tx.async_iterate_all(&cf, None).await?;

        Ok(())
    }
}
