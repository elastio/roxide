//! An iterator in RocksDB is how to scan through the key/value pairs in a column family.
//! Conceptually they're a lot like Rust's `Iterator` trait, but of course much more elaborate
//! under the covers.
//!
//! Implementation of iteration is made more complicated by the fact that we want to present a
//! consistent iterator API to callers, but the actual implementation of iteration is slightly
//! different for iterators on transactions compared to iterators on databases themselves.
//!
//! Specifically, in RocksDB iterators created from transactions will ignore any options specifying
//! either a range bound for the iterator or use of a prefix.  Those are both important
//! capabilities so this layer provides a polyfill that works the same (albeit with less
//! performance).  As a result there's a somewhat complex array of types:
//!
//! ## Iterator Traits
//!
//! * [`RocksIterator`] - The RocksDB iterator API, as projected onto idiomatic Rust.  Note that
//! in spite of the name, this is by no means a Rust `Iterator`.  Trying to treat it like one will
//! only end in tears.
//!
//! ## Iterator implementations
//!
//! * [`RawIterator`] - Low level internal wrapper around the C iteration API.  This is not used
//! outside of this crate, but the other iterator implementations rely on it internally.  This
//! doesn't actually implement the `RocksIterator` trait, unlike all of the rest of the iterator
//! implementations listed here.
//!
//! * [`UnboundedIterator`] - An iterator for iterating over all records in a column family,
//! unbounded.  This works with both transactionless database iterators, and transaction iterators.
//! This is, perhaps confusingly, also used for DB-level prefix iteration, because that is
//! implemented entirely within RocksDB and doesn't need a separate iterator impl.
//!
//! * [`DbRangeIterator`] - An iterator which iterates over a specific range of values, using a
//! transactionless database iterator.  This uses RocksDB's internal implementation to limit the
//! iterator to the initial range, so it's high performance.  This uses `UnboundedIterator`
//! internally, the only difference is that is holds onto the memory containing the range keys for
//! the life of the iterator so Rocks will be able to access that memory for the life of the
//! iterator.
//!
//! * [`TransactionRangeIterator`] - Implements range iteration for a transaction by detecting when
//! the iterator has advanced outside of the range of keys specified at iterator creation time.
//! This isn't nearly as efficient as Rocks doing this internally, but it allows higher-level code
//! to support either transactions or transactionless operations, getting a performance boost if
//! transactions aren't used.

use crate::error::prelude::*;
use crate::ffi;
use crate::ffi_try;
use crate::handle;
use crate::handle::RocksObjectDefault;
use crate::ops::BinaryStr;
use crate::ops::OpenKeyRange;
use crate::rocks_class;
use rocksdb::ReadOptions;
use std::ptr;

rocks_class!(IteratorHandle, ffi::rocksdb_iterator_t, ffi::rocksdb_iter_destroy, @send);

/// Iterators can either more forward or backward through the data
enum Direction {
    Forward,
    Reverse,
}

/// Exposes the RocksDB iterator API in a somewhat Rust-y shape.
///
/// It's important that you understand what operation an iterator came from, as that determines the
/// constraints on the iterator.  For example, an iterator from `iterate_all` has different
/// restrictions than one from `iterate_range`.  See the documentation for [`IterateAll`],
/// [`IterateRange`], and [`IteratePrefix`] for details on these restrictions.  No matter what the
/// source, all iterators have the same basic API.
///
/// [`IterateAll`]: crate::ops::IterateAll
/// [`IterateRange`]: crate::ops::IterateRange
/// [`IteratePrefix`]: crate::ops::IteratePrefix
pub trait RocksIterator: Send + 'static {
    /// Make this iterator operate from the start of the collection, working forwards.
    ///
    /// This is the default behavior of a new iterator, so this is only needed to convert an
    /// existing iterator that was previously created with another `from_X` call.
    #[allow(clippy::wrong_self_convention)]
    fn from_start(&mut self);

    /// Make this iterator operate from the end of the collection, working backwards
    #[allow(clippy::wrong_self_convention)]
    fn from_end(&mut self);

    /// Make this iterator operate starting from a specific key (or if that key doesn't exist, the
    /// first key after it lexicographically).
    ///
    /// # Notes
    ///
    /// If this iterator was created with a prefix, this key must have the same prefix as the key
    /// the iterator was created with.  If this is not the case, the result is undefined behavior.
    /// Similarly if this is a range iterator, the key must be in the range the iterator was
    /// created with of the behavior is undefined.
    #[allow(clippy::wrong_self_convention)]
    fn from_key(&mut self, from: impl BinaryStr);

    /// Make this iterator operate starting from a specific key (or if that key doesn't exist, the
    /// first key before it lexicographically), and moving in reverse.
    ///
    /// # Notes
    ///
    /// See the restrictions that apply to [`RocksIterator::from_key`] also apply to the key provided to this
    /// method.
    #[allow(clippy::wrong_self_convention)]
    fn from_key_reverse(&mut self, from: impl BinaryStr);

    /// Seeks to the specified key or the first key that lexicographically follows it.
    ///
    /// This method will attempt to seek to the specified key. If that key does not exist, it will
    /// find and seek to the key that lexicographically follows it instead.
    ///
    /// # Notes
    ///
    /// See the restrictions that apply to [`RocksIterator::from_key`] also apply to the key provided to this
    /// method.
    fn seek(&mut self, key: impl BinaryStr);

    /// Seeks to the specified key, or the first key that lexicographically precedes it.
    ///
    /// Like ``.seek()`` this method will attempt to seek to the specified key.
    /// The difference with ``.seek()`` is that if the specified key do not exist, this method will
    /// seek to key that lexicographically precedes it instead.
    ///
    /// # Notes
    ///
    /// See the restrictions that apply to [`RocksIterator::from_key`] also apply to the key provided to this
    /// method.
    fn seek_for_prev(&mut self, key: impl BinaryStr);

    /// Indicates if this iterator is valid.  If it's not valid you cannot read from it.
    fn valid(&self) -> bool;

    /// If `valid` is false, call this method to get the status of the iterator.  `valid` being
    /// false could just mean the iterator is reached the end, or it could mean an error.
    fn status(&self) -> Result<()>;

    /// Gets the current (key, value) pair in this iterator, if there is a valid value.
    ///
    /// If `valid()` is `true`, that means `get_current` will return a non-`None` pair.  Otherwise
    /// it will return either `None`, or an error if there's an underlying RocksDB iterator API
    /// error.
    ///
    /// # Safety
    ///
    /// The slices returned by this function are direct pointers into RocksDB memory allocated for
    /// this iterator.  The slices will be invalid after the next call to `advance`.  That's why
    /// this is `unsafe`.  You must immediately convert these slices into Rust-managed memory.
    unsafe fn get_current(&self) -> Result<Option<(&[u8], &[u8])>>;

    /// Advances the iterator to the next logical position, if any.
    ///
    /// "Logical" here means that "advance" isn't always "the next highest key in lexicographical
    /// order", because the `direction` of this iterator is honored.  So if this is a reverse
    /// iterator, `advance` may in fact call `prev` on the underlying Rocks iterator to obtain the
    /// next value.
    fn advance(&mut self);
}

/// Implements the iteration over an unbounded range of keys.  Also provides the base implemntation
/// for iterators bounded by range or prefix.
pub struct UnboundedIterator {
    raw_iter: RawIterator,
    direction: Direction,
}

impl UnboundedIterator {
    pub(crate) fn new(
        parent: handle::AnonymousHandle,
        iterator: impl Into<IteratorHandle>,
        options: handle::CowHandle<ffi::rocksdb_readoptions_t, ReadOptions>,
    ) -> Self {
        let mut raw_iter = RawIterator::new(parent, iterator, options);
        raw_iter.seek_to_first();
        Self {
            raw_iter,
            direction: Direction::Forward,
        }
    }
}

impl RocksIterator for UnboundedIterator {
    fn from_start(&mut self) {
        self.raw_iter.seek_to_first();
        self.direction = Direction::Forward;
    }

    fn from_end(&mut self) {
        self.raw_iter.seek_to_last();
        self.direction = Direction::Reverse;
    }

    fn from_key(&mut self, from: impl BinaryStr) {
        self.raw_iter.seek(from);
        self.direction = Direction::Forward;
    }

    fn from_key_reverse(&mut self, from: impl BinaryStr) {
        self.seek_for_prev(from);
        self.direction = Direction::Reverse;
    }

    fn seek(&mut self, key: impl BinaryStr) {
        self.raw_iter.seek(key);
    }

    fn seek_for_prev(&mut self, key: impl BinaryStr) {
        self.raw_iter.seek_for_prev(key);
    }

    fn valid(&self) -> bool {
        self.raw_iter.valid()
    }

    fn status(&self) -> Result<()> {
        self.raw_iter.status()
    }

    unsafe fn get_current(&self) -> Result<Option<(&[u8], &[u8])>> {
        // Make sure the iterator isn't in an error state.  If it is fail.
        self.raw_iter.status()?;

        // Iterator is healthy, but does it have a value fo rus?
        if self.raw_iter.valid() {
            // There is a valid next item
            //
            // Convert from C pointer/length to a slice
            let (key, value) = (
                self.raw_iter
                    .key_inner()
                    .map(|(ptr, len)| std::slice::from_raw_parts(ptr, len)),
                self.raw_iter
                    .value_inner()
                    .map(|(ptr, len)| std::slice::from_raw_parts(ptr, len)),
            );

            // Since `valid` returned true, both the key and value should be valid
            // If they're not it's a bug and we want the ensuing panic
            Ok(Some((key.unwrap(), value.unwrap())))
        } else {
            // Iterator is at EOF
            Ok(None)
        }
    }

    fn advance(&mut self) {
        // Advance the iterator to the next item
        match self.direction {
            Direction::Forward => self.raw_iter.next(),
            Direction::Reverse => self.raw_iter.prev(),
        };
    }
}

/// An iterator over a range of keys in a RocksDB column family, outside of any transaction.
///
/// This uses `UnboundedIterator` internally, because Rocks implements the range limits itself.
/// The only reason there's a separate type for DB range iteration is the requirement to keep the
/// range bounds pinned in memory and accessible to Rocks for the life of the iterator.
pub struct DbRangeIterator {
    inner: UnboundedIterator,

    /// We have to hold on to this memory where the upper and lower key bounds are stored, until
    /// Rocks is done with it (that is, the FFI iterator object stored in `inner` is freed).
    range: Option<OpenKeyRange<(*mut u8, usize)>>,
}

impl DbRangeIterator {
    pub(crate) fn new(
        parent: handle::AnonymousHandle,
        iterator: impl Into<IteratorHandle>,
        options: ReadOptions,
        range: OpenKeyRange<(*mut u8, usize)>,
    ) -> Self {
        Self {
            inner: UnboundedIterator::new(parent, iterator, ReadOptions::from_option(options)),
            range: Some(range),
        }
    }
}

impl RocksIterator for DbRangeIterator {
    fn from_start(&mut self) {
        self.inner.from_start();
    }

    fn from_end(&mut self) {
        self.inner.from_end();
    }

    fn from_key(&mut self, from: impl BinaryStr) {
        self.inner.from_key(from);
    }

    fn from_key_reverse(&mut self, from: impl BinaryStr) {
        self.inner.from_key_reverse(from);
    }

    fn seek(&mut self, key: impl BinaryStr) {
        self.inner.seek(key);
    }

    fn seek_for_prev(&mut self, key: impl BinaryStr) {
        self.inner.seek_for_prev(key);
    }

    fn valid(&self) -> bool {
        self.inner.valid()
    }

    fn status(&self) -> Result<()> {
        self.inner.status()
    }

    unsafe fn get_current(&self) -> Result<Option<(&[u8], &[u8])>> {
        self.inner.get_current()
    }

    fn advance(&mut self) {
        self.inner.advance()
    }
}

impl Drop for DbRangeIterator {
    fn drop(&mut self) {
        // Everything else will free itself properly but the range keys need to be turned back into
        // Rust `Vec`s and freed
        if let Some(range) = self.range.take() {
            unsafe { range.free() }
        }
    }
}

/// An iterator over a range of keys in a RocksDB column family within a transaction
///
/// Since Rocks doesn't support range iteration on a transaction natively
/// (<https://github.com/facebook/rocksdb/issues/2343>), it must be implemented manually.  That's not
/// nearly as performant as doing it at the rocks level; hopefully someday they get around to
/// implementing this properly.
pub struct TransactionRangeIterator {
    inner: UnboundedIterator,
    range: OpenKeyRange<Vec<u8>>,
}

impl TransactionRangeIterator {
    pub(crate) fn new(inner: UnboundedIterator, range: OpenKeyRange<Vec<u8>>) -> Self {
        let mut iter = Self { inner, range };

        iter.from_start();

        iter
    }
}

impl RocksIterator for TransactionRangeIterator {
    fn from_start(&mut self) {
        match self.range.start() {
            Some(start) => self.inner.from_key(start),
            None => self.inner.from_start(),
        };
    }

    fn from_end(&mut self) {
        match self.range.end() {
            Some(end) => {
                // `from_key_reverse` will put the iterator on `end` if that key exists.
                // That's not what we want because our `get_current` will see that `end` it outside
                // of the range and return `None` instead of the record.
                //
                // Detect that case and iterate back one.
                self.inner.from_key_reverse(end);

                unsafe {
                    match self.inner.get_current() {
                        Ok(Some((e, _))) if e == end => {
                            self.inner.advance();
                        }
                        _ => (),
                    };
                }
            }
            None => self.inner.from_end(),
        };
    }

    fn from_key(&mut self, from: impl BinaryStr) {
        assert!(
            self.range.is_in_range(&from),
            "calling `from_key` with a key outside the iteration range is undefined"
        );
        self.inner.from_key(from);
    }

    fn from_key_reverse(&mut self, from: impl BinaryStr) {
        assert!(
            self.range.is_in_range(&from),
            "calling `from_key_reverse` with a key outside the iteration range is undefined"
        );
        self.inner.from_key_reverse(from);
    }

    fn seek(&mut self, key: impl BinaryStr) {
        assert!(
            self.range.is_in_range(&key),
            "calling `seek` with a key outside the iteration range is undefined"
        );
        self.inner.seek(key);
    }

    fn seek_for_prev(&mut self, key: impl BinaryStr) {
        assert!(
            self.range.is_in_range(&key),
            "calling `seek_for_prev` with a key outside the iteration range is undefined"
        );
        self.inner.seek_for_prev(key);
    }

    fn valid(&self) -> bool {
        // In addition to the internal test for validity, also report the iterator invalid if it's
        // moved outside of the specified range.
        self.inner.valid() && unsafe { self.range.is_in_range(&self.inner.raw_iter.key().unwrap()) }
    }

    fn status(&self) -> Result<()> {
        self.inner.status()
    }

    unsafe fn get_current(&self) -> Result<Option<(&[u8], &[u8])>> {
        let current = self.inner.get_current();
        let range = &self.range;

        current.map(|opt_current| {
            opt_current.and_then(|(key, value)| {
                if range.is_in_range(&key) {
                    Some((key, value))
                } else {
                    None
                }
            })
        })
    }

    fn advance(&mut self) {
        self.inner.advance()
    }
}

/// Internal wrapper around the Rocks iterator C FFI
struct RawIterator {
    inner: IteratorHandle,

    /// The owned copy of `ReadOptions`, a pointer to which RocksDB internally is using for this
    /// iterator operation.
    _options: handle::CowHandle<ffi::rocksdb_readoptions_t, ReadOptions>,

    /// The handle to the database or transaction this iterator is opened on.  Since struct members are dropped
    /// in order of declaration, this ensures the iterator handle `inner` is dropped first, so
    /// that even if the last remaining handle to the database is this one, the transaction still
    /// gets dropped first
    _parent: handle::AnonymousHandle,
}

impl RawIterator {
    pub(crate) fn new(
        parent: handle::AnonymousHandle,
        iterator: impl Into<IteratorHandle>,
        options: handle::CowHandle<ffi::rocksdb_readoptions_t, ReadOptions>,
    ) -> Self {
        RawIterator {
            inner: iterator.into(),
            _options: options,
            _parent: parent,
        }
    }

    /// Indicates if this iterator is valid.  If it's not valid you cannot read from it.
    pub fn valid(&self) -> bool {
        unsafe { ffi::rocksdb_iter_valid(self.inner.as_ptr()) != 0 }
    }

    /// If `valid` is false, call this method to get the status of the iterator.  `valid` being
    /// false could just mean the iterator is reached the end, or it could mean an error.
    pub fn status(&self) -> Result<()> {
        unsafe { ffi_try!(ffi::rocksdb_iter_get_error(self.inner.as_ptr(),)) }
    }

    /// Seeks to the first key in the column family.
    pub fn seek_to_first(&mut self) {
        unsafe {
            ffi::rocksdb_iter_seek_to_first(self.inner.as_ptr());
        }
    }

    /// Seeks to the last key in the column family.
    pub fn seek_to_last(&mut self) {
        unsafe {
            ffi::rocksdb_iter_seek_to_last(self.inner.as_ptr());
        }
    }

    /// Seeks to the specified key or the first key that lexicographically follows it.
    ///
    /// This method will attempt to seek to the specified key. If that key does not exist, it will
    /// find and seek to the key that lexicographically follows it instead.
    pub fn seek<K: BinaryStr>(&mut self, key: K) {
        unsafe {
            ffi::rocksdb_iter_seek(
                self.inner.as_ptr(),
                key.as_ptr() as *const libc::c_char,
                key.len() as libc::size_t,
            );
        }
    }

    /// Seeks to the specified key, or the first key that lexicographically precedes it.
    ///
    /// Like ``.seek()`` this method will attempt to seek to the specified key.
    /// The difference with ``.seek()`` is that if the specified key do not exist, this method will
    /// seek to key that lexicographically precedes it instead.
    pub fn seek_for_prev<K: BinaryStr>(&mut self, key: K) {
        unsafe {
            ffi::rocksdb_iter_seek_for_prev(
                self.inner.as_ptr(),
                key.as_ptr() as *const libc::c_char,
                key.len() as libc::size_t,
            );
        }
    }

    /// Seeks to the next key.  You must test `valid` to see if the iterator is still valid after
    /// seeking.
    pub fn next(&mut self) {
        unsafe {
            ffi::rocksdb_iter_next(self.inner.as_ptr());
        }
    }

    /// Seeks to the previous key.  You must test `valid` to see if the iterator is still valid after
    /// seeking.
    pub fn prev(&mut self) {
        unsafe {
            ffi::rocksdb_iter_prev(self.inner.as_ptr());
        }
    }

    pub unsafe fn key(&self) -> Option<&[u8]> {
        self.key_inner()
            .map(|(ptr, len)| std::slice::from_raw_parts(ptr, len))
    }

    /// Returns a slice to the internal buffer storing the current key.
    ///
    /// # Safety
    ///
    /// The pointer to the buffer returned by this method is valid only while the iterator is on
    /// its current position.  If you move it with a `seek_` or `next` or `previous`, this pointer
    /// will not be valid, and any attempt to access it will be undefined.
    pub unsafe fn key_inner(&self) -> Option<(*const u8, usize)> {
        if self.valid() {
            let mut key_len: libc::size_t = 0;
            let key_len_ptr: *mut libc::size_t = &mut key_len;
            let key_ptr =
                ffi::rocksdb_iter_key(self.inner.as_ptr(), key_len_ptr) as *const libc::c_uchar;

            Some((key_ptr, key_len as usize))
        } else {
            None
        }
    }

    /// Returns a slice to the internal buffer storing the current value.
    ///
    /// # Safety
    ///
    /// The pointer to the buffer returned by this method is valid only while the iterator is on
    /// its current position.  If you move it with a `seek_` or `next` or `previous`, this pointer
    /// will not be valid, and any attempt to access it will be undefined.
    pub unsafe fn value_inner(&self) -> Option<(*const u8, usize)> {
        if self.valid() {
            let mut val_len: libc::size_t = 0;
            let val_len_ptr: *mut libc::size_t = &mut val_len;
            let val_ptr =
                ffi::rocksdb_iter_value(self.inner.as_ptr(), val_len_ptr) as *const libc::c_uchar;

            Some((val_ptr, val_len as usize))
        } else {
            None
        }
    }
}

impl handle::RocksObject<ffi::rocksdb_iterator_t> for RawIterator {
    fn rocks_ptr(&self) -> ptr::NonNull<ffi::rocksdb_iterator_t> {
        self.inner.rocks_ptr()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::{Db, DbLike, TransactionDb};
    use crate::ops::{BeginTrans, DbOpen, IterateAll, IteratePrefix, IterateRange, Put};
    use crate::test::TempDbPath;
    use crate::DbOptions;
    use crate::Result;
    use maplit::hashmap;
    use rand::{prelude::*, seq::SliceRandom};

    fn u64_to_bytes(val: u64) -> Vec<u8> {
        Vec::from(val.to_be_bytes().as_ref())
    }

    fn bytes_to_u64(bytes: &[u8]) -> u64 {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(bytes);

        u64::from_be_bytes(arr)
    }

    fn into_vec<'a, I: IntoIterator<Item = &'a (K, V)>, K, V>(iter: I) -> Vec<(Vec<u8>, Vec<u8>)>
    where
        K: AsRef<[u8]> + 'a,
        V: AsRef<[u8]> + 'a,
    {
        iter.into_iter()
            .map(|(key, value)| (Vec::from(key.as_ref()), Vec::from(value.as_ref())))
            .collect()
    }

    fn assert_eq_pairs<K1, K2, V1, V2>(i1: &[(K1, V1)], i2: &[(K2, V2)])
    where
        K1: AsRef<[u8]>,
        V1: AsRef<[u8]>,
        K2: AsRef<[u8]>,
        V2: AsRef<[u8]>,
    {
        let v1 = into_vec(i1);
        let v2 = into_vec(i2);

        assert_eq!(v1, v2);
    }

    // When testing assumptions about the output of iterators, it's really inconvenient to deal
    // with the raw iterator API, sometimes it's better to convert into a vector
    fn iter_to_vec<I: RocksIterator>(mut iter: I) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut pairs = Vec::new();

        while let Some((key, value)) = unsafe { iter.get_current()? } {
            pairs.push((Vec::from(key), Vec::from(value)));

            iter.advance();
        }

        Ok(pairs)
    }

    /// Exercise iterating when the key is a u64 whose expected order we know in advance
    #[test]
    fn db_iterate_u64() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        // Fill the database with some values and expect the iterator to produce them in sorted
        // order
        let mut expected_pairs = Vec::new();
        for i in 2..100u64 {
            // Skip from 10 to 20
            if !(10..=20).contains(&i) {
                expected_pairs.push((u64_to_bytes(i), format!("foo{}", i).into_bytes()));
            }
        }

        // Insert the data in a random order; the iterator will ensure it's sorted
        let mut rng = thread_rng();
        let mut pairs = expected_pairs.clone();
        pairs.shuffle(&mut rng);

        for (key, value) in pairs {
            db.put(&cf, key, value, None)?;
        }

        let iter = db.iterate_all(&cf, None)?;
        let iterated_pairs = iter_to_vec(iter)?;
        assert_eq_pairs(&expected_pairs, &iterated_pairs);

        // Now do it again but with a range
        // Iterate from 0 to 15.  We actually have data from 2 to 9
        let iter = db.iterate_range(&cf, None, (u64_to_bytes(0), u64_to_bytes(15)))?;
        let iterated_pairs = iter_to_vec(iter)?;

        let iterated_keys: Vec<_> = iterated_pairs
            .into_iter()
            .map(|(key, _value)| key)
            .collect();
        let expected_keys: Vec<_> = (2..10u64).map(u64_to_bytes).collect();
        assert_eq!(expected_keys, iterated_keys);

        Ok(())
    }

    /// Exercise all and range iteration in the database using binary strings
    #[test]
    fn db_iterate_binarystr() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        // Fill the database with some values and expect the iterator to produce them in sorted
        // order
        let expected_pairs: Vec<(&[u8], &[u8])> = vec![
            (b"baz1", b"boo1"),
            (b"baz2", b"boo2"),
            (b"baz3", b"boo3"),
            (b"foo1", b"bar1"),
            (b"foo2", b"bar2"),
            (b"foo3", b"bar3"),
            (b"zip1", b"zub1"),
        ];

        // Insert the data in a random order; the iterator will ensure it's sorted
        let mut pairs = expected_pairs.clone();
        let mut rng = thread_rng();
        pairs.shuffle(&mut rng);

        for (key, value) in pairs {
            db.put(&cf, key, value, None)?;
        }

        let iter = db.iterate_all(&cf, None)?;
        let iterated_pairs = iter_to_vec(iter)?;
        assert_eq_pairs(&expected_pairs, &iterated_pairs);

        // Now do it again but with a range
        let iter = db.iterate_range(&cf, None, (&b"foo0"[..], &b"foo4"[..]))?;
        let iterated_pairs = iter_to_vec(iter)?;

        assert_eq_pairs(
            &[(b"foo1", b"bar1"), (b"foo2", b"bar2"), (b"foo3", b"bar3")],
            &iterated_pairs,
        );

        // Now again but starting from a specific key within the range
        let mut iter = db.iterate_range(&cf, None, (&b"foo0"[..], &b"foo4"[..]))?;
        iter.from_key(b"foo2");
        let iterated_pairs = iter_to_vec(iter)?;

        assert_eq_pairs(&[(b"foo2", b"bar2"), (b"foo3", b"bar3")], &iterated_pairs);

        // Now again but in reverse order
        let mut iter = db.iterate_range(&cf, None, (&b"foo0"[..], &b"foo5"[..]))?;
        iter.from_key_reverse(b"foo5");
        let iterated_pairs = iter_to_vec(iter)?;

        // as above there is no `foo5`, so the first key before `foo5` in lexicographical order
        // shoudl be found

        assert_eq_pairs(
            &[(b"foo3", b"bar3"), (b"foo2", b"bar2"), (b"foo1", b"bar1")],
            &iterated_pairs,
        );

        Ok(())
    }

    /// Test the behavior of prefix iterators on the database.
    #[test]
    fn db_iterate_prefix() -> Result<()> {
        // When a column family is configured with a prefix extractor, iterator seeks are assumed
        // to be seeks to the first of all keys that have a prefix equal to the seek value.
        let path = TempDbPath::new();
        let mut options = DbOptions::default();
        options.add_column_family("standard");
        // Configure the `prefixed` CF with 3 byte fixed prefix extractor, so the first three bytes
        // of all keys will be treated as a prefix, grouping all keys with that same prefix
        // together during iteration
        options.add_column_family_opts("prefixed", &hashmap!["prefix_extractor" => "fixed:3" ]);

        let db = Db::open(&path, options)?;
        let std_cf = db.get_cf("standard").unwrap();
        let prefixed_cf = db.get_cf("prefixed").unwrap();

        // Fill both CFs with the same data
        let mut keys = Vec::new();

        for i in 0..10 {
            for j in 0..10 {
                keys.push(format!("foo{}{}", i, j));
                keys.push(format!("bar{}{}", i, j));
            }
        }

        for key in &keys {
            db.put(&std_cf, key, key, None)?;
            db.put(&prefixed_cf, key, key, None)?;
        }

        // Both CFs will start to seek somewhere inside `bar`.  The standard CF has no concept of prefix and
        // will keep scanning through the entire data set.
        //
        // `prefixed_cf` should stop at `bar99`.
        let mut prefixed_keys = Vec::new();
        let mut std_keys = Vec::new();

        let mut iter = db.iterate_all(&std_cf, None)?;
        iter.from_key("bar90");
        for (key, _value) in iter_to_vec(iter)? {
            std_keys.push(key)
        }

        let mut iter = db.iterate_prefix(&prefixed_cf, None, "bar90")?;
        iter.from_key("bar90");
        for (key, _value) in iter_to_vec(iter)? {
            prefixed_keys.push(key)
        }

        // prefix aware iterator knows to stop after the last `bar` key
        assert_eq!(b"bar90", prefixed_keys[0].as_slice());
        assert_eq!(b"bar99", prefixed_keys[9].as_slice());
        assert_eq!(10, prefixed_keys.len());

        // std doesn't have any concept of a prefix, so it won't stop until it iterates through the
        // entire dataset
        assert_eq!(b"bar90", std_keys[0].as_slice());
        assert_eq!(b"bar99", std_keys[9].as_slice());
        assert_eq!(b"foo00", std_keys[10].as_slice());
        assert_eq!(b"foo99", std_keys[109].as_slice());
        assert_eq!(110, std_keys.len());

        Ok(())
    }

    /// Iterating over a range within a prefix: when you really like to live dangerously
    #[test]
    fn db_iterate_prefix_range() -> Result<()> {
        let path = TempDbPath::new();
        let mut options = DbOptions::default();
        // Configure the `prefixed` CF with 3 byte fixed prefix extractor, so the first three bytes
        // of all keys will be treated as a prefix, grouping all keys with that same prefix
        // together during iteration
        options.add_column_family_opts("prefixed", &hashmap!["prefix_extractor" => "fixed:3" ]);

        let db = Db::open(&path, options)?;
        let prefixed_cf = db.get_cf("prefixed").unwrap();

        // Fill both CFs with the same data
        let mut keys = Vec::new();

        for i in 0..10 {
            for j in 0..10 {
                keys.push(format!("foo{}{}", i, j));
                keys.push(format!("bar{}{}", i, j));
            }
        }

        for key in &keys {
            db.put(&prefixed_cf, key, key, None)?;
        }

        // Using a combination of range and prefix, we can achieve potentially better performance
        // when we know we want a range that makes up a small subset of the entire contents of a
        // prefix
        //
        // Of course this same result could be obtained with a regualr `iterate_range`, but the
        // RocksDB docs suggest that prefix iteration is more efficient than regular range
        // iteration because it can make use of prefix bloom filters.
        let iter = db.iterate_prefix_range(&prefixed_cf, None, ("foo11", "foo88"))?;
        let prefixed_keys = iter_to_vec(iter)?
            .into_iter()
            .map(|(key, _value)| key)
            .collect::<Vec<_>>();

        assert_eq!(b"foo11", prefixed_keys.first().unwrap().as_slice());
        assert_eq!(b"foo87", prefixed_keys.last().unwrap().as_slice());

        Ok(())
    }

    /// Make two iterators, one a DB range iterator using the native RocksDB range iteration code,
    /// and one on a transaction for the same range.  Drive both of the iterators with the same
    /// commands and verify the result is the same.
    #[test]
    fn transaction_range_iterator_conformance() -> Result<()> {
        let path = TempDbPath::new();
        let db = TransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        // Use numbers to make reasoning about the range easier.
        // numbers 2 through 99, with 10-20 (inclusive) missing
        for i in 2..100u64 {
            // Skip from 10 to 20
            if !(10..=20).contains(&i) {
                let key = u64_to_bytes(i);
                let value = format!("foo{}", i).into_bytes();

                db.put(&cf, key, value, None)?;
            }
        }

        // test that various ranges produce idential results with the database and tx iterators
        for (start, end) in &[
            (None, None),               // fully open range
            (Some(10u64), None),        // start at a non-existent point, no end point
            (Some(5u64), None),         // start at an existing point, no end
            (None, Some(50u64)),        // start at beginning, end at an existing point
            (None, Some(500u64)),       // start at beginning, end ata  non-existing point
            (Some(10u64), Some(50u64)), // range between a non-existing and existing point
            (Some(5u64), Some(50u64)),  // range between two existing points
            (Some(0u64), Some(500u64)), // range between two points that are both outside the range of values (so return everything)
        ] {
            let range =
                OpenKeyRange::<Vec<u8>>::new(start.map(u64_to_bytes), end.map(u64_to_bytes));

            println!("Testing conformance with range {:?}", range);
            let tx = db.begin_trans(None, None)?;
            let db_iter = db.iterate_range(&cf, None, range.clone())?;
            let tx_iter = tx.iterate_range(&cf, None, range.clone())?;

            // first just convert the entire contents of the iterator into vecs; that should match
            assert_eq!(iter_to_vec(db_iter)?, iter_to_vec(tx_iter)?);

            // try again but this time with the iterators going in the reverse order
            let mut db_iter = db.iterate_range(&cf, None, range.clone())?;
            db_iter.from_end();
            let mut tx_iter = tx.iterate_range(&cf, None, range.clone())?;
            tx_iter.from_end();
            assert_eq!(iter_to_vec(db_iter)?, iter_to_vec(tx_iter)?);

            // try again but this time seeking from a key (or if that key doesn't exist the first
            // key after it that does).  In this case 15 doesn't exist, so the series should start
            // at 21
            let mut db_iter = db.iterate_range(&cf, None, range.clone())?;
            db_iter.from_key(u64_to_bytes(15));
            let mut tx_iter = tx.iterate_range(&cf, None, range.clone())?;
            tx_iter.from_key(u64_to_bytes(15));
            assert_eq!(iter_to_vec(db_iter)?, iter_to_vec(tx_iter)?);

            // Same thing but this time seeking in reverse
            let mut db_iter = db.iterate_range(&cf, None, range.clone())?;
            db_iter.from_key_reverse(u64_to_bytes(15));
            let mut tx_iter = tx.iterate_range(&cf, None, range.clone())?;
            tx_iter.from_key_reverse(u64_to_bytes(15));
            assert_eq!(iter_to_vec(db_iter)?, iter_to_vec(tx_iter)?);

            // Seek to some key that falls within the range but does not actually exist
            let mut db_iter = db.iterate_range(&cf, None, range.clone())?;
            db_iter.seek(u64_to_bytes(15));
            let mut tx_iter = tx.iterate_range(&cf, None, range.clone())?;
            tx_iter.seek(u64_to_bytes(15));
            assert_eq!(iter_to_vec(db_iter)?, iter_to_vec(tx_iter)?);

            // Same but in reverse
            let mut db_iter = db.iterate_range(&cf, None, range.clone())?;
            db_iter.seek_for_prev(u64_to_bytes(15));
            let mut tx_iter = tx.iterate_range(&cf, None, range.clone())?;
            tx_iter.seek_for_prev(u64_to_bytes(15));
            assert_eq!(iter_to_vec(db_iter)?, iter_to_vec(tx_iter)?);
        }

        Ok(())
    }

    /// Verifies that, absent our polyfill, the Rocks iterator on a transaction ignores range
    /// parameters when creating an iterator.
    ///
    /// Hopefully some day https://github.com/facebook/rocksdb/issues/2343 is fixed and this test
    /// breaks.  If that happens, rejoice, delete the `TransactionRangeIterator`, and simplify the
    /// iterator code immensely!
    #[test]
    fn rocks_tx_iterator_doesnt_support_range() -> Result<()> {
        let path = TempDbPath::new();
        let db = TransactionDb::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let tx = db.begin_trans(None, None)?;

        let expected_pairs: Vec<(&[u8], &[u8])> = vec![
            (b"baz1", b"boo1"),
            (b"baz2", b"boo2"),
            (b"baz3", b"boo3"),
            (b"foo1", b"bar1"),
            (b"foo2", b"bar2"),
            (b"foo3", b"bar3"),
            (b"zip1", b"zub1"),
        ];

        // Insert the data within the context of a transaction
        for (key, value) in &expected_pairs {
            tx.put(&cf, key, value, None)?;
        }

        // The `IterateRange` trait implementation for transactions will use a Rust-based polyfill,
        // but we can force the use of Rocks native range iteration by putting the range in the
        // ReadOptions passed to iterate_all.  Caution kids, don't try this at home.
        let range: OpenKeyRange<_> = (b"baz2", b"baz5").into();
        let range = range.to_owned().into_raw_parts();
        let mut options = ReadOptions::default();
        range.set_read_options_bounds(&mut options);

        // yes `range` will leak if either of these lines fail; who cares it's a test?
        let iter = tx.iterate_all(&cf, options)?;
        let iterated_pairs = iter_to_vec(iter)?;
        let range = unsafe { OpenKeyRange::from_raw_parts(range) };

        // Even though we specified a range of keys in the read options, we get the entire CF
        assert_eq_pairs(&expected_pairs, &iterated_pairs);

        // Yet if we use the `IterateRange` trait we'll get a polyfill that actually does contrain
        // the iterator correctly
        let iter = tx.iterate_range(&cf, None, range.clone())?;
        assert!(iter.valid());
        let iterated_pairs = iter_to_vec(iter)?;
        assert_eq_pairs(&[(b"baz2", b"boo2"), (b"baz3", b"boo3")], &iterated_pairs);

        // And commiting the transaction doing a range iterate with the database, we get the same
        // result.
        tx.commit()?;

        let iter = db.iterate_range(&cf, None, range)?;
        let iterated_pairs = iter_to_vec(iter)?;
        assert_eq_pairs(&[(b"baz2", b"boo2"), (b"baz3", b"boo3")], &iterated_pairs);

        Ok(())
    }

    /// Verifies that RocksDB transaction iterators ignore prefixes even when told explicitly to
    /// use them.  We have no polyfill for this, it's simply not supported.
    ///
    /// Hopefully some day https://github.com/facebook/rocksdb/issues/5100 is fixed and this test
    /// breaks.  If that happens, rejoice, get rid of `iterate_with_prefix_hint`, and simplfy the
    /// code
    #[test]
    fn rocks_tx_iterator_doesnt_support_prefix() -> Result<()> {
        let path = TempDbPath::new();
        let mut options = DbOptions::default();
        options.add_column_family_opts("prefixed", &hashmap!["prefix_extractor" => "fixed:3" ]);
        let db = TransactionDb::open(&path, options)?;
        let cf = db.get_cf("prefixed").unwrap();

        let tx = db.begin_trans(None, None)?;

        let expected_pairs: Vec<(&[u8], &[u8])> = vec![
            (b"baz1", b"boo1"),
            (b"baz2", b"boo2"),
            (b"baz3", b"boo3"),
            (b"foo1", b"bar1"),
            (b"foo2", b"bar2"),
            (b"foo3", b"bar3"),
            (b"zip1", b"zub1"),
        ];

        // Insert the data within the context of a transaction
        for (key, value) in &expected_pairs {
            tx.put(&cf, key, value, None)?;
        }

        // We don't implement `IteratePrefix` on transactions because we know it's not supported.
        // So to test this we'll reach into the Rocks FFI to set that field on the read options
        let options = ReadOptions::default();
        unsafe {
            crate::ffi::rocksdb_readoptions_set_prefix_same_as_start(
                options.inner,
                true as libc::c_uchar,
            );
        }

        let mut iter = tx.iterate_all(&cf, options)?;
        // first call to 'seek` will set the prefix that the iterator operates in
        //
        // what we would expect if this worked was the iterator would return everything with a
        // `baz` prefix starting at `baz0` (which would be every `baz` record in this case)
        iter.seek(b"baz0");
        let iterated_pairs = iter_to_vec(iter)?;
        assert_eq_pairs(&expected_pairs, &iterated_pairs);

        tx.commit()?;

        // But do this with the database-level iterate_prefix and we get the expected result:
        let iter = db.iterate_prefix(&cf, None, b"baz0")?;
        let iterated_pairs = iter_to_vec(iter)?;
        assert_eq_pairs(
            &[(b"baz1", b"boo1"), (b"baz2", b"boo2"), (b"baz3", b"boo3")],
            &iterated_pairs,
        );

        Ok(())
    }

    /// The behavior of iterators produced by `iterate_with_prefix_hint` and `iterate_prefix`
    /// differ in suble ways.
    #[test]
    fn iterate_with_prefix_hint_vs_iterate_prefix() -> Result<()> {
        // All database and transaction types support `iterate_with_prefix_hint`.  But only the
        // database types actually use the prefix hit to accelerate iterator lookups.
        let path = TempDbPath::new();
        let mut options = DbOptions::default();
        options.add_column_family_opts("prefixed", &hashmap!["prefix_extractor" => "fixed:3" ]);
        let db = TransactionDb::open(&path, options)?;
        let cf = db.get_cf("prefixed").unwrap();

        let expected_pairs: Vec<(&[u8], &[u8])> = vec![
            (b"baz1", b"boo1"),
            (b"baz2", b"boo2"),
            (b"baz3", b"boo3"),
            (b"foo1", b"bar1"),
            (b"foo2", b"bar2"),
            (b"foo3", b"bar3"),
            (b"zip1", b"zub1"),
        ];

        // Insert the data within the context of a transaction
        for (key, value) in &expected_pairs {
            db.put(&cf, key, value, None)?;
        }

        // Using `iterate_with_prefix_hint` on the database actually uses the prefix to control
        // Rocks behavior, so the output of the iterator is limited to that prefix
        let mut iter = db.iterate_with_prefix_hint(&cf, None, b"baz0")?;

        // whenever an iterator might be a prefix iterator, always position it explicitly, don't
        // just assume it's already in the right place
        iter.from_key(b"baz0");
        let iterated_pairs = iter_to_vec(iter)?;
        assert_eq_pairs(
            &[(b"baz1", b"boo1"), (b"baz2", b"boo2"), (b"baz3", b"boo3")],
            &iterated_pairs,
        );

        // Using `iterate_with_prefix_hint` on a transaction doesn't actually apply a prefix
        // constraint to the iterator because transactions don't support that currently
        let tx = db.begin_trans(None, None)?;
        let mut iter = tx.iterate_with_prefix_hint(&cf, None, b"baz0")?;
        iter.from_key(b"baz0");
        let iterated_pairs = iter_to_vec(iter)?;

        // This will match everything, starting at `baz2` through to the end of the column family
        // data
        assert_eq_pairs(&expected_pairs, &iterated_pairs);

        // With a real prefix iterator, if you seek beyond the last key (but still to a key with
        // the same prefix), the iterator produces nothing further.  When using a transaction's
        // `iterate_with_prefix_hint` you get all records after that key you seek to, whether they
        // share a prefix or not.
        let mut iter = db.iterate_with_prefix_hint(&cf, None, b"foo8")?;
        iter.from_key(b"foo8");
        let iterated_pairs = iter_to_vec(iter)?;
        // Empty, because there is no `foo` key at `foo8` or higher
        assert!(iterated_pairs.is_empty());

        let tx = db.begin_trans(None, None)?;
        let mut iter = tx.iterate_with_prefix_hint(&cf, None, b"foo8")?;
        iter.from_key(b"foo8");
        let iterated_pairs = iter_to_vec(iter)?;

        // There's no `foo8`, but this iterator will get everything after that in the column
        // family, regardless of prefix
        assert_eq_pairs(&[(b"zip1", b"zub1")], &iterated_pairs);

        Ok(())
    }

    //
    // #[test]
    // fn tx_seek_prefix_test() -> Result<()> {
    //    // When a column family is configured with a prefix extractor, iterator seeks are assumed
    //    // to be seeks to the first of all keys that have a prefix equal to the seek value.
    //    let path = TempDBPath::new();
    //    let mut options = DBOptions::default();
    //    options.add_column_family("standard");
    //    // Configure the `prefixed` CF with 3 byte fixed prefix extractor, so the first three bytes
    //    // of all keys will be treated as a prefix, grouping all keys with that same prefix
    //    // together during iteration
    //    options.add_column_family_opts("prefixed", &hashmap!["prefix_extractor" => "fixed:3" ]);
    //
    //    let db = TransactionDB::open(&path, options)?;
    //    let std_cf = db.get_cf("standard").unwrap();
    //    let prefixed_cf = db.get_cf("prefixed").unwrap();
    //    let tx = db.begin_trans(None, None)?;
    //
    //    // Fill both CFs with the same data
    //    let mut keys = Vec::new();
    //
    //    for i in 0..10 {
    //        for j in 0..10 {
    //            keys.push(format!("foo{}{}", i, j));
    //            keys.push(format!("bar{}{}", i, j));
    //        }
    //    }
    //
    //    for key in keys.iter() {
    //        tx.put(&std_cf, key, key, None)?;
    //        tx.put(&prefixed_cf, key, key, None)?;
    //    }
    //
    //    // Both CFs will start to seek somewhere inside `bar`.  The standard CF has no concept of prefix and
    //    // will keep scanning through the entire data set.
    //    //
    //    // `prefixed_cf` should stop at `bar99`.
    //    let mut prefixed_keys = Vec::new();
    //    let mut std_keys = Vec::new();
    //
    //    let iter = tx
    //        .iterate_all(&std_cf, None)?
    //        .into_strings()
    //        .from_key("bar90");
    //    for (key, _value) in iter.unwrap() {
    //        std_keys.push(key)
    //    }
    //
    //    let iter = tx
    //        .iterate_prefix(&prefixed_cf, None)?
    //        .into_strings()
    //        .from_key("bar90");
    //    for (key, _value) in iter.unwrap() {
    //        prefixed_keys.push(key)
    //    }
    //
    //    assert_eq!("bar90", prefixed_keys[0]);
    //    assert_eq!("bar99", prefixed_keys[9]);
    //    assert_eq!(10, prefixed_keys.len());
    //
    //    // std doesn't have any concept of a prefix, so it won't stop until it iterates through the
    //    // entire dataset
    //    assert_eq!("bar90", std_keys[0]);
    //    assert_eq!("bar99", std_keys[9]);
    //    assert_eq!("foo00", std_keys[10]);
    //    assert_eq!("foo99", std_keys[109]);
    //    assert_eq!(110, std_keys.len());
    //
    //    Ok(())
    // }

    /// Test the seeking to a specific key forward and backward with an iterator
    #[test]
    fn from_key_forward_reverse() -> Result<()> {
        // from_key and from_key_reverse seek the iterator to a specified key or, if that key isn't
        // found, the next (from_key) or previous (from_key_reverse) key in the table in sort order
        // from the speciifed key.
        //
        // to test this we'll generate some random keys, inserting half of them in the database and
        // leaving half of them absent.  We'll then query both sets to confirm they match
        // accordingly.
        const KEY_COUNT: usize = 100_000;

        fn get_next_pair(iter: &mut impl RocksIterator) -> Result<Option<(u64, Vec<u8>)>> {
            let current = unsafe {
                iter.get_current()?
                    .map(|(key, value)| (bytes_to_u64(key), Vec::from(value)))
            };
            Ok(current)
        }

        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let mut all_keys = Vec::new();
        let mut keys_in_db = Vec::new();
        let mut rand = rand::thread_rng();

        for _ in 0..KEY_COUNT {
            let key = rand.gen_range(0..(KEY_COUNT * 10) as u64);
            all_keys.push(key);

            if key % 2 == 0 {
                db.put(&cf, key.to_be_bytes(), b"", None)?;
                keys_in_db.push(key);
            }
        }

        // Put the vecs in sorted order so we can easily find adjacent entries
        all_keys.sort_unstable();
        keys_in_db.sort_unstable();

        let mut iter = db.iterate_all(&cf, None)?;
        for (index, key) in all_keys.iter().enumerate() {
            // Find this key or the first key after it
            iter.from_key(key.to_be_bytes());
            let from_key = get_next_pair(&mut iter)?;

            iter.from_key_reverse(key.to_be_bytes());
            let from_key_reverse = get_next_pair(&mut iter)?;

            let context = format!(
                "index={} key={} from_key={:?} from_key_reverse={:?}",
                index, key, from_key, from_key_reverse
            );

            // If we expect this key to be in the database, verify it was found
            // If not, verify the expected adjacent key is found
            match keys_in_db.binary_search(key) {
                Ok(_index) => {
                    // This key was inserted into the database.  Which means both `from_key` and
                    // `from_key_reverse` should be exactly `key`
                    let (db_key, _value) = from_key.unwrap();
                    assert_eq!(*key, db_key, "context: {}", context);
                    let (db_key, _value) = from_key_reverse.unwrap();
                    assert_eq!(*key, db_key, "context: {}", context);
                }
                Err(index) => {
                    // This key was not inserted into the database.  So we don't expect `from_key`
                    // or `from_key_reverse` to find this key.  Instead they should find the
                    // closest adjacent key either forward or backward.
                    //
                    // `index` is the index into `keys_in_db` where `key` would be, if it were
                    // inserted into that vec in sorted order.  That means the key that would be
                    // immediately before `key` in the vector is at `index-1`, and the key that
                    // would be immediatley after `key` is at `index`.
                    if index < keys_in_db.len() {
                        // This key is not the highest value key in the set, so it has a successor
                        let next_key = keys_in_db[index];
                        let (db_key, _value) = from_key.unwrap();
                        assert_eq!(
                            next_key, db_key,
                            "context: {} keys_in_db_index: {}",
                            context, index
                        );
                    } else {
                        // This key would be the last in the set, which means `from_key`
                        // would not return any key
                        assert_eq!(
                            None, from_key,
                            "context: {} keys_in_db_index: {}",
                            context, index
                        );
                    }

                    if index > 0 {
                        // This key is not the lowest value key in the set, so it has a predecessor
                        let prev_key = keys_in_db[index - 1];
                        let (db_key, _value) = from_key_reverse.unwrap();
                        assert_eq!(
                            prev_key, db_key,
                            "context: {} keys_in_db_index: {}",
                            context, index
                        );
                    } else {
                        // This key would be the first in the set, which means `from_key_reverse`
                        // would not return any key
                        assert_eq!(
                            None, from_key_reverse,
                            "context: {} keys_in_db_index: {}",
                            context, index
                        );
                    }
                }
            }
        }

        Ok(())
    }
}
