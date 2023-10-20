//! Every operation supported by RocksDB is exposed as a trait along with implementations of that
//! trait for every wrapper type for which it makes sense.  For example, the `Put` operation can be
//! performed on one of the three database types, or a `WriteBatch`, or a transaction.  The
//! semantics are the same, but due to the way the RocksDB C FFI is implemented the function calls
//! to be made are slightly different.
//!
//! The goal of this ops abstraction is to hide most of that complexity from the caller, while
//! avoiding excessive repetition in this library code.

use std::fmt;
use std::ptr;

mod all_ops;
mod begin_tx;
mod checkpoint;
mod compact;
mod delete;
mod flush;
mod get;
mod get_db_id;
mod get_db_ptr;
mod get_labels;
mod get_prop;
mod get_seq_num;
mod iterate;
mod key_exists;
mod live_files;
mod merge;
// Most op code is in `ops`, but the `tx` module also needs to report op metrics for `commit` and
// `rollback` operations
pub(crate) mod op_metrics;
mod open;
mod put;
mod stats;
mod with_logger;
mod write;

// Export all of the ops traits from this module
pub use self::{
    all_ops::*,
    begin_tx::BeginTrans,
    checkpoint::CreateCheckpoint,
    compact::Compact,
    delete::{Delete, DeleteRange},
    flush::Flush,
    get::{Get, GetForUpdate},
    get_db_id::GetDbId,
    get_db_ptr::GetDbPtr,
    get_prop::*,
    get_seq_num::GetLatestSequenceNumber,
    iterate::{IterateAll, IteratePrefix, IterateRange},
    key_exists::KeyExists,
    live_files::{GetLiveFiles, SstFile},
    merge::Merge,
    open::{DbOpen, DbOpenReadOnly},
    put::Put,
    stats::*,
    write::Write,
};

// Make select functions available to other modules in the crate
pub(crate) use self::{live_files::get_live_files_impl, open::ffi_open_helper};

// These are not used now but they're in place to support more advanced logging cases than are
// currently possible with the op-level instrumentation logic.
#[allow(unused_imports)]
pub(crate) use get_labels::*;
pub(crate) use with_logger::WithLogger;

/// Common base trait for all RocksDB operation traits.
///
/// For every type of operation supported in Rocks, there is a corresponding trait which exposes
/// methods related to that op.  For example:
///
/// * `Get`
/// * `Put`
/// * `Delete`
/// * etc
///
/// Each of those operation traits have this trait in common.
///
/// Furthermore, the operation traits are implemented on multiple concrete types.  This base trait
/// allows the `async_ops` module to provide high performance async implementations without
/// excessive cloning or reference counting overhead.  See that module for more.
pub trait RocksOpBase: Send {
    /// The handle of the type that implements this operation.  For the `DBLike` types, this will
    /// be the corresponding handle type (e.g., `DBHandle` for the `DB` class).
    ///
    /// The important property of this handle type is that it can be cloned very cheaply, because
    /// every time an async operaiton is performed, this handle will be cloned and passed to the
    /// worker thread where the operation runs.
    type HandleType: Send + Clone + 'static;

    fn handle(&self) -> &Self::HandleType;
}

/// All keys and values in RocksDB are opaque byte sequences.  There is no concept of typing or
/// scheme at this level.
///
/// This trait is implemented by any type which can be used as an immutable key or value type.
/// Default implementations are provide for most types; callers are unlikely to have to implement
/// this type themselves.
///
/// Any `AsRef<[u8]>` type is a `BinaryStr`.  This lets us do various convenient things:
///
/// # Examples
///
/// ```
/// # use roxide::*;
///
/// fn some_rocks_fn<K: BinaryStr>(key: K) {
///     # let _ = key;
///     //...
/// }
///
/// let array_key = [0u8; 10];
/// let vec_key = vec![0u8; 20];
/// let slice_key = vec_key.as_slice();
///
/// some_rocks_fn(&array_key);
/// some_rocks_fn(&vec_key);
/// some_rocks_fn(slice_key);
///
/// // Can also pass owned types as long as they as `AsRef<[u8]>`...
/// some_rocks_fn(array_key);
/// some_rocks_fn(vec_key);
/// ```
pub trait BinaryStr {
    fn as_slice(&self) -> &[u8];

    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    fn as_ptr(&self) -> *const u8 {
        self.as_slice().as_ptr()
    }

    fn as_ptr_and_len(&self) -> (*const u8, usize) {
        (self.as_ptr(), self.len())
    }
}

impl<T: AsRef<[u8]>> BinaryStr for T {
    fn as_slice(&self) -> &[u8] {
        self.as_ref()
    }
}

/// In some places, a binary string is optional, specifically with some range operations.  In that
/// case, `OptionalBinaryStr` is used, or its implementation on `Option<K> where K: BinaryStr`.  As
/// with `BinaryStr` callers are unlikely to need to implement this themselves.
pub trait OptionalBinaryStr {
    fn as_slice_opt(&self) -> Option<&[u8]>;

    fn len(&self) -> usize {
        self.as_slice_opt().map(|x| x.len()).unwrap_or(0)
    }

    fn is_empty(&self) -> bool {
        self.as_slice_opt().map(|x| x.is_empty()).unwrap_or(true)
    }

    fn as_ptr(&self) -> *const u8 {
        self.as_slice_opt()
            .map(|x| x.as_ptr())
            .unwrap_or(ptr::null())
    }

    fn as_ptr_and_len(&self) -> (*const u8, usize) {
        (self.as_ptr(), self.len())
    }
}

impl<T: BinaryStr> OptionalBinaryStr for Option<T> {
    fn as_slice_opt(&self) -> Option<&[u8]> {
        // Re-use the &'a version below
        self.as_ref().map(|x| x.as_slice())
    }
}

impl<'a, T: BinaryStr> OptionalBinaryStr for &'a Option<T> {
    fn as_slice_opt(&self) -> Option<&[u8]> {
        self.as_ref().map(|x| x.as_slice())
    }
}

/// A finite (closed) range of RocksDB keys.
///
/// Ranges in RocksDB are always `[start, end)`, which is mathemtical notation meaning that the
/// `start` key is included in the range, and the `end` key is the first key after the end of the
/// range.
///
/// Unlike `OpenKeyRange`, a regular `KeyRange` cannot be open at either end.  There must be
/// specific key values for both `start` and `end`.  It's an error if `end` is lexicographically
/// before `start`.
pub struct KeyRange<K: BinaryStr>(K, K);

impl<K: BinaryStr> KeyRange<K> {
    pub fn new(start: K, end: K) -> Self {
        debug_assert!(start.as_slice() < end.as_slice());
        KeyRange(start, end)
    }

    /// Given an inclusive range `[start, end]`, creates a range `[start, end+1]`, where it is
    /// assumed that `end+1` can be computed by incrementing the least significant byte of `end`,
    /// overflowing if necessary to the second least significant, etc.
    ///
    /// Be careful when using this, that this interpretation of `+1` is consistent with your key's
    /// binary representation.
    ///
    /// It's guaranteed that the resulting range will have the following properties:
    ///
    /// * Its `start` will be equal to `start` (albeit copied into a `Vec`)
    /// * Its `end` will be greater than `end` according to the lexigraphical definition of `>` as
    /// applied to types of `Vec<u8>`.
    ///
    /// This second property is the most important for forming an exclusive upper bound from an
    /// inclusive upper bound, but it may produce surprising results if you're not careful about
    /// how your keys are represented as binary slices.
    ///
    /// Note that this method always copies `start` and `end` into new `Vec`s; the original key
    /// values are not retained.
    ///
    /// # Examples
    ///
    /// ```
    /// # use roxide::KeyRange;
    ///
    /// let range = KeyRange::from_inclusive(b"foo1", b"foo8");
    ///
    /// // The simple ` + 1`logic adds one to the byte value for `8`, which happens to be the
    /// // character `9`
    /// assert_eq!(b"foo1", range.start().as_slice());
    /// assert_eq!(b"foo9", range.end().as_slice());
    /// ```
    ///
    /// But look at a more tricky example, where the lowest byte in `end` overflows:
    ///
    /// ```
    /// # use roxide::KeyRange;
    ///
    /// let range = KeyRange::from_inclusive(b"foo1", b"foo\xff");
    ///
    /// // The last character of `end` was already `0xff`, so the `+1` carries
    /// assert_eq!(b"foo1", range.start().as_slice());
    /// assert_eq!(b"fop\x00", range.end().as_slice());
    /// ```
    ///
    /// It gets even more strange if the entire `end` key is already `0xff`:
    ///
    /// ```
    /// # use roxide::KeyRange;
    ///
    /// let range = KeyRange::from_inclusive(b"foo1", b"\xff\xff\xff\xff");
    ///
    /// // In order to ensure the relation `range.end() > end`, a new byte was added to the end of
    /// // `end` key
    /// assert_eq!(b"foo1", range.start().as_slice());
    /// assert_eq!(b"\xff\xff\xff\xff\x01", range.end().as_slice());
    /// ```
    ///
    pub fn from_inclusive(start: K, end: K) -> KeyRange<Vec<u8>> {
        let start = Vec::from(start.as_slice());
        let mut end = Vec::from(end.as_slice());

        // Add one to end, starting at the highest (which lexicographically is also the
        // least-significant) byte and overflowing if need be
        let mut idx: usize = end.len();
        while idx > 0 && end[idx - 1] == 0xffu8 {
            idx -= 1;

            // 0xff + 1 = 0x00 (carry the 1)
            end[idx] = 0x00;
        }

        if idx > 0 {
            // Common case; `idx` is the first byte in from the end of the vector that is not
            // `0xff`.  Add one to it and we're done.
            end[idx - 1] += 1;
        } else {
            // `end` is all `0xff` already; need to work around this by making a new `end` that is
            // all `0xff` plus another byte at the end equal to 0x01
            end = vec![0xffu8; end.len()];
            end.push(0x01);
        }

        KeyRange::new(start, end)
    }

    /// Creates a key range that covers only one key: `key`.
    ///
    /// This uses the same technique as `from_inclusive` to perform the `+ 1` operation on `key`,
    /// so make sure that implementation is meaningful for your particular key type's binary
    /// representation.
    pub fn from_point(key: K) -> KeyRange<Vec<u8>> {
        KeyRange::from_inclusive(key.as_slice(), key.as_slice())
    }

    pub fn start(&self) -> &K {
        &self.0
    }

    pub fn end(&self) -> &K {
        &self.1
    }
}

impl<K: BinaryStr> fmt::Display for KeyRange<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}, {})",
            hex::encode(self.0.as_slice()),
            hex::encode(self.1.as_slice())
        )
    }
}

impl<K: BinaryStr> From<(K, K)> for KeyRange<K> {
    fn from(from: (K, K)) -> Self {
        KeyRange::new(from.0, from.1)
    }
}

impl<'a, K: BinaryStr> From<&'a (K, K)> for KeyRange<&'a [u8]> {
    fn from(from: &'a (K, K)) -> Self {
        KeyRange::new(from.0.as_slice(), from.1.as_slice())
    }
}

impl<'a, K: BinaryStr> From<&'a KeyRange<K>> for KeyRange<&'a [u8]> {
    fn from(from: &'a KeyRange<K>) -> Self {
        KeyRange::new(from.start().as_slice(), from.end().as_slice())
    }
}

/// An open key range; that is to say, a range of keys that can optionally be open at one or both
/// ends.
///
/// This allows representing any range which `KeyRange` can represent, with the addition of:
///
/// * From the first key up to (but not including) `K`
/// * From `K` (inclusive) to the last key (inclusive)
///
/// Some operations operate over a range of keys.  This struct provides some convenience methods to
/// make it more ergonomic to create such ranges.
///
/// Ranges can be open on either end by using the value `None` for either `start` or `end`,
/// respectively.
///
/// `OpenKeyRange` can be constructed in many different ways through it's various `From<T>`
/// implementations, so most of the time callers needn't construct one directly:
///
/// # Examples
///
/// # use roxide::*;
///
/// //You can use a tuple as a range
/// let keys = (b"foo", b"bar");
/// test_range(keys);
///
/// //Or a reference to a tuple
/// let keys = (b"foo", b"bar");
/// test_range(&keys);
///
/// //Or even a reference to another `KeyRange`
/// let range = KeyRange::new(b"foo", b"bar");
/// test_range(&range);
///
/// fn test_range<K: BinaryStr>(range: impl Into<OpenKeyRange<K>) {
///     assert_eq!(range.start().as_slice_opt(), Some(b"foo".as_slice()));
///     assert_eq!(range.end().as_slice_opt(), Some(b"bar".as_slice()));
/// }
///
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpenKeyRange<K> {
    /// All keys regardless of value
    AllKeys,

    /// All keys from the start up to (but not including) `0`
    KeysUpTo(K),

    /// All keys from `0` (inclusive) until the end
    KeysStartingFrom(K),

    /// All keys from `0` (inclusive) until `1` (exclusive).
    ///
    /// This is equivalent to a normal `KeyRange
    KeysBetween(K, K),
}

/// Internal helper used as a default value when we want to express a range open on both ends
fn key_range_all() -> OpenKeyRange<&'static [u8]> {
    OpenKeyRange::all()
}

impl<K> OpenKeyRange<K> {
    pub fn new(start: impl Into<Option<K>>, end: impl Into<Option<K>>) -> Self {
        let start = start.into();
        let end = end.into();

        match (start, end) {
            (Some(start), Some(end)) => OpenKeyRange::KeysBetween(start, end),
            (Some(start), None) => OpenKeyRange::KeysStartingFrom(start),
            (None, Some(end)) => OpenKeyRange::KeysUpTo(end),
            (None, None) => OpenKeyRange::AllKeys,
        }
    }

    fn all() -> Self {
        Self::new(None, None)
    }

    fn into_tuple(self) -> (Option<K>, Option<K>) {
        match self {
            OpenKeyRange::AllKeys => (None, None),
            OpenKeyRange::KeysBetween(start, end) => (Some(start), Some(end)),
            OpenKeyRange::KeysUpTo(end) => (None, Some(end)),
            OpenKeyRange::KeysStartingFrom(start) => (Some(start), None),
        }
    }
}

impl<K: BinaryStr> OpenKeyRange<K> {
    /// Tests if the specified value falls within the range
    pub fn is_in_range<T: BinaryStr>(&self, val: &T) -> bool {
        match self {
            OpenKeyRange::AllKeys => true,
            OpenKeyRange::KeysBetween(start, end) => {
                val.as_slice() >= start.as_slice() && val.as_slice() < end.as_slice()
            }
            OpenKeyRange::KeysUpTo(end) => val.as_slice() < end.as_slice(),
            OpenKeyRange::KeysStartingFrom(start) => val.as_slice() >= start.as_slice(),
        }
    }

    pub fn start(&self) -> Option<&[u8]> {
        match self {
            OpenKeyRange::KeysStartingFrom(start) => Some(start.as_slice()),
            OpenKeyRange::KeysBetween(start, _end) => Some(start.as_slice()),
            _ => None,
        }
    }

    pub fn end(&self) -> Option<&[u8]> {
        match self {
            OpenKeyRange::KeysUpTo(end) => Some(end.as_slice()),
            OpenKeyRange::KeysBetween(_start, end) => Some(end.as_slice()),
            _ => None,
        }
    }

    /// Creates a new `OpenKeyRange` with owned copies of the key(s) in this range,
    pub fn to_owned(&self) -> OpenKeyRange<Vec<u8>> {
        OpenKeyRange::new(self.start().map(Vec::from), self.end().map(Vec::from))
    }
}

/// When `OpenKeyRange` owns the keys, it has additional features
impl OpenKeyRange<Vec<u8>> {
    /// Convert the owned key range into raw pointer and length tuples.
    ///
    /// This form is not very useful in Rust, but it is very useful when passing those pointers to
    /// RocksDB
    pub(crate) fn into_raw_parts(self) -> OpenKeyRange<RawVec> {
        // Forget the two owned Vec values, and instead convert them into raw pointers.
        let (start, end) = self.into_tuple();

        let start = start.map(|mut vec| {
            vec.shrink_to_fit();
            let ptr = vec.as_mut_ptr();
            let len = vec.len();

            std::mem::forget(vec);

            RawVec(ptr, len)
        });

        let end = end.map(|mut vec| {
            vec.shrink_to_fit();
            let ptr = vec.as_mut_ptr();
            let len = vec.len();

            std::mem::forget(vec);

            RawVec(ptr, len)
        });

        OpenKeyRange::new(start, end)
    }

    /// Restores components previously produced with `into_raw_parts` into `Vec`s controlled by
    /// Rust.  This must be called at some point after `into_raw_parts`, when the memory is no
    /// longer being used by RocksDB C++ code, for example when the range iterator has been freed.
    pub(crate) unsafe fn from_raw_parts(range: OpenKeyRange<RawVec>) -> Self {
        let (start, end) = range.into_tuple();

        let start = start.map(|RawVec(ptr, len)| Vec::from_raw_parts(ptr, len, len));
        let end = end.map(|RawVec(ptr, len)| Vec::from_raw_parts(ptr, len, len));

        OpenKeyRange::new(start, end)
    }
}

// When the key range is expressed as raw pointers, it can't be used as a normal key range but does
// have some helpers for getting working with the pointers
impl OpenKeyRange<RawVec> {
    /// Free the memory associated with this range.  This MUST be called by whatever type owns this
    /// OpenKeyRange in that type's `Drop` implementation, or memory will be leaked!
    pub(crate) unsafe fn free(self) {
        // Just restoring the pointers to the original Vec types and then letting those go out of
        // scope is enough to free the memory.
        let _ = OpenKeyRange::<Vec<u8>>::from_raw_parts(self);
    }

    /// Gets the raw pointer and length of the start key, if any.
    pub(crate) fn raw_start(&self) -> Option<RawVec> {
        match self {
            OpenKeyRange::KeysStartingFrom(start) => Some(*start),
            OpenKeyRange::KeysBetween(start, _end) => Some(*start),
            _ => None,
        }
    }

    /// Treats the raw start pointer/length as a slice, assuming it's valid.
    #[cfg(test)]
    pub(crate) unsafe fn start_as_slice(&self) -> Option<&[u8]> {
        self.raw_start()
            .map(|RawVec(ptr, len)| std::slice::from_raw_parts(ptr, len))
    }

    /// Gets the raw pointer and length of the end key, if any.
    pub(crate) fn raw_end(&self) -> Option<RawVec> {
        match self {
            OpenKeyRange::KeysUpTo(end) => Some(*end),
            OpenKeyRange::KeysBetween(_start, end) => Some(*end),
            _ => None,
        }
    }

    /// Treats the raw start pointer/length as a slice, assuming it's valid.
    #[cfg(test)]
    pub(crate) unsafe fn end_as_slice(&self) -> Option<&[u8]> {
        self.raw_end()
            .map(|RawVec(ptr, len)| std::slice::from_raw_parts(ptr, len))
    }

    /// Sets the lower and upper iteration bounds of a `ReadOptions` struct to the pointers stored
    /// in this instance.
    ///
    /// NB: It is your responsibility to ensure that this memory lives at least as long as
    /// RocksDB's use of these read options.  If not, "undefined behavior" just doesn't describe
    /// the horrors you will unleash upon yourself and your descendents.
    pub(crate) fn set_read_options_bounds(&self, options: &mut crate::db_options::ReadOptions) {
        if let Some(RawVec(ptr, len)) = self.raw_start() {
            unsafe {
                crate::ffi::rocksdb_readoptions_set_iterate_lower_bound(
                    options.inner,
                    ptr as *const libc::c_char,
                    len as libc::size_t,
                );
            }
        }

        if let Some(RawVec(ptr, len)) = self.raw_end() {
            unsafe {
                crate::ffi::rocksdb_readoptions_set_iterate_upper_bound(
                    options.inner,
                    ptr as *const libc::c_char,
                    len as libc::size_t,
                );
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct RawVec(*mut u8, usize);

/// Rust doesn't allow mut pointers to be `Send` for obvious safety reasons, but in this case it's
/// ok because that pointer represents ownership of the Vec it came from.
unsafe impl Send for RawVec {}

impl<K: BinaryStr> fmt::Display for OpenKeyRange<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}, {})",
            self.start()
                .map(hex::encode)
                .unwrap_or_else(|| "None".to_string()),
            self.end()
                .map(hex::encode)
                .unwrap_or_else(|| "None".to_string()),
        )
    }
}

// Implement `From` for some convenient types that can be used to represent a key range

impl<K: BinaryStr> From<KeyRange<K>> for OpenKeyRange<K> {
    fn from(from: KeyRange<K>) -> Self {
        OpenKeyRange::KeysBetween(from.0, from.1)
    }
}

impl<K: BinaryStr> From<(K, K)> for OpenKeyRange<K> {
    fn from(from: (K, K)) -> Self {
        OpenKeyRange::new(from.0, from.1)
    }
}

impl<'a, K: BinaryStr> From<&'a (K, K)> for OpenKeyRange<&'a [u8]> {
    fn from(from: &'a (K, K)) -> Self {
        OpenKeyRange::new(from.0.as_slice(), from.1.as_slice())
    }
}

impl<'a, 'b, K: BinaryStr> From<&'a OpenKeyRange<K>> for OpenKeyRange<&'b [u8]>
where
    'a: 'b,
{
    fn from(from: &'a OpenKeyRange<K>) -> Self {
        let start: Option<&'a [u8]> = from.start();
        let end: Option<&'a [u8]> = from.end();

        OpenKeyRange::new(start, end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn okr_round_trip_with_raw_parts() {
        unsafe {
            // Test with a range open on both ends
            let range = key_range_all().to_owned();

            let raw = range.clone().into_raw_parts();
            assert_eq!(None, raw.start_as_slice());
            assert_eq!(None, raw.end_as_slice());

            let range2 = OpenKeyRange::from_raw_parts(raw);
            assert_eq!(range, range2);

            // Test with a range with a closed start
            let range = OpenKeyRange::KeysStartingFrom(b"foo").to_owned();

            let raw = range.clone().into_raw_parts();
            assert_eq!(Some(&b"foo"[..]), raw.start_as_slice());
            assert_eq!(None, raw.end_as_slice());

            let range2 = OpenKeyRange::from_raw_parts(raw);
            assert_eq!(range, range2);

            // Test range with a closed end
            let range = OpenKeyRange::KeysUpTo(b"bar").to_owned();

            let raw = range.clone().into_raw_parts();
            assert_eq!(None, raw.start_as_slice());
            assert_eq!(Some(&b"bar"[..]), raw.end_as_slice());

            let range2 = OpenKeyRange::from_raw_parts(raw);
            assert_eq!(range, range2);

            // Test range open at both ends
            let range = OpenKeyRange::KeysBetween(b"foo", b"bar").to_owned();

            let raw = range.clone().into_raw_parts();
            assert_eq!(Some(&b"foo"[..]), raw.start_as_slice());
            assert_eq!(Some(&b"bar"[..]), raw.end_as_slice());

            let range2 = OpenKeyRange::from_raw_parts(raw);
            assert_eq!(range, range2);
        }
    }
}
