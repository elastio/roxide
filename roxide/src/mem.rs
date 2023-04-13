// Copyright 2014 Tyler Neely
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

//! The RocksDB engine recognizes a distinction between data that it only needs to read, like keys
//! (and values for `Put` operations), and data that it returns to the caller.  In the former case,
//! no special Rust wrapper is required; any Rust `&[u8]` slice will work.
//!
//! In the latter case, Rocks has two options: it can allocate memory itself with `malloc` and
//! return the pointer to the caller, or it can return to the caller a pointer directly into its
//! block cache, sparing a memcopy.  This second variant is called `PinnedSlice` in the Rocks
//! source code.
//!
//! We don't support pinned slices now, as they are intended as an optimization for large (1KB+)
//! values, and for smaller values they have a cost in the synchronization they require with the
//! RocksDB cache to avoid freeing cache blocks that are still pinned.  In the future we may decide
//! to support this.
use std::borrow::Cow;
use std::ops::Deref;
use std::slice;

/// The maximum length of a buffer that should fit within a `SmallVec` used to efficiently store
/// values as part of a `Vec` of multiple values.
///
/// In cases where the RocksDB values being read are small (where "small" is subjective), and when
/// the values are being read in a vectorized operation that reads many keys at once and returns
/// the corresponding values in a `Vec`, the overhead associated with storing each value separately
/// on the heap can be substantial.  Note just the heap allocation (which is actually pretty well
/// optimized), but the loss of cache locality.
///
/// In such cases we use `SmallVec` to prefer to use an array to store the data, unless it won't
/// fit in the array.  This constant specifies the size of that `SmallVec` array.  Values larger
/// than this will be stored on the heap instead.
pub(crate) const SMALL_BUFFER_SIZE: usize = 128;

/// Vector of bytes stored in the database.
///
/// This is a `C` allocated byte array and a length value.
/// Normal usage would be to utilize the fact it implements `Deref<[u8]>` and use it as
/// a slice.
#[derive(Debug)]
pub struct DbVector {
    base: *mut u8,
    len: usize,
}

impl Deref for DbVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.base, self.len) }
    }
}

impl AsRef<[u8]> for DbVector {
    fn as_ref(&self) -> &[u8] {
        // Implement this via Deref so as not to repeat ourselves
        self
    }
}

impl Drop for DbVector {
    fn drop(&mut self) {
        unsafe {
            crate::ffi::rocksdb_free(self.base as *mut libc::c_void);
        }
    }
}

impl std::cmp::PartialEq for DbVector {
    fn eq(&self, rhs: &Self) -> bool {
        self.as_ref() == rhs.as_ref()
    }
}

/// `DBVector` has no thread afinity, although it's not thread-safe so it can only be accessed from
/// one thread at once
unsafe impl Send for DbVector {}

impl DbVector {
    /// Used internally to create a DBVector from a `C` memory block
    ///
    /// # Unsafe
    /// Requires that the ponter be allocated by a `malloc` derivative (all C libraries), and
    /// `val_len` be the length of the C array to be safe (since `sizeof(u8) = 1`).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let buf_len: libc::size_t = unsafe { mem::uninitialized() };
    /// // Assume the function fills buf_len with the length of the returned array
    /// let buf: *mut u8 = unsafe { ffi_function_returning_byte_array(&buf_len) };
    /// DBVector::from_c(buf, buf_len)
    /// ```
    ///
    /// # Safety
    ///
    /// Never call this function; it's intended for internal use by the `roxide`
    /// implementation.
    pub unsafe fn from_c(val: *mut u8, val_len: libc::size_t) -> DbVector {
        DbVector {
            base: val,
            len: val_len as usize,
        }
    }

    /// Convenience function to attempt to reinterperet value as string.
    pub fn to_string_lossy(&self) -> Cow<str> {
        String::from_utf8_lossy(self.as_ref())
    }
}

/// Wraps a `rocksdb::PinnableSlice` containing data returned from the database.
///
/// From the caller's perspective this is equivalent to `DBVector`, however the internal
/// implementation is completely different.
///
/// As of now, this doesn't actually preseve the `rocksdb::PinnableSlice`; instead it immediately
/// copies the data into a `SmallVec`.  This isn't quite as efficient as it would be to hold the
/// `PinnableSlice` and expose its contents directly, however that has a lot of implementation
/// challenges that would require in much more complex and unsafe code, particularly given that
/// this is intended for use with C++ arrays of `PinnableSlice` which means we'd have to put an
/// `Arc` around the array itself and `clone()` it for each instance of `DbPinnableSlice`.
///
/// Without having done any benchmarks we can't say for sure that all of that overhead is not worth
/// it from a performance perspective, but it seems likely to be of limited value.
#[derive(Debug)]
pub struct DbPinnableSlice {
    data: smallvec::SmallVec<[u8; SMALL_BUFFER_SIZE]>,
}

impl Deref for DbPinnableSlice {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.data.as_slice()
    }
}

impl AsRef<[u8]> for DbPinnableSlice {
    fn as_ref(&self) -> &[u8] {
        // Implement this via Deref so as not to repeat ourselves
        self.data.as_slice()
    }
}

impl std::cmp::PartialEq for DbPinnableSlice {
    fn eq(&self, rhs: &Self) -> bool {
        self.as_ref() == rhs.as_ref()
    }
}

impl DbPinnableSlice {
    /// Convenience function to attempt to reinterperet value as string.
    pub fn to_string_lossy(&self) -> Cow<str> {
        String::from_utf8_lossy(self.as_ref())
    }

    /// Create a new `DbPinnableSlice` instance from a pointer to a `rocksdb::PinnableSlice`.
    ///
    /// # Safety
    ///
    /// This function will copy the contents of the pinnable slice into memory Rust controls,
    /// therefore the caller must free this slice itself.  Failure to do so will not only leak
    /// memory but will also keep this block pinned in the block cache which will have other
    /// negative performance consequences.
    pub(crate) unsafe fn from_rocks_pinnable_slice(rocks_ptr: *const std::ffi::c_void) -> Self {
        let ptr = cpp!([rocks_ptr as "rocksdb::PinnableSlice*"] -> *const u8 as "const char*" {
            return rocks_ptr->data();
        });
        let len = cpp!([rocks_ptr as "rocksdb::PinnableSlice*"] -> usize as "size_t" {
            return rocks_ptr->size();
        });

        let slice = std::slice::from_raw_parts(ptr, len);
        let mut data = smallvec::SmallVec::with_capacity(len);
        data.extend_from_slice(slice);

        Self { data }
    }

    /// From a `std::vector` of `rocksdb::PinnableSlice`, construct a `Vec<DbPinnableSlice>`,
    /// copying the contents of each slice into a `DbPinnableSlice`
    ///
    /// # Safety
    ///
    /// `stl_vector` must point to a `std::vector<rocksdb::PinnableSlice>`.  It's not required that
    /// this pointer be to heap-allocated memory; it may be on the stack, and this function will
    /// not attempt to free any of that memory.
    ///
    /// Upon return from this function it's the caller's responsibility to free the STL vector and
    /// the pinnable slices it contains.
    pub(crate) fn extend_vec_from_pinnable_slices(
        rust_vec: &mut Vec<Self>,
        stl_vector: *mut std::ffi::c_void,
    ) {
        unsafe {
            let len = cpp!([stl_vector as "std::vector<rocksdb::PinnableSlice>*"] -> usize as "size_t" {
                return stl_vector->size();
            });

            rust_vec.reserve(len);
            let rust_vec_ptr: *mut Vec<Self> = rust_vec;

            cpp!([stl_vector as "std::vector<rocksdb::PinnableSlice>*", rust_vec_ptr as "void*"] {
                for (auto& rocksdb_slice : *stl_vector) {
                    rocksdb::PinnableSlice* slice_ptr = &rocksdb_slice;

                    rust!(MEMRocksSliceToRustSlice [rust_vec_ptr: *mut Vec<DbPinnableSlice> as "void*", slice_ptr: *const std::ffi::c_void as "rocksdb::PinnableSlice*"] {
                        unsafe {
                            (*rust_vec_ptr).push(DbPinnableSlice::from_rocks_pinnable_slice(slice_ptr));
                        }
                    });
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinnable_slice_round_trip() {
        unsafe {
            // Create a C++ vector of pinnable slices with some test data, then construct a vector of
            // DbPinnableSlice from that
            let stl_vector_ptr = cpp!([] -> *mut std::ffi::c_void as "std::vector<rocksdb::PinnableSlice>*" {
                auto stl_vector = new std::vector<rocksdb::PinnableSlice>();

                auto foo = rocksdb::PinnableSlice();
                foo.PinSelf(rocksdb::Slice("foo", 3));
                auto bar = rocksdb::PinnableSlice();
                bar.PinSelf(rocksdb::Slice("bar", 3));
                auto baz = rocksdb::PinnableSlice();
                baz.PinSelf(rocksdb::Slice("baz", 3));
                stl_vector->emplace_back(std::move(foo));
                stl_vector->emplace_back(std::move(bar));
                stl_vector->emplace_back(std::move(baz));

                return stl_vector;
            });

            let mut vec = Vec::new();
            DbPinnableSlice::extend_vec_from_pinnable_slices(&mut vec, stl_vector_ptr);

            let refs = vec.iter().map(|slice| slice.as_ref()).collect::<Vec<_>>();

            assert_eq!(vec![b"foo", b"bar", b"baz"], refs);
        }
    }
}
