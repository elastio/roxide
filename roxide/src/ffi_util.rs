// Copyright 2016 Alex Regueiro
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

use crate::error::{self, prelude::*};
use libc::{self, c_char, c_void};
use std::collections::HashMap;
use std::ffi;
use std::ffi::CStr;
use std::ffi::CString;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

cpp! {{
    #include "src/ffi_util.h"
    #include "src/ffi_util.cpp"
}}

pub(crate) fn error_message(ptr: *const c_char) -> String {
    let cstr = unsafe { CStr::from_ptr(ptr as *const _) };
    let s = String::from_utf8_lossy(cstr.to_bytes()).into_owned();
    unsafe {
        crate::ffi::rocksdb_free(ptr as *mut c_void);
    }
    s
}

/// RocksDB FFI APIs usually report an error by setting their `char**` `err` parameter to a
/// heap-allocated string containing an error message if there's an error, or NULL if the call
/// succeeded.  This function handles checking for failure and if the call failed, creating a
/// proper `Error` object and freeing the heap allocated string.
pub(crate) fn make_result<T>(ok: T, err: *const c_char) -> Result<T> {
    if !err.is_null() {
        error::DatabaseError {
            message: error_message(err),
        }
        .fail()
        .map_err(Error::report)
    } else {
        Ok(ok)
    }
}

pub(crate) unsafe fn string_from_char_ptr(string: *const libc::c_char) -> String {
    // First copy this into a buffer we control
    let string = CStr::from_ptr(string);

    // TODO: If the conversion to string fails should we silently replace it with the dummy char
    // like `to_string_lossy` does, or fail the operation?
    string.to_string_lossy().to_string()
}

pub(crate) unsafe fn path_from_char_ptr(path: *const libc::c_char) -> PathBuf {
    // First copy this into a buffer we control
    let path = CStr::from_ptr(path).to_owned();

    // Get the raw bytes
    let path_bytes = path.as_bytes();

    // On Unix, native chars are UTF-8, single bytes.  That makes it easy to get a OsStr
    #[cfg(unix)]
    let path_os_str = ffi::OsStr::from_bytes(path_bytes);

    // On Windows, native chars are UTF-16 so an OS string is made of UTF-16 chars
    // The RocksDB API on windows nonetheless produces single byte strings, so
    // we need to convert to a String first, and convert that to an OsString
    #[cfg(windows)]
    let path_os_str: ffi::OsString = String::from_utf8_lossy(path_bytes).to_string().into();

    path_os_str.into()
}

pub(crate) fn path_to_string<P: AsRef<Path>>(path: P) -> Result<String> {
    path.as_ref()
        .to_str()
        .map(|s| s.to_owned())
        .ok_or_else(|| Error::PathNotValidUtf8 {
            path: path.as_ref().to_owned(),
        })
}

pub(crate) fn path_to_cstring<P: AsRef<Path>>(path: P) -> Result<CString> {
    CString::new(path_to_string(path.as_ref())?).context(error::PathHasNullBytes {
        path: path.as_ref().to_owned(),
    })
}

/// Given a pointer to a `std::vector<std::string>`, returns a Rust `Vec` of the raw char pointers
/// for each string.
pub(crate) unsafe fn stl_str_vector_to_vec(
    stl_vector: *const libc::c_void,
) -> Vec<*const libc::c_char> {
    let mut rust_vec = Vec::<*const libc::c_char>::new();
    let rust_vec_ptr: *mut Vec<*const libc::c_char> =
        &mut rust_vec as *mut Vec<*const libc::c_char>;

    cpp!([stl_vector as "const std::vector<std::string>*", rust_vec_ptr as "void*"] {
        for(auto iterator=stl_vector->begin(); iterator != stl_vector->end(); iterator++) {
            auto c_ptr = iterator->c_str();
                rust!(FFIUtil_add_to_vec [rust_vec_ptr: *mut Vec<*const libc::c_char> as "void*", c_ptr: *const libc::c_char as "const char*"] {
                    unsafe {
                        (*rust_vec_ptr).push(c_ptr);
                    }
                });
        }
    });

    rust_vec
}

/// Internal helper which converts a `HashMap` of string name/value pairs into a C++
/// `std::unordered_map` which can be used with the RocksDB convenience APIs for building options
/// objects from such maps.
///
/// Returns a pointer to a std::unordered_map which must be freed with `free_unordered_map`.
#[allow(clippy::transmute_num_to_bytes)]
pub(crate) unsafe fn hashmap_to_stl_unordered_map<
    K: AsRef<str> + Into<Vec<u8>>,
    V: AsRef<str> + Into<Vec<u8>>,
>(
    map: &HashMap<K, V>,
) -> *const c_void {
    // Need to make the map into separate arrays of keys and values which will be easier
    // to put into an STL unordered map.
    let mut keys = Vec::<CString>::with_capacity(map.len());
    let mut values = Vec::<CString>::with_capacity(map.len());

    for (key, value) in map.iter() {
        keys.push(CString::new(key.as_ref()).expect("invalid key"));
        values.push(CString::new(value.as_ref()).expect("invalid value"));
    }

    let keys_ptrs: Vec<_> = keys.iter().map(|key| key.as_ptr()).collect();
    let values_ptrs: Vec<_> = values.iter().map(|value| value.as_ptr()).collect();

    let keys_ptr = keys_ptrs.as_ptr();
    let values_ptr = values_ptrs.as_ptr();
    let len = map.len();

    let unordered_map_ptr = cpp!([keys_ptr as "const char**", values_ptr as "const char**", len as "size_t"] -> *const c_void as "const UnorderedStringMap*" {
        auto map = new std::unordered_map<std::string, std::string>();

        for (size_t i = 0; i < len; i++) {
            map->insert(UnorderedStringMap::value_type(std::string(keys_ptr[i]), std::string(values_ptr[i])));
        }

        return map;
    });

    unordered_map_ptr
}

/// Frees a map previously allocated by `hashmap_to_stl_unordered_map`
pub(crate) unsafe fn free_unordered_map(map_ptr: *const c_void) {
    cpp!([map_ptr as "const UnorderedStringMap*"] -> () as "void" {
        delete map_ptr;
    });
}

/// Given a vector of Rust `&[u8]` slices, constructs a C++ `std::vector` of `rocksdb::Slice` containing the same
/// pointer and length information that the slices have internally.
///
/// # Safety
///
/// It's the caller's responsibility to ensure that the resulting `rocksdb::Slice` is only used
/// during the time when the corresponding Rust slice is on active.
///
/// It's also the caller's responsibility to ensure this `std::vector` is freed when no longer
/// used by calling `free_rocks_slices_vector`.
#[allow(clippy::transmute_num_to_bytes)]
pub(crate) unsafe fn rust_slices_to_rocks_slices<'a>(
    rust_slices: impl IntoIterator<Item = &'a [u8]>,
) -> *mut std::ffi::c_void {
    let rust_slices = rust_slices.into_iter();
    let len = rust_slices.size_hint().0;

    let stl_vector = cpp!([len as "size_t"] -> *mut c_void as "std::vector<rocksdb::Slice>*" {
        auto vec = new std::vector<rocksdb::Slice>();

        vec->reserve(len);

        return vec;
    });

    for slice in rust_slices {
        let len = slice.len();
        let ptr = slice.as_ptr();

        cpp!([stl_vector as "std::vector<rocksdb::Slice>*", ptr as "const char*", len as "size_t"] {
            stl_vector->emplace_back(rocksdb::Slice(ptr, len));
        });
    }

    stl_vector
}

/// Free the vector allocated with `rust_slices_to_rocks_slices`
pub(crate) unsafe fn free_rocks_slices_vector(rocks_slices: *mut std::ffi::c_void) {
    cpp!([rocks_slices as "std::vector<rocksdb::Slice>*"] {
        delete rocks_slices;
    });
}

/// Helper for cases where we have a `dyn T` for some trait `T`, and we want to be able to represent
/// it as a thin pointer (meaning a regular C pointer). `Arc::into_raw` loses the vtable
/// information when used with a `dyn` trait, so internally this struct just uses an Arc in a Box.
///
/// Using `Arc<T>` for the inner payload instead of `Box<T>` (which is what was used initially)
/// means that this is trivially `clone()`able.
#[allow(clippy::redundant_allocation)]
pub(crate) struct DynTraitArc<T: ?Sized>(Box<Arc<T>>);

impl<T: ?Sized> DynTraitArc<T> {
    pub(crate) fn new(value: Arc<T>) -> Self {
        DynTraitArc(Box::new(value))
    }

    /// Consume this wrapper and produce a raw pointer which can be passed to and from C++ code
    pub(crate) fn into_raw(self) -> *mut Arc<T> {
        Box::into_raw(self.0)
    }

    pub(crate) fn into_raw_void(self) -> *mut std::ffi::c_void {
        self.into_raw() as *mut std::ffi::c_void
    }

    /// Reconstitute this wrapper using a raw pointer previously produced by `into_raw`
    pub(crate) unsafe fn from_raw(raw: *mut Arc<T>) -> Self {
        DynTraitArc(Box::from_raw(raw))
    }

    pub(crate) unsafe fn from_raw_void(raw: *mut std::ffi::c_void) -> Self {
        DynTraitArc::from_raw(raw as *mut Arc<T>)
    }
}

impl<T: ?Sized> std::ops::Deref for DynTraitArc<T> {
    type Target = Box<Arc<T>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: ?Sized> std::ops::DerefMut for DynTraitArc<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: ?Sized> Clone for DynTraitArc<T> {
    fn clone(&self) -> Self {
        // The `Box` isn't clonable but the `Arc<T>` inside of it is
        let clone: Arc<T> = *(self.0).clone();

        Self::new(clone)
    }
}

/// Trait implemented by the structs which we use to wrap Rust trait objects so they can be passed
/// to and from C++.  By implementing this trait those structs get the from/into raw pointer
/// functions for free, and avoid repetition.
pub(crate) trait DynTraitWrapper<T: ?Sized + 'static>: Sized {
    /// Creates a new wrapper struct around an already-created `DynTraitArc` instance.  This should
    /// be implemented by the struct as a kind of internal constructor.  Users of `DynTraitWrapper`
    /// implementations should use `wrap` to create instances
    fn new(value: DynTraitArc<T>) -> Self;

    /// Creates a new wrapper struct around a boxed trait object
    fn wrap(value: Arc<T>) -> Self {
        Self::new(DynTraitArc::new(value))
    }

    /// Consumes the wrapper struct and returns the internal `DynTraitArc` object
    fn unwrap(self) -> DynTraitArc<T>;

    /// Returns a reference to the internal `DynTraitArc` object
    fn inner(&self) -> &DynTraitArc<T>;

    fn inner_mut(&mut self) -> &mut DynTraitArc<T>;

    /// Consume the struct and return a C pointer for passing the wrapped trait object into C code
    fn into_raw_void(self) -> *mut std::ffi::c_void {
        self.unwrap().into_raw_void()
    }

    /// Resurrect the wrapper struct from a pointer previously returned by `into_raw_void`.
    /// Obviously very unsafe so be careful.
    ///
    /// In particular be mindful of two things:
    ///
    /// * It is UB to use `from_raw_void` more than once on a given pointer.  This can create
    /// multiple copies of the wrapper struct each pointing to the same underlying value.  That
    /// will lead to a double free and all manner of other nastiness.
    ///
    /// * Once the value returned from `from_raw_void` goes out of scope, the wrapped trait object
    /// will be freed.  If there is still some C code somewhere carrying around this pointer, it is
    /// now holding a dangling pointer.  Make sure you only let this happen when you know the C
    /// code is no longer holding that pointer.   In a case where you want to temporarily ressurect
    /// the wrapper but not take ownership of it and free it, use `temp_from_raw_void`.
    unsafe fn from_raw_void(raw: *mut std::ffi::c_void) -> Self {
        Self::new(DynTraitArc::from_raw_void(raw))
    }

    /// Temporarily convert a raw void pointer to a mutable reference to the wrapper, pass it to a
    /// closure, then immediately put it back to being a raw pointer again.
    unsafe fn temp_from_raw_void<R, F: FnOnce(&Self) -> R>(raw: *mut std::ffi::c_void, func: F) -> R
    where
        F: std::panic::UnwindSafe,
        Self: std::panic::RefUnwindSafe,
    {
        let me = Self::from_raw_void(raw);

        // if `func` panics, the default behavior will be to unwind the stack, which would mean
        // `me` would be dropped, which means the wrapped object will be dropped.  That's not the
        // behavior we want.  Capture any panics and put the struct back into its raw form before
        // propagating
        let panic_result = std::panic::catch_unwind(|| func(&me));

        match panic_result {
            Ok(result) => {
                // No panics, `result` is the return value of `func`, process as normal

                // if we let `me` go out of scope right now, the trait object will be dropped.  That is
                // contrary to what we want.
                let me_raw = me.into_raw_void();

                // TODO: It's not clear to me from the Rust docs whether or not it's possible for the
                // memory pointed to in a `Arc` to be moved.  This assert is here to ease my mind that this
                // doesn't happen.  It might be necessary to use `Pin` to force the pointer to remain
                // unchanged
                assert!(me_raw == raw);

                result
            }
            Err(cause) => {
                // Panic.  We'll propagate the panic, but first forget `me` so it doesn't get freed
                // during the unwind
                let _me_raw = me.into_raw_void();

                std::panic::resume_unwind(cause);
            }
        }
    }

    /// Temporarily access a reference to the wrapped object
    fn with_inner<R, F: FnOnce(&Arc<T>) -> R>(&self, func: F) -> R
    where
        T: 'static,
    {
        // Falling down the rabbit hole....
        let inner: &DynTraitArc<T> = self.inner();

        // Despite the clippy warning, my intention here is to show with explicit types how we get
        // to the ultimate value passed to the func.  I know what I'm doing, this is just to make
        // the code a bit more clear and less magical
        #[allow(clippy::borrowed_box)]
        let inner_inner: &Box<Arc<T>> = inner;

        #[allow(clippy::borrowed_box)]
        let innermost: &Arc<T> = inner_inner;

        func(innermost)
    }

    /// Frees memory associated with a pointer returned from `into_raw_void`.
    ///
    /// # Danger
    ///
    /// Be sure you understand when to use this and when not to!
    ///
    /// NEVER call this function on a pointer that you have also passed to `from_raw_void`, as that
    /// will lead to a double free.  You can EITHER call `from_raw_void` if you need to access
    /// something in the struct before it gets freed automatically when it goes out of scope, OR
    /// you can call `free_raw_void` if you just want to free the struct and don't need to access
    /// any of its members.
    unsafe fn free_raw_void(raw: *mut std::ffi::c_void) {
        // Rust will free the memory automatically when `_me` goes out of scope
        let _me = Self::from_raw_void(raw);
    }
}

#[macro_export]
macro_rules! make_dyn_trait_wrapper {
    ( $wrapper_type:ident, $trait_type:ident) => {
        make_dyn_trait_wrapper!(pub(self) $wrapper_type, $trait_type);
    };
    ( $vis:vis $wrapper_type:ident, $trait_type:ident) => {
        $vis struct $wrapper_type($crate::ffi_util::DynTraitArc<dyn $trait_type + 'static>);

        impl $wrapper_type {
            $vis fn new(value: $crate::ffi_util::DynTraitArc<dyn $trait_type + 'static>) -> Self {
                Self(value)
            }
        }

        impl std::ops::Deref for $wrapper_type {
            type Target = ::std::sync::Arc<dyn $trait_type + 'static>;

            fn deref(&self) -> &Self::Target {
                // Call the trait function `inner`, but with thsi awkward syntax required in a macro
                let inner = <Self as $crate::ffi_util::DynTraitWrapper<dyn $trait_type + 'static>>::inner(self);
                &*inner
            }
        }

        impl $crate::ffi_util::DynTraitWrapper<dyn $trait_type> for $wrapper_type {
            fn new(value: $crate::ffi_util::DynTraitArc<dyn $trait_type + 'static>) -> Self {
                Self::new(value)
            }

            fn unwrap(self) -> $crate::ffi_util::DynTraitArc<dyn $trait_type + 'static> {
                self.0
            }

            fn inner(&self) -> &$crate::ffi_util::DynTraitArc<dyn $trait_type + 'static> {
                &self.0
            }

            fn inner_mut(&mut self) -> &mut $crate::ffi_util::DynTraitArc<dyn $trait_type + 'static> {
                &mut self.0
            }
        }

        /// The wrapper type is unwind safe, in that it is written so that it cannot expose broken
        /// invariants due to a panic.
        impl std::panic::UnwindSafe for $wrapper_type {
        }
        impl std::panic::RefUnwindSafe for $wrapper_type {
        }
    };
}

/// Invokes a RocksDB C FFI function which adheres to the Rocks convention of taking a char**
/// parameter as the last argument, which is populated with an error message if the call fails,
/// otherwise is set to NULL.
///
/// This macro evaluates to a Result<T> where `T` is whatever the return type of the function is
#[macro_export]
macro_rules! ffi_try {
    ( $($function:ident)::*( $( $arg:expr,)* ) ) => ({
        let mut err: *mut ::libc::c_char = ::std::ptr::null_mut();
        let result = $($function)::*($($arg),*, &mut err);
        $crate::ffi_util::make_result(result, err)
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Mutex;

    /// A simple test trait which adds some internal number to a caller provided number and returns
    /// the result
    pub(crate) trait TestTrait {
        fn add_something(&self, num: usize) -> usize;

        fn set_base(&self, num: usize);
    }

    /// Simple implementation that has some internal state
    #[allow(clippy::mutex_atomic)]
    struct TraitImpl(Mutex<usize>);

    impl TraitImpl {
        #[allow(clippy::mutex_atomic)]
        fn new(num: usize) -> Self {
            TraitImpl(Mutex::new(num))
        }
    }

    impl TestTrait for TraitImpl {
        #[allow(clippy::mutex_atomic)]
        fn add_something(&self, num: usize) -> usize {
            *self.0.lock().unwrap() + num
        }

        #[allow(clippy::mutex_atomic)]
        fn set_base(&self, num: usize) {
            let mut guard = self.0.lock().unwrap();
            *guard = num;
        }
    }

    make_dyn_trait_wrapper!(pub(crate) TestTraitCppWrapper, TestTrait);

    #[test]
    fn dyn_trait_arc_round_trip() {
        // Make sure the DynTraitArc can round trip to a C pointer and back and still work
        let test_struct = TraitImpl::new(42);
        let test_struct: Arc<dyn TestTrait> = Arc::new(test_struct);
        let test_struct = DynTraitArc::<dyn TestTrait>::new(test_struct);

        let test_struct_clone = test_struct.clone();

        assert_eq!(43, test_struct.add_something(1));

        test_struct_clone.set_base(50);

        assert_eq!(51, test_struct.add_something(1));

        let test_ptr = test_struct.into_raw_void();

        let test_struct = unsafe { DynTraitArc::<dyn TestTrait>::from_raw_void(test_ptr) };

        assert_eq!(51, test_struct.add_something(1));

        drop(test_struct);

        assert_eq!(51, test_struct_clone.add_something(1));
    }

    #[test]
    fn wrapper_round_trip() {
        // Same test as above, but now we're also exercising the macro-generated wrapper struct
        let test_struct = TestTraitCppWrapper::wrap(Arc::new(TraitImpl::new(42)));

        assert_eq!(43, test_struct.add_something(1));

        let test_ptr = test_struct.into_raw_void();

        let test_struct = unsafe { TestTraitCppWrapper::from_raw_void(test_ptr) };

        assert_eq!(43, test_struct.add_something(1));
    }

    #[test]
    fn temp_from_raw_void_doesnt_free() {
        let test_struct = TestTraitCppWrapper::wrap(Arc::new(TraitImpl::new(42)));

        let test_ptr = test_struct.into_raw_void();

        unsafe {
            TestTraitCppWrapper::temp_from_raw_void(test_ptr, |test_struct| {
                assert_eq!(43, test_struct.add_something(1));
            });

            TestTraitCppWrapper::temp_from_raw_void(test_ptr, |test_struct| {
                assert_eq!(43, test_struct.add_something(1));
            });
        }

        let test_struct = unsafe { TestTraitCppWrapper::from_raw_void(test_ptr) };
        assert_eq!(43, test_struct.add_something(1));
    }

    #[test]
    fn free_raw_void_works() {
        let test_struct = TestTraitCppWrapper::wrap(Arc::new(TraitImpl::new(42)));
        let test_ptr = test_struct.into_raw_void();
        unsafe {
            TestTraitCppWrapper::free_raw_void(test_ptr);
        }
    }

    #[test]
    fn with_inner_works() {
        // Make sure we can expose the inner wrapped object as a mutable reference
        let test_struct = TestTraitCppWrapper::wrap(Arc::new(TraitImpl::new(42)));

        assert_eq!(43, test_struct.add_something(1));

        test_struct.with_inner(|inner| {
            assert_eq!(43, inner.add_something(1));
            inner.set_base(101);
            assert_eq!(102, inner.add_something(1));
        });

        assert_eq!(102, test_struct.add_something(1));
    }

    #[test]
    fn temp_with_raw_pointer_and_with_inner_works() {
        // Combine two tests; using just a void pointer operate on a mutable reference to the inner
        // object
        let test_struct = TestTraitCppWrapper::wrap(Arc::new(TraitImpl::new(42)));

        let test_ptr = test_struct.into_raw_void();

        unsafe {
            TestTraitCppWrapper::temp_from_raw_void(test_ptr, |test_struct| {
                test_struct.with_inner(|inner| {
                    assert_eq!(43, inner.add_something(1));
                    inner.set_base(101);
                    assert_eq!(102, inner.add_something(1));
                });
            });

            TestTraitCppWrapper::temp_from_raw_void(test_ptr, |test_struct| {
                assert_eq!(102, test_struct.add_something(1));
            });
        }

        let test_struct = unsafe { TestTraitCppWrapper::from_raw_void(test_ptr) };
        assert_eq!(102, test_struct.add_something(1));
    }

    #[test]
    fn temp_with_raw_pointer_propagates_panic() {
        // If there's a panic in the `test_with_raw_pointer` callback, it should propagate that
        // panic and _NOT_ free the wrapped object prematurely
        let test_struct = TestTraitCppWrapper::wrap(Arc::new(TraitImpl::new(42)));

        let test_ptr = test_struct.into_raw_void();
        let result = std::panic::catch_unwind(|| unsafe {
            TestTraitCppWrapper::temp_from_raw_void(test_ptr, |_test_struct| {
                panic!("I'm freaking out, man!");
            });
        });

        let err = result.err().expect("expected to capture panic");
        let err = err
            .downcast::<&'static str>()
            .expect("panic was not of the expected type");

        assert!(err.contains("I'm freaking out"));

        // In spite of this, our test object should still be valid, having been reverted back to a
        // pointer before the panic dropped the object
        unsafe {
            TestTraitCppWrapper::temp_from_raw_void(test_ptr, |test_struct| {
                assert_eq!(43, test_struct.add_something(1));
            });
        }
    }
}
