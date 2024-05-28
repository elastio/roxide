//! The `rust-rocksdb` crate provides a `DB` class but it's very awkward to adapt this to support
//! the two different kind of transaction databases plus a read-only database.
//!
//! Based on an idea I first saw in [a rust-rocksdb
//! PR](https://github.com/rust-rocksdb/rust-rocksdb/pull/268/files), this crate bypasses much of
//! the Rust layer and presents a new more idiomatic set of types.
//!
//! In this module we define some types which let us interact with the RocksDB FFI types in type
//! safe and (relatively) memory-safe ways.
//!
//! Relevant concepts are:
//!
//! * `RocksClass` - This is the FFI data type corresponding to a particular class in Rocks.  For
//! example `rocksdb_t` is the type that corresponds to `DB`.  Each `RocksClass` defines how to
//! free itself, when it gets dropped
//!
//! * `RocksObject` - This is some type which in some way contains a pointer to a `RocksClass`, and
//! logically represents an instance of that class.  For example `DB` contains in its
//! implementation a pointer to `rocksdb_t`, so an instance of `DB` is a `RocksObject`.
//! `RocksObject`s don't necessarily have any responsibility to free their underlying Rocks object,
//! although all of the implementations that callers would interact with (like `DB`, `Transaction`,
//! `WriteOptions`, etc, do).
//!
//! * `Handle` - A Rust struct which contains a pointer to a Rocks object, and when `drop()`ed
//! ensures that the Rocks FFI function to free that object gets called.  `Handle` also implements
//! the `RocksObject` trait so it can be used this way.
//!
//! * `ArcHandle` - A Rust struct which uses `Arc` to provide a reference counted, thread-safe
//! layer on top of `Handle`.  This ensures the underlying `Handle` (and thus the underlying Rocks
//! object) is only freed when there are no more references to it.  Code can very cheaply `clone()`
//! an `ArcHandle` and send it to another thread, etc.

use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::Arc;

/// A `RocksClass` is any of the types which `bindgen` created based on the RocksDB C FFI which
/// must be freed by the caller (that is, us) by calling some C function in the API.  Obviously
/// each type is freed differently, so there is one implementatin of this trait for each type of
/// object we're interested in.
pub trait RocksClass {
    unsafe fn delete(ptr: NonNull<Self>);
}

/// Marker trait which indicates a particular `RocksClass` FFI type is safe to send between
/// threads.  If this is implemented, then the corresponding `Handle` and `ArcHandle` types will
/// also be `Send`
#[allow(clippy::missing_safety_doc)]
pub unsafe trait RocksClassSend: RocksClass {}

/// Marker trait which indicates a particular `RocksClass` FFI type is safe to share between
/// threads.  If this is implemented, then the corresponding `Handle` and `ArcHandle` types will
/// also be `Sync`
#[allow(clippy::missing_safety_doc)]
#[allow(dead_code)]
pub unsafe trait RocksClassSync: RocksClass {}

/// A `Handle` is a type that wraps a pointer to one of the RocksDB FFI structs, like `rocksdb_t*`
/// or something.  These are always allocated and freed by the RocksDB C API, though each one is
/// freed in a different way.
pub(crate) struct Handle<R: RocksClass>(NonNull<R>);

impl<R: RocksClass> Handle<R> {
    pub fn new(ptr: NonNull<R>) -> Self {
        Self(ptr)
    }
}

impl<R: RocksClass> Drop for Handle<R> {
    fn drop(&mut self) {
        unsafe { R::delete(self.0) }
    }
}

/// Handles are `Send` if the underlying Rocks class is
unsafe impl<R: RocksClassSend> Send for Handle<R> {}

/// Handles are `Sync` if the underlying Rocks class is
unsafe impl<R: RocksClassSync> Sync for Handle<R> {}

/// Helper macro to make it easy to implement `RocksClass` for the various FFI types
#[macro_export]
macro_rules! rocks_class {
    ($handle_type:ident, $ffi_type:ty, $delete_func:path) => {
            impl $crate::handle::RocksClass for $ffi_type {
                unsafe fn delete(ptr: ::std::ptr::NonNull<$ffi_type>) {
                $delete_func(ptr.as_ptr());
            }
        }

        pub(crate) type $handle_type = $crate::handle::ArcHandle<$ffi_type>;
    };
    ($handle_type:ident, $ffi_type:ty, $delete_func:path, @send) => {
        rocks_class!($handle_type, $ffi_type, $delete_func);

        unsafe impl $crate::handle::RocksClassSend for $ffi_type {}
    };
    ($handle_type:ident, $ffi_type:ty, $delete_func:path, @sync) => {
        rocks_class!($handle_type, $ffi_type, $delete_func);

        unsafe impl $crate::handle::RocksClassSync for $ffi_type {}
    };
    ($handle_type:ident, $ffi_type:ty, $delete_func:path, @sync, @send) => {
        rocks_class!($handle_type, $ffi_type, $delete_func);

        unsafe impl $crate::handle::RocksClassSync for $ffi_type {}
        unsafe impl $crate::handle::RocksClassSend for $ffi_type {}
    };
    ($handle_type:ident, $ffi_type:ty, $delete_func:path, @send, @sync) => {
        rocks_class!($handle_type, $ffi_type, $delete_func, @sync, @send);
    }
}

/// When one of our structs wraps a particular RocksDB FFI type, it will implement this trait for
/// that type.  Then we can define higher-level operations as additional traits implemented in
/// terms of this one.
pub trait RocksObject<R> {
    fn rocks_ptr(&self) -> NonNull<R>;
}

/// Almost by definition, a pointer to a Rocks class has a handle to that class.  This allows us to
/// use naked pointers as handles in cases where concepts of ownership are not relevant
impl<R> RocksObject<R> for NonNull<R> {
    fn rocks_ptr(&self) -> NonNull<R> {
        *self
    }
}

/// For our convenience mostly, a reference to a `RocksObject` is also a `RocksObject`, so we can
/// declare args as `impl RocksObject<T>` and still pass references to those args
impl<'a, R, T> RocksObject<R> for &'a T
where
    T: RocksObject<R>,
{
    fn rocks_ptr(&self) -> NonNull<R> {
        (*self).rocks_ptr()
    }
}

/// Some rocks objects, notably the ones that wrap options types like `rocksdb_writeoptions_t`,
/// are required parameters to operations (`Put` in the case of `rocksdb_writeoptions_t`).  But
/// most of the time we don't need to override the defaults, so we can use the default contents of
/// the options struct.
///
/// Doing this each call requires a call into the C FFI and a heap allocation, and a corresponding
/// free at the end of the operation.  That's wasteful.
///
/// Thus, rocks objects that need to be used in this way implement this trait which provides a
/// global, static default that will be passed to the Rocks C FFI in the common case when no
/// explicit value is required.
pub trait RocksObjectDefault<R>: RocksObject<R> + Sized + 'static {
    fn default_object() -> &'static Self;

    fn from_option(value: impl Into<Option<Self>>) -> CowHandle<R, Self> {
        let value = value.into();

        match value {
            None => {
                // No value is provided, so use the default
                CowHandle::Borrowed(Self::default_object().rocks_ptr())
            }

            Some(value) => {
                // A specific value is provided, so use that
                CowHandle::Owned(value)
            }
        }
    }
}

/// `ArcHandle` is a reference-counted instance of `Handle`.  The underlying `Handle` is freed only
/// when the reference count goes to zero.  This allows types whose lifetime depends on some parent
/// object to hold a reference to that object.  In this way we avoid polluting the code with
/// elaborate lifetime contortions caused by trying to fit C's anything-goes semantics with Rust's
/// rigid borrow checker.
pub struct ArcHandle<R: RocksClass>(Arc<Handle<R>>);

impl<R: RocksClass> ArcHandle<R> {
    pub(crate) fn new(t: NonNull<R>) -> Self {
        Self(Arc::new(Handle::<R>::new(t)))
    }

    /// Reconstitutes this `ArcHandle` from the raw pointer previously produced by `into_raw`
    pub(crate) unsafe fn from_raw(raw: NonNull<Handle<R>>) -> Self {
        Self(Arc::from_raw(raw.as_ptr() as *const _))
    }

    /// Consumes this handle and converts it into a raw pointer.  The only reasonably safe thing
    /// possible to do with this pointer is to use it to get back the `ArcHandle` with `from_raw`
    pub(crate) fn into_raw(self) -> NonNull<Handle<R>> {
        unsafe { NonNull::new_unchecked(Arc::into_raw(self.0) as *mut _) }
    }
}

impl<R: RocksClass> Clone for ArcHandle<R> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<R: RocksClass> From<NonNull<R>> for ArcHandle<R> {
    fn from(t: NonNull<R>) -> Self {
        Self::new(t)
    }
}

/// If the rocks class is safe to send between threads, then the handle should be too.
///
/// It may not be immediately obvious why this is required.
///
/// The `Arc<T>` struct is `Send` only if `T` is _both_ `Sync` and `Send`.  That's because the ref
/// count on `T` can go to zero on possibly any thread, meaning `T` can be `drop`ed from an
/// arbitrary thread.  Therefore, out of an abundance of caution, `Arc<T>` requires `T` be `Sync`
/// in order for `Arc<T>` to implement `Send`.
///
/// In our very specific case, we know that the Rocks equivalent of `drop`, which is calling the
/// applicable destroyer function to free the memory associated with an object, is in fact safe to
/// call from any thread (although, obviously, it can only be called once).
///
/// Therefore we override the default behavior and force `ArcHandle` to be `Send` only if `R` is
/// `Send`, without regard to whether `R` is `Sync`.
unsafe impl<R: RocksClassSend> Send for ArcHandle<R> {}

impl<R: RocksClass> Deref for ArcHandle<R> {
    type Target = NonNull<R>;

    fn deref(&self) -> &Self::Target {
        &self.0.as_ref().0
    }
}

/// A frequently recurring pattern in the RocksDB APIs is an operation that takes an optional
/// parameter which contains some kind of options struct, like `WriteOptions` or
/// `TransactionOptions`, etc.  Most of the time the defaults are fine so we allow the caller to
/// pass `None`, in which case default values should be used.  Alternatively the caller can pass in
/// a reference to an existing options struct, in which case we'll use that reference.
///
/// This happens so often that this helper was created to avoid repetition and make the code
/// clearer.
///
/// This takes as input one of these optional parameters (using `Into` is an ergonomic feature
/// allowing users to pass their value directly instead of wrapping it in `Some` first), and
/// returns either that reference (if there was one), or an owned version of `T` initialized with
/// defaults.
///
/// If the reference was used, then when `CowHandle` is dropped, `delete()` is not called.  However
/// if the reference was `None` and a default instance was used, then that default instance is
/// cleaned up when this goes out of scope.
///
/// The name (admitedly confusing) is derived from the `std::borrow::Cow` type.  In that case `Cow`
/// stands for "copy on write", but this version doesn't support mutation at all so there's no
/// "write" to "copy on".  Nonetheless this is otherwise very similar in concept and
/// implementation.
pub enum CowHandle<R, T: RocksObject<R>> {
    Owned(T),
    Borrowed(NonNull<R>),
}

impl<R, T: RocksObject<R>> RocksObject<R> for CowHandle<R, T> {
    fn rocks_ptr(&self) -> NonNull<R> {
        match self {
            CowHandle::Owned(h) => h.rocks_ptr(),
            CowHandle::Borrowed(h) => *h,
        }
    }
}

/// If the options type itself is `Send`, so is the handle
unsafe impl<R, T: RocksObject<R> + Send> Send for CowHandle<R, T> {}

/// If the options type itself is `Sync`, so is the handle
unsafe impl<R, T: RocksObject<R> + Sync> Sync for CowHandle<R, T> {}

/// All of the other handle types have a type parameter that indicates the `RocksClass` of the
/// pointer they hold.  But some applications don't require a strongly typed handle, just the
/// ability to hold a clonable reference to "something", and decrement its ref count when dropped.
/// This is also used to avoid contaminating the public API with handle-related types.
///
/// Thus this handle, which is meant to wrap some other handle type, while erasing the type
/// information that indicates what type it is.
pub struct AnonymousHandle {
    raw_ptr: NonNull<std::ffi::c_void>,
    cloner: &'static dyn Fn(NonNull<std::ffi::c_void>) -> NonNull<std::ffi::c_void>,
    dropper: &'static dyn Fn(NonNull<std::ffi::c_void>),
}

impl AnonymousHandle {
    /// Wraps any `ArcHandle`, as long as the handle is `Send`.  Because `AnonymousHandle` hides
    /// the handle type, it's not possible to call any of the wrapped handle's methods, except
    /// `clone()` (which is handled by `Arc` and not the underlying RocksDB object) and `drop()`
    /// (which will be called exactly once from only one thread).  So it doesn't matter if the
    /// underlying object is `Sync` or not, as long as it's okay to create it in one thread and
    /// then transfer ownership to another (aka `Send`).
    pub(crate) fn wrap_handle<R: RocksClass + RocksClassSend>(
        handle: ArcHandle<R>,
    ) -> AnonymousHandle {
        AnonymousHandle {
            raw_ptr: handle.into_raw().cast::<std::ffi::c_void>(),
            cloner: &|raw_ptr| {
                // Re-constitute the handle just to run clone() on it, then convert it back to raw so it
                // doesn't get `drop`ed here
                let handle: ArcHandle<R> =
                    unsafe { ArcHandle::from_raw(raw_ptr.cast::<Handle<R>>()) };

                let clone = handle.clone();

                // This should always be the case because the boxed address shouldn't move, but
                // just to make sure the code will panic if the previous `raw_ptr` is no longer the
                // correct address
                debug_assert_eq!(raw_ptr, handle.into_raw().cast::<std::ffi::c_void>());

                clone.into_raw().cast::<std::ffi::c_void>()
            },
            dropper: &|raw_ptr| {
                // Re-constitute the handle just to drop it
                let handle: ArcHandle<R> =
                    unsafe { ArcHandle::from_raw(raw_ptr.cast::<Handle<R>>()) };
                drop(handle)
            },
        }
    }

    /// Wraps anything that's `Send + Clone + Drop`, regardless of it's actual type.
    ///
    /// This is a more generalized version of `wrap_handle`, for use in the relatively rare cases
    /// where we don't know have a concrete type that can be constrained to a `ArcHandle`.
    ///
    /// This will `Box` the value and so it has slightly higher overhead than `wrap_handle`, which
    /// should be used instead of this method whenever possible
    pub(crate) fn wrap_opaque_type<T: Send + Clone>(handle: T) -> AnonymousHandle {
        AnonymousHandle {
            raw_ptr: NonNull::new(Box::into_raw(Box::new(handle)))
                .unwrap()
                .cast::<std::ffi::c_void>(),
            cloner: &|raw_ptr| {
                // Re-constitute the handle just to run clone() on it, then convert it back to raw so it
                // doesn't get `drop`ed here
                let handle: Box<T> = unsafe { Box::from_raw(raw_ptr.cast::<T>().as_ptr()) };

                let clone = handle.clone();

                // This should always be the case because the boxed address shouldn't move, but
                // just to make sure the code will panic if the previous `raw_ptr` is no longer the
                // correct address
                debug_assert_eq!(
                    raw_ptr,
                    NonNull::new(Box::into_raw(handle))
                        .unwrap()
                        .cast::<std::ffi::c_void>()
                );

                NonNull::new(Box::into_raw(clone))
                    .unwrap()
                    .cast::<std::ffi::c_void>()
            },
            dropper: &|raw_ptr| {
                // Re-constitute the handle just to drop it
                let handle: Box<T> = unsafe { Box::from_raw(raw_ptr.cast::<T>().as_ptr()) };
                drop(handle)
            },
        }
    }
}

impl Clone for AnonymousHandle {
    fn clone(&self) -> Self {
        AnonymousHandle {
            raw_ptr: (self.cloner)(self.raw_ptr),
            cloner: self.cloner,
            dropper: self.dropper,
        }
    }
}

impl Drop for AnonymousHandle {
    fn drop(&mut self) {
        (self.dropper)(self.raw_ptr)
    }
}

/// We know the handle is `Send` because it's an `ArcHandle` and all of the RocksDB types have no
/// thread affinity.
unsafe impl Send for AnonymousHandle {}

/// Technically it's possible the rocks class this handle wraps is not `Sync`, but this handle
/// doesn't allow the owner to interact with the underlying pointer at all.  The only operations
/// are `clone()` (which operates on `Arc` not on the Rocks object) and `drop`, which also operates
/// on `Arc` and only calls the destroy function for the Rocks object exactly once, so this is also
/// safe.
unsafe impl Sync for AnonymousHandle {}

#[cfg(test)]
mod test {
    use super::*;
    use std::clone::Clone;

    // Implements the `RocksClass` trait with a `delete` function that doesn't free memory as a
    // real impl would, but simply increments the integer the pointer points to.  That lets us
    // test the behavior of the rest of the handle code without actually doing anything unsafe
    rocks_class!(UsizeHandle, usize, airquote_delete_airquote, @sync, @send);

    struct TestStruct(NonNull<usize>);

    impl RocksObject<usize> for TestStruct {
        fn rocks_ptr(&self) -> NonNull<usize> {
            self.0
        }
    }

    static DEFAULT_USIZE: usize = 0;

    fn default_usize_ptr() -> *mut usize {
        let ptr: *const usize = &DEFAULT_USIZE;
        ptr as *mut usize
    }

    impl Default for TestStruct {
        fn default() -> Self {
            // Note that `DEFAULT_USIZE` is static, which means it's mapped into a read-only memory
            // page, so any attempt to modify it (like our faked out `airquote_delete_airquote`
            // function does) will crash the process.  This ensures we will know if the delete
            // operation happens at the wrong time
            TestStruct(NonNull::new(default_usize_ptr()).unwrap())
        }
    }

    unsafe fn airquote_delete_airquote(ptr: *mut usize) {
        *ptr += 1;
    }

    #[test]
    fn deletes_on_drop() {
        let mut delete_count = 0usize;

        let ptr = NonNull::new(&mut delete_count).unwrap();

        let handle: UsizeHandle = ArcHandle::from(ptr);

        assert_eq!(delete_count, 0);

        drop(handle);

        assert_eq!(delete_count, 1);
    }

    #[test]
    fn deletes_only_once() {
        let mut delete_count = 0usize;

        let ptr = NonNull::new(&mut delete_count).unwrap();
        let handle: UsizeHandle = ArcHandle::from(ptr);

        assert_eq!(delete_count, 0);

        {
            let _handle2 = handle.clone();
            let _handle3 = handle.clone();
        }

        assert_eq!(delete_count, 0);
        drop(handle);
        assert_eq!(delete_count, 1);
    }

    #[test]
    fn anon_handle_decrements_reference() {
        let mut delete_count = 0usize;

        let ptr = NonNull::new(&mut delete_count).unwrap();
        let handle: UsizeHandle = ArcHandle::from(ptr);

        assert_eq!(delete_count, 0);

        {
            let _handle2 = AnonymousHandle::wrap_handle(handle.clone());
            assert_eq!(delete_count, 0);
        }
        assert_eq!(delete_count, 0);

        let handle3 = AnonymousHandle::wrap_handle(handle.clone());
        assert_eq!(delete_count, 0);
        drop(handle);
        assert_eq!(delete_count, 0);
        drop(handle3);
        assert_eq!(delete_count, 1);
    }

    #[test]
    fn cowhandle_frees_only_owned() {
        let mut delete_count = 0usize;

        let ptr = NonNull::new(&mut delete_count).unwrap();

        assert_eq!(delete_count, 0);

        {
            let cow1: CowHandle<usize, TestStruct> = CowHandle::Owned(TestStruct::default());

            // There was no handle passed in, so the default should have been used
            assert_eq!(cow1.rocks_ptr().as_ptr(), default_usize_ptr());

            // the defalt test struct will have been freed here.  Unfortunately we cant' confirm
            // this because the static usize pointer is read-only so we're unable to properly
            // simulate a drop.  I'm open to ideas for how to test this case better
        }

        {
            let handle: UsizeHandle = ArcHandle::from(ptr);
            let test_struct = TestStruct(*handle);
            let cow2: CowHandle<usize, TestStruct> = CowHandle::Borrowed(test_struct.rocks_ptr());

            // In this case a value was provided, so that value should be used
            assert_eq!(test_struct.rocks_ptr(), cow2.rocks_ptr());

            // When the cow is dropped this time, nothing is freed, because the test struct itself
            // is responsible for freeing the handle when it drops
            assert_eq!(delete_count, 0);
        }

        // Now the test struct has gone out of scope, so the delete should have happend
        assert_eq!(delete_count, 1);
    }
}
