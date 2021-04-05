//! RocksDB internally has a `DB` abstract base class, with a few concrete implementions.  Some of
//! these are not public, and hide behind the `DB` interface, but some, like `TransactionDb`,
//!
//! `OptimisticTransactionDb`, and `ReadonlyDB`, are exposed explicitly.
//!
//! The C FFI makes a version of this available as well.  It's awkward to adapt this to Rust
//! because it doesn't have the same kind of polymorphism idioms, at least not in idiomatic Rust.
//!
//! So instead we break up the API into operations.  For example, the two transactional types of
//! database support `Begin/Commit`, all types support `Get`, the non-readonly types support `Put`,
//! etc.
//!
//! Each type of operation is in its own module in the `ops` module.  This module is concerned only
//! with the wrappers for the specific database types.
//!
//! # Notes on Ownership
//!
//! The RocksDB API is C++, and makes extensive use of `std::shared_ptr` to maintain reference
//! counted pointers between various objects.  It's very difficult to fit this into the Rust
//! ownership model.  So we don't try; see the `handle` module for more on how we wrap what are
//! effectively C++ pointers into an `Arc<T>` so we can clone them and pass them around.
//!
//! In this module you'll see another level of indirection, also using `Arc<T>`.  A
//! `ColumnFamilyLike` instance is entirely dependent upon, and can't live longer than, the
//! corresponding `DBLike` implementation it came from.  Rather than try to contort ourselves with
//! lifetime parameters to make this relationship explicit, we've taken the easy way out: each
//! `ColumnFamilyLike` implementation clones the `Arc<DbLikeCore>` that contains the DB it came
//! from.  Therefore, even if the last remnant of a database in scope is one of that database's
//! column family objects, the entire database is still in memory and active.  Only when the last
//! DB or CF instance goes out of scope is the database itself freed.
//!
//! This second level of indiration is not required to enforce the RocksDB lifetime constraints.
//! Instead it's an optimization to allow the `DBLike` impl to cache some metadata about itself,
//! and have the `ColumnFamilyLike` implementations be able to easily call back into `DBLike` and
//! get that info.  To do that `ColumnFamilyLike` has to hold some kind of reference to the DB, and
//! that reference is implemented as an `Arc`.
#![allow(clippy::rc_buffer)] // Using Arc<String> in an immutable way to save heap allocs, it's a legitimate strategy
use crate::db_options::prelude::*;
use crate::error::prelude::*;
use crate::ffi;
use crate::ffi_try;
use crate::ffi_util::path_to_cstring;
use crate::handle;
use crate::handle::RocksObject;
use crate::ops::WithLogger;
use crate::rocks_class;
use std::sync::Arc;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::ptr;

cpp! {{
    #include "src/lib.h"
}}

/// The base trait for all database types.  Each database type is different enough that it has its
/// own implementation, but wherever possible we code against the traits to keep things generic.
pub trait DbLike:
    Clone + Send + Sync + crate::error::ErrorPostprocessor + crate::ops::DbOps + 'static
{
    /// Each database exposes its own type representing its column families.
    type ColumnFamilyType: ColumnFamilyLike;

    /// Gets the path of this database
    fn path(&self) -> &Path;

    /// Gets the path of this database, as a Rust string slice.
    ///
    /// Yes that seems pointless and duplicative, but our metrics code need to be able to get this
    /// as a Rust string very quickly, so it needs to be cached at this level
    fn path_str(&self) -> &str;

    /// Gets the unique ID of this database, which never changes once a database is created.
    fn db_id(&self) -> &str;

    /// Returns a `ColumnFamilyLike` implementation for the given named column family.  Note that
    /// this implementation will increment the ref count on the database handle, so as long as this
    /// column family instance is in scope, the database will remain opened, even if the database
    /// object itself goes out of scope or is explicitly dropped
    fn get_cf<T: AsRef<str>>(&self, name: T) -> Option<Self::ColumnFamilyType>;

    /// Gets the names of the column families in this database
    fn get_cf_names(&self) -> Vec<&str>;

    /// Updates all metrics related to this database.
    ///
    /// Note that some metrics are updated contiuously, for example counters of the number of
    /// operations, size of keys, etc.  However there are other metrics that are computed only on
    /// demand, which come from the RocksDB `Statistics` and `Properties` APIs.  These metrics are
    /// updated here.
    ///
    /// This should be called within the `/metrics` endpoint that serves metrics to Prometheus.
    /// Right after this call completes successfully, use `cheburashka::metrics::write_all_metrics`
    /// to write the metrics to whatever format is required by the caller.
    fn update_metrics(&self) -> Result<()>
    where
        Self: crate::ops::Stats;

    /// Returns an opaque handle to the database which the caller cannot interpret or do anything
    /// with, except `clone()`.  The caller is guaranteed that the underlying Rocks database object
    /// will not be destroyed as long as any clones of this handle are in scope.
    fn db_handle(&self) -> handle::AnonymousHandle;
}

/// A column family (really bad name by the way) is conceptually like a table in relational
/// databases.  This struct also holds a handle to the database the column family came from to
/// ensure the database will outlive the column family.  This approach is not idiomatic Rust but
/// when dealing with the C-style lifetime approach of the Rocks API it makes things easier on
/// developers
///
/// Callers of this library should use this trait, though there are concrete implementations for
/// each database type
pub trait ColumnFamilyLike:
    Clone
    + Sync
    + Send
    + crate::error::ErrorPostprocessor
    + RocksObject<ffi::rocksdb_column_family_handle_t>
    + 'static
{
    /// Gets the path of this column family's database
    fn db_path(&self) -> &Path;

    /// Gets the path of this column family's database as a Rust string slice
    fn db_path_str(&self) -> &str;

    /// Gets the unique ID of this column family's database, which never changes once a database is created.
    fn db_id(&self) -> &str;

    /// Gets the name of this column family
    ///
    /// This needs to be an `Arc<String>` instead of a `str` because the CF name is used as a label
    /// for certain metrics and error logging in a way that would be too complicated to work with
    /// if it were a `&str`.  This way an owned clone of the CF name is cheap to create and
    /// simplifies the code that uses it.
    fn name(&self) -> &Arc<String>;

    /// Returns the raw pointer to the RocksDB C++ class `rocksdb::ColumnFamilyHandle`.
    ///
    /// # Safety
    ///
    /// This is intended only for use by the internal implementation and should never be used by
    /// downstream crates.
    unsafe fn cf_ptr(&self) -> *mut libc::c_void {
        let cf_ptr = self.rocks_ptr().as_ptr();

        cpp!([cf_ptr as "::rocksdb_column_family_handle_t*"] -> *mut libc::c_void as "rocksdb::ColumnFamilyHandle*" {
            return cast_to_cf(cf_ptr);
        })
    }
}

/// Each database struct generated by `rocks_db_impl!` is a lightweight, small, cheaply clonable
/// wrapper around an `Arc<>` which contains this struct.  This contains all of the information
/// about a database that matters.
struct DbLikeCore<HT> {
    path: PathBuf,

    path_str: String,

    db_id: String,

    // Struct members are dropped in the order they are declared.  Thus the column family
    // handles will drop first
    cfs: HashMap<String, ColumnFamilyHandle>,

    // The database handle drops last
    handle: HT,
}

/// Contains all the information on a column family.  Just like `DbLikeCore`, there is only one
/// instance of this struct per column family; the actual `ColumnFamilyLike` implementation just
/// holds an `Arc<>` so it's very cheap to clone and move around.
struct ColumnFamilyLikeCore<DbType: DbLike> {
    // Rust drops members of a struct in order of declaration, so this indicates we
    // want the column family handle to be dropped before the database handle.  On the off
    // chance that the only remaining handle to the database is this one, we don't want the
    // database to be dropped before the column family handle that depends on it
    handle: ColumnFamilyHandle,

    /// All column family instances hold a reference to the DB they belong to, ensuring
    /// that DB will always be alive for as long as the CF is in scope somewhere
    db: DbType,

    name: Arc<String>,
}

/// Called immediately after a database is opened, before it's returned to the caller.
///
/// Sets up the logger for the database with the context information describing the database
fn setup_database_loggers(db: &impl DbLike) {
    let db_labels = db.into();

    db.with_logger(|logger| {
        if let Some(logger) = logger {
            logger.set_context(db_labels);
        }
    })
}

/// Generates a `DBLike` implementation and supporting code for each possible database type
macro_rules! rocks_db_impl {
    ($name:ident, $cf_name:ident, $handle_type:ty, $handle_rocks_class:ty) => {
        #[derive(Clone)]
        pub struct $name {
            inner: Arc<DbLikeCore<$handle_type>>,
        }

        impl $name {
            pub(crate) fn new(
                path: impl AsRef<Path>,
                handle: $handle_type,
                cfs: HashMap<String, ColumnFamilyHandle>,
            ) -> Self {
                // Cache the database ID for future use
                let db_id = unsafe {
                    let db_ptr = <$name as $crate::ops::GetDbPtr>::raw_get_db_ptr(&handle);

                    <$name as $crate::ops::GetDbId>::raw_get_db_id(db_ptr)
                }
                .unwrap_or_else(|| "NULL_ID".to_owned());

                let path = path.as_ref().to_owned();
                let path_str = path.to_string_lossy().into_owned();
                let db = $name {
                    inner: Arc::new(DbLikeCore {
                        path,
                        path_str,
                        db_id,
                        handle,
                        cfs,
                    }),
                };

                // Configure the logger with the database logging context
                $crate::db::setup_database_loggers(&db);

                db
            }
        }

        impl DbLike for $name {
            type ColumnFamilyType = $cf_name;

            fn path(&self) -> &Path {
                &self.inner.path
            }

            fn path_str(&self) -> &str {
                &self.inner.path_str
            }

            fn db_id(&self) -> &str {
                &self.inner.db_id
            }

            fn get_cf<T: AsRef<str>>(&self, name: T) -> Option<Self::ColumnFamilyType> {
                self.inner.cfs.get(name.as_ref()).map(|cf_handle| {
                    $cf_name::new(self.clone(), cf_handle.clone(), name.as_ref().to_owned())
                })
            }

            fn get_cf_names(&self) -> Vec<&str> {
                self.inner.cfs.keys().map(|name| name.as_ref()).collect()
            }

            fn update_metrics(&self) -> Result<()>
            where
                Self: $crate::ops::Stats,
            {
                $crate::metrics::update_rocks_metrics(self)
            }

            fn db_handle(&self) -> $crate::handle::AnonymousHandle {
                $crate::handle::AnonymousHandle::wrap_handle(self.inner.handle.clone())
            }
        }

        impl $crate::ops::RocksOpBase for $name {
            type HandleType = $handle_type;

            fn handle(&self) -> &Self::HandleType {
                &self.inner.handle
            }
        }

        impl $crate::handle::RocksObject<$handle_rocks_class> for $name {
            fn rocks_ptr(&self) -> ::std::ptr::NonNull<$handle_rocks_class> {
                *self.inner.handle
            }
        }

        #[derive(Clone)]
        pub struct $cf_name {
            inner: Arc<ColumnFamilyLikeCore<$name>>,
        }

        impl $cf_name {
            fn new(db: $name, cf_handle: ColumnFamilyHandle, name: String) -> Self {
                $cf_name {
                    inner: Arc::new(ColumnFamilyLikeCore {
                        handle: cf_handle,
                        db,
                        name: Arc::new(name),
                    }),
                }
            }
        }

        impl $crate::handle::RocksObject<ffi::rocksdb_column_family_handle_t> for $cf_name {
            fn rocks_ptr(&self) -> ::std::ptr::NonNull<ffi::rocksdb_column_family_handle_t> {
                *self.inner.handle
            }
        }

        impl ColumnFamilyLike for $cf_name {
            fn db_path(&self) -> &Path {
                &self.inner.db.path()
            }

            fn db_path_str(&self) -> &str {
                &self.inner.db.path_str()
            }

            fn db_id(&self) -> &str {
                &self.inner.db.db_id()
            }

            fn name(&self) -> &Arc<String> {
                &self.inner.name
            }
        }

        // Implement `ErrorPostprocessor` by delegating to the database itself
        impl crate::error::ErrorPostprocessor for $cf_name {
            fn postprocess_error(&self, err: Error) -> Error {
                self.inner.db.postprocess_error(err)
            }
        }
    };
}

rocks_class!(ColumnFamilyHandle, ffi::rocksdb_column_family_handle_t, ffi::rocksdb_column_family_handle_destroy, @send, @sync);

// This looks like "module inception" but Rocks has three kinds of databases: DB,
// OptimisticTransactionDb, and TransactionDb.  There's one sub-module for each one.  They are all
// together in the parent module 'db' because that's the logical place to put all of the database
// types.
#[allow(clippy::module_inception)]
pub(crate) mod db;
pub(crate) mod opt_txdb;
pub(crate) mod txdb;

// Re-rexport the various db types
pub use self::{db::*, opt_txdb::*, txdb::*};
