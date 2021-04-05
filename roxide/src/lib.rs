// Needed to support the cpp! macro
#![recursion_limit = "512"]
#![allow(clippy::cognitive_complexity)]

use std::path::Path;
use tempfile::TempDir;

mod db;
mod db_options;
mod error;
#[macro_use]
mod ffi_util;
#[macro_use]
mod handle;
mod checkpoint;
mod events;
mod iterator;
mod labels;
mod logging;
mod mem;
mod merge;
mod metrics;
mod ops;
mod perf;
mod stats;
mod status;
mod tx;
mod write_batch;

// Let the rest of the code use `crate::ffi` as an alias for the bindgen FFI from the `-sys` crate
pub use librocksdb_sys as ffi;

// As a convenience and to improve readability, re-export the `elasyncio` type
// `BlockingOpFuture`, with the error type defaulting to this crate's own error enum
pub mod future {
    pub type BlockingOpFuture<T, E = crate::error::Error> =
        elasyncio::future::BlockingOpFuture<T, E>;
}

// Export the types we want to share from each module here at the root of the crate
use cheburashka::logging::prelude::*;
pub use checkpoint::Checkpoint;
pub use checkpoint::*;
pub use db::*;
pub use db_options::*;
pub use error::{Error, Result, TransactionRetryError};
pub use events::*;
pub use handle::RocksObject;
pub use iterator::*;
pub use mem::DbVector;
pub use merge::{MergeFn, MergeOperands, MergeOperatorCallback};
pub use ops::*;
pub use perf::*;
pub use stats::*;
pub use status::*;
pub use tx::*;
pub use write_batch::*;

#[macro_use]
extern crate cpp;

cpp! {{
    #include "src/lib.h"
    #include "src/lib.cpp"
}}

#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

/// Ensures that DB::Destroy is called for this database when TempDBPath is dropped.
pub struct TempDbPath {
    temp_dir: TempDir,
}

impl TempDbPath {
    /// Creates a new temporary database path in the temp directory, and guarantees that this
    /// database will be deleted when this struct goes out of scope
    pub fn new() -> TempDbPath {
        // In this case, having a `default` value would be misleading because we want each temp
        // path to be unique
        #![allow(clippy::new_without_default)]
        let temp_dir = tempfile::tempdir().unwrap();

        TempDbPath { temp_dir }
    }

    pub fn path(&self) -> &Path {
        self.temp_dir.path()
    }
}

impl Drop for TempDbPath {
    fn drop(&mut self) {
        trace!(temp_dir = %self.temp_dir.path().display(),
            "Destroying database in temp directory");
        // It's important not to panic inside `drop`
        let _dontcare = crate::db::Db::destroy(None, &self.temp_dir.path());

        // Some test cases cause other files to be created in a DB dir, not just the database
        // files themselves.  So in addition to the above, explicitly delete the entire
        // directory
        trace!(temp_dir = %self.temp_dir.path().display(),
            "Removing temp directory entirely");
        let _dontcare = std::fs::remove_dir_all(&self.temp_dir.path());
    }
}

// Implement `AsRef<Path>` for references to `TempDBPath`.  It's important that this is only
// supported for references, because when the owned object goes out of scope the database is
// destroyed.  We dont' want tests passing this TempDBPath by value to `DB::open`, which will cause
// very strange behavior.
impl AsRef<Path> for &TempDbPath {
    fn as_ref(&self) -> &Path {
        self.path()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::ops::Put;
    use rand::prelude::*;

    /// Re-export `TempDBPath` from `test`.  All of the test code uses `test::TempDBPath`.
    /// `TempDBPath` is only public and outside of `test` because right now doc tests are not
    /// compiled with `cfg(test)`, so I can't write doctests using `TempDBPath` unless it's not
    /// conditional on the `test` config.  I hope that gets addressed in a future release of Rust.
    pub use super::TempDbPath;

    pub fn random_key() -> Vec<u8> {
        random_data(16)
    }

    pub fn random_value() -> Vec<u8> {
        random_data(256)
    }

    pub fn random_data(len: usize) -> Vec<u8> {
        let mut buffer = vec![0u8; len];

        thread_rng().fill_bytes(buffer.as_mut_slice());

        buffer
    }

    pub fn random_data_deterministic(seed: u64, len: usize) -> Vec<u8> {
        let mut buffer = vec![0u8; len];

        let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
        rng.fill_bytes(buffer.as_mut_slice());

        buffer
    }

    pub fn random_keys(len: usize) -> Vec<Vec<u8>> {
        let mut keys = Vec::with_capacity(len);

        assert_eq!(0, keys.len());
        keys.resize_with(len, random_key);
        assert_eq!(len, keys.len());

        assert_ne!(keys[0], keys[1]);

        keys
    }

    pub fn random_keys_deterministic(seed: u64, len: usize) -> Vec<Vec<u8>> {
        let mut keys = Vec::with_capacity(len);

        assert_eq!(0, keys.len());
        for idx in 0..len {
            keys.push(random_data_deterministic(seed + idx as u64, 16))
        }
        assert_eq!(len, keys.len());

        assert_ne!(keys[0], keys[1]);

        keys
    }

    pub fn fill_db<DB: Put, CF: db::ColumnFamilyLike>(
        db: &DB,
        cf: &CF,
        count: usize,
    ) -> Result<()> {
        for _ in 0..count {
            db.put(cf, &random_key(), &random_value(), None)?;
        }

        Ok(())
    }

    /// Fill the database with random values but deterministic keys.  If run more than once,
    /// multiple values will have been written for the same keys, which will need to be merged
    /// during compaction
    pub fn fill_db_deterministic<DB: Put, CF: db::ColumnFamilyLike>(
        db: &DB,
        cf: &CF,
        seed: u64,
        count: usize,
    ) -> Result<()> {
        for key in random_keys_deterministic(seed, count) {
            let value = random_value();
            db.put(cf, key, value, None)?;
        }

        Ok(())
    }

    #[test]
    fn db_path_nothing_there() {
        let _path = TempDbPath::new();
    }
}
