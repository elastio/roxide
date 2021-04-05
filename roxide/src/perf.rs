//! This module provides a Rust interface to the RocksDB performance context.
//!
//! https://github.com/facebook/rocksdb/wiki/Perf-Context-and-IO-Stats-Context
//!
//! This is a thread-local data structure which can be configured to capture various performance
//! counters specifically related to operations on that thread.  This allows higher level code to
//! instrument specific database operations or sequences, providing detailed insight into the
//! performance and behavior of just those operations in isolation.
//!
//! This is separate and distinct from the database-wide statistics in the `stats` module.

// In the C++ source file which the cpp macro will generate make sure the relevant includes are
// present
cpp! {{
    #include "src/perf.h"
    #include "src/perf.cpp"
}}

use std::collections::HashMap;

/// Possible levels of performance tracking.  Higher levels capture more detail but also impose
/// higher overhead.
///
/// Profile levels are described at
/// https://github.com/facebook/rocksdb/wiki/Perf-Context-and-IO-Stats-Context
///
/// The values for these levels are taken from the RocksDB source code
/// `includes/rocksdb/perf_level.h`.
#[allow(dead_code)] // We don't use all of these levels but it would be misleading to remove some; they're all valid in the C code
enum RocksDbPerfLevel {
    /// unknown setting
    Uninitialized = 0,

    /// disable perf stats
    Disable = 1,

    /// enable only count stats
    EnableCount = 2,

    /// Other than count stats, also enable time
    /// stats except for mutexes
    EnableTimeExceptForMutex = 3,

    /// Other than time, also measure CPU time counters. Still don't measure
    /// time (neither wall time nor CPU time) for mutexes.
    EnableTimeAndCpuTimeExceptForMutex = 4,

    /// enable count and time stats
    EnableTime = 5,

    /// N.B. Must always be the last value!
    OutOfBounds = 6,
}

/// This library doesn't expose the fine-grained performance profiling options that Rocks uses
/// internally.  Instead there are the following options
///
/// * `Minimal`
/// * `Full`
///
/// `Minimal` doesn't impose much of an overhead and can be used at all times.  `Full` will impose
/// considerable overhead and should only be used when sampling or when tracing a specific request.
#[derive(Copy, Clone, PartialEq, Debug)]
pub enum PerfLevel {
    Minimal,

    Full,
}

pub struct PerfContext {
    level: PerfLevel,
}

impl PerfContext {
    pub fn new(level: PerfLevel) -> PerfContext {
        set_perf_level(level);

        PerfContext { level }
    }

    pub fn collect_stats(self) -> PerfStats {
        // In the `perf.cpp` source code there's a helper function get_stats() that gets all of the
        // performance and IO context stats into a C++ STL `unordered_map`.  All we have to do is
        // re-package it as a Rust `HashMap`.  How hard can it be?
        let mut stats_map = HashMap::<String, u64>::with_capacity(128); // Probably not literally 128 stats but close enough

        unsafe {
            let stats_map_ptr: *mut HashMap<_, _> = &mut stats_map;

            cpp!([stats_map_ptr as "void*"] -> () as "void" {
                auto stats = get_stats();

                for (auto kv : stats) {
                    const char* stat_name = kv.first.c_str();
                    uint64_t stat_value = kv.second;

                    rust!(PERF_add_to_map [stats_map_ptr: *mut HashMap<String, u64> as "void*", stat_name: *const libc::c_char as "const char*", stat_value: u64 as "uint64_t"] {
                        unsafe {
                            let stat_name = std::ffi::CStr::from_ptr(stat_name).to_string_lossy().into_owned();
                            (*stats_map_ptr).insert(stat_name, stat_value);
                        }
                    });
                }
            });

            // Stats have been captured.  Now reset them and disable perf tracking
            set_rocks_perf_level(RocksDbPerfLevel::Disable, false);
        }

        PerfStats {
            level: self.level,
            stats: stats_map,
        }
    }
}

pub struct PerfStats {
    pub level: PerfLevel,
    pub stats: HashMap<String, u64>,
}

/// Sets the thread-local perf tracking level
fn set_perf_level(level: PerfLevel) {
    let rocks_level = match level {
        PerfLevel::Minimal => RocksDbPerfLevel::EnableCount,
        PerfLevel::Full => RocksDbPerfLevel::EnableTimeAndCpuTimeExceptForMutex,
    };

    unsafe { set_rocks_perf_level(rocks_level, level == PerfLevel::Full) };
}

unsafe fn set_rocks_perf_level(rocks_level: RocksDbPerfLevel, per_level_enabled: bool) {
    cpp!([rocks_level as "rocksdb::PerfLevel", per_level_enabled as "bool"] -> () as "void" {
        rocksdb::SetPerfLevel(rocks_level);
        if (rocksdb::get_perf_context()) {
            if (per_level_enabled) {
                rocksdb::get_perf_context()->EnablePerLevelPerfContext();
            } else {
                rocksdb::get_perf_context()->DisablePerLevelPerfContext();
            }

            rocksdb::get_perf_context()->Reset();
        }

        if (rocksdb::get_iostats_context()) rocksdb::get_iostats_context()->Reset();
    });
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::{Db, DbLike};
    use crate::ops::{Compact, DbOpen, Get};
    use crate::test::TempDbPath;
    use crate::Result;
    use more_asserts::*;

    #[test]
    fn perf_stats_on_put() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        // Generate several test records which will be compacted
        crate::test::fill_db_deterministic(&db, &cf, 42, 1_000)?;

        // Trigger a compaction so the data we are about to read will come from an SST not a
        // memtable
        db.compact_all(&cf, None)?;

        // Enable minimal performance logging on this thread
        let context = PerfContext::new(PerfLevel::Minimal);
        assert!(is_rocks_perf_enabled());

        // Generate some GET operations
        for key in crate::test::random_keys_deterministic(42, 1_000) {
            db.get(&cf, key, None)?.unwrap();
        }

        // Consume the context and get the perf stats
        // This should have re-set the rocks perf context to disabled
        let stats = context.collect_stats();
        assert_eq!(false, is_rocks_perf_enabled());

        // We did a couple of GET operations so there should be some non-zero stats
        assert_gt!(stats.stats["get_read_bytes"], 0);

        // Do a few more reads this time with more intrusive metrics
        let context = PerfContext::new(PerfLevel::Full);
        assert!(is_rocks_perf_enabled());
        for key in crate::test::random_keys_deterministic(42, 1_000) {
            db.get(&cf, key, None)?.unwrap();
        }
        let stats = context.collect_stats();

        println!("Stats: {:#?}", stats.stats);

        // There should be some additional non-zero metrics including timing-related metrics
        assert_gt!(stats.stats["get_read_bytes"], 0);

        // When `Full` is enabled, additional per-level metrics are included
        assert_gt!(stats.stats["block_cache_hit_count_level0"], 0);

        Ok(())
    }

    /// Internally Rocks is supposed to use thread-local storage for the performance context, so
    /// whatever the perf context is on other threads shouldn't matter.
    ///
    /// Due to a bug, thread local storage can be disable in the RocksDB build config.  That would
    /// be a problem.
    #[test]
    fn perf_stats_thread_locality() -> Result<()> {
        let path = TempDbPath::new();
        let db = Db::open(&path, None)?;
        let cf = db.get_cf("default").unwrap();

        let threads = (0u64..num_cpus::get() as u64 * 2)
            .map(|n| {
                let db = db.clone();
                let cf = cf.clone();
                std::thread::Builder::new()
                    .name(format!("perf-stats-thread-{}", n))
                    .spawn(move || {
                        // Generate several test records which will be compacted
                        crate::test::fill_db_deterministic(&db, &cf, 42 + n, 1_000)?;

                        // Trigger a compaction so the data we are about to read will come from an SST not a
                        // memtable
                        db.compact_all(&cf, None)?;

                        let context = PerfContext::new(PerfLevel::Minimal);
                        assert!(is_rocks_perf_enabled());
                        // Enable minimal performance logging on this thread
                        // Generate some GET operations
                        for key in crate::test::random_keys_deterministic(42 + n, 1_000) {
                            db.get(&cf, key, None)?.unwrap();
                        }

                        // Consume the context and get the perf stats
                        // This should have re-set the rocks perf context to disabled
                        let stats = context.collect_stats();
                        assert_eq!(false, is_rocks_perf_enabled());

                        // We did a couple of GET operations so there should be some non-zero stats
                        assert_gt!(stats.stats["get_read_bytes"], 0);

                        // Do a few more reads this time with more intrusive metrics
                        let context = PerfContext::new(PerfLevel::Full);
                        assert!(is_rocks_perf_enabled());
                        for key in crate::test::random_keys_deterministic(42 + n, 1_000) {
                            db.get(&cf, key, None)?.unwrap();
                        }
                        let stats = context.collect_stats();

                        // There should be some additional non-zero metrics including timing-related metrics
                        assert_gt!(stats.stats["get_read_bytes"], 0);

                        // When `Full` is enabled, additional per-level metrics are included.
                        //
                        // Unlike inthe single-threaded test that just assumes a `level0` metric is
                        // preent, this test generates enough IO that it will trigger compaction
                        // across levels.  In that case we can't predict which level(s) will be
                        // occupied and which ones won't because of universal compaction.  All we
                        // can say for sure is that there should be stats for at least one level.
                        let hit_count_stats = stats.stats.iter().find(|(key, value)| {
                            key.starts_with("block_cache_hit_count_level") && **value > 0
                        });
                        assert_ne!(None, hit_count_stats);

                        Result::<_>::Ok(())
                    })
                    .unwrap()
            })
            .collect::<Vec<_>>();

        for thread in threads {
            thread.join().unwrap()?;
        }

        Ok(())
    }

    fn is_rocks_perf_enabled() -> bool {
        unsafe {
            cpp!([] -> bool as "bool" {
                auto level = rocksdb::GetPerfLevel();
                return level != rocksdb::PerfLevel::kDisable &&
                    level != rocksdb::PerfLevel::kUninitialized;
            })
        }
    }
}
