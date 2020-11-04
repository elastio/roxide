//! This module provides access to the RocksDB instance and CF statistics.  The root-level `stats`
//! module implements most of the logic that translates from RocksDB to Rust structs; this module
//! provides the bridge to get a `rocksdb::Statistics*` for a given database instance.

use super::*;
use crate::db::DBLike;
use crate::ops::get_db_ptr::GetDBPtr;
use crate::stats::DBStatistics;
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

cpp! {{
#include <rocksdb/options.h>
#include <rocksdb/statistics.h>

static const rocksdb::StatsLevel STATS_LEVEL_FULL = rocksdb::StatsLevel::kAll;
static const rocksdb::StatsLevel STATS_LEVEL_MINIMAL = rocksdb::StatsLevel::kExceptDetailedTimers;

static std::shared_ptr<rocksdb::Statistics> get_stats_ptr(rocksdb::DB* db) {
    return db->GetDBOptions().statistics;
}

}}

/// Rocks defines several different stat levels but we simplify this into three:
///
/// * `Disabled` is equivalent to a `nullptr` value for the `statistics` object in `Options`
/// * `Minimal` is equivalent to `kExceptDetailedTimers`
/// * `Full` is equivalent to `kAll`
#[derive(PartialEq, Copy, Clone, Debug, Serialize, Deserialize)]
pub enum StatsLevel {
    Disabled,

    Minimal,

    Full,
}

impl Default for StatsLevel {
    fn default() -> Self {
        StatsLevel::Disabled
    }
}

impl StatsLevel {
    /// Gets the RocksDB `StatsLevel` enum value, corresponding to this enum
    pub(crate) fn as_rocks_level(self) -> Option<u8> {
        unsafe {
            if self == StatsLevel::Disabled {
                None
            } else {
                let is_full = self == StatsLevel::Full;
                let level = cpp!([is_full as "bool"] -> u8 as "uint8_t" {
                    if (is_full) {
                        return STATS_LEVEL_FULL;
                    } else {
                        return STATS_LEVEL_MINIMAL;
                    }
                });

                Some(level)
            }
        }
    }
}

pub trait Stats: RocksOpBase {
    fn get_db_stats(&self) -> Option<DBStatistics>;

    fn set_db_stats_level(&mut self, level: StatsLevel) -> Result<()>;
}

/// Internal helper trait which abstracts away the messy business of getting a `Statistics*`
trait GetStatsPtr {
    unsafe fn get_stats_ptr(&self) -> Option<*mut libc::c_void>;
}

impl<DB> GetStatsPtr for DB
where
    DB: GetDBPtr,
{
    unsafe fn get_stats_ptr(&self) -> Option<*mut libc::c_void> {
        let db_ptr = self.get_db_ptr();

        let stats_ptr = cpp!([db_ptr as "rocksdb::DB*"] -> *mut libc::c_void as "rocksdb::Statistics*" {
            auto stats_ptr = get_stats_ptr(db_ptr);

            if (stats_ptr) {
                // Unwrap the `shared_ptr` and produce a raw pointer
                // This is somewhat safe because we know that our `self` owns the `DB` pointer and
                // thus this stats object, the `Objects` object that owns it, and the `DB` that
                // owns the options won't go away during this function call
                return stats_ptr.get();
            } else {
                return nullptr;
            }
        });

        if stats_ptr.is_null() {
            None
        } else {
            Some(stats_ptr)
        }
    }
}

impl<DB> Stats for DB
where
    DB: GetDBPtr + DBLike,
{
    fn get_db_stats(&self) -> Option<DBStatistics> {
        unsafe {
            self.get_stats_ptr()
                .map(|stats_ptr| DBStatistics::from_rocks_stats_ptr(stats_ptr))
        }
    }

    fn set_db_stats_level(&mut self, level: StatsLevel) -> Result<()> {
        unsafe {
            match self.get_stats_ptr() {
                Some(stats_ptr) => {
                    // There is a non-null Statistics object, so set the stat level on it
                    //
                    // NB: There is no way to fully disable stats after the DB has been created.  Thus,
                    // if `level` here is `Disabled` it doesn't matter; the result will be to set the
                    // stats level to minimal.  If you want to disable stats entirely you must do so
                    // when the database is opened.
                    let full_stats = level == StatsLevel::Full;

                    cpp!([stats_ptr as "rocksdb::Statistics*", full_stats as "bool"] {
                        stats_ptr->set_stats_level(full_stats ? STATS_LEVEL_FULL : STATS_LEVEL_MINIMAL);
                    });

                    Ok(())
                }

                None => {
                    // The database has no stats object.  If the desired level is `Disabled` this is
                    // fine, otherwise it's an error
                    if level == StatsLevel::Disabled {
                        Ok(())
                    } else {
                        Err(Error::DBStatsDisabledError)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::{db::*, DBLike};
    use crate::db_options::DBOptions;
    use crate::ops::DBOpen;
    use crate::stats;
    use crate::test::TempDBPath;
    use crate::Result;
    use more_asserts::*;

    #[test]
    fn db_get_stats_enabled() -> Result<()> {
        let path = TempDBPath::new();
        let mut options = DBOptions::default();
        options.set_stats_level(StatsLevel::Full);
        let db = DB::open(&path, options)?;

        let stats = db.get_db_stats().unwrap();

        for counter in stats.counters.iter() {
            println!(" - counter: {:?}", counter);
        }

        for histogram in stats.histograms.iter() {
            println!(" - histogram: {:?}", histogram);
        }

        assert_eq!(
            0,
            stats
                .counters
                .iter()
                .find(|t| t.name == "rocksdb.number.keys.read")
                .unwrap()
                .count
        );

        assert_eq!(
            0,
            stats
                .histograms
                .iter()
                .find(|t| t.name == "rocksdb.bytes.per.multiget")
                .unwrap()
                .count
        );

        Ok(())
    }

    #[test]
    fn stats_sanity_check_histos() -> Result<()> {
        // I'm seeing what looks like invalid data in some of the histograms.
        // This test sanity-checks that data.
        let path = TempDBPath::new();
        let mut options = DBOptions::default();
        options.set_stats_level(StatsLevel::Full);
        let db = DB::open(&path, options)?;
        let cf = db.get_cf("default").unwrap();

        let stats = db.get_db_stats().unwrap();
        for histogram in stats.histograms {
            sanity_check_histo(&histogram);
        }

        // Do some work on the database to generate some real data in the histograms
        const RECORD_COUNT: usize = 100_000;
        crate::test::fill_db(&db, &cf, RECORD_COUNT)?;

        let stats = db.get_db_stats().unwrap();
        for histogram in &stats.histograms {
            if histogram.name == "rocksdb.bytes.per.write" {
                // This particular histogram counts the total byte written per put operation.
                // We know each inserted record triggers a put operation so we can make some
                // assumptions
                assert_eq!(histogram.count, RECORD_COUNT as u64);
                assert_gt!(histogram.min, 0f64); // all writes are bigger than 0 bytes
            }

            sanity_check_histo(histogram);
        }

        Ok(())
    }

    fn sanity_check_histo(histogram: &stats::HistogramData) {
        println!("Histo: {}", histogram);

        // Min is always less than or equal to max
        assert_le!(histogram.min, histogram.max, "histogram {}", histogram.name);

        // The various pN values should be less than or equal to the one following
        assert_le!(
            histogram.min,
            histogram.average,
            "histogram {}",
            histogram.name
        );

        assert_le!(
            histogram.median,
            histogram.percentile95,
            "histogram {}",
            histogram.name
        );
        assert_le!(
            histogram.percentile95,
            histogram.percentile99,
            "histogram {}",
            histogram.name
        );
        assert_le!(
            histogram.percentile99,
            histogram.max,
            "histogram {}",
            histogram.name
        );

        // The max value should be less than or equal to the sum of all values
        assert_le!(
            histogram.max,
            histogram.sum as f64,
            "histogram {}",
            histogram.name
        );
    }

    #[test]
    fn db_get_stats_disabled() -> Result<()> {
        let path = TempDBPath::new();
        let options = DBOptions::default();
        let db = DB::open(&path, options)?;

        let stats = db.get_db_stats();

        assert!(stats.is_none());

        Ok(())
    }
}
