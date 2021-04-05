//! Exposes the RocksDB `Statistics` struct to Rust code.
//!
//! Unlike the `perf` module which implements thread-local performance profiling of specific
//! database operations, `stats` are global to the database and span the entire life of the `DB`
//! object.  These are useful to expose as service-level metrics to monitor the overall performance
//! of the system.

use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

pub type TickerId = u32;
pub type TickerCount = u64;
pub type HistogramId = u32;

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct HistogramData {
    pub id: HistogramId,
    pub name: &'static str,
    pub median: f64,
    pub percentile95: f64,
    pub percentile99: f64,
    pub average: f64,
    pub standard_deviation: f64,
    pub max: f64,
    pub min: f64,
    pub sum: u64,
    pub count: u64,
}

impl Default for HistogramData {
    fn default() -> Self {
        HistogramData {
            id: 0,
            name: "",
            median: 0.0,
            percentile95: 0.0,
            percentile99: 0.0,
            average: 0.0,
            standard_deviation: 0.0,
            max: 0.0,
            min: 0.0,
            sum: 0,
            count: 0,
        }
    }
}

impl fmt::Display for HistogramData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,
            "{}: min={min} median={median} average={average} p95={p95} p99={p99} max={max} count={count}",
            self.name,
            min = self.min,
            median = self.median,
            average = self.average,
            p95 = self.percentile95,
            p99 = self.percentile99,
            max = self.max,
            count = self.count)
    }
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct TickerData {
    pub id: TickerId,
    pub name: &'static str,
    pub count: TickerCount,
}

impl fmt::Display for TickerData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.name, self.count)
    }
}

/// Cached mapping of ticker names and IDs.  This comes from RocksDB native code so it's a bit
/// expensive to produce
static TICKERS: OnceCell<HashMap<TickerId, String>> = OnceCell::new();
static HISTOGRAMS: OnceCell<HashMap<HistogramId, String>> = OnceCell::new();

/// Gets the map of ticker indexes to names.  This is cached statically so it is cheap to call
/// after the first time, and the result has a static lifetime
pub fn tickers_map() -> &'static HashMap<TickerId, String> {
    TICKERS.get_or_init(|| {
        // Dive deep into the C++ code to construct a mapping of ticker IDs and names
        // extern const std::vector<std::pair<Tickers, std::string>> TickersNameMap;

        let mut map = HashMap::new();
        unsafe {
            let map_ptr: *mut HashMap<_, _> = &mut map;

            cpp!([map_ptr as "void*"] {
                for (auto& pair : rocksdb::TickersNameMap) {
                    uint32_t id = pair.first;
                    const char* name = pair.second.c_str();

                    rust!(Stats_add_ticker [map_ptr : *mut HashMap<TickerId, String> as "void*", id: u32 as "uint32_t", name: *const libc::c_char as "const char*"] {
                        let name = std::ffi::CStr::from_ptr(name) .to_string_lossy() .into_owned();

                        (*map_ptr).insert(id, name);
                    });
                }
            });
        };

        map
    })
}

/// Gets the map of all available histogram IDs to their names
pub fn histograms_map() -> &'static HashMap<HistogramId, String> {
    HISTOGRAMS.get_or_init(|| {
        // extern const std::vector<std::pair<Histograms, std::string>> HistogramsNameMap;

        let mut map = HashMap::new();
        unsafe {
            let map_ptr: *mut HashMap<_, _> = &mut map;

            cpp!([map_ptr as "void*"] {
                for (auto& pair : rocksdb::HistogramsNameMap) {
                    uint32_t id = pair.first;
                    const char* name = pair.second.c_str();

                    rust!(Stats_add_histogram [map_ptr : *mut HashMap<HistogramId, String> as "void*", id: u32 as "uint32_t", name: *const libc::c_char as "const char*"] {
                        let name = std::ffi::CStr::from_ptr(name) .to_string_lossy() .into_owned();

                        (*map_ptr).insert(id, name);
                    });
                }
            });
        };

        map
    })
}

/// Gets the values of the tickers from a C++ `Statistics` object and puts them into a Rust vector
fn get_tickers(map: &mut Vec<TickerData>, stats_ptr: *mut libc::c_void) {
    for (id, name) in tickers_map().iter() {
        let id = *id;
        let count: TickerCount = unsafe {
            cpp!([stats_ptr as "rocksdb::Statistics*", id as "uint32_t"] -> TickerCount as "uint64_t" {
                return stats_ptr->getTickerCount(id);
            })
        };

        let data = TickerData { id, name, count };

        map.push(data);
    }
}

/// Get the histogram stats from the Rocksdb `Statistics` object into a Rust form.
#[allow(clippy::cognitive_complexity)] // Ths complexity is not that bad it's just a lot of assignments
fn get_histograms(map: &mut Vec<HistogramData>, stats_ptr: *mut libc::c_void) {
    for (id, name) in histograms_map().iter() {
        let id = *id;
        let enabled = unsafe {
            cpp!([stats_ptr as "rocksdb::Statistics*", id as "uint32_t"] -> bool as "bool" {
                return stats_ptr->HistEnabledForType(id);
            })
        };

        if enabled {
            let mut data = HistogramData {
                id,
                name,
                ..Default::default()
            };

            unsafe {
                let median: *mut f64 = &mut data.median;
                let percentile95: *mut f64 = &mut data.percentile95;
                let percentile99: *mut f64 = &mut data.percentile99;
                let average: *mut f64 = &mut data.average;
                let standard_deviation: *mut f64 = &mut data.standard_deviation;
                let max: *mut f64 = &mut data.max;
                let min: *mut f64 = &mut data.min;
                let sum: *mut u64 = &mut data.sum;
                let count: *mut u64 = &mut data.count;

                cpp!([stats_ptr as "rocksdb::Statistics*",
                    id as "uint32_t",
                    median as "double*",
                    percentile95 as "double*",
                    percentile99 as "double*",
                    average as "double*",
                    standard_deviation as "double*",
                    max as "double*",
                    min as "double*",
                    sum as "uint64_t*",
                    count as "uint64_t*"
                ] {
                    // Zero-initialize the struct
                    rocksdb::HistogramData data = rocksdb::HistogramData();
                    stats_ptr->histogramData(id, &data);

                    *median = data.median;
                    *percentile95 = data.percentile95;
                    *percentile99 = data.percentile99;
                    *average = data.average;
                    *standard_deviation = data.standard_deviation;
                    *max = data.max;

                    // In the `histogram.cc` source file, we can see that if a histogram has no
                    // data, `max` is initialized to 0 (that starting point of the first bucket),
                    // and `min` is initialized to a very large number (the last bucket).  On my
                    // system this value is always 13000000000000000000.  This ensures that
                    // whatever value is added will be smaller and thus `min` will be updated with
                    // that value.
                    //
                    // That's not at all obvious or convenient, so this code detects then there are
                    // no values in the histogram, and sets min to zero.
                    if (data.count == 0) {
                        *min = 0;
                    } else {
                        *min = data.min;
                    }

                    *sum = data.sum;
                    *count = data.count;
                });
            }

            map.push(data);
        }
    }
}

/// Statistics exposed at the database level when `DBOptions::set_stats_level` was called with a
/// level higher than `Disabled`.
#[derive(PartialEq, Clone, Debug)]
pub struct DbStatistics {
    pub counters: Vec<TickerData>,
    pub histograms: Vec<HistogramData>,
}

impl DbStatistics {
    /// Creates a statistics object populated with information in the `rocksdb::Statistics` struct
    /// at the given pointer address.
    pub(crate) unsafe fn from_rocks_stats_ptr(stats_ptr: *mut libc::c_void) -> Self {
        let mut counters = Vec::<TickerData>::with_capacity(tickers_map().len());
        get_tickers(&mut counters, stats_ptr);
        counters.sort_by_key(|counter| counter.name);

        let mut histograms = Vec::<HistogramData>::with_capacity(histograms_map().len());
        get_histograms(&mut histograms, stats_ptr);
        histograms.sort_by_key(|counter| counter.name);

        DbStatistics {
            counters,
            histograms,
        }
    }
}

impl fmt::Display for DbStatistics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Counters:")?;

        for counter in &self.counters {
            writeln!(f, "  {}", counter)?;
        }
        writeln!(f, "Histograms:")?;

        for histogram in &self.histograms {
            writeln!(f, "  {}", histogram)?;
        }

        Ok(())
    }
}

// TODO: Look into GetIntProperty as source of some additional interesting stats
