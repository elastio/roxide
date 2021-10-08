//! Wrap the RocksDB `LRUCache` in a Rust type allowing callers to configure block caching at the
//! DB or CF level
use crate::error;
use crate::ffi;
use crate::Result;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

cpp! {{
    #include "src/lib.h"
}}

/// A RocksDB `Cache` impl (currently `LRUCache`) which can be shared between CFs or even between
/// databases.
pub(crate) struct CacheInner(*mut ffi::rocksdb_cache_t);

impl Drop for CacheInner {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_cache_destroy(self.0);
        }
    }
}

/// Rust assumes that a *mut pointer isn't sync or send, but in our particular case it's a
/// documented property of RocksDB cache objects that they are fully multi-thread capable
unsafe impl Sync for CacheInner {}
unsafe impl Send for CacheInner {}

/// A RocksDB `Cache` impl (currently `LRUCache`) which can be shared between CFs or even between
/// databases.
#[derive(Clone)]
pub struct Cache(Arc<CacheInner>);

impl Cache {
    /// Construct a `Cache` object wrapping around existing pointer to a cache in the RocksDB C API
    ///
    /// # Safety
    ///
    /// This cache will be freed when the returned `Cache` struct is dropped.  The caller must not
    /// do anything further with this cache pointer directly.
    unsafe fn from_rocksdb_cache(cache_ptr: *mut ffi::rocksdb_cache_t) -> Self {
        Self(Arc::new(CacheInner(cache_ptr)))
    }

    /// Create a new cache with the specified capacity, in bytes.
    ///
    /// Currently this uses the RocksDB C API internally, which in turn calls `NewLRUCache` in the
    /// C++ API.  It has the following defaults:
    ///
    /// * `num_shard_bits = -1` - This automatically determines the shard bits, with every shard at
    ///   least 512KB and the number of shard bits no more than 6
    /// * `strict_capacity_limit = false` - This allows the cache to grow a bit bigger than the
    ///   limit
    /// * `high_pri_pool_ratio = 0.5` - Reserves half of the cache for the high priority pool
    ///
    /// If in the future we need more control over the cache parameters we will need to use the C++
    /// API directly.
    pub fn with_capacity(capacity: usize) -> Result<Self> {
        let cache_ptr = unsafe { ffi::rocksdb_cache_create_lru(capacity) };

        if cache_ptr.is_null() {
            error::RocksDbCacheAlloc { capacity }.fail()
        } else {
            unsafe { Ok(Self::from_rocksdb_cache(cache_ptr)) }
        }
    }

    /// Create a new cache from the string representation used by RocksDB for setting cache
    /// parameters in config files.
    ///
    /// As of now this will always create an LRU cache.
    ///
    /// The possible parameters here generally correspond to the fields in the `LRUCacheOptions`
    /// struct in the RocksDB C++ code.
    ///
    /// ## Examples
    ///
    /// ```
    /// # use roxide::Cache;
    ///
    /// // An LRU cache 10 MB in size
    /// let cache = Cache::from_string("10M").unwrap();
    /// assert_eq!(10 * 1024 * 1024, cache.capacity());
    /// ```
    ///
    /// ```
    /// # use roxide::Cache;
    ///
    /// // An LRU cache 10 MB in size, with 4 bits in the shard key (meaning 16 shards)
    /// let cache = Cache::from_string("capacity=10M;num_shard_bits=4").unwrap();
    /// assert_eq!(10 * 1024 * 1024, cache.capacity());
    /// ```
    pub fn from_string(value: impl AsRef<str>) -> Result<Self> {
        let value = value.as_ref();
        let value_ptr = value.as_ptr();
        let value_len = value.len() as isize;

        unsafe {
            let cache_ptr = cpp!([value_ptr as "const char*", value_len as "size_t"] -> *mut ffi::rocksdb_cache_t as "::rocksdb_cache_t*" {
                rocksdb::ConfigOptions config_options;
                std::string value(value_ptr, value_len);
                std::shared_ptr<rocksdb::Cache> cache_ptr;

                try {
                    auto status = rocksdb::Cache::CreateFromString(config_options,
                        value,
                        &cache_ptr);

                    if (status.ok()) {
                        // Need to wrap the C++ shared_ptr in the C struct so we can pass it back to
                        // Rust without problems
                        return wrap_cache(cache_ptr);
                    } else {
                        // TODO: we could make the Rust code a bit more complex and recover the error
                        // status to make for a more meaningful error message.  Since this is just
                        // parsing a simple string it's probably obvious enough when there's something
                        // wrong with it, and Rocks error messages are typically pretty useless anyway
                        return nullptr;
                    }
                } catch (const std::invalid_argument& ia) {
                    // the stoull function that Rocks used as part of string parsing can throw this
                    // when the string is sufficiently invalid.  C++ exceptions translate to Rust
                    // process termination so we can't have that
                    std::cout << "Caught C++ exception trying to parse '" << value << "': " << ia.what() << std::endl;
                    return nullptr;
                }
            });

            if cache_ptr.is_null() {
                error::RocksDbCacheParse {
                    value: value.to_string(),
                }
                .fail()
            } else {
                Ok(Self::from_rocksdb_cache(cache_ptr))
            }
        }
    }

    /// Total capacity of the cache including free and in use bytes
    pub fn capacity(&self) -> usize {
        unsafe { ffi::rocksdb_cache_get_capacity(self.0 .0) }
    }

    /// Number of bytes in the cache currently in use
    pub fn usage(&self) -> usize {
        unsafe { ffi::rocksdb_cache_get_usage(self.0 .0) }
    }

    /// Number of bytes in the cache currently in use and pinned.
    ///
    /// This is a subset of [`Self::usage`] reflecting the size of the cache entries that are in
    /// use by the system in some way and thus cannot be removed from the cache.
    pub fn pinned_usage(&self) -> usize {
        unsafe { ffi::rocksdb_cache_get_pinned_usage(self.0 .0) }
    }

    /// Get the RocksDB C API pointer to the cache wrapped by this struct.
    ///
    /// # Safety
    ///
    /// When the last `Cache` Rust clone is dropped, `rocksdb_cache_destroy` is called on this
    /// cache, freeing all memory associated with it.  That will not necessarily free the RocksDB C++
    /// `Cache` object, because it's stored in a `shared_ptr`, so as long as there is another
    /// reference to it somewhere in the process, the cache itself will remain in memory.  However
    /// it will mean that this specific `rocksdb_cache_t` pointer is no longer valid, and attempts
    /// to dereference it will result in UB.
    ///
    /// This function is marked `unsafe` to force callers to think carefully about this
    /// and understand the implications.
    pub(crate) unsafe fn rocksdb_cache_t(&self) -> *mut ffi::rocksdb_cache_t {
        self.0 .0
    }

    /// Get the memory address of the RocksDB C++ object `Cache`
    ///
    /// Note that this is not the same as [`Self::rocksdb_cache_t`].  The C++ object address is
    /// better for use as a unique identifier for the cache instance, since multiple
    /// `rocksdb_cache_t` structs can point to the same `Cache` object.
    ///
    /// # Safety
    ///
    /// This is intended to be used as an opaque pointer and identifier of the cache.  *DO NOT*
    /// attempt to dereference it or make any assumptions about the memory it points to.
    pub(crate) unsafe fn rocksdb_cache_ptr(&self) -> *mut libc::c_void {
        let cache_ptr = self.rocksdb_cache_t();

        cpp!([cache_ptr as "rocksdb_cache_t*"] -> *mut libc::c_void as "rocksdb::Cache*" {
            return cast_to_cache(cache_ptr);
        })
    }
}

/// It's sometimes useful to be able to see the current capacity and usage numbers when dumping a
/// config struct with a cache in it
impl fmt::Debug for Cache {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.debug_struct("Cache")
            .field("capacity", &self.capacity())
            .field("usage", &self.usage())
            .finish()
    }
}

/// Equality for a cache is defined as pointing to the same `Cache` struct.
///
/// Note that we don't care if they are different `rocksdb_cache_t*` because those are just a layer
/// of indirection; we care if these two cache objects actually point to the same cache with the
/// same items
impl PartialEq for Cache {
    fn eq(&self, other: &Self) -> bool {
        unsafe { self.rocksdb_cache_ptr() == other.rocksdb_cache_ptr() }
    }
}

impl Eq for Cache {}

/// Implementing `FromStr` lets use create an instance of `Cache` using `str::parse` which is a
/// nice convenience, and also is required if we're going to provide a default value for `Cache` in
/// a struct that uses `SmartDefault`.
impl FromStr for Cache {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Cache::from_string(s)
    }
}

/// Serialize for `Cache` just means serializing the cache config, not the actual contents of the
/// cache.
///
/// This is mainly intended for debugging purposes, so that the `Cache` can be included in amongst
/// all other `DbOptions` elements.  This isn't a perfect round-trip, as it doesn't preserve
/// settings other than capacity due to limitations in the C++ API preventing us from getting the
/// value of all other parameters.
impl Serialize for Cache {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize as an integer reflecting the total capacity of this cache
        serializer.serialize_u64(self.capacity() as u64)
    }
}

/// Deserialization is used when loading a cache config from a TOML, YAML, or other config format.
///
/// Unlike serialization which is quite limited, the full cache config is available to control
/// here.
///
/// There are two supported deserialization cases:
/// * From an integer representation of the cache capacity
/// * From a string containing the LRUCache parameters in a form that [`Cache::from_string`] can
///   parse.
///
/// Either of these are supported, and the result will be an (initially empty) cache with the
/// specified parameters.
impl<'de> Deserialize<'de> for Cache {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Try to deserialize an integer representation first, if it's not an integer fall back to
        // a string

        use serde::de::Visitor;

        struct CacheVisitor;

        impl<'de> Visitor<'de> for CacheVisitor {
            type Value = Cache;

            fn expecting(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt.write_str("integer or string")
            }

            fn visit_u64<E>(self, val: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Cache::with_capacity(val as usize).map_err(|e| E::custom(format!("{}", e)))
            }

            fn visit_str<E>(self, val: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Cache::from_string(val).map_err(|e| E::custom(format!("{}", e)))
            }
        }

        deserializer.deserialize_any(CacheVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_cache_with_capacity() -> Result<()> {
        let cache = Cache::with_capacity(10 * 1024 * 1024)?;

        assert_eq!(10 * 1024 * 1024, cache.capacity());
        assert_eq!(0, cache.usage());
        assert_eq!(0, cache.pinned_usage());

        Ok(())
    }

    #[test]
    fn from_valid_string() -> Result<()> {
        let cache: Cache = "10M".parse()?;
        assert_eq!(10 * 1024 * 1024, cache.capacity());

        let cache: Cache = "capacity=10M".parse()?;
        assert_eq!(10 * 1024 * 1024, cache.capacity());

        let cache: Cache = "capacity=10M;num_shard_bits=4".parse()?;
        assert_eq!(10 * 1024 * 1024, cache.capacity());

        Ok(())
    }

    #[test]
    fn from_invalid_string() {
        assert!(Cache::from_string("foo").is_err());
        assert!(Cache::from_string("capacity=wombat").is_err());
    }

    #[test]
    fn eq() -> Result<()> {
        // Two caches created with the same size are not equal
        let cache1 = Cache::with_capacity(10 * 1024 * 1024)?;
        let cache2 = Cache::with_capacity(10 * 1024 * 1024)?;

        assert_ne!(cache1, cache2);

        // Clones are however equal
        let cache2 = cache1.clone();
        assert_eq!(cache1, cache2);

        Ok(())
    }

    #[test]
    fn serialize() -> Result<()> {
        let cache = Cache::with_capacity(10_000_000)?;

        let json = serde_json::to_string(&cache).unwrap();

        assert_eq!(r#"10000000"#, json);

        Ok(())
    }

    /// Should be able to deserialize from either an integer representation of capacity, or a
    /// string with a valid parameter string
    #[test]
    fn deserialize() {
        const JSON_INT: &str = r#"10000000"#;
        const JSON_STRING: &str = r#""capacity=10M;num_shard_bits=4""#;
        const JSON_INVALID: &str = r#""foo;bar""#;

        let cache: Cache = serde_json::from_str(JSON_INT).unwrap();
        assert_eq!(10_000_000, cache.capacity());

        let cache: Cache = serde_json::from_str(JSON_STRING).unwrap();
        assert_eq!(10 * 1024 * 1024, cache.capacity());

        assert!(serde_json::from_str::<Cache>(JSON_INVALID).is_err());
    }
}
