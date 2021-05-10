RocksDB bindings
================

Low-level bindings to RocksDB's C API.

Based on original work by Tyler Neely
https://github.com/rust-rocksdb/rust-rocksdb
and Jeremy Fitzhardinge
https://github.com/jsgf/rocksdb-sys

# Updating to a new version

1. Update the `rocksdb` submodule to the tag for whatever the new release is.  
1. ~~In the `rocksdb` subdirectory run `make util/build_version.cc`~~
   As of 6.20 this no longer works; instead do `make static_lib` instead
1. In the `rocksdb` subdirectory run `make unity.cc`
1. Copy `rocksdb/util/build_version.cc` to this directory
1. Use the contents of `unity.cc` to recreate the `rocksdb_lib_sources.txt` file in this directory.  The latest code
   from upstream uses one file name per line in `rocksdb_lib_sources.txt` so `unity.cc` only needs to be modified to
   remove the `#include` directives around the file names
1. Don't forget to update the version of the crate in `Cargo.toml` to match the RocksDB version

