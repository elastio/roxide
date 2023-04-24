RocksDB bindings
================

Low-level bindings to RocksDB's C API.

Based on original work by Tyler Neely
https://github.com/rust-rocksdb/rust-rocksdb
and Jeremy Fitzhardinge
https://github.com/jsgf/rocksdb-sys

# Updating to a new version of RocksDB

1. `cd` to the `rocksdb` submodule directory where the RocksDB sources are checked out
  1. Update the `rocksdb` submodule to the tag for whatever the new release is.  
  1. ~~In the `rocksdb` subdirectory run `make util/build_version.cc`~~
     As of 6.20 this no longer works; instead do `make -j16 static_lib` instead
  1. Run `make unity.cc`
  1. Copy `util/build_version.cc` to the `roxide-librocksdb-sys` crate root directory
1. `cd` to the `roxide-librocksdb-sys` crate root
  1. Use the contents of `rocksdb/unity.cc` to recreate the `rocksdb_lib_sources.txt` file in this directory.  The latest code
     from upstream uses one file name per line in `rocksdb_lib_sources.txt` so `unity.cc` only needs to be modified to
     remove the `#include` directives around the file names
  1. Don't forget to update the build metadata portion of the version of the crate in `Cargo.toml` to match the RocksDB
     version.  Note that this change needs to be made to all three crates in this workspace; see the root-level
     `README.md` for more details

# Updating to a new version of liburing

The `liburing` folder contains a submodule with the most recent release of liburing.  This needs to be updated from time
to time as RocksDB uses the latest and greatest features.

To do so:

1. `cd` into `liburing`
1. Go a `git pull` and then checkout whatever tag corresponds to the version you want
1. Run `./configure` to generate the config header `config-host.h`.
1. Modify the `build_io_uring` function in `build.rs` to make sure all defines from `config-host.h` are defined when
   building

