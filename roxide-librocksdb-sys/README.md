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

`liburing` can't be included as a submodule, because the code as it exists in the git repo isn't compilable as it is.
So updating liburing means downloading a new source tarball, extracting it, and then running some preparatory commands
to get the code in a buildable state

To do so:

1. download the latest source tarball and extract it into the `roxide-librocksdb-sys` directory.  That will result in
   a subdirectory like `liburing-liburing-2.3`.
1. rename the newly extracted subdirectory to just `liburing`
1. cd into `liburing`
1. Run `./configure` to generate the config header `config-host.h` and also the `compat.h` header plus probably some
   other cruft
1. commit the changes to the repo accordingly.

