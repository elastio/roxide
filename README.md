# Advanced Rust bindings for RocksDB

This repo contains `roxide`, Elastio's Rust bindings for RocksDB.

Roxide sits on top of our fork of [rust-rocksdb](https://github.com/rust-rocksdb/rust-rocksdb), providing a more
ergonomic Rust API and a lot of functionality missing in `rust-rocksdb` (transactions, logging, async, and a lot more).

# Development Tasks

## How to Update to a New RocksDB

* Update the linked RocksDB version.  See the `roxide-librocksdb-sys/README.md` file for instructions
* Update the build metadata in the semver version of all three crates, to reflect the current RocksDB version.  For
  example, if the new RocksDB version is 5.6.7, add `+rocksdb.5.6.7` as the build metadata to the version in
  `Cargo.toml` for `roxide`, `roxide-rocksdb`, and `roxide-librocksdb-sys`
* Consider publishing a new release of these crates so that other projects can take advantage of this new version

Note that the version numbers from `rust-rocksdb` were reset when we make a fork.  That project set the version of
`librocksdb-sys` to correspond with the version of RocksDB that it wrapped, but we don't do that.

## Required Dependencies

- This requires `liburing` be present to enable async I/O support.  On Fedora that's the `liburing-devel` package.

# WIP - Async Support

When updating to RocksDB 8.1, I tried to add support for async `MultiGet`.  Unfortunately I failed, but I left all of
the pieces in place in the hopes that some brave soul can finish this work later.

The featuer `io_uring` in roxide and in lower-level crates sets #defines to enable this in RocksDB.  However the RocksDB
docs are clear that async support in `MultiGet` requires the Facebook C++ ~~radioactive dumpster fire~~ framework
`folly` in order to function.  I tried for days to get that piece of shit to compile and link cleanly on both a local
dev system and our CI system, with no success.

So I left the `io_uring` feature in place (just know that it doesn't actually enable async `MultiGet`, but it might
enable async iterators, the docs are ambiguous), but I commented out the `folly` feature because otherwise
`--all-features` builds would fail.  I left in place in `build.rs` the `build_folly` method (conditional on the
`follly`) feature being enabled), but what's in that function doesn't work right.  I could not get it to compile and
link cleanly.  

`folly` uses a python script (yes really) to wrap all of the contortions required to compile it.  There is an option to
install dependencies, and that option can either download and compile from source the dependencies, or try to install
system packages.  Then there's another option in the Pythong script to build folly, and that can optionally try to use
system pacakges for deps or not.  Neither of these options worked.  

I tried to use the `pkg-config` entry that is in the Folly code for `libfolly`, but it must not be used by facebook
because it's missing many of the dependencies that libfolly actually has.  I tried explicitly adding these one at a time
but that didn't work either, because then I ran into problems with the include paths for `fmt`, conflicting between the
locally installed Fedora package (which is apparently old), and the latest and greatest built from source.

All of this was just not worth it, especially since we have no idea if any of that async crap will actually lead to
a measurable performance improvement.



# Credits

The original authors and maintainers of [rust-rocksdb](https://github.com/rust-rocksdb/rust-rocksdb) built the
foundation upon which `roxide` was based.  While we no longer follow this project, it's likely Roxide would have taken
a much different form without the base of `rust-rocksdb` upon which to grow.

