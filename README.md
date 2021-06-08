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


# Credits

The original authors and maintainers of [rust-rocksdb](https://github.com/rust-rocksdb/rust-rocksdb) built the
foundation upon which `roxide` was based.  While we no longer follow this project, it's likely Roxide would have taken
a much different form without the base of `rust-rocksdb` upon which to grow.

