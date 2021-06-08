# IMPORTANT

This is based on the [`rocksdb`](https://github.com/rust-rocksdb/rust-rocksdb) crate which provided the first usable
Rust bindings for RocksDB.  This was forked by Elastio and maintained at `github.com/elastio/rust-rocksdb` for a while,
but has since been incorporated into the `roxide` repository.  It's no longer connected to the original upstream work,
although credit is due to the authors and maintainers of that crate for building the foundation upon which `roxide` is
based.

# WINDOWS

My changes to the RocksDB build process in this fork break Windows support.  Specifically, in `build.rs` I combine all
of the source files together into one artifact which is compiled as one unit.  That causes a conflict between the
Winodws function `CreateFile` and an internal RocksDB library function `CreateFile`.  To fix this would require
reverting to the per-file compilation which breaks the logging support.  Windows support is not worth it to me so it's
simply disabled.

# What is this?

We use our own layer on top of this crate to improve the RocksDB ergonomics in our Rust code.  That layer needs access
to some of the internals of `rust-rocksdb` which in the upstream version are private.  So this fork makes those public,
but otherwise strives to be as close as possible to the upstream code.

It also changes how `librocksdb-sys` is built to work around a very stupid assumption bug in the RocksDB code that
causes log output to be corrupted.

# Procedure to update:

* Update the linked RocksDB version.  See the `librocksdb-sys/README.md` file for instructions
* Merge the update into the `elastio` branch (we reserve `master` to sync changes from upstream)
* Create a tag on the repo with the version of Rocks it corresponds to.  Eg `tags/elastio-6.6.4` for the version that uses
    RocksDB 6.6.4.

Note that, somewhat counterintuitively, the version of the `rust-rocksdb` crate doesn't reflect the RocksDB version.
Rather, the version of the `librocksdb-sys` does.  Rather than fight that convention, we leave it in place for now.
Downstream crates incorporate this one via Git submodules using the tag created above.  It's not idea but it works for
now.

rust-rocksdb
============
![RocksDB build](https://github.com/rust-rocksdb/rust-rocksdb/workflows/RocksDB%20build/badge.svg?branch=master)
[![crates.io](https://img.shields.io/crates/v/rocksdb.svg)](https://crates.io/crates/rocksdb)
[![documentation](https://docs.rs/rocksdb/badge.svg)](https://docs.rs/rocksdb)
[![license](https://img.shields.io/crates/l/rocksdb.svg)](https://github.com/rust-rocksdb/rust-rocksdb/blob/master/LICENSE)
[![Gitter chat](https://badges.gitter.im/rust-rocksdb/gitter.png)](https://gitter.im/rust-rocksdb/lobby)


![GitHub commits (since latest release)](https://img.shields.io/github/commits-since/rust-rocksdb/rust-rocksdb/latest.svg)

## Requirements

- Clang and LLVM

## Contributing

Feedback and pull requests welcome!  If a particular feature of RocksDB is 
important to you, please let me know by opening an issue, and I'll 
prioritize it.

## Usage

This binding is statically linked with a specific version of RocksDB. If you 
want to build it yourself, make sure you've also cloned the RocksDB and 
compression submodules:

    git submodule update --init --recursive

## Compression Support
By default, support for the [Snappy](https://github.com/google/snappy), 
[LZ4](https://github.com/lz4/lz4), [Zstd](https://github.com/facebook/zstd), 
[Zlib](https://zlib.net), and [Bzip2](http://www.bzip.org) compression 
is enabled through crate features.  If support for all of these compression 
algorithms is not needed, default features can be disabled and specific 
compression algorithms can be enabled. For example, to enable only LZ4 
compression support, make these changes to your Cargo.toml:

```
[dependencies.rocksdb]
default-features = false
features = ["lz4"]
```

## Multi-threaded ColumnFamily alternation

The underlying RocksDB does allow column families to be created and dropped
from multiple threads concurrently. But this crate doesn't allow it by default
for compatibility. If you need to modify column families concurrently, enable
crate feature called `multi-threaded-cf`, which makes this binding's
data structures to use RwLock by default. Alternatively, you can directly create
`DBWithThreadMode<MultiThreaded>` without enabling the crate feature.
