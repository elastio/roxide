# Advanced Rust bindings for RocksDB

This crate sits on top of our fork of [rust-rocksdb](https://github.com/rust-rocksdb/rust-rocksdb), providing a more
ergonomic API for our specific use case.  It's probably not suitable to the general purpose uses.

The intention is to minimize the amount of changes we have to maintain in our fork, and instead put the bulk of the
changes we need here.  That will make it easier to keep up with the upstream changes.  Eventually this may turn into
a proper fork, or we may try to get our changes merged upstream, we'll see.
