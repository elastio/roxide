//! Uses the `cpp_build` crate to scan this crate's Rust code for `cpp!` macros, and compile a C++
//! static library that contains all of the embedded C++ code in the `cpp!` macros.  It's the
//! closest thing to C++ interop you can get in Rust right now.

fn main() {
    let include_path = std::env::var("DEP_ROCKSDB_INCLUDE")
        .expect("librocksdb-sys didn't expose an include path for RocksDB");

    let include_paths = std::env::split_paths(&include_path);
    let mut config = cpp_build::Config::new();

    // Rocks starting from 7.0 requires C++ 17
    #[cfg(unix)]
    config.flag("-std=c++17");
    #[cfg(windows)]
    config.flag("-std:c++17");

    for include_path in include_paths {
        config.include(&include_path);
    }

    config.build("src/lib.rs");
}
