use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

fn link(name: &str, bundled: bool) {
    use std::env::var;
    let target = var("TARGET").unwrap();
    let target: Vec<_> = target.split('-').collect();
    if target.get(2) == Some(&"windows") {
        println!("cargo:rustc-link-lib=dylib={}", name);
        if bundled && target.get(3) == Some(&"gnu") {
            let dir = var("CARGO_MANIFEST_DIR").unwrap();
            println!("cargo:rustc-link-search=native={}/{}", dir, target[0]);
        }
    }
}

fn fail_on_empty_directory(name: &str) {
    if fs::read_dir(name).unwrap().count() == 0 {
        println!(
            "The `{}` directory is empty, did you forget to pull the submodules?",
            name
        );
        println!("Try `git submodule update --init --recursive`");
        panic!();
    }
}

fn rocksdb_include_dir() -> String {
    match env::var("ROCKSDB_INCLUDE_DIR") {
        Ok(val) => val,
        Err(_) => "rocksdb/include".to_string(),
    }
}

fn bindgen_rocksdb() {
    let bindings = bindgen::Builder::default()
        .header(rocksdb_include_dir() + "/rocksdb/c.h")
        .derive_debug(false)
        .blocklist_type("max_align_t") // https://github.com/rust-lang-nursery/rust-bindgen/issues/550
        .ctypes_prefix("libc")
        .size_t_is_usize(true)
        .generate()
        .expect("unable to generate rocksdb bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("unable to write rocksdb bindings");
}

fn build_rocksdb() {
    let target = env::var("TARGET").unwrap();

    let mut config = cc::Build::new();
    let mut defines = HashMap::new();

    /// Add a #define to the cc compiler config and to our hashtable at the same time.
    ///
    /// We need to be able to re-create the #define's used to build rocks in the C++ code in
    /// `roxide` so we have to capture them at the source
    fn add_define<'a>(
        config: &mut cc::Build,
        defines: &mut HashMap<String, Option<String>>,
        name: impl Into<String>,
        value: impl Into<Option<&'a str>>,
    ) {
        let name = name.into();
        let value = value.into();

        config.define(&name, value);
        defines.insert(name, value.map(|s| s.to_string()));
    }

    //Canonicalize the include paths into absolute paths
    let mut include_paths: Vec<PathBuf> = vec![
        "rocksdb/include".into(),
        "rocksdb".into(),
        "rocksdb/third-party/gtest-1.8.1/fused-src/".into(),
    ];

    if cfg!(feature = "snappy") {
        add_define(&mut config, &mut defines, "SNAPPY", "1");
        include_paths.push("snappy/".into());
    }

    if cfg!(feature = "lz4") {
        add_define(&mut config, &mut defines, "LZ4", Some("1"));
        include_paths.push("lz4/lib/".into());
    }

    if cfg!(feature = "zstd") {
        add_define(&mut config, &mut defines, "ZSTD", Some("1"));
        include_paths.push("zstd/lib/".into());
        include_paths.push("zstd/lib/dictBuilder/".into());
    }

    if cfg!(feature = "zlib") {
        add_define(&mut config, &mut defines, "ZLIB", Some("1"));
        include_paths.push("zlib/".into());
    }

    if cfg!(feature = "bzip2") {
        add_define(&mut config, &mut defines, "BZIP2", Some("1"));
        include_paths.push("bzip2/".into());
    }

    if cfg!(feature = "jemalloc") {
        add_define(&mut config, &mut defines, "ROCKSDB_JEMALLOC", None);
        add_define(&mut config, &mut defines, "JEMALLOC_NO_DEMANGLE", None);

        // We take a dep on the `jemalloc-sys` crate, which internally builds jemalloc from source
        // into a static library.  It helpfully reports the root path where the static lib and
        // headers are installed, in a variable called `root`.  Cargo will expose that to us in the
        // env var `DEP_JEMALLOC_ROOT`.
        let jemalloc_root = PathBuf::from(
            std::env::var("DEP_JEMALLOC_ROOT")
                .expect("missing DEP_JEMALLOC_ROOT env var; is jemalloc-sys a dependency?"),
        );

        // jemalloc-sys will also tell cargo to link the jemalloc static library, and set the lib
        // path.  All we need to do is set the include path
        include_paths.push(jemalloc_root.join("include"));
    }

    add_define(&mut config, &mut defines, "NDEBUG", Some("1"));

    let mut lib_sources = include_str!("rocksdb_lib_sources.txt")
        .trim()
        .split('\n')
        .map(str::trim)
        .collect::<Vec<&'static str>>();

    // We have a pregenerated a version of build_version.cc in the local directory
    lib_sources = lib_sources
        .iter()
        .cloned()
        .filter(|file| *file != "util/build_version.cc")
        .collect::<Vec<&'static str>>();

    if target.contains("x86_64") {
        // This is needed to enable hardware CRC32C. Technically, SSE 4.2 is
        // only available since Intel Nehalem (about 2010) and AMD Bulldozer
        // (about 2011).
        add_define(&mut config, &mut defines, "HAVE_SSE42", Some("1"));
        config.flag_if_supported("-msse2");
        config.flag_if_supported("-msse4.1");
        config.flag_if_supported("-msse4.2");

        if !target.contains("android") {
            add_define(&mut config, &mut defines, "HAVE_PCLMUL", Some("1"));
            config.flag_if_supported("-mpclmul");
        }
    }

    if target.contains("aarch64") {
        lib_sources.push("util/crc32c_arm64.cc")
    }

    // All targets support thread-local storage
    add_define(
        &mut config,
        &mut defines,
        "ROCKSDB_SUPPORT_THREAD_LOCAL",
        Some("1"),
    );

    if target.contains("darwin") {
        add_define(&mut config, &mut defines, "OS_MACOSX", None);
        add_define(&mut config, &mut defines, "ROCKSDB_PLATFORM_POSIX", None);
        add_define(&mut config, &mut defines, "ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("android") {
        add_define(&mut config, &mut defines, "OS_ANDROID", None);
        add_define(&mut config, &mut defines, "ROCKSDB_PLATFORM_POSIX", None);
        add_define(&mut config, &mut defines, "ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("linux") {
        add_define(&mut config, &mut defines, "OS_LINUX", None);
        add_define(&mut config, &mut defines, "ROCKSDB_PLATFORM_POSIX", None);
        add_define(&mut config, &mut defines, "ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("freebsd") {
        add_define(&mut config, &mut defines, "OS_FREEBSD", None);
        add_define(&mut config, &mut defines, "ROCKSDB_PLATFORM_POSIX", None);
        add_define(&mut config, &mut defines, "ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("windows") {
        link("rpcrt4", false);
        link("shlwapi", false);
        add_define(&mut config, &mut defines, "DWIN32", None);
        add_define(&mut config, &mut defines, "OS_WIN", None);
        add_define(&mut config, &mut defines, "_MBCS", None);
        add_define(&mut config, &mut defines, "WIN64", None);
        add_define(&mut config, &mut defines, "NOMINMAX", None);

        if &target == "x86_64-pc-windows-gnu" {
            // Tell MinGW to create localtime_r wrapper of localtime_s function.
            add_define(&mut config, &mut defines, "_POSIX_C_SOURCE", Some("1"));
            // Tell MinGW to use at least Windows Vista headers instead of the ones of Windows XP.
            // (This is minimum supported version of rocksdb)
            add_define(
                &mut config,
                &mut defines,
                "_WIN32_WINNT",
                Some("_WIN32_WINNT_VISTA"),
            );
        }

        // Remove POSIX-specific sources
        lib_sources = lib_sources
            .iter()
            .cloned()
            .filter(|file| {
                !matches!(
                    *file,
                    "port/port_posix.cc"
                        | "env/env_posix.cc"
                        | "env/fs_posix.cc"
                        | "env/io_posix.cc"
                )
            })
            .collect::<Vec<&'static str>>();

        // Add Windows-specific sources
        lib_sources.push("port/win/port_win.cc");
        lib_sources.push("port/win/env_win.cc");
        lib_sources.push("port/win/env_default.cc");
        lib_sources.push("port/win/win_logger.cc");
        lib_sources.push("port/win/io_win.cc");
        lib_sources.push("port/win/win_thread.cc");
    }

    config.flag(&cxx_standard());

    if target.contains("msvc") {
        config.flag("-EHsc");
    } else {
        // this was breaking the build on travis due to
        // > 4mb of warnings emitted.
        config.flag("-Wno-unused-parameter");

        // starting with c++17 the use of offsetof the way Rocks uses it triggers
        // this warning which is distracting
        config.flag("-Wno-invalid-offsetof");
    }

    // Write an include file with the #define's used to build rocks, so that our downstream Rust
    // code (or rather the C++ code embedded in it with the cpp! macro) can use the same #defines
    // when it includes Rocks headers
    let rust_include_path =
        PathBuf::from(env::var("OUT_DIR").unwrap()).join("roxide-librocksdb-sys");
    std::fs::create_dir_all(&rust_include_path)
        .expect("Failed to create rust include file output directory");
    let cpp_defines_path = rust_include_path.join("cpp_defines.h");

    let mut cpp_defines_content = Vec::new();
    writeln!(
        &mut cpp_defines_content,
        "// Auto-generated by roxide-librocksdb-sys/build.rs"
    )
    .unwrap();
    writeln!(&mut cpp_defines_content, "#pragma once").unwrap();
    for (name, value) in defines {
        if let Some(value) = value {
            writeln!(&mut cpp_defines_content, "#define {} {}", name, value).unwrap();
        } else {
            writeln!(&mut cpp_defines_content, "#define {}", name).unwrap();
        }
    }

    // Only write this file if it didn't already exist or has changed, so the compiler doesn't try
    // to re-build all the time
    if !cpp_defines_path.exists() {
        fs::write(&cpp_defines_path, &cpp_defines_content).unwrap();
    } else {
        let existing = fs::read(&cpp_defines_path).unwrap();

        if existing != cpp_defines_content {
            fs::write(&cpp_defines_path, &cpp_defines_content).unwrap();
        }
    }

    include_paths.push(rust_include_path);

    // Canonicalize all of the include paths so they are fully qualified
    let include_paths = include_paths
        .iter()
        .map(|include| {
            dunce::canonicalize(include).unwrap_or_else(|e| {
                panic!("Failed to canonicalize path {}: {}", include.display(), e)
            })
        })
        .collect::<Vec<_>>();

    // Tell cc about all of these include paths
    for include in include_paths.iter() {
        config.include(include.as_path());
    }

    config.include(".");

    // Report the include paths via cargo so downstream crates can use them also
    let include_path = std::env::join_paths(include_paths).expect("join_paths failed");
    println!(
        "cargo:include={}",
        include_path.to_str().expect("to_str failed")
    );

    //Build C++ wrapper files in the out dir which will `#include` every source file in the
    //library.  This sucks compared to just calling `config.file` once for each source file, but
    //that doesn't work. Why?  It's complicated.
    //
    //First: It's important to canonicalize the source file names.  the RocksDB logging system
    //makes an assumption that the `__FILE__` intrinsic for all source *and* header files that
    //interact with the logging system will all have the same shared prefix.  Since dependent
    //crates (like `rocksdb`) will use the absolute path to the header files, and therefore the
    //absolute path will be in the `__FILE__` intrinsic for those headers, the RocksDB source
    //files must be compiled such that their `__FILE__` intrinsic has the same absolute path,
    //otherwise the log output is corrupted.
    //
    //For the same reason, include paths are canonicalized above with `canonically_include`
    //
    //So why does that require us to do this hack with making dummy files in OUT_DIR?  Due to a bug in the `cc`
    //crate:
    //`config.file()` needs to determine a suitable name for the `.o` file that gets produced when
    //compiling this file.  How does it do that?  Well, if `file` is relative then it's easy: it
    //just appends `file` to the dest dir and adds `.o  But if `file` is absolute, then it appends
    //only the file NAME to the dest dir.  That would be find except the RocksDB code has multiple
    //files with the name `format.cc` in different directories, and this means they clobber
    //eachother on output leading to linker errors that are hard to debug.
    for file in lib_sources {
        if !file.is_empty() {
            let file = "rocksdb/".to_string() + file;

            // Make a file with path `file` relative to `OUT_DIR`, that does nothing but #include
            // the fully-qualified path of the actual source file in the RocksDB library soruces
            let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
            let dummy_file_path = out_path.join(&file);
            if let Some(parent) = dummy_file_path.parent() {
                std::fs::create_dir_all(parent)
                    .expect("Failed to create dummy file output directory");
            }

            // Don't re-create this file if it already exists; that will just make the compiler
            // think it needs to be rebuilt
            if !dummy_file_path.exists() {
                let mut dummy_file = fs::File::create(&dummy_file_path).unwrap();

                let actual_file_path = dunce::canonicalize(&file).unwrap_or_else(|e| {
                    panic!("Failed to canonicalize source file path {}: {}", file, e)
                });

                writeln!(
                    dummy_file,
                    "#include \"{}\"",
                    actual_file_path.to_string_lossy()
                )
                .unwrap();
            }

            // Include the dummy file in the list of source files to compile.
            config.file(&dummy_file_path);
        }
    }

    config.file("build_version.cc");
    config.cpp(true);
    config.compile("librocksdb.a");
}

fn build_snappy() {
    let target = env::var("TARGET").unwrap();
    let endianness = env::var("CARGO_CFG_TARGET_ENDIAN").unwrap();
    let mut config = cc::Build::new();

    config.include("snappy/");
    config.include(".");
    config.define("NDEBUG", Some("1"));
    config.extra_warnings(false);

    if target.contains("msvc") {
        config.flag("-EHsc");
    } else {
        // Snappy requires C++11.
        // See: https://github.com/google/snappy/blob/master/CMakeLists.txt#L32-L38
        config.flag("-std=c++11");
    }

    if endianness == "big" {
        config.define("SNAPPY_IS_BIG_ENDIAN", Some("1"));
    }

    config.file("snappy/snappy.cc");
    config.file("snappy/snappy-sinksource.cc");
    config.file("snappy/snappy-c.cc");
    config.cpp(true);
    config.compile("libsnappy.a");
}

fn build_lz4() {
    let mut compiler = cc::Build::new();

    compiler
        .file("lz4/lib/lz4.c")
        .file("lz4/lib/lz4frame.c")
        .file("lz4/lib/lz4hc.c")
        .file("lz4/lib/xxhash.c");

    compiler.opt_level(3);

    let target = env::var("TARGET").unwrap();

    if &target == "i686-pc-windows-gnu" {
        compiler.flag("-fno-tree-vectorize");
    }

    compiler.compile("liblz4.a");
}

fn build_zstd() {
    let mut compiler = cc::Build::new();

    compiler.include("zstd/lib/");
    compiler.include("zstd/lib/common");
    compiler.include("zstd/lib/legacy");

    let globs = &[
        "zstd/lib/common/*.c",
        "zstd/lib/compress/*.c",
        "zstd/lib/decompress/*.c",
        "zstd/lib/dictBuilder/*.c",
        "zstd/lib/legacy/*.c",
    ];

    for pattern in globs {
        for path in glob::glob(pattern).unwrap() {
            let path = path.unwrap();
            compiler.file(path);
        }
    }

    compiler.opt_level(3);
    compiler.extra_warnings(false);

    compiler.define("ZSTD_LIB_DEPRECATED", Some("0"));

    // Hide symbols from resulting library,
    // so we can be used with another zstd-linking lib.
    // See https://github.com/gyscos/zstd-rs/issues/58
    // MSVC's cl.exe doesn't recognize this param and there doesn't seem to be an
    // alternative.
    #[cfg(not(windows))]
    compiler.flag("-fvisibility=hidden");
    compiler.define("ZSTDLIB_VISIBILITY", Some(""));
    compiler.define("ZDICTLIB_VISIBILITY", Some(""));
    compiler.define("ZSTDERRORLIB_VISIBILITY", Some(""));

    // Force the XXHash code to prefix its symbols so that they don't collide with the identical
    // xxhash code in the lz4 libary
    compiler.define("XXH_NAMESPACE", Some("ZSTD_"));

    compiler.compile("libzstd.a");
}

fn build_zlib() {
    let mut compiler = cc::Build::new();

    let globs = &["zlib/*.c"];

    for pattern in globs {
        for path in glob::glob(pattern).unwrap() {
            let path = path.unwrap();
            compiler.file(path);
        }
    }

    compiler.flag_if_supported("-Wno-implicit-function-declaration");
    compiler.opt_level(3);
    compiler.extra_warnings(false);
    compiler.compile("libz.a");
}

fn build_bzip2() {
    let mut compiler = cc::Build::new();

    compiler
        .file("bzip2/blocksort.c")
        .file("bzip2/bzlib.c")
        .file("bzip2/compress.c")
        .file("bzip2/crctable.c")
        .file("bzip2/decompress.c")
        .file("bzip2/huffman.c")
        .file("bzip2/randtable.c");

    compiler
        .define("_FILE_OFFSET_BITS", Some("64"))
        .define("BZ_NO_STDIO", None);

    compiler.extra_warnings(false);
    compiler.opt_level(3);
    compiler.extra_warnings(false);
    compiler.compile("libbz2.a");
}

fn try_to_find_and_link_lib(lib_name: &str) -> bool {
    if let Ok(v) = env::var(&format!("{}_COMPILE", lib_name)) {
        if v.to_lowercase() == "true" || v == "1" {
            return false;
        }
    }

    if let Ok(lib_dir) = env::var(&format!("{}_LIB_DIR", lib_name)) {
        println!("cargo:rustc-link-search=native={}", lib_dir);
        let mode = match env::var_os(&format!("{}_STATIC", lib_name)) {
            Some(_) => "static",
            None => "dylib",
        };
        println!("cargo:rustc-link-lib={}={}", mode, lib_name.to_lowercase());
        return true;
    }
    false
}

#[cfg(linux)]
fn cxx_standard() -> String {
    env::var("ROCKSDB_CXX_STD").map_or("-std=c++17".to_owned(), |cxx_std| {
        if !cxx_std.starts_with("-std=") {
            format!("-std:{}", cxx_std)
        } else {
            cxx_std
        }
    })
}

#[cfg(windows)]
fn cxx_standard() -> String {
    env::var("ROCKSDB_CXX_STD").map_or("-std:c++17".to_owned(), |cxx_std| {
        if !cxx_std.starts_with("-std:") {
            format!("-std:{}", cxx_std)
        } else {
            cxx_std
        }
    })
}

fn main() {
    bindgen_rocksdb();

    if !try_to_find_and_link_lib("ROCKSDB") {
        println!("cargo:rerun-if-changed=rocksdb/");
        fail_on_empty_directory("rocksdb");
        build_rocksdb();
    } else {
        let target = env::var("TARGET").unwrap();
        // according to https://github.com/alexcrichton/cc-rs/blob/master/src/lib.rs#L2189
        if target.contains("apple") || target.contains("freebsd") || target.contains("openbsd") {
            println!("cargo:rustc-link-lib=dylib=c++");
        } else if target.contains("linux") {
            println!("cargo:rustc-link-lib=dylib=stdc++");
        }
    }
    if cfg!(feature = "snappy") && !try_to_find_and_link_lib("SNAPPY") {
        println!("cargo:rerun-if-changed=snappy/");
        fail_on_empty_directory("snappy");
        build_snappy();
    }
    if cfg!(feature = "lz4") && !try_to_find_and_link_lib("LZ4") {
        println!("cargo:rerun-if-changed=lz4/");
        fail_on_empty_directory("lz4");
        build_lz4();
    }
    if cfg!(feature = "zstd") && !try_to_find_and_link_lib("ZSTD") {
        println!("cargo:rerun-if-changed=zstd/");
        fail_on_empty_directory("zstd");
        build_zstd();
    }
    if cfg!(feature = "zlib") && !try_to_find_and_link_lib("Z") {
        println!("cargo:rerun-if-changed=zlib/");
        fail_on_empty_directory("zlib");
        build_zlib();
    }
    if cfg!(feature = "bzip2") && !try_to_find_and_link_lib("BZ2") {
        println!("cargo:rerun-if-changed=bzip2/");
        fail_on_empty_directory("bzip2");
        build_bzip2();
    }
}
