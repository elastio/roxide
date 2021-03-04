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
        .blacklist_type("max_align_t") // https://github.com/rust-lang-nursery/rust-bindgen/issues/550
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

    //Canonicalize the include paths into absolute paths
    let include_paths: Vec<_> = [
        "rocksdb/include",
        "rocksdb",
        "rocksdb/third-party/gtest-1.8.1/fused-src/",
    ]
    .iter()
    .map(|include| {
        dunce::canonicalize(include).expect(&format!("Failed to canonicalize path {}", include))
    })
    .collect();

    for include in include_paths.iter() {
        config.include(include.as_path());
    }

    // Report the include paths via cargo so downstream crates can use them
    let include_path = std::env::join_paths(include_paths.iter()).expect("join_paths failed");
    println!(
        "cargo:include={}",
        include_path.to_str().expect("to_str failed")
    );

    if cfg!(feature = "snappy") {
        config.define("SNAPPY", Some("1"));
        config.include("snappy/");
    }

    if cfg!(feature = "lz4") {
        config.define("LZ4", Some("1"));
        config.include("lz4/lib/");
    }

    if cfg!(feature = "zstd") {
        config.define("ZSTD", Some("1"));
        config.include("zstd/lib/");
        config.include("zstd/lib/dictBuilder/");
    }

    if cfg!(feature = "zlib") {
        config.define("ZLIB", Some("1"));
        config.include("zlib/");
    }

    if cfg!(feature = "bzip2") {
        config.define("BZIP2", Some("1"));
        config.include("bzip2/");
    }

    config.include(".");
    config.define("NDEBUG", Some("1"));

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
        config.define("HAVE_SSE42", Some("1"));
        config.flag_if_supported("-msse2");
        config.flag_if_supported("-msse4.1");
        config.flag_if_supported("-msse4.2");

        if !target.contains("android") {
            config.define("HAVE_PCLMUL", Some("1"));
            config.flag_if_supported("-mpclmul");
        }
    }

    if target.contains("aarch64") {
        lib_sources.push("util/crc32c_arm64.cc")
    }

    // All targets support thread-local storage
    config.define("ROCKSDB_SUPPORT_THREAD_LOCAL", Some("1"));

    if target.contains("darwin") {
        config.define("OS_MACOSX", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("android") {
        config.define("OS_ANDROID", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("linux") {
        config.define("OS_LINUX", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("freebsd") {
        config.define("OS_FREEBSD", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("windows") {
        link("rpcrt4", false);
        link("shlwapi", false);
        config.define("DWIN32", None);
        config.define("OS_WIN", None);
        config.define("_MBCS", None);
        config.define("WIN64", None);
        config.define("NOMINMAX", None);

        if &target == "x86_64-pc-windows-gnu" {
            // Tell MinGW to create localtime_r wrapper of localtime_s function.
            config.define("_POSIX_C_SOURCE", Some("1"));
            // Tell MinGW to use at least Windows Vista headers instead of the ones of Windows XP.
            // (This is minimum supported version of rocksdb)
            config.define("_WIN32_WINNT", Some("_WIN32_WINNT_VISTA"));
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

    if target.contains("msvc") {
        config.flag("-EHsc");
    } else {
        config.flag(&cxx_standard());
        // this was breaking the build on travis due to
        // > 4mb of warnings emitted.
        config.flag("-Wno-unused-parameter");
    }

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
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    for file in lib_sources {
        if !file.is_empty() {
            let file = "rocksdb/".to_string() + file;

            // Make a file with path `file` relative to `OUT_DIR`, that does nothing but #include
            // the fully-qualified path of the actual source file in the RocksDB library soruces
            let dummy_file_path = out_path.join(&file);
            if let Some(parent) = dummy_file_path.parent() {
                std::fs::create_dir_all(parent)
                    .expect("Failed to create dummy file output directory");
            }

            // Don't re-create this file if it already exists; that will just make the compiler
            // think it needs to be rebuilt
            if !dummy_file_path.exists() {
                let mut dummy_file = fs::File::create(&dummy_file_path).unwrap();

                let actual_file_path = dunce::canonicalize(&file)
                    .expect(&format!("Failed to canonicalize source file path {}", file));

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
    compiler.flag("-fvisibility=hidden");
    compiler.define("ZSTDLIB_VISIBILITY", Some(""));
    compiler.define("ZDICTLIB_VISIBILITY", Some(""));
    compiler.define("ZSTDERRORLIB_VISIBILITY", Some(""));
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

fn cxx_standard() -> String {
    env::var("ROCKSDB_CXX_STD").map_or("-std=c++11".to_owned(), |cxx_std| {
        if !cxx_std.starts_with("-std=") {
            format!("-std={}", cxx_std)
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
