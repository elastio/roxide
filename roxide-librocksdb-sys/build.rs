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

    #[cfg(feature = "folly")]
    if env::var("TARGET").unwrap().contains("linux") {
        build_folly(&mut config);
    }

    if cfg!(feature = "io_uring") {
        include_paths.push("liburing/src/include/".into());
    }

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

        // See elsehwere `build_io_uring` where we build liburing from source.  This will
        // statically link liburing.  According to the liburing docs, newer liburing can run
        // against older kernel versions, although newer kernel uring features are obviously not
        // available in that case.
        if cfg!(feature = "io_uring") {
            add_define(&mut config, &mut defines, "ROCKSDB_IOURING_PRESENT", None);
            println!("cargo:rustc-link-lib=static=uring");
        }

        if cfg!(feature = "folly") {
            // If the use of the facebook open source library "folly" is enabled, then we can use
            // coroutines for async support.
            add_define(&mut config, &mut defines, "USE_COROUTINES", None);
            config.flag_if_supported("-fcoroutines");

            // folly sucks so much that on most distros there isn't an official package for it.
            // you're meant to build it from source.  On our Github Actions builds, running the
            // latest Ubuntu, we have to build a specific old version compatible with the older
            // kernel that Ubuntu 22.04 runs.  We need a way to find the folly library and include
            // files.  The hack is to set env vars.
            //
            // But at least on Fedora, there is a `folly` OS package, and that's so much more
            // convenient that we want to support that do.  So setting this env var isn't required.
            //if let Ok(folly_install_dir) = std::env::var_os("FOLLY_INSTALL_DIR") {
            //    include_paths.push(PathBuf::from(folly_install_dir).join("include"));
            //    println!("cargo:rustc-link-search=native={}", folly_install_dir);
            //}

            // linked.  `folly` in turn requires other deps including boost.
            //
            // Sadly all of deps are such insanely complex C++ projects there's no way to vendor
            // them, they must be installed as shared libraries
            println!("cargo:rustc-link-lib=folly");
            println!("cargo:rustc-link-lib=boost_context");
            println!("cargo:rustc-link-lib=glog");
        }

        // The following are detected dynamically in the CMakeLists.txt file in the official
        // RocksDB build.  We dont' have access to cmake so we just assume these are present
        // because we've made them a required part of our build environment on Linux.

        add_define(&mut config, &mut defines, "ROCKSDB_FALLOCATE_PRESENT", None);
        add_define(&mut config, &mut defines, "ROCKSDB_RANGESYNC_PRESENT", None);
        add_define(
            &mut config,
            &mut defines,
            "ROCKSDB_PTHREAD_ADAPTIVE_MUTEX",
            None,
        );
        add_define(
            &mut config,
            &mut defines,
            "ROCKSDB_MALLOC_USABLE_SIZE",
            None,
        );
        add_define(
            &mut config,
            &mut defines,
            "ROCKSDB_SCHED_GETCPU_PRESENT",
            None,
        );
        add_define(
            &mut config,
            &mut defines,
            "ROCKSDB_AUXV_GETAUXVAL_PRESENT",
            None,
        );
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

    if target.contains("msvc") {
        config.flag("-EHsc");
    } else {
        config.flag(&cxx_standard());
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

/// Build the Folly library from the git submodule that's embedded in this crate.
///
/// NOTE: this currently doesn't work reliably.  Getting `folly` and its deps to build and link has
/// proven to be too hard for me to implement.  I've left this here because the code as is does
/// solve a number of nasty problems that came up, but it still doesn't work right.
///
/// Presently I can't find a way to get the include path for compiling the rocksdb C++ files to
/// point to the `fmt` library version that the folly build script downloads and builds from
/// source.  Maybe a better programmer than I can figure it out.  What follows is the doc comment
/// for this function if you assume the function actually works.
///
/// Folly is a hot mess, a total shit show and a perfect example of what happens when you let C++
/// developers build a framework without any supervision.  Nothing of what you would expect from a
/// modern software library applies to Folly.  It's not installed anywhere, it's not distributed as
/// an OS package, it works with third-party dependencies that need to be downloaded and installed
/// with a specific script.
///
/// Unless you're trying to fix a problem with folly compilation on your system, ignore this
/// function and pretend you didn't see it.  If you *are* trying to figure out why this doens't
/// work on your system, then you have my sympathies.  Make absolutely sure you really need that
/// io_uring support; maybe you don't and you can avoid this by not enabling the `io_uring` feature
/// which in turn requires folly.  If not, well, godspeed you poor bastard!
#[cfg(feature = "folly")]
fn build_folly(config: &mut cc::Build) {
    use duct::cmd;
    use std::io::BufRead;
    use std::path::Path;

    /// USe the `pkg-config` crate to read a pkg-config file and look up the pkg config for the
    /// specified package
    ///
    /// Unfortunately the only way we have to direct pkgconfig to the scratch path where folly's
    /// build script writes the pkg-config file is via fucking with env vars
    fn pkg_config(pkg_config_path: &Path, package: &str) -> pkg_config::Library {
        let old_pkgconfig_path = env::var("PKG_CONFIG_PATH").ok();
        env::set_var("PKG_CONFIG_PATH", pkg_config_path);
        let library = pkg_config::Config::new()
            .statik(true)
            .probe(package)
            .unwrap();
        env::set_var(
            "PKG_CONFIG_PATH",
            old_pkgconfig_path.as_deref().unwrap_or(""),
        );

        library
    }

    /// pkg-config prints the output that will let cargo link to the requested library, and all of
    /// it's dependent libs, however for some reason pkg-config doesn't automatically do that for
    /// include paths.
    ///
    /// This issue https://github.com/rust-lang/pkg-config-rs/issues/43 from 2017 is still open; I
    /// assume it's just not something the Rust team cares about.
    fn set_include_and_defines(config: &mut cc::Build, library: pkg_config::Library) {
        for include_path in library.include_paths {
            config.include(include_path);
        }

        for (name, value) in library.defines {
            config.define(&name, value.as_deref());
        }
    }

    /// Run a duct command, streaming stdout and stderr to the parent process but with the
    /// a distinctive prefix to make it clear where it comes from
    fn run_duct_command(command: duct::Expression) {
        let reader = command
            .stderr_to_stdout()
            .reader()
            .expect("Failed to start command");

        let reader = std::io::BufReader::new(reader);
        for lin in reader.lines() {
            println!("build_folly: {}", lin.unwrap());
        }

        // NOTE: duct's reader will automatically call `wait` at EOF and return an I/O error if the
        // process doesn't termiante correctly, so no need for further error handling
    }

    let folly_root = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap()).join("folly");
    let scratch_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("folly-scratch");
    let actual_lib_path = scratch_path.join("installed/folly/lib");
    let pkg_config_path = actual_lib_path.join("pkgconfig");

    // Only invoke this crap if folly isn't present (or if an env var is set to force re-running
    // this logic which greatly speeds up debugging)
    if !pkg_config_path.exists() && env::var("ROXIDE_FOLLY_FORCE_BUILD").is_ok() {
        // Make sure the deps that folly needs are installed.
        //
        // Folly is shit, so this wont' actually literally install all necessary deps.  It also
        // requires some deps that are assumed to be present.  Look at the ci.yml file to see what
        // Ubuntu packages are installed prior to building, for the latest on what packages need to be
        // present first
        //
        // Because this installs system packages, it needs to run as root.  What's that, you don't like
        // that a build.rs script runs `sudo`?  Well then you should have stuck to building Electron
        // apps in Typescript.
        println!(
            "cargo:warning=Installing folly dependencies (this may require your sudo password)"
        );
        run_duct_command(
            cmd!(
                "sudo",
                "./build/fbcode_builder/getdeps.py",
                "install-system-deps",
                "--recursive"
            )
            .dir(&folly_root),
        );

        // Now compile folly itself, using the out dir for the "scratch" dir.
        println!("Building folly from source");
        // FUCK: I think it's using the `fmt` library installed on the system and not the one it
        // downloaded.  FML.  How do I control the include path order??
        run_duct_command(
            cmd!(
                "python3",
                "./build/fbcode_builder/getdeps.py",
                "build",
                "--allow-system-packages",
                "--no-tests",
                // This little gem courtsey of https://github.com/facebook/folly/issues/1780
                r#"--extra-cmake-defines={"CMAKE_CXX_FLAGS": "-fPIC"}"#,
                "--scratch-path",
                &scratch_path
            )
            .dir(&folly_root),
        );
    }

    // Use the pkgconfig support to pull in the header files and libs as needed
    let library = pkg_config(&pkg_config_path, "libfolly");
    set_include_and_defines(config, library);

    // For unknown reasons, the cmake incantation in folly that produces the libfolly.pc pkgconfig
    // file is broken, and uses `lib64` instead of the actual `lib` where `libfolly.a` is written.
    // Rather than fuck around trying to fix this, just hack another workaround in here.
    // Everything about support for folly is ugly hacks and shit, so who will notice a little more?
    println!(
        "cargo:rustc-link-search=native={}",
        actual_lib_path.display()
    );

    // The pkgconfig entry for libfolly is stupid in another way: among the many include paths it
    // specifies, it forgets the include path for `fmt`, `gflag`, and probably others.  This leads
    // to compile errors if the system where the build runs doesn't have fmt installed, or even
    // worse, if the system has a distro package version of fmt installed but it's an incompatible
    // version.  Total amateur hours here.
    //
    // So add to the pile of bullshit hacks, and scan in the `installed/*-*` directories looking
    // for pkgconfig files.  Assume at least that the folly python build script is at least smart
    // enough to not download and compile anything that folly doesn't need.
    for (lib_path_frag, pkgconfig_package_name) in
        &[("fmt", "fmt"), ("glog", "libglog"), ("gflags", "gflags")]
    {
        let pattern = format!("{}/installed/{lib_path_frag}-*", scratch_path.display());
        for path_result in glob::glob(&pattern).unwrap() {
            let path = path_result.unwrap();
            if path.is_dir() {
                println!(
                    "Found a folly dependent lib '{lib_path_frag}' in {}",
                    path.display()
                );

                // THere seems to be no rhyme or reason to whether a given library uses `lib` or
                // `lib64`.  They're all 64-bit code.
                let mut pkg_config_path: Option<PathBuf> = None;

                for lib_bullshit in &["lib", "lib64"] {
                    let path = path.join(lib_bullshit).join("pkgconfig");
                    if path.is_dir() {
                        pkg_config_path = Some(path);
                        break;
                    }
                }

                let pkg_config_path = pkg_config_path
                    .as_ref()
                    .unwrap_or_else(|| panic!("No pkgconfig path found for {}", lib_path_frag));

                println!(
                    "Looking for package '{pkgconfig_package_name}' file in {}",
                    pkg_config_path.display()
                );

                let library = pkg_config(pkg_config_path, pkgconfig_package_name);
                set_include_and_defines(config, library);
            }
        }
    }

    // The folly code is so incredibly sloppy that the headers throw up a ton of warnings.
    // Suppress those so we can troubleshoot legit warnings
    config.flag_if_supported("-Wno-deprecated");

    println!("Folly has been built from source.  We'll find out later if it links cleanly or not");
}

fn build_io_uring() {
    let mut config = cc::Build::new();

    config.include("liburing/src/include");
    config.include("liburing");
    config.define("LIBURING_INTERNAL", None);
    config.define("_GNU_SOURCE", None);

    // Load the config defines directly from a header file
    config.flag("-include");
    config.flag(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/liburing/config-host.h"
    ));

    config.flag_if_supported("-Wno-stack-protector");

    for file in ["setup.c", "queue.c", "register.c", "syscall.c"] {
        config.file(format!("liburing/src/{file}"));
    }
    config.compile("uring"); // produces liburing.a
}

fn build_snappy() {
    let target = env::var("TARGET").unwrap();
    let endianness = env::var("CARGO_CFG_TARGET_ENDIAN").unwrap();
    let mut config = cc::Build::new();

    config.include("snappy/");
    config.include(".");
    config.define("NDEBUG", Some("1"));
    config.extra_warnings(false);

    // Snappy doesn't compile cleanly without this warning being suppressed.  Nice work guys.
    config.flag_if_supported("-Wno-sign-compare");

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
        "zstd/lib/decompress/*.S",
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
    compiler.define("ZSTDLIB_VISIBLE", Some(""));
    compiler.define("ZDICTLIB_VISIBLE", Some(""));
    compiler.define("ZSTDERRORLIB_VISIBLE", Some(""));

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

fn cxx_standard() -> String {
    env::var("ROCKSDB_CXX_STD").map_or("-std=c++20".to_owned(), |cxx_std| {
        if !cxx_std.starts_with("-std=") {
            format!("-std={}", cxx_std)
        } else {
            cxx_std
        }
    })
}

fn main() {
    bindgen_rocksdb();

    // ANELSON TESTING REMOVE
    if cfg!(feature = "io_uring") && !try_to_find_and_link_lib("LIBURING") {
        println!("cargo:rerun-if-changed=liburing/");
        fail_on_empty_directory("liburing");
        build_io_uring();
    }

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

    if cfg!(feature = "io_uring") && !try_to_find_and_link_lib("LIBURING") {
        println!("cargo:rerun-if-changed=liburing/");
        fail_on_empty_directory("liburing");
        build_io_uring();
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
