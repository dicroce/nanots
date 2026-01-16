// build.rs
use std::path::PathBuf;

fn main() {
    let src_dir = PathBuf::from("../../amalgamated_src");

    if !src_dir.exists() {
        panic!("Amalgamated sources not found at: {}", src_dir.display());
    }

    // Set up separate builds for C and C++ files
    let mut cpp_build = cc::Build::new();
    let mut c_build = cc::Build::new();

    // C++ build configuration for nanots.cpp
    cpp_build
        .cpp(true)
        .std("c++17")
        .include(&src_dir)
        .warnings(false)
        .opt_level(2);

    // C build configuration for sqlite3.c
    c_build
        .cpp(false)
        .include(&src_dir)
        .warnings(false)
        .opt_level(2);

    // Add source files
    let nanots_cpp = src_dir.join("nanots.cpp");
    let sqlite3_c = src_dir.join("sqlite3.c");

    if nanots_cpp.exists() {
        cpp_build.file(&nanots_cpp);
    } else {
        panic!("nanots.cpp not found at {}", nanots_cpp.display());
    }

    if sqlite3_c.exists() {
        c_build.file(&sqlite3_c);
    } else {
        panic!("sqlite3.c not found at {}", sqlite3_c.display());
    }

    // Add defines to both builds
    for build in [&mut cpp_build, &mut c_build] {
        build.define("NANOTS_BUILD", None);

        // SQLite configuration
        build.define("SQLITE_THREADSAFE", "1");
        build.define("SQLITE_ENABLE_FTS5", None);
        build.define("SQLITE_ENABLE_JSON1", None);
        build.define("SQLITE_ENABLE_RTREE", None);

        // Platform-specific settings
        if cfg!(target_os = "windows") {
            build.define("WIN32", None);
            build.define("_WIN32", None);
            build.define("SQLITE_OS_WIN", "1");
        } else if cfg!(target_os = "linux") {
            build.define("LINUX", None);
            build.define("SQLITE_OS_UNIX", "1");
        } else if cfg!(target_os = "macos") {
            build.define("MACOS", None);
            build.define("SQLITE_OS_UNIX", "1");
        }
    }

    // Add compiler-specific flags
    if !cfg!(target_env = "msvc") {
        c_build.flag("-Wno-unused-parameter");
        c_build.flag("-Wno-unused-function");
        c_build.flag("-Wno-unused-variable");
        c_build.flag("-Wno-unused-but-set-variable");
        c_build.flag("-Wno-sign-compare");
    }

    // Compile both libraries
    cpp_build.compile("nanots_cpp");
    c_build.compile("nanots_c");

    // Link required system libraries
    if cfg!(target_os = "linux") || cfg!(target_os = "macos") {
        println!("cargo:rustc-link-lib=pthread");
        println!("cargo:rustc-link-lib=dl");
        println!("cargo:rustc-link-lib=m");
    }

    // Tell Cargo when to rebuild
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={}", src_dir.display());
}
