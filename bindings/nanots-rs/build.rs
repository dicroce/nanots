// build.rs
use std::path::PathBuf;
use std::fs;

fn main() {
    let external_src_dir = PathBuf::from("../../amalgamated_src");
    let local_src_dir = PathBuf::from("src/nanots");
    
    // Auto-copy strategy: Use external sources if available, otherwise use bundled
    let src_dir = if external_src_dir.exists() {
        println!("Found external sources at {}", external_src_dir.display());
        
        // Create local directory
        fs::create_dir_all(&local_src_dir)
            .expect("Failed to create src/nanots directory");
        
        // Copy all files from external to local
        copy_dir_all(&external_src_dir, &local_src_dir)
            .expect("Failed to copy external sources");
            
        println!("Copied sources from {} to {}", external_src_dir.display(), local_src_dir.display());
        local_src_dir
    } else {
        // Use bundled sources (for crates.io or when external sources not available)
        if !local_src_dir.exists() {
            panic!("No sources found! Need either:\n  - External sources at: {}\n  - Bundled sources at: {}", 
                   external_src_dir.display(), local_src_dir.display());
        }
        println!("Using bundled sources from {}", local_src_dir.display());
        local_src_dir
    };
    
    // Set up separate builds for C and C++ files
    let mut cpp_build = cc::Build::new();
    let mut c_build = cc::Build::new();
    
    // C++ build configuration for nanots.cpp
    cpp_build
        .cpp(true)                    // Enable C++ compilation
        .std("c++17")                 // C++ standard
        .include(&src_dir)            // Include the source directory
        .warnings(false)              // Disable warnings for cleaner output
        .opt_level(2);                // Optimization level
    
    // C build configuration for sqlite3.c
    c_build
        .cpp(false)                   // Disable C++ compilation (compile as C)
        .include(&src_dir)            // Include the source directory
        .warnings(false)              // Disable warnings for cleaner output
        .opt_level(2);                // Optimization level
    
    // Add your specific source files to appropriate builds
    let nanots_cpp = src_dir.join("nanots.cpp");
    let sqlite3_c = src_dir.join("sqlite3.c");
    
    if nanots_cpp.exists() {
        println!("Adding nanots.cpp (C++)");
        cpp_build.file(&nanots_cpp);
    } else {
        panic!("nanots.cpp not found at {}", nanots_cpp.display());
    }
    
    if sqlite3_c.exists() {
        println!("Adding sqlite3.c (C)");
        c_build.file(&sqlite3_c);
    } else {
        panic!("sqlite3.c not found at {}", sqlite3_c.display());
    }
    
    // Add defines to both builds
    for build in [&mut cpp_build, &mut c_build] {
        build.define("NANOTS_BUILD", None);
        
        // SQLite-specific configuration
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
        // GCC/Clang flags for SQLite to suppress warnings
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
    if external_src_dir.exists() {
        println!("cargo:rerun-if-changed={}", external_src_dir.display());
    }
}

// Helper function to copy specific source files
fn copy_dir_all(src: &PathBuf, dst: &PathBuf) -> std::io::Result<()> {
    // List of files we expect to copy
    let expected_files = ["nanots.h", "nanots.cpp", "sqlite3.h", "sqlite3.c"];
    
    for filename in &expected_files {
        let src_path = src.join(filename);
        let dst_path = dst.join(filename);
        
        if src_path.exists() {
            // Only copy if source is newer or destination doesn't exist
            let should_copy = if dst_path.exists() {
                let src_modified = src_path.metadata()?.modified()?;
                let dst_modified = dst_path.metadata()?.modified()?;
                src_modified > dst_modified
            } else {
                true
            };
            
            if should_copy {
                fs::copy(&src_path, &dst_path)?;
                println!("Copied: {}", filename);
            } else {
                println!("Skipped: {} (up to date)", filename);
            }
        } else {
            println!("Warning: {} not found in source directory", filename);
        }
    }
    Ok(())
}
