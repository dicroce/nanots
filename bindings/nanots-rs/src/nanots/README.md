# Auto-copied Sources

The C++ source files in this directory are automatically copied from `../../amalgamated_src/` during the build process.

**Do not edit files here directly** - they will be overwritten!

Edit the original files in `../../amalgamated_src/` instead.

## How it works

1. During `cargo build`, the `build.rs` script checks for sources at `../../amalgamated_src/`
2. If found, it copies them to this directory (only if they're newer)
3. If not found, it uses the sources already in this directory (for crates.io builds)

This allows local development with external sources while still supporting crates.io publishing.
