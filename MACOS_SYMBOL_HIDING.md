# macOS Symbol Hiding Implementation

## The Problem

The initial implementation used GNU ld's `--version-script` option, which doesn't work on macOS:

```
ld: unknown options: --version-script=/Users/runner/work/nanots/nanots/nanots.version
```

macOS uses Apple's linker (ld64), which has different command-line options.

## The Solution

macOS uses `-exported_symbols_list` instead of `--version-script`:

### 1. Created `nanots.exports`

This file lists the symbols to export (wildcards supported):

```
# macOS exported symbols list
_nanots_*
*nanots_writer*
*nanots_reader*
*nanots_iterator*
# ... etc
```

**Note**: macOS C symbols have a leading underscore `_` due to the C calling convention.

### 2. Updated CMakeLists.txt

Added platform detection to use the correct linker script:

```cmake
if(UNIX)
    target_compile_options(nanots PRIVATE -fvisibility=hidden)

    if(APPLE)
        # macOS uses -exported_symbols_list
        set_target_properties(nanots PROPERTIES
            LINK_FLAGS "-Wl,-exported_symbols_list,${CMAKE_CURRENT_SOURCE_DIR}/nanots.exports"
        )
    else()
        # Linux uses --version-script
        set_target_properties(nanots PROPERTIES
            LINK_FLAGS "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/nanots.version"
        )
    endif()
endif()
```

## Differences Between Platforms

| Feature | Linux (GNU ld) | macOS (ld64) |
|---------|---------------|--------------|
| **Linker script flag** | `--version-script` | `-exported_symbols_list` |
| **File format** | Version script syntax | Plain list with wildcards |
| **Symbol names** | As-is | C symbols get `_` prefix |
| **Wildcards** | `nanots_*` | `_nanots_*` (for C) |
| **Version info** | Supports symbol versioning | No versioning support |

## Symbol Name Mangling on macOS

### C Functions
C functions get a leading underscore:
- Source: `nanots_writer_create`
- Symbol: `_nanots_writer_create`
- Export pattern: `_nanots_*`

### C++ Functions
C++ functions use name mangling (no leading underscore added):
- Source: `nanots_writer::write()`
- Symbol: `_ZN13nanots_writer5writeEv` (or similar)
- Export pattern: `*nanots_writer*` (matches anywhere in mangled name)

## Testing on macOS

### Using the test script:

```bash
chmod +x test_symbols.sh
./test_symbols.sh
```

The script auto-detects macOS and uses appropriate tools.

### Manual verification:

```bash
# List all exported symbols (undefined or global)
nm -gU build/libnanots.dylib

# Check for sqlite3 symbols (should be empty)
nm -gU build/libnanots.dylib | grep sqlite3

# Check for nanots symbols (should have many)
nm -gU build/libnanots.dylib | grep nanots

# Check library dependencies
otool -L build/libnanots.dylib
```

## Key Points

1. **Different linker**: macOS doesn't use GNU ld, so GNU ld options don't work
2. **Different syntax**: Export lists are simpler than version scripts
3. **Symbol prefixes**: C functions have `_` prefix on macOS
4. **Same result**: SQLite symbols are still hidden, nanots symbols are visible
5. **Same visibility flags**: `-fvisibility=hidden` works the same way

## Benefits of This Approach

1. **Cross-platform**: Same code works on Linux and macOS
2. **Maintainable**: Platform detection is automatic via CMake
3. **Consistent**: Both platforms achieve the same goal
4. **Native**: Uses each platform's preferred method

## Further Reading

- [Apple's ld man page](https://www.unix.com/man-page/osx/1/ld/)
- [GNU ld version scripts](https://www.gnu.org/software/gnulib/manual/html_node/LD-Version-Scripts.html)
- [Symbol visibility](https://gcc.gnu.org/wiki/Visibility)
