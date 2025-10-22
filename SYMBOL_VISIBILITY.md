# Symbol Visibility - Hiding SQLite Symbols

This document explains how nanots hides SQLite symbols to prevent conflicts when the library is used in applications that may also link against SQLite.

## Problem

nanots includes SQLite as an embedded dependency. If SQLite symbols are exported from the nanots shared library, they can conflict with:
1. Other versions of SQLite used by the application
2. Other libraries that embed SQLite
3. System-installed SQLite libraries

## Solution

We use two complementary approaches to hide SQLite symbols:

### 1. Linker Export Control

Different platforms use different mechanisms:

**Linux** - Version script (`nanots.version`):
```
{
  global:
    # Export only nanots symbols
    nanots_*;
    *nanots_writer*;
    *nanots_reader*;
    # ... etc

  local:
    # Hide everything else (including SQLite)
    *;
};
```
Applied via: `-Wl,--version-script=nanots.version`

**macOS** - Exported symbols list (`nanots.exports`):
```
# Export only nanots symbols
_nanots_*
*nanots_writer*
*nanots_reader*
# ... etc
```
Applied via: `-Wl,-exported_symbols_list,nanots.exports`

Note: macOS symbol names have a leading underscore `_` due to C calling convention.

**Windows** - Uses `__declspec(dllexport)` (no linker script needed)

### 2. Visibility Attributes

We compile with `-fvisibility=hidden` by default on Unix platforms, which makes all symbols hidden unless explicitly marked otherwise.

The `NANOTS_API` macro marks nanots symbols as visible:
```c++
#define NANOTS_API __attribute__((visibility("default")))
```

SQLite symbols have no such marking and remain hidden.

### 3. SQLITE_API Override

We define `SQLITE_API=` (empty) to remove any export declarations from SQLite headers.

## Implementation Details

### CMakeLists.txt

```cmake
# Make SQLite symbols internal/private on all platforms
target_compile_definitions(nanots PRIVATE SQLITE_API=)

if(UNIX)
    # Hide all symbols by default
    target_compile_options(nanots PRIVATE -fvisibility=hidden)

    # Platform-specific linker scripts
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

### utils.h

```c++
#if defined(__GNUC__) || defined(__clang__)
  #define NANOTS_API __attribute__((visibility("default")))
#endif
```

## Testing

Use the provided test scripts to verify symbol visibility:

### Linux/Unix

```bash
chmod +x test_symbols.sh
./test_symbols.sh
```

This will:
1. Show exported nanots symbols (should be present)
2. Check for SQLite symbols (should be absent)
3. Display all exported symbols
4. Show version script information

### macOS

```bash
chmod +x test_symbols.sh
./test_symbols.sh
```

Or manually:
```bash
nm -gU libnanots.dylib | grep sqlite3
# Should show: (no output = success)

nm -gU libnanots.dylib | grep nanots
# Should show: (many exported symbols)
```

### Windows

Use the provided PowerShell test script (from Visual Studio Developer Command Prompt):
```powershell
.\test_symbols.ps1
```

Or manually verify using dumpbin:
```cmd
dumpbin /EXPORTS nanots.dll | findstr sqlite3
REM Should show: (no output = success)

dumpbin /EXPORTS nanots.dll | findstr nanots
REM Should show: (many exported symbols)
```

## Platform Support

### Linux/Unix
- Uses linker version scripts (`nanots.version`) to explicitly control exports
- Uses `-fvisibility=hidden` to hide symbols by default
- `NANOTS_API` with `visibility("default")` exports only nanots symbols

### macOS
- Uses exported symbols list (`nanots.exports`) instead of version scripts
- Uses `-fvisibility=hidden` to hide symbols by default
- macOS linker doesn't support `--version-script` (GNU ld specific)
- Symbol names have leading underscore due to C calling convention

### Windows
- Uses explicit export model: only `__declspec(dllexport)` symbols are exported
- SQLite symbols are **automatically hidden** (no dllexport marking)
- `NANOTS_API` becomes `__declspec(dllexport)` for nanots symbols only
- No additional work needed - Windows DLLs hide symbols by default!

## Benefits

1. **No symbol conflicts**: Applications can use their own SQLite version
2. **Cleaner API**: Only nanots symbols are visible
3. **Better encapsulation**: SQLite is truly an implementation detail
4. **Smaller symbol table**: Fewer exported symbols means faster dynamic linking

## Troubleshooting

### macOS: "ld: unknown options: --version-script"

This means the build is trying to use Linux-style version scripts on macOS. Make sure:
1. CMake correctly detects `APPLE` platform
2. The `if(APPLE)` branch in CMakeLists.txt is being used
3. The `nanots.exports` file exists

### Linux: Symbols still visible

Check:
1. `-fvisibility=hidden` is in compile flags: `cmake --build . --verbose`
2. Version script is being applied: check linker command line
3. `NANOTS_API` is properly defined in headers

### Windows: nanots symbols not visible

Check:
1. `NANOTS_BUILDING_DLL` is defined when building the DLL
2. `NANOTS_API` expands to `__declspec(dllexport)`
3. Headers are using `NANOTS_API` on all public classes/functions
