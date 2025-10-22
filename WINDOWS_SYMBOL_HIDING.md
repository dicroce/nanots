# How Symbol Hiding Works on Windows

## TL;DR

**Yes, the symbol hiding works on Windows!** In fact, it works *automatically* due to how Windows DLLs work.

## Windows DLL Export Model

Windows uses an **explicit export model**:
- By default, **nothing** is exported from a DLL
- Only symbols marked with `__declspec(dllexport)` are visible
- This is the **opposite** of Linux, where everything is exported by default

## How nanots Handles This

### On Windows:

```cpp
// From utils.h when NANOTS_BUILDING_DLL is defined:
#define NANOTS_API __declspec(dllexport)

// Applied to nanots functions:
class NANOTS_API nanots_writer { ... };  // EXPORTED ✓

// SQLite functions have no such marking:
int sqlite3_open(...) { ... }             // NOT EXPORTED ✓
```

### Result:
- **nanots symbols**: Marked with `NANOTS_API` → `__declspec(dllexport)` → **EXPORTED** ✓
- **SQLite symbols**: No marking → **HIDDEN** ✓

## Comparison with Other Platforms

| Aspect | Linux | macOS | Windows |
|--------|-------|-------|---------|
| **Default** | All exported | All exported | Nothing exported |
| **To Export** | `visibility("default")` + version script | `visibility("default")` + export list | `__declspec(dllexport)` |
| **To Hide** | `-fvisibility=hidden` | `-fvisibility=hidden` | Do nothing (default) |
| **SQLite** | Needs explicit hiding | Needs explicit hiding | Hidden automatically |

## Why We Still Define SQLITE_API=

Even though Windows doesn't need it, we set `SQLITE_API=` (empty) on all platforms:

```cmake
target_compile_definitions(nanots PRIVATE SQLITE_API=)
```

This is defensive programming:
1. **Consistency**: Same approach across platforms
2. **Safety**: Prevents any future SQLite headers from trying to export symbols
3. **Documentation**: Makes the intent clear in the code

## Testing on Windows

Run the provided PowerShell script:

```powershell
.\test_symbols.ps1
```

This uses `dumpbin /EXPORTS` to verify:
1. nanots symbols are exported (✓ good)
2. sqlite3 symbols are NOT exported (✓ good)

## Summary

On Windows, SQLite symbols are **automatically private** because:
1. Windows DLLs hide everything by default
2. SQLite functions don't have `__declspec(dllexport)`
3. Only nanots symbols (marked with `NANOTS_API`) are exported

The existing code already handles this correctly - no special Windows-specific code needed beyond the standard `NANOTS_API` macro!
