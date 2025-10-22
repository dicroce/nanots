#!/bin/bash
# Test script to verify SQLite symbols are hidden (Linux and macOS)

echo "=== Testing Symbol Visibility ==="
echo ""

# Detect platform and set library path
if [[ "$OSTYPE" == "darwin"* ]]; then
    PLATFORM="macOS"
    LIB_PATH="build/libnanots.dylib"
    NM_FLAGS="-gU"
else
    PLATFORM="Linux"
    LIB_PATH="build/libnanots.so"
    NM_FLAGS="-D"
fi

echo "Platform: $PLATFORM"
echo "Library: $LIB_PATH"
echo ""

# Check if library exists
if [ ! -f "$LIB_PATH" ]; then
    echo "Error: $LIB_PATH not found. Please build first."
    exit 1
fi

echo "1. Checking for exported nanots symbols (should be visible):"
nm $NM_FLAGS "$LIB_PATH" | grep -i "nanots" | head -20
echo ""

echo "2. Checking for exported sqlite3 symbols (should NOT be visible):"
SQLITE_COUNT=$(nm $NM_FLAGS "$LIB_PATH" | grep -i "sqlite3" | wc -l | tr -d ' ')
if [ "$SQLITE_COUNT" -eq 0 ]; then
    echo "   ✓ SUCCESS: No sqlite3 symbols exported!"
else
    echo "   ✗ FAILURE: Found $SQLITE_COUNT sqlite3 symbols exported:"
    nm $NM_FLAGS "$LIB_PATH" | grep -i "sqlite3" | head -20
fi
echo ""

echo "3. All exported symbols (first 30):"
if [[ "$OSTYPE" == "darwin"* ]]; then
    nm $NM_FLAGS "$LIB_PATH" | head -30
else
    nm $NM_FLAGS "$LIB_PATH" | grep " T " | head -30
fi
echo ""

echo "4. Platform-specific info:"
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "Library dependencies:"
    otool -L "$LIB_PATH"
else
    echo "Version script info:"
    readelf -V "$LIB_PATH"
fi
