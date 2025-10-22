#!/bin/bash
# Test script to verify SQLite symbols are hidden on macOS

echo "=== Testing Symbol Visibility on macOS ==="
echo ""

# Check if library exists
if [ ! -f "build/libnanots.dylib" ]; then
    echo "Error: build/libnanots.dylib not found. Please build first."
    exit 1
fi

echo "1. Checking for exported nanots symbols (should be visible):"
nm -gU build/libnanots.dylib | grep -i "nanots" | head -20
echo ""

echo "2. Checking for exported sqlite3 symbols (should NOT be visible):"
SQLITE_COUNT=$(nm -gU build/libnanots.dylib | grep -i "sqlite3" | wc -l | tr -d ' ')
if [ "$SQLITE_COUNT" -eq 0 ]; then
    echo "   ✓ SUCCESS: No sqlite3 symbols exported!"
else
    echo "   ✗ FAILURE: Found $SQLITE_COUNT sqlite3 symbols exported:"
    nm -gU build/libnanots.dylib | grep -i "sqlite3" | head -20
fi
echo ""

echo "3. All exported symbols (first 30):"
nm -gU build/libnanots.dylib | head -30
echo ""

echo "4. Symbol counts:"
TOTAL=$(nm -gU build/libnanots.dylib | wc -l | tr -d ' ')
NANOTS=$(nm -gU build/libnanots.dylib | grep -i "nanots" | wc -l | tr -d ' ')
echo "   Total exported: $TOTAL"
echo "   nanots symbols: $NANOTS"
echo "   sqlite3 symbols: $SQLITE_COUNT"
echo ""

echo "5. Checking library dependencies:"
otool -L build/libnanots.dylib
