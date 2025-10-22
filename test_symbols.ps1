# PowerShell script to test symbol visibility on Windows

Write-Host "=== Testing Symbol Visibility on Windows ===" -ForegroundColor Cyan
Write-Host ""

$dllPath = "build\Release\nanots.dll"
if (-not (Test-Path $dllPath)) {
    $dllPath = "build\Debug\nanots.dll"
}

if (-not (Test-Path $dllPath)) {
    Write-Host "Error: nanots.dll not found in build/Release or build/Debug" -ForegroundColor Red
    Write-Host "Please build the project first." -ForegroundColor Red
    exit 1
}

Write-Host "Testing DLL: $dllPath" -ForegroundColor Green
Write-Host ""

# Use dumpbin to check exports (requires Visual Studio)
$dumpbin = "dumpbin.exe"

# Try to find dumpbin in common Visual Studio locations
$vsWhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
if (Test-Path $vsWhere) {
    $vsPath = & $vsWhere -latest -property installationPath
    if ($vsPath) {
        $dumpbin = Join-Path $vsPath "VC\Tools\MSVC\*\bin\Hostx64\x64\dumpbin.exe"
        $dumpbin = (Get-Item $dumpbin | Select-Object -First 1).FullName
    }
}

if (-not (Get-Command $dumpbin -ErrorAction SilentlyContinue)) {
    Write-Host "Warning: dumpbin.exe not found. Please run from Visual Studio Developer Command Prompt." -ForegroundColor Yellow
    Write-Host "Or use: dumpbin /EXPORTS $dllPath" -ForegroundColor Yellow
    exit 1
}

Write-Host "1. Checking for exported nanots symbols (should be visible):" -ForegroundColor Yellow
$exports = & $dumpbin /EXPORTS $dllPath | Select-String "nanots"
if ($exports) {
    Write-Host "   Found nanots symbols:" -ForegroundColor Green
    $exports | Select-Object -First 20
} else {
    Write-Host "   WARNING: No nanots symbols found!" -ForegroundColor Red
}
Write-Host ""

Write-Host "2. Checking for exported sqlite3 symbols (should NOT be visible):" -ForegroundColor Yellow
$sqliteExports = & $dumpbin /EXPORTS $dllPath | Select-String "sqlite3"
if ($sqliteExports) {
    $count = ($sqliteExports | Measure-Object).Count
    Write-Host "   FAILURE: Found $count sqlite3 symbols exported:" -ForegroundColor Red
    $sqliteExports | Select-Object -First 20
} else {
    Write-Host "   SUCCESS: No sqlite3 symbols exported!" -ForegroundColor Green
}
Write-Host ""

Write-Host "3. All exported functions:" -ForegroundColor Yellow
& $dumpbin /EXPORTS $dllPath | Select-String "^\s+\d+\s+\w+\s+[0-9A-F]+\s+" | Select-Object -First 30
