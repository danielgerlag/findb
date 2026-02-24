<#
.SYNOPSIS
    FinanceDB build script for Windows (PowerShell equivalent of Makefile).

.DESCRIPTION
    Run: .\make.ps1 <target>
    Targets: build, build-release, build-ui, build-all, test, test-all,
             lint, fmt, fmt-check, check, run, run-release, run-ui,
             demo, docker, docker-run, bench, clean, help

.EXAMPLE
    .\make.ps1 demo
    .\make.ps1 check
    .\make.ps1 run -Port 8080
#>
param(
    [Parameter(Position = 0)]
    [string]$Target = "help",

    [int]$Port = 3001
)

$ErrorActionPreference = "Stop"
$Root = $PSScriptRoot

function Invoke-Step([string]$Name, [scriptblock]$Action) {
    Write-Host "`n--- $Name ---" -ForegroundColor Cyan
    & $Action
    if ($LASTEXITCODE -and $LASTEXITCODE -ne 0) {
        Write-Host "FAILED: $Name (exit code $LASTEXITCODE)" -ForegroundColor Red
        exit $LASTEXITCODE
    }
}

# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------

function Target-Help {
    Write-Host ""
    Write-Host "FinanceDB build script" -ForegroundColor Cyan
    Write-Host "Usage: .\make.ps1 <target> [-Port 3001]" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  build            Build the Rust backend (debug)"
    Write-Host "  build-release    Build the Rust backend (release)"
    Write-Host "  build-ui         Build the web UI for production"
    Write-Host "  build-all        Build both backend and UI"
    Write-Host "  test             Run Rust tests"
    Write-Host "  test-all         Run all tests including Postgres"
    Write-Host "  lint             Run clippy linter"
    Write-Host "  fmt              Format Rust code"
    Write-Host "  fmt-check        Check Rust code formatting"
    Write-Host "  check            Run fmt-check + lint + test"
    Write-Host "  run              Run findb server (debug)"
    Write-Host "  run-release      Run findb server (release)"
    Write-Host "  run-ui           Start the Vite dev server"
    Write-Host "  demo             Build and run backend + UI together"
    Write-Host "  docker           Build Docker image"
    Write-Host "  docker-run       Build and run Docker container"
    Write-Host "  bench            Run benchmarks"
    Write-Host "  clean            Remove build artifacts"
    Write-Host ""
}

function Target-Build         { Invoke-Step "build"         { cargo build } }
function Target-BuildRelease  { Invoke-Step "build-release" { cargo build --release } }
function Target-BuildUi {
    Invoke-Step "build-ui" {
        Push-Location "$Root\ui"
        try { npm install; npm run build }
        finally { Pop-Location }
    }
}
function Target-BuildAll      { Target-Build; Target-BuildUi }

function Target-Test          { Invoke-Step "test"     { cargo test } }
function Target-TestAll       { Invoke-Step "test-all" { cargo test -- --include-ignored } }
function Target-Lint          { Invoke-Step "lint"     { cargo clippy --all-targets -- -D warnings } }
function Target-Fmt           { Invoke-Step "fmt"      { cargo fmt --all } }
function Target-FmtCheck      { Invoke-Step "fmt-check"{ cargo fmt --all -- --check } }
function Target-Check         { Target-FmtCheck; Target-Lint; Target-Test }

function Target-Run           { Invoke-Step "run"         { cargo run -- --port $Port } }
function Target-RunRelease    { Invoke-Step "run-release" { cargo run --release -- --port $Port } }
function Target-RunUi {
    Invoke-Step "run-ui" {
        Push-Location "$Root\ui"
        try { npm run dev }
        finally { Pop-Location }
    }
}

function Target-Demo {
    Target-Build
    Target-BuildUi

    Write-Host ""
    Write-Host "============================================" -ForegroundColor Green
    Write-Host "  Starting FinanceDB demo"                    -ForegroundColor Green
    Write-Host "  Backend: http://localhost:$Port"             -ForegroundColor Green
    Write-Host "  UI:      http://localhost:5173"              -ForegroundColor Green
    Write-Host "  Press Ctrl+C to stop"                       -ForegroundColor Green
    Write-Host "============================================" -ForegroundColor Green
    Write-Host ""

    $backend = $null
    $frontend = $null
    try {
        $backend = Start-Process -FilePath "cmd.exe" `
            -ArgumentList "/c","cargo","run","--","--port",$Port `
            -WorkingDirectory $Root `
            -PassThru -NoNewWindow
        Start-Sleep -Seconds 3

        $frontend = Start-Process -FilePath "cmd.exe" `
            -ArgumentList "/c","npm","run","dev" `
            -WorkingDirectory "$Root\ui" `
            -PassThru -NoNewWindow

        Write-Host "Both servers running. Press Ctrl+C to stop..." -ForegroundColor Yellow

        # Wait until either process exits or user presses Ctrl+C
        while ($true) {
            if ($backend.HasExited) {
                Write-Host "Backend exited (code $($backend.ExitCode))" -ForegroundColor Red
                break
            }
            if ($frontend.HasExited) {
                Write-Host "Frontend exited (code $($frontend.ExitCode))" -ForegroundColor Red
                break
            }
            Start-Sleep -Seconds 1
        }
    }
    finally {
        if ($backend -and -not $backend.HasExited) {
            Write-Host "Stopping backend..." -ForegroundColor Gray
            Stop-Process -Id $backend.Id -Force -ErrorAction SilentlyContinue
        }
        if ($frontend -and -not $frontend.HasExited) {
            Write-Host "Stopping frontend..." -ForegroundColor Gray
            Stop-Process -Id $frontend.Id -Force -ErrorAction SilentlyContinue
        }
        # Kill child processes (findb.exe spawned by cargo run)
        $findbProcs = Get-Process -Name "findb" -ErrorAction SilentlyContinue
        foreach ($p in $findbProcs) { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue }
    }
}

function Target-Docker    { Invoke-Step "docker"     { docker build -t findb:latest . } }
function Target-DockerRun { Target-Docker; Invoke-Step "docker-run" { docker run --rm -p "${Port}:3000" findb:latest } }
function Target-Bench     { Invoke-Step "bench"      { cargo bench } }
function Target-Clean {
    Invoke-Step "clean" {
        cargo clean
        if (Test-Path "$Root\ui\node_modules") { Remove-Item -Recurse -Force "$Root\ui\node_modules" }
        if (Test-Path "$Root\ui\dist")         { Remove-Item -Recurse -Force "$Root\ui\dist" }
    }
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

Set-Location $Root

switch ($Target) {
    "help"          { Target-Help }
    "build"         { Target-Build }
    "build-release" { Target-BuildRelease }
    "build-ui"      { Target-BuildUi }
    "build-all"     { Target-BuildAll }
    "test"          { Target-Test }
    "test-all"      { Target-TestAll }
    "lint"          { Target-Lint }
    "fmt"           { Target-Fmt }
    "fmt-check"     { Target-FmtCheck }
    "check"         { Target-Check }
    "run"           { Target-Run }
    "run-release"   { Target-RunRelease }
    "run-ui"        { Target-RunUi }
    "demo"          { Target-Demo }
    "docker"        { Target-Docker }
    "docker-run"    { Target-DockerRun }
    "bench"         { Target-Bench }
    "clean"         { Target-Clean }
    default {
        Write-Host "Unknown target: $Target" -ForegroundColor Red
        Target-Help
        exit 1
    }
}
