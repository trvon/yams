# PowerShell wrapper for clang-format (Windows-friendly)

[CmdletBinding()]
param(
    [switch]$Check,
    [switch]$DryRun,
    [switch]$Git,
    [switch]$Serial,
    [switch]$Verbose,
    [Alias("stdin-files")]
    [switch]$StdinFiles
)

$ErrorActionPreference = "Stop"

function Write-Info([string]$Message) {
    Write-Host $Message
}

function Fail([string]$Message) {
    Write-Error $Message
    exit 1
}

if (-not (Get-Command clang-format -ErrorAction SilentlyContinue)) {
    Fail "Error: clang-format not found in PATH"
}

$repoRoot = (git rev-parse --show-toplevel 2>$null)
if (-not $repoRoot) {
    Fail "Error: not inside a git repository"
}

Set-Location $repoRoot

if (-not (Test-Path ".clang-format")) {
    Fail "Error: .clang-format file not found in project root"
}

$clangArgs = @("--style=file")
if ($IsWindows) {
    $clangArgs += "--sort-includes=0"
}

$files = New-Object System.Collections.Generic.List[string]

if ($StdinFiles) {
    foreach ($line in $input) {
        if (-not [string]::IsNullOrWhiteSpace($line)) {
            $files.Add($line.Trim())
        }
    }
} elseif ($Git) {
    $gitFiles = @()
    $gitFiles += (git diff --name-only --diff-filter=ACMR HEAD 2>$null)
    $gitFiles += (git diff --cached --name-only --diff-filter=ACMR 2>$null)
    $gitFiles = $gitFiles | Where-Object { $_ -match '\.(c|cc|cpp|cxx|h|hh|hpp)$' } | Select-Object -Unique
    foreach ($f in $gitFiles) { $files.Add($f) }
} else {
    $gitFiles = (git diff --cached --name-only --diff-filter=ACM 2>$null)
    foreach ($f in $gitFiles) {
        if ($f -match '\.(c|cc|cpp|cxx|h|hh|hpp)$') {
            $files.Add($f)
        }
    }
}

if ($files.Count -eq 0) {
    exit 0
}

if ($Check) {
    $failed = $false
    foreach ($file in $files) {
        & clang-format @clangArgs --dry-run --Werror -- $file
        if ($LASTEXITCODE -ne 0) {
            $failed = $true
        }
    }
    if ($failed) {
        Fail "clang-format check failed. Run 'scripts/format-code.ps1 -Git' to fix formatting."
    }
    exit 0
}

foreach ($file in $files) {
    if (Test-Path $file) {
        & clang-format @clangArgs -i -- $file
        if ($LASTEXITCODE -ne 0) {
            Fail "clang-format failed on $file"
        }
    }
}

if ($DryRun) {
    Write-Info "Dry-run complete."
}

exit 0
