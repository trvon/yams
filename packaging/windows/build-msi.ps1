<#
.SYNOPSIS
    Builds YAMS MSI installer using WiX Toolset v4+

.DESCRIPTION
    This script builds an MSI installer for YAMS from the meson install output.
    It requires WiX Toolset v4+ to be installed via: dotnet tool install --global wix

.PARAMETER StageDir
    Path to the meson install --destdir output directory.
    Default: .\build\release\stage

.PARAMETER Version
    Product version string (e.g., 0.1.0 or nightly-20251126-abc1234).
    Default: extracted from meson.build or "0.0.0"

.PARAMETER OutputDir
    Directory where the MSI will be created.
    Default: .\build\release

.EXAMPLE
    .\build-msi.ps1 -StageDir .\build\release\stage -Version 0.1.0
#>

param(
    [string]$StageDir = ".\build\release\stage",
    [string]$Version = "",
    [string]$OutputDir = ".\build\release"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Get-Item "$ScriptDir\..\..").FullName

# If StageDir doesn't exist, try to create it via meson install
if (-not (Test-Path $StageDir)) {
    Write-Host "Stage directory not found: $StageDir" -ForegroundColor Yellow

    # Try to find the build directory
    $buildDir = Split-Path $StageDir -Parent
    if (-not (Test-Path (Join-Path $buildDir "build.ninja"))) {
        # Default to build/release
        $buildDir = Join-Path $RepoRoot "build\release"
    }

    if (Test-Path (Join-Path $buildDir "build.ninja")) {
        Write-Host "Found build at: $buildDir" -ForegroundColor Cyan

        # Reconfigure with root prefix for clean MSI staging
        Write-Host "Reconfiguring meson with root prefix for MSI staging..." -ForegroundColor Cyan
        & meson configure $buildDir --prefix=/
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "meson configure failed, continuing with current prefix..."
        }

        Write-Host "Running meson install to create stage directory..." -ForegroundColor Cyan
        $stageParent = Split-Path $StageDir -Parent
        if (-not (Test-Path $stageParent)) {
            New-Item -ItemType Directory -Path $stageParent -Force | Out-Null
        }

        & meson install -C $buildDir --destdir $StageDir
        if ($LASTEXITCODE -ne 0) {
            Write-Error "meson install failed. Please build first: .\setup.ps1 -BuildType Release"
            exit 1
        }
    } else {
        Write-Error @"
Stage directory not found and no build available.

Please either:
1. Build first:     .\setup.ps1 -BuildType Release
2. Then package:    .\setup.ps1 -BuildType Release -Package

Or manually:
1. Build:           meson compile -C build/release
2. Stage:           meson install -C build/release --destdir build/release/stage
3. Package:         .\packaging\windows\build-msi.ps1 -StageDir build/release/stage
"@
        exit 1
    }
}

# Verify WiX is installed
Write-Host "Checking for WiX Toolset..." -ForegroundColor Cyan
$wixPath = Get-Command wix -ErrorAction SilentlyContinue
if (-not $wixPath) {
    Write-Host "WiX Toolset not found. Installing via dotnet tool..." -ForegroundColor Yellow
    & dotnet tool install --global wix
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to install WiX Toolset. Please install manually: dotnet tool install --global wix"
        exit 1
    }
    # Refresh PATH
    $env:PATH = [System.Environment]::GetEnvironmentVariable("PATH", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("PATH", "User")
}

# Verify WiX version
Write-Host "WiX version:" -ForegroundColor Cyan
& wix --version

# Add WiX UI extension
Write-Host "Adding WiX UI extension..." -ForegroundColor Cyan
& wix extension add -g WixToolset.UI.wixext
if ($LASTEXITCODE -ne 0) {
    Write-Warning "WiX UI extension may already be installed, continuing..."
}

# Resolve stage directory
$StageDir = (Resolve-Path $StageDir -ErrorAction Stop).Path
Write-Host "Stage directory: $StageDir" -ForegroundColor Cyan

# Find yams.exe anywhere in the stage directory
# meson install with --destdir prepends destdir to prefix, creating nested paths
Write-Host "Searching for yams.exe in stage directory..." -ForegroundColor Cyan
$yamsExeFiles = Get-ChildItem -Path $StageDir -Recurse -Filter "yams.exe" -ErrorAction SilentlyContinue

if ($yamsExeFiles.Count -eq 0) {
    Write-Error "yams.exe not found anywhere in stage directory: $StageDir"
    exit 1
}

# Use the first found yams.exe and derive the bin directory
$yamsBin = $yamsExeFiles[0].FullName
$binDir = Split-Path $yamsBin -Parent

# The "effective stage root" is the parent of bin/
$EffectiveStageDir = Split-Path $binDir -Parent

Write-Host "Found yams.exe at: $yamsBin" -ForegroundColor Green
Write-Host "Effective stage root: $EffectiveStageDir" -ForegroundColor Cyan

# Use EffectiveStageDir for WiX (it contains bin/yams.exe)
$StageDir = $EffectiveStageDir

# Create LICENSE.rtf for WiX installer (WiX requires RTF format)
$licenseSource = Join-Path $RepoRoot "LICENSE"
$licenseRtf = Join-Path $StageDir "LICENSE.rtf"
if (Test-Path $licenseSource) {
    Write-Host "Converting LICENSE to RTF format..." -ForegroundColor Cyan
    $licenseText = Get-Content $licenseSource -Raw
    # Simple RTF wrapper - escape special chars and wrap in RTF
    $licenseText = $licenseText -replace '\\', '\\\\' -replace '\{', '\{' -replace '\}', '\}'
    $rtfContent = "{\rtf1\ansi\deff0{\fonttbl{\f0 Consolas;}}\f0\fs18 " + ($licenseText -replace "`r`n", "\par`r`n" -replace "`n", "\par`r`n") + "}"
    $rtfContent | Out-File -FilePath $licenseRtf -Encoding ascii
    Write-Host "License RTF created at: $licenseRtf" -ForegroundColor Cyan
} else {
    Write-Warning "LICENSE file not found at: $licenseSource"
}

# Extract version if not provided
if ([string]::IsNullOrEmpty($Version)) {
    $mesonBuild = Join-Path $RepoRoot "meson.build"
    if (Test-Path $mesonBuild) {
        $content = Get-Content $mesonBuild -Raw
        if ($content -match "version\s*:\s*'([^']+)'") {
            $Version = $matches[1]
            Write-Host "Extracted version from meson.build: $Version" -ForegroundColor Cyan
        }
    }
    if ([string]::IsNullOrEmpty($Version)) {
        $Version = "0.0.0"
        Write-Host "Using default version: $Version" -ForegroundColor Yellow
    }
}

# Normalize version for MSI (must be X.Y.Z.W format)
# Handle nightly-YYYYMMDD-hash format
if ($Version -match "nightly-(\d{4})(\d{2})(\d{2})-") {
    $year = [int]$matches[1] - 2020  # Offset from 2020
    $month = [int]$matches[2]
    $day = [int]$matches[3]
    $MsiVersion = "$year.$month.$day.0"
    Write-Host "Normalized nightly version to MSI format: $MsiVersion" -ForegroundColor Cyan
} elseif ($Version -match "^(\d+)\.(\d+)\.(\d+)") {
    $MsiVersion = "$($matches[1]).$($matches[2]).$($matches[3]).0"
} else {
    $MsiVersion = "0.0.0.0"
    Write-Host "Could not parse version, using: $MsiVersion" -ForegroundColor Yellow
}

# Create output directory
$OutputDir = (New-Item -ItemType Directory -Force -Path $OutputDir).FullName
$MsiName = "yams-$Version-windows-x86_64.msi"
$MsiPath = Join-Path $OutputDir $MsiName

Write-Host "`nBuilding MSI..." -ForegroundColor Cyan
Write-Host "  Version: $Version (MSI: $MsiVersion)" -ForegroundColor Gray
Write-Host "  Stage:   $StageDir" -ForegroundColor Gray
Write-Host "  Output:  $MsiPath" -ForegroundColor Gray

# Build MSI using WiX
$wxsPath = Join-Path $ScriptDir "yams.wxs"
& wix build $wxsPath `
    -o $MsiPath `
    -d Version=$MsiVersion `
    -d StageDir=$StageDir `
    -ext WixToolset.UI.wixext

if ($LASTEXITCODE -ne 0) {
    Write-Error "WiX build failed with exit code $LASTEXITCODE"
    exit 1
}

# Verify MSI was created
if (Test-Path $MsiPath) {
    $msiSize = (Get-Item $MsiPath).Length / 1MB
    Write-Host "`nMSI created successfully!" -ForegroundColor Green
    Write-Host "  Path: $MsiPath" -ForegroundColor Gray
    Write-Host "  Size: $([math]::Round($msiSize, 2)) MB" -ForegroundColor Gray

    # Generate SHA256 hash
    $hash = (Get-FileHash -Path $MsiPath -Algorithm SHA256).Hash.ToLower()
    Write-Host "  SHA256: $hash" -ForegroundColor Gray

    # Write hash to file
    $hashFile = "$MsiPath.sha256"
    "$hash  $MsiName" | Out-File -FilePath $hashFile -Encoding utf8 -NoNewline
    Write-Host "  Hash file: $hashFile" -ForegroundColor Gray
} else {
    Write-Error "MSI file was not created at expected path: $MsiPath"
    exit 1
}

Write-Host "`nTo install:" -ForegroundColor Cyan
Write-Host "  msiexec /i `"$MsiPath`"" -ForegroundColor White
Write-Host "`nTo uninstall:" -ForegroundColor Cyan
Write-Host "  msiexec /x `"$MsiPath`"" -ForegroundColor White
