<#
.SYNOPSIS
    Generates winget manifest files for YAMS

.DESCRIPTION
    Creates the required YAML manifest files for submitting YAMS to the
    Windows Package Manager (winget) community repository.

    Winget manifests consist of:
    - Version manifest (.yaml) - package version info
    - Installer manifest (.installer.yaml) - download URLs and hashes
    - Locale manifest (.locale.en-US.yaml) - descriptions and metadata

    NOTE: Winget only accepts stable releases with semantic versions (X.Y.Z).
    Nightly/weekly releases (nightly-YYYYMMDD-hash, weekly-YYYYwWW-hash) are
    not submitted to winget.

.PARAMETER Version
    The version string from release workflow (e.g., 0.1.0, nightly-20251127-abc1234)
    For stable releases, this should be semantic version (X.Y.Z)

.PARAMETER Tag
    The git tag for the release (e.g., v0.1.0, nightly-20251127-abc1234)
    Used to construct the download URL

.PARAMETER MsiPath
    Path to the MSI file (to compute SHA256 hash)

.PARAMETER GitHubRepo
    GitHub repository in owner/repo format
    Default: serviteur/yams

.PARAMETER OutputDir
    Directory where manifest files will be created
    Default: .\winget-manifests

.PARAMETER Publisher
    Publisher name for the manifest
    Default: YAMS Project

.PARAMETER PackageIdentifier
    Winget package identifier (Publisher.PackageName format)
    Default: YAMSProject.YAMS

.PARAMETER Force
    Generate manifest even for non-stable versions (for testing only)

.EXAMPLE
    # Stable release
    .\generate-winget-manifest.ps1 -Version 0.1.0 -Tag v0.1.0 -MsiPath .\yams-0.1.0-windows-x86_64.msi

.EXAMPLE
    # Local testing with nightly (use -Force)
    .\generate-winget-manifest.ps1 -Version nightly-20251127-abc1234 -Tag nightly-20251127-abc1234 -MsiPath .\yams-nightly-windows-x86_64.msi -Force

.NOTES
    After generating, submit to: https://github.com/microsoft/winget-pkgs
    Use: winget validate --manifest <path>
    Test: winget install --manifest <path>
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$Version,

    [Parameter(Mandatory=$true)]
    [string]$Tag,

    [Parameter(Mandatory=$true)]
    [string]$MsiPath,

    [string]$GitHubRepo = "serviteur/yams",

    [string]$OutputDir = ".\winget-manifests",

    [string]$Publisher = "YAMS Project",

    [string]$PackageIdentifier = "YAMSProject.YAMS",

    [switch]$Force
)

$ErrorActionPreference = "Stop"

# Check if this is a stable release (semantic version X.Y.Z)
$isStable = $Version -match '^\d+\.\d+\.\d+$'

if (-not $isStable -and -not $Force) {
    Write-Host "Version '$Version' is not a stable semantic version (X.Y.Z)." -ForegroundColor Yellow
    Write-Host "Winget only accepts stable releases. Use -Force to generate anyway (for testing)." -ForegroundColor Yellow
    Write-Host "Skipping winget manifest generation." -ForegroundColor Yellow
    exit 0
}

if (-not $isStable) {
    Write-Warning "Generating winget manifest for non-stable version (testing mode)"
}

# Validate MSI exists
if (-not (Test-Path $MsiPath)) {
    Write-Error "MSI file not found: $MsiPath"
    exit 1
}

# Compute SHA256 hash
Write-Host "Computing SHA256 hash for: $MsiPath" -ForegroundColor Cyan
$hash = (Get-FileHash -Path $MsiPath -Algorithm SHA256).Hash
Write-Host "SHA256: $hash" -ForegroundColor Green

# Construct MSI download URL
$msiFileName = "yams-$Version-windows-x86_64.msi"
$MsiUrl = "https://github.com/$GitHubRepo/releases/download/$Tag/$msiFileName"
Write-Host "MSI URL: $MsiUrl" -ForegroundColor Cyan

# Create output directory structure (winget requires Publisher/PackageName/Version/)
$manifestDir = Join-Path $OutputDir "$($PackageIdentifier.Replace('.', '/'))\$Version"
New-Item -ItemType Directory -Force -Path $manifestDir | Out-Null
Write-Host "Creating manifests in: $manifestDir" -ForegroundColor Cyan

# Manifest schema version
$manifestVersion = "1.6.0"

# Version manifest
$versionManifest = @"
# yaml-language-server: `$schema=https://aka.ms/winget-manifest.version.1.6.0.schema.json
PackageIdentifier: $PackageIdentifier
PackageVersion: $Version
DefaultLocale: en-US
ManifestType: version
ManifestVersion: $manifestVersion
"@

# Installer manifest
$installerManifest = @"
# yaml-language-server: `$schema=https://aka.ms/winget-manifest.installer.1.6.0.schema.json
PackageIdentifier: $PackageIdentifier
PackageVersion: $Version
Platform:
  - Windows.Desktop
MinimumOSVersion: 10.0.17763.0
InstallerType: msi
Scope: machine
InstallModes:
  - interactive
  - silent
  - silentWithProgress
UpgradeBehavior: install
Commands:
  - yams
Installers:
  - Architecture: x64
    InstallerUrl: $MsiUrl
    InstallerSha256: $hash
    ProductCode: '{69FC51D5-B9FC-4DA9-9CA3-C889EDF8AFB8}'
ManifestType: installer
ManifestVersion: $manifestVersion
"@

# Locale manifest (en-US)
$localeManifest = @"
# yaml-language-server: `$schema=https://aka.ms/winget-manifest.defaultLocale.1.6.0.schema.json
PackageIdentifier: $PackageIdentifier
PackageVersion: $Version
PackageLocale: en-US
Publisher: $Publisher
PublisherUrl: https://github.com/$GitHubRepo
PublisherSupportUrl: https://github.com/$GitHubRepo/issues
PackageName: YAMS
PackageUrl: https://github.com/$GitHubRepo
License: GPL-3.0
LicenseUrl: https://github.com/$GitHubRepo/blob/main/LICENSE
ShortDescription: Yet Another Memory System - Content-addressable storage and semantic search
Description: |
  YAMS is a high-performance content-addressable storage system with semantic search capabilities.
  Features include:
  - Content-addressable storage with deduplication
  - Full-text and semantic search
  - Git-like versioning and snapshots
  - Plugin system for extensibility
  - MCP server for AI assistant integration
Tags:
  - storage
  - search
  - semantic
  - content-addressable
  - cli
  - developer-tools
ManifestType: defaultLocale
ManifestVersion: $manifestVersion
"@

# Write manifest files
$versionFile = Join-Path $manifestDir "$PackageIdentifier.yaml"
$installerFile = Join-Path $manifestDir "$PackageIdentifier.installer.yaml"
$localeFile = Join-Path $manifestDir "$PackageIdentifier.locale.en-US.yaml"

# Write files as UTF-8 without BOM (required by winget)
# PowerShell 5.x doesn't support utf8NoBOM, so use .NET directly
[System.IO.File]::WriteAllText($versionFile, $versionManifest, [System.Text.UTF8Encoding]::new($false))
[System.IO.File]::WriteAllText($installerFile, $installerManifest, [System.Text.UTF8Encoding]::new($false))
[System.IO.File]::WriteAllText($localeFile, $localeManifest, [System.Text.UTF8Encoding]::new($false))

Write-Host "`nGenerated winget manifest files:" -ForegroundColor Green
Write-Host "  $versionFile" -ForegroundColor Gray
Write-Host "  $installerFile" -ForegroundColor Gray
Write-Host "  $localeFile" -ForegroundColor Gray

# Validate if winget is available
$wingetAvailable = Get-Command winget -ErrorAction SilentlyContinue
if ($wingetAvailable) {
    Write-Host "`nValidating manifest..." -ForegroundColor Cyan
    & winget validate --manifest $manifestDir
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Manifest validation passed!" -ForegroundColor Green
    } else {
        Write-Warning "Manifest validation failed. Please review the files."
    }

    Write-Host "`nTo test installation locally:" -ForegroundColor Cyan
    Write-Host "  winget install --manifest $manifestDir" -ForegroundColor White
} else {
    Write-Host "`nwinget not found. To validate manually:" -ForegroundColor Yellow
    Write-Host "  winget validate --manifest $manifestDir" -ForegroundColor White
}

Write-Host "`nTo submit to winget-pkgs repository:" -ForegroundColor Cyan
Write-Host "  1. Fork https://github.com/microsoft/winget-pkgs" -ForegroundColor White
Write-Host "  2. Copy manifests to: manifests/y/YAMSProject/YAMS/$Version/" -ForegroundColor White
Write-Host "  3. Create a pull request" -ForegroundColor White
Write-Host "`nOr use wingetcreate:" -ForegroundColor Cyan
Write-Host "  wingetcreate submit $manifestDir" -ForegroundColor White

# Explicitly exit with success
exit 0
