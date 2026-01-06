Param(
    [ValidateSet('Debug','Release','Profiling','Fuzzing')]
    [string]$BuildType = 'Release',

    [switch]$Package,

    [string]$Version = ''
)

$ErrorActionPreference = 'Stop'
$forceVs2022Toolset = $false

# Initialize Visual Studio environment if not already set
if (-not $env:VSINSTALLDIR -or -not (Get-Command cl.exe -ErrorAction SilentlyContinue)) {
    Write-Host 'Visual Studio environment not detected, initializing...'
    
    # Find Visual Studio installation
    $vswhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
    if (-not (Test-Path $vswhere)) {
        $vswhere = "$env:ProgramFiles\Microsoft Visual Studio\Installer\vswhere.exe"
    }
    
    if (Test-Path $vswhere) {
        # Prefer VS 2022 (version 17) if available, as it's the stable target for this project
        $vsPath = & $vswhere -version "[17.0,18.0)" -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
        
        if (-not $vsPath) {
            # Fallback to latest (likely VS 2025)
            $vsPath = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
        }

        if ($vsPath) {
            $vsDevCmd = Join-Path $vsPath "Common7\Tools\VsDevCmd.bat"
            if (Test-Path $vsDevCmd) {
                Write-Host "Found Visual Studio at: $vsPath"
                
                $toolsetArg = ""
                $forceVs2022Toolset = $false
                
                # Check for VS 2025 (v18.x) and auto-downgrade to v143 (VS 2022) if available
                # This is required because Boost 1.85/1.86 build system (b2) fails with VS 2025 toolset (v145)
                $vsInstallVersion = & $vswhere -path $vsPath -property installationVersion
                $compatMsvcVersion = '193' # Default to 193 if forcing compatibility
                
                if ($vsInstallVersion -match '^18\.') {
                    Write-Host "VS 2025 detected (Version $vsInstallVersion)"
                    # Check if v143 toolset is installed
                    $hasV143 = & $vswhere -path $vsPath -requires Microsoft.VisualStudio.Component.VC.v143.x86.x64
                    
                    # Check if v144 toolset is installed (VS 2022 17.10+) - often present in VS 2025 Preview
                    $vcToolsPath = Join-Path $vsPath "VC\Tools\MSVC"
                    $v144Dir = $null
                    if (Test-Path $vcToolsPath) {
                        $v144Dir = Get-ChildItem -Path $vcToolsPath -Filter "14.4*" -Directory | Sort-Object Name -Descending | Select-Object -First 1
                    }

                    if ($hasV143) {
                        # Try to find the exact version directory to be more robust
                        if (Test-Path $vcToolsPath) {
                            $v143Dir = Get-ChildItem -Path $vcToolsPath -Filter "14.3*" -Directory | Sort-Object Name -Descending | Select-Object -First 1
                            if ($v143Dir) {
                                Write-Host "Found v143 toolset: $($v143Dir.Name)"
                                $toolsetArg = "-vcvars_ver=$($v143Dir.Name)"
                                $forceVs2022Toolset = $true
                            } else {
                                Write-Host "Automatically downgrading to v143 (VS 2022) toolset..."
                                $toolsetArg = "-vcvars_ver=14.3"
                                $forceVs2022Toolset = $true
                            }
                        } else {
                             $toolsetArg = "-vcvars_ver=14.3"
                             $forceVs2022Toolset = $true
                        }
                        $compatMsvcVersion = '193'
                    } elseif ($v144Dir) {
                        # Fallback to v144 if v143 is missing but v144 is present
                        Write-Host "Found v144 toolset: $($v144Dir.Name)"
                        Write-Host "Using v144 (VS 2022 17.10+) as fallback for VS 2025 compatibility."
                        $toolsetArg = "-vcvars_ver=$($v144Dir.Name)"
                        $forceVs2022Toolset = $true
                        $compatMsvcVersion = '194'
                    } else {
                        # If v143/v144 is missing, try to use the default v145 toolset but warn heavily
                        # This is a "Hail Mary" for users who refuse to install v143
                        Write-Warning "VS 2025 detected but 'MSVC v143/v144' toolsets not found."
                        Write-Warning "Attempting to build with default VS 2025 toolset (v145). This is known to fail with Boost 1.85/1.86."
                        $toolsetArg = "" 
                        $forceVs2022Toolset = $false
                        # Reset our "lie" to Conan so it matches reality
                        $msvcVersion = '195'
                    }
                }

                # Allow manual override
                if ($env:YAMS_VS_TOOLSET) {
                    Write-Host "Forcing Toolset Version (Override): $($env:YAMS_VS_TOOLSET)"
                    $toolsetArg = "-vcvars_ver=$($env:YAMS_VS_TOOLSET)"
                    $forceVs2022Toolset = $env:YAMS_VS_TOOLSET -like '14.3*'
                }

                # Import VS environment variables into current PowerShell session for x64 architecture
                $varsLoaded = $false
                & "${env:COMSPEC}" /s /c "`"$vsDevCmd`" -arch=amd64 -host_arch=amd64 $toolsetArg -no_logo && set" | ForEach-Object {
                    $name, $value = $_ -split '=', 2
                    if ($name -and $value) {
                        Set-Item -Force -Path "ENV:\$name" -Value $value
                        $varsLoaded = $true
                    }
                }
                
                if (-not $varsLoaded) {
                    throw "Failed to initialize Visual Studio environment. The toolset ($($toolsetArg -replace '-vcvars_ver=','')) might be missing or broken."
                }

                Write-Host 'Visual Studio environment initialized successfully (x64)'
            }
        }
    }
    
    # Verify compiler is now available
    if (-not (Get-Command cl.exe -ErrorAction SilentlyContinue)) {
        Write-Error @"
Visual Studio 2022 with C++ tools not found or could not be initialized.

Please either:
1. Run this script from 'Developer PowerShell for VS 2022' (Start Menu), OR
2. Install Visual Studio 2022 with 'Desktop development with C++' workload, OR
3. Ensure vswhere.exe is available at: ${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe

For installation: https://visualstudio.microsoft.com/downloads/
"@
    }
}

# Detect MSVC version
$msvcVersion = '195' # Default fallback
$vsVersion = '18'    # Default fallback

# Try environment variable first (fastest and safest)
if ($env:VCToolsVersion -match '^(\d+)\.(\d+)') {
    $toolsetMajor = [int]$matches[1]
    # MSVC version is Toolset Version + 5 (e.g. 14.x -> 19.x)
    $msvcMajor = $toolsetMajor + 5
    $msvcVersion = "$msvcMajor$($matches[2].Substring(0,1))"
    Write-Host "Detected MSVC Version (from env): $msvcVersion"
} 
# Fallback to cl.exe output
elseif (Get-Command cl.exe -ErrorAction SilentlyContinue) {
    try {
        # Run via cmd to avoid PowerShell exit code issues with Stop preference
        $clOutput = cmd /c "cl.exe 2>&1"
        # Output format: Microsoft (R) C/C++ Optimizing Compiler Version 19.40.33811 for x64
        if ($clOutput -match 'Version (\d+)\.(\d+)') {
            $major = $matches[1]
            $minor = $matches[2]
            $msvcVersion = "$major$($minor.Substring(0,1))"
            Write-Host "Detected MSVC Version (from cl.exe): $msvcVersion"
        }
    } catch {
        Write-Warning "Failed to detect MSVC version from cl.exe, using defaults. Error: $_"
    }
}

# Map MSVC version to VS version
# 193 -> VS 17 (2022)
# 194 -> VS 17 (2022) updates
# 195 -> VS 18 (2025)
if ($msvcVersion -ge 195) {
    $vsVersion = '18'
} elseif ($msvcVersion -ge 193) {
    $vsVersion = '17'
} elseif ($msvcVersion -ge 192) {
    $vsVersion = '16'
}

# Force VS 2022 toolset (v143/v144) if we are on VS 2025 to fix Boost build
# This overrides the detection above because Boost 1.85/1.86 b2 build system
# does not support the v145 toolset (VS 2025) correctly yet.
if ($msvcVersion -ge 195 -and $forceVs2022Toolset) {
    Write-Host "VS 2025 detected. Forcing Conan to use VS 2022 (v17) settings for compatibility."
    # We must keep vsVersion='18' (VS 2025) because that's what is actually installed on disk.
    # Conan checks the registry/vswhere for this version.
    # However, we lie about the compiler version to trick recipes into using v143/v144 logic.
    $vsVersion = '18' 
    $msvcVersion = $compatMsvcVersion
}

# Basic config (match setup.sh semantics as closely as practical)
if (-not $env:YAMS_CPPSTD) {
    # Default to C++20 for MSVC to ensure stability with Boost and other dependencies
    $env:YAMS_CPPSTD = '20'
}

# Safety check: C++23 with MSVC + Boost 1.86 is currently problematic on Windows
if ($env:YAMS_CPPSTD -eq '23' -and $msvcVersion -ge 190) {
    Write-Warning "C++23 with MSVC and Boost is currently unstable. Forcing downgrade to C++20 for this build."
    $env:YAMS_CPPSTD = '20'
}

# C++20 Modules auto-detection
if (-not $env:YAMS_ENABLE_MODULES) {
    $enableModules = $false
    $moduleTestCode = 'export module test; export int foo() { return 42; }'
    $moduleTestFile = [System.IO.Path]::GetTempFileName() + '.cppm'
    try {
        $moduleTestCode | Out-File -FilePath $moduleTestFile -Encoding utf8
        $clOutput = & cl.exe /std:c++20 /interface /TP /c $moduleTestFile /Fo:NUL 2>&1
        if ($LASTEXITCODE -eq 0) {
            $enableModules = $true
            Write-Host "Auto-detected C++20 module support (MSVC /interface)"
        } else {
            Write-Host "C++20 modules not fully supported by compiler, disabled"
        }
    } catch {
        Write-Host "C++20 modules not fully supported by compiler, disabled"
    } finally {
        Remove-Item -Path $moduleTestFile -ErrorAction SilentlyContinue
    }
} else {
    if ($env:YAMS_ENABLE_MODULES -eq 'true') {
        $enableModules = $true
        Write-Host "C++20 modules enabled via YAMS_ENABLE_MODULES=true"
    } else {
        $enableModules = $false
        Write-Host "C++20 modules disabled via YAMS_ENABLE_MODULES=false"
    }
}

# Ensure Python tools
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Error 'python not found in PATH. Please install Python 3.x and retry.'
}

# Check if required tools are already installed (e.g., by CI workflow)
# Only install if missing to avoid version conflicts with globally installed packages
$needsInstall = @()
if (-not (Get-Command conan -ErrorAction SilentlyContinue)) { $needsInstall += 'conan' }
if (-not (Get-Command meson -ErrorAction SilentlyContinue)) { $needsInstall += 'meson' }
if (-not (Get-Command ninja -ErrorAction SilentlyContinue)) { $needsInstall += 'ninja' }

if ($needsInstall.Count -gt 0) {
    Write-Host "Installing missing tools: $($needsInstall -join ', ')"
    python -m pip install --user --upgrade pip | Out-Null
    # Install numpy as it is often required by boost build checks even if python is disabled
    python -m pip install --user conan meson ninja numpy | Out-Null
} else {
    Write-Host "Build tools already installed (conan, meson, ninja)"
}

# Make sure user-local Python scripts are on PATH
$UserBase = python -m site --user-base
$UserBin = Join-Path $UserBase 'Scripts'
if ($env:PATH -notlike "*$UserBin*") {
    $env:PATH = "$UserBin;$env:PATH"
}

# Map BuildType to build directories similar to setup.sh
switch ($BuildType) {
    'Debug'     { $buildDir = 'builddir'; $conanSubdir = 'build-debug' }
    'Profiling' { $buildDir = 'build/profiling'; $conanSubdir = 'build-profiling' }
    'Fuzzing'   { $buildDir = 'build/fuzzing'; $conanSubdir = 'build-fuzzing' }
    Default     { $buildDir = "build/$($BuildType.ToLower())"; $conanSubdir = "build-$($BuildType.ToLower())" }
}

$buildDir = $buildDir -replace '/', '\\'

Write-Host "Build Type:      $BuildType"
Write-Host "Build Dir:       $buildDir"
Write-Host "C++ Std (Conan): $($env:YAMS_CPPSTD)"

# Conan args (mirror setup.sh defaults, but with MSVC settings)
# Allow overriding MSVC version via env vars for CI (e.g. windows-2022 uses 193/17)
if ($env:YAMS_MSVC_VERSION) { $msvcVersion = $env:YAMS_MSVC_VERSION }
if ($env:YAMS_VS_VERSION) { $vsVersion = $env:YAMS_VS_VERSION }

$conanArgs = @(
    '.',
    '-of', $buildDir,
    '-s', "build_type=$BuildType",
    '-s', 'os=Windows',
    '-s', 'arch=x86_64',
    '-s', 'compiler=msvc',
    '-s', "compiler.version=$msvcVersion",
    '-s', 'compiler.runtime=dynamic',
    '-s', "compiler.cppstd=$($env:YAMS_CPPSTD)",
    '--update'
)

# Configure VS version for bzip2 and other packages that need MSBuild detection
# VS 2025/2024 is version 18, MSVC compiler version 195
$conanArgs += @('-c', "tools.microsoft.msbuild:vs_version=$vsVersion")

# Always rebuild missing packages
$conanArgs += @('--build=missing', '-c', 'tools.env.virtualenv:powershell=True')

# Export custom Conan recipes before install (must happen before conan install)
Write-Host '--- Exporting custom Conan recipes... ---'
# qpdf export removed - PDF plugin will be updated in separate PBI

if (Test-Path 'conan/onnxruntime/conanfile.py') {
    Write-Host 'Exporting onnxruntime/1.23.2 from conan/onnxruntime/'
    conan export conan/onnxruntime --name=onnxruntime --version=1.23.2
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to export onnxruntime recipe"
        exit $LASTEXITCODE
    }
}

if ($env:YAMS_CONAN_HOST_PROFILE) {
    Write-Host "Using explicit Conan host profile: $($env:YAMS_CONAN_HOST_PROFILE)"
    $conanArgs += @('-pr:h', $env:YAMS_CONAN_HOST_PROFILE, '-pr:b', 'default')
}

# Enable tests/benchmarks for Debug/Profiling/Fuzzing builds (parity with setup.sh)
if ($BuildType -in @('Debug','Profiling','Fuzzing')) {
    $conanArgs += @('-o', 'build_tests=True', '-o', 'build_benchmarks=True')
}

# Optional feature flags (match environment knobs from setup.sh)
if ($env:YAMS_DISABLE_ONNX -eq 'true') {
    Write-Host 'ONNX support disabled (YAMS_DISABLE_ONNX=true)'
    $conanArgs += @('-o', 'yams/*:enable_onnx=False')
}

if ($env:YAMS_DISABLE_SYMBOL_EXTRACTION -eq 'true') {
    Write-Host 'Symbol extraction disabled (YAMS_DISABLE_SYMBOL_EXTRACTION=true)'
    $conanArgs += @('-o', 'yams/*:enable_symbol_extraction=False')
}

if ($env:YAMS_DISABLE_PDF -eq 'true') {
    Write-Host 'PDF support disabled (YAMS_DISABLE_PDF=true)'
    $conanArgs += @('-o', 'yams/*:enable_pdf=False')
}

Write-Host '--- Running conan install (Windows/MSVC)... ---'
try {
    # Use runtime_deploy to copy DLLs next to executables
    conan install @conanArgs --deployer=runtime_deploy --deployer-folder=$buildDir
    if ($LASTEXITCODE -ne 0) {
        throw "Conan install failed with exit code $LASTEXITCODE"
    }
} catch {
    Write-Error $_
    Write-Warning "Build failed. Common fixes:"
    Write-Warning "1. If using VS 2025 (Preview), try installing the VS 2022 (v143) toolset via Visual Studio Installer."
    Write-Warning "2. Try cleaning the cache: conan remove boost/* -c"
    Write-Warning "3. Vcpkg is often more stable on Windows for bleeding-edge compilers, but this project uses Conan for cross-platform consistency."
    exit 1
}

# Meson setup: prefer native file; if cross file is generated, use that instead
$nativeFile = Join-Path $buildDir "$conanSubdir/conan/conan_meson_native.ini"
$crossFile  = Join-Path $buildDir "$conanSubdir/conan/conan_meson_cross.ini"

if (-not (Test-Path $buildDir)) {
    New-Item -ItemType Directory -Path $buildDir | Out-Null
}

$mesonToolchainArg = $null
$mesonToolchainFile = $null

if (Test-Path $nativeFile) {
    $mesonToolchainArg  = '--native-file'
    $mesonToolchainFile = $nativeFile
} elseif (Test-Path $crossFile) {
    $mesonToolchainArg  = '--cross-file'
    $mesonToolchainFile = $crossFile
} else {
    Write-Error "Conan Meson toolchain file not found. Expected: $nativeFile or $crossFile"
}

# Activate Conan build environment to ensure tools like pkg-config are in PATH
$conanBuildPs1 = Join-Path $buildDir "$conanSubdir/conan/conanbuild.ps1"
if (Test-Path $conanBuildPs1) {
    Write-Host "Activating Conan build environment: $conanBuildPs1"
    . $conanBuildPs1
}

# Detect install prefix
if ($env:YAMS_INSTALL_PREFIX) {
    $InstallPrefix = $env:YAMS_INSTALL_PREFIX
} else {
    # Check if running as Administrator
    $isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
    
    if ($isAdmin) {
        $InstallPrefix = "C:\Program Files\yams"
    } else {
        # Default to user-local install if not admin
        $InstallPrefix = Join-Path $env:LOCALAPPDATA "Programs\yams"
        Write-Host "Running as non-admin. Defaulting install prefix to user-local path." -ForegroundColor Yellow
    }
}
Write-Host "Install Prefix:    $InstallPrefix"

if (-not (Test-Path (Join-Path $buildDir 'meson-private'))) {
    Write-Host 'Configuring Meson builddir...'
    $buildTypeLower = $BuildType.ToLower()
    
    # Parse extra Meson flags from environment variable
    # PowerShell can expand environment variables character-by-character in some contexts
    # We need to ensure we get a proper string array
    $extraMesonFlags = @()
    if ($env:YAMS_EXTRA_MESON_FLAGS) {
        # Force conversion to string and trim any whitespace
        $flagsString = "$($env:YAMS_EXTRA_MESON_FLAGS)".Trim()
        Write-Host "DEBUG: Raw YAMS_EXTRA_MESON_FLAGS = '$flagsString'"
        
        # Split on whitespace, filter empty entries
        $extraMesonFlags = @($flagsString -split '\s+' | Where-Object { $_.Length -gt 0 })
        
        Write-Host "DEBUG: Parsed $($extraMesonFlags.Count) flag(s):"
        foreach ($i in 0..($extraMesonFlags.Count - 1)) {
            Write-Host "DEBUG:   [$i] = '$($extraMesonFlags[$i])'"
        }
    }
    
    $enableModulesFlag = if ($enableModules) { 'true' } else { 'false' }

    # Auto-enable zyp plugin if Zig 0.15.2+ is installed
    $enableZyp = $false
    if (Get-Command zig -ErrorAction SilentlyContinue) {
        $zigVersion = (zig version 2>$null)
        if ($zigVersion -match '^(\d+)\.(\d+)\.(\d+)') {
            $zigMajor = [int]$matches[1]
            $zigMinor = [int]$matches[2]
            $zigPatch = [int]$matches[3]
            # Check if >= 0.15.2
            if ($zigMajor -gt 0 -or ($zigMajor -eq 0 -and $zigMinor -gt 15) -or ($zigMajor -eq 0 -and $zigMinor -eq 15 -and $zigPatch -ge 2)) {
                Write-Host "Zig $zigVersion detected, enabling zyp PDF plugin"
                $enableZyp = $true
            } else {
                Write-Host "Zig $zigVersion found but requires 0.15.2+ for zyp plugin"
            }
        }
    }

    # Build meson command arguments as a proper array
    $mesonArgs = @('setup', $buildDir, "--buildtype=$buildTypeLower", "--prefix=$InstallPrefix", "-Denable-modules=$enableModulesFlag")
    if ($enableZyp) { $mesonArgs += '-Dplugin-zyp=true' }
    if ($mesonToolchainArg) { $mesonArgs += $mesonToolchainArg }
    if ($mesonToolchainFile) { $mesonArgs += $mesonToolchainFile }
    
    # Enable vector tests for Debug/Profiling/Fuzzing builds (parity with setup.sh)
    if ($BuildType -in @('Debug','Profiling','Fuzzing')) {
        Write-Host "Enabling vector/embedding tests for $BuildType build"
        $mesonArgs += '-Denable-vector-tests=true'
    }
    
    $mesonArgs += $extraMesonFlags
    
    Write-Host "DEBUG: Final meson command: meson $($mesonArgs -join ' ')"
    & meson @mesonArgs
} else {
    Write-Host 'Meson builddir already configured, reconfiguring...'
    Write-Host "Ensuring prefix is set to: $InstallPrefix"
    # Use --reconfigure to handle Meson version upgrades gracefully
    $reconfigureArgs = @('setup', '--reconfigure', $buildDir, "-Dprefix=$InstallPrefix")
    if ($mesonToolchainArg) { $reconfigureArgs += $mesonToolchainArg }
    if ($mesonToolchainFile) { $reconfigureArgs += $mesonToolchainFile }
    
    # Ensure vector tests are enabled for Debug/Profiling/Fuzzing builds (parity with setup.sh)
    if ($BuildType -in @('Debug','Profiling','Fuzzing')) {
        $reconfigureArgs += '-Denable-vector-tests=true'
    }
    
    & meson @reconfigureArgs
}

Write-Host 'Compiling...'
meson compile -C $buildDir

# Note: Windows runtime DLLs (onnxruntime.dll, tbb*.dll) are now deployed
# automatically by Meson custom_target during the build phase.
# See meson.build files in src/daemon, tools/yams-cli, tools/yams-mcp, and plugins/onnx.

Write-Host "----------------------------------------------------------------"
Write-Host "Build complete."

if ($Package) {
    Write-Host "----------------------------------------------------------------"
    Write-Host "Packaging MSI installer..." -ForegroundColor Cyan

    # Stage the install for MSI packaging (use absolute path to avoid meson nesting issues)
    $stageDir = Join-Path (Resolve-Path $buildDir).Path "stage"
    Write-Host "Staging install to: $stageDir"

    # Remove old stage if exists
    if (Test-Path $stageDir) {
        Remove-Item -Recurse -Force $stageDir
    }
    New-Item -ItemType Directory -Path $stageDir -Force | Out-Null

    # Install to stage directory (destdir prepends to prefix, build-msi.ps1 handles finding files)
    meson install -C $buildDir --destdir $stageDir
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Meson install failed"
        exit 1
    }

    # Determine version
    if ([string]::IsNullOrEmpty($Version)) {
        # Try to extract from meson.build
        $mesonBuild = Join-Path $PSScriptRoot "meson.build"
        if (Test-Path $mesonBuild) {
            $content = Get-Content $mesonBuild -Raw
            if ($content -match "version\s*:\s*'([^']+)'") {
                $Version = $matches[1]
            }
        }
        if ([string]::IsNullOrEmpty($Version)) {
            $Version = "0.0.0"
        }
    }
    Write-Host "Package version: $Version"

    # Build MSI - the script handles finding yams.exe wherever meson placed it
    $msiScript = Join-Path $PSScriptRoot "packaging\windows\build-msi.ps1"
    if (-not (Test-Path $msiScript)) {
        Write-Error "MSI build script not found at: $msiScript"
        exit 1
    }

    & $msiScript -StageDir $stageDir -Version $Version -OutputDir $buildDir
    if ($LASTEXITCODE -ne 0) {
        Write-Error "MSI build failed"
        exit 1
    }

    Write-Host "----------------------------------------------------------------"
    Write-Host "MSI package created in: $buildDir" -ForegroundColor Green
    Get-ChildItem $buildDir -Filter "*.msi" | ForEach-Object {
        Write-Host "  $($_.Name)" -ForegroundColor Green
    }
} else {
    Write-Host "To install, run: meson install -C $buildDir"
    Write-Host "To build MSI:    .\setup.ps1 -BuildType $BuildType -Package"
    Write-Host "Note: You may need to add '$InstallPrefix\bin' to your PATH."
}
Write-Host "----------------------------------------------------------------"
