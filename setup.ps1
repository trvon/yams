Param(
    [ValidateSet('Debug','Release','Profiling','Fuzzing')]
    [string]$BuildType = 'Release',

    [switch]$Package,

    [switch]$Offline,

    [switch]$SystemDeps,

    [string]$Version = ''
)

$ErrorActionPreference = 'Stop'
$initialMeson = Get-Command meson -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source -First 1
$initialNinja = Get-Command ninja -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source -First 1

function Import-BatchEnvironment {
    param(
        [Parameter(Mandatory = $true)]
        [string]$BatchFile,

        [string[]]$Arguments = @()
    )

    if (-not (Test-Path $BatchFile)) {
        return $false
    }

    $commandParts = @("`"$BatchFile`"")
    foreach ($arg in $Arguments) {
        if ($null -ne $arg -and $arg -ne '') {
            $commandParts += $arg
        }
    }

    $varsLoaded = $false
    & $env:COMSPEC /s /c (($commandParts -join ' ') + ' && set') | ForEach-Object {
        $name, $value = $_ -split '=', 2
        if ($name -and $value) {
            Set-Item -Force -Path "ENV:\$name" -Value $value
            $varsLoaded = $true
        }
    }

    return $varsLoaded
}

if (-not $Offline -and $env:YAMS_OFFLINE -match '^(1|true|yes|on)$') {
    $Offline = $true
}

if (-not $SystemDeps -and $env:YAMS_USE_SYSTEM_DEPS -match '^(1|true|yes|on)$') {
    $SystemDeps = $true
}

# Initialize Visual Studio environment if not already set
if (-not $env:VSINSTALLDIR -or -not (Get-Command cl.exe -ErrorAction SilentlyContinue)) {
    Write-Host 'Visual Studio environment not detected, initializing...'
    
    # Find Visual Studio installation
    $vswhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
    if (-not (Test-Path $vswhere)) {
        $vswhere = "$env:ProgramFiles\Microsoft Visual Studio\Installer\vswhere.exe"
    }
    
    if (Test-Path $vswhere) {
        # Prefer the newest installed MSVC toolchain by default.
        $vsPath = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath

        if (-not $vsPath) {
            $vsPath = & $vswhere -version "[17.0,18.0)" -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
        }

        if ($vsPath) {
            $vsDevCmd = Join-Path $vsPath "Common7\Tools\VsDevCmd.bat"
            if (Test-Path $vsDevCmd) {
                Write-Host "Found Visual Studio at: $vsPath"
                
                $toolsetArg = ""

                # Resolve installationVersion via JSON and matching installationPath.
                $vsInstallVersion = $null
                try {
                    $instancesJson = & $vswhere -all -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -format json
                    $instances = $instancesJson | ConvertFrom-Json
                    $thisInstance = $instances | Where-Object { $_.installationPath -eq $vsPath } | Select-Object -First 1
                    if ($thisInstance) {
                        $vsInstallVersion = $thisInstance.installationVersion
                    }
                } catch {
                    # Best-effort; continue without installationVersion string.
                    $vsInstallVersion = $null
                }

                $vcToolsPath = Join-Path $vsPath "VC\Tools\MSVC"
                $v143Dir = $null
                $v144Dir = $null
                if (Test-Path $vcToolsPath) {
                    $v143Dir = Get-ChildItem -Path $vcToolsPath -Filter "14.3*" -Directory -ErrorAction SilentlyContinue | Sort-Object Name -Descending | Select-Object -First 1
                    $v144Dir = Get-ChildItem -Path $vcToolsPath -Filter "14.4*" -Directory -ErrorAction SilentlyContinue | Sort-Object Name -Descending | Select-Object -First 1
                }

                if ($vsPath -match "\\18\\" -or ($vsInstallVersion -and $vsInstallVersion -match '^18\.')) {
                    if ($vsInstallVersion) {
                        Write-Host "VS 2025 detected (Version $vsInstallVersion)"
                    } else {
                        Write-Host "VS 2025 detected"
                    }

                    if ($v143Dir -or $v144Dir) {
                        $legacyToolsets = @()
                        if ($v143Dir) { $legacyToolsets += $v143Dir.Name }
                        if ($v144Dir) { $legacyToolsets += $v144Dir.Name }
                        Write-Host "Optional legacy MSVC toolsets detected: $($legacyToolsets -join ', ')"
                    }
                    Write-Host "Using the default VS 2025 toolset unless YAMS_VS_TOOLSET is explicitly set."
                }

                # Allow manual override
                if ($env:YAMS_VS_TOOLSET) {
                    Write-Host "Forcing Toolset Version (Override): $($env:YAMS_VS_TOOLSET)"
                    $toolsetArg = "-vcvars_ver=$($env:YAMS_VS_TOOLSET)"
                }

                # Import VS environment variables into current PowerShell session for x64 architecture.
                $vsDevCmdArgs = @('-arch=amd64', '-host_arch=amd64')
                if ($toolsetArg) {
                    $vsDevCmdArgs += $toolsetArg
                }
                $vsDevCmdArgs += '-no_logo'
                $varsLoaded = Import-BatchEnvironment -BatchFile $vsDevCmd -Arguments $vsDevCmdArgs
                
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
MSVC compiler (cl.exe) not found or could not be initialized.

Detected Visual Studio instance:
    $vsPath

Common fixes:
1. Install the 'Desktop development with C++' workload for your Visual Studio installation.
2. Ensure the Windows 10/11 SDK is installed.
3. If using VS 2025 and you need a specific MSVC toolset:
    - Install the matching optional MSVC toolset in Visual Studio Installer, OR
    - Override toolset selection via:  `$env:YAMS_VS_TOOLSET = '14.5'`
4. Ensure vswhere.exe exists at: ${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe

For installation: https://visualstudio.microsoft.com/downloads/
"@
    }
}

# Detect MSVC version
$msvcVersion = $null
$msvcVersionFromEnv = $null
$msvcVersionFromCl = $null
$vsVersion = $null

# Try environment variable first (fastest and safest)
if ($env:VCToolsVersion -match '^(\d+)\.(\d+)') {
    $toolsetMajor = [int]$matches[1]
    # MSVC version is Toolset Version + 5 (e.g. 14.x -> 19.x)
    $msvcMajor = $toolsetMajor + 5
    $msvcVersionFromEnv = "$msvcMajor$($matches[2].Substring(0,1))"
    Write-Host "Detected MSVC Version (from env): $msvcVersionFromEnv"
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
            $msvcVersionFromCl = "$major$($minor.Substring(0,1))"
            Write-Host "Detected MSVC Version (from cl.exe): $msvcVersionFromCl"
        }
    } catch {
        Write-Warning "Failed to detect MSVC version from cl.exe, using defaults. Error: $_"
    }
}

if ($msvcVersionFromEnv -and $msvcVersionFromCl -and $msvcVersionFromEnv -ne $msvcVersionFromCl) {
    Write-Warning "MSVC version mismatch: env=$msvcVersionFromEnv, cl.exe=$msvcVersionFromCl. Using env toolset version."
}

if ($msvcVersionFromEnv) {
    $msvcVersion = $msvcVersionFromEnv
} elseif ($msvcVersionFromCl) {
    $msvcVersion = $msvcVersionFromCl
} else {
    $msvcVersion = '195' # Default fallback
}

# Map MSVC version to VS version
# Prefer actual VS installation version when available.
if ($vsInstallVersion -and $vsInstallVersion -match '^(\d+)\.') {
    $vsVersion = $matches[1]
} else {
    # 193 -> VS 17 (2022)
    # 194 -> VS 17 (2022) updates
    # 195 -> VS 18 (2025)
    if ($msvcVersion -ge 195) {
        $vsVersion = '18'
    } elseif ($msvcVersion -ge 193) {
        $vsVersion = '17'
    } elseif ($msvcVersion -ge 192) {
        $vsVersion = '16'
    } else {
        $vsVersion = '18'
    }
}

# Basic config (match setup.sh semantics as closely as practical)
if (-not $env:YAMS_CPPSTD) {
    # Default to C++20 for MSVC to ensure stability with Boost and other dependencies
    $env:YAMS_CPPSTD = '20'
}

# Enforce minimum C++20
$cppStdValue = $null
if ([int]::TryParse($env:YAMS_CPPSTD, [ref]$cppStdValue)) {
    if ($cppStdValue -lt 20) {
        Write-Error "YAMS requires C++20 or newer on Windows. Detected YAMS_CPPSTD=$($env:YAMS_CPPSTD)."
    }
} else {
    Write-Warning "Could not parse YAMS_CPPSTD='$($env:YAMS_CPPSTD)'. Proceeding with compiler detection."
}

# Safety check: C++23 with MSVC + Boost 1.86 is currently problematic on Windows
if ($env:YAMS_CPPSTD -eq '23' -and $msvcVersion -ge 190) {
    Write-Warning "C++23 with MSVC and Boost is currently unstable. Forcing downgrade to C++20 for this build."
    $env:YAMS_CPPSTD = '20'
}

# C++20 capability probe (non-modules)
$cxx20ProbeOk = $false
$cxx20TestCode = '#include <vector>\nint main(){std::vector<int> v; return (int)v.size(); }'
$cxx20TestFile = [System.IO.Path]::GetTempFileName() + '.cpp'
try {
    $cxx20TestCode | Out-File -FilePath $cxx20TestFile -Encoding ascii
    $clOutput = & cl.exe /nologo /std:c++20 /EHsc /TP /c $cxx20TestFile /Fo:NUL 2>&1
    if ($LASTEXITCODE -eq 0) {
        $cxx20ProbeOk = $true
        Write-Host "C++20 compile probe succeeded"
    } else {
        Write-Warning "C++20 compile probe failed. Compiler output: $clOutput"
    }
} catch {
    Write-Warning "C++20 compile probe failed: $_"
} finally {
    Remove-Item -Path $cxx20TestFile -ErrorAction SilentlyContinue
}

if (-not $cxx20ProbeOk) {
    Write-Error "C++20 support is required but not detected. Ensure MSVC toolset supports /std:c++20."
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

Write-Host "Decision trace: VS=$vsVersion (install=$vsInstallVersion), MSVC=$msvcVersion, Toolset=$($env:VCToolsVersion), C++=$($env:YAMS_CPPSTD), Modules=$enableModules"

function Resolve-PythonExecutable {
    $candidates = [System.Collections.Generic.List[string]]::new()

    $pyLauncher = Get-Command py -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source -First 1
    if ($pyLauncher) {
        try {
            $resolved = & $pyLauncher -3 -c "import sys; print(sys.executable)" 2>$null
            if ($LASTEXITCODE -eq 0 -and $resolved) {
                $candidates.Add($resolved.Trim())
            }
        } catch {}
    }

    $pythonCmd = Get-Command python -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source -First 1
    if ($pythonCmd -and $pythonCmd -notmatch '\\WindowsApps\\') {
        $candidates.Add($pythonCmd)
    }

    $scoopPythonRoot = Join-Path $env:USERPROFILE 'scoop\apps\python'
    if (Test-Path $scoopPythonRoot) {
        Get-ChildItem -Path $scoopPythonRoot -Directory -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -match '^\d' } |
            Sort-Object Name -Descending |
            ForEach-Object {
                $candidate = Join-Path $_.FullName 'python.exe'
                if (Test-Path $candidate) {
                    $candidates.Add($candidate)
                }
            }
    }

    $seen = @{}
    foreach ($candidate in $candidates) {
        if (-not $candidate) {
            continue
        }

        try {
            $normalized = [System.IO.Path]::GetFullPath($candidate)
        } catch {
            $normalized = $candidate
        }

        if ($seen.ContainsKey($normalized)) {
            continue
        }
        $seen[$normalized] = $true

        try {
            $resolved = & $normalized -c "import sys; print(sys.executable)" 2>$null
            if ($LASTEXITCODE -eq 0 -and $resolved) {
                return $resolved.Trim()
            }
        } catch {}
    }

    return $null
}

function Prepend-PathEntry {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PathEntry
    )

    if ((Test-Path $PathEntry) -and ($env:PATH -notlike "*$PathEntry*")) {
        $env:PATH = "$PathEntry;$env:PATH"
    }
}

function Resolve-ToolExecutable {
    param(
        [Parameter(Mandatory = $true)]
        [string]$CommandName,

        [string[]]$ScoopRelativePaths = @("$CommandName.exe"),

        [string[]]$VersionArguments = @('--version')
    )

    $candidates = [System.Collections.Generic.List[string]]::new()
    $commandPath = Get-Command $CommandName -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source -First 1
    if ($commandPath -and $commandPath -notmatch '\\WindowsApps\\' -and $commandPath -notmatch '\\scoop\\shims\\') {
        $candidates.Add($commandPath)
    }

    $scoopToolRoot = Join-Path $env:USERPROFILE ("scoop\\apps\\" + $CommandName)
    if (Test-Path $scoopToolRoot) {
        Get-ChildItem -Path $scoopToolRoot -Directory -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -match '^\d' } |
            Sort-Object Name -Descending |
            ForEach-Object {
                foreach ($relativePath in $ScoopRelativePaths) {
                    $candidate = Join-Path $_.FullName $relativePath
                    if (Test-Path $candidate) {
                        $candidates.Add($candidate)
                    }
                }
            }
    }

    $seen = @{}
    foreach ($candidate in $candidates) {
        if (-not $candidate) {
            continue
        }

        try {
            $normalized = [System.IO.Path]::GetFullPath($candidate)
        } catch {
            $normalized = $candidate
        }

        if ($seen.ContainsKey($normalized)) {
            continue
        }
        $seen[$normalized] = $true

        try {
            & $normalized @VersionArguments *> $null
            if ($LASTEXITCODE -eq 0) {
                return $normalized
            }
        } catch {}
    }

    return $null
}

# Ensure Python tools
$pythonExe = Resolve-PythonExecutable
if (-not $pythonExe) {
    Write-Error 'python not found in PATH. Please install Python 3.x and retry.'
}

$pythonDir = Split-Path -Parent $pythonExe
Prepend-PathEntry $pythonDir
$pythonScriptsDir = Join-Path $pythonDir 'Scripts'
if (Test-Path $pythonScriptsDir) {
    Prepend-PathEntry $pythonScriptsDir
}
$env:PYTHON = $pythonExe
$env:YAMS_PYTHON = $pythonExe
Write-Host "Using Python: $pythonExe"

$conanExe = Resolve-ToolExecutable -CommandName 'conan'
$mesonExe = Resolve-ToolExecutable -CommandName 'meson'
$ninjaExe = Resolve-ToolExecutable -CommandName 'ninja'
$gitExe = Resolve-ToolExecutable -CommandName 'git' -ScoopRelativePaths @('cmd\\git.exe', 'bin\\git.exe')
foreach ($toolExe in @($gitExe, $ninjaExe, $mesonExe, $conanExe)) {
    if ($toolExe) {
        Prepend-PathEntry (Split-Path -Parent $toolExe)
    }
}
if ($conanExe) { Write-Host "Using Conan: $conanExe" }
if ($mesonExe) { Write-Host "Using Meson: $mesonExe" }
if ($ninjaExe) { Write-Host "Using Ninja: $ninjaExe" }
if ($gitExe) { Write-Host "Using Git: $gitExe" }

# Check if required tools are already installed (e.g., by CI workflow)
# Only install if missing to avoid version conflicts with globally installed packages
$needsInstall = @()
if (-not $conanExe) { $needsInstall += 'conan' }
if (-not $mesonExe) { $needsInstall += 'meson' }
if (-not $ninjaExe) { $needsInstall += 'ninja' }

if ($needsInstall.Count -gt 0) {
    if ($Offline -or $SystemDeps) {
        Write-Error "Missing required build tools in offline/system dependency mode: $($needsInstall -join ', '). Install them ahead of time and retry."
    }

    Write-Host "Installing missing tools: $($needsInstall -join ', ')"
    & $pythonExe -m pip install --user --upgrade pip | Out-Null
    # Install numpy as it is often required by boost build checks even if python is disabled
    & $pythonExe -m pip install --user conan meson ninja numpy | Out-Null
} else {
    Write-Host "Build tools already installed (conan, meson, ninja)"
}

# Make sure user-local Python scripts are on PATH
$UserBase = & $pythonExe -m site --user-base
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
Write-Host "Offline Mode:    $Offline"
Write-Host "System Deps:     $SystemDeps"

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
    '-s', "compiler.cppstd=$($env:YAMS_CPPSTD)"
)

if ($Offline) {
    $conanArgs += @('-nr', '--build=never')
} else {
    $conanArgs += @('--update', '--build=missing')
}

# Prefer Ninja generator on VS 2025 to avoid unsupported VS generator names
$cmakeGenerator = $env:YAMS_CMAKE_GENERATOR
$vsVersionNumber = $null
if (-not $cmakeGenerator -and [int]::TryParse($vsVersion, [ref]$vsVersionNumber)) {
    if ($vsVersionNumber -ge 18) {
        $cmakeGenerator = 'Ninja'
        Write-Host "Using CMake generator: $cmakeGenerator (override with YAMS_CMAKE_GENERATOR)"
    }
}
if ($cmakeGenerator) {
    $conanArgs += @('-c', "tools.cmake.cmaketoolchain:generator=$cmakeGenerator")
}

# Configure VS version for bzip2 and other packages that need MSBuild detection
# VS 2025/2024 is version 18, MSVC compiler version 195
if (-not $env:YAMS_CONAN_COMPILER_VERSION) {
    $env:YAMS_CONAN_COMPILER_VERSION = $msvcVersion
    Write-Host "Using detected Conan compiler.version: $($env:YAMS_CONAN_COMPILER_VERSION)"
} else {
    Write-Host "Using pinned Conan compiler.version from YAMS_CONAN_COMPILER_VERSION: $($env:YAMS_CONAN_COMPILER_VERSION)"
}

if (-not $env:YAMS_MSBUILD_VS_VERSION) {
    $env:YAMS_MSBUILD_VS_VERSION = $vsVersion
    Write-Host "Using detected MSBuild Visual Studio version: $($env:YAMS_MSBUILD_VS_VERSION)"
} else {
    Write-Host "Using pinned MSBuild Visual Studio version from YAMS_MSBUILD_VS_VERSION: $($env:YAMS_MSBUILD_VS_VERSION)"
}

if (-not $SystemDeps) {
    # Export custom Conan recipes before install (must happen before conan install)
    Write-Host '--- Exporting custom Conan recipes... ---'

    if (Test-Path 'conan/onnxruntime/conanfile.py') {
        Write-Host 'Exporting onnxruntime/1.23.0 from conan/onnxruntime/'
        conan export conan/onnxruntime --name=onnxruntime --version=1.23.0
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Failed to export onnxruntime recipe"
            exit $LASTEXITCODE
        }
    }
}

$conanHostProfile = $env:YAMS_CONAN_HOST_PROFILE
if (-not $conanHostProfile) {
    $conanHostProfile = '.\conan\profiles\host-windows-msvc'
}

if (-not (Test-Path $conanHostProfile)) {
    Write-Error "Conan host profile not found: $conanHostProfile"
    exit 1
}

Write-Host "Using Conan host profile: $conanHostProfile"
$conanArgs += @('-pr:h', $conanHostProfile, '-pr:b', 'default')

# Enable tests/benchmarks for Debug/Profiling/Fuzzing builds (parity with setup.sh)
if ($BuildType -in @('Debug','Profiling','Fuzzing')) {
    $conanArgs += @('-o', '&:build_tests=True', '-o', '&:build_benchmarks=True')
}

# Optional feature flags (match environment knobs from setup.sh)
if ($env:YAMS_DISABLE_ONNX -eq 'true') {
    Write-Host 'ONNX support disabled (YAMS_DISABLE_ONNX=true)'
    $conanArgs += @('-o', 'yams/*:enable_onnx=False')
} else {
    # Auto-detect GPU for ONNX acceleration
    # Override with YAMS_ONNX_GPU=cuda|directml|none
    $onnxGpu = $env:YAMS_ONNX_GPU
    if (-not $onnxGpu -or $onnxGpu -eq 'auto') {
        # Windows: Check for DirectX 12 capable GPU (DirectML)
        # DirectML works with any DX12 GPU (NVIDIA, AMD, Intel)
        $hasDx12Gpu = $false
        try {
            # Check for any display adapter that supports DX12
            $adapters = Get-CimInstance -ClassName Win32_VideoController -ErrorAction SilentlyContinue
            if ($adapters) {
                foreach ($adapter in $adapters) {
                    # Most modern GPUs (2015+) support DX12
                    if ($adapter.AdapterRAM -gt 0 -and $adapter.Name -notmatch 'Microsoft Basic') {
                        $hasDx12Gpu = $true
                        Write-Host "GPU detected: $($adapter.Name)"
                        break
                    }
                }
            }
        } catch {
            Write-Host "Could not detect GPU: $_"
        }

        # Also check for NVIDIA GPU (prefer CUDA if available)
        $hasCuda = $false
        $hasCudaToolkit = $false
        if (Get-Command nvidia-smi -ErrorAction SilentlyContinue) {
            try {
                $null = nvidia-smi 2>$null
                if ($LASTEXITCODE -eq 0) {
                    $hasCuda = $true
                    Write-Host "NVIDIA GPU detected via nvidia-smi"

                    # Validate CUDA toolkit is actually available (not just the GPU driver)
                    # nvidia-smi alone doesn't guarantee CUDA runtime is usable
                    if (Get-Command nvcc -ErrorAction SilentlyContinue) {
                        $hasCudaToolkit = $true
                        Write-Host "CUDA toolkit (nvcc) detected"
                    } elseif ($env:CUDA_PATH -and (Test-Path "$env:CUDA_PATH\bin\nvcc.exe")) {
                        $hasCudaToolkit = $true
                        Write-Host "CUDA toolkit detected at $env:CUDA_PATH"
                    } elseif (Get-ChildItem -Path "$env:ProgramFiles\NVIDIA GPU Computing Toolkit\CUDA\*\bin\cudart64_*.dll" -ErrorAction SilentlyContinue) {
                        $hasCudaToolkit = $true
                        Write-Host "CUDA runtime (cudart64) detected"
                    }

                    if (-not $hasCudaToolkit) {
                        Write-Host "NVIDIA GPU detected but CUDA toolkit missing - will use DirectML or CPU"
                        Write-Host "  Install CUDA toolkit for GPU acceleration: https://developer.nvidia.com/cuda-downloads"
                    }
                }
            } catch {}
        }

        if ($hasCuda -and $hasCudaToolkit) {
            $onnxGpu = 'cuda'
            Write-Host "Enabling CUDA GPU acceleration"
        } elseif ($hasDx12Gpu) {
            $onnxGpu = 'directml'
            Write-Host "Enabling DirectML GPU acceleration (DX12)"
        } else {
            $onnxGpu = 'none'
            Write-Host "No GPU detected: using CPU-only ONNX Runtime"
        }
    }

    if ($onnxGpu -ne 'none') {
        $conanArgs += @('-o', "onnxruntime/*:with_gpu=$onnxGpu")
        Write-Host "ONNX GPU provider: $onnxGpu"
    }
}

if ($env:YAMS_DISABLE_SYMBOL_EXTRACTION -eq 'true') {
    Write-Host 'Symbol extraction disabled (YAMS_DISABLE_SYMBOL_EXTRACTION=true)'
    $conanArgs += @('-o', 'yams/*:enable_symbol_extraction=False')
}

if ($env:YAMS_DISABLE_PDF -eq 'true') {
    Write-Host 'PDF support disabled (YAMS_DISABLE_PDF=true)'
    $conanArgs += @('-o', 'yams/*:enable_pdf=False')
}

if (-not $SystemDeps) {
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
        Write-Warning "1. Verify setup.ps1 selected the installed MSVC toolset you expect in the decision trace."
        Write-Warning "2. Try cleaning the cache: conan remove boost/* -c"
        Write-Warning "3. If you need a non-default MSVC toolset, set YAMS_VS_TOOLSET before rerunning setup.ps1."
        exit 1
    }
}

# Workaround: libiconv static libs may be missing from Conan package on Windows
$libiconvPc = Join-Path $buildDir "$conanSubdir/conan/libiconv.pc"
if ((-not $SystemDeps) -and (Test-Path $libiconvPc)) {
    $libiconvPrefix = $null
    $libiconvLibDir = $null
    try {
        $libiconvPrefix = (Get-Content $libiconvPc | Where-Object { $_ -match '^prefix=' }) -replace '^prefix=', ''
        if ($libiconvPrefix) {
            $libiconvLibDir = Join-Path $libiconvPrefix 'lib'
        }
    } catch {
        $libiconvPrefix = $null
    }

    if ($libiconvLibDir) {
        if (-not (Test-Path $libiconvLibDir)) {
            New-Item -ItemType Directory -Path $libiconvLibDir | Out-Null
        }

        $iconvLibPath = Join-Path $libiconvLibDir 'iconv.lib'
        $charsetLibPath = Join-Path $libiconvLibDir 'charset.lib'

        if (-not (Test-Path $iconvLibPath) -or -not (Test-Path $charsetLibPath)) {
            $conanCacheRoot = Join-Path $env:USERPROFILE '.conan2\p'
            $iconvNoI18n = $null
            $iconvDllLib = $null
            $charsetLib = $null
            $charsetDllLib = $null

            if (Test-Path $conanCacheRoot) {
                $iconvNoI18n = Get-ChildItem -Path $conanCacheRoot -Filter 'iconv_no_i18n.lib' -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
                $iconvDllLib = Get-ChildItem -Path $conanCacheRoot -Filter 'iconv.dll.lib' -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
                $charsetLib = Get-ChildItem -Path $conanCacheRoot -Filter 'charset.lib' -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
                $charsetDllLib = Get-ChildItem -Path $conanCacheRoot -Filter 'charset.dll.lib' -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
            }

            if (-not (Test-Path $iconvLibPath)) {
                if ($iconvNoI18n) {
                    Copy-Item -Path $iconvNoI18n.FullName -Destination $iconvLibPath -Force
                    Write-Host "Patched libiconv: copied iconv_no_i18n.lib to $iconvLibPath"
                } elseif ($iconvDllLib) {
                    Copy-Item -Path $iconvDllLib.FullName -Destination $iconvLibPath -Force
                    Write-Host "Patched libiconv: copied iconv.dll.lib to $iconvLibPath"
                }
            }

            if (-not (Test-Path $charsetLibPath)) {
                if ($charsetLib) {
                    Copy-Item -Path $charsetLib.FullName -Destination $charsetLibPath -Force
                    Write-Host "Patched libiconv: copied charset.lib to $charsetLibPath"
                } elseif ($charsetDllLib) {
                    Copy-Item -Path $charsetDllLib.FullName -Destination $charsetLibPath -Force
                    Write-Host "Patched libiconv: copied charset.dll.lib to $charsetLibPath"
                }
            }
        }
    }
}

# Meson setup: prefer explicit overrides first, then Conan-generated files
$nativeFile = Join-Path $buildDir "$conanSubdir/conan/conan_meson_native.ini"
$crossFile  = Join-Path $buildDir "$conanSubdir/conan/conan_meson_cross.ini"

if (-not (Test-Path $buildDir)) {
    New-Item -ItemType Directory -Path $buildDir | Out-Null
}

$mesonToolchainArg = $null
$mesonToolchainFile = $null

if ($env:YAMS_MESON_NATIVE_FILE -and $env:YAMS_MESON_CROSS_FILE) {
    Write-Error 'Set only one of YAMS_MESON_NATIVE_FILE or YAMS_MESON_CROSS_FILE.'
}

if ($env:YAMS_MESON_NATIVE_FILE) {
    if (-not (Test-Path $env:YAMS_MESON_NATIVE_FILE)) {
        Write-Error "Meson native file not found: $($env:YAMS_MESON_NATIVE_FILE)"
    }
    $mesonToolchainArg = '--native-file'
    $mesonToolchainFile = $env:YAMS_MESON_NATIVE_FILE
} elseif ($env:YAMS_MESON_CROSS_FILE) {
    if (-not (Test-Path $env:YAMS_MESON_CROSS_FILE)) {
        Write-Error "Meson cross file not found: $($env:YAMS_MESON_CROSS_FILE)"
    }
    $mesonToolchainArg = '--cross-file'
    $mesonToolchainFile = $env:YAMS_MESON_CROSS_FILE
} elseif (-not $SystemDeps) {
    if (Test-Path $nativeFile) {
        $mesonToolchainArg  = '--native-file'
        $mesonToolchainFile = $nativeFile
    } elseif (Test-Path $crossFile) {
        $mesonToolchainArg  = '--cross-file'
        $mesonToolchainFile = $crossFile
    } else {
        Write-Error "Conan Meson toolchain file not found. Expected: $nativeFile or $crossFile"
    }
}

# Activate Conan build environment to ensure tools like pkg-config are in PATH.
# Conan can generate either PowerShell or batch activation scripts depending on host settings.
$conanBuildPs1 = Join-Path $buildDir "$conanSubdir/conan/conanbuild.ps1"
$conanBuildBat = Join-Path $buildDir "$conanSubdir/conan/conanbuild.bat"
$conanBuildEnvScript = $null
if (-not $SystemDeps) {
    if (Test-Path $conanBuildPs1) {
        Write-Host "Activating Conan build environment: $conanBuildPs1"
        . $conanBuildPs1
        $conanBuildEnvScript = $conanBuildPs1
    } elseif (Test-Path $conanBuildBat) {
        Write-Host "Activating Conan build environment: $conanBuildBat"
        if (-not (Import-BatchEnvironment -BatchFile $conanBuildBat)) {
            throw "Failed to import Conan batch environment: $conanBuildBat"
        }
        $conanBuildEnvScript = $conanBuildBat
    }
}

# Export BOOST_ROOT so meson's built-in Boost dep finds the Conan-installed
# Boost (Windows has no system Boost fallback). Resolve the prefix from Conan's
# boost.pc. Conan 2.x writes a relocatable form (`prefix=${pcfiledir}/..`)
# which pkg-config substitutes at query time — a naive Select-String capture
# yields the literal placeholder, so call pkgconf when available, otherwise
# expand `${pcfiledir}` manually.
if ((-not $SystemDeps) -and (-not $env:BOOST_ROOT)) {
    $conanGenDir = (Join-Path $buildDir "$conanSubdir/conan")
    $boostPc = Join-Path $conanGenDir 'boost.pc'
    if (Test-Path $boostPc) {
        $boostPath = $null
        # Preferred: ask pkgconf to do the substitution. Conan installs a
        # pkgconf binary alongside the generators; meson's PkgConfigDeps also
        # points PKG_CONFIG_PATH at this dir.
        $pkgconf = Get-Command pkg-config -ErrorAction SilentlyContinue
        if (-not $pkgconf) { $pkgconf = Get-Command pkgconf -ErrorAction SilentlyContinue }
        if ($pkgconf) {
            $env:PKG_CONFIG_PATH = "$conanGenDir;$($env:PKG_CONFIG_PATH)"
            try {
                $resolved = & $pkgconf.Source --variable=prefix boost 2>$null
                if ($LASTEXITCODE -eq 0 -and $resolved) {
                    $boostPath = $resolved.Trim()
                }
            } catch {}
        }
        # Fallback: parse `prefix=` line and expand ${pcfiledir} ourselves.
        if (-not $boostPath) {
            $prefixLine = Select-String -Path $boostPc `
                -Pattern '^\s*prefix\s*=' -ErrorAction SilentlyContinue |
                Select-Object -First 1
            if ($prefixLine) {
                $raw = ($prefixLine.Line -replace '^\s*prefix\s*=\s*', '').Trim()
                $raw = $raw -replace '\$\{pcfiledir\}', $conanGenDir
                try {
                    $boostPath = [System.IO.Path]::GetFullPath($raw)
                } catch { $boostPath = $raw }
            }
        }
        if ($boostPath -and (Test-Path (Join-Path $boostPath 'include/boost'))) {
            $env:BOOST_ROOT = $boostPath
            $env:BOOST_INCLUDEDIR = (Join-Path $boostPath 'include')
            $env:BOOST_LIBRARYDIR = (Join-Path $boostPath 'lib')
            Write-Host "BOOST_ROOT       set to: $boostPath"
            Write-Host "BOOST_INCLUDEDIR set to: $($env:BOOST_INCLUDEDIR)"
            Write-Host "BOOST_LIBRARYDIR set to: $($env:BOOST_LIBRARYDIR)"
            # Persist to GITHUB_ENV so any downstream workflow step (e.g.
            # 'Run Tests with Meson (Windows)') still sees the same hints.
            # No-op when not running under GitHub Actions.
            if ($env:GITHUB_ENV) {
                Add-Content -Path $env:GITHUB_ENV -Value "BOOST_ROOT=$boostPath"
                Add-Content -Path $env:GITHUB_ENV -Value "BOOST_INCLUDEDIR=$($env:BOOST_INCLUDEDIR)"
                Add-Content -Path $env:GITHUB_ENV -Value "BOOST_LIBRARYDIR=$($env:BOOST_LIBRARYDIR)"
                Write-Host "Persisted Boost env hints to GITHUB_ENV"
            }
        } elseif ($boostPath) {
            Write-Warning "boost.pc resolved prefix '$boostPath' has no include/boost subdir; not exporting BOOST_ROOT"
        } else {
            Write-Warning "boost.pc found at $boostPc but prefix could not be resolved"
        }
    }
}

$activeMeson = Get-Command meson -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source -First 1
$activeNinja = Get-Command ninja -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source -First 1
$conanToolsOverride = ($activeMeson -ne $initialMeson) -or ($activeNinja -ne $initialNinja)
$mesonWrapperPs1 = $null
if ((-not $SystemDeps) -and $conanBuildEnvScript) {
    $mesonWrapperPs1 = Join-Path $buildDir 'mesonw.ps1'
    $wrapperContent = @"
Param(
    [Parameter(ValueFromRemainingArguments = `$true)]
    [string[]]`$MesonArgs
)

`$ErrorActionPreference = 'Stop'
`$conanBuildPs1 = '$($conanBuildPs1 -replace "'", "''")'
`$conanBuildBat = '$($conanBuildBat -replace "'", "''")'

function Import-BatchEnvironment {
    param(
        [Parameter(Mandatory = `$true)]
        [string]`$BatchFile
    )

    if (-not (Test-Path `$BatchFile)) {
        return `$false
    }

    `$varsLoaded = `$false
    & `$env:COMSPEC /s /c "`"`$BatchFile`" && set" | ForEach-Object {
        `$name, `$value = `$_ -split '=', 2
        if (`$name -and `$value) {
            Set-Item -Force -Path "ENV:\`$name" -Value `$value
            `$varsLoaded = `$true
        }
    }

    return `$varsLoaded
}

if (Test-Path `$conanBuildPs1) {
    . `$conanBuildPs1
} elseif (Test-Path `$conanBuildBat) {
    if (-not (Import-BatchEnvironment -BatchFile `$conanBuildBat)) {
        throw "Failed to import Conan batch environment: `$conanBuildBat"
    }
}
& meson @MesonArgs
exit `$LASTEXITCODE
"@
    Set-Content -Path $mesonWrapperPs1 -Value $wrapperContent -Encoding ASCII
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

# Mobile bindings are off by default; allow CI/users to opt-in.
$enableMobileBindingsArg = $null
if ($env:YAMS_ENABLE_MOBILE_BINDINGS -eq 'true') {
    $enableMobileBindingsArg = '-Denable-mobile-bindings=true'
    Write-Host 'Mobile bindings enabled (YAMS_ENABLE_MOBILE_BINDINGS=true)'
} elseif ($env:YAMS_ENABLE_MOBILE_BINDINGS -eq 'false') {
    $enableMobileBindingsArg = '-Denable-mobile-bindings=false'
    Write-Host 'Mobile bindings disabled (YAMS_ENABLE_MOBILE_BINDINGS=false)'
}

if (-not (Test-Path (Join-Path $buildDir 'meson-private'))) {
    Write-Host 'Configuring Meson builddir...'
    $buildTypeLower = if ($BuildType -eq 'Release') { 'debugoptimized' } else { $BuildType.ToLower() }
    
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

    # Auto-enable Glint NL entity extractor plugin (GLiNER-based)
    # Requires ONNX Runtime which is already a dependency
    $enableGlint = $false
    if ($env:YAMS_DISABLE_ONNX -ne 'true') {
        Write-Host "Enabling Glint NL entity extractor plugin"
        $enableGlint = $true
    }

    # Auto-detect libSQL backend: prefer libsql when subproject or pkg-config is available
    $dbBackend = if ($env:YAMS_DATABASE_BACKEND) { $env:YAMS_DATABASE_BACKEND } else { 'libsql' }
    $dbBackend = $dbBackend.ToLowerInvariant()
    if ($dbBackend -ne 'libsql' -and $dbBackend -ne 'sqlite') {
        Write-Error "Unknown database backend: $dbBackend. Expected libsql or sqlite."
    }

    if ($dbBackend -eq 'libsql' -and $SystemDeps) {
        $libsqlPkgFound = $false
        if (Get-Command pkg-config -ErrorAction SilentlyContinue) {
            if ((pkg-config --exists libsql) -or (pkg-config --exists libsql-sqlite3)) {
                $libsqlPkgFound = $true
            }
        }

        if (-not $libsqlPkgFound) {
            Write-Warning 'System dependency mode: libSQL not found via pkg-config; falling back to sqlite'
            $dbBackend = 'sqlite'
        }
    }

    if ($dbBackend -eq 'libsql') {
        $libsqlPkgFound = $false
        if (Get-Command pkg-config -ErrorAction SilentlyContinue) {
            if ((pkg-config --exists libsql) -or (pkg-config --exists libsql-sqlite3)) {
                $libsqlPkgFound = $true
            }
        }

        if (-not $libsqlPkgFound -and -not (Test-Path 'subprojects\libsql')) {
            if (Test-Path 'subprojects\libsql.wrap') {
                if ($SystemDeps) {
                    Write-Warning 'libSQL not found via system dependencies; falling back to sqlite'
                    $dbBackend = 'sqlite'
                } elseif ($Offline) {
                    Write-Warning 'Offline mode: not fetching libSQL subproject; falling back to sqlite'
                    $dbBackend = 'sqlite'
                } elseif (Get-Command meson -ErrorAction SilentlyContinue) {
                    Write-Host 'Fetching libSQL subproject (meson wrap)...'
                    meson subprojects download libsql | Out-Null
                    if ($LASTEXITCODE -ne 0) {
                        Write-Warning 'Failed to fetch libSQL subproject; falling back to sqlite'
                        $dbBackend = 'sqlite'
                    }
                } else {
                    Write-Warning 'Meson not found; cannot fetch libSQL subproject. Falling back to sqlite'
                    $dbBackend = 'sqlite'
                }
            } else {
                Write-Warning 'libSQL wrap not found; falling back to sqlite'
                $dbBackend = 'sqlite'
            }
        }
    }

    Write-Host "Database backend: $dbBackend"

    $wrapMode = if ($env:YAMS_MESON_WRAP_MODE) {
        $env:YAMS_MESON_WRAP_MODE
    } elseif ($SystemDeps) {
        'nofallback'
    } elseif ($Offline) {
        'nodownload'
    } else {
        'default'
    }

    # Build meson command arguments as a proper array
    $mesonArgs = @('setup', $buildDir, "--buildtype=$buildTypeLower", "--prefix=$InstallPrefix", "-Denable-modules=$enableModulesFlag", "-Ddatabase-backend=$dbBackend")
    if ($enableZyp) { $mesonArgs += '-Dplugin-zyp=true' }
    if ($enableGlint) { $mesonArgs += '-Dplugin-glint=true' }
    if ($enableMobileBindingsArg) { $mesonArgs += $enableMobileBindingsArg }
    if ($mesonToolchainArg) { $mesonArgs += $mesonToolchainArg }
    if ($mesonToolchainFile) { $mesonArgs += $mesonToolchainFile }
    if ($wrapMode -ne 'default') { $mesonArgs += @('--wrap-mode', $wrapMode) }
    if ($env:YAMS_PKG_CONFIG_PATH) { $mesonArgs += @('--pkg-config-path', $env:YAMS_PKG_CONFIG_PATH) }
    if ($env:YAMS_BUILD_PKG_CONFIG_PATH) { $mesonArgs += @('--build.pkg-config-path', $env:YAMS_BUILD_PKG_CONFIG_PATH) }
    if ($env:YAMS_CMAKE_PREFIX_PATH) { $mesonArgs += @('--cmake-prefix-path', $env:YAMS_CMAKE_PREFIX_PATH) }
    if ($env:YAMS_BUILD_CMAKE_PREFIX_PATH) { $mesonArgs += @('--build.cmake-prefix-path', $env:YAMS_BUILD_CMAKE_PREFIX_PATH) }
    if ($buildTypeLower -eq 'debugoptimized') { $mesonArgs += '-Db_ndebug=true' } else { $mesonArgs += '-Db_ndebug=false' }
    if ($env:YAMS_DISABLE_RE2 -eq 'true') { $mesonArgs += '-Denable-re2=disabled' }
    
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
    $buildTypeLower = if ($BuildType -eq 'Release') { 'debugoptimized' } else { $BuildType.ToLower() }
    # Auto-detect libSQL backend for reconfigure as well
    $dbBackend = if ($env:YAMS_DATABASE_BACKEND) { $env:YAMS_DATABASE_BACKEND } else { 'libsql' }
    $dbBackend = $dbBackend.ToLowerInvariant()
    if ($dbBackend -ne 'libsql' -and $dbBackend -ne 'sqlite') {
        Write-Error "Unknown database backend: $dbBackend. Expected libsql or sqlite."
    }

    if ($dbBackend -eq 'libsql' -and $SystemDeps) {
        $libsqlPkgFound = $false
        if (Get-Command pkg-config -ErrorAction SilentlyContinue) {
            if ((pkg-config --exists libsql) -or (pkg-config --exists libsql-sqlite3)) {
                $libsqlPkgFound = $true
            }
        }

        if (-not $libsqlPkgFound) {
            Write-Warning 'System dependency mode: libSQL not found via pkg-config; falling back to sqlite'
            $dbBackend = 'sqlite'
        }
    }

    if ($dbBackend -eq 'libsql') {
        $libsqlPkgFound = $false
        if (Get-Command pkg-config -ErrorAction SilentlyContinue) {
            if ((pkg-config --exists libsql) -or (pkg-config --exists libsql-sqlite3)) {
                $libsqlPkgFound = $true
            }
        }

        if (-not $libsqlPkgFound -and -not (Test-Path 'subprojects\libsql')) {
            if (Test-Path 'subprojects\libsql.wrap') {
                if ($SystemDeps) {
                    Write-Warning 'libSQL not found via system dependencies; falling back to sqlite'
                    $dbBackend = 'sqlite'
                } elseif ($Offline) {
                    Write-Warning 'Offline mode: not fetching libSQL subproject; falling back to sqlite'
                    $dbBackend = 'sqlite'
                } elseif (Get-Command meson -ErrorAction SilentlyContinue) {
                    Write-Host 'Fetching libSQL subproject (meson wrap)...'
                    meson subprojects download libsql | Out-Null
                    if ($LASTEXITCODE -ne 0) {
                        Write-Warning 'Failed to fetch libSQL subproject; falling back to sqlite'
                        $dbBackend = 'sqlite'
                    }
                } else {
                    Write-Warning 'Meson not found; cannot fetch libSQL subproject. Falling back to sqlite'
                    $dbBackend = 'sqlite'
                }
            } else {
                Write-Warning 'libSQL wrap not found; falling back to sqlite'
                $dbBackend = 'sqlite'
            }
        }
    }

    Write-Host "Database backend: $dbBackend"

    $wrapMode = if ($env:YAMS_MESON_WRAP_MODE) {
        $env:YAMS_MESON_WRAP_MODE
    } elseif ($SystemDeps) {
        'nofallback'
    } elseif ($Offline) {
        'nodownload'
    } else {
        'default'
    }

    # Use --reconfigure to handle Meson version upgrades gracefully
    $reconfigureArgs = @('setup', '--reconfigure', $buildDir, "-Dprefix=$InstallPrefix", "-Ddatabase-backend=$dbBackend")
    if ($mesonToolchainArg) { $reconfigureArgs += $mesonToolchainArg }
    if ($mesonToolchainFile) { $reconfigureArgs += $mesonToolchainFile }
    if ($enableMobileBindingsArg) { $reconfigureArgs += $enableMobileBindingsArg }
    if ($wrapMode -ne 'default') { $reconfigureArgs += @('--wrap-mode', $wrapMode) }
    if ($env:YAMS_PKG_CONFIG_PATH) { $reconfigureArgs += @('--pkg-config-path', $env:YAMS_PKG_CONFIG_PATH) }
    if ($env:YAMS_BUILD_PKG_CONFIG_PATH) { $reconfigureArgs += @('--build.pkg-config-path', $env:YAMS_BUILD_PKG_CONFIG_PATH) }
    if ($env:YAMS_CMAKE_PREFIX_PATH) { $reconfigureArgs += @('--cmake-prefix-path', $env:YAMS_CMAKE_PREFIX_PATH) }
    if ($env:YAMS_BUILD_CMAKE_PREFIX_PATH) { $reconfigureArgs += @('--build.cmake-prefix-path', $env:YAMS_BUILD_CMAKE_PREFIX_PATH) }
    if ($buildTypeLower -eq 'debugoptimized') { $reconfigureArgs += '-Db_ndebug=true' } else { $reconfigureArgs += '-Db_ndebug=false' }
    if ($env:YAMS_DISABLE_RE2 -eq 'true') { $reconfigureArgs += '-Denable-re2=disabled' }
    
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

    # Install to stage directory (destdir prepends to prefix, build-msi.ps1 handles finding files).
    # --no-rebuild: skip the implicit recompile; setup.ps1 already built above.
    meson install -C $buildDir --destdir $stageDir --no-rebuild
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
    if ($conanToolsOverride -and $mesonWrapperPs1) {
        Write-Host "Conan provided Meson/Ninja for this build directory."
        Write-Host "To install, run: $mesonWrapperPs1 install -C $buildDir"
    } else {
        Write-Host "To install, run: meson install -C $buildDir"
        if ($mesonWrapperPs1) {
            Write-Host "Fallback wrapper (replays Conan build env if needed): $mesonWrapperPs1"
        }
    }
    Write-Host "To build MSI:    .\setup.ps1 -BuildType $BuildType -Package"
    Write-Host "Note: You may need to add '$InstallPrefix\bin' to your PATH."
}
Write-Host "----------------------------------------------------------------"
