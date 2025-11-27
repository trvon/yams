# C++20 Modules Build Infrastructure
#
# This file detects C++20 module support and configures compiler-specific flags.
# Include this from the root meson.build when enable-modules=true.
#
# Exports:
#   yams_modules_supported - boolean, true if modules can be built
#   yams_module_args       - compiler arguments for module compilation
#   yams_module_flags      - additional flags for module interface units
#   yams_use_modules_def   - '-DYAMS_USE_MODULES=1' when enabled

cpp = meson.get_compiler('cpp')
cpp_id = cpp.get_id()

# Initialize module support variables
yams_modules_supported = false
yams_module_args = []
yams_module_flags = []
yams_use_modules_def = []

message('Checking C++20 module support...')

# Compiler-specific module detection and configuration
if cpp_id == 'msvc' or cpp_id == 'clang-cl'
  # MSVC / clang-cl: Native module support via /interface
  # Test with a simple module interface
  msvc_module_test = cpp.compiles('''
    export module test_module;
    export int test_func() { return 42; }
  ''',
    name: 'MSVC C++20 modules',
    args: ['/std:c++20', '/interface']
  )

  if msvc_module_test
    yams_modules_supported = true
    yams_module_args = ['/interface']
    yams_module_flags = ['/std:c++20']
    message('  MSVC: C++20 modules supported')
    message('  Module interface extension: .ixx')
    message('  BMI extension: .ifc')
  else
    warning('  MSVC: C++20 modules not supported (requires VS 2022 17.4+)')
  endif

elif cpp_id == 'gcc'
  # GCC: Requires -fmodules-ts flag
  # Note: GCC has limited module support - private module fragment not implemented
  gcc_module_test = cpp.compiles('''
    export module test_module;
    export int test_func() { return 42; }
  ''',
    name: 'GCC C++20 modules',
    args: ['-std=c++20', '-fmodules-ts']
  )

  if gcc_module_test
    yams_modules_supported = true
    yams_module_args = ['-fmodules-ts']
    yams_module_flags = ['-std=c++20', '-fmodules-ts']
    message('  GCC: C++20 modules supported (with limitations)')
    message('  Module interface extension: .cppm (or any)')
    message('  BMI extension: .gcm (in gcm.cache/)')
    warning('  GCC limitation: Private module fragments will cause errors')
    warning('  GCC limitation: Standard library modules not provided')
  else
    warning('  GCC: C++20 modules not supported (requires GCC 13+)')
  endif

elif cpp_id == 'clang'
  # Clang: Best module support, use -fmodules-ts or -fprebuilt-module-path
  clang_module_test = cpp.compiles('''
    export module test_module;
    export int test_func() { return 42; }
  ''',
    name: 'Clang C++20 modules',
    args: ['-std=c++20']
  )

  if clang_module_test
    yams_modules_supported = true
    # Clang 16+ doesn't need -fmodules-ts for standard modules
    yams_module_args = []
    yams_module_flags = ['-std=c++20']

    # Add prebuilt module path for BMI discovery
    yams_module_args += ['-fprebuilt-module-path=' + meson.current_build_dir()]

    message('  Clang: C++20 modules fully supported')
    message('  Module interface extension: .cppm')
    message('  BMI extension: .pcm')
  else
    # Try with -fmodules-ts for older Clang versions
    clang_module_test_ts = cpp.compiles('''
      export module test_module;
      export int test_func() { return 42; }
    ''',
      name: 'Clang C++20 modules (with -fmodules-ts)',
      args: ['-std=c++20', '-fmodules-ts']
    )

    if clang_module_test_ts
      yams_modules_supported = true
      yams_module_args = ['-fmodules-ts']
      yams_module_flags = ['-std=c++20', '-fmodules-ts']
      yams_module_args += ['-fprebuilt-module-path=' + meson.current_build_dir()]
      message('  Clang: C++20 modules supported (legacy mode with -fmodules-ts)')
    else
      warning('  Clang: C++20 modules not supported (requires Clang 16+)')
    endif
  endif

else
  warning('  Unknown compiler: ' + cpp_id + ' - modules not supported')
endif

# Set the YAMS_USE_MODULES define if modules are supported
if yams_modules_supported
  yams_use_modules_def = ['-DYAMS_USE_MODULES=1']
  message('C++20 modules enabled')
  message('  Compiler: ' + cpp_id)
  message('  Module args: ' + ' '.join(yams_module_args))
else
  message('C++20 modules disabled (compiler does not support modules)')
endif

# Helper function to get module-specific compiler args
# Usage: cpp_args: yams_module_cpp_args + other_args
yams_module_cpp_args = yams_module_args + yams_use_modules_def
