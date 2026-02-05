from conan import ConanFile
from conan.tools.files import get, copy, save, mkdir, download, chdir
from conan.tools.scm import Git
from conan.errors import ConanInvalidConfiguration
import os
import shutil
import subprocess


class OnnxRuntimeConan(ConanFile):
    name = "onnxruntime"
    version = "1.23.2"
    license = "MIT"
    url = "https://github.com/microsoft/onnxruntime"
    description = "ONNX Runtime: cross-platform, high performance ML inferencing and training accelerator"
    topics = ("onnx", "machine-learning", "inference", "neural-network")

    settings = "os", "compiler", "build_type", "arch"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        # GPU acceleration options:
        # - cuda: NVIDIA CUDA (prebuilt binary)
        # - directml: Windows DirectML (prebuilt binary)
        # - coreml: macOS CoreML (prebuilt binary)
        # - migraphx: AMD ROCm MIGraphX (built from source, requires ROCm)
        "with_gpu": [None, "cuda", "directml", "coreml", "migraphx"],
        # ROCm installation path (for migraphx builds)
        "rocm_path": ["ANY"],
        # Parallel build jobs (for source builds)
        "parallel_jobs": ["ANY"],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "with_gpu": None,
        "rocm_path": "/opt/rocm",
        "parallel_jobs": "4",
    }

    # Source builds need these
    exports_sources = []

    def requirements(self):
        # ONNX Runtime vendored most dependencies
        # For source builds, we rely on system ROCm/MIGraphX
        pass

    def build_requirements(self):
        if self.options.with_gpu == "migraphx":
            # Source builds need CMake 3.28+
            self.tool_requires("cmake/[>=3.28]")

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")

        # Validate GPU options per platform
        gpu = self.options.with_gpu
        os_name = str(self.settings.os)

        if gpu == "cuda":
            if os_name not in ["Linux", "Windows"]:
                raise ConanInvalidConfiguration(
                    f"CUDA is only supported on Linux and Windows, not {os_name}"
                )
        elif gpu == "directml":
            if os_name != "Windows":
                raise ConanInvalidConfiguration(
                    f"DirectML is only supported on Windows, not {os_name}"
                )
        elif gpu == "coreml":
            if os_name != "Macos":
                raise ConanInvalidConfiguration(
                    f"CoreML is only supported on macOS, not {os_name}"
                )
        elif gpu == "migraphx":
            if os_name != "Linux":
                raise ConanInvalidConfiguration(
                    f"MIGraphX is only supported on Linux, not {os_name}"
                )
            # Validate ROCm installation
            rocm_path = str(self.options.rocm_path)
            if not os.path.isdir(rocm_path):
                raise ConanInvalidConfiguration(
                    f"ROCm not found at {rocm_path}. Install ROCm or set "
                    f"-o onnxruntime/*:rocm_path=/path/to/rocm"
                )
            # Check for MIGraphX library (ROCm 7.x uses libmigraphx_c.so)
            migraphx_lib = os.path.join(rocm_path, "lib", "libmigraphx.so")
            migraphx_c_lib = os.path.join(rocm_path, "lib", "libmigraphx_c.so")
            if not os.path.exists(migraphx_lib) and not os.path.exists(migraphx_c_lib):
                raise ConanInvalidConfiguration(
                    f"MIGraphX not found at {rocm_path}. Ensure MIGraphX is installed "
                    f"(typically part of ROCm or install separately)."
                )
            self.output.info(f"MIGraphX found at {rocm_path}")

    def _is_source_build(self):
        """Check if this GPU option requires building from source"""
        return self.options.with_gpu == "migraphx"

    def _get_download_url(self):
        """Get platform-specific download URL for prebuilt binaries"""
        base_url = f"https://github.com/microsoft/onnxruntime/releases/download/v{self.version}"

        os_name = str(self.settings.os)
        arch = str(self.settings.arch)
        gpu = str(self.options.with_gpu) if self.options.with_gpu else None

        if os_name == "Linux":
            if arch == "x86_64":
                if gpu == "cuda":
                    filename = f"onnxruntime-linux-x64-gpu-{self.version}.tgz"
                else:
                    filename = f"onnxruntime-linux-x64-{self.version}.tgz"
            elif arch == "aarch64" or arch == "armv8":
                if gpu == "cuda":
                    raise ConanInvalidConfiguration(
                        "CUDA GPU build not available for Linux ARM64"
                    )
                filename = f"onnxruntime-linux-aarch64-{self.version}.tgz"
            else:
                raise ConanInvalidConfiguration(f"Unsupported arch: {arch}")
        elif os_name == "Macos":
            if arch == "x86_64":
                filename = f"onnxruntime-osx-x86_64-{self.version}.tgz"
            elif arch == "armv8":
                filename = f"onnxruntime-osx-arm64-{self.version}.tgz"
            else:
                raise ConanInvalidConfiguration(f"Unsupported arch: {arch}")
        elif os_name == "Windows":
            if arch == "x86_64":
                if gpu == "cuda":
                    filename = f"onnxruntime-win-x64-gpu-{self.version}.zip"
                elif gpu == "directml":
                    return self._get_directml_url()
                else:
                    filename = f"onnxruntime-win-x64-{self.version}.zip"
            else:
                raise ConanInvalidConfiguration(f"Unsupported arch: {arch}")
        else:
            raise ConanInvalidConfiguration(f"Unsupported OS: {os_name}")

        return f"{base_url}/{filename}"

    def _get_directml_url(self):
        """Get DirectML package URL from NuGet"""
        return f"https://www.nuget.org/api/v2/package/Microsoft.ML.OnnxRuntime.DirectML/{self.version}"

    def source(self):
        # Source download happens in build() where options are available
        pass

    def _build_from_source(self):
        """Build ONNX Runtime from source with MIGraphX support"""
        rocm_path = str(self.options.rocm_path)
        build_type = str(self.settings.build_type)
        parallel_jobs = str(self.options.parallel_jobs)

        # Clone the repository
        source_dir = os.path.join(self.build_folder, "onnxruntime-src")
        if not os.path.exists(source_dir):
            self.output.info(f"Cloning ONNX Runtime v{self.version}...")
            git = Git(self, folder=source_dir)
            git.clone(
                url="https://github.com/microsoft/onnxruntime.git",
                target=".",
                args=["--depth", "1", "--branch", f"v{self.version}", "--recursive"],
            )

        build_script = os.path.join(source_dir, "build.sh")
        if not os.path.exists(build_script):
            raise ConanInvalidConfiguration(
                f"build.sh not found at {build_script}. Source checkout may have failed."
            )

        # Prepare build arguments
        build_args = [
            build_script,
            "--config",
            build_type,
            "--build_dir",
            self.build_folder,
            "--use_migraphx",
            "--migraphx_home",
            rocm_path,
            "--rocm_home",
            rocm_path,
            "--parallel",
            parallel_jobs,
            "--skip_tests",
            "--build_shared_lib",
            "--cmake_extra_defines",
            f"CMAKE_HIP_COMPILER={rocm_path}/llvm/bin/clang++",
        ]

        # GPU targets - use auto-detected from environment or fall back to common targets
        # Auto-detection happens in setup.sh via rocminfo
        # Common architectures:
        #   gfx906  = Vega 20 (MI50/60, Radeon VII)
        #   gfx908  = CDNA (MI100)
        #   gfx90a  = CDNA2 (MI200 series)
        #   gfx942  = CDNA3 (MI300 series)
        #   gfx1030 = RDNA2 (RX 6800/6900)
        #   gfx1100 = RDNA3 (RX 7900)
        gpu_targets = os.environ.get("YAMS_ROCM_GPU_TARGETS", "")
        if gpu_targets:
            self.output.info(f"Using GPU targets from environment: {gpu_targets}")
            build_args.extend(
                [
                    "--cmake_extra_defines",
                    f"GPU_TARGETS={gpu_targets}",
                ]
            )
        else:
            self.output.info(
                "No GPU targets specified, ONNX Runtime will use its defaults"
            )

        self.output.info(f"Building ONNX Runtime with MIGraphX support...")
        self.output.info(f"ROCm path: {rocm_path}")
        self.output.info(f"Build type: {build_type}")
        self.output.info(f"Build command: {' '.join(build_args)}")

        # Set environment for ROCm
        env = os.environ.copy()
        env["ROCM_PATH"] = rocm_path
        env["HIP_PATH"] = rocm_path
        env["PATH"] = f"{rocm_path}/bin:{rocm_path}/llvm/bin:{env.get('PATH', '')}"
        env["LD_LIBRARY_PATH"] = f"{rocm_path}/lib:{env.get('LD_LIBRARY_PATH', '')}"

        # Run the build
        try:
            subprocess.run(
                build_args,
                cwd=source_dir,
                env=env,
                check=True,
                capture_output=False,  # Let output stream to console
            )
        except subprocess.CalledProcessError as e:
            raise ConanInvalidConfiguration(
                f"ONNX Runtime build failed with exit code {e.returncode}. "
                f"Check the build output for errors."
            )

    def build(self):
        if self._is_source_build():
            self._build_from_source()
        else:
            # Download prebuilt binaries
            url = self._get_download_url()
            self.output.info(f"Downloading ONNX Runtime from: {url}")
            get(self, url, destination=self.build_folder, strip_root=False)

    def _package_source_build(self):
        """Package libraries from source build"""
        build_type = str(self.settings.build_type)
        # ONNX Runtime build.sh creates output in build_folder/<build_type>
        build_output = os.path.join(self.build_folder, build_type)

        if not os.path.isdir(build_output):
            # Sometimes it's just in the build folder directly
            build_output = self.build_folder
            # Or try to find it
            for item in os.listdir(self.build_folder):
                candidate = os.path.join(self.build_folder, item)
                if os.path.isdir(candidate) and item in [
                    "Release",
                    "Debug",
                    "RelWithDebInfo",
                ]:
                    build_output = candidate
                    break

        self.output.info(f"Packaging from build output: {build_output}")

        # Find and copy headers from source (cloned in build step)
        source_dir = os.path.join(self.build_folder, "onnxruntime-src")
        include_src = os.path.join(source_dir, "include", "onnxruntime")

        # Copy main headers
        if os.path.exists(include_src):
            copy(
                self,
                "*.h",
                src=include_src,
                dst=os.path.join(self.package_folder, "include", "onnxruntime"),
                keep_path=True,
            )

        # Also copy the C API headers
        c_api_include = os.path.join(
            source_dir, "include", "onnxruntime", "core", "session"
        )
        if os.path.exists(c_api_include):
            copy(
                self,
                "*.h",
                src=c_api_include,
                dst=os.path.join(
                    self.package_folder, "include", "onnxruntime", "core", "session"
                ),
                keep_path=True,
            )

        # Copy built libraries
        lib_dst = os.path.join(self.package_folder, "lib")
        mkdir(self, lib_dst)

        # Look for libraries in various possible locations
        lib_search_paths = [
            build_output,
            os.path.join(build_output, "lib"),
            os.path.join(build_output, "Release"),
            os.path.join(build_output, "Release", "lib"),
        ]

        for search_path in lib_search_paths:
            if not os.path.isdir(search_path):
                continue
            for item in os.listdir(search_path):
                src_path = os.path.join(search_path, item)
                if not os.path.isfile(src_path) and not os.path.islink(src_path):
                    continue
                # Copy library files
                if item.endswith((".so", ".a")) or ".so." in item:
                    dst_path = os.path.join(lib_dst, item)
                    if os.path.islink(src_path):
                        link_target = os.readlink(src_path)
                        if os.path.exists(dst_path) or os.path.islink(dst_path):
                            os.remove(dst_path)
                        os.symlink(link_target, dst_path)
                        self.output.info(f"Preserved symlink: {item} -> {link_target}")
                    else:
                        shutil.copy2(src_path, dst_path)
                        self.output.info(f"Copied library: {item}")

        # Copy license
        license_path = os.path.join(source_dir, "LICENSE")
        if os.path.exists(license_path):
            copy(
                self,
                "LICENSE",
                src=source_dir,
                dst=os.path.join(self.package_folder, "licenses"),
                keep_path=False,
            )

    def _package_prebuilt(self):
        """Package prebuilt binaries"""
        # Find the extracted directory
        extracted_dir = None
        for item in os.listdir(self.build_folder):
            item_path = os.path.join(self.build_folder, item)
            if os.path.isdir(item_path) and item.startswith("onnxruntime-"):
                extracted_dir = item_path
                break

        if not extracted_dir:
            self.output.error(f"Could not find extracted onnxruntime directory")
            self.output.info(f"Build folder contents: {os.listdir(self.build_folder)}")
            raise ConanInvalidConfiguration(
                "ONNX Runtime directory not found after extraction"
            )

        include_src = os.path.join(extracted_dir, "include")
        if not os.path.exists(include_src):
            self.output.error(f"Include directory not found at {include_src}")
            self.output.info(f"Extracted dir contents: {os.listdir(extracted_dir)}")
            raise ConanInvalidConfiguration(
                "ONNX Runtime headers not found in expected location"
            )

        # Copy headers
        copy(
            self,
            "*.h",
            src=include_src,
            dst=os.path.join(self.package_folder, "include"),
            keep_path=True,
        )

        copy(
            self,
            "*.hpp",
            src=include_src,
            dst=os.path.join(self.package_folder, "include"),
            keep_path=True,
        )

        # Copy libraries, preserving symlinks
        lib_src = os.path.join(extracted_dir, "lib")
        lib_dst = os.path.join(self.package_folder, "lib")
        bin_dst = os.path.join(self.package_folder, "bin")
        mkdir(self, lib_dst)

        for item in os.listdir(lib_src):
            src_path = os.path.join(lib_src, item)

            if item.endswith(".dll"):
                mkdir(self, bin_dst)
                dst_path = os.path.join(bin_dst, item)
            elif (
                item.endswith((".so", ".a", ".dylib", ".lib"))
                or ".so." in item
                or ".dylib." in item
            ):
                dst_path = os.path.join(lib_dst, item)
            else:
                continue

            if os.path.islink(src_path):
                link_target = os.readlink(src_path)
                if os.path.exists(dst_path) or os.path.islink(dst_path):
                    os.remove(dst_path)
                os.symlink(link_target, dst_path)
                self.output.info(f"Preserved symlink: {item} -> {link_target}")
            elif os.path.isfile(src_path):
                shutil.copy2(src_path, dst_path)
                self.output.info(f"Copied library: {item}")

        # Copy license
        license_path = os.path.join(extracted_dir, "LICENSE")
        if os.path.exists(license_path):
            copy(
                self,
                "LICENSE",
                src=extracted_dir,
                dst=os.path.join(self.package_folder, "licenses"),
                keep_path=False,
            )

    def package(self):
        if self._is_source_build():
            self._package_source_build()
        else:
            self._package_prebuilt()

        # Generate pkg-config file
        pc_dir = os.path.join(self.package_folder, "lib", "pkgconfig")
        mkdir(self, pc_dir)
        pc_content = f"""prefix={self.package_folder}
libdir=${{prefix}}/lib
includedir=${{prefix}}/include

Name: onnxruntime
Description: ONNX Runtime - cross-platform ML inference
Version: {self.version}
Libs: -L${{libdir}} -lonnxruntime
Libs.private: -lpthread -ldl -lm
Cflags: -I${{includedir}}
"""
        save(self, os.path.join(pc_dir, "onnxruntime.pc"), pc_content)

    def package_info(self):
        gpu = str(self.options.with_gpu) if self.options.with_gpu else None

        # Base library
        self.cpp_info.libs = ["onnxruntime"]

        # GPU-specific libraries and defines
        if gpu == "cuda":
            self.cpp_info.libs.extend(
                [
                    "onnxruntime_providers_shared",
                    "onnxruntime_providers_cuda",
                ]
            )
            self.cpp_info.defines = ["YAMS_ONNX_CUDA_ENABLED=1"]
        elif gpu == "directml":
            self.cpp_info.libs.append("onnxruntime_providers_dml")
            self.cpp_info.defines = ["YAMS_ONNX_DIRECTML_ENABLED=1"]
        elif gpu == "coreml":
            self.cpp_info.defines = ["YAMS_ONNX_COREML_ENABLED=1"]
        elif gpu == "migraphx":
            # MIGraphX provider is built into the main library when built from source
            self.cpp_info.defines = ["YAMS_ONNX_MIGRAPHX_ENABLED=1"]
            # Add ROCm library path for runtime linking
            rocm_path = str(self.options.rocm_path)
            self.cpp_info.libdirs.append(os.path.join(rocm_path, "lib"))

        # Include dirs
        self.cpp_info.includedirs = ["include"]

        # System libs
        if self.settings.os == "Linux":
            self.cpp_info.system_libs = ["pthread", "dl", "m"]
            if gpu == "migraphx":
                # ROCm runtime libraries
                self.cpp_info.system_libs.extend(["amdhip64", "migraphx"])

        # Library paths
        if self.settings.os in ["Linux", "Macos"]:
            self.cpp_info.libdirs = ["lib"]
            self.cpp_info.rpath_dirs = ["lib"]
        elif self.settings.os == "Windows":
            self.cpp_info.libdirs = ["lib"]
            self.cpp_info.bindirs = ["bin"]

        # CMake properties
        self.cpp_info.set_property("cmake_file_name", "onnxruntime")
        self.cpp_info.set_property("cmake_target_name", "onnxruntime::onnxruntime")

        # Pkg-config properties
        self.cpp_info.set_property("pkg_config_name", "onnxruntime")
