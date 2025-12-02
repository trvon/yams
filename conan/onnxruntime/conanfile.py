from conan import ConanFile
from conan.tools.files import get, copy, save, mkdir
import os
import shutil


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
    }
    default_options = {
        "shared": False,
        "fPIC": True,
    }
    
    def requirements(self):
        # ONNX Runtime has minimal external dependencies when building from source
        # Most deps are vendored in the source tree
        pass
    
    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC
    
    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")
    
    def _get_download_url(self):
        """Get platform-specific download URL"""
        base_url = f"https://github.com/microsoft/onnxruntime/releases/download/v{self.version}"  # noqa: E501
        
        os_name = str(self.settings.os)
        arch = str(self.settings.arch)
        
        if os_name == "Linux":
            if arch == "x86_64":
                filename = f"onnxruntime-linux-x64-{self.version}.tgz"
            elif arch == "aarch64" or arch == "armv8":
                filename = f"onnxruntime-linux-aarch64-{self.version}.tgz"
            else:
                raise Exception(f"Unsupported arch: {arch}")
        elif os_name == "Macos":
            if arch == "x86_64":
                filename = f"onnxruntime-osx-x86_64-{self.version}.tgz"
            elif arch == "armv8":
                filename = f"onnxruntime-osx-arm64-{self.version}.tgz"
            else:
                raise Exception(f"Unsupported arch: {arch}")
        elif os_name == "Windows":
            if arch == "x86_64":
                filename = f"onnxruntime-win-x64-{self.version}.zip"
            else:
                raise Exception(f"Unsupported arch: {arch}")
        else:
            raise Exception(f"Unsupported OS: {os_name}")
        
        return f"{base_url}/{filename}"
    
    def source(self):
        # Download prebuilt binaries from GitHub releases
        # Note: URL determination done in build() where settings are available
        pass
    
    def build(self):
        # Download prebuilt binaries in build step (settings available here)
        url = self._get_download_url()
        self.output.info(f"Downloading ONNX Runtime from: {url}")
        # Extract to a temp location, then we'll find the extracted directory
        get(self, url, destination=self.build_folder, strip_root=False)
    
    def package(self):
        # The prebuilt archive extracts to a subdirectory like onnxruntime-osx-arm64-1.23.2
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
            raise Exception("ONNX Runtime directory not found after extraction")
        
        include_src = os.path.join(extracted_dir, "include")
        if not os.path.exists(include_src):
            self.output.error(f"Include directory not found at {include_src}")
            self.output.info(f"Extracted dir contents: {os.listdir(extracted_dir)}")
            raise Exception("ONNX Runtime headers not found in expected location")
        
        # Copy headers directly, not preserving the parent directory name
        copy(self, "*.h", 
             src=include_src,
             dst=os.path.join(self.package_folder, "include"), 
             keep_path=True)
        
        copy(self, "*.hpp", 
             src=include_src,
             dst=os.path.join(self.package_folder, "include"), 
             keep_path=True)
        
        # Copy libraries, preserving symlinks for proper ldconfig behavior
        lib_src = os.path.join(extracted_dir, "lib")
        lib_dst = os.path.join(self.package_folder, "lib")
        bin_dst = os.path.join(self.package_folder, "bin")
        mkdir(self, lib_dst)

        # Process library files, preserving symlinks to avoid ldconfig warnings
        # ("is not a symbolic link" error occurs when symlinks are copied as files)
        for item in os.listdir(lib_src):
            src_path = os.path.join(lib_src, item)

            # Determine destination based on file type
            if item.endswith('.dll'):
                mkdir(self, bin_dst)
                dst_path = os.path.join(bin_dst, item)
            elif item.endswith(('.so', '.a', '.dylib', '.lib')) or '.so.' in item or '.dylib.' in item:
                dst_path = os.path.join(lib_dst, item)
            else:
                continue  # Skip non-library files

            if os.path.islink(src_path):
                # Preserve symbolic links exactly as they are
                link_target = os.readlink(src_path)
                if os.path.exists(dst_path) or os.path.islink(dst_path):
                    os.remove(dst_path)
                os.symlink(link_target, dst_path)
                self.output.info(f"Preserved symlink: {item} -> {link_target}")
            elif os.path.isfile(src_path):
                shutil.copy2(src_path, dst_path)
                self.output.info(f"Copied library: {item}")
        
        # Copy license if present
        license_path = os.path.join(extracted_dir, "LICENSE")
        if os.path.exists(license_path):
            copy(self, "LICENSE", 
                 src=extracted_dir,
                 dst=os.path.join(self.package_folder, "licenses"), 
                 keep_path=False)
        
        # Generate pkg-config file to ensure proper include path resolution
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
        self.cpp_info.libs = ["onnxruntime"]
        
        # Set include dirs
        self.cpp_info.includedirs = ["include"]
        
        # System libs
        if self.settings.os == "Linux":
            self.cpp_info.system_libs = ["pthread", "dl", "m"]
        
        # Runtime library path
        if self.settings.os in ["Linux", "Macos"]:
            self.cpp_info.libdirs = ["lib"]
            # Add RPATH for runtime library discovery
            self.cpp_info.rpath_dirs = ["lib"]
        elif self.settings.os == "Windows":
            # On Windows, DLLs go in bin, .lib files in lib
            self.cpp_info.libdirs = ["lib"]
            self.cpp_info.bindirs = ["bin"]
        
        # CMake properties
        self.cpp_info.set_property("cmake_file_name", "onnxruntime")
        self.cpp_info.set_property("cmake_target_name", "onnxruntime::onnxruntime")
        
        # Pkg-config properties (for PkgConfigDeps generator)
        self.cpp_info.set_property("pkg_config_name", "onnxruntime")
