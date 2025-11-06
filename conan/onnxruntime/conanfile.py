from conan import ConanFile
from conan.tools.files import get, copy, save
import os


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
        get(self, url, strip_root=True, destination=self.source_folder)
    
    def package(self):
        # Copy headers
        copy(self, "*.h", src=os.path.join(self.source_folder, "include"),
             dst=os.path.join(self.package_folder, "include"), keep_path=True)
        
        # Copy libraries
        copy(self, "*.so*", src=os.path.join(self.source_folder, "lib"),
             dst=os.path.join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.a", src=os.path.join(self.source_folder, "lib"),
             dst=os.path.join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.dylib*", src=os.path.join(self.source_folder, "lib"),
             dst=os.path.join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.dll", src=os.path.join(self.source_folder, "lib"),
             dst=os.path.join(self.package_folder, "bin"), keep_path=False)
        copy(self, "*.lib", src=os.path.join(self.source_folder, "lib"),
             dst=os.path.join(self.package_folder, "lib"), keep_path=False)
        
        # Copy license
        copy(self, "LICENSE", src=self.source_folder,
             dst=os.path.join(self.package_folder, "licenses"), keep_path=False)
        
        # Generate pkg-config file manually
        pc_content = f"""prefix={self.package_folder}
libdir=${{prefix}}/lib
includedir=${{prefix}}/include

Name: onnxruntime
Description: ONNX Runtime - cross-platform ML inference
Version: {self.version}
Libs: -L${{libdir}} -lonnxruntime -lpthread -ldl -lm
Cflags: -I${{includedir}}
"""
        save(self, os.path.join(self.package_folder, "lib", "pkgconfig", "onnxruntime.pc"), pc_content)
    
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
        
        # CMake properties
        self.cpp_info.set_property("cmake_file_name", "onnxruntime")
        self.cpp_info.set_property("cmake_target_name", "onnxruntime::onnxruntime")
        
        # Pkg-config properties (for PkgConfigDeps generator)
        self.cpp_info.set_property("pkg_config_name", "onnxruntime")
