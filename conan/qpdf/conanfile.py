from conan import ConanFile
from conan.tools.files import get, rmdir, replace_in_file
from conan.tools.cmake import CMake, CMakeToolchain, cmake_layout
import os


class QpdfConan(ConanFile):
    name = "qpdf"
    version = "11.9.0"  # Latest stable version
    license = "Apache-2.0"
    url = "https://github.com/qpdf/qpdf"
    description = "QPDF: A Content-Preserving PDF Transformation System"
    topics = ("pdf", "pdf-parser", "pdf-manipulation")
    
    settings = "os", "compiler", "build_type", "arch"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "with_jpeg": [False, "libjpeg"],
        "with_ssl": [False, "openssl"],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "with_jpeg": "libjpeg",
        "with_ssl": "openssl",
    }
    
    def requirements(self):
        self.requires("zlib/1.3.1")
        if self.options.with_jpeg == "libjpeg":
            self.requires("libjpeg/9e")
        if self.options.with_ssl == "openssl":
            self.requires("openssl/3.2.0")

    def build_requirements(self):
        self.tool_requires("pkgconf/2.1.0")
    
    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC
    
    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")
    
    def layout(self):
        cmake_layout(self, src_folder="src")
    
    def source(self):
        # Download from GitHub
        get(self, f"https://github.com/qpdf/qpdf/archive/refs/tags/v{self.version}.tar.gz",
            strip_root=True, destination=self.source_folder)
    
    def generate(self):
        from conan.tools.env import VirtualBuildEnv
        env = VirtualBuildEnv(self)
        env.generate()

        from conan.tools.gnu import PkgConfigDeps
        pc = PkgConfigDeps(self)
        pc.generate()

        from conan.tools.cmake import CMakeDeps
        deps = CMakeDeps(self)
        deps.set_property("zlib", "cmake_file_name", "ZLIB")
        deps.set_property("libjpeg", "cmake_file_name", "JPEG")
        deps.set_property("openssl", "cmake_file_name", "OpenSSL")
        deps.generate()

        tc = CMakeToolchain(self)
        # Force PIC for all targets - critical for use in plugins
        tc.variables["CMAKE_POSITION_INDEPENDENT_CODE"] = "ON"
        if self.settings.os != "Windows":
            tc.variables["CMAKE_CXX_FLAGS"] = "-fPIC"
            tc.variables["CMAKE_C_FLAGS"] = "-fPIC"
        tc.variables["BUILD_SHARED_LIBS"] = self.options.shared
        tc.variables["BUILD_STATIC_LIBS"] = not self.options.shared
        # Disable unnecessary features - build only the library
        tc.variables["BUILD_DOC"] = "OFF"
        tc.variables["BUILD_EXAMPLES"] = "OFF"
        tc.variables["INSTALL_EXAMPLES"] = "OFF"
        tc.variables["BUILD_TESTING"] = "OFF"
        tc.variables["INSTALL_CMAKE_PACKAGE"] = "ON"
        tc.variables["INSTALL_PKGCONFIG"] = "ON"
        # Configure dependencies
        tc.variables["REQUIRE_CRYPTO_OPENSSL"] = self.options.with_ssl == "openssl"
        tc.variables["USE_IMPLICIT_CRYPTO"] = "OFF"
        tc.generate()
    
    def build(self):
        # Patch QPDFLogger.hh to remove #warning which MSVC doesn't support
        # Also patch PointerHolder.hh and others
        import glob
        import re
        
        # Patch all header files in include/qpdf
        # The source is in self.source_folder/include/qpdf because of strip_root=True in source()
        # But cmake_layout sets src_folder="src", so it's src/include/qpdf
        include_dir = os.path.join(self.source_folder, "include", "qpdf")
        self.output.info(f"Patching headers in {include_dir}")
        
        if os.path.exists(include_dir):
            for ext in ["*.hh", "*.h"]:
                for filepath in glob.glob(os.path.join(include_dir, ext)):
                    # Read file content
                    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                    
                    # Replace #warning with // IGNORED WARNING, handling optional whitespace
                    # Case insensitive just in case
                    new_content = re.sub(r'^\s*#\s*warning', '// IGNORED WARNING', content, flags=re.MULTILINE | re.IGNORECASE)
                    
                    if content != new_content:
                        with open(filepath, 'w', encoding='utf-8') as f:
                            f.write(new_content)
                        self.output.info(f"Patched #warning in {filepath}")
        else:
            self.output.warning(f"Include directory not found: {include_dir}")

        cmake = CMake(self)
        cmake.configure()
        # Build only the library target, skip tests/examples
        cmake.build(target="libqpdf")
    
    def package(self):
        from conan.tools.files import copy
        import glob
        import re
        
        # Manual install since CMake install tries to install binaries we didn't build
        copy(self, "*.h", os.path.join(self.source_folder, "include"), 
             os.path.join(self.package_folder, "include"), keep_path=True)
        copy(self, "*.hh", os.path.join(self.source_folder, "include"),
             os.path.join(self.package_folder, "include"), keep_path=True)
        copy(self, "*.a", self.build_folder, 
             os.path.join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.lib", self.build_folder, 
             os.path.join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.so*", self.build_folder,
             os.path.join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.dll", self.build_folder,
             os.path.join(self.package_folder, "bin"), keep_path=False)
        copy(self, "*.dylib", self.build_folder,
             os.path.join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.pc", self.build_folder,
             os.path.join(self.package_folder, "lib", "pkgconfig"), keep_path=False)
        copy(self, "LICENSE.txt", self.source_folder,
             os.path.join(self.package_folder, "licenses"), keep_path=False)

        # Patch headers in the package folder to ensure they are fixed
        # This handles cases where package() copies unpatched files from source
        include_dir = os.path.join(self.package_folder, "include", "qpdf")
        self.output.info(f"Patching headers in package: {include_dir}")
        
        if os.path.exists(include_dir):
            for ext in ["*.hh", "*.h"]:
                for filepath in glob.glob(os.path.join(include_dir, ext)):
                    # Read file content
                    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                    
                    # Replace #warning with // IGNORED WARNING
                    # Use a more flexible regex to catch variations
                    new_content = re.sub(r'^\s*#\s*warning', '// IGNORED WARNING', content, flags=re.MULTILINE | re.IGNORECASE)
                    
                    if content != new_content:
                        with open(filepath, 'w', encoding='utf-8') as f:
                            f.write(new_content)
                        self.output.info(f"Patched #warning in {filepath}")

    
    def package_info(self):
        self.cpp_info.libs = ["qpdf"]
        self.cpp_info.set_property("cmake_file_name", "qpdf")
        self.cpp_info.set_property("cmake_target_name", "qpdf::libqpdf")
        self.cpp_info.set_property("pkg_config_name", "libqpdf")
        
        if self.settings.os in ["Linux", "FreeBSD"]:
            self.cpp_info.system_libs.append("m")
