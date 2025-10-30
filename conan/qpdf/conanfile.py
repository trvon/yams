from conan import ConanFile
from conan.tools.files import get, rmdir
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
        from conan.tools.env import Environment
        
        # Force fPIC via environment variables - most reliable method
        env = Environment()
        env.append("CXXFLAGS", "-fPIC")
        env.append("CFLAGS", "-fPIC")
        env.vars(self).save_script("conanqpdfenv")
        
        tc = CMakeToolchain(self)
        # Force PIC for all targets
        tc.variables["CMAKE_POSITION_INDEPENDENT_CODE"] = "ON"
        tc.variables["CMAKE_CXX_FLAGS_INIT"] = "-fPIC"
        tc.variables["CMAKE_C_FLAGS_INIT"] = "-fPIC"
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
        cmake = CMake(self)
        cmake.configure()
        # Build only the library target, skip tests/examples
        cmake.build(target="libqpdf")
    
    def package(self):
        from conan.tools.files import copy
        # Manual install since CMake install tries to install binaries we didn't build
        copy(self, "*.h", os.path.join(self.source_folder, "include"), 
             os.path.join(self.package_folder, "include"), keep_path=True)
        copy(self, "*.hh", os.path.join(self.source_folder, "include"),
             os.path.join(self.package_folder, "include"), keep_path=True)
        copy(self, "*.a", self.build_folder, 
             os.path.join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.so*", self.build_folder,
             os.path.join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.pc", self.build_folder,
             os.path.join(self.package_folder, "lib", "pkgconfig"), keep_path=False)
        copy(self, "LICENSE.txt", self.source_folder,
             os.path.join(self.package_folder, "licenses"), keep_path=False)
    
    def package_info(self):
        self.cpp_info.libs = ["qpdf"]
        self.cpp_info.set_property("cmake_file_name", "qpdf")
        self.cpp_info.set_property("cmake_target_name", "qpdf::libqpdf")
        self.cpp_info.set_property("pkg_config_name", "libqpdf")
        
        if self.settings.os in ["Linux", "FreeBSD"]:
            self.cpp_info.system_libs.append("m")
