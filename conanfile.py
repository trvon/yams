from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMake, cmake_layout, CMakeDeps
from conan.tools.build import check_min_cppstd

class YamsConan(ConanFile):
    name = "yams"
    version = "0.0.3"
    license = "MIT"
    author = "YAMS Contributors"
    url = "https://github.com/your-org/yams"
    description = "Yet Another Memory System - Persistent memory for LLMs"
    topics = ("storage", "content-addressed", "deduplication", "search")
    
    settings = "os", "compiler", "build_type", "arch"
    options = {
        "build_cli": [True, False],
        "build_mcp_server": [True, False],
        "build_tests": [True, False],
        "build_benchmarks": [True, False],
    }
    default_options = {
        "build_cli": True,
        "build_mcp_server": True,  # Enabled by default for v0.0.4
        "build_tests": False,
        "build_benchmarks": False,
    }
    
    generators = "CMakeDeps"  # CMakeToolchain is handled in generate()
    
    def requirements(self):
        # Core dependencies
        self.requires("spdlog/1.13.0")
        self.requires("cli11/2.4.1")
        self.requires("nlohmann_json/3.11.3")
        self.requires("sqlite3/3.44.2")
        self.requires("zstd/1.5.5")
        self.requires("lz4/1.9.4")
        self.requires("openssl/3.2.0")
        self.requires("protobuf/3.21.12")
        
        # For TUI
        if self.options.build_cli:
            self.requires("ncurses/6.4")
            # Note: ImTUI needs custom recipe as it's not in Conan Center
            
        # For HTTP API (if Drogon is used)
        if self.options.build_mcp_server:
            self.requires("drogon/1.9.1")
            self.requires("boost/1.83.0")
    
    def build_requirements(self):
        if self.options.build_tests:
            self.test_requires("gtest/1.14.0")
        if self.options.build_benchmarks:
            self.test_requires("benchmark/1.8.3")
    
    def configure(self):
        # SQLite3 configuration - enable FTS5 for full-text search
        self.options["sqlite3"].enable_fts5 = True
        self.options["sqlite3"].enable_fts4 = True  # For additional compatibility
        self.options["sqlite3"].enable_fts3_parenthesis = True  # For advanced query syntax
        
        # Drogon configuration
        if self.options.build_mcp_server:
            self.options["drogon"].with_ctl = False
            self.options["drogon"].with_orm = False
            self.options["drogon"].with_yaml = False
            self.options["drogon"].with_redis = False
    
    def validate(self):
        check_min_cppstd(self, "20")
    
    def layout(self):
        cmake_layout(self)
    
    def generate(self):
        # The toolchain is auto-generated, but we can configure cache variables
        # by creating a toolchain_file.cmake in the generators folder
        from conan.tools.cmake import CMakeToolchain
        tc = CMakeToolchain(self, generator="Unix Makefiles")
        tc.variables["YAMS_USE_CONAN"] = "ON"  # Must be string "ON" for CMake
        # Convert Conan boolean options to CMake ON/OFF strings
        tc.variables["YAMS_BUILD_CLI"] = "ON" if self.options.build_cli else "OFF"
        tc.variables["YAMS_BUILD_MCP_SERVER"] = "ON" if self.options.build_mcp_server else "OFF"
        tc.variables["YAMS_BUILD_TESTS"] = "ON" if self.options.build_tests else "OFF"
        tc.variables["YAMS_BUILD_BENCHMARKS"] = "ON" if self.options.build_benchmarks else "OFF"
        tc.generate()
    
    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
    
    def package(self):
        cmake = CMake(self)
        cmake.install()