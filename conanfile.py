from conan import ConanFile
import os
from conan.tools.layout import basic_layout
from conan.tools.build import check_min_cppstd

class YamsConan(ConanFile):
    name = "yams"
    version = "0.6.0"
    license = "APACHE 2.0"
    author = "YAMS Contributors"
    url = "https://github.com/trvon/yams"
    description = "Yet Another Memory System - Persistent memory for LLMs"
    topics = ("storage", "content-addressed", "deduplication", "search")

    settings = "os", "compiler", "build_type", "arch"
    options = {
        "build_cli": [True, False],
        "build_mcp_server": [True, False],
        "build_tests": [True, False],
        "build_benchmarks": [True, False],
        "enable_pdf": [True, False],
        "enable_tui": [True, False],  # Separate TUI from CLI
        "enable_onnx": [True, False],  # Gate ONNX Runtime and its transitive graph
        "enable_wasmtime": [True, False],  # Gate WASM host (wasmtime-cpp integration)
    }
    default_options = {
        "build_cli": True,
        "build_mcp_server": True,  # Enabled by default for v0.0.4
        "build_tests": False,
        "build_benchmarks": False,
        "enable_pdf": True,  # PDF support enabled by default
        "enable_tui": False,  # TUI disabled by default to reduce dependencies
        "enable_onnx": True,      # ONNX enabled by default; can be disabled to drop Boost
        "enable_wasmtime": True,  # WASM host enabled by default (bring your own wasmtime-cpp)
    }

    generators = ("MesonToolchain", "PkgConfigDeps")

    def requirements(self):
        # Core dependencies
        self.requires("spdlog/1.13.0")
        self.requires("cli11/2.4.1")
        self.requires("nlohmann_json/3.11.3")
        self.requires("sqlite3/3.44.2")
        self.requires("zlib/1.3.1")
        self.requires("zstd/1.5.5")
        self.requires("libarchive/3.7.6")
        self.requires("openssl/3.2.0")
        self.requires("libcurl/8.10.1")
        self.requires("protobuf/3.21.12")
        self.requires("taglib/2.0")
        self.requires('tl-expected/1.1.0')
        self.requires("boost/1.86.0", override=True)
        if self.options.enable_tui:
            self.requires("ncurses/6.4")
        if self.options.enable_onnx:
            self.requires("onnxruntime/1.18.1")
        self.requires("xz_utils/5.4.5")
        if self.options.enable_pdf:
            self.requires("pdfium/95.0.4629")
            self.requires("libmediainfo/22.03")
            # Allow environment-controlled OpenJPEG version to accommodate remote availability.
            # Default to 2.5.0 for broadest compatibility; set YAMS_OPENJPEG_VERSION=2.5.3 when available.
            # Default to 2.5.3 (works with modern CMake). Docker CI can override to 2.5.0 only
            # when 2.5.3 is unavailable in the remote.
            oj_ver = os.getenv("YAMS_OPENJPEG_VERSION", "2.5.3").strip() or "2.5.3"
            self.requires(f"openjpeg/{oj_ver}", override=True)

    def build_requirements(self):
        if self.options.build_tests:
            self.test_requires("gtest/1.15.0")
        if self.options.build_benchmarks:
            self.test_requires("benchmark/1.8.3")
        if self.settings.build_type == "Debug":
            self.requires("tracy/0.12.2")

    def configure(self):
        self.options["sqlite3"].enable_fts5 = True
        self.options["sqlite3"].enable_fts4 = True
        self.options["sqlite3"].enable_fts3_parenthesis = True
        self.options["libcurl"].with_ssl = "openssl"
        self.options["libcurl"].with_zlib = True
        self.options["libcurl"].shared = False
        self.options["openssl"].shared = False
        self.options["libarchive"].shared = False
        self.options["taglib"].shared = False

    def validate(self):
        check_min_cppstd(self, "20")

    def layout(self):
        basic_layout(self)
