from conan import ConanFile
import os
from conan.tools.layout import basic_layout
from conan.tools.build import check_min_cppstd


class YamsConan(ConanFile):
    name = "yams"
    version = "0.7.1"
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
        "enable_onnx": True,  # ONNX enabled by default; can be disabled to drop Boost
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
        self.requires("tl-expected/1.1.0")
        # Boost is used directly (e.g., boost.asio for daemon comms); declare as a direct requirement
        # Using only override=True will not pull Boost into the graph when no transitive dep needs it
        self.requires("boost/1.83.0")
        if self.options.enable_tui:
            self.requires("ncurses/6.4")
        if self.options.enable_onnx:
            self.requires("onnxruntime/1.18.1")
        self.requires("xz_utils/5.4.5")
        if self.options.enable_pdf:
            self.requires("pdfium/95.0.4629")
            self.requires("libmediainfo/22.03")
            # macOS builds hit a CMake policy issue when pdfium resolves openjpeg/2.5.0.
            # Prefer openjpeg/2.5.3 on macOS only (ConanCenter provides it), while leaving
            # Linux/Docker unpinned to avoid remote-resolution failures.
            try:
                if str(self.settings.os) == "Macos":
                    self.requires("openjpeg/2.5.3", override=True)
            except Exception:
                pass

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
        self.options["spdlog"].header_only = False

    def validate(self):
        check_min_cppstd(self, "20")

    def layout(self):
        basic_layout(self)
