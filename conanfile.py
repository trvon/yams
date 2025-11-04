from conan import ConanFile
import os  # noqa: F401
from conan.tools.layout import basic_layout
from conan.tools.build import check_min_cppstd


# type: ignore[assignment,attr-defined,import-error]

class YamsConan(ConanFile):
    name = "yams"
    version = "0.8.0"
    license = "GPL-3.0-or-later"
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
        "enable_onnx": [True, False],  # Gate ONNX Runtime and its
        # transitive graph
        "enable_wasmtime": [True, False],  # Gate WASM host
        # (wasmtime-cpp integration)
        "enable_symbol_extraction": [True, False],  # Gate symbol extraction
        # features and deps
    }
    default_options = {
        "build_cli": True,
        "build_mcp_server": True,  # Enabled by default for v0.0.4
        "build_tests": False,
        "build_benchmarks": False,
        "enable_pdf": True,  # PDF support enabled by default
        "enable_tui": True,  # TUI enabled by default
        "enable_onnx": True,  # ONNX enabled by default; can be disabled to drop Boost
        "enable_wasmtime": True,  # WASM host enabled by default (bring your own wasmtime-cpp)
        "enable_symbol_extraction": True,  # Enabled by default; disable to drop extractors
    }

    generators = ("MesonToolchain", "PkgConfigDeps", "CMakeDeps")

    def requirements(self):
        # Core dependencies
        self.requires("spdlog/1.13.0")
        self.requires("cli11/2.4.1")
        self.requires("nlohmann_json/3.11.3")
        # sqlite3 with FTS5 enabled by default (no user flags needed)
        # Also enable JSON1 and keep load_extension available for FTS5.
        # Conan v2 requires passing options at requires()-time; mutating in
        # configure() is unreliable for downstream packages.
        self.requires(
            "sqlite3/3.44.2",
            options={
                # ConanCenter sqlite3 options (3.44.x)
                "enable_fts5": True,
                "enable_json1": True,
                "enable_rtree": True,
                # Keep extension loading so FTS virtual tables can operate
                "omit_load_extension": False,
            },
        )
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

        # TUI framework dependencies
        if self.options.enable_tui:  # type: ignore
            self.requires("ftxui/5.0.0")

        if self.options.enable_symbol_extraction:  # type: ignore
            try:
                self.requires("tree-sitter/0.25.9")
            except Exception:
                pass

        if self.options.enable_onnx:  # type: ignore
            # Build ONNX Runtime from source to ensure clang 19 compatibility
            self.requires("onnxruntime/1.18.1", override=True)
            # Set compiler flags for clang 19
            self.conf.append("tools.build:cxxflags", "-Wno-unused-but-set-variable")
            self.conf.append("tools.build:cxxflags", "-Wno-unused-function")
            self.conf.append("tools.build:cxxflags", "-Wno-deprecated-declarations")
            # Ensure TBB is available
            try:
                self.requires("onetbb/2021.12.0")
            except Exception:
                pass
        self.requires("xz_utils/5.4.5")
        if self.options.enable_pdf:  # type: ignore
            # Build qpdf from source with proper fPIC flags using custom recipe
            # This ensures PIC code generation for shared library linking
            self.requires("qpdf/11.9.0")  # Custom recipe in conan/qpdf/
            self.requires("libmediainfo/22.03")

    def build_requirements(self):
        # Use requires() instead of test_requires() to ensure pkg-config files
        # are generated for Meson to find gtest/benchmark dependencies
        if self.options.build_tests:  # type: ignore
            self.requires("gtest/1.15.0")
            # Add Catch2 for modern test framework migration (PBI-050)
            self.requires("catch2/3.5.2")
        if self.options.build_benchmarks:  # type: ignore
            self.requires("benchmark/1.8.3")
        if self.settings.build_type == "Debug":  # type: ignore
            self.requires("tracy/0.12.2")

    def configure(self):
        # Force new C++11 ABI globally for consistency (Ubuntu 24.04 compatibility)
        # This ensures all dependencies use the same ABI as the main project
        if self.settings.compiler == "gcc" or (self.settings.compiler == "clang" and self.settings.compiler.libcxx == "libstdc++11"):  # type: ignore
            self.settings.compiler.libcxx = "libstdc++11"  # type: ignore

        # Enable FTS extensions on sqlite3 across both legacy and modern option naming.
        try:
            if hasattr(self.options["sqlite3"], "fts5"):  # type: ignore
                self.options["sqlite3"].fts5 = True
            if hasattr(self.options["sqlite3"], "enable_fts5"):  # type: ignore
                self.options["sqlite3"].enable_fts5 = True
            if hasattr(self.options["sqlite3"], "fts4"):  # type: ignore
                self.options["sqlite3"].fts4 = True
            if hasattr(self.options["sqlite3"], "enable_fts4"):  # type: ignore
                self.options["sqlite3"].enable_fts4 = True
            if hasattr(self.options["sqlite3"], "fts3_parenthesis"):  # type: ignore
                self.options["sqlite3"].fts3_parenthesis = True
            if hasattr(self.options["sqlite3"], "enable_fts3_parenthesis"):  # type: ignore
                self.options["sqlite3"].enable_fts3_parenthesis = True
            if hasattr(self.options["sqlite3"], "json1"):  # type: ignore
                self.options["sqlite3"].json1 = True
        except Exception:
            pass
        self.options["libcurl"].with_ssl = "openssl"
        self.options["libcurl"].with_zlib = True
        self.options["libcurl"].shared = False
        self.options["openssl"].shared = False
        self.options["libarchive"].shared = False
        self.options["taglib"].shared = False
        self.options["spdlog"].header_only = False
        # Configure custom qpdf build with fPIC
        if self.options.enable_pdf:  # type: ignore
            self.options["qpdf"].fPIC = True
            self.options["qpdf"].shared = False
            self.options["qpdf"].with_jpeg = "libjpeg"
            self.options["qpdf"].with_ssl = "openssl"

    def validate(self):
        check_min_cppstd(self, "20")

    def layout(self):
        basic_layout(self)

    def generate(self):
        try:
            from conan.tools.meson import MesonToolchain
            from conan.tools.gnu import PkgConfigDeps
            from conan.tools.cmake import CMakeDeps

            MesonToolchain(self).generate()
            PkgConfigDeps(self).generate()
            CMakeDeps(self).generate()
        except Exception:
            # Fallback for older Conan: rely on class-level generators
            pass
