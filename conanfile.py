from conan import ConanFile
from conan.tools.layout import basic_layout
from conan.tools.build import check_min_cppstd


# type: ignore[assignment,attr-defined,import-error]


class YamsConan(ConanFile):
    name = "yams"
    version = "0.13.0"  # x-release-please-version
    license = "GPL-3.0-or-later"
    author = "YAMS Contributors"
    url = "https://github.com/trvon/yams"
    description = "Yet Another Memory System - Persistent memory for LLMs"
    topics = ("storage", "content-addressed", "deduplication", "search")

    settings = "os", "compiler", "build_type", "arch"
    options = {
        "build_tests": [True, False],
        "build_benchmarks": [True, False],
        "enable_profiling": [True, False],
        "enable_onnx": [True, False],  # Gate ONNX Runtime and its
        # transitive graph
        "enable_symbol_extraction": [True, False],  # Gate symbol extraction
        # features and deps
        "enable_re2": [True, False],  # Gate RE2 regex engine for grep
    }
    default_options = {
        "build_tests": False,
        "build_benchmarks": False,
        "enable_profiling": False,
        "enable_onnx": True,  # ONNX enabled by default; can be disabled to drop Boost
        "enable_symbol_extraction": True,  # Enabled by default; disable to drop extractors
        "enable_re2": True,  # RE2 regex engine for grep (10-50x faster than std::regex)
    }

    generators = ("MesonToolchain", "PkgConfigDeps", "CMakeDeps")

    def requirements(self):
        # Core dependencies
        self.requires("spdlog/1.13.0")
        self.requires("cli11/2.4.1")
        self.requires("nlohmann_json/3.11.3")
        # Force sqlite3 build from source with FTS5 enabled
        self.requires(
            "sqlite3/3.44.2",
            options={
                "enable_fts5": True,
                "enable_json1": True,
                "enable_rtree": True,
                "omit_load_extension": False,
            },
            force=True,  # Force rebuild with our options
        )
        self.requires("zlib/1.3.1")
        self.requires("zstd/1.5.5")
        self.requires("libarchive/3.7.6")
        self.requires("openssl/3.2.0")
        self.requires("libcurl/8.10.1")
        self.requires("protobuf/3.21.12")
        self.requires("taglib/2.0")
        self.requires("tl-expected/1.1.0")
        self.requires("boost/1.85.0")

        if self.options.build_tests:  # type: ignore
            self.requires("gtest/1.15.0")
            # Catch2 must be a regular requirement so Meson dependency()
            # can resolve catch2/catch2-with-main via generated deps.
            self.requires("catch2/3.5.2")

        if self.options.enable_symbol_extraction:  # type: ignore
            try:
                self.requires("tree-sitter/0.25.9")
            except Exception:
                pass

        # ONNX Runtime for vector/embedding support (optional)
        if self.options.enable_onnx:  # type: ignore
            # Keep in sync with conan/onnxruntime recipe.
            # DirectML NuGet releases can lag behind GitHub releases.
            self.requires("onnxruntime/1.23.0")
            # Set compiler flags for clang 19
            if self.settings.compiler != "msvc":
                self.conf.append("tools.build:cxxflags", "-Wno-unused-but-set-variable")
                self.conf.append("tools.build:cxxflags", "-Wno-unused-function")
                self.conf.append("tools.build:cxxflags", "-Wno-deprecated-declarations")
            # Ensure TBB is available (required by ONNX Runtime)
            try:
                self.requires("onetbb/2021.12.0")
            except Exception:
                pass

        self.requires("xz_utils/5.4.5")

        # RE2 regex engine for high-performance grep (optional)
        if self.options.enable_re2:  # type: ignore
            self.requires("re2/20251105")

    def build_requirements(self):
        self.tool_requires("pkgconf/2.1.0")
        self.tool_requires("meson/[>=1.2.2 <2]")
        self.tool_requires("ninja/[>=1.10.2 <2]")

        if self.options.build_benchmarks:  # type: ignore
            self.requires("benchmark/1.8.3")
        if self.options.enable_profiling:  # type: ignore
            self.requires("tracy/0.13.1")

    def configure(self):
        # Force new C++11 ABI on Linux (Ubuntu 24.04+ compatibility)
        if self.settings.os == "Linux":  # type: ignore
            if self.settings.compiler in ["gcc", "clang"]:  # type: ignore
                self.settings.compiler.libcxx = "libstdc++11"  # type: ignore

        # Force SQLite3 to build from source with FTS5 (don't use pre-built binaries)
        self.options["sqlite3"].enable_fts5 = True  # type: ignore
        self.options["sqlite3"].enable_json1 = True  # type: ignore
        self.options["sqlite3"].enable_rtree = True  # type: ignore
        self.options["sqlite3"].omit_load_extension = False  # type: ignore
        self.options["libcurl"].with_ssl = "openssl"
        self.options["libcurl"].with_zlib = True
        self.options["libcurl"].shared = False
        self.options["openssl"].shared = False
        self.options["libarchive"].shared = False
        self.options[
            "libarchive"
        ].with_acl = False  # Avoid libacl link issues on Linux CI
        self.options["taglib"].shared = False
        self.options["spdlog"].header_only = False

        if self.options.enable_onnx:  # type: ignore
            self.options["onnxruntime"].fPIC = True
            self.options["onnxruntime"].shared = True
            # OneTBB requires hwloc with shared=True
            self.options["hwloc"].shared = True

    def validate(self):
        check_min_cppstd(self, "20")

    def layout(self):
        basic_layout(self)

    def generate(self):
        try:
            from conan.tools.meson import MesonToolchain
            from conan.tools.gnu import PkgConfigDeps
            from conan.tools.cmake import CMakeDeps
            from conan.tools.env import VirtualBuildEnv

            # Ensure PowerShell scripts are generated on Windows for setup.ps1
            if self.settings.os == "Windows":
                self.conf.define("tools.env.virtualenv:powershell", True)

            VirtualBuildEnv(self).generate()
            tc = MesonToolchain(self)
            if (
                self.settings.os == "Linux"
                and self.settings.compiler.libcxx == "libstdc++11"
            ):  # type: ignore
                tc.preprocessor_definitions["_GLIBCXX_USE_CXX11_ABI"] = "1"
            tc.generate()

            PkgConfigDeps(self).generate()
            CMakeDeps(self).generate()
        except Exception:
            pass
