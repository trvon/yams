import re
import shutil
import subprocess

from conan import ConanFile
from conan.tools.build import check_min_cppstd, cross_building
from conan.tools.layout import basic_layout


# type: ignore[assignment,attr-defined,import-error]


def _parse_version_tuple(text: str):
    match = re.search(r"(\d+)\.(\d+)(?:\.(\d+))?", text)
    if not match:
        return None
    major = int(match.group(1))
    minor = int(match.group(2))
    patch = int(match.group(3) or "0")
    return (major, minor, patch)


def _tool_on_path_satisfies(name: str, minimum: tuple[int, int, int]) -> bool:
    tool_path = shutil.which(name)
    if not tool_path:
        return False

    try:
        result = subprocess.run(
            [tool_path, "--version"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.SubprocessError):
        return False

    version_text = (result.stdout or result.stderr or "").strip()
    version = _parse_version_tuple(version_text)
    if version is None:
        return False

    return minimum <= version < (2, 0, 0)


class YamsConan(ConanFile):
    name = "yams"
    version = "0.18.1"  # x-release-please-version
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
        "enable_onnx": [True, False],
        "enable_faiss": [False, True],
        "enable_symbol_extraction": [True, False],
        "enable_re2": [True, False],
    }
    default_options = {
        "build_tests": False,
        "build_benchmarks": False,
        "enable_profiling": False,
        "enable_onnx": True,
        "enable_symbol_extraction": True,
        "enable_re2": True,
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
        if self.settings.os not in ["iOS", "Android"]:  # type: ignore
            self.requires("libarchive/3.7.6")
        self.requires("openssl/3.2.0")
        self.requires("libcurl/8.10.1")
        self.requires("protobuf/3.21.12")
        if self.settings.os not in ["iOS", "Android"]:  # type: ignore
            self.requires("taglib/2.0")
        self.requires("tl-expected/1.1.0")
        self.requires("boost/1.85.0")

        if self.options.build_tests:  # type: ignore
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
            # Windows uses the packaged ONNX Runtime binaries directly and does
            # not need the extra oneTBB/hwloc graph that breaks local MSVC 195
            # builds in Conan Center's hwloc recipe.
            if self.settings.os != "Windows":  # type: ignore
                try:
                    self.requires("onetbb/2021.12.0")
                except Exception:
                    pass

        self.requires("xz_utils/5.8.3")

        # RE2 regex engine for high-performance grep (optional)
        if self.options.enable_re2:  # type: ignore
            self.requires("re2/20251105")

        if self.options.enable_faiss:  # type: ignore
            self.requires("faiss/1.12.0")

    def build_requirements(self):
        self.tool_requires("pkgconf/2.1.0")
        if _tool_on_path_satisfies("meson", (1, 2, 2)):
            self.output.info("Using Meson from PATH; skipping Conan Meson tool_requires")
        else:
            self.tool_requires("meson/[>=1.2.2 <2]")

        if _tool_on_path_satisfies("ninja", (1, 10, 2)):
            self.output.info("Using Ninja from PATH; skipping Conan Ninja tool_requires")
        else:
            self.tool_requires("ninja/[>=1.10.2 <2]")

        if cross_building(self):
            # Cross-builds still need a build-machine protoc for Meson codegen.
            self.tool_requires("protobuf/3.21.12")

        if self.options.build_benchmarks:  # type: ignore
            self.requires("benchmark/1.8.3")
        if self.options.enable_profiling:  # type: ignore
            self.requires("tracy/0.13.1")

    def _has_openmp(self):
        """Check whether the current compiler can compile OpenMP code.

        GCC and MSVC have built-in OpenMP.  Linux clang needs libomp-dev;
        the faiss recipe itself rejects apple-clang entirely so that case
        is handled separately in configure().
        """
        import os
        compiler = str(self.settings.compiler)
        if compiler in ("gcc", "msvc"):
            return True
        if compiler == "clang":
            # Probe common libomp-dev install locations on Linux
            paths = (
                "/usr/lib/llvm-18/lib/libomp.so",
                "/usr/lib/llvm-18/lib/libomp.a",
                "/usr/lib/llvm-19/lib/libomp.so",
                "/usr/lib/x86_64-linux-gnu/libomp.so",
                "/usr/lib/aarch64-linux-gnu/libomp.so",
            )
            for p in paths:
                if os.path.isfile(p):
                    return True
            return False
        return False

    def configure(self):
        # Allow explicit disable via env (e.g. Docker ARM64 openblas compat).
        import os
        if os.environ.get("YAMS_DISABLE_FAISS", "") != "1":
            # Enable faiss unless OpenMP is unavailable for the current compiler.
            # Apple Clang is always rejected by the faiss recipe regardless of
            # libomp availability; all other compilers need a working OpenMP.
            enable_faiss = True
            compiler = str(self.settings.compiler)
            if compiler == "apple-clang":  # type: ignore
                enable_faiss = False
                self.output.info(
                    "faiss recipe rejects Apple Clang; disabling faiss on macOS"
                )
            elif not self._has_openmp():
                enable_faiss = False
                self.output.info(
                    "OpenMP not found for compiler=" + compiler +
                    "; disabling faiss. Install libomp-dev (Linux) or brew libomp (macOS)."
                )
            self.options.enable_faiss = enable_faiss  # type: ignore
        else:
            self.options.enable_faiss = False  # type: ignore

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
        try:
            self.options["libarchive"].shared = False
            self.options[
                "libarchive"
            ].with_acl = False  # Avoid libacl link issues on Linux CI
        except Exception:
            pass
        try:
            self.options["taglib"].shared = False
        except Exception:
            pass
        self.options["spdlog"].header_only = False

        if self.settings.os in ["iOS", "Android"]:  # type: ignore
            # Mobile cross-builds do not benefit from a compiled fmt library and
            # currently hit simulator-specific toolchain failures in fmt's
            # source build path. Header-only fmt keeps the dependency surface
            # smaller and avoids that compiler path entirely.
            self.options["fmt"].header_only = True  # type: ignore
            # Mobile builds also do not need a separately compiled spdlog
            # library. Keeping it header-only avoids Apple SDK-specific
            # failures in spdlog's bundled/external fmt compile path.
            self.options["spdlog"].header_only = True  # type: ignore
            # The mobile corpus ABI only relies on Boost.Asio plus the linked
            # system/thread modules. Disable unrelated Boost libraries so
            # mobile cross-builds do not drag in platform-specific components
            # like boost.context assembly or locale/iconv stacks.
            for boost_opt in (
                "without_cobalt",
                "without_context",
                "without_contract",
                "without_coroutine",
                "without_fiber",
                "without_graph",
                "without_iostreams",
                "without_json",
                "without_locale",
                "without_log",
                "without_math",
                "without_nowide",
                "without_program_options",
                "without_random",
                "without_regex",
                "without_serialization",
                "without_stacktrace",
                "without_test",
                "without_type_erasure",
                "without_url",
                "without_wave",
            ):
                setattr(self.options["boost"], boost_opt, True)  # type: ignore

        if self.options.enable_onnx:  # type: ignore
            self.options["onnxruntime"].fPIC = True
            self.options["onnxruntime"].shared = True
            if self.settings.os != "Windows":  # type: ignore
                # oneTBB requires hwloc with shared=True on non-Windows builds.
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
