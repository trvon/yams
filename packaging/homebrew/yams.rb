class Yams < Formula
  desc "Yet Another Memory System - High-performance content-addressed storage"
  homepage "https://github.com/trvon/yams"
  version "0.0.0" # placeholder; release workflow will update
  url "https://github.com/trvon/yams/archive/refs/tags/v#{version}.tar.gz"
  sha256 "SHA256_PLACEHOLDER" # replaced in release workflow
  license "MIT"
  head "https://github.com/trvon/yams.git", branch: "main"

  depends_on "cmake" => :build
  depends_on "openssl@3"
  depends_on "protobuf"
  depends_on "sqlite"
  depends_on "boost"
  depends_on macos: :monterey

  on_linux do
    depends_on "gcc@11" if ENV.compiler == :clang
  end

  livecheck do
    url :stable
    regex(/^v?(\d+(?:\.\d+)+)$/i)
  end

  def install
    ENV.cxx20

    # Set OpenSSL path for macOS
    if OS.mac?
      ENV["OPENSSL_ROOT_DIR"] = Formula["openssl@3"].opt_prefix
    end

    system "cmake", "-S", ".", "-B", "build",
                    "-DCMAKE_BUILD_TYPE=Release",
                    "-DYAMS_BUILD_PROFILE=release",
                    "-DYAMS_BUILD_DOCS=OFF",
                    "-DYAMS_BUILD_TESTS=OFF",
                    "-DYAMS_BUILD_MCP_SERVER=ON",
                    "-DCMAKE_INSTALL_PREFIX=#{prefix}",
                    *std_cmake_args

    system "cmake", "--build", "build", "--parallel"
    system "cmake", "--install", "build"

    # Install shell completions if the binary supports them
    if (bin/"yams").exist?
      generate_completions_from_executable(bin/"yams", "completion")
    end
  end

  def caveats
    <<~EOS
      To initialize YAMS storage:
        yams init --non-interactive

      For custom storage location:
        export YAMS_STORAGE="$HOME/.local/share/yams"
        yams init --non-interactive

      Documentation and examples:
        https://github.com/trvon/yams/tree/main/docs
    EOS
  end

  test do
    # Test that the binary was installed and can show version
    assert_match version.to_s, shell_output("#{bin}/yams --version")
    
    # Test basic functionality - init in a temp directory
    system "#{bin}/yams", "init", "--non-interactive", "--storage", testpath/"yams-test"
    assert_predicate testpath/"yams-test/yams.db", :exist?
    assert_predicate testpath/".config/yams/config.toml", :exist?
  end
end