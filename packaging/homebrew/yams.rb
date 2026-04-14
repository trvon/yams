class Yams < Formula
  desc 'Yet Another Memory System - High-performance content-addressed storage'
  homepage 'https://github.com/trvon/yams'
  version '0.0.0' # placeholder; release workflow will update
  url "https://github.com/trvon/yams/archive/refs/tags/v#{version}.tar.gz"
  sha256 'SHA256_PLACEHOLDER' # replaced in release workflow
  license 'GPL-3.0-or-later'
  head 'https://github.com/trvon/yams.git', branch: 'main'

  depends_on 'cmake' => :build
  depends_on 'openssl@3'
  depends_on 'protobuf'
  depends_on 'sqlite'
  depends_on 'boost'
  depends_on 'onnxruntime'
  depends_on macos: :monterey

  on_linux do
    depends_on 'gcc@11' if ENV.compiler == :clang
  end

  livecheck do
    url :stable
    regex(/^v?(\d+(?:\.\d+)+)$/i)
  end

  def install
    ENV.cxx20

    # Set OpenSSL path for macOS
    ENV['OPENSSL_ROOT_DIR'] = Formula['openssl@3'].opt_prefix if OS.mac?

    system 'cmake', '-S', '.', '-B', 'build',
           '-DCMAKE_BUILD_TYPE=Release',
           '-DYAMS_BUILD_PROFILE=release',
           '-DYAMS_BUILD_DOCS=OFF',
           '-DYAMS_BUILD_TESTS=OFF',
           '-DYAMS_BUILD_MCP_SERVER=ON',
           "-DCMAKE_INSTALL_PREFIX=#{prefix}",
           *std_cmake_args

    system 'cmake', '--build', 'build', '--parallel'
    system 'cmake', '--install', 'build'

    # Install shell completions if the binary supports them
    return unless (bin / 'yams').exist?

    generate_completions_from_executable(bin / 'yams', 'completion')
  end

  def caveats
    <<~EOS
      To initialize YAMS storage:
        yams init --non-interactive

      For custom storage location:
        export YAMS_STORAGE="$HOME/.local/share/yams"
        yams init --non-interactive

      Homebrew installs completion files for bash, zsh, and fish.
      If completion is not active in your current shell yet, start a new shell or use:
        source <(yams completion bash)
        autoload -U compinit && compinit && source <(yams completion zsh)
        mkdir -p ~/.config/fish/completions && yams completion fish > ~/.config/fish/completions/yams.fish

      Zsh persistent setup:
        mkdir -p ~/.local/share/zsh/site-functions
        yams completion zsh > ~/.local/share/zsh/site-functions/_yams
        # Ensure ~/.local/share/zsh/site-functions is on fpath before compinit
        # then run: autoload -U compinit && compinit

      Nested subcommands are included, e.g.:
        yams config embeddings <TAB>
        yams plugin trust <TAB>
        yams plugins trust <TAB>
        yams daemon start --log-level <TAB>
        yams config search path-tree enable --mode <TAB>

      PowerShell completion is available manually:
        pwsh -NoLogo -NoProfile -Command 'Invoke-Expression (yams completion powershell | Out-String)'

      Documentation and examples:
        https://github.com/trvon/yams/tree/main/docs
    EOS
  end

  test do
    # Test that the binary was installed and can show version
    assert_match version.to_s, shell_output("#{bin}/yams --version")

    # Test basic functionality - init in a temp directory
    system "#{bin}/yams", 'init', '--non-interactive', '--storage', testpath / 'yams-test'
    assert_predicate testpath / 'yams-test/yams.db', :exist?
    assert_predicate testpath / '.config/yams/config.toml', :exist?
  end
end
