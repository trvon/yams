# PBI: Shell Completion Support for YAMS CLI

## Overview

**Feature:** Implement shell completion (tab completion) support for the YAMS CLI tool

**Priority:** Medium  
**Effort:** 2-3 days  
**Status:** Not Started

## Problem Statement

Currently, YAMS users must manually type all commands and options without shell completion assistance. This reduces productivity and increases typing errors. The install.sh script and Homebrew formula already expect completion support but YAMS doesn't provide it.

**Current Pain Points:**
- No tab completion for subcommands (`yams <TAB>` shows nothing)
- No completion for command options (`yams list --<TAB>` shows nothing)
- No file path completion for relevant options
- Install script silently fails to set up completions
- Homebrew formula expects `yams completion` command that doesn't exist

## Success Criteria

**Must Have:**
- [ ] `yams completion bash` outputs bash completion script
- [ ] `yams completion zsh` outputs zsh completion script  
- [ ] `yams completion fish` outputs fish completion script
- [ ] Tab completion works for all subcommands (add, get, list, search, etc.)
- [ ] Tab completion works for common options (--help, --verbose, --json, etc.)
- [ ] install.sh automatically sets up completions after installation
- [ ] Homebrew formula works with `generate_completions_from_executable`

**Should Have:**
- [ ] File path completion for file-based options (--output, etc.)
- [ ] Contextual completion (e.g., hash completion for `yams get <TAB>`)
- [ ] Help text shows completion command in usage

**Could Have:**
- [ ] Dynamic completion based on YAMS data (tag completion, etc.)
- [ ] Completion for custom commands/aliases

## Technical Implementation

### Architecture

**CLI11 Integration:**
- YAMS uses CLI11 v2.4.1 which has built-in completion support
- CLI11 provides `generate_completions()` method for bash/zsh/fish
- Need to expose this via a new "completion" subcommand

**Implementation Plan:**

1. **Create CompletionCommand class** (`src/cli/commands/completion_command.cpp`)
   ```cpp
   class CompletionCommand : public ICommand {
   public:
       std::string getName() const override { return "completion"; }
       std::string getDescription() const override { return "Generate shell completion scripts"; }
       void registerCommand(CLI::App& app, YamsCLI& cli) override;
       Result<void> execute() override;
   private:
       std::string shell_; // bash, zsh, fish
   };
   ```

2. **Enable CLI11 completions** in `YamsCLI` constructor:
   ```cpp
   // Enable shell completions in CLI11 app
   app_->set_help_all_flag("--help-all", "Show extended help");
   // CLI11 will automatically handle completion generation
   ```

3. **Register completion command** in CommandRegistry

4. **Update command list** in yams_cli.cpp to include "completion"

### Expected Behavior

**Command Structure:**
```bash
yams completion bash   # Output bash completion script
yams completion zsh    # Output zsh completion script  
yams completion fish   # Output fish completion script
yams completion --help # Show completion help
```

**Integration Points:**
- install.sh calls `yams completion bash` and installs to `~/.local/share/bash-completion/completions/yams`
- Homebrew calls `generate_completions_from_executable(bin/"yams", "completion")`
- Users can manually source: `source <(yams completion bash)`

### File Locations

**New Files:**
- `src/cli/commands/completion_command.cpp`
- `src/cli/commands/completion_command.h` 
- `tests/unit/cli/completion_command_test.cpp`

**Modified Files:**
- `src/cli/command_registry.cpp` (register command)
- `src/cli/yams_cli.cpp` (add to command list)
- `src/cli/CMakeLists.txt` (add source files)

## Dependencies

**Technical Dependencies:**
- CLI11 v2.4.1+ (already available)
- No additional external dependencies

**Workflow Dependencies:**
- Should be implemented after current CI/Release workflow improvements
- Can be developed independently of other features

## Testing Strategy

**Unit Tests:**
- Test completion command registration
- Test output format for each shell type
- Test error handling (invalid shell type)

**Integration Tests:**
- Test actual bash completion functionality
- Test install.sh completion setup
- Test that completions work in real shells

**Manual Verification:**
```bash
# Install completion
yams completion bash > ~/.local/share/bash-completion/completions/yams
source ~/.local/share/bash-completion/completions/yams

# Test completion
yams <TAB>          # Should show: add get list search delete ...
yams list --<TAB>   # Should show: --help --verbose --json --format ...
```

## Risks and Considerations

**Low Risk:**
- CLI11 handles the complex completion logic
- Straightforward command implementation
- Non-breaking addition

**Considerations:**
- Shell-specific completion syntax differences (handled by CLI11)
- Installation path variations across systems (handled by install.sh)
- Completion performance with large datasets (acceptable for CLI tool)

## Success Metrics

- [ ] install.sh successfully installs completions without errors
- [ ] Users report improved CLI productivity
- [ ] All major shells (bash, zsh, fish) working
- [ ] Zero regression in existing CLI functionality

## Implementation Notes

**CLI11 Features to Leverage:**
- `generate_completions()` method
- Automatic subcommand discovery
- Option and flag completion
- Built-in help integration

**Installation Integration:**
- install.sh already has completion setup logic
- Homebrew formula already expects completion support
- Just need to implement the missing command

This PBI provides comprehensive shell completion support to improve YAMS CLI user experience and fulfill existing infrastructure expectations.