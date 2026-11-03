#!/usr/bin/env python3
"""Exercise generated shell completions, including real zsh editor completion."""

import argparse
import os
import select
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

CLI_BINARY = None


class CompletionRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if CLI_BINARY is None:
            raise unittest.SkipTest(
                "requires --cli pointing to the built YAMS executable"
            )
        cls.temporary = tempfile.TemporaryDirectory(prefix="yams-completion-runtime-")
        cls.root = Path(cls.temporary.name)
        cls.environment = os.environ.copy()
        cls.environment.update(
            {
                "YAMS_CONFIG": str(cls.root / "config.toml"),
                "YAMS_DATA_DIR": str(cls.root / "data"),
                "YAMS_STORAGE": str(cls.root / "data"),
                "YAMS_NON_INTERACTIVE": "1",
                "YAMS_CLI_DISABLE_DAEMON_AUTOSTART": "1",
            }
        )
        cls.scripts = {}
        for shell in ("bash", "zsh"):
            path = cls.root / f"yams.{shell}"
            result = subprocess.run(
                [CLI_BINARY, "completion", shell],
                env=cls.environment,
                capture_output=True,
                text=True,
                check=True,
                timeout=15,
            )
            path.write_text(result.stdout, encoding="utf-8")
            cls.scripts[shell] = path

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def test_generated_scripts_parse(self):
        for shell, script in self.scripts.items():
            if shutil.which(shell):
                with self.subTest(shell=shell):
                    subprocess.run(
                        [shell, "-n", str(script)],
                        check=True,
                        capture_output=True,
                        timeout=5,
                    )

    def test_bash_nested_commands_group_options_and_global_values(self):
        if not shutil.which("bash"):
            self.skipTest("bash is not installed")
        for words, expected in (
            (["yams", "plugin", "tr"], "trust"),
            (["yams", "get", "--ha"], "--hash"),
            (["yams", "--data-dir", "/corpora/with spaces", "plugin", "tr"], "trust"),
        ):
            with self.subTest(words=words):
                command = (
                    f"source {shlex.quote(str(self.scripts['bash']))}; "
                    f"COMP_WORDS=({shlex.join(words)}); COMP_CWORD={len(words) - 1}; "
                    "_yams_completion; printf '%s\\n' \"${COMPREPLY[@]}\""
                )
                result = subprocess.run(
                    ["bash", "-c", command],
                    env=self.environment,
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                self.assertIn(expected, result.stdout.splitlines())

    @unittest.skipUnless(
        os.name == "posix" and shutil.which("zsh"), "requires POSIX zsh"
    )
    def test_zsh_completes_in_the_real_line_editor(self):
        import pty

        master, slave = pty.openpty()
        read_fd, write_fd = os.pipe()
        environment = self.environment | {"TERM": "dumb", "ZDOTDIR": str(self.root)}
        process = subprocess.Popen(
            ["zsh", "-f"],
            stdin=slave,
            stdout=slave,
            stderr=slave,
            env=environment,
            pass_fds=(write_fd,),
            start_new_session=True,
        )
        os.close(slave)
        os.close(write_fd)
        buffered = bytearray()

        def read_line():
            deadline = time.monotonic() + 10
            while b"\n" not in buffered:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.fail(
                        "zsh completion widget did not signal within its deadline"
                    )
                # Drain terminal output as well, so terminal backpressure cannot stall the widget.
                ready, _, _ = select.select([read_fd, master], [], [], remaining)
                if master in ready:
                    os.read(master, 65536)
                if read_fd in ready:
                    data = os.read(read_fd, 65536)
                    if not data:
                        self.fail("zsh exited before signaling completion")
                    buffered.extend(data)
            line, _, rest = buffered.partition(b"\n")
            buffered[:] = rest
            return line.decode("utf-8")

        try:
            setup = (
                "PS1=''; autoload -Uz compinit; "
                f"compinit -d {shlex.quote(str(self.root / 'zcompdump'))}; "
                f"source {shlex.quote(str(self.scripts['zsh']))}; "
                "_test_yams_complete() { zle complete-word; "
                f'print -r -- "$BUFFER" >&{write_fd}; }}; '
                "zle -N _test_yams_complete; bindkey '^I' _test_yams_complete; "
                f"print READY >&{write_fd}\n"
            )
            os.write(master, setup.encode())
            self.assertEqual(read_line(), "READY")
            for command, expected in (
                ("yams plugin tr", "yams plugin trust"),
                ("yams get --ha", "yams get --hash"),
                (
                    "yams --data-dir /corpora/example plugin tr",
                    "yams --data-dir /corpora/example plugin trust",
                ),
                ("yams config em", "yams config embeddings"),
            ):
                with self.subTest(command=command):
                    os.write(master, ("\x15" + command + "\t").encode())
                    self.assertEqual(read_line().rstrip(), expected)
        finally:
            os.close(master)
            os.close(read_fd)
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", required=True)
    options, remaining = parser.parse_known_args()
    CLI_BINARY = str(Path(options.cli).resolve())
    unittest.main(argv=[sys.argv[0], *remaining])
