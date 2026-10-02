#!/usr/bin/env python3
"""List the ONNX Runtime shared libraries to bundle (used by Meson at configure time).

usage: list-onnxruntime-libs.py <dir> [<dir> ...]

Prints, for the first directory that contains an ONNX Runtime core library:
  dir <path>
  file <name>            real files to install
  link <name> <target>   symlinks to recreate (keeps libonnxruntime.so.1 -> .so.1.x.y)
Only runtime shared libraries are listed (core + providers_shared); static
archives, CMake and pkg-config files are left out. Prints nothing when no
directory has a runtime library.
"""

import os
import re
import sys

CORE = re.compile(r"^libonnxruntime(\.so(\.\d+)*|(\.\d+)*\.dylib)$")
EXTRA = re.compile(r"^libonnxruntime_providers_shared(\.so(\.\d+)*|\.dylib)$")


def main(dirs):
    for d in dirs:
        if not d or not os.path.isdir(d):
            continue
        names = sorted(os.listdir(d))
        if not any(CORE.match(n) for n in names):
            continue
        print(f"dir {d}")
        for n in names:
            if not (CORE.match(n) or EXTRA.match(n)):
                continue
            path = os.path.join(d, n)
            if os.path.islink(path):
                target = os.readlink(path)
                if os.sep in target:
                    target = os.path.basename(target)
                print(f"link {n} {target}")
            elif os.path.isfile(path):
                print(f"file {n}")
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
