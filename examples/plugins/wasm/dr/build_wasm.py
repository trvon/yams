#!/usr/bin/env python3

import os, subprocess, sys, textwrap, shutil

WAT = r"""
(module
  (memory (export "memory") 1)
  (global $bump (mut i32) (i32.const 4096))
  (func (export "alloc") (param $n i32) (result i32)
    (local $old i32)
    global.get $bump
    local.set $old
    global.get $bump
    local.get $n
    i32.add
    global.set $bump
    local.get $old)
  ;; Return static JSON: {"ok":true,"reason":"demo"}
  (data (i32.const 8192) "{\22ok\22:true,\22reason\22:\22demo\22}")
  (func (export "dr_provider_v1_is_replication_ready")
        (param $k i32) (param $kl i32) (param $o i32) (param $ol i32) (result i64)
    ;; Ignore inputs; return ptr/len of static JSON at 8192
    (i64.or (i64.shl (i64.const 8192) (i64.const 32)) (i64.const 30)))
)
"""

def main():
    out_wat = "plugin.wat"
    out_wasm = "plugin.wasm"
    with open(out_wat, "w") as f:
        f.write(WAT)
    wat2wasm = shutil.which("wat2wasm")
    if not wat2wasm:
        print("wat2wasm not found in PATH. Install wabt or provide a wasm module manually.")
        print(f"Wrote {out_wat}. You can run: wat2wasm {out_wat} -o plugin.wasm")
        return 0
    subprocess.check_call([wat2wasm, out_wat, "-o", out_wasm])
    with open("plugin.wasm.manifest.json", "w") as f:
        f.write(textwrap.dedent('''
        {"name":"dr_wasm","version":"0.1.0","interfaces":["dr_provider_v1"]}
        '''))
    print("Built plugin.wasm and plugin.wasm.manifest.json")
    return 0

if __name__ == "__main__":
    sys.exit(main())

