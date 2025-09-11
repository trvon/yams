#!/usr/bin/env python3
import base64
import os
import tempfile
from typing import Optional, Dict, Any

from yams_sdk.base import BasePlugin
from yams_sdk.decorators import rpc


class GhidraPlugin(BasePlugin):
    def __init__(self) -> None:
        super().__init__()
        self._project_dir: Optional[str] = None
        self._project_name: str = "YamsGhidra"
        self._ghidra_install: Optional[str] = os.environ.get("GHIDRA_INSTALL_DIR")
        self._started = False
        # Register RPCs discovered via decorator metadata
        for name in dir(self):
            fn = getattr(self, name)
            rpc_name = getattr(fn, "__rpc_name__", None)
            if rpc_name:
                self.register(rpc_name, fn)

    def manifest(self) -> dict:
        return {"name": "yams_ghidra", "version": "0.0.1", "interfaces": ["ghidra_analysis_v1"]}

    def init(self, config: Dict[str, Any]) -> None:  # noqa: A003
        # Lazy-import pyghidra on init to avoid hard dependency for non-users
        try:
            import pyghidra  # type: ignore
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"pyghidra not available: {e}")

        self._ghidra_install = config.get("ghidra_install", self._ghidra_install)
        self._project_dir = config.get("project_dir") or tempfile.mkdtemp(prefix="yams-ghidra-")
        self._project_name = config.get("project_name", self._project_name)

        if not self._started:
            # Start JVM and initialize Ghidra in headless mode
            if self._ghidra_install:
                pyghidra.start(install_dir=self._ghidra_install)
            else:
                pyghidra.start()
            self._started = True

        # Probe project creation/open (no persistent handle kept; opened per call)
        try:
            with pyghidra.open_project(self._project_dir, self._project_name, create=True):
                pass
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"Failed to open/create Ghidra project: {e}")

    def health(self) -> dict:
        return {"status": "ok", "started": self._started, "project_dir": self._project_dir}

    @staticmethod
    def _materialize_source(source: Dict[str, Any]) -> str:
        st = source.get("type")
        if st == "path":
            return source["path"]
        if st == "bytes":
            data_b64 = source.get("data")
            if not data_b64:
                raise ValueError("bytes source requires 'data' base64 field")
            data = base64.b64decode(data_b64)
            fd, p = tempfile.mkstemp(prefix="yams-ghidra-bin-")
            os.write(fd, data)
            os.close(fd)
            return p
        raise ValueError("unsupported source.type; expected 'path' or 'bytes'")

    def _open_program_ctx(self, project, path):
        import pyghidra  # type: ignore
        try:
            # Preferred convenience (may be deprecated in future)
            return pyghidra.open_program(path, analyze=True)  # type: ignore[attr-defined]
        except AttributeError:
            # Fallback: load via ProgramLoader then access via program_context
            loader = pyghidra.program_loader().project(project).source(path)
            with loader.load() as load_results:
                load_results.save(pyghidra.dummy_monitor())
            return pyghidra.program_context(project, "/" + os.path.basename(path))

    def _iter_functions(self, program, limit: int, offset: int = 0):
        listing = program.getListing()
        it = listing.getFunctions(True)
        skipped = 0
        collected = 0
        while it.hasNext():
            f = it.next()
            if skipped < offset:
                skipped += 1
                continue
            yield f
            collected += 1
            if collected >= limit:
                break

    @rpc("ghidra.analyze")
    def analyze(self, source: Dict[str, Any], opts: Optional[Dict[str, Any]] = None) -> dict:
        import pyghidra  # type: ignore
        path = self._materialize_source(source)
        max_functions = int((opts or {}).get("max_functions", 50))
        with pyghidra.open_project(self._project_dir, self._project_name, create=True) as project:
            with self._open_program_ctx(project, path) as ctx:
                # ctx may be FlatProgramAPI or Program; normalize
                try:
                    program = ctx.getCurrentProgram()
                except AttributeError:
                    program = ctx
                lang = program.getLanguage().getLanguageID().getIdAsString()
                funcs = []
                for f in self._iter_functions(program, limit=max_functions):
                    funcs.append({"name": f.getName(), "addr": f.getEntryPoint().toString()})
                return {"arch": lang, "count": len(funcs), "functions": funcs}

    @rpc("ghidra.list_functions")
    def list_functions(self, source: Dict[str, Any], opts: Optional[Dict[str, Any]] = None) -> dict:
        import pyghidra  # type: ignore
        path = self._materialize_source(source)
        limit = int((opts or {}).get("limit", 100))
        offset = int((opts or {}).get("offset", 0))
        with pyghidra.open_project(self._project_dir, self._project_name, create=True) as project:
            with self._open_program_ctx(project, path) as ctx:
                try:
                    program = ctx.getCurrentProgram()
                except AttributeError:
                    program = ctx
                items = []
                for f in self._iter_functions(program, limit=limit, offset=offset):
                    items.append({"name": f.getName(), "addr": f.getEntryPoint().toString()})
                # total is best-effort; iterate fully if requested
                total = offset + len(items)
                return {"items": items, "total": total}

    @rpc("ghidra.get_function")
    def get_function(self, source: Dict[str, Any], func: Dict[str, Any], include_body: bool = False) -> dict:
        import pyghidra  # type: ignore
        path = self._materialize_source(source)
        with pyghidra.open_project(self._project_dir, self._project_name, create=True) as project:
            with self._open_program_ctx(project, path) as ctx:
                try:
                    program = ctx.getCurrentProgram()
                except AttributeError:
                    program = ctx
                listing = program.getListing()
                addr_factory = program.getAddressFactory()
                target = None
                if "addr" in func:
                    a = addr_factory.getAddress(func["addr"])
                    target = listing.getFunctionAt(a)
                elif "name" in func:
                    it = listing.getFunctions(True)
                    while it.hasNext():
                        f = it.next()
                        if f.getName() == func["name"]:
                            target = f
                            break
                if target is None:
                    return {"ok": False, "error": "NotFound"}
                out = {"name": target.getName(), "addr": target.getEntryPoint().toString()}
                if include_body:
                    # Best-effort: dump code units around entry (placeholder)
                    out["body"] = f"Function {target.getName()} at {out['addr']}"
                return out

    @rpc("ghidra.decompile_function")
    def decompile_function(self, source: Dict[str, Any], func: Dict[str, Any], opts: Optional[Dict[str, Any]] = None) -> dict:
        import pyghidra  # type: ignore
        try:
            from ghidra.app.decompiler import DecompInterface  # type: ignore
        except Exception:
            return {"ok": False, "error": "DecompilerUnavailable"}
        path = self._materialize_source(source)
        with pyghidra.open_project(self._project_dir, self._project_name, create=True) as project:
            with self._open_program_ctx(project, path) as ctx:
                try:
                    program = ctx.getCurrentProgram()
                except AttributeError:
                    program = ctx
                listing = program.getListing()
                addr_factory = program.getAddressFactory()
                target = None
                if "addr" in func:
                    a = addr_factory.getAddress(func["addr"])
                    target = listing.getFunctionAt(a)
                elif "name" in func:
                    it = listing.getFunctions(True)
                    while it.hasNext():
                        f = it.next()
                        if f.getName() == func["name"]:
                            target = f
                            break
                if target is None:
                    return {"ok": False, "error": "NotFound"}
                di = DecompInterface()
                if not di.openProgram(program):
                    return {"ok": False, "error": "OpenProgramFailed"}
                res = di.decompileFunction(target, int((opts or {}).get("timeout_sec", 30)), pyghidra.dummy_monitor())
                if not res or not res.getDecompiledFunction():
                    return {"ok": False, "error": "DecompileFailed"}
                decomp = res.getDecompiledFunction().getC()
                return {"ok": True, "decomp": decomp, "meta": {"name": target.getName(), "addr": target.getEntryPoint().toString()}}

    @rpc("ghidra.search")
    def search(self, source: Dict[str, Any], query: Dict[str, Any], opts: Optional[Dict[str, Any]] = None) -> dict:
        """Simple search across functions by name or decompiled text substring.
        query: { text: "..." }
        opts: { max_hits?: int }
        """
        import pyghidra  # type: ignore
        text = (query or {}).get("text", "").strip()
        if not text:
            return {"hits": [], "total": 0}
        path = self._materialize_source(source)
        max_hits = int((opts or {}).get("max_hits", 25))
        hits = []
        total = 0
        try:
            from ghidra.app.decompiler import DecompInterface  # type: ignore
        except Exception:
            DecompInterface = None
        with pyghidra.open_project(self._project_dir, self._project_name, create=True) as project:
            with self._open_program_ctx(project, path) as ctx:
                try:
                    program = ctx.getCurrentProgram()
                except AttributeError:
                    program = ctx
                listing = program.getListing()
                it = listing.getFunctions(True)
                di = None
                if DecompInterface is not None:
                    di = DecompInterface()
                    di.openProgram(program)
                while it.hasNext():
                    f = it.next()
                    name = f.getName()
                    addr = f.getEntryPoint().toString()
                    found = text.lower() in name.lower()
                    snippet = ""
                    if not found and di is not None:
                        try:
                            res = di.decompileFunction(f, int((opts or {}).get("timeout_sec", 10)), pyghidra.dummy_monitor())
                            if res and res.getDecompiledFunction():
                                c = res.getDecompiledFunction().getC()
                                if text.lower() in c.lower():
                                    found = True
                                    # create a small snippet around first occurrence
                                    idx = c.lower().find(text.lower())
                                    start = max(0, idx - 40)
                                    end = min(len(c), idx + 40)
                                    snippet = c[start:end]
                        except Exception:
                            pass
                    if found:
                        total += 1
                        if len(hits) < max_hits:
                            hits.append({"title": name, "addr": addr, "snippet": snippet})
                return {"hits": hits, "total": total}

    @rpc("ghidra.grep")
    def grep(self, source: Dict[str, Any], pattern: Dict[str, Any], opts: Optional[Dict[str, Any]] = None) -> dict:
        """Regex grep over decompiled function text.
        pattern: { regex: "...", flags?: "i" }
        opts: { max_hits?: int }
        """
        import pyghidra  # type: ignore
        import re
        regex = (pattern or {}).get("regex", "")
        if not regex:
            return {"matches": [], "total": 0}
        flags = 0
        if (pattern or {}).get("flags") == "i":
            flags |= re.IGNORECASE
        rx = re.compile(regex, flags)
        path = self._materialize_source(source)
        max_hits = int((opts or {}).get("max_hits", 25))
        matches = []
        total = 0
        try:
            from ghidra.app.decompiler import DecompInterface  # type: ignore
        except Exception:
            return {"matches": [], "total": 0, "error": "DecompilerUnavailable"}
        with pyghidra.open_project(self._project_dir, self._project_name, create=True) as project:
            with self._open_program_ctx(project, path) as ctx:
                try:
                    program = ctx.getCurrentProgram()
                except AttributeError:
                    program = ctx
                listing = program.getListing()
                it = listing.getFunctions(True)
                di = DecompInterface()
                if not di.openProgram(program):
                    return {"matches": [], "total": 0, "error": "OpenProgramFailed"}
                while it.hasNext():
                    f = it.next()
                    try:
                        res = di.decompileFunction(f, int((opts or {}).get("timeout_sec", 10)), pyghidra.dummy_monitor())
                        if not res or not res.getDecompiledFunction():
                            continue
                        c = res.getDecompiledFunction().getC()
                        for m in rx.finditer(c):
                            total += 1
                            if len(matches) < max_hits:
                                start = max(0, m.start() - 40)
                                end = min(len(c), m.end() + 40)
                                matches.append({"path": f.getName(), "addr": f.getEntryPoint().toString(), "snippet": c[start:end]})
                    except Exception:
                        continue
                return {"matches": matches, "total": total}


if __name__ == "__main__":
    GhidraPlugin().run()
