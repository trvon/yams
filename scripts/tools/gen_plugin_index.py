#!/usr/bin/env python3
"""Generate a simple plugins index JSON from docs/ecosystem/registry.yaml.

Writes docs/plugins/index.json usable by a static site (mkdocs) to expose
an installable catalog. Maintained vs community sections are flattened with
"origin" = maintained|community.
"""
import json, sys, os
try:
    import yaml
except Exception:
    print("PyYAML not installed; 'pip install pyyaml' to use this tool", file=sys.stderr)
    sys.exit(2)

def main() -> int:
    repo = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    reg_path = os.path.join(repo, 'docs', 'ecosystem', 'registry.yaml')
    out_dir = os.path.join(repo, 'docs', 'plugins')
    os.makedirs(out_dir, exist_ok=True)
    if not os.path.exists(reg_path):
        print('registry not found:', reg_path, file=sys.stderr)
        return 2
    reg = yaml.safe_load(open(reg_path)) or {}
    items = []
    def add(origin, lst):
        for it in lst or []:
            it = dict(it)
            it['origin'] = origin
            items.append(it)
    add('maintained', reg.get('maintained_plugins'))
    add('community', reg.get('community_plugins'))
    out = {'plugins': items}
    out_path = os.path.join(out_dir, 'index.json')
    json.dump(out, open(out_path,'w'), indent=2)
    print('Wrote', out_path)
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
