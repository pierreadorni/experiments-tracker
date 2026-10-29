"""Refresh the separate Site checkout from tracked source, preserving its identity and Git metadata."""

import json
import shutil
from pathlib import Path

root = Path(__file__).resolve().parents[1]
site = root / ".local/phone-site"
if not (site / ".openai/hosting.json").exists():
    raise SystemExit("Initialize/register the Site checkout first; see the runbook")
manifest = json.loads((site / ".openai/hosting.json").read_text())
if not manifest.get("project_id"):
    raise SystemExit("Registered project_id is required; do not create a replacement Site")
manifest.update({"d1": "DB", "r2": "BUCKET"})
(site / ".openai/hosting.json").write_text(json.dumps(manifest, indent=2) + "\n")
gateway = root / "gateway"
for name in ("src", "db", "drizzle", "test", "scripts"):
    shutil.copytree(gateway / name, site / name, dirs_exist_ok=True)
for name in ("package.json", "package-lock.json", "tsconfig.json", "drizzle.config.ts", ".gitignore"):
    shutil.copyfile(gateway / name, site / name)
for name in ("src", "public", "test", "scripts"):
    shutil.copytree(root / "web" / name, site / "web" / name, dirs_exist_ok=True)
for name in ("package.json", "package-lock.json", "tsconfig.json", "vite.config.ts", "index.html"):
    shutil.copyfile(root / "web" / name, site / "web" / name)
print(f"Refreshed {site}; project identity preserved")
