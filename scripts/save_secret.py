"""Store a deployment credential via hidden stdin, never via argv or logs."""

import argparse
import getpass
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("path", type=Path)
args = parser.parse_args()
value = getpass.getpass("Credential (hidden): ")
if not value or "\n" in value:
    raise SystemExit("A single non-empty credential is required")
args.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
args.path.write_text(value)
args.path.chmod(0o600)
print(f"Stored protected credential in {args.path}")
