#!/usr/bin/env python3
"""Package the two committed Medic atlases at their exact Cloudflare bucket keys."""
import argparse
import hashlib
import json
from pathlib import Path
import zipfile

ROOT = Path(__file__).resolve().parents[3]
KEYS = ("units/medic-healing.webp", "effects/medic-healing.webp")

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    index = json.loads((ROOT / "game/turncraft/assets/visual-asset-index.json").read_text())
    payloads = {}
    for key in KEYS:
        data = (ROOT / key).read_bytes()
        record = index["images"][key]
        if len(data) != record["bytes"] or hashlib.sha256(data).hexdigest() != record["sha256"]:
            raise SystemExit(f"Artwork differs from its manifest: {key}")
        payloads[key] = data
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(args.out, "w", zipfile.ZIP_DEFLATED) as archive:
        for key, data in payloads.items():
            archive.writestr(key, data)
    with zipfile.ZipFile(args.out) as archive:
        if archive.testzip() or set(archive.namelist()) != set(KEYS):
            raise SystemExit("Medic artwork archive validation failed")
    print(json.dumps({"file": str(args.out.resolve()), "keys": list(KEYS), "bytes": args.out.stat().st_size}))

if __name__ == "__main__":
    main()
