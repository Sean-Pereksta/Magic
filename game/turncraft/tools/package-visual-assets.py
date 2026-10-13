#!/usr/bin/env python3
"""Package Cloudflare graphics in four top-level folders, separately from code."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[3]
GAME = ROOT / "game" / "turncraft"
sys.path.insert(0, str(GAME / "scripts"))
from visual_assets import BASE_URL, INDEX, asset_path, download_assets


def write_archive(destination: Path, files: list[tuple[str, Path]]) -> dict:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for name, file in files:
            entry = zipfile.ZipInfo(name, (2026, 1, 1, 0, 0, 0))
            entry.compress_type = zipfile.ZIP_DEFLATED
            entry.external_attr = 0o100644 << 16
            archive.writestr(entry, file.read_bytes())
    with zipfile.ZipFile(destination) as archive:
        bad = archive.testzip()
        if bad:
            raise SystemExit(f"Archive validation failed: {bad}")
    return {"file": str(destination), "entries": len(files), "bytes": destination.stat().st_size,
            "sha256": hashlib.sha256(destination.read_bytes()).hexdigest()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--assets", type=Path, required=True, help="Graphics-only ZIP destination")
    parser.add_argument("--code", type=Path, help="Optional source-only ZIP destination")
    args = parser.parse_args()
    download_assets()
    graphics = [(key, asset_path(BASE_URL + key)) for key in sorted(INDEX["images"])]
    result = [write_archive(args.assets.resolve(), graphics)]
    if args.code:
        code = {ROOT / "game/turncraft.html", ROOT / "scripts/build-structure-atlases.py",
                ROOT / ".github/workflows/turncraft.yml", ROOT / "graphics/turncraft/.gitignore"}
        suffixes = {".js", ".cjs", ".json", ".css", ".py", ".md"}
        code.update(file for file in GAME.rglob("*") if file.is_file() and file.suffix in suffixes
                    and not {"node_modules", "__pycache__"}.intersection(file.relative_to(GAME).parts))
        code.update((ROOT / "tests").glob("turncraft-*"))
        files = [(file.relative_to(ROOT).as_posix(), file) for file in sorted(code) if file.is_file()]
        result.append(write_archive(args.code.resolve(), files))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
