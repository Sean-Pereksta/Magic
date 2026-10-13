#!/usr/bin/env python3
"""Verify decoded atlas dimensions, all live IDs and all referenced frame bounds."""
import hashlib
import json
import re
import argparse
from pathlib import Path
from PIL import Image
from visual_assets import INDEX, asset_path, download_assets

ROOT = Path(__file__).resolve().parents[3]
GAME = ROOT / "game" / "turncraft"
manifest = json.loads((GAME / "assets/animation-manifest.json").read_text())
roster = json.loads((GAME / "assets/unit-artwork-roster.json").read_text())
source = (ROOT / "game/turncraft.html").read_text()
base = source.split("const STRUCTURES={", 1)[1].split("\n};", 1)[0]
structures = set(re.findall(r"^\s*(\w+):\{", base, re.M)) | {"tunnelExit"}
assets = manifest["assets"]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--allow-missing", nargs="*", default=[])
args = parser.parse_args()
download_assets()
required_units = set(roster)-set(args.allow_missing)
assert required_units.issubset(assets), f"Missing units: {sorted(required_units-set(assets))}"
assert structures.issubset(assets), f"Missing structures: {sorted(structures-set(assets))}"
assert {"larva","cocoon"}.issubset(assets), "Missing larva/cocoon developmental assets"
images = {}
frame_count = 0
for uid, entry in assets.items():
    states = entry["states"]
    expected = {"idle", "move", "attack", "hit", "death", "ability"} if uid in roster else {"idle", "production", "construction", "death"}
    if uid in {"larva", "cocoon"}:
        expected = {"idle", "hit", "death"} | ({"morph"} if uid == "cocoon" else set())
    assert expected.issubset(states), f"Missing animation state {uid}: {expected-set(states)}"
    for state_name, state in states.items():
        path = asset_path(state["sheet"])
        if path not in images:
            image = Image.open(path).convert("RGBA")
            assert max(image.size) <= 2048, (path, image.size)
            assert image.getchannel("A").getextrema()[0] < 255, f"Opaque atlas {path}"
            images[path] = image
        image = images[path]
        directions = state["directions"]
        if uid in roster:
            assert directions == manifest["directions"], f"Incomplete directions: {uid}.{state_name}"
        for direction in directions:
            hashes = set()
            for index in range(state["frames"]):
                coords = state.get("coordinates", {}).get(direction)
                if coords:
                    rect = coords[index]
                    x,y,w,h = (rect[k] for k in ("x", "y", "w", "h"))
                else:
                    w,h = state["frameWidth"], state["frameHeight"]
                    x = (state.get("firstColumn", 0)+index)*w
                    y = state["rows"][direction]*h
                assert 0 <= x < image.width and 0 <= y < image.height and x+w <= image.width and y+h <= image.height, (uid,state_name,direction,index)
                frame = image.crop((x,y,x+w,y+h))
                assert frame.getchannel("A").getbbox(), f"Empty frame: {uid}.{state_name}.{direction}.{index}"
                hashes.add(hashlib.sha256(frame.tobytes()).digest())
                frame_count += 1
            assert len(hashes) > 1 or state["frames"] == 1, f"Repeated static frames: {uid}.{state_name}.{direction}"
for key, record in INDEX["images"].items():
    path = asset_path(INDEX["baseUrl"] + key)
    with Image.open(path) as image:
        image.load()
        assert image.size == (record["width"], record["height"]), key
print(json.dumps({"unitTypes":len(roster), "structureTypes":len(structures), "assets":len(assets), "atlases":len(images), "productionImages":len(INDEX["images"]),
                  "referencedFrames":frame_count, "artworkBytes":sum(p.stat().st_size for p in images),
                  "missingUnits":sorted(set(roster)-set(assets))}, indent=2))
