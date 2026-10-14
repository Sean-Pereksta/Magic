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
repaired_frames = 0
for url, repair in manifest.get("cropRepairs", {}).items():
    image = images[asset_path(url)]
    assert hashlib.sha256(asset_path(url).read_bytes()).hexdigest() == repair["sourceSha256"], url
    assert len(repair["crops"]) == 8 and repair["sourceFrameWidth"] in {96, 128}, url
    assert repair["height"] == 8 * repair["frameHeight"] and max(repair["width"], repair["height"]) <= 2048, url
    for row, crops in enumerate(repair["crops"]):
        assert len(crops) * repair["frameWidth"] == repair["width"], url
        for col, (x, y, w, h) in enumerate(crops):
            assert 0 <= x < x+w <= image.width and 0 <= y < y+h <= image.height, (url, row, col)
            assert w <= repair["frameWidth"]-8, (url, row, col, "missing gutter")
            offsets = repair.get("destinationOffsets")
            if offsets is not None:
                assert len(offsets) == len(repair["crops"]) and len(offsets[row]) == len(crops), (url, "incomplete placements")
                dx, dy = offsets[row][col]
                assert 4 <= dx and dx+w <= repair["frameWidth"]-4, (url, row, col, "horizontal gutter")
                assert 4 <= dy and dy+h <= repair["frameHeight"]-4, (url, row, col, "vertical gutter")
                assert repair["sourceFrameHeight"] == 144, url
            else:
                assert row*144 <= y < y+h <= (row+1)*144, (url, row, col)
            assert image.crop((x,y,x+w,y+h)).getchannel("A").getbbox(), (url, row, col, "empty crop")
            if url.endswith("-demise.webp") and col < 5:
                limit = repair.get("deathRegionEnds", [repair.get("deathRegionEnd", 5*repair["sourceFrameWidth"]+4)]*8)[row]
                assert x+w <= limit, (url, row, col, "death frame selected a living ability pose")
            for ex, ey, ew, eh in repair.get("erasures", {}).get(f"{row},{col}", []):
                assert 0 <= ex < ex+ew <= w and 0 <= ey < ey+eh <= h, (url, row, col, "scrap outside crop")
            repaired_frames += 1
print(json.dumps({"unitTypes":len(roster), "structureTypes":len(structures), "assets":len(assets), "atlases":len(images), "productionImages":len(INDEX["images"]),
                  "referencedFrames":frame_count, "artworkBytes":sum(p.stat().st_size for p in images),
                  "repairedSheets":len(manifest.get("cropRepairs", {})), "repairedFrames":repaired_frames,
                  "missingUnits":sorted(set(roster)-set(assets))}, indent=2))
