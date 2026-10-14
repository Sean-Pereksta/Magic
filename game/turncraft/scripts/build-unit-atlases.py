#!/usr/bin/env python3
"""Pack actual generated animation grids; never synthesizes animation poses.

Generation record paths point to transient original imagegen outputs. Supply
--records with archived records/source paths to rebuild. Pixel operations are
restricted to regular-grid crops, one uniform resize and atlas placement.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import hashlib
from pathlib import Path
from PIL import Image
import numpy as np
from scipy import ndimage

ROOT = Path(__file__).resolve().parents[3]
GAME = ROOT / "game" / "turncraft"
DIRECTIONS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
FRAME_W, FRAME_H = 96, 144
PACKER_VERSION = "painted-grid-v3-quality88-clean-border"
GROUPS = [("locomotion", 0, 10), ("combat", 10, 7), ("demise", 17, 7)]
STATES = {
    "idle": ("locomotion", 0, 4, 5, True),
    "move": ("locomotion", 4, 6, 10, True),
    "attack": ("combat", 0, 5, 10, False),
    "hit": ("combat", 5, 2, 12, False),
    "death": ("demise", 0, 5, 8, False),
    "ability": ("demise", 5, 2, 6, True),
}

def clean_border_scraps(frame: Image.Image) -> Image.Image:
    """Remove tiny disconnected atlas-neighbor scraps; keep substantive effects."""
    pixels = np.array(frame)
    mask = pixels[:, :, 3] > 16
    labels, count = ndimage.label(mask)
    areas = np.bincount(labels.ravel())
    for label, bounds in enumerate(ndimage.find_objects(labels), 1):
        if bounds is None:
            continue
        y, x = bounds
        border = y.start <= 1 or x.start <= 1 or y.stop >= frame.height-1 or x.stop >= frame.width-1
        thin_fragment = areas[label] / max(1, (y.stop-y.start)*(x.stop-x.start)) < .35
        if border and (areas[label] < max(128, int(mask.sum() * .04)) or (thin_fragment and areas[label] < 300)):
            pixels[labels == label] = 0
    pixels[pixels[:, :, 3] == 0, :3] = 0
    return Image.fromarray(pixels, "RGBA")

def adaptive_row_boundaries(image: Image.Image, row: int, rows: int, columns: int) -> list[int]:
    """Read actual inter-sprite gaps when generated wide subjects have uneven spacing."""
    y0,y1 = round(row*image.height/rows),round((row+1)*image.height/rows)
    alpha = np.asarray(image)[y0:y1,:,3]
    projection = (alpha > 160).sum(axis=0).astype(float)
    projection = np.convolve(projection, np.ones(3)/3, mode="same")
    width, ideal = image.width, image.width/columns
    min_width,max_width = int(ideal*.58),int(ideal*1.6)
    previous = np.full(width+1,np.inf)
    previous[0] = 0
    back = []
    positions = np.arange(width+1)
    cut_cost = np.r_[projection,0]*.8
    cut_cost[0] = 0
    for index in range(1,columns+1):
        best = np.full(width+1,np.inf)
        widths = np.zeros(width+1,dtype=np.int16)
        for span in range(min_width,max_width+1):
            values = previous[:-span] + 3*((span-ideal)/ideal)**2
            target = best[span:]
            better = values < target
            target[better] = values[better]
            widths[span:][better] = span
        best += cut_cost + .6*((positions-index*ideal)/ideal)**2
        valid = (positions >= max(index*min_width,width-(columns-index)*max_width)) & (positions <= min(index*max_width,width-(columns-index)*min_width))
        valid &= np.abs(positions-index*ideal) <= ideal*1.5
        best[~valid] = np.inf
        previous = best
        back.append(widths)
    assert np.isfinite(previous[width]), "Could not extract actual sprite spacing"
    boundaries = [width]
    cursor = width
    for widths in reversed(back):
        cursor -= int(widths[cursor])
        boundaries.append(cursor)
    return list(reversed(boundaries))

def load_records(paths: list[Path]) -> list[dict]:
    records = []
    for path in paths:
        data = json.loads(path.read_text())
        records.extend(data if isinstance(data, list) else data.get("records", []))
    return records

def record_hash(record: dict) -> str:
    digest = hashlib.sha256(Path(record["source"]).read_bytes())
    for group, part in sorted(record.get("stateSources", {}).items()):
        digest.update(group.encode())
        digest.update(Path(part["source"]).read_bytes())
    return digest.hexdigest()

def fit_frame(crop: Image.Image, width: int, height: int, ground: bool = True) -> Image.Image:
    """Fit with a single scale factor, preserving anatomy and machinery proportions."""
    factor = min(width/crop.width,height/crop.height)
    crop = clean_border_scraps(crop).resize((round(crop.width*factor),round(crop.height*factor)),Image.Resampling.LANCZOS)
    frame = Image.new("RGBA",(width,height))
    frame.alpha_composite(crop,((width-crop.width)//2,height-crop.height if ground else (height-crop.height)//2))
    return frame

def pack_record(record: dict, roster: dict) -> tuple[str, dict]:
    uid = record["id"]
    unit = roster[uid]
    image = Image.open(record["source"]).convert("RGBA")
    cols, rows = record.get("layout", {}).get("columns", 24), record.get("layout", {}).get("rows", 8)
    if (cols, rows) != (24, 8):
        raise ValueError(f"Unexpected core layout for {uid}: {cols}x{rows}")
    if not image.getchannel("A").getextrema()[0] < 255:
        raise ValueError(f"Source atlas has no transparency: {uid}")
    output_dir = ROOT / "graphics" / "turncraft" / "animated" / "units"
    output_dir.mkdir(parents=True, exist_ok=True)
    sheet_paths = {}
    frame_checks = []
    wide = uid in {"latticeSkiff","voidLancer"}
    frame_w,frame_h = (128,144) if wide else (FRAME_W,FRAME_H)
    boundaries = [adaptive_row_boundaries(image,row,rows,cols) for row in range(rows)] if wide else None
    widest_source = max(b-a for row in boundaries for a,b in zip(row,row[1:])) if wide else 0
    source_row_height = round(image.height/rows)
    uniform_scale = min(frame_w/widest_source,frame_h/source_row_height) if wide else 1
    for name, first, count in GROUPS:
        part = record.get("stateSources",{}).get(name)
        group_image = Image.open(part["source"]).convert("RGBA") if part else image
        group_cols = part["columns"] if part else cols
        group_rows = part["rows"] if part else rows
        group_first = 0 if part else first
        row_bounds = adaptive_row_boundaries(group_image.transpose(Image.Transpose.TRANSPOSE),0,1,group_rows) if part else None
        output = Image.new("RGBA", (frame_w * count, frame_h * rows))
        for direction in range(rows):
            for outcol in range(count):
                col = group_first + outcol
                if part:
                    rect = (round(col*group_image.width/group_cols),row_bounds[direction],
                            round((col+1)*group_image.width/group_cols),row_bounds[direction+1])
                    frame = fit_frame(group_image.crop(rect),frame_w,frame_h,ground=not unit["flying"])
                else:
                    rect = ((boundaries[direction][col] if wide else round(col*image.width/cols)),round(direction*image.height/rows),
                            (boundaries[direction][col+1] if wide else round((col+1)*image.width/cols)),round((direction+1)*image.height/rows))
                if wide and not part:
                    crop = image.crop(rect)
                    crop = clean_border_scraps(crop)
                    crop = crop.resize((round(crop.width*uniform_scale),round(crop.height*uniform_scale)),Image.Resampling.LANCZOS)
                    frame = Image.new("RGBA",(frame_w,frame_h))
                    frame.alpha_composite(crop,((frame_w-crop.width)//2,(frame_h-crop.height)//2 if unit["flying"] else frame_h-crop.height))
                elif not part:
                    frame = clean_border_scraps(image.crop(rect).resize((frame_w,frame_h),Image.Resampling.LANCZOS))
                nonzero = int((np.asarray(frame)[:, :, 3] > 16).sum())
                if nonzero < 24:
                    raise ValueError(f"Empty painted frame: {uid} {DIRECTIONS[direction]} col {col}")
                frame_checks.append(nonzero)
                output.alpha_composite(frame, (outcol * frame_w, direction * frame_h))
        filename = f"{uid}-{name}.webp"
        destination = output_dir / filename
        temporary = output_dir / f"{filename}.{os.getpid()}.tmp"
        output.save(temporary, "WEBP", lossless=False, quality=88, method=4)
        os.replace(temporary, destination)
        sheet_paths[name] = f"https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/units/{filename}"
    states = {}
    for name, (group, first, count, fps, loop) in STATES.items():
        state = dict(sheet=sheet_paths[group], frameWidth=frame_w, frameHeight=frame_h,
                     frames=count, directions=DIRECTIONS, rows=record.get("directionRows",{d:i for i,d in enumerate(DIRECTIONS)}),
                     firstColumn=first, fps=fps, loop=loop)
        if name == "attack":
            state["contactMs"] = 270 if unit["melee"] else 0
            state["contactFrame"] = 2
        if record.get("directionFlipX"):
            state["directionFlipX"] = record["directionFlipX"]
        states[name] = state
    if unit["worker"]:
        # Generated tool/channel art is suitable for repeatable worker activity;
        # a dedicated work record, when present, overrides these two poses.
        states["work"] = {**states["ability"], "fps": 6, "loop": True}
    if uid in {"siegeTank", "lurker", "forwardFabricator", "tunnelMaw"}:
        states["deploy"] = {**states["ability"], "fps": 4, "loop": False}
    entry = dict(kind="unit", faction=unit["faction"], fallback=unit["source"],
                 anchor=[.5,.5] if unit["flying"] else [.5,.78], scale=1,
                 states=states, provenance=dict(generator="built-in-imagegen", paintedFrames=192,
                 originalIdentity=unit["source"], minOpaquePixels=min(frame_checks), flying=unit["flying"],
                 sourceSha256=record_hash(record),
                 packerVersion=PACKER_VERSION+("-wide-source-gaps-v4" if wide else "")))
    if record.get("directionFlipX"):
        entry["provenance"]["bilateralProfileReflection"] = record["directionFlipX"]
    return uid, entry

def atomic_json(path: Path, data: dict) -> None:
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(data, indent=2) + "\n")
    os.replace(temporary, path)

def pack_overrides(entries: dict) -> int:
    """Add dedicated worker/deploy art from explicit multi-subject generated grids."""
    painted_frames = 0
    for record_name in ["worker-work-generation-record.json", "unit-deploy-generation-record.json"]:
        path = GAME / "assets" / record_name
        if not path.exists():
            continue
        record = json.loads(path.read_text())
        image = Image.open(record["source"]).convert("RGBA")
        columns, rows = record["layout"]["columns"], record["layout"]["rows"]
        for block in record["blocks"]:
            uid, state, count, first = block["id"], block["state"], block["frames"], block["firstColumn"]
            if uid not in entries:
                continue
            output = Image.new("RGBA", (FRAME_W*count, FRAME_H*rows))
            for row in range(rows):
                for frame_index in range(count):
                    col = first+frame_index
                    rect = (round(col*image.width/columns), round(row*image.height/rows),
                            round((col+1)*image.width/columns), round((row+1)*image.height/rows))
                    frame = fit_frame(image.crop(rect),FRAME_W,FRAME_H,ground=entries[uid].get("anchor",[.5,.78])[1] > .6)
                    output.alpha_composite(frame, (frame_index*FRAME_W,row*FRAME_H))
            destination = ROOT / "graphics/turncraft/animated/units" / f"{uid}-{state}.webp"
            temporary = destination.with_name(f"{destination.name}.{os.getpid()}.tmp")
            output.save(temporary, "WEBP", quality=88, method=4)
            os.replace(temporary, destination)
            entries[uid]["states"][state] = dict(sheet=f"https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/units/{uid}-{state}.webp",
                frameWidth=FRAME_W,frameHeight=FRAME_H,frames=count,directions=DIRECTIONS,
                rows=block.get("directionRows",{d:i for i,d in enumerate(DIRECTIONS)}),firstColumn=0,fps=8,loop=state=="work",
                progressDriven=state=="deploy")
            painted_frames += count*rows
    return painted_frames

def pack_developmental() -> dict:
    path = GAME / "assets/developmental-generation-record.json"
    if not path.exists():
        return {}
    record = json.loads(path.read_text())
    image = Image.open(record["source"]).convert("RGBA")
    columns, rows = record["layout"]["columns"], record["layout"]["rows"]
    result = {}
    for uid, definitions in record["entities"].items():
        output = Image.new("RGBA", (4*128,5*128))
        states = {}
        cursor = 0
        for state_name, indices in definitions.items():
            coordinates = []
            for index in indices:
                col,row = index%columns,index//columns
                rect = (round(col*image.width/columns),round(row*image.height/rows),
                        round((col+1)*image.width/columns),round((row+1)*image.height/rows))
                frame = clean_border_scraps(image.crop(rect).resize((128,128),Image.Resampling.LANCZOS))
                x,y = cursor%4*128,cursor//4*128
                output.alpha_composite(frame,(x,y))
                coordinates.append(dict(x=x,y=y,w=128,h=128))
                cursor += 1
            states[state_name] = dict(sheet=f"https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/units/{uid}.webp",frameWidth=128,
                frameHeight=128,frames=len(indices),directions=["S"],coordinates={"S":coordinates},
                rows={"S":0},firstColumn=0,fps=6,loop=state_name=="idle",progressDriven=state_name=="morph")
        destination = ROOT / "graphics/turncraft/animated/units" / f"{uid}.webp"
        temporary = destination.with_name(f"{destination.name}.{os.getpid()}.tmp")
        output.save(temporary,"WEBP",quality=90,method=4)
        os.replace(temporary,destination)
        result[uid] = dict(kind=uid,faction="brood",fallback="brood larva.png",anchor=[.5,.75],scale=.72,
                          states=states,provenance={"generator":"built-in-imagegen","paintedFrames":cursor})
    return result

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", nargs="*", type=Path)
    parser.add_argument("--exclude", nargs="*", default=[], help="Leave explicitly unfinished type masters out of a staging pack")
    args = parser.parse_args()
    combined_path = GAME / "assets/animation-manifest.json"
    previous_combined = json.loads(combined_path.read_text()) if combined_path.exists() else {}
    roster = json.loads((GAME / "assets/unit-artwork-roster.json").read_text())
    records = args.records or sorted((GAME / "assets").glob("*unit-generation-record.json"))
    records = [p for p in records if p.exists()]
    deduped = {r["id"]: r for r in load_records(records)}
    for faction in ["frontier", "ascendant", "brood"]:
        for path in sorted((GAME / "art-source" / faction).glob("*.json")):
            record = json.loads(path.read_text())
            if isinstance(record, dict) and record.get("id") in roster and record.get("source"):
                deduped[record["id"]] = record
    for uid in args.exclude:
        deduped.pop(uid,None)
    previous_path = GAME / "assets/unit-animation-manifest.json"
    previous = json.loads(previous_path.read_text()).get("units", {}) if previous_path.exists() else {}
    entries = {}
    for uid, record in deduped.items():
        prior = previous.get(uid, {})
        source_hash = record_hash(record)
        provenance = prior.get("provenance", {})
        expected_version = PACKER_VERSION+("-wide-source-gaps-v4" if uid in {"latticeSkiff","voidLancer"} else "")
        cached = provenance.get("sourceSha256") == source_hash and provenance.get("packerVersion") == expected_version
        cached = cached and all((ROOT/"graphics/turncraft/animated/units"/s["sheet"].rsplit("/",1)[-1]).exists() for s in prior.get("states",{}).values())
        if cached:
            entries[uid] = prior
            if record.get("directionRows"):
                for state_name in STATES:
                    entries[uid]["states"][state_name]["rows"] = record["directionRows"]
            for state_name in STATES:
                if record.get("directionFlipX"):
                    entries[uid]["states"][state_name]["directionFlipX"] = record["directionFlipX"]
                else:
                    entries[uid]["states"][state_name].pop("directionFlipX",None)
            if record.get("directionFlipX"):
                entries[uid]["provenance"]["bilateralProfileReflection"] = record["directionFlipX"]
        else:
            key, entry = pack_record(record, roster)
            entries[key] = entry
    extra_painted_frames = pack_overrides(entries)
    path = GAME / "assets/unit-animation-manifest.json"
    atomic_json(path,{"units": entries, "directions": DIRECTIONS})
    structure_path = GAME / "assets/structure-animation-manifest.json"
    structures = json.loads(structure_path.read_text()).get("structures", {}) if structure_path.exists() else {}
    game_source = (ROOT / "game/turncraft.html").read_text()
    structure_definitions = game_source.split("const STRUCTURES={", 1)[1].split("\n};", 1)[0]
    for line in structure_definitions.splitlines():
        match = re.match(r"\s*(\w+):\{f:'([^']+)',name:'([^']+)'", line)
        if match and match[1] in structures:
            structures[match[1]].update(kind="structure", faction=match[2], fallback=match[3].lower()+".png")
    if "tunnelExit" in structures:
        structures["tunnelExit"].update(kind="structure", faction="brood", fallback="tunnel maw open.png")
    developmental = pack_developmental()
    merged = {**entries, **structures, **developmental}
    manifest = dict(version=1, directions=DIRECTIONS, assets=merged)
    # Crop measurements belong to exact production bytes. Keep them when an
    # unchanged atlas is rebuilt, and discard stale measurements after new art.
    unit_sheets = {state["sheet"] for asset in {**entries, **developmental}.values()
                   for state in asset["states"].values()}
    repairs = {}
    for url, repair in previous_combined.get("cropRepairs", {}).items():
        source = ROOT / "graphics/turncraft/animated/units" / url.rsplit("/", 1)[-1]
        if (url in unit_sheets and source.is_file()
                and hashlib.sha256(source.read_bytes()).hexdigest() == repair.get("sourceSha256")):
            repairs[url] = repair
    if repairs:
        manifest["cropRepairs"] = repairs
    atomic_json(combined_path,manifest)
    print(json.dumps({"units": len(entries), "structures": len(structures), "paintedUnitFrames":len(entries)*192,
                      "missingUnits":sorted(set(roster)-set(entries)), "sheets":len(entries)*3,
                      "additionalPaintedFrames":extra_painted_frames+sum(v["provenance"]["paintedFrames"] for v in developmental.values())}))

if __name__ == "__main__":
    main()
