#!/usr/bin/env python3
"""Pack imagegen-painted structure frames into compact transparent game atlases.

No frame synthesis: this only crops, downsizes and packs original painted cells.
Run from the repository root after generating the contact sheets recorded in
game/turncraft/art-source/structures/generation-log.json.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from PIL import Image

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'graphics/turncraft/animated/structures'
SOURCE = ROOT / 'game/turncraft/art-source/structures'
CELL = 128
DEFENSE = {'missileTurret', 'bunker', 'photonCannon', 'sunkenColony'}
STATE = ['idle', 'production', 'construction', 'death']


def rgba_cell(sheet: Image.Image, box: tuple[int, int, int, int]) -> Image.Image:
    """Preserve artist's shared frame registration; retain generated alpha."""
    tile = sheet.crop(box).convert('RGBA')
    tile = tile.resize((CELL, CELL), Image.Resampling.LANCZOS)
    return remove_boundary_scraps(tile)


def remove_boundary_scraps(tile: Image.Image) -> Image.Image:
    """Discard only disconnected <0.5% alpha islands touching a cell edge.

    This is atlas crop cleanup, never pose or animation synthesis. Body-connected
    glows, weapon tips and larger independent meaningful effects remain intact.
    """
    alpha = tile.getchannel('A')
    values = list(alpha.tobytes())
    visited = bytearray(CELL * CELL)
    remove = []
    for seed, value in enumerate(values):
        if value <= 8 or visited[seed]:
            continue
        stack = [seed]
        visited[seed] = 1
        component = []
        touches_edge = False
        while stack:
            current = stack.pop()
            component.append(current)
            x, y = current % CELL, current // CELL
            touches_edge |= x == 0 or x == CELL - 1 or y == 0 or y == CELL - 1
            for ny in range(max(0, y - 1), min(CELL, y + 2)):
                for nx in range(max(0, x - 1), min(CELL, x + 2)):
                    neighbor = ny * CELL + nx
                    if values[neighbor] > 8 and not visited[neighbor]:
                        visited[neighbor] = 1
                        stack.append(neighbor)
        if touches_edge and len(component) < CELL * CELL * 0.005:
            remove.extend(component)
    if remove:
        for index in remove:
            values[index] = 0
        alpha.putdata(values)
        tile.putalpha(alpha)
    return tile


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--log', type=Path, default=SOURCE / 'generation-log.json')
    parser.add_argument('--source-dir', type=Path, default=SOURCE,
                        help='Folder containing regenerated contact sheets named in the log')
    parser.add_argument('--skip-existing', action='store_true',
                        help='Retain already packed sheets from this same generation batch')
    args = parser.parse_args()
    log = json.loads(args.log.read_text())
    manifest = {'structures': {}, 'generation': {
        'mode': 'built-in imagegen', 'log': '../art-source/structures/generation-log.json',
        'packing': 'Original painted cells only; no synthesized motion frames.'}}
    report = []
    for group in log['groups']:
        image_path = Path(group.get('sourcePath', SOURCE / group['file']))
        if not image_path.exists():
            image_path = args.source_dir / group['file']
        sheet = Image.open(image_path).convert('RGBA')
        columns, rows = group.get('columns', 8), group.get('rows', len(group['keys']) * 2)
        source_width, source_height = sheet.size
        if sheet.getextrema()[3][0] == 255:
            raise ValueError(f'{image_path.name}: generation did not retain transparent alpha')
        for index, key in enumerate(group['keys']):
            atlas = Image.new('RGBA', (CELL * 4, CELL * 4))
            hashes = []
            for state_index, state in enumerate(STATE):
                source_row = index * 2 + state_index // 2
                source_column = (state_index % 2) * 4
                for frame in range(4):
                    col = source_column + frame
                    box = (round(col * source_width / columns),
                           round(source_row * source_height / rows),
                           round((col + 1) * source_width / columns),
                           round((source_row + 1) * source_height / rows))
                    cell = rgba_cell(sheet, box)
                    if not cell.getbbox():
                        raise ValueError(f'{key} {state} frame {frame}: empty cell')
                    atlas.alpha_composite(cell, (frame * CELL, state_index * CELL))
                    hashes.append(hashlib.sha256(cell.tobytes()).hexdigest())
            output = OUT / f'{key}.webp'
            if not (args.skip_existing and output.exists() and output.stat().st_size > 0):
                temporary_output = output.with_suffix('.tmp.webp')
                atlas.save(temporary_output, format='WEBP', quality=88, method=4, exact=True)
                with Image.open(temporary_output) as packed:
                    packed.verify()
                temporary_output.replace(output)
            states = {}
            for state_index, state in enumerate(STATE):
                states[state] = {'sheet': f'https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/structures/{key}.webp',
                    'frameWidth': CELL, 'frameHeight': CELL, 'frames': 4,
                    'directions': ['S'], 'rows': {'S': state_index},
                    'firstColumn': 0, 'fps': 3.5 if state == 'death' else 6 if state == 'construction' else 3,
                    'loop': state in ('idle', 'production')}
                if state == 'construction':
                    states[state]['progressDriven'] = True
                if state == 'death':
                    states[state]['durationMs'] = round(4000 / 3.5)
            if key in DEFENSE:
                states['attack'] = {**states['production'], 'loop': False, 'fps': 8, 'contactMs': 125}
            manifest['structures'][key] = {'anchor': [0.5, 0.8], 'frameWidth': CELL,
                'frameHeight': CELL, 'states': states,
                'sourceRecord': '../art-source/structures/generation-log.json', 'paintedFrameCount': 16}
            report.append({'type': key, 'bytes': output.stat().st_size,
                'uniqueFrames': len(set(hashes)), 'states': len(states), 'alpha': atlas.getextrema()[3]})
    manifest_path = ROOT / 'game/turncraft/assets/structure-animation-manifest.json'
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2) + '\n')
    (SOURCE / 'packing-report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'structures': len(report), 'frames': len(report) * 16,
                      'bytes': sum(r['bytes'] for r in report),
                      'minimumUniqueFrames': min(r['uniqueFrames'] for r in report)}, indent=2))


if __name__ == '__main__':
    main()
