"""Repack generated source sheets into small, named WebP atlases for R2.

Usage: python scripts/package-dwarf-world-art.py SOURCE_INDEX.json OUTPUT_DIR
SOURCE_INDEX maps atlas names to absolute source PNG paths. Pillow is an authoring
dependency only; the game needs no Python, Node.js, server, or image processor.
"""
import hashlib
import json
from pathlib import Path
import sys
import zipfile
from PIL import Image

CELL = 128
BIOMES = ['ancient', 'fungal', 'crystal', 'volcanic', 'ruins', 'abyssal']
TILES = ['floor_a', 'floor_b', 'wall', 'edge_n', 'corner_nw', 'gold', 'gem', 'detail']
ACTIONS = ['walk', 'mine', 'fight', 'shoot', 'smith', 'haul', 'eat', 'drink', 'sleep', 'rest', 'construct', 'celebrate']
PROFESSIONS = ['farm', 'cook', 'heal', 'engineer', 'explore', 'soldier_walk']
BUILDINGS = ['great_hall', 'tavern', 'treasury', 'barracks', 'workshop', 'mushroom_farm', 'infirmary', 'survey_hall', 'lift_shaft', 'shrine', 'great_vault', 'forge', 'depot', 'arrow', 'cannon', 'frost', 'heal', 'haste', 'guard', 'fortress', 'gate', 'barricade', 'rail', 'great_hall_upgraded']
ENEMIES = ['goblin', 'orc', 'troll', 'bat', 'runner', 'armored', 'splitter', 'bomber', 'shaman', 'phaser', 'necromancer', 'skeleton', 'goblin_king', 'stone_colossus', 'brood_mother', 'ancient_lich', 'deepfire_wyrm', 'crystal_sentinel', 'sapper', 'commander', 'healer', 'abyssal_stalker', 'spider', 'siege']
INTERIORS = ['quarters', 'dining', 'kitchen', 'garden', 'granary', 'armory', 'smelter', 'smithy', 'stoneworks', 'vault', 'medical', 'library', 'guardroom', 'market', 'throne', 'tavern', 'engineer', 'explorer', 'rail', 'rail_corner', 'road', 'door', 'stockpile', 'hauling']
DECORATIONS = ['pillar', 'stalagmites', 'mushroom', 'moss', 'crystal', 'crystal_shrine', 'steam_vent', 'fissure', 'statue', 'arch', 'treasure', 'workbench', 'banner', 'torch', 'lantern', 'crates']
ICONS = ['build', 'dwarves', 'military', 'economy', 'exploration', 'inventory', 'alerts', 'map', 'food', 'ore', 'metal', 'stone', 'fortress', 'morale', 'settings', 'history']

def main():
    source_index = json.loads(Path(sys.argv[1]).read_text(encoding='utf-8-sig'))
    target = Path(sys.argv[2]).resolve()
    target.mkdir(parents=True, exist_ok=True)
    manifest = {'version': 1, 'pack': 'living-mountain-v1', 'prefix': 'dwarf-world/living-mountain-v1/', 'frameSize': [CELL, CELL], 'atlases': {}, 'frames': {}, 'animations': {}, 'essential': ['terrain_ancient', 'dwarves', 'buildings', 'enemies', 'ui'], 'provenance': {'method': 'OpenAI built-in image generation; original illustrated poses; normalized and WebP packed with Pillow', 'date': '2026-10-21'}}
    def sheet(key, source_key, path, cols, rows, names, source_row=None, terrain=False):
        src = Image.open(source_index[source_key]).convert('RGBA')
        if source_row is not None:
            h = src.height // 6
            src = src.crop((0, source_row*h, src.width, (source_row+1)*h))
        output = Image.new('RGBA', (cols*CELL, rows*CELL))
        for i, name in enumerate(names):
            x,y = i%cols,i//cols
            # Preserve each genuinely different source cell; never duplicate poses.
            box = (round(x*src.width/cols),round(y*src.height/rows),round((x+1)*src.width/cols),round((y+1)*src.height/rows))
            sprite = src.crop(box)
            if terrain and x < 2 and 'floors' in source_index:
                # Corrective quiet floor art replaces the first two columns;
                # existing authored walls, transitions and ores stay intact.
                floors = Image.open(source_index['floors']).convert('RGBA')
                fx = [0,2,3,4,1,5][source_row]
                sprite = floors.crop((round(fx*floors.width/6),round(x*floors.height/2),round((fx+1)*floors.width/6),round((x+1)*floors.height/2)))
            if terrain:
                sprite = sprite.resize((CELL,CELL), Image.Resampling.LANCZOS)
                output.paste(sprite,(x*CELL,y*CELL))
            else:
                # Generative alpha can contain virtually invisible ghost pixels;
                # ignore those for trim bounds without altering visible paint.
                bounds = sprite.getchannel('A').point(lambda a: 255 if a >= 16 else 0).getbbox()
                if bounds: sprite = sprite.crop(bounds)
                if key == 'dwarves' or (key == 'professions' and i < 18):
                    # A whole authored action shares one scale, so raising a tool
                    # cannot make the character shrink between animation frames.
                    widths, heights = [], []
                    for other in range(i//3*3, i//3*3+3):
                        ox,oy=other%cols,other//cols
                        cell=src.crop((round(ox*src.width/cols),round(oy*src.height/rows),round((ox+1)*src.width/cols),round((oy+1)*src.height/rows)))
                        b=cell.getchannel('A').point(lambda a: 255 if a >= 16 else 0).getbbox()
                        if b: widths.append(b[2]-b[0]);heights.append(b[3]-b[1])
                    scale=min((CELL-12)/max(widths),(CELL-12)/max(heights))
                    sprite=sprite.resize((max(1,round(sprite.width*scale)),max(1,round(sprite.height*scale))),Image.Resampling.LANCZOS)
                else: sprite.thumbnail((CELL-12,CELL-12),Image.Resampling.LANCZOS)
                output.alpha_composite(sprite,(x*CELL+(CELL-sprite.width)//2,y*CELL+CELL-6-sprite.height))
            manifest['frames'][f'{key}.{name}'] = {'atlas':key,'rect':[x*CELL,y*CELL,CELL,CELL]}
        file = target/path
        file.parent.mkdir(parents=True,exist_ok=True)
        output.save(file,'WEBP',quality=85,method=6,exact=True)
        content=file.read_bytes()
        manifest['atlases'][key]={'path':path,'width':output.width,'height':output.height,'columns':cols,'rows':rows,'bytes':len(content),'sha256':hashlib.sha256(content).hexdigest()}
    for row,biome in enumerate(BIOMES):
        sheet(f'terrain_{biome}','terrain',f'terrain/{biome}.webp',8,1,TILES,row,True)
    sheet('dwarves','dwarves','dwarves/actions.webp',6,6,[f'{action}_{i}' for action in ACTIONS for i in range(3)])
    sheet('professions','professions','dwarves/professions.webp',6,4,[f'{action}_{i}' for action in PROFESSIONS for i in range(3)]+['armor_cloth','armor_leather','armor_chain','armor_steel','armor_titanium','armor_relic'])
    sheet('equipment','equipment','dwarves/equipment.webp',4,4,['armor_leather','armor_chain','armor_steel','armor_titanium','armor_relic','pickaxe_iron','pickaxe_steel','pickaxe_relic','axe_iron','axe_relic','sword_steel','sword_relic','bow_wood','bow_relic','hammer','backpack'])
    sheet('buildings','buildings','buildings/structures.webp',6,4,BUILDINGS)
    sheet('enemies','enemies','enemies/inhabitants.webp',6,4,ENEMIES)
    sheet('interiors','interiors','interiors/furniture.webp',6,4,INTERIORS)
    sheet('decorations','decorations','decorations/caverns.webp',4,4,DECORATIONS)
    sheet('effects','effects','effects/activity.webp',4,4,[f'{action}_{i}' for action in ['sparks','torch','dust','spores'] for i in range(4)])
    sheet('ui','ui','ui/commands.webp',4,4,ICONS)
    sheet('lighting','lighting','lighting/soft-pools.webp',2,2,['torch','crystal','forge','river'])
    for atlas,actions,count,fps in [('dwarves',ACTIONS,3,5),('professions',PROFESSIONS,3,4),('effects',['sparks','torch','dust','spores'],4,6)]:
        for action in actions:
            manifest['animations'][f'{atlas}.{action}']={'frames':[f'{atlas}.{action}_{i}' for i in range(count)],'fps':fps,'loop':True}
    manifest['limitations']=['Characters use one painted view with horizontal facing, not eight authored directions.', 'Equipment tier idle silhouettes and active equipment overlays are distinct; every action does not have a fully repainted armor and weapon permutation.', 'Enemy art has single poses; movement is interpolated and bobbed, not a complete authored enemy animation set.', 'Only the Great Hall has an authored upgraded silhouette; other upgrades use visible rank accents.', 'Illustrated terrain has edge and corner variants but is not an exhaustive 47-tile Wang atlas.']
    (target/'art-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
    (target/'UPLOAD.txt').write_text('Upload the CONTENTS of this folder under the exact R2 object prefix:\ndwarf-world/living-mountain-v1/\n\nThen configure DWARF_ART_CONFIG.assetBase in game/dwarf-world/config.js to the public R2/custom-domain origin. No public URL has been provisioned by this pack. See ART.md for cache/CORS instructions.\n',encoding='utf-8')
    print(json.dumps({'files':len(manifest['atlases']),'frames':len(manifest['frames']),'animations':len(manifest['animations']),'bytes':sum(a['bytes'] for a in manifest['atlases'].values()),'manifest':str(target/'art-manifest.json')}))

if __name__=='__main__':main()
