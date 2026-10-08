"""Read authored RGBA sheets and rebuild crop/attachment metadata; never repaint art.
Run with Pillow installed. Cell partitions were checked against the actual images,
not inferred from an equal grid (barrels and tails extend past nominal cells).
"""
from pathlib import Path
import json
from PIL import Image

ROOT = Path(__file__).resolve().parents[1] / 'assets' / 'visual' / 'vehicles'
atlas_specs = {
    'light': ([0, 265, 551, 889, 1173, 1402], [0, 282, 577, 847, 1122]),
    'heavy': ([0, 246, 576, 943, 1289, 1536], [0, 279, 501, 746, 1024]),
    'turrets': ([0, 244, 509, 816, 1066, 1295], [0, 251, 511, 746, 951, 1214]),
    'aircraft': ([0, 220, 505, 887, 1183, 1402], [0, 294, 578, 850, 1122]),
}
frames = {}
atlases = []

def add(name, family, row, col, anchor=None, cell_override=None):
    image, xs, ys = sheets[family]
    cell = cell_override or (xs[col], ys[row], xs[col+1], ys[row+1])
    # Ignore near-invisible anti-alias noise for bounds; preserve original pixels.
    box = image.getchannel('A').crop(cell).point(lambda a: 255 if a > 8 else 0).getbbox()
    if box is None:
        raise ValueError(name + ' has no visible artwork')
    x, y = cell[0] + box[0], cell[1] + box[1]
    w, h = box[2]-box[0], box[3]-box[1]
    point = [round(w/2, 2), round(h*.88, 2)] if anchor is None else [anchor[0]-x, anchor[1]-y]
    frames[name] = {'atlas': 'vehicles-'+family, 'rect': [x, y, w, h], 'anchor': point}
    return frames[name]

sheets = {}
for family, (xs, ys) in atlas_specs.items():
    im = Image.open(ROOT / (family+'.png'))
    assert im.mode == 'RGBA' and im.getchannel('A').getextrema()[0] == 0
    assert im.size == (xs[-1], ys[-1]), (family, im.size)
    sheets[family] = (im, xs, ys)
    atlases.append({'id': 'vehicles-'+family, 'file': 'vehicles/'+family+'.png', 'width': im.width, 'height': im.height})

directions = ['south', 'southeast', 'east', 'northeast', 'north']
for row, kind in enumerate(['jeep', 'command', 'truck', 'armored']):
    for col, direction in enumerate(directions):
        f = add(kind+'-'+direction, 'light', row, col)
        # Visible hub centres in the authored side views; animation is a small
        # highlight over the existing tires, never a replacement wheel drawing.
        wheels = {'jeep': [(632,215,10),(813,219,10)],
                  'command': [(630,507,10),(817,514,10)],
                  'truck': [(615,775,10),(664,776,10),(837,777,10)],
                  'armored': [(628,1037,10),(823,1038,10)]}
        if direction == 'east':
            f['wheels'] = [[x-f['rect'][0], y-f['rect'][1], radius] for x,y,radius in wheels[kind]]
for row, kind in enumerate(['apc', 'ifv', 'tank']):
    for col, direction in enumerate(directions):
        f = add(kind+'-'+direction, 'heavy', row, col)
        # Absolute attachment centres are the actual sockets in the authored hull.
        mounts = [[137, 146], [411, 133], [753, 145], [1100, 139], [1406, 149]]
        if row > 0:
            mounts = [[133, 341 if row == 1 else 581], [408, 347 if row == 1 else 581],
                      [771, 354 if row == 1 else 599], [1101, 334 if row == 1 else 576],
                      [1400, 341 if row == 1 else 581]]
        f['mount'] = [mounts[col][0]-f['rect'][0], mounts[col][1]-f['rect'][1]]
for col, kind in enumerate(['jeep', 'truck', 'apc', 'tank', 'heli']):
    add('wreck-'+kind, 'heavy', 3, col)

# The swivel ring, rather than image centre (which includes the barrel), anchors turrets.
turret_rings = [
    [(121,205),(360,202),(592,202),(924,211),(1173,214)],
    [(119,431),(359,423),(592,431),(922,444),(1177,450)],
    [(119,669),(360,669),(590,679),(923,700),(1177,703)],
    [(120,912),(364,904),(607,910),(926,913),(1178,922)],
    [(119,1128),(359,1127),(590,1135),(928,1149),(1174,1151)],
]
for row, kind in enumerate(['ifv', 'tank', 'repeater', 'bombard', 'cyclone']):
    for col, direction in enumerate(directions):
        add('turret-'+kind+'-'+direction, 'turrets', row, col, turret_rings[row][col])

masts = [[(118,111),(392,99),(768,100),(1088,80),(1295,98)],
         [(117,384),(394,375),(748,378),(1083,357),(1295,375)],
         [(116,660),(394,650),(748,650),(1088,629),(1295,650)]]
tails = [[(116,36),(243,61),(529,111),(948,229),(1295,253)],
         [(117,326),(263,340),(524,389),(947,516),(1294,527)],
         [(116,609),(265,615),(528,674),(948,771),(1294,797)]]
for row, kind in enumerate(['recon', 'scout', 'gunship']):
    for col, direction in enumerate(directions):
        f = add('heli-'+kind+'-'+direction, 'aircraft', row, col, masts[row][col])
        f['tail'] = [tails[row][col][0]-masts[row][col][0], tails[row][col][1]-masts[row][col][1]]
        if kind == 'gunship' and direction == 'east':
            # The adjacent diagonal nose extends into this cell's lower-left
            # corner. Use an authored crop polygon; source alpha stays untouched.
            f['clip'] = [[x-f['rect'][0], y-f['rect'][1]] for x,y in [(508,628),(884,628),(884,805),(660,805),(620,750),(508,739)]]
rotor_cells = [(0,850,290,1122),(290,850,578,1122),(578,850,888,1122),(888,850,1168,1122),(1168,850,1402,1122)]
rotor_hubs = [(146,977),(429,977),(734,978),(1023,986),(1287,991)]
for col, name in enumerate(['rotor-main', 'rotor-main-angle', 'rotor-main-blur', 'rotor-tail', 'rotor-tail-blur']):
    f = add(name, 'aircraft', 3, col, rotor_hubs[col], rotor_cells[col])

manifest = {'version': 1, 'projection': {'x': .8, 'y': .42}, 'source': 'Original built-in imagegen artwork; original RGBA alpha preserved byte-for-byte; prompts.json records prompts.',
            'atlases': atlases, 'frames': frames,
            'directions': directions, 'screenSectors': ['east','southeast','south','southwest','west','northwest','north','northeast'],
            'headingColumns': [2,1,0,1,2,3,4,3], 'mirrorSectors': [3,4,5],
            'classes': ['jeep','command','armored','truck','apc','ifv','tank'],
            'helicopters': ['recon','scout','gunship'],
            'notes': 'Five illustrated headings and mirrored counterparts resolve eight directions. Rotors and turret heads are independently animated; image bounds never define collision geometry.'}
(ROOT/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n', encoding='utf-8')
print('Validated', len(atlases), 'RGBA atlases and wrote', len(frames), 'frames.')
