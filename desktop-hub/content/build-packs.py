"""Build offline game packs from the exact Magic and Truth release checkouts."""
import argparse
import hashlib
import json
import re
from pathlib import Path
import shutil
import subprocess
import tempfile
from zipfile import ZipFile

parser = argparse.ArgumentParser()
parser.add_argument('magic', type=Path)
parser.add_argument('truth', type=Path)
parser.add_argument('--only', nargs='+', help='Rebuild specific catalog keys in an existing release tree')
args = parser.parse_args()
magic, truth = args.magic.resolve(), args.truth.resolve()
out = Path(__file__).resolve().parent
catalog = json.loads((out / 'catalog.json').read_text(encoding='utf8'))
records = {g['id']: g for g in catalog['games']}
manifest = json.loads((out/'pack-index.json').read_text(encoding='utf8')) if args.only else {}
excluded = {'source', 'tests', 'docs', 'scripts', 'tools', 'node_modules',
            'desktop', 'hosting', 'server', 'directory', '.git', 'art-pack'}
text_types = {'.html','.js','.mjs','.css','.svg','.json','.txt','.md'}

def copy(source, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.suffix.lower() in text_types:
        # Git for Windows may check out CRLF. Several official loaders match LF
        # sentinels to inject parts of their games, so preserve the deployed bytes.
        data = source.read_bytes()
        if source.suffix.lower()=='.html':
            data=re.sub(rb'<script type="module" src="/(?:Truth/)?hub/play-signal\.mjs"[^>]*></script>', b'', data)
        target.write_bytes(data.replace(b'\r\n', b'\n'))
    else:
        shutil.copyfile(source,target)

def copy_tree(source, target):
    if not source.is_dir():
        raise ValueError(f'Missing game folder {source}')
    for item in source.rglob('*'):
        rel = item.relative_to(source)
        if item.is_file() and not (set(rel.parts) & excluded) and not item.name.startswith('.') and not any(
            suffix in item.name for suffix in ('.test.', '-prompt', 'prompts.', 'package-lock.', '.spec.')
        ):
            copy(item,target/rel)

def build(key, repo, entry, files=(), folders=(), edition=None):
    if args.only and key not in args.only:
        return
    if key not in records or 'single' not in records[key]['modes']:
        raise ValueError('Pack for missing solo catalog entry '+key)
    root = out/'packs'/key
    root.mkdir(parents=True, exist_ok=True)
    for rel in files:
        copy(repo/rel,root/rel)
    for rel in folders:
        copy_tree(repo/rel,root/rel)
    if edition:
        edition(root)
    if not (root/entry).is_file():
        raise ValueError('Missing offline entry '+entry)
    files = sorted(p for p in root.rglob('*') if p.is_file())
    # Include every byte, not just the entry page: verify non-HTML assets too.
    digest = hashlib.sha256()
    for p in files:
        digest.update(p.relative_to(root).as_posix().encode('utf8')+b'\x00')
        with p.open('rb') as stream:
            digest.update(hashlib.file_digest(stream,'sha256').digest())
    manifest[key] = {'type':'static','root':'packs/'+key,'entry':entry,
        'bytes':sum(p.stat().st_size for p in files),
        'version':catalog['source']['magic' if repo==magic else 'truth'],
        'digest':digest.hexdigest()}

def bannerlands(root):
    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run(['node',str(truth/'games/bannerlands/source/build-standalone.mjs'),tmp],check=True)
        copy(Path(tmp)/'Bannerlands.html',root/'Bannerlands.html')

def hearthmouse(root):
    with ZipFile(magic/'game/hearthmouse/Hearthmouse-Browser.zip') as z:
        for item in z.infolist():
            target = (root/item.filename).resolve()
            if not target.is_relative_to(root) or target==root:
                raise ValueError('Invalid Hearthmouse zip member')
            if not item.is_dir():
                target.parent.mkdir(parents=True,exist_ok=True)
                with target.open('wb') as dest, z.open(item) as src:
                    shutil.copyfileobj(src,dest)
    copy_tree(magic/'game/hearthmouse/audio',root/'audio')

def local_three(entry):
    def apply(root):
        target = root/entry
        local = target.parent/'vendor'
        copy_tree(magic/'game/receiver-window-qb/vendor',local)
        source = target.read_text(encoding='utf8')
        original = 'https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js'
        if original not in source or '160' not in (local/'three.min.js').read_text(encoding='utf8'):
            raise ValueError('Review Three.js version for '+entry)
        target.write_text(source.replace(original,'./vendor/three.min.js'),encoding='utf8',newline='\n')
    return apply

build('cat',magic,'game/catandmouse/edition/index.html',folders=['game/catandmouse/edition'])
build('trashtactics',magic,'game/trash-tactics/index.html',folders=['game/trash-tactics'])
build('bannerlands',truth,'Bannerlands.html',edition=bannerlands)
build('hearthmouse',magic,'index.html',edition=hearthmouse)
build('ironfield',magic,'game/ironfield/IRONFIELD.html',['game/ironfield/IRONFIELD.html'])
for key,slug in [('pirates','pirates'),('monstermallexpanded','monster-mall-expanded'),
    ('pacmanblackout','pacman'),('academystandards','academy-standards'),('operationwhiskerfall','operation-whiskerfall')]:
    build(key,truth,'index.html',edition=lambda p:copy_tree(truth/'games'/slug,p))
for key in ['apestogetherstrong','balatlesolo','cardrealms','deepsea','microwars4','mountain','starcraft2','swarm']:
    entry = records[key]['route'].lstrip('/')
    build(key,magic,entry,[entry])
for key, folder, extras in [('bramblebound','bramblebound',[]),('flag','flag-capture',[]),
       ('matchwars','matchwars',[]),('circuitbound','circuitbound',[]),
       ('splinters','splinters',['game/receiver-window-qb/vendor/three.min.js','game/receiver-window-qb/vendor/THREE-LICENSE.txt','lobby/assets/game-tiles/splinters.webp']),
       ('spacetyrants','space-tyrants',['game/space-tyrants-core.html']),
       ('dwarfworld','dwarf-world',['game/DwarfWorld-core.html'])]:
    entry = records[key]['route'].lstrip('/')
    entry += 'index.html' if entry.endswith('/') else ''
    build(key,magic,entry,[entry,*extras],['game/'+folder])
build('gateguard',magic,'game/gateguard.html',['game/gateguard.html','game/gateguard-core.html','game/gateguard-expansion.js'],['game/gateguard-expansion','graphics/lastfive'])
build('goofywizard',magic,'game/Goofy Wizard.html',['game/Goofy Wizard.html'],edition=local_three('game/Goofy Wizard.html'))
build('basketball2',magic,'game/basketball2/index.html',folders=['game/basketball2'],edition=local_three('game/basketball2/index.html'))
def catlanes(root):
    # The two case-distinct filenames collide in a Windows checkout.
    data=subprocess.check_output(['git','-C',str(magic),'show','HEAD:game/CatLanes.html'])
    (root/'index.html').write_bytes(data)
build('mobamania',magic,'index.html',edition=catlanes)
(out/'pack-index.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf8')
print(f'{len(manifest)} offline packs, {sum(g["bytes"] for g in manifest.values()):,} bytes')
