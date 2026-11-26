"""Build offline game packs from the exact Magic and Truth release checkouts."""
import argparse
import hashlib
import json
import os
import re
from functools import cache
from pathlib import Path
import shutil
import subprocess
import tempfile
from zipfile import ZipFile
from urllib.parse import urlsplit

parser = argparse.ArgumentParser()
parser.add_argument('magic', type=Path)
parser.add_argument('truth', type=Path)
parser.add_argument('--only', nargs='+', help='Rebuild specific catalog keys in an existing release tree')
parser.add_argument('--last-signal-build', type=Path, help='Package the Windows executable built from the selected Truth source')
parser.add_argument('--cloudflared', type=Path, help='Pinned Windows x64 Cloudflare runtime for the local multiplayer packs')
args = parser.parse_args()
magic, truth = args.magic.resolve(), args.truth.resolve()
out = Path(__file__).resolve().parent
catalog = json.loads((out / 'catalog.json').read_text(encoding='utf8'))
records = {g['id']: g for g in catalog['games']}
manifest = json.loads((out/'pack-index.json').read_text(encoding='utf8')) if args.only else {}
excluded = {'source', 'tests', 'docs', 'scripts', 'tools', 'node_modules',
            'desktop', 'hosting', 'server', 'directory', '.git', 'art-pack'}
text_types = {'.html','.js','.mjs','.css','.svg','.json','.txt','.md'}
runtime_hashes = {
    'node.exe': '3602f2bb1a10f2cbab4c36886218a33c1ab3db87290e73b033c46c77147d0237',
    'cloudflared.exe': 'd5c1d82031da6253dcdcff64c944cd0fe063016edeb2e4fd088075fc9bd52d0a',
}

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

def copy_tree(source, target, *, skip=()):
    if not source.is_dir():
        raise ValueError(f'Missing game folder {source}')
    for item in source.rglob('*'):
        rel = item.relative_to(source)
        if item.is_file() and rel.as_posix() not in skip and not (set(rel.parts) & excluded) and not item.name.startswith('.') and not any(
            suffix in item.name for suffix in ('.test.', '-prompt', 'prompts.', 'package-lock.', '.spec.')
        ):
            copy(item,target/rel)

def build(key, repo, entry, files=(), folders=(), edition=None, hosting=None, adapter=None):
    if args.only and key not in args.only:
        return
    if key not in records or 'single' not in records[key]['modes']:
        raise ValueError('Pack for missing solo catalog entry '+key)
    root = out/'packs'/key
    if root.exists():
        pack_root = root.resolve()
        if root.is_symlink() or pack_root.parent != (out/'packs').resolve():
            raise ValueError('Invalid pack directory '+str(root))
        shutil.rmtree(pack_root)
    root.mkdir(parents=True, exist_ok=True)
    for rel in files:
        copy(repo/rel,root/rel)
    for rel in folders:
        copy_tree(repo/rel,root/rel)
    if edition:
        edition(root)
    if adapter:
        manager = root/'manager'
        copy_tree(magic/'game/server-manager/host', manager)
        for module in ('community-directory.cjs', 'community-tunnel.cjs'):
            copy(truth/'games/acorn-rally/server'/module, manager/module)
        copy(repo/adapter, manager/('adapter'+Path(adapter).suffix))
        if not (manager/'supervisor.cjs').is_file():
            raise ValueError('Missing Server Manager supervisor for '+key)
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
    if hosting:
        server, runtime_folder = hosting
        runtime = runtime_folder+'/node.exe'
        tunnel = runtime_folder+'/cloudflared.exe'
        if not all((root/item).is_file() for item in (server, runtime, tunnel)):
            raise ValueError('Missing local hosting files for '+key)
        manifest[key].update(hosting=True, server=server, runtime=runtime, tunnel=tunnel)
    if adapter:
        manifest[key]['manager'] = 'manager/supervisor.cjs'

@cache
def host_runtimes():
    node = shutil.which('node.exe')
    if not node or not args.cloudflared:
        raise ValueError('Use the pinned Windows x64 Node and pass --cloudflared for the local multiplayer packs')
    binaries = {'node.exe':Path(node), 'cloudflared.exe':args.cloudflared.resolve()}
    for name, file in binaries.items():
        with file.open('rb') as stream:
            if hashlib.file_digest(stream,'sha256').hexdigest() != runtime_hashes[name]:
                raise ValueError(name+' does not match the pinned Windows x64 runtime')
    return binaries

def bundle_host_runtime(root, runtime_folder='hub-runtime'):
    for name, file in host_runtimes().items():
        copy(file,root/runtime_folder/name)
    copy_tree(truth/'games/bannerlands/source/hosting/licenses',root/'notices')

def install_host(root, folder, *, locked):
    bundle_host_runtime(root)
    npm = shutil.which('npm.cmd' if os.name == 'nt' else 'npm')
    if not npm:
        raise ValueError('npm is required to package the local match server')
    if locked:
        copy(magic/folder/'package-lock.json',root/folder/'package-lock.json')
    subprocess.run([npm,'ci' if locked else 'install','--omit=dev','--omit=optional',
                    '--ignore-scripts','--no-audit','--no-fund','--package-lock=' + str(locked).lower()],
                   cwd=root/folder,check=True)
    package = json.loads((root/folder/'package.json').read_text(encoding='utf8'))
    installed = json.loads((root/folder/'node_modules/ws/package.json').read_text(encoding='utf8'))
    if installed['version'] != package['dependencies']['ws']:
        raise ValueError('Incorrect WebSocket runtime version for '+str(folder))

cat_folder = Path('game/catandmouse/edition')
trash_folder = Path('game/trash-tactics')

def cat_edition(root):
    copy_tree(magic/cat_folder/'server',root/cat_folder/'server')
    install_host(root,cat_folder,locked=False)
    directory_url = os.environ.get('CAT_MOUSE_DIRECTORY_URL','').strip().rstrip('/')
    if directory_url:
        url = urlsplit(directory_url)
        if url.scheme != 'https' or not url.hostname or url.username or url.password or url.query or url.fragment:
            raise ValueError('CAT_MOUSE_DIRECTORY_URL must be an HTTPS service URL')
        config = root/cat_folder/'directory-config.mjs'
        current = config.read_text(encoding='utf8')
        config_text, count = re.subn(r'^export const CAT_DIRECTORY_URL = .+;$',
                                     lambda _: 'export const CAT_DIRECTORY_URL = '+json.dumps(directory_url)+';',
                                     current, flags=re.MULTILINE)
        if count != 1:
            raise ValueError('Review the Cat & Mouse directory configuration')
        config.write_text(config_text,encoding='utf8',newline='\n')

def trash_edition(root):
    for name in ('launch.mjs','tunnel.mjs','quick-tunnel.yml'):
        copy(magic/trash_folder/'hosting'/name,root/trash_folder/'hosting'/name)
    install_host(root,trash_folder,locked=True)

def acorn_rally(root):
    source = truth/'games/acorn-rally'
    copy_tree(source,root,skip={'assets/Acorn-Rally-PC.zip','assets/pc-expansion.bin','pc-download.js'})
    copy_tree(source/'server',root/'server')
    bundle_host_runtime(root,'server/runtime')
    for required in ('server/host.cjs','server/community-host.cjs','server/community-directory.cjs',
                     'server/community-tunnel.cjs','server/community-poll.cjs','server/race-queue.cjs','race-sim.js','network.js'):
        if not (root/required).is_file():
            raise ValueError('Missing Acorn Rally hosting resource '+required)
    for relative in ('index.html','network.js'):
        file = root/relative
        text = file.read_text(encoding='utf8')
        text = re.sub(r'\s*<script src="pc-download\.js(?:\?[^"]*)?"></script>', '', text)
        text = text.replace('href="?download=pc"', 'href="https://sean-pereksta.github.io/Truth/games/acorn-rally/?download=pc"')
        file.write_text(text, encoding='utf8', newline='\n')

def bannerlands(root):
    binaries = host_runtimes()
    subprocess.run([str(binaries['node.exe']),str(truth/'games/bannerlands/source/build-host.mjs'),
                    '--hub','--output',str(root),'--cloudflared',str(binaries['cloudflared.exe'])],check=True)

def last_signal(root):
    source = truth/'games'/'last-signal'
    required = {'Last Signal.exe', 'Assets/models.bin', 'Hosting/cloudflared.exe'}
    if args.last_signal_build:
        native = args.last_signal_build.resolve()
        if not all((native/file).is_file() for file in required | {'Mobile/index.html'}):
            raise ValueError('Last Signal build is missing runtime files')
        shutil.copyfile(native/'Last Signal.exe', root/'Last Signal.exe')
        for directory in ('Assets', 'Hosting', 'Mobile'):
            shutil.copytree(native/directory, root/directory, dirs_exist_ok=True)
        bundle_host_runtime(root, 'Hosting')
        return

    def extract(archive):
        with ZipFile(archive) as z:
            names = set(z.namelist())
            if not required.issubset(names):
                raise ValueError('Last Signal archive is missing released game files')
            for entry in z.infolist():
                if entry.is_dir() or not (entry.filename=='Last Signal.exe' or entry.filename.startswith(('Assets/', 'Hosting/'))):
                    continue
                target=(root/entry.filename).resolve()
                if not target.is_relative_to(root.resolve()):
                    raise ValueError('Invalid Last Signal archive member')
                target.parent.mkdir(parents=True, exist_ok=True)
                with z.open(entry) as reader, target.open('wb') as dest:
                    shutil.copyfileobj(reader, dest)
    archive = source/'Last-Signal-Windows-x64.zip'
    if archive.is_file():
        extract(archive)
        return
    parts = source/'release-parts'
    meta = json.loads((parts/'manifest.json').read_text(encoding='utf8'))
    if meta['filename'] != archive.name or not meta['parts']:
        raise ValueError('Invalid Last Signal release manifest')
    with tempfile.TemporaryFile() as joined:
        total = hashlib.sha256()
        for part in meta['parts']:
            if not re.fullmatch(r'part-[0-9]+\.bin',part['file']):
                raise ValueError('Invalid Last Signal part path')
            path = parts/part['file']
            digest = hashlib.sha256()
            with path.open('rb') as stream:
                while data := stream.read(1024*1024):
                    joined.write(data)
                    total.update(data)
                    digest.update(data)
            if path.stat().st_size != part['size'] or digest.hexdigest() != part['sha256'].lower():
                raise ValueError('Last Signal release part failed its integrity check')
        if joined.tell()!=meta['size'] or total.hexdigest()!=meta['sha256'].lower():
            raise ValueError('Last Signal release archive failed its integrity check')
        joined.seek(0)
        extract(joined)

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

build('cat',magic,'game/catandmouse/edition/index.html',folders=[cat_folder],edition=cat_edition,
      hosting=('game/catandmouse/edition/server/launcher.mjs','hub-runtime'), adapter=cat_folder/'server/managed.mjs')
build('trashtactics',magic,'game/trash-tactics/index.html',folders=[trash_folder],edition=trash_edition,
      hosting=('game/trash-tactics/hosting/launch.mjs','hub-runtime'), adapter=trash_folder/'hosting/manager-adapter.mjs')
build('bannerlands',truth,'game/index.html',edition=bannerlands,
      hosting=('game/source/desktop/launcher.mjs','runtime'), adapter='games/bannerlands/source/hosting/manager-adapter.cjs')
# Historical Truth checkouts predate Server Manager and still build their original packs.
if (truth/'games/acorn-rally/server/community-host.cjs').is_file() and 'acornrally' in records:
    build('acornrally',truth,'index.html',edition=acorn_rally,
          hosting=('server/lobby-supervisor.cjs','server/runtime'))
# The archived executable predates the headless host. Only the fresh native
# build can advertise this edition in a release catalog.
build('lastsignal',truth,'Last Signal.exe',edition=last_signal,
      hosting=('manager/supervisor.cjs','Hosting') if args.last_signal_build else None,
      adapter='games/last-signal/manager/adapter.mjs' if args.last_signal_build else None)
if not args.only or 'lastsignal' in args.only:
    manifest['lastsignal']['type']='native-windows'
build('hearthmouse',magic,'index.html',edition=hearthmouse)
build('ironfield',magic,'game/ironfield/IRONFIELD.html',['game/ironfield/IRONFIELD.html'])
for key,slug in [('pirates','pirates'),('monstermallexpanded','monster-mall-expanded'),
    ('pacmanblackout','pacman'),('academystandards','academy-standards'),('operationwhiskerfall','operation-whiskerfall')]:
    build(key,truth,'index.html',edition=lambda p:copy_tree(truth/'games'/slug,p))
for key in ['apestogetherstrong','balatlesolo','cardrealms','deepsea','microwars4','mountain','starcraft2','swarm']:
    entry = records[key]['route'].lstrip('/')
    shared_folders = {'balatlesolo': ['game/balatle'], 'cardrealms': ['game/cardrealms']}
    build(key,magic,entry,[entry],shared_folders.get(key, []))
for key, folder, extras in [('bramblebound','bramblebound',[]),('flag','flag-capture',[]),
       ('matchwars','matchwars',[]),('circuitbound','circuitbound',[]),
       ('spelltower','spelltower',[]),
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
    # Read current edits where both case-distinct entries can coexist. Windows
    # checkouts collapse CatLanes.html/catlanes.html; git preserves the exact case.
    names = {p.name for p in (magic/'game').iterdir()}
    if {'CatLanes.html', 'catlanes.html'} <= names:
        data = (magic/'game/CatLanes.html').read_bytes()
    else:
        data = subprocess.check_output(['git','-C',str(magic),'show','HEAD:game/CatLanes.html'])
    data = re.sub(rb'<script type="module" src="/(?:Truth/)?hub/play-signal\.mjs"[^>]*></script>', b'', data)
    (root/'index.html').write_bytes(data.replace(b'\r\n', b'\n'))
    copy_tree(magic/'game/moba-mania', root/'moba-mania', skip={'README.md', 'CONTRACT.md'})
    copy(magic/'shared/managed-servers.js', root/'moba-mania/managed-servers.js')
    if args.cloudflared:
        copy_tree(magic/'game/moba-mania/server', root/'moba-mania/server')
        install_host(root, Path('moba-mania'), locked=False)

build('mobamania', magic, 'index.html', edition=catlanes,
      hosting=('moba-mania/server/host.mjs', 'hub-runtime') if args.cloudflared else None,
      adapter='game/moba-mania/server/manager-adapter.mjs' if args.cloudflared else None)
def orbit_bloom(root):
    source = truth/'games/orbit-bloom'
    copy_tree(source, root/'game')
    if args.cloudflared:
        copy(source/'server/host.cjs', root/'game/server/host.cjs')
        bundle_host_runtime(root)

build('orbitbloom', truth, 'game/index.html', edition=orbit_bloom,
      hosting=('game/server/host.cjs', 'hub-runtime') if args.cloudflared else None,
      adapter='games/orbit-bloom/server/manager-adapter.cjs' if args.cloudflared else None)
(out/'pack-index.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf8')
print(f'{len(manifest)} offline packs, {sum(g["bytes"] for g in manifest.values()):,} bytes')
