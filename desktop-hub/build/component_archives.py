"""Create deterministic independent release packages from the selected website revisions."""
from pathlib import Path
import hashlib, json, sys, zipfile
root=Path(__file__).resolve().parents[1]
out=Path(sys.argv[1]).resolve()
out.mkdir(parents=True,exist_ok=True)
catalog=json.loads((root/'content/catalog.json').read_text(encoding='utf8'))
components=[]
def files(folder):
    return sorted(p for p in folder.rglob('*') if p.is_file())
def pack(key, base, paths, descriptor=None, extra=()):
    target=out/(key+'.zip')
    with zipfile.ZipFile(target,'w',allowZip64=True) as archive:
        if descriptor:
            info=zipfile.ZipInfo('hub-pack.json',date_time=(2000,1,1,0,0,0))
            archive.writestr(info,json.dumps(descriptor,sort_keys=True))
        entries=[(p.relative_to(base).as_posix(), p) for p in paths]
        for name, p in sorted(entries + list(extra)):
            info=zipfile.ZipInfo(name,date_time=(2000,1,1,0,0,0))
            info.compress_type=zipfile.ZIP_DEFLATED
            info.external_attr=0o644<<16
            with p.open('rb') as src, archive.open(info,'w') as dest:
                import shutil
                shutil.copyfileobj(src,dest)
    digest=hashlib.sha256()
    with target.open('rb') as f:
        for chunk in iter(lambda:f.read(1<<20),b''):digest.update(chunk)
    components.append({'id':key,'sha256':digest.hexdigest(),'size':target.stat().st_size})
pack('hub',root, files(root/'desktop')+files(root/'ui')+[root/'services/read-model.mjs',root/'services/web/activity.mjs'],
     extra=[('desktop/managed-servers.js',root.parent/'shared/managed-servers.js')])
content=root/'content'
pack('catalog',content,[content/'catalog.json']+files(content/'art'))
for game in catalog['games']:
    if game['offline']:
        base=content/'packs'/game['id']
        if not (base/game['offline']['entry'].split('?')[0]).is_file(): raise ValueError(game['id'])
        # The catalog records both source commits. Keep the installable archive
        # stable when an unrelated game changes in the same repository.
        descriptor = {**game['offline'], 'version': game['offline']['digest']}
        pack(game['id'],base,files(base),descriptor)
(out/'components.json').write_text(json.dumps(components),encoding='utf8')
