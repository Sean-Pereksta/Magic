"""Lossless runtime packaging; retain original PNG masters and verify visible pixels."""
from PIL import Image
from pathlib import Path
import numpy as np,json,hashlib,re
root=Path(__file__).resolve().parents[1]/'assets/visual';file=root/'environment/manifest.json';original=file.read_text();data=json.loads(original);saved=0
for atlas in data['atlases']:
 source=atlas.get('source',atlas['file']);p=root/source
 if p.suffix!='.png':continue
 image=Image.open(p).convert('RGBA');target=p.with_suffix('.webp');image.save(target,'WEBP',lossless=True,method=6)
 a=np.array(image);b=np.array(Image.open(target).convert('RGBA'));assert np.array_equal(a[:,:,3],b[:,:,3]);assert np.array_equal(a[a[:,:,3]>0],b[a[:,:,3]>0])
 atlas['source']=source;atlas['file']=str(target.relative_to(root));atlas['sha256']=hashlib.sha256(target.read_bytes()).hexdigest();saved+=p.stat().st_size-target.stat().st_size
block='  "atlases": [\n'+',\n'.join('    '+json.dumps(v,separators=(', ',': ')) for v in data['atlases'])+'\n  ],';start=original.index('  "atlases": [');end=original.index('\n  ],',start)+5;file.write_text(original[:start]+block+original[end:]);print('Lossless environment savings:',saved,'bytes')
