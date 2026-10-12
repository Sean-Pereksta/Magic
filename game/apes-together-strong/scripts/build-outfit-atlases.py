"""Package original generated artwork losslessly; alpha-measured crops and anchors.
No painting, recoloring, or procedural replacement art. Sources remain in source/.
"""
from pathlib import Path
import json, hashlib, sys
from PIL import Image
import numpy as np
from scipy import ndimage
ROOT=Path(__file__).resolve().parents[1]/'assets/visual/outfits'
SPECIES=['gorilla','chimpanzee','orangutan','gibbon','mandrill','capuchin']
CONFIG={'royal-rear':(8,2),'ironback':(8,9),'shadowhand':(8,9),'emberwood':(8,10),'follower-armor':(8,6),'king-armor':(10,3),'workers-front':(8,6),'workers-side':(8,6),'workers-rear':(8,6),'tribal-structures':(6,3),'shields-tools':(6,4)}
manifest={'version':2,'generator':'built-in image_gen','sourcePrompts':'outfits/prompts.json','atlases':[],'characterFrames':{},'frames':{},'heldFrames':{}}
for name,(cols,rows) in CONFIG.items():
 source=ROOT/'source'/f'{name}.png'
 if not source.exists():continue
 image=Image.open(source).convert('RGBA');w,h=image.size;alpha=np.asarray(image.getchannel('A'));character=name in ['royal-rear','ironback','shadowhand','emberwood'] or name.startswith('workers-');ident=('characters-' if character else 'outfits-')+name
 # Generators leave margins and can vary row spacing. Measure the lowest-alpha
 # separator near each expected gutter, then retain every pixel in that cell.
 density=(alpha>24).sum(axis=1)
 if name=='ironback':expected=[0,150,300,452,604,752,906,1057,1213,h]
 elif name.startswith('workers-'):expected=[0,233,418,618,807,974,h]
 elif name=='follower-armor':expected=[0,230,436,652,879,1104,h]
 elif name=='shadowhand':expected=[0,156,310,468,623,779,934,1091,1245,h]
 else:expected=[round(i*h/rows) for i in range(rows+1)]
 edges=[0]
 for y in expected[1:-1]:
  span=max(4,int(h/rows*.14));lo,hi=max(edges[-1]+10,y-span),min(h-1,y+span);edges.append(lo+int(np.argmin(density[lo:hi])))
 edges.append(h)
 output=ROOT/f'{name}.webp'
 if '--metadata-only' not in sys.argv:image.save(output,'WEBP',lossless=True,method=6)
 manifest['atlases'].append({'id':ident,'file':f'outfits/{output.name}','width':w,'height':h,'columns':cols,'rows':rows,'frameCount':cols*rows,'sha256':hashlib.sha256(output.read_bytes()).hexdigest(),'source':f'outfits/source/{name}.png','rowEdges':edges,'drawScale':.75 if name=='royal-rear' else 1})
 table={}
 for row in range(rows):
  yd,yu=edges[row],edges[row+1]
  for col in range(cols):
   xl,xr=round(col*w/cols),round((col+1)*w/cols)
   crop=alpha[yd:yu,xl:xr];labels,total=ndimage.label(crop>24);sizes=np.bincount(labels.ravel());sizes[0]=0;main=int(np.argmax(sizes));ys,xs=np.nonzero(labels==main)
   if not len(xs):raise ValueError(f'Empty {name} {row}:{col}')
   x,y=xl+int(xs.min()),yd+int(ys.min());fw,fh=int(xs.max()-xs.min()+1),int(ys.max()-ys.min()+1)
   # Contact-plane anchor is the average of the lowest opaque support pixels.
   bottom=int(ys.max());support=xs[ys>=bottom-max(2,int(fh*.025))];anchorX=float(np.median(support))-float(xs.min());anchorY=float(fh-1)
   frame={'x':x,'y':y,'w':fw,'h':fh,'anchorX':round(anchorX,2),'anchorY':anchorY,'opaquePixels':int(len(xs))}
   if character:
    king=not name.startswith('workers-');action=king and (row>=5 or name=='royal-rear')
    if not king:
     hands=[[(.38,.12),(.72,.12)],[(.67,.76),(.83,.8)],[(.56,.84),(.82,.86)],[(.47,.36),(.82,.34)],[(.31,.61),(.77,.17)],[(.49,.79),(.83,.9)],[(.52,.62),(.8,.62)],[(.42,.59),(.8,.56)]][col]
     body=[(.48,.52),(.43,.44),(.44,.39),(.48,.55),(.48,.52),(.38,.38),(.44,.42),(.37,.4)][col]
     # Raised palms differ between the independently authored front/side/rear
     # sheets. These landmarks were checked against the actual source poses.
     if col==0:
      raised={'workers-front':[(.65,.045),(.67,.045),(.58,.025),(.66,.025),(.65,.04),(.77,.12)],'workers-side':[(.73,.055),(.62,.055),(.60,.035),(.60,.04),(.57,.04),(.80,.12)],'workers-rear':[(.69,.035),(.67,.04),(.70,.035),(.69,.04),(.66,.04),(.84,.11)]}[name][row]
      hands=[(raised[0]-.045,raised[1]+.025),(raised[0]+.025,raised[1])]
     elif col==3:
      hands=[(.76,.13),(.91,.055)] if name!='workers-rear' else [(.66,.10),(.9,.055)]
     elif col==4:
      raised={'workers-front':[(.39,.06),(.47,.06),(.42,.07),(.47,.06),(.44,.07),(.84,.09)],'workers-side':[(.86,.055),(.78,.055),(.84,.06),(.84,.04),(.77,.065),(.87,.07)],'workers-rear':[(.78,.055),(.74,.06),(.79,.07),(.75,.06),(.72,.065),(.91,.11)]}[name][row]
      hands=[(.88,.79) if name=='workers-front' else (.65,.85),raised]
     elif col==6:hands=[(.77,.57),(.90,.53)]
     elif col==7:hands=[(.48,.40),(.91,.62)]
    elif action:
     hands=[[(.31,.57),(.78,.39)],[(.45,.88),(.79,.89)],[(.34,.25),(.78,.07)],[(.3,.06),(.7,.38)],[(.26,.1),(.8,.46)],[(.24,.09),(.77,.09)],[(.3,.35),(.7,.28)],[(.3,.6),(.68,.5)]][col]
     body=[(.53,.49),(.48,.46),(.49,.47),(.49,.45),(.5,.55),(.5,.52),(.5,.55),(.5,.52)][col]
    else:
     hands=[[(.2,.88),(.8,.88)],[(.36,.87),(.8,.73)],[(.23,.7),(.78,.9)],[(.24,.42),(.77,.7)],[(.29,.76),(.8,.43)],[(.43,.62),(.72,.62)],[(.26,.5),(.7,.17)],[(.41,.83),(.77,.85)]][col]
     body=(.5,.52)
    frame['hands']={key:{'x':round(fw*xy[0],2),'y':round(fh*xy[1],2)} for key,xy in zip(['left','right'],hands)}
    frame['body']={'x':round(fw*body[0],2),'y':round(fh*body[1],2),'width':round(fw*(.57 if king else .53),2),'height':round(fh*(.39 if king else .38),2)}
    frame['groundContacts']=[{'x':round(anchorX,2),'width':round(max(5,fw*.28),2)}]
   table[f'{row}:{col}']=frame
 if character:manifest['characterFrames'][name]=table
 else:manifest['frames'][name]=table
 if name=='shields-tools':
  for row,kind in enumerate(['buckler','ironShield','ancientShield','tools']):
   for col in range(cols):
    key=(kind+str(col)) if kind!='tools' else ['axe','saw','trowel','ladle','armguard','headguard'][col]
    f=table[f'{row}:{col}'];manifest['heldFrames'][key]={**f,'atlas':ident,'gripX':f['w']*.5,'gripY':f['h']*.6,'drawWidth':32 if row==0 else 45 if row<3 else 24,'drawHeight':36 if row==0 else 49 if row<3 else 30}
(ROOT/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
print('Packaged',len(manifest['atlases']),'atlases;',sum(len(v) for v in manifest['characterFrames'].values()),'body frames')
