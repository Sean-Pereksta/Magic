"""Extract and pack generated complete bodies, retaining their source alpha.

The authored row map is explicit because image generation can vary layout.
No armor layers, paint, recolors, or runtime per-unit textures are produced.
"""
from pathlib import Path
from collections import Counter
import hashlib,json,sys
import numpy as np
from PIL import Image
from scipy import ndimage
ROOT=Path(__file__).resolve().parents[1]/'assets/visual/armor-sprites'
SPECIES=['gorilla','chimpanzee','orangutan','gibbon','mandrill','capuchin']
TRIBES=['ironback','shadowhand','emberwood']
NAMES=[f'{s}-{v}' for s in SPECIES for v in ['base','tier1','tier2','tier3']]+[f'{s}-{v}' for s in TRIBES for v in ['base','age1','age2','command','power','shadow']]
CONFIG={n:{'rows':8,'motionRows':[0,0,1,2,3],'actionRows':[4,4,5,6,7]} for n in NAMES}
for s in ['gorilla','chimpanzee']:
 for v in ['base','tier1','tier2','tier3']:CONFIG[f'{s}-{v}']={'rows':10,'motionRows':[0,1,2,3,4],'actionRows':[5,6,7,8,9]}
for n in ['gorilla-tier2','chimpanzee-tier1']:CONFIG[n]={'rows':9,'motionRows':[0,1,2,3,3],'actionRows':[4,5,6,7,8]}
CONFIG['chimpanzee-tier2']={'rows':10,'motionRows':[0,1,2,3,9],'actionRows':[4,5,6,7,8]}
for n in ['gibbon-tier2','mandrill-tier1','mandrill-tier2']:
 CONFIG[n]={'rows':7,'motionRows':[0,0,1,2,2] if n.startswith('mandrill') else [0,0,1,2,3],'actionRows':[3,3,4,5,6] if n.startswith('mandrill') else [4,4,5,6,6]}
for n in ['gibbon-base','gibbon-tier3','capuchin-base','capuchin-tier1']:
 CONFIG[n]['actionRows']=[4,4,5,6,6]
 if n!='gibbon-base':CONFIG[n]['overrides']={f'm4:{c}':[2,c] for c in range(3,8)}
CONFIG['capuchin-tier3']['overrides']={**{f'a4:{c}':[6,c] for c in range(3)},'m4:7':[2,7]}
CONFIG['capuchin-tier3']['overrides']['a4:7']=[6,7]
CONFIG['gibbon-base']['overrides']={'m4:7':[2,7]}
CONFIG['ironback-base']['motionRows']=[0,1,2,3,3]
CONFIG['ironback-base']['rowEdges']=[0,176,340,511,685,839,991,1143,1254]
for n in ['ironback-age2','shadowhand-power']:
 CONFIG[n]={'rows':7,'motionRows':[0,0,1,2,3],'actionRows':[4,4,5,6,6]}
CONFIG['shadowhand-power']['overrides']={'m4:7':[2,7]}
manifest={'version':3,'generator':'built-in image_gen','sourcePrompts':'armor-sprites/prompts.json','atlases':[],'characterFrames':{},'sheets':{}}
report=[]
for name in NAMES:
 source=ROOT/'source'/f'{name}.png'
 if not source.exists():continue
 config=CONFIG[name];rows=config['rows'];src=Image.open(source).convert('RGBA');sw,sh=src.size;alpha=np.array(src.getchannel('A'));density=(alpha>24).sum(axis=1)
 # Find row gutters without changing illustration pixels.
 edges=[0]
 for k in range(1,rows):
  expected=round(k*sh/rows);span=int(sh/rows*.19);lo=max(edges[-1]+10,expected-span);hi=min(sh-1,expected+span);edges.append(lo+int(np.argmin(density[lo:hi])))
 edges.append(sh)
 if 'rowEdges' in config:edges=config['rowEdges']
 cw,ch=128,160;canvas=Image.new('RGBA',(cw*8,ch*rows));frames={};sheet='armor-'+name
 whole_labels,_=ndimage.label(alpha>24);whole_slices=ndimage.find_objects(whole_labels);bodies=[];owners=Counter()
 for yt,yb in zip(edges,edges[1:]):
  for col in range(8):
   counts=np.bincount(whole_labels[yt:yb,round(col*sw/8):round((col+1)*sw/8)].ravel());counts[0]=0
   if counts.max()>100:owners[int(np.argmax(counts))]+=1
 action_rows=set(config['actionRows'])
 for row in range(rows):
  for col in range(8):
   xl,xr=round(col*sw/8),round((col+1)*sw/8);yt,yb=edges[row:row+2];a=alpha[yt:yb,xl:xr];labels,count=ndimage.label(a>24);sizes=np.bincount(labels.ravel());sizes[0]=0;ys,xs=np.nonzero(labels==int(np.argmax(sizes))) if count else ([],[])
   if not len(xs):
    print(f'Unused empty source cell {name}:{row}:{col}',flush=True)
    continue
   left,top,right,bottom=int(xs.min()),int(ys.min()),int(xs.max()+1),int(ys.max()+1)
   # A complete isolated body may cross a nominal grid boundary (especially
   # a punching fist). Extract it from the whole source before packing. Joined
   # adjacent bodies still use the authored row/cell boundary as a separator.
   candidates=np.bincount(whole_labels[yt:yb,xl:xr].ravel());candidates[0]=0;label=int(np.argmax(candidates));sy,sx=whole_slices[label-1]
   if owners[label]==1 and sx.stop-sx.start<sw/8*1.5 and sy.stop-sy.start<max(np.diff(edges))*1.5:
    xl,yt=sx.start,sy.start;a=alpha[sy,sx];labels=whole_labels[sy,sx];main=labels==label;ys,xs=np.nonzero(main);left=top=0;right=sx.stop-sx.start;bottom=sy.stop-sy.start
   else:main=labels==int(np.argmax(sizes))
   crop=src.crop((xl+left,yt+top,xl+right,yt+bottom))
   # A neighboring fist/crown can enter a cell while remaining disconnected
   # from this body. Extract the selected body and its antialiased edge, rather
   # than copying a foreign sprite fragment inside its rectangular bounds.
   halo=ndimage.binary_dilation(main,iterations=2)[top:bottom,left:right]
   crop.putalpha(Image.fromarray(np.where(halo,a[top:bottom,left:right],0).astype('uint8')))
   bodies.append((row,col,crop,xs,ys,left,top,bottom))
 # One scale for the entire sheet keeps the body size stable during animation.
 scale=min((cw-8)/(sw/8),(ch-8)/max(np.diff(edges)),(cw-8)/max(b[2].width for b in bodies),(ch-8)/max(b[2].height for b in bodies))
 for row,col,crop,xs,ys,left,top,bottom in bodies:
   fw,fh=max(1,round(crop.width*scale)),max(1,round(crop.height*scale));crop=crop.resize((fw,fh),Image.Resampling.LANCZOS)
   # Uniform source scale preserves limb motion. Feet anchor to the same plane.
   x=col*cw+(cw-fw)//2;y=row*ch+ch-4-fh;canvas.alpha_composite(crop,(x,y));support=xs[ys>=ys.max()-max(2,int((bottom-top)*.025))];ax=float(np.median(support)-left)*scale;ay=fh-1
   action=row in action_rows
   hands=([(0.25,.38),(.78,.35)],[(.35,.55),(.91,.4)],[(.40,.89),(.68,.9)],[(.32,.09),(.75,.46)],[(.29,.44),(.75,.08)],[(.27,.08),(.76,.53)],[(.25,.35),(.73,.31)],[(.29,.6),(.72,.48)])[col] if action else ([(.2,.86),(.8,.86)],[(.30,.85),(.78,.73)],[(.24,.71),(.80,.89)],[(.24,.43),(.78,.68)],[(.30,.74),(.79,.43)],[(.40,.60),(.68,.60)],[(.41,.59),(.70,.6)],[(.24,.49),(.72,.15)])[col]
   frames[f'{row}:{col}']={'x':x,'y':y,'w':fw,'h':fh,'anchorX':round(ax,2),'anchorY':ay,'opaquePixels':int((np.array(crop.getchannel('A'))>24).sum()),'hands':{key:{'x':round(fw*u,2),'y':round(fh*v,2)} for key,(u,v) in zip(['left','right'],hands)},'body':{'x':fw*.5,'y':fh*.52,'width':fw*.55,'height':fh*.4},'groundContacts':[{'x':round(ax,2),'width':round(max(5,fw*.28),2)}]}
 out=ROOT/f'{name}.webp'
 only=sys.argv[sys.argv.index('--only')+1] if '--only' in sys.argv else None
 if '--metadata-only' not in sys.argv and (not only or name==only):canvas.save(out,'WEBP',quality=86,method=6,exact=True)
 manifest['atlases'].append({'id':'characters-'+sheet,'file':'armor-sprites/'+out.name,'source':'armor-sprites/source/'+source.name,'width':canvas.width,'height':canvas.height,'columns':8,'rows':rows,'frameCount':len(frames),'sha256':hashlib.sha256(out.read_bytes()).hexdigest(),'drawScale':1})
 manifest['characterFrames'][sheet]=frames;manifest['sheets'][sheet]={k:v for k,v in config.items() if k!='rows'}
 report.append({'name':name,'rows':rows,'rowEdges':edges,'bytes':out.stat().st_size})
(ROOT/'manifest.json').write_text(json.dumps(manifest,separators=(',',':'))+'\n',encoding='utf-8')
(ROOT/'packing-report.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
print(f'Packed {len(report)}/42 full-body sheets; {sum(r["bytes"] for r in report)/1024/1024:.2f} MiB')
if len(report)!=42:raise SystemExit('Missing source sheets; generation must finish before release.')
