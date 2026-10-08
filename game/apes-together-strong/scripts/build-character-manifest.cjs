/* Detect complete painted cutouts instead of clipping them to assumed cells.
 * This only writes metadata. All source PNG pixels remain unchanged. */
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const {png}=require('./inspect-character-atlases.cjs');
const root=path.join(__dirname,'../assets/visual/characters');
const specs=[{name:'king',cols:6,rows:5},{name:'apes',cols:6,rows:6},{name:'humans',cols:8,rows:4},{name:'actions',cols:6,rows:6},{name:'humanactions',cols:4,rows:4},{name:'kingactions',cols:3,rows:2},{name:'humanwalk',cols:4,rows:4}];
const manifest={version:1,style:'Moonlit hand-painted isometric wildlife',projection:{x:.8,y:.42},light:'upper left',generator:'OpenAI built-in imagegen',sourcePrompts:'characters/prompts.json',atlases:[],species:['gorilla','chimpanzee','orangutan','gibbon','mandrill','capuchin'],kingDirections:['south','southeast','east','northeast','north'],humanClasses:['patrol','rifle','heavy','scout'],frames:{},animation:{gait:'Alternating authored limb poses with contact passing poses',kingColumns:['idle','stepLeft','stepRight','anticipation','impact','recover'],apeColumns:['idle','stepLeft','stepRight','rearStepLeft','rearStepRight','strike'],humanColumns:['idle','stepLeft','stepRight','aim','recoil','rearStepLeft','rearStepRight','fallen'],events:['footLeft','footRight','anticipation','impact','recover','muzzle'],notes:'Eight King facings use five authored views plus mirrored counterparts. Crowds use four views (front/rear and mirrored). Rendering events are visual only; the simulation remains authoritative.'}};
for(const spec of specs){const file=path.join(root,spec.name+'.png'),im=png(file),seen=new Uint8Array(im.w*im.h),queue=new Int32Array(im.w*im.h),frames={},columns=Array.from({length:spec.cols},()=>[]);let transparent=0;for(let i=0;i<seen.length;i++)if(im.data[i*4+3]<20)transparent++;
 for(let i=0;i<seen.length;i++){if(seen[i]||im.data[i*4+3]<70)continue;let read=0,end=0,count=0,x1=im.w,y1=im.h,x2=0,y2=0;queue[end++]=i;seen[i]=1;while(read<end){const v=queue[read++],x=v%im.w,y=Math.floor(v/im.w);count++;x1=Math.min(x1,x);x2=Math.max(x2,x);y1=Math.min(y1,y);y2=Math.max(y2,y);for(const z of [x?v-1:-1,x<im.w-1?v+1:-1,y?v-im.w:-1,y<im.h-1?v+im.w:-1])if(z>=0&&!seen[z]&&im.data[z*4+3]>=70){seen[z]=1;queue[end++]=z}}
  if(count<700)continue;const col=Math.min(spec.cols-1,Math.floor((x1+x2)/2/im.w*spec.cols));const x=Math.max(0,x1-1),y=Math.max(0,y1-1),w=Math.min(im.w-1,x2+1)-x+1,h=Math.min(im.h-1,y2+1)-y+1;
  // Extract the lowermost opaque support spans, not the center of the tile.
  // Some gait frames support the body on only one knuckle and the opposite foot.
  const contacts=[];let span=-1,last=-1;
  for(let xx=x1;xx<=x2+4;xx++){let solid=false;if(xx<=x2)for(let yy=Math.max(y1,y2-5);yy<=y2;yy++)if(im.data[(yy*im.w+xx)*4+3]>100){solid=true;break}
    if(solid){if(span<0)span=xx;last=xx}else if(span>=0&&xx-last>3){if(last-span>=2)contacts.push({x:(span+last)/2-x,width:last-span+1});span=-1}}
  columns[col].push({x,y,w,h,anchorX:im.w/spec.cols*(col+.5)-x,anchorY:h-1,groundContacts:contacts,opaquePixels:count});
 }
 for(let col=0;col<spec.cols;col++){if(columns[col].length!==spec.rows)throw Error(spec.name+' column '+col+' expected '+spec.rows+' separate figures, got '+columns[col].length);columns[col].sort((a,b)=>a.y+a.h/2-b.y-b.h/2).forEach((f,row)=>frames[row+':'+col]=f)}
 if(Object.keys(frames).length!==spec.cols*spec.rows)throw Error('Missing frames '+spec.name);
 manifest.atlases.push({id:'characters-'+spec.name,file:'characters/'+spec.name+'.png',width:im.w,height:im.h,columns:spec.cols,rows:spec.rows,frameCount:spec.cols*spec.rows,transparentFraction:Number((transparent/(im.w*im.h)).toFixed(4)),sha256:crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex')});manifest.frames[spec.name]=frames;
}
manifest.supplementalPrompts='characters/supplemental-prompts.json';
manifest.animation.actionColumns=['anticipation','downwardImpact','sweep','climbLeft','climbRight','fallen'];
manifest.animation.humanActionColumns=['rearAim','rearRecoil','radio','throw'];
manifest.animation.kingActionRows=[['climbLeft','climbRight','command'],['collapse','falling','fallen']];
manifest.animation.humanWalkColumns=['frontLeft','frontRight','rearLeft','rearRight'];
manifest.walkPrompts='characters/walk-prompts.json';
manifest.animation.timing={sampleRates:{low:8,medium:12,high:18,ultra:24},gaitCyclesPerSecond:{gorilla:1.45,chimpanzee:2.05,orangutan:1.25,gibbon:1.75,mandrill:1.95,capuchin:2.45,human:1.9},urgentMultiplier:1.35,contactPhases:{footLeft:0,footRight:.5},melee:'The simulation hits at animation.start: contact artwork displays first, then follow-through and recovery. Existing animation.duration is authoritative.',throwReleaseNormalized:.525,collapseSeconds:.65,idle:'Breathing scales about the measured foot origin; no whole-body translation.'};
manifest.grounding='Every cutout rests on its measured opaque bottom. Contact shadows use groundContacts from the bottom six alpha rows. Idle breathing scales around this fixed origin; gait plays articulated frames without whole-body lift.';
fs.writeFileSync(path.join(root,'manifest.json'),JSON.stringify(manifest,null,2)+'\n');
console.log('Character metadata: '+manifest.atlases.map(a=>a.id+' '+a.frameCount+' frames').join(', '));
