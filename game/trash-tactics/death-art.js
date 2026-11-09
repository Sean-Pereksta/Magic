/* Eleven authored knockout animations. Painted bodies separate into real pieces,
 * then settle into the matching generated remains tile. No simulation entities. */
export const DEATH_TYPES=Object.freeze(['twig','rock','paper','scissors','cd','sniper','airplane','mage','divider','rollingrock','shears']);
export const DEATH_FRAMES=10,DEATH_FRAME_SIZE=160,DEATH_CACHE_LIMIT=11;
export const DEATH_DURATION=1.3;
const TAU=Math.PI*2,INK='#354549',PAPER='#fff5d9',TEAL='#6cafa5',VIOLET='#9b7dc3';
const BODY_SIZE={twig:57,rock:57,paper:57,scissors:57,cd:73,sniper:69,airplane:77,mage:70,divider:98,rollingrock:104,shears:77};
const REMAINS_BOUNDS=[[32,94,343,340],[387,107,696,339],[739,129,1051,348],[1093,101,1418,331],[31,429,349,669],[383,418,704,679],[735,439,1058,661],[1090,425,1426,686],[36,756,338,1013],[378,765,689,1016],[735,766,1067,1015]];
const CACHE=new Map();
let remains=null;
if(typeof Image!=='undefined'){
 remains=new Image();remains.onerror=()=>{if(!remains._pngFallback){remains._pngFallback=true;remains.src=new URL('./assets/death-remains.png',import.meta.url).href;}};
 remains.src=new URL('./assets/death-remains.webp',import.meta.url).href;
}
export const loadDeathArtwork=()=>remains?.decode?.().catch(()=>{})||Promise.resolve();
export const deathArtworkStatus=()=>({remains:!!(remains?.complete&&remains.naturalWidth),types:DEATH_TYPES.length,cachedSheets:CACHE.size,cacheBytes:CACHE.size*DEATH_FRAME_SIZE**2*DEATH_FRAMES*4,cacheLimit:DEATH_CACHE_LIMIT});
export const clearDeathSpriteCache=()=>CACHE.clear();
const clamp=(n,min=0,max=1)=>Math.max(min,Math.min(max,Number(n)||0));
const ease=n=>1-(1-clamp(n))**3;
function paint(c,fill,stroke=INK,width=1.5){if(fill){c.fillStyle=fill;c.fill();}if(stroke){c.strokeStyle=stroke;c.lineWidth=width;c.stroke();}}
function path(c,points,fill,stroke=INK,width=1.5){c.beginPath();points.forEach(([x,y],i)=>i?c.lineTo(x,y):c.moveTo(x,y));if(fill)c.closePath();paint(c,fill,stroke,width);}
function line(c,points,color=INK,width=2){path(c,points,null,color,width);}
function oval(c,x,y,rx,ry,fill,stroke=null,width=1.5){c.beginPath();c.ellipse(x,y,Math.max(.01,rx),Math.max(.01,ry),0,0,TAU);paint(c,fill,stroke,width);}
function rect(c,x,y,w,h,fill,stroke=INK,r=2){c.beginPath();c.roundRect(x,y,w,h,r);paint(c,fill,stroke);}
function star(c,x,y,r,fill){const pts=[];for(let i=0;i<10;i++){const a=i*Math.PI/5-Math.PI/2,rr=i%2?r*.42:r;pts.push([x+Math.cos(a)*rr,y+Math.sin(a)*rr]);}path(c,pts,fill,INK,1);}
function stone(c,x,y,r,angle,fill='#a7b7b8'){
 c.save();c.translate(x,y);c.rotate(angle);path(c,[[-r,-r*.4],[-r*.5,-r],[r*.45,-r*.85],[r,r*.2],[r*.35,r*.8],[-r*.65,r*.7]],fill);
 path(c,[[-r*.8,-r*.35],[-r*.4,-r*.8],[r*.4,-r*.65],[0,0]],'#d4dedd',null);line(c,[[0,0],[r*.4,r*.55]],'#7c9395',1);c.restore();
}
function paperScrap(c,x,y,w,h,angle,teal=false){c.save();c.translate(x,y);c.rotate(angle);path(c,[[-w/2,-h/2],[w/2,-h/2],[w/2-2,0],[w/2,h/2],[0,h/2-2],[-w/2,h/2]],teal?'#a0c5b6':PAPER);line(c,[[-w*.3,-h*.15],[w*.28,-h*.15]],teal?'#70a396':'#d2c7a5',1);c.restore();}
function blade(c,x,y,len,angle,dark=false){c.save();c.translate(x,y);c.rotate(angle);path(c,[[-4,3],[-3,-len],[0,-len-6],[6,-6],[4,5]],dark?'#8c8f9f':'#c7dce0');path(c,[[0,-len+3],[1,-7],[4,-5]],'#f7f8ec',null);oval(c,0,8,7,9,dark?'#9272ad':'#db8c55',INK,1.6);oval(c,0,8,3,4,PAPER,INK,1);c.restore();}
function leaf(c,x,y,a){c.save();c.translate(x,y);c.rotate(a);c.beginPath();c.moveTo(-8,0);c.quadraticCurveTo(1,-11,10,-3);c.quadraticCurveTo(5,7,-8,0);paint(c,'#91b86f');line(c,[[-7,0],[7,-2]],'#536e44',1);c.restore();}
function fallbackBody(c,type,size){
 c.save();c.scale(size/60,size/60);
 if(type==='twig'){line(c,[[0,19],[-1,-18],[-13,-30]],INK,9);line(c,[[0,19],[-1,-18],[-13,-30]],'#bb8b58',6);line(c,[[0,-9],[11,-25]],'#a87848',5);oval(c,0,2,11,15,'#c79b67',INK);leaf(c,13,-27,-.5);}
 else if(['rock','mage','rollingrock'].includes(type)){stone(c,0,-5,24,0);if(type==='mage'){path(c,[[-27,-24],[0,-53],[18,-25],[31,-20],[-28,-17]],'#5d6284');star(c,2,-28,5,'#e6b75f');line(c,[[24,16],[33,-27]],'#bc8d53',4);path(c,[[27,-28],[34,-40],[40,-28],[33,-19]],'#8de9dc');}else line(c,[[-21,-20],[20,-17]],'#e5a35b',5);}
 else if(['scissors','sniper','shears'].includes(type)){blade(c,-8,-1,28,-.3,type==='shears');blade(c,9,-1,28,.35,type==='shears');if(type==='sniper'){line(c,[[-25,6],[29,-8]],'#c39950',6);rect(c,-16,-29,33,8,'#8e9b6f');}if(type==='shears'){line(c,[[-10,4],[-29,-3],[-34,7]],'#8b6aac',6);}}
 else{path(c,[[-21,-29],[12,-29],[23,-15],[21,20],[-23,20]],PAPER);path(c,[[12,-29],[12,-15],[23,-15]],TEAL);if(type==='airplane')path(c,[[-31,7],[28,-27],[7,23],[-1,5]],PAPER);if(type==='divider'){path(c,[[-20,-29],[-22,-43],[-9,-37],[1,-48],[10,-36],[21,-43],[17,-28]],'#9c81b3');}}
 if(type!=='airplane'){line(c,[[-9,18],[-11,27],[-18,27]],INK,4);line(c,[[10,18],[13,27],[20,27]],INK,4);line(c,[[-8,-3],[-3,2]],INK,1.6);line(c,[[3,2],[8,-3]],INK,1.6);}
 c.restore();
}
const BOX=[[0,0],[1,0],[1,1],[0,1]];
function piece(c,type,sprite,size,mask=BOX,{x=0,y=0,a=0,sx=1,sy=1,px=.5,py=.5}={}){
 const pivotX=(px-.5)*size,pivotY=(py-.5)*size-9;
 c.save();c.translate(x+pivotX,y+pivotY);c.rotate(a);c.scale(sx,sy);c.translate(-pivotX,-pivotY);
 c.beginPath();mask.forEach(([u,v],i)=>{const xx=(u-.5)*size,yy=(v-.5)*size-9;i?c.lineTo(xx,yy):c.moveTo(xx,yy);});c.closePath();c.clip();
 if(sprite?.complete&&sprite.naturalWidth)c.drawImage(sprite,-size/2,-size/2-9,size,size);else{c.translate(0,-9);fallbackBody(c,type,size);}
 c.restore();
}
function dust(c,p,size=30){if(p<.22||p>.85)return;const q=clamp((p-.22)/.63);for(let i=0;i<4;i++)oval(c,(i-1.5)*(9+q*8),14-Math.sin(q*Math.PI)*(3+i%2*5),size*(.08+q*.05),size*(.035+q*.02),'#c0af8b50');}

/** Continuous authored artwork also serves as a no-OffscreenCanvas fallback. */
export function paintDeathPose(c,type,p,sprite=null){
 if(!DEATH_TYPES.includes(type))return false;
 p=clamp(p);const q=ease((p-.06)/.72),lift=Math.sin(clamp((p-.14)/.7)*Math.PI),size=BODY_SIZE[type],split=clamp((p-.12)/.65);
 c.save();c.lineCap='round';c.lineJoin='round';c.globalAlpha=1;
 oval(c,0,11,size*(.29+q*.11),size*.095,'#4b4a3624');
 c.globalAlpha=1-clamp((p-.53)/.20);
 if(type==='twig'){
  piece(c,type,sprite,size,[[0,0],[1,0],[1,.42],[0,.42]],{x:20*q,y:18*q,a:1.3*q,py:.42});
  piece(c,type,sprite,size,[[0,.42],[1,.42],[1,.76],[0,.76]],{x:-5*q,y:12*q,a:-.85*q,py:.76});
  piece(c,type,sprite,size,[[0,.76],[1,.76],[1,1],[0,1]],{x:5*q,y:5*q,a:.4*q,sy:1-q*.4});
  leaf(c,22+q*15,-28+q*47-lift*11,q*3);c.save();c.translate(-18-q*10,5+q*10-lift*12);c.rotate(-q*.8);rect(c,-7,-5,14,10,PAPER);line(c,[[-6,0],[6,0]],'#b58b56',1.4);line(c,[[0,-5],[0,5]],'#b58b56',1.4);c.restore();
 }else if(type==='rock'||type==='rollingrock'){
  const heavy=type==='rollingrock',spread=(heavy?20:14)*split;
  const masks=[[[0,0],[.52,0],[.4,.5],[0,.72]],[[.52,0],[1,0],[1,.67],[.4,.5]],[[0,.72],[.4,.5],[.55,1],[0,1]],[[.4,.5],[1,.67],[1,1],[.55,1]]];
  masks.forEach((mask,i)=>piece(c,type,sprite,size,mask,{x:(i%2?1:-1)*spread,y:q*(i<2?14:7)-lift*(i<2?8:3),a:(i%2?1:-1)*q*(heavy?.3:.6),sy:1-q*.2}));
  if(p<.5){line(c,[[0,-size*.38],[-5,-13],[6,-4],[-2,size*.28]],'#405256',1+q*2);line(c,[[-5,-13],[-size*.22,-8]],'#f6efd3',1.2);}
  for(let i=0;i<(heavy?6:4);i++){const aa=i*2.399;stone(c,Math.cos(aa)*(11+q*(heavy?41:30)),8+Math.sin(aa)*(7+q*8)-lift*(12+i%3*5),heavy?5:3.5,aa+q*2);}
  if(!heavy){c.save();c.translate(-20-q*16,-14+q*27-lift*13);c.rotate(-q*1.2);path(c,[[-12,-2],[6,-4],[11,1],[-8,3]],'#e8a24b',INK,1);c.restore();}dust(c,p,heavy?54:33);
 }else if(type==='paper'){
  piece(c,type,sprite,size,[[0,0],[.52,0],[.46,.38],[.55,.58],[.42,1],[0,1]],{x:-18*split,y:16*q,a:-.9*q,sx:1-.5*q,px:.5,py:.8});
  piece(c,type,sprite,size,[[.52,0],[1,0],[1,1],[.42,1],[.55,.58],[.46,.38]],{x:19*split,y:12*q,a:1.15*q,sx:1-.25*q,px:.5,py:.8});
  for(let i=0;i<4;i++)paperScrap(c,(i-1.5)*(8+q*10),6+q*11-lift*(9+i*2),7,10,i*.7+q*(i%2?2:-2),i===3);
  line(c,[[-size*.1,-20+q*24],[3,3+q*12],[-6,16+q*6]],'#c2b89d',1.2);
 }else if(type==='scissors'||type==='shears'){
  const shadow=type==='shears';
  piece(c,type,sprite,size,[[0,0],[.5,0],[.5,1],[0,1]],{x:-18*split,y:14*q,a:-q*(shadow?1.5:1.15),px:.5,py:.65});
  piece(c,type,sprite,size,[[.5,0],[1,0],[1,1],[.5,1]],{x:20*split,y:12*q,a:q*1.4,px:.5,py:.65});
  oval(c,q*11,-2+q*22-lift*23,4,2+Math.abs(Math.cos(q*9))*2,'#b9c9cb',INK,1);line(c,[[q*11-2,-3+q*22-lift*23],[q*11+2,1+q*22-lift*23]],'#5e747b',1);
  if(shadow){for(let i=0;i<3;i++){c.beginPath();c.moveTo(-12-q*24,8+i*4);c.bezierCurveTo(-35-q*15,-6-lift*12,14-q*14,-18-lift*10,23+q*12,12+i*2);c.strokeStyle=i?'#b799d299':'#74578f';c.lineWidth=4-i;c.stroke();}oval(c,0,14,29+q*13,8,'#7f5a9b22');}
 }else if(type==='cd'){
  const drop=-29+q*44;
  for(let i=0;i<8;i++){const a=i*TAU/8,scatter=split*20;c.save();c.translate(Math.cos(a)*scatter,drop+Math.sin(a)*scatter*.38-lift*8);c.rotate(a+q*(i%2?.5:-.5));c.scale(1,1-q*.55);c.beginPath();c.moveTo(6,-2);c.lineTo(32,-2);c.arc(0,0,32,-.06,TAU/8+.06);c.lineTo(Math.cos(TAU/8)*7,Math.sin(TAU/8)*7);c.closePath();paint(c,['#c0d9ec','#d8caef','#b0dfd6','#f3e1ad'][i%4]);c.beginPath();c.arc(0,0,24,.10,.63);paint(c,null,'#fff9e4',2.4);c.restore();}
  oval(c,0,drop+3,9,Math.max(3,8-q*4),'#e3ece8',INK,1.7);oval(c,0,drop+3,3,Math.max(1,3-q),'#565f5f');dust(c,p,40);
 }else if(type==='sniper'){
  piece(c,type,sprite,size,[[0,0],[1,0],[1,.43],[0,.43]],{x:-q*15,y:25*q,a:-q*.9,py:.45});
  piece(c,type,sprite,size,[[0,.43],[1,.43],[1,.72],[0,.72]],{x:q*12,y:10*q,a:q*.8,py:.7});
  for(let i=0;i<2;i++)piece(c,type,sprite,size,[[i*.5,.72],[(i+1)*.5,.72],[(i+1)*.5,1],[i*.5,1]],{x:(i?1:-1)*q*13,y:q*6,a:(i?1:-1)*q*.75,py:.8});
  c.save();c.translate(15+q*19,7+q*7-lift*10);c.rotate(q*.8);line(c,[[-22,0],[-3,0]],'#b28b4a',6);line(c,[[2,1],[15,3]],'#d7b365',5);path(c,[[-3,-3],[2,0],[-2,4]],PAPER,INK,1);rect(c,-12,-8,11,5,'#657a6c');oval(c,-12,-5,2.5,2.5,'#92d1c6',INK,1);c.restore();
 }else if(type==='airplane'){
  const drop=-32+q*50;
  piece(c,type,sprite,size,[[0,0],[.5,0],[.5,1],[0,1]],{x:-q*9,y:drop,a:-q*1.1,sx:1-q*.25,sy:1-q*.45,py:.65});
  piece(c,type,sprite,size,[[.5,0],[1,0],[1,1],[.5,1]],{x:q*14,y:drop+q*4,a:q*.65,sx:1-q*.45,sy:1-q*.4,py:.65});
  for(let i=0;i<3;i++)paperScrap(c,-26+q*(10+i*12),drop+5-lift*(7+i*4),9+i*2,6+i*2,q*3+i,i===2);dust(c,p,34);
 }else if(type==='mage'){
  piece(c,type,sprite,size,[[0,0],[1,0],[1,.46],[0,.46]],{x:-q*25,y:q*21-lift*18,a:-q*1.1,py:.45});
  piece(c,type,sprite,size,[[0,.46],[.56,.46],[.56,1],[0,1]],{x:-q*8,y:q*10,a:-q*.55,py:.8});
  piece(c,type,sprite,size,[[.56,.46],[1,.46],[1,1],[.56,1]],{x:q*19,y:q*10,a:q*.7,py:.8});
  c.save();c.translate(26+q*9,-3+q*18);c.rotate(q*1.6);line(c,[[0,22],[0,-15]],'#b58b5c',4);path(c,[[-4,-17],[1,-30],[7,-18],[1,-8]],'#a3f3e3','#5a9995',1.4);c.restore();
  for(let i=0;i<4;i++){const a=i*1.8+q;path(c,[[31+Math.cos(a)*(8+q*17),-15+q*28-lift*14],[35+Math.cos(a)*(8+q*17),-19+q*28-lift*14],[37+Math.cos(a)*(8+q*17),-11+q*28-lift*14]],i%2?'#a9ebe3':'#74c4be','#5d9997',1);}
 }else if(type==='divider'){
  const masks=[[[0,0],[.5,0],[.5,.5],[0,1]],[[.5,0],[1,0],[1,1],[.5,.5]],[[0,1],[.5,.5],[1,1]]];
  masks.forEach((mask,i)=>piece(c,type,sprite,size,mask,{x:(i-1)*q*17,y:-q*18-lift*(i===1?7:3),a:(i-1)*q*.8,sy:1-q*.4,py:.8}));
  c.save();c.translate(-10+q*23,-33+q*45-lift*11);c.rotate(q*1.3);path(c,[[-14,5],[-17,-10],[-5,-3],[1,-15],[7,-3],[17,-8],[12,6]],'#a38abd');star(c,0,0,4,'#efc26f');c.restore();
  for(let i=0;i<3;i++)paperScrap(c,(i-1)*(22+q*14),14-lift*11,12,10,i+q*2,i===1);
 }
 const ready=remains?.complete&&remains.naturalWidth;
 if(ready&&p>.54){
  const [x0,y0,x1,y1]=REMAINS_BOUNDS[DEATH_TYPES.indexOf(type)],w=x1-x0+1,h=y1-y0+1,dw=size*(type==='cd'?1.22:1.3),dh=dw*h/w;
  c.globalAlpha=ease((p-.54)/.24)*(1-clamp((p-.90)/.10));
  c.drawImage(remains,x0,y0,w,h,-dw/2,18-dh,dw,dh);
 }
 c.restore();return true;
}

function deathSheet(type,sprite){
 if(typeof Image!=='undefined'){
  let entry=CACHE.get(type);
  if(!entry?.image){
   const image=new Image();image.onerror=()=>{if(!image._pngFallback){image._pngFallback=true;image.src=new URL('./assets/deaths/'+type+'-death.png',import.meta.url).href;}};
   image.src=new URL('./assets/deaths/'+type+'-death.webp',import.meta.url).href;entry={image};
  }
  CACHE.delete(type);CACHE.set(type,entry);while(CACHE.size>DEATH_CACHE_LIMIT)CACHE.delete(CACHE.keys().next().value);
  return entry.image.complete&&entry.image.naturalWidth?entry.image:null;
 }
 // Non-browser canvas hosts can bake the same authored frames on demand.
 const stamp=(sprite?.complete?sprite.naturalWidth||0:0)+':'+!!(remains?.complete&&remains.naturalWidth),old=CACHE.get(type);
 if(old?.stamp===stamp){CACHE.delete(type);CACHE.set(type,old);return old.canvas;}
 let canvas;
 try{canvas=typeof OffscreenCanvas!=='undefined'?new OffscreenCanvas(DEATH_FRAME_SIZE*DEATH_FRAMES,DEATH_FRAME_SIZE):globalThis.document?.createElement?.('canvas');if(!canvas)return null;canvas.width=DEATH_FRAME_SIZE*DEATH_FRAMES;canvas.height=DEATH_FRAME_SIZE;const c=canvas.getContext('2d');if(!c)return null;
  for(let i=0;i<DEATH_FRAMES;i++){c.save();c.translate(i*DEATH_FRAME_SIZE+DEATH_FRAME_SIZE/2,DEATH_FRAME_SIZE*.66);paintDeathPose(c,type,i/DEATH_FRAMES,sprite);c.restore();}
 }catch{return null;}
 CACHE.delete(type);CACHE.set(type,{stamp,canvas});while(CACHE.size>DEATH_CACHE_LIMIT)CACHE.delete(CACHE.keys().next().value);return canvas;
}

export function drawDeathAnimation(renderer,c,fx){
 if(fx.type!=='death')return false;
 const type=fx.entityType||fx.unitType||fx.sourceType;
 if(!DEATH_TYPES.includes(type))return false;
 if(!(renderer.pointVisible?.(fx.x,fx.y)??true)||!(renderer.visible?.(fx.x,fx.y,100)??true))return true;
 const p=clamp(1-(fx.life??0)/(fx.total||DEATH_DURATION));if(p>=1)return true;
 const sprite=renderer.artSprites?.[type],sheet=deathSheet(type,sprite);
 c.save();c.translate(fx.x,fx.y);
 c.globalAlpha=1-clamp((p-.9)/.1);
 // Side-facing portraits fall in the same direction as their living unit.
 if(['sniper','shears'].includes(type)&&Math.cos(fx.facing||0)<0)c.scale(-1,1);
 if(sheet){const frame=Math.min(DEATH_FRAMES-1,Math.floor(p*DEATH_FRAMES));c.drawImage(sheet,frame*DEATH_FRAME_SIZE,0,DEATH_FRAME_SIZE,DEATH_FRAME_SIZE,-DEATH_FRAME_SIZE/2,-DEATH_FRAME_SIZE*.66,DEATH_FRAME_SIZE,DEATH_FRAME_SIZE);}
 else paintDeathPose(c,type,p,sprite);
 c.restore();return true;
}
