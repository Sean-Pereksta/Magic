/* Original painted sprites and Canvas animation for the Warfare expansion. */
const INK='#354549', GOLD='#e6b454', PAPER='#fff9df', VIOLET='#9276c6';
const COLORS=['#269b98','#ed795d','#9e7bc5','#d4aa4e'];
const team=t=>COLORS[t]||'#9da78c';
const TAU=Math.PI*2;
function effectImage(name){if(typeof Image==='undefined')return null;const img=new Image();img.onerror=()=>{if(!img._pngFallback){img._pngFallback=true;img.src=new URL('./assets/'+name+'.png',import.meta.url).href;}};img.src=new URL('./assets/'+name+'.webp',import.meta.url).href;return img;}
const EFFECT_TEXTURE=effectImage('warfare-effects');
const EFFECT_SHEET=effectImage('warfare-effects-sheet');
export const loadWarfareEffects=()=>Promise.allSettled([EFFECT_TEXTURE,EFFECT_SHEET].filter(Boolean).map(img=>img.decode?.()||Promise.resolve()));
export const warfareEffectsLoaded=()=>!!(EFFECT_SHEET?.complete&&EFFECT_SHEET.naturalWidth);
export const ANIMATION_ROWS=Object.freeze({idle:0,move:1,attack:2,special:3});
export const ANIMATION_FPS=Object.freeze({idle:4,move:8,attack:12,special:8});
const clamp=(n,a=0,b=1)=>Math.max(a,Math.min(b,n));
const oval=(c,x,y,rx,ry,fill,stroke,width=1.5)=>{c.beginPath();c.ellipse(x,y,Math.max(.01,rx),Math.max(.01,ry),0,0,TAU);paint(c,fill,stroke,width);};
function paint(c,fill,stroke=INK,width=1.5){if(fill){c.fillStyle=fill;c.fill();}if(stroke){c.strokeStyle=stroke;c.lineWidth=width;c.stroke();}}
function path(c,points,fill,stroke=INK,width=1.5){c.beginPath();points.forEach(([x,y],i)=>i?c.lineTo(x,y):c.moveTo(x,y));if(fill)c.closePath();paint(c,fill,stroke,width);}
function rect(c,x,y,w,h,fill,stroke=INK,r=3,width=1.5){c.beginPath();c.roundRect(x,y,w,h,r);paint(c,fill,stroke,width);}
function stroke(c,points,color=INK,width=1.5){path(c,points,null,color,width);}
function eyes(c,x,y,spacing=6,color=INK){oval(c,x-spacing,y,1.7,2.3,color,null);oval(c,x+spacing,y,1.7,2.3,color,null);}
function glint(c,x,y,color=PAPER,r=5){stroke(c,[[x-r,y],[x+r,y]],color,1.3);stroke(c,[[x,y-r],[x,y+r]],color,1.3);oval(c,x,y,1.4,1.4,color,null);}
function gear(c,x,y,r,phase,color='#a9b9b8'){c.save();c.translate(x,y);c.rotate(phase);for(let i=0;i<8;i++){c.rotate(TAU/8);rect(c,r*.55,-r*.2,r*.5,r*.4,color,INK,1,1);}oval(c,0,0,r*.73,r*.73,color,INK,1.4);oval(c,0,0,r*.27,r*.27,'#5f7579',INK,1);c.restore();}
function familyMark(c,type,x,y,size=12){c.save();c.translate(x,y);c.scale(size/12,size/12);if(type==='rock'){path(c,[[-8,3],[-6,-5],[0,-8],[7,-4],[8,4],[0,7]],'#b4c4cc',INK,1.3);stroke(c,[[-5,-3],[0,-5],[3,-3]],'#f5f5df',1.2);}else if(type==='paper'){path(c,[[-6,-8],[2,-8],[7,-3],[6,8],[-7,7]],PAPER,INK,1.2);stroke(c,[[2,-8],[2,-3],[7,-3]],'#8eaeb4',1.1);}else{stroke(c,[[-5,-8],[4,4]],'#d5e7e7',3);stroke(c,[[5,-8],[-4,4]],'#d5e7e7',3);oval(c,-4,5,3,3,'#dda168',INK,1.1);oval(c,4,5,3,3,'#dda168',INK,1.1);}c.restore();}

export function getAnimationFrame(type,options={}){
 const e=options.entity||{},time=Math.max(0,options.animationTime??options.phase??options.time??0),attack=options.attackAnim??e.attackAnim??0;
 const special=!!e.aim||!!e.crash?.diving||!!e.roll||!!e.shadow||e.mageCasting>0||e.divisionCast>0;
 const state=options.animationState in ANIMATION_ROWS?options.animationState:special?'special':attack>0?'attack':options.walk||options.moving?'move':'idle';
 const fps=ANIMATION_FPS[state];let frame=Math.floor(time*fps)%4;
 if(!options.animationState&&state==='special'){
  if(type==='sniper'&&e.aim)frame=Math.min(3,Math.floor(clamp(1-e.aim.remaining/(e.aim.total||1.5))*4));
  else if(type==='divider'&&e.divisionCast>0)frame=Math.min(3,Math.floor(clamp(1-e.divisionCast/1.5)*4));
  else if(type==='mage'&&e.mageCasting>0)frame=Math.min(3,Math.floor(clamp(1-e.mageCasting/.4)*4));
 }
 return {state,row:ANIMATION_ROWS[state],frame,index:ANIMATION_ROWS[state]*4+frame,fps};
}

/** Uses real frame crops for troops; portraits remain the interface artwork. */
export function drawExpansionSprite(c,type,sprite,tc,options={}){
 if(!['sniper','airplane','mage','divider','rollingrock','shears'].includes(type))return false;
 const sheet=options.sheet,sheetReady=sheet?.complete&&sheet.naturalWidth>=4&&sheet.naturalHeight>=4;
 if(!sheetReady&&(!sprite?.complete||!sprite.naturalWidth))return false;
 const e=options.entity||{},t=options.phase||options.time||0,walk=options.walk||0,attack=clamp((options.attackAnim||0)/.25),facing=Number.isFinite(options.facing)?options.facing:0,alpha=typeof c.globalAlpha==='number'?c.globalAlpha:1;
 c.save();
 if(type==='airplane'){c.rotate(facing+Math.sin(t*4+(e.id||0))*(walk?.1:.025));if(e.crash?.diving)c.scale(1,.72);c.translate(-attack*2,0);}
 else if(type==='rollingrock'&&e.roll)c.rotate(t*9);
 else{c.rotate(walk?Math.sin(walk)*.035:Math.sin(t*1.8)*.009);if(type==='sniper'){if(Math.abs(facing)>Math.PI/2)c.scale(-1,1);c.translate(-attack*4,e.aim?2:0);if(e.aim)c.scale(1,.94);}else if(type==='shears'&&Math.abs(facing)>Math.PI/2)c.scale(-1,1);}
 const width=58,height=58,ratio=sheetReady?(sheet.naturalWidth/sheet.naturalHeight):sprite.naturalWidth/(sprite.naturalHeight||sprite.naturalWidth),dw=ratio>1?width:height*ratio,dh=ratio>1?width/ratio:height;
 if(sheetReady){const frame=getAnimationFrame(type,options),cw=sheet.naturalWidth/4,ch=sheet.naturalHeight/4;c.drawImage(sheet,frame.frame*cw,frame.row*ch,cw,ch,-dw/2,-dh/2-2,dw,dh);}
 else c.drawImage(sprite,-dw/2,-dh/2-2,dw,dh);
 // The team sash remains readable even when an illustrated soldier turns.
 if(type!=='airplane'&&type!=='rollingrock'){c.globalAlpha=alpha*.8;rect(c,-8,20,16,3,tc,null,1);c.globalAlpha=alpha;}
 if(!sheetReady&&type==='sniper'){glint(c,17,-8,e.aim?'#fff7bc':'#cbfff1',e.aim?3+Math.sin(t*8):1.7);if(attack>0){glint(c,28,0,'#fff1b0',4+attack*4);}}
 if(!sheetReady&&type==='mage'&&(e.mageCasting||e.mageRecharge>4.6)){oval(c,21,-24,5,6,'#b9ffe9aa',tc,1);glint(c,21,-24,'#eaffdc',6);}
 if(!sheetReady&&type==='divider'&&e.divisionCast>0)for(let i=0;i<3;i++)familyMark(c,['rock','paper','scissors'][i],(i-1)*22,-38-Math.sin(t*5+i)*2,10);
 c.restore();return true;
}

/** Draws at the shared icon renderer's nominal 50px coordinate scale. */
export function drawExpansionIcon(c,type,tc,options={}){
 const t=options.phase||options.time||0,e=options.entity||{},walk=options.walk||0,active=options.active||options.working||options.production,attack=clamp((options.attackAnim||0)/.25);
 if(!['sniper','airplane','mage','divider','rollingrock','shears','workshop','laboratory','staplergate','gate','station','controlstation'].includes(type))return false;
 c.save();c.lineJoin='round';c.lineCap='round';
 if(type==='sniper'){
  const aiming=!!e.aim,crouch=aiming?4:Math.sin(walk)*1.1;
  stroke(c,[[-7,13],[-14,24],[-23,24]],INK,3);stroke(c,[[9,12],[13,24],[21,24]],INK,3);
  c.translate(0,crouch);path(c,[[-7,-1],[-16,-26],[-11,-25],[8,-5]],'#d9e8e5',INK,2);path(c,[[4,-1],[16,-27],[19,-26],[10,7]],'#9eb9c1',INK,2);
  oval(c,-7,12,8,10,tc,INK,2);oval(c,10,12,8,10,tc,INK,2);oval(c,-7,13,3.5,5,PAPER,INK,1);oval(c,10,13,3.5,5,PAPER,INK,1);
  oval(c,2,-1,10,8,'#e4b98a',INK,1.8);rect(c,-7,-6,18,4,'#657d78',INK,1,1);eyes(c,2,-1,4);stroke(c,[[0,5],[5,5]],INK,1.2);
  // Pencil stock, binder-clip receiver, blade barrel and animated optical scope.
  c.save();c.translate(4,5);if((options.facing||0)>Math.PI/2||(options.facing||0)<-Math.PI/2)c.scale(-1,1);c.rotate(aiming?-.04:-.12);c.translate(-attack*5,0);
  path(c,[[-23,1],[-12,-3],[-5,-1],[-8,5],[-22,8]],'#bd925d',INK,1.7);rect(c,-8,-5,24,9,'#6f8688',INK,2,1.8);path(c,[[15,-4],[36,-4],[42,-1],[16,1]],'#d9e9e7',INK,1.6);stroke(c,[[17,-2],[35,-2]],'#f9ffe8',1.3);
  rect(c,0,-11,15,6,'#4c626b',INK,2,1.4);oval(c,16,-8,3.3,3.5,aiming?'#f6d17a':'#82d5cb',INK,1.2);glint(c,17,-8,aiming?'#fff8cc':'#c9fff0',2+Math.sin(t*7)*1.2);stroke(c,[[2,4],[0,10],[8,10]],INK,1.3);
  if(attack>0){path(c,[[41,-2],[48,-7],[46,-1],[52,2],[43,2]],'#fff3b3',GOLD,1.2);}c.restore();
 }else if(type==='airplane'){
  const dive=e.crash?.diving,bank=options.walk?Math.sin(t*4+(e.id||0))*.09:Math.sin(t*2)*.035;
  c.rotate((Number.isFinite(options.facing)?options.facing:0)+bank);c.scale(1,dive?.72:1);
  path(c,[[-26,-21],[29,0],[-26,21],[-11,1]],'#e4e9d8',INK,2);path(c,[[-26,-21],[5,-3],[29,0],[-9,1]],'#fffbed',INK,1.7);path(c,[[-26,21],[-9,1],[29,0],[5,5]],'#ccded6',INK,1.6);path(c,[[-11,1],[1,-5],[29,0]],'#f5f1d1',INK,1.2);
  stroke(c,[[-20,-15],[-5,-8]],tc,3);stroke(c,[[-20,15],[-5,8]],tc,3);stroke(c,[[-17,-11],[-10,-8]],'#f9d28a',1);oval(c,6,-3,1.7,2.4,INK,null);oval(c,6,4,1.7,2.4,INK,null);stroke(c,[[14,-2],[17,0],[14,2]],INK,1.2);rect(c,-3,-2,13,4,'#7e9b9b',INK,1,1);
  if(attack>0)glint(c,23,0,'#f0bd5d',4+attack*4);
  if(dive){for(let i=0;i<3;i++)stroke(c,[[-32-i*9,-4+i*4],[-24-i*9,-3+i*4]],'#fff6d7',1.5);}
 }else if(type==='mage'){
  stroke(c,[[-8,16],[-12,25],[-18,25]],INK,3);stroke(c,[[7,16],[9,25],[15,25]],INK,3);
  path(c,[[-18,9],[-17,-7],[-4,-17],[12,-15],[20,2],[13,17],[-7,20]],'#9fabbf',INK,2.1);path(c,[[-14,-5],[-4,-14],[10,-12],[3,-4],[-10,1]],'#d3d9de',null);eyes(c,0,1,6);stroke(c,[[-4,8],[3,10],[9,6]],INK,1.4);
  path(c,[[-18,-12],[-8,-21],[-5,-35],[7,-21],[16,-11]],'#697a95',INK,2);path(c,[[-5,-35],[0,-27],[3,-19],[-8,-20]],'#b3bacf',null);rect(c,-17,-15,33,5,tc,INK,1,1.2);glint(c,-2,-23,GOLD,3);
  const raised=e.casting||e.cast||e.mageCasting||e.mageRecharge>4.6?6:0;stroke(c,[[12,8],[22,3-raised],[24,-22-raised]],'#9e805d',3.5);stroke(c,[[22,-20-raised],[29,-28-raised],[21,-33-raised],[15,-25-raised],[22,-20-raised]],'#c6af7e',2);oval(c,22,-26-raised,4,5,'#b9f6e7',tc,1.4);glint(c,22,-26-raised,'#e1fff1',3+Math.sin(t*5));
  for(let i=0;i<3;i++){const a=t*1.8+i*TAU/3;const x=Math.cos(a)*26,y=Math.sin(a)*10+6;path(c,[[x-4,y],[x-1,y-5],[x+4,y-1],[x+3,y+4],[x-3,y+3]],'#b4becb',VIOLET,1);}
 }else if(type==='divider'){
  // Floating folded-paper archmage, crowned with three family sigils.
  const hover=Math.sin(t*2.7)*1.5;c.translate(0,hover);path(c,[[-20,22],[-14,-6],[-8,-15],[12,-12],[17,1],[23,22],[5,16],[-2,24],[-8,17]],'#e3dbeb',INK,2);path(c,[[-14,-6],[-3,2],[-8,17],[-20,22]],'#fff6dd',INK,1.3);path(c,[[-3,2],[12,-12],[17,1],[23,22],[5,16]],'#d8cdea',INK,1.2);
  path(c,[[-13,-7],[-13,-21],[3,-29],[15,-18],[13,-5],[1,1]],PAPER,INK,1.7);path(c,[[3,-29],[3,-18],[15,-18]],'#c9d9d8',INK,1.1);eyes(c,1,-12,5,VIOLET);stroke(c,[[-2,-5],[4,-5]],INK,1.1);
  stroke(c,[[-8,4],[-2,8],[-7,12]],tc,1.4);stroke(c,[[6,2],[11,6],[5,11]],GOLD,1.4);stroke(c,[[0,14],[1,19]],VIOLET,1.8);
  stroke(c,[[13,6],[25,2],[26,-25]],'#c9bd9b',3);path(c,[[26,-35],[35,-26],[27,-18],[18,-25]],PAPER,VIOLET,1.5);glint(c,26,-26,'#fff9c8',5);
  for(let i=0;i<3;i++){const a=t*.7+i*TAU/3,x=Math.cos(a)*29,y=Math.sin(a)*15-10;c.save();c.translate(x,y);c.rotate(a*.3);path(c,[[-5,-5],[5,-4],[6,5],[-5,4]],PAPER,VIOLET,1);stroke(c,[[-2,0],[3,0]],tc,1);c.restore();}
  if(e.casting||e.cast||e.divisionCast||options.casting)for(let i=0;i<3;i++)familyMark(c,['rock','paper','scissors'][i],(i-1)*22,-44-Math.sin(t*5+i)*2,10);
 }else if(type==='rollingrock'){
  const rolling=!!e.roll,angle=rolling?t*9:Math.sin(walk)*.025;c.rotate(angle);
  if(!rolling){stroke(c,[[-12,18],[-17,27],[-24,27]],INK,4);stroke(c,[[12,18],[17,27],[24,27]],INK,4);stroke(c,[[-21,2],[-28,8],[-29,3]],INK,4);stroke(c,[[21,2],[28,8],[29,3]],INK,4);}
  path(c,[[-27,4],[-24,-15],[-10,-26],[12,-25],[27,-8],[26,13],[12,26],[-10,27],[-25,16]],'#8d9da6',INK,2.3);path(c,[[-24,-13],[-9,-23],[10,-22],[4,-11],[-13,-7],[-20,7]],'#c1cbd0',null);path(c,[[26,-7],[25,12],[12,24],[5,11],[15,2]],'#70858f',null);
  path(c,[[-22,4],[-13,0],[-17,-8],[-7,-5]],null,INK,1.5);stroke(c,[[13,-18],[10,-8],[17,-4],[12,4]],INK,1.3);stroke(c,[[-6,25],[-2,15],[-7,11]],'#536971',1.4);
  if(!rolling){eyes(c,1,0,8);stroke(c,[[-13,-6],[-5,-8]],INK,2);stroke(c,[[5,-8],[14,-6]],INK,2);stroke(c,[[-7,11],[0,14],[8,10]],INK,2);rect(c,-15,17,30,5,tc,INK,1.5,1);}
  else{stroke(c,[[-16,-13],[-5,-18],[9,-16]],'#e1e8df',2);path(c,[[-2,-3],[9,0],[2,9],[-7,4]],'#a9b9bd',INK,1.4);}
 }else if(type==='shears'){
  const shadow=!!e.shadow;c.rotate(Math.sin(walk)*.04);for(let i=0;i<2;i++){c.beginPath();c.moveTo(-8+i*15,8);c.bezierCurveTo(-22+i*8,12,-30+i*9,4+Math.sin(t*7)*8,-38+i*8,15);paint(c,null,'#504d65',3-i);}
  stroke(c,[[-8,17],[-12,26],[-18,25]],INK,3);stroke(c,[[10,17],[14,26],[20,25]],INK,3);
  path(c,[[-5,0],[-18,-28],[-12,-25],[7,-5]],'#667281',INK,2);path(c,[[4,2],[23,-28],[24,-22],[11,8]],'#8e9caa',INK,2);stroke(c,[[-15,-24],[0,-6]],'#c7b7e8',1);stroke(c,[[21,-24],[9,-3]],'#d9b6ff',1);
  oval(c,-8,12,9,11,'#52586f',INK,2);oval(c,12,12,9,11,'#52586f',INK,2);oval(c,-8,13,4,6,'#b5a4c8',INK,1);oval(c,12,13,4,6,'#b5a4c8',INK,1);
  path(c,[[-8,-5],[4,-11],[14,-4],[11,5],[-5,6]],'#5c6379',INK,1.5);stroke(c,[[-5,-1],[-1,1]],'#d3b4ff',2.4);stroke(c,[[7,1],[11,-2]],'#d3b4ff',2.4);rect(c,-5,6,16,3,tc,null,1);oval(c,2,-6,2,2,GOLD,INK,1);
  if(shadow||attack>0){c.beginPath();c.arc(9,-5,32,-1.6,.6);paint(c,null,'#d2aaff',2.6);}
 }else if(type==='workshop'){
  rect(c,-29,-14,58,38,'#b7c6bd',INK,4,2.2);rect(c,-32,-18,64,20,'#e4dfc8',INK,4,2);path(c,[[-30,-18],[-21,-26],[22,-26],[31,-18]],'#a7b7b3',INK,1.8);
  rect(c,-23,5,27,15,'#586d70',INK,2);stroke(c,[[-18,9],[-8,9],[-8,15],[-15,15]],tc,1.5);rect(c,10,1,14,17,'#d2c49f',INK,2);oval(c,17,6,2,2,active&&Math.sin(t*9)>0?'#ffe08a':'#9bd6aa',INK,.7);
  gear(c,-18,-13,9,active?t*2:.2,'#9cabae');gear(c,-4,-17,6,active?-t*3:0,'#d5b97f');stroke(c,[[11,-26],[15,-40]],'#deb564',4);stroke(c,[[18,-25],[24,-38]],'#8ca9b0',3);path(c,[[13,-41],[16,-43],[16,-38]],'#f0d7a3',INK,1);
  rect(c,-5,-31,16,7,PAPER,INK,1);stroke(c,[[-2,-28],[7,-28]],tc,1.4);rect(c,-27,23,55,5,'#9b8061',INK,1.5);stroke(c,[[-21,28],[-21,31]],INK,3);stroke(c,[[22,28],[22,31]],INK,3);
  if(active){const h=Math.sin(t*5)*4;stroke(c,[[4,-13],[11,-13],[11,-6+h],[20,-6+h]],'#617f82',3);glint(c,20,-6+h,GOLD,3);}
 }else if(type==='laboratory'){
  rect(c,-32,-15,64,42,'#859b9f',INK,4,2.3);rect(c,-35,-21,70,21,'#cbd4cc',INK,4,2.3);path(c,[[-31,-21],[-20,-36],[25,-36],[33,-21]],'#e1e5d7',INK,1.9);
  rect(c,-21,-31,42,10,'#475b65',INK,2);for(let i=0;i<8;i++){const x=-18+i*5;path(c,[[x,-30],[x+4,-30],[x+2,-23+(active?Math.sin(t*8+i)*1.3:0)]],'#a2b6b3',INK,.7);}
  // Front conveyor, powered twin shredders and glowing operator panel.
  rect(c,-22,6,46,20,'#455e66',INK,2);for(let i=0;i<7;i++){const xx=-19+i*6+(active?(t*10)%6:0);stroke(c,[[xx,8],[xx-3,23]],'#899e99',1.7);}gear(c,-26,2,10,active?t*3:0,'#d2b37a');gear(c,22,2,12,active?-t*2.2:0,'#aab9b8');
  rect(c,-12,-15,25,10,'#455b62',INK,1);rect(c,-9,-12,10,4,'#99e1c5',null,1);for(let i=0;i<3;i++)oval(c,5+i*3,-10,1.2,1.2,i===1?'#f3cd78':tc,null);stroke(c,[[29,-18],[29,-39],[22,-39]],'#667e7f',4);oval(c,22,-39,3,3,active?'#e6ca78':tc,INK,1);
  rect(c,-31,27,63,5,'#6a7c7d',INK,1);for(let i=0;i<4;i++){c.save();c.translate(-15+i*10,17);c.rotate((i-1)*.22);rect(c,-3,-4,6,8,PAPER,INK,.4,.5);c.restore();}
  if(active){for(let i=0;i<3;i++){const p=(t*.7+i/3)%1;c.save();c.translate(-10+i*10,7+p*18);c.rotate(p*.7);rect(c,-2,-3,4,6,PAPER,null,.5);c.restore();}}
 }else if(type==='staplergate'||type==='gate'){
  rect(c,-34,14,68,14,'#719293',INK,3,2);path(c,[[-33,12],[-27,-9],[22,-9],[34,10]],'#a9bec0',INK,2);c.save();c.rotate(-.13);rect(c,-32,-19,66,20,'#8e626d',INK,5,2);rect(c,-26,-16,55,8,'#c48d89',null,3);stroke(c,[[-21,-10],[23,-10]],'#edb2a0',1.4);c.restore();rect(c,-25,9,43,5,'#d9ddd2',INK,1);oval(c,-23,4,4,4,'#6b8388',INK,1);stroke(c,[[-8,18],[11,18]],GOLD,3);for(let i=0;i<3;i++)stroke(c,[[-19+i*17,25],[-11+i*17,17]],'#465e65',3);
 }else{
  rect(c,-19,6,38,19,'#849c98',INK,3,2);path(c,[[-15,6],[-12,-22],[17,-22],[19,6]],'#ced8c8',INK,2);rect(c,-8,-17,20,13,'#4c6267',INK,1);stroke(c,[[-4,-10],[7,-10],[3,-14]],tc,2);oval(c,1,2,4,4,tc,INK,1.1);stroke(c,[[-9,22],[-9,27]],INK,3);stroke(c,[[11,22],[11,27]],INK,3);stroke(c,[[17,-17],[25,-22],[25,-32]],'#8f9b81',2);oval(c,25,-33,3,3,tc,INK,1);
 }
 c.restore();return true;
}

export function drawWarfareEntityOverlay(renderer,c,e,selected){
 const t=renderer.time||0,g=renderer.game,tc=team(e.team);
 if(e.aim){const target=g.getEntity?.(e.aim.targetId);if(target&&renderer.entityVisible(target)){const progress=clamp(1-e.aim.remaining/(e.aim.total||1.5));c.save();c.setLineDash([6,5]);stroke(c,[[e.x,e.y-14],[target.x,target.y-12]],tc+'b0',1.2/renderer.camera.zoom);c.setLineDash([]);oval(c,target.x,target.y-12,18,18,null,GOLD,1.5);for(let i=0;i<4;i++){const a=i*Math.PI/2+t*.6;stroke(c,[[target.x+Math.cos(a)*15,target.y-12+Math.sin(a)*15],[target.x+Math.cos(a)*23,target.y-12+Math.sin(a)*23]],'#fff4c7',1.6);}c.beginPath();c.arc(e.x,e.y-16,33,-Math.PI/2,-Math.PI/2+progress*TAU);paint(c,null,GOLD,3);c.restore();}}
 if(e.crash){const p=e.crash;c.save();c.setLineDash([5,8]);if(selected){stroke(c,[[e.x,e.y],[p.x,p.y]],tc+'88',1);oval(c,p.x,p.y,19,10,null,COLORS[1],1.5);}c.setLineDash([]);const a=Number.isFinite(e.facing)?e.facing:Math.atan2(p.y-e.y,p.x-e.x);for(let i=0;i<6;i++){const age=(t*3+i*.16)%1,back=18+age*65;c.globalAlpha=(1-age)*.5;oval(c,e.x-Math.cos(a)*back,e.y-25-Math.sin(a)*back-18*age,3+age*7,2+age*5,i%2?'#fff5dc':'#9c9d91',null);}c.restore();}
 if(e.roll){const a=Number.isFinite(e.facing)?e.facing:Math.atan2(e.roll.y-e.y,e.roll.x-e.x);c.save();for(let i=0;i<6;i++){const p=(t*3+i/6)%1,x=e.x-Math.cos(a)*(25+p*65)+Math.sin(a)*Math.sin(i*4)*14,y=e.y-Math.sin(a)*(25+p*65)-Math.cos(a)*Math.sin(i*4)*14;c.globalAlpha=(1-p)*.36;oval(c,x,y+12,6+p*15,4+p*8,'#c4ad80',null);}c.globalAlpha=1;if(selected){stroke(c,[[e.roll.x-8,e.roll.y],[e.roll.x+8,e.roll.y]],GOLD,1.6);stroke(c,[[e.roll.x,e.roll.y-8],[e.roll.x,e.roll.y+8]],GOLD,1.6);}c.restore();}
 if(e.shadow){const points=e.shadow.points||[],index=e.shadow.index||0,prev=points[Math.max(0,index-1)],a=Number.isFinite(e.facing)?e.facing:prev?Math.atan2(e.y-prev.y,e.x-prev.x):0;c.save();for(let i=4;i>0;i--){c.globalAlpha=.22*(1-i/5);c.save();c.translate(e.x-Math.cos(a)*i*20,e.y-Math.sin(a)*i*20-9);c.scale(1.3,1.3);if(!drawExpansionSprite(c,'shears',renderer.artSprites?.shears,'#8e7cb7',{phase:t-i*.04,facing:a,sheet:renderer.artSheets?.shears,entity:e}))drawExpansionIcon(c,'shears','#8e7cb7',{phase:t,facing:a});c.restore();}c.globalAlpha=.7;stroke(c,[[e.x-Math.cos(a)*75,e.y-Math.sin(a)*75],[e.x,e.y]],'#cba3f5',3);stroke(c,[[e.x-Math.cos(a)*55,e.y-Math.sin(a)*55-4],[e.x,e.y-4]],'#fff2ff',1);c.restore();}
 const cooldown=typeof e.abilityCooldown==='number'?e.abilityCooldown:0;
 if(selected&&cooldown>0){const total=({sniper:18,divider:38,rollingrock:17,shears:18})[e.type]||18;c.save();c.beginPath();c.arc(e.x,e.y+10,29,-Math.PI/2,-Math.PI/2+TAU*clamp(1-cooldown/total));paint(c,null,GOLD,2);c.restore();}
 if(selected&&e.type==='mage'){for(let i=0;i<2;i++)oval(c,e.x-5+i*10,e.y+32,3,3,i<(e.mageCharges??2)?'#b7f1df':'#657b7c',tc,1);}
 if(e.type==='station'){const station=g.map?.stations?.find(s=>s.entityId===e.id);if(station){c.save();if(station.captureProgress>0){c.beginPath();c.arc(e.x,e.y+4,43,-Math.PI/2,-Math.PI/2+TAU*clamp(station.captureProgress));paint(c,null,team(station.captureTeam),3);}if(selected){const remaining=Math.max(0,(station.cooldownUntil||0)-(g.time||0));rect(c,e.x-45,e.y+35,90,16,'#fff8e3e0',tc,4,1);c.textAlign='center';c.fillStyle=INK;c.font='bold 9px "Trebuchet MS",sans-serif';c.fillText(remaining>0?`RECHARGE ${Math.ceil(remaining)}s`:station.owner>=0?'BELT CONTROL':'CAPTURE POINT',e.x,e.y+46);}c.restore();}}
}

/** Ground fills precede troops; translucent shield caps sit above them. */
export function drawWarfareFields(renderer,c,layer='ground'){
 const g=renderer.game,t=renderer.time||0;
 for(const f of g.forceFields||[]){const r=f.r||110;if(!renderer.visible(f.x,f.y,r+40)||!renderer.pointVisible(f.x,f.y))continue;const fade=clamp((f.remaining??10)/1.7),tc=team(f.team);c.save();c.globalAlpha=fade;
  if(layer==='ground'){oval(c,f.x,f.y,r,r,tc+'12',tc,2);oval(c,f.x,f.y,r-5,r-5,null,'#c8fff394',1);for(let i=0;i<10;i++){const a=t*.5+i*TAU/10;glint(c,f.x+Math.cos(a)*r,f.y+Math.sin(a)*r,tc,2.5);}}
  else{const glow=c.createLinearGradient(f.x,f.y-r*.9,f.x,f.y+r);glow.addColorStop(0,'#c7fff524');glow.addColorStop(.5,tc+'0a');glow.addColorStop(1,tc+'19');c.beginPath();c.ellipse(f.x,f.y,r,r,0,0,Math.PI);c.bezierCurveTo(f.x-r,f.y-r*1.25,f.x+r,f.y-r*1.25,f.x+r,f.y);paint(c,glow,tc+'90',1.5);c.save();c.beginPath();c.ellipse(f.x,f.y,r,r,0,0,Math.PI);c.bezierCurveTo(f.x-r,f.y-r*1.25,f.x+r,f.y-r*1.25,f.x+r,f.y);c.clip();for(let j=0;j<6;j++){const yy=f.y-r*.7+j*r*.25;stroke(c,[[f.x-r,yy],[f.x+r,yy-r*.1]],'#c2fff52b',1);}c.beginPath();c.ellipse(f.x,f.y,r*.58,r*.88,0,Math.PI,TAU);paint(c,null,'#c2fff53a',1);c.restore();c.beginPath();c.ellipse(f.x,f.y,r,r*.72,0,Math.PI*1.1,Math.PI*1.65);paint(c,null,'#e1fff590',2);}
  c.restore();
 }
 for(const f of g.divisions||[]){const r=f.r||350;if(!renderer.visible(f.x,f.y,r+30)||!renderer.pointVisible(f.x,f.y))continue;const casting=typeof f.casting==='number'&&f.casting>0,fade=clamp((f.remaining??9)/1.5),reveal=casting?clamp(1-f.casting/1.5):1,rotation=f.angle??-Math.PI/2,colors=['#b7a06b','#50b8b0','#a58bd0'];c.save();c.globalAlpha=fade*(casting?.55:1);
  if(layer==='ground'){for(let i=0;i<3;i++){const a=rotation+i*TAU/3;c.beginPath();c.moveTo(f.x,f.y);c.arc(f.x,f.y,r*(casting?.2+reveal*.8:1),a,a+TAU/3);c.closePath();paint(c,colors[i]+'1c',colors[i]+'aa',2);familyMark(c,['rock','paper','scissors'][i],f.x+Math.cos(a+Math.PI/3)*r*.64,f.y+Math.sin(a+Math.PI/3)*r*.64,24);}oval(c,f.x,f.y,r,r,null,'#e2d2f4',3);oval(c,f.x,f.y,r+7,r+7,null,VIOLET+'90',1.3);for(let i=0;i<24;i++){const a=i*TAU/24+t*.08;stroke(c,[[f.x+Math.cos(a)*(r-9),f.y+Math.sin(a)*(r-9)],[f.x+Math.cos(a)*(r+3),f.y+Math.sin(a)*(r+3)]],'#f4e9ff',i%3?1:2);}}
  else{for(let i=0;i<3;i++){const a=rotation+i*TAU/3,x=f.x+Math.cos(a)*r,y=f.y+Math.sin(a)*r;path(c,[[f.x,f.y],[x,y],[x,y-34],[f.x,f.y-34]],colors[i]+'18',colors[i]+'a0',1.5);stroke(c,[[f.x,f.y-34],[x,y-34]],'#eadfff',2);for(let j=0;j<4;j++){const p=(t*.3+j/4)%1,xx=f.x+(x-f.x)*p,yy=f.y+(y-f.y)*p-34;glint(c,xx,yy,colors[i],2.5);}}for(let i=0;i<7;i++){const a=t*.35+i*TAU/7,x=f.x+Math.cos(a)*r*.9,y=f.y+Math.sin(a)*r*.9-24;c.save();c.translate(x,y);c.rotate(a);path(c,[[-9,-3],[8,-4],[12,4],[-5,3]],PAPER+'b0',VIOLET,1);c.restore();}}
  c.restore();
 }
}

export function drawWarfareProjectile(renderer,c,p){
 if(!['dart','paperdart','snipe','sniper','magic','stonebolt'].includes(p.type))return false;
 if(p.life<=0||!renderer.visible(p.x,p.y,100))return true;const target=renderer.game.getEntity?.(p.targetId),a=Number.isFinite(p.angle)?p.angle:Math.atan2((target?.y??p.y)-(p.fromY??p.y),(target?.x??p.x)-(p.fromX??p.x)),tc=team(p.team);c.save();c.translate(p.x,p.y-17);c.rotate(a);
 if(p.type==='dart'||p.type==='paperdart'){stroke(c,[[-25,0],[-9,0]],tc+'66',2);path(c,[[11,0],[-8,-5],[-3,0],[-8,5]],PAPER,INK,1.1);stroke(c,[[-3,0],[8,0]],tc,1);}
 else if(p.type==='snipe'||p.type==='sniper'){stroke(c,[[-72,0],[8,0]],'#fff0bf',2);stroke(c,[[-45,0],[5,0]],GOLD,4);path(c,[[12,0],[0,-3],[0,3]],'#effcf6',INK,.8);}
 else{oval(c,0,0,6,5,p.type==='stonebolt'?'#aabbc7':'#a4eed8',tc,1.4);stroke(c,[[-22,0],[-7,0]],tc+'88',2);glint(c,-9,0,'#f0ffe1',2);}
 c.restore();return true;
}

export function drawWarfareEffect(renderer,c,fx){
 const types=['snipe','snipeShot','snipeInterrupted','dart','dartImpact','magic','stonebolt','crash','forcefield','forcefieldEnd','division','divisionEnd','divisionCollapse','rollhit','shadowSlash','shadowEnd','shadowFinish','dust','paperSmoke','capture','gateCollapse'];if(!types.includes(fx.type))return false;
 const radius=fx.radius||75;if(!renderer.visible(fx.x,fx.y,Math.max(160,radius)))return true;const p=clamp(1-(fx.life||0)/(fx.total||.7)),tc=team(fx.team);c.save();c.globalAlpha=1-p;
 const tile=({snipe:0,snipeShot:0,dartImpact:0,crash:1,shadowSlash:2,shadowEnd:2,shadowFinish:2,forcefield:3,forcefieldEnd:3,division:4,divisionEnd:4,divisionCollapse:4,rollhit:5})[fx.type];
 const texture=EFFECT_SHEET?.complete&&EFFECT_SHEET.naturalWidth?EFFECT_SHEET:EFFECT_TEXTURE;
 if(tile!==undefined&&texture?.complete&&texture.naturalWidth){
  const animated=texture===EFFECT_SHEET,cw=texture.naturalWidth/3,ch=texture.naturalHeight/(animated?6:2),column=animated?Math.min(2,Math.floor(p*3)):tile%3,row=animated?tile:Math.floor(tile/3),size=({crash:radius*2.8,forcefield:radius*2.1,forcefieldEnd:radius*2.1,division:radius*1.9,divisionEnd:radius*1.9,divisionCollapse:radius*1.9,rollhit:105,snipe:110,snipeShot:70,dartImpact:45})[fx.type]||110;
  c.save();c.translate(fx.x,fx.y-12);c.rotate(['shadowSlash','shadowEnd','shadowFinish'].includes(fx.type)?fx.angle||0:fx.type.startsWith('division')?p*.3:0);const scale=.55+p*.75;c.scale(scale,scale);c.globalAlpha=(1-p)*(['division','divisionEnd','forcefield','forcefieldEnd'].includes(fx.type)?.55:.95);c.drawImage(texture,column*cw,row*ch,cw,ch,-size/2,-size/2,size,size);c.restore();
 }
 if(['snipe','snipeShot','dart','dartImpact','magic','stonebolt'].includes(fx.type)){const rifle=fx.type==='snipe'||fx.type==='snipeShot',magic=fx.type==='magic'||fx.type==='stonebolt';if(Number.isFinite(fx.fromX)){const end=rifle&&p<.4?{x:fx.fromX+(fx.x-fx.fromX)*clamp(p*3),y:fx.fromY+(fx.y-fx.fromY)*clamp(p*3)}:fx;stroke(c,[[fx.fromX,fx.fromY-14],[end.x,end.y-14]],rifle?'#fff3c7':tc+'99',rifle?2.8:1.6);}glint(c,fx.x,fx.y-12,magic?'#b8f5e5':'#ffe09b',8+p*9);oval(c,fx.x,fx.y-12,9+p*16,7+p*10,null,magic?tc:GOLD,2);}
 else if(fx.type==='snipeInterrupted'){const x=fx.x,y=fx.y-27;stroke(c,[[x-5,y-5],[x+5,y+5]],'#dc9970',2);stroke(c,[[x-5,y+5],[x+5,y-5]],'#dc9970',2);}
 else if(fx.type==='crash'){const r=radius*(.2+p);oval(c,fx.x,fx.y,r,r*.65,'#f3c57d'+(p<.3?'55':'11'),GOLD,4*(1-p)+1);oval(c,fx.x,fx.y,r*.65,r*.43,null,'#fff4c8',3);for(let i=0;i<15;i++){const a=i*2.399,rr=(18+p*radius)*(i%3?.8:1.3),x=fx.x+Math.cos(a)*rr,y=fx.y+Math.sin(a)*rr*.7-Math.sin(p*Math.PI)*(15+i%4*5);c.save();c.translate(x,y);c.rotate(a+p*6);if(i%3===0)oval(c,0,-p*10,8+p*11,6+p*8,'#a49e8c55',null);else path(c,[[-4,-3],[5,-2],[3,5],[-3,4]],i%2?PAPER:'#d9e8de',INK,.6);c.restore();}if(p<.35)glint(c,fx.x,fx.y-7,'#fffbe2',28*(1-p));}
 else if(fx.type==='forcefield'||fx.type==='forcefieldEnd'){const r=radius*(fx.type==='forcefieldEnd'?1-p:.2+p);oval(c,fx.x,fx.y,r,r,null,'#acf7e6',3);for(let i=0;i<8;i++){const a=i*TAU/8;glint(c,fx.x+Math.cos(a)*r,fx.y+Math.sin(a)*r,tc,3);}}
 else if(fx.type.startsWith('division')){const collapse=fx.type!=='division',r=radius*(collapse?1-p:.5+p*.5);for(let i=0;i<3;i++){const a=-Math.PI/2+i*TAU/3;stroke(c,[[fx.x,fx.y],[fx.x+Math.cos(a)*r,fx.y+Math.sin(a)*r]],['#d2b583','#88ddd2','#d2b0ef'][i],4*(1-p));}oval(c,fx.x,fx.y,r,r,null,VIOLET,3);for(let i=0;i<12;i++){const a=i*2.399+p*2;glint(c,fx.x+Math.cos(a)*r,fx.y+Math.sin(a)*r-10,VIOLET,3);}}
 else if(fx.type==='rollhit'){for(let i=0;i<9;i++){const a=i*2.399;path(c,[[fx.x+Math.cos(a)*p*48-3,fx.y+Math.sin(a)*p*32],[fx.x+Math.cos(a)*p*48,fx.y+Math.sin(a)*p*32-6],[fx.x+Math.cos(a)*p*48+5,fx.y+Math.sin(a)*p*32]],'#9daeb3',INK,.9);}oval(c,fx.x,fx.y,13+p*34,7+p*18,null,'#d3ba83',2.4);}
 else if(fx.type==='shadowSlash'||fx.type==='shadowEnd'||fx.type==='shadowFinish'){c.translate(fx.x,fx.y-8);c.rotate(fx.angle||0);for(let i=0;i<3;i++){c.beginPath();c.ellipse(0,0,12+p*31+i*7,4+p*11,0,-1.4,.9);paint(c,null,i===1?'#f9eaff':'#b791dc',3-i*.6);}glint(c,7,0,'#f1d7ff',5);}
 else if(fx.type==='capture'){oval(c,fx.x,fx.y,25+p*65,15+p*38,null,tc,3);for(let i=0;i<5;i++)glint(c,fx.x+Math.cos(i*TAU/5)*p*60,fx.y+Math.sin(i*TAU/5)*p*40,tc,4);}
 else if(fx.type==='gateCollapse'){for(let i=0;i<12;i++){const a=i*2.399,x=fx.x+Math.cos(a)*p*120,y=fx.y+Math.sin(a)*p*65;c.save();c.translate(x,y-Math.sin(p*Math.PI)*55);c.rotate(a+p*4);rect(c,-7,-4,14,8,i%3?'#a7beb8':'#b7827c',INK,1,1);c.restore();}}
 else{for(let i=0;i<4;i++)oval(c,fx.x+Math.sin(i*4)*p*24,fx.y-p*25+i*4,4+p*13,3+p*8,'#b4a58c66',null);}
 c.restore();return true;
}

export function drawWarfarePlacement(renderer,c,state){
 const p=state.pointer;if(!p)return;const mode=state.mode,ability=state.ability||state.abilityName||state.abilityType,colors={snipe:GOLD,forcefield:'#53bdb1',division:VIOLET,crash:'#e59162',roll:GOLD,shadowpath:'#b18bdf',patrol:'#66bcb3',intercept:'#66bcb3'};const key=typeof ability==='string'?ability:ability?.id;
 if(mode==='workshop'||mode==='laboratory'){const r=mode==='laboratory'?62:53,size=mode==='laboratory'?116:102,scouted=renderer.pointVisible(p.x,p.y),raw=scouted?renderer.game.canBuild?.(mode,p.x,p.y):false,valid=typeof raw==='object'?raw.ok:raw!==false;c.save();c.globalAlpha=.6;c.translate(p.x,p.y);c.scale(size/50,size/50);drawExpansionIcon(c,mode,team(0),{phase:renderer.time});c.restore();oval(c,p.x,p.y+12,r,r*.5,null,valid?team(0):scouted?COLORS[1]:'#9aa9a5',2);return;}
 if(!colors[key])return;const col=colors[key],selected=(renderer.game.entities||[]).filter(e=>state.selected?.has(e.id)&&e.hp>0),range={snipe:700,forcefield:420,division:600};c.save();
 if(range[key]){c.setLineDash([5,10]);for(const e of selected)if(({snipe:'sniper',forcefield:'mage',division:'divider'})[key]===e.type&&renderer.visible(e.x,e.y,range[key]))oval(c,e.x,e.y,range[key],range[key],null,col+'44',1);c.setLineDash([]);}
 const radius={forcefield:110,division:350,crash:Math.max(75,Math.sqrt(selected.filter(e=>e.type==='airplane').length)*35)}[key];if(radius)oval(c,p.x,p.y,radius,radius,col+'0b',col+'bb',1.8);
 if(key==='division')for(let i=0;i<3;i++){const a=-Math.PI/2+i*TAU/3;stroke(c,[[p.x,p.y],[p.x+Math.cos(a)*350,p.y+Math.sin(a)*350]],col+'aa',1.5);familyMark(c,['rock','paper','scissors'][i],p.x+Math.cos(a+Math.PI/3)*210,p.y+Math.sin(a+Math.PI/3)*210,20);}
 const points=state.routePoints||state.abilityPoints||state.pathPoints||state.path||[];if(Array.isArray(points)&&points.length){const route=points.map(v=>Array.isArray(v)?v:[v.x,v.y]);if(route.every(v=>v.every(Number.isFinite))){stroke(c,route,col+'33',11);stroke(c,route,col,2);for(const [x,y]of route)oval(c,x,y,3,3,PAPER,col,1);c.setLineDash([6,6]);stroke(c,[route[route.length-1],[p.x,p.y]],col+'a0',1.5);c.setLineDash([]);}}
 if(['roll','shadowpath','intercept'].includes(key))for(const e of selected)stroke(c,[[e.x,e.y],[p.x,p.y]],col+'66',1.3);
 stroke(c,[[p.x-8,p.y],[p.x+8,p.y]],col,2);stroke(c,[[p.x,p.y-8],[p.x,p.y+8]],col,2);oval(c,p.x,p.y,13,13,null,col,1.3);c.restore();
}

export function paintWarfareMap(renderer,c){
 const map=renderer.game.map||{},w=renderer.game.width,h=renderer.game.height;
 if(!['canyon','stapler','staplercanyon','stapler-canyon','conveyor','conveyors','conveyorchaos','conveyor-chaos'].includes(map.id)&&!map.conveyors&&!map.canyons)return;
 if((map.id||'').includes('conveyor')||map.conveyors?.length){
  // Recycled factory floor, expansion pads and lightweight printed safety markings.
  c.save();c.globalCompositeOperation='source-atop';for(let y=100;y<h-80;y+=160)for(let x=100;x<w-80;x+=160){rect(c,x,y,150,150,(Math.round(x/160)+Math.round(y/160))%2?'#c2cec015':'#7e96901b','#8da19822',3,.8);}c.restore();
  for(const b of map.conveyors||[])drawBelt(renderer,c,b,true);
 }else{
  for(const o of map.canyons||map.chasms||map.obstacles?.filter(o=>['canyon','chasm'].includes(o.type))||[]){const{x,y,w,h}=o;rect(c,x+9,y+12,w,h,'#5a544953',null,3);const depth=c.createLinearGradient(x,y,x+w,y);depth.addColorStop(0,'#b18e60');depth.addColorStop(.17,'#796c58');depth.addColorStop(.45,'#454d48');depth.addColorStop(.6,'#404943');depth.addColorStop(.83,'#786956');depth.addColorStop(1,'#b09168');rect(c,x,y,w,h,depth,'#8e7b54',3,2);c.save();c.beginPath();c.rect(x,y,w,h);c.clip();for(let j=0;j<5;j++){const inset=5+j*8,shade=['#d0b07c','#c4a16d','#a98d63','#8a775a','#75664f'][j];stroke(c,[[x+inset,y],[x+inset+Math.sin(j)*3,y+h]],shade,j<2?4:3);stroke(c,[[x+w-inset,y],[x+w-inset-3,y+h]],shade,j<2?4:3);}for(let yy=y+20;yy<y+h;yy+=34){const bend=Math.sin(yy*.027)*5;stroke(c,[[x+2,yy],[x+28,yy+5],[x+55,yy+12+bend]],'#d3b783',1.3);stroke(c,[[x+w-2,yy+11],[x+w-27,yy+17],[x+w-55,yy+23-bend]],'#c3a77a',1.3);if(Math.round(yy/34)%3===0){path(c,[[x+40,yy],[x+62,yy+10],[x+48,yy+26],[x+34,yy+12]],'#a68c63','#685f4e',1);path(c,[[x+w-41,yy+13],[x+w-61,yy+21],[x+w-46,yy+36],[x+w-32,yy+28]],'#998160','#685f4e',1);}}for(let i=0;i<Math.floor(h/110);i++){const xx=x+w*.4+Math.sin(i*2.7)*w*.12,yy=y+i*110;path(c,[[xx,yy],[xx+12,yy+7],[xx+5,yy+18],[xx-9,yy+9]],'#657060',null);}c.restore();}
  for(const b of map.bridges||[]){const{x,y,w,h}=b;rect(c,x+4,y+10,w,h,'#736d5560',null,3);rect(c,x,y,w,h,'#dec88f','#968351',2,2);const vertical=h>w,length=vertical?h:w,card=b.type==='cardboardbridge';for(let i=12;i<length-6;i+=12){const major=i%60===0;if(vertical)stroke(c,[[x+3,y+i],[x+(card?w-6:major?Math.min(w*.45,30):12),y+i]],card?'#b69b66':'#7d7754',major?1.8:1);else stroke(c,[[x+i,y+3],[x+i,y+(card?h-6:major?Math.min(h*.45,30):12)]],card?'#b69b66':'#7d7754',major?1.8:1);}stroke(c,[[x+3,y+3],[x+w-3,y+3]],'#fff0b9',2);stroke(c,[[x+3,y+h-3],[x+w-3,y+h-3]],'#aa925b',2);if(!card){for(let i=60;i<w-30;i+=60){c.fillStyle='#8d8158';c.font='bold 13px "Trebuchet MS",sans-serif';c.textAlign='center';c.fillText(String(i/60),x+i,y+h*.35);}}else{for(const dx of [w*.23,w*.7]){c.save();c.translate(x+dx,y+h*.45);c.rotate(dx<w*.5?-.18:.14);rect(c,-20,-h*.4,40,h*.8,'#f0dbaa77',null,1);c.restore();}}}
 }
}

function beltBounds(b){return {x:b.x??Math.min(b.from?.x??0,b.to?.x??0),y:b.y??Math.min(b.from?.y??0,b.to?.y??0),w:b.w??b.width??120,h:b.h??b.height??120};}
function beltDirection(b){let d=b.direction||b.dir||b.vector||{x:b.dx??1,y:b.dy??0};if(Array.isArray(d))return {x:d[0],y:d[1]};if(typeof d==='number')return {x:Math.cos(d),y:Math.sin(d)};if(typeof d==='string')return ({east:{x:1,y:0},west:{x:-1,y:0},north:{x:0,y:-1},south:{x:0,y:1},right:{x:1,y:0},left:{x:-1,y:0},up:{x:0,y:-1},down:{x:0,y:1}})[d]||{x:1,y:0};return d;}
function drawBelt(renderer,c,b,base=false){const {x,y,w,h}=beltBounds(b),d=beltDirection(b),a=Math.atan2(d.y||0,d.x||0),vertical=Math.abs(d.y||0)>Math.abs(d.x||0),t=renderer.time||0,reversed=b.reversed||(b.reversedUntil||0)>(renderer.game.time||0),sign=reversed?-1:1;
 if(base){rect(c,x-8,y-8,w+16,h+16,'#526a6b','#3d555c',5,2);rect(c,x,y,w,h,'#8e9e95','#405a60',3,2);if(vertical){rect(c,x-5,y,7,h,'#bac4ad',INK,1,1);rect(c,x+w-2,y,7,h,'#bac4ad',INK,1,1);for(const yy of [y+7,y+h-7]){rect(c,x+4,yy-4,w-8,8,'#aabcae','#506d70',3,1);}}else{rect(c,x,y-5,w,7,'#bac4ad',INK,1,1);rect(c,x,y+h-2,w,7,'#bac4ad',INK,1,1);for(const xx of [x+7,x+w-7])rect(c,xx-4,y+4,8,h-8,'#aabcae','#506d70',3,1);}return;}
 if(!renderer.visible(x+w/2,y+h/2,Math.max(w,h)/2+20))return;
 c.save();c.beginPath();c.rect(x,y,w,h);c.clip();const offset=(t*40*sign*(vertical?(d.y||1):(d.x||1)))%38;if(vertical){for(let yy=y-40;yy<y+h+40;yy+=38)stroke(c,[[x,yy+offset],[x+w,yy+offset]],'#4f696b',2);}else for(let xx=x-40;xx<x+w+40;xx+=38)stroke(c,[[xx+offset,y],[xx+offset,y+h]],'#4f696b',2);
 const count=Math.max(1,Math.floor((vertical?h:w)/115));for(let i=0;i<count;i++){const p=(i+.5)/count,xx=x+(vertical?w/2:w*p),yy=y+(vertical?h*p:h/2);c.save();c.translate(xx,yy);c.rotate(a+(reversed?Math.PI:0));stroke(c,[[-16,-13],[0,0],[-16,13]],reversed?'#edc28a':'#e4e9c5',4);c.restore();}c.restore();
}

export function drawWarfareObstacle(renderer,c,o){
 if(!['machine','recycler','cardboard'].includes(o.type))return false;
 const{x,y,w,h}=o;c.save();c.translate(x,y);
 if(o.type==='cardboard'){
  rect(c,4,9,w,h,'#71604a33',null,5);rect(c,0,0,w,h,'#c5aa78','#927e54',4,2);rect(c,4,3,w-8,h-13,'#dfc797','#ab8c57',3,1);for(let j=12;j<h-15;j+=12)stroke(c,[[5,j],[w-5,j]],'#c1a575',1);path(c,[[w*.2,0],[w*.45,0],[w*.51,h],[w*.27,h]],'#e9d9b2aa',null);stroke(c,[[w*.35,h*.25],[w*.65,h*.25]],'#8a815b',3);
 }else{
  rect(c,8,13,w,h,'#3b555039',null,12);rect(c,0,0,w,h,'#79958f','#456267',8,3);rect(c,5,-5,w-10,h*.3,'#d2d8c1','#617e79',6,2.5);rect(c,w*.1,h*.34,w*.8,h*.55,'#aebdb0','#506d6d',5,2);
  if(o.type==='recycler'){
   rect(c,w*.21,h*.05,w*.57,h*.11,'#405c60',INK,3,2);for(let i=0;i<10;i++)stroke(c,[[w*.24+i*w*.05,h*.06],[w*.24+i*w*.05,h*.14]],'#a9c0b5',2);const cx=w*.5,cy=h*.59,r=Math.min(w,h)*.2;oval(c,cx,cy,r,r,'#789b8e','#4f7470',2);for(let i=0;i<3;i++){const a=-Math.PI/2+i*TAU/3;c.save();c.translate(cx,cy);c.rotate(a);path(c,[[0,-r*.75],[r*.54,r*.23],[r*.3,r*.1],[r*.17,r*.36],[-r*.1,r*.2],[0,0],[-r*.2,-r*.14]],'#cee0bb','#567c6e',1);c.restore();}rect(c,w*.76,h*.43,w*.12,h*.23,'#465f65',INK,2,1);for(let i=0;i<3;i++)oval(c,w*.82,h*(.48+i*.06),3,3,i===1?GOLD:'#a9dbbd',INK,.8);
  }else{
   rect(c,w*.12,h*.38,w*.47,h*.46,'#48646b',INK,4,2);for(let j=0;j<5;j++)stroke(c,[[w*.16,h*(.43+j*.075)],[w*.55,h*(.43+j*.075)]],'#819d96',3);rect(c,w*.66,h*.37,w*.22,h*.27,'#425d64',INK,3,1.7);rect(c,w*.69,h*.4,w*.16,h*.16,'#abd6b7',null,2);stroke(c,[[w*.71,h*.5],[w*.75,h*.46],[w*.8,h*.49],[w*.82,h*.43]],'#5b9783',1.7);gear(c,w*.77,h*.75,Math.min(w,h)*.11,.2,'#c5ba8f');
  }
  for(const xx of [w*.08,w*.91]){oval(c,xx,h*.1,3,3,'#6a8682',INK,1);stroke(c,[[xx-2,h*.1],[xx+2,h*.1]],'#c0cbbb',1);}stroke(c,[[w*.1,h*.98],[w*.9,h*.98]],'#d2c28e',4);for(let i=0;i<6;i++)stroke(c,[[w*(.1+i*.13),h*.96],[w*(.17+i*.13),h*.99]],'#566c68',4);
 }c.restore();return true;
}

export function animateWarfareMap(renderer,c){
 for(const b of renderer.game.map?.conveyors||[])drawBelt(renderer,c,b,false);
 for(const o of renderer.game.map?.obstacles||[])if(o.type==='machine'&&renderer.visible(o.x+o.w/2,o.y+o.h/2,Math.max(o.w,o.h))){gear(c,o.x+o.w*.77,o.y+o.h*.75,Math.min(o.w,o.h)*.11,renderer.time*.4,'#c5ba8f');}
}
