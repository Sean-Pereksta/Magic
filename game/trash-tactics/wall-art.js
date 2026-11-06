/* Connected office-fortification artwork; its floor footprint is the real grid. */
import {WALL_SIZE,snapWallPoint,wallConnections} from './walls.js';
const INK='#354549',TEAM=['#269b98','#ed795d','#9e7bc5','#d4aa4e'];
const team=t=>TEAM[t]||'#9aa78d';
const clamp=n=>Math.max(0,Math.min(1,n));
function line(c,points,color=INK,width=1){c.beginPath();points.forEach(([x,y],i)=>i?c.lineTo(x,y):c.moveTo(x,y));c.strokeStyle=color;c.lineWidth=width;c.lineCap='round';c.lineJoin='round';c.stroke();}
function box(c,x,y,w,h,fill,stroke=null,width=1,r=0){c.beginPath();if(r)c.roundRect(x,y,w,h,r);else c.rect(x,y,w,h);if(fill){c.fillStyle=fill;c.fill();}if(stroke){c.strokeStyle=stroke;c.lineWidth=width;c.stroke();}}
function polygon(c,points,fill,stroke=null,width=1){c.beginPath();points.forEach(([x,y],i)=>i?c.lineTo(x,y):c.moveTo(x,y));c.closePath();if(fill){c.fillStyle=fill;c.fill();}if(stroke){c.strokeStyle=stroke;c.lineWidth=width;c.stroke();}}
function screw(c,x,y){box(c,x-1.8,y-1.8,3.6,3.6,'#c5d0c7','#607b7b',.65,1);line(c,[[x-1,y],[x+1,y]],'#657878',.65);}
function clipCorner(c,x,y,dx,dy){polygon(c,[[x,y],[x+dx*10,y],[x+dx*10,y+dy*3],[x+dx*3,y+dy*3],[x+dx*3,y+dy*10],[x,y+dy*10]],'#7e9795',INK,.9);line(c,[[x+dx*1.5,y+dy*8],[x+dx*1.5,y+dy*1.5],[x+dx*8,y+dy*1.5]],'#d8e4d6',.8);screw(c,x+dx*5.5,y+dy*1.5);}
function coupler(c,x,y,vertical,tc){c.save();c.translate(x,y);if(vertical)c.rotate(Math.PI/2);box(c,-6,-3.5,12,7,'#93a9a2',INK,.9,1.3);box(c,-2,-2,4,4,tc,null,1,1);screw(c,-4,0);screw(c,4,0);c.restore();}
const tileCache=new Map();

/** Paint an exact square tile. Neighbor bits remove endcaps, never the solid core. */
export function drawWallBlock(c,options={}){
 // Reuse small detailed tiles instead of replaying hundreds of paths per block
 // in a long fortification. The bounded cache uses at most 8 MiB of pixel data.
 if(typeof CanvasRenderingContext2D!=='undefined'&&c instanceof CanvasRenderingContext2D&&typeof document!=='undefined'){
  const mask=options.mask||0,owner=options.team||0,health=options.health??1,stage=health<.35?2:health<.7?1:0,shadow=options.shadow!==false,key=`${mask}:${owner}:${stage}:${shadow?1:0}`;
  let image=tileCache.get(key);
  if(!image){image=document.createElement('canvas');image.width=128;image.height=128;const ink=image.getContext('2d');ink.scale(2,2);ink.translate(32,32);paintWallBlock(ink,{mask,team:owner,health:[1,.5,.2][stage],shadow,seed:mask});tileCache.set(key,image);if(tileCache.size>128)tileCache.delete(tileCache.keys().next().value);}
  c.save();const scale=(options.size||WALL_SIZE)/WALL_SIZE;c.scale(scale,scale);c.drawImage(image,-32,-32,64,64);c.restore();return;
 }
 paintWallBlock(c,options);
}
function paintWallBlock(c,{mask=0,team:owner=0,health=1,size=WALL_SIZE,shadow=true,seed=0}={}){
 const tc=team(owner),n=!!(mask&1),e=!!(mask&2),s=!!(mask&4),w=!!(mask&8);c.save();c.scale(size/WALL_SIZE,size/WALL_SIZE);c.lineJoin='round';
 if(shadow)box(c,-21,-19,48,48,'#4d50362b',null,1,1);
 // A fully filled 48px cardboard core keeps adjoining tiles visibly sealed.
 const top=c.createLinearGradient(-24,-24,20,24);top.addColorStop(0,'#f2d6a0');top.addColorStop(.35,'#e6c58d');top.addColorStop(1,'#c3a174');box(c,-24,-24,48,48,top);
 // Paper grain, inset corrugated slab and layered front / side faces.
 box(c,-20,-20,40,34,'#eed39a','#ad8c59',.7,1);line(c,[[-18,-17],[16,-17]],'#fff1cd',1);line(c,[[-17,-11],[17,-11]],'#c6a56b66',.7);line(c,[[-17,3],[17,3]],'#c7a76b55',.65);
 if(!s){polygon(c,[[-24,12],[24,12],[24,24],[-24,24]],'#ba9563','#826e4c',1);for(const yy of [15,19,22])line(c,[[-23,yy],[23,yy]],yy===15?'#f1d09c':'#98794e',.8);for(let xx=-19;xx<=20;xx+=6)line(c,[[xx,16],[xx+1.5,20]],'#d8b77d',.75);}
 if(!e){polygon(c,[[19,-24],[24,-24],[24,24],[19,12]],'#a58d65','#826e4c',.7);line(c,[[21,-21],[21,10]],'#d8c08a',.75);}
 if(!w)line(c,[[-23,-23],[-23,23]],'#f8e5ba',2);
 // Wood-pencil coping rails along exposed edges, drawn continuously at joins.
 if(!n){line(c,[[-24,-22],[24,-22]],'#6b634d',6);line(c,[[-24,-23],[24,-23]],'#cfaa65',3.4);line(c,[[-23,-24],[23,-24]],'#fae0a6',1);}
 if(!s){line(c,[[-24,12],[24,12]],'#6b634d',4);line(c,[[-24,11],[24,11]],'#caa365',2);}
 if(!w){line(c,[[-22,-22],[-22,12]],'#7a6a4d',4);line(c,[[-22.5,-21],[-22.5,11]],'#d5b176',2);}
 if(!e){line(c,[[20,-22],[20,12]],'#7a6a4d',3);line(c,[[19.5,-21],[19.5,11]],'#e2c088',1.2);}
 // Team tape and a binder-clip reinforcement make the block an office prop.
 const left=w?-24:-17,right=e?24:17;box(c,left,-8,right-left,4,tc);line(c,[[left,-8],[right,-8]],'#fffbe266',.7);
 if(n||s){box(c,-6,n?-24:-16,4,(s?24:10)-(n?-24:-16),tc+'a6');line(c,[[-6,n?-24:-16],[-6,s?24:10]],'#fff8dc55',.5);}
 c.save();c.translate(7,-2);c.rotate(-.17);box(c,-3,-11,6,21,'#f4e4b78c',null,1,1);line(c,[[-2,-10],[-2,8]],'#fff6d0aa',.5);c.restore();
 polygon(c,[[-6,-3],[5,-3],[7,5],[-8,5]],'#647d7b',INK,.8);line(c,[[-5,-2],[-5,-7],[4,-7],[4,-2]],'#d5ddd1',1.5);line(c,[[-7,5],[6,5]],'#b7c7b8',.7);
 // L plates cap exposed corners; bolted joints link every connected direction.
 if(!n&&!w)clipCorner(c,-23,-23,1,1);if(!n&&!e)clipCorner(c,23,-23,-1,1);if(!s&&!w)clipCorner(c,-23,21,1,-1);if(!s&&!e)clipCorner(c,23,21,-1,-1);
 if(n)coupler(c,-4,-24,true,tc);if(e)coupler(c,24,-6,false,tc);if(s)coupler(c,-4,24,true,tc);if(w)coupler(c,-24,-6,false,tc);
 // The seam stays hairline, without drawing a dark gap through a continuous wall.
 if(e)line(c,[[23.5,-23],[23.5,23]],'#806a4740',.7);if(s)line(c,[[-23,23.5],[23,23.5]],'#806a4740',.7);
 if(health<.7){const damage=clamp((.7-health)/.7);c.save();c.globalAlpha=.45+damage*.5;line(c,[[-15,-16],[-10,-10],[-12,-4],[-5,3],[-7,9]],'#6a604e',1.1);line(c,[[-10,-10],[-5,-12]],'#6a604e',.8);line(c,[[-12,-4],[-17,0]],'#6a604e',.8);polygon(c,[[14,4],[19,6],[18,11],[12,9]],'#9e845b',null);if(health<.35){line(c,[[8,-19],[11,-13],[7,-8],[12,-2],[9,5]],INK,1.2);line(c,[[11,-13],[16,-14]],INK,.8);polygon(c,[[-22,14],[-15,16],[-18,22],[-24,21]],'#7c6c53',null);line(c,[[-1,16],[4,20],[12,17]],'#6a604e',1.1);}c.restore();}
 // Tiny fixed nicks prevent sterile repeated tiles, with no frame-time randomness.
 const nick=(Number(seed)||0)%5;line(c,[[9-nick*2,-15],[13-nick*2,-14]],'#a68a6055',.75);c.restore();
}

function maskFor(game,e){return wallConnections(typeof game.elevationAt==='function'?game:{...game,elevationAt:()=>0,entities:game.entities||[]},e);}

export function drawWallEntity(renderer,c,e,selected){
 const tc=team(e.team),mask=maskFor(renderer.game,e),building=e.constructing>0,progress=building?clamp(1-e.constructing/(e.buildTotal||3)):1,health=e.maxHp?clamp(e.hp/e.maxHp):1;
 c.save();c.translate(e.x,e.y);
 if(building){c.save();c.globalAlpha=.22;drawWallBlock(c,{mask,team:e.team,seed:e.id});c.restore();c.save();c.beginPath();c.rect(-27,24-WALL_SIZE*progress,54,WALL_SIZE*progress+6);c.clip();drawWallBlock(c,{mask,team:e.team,seed:e.id});c.restore();c.save();c.setLineDash([4,3]);box(c,-24,-24,48,48,tc+'0d',tc,1.2);c.restore();}
 else drawWallBlock(c,{mask,team:e.team,health,seed:e.id});
 if(selected){box(c,-25,-25,50,50,null,tc,1.8/renderer.camera.zoom);for(const [x,y,dx,dy]of [[-28,-28,1,1],[28,-28,-1,1],[-28,28,1,-1],[28,28,-1,-1]])line(c,[[x,y+dy*7],[x,y],[x+dx*7,y]],tc,1.5/renderer.camera.zoom);}
 if((selected||health<.999)&&!building){box(c,-18,-31,36,3,'#64716855',null,1,1);box(c,-18,-31,36*health,3,health<.3?'#e67659':tc,null,1,1);}
 if(building){box(c,-19,29,38,3,'#bcbf9f',null,1,1);box(c,-19,29,38*progress,3,'#e6b34d',null,1,1);}
 c.restore();
}

export function drawWallPlacement(renderer,c,state){
 if(state.mode!=='wall'||!state.pointer)return false;
 const p=snapWallPoint(state.pointer.x,state.pointer.y),scouted=renderer.pointVisible(p.x,p.y),raw=scouted?renderer.game.canBuild?.('wall',p.x,p.y):false,valid=typeof raw==='object'?raw.ok:raw!==false,tc=valid?team(0):scouted?'#dc795e':'#9aa9a5';
 const ghost={...p,type:'wall',team:0,hp:1200},mask=scouted?maskFor(renderer.game,ghost):0;c.save();c.translate(p.x,p.y);c.globalAlpha=.68;drawWallBlock(c,{mask,team:0,shadow:false});c.globalAlpha=1;c.setLineDash([4,3]);box(c,-24,-24,48,48,tc+'12',tc,1.4/renderer.camera.zoom);c.setLineDash([]);
 for(const [x,y,dx,dy]of [[-27,-27,1,1],[27,-27,-1,1],[-27,27,1,-1],[27,27,-1,-1]])line(c,[[x,y+dy*6],[x,y],[x+dx*6,y]],tc,1.4/renderer.camera.zoom);
 c.restore();return true;
}
