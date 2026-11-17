import {billboardBasis,ELEVATION,projectHeading,projectWorld} from './world-camera.js';
import {getAnimationFrame} from './warfare-art.js';
import {terrainHeight} from './terrain.js';
import {wallConnections} from './walls.js';
import {actorLift,actorTerrainHeight} from './flight-visual.js';

const CELL=128, PAGE=2048, PER_ROW=PAGE/CELL;
const PALETTE=[[.14,.86,.80],[1,.43,.29],[.69,.47,.94],[1,.77,.25]];
const WHITE=[1,1,1];
const unitSize={rock:49,paper:49,scissors:49,twig:49,sniper:59,airplane:66,mage:60,divider:85,rollingrock:90,shears:66};
const structureSize={workshop:111,laboratory:136,station:82,wall:50,desk:112,printer:94,cdreader:98,mouse:80,cabinet:74};
const ART_SIZE=76;
const FACING_STEP=Math.PI/8;
const BASE_TYPES=new Set(['twig','rock','paper','scissors']);
const SHEET_TYPES=new Set(['sniper','airplane','mage','divider','rollingrock','shears']);
const DECAL=new Set(['@shade','@ring','@selected']);
const QUAD=[0,1,2,0,2,3];

function sizeOf(e){return unitSize[e.type]||structureSize[e.type]||(e.type==='staplergate'?(e.r||180)*100/68:68);}
function elevation(map,x,y){return terrainHeight(map||{},x,y)*ELEVATION;}
export function actorBounds(renderer,e){
 const z=actorTerrainHeight(renderer.game.map,e)*ELEVATION+1.5;
 const g=projectWorld(renderer.camera,renderer.width,renderer.height,e.x,e.y,z);
 const box=renderer.billboards?.bounds.get(e.id);
 const nominal=sizeOf(e),scale=renderer.camera.zoom;
 const [left,bottom,width,height]=box?.dimensions||[-nominal*.51,0,nominal*1.02,nominal];
 const lift=actorLift(e,renderer.time||0);
 return {x:g.x+left*scale,y:g.y-(height+bottom+lift)*scale,width:width*scale,height:height*scale,depth:g.depth+(box?.depthOffset||0)};
}
function compile(gl,type,source){
 const shader=gl.createShader(type);gl.shaderSource(shader,source);gl.compileShader(shader);
 if(!gl.getShaderParameter(shader,gl.COMPILE_STATUS))throw Error(gl.getShaderInfoLog(shader));
 return shader;
}
function makeProgram(gl){
 const program=gl.createProgram();
 const vs=compile(gl,gl.VERTEX_SHADER,'attribute vec3 p;attribute vec2 uv;attribute vec4 tint;uniform mat4 view;varying vec2 v;varying vec4 color;void main(){gl_Position=view*vec4(p,1.);v=uv;color=tint;}');
 const fs=compile(gl,gl.FRAGMENT_SHADER,'precision mediump float;uniform sampler2D art;varying vec2 v;varying vec4 color;void main(){vec4 s=texture2D(art,v)*color;if(s.a<.035)discard;gl_FragColor=s;}');
 gl.attachShader(program,vs);gl.attachShader(program,fs);gl.linkProgram(program);gl.deleteShader(vs);gl.deleteShader(fs);
 if(!gl.getProgramParameter(program,gl.LINK_STATUS))throw Error(gl.getProgramInfoLog(program));
 return program;
}
function drawDecal(c,kind){
 if(kind==='@shade'){const g=c.createRadialGradient(64,64,1,64,64,59);g.addColorStop(0,'rgba(12,18,22,.76)');g.addColorStop(.45,'rgba(12,18,22,.43)');g.addColorStop(1,'rgba(12,18,22,0)');c.fillStyle=g;c.fillRect(0,0,CELL,CELL);return;}
 c.strokeStyle='white';c.lineWidth=kind==='@selected'?4:3;c.beginPath();c.arc(64,64,55,0,Math.PI*2);c.stroke();
 if(kind==='@selected'){c.lineWidth=2;for(let i=0;i<4;i++){c.beginPath();c.arc(64,64,60,i*Math.PI/2-.16,i*Math.PI/2+.16);c.stroke();}}
}
export class ActorBillboards {
 constructor(gl,paint){
  this.gl=gl;this.paint=paint;this.program=makeProgram(gl);
  this.buffer=gl.createBuffer();this.cache=new Map();this.bounds=new Map();this.pages=[];
  this.cell=document.createElement('canvas');this.cell.width=this.cell.height=CELL;this.c=this.cell.getContext('2d',{willReadFrequently:true});
  this.attrs=['p','uv','tint'].map(n=>gl.getAttribLocation(this.program,n));
  this.view=gl.getUniformLocation(this.program,'view');this.art=gl.getUniformLocation(this.program,'art');
 }
 addCell(key,type,team,opts){
  const cached=this.cache.get(key);if(cached)return cached;
  const gl=this.gl,c=this.c;const idx=this.cache.size;const page=Math.floor(idx/(PER_ROW*PER_ROW));
  if(!this.pages[page]){const texture=gl.createTexture();gl.bindTexture(gl.TEXTURE_2D,texture);gl.texImage2D(gl.TEXTURE_2D,0,gl.RGBA,PAGE,PAGE,0,gl.RGBA,gl.UNSIGNED_BYTE,null);
   gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_MIN_FILTER,gl.LINEAR);gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_MAG_FILTER,gl.LINEAR);gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_WRAP_S,gl.CLAMP_TO_EDGE);gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_WRAP_T,gl.CLAMP_TO_EDGE);this.pages.push(texture);}
  c.clearRect(0,0,CELL,CELL);
  if(DECAL.has(type))drawDecal(c,type);
  else if(opts.crumpled){
   c.fillStyle='#faf3d8';c.strokeStyle='#394b48';c.lineWidth=3;c.beginPath();
   for(let i=0;i<12;i++){const a=i*Math.PI/6,r=i%2?24:29;const x=64+Math.cos(a)*r,y=64+Math.sin(a)*r;i?c.lineTo(x,y):c.moveTo(x,y);}c.closePath();c.fill();c.stroke();
   c.strokeStyle='#a5a995';c.lineWidth=2;for(let i=0;i<5;i++){const a=i*Math.PI*.4;c.beginPath();c.moveTo(64+Math.cos(a)*22,64+Math.sin(a)*22);c.lineTo(64+Math.cos(a+.6)*8,64+Math.sin(a+.6)*8);c.stroke();}
  }else this.paint(c,type,64,64,ART_SIZE,team,opts);
  let l=0,t=0,r=CELL-1,b=CELL-1,contact=CELL-1;
  if(!DECAL.has(type)){
   const data=c.getImageData(0,0,CELL,CELL).data;l=t=CELL-1;r=b=0;
   for(let y=0;y<CELL;y++)for(let x=0;x<CELL;x++)if(data[(y*CELL+x)*4+3]>20){l=Math.min(l,x);t=Math.min(t,y);r=Math.max(r,x);b=Math.max(b,y);}
   contact=b;
   const needed=Math.max(2,Math.round((r-l)*.045));
   for(let y=b;y>=t;y--){let solid=0;for(let x=l;x<=r;x++)if(data[(y*CELL+x)*4+3]>=164)solid++;if(solid>=needed){contact=y;break;}}
   l=Math.max(0,l-1);r=Math.min(CELL-1,r+1);t=Math.max(0,t-1);b=Math.min(CELL-1,b+1);
  }
  const cx=(idx%PER_ROW)*CELL,cy=(Math.floor(idx/PER_ROW)%PER_ROW)*CELL;
  gl.bindTexture(gl.TEXTURE_2D,this.pages[page]);gl.pixelStorei(gl.UNPACK_FLIP_Y_WEBGL,false);gl.pixelStorei(gl.UNPACK_PREMULTIPLY_ALPHA_WEBGL,false);
  gl.texSubImage2D(gl.TEXTURE_2D,0,cx,cy,gl.RGBA,gl.UNSIGNED_BYTE,this.cell);
  const tile={page,l,t,r,b,contact,uv:[[(cx+l+.5)/PAGE,(cy+b+.5)/PAGE],[(cx+r+.5)/PAGE,(cy+b+.5)/PAGE],[(cx+r+.5)/PAGE,(cy+t+.5)/PAGE],[(cx+l+.5)/PAGE,(cy+t+.5)/PAGE]]};
  this.cache.set(key,tile);return tile;
 }
 draw(gl,matrix,renderer,entities,selected){
  gl.activeTexture(gl.TEXTURE0);gl.useProgram(this.program);gl.uniformMatrix4fv(this.view,false,matrix);gl.uniform1i(this.art,0);
  gl.bindBuffer(gl.ARRAY_BUFFER,this.buffer);
  for(let i=0;i<this.attrs.length;i++){gl.enableVertexAttribArray(this.attrs[i]);gl.vertexAttribPointer(this.attrs[i],[3,2,4][i],gl.FLOAT,false,36,[0,12,20][i]);}
  gl.disable(gl.CULL_FACE);gl.enable(gl.DEPTH_TEST);gl.enable(gl.BLEND);gl.blendFunc(gl.SRC_ALPHA,gl.ONE_MINUS_SRC_ALPHA);
  const mesh=[],decals=[],map=renderer.game.map,time=renderer.time||0;
  const {right,up}=billboardBasis(renderer.camera);
  this.bounds.clear();
  const quad=(list,tile,points,color,alpha)=>{list.push({page:tile.page,data:QUAD.flatMap(i=>[...points[i],...tile.uv[i],...color,alpha])});};
  const ground=(type,x,y,rx,ry,color,alpha)=>{
   const tile=this.addCell(type,type,0,{});const pts=[[-1,1],[1,1],[1,-1],[-1,-1]].map(([a,b])=>{const px=x+a*rx,py=y+b*ry;return [px,py,elevation(map,px,py)+1.5];});quad(decals,tile,pts,color,alpha);
  };
  const ordered=entities.map(e=>[e,renderer.worldToScreen(e.x,e.y).depth]).sort((a,b)=>a[1]-b[1]);
  for(const [e] of ordered){
   const size=sizeOf(e),movingUnit=!!unitSize[e.type],friendly=e.team>=0,tc=PALETTE[e.team]||WHITE;
   const old=renderer.motion.get(e.id),delta=old?Math.hypot(e.x-old.x,e.y-old.y):0;
   const walk=(old?.walk||0)+delta/12,movedAt=delta>.04?time:old?.movedAt??-1,moving=movingUnit&&time-movedAt<.21;
   renderer.motion.set(e.id,{x:e.x,y:e.y,walk,movedAt});
   const lift=actorLift(e,time),phase=moving?Math.floor(walk*8/(Math.PI*2))%8:0,attack=(e.attackAnim||0)>.08;
   const mask=e.type==='wall'?(renderer.game.elevationAt?wallConnections(renderer.game,e):e.wallMask||0):0;
   const direction=projectHeading(renderer.camera,e.facing||0),aircraft=e.type==='airplane'||e.type==='cd';
   const facing=aircraft?(Math.round(direction/FACING_STEP)+16)%16*FACING_STEP:Math.cos(direction)<0?Math.PI:0;
   const opts={entity:e,battlefield:true,sprite:BASE_TYPES.has(e.type),facing,
    walk:moving?((phase+.1)*Math.PI/4):0,attackAnim:attack?.2:0,phase:phase/8,animationTime:time,
    active:!!e.queue?.length,amount:e.amount,mask,crumpled:e.paperInvulnerable>0};
   const sheetFrame=SHEET_TYPES.has(e.type)?getAnimationFrame(e.type,opts).index:0;
   const key=[e.type,e.team,facing,moving&&!SHEET_TYPES.has(e.type)?phase+1:0,sheetFrame,+attack,+opts.active,+((e.amount??1)>0),mask,+opts.crumpled].join('/');
   const tile=this.addCell(key,e.type,e.team,opts);
   const scale=size/ART_SIZE,w=(tile.r-tile.l)*scale,h=(tile.b-tile.t)*scale,left=(tile.l-64)*scale;
   const bob=moving?Math.abs(Math.sin(walk))*1.9:0;
   const z=actorTerrainHeight(map,e)*ELEVATION+1.5;
   const point=(u,v)=>[e.x+right[0]*u+up[0]*v,e.y+right[1]*u+up[1]*v,z+up[2]*v];
   // A turned aircraft keeps its center anchored as the painted bounds change.
   const base=bob+(aircraft?(64-tile.b)*scale+size*(e.type==='cd'?.45:.35):-(tile.b-tile.contact)*scale);
   const low=lift+base;quad(mesh,tile,[point(left,low),point(left+w,low),point(left+w,low+h),point(left,low+h)],WHITE,e.constructing>0?.55:1);
   this.bounds.set(e.id,{dimensions:[left,base,w,h]});
   const radius=e.type==='wall'?22:Math.min(size*.52,Math.max(size*.25,(e.r||20)*1.1));
   ground('@shade',e.x+radius*.16,e.y+radius*.06,radius*1.45,radius*.88,WHITE,.72);
   if(friendly){const sel=selected.has(e.id);ground(sel?'@selected':'@ring',e.x,e.y,radius*(sel?1.28:1.05),radius*(sel?1.28:1.05),tc,sel?.95:.50);}
  }
  const flush=list=>{let page=-1,vertices=[];const submit=()=>{if(!vertices.length)return;gl.bindTexture(gl.TEXTURE_2D,this.pages[page]);gl.bufferData(gl.ARRAY_BUFFER,new Float32Array(vertices),gl.DYNAMIC_DRAW);gl.drawArrays(gl.TRIANGLES,0,vertices.length/9);vertices=[];};for(const item of list){if(page!==item.page){submit();page=item.page;}vertices.push(...item.data);}submit();};
  gl.depthMask(false);flush(decals);gl.depthMask(true);flush(mesh);gl.disable(gl.BLEND);
  for(const attr of this.attrs)gl.disableVertexAttribArray(attr);
 }
}
