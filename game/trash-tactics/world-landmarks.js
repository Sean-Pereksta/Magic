import {ELEVATION} from './world-camera.js';

const TAU=Math.PI*2;
const add=(a,b)=>a.map((n,i)=>n+b[i]);
const sub=(a,b)=>a.map((n,i)=>n-b[i]);
const mul=(a,n)=>a.map(v=>v*n);
const cross=(a,b)=>[a[1]*b[2]-a[2]*b[1],a[2]*b[0]-a[0]*b[2],a[0]*b[1]-a[1]*b[0]];
const norm=a=>mul(a,1/(Math.hypot(...a)||1));
const along=(a,b,t)=>add(a,mul(sub(b,a),t));

function tube(w,a,b,radius,finish,sides=8,endRadius=radius,cap=true) {
 const axis=norm(sub(b,a)), side=norm(cross(axis,Math.abs(axis[2])>.91?[0,1,0]:[0,0,1]));
 const up=cross(side,axis);
 const cheek=(i,r)=>mul(add(mul(side,Math.cos(i*TAU/sides)),mul(up,Math.sin(i*TAU/sides))),r);
 for(let i=0;i<sides;i++){
  const next=i+1, ap=add(a,cheek(i,radius)),aq=add(a,cheek(next,radius)),bp=add(b,cheek(i,endRadius)),bq=add(b,cheek(next,endRadius));
  w.face([ap,aq,bq,bp],finish,norm(cheek(i+.5,1)));
  if(cap){
   w.face([b,bp,bq],finish,axis);
   w.face([a,aq,ap],finish,mul(axis,-1));
  }
 }
}

function wire(w,a,b,finish,r=2.1){tube(w,a,b,r,finish,5);}

function bend(w,center,radius,from,to,tubeRadius,finish) {
 const sections=12,sides=14;
 const normal=(a,b)=>[Math.cos(a)*Math.cos(b),Math.sin(b),Math.sin(a)*Math.cos(b)];
 const ring=(a,b)=>add(add(center,[Math.cos(a)*radius,0,Math.sin(a)*radius]),mul(normal(a,b),tubeRadius));
 for(let i=0;i<sections;i++){
  const a=from+(to-from)*i/sections,aa=from+(to-from)*(i+1)/sections;
  for(let j=0;j<sides;j++){
   const b=j*TAU/sides,bb=(j+1)*TAU/sides;
   w.face([ring(a,b),ring(aa,b),ring(aa,bb),ring(a,bb)],finish,normal((a+aa)/2,(b+bb)/2));
  }
 }
}

function groundShadow(w,a,b,diameter,ground=0) {
 const project=([x,y,z])=>[x+Math.max(0,z-ground)*.62,y+Math.max(0,z-ground)*.73];
 const start=project(a),end=project(b);
 for(const shell of [1.42,1.16,1]){
  const k=shell===1?.20:.064;
  w.shadow(start,end,diameter*shell,ground+.64,w.material([.053,.082,.096,k]));
 }
}

function finishes(w,mapId) {
 const [deck,wall,prop]=w.customAtlas?[1,2,5]:mapId==='rainyard'?[2,6,6]:[5,3,3];
 const painted=(color)=>w.material(color,prop+32,.0105);
 return {
  dark:painted('#243a40'), brass:painted('#cfaa54'), amber:painted('#e4b958'),
  teal:painted('#2d6264'), sea:painted('#587d77'), oxide:painted('#a65c40'),
  iron:painted('#4e5554'),
  cargo:w.material('#b38851',prop,.0068), container:w.material('#775d3d',prop,.0075),
  seal:w.material('#f0d592'), lamp:w.material('#f6e7ae',-4), glass:w.material('#75b6a5'),
  porcelain:w.material('#c6bda3',deck,.006), grate:w.material('#637375',wall,.008)
 };
}

function crate(w,cx,cy,z,width,depth,height,f) {
 const x=cx-width/2,y=cy-depth/2;
 w.box(x,y,width,depth,z,height,f.cargo,f.container);
 const band=width*.103;
 for(const x0 of [x+width*.24,x+width*.69]){
  w.top(x0,y,band,depth,z+height+.32,f.seal);
  w.wall([x0,y+depth+.3],[x0+band,y+depth+.3],z,z,z+height,z+height,f.dark,[0,1,0]);
 }
 w.top(x+width*.08,y+depth*.18,width*.133,depth*.27,z+height+.39,f.porcelain);
}

function gantry(w,position,direction,scale,f,compact=false) {
 const [x,y,foot]=position,d=direction,s=scale;
 const P=(xx,yy,z)=>[x+d*xx*s,y+yy*s,foot+z*s];
 const boomRoot=P(-.4,-8,335), tip=P(390,-8,375);
 if(compact){
  // This assembly bolts onto an existing cliff, above the running surface.
  w.box(x-30*s,y-6*s,60*s,12*s,foot-52*s,67*s,f.teal,f.dark);
 }else{
  w.box(x-94*s,y-79*s,188*s,158*s,foot,36*s,f.grate,f.dark);
  tube(w,P(0,0,36),P(0,0,63),69*s,f.iron,14);
 }
 for(const yy of [-47,47]){
  const a=P(-39,yy,compact?0:62),b=P(-20,yy,322),c=P(34,yy,62),k=P(29,yy,322);
  tube(w,a,b,13.5*s,f.teal,4);tube(w,c,k,13.5*s,f.teal,4);
  tube(w,a,k,8.5*s,f.sea,4);
  tube(w,c,b,8*s,f.sea,4);
  for(const level of [108,192,263]){
   tube(w,P(-34,yy,level),P(32,yy,level),7*s,f.amber,4);
  }
 }
 for(const xx of [-28,31])tube(w,P(xx,-54,318),P(xx,54,318),9*s,f.dark,4);
 const operator=P(9,0,321);
 w.box(operator[0]-34*s,operator[1]-30*s,68*s,60*s,operator[2],30*s,f.teal,f.dark);
 w.box(operator[0]-24*s,operator[1]-31*s,48*s,12*s,operator[2]+14*s,14*s,f.glass,f.glass);
 const top=P(12,-8,425);
 tube(w,boomRoot,top,14*s,f.teal,4);
 for(const rail of [-25,25]){
  const start=P(-104,rail,331),end=P(390,rail,375);
  tube(w,start,end,13*s,f.brass,4);
  tube(w,top,end,4.6*s,f.dark,5);
  tube(w,top,start,4.6*s,f.dark,5);
  for(const pos of [18,94,170,246,322]){
   const z=345+pos*.085;
   tube(w,P(pos,rail,z),P(pos+36,rail,370+pos*.074),5.5*s,f.dark,4);
  }
 }
 for(const xx of [3,65,150,240,325,385]){
  const z=344+xx*.083;
  tube(w,P(xx,-25,z),P(xx,25,z),4.9*s,f.teal,4);
 }
 const tail=P(-123,0,302);
 w.box(tail[0]-39*s,tail[1]-48*s,78*s,96*s,tail[2],38*s,f.oxide,f.dark);
 w.top(tail[0]-33*s,tail[1]-42*s,66*s,84*s,tail[2]+38*s+.3,f.brass);
 const hook=P(299,-8,compact?210:183), rail=P(299,-8,371);
 tube(w,P(299,-33,373),P(299,22,373),17*s,f.teal,10);
 wire(w,rail,hook,f.dark,3.1*s);
 tube(w,hook,P(299,-8,153+(compact?31:0)),9*s,f.amber,7);
 const lower=compact?150:126,load=P(299,-8,lower);
 crate(w,load[0],load[1],load[2],79*s,68*s,40*s,f);
 for(const yy of [-40,24]){
  const claw=P(299,yy,lower+15),inner=P(299,yy+(yy<0?10:-10),lower+7);
  wire(w,hook,claw,f.dark,4.5*s);
  tube(w,claw,inner,4.5*s,f.iron,5);
 }
 const shadowGround=compact?0:foot;
 groundShadow(w,P(-45,0,60),boomRoot,104*s,shadowGround);
 groundShadow(w,boomRoot,tip,72*s,shadowGround);
 groundShadow(w,P(273,-8,lower+42),P(322,-8,lower+42),54*s,shadowGround);
}

function handWheel(w,center,radius,f) {
 const mark=a=>[center[0]+Math.cos(a)*radius,center[1],center[2]+Math.sin(a)*radius];
 for(let i=0;i<18;i++)tube(w,mark(i*TAU/18),mark((i+1)*TAU/18),radius*.106,f.oxide,7,radius*.106,false);
 for(let i=0;i<5;i++)tube(w,center,mark(i*TAU/5),radius*.079,f.brass,6);
 tube(w,add(center,[0,-14,0]),add(center,[0,9,0]),radius*.2,f.iron,10);
}

function stackedPump(w,o,f) {
 const x=o.x+o.w*.24,y=o.y+o.h*.51,span=o.w*.53,size=Math.min(o.h*.22,71);
 const base=135;
 for(const [i,xx] of [x,x+span].entries()){
  tube(w,[xx,y,base-12],[xx,y,base+18],size*1.15,f.iron,13);
  tube(w,[xx,y,base+18],[xx,y,base+128+i*31],size,f.teal,16,size*.78);
  for(const z of [base+27,base+105+i*19])tube(w,[xx,y,z],[xx,y,z+11],size*.94,f.sea,14);
  tube(w,[xx,y,base+120+i*25],[xx,y,base+177+i*24],size*.35,f.oxide,10);
  tube(w,[xx,y,base+177+i*24],[xx,y,base+192+i*24],size*.47,f.iron,10);
 }
 const pipeZ=base+150,r=20;
 tube(w,[x,y,pipeZ],[x+span,y,pipeZ],r,f.oxide,12);
 tube(w,[x,y+size*.68,base+68],[x+span,y+size*.68,base+68],r*.65,f.brass,12);
 tube(w,[x+span*.3,y,pipeZ],[x+span*.69,y+size*.67,base+68],r*.65,f.sea,12);
 for(const t of [.13,.46,.74,.9]){
  const a=x+span*t;
  tube(w,[a,y,pipeZ],[a+11,y,pipeZ],r*1.22,f.iron,10);
 }
 const hub=[x+span*.62,y+size*.77,base+112];
 tube(w,[hub[0],y,hub[2]],hub,r*.67,f.iron,8);
 handWheel(w,hub,size*.57,f);
 groundShadow(w,[x,y,base+182],[x+span,y,base+180],size*2);
}

function floodwall(w,o,f) {
 const {x,y,w:wide,h}=o;
 const high=165,ix=x+wide*.24,jx=x+wide*.78;
 const topY=y+(y<2600?h-89:89);
 const floor=-63,wet=topY+14;
 const radius=43,curve=56;
 for(const [i,px] of [ix,jx].entries()){
  w.box(px-57,wet-49,114,98,floor,41,f.grate,f.dark);
  tube(w,[px,wet,floor+37],[px,wet,high+(i?0:21)],58,f.teal,14,47);
  tube(w,[px,wet,62],[px,wet,74],57,f.sea,13);
  tube(w,[px,wet,high-22],[px,wet,high-8],55,f.brass,14);
 }
 tube(w,[ix+curve,wet,high+curve],[jx-curve,wet,high+curve],radius,f.oxide,14);
 bend(w,[ix+curve,wet,high],curve,Math.PI,Math.PI/2,radius,f.oxide);
 bend(w,[jx-curve,wet,high],curve,Math.PI/2,0,radius,f.oxide);
 for(const xx of [ix+curve+27,jx-curve-30]){
  tube(w,[xx-8,wet,high+curve],[xx+8,wet,high+curve],radius*1.12,f.dark,13);
 }
 const drainX=ix+(jx-ix)*.55;
 tube(w,[drainX-41,wet,high+curve],[drainX+72,wet-46,63],21,f.brass,11);
 tube(w,[drainX,wet,high+curve],[drainX,wet+59,high+curve],16,f.iron,8);
 handWheel(w,[drainX,wet+78,high+curve],40,f);
 groundShadow(w,[ix,wet,high+curve],[jx,wet,high+curve],125,floor);
}

function bridgeLights(w,bridge,f) {
 const y=bridge.y+(bridge.y<2500?bridge.h-17:17),safeEnd=bridge.x+bridge.w-54;
 for(let x=bridge.x+58;x<safeEnd;x+=185){
  w.box(x-14,y-8,28,16,.8,8.5,f.iron,f.dark);
  tube(w,[x,y,7],[x,y,68],4.5,f.dark,6);
  tube(w,[x,y,66],[x,y+15,66],5,f.dark,6);
  tube(w,[x,y+16,50],[x,y+16,64],9,f.lamp,10,7);
  tube(w,[x,y+16,64],[x,y+16,68],12,f.dark,10,4);
  w.glow(x,y+16,22,14,.79,w.material([.83,.59,.26,.18]));
 }
}

function dockStencil(w,plateau,index,f,rain) {
 const {x,y,w:wide,h}=plateau;
 const stroke=7,span=27,height=48,z=ELEVATION+.71;
 const cx=x+wide*.28,cy=y+h*.77;
 if((plateau.ramps||[]).some(r=>cx>r.x-55&&cx<r.x+r.w+55&&cy>r.y-33&&cy<r.y+r.h+33))return;
 const paint=rain?f.porcelain:f.teal;
 const segments=[[0,0,span,stroke],[span-stroke,0,stroke,height/2],[span-stroke,height/2,stroke,height/2],
  [0,height-stroke,span,stroke],[0,height/2,stroke,height/2],[0,0,stroke,height/2],[0,(height-stroke)/2,span,stroke]];
 const digits=[[0,1,2,3,4,5],[1,2],[0,1,6,4,3],[0,1,6,2,3],[5,6,1,2],[0,5,6,2,3],[0,5,6,4,2,3],[0,1,2],[0,1,2,3,4,5,6],[0,1,2,3,5,6]];
 for(const [slot,digit] of [0,index%10].entries())for(const n of digits[digit]){
  const [sx,sy,sw,sh]=segments[n];
  w.top(cx+slot*(span+11)+sx,cy+sy,sw,sh,z,paint);
 }
 for(let j=0;j<5;j++)w.strip([cx-17+j*18,cy+height+14],[cx-4+j*18,cy+height+24],4,z,f.brass);
}

function terracePump(w,p,dir,f) {
 const x=p.x+p.w*(dir>0?.15:.85),y=p.y+p.h;
 const span=115*dir,top=ELEVATION+113;
 w.box(x-16,y-7,32,14,0,ELEVATION+21,f.iron,f.dark);
 tube(w,[x,y+18,72],[x,y+18,top],16,f.teal,10);
 tube(w,[x,y+18,top],[x+span,y+18,top],14,f.oxide,11);
 tube(w,[x+span,y+18,top],[x+span,y+18,ELEVATION+47],17,f.teal,10);
 tube(w,[x+span,y+18,ELEVATION+47],[x+span,y-2,ELEVATION+47],22,f.iron,10);
 const wheel=[x+span*.21,y+32,ELEVATION+68];
 handWheel(w,wheel,25,f);
 wire(w,[x+span*.78,y+18,top],[x,y+9,ELEVATION+2],f.brass,4.7);
 tube(w,[x+7,y+11,top],[x+7,y+11,top+17],12,f.lamp,10,6);
 groundShadow(w,[x,y,top],[x+span,y,top],29);
}

export function paintLandmarks(w,map,holes) {
 if(map.id!=='rainyard'&&map.id!=='cartonworks')return;
 const rain=map.id==='rainyard',f=finishes(w,map.id);
 for(const [index,p] of (map.plateaus||[]).entries()){
  dockStencil(w,p,p.campTeam===undefined?index+1:p.campTeam+1,f,rain);
  if(p.campTeam===undefined) {
   if(!rain&&p.label==='Shipping deck'){
    gantry(w,[p.x+p.w*.135,p.y+p.h,ELEVATION],1,1.04,f,true);
   }
   continue;
  }
  const dir=p.x<map.width/2?1:-1;
  if(rain)terracePump(w,p,dir,f);
  else gantry(w,[p.x+p.w*(dir>0?.15:.85),p.y+p.h,ELEVATION],dir,.57,f,true);
 }
 if(rain){
  for(const o of holes)if(o.h>800&&o.w>700)floodwall(w,o,f);
  for(const bridge of map.bridges||[])bridgeLights(w,bridge,f);
  for(const rack of map.obstacles||[])if(rack.type==='serverrack'&&rack.w>=550)stackedPump(w,rack,f);
 } else {
  for(const o of map.obstacles||[])if(o.type==='cardboard'&&o.w>=600){
   const dir=o.x<map.width/2?1:-1;
   const scale=Math.min(1.2,o.w/850);
   gantry(w,[o.x+o.w*(dir>0?.43:.57),o.y+o.h*.65,0],dir,scale,f);
  }
 }
}
