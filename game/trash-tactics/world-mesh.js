import {ELEVATION} from './world-camera.js';
import {terrainHeight} from './terrain.js';
import {paintLandmarks} from './world-landmarks.js';

const shades = {
 desktop:     [0,5,8,'#e0bf8d','#e6d2a7','#a27e54','#eac994','#b49961'],
 breakroom:   [1,4,0,'#cfdfbf','#d2e2bc','#657c68','#d1d9bd','#9fac88'],
 canyon:      [2,0,7,'#bca385','#dcb289','#745e51','#c8a685','#989b87'],
 conveyor:    [3,2,6,'#9eaa9d','#bfc8bc','#626f68','#a4ac9b','#697d7c'],
 inkdelta:    [4,1,6,'#9cbeb2','#cad3b4','#517673','#add3b6','#658b77'],
 corkcross:   [5,8,3,'#d1b180','#e1c291','#88714f','#ddc08c','#9a7656'],
 serverstack: [6,2,3,'#9cb5ad','#bdc4ba','#566d72','#b5c8b8','#677f7e'],
 rainyard:    [7,2,6,'#8ea5b2','#adb9bb','#586c82','#a7b3ab','#87a1a9'],
 cartonworks: [8,5,3,'#d5b582','#e0ba82','#a17d50','#ddc38a','#bb9866']
};

export const PAINTED_WORLD_ATLASES = Object.freeze({
 default: { path: 'painted-terrain.webp', cols: 3, rows: 3 },
 rainyard: { path: 'terrain-rainyard.webp', cols: 2, rows: 3 },
 cartonworks: { path: 'terrain-cartonworks.webp', cols: 2, rows: 3 }
});

const rgb = hex => [
 Number.parseInt(hex.slice(1,3),16)/255,
 Number.parseInt(hex.slice(3,5),16)/255,
 Number.parseInt(hex.slice(5,7),16)/255
];
const rgba = (hex, alpha = 1) => [...rgb(hex),alpha];
const colorMix = (color,amount) => [color[0]*amount,color[1]*amount,color[2]*amount,color[3]];
const point = (x,y,z) => [x,y,z];
const inside = (x,y,r) => x >= r.x && x <= r.x+r.w && y >= r.y && y <= r.y+r.h;
const overlaps = (a,b) => a.x < b.x+b.w && a.x+a.w > b.x && a.y < b.y+b.h && a.y+a.h > b.y;
const rounds = [0,1,2,0,2,3];
const clockwise = (x,y,a,r,z) => [x+Math.cos(a)*r,y+Math.sin(a)*r,z];
const WALL = 6;
const GROUND = 0;
const SIDE_MATERIAL = .009;
const TOP_MATERIAL = .00265;
const decalHeight = .48;

function material(color, tile = -1, scale = TOP_MATERIAL) {
 return {color:typeof color==='string'?rgba(color):color,tile,scale};
}

function face(mesh, vertices, surface, normal, coordinates) {
 const box = vertices[0];
 const projection = Math.abs(normal[2])>.25
  ? p => [p[0]*surface.scale,p[1]*surface.scale]
  : p => [Math.hypot(p[0]-box[0],p[1]-box[1])*surface.scale,p[2]*surface.scale];
 const tex = coordinates || vertices.map(projection);
 for (const i of vertices.length===3?[0,1,2]:rounds) {
  mesh.push(...vertices[i],...normal,...tex[i],...surface.color,surface.tile);
 }
}

function top(mesh, x,y,w,h,z,surface) {
 if(w<=0 || h<=0)return;
 face(mesh,[[x,y,z],[x+w,y,z],[x+w,y+h,z],[x,y+h,z]],surface,[0,0,1]);
}

function side(mesh,a,b,lowerA,lowerB,upperA,upperB,surface,normal) {
 if(Math.hypot(a[0]-b[0],a[1]-b[1])<.01)return;
 const length=Math.hypot(b[0]-a[0],b[1]-a[1])*surface.scale;
 face(mesh,[[...a,lowerA],[...b,lowerB],[...b,upperB],[...a,upperA]],surface,normal,[
  [0,lowerA*surface.scale],[length,lowerB*surface.scale],
  [length,upperB*surface.scale],[0,upperA*surface.scale]
 ]);
}

function box(mesh, x,y,w,h,z,thick,topMaterial,sideMaterial=topMaterial) {
 if(w<=0 || h<=0 || thick<=0)return;
 const right=x+w,bottom=y+h,up=z+thick;
 side(mesh,[x,y],[right,y],z,z,up,up,sideMaterial,[0,-1,0]);
 side(mesh,[right,y],[right,bottom],z,z,up,up,sideMaterial,[1,0,0]);
 side(mesh,[right,bottom],[x,bottom],z,z,up,up,sideMaterial,[0,1,0]);
 side(mesh,[x,bottom],[x,y],z,z,up,up,sideMaterial,[-1,0,0]);
 top(mesh,x,y,w,h,up,topMaterial);
}

function disc(mesh,x,y,rx,ry,z,surface,n=14) {
 for(let i=0;i<n;i++){
  const a=i*Math.PI*2/n,b=(i+1)*Math.PI*2/n;
  face(mesh,[[x,y,z],[x+rx*Math.cos(a),y+ry*Math.sin(a),z],[x+rx*Math.cos(b),y+ry*Math.sin(b),z]],surface,[0,0,1]);
 }
}

function cylinder(mesh,x,y,r,z,height,surface,sideMaterial,steps=14) {
 for(let i=0;i<steps;i++){
  const a=i*Math.PI*2/steps,b=(i+1)*Math.PI*2/steps,mid=(a+b)/2;
  const start=clockwise(x,y,a,r,z).slice(0,2),end=clockwise(x,y,b,r,z).slice(0,2);
  side(mesh,start,end,z,z,z+height,z+height,sideMaterial,[Math.cos(mid),Math.sin(mid),0]);
  face(mesh,[[x,y,z+height],clockwise(x,y,a,r,z+height),clockwise(x,y,b,r,z+height)],surface,[0,0,1]);
 }
}

function strip(mesh, a, b, width, z, surface) {
 const dx=b[0]-a[0],dy=b[1]-a[1],length=Math.hypot(dx,dy);
 if(length<.01)return;
 const ox=dy*width/(2*length),oy=-dx*width/(2*length);
 face(mesh,[[a[0]+ox,a[1]+oy,z],[b[0]+ox,b[1]+oy,z],[b[0]-ox,b[1]-oy,z],[a[0]-ox,a[1]-oy,z]],surface,[0,0,1],[
  [0,0],[length*surface.scale,0],[length*surface.scale,width*surface.scale],[0,width*surface.scale]
 ]);
}

function footprintShadow(decals,r) {
 for(let i=3;i>0;i--){
  const spread=i*13;
  top(decals,r.x+17-spread,r.y+31-spread,r.w+spread*2,r.h+spread*2,decalHeight,
   material(rgba('#19272e',.11/i)));
 }
}

function floorWithHoles(mesh, region, holes, z, surface) {
 const thisRegion=holes.filter(o=>overlaps(region,o));
 const right=region.x+region.w,bottom=region.y+region.h;
 const xs=[region.x,right],ys=[region.y,bottom];
 for(const r of thisRegion){
  xs.push(Math.max(region.x,r.x),Math.min(right,r.x+r.w));
  ys.push(Math.max(region.y,r.y),Math.min(bottom,r.y+r.h));
 }
 const columns=[...new Set(xs)].sort((a,b)=>a-b),rows=[...new Set(ys)].sort((a,b)=>a-b);
 for(let j=1;j<rows.length;j++)for(let i=1;i<columns.length;i++){
  const x=columns[i-1],y=rows[j-1],w=columns[i]-x,h=rows[j]-y;
  if(!thisRegion.some(r=>inside(x+w/2,y+h/2,r)))top(mesh,x,y,w,h,z,surface);
 }
}

function rampHeight(r,x,y) {
 if(r.side==='west') return ELEVATION*(x-r.x)/r.w;
 if(r.side==='east') return ELEVATION*(1-(x-r.x)/r.w);
 if(r.side==='north')return ELEVATION*(y-r.y)/r.h;
 return ELEVATION*(1-(y-r.y)/r.h);
}

function slopedTile(mesh,r,x,y,w,h,surface,offset = 0) {
 const points=[[x,y],[x+w,y],[x+w,y+h],[x,y+h]];
 const dx=(rampHeight(r,x+w,y)-rampHeight(r,x,y))/w;
 const dy=(rampHeight(r,x,y+h)-rampHeight(r,x,y))/h;
 const d=Math.hypot(dx,dy,1);
 face(mesh,points.map(p=>[...p,rampHeight(r,...p)+offset]),surface,[-dx/d,-dy/d,1/d]);
}

function paintRamp(mesh,r,plateau,palette) {
 const {x,y,w,h}=r;
 const horizontal=r.side==='west'||r.side==='east';
 slopedTile(mesh,r,x,y,w,h,palette.ramp);
 const tread = palette.tread;
 const length=horizontal?w:h;
 for(let offset=18;offset<length-12;offset+=28) {
  if(horizontal)slopedTile(mesh,r,x+offset,y+8,2.8,h-16,tread,.34);
  else slopedTile(mesh,r,x+8,y+offset,w-16,2.8,tread,.34);
 }
 for(const edge of [-1,1]){
  if(horizontal)slopedTile(mesh,r,x+4,y+(edge<0?5:h-10),w-8,5,palette.rampEdge,.33);
  else slopedTile(mesh,r,x+(edge<0?5:w-10),y+4,5,h-8,palette.rampEdge,.33);
 }
 const from=horizontal?x:y,to=from+length;
 const edge=horizontal?(r.side==='west'?plateau.x:plateau.x+plateau.w):(r.side==='north'?plateau.y:plateau.y+plateau.h);
 const ends=[from,Math.max(from,Math.min(to,edge)),to];
 for(let j=1;j<ends.length;j++){
  const start=ends[j-1],end=ends[j];
  const raised=horizontal?(start+end)/2>plateau.x&&(start+end)/2<plateau.x+plateau.w:(start+end)/2>plateau.y&&(start+end)/2<plateau.y+plateau.h;
  for(const edgeSide of [-1,1]) {
   const fixed=horizontal?y+(edgeSide===1?h:0):x+(edgeSide===1?w:0);
   const a=horizontal?[start,fixed]:[fixed,start], b=horizontal?[end,fixed]:[fixed,end];
   const za=rampHeight(r,...a),zb=rampHeight(r,...b);
   side(mesh,a,b,raised?za:0,raised?zb:0,raised?ELEVATION:za,raised?ELEVATION:zb,palette.face,horizontal?[0,edgeSide,0]:[edgeSide,0,0]);
  }
 }
}

function terraceEdges(mesh,map,plateau,palette) {
 const inner=material(colorMix(palette.face.color,.83),palette.face.tile,SIDE_MATERIAL);
 for(const c of map.cliffs||[]){
  if(c.plateauId!==plateau.id||c.w<=0||c.h<=0)continue;
  const {x,y,w,h}=c;
  const vertical=c.side==='west'||c.side==='east'||(!c.side&&h>w);
  const a=vertical?[x+w/2,y-1]:[x-1,y+h/2];
  const b=vertical?[x+w/2,y+h+1]:[x+w+1,y+h/2];
  const positive=c.side==='east'||c.side==='south';
  const normal=vertical?[positive?1:-1,0,0]:[0,positive?1:-1,0];
  side(mesh,a,b,0,0,ELEVATION,ELEVATION,palette.face,normal);
  side(mesh,[a[0]+normal[0]*.44,a[1]+normal[1]*.44],[b[0]+normal[0]*.44,b[1]+normal[1]*.44],
   8,8,16,16,inner,normal);
  side(mesh,[a[0]+normal[0]*.48,a[1]+normal[1]*.48],[b[0]+normal[0]*.48,b[1]+normal[1]*.48],
   ELEVATION-10,ELEVATION-10,ELEVATION-3,ELEVATION-3,palette.edge,normal);
  const length=Math.hypot(b[0]-a[0],b[1]-a[1]);
  for(let along=25;along<length-19;along+=122){
   const px=a[0]+(b[0]-a[0])*along/length,py=a[1]+(b[1]-a[1])*along/length;
   const inset=10,tx=px-normal[0]*inset,ty=py-normal[1]*inset;
   top(mesh,tx-3,ty-3,6,6,ELEVATION+.36,palette.edge);
   const fixtureX=px+normal[0]*2,fixtureY=py+normal[1]*2;
   const fixtureW=vertical?8:9,fixtureH=vertical?9:8;
   box(mesh,fixtureX-fixtureW/2,fixtureY-fixtureH/2,fixtureW,fixtureH,18,ELEVATION-31,palette.edge,inner);
   if(plateau.campTeam!==undefined&&along%2===1&&along<45){
    box(mesh,tx-7,ty-7,14,14,ELEVATION,12,palette.edge,palette.face);
   }
  }
 }
}

function stampCamp(mesh,camp,palette,seat) {
 const [x,y]=[camp.x,camp.y],z=ELEVATION+.4;
 const team=material(rgba(['#479e9e','#da815c','#aa83be','#d7b16a'][seat]||'#bbac7e',.86));
 top(mesh,x-149,y-122,298,244,z,palette.camp);
 for(const sx of [-1,1])for(const sy of [-1,1]){
  const xx=x+sx*143,yy=y+sy*115;
  strip(mesh,[xx,yy],[xx-sx*29,yy],4,z+.21,team);
  strip(mesh,[xx,yy],[xx,yy-sy*29],4,z+.21,team);
  top(mesh,xx-2,yy-2,4,4,z+.45,palette.edge);
 }
}

function stampResourceField(mesh,map,site,palette) {
 const {x,y}=site,dims={x:x-120,y:y-115,w:240,h:230};
 if((map.ramps||[]).some(r=>overlaps(dims,r)))return;
 const z=terrainHeight(map,x,y)*ELEVATION+.39;
 const margin=site.role==='outer'?120:105;
 const bracket=site.role==='natural'?palette.rampEdge:palette.edge;
 for(const sx of [-1,1])for(const sy of [-1,1]){
  const a=x+sx*margin,b=y+sy*102;
  strip(mesh,[a,b],[a-sx*19,b],3.5,z,bracket);
  strip(mesh,[a,b],[a,b-sy*18],3.5,z,bracket);
 }
}

function breakGround(mesh,o,mapId,palette) {
 const {x,y,w,h}=o;
 const wet=o.type!=='canyon'&&o.type!=='chasm';
 const depth=wet?-63:-132;
 const fluid=material(mapId==='rainyard'?'#3b6b7d':o.type==='inkpool'?'#315259':'#263e46',wet?-2:-1);
 top(mesh,x,y,w,h,depth,fluid);
 side(mesh,[x,y],[x+w,y],depth,depth,0,0,palette.face,[0,1,0]);
 side(mesh,[x+w,y],[x+w,y+h],depth,depth,0,0,palette.face,[-1,0,0]);
 side(mesh,[x+w,y+h],[x,y+h],depth,depth,0,0,palette.face,[0,-1,0]);
 side(mesh,[x,y+h],[x,y],depth,depth,0,0,palette.face,[1,0,0]);
 if(wet){
  for(const xx of [x+9,x+w-12]){
   box(mesh,xx,y,4,h,-8,8,palette.tread,palette.face);
   const runoff=mapId==='rainyard'?palette.ink:palette.face;
   for(let yy=y+48;yy<y+h-24;yy+=157)top(mesh,xx-1,yy,7,13,GROUND+.15,runoff);
  }
 }
}

function spanBridge(mesh,r,palette) {
 const {x,y,w,h}=r;
 top(mesh,x,y,w,h,.62,palette.path);
 // Seams, marked shoulders and rivets sit flush so the entire crossing stays usable.
 const alongX=w>=h;
 const len=alongX?w:h;
 for(let i=18;i<len-12;i+=33){
  if(alongX)top(mesh,x+i,y+5,2,h-10,.78,palette.tread);
  else top(mesh,x+5,y+i,w-10,2,.78,palette.tread);
 }
 for(const fraction of [.08,.92]){
  if(alongX)strip(mesh,[x+8,y+h*fraction],[x+w-8,y+h*fraction],3,.88,palette.edge);
  else strip(mesh,[x+w*fraction,y+8],[x+w*fraction,y+h-8],3,.88,palette.edge);
 }
}

function paintBelts(mesh,map,palette,now) {
 for(const belt of map.conveyors||[]){
  const {x,y,w,h}=belt;
  const horizontal=Math.abs(belt.dx)>Math.abs(belt.dy),reverse=belt.reversedUntil>now;
  const travel=(horizontal?belt.dx:belt.dy)*(reverse?-1:1);
  const px=horizontal?x+6:x+17,py=horizontal?y+17:y+6;
  const right=horizontal?x+w-6:x+w-17,bottom=horizontal?y+h-17:y+h-6;
  top(mesh,x,y,w,h,.33,palette.propWall);
  const tread=material('#6f7b75',-3);
  const xy=[[px,py],[right,py],[right,bottom],[px,bottom]];
  const uv=xy.map(p=>horizontal?[(p[0]-px)*travel/38,(p[1]-py)/52]:[(p[1]-py)*travel/38,(p[0]-px)/52]);
  face(mesh,xy.map(p=>[...p,.52]),tread,[0,0,1],uv);
  const a=horizontal?px:py,b=horizontal?right:bottom;
  for(let q=a+18;q<b-12;q+=64){
   for(const side of [0,1]){
    const xx=horizontal?q:side?right+8:px-9,yy=horizontal?(side?bottom+8:py-9):q;
    top(mesh,xx,yy,5.2,5.2,.56,palette.edge);
   }
  }
 }
}

function boundsOfRoute(a,b,width){
 return {x:Math.min(a[0],b[0])-width/2,y:Math.min(a[1],b[1])-width/2,
  w:Math.abs(b[0]-a[0])+width,h:Math.abs(b[1]-a[1])+width};
}

function paintRoutes(mesh,map,holes,palette) {
 // Draw the tactical routes as material changes rather than diagram strokes.
 for(const [i,lane] of (map.lanes||[]).entries()){
  const points=Array.isArray(lane)?lane:lane.points||[],info=map.laneInfo?.[i],primary=info?.role==='main';
  const width=Math.min(info?.width||400,primary?240:174);
  for(let j=1;j<points.length;j++){
   const a=points[j-1],b=points[j],pathBox=boundsOfRoute(a,b,width);
   if(holes.some(o=>overlaps(o,pathBox)))continue;
   const z=.09+(i%19)*.013;
   const road=primary?palette.mainPath:palette.path;
   strip(mesh,a,b,width,z,road);
   disc(mesh,a[0],a[1],width/2,width/2,z,road,10);
   if(j===points.length-1)disc(mesh,b[0],b[1],width/2,width/2,z,road,10);
   if(primary){
    const d=Math.hypot(b[0]-a[0],b[1]-a[1]);
    for(let offset=55;offset<d-33;offset+=154){
     const p=offset/d,q=(offset+28)/d;
     strip(mesh,[a[0]+(b[0]-a[0])*p,a[1]+(b[1]-a[1])*p],[a[0]+(b[0]-a[0])*q,a[1]+(b[1]-a[1])*q],
      3,z+.019,palette.tread);
    }
   }
  }
 }
}

function cargo(mesh,o,palette) {
 const {x,y,w,h}=o;
 const columns=w>520?3:w>260?2:1;
 const rows=h>530?2:1;
 const pw=w/columns,ph=h/rows;
 for(let row=0;row<rows;row++)for(let col=0;col<columns;col++){
  const xx=x+col*pw+2,yy=y+row*ph+2,topZ=62+(row*51+col*47)%118;
  box(mesh,xx,yy,pw-4,ph-4,0,topZ,palette.prop,palette.propWall);
  top(mesh,xx+pw*.37,yy,pw*.195,ph-4,topZ+.31,palette.tape);
  top(mesh,xx+pw*.17,yy+ph*.64,pw*.23,ph*.16,topZ+.38,palette.label);
  for(let bar=0;bar<6;bar++)top(mesh,xx+pw*.187+bar*pw*.032,yy+ph*.66,pw*.011,ph*.11,topZ+.47,palette.ink);
  if(row===0&&col%2===0) {
   const slabH=28+(col%3)*18;
   box(mesh,xx+pw*.10,yy+ph*.07,pw*.73,ph*.43,topZ+.06,slabH,palette.propLight,palette.propWall);
   top(mesh,xx+pw*.39,yy+ph*.07,pw*.16,ph*.43,topZ+slabH+.31,palette.tape);
  }
 }
}

function machinery(mesh,o,palette,mapId) {
 const {x,y,w,h}=o;
 const height=(o.type==='serverrack'?113:77)+Math.min(35,w*.025);
 const ribs=Math.max(2,Math.floor(w/112));
 box(mesh,x,y,w,h,0,height,palette.prop,palette.propWall);
 box(mesh,x+7,y+7,w-14,h-14,height,8,palette.propLight,palette.propWall);
 const step=(w-25)/ribs;
 for(let i=0;i<ribs;i++){
  const xx=x+12+i*step,yy=y+14,panels=Math.max(1,Math.floor(h/140));
  const ph=(h-25)/panels;
  for(let j=0;j<panels;j++){
   const by=yy+j*ph;
   top(mesh,xx,by,step-9,ph-9,height+8.3,palette.ink);
   for(let l=0;l<5;l++){
    const ly=by+9+l*(ph-20)/5;
    const near=xx+6,far=xx+step-16;
    top(mesh,near,ly,far-near,3,height+9.9,palette.hardware);
    side(mesh,[near,ly+3],[far,ly+3],height+8.4,height+8.4,height+9.9,height+9.9,palette.propWall,[0,1,0]);
   }
   top(mesh,xx+5,by+4,9,4,height+8.7,mapId==='rainyard'?palette.light:palette.indicator);
  }
 }
 for(const yy of [y-3,y+h-2])top(mesh,x+2,yy,w-4,5,.34,palette.edge);
}

function ordinaryProp(mesh,o,palette) {
 const {x,y,w,h,type}=o;
 const tuck=Math.min(w,h)*.1;
 if(type==='cup'||type==='tape'){
  const r=Math.min(w,h)*.48,cx=x+w/2,cy=y+h/2;
  const lift=type==='cup'?125:60;
  box(mesh,x,y,w,h,0,11,palette.propWall,palette.ink);
  cylinder(mesh,cx,cy,r,6,lift,palette.prop,palette.propWall);
  cylinder(mesh,cx,cy,r*.86,lift+6,4,palette.propLight,palette.propWall);
  cylinder(mesh,cx,cy,r*.67,lift+10,2,palette.ink,palette.propWall);
  if(type==='cup')box(mesh,cx+r*.61,cy-r*.24,r*.68,r*.48,26,r*.31,palette.propLight,palette.propWall);
  return;
 }
 const z=type==='ruler'?21:type==='book'?52:45;
 box(mesh,x,y,w,h,0,z,palette.prop,palette.propWall);
 box(mesh,x+2,y+2,w-4,h-4,z,z*.18,palette.propLight,palette.propWall);
 if(type==='ruler'){
  const length=Math.max(w,h),horizontal=w>=h;
  for(let p=12;p<length-8;p+=20){
   if(horizontal)top(mesh,x+p,y+8,1.8,p%3===0?13:7,z*1.18+.1,palette.ink);
   else top(mesh,x+8,y+p,p%3===0?13:7,1.8,z*1.18+.1,palette.ink);
  }
 }else{
  top(mesh,x+tuck,y+tuck,w-tuck*2,h-tuck*2,z*1.18+.1,palette.label);
  top(mesh,x+tuck*2,y+h*.40,w-tuck*4,3,z*1.18+.2,palette.tape);
 }
}

function placeObstacle(mesh,decals,o,palette,id) {
 if(o.w<=0||o.h<=0)return;
 footprintShadow(decals,o);
 if(['cardboard','corkbundle','pinbox','crate','cartonstack','boxstack'].includes(o.type))cargo(mesh,o,palette);
 else if(['machine','serverrack','recycler','culvert','rainrail','drainbox','sorter'].includes(o.type))machinery(mesh,o,palette,id);
 else ordinaryProp(mesh,o,palette);
}

function dressClearGround(mesh,map,holes,palette) {
 const rain=map.id==='rainyard';
 const specific=rain||map.id==='cartonworks';
 const blockers=[...holes,...(map.plateaus||[]),...(map.obstacles||[])];
 const max=specific?510:320;
 for(let i=0;i<max;i++){
  const x=135+((i+1)*587.1703)%(map.width-270),y=140+((i+9)*343.8271)%(map.height-280);
  if(blockers.some(o=>inside(x,y,o))||(map.camps||[]).some(c=>Math.hypot(x-c.x,y-c.y)<440))continue;
  if((map.expansions||[]).some(c=>Math.hypot(x-c.x,y-c.y)<130))continue;
  const size=9+(i%8)*1.5;
  if(rain&&i%11===0){
   disc(mesh,x,y,size*2.25,size*.72,.405,palette.puddle,9);
   strip(mesh,[x-size*.82,y-size*.12],[x+size*.85,y-size*.12],1.3,.427,palette.light);
  }else{
   const angle=i*1.726,dx=Math.cos(angle)*size,dy=Math.sin(angle)*size;
   strip(mesh,[x-dx,y-dy],[x+dx,y+dy],i%4===0?size*.58:2.9,.424,i%3===0?palette.tread:palette.scrap);
  }
 }
}

function mapPalette(theme,useCustom) {
 const [base,upper,faceTile,baseTint,upperTint,faceTint,roadTint,propTint]=shades[theme]||shades.desktop;
 const [ground,deck,wall,ramp,road,prop]=useCustom?[0,1,2,3,4,5]:[base,upper,faceTile,upper,base,faceTile];
 const rain=theme==='rainyard';
 const neutral=rain?'#829695':'#ba9c6d';
 return {
  ground:material(baseTint,ground+16),
  deck:material(upperTint,deck),
  face:material(faceTint,wall,SIDE_MATERIAL),
  ramp:material(neutral,ramp,.0034),
  rampEdge:material(rain?'#9ba67b':'#b89865'),
  edge:material(rain?'#b0c4bd':'#d2bb95'),
  tread:material(rain?'#657b82':'#a18058'),
  light:material(rain?'#cbd6bb':'#ecd4a2'),
  camp:material(rain?'#98aaaa':'#d1b58a',deck,.003),
  path:material(roadTint,road,.0042),
  mainPath:material(colorMix(rgba(roadTint),1.11),road,.0041),
  prop:material(propTint,prop,.0048),
  propWall:material(faceTint,prop,.0048),
  propLight:material(colorMix(rgba(propTint),1.17),prop,.0048),
  tape:material(rain?'#86988e':'#cfaf75'),
  hardware:material(rain?'#9aada9':'#b4ad8b'),
  label:material(rain?'#95a9a8':'#ccb688',prop,.006),
  indicator:material(rain?'#bad49c':'#d8c78c'),
  ink:material(rain?'#435960':'#675647'),
  scrap:material(rain?'#728182':'#c3a774'),
  puddle:material('#637e8b',-2)
 };
}

export function buildTerrainMesh(map,useCustomAtlas = false,now=0) {
 const mesh=[],decals=[],palette=mapPalette(map.id,useCustomAtlas);
 const extent={x:0,y:0,w:map.width,h:map.height};
 const holes=(map.obstacles||[]).filter(o=>['inkpool','canyon','chasm','stormdrain','channel'].includes(o.type));
 floorWithHoles(mesh,extent,holes,0,palette.ground);
 for(const o of holes)breakGround(mesh,o,map.id,palette);
 for(const r of map.bridges||[])spanBridge(mesh,r,palette);
 paintRoutes(mesh,map,holes,palette);
 paintBelts(mesh,map,palette,now);
 for(const p of map.plateaus||[]){
  const associated=p.ramps||[];
  footprintShadow(decals,p);
  floorWithHoles(mesh,p,associated,ELEVATION,palette.deck);
  terraceEdges(mesh,map,p,palette);
  for(const r of associated)paintRamp(mesh,r,p,palette);
 }
 for(const [seat,camp] of (map.camps||[]).entries())stampCamp(mesh,camp,palette,seat);
 for(const site of map.expansions||[])stampResourceField(mesh,map,site,palette);
 for(const o of map.obstacles||[]){
  if(!['cliff','gatebarrier','staplergate','gate'].includes(o.type)&&!holes.includes(o))placeObstacle(mesh,decals,o,palette,map.id);
 }
 paintLandmarks({
  customAtlas:useCustomAtlas,material,
  face:(...args)=>face(mesh,...args), box:(...args)=>box(mesh,...args),
  top:(...args)=>top(mesh,...args), wall:(...args)=>side(mesh,...args),
  strip:(...args)=>strip(mesh,...args), shadow:(...args)=>strip(decals,...args),
  glow:(...args)=>disc(decals,...args)
 },map,holes);
 dressClearGround(mesh,map,holes,palette);
 const sum=13;
 return { solids:new Float32Array(mesh), translucent:new Float32Array(decals), stride:sum };
}
