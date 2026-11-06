// Wall geometry is shared by construction, collision, navigation and artwork.
export const WALL_SIZE=48;
export const WALL_TYPE={label:'Wall block',cost:20,hp:1200,r:24,halfSize:24,vision:120,buildTime:3,supplyCap:0,building:true};
export function snapWallPoint(x,y){return {x:Math.round(x/WALL_SIZE)*WALL_SIZE,y:Math.round(y/WALL_SIZE)*WALL_SIZE};}
const boundsCache=new WeakMap();
export function wallBounds(e){const half=e.halfSize||WALL_SIZE/2;let cached=boundsCache.get(e);if(!cached||cached.cx!==e.x||cached.cy!==e.y||cached.half!==half){cached={cx:e.x,cy:e.y,half,bounds:{x:e.x-half,y:e.y-half,w:half*2,h:half*2,left:e.x-half,right:e.x+half,top:e.y-half,bottom:e.y+half}};boundsCache.set(e,cached);}return cached.bounds;}
const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
export function wallPointDistance2(wall,x,y){const b=wallBounds(wall),dx=x-clamp(x,b.left,b.right),dy=y-clamp(y,b.top,b.bottom);return dx*dx+dy*dy;}
export function wallOverlapsPoint(wall,x,y,r=0,padding=2){return wallPointDistance2(wall,x,y)<(r+padding)**2;}
export function wallSegmentBlocked(wall,ax,ay,bx,by,r=0,padding=2){
  const b=wallBounds(wall),expand=r+padding;
  if(Math.max(ax,bx)<b.left-expand||Math.min(ax,bx)>b.right+expand||Math.max(ay,by)<b.top-expand||Math.min(ay,by)>b.bottom+expand)return false;
  if(wallPointDistance2(wall,ax,ay)<expand*expand-1e-7||wallPointDistance2(wall,bx,by)<expand*expand-1e-7)return true;
  const pointSegment2=(px,py,cx,cy,dx,dy)=>{const vx=dx-cx,vy=dy-cy,t=clamp(((px-cx)*vx+(py-cy)*vy)/(vx*vx+vy*vy||1),0,1);return (px-cx-vx*t)**2+(py-cy-vy*t)**2;};
  for(const [cx,cy,dx,dy] of [[b.left,b.top,b.right,b.top],[b.right,b.top,b.right,b.bottom],[b.right,b.bottom,b.left,b.bottom],[b.left,b.bottom,b.left,b.top]]){
    const rx=bx-ax,ry=by-ay,sx=dx-cx,sy=dy-cy,den=rx*sy-ry*sx;
    if(Math.abs(den)>1e-9){const t=((cx-ax)*sy-(cy-ay)*sx)/den,u=((cx-ax)*ry-(cy-ay)*rx)/den;if(t>=0&&t<=1&&u>=0&&u<=1)return true;}
    if(Math.min(pointSegment2(ax,ay,cx,cy,dx,dy),pointSegment2(bx,by,cx,cy,dx,dy),pointSegment2(cx,cy,ax,ay,bx,by),pointSegment2(dx,dy,ax,ay,bx,by))<expand*expand-1e-7)return true;
  }
  return false;
}
const wallIndexes=new WeakMap();
export function wallConnections(game,e){
  let mask=0;const height=game.elevationAt(e.x,e.y);
  let index=wallIndexes.get(game);
  if(!index||index.source!==game.entities||index.length!==game.entities.length||index.revision!==game._barrierRevision){index={source:game.entities,length:game.entities.length,revision:game._barrierRevision,grid:new Map()};for(const wall of game.entities)if(wall.type==='wall')index.grid.set(`${wall.x},${wall.y}`,wall);wallIndexes.set(game,index);}
  for(const [dx,dy,bit] of [[0,-WALL_SIZE,1],[WALL_SIZE,0,2],[0,WALL_SIZE,4],[-WALL_SIZE,0,8]]){const other=index.grid.get(`${e.x+dx},${e.y+dy}`);if(other?.hp>0&&other.team===e.team&&Math.abs(game.elevationAt(other.x,other.y)-height)<=.2)mask|=bit;}
  return mask;
}
const rectOverlap=(a,b)=>a.left<b.x+b.w-1e-6&&a.right>b.x+1e-6&&a.top<b.y+b.h-1e-6&&a.bottom>b.y+1e-6;
export function canPlaceWall(game,x,y){
  const p=snapWallPoint(x,y),b=wallBounds(p);
  if(b.left<0||b.top<0||b.right>game.width||b.bottom>game.height)return false;
  if(game.map.obstacles.some(o=>rectOverlap(b,o))||game.map.ramps.some(o=>rectOverlap(b,o)))return false;
  const height=game.elevationAt(p.x,p.y);
  for(const [px,py] of [[b.left+1,b.top+1],[b.right-1,b.top+1],[b.left+1,b.bottom-1],[b.right-1,b.bottom-1]])if(Math.abs(game.elevationAt(px,py)-height)>.2)return false;
  for(const e of game.entities){
    if(e.hp<=0||!game.isStructure(e)&&e.type!=='trash')continue;
    if(e.type==='wall'){if(rectOverlap(b,wallBounds(e)))return false;}
    else if(wallPointDistance2(p,e.x,e.y)<(e.r+8)**2)return false;
  }
  return true;
}
export function wallBuildPosition(game,wall,worker){
  const distance=WALL_SIZE/2+worker.r+8;
  const choices=[{x:wall.x,y:wall.y-distance},{x:wall.x+distance,y:wall.y},{x:wall.x,y:wall.y+distance},{x:wall.x-distance,y:wall.y}]
    .filter(p=>!game._blocked(p.x,p.y,worker.r)).sort((a,b)=>Math.hypot(a.x-worker.x,a.y-worker.y)-Math.hypot(b.x-worker.x,b.y-worker.y));
  for(const p of choices){
    if(game._lineFree(worker.x,worker.y,p.x,p.y,worker.r))return p;
    const path=game._path(worker,p.x,p.y);if(path.length&&Math.hypot(path.at(-1).x-p.x,path.at(-1).y-p.y)<8)return p;
  }
  return null;
}
export function structureBuildPosition(game,structure,worker){
  if(structure.type==='wall')return wallBuildPosition(game,structure,worker);
  const distance=structure.r+worker.r+10,choices=[];
  for(let i=0;i<16;i++){const a=i*Math.PI/8,p={x:structure.x+Math.cos(a)*distance,y:structure.y+Math.sin(a)*distance};if(!game._blocked(p.x,p.y,worker.r))choices.push(p);}
  choices.sort((a,b)=>Math.hypot(a.x-worker.x,a.y-worker.y)-Math.hypot(b.x-worker.x,b.y-worker.y));
  for(const p of choices){if(game._lineFree(worker.x,worker.y,p.x,p.y,worker.r))return p;const path=game._path(worker,p.x,p.y);if(path.length&&Math.hypot(path.at(-1).x-p.x,path.at(-1).y-p.y)<8)return p;}return null;
}
export function wallBuilderCanReach(game,wall,worker){
  // Include the proposed blueprint when checking its build faces. Existing
  // queued blocks are already physical, so a closing enclosure cannot sell a
  // later plan which its selected worker would be unable to reach.
  const probe={...wall,type:'wall',hp:1200,r:24,id:-1,queue:[]},geometry=Object.create(game);
  geometry.entities=[...game.entities,probe];geometry._collisionBuildings=null;geometry._navigation=null;
  let source=worker;
  if(wallOverlapsPoint(probe,source.x,source.y,source.r)){const free=geometry._freePoint(source.x,source.y,source.r);source={...source,...free};}
  const position=wallBuildPosition(geometry,probe,source);if(!position)return null;
  const planned=[...(worker.order.type==='build'?[worker.order.targetId]:[]),...(worker.buildQueue||[])];
  for(const id of planned){const target=game.getEntity(id);if(target?.constructing&&!structureBuildPosition(geometry,target,source))return null;}
  return position;
}
