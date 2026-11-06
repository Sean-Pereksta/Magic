// A shared clearance graph avoids rebuilding the same ramp routes for each
// member of a squad. Dynamic building footprints invalidate it automatically.
import {conveyorTravelCost} from './battlefields.js';
const DIRS=[[1,0],[-1,0],[0,1],[0,-1],[1,1],[1,-1],[-1,1],[-1,-1]];
const clamp=(v,min,max)=>Math.max(min,Math.min(max,v));
class Frontier {
  constructor(){this.heap=[];this.sequence=0;}
  before(a,b){return a.f<b.f||a.f===b.f&&a.order<b.order;}
  push(id,f,cost){const item={id,f,cost,order:this.sequence++},h=this.heap;let i=h.length;h.push(item);while(i){const p=(i-1)>>1;if(!this.before(item,h[p]))break;h[i]=h[p];i=p;}h[i]=item;}
  pop(){const h=this.heap,first=h[0],last=h.pop();if(h.length){let i=0;while(true){const l=i*2+1;if(l>=h.length)break;const r=l+1,c=r<h.length&&this.before(h[r],h[l])?r:l;if(!this.before(h[c],last))break;h[i]=h[c];i=c;}h[i]=last;}return first;}
}
export function findPath(game,e,tx,ty,ignoreId){
  const buildings=game.entities.filter(b=>b.hp>0&&b.type!=='trash'&&b.queue&&['desk','printer','cabinet','mouse','cdreader','workshop','laboratory','staplergate','station','wall'].includes(b.type));
  // A destroyed wall leaves a 48-pixel breach. Align nodes with wall centers so
  // a normal troop can use that opening instead of missing it between nodes.
  const wallGrid=buildings.some(b=>b.type==='wall'),cell=wallGrid?48:64,offset=wallGrid?0:cell/2,cols=Math.ceil(game.width/cell),rows=Math.ceil(game.height/cell),size=cols*rows;
  const index=value=>wallGrid?Math.round(value/cell):Math.floor(value/cell);
  const key=(x,y)=>y*cols+x,sx=clamp(index(e.x),0,cols-1),sy=clamp(index(e.y),0,rows-1),gx=clamp(index(tx),0,cols-1),gy=clamp(index(ty),0,rows-1);
  const start=key(sx,sy),goal=key(gx,gy);
  const signature=`${game._barrierRevision||0}:`+buildings.map(b=>`${b.id}:${b.x}:${b.y}:${b.r}`).join('|');
  if(!game._navigation||game._navigation.obstacles!==game.map.obstacles||game._navigation.signature!==signature||game._navigation.cols!==cols||game._navigation.rows!==rows){
    game._navigation={obstacles:game.map.obstacles,signature,cell,offset,cols,rows,graphs:new Map()};
  }
  const graphs=game._navigation.graphs,graphKey=`${e.r}:${ignoreId??''}`;
  let graph=graphs.get(graphKey);
  if(!graph){if(graphs.size>=32)graphs.delete(graphs.keys().next().value);graph={clear:new Uint8Array(size),edges:new Uint8Array(size*8)};graphs.set(graphKey,graph);}
  const clear=id=>{if(!graph.clear[id])graph.clear[id]=game._blocked((id%cols)*cell+offset,Math.floor(id/cols)*cell+offset,e.r,ignoreId)?2:1;return graph.clear[id]===1;};
  const frontier=new Frontier(),cost=new Float64Array(size),came=new Int32Array(size),closed=new Uint8Array(size);
  cost.fill(Infinity);came.fill(-1);cost[start]=0;frontier.push(start,Math.hypot(gx-sx,gy-sy),0);
  let found=start,best=Infinity,connected=false;
  while(frontier.heap.length){
    const entry=frontier.pop(),current=entry.id;if(closed[current]||entry.cost!==cost[current])continue;
    const cx=current%cols,cy=Math.floor(current/cols),h=Math.hypot(gx-cx,gy-cy);
    if(h<best){found=current;best=h;}
    const currentX=current===start?e.x:current===goal?tx:cx*cell+offset,currentY=current===start?e.y:current===goal?ty:cy*cell+offset;
    if(h<2.5&&game._lineFree(currentX,currentY,tx,ty,e.r,ignoreId)){found=current;connected=true;break;}
    closed[current]=1;
    for(let direction=0;direction<DIRS.length;direction++){
      const [dx,dy]=DIRS[direction],nx=cx+dx,ny=cy+dy,id=key(nx,ny);
      if(nx<0||ny<0||nx>=cols||ny>=rows||closed[id])continue;
      const nodeX=id===goal?tx:nx*cell+offset,nodeY=id===goal?ty:ny*cell+offset;
      if(id===goal?game._blocked(nodeX,nodeY,e.r,ignoreId):!clear(id))continue;
      if(dx&&dy&&(!clear(key(cx+dx,cy))||!clear(key(cx,cy+dy))))continue;
      const special=current===start||current===goal||id===start||id===goal,edge=current*8+direction;
      if(special){if(!game._lineFree(currentX,currentY,nodeX,nodeY,e.r,ignoreId))continue;}
      else{if(!graph.edges[edge])graph.edges[edge]=game._lineFree(currentX,currentY,nodeX,nodeY,e.r,ignoreId)?1:2;if(graph.edges[edge]===2)continue;}
      const next=cost[current]+(dx&&dy?1.414:1)*conveyorTravelCost(game,nodeX,nodeY,dx,dy);
      if(next<cost[id]){cost[id]=next;came[id]=current;frontier.push(id,next+Math.hypot(gx-nx,gy-ny),next);}
    }
  }
  const path=[];for(let id=found;id!==start&&came[id]!==-1;id=came[id])path.push(id===goal?{x:tx,y:ty}:{x:(id%cols)*cell+offset,y:Math.floor(id/cols)*cell+offset});path.reverse();
  if(connected&&(!path.length||path.at(-1).x!==tx||path.at(-1).y!==ty))path.push({x:tx,y:ty});return path;
}
