import { GeographyCache, maskEdges, neighborId, oppositeEdge } from './geography.mjs';
import { neighbors, distance, passable } from './world-hex.mjs';
const caches=new WeakMap();
export const nodeTile = node => typeof node==='string'?node.slice(node.indexOf(':')+1):null;
export function navalGraph(s) {
  let cache=caches.get(s.tiles);if(!cache){cache={geography:new GeographyCache()};caches.set(s.tiles,cache);}
  const geography=cache.geography.get(s.tiles);if(cache.source===geography)return cache.graph;
  const graph=new Map();
  for(const t of Object.values(s.tiles)){
    if(t.terrain==='water')graph.set(`sea:${t.id}`,[]);
    else if(geography.get(t.id)?.hasRiver)graph.set(`river:${t.id}`,[]);
  }
  const link=(a,b)=>{if(graph.has(a)&&graph.has(b)&&!graph.get(a).includes(b)){graph.get(a).push(b);graph.get(b).push(a);}};
  for(const t of Object.values(s.tiles)){
    if(t.terrain==='water')for(const n of neighbors(s,t))if(n.terrain==='water')link(`sea:${t.id}`,`sea:${n.id}`);
    const g=geography.get(t.id);if(!g?.hasRiver)continue;
    for(const edge of maskEdges(g.riverMask)){
      const id=neighborId(t,edge),n=s.tiles[id];if(!n)continue;
      if(n.terrain==='water'&&(g.mouthMask&(1<<edge)))link(`river:${t.id}`,`sea:${id}`);
      else if(geography.get(id)?.riverMask&(1<<oppositeEdge(edge)))link(`river:${t.id}`,`river:${id}`);
    }
  }
  cache.source=geography;cache.graph=graph;return graph;
}
export const navalNode = (s,tile,graph=navalGraph(s)) => ['sea:','river:'].map(prefix=>prefix+tile).find(id=>graph.has(id))||null;
export function shoreNodes(s,tile,graph=navalGraph(s)) {
  const t=s.tiles[tile];if(!passable(t))return [];
  return [t,...neighbors(s,t)].map(n=>navalNode(s,n.id,graph)).filter(Boolean).sort();
}
export const adjacentShore = (s,f,tile) => passable(s.tiles[tile])&&!!s.tiles[f.tile]&&distance(s.tiles[f.tile],s.tiles[tile])<=1;
export function navalPath(s,start,end,{blocked=new Set(),graph=navalGraph(s)}={}) {
  if(!graph.has(start)||!graph.has(end))return null;
  const queue=[start],previous=new Map([[start,null]]);
  for(let i=0;i<queue.length;i++){
    const at=queue[i];if(at===end){const path=[];for(let p=end;p!==start;p=previous.get(p))path.unshift(p);return path;}
    for(const n of graph.get(at))if(!previous.has(n)&&(!blocked.has(n)||n===end)){previous.set(n,at);queue.push(n);}
  }
  return null;
}
