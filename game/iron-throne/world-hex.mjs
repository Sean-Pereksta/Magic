import { TERRAINS } from './data.mjs';
export const DIRECTIONS = [[1,0],[1,-1],[0,-1],[-1,0],[-1,1],[0,1]];
export const tileId = (q,r) => `${q},${r}`;
export const distance = (a,b) => (Math.abs(a.q-b.q)+Math.abs(a.r-b.r)+Math.abs(a.q+a.r-b.q-b.r))/2;
export const neighbors = (s,t) => DIRECTIONS.map(([q,r])=>s.tiles[tileId(t.q+q,t.r+r)]).filter(Boolean);
export const passable = t => !!t && Number.isFinite(TERRAINS[t.terrain]?.cost);
// Separate streams keep world layout independent of gameplay RNG consumption.
export function hash(seed, ...parts) {
  let n=seed>>>0;
  for(const ch of parts.join(':')){n=Math.imul(n^ch.charCodeAt(0),16777619);n^=n>>>13;}
  n=Math.imul(n^(n>>>16),2246822507);n=Math.imul(n^(n>>>13),3266489909);
  return (n^(n>>>16))>>>0;
}
export function seededRandom(seed) {
  let n=seed>>>0||8147;
  return ()=>{n^=n<<13;n^=n>>>17;n^=n<<5;return (n>>>0)/4294967296;};
}
export const sample = (seed,...parts) => hash(seed,...parts)/4294967296;
