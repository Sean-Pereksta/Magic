import { canonicalMask, maskEdges } from './geography.mjs';

export const GEOGRAPHY_MASKS=Object.freeze([...new Set(Array.from({length:64},(_,mask)=>canonicalMask(mask).mask))]);
const suffix=mask=>mask.toString(16).padStart(2,'0');
// ZIP paths and R2 object keys are identical. Keep the geography/ folder at bucket root.
export const GEOGRAPHY_PATHS=Object.freeze(Object.fromEntries([
  ...GEOGRAPHY_MASKS.filter(Boolean).flatMap(mask=>['beach','cliff'].map(style=>`coast_${style}_${suffix(mask)}`)),
  ...GEOGRAPHY_MASKS.map(mask=>`river_${suffix(mask)}`),
  'river_mouth_overlay'
].map(name=>[name,`geography/${name}.png`])));

export function geographyLayers(g) {
  const layers=[];
  if(g.coastMask) {
    const {mask,rotation}=canonicalMask(g.coastMask),style=['hills','mountain'].includes(g.ground)?'cliff':'beach';
    layers.push({name:`coast_${style}_${suffix(mask)}`,rotation});
  }
  if(g.hasRiver) {
    const {mask,rotation}=canonicalMask(g.riverMask);
    layers.push({name:`river_${suffix(mask)}`,rotation});
    for(const edge of maskEdges(g.mouthMask)) layers.push({name:'river_mouth_overlay',rotation:edge});
  }
  return layers;
}
