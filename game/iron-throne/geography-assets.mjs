import { canonicalMask, maskEdges } from './geography.mjs';

// Four coast drawings and five river drawings, with rotations already baked
// into real PNG files. Unrepresented topologies retain the exact native art.
export const GEOGRAPHY_MASKS=Object.freeze([...new Set(Array.from({length:64},(_,mask)=>canonicalMask(mask).mask))]);
export const TILE_SHAPES=Object.freeze({
  coast_single:{mask:1,rotations:6},coast_corner:{mask:3,rotations:6},
  coast_bay:{mask:7,rotations:6},coast_point:{mask:31,rotations:6},
  river_source:{mask:1,rotations:6},river_bend:{mask:5,rotations:6},
  river_straight:{mask:9,rotations:3},river_fork:{mask:21,rotations:2},
  river_mouth:{mask:9,rotations:6}
});
const key=(shape,rotation)=>`${shape}_${String(rotation*60).padStart(3,'0')}`;
export const GEOGRAPHY_PATHS=Object.freeze(Object.fromEntries(
  Object.entries(TILE_SHAPES).flatMap(([shape,{rotations}])=>Array.from({length:rotations},(_,r)=>{
    const name=key(shape,r);return [name,`geography-v2/${name}.png`];
  }))
));
const coasts={1:'coast_single',3:'coast_corner',7:'coast_bay',31:'coast_point'};
const rivers={1:'river_source',5:'river_bend',9:'river_straight',21:'river_fork'};
const imageLayer=(shape,rotation)=>({name:key(shape,rotation%TILE_SHAPES[shape].rotations),rotation:0});
export function geographyLayers(g) {
  const layers=[];
  if(g.coastMask) {
    const {mask,rotation}=canonicalMask(g.coastMask),shape=coasts[mask];
    if(!shape) return []; // Never substitute a picture facing different water.
    layers.push(imageLayer(shape,rotation));
  }
  if(g.hasRiver) {
    if(g.mouthMask) {
      const edges=maskEdges(g.mouthMask),edge=edges[0];
      // The mouth drawing is a complete west-to-east channel, not an overlay.
      if(edges.length!==1||g.riverMask!==((1<<edge)|(1<<((edge+3)%6)))) return [];
      layers.push(imageLayer('river_mouth',edge));
    } else {
      const {mask,rotation}=canonicalMask(g.riverMask),shape=rivers[mask];
      if(!shape) return [];
      layers.push(imageLayer(shape,rotation));
    }
  }
  return layers;
}
