// Run from any directory: node game/iron-throne/assets/generate-geography.mjs
// These transparent vector assets use the same drawing definitions as Canvas.
import { mkdir, writeFile } from 'node:fs/promises';
import { canonicalMask } from '../geography.mjs';
import { coastDrawing, riverDrawing, drawingSVG } from '../geography-art.mjs';
const directory=new URL('./geography/',import.meta.url);
await mkdir(directory,{recursive:true});
const masks=[...new Set(Array.from({length:64},(_,mask)=>canonicalMask(mask).mask))];
const files={};
for(const mask of masks) {
  const suffix=mask.toString(16).padStart(2,'0');
  if(mask) for(const style of ['beach','cliff']) files[`coast_${style}_${suffix}.svg`]=coastDrawing(mask,style);
  files[`river_${suffix}.svg`]=riverDrawing(mask);
}
for(let inlet=1;inlet<6;inlet++) files[`river_mouth_${inlet}.svg`]=riverDrawing(1|(1<<inlet),1);
for(const [name,layers] of Object.entries(files)) await writeFile(new URL(name,directory),drawingSVG(layers));
console.log(`Generated ${Object.keys(files).length} transparent directional SVG overlays.`);
