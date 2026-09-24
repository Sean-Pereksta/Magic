// SVG intermediates for the Cloudflare PNG pack. Generated art stays outside git.
// node game/iron-throne/assets/generate-geography.mjs /absolute/output-directory
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { GEOGRAPHY_MASKS } from '../geography-assets.mjs';
import { coastDrawing, riverDrawing, mouthDrawing, drawingSVG } from '../geography-art.mjs';
const directory=path.resolve(process.argv[2]||'geography-export');
await mkdir(directory,{recursive:true});
const files={};
for(const mask of GEOGRAPHY_MASKS) {
  const suffix=mask.toString(16).padStart(2,'0');
  if(mask) for(const style of ['beach','cliff']) files[`coast_${style}_${suffix}.svg`]=coastDrawing(mask,style);
  files[`river_${suffix}.svg`]=riverDrawing(mask);
}
files['river_mouth_overlay.svg']=mouthDrawing();
for(const [name,layers] of Object.entries(files)) await writeFile(path.join(directory,name),drawingSVG(layers));
console.log(`Generated ${Object.keys(files).length} transparent directional SVG intermediates.`);
