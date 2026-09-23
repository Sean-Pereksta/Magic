import { BUILDINGS, RESOURCES, UNITS } from './data.mjs';
// All runtime artwork paths originate here; files are committed beside the game.
const url=path=>new URL(`./assets/${path}.svg`,import.meta.url).href;
export const ART = {
  structures:Object.fromEntries(Object.entries(BUILDINGS).map(([id,b])=>[id,Object.fromEntries(b.levels.map(l=>[l.level,url(`structures/${id}_${l.level}`)]))])),
  units:Object.fromEntries(Object.keys(UNITS).map(id=>[id,url(`units/${id}`)])),
  resources:Object.fromEntries(RESOURCES.map(id=>[id,url(`resources/${id}`)])),
  titles:Object.fromEntries(['kingdom','trade','construction','army','battle','diplomacy','treasury','great_projects'].map(id=>[id,url(`titles/${id}`)])),
  construction:[1,2,3].map(n=>url(`construction/stage_${n}`))
};
export class AssetCache {
  constructor(redraw=()=>{}){this.images=new Map();this.failed=new Set();this.redraw=redraw;}
  get(url){
    if(!url||this.failed.has(url))return null;
    let image=this.images.get(url);
    if(!image){image=new Image();image.onload=()=>this.redraw();image.onerror=()=>{this.failed.add(url);this.images.delete(url);};image.src=url;this.images.set(url,image);if(this.images.size>128)this.images.delete(this.images.keys().next().value);}
    return image.complete&&image.naturalWidth?image:null;
  }
  draw(c,url,x,y,w,h){const image=this.get(url);if(!image)return false;c.drawImage(image,x,y,w,h);return true;}
}
