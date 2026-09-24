// Cloudflare directional artwork has a local, connection-preserving fallback.
import { GEOGRAPHY_PATHS } from './geography-assets.mjs';
import { SpriteOutlines } from './sprite-outline.mjs';
import { BUILDINGS, RESOURCES, UNITS } from './data.mjs';
export const IRON_THRONES_ASSET_BASE = 'https://pub-47f679f65f034fbda4c4b2ee31b3818a.r2.dev';
export const ironThronesAsset = path => `${IRON_THRONES_ASSET_BASE}/${path.replace(/^\/+/, '')}`;
const local = path => new URL(`./assets/${path}.svg`, import.meta.url).href;
// City tiers reuse the supplied city artwork; no additional PNGs are required.
const buildingPath = (id,level) => `buildings/${id === 'intelligenceOffice' ? 'chancery' : id}_${id === 'city' ? 1 : level}.png`;
const levels = (id, b) => [...new Set(b.levels.map(l => buildingPath(id,l.level)))];
export const IRON_THRONES_ART = {
  buildings: Object.fromEntries(Object.entries(BUILDINGS).map(([id,b]) => [id, levels(id,b)])),
  troops: Object.fromEntries(Object.keys(UNITS).map(id => [id, `troops/${id}.png`])),
  terrain: Object.fromEntries(['plains','forest','hills','mountain','water'].map(id => [id, Array.from({length:6},(_,i) => `terrain/${id}_${String(i+1).padStart(2,'0')}.png`)])),
  resources: Object.fromEntries(RESOURCES.map(id => [id, `resources/${id}.png`])),
  geography: GEOGRAPHY_PATHS,
  construction: [1,2,3].map(n => `construction/stage_${n}.png`),
  overlays: Object.fromEntries(['bridge_wood','bridge_stone'].map(id => [id, `overlays/${id}.png`]))
};
const flatten = value => typeof value === 'string' ? [value] : Object.values(value).flatMap(flatten);
export const ALL_ART_PATHS = [...new Set(flatten(IRON_THRONES_ART))];
export const ART = {
  structures: Object.fromEntries(Object.entries(BUILDINGS).map(([id,b]) => [id, Object.fromEntries(b.levels.map(l => [l.level, ironThronesAsset(buildingPath(id,l.level))]))])),
  units: Object.fromEntries(Object.entries(IRON_THRONES_ART.troops).map(([id,p]) => [id,ironThronesAsset(p)])),
  resources: Object.fromEntries(Object.entries(IRON_THRONES_ART.resources).map(([id,p]) => [id,ironThronesAsset(p)])),
  terrain: Object.fromEntries(Object.entries(IRON_THRONES_ART.terrain).map(([id,ps]) => [id,ps.map(ironThronesAsset)])),
  overlays: Object.fromEntries(Object.entries(IRON_THRONES_ART.overlays).map(([id,p]) => [id,ironThronesAsset(p)])),
  geography: Object.fromEntries(Object.entries(GEOGRAPHY_PATHS).map(([name,path])=>[name,ironThronesAsset(path)])),
  titles: Object.fromEntries(['kingdom','trade','construction','army','battle','diplomacy','treasury','great_projects'].map(id => [id,local(`titles/${id}`)])),
  construction: IRON_THRONES_ART.construction.map(ironThronesAsset)
};
const fallbacks = new Map();
for (const [id,b] of Object.entries(BUILDINGS)) for (const l of b.levels) fallbacks.set(ART.structures[id][l.level],local(`structures/${id === 'intelligenceOffice' ? 'chancery' : id}_${id === 'city' ? 1 : l.level}`));
for (const id of Object.keys(UNITS)) fallbacks.set(ART.units[id],local(`units/${id}`));
for (const id of RESOURCES) fallbacks.set(ART.resources[id],local(`resources/${id}`));
ART.construction.forEach((url,i) => fallbacks.set(url,local(`construction/stage_${i+1}`)));
// Shared for preload, map and DOM. Never evict decoded manifest images while playing.
const records = new Map();
export function loadArt(url, {timeout=10000} = {}) {
  if (records.has(url)) return records.get(url).promise;
  const record = {image:null,failed:false,promise:null};
  records.set(url,record);
  record.promise = new Promise(resolve => {
    const image = new Image(); let done = false;
    const finish = ok => {
      if (done) return; done = true; clearTimeout(timer); image.onload = image.onerror = null;
      record.image = ok ? image : null; record.failed = !ok;
      if (!ok) { image.src = ''; console.warn('[Iron Thrones Art] FAILED:',url); }
      else if (globalThis.IRON_THRONES_ART_DEBUG) console.info('[Iron Thrones Art] Loaded:',url);
      resolve(record.image);
    };
    const timer = setTimeout(() => finish(false),timeout);
    image.onload = async () => {
      try { if (image.decode) await image.decode(); finish(image.naturalWidth > 0 && image.naturalHeight > 0); }
      catch { finish(false); }
    };
    image.onerror = () => finish(false);
    // Canvas display needs no CORS grant; no pixel readback/export is performed.
    image.src = url;
  });
  return record.promise;
}
export const fallbackArtURL = url => fallbacks.get(url);
export function displayArtURL(url) { return records.get(url)?.failed ? fallbacks.get(url) || '' : url; }
export function installArtFallbacks(root) {
  root.addEventListener('error', event => {
    const img = event.target;
    if (img.tagName !== 'IMG') return;
    const fallback = fallbacks.get(img.src);
    if (fallback) img.src = fallback;
    else if (img.dataset.ironArt !== undefined) img.hidden = true;
  }, true);
}
export async function preloadAllArt(onProgress=()=>{}, {concurrency=6,timeout=10000,budget=45000}={}) {
  let completed=0,failed=0;
  const deadline=Date.now()+budget;
  const total=ALL_ART_PATHS.length;
  const load = async path => {
    const image = await loadArt(ironThronesAsset(path),{timeout:Math.max(1,Math.min(timeout,deadline-Date.now()))});
    if (!image) failed++;
    onProgress({completed:++completed,total,failed,path});
  };
  onProgress({completed,total,failed,path:'buildings/lumber_1.png'});
  // Probe the requested lumber image first, then load every remaining PNG.
  await load('buildings/lumber_1.png');
  const queue=ALL_ART_PATHS.filter(p=>p!=='buildings/lumber_1.png'); let index=0;
  await Promise.all(Array.from({length:Math.max(1,Math.min(12,concurrency))},async()=>{
    while(index<queue.length) await load(queue[index++]);
  }));
  return {completed,total,failed};
}
export class AssetCache {
  constructor(redraw=()=>{}) { this.redraw=redraw; this.pending=new Set(); this.outlines=new SpriteOutlines(); }
  get(url) {
    if(!url)return null;
    const record=records.get(url);
    if(record?.image)return record.image;
    if(record?.failed)return this.get(fallbacks.get(url));
    if(!this.pending.has(url)) { this.pending.add(url); loadArt(url).then(()=>{this.pending.delete(url);this.redraw();}); }
    return null;
  }
  drawOutlined(c,url,x,y,w,h,color,zoom=1,dpr=1) {
    const image=this.get(url);if(!image)return false;
    const sprite=this.outlines.get(image,url,w,h,color||'#d7d3b5',zoom,dpr);
    c.drawImage(sprite.canvas,x-sprite.pad,y-sprite.pad,sprite.width,sprite.height);return true;
  }
  draw(c,url,x,y,w,h) {
    const image=this.get(url);if(!image)return false;
    const scale=Math.min(w/image.naturalWidth,h/image.naturalHeight),dw=image.naturalWidth*scale,dh=image.naturalHeight*scale;
    c.drawImage(image,x+(w-dw)/2,y+(h-dh)/2,dw,dh);return true;
  }
}
