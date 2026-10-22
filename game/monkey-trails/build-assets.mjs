// Reproducible art package. Node standard library only; no image services/tools.
// node game/monkey-trails/build-assets.mjs --output /path/monkey-trails-assets.zip
import {writeFile,mkdir,readFile} from 'node:fs/promises';
import {dirname,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {deflateRawSync} from 'node:zlib';
import {SPECIES,ELEMENTS} from './data.mjs';
import {portrait,battlefield,pickupIcon,elementIcon,ASSET_MANIFEST,ANIMATION_STATES,ULTRA_STATES} from './art.mjs';
const gameDir=dirname(fileURLToPath(import.meta.url));
const repoDir=resolve(gameDir,'../..'),buildDir=resolve(repoDir,'work');
const args=process.argv.slice(2),value=key=>{const i=args.indexOf(key);return i<0?null:args[i+1];};
const output=resolve(value('--output')||resolve(buildDir,'monkey-trails-assets.zip'));
const contact=resolve(value('--contact')||resolve(buildDir,'monkey-trails-contact.html'));
const prefix='monkey-trails/wild-companions-v2/';
const manifest=structuredClone(ASSET_MANIFEST);
manifest.prefix=prefix;
manifest.artwork={author:'Monkey Trails original vector artwork',license:'Distributed with the Monkey Trails game source',anatomyFamilies:12,species:60,animationFiles:436,evolutionVariants:180};
manifest.hosting={assetBaseVariable:'MONKEY_TRAILS_ASSET_BASE',contentType:'image/svg+xml',cacheControl:'public, max-age=31536000, immutable',cors:'Allow your game origin when using cross-origin fetch. Direct image display does not require authenticated requests.'};
const entries=[];
function add(name,content){entries.push({name:prefix+name,data:Buffer.from(content)});}
for(const s of Object.values(SPECIES)){
 const states=[...ANIMATION_STATES,...(s.rarity==='ultra'?ULTRA_STATES:[])];
 for(const state of states)add(`monkeys/${s.id}/${state}.svg`,portrait({species:s.id,types:s.types,stage:0},{state}));
 for(const [variant,stage] of [['attuned',1],['elder',2],['ascendant',3]]){
  const src=`monkeys/${s.id}/variants/${variant}.svg`;
  add(src,portrait({species:s.id,types:s.types.length?s.types:['leaf'],stage}));
  Object.assign(manifest.species[s.id].variants[variant],{src,types:s.types.length?s.types:['leaf']});
 }
}
for(const t of Object.keys(ELEMENTS)){
 add(`environments/${t}.svg`,battlefield([t]));
 add(`interface/element-${t}.svg`,elementIcon(t));
}
for(const [id,src] of Object.entries(manifest.pickups))add(src,pickupIcon(id));
add('interface/loading.svg',await readFile(resolve(gameDir,'assets/interface/loading.svg'),'utf8'));
add('lobby/tile.svg',await readFile(resolve(gameDir,'tile.svg'),'utf8'));
// The library cover ships locally so the lobby never waits for a bucket upload.
// Include the same optimized artwork in the optional hosted asset package.
const cover=await readFile(resolve(gameDir,'assets/cover-v2.webp'));
add('lobby/cover-v2.webp',cover);
manifest.lobby={tile:'lobby/tile.svg',cover:'lobby/cover-v2.webp',coverSize:[1536,1024],coverContentType:'image/webp',coverBytes:cover.length};
const readme=`Monkey Trails: Wild Companions — version 2 vector asset package

Upload the contents of this ZIP preserving the object prefix:
  ${prefix}

Use your EXISTING public Cloudflare asset bucket. This package intentionally
contains no invented bucket URL, access token, or hosting credentials.
Set window.MONKEY_TRAILS_ASSET_BASE before the game module loads to your public
bucket base followed by ${prefix}. One base controls every packaged asset URL.
The source game's finished inline SVG rigs remain the dependable local default.

SVG response Content-Type: image/svg+xml
Cover response Content-Type: image/webp
JSON response Content-Type: application/json
Versioned asset Cache-Control: public, max-age=31536000, immutable
Use a new prefix for subsequent art revisions so cached files stay consistent.
If fetching the manifest from a different origin, enable CORS for the game origin.

manifest.json describes every animation, duration, frame index, anchor, scale,
variant, environment, and interface asset. These are animated vector scenes;
frames:[0] names one SVG scene with articulated CSS keyframe animations.
All 60 species have seven animation scenes. The eight Ultras add arrival and
unique signature scenes. Three evolution variants per species add silhouette
attachments while preserving species identity. Reduced-motion media queries
and runtime preference classes stop the vector motion.
The library cover is an original generated illustration, packaged as WebP.

Regenerate from source: node game/monkey-trails/build-assets.mjs --output FILE
Art source: game/monkey-trails/art.mjs. Catalogue: data.mjs.
`;
add('HOSTING.txt',readme);
add('manifest.json',JSON.stringify(manifest,null,2)+'\n');

// A deterministic ZIP writer (DEFLATE, UTF-8 names, fixed DOS timestamp).
const crcTable=Array.from({length:256},(_,n)=>{for(let k=0;k<8;k++)n=n&1?0xedb88320^(n>>>1):n>>>1;return n>>>0;});
function crc32(data){let n=0xffffffff;for(const b of data)n=crcTable[(n^b)&255]^(n>>>8);return (n^0xffffffff)>>>0;}
const local=[],central=[];let offset=0;
for(const e of entries.sort((a,b)=>a.name.localeCompare(b.name,'en'))){
 const name=Buffer.from(e.name),compressed=deflateRawSync(e.data,{level:9}),crc=crc32(e.data);
 const head=Buffer.alloc(30);head.writeUInt32LE(0x04034b50);head.writeUInt16LE(20,4);head.writeUInt16LE(0x0800,6);head.writeUInt16LE(8,8);head.writeUInt16LE(0x5821,12);head.writeUInt32LE(crc,14);head.writeUInt32LE(compressed.length,18);head.writeUInt32LE(e.data.length,22);head.writeUInt16LE(name.length,26);
 local.push(head,name,compressed);
 const dir=Buffer.alloc(46);dir.writeUInt32LE(0x02014b50);dir.writeUInt16LE(20,4);dir.writeUInt16LE(20,6);dir.writeUInt16LE(0x0800,8);dir.writeUInt16LE(8,10);dir.writeUInt16LE(0x5821,14);dir.writeUInt32LE(crc,16);dir.writeUInt32LE(compressed.length,20);dir.writeUInt32LE(e.data.length,24);dir.writeUInt16LE(name.length,28);dir.writeUInt32LE(offset,42);central.push(dir,name);offset+=head.length+name.length+compressed.length;
}
const directory=Buffer.concat(central),end=Buffer.alloc(22);end.writeUInt32LE(0x06054b50);end.writeUInt16LE(entries.length,8);end.writeUInt16LE(entries.length,10);end.writeUInt32LE(directory.length,12);end.writeUInt32LE(offset,16);
await mkdir(dirname(output),{recursive:true});await writeFile(output,Buffer.concat([...local,directory,end]));
await mkdir(resolve(gameDir,'assets'),{recursive:true});await writeFile(resolve(gameDir,'assets/manifest.json'),JSON.stringify(manifest,null,2)+'\n');
await mkdir(dirname(contact),{recursive:true});
const cards=Object.values(SPECIES).map(s=>`<article><div>${portrait({species:s.id,types:s.types,stage:0},{reducedMotion:true})}</div><small>${String(s.number).padStart(2,'0')} · ${s.affinity}</small><h2>${s.name}</h2><p>${s.title}</p></article>`).join('');
await writeFile(contact,`<!doctype html><html lang="en"><meta charset="utf-8"><title>Monkey Trails — 60 original companions</title><style>*{box-sizing:border-box}body{margin:0;padding:36px;background:#e8e9d8;color:#264d43;font:15px system-ui}h1{font-size:32px}main{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:16px}article{background:#f7f5e7;border:1px solid #c9d2bd;border-radius:18px;padding:12px;text-align:center}article>div{aspect-ratio:1;display:grid;place-items:center}.monkey-art{width:100%;height:100%;max-height:215px}h2{font-size:14px;margin:7px 0;min-height:34px}p{font-size:12px;margin:0;color:#65846b}small{font-size:11px;color:#708b72}@media(max-width:800px){main{grid-template-columns:repeat(3,minmax(0,1fr))}}</style><h1>60 companions. A whole wild world.</h1><p>Original starters · elemental specialists · hybrids · eight Ultras. Art contact sheet, motion paused for inspection.</p><main>${cards}</main></html>`);
console.log(JSON.stringify({output,bytes:(await (await import('node:fs/promises')).stat(output)).size,entries:entries.length,manifest:resolve(gameDir,'assets/manifest.json'),contact},null,2));
