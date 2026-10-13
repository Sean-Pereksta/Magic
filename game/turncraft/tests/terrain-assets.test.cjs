const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const crypto=require('node:crypto');
const {assetPath}=require('./helpers/cloudflare-assets.cjs');
const {MAP_IDS}=require('../terrain.js');
const base=path.resolve(__dirname,'../assets');
const terrain=JSON.parse(fs.readFileSync(path.join(base,'terrain-manifest.json'),'utf8'));
const effects=JSON.parse(fs.readFileSync(path.join(base,'effects-manifest.json'),'utf8'));
function webpInfo(bytes){assert.equal(bytes.toString('ascii',0,4),'RIFF');assert.equal(bytes.toString('ascii',8,12),'WEBP');for(let offset=12;offset+8<=bytes.length;){const id=bytes.toString('ascii',offset,offset+4),length=bytes.readUInt32LE(offset+4),data=offset+8;if(id==='VP8X')return {width:bytes.readUIntLE(data+4,3)+1,height:bytes.readUIntLE(data+7,3)+1,alpha:!!(bytes[data]&16)};if(id==='VP8 ')return {width:bytes.readUInt16LE(data+6)&0x3fff,height:bytes.readUInt16LE(data+8)&0x3fff,alpha:false};if(id==='VP8L'){const bits=bytes.readUInt32LE(data+1);return {width:(bits&0x3fff)+1,height:((bits>>>14)&0x3fff)+1,alpha:!!(bits&(1<<28))}}offset=data+length+(length%2)}throw new Error('No WebP frame')}
function asset(record){const file=assetPath(record.file);assert.ok(file,record.file);const bytes=fs.readFileSync(file);assert.ok(bytes.length>2000,'real painted bitmap content');const info=webpInfo(bytes);assert.equal(info.width,record.width,file);assert.equal(info.height,record.height,file);if(record.transparent)assert.equal(info.alpha,true,file);return crypto.createHash('sha256').update(bytes).digest('hex')}

test('all six maps contain unique real optimized ground and transparent looping prop sheets',()=>{
  assert.deepEqual(Object.keys(terrain.maps),MAP_IDS);const hashes=new Set();for(const id of MAP_IDS){const map=terrain.maps[id];hashes.add(asset(map.terrain));hashes.add(asset(map.props));assert.equal(map.terrain.columns*map.terrain.frameSize,map.terrain.width);assert.equal(map.props.columns*map.props.frameSize,map.props.width);assert.equal(map.terrain.rows*map.terrain.frameSize,map.terrain.height);assert.equal(map.props.rows*map.props.frameSize,map.props.height);for(const state of ['mineral','gas','ambient']){assert.equal(map.props.states[state].frames.length,4);assert.equal(map.props.states[state].loop,true)}}assert.equal(hashes.size,12);
});

test('shared combat effects have six distinct eight-frame state rows and transparent alpha',()=>{
  asset(effects);assert.equal(effects.columns*effects.frameSize,effects.width);assert.equal(effects.rows*effects.frameSize,effects.height);assert.deepEqual(Object.keys(effects.states),['ballistic','energy','organic','shield','explosion','dust']);for(const [i,state]of Object.values(effects.states).entries()){assert.equal(state.row,i);assert.equal(state.frames.length,8);assert.deepEqual(state.frames,Array.from({length:8},(_,n)=>i*8+n))}
});

test('generation records preserve per-map prompts and provenance for all shipped sheets',()=>{
  const prompts=JSON.parse(fs.readFileSync(path.join(base,'terrain-prompts.json'),'utf8'));assert.equal(prompts.generator,'built-in image_gen');assert.equal(prompts.prompts.length,6);for(const id of MAP_IDS){const prompt=prompts.prompts.find(p=>p.id===id);assert.ok(prompt.terrain.length>200);assert.ok(prompt.props.length>200)}assert.ok(prompts.combat.includes('EIGHT COLUMNS'));
});
