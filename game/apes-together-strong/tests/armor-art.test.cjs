'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const root=path.resolve(__dirname,'../assets/visual'),m=JSON.parse(fs.readFileSync(path.join(root,'armor-sprites/manifest.json')));
test('42 distinct source sheets have valid transparent atlases and complete mapped action coverage',()=>{
 assert.equal(m.atlases.length,42);const hashes=new Set();
 for(const atlas of m.atlases){const bytes=fs.readFileSync(path.join(root,atlas.file));assert.equal(bytes.toString('ascii',8,12),'WEBP');assert.equal(crypto.createHash('sha256').update(bytes).digest('hex'),atlas.sha256);assert.equal(fs.readFileSync(path.join(root,atlas.source)).subarray(0,8).toString('hex'),'89504e470d0a1a0a');hashes.add(atlas.sha256);const id=atlas.id.replace('characters-',''),spec=m.sheets[id],frames=m.characterFrames[id];assert.equal(Object.keys(frames).length,atlas.frameCount);
  for(const f of Object.values(frames)){assert.ok(f.w>0&&f.h>0&&f.x>=0&&f.y>=0&&f.x+f.w<=atlas.width&&f.y+f.h<=atlas.height);assert.ok(f.anchorX>=0&&f.anchorX<=f.w&&f.anchorY>=0&&f.anchorY<=f.h)}
  for(const [type,rows] of [['m',spec.motionRows],['a',spec.actionRows]])for(let d=0;d<5;d++)for(let column=0;column<8;column++){const [r,c]=spec.overrides?.[type+d+':'+column]||[rows[d],column];assert.ok(frames[r+':'+c],id+' '+type+d+':'+column);assert.ok(frames[r+':'+c].opaquePixels>100)}
 }
 assert.equal(hashes.size,42);
});
test('armor decoding deduplicates pending loads and bounds resident variants without evicting base art',async()=>{
 let loads=0;class Image{set src(v){loads++;queueMicrotask(()=>{this.naturalWidth=100;this.naturalHeight=100;this.onload()})}}
 const sources={'characters-armor-gorilla-base':'base'};for(let i=0;i<40;i++)sources['characters-armor-test-'+i]='variant-'+i;
 const c=vm.createContext({Image,Promise,Map,Set,Object,setTimeout,clearTimeout,console});c.window=c;c.ATS_VISUAL_BUNDLE={manifest:{},sources};vm.runInContext(fs.readFileSync(path.join(__dirname,'../visual-assets.js'),'utf8'),c);const a=c.ATSVisualAssets;await a.ready;assert.equal(loads,1);
 const id='characters-armor-test-0';for(let i=0;i<100;i++)a.get(id);await a.preload([id]);assert.equal(loads,2);
 for(let i=1;i<40;i++)await a.preload(['characters-armor-test-'+i]);assert.equal(a.cacheStats().lazy,24);assert.equal(a.cacheStats().decoded,25);assert.ok(a.get('characters-armor-gorilla-base'));assert.equal(a.get('characters-armor-test-0'),null);await a.preload(['characters-armor-test-0']);assert.equal(a.cacheStats().lazy,24);assert.ok(a.get('characters-armor-test-0'));
});

test('reduced motion freezes strides while preserving moving actors attack, climbing, hurt and death poses',()=>{
 const c=vm.createContext({ATSRenderer:class{draw(){}},ATSVisualAssets:{manifest:{'armor-sprites':m},get:()=>({})}});c.window=c;
 vm.runInContext(fs.readFileSync(path.join(__dirname,'../outfit-art.js'),'utf8'),c);
 const a={id:'ape-moving',species:'gorilla',moving:true,hp:100},p={direction:2,beat:1,phase:1},options={reducedMotion:true};
 const idle=c.ATSOutfitArt.pose(a,{...p,state:'idle'},options,1),walk=c.ATSOutfitArt.pose(a,{...p,state:'run'},options,1);
 assert.equal(walk.row,idle.row);assert.equal(walk.column,idle.column);
 for(const state of ['attack','climb','hurt','death']){
  const actor={...a,hp:state==='death'?0:100},normal=c.ATSOutfitArt.pose(actor,{...p,state},{},1),reduced=c.ATSOutfitArt.pose(actor,{...p,state},options,1);
  assert.equal(reduced.row,normal.row,state);assert.equal(reduced.column,normal.column,state);assert.ok(reduced.row!==idle.row||reduced.column!==idle.column,state+' must remain visible');
 }
});
