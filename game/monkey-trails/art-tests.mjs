import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile,stat} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {dirname,resolve} from 'node:path';
import {inflateRawSync} from 'node:zlib';
import {SPECIES,ELEMENTS,MOVES} from './data.mjs';
import {portrait,ANIMATION_STATES,ULTRA_STATES,ASSET_MANIFEST,assetUrl,battlefield,pickupIcon,elementIcon,hydrateHostedPortraits} from './art.mjs';

const gameDir=dirname(fileURLToPath(import.meta.url));
const roster=Object.values(SPECIES),ultras=roster.filter(s=>s.rarity==='ultra');
const model=(s,stage=0)=>({species:s.id,types:s.types.length?s.types:['leaf'],stage});

// Ignore labels, paint and hidden elemental effects. Distinction must come from
// the monkey's own path/shape coordinates and anatomy, rather than recoloring.
function withoutGroup(svg,className){
 const opening=`<g class="${className}">`,start=svg.indexOf(opening);
 if(start<0)return svg;
 const groups=/<g\b[^>]*>|<\/g>/g;groups.lastIndex=start;
 let depth=0,match;
 while((match=groups.exec(svg))){depth+=match[0].startsWith('</')?-1:1;if(depth===0)return svg.slice(0,start)+svg.slice(groups.lastIndex);}
 throw Error(`Unclosed ${className} vector group`);
}
function geometry(svg){
 const cleaned=withoutGroup(withoutGroup(svg.replace(/<style>[\s\S]*?<\/style>/g,''),'signature-fx'),'cast-fx');
 return [...cleaned.matchAll(/<(path|ellipse|circle|rect|polygon|polyline|line|g|svg)\b([^>]*)>/g)].map(([,tag,attrs])=>{
  const coords=[...attrs.matchAll(/(?:^|\s)(d|points|x|y|x1|y1|x2|y2|cx|cy|r|rx|ry|width|height|transform|viewBox)="([^"]*)"/g)].map(([,key,value])=>`${key}=${value}`).sort().join(';');
  return `${tag}:${coords}`;
 }).join('|');
}
function styles(svg){return svg.match(/<style>([\s\S]*?)<\/style>/)?.[1]||'';}
function rule(css,selector){
 for(const [,selectors,body] of css.matchAll(/([^{}]+)\{([^{}]*)\}/g))if(selectors.trim().split(',').map(s=>s.trim()).includes(selector))return body;
 throw Error(`Missing animation selector ${selector}`);
}
function keyframes(css,name){
 const start=css.indexOf(`@keyframes ${name}{`);assert.ok(start>=0,`Missing keyframes ${name}`);
 const open=css.indexOf('{',start);let depth=1;
 for(let i=open+1;i<css.length;i++){if(css[i]==='{')depth++;if(css[i]==='}')depth--;if(!depth)return css.slice(open+1,i);}
 throw Error(`Unclosed animation ${name}`);
}
function assertArticulatedMotion(svg,selector,part){
 const css=styles(svg),body=rule(css,selector),name=body.match(/animation:\s*([\w-]+)/)?.[1];
 assert.ok(name,`${selector} binds a real animation`);
 const frames=keyframes(css,name),transforms=[...frames.matchAll(/transform:\s*([^;}]+)/g)].map(m=>m[1]);
 assert.ok(transforms.some(t=>/(translate[XY]?|rotate|scale[XY]?)\([^)]*[1-9]/.test(t)),`${name} must move or reshape geometry`);
 assert.ok(svg.includes(`class="${part}"`),`${part} exists on the SVG rig`);
 // CSS can target a real articulated part only when that part contains artwork.
 const beginning=svg.indexOf(`class="${part}"`);
 assert.match(svg.slice(beginning,beginning+4000),/<(path|circle|ellipse|rect)\b/,`${part} contains actual vector shapes`);
}

test('all 90 species have distinct authored geometry after ignoring paint, labels and hidden effects',()=>{
 assert.equal(roster.length,90);const seen=new Map();
 for(const s of roster){const svg=portrait(model(s)),shape=geometry(svg);assert.equal(geometry(svg.replace(/(?:fill|stroke)="[^"]*"/g,'fill="#000"')),shape,'paint must never influence identity');assert.ok(!seen.has(shape),`${s.id} shares geometry with ${seen.get(shape)}`);seen.set(shape,s.id);}
 assert.equal(seen.size,90);
});

test('all 654 required animation scenes bind state-specific motion to actual body geometry',()=>{
 assert.equal(ultras.length,12);assert.deepEqual(ANIMATION_STATES,['idle','walk','attack','cast','hit','knockout','victory']);
 const bindings={idle:['.monkey-art .rig','rig'],walk:['.monkey-art[data-state="walk"] .rig','rig'],attack:['.monkey-art[data-state="attack"] .arm-r','arm-r'],cast:['.monkey-art[data-state="cast"] .arm-l','arm-l'],hit:['.monkey-art[data-state="hit"] .rig','rig'],knockout:['.monkey-art[data-state="knockout"] .rig','rig'],victory:['.monkey-art[data-state="victory"] .rig','rig'],arrival:['.monkey-art[data-state="arrival"] .rig','rig'],signature:['.monkey-art[data-state="signature"] .rig','rig']};
 let scenes=0;
 for(const s of roster)for(const state of [...ANIMATION_STATES,...(s.rarity==='ultra'?ULTRA_STATES:[])]){
  const svg=portrait(model(s),{state});scenes++;
  assert.ok(svg.startsWith('<svg xmlns="http://www.w3.org/2000/svg"'));
  assert.ok(svg.includes(`data-state="${state}"`));assert.doesNotMatch(svg,/undefined|NaN|<image\b|href="https?:/);
  assertArticulatedMotion(svg,...bindings[state]);
  if(state==='walk'){assertArticulatedMotion(svg,'.monkey-art[data-state="walk"] .leg-l','leg-l');assertArticulatedMotion(svg,'.monkey-art[data-state="walk"] .arm-l','arm-l');}
  if(state==='knockout'){assert.equal(rule(styles(svg),'.monkey-art[data-state="knockout"] .ko-eyes'),'display:block');assert.match(rule(styles(svg),'.monkey-art[data-state="knockout"] .eyes'),/display:none/);}
  if(state==='signature'){const selector=`.monkey-art[data-state="signature"][data-signature="${s.signature}"] .signature-fx`;assertArticulatedMotion(svg,selector,'signature-fx');}
 }
 assert.equal(scenes,654);
});

test('twelve Ultras have different signature geometry and different signature motion',()=>{
 const scenes=new Set(),motions=new Set();
 for(const s of ultras){
  const svg=portrait(model(s),{state:'signature'}),selector=`.monkey-art[data-state="signature"][data-signature="${s.signature}"] .signature-fx`;
  motions.add(rule(styles(svg),selector).match(/animation:([\w-]+)/)[1]);
  const start=svg.indexOf('<g class="signature-fx">'),allButFx=withoutGroup(svg,'signature-fx');
  assert.ok(svg.length>allButFx.length+100,`${s.id} has a substantial signature effect`);
  scenes.add(svg.slice(start,start+svg.length-allButFx.length));
 }
 assert.equal(scenes.size,12);assert.equal(motions.size,12);
});

test('all 90 species retain identity while adding three distinct evolution silhouettes',()=>{
 let variants=0;
 for(const s of roster){
  const forms=[0,1,2,3].map(stage=>portrait(model(s,stage)));
  assert.equal(new Set(forms.map(geometry)).size,4,`${s.id} must gain geometry at each evolution`);
  for(const form of forms){assert.ok(form.includes(`data-species="${s.id}"`));assert.doesNotMatch(form,/undefined|NaN/);}
  for(const stage of [1,2,3]){assert.ok(forms[stage].includes('class="evolution-parts"'));variants++;}
 }
 assert.equal(variants,270);
});

test('the thirty new monkeys have unique face geometry and body proportions before paint or props',()=>{
 const newcomers=roster.filter(s=>s.number>60),faces=new Set(),bodies=new Set();assert.equal(newcomers.length,30);
 for(const s of newcomers){
  const svg=portrait(model(s)),head=svg.slice(svg.indexOf('<g class="head">'),svg.indexOf('<g class="cast-fx">'));
  faces.add(geometry(head));
  // The torso is the first pair of ellipses after the articulated legs.
  const body=svg.slice(svg.indexOf('<g class="leg-r">'),svg.indexOf('<g class="arm-l">'));
  bodies.add(geometry(body));
 }
 assert.equal(faces.size,30);assert.equal(bodies.size,30);
});

test('source and packaged manifests cover every scene, variant, scale, anchor and local placeholder',async()=>{
 const manifest=JSON.parse(await readFile(resolve(gameDir,'assets/manifest.json'),'utf8'));
 assert.equal(manifest.prefix,'monkey-trails/wild-companions-v3/');assert.equal(manifest.format,'animated-svg');
 assert.deepEqual(manifest.animations,ASSET_MANIFEST.animations);assert.equal(Object.keys(manifest.species).length,90);
 let scenes=0,variants=0;
 for(const s of roster){
  const entry=manifest.species[s.id],states=[...ANIMATION_STATES,...(s.rarity==='ultra'?ULTRA_STATES:[])];
  assert.deepEqual(Object.keys(entry.states),states);assert.ok(entry.scale>0);assert.deepEqual(entry.anchor,[.5,.90]);
  for(const state of states){assert.equal(entry.states[state].src,`monkeys/${s.id}/${state}.svg`);assert.deepEqual(entry.states[state].frames,[0]);assert.deepEqual(entry.states[state].anchor,[.5,.90]);assert.ok(manifest.animations[state].durationMs>0);scenes++;}
  for(const variant of ['attuned','elder','ascendant']){assert.equal(entry.variants[variant].src,`monkeys/${s.id}/variants/${variant}.svg`);assert.ok(entry.variants[variant].modular);variants++;}
 }
 assert.equal(scenes,654);assert.equal(variants,270);
 assert.ok((await stat(resolve(gameDir,'assets',manifest.placeholder))).size>100);
 assert.match(await readFile(resolve(gameDir,'assets',manifest.placeholder),'utf8'),/<svg[^>]+aria-label="Loading companion"/);
});

test('all techniques are learnable and each species always has a reliable zero-energy action',()=>{
 const learned=new Set();
 for(const s of roster){assert.ok(s.learnset.includes('strike'));for(const id of s.learnset){assert.ok(MOVES[id],`${s.id} unknown move ${id}`);learned.add(id);}}
 assert.equal(learned.size,Object.keys(MOVES).length);assert.equal(MOVES.strike.cost,0);assert.ok(MOVES.strike.power>0);
});

test('every manifest object exists in GitHub assets and the committed Cloudflare ZIP with identical bytes',async()=>{
 const manifest=JSON.parse(await readFile(resolve(gameDir,'assets/manifest.json'),'utf8'));
 const zip=await readFile(resolve(gameDir,'art-pack/monkey-trails-wild-companions-v3.zip')),objects=new Map();let offset=0;
 while(zip.readUInt32LE(offset)===0x04034b50){
  const compressed=zip.readUInt32LE(offset+18),nameLength=zip.readUInt16LE(offset+26),extra=zip.readUInt16LE(offset+28),name=zip.subarray(offset+30,offset+30+nameLength).toString(),start=offset+30+nameLength+extra;
  objects.set(name,inflateRawSync(zip.subarray(start,start+compressed)));offset=start+compressed;
 }
 const paths=[manifest.placeholder,...Object.values(manifest.environments),...Object.values(manifest.pickups),...Object.keys(ELEMENTS).map(id=>`interface/element-${id}.svg`)];
 for(const s of roster){const entry=manifest.species[s.id];paths.push(...Object.values(entry.states).map(state=>state.src),...Object.values(entry.variants).filter(v=>v.src).map(v=>v.src));}
 for(const file of new Set(paths)){const local=await readFile(resolve(gameDir,'assets',file));assert.deepEqual(objects.get(manifest.prefix+file),local,`${file} is saved locally and in the upload ZIP`);assert.match(local.toString(),/^<svg\b/);assert.ok(local.length<30000,`${file} remains a small vector asset`);}
 assert.deepEqual(JSON.parse(objects.get(manifest.prefix+'manifest.json').toString()),manifest);
 assert.equal(manifest.artwork.species,90);assert.equal(manifest.artwork.animationFiles,654);assert.equal(manifest.artwork.evolutionVariants,270);
 assert.ok(zip.length<4*1024*1024,'the complete upload ZIP stays below 4 MiB');
});

test('hosted portraits load configured art, fall back locally, preserve inline on failures and respect reduced motion',()=>{
 const previous=globalThis.MONKEY_TRAILS_ASSET_BASE;
 try{
  globalThis.MONKEY_TRAILS_ASSET_BASE='https://art.example.test/monkey-trails/wild-companions-v3';
  for(const outcome of ['remote','local','inline','reduced','branch','variant']){
   const requests=[],svg={dataset:{species:'acornbuckler',state:'idle',stage:'0',types:'["leaf"]',reduced:'false'},isConnected:true,closest:()=>null,getAttribute:key=>key==='class'?'monkey-art':'Acornbuckler, idle',replaceWith(img){this.replacement=img;}};
   class FakeImage{dataset={};listeners={};addEventListener(name,callback){this.listeners[name]=callback;}set src(value){this.url=value;requests.push(value);const success=outcome==='remote'||outcome==='variant'||(outcome==='local'&&!value.startsWith('https:'));this.listeners[success?'load':'error']?.();}}
   if(outcome==='branch'||outcome==='variant'){svg.dataset.stage='2';svg.dataset.types=JSON.stringify(outcome==='branch'?['fire']:['leaf']);}
   const root={ownerDocument:{defaultView:{Image:FakeImage,matchMedia:()=>({matches:false})}},querySelectorAll:()=>[svg]};
   hydrateHostedPortraits(root,{reducedMotion:outcome==='reduced'});
   if(outcome==='remote'){assert.equal(requests.length,1);assert.equal(svg.replacement?.url,'https://art.example.test/monkey-trails/wild-companions-v3/monkeys/acornbuckler/idle.svg');}
   if(outcome==='local'){assert.equal(requests.length,2);assert.equal(svg.replacement?.url,new URL('./assets/monkeys/acornbuckler/idle.svg',import.meta.url).href);}
   if(outcome==='inline'){assert.equal(requests.length,2);assert.equal(svg.replacement,undefined);}
   if(outcome==='reduced'||outcome==='branch'){assert.equal(requests.length,0);assert.equal(svg.replacement,undefined);}
   if(outcome==='variant'){assert.equal(requests.length,1);assert.ok(svg.replacement?.url.endsWith('/monkeys/acornbuckler/variants/elder.svg'));}
  }
 }finally{if(previous===undefined)delete globalThis.MONKEY_TRAILS_ASSET_BASE;else globalThis.MONKEY_TRAILS_ASSET_BASE=previous;}
});

test('art generation is deterministic and all optional asset URLs use one configurable base',()=>{
 for(const s of roster)assert.equal(portrait(model(s)),portrait(model(s)));
 const previous=globalThis.MONKEY_TRAILS_ASSET_BASE;
 try{globalThis.MONKEY_TRAILS_ASSET_BASE='https://assets.example.test/monkey-trails/wild-companions-v3';assert.equal(assetUrl('/monkeys/capuchin/idle.svg'),'https://assets.example.test/monkey-trails/wild-companions-v3/monkeys/capuchin/idle.svg');}
 finally{if(previous===undefined)delete globalThis.MONKEY_TRAILS_ASSET_BASE;else globalThis.MONKEY_TRAILS_ASSET_BASE=previous;}
 for(const t of Object.keys(ELEMENTS)){assert.doesNotMatch(battlefield([t]),/undefined|NaN/);assert.doesNotMatch(elementIcon(t),/undefined|NaN/);}
 for(const id of Object.keys(ASSET_MANIFEST.pickups))assert.doesNotMatch(pickupIcon(id),/undefined|NaN/);
 const svg=portrait(model(roster[0]),{reducedMotion:true});assert.match(svg,/data-reduced="true"/);assert.match(styles(svg),/@media\(prefers-reduced-motion:reduce\)/);assert.match(styles(svg),/\.battery-saver \.monkey-art \*/);
});
