'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const root=path.resolve(__dirname,'..'),read=name=>fs.readFileSync(path.join(root,name+'.js'),'utf8');
const species=['gorilla','chimpanzee','orangutan','gibbon','mandrill','capuchin'],sizes=[76,66,78,64,63,49];
function context(calls=[]){return new Proxy({globalAlpha:1},{get:(o,k)=>k in o?o[k]:(...args)=>{for(const v of args)if(typeof v==='number')assert.ok(Number.isFinite(v),String(k)+' stays finite');calls.push([k,...args])}})}
function harness(){
 class Renderer{};for(const name of['draw','drawApe','drawApeSprite','drawMap','drawPrimateGeometry','drawHuman','drawHumanSprite','drawCorpses','drawObject','drawSiege'])Renderer.prototype[name]=()=>{};
 class Canvas{constructor(w,h){this.width=w;this.height=h;this.ctx=context()}getContext(){return this.ctx}}
 const math=Object.create(Math);math.random=()=>{throw Error('Artwork must not consume simulation randomness')};
 const c=vm.createContext({ATSRenderer:Renderer,Math:math,Map,Set,WeakMap,OffscreenCanvas:Canvas});c.window=c;
 const manifest={};for(const name of['characters','armor-sprites','held-items'])manifest[name]=JSON.parse(fs.readFileSync(path.join(root,'assets/visual',name,'manifest.json')));
 manifest.characters.atlases.push(...manifest['armor-sprites'].atlases);Object.assign(manifest.characters.frames,manifest['armor-sprites'].characterFrames);
 c.ATSVisualAssets={manifest,get:id=>({id})};c.ATSDivisions=[];
 for(const name of['champions-render','character-art','held-item-art','outfit-art','prisons-render'])vm.runInContext(read(name),c,{filename:name+'.js'});
 const r=new Renderer();Object.assign(r,{time:10,camera:{zoom:1},quality:'high',detailLevel:0,graphicsProfile:{animationHz:18},reducedMotion:true});return {c,r};
}
const actor=(kind,dir=0,extra={})=>({id:'visual-'+kind,species:kind,hp:550,maxHp:550,age:120,bodyScale:1,dir,phase:0,state:'follow',champion:{archetype:'wallbreaker',prisonChampion:true},...extra});
const angle=d=>{const t=d*Math.PI/4;return Math.atan2(Math.sin(t)/.42-Math.cos(t)/.8,Math.sin(t)/.42+Math.cos(t)/.8)};

test('prison champions retain every species and use complete heavy armor in all facings and action states',()=>{
 const {c}=harness();
 for(const kind of species)for(let d=0;d<8;d++)for(const fields of[{}, {moving:true}, {hitTimer:.2}, {animation:{kind:'slam',start:9.9,duration:.5}}, {siegeTransition:{}}, {hp:0}]){
  const a=actor(kind,angle(d),fields),before=JSON.stringify(a),p=c.ATSCharacterArt.resolve(a,10),atlas='armor-'+kind+'-tier3';
  assert.equal(p.atlas,atlas);assert.ok(c.ATSCharacterArt.frameRect(atlas,p.row,p.column));assert.equal(c.ATSOutfitArt.armor(a,p).grade,3);assert.equal(JSON.stringify(a),before);
 }
 const ordinary=actor('gibbon',0,{champion:undefined}),named=actor('gibbon',0,{champion:{archetype:'skyRunner'}});
 assert.equal(c.ATSOutfitArt.atlasFor(ordinary),'armor-gibbon-base');assert.equal(c.ATSOutfitArt.atlasFor(named),'armor-gibbon-base');
});

test('final painted renderer draws a back-mounted flag once, in body-depth order even at low detail',()=>{
 const {c,r}=harness(),seen=new Set();
 for(const quality of['high','low'])for(let s=0;s<species.length;s++)for(let d=0;d<8;d++){
  r.quality=quality;r.detailLevel=quality==='low'?4:0;r.camera.zoom=quality==='low'?.5:1;
  const a=actor(species[s],angle(d)),before=JSON.stringify(a),p=c.ATSCharacterArt.resolve(a,10),calls=[];
  r.drawPrimateGeometry(context(calls),a,{mini:quality==='low'});
  const images=calls.filter(v=>v[0]==='drawImage'),flag=images.findIndex(v=>v[1].atsChampionStandard),body=images.findIndex(v=>!v[1].atsChampionStandard);
  assert.equal(images.filter(v=>v[1].atsChampionStandard).length,1,'one drawn image for the back flag');assert.ok(body>=0);assert.ok(p.rear?flag>body:flag<body,'back mounting follows body facing');
  const at=c.ATSChampionArt.standardPlacement(a,p,sizes[s]);assert.equal(at.layer,p.rear?'front':'rear');assert.ok([at.x,at.y,at.width,at.height].every(Number.isFinite));seen.add(p.direction);assert.equal(JSON.stringify(a),before);
 }
 assert.equal(seen.size,8);assert.equal(c.ATSChampionArt.status().sharedStandards,1,'all matching champions share the authored flag image');
});

test('standards are absent from ordinary monkeys, named champions, King, cache sprites and fallen actors',()=>{
 const {c}=harness();for(const fields of[{champion:undefined},{champion:{archetype:'wallbreaker'}},{hp:0},{_atlas:true}])assert.equal(c.ATSChampionArt.standardPlacement(actor('gorilla',0,fields),{},76),null);
 assert.equal(c.ATSChampionArt.standardPlacement(actor('gorilla'),{king:true},76),null);
 for(let i=0;i<30;i++)c.ATSChampionArt.standardImage('#'+i.toString(16).padStart(6,'0'));assert.ok(c.ATSChampionArt.status().sharedStandards<=8);
});

test('real rescued member ledgers never render the old inert count stack',()=>{
 const {r}=harness(),calls=[];r.ctx=context();r.project=()=>({x:0,y:0});r.visible=()=>true;r.drawApeMini=(...args)=>calls.push(args);
 r.drawSiege({_prisons:{active:[{prison:{cohorts:[{id:'rescued',memberIds:['ape-1'],count:6,x:0,y:0,species:'gorilla'}]}}]}});assert.equal(calls.length,0);
});

test('bounded cage previews include both pending and recaptured prison champions',()=>{
 const {c,r}=harness(),occupants=[];
 c.ATSVisualAssets.manifest.environment=JSON.parse(fs.readFileSync(path.join(root,'assets/visual/environment/manifest.json')));vm.runInContext(read('environment-art'),c);
 r.drawApeMini=(...args)=>occupants.push(args[4]);r.drawArtworkDamage=()=>{};
 r.drawCage(context(),{id:'recaptured-cage',count:20,w:85,h:65,championCount:2,championRemaining:0,recapturedApes:[{champion:{prisonChampion:true}},{species:'gibbon'}]});
 assert.equal(occupants.length,4);assert.equal(occupants.filter(a=>a.champion?.prisonChampion).length,1,'released champions are not counted again while recaptured ones regain their preview');
});
