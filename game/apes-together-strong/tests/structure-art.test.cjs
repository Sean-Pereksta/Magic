'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const root=path.resolve(__dirname,'..'),manifest=JSON.parse(fs.readFileSync(path.join(root,'assets/visual/settlements/manifest.json')));
function load(){
 const draws=[],ctx=new Proxy({drawImage(...args){draws.push(args)}},{get:(o,k)=>k in o?o[k]:()=>{}});
 class Renderer{constructor(){this.time=1;this.detailLevel=0;this.quality='high'} health(){} glow(){} drawRubble(){} drawArtworkDamage(){}}
 for(const name of ['draw','drawObject','drawBuilding','drawSettlement','drawHut','drawSettlementProp','drawConstruction','drawVillageGeometry','drawGroundChunk'])Renderer.prototype[name]=()=>{};
 const images=Object.fromEntries(manifest.atlases.map(a=>[a.id,{id:a.id}])),c=vm.createContext({ATSRenderer:Renderer});c.window=c;c.ATSVisualAssets={manifest:{settlements:manifest},get:id=>images[id]};
 vm.runInContext(fs.readFileSync(path.join(root,'structure-art.js'),'utf8'),c);return{api:c.ATSStructureArt,Renderer,ctx,draws};
}
test('15 full construction journeys contain 75 distinct bounded RGBA frames with stable anchors',()=>{
 assert.equal(Object.keys(manifest.construction).length,15);
 for(const a of manifest.atlases){const b=fs.readFileSync(path.join(root,'assets/visual',a.sourceFile||a.file));assert.equal(b.readUInt32BE(16),a.width);assert.equal(b.readUInt32BE(20),a.height);assert.equal(b[25],6,'actual alpha channel');assert.ok(fs.existsSync(path.join(root,'assets/visual',a.file)))}
 for(const [name,frames]of Object.entries(manifest.construction)){
  assert.equal(frames.length,5,name);const positions=new Set();
  for(const [stage,f]of frames.entries()){const a=manifest.atlases.find(a=>a.id===f.atlas),[x,y,w,h]=f.rect;assert.equal(f.stage,stage);assert.ok(x>=0&&y>=0&&x+w<=a.width&&y+h<=a.height);positions.add(x);assert.equal(f.anchor[1],frames[0].anchor[1]);assert.equal(f.anchor[0]/w,.5)}
  assert.equal(positions.size,5,'five authored source positions for '+name);
 }
});
test('actual construction stage chooses source art; finished homes reuse final strip frame',()=>{
 const{api,Renderer,ctx,draws}=load(),r=new Renderer();
 for(const kind of ['hut','lodge','longhouse','canopyHut','spearTower','spearBattery','spearBallista','workShelter','storage','training','nursery','rallyGrove','garden','orchard','cooking','barrier']){
  const name=api.appearance(kind)[0],rects=[];
  for(let stage=0;stage<4;stage++){draws.length=0;const p=Object.freeze({kind,stage,progress:stage/4});r.drawConstruction(ctx,p);const d=draws[0];assert.deepEqual(d.slice(1,5),manifest.construction[name][stage].rect);rects.push(d[1])}
  assert.equal(new Set(rects).size,4,kind);
  draws.length=0;if(['hut','longhouse','canopyHut'].includes(kind))r.drawHut(ctx,{kind,hp:100,maxHp:100,stage:4});else r.drawSettlementProp(ctx,{kind,hp:100,maxHp:100,stage:4});assert.deepEqual(draws[0].slice(1,5),manifest.construction[name][4].rect);
 }
});
test('building progression and damage read existing state without advancing construction',()=>{
 const{api,Renderer,ctx}=load(),r=new Renderer(),s={level:1,expansionLevel:0,lodge:{hp:400,maxHp:400}};
 assert.equal(api.lodgeStage(s),0);assert.equal(api.lodgeStage({...s,expansionLevel:1}),1);assert.equal(api.lodgeStage({...s,expansionLevel:2}),2);
 const before=JSON.stringify(s);r.drawSettlement(ctx,s);assert.equal(JSON.stringify(s),before);
 assert.equal(api.constructionKind('royalExpansion'),'longhouse');assert.equal(api.constructionKind('warlordExpansion'),'canopyHut');
});
test('crossing artwork maps exact navigable bounds with finite ground-only transforms',()=>{
 const{Renderer}=load(),r=new Renderer(),transforms=[],rects=[],c=new Proxy({transform(...a){transforms.push(a)},rect(...a){rects.push(a)},drawImage(){}},{get:(o,k)=>k in o?o[k]:()=>{}});
 for(const type of ['wood','stone','military','ford'])r.drawCrossingDeck(c,{type,minX:100,maxX:212,minY:50,maxY:250},0,0);
 assert.equal(transforms.length,4);for(const t of transforms)assert.deepEqual(t,[.8,.42,-.8,.42,0,0]);for(const b of rects)assert.deepEqual(b,[0,0,112,200]);
});
test('lodge scaffolding follows only actual unfinished lodge work, including paused progress',()=>{
 const{Renderer,ctx}=load(),r=new Renderer(),s={level:3,buildProgress:14,lodge:{hp:400,maxHp:400}},scaffolds=[];
 r.drawBuildingScaffold=(...args)=>scaffolds.push(args);
 const cases=[
  {project:{kind:'garden',work:14,progress:.5,stage:2},expected:0},
  {project:{kind:'lodge',work:0,progress:0,stage:0,waiting:true},expected:0},
  {project:{kind:'lodge',work:14,progress:.5,stage:2},expected:1},
  {project:{kind:'royalExpansion',work:14,progress:.5,stage:2,waiting:true},expected:1},
  {project:{kind:'warlordExpansion',work:14,progress:.5,stage:2},expected:1},
  {project:{kind:'lodge',work:35,progress:1,stage:4,done:true},expected:0}
 ];
 for(const{project,expected}of cases){scaffolds.length=0;s.projects=[project];const before=JSON.stringify(s);r.drawSettlement(ctx,s);assert.equal(scaffolds.length,expected,JSON.stringify(project));assert.equal(JSON.stringify(s),before)}
});
test('commissioned construction renders real worker stages and preserves the partial strip through a save',()=>{
 const{loadEngine}=require('./performance-harness.cjs'),engine=loadEngine(),g=new engine.ATSGame('CONSTRUCTION-SAVE');
 function open(game){const w=game.world;w.objects.clear();w._spatial.clear();w.sites.clear();w.ensure=()=>{};w.getSites=()=>[];w.terrain=()=>({biome:'forest',water:false});w.blocked=()=>false;w.lineClear=()=>true;w.getObjects=()=>[];w.settlementPlot=()=>({valid:true,trees:[],blocked:[]});w.syncSettlementBuildings=()=>{};game.colonies.plan=()=>{}}
 open(g);const s={id:'build-home',name:'Willow',x:0,y:0,population:60,level:6,radius:300,food:2000,wood:300,livingFounding:true,birthTimer:-100000};g.settlements.push(s);for(let i=0;i<60;i++)g.makeApe(0,0,'settled',s.id);g.syncIndexes();g.refreshSettlements();g.colonies.init(s);s.suitability={fertility:1,capacity:1000,water:true};g.colonies.layout(s);g.colonies.assignJobs(s,g.apes);
 const result=g.colonies.commission(s.id,'spearTower');assert.equal(result.ok,true,result.reason);const p=s.projects.find(p=>p.id===result.projectId),{Renderer,ctx,draws}=load(),r=new Renderer(),seen=new Set([0]);let saved=false;
 for(let i=0;i<160&&!p.done;i++){
  g.time++;g.colonies.tick(s);for(const a of g.apes){const target=g.colonies.activityTarget(a,s);a.x=target.x;a.y=target.y}seen.add(p.stage);
  if(p.stage<4){draws.length=0;r.drawConstruction(ctx,p);assert.deepEqual(draws[0].slice(1,5),manifest.construction.lookout[p.stage].rect)}
  if(p.stage===2&&!saved){const restored=engine.ATSGame.fromJSON(JSON.parse(JSON.stringify(g.serialize()))),copy=restored.settlements[0].projects.find(q=>q.id===p.id);assert.equal(copy.stage,p.stage);assert.equal(copy.progress,p.progress);draws.length=0;r.drawConstruction(ctx,copy);assert.deepEqual(draws[0].slice(1,5),manifest.construction.lookout[2].rect);saved=true}
 }
 assert.deepEqual([...seen],[0,1,2,3,4]);assert.equal(saved,true);const tower=s.facilities.find(f=>f.kind==='spearTower');assert.ok(tower);draws.length=0;r.drawSettlementProp(ctx,tower);assert.deepEqual(draws[0].slice(1,5),manifest.construction.lookout[4].rect);
});
