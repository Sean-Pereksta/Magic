'use strict';
const assert=require('node:assert/strict'),path=require('node:path');
const {bootUI}=require('./command-ui-test.cjs');
const ROOT=process.argv[2]||path.resolve(__dirname,'..');
const ui=bootUI(ROOT),D=ui.window.GameData,el=id=>ui.document.getElementById(id);
const change=(id,value)=>{el(id).value=value;el(id).dispatch('change');};
function paint(type,x,y,x2=x,y2=y){
  const tool=ui.document.querySelector(`[data-terrain-tool="${type}"]`);assert(tool);tool.click();
  el('battlefield').dispatch('pointerdown',{clientX:x,clientY:y});
  if(x!==x2||y!==y2)el('battlefield').dispatch('pointermove',{clientX:x2,clientY:y2});
  el('battlefield').dispatch('pointerup',{clientX:x2,clientY:y2});
}
function load(config){
  ui.storage.set('ironfield.scenarios.v1',JSON.stringify([{format:'ironfield-scenario',version:2,name:config.name,config}]));
  el('scenariosButton').click();const list=el('savedScenarios');list.dispatch('click',{target:list.querySelector('[data-load-scenario="0"]')});
}
assert.equal(D.MAX_UNITS,40000);
const massive={name:'Forty thousand through the real load controls',map:'custom',seed:31337,
  customMap:{name:'Great muster',width:16000,height:11000,biome:'grassland',regions:[]},
  armies:[{name:'Western army',doctrine:'steady',roster:{militia:18000,archer:2000}},{name:'Eastern army',doctrine:'steady',roster:{militia:18000,archer:2000}}]};
load(massive);
assert.equal(ui.engine.units.length,40000);
assert.equal(ui.engine.getStats().teams[0].initial,20000);assert.equal(ui.engine.getStats().teams[1].initial,20000);
const input=el('armyList').querySelector('[data-roster="militia"]');assert.equal(input.getAttribute('max'),'20000');
el('scenarioName').value='Saved forty thousand';el('saveScenario').click();
const full=JSON.parse(ui.storage.get('ironfield.scenarios.v1'))[0];
assert.equal(full.config.formations.reduce((sum,f)=>sum+f.count,0),40000,'Save truncated the doubled army');
assert.equal(full.config.formations.reduce((sum,f)=>sum+(f.offsets?.length||0),0),40000,'Save dropped large-army formation offsets');
console.log('PASS 40,000 troops load through the interface and all soldier offsets survive Save');
el('newEncounter').click();change('customWorldSize','colossal');
for(const [i,type] of ['road','mud','marsh','snow','trench'].entries()){
  const x=1200+i*1200;paint(type,x,1600,x+500,1900);
  assert.equal(ui.engine.terrainAt(x+250,1750).type,type);
  assert.equal(ui.engine.terrainAt(x+250,1750).blocked,false);
}
// Later passable paint must agree with what the player sees on top.
paint('road',2400,1600,2900,1900);assert.equal(ui.engine.terrainAt(2650,1750).type,'road');
console.log('PASS All five new terrain brushes work, including a road over older mud');
el('clearTerrain').click();paint('water',4350,2200);change('fortressStyle','curtain');paint('castle',3000,2200);
assert.equal(ui.engine.structures.length,0,'A secondary gate was built over unbridged water');
assert.match(ui.toast().at(-1),/Every entrance/);
for(const kind of Object.keys(D.FORTRESS_TYPES)){
  el('clearTerrain').click();change('fortressStyle',kind);paint('castle',8000,5500);
  assert(ui.engine.structures.length>4,'Castle template failed: '+kind);
  assert(ui.engine.structures.every(s=>s.structureKind),'Structure material was stripped: '+kind);
  assert(ui.engine.structures.some(s=>s.castleGate),'Template has no actual operated gate: '+kind);
  if(kind==='moated'){assert(ui.engine.map.regions.some(r=>r.type==='water'));assert.equal(ui.engine.map.regions.filter(r=>r.type==='bridge').length,4);}
  const count=ui.engine.map.regions.length;assert(count<=D.MAX_TERRAIN_REGIONS);
  el('undoTerrain').click();assert.equal(ui.engine.map.regions.length,0,'Undo did not remove the entire castle: '+kind);
}
console.log('PASS Every castle style places physical gates, preserves materials, and undoes as one edit');
for(const [i,kind] of ['house','watchtower','barricade','keep'].entries())paint(kind,2500+i*2200,5000);
for(const kind of ['house','watchtower','barricade','keep'])assert(ui.engine.structures.some(s=>s.structureKind===kind),kind+' did not create its real footprint');
el('scenarioName').value='New buildings preserved';el('saveScenario').click();
const saved=JSON.parse(ui.storage.get('ironfield.scenarios.v1'))[0];
for(const kind of ['house','watchtower','barricade','keep'])assert(saved.config.customMap.regions.some(r=>r.structureKind===kind));
el('newEncounter').click();load(saved.config);
for(const kind of ['house','watchtower','barricade','keep'])assert(ui.engine.structures.some(s=>s.structureKind===kind&&s.name));
console.log('PASS Houses, towers, barricades, and hollow keeps retain physical materials and names through save/load');
el('menuButton').click();ui.document.querySelector('[data-map-filter="new"]').click();
const visible=ui.document.querySelectorAll('[data-menu-map]').filter(e=>!e.hidden);
assert.equal(visible.length,8);assert.equal(visible.length,D.EXPANSION_MAPS.length);
assert.equal(D.HISTORICAL_BATTLES.length,30);
console.log('PASS Atlas exposes all eight new battlefields and thirty historical battles');
(async()=>{
  const payload=JSON.stringify(full,null,2),file=el('scenarioFile');
  assert(Buffer.byteLength(payload)<8*1024*1024,'An exported maximum-size battle exceeds its import limit');
  file.files=[{size:Buffer.byteLength(payload),text:async()=>payload}];
  await file.dispatchAsync('change');
  assert.equal(ui.engine.units.length,40000,'Real file import truncated the doubled battle');
  assert.equal(ui.window.IRONFIELD.config.formations.reduce((sum,f)=>sum+f.count,0),40000);
  console.log('PASS A full 40,000-soldier saved file imports through the actual file handler');
})().catch(error=>{console.error(error);process.exitCode=1;});
