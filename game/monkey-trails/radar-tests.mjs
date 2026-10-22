import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {EncounterRadar} from './radar.mjs';
import {newSave,validate} from './core.mjs';
import {Vault,seal,unseal} from './storage.mjs';
const memory=()=>{const map=new Map();return {getItem:k=>map.get(k)||null,setItem:(k,v)=>map.set(k,v),removeItem:k=>map.delete(k),map};};
const sighting=()=>{const s=newSave('Map Monkey','capuchin','practice',42);s.run.ultra={id:'ultra-pinned',species:'solkar',bearing:0,spawnDistance:250,status:'sighting',safeAlternative:false};return s;};
const fix=(lat=40,lon=-75,timestamp=10000)=>({lat,lon,timestamp,meters:10,east:0,north:10});
test('an Ultra target stays geographically anchored; rejected readings cannot approach it',async()=>{
  let saved;const vault={loadDeviceData:async()=>null,saveDeviceData:async(_l,data)=>{saved=structuredClone(data);}};
  const radar=new EncounterRadar(),s=sighting();await radar.bind(vault);radar.observe(fix(),s);await radar.pending;
  const anchor=structuredClone(saved);assert.ok(Math.abs(radar.view(s,10000).distance-250)<.01);
  radar.observe({...fix(40.002),meters:0},s);assert.deepEqual(saved,anchor);assert.ok(radar.view(s,10000).distance>200);
  radar.observe(fix(40.002,-75,12000),s);assert.ok(radar.view(s,12000).distance<30.48);assert.equal(radar.view(s,12000).withinRadius,true);assert.equal(radar.view(s,23001).withinRadius,false);assert.deepEqual(saved,anchor);
  assert.ok(!JSON.stringify(s).includes('latitude'));assert.ok(!JSON.stringify(s).includes('-75'));
});
test('a new device recalibrates without free approach and preserves sighting identity',async()=>{
  const s=sighting(),radar=new EncounterRadar();await radar.bind({loadDeviceData:async()=>null,saveDeviceData:async()=>{}});
  assert.equal(radar.view(s,10000).calibrating,true);radar.observe(fix(45,10),s);assert.ok(radar.view(s,10000).distance>249);assert.equal(s.run.ultra.id,'ultra-pinned');
});
test('trusted stationary fixes refresh a pinned approach without distance or a new anchor',async()=>{
  const s=sighting(),radar=new EncounterRadar();let anchor;
  await radar.bind({loadDeviceData:async()=>null,saveDeviceData:async(_k,v)=>anchor=v});
  radar.observe(fix(),s);await radar.pending;const saved=structuredClone(anchor),distance=s.run.distance;
  radar.observe({...fix(anchor.lat,anchor.lon,30000),meters:0,positionValid:true},s);
  assert.equal(radar.view(s,30000).withinRadius,true);assert.deepEqual(anchor,saved);assert.equal(s.run.distance,distance);
  const other=new EncounterRadar();await other.bind({loadDeviceData:async()=>null,saveDeviceData:async()=>{}});other.observe({...fix(),meters:0,positionValid:true},s);assert.equal(other.view(s,10000).calibrating,true);
});
test('private device anchors are encrypted and absent from checkpoints and exports',async()=>{
  const m=memory(),v=new Vault(()=>{},m);const s=await v.open('Map Monkey','private-map-password',{create:true,mode:'practice'});
  await v.saveDeviceData('radar',{id:'pinned',lat:40.12345,lon:-75.98765});
  const restored=await v.loadDeviceData('radar');assert.equal(restored.lat,40.12345);
  assert.ok(![...m.map.values()].some(value=>value.includes('40.12345')));assert.ok(!(await v.export()).includes('pinned'));
  const checkpoint=await unseal(v.envelope,v.key,v.id);assert.equal(checkpoint.run.ultra,null);
  await v.saveDeviceData('radar',null);assert.equal(await v.loadDeviceData('radar'),null);
});
test('legacy migration preserves current expedition, evolution, equipment and story',async()=>{
  const legacy=JSON.parse(await readFile(new URL('./fixtures/v1-checkpoint.json',import.meta.url),'utf8'));
  const migrated=validate(legacy),p=migrated.run.monkey;
  assert.equal(migrated.version,2);assert.equal(migrated.run.distance,300);assert.equal(p.species,'capuchin');assert.equal(p.hp,42);assert.equal(p.energy,7);assert.equal(p.level,6);assert.equal(p.stage,2);assert.deepEqual(p.types,['leaf','fire']);assert.equal(p.training,96);assert.equal(migrated.meta.gear.fang,2);assert.equal(migrated.meta.history[0].id,'prior-run');
  assert.equal(migrated.run.party.length,1);assert.ok(Object.values(migrated.collection).some(m=>m.id===p.id));
  const again=validate(JSON.parse(JSON.stringify(migrated)));assert.equal(Object.keys(again.collection).length,1);assert.equal(again.run.monkey.id,p.id);
});
test('large valid encrypted checkpoints avoid argument-limit failures',async()=>{
  const m=memory(),v=new Vault(()=>{},m);const s=await v.open('Many Monkeys','many-monkey-password',{create:true,mode:'practice'});
  for(let i=0;i<120;i++){const p=structuredClone(s.run.monkey);p.id='owned-'+i;p.nickname='Companion '+i;s.collection[p.id]=p;}
  const e=await seal(s,v.key,v.salt,v.id);assert.ok(e.cipher.length>50000);const r=await unseal(e,v.key,v.id);assert.equal(Object.keys(r.collection).length,121);
});
