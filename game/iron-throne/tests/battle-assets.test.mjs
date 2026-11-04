import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {buildBattlePack} from '../tools/battle-model-source.mjs';
import {validateBattlePack} from '../battle-assets.mjs';
import {UNITS,CAMPAIGN_HOUSES} from '../data.mjs';
import {createBattle,beginBattle,stepBattle,terrainHeight} from '../battle-sim.mjs';
import {battlefieldClimate,TERRAIN_STEP} from '../battle-terrain.mjs';
import {battleGroundHeight} from '../battle-renderer.mjs';
test('shipped model package is reproducible and covers every roster entry and House',async()=>{
  const shipped=await readFile(new URL('../assets/battle/v1/pack.json',import.meta.url),'utf8'),pack=validateBattlePack(JSON.parse(shipped));
  assert.equal(JSON.stringify(buildBattlePack()),shipped);
  for(const [type,unit]of Object.entries(UNITS)){const model=pack.models[type];assert.equal(model.family,unit.family);assert.ok(model.nodes.length>=20);const names=new Set();for(const node of model.nodes){if(node.parent)assert.ok(names.has(node.parent),`${type}: ${node.parent} must precede child ${node.name}`);names.add(node.name);if(node.mesh&&node.mesh!=='heraldry')assert.ok(pack.meshes[node.mesh]);}assert.ok(model.clips.includes('attack'));}
  for(const h of CAMPAIGN_HOUSES){assert.equal(pack.houses[h.id].color,h.color);assert.ok(pack.meshes[pack.houses[h.id].mesh]);}
  for(const weapon of ['sword','spear','axe','mace','bow','crossbow','lance'])assert.ok(pack.weapons[weapon].length>1);
  const manifest=JSON.parse(await readFile(new URL('../assets/battle/v1/manifest.json',import.meta.url),'utf8'));
  assert.equal(manifest.files[0].sha256,createHash('sha256').update(shipped).digest('hex'));
});
test('damaged and incomplete asset packages are rejected before combat starts',()=>{
  assert.throws(()=>validateBattlePack({}),/incompatible/);
  const missing=buildBattlePack();delete missing.models.trebuchet;assert.throws(()=>validateBattlePack(missing),/trebuchet/);
  const broken=buildBattlePack();broken.meshes.torso.positions[0]=NaN;assert.throws(()=>validateBattlePack(broken),/damaged/);
});
test('renderer terrain heights match physical movement and picking',()=>{
  for(const terrain of ['plains','forest','hills','river','pass','desert'])for(const [x,z]of [[0,0],[24,-37],[-50,70]])assert.equal(battleGroundHeight({terrain},x,z),terrainHeight({layout:{terrain}},x,z));
});
test('seeded terrain keeps the same triangulated surface through a save and blocks a shot crossing a ridge',()=>{
  const b=createBattle({id:'ridge-cover',seed:'ridge',playerOwner:'ashen',tile:{terrain:'hills'},participants:[
    {armyId:'a',side:0,owner:'ashen',units:{archer:1}},{armyId:'d',side:1,owner:'wintermere',units:{menAtArms:1}}
  ]});
  const layout=b.layout,restored=structuredClone(layout),s=TERRAIN_STEP;
  for(const [x,z] of [[.8,.2],[1.2,1.8],[25,-10],[-53,37]]){
    assert.equal(terrainHeight(b,x,z),battleGroundHeight(restored,x,z));
    assert.notEqual(battleGroundHeight({...layout,climate:battlefieldClimate('other')},x,z),terrainHeight(b,x,z));
  }
  const a=terrainHeight(b,0,0),xEdge=terrainHeight(b,s,0),zEdge=terrainHeight(b,0,s);
  assert.equal(terrainHeight(b,s*.25,s*.25),a+(xEdge-a)*.25+(zEdge-a)*.25);
  beginBattle(b);
  // Put the arrow's origin and destination in the low ground on either side of the crest.
  let best=-Infinity,line;
  for(let z=-76;z<=30;z++){
    const end=z+45,startY=terrainHeight(b,0,z)+1.6,endY=terrainHeight(b,0,end)+1.05;
    const margin=terrainHeight(b,0,z+22.5)-(startY+endY)/2;
    if(margin>best){best=margin;line={z,end,startY,endY};}
  }
  assert.ok(best>.22);
  const [archer,enemy]=b.units;
  Object.assign(archer,{x:0,z:line.z,y:line.startY-1.6,holdFire:true});
  Object.assign(enemy,{x:0,z:line.end,y:line.endY-1.05,order:{kind:'hold',x:0,z:line.end},explicitOrder:true});
  b.projectiles.push({id:'ridge-arrow',sourceId:archer.id,kind:'arrow',side:0,x:0,z:line.z,y:line.startY,startX:0,startZ:line.z,startY:line.startY,targetX:0,targetZ:line.end,targetY:line.endY,duration:1,elapsed:0,damage:50});
  for(let i=0;i<22&&b.projectiles.length;i++)stepBattle(b,.05,{});
  assert.ok(b.events.some(e=>e.type==='missileBlocked'&&Math.abs(e.y-terrainHeight(b,e.x,e.z))<.1));
  assert.equal(enemy.hp,enemy.maxHp);
});
