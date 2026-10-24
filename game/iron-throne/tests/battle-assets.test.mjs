import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {buildBattlePack} from '../tools/battle-model-source.mjs';
import {validateBattlePack} from '../battle-assets.mjs';
import {UNITS,CAMPAIGN_HOUSES} from '../data.mjs';
import {terrainHeight} from '../battle-sim.mjs';
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
