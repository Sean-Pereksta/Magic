import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {dirname,join} from 'node:path';

const here=dirname(fileURLToPath(import.meta.url));
const source=readFileSync(join(here,'content-pack-30.js'),'utf8');
const html=readFileSync(join(here,'..','arcane-wilds.html'),'utf8');

const spells=['voidGuillotine','thunderCathedral','dragonfireTorrent','crystalRailway','moonfall','spiritStampede','worldroot','starbreaker','arcaneRailgun','runicBarrage'];
const enemies=['aw30_sunmoth','aw30_mimic','aw30_cinderwheel','aw30_mirrorknight','aw30_boglantern','aw30_stormram','aw30_puppeteer','aw30_burrower','aw30_ashchoir','aw30_behemoth'];
const items=['Worldroot Staff','Frostbite Fang','Graviton Scepter','Mirrorsteel Breastplate','Stormglass Crown','Riftwalker Boots','Meteor-Iron Mantle','Sunforged Gauntlet','Cinderheart','Choir Bell'];

test('content pack registers ten new abilities',()=>{
  for(const id of spells)assert.match(source,new RegExp(`${id}:`));
  assert.equal(spells.length,10);
});

test('content pack registers ten custom enemy types and AI routes',()=>{
  for(const id of enemies){assert.match(source,new RegExp(`${id}:`));assert.match(source,new RegExp(`e\\.ai==='${id}'`));}
  assert.equal(enemies.length,10);
});

test('content pack contains ten named items with special hooks',()=>{
  for(const name of items)assert.ok(source.includes(`name:'${name}'`),name);
  for(const special of ['worldrootStaff','frostbiteFang','gravitonScepter','mirrorsteel','stormglassCrown','riftwalker','meteorMantle','sunforgedGauntlet','cinderheart','choirBell'])assert.ok(source.includes(`'${special}'`),special);
  assert.equal(items.length,10);
});

test('content pack loads after ability visuals and before mobile interaction',()=>{
  const ability=html.indexOf('arcane-wilds/ability-visuals.js');
  const pack=html.indexOf('arcane-wilds/content-pack-30.js');
  const mobile=html.indexOf('arcane-wilds/mobile-interaction.js');
  assert.ok(ability>=0&&pack>ability&&mobile>pack);
});