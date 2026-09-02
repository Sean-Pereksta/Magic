import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {spawnSync} from 'node:child_process';
import test from 'node:test';

const sourcePath=new URL('./grand-update.js',import.meta.url);
const source=readFileSync(sourcePath,'utf8');
const html=readFileSync(new URL('../arcane-wilds.html',import.meta.url),'utf8');

function section(start,end){
  const a=source.indexOf(start),b=source.indexOf(end,a+start.length);
  assert.ok(a>=0&&b>a,`missing section ${start}`);
  return source.slice(a,b);
}

test('grand update is valid JavaScript and loads after regional content but before final mobile/performance wrappers',()=>{
  const checked=spawnSync(process.execPath,['--check',sourcePath.pathname],{encoding:'utf8'});
  assert.equal(checked.status,0,checked.stderr);
  const regionalAt=html.indexOf('arcane-wilds/regional-expansion.js');
  const grandAt=html.indexOf('arcane-wilds/grand-update.js');
  const mobileAt=html.indexOf('arcane-wilds/mobile-interaction.js');
  assert.ok(regionalAt>=0&&grandAt>regionalAt&&mobileAt>grandAt);
});

test('adds at least ten spells, weapons, and enemies',()=>{
  const spells=section('const NEW_SPELLS={','for(const [id,spell]');
  const weapons=section('const NEW_WEAPONS=[','WEAPON_BASES.push');
  const enemies=section('Object.assign(ENEMY_TYPES,{','const NEW_ENEMY_IDS');
  assert.ok((spells.match(/upgradeBase:/g)||[]).length>=10);
  assert.ok((weapons.match(/\{name:/g)||[]).length>=10);
  assert.ok((enemies.match(/awx_[a-z]+:/g)||[]).length>=10);
});

test('new spells inherit working mutation behavior from established spell families',()=>{
  assert.match(source,/UPGRADE_POOLS\[id\]=source\.map/);
  assert.match(source,/u\[4\]/);
  assert.match(source,/cast:'icelance'/);
  assert.match(source,/cast:'firebolt'/);
  assert.match(source,/cast:'chain'/);
  assert.match(source,/cast:'spirits'/);
});

test('performance pass only reduces cosmetics and leaves gameplay entity collections alone',()=>{
  assert.match(source,/game\.particles\.splice/);
  assert.match(source,/opts\.trail==='weapon'/);
  assert.match(source,/kind==='muzzle'/);
  assert.doesNotMatch(source,/game\.enemies\.splice/);
  assert.doesNotMatch(source,/game\.projectiles\.splice/);
  assert.doesNotMatch(source,/game\.hazards\.splice/);
  assert.doesNotMatch(source,/game\.telegraphs\.splice/);
});

test('catalog exposes spells, weapons, enemies, search, HUD access, and main-menu access',()=>{
  assert.match(source,/data-catalog-tab="spells"/);
  assert.match(source,/data-catalog-tab="weapons"/);
  assert.match(source,/data-catalog-tab="enemies"/);
  assert.match(source,/id="catalogSearch"/);
  assert.match(source,/id='catalogBtn'/);
  assert.match(source,/id='startCatalogBtn'/);
});
