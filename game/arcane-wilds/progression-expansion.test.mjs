import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {dirname,join} from 'node:path';

const here=dirname(fileURLToPath(import.meta.url));
const source=readFileSync(join(here,'progression-expansion.js'),'utf8');
const levelChoices=readFileSync(join(here,'level-up-choices.js'),'utf8');
const html=readFileSync(join(here,'..','arcane-wilds.html'),'utf8');

const spells=['runicNeedle','celestialFurnace','eventideGate','seraphicArray'];
const slotItems=[
  'Runebound Conductor',
  "Spellthief's Longbow",
  'Twin-Sigil Scepter',
  'Riftglass Vestments',
  'Grand Arcanum Mantle',
  'Covenant Prism',
  'Fifth-Star Reliquary',
  'Witchroad Signet'
];

test('adds one early spell and three late-game spells',()=>{
  for(const id of spells)assert.match(source,new RegExp(`${id}:`));
  assert.match(source,/runicNeedle:\{name:'Runic Needle'.*rarity:'Common'/s);
  assert.match(source,/celestialFurnace:\{name:'Celestial Furnace'.*rarity:'Epic'/s);
  assert.match(source,/eventideGate:\{name:'Eventide Gate'.*rarity:'Legendary'/s);
  assert.match(source,/seraphicArray:\{name:'Seraphic Array'.*rarity:'Legendary'/s);
});

test('adds eight rare or legendary spell-slot items and caps capacity at five',()=>{
  for(const name of slotItems)assert.ok(source.includes(`name:'${name}'`)||source.includes(`name:"${name}"`),name);
  assert.equal(slotItems.length,8);
  assert.match(source,/spellSlotBonus:1/);
  assert.match(source,/spellSlotBonus:2/);
  assert.match(source,/return Math\.min\(5,3\+bonus\)/);
  assert.ok(!source.includes("minRarity:'Common'"));
  assert.ok(!source.includes("minRarity:'Uncommon'"));
});

test('normal level-up evolution is restricted to exactly one active spell choice',()=>{
  assert.match(levelChoices,/activeSpells\|\|\[\]\)\.slice\(0,awCurrentSpellLimit\(\)\)/);
  assert.match(levelChoices,/const upgradeable=awUpgradeableOwnedSpells\(\)/);
  assert.match(levelChoices,/const remainingPool=pool\.filter\(id=>!ids\.includes\(id\)&&!active\.has\(id\)\)/);
  assert.match(levelChoices,/Evolve active spell • Slot/);
  assert.match(levelChoices,/if\(!awIsSpellActive\(id\)\)return openReplaceChoice\(id\)/);
});

test('known inactive spells retain mutations and reassign instead of evolving',()=>{
  assert.match(levelChoices,/Reassign known spell • upgrades retained/);
  assert.match(levelChoices,/if\(awIsSpellActive\(id\)\)openUpgradeChoice\(id\);\s*else openReplaceChoice\(id\)/s);
  assert.match(levelChoices,/keeps all \$\{upgrades\} existing mutation/);
});

test('known inactive spells can fill newly unlocked fourth and fifth slots',()=>{
  assert.match(source,/function awOpenEmptySpellSlot\(slot\)/);
  assert.match(source,/n<4\|\|n>5\|\|n>awSpellSlotLimit\(\)/);
  assert.match(source,/Choose Spell/);
});

test('villages are slightly more common and have additional names',()=>{
  assert.match(source,/hash2\(x,y,game\.seed\+71\)>\.925/);
  for(const name of ['Moonwell Refuge','Embercross','Lanternrest','Willowmere','Frostbell','Starfall Rest','Rookhaven','Cinderwatch'])assert.ok(source.includes(name),name);
});

test('progression expansion loads after cloud save integration so it owns final UI wrappers',()=>{
  const cloud=html.indexOf('arcane-wilds/cloud-save-integration.js');
  const progression=html.indexOf('arcane-wilds/progression-expansion.js');
  assert.ok(cloud>=0&&progression>cloud);
  assert.ok(html.includes('Rare 4th & 5th spell slots'));
});
