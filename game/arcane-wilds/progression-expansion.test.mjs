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

test('normal level-up reserves exactly one active spell evolution or mastery choice',()=>{
  assert.match(levelChoices,/activeSpells\|\|\[\]\)\.slice\(0,awCurrentSpellLimit\(\)\)/);
  assert.match(levelChoices,/const upgradeable=awUpgradeableOwnedSpells\(\)/);
  assert.match(levelChoices,/const candidates=upgradeable\.length\?upgradeable:activeIds/);
  assert.match(levelChoices,/ids\.push\(candidates\[irnd\(candidates\.length\)\]\)/);
  assert.match(levelChoices,/pool\.filter\(id=>!ids\.includes\(id\)&&!active\.has\(id\)\)/);
  assert.match(levelChoices,/Evolve active spell • Slot/);
  assert.match(levelChoices,/if\(!awIsSpellActive\(id\)\)return awEquipSpellForFlow\(id,\(\)=>openUpgradeChoice\(id\)\)/);
});

test('known inactive spell selections equip instead of being treated as active ownership',()=>{
  assert.match(levelChoices,/owned&&openSlot>=0\?`Equip known spell • fills Slot/);
  assert.match(levelChoices,/if\(awIsSpellActive\(id\)\)openUpgradeChoice\(id\);\s*else awEquipSpellForFlow\(id\)/s);
  assert.match(levelChoices,/keeps all \$\{upgrades\} existing mutation/);
  assert.match(levelChoices,/owned:active/);
});

test('selected spells automatically fill an unlocked empty fourth or fifth slot',()=>{
  assert.match(levelChoices,/function awFirstOpenSpellSlot\(\)/);
  assert.match(levelChoices,/const open=awFirstOpenSpellSlot\(\)/);
  assert.match(levelChoices,/if\(open>=0\)\{\s*awEquipSpellInSlot\(id,open\)/s);
  assert.match(levelChoices,/game\.player\.unlocked\.push\(id\);\s*awEquipSpellForFlow\(id\)/s);
  assert.match(source,/function awOpenEmptySpellSlot\(slot\)/);
  assert.match(source,/n<4\|\|n>5\|\|n>awSpellSlotLimit\(\)/);
  assert.match(source,/Choose Spell/);
});

test('an inactive spell cannot be upgraded until it has become equipped',()=>{
  assert.match(levelChoices,/function awEquipSpellForFlow\(id,onEquipped=null\)/);
  assert.match(levelChoices,/if\(!awIsSpellActive\(id\)\)return awEquipSpellForFlow\(id,\(\)=>openUpgradeChoice\(id\)\)/);
  assert.match(levelChoices,/openReplaceChoice\(id,onEquipped\)/);
  assert.match(levelChoices,/if\(typeof onEquipped==='function'\)\{\s*saveGame\(\);\s*updateHUD\(\);\s*onEquipped\(i\)/s);
});

test('spell assignment only writes into currently unlocked active slots',()=>{
  assert.match(levelChoices,/const limit=awCurrentSpellLimit\(\);\s*if\(!game\.player\|\|slot<0\|\|slot>=limit\)return false/s);
  assert.match(levelChoices,/for\(let i=0;i<active\.length;i\+\+\)if\(i!==slot&&active\[i\]===id\)active\[i\]=null/);
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
