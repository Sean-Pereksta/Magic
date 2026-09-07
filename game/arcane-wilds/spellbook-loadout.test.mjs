import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const here=path.dirname(fileURLToPath(import.meta.url));
const source=fs.readFileSync(path.join(here,'spellbook-loadout.js'),'utf8');
const html=fs.readFileSync(path.join(here,'..','arcane-wilds.html'),'utf8');

test('Spellbook manager loads after dynamic spell-slot progression',()=>{
  const progression=html.indexOf('arcane-wilds/progression-expansion.js');
  const loadout=html.indexOf('arcane-wilds/spellbook-loadout.js');
  assert.ok(progression>=0,'progression expansion should be loaded');
  assert.ok(loadout>progression,'Spellbook manager must load after progression expansion');
  assert.match(source,/window\.awSpellSlotLimit/);
});

test('Spellbook renders five possible slots while respecting the current 3 to 5 capacity',()=>{
  assert.match(source,/Array\.from\(\{length:5\}/);
  assert.match(source,/if\(i>=limit\)return/);
  assert.match(source,/Your three core slots are available/);
  assert.match(source,/opened slot 4/);
  assert.match(source,/Maximum five-spell loadout unlocked/);
});

test('known spells support drag and tap based equipping',()=>{
  assert.match(source,/draggable="true" data-aw-spell-id/);
  assert.match(source,/addEventListener\('dragstart'/);
  assert.match(source,/addEventListener\('drop'/);
  assert.match(source,/if\(selected\)\{moveSpellToSlot\(selected\.id,target,selected\.sourceSlot\)/);
  assert.match(source,/On touch, tap a spell and then tap its destination slot/);
});

test('moving an equipped spell swaps slots and moving a known inactive spell replaces the target',()=>{
  assert.match(source,/const displaced=active\[targetSlot\]\|\|null/);
  assert.match(source,/active\[targetSlot\]=id/);
  assert.match(source,/if\(source>=0&&source!==targetSlot\)active\[source\]=displaced/);
  assert.match(source,/previous spell.*returned to the Spellbook/);
});

test('Spellbook can unequip spells without forgetting the learned spell or mutations',()=>{
  assert.match(source,/function unequipSlot\(slot\)/);
  assert.match(source,/active\[slot\]=null/);
  assert.match(source,/game\.player\?\.upgrades\?\.\[id\]/);
  assert.doesNotMatch(source,/unlocked\.splice|delete game\.player\.upgrades/);
});

test('loadout changes are saved and refresh the live HUD',()=>{
  assert.match(source,/if\(typeof saveGame==='function'\)saveGame\(\)/);
  assert.match(source,/if\(typeof updateHUD==='function'\)updateHUD\(\)/);
  assert.match(source,/renderInventory\(\)/);
});