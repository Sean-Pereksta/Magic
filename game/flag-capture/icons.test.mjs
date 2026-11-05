import test from 'node:test';
import assert from 'node:assert/strict';
import {FACTIONS} from './engine.mjs';
import {abilityIcon,ABILITY_ICON_KEYS} from './icons.mjs';
test('every troop skill has its own drawn icon and accessible buttons can label it separately',()=>{
  const skills=Object.values(FACTIONS).flatMap(f=>Object.values(f.units).flatMap(u=>u.abilities||[u.ability]));
  assert.equal(skills.length,21);
  assert.equal(new Set(skills.map(s=>abilityIcon(s.kind))).size,21);
  for(const skill of skills){assert.ok(ABILITY_ICON_KEYS.includes(skill.kind),skill.name);assert.match(abilityIcon(skill.kind),/^<svg[^>]+aria-hidden="true"/);}
});
