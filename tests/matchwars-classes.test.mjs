import test from 'node:test';
import assert from 'node:assert/strict';
import { CLASSES, CLASS_CHOICES, DEFAULT_LOADOUT, normalizeLoadout, isClassUnlocked, getTroop, COMBOS } from '../game/matchwars/classes.mjs?v=3';
import { Battle, ENEMIES } from '../game/matchwars/combat.mjs';

const close = (a, b) => assert.ok(Math.abs(a - b) < 1e-6, `${a} is close to ${b}`);
const wait = (battle, seconds) => { for (let t = 0; t < seconds && battle.status === 'playing'; t += .05) battle.update(.05); };
function addEnemy(battle, definition = ENEMIES.ashling, x = 530, lane = 1, health = 800) {
  const unit = battle._makeUnit(definition, 'enemy', lane);
  unit.x = x; unit.hp = health; unit.maxHp = health; unit.cooldown = 100; unit.spellCooldown = 100;
  battle.units.push(unit);
  return unit;
}
function corpse(battle, x = 420, lane = 1) {
  const victim = addEnemy(battle, ENEMIES.ashling, x, lane, 10);
  battle._hit(null, victim, 10000);
  return victim;
}

test('each element has three complete, distinct classes and milestone unlocks are enforced', () => {
  assert.equal(Object.keys(CLASSES).length, 18);
  assert.equal(new Set(Object.values(CLASSES).map(def => def.spell.id)).size, 18);
  for (const [key, choices] of Object.entries(CLASS_CHOICES)) {
    assert.equal(choices.length, 3);
    for (const def of choices) {
      assert.equal(def.element, key); assert.equal(def.key, key);
      assert.ok(def.hp > 0 && def.attack > 0 && def.speed > 0 && def.range > 0 && def.interval > 0);
      assert.ok(def.spell.cooldown >= 7 && def.spell.cooldown <= 11);
      assert.ok(def.spell.name && def.spell.description && def.spell.type);
      assert.ok(!isClassUnlocked(def.id, def.unlockLevel - 1));
      assert.ok(isClassUnlocked(def.id, def.unlockLevel));
      assert.equal(normalizeLoadout({ [key]: def.id }, def.unlockLevel)[key], def.id);
      if (def.unlockLevel > 1) assert.equal(normalizeLoadout({ [key]: def.id }, def.unlockLevel - 1)[key], DEFAULT_LOADOUT[key]);
      assert.equal(getTroop(key, def.id), def);
      assert.ok(Object.isFrozen(def) && Object.isFrozen(def.spell));
    }
  }
  assert.equal(CLASSES.windcaller.unlockLevel, 60);
  assert.equal(CLASSES['sun-crusader'].unlockLevel, 72);
  assert.equal(CLASSES.hexweaver.unlockLevel, 80);
});

test('loadouts whitelist class IDs, reject cross-element/prototype data, and are snapshotted per battle', () => {
  assert.deepEqual(normalizeLoadout({ ember: 'necromancer', tide: 'constructor', void: 'made-up' }), DEFAULT_LOADOUT);
  assert.deepEqual(normalizeLoadout(Object.create({ void: 'necromancer' })), DEFAULT_LOADOUT);
  assert.equal(getTroop('toString', 'ember-mage'), null);
  const loadout = { ...DEFAULT_LOADOUT, void: 'necromancer' };
  const battle = new Battle({ stage: 1, loadout });
  loadout.void = 'hexweaver';
  assert.equal(battle.troopFor('void').id, 'necromancer', 'earned classes remain usable when replaying stage one');
  assert.ok(Object.isFrozen(battle.loadout));
  const necromancer = battle.summon('void')[0];
  assert.equal(necromancer.classId, 'necromancer');
  assert.equal(necromancer.spriteAtlas, 'classes');
  assert.equal(necromancer.spriteRow, 5);
  assert.equal(necromancer.spriteColumn, 0);
});

test('all eighteen player classes automatically cast their own spell on useful targets', () => {
  for (const def of Object.values(CLASSES)) {
    const events = [], battle = new Battle({ loadout: { [def.key]: def.id }, onEvent: event => events.push(event) });
    const caster = battle.summon(def.key)[0]; caster.x = 400; caster.spellCooldown = 0;
    const ally = battle.summon(def.key === 'sun' ? 'grove' : 'sun')[0]; ally.x = 415; ally.hp -= 45; ally.spellCooldown = 100;
    addEnemy(battle); addEnemy(battle, ENEMIES.ashling, 550); addEnemy(battle, ENEMIES.shade, 570);
    if (def.id === 'necromancer') corpse(battle);
    battle.update(.05);
    assert.equal(caster.spellCasts, 1, `${def.name} casts automatically`);
    assert.ok(caster.spellCooldown > 0 && caster.casting > 0);
    assert.ok(events.some(event => event.type === 'troop-spell' && event.unitId === caster.id && event.name === def.spell.name));
    assert.ok(battle.effects.some(effect => effect.type === def.spell.type));
    assert.equal(caster.state, 'cast');
  }
});

test('healers wait for wounds, oracle cleanses, cryomancer controls, and thorn guardian reflects', () => {
  const battle = new Battle({ loadout: { tide: 'tide-oracle' } });
  const oracle = battle.summon('tide')[0]; oracle.x = 400; oracle.spellCooldown = 0;
  battle.update(.05);
  assert.equal(oracle.spellCasts, 0, 'healers do not waste spells on healthy allies');
  const ally = battle.summon('sun')[0]; ally.x = 410; ally.hp -= 60; ally.spellCooldown = 100;
  battle._status(ally, 'poison', 5, 5); battle._status(ally, 'burn', 5, 5); battle._status(ally, 'curse', 5, .15);
  oracle.spellCooldown = 0; battle.update(.05);
  assert.ok(ally.hp > ally.maxHp - 60);
  assert.ok(!ally.statuses.poison && !ally.statuses.burn && !ally.statuses.curse);
  const cryo = new Battle({ loadout: { tide: 'cryomancer' } });
  const ice = cryo.summon('tide')[0]; ice.x = 400; ice.spellCooldown = 0;
  const foe = addEnemy(cryo); cryo.update(.05);
  assert.ok(foe.statuses.freeze);
  const frozenX = foe.x; wait(cryo, .5); assert.equal(foe.x, frozenX);
  const thorns = new Battle({ loadout: { grove: 'thorn-guardian' } });
  const guardian = thorns.summon('grove')[0], attacker = addEnemy(thorns);
  const before = attacker.hp; thorns._hit(attacker, guardian, 25);
  assert.ok(attacker.hp < before, 'melee strikes receive bounded thorn reflection');
});

test('Dawn Priest purges enemy undead and Phoenix Adept can rebirth only once', () => {
  const battle = new Battle({ loadout: { sun: 'dawn-priest', ember: 'phoenix-adept' } });
  const priest = battle.summon('sun')[0]; priest.x = 400; priest.spellCooldown = 0;
  const undead = addEnemy(battle, ENEMIES.shade, 500), living = addEnemy(battle, ENEMIES.shade, 520);
  undead.undead = true;
  battle.update(.05);
  assert.ok(undead.maxHp - undead.hp > living.maxHp - living.hp);
  const phoenix = battle.summon('ember')[0]; phoenix.shield = 0;
  const corpsesBefore = battle.corpses.length;
  battle._hit(null, phoenix, 10000);
  assert.equal(phoenix.dead, false); assert.equal(phoenix.reborn, true);
  assert.ok(phoenix.hp > 0); assert.equal(battle.corpses.length, corpsesBefore);
  battle._hit(null, phoenix, 10000);
  assert.equal(phoenix.dead, true); assert.equal(battle.corpses.length, corpsesBefore + 1);
});

test('formation bonds require living allies in one lane, trigger once, and have capped numerical effects', () => {
  assert.equal(COMBOS.length, 6);
  const events = [], battle = new Battle({ onEvent: event => events.push(event) });
  const grove = battle.summon('grove', { lane: 0 })[0];
  battle.summon('sun', { lane: 1 });
  assert.equal(battle.activeCombos(0).length, 0);
  const sun = battle.summon('sun', { lane: 0 })[0];
  assert.ok(battle.activeCombos(0).some(combo => combo.id === 'verdant-covenant'));
  grove.hp -= 50; const before = grove.hp; battle._heal(grove, 20); close(grove.hp - before, 24);
  grove.shield = 0; close(battle._shield(grove, 20), 24);
  for (let i = 0; i < 100; i++) battle._shield(grove, 100);
  assert.ok(grove.shield <= grove.maxHp * .8 + 50);
  wait(battle, 1);
  assert.equal(events.filter(event => event.type === 'combo' && event.id === 'verdant-covenant').length, 1);
  battle._hit(null, sun, 10000);
  assert.ok(!battle.activeCombos(0).some(combo => combo.id === 'verdant-covenant'));
});

test('Shatter, Soulfire, Steamguard and Thunderous Aegis change combat rather than labels', () => {
  const shatter = new Battle();
  const storm = shatter.summon('storm')[0]; shatter.summon('tide');
  const frozen = addEnemy(shatter, ENEMIES.frostwolf); shatter._status(frozen, 'slow', 3, .3);
  let hp = frozen.hp; shatter._hit(storm, frozen, 20); close(hp - frozen.hp, 20 * 1.5 * 1.25);
  const soul = new Battle(); const reaper = soul.summon('void')[0]; soul.summon('ember');
  const burning = addEnemy(soul, ENEMIES.thornling); soul._status(burning, 'burn', 3, 3);
  hp = burning.hp; soul._hit(reaper, burning, 20); close(hp - burning.hp, 25);
  const steam = new Battle(); const tide = steam.summon('tide')[0]; steam.summon('ember');
  const raider = addEnemy(steam); steam._status(raider, 'burn', 3, 3);
  hp = tide.hp; steam._hit(raider, tide, 20); close(hp - tide.hp, 20 * .7 * .85);
  tide.shield = 0; close(steam._shield(tide, 20), 22);
  const aegis = new Battle(); const lancer = aegis.summon('storm')[0]; aegis.summon('sun');
  lancer.x = 400; lancer.shield = 20; lancer.cooldown = 0; lancer.spellCooldown = 100;
  addEnemy(aegis, ENEMIES.ashling, 430); aegis.update(.05);
  close(lancer.cooldown, lancer.interval / 1.12);
});

test('necromancy consumes a corpse once; raised units decay and cannot create corpse or bounty loops', () => {
  const battle = new Battle({ loadout: { void: 'necromancer' } });
  const necromancer = battle.summon('void')[0]; necromancer.x = 400; necromancer.spellCooldown = 0;
  const body = corpse(battle);
  battle.update(.05);
  const guards = battle.units.filter(unit => unit.undead);
  assert.equal(guards.length, 1); assert.equal(necromancer.raises, 1);
  assert.equal(guards[0].corpseId, body.id); assert.equal(battle.corpses.length, 0);
  assert.equal(guards[0].spriteAtlas, 'enemies'); assert.equal(guards[0].spriteRow, 5);
  necromancer.spellCooldown = 0; battle.update(.05);
  assert.equal(necromancer.raises, 1);
  const kills = battle.kills, gold = battle.gold;
  guards[0].decay = .05; battle.update(.1);
  assert.equal(guards[0].dead, true); assert.equal(battle.corpses.length, 0);
  assert.equal(battle.kills, kills); assert.equal(battle.gold, gold);
  for (let i = 0; i < 8; i++) corpse(battle);
  for (let i = 0; i < 8; i++) battle._raiseGuard(necromancer);
  assert.equal(necromancer.raises, 5, 'each player necromancer has a finite lifetime raise budget');
});

test('Gravebloom heals undead fully and extends newly raised guards, with queue and army limits', () => {
  const battle = new Battle({ loadout: { void: 'necromancer' } });
  const caster = battle.summon('void')[0]; caster.x = 400;
  corpse(battle); assert.equal(battle._raiseGuard(caster), true);
  const first = battle.units.find(u => u.undead); first.hp -= 30;
  let hp = first.hp; battle._heal(first, 20); close(first.hp - hp, 10);
  battle.summon('grove'); first.hp -= 30;
  hp = first.hp; battle._heal(first, 20); close(first.hp - hp, 20);
  corpse(battle); assert.equal(battle._raiseGuard(caster), true);
  const second = battle.units.filter(u => u.undead)[1]; assert.equal(second.decay, 22);
  for (let i = 0; i < 40; i++) corpse(battle);
  assert.equal(battle.corpses.length, 32);
  for (let i = 0; i < 100; i++) battle.summon('sun', { lane: i % 3, count: 49 });
  const before = battle.corpses.filter(body => !body.consumed).length;
  assert.equal(battle._raiseGuard(caster), false);
  assert.equal(battle.corpses.filter(body => !body.consumed).length, before, 'a full army does not waste corpses');
  assert.ok(battle.units.filter(u => !u.dead && u.side === 'player').length <= 40);
  battle._schedule = [{ time: 999, wave: 1, lane: 1, key: 'ashling' }]; battle._next = 0;
  wait(battle, 13); assert.equal(battle.corpses.length, 0);
});

test('all twelve new enemy species cast distinct automatic spells and enemy raises are bounded', () => {
  const fresh = Object.values(ENEMIES).filter(def => def.spriteAtlas === 'enemies');
  assert.equal(fresh.length, 12); assert.equal(new Set(fresh.map(def => def.spell.id)).size, 12);
  for (const def of fresh) {
    const battle = new Battle({ stage: 24 });
    const foe = battle._makeUnit(def, 'enemy', 1); foe.x = 500; foe.spellCooldown = 0; battle.units.push(foe);
    const friend = battle._makeUnit(ENEMIES.ashling, 'enemy', 1); friend.x = 510; friend.hp -= 20; friend.spellCooldown = 100; battle.units.push(friend);
    const target = battle.summon('sun')[0]; target.x = 400; target.spellCooldown = 100;
    if (def.key === 'graveknight') corpse(battle, 490);
    battle.update(.05);
    assert.equal(foe.spellCasts, 1, `${def.name} uses its advertised spell`);
    assert.ok(battle.effects.some(effect => effect.type === def.spell.type));
  }
  const battle = new Battle(); const knight = battle._makeUnit(ENEMIES.graveknight, 'enemy', 1); knight.x = 400; battle.units.push(knight);
  for (let i = 0; i < 6; i++) corpse(battle);
  for (let i = 0; i < 6; i++) battle._raiseGuard(knight);
  assert.equal(knight.raises, 3);
  const raised = battle.units.find(u => u.undead); const gold = battle.gold, kills = battle.kills, bodies = battle.corpses.length;
  battle._hit(null, raised, 10000);
  assert.equal(battle.gold, gold); assert.equal(battle.kills, kills); assert.equal(battle.corpses.length, bodies);
});
