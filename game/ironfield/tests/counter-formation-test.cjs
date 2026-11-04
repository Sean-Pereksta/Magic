'use strict';
// Public-combat armor, counter and shield-formation checks.
// Run: node tests/counter-formation-test.cjs [alternate-project-root]
const assert = require('node:assert/strict');
const path = require('node:path');
const { performance } = require('node:perf_hooks');

function register(test) {
  const D = global.GameData, Engine = global.BattleEngine, seeds = [4719, 731, 120001];
  const rounded = n => Math.round(n * 1000) / 1000;
  const median = a => a.slice().sort((x, y) => x - y)[Math.floor(a.length / 2)];
  const mean = a => a.reduce((s, n) => s + n, 0) / Math.max(1, a.length);
  const config = (blue, red, seed = 4719, regions = []) => ({ map: 'custom', seed, customMap: { biome: 'grassland', regions },
    armies: [{ name: 'Azure', doctrine: 'balanced', roster: blue }, { name: 'Ember', doctrine: 'balanced', roster: red }] });
  const living = (e, r) => r.unitIds.map(id => e.units[id]).filter(u => u.alive && !u.routed);
  const health = (e, r) => r.unitIds.reduce((sum, id) => sum + Math.max(0, e.units[id].hp), 0);
  const center = (e, r) => { const us = living(e, r); return { x: mean(us.map(u => u.x)), y: mean(us.map(u => u.y)) }; };
  const snapshot = e => e.units.map(u => ({ x: u.x, y: u.y, hp: u.hp, angle: u.angle, attack: u._lastAttack, target: u.target }));
  function audit(e, before) {
    const buckets = new Map(); let minimum = Infinity, maximumStep = 0;
    for (const u of e.units) if (u.alive && !u.routed) {
      const p = before[u.id], radius = D.TYPES[u.type].radius;
      assert([u.x, u.y, u.hp, u.angle].every(Number.isFinite), 'Counter combat produced nonfinite physical state');
      assert(u.x >= 0 && u.x <= e.width && u.y >= 0 && u.y <= e.height);
      assert.equal(e.terrainAt(u.x, u.y).blocked, false, 'A body moved into impassable terrain');
      for (const block of e.map.regions) if (block.type === 'rock') {
        const x = Math.max(block.x, Math.min(block.x + block.w, u.x)), y = Math.max(block.y, Math.min(block.y + block.h, u.y));
        assert(Math.hypot(u.x - x, u.y - y) >= radius - .1, 'A pushed body crossed a rock boundary');
      }
      const step = Math.hypot(u.x - p.x, u.y - p.y); maximumStep = Math.max(maximumStep, step);
      assert(step <= D.TYPES[u.type].speed / 30 * 1.8 + .25, 'Counter combat teleported or over-accelerated a body');
      const gx = Math.floor(p.x / 64), gy = Math.floor(p.y / 64);
      for (let y = gy - 1; y <= gy + 1; y++) for (let x = gx - 1; x <= gx + 1; x++) for (const v of buckets.get(x + ':' + y) || []) {
        const q = before[v.id], combined = radius + D.TYPES[v.type].radius;
        if (Math.abs(p.x - q.x) > combined + 8 || Math.abs(p.y - q.y) > combined + 8) continue;
        const px = p.x - q.x, py = p.y - q.y, vx = u.x - p.x - v.x + q.x, vy = u.y - p.y - v.y + q.y, vv = vx * vx + vy * vy;
        const t = vv ? Math.max(0, Math.min(1, -(px * vx + py * vy) / vv)) : 0;
        minimum = Math.min(minimum, Math.hypot(px + vx * t, py + vy * t) / combined);
      }
      const key = gx + ':' + gy; if (!buckets.has(key)) buckets.set(key, []); buckets.get(key).push(u);
    }
    assert(minimum >= .90, 'Counter combat swept soldiers through occupied bodies');
    for (const s of e.getStats().teams) assert.equal(s.initial, s.alive + s.dead + s.routed);
    return { minimum, maximumStep };
  }

  function firstHit(attacker, defender, seed) {
    const a = D.TYPES[attacker], d = D.TYPES[defender];
    const distance = a.ranged ? Math.max(120, Math.min(200, a.range * .55)) : a.range + d.radius - 1;
    const source = config({ [attacker]: 1 }, { [defender]: 1 }, seed);
    source.formations = [
      { team: 0, type: attacker, count: 1, x: 2800, y: 2200, facing: 0, formation: 'line', offsets: [{ x: 0, y: 0 }] },
      { team: 1, type: defender, count: 1, x: 2800 + distance, y: 2200, facing: Math.PI, formation: 'line', offsets: [{ x: 0, y: 0 }] }
    ];
    const e = new Engine(source), shooter = e.units[0], victim = e.units[1];
    // A lethal artillery hit can exceed a levy's entire health pool. Observe
    // its actual damage before casualty bookkeeping floors the display at zero.
    let overkill = 0;
    const kill = e._kill;
    e._kill = function(unit, ...args) {
      if (unit === victim) overkill = Math.max(0, -unit.hp);
      return kill.call(this, unit, ...args);
    };
    e.command(e.regiments.map(r => r.id), 'hold'); e.start();
    let launched = false, attacked = false, damage = 0, at = null;
    for (let tick = 0; tick < Math.ceil(Math.max(15, a.cooldown + 3) * 30) && e.phase === 'running'; tick++) {
      const previous = snapshot(e); e.step(1 / 30); audit(e, previous);
      if (shooter._lastAttack > previous[shooter.id].attack) attacked = true;
      if (e.projectiles.some(p => p.attacker === shooter.id && p.team === 0 && p.age <= 1 / 30 + 1e-6)) launched = true;
      if (victim.hp < previous[victim.id].hp) {
        damage = previous[victim.id].hp - victim.hp + overkill; at = e.time;
        if (!a.ranged) assert(Math.hypot(shooter.x - victim.x, shooter.y - victim.y) <= a.range + d.radius + .01, 'A sampled melee hit was beyond actual reach');
        break;
      }
    }
    assert(damage > 0 && at !== null && attacked, `${attacker} failed to land a real attack on ${defender}`);
    if (a.ranged) assert(launched, `${attacker} damaged its distant target without a real launched projectile`);
    return { attacker, defender, seed, damage, firstHit: at };
  }

  let armorSamples;
  function getArmorSamples() {
    if (armorSamples) return armorSamples;
    const attacking = [], defending = [];
    for (const seed of seeds) for (const attacker of D.TYPE_ORDER) for (const defender of ['militia', 'swordsman', 'guardian']) attacking.push(firstHit(attacker, defender, seed));
    for (const seed of seeds) for (const defender of D.TYPE_ORDER) for (const attacker of ['archer', 'crossbow']) defending.push(firstHit(attacker, defender, seed));
    return armorSamples = { attacking, defending };
  }
  const sampledMedian = (samples, attacker, defender) => median(samples.filter(s => s.attacker === attacker && s.defender === defender).map(s => s.damage));

  test('Every troop has a complete counter profile and lands actual hits against all three armor classes', () => {
    assert.equal(D.TYPE_ORDER.length, 23); assert(D.TYPES.hoplite, 'The shield-formation troop is absent');
    assert.deepEqual(Object.keys(D.ARMOR_CLASSES).sort(), ['heavy', 'light', 'medium']);
    for (const id of D.TYPE_ORDER) {
      const type = D.TYPES[id], profile = D.ATTACK_PROFILES[type.attackProfile];
      assert(D.ARMOR_CLASSES[type.armorClass] && profile, id + ' has incomplete playable armor or weapon data');
      assert(Number.isFinite(type.mass) && type.mass > 0);
      for (const key of ['strongAgainst', 'weakAgainst']) {
        assert(Array.isArray(type.matchups[key]) && type.matchups[key].length >= 1, id + ' has no meaningful ' + key + ' entries');
        for (const entry of type.matchups[key]) assert(D.TYPES[entry.type] && typeof entry.reason === 'string' && entry.reason.length >= 20);
      }
      assert(typeof type.matchups.roleNote === 'string' && type.matchups.roleNote.length >= 20);
      for (const armor of ['light', 'medium', 'heavy']) {
        const factor = D.armorMultiplier(id, armor);
        assert(Number.isFinite(factor) && factor >= .78 && factor <= 1.18);
        assert.equal(factor, profile.multipliers[armor]);
      }
      for (const defender of ['militia', 'swordsman', 'guardian', 'lancer']) {
        const declared = D.getMatchup(id, defender), target = D.TYPES[defender];
        assert.equal(declared.armorClass, target.armorClass); assert.equal(declared.attackProfile, type.attackProfile);
        assert.equal(declared.armorMultiplier, D.armorMultiplier(id, target.armorClass));
        assert.equal(declared.armorPierce, type.armorPierce);
        assert.equal(declared.antiMountedBonus, target.mounted ? type.antiCavalry : 0);
      }
    }
    const { attacking } = getArmorSamples(), medians = {};
    assert.equal(attacking.length, D.TYPE_ORDER.length * 3 * seeds.length);
    for (const id of D.TYPE_ORDER) {
      const values = Object.fromEntries(['militia', 'swordsman', 'guardian'].map(target => [target, sampledMedian(attacking, id, target)]));
      assert(Object.values(values).every(value => value > 0), id + ' has only a text counter profile');
      assert(values.guardian <= values.militia * .85, id + ' ignored the real heavy target protection ' + JSON.stringify(values));
      medians[id] = Object.fromEntries(Object.entries(values).map(([key, value]) => [key, rounded(value)]));
    }
    const weak = sampledMedian(attacking, 'swordsman', 'guardian');
    for (const id of ['halberdier', 'crossbow', 'ballista']) assert(sampledMedian(attacking, id, 'guardian') >= weak * 3, id + ' does not provide an actual armor-piercing alternative');
    return { troopProfiles: D.TYPE_ORDER.length, realAttackingSamples: attacking.length, seeds, actualMedianDamage: medians };
  });

  test('Armor classes change real arrow and bolt damage for every defender type', () => {
    assert(D.ARMOR_CLASSES && D.ARMOR_CLASSES.heavy, 'The playable armor classes are absent');
    const { attacking, defending } = getArmorSamples();
    assert.equal(defending.length, D.TYPE_ORDER.length * 2 * seeds.length);
    const hit = (attacker, defender) => sampledMedian(defending, attacker, defender);
    const arrowLight = hit('archer', 'militia'), arrowHeavy = hit('archer', 'guardian'), boltHeavy = hit('crossbow', 'guardian');
    assert(arrowLight >= 6.8, 'Light armor has only a label: ordinary arrows did not gain meaningful real damage');
    assert(arrowHeavy <= 1.2, 'Heavy armor has only a label: armor-floor arrow hits did not become less damaging');
    assert(boltHeavy >= 10.5, 'The piercing bolt profile did not improve actual damage to heavy guardians');
    const cavalryArrow = hit('archer', 'cavalry'), lancerArrow = hit('archer', 'lancer');
    const equalFloorRatio = lancerArrow / cavalryArrow, expectedRatio = D.armorMultiplier('archer', 'heavy') / D.armorMultiplier('archer', 'medium');
    assert(equalFloorRatio <= .90 && Math.abs(equalFloorRatio - expectedRatio) <= .01,
      'Equal pre-class armor-floor hits did not apply the advertised heavy-versus-medium modifier');
    const secondFloorRatio = arrowHeavy / hit('archer', 'halberdier');
    assert(Math.abs(secondFloorRatio - expectedRatio) <= .01, 'The same armor classes produced inconsistent floor-hit behavior on foot troops');
    const cavalryBolt = hit('crossbow', 'cavalry'), lancerBolt = hit('crossbow', 'lancer');
    assert(lancerBolt >= cavalryBolt * 1.04, 'Heavy-armor bolt advantage did not overcome the lancer extra armor point');
    const arrowRetention = arrowHeavy / arrowLight, boltRetention = boltHeavy / hit('crossbow', 'militia');
    assert(boltRetention >= arrowRetention * 2.5, 'Bolts and arrows did not have meaningfully different armor counter behavior');
    const byType = Object.fromEntries(D.TYPE_ORDER.map(id => [id, { armorClass: D.TYPES[id].armorClass,
      arrow: rounded(hit('archer', id)), bolt: rounded(hit('crossbow', id)) }]));
    assert(attacking.length > 0);
    return { defendingTypes: D.TYPE_ORDER.length, realDefendingSamples: defending.length, arrowLight: rounded(arrowLight), arrowHeavy: rounded(arrowHeavy),
      boltHeavy: rounded(boltHeavy), heavyMediumArrowRatio: rounded(equalFloorRatio), heavyMediumBoltRatio: rounded(lancerBolt / cavalryBolt), actualMedianDamage: byType };
  });

  test('A deployed trebuchet reloads between successive live launches', () => {
    const intervals = [];
    for (const seed of seeds) {
      const source = config({ trebuchet: 1 }, { elephant: 1 }, seed);
      source.formations = ['trebuchet', 'elephant'].map((type, team) => ({
        team, type, count: 1, x: 1400 + team * 750, y: 1400, facing: team * Math.PI,
        formation: 'line', offsets: [{ x: 0, y: 0 }]
      }));
      const e = new Engine(source), shots = [];
      e.command([0, 1], 'hold'); e.start();
      for (let tick = 0; tick < 60 * 30 && shots.length < 2 && e.phase === 'running'; tick++) {
        e.step(1 / 30);
        if (e.projectiles.some(p => p.type === 'trebuchet' && p.age <= 1 / 30 + 1e-6)) shots.push(e.time);
      }
      assert.equal(shots.length, 2);
      const interval = shots[1] - shots[0];
      assert(interval >= 22 && interval <= 26, 'Trebuchet did not spend its full reload in formation');
      intervals.push(rounded(interval));
    }
    return { seeds, secondsBetweenStones: intervals };
  });

  function pikeCharge(type, side, formation, seed) {
    const incoming = side === 'front' ? { x: 3800, y: 2200, facing: Math.PI } : { x: 3200, y: 1600, facing: Math.PI / 2 };
    const source = config({ pikeman: 32 }, { [type]: 16 }, seed);
    source.formations = [{ team: 0, type: 'pikeman', count: 32, x: 3200, y: 2200, facing: 0, formation },
      { team: 1, type, count: 16, ...incoming, formation: 'line' }];
    const e = new Engine(source), pikes = e.regiments[0], riders = e.regiments[1], initial = [health(e, pikes), health(e, riders)], hitters = [new Set(), new Set()];
    e.command([pikes.id], 'hold'); e.command([riders.id], 'charge', undefined, undefined, { targetRegimentId: pikes.id }); e.start();
    let contact = null, impact = null, minimum = Infinity, maximumStep = 0;
    for (let tick = 0; tick < 35 * 30 && e.phase === 'running'; tick++) {
      const before = snapshot(e); e.step(1 / 30); const physical = audit(e, before);
      minimum = Math.min(minimum, physical.minimum); maximumStep = Math.max(maximumStep, physical.maximumStep);
      for (const u of e.units) if (u._lastAttack > before[u.id].attack) {
        const target = e.units[u.target]; if (target && target.hp < before[target.id].hp) hitters[u.team].add(u.id);
      }
      const loss = [initial[0] - health(e, pikes), initial[1] - health(e, riders)];
      if (contact === null && (loss[0] > 0 || loss[1] > 0)) contact = e.time;
      if (contact !== null && impact === null && e.time >= contact + 2) impact = loss;
      if (contact !== null && e.time >= contact + 10) break;
    }
    const loss = [initial[0] - health(e, pikes), initial[1] - health(e, riders)], fractions = loss.map((n, i) => n / initial[i]);
    assert(contact !== null && contact <= 10 && impact && hitters[0].size >= 8 && hitters[1].size >= 4, 'Pike comparison never developed actual reciprocal combat');
    assert(loss.every(n => n > 30), 'The directional pike comparison did not inflict meaningful health loss');
    return { type, side, formation, seed, contact, loss, fractions, impact, trade: fractions[1] / fractions[0], hitters: hitters.map(s => s.size),
      minimumBodyRatio: minimum, maximumStep, survivors: e.getStats().teams.map(s => s.alive) };
  }

  test('Settled facing pikes punish mounted frontal charges while flanks and loose ranks remain vulnerable', () => {
    const results = [];
    for (const type of ['cavalry', 'lancer']) {
      const front = seeds.map(seed => pikeCharge(type, 'front', 'line', seed)), flank = seeds.map(seed => pikeCharge(type, 'flank', 'line', seed)), loose = seeds.map(seed => pikeCharge(type, 'front', 'loose', seed));
      const frontTrade = median(front.map(s => s.trade)), flankTrade = median(flank.map(s => s.trade)), looseTrade = median(loose.map(s => s.trade));
      const frontLoss = median(front.map(s => s.loss[0])), flankLoss = median(flank.map(s => s.loss[0]));
      assert(frontTrade >= (type === 'cavalry' ? 2.5 : 2), `${type}: settled pikes did not provide the stronger frontal health trade`);
      assert(frontTrade >= flankTrade * 1.45 && frontLoss <= flankLoss * .75, `${type}: pikes retained their frontal protection when flanked`);
      assert(frontTrade >= looseTrade * 1.3, `${type}: loose pikes retained a formed line's charge defense`);
      assert(front.every(s => s.trade > 1.5), `${type}: the frontal pike counter failed in a representative seed`);
      results.push({ type, frontalTrade: rounded(frontTrade), flankTrade: rounded(flankTrade), looseTrade: rounded(looseTrade),
        frontalPikeHealthLost: rounded(frontLoss), flankPikeHealthLost: rounded(flankLoss),
        frontalPikeHitters: front.map(s => s.hitters[0]), minimumBodyRatio: rounded(Math.min(...front.concat(flank, loose).map(s => s.minimumBodyRatio))),
        seedResults: front.concat(flank, loose).map(s => ({ side: s.side, formation: s.formation, seed: s.seed, contact: rounded(s.contact),
          pikeLoss: rounded(s.loss[0]), riderLoss: rounded(s.loss[1]), trade: rounded(s.trade), survivors: s.survivors })) });
    }
    return { seeds, comparisons: results };
  });

  const attackRecords = new WeakMap();
  function observeAttacks(e) {
    // Read-only observation at a naturally dispatched attack. A valid strike
    // can be followed by collision sliding in this same tick, so end-of-step
    // positions do not establish whether the original strike was in range.
    const records = [], attack = e._attack;
    attackRecords.set(e, records);
    e._attack = function(attacker, target) {
      const distance = Math.hypot(attacker.x - target.x, attacker.y - target.y), hp = target.hp, last = attacker._lastAttack;
      const result = attack.call(this, attacker, target);
      if (attacker._lastAttack > last && target.hp < hp) records.push({ id: attacker.id, team: attacker.team, distance,
        reach: D.TYPES[attacker.type].range + D.TYPES[target.type].radius });
      return result;
    };
  }
  function recordHits(e, before, hitters) {
    for (const hit of attackRecords.get(e).splice(0)) {
      assert(hit.distance <= hit.reach + .01, 'A shield comparison counted an attack beyond its actual flat-ground reach');
      hitters[hit.team].add(hit.id);
    }
  }

  function heldHoplites(side, formation, seed) {
    const incoming = side === 'front' ? { x: 3600, y: 2200, facing: Math.PI } : { x: 3200, y: 1800, facing: Math.PI / 2 };
    const source = config({ hoplite: 32 }, { swordsman: 32 }, seed);
    source.formations = [{ team: 0, type: 'hoplite', count: 32, x: 3200, y: 2200, facing: 0, formation },
      { team: 1, type: 'swordsman', count: 32, ...incoming, formation: 'line' }];
    const e = new Engine(source), shields = e.regiments[0], enemy = e.regiments[1], starting = snapshot(e), initial = [health(e, shields), health(e, enemy)], hitters = [new Set(), new Set()];
    observeAttacks(e);
    e.command([shields.id], 'hold'); e.command([enemy.id], 'charge', undefined, undefined, { targetRegimentId: shields.id }); e.start();
    let contact = null, minimum = Infinity;
    for (let tick = 0; tick < 30 * 30 && e.phase === 'running'; tick++) {
      const before = snapshot(e); e.step(1 / 30); minimum = Math.min(minimum, audit(e, before).minimum); recordHits(e, before, hitters);
      if (contact === null && (health(e, shields) < initial[0] || health(e, enemy) < initial[1])) contact = e.time;
      if (contact !== null && e.time >= contact + 8) break;
    }
    const survivors = living(e, shields), loss = [initial[0] - health(e, shields), initial[1] - health(e, enemy)];
    assert(contact !== null && e.time >= contact + 8 && hitters[0].size >= 8 && loss.every(n => n > 30), 'Shield defense comparison did not develop sustained real reciprocal fighting');
    const drift = mean(survivors.map(u => Math.hypot(u.x - starting[u.id].x, u.y - starting[u.id].y))), lateral = mean(survivors.map(u => Math.abs(u.y - starting[u.id].y)));
    if (side === 'front' && formation === 'line') {
      assert.equal(shields.order, 'hold'); assert.equal(shields.fallingBack, false);
      assert(drift <= 10 && lateral <= 6, 'Held shield ranks abandoned their physical deployment');
    }
    return { side, formation, seed, contact: rounded(contact), loss, hitters: hitters.map(s => s.size), drift: rounded(drift), lateral: rounded(lateral), minimumBodyRatio: rounded(minimum) };
  }

  test('Hoplite shield fronts hold their physical ranks and lose protection when loose or flanked', () => {
    const samples = [];
    for (const seed of seeds.slice(0, 2)) for (const [side, formation] of [['front', 'line'], ['front', 'loose'], ['flank', 'line']]) samples.push(heldHoplites(side, formation, seed));
    const loss = (side, formation) => median(samples.filter(s => s.side === side && s.formation === formation).map(s => s.loss[0]));
    const front = loss('front', 'line'), loose = loss('front', 'loose'), flank = loss('flank', 'line');
    assert(front <= loose * .75, 'A coherent hoplite front did not materially outperform loose shields');
    assert(front <= flank * .80, 'Hoplites kept their frontal shield protection when attacked from the side');
    return { seeds: seeds.slice(0, 2), frontalHealthLost: rounded(front), looseHealthLost: rounded(loose), flankHealthLost: rounded(flank), samples };
  });

  const centralWidth = units => {
    const ys = units.map(u => u.y).sort((a, b) => a - b);
    return ys[Math.min(ys.length - 1, Math.floor(ys.length * .9))] - ys[Math.floor(ys.length * .1)];
  };
  function shieldPressure({ type = 'hoplite', formation = 'line', defender = 'militia', wall = false, rearGuard = false, seed = 4719 } = {}) {
    const tight = wall || rearGuard, offsets = tight ? Array.from({ length: 32 }, (_, i) => ({ x: (Math.floor(i / 8) - 1.5) * 13, y: (i % 8 - 3.5) * 13 })) : undefined;
    const rockX = 3200 + 19.5 + D.TYPES.militia.radius + 2;
    const regions = wall ? [{ type: 'rock', x: rockX, y: 2050, w: 180, h: 300 }] : [];
    const redRoster = { [defender]: 32 }; if (rearGuard) redRoster.guardian = 4;
    const source = config({ [type]: 64 }, redRoster, seed, regions);
    source.formations = [{ team: 0, type, count: 64, x: 2800, y: 2200, facing: 0, formation },
      { team: 1, type: defender, count: 32, x: 3200, y: 2200, facing: Math.PI, formation: 'line', ...(offsets ? { offsets } : {}) }];
    if (rearGuard) source.formations.push({ team: 1, type: 'guardian', count: 4,
      x: 3200 + 19.5 + D.TYPES.militia.radius + D.TYPES.guardian.radius + 2, y: 2200, facing: Math.PI, formation: 'line',
      offsets: Array.from({ length: 4 }, (_, i) => ({ x: 0, y: (i - 1.5) * 26 })) });
    const e = new Engine(source), pressers = e.regiments[0], held = e.regiments[1], starting = snapshot(e), initial = [health(e, pressers), health(e, held)], hitters = [new Set(), new Set()];
    observeAttacks(e);
    const firstCenter = center(e, pressers), initialWidth = centralWidth(living(e, pressers)), frontX = Math.max(...living(e, pressers).map(u => u.x));
    const originalFront = pressers.unitIds.filter(id => starting[id].x >= frontX - .01);
    e.command(e.regiments.filter(r => r.team === 1).map(r => r.id), 'hold'); e.start();
    let contact = null, minimum = Infinity, maximumStep = 0;
    for (let tick = 0; tick < 40 * 30 && e.phase === 'running'; tick++) {
      const before = snapshot(e); e.step(1 / 30); const physical = audit(e, before);
      minimum = Math.min(minimum, physical.minimum); maximumStep = Math.max(maximumStep, physical.maximumStep); recordHits(e, before, hitters);
      if (contact === null && (health(e, pressers) < initial[0] || health(e, held) < initial[1])) contact = e.time;
      if (contact !== null && e.time >= contact + 6) break;
    }
    assert(contact !== null && e.time >= contact + 6, 'The pressure comparison never reached the fixed sustained-contact window');
    const defenders = living(e, held), displacements = defenders.map(u => u.x - starting[u.id].x), survivorFront = originalFront.map(id => e.units[id]).filter(u => u.alive && !u.routed);
    const finalCenter = center(e, pressers), frontInSlot = survivorFront.filter(u => Math.abs((u.y - finalCenter.y) - (starting[u.id].y - firstCenter.y)) <= 30).length;
    const loss = [initial[0] - health(e, pressers), initial[1] - health(e, held)];
    assert(loss.every(n => n > 10) && hitters.every(s => s.size >= 4), 'A pressure control lacked actual reciprocal combat');
    assert.equal(held.order, 'hold'); assert.equal(held.fallingBack, false, 'Retreating defenders were counted as a physical shove');
    const detail = { type, formation, defender, wall, rearGuard, seed, contact: rounded(contact), survivors: defenders.length,
      medianDisplacement: rounded(median(displacements)), displacedTen: displacements.filter(n => n >= 10).length, loss: loss.map(rounded), hitters: hitters.map(s => s.size),
      widthRatio: rounded(centralWidth(living(e, pressers)) / initialWidth), retainedFrontSlots: frontInSlot, survivingOriginalFront: survivorFront.length,
      minimumBodyRatio: rounded(minimum), maximumStep: rounded(maximumStep) };
    if (rearGuard) {
      const guard = e.regiments[2], heavy = living(e, guard);
      detail.heavyRearDisplacement = rounded(median(heavy.map(u => Math.hypot(u.x - starting[u.id].x, u.y - starting[u.id].y))));
      detail.rearContactGap = rounded(Math.min(...defenders.flatMap(a => heavy.map(b => Math.hypot(a.x - b.x, a.y - b.y) - D.TYPES[a.type].radius - D.TYPES[b.type].radius))));
    }
    return { e, pressers, held, detail };
  }

  test('Supported hoplites physically push held light ranks while heavy bodies and rock stop pressure', () => {
    const fronts = seeds.slice(0, 2).map(seed => shieldPressure({ seed }).detail);
    for (const sample of fronts) {
      assert(sample.survivors >= 12 && sample.displacedTen >= 12 && sample.medianDisplacement >= 15,
        'The shield front did not push enough living held defenders through real space: ' + JSON.stringify(sample));
      assert(sample.widthRatio <= 1.35 && sample.retainedFrontSlots >= sample.survivingOriginalFront / 2,
        'Pressing hoplites lost their coherent physical front: ' + JSON.stringify(sample));
    }
    const sword = shieldPressure({ type: 'swordsman' }).detail, loose = shieldPressure({ formation: 'loose' }).detail;
    const heavy = shieldPressure({ defender: 'guardian' }).detail, wall = shieldPressure({ wall: true }).detail, rear = shieldPressure({ rearGuard: true }).detail;
    const reference = fronts[0].medianDisplacement;
    assert(sword.medianDisplacement <= reference * .5 && loose.medianDisplacement <= reference * .5, 'The shield push is indistinguishable from ordinary or loose infantry contact');
    assert(heavy.medianDisplacement <= 3, 'Shield pressure pushed planted heavy troops like light levies');
    assert(wall.medianDisplacement <= 6, 'Shield pressure passed through the tightly packed ranks against a rock wall');
    assert(rear.heavyRearDisplacement <= 3 && rear.rearContactGap <= 2, 'The mass check failed when pressure reached a heavy rear screen: ' + JSON.stringify(rear));
    return { contactWindowSeconds: 6, formed: fronts, controls: { ordinarySword: sword, looseHoplite: loose, heavyFront: heavy, rockWall: wall, heavyRear: rear } };
  });

  test('Fresh commands cancel real outgoing shield pressure and Move or Retreat physically withdraws hoplites', () => {
    const outcomes = [];
    for (const order of ['move', 'retreat']) {
      const { e, pressers, held, detail } = shieldPressure();
      assert(detail.displacedTen >= 12, 'The command comparison did not start during demonstrated physical shield pressure');
      const sourceIds = new Set(pressers.unitIds);
      if (order === 'move') for (const replacement of ['hold', 'auto', 'charge']) {
        const previousEpoch = pressers._commandEpoch;
        assert(e.units.some(u => u._pushUntil > e.time && sourceIds.has(u._pushSource)), 'No actual queued pressure existed before replacement ' + replacement);
        assert.equal(e.command([pressers.id], replacement), 1);
        const before = snapshot(e); e.step(1 / 30); audit(e, before);
        assert.notEqual(pressers._commandEpoch, previousEpoch, 'A fresh shield order reused old outgoing-force authority');
        for (const u of e.units) if (u._pushUntil > e.time && sourceIds.has(u._pushSource)) assert.notEqual(u._pushEpoch, previousEpoch, 'Outgoing pressure from a canceled command remained live');
      }
      const beforeOrder = snapshot(e), initialCenter = center(e, pressers), forcedMovement = new Map(), resolvePush = e._resolveShieldPush;
      // Opponents may voluntarily fall back after losses. Observe only the
      // displacement performed by naturally invoked shield-force resolution;
      // the test neither requests a push nor changes any body or force state.
      e._resolveShieldPush = function(u, dt) {
        const source = u._pushSource, x = u.x, y = u.y, result = resolvePush.call(this, u, dt);
        if (sourceIds.has(source)) forcedMovement.set(u.id, (forcedMovement.get(u.id) || 0) + Math.hypot(u.x - x, u.y - y));
        return result;
      };
      assert.equal(e.command([pressers.id], order, initialCenter.x - 500, initialCenter.y), 1);
      for (const id of pressers.unitIds) assert.deepEqual([e.units[id].x, e.units[id].y, e.units[id].angle], [beforeOrder[id].x, beforeOrder[id].y, beforeOrder[id].angle], 'A fresh order teleported a shield rank');
      let minimum = Infinity, firstTenthWithdrawal = 0;
      for (let tick = 0; tick < 60; tick++) {
        const before = snapshot(e); e.step(1 / 30); minimum = Math.min(minimum, audit(e, before).minimum);
        if (tick === 2) firstTenthWithdrawal = mean(living(e, pressers).map(u => beforeOrder[u.id].x - u.x));
        assert.equal(e.units.some(u => u._pushUntil > e.time && sourceIds.has(u._pushSource)), false, 'Withdrawn shield troops kept an outgoing push active');
      }
      const survivors = living(e, pressers), meanWithdrawal = mean(survivors.map(u => beforeOrder[u.id].x - u.x));
      const latestOldPush = Math.max(0, ...forcedMovement.values());
      assert(meanWithdrawal >= 25, 'A new ' + order + ' did not physically take control of the pushing hoplites');
      assert(latestOldPush <= 1, 'Held opponents kept being shoved after the source withdrew');
      assert(firstTenthWithdrawal > .01, 'A fresh withdrawal failed to move the engaged shield line away');
      outcomes.push({ order, meanWithdrawal: rounded(meanWithdrawal), firstTenthWithdrawal: rounded(firstTenthWithdrawal), maximumFurtherDefenderPush: rounded(latestOldPush),
        opponentVoluntaryFallback: held.fallingBack, minimumBodyRatio: rounded(minimum) });
    }
    return { outcomes, replacementEpochsChecked: ['hold', 'auto', 'charge'] };
  });
}

if (require.main === module) {
  const root = process.argv[2] || path.resolve(__dirname, '..'); global.window = global;
  require(path.join(root, 'src/data.js'));
  if (require('node:fs').existsSync(path.join(root, 'src/tactics.js'))) require(path.join(root, 'src/tactics.js'));
  for (const name of ['impacts', 'siege']) {
    const file = path.join(root, 'src', name + '.js');
    if (require('node:fs').existsSync(file)) require(file);
  }
  require(path.join(root, 'src/engine.js'));
  const results = [], filter = (process.env.IRONFIELD_TEST_FILTER || '').toLowerCase();
  register((name, body) => {
    if (filter && !name.toLowerCase().includes(filter)) return;
    const began = performance.now();
    try { const detail = body(); results.push({ name, passed: true, milliseconds: Math.round(performance.now() - began), ...detail }); console.log('PASS ' + name + ' ' + JSON.stringify(detail)); }
    catch (error) { results.push({ name, passed: false, error: error.message }); console.error('FAIL ' + name + ': ' + error.stack); process.exitCode = 1; }
  });
  console.log(JSON.stringify({ passed: results.filter(r => r.passed).length, failed: results.filter(r => !r.passed).length, results }, null, 2));
}
module.exports = register;
