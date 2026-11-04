/** Match Wars combat. Coordinates are logical units; time is always seconds. */
import { TOTAL_STAGES, getStage, getChapter } from './campaign.mjs';
export const ELEMENT_INFO = {
  ember: { name: 'Ember', color: '#ff805e', icon: 'flame', strong: 'grove', weak: 'tide' },
  tide: { name: 'Tide', color: '#61c7f4', icon: 'drop', strong: 'ember', weak: 'storm' },
  grove: { name: 'Grove', color: '#83d18b', icon: 'leaf', strong: 'storm', weak: 'ember' },
  storm: { name: 'Storm', color: '#e7c76d', icon: 'bolt', strong: 'tide', weak: 'grove' },
  sun: { name: 'Sun', color: '#ffe3a0', icon: 'sun', strong: 'void', weak: 'void' },
  void: { name: 'Void', color: '#b79aef', icon: 'moon', strong: 'sun', weak: 'sun' },
};

export const TROOPS = {
  ember: { key: 'ember', name: 'Ember Mage', role: 'Burn / splash', description: 'Fireballs scorch clustered enemies. Strong against Grove.', color: '#ff805e', icon: 'flame', element: 'ember', spriteRow: 0, hp: 70, attack: 15, speed: 36, range: 190, interval: 1.55 },
  tide: { key: 'tide', name: 'Tide Ranger', role: 'Ranged / slow', description: 'Long-range frost arrows slow threats before they reach your line.', color: '#61c7f4', icon: 'drop', element: 'tide', spriteRow: 1, hp: 62, attack: 13, speed: 39, range: 245, interval: 1.3 },
  grove: { key: 'grove', name: 'Grove Warden', role: 'Heal / poison', description: 'Heals nearby allies and poisons enemies. Pair with a Sun tank.', color: '#83d18b', icon: 'leaf', element: 'grove', spriteRow: 2, hp: 100, attack: 9, speed: 31, range: 125, interval: 1.6 },
  storm: { key: 'storm', name: 'Storm Lancer', role: 'Chain / anti-air', description: 'Fast lightning strikes jump between foes and deal full damage to flyers.', color: '#e7c76d', icon: 'bolt', element: 'storm', spriteRow: 3, hp: 90, attack: 17, speed: 56, range: 38, interval: 1.05 },
  sun: { key: 'sun', name: 'Sun Paladin', role: 'Tank / shield', description: 'Armored frontline that shields the next ally behind it.', color: '#ffe3a0', icon: 'sun', element: 'sun', spriteRow: 4, hp: 155, attack: 12, speed: 29, range: 34, interval: 1.5 },
  void: { key: 'void', name: 'Void Reaper', role: 'Drain / execute', description: 'Steals life and deals extra damage to wounded targets.', color: '#b79aef', icon: 'moon', element: 'void', spriteRow: 5, hp: 100, attack: 18, speed: 42, range: 40, interval: 1.3 },
};

const enemy = (key, name, element, role, hp, attack, speed, range, interval, traits = {}) => ({ key, name, element, role, hp, attack, speed, range, interval, color: ELEMENT_INFO[element].color, spriteRow: 6 + Object.keys(ELEMENT_INFO).indexOf(element), ...traits });
export const ENEMIES = {
  ashling: enemy('ashling', 'Ashling', 'ember', 'Burning raider', 54, 9, 39, 32, 1.4, { burn: true }),
  cinder: enemy('cinder', 'Cinder Imp', 'ember', 'Splash caster', 44, 11, 32, 185, 1.8, { splash: 45 }),
  furnace: enemy('furnace', 'Furnace Brute', 'ember', 'Armored siege', 125, 14, 24, 35, 1.9, { armor: .24, siege: 1.4 }),
  frostwolf: enemy('frostwolf', 'Frost Wolf', 'tide', 'Fast / slow', 52, 9, 57, 32, 1.2, { slow: true }),
  iceseer: enemy('iceseer', 'Glacial Seer', 'tide', 'Frost sniper', 46, 13, 30, 225, 1.9, { slow: true }),
  glacier: enemy('glacier', 'Glacier Knight', 'tide', 'Frost armor', 115, 11, 27, 35, 1.7, { armor: .22, slow: true }),
  thornling: enemy('thornling', 'Thorn Crawler', 'grove', 'Poison hunter', 60, 8, 40, 40, 1.4, { poison: true }),
  briar: enemy('briar', 'Briar Shaman', 'grove', 'Enemy healer', 60, 7, 29, 160, 1.8, { healer: true }),
  barkhide: enemy('barkhide', 'Bark Colossus', 'grove', 'Regenerating tank', 140, 12, 22, 35, 1.9, { armor: .18, regen: 2.5 }),
  harpy: enemy('harpy', 'Gale Harpy', 'storm', 'Flying skirmisher', 52, 8, 55, 65, 1.3, { flying: true }),
  stormwisp: enemy('stormwisp', 'Storm Wisp', 'storm', 'Chain caster', 48, 10, 37, 170, 1.7, { chain: true }),
  thunderfoot: enemy('thunderfoot', 'Thunder Runner', 'storm', 'Swift siege', 78, 11, 65, 35, 1.35, { siege: 1.3 }),
  sungard: enemy('sungard', 'Solar Guard', 'sun', 'Shield bearer', 90, 10, 30, 34, 1.6, { shield: 36 }),
  dawncleric: enemy('dawncleric', 'Dawn Cleric', 'sun', 'Shield / healer', 58, 8, 28, 160, 1.85, { healer: true, shield: 15 }),
  prism: enemy('prism', 'Prism Sentinel', 'sun', 'Ranged-resistant tank', 120, 12, 25, 38, 1.7, { armor: .12, rangedResist: .28 }),
  shade: enemy('shade', 'Hollow Shade', 'void', 'Life stealer', 72, 12, 43, 38, 1.4, { lifesteal: .25 }),
  conjurer: enemy('conjurer', 'Dusk Conjurer', 'void', 'Summoner', 62, 9, 30, 170, 1.8, { summoner: true }),
  assassin: enemy('assassin', 'Eclipse Stalker', 'void', 'Shield breaker', 65, 16, 52, 35, 1.55, { shieldBreak: true }),
  rootking: enemy('rootking', 'The Root King', 'grove', 'Boss / regeneration', 600, 23, 20, 55, 1.8, { boss: true, armor: .12, regen: 4, poison: true, siege: 1.4 }),
  frostqueen: enemy('frostqueen', 'Frostbound Queen', 'tide', 'Boss / frost volley', 610, 22, 25, 220, 1.8, { boss: true, slow: true, splash: 65 }),
  eclipselord: enemy('eclipselord', 'The Eclipse Lord', 'void', 'Boss / summoner', 760, 27, 25, 180, 1.8, { boss: true, summoner: true, lifesteal: .18 }),
};

const upgrade = (id, name, description, group, cost, max = 3) => ({ id, name, description, group, cost, max });
export const UPGRADES = [
  upgrade('ember-burn', 'Lingering Embers', 'Burn deals +35% damage per rank.', 'ember', 50),
  upgrade('ember-splash', 'Wildfire', 'Fireball splash radius +18 and splash damage +10% per rank.', 'ember', 65),
  upgrade('tide-slow', 'Deep Freeze', 'Frost slows last +1 second and slow +6% per rank.', 'tide', 50),
  upgrade('tide-range', 'Farshot', 'Rangers gain +25 range and +8% attack per rank.', 'tide', 65),
  upgrade('grove-heal', 'Living Spring', 'Warden healing is 25% stronger per rank.', 'grove', 50),
  upgrade('grove-poison', 'Venom Bloom', 'Poison deals +35% damage per rank.', 'grove', 65),
  upgrade('storm-chain', 'Forked Lightning', 'Lightning hits one extra target per rank.', 'storm', 55),
  upgrade('storm-speed', 'Swift Formation', 'Lancers move and attack 10% faster per rank.', 'storm', 65),
  upgrade('sun-shield', 'Dawn Aegis', 'Paladins and their shielded allies gain +18 shield per rank.', 'sun', 50),
  upgrade('sun-armor', 'Golden Bulwark', 'Paladins take 8% less damage per rank.', 'sun', 65),
  upgrade('void-drain', 'Soul Harvest', 'Reapers steal an extra 10% life per rank.', 'void', 55),
  upgrade('void-execute', 'Final Eclipse', 'Execute bonus +20% per rank against targets below 35% health.', 'void', 65),
  upgrade('combo', 'Battle Rhythm', 'Cascade summons gain +6% power per chain per rank.', 'command', 75),
  upgrade('relic', 'Relic Attunement', 'Matched relic buffs are 20% stronger per rank.', 'command', 70),
  upgrade('bounty', 'Spoils of War', 'Enemy kills award +20% gold per rank.', 'command', 65),
  upgrade('fortress', 'Reinforced Keep', 'Your keep gains +40 health per rank.', 'command', 70),
  upgrade('spellcraft', 'Arcane Tactics', 'Spells recharge 10% faster and gain +10% strength per rank.', 'command', 80),
  upgrade('sentries', 'Keep Sentries', 'Keep defenders deal +4 damage per rank.', 'command', 60),
];

const clamp = (v, low, high) => Math.min(high, Math.max(low, Number.isFinite(v) ? v : low));
const rounded = (value, digits = 3) => Number(value.toFixed(digits));

/**
 * Stable campaign tuning, separate from troop talents. The original twelve battles
 * retain their timings and scaling. Later fronts add waves, formations and veterans
 * while health and attack scale separately so a max-trained army remains viable.
 * Pressure is average incoming bodies/second × health × attack, expressed ×100.
 */
export function difficultyForStage(value = 1) {
  const stage = Math.floor(clamp(value, 1, TOTAL_STAGES));
  const beyond = Math.max(0, stage - 12);
  const totalWaves = stage <= 12 ? 3 : stage < 33 ? 4 : 5;
  const unitsPerWave = stage <= 12 ? 4 + Math.min(4, Math.floor(stage / 3)) : 8 + Math.floor((stage - 13) / 9);
  const healthMultiplier = stage <= 12 ? 1 + (stage - 1) * .055 : 1.605 + beyond * .026;
  const attackMultiplier = stage <= 12 ? healthMultiplier : 1.605 + beyond * .017;
  const waveInterval = 20 - beyond * .075;
  const spawnInterval = 1.65 - beyond * .009;
  const bossHealthMultiplier = stage <= 12 ? 1 + Math.max(0, stage - 4) * .035 : 1.28 + beyond * .03;
  const bossAttackMultiplier = stage <= 12 ? bossHealthMultiplier : 1.28 + beyond * .012;
  const eliteEvery = stage <= 12 ? 0 : Math.max(7, 12 - Math.floor((stage - 13) / 6));
  const averageUnits = unitsPerWave + (totalWaves - 1) / 2;
  const waveAttackGrowth = stage <= 12 ? .035 : .018;
  const pressure = averageUnits / waveInterval * (healthMultiplier + (totalWaves - 1) * .035 / 2) *
    (attackMultiplier + (totalWaves - 1) * waveAttackGrowth / 2) * 100;
  return Object.freeze({
    stage, totalWaves, unitsPerWave, waveInterval: rounded(waveInterval), spawnInterval: rounded(spawnInterval),
    healthMultiplier: rounded(healthMultiplier), attackMultiplier: rounded(attackMultiplier),
    bossHealthMultiplier: rounded(bossHealthMultiplier), bossAttackMultiplier: rounded(bossAttackMultiplier),
    speedMultiplier: rounded(1 + beyond * .0015), waveHealthGrowth: .035, waveAttackGrowth,
    eliteEvery, eliteChance: eliteEvery ? rounded(1 / eliteEvery) : 0,
    keepHealth: 180 + stage * 18 + beyond * 10,
    scheduledUnits: totalWaves * unitsPerWave + totalWaves * (totalWaves - 1) / 2 + (stage % 4 === 0 ? 1 : 0),
    bossAt: rounded(5 + (totalWaves - 1) * waveInterval + 4 + Math.min(3, beyond * .08)),
    pressure: rounded(pressure, 2), rewardBonus: 65 + stage * 12 + (stage % 4 === 0 ? 45 : 0),
  });
}

export function upgradeCost(id, level = 0) {
  const def = UPGRADES.find(item => item.id === id);
  return def ? Math.round(def.cost * Math.pow(1.65, clamp(level, 0, def.max))) : Infinity;
}

export function elementMultiplier(attacker, defender) {
  if (!ELEMENT_INFO[attacker] || !ELEMENT_INFO[defender] || attacker === defender) return 1;
  if ((attacker === 'sun' && defender === 'void') || (attacker === 'void' && defender === 'sun')) return 1.35;
  if (ELEMENT_INFO[attacker].strong === defender) return 1.5;
  if (ELEMENT_INFO[attacker].weak === defender) return .7;
  return 1;
}

/** A Battle never reads the DOM, storage, wall clock, or global randomness. */
export class Battle {
  constructor({ stage = 1, upgrades = {}, rng = Math.random, onEvent = () => {} } = {}) {
    this.stage = Math.floor(clamp(stage, 1, TOTAL_STAGES));
    this.difficulty = difficultyForStage(this.stage);
    this.chapter = getChapter(this.stage);
    this.stageInfo = getStage(this.stage);
    this.upgrades = Object.fromEntries(UPGRADES.map(def => [def.id, Math.floor(clamp(upgrades[def.id] || 0, 0, def.max))]));
    this.rng = rng;
    this.onEvent = onEvent;
    this.units = [];
    this.projectiles = [];
    this.effects = [];
    this.time = 0;
    this.wave = 0;
    this.totalWaves = this.difficulty.totalWaves;
    this.playerMax = 180 + this.rank('fortress') * 40;
    this.enemyMax = this.difficulty.keepHealth;
    this.playerHp = this.playerMax;
    this.enemyHp = this.enemyMax;
    this.enemyBarrier = this.stage % 4 === 0;
    this.kills = 0;
    this.gold = 0;
    this.status = 'playing';
    this.cooldowns = { meteor: 0, rally: 0, heal: 0 };
    this.spellDurations = { meteor: 30, rally: 26, heal: 24 };
    this.synergy = Object.fromEntries(Object.keys(TROOPS).map(key => [key, 0]));
    this._id = 0;
    this._projectileId = 0;
    this._guardTimer = 1;
    this._next = 0;
    this._schedule = this._buildSchedule();
  }

  rank(id) { return this.upgrades[id] || 0; }

  _emit(type, details = {}) { this.onEvent({ type, time: this.time, ...details }); }

  _buildSchedule() {
    const factions = [
      ['ember', 'grove', 'tide'], ['tide', 'storm', 'ember'], ['grove', 'sun', 'storm'],
      ['grove', 'ember', 'void'], ['storm', 'tide', 'sun'], ['sun', 'void', 'grove'],
      ['void', 'sun', 'ember'], ['tide', 'storm', 'void'], ['ember', 'grove', 'storm'],
      ['sun', 'tide', 'void'], ['void', 'grove', 'sun'], ['void', 'tide', 'ember'],
    ];
    const faction = this.stage <= 12 ? factions[this.stage - 1] : this.chapter.elements;
    const count = this.difficulty.unitsPerWave;
    const schedule = [];
    for (let wave = 1; wave <= this.totalWaves; wave++) {
      for (let index = 0; index < count + wave - 1; index++) {
        const lane = (index + wave - 1) % 3;
        const element = faction[(lane + wave - 1) % 3];
        const roster = Object.values(ENEMIES).filter(def => def.element === element && !def.boss);
        // Stage one previews simple enemies; later encounters deliberately mix roles.
        const variant = this.stage <= 12
          ? this.stage === 1 && wave === 1 ? 0 : (index + wave + Math.floor(this.stage / 2)) % roster.length
          : [2, 1, 0, 1, 2][Math.floor(index / 3) % 5];
        const serial = schedule.length + 1;
        schedule.push({
          time: rounded(5 + (wave - 1) * this.difficulty.waveInterval + index * this.difficulty.spawnInterval),
          wave, lane, key: roster[variant].key,
          elite: !!this.difficulty.eliteEvery && serial % this.difficulty.eliteEvery === 0,
        });
      }
      if (wave === this.totalWaves && this.stageInfo.boss) {
        schedule.push({ time: this.difficulty.bossAt, wave, lane: 1, key: this.stageInfo.bossKey, name: this.stageInfo.bossName });
      }
    }
    return schedule.sort((a, b) => a.time - b.time);
  }

  forecast() {
    return this._schedule.slice(this._next, this._next + 9).map(entry => {
      const def = this._enemyDefinition(entry);
      const counter = Object.keys(TROOPS).find(key => ELEMENT_INFO[key].strong === def.element) || 'storm';
      return { ...entry, time: Math.max(0, entry.time - this.time), element: def.element, name: def.name, role: def.role, counter, boss: !!def.boss, elite: !!entry.elite };
    });
  }

  _enemyDefinition(entry) {
    const base = ENEMIES[entry.key];
    if (!base.boss) return entry.elite ? { ...base, elite: true, name: `Veteran ${base.name}`, role: `Veteran / ${base.role}`, armor: Math.min(.35, (base.armor || 0) + .04) } : base;
    const tricks = {
      4: { shield: 65, armor: .18 },
      5: { chain: true, flying: true },
      6: { healer: true, shieldBreak: true },
      7: { burn: true, splash: 55, shield: 80 },
      8: { armor: .18, shield: 120, rangedResist: .12 },
      9: { healer: true, summoner: true, summonLimit: 3, regen: 5 },
      10: { shieldBreak: true, chain: true },
      11: { flying: true, chain: true, shield: 100 },
      12: { chain: true, splash: 55, shield: 160, summonLimit: 6 },
    };
    return { ...base, ...tricks[this.chapter.number], name: entry.name || this.stageInfo.bossName || base.name,
      secondWind: this.stage > 12, phasePower: .12 + Math.max(0, this.stage - 12) * .002,
    };
  }

  summon(key, { lane = 1, power = 1, chain = 1, buffs = [], elite = false, count = 3 } = {}) {
    if (this.status !== 'playing' || !TROOPS[key]) return [];
    const def = TROOPS[key];
    lane = Math.floor(clamp(lane, 0, 2));
    chain = clamp(chain, 1, 24);
    const validBuffs = [...new Set((Array.isArray(buffs) ? buffs : []).filter(buff => ['fury', 'aegis', 'haste', 'vitality'].includes(buff)))];
    const relic = 1 + this.rank('relic') * .2;
    const resonance = Math.min(3, Math.floor(this.synergy[key] / 4));
    const strength = clamp(power, .25, 8) * (elite ? 1.32 : 1) * (1 + (chain - 1) * (.14 + this.rank('combo') * .06)) * (1 + resonance * .05);
    const amount = Math.min(4, 1 + Math.floor((clamp(count, 3, 49) - 3) / 2));
    const spawned = [];
    // Reserve room for incoming waves; otherwise a full friendly army could block a warded boss forever.
    let friendlyCount = this.units.filter(u => u.side === 'player' && !u.dead).length;
    for (let index = 0; index < amount && this.units.length < 64 && friendlyCount < 40; index++, friendlyCount++) {
      const u = this._makeUnit(def, 'player', lane, strength);
      u.x = 85 - index * 21;
      u.buffs = [...validBuffs];
      u.elite = elite;
      u.chain = chain;
      u.resonance = resonance;
      if (validBuffs.includes('fury')) u.attack *= 1 + .3 * relic;
      if (validBuffs.includes('haste')) { u.speed *= 1 + .25 * relic; u.interval /= 1 + .2 * relic; }
      if (validBuffs.includes('aegis')) u.shield += 30 * relic;
      if (validBuffs.includes('vitality')) { u.hp *= 1 + .35 * relic; u.maxHp = u.hp; }
      if (key === 'sun') { u.armor = .15 + this.rank('sun-armor') * .08; u.shield += 22 + this.rank('sun-shield') * 18; }
      if (key === 'tide') { u.range += this.rank('tide-range') * 25; u.attack *= 1 + this.rank('tide-range') * .08; }
      if (key === 'storm') { const swift = 1 + this.rank('storm-speed') * .1; u.speed *= swift; u.interval /= swift; }
      this.units.push(u);
      this.synergy[key]++;
      spawned.push(u);
      this._emit('spawn', { unit: u, side: 'player', key, lane });
      this._effect('summon', u.x, lane, def.color, .65);
      if (key === 'sun') {
        const ally = this.units.filter(other => other.side === 'player' && other.lane === lane && other.id !== u.id).sort((a, b) => Math.abs(a.x - u.x) - Math.abs(b.x - u.x))[0];
        if (ally) { ally.shield += 16 + this.rank('sun-shield') * 18; this._effect('shield', ally.x, lane, '#ffe3a0', .7); }
      }
    }
    return spawned;
  }

  _makeUnit(def, side, lane, strength = 1) {
    return {
      id: ++this._id, key: def.element, enemyType: side === 'enemy' ? def.key : null,
      name: def.name, role: def.role, element: def.element, color: def.color,
      side, lane, x: side === 'player' ? 85 : 915, hp: def.hp * strength,
      maxHp: def.hp * strength, attack: def.attack * strength, speed: def.speed,
      range: def.range, interval: def.interval, cooldown: .3, state: 'walk', anim: 0,
      shield: def.shield || 0, armor: def.armor || 0, buffs: [], boss: !!def.boss,
      spriteRow: def.spriteRow, statuses: {}, supportCooldown: .8, elite: !!def.elite,
      summonCooldown: 7, summons: 0, trait: def, dead: false, hitFlash: 0,
    };
  }

  _spawnEnemy(entry) {
    if (this.units.length >= 64) return false;
    const def = this._enemyDefinition(entry);
    const healthScale = def.boss ? this.difficulty.bossHealthMultiplier :
      (this.difficulty.healthMultiplier + (entry.wave - 1) * this.difficulty.waveHealthGrowth) * (entry.elite ? 1.18 : 1);
    const attackScale = def.boss ? this.difficulty.bossAttackMultiplier :
      (this.difficulty.attackMultiplier + (entry.wave - 1) * this.difficulty.waveAttackGrowth) * (entry.elite ? 1.1 : 1);
    const u = this._makeUnit(def, 'enemy', entry.lane, healthScale);
    u.attack = def.attack * attackScale;
    u.speed *= this.difficulty.speedMultiplier;
    if (this.stage > 12) u.shield *= Math.sqrt(healthScale);
    u.x += (this.units.filter(other => other.side === 'enemy' && other.lane === u.lane && other.x > 890).length) * 20;
    this.units.push(u);
    this._emit('spawn', { unit: u, side: 'enemy', key: u.key, lane: u.lane });
    this._effect('summon', u.x, u.lane, def.color, .6);
    return true;
  }

  cast(spell, lane = 1) {
    lane = Math.floor(clamp(lane, 0, 2));
    if (this.status !== 'playing' || !(spell in this.cooldowns) || this.cooldowns[spell] > 0) return false;
    const strength = 1 + this.rank('spellcraft') * .1;
    const targets = this.units.filter(u => u.lane === lane && !u.dead);
    if (spell === 'meteor') {
      for (const u of targets.filter(u => u.side === 'enemy')) {
        this._hit(null, u, 48 * strength, { element: 'ember', ignoreArmor: .5 });
        this._status(u, 'burn', 5, 5 * strength);
      }
      this._effect('meteor', 500, lane, '#ff9a62', 1);
    } else if (spell === 'rally') {
      for (const u of targets.filter(u => u.side === 'player')) {
        this._status(u, 'rally', 8, .35 * strength);
        u.shield += 12 * strength;
      }
      this._effect('rally', 500, lane, '#f9d777', 1);
    } else {
      this.playerHp = Math.min(this.playerMax, this.playerHp + 25 * strength);
      for (const u of targets.filter(u => u.side === 'player')) this._heal(u, 38 * strength);
      this._effect('heal', 400, lane, '#86dfba', .9);
    }
    this.cooldowns[spell] = this.spellDurations[spell] * (1 - this.rank('spellcraft') * .1);
    this._emit('spell', { spell, lane });
    this._finishIfNeeded();
    return true;
  }

  update(dt) {
    if (this.status !== 'playing' || !Number.isFinite(dt) || dt <= 0) return;
    // Substeps prevent melee tunneling and preserve timers through slower frames.
    let remaining = Math.min(dt, .5);
    while (remaining > 0 && this.status === 'playing') {
      const step = Math.min(.05, remaining);
      this._step(step);
      remaining -= step;
    }
  }

  _step(dt) {
    this.time += dt;
    for (const key of Object.keys(this.cooldowns)) this.cooldowns[key] = Math.max(0, this.cooldowns[key] - dt);
    while (this._next < this._schedule.length && this._schedule[this._next].time <= this.time) {
      const entry = this._schedule[this._next];
      if (!this._spawnEnemy(entry)) break;
      this._next++;
      if (entry.wave > this.wave) { this.wave = entry.wave; this._emit('wave', { wave: this.wave }); }
    }
    const alive = this.units.filter(u => !u.dead);
    for (const u of alive) this._tickUnit(u, dt, alive);
    this._tickProjectiles(dt);
    this._guardTimer -= dt;
    if (this._guardTimer <= 0) {
      this._guardTimer = 2;
      for (let lane = 0; lane < 3; lane++) {
        const enemy = this.units.filter(u => u.side === 'enemy' && !u.dead && u.lane === lane && u.x < 215).sort((a, b) => a.x - b.x)[0];
        if (enemy) this._hit(null, enemy, 8 + this.rank('sentries') * 4, { element: 'sun' });
      }
    }
    this.units = this.units.filter(u => !u.dead);
    for (const effect of this.effects) { effect.life -= dt; effect.age += dt; }
    this.effects = this.effects.filter(effect => effect.life > 0).slice(-120);
    this._finishIfNeeded();
  }

  _tickUnit(u, dt, alive) {
    if (u.dead) return;
    if (u.boss && u.trait.secondWind && !u.phaseUsed && u.hp < u.maxHp * .5) {
      u.phaseUsed = true;
      this._status(u, 'rally', 9, u.trait.phasePower);
      u.shield += 25 + this.stage;
      this._effect('rally', u.x, u.lane, u.color, 1);
      this._emit('boss-phase', { name: u.name, unitId: u.id, lane: u.lane, x: u.x });
    }
    u.anim += dt;
    u.hitFlash = Math.max(0, u.hitFlash - dt);
    u.cooldown -= dt;
    u.supportCooldown -= dt;
    for (const [name, effect] of Object.entries(u.statuses)) {
      const applied = Math.min(dt, effect.time);
      if (name === 'burn' || name === 'poison') this._hit(null, u, effect.power * applied, { element: name === 'burn' ? 'ember' : 'grove', dot: true });
      effect.time -= dt;
      if (effect.time <= 0) delete u.statuses[name];
    }
    if (u.dead) return;
    if (u.trait.regen) u.hp = Math.min(u.maxHp, u.hp + u.trait.regen * dt);
    const allies = alive.filter(other => other.side === u.side && other.lane === u.lane && !other.dead);
    if ((u.key === 'grove' && u.side === 'player' || u.trait.healer) && u.supportCooldown <= 0) {
      const target = allies.filter(other => Math.abs(other.x - u.x) < 190 && other.hp < other.maxHp).sort((a, b) => a.hp / a.maxHp - b.hp / b.maxHp)[0];
      if (target) {
        const amount = u.side === 'player' ? 12 * (1 + this.rank('grove-heal') * .25) : 10 + this.stage * .6;
        this._heal(target, amount);
        u.supportCooldown = 3;
      }
    }
    if (u.trait.summoner) {
      u.summonCooldown -= dt;
      if (u.summonCooldown <= 0 && u.summons < (u.trait.summonLimit || (u.boss ? 4 : 2)) && this.units.length < 60) {
        const minion = this._makeUnit(ENEMIES.shade, 'enemy', u.lane, .65 + this.stage * .025);
        minion.x = u.x - 25;
        this.units.push(minion);
        u.summons++;
        u.summonCooldown = 10;
        this._effect('summon', minion.x, minion.lane, '#b79aef', .6);
        this._emit('spawn', { unit: minion, side: 'enemy', key: minion.key, lane: minion.lane });
      }
    }
    const targets = alive.filter(other => other.side !== u.side && other.lane === u.lane && !other.dead);
    const target = targets.sort((a, b) => Math.abs(a.x - u.x) - Math.abs(b.x - u.x))[0];
    const direction = u.side === 'player' ? 1 : -1;
    const range = u.range + (target?.boss ? 14 : 0);
    if (target && Math.abs(target.x - u.x) <= range) {
      u.state = 'attack';
      if (u.cooldown <= 0) {
        this._attack(u, target);
        const rally = u.statuses.rally?.power || 0;
        u.cooldown = u.interval / (1 + rally);
        u.anim = 0;
      }
      return;
    }
    const baseX = u.side === 'player' ? 920 : 80;
    if (!target && Math.abs(baseX - u.x) <= Math.max(35, u.range * .65)) {
      u.state = 'attack';
      if (u.cooldown <= 0) {
        const amount = u.attack * .75 * (u.trait.siege || 1);
        if (u.side === 'player') this.enemyHp = Math.max(this.enemyBarrier ? 1 : 0, this.enemyHp - amount);
        else this.playerHp = Math.max(0, this.playerHp - amount);
        this._emit('damage', { x: baseX, lane: u.lane, amount, side: u.side === 'player' ? 'enemy' : 'player', base: true, element: u.element });
        this._effect('impact', baseX, u.lane, u.color, .3);
        u.cooldown = u.interval;
        u.anim = 0;
      }
      return;
    }
    u.state = 'walk';
    const slow = u.statuses.slow?.power || 0;
    const rally = u.statuses.rally?.power || 0;
    let movement = u.speed * dt * (1 - slow) * (1 + rally);
    // Front-to-back spacing keeps friendly sprites legible without blocking a melee unit behind a ranger.
    const obstacle = allies.filter(other => other.id !== u.id && (other.x - u.x) * direction > 0 && other.range <= u.range + 10).sort((a, b) => (a.x - b.x) * direction)[0];
    if (obstacle) movement = Math.min(movement, Math.max(0, (obstacle.x - u.x) * direction - 23));
    if (target) {
      const toward = Math.sign(target.x - u.x);
      u.x += toward * Math.min(movement, Math.max(0, Math.abs(target.x - u.x) - range + 1));
    } else u.x += direction * movement;
    u.x = clamp(u.x, 55, 945);
  }

  _attack(u, target) {
    let amount = u.attack * (1 + (u.statuses.rally?.power || 0));
    if (u.side === 'player' && u.key === 'void' && target.hp / target.maxHp < .35) amount *= 1.4 + this.rank('void-execute') * .2;
    if (u.range > 80) {
      if (this.projectiles.length < 100) this.projectiles.push({ id: ++this._projectileId, sourceId: u.id, targetId: target.id, x: u.x, lane: u.lane, element: u.element, color: u.color, side: u.side, speed: 470, damage: amount, age: 0, trait: u.trait, key: u.key });
    } else {
      this._impactAttack(u, target, amount);
      this._effect('slash', target.x, target.lane, u.color, .25);
    }
  }

  _tickProjectiles(dt) {
    for (const p of this.projectiles) {
      const target = this.units.find(u => u.id === p.targetId && !u.dead);
      p.age += dt;
      if (!target || p.age > 4) { p.dead = true; continue; }
      const delta = target.x - p.x;
      if (Math.abs(delta) <= p.speed * dt + 8) {
        const source = this.units.find(u => u.id === p.sourceId) || { ...p, id: p.sourceId, attack: p.damage, range: 150, hp: 0, maxHp: 0, dead: true };
        this._impactAttack(source, target, p.damage);
        this._effect('impact', target.x, target.lane, p.color, .3);
        p.dead = true;
      } else p.x += Math.sign(delta) * p.speed * dt;
    }
    this.projectiles = this.projectiles.filter(p => !p.dead);
  }

  _impactAttack(u, target, damage) {
    if (target.dead) return;
    const dealt = this._hit(u, target, damage, { shieldBreak: !!u.trait.shieldBreak });
    const player = u.side === 'player';
    const key = u.key;
    if ((player && key === 'ember') || u.trait.burn) this._status(target, 'burn', 4, (player ? 3.5 * (1 + this.rank('ember-burn') * .35) : 2.4));
    if ((player && key === 'tide') || u.trait.slow) this._status(target, 'slow', player ? 3 + this.rank('tide-slow') : 2.4, player ? .32 + this.rank('tide-slow') * .06 : .25);
    if ((player && key === 'grove') || u.trait.poison) this._status(target, 'poison', 5, player ? 3 * (1 + this.rank('grove-poison') * .35) : 2.8);
    const splash = player && key === 'ember' ? 48 + this.rank('ember-splash') * 18 : u.trait.splash || 0;
    if (splash) {
      for (const other of this.units.filter(other => other.id !== target.id && !other.dead && other.side !== u.side && other.lane === u.lane && Math.abs(other.x - target.x) <= splash)) {
        this._hit(u, other, damage * (player ? .45 + this.rank('ember-splash') * .1 : .4));
      }
      this._effect('burst', target.x, target.lane, u.color, .45);
    }
    if ((player && key === 'storm') || u.trait.chain) {
      const others = this.units.filter(other => other.id !== target.id && !other.dead && other.side !== u.side && other.lane === u.lane && Math.abs(other.x - target.x) < 170).sort((a, b) => Math.abs(a.x - target.x) - Math.abs(b.x - target.x)).slice(0, player ? 1 + this.rank('storm-chain') : 1);
      for (const other of others) {
        this._hit(u, other, damage * .55);
        this.effects.push({ type: 'lightning', x: target.x, toX: other.x, lane: u.lane, color: u.color, life: .25, maxLife: .25, age: 0 });
      }
    }
    const drain = player && key === 'void' ? .28 + this.rank('void-drain') * .1 : u.trait.lifesteal || 0;
    if (drain && !u.dead && u.hp > 0) this._heal(u, dealt * drain, true);
  }

  _status(u, name, time, power) {
    if (u.dead) return;
    const current = u.statuses[name];
    u.statuses[name] = { time: Math.max(current?.time || 0, time), power: Math.max(current?.power || 0, power) };
  }

  _hit(source, target, amount, options = {}) {
    if (target.dead || amount <= 0) return 0;
    const element = options.element || source?.element;
    amount *= elementMultiplier(element, target.element);
    const armor = clamp(target.armor * (1 - (options.ignoreArmor || 0)), 0, .65);
    if (!options.dot) amount *= 1 - armor;
    if (source?.range > 80 && target.trait.rangedResist) amount *= 1 - target.trait.rangedResist;
    if (source && source.range <= 80 && target.trait.flying && source.element !== 'storm') amount *= .55;
    const prior = target.hp;
    if (target.shield > 0) {
      const shieldDamage = amount * (options.shieldBreak ? 2 : 1);
      const absorbed = Math.min(target.shield, shieldDamage);
      target.shield -= absorbed;
      amount -= absorbed / (options.shieldBreak ? 2 : 1);
      if (!options.dot) this._effect('shield', target.x, target.lane, '#ffe7b0', .25);
    }
    target.hp = Math.max(0, target.hp - Math.max(0, amount));
    if (!options.dot) {
      target.hitFlash = .12;
      this._emit('damage', { unitId: target.id, x: target.x, lane: target.lane, amount: prior - target.hp, side: target.side, element, effective: elementMultiplier(element, target.element) > 1 });
    }
    if (target.hp <= 0) {
      target.dead = true;
      this._effect('death', target.x, target.lane, target.color, .55);
      if (target.side === 'enemy') {
        this.kills++;
        this.gold += Math.round((target.boss ? 30 : 4) * (1 + this.rank('bounty') * .2));
        if (target.boss) {
          this.enemyBarrier = false;
          if (this.enemyHp <= 1) this.enemyHp = 0;
        }
      }
      this._emit('death', { unitId: target.id, x: target.x, lane: target.lane, side: target.side, boss: target.boss });
    }
    return Math.max(0, prior - target.hp);
  }

  _heal(u, amount, silent = false) {
    if (u.dead || amount <= 0) return;
    const healed = Math.min(u.maxHp - u.hp, amount);
    if (healed <= 0) return;
    u.hp += healed;
    this._effect('heal', u.x, u.lane, '#85d7a2', .45);
    if (!silent) this._emit('heal', { unitId: u.id, x: u.x, lane: u.lane, amount: healed });
  }

  _effect(type, x, lane, color, life) {
    this.effects.push({ type, x, lane, color, life, maxLife: life, age: 0 });
    if (this.effects.length > 120) this.effects.shift();
  }

  _finishIfNeeded() {
    if (this.status !== 'playing') return;
    if (this.playerHp <= 0) {
      this.status = 'lost';
      this._emit('lose', { stage: this.stage });
    } else if (!this.enemyBarrier && (this.enemyHp <= 0 || this._next >= this._schedule.length && !this.units.some(u => u.side === 'enemy' && !u.dead))) {
      this.status = 'won';
      this._emit('win', { stage: this.stage, gold: this.reward() });
    }
  }

  reward() { return this.status === 'won' ? this.gold + this.difficulty.rewardBonus : Math.floor(this.gold / 2); }
}
