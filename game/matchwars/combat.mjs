/** Match Wars combat. Coordinates are logical units; time is always seconds. */
import { TOTAL_STAGES, getStage, getChapter } from './campaign.mjs?v=4';
import { CLASSES, DEFAULT_LOADOUT, normalizeLoadout, getTroop, COMBOS } from './classes.mjs?v=4';
import { normalizeMastery, masteryBonuses } from './mastery.mjs?v=4';
export { COMBOS } from './classes.mjs?v=4';
export const ELEMENT_INFO = {
  ember: { name: 'Ember', color: '#ff805e', icon: 'flame', strong: 'grove', weak: 'tide' },
  tide: { name: 'Tide', color: '#61c7f4', icon: 'drop', strong: 'ember', weak: 'storm' },
  grove: { name: 'Grove', color: '#83d18b', icon: 'leaf', strong: 'storm', weak: 'ember' },
  storm: { name: 'Storm', color: '#e7c76d', icon: 'bolt', strong: 'tide', weak: 'grove' },
  sun: { name: 'Sun', color: '#ffe3a0', icon: 'sun', strong: 'void', weak: 'void' },
  void: { name: 'Void', color: '#b79aef', icon: 'moon', strong: 'sun', weak: 'sun' },
};

export const TROOPS = Object.freeze(Object.fromEntries(Object.entries(DEFAULT_LOADOUT).map(([key, id]) => [key, CLASSES[id]])));

const enemy = (key, name, element, role, hp, attack, speed, range, interval, traits = {}) => ({ key, name, element, role, hp, attack, speed, range, interval, color: ELEMENT_INFO[element].color, spriteAtlas: 'troops', spriteRow: Object.keys(ELEMENT_INFO).indexOf(element), spriteColumn: 4, ...traits });
const enemySpell = (id, name, cooldown, type, description) => Object.freeze({ id, name, cooldown, type, description });
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
  emberhound: enemy('emberhound', 'Ember Hound', 'ember', 'Pounce / burn', 74, 10, 61, 34, 1.25, { spriteAtlas: 'enemies', spriteColumn: 0, unlockStage: 5, burn: true, spell: enemySpell('hound-pounce', 'Blazing Pounce', 10, 'firewave', 'Leaps at the front and burns its target.') }),
  ashwitch: enemy('ashwitch', 'Ash Witch', 'ember', 'Hexfire / weakness', 58, 9, 30, 195, 1.7, { spriteAtlas: 'enemies', unlockStage: 13, burn: true, spell: enemySpell('hexfire', 'Hexfire', 9, 'curse', 'Curses two targets and ignites their defenses.') }),
  tidecrab: enemy('tidecrab', 'Tide Crab', 'tide', 'Shell / armored tank', 148, 11, 23, 33, 1.8, { spriteAtlas: 'enemies', spriteColumn: 0, unlockStage: 5, armor: .28, shield: 25, spell: enemySpell('tidal-shell', 'Tidal Shell', 10, 'blessing', 'Renews its shell and roots a nearby attacker.') }),
  siren: enemy('siren', 'Siren', 'tide', 'Song / healing control', 66, 7, 30, 190, 1.8, { spriteAtlas: 'enemies', unlockStage: 9, spell: enemySpell('siren-song', 'Siren Song', 11, 'frostnova', 'Slows the enemy formation while mending an ally.') }),
  briarspider: enemy('briarspider', 'Briar Spider', 'grove', 'Web / poison', 64, 9, 50, 70, 1.35, { spriteAtlas: 'enemies', spriteColumn: 0, unlockStage: 6, poison: true, spell: enemySpell('snare-web', 'Snare Web', 9, 'vines', 'Roots and poisons two nearby foes.') }),
  sporealchemist: enemy('sporealchemist', 'Spore Alchemist', 'grove', 'Poison cloud / healing', 70, 8, 29, 175, 1.7, { spriteAtlas: 'enemies', unlockStage: 14, poison: true, spell: enemySpell('spore-cloud', 'Spore Cloud', 10, 'vines', 'Poisons a cluster and restores the most wounded ally.') }),
  stormbeetle: enemy('stormbeetle', 'Storm Beetle', 'storm', 'Static / armored tank', 128, 11, 33, 35, 1.6, { spriteAtlas: 'enemies', spriteColumn: 0, unlockStage: 5, armor: .22, spell: enemySpell('static-carapace', 'Static Carapace', 10, 'lightningstorm', 'Gains a static shield and shocks two attackers.') }),
  zephyrdjinn: enemy('zephyrdjinn', 'Zephyr Djinn', 'storm', 'Flying / cyclone', 76, 11, 43, 205, 1.65, { spriteAtlas: 'enemies', unlockStage: 9, flying: true, spell: enemySpell('djinn-cyclone', 'Cyclone', 11, 'whirlwind', 'Pushes back two foes and weakens their attacks.') }),
  solarchimera: enemy('solarchimera', 'Solar Chimera', 'sun', 'Roar / holy assault', 138, 15, 37, 45, 1.8, { spriteAtlas: 'enemies', spriteColumn: 0, unlockStage: 8, shield: 30, siege: 1.2, spell: enemySpell('solar-roar', 'Solar Roar', 11, 'radiance', 'Rallies its allies and scorches the nearby front.') }),
  inquisitor: enemy('inquisitor', 'Inquisitor', 'sun', 'Judgment / anti-undead', 72, 11, 30, 215, 1.75, { spriteAtlas: 'enemies', unlockStage: 16, shieldBreak: true, spell: enemySpell('inquisition', 'Inquisition', 10, 'radiance', 'Breaks shields; summoned undead suffer double damage.') }),
  graveknight: enemy('graveknight', 'Grave Knight', 'void', 'Necromancy / armored tank', 130, 12, 27, 38, 1.75, { spriteAtlas: 'enemies', spriteColumn: 0, unlockStage: 7, armor: .2, raiseLimit: 3, spell: enemySpell('enemy-raise', 'Raise the Fallen', 10, 'resurrect', 'Consumes fresh corpses to raise temporary guards, at most three per knight.') }),
  soulleech: enemy('soulleech', 'Soul Leech', 'void', 'Siphon / life steal', 60, 9, 49, 160, 1.4, { spriteAtlas: 'enemies', unlockStage: 11, lifesteal: .35, spell: enemySpell('siphon', 'Soul Siphon', 9, 'voidrift', 'Drains a target and restores its own life.') }),
};

export const CHAMPION_AFFIXES = Object.freeze({
  warded: Object.freeze({ name: 'Warded', color: '#d4c58f', description: 'Starts shielded and renews its ward once below half health.' }),
  frenzied: Object.freeze({ name: 'Frenzied', color: '#e7a282', description: 'Moves and attacks faster, then periodically rallies itself.' }),
  mender: Object.freeze({ name: 'Mender', color: '#9ed6a1', description: 'Periodically restores one wounded ally.' }),
  hexer: Object.freeze({ name: 'Hexer', color: '#bca3d7', description: 'Periodically weakens and curses its nearest opponent.' }),
  siphoner: Object.freeze({ name: 'Siphoner', color: '#c7a1ca', description: 'Steals life with its attacks.' }),
  ironhide: Object.freeze({ name: 'Ironhide', color: '#aabdc4', description: 'Armored and reflects some melee health damage.' }),
});

const upgrade = (id, name, description, group, cost, max = 3) => ({ id, name, description, group, cost, max });
export const UPGRADES = [
  upgrade('ember-burn', 'Lingering Embers', 'Burn deals +35% damage per rank.', 'ember', 50),
  upgrade('ember-splash', 'Wildfire', 'Fireball splash radius +18, splash damage +10%, and fire spell power +8% per rank.', 'ember', 65),
  upgrade('tide-slow', 'Deep Freeze', 'Frost slows last +1 second and slow +6%; Tide Oracle healing gains +10% per rank.', 'tide', 50),
  upgrade('tide-range', 'Farshot', 'Rangers gain +25 range and +8% attack per rank.', 'tide', 65),
  upgrade('grove-heal', 'Living Spring', 'Grove healing and Thorn Guardian spell shields gain +25% strength per rank.', 'grove', 50),
  upgrade('grove-poison', 'Venom Bloom', 'Poison damage and Bloom Druid regeneration gain +35% strength per rank.', 'grove', 65),
  upgrade('storm-chain', 'Forked Lightning', 'Lightning hits one extra target; Windcaller affects one extra ally and enemy per rank (five maximum).', 'storm', 55),
  upgrade('storm-speed', 'Swift Formation', 'Lancers move and attack 10% faster per rank.', 'storm', 65),
  upgrade('sun-shield', 'Dawn Aegis', 'Sun troops gain +18 starting shield per rank; Sun shield spells also grow stronger.', 'sun', 50),
  upgrade('sun-armor', 'Golden Bulwark', 'Sun troops gain 8% damage reduction per rank, up to 50% total armor.', 'sun', 65),
  upgrade('void-drain', 'Soul Harvest', 'Void troops steal an extra 10% life; raised guards gain +6% health per rank.', 'void', 55),
  upgrade('void-execute', 'Final Eclipse', 'Reaper execute bonus +20%, Hexweaver vulnerability +3%, and bone guard attack +8% per rank.', 'void', 65),
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
  const extension = Math.max(0, stage - 84);
  const coreBeyond = Math.min(72, beyond);
  const boss = getStage(stage).boss;
  const totalWaves = stage <= 12 ? 3 : stage < 33 ? 4 : stage < 57 ? 5 : 6;
  const unitsPerWave = stage <= 12 ? 4 + Math.min(4, Math.floor(stage / 3)) : 8 + Math.min(6, Math.floor((stage - 13) / 9));
  const healthMultiplier = stage <= 12 ? 1 + (stage - 1) * .055 : 1.605 + coreBeyond * .026 + extension * .012;
  const attackMultiplier = stage <= 12 ? healthMultiplier : 1.605 + coreBeyond * .017 + extension * .0075;
  const waveInterval = 20 - coreBeyond * .075 - extension * .015;
  const spawnInterval = 1.65 - coreBeyond * .009 - extension * .003;
  const bossHealthMultiplier = stage <= 12 ? 1 + Math.max(0, stage - 4) * .035 : 1.28 + coreBeyond * .03 + extension * .016;
  const bossAttackMultiplier = stage <= 12 ? bossHealthMultiplier : 1.28 + coreBeyond * .012 + extension * .006;
  const eliteEvery = stage <= 12 ? 0 : Math.max(7, 12 - Math.floor((stage - 13) / 6));
  const averageUnits = unitsPerWave + (totalWaves - 1) / 2;
  const waveAttackGrowth = stage <= 12 ? .035 : .018;
  const championEvery = extension ? Math.max(10, 14 - Math.floor((extension - 1) / 12)) : 0;
  const pressure = averageUnits / waveInterval * (healthMultiplier + (totalWaves - 1) * .035 / 2) *
    (attackMultiplier + (totalWaves - 1) * waveAttackGrowth / 2) * 100 * (championEvery ? 1 + .08 / championEvery : 1);
  return Object.freeze({
    stage, totalWaves, unitsPerWave, waveInterval: rounded(waveInterval), spawnInterval: rounded(spawnInterval),
    healthMultiplier: rounded(healthMultiplier), attackMultiplier: rounded(attackMultiplier),
    bossHealthMultiplier: rounded(bossHealthMultiplier), bossAttackMultiplier: rounded(bossAttackMultiplier),
    speedMultiplier: rounded(1 + coreBeyond * .0015 + extension * .0005), waveHealthGrowth: .035, waveAttackGrowth,
    eliteEvery, eliteChance: eliteEvery ? rounded(1 / eliteEvery) : 0,
    championEvery, championChance: championEvery ? rounded(1 / championEvery) : 0,
    keepHealth: 180 + stage * 18 + beyond * 10,
    scheduledUnits: totalWaves * unitsPerWave + totalWaves * (totalWaves - 1) / 2 + (boss ? 1 : 0),
    bossAt: rounded(5 + (totalWaves - 1) * waveInterval + 4 + Math.min(3, beyond * .08)),
    pressure: rounded(pressure, 2), rewardBonus: 65 + stage * 12 + (boss ? 45 : 0),
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
  constructor({ stage = 1, upgrades = {}, loadout = DEFAULT_LOADOUT, mastery = {}, rng = Math.random, onEvent = () => {} } = {}) {
    this.stage = Math.floor(clamp(stage, 1, TOTAL_STAGES));
    this.difficulty = difficultyForStage(this.stage);
    this.chapter = getChapter(this.stage);
    this.stageInfo = getStage(this.stage);
    this.upgrades = Object.fromEntries(UPGRADES.map(def => [def.id, Math.floor(clamp(upgrades[def.id] || 0, 0, def.max))]));
    this.loadout = Object.freeze(normalizeLoadout(loadout));
    this.mastery = Object.freeze(Object.fromEntries(Object.entries(normalizeMastery(mastery)).map(([id, record]) => [id, Object.freeze(record)])));
    this.classStats = Object.fromEntries(Object.keys(CLASSES).map(id => [id, { deployments: 0, casts: 0, kills: 0, damage: 0, healing: 0, shields: 0, raised: 0 }]));
    this.rng = rng;
    this.onEvent = onEvent;
    this.units = [];
    this.projectiles = [];
    this.effects = [];
    this.corpses = [];
    this.time = 0;
    this.wave = 0;
    this.totalWaves = this.difficulty.totalWaves;
    this.playerMax = 180 + this.rank('fortress') * 40;
    this.enemyMax = this.difficulty.keepHealth;
    this.playerHp = this.playerMax;
    this.enemyHp = this.enemyMax;
    this.enemyBarrier = this.stageInfo.boss;
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
    this._formationRevision = 0;
    this._comboCache = [null, null, null];
    this._comboAnnounced = [new Set(), new Set(), new Set()];
    this._schedule = this._buildSchedule();
  }

  rank(id) { return this.upgrades[id] || 0; }

  troopFor(key) { return getTroop(key, this.loadout[key]); }

  _stat(unit, field, amount = 1) {
    const id = unit?.masteryClassId || unit?.classId;
    if (unit?.side !== 'player' || !this.classStats[id] || !Number.isFinite(amount) || amount <= 0) return;
    this.classStats[id][field] = Math.min(1_000_000, this.classStats[id][field] + amount);
  }

  activeCombos(value = 1) {
    const lane = Math.floor(clamp(value, 0, 2));
    const signature = `${this._formationRevision}:${this.units.length}`;
    if (this._comboCache[lane]?.signature === signature) return this._comboCache[lane].combos;
    const elements = new Set(this.units.filter(u => !u.dead && u.hp > 0 && u.side === 'player' && u.lane === lane).map(u => u.element));
    const combos = Object.freeze(COMBOS.filter(combo => combo.elements.every(element => elements.has(element))));
    this._comboCache[lane] = { signature, combos };
    return combos;
  }

  _hasCombo(lane, id) { return this.activeCombos(lane).some(combo => combo.id === id); }

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
        const roster = Object.values(ENEMIES).filter(def => def.element === element && !def.boss && (def.unlockStage || 1) <= this.stage);
        // Stage one previews simple enemies; later encounters deliberately mix roles.
        let variant = this.stage <= 12
          ? this.stage === 1 && wave === 1 ? 0 : (index + wave + Math.floor(this.stage / 2)) % roster.length
          : [2, 1, 0, 1, 2][Math.floor(index / 3) % 5];
        if (this.stage > 12 && roster.length > 3) {
          const formation = Math.floor(index / 3);
          if ((this.stage + wave + lane + formation) % 3 === 0) variant = formation % 2 && roster[4] ? 4 : 3;
        }
        const serial = schedule.length + 1;
        const affixes = Object.keys(CHAMPION_AFFIXES);
        schedule.push({
          time: rounded(5 + (wave - 1) * this.difficulty.waveInterval + index * this.difficulty.spawnInterval),
          wave, lane, key: roster[variant].key,
          elite: !!this.difficulty.eliteEvery && serial % this.difficulty.eliteEvery === 0,
          affix: this.difficulty.championEvery && serial % this.difficulty.championEvery === 0 ? affixes[(serial + this.chapter.number + wave) % affixes.length] : null,
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
      return { ...entry, time: Math.max(0, entry.time - this.time), element: def.element, name: def.name, role: def.role, counter, boss: !!def.boss, elite: !!entry.elite, tag: def.tag || null };
    });
  }

  _enemyDefinition(entry) {
    const base = ENEMIES[entry.key];
    if (!base.boss) {
      const veteran = entry.elite ? { ...base, elite: true, name: `Veteran ${base.name}`, role: `Veteran / ${base.role}`, armor: Math.min(.35, (base.armor || 0) + .04) } : base;
      const affix = CHAMPION_AFFIXES[entry.affix];
      if (!affix) return veteran;
      return { ...veteran, affix: entry.affix, tag: `${affix.name} Champion`, name: `${affix.name} ${base.name}`, role: `${affix.name} / ${base.role}`,
        armor: Math.min(.4, (veteran.armor || 0) + (entry.affix === 'ironhide' ? .08 : 0)),
        reflect: entry.affix === 'ironhide' ? .08 : veteran.reflect || 0,
        lifesteal: Math.min(.45, (veteran.lifesteal || 0) + (entry.affix === 'siphoner' ? .15 : 0)),
      };
    }
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
      13: { poison: true, shieldBreak: true, armor: .16 },
      14: { healer: true, regen: 6, shield: 120 },
      15: { burn: true, splash: 90, shield: 120, siege: 1.5 },
      16: { healer: true, chain: true, shield: 200 },
      17: { flying: true, chain: true, splash: 80 },
      18: { chain: true, splash: 75, shield: 240, summonLimit: 8, shieldBreak: true },
      19: { healer: true, chain: true, shield: 220, regen: 6 },
      20: { flying: true, shieldBreak: true, splash: 85, shield: 200 },
      21: { chain: true, splash: 85, shield: 260, summonLimit: 8, shieldBreak: true, poison: true },
    };
    const late = this.stage > 84 ? [
      { armor: .16, shield: 160, shieldBreak: true }, { chain: true, shield: 150 },
      { healer: true, shield: 140, regen: 5 }, { poison: true, splash: 60, shield: 170 },
    ][(this.chapter.number - 22) % 4] : {};
    const bossSpell = this.stage > 84 ? base.key === 'rootking' ? enemySpell('boss-vines', 'Sovereign Roots', 14, 'vines', 'Roots two nearby attackers.') : base.key === 'frostqueen' ? enemySpell('boss-whiteout', 'Royal Whiteout', 15, 'frostnova', 'Briefly freezes two nearby attackers.') : enemySpell('boss-ruin', 'Ruin Pulse', 15, 'voidrift', 'Sears and weakens two nearby attackers.') : null;
    return { ...base, ...tricks[this.chapter.number], ...late, ...(bossSpell ? { spell: bossSpell } : {}), name: entry.name || this.stageInfo.bossName || base.name,
      secondWind: this.stage > 12, phaseThresholds: this.stage > 100 ? [.65, .3] : [.5], phasePower: Math.min(.3, .12 + Math.max(0, this.stage - 12) * .002),
    };
  }

  /** One event reinforcement; never advances the encounter director or its waves. */
  reinforce(key, { lane = 1, affix = null } = {}) {
    if (this.status !== 'playing' || this.units.length >= 64) return null;
    let definition = Object.prototype.hasOwnProperty.call(ENEMIES, key) ? ENEMIES[key] : null;
    if (!definition && ELEMENT_INFO[key]) definition = Object.values(ENEMIES).find(enemy => enemy.element === key && !enemy.boss && (enemy.unlockStage || 1) <= this.stage);
    if (!definition || definition.boss) return null;
    const entry = { key: definition.key, lane: Math.floor(clamp(lane, 0, 2)), wave: Math.max(1, this.wave), affix: CHAMPION_AFFIXES[affix] ? affix : null };
    const index = this.units.length;
    return this._spawnEnemy(entry) ? this.units[index] : null;
  }

  summon(key, { lane = 1, power = 1, chain = 1, buffs = [], elite = false, count = 3 } = {}) {
    if (this.status !== 'playing' || !TROOPS[key]) return [];
    const def = this.troopFor(key);
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
      if (key === 'sun') { u.armor = Math.min(.5, (def.armor || 0) + this.rank('sun-armor') * .08); u.shield += this.rank('sun-shield') * 18; }
      if (key === 'tide') { u.range += this.rank('tide-range') * 25; u.attack *= 1 + this.rank('tide-range') * .08; }
      if (key === 'storm') { const swift = 1 + this.rank('storm-speed') * .1; u.speed *= swift; u.interval /= swift; }
      const initialShield = u.shield; u.shield = 0;
      this.units.push(u);
      this._formationRevision++;
      this._shield(u, initialShield, u);
      this.synergy[key]++;
      this._stat(u, 'deployments');
      spawned.push(u);
      this._emit('spawn', { unit: u, side: 'player', key, lane });
      this._effect('summon', u.x, lane, def.color, .65);
      if (def.shieldOnSpawn) {
        const ally = this.units.filter(other => other.side === 'player' && other.lane === lane && other.id !== u.id).sort((a, b) => Math.abs(a.x - u.x) - Math.abs(b.x - u.x))[0];
        if (ally) { this._shield(ally, 16 + this.rank('sun-shield') * 18, u); this._effect('shield', ally.x, lane, '#ffe3a0', .7); }
      }
    }
    return spawned;
  }

  _makeUnit(def, side, lane, strength = 1) {
    const mastery = side === 'player' && def.id && CLASSES[def.id] ? masteryBonuses(def.id, this.mastery[def.id]) : null;
    const mods = mastery?.mods || {};
    return {
      id: ++this._id, key: def.element, enemyType: side === 'enemy' ? def.key : null,
      name: def.name, role: def.role, element: def.element, color: def.color,
      side, lane, power: strength, x: side === 'player' ? 85 : 915, hp: def.hp * strength * (mastery?.hp || 1),
      maxHp: def.hp * strength * (mastery?.hp || 1), attack: def.attack * strength * (mastery?.attack || 1), speed: def.speed * (mods.speed || 1),
      range: def.range + (mods.range || 0), interval: def.interval / (mods.attackRate || 1), cooldown: .3, state: 'walk', anim: 0,
      shield: def.shield || 0, armor: Math.min(.5, (def.armor || 0) + (mods.armor || 0)), buffs: [], boss: !!def.boss,
      mods, masteryRank: mastery?.rank || 1, specialization: mastery?.specialization?.id || null, specializationName: mastery?.specialization?.name || null,
      masteryCooldown: mastery?.cooldown || 1, affix: def.affix || null, tag: def.tag || null, affixCooldown: 8,
      classId: side === 'player' ? def.id || null : null, spriteAtlas: def.spriteAtlas || 'troops', spriteRow: def.spriteRow, spriteColumn: def.spriteColumn || 0,
      spell: def.spell || null, spellCooldown: def.spell ? 2.5 + (this._id % 3) * .4 : 0, casting: 0, spellCasts: 0, raises: 0,
      statuses: {}, supportCooldown: .8, elite: !!def.elite, undead: false,
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
    if (u.affix) { u.hp *= 1.12; u.maxHp = u.hp; u.attack *= 1.05; }
    if (u.affix === 'warded') u.shield += 45 + u.maxHp * .12;
    if (u.affix === 'frenzied') { u.speed *= 1.08; u.interval /= 1.12; }
    if (this.stage > 12) u.shield *= Math.sqrt(healthScale);
    u.x += (this.units.filter(other => other.side === 'enemy' && other.lane === u.lane && other.x > 890).length) * 20;
    this.units.push(u);
    this._formationRevision++;
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
        this._shield(u, 12 * strength);
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
    for (let lane = 0; lane < 3; lane++) {
      const active = this.activeCombos(lane);
      for (const combo of active) if (!this._comboAnnounced[lane].has(combo.id)) {
        this._effect('combo', 500, lane, '#e5ce92', 1, { label: combo.name });
        this._emit('combo', { id: combo.id, name: combo.name, lane });
      }
      this._comboAnnounced[lane] = new Set(active.map(combo => combo.id));
    }
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
    for (const corpse of this.corpses) corpse.life -= dt;
    this.corpses = this.corpses.filter(corpse => corpse.life > 0 && !corpse.consumed).slice(-32);
    for (const effect of this.effects) { effect.life -= dt; effect.age += dt; }
    this.effects = this.effects.filter(effect => effect.life > 0).slice(-120);
    this._finishIfNeeded();
  }

  _tickUnit(u, dt, alive) {
    if (u.dead) return;
    const thresholds = u.trait.phaseThresholds || [.5];
    const phaseIndex = u.phaseIndex || 0;
    if (u.boss && u.trait.secondWind && phaseIndex < thresholds.length && u.hp < u.maxHp * thresholds[phaseIndex]) {
      u.phaseUsed = true;
      u.phaseIndex = phaseIndex + 1;
      this._status(u, 'rally', 9, u.trait.phasePower);
      u.shield += 25 + this.stage;
      this._effect('rally', u.x, u.lane, u.color, 1);
      this._emit('boss-phase', { name: u.name, unitId: u.id, lane: u.lane, x: u.x, phase: u.phaseIndex });
    }
    u.anim += dt;
    u.hitFlash = Math.max(0, u.hitFlash - dt);
    u.casting = Math.max(0, u.casting - dt);
    u.spellCooldown = Math.max(0, u.spellCooldown - dt);
    if (u.decay != null) {
      u.decay -= dt;
      if (u.decay <= 0) { this._kill(u, true); return; }
    }
    u.cooldown -= dt;
    u.supportCooldown -= dt;
    for (const [name, effect] of Object.entries(u.statuses)) {
      const applied = Math.min(dt, effect.time);
      if (name === 'burn' || name === 'poison') this._hit(null, u, effect.power * applied, { element: name === 'burn' ? 'ember' : 'grove', dot: true });
      if (name === 'regeneration') this._heal(u, effect.power * applied, true);
      effect.time -= dt;
      if (effect.time <= 0) delete u.statuses[name];
    }
    if (u.dead) return;
    if (u.trait.regen) u.hp = Math.min(u.maxHp, u.hp + u.trait.regen * dt);
    const allies = alive.filter(other => other.side === u.side && other.lane === u.lane && !other.dead);
    if (u.affix) this._tickAffix(u, dt, allies, alive);
    if ((u.side === 'player' && u.trait.passiveHeal || u.side === 'enemy' && u.trait.healer) && u.supportCooldown <= 0) {
      const target = allies.filter(other => Math.abs(other.x - u.x) < 190 && other.hp < other.maxHp).sort((a, b) => a.hp / a.maxHp - b.hp / b.maxHp)[0];
      if (target) {
        const amount = u.side === 'player' ? 12 * (1 + this.rank('grove-heal') * .25) * (u.mods.passiveHealPower || 1) : 10 + Math.min(50, this.stage * .6);
        this._heal(target, amount, false, u);
        u.supportCooldown = 3;
      }
    }
    if (u.trait.summoner) {
      u.summonCooldown -= dt;
      if (u.summonCooldown <= 0 && u.summons < (u.trait.summonLimit || (u.boss ? 4 : 2)) && this.units.length < 60) {
        const minion = this._makeUnit(ENEMIES.shade, 'enemy', u.lane, .65 + this.stage * .025);
        minion.x = u.x - 25;
        this.units.push(minion);
        minion.undead = true; minion.noCorpse = true; minion.noReward = true; minion.decay = 20;
        this._formationRevision++;
        u.summons++;
        u.summonCooldown = 10;
        this._effect('summon', minion.x, minion.lane, '#b79aef', .6);
        this._emit('spawn', { unit: minion, side: 'enemy', key: minion.key, lane: minion.lane });
      }
    }
    const targets = alive.filter(other => other.side !== u.side && other.lane === u.lane && !other.dead);
    if (u.statuses.freeze) { u.state = 'idle'; return; }
    if (u.casting > 0) { u.state = 'cast'; return; }
    if (u.spell && u.spellCooldown <= 0) {
      if (this._autoCast(u, allies, targets)) { u.state = 'cast'; return; }
      u.spellCooldown = .5;
    }
    const target = targets.sort((a, b) => Math.abs(a.x - u.x) - Math.abs(b.x - u.x))[0];
    const direction = u.side === 'player' ? 1 : -1;
    const range = u.range + (target?.boss ? 14 : 0);
    if (target && Math.abs(target.x - u.x) <= range) {
      u.state = 'attack';
      if (u.cooldown <= 0) {
        this._attack(u, target);
        const rally = u.statuses.rally?.power || 0;
        const aegis = u.side === 'player' && u.shield > 0 && this._hasCombo(u.lane, 'thunderous-aegis') ? .12 : 0;
        u.cooldown = u.interval / (1 + rally + aegis);
        u.anim = 0;
      }
      return;
    }
    const baseX = u.side === 'player' ? 920 : 80;
    if (!target && Math.abs(baseX - u.x) <= Math.max(35, u.range * .65)) {
      u.state = 'attack';
      if (u.cooldown <= 0) {
        const weakness = u.statuses.weakness?.power || 0;
        const steam = u.side === 'enemy' && u.statuses.burn && this._hasCombo(u.lane, 'steamguard') ? .85 : 1;
        const amount = u.attack * .75 * (u.trait.siege || 1) * (1 - weakness) * (1 + (u.statuses.rally?.power || 0)) * steam;
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
    const aegis = u.side === 'player' && u.shield > 0 && this._hasCombo(u.lane, 'thunderous-aegis') ? .1 : 0;
    let movement = u.statuses.root ? 0 : u.speed * dt * (1 - slow) * (1 + rally + aegis);
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
    if (u.side === 'player' && u.trait.execute && target.hp / target.maxHp < .35) amount *= 1.4 + this.rank('void-execute') * .2 + (u.mods.executeBonus || 0);
    if (u.range > 80) {
      if (this.projectiles.length < 100) this.projectiles.push({ id: ++this._projectileId, sourceId: u.id, targetId: target.id, x: u.x, lane: u.lane, element: u.element, color: u.color, side: u.side, speed: 470, damage: amount, age: 0, trait: u.trait, key: u.key, classId: u.classId, masteryClassId: u.masteryClassId, mods: u.mods, statuses: { ...u.statuses } });
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
    if (u.trait.burn) this._status(target, 'burn', 4 + (u.mods?.burnDuration || 0), (player ? 3.5 * (1 + this.rank('ember-burn') * .35) : 2.4) * (u.mods?.burnPower || 1));
    if (u.trait.slow) this._status(target, 'slow', (player ? 3 + this.rank('tide-slow') : 2.4) + (u.mods?.slowDuration || 0), Math.min(.65, (player ? .32 + this.rank('tide-slow') * .06 : .25) + (u.mods?.slowPower || 0)));
    if (u.trait.poison) this._status(target, 'poison', 5, (player ? 3 * (1 + this.rank('grove-poison') * .35) : 2.8) * (u.mods?.poisonPower || 1));
    const splash = u.trait.splash ? u.trait.splash + (player ? this.rank('ember-splash') * 18 : 0) + (u.mods?.splashRadius || 0) : 0;
    if (splash) {
      for (const other of this.units.filter(other => other.id !== target.id && !other.dead && other.side !== u.side && other.lane === u.lane && Math.abs(other.x - target.x) <= splash)) {
        this._hit(u, other, damage * (player ? .45 + this.rank('ember-splash') * .1 : .4));
      }
      this._effect('burst', target.x, target.lane, u.color, .45);
    }
    if (u.trait.chain) {
      const others = this.units.filter(other => other.id !== target.id && !other.dead && other.side !== u.side && other.lane === u.lane && Math.abs(other.x - target.x) < 170).sort((a, b) => Math.abs(a.x - target.x) - Math.abs(b.x - target.x)).slice(0, player ? Math.min(5, 1 + this.rank('storm-chain') + (u.mods?.chainTargets || 0)) : 1);
      for (const other of others) {
        this._hit(u, other, damage * .55);
        this.effects.push({ type: 'lightning', x: target.x, toX: other.x, lane: u.lane, color: u.color, life: .25, maxLife: .25, age: 0 });
      }
    }
    const drain = Math.min(.7, (u.trait.lifesteal || 0) + (player && key === 'void' ? this.rank('void-drain') * .1 : 0) + (u.mods?.drainBonus || 0));
    if (drain && !u.dead && u.hp > 0) this._heal(u, dealt * drain, true, u);
  }

  _shield(target, amount, source = null) {
    if (target.dead || amount <= 0) return 0;
    let bonus = 0;
    if (target.side === 'player') {
      if (this._hasCombo(target.lane, 'verdant-covenant')) bonus += .2;
      if (this._hasCombo(target.lane, 'steamguard')) bonus += .1;
    }
    const added = Math.min(amount * (1 + bonus) * (source?.mods?.shieldPower || target.mods?.shieldPower || 1), Math.max(0, target.maxHp * .8 + 50 - target.shield));
    target.shield += added;
    this._stat(source, 'shields', added);
    return added;
  }

  _cleanse(target) {
    for (const name of ['burn', 'poison', 'weakness', 'curse']) delete target.statuses[name];
  }

  _tickAffix(u, dt, allies, alive) {
    u.affixCooldown -= dt;
    if (u.affix === 'warded' && !u.wardRenewed && u.hp < u.maxHp * .5) {
      u.wardRenewed = true;
      this._shield(u, 35 + u.maxHp * .1, u);
      this._effect('blessing', u.x, u.lane, '#d4c58f', .7, { label: 'Ward renewed' });
      this._emit('enemy-affix', { unitId: u.id, affix: u.affix, tag: u.tag, lane: u.lane, x: u.x });
      return;
    }
    if (u.affixCooldown > 0) return;
    let useful = false;
    if (u.affix === 'frenzied') {
      this._status(u, 'rally', 5, .16); useful = true;
      this._effect('rally', u.x, u.lane, '#e7a282', .6, { label: 'Frenzy' });
    } else if (u.affix === 'mender') {
      const target = allies.filter(other => !other.dead && other.hp < other.maxHp - 1 && Math.abs(other.x - u.x) < 220).sort((a, b) => a.hp / a.maxHp - b.hp / b.maxHp)[0];
      if (target) { this._heal(target, 18 + Math.min(14, this.stage * .1), false, u); this._effect('healbeam', u.x, u.lane, '#9ed6a1', .65, { toX: target.x, label: 'Mending pulse' }); useful = true; }
    } else if (u.affix === 'hexer') {
      const target = alive.filter(other => !other.dead && other.side !== u.side && other.lane === u.lane && Math.abs(other.x - u.x) < 250).sort((a, b) => Math.abs(a.x - u.x) - Math.abs(b.x - u.x))[0];
      if (target) { this._status(target, 'weakness', 4, .15); this._status(target, 'curse', 4, .08); this._effect('curse', u.x, u.lane, '#bca3d7', .65, { toX: target.x, label: 'Champion hex' }); useful = true; }
    }
    u.affixCooldown = useful ? 14 : 1;
    if (useful) this._emit('enemy-affix', { unitId: u.id, affix: u.affix, tag: u.tag, lane: u.lane, x: u.x });
  }

  /** Cast only when there is a useful target; idle healers and necromancers wait. */
  _autoCast(u, allies, foes) {
    const spell = u.spell;
    if (!spell || u.dead) return false;
    const near = foes.filter(other => !other.dead && Math.abs(other.x - u.x) < Math.max(245, u.range + 35))
      .sort((a, b) => Math.abs(a.x - u.x) - Math.abs(b.x - u.x));
    const friends = allies.filter(other => !other.dead && Math.abs(other.x - u.x) < 245);
    const wounded = friends.filter(other => other.hp < other.maxHp - 1 || other.statuses.burn || other.statuses.poison || other.statuses.curse)
      .sort((a, b) => a.hp / a.maxHp - b.hp / b.maxHp);
    const player = u.side === 'player';
    const mods = u.mods || {};
    const strength = player ? Math.sqrt(clamp(u.power || 1, .25, 8)) * (1 + this.rank('spellcraft') * .05) * (u.key === 'ember' ? 1 + this.rank('ember-splash') * .08 : 1) : 1;
    const healPower = (player ? 22 * (u.key === 'grove' ? 1 + this.rank('grove-heal') * .25 : 1) : 16 + Math.min(20, this.stage * .25)) * strength;
    const blast = (base, factor = .65) => (base + u.attack * factor) * strength;
    const hit = (target, amount, options = {}) => this._hit(u, target, amount, options);
    const heal = (target, amount, silent = false) => this._heal(target, amount * (mods.healPower || 1), silent, u);
    const shield = (target, amount) => this._shield(target, amount, u);
    const debuff = (target, name, seconds, power) => {
      if (name === 'burn') { seconds += mods.burnDuration || 0; power *= mods.burnPower || 1; }
      if (name === 'poison') power *= mods.poisonPower || 1;
      if (name === 'slow') { seconds += mods.slowDuration || 0; power = Math.min(.65, power + (mods.slowPower || 0)); }
      if (name === 'freeze') seconds += mods.freezeDuration || 0;
      if (name === 'root') seconds += mods.rootDuration || 0;
      if (name === 'curse') { seconds += mods.curseDuration || 0; power = Math.min(.3, power + (mods.cursePower || 0)); }
      if (name === 'weakness') power = Math.min(.4, power + (mods.weaknessPower || 0));
      this._status(target, name, seconds, power);
    };
    let target = near[0];
    let useful = false;
    switch (spell.id) {
      case 'firewave':
        if (!near.length) return false;
        for (const foe of near.slice(0, 3 + (mods.spellTargets || 0))) { hit(foe, blast(8)); debuff(foe, 'burn', 5, 3 * (1 + this.rank('ember-burn') * .35)); }
        useful = true; break;
      case 'cinder-charge': case 'hound-pounce':
        if (!target) return false;
        u.x = clamp(u.x + Math.sign(target.x - u.x) * Math.min(40, Math.max(0, Math.abs(target.x - u.x) - 38)), 55, 945);
        hit(target, blast(12) * (mods.chargeDamage || 1)); debuff(target, 'burn', 4, 3);
        if (mods.chargeRoot) debuff(target, 'root', mods.chargeRoot, 1);
        if (mods.chargeFortify) this._status(u, 'fortify', 4, mods.chargeFortify);
        if (player) shield(u, 24 * strength);
        useful = true; break;
      case 'phoenix-flare':
        if (!near.length && !wounded.length) return false;
        for (const foe of near.slice(0, 2 + (mods.phoenixTargets || 0))) { hit(foe, blast(7)); debuff(foe, 'burn', 4, 3); }
        if (wounded[0]) heal(wounded[0], healPower);
        useful = true; break;
      case 'piercing-frost':
        if (!target) return false;
        for (const foe of near.slice(0, 2 + (mods.pierceTargets || 0))) { hit(foe, blast(9)); debuff(foe, 'slow', 4 + this.rank('tide-slow'), .4 + this.rank('tide-slow') * .04); }
        useful = true; break;
      case 'frostnova':
        if (!target) return false;
        for (const foe of near.slice(0, 3 + (mods.freezeTargets || 0))) { hit(foe, blast(5)); debuff(foe, 'freeze', 1.35, 1); debuff(foe, 'slow', 4, .35); }
        useful = true; break;
      case 'restorative-tide':
        if (!wounded.length) return false;
        for (const ally of wounded.slice(0, 2 + (mods.healingTargets || 0))) { this._cleanse(ally); heal(ally, healPower * 1.15 * (1 + this.rank('tide-slow') * .1)); if (mods.cleanseShield) shield(ally, mods.cleanseShield); }
        target = wounded[0]; useful = true; break;
      case 'living-roots':
        if (!target && !wounded.length) return false;
        if (target) { hit(target, blast(4)); debuff(target, 'root', 2, 1); }
        if (wounded[0]) { heal(wounded[0], healPower * .8); if (mods.rootHealShield) shield(wounded[0], mods.rootHealShield); }
        useful = true; break;
      case 'renewal-garden':
        if (!wounded.length) return false;
        for (const ally of wounded.slice(0, 3 + (mods.healingTargets || 0))) { heal(ally, healPower * .7); this._status(ally, 'regeneration', 5 + (mods.regenDuration || 0), 4 * (1 + this.rank('grove-poison') * .35) * strength * (mods.regenPower || 1)); if (mods.regenerationShield) shield(ally, mods.regenerationShield); }
        target = wounded[0]; useful = true; break;
      case 'thicket-bulwark':
        if (!target && u.hp > u.maxHp * .85) return false;
        shield(u, 40 * strength * (1 + this.rank('grove-heal') * .25));
        for (const foe of near.slice(0, 2 + (mods.rootTargets || 0))) { debuff(foe, 'root', 2.4, 1); hit(foe, blast(3) * (mods.thornDamage || 1)); }
        useful = true; break;
      case 'chain-lance': case 'thunderstorm':
        if (!target) return false;
        for (const foe of near.slice(0, Math.min(6, Math.min(5, 3 + this.rank('storm-chain')) + (mods.spellTargets || 0)))) { hit(foe, blast(spell.id === 'thunderstorm' ? 9 : 5)); if (mods.lightningSlow) debuff(foe, 'slow', 4, mods.lightningSlow); }
        useful = true; break;
      case 'zephyr-aura':
        if (!target && friends.length < 2) return false;
        for (const ally of friends.slice(0, Math.min(5, 3 + this.rank('storm-chain')))) this._status(ally, 'rally', 6 + (mods.rallyDuration || 0), .22 + (mods.rallyPower || 0));
        for (const foe of near.slice(0, Math.min(6, Math.min(5, 2 + this.rank('storm-chain')) + (mods.windTargets || 0)))) { foe.x = clamp(foe.x + (player ? 1 : -1) * (25 + (mods.pushDistance || 0)), 55, 945); hit(foe, blast(2, .35) * (mods.windDamage || 1)); }
        useful = true; break;
      case 'dawn-barrier':
        if (!target && !wounded.length) return false;
        for (const ally of [...friends].sort((a, b) => a.shield - b.shield).slice(0, 2)) {
          shield(ally, (25 + this.rank('sun-shield') * 6) * strength);
          this._status(ally, 'fortify', 5, .12 + (mods.barrierFortify || 0));
        }
        if (target && mods.barrierRadiance) hit(target, mods.barrierRadiance * strength);
        useful = true; break;
      case 'cleansing-radiance':
        if (!wounded.length && !near.some(foe => foe.undead || foe.element === 'void')) return false;
        for (const ally of wounded.slice(0, 3)) { this._cleanse(ally); heal(ally, healPower); if (mods.cleanseFortify) this._status(ally, 'fortify', 4, mods.cleanseFortify); }
        for (const ally of friends.slice(0, 1 + (mods.priestShieldTargets || 0))) shield(ally, (18 + this.rank('sun-shield') * 5) * strength);
        for (const foe of near.filter(foe => foe.undead || foe.element === 'void').slice(0, 3)) hit(foe, blast(12) * (foe.undead ? 2 : 1) * (mods.purgePower || 1));
        useful = true; break;
      case 'solar-verdict': case 'inquisition':
        if (!target) return false;
        target = near.find(foe => foe.undead) || target;
        for (const foe of [target, ...near.filter(foe => foe.id !== target.id)].slice(0, 1 + (mods.verdictTargets || 0))) hit(foe, blast(18) * (foe.undead ? 2 : 1) * (mods.verdictPower || 1) * (foe.hp / foe.maxHp < .35 ? 1 + (mods.verdictExecute || 0) : 1), { shieldBreak: true });
        useful = true; break;
      case 'soul-harvest': case 'siphon': {
        if (!target) return false;
        const execute = spell.id === 'soul-harvest' && target.hp / target.maxHp < (mods.harvestThreshold || .35) ? 1.6 + this.rank('void-execute') * .15 + (mods.executeBonus || 0) : 1;
        const drained = hit(target, blast(12) * execute);
        heal(u, Math.max(healPower * .7, drained * .45) * (mods.harvestHeal || 1));
        useful = true; break;
      }
      case 'raise-boneguard': case 'enemy-raise':
        if (!this._raiseGuard(u)) return false;
        useful = true; break;
      case 'unravel-curse': case 'hexfire':
        if (!target) return false;
        for (const foe of near.slice(0, 2)) {
          hit(foe, blast(5)); debuff(foe, 'weakness', 6, .25); debuff(foe, 'curse', 6, .15 + (player ? this.rank('void-execute') * .03 : 0));
          if (spell.id === 'hexfire') debuff(foe, 'burn', 5, 3);
        }
        if (mods.curseShield) shield(u, mods.curseShield * Math.min(2, near.length));
        useful = true; break;
      case 'tidal-shell':
        if (!target && u.hp > u.maxHp * .85) return false;
        shield(u, 35 + this.stage * .2);
        if (target) debuff(target, 'root', 1.5, 1);
        useful = true; break;
      case 'siren-song':
        if (!target && !wounded.length) return false;
        for (const foe of near.slice(0, 3)) debuff(foe, 'slow', 4, .4);
        if (wounded[0]) heal(wounded[0], healPower);
        useful = true; break;
      case 'snare-web':
        if (!target) return false;
        for (const foe of near.slice(0, 2)) { debuff(foe, 'root', 1.8, 1); debuff(foe, 'poison', 5, 3.5); }
        useful = true; break;
      case 'spore-cloud':
        if (!target && !wounded.length) return false;
        for (const foe of near.slice(0, 3)) { hit(foe, blast(3)); debuff(foe, 'poison', 5, 4); }
        if (wounded[0]) heal(wounded[0], healPower);
        useful = true; break;
      case 'static-carapace':
        if (!target) return false;
        shield(u, 30);
        for (const foe of near.slice(0, 2)) hit(foe, blast(8));
        useful = true; break;
      case 'djinn-cyclone':
        if (!target) return false;
        for (const foe of near.slice(0, 2)) { foe.x = clamp(foe.x - 30, 55, 945); hit(foe, blast(4)); debuff(foe, 'weakness', 4, .18); }
        useful = true; break;
      case 'solar-roar':
        if (!target) return false;
        for (const ally of friends.slice(0, 3)) this._status(ally, 'rally', 5, .18);
        for (const foe of near.slice(0, 2)) hit(foe, blast(5));
        useful = true; break;
      case 'boss-vines':
        if (!target) return false;
        for (const foe of near.slice(0, 2)) { hit(foe, blast(4, .4)); debuff(foe, 'root', 1.25, 1); }
        useful = true; break;
      case 'boss-whiteout':
        if (!target) return false;
        for (const foe of near.slice(0, 2)) { hit(foe, blast(4, .4)); debuff(foe, 'freeze', .8, 1); }
        useful = true; break;
      case 'boss-ruin':
        if (!target) return false;
        for (const foe of near.slice(0, 2)) { hit(foe, blast(5, .4)); debuff(foe, 'weakness', 4, .12); }
        useful = true; break;
      default: return false;
    }
    if (!useful) return false;
    u.spellCooldown = spell.cooldown * (u.masteryCooldown || 1);
    u.casting = .45; u.anim = 0; u.spellCasts++;
    this._stat(u, 'casts');
    this._effect(spell.type, u.x, u.lane, u.color, .75, { toX: target?.x ?? u.x, label: spell.name });
    this._emit('troop-spell', { unitId: u.id, name: spell.name, key: u.key, lane: u.lane, x: u.x, side: u.side, classId: u.classId });
    return true;
  }

  _raiseGuard(caster) {
    if (caster.raises >= (caster.trait.raiseLimit || 3) + (caster.mods.raiseLimit || 0) || this.units.length >= 64) return false;
    if (caster.side === 'player' && this.units.filter(u => u.side === 'player' && !u.dead).length >= 40) return false;
    if (caster.side === 'enemy' && this.units.filter(u => u.side === 'enemy' && !u.dead).length >= 24) return false;
    const corpse = this.corpses.find(body => !body.consumed && body.life > 0 && body.lane === caster.lane && Math.abs(body.x - caster.x) < 280);
    if (!corpse) return false;
    corpse.consumed = true;
    const definition = { key: 'boneguard', element: 'void', name: 'Bone Guard', role: 'Undead / temporary guard', color: '#b6b3cb', hp: 64, attack: 10, speed: 35, range: 34, interval: 1.55, armor: .12, spriteAtlas: 'enemies', spriteRow: 5, spriteColumn: 0 };
    const strength = caster.side === 'player' ? Math.sqrt(caster.power || 1) : Math.min(1.8, .9 + this.stage * .012);
    const raised = this._makeUnit(definition, caster.side, caster.lane, strength);
    if (caster.side === 'player') {
      raised.maxHp *= 1 + this.rank('void-drain') * .06; raised.hp = raised.maxHp;
      raised.attack *= 1 + this.rank('void-execute') * .08;
      raised.maxHp *= caster.mods.guardHealth || 1; raised.hp = raised.maxHp;
      raised.attack *= caster.mods.guardAttack || 1;
    }
    raised.classId = 'bone-guard'; raised.undead = true; raised.noCorpse = true; raised.noReward = true;
    raised.raisedBy = caster.id; raised.corpseId = corpse.unitId;
    raised.masteryClassId = caster.classId;
    raised.x = clamp(corpse.x, 65, 935);
    raised.decay = 18 + (caster.side === 'player' && this._hasCombo(caster.lane, 'gravebloom') ? 4 : 0) + (caster.mods.guardDuration || 0);
    this.units.push(raised); this._formationRevision++;
    caster.raises++;
    this._stat(caster, 'raised');
    this._effect('resurrect', raised.x, raised.lane, '#bba2dd', .8, { label: 'Bone Guard' });
    this._emit('spawn', { unit: raised, side: raised.side, key: raised.key, lane: raised.lane });
    return true;
  }

  _kill(target, decay = false, source = null) {
    if (target.dead) return;
    if (!decay && target.side === 'player' && target.trait.rebirth && !target.reborn) {
      target.reborn = true; target.hp = target.maxHp * (target.mods.rebirthHealth || .3); target.casting = .6;
      this._cleanse(target); this._shield(target, 15 + (target.mods.rebirthShield || 0), target);
      this._stat(target, 'casts');
      this._effect('radiance', target.x, target.lane, '#ffc683', 1, { label: 'Rebirth' });
      this._emit('troop-spell', { unitId: target.id, name: 'Rebirth', key: target.key, lane: target.lane, x: target.x, side: target.side, classId: target.classId });
      return;
    }
    target.dead = true; target.hp = 0; this._formationRevision++;
    this._effect('death', target.x, target.lane, target.color, .55);
    if (!decay && !target.undead && !target.noCorpse && !target.boss) {
      this.corpses.push({ unitId: target.id, key: target.key, side: target.side, lane: target.lane, x: target.x, life: 12, consumed: false });
      if (this.corpses.length > 32) this.corpses.shift();
    }
    if (target.side === 'enemy' && !target.noReward && !target.undead && !decay) {
      this.kills++;
      this._stat(source, 'kills');
      this.gold += Math.round((target.boss ? 30 : 4) * (1 + this.rank('bounty') * .2));
    }
    if (target.boss && target.side === 'enemy') { this.enemyBarrier = false; if (this.enemyHp <= 1) this.enemyHp = 0; }
    this._emit('death', { unitId: target.id, x: target.x, lane: target.lane, side: target.side, boss: target.boss, undead: target.undead, decay });
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
    if (source?.statuses?.weakness) amount *= 1 - Math.min(.4, source.statuses.weakness.power);
    if (target.statuses.curse) amount *= 1 + Math.min(.3, target.statuses.curse.power);
    if (target.statuses.fortify) amount *= 1 - Math.min(.3, target.statuses.fortify.power);
    if (source?.side === 'player' && !options.dot) {
      if (source.element === 'storm' && (target.statuses.slow || target.statuses.root || target.statuses.freeze) && this._hasCombo(source.lane, 'shatter')) amount *= 1.25;
      if (source.element === 'void' && target.statuses.burn && this._hasCombo(source.lane, 'soulfire')) amount *= 1.25;
      if (target.statuses.freeze) amount *= source.mods?.frozenDamage || 1;
      if (target.statuses.freeze || target.statuses.root || target.statuses.slow) amount *= source.mods?.controlledDamage || 1;
      if (target.trait.flying) amount *= source.mods?.antiAirPower || 1;
    }
    if (source?.side === 'enemy' && source.statuses?.burn && this._hasCombo(target.lane, 'steamguard')) amount *= .85;
    const armor = clamp(target.armor * (1 - (options.ignoreArmor || 0)), 0, .65);
    if (!options.dot) amount *= 1 - armor;
    if (source?.range > 80 && target.trait.rangedResist) amount *= 1 - target.trait.rangedResist;
    if (source && source.range <= 80 && target.trait.flying && source.element !== 'storm') amount *= .55;
    const prior = target.hp;
    if (target.shield > 0) {
      const shieldFactor = (options.shieldBreak ? 2 : 1) * (source?.mods?.shieldBreakPower || 1);
      const shieldDamage = amount * shieldFactor;
      const absorbed = Math.min(target.shield, shieldDamage);
      target.shield -= absorbed;
      amount -= absorbed / shieldFactor;
      if (!options.dot) this._effect('shield', target.x, target.lane, '#ffe7b0', .25);
    }
    target.hp = Math.max(0, target.hp - Math.max(0, amount));
    this._stat(source, 'damage', Math.max(0, prior - target.hp));
    if (!options.dot) {
      target.hitFlash = .12;
      this._emit('damage', { unitId: target.id, x: target.x, lane: target.lane, amount: prior - target.hp, side: target.side, element, effective: elementMultiplier(element, target.element) > 1 });
    }
    const reflect = Math.min(.25, (target.trait.reflect || 0) + (target.mods.reflectBonus || 0));
    if (source && !source.dead && reflect && !options.reflected && !options.dot && source.range <= 80) this._hit(target, source, Math.max(0, prior - target.hp) * reflect, { reflected: true });
    if (target.hp <= 0) this._kill(target, false, source);
    return Math.max(0, prior - target.hp);
  }

  _heal(u, amount, silent = false, source = null) {
    if (u.dead || amount <= 0) return;
    if (u.side === 'player') {
      if (this._hasCombo(u.lane, 'verdant-covenant')) amount *= 1.2;
      if (u.undead && !this._hasCombo(u.lane, 'gravebloom')) amount *= .5;
    }
    const healed = Math.min(u.maxHp - u.hp, amount);
    if (healed <= 0) return;
    u.hp += healed;
    this._stat(source, 'healing', healed);
    if (!silent || amount >= 2) this._effect('heal', u.x, u.lane, '#85d7a2', .45);
    if (!silent) this._emit('heal', { unitId: u.id, x: u.x, lane: u.lane, amount: healed });
  }

  _effect(type, x, lane, color, life, extra = {}) {
    this.effects.push({ type, x, lane, color, life, maxLife: life, age: 0, ...extra });
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
