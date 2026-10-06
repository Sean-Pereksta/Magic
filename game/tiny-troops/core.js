/* Shared Tiny Troops rules. No DOM, timers, or network access. */
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.TinyTroopsRules = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';
  const VERSION = 3;
  const MODES = ['MENU', 'PLAYING', 'PAUSED', 'VICTORY', 'DEFEAT', 'RESULTS'];
  const PHASES = ['recruit', 'shop', 'relic', 'battle', 'evolve', 'ascend', 'lateStarChoice', 'ultimate10', 'tt-event', 'tt-path', 'dead', 'won'];
  const numeric = (v, fallback = 0, min = 0, max = 1e12) => Number.isFinite(Number(v)) ? Math.min(max, Math.max(min, Number(v))) : fallback;
  const copy = v => JSON.parse(JSON.stringify(v));
  const ROLE_INFO = {
    ARMORED: { emoji: '🛡️', name: 'Defender', strong: 'Physical hits and fast attackers', weak: 'Magic and shield breakers', partner: 'Ranged troops and healers', tip: 'Hold the right column to shelter fragile troops.' },
    RANGED: { emoji: '🏹', name: 'Ranged', strong: 'Slow melee enemies', weak: 'Fast attackers and backline hunters', partner: 'Defenders in the same row', tip: 'Use the left columns and build Volley.' },
    FAST: { emoji: '🐎', name: 'Fast striker', strong: 'Exposed ranged enemies', weak: 'Armored defenders', partner: 'Other fast troops and control', tip: 'Focus dangerous ranged enemies; speed means attack tempo.' },
    SUPPORT: { emoji: '🎺', name: 'Support', strong: 'Long fights and mixed armies', weak: 'Backline pressure; low solo damage', partner: 'Adjacent allies and durable frontliners', tip: 'Check its aura pattern before placing it.' },
    MAGIC: { emoji: '✨', name: 'Caster', strong: 'Armored enemies and groups', weak: 'Silence and fragile-target hunters', partner: 'Element specialists and defenders', tip: 'Match spell types to the enemy weaknesses.' },
    MELEE: { emoji: '⚔️', name: 'Melee', strong: 'Close formations and wounded targets', weak: 'Control and ranged pressure', partner: 'Commanders and Frost', tip: 'Link neighboring melee troops for Warband.' }
  };
  const ELEMENTS = { Fire: 'FIRE', Frost: 'FROST', Storm: 'LIGHTNING', Healer: 'HEALER', Machine: 'MACHINE', Shadow: 'SHADOW', Nature: 'NATURE', Blood: 'BLOOD', Holy: 'HOLY' };
  const rangedNames = /archer|sniper|marksman|gunner|crossbow|mortar|cannon|drone|javelin|boomerang/i;
  const assassinNames = /rogue|ninja|assassin|ronin|blader|dueler|reaper|shadow cat|duelist/i;
  const tagMemo = new WeakMap();
  function tags(u) {
    if (!u) return [];
    const memo = tagMemo.get(u), native = Array.isArray(u.t) ? u.t : [], custom = Array.isArray(u.tags) ? u.tags : [];
    const speed = u.baseSpd || u.spd, attack = u.team === 'enemy' ? 0 : u.baseAtk || u.atk, health = u.team === 'enemy' ? 0 : u.baseMaxHp || u.hp;
    if (memo && memo.n === u.n && memo.team === u.team && memo.kind === u.kind && memo.focus === u.focus && memo.armor === u.armor && memo.protector === u.protector && memo.banner === u.banner && memo.cleanser === u.cleanser && memo.summonBuff === u.summonBuff && memo.repairMachine === u.repairMachine && memo.attack === u.attack && memo.speed === speed && memo.damage === attack && memo.health === health && memo.star === u.star && memo.boss === u.boss && memo.elite === u.elite && memo.summoned === u.summoned && memo.path === u.ttPath && memo.native.length === native.length && native.every((t, i) => t === memo.native[i]) && memo.custom.length === custom.length && custom.every((t, i) => t === memo.custom[i])) return memo.result;
    const out = new Set(Array.isArray(u.tags) ? u.tags : []);
    native.forEach(t => { if (ELEMENTS[t]) out.add(ELEMENTS[t]); });
    if (u.team === 'enemy') {
      out.add(u.kind === 'ranged' ? 'RANGED' : 'MELEE');
      if (u.armor > 0 || u.protector || u.focus === 'armor' || /guard|sentinel|wall|bulwark|shield|knight|golem|tank|ogre/i.test(u.n || '')) out.add('ARMORED');
      if (numeric(u.spd) >= 1.3 || /diver|stalker|assassin|rider/i.test(u.n || '')) out.add('FAST');
      if (u.banner || u.cleanser || u.summonBuff || u.repairMachine || u.focus === 'support') out.add('SUPPORT');
      if (u.attack && !['hit', 'mark', 'thorn'].includes(u.attack)) out.add('MAGIC');
      const elem = { fire: 'FIRE', frost: 'FROST', storm: 'LIGHTNING', shadow: 'SHADOW', poison: 'POISON' }[u.attack];
      if (elem) out.add(elem);
    } else {
      if (native.includes('Guard') || native.includes('Stone')) out.add('ARMORED');
      if (native.includes('Mage')) out.add('MAGIC');
      if (u.focus === 'support' || native.includes('Healer')) out.add('SUPPORT');
      if (native.includes('Royal') && (u.focus === 'support' || /captain|banner|commander|tactician|standard|drummer/i.test(u.n || ''))) out.add('COMMANDER');
      if (/lancer|rider|cavalry|griffin/i.test(u.n || '')) { out.add('MOUNTED'); out.add('FAST'); }
      if (native.includes('Beast') && numeric(u.baseSpd || u.spd) >= 1.3 || assassinNames.test(u.n || '')) out.add('FAST');
      if (rangedNames.test(u.n || '') || native.includes('Mage') && u.focus !== 'support') out.add('RANGED');
      else if (u.focus !== 'support' && !native.includes('Healer')) out.add('MELEE');
      if (numeric(u.baseMaxHp || u.hp) <= 50 && numeric(u.baseAtk || u.atk) <= 20 && !out.has('SUPPORT')) out.add('SWARM');
    }
    if (native.includes('Poison')) out.add('POISON');
    if (u.boss || u.elite || u.team !== 'enemy' && (numeric(u.star, 1) >= 5 || numeric(u.baseAtk || u.atk) >= 28)) out.add('ELITE');
    if (u.summoned) out.add('SUMMONED');
    if (numeric(u.baseSpd || u.spd) <= .85) out.add('SLOW');
    const path = PATH_BY_ID[u.ttPath];
    (path?.tags || []).forEach(t => out.add(t));
    const result = Object.freeze([...out]);
    tagMemo.set(u, { n: u.n, team: u.team, kind: u.kind, focus: u.focus, armor: u.armor, protector: u.protector, banner: u.banner, cleanser: u.cleanser, summonBuff: u.summonBuff, repairMachine: u.repairMachine, attack: u.attack, speed, damage: attack, health, star: u.star, boss: u.boss, elite: u.elite, summoned: u.summoned, path: u.ttPath, native: native.slice(), custom: custom.slice(), result });
    return result;
  }
  const has = (u, ...wanted) => wanted.some(t => tags(u).includes(t));
  function role(u) {
    const list = tags(u);
    return ['SUPPORT', 'ARMORED', 'MAGIC', 'FAST', 'RANGED', 'MELEE'].find(t => list.includes(t)) || 'MELEE';
  }
  function countTags(squad, aliveOnly = false) {
    const out = {};
    squad.forEach(u => { if (u && (!aliveOnly || !u.dead && u.hp > 0)) tags(u).forEach(t => out[t] = (out[t] || 0) + 1); });
    return out;
  }
  const SYNERGIES = [
    { id: 'shieldwall', name: 'Shield Wall', emoji: '🛡️', tag: 'ARMORED', tiers: [3, 6, 9], bonus: '+5/8/11% damage resistance; +8/16/24 starting shield for defenders.', mods: { damageDR: [.05, .08, .11], shield: [8, 16, 24] } },
    { id: 'volley', name: 'Volley', emoji: '🏹', tag: 'RANGED', tiers: [3, 6, 9], bonus: '+8/14/20% attack tempo for ranged troops.', mods: { spdMult: [.08, .14, .20] } },
    { id: 'charge', name: 'Opening Charge', emoji: '🐎', tag: 'FAST', tiers: [2, 4, 6], bonus: '+20/30/40% first-hit damage for fast attackers. No movement required.', mods: { opening: [.20, .30, .40] } },
    { id: 'warband', name: 'Warband', emoji: '⚔️', tag: 'MELEE', tiers: [3, 6, 9], bonus: '+6/10/14% damage for melee troops with an adjacent melee ally.', mods: { adjacentAtk: [.06, .10, .14] } },
    { id: 'hospital', name: 'Field Hospital', emoji: '💚', req: { HEALER: 1, ARMORED: 2 }, bonus: '+18% healing from healers into defenders.', mods: { healArmored: .18 } },
    { id: 'command', name: 'Command Formation', emoji: '🎺', req: { COMMANDER: 1 }, distinctRoles: 3, bonus: '+6% squad damage with a commander and three different roles.', mods: { teamAtk: .06 } },
    { id: 'frostedge', name: 'Frost Edge', emoji: '❄️⚔️', req: { FROST: 2, MELEE: 3 }, bonus: '+12% melee damage against chilled enemies.', mods: { chilledAtk: .12 } },
    { id: 'wildfire', name: 'Wildfire', emoji: '🔥🐾', req: { FIRE: 2, FAST: 2 }, bonus: 'Fire hits add 1 extra burn stack. Fast allies deal +8% damage to burning targets.', mods: { burn: 1, burningAtk: .08 } },
    { id: 'chain', name: 'Storm Battery', emoji: '⚡🏹', req: { LIGHTNING: 2, RANGED: 2 }, bonus: '+8% Storm damage; +8 percentage points of chain chance, under existing chain caps.', mods: { stormAtk: .08, chain: .08 } }
  ];
  function synergies(squad, aliveOnly = false) {
    const counts = countTags(squad, aliveOnly);
    const units = squad.filter(u => u && (!aliveOnly || !u.dead && u.hp > 0));
    const roles = new Set(units.map(role));
    return SYNERGIES.map(d => {
      const count = d.tag ? counts[d.tag] || 0 : 0;
      const tier = d.tag ? d.tiers.filter(n => count >= n).length : Object.entries(d.req).every(([t, n]) => (counts[t] || 0) >= n) && (!d.distinctRoles || roles.size >= d.distinctRoles) ? 1 : 0;
      const missing = d.tag ? [[d.tag, Math.max(0, (d.tiers[tier] || d.tiers.at(-1)) - count)]] : Object.entries(d.req).map(([t, n]) => [t, Math.max(0, n - (counts[t] || 0))]);
      const mods = Object.fromEntries(Object.entries(d.mods).map(([k, v]) => [k, tier ? Array.isArray(v) ? v[tier - 1] : v : 0]));
      return { ...d, count, tier, next: d.tag ? d.tiers[tier] || null : null, missing, roles: roles.size, mods };
    });
  }
  function protectedBy(squad, index, aliveOnly = false) {
    const u = squad[index];
    if (!u || !has(u, 'RANGED', 'SUPPORT')) return [];
    const row = Math.floor(index / 4), column = index % 4;
    return squad.filter((a, i) => a && (!aliveOnly || !a.dead && a.hp > 0) && Math.floor(i / 4) === row && i % 4 > column && has(a, 'ARMORED'));
  }
  function recruitImpact(squad, recruit) {
    const before = synergies(squad), after = synergies([...squad, recruit]);
    const activated = after.filter((s, i) => s.tier > before[i].tier);
    if (activated.length) return activated.slice(0, 2).map(s => 'Activates ' + s.name + (s.tier > 1 ? ' ' + s.tier : '')).join(' • ');
    const progress = after.filter((s, i) => s.tag && s.count > before[i].count && s.next).sort((a, b) => (a.next - a.count) - (b.next - b.count));
    if (progress.length) return progress.slice(0, 2).map(s => s.name + ' ' + s.count + '/' + s.next).join(' • ');
    return 'Adds ' + ROLE_INFO[role(recruit)].name.toLowerCase() + ' options';
  }
  const COUNTERS = [
    { from: 'FAST', to: 'RANGED', mult: 1.15, label: 'Fast → Ranged: +15% damage' },
    { from: 'MAGIC', to: 'ARMORED', mult: 1.12, label: 'Magic → Armored: +12% damage' },
    { from: 'FAST', to: 'ARMORED', mult: .90, label: 'Armored defenders resist fast attacks by 10%' },
    { from: 'RANGED', to: 'SLOW', mult: 1.12, label: 'Ranged → Slow: +12% damage' }
  ];
  function counterMultiplier(src, target, type) {
    if (!src || !target || ['burn', 'poison', 'thorn', 'reflect'].includes(type)) return 1;
    return COUNTERS.reduce((n, c) => n * (has(src, c.from) && has(target, c.to) ? c.mult : 1), 1);
  }
  const PATHS = [
    { id: 'precision', name: 'Precision Shot', emoji: '🎯', role: 'RANGED', rarity: 'common', desc: '+20% damage, −12% attack tempo.', mods: { atkMult: .20, spdMult: -.12 } },
    { id: 'rapid', name: 'Rapid Fire', emoji: '🏹', role: 'RANGED', rarity: 'common', desc: '+22% attack tempo, −8% damage.', mods: { spdMult: .22, atkMult: -.08 } },
    { id: 'flaming', name: 'Flaming Shots', emoji: '🔥', role: 'RANGED', rarity: 'rare', tags: ['FIRE'], desc: 'Hits add 2 burn stacks and count toward Fire role synergies.', mods: { burn: 2 } },
    { id: 'tower', name: 'Tower Shield', emoji: '🛡️', role: 'ARMORED', rarity: 'common', desc: '+20 starting shield; 18% less damage from ranged enemies.', mods: { shield: 20, rangedDR: .18 } },
    { id: 'protector', name: 'Protector', emoji: '💠', role: 'ARMORED', rarity: 'uncommon', desc: 'Attacks grant 5 shield to the weakest adjacent ally.', mods: { allyShield: 5 } },
    { id: 'spikes', name: 'Spiked Armor', emoji: '🌵', role: 'ARMORED', rarity: 'rare', desc: '+10 starting shield; +8 percentage points of nonrecursive thorns.', mods: { shield: 10, thorns: .08 } },
    { id: 'momentum', name: 'Momentum', emoji: '🐎', role: 'FAST', rarity: 'common', desc: '+35% first-hit damage; +5% attack tempo.', mods: { opening: .35, spdMult: .05 } },
    { id: 'venom', name: 'Venom Edge', emoji: '☠️', role: 'FAST', rarity: 'uncommon', desc: 'Hits add 2 poison stacks. Counts toward Poison role synergies.', tags: ['POISON'], mods: { poison: 2 } },
    { id: 'hunter', name: 'Hunter', emoji: '🎯', role: 'FAST', rarity: 'rare', desc: '+18% damage against ranged and support enemies.', mods: { hunterAtk: .18 } },
    { id: 'triage', name: 'Triage', emoji: '💚', role: 'SUPPORT', rarity: 'common', desc: '+20% healing into allies below half health.', mods: { triageHeal: .20 } },
    { id: 'shelter', name: 'Sheltering Aura', emoji: '💠', role: 'SUPPORT', rarity: 'uncommon', desc: 'Acts grant 6 shield to the weakest adjacent ally.', mods: { allyShield: 6 } },
    { id: 'tempo', name: 'Battle Rhythm', emoji: '🎺', role: 'SUPPORT', rarity: 'rare', desc: '+16% own attack tempo; +8 starting shield to same-row allies.', mods: { spdMult: .16, rowShield: 8 } },
    { id: 'channel', name: 'Focused Channel', emoji: '✨', role: 'MAGIC', rarity: 'common', desc: '+18% damage, −8% attack tempo.', mods: { atkMult: .18, spdMult: -.08 } },
    { id: 'frostpath', name: 'Winter Spell', emoji: '❄️', role: 'MAGIC', rarity: 'uncommon', tags: ['FROST'], desc: 'Hits add 0.06 chill, capped at 0.6 total slow.', mods: { slow: .06 } },
    { id: 'stormpath', name: 'Chain Conduit', emoji: '⚡', role: 'MAGIC', rarity: 'rare', tags: ['LIGHTNING'], desc: '+12 percentage points of lightning chain chance under existing caps.', mods: { chain: .12 } },
    { id: 'duelist', name: 'Duelist Training', emoji: '⚔️', role: 'MELEE', rarity: 'common', desc: '+14% damage; −5% attack tempo.', mods: { atkMult: .14, spdMult: -.05 } },
    { id: 'frenzy', name: 'Last Stand', emoji: '🪓', role: 'MELEE', rarity: 'uncommon', desc: 'Up to +22% damage as health falls.', mods: { woundedAtk: .22 } },
    { id: 'lifedrinker', name: 'Lifedrinker', emoji: '🩸', role: 'MELEE', rarity: 'rare', tags: ['BLOOD'], desc: '+8 percentage points of lifesteal; +5% attack tempo.', mods: { leech: .08, spdMult: .05 } }
  ];
  const PATH_BY_ID = Object.fromEntries(PATHS.map(p => [p.id, p]));
  const NO_MODS = Object.freeze({});
  const pathsFor = u => PATHS.filter(p => p.role === role(u)).slice(0, 3);
  const pathMods = u => PATH_BY_ID[u?.ttPath]?.mods || NO_MODS;
  const STARTERS = [
    { id: 'defender', name: 'The Defender', emoji: '🛡️', desc: 'Frontline, protection, and sustain.', tags: ['ARMORED', 'HEALER'], units: ['Squire', 'Knight', 'Archer'] },
    { id: 'ranger', name: 'The Ranger', emoji: '🏹', desc: 'Protected ranged damage and focus fire.', tags: ['RANGED'], units: ['Archer', 'Gunner', 'Squire'] },
    { id: 'warlord', name: 'The Warlord', emoji: '⚔️', desc: 'Melee formations and aggressive carries.', tags: ['MELEE', 'BLOOD'], units: ['Berserker', 'Samurai', 'Squire'] },
    { id: 'arcane', name: 'The Arcanist', emoji: '✨', desc: 'Status combinations and spell support.', tags: ['MAGIC', 'FIRE', 'FROST', 'LIGHTNING'], units: ['Wizard', 'Pyro', 'Frost Archer'], unlock: 'boss' },
    { id: 'riders', name: 'The Pack Leader', emoji: '🐎', desc: 'Fast attacks, hunters, and opening bursts.', tags: ['FAST', 'MOUNTED'], units: ['Lancer', 'Wolf', 'Harpy'], unlock: 'run' },
    { id: 'random', name: 'Surprise Me', emoji: '🎲', desc: 'Discover a build as you draft.', tags: [], units: [] }
  ];
  const MODIFIERS = [
    { id: 'standard', name: 'Open Campaign', emoji: '⚔️', desc: 'Standard recruiting and combat rules.' },
    { id: 'swarm', name: 'Swarm Season', emoji: '🐾', desc: 'Small, light troops appear more often.' },
    { id: 'heroes', name: 'Age of Heroes', emoji: '🌟', desc: 'Elite recruits and duplicate training appear more often.' },
    { id: 'frozen', name: 'Frozen Field', emoji: '❄️', desc: 'Applied chill is 20% stronger for both sides, capped at 0.6.' },
    { id: 'economy', name: 'War Economy', emoji: '🪙', desc: 'Store costs +15%; enemy coin rewards +20%.' },
    { id: 'chaos', name: 'Chaos Draft', emoji: '🌀', desc: 'Element effects appear more often; draft across several roles.', unlock: 'run' }
  ];
  const ARCHETYPES = [
    { id: 'balanced', name: 'Royal Company', emoji: '👑', prefer: [], counter: 'Mix damage, defense, and sustain.' },
    { id: 'wall', name: 'The Iron Wall', emoji: '🛡️', prefer: ['ARMORED'], counter: 'Bring magic or armor breakers.' },
    { id: 'volley', name: 'The Volley', emoji: '🏹', prefer: ['RANGED'], counter: 'Shield fragile allies or use fast hunters.' },
    { id: 'riders', name: 'The Hunters', emoji: '🐎', prefer: ['FAST'], counter: 'Use durable frontliners and chill.' },
    { id: 'cult', name: 'Elemental Cult', emoji: '🔥', prefer: ['MAGIC'], counter: 'Exploit elemental weaknesses; cleanse statuses.' },
    { id: 'horde', name: 'The Horde', emoji: '⚔️', prefer: ['MELEE'], counter: 'Use splash, thorns, and sustained healing.' }
  ];
  const EVENTS = [
    { id: 'armory', name: 'Abandoned Armory', emoji: '🔨', desc: 'A supply cart holds two kinds of equipment.', options: [{ id: 'shield', name: 'Reinforce defenders', desc: '+8 starting shield for armored troops.', bonus: { tag: 'ARMORED', shield: 8 } }, { id: 'blades', name: 'Sharpen weapons', desc: '+5% damage for melee troops.', bonus: { tag: 'MELEE', atkMult: .05 } }, { id: 'supplies', name: 'Take supplies', desc: '+18 coins.', coins: 18 }] },
    { id: 'shrine', name: 'Elemental Shrine', emoji: '🔮', desc: 'Choose one blessing for this run.', options: [{ id: 'ember', name: 'Ember blessing', desc: '+1 burn stack on Fire hits.', bonus: { tag: 'FIRE', burn: 1 } }, { id: 'winter', name: 'Winter blessing', desc: '+8 starting shield for Frost troops.', bonus: { tag: 'FROST', shield: 8 } }, { id: 'supplies', name: 'Take offerings', desc: '+18 coins.', coins: 18 }] },
    { id: 'hospital', name: 'Field Infirmary', emoji: '💚', desc: 'The field medics offer their expertise.', options: [{ id: 'medics', name: 'Study triage', desc: '+10% healing from healers.', bonus: { tag: 'HEALER', healMult: .10 } }, { id: 'tempo', name: 'Study battle rhythm', desc: '+5% attack tempo for support troops.', bonus: { tag: 'SUPPORT', spdMult: .05 } }, { id: 'supplies', name: 'Take supplies', desc: '+18 coins.', coins: 18 }] }
  ];
  const OBJECTIVES = [
    { id: 'protect', name: 'Protect the Volley', desc: 'Win without a ranged troop falling.', reward: 20 },
    { id: 'melee', name: 'Close Combat', desc: 'Score 3 enemy kills with melee troops.', reward: 20, target: 3 },
    { id: 'tempo', name: 'Quick Victory', desc: 'Win within 40 simulation steps (10 seconds).', reward: 20, target: 40 }
  ];
  function nextRandom(run) {
    let x = numeric(run.rng, 1, 1, 0xffffffff) >>> 0;
    x ^= x << 13; x ^= x >>> 17; x ^= x << 5;
    run.rng = (x >>> 0) || 1;
    return run.rng / 4294967296;
  }
  function weighted(items, weight, random) {
    if (!items.length) return null;
    const weights = items.map(x => Math.max(.01, numeric(weight(x), 1, .01, 100)));
    let r = random() * weights.reduce((a, b) => a + b, 0);
    for (let i = 0; i < items.length; i++) { r -= weights[i]; if (r <= 0) return items[i]; }
    return items.at(-1);
  }
  function newRun(config = {}, seed = 1) {
    const starter = STARTERS.some(s => s.id === config.starter) ? config.starter : 'defender';
    const modifier = MODIFIERS.some(m => m.id === config.modifier) ? config.modifier : 'standard';
    return { version: VERSION, id: 'tt-' + Date.now() + '-' + seed, rng: Math.max(1, seed >>> 0), starter, modifier, difficulty: ['relaxed', 'standard', 'tactician'].includes(config.difficulty) ? config.difficulty : 'standard', mode: 'PLAYING', stats: {}, seenSynergies: {}, eventBonuses: [], event: null, lastEventRound: 0, wavePlan: null, objective: null, outcome: null, finished: false, steps: 0, bestHit: 0, checkpoint: null };
  }
  function armyStyle(squad) {
    const c = countTags(squad), n = squad.filter(Boolean).length;
    if ((c.ARMORED || 0) >= 3 && (c.RANGED || 0) >= 3) return 'Fortified Volley';
    if ((c.MAGIC || 0) >= 3 && ['FIRE', 'FROST', 'LIGHTNING', 'POISON'].filter(t => c[t]).length >= 2) return 'Elemental Circle';
    if ((c.FAST || 0) >= 3) return 'Lightning Warband';
    if ((c.SUPPORT || 0) >= 3) return 'Living Fortress';
    if (n >= 8 && (c.SWARM || 0) >= n * .5) return 'Emoji Horde';
    if (squad.filter(u => u && u.star >= 4).length >= 2 && n <= 6) return 'Elite Guard';
    if ((c.BLOOD || 0) >= 2) return 'Berserker Company';
    if ((c.RANGED || 0) >= 3) return 'Arrowstorm';
    if ((c.ARMORED || 0) >= 3) return 'Iron Fortress';
    return 'Mixed Company';
  }
  function sanitizeUnit(value, roster = []) {
    if (!value || typeof value !== 'object' || Array.isArray(value) || typeof value.n !== 'string') return null;
    const def = roster.find(h => h.n === value.n);
    if (!def && (!value.e || !Array.isArray(value.t))) return null;
    const u = { ...(def ? copy(def) : {}), ...copy(value) };
    u.n = u.n.slice(0, 100); u.e = String(u.e || '⚔️').slice(0, 24);
    u.t = [...new Set((Array.isArray(u.t) ? u.t : def?.t || ['Blade']).filter(t => typeof t === 'string'))].slice(0, 10);
    u.team = 'ally'; u.star = Math.floor(numeric(u.star, 1, 1, 13));
    u.baseMaxHp = numeric(u.baseMaxHp || u.maxHp || u.hp, def?.hp || 40, 1);
    u.baseAtk = numeric(u.baseAtk ?? u.atk, def?.atk || 8, 0);
    u.baseSpd = numeric(u.baseSpd || u.spd, def?.spd || 1, .1, 20);
    u.maxHp = u.baseMaxHp; u.hp = u.maxHp; u.atk = u.baseAtk; u.spd = u.baseSpd;
    u.shield = numeric(u.shield, 0); u.cd = 0; u.dead = false;
    ['burn', 'poison', 'slow', 'marked', 'silenced', 'stun', 'rooted', 'stagger', 'weakened', 'delay', 'hex', 'healCut', 'healDown', 'goldShield', 'fractured', 'riftTouched', 'fragile', 'cursed', 'suppressed'].forEach(k => u[k] = 0);
    ['lastHit', 'lastAttacker', 'target', 'attacker', 'victim', 'source', 'owner', 'parent', 'linked', 'link', 'from', 'to', '_el', 'el'].forEach(k => delete u[k]);
    u.kills = Math.floor(numeric(u.kills)); u.starProg = numeric(u.starProg);
    u.fx = (Array.isArray(u.fx) ? u.fx : []).filter(f => f && typeof f.id === 'string').slice(0, 30);
    if (!PATHS.some(p => p.id === u.ttPath)) delete u.ttPath;
    return u;
  }
  function sanitizeSnapshot(input, roster = []) {
    if (!input || typeof input !== 'object' || Array.isArray(input)) throw new Error('Saved run is missing.');
    let snap = copy(input);
    if (snap.tt?.checkpoint?.state && ['battle', 'paused'].includes(snap.phase)) snap = copy(snap.tt.checkpoint.state);
    snap.schema = VERSION; snap.saveVersion = 'tiny_troops_v1';
    ['round', 'points', 'coins', 'kills', '_battleSeq'].forEach(k => snap[k] = Math.floor(numeric(snap[k], k === 'round' ? 1 : 0, k === 'round' ? 1 : 0)));
    const slots = snap.round >= 250 || snap.boardRowsUnlocked || (Array.isArray(snap.squad) && snap.squad.slice(16, 20).some(Boolean)) ? 20 : 16;
    snap.boardRowsUnlocked = slots === 20;
    snap.squad = Array.from({ length: slots }, (_, i) => sanitizeUnit(Array.isArray(snap.squad) ? snap.squad[i] : null, roster));
    snap.bench = (Array.isArray(snap.bench) ? snap.bench : []).map(u => sanitizeUnit(u, roster)).filter(Boolean).slice(0, 3);
    const baseBoost = { hp: 0, atk: 0, spd: 0, shield: 0, coins: 0, poison: 0, thorns: 0, revive: false, revivePower: .35, frontShield: 0, tb: {} };
    snap.boost = { ...baseBoost, ...(snap.boost && typeof snap.boost === 'object' && !Array.isArray(snap.boost) ? snap.boost : {}) };
    Object.keys(baseBoost).filter(k => typeof baseBoost[k] === 'number').forEach(k => snap.boost[k] = numeric(snap.boost[k], baseBoost[k]));
    if (!snap.boost.tb || typeof snap.boost.tb !== 'object' || Array.isArray(snap.boost.tb)) snap.boost.tb = {};
    ['upgrades', 'abilities', 'abilityTimers', 'spellMods', '_seenMixed', '_offeredHeroes'].forEach(k => { if (!snap[k] || typeof snap[k] !== 'object' || Array.isArray(snap[k])) snap[k] = {}; });
    ['upgrades', 'abilities'].forEach(k => Object.keys(snap[k]).forEach(id => snap[k][id] = Math.floor(numeric(snap[k][id], 0, 0, 100000))));
    ['relics', 'log', 'combos', 'combatCauses', 'pendingStarChoices'].forEach(k => { if (!Array.isArray(snap[k])) snap[k] = []; snap[k] = snap[k].slice(-100); });
    const savedRun = snap.tt && typeof snap.tt === 'object' && !Array.isArray(snap.tt) ? snap.tt : {};
    // Every run is endless; obsolete finite-run settings are discarded on migration.
    const defaults = newRun(savedRun, savedRun.rng || 1);
    snap.tt = { ...defaults, ...savedRun, version: VERSION, starter: defaults.starter, modifier: defaults.modifier, difficulty: defaults.difficulty, checkpoint: null };
    if (!MODES.includes(snap.tt.mode)) snap.tt.mode = 'PLAYING';
    delete snap.tt.limit;
    if (!snap.tt.stats || typeof snap.tt.stats !== 'object' || Array.isArray(snap.tt.stats)) snap.tt.stats = {};
    Object.keys(snap.tt.stats).forEach(id => { const s = snap.tt.stats[id]; if (!s || typeof s !== 'object') { delete snap.tt.stats[id]; return; } ['damage', 'heal', 'shield', 'kills'].forEach(k => s[k] = numeric(s[k])); s.n = String(s.n || 'Troop').slice(0, 100); s.e = String(s.e || '⚔️').slice(0, 24); });
    if (!snap.tt.seenSynergies || typeof snap.tt.seenSynergies !== 'object') snap.tt.seenSynergies = {};
    snap.tt.eventBonuses = (Array.isArray(snap.tt.eventBonuses) ? snap.tt.eventBonuses : []).filter(b => b && typeof b.tag === 'string').slice(0, 80);
    if (snap.tt.event && !EVENTS.some(e => e.id === snap.tt.event.id)) snap.tt.event = null;
    if (snap.tt.wavePlan && (!Array.isArray(snap.tt.wavePlan.enemies) || snap.tt.wavePlan.enemies.length > 40)) snap.tt.wavePlan = null;
    if (snap.tt.wavePlan?.enemies.some(e => !e || typeof e.n !== 'string' || !Number.isFinite(e.maxHp) || e.maxHp <= 0 || !Number.isFinite(e.atk) || e.atk < 0 || !Number.isFinite(e.spd) || e.spd <= 0)) snap.tt.wavePlan = null;
    snap.tt.steps = Math.floor(numeric(snap.tt.steps)); snap.tt.bestHit = numeric(snap.tt.bestHit); snap.tt.nextUnitId = Math.floor(numeric(snap.tt.nextUnitId));
    if (snap.tt.basicCombatRound !== snap.round) delete snap.tt.basicCombatRound;
    snap.tt.draft = (Array.isArray(snap.tt.draft) ? snap.tt.draft : []).slice(0, 3).map(c => { if (!c || !['recruit', 'effect', 'upgrade', 'tt-path'].includes(c.type)) return null; if (c.type === 'recruit') { c.unit = sanitizeUnit(c.unit, roster); if (!c.unit) return null; } return c; }).filter(Boolean);
    snap.runEnded = !!snap.runEnded || ['dead', 'won'].includes(snap.phase) || !!snap.tt.finished;
    snap.phase = snap.runEnded ? snap.tt.outcome === 'victory' || snap.phase === 'won' ? 'won' : 'dead' : snap.tt.event ? 'tt-event' : snap.phase === 'relic' ? 'relic' : snap.shopOpen && snap.phase === 'shop' ? 'shop' : 'recruit';
    snap.battle = null; snap.enemies = []; snap.inspect = null; snap.comboCells = [];
    ['placing', 'placingEffect', 'placingSpecialization', 'evoTarget', 'ascTarget', 'starChoiceTarget', 'ultimate10Target'].forEach(k => snap[k] = null);
    snap.placingUpgrade = false;
    ['choices', 'items', 'relicChoices', 'evoChoices', 'ascChoices', 'starChoiceChoices', 'ultimate10Choices'].forEach(k => snap[k] = []);
    return snap;
  }
  // A recovery battle deliberately avoids native spell/ability callbacks. It still
  // fights the same enemies; victory and defeat are decided by actual HP loss.
  function basicBonus(state, unit, key) {
    return numeric(state.boost?.[key]) + (Array.isArray(unit.t) ? unit.t : []).reduce((total, tag) => total + numeric(state.boost?.tb?.[tag]?.[key]), 0);
  }
  function initializeBasicBattle(state) {
    if (!Array.isArray(state.tt?.wavePlan?.enemies) || !state.tt.wavePlan.enemies.length) throw new Error('The enemy checkpoint is missing. Restore preparation to rebuild the wave.');
    state.enemies = copy(state.tt.wavePlan.enemies);
    state.enemies.forEach(u => { u.maxHp = numeric(u.maxHp || u.hp, 1, 1); u.hp = u.maxHp; u.atk = numeric(u.atk, 1, 1); u.spd = numeric(u.spd, 1, .1, 20); u.shield = numeric(u.shield); u.cd = 0; u.dead = false; });
    state.squad.filter(Boolean).forEach((u) => {
      u.maxHp = numeric(numeric(u.baseMaxHp || u.maxHp, 1, 1) + basicBonus(state, u, 'hp'), 1, 1);
      u.hp = u.maxHp; u.atk = numeric(numeric(u.baseAtk ?? u.atk, 1) + basicBonus(state, u, 'atk'), 1, 1);
      u.spd = numeric(numeric(u.baseSpd || u.spd, 1, .1, 20) + basicBonus(state, u, 'spd'), 1, .1, 20);
      u.shield = numeric(u.shield) + basicBonus(state, u, 'shield') + (state.squad.indexOf(u) % 4 === 3 ? numeric(state.boost?.frontShield) : 0);
      u.dead = false; u.cd = 0;
      ['burn', 'poison', 'slow', 'stun', 'marked'].forEach(k => u[k] = 0);
    });
    state._battleSeq = Math.floor(numeric(state._battleSeq)) + 1;
    state.battle = { id: state._battleSeq, tick: 0, simTime: 0, basic: true };
    state.phase = 'battle'; state.tt.objective = null;
  }
  function stepBasicBattle(state) {
    const living = list => list.filter(u => u && !u.dead && numeric(u.hp) > 0);
    const events = [], allies = () => living(state.squad), enemies = () => living(state.enemies);
    state.battle.tick++; state.battle.simTime = state.battle.tick * .25; state.tt.steps = Math.floor(numeric(state.tt.steps)) + 1;
    function targetFor(source, list, attackingAllies) {
      let targets = list.slice();
      if (attackingAllies && source.kind !== 'ranged') {
        const front = Math.max(...targets.map(u => state.squad.indexOf(u) % 4));
        targets = targets.filter(u => state.squad.indexOf(u) % 4 === front);
      }
      if (source.focus === 'weak') targets.sort((a, b) => a.hp / a.maxHp - b.hp / b.maxHp);
      else if (source.focus === 'strong') targets.sort((a, b) => b.atk - a.atk);
      else if (source.focus === 'armor') targets.sort((a, b) => numeric(b.shield) - numeric(a.shield));
      else if (source.focus === 'back' && attackingAllies) targets.sort((a, b) => state.squad.indexOf(a) % 4 - state.squad.indexOf(b) % 4);
      return targets[0];
    }
    function attack(source, target, amount) {
      if (!target) return;
      amount = numeric(amount, 1, 1); target.shield = numeric(target.shield);
      const blocked = Math.min(target.shield, amount); target.shield -= blocked;
      const before = numeric(target.hp); target.hp = Math.max(0, before - amount + blocked);
      const killed = target.hp === 0; if (killed) target.dead = true;
      events.push({ source, target, damage: before - target.hp + blocked, blocked, killed });
    }
    for (const u of allies()) {
      if (!enemies().length) break;
      u.cd = numeric(u.cd, 0, -100) - .25;
      if (u.cd > 0) continue;
      if (has(u, 'HEALER') || u.focus === 'support') {
        const weakest = allies().sort((a, b) => a.hp / a.maxHp - b.hp / b.maxHp)[0];
        const before = weakest.hp; weakest.hp = Math.min(weakest.maxHp, before + 6 + numeric(u.star, 1) * 2 + basicBonus(state, u, 'heal'));
        events.push({ source: u, target: weakest, heal: weakest.hp - before });
      }
      const target = targetFor(u, enemies(), false);
      attack(u, target, u.atk * counterMultiplier(u, target));
      u.cd = Math.max(.38, 1.75 / numeric(u.spd, 1, .35, 20));
    }
    for (const e of enemies()) {
      if (!allies().length) break;
      e.cd = numeric(e.cd, 0, -100) - .25;
      if (e.cd > 0) continue;
      attack(e, targetFor(e, allies(), true), e.atk * (e.boss ? 1.08 : 1));
      e.cd = Math.max(.45, 1.9 / numeric(e.spd, 1, .35, 20));
    }
    return { events, outcome: !allies().length ? 'defeat' : !enemies().length ? 'victory' : null };
  }
  return { VERSION, MODES, PHASES, ROLE_INFO, SYNERGIES, COUNTERS, PATHS, STARTERS, MODIFIERS, ARCHETYPES, EVENTS, OBJECTIVES, numeric, copy, tags, has, role, countTags, synergies, protectedBy, recruitImpact, counterMultiplier, pathsFor, pathMods, nextRandom, weighted, newRun, armyStyle, sanitizeUnit, sanitizeSnapshot, basicBonus, initializeBasicBattle, stepBasicBattle };
});
