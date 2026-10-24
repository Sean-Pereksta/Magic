/* Tower armory rules. Stationary defenders are the native game's towers.
   Pure data/transactions: no DOM, timers, storage, or combat callbacks. */
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.TinyTroopsEquipmentRules = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';
  const VERSION = 1;
  const RARITIES = ['common', 'uncommon', 'rare', 'epic', 'legendary'];
  const RARITY_WEIGHTS = [52, 28, 14, 5, 1];
  const DROP_CHANCES = Object.freeze({ normal: .12, elite: .35, boss: 1 });
  const ITEMS = [
    { id: 'sharpened-bolts', name: 'Sharpened Bolts', emoji: '🏹', rarity: 'common', category: 'damage', compatible: ['all'], description: '+8% attack damage.', mods: { damage: .08 } },
    { id: 'reinforced-gears', name: 'Reinforced Gears', emoji: '⚙️', rarity: 'common', category: 'speed', compatible: ['all'], description: '+6% attack speed.', mods: { speed: .06 } },
    { id: 'watchers-lens', name: "Watcher's Lens", emoji: '🔍', rarity: 'common', category: 'range', compatible: ['ranged', 'magic', 'machine'], description: '+8% targeting reach: 8% chance per attack to reach one extra enemy for 40% damage. Native focus targeting stays available.', mods: { reach: .08 } },
    { id: 'stone-plating', name: 'Stone Plating', emoji: '🪨', rarity: 'common', category: 'defense', compatible: ['all'], description: '+12% maximum durability (health) at battle start.', mods: { durability: .12 } },
    { id: 'swift-trigger', name: 'Swift Trigger', emoji: '🎯', rarity: 'uncommon', category: 'speed', compatible: ['ranged', 'machine', 'magic'], description: '+12% attack speed.', mods: { speed: .12 } },
    { id: 'longshot-scope', name: 'Longshot Scope', emoji: '🔭', rarity: 'uncommon', category: 'range', compatible: ['ranged', 'magic', 'machine'], description: '+15% targeting reach: 15% chance per attack to reach one extra enemy for 40% damage. Native focus targeting stays available.', mods: { reach: .15 } },
    { id: 'flame-oil', name: 'Flame Oil', emoji: '🛢️', rarity: 'uncommon', category: 'element', compatible: ['ranged', 'machine', 'magic', 'fire'], description: 'Attacks add 2 burn stacks. Fire troops deal +5% damage to burning enemies while this is deployed.', mods: { burn: 2 } },
    { id: 'frost-crystal', name: 'Frost Crystal', emoji: '🧊', rarity: 'uncommon', category: 'control', compatible: ['ranged', 'magic', 'frost'], description: '35% chance per attack to add 0.10 slow (native cap 0.60). Ranged allies deal +4% damage to slowed enemies.', mods: { frostChance: .35, slow: .10 } },
    { id: 'piercing-arrows', name: 'Piercing Arrows', emoji: '🗡️', rarity: 'rare', category: 'pierce', compatible: ['ranged', 'machine', 'heavy'], description: 'Ignore 30% of native enemy armor reduction. Defenders already ignoring armor retain their native benefit. Heavy allies deal +4% damage to armored enemies.', mods: { pierce: .30 } },
    { id: 'explosive-payload', name: 'Explosive Payload', emoji: '💣', rarity: 'rare', category: 'splash', compatible: ['ranged', 'machine', 'heavy'], description: 'Attacks splash 22% damage to up to two living neighboring enemies.', mods: { splash: .22 } },
    { id: 'venom-chamber', name: 'Venom Chamber', emoji: '☠️', rarity: 'rare', category: 'element', compatible: ['ranged', 'machine', 'magic', 'poison'], description: 'Attacks add 3 poison stacks (equipment stacks capped at 30 per enemy). Poison allies deal +5% damage to poisoned enemies.', mods: { poison: 3 } },
    { id: 'guardian-charm', name: 'Guardian Charm', emoji: '🧿', rarity: 'rare', category: 'defense', compatible: ['support', 'armored'], description: 'Every third attack grants 8 shield to up to three orthogonally adjacent allies. Neighboring armored allies take 4% less damage.', mods: { guardian: 8 } },
    { id: 'chain-lightning-coil', name: 'Chain Lightning Coil', emoji: '⚡', rarity: 'epic', category: 'element', compatible: ['machine', 'magic', 'ranged'], description: '25% chance per attack to arc lightning to two extra enemies for 35% damage each.', mods: { chainChance: .25, chainDamage: .35 } },
    { id: 'echo-mechanism', name: 'Echo Mechanism', emoji: '🔁', rarity: 'epic', category: 'damage', compatible: ['machine', 'ranged', 'magic'], description: '20% chance per attack to fire an extra hit for 55% damage. Equipment procs cannot trigger further procs.', mods: { echoChance: .20, echoDamage: .55 } },
    { id: 'blizzard-core', name: 'Blizzard Core', emoji: '❄️', rarity: 'epic', category: 'control', compatible: ['frost', 'magic', 'support'], description: 'Every fourth attack adds 0.16 slow to up to five live enemies. Ranged allies deal +4% damage to slowed enemies.', mods: { blizzard: .16 } },
    { id: 'phoenix-ember', name: 'Phoenix Ember', emoji: '🔥', rarity: 'epic', category: 'damage', compatible: ['fire', 'magic', 'machine'], description: '+25% attack damage against burning enemies. Fire allies deal +5% damage to burning enemies.', mods: { burningDamage: .25 } },
    { id: 'stormheart-engine', name: 'Stormheart Engine', emoji: '🌩️', rarity: 'legendary', category: 'element', compatible: ['machine', 'magic', 'ranged'], description: 'Every fifth attack calls a lightning storm on up to five live enemies for 50% damage each.', mods: { storm: .50 } },
    { id: 'ancient-siege-relic', name: 'Ancient Siege Relic', emoji: '🏺', rarity: 'legendary', category: 'pierce', compatible: ['heavy', 'machine', 'ranged'], description: '+15% attack damage, ignore 60% of native armor reduction, and splash 35% damage to two neighboring enemies.', mods: { damage: .15, pierce: .60, splash: .35 } }
  ];
  const BY_ID = Object.fromEntries(ITEMS.map(item => [item.id, Object.freeze(item)]));
  const num = (value, fallback = 0, min = 0, max = 1e12) => Number.isFinite(Number(value)) ? Math.min(max, Math.max(min, Number(value))) : fallback;
  const units = state => [...(Array.isArray(state?.squad) ? state.squad : []), ...(Array.isArray(state?.bench) ? state.bench : [])].filter(Boolean);
  function towerKinds(unit) {
    if (!unit || unit.team === 'enemy') return [];
    const tags = Array.isArray(unit.t) ? unit.t : [], name = String(unit.n || '');
    const kinds = ['all'];
    if (/archer|sniper|marksman|gunner|crossbow|mortar|cannon|drone|javelin|boomerang/i.test(name) || unit.kind === 'ranged' || (Array.isArray(unit.tags) && unit.tags.includes('RANGED'))) kinds.push('ranged');
    if (tags.includes('Machine')) kinds.push('machine');
    if (tags.includes('Mage')) kinds.push('magic');
    if (tags.includes('Guard') || tags.includes('Stone')) kinds.push('armored');
    if (tags.includes('Healer') || unit.focus === 'support') kinds.push('support');
    if (tags.includes('Fire')) kinds.push('fire');
    if (tags.includes('Frost')) kinds.push('frost');
    if (tags.includes('Poison')) kinds.push('poison');
    if (num(unit.baseAtk ?? unit.atk) >= 18 || num(unit.baseSpd || unit.spd, 1) <= .65 || /mortar|cannon|siege|giant/i.test(name)) kinds.push('heavy');
    return kinds;
  }
  const slotsFor = unit => num(unit?.star, 1) >= 5 ? 2 : 1;
  const compatible = (item, unit) => {
    const definition = typeof item === 'string' ? BY_ID[item] : item;
    return !!unit && Array.isArray(definition?.compatible) && towerKinds(unit).some(kind => definition.compatible.includes(kind));
  };
  function identifyUnits(state) {
    if (!state.tt || typeof state.tt !== 'object') state.tt = {};
    const seen = new Set();
    units(state).forEach(unit => {
      if (typeof unit.ttId !== 'string' || !unit.ttId || seen.has(unit.ttId)) {
        do { state.tt.nextUnitId = Math.floor(num(state.tt.nextUnitId)) + 1; unit.ttId = String(state.tt.id || 'legacy') + '-u' + state.tt.nextUnitId; } while (seen.has(unit.ttId));
      }
      seen.add(unit.ttId);
    });
  }
  function migrate(state) {
    identifyUnits(state);
    const previous = state.tt.equipment && typeof state.tt.equipment === 'object' && !Array.isArray(state.tt.equipment) ? state.tt.equipment : {};
    const equipment = { version: VERSION, nextId: Math.floor(num(previous.nextId)), items: [], rewarded: {}, misses: Math.floor(num(previous.misses, 0, 0, 7)), introClaimed: !!previous.introClaimed };
    const ids = new Set();
    (Array.isArray(previous.items) ? previous.items : []).forEach(value => {
      if (!value || !BY_ID[value.item || value.itemId]) return;
      let id = typeof value.id === 'string' && value.id.length <= 180 ? value.id : '';
      if (!id || ids.has(id)) { do { id = String(state.tt.id || 'legacy') + '-e' + ++equipment.nextId; } while (ids.has(id)); }
      ids.add(id);
      equipment.items.push({ id, item: value.item || value.itemId, towerId: typeof value.towerId === 'string' ? value.towerId : null, slot: Number.isInteger(value.slot) ? value.slot : null, acquiredWave: Math.floor(num(value.acquiredWave)), source: String(value.source || 'legacy').slice(0, 80) });
    });
    if (previous.rewarded && typeof previous.rewarded === 'object' && !Array.isArray(previous.rewarded)) Object.keys(previous.rewarded).forEach(key => {
      if (/^wave:[1-9]\d*$|^challenge:[\w-]{1,80}$/.test(key)) equipment.rewarded[key] = typeof previous.rewarded[key] === 'string' ? previous.rewarded[key].slice(0, 180) : null;
    });
    state.tt.equipment = equipment;
    reconcile(state);
    return equipment;
  }
  function reconcile(state) {
    const equipment = state?.tt?.equipment;
    if (!equipment || !Array.isArray(equipment.items)) return [];
    identifyUnits(state);
    const towers = new Map(units(state).map(unit => [unit.ttId, unit])), occupied = new Set(), recovered = [];
    equipment.items.forEach(instance => {
      if (!instance.towerId) { instance.towerId = null; instance.slot = null; return; }
      const tower = towers.get(instance.towerId), key = instance.towerId + ':' + instance.slot;
      if (!tower || !compatible(instance.item, tower) || !Number.isInteger(instance.slot) || instance.slot < 0 || instance.slot >= slotsFor(tower) || occupied.has(key)) {
        recovered.push(instance.id); instance.towerId = null; instance.slot = null;
      } else occupied.add(key);
    });
    return recovered;
  }
  function equipped(state, unit) { return (state?.tt?.equipment?.items || []).filter(instance => instance.towerId === unit?.ttId && Number.isInteger(instance.slot) && instance.slot < slotsFor(unit)); }
  function modifiers(state, unit, replacement) {
    const instances = equipped(state, unit).filter(instance => !replacement || instance.slot !== replacement.slot);
    if (replacement?.item) instances.push({ item: replacement.item });
    return instances.reduce((mods, instance) => { const item = BY_ID[instance.item]; if (item && compatible(item, unit)) Object.entries(item.mods).forEach(([key, value]) => mods[key] = (mods[key] || 0) + value); return mods; }, {});
  }
  function canChange(state) {
    return !!state?.tt && !state.tt.finished && state.phase !== 'battle' && !['MENU', 'RESULTS', 'DEFEAT'].includes(state.tt.mode) && !state.placing && !state.placingEffect && !state.placingUpgrade && !state.placingSpecialization;
  }
  function equip(state, towerId, slot, instanceId) {
    if (!canChange(state)) return { ok: false, reason: 'Equipment changes are available during preparation.' };
    const equipment = state.tt.equipment || migrate(state), tower = units(state).find(unit => unit.ttId === towerId), instance = equipment.items.find(item => item.id === instanceId);
    if (!tower || !instance || !Number.isInteger(slot) || slot < 0 || slot >= slotsFor(tower)) return { ok: false, reason: 'This defender or equipment slot is no longer available.' };
    if (!compatible(instance.item, tower)) return { ok: false, reason: 'This item does not fit this defender.' };
    if (instance.towerId && (instance.towerId !== towerId || instance.slot !== slot)) return { ok: false, reason: 'Unequip this unique copy from its current defender first.' };
    const replaced = equipment.items.find(item => item.towerId === towerId && item.slot === slot);
    if (replaced && replaced !== instance) { replaced.towerId = null; replaced.slot = null; }
    instance.towerId = towerId; instance.slot = slot;
    return { ok: true, instance, replaced: replaced && replaced !== instance ? replaced : null };
  }
  function unequip(state, towerId, slot) {
    if (!canChange(state)) return { ok: false, reason: 'Equipment changes are available during preparation.' };
    const instance = state.tt.equipment?.items.find(item => item.towerId === towerId && item.slot === slot);
    if (!instance) return { ok: false, reason: 'This slot is already empty.' };
    instance.towerId = null; instance.slot = null;
    return { ok: true, instance };
  }
  function addItem(state, itemId, source = 'reward', wave = state.round) {
    const equipment = state.tt.equipment || migrate(state);
    if (!BY_ID[itemId]) return null;
    let id;
    do { id = String(state.tt.id || 'legacy') + '-e' + ++equipment.nextId; } while (equipment.items.some(instance => instance.id === id));
    const instance = { id, item: itemId, towerId: null, slot: null, acquiredWave: Math.floor(num(wave)), source: String(source).slice(0, 80) };
    equipment.items.push(instance); return instance;
  }
  function rollItem(random) {
    let roll = num(random(), 0, 0, .999999999999) * 100, rarity = RARITIES[RARITIES.length - 1];
    for (let i = 0; i < RARITIES.length; i++) { roll -= RARITY_WEIGHTS[i]; if (roll < 0) { rarity = RARITIES[i]; break; } }
    const pool = ITEMS.filter(item => item.rarity === rarity);
    return pool[Math.floor(num(random(), 0, 0, .999999999999) * pool.length)];
  }
  function reward(state, options = {}, random = Math.random) {
    const equipment = state.tt.equipment || migrate(state), wave = Math.floor(num(options.wave ?? state.round, 1, 1)), activity = options.activity || 'normal';
    const key = options.challengeId ? 'challenge:' + String(options.challengeId).replace(/[^\w-]/g, '').slice(0, 80) : 'wave:' + wave;
    if (Object.prototype.hasOwnProperty.call(equipment.rewarded, key)) return { awarded: false, duplicate: true, instance: equipment.items.find(item => item.id === equipment.rewarded[key]) || null };
    // The very first victory teaches the armory without an optional reward modal.
    const introductory = !equipment.introClaimed && !options.challengeId;
    const chance = options.challengeId ? num(options.chance, 1, 0, 1) : DROP_CHANCES[activity] ?? DROP_CHANCES.normal;
    const protectedDrop = !options.challengeId && equipment.misses >= 7;
    const drops = introductory || protectedDrop || chance >= 1 || num(random(), 0, 0, 1) < chance;
    const item = drops ? introductory ? BY_ID['sharpened-bolts'] : options.item && BY_ID[options.item] || rollItem(random) : null;
    const instance = item ? addItem(state, item.id, introductory ? 'First victory' : protectedDrop ? 'Supply guarantee' : activity, wave) : null;
    if (!options.challengeId) { equipment.introClaimed = true; equipment.misses = drops ? 0 : equipment.misses + 1; }
    equipment.rewarded[key] = instance?.id || null;
    return { awarded: !!instance, duplicate: false, introductory, protectedDrop, instance };
  }
  function filterItems(state, filters = {}, tower) {
    return (state?.tt?.equipment?.items || []).filter(instance => {
      const item = BY_ID[instance.item];
      return item && (!filters.rarity || item.rarity === filters.rarity) && (!filters.category || item.category === filters.category) && (!filters.compatibility || tower && compatible(item, tower)) && (filters.status !== 'available' || !instance.towerId) && (filters.status !== 'equipped' || !!instance.towerId);
    });
  }
  function preview(state, unit, replacement) {
    const mods = modifiers(state, unit, replacement);
    return { damage: num(unit?.baseAtk ?? unit?.atk) * (1 + (mods.damage || 0)), speed: num(unit?.baseSpd || unit?.spd, 1) * (1 + (mods.speed || 0)), health: num(unit?.baseMaxHp || unit?.maxHp || unit?.hp, 1) * (1 + (mods.durability || 0)), reach: mods.reach || 0, mods };
  }
  return { VERSION, ITEMS, BY_ID, RARITIES, RARITY_WEIGHTS, DROP_CHANCES, towerKinds, slotsFor, compatible, units, identifyUnits, migrate, reconcile, equipped, modifiers, canChange, equip, unequip, addItem, rollItem, reward, filterItems, preview };
});
