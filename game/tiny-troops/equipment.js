/* Tower armory integration. Native stationary troops remain authoritative:
   equipment never replaces their abilities, targeting, evolutions, or effects. */
(function () {
  'use strict';
  const E = window.TinyTroopsEquipmentRules, R = window.TinyTroopsRules;
  if (!E || !R) throw new Error('The tower armory rules must load before equipment.');
  const esc = value => String(value ?? '').replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
  const node = id => document.getElementById(id);
  const living = unit => !!unit && !unit.dead && Number.isFinite(unit.hp) && unit.hp > 0;
  const nativeHas = (unit, ...tags) => !!unit && Array.isArray(unit.t) && tags.some(tag => unit.t.includes(tag));
  const state = { equipment: null, run: null, mods: new Map(), selection: null, preview: null, filters: { rarity: '', category: '', status: '', compatibility: false }, lastInventoryScroll: 0 };
  const api = window.TinyTroopsEquipment = { ready: false, rules: E };
  function ensure() {
    if (!S.tt) return null;
    if (state.equipment !== S.tt.equipment || state.run !== S.tt.id || S.tt.equipment?.version !== E.VERSION) {
      E.migrate(S); state.equipment = S.tt.equipment; state.run = S.tt.id;
    }
    E.reconcile(S);
    const units = new Map(E.units(S).map(unit => [unit.ttId, unit]));
    state.mods = new Map([...units.keys()].map(id => [id, {}]));
    state.equipment.items.forEach(instance => {
      const unit = units.get(instance.towerId), item = E.BY_ID[instance.item];
      if (unit && item && E.compatible(item, unit)) Object.entries(item.mods).forEach(([key, value]) => {
        const values = state.mods.get(unit.ttId); values[key] = (values[key] || 0) + value;
      });
    });
    return state.equipment;
  }
  const mods = unit => state.mods.get(unit?.ttId) || {};
  const towerById = id => E.units(S).find(unit => unit.ttId === id);
  const location = instance => {
    const unit = instance.towerId && towerById(instance.towerId);
    return unit ? unit.n + ' · slot ' + (instance.slot + 1) : 'Available in inventory';
  };
  const label = instance => E.BY_ID[instance?.item]?.name || 'Empty';
  function persist(reason) { ensure(); render(); if (typeof saveProfile === 'function') saveProfile(reason); }
  function notify(text) { if (typeof msg === 'function') msg(text); }
  function effect(unit, text, type = 'storm') {
    // Presentation is best-effort and can never cancel a combat transaction.
    try {
      if (!living(unit)) return;
      const el = typeof elForUnit === 'function' ? elForUnit(unit) : null;
      if (el && typeof intentTag === 'function') intentTag(el, text, type);
    } catch (error) { console.warn('Tiny Troops equipment visual', error); }
  }
  function canManage() { return E.canChange(S) && !window.TinyTroopsPolish?.recovery; }
  function slotHtml(unit) {
    const equipped = E.equipped(S, unit);
    return '<div class="tt-equipment-slots">' + Array.from({ length: E.slotsFor(unit) }, (_, slot) => {
      const instance = equipped.find(item => item.slot === slot), item = E.BY_ID[instance?.item];
      return `<div class="tt-equipment-slot ${item ? 'rarity-' + item.rarity : ''}"><button class="secondary" data-tt-eq-slot="${slot}" data-tt-eq-unit="${esc(unit.ttId)}" aria-label="Equipment slot ${slot + 1}: ${esc(item?.name || 'Empty')}">${item?.emoji || '＋'} ${esc(item?.name || 'Equip item')}<small>Slot ${slot + 1}</small></button>${instance ? `<button class="secondary" data-tt-eq-remove="${slot}" data-tt-eq-unit="${esc(unit.ttId)}" ${canManage() ? '' : 'disabled'} aria-label="Unequip ${esc(item.name)}">Unequip</button>` : ''}</div>`;
    }).join('') + (E.slotsFor(unit) === 1 ? '<p class="hint">Second slot unlocks at 5★.</p>' : '') + '</div>';
  }
  function comparisonHtml(before, after) {
    const f = value => Math.round(value * 100) / 100;
    return `<div class="tt-item-preview"><p><b>Equipment stat comparison</b></p><table><thead><tr><th>Stat</th><th>Current</th><th>Proposed</th></tr></thead><tbody><tr><th>Base damage</th><td>${f(before.damage)}</td><td>${f(after.damage)}</td></tr><tr><th>Attack speed</th><td>${f(before.speed)}</td><td>${f(after.speed)}</td></tr><tr><th>Durability</th><td>${f(before.health)}</td><td>${f(after.health)}</td></tr><tr><th>Extra target reach</th><td>${f(before.reach * 100)}%</td><td>${f(after.reach * 100)}%</td></tr></tbody></table><small>Equipment bonuses shown here. Native traits, growth, and battle effects still apply.</small></div>`;
  }
  function unitSection(unit) {
    if (!unit || unit.team === 'enemy') return '';
    const preview = E.preview(S, unit), b = mods(unit), target = typeof FO === 'object' ? FO[unit.focus] || unit.focus : unit.focus;
    return `<section class="tt-equipment-section"><h3>🗼 Tower equipment</h3><p class="hint">This stationary defender uses your native troop progression. ${unit.star || 1}★ · ${esc(E.towerKinds(unit).filter(kind => kind !== 'all').join(' / ') || 'frontline')} · Targeting: ${esc(target || 'native focus')}</p><div class="tt-chips"><span>⚔️ ${Math.round(preview.damage)} base damage</span><span>⚡ ${(preview.speed / 1.75).toFixed(2)} base attacks/s</span><span>🎯 Native focus${b.reach ? ' + ' + Math.round(b.reach * 100) + '% extra reach' : ''}</span></div>${slotHtml(unit)}<p class="hint">Equip during preparation. Defeated defenders revive with their items; sold or permanently removed defenders return items to inventory.</p><button class="secondary" data-tt-eq-action="armory">Tower armory & collection</button></section>`;
  }
  function openUnit(unit) {
    ensure();
    if (!unit || !E.units(S).includes(unit)) return openArmory();
    state.selection = { towerId: unit.ttId, slot: 0 };
    show(unit.e + ' ' + unit.n + ' · Tower equipment', 'Damage, targeting, slots, and compatible equipment.', unitSection(unit));
    node('drawer').dataset.ttView = 'equipment-unit';
  }
  function synergies() {
    const deployed = (S.squad || []).filter(living), found = [];
    if (deployed.some(unit => mods(unit).slow || mods(unit).blizzard) && deployed.some(unit => E.towerKinds(unit).includes('ranged'))) found.push({ id: 'frozen-barrage', name: '❄️ Frozen Barrage', description: 'Ranged defenders deal +4% damage to slowed enemies.' });
    if (deployed.some(unit => mods(unit).burn || mods(unit).burningDamage) && deployed.some(unit => nativeHas(unit, 'Fire'))) found.push({ id: 'burning-front', name: '🔥 Burning Front', description: 'Fire defenders deal +5% damage to burning enemies.' });
    if (deployed.some(unit => mods(unit).guardian)) found.push({ id: 'shield-wall', name: '🛡️ Shield Wall', description: 'Armored defenders orthogonally adjacent to a Guardian Charm take 4% less damage.' });
    if (deployed.some(unit => mods(unit).poison) && deployed.some(unit => nativeHas(unit, 'Poison'))) found.push({ id: 'poison-volley', name: '☠️ Poison Volley', description: 'Poison defenders deal +5% damage to poisoned enemies.' });
    if (deployed.some(unit => mods(unit).pierce) && deployed.some(unit => E.towerKinds(unit).includes('heavy'))) found.push({ id: 'siege-breaker', name: '🏺 Siege Breaker', description: 'Heavy defenders deal +4% damage to armored enemies.' });
    return found;
  }
  function synergyHtml() {
    const active = synergies();
    return `<details class="tt-equipment-synergies"><summary>Equipment synergies · ${active.length} active</summary>${active.length ? active.map(synergy => `<p><b>${synergy.name}</b><br>${synergy.description}</p>`).join('') : '<p>Combine elemental or defensive gear with matching defenders to discover modest team bonuses.</p>'}</details>`;
  }
  function openArmory() {
    ensure();
    if (!S.tt || S.tt.mode === 'MENU') return notify('Start or load a run to view its tower armory.');
    state.selection = null; state.preview = null;
    const towers = E.units(S), equipment = S.tt.equipment;
    show('🗼 Tower armory', 'Stationary defenders share the troop roster. Existing spells, effects, and evolution remain available.', `<p>${equipment.items.length} collected item${equipment.items.length === 1 ? '' : 's'} · ${equipment.items.filter(item => item.towerId).length} equipped</p><button data-tt-eq-action="inventory">Open item collection</button>${synergyHtml()}<div class="tt-equipment-towers">${towers.map(unit => `<button class="choice secondary" data-tt-eq-action="unit" data-tt-eq-unit="${esc(unit.ttId)}"><span class="be">${esc(unit.e)}</span><span><strong>${esc(unit.n)} · ${unit.star || 1}★</strong><p>${S.squad.includes(unit) ? 'Deployed' : 'Reserve'} · ${E.equipped(S, unit).length}/${E.slotsFor(unit)} slots · ${E.equipped(S, unit).map(item => E.BY_ID[item.item].emoji).join(' ') || 'No equipment'}</p></span></button>`).join('') || '<p>Recruit a defender to equip your first item.</p>'}</div><p class="hint">First victory guarantees Sharpened Bolts. Normal / elite / boss victories: 12% / 35% / 100% item chance. After seven eligible victories without an item, the next victory guarantees one. Rarities: 52% common, 28% uncommon, 14% rare, 5% epic, 1% legendary.</p>`);
    node('drawer').dataset.ttView = 'equipment-armory';
  }
  function filterHtml(tower) {
    const select = (name, title, values) => `<label>${title}<select data-tt-eq-filter="${name}"><option value="">All</option>${values.map(value => `<option value="${value}" ${state.filters[name] === value ? 'selected' : ''}>${value[0].toUpperCase() + value.slice(1)}</option>`).join('')}</select></label>`;
    return '<div class="tt-item-filters">' + select('rarity', 'Rarity', E.RARITIES) + select('category', 'Effect', ['damage', 'speed', 'range', 'defense', 'element', 'control', 'pierce', 'splash']) + select('status', 'Status', ['available', 'equipped']) + (tower ? `<label><input type="checkbox" data-tt-eq-filter="compatibility" ${state.filters.compatibility ? 'checked' : ''}>Compatible with ${esc(tower.n)}</label>` : '') + '</div>';
  }
  function drawInventory() {
    ensure();
    const tower = state.selection && towerById(state.selection.towerId), slot = state.selection?.slot || 0;
    const items = E.filterItems(S, state.filters, tower), prior = tower && E.equipped(S, tower).find(item => item.slot === slot);
    const html = `${tower ? `<p>Slot ${slot + 1} · ${esc(tower.n)} · Current: ${esc(label(prior))}</p>` : '<p>Each icon is one unique copy. Select an item to view its exact effect and location.</p>'}${filterHtml(tower)}<div class="tt-item-grid" role="list" aria-label="Owned tower items">${items.map(instance => {
      const item = E.BY_ID[instance.item], fits = !tower || E.compatible(item, tower);
      return `<button role="listitem" class="tt-item-card rarity-${item.rarity} ${fits ? '' : 'incompatible'}" data-tt-eq-preview="${esc(instance.id)}" aria-label="${esc(item.name + ', ' + item.rarity + ', ' + location(instance))}"><span class="be">${item.emoji}</span><strong>${esc(item.name)}</strong><small>${item.rarity} · ${instance.towerId ? 'Equipped' : 'Available'}${fits ? '' : ' · Incompatible'}</small></button>`;
    }).join('') || '<p>No items match these filters. Equipment arrives from victories.</p>'}</div><div id="ttItemPreview" aria-live="polite"></div><button class="secondary" data-tt-eq-action="armory">Back to tower armory</button>`;
    show(tower ? 'Equip ' + tower.n : '🧰 Tower item collection', tower ? 'Preview before equipping. Closing leaves your current item in place.' : 'Filter, inspect, and choose a defender.', html);
    node('drawer').dataset.ttView = 'equipment-inventory';
    node('drawer').scrollTop = state.lastInventoryScroll;
    if (state.preview) previewItem(state.preview, false);
  }
  function openInventory(unit, slot = 0) {
    ensure();
    if (!S.tt || S.tt.mode === 'MENU') return notify('Start or load a run to view its collection.');
    if (unit && E.units(S).includes(unit) && Number.isInteger(slot) && slot >= 0 && slot < E.slotsFor(unit)) {
      state.selection = { towerId: unit.ttId, slot }; state.filters.compatibility = true;
    } else { state.selection = null; state.filters.compatibility = false; }
    state.preview = null; state.lastInventoryScroll = 0; drawInventory();
  }
  function previewItem(instanceId, scroll = true) {
    ensure();
    const instance = S.tt.equipment.items.find(item => item.id === instanceId), panel = node('ttItemPreview');
    if (!instance || !panel) return;
    state.preview = instanceId;
    const item = E.BY_ID[instance.item], tower = state.selection && towerById(state.selection.towerId), slot = state.selection?.slot || 0;
    const fits = tower && E.compatible(item, tower), free = !instance.towerId || instance.towerId === tower?.ttId && instance.slot === slot;
    const prior = tower && E.equipped(S, tower).find(copy => copy.slot === slot);
    panel.innerHTML = `<article class="tt-item-preview rarity-${item.rarity}"><h3>${item.emoji} ${esc(item.name)}</h3><p>${item.rarity} · ${item.category}</p><p>${esc(item.description)}</p><p><b>Compatible:</b> ${esc(item.compatible.join(', '))}<br><b>Location:</b> ${esc(location(instance))}<br><b>Discovered:</b> Wave ${instance.acquiredWave || '—'} · ${esc(instance.source)}</p>${tower ? comparisonHtml(E.preview(S, tower), E.preview(S, tower, { slot, item: item.id })) + `<button data-tt-eq-equip="${esc(instance.id)}" ${fits && free && canManage() ? '' : 'disabled'}>${prior?.id === instance.id ? 'Already equipped' : prior ? 'Replace ' + esc(label(prior)) : 'Equip to slot ' + (slot + 1)}</button><p class="hint">${!fits ? 'This defender cannot use this item.' : !free ? 'Unequip this unique copy from its current defender first.' : !canManage() ? 'Changes unlock during preparation.' : prior && prior.id !== instance.id ? 'Your current item returns to inventory when replaced.' : 'This item stays in your collection when unequipped.'}</p>` : `<p>Choose a compatible defender:</p><div class="tt-equipment-towers">${E.units(S).filter(unit => E.compatible(item, unit)).map(unit => `<button class="secondary" data-tt-eq-select="${esc(unit.ttId)}" data-tt-eq-copy="${esc(instance.id)}">${esc(unit.e)} ${esc(unit.n)} · ${unit.star || 1}★</button>`).join('') || '<p>Recruit a compatible defender to use this item.</p>'}</div>`}</article>`;
    if (scroll) panel.scrollIntoView({ block: 'nearest', behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth' });
  }
  function equipSelected(instanceId) {
    if (!state.selection || !canManage()) return notify('Equipment changes are available during preparation.');
    const result = E.equip(S, state.selection.towerId, state.selection.slot, instanceId);
    if (!result.ok) return notify(result.reason);
    const unit = towerById(state.selection.towerId); persist('tower item equipped'); notify(label(result.instance) + ' equipped to ' + unit.n + '.');
    drawInventory();
  }
  function removeSelected(unitId, slot) {
    if (!canManage()) return notify('Equipment changes are available during preparation.');
    const result = E.unequip(S, unitId, slot);
    if (!result.ok) return notify(result.reason);
    persist('tower item unequipped'); notify(label(result.instance) + ' returned to inventory.');
    openUnit(towerById(unitId));
  }
  function decorate() {
    if (!S.tt || S.tt.mode === 'MENU') return;
    document.querySelectorAll('.cell').forEach((cell, index) => {
      const unit = S.squad[index], gear = unit && E.equipped(S, unit);
      let badge = cell.querySelector('.tt-equipment-badge');
      if (!gear?.length) { badge?.remove(); return; }
      if (!badge) { badge = document.createElement('span'); badge.className = 'tt-equipment-badge'; cell.appendChild(badge); }
      badge.textContent = gear.map(copy => E.BY_ID[copy.item].emoji).join(''); badge.title = gear.map(copy => label(copy)).join(', ');
      badge.setAttribute('aria-label', 'Equipped: ' + badge.title);
    });
    const parent = document.querySelector('.tt-army-status');
    if (parent && !node('ttArmoryButton')) { const button = document.createElement('button'); button.id = 'ttArmoryButton'; button.className = 'secondary'; button.dataset.ttEqAction = 'armory'; button.textContent = '🗼 Armory'; parent.appendChild(button); }
  }

  // Save migration uses the same validated S.tt subtree as battle checkpoints.
  const sanitizeSnapshot = R.sanitizeSnapshot;
  R.sanitizeSnapshot = function (input, roster) { const snap = sanitizeSnapshot(input, roster); E.migrate(snap); return snap; };
  const nativeBonus = bonus;
  bonus = function (unit) {
    const b = nativeBonus(unit), m = mods(unit);
    if (unit?.team !== 'ally') return b;
    b.atk = (b.atk || 0) + (unit.baseAtk ?? unit.atk ?? 0) * (m.damage || 0);
    b.spd = (b.spd || 0) + (unit.baseSpd || unit.spd || 1) * (m.speed || 0);
    b.hp = (b.hp || 0) + (unit.baseMaxHp || unit.maxHp || 1) * (m.durability || 0);
    return b;
  };
  const nativeDamage = dmg;
  let secondary = false;
  dmg = function (source, target, amount, type = 'hit') {
    if (!living(target) || !Number.isFinite(amount) || amount <= 0) return 0;
    let value = amount;
    if (source?.team === 'ally' && S.squad.includes(source) && target.team === 'enemy') {
      const m = mods(source), nativePierce = source.focus === 'armor' || nativeHas(source, 'Machine', 'Stone', 'Storm');
      if (m.pierce && type !== 'poison' && target.armor > 0 && !nativePierce) {
        // Compensate the known native armor formula without mutating the target.
        const armorReduction = Math.min(.45, target.armor * .035);
        value *= (1 - armorReduction * (1 - Math.min(1, m.pierce))) / (1 - armorReduction);
      }
      if (m.burningDamage && target.burn > 0) value *= 1 + m.burningDamage;
      const withGear = predicate => S.squad.some(unit => living(unit) && predicate(mods(unit)));
      if (target.slow > 0 && E.towerKinds(source).includes('ranged') && withGear(gear => gear.slow || gear.blizzard)) value *= 1.04;
      if (target.burn > 0 && nativeHas(source, 'Fire') && withGear(gear => gear.burn || gear.burningDamage)) value *= 1.05;
      if (target.poison > 0 && nativeHas(source, 'Poison') && withGear(gear => gear.poison)) value *= 1.05;
      if (target.armor > 0 && E.towerKinds(source).includes('heavy') && withGear(gear => gear.pierce)) value *= 1.04;
    } else if (target.team === 'ally' && E.towerKinds(target).includes('armored')) {
      const index = S.squad.indexOf(target);
      if (index >= 0 && S.squad.some((unit, other) => living(unit) && mods(unit).guardian && Math.abs(Math.floor(other / 4) - Math.floor(index / 4)) + Math.abs(other % 4 - index % 4) === 1)) value *= .96;
    }
    return nativeDamage(source, target, value, type);
  };
  function procDamage(source, target, amount, type) {
    if (!living(source) || !living(target) || !S.squad.includes(source) || !S.enemies.includes(target) || S.phase !== 'battle') return;
    const previous = secondary; secondary = true;
    try { dmg(source, target, amount, type); } finally { secondary = previous; }
  }
  const nativeAttack = allyAtk;
  allyAtk = function (unit) {
    if (secondary || !living(unit) || !S.squad.includes(unit) || S.phase !== 'battle' || S.tt.mode !== 'PLAYING') return nativeAttack(unit);
    const m = mods(unit), enemyBefore = S.enemies.filter(living), battle = S.battle;
    let target = null;
    // Capture the target actually damaged by native callbacks; target selection
    // may itself be random, so calling pickEnemy again would desynchronize it.
    const before = new Map(enemyBefore.map(enemy => [enemy, enemy.hp + (enemy.shield || 0)]));
    const result = nativeAttack(unit);
    if (!living(unit) || S.phase !== 'battle' || S.battle !== battle || !Object.keys(m).length) return result;
    target = enemyBefore.sort((a, b) => (before.get(b) - b.hp - (b.shield || 0)) - (before.get(a) - a.hp - (a.shield || 0)))[0];
    if (!target) return result;
    const actions = battle.equipmentActs ||= {}, count = actions[unit.ttId] = (Number.isFinite(actions[unit.ttId]) ? actions[unit.ttId] : 0) + 1;
    const amount = Math.max(1, (unit.atk || unit.baseAtk || 1) + (bonus(unit).atk || 0));
    const random = () => R.nextRandom(S.tt), otherEnemies = () => S.enemies.filter(enemy => living(enemy) && enemy !== target);
    if (living(target)) {
      if (m.burn) { target.burn = Math.min(1000, (target.burn || 0) + m.burn); effect(target, '🔥 Flame oil', 'burn'); }
      if (m.poison) { const old = target.equipmentPoison || 0, added = Math.min(m.poison, Math.max(0, 30 - old)); target.equipmentPoison = old + added; target.poison = Math.min(1000, (target.poison || 0) + added); effect(target, '☠ Venom', 'poison'); }
      if (m.frostChance && random() < m.frostChance) { target.slow = Math.min(.6, (target.slow || 0) + m.slow); effect(target, '❄ Frost crystal', 'frost'); }
      if (m.echoChance && random() < m.echoChance) { effect(unit, '🔁 Echo shot'); procDamage(unit, target, amount * m.echoDamage, 'hit'); }
    }
    if (m.reach && random() < Math.min(.5, m.reach)) { const extra = otherEnemies().at(-1); if (extra) { effect(unit, '🔭 Extended reach'); procDamage(unit, extra, amount * .4, 'hit'); } }
    if (m.splash) {
      const index = S.enemies.indexOf(target);
      S.enemies.filter((enemy, other) => living(enemy) && enemy !== target && Math.abs(other - index) === 1).slice(0, 2).forEach(enemy => procDamage(unit, enemy, amount * m.splash, 'hit'));
      effect(unit, '💥 Payload');
    }
    if (m.chainChance && random() < m.chainChance) { effect(unit, '⚡ Chain coil'); otherEnemies().slice(0, 2).forEach(enemy => procDamage(unit, enemy, amount * m.chainDamage, 'storm')); }
    if (m.guardian && count % 3 === 0) {
      const index = S.squad.indexOf(unit);
      S.squad.filter((ally, other) => living(ally) && Math.abs(Math.floor(other / 4) - Math.floor(index / 4)) + Math.abs(other % 4 - index % 4) === 1).slice(0, 3).forEach(ally => { giveShield(ally, m.guardian); effect(ally, '🛡 Guardian shield', 'holy'); });
    }
    if (m.blizzard && count % 4 === 0) { S.enemies.filter(living).slice(0, 5).forEach(enemy => enemy.slow = Math.min(.6, (enemy.slow || 0) + m.blizzard)); effect(unit, '❄ Blizzard core', 'frost'); }
    if (m.storm && count % 5 === 0) { effect(unit, '🌩 Stormheart'); S.enemies.filter(living).slice(0, 5).forEach(enemy => procDamage(unit, enemy, amount * m.storm, 'storm')); }
    return result;
  };
  const nativeStartBuffs = startBuffs;
  startBuffs = function () { ensure(); S.enemies.forEach(enemy => enemy.equipmentPoison = 0); return nativeStartBuffs(); };
  const nativeBasic = R.initializeBasicBattle;
  R.initializeBasicBattle = function (snapshot) {
    const result = nativeBasic(snapshot);
    snapshot.squad.filter(Boolean).forEach(unit => { const m = E.modifiers(snapshot, unit); unit.atk += (unit.baseAtk || unit.atk) * (m.damage || 0); unit.spd += (unit.baseSpd || unit.spd) * (m.speed || 0); const hp = (unit.baseMaxHp || unit.maxHp) * (m.durability || 0); unit.maxHp += hp; unit.hp += hp; });
    return result;
  };
  const nativeEnd = end;
  end = function (win) {
    if (typeof win !== 'boolean' || window.TinyTroopsPolish?.recovery) return nativeEnd(win);
    if (S.phase !== 'battle' || !S.battle || S.battle.ttEnded) return nativeEnd(win);
    ensure();
    if (win) {
      const activity = S.enemies.some(enemy => enemy.boss) ? 'boss' : S.enemies.some(enemy => enemy.elite) ? 'elite' : 'normal';
      const reward = E.reward(S, { wave: S.round, activity }, () => R.nextRandom(S.tt));
      if (reward.awarded) S.tt.lastEquipmentReward = { item: reward.instance.item, wave: S.round, introductory: reward.introductory, protectedDrop: reward.protectedDrop };
    }
    const result = nativeEnd(win); ensure();
    const loot = S.tt.lastEquipmentReward;
    if (win && loot && loot.wave === window.TinyTroopsPolish?.lastRecap?.wave) {
      const item = E.BY_ID[loot.item]; notify((node('msg')?.textContent || 'Victory!') + ' · ' + item.emoji + ' Found ' + item.name + ' (' + item.rarity + '). Open Armory to equip.');
    }
    return result;
  };
  const nativeDetailHtml = detailHtml;
  detailHtml = function (unit) { return nativeDetailHtml(unit) + unitSection(unit); };
  const nativeRender = render;
  render = function () { ensure(); const result = nativeRender(); decorate(); return result; };

  document.addEventListener('change', event => {
    const control = event.target.closest('[data-tt-eq-filter]'); if (!control) return;
    state.lastInventoryScroll = node('drawer').scrollTop;
    state.filters[control.dataset.ttEqFilter] = control.type === 'checkbox' ? control.checked : control.value; drawInventory();
  });
  document.addEventListener('click', event => {
    const control = event.target.closest('[data-tt-eq-action],[data-tt-eq-slot],[data-tt-eq-preview],[data-tt-eq-equip],[data-tt-eq-remove],[data-tt-eq-select]');
    if (!control || control.disabled) return;
    const data = control.dataset;
    if (data.ttEqSlot !== undefined) return openInventory(towerById(data.ttEqUnit), Number(data.ttEqSlot));
    if (data.ttEqPreview) return previewItem(data.ttEqPreview);
    if (data.ttEqEquip) return equipSelected(data.ttEqEquip);
    if (data.ttEqRemove !== undefined) return removeSelected(data.ttEqUnit, Number(data.ttEqRemove));
    if (data.ttEqSelect) {
      const unit = towerById(data.ttEqSelect); if (!unit) return;
      state.selection = { towerId: unit.ttId, slot: E.equipped(S, unit).length < E.slotsFor(unit) ? Array.from({ length: E.slotsFor(unit) }, (_, slot) => slot).find(slot => !E.equipped(S, unit).some(item => item.slot === slot)) : 0 };
      state.filters.compatibility = true; state.preview = data.ttEqCopy; drawInventory(); return;
    }
    if (data.ttEqAction === 'armory') return openArmory();
    if (data.ttEqAction === 'inventory') return openInventory();
    if (data.ttEqAction === 'unit') return openUnit(towerById(data.ttEqUnit));
  });
  Object.assign(api, { ensure, openArmory, open: openArmory, openUnit, openInventory, synergies, unitSection, previewItem, equip: (towerId, slot, id) => { ensure(); const result = canManage() ? E.equip(S, towerId, slot, id) : { ok: false, reason: 'Equipment changes are available during preparation.' }; if (result.ok) persist('tower item equipped'); return result; }, unequip: (towerId, slot) => { ensure(); const result = canManage() ? E.unequip(S, towerId, slot) : { ok: false, reason: 'Equipment changes are available during preparation.' }; if (result.ok) persist('tower item unequipped'); return result; } });
  Object.assign(window, { bonus, dmg, allyAtk, startBuffs, end, render, detailHtml });
  ensure(); decorate(); api.ready = true;
})();
