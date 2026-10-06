/* Stationary Tiny Troops integration. Existing roster, spell store, evolutions,
   and campaign mechanics remain authoritative; core.js supplies the new rules. */
(function () {
  'use strict';
  const R = window.TinyTroopsRules;
  const TP = window.TinyTroopsPolish = { ready: false, speed: 1, timer: null, preparingWave: false, stats: new Map(), focusTarget: null, lastRecap: null, unitSeq: 0, fx: new Map(), flashes: new Map(), structures: '', drawing: false, actions: new Set(), deaths: new Set() };
  const q = (s, root = document) => root.querySelector(s);
  const qa = (s, root = document) => [...root.querySelectorAll(s)];
  const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const storage = {
    read(key, fallback) { try { return JSON.parse(localStorage.getItem(key) || 'null') ?? fallback; } catch { return fallback; } },
    write(key, value) { try { localStorage.setItem(key, JSON.stringify(value)); return true; } catch { return false; } }
  };
  TP.speed = [1, 2, 3].includes(storage.read('ttBattleSpeed', 1)) ? storage.read('ttBattleSpeed', 1) : 1;
  let metaKey;
  function loadMeta() {
    metaKey = 'tinyTroops.meta.v3.' + cleanUsername($('menuUser').value || 'guest');
    const saved = storage.read(metaKey, {});
    TP.meta = { units: [], synergies: [], paths: [], runs: [], boss: false, ...saved };
    ['units', 'synergies', 'paths'].forEach(k => TP.meta[k] = Array.isArray(TP.meta[k]) ? TP.meta[k].filter(v => typeof v === 'string').slice(-1000) : []);
    TP.meta.runs = Array.isArray(TP.meta.runs) ? TP.meta.runs.filter(r => r && typeof r.id === 'string').slice(0, 25) : [];
  }
  const saveMeta = () => storage.write(metaKey, TP.meta);
  const unlocked = s => !s.unlock || s.unlock === 'boss' && TP.meta.boss || s.unlock === 'run' && TP.meta.runs.length > 0;
  const seed = () => { const a = new Uint32Array(1); crypto.getRandomValues(a); return a[0] || 1; };
  const random = () => R.nextRandom(S.tt);
  const sample = a => a.length ? a[Math.min(a.length - 1, Math.floor(random() * a.length))] : null;
  const active = u => !!u && !u.dead && Number.isFinite(u.hp) && u.hp > 0;
  const isBattle = () => S.phase === 'battle' && !!S.battle;
  const mode = () => S.tt?.mode || 'MENU';
  const isPlaying = () => mode() === 'PLAYING' && !$('mainMenu').classList.contains('open');
  function identify(u) { if (u && !u.ttId) { TP.unitSeq = Math.max(TP.unitSeq, S.tt.nextUnitId || 0) + 1; S.tt.nextUnitId = TP.unitSeq; u.ttId = S.tt.id + '-u' + TP.unitSeq; } return u; }
  function discover(u) { if (u && !TP.meta.units.includes(u.n)) { TP.meta.units.push(u.n); saveMeta(); } }
  function setMode(next) { if (!R.MODES.includes(next)) throw new Error('Unknown run mode'); S.tt.mode = next; $('app').dataset.runState = next; }
  function stopLoop() { if (TP.timer !== null) ttCancelTimeout(TP.timer); TP.timer = null; }
  function clearEffects() {
    TP.fx.forEach((_, node) => node.remove()); TP.fx.clear();
    TP.flashes.forEach((entry, node) => entry.classes.forEach(c => node.classList.remove(c))); TP.flashes.clear(); if (TP.fxFrame) cancelAnimationFrame(TP.fxFrame); TP.fxFrame = null;
    qa('.float,.atkFx,.targetRing,.intentTag,.supportLink,.regionBanner,.tt-toast,.nova,.iceBurst,.poisonCloud,.shadowRip,.gearBurst,.holyFlash,.mortarBurst,.auraPulse,.beam,.slash,.spark,.reviveRing,.laneFlash').forEach(node => node.remove());
    window.__bonusDmgQueue = []; window.__dmgDepth = 0;
  }
  function scheduleStep(delay = 250 / TP.speed) {
    if (TP.timer !== null || !isBattle() || !isPlaying()) return;
    const id = S.battle.id;
    TP.timer = ttTimeout(() => { TP.timer = null; loop(id); }, delay);
  }
  function pause() {
    if (!isBattle() || !['PLAYING', 'PAUSED'].includes(mode())) return;
    if (mode() === 'PAUSED') { setMode('PLAYING'); scheduleStep(); }
    else { stopLoop(); setMode('PAUSED'); }
    postRender();
  }
  function setSpeed(n) { if (![1, 2, 3].includes(n)) return; TP.speed = n; storage.write('ttBattleSpeed', n); stopLoop(); scheduleStep(); postRender(); }

  // Cache only formation-dependent queries; HP, targeting, and damage stay live.
  let formationKey = '', formationRecords = [], tagCache = {}, mixedCache = null, roleCache = [];
  const oldCounts = counts, oldMixed = activeMixedSynergies;
  function refreshFormation() {
    // Compare small scalar records instead of allocating a formation string on every damage query.
    if (formationKey && formationRecords.length === S.squad.length && S.squad.every((u, i) => { const r = formationRecords[i]; return !u ? r === null : r?.u === u && r.star === u.star && r.t === u.t && r.tags === u.t.length && r.path === u.ttPath && r.alive === active(u) && r.hp === u.baseMaxHp && r.atk === u.baseAtk && r.spd === u.baseSpd; })) return;
    formationKey = 'ready'; formationRecords = S.squad.map(u => u ? { u, star: u.star, t: u.t, tags: u.t.length, path: u.ttPath, alive: active(u), hp: u.baseMaxHp, atk: u.baseAtk, spd: u.baseSpd } : null); tagCache = oldCounts(); mixedCache = null;
    roleCache = R.synergies(S.squad, isBattle());
  }
  counts = function () { refreshFormation(); return tagCache; };
  activeMixedSynergies = function () { refreshFormation(); if (!mixedCache) mixedCache = oldMixed(); return mixedCache; };
  const synergy = id => { refreshFormation(); return roleCache.find(s => s.id === id); };
  function modifiers(u) {
    const m = { ...R.pathMods(u) };
    (S.tt.eventBonuses || []).forEach(b => { if (R.has(u, b.tag)) Object.entries(b).filter(([k]) => k !== 'tag').forEach(([k, v]) => m[k] = (m[k] || 0) + R.numeric(v)); });
    return m;
  }
  const legacySpawn = spawn, legacyRand = rand;
  rand = function (list) {
    if (!TP.preparingWave || !list.some(v => v?.team === 'enemy')) return legacyRand(list);
    return R.weighted(list, e => TP.waveArchetype.prefer.some(t => R.has(e, t)) ? 4 : 1, random);
  };
  function prepareWave() {
    if (S.tt.finished || S.tt.wavePlan?.round === S.round) return S.tt.wavePlan;
    const previous = S.enemies, threat = S.lastThreat, rg = region();
    const pool = enemyDefs.filter(e => rg.main.includes(e.n) && !e.boss);
    const possible = R.ARCHETYPES.filter(a => !a.prefer.length || pool.some(e => a.prefer.some(t => R.has(e, t))));
    TP.waveArchetype = S.round < 3 ? R.ARCHETYPES[0] : sample(possible) || R.ARCHETYPES[0];
    TP.preparingWave = true;
    try {
      legacySpawn();
      // Later region adapters changed region() without replacing their older spawner.
      // Rebuild a mismatched formation from the current region's authoritative roster.
      const allowed = new Set([...rg.main, ...rg.boss]);
      if (S.enemies.some(e => !allowed.has(e.n.replace(/^Elite\s+/, '')))) {
        const bosses = enemyDefs.filter(e => rg.boss.includes(e.n));
        const bossWave = rg.finalFive ? rg.stage === 10 : S.round % 5 === 0;
        const count = bossWave ? 1 : Math.max(2, Math.min(34, S.enemies.length));
        S.enemies = Array.from({ length: count }, (_, i) => {
          const candidates = bossWave && i === 0 ? bosses : pool;
          if (!candidates.length) throw new Error('Missing enemy definitions for ' + rg.n);
          return mkEnemy(rand(candidates), enemyScale(), bossWave && i === 0);
        });
      }
      if (!S.enemies.length) throw new Error('This region has no available enemy formation.');
      const factor = S.tt.difficulty === 'relaxed' ? .88 : 1;
      const planned = S.enemies.slice(0, 40).map(e => { e.hp = e.maxHp = Math.max(1, Math.round(e.maxHp * factor)); e.atk = Math.max(1, Math.round(e.atk * factor)); return e; });
      S.tt.wavePlan = { round: S.round, region: rg.n, archetype: TP.waveArchetype.id, threat: S.lastThreat, enemies: R.copy(planned) };
    } finally { S.enemies = previous; S.lastThreat = threat; TP.preparingWave = false; }
    return S.tt.wavePlan;
  }
  spawn = function () { const plan = prepareWave(); S.enemies = R.copy(plan.enemies); S.lastThreat = plan.threat; };

  const legacyPickAlly = pickAlly, legacyPickEnemy = pickEnemy;
  pickAlly = function (e) {
    const base = legacyPickAlly(e);
    if (S.tt.difficulty !== 'tactician' || e.rooted > 0 || e.stun > 0 || liveA().some(u => u.taunt > 0)) return base;
    const allies = liveA(), preferred = R.has(e, 'FAST') ? allies.filter(u => R.has(u, 'RANGED', 'SUPPORT')) : e.kind === 'ranged' && !e.focus ? allies.filter(u => !R.has(u, 'ARMORED')) : [];
    return preferred.sort((a, b) => a.hp / a.maxHp - b.hp / b.maxHp)[0] || base;
  };
  pickEnemy = function (u) { if (TP.focusTarget && active(TP.focusTarget) && S.enemies.includes(TP.focusTarget) && ['near', 'random', 'line'].includes(u?.focus)) return TP.focusTarget; return legacyPickEnemy(u); };
  function stat(u) {
    if (!u || u.team !== 'ally') return null;
    identify(u);
    const total = S.tt.stats[u.ttId] ||= { n: u.n, e: u.e, damage: 0, heal: 0, shield: 0, kills: 0 };
    if (!TP.stats.has(u)) TP.stats.set(u, { n: u.n, e: u.e, damage: 0, heal: 0, shield: 0, kills: 0 });
    return [total, TP.stats.get(u)];
  }
  const legacyBonus = bonus;
  bonus = function (u) {
    const b = legacyBonus(u), m = modifiers(u);
    b.spd = (b.spd || 0) + (u.baseSpd || u.spd || 1) * (m.spdMult || 0);
    b.shield = (b.shield || 0) + (m.shield || 0); b.thorns = (b.thorns || 0) + (m.thorns || 0); b.leech = (b.leech || 0) + (m.leech || 0);
    b.chain = Math.min(.75, (b.chain || 0) + (m.chain || 0) + (R.has(u, 'LIGHTNING') ? synergy('chain')?.mods.chain || 0 : 0));
    if (R.has(u, 'RANGED')) b.spd += (u.baseSpd || u.spd || 1) * (synergy('volley')?.mods.spdMult || 0);
    if (R.has(u, 'ARMORED')) b.shield += synergy('shieldwall')?.mods.shield || 0;
    return b;
  };
  const legacyDmg = dmg;
  dmg = function (src, target, amount, type = 'hit') {
    if (!target || target.dead || !Number.isFinite(amount) || amount <= 0 || !(target.team === 'ally' ? S.squad.includes(target) : S.enemies.includes(target))) return 0;
    let value = amount * R.counterMultiplier(src, target, TP.act === src && type === 'burn' ? 'hit' : type);
    if (src?.team === 'ally') {
      const m = modifiers(src);
      value *= 1 + (m.atkMult || 0) + (m.woundedAtk || 0) * (1 - Math.max(0, src.hp) / src.maxHp);
      if (m.hunterAtk && R.has(target, 'RANGED', 'SUPPORT')) value *= 1 + m.hunterAtk;
      value *= 1 + (synergy('command')?.mods.teamAtk || 0);
      if (R.has(src, 'MELEE')) { if (adj(ix(src)).some(a => R.has(a, 'MELEE'))) value *= 1 + (synergy('warband')?.mods.adjacentAtk || 0); if (target.slow > 0) value *= 1 + (synergy('frostedge')?.mods.chilledAtk || 0); }
      if (R.has(src, 'FAST') && target.burn > 0) value *= 1 + (synergy('wildfire')?.mods.burningAtk || 0);
      if (type === 'storm') value *= 1 + (synergy('chain')?.mods.stormAtk || 0);
      if (TP.act === src && !TP.actions.has(src) && !window.__ttSecondaryDamage && !['poison', 'thorn'].includes(type)) { TP.actions.add(src); value *= 1 + (m.opening || 0) + (R.has(src, 'FAST') ? synergy('charge')?.mods.opening || 0 : 0); }
    } else if (target.team === 'ally') {
      const m = modifiers(target);
      if (R.has(target, 'ARMORED')) value *= 1 - (synergy('shieldwall')?.mods.damageDR || 0);
      if (src && R.has(src, 'RANGED')) value *= 1 - (m.rangedDR || 0);
      if (R.protectedBy(S.squad, ix(target), true).length) value *= .94;
    }
    const hp = Math.max(0, target.hp), shield = (target.shield || 0) + (target.goldShield || 0);
    const result = legacyDmg(src, target, Math.min(1e12, value), type);
    target.hp = R.numeric(target.hp, 0, -1e12); target.shield = R.numeric(target.shield);
    const absorbed = Math.max(0, shield - (target.shield || 0) - (target.goldShield || 0));
    const dealt = Math.max(0, hp - Math.max(0, target.hp)) + absorbed;
    stat(src)?.forEach(s => s.damage += dealt); stat(target)?.forEach(s => s.shield += absorbed);
    if (src?.team === 'ally') S.tt.bestHit = Math.max(S.tt.bestHit, dealt);
    return result;
  };
  const legacyHeal = heal;
  heal = function (src, target, amount) {
    if (!active(target) || !Number.isFinite(amount) || amount <= 0) return 0;
    let value = amount;
    if (src?.team === 'ally') { const m = modifiers(src); value *= 1 + (m.healMult || 0) + (target.hp / target.maxHp < .5 ? m.triageHeal || 0 : 0); if (R.has(src, 'HEALER') && R.has(target, 'ARMORED')) value *= 1 + (synergy('hospital')?.mods.healArmored || 0); }
    const before = target.hp, result = legacyHeal(src, target, value);
    stat(src)?.forEach(s => s.heal += Math.max(0, target.hp - before)); return result;
  };
  const legacyKill = kill;
  kill = function (src, target) {
    if (!target || target.dead) return;
    const before = S.coins, killer = src?.team === 'ally' ? src : target.lastHit;
    legacyKill(src, target); if (!target.dead) return;
    formationKey = '';
    if (target.team === 'enemy') {
      if (S.tt.modifier === 'economy') { S.tt.coinRemainder = (S.tt.coinRemainder || 0) + Math.max(0, S.coins - before) * .2; const extra = Math.floor(S.tt.coinRemainder + 1e-9); S.coins += extra; S.tt.coinRemainder -= extra; }
      stat(killer)?.forEach(s => s.kills++);
      if (S.tt.objective?.id === 'melee' && R.has(killer, 'MELEE')) S.tt.objective.progress++;
    } else { TP.deaths.add(target.ttId); target.burn = target.poison = target.slow = 0; }
  };
  const legacyAllyAtk = allyAtk, legacyEnemyAtk = enemyAtk;
  allyAtk = function (u) {
    if (!active(u) || !S.squad.includes(u) || !isBattle() || !isPlaying()) return;
    const target = pickEnemy(u); TP.act = u;
    try { legacyAllyAtk(u); } finally { TP.act = null; }
    if (!active(u)) return;
    const m = modifiers(u);
    if (active(target)) { target.burn = (target.burn || 0) + (m.burn || 0) + (R.has(u, 'FIRE') ? synergy('wildfire')?.mods.burn || 0 : 0); target.poison = (target.poison || 0) + (m.poison || 0); target.slow = Math.min(.6, (target.slow || 0) + (m.slow || 0)); }
    if (m.allyShield) { const ally = adj(ix(u)).filter(active).sort((a, b) => a.hp / a.maxHp - b.hp / b.maxHp)[0]; if (ally) giveShield(ally, m.allyShield); }
  };
  enemyAtk = function (u) { if (active(u) && S.enemies.includes(u) && isBattle() && isPlaying()) return legacyEnemyAtk(u); };

  function temporaryBonuses() {
    S.squad.filter(active).forEach(u => {
      for (const [timer, statName, base, clear] of [['warDrumTicks', 'spd', 'baseSpd'], ['beastHowlTicks', 'atk', 'baseAtk'], ['mageSurgeTicks', 'atk', 'baseAtk'], ['stoneSkinTicks', null, null, 'stoneSkinDR']]) {
        if (u[timer] > 0 && --u[timer] === 0) { if (statName) u[statName] = u[base] || u[statName]; if (clear) u[clear] = 0; }
      }
      if (u.regenBoost > 0) heal(null, u, Math.max(.5, u.regenBoost * .08));
    });
  }
  TP.step = function () {
    if (!isBattle() || !isPlaying()) return false;
    if (!liveA().length) { end(false); return false; } if (!liveE().length) { end(true); return false; }
    const chillBefore = S.tt.modifier === 'frozen' ? new Map([...liveA(), ...liveE()].map(u => [u, u.slow || 0])) : null;
    S.battle.tick++; S.battle.simTime = (S.battle.simTime || 0) + .25; S.tt.steps++; S.battle.lightningCastsThisTick = 0;
    tickBattleAbilities(.25); temporaryBonuses();
    liveE().forEach(e => { if (e.taxTimer > 0 && --e.taxTimer === 0 && liveE().some(b => b.n === 'The Debt King')) S.coins = Math.max(0, S.coins - 2); });
    if (S.battle.tick % 3 === 0) statuses(); regionTick(); evoPulse();
    for (const u of liveA()) {
      if (!active(u) || !liveE().length) continue;
      const b = bonus(u); u.cd = (u.cd || 0) - .25;
      let speed = Math.max(.35, u.spd + (b.spd || 0) - (u.slow || 0));
      if (hasMixed('bloodpack') && hasTag(u, 'Blood', 'Beast') && u.hp / u.maxHp < .55) speed += .12;
      if (hasMixed('stormfreeze') && hasTag(u, 'Frost') && region().id === 'water') speed += .08;
      if (u.cd <= 0) { allyAtk(u); u.cd = Math.max(.38, 1.75 / speed); }
    }
    for (const e of liveE()) {
      if (!active(e) || !liveA().length) continue;
      e.cd = (e.cd || 0) - .25;
      if (e.cd <= 0) { enemyAtk(e); e.cd = Math.max(.45, 1.9 / Math.max(.35, e.spd - (e.slow || 0))); }
    }
    [...S.squad.filter(Boolean), ...S.enemies].forEach(u => { ['hp', 'atk', 'shield', 'burn', 'poison', 'slow', 'spd'].forEach(k => { if (!Number.isFinite(u[k])) u[k] = k === 'spd' ? u.baseSpd || 1 : 0; }); if (chillBefore?.has(u)) u.slow += Math.max(0, u.slow - chillBefore.get(u)) * .2; u.slow = Math.min(.6, Math.max(0, u.slow)); });
    if (!liveA().length) end(false); else if (!liveE().length) end(true); else render();
    return true;
  };
  loop = function (id) {
    if (!isBattle() || !isPlaying() || S.battle.id !== id || TP.timer !== null) return;
    const began = performance.now();
    try { TP.step(); } catch (error) { stopLoop(); setMode('PAUSED'); msg('Combat paused after an error. Your pre-battle checkpoint is safe.'); console.error('Tiny Troops combat error', error); TP.lastError = error.message; }
    scheduleStep(Math.max(4, 250 / TP.speed - (performance.now() - began)));
  };
  const legacyFight = fight;
  fight = function () {
    if (isBattle() || S.tt.finished || ['MENU', 'RESULTS'].includes(mode())) return false;
    if (S.placing || S.placingEffect || S.placingUpgrade || S.placingSpecialization || S.tt.event || S.starChoiceTarget || S.ultimate10Target || S.evoTarget || S.ascTarget) return msg('Finish your current choice before starting the wave.');
    const requiredEvolution = S.squad.find(u => u && maybeEvolution(u)); if (requiredEvolution) return openEvolution(requiredEvolution);
    if (typeof processNextStarChoice === 'function' && processNextStarChoice()) return;
    if (!liveA().length) return msg('Place at least one fighter first.');
    stopLoop(); ttClearTimers(); clearEffects(); S.tt.checkpoint = null; S.tt.draftedRound = S.round;
    S.tt.checkpoint = { state: stableSnapshot() };
    TP.stats = new Map(); TP.deaths = new Set(); TP.actions = new Set(); TP.lastRecap = null; TP.focusTarget = null;
    S.squad.forEach(u => { if (u) { identify(u); discover(u); ['roundDamage', 'roundSupport', 'procActivity', 'silenced', 'suppressed', 'weakened', 'controlGrace', 'healCut', 'roundHealingReceived', 'roundShieldReceived', 'roundProcs', 'delay', 'desynced', 'mercyFatigue', 'formationMarked', 'targetShift'].forEach(k => u[k] = 0); } });
    formationKey = ''; setMode('PLAYING'); const result = legacyFight();
    if (isBattle()) {
      S.battle.simTime = 0;
      S.squad.filter(active).forEach(u => { const m = modifiers(u), i = ix(u); if (m.rowShield) filled(rowIds(rowOf(i))).filter(active).forEach(a => giveShield(a, m.rowShield)); if (R.protectedBy(S.squad, i, true).length) giveShield(u, 8); });
      const eligible = R.OBJECTIVES.filter(o => o.id !== 'protect' || liveA().some(u => R.has(u, 'RANGED')));
      S.tt.objective = S.round % 3 === 0 ? { ...sample(eligible), progress: 0 } : null;
      TP.structures = '';
      // Own the initializer's timer and watchdog so there is exactly one loop.
      ttClearTimers(); stopLoop(); scheduleStep(); render(); saveProfile('battle checkpoint');
      const boss = liveE().find(e => e.boss); if (boss) toast('👑 ' + boss.n, boss.ability || 'Inspect its abilities and counters.');
    }
    return result;
  };
  window.startNextFight = function () { if (mode() === 'PAUSED') return pause(); if (isBattle()) return false; if (S.phase !== 'recruit') return msg('Finish this reward or upgrade first.'); close(); return fight(); };

  // Drafts are bounded and saved with the wave. Reopening a menu is never a reroll.
  const legacyChoose = choose, legacyTapCell = tapCell, legacyChoice = choice;
  function draftCard(def) { return { type: 'recruit', unit: identify(clone(def)), e: def.e, n: def.n, desc: def.a, pick: R.ROLE_INFO[R.role(def)].name }; }
  function buildDraft() {
    const pool = [...new Map(heroes.filter(validHero).map(h => [h.n, h])).values()];
    const starter = R.STARTERS.find(c => c.id === S.tt.starter), occupied = S.squad.filter(Boolean);
    const selected = [], cards = [], first = !occupied.length;
    const weight = h => {
      let w = S._offeredHeroes[h.n] ? 1 : 2;
      if (S.round <= 5 && starter?.tags.some(t => R.has(h, t))) w *= 3;
      if (S.tt.modifier === 'swarm' && R.has(h, 'SWARM')) w *= 4;
      if (S.tt.modifier === 'heroes' && (R.has(h, 'ELITE') || occupied.some(u => u.n === h.n))) w *= 4;
      return w;
    };
    if (first && starter?.units.length) {
      const h = pool.find(h => starter.units.includes(h.n) && h.focus !== 'support' && !h.t.includes('Healer') && h.atk >= 5); if (h) { cards.push(draftCard(h)); selected.push(h.n); }
    }
    const count = first ? 3 : 2;
    while (cards.length < count) {
      const h = R.weighted(pool.filter(h => !selected.includes(h.n) && (!first || isDamageDealer(h))), weight, random);
      if (!h) break; selected.push(h.n); cards.push(draftCard(h));
    }
    if (!first) {
      if (occupied.some(u => !u.ttPath) && (S.round % 4 === 0 || random() < .32)) cards.push({ type: 'tt-path', e: '🔀', n: 'Specialize a troop', desc: 'Choose one troop, then one of three role-specific paths.', pick: '3 PATHS' });
      else if (random() < (S.tt.modifier === 'chaos' ? .7 : .4)) { const ef = sample(effects); cards.push({ type: 'effect', effect: ef, e: ef.e, n: ef.n, desc: ef.desc, pick: 'EFFECT' }); }
      else cards.push({ type: 'upgrade', e: '⭐', n: 'Star Training', desc: 'Choose a troop to advance its next star. Existing evolution and mastery paths still apply.', pick: 'TRAIN' });
    }
    cards.forEach(c => { if (c.unit) S._offeredHeroes[c.n] = 1; });
    return cards;
  }
  choice = function (c, i) {
    if (c.type !== 'recruit') return c.type === 'tt-path' ? `<button class="choice rarity-uncommon" onclick="choose(${i})"><span class="be">${c.e}</span><span><strong>${esc(c.n)}</strong><p>${esc(c.desc)}</p></span><span class="price">PATH</span></button>` : legacyChoice(c, i);
    const u = c.unit, info = R.ROLE_INFO[R.role(u)], owned = S.squad.filter(x => x?.n === u.n).length;
    return `<button class="choice" onclick="choose(${i})"><span class="be">${u.e}</span><span><strong>${esc(u.n)}</strong><p>${info.emoji} ${info.name} · ${u.t.map(t => esc(t)).join(' / ')}<br>${esc(u.a)}<br><b class="tt-impact">${owned ? 'Matching recruit trains your existing troop. ' : ''}${esc(R.recruitImpact(S.squad, u))}</b></p></span><span class="price">${owned ? '×' + owned : 'JOIN'}</span></button>`;
  };
  openRecruit = function () {
    if (!S.tt || S.tt.finished || mode() === 'MENU') return;
    if (S.tt.event) return openEvent();
    if (S.phase !== 'recruit' || S.placing || S.placingEffect || S.placingUpgrade || S.placingSpecialization) return;
    if (S.evoTarget || S.ascTarget || S.starChoiceTarget) return;
    const pendingEvolution = S.squad.find(u => u && maybeEvolution(u));
    if (pendingEvolution) return openEvolution(pendingEvolution);
    if (typeof processNextStarChoice === 'function' && processNextStarChoice()) return;
    prepareWave();
    if (S.tt.draftedRound === S.round) { render(); return msg('Checkpoint restored. Press Fight to replay this wave with the same army.'); }
    if (S.tt.offerRound !== S.round || !S.tt.draft?.length) { S.boost.usedFreeReroll = false; S.tt.offerRound = S.round; S.tt.draft = buildDraft(); }
    S.choices = S.tt.draft;
    show('Build your army', 'Choose one card, then a square. Your next enemy formation is already revealed.', '<div class="choices">' + S.choices.map(choice).join('') + '</div><button class="secondary tt-wide" onclick="reroll()">' + (S.boost.freeReroll && !S.boost.usedFreeReroll ? 'Free reroll' : 'Reroll · 4 🪙') + '</button>');
    render();
  };
  choose = function (i) {
    if (S.phase !== 'recruit' || S.tt.finished || !S.choices[i] || S.placing || S.placingEffect || S.placingUpgrade || S.placingSpecialization) return;
    const c = S.choices[i];
    if (c.type === 'tt-path') { S.placingSpecialization = true; close(); msg('Choose a troop that has no specialization yet.'); render(); }
    else legacyChoose(i);
    S.choices = []; saveProfile('draft choice');
  };
  reroll = function () {
    if (S.phase !== 'recruit' || S.tt.finished || S.placing || S.placingUpgrade || S.placingEffect || S.placingSpecialization) return;
    if (S.boost.freeReroll && !S.boost.usedFreeReroll) S.boost.usedFreeReroll = true;
    else { if (S.coins < 4) return msg('Need 4 coins to reroll.'); S.coins -= 4; }
    S.tt.draft = buildDraft(); openRecruit(); saveProfile('reroll');
  };
  function pathCard(p) { return `<button class="choice rarity-${p.rarity}" data-tt-path="${p.id}"><span class="be">${p.emoji}</span><span><strong>${p.name}</strong><p>${p.desc}</p></span><span class="price">${p.rarity}</span></button>`; }
  tapCell = function (i) {
    if (!Number.isInteger(i) || i < 0 || i >= S.squad.length || S.tt.finished || mode() === 'MENU') return;
    if (S.placingSpecialization) {
      const u = S.squad[i]; if (!u) return msg('Choose a filled square.'); if (u.ttPath) return msg('This troop already has a specialization.');
      S.placingSpecialization = false; S.tt.pathTarget = u.ttId || identify(u).ttId; S.phase = 'tt-path';
      show('Specialize ' + u.e + ' ' + u.n, 'One permanent path for this troop, for this run.', '<div class="choices">' + R.pathsFor(u).map(pathCard).join('') + '</div>'); render(); return;
    }
    return legacyTapCell(i);
  };
  function pickPath(id) {
    if (S.phase !== 'tt-path') return;
    const u = S.squad.find(u => u?.ttId === S.tt.pathTarget), p = R.pathsFor(u).find(p => p.id === id);
    if (!u || u.ttPath || !p) return;
    u.ttPath = id; S.tt.pathTarget = null; S.phase = 'recruit'; formationKey = '';
    if (!TP.meta.paths.includes(id)) { TP.meta.paths.push(id); saveMeta(); }
    close(); msg(u.e + ' ' + u.n + ' chose ' + p.name + '.'); saveProfile('specialization'); fight();
  }
  // Legacy saves may contain high-star troops with an unfinished earlier choice.
  const legacyOpenEvolution = openEvolution;
  openEvolution = function (u) {
    if (u && u.star >= 7 && (!u.evo || !u.ascension)) {
      const star = u.star; u.star = u.evo ? 6 : 5;
      try { return legacyOpenEvolution(u); } finally { u.star = star; }
    }
    return legacyOpenEvolution(u);
  };
  // Earlier evolution handlers hard-started private loops. All choices now use the same scheduler.
  pickEvolution = function (i) {
    const u = S.evoTarget, e = S.evoChoices?.[i]; if (!u || !e || S.phase !== 'evolve') return;
    e.apply(u); window.applySelectedUpgradeBoost?.(u, 5, 'evolution'); u.hp = u.maxHp; u.shield = (u.shield || 0) + 35; S.evoTarget = null; S.evoChoices = []; S.phase = 'recruit';
    close(); msg(u.e + ' ' + u.n + ' became ' + e.n + '.'); saveProfile('evolution'); fight();
  };
  window.pickAscension = function (i) {
    const u = S.ascTarget, a = S.ascChoices?.[i]; if (!u || !a || !['ascend', 'ascension'].includes(S.phase)) return;
    a.apply(u); if (u.star < 6) u.star = 6; if (u.star === 6) u.starProg = 0; u.hp = u.maxHp; u.shield = (u.shield || 0) + 70; S.ascTarget = null; S.ascChoices = []; S.phase = 'recruit';
    close(); msg(u.e + ' ' + u.n + ' chose ' + a.n + '.'); saveProfile('mastery'); fight();
  };
  function openEvent() {
    if (!S.tt.event || S.tt.finished || mode() === 'MENU') return;
    const e = R.EVENTS.find(e => e.id === S.tt.event.id); if (!e) { S.tt.event = null; return openRecruit(); }
    S.phase = 'tt-event';
    show(e.emoji + ' ' + e.name, e.desc, '<div class="choices">' + e.options.map(o => `<button class="choice" data-tt-event="${o.id}"><span class="be">${o.coins ? '🪙' : e.emoji}</span><span><strong>${o.name}</strong><p>${o.desc}</p></span></button>`).join('') + '</div>'); render();
  }
  function pickEvent(id) {
    if (S.phase !== 'tt-event' || !S.tt.event) return;
    const e = R.EVENTS.find(e => e.id === S.tt.event.id), o = e?.options.find(o => o.id === id); if (!o) return;
    if (o.coins) S.coins += o.coins;
    if (o.bonus) { const old = S.tt.eventBonuses.find(b => b.tag === o.bonus.tag && Object.keys(o.bonus).every(k => k === 'tag' || k in b)); if (old) Object.keys(o.bonus).filter(k => k !== 'tag').forEach(k => old[k] += o.bonus[k]); else S.tt.eventBonuses.push({ ...o.bonus }); }
    S.tt.event = null; S.phase = 'recruit'; close(); msg(e.name + ': ' + o.name + '.'); saveProfile('event'); openRecruit();
  }
  const legacyEnd = end;
  end = function (win) {
    if (!isBattle() || S.battle.ttEnded) return;
    S.battle.ttEnded = true;
    const wave = S.round, tick = S.battle.tick, objective = S.tt.objective;
    const complete = win && objective && (objective.id === 'protect' ? !S.squad.some(u => u && R.has(u, 'RANGED') && TP.deaths.has(u.ttId)) : objective.id === 'melee' ? objective.progress >= objective.target : tick <= objective.target);
    const ranking = [...TP.stats.values()].sort((a, b) => b.damage + b.heal + b.shield - a.damage - a.heal - a.shield);
    TP.lastRecap = { wave, win, seconds: (tick * .25).toFixed(1), mvp: ranking[0], objective: complete ? objective.name : null };
    stopLoop(); ttClearTimers(); clearEffects(); S.tt.checkpoint = null; S.tt.wavePlan = null; S.tt.draft = null; S.tt.offerRound = null; formationKey = '';
    if (complete) S.coins += objective.reward;
    setMode(win ? 'VICTORY' : 'DEFEAT');
    legacyEnd(win);
    if (!win) return finishRun(wave);
    if (wave % 5 === 0 && !TP.meta.boss) { TP.meta.boss = true; saveMeta(); toast('✨ The Arcanist unlocked', 'A new starting army is available for your next run.'); }
    if (wave % 3 === 0 && wave % 5 !== 0 && S.tt.lastEventRound !== wave) { S.tt.lastEventRound = wave; S.tt.event = { id: sample(R.EVENTS).id }; S.phase = 'tt-event'; }
    S.battle = null; setMode('PLAYING'); render(); saveProfile('wave complete');
    // Waves have no limit. The campaign keeps escalating after the last region.
  };
  function finishRun(wave) {
    stopLoop(); ttClearTimers(); clearEffects(); S.tt.finished = true; S.tt.outcome = 'defeat'; S.tt.checkpoint = null; S.phase = 'dead'; S.battle = null; setMode('DEFEAT');
    const values = Object.values(S.tt.stats), mvp = values.slice().sort((a, b) => b.damage + b.heal + b.shield - a.damage - a.heal - a.shield)[0], favorite = values.slice().sort((a, b) => b.kills - a.kills)[0];
    S.tt.summary = { id: S.tt.id, date: new Date().toISOString(), result: 'Defeat', wave, style: R.armyStyle(S.squad), size: S.squad.filter(Boolean).length, difficulty: S.tt.difficulty, modifier: S.tt.modifier, synergies: R.synergies(S.squad).filter(s => s.tier).map(s => ({ id: s.id, name: s.name, emoji: s.emoji, tier: s.tier })), mvp, favorite, bestHit: S.tt.bestHit };
    if (!TP.meta.runs.some(r => r.id === S.tt.id)) { TP.meta.runs.unshift(S.tt.summary); TP.meta.runs = TP.meta.runs.slice(0, 25); saveMeta(); }
    setMode('RESULTS'); render(); saveProfile('run results'); openResults(S.tt.summary);
  }
  function summaryHtml(s) {
    return `<div class="tt-result"><div class="tt-result-icon">${s.result === 'Victory' ? '🏆' : '🌙'}</div><p class="tt-eyebrow">${esc(s.result)} · Wave ${s.wave}</p><h3>${esc(s.style)}</h3><p>${s.size} troops · ${esc(s.difficulty)} · ${esc(R.MODIFIERS.find(m => m.id === s.modifier)?.name || 'Open Campaign')}</p><div class="tt-chips">${(s.synergies || []).map(a => `<span>${a.emoji} ${esc(a.name)} ${a.tier}</span>`).join('')}</div><div class="tt-result-stats"><p><b>Most valuable</b><br>${s.mvp ? `${s.mvp.e} ${esc(s.mvp.n)}<br>${Math.round(s.mvp.damage)} damage · ${Math.round(s.mvp.heal)} healing · ${Math.round(s.mvp.shield)} blocked` : '—'}</p><p><b>Favorite troop</b><br>${s.favorite ? `${s.favorite.e} ${esc(s.favorite.n)} · ${s.favorite.kills} kills` : '—'}</p><p><b>Largest hit</b><br>${Math.round(s.bestHit || 0)}</p></div></div>`;
  }
  function openResults(s) { show('Your army’s story', 'Endless campaigns end in defeat. Every build opens another possibility.', summaryHtml(s) + '<button class="tt-wide" data-tt-action="new">Try another army →</button><button class="secondary tt-wide" data-tt-action="history">Run history</button>'); }
  function openHistory() {
    show('Recent armies', 'The last 25 completed runs on this device, for this save name.', TP.meta.runs.length ? '<div class="choices">' + TP.meta.runs.map((s, i) => `<button class="choice" data-tt-history="${i}"><span class="be">${s.result === 'Victory' ? '🏆' : '🌙'}</span><span><strong>${esc(s.style)}</strong><p>${esc(s.difficulty)} · ${new Date(s.date).toLocaleDateString()}</p></span><span class="price">Wave ${s.wave}</span></button>`).join('') + '</div>' : '<p>Finish a run to record your first army.</p>');
  }
  openRelic = function () {
    if (S.tt.finished || mode() === 'MENU' || S.phase !== 'relic') return;
    const pool = relics.filter(r => !S.relics.includes(r.id));
    if (!pool.length) { S.phase = 'shop'; return openShop(); }
    if (!S.tt.relicDraft?.length) { const options = pool.slice(); S.tt.relicDraft = []; while (S.tt.relicDraft.length < 3 && options.length) { const r = sample(options); S.tt.relicDraft.push(r.id); options.splice(options.indexOf(r), 1); } }
    S.relicChoices = S.tt.relicDraft.map(id => pool.find(r => r.id === id)).filter(Boolean);
    show('Boss relic', 'Choose one free relic for this run, then visit the store.', '<div class="choices">' + S.relicChoices.map((r, i) => `<button class="choice" onclick="pickRelic(${i})"><span class="be">${r.e}</span><span><strong>${esc(r.n)}</strong><p>${esc(r.desc)}</p></span><span class="price">FREE</span></button>`).join('') + '</div>'); saveProfile('relic draft'); render();
  };
  pickRelic = function (i) {
    if (S.phase !== 'relic' || !S.relicChoices[i]) return;
    const r = S.relicChoices[i]; if (S.relics.includes(r.id)) return;
    r.take(); S.relics.push(r.id); S.relicChoices = []; S.tt.relicDraft = null; S.phase = 'shop'; close(); msg('Relic gained: ' + r.e + ' ' + r.n); saveProfile('relic'); openShop();
  };
  const legacyShop = openShop;
  openShop = function () {
    if (S.tt.finished || mode() === 'MENU') return;
    const result = legacyShop();
    if (S.tt.modifier === 'economy' && S.items?.length && S.phase === 'shop') S.items.forEach((it, i) => { const key = Number.isFinite(it.c) ? 'c' : 'cost'; if (!it.ttPriced) { it[key] = Math.ceil(it[key] * 1.15); it.ttPriced = true; } const el = qa('#body .choice')[i]?.querySelector('.price'); if (el) el.textContent = it[key] + ' 🪙'; });
    return result;
  };
  const freshState = stableSnapshot(), legacyReset = reset;
  reset = function () {
    const battleSeq = (S._battleSeq || 0) + 1;
    stopLoop(); ttClearTimers(); clearEffects(); loadMeta();
    const config = { starter: $('ttStarter')?.value, modifier: $('ttModifier')?.value, difficulty: $('ttDifficulty')?.value };
    if (!unlocked(R.STARTERS.find(s => s.id === config.starter) || {})) config.starter = 'defender';
    if (!unlocked(R.MODIFIERS.find(s => s.id === config.modifier) || {})) config.modifier = 'standard';
    Object.keys(S).forEach(k => delete S[k]); Object.assign(S, R.copy(freshState)); S.comboCells = new Set(); N = 16; S._battleSeq = battleSeq; S.tt = R.newRun(config, seed());
    TP.stats.clear(); TP.deaths.clear(); TP.actions.clear(); TP.unitSeq = 0; TP.lastRecap = null; TP.focusTarget = null; TP.structures = ''; formationKey = ''; forceClose();
    S.tt.started = true; TP.suspendedDrawer = null; setMode('PLAYING'); legacyReset(); setMode('PLAYING'); render();
  };
  TP.beforeRestore = function () { stopLoop(); clearEffects(); TP.stats.clear(); TP.deaths.clear(); TP.actions.clear(); formationKey = ''; TP.structures = ''; TP.lastRecap = null; TP.focusTarget = null; forceClose(); };
  TP.afterRestore = function () {
    loadMeta(); TP.unitSeq = Math.max(S.tt.nextUnitId || 0, ...[...Object.keys(S.tt.stats), ...[...S.squad, ...S.bench, ...(S.tt.draft || []).map(c => c.unit)].filter(Boolean).map(u => u.ttId)].map(id => Number(String(id || '').split('-u').at(-1)) || 0)); S.tt.nextUnitId = TP.unitSeq;
    S.squad.filter(Boolean).forEach(u => { identify(u); discover(u); }); S.tt.started = true; S.tt.finished = !!S.runEnded || !!S.tt.finished; setMode(S.tt.finished ? 'RESULTS' : 'PLAYING');
    if (S.tt.finished) ttTimeout(() => openResults(S.tt.summary || { result: 'Defeat', wave: S.round, style: R.armyStyle(S.squad), size: S.squad.filter(Boolean).length }), 90);
    else if (S.tt.event) ttTimeout(openEvent, 90); render();
  };

  // Menus never start or restart combat. Closing them restores the same battle.
  let lastFocus = null;
  const legacyShow = show, legacyClose = close, legacyDetailHtml = detailHtml;
  const forcedChoice = () => !!(S.tt?.event || S.phase === 'tt-path' || S.phase === 'relic' || S.evoTarget || S.ascTarget || S.starChoiceTarget || S.ultimate10Target);
  function forceClose() { S.inspect = null; $('shade').classList.remove('open'); $('drawer').classList.remove('open'); $('drawer').setAttribute('aria-hidden', 'true'); }
  show = function (title, hint, html) {
    lastFocus = document.activeElement; S.inspect = null; legacyShow(title, hint, html);
    $('drawer').setAttribute('role', 'dialog'); $('drawer').setAttribute('aria-modal', 'true'); $('drawer').setAttribute('aria-labelledby', 'dt'); $('drawer').setAttribute('aria-hidden', 'false'); $('drawer').scrollTop = 0;
    $('close').disabled = forcedChoice(); $('close').title = forcedChoice() ? 'Complete this choice to continue' : 'Close';
    q('#body button, #body input, #close')?.focus({ preventScroll: true });
  };
  close = function () {
    if (forcedChoice()) return msg('Finish this choice to continue.');
    legacyClose(); $('drawer').setAttribute('aria-hidden', 'true'); lastFocus?.isConnected && lastFocus.focus({ preventScroll: true });
    if (TP.pauseForDialog) { TP.pauseForDialog = false; if (mode() === 'PAUSED') pause(); }
  };
  openMainMenu = function () {
    if (S.tt?.started && window.TinyTroopsSaveFiles?.active()) saveProfile('menu checkpoint');
    if ($('drawer').classList.contains('open') && forcedChoice()) TP.suspendedDrawer = { title: $('dt').textContent, hint: $('dhint').textContent, html: $('body').innerHTML };
    stopLoop(); ttClearTimers(); clearEffects(); TP.pauseForDialog = false; forceClose(); setMode('MENU'); $('mainMenu').classList.add('open');
    $('ttResume').hidden = !S.tt.started || S.tt.finished; menuOptions(); refreshTop10(); window.dispatchEvent(new CustomEvent('tt-saves-changed'));
  };
  function resumeRun() {
    if (!S.tt.started || S.tt.finished) return;
    $('mainMenu').classList.remove('open'); setMode('PLAYING');
    if (isBattle()) scheduleStep();
    else if (TP.suspendedDrawer && forcedChoice()) { const d = TP.suspendedDrawer; TP.suspendedDrawer = null; show(d.title, d.hint, d.html); }
    else if (S.tt.event) openEvent(); else if (S.phase === 'shop') openShop(); else if (S.phase === 'relic') openRelic(); else openRecruit();
    render();
  }
  openRunMenu = function () {
    if (forcedChoice()) return msg('Finish this choice before opening the run menu.');
    if (isBattle() && mode() === 'PLAYING') { pause(); TP.pauseForDialog = true; }
    show('Your campaign', 'Wave ' + S.round + ' · Endless · ' + R.armyStyle(S.squad), '<div class="runMenuGrid"><button data-tt-action="resume">Continue</button><button class="secondary" onclick="saveProfile(\'manual\')">Save run</button><button class="secondary" data-tt-action="book">Army Book</button><button class="secondary" data-tt-action="history">Run history</button><button class="secondary" data-tt-action="new">Choose a new army</button><button class="secondary" data-tt-action="new">Browse saved runs</button></div>');
  };
  function menuOptions() {
    if (!$('ttStarter')) return;
    loadMeta();
    for (const [id, list] of [['ttStarter', R.STARTERS], ['ttModifier', R.MODIFIERS]]) {
      const before = $(id).value;
      $(id).innerHTML = list.map(s => `<option value="${s.id}" ${unlocked(s) ? '' : 'disabled'}>${s.emoji} ${s.name}${unlocked(s) ? '' : ' · unlock after ' + (s.unlock === 'boss' ? 'a boss' : 'a run')}</option>`).join('');
      if (list.some(s => s.id === before && unlocked(s))) $(id).value = before;
    }
    updateConfigHint();
  }
  function updateConfigHint() {
    $('ttConfigHint').textContent = (R.STARTERS.find(s => s.id === $('ttStarter').value)?.desc || '') + ' ' + (R.MODIFIERS.find(s => s.id === $('ttModifier').value)?.desc || '') + ' All campaigns are endless.';
  }
  function setupUi() {
    document.body.classList.add('tt-modern');
    const link = document.createElement('link'); link.rel = 'stylesheet'; link.href = 'tiny-troops/ui.css?v=20261006-saves'; document.head.appendChild(link);
    q('.title').textContent = 'Tiny Troops'; q('.menuTitle').textContent = 'Tiny Troops'; q('.menuSub').textContent = 'Small armies. Unexpected combinations. Endless waves.';
    $('menuUser').setAttribute('aria-label', 'Save name'); $('menuPass').setAttribute('aria-label', 'Save code'); $('menuNew').textContent = 'Start endless run →'; $('menuLoad').textContent = 'Load saved run';
    const settings = document.createElement('div'); settings.className = 'tt-run-settings'; settings.innerHTML = '<label>Choose your commander<select id="ttStarter"></select></label><div class="tt-setting-row"><label>Run rules<select id="ttModifier"></select></label><label>Enemy tactics<select id="ttDifficulty"><option value="standard">Standard</option><option value="relaxed">Relaxed · gentler opponents</option><option value="tactician">Tactician · smarter targeting</option></select></label></div><p id="ttConfigHint"></p>';
    q('.menuInputs').before(settings);
    const resume = document.createElement('button'); resume.id = 'ttResume'; resume.className = 'secondary'; resume.textContent = 'Resume current run'; resume.hidden = true; resume.onclick = resumeRun; q('.menuButtons').before(resume);
    const intel = document.createElement('section'); intel.id = 'ttIntel'; intel.innerHTML = '<div class="tt-control-row"><div><span class="tt-eyebrow">ENDLESS CAMPAIGN</span><b id="ttArmySize"></b></div><div class="tt-speeds" aria-label="Battle speed"><button data-tt-speed="1">1×</button><button data-tt-speed="2">2×</button><button data-tt-speed="3">3×</button><button id="ttPause" data-tt-action="pause">Pause</button><button class="secondary" data-tt-action="more" aria-label="More options">•••</button></div></div><div id="ttWavePreview"></div><div id="ttObjective"></div>';
    q('.main').prepend(intel);
    const syn = document.createElement('section'); syn.id = 'ttSynergies'; syn.setAttribute('aria-label', 'Army synergies'); $('traits').before(syn);
    const recap = document.createElement('div'); recap.id = 'ttRecap'; q('footer').prepend(recap);
    $('msg').setAttribute('role', 'status'); $('msg').setAttribute('aria-live', 'polite'); $('msg').setAttribute('aria-atomic', 'true'); $('menuStatus').setAttribute('role', 'status');
    $('close').onclick = close; $('shade').onclick = close; $('restart').onclick = openRunMenu; $('shop').onclick = openShop; $('help').onclick = help; $('recruit').onclick = () => S.tt.finished ? openMainMenu() : openRecruit();
    if ($('playNext')) { $('playNext').onclick = window.startNextFight; $('playNext').title = 'Start the next wave'; }
    ['ttStarter', 'ttModifier', 'ttDifficulty'].forEach(id => $(id).addEventListener('change', updateConfigHint)); $('menuUser').addEventListener('change', menuOptions);
  }
  detailHtml = function (u) {
    const info = R.ROLE_INFO[R.role(u)], b = u.team === 'ally' ? bonus(u) : {}, paths = R.PATHS.find(p => p.id === u.ttPath), protectedUnit = u.team === 'ally' && R.protectedBy(S.squad, ix(u), isBattle()).length;
    return `<div class="tt-unit-guide"><span class="tt-guide-emoji">${u.e}</span><div><span class="tt-eyebrow">${info.emoji} ${info.name}</span><h3>${esc(heroTitle(u))}</h3><p>${esc(u.a || u.ability || '')}</p></div></div><div class="detailStats"><div class="stat">Health<b>${Math.max(0, Math.ceil(u.hp))} / ${Math.ceil(u.maxHp)}</b></div><div class="stat">Damage<b>${Math.round(u.atk + (b.atk || 0))}</b></div><div class="stat">Attack tempo<b>${(Math.max(.35, u.spd + (b.spd || 0) - (u.slow || 0)) / (u.team === 'ally' ? 1.75 : 1.9)).toFixed(2)}/s</b></div></div><div class="tt-chips">${R.tags(u).map(t => `<span>${t.toLowerCase()}</span>`).join('')}${protectedUnit ? '<span>🛡️ Protected in this row · −6% incoming damage</span>' : ''}</div><p><b>Strong against</b> ${info.strong}<br><b>Watch out for</b> ${info.weak}<br><b>Build with</b> ${info.partner}</p><p class="hint">${info.tip}</p>${paths ? `<p class="tt-impact">${paths.emoji} ${paths.name}: ${paths.desc}</p>` : ''}<details class="tt-advanced"><summary>Abilities, stars, and equipment</summary>${legacyDetailHtml(u)}</details>`;
  };
  detail = function (u) {
    if (!u) return; show(u.e + ' ' + heroTitle(u), u.team === 'ally' ? 'Your troop · live stats' : 'Enemy · inspect its abilities and counters', detailHtml(u)); S.inspect = u;
  };
  refreshInspect = function () { /* Updated once per simulation step below, without replacing the board. */ };
  function synergyDescription(s) {
    const requirement = s.tag ? `${s.count} / ${s.next || s.tiers.at(-1)} ${s.tag.toLowerCase()} troops${s.next ? ' toward the next tier' : ' · maximum tier'}` : Object.entries(s.req).map(([t, n]) => `${R.countTags(S.squad)[t] || 0}/${n} ${t.toLowerCase()}`).join(' · ') + (s.distinctRoles ? ` · ${s.roles}/${s.distinctRoles} different roles` : '');
    return `<p>${requirement}</p><p>${s.bonus}</p><p class="hint">${s.tier ? 'Active tier ' + s.tier : 'Recruit the missing roles to activate this combination.'}</p>`;
  }
  function openSynergies(id) {
    const list = R.synergies(S.squad, isBattle()), s = list.find(s => s.id === id);
    if (s) return show(s.emoji + ' ' + s.name, s.tier ? 'Active · Tier ' + s.tier : 'Build toward this synergy', synergyDescription(s));
    show('Army synergies', 'Role synergies count troops. Original traits count star strength. Position matters.', '<div class="choices">' + list.map(s => `<button class="choice" data-tt-synergy="${s.id}"><span class="be">${s.emoji}</span><span><strong>${s.name}${s.tier ? ' · Tier ' + s.tier : ''}</strong><p>${s.tag ? s.count + '/' + (s.next || s.tiers.at(-1)) + ' ' + s.tag.toLowerCase() : s.missing.filter(([, n]) => n).map(([t, n]) => n + ' ' + t.toLowerCase()).join(' · ') || 'Role requirements met'}</p></span></button>`).join('') + '</div><details class="tt-advanced"><summary>Original traits and formations</summary><div class="tt-chips">' + Object.entries(counts()).map(([t, n]) => `<span>${TE[t] || ''} ${esc(t)} · ${n}★</span>`).join('') + '</div>' + (S.combos || []).map(c => `<p><b>${c.e} ${esc(c.n)}</b><br>${esc(c.desc)}</p>`).join('') + activeMixedSynergies().map(m => `<p><b>${m.icon} ${esc(m.name)} ${m.tier > 1 ? 'II' : ''}</b><br>${esc(m.desc || '')}</p>`).join('') + '</details>');
  }
  function openBook() {
    show('Army Book', TP.meta.units.length + ' troops discovered · options unlock through play', '<label class="tt-search">Find a discovered troop<input id="ttBookSearch" placeholder="Name, role, or element" autocomplete="off"></label><div id="ttBookEntries"></div><details class="tt-advanced"><summary>Discovered upgrades & synergies</summary>' + R.PATHS.filter(p => TP.meta.paths.includes(p.id)).map(p => `<p>${p.emoji} <b>${p.name}</b> · ${p.desc}</p>`).join('') + R.SYNERGIES.filter(s => TP.meta.synergies.includes(s.id)).map(s => `<p>${s.emoji} <b>${s.name}</b> · ${s.bonus}</p>`).join('') + '</details>');
    bookEntries(''); $('ttBookSearch').addEventListener('input', e => bookEntries(e.target.value));
  }
  function bookEntries(search) {
    const roster = [...new Map(heroes.filter(validHero).map(h => [h.n, h])).values()], known = roster.filter(h => TP.meta.units.includes(h.n)), query = search.toLowerCase();
    $('ttBookEntries').innerHTML = known.filter(h => (h.n + R.tags(h).join(' ') + R.ROLE_INFO[R.role(h)].name).toLowerCase().includes(query)).map(h => { const info = R.ROLE_INFO[R.role(h)]; return `<details class="tt-book-entry"><summary>${h.e} ${esc(h.n)} <span>${info.name}</span></summary><p>${esc(h.a)}</p><p><b>Strong:</b> ${info.strong}<br><b>Weak:</b> ${info.weak}<br><b>Partners:</b> ${info.partner}</p><p class="hint">${R.tags(h).join(' · ')}<br>${R.SYNERGIES.filter(s => s.tag && R.has(h, s.tag) || s.req && Object.keys(s.req).some(t => R.has(h, t))).map(s => s.name).join(' · ')}</p></details>`; }).join('') + `<p class="hint">??? · ${Math.max(0, roster.length - known.length)} troops left to discover</p>`;
  }
  help = function () { show('Tiny Troops, explained', 'An endless strategy campaign on a stationary formation grid.', '<ol class="tt-help"><li>Choose a commander and one card per wave. Your first recruit can deal damage.</li><li>Place defenders on the right and fragile ranged troops behind them in the same row. Units stay in their squares.</li><li>Draft matching roles to activate synergies. Matching troop recruits train an existing troop; separate squares build larger formations.</li><li>Specializations offer three paths with tradeoffs. Stars, effects, evolutions, mentors, relics, and spells retain their original mechanics.</li><li>Inspect the exact incoming enemy formation. Boss waves offer a relic and a store visit after winning.</li><li>Every third non-boss victory brings a quick event. Optional challenges reward 20 coins.</li><li>Pause or change speed at any time. 1×, 2×, and 3× use identical simulation steps. Space pauses combat.</li><li>Save with a name and code. Saving during combat returns to the same pre-battle checkpoint on reload; it never duplicates combat rewards.</li><li>There is no final wave. Enemies continue scaling after the last region. Defeat records your army, then you can try another build.</li></ol><button class="secondary tt-wide" data-tt-action="counters">See the matchups</button>'); };
  function openCounters() { show('Soft counters', 'Small bonuses reward good matchups. Composition, stars, and original elemental weaknesses still matter.', '<div class="choices">' + R.COUNTERS.map(c => `<div class="tt-counter"><b>${c.label}</b></div>`).join('') + '</div><p class="hint">Magic can punish armor. Defenders shelter ranged allies in the same row. Fast troops use burst and attack tempo; troops do not move.</p><button class="secondary tt-wide" data-tt-action="elements">Class &amp; element rules</button>'); }

  function addFx(node, life = 550) {
    if (matchMedia('(prefers-reduced-motion: reduce)').matches || !isBattle() || mode() !== 'PLAYING') return;
    while (TP.fx.size >= 32) { const old = TP.fx.keys().next().value; old.remove(); TP.fx.delete(old); }
    document.body.appendChild(node); TP.fx.set(node, performance.now() + life); if (!TP.fxFrame) TP.fxFrame = requestAnimationFrame(expireFx);
  }
  function expireFx(now) { TP.fxFrame = null; TP.fx.forEach((until, node) => { if (until <= now) { node.remove(); TP.fx.delete(node); } }); TP.flashes.forEach((entry, node) => { if (entry.until <= now) { entry.classes.forEach(c => node.classList.remove(c)); TP.flashes.delete(node); } }); if (TP.fx.size || TP.flashes.size) TP.fxFrame = requestAnimationFrame(expireFx); }
  TP.addLegacyFx = function (node, life) { if (node.classList.contains('regionBanner') || TP.fx.size >= 24) return; node.classList.add('tt-legacy-impact'); addFx(node, Math.min(life, 600)); };
  TP.legacyFlash = function (el, cls, life = 180) { if (!el || !isBattle() || !isPlaying()) return; el.classList.add(cls); const previous = TP.flashes.get(el); TP.flashes.set(el, { classes: new Set([...(previous?.classes || []), cls]), until: performance.now() + life }); if (!TP.fxFrame) TP.fxFrame = requestAnimationFrame(expireFx); };
  flash = function (u, i, cls) { TP.legacyFlash(elForUnit(u), cls); };
  beam = function (a, b) { const from = S.enemies[a], to = S.enemies[b]; if (from && to) attackVisual(from, to, 'storm'); };
  const originalElement = elForUnit;
  elForUnit = function (u) { if (!u) return null; return (u.team === 'ally' ? TP.allyNodes?.[ix(u)] : TP.enemyNodes?.[S.enemies.indexOf(u)]) || originalElement(u); };
  floatAlly = function (i, text, color) { placeFloat(TP.allyNodes?.[i], text, color); };
  floatEnemy = function (i, text, color) { placeFloat(TP.enemyNodes?.[i], text, color); };
  attackVisual = function (src, target, type = 'hit') {
    if (!active(target) || TP.fx.size > 24) return;
    const from = elForUnit(src), to = elForUnit(target); if (!from || !to) return;
    const a = centerOf(from), b = centerOf(to), d = document.createElement('i'); d.className = 'tt-projectile tt-' + (effectLabel(type) || 'hit'); d.style.left = a.x + 'px'; d.style.top = a.y + 'px'; d.style.setProperty('--tx', b.x - a.x + 'px'); d.style.setProperty('--ty', b.y - a.y + 'px'); addFx(d, 450);
  };
  placeFloat = function (el, text, color) {
    if (!el || TP.fx.size > 22 || !isBattle()) return;
    const now = performance.now(), last = Number(el.dataset.ttFloatAt || 0); if (now - last < 200 / TP.speed) return;
    el.dataset.ttFloatAt = now; const p = centerOf(el), d = document.createElement('span'); d.className = 'tt-combat-number'; d.textContent = text; d.style.color = color; d.style.left = p.x + 'px'; d.style.top = p.y + 'px'; addFx(d, 650);
  };
  bossVfx = function (kind, el) { if (!el) return; const p = centerOf(el), d = document.createElement('i'); d.className = 'tt-impact-flash'; d.style.left = p.x + 'px'; d.style.top = p.y + 'px'; addFx(d, 400); };
  orbit = function () { $('enemyOrbit').replaceChildren(); };
  showTargetRing = function () {}; intentTag = function () {}; supportLine = function () {}; flashLaneForEnemy = function () {};
  function toast(title, text) {
    qa('.tt-toast').forEach(n => n.remove()); const d = document.createElement('div'); d.className = 'tt-toast'; d.setAttribute('role', 'status'); d.innerHTML = '<b>' + esc(title) + '</b><span>' + esc(text) + '</span>'; document.body.appendChild(d); ttTimeout(() => d.remove(), 2500);
  }
  const legacyRender = render;
  function updateCard(el, u, ally) {
    if (!el || !u) return;
    const hp = q(ally ? '.hp i' : '.ehp i', el), sh = q('.sh i', el); if (hp) hp.style.width = Math.max(0, Math.min(100, u.hp / u.maxHp * 100)) + '%'; if (sh) sh.style.width = Math.max(0, Math.min(100, (u.shield || 0) / (u.maxHp * .7) * 100)) + '%';
    el.classList.toggle(ally ? 'deadAlly' : 'dead', !active(u));
    let status = q('.tt-status', el); if (!status) { status = document.createElement('span'); status.className = 'tt-status'; el.appendChild(status); } status.textContent = [u.burn > 0 ? '🔥' : '', u.poison > 0 ? '☠️' : '', u.slow > 0 ? '❄️' : '', u.stun > 0 ? '💫' : '', u.shield > 0 ? '🛡️' : ''].filter(Boolean).slice(0, 3).join('');
    let badge = q('.tt-role', el); if (!badge) { badge = document.createElement('span'); badge.className = 'tt-role'; badge.textContent = R.ROLE_INFO[R.role(u)].emoji; el.appendChild(badge); }
    el.title = `${u.n} · ${R.ROLE_INFO[R.role(u)].name}\nHP ${Math.max(0, Math.ceil(u.hp))}/${Math.ceil(u.maxHp)} · ${Math.round(u.atk)} damage\n${R.ROLE_INFO[R.role(u)].strong}\nWeak: ${R.ROLE_INFO[R.role(u)].weak}`;
    el.setAttribute('aria-label', el.title.replace(/\n/g, '. '));
  }
  function postRender() {
    if (!$('ttArmySize') || !S.tt) return;
    $('ttArmySize').textContent = S.squad.filter(Boolean).length + ' troops · ' + R.armyStyle(S.squad);
    qa('[data-tt-speed]').forEach(b => { b.classList.toggle('selected', Number(b.dataset.ttSpeed) === TP.speed); b.setAttribute('aria-pressed', String(Number(b.dataset.ttSpeed) === TP.speed)); });
    $('ttPause').disabled = !isBattle() || mode() === 'MENU'; $('ttPause').textContent = mode() === 'PAUSED' ? '▶ Resume' : 'Ⅱ Pause'; $('ttPause').setAttribute('aria-pressed', String(mode() === 'PAUSED'));
    const plan = S.tt.wavePlan, a = R.ARCHETYPES.find(a => a.id === plan?.archetype), boss = (isBattle() ? S.enemies : plan?.enemies || []).find(e => e.boss);
    const preview = `<span class="tt-eyebrow">${isBattle() ? 'FIGHTING' : 'UP NEXT'} · WAVE ${S.round} · ${esc(region().n)}</span><b>${boss ? '👑 ' + esc(boss.n) : a ? a.emoji + ' ' + a.name : 'The next wave'}</b><span>${esc(boss?.ability || a?.counter || '')}</span>`;
    if ($('ttWavePreview').innerHTML !== preview) $('ttWavePreview').innerHTML = preview;
    const o = S.tt.objective; $('ttObjective').hidden = !o || !isBattle(); if (o) $('ttObjective').textContent = '🎯 ' + o.desc + (o.id === 'melee' ? ' ' + o.progress + '/' + o.target : '') + ' · +20 🪙';
    const syns = R.synergies(S.squad, isBattle()), visible = syns.filter(s => s.tier || s.tag && s.count).sort((a, b) => b.tier - a.tier || (a.next || 99) - a.count - ((b.next || 99) - b.count)).slice(0, 4);
    const html = visible.map(s => `<button class="tt-synergy ${s.tier ? 'active' : ''}" data-tt-synergy="${s.id}" title="${esc(s.bonus)}">${s.emoji} ${s.name} <b>${s.tier ? ['I', 'II', 'III'][s.tier - 1] : s.count + '/' + s.next}</b></button>`).join('') + '<button class="tt-synergy" data-tt-action="synergies">All synergies ↗</button>';
    if ($('ttSynergies').innerHTML !== html) $('ttSynergies').innerHTML = html;
    if (mode() !== 'MENU') syns.filter(s => s.tier > (S.tt.seenSynergies[s.id] || 0)).forEach(s => { S.tt.seenSynergies[s.id] = s.tier; if (!TP.meta.synergies.includes(s.id)) { TP.meta.synergies.push(s.id); saveMeta(); } toast(s.emoji + ' ' + s.name + ' activated · Tier ' + s.tier, s.bonus); });
    $('pick').classList.toggle('show', !!(S.placing || S.placingEffect || S.placingUpgrade || S.placingSpecialization)); if (S.placingSpecialization) $('pick').textContent = '🔀 Choose a troop to specialize';
    $('phase').textContent = mode() === 'PAUSED' ? 'Paused · the battlefield will wait.' : S.placingSpecialization ? 'Choose a troop with no specialization.' : S.phase === 'tt-event' ? 'A quick decision before your next draft.' : S.phase === 'tt-path' ? 'Choose a path for this troop.' : S.tt.finished ? 'Run complete · try another army.' : S.placing ? 'Place ' + S.placing.n + ' · matching troops train.' : S.placingUpgrade ? 'Choose a troop for star training.' : S.placingEffect ? 'Choose a troop for ' + S.placingEffect.n + '.' : isBattle() ? 'Battle underway · inspect a troop for live details.' : 'Build, inspect, then fight the next wave.';
    if ($('playNext')) { $('playNext').disabled = isBattle() && mode() !== 'PAUSED' || S.tt.finished || forcedChoice() || !!(S.placing || S.placingUpgrade || S.placingEffect || S.placingSpecialization) || !liveA().length || S.phase !== 'recruit' && mode() !== 'PAUSED'; $('playNext').textContent = mode() === 'PAUSED' ? '▶ Resume' : '▶ Fight'; }
    $('recruit').disabled = S.phase !== 'recruit' || isBattle() || S.tt.finished || !!(S.placing || S.placingEffect || S.placingUpgrade || S.placingSpecialization); $('recruit').textContent = 'Draft'; $('shop').textContent = 'Store'; $('restart').textContent = 'Menu';
    const recap = TP.lastRecap; $('ttRecap').hidden = !recap || isBattle(); if (recap) $('ttRecap').textContent = `Wave ${recap.wave} ${recap.win ? 'cleared' : 'lost'} · ${recap.seconds}s` + (recap.mvp ? ` · MVP ${recap.mvp.e} ${recap.mvp.n}` : '') + (recap.objective ? ' · 🎯 Challenge complete +20 🪙' : '');
    qa('.cell').forEach((c, i) => { c.tabIndex = 0; c.setAttribute('role', 'button'); c.setAttribute('aria-label', S.squad[i] ? S.squad[i].n + ' · row ' + (Math.floor(i / 4) + 1) + ', column ' + (i % 4 + 1) : 'Empty square · row ' + (Math.floor(i / 4) + 1) + ', column ' + (i % 4 + 1)); updateCard(q('.unit', c), S.squad[i], true); });
    $('grid').style.setProperty('--tt-rows', Math.ceil(S.squad.length / 4)); $('grid').classList.toggle('tt-expanded', S.squad.length > 16);
    if (S.inspect && $('drawer').classList.contains('open')) { const open = q('.tt-advanced', $('body'))?.open, scroll = $('drawer').scrollTop; $('body').innerHTML = detailHtml(S.inspect); if (open) q('.tt-advanced', $('body')).open = true; $('drawer').scrollTop = scroll; }
  }
  render = function () {
    if (TP.drawing) return;
    TP.drawing = true;
    try {
      const structure = [S.phase, S.round, ...S.squad.map(u => u ? `${identify(u).ttId}:${u.star}:${u.ttPath || ''}:${u.evo || ''}` : '-'), ...S.enemies.map(e => e.n)].join('|');
      if (isBattle() && structure === TP.structures && performance.now() - (TP.lastDraw || 0) < 60) return;
      TP.lastDraw = performance.now();
      if (!isBattle() || structure !== TP.structures) { legacyRender(); TP.structures = structure; TP.allyNodes = qa('.cell').map(el => q('.unit', el) || el); }
      else hdr();
      if (!isBattle() && !S.tt.finished && S.tt.wavePlan?.round === S.round) { const previous = S.enemies; S.enemies = S.tt.wavePlan.enemies; enemies(); S.enemies = previous; qa('.enemy').forEach((el, i) => updateCard(el, S.tt.wavePlan.enemies[i], false)); }
      else qa('.enemy').forEach((el, i) => updateCard(el, S.enemies[i], false));
      TP.enemyNodes = qa('.enemy');
      postRender();
    } finally { TP.drawing = false; }
  };
  document.addEventListener('click', e => {
    const el = e.target.closest('[data-tt-action],[data-tt-speed],[data-tt-path],[data-tt-event],[data-tt-synergy],[data-tt-history]'); if (!el) return;
    if (el.dataset.ttSpeed) return setSpeed(Number(el.dataset.ttSpeed));
    if (el.dataset.ttPath) return pickPath(el.dataset.ttPath);
    if (el.dataset.ttEvent) return pickEvent(el.dataset.ttEvent);
    if (el.dataset.ttSynergy) return openSynergies(el.dataset.ttSynergy);
    if (el.dataset.ttHistory !== undefined) return openResults(TP.meta.runs[Number(el.dataset.ttHistory)]);
    const actions = { pause, resume: close, new: openMainMenu, book: openBook, history: openHistory, synergies: openSynergies, help, counters: openCounters, elements: () => window.openElements?.(), causes: () => show('Combat explanations', 'Recent immunities, resistances, and special interactions.', (S.combatCauses || []).slice(0, 12).map(c => `<p>${esc(c)}</p>`).join('') || '<p>Battle explanations will appear here.</p>'), bench: () => window.openBench?.(), more: () => show('Army tools', 'Details when you need them.', '<div class="runMenuGrid">' + [['book', '📖 Army Book'], ['synergies', '✨ Synergies'], ['bench', '🧺 Bench'], ['counters', '⚔️ Matchups'], ['causes', '📜 Combat explanations'], ['history', '🕰 Run history'], ['help', '❔ How to play']].map(([a, n]) => `<button class="secondary" data-tt-action="${a}">${n}</button>`).join('') + '</div>') };
    actions[el.dataset.ttAction]?.();
  });
  document.addEventListener('keydown', e => {
    if (/INPUT|SELECT|TEXTAREA/.test(e.target.tagName)) return;
    const drawer = $('drawer').classList.contains('open');
    if (e.key === 'Escape' && drawer) { e.preventDefault(); close(); return; }
    if (e.key === 'Tab' && drawer) { const buttons = qa('button:not(:disabled),input,select,summary,[tabindex="0"]', $('drawer')).filter(el => el.getClientRects().length); const first = buttons[0], last = buttons.at(-1); if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last?.focus(); } else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first?.focus(); } return; }
    if ((e.key === 'Enter' || e.code === 'Space') && e.target.classList.contains('cell')) { e.preventDefault(); tapCell(Number(e.target.dataset.i)); return; }
    if (e.code === 'Space' && !drawer && !e.repeat) { e.preventDefault(); pause(); }
  });
  document.addEventListener('visibilitychange', () => { if (document.hidden && isBattle() && mode() === 'PLAYING') pause(); });
  Object.assign(window, { fight, loop, end, reset, render, choose, reroll, choice, tapCell, pickEvolution, openEvolution, pickRelic, openRelic, openRecruit, openShop, openMainMenu, openRunMenu, close, show, detail, detailHtml, help });
  S.tt = R.newRun({}, seed()); S.tt.started = false; setMode('MENU'); loadMeta(); setupUi(); menuOptions(); TP.ready = true; ttClearTimers(); forceClose(); render(); openMainMenu();
})();
