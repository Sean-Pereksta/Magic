/* Endless Breach tactics. Movement/collision/damage remain owned by the game. */
(function (root) {
  'use strict';
  const clamp = (n, lo, hi) => Math.max(lo, Math.min(hi, n));
  const distance = (a, b) => Math.hypot(a.x - b.x, a.z - b.z);
  const alive = a => a && !a.dead && a.active !== false;
  const isDecoy = a => a?.decoy || a?.id === 'decoy' || a?.name === 'HOLOGRAM';
  const randomIndex = (list, random) => list[Math.min(list.length - 1, Math.floor(random() * list.length))];
  const SPECIES = Object.freeze({
    leaper: { unlock: 7, name: 'VAULT LEAPER', color: '#ffa949', r: .61, hp: 135, damage: 19, speed: 15.1, range: 1.65, cool: .8, scale: 1.06, pathing: true, jumpMin: 17, jumpMax: 21, special: 'leap' },
    stalker: { unlock: 12, name: 'BURROW STALKER', color: '#d49a62', r: .72, hp: 205, damage: 22, speed: 11.4, range: 1.8, cool: .8, scale: 1.12, pathing: true, jumpMin: 14, jumpMax: 17, special: 'burrow' },
    screecher: { unlock: 18, name: 'BROOD SCREECHER', color: '#ff81cf', r: .67, hp: 190, damage: 13, speed: 12.2, range: 1.6, cool: .9, scale: 1.08, pathing: true, jumpMin: 14, jumpMax: 17, special: 'rally' },
    artillery: { unlock: 24, name: 'ACID ARTILLERY', color: '#c7ff43', r: .94, hp: 300, damage: 20, speed: 8.8, range: 1.9, cool: 1.05, scale: 1.42, pathing: true, ideal: 29, jumpMin: 10, jumpMax: 12, special: 'acid' },
    lurker: { unlock: 28, name: 'PHASE LURKER', color: '#6ca9ff', r: .59, hp: 175, damage: 24, speed: 15.4, range: 1.65, cool: .72, scale: 1.06, pathing: true, jumpMin: 16, jumpMax: 19, special: 'phase' },
    warder: { unlock: 35, name: 'SWARM WARDER', color: '#81fff3', r: .83, hp: 325, damage: 15, speed: 9.8, range: 1.85, cool: 1, scale: 1.3, pathing: true, jumpMin: 12, jumpMax: 15, special: 'ward' }
  });
  const ENCOUNTERS = Object.freeze({
    swarm: { name: 'Swarm Assault', min: 1, weights: { blue: 8, yellow: 1, green: 2, screecher: 1 } },
    siege: { name: 'Siege Assault', min: 6, weights: { blue: 2, red: 6, yellow: 3, climber: 2, artillery: 4, warder: 2 } },
    hunters: { name: 'Hunter Pack', min: 7, weights: { blue: 3, green: 6, stripe: 4, leaper: 4, stalker: 3, lurker: 3 } },
    aerial: { name: 'Aerial Assault', min: 15, weights: { blue: 3, flyer: 6, climber: 5, yellow: 2, leaper: 2 } },
    strike: { name: 'Coordinated Strike', min: 12, weights: { blue: 2, red: 2, green: 4, purple: 4, climber: 3, screecher: 2, lurker: 3, warder: 2 } },
    elite: { name: 'Elite Hunt', min: 28, weights: { red: 3, purple: 3, stripe: 4, leaper: 3, stalker: 4, lurker: 5, warder: 3 } },
    brood: { name: 'Brood Assault', min: 18, weights: { blue: 5, climber: 6, leaper: 3, stalker: 2, screecher: 4, warder: 2 } },
    commander: { name: 'Commander Raid', min: 30, weights: { red: 4, yellow: 3, purple: 4, screecher: 3, artillery: 4, lurker: 3, warder: 4 } }
  });
  const UNLOCKS = { blue: 1, yellow: 3, red: 6, green: 10, purple: 12, stripe: 15, flyer: 15, climber: 4 };
  const MAP_BIAS = {
    breach_ring: ['siege', 'swarm'], derelict_station: ['strike', 'hunters'], xenofall_wastes: ['hunters', 'elite'],
    black_ice_cryolab: ['strike', 'siege'], living_hive: ['brood', 'hunters'], drowned_powerworks: ['siege', 'strike'],
    orbital_graveyard: ['aerial', 'strike'], swarm_heart: ['commander', 'brood', 'elite']
  };
  function speciesSpec(kind, wave) {
    const base = SPECIES[kind];
    if (!base) return null;
    const age = clamp(Number(wave) - base.unlock, 0, 35);
    // A 1000th-wave specialist remains shootable. Pressure comes from composition.
    return { ...base, hp: Math.round(base.hp * (1 + age * .018)), damage: base.damage + Math.floor(age / 9), speed: base.speed + Math.min(1.2, age * .035) };
  }
  function weightedPick(options, random) {
    const total = options.reduce((sum, item) => sum + Math.max(0, item[1]), 0);
    let roll = random() * total;
    for (const [id, weight] of options) { roll -= weight; if (roll <= 0) return id; }
    return options[options.length - 1][0];
  }
  function bossKinds(wave) {
    if (wave % 5) return [];
    const cycle = ['rammerBoss', 'spitterBoss', 'matriarchBoss', 'shieldBoss', 'burrowerBoss', 'commanderBoss'];
    const boss = cycle[(Math.floor(wave / 5) - 1) % cycle.length];
    const result = [boss];
    if (wave >= 50 && wave % 25 === 0) result.push(cycle[(cycle.indexOf(boss) + 2) % cycle.length]);
    if (wave >= 100 && wave % 50 === 0) result.push(cycle[(cycle.indexOf(boss) + 4) % cycle.length]);
    return result;
  }
  function planWave(inputWave, options = {}) {
    const wave = Math.max(1, Math.floor(Number(inputWave) || 1));
    const random = options.random || Math.random, history = options.history || [];
    const map = typeof options.map === 'string' ? options.map : options.map?.id || 'breach_ring';
    const performance = options.performance || {}, difficulty = options.difficulty || 'normal';
    const specialistPressure = ({ easy: .62, normal: 1, hard: 1.22, nightmare: 1.48 })[difficulty] || 1;
    const cadence = ({ easy: 1.2, normal: 1, hard: .9, nightmare: .78 })[difficulty] || 1;
    const encounter = weightedPick(Object.entries(ENCOUNTERS).filter(([, config]) => wave >= config.min).map(([id]) => {
      let weight = (MAP_BIAS[map] || []).includes(id) ? 3 : 1;
      if (history.slice(-2).includes(id)) weight *= .22;
      if (performance.fortified && id === 'siege') weight *= 1.8;
      if (performance.elevated && id === 'aerial') weight *= 1.8;
      if (performance.kiting && id === 'hunters') weight *= 1.8;
      // A struggling party receives less simultaneous specialty pressure, not hidden stat nerfs.
      if (performance.healthFraction < .3 && ['elite', 'commander'].includes(id)) weight *= .4;
      return [id, weight];
    }), random);
    const cfg = ENCOUNTERS[encounter];
    const veteranPressure = 1 + Math.min(2.5, Math.log2(1 + wave / 100) * .28);
    const pool = Object.entries({ ...UNLOCKS, ...Object.fromEntries(Object.entries(SPECIES).map(([id, config]) => [id, config.unlock])) }).filter(([, unlock]) => wave >= unlock).map(([id]) => [id, (cfg.weights[id] || .65) * (SPECIES[id] || ['purple', 'red', 'flyer', 'climber'].includes(id) ? specialistPressure * veteranPressure : 1)]);
    const ordinaryTotal = Math.min(144, 8 + Math.floor(wave * 2.5));
    const order = [], budget = Object.fromEntries(pool.map(([id]) => [id, 0]));
    // Introductions are guaranteed instead of disappearing in a random roll.
    for (const [id, spec] of Object.entries(SPECIES)) if (wave === spec.unlock) { order.push(id); budget[id]++; }
    while (order.length < ordinaryTotal) { const id = weightedPick(pool, random); order.push(id); budget[id]++; }
    // Fisher-Yates avoids an unstable comparator and preserves declared wave totals.
    for (let i = order.length - 1; i > 0; i--) { const j = Math.min(i, Math.floor(random() * (i + 1))); [order[i], order[j]] = [order[j], order[i]]; }
    const bosses = bossKinds(wave); order.unshift(...bosses);
    for (const id of bosses) budget[id] = (budget[id] || 0) + 1;
    budget.total = order.length; budget.boss = bosses[0] || null;
    return { wave, encounter, name: cfg.name, order, budget, bosses, total: order.length, activeCap: 25, squadSize: wave < 12 ? 4 : 6,
      spawnDelay: Math.max(2.5, (12 - Math.log2(wave + 1)) * cadence), eliteEvery: wave >= 35 ? Math.max(3, 11 - Math.floor(Math.log2(wave / 30 + 1) * specialistPressure)) : 0,
      hazardIntensity: Math.min(4, Math.floor(wave / 20)), phase: wave >= 60 ? 3 : wave >= 30 ? 2 : 1, difficulty };
  }
  function roleFor(a, index) {
    if (a.bossType === 'shield' || ['screecher', 'warder'].includes(a.alienKind)) return 'support';
    if (a.bossType === 'commander') return 'coordinator';
    if (a.bossType === 'rammer' || ['red', 'artillery'].includes(a.alienKind)) return 'breacher';
    if (a.ranged || a.rangedGun || ['yellow', 'purple'].includes(a.alienKind)) return 'suppressor';
    if (a.flying || a.climber) return 'vertical';
    if (['green', 'stripe', 'leaper', 'lurker', 'stalker'].includes(a.alienKind)) return 'flanker';
    return index % 3 === 2 ? 'flanker' : 'hunter';
  }
  let nextSquadId = 1;
  function assignSquad(aliens, options = {}) {
    const groups = [], live = aliens.filter(alive);
    for (let start = 0; start < live.length; start += 6) {
      const members = live.slice(start, start + 6), squad = { id: nextSquadId++, route: options.route || 0, wave: options.wave || 1, members, memory: null };
      members.forEach((a, i) => { a.survivalSquad = squad; a.survivalRole = roleFor(a, i); a.survivalSide = i % 2 ? 1 : -1; a.tacticalOffset = i * .07; });
      groups.push(squad);
    }
    return groups;
  }

  function createController(host) {
    const clock = host.time || (() => performance.now() / 1000), random = host.random || Math.random;
    const eye = host.eyeHeight || 1.86, decisionInterval = .4;
    let epoch = 0, lastRevision = '', graph = null, lastGraphSource = null, pathCount = 0, observationClock = 0;
    const paths = new Map(), memories = new WeakMap(), decisions = new WeakMap(), states = new WeakMap(), observations = new Map();
    const sight = (a, b) => host.lineClear ? host.lineClear(a.x, a.y - .55, a.z, b.x, b.y - .7, b.z) : false;
    const point = (p, a) => {
      // Explicit layered graph nodes are tested at their own floor height so a wall
      // beneath a bridge does not incorrectly block a perfectly valid upper route.
      const probe = Number.isFinite(p.height) ? { ...a, y: p.height + eye } : a;
      const info = host.pointInfo ? host.pointInfo(p.x, p.z, probe) : { walkable: host.topAtPoint(p.x, p.z) > -20, height: host.topAtPoint(p.x, p.z), hazard: 0 };
      const height = Number.isFinite(info.height) ? info.height : Number.isFinite(p.y) ? p.y - eye : 0;
      return { ...p, y: height + eye, info };
    };
    const hazardCost = hazard => typeof hazard === 'number' ? hazard : hazard && (hazard.damage > 0 || ['acid', 'electric', 'electrical', 'surge', 'heart_pulse', 'organic_growth'].includes(hazard.kind)) ? 1 : 0;
    const safe = (p, a, allowBarrier = false) => { const info = point(p, a).info; return info.walkable && (!info.obstacle || allowBarrier && info.destructible) && hazardCost(info.hazard) < .5; };
    const traverse = (a, b, actor, allowBarrier = false) => {
      const from = { ...a, height: a.height ?? (a.y || eye) - eye }, to = { ...b, height: b.height ?? (b.y || eye) - eye };
      // Each graph edge is evaluated at its source layer, not the alien's current
      // floor; otherwise a ground alien can never plan the upper half of a ramp.
      const probe = { ...actor, y: from.height + eye };
      const physical = host.canTraverse ? host.canTraverse(from, to, probe) : safe(b, actor) && Math.abs((b.y || a.y) - a.y) < 1.8;
      return physical && (allowBarrier || sight(a, b));
    };
    function invalidate() { epoch++; paths.clear(); graph = null; }
    function navigation() {
      const revision = String(host.revision?.() ?? '') + '/' + epoch, source = host.navigation?.();
      if (revision !== lastRevision || source !== lastGraphSource) { graph = null; paths.clear(); lastRevision = revision; lastGraphSource = source; }
      if (graph) return graph;
      const nodes = (source?.nodes || []).slice(0, 1500).map((n, i) => ({ ...n, id: n.id ?? i, y: (n.height ?? n.y ?? 0) + eye }));
      const byId = new Map(nodes.map((n, i) => [n.id, i])), links = nodes.map(() => []);
      for (const link of source?.links || []) {
        const from = byId.get(link.from), to = byId.get(link.to);
        if (from == null || to == null) continue;
        const type = link.kind || 'walk'; links[from].push({ to, type });
        if (!link.oneWay) links[to].push({ to: from, type });
      }
      graph = { nodes, links, burrowZones: source?.burrowZones || [], spawnZones: source?.spawnZones || [] }; return graph;
    }
    function allowedLink(type, a) { return !(['climb', 'fly', 'burrow'].includes(type)) || type === 'climb' && a.climber || type === 'fly' && a.flying || type === 'burrow' && (a.alienKind === 'stalker' || a.bossType === 'burrower'); }
    function nearestNode(pos, g, a, outgoing) {
      const candidates = g.nodes.map((n, i) => ({ i, n, d: distance(pos, n) + Math.abs((pos.y || eye) - n.y) * 2.5 })).sort((x, y) => x.d - y.d);
      for (const entry of candidates.slice(0, 10)) if (safe(entry.n, a, true) && (outgoing ? traverse(pos, entry.n, a, true) : traverse(entry.n, pos, a, true))) return entry.i;
      return null;
    }
    function findPath(a, goal) {
      if (a.flying || traverse(a, goal, a)) return [goal];
      const g = navigation(); if (!g.nodes.length) return [];
      const start = nearestNode(a, g, a, true), end = nearestNode(goal, g, a, false);
      if (start == null || end == null) return [];
      const key = [start, end, a.climber ? 'c' : 'w', Math.ceil((a.r || .6) * 2), lastRevision].join(':');
      const cached = paths.get(key), now = clock(); if (cached && cached.expires > now) return cached.path.length ? [...cached.path, goal] : [];
      pathCount++;
      const open = new Set([start]), came = new Map(), cost = new Map([[start, 0]]);
      let found = false, expanded = 0;
      while (open.size && expanded++ < 450) {
        let current = null, best = Infinity;
        for (const id of open) { const score = cost.get(id) + distance(g.nodes[id], g.nodes[end]); if (score < best) { current = id; best = score; } }
        if (current === end) { found = true; break; } open.delete(current);
        for (const link of g.links[current]) {
          if (!allowedLink(link.type, a)) continue;
          const from = g.nodes[current], to = g.nodes[link.to];
          if (!safe(to, a, true)) continue;
          if (['walk', 'ramp', 'stairs'].includes(link.type) && !traverse(from, to, a, true)) continue;
          if (link.type === 'jump' && (Math.abs(to.y - from.y) > 5 || distance(from, to) > 10)) continue;
          const terrain = point(to, a).info, step = distance(from, to) * Math.max(1, terrain.cost || to.cost || 1) + Math.abs(from.y - to.y) * .3 + hazardCost(terrain.hazard) * 18 + (link.type === 'walk' ? 0 : 1.5);
          const proposed = cost.get(current) + step;
          if (proposed < (cost.get(link.to) ?? Infinity)) { came.set(link.to, current); cost.set(link.to, proposed); open.add(link.to); }
        }
      }
      const result = [];
      if (found) { let id = end; for (let count = 0; count < 450; count++) { result.unshift(g.nodes[id]); if (id === start) break; id = came.get(id); if (id == null) break; } }
      paths.set(key, { expires: now + 1.6, path: result });
      while (paths.size > 160) paths.delete(paths.keys().next().value);
      return result.length ? [...result, goal] : [];
    }
    function visibleTarget(a) {
      let chosen = null, score = Infinity;
      const candidates = [...(host.humans?.() || []), ...(host.decoys?.() || [])];
      for (const h of candidates) {
        if (!alive(h) || h.spawnGrace > 0) continue;
        const d = distance(a, h);
        if (d > (a.flying ? 78 : 70) || !sight(a, h)) continue;
        const decoy = isDecoy(h), value = d + Math.abs(h.y - a.y) * .3 - (decoy && !a.boss ? 14 : 0);
        if (decoy && (a.boss || ['warder', 'screecher'].includes(a.alienKind))) continue;
        if (value < score) { chosen = h; score = value; }
      }
      return chosen;
    }
    function remember(a, h, now) {
      const memory = { x: h.x, y: h.y, z: h.z, vx: h.vx || 0, vz: h.vz || 0, time: now, source: h.entityId || h.name, dead: false, investigation: true };
      memories.set(a, memory);
      if (!isDecoy(h)) {
        const ground = host.pointInfo?.(h.x, h.z, { ...h, y: eye })?.height ?? 0;
        observations.set(h.entityId || h.name || 'human', { time: now, elevated: h.y - eye - ground > 2.5, kiting: Math.hypot(h.vx || 0, h.vz || 0) > 8, healthFraction: h.hp / (h.maxHp || 100) });
        while (observations.size > 4) observations.delete(observations.keys().next().value);
      }
      if (a.survivalSquad) a.survivalSquad.memory = { ...memory, observer: a };
      return memory;
    }
    function knownPoint(a, now) {
      let memory = memories.get(a), shared = a.survivalSquad?.memory;
      if (shared && alive(shared.observer) && distance(a, shared.observer) < 36 && (!memory || shared.time > memory.time)) { memory = { ...shared }; delete memory.observer; memories.set(a, memory); }
      return memory && now - memory.time < 9 ? memory : null;
    }
    function observedStructures(a) {
      return (host.structures?.() || []).filter(p => {
        if (!alive(p) || !p.destructible || distance(a, p) >= 34) return false;
        if (distance(a, p) < 5) return true;
        const x = clamp(a.x, p.x - (p.w || 2) / 2, p.x + (p.w || 2) / 2), z = clamp(a.z, p.z - (p.d || 2) / 2, p.z + (p.d || 2) / 2);
        const dx = a.x - x, dz = a.z - z, length = Math.hypot(dx, dz) || 1;
        const bottom = p.type === 'platform' ? p.y : p.y - (p.h || 3) / 2, top = bottom + (p.h || 3);
        // Test the visible face, not an endpoint buried inside the obstacle's own collision box.
        return sight(a, { x: x + dx / length * .15, z: z + dz / length * .15, y: clamp(a.y - .55, bottom + .1, top - .1) + .7 });
      });
    }
    function breachTarget(a, focus) {
      let best = null, score = -Infinity;
      for (const p of observedStructures(a)) {
        const nearThreat = distance(p, focus), nearSelf = distance(p, a);
        const scoreP = (p.breachMarked > 0 ? 35 : 0) + (p.builtBy ? 12 : 0) + Math.min(18, (p.hp || 0) / 90) - nearThreat * .7 - nearSelf * .6;
        if (scoreP > score && (nearThreat < 20 || nearSelf < 5)) { best = p; score = scoreP; }
      }
      return best;
    }
    function chooseGoal(a, focus, visible, now) {
      const role = a.survivalRole || roleFor(a, 0), dx = focus.x - a.x, dz = focus.z - a.z, d = Math.hypot(dx, dz) || 1, side = a.survivalSide || 1;
      let goal = { x: focus.x, y: focus.y, z: focus.z }, tactic = visible ? 'hunt' : 'investigate', chew = null;
      const neighbors = (host.aliens?.() || []).filter(o => alive(o) && o !== a && distance(a, o) < 17);
      if (role === 'support' && neighbors.length) {
        const centroid = neighbors.reduce((p, o) => ({ x: p.x + o.x / neighbors.length, z: p.z + o.z / neighbors.length }), { x: 0, z: 0 });
        goal = point({ x: centroid.x - dx / d * 6, z: centroid.z - dz / d * 6 }, a); tactic = 'support';
      } else if (role === 'flanker') {
        const lead = visible ? Math.min(.95, d / Math.max(12, a.alienSpeed || 12)) : 0;
        const spread = d > 8 ? Math.min(10, d * .3) : 2;
        goal.x += (focus.vx || 0) * lead + -dz / d * spread * side;
        goal.z += (focus.vz || 0) * lead + dx / d * spread * side; tactic = visible ? 'intercept' : 'flank';
      } else if (role === 'suppressor' || a.alienKind === 'artillery') {
        const ideal = a.idealRange || (a.alienKind === 'artillery' ? 29 : 20), retreat = a.hp < a.maxHp * .28;
        const desired = retreat ? ideal + 7 : ideal;
        const angles = [0, side * .55, -side * .55, side * 1.05];
        let candidate = null, best = Infinity;
        for (const angle of angles) {
          const yaw = Math.atan2(-dx, -dz) + angle, p = point({ x: focus.x + Math.sin(yaw) * desired, z: focus.z + Math.cos(yaw) * desired }, a);
          if (!safe(p, a) || Math.abs(p.y - a.y) > 8) continue;
          const clear = host.lineClear?.(p.x, p.y - .55, p.z, focus.x, focus.y - .7, focus.z);
          const value = distance(a, p) + (clear ? 0 : 18) + Math.abs(angle) * 4;
          if (value < best) { best = value; candidate = p; }
        }
        if (candidate) goal = candidate;
        tactic = retreat ? 'retreat' : visible ? 'firing-lane' : 'reposition';
      } else if (role === 'vertical' && focus.y > a.y + 2.2) tactic = 'high-ground';
      if (role === 'breacher' && (focus.y > a.y + 3 || !visible || a.bossType === 'rammer')) {
        chew = breachTarget(a, focus); if (chew) { goal = { x: chew.x, y: a.y, z: chew.z }; tactic = 'breach'; }
      }
      const limit = (host.bounds?.() || 98) - 2;
      goal.x = clamp(goal.x, -limit, limit); goal.z = clamp(goal.z, -limit, limit);
      if (!safe(goal, a) && !chew) goal = point({ x: focus.x, z: focus.z }, a);
      return { goal, tactic, chew, role, time: now, visible };
    }
    function acquire(a) {
      const now = clock(), visible = visibleTarget(a);
      if (now < observationClock) observations.clear(); observationClock = now;
      let previous = decisions.get(a), memory = knownPoint(a, now);
      if (visible && (!memory || !previous || now >= previous.nextThink)) memory = remember(a, visible, now);
      const focus = visible || memory;
      if (!previous || now >= previous.nextThink || previous.epoch !== epoch || previous.hadSight !== !!visible) {
        let localFocus = focus;
        if (!localFocus) {
          const g = navigation(), patrol = g.nodes.filter(n => distance(a, n) > 4 && safe(n, a));
          localFocus = patrol.length ? randomIndex(patrol.slice(0, 120), random) : { x: 0, y: a.y, z: 0, dead: false, investigation: true };
        }
        previous = chooseGoal(a, localFocus, !!visible, now);
        previous.route = findPath(a, previous.goal); previous.routeIndex = 0;
        previous.blocked = !previous.route.length && !traverse(a, previous.goal, a);
        previous.fallbackChew = previous.blocked ? breachTarget(a, memory || previous.goal) : null;
        previous.nextThink = now + decisionInterval + (a.tacticalOffset || 0); previous.epoch = epoch; previous.hadSight = !!visible;
        if (!focus) previous.tactic = 'patrol';
        decisions.set(a, previous);
      }
      while (previous.routeIndex < previous.route.length - 1 && distance(a, previous.route[previous.routeIndex]) < 2) previous.routeIndex++;
      const waypoint = previous.route[previous.routeIndex], destination = waypoint || previous.goal;
      // A blocked route is investigated locally; only a barrier actually sensed nearby is attacked.
      const chew = previous.chew || previous.fallbackChew;
      // Approach a sensed barrier before local collision/chewing; never circle an entire map
      // merely because a long straight-line query intersects a wall farther down the route.
      const localGoal = destination;
      const boost = a.rallyUntil > now ? 1.18 : 1;
      a.tacticalState = previous.tactic;
      return { target: visible, memory: memory ? { ...memory } : null, focus: focus || { ...previous.goal, dead: false, investigation: true }, visible: !!visible, goal: localGoal, chew: alive(chew) ? chew : null,
        tactic: previous.tactic, role: previous.role, squad: a.survivalSquad?.id || 0, speedMultiplier: boost, waypoint };
    }

    function warn(p, type, color, life, radius) { host.warn?.(p.x, p.z, type, color, life, radius); }
    function hitNearby(a, center, radius, amount) {
      for (const h of host.humans?.() || []) if (alive(h) && !h.spawnGrace && distance(h, center) < radius && Math.abs(h.y - center.y) < 3 && host.lineClear?.(center.x, center.y - .3, center.z, h.x, h.y - .7, h.z)) host.damage?.(h, amount, a, false, { x: h.x, y: h.y - .7, z: h.z });
    }
    function destination(a, focus, type) {
      if (!focus) return null;
      const g = navigation(), zones = g.burrowZones;
      if (type === 'burrow') {
        const candidates = zones.filter(z => distance(z, a) < 40 && distance(z, focus) < 14 && safe(z, a)).sort((x, y) => distance(x, focus) - distance(y, focus));
        const chosen = candidates[0]; if (!chosen) return null;
        const angle = random() * Math.PI * 2, spread = Math.min(3, chosen.r || 2);
        const p = point({ x: chosen.x + Math.sin(angle) * spread, z: chosen.z + Math.cos(angle) * spread }, a);
        return safe(p, a) && Math.abs(p.y - a.y) < 2.5 ? p : point(chosen, a);
      }
      const dx = focus.x - a.x, dz = focus.z - a.z, length = Math.hypot(dx, dz) || 1;
      for (const sign of [a.survivalSide || 1, -(a.survivalSide || 1)]) {
        const p = point({ x: a.x + dx / length * Math.min(6, length * .35) - dz / length * 6 * sign, z: a.z + dz / length * Math.min(6, length * .35) + dx / length * 6 * sign }, a);
        if (safe(p, a) && distance(a, p) <= 12 && Math.abs(p.y - a.y) < 2.5 && sight(a, p)) return p;
      }
      return null;
    }
    function specialState(a) { let state = states.get(a); if (!state) { state = { cooldown: 2 + random() * 3, mode: 'ready', timer: 0, pulse: 0 }; states.set(a, state); } return state; }
    function updateSpecial(a, dt, decision) {
      const spec = SPECIES[a.alienKind], burrowBoss = a.bossType === 'burrower';
      if (!spec && !burrowBoss) { updateBossSupport(a, dt, decision); return false; }
      const state = specialState(a), now = clock();
      state.cooldown = Math.max(0, state.cooldown - dt); state.timer -= dt;
      // EMP effects interrupt abilities before their harmful commit, including pending acid.
      if (a.empUntil > now || a.stagger > .12) {
        if (state.mode !== 'ready') {
          if (state.mode === 'tunnel' && state.origin && !safe(a, a)) { a.x = state.origin.x; a.y = state.origin.y; a.z = state.origin.z; }
          state.mode = 'ready'; state.cooldown = Math.max(state.cooldown, 2); a.burrowed = false; a.phaseWarning = null; a.specialWindup = 0;
        }
        return false;
      }
      const special = burrowBoss ? 'burrow' : spec.special, focus = decision.focus;
      if (state.mode === 'ready' && state.cooldown <= 0) {
        if (special === 'rally' || special === 'ward') {
          const friends = (host.aliens?.() || []).filter(o => alive(o) && o !== a && distance(a, o) < 17);
          if (friends.length) { state.mode = 'pulse'; state.timer = .8; a.specialWindup = .8; warn(a, special, a.color, .8, 12); }
          else state.cooldown = 1;
        } else if ((decision.visible || decision.memory && now - decision.memory.time < 3) && focus) {
          const d = distance(a, focus);
          if (special === 'leap' && decision.visible && a.onGround && d > 5 && d < 22) {
            state.target = { x: focus.x + (focus.vx || 0) * .35, y: focus.y, z: focus.z + (focus.vz || 0) * .35 };
            if (safe(state.target, a)) { state.mode = 'windup'; state.timer = .8; a.specialWindup = .8; warn(state.target, 'leap', a.color, 1.1, 3); }
          } else if (special === 'burrow' || special === 'phase') {
            const p = destination(a, focus, special);
            if (p) { state.target = p; state.origin = { x: a.x, y: a.y, z: a.z }; state.mode = 'windup'; state.timer = special === 'phase' ? 1.0 : 1.6; a.specialWindup = state.timer; a.phaseWarning = p; warn(p, special === 'burrow' ? 'crack' : 'phase', a.color, state.timer + .35, special === 'burrow' ? 4 : 2.5); }
            else state.cooldown = 1.5;
          } else if (special === 'acid' && d > 12 && d < 60 && decision.visible) {
            state.target = point({ x: focus.x + (focus.vx || 0) * .4, z: focus.z + (focus.vz || 0) * .4 }, a);
            state.mode = 'windup'; state.timer = 1.4; a.specialWindup = 1.4; warn(state.target, 'acidArc', a.color, 2.2, 3.8);
          }
        }
      }
      if (state.mode === 'ready') return false;
      if (state.mode === 'pulse') {
        a.specialWindup = Math.max(0, state.timer);
        if (state.timer <= 0) {
          for (const o of host.aliens?.() || []) if (alive(o) && o !== a && distance(a, o) < 17 && sight(a, o)) {
            if (special === 'rally') { o.rallyUntil = now + 3; const known = memories.get(a); if (known) memories.set(o, { ...known }); }
            else o.wardSource = a;
          }
          if (special === 'ward') a.wardUntil = now + 4;
          host.burst?.(a.x, a.y - .6, a.z, a.color, 12, .6); state.mode = 'ready'; state.cooldown = special === 'rally' ? 7 : 9;
        }
        return false;
      }
      if (state.mode === 'windup') {
        a.specialWindup = Math.max(0, state.timer); a.vx *= .75; a.vz *= .75; host.physics?.(a, dt);
        if (state.timer > 0) return true;
        if (!safe(state.target, a)) { state.mode = 'ready'; state.cooldown = 2; a.phaseWarning = null; return true; }
        if (special === 'leap') {
          const dx = state.target.x - a.x, dz = state.target.z - a.z, d = Math.hypot(dx, dz) || 1;
          a.vx = dx / d * Math.min(24, d * 1.35); a.vz = dz / d * Math.min(24, d * 1.35); a.vy = 16.5; a.onGround = false; state.mode = 'lunge'; state.timer = 1.15; state.hit = false; a.attackAnim = .6;
        } else if (special === 'acid') {
          state.origin = { x: a.x, y: a.y + .4, z: a.z }; state.lastPoint = { ...state.origin }; state.mode = 'acid-flight'; state.timer = .7; state.pulse = 0;
        } else if (special === 'phase') {
          // Arrival is advertised for a full second and validated again against new builds.
          host.burst?.(a.x, a.y - .8, a.z, a.color, 10, .4); a.x = state.target.x; a.y = state.target.y; a.z = state.target.z; a.vx = a.vz = a.vy = 0; a.onGround = true;
          host.burst?.(a.x, a.y - .8, a.z, a.color, 12, .5); a.phaseWarning = null; a.fireCd = Math.max(a.fireCd || 0, .65); state.mode = 'ready'; state.cooldown = 8;
        } else {
          a.burrowed = true; state.mode = 'tunnel'; state.timer = .85; a.phaseWarning = null;
        }
        return true;
      }
      if (state.mode === 'lunge') {
        host.physics?.(a, dt); a.walk = (a.walk || 0) + dt * 11;
        if (!state.hit) { for (const h of host.humans?.() || []) if (alive(h) && distance(a, h) < 2.7 && Math.abs(h.y - a.y) < 2.5 && sight(a, h)) { hitNearby(a, a, 2.8, a.alienDamage * 1.3); state.hit = true; break; } }
        if (state.timer <= 0 || a.onGround) { state.mode = 'ready'; state.cooldown = 6; a.vx *= .65; a.vz *= .65; } return true;
      }
      if (state.mode === 'acid-flight') {
        const t = clamp(1 - state.timer / .7, 0, 1), endY = state.target.y - eye + .2;
        const p = { x: state.origin.x + (state.target.x - state.origin.x) * t, z: state.origin.z + (state.target.z - state.origin.z) * t,
          y: state.origin.y + (endY - state.origin.y) * t + Math.sin(t * Math.PI) * 8 };
        if (state.lastPoint && !host.lineClear?.(state.lastPoint.x, state.lastPoint.y, state.lastPoint.z, p.x, p.y, p.z)) {
          host.burst?.(state.lastPoint.x, state.lastPoint.y, state.lastPoint.z, a.color, 8, .4); state.mode = 'ready'; state.cooldown = 7; return false;
        }
        state.lastPoint = p;
        state.pulse -= dt;
        if (state.pulse <= 0) { state.pulse = .055; host.burst?.(p.x, p.y, p.z, a.color, 3, .25); }
        if (state.timer <= 0) {
          host.acid?.({ x: state.target.x, y: endY - .2, z: state.target.z, r: 3.8, life: 6, maxLife: 6, tick: .1, owner: a });
          host.burst?.(state.target.x, endY, state.target.z, a.color, 12, .55); state.mode = 'ready'; state.cooldown = 7;
        }
        return false;
      }
      if (state.mode === 'tunnel') {
        // Only an authored burrowing location is usable; interpolation is visible as a moving ground trail.
        const t = clamp(1 - state.timer / .85, 0, 1); a.x = state.origin.x + (state.target.x - state.origin.x) * t; a.z = state.origin.z + (state.target.z - state.origin.z) * t;
        a.vx = a.vz = 0; state.pulse -= dt;
        if (state.pulse <= 0) { state.pulse = .12; host.burst?.(a.x, (host.topAtPoint?.(a.x, a.z) || 0) + .1, a.z, a.color, 3, .25); }
        if (state.timer <= 0) {
          a.burrowed = false; a.y = state.target.y; a.onGround = true; a.fireCd = Math.max(a.fireCd || 0, .6);
          // The advertised site can be closed by construction during the warning.
          if (safe(state.target, a)) { hitNearby(a, a, burrowBoss ? 5 : 3.8, a.alienDamage * 1.2); for (const p of observedStructures(a)) if (distance(a, p) < (burrowBoss ? 6 : 3.5)) host.damageStructure?.(p, burrowBoss ? 180 : 60, a, 'burrow'); }
          else { a.x = state.origin.x; a.y = state.origin.y; a.z = state.origin.z; }
          host.burst?.(a.x, a.y - eye, a.z, a.color, 18, .7); state.mode = 'ready'; state.cooldown = burrowBoss ? 7 : 9;
        }
        return true;
      }
      return false;
    }
    function updateBossSupport(a, dt, decision) {
      if (!a.boss) return;
      const now = clock();
      if (a.bossType === 'rammer' && a.bossState === 'hunt') { const valuable = breachTarget(a, decision.focus); if (valuable) a.tacticalBreachTarget = valuable; }
      if (a.bossType === 'shield') {
        a.wardUntil = now + .3;
        // Protection has a limited range, actual source, and line of sight.
        for (const o of host.aliens?.() || []) if (alive(o) && o !== a && distance(a, o) < 12 && sight(a, o)) o.wardSource = a;
      }
      if (a.bossType === 'matriarch' || a.bossType === 'commander') {
        a.coordinationTimer = (a.coordinationTimer || 0) - dt;
        if (a.coordinationTimer <= 0 && decision.memory) {
          a.coordinationTimer = 3.5;
          for (const o of host.aliens?.() || []) if (alive(o) && o !== a && distance(a, o) < 30 && (a.bossType === 'commander' || o.climber)) { memories.set(o, { ...decision.memory }); o.rallyUntil = now + 2; o.survivalSide = o.survivalSide || (random() < .5 ? -1 : 1); }
        }
      }
    }
    function damageMultiplier(a) {
      const source = a.wardSource;
      return alive(source) && source.wardUntil > clock() && !(source.empUntil > clock()) && distance(a, source) < (source.boss ? 12 : 17) && sight(source, a) ? .62 : 1;
    }
    return { acquire, updateSpecial, invalidate, assignSquad, damageMultiplier, navigation, findPath,
      stats: () => ({ cachedPaths: paths.size, pathSearches: pathCount, epoch }),
      observedPerformance: () => {
        const recent = [...observations.values()].filter(o => clock() - o.time < 15);
        return { fortified: (host.structures?.() || []).filter(p => p.builtBy && alive(p)).length >= 8,
          elevated: recent.some(o => o.elevated), kiting: recent.some(o => o.kiting), healthFraction: recent.length ? recent.reduce((sum, o) => sum + o.healthFraction, 0) / recent.length : 1 };
      } };
  }
  const api = Object.freeze({ SPECIES, ENCOUNTERS, speciesSpec, planWave, assignSquad, createController, balanceWave: (wave, boss = false) => Math.min(boss ? 60 : 30, Math.max(1, Number(wave) || 1)) });
  root.NeonSurvivalAI = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
