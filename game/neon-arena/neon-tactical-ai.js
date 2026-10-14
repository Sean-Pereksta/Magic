/* Human combat decisions. The host owns collision, damage, animation and weapons. */
(function (root) {
  'use strict';
  const clamp = (n, a, b) => Math.max(a, Math.min(b, n));
  const distance = (a, b) => Math.hypot(a.x - b.x, a.z - b.z);
  const alive = a => a && !a.dead && a.active !== false;
  const angleDelta = (a, b) => Math.atan2(Math.sin(a - b), Math.cos(a - b));
  const snapshot = (a, now, source = 'sight') => ({ x: a.x, y: a.y, z: a.z, vx: a.vx || 0, vz: a.vz || 0, time: now, id: a.entityId || a.name, source });
  const PROFILES = Object.freeze({
    aggressor: { label: 'AGGRESSOR', cover: .36, flank: .42, retreat: .22, push: 1.2, range: .72, sprint: 1.1 },
    flanker: { label: 'FLANKER', cover: .64, flank: .95, retreat: .32, push: 1, range: .9, sprint: 1.08 },
    defender: { label: 'DEFENDER', cover: .96, flank: .3, retreat: .4, push: .75, range: 1.1, sprint: .96 },
    marksman: { label: 'MARKSMAN', cover: .86, flank: .5, retreat: .43, push: .7, range: 1.32, sprint: .94 },
    balanced: { label: 'BALANCED SOLDIER', cover: .68, flank: .62, retreat: .34, push: 1, range: 1, sprint: 1 }
  });
  const DIFFICULTY = Object.freeze({
    easy: { reaction: .5, memory: 3.5, turn: 3.6, error: .065, teamwork: .3, decision: .44, burst: 3, pause: .7 },
    normal: { reaction: .32, memory: 5, turn: 5.4, error: .037, teamwork: .65, decision: .3, burst: 4, pause: .48 },
    hard: { reaction: .2, memory: 6.5, turn: 7, error: .021, teamwork: .85, decision: .24, burst: 5, pause: .34 },
    nightmare: { reaction: .14, memory: 7, turn: 8.4, error: .014, teamwork: .95, decision: .2, burst: 6, pause: .28 }
  });
  const profileName = a => a.tacticalProfile || ({ rusher: 'aggressor', skirmisher: 'flanker', heavy: 'defender', rifler: 'balanced', marksman: 'marksman' })[a.style] || 'balanced';

  function createController(host) {
    const clock = host.time || (() => performance.now() / 1000), random = host.random || Math.random;
    let states = new WeakMap(), outcomes = new WeakMap();
    const sounds = [], paths = new Map();
    let graph = null, source = null, revision = '', searches = 0, deferredSearches = 0, searchCredits = 3, creditTime = clock();
    const settings = () => DIFFICULTY[host.difficulty?.() || 'normal'] || DIFFICULTY.normal;
    const eye = a => host.eye?.(a) || { x: a.x, y: a.y, z: a.z };
    const aimHeight = a => host.aimHeight?.(a) ?? a.y - .55;
    const sight = (a, b) => { const origin = eye(a); return !!host.lineClear?.(origin.x, origin.y - .08, origin.z, b.x, aimHeight(b), b.z); };
    const walkable = (p, a) => host.pointInfo?.(p.x, p.z, { ...a, y: p.y ?? a.y })?.walkable !== false;
    const traverse = (from, to, a) => !!host.canTraverse?.(from, to, a);
    function state(a) {
      let s = states.get(a);
      if (!s) { s = { memory: null, enemy: null, acquired: 0, lostAt: -Infinity, nextThink: 0, decision: null, side: random() < .5 ? -1 : 1, seed: random() * 100, yaw: a.yaw || 0, pitch: 0, recoil: 0, burstLeft: 0, pauseUntil: 0, exposed: 0, lastProgress: clock(), anchor: { x: a.x, z: a.z } }; states.set(a, s); }
      return s;
    }
    function reset(a) { states.delete(a); }
    function clear() { states = new WeakMap(); outcomes = new WeakMap(); sounds.length = 0; paths.clear(); graph = null; source = null; searchCredits = 3; creditTime = clock(); }
    function adaptation(a) {
      const now = clock(), recent = (outcomes.get(a) || []).filter(event => now - event.time < 60);
      outcomes.set(a, recent);
      const confidence = clamp(recent.reduce((sum, event) => sum + (event.won ? .12 : -.2), 0), -.6, .36);
      return { confidence, risk: clamp(1 + confidence * .55, .67, 1.2), defeats: recent.filter(event => !event.won).length, successes: recent.filter(event => event.won).length,
        dangerousLanes: recent.filter(event => !event.won && now - event.time < 45).slice(-4).map(event => ({ x: event.x, y: event.y, z: event.z, time: event.time })) };
    }
    function recordOutcome(victim, killer) {
      const record = (a, won) => {
        if (!a || a.human || a.alien || !a.style && !states.has(a)) return;
        const recent = outcomes.get(a) || [];
        // A defeated soldier knows the place where they lost the fight. No
        // hidden attacker position or opponent state is copied into learning.
        recent.push({ won, x: a.x, y: a.y, z: a.z, time: clock() });
        outcomes.set(a, recent.slice(-8)); a.tacticalLearning = adaptation(a);
        const s = states.get(a); if (s) s.nextThink = 0;
      };
      record(victim, false); if (killer !== victim) record(killer, true); paths.clear();
    }
    function laneDanger(from, to, learning) {
      const dx = to.x - from.x, dz = to.z - from.z, lengthSq = dx * dx + dz * dz;
      return learning.dangerousLanes.reduce((sum, lane) => {
        if (Math.abs(lane.y - to.y) > 2.5) return sum;
        const t = lengthSq ? clamp(((lane.x - from.x) * dx + (lane.z - from.z) * dz) / lengthSq, 0, 1) : 0;
        const gap = Math.hypot(lane.x - from.x - dx * t, lane.z - from.z - dz * t);
        return sum + Math.max(0, 1 - gap / 12) * 10;
      }, 0);
    }
    function hear(event) {
      if (!event || !Number.isFinite(event.x + event.y + event.z)) return;
      // Copy the position at the shot, never retain a live hidden actor reference.
      sounds.push({ ...event, time: clock() });
      if (sounds.length > 64) sounds.splice(0, sounds.length - 64);
    }
    function navigation() {
      const next = host.navigation?.() || [], nextRevision = String(host.revision?.() ?? '');
      if (graph && source === next && revision === nextRevision) return graph;
      source = next; revision = nextRevision; paths.clear();
      const nodes = (Array.isArray(next) ? next : next.nodes || []).slice(0, 1800).map((n, i) => ({ ...n, id: n.id ?? i, y: n.y ?? 1.86 }));
      const links = nodes.map(() => []), byId = new Map(nodes.map((n, i) => [n.id, i]));
      if (!Array.isArray(next) && next.links) {
        for (const link of next.links) { const a = byId.get(link.from), b = byId.get(link.to); if (a == null || b == null || ['fly', 'burrow', 'climb'].includes(link.kind)) continue; links[a].push(b); if (!link.oneWay) links[b].push(a); }
      } else {
        const buckets = new Map(), cell = 12;
        nodes.forEach((n, i) => { const k = Math.floor(n.x / cell) + ':' + Math.floor(n.z / cell); if (!buckets.has(k)) buckets.set(k, []); buckets.get(k).push(i); });
        nodes.forEach((n, i) => {
          const bx = Math.floor(n.x / cell), bz = Math.floor(n.z / cell), near = [];
          for (let x = -1; x <= 1; x++) for (let z = -1; z <= 1; z++) for (const j of buckets.get((bx + x) + ':' + (bz + z)) || []) if (i !== j && distance(n, nodes[j]) <= 15 && Math.abs(n.y - nodes[j].y) <= 3.5) near.push(j);
          links[i] = near.sort((a, b) => distance(n, nodes[a]) - distance(n, nodes[b])).slice(0, 14);
        });
      }
      graph = { nodes, links }; return graph;
    }
    function findPath(a, goal, avoid = null) {
      if (!goal || !walkable(goal, a)) return [];
      const learning = adaptation(a);
      if (!avoid && laneDanger(a, goal, learning) < 2 && traverse(a, goal, a)) return [goal];
      const g = navigation();
      const nearest = (p, outgoing) => g.nodes.map((n, i) => ({ i, n, d: distance(p, n) + Math.abs(p.y - n.y) * 4 })).sort((a, b) => a.d - b.d).slice(0, 16).find(({ n }) => walkable(n, a) && (outgoing ? traverse(p, n, a) : traverse(n, p, a)))?.i;
      const start = nearest(a, true), end = nearest(goal, false);
      if (start == null || end == null) return [];
      const key = [start, end, Math.ceil((a.r || .52) * 4), avoid ? Math.floor(avoid.x / 8) + '/' + Math.floor(avoid.z / 8) : '', revision].join(':');
      const cached = paths.get(key), now = clock();
      if (cached && cached.expires > now) return [...cached.path, goal];
      // Spread full searches across decisions instead of allowing a spawn wave
      // to spend an unbounded frame budget. Valid direct/cached paths stay free.
      searchCredits = Math.min(3, searchCredits + Math.max(0, now - creditTime) * 24); creditTime = now;
      if (searchCredits < 1) { deferredSearches++; return []; }
      searchCredits--;
      const open = new Set([start]), costs = new Map([[start, 0]]), previous = new Map();
      let found = false, expanded = 0; searches++;
      while (open.size && expanded++ < 650) {
        let current = null, best = Infinity;
        for (const i of open) { const score = costs.get(i) + distance(g.nodes[i], g.nodes[end]); if (score < best) { current = i; best = score; } }
        if (current === end) { found = true; break; } open.delete(current);
        for (const i of g.links[current]) {
          const from = g.nodes[current], to = g.nodes[i];
          if (!walkable(to, a) || !traverse(from, to, { ...a, y: from.y })) continue;
          const exposed = avoid && sight(to, avoid) ? distance(from, to) * .95 : 0;
          const score = costs.get(current) + distance(from, to) + Math.abs(from.y - to.y) * .4 + exposed + laneDanger(from, to, learning);
          if (score < (costs.get(i) ?? Infinity)) { costs.set(i, score); previous.set(i, current); open.add(i); }
        }
      }
      const route = [];
      if (found) { let i = end; for (let count = 0; count < 650; count++) { route.unshift(g.nodes[i]); if (i === start) break; i = previous.get(i); if (i == null) break; } }
      if (route.length) paths.set(key, { path: route, expires: now + 1.4 });
      while (paths.size > 160) paths.delete(paths.keys().next().value);
      return route.length ? [...route, goal] : [];
    }
    function perceive(a, s, now) {
      const cfg = settings(); let chosen = null, score = -Infinity;
      const actors = host.actors?.() || [];
      for (const e of actors) {
        if (!alive(e) || e === a || e.spawnGrace > 0 || host.sameTeam?.(a, e)) continue;
        const d = distance(a, e); if (d > 82) continue;
        const angle = Math.abs(angleDelta(Math.atan2(e.x - a.x, e.z - a.z), a.faceYaw ?? a.yaw ?? 0));
        if (angle > Math.PI * .58 || !sight(a, e)) continue;
        // Health, motion and teammate pressure are considered only for visible opponents.
        const pressure = actors.filter(o => o !== a && alive(o) && host.sameTeam?.(a, o) && distance(a, o) < 30 && states.get(o)?.enemy === e).length;
        const value = 70 - d + (e === s.enemy ? 13 : 0) + (1 - e.hp / (e.maxHp || 100)) * 15 - Math.max(0, pressure - 1) * 12;
        if (value > score) { score = value; chosen = e; }
      }
      if (chosen) {
        if (s.enemy !== chosen || now - s.lostAt < .3) { s.acquired = now + cfg.reaction * (.85 + random() * .4); s.burstLeft = 0; }
        s.enemy = chosen; s.memory = snapshot(chosen, now); s.lostAt = -Infinity;
      } else {
        if (s.enemy) s.lostAt = now;
        s.enemy = null;
        for (const sound of sounds) {
          if (sound.team === a.team || sound.time <= (s.memory?.time ?? -Infinity) || now - sound.time > 1 || distance(a, sound) > (sound.range || 38)) continue;
          const muffled = !sight(a, sound); if (muffled && distance(a, sound) > (sound.range || 38) * .5) continue;
          const uncertainty = muffled ? 3.5 : 1.2, angle = random() * Math.PI * 2;
          s.memory = { x: sound.x + Math.cos(angle) * uncertainty, y: sound.y, z: sound.z + Math.sin(angle) * uncertainty, vx: 0, vz: 0, time: sound.time, id: sound.source, source: 'hearing' };
        }
        if (random() < cfg.teamwork) for (const ally of actors) {
          const shared = states.get(ally)?.memory;
          if (ally === a || !alive(ally) || !host.sameTeam?.(a, ally) || distance(a, ally) > 26 || !shared || now - shared.time > 2 || shared.source !== 'sight' || shared.time <= (s.memory?.time ?? -Infinity)) continue;
          s.memory = { ...shared, source: 'team' };
        }
        if (s.memory && (now - s.memory.time > cfg.memory || distance(a, s.memory) < 2.2 && now - s.memory.time > 1.2)) s.memory = null;
      }
      return chosen;
    }
    function findCover(a, threat) {
      let best = null, bestScore = Infinity;
      const nearby = (host.coverBlocks?.(a) || []).filter(b => b.structure?.active !== false && b.y + b.h / 2 >= a.y - .65 && b.y - b.h / 2 <= a.y + .4 && distance(a, b) <= 23).sort((p, q) => distance(a, p) - distance(a, q)).slice(0, 18);
      for (const b of nearby) {
        if (b.structure?.active === false || b.y + b.h / 2 < a.y - .65 || b.y - b.h / 2 > a.y + .4 || distance(a, b) > 23) continue;
        const dx = b.x - threat.x, dz = b.z - threat.z, len = Math.hypot(dx, dz) || 1, r = (a.r || .52) + .45;
        const hide = { x: b.x + dx / len * (b.w / 2 + r), y: a.y, z: b.z + dz / len * (b.d / 2 + r) };
        if (!walkable(hide, a) || sight(threat, hide)) continue;
        // An oblique angle can remain occluded just beyond the near corner;
        // probe a few modest offsets until the entire muzzle ray clears it.
        const corners = [r, r + 1.1, r + 2.2].flatMap(offset => [{ x: b.x - b.w / 2 - offset, z: hide.z }, { x: b.x + b.w / 2 + offset, z: hide.z }, { x: hide.x, z: b.z - b.d / 2 - offset }, { x: hide.x, z: b.z + b.d / 2 + offset }]);
        const peek = corners.map(p => ({ ...p, y: a.y })).filter(p => walkable(p, a) && sight(p, threat) && traverse(hide, p, a)).sort((p, q) => distance(hide, p) - distance(hide, q))[0];
        if (!peek) continue;
        const score = distance(a, hide) + distance(hide, peek) * .6;
        if (score < bestScore) { bestScore = score; best = { hide, peek, block: b.structure, until: clock() + 2.5 + random() * 1.5 }; }
      }
      return best;
    }
    function choose(a, s, now) {
      const p = PROFILES[profileName(a)] || PROFILES.balanced, cfg = settings(), threat = s.enemy || s.memory, ideal = (host.idealRange?.(a) || 27) * p.range, learned = adaptation(a);
      a.tacticalLearning = learned;
      let goal = null, tactic = 'patrol', cover = s.decision?.cover;
      const injured = a.hp / (a.maxHp || 100) < p.retreat + Math.max(0, -learned.confidence) * .1, reloading = a.reload > 0, cautious = learned.confidence < -.2;
      if (threat) {
        const d = distance(a, threat), dx = (threat.x - a.x) / (d || 1), dz = (threat.z - a.z) / (d || 1);
        const friends = (host.actors?.() || []).filter(o => o !== a && alive(o) && host.sameTeam?.(a, o) && distance(a, o) < 20);
        if (!cover || cover.block?.active === false || now > cover.until || !walkable(cover.hide, a) || sight(threat, cover.hide)) cover = null;
        if (!cover && now >= (s.coverSearchAt || 0) && (injured || reloading || (s.exposed > 2.2 || cautious) && random() < clamp(p.cover - learned.confidence * .4, .2, 1))) { cover = findCover(a, threat); s.coverSearchAt = now + .8; }
        if (cover) {
          const cycle = (now + s.seed) % 2.1, hiding = injured || reloading || cycle < .8 || now < s.pauseUntil;
          goal = hiding ? cover.hide : cover.peek; tactic = injured ? 'retreat' : hiding ? 'cover' : 'peek';
        } else if (injured || reloading) {
          goal = { x: a.x - dx * 8 - dz * s.side * 5, y: a.y, z: a.z - dz * 8 + dx * s.side * 5 }; tactic = 'retreat';
        } else if (!s.enemy) { goal = { ...threat }; tactic = 'investigate'; }
        else if ((profileName(a) === 'flanker' || s.exposed > 3.5 || friends.length > 1 || cautious) && d > 12 && random() < clamp(p.flank - learned.confidence * .35, .15, 1) * cfg.teamwork) {
          // A wider side destination and exposure-weighted path creates a real alternate route.
          goal = { x: threat.x - dx * ideal * .55 - dz * s.side * ideal * .7, y: a.y, z: threat.z - dz * ideal * .55 + dx * s.side * ideal * .7 }; tactic = friends.length ? 'crossfire' : 'flank';
        } else {
          const desired = profileName(a) === 'aggressor' && s.enemy.hp < s.enemy.maxHp * .4 ? ideal * .5 : ideal;
          const forward = clamp((d - desired) / Math.max(8, ideal * .65), -.7, 1) * p.push * learned.risk;
          goal = { x: a.x + dx * forward * 6 - dz * s.side * 3, y: a.y, z: a.z + dz * forward * 6 + dx * s.side * 3 };
          tactic = profileName(a) === 'defender' && Math.abs(d - ideal) < 9 ? 'hold-route' : friends.length && cfg.teamwork > .5 ? 'push' : 'engage';
          if (profileName(a) === 'marksman') {
            const firing = navigation().nodes.filter(n => distance(a, n) < 19 && Math.abs(distance(n, threat) - ideal) < 12 && walkable(n, a) && sight(n, threat)).sort((x, y) => (distance(a, x) - Math.min(6, x.y - a.y)) - (distance(a, y) - Math.min(6, y.y - a.y)))[0];
            if (firing && firing.y > a.y + 1) { goal = firing; tactic = 'high-ground'; }
          }
        }
      } else {
        const pickup = host.pickupGoal?.(a);
        if (pickup) { goal = pickup; tactic = 'resupply'; }
        else if (s.decision?.tactic === 'patrol' && distance(a, s.decision.goal) > 3) goal = s.decision.goal;
        else { const choices = navigation().nodes.filter(n => distance(a, n) > 8 && walkable(n, a)); goal = choices.length ? choices[Math.floor(random() * choices.length)] : { x: a.x, y: a.y, z: a.z }; }
      }
      if (!walkable(goal, a)) {
        const fallback = navigation().nodes.filter(n => distance(n, goal) < 15 && walkable(n, a)).sort((x, y) => distance(x, goal) - distance(y, goal))[0];
        goal = fallback || { x: a.x, y: a.y, z: a.z };
      }
      let route = findPath(a, goal, ['flank', 'crossfire', 'retreat'].includes(tactic) ? threat : null);
      if (!route.length) {
        // Do not drive into an unreachable target. Investigate a reachable nearby branch.
        const branches = navigation().nodes.filter(n => distance(a, n) > 3 && distance(a, n) < 16 && walkable(n, a) && traverse(a, n, a)).sort((x, y) => distance(x, goal) - distance(y, goal));
        if (branches.length) { route = [branches[Math.min(branches.length - 1, Math.floor(random() * 3))]]; if (tactic === 'patrol') goal = route[0]; }
      }
      s.decision = { goal, route, routeIndex: 0, tactic, cover, threatId: threat?.id || threat?.entityId };
      s.nextThink = now + cfg.decision * (.85 + random() * .3);
      return s.decision;
    }
    function update(a, dt) {
      const s = state(a), now = clock(), cfg = settings(), p = PROFILES[profileName(a)] || PROFILES.balanced;
      if (s.nextSense == null || now >= s.nextSense) { perceive(a, s, now); s.nextSense = now + .12; }
      // Losing physical sight immediately removes the live target, including between decisions.
      if (s.enemy && (!alive(s.enemy) || s.enemy.spawnGrace > 0 || !sight(a, s.enemy))) { s.enemy = null; s.lostAt = now; s.nextSense = now + .08; s.nextThink = 0; }
      const visible = s.enemy;
      s.exposed = visible ? s.exposed + dt : Math.max(0, s.exposed - dt * 2);
      if (distance(a, s.anchor) > 1.5) { s.anchor = { x: a.x, z: a.z }; s.lastProgress = now; }
      else if (now - s.lastProgress > 1.8 && s.decision?.route.length) { s.nextThink = 0; s.side *= -1; s.lastProgress = now; paths.clear(); }
      const decision = !s.decision || now >= s.nextThink ? choose(a, s, now) : s.decision;
      while (decision.routeIndex < decision.route.length - 1 && distance(a, decision.route[decision.routeIndex]) < 1.35 && Math.abs(a.y - decision.route[decision.routeIndex].y) < .8) decision.routeIndex++;
      const waypoint = decision.route[decision.routeIndex];
      if (waypoint && !traverse(a, waypoint, a)) { s.nextThink = 0; }
      const d = waypoint ? distance(a, waypoint) : 0, move = waypoint && d > .65 && traverse(a, waypoint, a) ? { x: (waypoint.x - a.x) / d, z: (waypoint.z - a.z) / d } : { x: 0, z: 0 };
      const speedMultiplier = (['flank', 'crossfire', 'retreat', 'resupply'].includes(decision.tactic) ? p.sprint * 1.12 : .86) * (d < 2 ? clamp(d / 2, .25, 1) : 1);
      const wantSlide = a.onGround && d > 4 && visible && now >= (s.slideAt || 0) && (decision.tactic === 'retreat' || decision.tactic === 'crossfire' || profileName(a) === 'aggressor' && distance(a, visible) < 18);
      if (wantSlide) s.slideAt = now + 4 + random() * 3;
      s.recoil = Math.max(0, s.recoil - dt * .07);
      let aim = null;
      if (visible) {
        const range = distance(a, visible), lead = Math.min(.24, range / 180) * (cfg.teamwork + .2), targetYaw = Math.atan2(visible.x + (visible.vx || 0) * lead - a.x, visible.z + (visible.vz || 0) * lead - a.z);
        const movement = Math.hypot(a.vx || 0, a.vz || 0), error = cfg.error * (1 + movement / 17 + (a.slideTimer > 0 ? .7 : 0)) + .003;
        const desiredYaw = targetYaw + Math.sin(now * 2.7 + s.seed) * error + Math.sin(now * 7.1 + s.seed) * error * .2;
        const targetPitch = Math.atan2(eye(a).y - aimHeight(visible), Math.max(.01, range)) + Math.cos(now * 3.4 + s.seed) * error * .5 - s.recoil;
        s.yaw += clamp(angleDelta(desiredYaw, s.yaw), -cfg.turn * dt, cfg.turn * dt); s.pitch += (targetPitch - s.pitch) * clamp(dt * cfg.turn * 2, 0, 1);
        const aligned = Math.abs(angleDelta(targetYaw, s.yaw)) < Math.max(.06, .65 / Math.max(4, range)) + cfg.error;
        aim = { yaw: s.yaw, pitch: s.pitch, error, reactionReady: now >= s.acquired, fire: now >= s.acquired && now >= s.pauseUntil && aligned && !['cover', 'retreat'].includes(decision.tactic), target: visible };
      } else if (move.x || move.z) { s.yaw += clamp(angleDelta(Math.atan2(move.x, move.z), s.yaw), -cfg.turn * dt, cfg.turn * dt); s.pitch *= Math.max(0, 1 - dt * 8); }
      a.tacticalState = decision.tactic; a.tacticLabel = p.label;
      return { enemy: visible, memory: s.memory ? { ...s.memory } : null, goal: decision.goal, waypoint, move, speedMultiplier, wantSlide, aim, yaw: s.yaw, pitch: s.pitch, tactic: decision.tactic };
    }
    function recordShot(a) {
      const s = state(a), cfg = settings();
      if (s.burstLeft <= 0) s.burstLeft = Math.max(2, cfg.burst + Math.floor(random() * 3) - 1);
      s.burstLeft--; s.recoil = Math.min(.035, s.recoil + .006);
      if (s.burstLeft <= 0) { s.pauseUntil = clock() + cfg.pause * (.75 + random() * .5); s.nextThink = Math.min(s.nextThink, clock() + .12); s.side = random() < .25 ? -s.side : s.side; }
    }
    return { update, hear, reset, clear, recordOutcome, adaptation, recordShot, findPath, findCover, stats: () => ({ cachedPaths: paths.size, pathSearches: searches, deferredSearches, sounds: sounds.length }) };
  }
  const api = Object.freeze({ PROFILES, DIFFICULTY, profileName, createController });
  root.NeonTacticalAI = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof window !== 'undefined' ? window : globalThis);
