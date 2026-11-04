/* IRONFIELD — persistent army plans.
 * A plan assigns a job and a sequence of physical destinations. It never moves
 * a soldier, changes weapon statistics, or replaces an explicit player order.
 * All travel goes through BattleEngine's usual terrain and body collision.
 */
(function (global) {
  'use strict';
  const D = global.GameData, TYPES = D.TYPES, clamp = D.clamp;
  const active = r => r.alive > 0 && !r._routed;
  const eligible = r => active(r) && r.order === 'auto' && !r.fallingBack && r._rallyTime <= 0;
  const horse = r => r.type === 'cavalry' || r.type === 'lancer' || r.type === 'horseArcher';
  const extended = new Set(['steady', 'rapid', 'refused', 'breakthrough', 'elastic', 'echelon']);
  const gap = (a, b) => Math.hypot(Math.max(0, a._actualMinX - b._actualMaxX, b._actualMinX - a._actualMaxX), Math.max(0, a._actualMinY - b._actualMaxY, b._actualMinY - a._actualMaxY));

  class BattleTactics {
    constructor(engine) {
      this.engine = engine;
      this.plans = [null, null];
      this.serial = 0;
    }

    invalidate(team) {
      this.plans[team] = null;
      for (const r of this.engine.regiments) if (r.team === team) {
        if (eligible(r)) this._release(r, true);
        r._tactic = null;
      }
    }

    onSplit(parent, child, originalCenter) {
      const p = this.plans[parent.team];
      if (!p || !p.jobs || !p.jobs.has(parent.id)) return;
      const original = p.jobs.get(parent.id);
      const shifted = r => {
        const dx = r._centroidX - originalCenter.x, dy = r._centroidY - originalCenter.y;
        return Object.assign({}, original, { x: original.x + dx, y: original.y + dy,
          normal: original.normal + dx * p.nx + dy * p.ny,
          lateral: original.lateral + dx * p.tx + dy * p.ty });
      };
      const parentJob = shifted(parent), childJob = shifted(child);
      childJob.index = p.jobs.size;
      if (p.id === 'rapid' && original.role === 'envelop') {
        // The current translated waypoints remain independent and untouched.
        // A later replan gives the detachment its own unused outer lane.
        let outer = original.lane || 0;
        for (const job of p.jobs.values()) if (job.objective === original.objective && job.side === original.side && Number.isFinite(job.lane)) outer = Math.max(outer, job.lane);
        childJob.lane = outer + 1; childJob.lanes = Math.max(original.lanes || 1, childJob.lane + 1);
      }
      p.jobs.set(parent.id, parentJob); p.jobs.set(child.id, childJob);
      const objective = p.objectives && p.objectives[original.objective];
      if (objective && !objective.assigned.includes(child.id)) objective.assigned.push(child.id);
    }

    _release(r, capture) {
      const e = this.engine;
      if (e.impacts) { e.impacts.cancelPressure(r); e.impacts.cancelFriendly(r); }
      // Discard traffic whose destination belonged to the previous phase.
      for (const traffic of e._trafficPlans.slice()) if (traffic.a === r.id || traffic.b === r.id) e._clearTraffic(traffic);
      r._path.length = 0; r._pathTimer = 0;
      r._approachEnemy = -1; r._localFront = false;
      r._assignmentUntil = 0;
      for (const id of r.unitIds) {
        const u = e.units[id];
        if (!u.alive || u.routed) continue;
        e._setTarget(u, -1);
        u._sense = 0; u._localPath.length = 0; u._localTimer = 0;
        u._portalCorner = null; u._portalGoal = null; u._portalUntil = 0;
        u._circulateUntil = 0; u._circulateNext = 0; u._jamTime = 0;
        u._yieldUntil = 0; u._detour = null;
      }
      if (capture) {
        const c = e._currentCenter(r);
        r.x = r._centroidX = c.x; r.y = r._centroidY = c.y;
        r._combatActive = false; r._missileActive = false;
        e._captureMarchLayout(r);
      }
    }

    _stage(r, stage, phaseName, options) {
      const s = r._tactic, changed = s.stage !== stage;
      if (changed) {
        this._release(r, !!(options && (options.transit || options.hold)) || s.hold);
        s.stage = stage; s.stageSince = this.engine.time;
        s.phaseName = phaseName;
        s.transit = false; s.hold = false; s.charge = false;
        Object.assign(s, options || {});
        s.progressDistance = null; s.progressAt = this.engine.time; s.contactSince = null;
        if (s.hold) {
          const c = this.engine._currentCenter(r);
          s.goalX = r.x = c.x; s.goalY = r.y = c.y;
          for (const id of r.unitIds) {
            const u = this.engine.units[id];
            if (u.alive && !u.routed) { u._tacticHoldX = u.x; u._tacticHoldY = u.y; }
          }
        }
      }
      return changed;
    }

    _context(team) {
      const e = this.engine;
      const friends = e.regiments.filter(r => r.team === team && active(r));
      const enemies = e.regiments.filter(r => r.team !== team && active(r));
      const center = list => {
        let x = 0, y = 0, n = 0;
        for (const r of list) { x += r._centroidX * r.alive; y += r._centroidY * r.alive; n += r.alive; }
        return { x: x / Math.max(1, n), y: y / Math.max(1, n) };
      };
      return { friends, enemies, own: center(friends), foe: center(enemies) };
    }

    _plan(team, context) {
      const e = this.engine, id = e.config.armies[team].doctrine;
      let p = this.plans[team];
      if (p && p.id === id) return p;
      const dx = context.foe.x - context.own.x, dy = context.foe.y - context.own.y, length = Math.hypot(dx, dy);
      // Freeze the deployment axis. Casualties must not rotate every job.
      const nx = length > 1e-6 ? dx / length : team === 0 ? 1 : -1, ny = length > 1e-6 ? dy / length : 0;
      p = { id, serial: ++this.serial, team, nx, ny, tx: -ny, ty: nx, origin: context.own,
        centerT: context.own.x * -ny + context.own.y * nx, started: e.time, engaged: false, lanes: new Map(), sides: { '-1': [], '1': [] } };
      const riders = context.friends.filter(r => horse(r) && !TYPES[r.type].commander);
      riders.sort((a, b) => (a._centroidX - b._centroidX) * p.tx + (a._centroidY - b._centroidY) * p.ty || a.id - b.id);
      for (let i = 0; i < riders.length; i++) {
        const side = id === 'refuseLeft' ? -1 : id === 'refuseRight' ? 1 : i < riders.length / 2 ? -1 : 1;
        p.sides[side].push(riders[i]);
      }
      p.horseHalf = Math.max(55, ...riders.map(r => Math.max(r.width, r.depth) * .5));
      p.horseDepth = Math.max(80, ...riders.map(r => r.depth));
      p.lanePitch = p.horseHalf * 2 + 55;
      for (const side of [-1, 1]) {
        // Inner riders use the inner lane; outer riders stay outside them.
        // Reversing this ordering sends entire formations through each other
        // before they have even begun to round the enemy wing.
        p.sides[side].sort((a, b) => side * ((a._centroidX - b._centroidX) * p.tx + (a._centroidY - b._centroidY) * p.ty) || a.id - b.id);
        for (let i = 0; i < p.sides[side].length; i++) {
          const r = p.sides[side][i];
          p.lanes.set(r.id, { side, index: i, count: p.sides[side].length, normal: r._centroidX * p.nx + r._centroidY * p.ny });
        }
      }
      this.plans[team] = p;
      if (extended.has(id)) this._preparePlan(p, context);
      return p;
    }

    _preparePlan(p, context) {
      const e = this.engine;
      const fighters = context.friends.filter(r => !TYPES[r.type].commander);
      const ordered = fighters.slice().sort((a, b) => (a._centroidX - b._centroidX) * p.tx + (a._centroidY - b._centroidY) * p.ty || a.id - b.id);
      p.jobs = new Map();
      for (let i = 0; i < ordered.length; i++) {
        const r = ordered[i];
        p.jobs.set(r.id, { index: i, count: ordered.length, x: r._centroidX, y: r._centroidY,
          normal: r._centroidX * p.nx + r._centroidY * p.ny,
          lateral: r._centroidX * p.tx + r._centroidY * p.ty });
      }
      if (p.id === 'rapid') { this._prepareEncirclement(p, context); return; }
      if (p.id === 'refused') {
        // Refuse the wing with greater enemy pressure. The choice persists;
        // one casualty must not flip the whole army's left/right jobs.
        const pressure = [-1, 1].map(side => context.enemies.reduce((sum, foe) => {
          const lateral = foe._centroidX * p.tx + foe._centroidY * p.ty - p.centerT;
          return sum + (side * lateral >= 0 ? foe.alive * (TYPES[foe.type].mounted ? 1.4 : 1) : 0);
        }, 0));
        p.refusedSide = pressure[0] >= pressure[1] ? -1 : 1;
      }
      if (p.id === 'breakthrough') {
        // A weak local sector becomes the assault objective. Nearby defending
        // ranks count toward its strength, preventing a fake gap behind them.
        let target = null, best = Infinity;
        for (const foe of context.enemies) {
          if (TYPES[foe.type].commander || foe.fallingBack) continue;
          let strength = 0;
          for (const other of context.enemies) if (gap(foe, other) < 230) strength += other.alive * (TYPES[other.type].armorClass === 'heavy' ? 1.35 : 1);
          const distance = Math.hypot(foe._centroidX - context.own.x, foe._centroidY - context.own.y);
          const value = strength * 2 + distance * .45 + (TYPES[foe.type].brace ? 120 : 0);
          if (value < best) { best = value; target = foe; }
        }
        p.objectiveId = target ? target.id : -1;
        const seam = target ? target._centroidX * p.tx + target._centroidY * p.ty : p.centerT;
        const assault = fighters.filter(r => !TYPES[r.type].ranged).sort((a, b) => {
          const score = r => Math.abs(p.jobs.get(r.id).lateral - seam) - (horse(r) ? 60 : 0);
          return score(a) - score(b) || a.id - b.id;
        });
        // Only a manageable first wave converges on the seam. Others protect
        // its shoulders or follow after a real opening, rather than piling up.
        const frontCount = Math.max(1, Math.min(3, Math.ceil(assault.length * .5)));
        assault.sort((a, b) => p.jobs.get(a.id).lateral - p.jobs.get(b.id).lateral || a.id - b.id);
        const nearest = assault.slice().sort((a, b) => Math.abs(p.jobs.get(a.id).lateral - seam) - Math.abs(p.jobs.get(b.id).lateral - seam) || a.id - b.id).slice(0, frontCount);
        nearest.sort((a, b) => p.jobs.get(a.id).lateral - p.jobs.get(b.id).lateral || a.id - b.id);
        const pitch = Math.max(150, ...nearest.map(r => r.width + 38));
        for (const r of assault) {
          const job = p.jobs.get(r.id), at = nearest.indexOf(r);
          job.assault = at >= 0; job.wave = at >= 0 ? 0 : 1;
          job.lane = at >= 0 ? seam + (at - (nearest.length - 1) / 2) * pitch : job.lateral;
        }
      }
      if (p.id === 'echelon') {
        const fronts = fighters.filter(r => !TYPES[r.type].ranged);
        const waves = Math.max(1, Math.min(4, fronts.length));
        p.echelonSide = context.foe.y >= context.own.y ? -1 : 1;
        const byWing = fronts.slice().sort((a, b) => p.echelonSide * (p.jobs.get(b.id).lateral - p.jobs.get(a.id).lateral) || a.id - b.id);
        for (let i = 0; i < byWing.length; i++) p.jobs.get(byWing[i].id).wave = Math.floor(i * waves / byWing.length);
        p.waves = waves;
      }
    }

    _enemyGroups(context, p) {
      const remaining = context.enemies.filter(r => !TYPES[r.type].commander && !r.fallingBack).sort((a, b) => a.id - b.id);
      const groups = [], spanLimit = Math.min(3000, Math.max(1800, this.engine.width * .25));
      while (remaining.length) {
        const members = [remaining.shift()];
        let changed = true;
        while (changed) {
          changed = false;
          for (let i = remaining.length - 1; i >= 0; i--) {
            const candidate = remaining[i];
            if (!members.some(member => gap(member, candidate) <= 320)) continue;
            const list = members.concat(candidate), front = this._bounds(list, p.nx, p.ny), across = this._bounds(list, p.tx, p.ty);
            // Limit both extents: a chain of remote squads cannot turn into
            // an all-map objective with an impractically long outside route.
            if (front.hi - front.lo > spanLimit || across.hi - across.lo > spanLimit) continue;
            members.push(candidate); remaining.splice(i, 1); changed = true;
          }
        }
        const strength = members.reduce((n, r) => n + r.alive, 0);
        const x = members.reduce((n, r) => n + r._centroidX * r.alive, 0) / strength;
        const y = members.reduce((n, r) => n + r._centroidY * r.alive, 0) / strength;
        groups.push({ id: groups.length, ids: members.map(r => r.id), strength, x, y, assigned: [], load: 0 });
      }
      return groups;
    }

    _prepareEncirclement(p, context) {
      p.objectives = this._enemyGroups(context, p);
      if (!p.objectives.length) return;
      const largest = Math.max(...p.objectives.map(g => g.strength));
      const mobiles = context.friends.filter(r => !TYPES[r.type].commander).sort((a, b) => {
        const nearest = r => Math.min(...p.objectives.map(g => Math.hypot(g.x - r._centroidX, g.y - r._centroidY)));
        return nearest(a) - nearest(b) || a.id - b.id;
      });
      for (const r of mobiles) {
        let choice = null, best = Infinity;
        for (const group of p.objectives) {
          const distance = Math.hypot(group.x - r._centroidX, group.y - r._centroidY);
          const tiny = group.strength < Math.min(70, largest * .2) ? 420 : 0;
          const saturation = Math.max(0, (group.load + r.alive * .35) / Math.max(45, group.strength) - .8);
          const score = distance + saturation * 650 + tiny;
          if (score < best) { best = score; choice = group; }
        }
        if (!choice) continue;
        choice.assigned.push(r.id); choice.load += r.alive * (TYPES[r.type].ranged ? .55 : 1);
        p.jobs.get(r.id).objective = choice.id;
      }
      for (const group of p.objectives) {
        const assigned = group.assigned.map(id => this.engine.regiments[id]);
        const infantry = assigned.filter(r => !TYPES[r.type].ranged && !TYPES[r.type].mounted);
        const centerT = group.x * p.tx + group.y * p.ty;
        infantry.sort((a, b) => Math.abs(p.jobs.get(a.id).lateral - centerT) - Math.abs(p.jobs.get(b.id).lateral - centerT) || a.id - b.id);
        const pins = new Set(infantry.slice(0, Math.max(1, Math.ceil(infantry.length * .4))).map(r => r.id));
        const wings = assigned.filter(r => !pins.has(r.id) && !TYPES[r.type].siege && (!TYPES[r.type].ranged || horse(r)));
        const sides = { '-1': [], '1': [] };
        wings.sort((a, b) => p.jobs.get(a.id).lateral - p.jobs.get(b.id).lateral || a.id - b.id);
        for (let i = 0; i < wings.length; i++) {
          const r = wings[i], job = p.jobs.get(r.id);
          job.side = wings.length === 1 ? job.lateral <= centerT ? -1 : 1 : i < wings.length / 2 ? -1 : 1;
          job.role = 'envelop'; sides[job.side].push(r);
        }
        for (const side of [-1, 1]) {
          sides[side].sort((a, b) => side * (p.jobs.get(a.id).lateral - p.jobs.get(b.id).lateral) || a.id - b.id);
          const half = Math.max(55, ...sides[side].map(r => Math.max(r.width, r.depth) * .5));
          const pitch = half * 2 + 55;
          for (let i = 0; i < sides[side].length; i++) Object.assign(p.jobs.get(sides[side][i].id), { lane: i, lanes: sides[side].length, laneHalf: half, lanePitch: pitch });
        }
        for (const r of assigned) if (!p.jobs.get(r.id).role) p.jobs.get(r.id).role = TYPES[r.type].ranged ? 'support' : 'pin';
      }
    }

    _objective(s) {
      return (s.targetIds || []).map(id => this.engine.regiments[id]).filter(r => r && active(r) && !r.fallingBack);
    }

    _scope(s, members) {
      s.targetIds = members.map(r => r.id); s.targetCount = members.length;
      s.targetStrength = members.reduce((n, r) => n + r.alive, 0);
      s.scopeLabel = members.length > 1 ? 'Enemy position' : 'Single formation';
    }

    _marchTo(r, stage, label, point, extra) {
      this._stage(r, stage, label, Object.assign({ transit: true }, extra || {}));
      r._tactic.goalX = point.x; r._tactic.goalY = point.y;
    }

    _arrived(r, margin) {
      const s = r._tactic;
      return Math.hypot(r._centroidX - s.goalX, r._centroidY - s.goalY) <= (margin || Math.max(48, Math.min(95, r.depth * .4)));
    }

    _rapidMembers(r, p, context) {
      const e = this.engine, s = r._tactic;
      let members = this._objective(s);
      if (!members.length) {
        // Re-evaluate local connected positions after an objective breaks.
        // Falling back to one nearest soldier group here would forget the
        // scope decision as soon as the first encirclement succeeded.
        const groups = this._enemyGroups(context, p);
        let best = Infinity, chosen = null;
        for (const group of groups) {
          const load = context.friends.reduce((n, other) => {
            const assigned = other._tactic;
            return n + (other.id !== r.id && eligible(other) && assigned && assigned.serial === p.serial && assigned.targetIds && assigned.targetIds.some(id => group.ids.includes(id)) ? other.alive : 0);
          }, 0);
          const score = Math.hypot(group.x - r._centroidX, group.y - r._centroidY) + Math.max(0, load / Math.max(45, group.strength) - 1) * 650;
          if (score < best) { best = score; chosen = group; }
        }
        if (chosen) members = chosen.ids.map(id => e.regiments[id]);
        else {
          const target = this._target(r, p, context, horse(r));
          if (!target) return [];
          members = [target];
        }
      }
      this._scope(s, members);
      return members;
    }

    _rapidTarget(r, p, context, members) {
      let target = null, best = Infinity;
      for (const candidate of members) {
        const committed = context.friends.reduce((n, other) => n + (other.id !== r.id && eligible(other) && other._tactic && other._tactic.serial === p.serial && other._tactic.targetId === candidate.id ? other.alive : 0), 0);
        const dx = candidate._centroidX - r._centroidX, dy = candidate._centroidY - r._centroidY;
        const score = Math.hypot(dx, dy) + Math.abs(dx * p.tx + dy * p.ty) * .3 + committed / Math.max(45, candidate.alive) * 330 - (candidate.id === r._tactic.targetId ? 180 : 0);
        if (score < best) { best = score; target = candidate; }
      }
      return target;
    }

    _rapidRoute(r, p, context, repeat) {
      const e = this.engine, s = r._tactic, job = p.jobs.get(r.id);
      const members = this._rapidMembers(r, p, context);
      if (!members.length) return false;
      const front = this._bounds(members, p.nx, p.ny), across = this._bounds(members, p.tx, p.ty);
      // Every wing lane shares the largest footprint on that side. Using an
      // individual regiment's width for lane×pitch can place a small outer
      // formation on top of a broad inner one before either reaches combat.
      const half = job.laneHalf || Math.max(r.width, r.depth) * .5, clearance = half + 95;
      const side = s.side || job.side || -1;
      const currentN = r._centroidX * p.nx + r._centroidY * p.ny;
      const rear = front.hi + half + 110;
      let limits = this._lateralLimits(p, rear, half + 25);
      const rearProbe = this._position(r, p, rear, (across.lo + across.hi) * .5);
      const rearBlocked = limits.lo >= limits.hi || rearProbe.x * p.nx + rearProbe.y * p.ny < front.hi + r.depth * .5 + 35;
      // A defender backed against the world edge has no rear rendezvous.
      // Compute the guarded-flank approach on its feasible front plane,
      // rather than interpreting an empty lateral interval as world zero.
      if (rearBlocked) limits = this._lateralLimits(p, Math.min(currentN + 150, front.lo - half - 150), half + 25);
      const edge = side < 0 ? across.lo : across.hi;
      const space = side < 0 ? edge - limits.lo : limits.hi - edge;
      const pitch = job.lanePitch || Math.max(r.width, r.depth) + 55;
      const capacity = Math.max(1, Math.floor((space - clearance) / pitch) + 1);
      const lane = Math.min(capacity - 1, job.lane || 0), wave = Math.floor((job.lane || 0) / capacity);
      const outside = clamp(edge + side * (clearance + lane * pitch), limits.lo, limits.hi);
      const room = side < 0 ? edge - outside : outside - edge;
      if (room < half + 25 && !repeat && !rearBlocked) { s.side = -side; return this._rapidRoute(r, p, context, true); }
      s.side = side; s.lane = lane; s.wave = wave;
      const target = members.slice().sort((a, b) => side * ((b._centroidX - a._centroidX) * p.tx + (b._centroidY - a._centroidY) * p.ty) || a.id - b.id)[0];
      s.targetId = target.id;
      const approach = Math.min(currentN + 150, front.lo - half - 150) - wave * (r.depth + 70);
      const a = this._position(r, p, approach, outside), b = this._position(r, p, rear, outside);
      const targetT = target._centroidX * p.tx + target._centroidY * p.ty;
      const c = this._position(r, p, rear, targetT + side * Math.min(70, r.width * .15));
      s.routeConstrained = rearBlocked || room < half + 25;
      s.waypoints = s.routeConstrained ? [a] : [a, b, c]; s.waypointIndex = 0;
      s.detail = s.targetCount > 1 ? 'Pin the connected enemy position while separated wings clear its whole frontage and close behind it.' : 'Round this isolated formation while other forces cover their own local objectives.';
      if (s.routeConstrained) s.detail = 'The boundary closes the rear route. Work toward the accessible flank, then commit to that local fight.';
      this._marchTo(r, 'rapid-spread', side < 0 ? 'Rapid left envelopment' : 'Rapid right envelopment', a, { speedScale: horse(r) ? 1.12 : 1.10 });
      if (!s.routeConstrained && currentN > front.hi + half) {
        s.waypointIndex = 2; this._marchTo(r, 'rapid-rear', 'Closing the rear of the position', c, { speedScale: 1.1 });
      }
      return true;
    }

    _initialExtended(r, p, context) {
      const e = this.engine, s = r._tactic, t = TYPES[r.type];
      let job = p.jobs.get(r.id);
      if (!job) {
        // A detachment created during battle gets an independent local job.
        job = { index: p.jobs.size, count: p.jobs.size + 1, x: r._centroidX, y: r._centroidY,
          normal: r._centroidX * p.nx + r._centroidY * p.ny, lateral: r._centroidX * p.tx + r._centroidY * p.ty };
        p.jobs.set(r.id, job);
      }
      s.originX = job.x; s.originY = job.y;
      if (p.id === 'steady') {
        s.role = t.ranged ? 'support' : t.mounted ? 'reserve' : 'steady-line';
        s.speedScale = t.ranged || t.mounted ? .76 : .64;
        s.movingBrace = !t.ranged && !t.mounted && !t.siege;
        if (t.mounted) { this._stage(r, 'steady-reserve', 'Keeping cavalry behind the steady line', { hold: true, speedScale: .76 }); return; }
        const target = this._target(r, p, context, false);
        s.targetId = target ? target.id : -1;
        if (t.ranged) { s.stage = 'steady-support'; s.phaseName = 'Supporting the deliberate advance'; return; }
        this._steadyGoal(r, p, context);
        return;
      }
      if (p.id === 'rapid') {
        const objective = p.objectives && p.objectives[job.objective];
        if (objective) this._scope(s, objective.ids.map(id => e.regiments[id]).filter(active));
        s.role = job.role || (t.ranged && !horse(r) ? 'support' : 'envelop');
        s.side = job.side || (job.lateral <= p.centerT ? -1 : 1);
        s.speedScale = s.role === 'envelop' ? 1.10 : 1;
        if (s.role === 'envelop') { this._rapidRoute(r, p, context); return; }
        s.stage = 'rapid-pin'; s.phaseName = s.role === 'support' ? 'Covering the encircling wings' : 'Pinning the enemy position';
        const target = this._rapidTarget(r, p, context, this._rapidMembers(r, p, context));
        s.targetId = target ? target.id : -1; return;
      }
      if (p.id === 'refused') {
        const refused = p.refusedSide * (job.lateral - p.centerT) >= 0;
        s.role = refused ? 'refused-wing' : t.ranged ? 'support' : 'attack-wing';
        if (refused) {
          const inset = Math.max(150, Math.min(310, Math.abs(job.lateral - p.centerT) * .28));
          const destination = this._position(r, p, job.normal - 230 - Math.min(180, Math.abs(job.lateral - p.centerT) * .24), job.lateral - p.refusedSide * inset);
          this._marchTo(r, 'refuse-fold', 'Drawing back the exposed wing', destination, { speedScale: .92 });
        } else { s.stage = 'refuse-attack'; s.phaseName = 'Advancing the strengthened wing'; s.speedScale = 1.05; }
        return;
      }
      if (p.id === 'breakthrough') {
        s.targetId = p.objectiveId;
        const target = e.regiments[p.objectiveId];
        if (!target) { s.stage = 'press'; return; }
        if (t.ranged) { s.role = 'support'; s.stage = 'breakthrough-fire'; s.phaseName = 'Concentrating fire on the selected seam'; return; }
        s.role = job.assault ? 'spearhead' : 'reserve';
        if (!job.assault) { this._stage(r, 'breakthrough-reserve', 'Holding the supporting wave', { hold: true }); return; }
        const front = e._projectFrontBounds(target, p.nx, p.ny).lo;
        this._marchTo(r, 'breakthrough-form', 'Mass the assault at the weak sector', this._position(r, p, Math.min(job.normal + 100, front - r.depth * .5 - 260), job.lane), { speedScale: .96 });
        return;
      }
      if (p.id === 'elastic') {
        s.role = t.ranged ? 'elastic-screen' : 'elastic-line'; s.withdrawals = 0;
        this._stage(r, 'elastic-receive', t.ranged ? 'Firing from the first defense line' : 'Waiting at the first defense line', { hold: true, faceAngle: Math.atan2(p.ny, p.nx) });
        return;
      }
      if (p.id === 'echelon') {
        s.role = t.ranged ? 'support' : 'echelon'; s.wave = job.wave || 0;
        if (t.ranged) { s.stage = 'echelon-fire'; s.phaseName = 'Supporting the successive assault'; return; }
        if (s.wave === 0) { s.stage = 'echelon-advance'; s.phaseName = 'Leading the first echelon'; s.speedScale = 1; }
        else this._stage(r, 'echelon-wait', 'Holding echelon ' + (s.wave + 1), { hold: true });
      }
    }

    _steadyGoal(r, p, context) {
      const s = r._tactic, t = TYPES[r.type], job = p.jobs.get(r.id);
      const enemy = this._nearestThreat(r, context) || this._enemy(r, context);
      if (!enemy) return;
      s.targetId = enemy.id;
      if (gap(r, enemy) <= t.range + 28) {
        this._stage(r, 'steady-contact', 'Pressing in cohesive ranks', { movingBrace: true, speedScale: .64 });
        return;
      }
      const front = this.engine._projectFrontBounds(enemy, p.nx, p.ny).lo;
      const across = this.engine._projectFrontBounds(enemy, p.tx, p.ty);
      const lateral = clamp(job.lateral, across.lo - Math.max(20, r.width * .18), across.hi + Math.max(20, r.width * .18));
      // Preserve an existing useful file, but guide an outer wing onto a
      // reachable frontage. Never keep adding forward distance after passing
      // an enemy whose narrow front missed the original deployment lane.
      const destination = this._position(r, p, front - r.depth * .5 - t.range - 12, lateral);
      this._marchTo(r, 'steady-march', 'Advancing under a walking brace', destination, { movingBrace: true, speedScale: .64 });
      s.detail = 'Advance deliberately in the existing files. A straight, cohesive walk keeps a partial brace; stopping gives polearms their full brace.';
    }

    _advanceRapid(r, p, context) {
      const e = this.engine, s = r._tactic;
      let members = this._objective(s);
      if (!members.length) {
        if (s.stage !== 'rapid-reassign') this._stage(r, 'rapid-reassign', 'Choosing the next local objective');
        s.targetIds = []; s.routeRetries = 0; this._rapidRoute(r, p, context); return;
      }
      this._scope(s, members);
      if (s.charge || s.stage === 'rapid-harass') {
        if (!members.some(other => other.id === s.targetId)) s.targetId = members[0].id;
        return;
      }
      const front = this._bounds(members, p.nx, p.ny);
      const ownRear = e._projectFrontBounds(r, p.nx, p.ny).lo;
      const passed = s.stage === 'rapid-round' && ownRear > front.hi + 25;
      const rearDeployment = s.stage === 'rapid-rear' && r._centroidX * p.nx + r._centroidY * p.ny > front.hi + TYPES[r.type].radius * 2 &&
        members.some(enemy => gap(r, enemy) < TYPES[r.type].range + 125);
      const remaining = Math.hypot(r._centroidX - s.goalX, r._centroidY - s.goalY);
      if (s.progressDistance === null) s.progressDistance = remaining;
      let stalled = false;
      if (e.time - s.progressAt >= 4) {
        stalled = s.progressDistance - remaining < TYPES[r.type].speed * .55;
        s.progressDistance = remaining; s.progressAt = e.time;
      }
      const pressing = r.engagedSoldiers >= Math.max(1, Math.ceil(r.alive * .12));
      if (pressing) { if (s.contactSince === null) s.contactSince = e.time; }
      else s.contactSince = null;
      if (!passed && !rearDeployment && ((stalled && r._contactDistance < TYPES[r.type].range + 65) || (s.contactSince !== null && e.time - s.contactSince > (r.alive <= 16 ? .8 : 1.8)))) {
        const blocked = this._nearestThreat(r, context); if (blocked) s.targetId = blocked.id;
        this._stage(r, 'rapid-intercept', 'Breaking through the intercepted wing', { charge: true, speedScale: 1.08 }); return;
      }
      if (stalled && !passed && !rearDeployment && e.time - s.stageSince > 6) {
        s.routeRetries = (s.routeRetries || 0) + 1;
        if (s.routeRetries <= 2) {
          // A closed terrain/traffic lane gets a bounded fresh route, with
          // one deliberate wing change. Never oscillate at every frame or
          // wait forever for a large enough percentage of cavalry to fight.
          this._release(r, true);
          if (s.routeRetries === 1) s.side = -s.side;
          this._rapidRoute(r, p, context);
          s.progressDistance = null; s.progressAt = e.time; s.stageSince = e.time;
          s.detail = 'The outside route stopped making progress. Replanning a physical route around the available wing.';
        } else {
          const nearest = this._rapidTarget(r, p, context, members);
          if (nearest) s.targetId = nearest.id;
          this._stage(r, 'rapid-local', 'Pressing the available local approach', { charge: true, speedScale: 1.04 });
          s.detail = 'Both outside routes were constrained. Commit through the reachable local approach instead of stalling in transit.';
        }
        return;
      }
      if (!this._arrived(r) && !passed && !rearDeployment) return;
      if (s.routeConstrained) { this._stage(r, 'rapid-intercept', 'Attacking the available flank', { charge: true, speedScale: 1.08 }); return; }
      if (s.waypointIndex === 0) {
        s.waypointIndex = 1;
        this._marchTo(r, 'rapid-round', 'Passing the entire enemy wing', s.waypoints[1], { speedScale: 1.12 });
      } else if (s.waypointIndex === 1) {
        if (r.type === 'horseArcher') { this._stage(r, 'rapid-harass', 'Firing into the exposed rear'); return; }
        // Select a rear target at the phase boundary, but keep the bounding
        // rear of every connected squad. Turning at the first squad would
        // cut through the rest of the position the player asked to surround.
        const target = members.slice().sort((a, b) => Math.hypot(a._centroidX - r._centroidX, a._centroidY - r._centroidY) - Math.hypot(b._centroidX - r._centroidX, b._centroidY - r._centroidY) || a.id - b.id)[0];
        s.targetId = target.id;
        s.waypoints[2] = this._position(r, p, front.hi + r.depth * .5 + 100, target._centroidX * p.tx + target._centroidY * p.ty + s.side * Math.min(70, r.width * .15));
        s.waypointIndex = 2;
        this._marchTo(r, 'rapid-rear', 'Closing the rear of the position', s.waypoints[2], { speedScale: 1.1 });
      } else {
        this._stage(r, 'rapid-charge', 'Charging inward from the rear', { charge: true, speedScale: 1.12 });
        s.detail = 'Both approach and attack use actual terrain routes and body collisions; the wings now commit inward.';
      }
    }

    _updateExtended(r, p, context) {
      const e = this.engine, s = r._tactic, t = TYPES[r.type], job = p.jobs.get(r.id);
      if (p.id === 'rapid') {
        if (s.role === 'envelop') this._advanceRapid(r, p, context);
        else {
          const target = this._rapidTarget(r, p, context, this._rapidMembers(r, p, context));
          s.targetId = target ? target.id : -1;
        }
        return;
      }
      if (p.id === 'steady') {
        if (s.stage === 'steady-reserve') {
          if (p.engaged || e.time - p.started > 30) this._stage(r, 'steady-counter', 'Committing the cavalry reserve', { charge: true, speedScale: 1 });
        } else if (s.movingBrace && s.stage !== 'steady-contact') this._steadyGoal(r, p, context);
        else if (s.stage === 'steady-contact' && r._contactDistance > t.range + 200) this._steadyGoal(r, p, context);
        return;
      }
      if (p.id === 'refused') {
        if (s.stage === 'refuse-fold' && this._arrived(r)) {
          this._stage(r, 'refuse-guard', 'Guarding the shortened defensive wing', { hold: true, faceAngle: Math.atan2(p.ny, p.nx) });
        } else if (s.stage === 'refuse-guard') {
          const threat = this._nearestThreat(r, context);
          if ((threat && gap(r, threat) < t.range + 70) || e.time - p.started > 35) {
            s.targetId = threat ? threat.id : -1; this._stage(r, 'refuse-counter', 'Counterattacking from the refused wing', { charge: !t.ranged, speedScale: 1 });
          }
        }
        return;
      }
      if (p.id === 'breakthrough') {
        const target = this._enemy(r, context);
        if (!target) return;
        if (s.stage === 'breakthrough-form' && (this._arrived(r) || gap(r, target) < t.range + 100 || e.time - s.stageSince > 16)) {
          this._stage(r, 'breakthrough-strike', 'Driving the concentrated assault through', { charge: true, speedScale: 1.12 });
        } else if (s.stage === 'breakthrough-reserve') {
          const leaders = context.friends.filter(other => other._tactic && other._tactic.serial === p.serial && other._tactic.role === 'spearhead');
          const open = target.alive < target.initial * .65 || target.fallingBack;
          const contact = leaders.some(other => other.engagedSoldiers > 0 || other._contactDistance < TYPES[other.type].range + 40);
          if (open || contact || e.time - p.started > 22) {
            this._stage(r, 'breakthrough-follow', open ? 'Exploiting the opening' : 'Reinforcing the assault shoulders', { charge: true, speedScale: 1.04 });
          }
        }
        return;
      }
      if (p.id === 'elastic') {
        const threat = this._nearestThreat(r, context);
        const distance = threat ? gap(r, threat) : Infinity;
        const trigger = t.ranged ? Math.max(110, t.range * .44) : t.range + 160;
        if (s.stage === 'elastic-receive' || s.stage === 'elastic-second') {
          if (distance < trigger && s.withdrawals < 2 && e.time - s.stageSince > 2) {
            s.withdrawals++;
            const behind = this._position(r, p, r._centroidX * p.nx + r._centroidY * p.ny - 230,
              job.lateral + (s.withdrawals === 1 ? (job.index % 2 ? 35 : -35) : 0));
            this._marchTo(r, 'elastic-fall', 'Yielding to the next defense line', behind, { speedScale: .96 });
          } else if ((s.withdrawals >= 2 && distance < trigger) || e.time - p.started > 42) {
            s.targetId = threat ? threat.id : -1;
            this._stage(r, 'elastic-counter', 'Counterattacking the extended pursuit', { charge: !t.ranged, speedScale: 1.06 });
          }
        } else if (s.stage === 'elastic-fall' && (this._arrived(r) || e.time - s.stageSince > 12)) {
          this._stage(r, 'elastic-second', 'Reforming at the next defense line', { hold: true, speedScale: 1, faceAngle: Math.atan2(p.ny, p.nx) });
        }
        return;
      }
      if (p.id === 'echelon' && s.stage === 'echelon-wait') {
        const ahead = context.friends.filter(other => other._tactic && other._tactic.serial === p.serial && other._tactic.role === 'echelon' && other._tactic.wave === s.wave - 1);
        const advance = ahead.some(other => {
          const start = p.jobs.get(other.id);
          return (other._centroidX - start.x) * p.nx + (other._centroidY - start.y) * p.ny > 170 || other.engagedSoldiers > 0;
        });
        if ((advance && e.time - p.started > s.wave * 3) || e.time - p.started > s.wave * 7) {
          this._stage(r, 'echelon-advance', 'Committing echelon ' + (s.wave + 1), { speedScale: 1 });
          s.detail = 'This wave advances in its existing lane after the preceding wave opens the attack.';
        }
      }
    }

    _bounds(list, nx, ny) {
      let lo = Infinity, hi = -Infinity;
      for (const r of list) {
        const b = this.engine._projectFrontBounds(r, nx, ny);
        lo = Math.min(lo, b.lo); hi = Math.max(hi, b.hi);
      }
      return { lo, hi };
    }

    _fit(r, x, y) {
      const e = this.engine, margin = Math.min(280, Math.max(65, r.width * .5 + 24, r.depth * .5 + 24));
      return e._nearestPassable(clamp(x, margin, e.width - margin), clamp(y, margin, e.height - margin), TYPES[r.type].radius + 8);
    }

    _position(r, p, normal, lateral) {
      return this._fit(r, normal * p.nx + lateral * p.tx, normal * p.ny + lateral * p.ty);
    }

    _lateralLimits(p, normal, margin) {
      let lo = -Infinity, hi = Infinity;
      for (const [base, component, limit] of [[normal * p.nx, p.tx, this.engine.width], [normal * p.ny, p.ty, this.engine.height]]) {
        if (Math.abs(component) < 1e-6) {
          if (base < margin || base > limit - margin) return { lo: 0, hi: 0 };
          continue;
        }
        const a = (margin - base) / component, b = (limit - margin - base) / component;
        lo = Math.max(lo, Math.min(a, b)); hi = Math.min(hi, Math.max(a, b));
      }
      return { lo, hi };
    }

    _target(r, p, context, cavalry) {
      let best = null, score = Infinity;
      const lane = p.lanes.get(r.id), side = lane ? lane.side : 0;
      const commitments = new Map();
      if (cavalry) for (const other of context.friends) {
        if (other.id === r.id || !eligible(other) || !other._tactic || other._tactic.serial !== p.serial || other._tactic.targetId < 0) continue;
        const weight = TYPES[other.type].ranged ? .55 : 1;
        commitments.set(other._tactic.targetId, (commitments.get(other._tactic.targetId) || 0) + other.alive * weight);
      }
      for (const enemy of context.enemies) {
        if (enemy.fallingBack) continue;
        const t = TYPES[enemy.type], dx = enemy._centroidX - r._centroidX, dy = enemy._centroidY - r._centroidY;
        const distance = Math.hypot(dx, dy), lateral = enemy._centroidX * p.tx + enemy._centroidY * p.ty;
        const sameWing = side * (lateral - p.centerT);
        const load = cavalry ? (commitments.get(enemy.id) || 0) / Math.max(40, enemy.alive) : 0;
        const value = distance + (t.commander ? 500 : 0) + (cavalry ? (t.brace ? 350 : 0) - (t.ranged ? 180 : 0) - clamp(sameWing, -600, 600) * .25 + load * 500 : 0);
        if (value < score) { score = value; best = enemy; }
      }
      return best || context.enemies[0] || null;
    }

    _route(r, p, context) {
      const e = this.engine, s = r._tactic, lane = p.lanes.get(r.id) || { side: r.id % 2 ? 1 : -1, index: 0 };
      const target = this._target(r, p, context, true);
      if (!target) return false;
      s.targetId = target.id; s.side = lane.side; s.lane = lane.index;
      const solid = context.enemies.filter(other => !TYPES[other.type].commander && !TYPES[other.type].mounted && !other.fallingBack);
      const screen = solid.length ? solid : context.enemies.filter(other => !TYPES[other.type].commander);
      const front = this._bounds(screen.length ? screen : [target], p.nx, p.ny);
      const across = this._bounds(screen.length ? screen : [target], p.tx, p.ty);
      const currentN = r._centroidX * p.nx + r._centroidY * p.ny;
      const half = p.horseHalf;
      const clearance = half + 105;
      const side = lane.side;
      // Round this objective's rear, not the deepest unrelated artillery
      // position in the army. Entry waves already provide arrival spacing.
      const rearBase = e._projectFrontBounds(target, p.nx, p.ny).hi + half + 110;
      const edge = side < 0 ? across.lo : across.hi;
      const boundary = this._lateralLimits(p, rearBase, half + 25);
      const available = side < 0 ? edge - boundary.lo : boundary.hi - edge;
      const capacity = Math.max(1, Math.floor((available - clearance) / p.lanePitch) + 1);
      const slot = Math.min(capacity - 1, Math.floor(lane.index * capacity / Math.max(1, lane.count || 1)));
      const peers = (p.sides[side] || [r]).filter(other => {
        const assigned = p.lanes.get(other.id);
        return assigned && Math.min(capacity - 1, Math.floor(assigned.index * capacity / Math.max(1, assigned.count))) === slot;
      }).sort((a, b) => p.lanes.get(b.id).normal - p.lanes.get(a.id).normal || a.id - b.id);
      const wave = Math.max(0, peers.findIndex(other => other.id === r.id));
      s.lane = slot; s.wave = wave;
      const outside = edge + side * (clearance + slot * p.lanePitch);
      const approach = Math.min(currentN + 180, front.lo - half - 140) - wave * (p.horseDepth + 70);
      const rear = rearBase;
      const targetT = target._centroidX * p.tx + target._centroidY * p.ty;
      const a = this._position(r, p, approach, outside);
      const b = this._position(r, p, rear, outside);
      const c = this._position(r, p, rear, targetT + side * Math.min(75, r.width * .15));
      // If one outer wing touches the map edge, use the available one. This is
      // a choice made once for the maneuver, not a per-tick left/right flip.
      const projected = a.x * p.tx + a.y * p.ty;
      const room = side < 0 ? across.lo - projected : projected - across.hi;
      if (room < half + 35 && !s.triedOtherWing && p.id !== 'refuseLeft' && p.id !== 'refuseRight') {
        s.triedOtherWing = true;
        p.lanes.set(r.id, { side: -side, index: lane.index, count: lane.count, normal: lane.normal });
        return this._route(r, p, context);
      }
      // On a field whose entire width is screened, prefer the chosen wing's
      // edge target to an impossible off-map destination. The normal route
      // finder may still narrow at real terrain; it never crosses enemy bodies.
      s.routeConstrained = room < half + 35;
      s.waypoints = [a, b, c]; s.waypointIndex = 0;
      s.detail = s.routeConstrained ? 'Working toward the open edge of the enemy line.' : 'Take the outside lane, clear the enemy wing, then approach its rear.';
      if (s.routeConstrained) {
        let edgeEnemy = target, edgeScore = Infinity;
        for (const candidate of screen) {
          const score = -side * (candidate._centroidX * p.tx + candidate._centroidY * p.ty) + (TYPES[candidate.type].commander ? 400 : 0);
          if (score < edgeScore) { edgeScore = score; edgeEnemy = candidate; }
        }
        s.targetId = edgeEnemy.id;
        s.waypoints = [this._position(r, p, front.lo - r.depth * .5 - 95, edge + side * Math.max(0, Math.min(clearance, available - 20)))];
        this._stage(r, 'outflank', 'Approaching the guarded flank', { transit: true });
        s.goalX = s.waypoints[0].x; s.goalY = s.waypoints[0].y;
        s.detail = 'The field edge leaves no safe rear lane. Engage the outside formation instead of forcing a blocked circuit.';
        return true;
      }
      const currentT = r._centroidX * p.tx + r._centroidY * p.ty;
      const alreadyOutside = side < 0 ? currentT < across.lo - half - 25 : currentT > across.hi + half + 25;
      if (currentN > front.hi + r.depth * .5 + 45) {
        // Applying Encircle to troops already behind the line should exploit
        // that position, not send them back around the entire enemy army.
        s.waypointIndex = 2;
        this._stage(r, 'rear-approach', 'Forming behind the enemy', { transit: true });
      } else if (alreadyOutside && currentN >= approach - 30) {
        s.waypointIndex = 1;
        this._stage(r, 'round-wing', 'Passing the enemy wing', { transit: true });
      } else this._stage(r, 'outflank', side < 0 ? 'Riding around the left wing' : 'Riding around the right wing', { transit: true });
      const first = s.waypoints[s.waypointIndex];
      s.goalX = first.x; s.goalY = first.y;
      return true;
    }

    _initial(r, p, context) {
      const t = TYPES[r.type];
      r._tactic = { plan: p.id, serial: p.serial, epoch: r._commandEpoch, role: 'line', stage: '', stageSince: this.engine.time,
        phaseName: 'Advancing with the line', detail: '', targetId: -1, transit: false, hold: false, charge: false,
        waypoints: [], waypointIndex: 0, goalX: r._centroidX, goalY: r._centroidY };
      const s = r._tactic;
      if (t.commander) { s.role = 'command'; s.stage = 'command'; s.phaseName = 'Supporting the army'; return; }
      if (extended.has(p.id)) { this._initialExtended(r, p, context); return; }
      if (p.id === 'balanced') { s.stage = 'line'; s.role = t.ranged ? 'support' : 'line'; s.phaseName = t.ranged ? 'Supporting the local front' : 'Advancing toward the local front'; return; }
      if (horse(r) && ['flank', 'hammer', 'refuseLeft', 'refuseRight'].includes(p.id)) {
        s.role = t.ranged ? 'harass' : 'encircle';
        if (this._route(r, p, context)) return;
      }
      if (p.id === 'skirmish') {
        s.role = t.ranged && !t.siege ? 'skirmish' : 'screen';
        if (t.ranged && !t.siege) this._stage(r, 'approach', 'Approaching to skirmish');
        else this._stage(r, 'screen', 'Covering the missile line', { hold: true });
        return;
      }
      if (p.id === 'defensive' || p.id === 'hammer') {
        s.role = t.ranged ? 'support' : 'anvil';
        this._stage(r, 'guard', t.ranged ? 'Holding a firing position' : 'Receiving the enemy attack', { hold: true });
        return;
      }
      if (p.id === 'refuseLeft' || p.id === 'refuseRight') {
        const side = p.id === 'refuseLeft' ? -1 : 1;
        const lateral = (r._centroidX - p.origin.x) * p.tx + (r._centroidY - p.origin.y) * p.ty;
        if (side * lateral < -20) {
          s.role = 'reserve'; this._stage(r, 'reserve', 'Guarding the refused wing', { hold: true }); return;
        }
        s.role = 'advance'; s.detail = 'Lead with this wing; the opposite wing follows after contact.';
      }
      // Full assault commits after the engine has allocated frontage and
      // counted earlier attackers. Picking a nearest target here would send
      // every depth-stacked regiment to the same small part of the line.
      s.targetId = -1;
      s.stage = 'press'; s.phaseName = t.ranged ? 'Moving into firing range' : p.id === 'flank' ? 'Pinning the enemy front' : 'Pressing the attack';
    }

    _enemy(r, context) {
      const s = r._tactic, old = this.engine.regiments[s.targetId];
      if (old && active(old) && !old.fallingBack) return old;
      const p = this.plans[r.team], next = this._target(r, p, context, horse(r));
      s.targetId = next ? next.id : -1;
      return next;
    }

    _nearestThreat(r, context) {
      let best = null, distance = Infinity;
      for (const enemy of context.enemies) {
        if (enemy.fallingBack) continue;
        const next = gap(r, enemy);
        if (next < distance) { best = enemy; distance = next; }
      }
      return best;
    }

    _advanceRoute(r, p, context) {
      const e = this.engine, s = r._tactic, enemy = this._enemy(r, context);
      if (!enemy) { this._stage(r, 'press', 'Seeking another opponent'); return; }
      if (s.charge || s.stage === 'harass') return;
      if (s.stage === 'wait-contact') {
        if (p.engaged || e.time - s.stageSince > 12) {
          this._stage(r, 'rear-charge', 'Charging the enemy rear', { charge: true });
          s.detail = p.engaged ? 'The anvil is engaged. Strike the rear now.' : 'The enemy has not committed; exploit the completed rear approach.';
        }
        return;
      }
      const destination = s.waypoints[s.waypointIndex];
      if (!destination) return;
      s.goalX = destination.x; s.goalY = destination.y;
      const distance = Math.hypot(r._centroidX - destination.x, r._centroidY - destination.y);
      const targetRear = e._projectFrontBounds(enemy, p.nx, p.ny).hi;
      const ownRear = e._projectFrontBounds(r, p.nx, p.ny).lo;
      const passedWing = s.stage === 'round-wing' && ownRear > targetRear + Math.max(28, TYPES[r.type].radius * 2);
      // A destination is only a guide for passing the wing. If the current
      // physical ranks already cleared the objective's rear, turn in now;
      // do not march on toward an old deep waypoint as that enemy advances.
      if (s.progressDistance === null) s.progressDistance = distance;
      const pressing = r.engagedSoldiers >= Math.max(3, Math.ceil(r.alive * .18));
      if (pressing) { if (s.contactSince === null) s.contactSince = e.time; }
      else s.contactSince = null;
      let stalled = false;
      if (e.time - s.progressAt >= 4) {
        // Count progress toward the accepted waypoint, not sideways weaving
        // or circular movement that leaves the same route blocked forever.
        // A passing queue can creep a few pixels between repeated holds.
        // Judge useful progress over the actual observation interval; the
        // former fixed half-second distance called a quarter-speed queue
        // healthy indefinitely, even beside an available enemy flank.
        stalled = s.progressDistance - distance < TYPES[r.type].speed * (e.time - s.progressAt) * .30;
        s.progressDistance = distance; s.progressAt = e.time;
      }
      if (!passedWing && ((stalled && r._contactDistance < TYPES[r.type].range + 65) || (s.contactSince !== null && e.time - s.contactSince >= 1.6))) {
        const blocking = this._nearestThreat(r, context);
        if (blocking) s.targetId = blocking.id;
        this._stage(r, 'flank-charge', 'Engaging the intercepted flank', { charge: true });
        s.detail = 'An enemy formation intercepted the maneuver. Break through that local threat before seeking another opening.';
        return;
      }
      const arrival = Math.max(48, Math.min(110, r.depth * .45));
      // The rear rendezvous guides the approach; it is not a point every
      // rider must reach before the front can deploy against a nearby enemy.
      const rearDeployment = s.stage === 'rear-approach' &&
        r._centroidX * p.nx + r._centroidY * p.ny > targetRear + TYPES[r.type].radius * 2 &&
        gap(r, enemy) < TYPES[r.type].range + 125;
      if (distance > arrival && !passedWing && !rearDeployment) return;
      if (s.routeConstrained) {
        this._stage(r, 'flank-charge', 'Attacking the guarded flank', { charge: true });
        return;
      }
      if (s.waypointIndex === 0) {
        s.waypointIndex = 1;
        this._stage(r, 'round-wing', 'Passing the enemy wing', { transit: true });
      } else if (s.waypointIndex === 1) {
        if (r.type === 'horseArcher') {
          this._stage(r, 'harass', 'Harassing the exposed flank');
          s.detail = 'Fire from the flank and keep the formation outside the main melee.';
          return;
        }
        // Refresh the rear rendezvous only at this phase boundary; a moving
        // enemy must not leave cavalry chasing an old starting position.
        const back = this.engine._projectFrontBounds(enemy, p.nx, p.ny).hi;
        const lateral = enemy._centroidX * p.tx + enemy._centroidY * p.ty;
        s.waypoints[2] = this._position(r, p, back + r.depth * .5 + 110, lateral + s.side * Math.min(60, r.width * .12));
        s.waypointIndex = 2;
        this._stage(r, 'rear-approach', 'Forming behind the enemy', { transit: true });
      } else {
        if (p.id === 'hammer' && !p.engaged) {
          this._stage(r, 'wait-contact', 'Waiting for the anvil to engage', { hold: true });
        } else this._stage(r, 'rear-charge', 'Charging the enemy rear', { charge: true });
        s.detail = 'Close on the selected formation from the rear; nearby enemies can still defend their line.';
      }
      const next = s.waypoints[s.waypointIndex];
      if (s.transit && next) { s.goalX = next.x; s.goalY = next.y; }
    }

    _skirmish(r, p, context) {
      const e = this.engine, s = r._tactic, t = TYPES[r.type], enemy = this._nearestThreat(r, context) || this._enemy(r, context);
      if (!enemy) return;
      const distance = gap(r, enemy);
      if (s.role === 'screen') {
        if (distance < t.range + 145 || (p.engaged && distance < 430)) {
          this._stage(r, 'counter', 'Intercepting a pursuer');
          return;
        }
        // Cover the nearest friendly missile formation from behind. Move the
        // screen only when the gap has genuinely opened, then hold that line.
        let missile = null, nearest = Infinity;
        for (const other of context.friends) {
          if (!TYPES[other.type].ranged || TYPES[other.type].siege || other.fallingBack) continue;
          const distance = Math.hypot(other._centroidX - r._centroidX, other._centroidY - r._centroidY);
          if (distance < nearest) { nearest = distance; missile = other; }
        }
        if (!missile || s.stage === 'counter') return;
        const support = this._fit(r, missile._centroidX - p.nx * 220, missile._centroidY - p.ny * 220);
        if (s.hold && Math.hypot(support.x - r._centroidX, support.y - r._centroidY) > 380) {
          this._stage(r, 'screen-move', 'Following the missile screen', { transit: true });
          s.goalX = support.x; s.goalY = support.y;
        } else if (s.stage === 'screen-move' && Math.hypot(s.goalX - r._centroidX, s.goalY - r._centroidY) < 70) this._stage(r, 'screen', 'Covering the missile line', { hold: true });
        return;
      }
      const close = Math.max(75, t.range * .36), shooting = t.range * .83;
      if (s.stage === 'displace') {
        if (Math.hypot(s.goalX - r._centroidX, s.goalY - r._centroidY) < 65 || e.time - s.stageSince > 7) {
          this._stage(r, 'fire', 'Firing from the new line', { hold: true }); s.nextDisplace = e.time + 3;
        }
      } else if (distance < close && e.time >= (s.nextDisplace || 0)) {
        const dx = r._centroidX - enemy._centroidX, dy = r._centroidY - enemy._centroidY, length = Math.max(1, Math.hypot(dx, dy));
        const destination = this._fit(r, r._centroidX + dx / length * Math.min(240, t.range * .55), r._centroidY + dy / length * Math.min(240, t.range * .55));
        this._stage(r, 'displace', 'Falling back to a firing line', { transit: true });
        s.goalX = destination.x; s.goalY = destination.y; s.waypoints = [destination]; s.waypointIndex = 0;
      } else if (s.stage === 'approach' && distance < shooting) {
        const dx = enemy._centroidX - r._centroidX, dy = enemy._centroidY - r._centroidY, length = Math.max(1, Math.hypot(dx, dy));
        const nx = dx / length, ny = dy / length;
        const own = e._projectFrontBounds(r, nx, ny), foe = e._projectFrontBounds(enemy, nx, ny);
        const ownSide = e._projectFrontBounds(r, -ny, nx), foeSide = e._projectFrontBounds(enemy, -ny, nx);
        const overhang = Math.max(0, foeSide.lo - ownSide.lo, ownSide.hi - foeSide.hi);
        // Settle the rear ranks into range too. A front-edge distance alone
        // plants the first row and leaves the rest of a javelin unit idle.
        if (Math.hypot(Math.max(0, foe.lo - own.lo), overhang) < t.range * .94) this._stage(r, 'fire', 'Firing behind the screen', { hold: true });
      } else if (s.stage === 'fire' && distance > t.range + 140 && e.time - s.stageSince > 4) {
        this._stage(r, 'approach', 'Advancing to regain range');
      }
    }

    update() {
      const e = this.engine;
      if (e.phase === 'deployment') return;
      for (let team = 0; team < 2; team++) {
        const context = this._context(team), p = this._plan(team, context);
        if (!context.enemies.length) continue;
        p.engaged = context.friends.some(r => !horse(r) && !TYPES[r.type].ranged && !TYPES[r.type].commander && !r.fallingBack && (r.engagedSoldiers > 0 || r._contactDistance < TYPES[r.type].range + 30));
        for (const r of context.friends) {
          if (!eligible(r)) { r._tactic = null; continue; }
          if (!r._tactic || r._tactic.serial !== p.serial || r._tactic.epoch !== r._commandEpoch) this._initial(r, p, context);
          const s = r._tactic, t = TYPES[r.type];
          if (t.commander) continue;
          if (extended.has(p.id)) this._updateExtended(r, p, context);
          else if (s.role === 'encircle' || s.role === 'harass') this._advanceRoute(r, p, context);
          else if (p.id === 'skirmish') this._skirmish(r, p, context);
          else if (s.stage === 'guard') {
            const enemy = this._nearestThreat(r, context), threat = enemy ? gap(r, enemy) : Infinity;
            if (!t.ranged && threat < t.range + 120) this._stage(r, 'counter', 'Counterattacking the local approach');
            // Two stationary armies still finish an encounter. Once no attack
            // has arrived, the held line is explicitly released to advance.
            else if (e.time - p.started > 38 && !p.engaged) this._stage(r, 'counter', t.ranged ? 'Advancing to bring fire to bear' : 'Advancing after the enemy held back');
          } else if (s.stage === 'reserve' && (p.engaged || e.time - p.started > 20)) this._stage(r, 'press', 'Committing the second wing');
          if (s.charge) {
            const target = this._enemy(r, context);
            if (target) { s.goalX = target._centroidX; s.goalY = target._centroidY; }
          }
        }
      }
    }

    goal(r) {
      const s = r._tactic;
      if (!eligible(r) || !s || !(s.transit || s.hold)) return null;
      return { x: s.goalX, y: s.goalY };
    }
  }
  global.BattleTactics = BattleTactics;
})(typeof window !== 'undefined' ? window : globalThis);
