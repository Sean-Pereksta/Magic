/* IRONFIELD — cavalry momentum and constrained impact pressure.
 * Impacts move real bodies through the ordinary collision and terrain solver.
 * Vertical flight is a short rendering pose over that collision-checked path.
 */
(function (global) {
  'use strict';
  const D = global.GameData, T = D.TYPES, clamp = D.clamp, CELL = 32;
  const horse = u => u.type === 'cavalry' || u.type === 'lancer' || u.type === 'horseArcher';
  const shock = u => u.type === 'cavalry' || u.type === 'lancer';

  class BattleImpacts {
    constructor(engine) {
      this.engine = engine;
      this.active = new Set();
      this.pairs = new Map();
      this.cleanupAt = 0;
      this.dt = 1 / 30;
      this.stats = { contacts: 0, damage: 0, launched: 0, deepestChain: 0 };
      this._pressureAt = 0;
      this._pressurePairs = new Map();
      this._pressureBatches = [];
      this._pressureReceivers = [];
      this._pressureComponents = new Int32Array(engine.units.length);
      this._pressureMove = { x: 0, y: 0 };
      this._friendlyGoal = { x: 0, y: 0 };
      this._friendlyEngaged = new Int32Array(engine.units.length);
    }

    isHorse(u) { return horse(u); }

    power(u) {
      return horse(u) ? clamp(u._mountSpeed / Math.max(1, T[u.type].speed), 0, 1.2) ** 2 : 0;
    }

    _startMotion(u) {
      if (u._motionTick !== this.engine._tick) {
        u._motionTick = this.engine._tick; u._motionDistance = 0;
      }
    }

    recordMove(u, distance) {
      this._startMotion(u); u._motionDistance += Math.max(0, distance);
      this.engine._bodyX[u.id] = u.x; this.engine._bodyY[u.id] = u.y;
      this.engine._bodyZ[u.id] = this.engine.unitAltitude(u);
      if (u._bulkStepTick === this.engine._tick) this.engine.regiments[u.regiment]._rigidContinuityTick = -1;
      // Buckets are rebuilt once per simulation step. A cooperative sidestep
      // can move an already visited rider, so later searches include the
      // largest real displacement since that rebuild.
      this.engine._bodyHashDrift = Math.max(this.engine._bodyHashDrift || 0, u._motionDistance);
    }

    remainingMotion(u, dt) {
      this._startMotion(u);
      return Math.max(0, T[u.type].speed * dt * 1.7 - u._motionDistance);
    }

    _clearFriendly(u) {
      u._friendlyOffsetX = 0; u._friendlyOffsetY = 0; u._friendlyUntil = 0;
      u._friendlySource = -1; u._friendlySourceEpoch = -1;
      u._friendlyFlowTick = -1; u._friendlyCarryTick = -1;
    }

    cancelFriendly(r) {
      if (!horse(r)) return;
      const e = this.engine;
      for (const u of e.units) {
        const source = e.units[u._friendlySource];
        if (u.regiment !== r.id && (!source || source.regiment !== r.id)) continue;
        this._clearFriendly(u);
        u._friendlyEpoch = e.regiments[u.regiment]._commandEpoch;
      }
    }

    _friendlyHeld(r) {
      return !r._traffic && (r.order === 'hold' || (r.order === 'auto' && r._tactic && r._tactic.hold));
    }

    _friendlyAxis(u) {
      const e = this.engine, r = e.regiments[u.regiment];
      // Current commands own the direction. A freshly stopped or turned
      // horse may still carry last step's velocity or local movement intent.
      if (r.fallingBack) {
        const manualLeg = r._manualFallback && r._withdrawLeg;
        const dx = (manualLeg ? r._withdrawGoalX : r._goalX) - u.x;
        const dy = (manualLeg ? r._withdrawGoalY : r._goalY) - u.y, distance = Math.hypot(dx, dy);
        if (distance > 1) return { x: dx / distance, y: dy / distance };
        return { x: r.team === 0 ? -1 : 1, y: 0 };
      }
      if (r.order === 'charge') return { x: r._chargeNX, y: r._chargeNY };
      const angle = r._marchActive ? r._marchAngle : r._travelAngle;
      return { x: Math.cos(angle), y: Math.sin(angle) };
    }

    _friendlyReady(u, source, nx, ny) {
      const e = this.engine, r = e.regiments[u.regiment];
      if (!horse(u) || !u.alive || u.routed || r._routed || r._rallyTime > 0 || r.order === 'face' ||
          u._turnEnd > e.time || u._marchTurnEnd > e.time || u._impactUntil > e.time ||
          u._braceImpactUntil > e.time || u._pressureTick === e._tick || r._formationPressureUntil > e.time) return false;
      if (shock(u)) {
        // A deployed fighting front owns its supporting riders' lateral
        // positions. Courtesy traffic must not keep rearranging those files
        // while enemy impacts and the committed plunge resolve the contact.
        if (r._deployCombat && r._contactDistance < T[u.type].range + e._maxRadius + 110) return false;
        if (e._tick > 0 && this._friendlyEngaged[u.id] === e._tick) return false;
        // Target selection can change immediately after a kill or knockback.
        // Any live opposing body in actual melee reach still owns this front;
        // allies cannot move the horse aside to renew a spent charge.
        if (r._contactDistance < T[u.type].range + e._maxRadius + 110) {
          let engaged = false;
          this._near(u, T[u.type].range + e._maxRadius, 1 - u.team, v => {
            if (Math.hypot(v.x - u.x, v.y - u.y) <= T[u.type].range + v._radius &&
                e._lineClear(u.x, u.y, v.x, v.y, Math.min(u._radius, v._radius))) engaged = true;
          });
          if (engaged) { this._friendlyEngaged[u.id] = e._tick; return false; }
        }
      }
      if (!source) return !this._friendlyHeld(r);
      const sr = e.regiments[source.regiment];
      if ((r._traffic || sr._traffic) && (!r._traffic || !sr._traffic ||
          r._traffic.plan !== sr._traffic.plan || !r._traffic.plan.parallel)) return false;
      if (this._friendlyHeld(r)) return true;
      const axis = this._friendlyAxis(u);
      return axis.x * nx + axis.y * ny > .65;
    }

    _friendlyState(u) {
      const e = this.engine, r = e.regiments[u.regiment], source = e.units[u._friendlySource];
      if (u._friendlyEpoch !== r._commandEpoch || (source &&
          (!source.alive || source.routed || e.regiments[source.regiment]._commandEpoch !== u._friendlySourceEpoch))) {
        this._clearFriendly(u); u._friendlyEpoch = r._commandEpoch;
      }
      if (u._friendlyTick === e._tick) return;
      u._friendlyTick = e._tick; u._friendlyDistance = 0; u._friendlyDX = 0; u._friendlyDY = 0;
      const length = Math.hypot(u._friendlyOffsetX, u._friendlyOffsetY);
      if (length < .001 || this._friendlyHeld(r) || e.time <= u._friendlyUntil) return;
      const amount = Math.min(length, T[u.type].speed * .065 * this.dt);
      const nx = -u._friendlyOffsetX / length, ny = -u._friendlyOffsetY / length;
      let occupied = false;
      this._near(u, u._radius + e._maxRadius + length + 3, u.team, v => {
        const dx = v.x - u.x, dy = v.y - u.y, along = clamp(dx * nx + dy * ny, 0, length);
        const gap = (u._radius + v._radius) * .96 + 1;
        if (Math.hypot(dx - nx * along, dy - ny * along) < gap) occupied = true;
      });
      // Restore a slot gently, and only after the passing tail has actually
      // vacated it. A timer alone would pull the horse back into its neighbor.
      if (!occupied) {
        const scale = Math.max(0, 1 - amount / length);
        u._friendlyOffsetX *= scale; u._friendlyOffsetY *= scale;
      }
    }

    friendlyGoal(u, gx, gy) {
      this._friendlyState(u);
      const r = this.engine.regiments[u.regiment];
      this._friendlyGoal.x = gx; this._friendlyGoal.y = gy;
      if (u._friendlyOffsetX === 0 && u._friendlyOffsetY === 0) return this._friendlyGoal;
      if (this._friendlyHeld(r) || (r._traffic && !r._traffic.plan.parallel) || r.order === 'face' ||
          u._plungeUntil > this.engine.time || !this._friendlyReady(u)) return this._friendlyGoal;
      const axis = this._friendlyAxis(u);
      const lateral = -u._friendlyOffsetX * axis.y + u._friendlyOffsetY * axis.x;
      if (Math.abs(lateral) < .001) return this._friendlyGoal;
      const slot = this.engine._formationGoal(u, r);
      if (Math.hypot(gx - slot.x, gy - slot.y) > .01) return this._friendlyGoal;
      const rigid = this.engine._formationGoal(u, r, true);
      const elastic = -(gx - rigid.x) * axis.y + (gy - rigid.y) * axis.x;
      const additional = lateral - clamp(elastic, Math.min(0, lateral), Math.max(0, lateral));
      // A bend must not turn the last leg's sideways clearance into a new
      // forward destination. The player's route and regiment anchor stay put.
      this._friendlyGoal.x -= axis.y * additional; this._friendlyGoal.y += axis.x * additional;
      return this._friendlyGoal;
    }

    _friendlyScoot(u, nx, ny, wanted, source, driveNX, driveNY, depth, work) {
      const e = this.engine;
      if (depth > 2 || work.visited.has(u.id) || work.left <= 0 || !this._friendlyReady(u, source, driveNX, driveNY)) return 0;
      // A chain can reach a moving rider on a slightly different course.
      // Its neighbor's lateral step must not shove it against that order.
      if (depth > 0 && !this._friendlyHeld(e.regiments[u.regiment])) {
        const axis = this._friendlyAxis(u);
        if (nx * axis.x + ny * axis.y < -.000001) return 0;
      }
      work.visited.add(u.id); work.left--;
      this._friendlyState(u);
      const t = T[u.type], r = e.regiments[u.regiment];
      // An angled single-rider crossing needs more lateral travel than a
      // parallel file. Keep the usual step rate and finite slot boundary.
      const limit = Math.max(t.radius * 2.15 + 2, Math.min(32, r._spacing || 0)) * (work.crossing ? 1.6 : 1);
      const offsetAlong = u._friendlyOffsetX * nx + u._friendlyOffsetY * ny;
      const offset2 = u._friendlyOffsetX ** 2 + u._friendlyOffsetY ** 2;
      const offsetRoom = Math.max(0, -offsetAlong + Math.sqrt(Math.max(0, offsetAlong * offsetAlong + limit * limit - offset2)));
      // At a gallop, the normal moving-file shuffle cannot clear a horse's
      // width before the rider reaches it. A stationary ally has no forward
      // movement to budget: let it quickstep to the side in response to the
      // incoming rider's real speed, still subject to every ordinary sweep.
      const sideRate = this._friendlyHeld(r) ? Math.max(t.speed * .35,
        Math.min(t.speed * .72, Math.min(source._mountSpeed, source._mountDriveSpeed) * .60)) : t.speed * .35;
      let amount = Math.min(wanted, sideRate * this.dt - u._friendlyDistance,
        this.remainingMotion(u, this.dt) / 1.15, offsetRoom);
      if (amount < .01) return 0;
      if (depth < 2 && work.left > 0) {
        const next = [];
        this._near(u, u._radius + e._maxRadius + amount + 3, u.team, v => {
          if (!horse(v)) return;
          const dx = v.x - u.x, dy = v.y - u.y, along = dx * nx + dy * ny;
          const gap = (u._radius + v._radius) * .96;
          if (along > 0 && Math.hypot(dx - nx * clamp(along, 0, amount), dy - ny * clamp(along, 0, amount)) < gap + .2 &&
              this._friendlyReady(v, source, driveNX, driveNY)) next.push(v);
        });
        next.sort((a, b) => (a.x - b.x) * nx + (a.y - b.y) * ny || a.id - b.id);
        for (let i = 0; i < Math.min(2, next.length); i++)
          this._friendlyScoot(next[i], nx, ny, amount, source, driveNX, driveNY, depth + 1, work);
      }
      // Reduce a pure lateral step to the nearest real body contact. Keeping
      // this direction avoids converting a courtesy step into forward force.
      for (let team = 0; team < 2; team++) this._near(u, u._radius + e._maxRadius + amount + 3, team, v => {
        const dx = v.x - u.x, dy = v.y - u.y, along = dx * nx + dy * ny;
        const side = -dx * ny + dy * nx, gap = (u._radius + v._radius) * .96;
        if (along <= 0 || Math.abs(side) >= gap) return;
        amount = Math.min(amount, Math.max(0, along - Math.sqrt(Math.max(0, gap * gap - side * side)) - .015));
      });
      if (amount < .01) return 0;
      while (amount > .01 && (e._bodyBlocked(u.x + nx * amount, u.y + ny * amount, u._radius) ||
          !e._lineClear(u.x, u.y, u.x + nx * amount, u.y + ny * amount, u._radius))) amount *= .5;
      if (amount < .01) return 0;
      const movement = e._collideMovement(u, nx * amount, ny * amount, amount);
      let mx = movement.x, my = movement.y, moved = Math.hypot(mx, my);
      if (moved < .01 || Math.abs(mx * ny - my * nx) > .001 || mx * nx + my * ny < 0 ||
          e._bodyBlocked(u.x + mx, u.y + my, u._radius) || !e._lineClear(u.x, u.y, u.x + mx, u.y + my, u._radius)) return 0;
      // The solver may add a small overlap correction. Clip on its already
      // checked lateral ray so that courtesy and offset limits stay exact.
      if (moved > amount) { mx *= amount / moved; my *= amount / moved; moved = amount; }
      u.x += mx; u.y += my; this.recordMove(u, moved); u.travelDistance += moved;
      u._friendlyDistance += moved; u._friendlyDX += mx; u._friendlyDY += my;
      u._friendlyOffsetX += mx; u._friendlyOffsetY += my; u._friendlyUntil = e.time + .9;
      u._friendlySource = source.id; u._friendlySourceEpoch = e.regiments[source.regiment]._commandEpoch;
      u._friendlyEpoch = r._commandEpoch;
      if (this._friendlyHeld(r)) {
        if (u._holdPosition) { u._holdPosition.x += mx; u._holdPosition.y += my; }
        if (r._tactic && r._tactic.hold) { u._tacticHoldX += mx; u._tacticHoldY += my; }
      }
      // Keep the recipient's facing and genuine forward momentum. Sideways
      // courtesy movement is visible, but never feeds charge power.
      return moved;
    }

    // The planner nominates a pair of isolated riders in passable ground.
    // Keep the courses intact unless their actual velocities predict contact.
    _crossRider(u, r, axis, nx, ny) {
      const e = this.engine, pass = r._localRiderPass;
      if (pass.until < e.time || pass.ownEpoch !== r._commandEpoch) {
        r._localRiderPass = null; return;
      }
      const v = e.units[pass.unitId], vr = v && e.regiments[v.regiment], back = vr && vr._localRiderPass;
      if (!v || !back || back.unitId !== u.id || back.until < e.time ||
          back.ownEpoch !== vr._commandEpoch || back.otherEpoch !== r._commandEpoch ||
          pass.otherEpoch !== vr._commandEpoch || !horse(v) || v.team !== u.team ||
          r.alive !== 1 || vr.alive !== 1 || r._traffic || vr._traffic || r.fallingBack || vr.fallingBack ||
          r._terrainPressure >= .2 || vr._terrainPressure >= .2 || r._bridge || vr._bridge ||
          r._siegeJob || vr._siegeJob || r._contactDistance < T[u.type].range + 220 ||
          vr._contactDistance < T[v.type].range + 220 || !this._friendlyReady(v)) return;
      // Resolve once, before either rider's ordinary movement in this step.
      // The next rider must not predict using two different travel times.
      if (v._mountMoveTick === e._tick || axis.x * nx + axis.y * ny < .93) return;
      const other = this._friendlyAxis(v);
      if (axis.x * other.x + axis.y * other.y > .9) return;
      const us = Math.min(u._mountSpeed, u._mountDriveSpeed), vs = Math.min(v._mountSpeed, v._mountDriveSpeed);
      if (us < 2 || vs < 2 || u._mountNX * axis.x + u._mountNY * axis.y < .85 ||
          v._mountNX * other.x + v._mountNY * other.y < .85) return;

      const dx = v.x - u.x, dy = v.y - u.y, cx = axis.x * us - other.x * vs, cy = axis.y * us - other.y * vs;
      const closing2 = cx * cx + cy * cy, approach = dx * cx + dy * cy;
      if (closing2 < 1 || approach <= 0 || approach > closing2 * 1.15) return;
      const closing = Math.sqrt(closing2), tx = -cy / closing, ty = cx / closing;
      const offset = dx * tx + dy * ty;
      // In a fixed step one body moves before the other. Include that one
      // frame of unspent sideways travel relative to the common sweep.
      const sweep = this.dt * Math.max(Math.abs((axis.x * tx + axis.y * ty) * us),
        Math.abs((other.x * tx + other.y * ty) * vs));
      const gap = (u._radius + v._radius) * .96 + 1.75 + sweep;
      if (Math.abs(offset) >= gap) return;

      // The pair sees the same signed offset whichever rider steps first.
      // Open the existing gap; an exact head-on tie shares one handedness.
      const sign = offset < -.05 ? -1 : 1;
      const a = -axis.y * tx + axis.x * ty, b = -other.y * tx + other.x * ty;
      const share = (gap - sign * offset) / Math.max(.01, Math.abs(a) + Math.abs(b));
      const leadTime = Math.min(1.15, Math.max(.45, share / (.35 * Math.min(T[u.type].speed, T[v.type].speed)) + 2 * this.dt + .08));
      if (approach > closing2 * leadTime) return;
      const sideU = -Math.sign(a) * sign, sideV = Math.sign(b) * sign;
      const work = { visited: new Set(), left: 2, crossing: true };
      // Each moves perpendicular to its own accepted course. Starting at
      // terminal depth prevents the pair from pushing any third soldier.
      const movedV = sideV && this._friendlyScoot(v, -other.y * sideV, other.x * sideV, share,
        u, other.x, other.y, 2, work);
      const movedU = sideU && this._friendlyScoot(u, -axis.y * sideU, axis.x * sideU, share,
        v, axis.x, axis.y, 2, work);
      if (movedU + movedV > .01) { u._friendlyFlowTick = e._tick; v._friendlyFlowTick = e._tick; }
    }

    clearFriendly(u, nx, ny, amount, dt) {
      const e = this.engine, r = e.regiments[u.regiment];
      if (amount < .001 || (r._traffic && !r._traffic.plan.parallel) || !this._friendlyReady(u)) return;
      this._friendlyState(u);
      const axis = this._friendlyAxis(u);
      if (axis.x * nx + axis.y * ny < .65) return;
      if (r._localRiderPass) this._crossRider(u, r, axis, nx, ny);
      const reach = Math.min(42, amount + u._mountDriveSpeed * .22 + .2);
      const nearby = [];
      this._near(u, u._radius + e._maxRadius + reach + 3, u.team, v => {
        if (!horse(v)) return;
        const dx = v.x - u.x, dy = v.y - u.y, along = dx * nx + dy * ny;
        const lateral = -dx * ny + dy * nx, gap = (u._radius + v._radius) * .96 + .8;
        if (!(along > -.1 && along < gap + reach && Math.abs(lateral) < gap)) return;
        const held = this._friendlyHeld(e.regiments[v.regiment]);
        const ahead = held ? 0 : Math.max(0, v._vx * nx + v._vy * ny);
        const closing = Math.max(0, u._mountDriveSpeed - ahead);
        const preview = Math.min(reach, amount + closing * .22 + .2);
        if (along < gap + preview &&
            Math.hypot(dx - nx * clamp(along, 0, preview), dy - ny * clamp(along, 0, preview)) < gap &&
            this._friendlyReady(v, u, nx, ny)) nearby.push(v);
      });
      if (!nearby.length) return;
      nearby.sort((a, b) => (a.x - b.x) * nx + (a.y - b.y) * ny || a.id - b.id);
      const work = { visited: new Set(), left: 6 };
      for (let i = 0; i < Math.min(2, nearby.length); i++) {
        const v = nearby[i], dx = v.x - u.x, dy = v.y - u.y, lateral = -dx * ny + dy * nx;
        const gap = (u._radius + v._radius) * .96 + .8;
        const vr = e.regiments[v.regiment], held = this._friendlyHeld(vr);
        const ahead = held ? 0 : Math.max(0, v._vx * nx + v._vy * ny);
        const preview = Math.min(reach, amount + Math.max(0, u._mountDriveSpeed - ahead) * .22 + .2);
        const future = Math.max(0, dx * nx + dy * ny - (held ? preview : amount + .2));
        const needed = Math.sqrt(Math.max(0, gap * gap - future * future)) - Math.abs(lateral);
        if (needed < .02) continue;
        // An already moving neighbor can keep sharing this opened lane
        // after its bounded sidestep budget has been spent. This marks a
        // present physical brush, never a queued request or predicted vacancy.
        if (!held && ahead > T[v.type].speed * .25) u._friendlyFlowTick = e._tick;
        const side = Math.abs(lateral) > .3 ? Math.sign(lateral) : ((u.id * 17 + v.id * 31) % 11 < 5 ? -1 : 1);
        const va = held ? { x: nx, y: ny } : this._friendlyAxis(v);
        const oldVX = v.x, oldVY = v.y;
        const moved = this._friendlyScoot(v, -va.y * side, va.x * side, needed, u, nx, ny, 0, work);
        const cleared = Math.max(0, (-(v.x - oldVX) * ny + (v.y - oldVY) * nx) * side);
        let own = 0;
        if (!held || moved < Math.min(needed, T[v.type].speed * dt * .18))
          own = this._friendlyScoot(u, axis.y * side, -axis.x * side, Math.max(0, needed - cleared), u, nx, ny, 0, work);
        if (moved + own > .01) u._friendlyFlowTick = e._tick;
      }
    }

    constrainPressureMove(u, mx, my) {
      const e = this.engine, r = e.regiments[u.regiment];
      if (u._pressureTick !== e._tick || !this._canReceivePressure(r)) return null;
      const length = Math.hypot(u._pressureDX, u._pressureDY);
      if (length < .001) return null;
      const nx = u._pressureDX / length, ny = u._pressureDY / length;
      const along = mx * nx + my * ny;
      if (along >= 0) return null;
      // Combat slots can still make room sideways. They cannot undo the
      // ground this body has just physically conceded to a stronger front.
      this._pressureMove.x = mx - nx * along;
      this._pressureMove.y = my - ny * along;
      return this._pressureMove;
    }

    cancelPressure(r) {
      for (const [key, pair] of this._pressurePairs) {
        if (pair.source.id === r.id || pair.target.id === r.id) { pair.cancelled = true; this._pressurePairs.delete(key); }
      }
      const released = new Set([r.id]);
      for (const batch of this._pressureBatches) {
        if (batch.influences.some(influence => influence.pair.cancelled || (influence.links && influence.links.some(link => link.regiment === r.id)))) released.add(batch.target.id);
      }
      for (const other of this.engine.regiments) if (released.has(other.id) || other._pressureSourceId === r.id) {
        other._formationPressureSpeed = 0; other._formationPressureUntil = 0; other._pressureSourceId = -1;
        other._formationPressureNX = 0; other._formationPressureNY = 0;
        for (const id of other.unitIds) {
          const u = this.engine.units[id]; u._pressureTick = -1; u._pressureDX = 0; u._pressureDY = 0;
        }
      }
      this._pressureAt = 0;
    }

    _canReceivePressure(r) {
      return r.alive > 0 && !r._routed && !r.fallingBack && r._rallyTime <= 0 && r.order !== 'move' && r.order !== 'face' &&
        !this.engine._advancingAttack(r) && !r._traffic && !T[r.type].equipment;
    }

    _canExertPressure(r) {
      const t = T[r.type];
      return this._canReceivePressure(r) && !t.ranged && !t.commander && !r._frontTurning &&
        r.order !== 'hold' && !(r.order === 'auto' && r._tactic && (r._tactic.hold || r._tactic.transit));
    }

    _pressureComponent(seed, components) {
      const e = this.engine, marked = this._pressureComponents;
      if (marked[seed.id]) return components[marked[seed.id] - 1];
      const r = e.regiments[seed.regiment], component = { regiment: r, ids: [], seams: new Map(), maximum: seed.id };
      const mark = components.length + 1;
      components.push(component); component.ids.push(seed.id); marked[seed.id] = mark;
      // Connectivity is local and terrain checked. A distant straggler, or
      // soldiers separated by a rock, never inherit another body's retreat.
      const link = Math.max(T[r.type].radius * 2 + 5, Math.min(46, r._spacing * 1.85));
      for (let head = 0; head < component.ids.length; head++) {
        const u = e.units[component.ids[head]];
        this._near(u, link, r.team, v => {
          if (v.regiment !== r.id) {
            const dx = v.x - u.x, dy = v.y - u.y, reach = u._radius + v._radius + 9;
            if (dx * dx + dy * dy > reach * reach || !e._lineClear(u.x, u.y, v.x, v.y, Math.min(u._radius, v._radius))) return;
            let seam = component.seams.get(v.regiment);
            if (!seam) component.seams.set(v.regiment, seam = new Map());
            seam.set(v.id, u.id);
            return;
          }
          if (marked[v.id]) return;
          const dx = v.x - u.x, dy = v.y - u.y;
          if (dx * dx + dy * dy > link * link || !e._lineClear(u.x, u.y, v.x, v.y, Math.min(u._radius, v._radius))) return;
          marked[v.id] = mark; component.ids.push(v.id); component.maximum = Math.max(component.maximum, v.id);
        });
      }
      return component;
    }

    _pressureStrength(component, contacts, opponents, nx, ny, components) {
      const e = this.engine, r = component.regiment, t = T[r.type], pitch = Math.max(14, r._spacing);
      const lanes = new Set();
      let front = 0, brace = 0, running = 0;
      for (const id of contacts) {
        const u = e.units[id];
        front += u.x * nx + u.y * ny;
        lanes.add(Math.floor((-u.x * ny + u.y * nx) / pitch));
        const opponent = e.units[opponents.get(id)];
        brace += Math.max(e._braceStrength(u, opponent), e._steadyGuardStrength ? e._steadyGuardStrength(u, opponent) : 0);
        if (horse(u) && (u._mountSpeed > t.speed * .25 || u._plungeUntil > e.time || e.time - u._lastImpact < 1.1)) running++;
      }
      front /= Math.max(1, contacts.size); brace /= Math.max(1, contacts.size);
      const cohesion = .40 + .60 * clamp(r.cohesion / 100, 0, 1);
      const quality = .72 + t.morale * .003 + t.damage * .007 + t.armor * .012;
      let support = 0, low = Infinity, high = -Infinity;
      for (const id of component.ids) {
        const u = e.units[id], across = -u.x * ny + u.y * nx;
        low = Math.min(low, across); high = Math.max(high, across);
        const depth = front - u.x * nx - u.y * ny, lane = Math.floor(across / pitch);
        if (depth < -pitch * 1.1 || depth > pitch * 6.25 || (!lanes.has(lane) && !lanes.has(lane - 1) && !lanes.has(lane + 1))) continue;
        const health = .55 + .45 * clamp(u.hp / u.maxHp, 0, 1);
        support += health / (1 + Math.max(0, depth) / (pitch * 2.4));
      }
      const formed = r.formation === 'line' || r.formation === 'shieldwall';
      let reinforcement = 0;
      // Follow actual rear-facing contacts out through adjoining companies.
      // Farther reserves transmit less force, and a planted heavy line ends the chain.
      const backing = [], seen = new Set([component]);
      const pile = [{ body: component, links: [] }];
      for (let index = 0; index < pile.length; index++) {
        const layer = pile[index], distance = layer.links.length + 1;
        if (distance > 3) continue;
        for (const [regiment, seam] of layer.body.seams) {
          const ally = e.regiments[regiment];
          if (!this._canReceivePressure(ally) || T[ally.type].commander) continue;
          const joined = new Map();
          for (const [id, origin] of seam) {
            const v = e.units[id], u = e.units[origin], depth = front - v.x * nx - v.y * ny;
            if (depth < pitch * .45 || depth > pitch * 7 * distance || (v.x - u.x) * nx + (v.y - u.y) * ny > -2) continue;
            const lane = Math.floor((-v.x * ny + v.y * nx) / pitch);
            if (!lanes.has(lane) && !lanes.has(lane - 1) && !lanes.has(lane + 1)) continue;
            const body = this._pressureComponent(v, components);
            if (seen.has(body)) continue;
            let joints = joined.get(body);
            if (!joints) joined.set(body, joints = []);
            joints.push([id, origin]);
          }
          for (const [body, joints] of joined) {
            if (joints.length < 2) continue;
            seen.add(body);
            const mass = T[ally.type].mass, anchored = ally.order === 'hold' && mass > T[layer.body.regiment.type].mass * 1.2;
            const files = Math.min(joints.length * 2, body.ids.length);
            reinforcement += files * mass * (.4 + .6 * clamp(ally.cohesion / 100, 0, 1)) * .65 ** (distance - 1) * (anchored ? .2 : 1);
            if (anchored) continue;
            const next = { body, links: [...layer.links, { regiment, joints }] };
            pile.push(next); backing.push(next);
          }
        }
      }
      const base = support * t.mass * quality * cohesion;
      const strength = base + Math.min(base * 1.75, reinforcement);
      const resistance = strength * (1 + (formed ? .12 + (t.rankStiffness || 0) * .65 : 0) + brace * (r.type === 'pikeman' ? 2.8 : 1.3));
      const frontage = Math.max(1, (high - low) / pitch + 1);
      // Ordinary files spend some effort keeping their ranks together.
      // Firm shield ranks transfer it more efficiently; loose ranks cannot
      // turn their full nearby headcount into a disciplined sustained shove.
      const transfer = formed ? .86 + Math.min(.14, (t.rankStiffness || 0) * .24) : .60;
      return { force: strength * transfer * (horse(r) ? .65 : 1), resistance, coverage: clamp(contacts.size / Math.max(2, frontage * .6), 0, 1),
        running: running > 0, fronts: contacts.size, backing };
    }

    _scanPressure() {
      const e = this.engine, components = [], encounters = new Map();
      if (this._pressureComponents.length !== e.units.length) this._pressureComponents = new Int32Array(e.units.length);
      else this._pressureComponents.fill(0);
      // One nearest physical contact per soldier, from one team only. Each
      // bounded neighborhood is examined at most once in this periodic scan.
      for (const u of e.units) {
        if (u.team !== 0 || !u.alive || u.routed || u._impactUntil > e.time) continue;
        const r = e.regiments[u.regiment];
        if (r._contactDistance > 100 || (!this._canReceivePressure(r) && !this._canExertPressure(r))) continue;
        let nearest = null, distance2 = Infinity;
        this._near(u, u._radius + e._maxRadius + 23, 1, v => {
          const other = e.regiments[v.regiment];
          if ((!this._canExertPressure(r) && !this._canExertPressure(other)) || (!this._canReceivePressure(other) && !this._canExertPressure(other)) ||
              v._impactUntil > e.time || e.time - v._lastImpact < .9 || e.time - u._lastImpact < .9) return;
          const dx = v.x - u.x, dy = v.y - u.y, d2 = dx * dx + dy * dy;
          const reach = Math.min(u._radius + v._radius + 22, Math.max(T[u.type].ranged ? 0 : T[u.type].range + v._radius, T[v.type].ranged ? 0 : T[v.type].range + u._radius));
          if (d2 > reach * reach || d2 >= distance2 || !e._lineClear(u.x, u.y, v.x, v.y, Math.min(u._radius, v._radius))) return;
          nearest = v; distance2 = d2;
        });
        if (!nearest) continue;
        const a = this._pressureComponent(u, components), b = this._pressureComponent(nearest, components);
        const key = a.regiment.id + ':' + a.maximum + '/' + b.regiment.id + ':' + b.maximum;
        let encounter = encounters.get(key);
        if (!encounter) { encounter = { key, a, b, aFront: new Set(), bFront: new Set(), aOpponents: new Map(), bOpponents: new Map(), samples: [], nx: 0, ny: 0 }; encounters.set(key, encounter); }
        encounter.aFront.add(u.id); encounter.bFront.add(nearest.id);
        encounter.aOpponents.set(u.id, nearest.id); encounter.bOpponents.set(nearest.id, u.id);
        const length = Math.max(.01, Math.sqrt(distance2));
        encounter.nx += (nearest.x - u.x) / length; encounter.ny += (nearest.y - u.y) / length;
        if (encounter.samples.length < 24) encounter.samples.push({ a: u.id, b: nearest.id });
      }
      const previous = this._pressurePairs, previousFronts = new Map(), pairs = new Map(), batches = new Map();
      for (const pair of previous.values()) {
        const key = pair.source.id + '/' + pair.target.id;
        let fronts = previousFronts.get(key);
        if (!fronts) { fronts = []; previousFronts.set(key, fronts); }
        if (fronts.length < 8) fronts.push(pair);
      }
      for (const encounter of encounters.values()) {
        const a = encounter.a.regiment, b = encounter.b.regiment;
        const length = Math.hypot(encounter.nx, encounter.ny);
        if (length < Math.max(1, encounter.aFront.size * .35)) continue;
        const nx = encounter.nx / length, ny = encounter.ny / length;
        const sa = this._pressureStrength(encounter.a, encounter.aFront, encounter.aOpponents, nx, ny, components);
        const sb = this._pressureStrength(encounter.b, encounter.bFront, encounter.bOpponents, -nx, -ny, components);
        const aDrive = this._canExertPressure(a) && !sa.running && sa.fronts >= 2 ? (sa.force - sb.resistance) / Math.max(1, sa.force + sb.resistance) : -1;
        const bDrive = this._canExertPressure(b) && !sb.running && sb.fronts >= 2 ? (sb.force - sa.resistance) / Math.max(1, sb.force + sa.resistance) : -1;
        const forwards = aDrive >= bDrive, drive = Math.max(aDrive, bDrive);
        if (drive <= .12) continue;
        const source = forwards ? a : b, target = forwards ? b : a, component = forwards ? encounter.b : encounter.a;
        if (!this._canReceivePressure(target)) continue;
        const coverage = forwards ? sa.coverage : sb.coverage;
        const speed = Math.min(8, T[target.type].speed * .16) * clamp((drive - .12) / .55, 0, 1) * coverage;
        if (speed < .25) continue;
        const key = encounter.key + (forwards ? '>' : '<');
        let old = previous.get(key);
        if (!old) {
          // Losing or reconnecting a rear file does not restart pressure at
          // an otherwise unchanged live front. Match only overlapping contact
          // bodies under the same command; a new distant cohort gets no history.
          const fronts = previousFronts.get(source.id + '/' + target.id);
          if (fronts) for (const candidate of fronts) {
            if (candidate.cancelled || candidate.broken || candidate.sourceEpoch !== source._commandEpoch || candidate.targetEpoch !== target._commandEpoch ||
                candidate.nx * (forwards ? nx : -nx) + candidate.ny * (forwards ? ny : -ny) < .75) continue;
            let shared = 0;
            for (const sample of candidate.samples) {
              if ((encounter.aFront.has(sample.a) || encounter.bFront.has(sample.b)) && ++shared >= 2) { old = candidate; break; }
            }
            if (old) break;
          }
        }
        const continuous = old && !old.broken && old.sourceEpoch === source._commandEpoch &&
          old.targetEpoch === target._commandEpoch && e.time - old.seen < .5;
        const pair = { source, target, component, samples: encounter.samples, nx: forwards ? nx : -nx, ny: forwards ? ny : -ny, speed,
          sourceEpoch: source._commandEpoch, targetEpoch: target._commandEpoch, seen: e.time,
          since: continuous ? old.since : e.time, entrySpeed: continuous ? this._pressureSpeed(old, e.time) : 0 };
        pairs.set(key, pair);
        // Move the backing first so a crowded front can yield into the space it makes.
        for (const { body, links } of [...(forwards ? sb : sa).backing.reverse(), { body: component }]) {
          const batchKey = body.regiment.id + ':' + body.maximum;
          let batch = batches.get(batchKey);
          if (!batch) { batch = { target: body.regiment, epoch: body.regiment._commandEpoch, component: body, influences: [], nx: 0, ny: 0 }; batches.set(batchKey, batch); }
          batch.influences.push({ pair, links }); batch.nx += pair.nx * speed; batch.ny += pair.ny * speed;
        }
      }
      for (const batch of batches.values()) {
        const nx = batch.nx, ny = batch.ny;
        batch.component.ids.sort((a, b) => (e.units[b].x - e.units[a].x) * nx + (e.units[b].y - e.units[a].y) * ny || a - b);
      }
      this._pressurePairs = pairs; this._pressureBatches = Array.from(batches.values());
      this._pressureAt = e.time + .24;
    }

    _pressureSpeed(pair, now) {
      // New ranks joining a fight change its capacity immediately, but the
      // entire body must build up to that pace. A lost contact drops its drive.
      const elapsed = Math.max(0, now - Math.max(pair.seen, pair.since + .35));
      const change = pair.speed - pair.entrySpeed;
      return pair.entrySpeed + clamp(change, -8 * elapsed, (2 + .55 * pair.speed) * elapsed);
    }

    _validPressure(pair) {
      const e = this.engine;
      if (pair.cancelled || pair.broken || pair.source._commandEpoch !== pair.sourceEpoch || pair.target._commandEpoch !== pair.targetEpoch ||
          !this._canExertPressure(pair.source) || !this._canReceivePressure(pair.target) || e.time - pair.seen > .34) { pair.broken = true; return false; }
      let touching = 0;
      for (const sample of pair.samples) {
        const a = e.units[sample.a], b = e.units[sample.b];
        if (!a.alive || a.routed || !b.alive || b.routed || a._impactUntil > e.time || b._impactUntil > e.time) continue;
        const dx = b.x - a.x, dy = b.y - a.y, reach = a._radius + b._radius + 25;
        if (dx * dx + dy * dy <= reach * reach && ++touching >= 2) return true;
      }
      pair.broken = true;
      return false;
    }

    _backingJoined(joints) {
      const e = this.engine;
      let live = 0;
      for (const [rearId, frontId] of joints) {
        const rear = e.units[rearId], front = e.units[frontId];
        if (!rear.alive || !front.alive || rear.routed || front.routed || rear._impactUntil > e.time || front._impactUntil > e.time) continue;
        const dx = rear.x - front.x, dy = rear.y - front.y, reach = rear._radius + front._radius + 9;
        if (dx * dx + dy * dy <= reach * reach && ++live >= 2) return true;
      }
      return false;
    }

    _stepPressure(dt) {
      const e = this.engine;
      for (const r of this._pressureReceivers) { r._formationPressureSpeed = 0; r._formationPressureUntil = 0; r._pressureSourceId = -1; }
      this._pressureReceivers.length = 0;
      if (e.time >= this._pressureAt) this._scanPressure();
      for (const batch of this._pressureBatches) {
        const r = batch.target;
        if (batch.epoch !== r._commandEpoch || !this._canReceivePressure(r)) continue;
        let vx = 0, vy = 0, strongest = null, largest = 0;
        for (const { pair, links } of batch.influences) {
          if (!this._validPressure(pair) || (links && !links.every(link => this._backingJoined(link.joints)))) continue;
          const speed = this._pressureSpeed(pair, e.time);
          vx += pair.nx * speed; vy += pair.ny * speed;
          if (speed > largest) { largest = speed; strongest = pair.source; }
        }
        const length = Math.hypot(vx, vy);
        if (length < .15 || !strongest) continue;
        const nx = vx / length, ny = vy / length, speed = Math.min(8, T[r.type].speed * .16, length);
        let movedX = 0, movedY = 0, movedCount = 0;
        for (const id of batch.component.ids) {
          const u = e.units[id];
          if (!u.alive || u.routed || u.regiment !== r.id || u._impactUntil > e.time) continue;
          const amount = Math.min(speed * dt, this.remainingMotion(u, dt));
          if (amount < .002) continue;
          const movement = e._collideMovement(u, nx * amount, ny * amount, amount);
          let mx = movement.x, my = movement.y, distance = Math.hypot(mx, my);
          if (distance > amount) { mx *= amount / distance; my *= amount / distance; }
          const forward = mx * nx + my * ny, sideways = Math.abs(my * nx - mx * ny);
          // A blocked rear can stop the advance. It cannot turn the entire
          // formation into a sideways sliding shove around a rock or heavy ally.
          if (forward < amount * .15 || sideways > forward * .75 || e._bodyBlocked(u.x + mx, u.y + my, u._radius) ||
              !e._lineClear(u.x, u.y, u.x + mx, u.y + my, u._radius)) continue;
          const oldX = u.x, oldY = u.y;
          u.x += mx; u.y += my; mx = u.x - oldX; my = u.y - oldY;
          distance = Math.hypot(mx, my); this.recordMove(u, distance); u.travelDistance += distance;
          if (u._pressureTick !== e._tick) { u._pressureTick = e._tick; u._pressureDX = 0; u._pressureDY = 0; }
          u._pressureDX += mx; u._pressureDY += my;
          if (u._holdPosition) { u._holdPosition.x += mx; u._holdPosition.y += my; }
          if (r._tactic && r._tactic.hold) { u._tacticHoldX += mx; u._tacticHoldY += my; }
          u._braceTime = 0;
          movedX += mx; movedY += my; movedCount++;
        }
        if (!movedCount) continue;
        const dx = movedX / Math.max(1, r.alive), dy = movedY / Math.max(1, r.alive), actual = Math.max(0, dx * nx + dy * ny) / dt;
        if (actual < .01) continue;
        r._formationPressureNX = nx; r._formationPressureNY = ny; r._formationPressureSpeed += actual;
        r._formationPressureUntil = e.time + dt * 1.5; r._pressureSourceId = strongest.id;
        this._pressureReceivers.push(r);
        if (e._translatePressureAnchor) e._translatePressureAnchor(r, dx, dy);
      }
    }

    beginStep(dt) {
      this.dt = dt;
      const e = this.engine;
      if (e.time >= this.cleanupAt) {
        for (const [key, until] of this.pairs) if (until <= e.time) this.pairs.delete(key);
        this.cleanupAt = e.time + 1;
      }
      for (const id of Array.from(this.active)) {
        const u = e.units[id];
        if (!u || u.routed || u._impactUntil <= e.time || u._impactSpeed < .8) {
          if (u) { u._impactSpeed = 0; u._impactUntil = 0; }
          this.active.delete(id); continue;
        }
        this._press(u, u._impactNX, u._impactNY, u._impactSpeed, 0, u._impactSource, new Set());
        // Unsupported light troops can be lifted clear of the ground and
        // keep more of the impact's horizontal momentum. A backed rank stays
        // grounded and sheds that impulse through contact with its supports.
        u._impactSpeed *= Math.exp(-(4.6 - this._yielding(u, e.units[u._impactSource]) * 3.60) * (u._impactLift > 0 ? .70 : 1) * dt);
      }
      this._stepPressure(dt);
    }

    beginUnit(u) {
      const e = this.engine, r = e.regiments[u.regiment];
      if (horse(u)) {
        this._friendlyState(u);
        const turning = u._marchTurnEnd > e.time || u._turnEnd > e.time || (r.order === 'face' && e.time < r._faceStart + .35);
        if ((r.order === 'hold' && !r._traffic) || turning) {
          u._mountSpeed = 0; u._mountDriveSpeed = 0; u._charge = 0; u._plungeUntil = 0;
          u._friendlyCarryTick = -1;
        } else if (u._mountMoveTick < e._tick - 1) {
          u._mountSpeed = Math.max(0, u._mountSpeed - T[u.type].speed * 3 * this.dt);
          u._mountDriveSpeed = Math.max(0, u._mountDriveSpeed - T[u.type].speed * 3 * this.dt);
          u._charge = this.power(u) * 200;
        }
      }
      if (u._impactUntil > e.time && u._impactSpeed > .8) {
        u.state = 'knocked'; u._vx = u._impactNX * u._impactSpeed; u._vy = u._impactNY * u._impactSpeed;
        u._cooldown -= this.dt; u._sense = 0; u._braceTime = 0;
        return true;
      }
      return false;
    }

    speedFor(u, wanted, nx, ny, dt) {
      if (!horse(u)) return wanted;
      const dot = nx * u._mountNX + ny * u._mountNY;
      if (u._mountSpeed > 2 && dot < .85) u._mountSpeed *= clamp((dot + .1) / .95, 0, 1);
      const guideDot = nx * u._mountDriveNX + ny * u._mountDriveNY;
      if (u._mountDriveSpeed > 2 && guideDot < .85) u._mountDriveSpeed *= clamp((guideDot + .1) / .95, 0, 1);
      const acceleration = T[u.type].speed * .62;
      // Collision sliding can remove forward gallop momentum without taking
      // away the ability to step sideways at a walk. Retain only a bounded
      // walking effort after obstruction; actual movement below still sets
      // charge energy. Feeding a zero projection back into every attempt
      // otherwise traps horses behind a neighbor at a perpetual first step.
      const flowing = u._friendlyCarryTick >= this.engine._tick - 1 && guideDot > .93;
      const effort = Math.max(u._mountSpeed, Math.min(T[u.type].speed * (flowing ? 1.15 : .4), u._mountDriveSpeed));
      u._mountDriveSpeed = Math.min(wanted, effort + acceleration * dt);
      u._mountDriveNX = nx; u._mountDriveNY = ny;
      u._mountMoveTick = this.engine._tick;
      return u._mountDriveSpeed;
    }

    afterMove(u, dx, dy, dt, movedDistance, friendlyOnly) {
      const distance = movedDistance === undefined ? Math.hypot(dx, dy) : movedDistance;
      this.recordMove(u, distance);
      if (!horse(u)) return;
      const actual = distance / dt;
      const forward = Math.max(0, (dx * u._mountDriveNX + dy * u._mountDriveNY) / dt);
      u._mountSpeed = Math.min(actual, forward);
      // Retain requested drive only while a real friendly clearance is
      // opening and the horse is still progressing. Hard stops, terrain,
      // enemies, and consumed impact momentum never receive this allowance.
      const flowing = friendlyOnly && u._friendlyFlowTick === this.engine._tick &&
        forward >= u._mountDriveSpeed * .3 && this.engine.time - u._lastImpact > dt * 1.5 &&
        u._braceImpactUntil <= this.engine.time;
      u._friendlyCarryTick = flowing ? this.engine._tick : -1;
      if (!flowing) u._mountDriveSpeed = Math.min(u._mountDriveSpeed, Math.max(T[u.type].speed * .4, u._mountSpeed));
      u._charge = this.power(u) * 200;
      if (actual > 2) { u._mountNX = dx / (actual * dt); u._mountNY = dy / (actual * dt); }
    }

    guideRegiment(r, dt, startX, startY, startAngle) {
      if (!horse(r) || r._traffic || r.fallingBack || r._rallyTime > 0 || r.order === 'hold' || r.order === 'face' ||
          r._deployCombat || r._deployMissiles || (r._tactic && r._tactic.hold)) return;
      const e = this.engine, dx = r.x - startX, dy = r.y - startY, distance = Math.hypot(dx, dy);
      const moveNX = distance ? dx / distance : 0, moveNY = distance ? dy / distance : 0;
      const marchNX = Math.cos(startAngle), marchNY = Math.sin(startAngle);
      let cx = 0, cy = 0, vx = 0, vy = 0, count = 0, rearCount = 0, rearLag = 0, rearSpeed = 0;
      for (const id of r.unitIds) {
        const u = e.units[id];
        if (!u.alive || u.routed) continue;
        cx += u.x; cy += u.y; vx += u._vx; vy += u._vy; count++;
        if (r._marchActive && distance > 0) {
          const lateral = u._marchOY * r._marchSqueeze;
          const slotX = startX + u._marchOX * marchNX - lateral * marchNY;
          const slotY = startY + u._marchOX * marchNY + lateral * marchNX;
          const lag = (slotX - u.x) * moveNX + (slotY - u.y) * moveNY;
          if (lag > 52) { rearCount++; rearLag += lag; rearSpeed += u._vx * moveNX + u._vy * moveNY; }
        }
      }
      if (count) { cx /= count; cy /= count; vx /= count; vy /= count; }
      if (distance > 0 && count) {
          const nx = dx / distance, ny = dy / distance;
          const lead = (startX - cx) * nx + (startY - cy) * ny;
          // A guide may lead by roughly one rank so the riders keep walking,
          // but cannot race away at full speed while their bodies accelerate
          // or negotiate a neighbor. This also keeps cornering ranks together.
          let allowed = Math.max(0, vx * nx + vy * ny + (32 - lead) * 2) * dt;
          // A healthy average can hide rear riders detouring around a rock.
          // Gather that lagging part of the ranks without stopping the whole
          // formation indefinitely for a separated straggler.
          if (rearCount >= Math.ceil(count * .125)) {
            const following = Math.max(0, rearSpeed / rearCount + (45 - rearLag / rearCount) * 2) * dt;
            allowed = Math.min(allowed, Math.max(distance * .15, following));
          }
          if (distance > allowed) { r.x = startX + nx * allowed; r.y = startY + ny * allowed; }
      }
      const turn = Math.atan2(Math.sin(r._marchAngle - startAngle), Math.cos(r._marchAngle - startAngle));
      if (r._marchActive && Math.abs(turn) < 1) {
        // Wait for real ranks to follow a wheel. Repeatedly rotating distant
        // slots ahead of slow horses sends the old rear through the old front.
        // The separate about-face operation still preserves its rank reversal.
        // The guide deliberately leads the center. That translation is not
        // rank deformation: counting it twice would continually brake a
        // coherent wheel and keep a whole wing circling outside the battle.
        const shapeLag = Math.max(0, r._marchLag - Math.hypot(r.x - cx, r.y - cy));
        const following = clamp(1 - Math.max(0, shapeLag - 22) / 70, .12, 1);
        r._marchAngle = startAngle + turn * following;
        r.facing = r._marchAngle; r.footprintFacing = r.facing;
        r._marchNX = Math.cos(r._marchAngle); r._marchNY = Math.sin(r._marchAngle);
      }
    }

    _offensive(u) {
      const r = this.engine.regiments[u.regiment];
      return shock(u) && u.alive && !u.routed && !r.fallingBack && r._rallyTime <= 0 &&
        (r.order === 'charge' || r.order === 'auto' || (r.order === 'attack' && !this.engine._advancingAttack(r)));
    }

    plungeGoal(u, enemy) {
      if (!this._offensive(u)) return null;
      const e = this.engine, r = e.regiments[u.regiment], t = T[u.type];
      if (r._tactic && (r._tactic.transit || r._tactic.hold) && r.order === 'auto') return null;
      if (enemy && enemy.alive && !enemy.routed) {
        const dx = enemy.x - u.x, dy = enemy.y - u.y, distance = Math.hypot(dx, dy);
        // A rider already aligned with an explicit charge keeps that axis
        // through small file corrections instead of peeling off its target.
        const committed = r.order === 'charge' && u._mountNX * r._chargeNX + u._mountNY * r._chargeNY > .75;
        const nx = committed ? r._chargeNX : u._mountNX, ny = committed ? r._chargeNY : u._mountNY;
        if (distance <= t.range + enemy._radius + 35 && dx * nx + dy * ny >= -enemy._radius && u._mountSpeed > t.speed * .42) {
          u._plungeUntil = e.time + (u.type === 'lancer' ? 2.50 : 1.75);
          u._plungeNX = nx; u._plungeNY = ny;
        }
      }
      // A falling front-rank target must not cancel the run into the ranks
      // behind it. Carry the existing heading briefly while real momentum
      // remains; collisions, braces, and fresh commands still stop that run.
      if (u._plungeUntil <= e.time) return null;
      return { x: u.x + u._plungeNX * 110, y: u.y + u._plungeNY * 110 };
    }

    intercept(rider, defender, strength) {
      if (!shock(rider) || strength <= 0) return;
      const e = this.engine;
      if (rider._braceImpactUntil > e.time) return;
      rider._braceImpactUntil = e.time + .8;
      const reduction = defender.type === 'pikeman' ? .94 : defender.type === 'halberdier' ? .48 : .24;
      rider._mountSpeed *= 1 - strength * reduction;
      rider._mountDriveSpeed = Math.min(rider._mountDriveSpeed, rider._mountSpeed);
      rider._charge = this.power(rider) * 200;
      if (defender.type === 'pikeman' && strength > .4) rider._plungeUntil = 0;
    }

    weaponPower(u) {
      if (!shock(u)) return null;
      return this._offensive(u) && u._mountSpeed > T[u.type].speed * .28 ? this.power(u) : 0;
    }

    _near(u, reach, team, visit) {
      const e = this.engine;
      const broadReach = reach + (e._bodyHashDrift || 0);
      const x0 = Math.max(0, Math.floor((u.x - broadReach) / CELL)), x1 = Math.min(e._bodyCols - 1, Math.floor((u.x + broadReach) / CELL));
      const y0 = Math.max(0, Math.floor((u.y - broadReach) / CELL)), y1 = Math.min(e._bodyRows - 1, Math.floor((u.y + broadReach) / CELL));
      const centerX = Math.floor(u.x / CELL), centerY = Math.floor(u.y / CELL);
      let visits = 0;
      const cell = (cx, cy) => {
        if (cx < x0 || cx > x1 || cy < y0 || cy > y1) return true;
        for (let id = e._bodyHeads[team][cy * e._bodyCols + cx]; id !== -1; id = e._bodyNext[id]) {
          if (++visits > 96) return false;
          const other = e.units[id];
          if (other.id !== u.id && other.alive && !other.routed && !other._siegeDeployed&&(!(u._rampartZ>0||other._rampartZ>0)||Math.abs(e.unitAltitude(u)-e.unitAltitude(other))<=18)) visit(other);
        }
        return true;
      };
      // Spend the bounded search on immediate neighbors first. A row-wise
      // scan of a dense battle could exhaust its budget on a distant edge
      // and miss the body directly in front of the horse.
      if (!cell(centerX, centerY)) return;
      const rings = Math.max(centerX - x0, x1 - centerX, centerY - y0, y1 - centerY);
      for (let ring = 1; ring <= rings; ring++) {
        for (let x = centerX - ring; x <= centerX + ring; x++) {
          if (!cell(x, centerY - ring) || !cell(x, centerY + ring)) return;
        }
        for (let y = centerY - ring + 1; y < centerY + ring; y++) {
          if (!cell(centerX - ring, y) || !cell(centerX + ring, y)) return;
        }
      }
    }

    _resistance(u, source) {
      const e = this.engine, t = T[u.type], r = e.regiments[u.regiment];
      if (t.equipment) return 36;
      const brace = source ? Math.max(e._braceStrength(u, source), e._steadyGuardStrength ? e._steadyGuardStrength(u, source) : 0) : 0;
      const formed = (r.formation === 'line' || r.formation === 'shieldwall') && !r.fallingBack ? clamp(r.cohesion / 100, 0, 1) : 0;
      return t.mass * (1 + formed * .18 + brace * (u.type === 'pikeman' ? 4.5 : 1.65) + (t.rankStiffness || 0) * formed);
    }

    _yielding(u, source) {
      const t = T[u.type];
      if (!source || !shock(source) || t.equipment || t.mounted || t.brace || t.armorClass !== 'light') return 0;
      // Weak foot troops lack the weight and prepared weapon front needed to
      // spend a heavy horse's full impulse. This affects push and drag, not
      // their health or armor; polearms and firm shield ranks retain resistance.
      const guard = this.engine._steadyGuardStrength ? this.engine._steadyGuardStrength(u, source) : 0;
      return clamp((1.35 - t.mass) / .60, 0, 1) * (source.type === 'lancer' ? 1 : .90) * (1 - guard * 2);
    }

    _backing(u, nx, ny) {
      let mass = 0, count = 0;
      this._near(u, u._radius + this.engine._maxRadius + 26, u.team, v => {
        const dx = v.x - u.x, dy = v.y - u.y, along = dx * nx + dy * ny;
        if (along > 0 && along < u._radius + v._radius + 23 && Math.abs(-dx * ny + dy * nx) < u._radius + v._radius + 3) {
          mass += T[v.type].mass; count++;
        }
      });
      return { mass: Math.min(10, mass), count };
    }

    beforeMove(u, nx, ny, amount) {
      if (!this._offensive(u) || u._mountSpeed < T[u.type].speed * .28 || this.engine.time < u._nextImpactAt) return;
      const e = this.engine, r = e.regiments[u.regiment];
      if (r._contactDistance > u._radius + e._maxRadius + amount + 90) return;
      let target = null, nearest = Infinity;
      this._near(u, u._radius + e._maxRadius + amount + 14, 1 - u.team, v => {
        const dx = v.x - u.x, dy = v.y - u.y, forward = dx * nx + dy * ny;
        if (forward <= 0) return;
        const side = Math.abs(-dx * ny + dy * nx), gap = (u._radius + v._radius) * .96;
        const step = clamp(forward, 0, amount), near = Math.hypot(dx - nx * step, dy - ny * step);
        if (side > gap || near > gap + .18) return;
        const key = u.id * e.units.length + v.id;
        if ((this.pairs.get(key) || 0) > e.time) return;
        if (forward < nearest) { nearest = forward; target = v; }
      });
      if (!target) return;
      const t = T[u.type], defense = T[target.type];
      const closing = Math.max(0, u._mountSpeed - Math.max(0, target._vx * nx + target._vy * ny));
      if (closing < t.speed * .25) return;
      // Faster horses carry their real closing energy into contact; raising
      // their top speed must not silently reduce the same physical impact.
      const power = clamp(closing / Math.min(t.speed, 100), 0, 1.2) ** 2;
      const brace = e._braceStrength(target, u), resistance = this._resistance(target, u), backing = this._backing(target, nx, ny);
      // A supporting body takes both the struck soldier's weight and the
      // effort of setting the next rank in motion. This dissipates a charge
      // even before tightly packed ranks exhaust their available room.
      const yielding = this._yielding(target, u) * clamp((power - .2) / .8, 0, 1);
      const combined = resistance + backing.mass * (1.25 - yielding * .70);
      const transfer = t.mass / (t.mass + combined);
      // The impulse can use almost the existing 1.7× force budget, but
      // actual displacement still passes through the same swept body solver.
      const pushSpeed = Math.min(defense.speed * 1.68 / (1 + backing.mass * (u.type === 'lancer' && yielding > 0 ? 0 : .28 * (1 - yielding))), closing * transfer * (.92 + yielding * .80) * (1 - brace * .35));
      // Heavy lances sustain the shove; ordinary shock cavalry shares the
      // same mass contact, with a shorter period of physical knockback.
      const duration = .56 + Math.min(.2, power * .15) + yielding * (u.type === 'lancer' ? 1.40 : .65);
      // A weak body struck off-center spills away from the horse's corridor.
      // Pushing every victim straight ahead preserves a moving wall that the
      // rider can never enter. The deflected path still uses normal swept
      // body/terrain collision and transfers a smaller force to its neighbors.
      const lateral = (target.x - u.x) * -ny + (target.y - u.y) * nx;
      const side = Math.abs(lateral) > .2 ? Math.sign(lateral) : ((u.id + target.id) & 1 ? 1 : -1);
      const spill = yielding * .65 * side, normal = Math.hypot(1, spill);
      const pushNX = (nx - ny * spill) / normal, pushNY = (ny + nx * spill) / normal;
      // Rear support prevents a launch; those bodies receive the diminished
      // pressure chain instead of becoming one airborne block.
      const flight = !defense.equipment && !defense.mounted && defense.mass < 1.3 && brace < .18 && power > .48 && backing.count === 0;
      const lift = flight ? Math.min(36, 12 + power * 24) : 0;
      this.pairs.set(u.id * e.units.length + target.id, e.time + 1.1);
      u._nextImpactAt = e.time + .055; u._lastImpact = e.time; u._lastImpactTarget = target.id;
      target._impactCount++; target._lastImpact = e.time; target._impactSource = u.id;
      target._impactStart = e.time; target._impactDuration = duration; target._impactUntil = e.time + duration;
      target._impactLift = lift; target._impactNX = pushNX; target._impactNY = pushNY;
      target._impactSpeed = Math.max(target._impactSpeed, pushSpeed);
      const hp = Math.max(0, target.hp);
      e._hit(u, target, (t.charge * .82 + 13) * power * (1 - brace * .5), false, true);
      const damage = hp - Math.max(0, target.hp);
      u._lastImpactDamage = target._lastImpactDamage = damage;
      this.stats.contacts++; this.stats.damage += damage; if (flight) this.stats.launched++;
      if (!defense.equipment && pushSpeed > .8) {
        this.active.add(target.id);
        this._press(target, pushNX, pushNY, pushSpeed, 0, u.id, new Set());
      } else target._impactUntil = 0;
      // Each occupied rank takes momentum. Backed, heavy, or braced ranks
      // consume more; motion can continue only into physically cleared space.
      const loss = clamp((.14 + combined / (t.mass + combined) * .55) * (1 - yielding * .56) + brace * .23, .10, .9);
      u._mountSpeed *= 1 - loss; u._charge = this.power(u) * 200;
      if (defense.equipment) { u._mountSpeed *= .15; u._plungeUntil = 0; }
      u._mountDriveSpeed = Math.min(u._mountDriveSpeed, u._mountSpeed);
      e._effect(target.x, target.y, 'cavalryImpact', 17 + power * 15, u.team, .4);
    }

    weaponImpulse(attacker, target, nx, ny, speed, duration, lift) {
      const defense = T[target.type], length = Math.hypot(nx, ny);
      if (defense.equipment || target.routed || length < .001) return;
      const resistance = Math.max(1, Math.sqrt(defense.mass));
      speed /= resistance;
      if (target._impactUntil > this.engine.time && target._impactSpeed > speed) return;
      target._impactCount++; target._impactSource = attacker.id;
      target._lastImpact = target._impactStart = this.engine.time;
      target._impactDuration = duration; target._impactUntil = this.engine.time + duration;
      target._impactNX = nx / length; target._impactNY = ny / length;
      target._impactLift = defense.siege ? 0 : lift / resistance;
      target._impactSpeed = speed; target._braceTime = 0;
      this.active.add(target.id);
      if (target._impactLift > 0) {
        this.stats.launched++;
        if (!target.alive) this.lastLanding = Math.max(this.lastLanding || 0, target._impactUntil + .34);
      }
    }

    _press(u, nx, ny, speed, depth, sourceId, visited) {
      const e = this.engine, t = T[u.type];
      if (visited.has(u.id) || depth > 4 || t.equipment || u.routed) return 0;
      visited.add(u.id); this._startMotion(u);
      const budget = Math.max(0, t.speed * this.dt * 1.7 - u._motionDistance);
      let amount = Math.min(speed * this.dt, budget);
      if (amount < .002) return 0;
      const source = e.units[sourceId];
      // Dense backing both absorbs the impulse and transmits a diminished
      // part to the next body; process the rear first to make actual room.
      if (depth < 4) {
        const next = [];
        this._near(u, u._radius + e._maxRadius + amount + 14, u.team, v => {
          const dx = v.x - u.x, dy = v.y - u.y, along = dx * nx + dy * ny;
          const side = Math.abs(-dx * ny + dy * nx), gap = (u._radius + v._radius) * .96;
          if (along > .01 && side < gap && Math.hypot(dx - nx * amount, dy - ny * amount) < gap + .25) next.push(v);
        });
        next.sort((a, b) => (a.x - b.x) * nx + (a.y - b.y) * ny || a.id - b.id);
        const shared = Math.min(3, next.length);
        for (let i = 0; i < shared; i++) {
          const v = next[i], massFactor = t.mass / Math.max(.1, this._resistance(v, source));
          const yielding = this._yielding(v, source);
          const passed = speed * (.70 + yielding * .18) * Math.min(1, massFactor) / Math.sqrt(shared);
          if (passed < 1 || (v._impactLift > 0 && v._impactUntil > e.time && v._impactSpeed > passed)) continue;
          v._impactNX = nx; v._impactNY = ny; v._impactSpeed = Math.max(v._impactSpeed, passed);
          v._impactSource = sourceId; v._impactStart = e.time; v._impactDuration = .38 + yielding * .28;
          v._impactUntil = Math.max(v._impactUntil, e.time + v._impactDuration); v._impactLift = 0;
          this.active.add(v.id); this.stats.deepestChain = Math.max(this.stats.deepestChain, depth + 1);
          this._press(v, nx, ny, passed, depth + 1, sourceId, visited);
        }
      }
      const oldX = u.x, oldY = u.y;
      const movement = e._collideMovement(u, nx * amount, ny * amount, amount);
      let mx = movement.x, my = movement.y, moved = Math.hypot(mx, my);
      if (moved > amount) { mx *= amount / moved; my *= amount / moved; }
      // Impacts cannot turn an obstruction into a sideways sliding launch.
      if (mx * nx + my * ny < 0) return 0;
      if (e._bodyBlocked(u.x + mx, u.y + my, u._radius) || !e._lineClear(u.x, u.y, u.x + mx, u.y + my, u._radius)) return 0;
      u.x += mx; u.y += my; moved = Math.hypot(u.x - oldX, u.y - oldY); this.recordMove(u, moved);
      if (moved > .001) {
        u._braceTime = 0; u._vx = mx / this.dt; u._vy = my / this.dt;
        if (u._holdPosition) { u._holdPosition.x += mx; u._holdPosition.y += my; }
        const r = e.regiments[u.regiment];
        if (r._tactic && r._tactic.hold) { u._tacticHoldX += mx; u._tacticHoldY += my; }
        if (u.alive) u.state = 'knocked';
      }
      return moved;
    }
  }
  global.BattleImpacts = BattleImpacts;
})(typeof window !== 'undefined' ? window : globalThis);
