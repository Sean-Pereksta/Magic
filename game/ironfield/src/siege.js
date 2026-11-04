/* IRONFIELD — physical siege engineering.
 * Structures change the same terrain used by every body and every route.
 * Engines must reach their work site; ramps appear only after deployment.
 */
(function (global) {
  'use strict';
  const D = global.GameData, TYPES = D.TYPES, clamp = D.clamp;
  const within = (x, y, r) => x >= r.x && x <= r.x + r.w && y >= r.y && y <= r.y + r.h;
  const gap = (u, s) => Math.hypot(Math.max(0, s.x - u.x, u.x - s.x - s.w), Math.max(0, s.y - u.y, u.y - s.y - s.h));
  const ready = r => r.alive > 0 && !r._routed && !r.fallingBack && r._rallyTime <= 0 && r.order === 'auto';
  const STRUCTURE_HEIGHT = Object.freeze({ stone: 36, palisade: 28, earthwork: 14, house: 32, watchtower: 64, barricade: 16, keep: 56 });

  class BattleSiege {
    constructor(engine) {
      this.engine = engine;
      this.ramps = [];
      this._components = null; this._componentRevision = -1;
      this._claims = new Map();
      engine.structures = engine._fortifications.map((region, i) => {
        const hp = clamp(Number(region.hp) || (region.type === 'gate' ? 1000 : 2200), 200, 10000);
        const structureKind = Object.prototype.hasOwnProperty.call(STRUCTURE_HEIGHT, region.structureKind) ? region.structureKind : 'stone';
        Object.assign(region, { id: 'fort-' + i, team: region.team === 0 ? 0 : 1,
          structureKind, structureHeight: region.castleTower && structureKind === 'stone' ? 68 : region.type === 'gate' && structureKind === 'stone' ? 29 : STRUCTURE_HEIGHT[structureKind],
          name: typeof region.name === 'string' ? region.name.replace(/[<>]/g, '').trim().slice(0, 80) : '',
          hp, maxHp: hp, breached: false, open: false, lastHit: -1000, passages: [] });
        return region;
      });
      this.structures = engine.structures;
      this.byId = new Map(this.structures.map(s => [s.id, s]));
      for (const r of engine.regiments) this._initialize(r);
      for(const s of this.structures)if(s.walkable)s.structureHeight=(Number(s.deckHeight)||36)+8;
    }
    manRampart(ids,structureId,options){return this.engine.ramparts?this.engine.ramparts.manRampart(ids,structureId,options):0;}
    setRampartFormation(ids,options){return this.engine.ramparts?this.engine.ramparts.setRampartFormation(ids,options):0;}
    leaveRamparts(ids){return this.engine.ramparts?this.engine.ramparts.leaveRamparts(ids,true):0;}
    previewRampart(ids,structureId,options){return this.engine.ramparts?this.engine.ramparts.preview(ids,structureId,options):[];}

    _initialize(r) {
      r._siegeJob = null; r.siegeStatus = ''; r._siegeManualTarget = null;
      r._siegeManualEpoch = -1; r._siegeReachRevision = -1;
      for (const id of r.unitIds) {
        const u = this.engine.units[id];
        u._siegeStage = ''; u._siegeTarget = null; u._siegeProgress = 0;
        u._siegeDeployed = false; u._siegeRamp = null; u._siegeEnemy = -1;
        u._siegeUndocking = false; u._siegeUndockCheck = 0; u._siegeOccupants = null;
      }
    }

    structureAt(x, y) {
      for (let i = this.structures.length - 1; i >= 0; i--) {
        const s = this.structures[i];
        if (!s.breached && within(x, y, s)) return s;
      }
      return null;
    }

    setGateOpen(id, open, team) {
      const e = this.engine, gate = this.byId.get(id);
      if (!gate || gate.type !== 'gate' || gate.breached || gate.team !== team ||
          typeof open !== 'boolean' || gate.open === open || e.phase === 'ended') return false;
      if (!open && (gate.passages.length || e.units.some(u => {
        if (!u.alive || u.escaped) return false;
        const radius = u._radius + 1;
        return u.x >= gate.x - radius && u.x <= gate.x + gate.w + radius &&
          u.y >= gate.y - radius && u.y <= gate.y + gate.h + radius;
      }))) return false;
      gate.open = open;
      if (open) for (const r of e.regiments) {
        if (r._siegeManualTarget === id) r._siegeManualTarget = null;
        if (r._siegeJob && r._siegeJob.structureId === id && r._siegeJob.stage !== 'deployed') this._release(r);
      }
      e._refreshTerrainNavigation();
      e._event(open ? 'The garrison has opened the gate.' : 'The garrison has closed the gate.', team, 'siege');
      return true;
    }

    command(ids, structureId) {
      const e = this.engine, s = this.byId.get(structureId);
      if (!s || s.breached || s.open || e.phase === 'ended') return 0;
      const accepted = [];
      for (const id of ids || []) {
        const r = e.regiments[id], t = r && TYPES[r.type];
        if (!r || r.team === s.team || r.alive <= 0 || r._routed || r.fallingBack || r._rallyTime > 0 || t.commander) continue;
        if (t.ranged && !t.siege) continue;
        accepted.push(r);
      }
      if (!accepted.length) return 0;
      e.command(accepted.map(r => r.id), 'auto');
      for (const r of accepted) {
        r._siegeManualTarget = s.id; r._siegeManualEpoch = r._commandEpoch;
        this._release(r);
      }
      e._strategyTimer = 0; e._strategize();
      e._event(accepted.length + ' formation' + (accepted.length === 1 ? '' : 's') + ' assigned to ' + (s.type === 'gate' ? 'breach the gate.' : 'assault the wall.'), accepted[0].team, 'order');
      return accepted.length;
    }

    _release(r) {
      r._siegeJob = null; r.siegeStatus = '';
      for (const id of r.unitIds) {
        const u = this.engine.units[id];
        u._siegeStage = ''; u._siegeTarget = null; u._siegeProgress = 0;
        // Accept the new intent now, but keep a docked machine stationary
        // until soldiers already under its deck have physically cleared it.
        // The departing footprint admits no new walkers during this phase.
        if (u._siegeDeployed) {
          if (!u._siegeUndocking) u._siegeOccupants = this._captureDeckOccupants(u);
          u._siegeUndocking = true; u._siegeUndockCheck = 0;
        }
        u._siegeEnemy = -1;
      }
      r._path.length = 0; r._pathTimer = 0;
    }

    _site(r, s, offset) {
      const t = TYPES[r.type], e = this.engine, u = e.units[r.unitIds.find(id => e.units[id].alive)] || r;
      const axis = s.w <= s.h ? 'x' : 'y';
      const dir = axis === 'x' ? (u.x < s.x + s.w / 2 ? 1 : -1) : (u.y < s.y + s.h / 2 ? 1 : -1);
      const lateral = axis === 'x' ? u.y : u.x, low = axis === 'x' ? s.y : s.x, length = axis === 'x' ? s.h : s.w;
      const half = r.type === 'siegeTower' ? 74 : Math.min(45, length * .2);
      let center = clamp(lateral + (offset || 0), low + Math.min(half, length * .5), low + length - Math.min(half, length * .5));
      if (r.type === 'siegeTower' && length >= 160) center = clamp(Math.round((center - 40) / 80) * 80 + 40, low + 74, low + length - 74);
      const artillery = t.siege && t.ranged && !t.equipment;
      const distance = artillery ? Math.max(120, t.range * .76) : t.ranged && !t.equipment ? t.range * .72 : t.radius + 9;
      const face = axis === 'x' ? (dir > 0 ? s.x : s.x + s.w) : (dir > 0 ? s.y : s.y + s.h);
      const x = axis === 'x' ? face - dir * distance : center;
      const y = axis === 'y' ? face - dir * distance : center;
      if (e._bodyBlocked(x, y, t.radius + 2)) return null;
      return { x, y, axis, dir, center, face, distance, structureId: s.id,
        stage: 'approach', progress: 0, epoch: r._commandEpoch, started: e.time,
        artillery, rangedStage: t.ranged && !t.equipment && !artillery };
    }

    _pick(r, manual) {
      const e = this.engine, t = TYPES[r.type];
      const candidates = this.structures.filter(s => !s.breached && !s.open && s.team !== r.team && (!manual || s.id === manual));
      candidates.sort((a, b) => {
        const score = s => gap(r, s) + (r.type === 'siegeTower' ? s.type === 'gate' ? 600 : 0 : s.type === 'wall' ? 1400 : 0)
          + (this._claims.get(s.id) || 0) * (r.type === 'siegeTower' ? 1000 : 180)
          + (s.passages.length ? 8000 : 0);
        return score(a) - score(b) || a.id.localeCompare(b.id);
      });
      for (const s of candidates.slice(0, manual ? 1 : 12)) {
        if (r.type === 'siegeTower' && s.passages.length && !manual) continue;
        const claimed = this._claims.get(s.id) || 0;
        for (const offset of [claimed ? (claimed % 2 ? 70 : -70) : 0, 0]) {
          const job = this._site(r, s, offset);
          if (!job) continue;
          const path = e._findPath(r._centroidX, r._centroidY, job.x, job.y, t.radius + 3);
          if (!path.length) continue;
          this._claims.set(s.id, claimed + 1);
          return job;
        }
      }
      return null;
    }

    prepare() {
      if (!this.structures.length) return;
      this._claims.clear();
      for (const r of this.engine.regiments) if (r._siegeJob && ready(r)) this._claims.set(r._siegeJob.structureId, (this._claims.get(r._siegeJob.structureId) || 0) + 1);
      for (const r of this.engine.regiments) {
        if (r._siegeJob === undefined) this._initialize(r);
        if (!ready(r)) { if (r._siegeJob) this._release(r); continue; }
        if (r._siegeManualEpoch !== r._commandEpoch) r._siegeManualTarget = null;
        let job = r._siegeJob;
        if (job && (job.epoch !== r._commandEpoch || !this.byId.get(job.structureId) || (this.byId.get(job.structureId).breached && job.stage !== 'deployed'))) {
          this._release(r); job = null;
        }
        if (job && !TYPES[r.type].siege && !r._siegeManualTarget) {
          const enemy = this.engine.regiments[r._enemyId];
          if (enemy && this._componentAt(r._centroidX, r._centroidY) === this._componentAt(enemy._centroidX, enemy._centroidY)) {
            // Another assault party may have opened a gate or ramp. Reserves
            // use that new route instead of remaining on their old wall job.
            this._release(r); job = null;
          }
        }
        if (job) continue;
        const special = TYPES[r.type].siege;
        if (!special && !r._siegeManualTarget) continue;
        r._siegeJob = this._pick(r, r._siegeManualTarget);
      }
    }

    _componentAt(x, y) {
      const e = this.engine;
      if (this._componentRevision !== e.terrainRevision) {
        const grid = e._navBlocked, cols = e._navCols, rows = e._navRows;
        const labels = new Int32Array(grid.length), queue = new Int32Array(grid.length);
        let component = 0;
        for (let i = 0; i < grid.length; i++) {
          if (grid[i] || labels[i]) continue;
          let head = 0, tail = 1; queue[0] = i; labels[i] = ++component;
          while (head < tail) {
            const cell = queue[head++], xx = cell % cols, yy = Math.floor(cell / cols);
            for (const next of [xx > 0 ? cell - 1 : -1, xx + 1 < cols ? cell + 1 : -1, yy > 0 ? cell - cols : -1, yy + 1 < rows ? cell + cols : -1]) {
              if (next < 0 || grid[next] || labels[next]) continue;
              labels[next] = component; queue[tail++] = next;
            }
          }
        }
        this._components = labels; this._componentRevision = e.terrainRevision;
      }
      return this._components[clamp(Math.floor(y / 80), 0, e._navRows - 1) * e._navCols + clamp(Math.floor(x / 80), 0, e._navCols - 1)];
    }

    goal(r, gx, gy) {
      if (!this.structures.length || !ready(r)) return null;
      const e = this.engine, t = TYPES[r.type];
      let job = r._siegeJob;
      if (!job && !t.commander && !t.equipment && this._componentAt(r._centroidX, r._centroidY) !== this._componentAt(gx, gy)) {
        // A sealed fort has no enemy-side route. March to a real accessible
        // gate instead of repeatedly planning toward an unreachable target.
        job = this._pick(r, null);
        if (job) r._siegeJob = job;
      }
      if (!job) return null;
      r._combatActive = false; r._missileActive = false; r._fireLocked = false;
      r.siegeStatus = job.stage === 'deployed' ? 'Assault ramp open' : job.artillery ? 'Bombarding fortification' : job.rangedStage ? 'Covering the assault' : r.type === 'siegeTower' ? job.stage === 'deploying' ? 'Deploying assault ramp' : 'Approaching wall' : job.stage === 'battering' ? 'Battering ' + this.byId.get(job.structureId).type : 'Approaching ' + this.byId.get(job.structureId).type;
      return { x: job.x, y: job.y };
    }

    _tickUnit(u, dt) {
      const e = this.engine;
      u.anim += dt; u.attackFlash = Math.max(0, u.attackFlash - dt * 3.7);
      u._cooldown -= dt; u._sense -= dt; u._terrainTimer -= dt; u._contactSense -= dt;
      if (u._terrainTimer <= 0) { u._terrain = e.terrainAt(u.x, u.y); u._terrainTimer = .4 + u._jitter * .25; }
    }

    _deckClear(u) {
      const e = this.engine, padding = u._radius + e._maxRadius + 48;
      const minX = Math.max(0, Math.floor((u.x - padding) / 32)), maxX = Math.min(e._bodyCols - 1, Math.floor((u.x + padding) / 32));
      const minY = Math.max(0, Math.floor((u.y - padding) / 32)), maxY = Math.min(e._bodyRows - 1, Math.floor((u.y + padding) / 32));
      for (let team = 0; team < 2; team++) for (let y = minY; y <= maxY; y++) for (let x = minX; x <= maxX; x++) {
        for (let id = e._bodyHeads[team][y * e._bodyCols + x]; id !== -1; id = e._bodyNext[id]) {
          const other = e.units[id];
          if (id === u.id || !other.alive || other.routed || other._siegeDeployed) continue;
          // A body resting on the exterior contact boundary is already
          // separated. It must not keep an otherwise empty tower deployed.
          const separation = (u._radius + other._radius) * .96 - .02;
          if ((u.x - other.x) ** 2 + (u.y - other.y) ** 2 < separation * separation) return false;
        }
      }
      return true;
    }

    _captureDeckOccupants(u) {
      const e = this.engine, ids = new Set(), padding = u._radius + e._maxRadius + 48;
      const minX = Math.max(0, Math.floor((u.x - padding) / 32)), maxX = Math.min(e._bodyCols - 1, Math.floor((u.x + padding) / 32));
      const minY = Math.max(0, Math.floor((u.y - padding) / 32)), maxY = Math.min(e._bodyRows - 1, Math.floor((u.y + padding) / 32));
      for (let team = 0; team < 2; team++) for (let y = minY; y <= maxY; y++) for (let x = minX; x <= maxX; x++) {
        for (let id = e._bodyHeads[team][y * e._bodyCols + x]; id !== -1; id = e._bodyNext[id]) {
          const other = e.units[id];
          if (id === u.id || !other.alive || other.routed || other._siegeDeployed) continue;
          const separation = (u._radius + other._radius) * .96 + .6;
          if ((u.x - other.x) ** 2 + (u.y - other.y) ** 2 < separation * separation) ids.add(id);
        }
      }
      return ids;
    }

    stepUnit(u, dt) {
      if(this.engine.ramparts&&this.engine.ramparts.stepUnit(u,dt))return true;
      const e = this.engine, r = e.regiments[u.regiment], t = TYPES[u.type], job = r._siegeJob;
      if (u._siegeUndocking) {
        let clear = false;
        if (e.time >= u._siegeUndockCheck) {
          clear = this._deckClear(u); u._siegeUndockCheck = e.time + .12;
        }
        if (clear) { u._siegeUndocking = false; u._siegeDeployed = false; u._siegeOccupants = null; u._siegeStage = ''; r.siegeStatus = ''; }
        else {
          this._tickUnit(u, dt); u._vx = 0; u._vy = 0;
          u.state = 'undocking'; u._siegeStage = 'undocking'; u._siegeProgress = 0;
          r.siegeStatus = 'Waiting for troops to clear the ramp';
          return true;
        }
      }
      if (!job || !ready(r) || u.routed || job.epoch !== r._commandEpoch) return false;
      const s = this.byId.get(job.structureId);
      if (!s || (s.breached && job.stage !== 'deployed')) return false;
      // Missile covering formations keep the normal target/volley code.
      if (job.rangedStage) return false;
      if (e.impacts && e.impacts.beginUnit(u)) return true;
      this._tickUnit(u, dt);
      u._siegeTarget = s.id; u._siegeStage = job.stage; u._siegeProgress = job.progress;
      e._setTarget(u, -1);
      let x = job.x, y = job.y;
      if (!t.equipment && !job.artillery) {
        // Gate attackers occupy separate files along its actual face. Only
        // front bodies can hit; rear bodies remain constrained by collision.
        const lateral = job.axis === 'x' ? u.y : u.x, low = job.axis === 'x' ? s.y : s.x, size = job.axis === 'x' ? s.h : s.w;
        const slot = clamp(lateral, low + t.radius + 8, low + size - t.radius - 8);
        if (job.axis === 'x') y = slot; else x = slot;
      }
      const distance = Math.hypot(x - u.x, y - u.y);
      const atSite = distance <= (job.artillery ? 24 : 6);
      if (!atSite && job.stage !== 'deployed') {
        e._moveUnit(u, x, y, dt, 1);
        job.stage = 'approach'; job.progress = 0;
        return true;
      }
      u._vx = 0; u._vy = 0; u.angle = job.axis === 'x' ? job.dir > 0 ? 0 : Math.PI : job.dir > 0 ? Math.PI / 2 : -Math.PI / 2;
      if (u.type === 'siegeTower') {
        if (job.stage !== 'deployed') {
          job.stage = 'deploying'; job.progress = Math.min(1, job.progress + dt / 4);
          u.state = 'deploying'; u._siegeStage = job.stage; u._siegeProgress = job.progress;
          r.siegeStatus = 'Deploying assault ramp';
          if (job.progress >= 1) this._deploy(u, r, s, job);
        } else {
          u.state = 'idle';
          if (u._sense <= 0) { u._siegeEnemy = e._chooseTarget(u, r, t); u._sense = .45; }
          const enemy = e.units[u._siegeEnemy];
          if (enemy && enemy.alive && !enemy.routed && u._cooldown <= 0) e._attack(u, enemy);
        }
        return true;
      }
      job.stage = job.artillery ? 'bombarding' : 'battering';
      u._siegeStage = job.stage; u.state = 'idle';
      const reach = job.artillery ? t.range : t.radius + t.range;
      if (u._cooldown <= 0 && gap(u, s) <= reach) {
        u._cooldown = t.cooldown; u._lastAttack = e.time; u.attackFlash = 1; u.state = 'attack';
        if (job.artillery) this._shoot(u, s);
        else {
          const amount = u.type === 'batteringRam' ? s.type === 'gate' ? 95 : 52 : Math.max(1, t.damage * (s.type === 'gate' ? .16 : .035));
          this.damage(s, amount, u);
        }
      }
      return true;
    }

    _deploy(u, r, s, job) {
      const axis = job.axis, half = 72, lead = 160;
      const ramp = { id: 'ramp-' + this.ramps.length, structureId: s.id, towerId: u.id, team: u.team,
        x: axis === 'x' ? s.x - lead : job.center - half,
        y: axis === 'y' ? s.y - lead : job.center - half,
        w: axis === 'x' ? s.w + lead * 2 : half * 2,
        h: axis === 'y' ? s.h + lead * 2 : half * 2,
        axis, dir: job.dir, height: 38 };
      this.ramps.push(ramp); s.passages.push(ramp);
      u._siegeRamp = ramp.id; u._siegeDeployed = true;
      job.stage = 'deployed'; job.progress = 1;
      u._siegeStage = 'deployed'; u._siegeProgress = 1; r.siegeStatus = 'Assault ramp open';
      this.engine._refreshTerrainNavigation();
      this.engine._event('A siege tower has docked. Its assault ramp is open.', u.team, 'siege');
    }

    heightAt(x, y) {
      let height = 0;
      for (const ramp of this.ramps) {
        if (!within(x, y, ramp)) continue;
        const along = ramp.axis === 'x' ? x - ramp.x : y - ramp.y;
        const length = ramp.axis === 'x' ? ramp.w : ramp.h;
        height = Math.max(height, ramp.height * clamp(Math.min(along / 160, (length - along) / 160), 0, 1));
      }
      return height;
    }

    _shoot(u, s) {
      const e = this.engine, t = TYPES[u.type];
      const tx = clamp(u.x, s.x + 8, s.x + s.w - 8), ty = clamp(u.y, s.y + 8, s.y + s.h - 8);
      const distance = Math.hypot(tx - u.x, ty - u.y);
      const p = { x: u.x, y: u.y, z: 0, sx: u.x, sy: u.y, tx, ty,
        team: u.team, kind: t.projectile || 'stone', target: -1, attacker: u.id,
        type: u.type, regiment: u.regiment, structureTarget: s.id, age: 0,
        duration: Math.max(.2, distance / (t.projectileSpeed || 370)),
        damage: t.damage * (u.type === 'catapult' ? 2.2 : u.type === 'ballista' ? .72 : 1), arc: t.projectileArc || Math.min(180, distance * .23) };
      e._launchProjectile(p);
    }

    impact(p) {
      const s = this.byId.get(p.structureTarget);
      if (!s || s.breached) return;
      this.damage(s, p.damage, this.engine.units[p.attacker]);
      this.engine._effect(p.tx, p.ty, 'explosion', p.type === 'catapult' ? 44 : 18, p.team, .7);
    }

    damage(structure, amount, attacker) {
      const s = typeof structure === 'string' ? this.byId.get(structure) : structure;
      if (!s || s.breached || s.open || !Number.isFinite(amount) || amount <= 0 || (attacker && attacker.team === s.team)) return 0;
      const previous = s.hp; s.hp = Math.max(0, s.hp - amount); s.lastHit = this.engine.time;
      const x = attacker ? clamp(attacker.x, s.x, s.x + s.w) : s.x + s.w / 2;
      const y = attacker ? clamp(attacker.y, s.y, s.y + s.h) : s.y + s.h / 2;
      this.engine._effect(x, y, 'siegeHit', 24, attacker ? attacker.team : 1 - s.team, .55);
      if (!s.hp) {
        s.breached = true;
        if(this.engine.ramparts)this.engine.ramparts.collapse(s);
        this.engine._refreshTerrainNavigation();
        this.engine._event((s.type === 'gate' ? 'The gate' : 'A section of wall') + ' has been breached. Troops can advance through the rubble.', attacker ? attacker.team : 1 - s.team, 'siege');
      }
      return previous - s.hp;
    }
  }

  global.BattleSiege = BattleSiege;
})(typeof window !== 'undefined' ? window : globalThis);
