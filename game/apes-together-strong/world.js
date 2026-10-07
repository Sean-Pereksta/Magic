(function () {
  'use strict';

  const TAU = Math.PI * 2;
  function hash(value) {
    const text = String(value);
    let h = 2166136261;
    for (let i = 0; i < text.length; i++) {
      h ^= text.charCodeAt(i);
      h = Math.imul(h, 16777619);
    }
    h ^= h >>> 16;
    h = Math.imul(h, 2246822507);
    h ^= h >>> 13;
    return h >>> 0;
  }
  function rng(seed) {
    let n = typeof seed === 'number' ? seed >>> 0 : hash(seed);
    return function () {
      n += 0x6D2B79F5;
      let t = n;
      t = Math.imul(t ^ t >>> 15, t | 1);
      t ^= t + Math.imul(t ^ t >>> 7, t | 61);
      return ((t ^ t >>> 14) >>> 0) / 4294967296;
    };
  }
  const clamp = (n, a, b) => Math.max(a, Math.min(b, n));
  const lerp = (a, b, t) => a + (b - a) * t;
  const dist2 = (x1, y1, x2, y2) => {
    if (typeof x1 === 'object') {
      const a = x1, b = y1;
      return (a.x - b.x) ** 2 + (a.y - b.y) ** 2;
    }
    return (x1 - x2) ** 2 + (y1 - y2) ** 2;
  };
  const dist = (x1, y1, x2, y2) => Math.sqrt(dist2(x1, y1, x2, y2));
  const random = (randomSource, a, b) => a + randomSource() * (b - a);
  const choice = (randomSource, list) => list[Math.floor(randomSource() * list.length)];
  const int = (randomSource, a, b) => Math.floor(random(randomSource, a, b + 1));
  function angleDifference(a, b) { return Math.atan2(Math.sin(a - b), Math.cos(a - b)); }
  window.ATSUtil = { hash, rng, clamp, lerp, dist, dist2, random, choice, int, TAU, angleDifference };

  const CHUNK = 768;
  const DISTRICT = CHUNK * 2;
  const DISCOVERY = 192;
  const HP = { tree: 155, rock: 360, berry: 1, cage: 70, gate: 190, wall: 145,
    tower: 170, alarm: 80, radio: 150, barracks: 300, depot: 270, fuel: 90,
    house: 240, vehicle: 205 };
  const ADJECTIVES = ['Blackpine', 'Moonfall', 'Ashwood', 'Ironwood', 'Coldwater', 'Raven',
    'Redfern', 'Briar', 'Hollow', 'Northstar', 'Greywolf', 'Stonebrook', 'Ember', 'Stillwater'];
  const SITE_LABELS = { transport: 'Transport Cage', hunter: 'Hunter Camp', research: 'Research House',
    checkpoint: 'Road Checkpoint', prison: 'Prison Compound', detention: 'Detention Center',
    experimental: 'Experimental Facility', forwardBase: 'Forward Operating Base',
    armoredDepot: 'Armored Depot', regionalCommand: 'Regional Command Base' };
  const MILITARY_TYPES = new Set(['forwardBase', 'armoredDepot', 'regionalCommand']);
  const VEHICLE_CLEARANCE = { jeep: 21, armored: 23, command: 22, truck: 24, apc: 25, ifv: 27, tank: 31 };

  class ATSWorld {
    constructor(seed) {
      this.seed = String(seed === undefined ? Date.now() : seed);
      this.seedHash = hash(this.seed);
      this.navRevision = 0;
      this.chunkSize = CHUNK;
      this.revealCellSize = DISCOVERY;
      this.chunks = new Map();
      this.sites = new Map();
      this.objects = new Map();
      this.discovered = new Set();
      this.intel = new Map();
      this._terrainCache = new Map();
      this._sitePlanCache = new Map();
      this._vehiclePlanCache = new Map();
      this._spatial = new Map();
      this._collisionCell = 128;
      this._queryStamp = 0;
      this._queryDepth = 0;
      this._chunkJobs = new Map();
      this._streamQueue = new Map();
      this._corridorRequests = new Map();
      this._streamSerial = 0;
      this._coldChunks = new Map();
      this._streaming = false;
      this.chunkRevision = 0;
      this.stats = { collisionQueries: 0, collisionCandidates: 0, objectQueries: 0,
        chunksGenerated: 0, streamSteps: 0, streamMs: 0, pendingChunks: 0, coldChunks: 0 };
      this._phase = (this.seedHash % 10000) / 10000 * TAU;
    }

    _hashAt(x, y, salt) {
      let h = this.seedHash ^ Math.imul(x | 0, 374761393) ^ Math.imul(y | 0, 668265263) ^ (salt || 0);
      h = Math.imul(h ^ h >>> 13, 1274126177);
      return ((h ^ h >>> 16) >>> 0) / 4294967296;
    }

    _noise(x, y, scale, salt) {
      x /= scale; y /= scale;
      const ix = Math.floor(x), iy = Math.floor(y);
      let tx = x - ix, ty = y - iy;
      tx = tx * tx * (3 - 2 * tx); ty = ty * ty * (3 - 2 * ty);
      return lerp(lerp(this._hashAt(ix, iy, salt), this._hashAt(ix + 1, iy, salt), tx),
        lerp(this._hashAt(ix, iy + 1, salt), this._hashAt(ix + 1, iy + 1, salt), tx), ty);
    }

    _roadInfo(x, y) {
      const ix = Math.round((x - 560) / DISTRICT), iy = Math.round((y - 120) / DISTRICT);
      const phaseX = this._hashAt(ix, 0, 9123) * TAU;
      const phaseY = this._hashAt(0, iy, 7311) * TAU;
      const rx = ix * DISTRICT + 560 + (Math.sin(y / 690 + phaseX) - Math.sin(120 / 690 + phaseX)) * 35;
      const ry = iy * DISTRICT + 120 + (Math.sin(x / 710 + phaseY) - Math.sin(560 / 710 + phaseY)) * 35;
      const dx = Math.abs(x - rx), dy = Math.abs(y - ry);
      return { dx, dy, x: rx, y: ry, road: dx < 23 || dy < 23 };
    }

    _riverInfo(x, y) {
      const row = Math.round((y - 1320) / 2304);
      const centerY = row * 2304 + 1320 + Math.sin(x / 560 + this._phase + row * 1.47) * 110
        + Math.sin(x / 1370 + this._phase) * 65;
      return { distance: Math.abs(y - centerY), width: 37 + Math.sin(x / 370 + row) * 8, centerY };
    }

    districtAt(x, y) {
      const dx=Math.floor(x/2304),dy=Math.floor(y/2304),n=this._hashAt(dx,dy,1947),distance=Math.hypot(x,y);
      const role=distance<1800?'frontier':n<.26?'watershed':n<.50?'farmland':n<.72?'highlands':n<.88?'research':'military';
      const names={frontier:'Blackpine Frontier',watershed:'The Drowned Woods',farmland:'Old Orchard Country',highlands:'Slateback Highlands',research:'Greywood Research Belt',military:'Iron Road District'};
      return {id:dx+','+dy,role,name:names[role],fertility:role==='farmland'?1.5:role==='watershed'?1.15:role==='highlands'?.65:1};
    }

    terrain(x, y) {
      const bx = Math.floor(x / 64), by = Math.floor(y / 64), key = bx + ',' + by;
      let biome = this._terrainCache.get(key);
      if (!biome) {
        const px = bx * 64 + 32, py = by * 64 + 32;
        const moisture = this._noise(px, py, 1150, 874);
        const development = this._noise(px, py, 1650, 8161);
        const elevation = this._noise(px, py, 720, 5553);
        biome = development > 0.73 ? 'ruins' : development > 0.60 && moisture < 0.59 ? 'farmland'
          : elevation > 0.76 ? 'rocky' : moisture > 0.70 ? 'wetland' : 'forest';
        const district=this.districtAt(px,py);
        if(district.role==='watershed'&&moisture>.44)biome='wetland';
        if(district.role==='farmland'&&elevation<.7)biome='farmland';
        if(district.role==='highlands'&&elevation>.43)biome='rocky';
        if(district.role==='military'&&development>.48)biome='ruins';
        if (px * px + py * py < 800 * 800) biome = 'forest';
        this._terrainCache.set(key, biome);
        if (this._terrainCache.size > 26000) this._terrainCache.clear();
      }
      const roadInfo = this._roadInfo(x, y);
      const river = this._riverInfo(x, y);
      const riverDistance = river.distance, riverWidth = river.width;
      const bridge = riverDistance < riverWidth + 12 && roadInfo.dx < 36;
      // Bridges occupy the road corridor; the same deterministic test governs rendering and collision.
      const water = riverDistance < riverWidth && !bridge;
      return { biome, road: roadInfo.road || bridge, water, bridge, walkable: !water,
        river: riverDistance < riverWidth + 48, moisture: biome === 'wetland' ? 0.8 : 0.3 };
    }

    ensure(x, y, radius) {
      radius = radius === undefined ? 1400 : Math.max(0, radius);
      const minX = Math.floor((x - radius) / CHUNK), maxX = Math.floor((x + radius) / CHUNK);
      const minY = Math.floor((y - radius) / CHUNK), maxY = Math.floor((y + radius) / CHUNK);
      for (let cy = minY; cy <= maxY; cy++) for (let cx = minX; cx <= maxX; cx++) this._generateChunk(cx, cy);
    }

    // Generation is staged ahead of travel. Collision never asks generation to
    // finish synchronously once streaming is active.
    stream(x, y, radius = 1400, options = {}) {
      this._streaming = true;
      this._streamSerial++;
      const now = () => typeof performance !== 'undefined' ? performance.now() : Date.now();
      const started = now(), budget = options.budgetMs ?? 1.5, maxSteps = options.maxSteps ?? 3;
      const dx = options.dx || 0, dy = options.dy || 0;
      const minX = Math.floor((x - radius) / CHUNK), maxX = Math.floor((x + radius) / CHUNK);
      const minY = Math.floor((y - radius) / CHUNK), maxY = Math.floor((y + radius) / CHUNK);
      const corridors = new Map();
      for (const [id, request] of this._corridorRequests) {
        if (this._streamSerial - request.touched > 180) { this._corridorRequests.delete(id); continue; }
        for (const [key, point] of request.chunks) {
          const score = point.distance + 1600000;
          if (!corridors.has(key) || score < corridors.get(key).score) corridors.set(key, { cx: point.cx, cy: point.cy, score });
        }
      }
      for (const [key, request] of this._streamQueue) {
        if (!corridors.has(key) && (request.cx < minX - 1 || request.cx > maxX + 1 || request.cy < minY - 1 || request.cy > maxY + 1)) this._streamQueue.delete(key);
      }
      for (let cy = minY; cy <= maxY; cy++) for (let cx = minX; cx <= maxX; cx++) {
        const key = cx + ',' + cy;
        if (this.chunks.has(key)) { this._streamQueue.delete(key); continue; }
        const ox = (cx + .5) * CHUNK - x, oy = (cy + .5) * CHUNK - y;
        this._streamQueue.set(key, { cx, cy, score: ox * ox + oy * oy - (ox * dx + oy * dy) * 350 });
      }
      for (const [key, request] of corridors) {
        if (this.chunks.has(key)) continue;
        const old = this._streamQueue.get(key);
        if (!old || request.score < old.score) this._streamQueue.set(key, request);
      }
      const queue = Array.from(this._streamQueue.entries()).sort((a, b) => a[1].score - b[1].score);
      let steps = 0;
      for (const [key, request] of queue) {
        while (steps < maxSteps && (steps === 0 || now() - started < budget)) {
          const job = this._chunkJobs.get(key) || this._startChunk(request.cx, request.cy);
          this._advanceChunk(job, 16); steps++;
          if (job.chunk.generated) { this._streamQueue.delete(key); break; }
        }
        if (steps >= maxSteps || now() - started >= budget) break;
      }
      // Abandoned incomplete jobs contain only deterministic private data; they
      // can be safely restarted when the player returns.
      for (const key of this._chunkJobs.keys()) if (!this._streamQueue.has(key)) this._chunkJobs.delete(key);
      this.stats.streamSteps = steps;
      this.stats.streamMs = now() - started;
      this.stats.pendingChunks = this._streamQueue.size;
      return steps;
    }

    requestCorridor(from, to, options = {}) {
      if (!from || !to || !Number.isFinite(from.x + from.y + to.x + to.y)) return;
      const id = String(options.id || 'journey:' + Math.floor(from.x / CHUNK) + ',' + Math.floor(from.y / CHUNK));
      const old = this._corridorRequests.get(id);
      if (old && dist2(from, old.from) < 180 ** 2 && dist2(to, old.to) < 180 ** 2) { old.touched = this._streamSerial; return; }
      const startRoad = this._roadInfo(from.x, from.y), endRoad = this._roadInfo(to.x, to.y);
      const points = options.profile ? [from, { x: startRoad.x, y: endRoad.y }, { x: endRoad.x, y: endRoad.y }, to] : [from, to];
      const chunks = new Map(), padding = Math.min(2, Math.max(1, Math.ceil(((options.radius || 0) + 384) / CHUNK)));
      for (let i = 1; i < points.length; i++) {
        const a = points[i - 1], b = points[i], steps = Math.max(1, Math.ceil(dist(a, b) / (CHUNK * .5)));
        for (let n = 0; n <= steps; n++) {
          const px = lerp(a.x, b.x, n / steps), py = lerp(a.y, b.y, n / steps), cx = Math.floor(px / CHUNK), cy = Math.floor(py / CHUNK);
          // One neighboring chunk on each side covers A* detours, facilities
          // that straddle a border, and full hull clearance at chunk seams.
          for (let dy = -padding; dy <= padding; dy++) for (let dx = -padding; dx <= padding; dx++) {
            const xx = cx + dx, yy = cy + dy, key = xx + ',' + yy;
            if (!chunks.has(key)) chunks.set(key, { cx: xx, cy: yy, distance: dist2(from.x, from.y, (xx + .5) * CHUNK, (yy + .5) * CHUNK) * .04 });
          }
        }
      }
      const bounded = new Map([...chunks].sort((a, b) => a[1].distance - b[1].distance).slice(0, 96));
      this._corridorRequests.set(id, { from: { x: from.x, y: from.y }, to: { x: to.x, y: to.y }, chunks: bounded, touched: this._streamSerial });
      if (this._corridorRequests.size > 24) this._corridorRequests.delete(this._corridorRequests.keys().next().value);
    }

    boundsReady(minX, minY, maxX, maxY) {
      if (!this._streaming) return true;
      // Fortified compounds can overlap the owning chunk. Require their
      // neighboring blueprint before permitting movement along a chunk edge.
      const pad = 384;
      for (let cy = Math.floor((minY - pad) / CHUNK); cy <= Math.floor((maxY + pad) / CHUNK); cy++) {
        for (let cx = Math.floor((minX - pad) / CHUNK); cx <= Math.floor((maxX + pad) / CHUNK); cx++) {
          if (!this.chunks.has(cx + ',' + cy)) return false;
        }
      }
      return true;
    }

    trimDistant(x, y, pins = [], distance = 6500, maxChunks = 6) {
      const distance2 = distance * distance;
      let removed = 0;
      for (const [key, chunk] of this.chunks) {
        if (removed >= maxChunks) break;
        if ([...this._corridorRequests.values()].some(request => this._streamSerial - request.touched <= 180 && request.chunks.has(key))) continue;
        const px = chunk.x + CHUNK / 2, py = chunk.y + CHUNK / 2;
        if ((px - x) ** 2 + (py - y) ** 2 < distance2
          || pins.some(p => (px - p.x) ** 2 + (py - p.y) ** 2 < 1800 ** 2)) continue;
        const changes = [];
        for (const id of chunk.objects) {
          const object = this.objects.get(id);
          if (!object) continue;
          this._unindexObject(object);
          // Site objects remain addressable by gameplay IDs for strategic raids
          // and sleeping garrisons. Procedural foliage can be regenerated from
          // its seed, retaining only actual changes rather than full geometry.
          if (object.siteId) continue;
          if (!this._pristineScenery(object)) changes.push([id, { ...object }]);
          this.objects.delete(id);
        }
        this._coldChunks.set(key, { id: key, cx: chunk.cx, cy: chunk.cy,
          sites: chunk.sites.slice(), changes });
        this.chunks.delete(key);removed++;
      }
      if (removed) this.chunkRevision++;
      if (this._sitePlanCache.size > 1500) {
        for (const key of this._sitePlanCache.keys()) {
          const [cx, cy] = key.split(',').map(Number), px = (cx + .5) * CHUNK, py = (cy + .5) * CHUNK;
          if ((px - x) ** 2 + (py - y) ** 2 > distance2 * 2.25) this._sitePlanCache.delete(key);
        }
      }
      this.stats.coldChunks = this._coldChunks.size;
      return removed;
    }

    _pristineScenery(object) {
      const natural = object.id.startsWith('obj:') || object.id === 'opening-berries';
      const hp = HP[object.type] || 100;
      const unchanged = natural && !object.dead && object.hp === hp && object.maxHp === hp
        && object.solid === (object.type !== 'berry')
        && (object.type !== 'berry' || object.food === object.count);
      if (!unchanged || object._pristineHash === undefined) return unchanged;
      const { _pristineHash, ...record } = object;
      return hash(JSON.stringify(record)) === _pristineHash;
    }

    _unindexObject(object) {
      if (object._spatialKeys) {
        for (const key of object._spatialKeys) {
          const bucket = this._spatial.get(key);
          if (!bucket) continue;
          bucket.delete(object);if (!bucket.size) this._spatial.delete(key);
        }
        object._spatialKeys.length = 0;
        return;
      }
      const cell = this._collisionCell;
      const hx = Math.max(object.r || 0, (object.w || 0) / 2) + 3;
      const hy = Math.max(object.r || 0, (object.h || 0) / 2) + 3;
      for (let cy = Math.floor((object.y - hy) / cell); cy <= Math.floor((object.y + hy) / cell); cy++) {
        for (let cx = Math.floor((object.x - hx) / cell); cx <= Math.floor((object.x + hx) / cell); cx++) {
          const key = cx + ',' + cy, bucket = this._spatial.get(key);
          if (!bucket) continue;
          bucket.delete(object);
          if (!bucket.size) this._spatial.delete(key);
        }
      }
    }

    _waterAt(x, y) {
      if (this.terrain !== ATSWorld.prototype.terrain) return this.terrain(x, y).water;
      const river = this._riverInfo(x, y);
      return river.distance < river.width && this._roadInfo(x, y).dx >= 36;
    }

    waterBlocked(x, y, radius = 0) {
      if (this._waterAt(x, y)) return true;
      const bank = radius * .7;
      return radius > 4 && (this._waterAt(x + bank, y) || this._waterAt(x - bank, y)
        || this._waterAt(x, y + bank) || this._waterAt(x, y - bank));
    }

    vehicleRadius(profile) { return VEHICLE_CLEARANCE[profile] || 21; }

    vehicleWaypoint(from, to, range = 560) {
      const distance = dist(from, to);
      if (distance <= range) return { x: to.x, y: to.y };
      if (this.terrain !== ATSWorld.prototype.terrain) return {
        x: lerp(from.x, to.x, range / distance), y: lerp(from.y, to.y, range / distance)
      };
      const start = this._roadInfo(from.x, from.y), end = this._roadInfo(to.x, to.y);
      const vertical = start.dx < start.dy, targetVertical = end.dx < end.dy;
      const destination = targetVertical ? { x: end.x, y: to.y } : { x: to.x, y: end.y };
      const along = (a, b) => a + clamp(b - a, -range, range);
      if (dist(from, destination) < 80) return { x: to.x, y: to.y };
      if (vertical) {
        const sameColumn = Math.round((from.x - 560) / DISTRICT) === Math.round((destination.x - 560) / DISTRICT);
        if (sameColumn) { const y = along(from.y, destination.y); return { x: this._roadInfo(from.x, y).x, y }; }
        const junctionY = this._roadInfo(from.x, end.y).y;
        if (Math.abs(from.y - junctionY) > 55) { const y = along(from.y, junctionY); return { x: this._roadInfo(from.x, y).x, y }; }
        const x = along(from.x, destination.x); return { x, y: this._roadInfo(x, end.y).y };
      }
      const sameRow = Math.round((from.y - 120) / DISTRICT) === Math.round((destination.y - 120) / DISTRICT);
      if (sameRow) { const x = along(from.x, destination.x); return { x, y: this._roadInfo(x, from.y).y }; }
      const junctionX = this._roadInfo(end.x, from.y).x;
      if (Math.abs(from.x - junctionX) > 55) { const x = along(from.x, junctionX); return { x, y: this._roadInfo(x, from.y).y }; }
      const y = along(from.y, destination.y); return { x: this._roadInfo(end.x, y).x, y };
    }

    vehicleObstacleRadius(object, profile) {
      // Infantry keeps its forgiving trunk collision. Armor needs the whole
      // tree clearance, and can crush only explicitly small vegetation.
      if (object.type === 'tree') {
        if (profile === 'tank' && (object.smallVegetation || (object.size < .8 && object.height < 75))) return -1;
        return object.r || 16;
      }
      return object._collision?.radius ?? object.moveRadius ?? object.r ?? 15;
    }

    vehicleTerrain(x, y) {
      const terrain = this.terrain(x, y);
      if (terrain.road || terrain.water) return terrain;
      // Blueprint lookups are deterministic and cached; no chunk generation or
      // global site scans are needed to recognize cleared military staging.
      const cx = Math.floor(x / CHUNK), cy = Math.floor(y / CHUNK);
      const key = cx + ',' + cy;
      let plans = this._vehiclePlanCache.get(key);
      if (!plans) {
        plans = [];
        for (let dy = -1; dy <= 1; dy++) for (let dx = -1; dx <= 1; dx++) {
          const plan = this._sitePlan(cx + dx, cy + dy);
          if (plan?.military) plans.push(plan);
        }
        this._vehiclePlanCache.set(key, plans);
        if (this._vehiclePlanCache.size > 1500) this._vehiclePlanCache.delete(this._vehiclePlanCache.keys().next().value);
      }
      for (const plan of plans) {
        if (Math.abs(x - plan.x) < plan.radius && Math.abs(y - plan.y) < plan.radius)
          return { ...terrain, compound: true };
        const points = plan.approach || [];
        for (let i = 1; i < points.length; i++) {
          const a = points[i - 1], b = points[i], vx = b.x - a.x, vy = b.y - a.y;
          const t = clamp(((x - a.x) * vx + (y - a.y) * vy) / (vx * vx + vy * vy || 1), 0, 1);
          if (dist2(x, y, a.x + vx * t, a.y + vy * t) < 48 * 48) return { ...terrain, road: true, accessRoad: true };
        }
      }
      return terrain;
    }

    vehicleBlocked(x, y, radius, profile = 'truck', ignoreId) {
      radius = Math.max(radius || 0, this.vehicleRadius(profile));
      if (this._streaming) { if (!this.boundsReady(x - radius, y - radius, x + radius, y + radius)) return true; }
      else this.ensure(x, y, 64);
      if (this.waterBlocked(x, y, radius)) return true;
      const terrain = this.vehicleTerrain(x, y);
      if (terrain.biome === 'wetland' && !terrain.road && !terrain.compound) return true;
      let blocked = false;
      this._queryCollision(x - radius, y - radius, x + radius, y + radius, object => {
        if (object.id === ignoreId || !object.solid || object.dead || object.hp <= 0) return;
        const obstacleRadius = this.vehicleObstacleRadius(object, profile);
        if (obstacleRadius < 0) return;
        const touches = object.collision === 'rect' ? this._touches(object, x, y, radius)
          : dist2(x, y, object.x, object.y) < (obstacleRadius + radius) ** 2;
        if (touches) { blocked = true; return false; }
      });
      return blocked;
    }

    militaryCapacity(site) {
      if (!site) return null;
      site.military = site.military ?? MILITARY_TYPES.has(site.type);
      // Legacy campaigns derive supply once. Consuming an inventory entry is
      // persistent and never replenished by calling this method or loading.
      if (!site.vehicleInventory) {
        const t = site.tier || 0, major = site.type === 'regionalCommand', depot = site.type === 'armoredDepot';
        site.vehicleInventory = { jeep: t >= 2 ? 2 : t === 1 ? 1 : 0,
          armored: t >= 4 ? 2 : t === 3 ? 1 : 0,
          command: t >= 4 ? 1 : 0, truck: t >= 3 ? major ? 5 : 3 : t === 2 ? 1 : 0,
          apc: t >= 4 || site.military ? major ? 4 : 2 : t === 3 ? 1 : 0,
          ifv: t >= 5 ? major ? 2 : 1 : 0, tank: t >= 5 || depot ? major ? 3 : depot ? 2 : 1 : 0,
          heli: t >= 4 ? major ? 3 : 2 : t === 3 ? 1 : 0 };
      }
      if (site.armorCapacity === undefined) site.armorCapacity = Object.entries(site.vehicleInventory)
        .reduce((sum, [kind, count]) => sum + count * ({ tank: 12, ifv: 9, apc: 7 }[kind] || 0), 0);
      const structures = (site.objects || []).map(id => this.objects.get(id)).filter(Boolean);
      const online = (type, fallback) => {
        const found = structures.filter(o => o.type === type);
        return found.length ? found.some(o => !o.dead && o.hp > 0) : fallback;
      };
      const radio = !site.radioDown && site.radioOnline !== false && online('radio', (site.tier || 0) >= 2);
      const depot = !site.depotDown && site.depotOnline !== false && online('depot', (site.tier || 0) >= 3);
      const barracks = !site.barracksDown && site.barracksOnline !== false && online('barracks', (site.tier || 0) >= 2);
      const fuel = !site.fuelDown && site.fuelOnline !== false && online('fuel', (site.tier || 0) >= 3);
      return { inventory: site.vehicleInventory, vehicleInventory: site.vehicleInventory, armorCapacity: Math.max(0, site.armorCapacity),
        radio, depot, barracks, fuel, coordination: radio ? 1 : .3,
        armorFactor: depot ? 1 : .15, infantryFactor: barracks ? 1 : .3,
        fuelFactor: fuel ? 1 : .25 };
    }

    vehicleStaging(site, profile, index = 0) {
      const points = site.staging?.length ? site.staging : site.approach?.length ? [site.approach.at(-1)]
        : [{ x: site.x, y: site.y + (site.radius || 160) + 90 }];
      const base = points[index % points.length], radius = this.vehicleRadius(profile);
      // Line columns up along the road instead of offsetting large hulls into
      // the trees bordering a staging point. No inventory is spent on failure.
      for (const offset of [0, 84, -84, 168, -168, 252, -252]) {
        const y = base.y + Math.floor(index / points.length) * 96 + offset;
        const road = this._roadInfo(base.x, y), p = { x: Math.abs(road.x - base.x) < 96 ? road.x : base.x, y };
        if (!this.vehicleBlocked(p.x, p.y, radius, profile)) return p;
      }
      return null;
    }

    _sitePlan(cx, cy) {
      const key = cx + ',' + cy;
      if (this._sitePlanCache.has(key)) return this._sitePlanCache.get(key);
      let plan = null;
      if (cx === 0 && cy === -1) {
        plan = { id: 'opening-rescue', x: 120, y: -100, name: 'Abandoned Transport Cage',
          type: 'transport', tier: 0, count: 3, guards: 0, radius: 68, tutorial: true };
      } else if (cx === 0 && cy === 0) {
        plan = { id: 'opening-hunters', x: 560, y: 120, name: 'Blackpine Hunter Camp',
          type: 'hunter', tier: 1, count: 12, guards: 2, radius: 153, tutorial: true };
      } else {
        const r = rng(this.seed + ':site:' + key);
        const distance = Math.hypot((cx + 0.5) * CHUNK, (cy + 0.5) * CHUNK);
        // River bands cross the middle chunk of each district. Put its hub on
        // the dry northern shelf, with satellites nearer the crossings.
        const hub = (cx % 3 + 3) % 3 === 1 && (cy % 3 + 3) % 3 === 0;
        const probability = hub && distance > 1800 ? 0.92 : distance < 1600 ? 0.32 : distance < 4500 ? 0.43 : 0.49;
        if (r() < probability) {
          let sx = cx * CHUNK + 205 + r() * (CHUNK - 410);
          let sy = cy * CHUNK + 205 + r() * (CHUNK - 410);
          const road = this._roadInfo(sx, sy);
          if (Math.min(road.dx, road.dy) < 265) {
            const setback = hub && distance > 3600 ? 290 : 155;
            if (road.dx < road.dy) sx = road.x + (r() > 0.5 ? setback : -setback);
            else sy = road.y + (r() > 0.5 ? setback : -setback);
            sx = clamp(sx, cx * CHUNK + 185, (cx + 1) * CHUNK - 185);
            sy = clamp(sy, cy * CHUNK + 185, (cy + 1) * CHUNK - 185);
          }
          // Facilities remain on dry land. Their footprints never occlude a river crossing.
          for (let i = 0; i < 6 && this.terrain(sx, sy).river; i++) sy += sy % 2304 > 1320 ? 78 : -78;
          const d = Math.hypot(sx, sy);
          if (d > 610 && dist(sx, sy, 560, 120) > 475 && !this.terrain(sx, sy).water) {
            const maxTier = d < 1800 ? 1 : d < 3600 ? 2 : d < 6200 ? 3 : d < 10000 ? 4 : 5;
            const district=this.districtAt(sx,sy);
            // Major compounds occupy one slot per district. Roads have satellites, not a fortress every chunk.
            const tier=hub?maxTier:Math.min(maxTier,r()<.15?3:Math.min(2,maxTier));
            let type;
            if (tier === 1) type = r() < 0.35 ? 'transport' : 'hunter';
            else if (tier === 2) type = district.role==='research'||(road.dx>180&&road.dy>180)?'research':'checkpoint';
            else if (tier === 3) type = r() < 0.2 ? 'checkpoint' : 'prison';
            else if (tier === 4) type = r() < 0.27 ? 'prison' : 'detention';
            else type = r() < 0.23 ? 'detention' : 'experimental';
            // Major hubs can house mechanized garrisons. Command bases are a
            // rare Tier 5 blueprint, not another facility in every chunk.
            if (hub && tier >= 3 && (district.role === 'military' || r() < .56)) {
              const militaryRoll = r();
              type = tier === 3 ? 'forwardBase' : tier === 4 ? militaryRoll < .55 ? 'forwardBase' : 'armoredDepot'
                : militaryRoll < .13 ? 'regionalCommand' : militaryRoll < .65 ? 'armoredDepot' : 'forwardBase';
            }
            const counts = { transport: [4, 7], hunter: [10, 18], research: [20, 36], checkpoint: [18, 32],
              prison: [40, 70], detention: [80, 120], experimental: [120, 180],
              forwardBase: [32, 60], armoredDepot: [36, 70], regionalCommand: [120, 180] };
            const guards = { transport: [0, 2], hunter: [2, 4], research: [4, 5], checkpoint: [4, 7],
              prison: [6, 10], detention: [10, 14], experimental: [14, 19],
              forwardBase: [30, 50], armoredDepot: [32, 48], regionalCommand: [62, 78] };
            plan = { id: 'site:' + key, x: sx, y: sy, name: choice(r, ADJECTIVES) + ' ' + SITE_LABELS[type],
              type, tier, count: int(r, ...counts[type]), guards: int(r, ...guards[type]),
              radius: type === 'regionalCommand' ? 430 : type === 'armoredDepot' ? 345 : type === 'forwardBase' ? 305
                : type === 'transport' ? 76 : type === 'hunter' ? 154 : 145 + tier * 20 };
            if (MILITARY_TYPES.has(type)) {
              // A larger perimeter must sit back from both main roads. Choose
              // a dry parcel within its owning chunk instead of walling off a
              // regional highway or letting the access lane cross a river.
              const candidates = [{ x: sx, y: sy }];
              for (const oy of [96, 288, 480, 672]) for (const ox of [96, 288, 480, 672])
                candidates.push({ x: cx * CHUNK + ox, y: cy * CHUNK + oy });
              const placement = candidates.filter(p => {
                const info = this._roadInfo(p.x, p.y);
                return Math.min(info.dx, info.dy) > plan.radius + 65
                  && this._riverInfo(p.x, p.y).distance > plan.radius + 115;
              }).sort((a, b) => dist2(a.x, a.y, sx, sy) - dist2(b.x, b.y, sx, sy))[0];
              if (placement) { plan.x = sx = placement.x; plan.y = sy = placement.y; }
              else plan = null;
            }
            if (plan && this._riverInfo(sx, sy).distance < plan.radius + 115) plan = null;
            if(plan){plan.district=district.id;plan.region=district.name;plan.military=MILITARY_TYPES.has(type);plan.role=plan.military?'military installation':hub?'regional hub':type==='checkpoint'?'road control':type==='research'?'capture and research':'supply outpost';plan.layout=Math.floor(r()*3);}
          }
        }
      }
      if (plan) {
        const entrance = { x: plan.x, y: plan.y + plan.radius + 26 };
        const road = this._roadInfo(entrance.x, entrance.y);
        plan.entrance = entrance;
        // Join the north/south road below the perimeter instead of drawing a
        // supply road through the cages and buildings.
        plan.approach = [entrance, { x: road.x, y: entrance.y }];
        if (plan.military) {
          plan.staging = [plan.approach[1], { x: this._roadInfo(road.x, entrance.y + 100).x, y: entrance.y + 100 }]
            .filter(p => !this.terrain(p.x, p.y).water);
          plan.roadblocks = [-320, 0, 320].map(offset => {
            const y = entrance.y + offset, info = this._roadInfo(road.x, y);
            return { x: info.x, y, angle: Math.PI / 2 };
          }).filter(p => !this.terrain(p.x, p.y).water);
        }
      }
      this._sitePlanCache.set(key, plan);
      return plan;
    }

    _object(chunk, data) {
      const hp = data.hp === undefined ? HP[data.type] || 100 : data.hp;
      const object = Object.assign({ r: 18, hp, maxHp: hp, solid: true, dead: false,
        height: 24, angle: 0, siteId: null }, data);
      if (!object.siteId && (object.id.startsWith('obj:') || object.id === 'opening-berries'))
        object._pristineHash = hash(JSON.stringify(object));
      const saved = chunk._changes?.get(object.id);
      if (saved) Object.assign(object, saved);
      chunk.objects.push(object.id);
      if (chunk._pendingObjects) chunk._pendingObjects.push(object);
      else { this.objects.set(object.id, object); this._indexObject(object); }
      return object;
    }

    _indexObject(object) {
      // Index resources as well as solids so harvesting and local structure
      // searches use the same bounded spatial query as collision.
      if (!Object.prototype.hasOwnProperty.call(object, '_atsQueryStamp'))
        Object.defineProperty(object, '_atsQueryStamp', { value: 0, writable: true });
      // Shape geometry is immutable; hp/dead/solid remain live and are checked
      // at query time, so destruction immediately changes collision behavior.
      Object.defineProperty(object, '_collision', { value: {
        hx: (object.w || object.r * 2) / 2, hy: (object.h || object.r * 2) / 2,
        radius: object.moveRadius ?? object.r ?? 15
      }, configurable: true });
      if (!object._spatialKeys) Object.defineProperty(object, '_spatialKeys', { value: [] });
      else if (object._spatialKeys.length) this._unindexObject(object);
      const cell = this._collisionCell;
      const hx = Math.max(object.r || 0, (object.w || 0) / 2) + 3;
      const hy = Math.max(object.r || 0, (object.h || 0) / 2) + 3;
      for (let cy = Math.floor((object.y - hy) / cell); cy <= Math.floor((object.y + hy) / cell); cy++) {
        for (let cx = Math.floor((object.x - hx) / cell); cx <= Math.floor((object.x + hx) / cell); cx++) {
          const key = cx + ',' + cy;
          if (!this._spatial.has(key)) this._spatial.set(key, new Set());
          this._spatial.get(key).add(object);
          object._spatialKeys.push(key);
        }
      }
    }

    _queryCollision(minX, minY, maxX, maxY, callback) {
      const cell = this._collisionCell, stamp = ++this._queryStamp;
      // Nested queries need a local mark set because an inner query changes the
      // shared object stamps. The common, non-nested path allocates no set.
      const seen = this._queryDepth ? new Set() : null;
      this._queryDepth++;
      this.stats.collisionQueries++;
      try {
      for (let cy = Math.floor(minY / cell); cy <= Math.floor(maxY / cell); cy++) {
        for (let cx = Math.floor(minX / cell); cx <= Math.floor(maxX / cell); cx++) {
          const bucket = this._spatial.get(cx + ',' + cy);
          if (!bucket) continue;
          for (const object of bucket) {
            if (seen ? seen.has(object) : object._atsQueryStamp === stamp) continue;
            if (seen) seen.add(object); else object._atsQueryStamp = stamp;
            this.stats.collisionCandidates++;
            if (callback(object) === false) return false;
          }
        }
      }
      return true;
      } finally { this._queryDepth--; }
    }

    _buildSite(chunk, plan) {
      if (this.sites.has(plan.id)) {
        // Rehydrate geometry around the same live site state, never a fresh
        // blueprint that would reset destroyed gates or wounded garrisons.
        const site = this.sites.get(plan.id);
        chunk.sites.push(site.id);
        for (const id of site.objects) {
          const object = this.objects.get(id);
          if (!object) continue;
          chunk.objects.push(id);
          if (chunk._pendingObjects) chunk._pendingObjects.push(object);
          else this._indexObject(object);
        }
        return;
      }
      const r = rng(this.seed + ':blueprint:' + plan.id);
      const site = Object.assign({ objects: [], spawned: false, rescued: false, cleared: false,
        strength: plan.guards * 3 + plan.tier * 5, alarm: false, lastRaid: 0, known: false,
        radioOnline: plan.tier >= 2, depotOnline: plan.tier >= 3,
        barracksOnline: plan.tier >= 2, lightAngle: r() * TAU }, plan);
      if (chunk._pendingSites) chunk._pendingSites.push(site);
      else this.sites.set(site.id, site);
      chunk.sites.push(site.id);
      let index = 0;
      const mirrored = site.layout === 1;
      const add = (type, dx, dy, extra) => {
        if (mirrored) dx = -dx;
        const o = this._object(chunk, Object.assign({ id: site.id + ':' + type + ':' + index++, type,
          x: site.x + dx, y: site.y + dy, siteId: site.id,
          hp: HP[type] + (type === 'cage' ? 0 : Math.max(0, site.tier - 1) * 20) }, extra || {}));
        site.objects.push(o.id);
        return o;
      };
      if (site.type === 'transport') {
        add('cage', 0, 0, { r: 28, w: 58, h: 46, height: 38, hp: site.tutorial ? 46 : 65,
          count: site.count, prisoners: site.count, solid: true });
        if (!site.tutorial) add('vehicle', 72, 13, { r: 29, w: 64, h: 37, height: 25, angle: -0.08 });
        return;
      }
      if (site.type === 'hunter') {
        add('cage', -13, -33, { r: 28, w: 58, h: 48, height: 40, count: site.count,
          prisoners: site.count, hp: 75 });
        add('house', 72, 28, { r: 35, w: 72, h: 61, height: 54, collision: 'rect' });
        add('alarm', -68, 40, { r: 11, height: 44, alarmRange: 660, lightRange: 110, active: false });
        if (!site.tutorial && r() < 0.58) add('tower', 48, -95, { r: 14, height: 82,
          lightRange: 340, angle: r() * TAU, sweep: 0.26 });
        add('berry', -90, -70, { r: 15, solid: false, height: 15, food: 36, count: 36 });
        return;
      }

      if (site.military) {
        this.militaryCapacity(site);
        this._buildMilitarySite(site, r, add);
        return;
      }

      const extent = site.radius - 34;
      site.serviceEntrance={x:site.x+extent*.36*(mirrored?-1:1),y:site.y-extent-24};
      const cageCount = site.count > 30 ? 4 : site.count > 13 ? 3 : 2;
      let prisonersLeft = site.count;
      for (let c = 0; c < cageCount; c++) {
        const count = Math.ceil(prisonersLeft / (cageCount - c));
        prisonersLeft -= count;
        const cageX = -46 + c % 2 * 82;
        const cageY = -58 + Math.floor(c / 2) * 78;
        add('cage', site.layout === 2 ? cageX - 10 : cageX,
          site.layout === 2 ? cageY - 22 : cageY, { r: 29, w: 64, h: 51,
          height: 45, hp: 85 + site.tier * 10, count, prisoners: count });
      }
      if (site.type === 'research') {
        add('house', extent - 31, 31, { r: 42, w: 82, h: 75, height: 62, collision: 'rect' });
        add('wall', -extent, 0, { r: 13, w: 17, h: extent * 1.3, height: 25, collision: 'rect' });
      } else {
        // An obvious front gate and a narrow rear service opening support both assault and infiltration.
        const spacing = 38;
        for (let pos = -extent; pos <= extent; pos += spacing) {
          if (Math.abs(pos) > 49) add('wall', pos, extent, { r: 19, w: 39, h: 15, height: 30,
            collision: 'rect' });
          if (Math.abs(pos - extent * 0.36) > 38) add('wall', pos, -extent, { r: 19, w: 39, h: 15,
            height: 30, collision: 'rect' });
          if (pos > -extent + 15 && pos < extent - 15) {
            add('wall', -extent, pos, { r: 19, w: 15, h: 39, height: 30, collision: 'rect' });
            add('wall', extent, pos, { r: 19, w: 15, h: 39, height: 30, collision: 'rect' });
          }
        }
        add('gate', 0, extent, { r: 39, w: 93, h: 19, height: 40, collision: 'rect',
          hp: 180 + site.tier * 30 });
        add('barracks', -extent + 45, 47, { r: 37, w: 77, h: 59, height: 49, collision: 'rect' });
      }
      add('alarm', 69, 49, { r: 12, height: 51, alarmRange: 900 + site.tier * 190,
        lightRange: 145, active: false });
      add('radio', extent - 28, -extent + 27, { r: 14, height: 85,
        radioRange: 1250 + site.tier * 390 });
      const towers = site.tier >= 4 ? 4 : site.tier >= 3 ? 3 : 2;
      const corners = [[-extent + 14, -extent + 16], [extent - 14, extent - 15],
        [-extent + 14, extent - 16], [extent - 14, extent - 16]];
      for (let i = 0; i < towers; i++) add('tower', corners[i][0], corners[i][1], { r: 16,
        height: 96 + site.tier * 6, lightRange: 330 + site.tier * 43, angle: r() * TAU,
        sweep: 0.14 + r() * 0.13, powered: true });
      if (site.tier >= 3) {
        add('depot', extent - 49, 48, { r: 31, w: 62, h: 53, height: 42, collision: 'rect' });
        add('vehicle', 43, extent - 48, { r: 28, w: 62, h: 35, height: 26,
          angle: Math.PI * 0.5, vehicleType: site.tier >= 4 ? 'armored' : 'jeep' });
        add('fuel', -extent + 39, -extent + 40, { r: 17, w: 28, h: 31, height: 30,
          explosive: true, blastRadius: 135 });
      }
      add('berry', -24, 77, { r: 17, solid: false, height: 15,
        food: 120 + site.tier * 45, count: 120 + site.tier * 45, supply: true });
    }

    _buildMilitarySite(site, r, add) {
      const extent = site.radius - 74, command = site.type === 'regionalCommand', depot = site.type === 'armoredDepot';
      site.serviceEntrance = { x: site.x, y: site.y - extent - 24 };
      // Wide supply gates and rear staging avoid sending a tank through the
      // infantry-sized side openings used by ordinary prison compounds.
      for (let p = -extent; p <= extent; p += 38) {
        if (Math.abs(p) > 82) add('wall', p, extent, { r: 19, w: 39, h: 18, height: 37, collision: 'rect' });
        if (Math.abs(p) > 70) add('wall', p, -extent, { r: 19, w: 39, h: 18, height: 37, collision: 'rect' });
        if (p > -extent + 15 && p < extent - 15) {
          add('wall', -extent, p, { r: 19, w: 18, h: 39, height: 37, collision: 'rect' });
          add('wall', extent, p, { r: 19, w: 18, h: 39, height: 37, collision: 'rect' });
        }
      }
      add('gate', 0, extent, { r: 68, w: 143, h: 21, height: 44, collision: 'rect', hp: 310 + site.tier * 30 });
      const outer = extent + 48;
      if (depot || command) for (let p = -outer; p <= outer; p += 46) {
        if (Math.abs(p) > 110) add('wall', p, outer, { r: 23, w: 47, h: 20, height: 27, collision: 'rect', barricade: true, defenseRing: 2, hp: 245 });
        if (Math.abs(p) > 96) add('wall', p, -outer, { r: 23, w: 47, h: 20, height: 27, collision: 'rect', barricade: true, defenseRing: 2, hp: 245 });
        if (p > -outer + 24 && p < outer - 24) {
          add('wall', -outer, p, { r: 23, w: 20, h: 47, height: 27, collision: 'rect', barricade: true, defenseRing: 2, hp: 245 });
          add('wall', outer, p, { r: 23, w: 20, h: 47, height: 27, collision: 'rect', barricade: true, defenseRing: 2, hp: 245 });
        }
      }
      else for (const sign of [-1, 1]) add('wall', sign * 136, outer,
        { r: 44, w: 86, h: 20, height: 26, collision: 'rect', barricade: true, defenseRing: 2, hp: 210 });
      // Protected firing positions flank the entrance while leaving the wide
      // center lane clear for outbound transports and armor.
      for (const sign of [-1, 1]) {
        add('tower', sign * 104, extent - 22, { r: 16, height: 94, lightRange: 590, angle: Math.PI / 2, sweep: .12, powered: true, floodlight: true });
        add('wall', sign * 106, extent - 59, { r: 26, w: 49, h: 18, height: 25, collision: 'rect', barricade: true, defenseRing: 1, hp: 240 });
      }
      if (command) for (const sign of [-1, 1]) add('tower', sign * (extent - 18), 0,
        { r: 17, height: 116, lightRange: 620, angle: sign < 0 ? Math.PI : 0, sweep: .18, powered: true, floodlight: true });
      const cageCount = command ? 6 : 4;
      let remaining = site.count;
      for (let i = 0; i < cageCount; i++) {
        const count = Math.ceil(remaining / (cageCount - i)); remaining -= count;
        add('cage', -96 + i % 2 * 78, -64 + Math.floor(i / 2) * 67,
          { r: 29, w: 64, h: 51, height: 46, hp: 110 + site.tier * 10, prisoners: count, count });
      }
      const barracksCount = command ? 3 : 2;
      for (let i = 0; i < barracksCount; i++) add('barracks', -extent + 52, -extent + 124 + i * 92,
        { r: 39, w: 76, h: 60, height: 52, collision: 'rect' });
      add('radio', -extent + 44, -extent + 37, { r: 16, height: command ? 134 : 106, radioRange: 2800 + site.tier * 420 });
      add('depot', extent - 74, -51, { r: 44, w: 105, h: 79, height: 53, collision: 'rect', repairBay: true, hp: 410 });
      if (command) add('depot', extent - 74, 59, { r: 41, w: 102, h: 74, height: 49, collision: 'rect', repairBay: true, hp: 410 });
      add('fuel', extent - 44, -extent + 45, { r: 20, w: 36, h: 38, height: 35, explosive: true, blastRadius: 165 });
      if (command || depot) add('fuel', extent - 104, -extent + 45,
        { r: 20, w: 36, h: 38, height: 35, explosive: true, blastRadius: 165 });
      if (command) add('house', 10, -extent + 64, { r: 49, w: 102, h: 76, height: 74, collision: 'rect', commandCenter: true, hp: 480 });
      add('alarm', 41, extent - 47, { r: 12, height: 54, alarmRange: 2200 + site.tier * 160, lightRange: 180, active: false });
      for (const [x, y] of [[-extent + 19, -extent + 18], [extent - 19, -extent + 18], [-extent + 19, extent - 18], [extent - 19, extent - 18]])
        add('tower', x, y, { r: 17, height: 118, lightRange: 550, angle: r() * TAU, sweep: .16, powered: true });
      const parked = command ? ['tank', 'apc', 'truck', 'ifv'] : depot ? ['tank', 'apc', 'truck'] : ['apc', 'truck'];
      for (let i = 0; i < parked.length; i++) {
        const column = command || depot ? i % 2 : 0, row = command || depot ? Math.floor(i / 2) : i;
        const x = column ? Math.min(238, extent - 64) : 126, y = extent - 160 + row * 90;
        add('vehicle', x, y,
          { r: parked[i] === 'tank' ? 35 : 29, w: parked[i] === 'tank' ? 84 : 71, h: parked[i] === 'tank' ? 51 : 40,
            height: parked[i] === 'tank' ? 38 : 31, angle: Math.PI / 2, vehicleType: parked[i], parked: true });
      }
      add('berry', -42, extent - 58, { r: 17, solid: false, height: 16, food: 310 + site.tier * 45, count: 310 + site.tier * 45, supply: true });
    }

    _startChunk(cx, cy) {
      const key = cx + ',' + cy;
      if (this._chunkJobs.has(key)) return this._chunkJobs.get(key);
      const chunk = { id: key, cx, cy, x: cx * CHUNK, y: cy * CHUNK, objects: [], sites: [], generated: false,
        _pendingObjects: [], _pendingSites: [] };
      const cold = this._coldChunks.get(key);
      if (cold) chunk._changes = new Map(cold.changes || []);
      const r = rng(this.seed + ':foliage:' + key);
      const plan = this._sitePlan(cx, cy);
      if (plan) this._buildSite(chunk, plan);
      const nearbySites = [];
      for (let sy = cy - 1; sy <= cy + 1; sy++) for (let sx = cx - 1; sx <= cx + 1; sx++) {
        const p = this._sitePlan(sx, sy);
        if (p) nearbySites.push(p);
      }
      const centerTerrain = this.terrain(chunk.x + CHUNK / 2, chunk.y + CHUNK / 2);
      const attempts = { forest: 112, wetland: 76, rocky: 58, farmland: 46, ruins: 54 }[centerTerrain.biome];
      const job = { chunk, r, nearbySites, attempts, occupied: [], index: 0 };
      this._chunkJobs.set(key, job);
      return job;
    }

    _advanceChunk(job, count) {
      const { chunk, r, nearbySites, attempts, occupied } = job, { cx, cy } = chunk, key = chunk.id;
      const end = Math.min(attempts, job.index + count);
      for (let i = job.index; i < end; i++) {
        const x = chunk.x + 20 + r() * (CHUNK - 40), y = chunk.y + 20 + r() * (CHUNK - 40);
        const t = this.terrain(x, y);
        const road = this._roadInfo(x, y);
        if (x * x + y * y < 172 * 172 || t.water || t.bridge || road.dx < 52 || road.dy < 52) continue;
        if (nearbySites.some(s => s.military ? Math.abs(x - s.x) < s.radius + 23 && Math.abs(y - s.y) < s.radius + 23
          : dist2(x, y, s.x, s.y) < (s.radius + 23) ** 2)) continue;
        if(nearbySites.some(s=>(s.approach||[]).some((a,index,points)=>{if(!index)return false;const b=points[index-1],dx=b.x-a.x,dy=b.y-a.y,t=clamp(((x-a.x)*dx+(y-a.y)*dy)/(dx*dx+dy*dy||1),0,1);return dist2(x,y,a.x+t*dx,a.y+t*dy)<(s.military?64:32)**2})))continue;
        if (occupied.some(o => dist2(x, y, o.x, o.y) < 45 * 45)) continue;
        const v = r();
        let type = v < 0.09 ? 'berry' : t.biome === 'rocky' ? v < 0.65 ? 'rock' : 'tree'
          : t.biome === 'farmland' ? v < 0.33 ? 'berry' : v < 0.48 ? 'rock' : 'tree'
          : t.biome === 'ruins' ? v < 0.43 ? 'rock' : 'tree' : v < 0.2 ? 'rock' : 'tree';
        const size = 0.72 + r() * 0.71;
        const obj = { id: 'obj:' + key + ':' + i, type, x, y, variant: int(r, 0, 4), size,
          r: type === 'tree' ? 16 * size : type === 'rock' ? 22 * size : 14,
          moveRadius:type==='tree'?7.5*size:undefined, biome:t.biome,
          height: type === 'tree' ? 65 + r() * 53 : type === 'rock' ? 20 + r() * 27 : 15,
          solid: type !== 'berry', angle: r() * TAU };
        if (type === 'berry') { obj.food = int(r, 18, 42); obj.count = obj.food; }
        this._object(chunk, obj);
        if (type !== 'berry') occupied.push({ x, y });
      }
      job.index = end;
      if (end < attempts) return chunk;
      if (cx === 0 && cy === 0) {
        this._object(chunk, { id: 'opening-berries', type: 'berry', x: 48, y: 68, r: 16,
          height: 17, solid: false, food: 48, count: 48, variant: 1, size: 1.1 });
      }
      for (const object of chunk._pendingObjects) { this.objects.set(object.id, object); this._indexObject(object); }
      for (const site of chunk._pendingSites) this.sites.set(site.id, site);
      delete chunk._pendingObjects; delete chunk._pendingSites; delete chunk._changes;
      chunk.generated = true;
      this.chunks.set(key, chunk);
      this._chunkJobs.delete(key);
      this._streamQueue.delete(key);
      this._coldChunks.delete(key);
      this.chunkRevision++;
      this.stats.chunksGenerated++;
      this.stats.coldChunks = this._coldChunks.size;
      return chunk;
    }

    _generateChunk(cx, cy) {
      const key = cx + ',' + cy;
      if (this.chunks.has(key)) return this.chunks.get(key);
      const job = this._chunkJobs.get(key) || this._startChunk(cx, cy);
      return this._advanceChunk(job, job.attempts);
    }

    _nearChunks(x, y, radius, callback) {
      // A site may extend past its owning chunk. Padding includes those overlapping structures.
      const padding = 550;
      const minX = Math.floor((x - radius - padding) / CHUNK), maxX = Math.floor((x + radius + padding) / CHUNK);
      const minY = Math.floor((y - radius - padding) / CHUNK), maxY = Math.floor((y + radius + padding) / CHUNK);
      for (let cy = minY; cy <= maxY; cy++) for (let cx = minX; cx <= maxX; cx++) {
        const chunk = this.chunks.get(cx + ',' + cy);
        if (chunk && callback(chunk) === false) return false;
      }
      return true;
    }

    getObjects(x, y, radius) {
      radius = radius === undefined ? 1000 : radius;
      const out = [];
      this.stats.objectQueries++;
      this._queryCollision(x - radius, y - radius, x + radius, y + radius, object => {
          const extent = Math.max(object.r || 0, (object.w || 0) / 2, (object.h || 0) / 2);
          if (dist2(x, y, object.x, object.y) <= (radius + extent) ** 2) out.push(object);
      });
      return out;
    }

    getSites(x, y, radius) {
      radius = radius === undefined ? 1400 : radius;
      const out = [];
      this._nearChunks(x, y, radius, chunk => {
        for (const id of chunk.sites) {
          const site = this.sites.get(id);
          if (site && dist2(x, y, site.x, site.y) <= (radius + site.radius) ** 2) out.push(site);
        }
      });
      return out;
    }

    _touches(object, x, y, radius) {
      if (object.collision === 'rect') {
        const hx = object._collision?.hx ?? (object.w || object.r * 2) / 2,
          hy = object._collision?.hy ?? (object.h || object.r * 2) / 2;
        const closestX = clamp(x, object.x - hx, object.x + hx);
        const closestY = clamp(y, object.y - hy, object.y + hy);
        return dist2(x, y, closestX, closestY) < radius * radius;
      }
      return dist2(x, y, object.x, object.y) < ((object._collision?.radius ?? object.moveRadius ?? object.r ?? 15) + radius) ** 2;
    }

    blocked(x, y, radius, ignoreId) {
      radius = radius === undefined ? 12 : radius;
      if (this._streaming) { if (!this.boundsReady(x - radius, y - radius, x + radius, y + radius)) return true; }
      else this.ensure(x, y, 64);
      if (this.waterBlocked(x, y, radius)) return true;
      let blocked = false;
      this._queryCollision(x - radius, y - radius, x + radius, y + radius, object => {
        if (object.id !== ignoreId && object.solid && !object.dead && object.hp > 0
          && this._touches(object, x, y, radius)) {
          blocked = true; return false;
        }
      });
      return blocked;
    }

    _rayHits(object, x1, y1, x2, y2) {
      if (object.collision === 'rect') {
        const hx = (object.w || object.r * 2) / 2, hy = (object.h || object.r * 2) / 2;
        const dx = x2 - x1, dy = y2 - y1;
        let lo = 0, hi = 1;
        for (const axis of [[x1, dx, object.x - hx, object.x + hx], [y1, dy, object.y - hy, object.y + hy]]) {
          if (Math.abs(axis[1]) < 0.00001) { if (axis[0] < axis[2] || axis[0] > axis[3]) return false; }
          else {
            let a = (axis[2] - axis[0]) / axis[1], b = (axis[3] - axis[0]) / axis[1];
            if (a > b) { const tmp = a; a = b; b = tmp; }
            lo = Math.max(lo, a); hi = Math.min(hi, b);
            if (lo > hi) return false;
          }
        }
        return lo < 0.995 && hi > 0.005;
      }
      const dx = x2 - x1, dy = y2 - y1;
      const lengthSquared = dx * dx + dy * dy;
      const t = lengthSquared ? clamp(((object.x - x1) * dx + (object.y - y1) * dy) / lengthSquared, 0, 1) : 0;
      const rr = object.type === 'tree' ? object.r * 1.08 : object.r;
      return t > 0.005 && t < 0.995 && dist2(x1 + t * dx, y1 + t * dy, object.x, object.y) < rr * rr;
    }

    lineClear(x1, y1, x2, y2) {
      if (!this.boundsReady(Math.min(x1, x2), Math.min(y1, y2), Math.max(x1, x2), Math.max(y1, y2))) return false;
      let clear = true;
      this._queryCollision(Math.min(x1, x2) - 3, Math.min(y1, y2) - 3,
        Math.max(x1, x2) + 3, Math.max(y1, y2) + 3, object => {
        if (object.dead || object.hp <= 0 || !object.solid) return;
        // A guard's own light fixture or the tree it stands behind cannot block light at its origin.
        if (this._touches(object, x1, y1, 2)) return;
        if (this._rayHits(object, x1, y1, x2, y2)) { clear = false; return false; }
      });
      return clear;
    }

    reveal(x, y, radius) {
      radius = radius === undefined ? 360 : radius;
      const minX = Math.floor((x - radius) / DISCOVERY), maxX = Math.floor((x + radius) / DISCOVERY);
      const minY = Math.floor((y - radius) / DISCOVERY), maxY = Math.floor((y + radius) / DISCOVERY);
      for (let cy = minY; cy <= maxY; cy++) for (let cx = minX; cx <= maxX; cx++) {
        if (dist2(x, y, cx * DISCOVERY + DISCOVERY / 2, cy * DISCOVERY + DISCOVERY / 2) < (radius + DISCOVERY * 0.55) ** 2)
          this.discovered.add(cx + ',' + cy);
      }
      for (const site of this.getSites(x, y, radius)) {
        if (dist2(x, y, site.x, site.y) < radius * radius) site.known = true;
      }
    }

    addSuspicion(x, y, amount) {
      const key = Math.floor(x / CHUNK) + ',' + Math.floor(y / CHUNK);
      const old = this.intel.get(key);
      const record = typeof old === 'number' ? { suspicion: old } : old || { suspicion: 0 };
      record.suspicion = clamp((record.suspicion || 0) + amount, 0, 100);
      record.x = x; record.y = y;
      this.intel.set(key, record);
      return record;
    }

    serialize() {
      return { version: 1, seed: this.seed, chunks: Array.from(this.chunks.entries()),
        sites: Array.from(this.sites.entries()), objects: Array.from(this.objects.entries()),
        coldChunks: Array.from(this._coldChunks.entries()),
        discovered: Array.from(this.discovered), intel: Array.from(this.intel.entries()) };
    }

    static fromJSON(data) {
      if (typeof data === 'string') data = JSON.parse(data);
      if (!data || !data.seed) throw new Error('This world save is incomplete.');
      const world = new ATSWorld(data.seed);
      world.chunks = new Map(data.chunks || []);
      world.sites = new Map(data.sites || []);
      world.objects = new Map(data.objects || []);
      world._coldChunks = new Map(data.coldChunks || []);
      const coldSites = new Set();
      for (const chunk of world._coldChunks.values()) for (const id of chunk.sites || []) coldSites.add(id);
      for (const object of world.objects.values()) {
        if(object.type==='tree')object.moveRadius=7.5*(object.size||1);
        if (!object.siteId || !coldSites.has(object.siteId)) world._indexObject(object);
      }
      world.stats.coldChunks = world._coldChunks.size;
      world.discovered = new Set(data.discovered || []);
      world.intel = new Map(data.intel || []);
      return world;
    }
  }

  window.ATSWorld = ATSWorld;
})();
