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
    experimental: 'Experimental Facility' };

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
      this._spatial = new Map();
      this._collisionCell = 128;
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

    _sitePlan(cx, cy) {
      const key = cx + ',' + cy;
      if (this._sitePlanCache.has(key)) return this._sitePlanCache.get(key);
      let plan = null;
      if (cx === 0 && cy === -1) {
        plan = { id: 'opening-rescue', x: 120, y: -100, name: 'Abandoned Transport Cage',
          type: 'transport', tier: 0, count: 3, guards: 0, radius: 68, tutorial: true };
      } else if (cx === 0 && cy === 0) {
        plan = { id: 'opening-hunters', x: 560, y: 120, name: 'Blackpine Hunter Camp',
          type: 'hunter', tier: 1, count: 6, guards: 2, radius: 153, tutorial: true };
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
            const counts = { transport: [2, 4], hunter: [4, 10], research: [8, 15], checkpoint: [5, 12],
              prison: [15, 28], detention: [30, 48], experimental: [45, 72] };
            const guards = { transport: [0, 2], hunter: [2, 4], research: [4, 5], checkpoint: [4, 7],
              prison: [6, 10], detention: [10, 14], experimental: [14, 19] };
            plan = { id: 'site:' + key, x: sx, y: sy, name: choice(r, ADJECTIVES) + ' ' + SITE_LABELS[type],
              type, tier, count: int(r, ...counts[type]), guards: int(r, ...guards[type]),
              radius: type === 'transport' ? 76 : type === 'hunter' ? 154 : 145 + tier * 20 };
            if (this._riverInfo(sx, sy).distance < plan.radius + 115) plan = null;
            if(plan){plan.district=district.id;plan.region=district.name;plan.role=hub?'regional hub':type==='checkpoint'?'road control':type==='research'?'capture and research':'supply outpost';plan.layout=Math.floor(r()*3);}
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
      }
      this._sitePlanCache.set(key, plan);
      return plan;
    }

    _object(chunk, data) {
      const hp = data.hp === undefined ? HP[data.type] || 100 : data.hp;
      const object = Object.assign({ r: 18, hp, maxHp: hp, solid: true, dead: false,
        height: 24, angle: 0, siteId: null }, data);
      this.objects.set(object.id, object);
      chunk.objects.push(object.id);
      this._indexObject(object);
      return object;
    }

    _indexObject(object) {
      if (!object.solid) return;
      const cell = this._collisionCell;
      const hx = Math.max(object.r || 0, (object.w || 0) / 2) + 3;
      const hy = Math.max(object.r || 0, (object.h || 0) / 2) + 3;
      for (let cy = Math.floor((object.y - hy) / cell); cy <= Math.floor((object.y + hy) / cell); cy++) {
        for (let cx = Math.floor((object.x - hx) / cell); cx <= Math.floor((object.x + hx) / cell); cx++) {
          const key = cx + ',' + cy;
          if (!this._spatial.has(key)) this._spatial.set(key, new Set());
          this._spatial.get(key).add(object.id);
        }
      }
    }

    _queryCollision(minX, minY, maxX, maxY, callback) {
      const cell = this._collisionCell, seen = new Set();
      for (let cy = Math.floor(minY / cell); cy <= Math.floor(maxY / cell); cy++) {
        for (let cx = Math.floor(minX / cell); cx <= Math.floor(maxX / cell); cx++) {
          const bucket = this._spatial.get(cx + ',' + cy);
          if (!bucket) continue;
          for (const id of bucket) {
            if (seen.has(id)) continue;
            seen.add(id);
            const object = this.objects.get(id);
            if (object && callback(object) === false) return false;
          }
        }
      }
      return true;
    }

    _buildSite(chunk, plan) {
      if (this.sites.has(plan.id)) return;
      const r = rng(this.seed + ':blueprint:' + plan.id);
      const site = Object.assign({ objects: [], spawned: false, rescued: false, cleared: false,
        strength: plan.guards * 3 + plan.tier * 5, alarm: false, lastRaid: 0, known: false,
        radioOnline: plan.tier >= 2, depotOnline: plan.tier >= 3,
        barracksOnline: plan.tier >= 2, lightAngle: r() * TAU }, plan);
      this.sites.set(site.id, site);
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
        food: 60 + site.tier * 18, count: 60 + site.tier * 18, supply: true });
    }

    _generateChunk(cx, cy) {
      const key = cx + ',' + cy;
      if (this.chunks.has(key)) return this.chunks.get(key);
      const chunk = { id: key, cx, cy, x: cx * CHUNK, y: cy * CHUNK, objects: [], sites: [], generated: true };
      this.chunks.set(key, chunk);
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
      const occupied = [];
      for (let i = 0; i < attempts; i++) {
        const x = chunk.x + 20 + r() * (CHUNK - 40), y = chunk.y + 20 + r() * (CHUNK - 40);
        const t = this.terrain(x, y);
        const road = this._roadInfo(x, y);
        if (x * x + y * y < 172 * 172 || t.water || t.bridge || road.dx < 52 || road.dy < 52) continue;
        if (nearbySites.some(s => dist2(x, y, s.x, s.y) < (s.radius + 23) ** 2)) continue;
        if(nearbySites.some(s=>(s.approach||[]).some((a,index,points)=>{if(!index)return false;const b=points[index-1],dx=b.x-a.x,dy=b.y-a.y,t=clamp(((x-a.x)*dx+(y-a.y)*dy)/(dx*dx+dy*dy||1),0,1);return dist2(x,y,a.x+t*dx,a.y+t*dy)<32*32})))continue;
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
      if (cx === 0 && cy === 0) {
        this._object(chunk, { id: 'opening-berries', type: 'berry', x: 48, y: 68, r: 16,
          height: 17, solid: false, food: 48, count: 48, variant: 1, size: 1.1 });
      }
      return chunk;
    }

    _nearChunks(x, y, radius, callback) {
      // A site may extend past its owning chunk. Padding includes those overlapping structures.
      const padding = 275;
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
      this._nearChunks(x, y, radius, chunk => {
        for (const id of chunk.objects) {
          const object = this.objects.get(id);
          if (!object) continue;
          const extent = Math.max(object.r || 0, (object.w || 0) / 2, (object.h || 0) / 2);
          if (dist2(x, y, object.x, object.y) <= (radius + extent) ** 2) out.push(object);
        }
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
        const hx = (object.w || object.r * 2) / 2, hy = (object.h || object.r * 2) / 2;
        const closestX = clamp(x, object.x - hx, object.x + hx);
        const closestY = clamp(y, object.y - hy, object.y + hy);
        return dist2(x, y, closestX, closestY) < radius * radius;
      }
      return dist2(x, y, object.x, object.y) < ((object.moveRadius ?? object.r ?? 15) + radius) ** 2;
    }

    blocked(x, y, radius, ignoreId) {
      radius = radius === undefined ? 12 : radius;
      this.ensure(x, y, 64);
      if (this.terrain(x, y).water) return true;
      // Also prevent a body straddling a riverbank, while leaving bridges wide enough for a horde.
      if (radius > 4 && (this.terrain(x + radius * 0.7, y).water || this.terrain(x - radius * 0.7, y).water
        || this.terrain(x, y + radius * 0.7).water || this.terrain(x, y - radius * 0.7).water)) return true;
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
        discovered: Array.from(this.discovered), intel: Array.from(this.intel.entries()) };
    }

    static fromJSON(data) {
      if (typeof data === 'string') data = JSON.parse(data);
      if (!data || !data.seed) throw new Error('This world save is incomplete.');
      const world = new ATSWorld(data.seed);
      world.chunks = new Map(data.chunks || []);
      world.sites = new Map(data.sites || []);
      world.objects = new Map(data.objects || []);
      for (const object of world.objects.values()){if(object.type==='tree')object.moveRadius=7.5*(object.size||1);world._indexObject(object);}
      world.discovered = new Set(data.discovered || []);
      world.intel = new Map(data.intel || []);
      return world;
    }
  }

  window.ATSWorld = ATSWorld;
})();
