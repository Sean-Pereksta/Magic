/* IRONFIELD — deterministic individual-soldier battle simulation.
 * Plain JavaScript, no DOM or network dependencies. Spatial hashing keeps
 * local combat bounded; formations plan routes on a separate coarse grid.
 */
(function (global) {
  'use strict';

  const DATA = global.GameData;
  const TYPES = DATA.TYPES;
  const FORMATIONS = DATA.FORMATIONS || {};
  const clamp = DATA.clamp;
  const TAU = Math.PI * 2;
  // World coordinates and velocities are bounded; a two-component square
  // root avoids Math.hypot's generic argument/overflow machinery in hot loops.
  const length2 = (x, y) => Math.sqrt(x * x + y * y);
  const CELL = 80;
  const BODY_CELL = 32;
  const NAV = 80;
  const MAX_EFFECTS = 420;
  const MAX_PROJECTILES = Math.max(16000, Math.floor((DATA.MAX_UNITS || 40000) * .8));
  const EMPTY_TERRAIN = Object.freeze({ type: 'plain', speed: 1, cover: 0, elevation: 0, blocked: false });
  const ground = id => Object.freeze({ type: id, speed: DATA.TERRAIN_TYPES[id].speed, cover: DATA.TERRAIN_TYPES[id].cover, elevation: 0, blocked: false });
  const TERRAINS = {
    forest: Object.freeze({ type: 'forest', speed: .78, cover: .36, elevation: 0, blocked: false }),
    hill: Object.freeze({ type: 'hill', speed: .88, cover: .06, elevation: 1, blocked: false }),
    water: Object.freeze({ type: 'water', speed: 0, cover: 0, elevation: 0, blocked: true }),
    bridge: Object.freeze({ type: 'bridge', speed: 1, cover: .04, elevation: 0, blocked: false }),
    rock: Object.freeze({ type: 'rock', speed: 0, cover: .4, elevation: 1, blocked: true }),
    wall: Object.freeze({ type: 'wall', speed: 0, cover: .65, elevation: 1, blocked: true }),
    gate: Object.freeze({ type: 'gate', speed: 0, cover: .50, elevation: 0, blocked: true }),
    rubble: Object.freeze({ type: 'rubble', speed: .78, cover: .12, elevation: 0, blocked: false }),
    ramp: Object.freeze({ type: 'ramp', speed: .70, cover: .18, elevation: 1, blocked: false }),
    road: ground('road'), mud: ground('mud'), marsh: ground('marsh'), snow: ground('snow'), trench: ground('trench')
  };
  const GROUND_CODES = Object.freeze({ forest: 1, hill: 2, road: 3, mud: 4, marsh: 5, snow: 6, trench: 7 });
  const GROUND_COST = Object.freeze([1, 1 / .78, 1 / .88, ...['road', 'mud', 'marsh', 'snow', 'trench'].map(id => 1 / TERRAINS[id].speed)]);
  const GROUND_OVERLAYS = new Set(['road', 'mud', 'marsh', 'snow', 'trench']);
  const STRUCTURE_KINDS = new Set(['stone', 'palisade', 'earthwork', 'house', 'watchtower', 'barricade', 'keep']);

  function inside(x, y, r, margin) {
    margin = margin || 0;
    return x >= r.x - margin && x <= r.x + r.w + margin && y >= r.y - margin && y <= r.y + r.h + margin;
  }

  function segmentBox(x, y, dx, dy, r, margin) {
    let enter = 0, leave = 1;
    const minX = r.x - margin, maxX = r.x + r.w + margin;
    const minY = r.y - margin, maxY = r.y + r.h + margin;
    if (minX > maxX || minY > maxY) return null;
    for (const [p, d, low, high] of [[x, dx, minX, maxX], [y, dy, minY, maxY]]) {
      if (Math.abs(d) < 1e-9) { if (p < low || p > high) return null; continue; }
      const a = (low - p) / d, b = (high - p) / d;
      enter = Math.max(enter, Math.min(a, b)); leave = Math.min(leave, Math.max(a, b));
      if (leave < enter) return null;
    }
    return [enter, leave];
  }

  function seedValue(value) {
    if (Number.isFinite(Number(value))) return (Number(value) >>> 0) || 1;
    let hash = 2166136261;
    const text = String(value || 'IRONFIELD');
    for (let i = 0; i < text.length; i++) hash = Math.imul(hash ^ text.charCodeAt(i), 16777619);
    return hash >>> 0;
  }

  class BattleEngine {
    constructor(input) {
      this.config = this._normalizeConfig(input || DATA.DEFAULT_CONFIG);
      this.seed = this.config.seed;
      this._random = DATA.RNG(this.seed);
      this.map = DATA.MAPS.find(m => m.id === this.config.map) || DATA.MAPS[0];
      this.map = Object.assign({}, this.map, { regions: this.map.regions.map(r => Object.assign({}, r)) });
      if (this.config.map === 'custom') this.map = Object.assign({}, this.map, this.config.customMap, { regions: this.config.customMap.regions.map(r => Object.assign({}, r)), seed: this.seed });
      this.width = this.map.width;
      this.height = this.map.height;
      this.time = 0;
      this.phase = 'deployment';
      this.winner = null;
      this.reason = '';
      this.units = [];
      // Deaths remain physical records for the entire encounter. Renderers
      // consume this append-only index instead of rescanning every casualty.
      this.deadUnitIds = [];
      this.regiments = [];
      this.projectiles = [];
      this.effects = [];
      this.events = [];
      this._trafficPlans = [];
      this._crossingCooldowns = new Map();
      this._battleAxis = null;
      this._tick = 0;
      this._inUnitStep = false;
      this._marchStreams = [];
      this._strategyTimer = 0;
      this._moraleTimer = 0;
      this._lastCasualty = 0;
      this._lastStallEvent = 0;
      this._stats = [0, 1].map(() => ({ initial: 0, alive: 0, dead: 0, routed: 0, kills: 0, morale: 0, power: 0, fallingBack: 0, rallied: 0 }));
      this._regions = this.map.regions || [];
      this._minimumGroundCost = this._regions.some(r => r.type === 'road') ? 1 / TERRAINS.road.speed : 1;
      this._fortifications = this._regions.filter(r => r.type === 'wall' || r.type === 'gate');
      this.terrainRevision = 0;
      this.structures = [];
      this._bridges = this._regions.filter(r => r.type === 'bridge');
      this._waters = this._regions.filter(r => r.type === 'water');
      this._blocks = this._regions.filter(r => r.type === 'water' || r.type === 'rock' || r.type === 'wall' || r.type === 'gate');
      this._cols = Math.ceil(this.width / CELL);
      this._rows = Math.ceil(this.height / CELL);
      this._heads = [new Int32Array(this._cols * this._rows), new Int32Array(this._cols * this._rows)];
      this._cellCounts = [new Uint16Array(this._cols * this._rows), new Uint16Array(this._cols * this._rows)];
      this._bodyCols = Math.ceil(this.width / BODY_CELL);
      this._bodyRows = Math.ceil(this.height / BODY_CELL);
      this._bodyHeads = [new Int32Array(this._bodyCols * this._bodyRows), new Int32Array(this._bodyCols * this._bodyRows)];
      // Infantry/horse contacts use the fine grid. A handful of elephants or
      // engines must not inflate every infantry neighborhood to their radius.
      this._largeBodyCols = Math.ceil(this.width / 128);
      this._largeBodyRows = Math.ceil(this.height / 128);
      this._smallSolidHeads = [new Int32Array(this._bodyCols * this._bodyRows), new Int32Array(this._bodyCols * this._bodyRows)];
      this._largeSolidHeads = [new Int32Array(this._largeBodyCols * this._largeBodyRows), new Int32Array(this._largeBodyCols * this._largeBodyRows)];
      this._navCols = Math.ceil(this.width / NAV);
      this._navRows = Math.ceil(this.height / NAV);
      this._navBlocked = new Uint8Array(this._navCols * this._navRows);
      this._navTerrain = new Uint8Array(this._navCols * this._navRows);
      // Conservative empty-space cache. The exact rock/water/bridge test is
      // still used near every solid region; ordinary open-ground steps avoid
      // scanning the map's region list for each individual soldier.
      this._nearSolid = new Uint8Array(this._navCols * this._navRows);
      for(const block of this._blocks){
        const minX=Math.max(0,Math.floor((block.x-65)/NAV)),maxX=Math.min(this._navCols-1,Math.floor((block.x+block.w+65)/NAV));
        const minY=Math.max(0,Math.floor((block.y-65)/NAV)),maxY=Math.min(this._navRows-1,Math.floor((block.y+block.h+65)/NAV));
        for(let y=minY;y<=maxY;y++)this._nearSolid.fill(1,y*this._navCols+minX,y*this._navCols+maxX+1);
      }
      this._navClearance = new Map();
      for (let y = 0; y < this._navRows; y++) {
        for (let x = 0; x < this._navCols; x++) {
          this._navBlocked[y * this._navCols + x] = this._blocked((x + .5) * NAV, (y + .5) * NAV) ? 1 : 0;
          const terrain = this.terrainAt((x + .5) * NAV, (y + .5) * NAV);
          this._navTerrain[y * this._navCols + x] = GROUND_CODES[terrain.type] || 0;
        }
      }
      this._navClearance.set(0, this._navBlocked);
      this._approachFields = [new Float64Array(this._navBlocked.length), new Float64Array(this._navBlocked.length)];
      this._approachReservations = [new Map(), new Map()];
      this._approachNext = 0;
      const navCount = this._navBlocked.length;
      this._routeScratch = { cost: new Float64Array(navCount), parent: new Int32Array(navCount), seen: new Uint32Array(navCount), closed: new Uint32Array(navCount), heap: new Int32Array(navCount * 8), priority: new Float64Array(navCount * 8), generation: 0 };
      this._buildArmies();
      // The first few literals can retain superseded numeric field layouts
      // while the roster initializes. Canonicalize those warm-up records to
      // the final shape before any consumer holds a soldier reference.
      if (this.units.length > 32) {
        const template = this.units[this.units.length - 1];
        for (let i = 0; i < 20; i++) this.units[i] = Object.assign({ ...template }, this.units[i]);
      }
      this._next = new Int32Array(this.units.length);
      this._bodyNext = new Int32Array(this.units.length);
      this._collisionCandidates = new Int32Array(this.units.length);
      this._solidNext = new Int32Array(this.units.length);
      // Compact hot geometry keeps broad-phase neighbor scans out of each
      // soldier's much larger animation/command/combat record. These values
      // are synchronized after every real voluntary or forced displacement.
      this._bodyX = new Float64Array(this.units.length); this._bodyY = new Float64Array(this.units.length);
      this._bodyRadius = new Float64Array(this.units.length); this._bodyLive = new Uint8Array(this.units.length);
      this._bodyTeam = new Uint8Array(this.units.length); this._bodyRegiment = new Uint16Array(this.units.length);
      this._bodyZ = new Float64Array(this.units.length);
      this._collisionDX = new Float64Array(this.units.length);
      this._collisionDY = new Float64Array(this.units.length);
      this._collisionGap = new Float64Array(this.units.length);
      this._collisionDistance2 = new Float64Array(this.units.length);
      this._targetClaims = new Uint16Array(this.units.length);
      this._maxRadius = this.units.reduce((largest, u) => Math.max(largest, u._radius), 0);
      this._smallRadius = this.units.reduce((largest,u)=>u._radius<=12?Math.max(largest,u._radius):largest,0);
      this._largeRadius = this.units.reduce((largest,u)=>u._radius>12?Math.max(largest,u._radius):largest,0);
      this.siege = global.BattleSiege ? new global.BattleSiege(this) : null;
      this._siege = this.siege;
      this._rebuildHash();
      this._refreshStats();
      this.impacts = global.BattleImpacts ? new global.BattleImpacts(this) : null;
      this.ramparts = global.BattleRamparts ? new global.BattleRamparts(this) : null;
      if (this.ramparts) this.ramparts.initializeOrders();
      this._event('Armies assembled. Position your formations, then sound the advance.', null, 'deployment');
    }

    _normalizeConfig(input) {
      const source = input && typeof input === 'object' ? input : DATA.DEFAULT_CONFIG;
      const validMap = DATA.MAPS.find(m => m.id === source.map);
      const cap = Math.floor((DATA.MAX_UNITS || 40000) / 2);
      const armies = [0, 1].map(team => {
        const army = (Array.isArray(source.armies) && source.armies[team]) || {};
        const raw = army.roster && typeof army.roster === 'object' ? army.roster : {};
        const roster = {};
        let count = 0;
        for (const type of DATA.TYPE_ORDER) {
          const value = Number(raw[type]);
          roster[type] = Number.isFinite(value) ? Math.floor(clamp(value, 0, Math.min(cap, TYPES[type].maxCount || (TYPES[type].commander ? 16 : cap)))) : 0;
          count += roster[type];
        }
        if (count > cap) {
          let used = 0;
          for (const type of DATA.TYPE_ORDER) {
            roster[type] = Math.floor(roster[type] * cap / count);
            used += roster[type];
          }
          for (const type of DATA.TYPE_ORDER) {
            if (used >= cap) break;
            if (Number(raw[type]) > 0 && roster[type] < (TYPES[type].maxCount || cap)) { roster[type]++; used++; }
          }
        }
        return {
          name: String(army.name || (team === 0 ? 'Azure Crown' : 'Ember Dominion')).slice(0, 64),
          doctrine: DATA.DOCTRINES && Object.prototype.hasOwnProperty.call(DATA.DOCTRINES, army.doctrine) ? army.doctrine : 'balanced',
          roster
        };
      });
      const normalized = { map: validMap ? validMap.id : DATA.MAPS[0].id, seed: seedValue(source.seed), armies };
      let worldWidth = (validMap || DATA.MAPS[0]).width, worldHeight = (validMap || DATA.MAPS[0]).height;
      if (normalized.map === 'custom') {
        const custom = source.customMap && typeof source.customMap === 'object' ? source.customMap : {};
        const size = DATA.normalizeWorldSize ? DATA.normalizeWorldSize(custom.width, custom.height) : { width: 6400, height: 4400 };
        worldWidth = size.width; worldHeight = size.height;
        const biomes = ['grassland', 'wetland', 'desert', 'tundra', 'woodland', 'highland', 'ashland'];
        normalized.customMap = { name: String(custom.name || 'Custom Map').slice(0, 64), biome: biomes.includes(custom.biome) ? custom.biome : 'grassland', width: worldWidth, height: worldHeight, regions: [] };
        for (const raw of (Array.isArray(custom.regions) ? custom.regions : []).slice(0, DATA.MAX_TERRAIN_REGIONS || 192)) {
          if (!raw || !['forest', 'hill', 'rock', 'water', 'bridge', 'wall', 'gate', 'road', 'mud', 'marsh', 'snow', 'trench'].includes(raw.type) || ![raw.x, raw.y, raw.w, raw.h].every(Number.isFinite) || raw.w <= 0 || raw.h <= 0) continue;
          const x = clamp(raw.x, 0, worldWidth - 20), y = clamp(raw.y, 0, worldHeight - 20);
          const region = { type: raw.type, x, y, w: clamp(raw.w, 20, worldWidth - x), h: clamp(raw.h, 20, worldHeight - y) };
          if (raw.type === 'wall' || raw.type === 'gate') {
            region.team = raw.team === 0 ? 0 : 1;
            region.hp = Number.isFinite(raw.hp) ? clamp(raw.hp, 200, 10000) : raw.type === 'wall' ? 2200 : 1000;
            if (raw.type === 'wall' && raw.castleTower === true) region.castleTower = true;
            if (raw.type === 'gate' && raw.castleGate === true) region.castleGate = true;
            region.structureKind = STRUCTURE_KINDS.has(raw.structureKind) ? raw.structureKind : 'stone';
            if (typeof raw.name === 'string' && raw.name.trim()) region.name = raw.name.replace(/[<>]/g, '').trim().slice(0, 80);
            if(raw.walkable===true){region.walkable=true;region.deckHeight=clamp(Number(raw.deckHeight)||36,8,100);region.rampartSide=['west','east','north','south'].includes(raw.rampartSide)?raw.rampartSide:'';
              if(Number.isFinite(raw.courtyardX)&&Number.isFinite(raw.courtyardY)){region.courtyardX=clamp(raw.courtyardX,20,worldWidth-20);region.courtyardY=clamp(raw.courtyardY,20,worldHeight-20);}
              if(Array.isArray(raw.stairs))region.stairs=raw.stairs.slice(0,8).filter(s=>s&&[s.x,s.y,s.w,s.h,s.groundX,s.groundY,s.deckX,s.deckY].every(Number.isFinite)&&s.w>0&&s.h>0).map(s=>({id:String(s.id||'stairs').slice(0,80),x:s.x,y:s.y,w:s.w,h:s.h,axis:s.axis==='y'?'y':'x',dir:s.dir===-1?-1:1,width:clamp(Number(s.width)||64,24,200),height:region.deckHeight,groundX:s.groundX,groundY:s.groundY,deckX:s.deckX,deckY:s.deckY}));}
          }
          normalized.customMap.regions.push(region);
        }
      }
      if (Array.isArray(source.formations)) {
        const remaining = armies.map(a => Object.assign({}, a.roster));
        normalized.formations = [];
        for (const raw of source.formations.slice(0, 512)) {
          if (!raw || ![0, 1].includes(raw.team) || !TYPES[raw.type] || !Number.isFinite(raw.count) || ![raw.x, raw.y].every(Number.isFinite)) continue;
          const count = Math.min(remaining[raw.team][raw.type], Math.max(0, Math.floor(raw.count)), TYPES[raw.type].groupSize || (TYPES[raw.type].commander ? 1 : cap));
          if (!count) continue;
          const group = { team: raw.team, type: raw.type, count, x: clamp(raw.x, 35, worldWidth - 35), y: clamp(raw.y, 35, worldHeight - 35), formation: FORMATIONS[raw.formation] ? raw.formation : raw.type === 'musketeer' ? 'volley' : 'line' };
          if (typeof raw.name === 'string' && raw.name.trim()) group.name = raw.name.replace(/[<>]/g, '').trim().slice(0, 80);
          if(raw.rampart&&typeof raw.rampart==='object')group.rampart={structureId:typeof raw.rampart.structureId==='string'?raw.rampart.structureId.slice(0,80):'',ranks:clamp(Math.floor(Number(raw.rampart.ranks)||2),1,3),spacing:['compact','normal','wide'].includes(raw.rampart.spacing)?raw.rampart.spacing:'normal'};
          if (Number.isFinite(raw.facing)) group.facing = Math.atan2(Math.sin(raw.facing), Math.cos(raw.facing));
          if (Array.isArray(raw.offsets) && raw.offsets.length >= count) {
            const offsets = raw.offsets.slice(0, count);
            if (offsets.every(p => p && Number.isFinite(p.x) && Number.isFinite(p.y) && Math.abs(p.x) <= worldWidth && Math.abs(p.y) <= worldHeight)) group.offsets = offsets.map(p => ({ x: p.x, y: p.y }));
          }
          normalized.formations.push(group);
          remaining[raw.team][raw.type] -= count;
        }
        const pendingGroups = () => remaining.reduce((sum, roster) => sum + DATA.TYPE_ORDER.reduce((n, type) => n + (TYPES[type].commander || TYPES[type].groupSize === 1 ? roster[type] : roster[type] > 0 ? 1 : 0), 0), 0);
        while (normalized.formations.length + pendingGroups() > 512) {
          const removed = normalized.formations.pop();
          remaining[removed.team][removed.type] += removed.count;
        }
      }
      return normalized;
    }

    _buildArmies() {
      const sort = { guardian: 0, pikeman: 1, hoplite: 1.5, halberdier: 2, swordsman: 3, militia: 4, berserker: 5, elephant: 6, batteringRam: 6.3, siegeTower: 6.5, lancer: 7, cavalry: 8, horseArcher: 9, javelin: 10, slinger: 11, archer: 12, crossbow: 13, musketeer: 13.5, catapult: 14, ballista: 15, cannon: 15.3, trebuchet: 15.6, commander: 16 };
      const explicit = this.config.formations || [];
      const rosters = this.config.armies.map(army => Object.assign({}, army.roster));
      const companies = [{}, {}];
      // Preserve layout order, including later detachments appended after the
      // opposite army. Saved IDs and deployment positions then remain aligned.
      for (const group of explicit) {
        const number = (companies[group.team][group.type] || 0) + 1;
        companies[group.team][group.type] = number;
        this._createRegiment(group.team, group.type, group.count, number, group.x, group.y, group.facing);
        const r = this.regiments[this.regiments.length - 1];
        if (group.name) r.name = group.name;
        if (group.rampart) r._initialRampart = group.rampart;
        if (group.formation !== r.formation && !TYPES[group.type].commander) this.setFormation([r.id], group.formation);
        if (group.offsets) {
          // A detached half is a real body of troops, not a newly generated
          // rectangular company. Keep the exact saved deployment footprint.
          for (let slot = 0; slot < r.unitIds.length; slot++) {
            const u = this.units[r.unitIds[slot]], offset = group.offsets[slot];
            u.x = group.x + offset.x; u.y = group.y + offset.y;
            u._orderOffsetX = offset.x; u._orderOffsetY = offset.y;
            u._localGoalX = u.x; u._localGoalY = u.y;
            u._terrain = this.terrainAt(u.x, u.y);
          }
          r._travelSlots = true; r._approachBlendUntil = 2;
        }
        rosters[group.team][group.type] -= group.count;
      }
      // Explicit deployments keep their real bodies. New roster additions
      // need an empty footprint rather than restarting on the first occupied
      // army-grid position. Repair only an invalid imported/map-edited slot.
      const largeDeployment = this.config.armies.reduce((sum, army) => sum + Object.values(army.roster).reduce((n, count) => n + count, 0), 0) > 20000;
      const occupancy = explicit.length || largeDeployment ? { cells: new Map(), maxRadius: Math.max(...Object.values(TYPES).map(t => t.radius)) } : null;
      if (occupancy) {
        const repair = [];
        for (const u of this.units) {
          if (this._deploymentFree(u.x, u.y, u._radius, occupancy)) this._reserveDeploymentBody(u, occupancy);
          else repair.push(u);
        }
        for (const u of repair) this._repairDeploymentBody(u, occupancy);
      }
      const pendingTypes = rosters.reduce((sum, roster) => sum + DATA.TYPE_ORDER.filter(type => !TYPES[type].commander && !TYPES[type].equipment && roster[type] > 0).length, 0);
      const pendingCommanders = rosters.reduce((sum, roster) => sum + DATA.TYPE_ORDER.reduce((n, type) => n + (TYPES[type].commander || TYPES[type].equipment ? roster[type] : 0), 0), 0);
      const layoutBudget = Math.max(1, Math.floor((512 - explicit.length - pendingCommanders) / Math.max(1, pendingTypes)));
      for (let team = 0; team < 2; team++) {
        const roster = rosters[team], companyNumbers = companies[team];
        const groups = [];
        const occupiedTypes = DATA.TYPE_ORDER.filter(type => roster[type] > 0).length;
        const groupsPerType = Math.max(6, Math.floor(100 / Math.max(1, occupiedTypes)));
        for (const type of DATA.TYPE_ORDER.slice().sort((a, b) => sort[a] - sort[b])) {
          const t = TYPES[type];
          const baseSize = t.groupSize || (t.commander ? 1 : t.siege ? 12 : type === 'musketeer' ? 90 : type === 'elephant' ? 16 : t.mounted ? 80 : 160);
          // Extreme all-artillery or all-elephant sandboxes still use a bounded
          // number of strategic formations; every soldier remains simulated.
          const maxSize = t.groupSize || (t.commander ? 1 : type === 'musketeer' ? 90 : Math.max(baseSize, Math.ceil(roster[type] / Math.min(groupsPerType, layoutBudget))));
          let remaining = roster[type];
          let company = (companyNumbers[type] || 0) + 1;
          while (remaining > 0) {
            const amount = Math.min(remaining, maxSize);
            groups.push({ type, amount, company: company++ });
            remaining -= amount;
          }
        }
        const rowCount = Math.min(11, Math.max(4, Math.ceil(Math.sqrt(groups.length * 2))));
        const columnCount = Math.max(1, Math.ceil(groups.length / rowCount));
        const columnSpacing = Math.min(300, 1950 / Math.max(1, columnCount - 1));
        const halfFrontage = group => {
          const type = TYPES[group.type], pitch = type.siege ? 48 : group.type === 'elephant' ? 56 : type.mounted ? 28 : 17;
          return ((group.type === 'musketeer' ? Math.ceil(group.amount / 3) : Math.ceil(Math.sqrt(group.amount * 1.6))) - 1) * pitch * .5 + type.radius;
        };
        let requiredRowSpacing = 320;
        for (let i = 1; i < groups.length; i++) if (i % rowCount !== 0) {
          requiredRowSpacing = Math.max(requiredRowSpacing, halfFrontage(groups[i - 1]) + halfFrontage(groups[i]) + 4);
        }
        // A full mounted frontage can exceed the old 320-unit grid pitch.
        // Include body radii so adjacent default ranks never begin interlocked.
        // Saved formation positions above remain exact and keep their own fit.
        const rowSpacing = Math.min(requiredRowSpacing, (this.height - 650) / Math.max(1, rowCount - 1));
        const top = this.height / 2 - (rowCount - 1) * rowSpacing / 2;
        const packed = [];
        if (largeDeployment) {
          // Pack actual company footprints into vertical shelves. A fixed
          // 1950-unit army grid interlocked deep companies at the new cap.
          let offset = 0, shelfDepth = 0, cursor = 55;
          const shelfGap = this.width >= 9600 ? 280 : 24;
          for (const group of groups) {
            const t = TYPES[group.type], pitch = t.siege ? 48 : group.type === 'elephant' ? 56 : t.mounted ? 28 : 17;
            const columns = group.type === 'musketeer' ? Math.ceil(group.amount / 3) : Math.ceil(Math.sqrt(group.amount * 1.6));
            const rows = Math.ceil(group.amount / columns);
            const width = (columns - 1) * pitch + t.radius * 2, depth = (rows - 1) * pitch + t.radius * 2;
            if (cursor + width > this.height - 55 && cursor > 55) { offset += shelfDepth + shelfGap; shelfDepth = 0; cursor = 55; }
            packed.push({ x: team === 0 ? this.width * .42 - offset - depth / 2 : this.width * .58 + offset + depth / 2, y: cursor + width / 2 });
            cursor += width + 24; shelfDepth = Math.max(shelfDepth, depth);
          }
        }
        for (let i = 0; i < groups.length; i++) {
          const group = groups[i];
          const col = Math.floor(i / rowCount);
          const row = i % rowCount;
          let x = team === 0 ? this.width * .38 - col * columnSpacing : this.width * .62 + col * columnSpacing;
          let y = top + row * rowSpacing;
          if (largeDeployment) { x = packed[i].x; y = packed[i].y; }
          if (occupancy) {
            const point = this._emptyDeploymentOrigin(team, group.type, group.amount, x, y, occupancy);
            x = point.x; y = point.y;
          }
          this._createRegiment(team, group.type, group.amount, group.company, x, y);
          if (occupancy) for (const id of this.regiments[this.regiments.length - 1].unitIds) {
            const u = this.units[id];
            if (this._deploymentFree(u.x, u.y, u._radius, occupancy)) this._reserveDeploymentBody(u, occupancy);
            else this._repairDeploymentBody(u, occupancy);
          }
        }
      }
      if (!explicit.length && this._blocks.length) {
        // A default army grid can intersect a new river or fortification.
        // Keep every valid authored slot; repair only bodies that actually
        // overlap terrain or another body. Center-only projection can leave
        // a radius straddling a bank even when terrainAt(center) is plain.
        const occupied = { cells: new Map(), maxRadius: Math.max(...Object.values(TYPES).map(t => t.radius)) }, invalid = [];
        for (const u of this.units) {
          if (this._deploymentFree(u.x, u.y, u._radius, occupied)) this._reserveDeploymentBody(u, occupied);
          else invalid.push(u);
        }
        for (const u of invalid) this._repairDeploymentBody(u, occupied);
      }
    }

    _deploymentFree(x, y, radius, occupancy) {
      if (this._bodyBlocked(x, y, radius)) return false;
      const reach = radius + occupancy.maxRadius + 2;
      for (let cy = Math.floor((y - reach) / BODY_CELL); cy <= Math.floor((y + reach) / BODY_CELL); cy++) {
        for (let cx = Math.floor((x - reach) / BODY_CELL); cx <= Math.floor((x + reach) / BODY_CELL); cx++) {
          const bodies = occupancy.cells.get(cy * this._bodyCols + cx);
          if (!bodies) continue;
          for (const body of bodies) if (Math.hypot(x - body.x, y - body.y) < (radius + body._radius) * .96 - 1e-6) return false;
        }
      }
      return true;
    }

    _reserveDeploymentBody(u, occupancy) {
      const key = Math.floor(u.y / BODY_CELL) * this._bodyCols + Math.floor(u.x / BODY_CELL);
      if (!occupancy.cells.has(key)) occupancy.cells.set(key, []);
      occupancy.cells.get(key).push(u);
    }

    _repairDeploymentBody(u, occupancy) {
      const sx = clamp(u.x, u._radius + 9, this.width - u._radius - 9), sy = clamp(u.y, u._radius + 9, this.height - u._radius - 9);
      let point = null;
      const pitch = Math.max(16, u._radius * 2 + 3);
      // Deployment occupancy only grows. For an identical origin and radius,
      // failed candidates stay occupied; a previously chosen point is now
      // occupied too. Resume that exact scan instead of probing its prefix
      // again for every body compressed against the same crowded border.
      if(!occupancy.repairCursors)occupancy.repairCursors=new Map();
      const key=u._radius+':'+sx+':'+sy,cursor=occupancy.repairCursors.get(key)||{ring:0,sample:0};
      let nextRing=73,nextSample=0;
      for (let ring = cursor.ring; ring <= 72 && !point; ring++) {
        const samples = Math.max(1, ring * 8);
        for (let sample = ring===cursor.ring?cursor.sample:0; sample < samples; sample++) {
          const angle = sample / samples * TAU, x = sx + Math.cos(angle) * ring * pitch, y = sy + Math.sin(angle) * ring * pitch;
          if (this._deploymentFree(x, y, u._radius, occupancy)) { point = { x, y };nextRing=sample+1<samples?ring:ring+1;nextSample=sample+1<samples?sample+1:0;break; }
        }
      }
      // One small record per repeated origin is useful; pathological imports
      // cannot retain more than this bounded, constructor-local cache.
      if(occupancy.repairCursors.has(key)||occupancy.repairCursors.size<8192)occupancy.repairCursors.set(key,{ring:nextRing,sample:nextSample});
      if (point) { u.x = point.x; u.y = point.y; }
      const r = this.regiments[u.regiment];
      u._orderOffsetX = u.x - r.x; u._orderOffsetY = u.y - r.y;
      u._localGoalX = u.x; u._localGoalY = u.y; u._terrain = this.terrainAt(u.x, u.y);
      this._reserveDeploymentBody(u, occupancy);
    }

    _emptyDeploymentOrigin(team, type, amount, x, y, occupancy) {
      const t = TYPES[type], spacing = t.siege ? 48 : type === 'elephant' ? 56 : t.mounted ? 28 : 17;
      const columns = type === 'musketeer' ? Math.ceil(amount / 3) : Math.ceil(Math.sqrt(amount * 1.6)), rows = Math.ceil(amount / columns), direction = team === 0 ? 1 : -1;
      const clear = (cx, cy) => {
        for (let slot = 0; slot < amount; slot++) {
          const px = cx + ((rows - 1) / 2 - Math.floor(slot / columns)) * spacing * direction;
          const py = cy + (slot % columns - (columns - 1) / 2) * spacing * direction;
          if (!this._deploymentFree(px, py, t.radius, occupancy)) return false;
        }
        return true;
      };
      if (clear(x, y)) return { x, y };
      for (let ring = 1; ring <= 64; ring++) {
        const samples = ring * 8;
        for (let sample = 0; sample < samples; sample++) {
          const angle = sample / samples * TAU, px = x + Math.cos(angle) * ring * 80, py = y + Math.sin(angle) * ring * 80;
          if (clear(px, py)) return { x: px, y: py };
        }
      }
      return { x, y };
    }

    _createRegiment(team, type, amount, number, x, y, facing) {
      const t = TYPES[type];
      const spacing = t.siege ? 48 : type === 'elephant' ? 56 : t.mounted ? 28 : 17;
      const columns = type === 'musketeer' ? Math.ceil(amount / 3) : Math.ceil(Math.sqrt(amount * 1.6));
      const rows = Math.ceil(amount / columns);
      const origin = this._nearestPassable(x, y);
      facing = Number.isFinite(facing) ? facing : team === 0 ? 0 : Math.PI;
      const facingX = Math.cos(facing), facingY = Math.sin(facing);
      const r = {
        id: this.regiments.length, team, type,
        name: t.shortName + ' ' + number,
        unitIds: [], initial: amount, alive: amount, morale: t.morale,
        x: origin.x, y: origin.y, order: 'auto',
        targetX: origin.x, targetY: origin.y,
        width: columns * spacing, depth: rows * spacing,
        facing, kills: 0,
        formation: type === 'musketeer' ? 'volley' : 'line', cohesion: 100, status: 'steady', fallingBack: false,
        rallyRadius: t.commander ? (t.rallyRadius || 620) : 0, rallyCooldown: 0,
        regroupWith: '', supportRegimentId: -1,
        maneuver: 'formed', effectiveWidth: columns * spacing, effectiveDepth: rows * spacing,
        deformation: 0, trafficPressure: 0, footprintFacing: facing,
        tacticalRole: t.commander ? 'command' : t.ranged ? 'support' : 'advance', engagedSoldiers: 0, bracedSoldiers: 0, pushingSoldiers: 0,
        commandStatus: 'automatic', commandTargetId: -1, commandTargetName: '', commandTurnRemaining: 0,
        commandX: origin.x, commandY: origin.y, commandFacing: facing,
        frontX: origin.x, frontY: origin.y, frontageWidth: columns * spacing,
        _spacing: spacing, _columns: columns, _rows: rows,
        _baseX: origin.x, _baseY: origin.y,
        _holdX: origin.x, _holdY: origin.y,
        _goalX: origin.x, _goalY: origin.y,
        _path: [], _pathTimer: 0, _enemyId: -1,
        _enemyDistance: Infinity, _recentHits: 0,
        _routed: false, _flankSign: this.regiments.length % 2 ? 1 : -1,
        _bridge: null, _hasDestination: false,
        _blockedMoves: 0, _moveAttempts: 0, _flankHits: 0,
        _fallbackTime: 0, _fallbackCount: 0, _unsupportedTime: 0,
        _rallyTime: 0, _manualFallback: false, _collisionSlow: 1,
        _adaptiveColumns: columns, _terrainColumns: columns, _shapeColumns: columns,
        _terrainOffset: 0, _adaptiveOffset: 0, _forestCompression: 1, _marchSqueeze: 1,
        _travelAngle: facing, _shapeSpacing: spacing,
        _maneuverUntil: 0, _terrainPressure: 0, _terrainContacts: 0,
        _traffic: null, _allyContacts: 0, _enemyContacts: 0,
        _actualMinX: origin.x, _actualMaxX: origin.x, _actualMinY: origin.y, _actualMaxY: origin.y,
        _centroidX: origin.x, _centroidY: origin.y, _movementScale: 1, _steadyBraceScale: 0,
        _formationPressureSpeed: 0, _formationPressureNX: 0, _formationPressureNY: 0, _formationPressureUntil: 0, _pressureSourceId: -1,
        _arrivalFraction: 0, _arrivalDistance: Infinity,
        _frontNX: team === 0 ? 1 : -1, _frontNY: 0, _frontTX: 0, _frontTY: team === 0 ? 1 : -1,
        _combatActive: false, _combatColumns: columns, _combatRows: rows, _combatSpacing: spacing,
        _combatAssignedCount: 0, _combatSlotsTime: -10, _combatEnemy: -1, _laneAdvance: null,
        _assignmentUntil: 0, _nearestEnemyDistance: Infinity, _contactDistance: Infinity, _routeDetour: false,
        _chargeTargetId: -1, _chargeNX: team === 0 ? 1 : -1, _chargeNY: 0,
        _mountedStream: null, _localRiderPass: null, _tightMountedMarch: false,
        _commandTime: -1000, _commandEpoch: 0, _frontTurning: false, _commandBlocked: false, _travelSlots: false, _approachBlendUntil: 0,
        _travelSlotColumns: columns, _travelSlotCount: amount,
        _attackBlocked: false, _retreatTurnUntil: 0, _withdrawLeg: false,
        _retreatNX: team === 0 ? -1 : 1, _retreatNY: 0,
        _withdrawStartX: origin.x, _withdrawStartY: origin.y,
        _withdrawGoalX: origin.x, _withdrawGoalY: origin.y, flexingSoldiers: 0,
        _missileActive: false, _missileColumns: columns, _missileRows: rows, _missileSpacing: spacing,
        _missileSlotsTime: -10, _missileEnemy: -1, _missileAssignedCount: 0,
        _fireLocked: false, _fireEnemy: -1, _fireAnchorX: origin.x, _fireAnchorY: origin.y, _fireLastInRange: -1000,
        _marchLayout: false, _marchAngle: facing, _marchRadius: 1, _marchLag: 0, _marchTurnUntil: 0,
        _marchActive: false, _deployCombat: false, _deployMissiles: false, _sharedChargeLane: false,
        _marchNX: facingX, _marchNY: facingY, _slotAngle: facing, _slotNX: facingX, _slotNY: facingY, _speedFactor: 1,
        _tactic: null, _tacticalMovement: false, _tacticalHold: false, _frontBounds: null, _travelSlotAngle: facing,
        _missileAcrossLo: 0, _missileAcrossHi: 0, _missileEnemyFront: 0, _missileGap: 0, _missileRowSpacing: spacing,
        _approachNX: facingX, _approachNY: facingY, _approachEnemy: -1, _localFront: false,
        _faceStart: 0, _faceDuration: 0, _faceFrom: facing, _faceDelta: 0, _faceError: 0,
        fireBehavior: t.missile ? t.missile.label : '',
        nextVolleyRank: 1, nextVolleyAt: 0, volleyRankCount: type === 'musketeer' ? rows : 0,
        volleyStatus: type === 'musketeer' ? 'forming' : '', lastVolleyAt: -1000, lastVolleyRank: 0,
        _volleyNext: 0, _volleyBrokenUntil: 0, _volleyDispatch: false, _volleyLayout: '',
        _siegeJob: null, siegeStatus: '', _siegeManualTarget: null, _siegeManualEpoch: -1, _siegeReachRevision: -1,
        _lancerPass: null,
        _rigidMinX: Infinity, _rigidMaxX: -Infinity, _rigidMinY: Infinity, _rigidMaxY: -Infinity,
        _rigidCenterX: 0, _rigidCenterY: 0, _rigidAngleDelta: 0,
        _rigidCertifiedEpoch: -1, _rigidContinuityTick: -1, _rigidCount: 0, _rigidCheckAfter: 0
        ,_initialRampart:null,_rampartJob:null,_rampartSlots:[],rampartRanks:2,rampartSpacing:'normal',rampartStatus:''
      };
      this.regiments.push(r);
      for (let slot = 0; slot < amount; slot++) {
        const row = Math.floor(slot / columns);
        const col = slot % columns;
        const ox = ((rows - 1) / 2 - row) * spacing;
        const oy = (col - (columns - 1) / 2) * spacing;
        const position = this._nearestPassable(origin.x + ox * facingX - oy * facingY, origin.y + ox * facingY + oy * facingX);
        const u = {
          id: this.units.length, team, type, regiment: r.id,
          x: position.x, y: position.y, hp: t.hp, maxHp: t.hp,
          alive: true, routed: false, angle: r.facing,
          state: 'idle', anim: this._random() * 8,
          attackFlash: 0, target: -1,
          _slot: slot, _cooldown: type === 'musketeer' ? 0 : this._random() * t.cooldown,
          _volleyRank: type === 'musketeer' ? row : -1, _meleeCooldown: 0, _lastMusketMelee: -1000,
          _radius: t.radius, _contact: false,
          travelDistance: 0,
          _sense: this._random() * .65, _charge: 0,
          _contactSense: 0, _flex: 0, _turnStart: 0, _turnEnd: 0, _turnFrom: 0, _turnDelta: 0,
          _vx: 0, _vy: 0, _stuck: 0, _detour: null,
          _localPath: [], _localTimer: this._random() * .7, _localGoalX: position.x, _localGoalY: position.y,
          _trafficSlot: -1,
          _combatSlot: -1, _missileSlot: -1, _lastAttack: -1000, _jamTime: 0, _idleContactTime: 0, _circulateUntil: 0, _circulateNext: 0, _circulateX: position.x, _circulateY: position.y, _pressUntil: 0, _pressColumn: -1,
          _yieldX: position.x, _yieldY: position.y, _yieldUntil: 0,
          _terrain: this.terrainAt(position.x, position.y), _terrainTimer: this._random() * .5,
          _jitter: this._random(), _deathTime: -1,
          // Every soldier has the same field layout before marching, contact,
          // or a tactical phase begins. Late property additions to only one
          // wing made the shared movement loop repeatedly change object shape.
          _moveSlot: -1, _orderOffsetX: 0, _orderOffsetY: 0,
          _marchOX: 0, _marchOY: 0, _marchTurnStart: 0, _marchTurnEnd: 0, _marchTurnFrom: facing, _marchTurnDelta: 0,
          _moveIntentNX: facingX, _moveIntentNY: facingY, _moveIntentTime: -1000,
          _holdPosition: null, _portalCorner: null, _portalGoal: null, _portalUntil: 0,
          _faceFromAngle: facing, _faceHeadDelta: 0, _faceOX: 0, _faceOY: 0,
          _fireReachTime: -1000, _fireReachEnemy: -1, _passageNext: 0, _passageUntil: 0, _passageX: 0, _passageY: 0,
          _tacticHoldX: 0, _tacticHoldY: 0, _tacticYielding: false, _withdrawOffsetX: 0, _withdrawOffsetY: 0,
          _mountSpeed: 0, _mountDriveSpeed: 0, _mountNX: facingX, _mountNY: facingY,
          _mountDriveNX: facingX, _mountDriveNY: facingY, _mountMoveTick: -1,
          _plungeUntil: 0, _plungeNX: facingX, _plungeNY: facingY, _braceImpactUntil: 0,
          _nextImpactAt: 0, _lastImpact: -1000, _lastImpactTarget: -1, _lastImpactDamage: 0, _impactCount: 0,
          _impactStart: -1000, _impactDuration: 0, _impactUntil: 0, _impactLift: 0,
          _impactNX: 0, _impactNY: 0, _impactSpeed: 0, _impactSource: -1,
          _motionTick: -1, _motionDistance: 0, _pressureTick: -1, _pressureDX: 0, _pressureDY: 0,
          _friendlyOffsetX: 0, _friendlyOffsetY: 0, _friendlyUntil: 0,
          _friendlySource: -1, _friendlySourceEpoch: -1, _friendlyEpoch: -1,
          _friendlyTick: -1, _friendlyDistance: 0, _friendlyDX: 0, _friendlyDY: 0,
          _friendlyFlowTick: -1, _friendlyCarryTick: -1,
          _braceTime: 0, _chargeCheckedUntil: 0,
          _pushSource: -1, _pushSpeed: 0, _pushForce: 0, _pushEpoch: -1, _pushUntil: 0, _pushDepth: 0,
          _pushNX: 0, _pushNY: 0, _lastPush: -1000,
          _siegeStage: '', _siegeTarget: null, _siegeProgress: 0, _siegeDeployed: false,
          _siegeRamp: null, _siegeEnemy: -1, _siegeUndocking: false, _siegeUndockCheck: 0, _siegeOccupants: null,
          _bulkStepTick: -1,
          _onRampart:false,_rampartId:'',_rampartZ:0,_rampartClimb:0,_rampartStage:'',_rampartAccess:null,
          _rampartGroundPath:[],_rampartPath:[],_rampartSlot:-1,_rampartRank:0,_rampartCourtGoal:null,_rampartFallTarget:null,_rampartFallDamage:0
        };
        r.unitIds.push(u.id);
        this.units.push(u);
      }
      this._stats[team].initial += amount;
      this._stats[team].alive += amount;
    }

    start() {
      if (this.phase !== 'deployment') return false;
      this._rebuildHash();
      for (const r of this.regiments) if (r.alive > 0) this._captureMarchLayout(r);
      this.phase = 'running';
      this._event('The advance is sounded. The armies march.', null, 'start');
      this._strategize();
      this._checkVictory();
      return true;
    }

    pause() {
      if (this.phase === 'running') this.phase = 'paused';
    }

    resume() {
      if (this.phase === 'paused') this.phase = 'running';
    }

    getRegiment(id) {
      return this.regiments[Number(id)] || null;
    }

    setFormation(ids, formation) {
      if (!FORMATIONS[formation] || this.phase === 'ended') return 0;
      let changed = 0;
      for (const id of new Set(ids || [])) {
        const r = this.getRegiment(id);
        if (!r || r.alive <= 0 || TYPES[r.type].commander) continue;
        if (this.impacts) { this.impacts.cancelPressure(r); this.impacts.cancelFriendly(r); }
        for (let i = this._trafficPlans.length - 1; i >= 0; i--) {
          const plan = this._trafficPlans[i];
          if (plan.a === r.id || plan.b === r.id) this._clearTraffic(plan);
        }
        r._marchLayout = false;
        r._approachBlendUntil = 0;
        if (r.order === 'hold') for (const uid of r.unitIds) this.units[uid]._holdPosition = null;
        if (r.formation !== formation && this.phase !== 'deployment') r.cohesion = Math.max(30, r.cohesion - 15);
        r.formation = formation;
        r._pathTimer = 0;
        const f = FORMATIONS[formation] || {};
        const columns = f.files ? Math.min(r.alive, f.files) : f.ranks ? Math.ceil(r.alive / f.ranks) : r.type === 'musketeer' && formation !== 'wedge' ? Math.ceil(r.alive / 3) : formation === 'shieldwall' ? Math.ceil(r._columns * 1.25) : formation === 'wedge' ? Math.ceil((Math.sqrt(8 * r.alive + 1) - 1) / 2) : r._columns;
        const rows = formation === 'wedge' ? columns : Math.ceil(r.alive / columns);
        r.width = columns * r._spacing * (f.spacing || 1);
        r.depth = rows * r._spacing * (f.spacing || 1);
        r._shapeColumns = columns;
        if (r.type === 'musketeer') { r._volleyLayout = ''; r._missileSlotsTime = -10; r._volleyNext = 0; }
        if (this.phase === 'deployment') {
          r._adaptiveColumns = columns; r._terrainColumns = columns;
          r.effectiveWidth = r.width; r.effectiveDepth = r.depth;
        }
        if (this.phase === 'deployment') {
          for (const uid of r.unitIds) {
            const u = this.units[uid];
            const goal = this._formationGoal(u, r), position = this._nearestPassable(goal.x, goal.y);
            u.x = position.x; u.y = position.y; u._terrain = this.terrainAt(u.x, u.y);
          }
        }
        if (r.order === 'hold') {
          r._travelSlots = false;
          for (const uid of r.unitIds) {
            const u = this.units[uid];
            if (u.alive && !u.routed) u._holdPosition = this.phase === 'deployment' ? { x: u.x, y: u.y } : this._formationGoal(u, r);
          }
        }
        if (r.type === 'musketeer') this._assignMusketRanks(r, false);
        changed++;
      }
      if (changed) this._strategyTimer = 0;
      if (changed && this.phase === 'deployment' && this._next) this._rebuildHash();
      return changed;
    }

    captureFormations() {
      return this.regiments.filter(r => r.unitIds.length > 0).map(r => ({ team: r.team, type: r.type, name: r.name, count: r.unitIds.length, x: r.x, y: r.y, formation: r.formation, facing: r.facing,
        ...(r._rampartJob&&!r._rampartJob.leaving?{rampart:{structureId:r._rampartJob.structureId,ranks:r.rampartRanks,spacing:r.rampartSpacing}}:{}),
        offsets: r.unitIds.map(id => ({ x: this.units[id].x - r.x, y: this.units[id].y - r.y })) }));
    }

    _captureMarchLayout(r) {
      const center = this._currentCenter(r), nx = Math.cos(r.facing), ny = Math.sin(r.facing);
      const mounted = r.type === 'cavalry' || r.type === 'lancer' || r.type === 'horseArcher';
      let count = 0, minOX = Infinity, maxOX = -Infinity, minOY = Infinity, maxOY = -Infinity;
      r.x = r._centroidX = center.x; r.y = r._centroidY = center.y;
      r._marchAngle = r.facing; r._marchLayout = true; r._marchRadius = 1; r._marchLag = 0; r._marchSqueeze = 1;
      r._travelSlots = true;
      r._actualMinX = r._actualMinY = Infinity; r._actualMaxX = r._actualMaxY = -Infinity;
      for (const id of r.unitIds) {
        const u = this.units[id];
        if (!u.alive || u.routed) continue;
        const dx = u.x - center.x, dy = u.y - center.y;
        u._marchOX = dx * nx + dy * ny; u._marchOY = -dx * ny + dy * nx;
        if (mounted) {
          count++; minOX = Math.min(minOX, u._marchOX); maxOX = Math.max(maxOX, u._marchOX);
          minOY = Math.min(minOY, u._marchOY); maxOY = Math.max(maxOY, u._marchOY);
        }
        u._orderOffsetX = dx; u._orderOffsetY = dy; u._moveSlot = -1;
        // This new layout already includes the real courtesy sidestep.
        // Reapplying its temporary offset would widen the ranks twice.
        u._friendlyOffsetX = u._friendlyOffsetY = 0; u._friendlyUntil = 0;
        u._friendlySource = u._friendlySourceEpoch = u._friendlyEpoch = -1;
        r._marchRadius = Math.max(r._marchRadius, Math.hypot(dx, dy));
        r._actualMinX = Math.min(r._actualMinX, u.x); r._actualMaxX = Math.max(r._actualMaxX, u.x);
        r._actualMinY = Math.min(r._actualMinY, u.y); r._actualMaxY = Math.max(r._actualMaxY, u.y);
      }
      const diameter = TYPES[r.type].radius * 2;
      r._tightMountedMarch = mounted && count > 1 &&
        count * diameter * diameter > (maxOX - minOX + diameter) * (maxOY - minOY + diameter) * .8;
    }

    _translatePressureAnchor(r, dx, dy) {
      // A yielding line keeps its facing and its own footprint. Translate
      // only the anchors belonging to that footprint by the bodies' actual
      // mean displacement; player destinations and future routes stay put.
      r.x += dx; r.y += dy;
      r._centroidX += dx; r._centroidY += dy;
      r._baseX += dx; r._baseY += dy;
      r._holdX += dx; r._holdY += dy;
      r.frontX += dx; r.frontY += dy;
      r._fireAnchorX += dx; r._fireAnchorY += dy;
      if (r.order === 'hold' || (r.order === 'auto' && r._tactic && r._tactic.hold)) {
        r._goalX += dx; r._goalY += dy;
        if (r._tactic && r._tactic.hold) { r._tactic.goalX += dx; r._tactic.goalY += dy; }
      }
      r._frontBounds = null;
    }

    faceRegiments(ids, x, y) {
      if (this.phase === 'ended' || !Number.isFinite(x) || !Number.isFinite(y)) return 0;
      const groups = Array.from(new Set(Array.isArray(ids) ? ids : [])).filter(Number.isInteger).map(id => this.getRegiment(id)).filter(r => r && r.alive > 0 && !r._routed && !r.fallingBack && r._rallyTime <= 0);
      const plans = groups.map(r => {
        const center = this._currentCenter(r), distance = Math.hypot(x - center.x, y - center.y);
        if (distance < 2) return null;
        const angle = Math.atan2(y - center.y, x - center.x), delta = Math.atan2(Math.sin(angle - r.facing), Math.cos(angle - r.facing));
        const nx = Math.cos(delta), ny = Math.sin(delta);
        const positions = r.unitIds.map(id => this.units[id]).filter(u => u.alive && !u.routed).map(u => ({ u, x: center.x + (u.x - center.x) * nx - (u.y - center.y) * ny, y: center.y + (u.x - center.x) * ny + (u.y - center.y) * nx, _radius: u._radius }));
        return { r, center, angle, delta, positions };
      }).filter(Boolean);
      if (!plans.length) return 0;
      if (this.phase === 'deployment') {
        // A deployment rotation is a single rigid edit. Validate the whole
        // selected result before touching any body or command state.
        const selected = new Set(plans.map(p => p.r.id));
        const occupancy = { cells: new Map(), maxRadius: this._maxRadius };
        for (const u of this.units) if (u.alive && !u.routed && !selected.has(u.regiment)) this._reserveDeploymentBody(u, occupancy);
        for (const p of plans) for (const position of p.positions) {
          if (!this._deploymentFree(position.x, position.y, position._radius, occupancy)) return 0;
          this._reserveDeploymentBody(position, occupancy);
        }
      }
      for (const p of plans) {
        const r = p.r, from = r.facing;
        this._resetCommandIntent(r);
        r.x = r._holdX = r._centroidX = p.center.x; r.y = r._holdY = r._centroidY = p.center.y;
        r._goalX = r.targetX = r.x; r._goalY = r.targetY = r.y;
        r.commandFacing = p.angle; r._commandTime = this.time;
        r._path = []; r._pathTimer = 0; r._hasDestination = false;
        r._faceFrom = from; r._faceDelta = p.delta; r._faceStart = this.time;
        let radius = 1;
        for (const position of p.positions) {
          const u = position.u;
          u._faceOX = u.x - r.x; u._faceOY = u.y - r.y;
          u._faceFromAngle = u.angle; u._faceHeadDelta = Math.atan2(Math.sin(p.angle - u.angle), Math.cos(p.angle - u.angle));
          radius = Math.max(radius, Math.hypot(u._faceOX, u._faceOY));
          if (this.phase === 'deployment') { u.x = position.x; u.y = position.y; u.angle = p.angle; u._terrain = this.terrainAt(u.x, u.y); }
        }
        if (this.phase === 'deployment') {
          r.facing = r._travelAngle = p.angle; r.order = 'auto'; this._captureMarchLayout(r);
        } else {
          r.order = 'face'; r._faceDuration = .35 + Math.max(.25, Math.abs(p.delta) * radius / (TYPES[r.type].speed * .62));
          r._faceError = Infinity; r._marchLayout = false;
        }
        this._commandFeedback(r);
      }
      if (this.phase === 'deployment') this._rebuildHash();
      this._strategyTimer = 0;
      this._event((plans.length === 1 ? plans[0].r.name : plans.length + ' formations') + ' turns to face the marked direction.', plans[0].r.team, 'order');
      return plans.length;
    }

    splitRegiments(ids) {
      if (this.phase === 'ended') return [];
      const created = [];
      for (const id of new Set(Array.isArray(ids) ? ids : [])) {
        if (!Number.isInteger(id) || id < 0) continue;
        const r = this.getRegiment(id);
        if (!r || r._routed || r.alive < 2 || TYPES[r.type].commander || this.regiments.length >= 512) continue;
        const living = r.unitIds.map(uid => this.units[uid]).filter(u => u.alive && !u.routed);
        if (living.length < 2) continue;
        if (this.impacts) { this.impacts.cancelPressure(r); this.impacts.cancelFriendly(r); }
        if (r._traffic) this._clearTraffic(r._traffic.plan);
        const tx = -Math.sin(r.facing), ty = Math.cos(r.facing);
        living.sort((a, b) => (a.x - b.x) * tx + (a.y - b.y) * ty || a.id - b.id);
        const count = Math.floor(living.length / 2), detached = living.slice(living.length - count);
        const moved = new Set(detached.map(u => u.id));
        const inactive = r.unitIds.filter(uid => !this.units[uid].alive || this.units[uid].routed);
        for (const uid of inactive.slice(0, Math.round(inactive.length * count / living.length))) moved.add(uid);
        const all = r.unitIds.slice(), oldCenter = this._currentCenter(r), oldKills = r.kills;
        const child = Object.assign({}, r, { id: this.regiments.length, name: TYPES[r.type].shortName + ' ' + (this.regiments.filter(other => other.team === r.team && other.type === r.type).length + 1) });
        r.unitIds = all.filter(uid => !moved.has(uid));
        child.unitIds = all.filter(uid => moved.has(uid));
        child.kills = Math.round(oldKills * child.unitIds.length / all.length); r.kills = oldKills - child.kills;
        for (const key of ['_recentHits', '_flankHits', '_allyContacts', '_enemyContacts', '_blockedMoves', '_moveAttempts']) {
          child[key] = r[key] * child.unitIds.length / all.length; r[key] -= child[key];
        }
        this.regiments.push(child);
        for (const part of [r, child]) {
          part.initial = part.unitIds.length;
          part.alive = part.unitIds.reduce((sum, uid) => sum + Number(this.units[uid].alive && !this.units[uid].routed), 0);
          for (const uid of part.unitIds) this.units[uid].regiment = part.id;
          const center = this._currentCenter(part), dx = center.x - oldCenter.x, dy = center.y - oldCenter.y;
          part.x = part._centroidX = center.x; part.y = part._centroidY = center.y;
          part._baseX += dx; part._baseY += dy; part._holdX += dx; part._holdY += dy;
          if (part._tactic) {
            // Each half keeps the same job with its own nearby route. Sharing
            // one mutable phase object lets either half turn both regiments.
            part._tactic = Object.assign({}, part._tactic, {
              goalX: part._tactic.goalX + dx, goalY: part._tactic.goalY + dy,
              waypoints: part._tactic.waypoints.map(p => ({ x: p.x + dx, y: p.y + dy }))
            });
            if (part._tactic.targetIds) part._tactic.targetIds = part._tactic.targetIds.slice();
          }
          if (part.order === 'move' || part.order === 'attack') { part.targetX += dx; part.targetY += dy; }
          part._columns = Math.max(1, Math.ceil(Math.sqrt(part.alive * 1.6))); part._rows = Math.ceil(part.alive / part._columns);
          const factor = (FORMATIONS[part.formation] || {}).spacing || 1;
          const columns = part.formation === 'shieldwall' ? Math.ceil(part._columns * 1.25) : part.formation === 'wedge' ? Math.ceil((Math.sqrt(8 * part.alive + 1) - 1) / 2) : part._columns;
          const rows = part.formation === 'wedge' ? columns : Math.ceil(part.alive / columns);
          part.width = columns * part._spacing * factor; part.depth = rows * part._spacing * factor;
          part._adaptiveColumns = part._terrainColumns = part._shapeColumns = columns;
          part.effectiveWidth = part.width; part.effectiveDepth = part.depth;
          part._path = []; part._pathTimer = 0; part._traffic = null; part._laneAdvance = null;
          part._combatActive = false; part._combatColumns = columns; part._combatRows = rows; part._combatAssignedCount = 0; part._combatSlotsTime = -10;
          part._missileActive = false; part._missileSlotsTime = -10; part._missileEnemy = -1; part._fireLocked = false;
          part.engagedSoldiers = part.unitIds.reduce((sum, uid) => sum + Number(this.units[uid].alive && !this.units[uid].routed && this.units[uid]._lastAttack >= this.time - 1.5), 0);
          part._travelSlots = part.order !== 'hold'; part._approachBlendUntil = this.time + 2;
          part._frontBounds = null;
          part._actualMinX = part._actualMinY = Infinity; part._actualMaxX = part._actualMaxY = -Infinity;
          let slot = 0;
          for (const uid of part.unitIds) {
            const u = this.units[uid];
            if (!u.alive || u.routed) continue;
            u._slot = slot++; u._moveSlot = -1; u._combatSlot = -1; u._missileSlot = -1;
            u._orderOffsetX = u.x - center.x; u._orderOffsetY = u.y - center.y;
            u._localPath = []; u._localTimer = 0; u._portalCorner = null; u._trafficSlot = -1;
            part._actualMinX = Math.min(part._actualMinX, u.x); part._actualMaxX = Math.max(part._actualMaxX, u.x);
            part._actualMinY = Math.min(part._actualMinY, u.y); part._actualMaxY = Math.max(part._actualMaxY, u.y);
          }
          part._travelSlotColumns = columns; part._travelSlotCount = part.alive;
          if (part.order === 'auto' || part.order === 'move' || part.order === 'attack' || part.order === 'charge') this._captureMarchLayout(part);
          if (part.order === 'face') {
            part._holdX = part.x; part._holdY = part.y;
            part._faceFrom = part.facing; part._faceDelta = Math.atan2(Math.sin(part.commandFacing - part.facing), Math.cos(part.commandFacing - part.facing));
            part._faceStart = this.time; part._faceError = Infinity;
            let radius = 1;
            for (const uid of part.unitIds) {
              const u = this.units[uid];
              if (!u.alive || u.routed) continue;
              u._faceOX = u.x - part.x; u._faceOY = u.y - part.y;
              u._faceFromAngle = u.angle; u._faceHeadDelta = Math.atan2(Math.sin(part.commandFacing - u.angle), Math.cos(part.commandFacing - u.angle));
              radius = Math.max(radius, Math.hypot(u._faceOX, u._faceOY));
            }
            part._faceDuration = .35 + Math.max(.25, Math.abs(part._faceDelta) * radius / (TYPES[part.type].speed * .62));
          }
          this._commandFeedback(part);
        }
        if (this._tactics && this._tactics.onSplit) this._tactics.onSplit(r, child, oldCenter);
        created.push(child.id);
        this._event(r.name + ' divides into two independently commanded formations.', r.team, 'split');
      }
      if (created.length) { this._strategyTimer = 0; this._rebuildHash(); this._refreshStats(); }
      return created;
    }

    _formationFactor(r, key) {
      const f = FORMATIONS[r.formation] || {};
      const value = Number.isFinite(f[key]) ? f[key] : 1;
      return 1 + (value - 1) * clamp(r.cohesion / 100, 0, 1);
    }

    _braceReadiness(u) {
      const t = TYPES[u.type], r = this.regiments[u.regiment], brace = t.brace;
      if (!brace || r._frontTurning || r.fallingBack || r._rallyTime > 0 || r.order === 'move' || r.order === 'charge' || r.order === 'face' || this._advancingAttack(r) ||
          (r.formation !== 'line' && r.formation !== 'shieldwall')) return 0;
      const moving = u._vx * u._vx + u._vy * u._vy >= t.speed * t.speed * .0324;
      // Deliberately advancing ranks can retain a partial brace. Actual
      // stationary pikes still have their full planted advantage; a sprint,
      // turn or sideways shuffle has no walking protection to preserve.
      const scale = moving ? this._steadyMotion(u, r) ? r._steadyBraceScale : 0 : 1;
      return scale * clamp(u._braceTime / brace.settle, 0, 1) * clamp((r.cohesion - brace.minCohesion) / (85 - brace.minCohesion), 0, 1) *
        (r._terrainPressure > .2 ? .6 : 1);
    }

    _steadyMotion(u, r) {
      if (!r._steadyBraceScale || r.order !== 'auto' || !r._tactic || !r._tactic.movingBrace || r.fallingBack || r._rallyTime > 0 || u._impactUntil > this.time) return false;
      const nx = r._combatActive ? r._slotNX : r._marchNX, ny = r._combatActive ? r._slotNY : r._marchNY;
      const forward = u._vx * nx + u._vy * ny;
      const side = u._vy * nx - u._vx * ny;
      const maximum = TYPES[u.type].speed * .74;
      return forward > .5 && Math.abs(side) < Math.max(1.5, forward * .12) && u._vx * u._vx + u._vy * u._vy <= maximum * maximum;
    }

    _braceStrength(u, opponent) {
      const ready = this._braceReadiness(u);
      if (!ready || !opponent) return 0;
      const r = this.regiments[u.regiment], dx = opponent.x - u.x, dy = opponent.y - u.y, distance = Math.max(1, Math.hypot(dx, dy));
      // A head turning to swing at a flanker does not rotate the coherent
      // formation or instantly plant its pikes in a new direction.
      const alignment = (dx * Math.cos(r.facing) + dy * Math.sin(r.facing)) / distance;
      return ready * clamp((alignment - .35) / .45, 0, 1);
    }

    _steadyGuardStrength(u, opponent) {
      const r = this.regiments[u.regiment];
      const stance = r.order === 'auto' && r._tactic && r._tactic.movingBrace && r._steadyBraceScale > 0 && !r.fallingBack && r._rallyTime <= 0 && u._impactUntil <= this.time;
      if (!opponent || !stance || (u._vx * u._vx + u._vy * u._vy > 1 && !this._steadyMotion(u, r))) return 0;
      const dx = opponent.x - u.x, dy = opponent.y - u.y, distance = Math.max(1, length2(dx, dy));
      const alignment = (dx * Math.cos(r.facing) + dy * Math.sin(r.facing)) / distance;
      // This guard is resistance, never a synthetic pike damage/retaliation
      // bonus. Mounted impacts may use it for ordinary formed infantry too.
      return r._steadyBraceScale * clamp((r.cohesion - 45) / 40, 0, 1) * clamp((alignment - .35) / .45, 0, 1);
    }

    _canShieldPress(u) {
      const t = TYPES[u.type], r = this.regiments[u.regiment];
      return !!t.push && u.alive && !u.routed && !r._frontTurning && !r.fallingBack && r._rallyTime <= 0 && r.order !== 'move' && r.order !== 'face' &&
        !this._advancingAttack(r) && (r.formation === 'line' || r.formation === 'shieldwall') && r.cohesion >= t.push.minCohesion && r._terrainPressure < .2;
    }

    _shieldSupport(u, nx, ny) {
      const r = this.regiments[u.regiment], reach = r._spacing * 2.6;
      const minX = clamp(Math.floor((u.x - reach) / BODY_CELL), 0, this._bodyCols - 1), maxX = clamp(Math.floor((u.x + reach) / BODY_CELL), 0, this._bodyCols - 1);
      const minY = clamp(Math.floor((u.y - reach) / BODY_CELL), 0, this._bodyRows - 1), maxY = clamp(Math.floor((u.y + reach) / BODY_CELL), 0, this._bodyRows - 1);
      let support = 0;
      for (let cy = minY; cy <= maxY; cy++) for (let cx = minX; cx <= maxX; cx++) {
        for (let id = this._bodyHeads[u.team][cy * this._bodyCols + cx]; id !== -1; id = this._bodyNext[id]) {
          const other = this.units[id];
          if (id === u.id || !other.alive || other.routed || other.regiment !== u.regiment) continue;
          const dx = other.x - u.x, dy = other.y - u.y, behind = -(dx * nx + dy * ny);
          if (behind > 5 && behind < reach && Math.abs(-dx * ny + dy * nx) < r._spacing * 1.3 && ++support >= 3) return support;
        }
      }
      return support;
    }

    _shieldResistance(target, source) {
      const r = this.regiments[target.regiment];
      const planted = (r.formation === 'line' || r.formation === 'shieldwall') && !r.fallingBack ? clamp(r.cohesion / 100, 0, 1) : 0;
      return TYPES[target.type].mass * (1 + planted * (r.formation === 'shieldwall' ? .5 : .25) + this._braceStrength(target, source) * .6);
    }

    _queueShieldPush(target, source, nx, ny, force, depth) {
      if (!target.alive || target.routed) return;
      const resistance = this._shieldResistance(target, source);
      if (force <= resistance) return;
      const speed = Math.min(TYPES[source.type].push.speed * (1 - resistance / force), TYPES[target.type].speed * .20);
      if (speed < .35) return;
      const previous = this.units[target._pushSource];
      const active = previous && this._canShieldPress(previous) && target._pushEpoch === this.regiments[previous.regiment]._commandEpoch;
      if (active && target._pushUntil > this.time && target._pushSpeed > speed + .05) return;
      target._pushSource = source.id; target._pushNX = nx; target._pushNY = ny;
      target._pushSpeed = speed; target._pushForce = force; target._pushDepth = depth; target._pushUntil = this.time + .16;
      target._pushEpoch = this.regiments[source.regiment]._commandEpoch;
    }

    _tryShieldPush(u, target) {
      if (!this._canShieldPress(u) || !target.alive || target.routed) return;
      const t = TYPES[u.type], r = this.regiments[u.regiment];
      const dx = target.x - u.x, dy = target.y - u.y, distance = Math.max(1, Math.hypot(dx, dy));
      if (distance > u._radius + target._radius + t.push.reach) return;
      const nx = Math.cos(r.facing), ny = Math.sin(r.facing);
      if ((dx * nx + dy * ny) / distance < .65) return;
      const support = this._shieldSupport(u, nx, ny);
      if (!support) return;
      const strength = t.mass * (.65 + support * .15) * clamp(r.cohesion / 100, 0, 1);
      this._queueShieldPush(target, u, nx, ny, strength, 0);
    }

    _resolveShieldPush(u, dt) {
      if (u._pushUntil <= this.time) return;
      const source = this.units[u._pushSource];
      if (!source || !this._canShieldPress(source) || u._pushEpoch !== this.regiments[source.regiment]._commandEpoch ||
          u._pushForce <= this._shieldResistance(u, source)) { u._pushUntil = 0; return; }
      let amount = Math.min(u._pushSpeed, TYPES[u.type].speed * .20) * dt;
      if (this.impacts) amount = Math.min(amount, this.impacts.remainingMotion(u, dt));
      if (amount < .0001) return;
      const nx = u._pushNX, ny = u._pushNY;
      const movement = this._collideMovement(u, nx * amount, ny * amount, amount);
      let mx = movement.x, my = movement.y;
      const distance = Math.hypot(mx, my);
      if (distance > amount) { mx *= amount / distance; my *= amount / distance; }
      if (!this._bodyBlocked(u.x + mx, u.y + my, u._radius) && this._lineClear(u.x, u.y, u.x + mx, u.y + my, u._radius)) {
        u.x += mx; u.y += my;
        const moved = Math.hypot(mx, my);
        u.travelDistance += moved;
        if (this.impacts) this.impacts.recordMove(u, moved);
        else { this._bodyX[u.id] = u.x; this._bodyY[u.id] = u.y; }
        if (mx * nx + my * ny > .001) { source._lastPush = this.time; u._braceTime = 0; }
      }
      // Pressure can pass through touching lighter allies. Every body gets
      // its own small swept movement; a rock, heavy rear rank, or closed gap
      // stops the chain rather than moving an entire formation by fiat.
      if (!movement.ally || u._pushDepth >= TYPES[source.type].push.maxChain) return;
      const reach = u._radius + this._maxRadius + 4;
      const minX = clamp(Math.floor((u.x - reach) / BODY_CELL), 0, this._bodyCols - 1), maxX = clamp(Math.floor((u.x + reach) / BODY_CELL), 0, this._bodyCols - 1);
      const minY = clamp(Math.floor((u.y - reach) / BODY_CELL), 0, this._bodyRows - 1), maxY = clamp(Math.floor((u.y + reach) / BODY_CELL), 0, this._bodyRows - 1);
      let next = null, nearest = Infinity;
      for (let cy = minY; cy <= maxY; cy++) for (let cx = minX; cx <= maxX; cx++) {
        for (let id = this._bodyHeads[u.team][cy * this._bodyCols + cx]; id !== -1; id = this._bodyNext[id]) {
          const other = this.units[id];
          if (id === u.id || !other.alive || other.routed) continue;
          const dx = other.x - u.x, dy = other.y - u.y, ahead = dx * nx + dy * ny, gap = u._radius + other._radius;
          if (ahead <= 0 || ahead > gap + 3 || Math.abs(-dx * ny + dy * nx) > gap * .8) continue;
          if (ahead < nearest) { nearest = ahead; next = other; }
        }
      }
      if (next) this._queueShieldPush(next, source, nx, ny, u._pushForce * .94, u._pushDepth + 1);
    }

    getUnitsInRect(x1, y1, x2, y2, team) {
      const loX = Math.min(x1, x2), hiX = Math.max(x1, x2);
      const loY = Math.min(y1, y2), hiY = Math.max(y1, y2);
      const found = new Set();
      for (let i = 0; i < this.units.length; i++) {
        const u = this.units[i];
        if (u.alive && !u.routed && (team === undefined || team === null || u.team === team) && u.x >= loX && u.x <= hiX && u.y >= loY && u.y <= hiY) found.add(u.regiment);
      }
      return Array.from(found);
    }

    command(ids, order, x, y, options) {
      if (order === 'face') return this.faceRegiments(ids, x, y);
      if (!['auto', 'move', 'attack', 'hold', 'charge', 'retreat', 'rally'].includes(order) || this.phase === 'ended') return 0;
      const list = Array.from(new Set(ids || []));
      const targets = list.map(id => this.getRegiment(id)).filter(r => r && r.alive > 0);
      if (order === 'rally') {
        let activated = 0;
        for (const r of targets) if (TYPES[r.type].commander && !r.fallingBack && r.rallyCooldown <= 0) { this._rally(r, true); activated++; }
        this._refreshStats();
        return activated;
      }
      const hasPoint = Number.isFinite(x) && Number.isFinite(y);
      const accepted = [];
      const explicitTarget = options && Number.isInteger(options.targetRegimentId) ? this.getRegiment(options.targetRegimentId) : null;
      const specifiedTarget = !!(options && Object.prototype.hasOwnProperty.call(options, 'targetRegimentId'));
      const recall = order === 'move' && hasPoint;
      const ready = targets.filter(r => recall || (!r.fallingBack && r._rallyTime <= 0));
      const destinations = hasPoint && (order === 'move' || order === 'attack') ? this._commandDestinations(ready, x, y) : null;
      for (let i = 0; i < targets.length; i++) {
        const r = targets[i];
        if (order === 'retreat') { if (this._startFallback(r, true)) { if(this.ramparts)this.ramparts.onCommand(r,order);accepted.push(r); } continue; }
        if (recall && (r.fallingBack || r._rallyTime > 0)) {
          r.fallingBack = r._manualFallback = false; r._rallyTime = 0;
          r.status = r.morale < 42 || r.cohesion < 40 ? 'shaken' : 'steady';
          r.supportRegimentId = -1; r.regroupWith = '';
        }
        if (r.fallingBack || r._rallyTime > 0) { r.commandStatus = r.fallingBack ? 'withdrawing' : 'regrouping'; continue; }
        if (order === 'charge' && specifiedTarget && (!explicitTarget || explicitTarget.team === r.team || explicitTarget.alive <= 0 || explicitTarget._routed)) continue;
        if(order==='charge'&&explicitTarget&&TYPES[r.type].mounted&&explicitTarget.unitIds.every(id=>!this.units[id].alive||this.units[id]._onRampart))continue;
        accepted.push(r);
        if(this.ramparts)this.ramparts.onCommand(r,order);
        this._resetCommandIntent(r);
        const center = this._currentCenter(r);
        const anchor = this._nearestPassable(center.x, center.y, TYPES[r.type].radius + 2);
        r.x = anchor.x; r.y = anchor.y;
        r._centroidX = center.x; r._centroidY = center.y;
        r.order = order;
        r._holdX = r.x;
        r._holdY = r.y;
        r._hasDestination = hasPoint && (order === 'move' || order === 'attack');
        if (r._hasDestination) {
          const destination = destinations.get(r.id);
          r.targetX = destination.x;
          r.targetY = destination.y;
        } else {
          r.targetX = r.x;
          r.targetY = r.y;
        }
        if (order === 'hold') {
          for (const uid of r.unitIds) {
            const u = this.units[uid];
            if (u.alive && !u.routed) u._holdPosition = { x: u.x, y: u.y };
          }
        }
        if (order === 'charge') this._setChargeTarget(r, explicitTarget || this._nearestChargeTarget(r));
        r._commandTime = this.time;
        r._commandBlocked = false;
        r._travelSlots = order === 'move' || order === 'attack' || order === 'charge';
        if (order === 'auto' || order === 'move' || order === 'attack' || order === 'charge') this._captureMarchLayout(r);
        if (r._travelSlots) {
          r._approachBlendUntil = this.time + 2;
          for (const uid of r.unitIds) {
            const u = this.units[uid];
            u._orderOffsetX = u.x - r.x; u._orderOffsetY = u.y - r.y;
          }
        }
      }
      if (accepted.length && order !== 'retreat') {
        const labels = { auto: 'returns to army doctrine', move: 'moves to the marked position', attack: 'advances to engage', hold: 'holds formation', charge: 'begins its charge' };
        this._event((accepted.length === 1 ? accepted[0].name : accepted.length + ' formations') + ' ' + labels[order] + '.', accepted[0].team, 'order');
      }
      this._strategize();
      this._refreshStats();
      return accepted.length;
    }

    setDoctrine(team, doctrine) {
      if ((team !== 0 && team !== 1) || !DATA.DOCTRINES || !Object.prototype.hasOwnProperty.call(DATA.DOCTRINES, doctrine) || this.phase === 'ended') return false;
      if (this.config.armies[team].doctrine === doctrine) return true;
      this.config.armies[team].doctrine = doctrine;
      if (global.BattleTactics) {
        if (!this._tactics) this._tactics = new global.BattleTactics(this);
        this._tactics.invalidate(team);
      }
      this._strategyTimer = 0;
      if (this.phase !== 'deployment') this._strategize();
      this._event(this.config.armies[team].name + ' adopts ' + DATA.DOCTRINES[doctrine].name + '. Formations under Auto take the new plan.', team, 'order');
      return true;
    }

    _currentCenter(r) {
      let x = 0, y = 0, count = 0;
      for (const id of r.unitIds) {
        const u = this.units[id];
        if (u.alive && !u.routed) { x += u.x; y += u.y; count++; }
      }
      return count ? { x: x / count, y: y / count } : { x: r.x, y: r.y };
    }

    _commandDestinations(regiments, x, y) {
      const result = new Map();
      if (!regiments.length) return result;
      const centers = regiments.map(r => this._currentCenter(r));
      const cx = centers.reduce((sum, p) => sum + p.x, 0) / centers.length;
      const cy = centers.reduce((sum, p) => sum + p.y, 0) / centers.length;
      const minX = Math.min(...centers.map(p => p.x - cx)), maxX = Math.max(...centers.map(p => p.x - cx));
      const minY = Math.min(...centers.map(p => p.y - cy)), maxY = Math.max(...centers.map(p => p.y - cy));
      const scale = Math.min(1, (this.width - 140) / Math.max(1, maxX - minX), (this.height - 140) / Math.max(1, maxY - minY));
      const targetX = clamp(x, 70 - minX * scale, this.width - 70 - maxX * scale);
      const targetY = clamp(y, 70 - minY * scale, this.height - 70 - maxY * scale);
      const used = new Set();
      for (let i = 0; i < regiments.length; i++) {
        const r = regiments[i], baseX = targetX + (centers[i].x - cx) * scale, baseY = targetY + (centers[i].y - cy) * scale;
        let p = this._nearestPassable(baseX, baseY, TYPES[r.type].radius + 2);
        // Whole-group fitting avoids clamping many formations onto one map
        // edge. Resolve the occasional shared terrain projection separately.
        let key = Math.round(p.x / 24) + ':' + Math.round(p.y / 24);
        for (let n = 1; used.has(key) && n <= 48; n++) {
          const angle = n * 2.399963229728653, radius = 38 * Math.sqrt(n);
          p = this._nearestPassable(clamp(baseX + Math.cos(angle) * radius, 45, this.width - 45), clamp(baseY + Math.sin(angle) * radius, 45, this.height - 45), TYPES[r.type].radius + 2);
          key = Math.round(p.x / 24) + ':' + Math.round(p.y / 24);
        }
        used.add(key); result.set(r.id, p);
      }
      return result;
    }

    _resetCommandIntent(r) {
      r._commandEpoch++; r._frontTurning = false;
      if (this.impacts) { this.impacts.cancelPressure(r); this.impacts.cancelFriendly(r); }
      r._steadyBraceScale = 0; r._tacticalMovement = false; r._tacticalHold = false;
      if (r._traffic) this._clearTraffic(r._traffic.plan);
      r._path = []; r._pathTimer = 0; r._routeDetour = false; r._bridge = null;
      r._enemyId = -1; r._assignmentUntil = 0; r._chargeTargetId = -1;
      r._combatActive = false; r._combatEnemy = -1; r._combatSlotsTime = -10;
      r._missileActive = false; r._missileSlotsTime = -10; r._missileEnemy = -1; r._fireLocked = false; r._fireLastInRange = -1000;
      r._marchLayout = false; r._faceDuration = 0;
      r._approachEnemy = -1; r._localFront = false; r._marchTurnUntil = 0;
      r._approachNX = Math.cos(r.facing); r._approachNY = Math.sin(r.facing);
      r.bracedSoldiers = 0; r.pushingSoldiers = 0;
      r._commandBlocked = false; r._travelSlots = false; r._approachBlendUntil = 0;
      r._attackBlocked = false; r._retreatTurnUntil = 0; r.commandTurnRemaining = 0; r._withdrawLeg = false;
      for (const uid of r.unitIds) {
        const u = this.units[uid];
        this._setTarget(u, -1);
        u._sense = 0; u._localPath.length = 0; u._localTimer = 0;
        u._localGoalX = u.x; u._localGoalY = u.y;
        u._portalCorner = null; u._portalGoal = null; u._portalUntil = 0;
        u._circulateUntil = 0; u._circulateNext = 0; u._idleContactTime = 0;
        u._circulateX = u.x; u._circulateY = u.y;
        u._pressUntil = 0; u._pressColumn = -1; u._jamTime = 0;
        u._detour = null; u._stuck = 0; u._yieldUntil = 0;
        u._holdPosition = null; u._moveSlot = -1;
        u._contactSense = 0; u._turnEnd = 0; u._missileSlot = -1;
        u._passageUntil = 0; u._passageNext = 0; u._fireReachTime = -1000; u._fireReachEnemy = -1;
        u._marchTurnEnd = 0; u._plungeUntil = 0;
        u._braceTime = 0; u._lastPush = -1000; u._pushUntil = 0;
      }
    }

    _nearestChargeTarget(r) {
      const center = this._currentCenter(r), candidates = [];
      for (const other of this.regiments) {
        if (other.team === r.team || other.alive <= 0 || other._routed) continue;
        const px = clamp(center.x, other._actualMinX, other._actualMaxX);
        const py = clamp(center.y, other._actualMinY, other._actualMaxY);
        const score = Math.hypot(px - center.x, py - center.y) + Math.hypot(other._centroidX - center.x, other._centroidY - center.y) * .12;
        candidates.push({ r: other, x: px, y: py, score });
      }
      candidates.sort((a, b) => a.score - b.score || a.r.id - b.r.id);
      let best = null, score = Infinity;
      for (const candidate of candidates.slice(0, 5)) {
        const path = this._findPath(center.x, center.y, candidate.x, candidate.y, TYPES[r.type].radius + 3);
        if (!path.length) continue;
        let distance = 0, x = center.x, y = center.y;
        for (const p of path) { distance += Math.hypot(p.x - x, p.y - y); x = p.x; y = p.y; }
        const value = distance + candidate.score * .12;
        if (value < score) { score = value; best = candidate.r; }
      }
      return best || (candidates[0] && candidates[0].r) || null;
    }

    _setChargeTarget(r, target) {
      r._chargeTargetId = target ? target.id : -1;
      r.commandTargetId = r._chargeTargetId; r.commandTargetName = target ? target.name : '';
      r._enemyId = r._chargeTargetId; r._combatSlotsTime = -10; r._pathTimer = 0;
      if (target) {
        const dx = target._centroidX - r._centroidX, dy = target._centroidY - r._centroidY, distance = Math.max(1, Math.hypot(dx, dy));
        r._chargeNX = dx / distance; r._chargeNY = dy / distance;
        r.facing = Math.atan2(dy, dx); r._travelAngle = r.facing;
      }
    }

    _advancingAttack(r) {
      return r.order === 'attack' && r._hasDestination && !r._attackBlocked;
    }

    _commandFeedback(r) {
      const target = this.regiments[r.order === 'charge' ? r._chargeTargetId : r._enemyId];
      r.commandTargetId = target && target.alive > 0 && (r.order === 'charge' || r.order === 'attack') ? target.id : -1;
      r.commandTargetName = r.commandTargetId >= 0 ? target.name : '';
      r.commandX = r.order === 'charge' && target ? target._centroidX : r.targetX;
      r.commandY = r.order === 'charge' && target ? target._centroidY : r.targetY;
      if (r.fallingBack) r.commandStatus = r.commandTurnRemaining > 0 ? 'turning' : 'withdrawing';
      else if (r._rallyTime > 0) r.commandStatus = 'regrouping';
      else if (r.order === 'face') r.commandStatus = 'facing';
      else if (r.order === 'hold') r.commandStatus = 'holding';
      else if (r.order === 'auto') r.commandStatus = 'automatic';
      else if (r.order === 'charge' && !target) r.commandStatus = 'no-target';
      else if (r.order !== 'move' && r.engagedSoldiers > 0) r.commandStatus = TYPES[r.type].ranged ? 'firing' : 'engaging';
      else if (r._commandBlocked) r.commandStatus = 'blocked';
      else r.commandStatus = r.order === 'charge' ? 'charging' : r.order === 'move' ? 'moving' : 'advancing';
    }

    relocateRegiments(ids, x, y) {
      if (this.phase !== 'deployment' || !Number.isFinite(x) || !Number.isFinite(y)) return 0;
      const groups = Array.from(new Set(ids || [])).map(id => this.getRegiment(id)).filter(Boolean);
      if (!groups.length) return 0;
      const cx = groups.reduce((v, r) => v + r.x, 0) / groups.length;
      const cy = groups.reduce((v, r) => v + r.y, 0) / groups.length;
      for (const r of groups) {
        const p = this._nearestPassable(clamp(x + r.x - cx, 100, this.width - 100), clamp(y + r.y - cy, 100, this.height - 100));
        const dx = p.x - r.x, dy = p.y - r.y;
        for (const id of r.unitIds) {
          const u = this.units[id];
          const q = this._nearestPassable(u.x + dx, u.y + dy);
          u.x = q.x; u.y = q.y;
          u._terrain = this.terrainAt(u.x, u.y);
        }
        r.x = r._baseX = r._holdX = r.targetX = p.x;
        r.y = r._baseY = r._holdY = r.targetY = p.y;
        r._centroidX = p.x; r._centroidY = p.y;
        r._actualMinX = r._actualMinY = Infinity;
        r._actualMaxX = r._actualMaxY = -Infinity;
        for (const id of r.unitIds) {
          const u = this.units[id];
          r._actualMinX = Math.min(r._actualMinX, u.x); r._actualMaxX = Math.max(r._actualMaxX, u.x);
          r._actualMinY = Math.min(r._actualMinY, u.y); r._actualMaxY = Math.max(r._actualMaxY, u.y);
          u._localPath.length = 0; u._localTimer = 0;
        }
        r._pathTimer = 0;
      }
      this._rebuildHash();
      return groups.length;
    }

    terrainAt(x, y) {
      for (const fort of this._fortifications) if (inside(x, y, fort)) {
        if (fort.open) continue;
        if (fort.breached) return TERRAINS.rubble;
        if (fort.passages && fort.passages.some(p => inside(x, y, p))) return TERRAINS.ramp;
        return TERRAINS[fort.type];
      }
      if (this.siege && this.siege.ramps.length && this.siege.ramps.some(r => inside(x, y, r))) return TERRAINS.ramp;
      for (let i = 0; i < this._bridges.length; i++) if (inside(x, y, this._bridges[i])) return TERRAINS.bridge;
      // Roads and other newly painted ground override older passable ground.
      // Solid rocks/water still own the body and projectile geometry.
      for (const block of this._blocks) if (block.type !== 'wall' && block.type !== 'gate' && inside(x, y, block)) return TERRAINS[block.type];
      for (let i = this._regions.length - 1; i >= 0; i--) {
        const region = this._regions[i];
        if (GROUND_OVERLAYS.has(region.type) && inside(x, y, region)) return TERRAINS[region.type];
      }
      for (let i = 0; i < this._regions.length; i++) {
        const r = this._regions[i];
        if (r.type === 'wall' || r.type === 'gate') continue;
        if (inside(x, y, r)) return TERRAINS[r.type] || EMPTY_TERRAIN;
      }
      return EMPTY_TERRAIN;
    }
    unitAltitude(u){return u?(u._rampartZ||this.siege&&this.siege.ramps.length&&this.siege.heightAt(u.x,u.y)||0):0;}

    _blocked(x, y) {
      if (x < 8 || y < 8 || x > this.width - 8 || y > this.height - 8) return true;
      for (const fort of this._fortifications) if (!fort.breached && !fort.open && inside(x, y, fort) && !(fort.passages && fort.passages.some(p => inside(x, y, p)))) return true;
      for (let i = 0; i < this._bridges.length; i++) if (inside(x, y, this._bridges[i])) return false;
      for (let i = 0; i < this._blocks.length; i++) if (this._blocks[i].type !== 'wall' && this._blocks[i].type !== 'gate' && inside(x, y, this._blocks[i])) return true;
      return false;
    }

    _bodyBlocked(x, y, radius) {
      if (!radius) return this._blocked(x, y);
      if (x < radius + 8 || y < radius + 8 || x > this.width - radius - 8 || y > this.height - radius - 8) return true;
      if(radius<=64&&!this._nearSolid[Math.floor(y/NAV)*this._navCols+Math.floor(x/NAV)])return false;
      for (const block of this._blocks) {
        if (block.breached || block.open) continue;
        if (!inside(x, y, block, radius + 1)) continue;
        if (block.type === 'rock') return true;
        if (block.type === 'wall' || block.type === 'gate') {
          if (!(block.passages && block.passages.some(p => inside(x, y, p, -radius - 1)))) return true;
          continue;
        }
        let bridgeClear = false;
        for (const bridge of this._bridges) {
          if (x >= bridge.x + radius && x <= bridge.x + bridge.w - radius && y >= bridge.y + radius && y <= bridge.y + bridge.h - radius) { bridgeClear = true; break; }
        }
        if (!bridgeClear) return true;
      }
      return false;
    }

    _refreshTerrainNavigation() {
      // A completed engineering operation is the only event that rebuilds
      // solid terrain. Normal frames reuse all clearance/path caches.
      this.terrainRevision++;
      this._nearSolid.fill(0);
      for (const block of this._blocks) {
        if (block.breached || block.open) continue;
        const minX = Math.max(0, Math.floor((block.x - 65) / NAV)), maxX = Math.min(this._navCols - 1, Math.floor((block.x + block.w + 65) / NAV));
        const minY = Math.max(0, Math.floor((block.y - 65) / NAV)), maxY = Math.min(this._navRows - 1, Math.floor((block.y + block.h + 65) / NAV));
        for (let y = minY; y <= maxY; y++) this._nearSolid.fill(1, y * this._navCols + minX, y * this._navCols + maxX + 1);
      }
      for (let y = 0; y < this._navRows; y++) for (let x = 0; x < this._navCols; x++) {
        const i = y * this._navCols + x, xx = (x + .5) * NAV, yy = (y + .5) * NAV;
        this._navBlocked[i] = this._blocked(xx, yy) ? 1 : 0;
        const terrain = this.terrainAt(xx, yy);
        this._navTerrain[i] = GROUND_CODES[terrain.type] || 0;
      }
      this._navClearance.clear(); this._navClearance.set(0, this._navBlocked);
      this._approachNext = 0; this._strategyTimer = 0;
      for (const r of this.regiments) { r._path.length = 0; r._pathTimer = 0; r._siegeReachRevision = -1; }
      for (const u of this.units) {
        u._localPath.length = 0; u._localTimer = 0;
        u._portalCorner = null; u._portalGoal = null; u._portalUntil = 0;
        u._detour = null; u._terrainTimer = 0;
      }
    }

    _clearanceGrid(radius) {
      const key = Math.ceil(radius / 4) * 4;
      if (this._navClearance.has(key)) return this._navClearance.get(key);
      const grid = new Uint8Array(this._navCols * this._navRows);
      for (let y = 0; y < this._navRows; y++) for (let x = 0; x < this._navCols; x++) grid[y * this._navCols + x] = this._bodyBlocked((x + .5) * NAV, (y + .5) * NAV, key) ? 1 : 0;
      this._navClearance.set(key, grid);
      return grid;
    }

    _nearestPassable(x, y, clearance) {
      clearance = clearance || 0;
      x = clamp(x, 28, this.width - 28); y = clamp(y, 28, this.height - 28);
      if (!this._bodyBlocked(x, y, clearance)) return { x, y };
      // A horse can stand safely beside a wall while its next slot falls
      // inside navigation's extra padding. Find the nearby clear center of
      // the passage before a coarse probe sends it around the whole wall.
      if (clearance > 0 && !this._bodyBlocked(x, y, Math.max(0, clearance - 3))) {
        for (let radius = 2; radius <= 16; radius *= 2) {
          for (let a = 0; a < 16; a++) {
            const nx = clamp(x + Math.cos(a * TAU / 16) * radius, 28, this.width - 28);
            const ny = clamp(y + Math.sin(a * TAU / 16) * radius, 28, this.height - 28);
            if (!this._bodyBlocked(nx, ny, clearance)) return { x: nx, y: ny };
          }
        }
      }
      for (let radius = 40; radius <= 1000; radius += 40) {
        for (let a = 0; a < 16; a++) {
          const nx = clamp(x + Math.cos(a * TAU / 16) * radius, 28, this.width - 28);
          const ny = clamp(y + Math.sin(a * TAU / 16) * radius, 28, this.height - 28);
          if (!this._bodyBlocked(nx, ny, clearance)) return { x: nx, y: ny };
        }
      }
      return { x: 200, y: this.height / 2 };
    }

    _lineClear(x1, y1, x2, y2, clearance) {
      clearance = clearance || 0;
      const dx = x2 - x1, dy = y2 - y1, margin = clearance ? clearance + 1 : 0;
      if (x2 < clearance + 8 || y2 < clearance + 8 || x2 > this.width - clearance - 8 || y2 > this.height - clearance - 8) return false;
      // Exact rectangle intervals prevent a short diagonal corner crossing
      // from hiding between ray samples and freezing a formation's anchor.
      for (const block of this._blocks) {
        if (block.breached || block.open) continue;
        const interval = segmentBox(x1, y1, dx, dy, block, margin);
        if (!interval) continue;
        // Navigation adds a small margin to physical bodies. A soldier may
        // leave that extra margin without being sent around the whole rock.
        if (interval[0] <= 1e-8 && interval[1] < 1 && clearance && !segmentBox(x1, y1, dx, dy, block, Math.max(0, clearance - 2))) continue;
        if (block.type === 'rock') return false;
        const crossings = [];
        const passages = block.type === 'wall' || block.type === 'gate' ? block.passages || [] : this._bridges;
        for (const bridge of passages) {
          const span = segmentBox(x1, y1, dx, dy, bridge, -clearance);
          if (span && span[1] >= interval[0] && span[0] <= interval[1]) crossings.push(span);
        }
        crossings.sort((a, b) => a[0] - b[0]);
        let covered = interval[0];
        for (const span of crossings) {
          if (span[0] > covered + 1e-8) break;
          covered = Math.max(covered, span[1]);
        }
        if (covered < interval[1] - 1e-8) return false;
      }
      return true;
    }

    _findPath(x1, y1, x2, y2, clearance) {
      clearance = clearance || 0;
      const goal = this._nearestPassable(x2, y2, clearance);
      if (this._lineClear(x1, y1, goal.x, goal.y, clearance)) return [goal];
      const blockedGrid = this._clearanceGrid(clearance);
      const cols = this._navCols, rows = this._navRows, count = cols * rows;
      const toCell = (x, y) => clamp(Math.floor(y / NAV), 0, rows - 1) * cols + clamp(Math.floor(x / NAV), 0, cols - 1);
      let start = toCell(x1, y1), finish = toCell(goal.x, goal.y);
      const closestCell = id => {
        if (!blockedGrid[id]) return id;
        const sx = id % cols, sy = Math.floor(id / cols);
        for (let radius = 1; radius < 12; radius++) {
          for (let oy = -radius; oy <= radius; oy++) for (let ox = -radius; ox <= radius; ox++) {
            const xx = sx + ox, yy = sy + oy;
            if (xx >= 0 && yy >= 0 && xx < cols && yy < rows && !blockedGrid[yy * cols + xx]) return yy * cols + xx;
          }
        }
        return id;
      };
      start = closestCell(start); finish = closestCell(finish);
      const queue = new Int32Array(count), previous = new Int32Array(count);
      previous.fill(-1); previous[start] = start;
      let head = 0, tail = 0;
      queue[tail++] = start;
      const deltas = [1, -1, cols, -cols, cols + 1, cols - 1, -cols + 1, -cols - 1];
      while (head < tail && previous[finish] === -1) {
        const current = queue[head++], cx = current % cols, cy = Math.floor(current / cols);
        for (let i = 0; i < deltas.length; i++) {
          const next = current + deltas[i];
          if (next < 0 || next >= count || previous[next] !== -1 || blockedGrid[next]) continue;
          const nx = next % cols, ny = Math.floor(next / cols);
          if (Math.abs(nx - cx) > 1 || Math.abs(ny - cy) > 1) continue;
          if (i >= 4 && (blockedGrid[cy * cols + nx] || blockedGrid[ny * cols + cx])) continue;
          previous[next] = current;
          queue[tail++] = next;
        }
      }
      if (previous[finish] === -1) return [];
      const reversed = [];
      for (let cell = finish; cell !== start; cell = previous[cell]) reversed.push({ x: (cell % cols + .5) * NAV, y: (Math.floor(cell / cols) + .5) * NAV });
      reversed.reverse();
      reversed.push(goal);
      const simplified = [];
      let px = x1, py = y1, index = 0;
      while (index < reversed.length) {
        let furthest = index;
        for (let j = index + 1; j < reversed.length; j++) {
          if (!this._lineClear(px, py, reversed[j].x, reversed[j].y, clearance)) break;
          furthest = j;
        }
        const waypoint = reversed[furthest];
        simplified.push(waypoint); px = waypoint.x; py = waypoint.y;
        index = furthest + 1;
      }
      return simplified;
    }

    _rebuildHash(useCache) {
      this._bodyHashDrift = 0;
      this._heads[0].fill(-1); this._heads[1].fill(-1);
      this._cellCounts[0].fill(0); this._cellCounts[1].fill(0);
      this._bodyHeads[0].fill(-1); this._bodyHeads[1].fill(-1);
      this._smallSolidHeads[0].fill(-1);this._smallSolidHeads[1].fill(-1);
      this._largeSolidHeads[0].fill(-1);this._largeSolidHeads[1].fill(-1);
      const compact = useCache && this.units.length > 20000 && this.structures.length === 0;
      if (!compact) this._bodyLive.fill(0);
      if (this.units.length > 20000) for (const r of this.regiments) {
        r._rigidMinX = r._rigidMinY = Infinity; r._rigidMaxX = r._rigidMaxY = -Infinity;
        r._rigidCenterX = r._rigidCenterY = 0;
      }
      for (let i = 0; i < this.units.length; i++) {
        const u = compact ? null : this.units[i];
        if (compact ? !this._bodyLive[i] : !u.alive || u.routed&&!this.regiments[u.regiment]._rampartJob) continue;
        if (!compact) { this._bodyX[i] = u.x; this._bodyY[i] = u.y; this._bodyRadius[i] = u._radius; this._bodyLive[i] = 1; this._bodyTeam[i] = u.team; this._bodyRegiment[i] = u.regiment; this._bodyZ[i]=this.unitAltitude(u); }
        const px = this._bodyX[i], py = this._bodyY[i], radius = this._bodyRadius[i], team = this._bodyTeam[i];
        if (this.units.length > 20000) {
          const r = this.regiments[this._bodyRegiment[i]];
          r._rigidMinX = Math.min(r._rigidMinX, px - radius); r._rigidMaxX = Math.max(r._rigidMaxX, px + radius);
          r._rigidMinY = Math.min(r._rigidMinY, py - radius); r._rigidMaxY = Math.max(r._rigidMaxY, py + radius);
          r._rigidCenterX += px; r._rigidCenterY += py;
        }
        const cell = clamp(Math.floor(py / CELL), 0, this._rows - 1) * this._cols + clamp(Math.floor(px / CELL), 0, this._cols - 1);
        this._next[i] = this._heads[team][cell];
        this._heads[team][cell] = i;
        this._cellCounts[team][cell]++;
        // A deployed tower is a raised platform; its crew remains targetable,
        // while soldiers cross the visible ramp underneath its firing deck.
        if (!compact && u._siegeDeployed && !u._siegeUndocking) continue;
        const body = clamp(Math.floor(py / BODY_CELL), 0, this._bodyRows - 1) * this._bodyCols + clamp(Math.floor(px / BODY_CELL), 0, this._bodyCols - 1);
        this._bodyNext[i] = this._bodyHeads[team][body];
        this._bodyHeads[team][body] = i;
        if(radius<=12){this._solidNext[i]=this._smallSolidHeads[team][body];this._smallSolidHeads[team][body]=i;}
        else{
          const large=clamp(Math.floor(py/128),0,this._largeBodyRows-1)*this._largeBodyCols+clamp(Math.floor(px/128),0,this._largeBodyCols-1);
          this._solidNext[i]=this._largeSolidHeads[team][large];this._largeSolidHeads[team][large]=i;
        }
      }
    }

    _planRegimentRoute(r, gx, gy) {
      const t = TYPES[r.type], distance = Math.hypot(gx - r.x, gy - r.y);
      r._routeDetour = false;
      // Explicit movement preserves the requested direct route. Infantry also
      // take a clear woodland path unless real congestion warrants a detour.
      if (r.order === 'move' || r.order === 'hold' || distance < 230) return this._findPath(r.x, r.y, gx, gy, t.radius + 3);
      let crowded = false, forest = false;
      const samples = Math.max(1, Math.ceil(distance / 110));
      for (let i = 1; i < samples; i++) {
        const x = r.x + (gx - r.x) * i / samples, y = r.y + (gy - r.y) * i / samples;
        const cell = clamp(Math.floor(y / NAV), 0, this._navRows - 1) * this._navCols + clamp(Math.floor(x / NAV), 0, this._navCols - 1);
        if (this._navTerrain[cell] === 1) forest = true;
        if (i / samples * distance > r.depth + 100 && this._cellCounts[r.team][cell] > 22) crowded = true;
      }
      if ((!t.mounted || !forest) && !crowded && this._lineClear(r.x, r.y, gx, gy, t.radius + 3)) return [{ x: gx, y: gy }];
      if (!t.mounted && !crowded && this._lineClear(r.x, r.y, gx, gy)) return [{ x: gx, y: gy }];
      const blocked = this._clearanceGrid(t.radius + 3), cols = this._navCols, rows = this._navRows, count = cols * rows;
      const goal = this._nearestPassable(gx, gy, t.radius + 3);
      const cellAt = (x, y) => clamp(Math.floor(y / NAV), 0, rows - 1) * cols + clamp(Math.floor(x / NAV), 0, cols - 1);
      const accessible = initial => {
        if (!blocked[initial]) return initial;
        const xx = initial % cols, yy = Math.floor(initial / cols);
        for (let radius = 1; radius < 10; radius++) for (let y = -radius; y <= radius; y++) for (let x = -radius; x <= radius; x++) {
          const nx = xx + x, ny = yy + y;
          if (nx >= 0 && nx < cols && ny >= 0 && ny < rows && !blocked[ny * cols + nx]) return ny * cols + nx;
        }
        return initial;
      };
      const start = accessible(cellAt(r.x, r.y)), finish = accessible(cellAt(goal.x, goal.y));
      const scratch = this._routeScratch, generation = ++scratch.generation;
      const cost = scratch.cost, parent = scratch.parent, seen = scratch.seen, closed = scratch.closed;
      const heap = scratch.heap, priority = scratch.priority;
      let size = 0;
      const push = (id, value) => {
        if (size >= heap.length) return;
        let index = size++;
        while (index > 0) { const above = (index - 1) >> 1; if (priority[above] <= value) break; heap[index] = heap[above]; priority[index] = priority[above]; index = above; }
        heap[index] = id; priority[index] = value;
      };
      const pop = () => {
        const result = heap[0], id = heap[--size], value = priority[size];
        let index = 0;
        while (index * 2 + 1 < size) { let child = index * 2 + 1; if (child + 1 < size && priority[child + 1] < priority[child]) child++; if (priority[child] >= value) break; heap[index] = heap[child]; priority[index] = priority[child]; index = child; }
        heap[index] = id; priority[index] = value; return result;
      };
      const fx = finish % cols, fy = Math.floor(finish / cols);
      const heuristic = id => { const x = Math.abs(id % cols - fx), y = Math.abs(Math.floor(id / cols) - fy); return (Math.max(x, y) + Math.min(x, y) * .41421356) * this._minimumGroundCost; };
      seen[start] = generation; cost[start] = 0; parent[start] = start; push(start, heuristic(start));
      const directions = [[1,0],[-1,0],[0,1],[0,-1],[1,1],[1,-1],[-1,1],[-1,-1]];
      let found = false, expanded = 0;
      while (size && expanded++ < count) {
        const current = pop();
        if (closed[current] === generation) continue;
        if (current === finish) { found = true; break; }
        closed[current] = generation;
        const x = current % cols, y = Math.floor(current / cols);
        for (let i = 0; i < directions.length; i++) {
          const xx = x + directions[i][0], yy = y + directions[i][1];
          if (xx < 0 || xx >= cols || yy < 0 || yy >= rows) continue;
          const next = yy * cols + xx;
          if (blocked[next] || closed[next] === generation || (i >= 4 && (blocked[y * cols + xx] || blocked[yy * cols + x]))) continue;
          const terrain = this._navTerrain[next];
          const forestMultiplier = Number.isFinite(t.forestSpeed) ? t.forestSpeed : t.mounted ? .62 : 1;
          const ground = terrain === 1 ? 1 / (.78 * forestMultiplier) : GROUND_COST[terrain] || 1;
          const ownDistance = Math.hypot((xx + .5) * NAV - r.x, (yy + .5) * NAV - r.y);
          const traffic = ownDistance > r.depth + 70 ? Math.min(2, Math.max(0, this._cellCounts[r.team][next] - 7) / 18) * .65 : 0;
          const candidate = cost[current] + (i < 4 ? 1 : Math.SQRT2) * (ground + traffic);
          if (seen[next] !== generation || candidate < cost[next]) { seen[next] = generation; cost[next] = candidate; parent[next] = current; push(next, candidate + heuristic(next)); }
        }
      }
      if (!found) return this._findPath(r.x, r.y, gx, gy, t.radius + 3);
      const cells = [];
      for (let cell = finish; cell !== start; cell = parent[cell]) cells.push(cell);
      cells.reverse();
      const path = [];
      let previous = start, lastDX = 0, lastDY = 0;
      for (let i = 0; i < cells.length; i++) {
        const cell = cells[i], dx = cell % cols - previous % cols, dy = Math.floor(cell / cols) - Math.floor(previous / cols);
        if (i > 0 && (dx !== lastDX || dy !== lastDY)) path.push({ x: (previous % cols + .5) * NAV, y: (Math.floor(previous / cols) + .5) * NAV });
        lastDX = dx; lastDY = dy; previous = cell;
      }
      path.push(goal);
      r._routeDetour = path.length > 1 && t.mounted && forest;
      return path;
    }

    _nearestEnemy(u, radius) {
      let best = -1, bestSquared = radius * radius;
      const minX = clamp(Math.floor((u.x - radius) / CELL), 0, this._cols - 1);
      const maxX = clamp(Math.floor((u.x + radius) / CELL), 0, this._cols - 1);
      const minY = clamp(Math.floor((u.y - radius) / CELL), 0, this._rows - 1);
      const maxY = clamp(Math.floor((u.y + radius) / CELL), 0, this._rows - 1);
      let visited = 0;
      const heads = this._heads[1 - u.team];
      for (let y = minY; y <= maxY; y++) for (let x = minX; x <= maxX; x++) {
        for (let id = heads[y * this._cols + x]; id !== -1; id = this._next[id]) {
          const other = this.units[id];
          if (!other.alive || other.routed) continue;
          const dx = other.x - u.x, dy = other.y - u.y, distance = dx * dx + dy * dy;
          if (distance < bestSquared) { bestSquared = distance; best = id; }
          if (++visited > 100 && best !== -1) return best;
        }
      }
      return best;
    }

    _setTarget(u, id) {
      if (u.target >= 0 && this._targetClaims[u.target] > 0) this._targetClaims[u.target]--;
      u.target = id;
      if (id >= 0) this._targetClaims[id]++;
    }

    _contactTarget(u, reach) {
      const radius = reach + this._maxRadius, heads = this._bodyHeads[1 - u.team];
      const minX = clamp(Math.floor((u.x - radius) / BODY_CELL), 0, this._bodyCols - 1), maxX = clamp(Math.floor((u.x + radius) / BODY_CELL), 0, this._bodyCols - 1);
      const minY = clamp(Math.floor((u.y - radius) / BODY_CELL), 0, this._bodyRows - 1), maxY = clamp(Math.floor((u.y + radius) / BODY_CELL), 0, this._bodyRows - 1);
      let best = -1, score = Infinity;
      for (let y = minY; y <= maxY; y++) for (let x = minX; x <= maxX; x++) {
        for (let id = heads[y * this._bodyCols + x]; id !== -1; id = this._bodyNext[id]) {
          const enemy = this.units[id];
          if (!enemy.alive || enemy.routed) continue;
          if(Math.abs(this.unitAltitude(u)-this.unitAltitude(enemy))>18)continue;
          const dx = enemy.x - u.x, dy = enemy.y - u.y, d2 = dx * dx + dy * dy;
          if (d2 > (reach + enemy._radius) * (reach + enemy._radius)) continue;
          const value = d2 + Math.min(3, Math.max(0, this._targetClaims[id] - 1)) * 20;
          if (value < score) { score = value; best = id; }
        }
      }
      return best;
    }

    _chooseTarget(u, r, t) {
      const reach = t.range * (u._terrain.elevation ? 1.12 : 1);
      if (!t.ranged) {
        if (r.order === 'auto' || r.order === 'hold') {
          const contact = this._contactTarget(u, reach);
          if (contact !== -1) return contact;
        }
        const radius = Math.min(180, Math.max(120, reach + 110));
        const minX = clamp(Math.floor((u.x - radius) / CELL), 0, this._cols - 1), maxX = clamp(Math.floor((u.x + radius) / CELL), 0, this._cols - 1);
        const minY = clamp(Math.floor((u.y - radius) / CELL), 0, this._rows - 1), maxY = clamp(Math.floor((u.y + radius) / CELL), 0, this._rows - 1);
        const goal = r._combatActive ? this._combatGoal(u, r) : u;
        const lane = goal.x * r._frontTX + goal.y * r._frontTY;
        let best = -1, bestScore = Infinity, scanned = 0;
        const heads = this._heads[1 - u.team];
        for (let y = minY; y <= maxY; y++) for (let x = minX; x <= maxX; x++) {
          for (let id = heads[y * this._cols + x]; id !== -1; id = this._next[id]) {
            const enemy = this.units[id];
            if (!enemy.alive || enemy.routed) continue;
            if(Math.abs(this.unitAltitude(u)-this.unitAltitude(enemy))>18)continue;
            const dx = enemy.x - u.x, dy = enemy.y - u.y, d2 = dx * dx + dy * dy;
            if (d2 > radius * radius) continue;
            const lateral = enemy.x * r._frontTX + enemy.y * r._frontTY - lane;
            const inReach = d2 <= (reach + enemy._radius) * (reach + enemy._radius);
            const claims = Math.max(0, this._targetClaims[id] - (u.target === id ? 1 : 0));
            const score = d2 * (inReach ? .35 : 1) + lateral * lateral * (inReach ? .10 : .65) + Math.max(0, claims - 1) * (inReach ? 250 : 1050);
            if (score < bestScore) { bestScore = score; best = id; }
            if (++scanned >= 150 && best !== -1) return best;
          }
        }
        return best;
      }
      if (r.order !== 'charge') {
        const localThreat = this._nearestEnemy(u, 80);
        if (localThreat !== -1) return localThreat;
      }
      const enemy = this.regiments[r._enemyId];
      if (!enemy || enemy.alive <= 0) return this._rangedLocalTarget(u, reach);
      let best = -1, bestScore = Infinity;
      const ids = enemy.unitIds, sample = Math.min(ids.length, 36), radius2 = (reach + 130) * (reach + 130);
      const offset = (u.id * 13 + this._tick) % Math.max(1, ids.length), stride = Math.max(1, Math.floor(ids.length / sample));
      for (let k = 0; k < sample; k++) {
        const candidate = this.units[ids[(offset + k * stride) % ids.length]];
        if (!candidate.alive || candidate.routed) continue;
        const dx = candidate.x - u.x, dy = candidate.y - u.y, distance = dx * dx + dy * dy;
        if (distance > radius2) continue;
        const preference = t.missile ? t.missile.armorPreference || 0 : 0;
        const score = distance * clamp(1 - TYPES[candidate.type].armor * preference, .55, 1.35) + Math.max(0, this._targetClaims[candidate.id] - 2) * 1700;
        if (score < bestScore) { best = candidate.id; bestScore = score; }
      }
      if (r.order !== 'charge' && t.missile && Math.abs(t.missile.armorPreference || 0) > .015) {
        const preferred = this._rangedLocalTarget(u, reach);
        if (preferred !== -1) return preferred;
      }
      const chosen = this.units[best];
      if (!chosen || (chosen.x - u.x) * (chosen.x - u.x) + (chosen.y - u.y) * (chosen.y - u.y) > reach * reach) {
        const local = this._rangedLocalTarget(u, reach);
        if (local !== -1) return local;
      }
      return best;
    }

    _rangedLocalTarget(u, reach) {
      const cx = Math.floor(u.x / CELL), cy = Math.floor(u.y / CELL), radius = Math.ceil(reach / CELL);
      const heads = this._heads[1 - u.team], reach2 = reach * reach;
      let best = -1, bestScore = Infinity, visited = 0, cells = 0;
      // Assigned regiments coordinate the approach, but must not prevent a
      // missile soldier firing on another enemy already within actual reach.
      // Nearby cell rings bound both candidate and empty-cell work.
      for (let ring = 0; ring <= radius; ring++) {
        for (let oy = -ring; oy <= ring; oy++) for (let ox = -ring; ox <= ring; ox++) {
          if (Math.max(Math.abs(ox), Math.abs(oy)) !== ring) continue;
          const x = cx + ox, y = cy + oy;
          if (x < 0 || y < 0 || x >= this._cols || y >= this._rows) continue;
          if (++cells > 512) return best;
          for (let id = heads[y * this._cols + x]; id !== -1; id = this._next[id]) {
            if (++visited > 120) return best;
            const target = this.units[id];
            if (!target.alive || target.routed) continue;
            const dx = target.x - u.x, dy = target.y - u.y, distance = dx * dx + dy * dy;
            if (distance > reach2) continue;
            const claims = Math.max(0, this._targetClaims[id] - (u.target === id ? 1 : 0));
            const preference = TYPES[u.type].missile ? TYPES[u.type].missile.armorPreference || 0 : 0;
            const score = distance * clamp(1 - TYPES[target.type].armor * preference, .55, 1.35) + Math.max(0, claims - 2) * 1700;
            if (score < bestScore) { bestScore = score; best = id; }
          }
        }
      }
      return best;
    }

    _updateApproachFields() {
      if (this.time < this._approachNext) return;
      this._approachNext = this.time + 1.1;
      const cols = this._navCols, rows = this._navRows, count = cols * rows;
      const blocked = this._clearanceGrid(8), scratch = this._routeScratch;
      const heap = scratch.heap, priorities = scratch.priority;
      const directions = [[1,0],[-1,0],[0,1],[0,-1],[1,1],[1,-1],[-1,1],[-1,-1]];
      for (let team = 0; team < 2; team++) {
        const field = this._approachFields[team]; field.fill(Infinity);
        let size = 0;
        const push = (id, priority) => {
          if (size >= heap.length) return;
          let at = size++;
          while (at > 0) { const above = (at - 1) >> 1; if (priorities[above] <= priority) break; heap[at] = heap[above]; priorities[at] = priorities[above]; at = above; }
          heap[at] = id; priorities[at] = priority;
        };
        const pop = () => {
          const id = heap[0], priority = priorities[0], tail = heap[--size], value = priorities[size];
          let at = 0;
          while (at * 2 + 1 < size) { let child = at * 2 + 1; if (child + 1 < size && priorities[child + 1] < priorities[child]) child++; if (priorities[child] >= value) break; heap[at] = heap[child]; priorities[at] = priorities[child]; at = child; }
          heap[at] = tail; priorities[at] = value;
          return [id, priority];
        };
        for (let cell = 0; cell < count; cell++) if (!blocked[cell] && this._cellCounts[1 - team][cell]) { field[cell] = 0; push(cell, 0); }
        while (size) {
          const [current, value] = pop();
          if (value > field[current] + .0001) continue;
          const x = current % cols, y = Math.floor(current / cols);
          for (let i = 0; i < directions.length; i++) {
            const xx = x + directions[i][0], yy = y + directions[i][1];
            if (xx < 0 || yy < 0 || xx >= cols || yy >= rows) continue;
            const next = yy * cols + xx;
            if (blocked[next] || (i >= 4 && (blocked[y * cols + xx] || blocked[yy * cols + x]))) continue;
            // Idle reserves pay for walking through already crowded friendly
            // cells. The field leads toward all real enemy fronts, allowing
            // them to circulate around an occupied line to another opening.
            const crowd = Math.max(0, this._cellCounts[team][next] - 7) * .16;
            const terrain = this._navTerrain[next] === 1 ? 1.38 : this._navTerrain[next] === 2 ? 1.15 : GROUND_COST[this._navTerrain[next]] || 1;
            const candidate = value + (i < 4 ? 1 : Math.SQRT2) * (terrain + crowd);
            if (candidate + .0001 < field[next]) { field[next] = candidate; push(next, candidate); }
          }
        }
        for (const [key, reservation] of this._approachReservations[team]) if (reservation.until < this.time) this._approachReservations[team].delete(key);
      }
    }

    _reserveContactGoal(u, enemy) {
      if (!enemy || !enemy.alive || enemy.routed) return null;
      const t = TYPES[u.type], reach = t.range + enemy._radius;
      const distance = Math.hypot(u.x - enemy.x, u.y - enemy.y);
      if (distance > reach + 180) return null;
      const angle = Math.atan2(u.y - enemy.y, u.x - enemy.x);
      const stand = Math.max(u._radius + enemy._radius + 2, reach * .87);
      const turns = [0, -.65, .65, -1.3, 1.3, -1.95, 1.95];
      const reservations = this._approachReservations[u.team];
      let best = null, bestScore = Infinity, bestKey = 0;
      for (let i = 0; i < turns.length; i++) {
        const turn = turns[(i + (u.id % 2)) % turns.length];
        const x = enemy.x + Math.cos(angle + turn) * stand, y = enemy.y + Math.sin(angle + turn) * stand;
        if (this._bodyBlocked(x, y, u._radius + 2) || !this._openBodySpot(u, x, y) || !this._openContactPath(u, x, y)) continue;
        const key = Math.round(y / 18) * Math.ceil(this.width / 18) + Math.round(x / 18);
        const reservation = reservations.get(key);
        if (reservation && reservation.id !== u.id && reservation.until > this.time) continue;
        const cell = clamp(Math.floor(y / CELL), 0, this._rows - 1) * this._cols + clamp(Math.floor(x / CELL), 0, this._cols - 1);
        const score = Math.hypot(x - u.x, y - u.y) + this._cellCounts[u.team][cell] * 3 + Math.abs(turn) * 8;
        if (score < bestScore) { bestScore = score; best = { x, y }; bestKey = key; }
      }
      if (best) reservations.set(bestKey, { id: u.id, until: this.time + 1.8 });
      return best;
    }

    _openBodySpot(u, x, y) {
      const reach = u._radius + this._maxRadius + 1;
      const minX = clamp(Math.floor((x - reach) / BODY_CELL), 0, this._bodyCols - 1), maxX = clamp(Math.floor((x + reach) / BODY_CELL), 0, this._bodyCols - 1);
      const minY = clamp(Math.floor((y - reach) / BODY_CELL), 0, this._bodyRows - 1), maxY = clamp(Math.floor((y + reach) / BODY_CELL), 0, this._bodyRows - 1);
      for (let team = 0; team < 2; team++) for (let cy = minY; cy <= maxY; cy++) for (let cx = minX; cx <= maxX; cx++) {
        for (let id = this._bodyHeads[team][cy * this._bodyCols + cx]; id !== -1; id = this._bodyNext[id]) {
          const other = this.units[id];
          if (id === u.id || !other.alive || other.routed) continue;
          const gap = (u._radius + other._radius) * .96 + .5;
          if ((x - other.x) ** 2 + (y - other.y) ** 2 < gap * gap) return false;
        }
      }
      return true;
    }

    _openContactPath(u, x, y) {
      const dx = x - u.x, dy = y - u.y, squared = dx * dx + dy * dy;
      if (squared < 1) return true;
      const margin = u._radius + this._maxRadius;
      const minX = clamp(Math.floor((Math.min(u.x, x) - margin) / BODY_CELL), 0, this._bodyCols - 1);
      const maxX = clamp(Math.floor((Math.max(u.x, x) + margin) / BODY_CELL), 0, this._bodyCols - 1);
      const minY = clamp(Math.floor((Math.min(u.y, y) - margin) / BODY_CELL), 0, this._bodyRows - 1);
      const maxY = clamp(Math.floor((Math.max(u.y, y) + margin) / BODY_CELL), 0, this._bodyRows - 1);
      for (let team = 0; team < 2; team++) for (let cy = minY; cy <= maxY; cy++) for (let cx = minX; cx <= maxX; cx++) {
        for (let id = this._bodyHeads[team][cy * this._bodyCols + cx]; id !== -1; id = this._bodyNext[id]) {
          const other = this.units[id];
          if (id === u.id || !other.alive || other.routed) continue;
          const ox = u.x - other.x, oy = u.y - other.y;
          const along = clamp(-(ox * dx + oy * dy) / squared, 0, 1);
          const near = (ox + dx * along) ** 2 + (oy + dy * along) ** 2;
          const gap = (u._radius + other._radius) * .96;
          if (near < gap * gap - .01 && near < ox * ox + oy * oy - .01) return false;
        }
      }
      return true;
    }

    _circulationGoal(u, r, original) {
      if (u._circulateNext > this.time && Math.hypot(u._circulateX - u.x, u._circulateY - u.y) > 12) return { x: u._circulateX, y: u._circulateY };
      u._circulateNext = this.time + .65 + u._jitter * .35;
      const field = this._approachFields[u.team], cols = this._navCols, rows = this._navRows;
      const cx = clamp(Math.floor(u.x / NAV), 0, cols - 1), cy = clamp(Math.floor(u.y / NAV), 0, rows - 1);
      const charging = r.order === 'charge';
      const chargeEnemy = charging ? this.regiments[r._chargeTargetId] : null;
      const chargePoint = chargeEnemy ? { x: clamp(u.x, chargeEnemy._actualMinX, chargeEnemy._actualMaxX), y: clamp(u.y, chargeEnemy._actualMinY, chargeEnemy._actualMaxY) } : original;
      const current = charging ? Math.hypot(chargePoint.x - u.x, chargePoint.y - u.y) / NAV : field[cy * cols + cx];
      if (!Number.isFinite(current)) return original;
      // The approach field includes congestion and terrain costs; it is not
      // distance. Let nearby reserves try the helper's real-distance bound
      // even when the crowded cell has a large field value.
      // Target load balancing may select a defender behind the exposed rank.
      // A reserve must seek actual room beside the nearest physical contact
      // before treating that inner target as a walking destination.
      const contactId = r.order === 'auto' ? this._nearestEnemy(u, Math.min(115, TYPES[u.type].range + 85)) : -1;
      let goal = contactId >= 0 ? this._reserveContactGoal(u, this.units[contactId]) : null;
      if (!goal) goal = this._reserveContactGoal(u, this.units[u.target]);
      if (!goal) {
        let best = Infinity;
        const directions = [[1,0],[-1,0],[0,1],[0,-1],[1,1],[1,-1],[-1,1],[-1,-1]];
        for (let k = 0; k < directions.length; k++) {
          const direction = directions[(k + u.id) % directions.length], dx = direction[0], dy = direction[1];
          const xx = cx + dx, yy = cy + dy;
          if (xx < 0 || yy < 0 || xx >= cols || yy >= rows) continue;
          const cell = yy * cols + xx;
          if (!charging && !Number.isFinite(field[cell])) continue;
          // Keep the current position within its grid cell. Adjacent soldiers
          // therefore keep separate paths instead of sharing the cell center.
          const x = u.x + dx * NAV, y = u.y + dy * NAV;
          if (this._bodyBlocked(x, y, u._radius + 3) || !this._lineClear(u.x, u.y, x, y, u._radius + 3)) continue;
          const step = dx && dy ? Math.SQRT2 : 1;
          if (!this._openBodySpot(u, x, y)) continue;
          if (u._jamTime > 1.2 && !this._openBodySpot(u, u.x + dx / step * 22, u.y + dy / step * 22)) continue;
          const score = charging ? Math.hypot(x - chargePoint.x, y - chargePoint.y) / NAV + step * .5 + this._cellCounts[u.team][cell] * .18 : field[cell] + step * .65 + this._cellCounts[u.team][cell] * .06;
          if (score < best) { best = score; goal = { x, y }; }
        }
      }
      if (!goal) return original;
      u._circulateX = goal.x; u._circulateY = goal.y;
      return goal;
    }

    _assignEngagements() {
      const centers = [{ x: 0, y: 0, n: 0 }, { x: 0, y: 0, n: 0 }];
      for (const r of this.regiments) if (r.alive > 0) {
        const c = centers[r.team]; c.x += r._centroidX * r.alive; c.y += r._centroidY * r.alive; c.n += r.alive;
      }
      for (const c of centers) { c.x /= Math.max(1, c.n); c.y /= Math.max(1, c.n); }
      const dx = centers[1].x - centers[0].x, dy = centers[1].y - centers[0].y, length = Math.hypot(dx, dy);
      const initialX = length > 1e-6 ? dx / length : 1, initialY = length > 1e-6 ? dy / length : 0;
      // The deployment defines the army front. Recomputing this axis from
      // casualty-shifted centers after contact would rotate entire ranks as
      // the distance between armies approaches zero.
      if (!this._battleAxis && this.phase !== 'deployment') this._battleAxis = { x: initialX, y: initialY };
      const axisX = this._battleAxis ? this._battleAxis.x : initialX;
      const axisY = this._battleAxis ? this._battleAxis.y : initialY;
      const load = new Float64Array(this.regiments.length);
      const faceLoad = new Map();
      const assignments = new Map(), supportGroups = new Map();
      for (let team = 0; team < 2; team++) {
        const nx = team === 0 ? axisX : -axisX, ny = team === 0 ? axisY : -axisY, tx = -ny, ty = nx;
        const attackers = this.regiments.filter(r => r.team === team && r.alive > 0);
        attackers.sort((a, b) => Number(TYPES[a.type].ranged) - Number(TYPES[b.type].ranged) || (b.x - a.x) * nx + (b.y - a.y) * ny || a.id - b.id);
        for (const r of attackers) {
          const t = TYPES[r.type];
          let best = null, bestScore = Infinity, nearestDistance = Infinity, contactDistance = Infinity;
          const tactic = r.order === 'auto' && !r.fallingBack && r._rallyTime <= 0 ? r._tactic : null;
          const tacticalTravel = !!(tactic && (tactic.transit || tactic.hold));
          const pointAttack = r.order === 'attack' && r._hasDestination;
          const orderDX = r.targetX - r._centroidX, orderDY = r.targetY - r._centroidY, orderLength = Math.max(1, Math.hypot(orderDX, orderDY));
          const orderNX = orderDX / orderLength, orderNY = orderDY / orderLength;
          let routeEnemy = null, routeDistance = Infinity;
          for (const other of this.regiments) {
            if (other.team === team || other.alive <= 0) continue;
            const ox = other._centroidX - r._centroidX, oy = other._centroidY - r._centroidY;
            const distance = Math.hypot(ox, oy);
            nearestDistance = Math.min(nearestDistance, distance);
            const gapX = Math.max(0, r._actualMinX - other._actualMaxX, other._actualMinX - r._actualMaxX);
            const gapY = Math.max(0, r._actualMinY - other._actualMaxY, other._actualMinY - r._actualMaxY);
            contactDistance = Math.min(contactDistance, Math.hypot(gapX, gapY));
            if (pointAttack && ox * orderNX + oy * orderNY > Math.max(24, r.depth * .12)) {
              const side = this._projectFrontBounds(other, -orderNY, orderNX);
              const lane = r._centroidX * -orderNY + r._centroidY * orderNX;
              const halfLane = Math.max(28, Math.min(90, r.width * .3));
              const front = this._projectFrontBounds(other, orderNX, orderNY).lo - (r._centroidX * orderNX + r._centroidY * orderNY);
              if (side.hi >= lane - halfLane && side.lo <= lane + halfLane && front < Math.max(t.range + 65, r.depth * .5 + 75) && front < routeDistance) {
                routeEnemy = other; routeDistance = front;
              }
            }
            const capacity = Math.max(other.initial * .5, other.alive, 1);
            const frame = this._approachFrame(r, other, nx, ny);
            const committedHere = r.order === 'auto' ? faceLoad.get(other.id + ':' + frame.sector) || 0 : load[other.id];
            const saturation = committedHere / capacity;
            const lateral = Math.abs(-ox * frame.ny + oy * frame.nx);
            const prior = other.id === r._enemyId;
            const stable = prior ? (r._combatActive || this.time < r._assignmentUntil ? 520 : 160) : 0;
            const score = distance + lateral * .5 + saturation * 900 - stable + (TYPES[other.type].commander ? 320 : 0) + (other.fallingBack && !t.mounted ? 120 : 0);
            if (score < bestScore) { bestScore = score; best = other; }
          }
          const priorEnemy = this.regiments[r._enemyId];
          if (priorEnemy && priorEnemy.alive > 0 && !priorEnemy.fallingBack && (r._combatActive || this.time < r._assignmentUntil)) {
            const priorDistance = Math.hypot(priorEnemy._centroidX - r._centroidX, priorEnemy._centroidY - r._centroidY);
            const priorFrame = this._approachFrame(r, priorEnemy, nx, ny);
            const occupied = r.order === 'auto' ? faceLoad.get(priorEnemy.id + ':' + priorFrame.sector) || 0 : load[priorEnemy.id];
            if (priorDistance < nearestDistance + 500 && (occupied < Math.max(priorEnemy.initial * .5, priorEnemy.alive) * 1.8 || (r._localFront && priorDistance < 650))) best = priorEnemy;
          }
          r._attackBlocked = !!routeEnemy;
          if (routeEnemy) best = routeEnemy;
          if (r.order === 'charge' && !r.fallingBack && r._rallyTime <= 0) {
            let charged = this.regiments[r._chargeTargetId];
            if (!charged || charged.alive <= 0 || charged._routed) {
              charged = this._nearestChargeTarget(r);
              this._setChargeTarget(r, charged);
              for (const id of r.unitIds) {
                const u = this.units[id];
                this._setTarget(u, -1); u._sense = 0;
                u._localPath.length = 0; u._localTimer = 0; u._portalCorner = null;
                u._circulateNext = 0; u._circulateUntil = 0;
              }
            }
            best = charged;
          }
          const plannedTarget = tactic && this.regiments[tactic.targetId];
          if (plannedTarget && plannedTarget.team !== r.team && plannedTarget.alive > 0 && !plannedTarget._routed && !plannedTarget.fallingBack) best = plannedTarget;
          else if (tactic && tactic.plan === 'aggressive' && best) tactic.targetId = best.id;
          if (best && r._enemyId !== best.id) { r._assignmentUntil = this.time + 7; r._combatSlotsTime = -10; }
          r._enemyId = best ? best.id : -1;
          r._enemyDistance = best ? Math.hypot(best._centroidX - r._centroidX, best._centroidY - r._centroidY) : Infinity;
          r._nearestEnemyDistance = nearestDistance; r._contactDistance = contactDistance;
          const frame = best ? this._approachFrame(r, best, nx, ny) : { nx, ny, local: false, sector: 0 };
          if (best) {
            r._approachNX = frame.nx; r._approachNY = frame.ny; r._localFront = frame.local; r._approachEnemy = best.id;
          }
          const committed = r.fallingBack || r._rallyTime > 0 || r.order === 'move' || r.order === 'face' || tacticalTravel ? 0 : r.order === 'hold' ? (nearestDistance < t.range + 150 ? .1 : 0) : t.commander ? .03 : t.ranged ? (t.range < 300 ? .85 : .55) : 1;
          if (best) {
            load[best.id] += r.alive * committed;
            const key = best.id + ':' + frame.sector;
            faceLoad.set(key, (faceLoad.get(key) || 0) + r.alive * committed);
          }
          r.tacticalRole = r.fallingBack ? 'withdraw' : r._rallyTime > 0 ? 'reserve' : r.order === 'charge' ? 'charge' : t.commander ? 'command' : r.order === 'hold' ? 'hold' : r.order === 'move' ? 'advance' : t.ranged ? 'support' : 'line';
          const wasDeployed = r._combatActive || r._missileActive;
          r._sharedChargeLane = false;
          const assignedGap = best ? Math.hypot(Math.max(0, r._actualMinX - best._actualMaxX, best._actualMinX - r._actualMaxX), Math.max(0, r._actualMinY - best._actualMaxY, best._actualMinY - r._actualMaxY)) : Infinity;
          const mountedCharge = r.order === 'charge' && r._tightMountedMarch && this.impacts && this.impacts.isHorse(r);
          // Keep densely packed riders in their captured lanes until the actual
          // front is near the enemy. A distant centroid used to release every
          // row toward new combat slots hundreds of units before contact.
          const contactReady = mountedCharge ? assignedGap < t.range + 125 || contactDistance < t.range + 70 :
            r.order === 'auto' ? assignedGap < t.range + 125 || r._enemyDistance < Math.max(220, r.depth * .5 + (best ? best.depth : 0) * .5 + t.range + 115) : r._enemyDistance < 670 || contactDistance < 260;
          r._missileActive = !!best && !tacticalTravel && t.ranged && !r.fallingBack && r._rallyTime <= 0 && r.order !== 'move' && r.order !== 'hold' && r.order !== 'face' && !this._advancingAttack(r) && (r._enemyDistance < t.range + 420 || contactDistance < t.range + 180);
          r._combatActive = !!best && !tacticalTravel && !t.ranged && (!t.commander || r.order === 'charge') && !r.fallingBack && r._rallyTime <= 0 && r.order !== 'move' && r.order !== 'hold' && r.order !== 'face' && !this._advancingAttack(r) && contactReady;
          if (r._combatActive || r._missileActive) r._marchLayout = false;
          else if (wasDeployed && r.order === 'auto' && !r.fallingBack && r._rallyTime <= 0) this._captureMarchLayout(r);
          if (!best) continue;
          let frontNX = r.order === 'auto' ? frame.nx : nx, frontNY = r.order === 'auto' ? frame.ny : ny;
          if (r.order === 'charge') {
            if (r._enemyDistance > 500) {
              const dx = best._centroidX - r._centroidX, dy = best._centroidY - r._centroidY, d = Math.max(1, Math.hypot(dx, dy));
              r._chargeNX = dx / d; r._chargeNY = dy / d;
            }
            frontNX = r._chargeNX; frontNY = r._chargeNY;
          } else if (pointAttack && orderLength > 1) {
            frontNX = orderNX; frontNY = orderNY;
          }
          r._frontNX = frontNX; r._frontNY = frontNY; r._frontTX = -frontNY; r._frontTY = frontNX;
          if (!t.ranged && (!t.commander || r.order === 'charge') && !r.fallingBack && r._rallyTime <= 0 && r.order !== 'move' && r.order !== 'hold' && r.order !== 'face' && (r.order !== 'auto' || r._combatActive)) {
            const key = r.order === 'charge' || pointAttack ? r.order + ':' + best.id + ':' + Math.round(Math.atan2(frontNY, frontNX) / (Math.PI / 2)) : 'auto:' + best.id + ':' + frame.sector;
            if (!assignments.has(key)) assignments.set(key, []);
            assignments.get(key).push(r);
          } else {
            const fnx = r._frontNX, fny = r._frontNY, ftx = r._frontTX, fty = r._frontTY;
            const normal = best._centroidX * fnx + best._centroidY * fny - (t.ranged ? t.range * .72 : best.depth * .5 + 35);
            const lateral = best._centroidX * ftx + best._centroidY * fty;
            r.frontX = normal * fnx + lateral * ftx; r.frontY = normal * fny + lateral * fty;
            r.frontageWidth = r.width;
            if (t.ranged && !tacticalTravel && !r.fallingBack && r._rallyTime <= 0 && r.order !== 'move' && r.order !== 'hold' && r.order !== 'face' && !this._advancingAttack(r)) {
              const key = r.order === 'charge' || pointAttack ? r.order + ':' + best.id + ':' + Math.round(Math.atan2(frontNY, frontNX) / (Math.PI / 2)) : 'auto:' + best.id + ':' + frame.sector;
              if (!supportGroups.has(key)) supportGroups.set(key, []);
              supportGroups.get(key).push(r);
            }
          }
        }
      }
      for (const [, group] of assignments) {
        const enemyId = group[0]._enemyId, enemy = this.regiments[enemyId];
        const first = group[0], nx = first._frontNX, ny = first._frontNY, tx = -ny, ty = nx;
        const across = this._projectFrontBounds(enemy, tx, ty), forward = this._projectFrontBounds(enemy, nx, ny);
        const enemyWidth = Math.max(enemy.width * .65, across.hi - across.lo, 32);
        const enemyDepth = Math.max(enemy.depth * .55, forward.hi - forward.lo, 28);
        const centerT = enemy._centroidX * tx + enemy._centroidY * ty;
        group.sort((a, b) => (a._centroidX - b._centroidX) * tx + (a._centroidY - b._centroidY) * ty || a.id - b.id);
        // Neighboring wings can share a heading long before their front
        // ranks touch. Keep their approach files apart until combat deployment
        // actually needs the narrower sectors on the enemy front.
        if (first.order === 'charge' && group.length > 1 && this.impacts) {
          let previous = null;
          for (const r of group) {
            if (!this.impacts.isHorse(r) || TYPES[r.type].ranged || !r._marchLayout || r._combatActive ||
                r._traffic || r._bridge || r._terrainPressure >= .2 || r._contactDistance <= TYPES[r.type].range + 220) continue;
            if (previous && r._chargeNX * previous._chargeNX + r._chargeNY * previous._chargeNY > .86) {
              const dx = r._centroidX - previous._centroidX, dy = r._centroidY - previous._centroidY;
              const clearance = Math.max(r._spacing, previous._spacing) * 2;
              if (Math.abs(dx * nx + dy * ny) <= (r.depth + previous.depth) / 2 + clearance &&
                  Math.abs(dx * tx + dy * ty) <= (r.width + previous.width) / 2 + clearance) {
                const lane = previous._sharedChargeLane || { x: previous._chargeNX, y: previous._chargeNY };
                lane.x += r._chargeNX; lane.y += r._chargeNY;
                previous._sharedChargeLane = r._sharedChargeLane = lane;
              }
            }
            previous = r;
          }
        }
        const desired = group.map(r => {
          const t = TYPES[r.type], rows = r.formation === 'shieldwall' ? 8 : t.mounted ? 4 : 6;
          r._combatSpacing = r._spacing * ((FORMATIONS[r.formation] || {}).spacing || 1);
          return Math.min(t.mounted ? 600 : 520, Math.max(r.width, Math.ceil(r.alive / rows) * r._combatSpacing));
        });
        const total = desired.reduce((sum, value) => sum + value, 0);
        const coverage = Math.max(enemyWidth, Math.min(total, enemyWidth * 1.6));
        let cursor = centerT - coverage / 2;
        for (let i = 0; i < group.length; i++) {
          const r = group[i], t = TYPES[r.type];
          const share = coverage * desired[i] / Math.max(1, total);
          let lateral = cursor + share / 2; cursor += share;
          let normal = forward.lo - Math.max(t.radius * 2 + 2, t.range * .78);
          let fnx = nx, fny = ny, ftx = tx, fty = ty;
          // A lone formed wing keeps its useful approach frontage within its
          // allocated contact sector. Facing an enemy's shallow flank must
          // not squeeze the whole wing into extra-deep files before contact.
          const singleWidth = Math.max(enemyWidth + 40, Math.min(r.width, coverage));
          let width = Math.max(r._spacing * 2, Math.min(desired[i], group.length === 1 ? singleWidth : share + 24));
          const flank = group.length >= 3 && total > enemyWidth * 1.55 && (i === 0 || i === group.length - 1);
          if (flank && r.order !== 'hold') {
            const side = i === 0 ? -1 : 1;
            fnx = -tx * side; fny = -ty * side; ftx = -fny; fty = fnx;
            const faceT = centerT + side * (enemyWidth / 2 + Math.max(t.radius * 2 + 2, t.range * .78));
            const faceN = (forward.lo + forward.hi) / 2;
            r.frontX = faceN * nx + faceT * tx; r.frontY = faceN * ny + faceT * ty;
            width = Math.min(desired[i], enemyDepth + 70);
            r.tacticalRole = r.order === 'charge' ? 'charge' : 'flank';
          } else {
            if (group.length === 1) lateral = centerT;
            r.frontX = normal * nx + lateral * tx; r.frontY = normal * ny + lateral * ty;
            if (Math.abs((r._centroidX * tx + r._centroidY * ty) - lateral) > r.width && r.tacticalRole === 'line') r.tacticalRole = 'flank';
          }
          r._frontNX = fnx; r._frontNY = fny; r._frontTX = ftx; r._frontTY = fty;
          r.frontageWidth = width;
          const columns = Math.max(2, Math.min(r.alive, Math.round(width / r._combatSpacing)));
          // Keep file membership while the contact front breathes. Periodic
          // re-sorting made neighbors trade places even in a stable fight.
          const widthChanged = Math.abs(columns - r._combatColumns) >= Math.max(2, Math.ceil(r._combatColumns * .18));
          if (r._combatActive && (r._combatEnemy !== enemyId || widthChanged || r._combatAssignedCount - r.alive > Math.max(4, r.alive * .1))) {
            r._combatColumns = columns; this._assignCombatSlots(r); r._combatEnemy = enemyId;
          }
          if (r._combatActive) this._updateContactLanes(r, enemy);
        }
      }
      for (const [, group] of supportGroups) {
        const enemy = this.regiments[group[0]._enemyId], nx = group[0]._frontNX, ny = group[0]._frontNY, tx = -ny, ty = nx;
        const across = this._projectFrontBounds(enemy, tx, ty), forward = this._projectFrontBounds(enemy, nx, ny);
        const enemyWidth = Math.max(enemy.width * .65, across.hi - across.lo, 32);
        const centerT = enemy._centroidX * tx + enemy._centroidY * ty;
        group.sort((a, b) => (a._centroidX - b._centroidX) * tx + (a._centroidY - b._centroidY) * ty || a.id - b.id);
        const desired = group.map(r => {
          const t = TYPES[r.type], profile = t.missile || { ranks: 4, spacing: 1, maxWidth: 600 };
          const form = FORMATIONS[r.formation] || {};
          r._missileSpacing = Math.max(t.radius * 2 + 5, r._spacing * profile.spacing * (form.spacing || 1));
          // Planted bow and bolt files leave body-sized intervals for heavy
          // foot troops as well as the wider packet corridors. Otherwise a
          // stationary screen remains a wall between those few corridors.
          if (!t.siege && !t.mounted && profile.reloadStep === 0) r._missileSpacing = Math.max(r._missileSpacing, (t.radius + TYPES.guardian.radius) * 2 + 1);
          r._missileGap = t.mounted || t.siege ? 0 : 38;
          const columns = Math.ceil(r.alive / profile.ranks);
          const width = columns * r._missileSpacing + Math.floor((columns - 1) / 5) * r._missileGap;
          return Math.max(r._missileSpacing * 2, Math.min(profile.maxWidth, width, enemyWidth + t.range * .85));
        });
        const total = desired.reduce((sum, width) => sum + width + 25, 0);
        // Bound the support arc. Endless side-by-side reservations sent the
        // outside companies beyond weapon reach and into diagonal queues.
        const coverage = Math.min(total, enemyWidth + Math.max(...group.map(r => TYPES[r.type].range)) * .85);
        let cursor = centerT - coverage / 2;
        for (let i = 0; i < group.length; i++) {
          const r = group[i], t = TYPES[r.type], profile = t.missile || { standoff: .74 };
          const share = coverage * (desired[i] + 25) / Math.max(1, total);
          const lateral = group.length === 1 ? centerT : cursor + share / 2;
          cursor += share;
          const form = FORMATIONS[r.formation] || {};
          let columns = form.files ? Math.min(r.alive, form.files) : r.type === 'musketeer' ? Math.ceil(r.alive / 3) : form.ranks ? Math.ceil(r.alive / form.ranks) : Math.max(1, Math.min(r.alive, Math.floor(desired[i] / r._missileSpacing)));
          if (r.type !== 'musketeer' && !form.files && !form.ranks) while (columns > 1 && columns * r._missileSpacing + Math.floor((columns - 1) / 5) * r._missileGap > desired[i]) columns--;
          const rows = Math.ceil(r.alive / columns);
          r._missileRowSpacing = Math.max(t.radius * 2 + 4, Math.min(r._missileSpacing, t.range * .6 / Math.max(1, rows - 1)));
          const stand = Math.max(60, t.range * profile.standoff);
          const normal = forward.lo - stand - (group.length > 1 ? (i % 2) * r._missileSpacing * .5 : 0);
          r.frontX = normal * nx + lateral * tx; r.frontY = normal * ny + lateral * ty;
          let firing = 0;
          for (const id of r.unitIds) {
            const u = this.units[id];
            if (u.alive && !u.routed && u._fireReachEnemy === enemy.id && this.time - u._fireReachTime < .8) firing++;
          }
          if (firing >= Math.max(1, Math.ceil(r.alive * .2))) { r._fireLastInRange = this.time; r._fireEnemy = enemy.id; }
          if (r._fireLocked && (r._fireEnemy !== enemy.id || this.time - r._fireLastInRange > 2.4)) r._fireLocked = false;
          if (!r._fireLocked && r._missileActive && r._fireEnemy === enemy.id && this.time - r._fireLastInRange < .7) {
            r._fireLocked = true; r._fireAnchorX = r.frontX; r._fireAnchorY = r.frontY;
          }
          if (r._fireLocked) { r.frontX = r._fireAnchorX; r.frontY = r._fireAnchorY; }
          r.frontageWidth = columns * r._missileSpacing + Math.floor((columns - 1) / 5) * r._missileGap;
          r._missileAcrossLo = across.lo; r._missileAcrossHi = across.hi; r._missileEnemyFront = forward.lo;
          if (r._missileActive && (r._missileColumns !== columns || r._missileEnemy !== enemy.id || r._missileAssignedCount - r.alive > Math.max(4, r.alive * .1))) {
            r._missileColumns = columns; r._missileRows = rows; this._assignMissileSlots(r); r._missileEnemy = enemy.id;
          }
        }
      }
      this._targetClaims.fill(0);
      for (const u of this.units) if (u.alive && !u.routed && u.target >= 0) {
        const target = this.units[u.target];
        if (target && target.alive && !target.routed) this._targetClaims[u.target]++;
      }
    }

    _approachFrame(r, enemy, axisX, axisY) {
      let nx = axisX, ny = axisY, local = false;
      if (r.order === 'auto') {
        const dx = enemy._centroidX - r._centroidX, dy = enemy._centroidY - r._centroidY, distance = Math.max(1, Math.hypot(dx, dy));
        local = (r._approachEnemy === enemy.id && r._localFront) || (dx * axisX + dy * axisY) / distance < .65;
        if (local) {
          if (r._approachEnemy === enemy.id && r._localFront) { nx = r._approachNX; ny = r._approachNY; }
          else { nx = dx / distance; ny = dy / distance; }
        }
      }
      const sector = (Math.round(Math.atan2(ny, nx) / (Math.PI / 4)) + 8) % 8;
      return { nx, ny, local, sector };
    }

    _assignCombatSlots(r) {
      const columns = r._combatColumns;
      const soldiers = r.unitIds.map(id => this.units[id]).filter(u => u.alive && !u.routed);
      soldiers.sort((a, b) => (a.x - b.x) * r._frontTX + (a.y - b.y) * r._frontTY || a.id - b.id);
      const lanes = Array.from({ length: columns }, () => []);
      for (let i = 0; i < soldiers.length; i++) lanes[Math.min(columns - 1, Math.floor(i * columns / Math.max(1, soldiers.length)))].push(soldiers[i]);
      let rows = 1;
      for (let col = 0; col < columns; col++) {
        lanes[col].sort((a, b) => (b.x - a.x) * r._frontNX + (b.y - a.y) * r._frontNY || a.id - b.id);
        for (let row = 0; row < lanes[col].length; row++) {
          lanes[col][row]._combatSlot = row * columns + col;
          lanes[col][row]._pressColumn = -1; lanes[col][row]._pressUntil = 0;
        }
        rows = Math.max(rows, lanes[col].length);
      }
      r._combatRows = rows; r._combatSlotsTime = this.time; r._combatAssignedCount = r.alive;
    }

    _assignMissileSlots(r) {
      const columns = r._missileColumns, nx = r._frontNX, ny = r._frontNY, tx = -ny, ty = nx;
      const soldiers = r.unitIds.map(id => this.units[id]).filter(u => u.alive && !u.routed);
      soldiers.sort((a, b) => (a.x - b.x) * tx + (a.y - b.y) * ty || a.id - b.id);
      const lanes = Array.from({ length: columns }, () => []);
      for (let i = 0; i < soldiers.length; i++) lanes[Math.min(columns - 1, Math.floor(i * columns / Math.max(1, soldiers.length)))].push(soldiers[i]);
      let rows = 1;
      for (let col = 0; col < columns; col++) {
        lanes[col].sort((a, b) => (b.x - a.x) * nx + (b.y - a.y) * ny || a.id - b.id);
        for (let row = 0; row < lanes[col].length; row++) lanes[col][row]._missileSlot = row * columns + col;
        rows = Math.max(rows, lanes[col].length);
      }
      r._missileRows = rows; r._missileAssignedCount = r.alive; r._missileSlotsTime = this.time;
      if (r.type === 'musketeer') this._assignMusketRanks(r, true);
    }

    _assignMusketRanks(r, missiles) {
      if(r._rampartJob){let ranks=1;for(const id of r.unitIds){const u=this.units[id];u._volleyRank=u._rampartRank;ranks=Math.max(ranks,u._volleyRank+1);}r.volleyRankCount=ranks;r._volleyNext%=ranks;r.nextVolleyRank=r._volleyNext+1;return;}
      const form = FORMATIONS[r.formation] || {};
      const columns = missiles ? r._missileColumns : form.files ? Math.min(r.alive, form.files) : form.ranks ? Math.ceil(r.alive / form.ranks) : r.formation === 'wedge' ? r._shapeColumns : Math.ceil(r.alive / 3);
      const wedge = !missiles && r.formation === 'wedge';
      let ranks = 1, liveSlot = 0;
      for (const id of r.unitIds) {
        const u = this.units[id];
        if (!u.alive || u.routed) continue;
        // Casualties are resolved every step; the slower morale pass must not
        // leave holes in compact slot numbers and invent a fourth volley rank.
        if (!missiles) u._slot = liveSlot++;
        const slot = missiles && u._missileSlot >= 0 ? u._missileSlot : u._slot;
        u._volleyRank = wedge ? Math.floor((Math.sqrt(8 * slot + 1) - 1) / 2) : Math.floor(slot / Math.max(1, columns));
        ranks = Math.max(ranks, u._volleyRank + 1);
      }
      r.volleyRankCount = ranks;
      r._volleyNext %= ranks; r.nextVolleyRank = r._volleyNext + 1;
      r._volleyLayout = [missiles ? 'fire' : 'march', r.formation, columns, r.alive].join(':');
    }

    _musketMelee(u, enemy) {
      if(enemy&&Math.abs(this.unitAltitude(u)-this.unitAltitude(enemy))>18)return;
      if (!enemy || !enemy.alive || enemy.routed || u._meleeCooldown > 0 || Math.hypot(enemy.x - u.x, enemy.y - u.y) > 32 + enemy._radius || !this._meleeLineClear(u,enemy)) return;
      u.angle = Math.atan2(enemy.y - u.y, enemy.x - u.x);
      u._meleeCooldown = 1.7; u._lastMusketMelee = u._lastAttack = this.time; u.attackFlash = 1; u.state = 'attack';
      this._hit(u, enemy, 9, false, false);
    }

    _stepMusketVolleys() {
      for (const r of this.regiments) {
        if (r.type !== 'musketeer' || r.alive <= 0 || r._routed) continue;
        const t = TYPES.musketeer, missiles = this._canDeployMissiles(r);
        const columns = missiles ? r._missileColumns : (FORMATIONS[r.formation] || {}).files || ((FORMATIONS[r.formation] || {}).ranks ? Math.ceil(r.alive / FORMATIONS[r.formation].ranks) : r.formation === 'wedge' ? r._shapeColumns : Math.ceil(r.alive / 3));
        const layout = [missiles ? 'fire' : 'march', r.formation, columns, r.alive].join(':');
        if (layout !== r._volleyLayout) this._assignMusketRanks(r, missiles);
        const soldiers = r.unitIds.map(id => this.units[id]).filter(u => u.alive && !u.routed&&(!r._rampartJob||(u._onRampart&&!u._rampartPath.length&&!r._rampartJob.leaving)));
        if(r._rampartJob)this._assignMusketRanks(r,true);
        const rankMembers = Array.from({ length: r.volleyRankCount }, () => []);
        for (const u of soldiers) rankMembers[u._volleyRank].push(u);
        let contact = false;
        for (const u of soldiers) {
          const id = this._nearestEnemy(u, 42);
          if (id >= 0 && Math.abs(this.unitAltitude(u)-this.unitAltitude(this.units[id]))<=18 && this._meleeLineClear(u,this.units[id])) {
            contact = true;
            this._musketMelee(u, this.units[id]);
          }
        }
        if (contact) {
          r._volleyBrokenUntil = this.time + 3;
          r.volleyStatus = 'melee'; r._fireLocked = false;
          r.nextVolleyAt = Math.max(r.nextVolleyAt, r._volleyBrokenUntil);
          continue;
        }
        if (this.time < r._volleyBrokenUntil) { r.volleyStatus = 'reforming'; continue; }
        if (r.fallingBack || r._rallyTime > 0 || r.order === 'move' || r.order === 'face' || this._advancingAttack(r) || r._frontTurning || r._traffic || r._bridge) {
          r.volleyStatus = 'marching'; continue;
        }
        if (this.time + 1e-8 < r.nextVolleyAt) { r.volleyStatus = 'reloading'; continue; }
        // A row is a single firing transaction. Soldiers never acquire an
        // individual permission window that could trickle shots over frames.
        // Empty/out-of-range rows are skipped, and only one row fires per step.
        let fired = false;
        for (let attempt = 0; attempt < r.volleyRankCount && !fired; attempt++) {
          const rank = (r._volleyNext + attempt) % r.volleyRankCount;
          const members = rankMembers[rank];
          if (!members.length) continue;
          if(r._rampartJob&&r.unitIds.some(id=>{const u=this.units[id];return u.alive&&!u.routed&&u._volleyRank===rank&&(!u._onRampart||u._rampartPath.length);}))continue;
          const ready = members.every(u => u._cooldown <= 1e-8 && u._impactUntil <= this.time && u._vx * u._vx + u._vy * u._vy <= 25);
          if (!ready) continue;
          const shots = [];
          for (const u of members) {
            let target = this.units[u.target];
            const reach = t.range * (u._terrain.elevation ? 1.12 : 1);
            if (!target || !target.alive || target.routed || Math.hypot(target.x - u.x, target.y - u.y) > reach) target = this.units[this._rangedLocalTarget(u, reach)];
            if (target && target.alive && !target.routed && Math.hypot(target.x - u.x, target.y - u.y) <= reach) shots.push([u, target]);
          }
          if (shots.length < Math.ceil(members.length * .5)) continue;
          // Edge files discharge with their row even when their line of aim
          // ends short of an enemy. The actual projectile still travels only
          // to weapon range and can hit only a body crossing that path.
          const reference = shots[Math.floor(shots.length / 2)][1];
          const armed = new Set(shots.map(shot => shot[0].id));
          for (const u of members) if (!armed.has(u.id)) {
            const dx = reference.x - u.x, dy = reference.y - u.y, distance = Math.hypot(dx, dy) || 1;
            const reach = t.range * (u._terrain.elevation ? 1.12 : 1) * .98;
            shots.push([u, { id: -1, x: u.x + dx / distance * reach, y: u.y + dy / distance * reach, _vx: 0, _vy: 0, _radius: 0 }]);
          }
          r._volleyDispatch = true;
          for (const [u, target] of shots) this._attack(u, target);
          r._volleyDispatch = false;
          r.lastVolleyAt = this.time; r.lastVolleyRank = rank + 1;
          r._volleyNext = (rank + 1) % r.volleyRankCount; r.nextVolleyRank = r._volleyNext + 1;
          r.nextVolleyAt = this.time + t.cooldown / r.volleyRankCount;
          r.volleyStatus = 'reloading'; fired = true;
        }
        if (!fired) r.volleyStatus = soldiers.some(u => u._cooldown > 0) ? 'reloading' : 'ready';
      }
    }

    _canDeployMissiles(r) {
      if (this._inUnitStep) return r._deployMissiles;
      if (!r._missileActive || r._traffic || r._bridge || r.fallingBack || r._rallyTime > 0 || r.order === 'move' || r.order === 'hold' || r.order === 'face' || this._advancingAttack(r)) return false;
      return !r._routeDetour || r._nearestEnemyDistance < TYPES[r.type].range + 120;
    }

    _missileGoal(u, r, rigid) {
      const t = TYPES[u.type], slot = u._missileSlot >= 0 ? u._missileSlot : u._slot;
      const col = slot % r._missileColumns, row = Math.floor(slot / r._missileColumns), spacing = r._missileSpacing;
      // Groups of five files leave real corridors for allied reserves. Body
      // collision still applies; a planted firing line must not be a wall.
      const lateral = (col - (r._missileColumns - 1) / 2) * spacing + (Math.floor(col / 5) - Math.floor((r._missileColumns - 1) / 5) / 2) * r._missileGap;
      const rowSpacing = r._missileRowSpacing || spacing, halfDepth = (r._missileRows - 1) * rowSpacing / 2;
      let advance = halfDepth - row * rowSpacing;
      const absoluteSide = r.frontX * r._frontTX + r.frontY * r._frontTY + lateral;
      const overhang = Math.max(0, r._missileAcrossLo - absoluteSide, absoluteSide - r._missileAcrossHi);
      const reach = t.range * .94;
      const depthBudget = Math.sqrt(Math.max(reach * reach * .25, reach * reach - overhang * overhang));
      const normal = r.frontX * r._frontNX + r.frontY * r._frontNY;
      // Curve a whole file into range, preserving the space between rows.
      // Clamping each rear soldier to the same range edge collapsed the file.
      const safeFront = Math.max(t.radius * 2 + 12, (t.missile.breakRange || 0) * .48);
      const curve = clamp(r._missileEnemyFront - depthBudget - normal + halfDepth, 0, Math.max(0, r._missileEnemyFront - normal - halfDepth - safeFront));
      advance += curve;
      const goal = { x: r.frontX + r._frontTX * lateral + r._frontNX * advance, y: r.frontY + r._frontTY * lateral + r._frontNY * advance };
      return rigid ? goal : this._elasticGoal(u, r, goal, 0, r._frontNX, r._frontNY);
    }

    _missilePassageGoal(u, r, goal) {
      if (this.time >= (u._passageNext || 0)) {
        u._passageNext = this.time + .3 + u._jitter * .12;
        const reach = 62, heads = this._bodyHeads[u.team];
        const minX = clamp(Math.floor((u.x - reach) / BODY_CELL), 0, this._bodyCols - 1), maxX = clamp(Math.floor((u.x + reach) / BODY_CELL), 0, this._bodyCols - 1);
        const minY = clamp(Math.floor((u.y - reach) / BODY_CELL), 0, this._bodyRows - 1), maxY = clamp(Math.floor((u.y + reach) / BODY_CELL), 0, this._bodyRows - 1);
        let best = Infinity, offset = null;
        for (let cy = minY; cy <= maxY; cy++) for (let cx = minX; cx <= maxX; cx++) {
          for (let id = heads[cy * this._bodyCols + cx]; id !== -1; id = this._bodyNext[id]) {
            const other = this.units[id], company = this.regiments[other.regiment];
            if (other.regiment === r.id || !other.alive || other.routed || TYPES[other.type].ranged || company.order === 'hold' || company._rallyTime > 0 || this.time - (other._moveIntentTime || -1000) > .4) continue;
            const nx = other._moveIntentNX, ny = other._moveIntentNY, tx = -ny, ty = nx;
            const dx = u.x - other.x, dy = u.y - other.y, along = dx * nx + dy * ny, side = dx * tx + dy * ty;
            const clearance = u._radius + other._radius + 9;
            if (along < -12 || along > 52 || Math.abs(side) > clearance + 8) continue;
            const score = Math.abs(along) + Math.abs(side) * 2;
            if (score >= best) continue;
            const sign = Math.abs(side) > .5 ? Math.sign(side) : (u._missileSlot % Math.max(1, r._missileColumns) < r._missileColumns / 2 ? -1 : 1);
            const shift = sign * Math.max(0, clearance - Math.abs(side));
            const ox = u.x + tx * shift - goal.x, oy = u.y + ty * shift - goal.y, extent = Math.hypot(ox, oy);
            offset = extent > 42 ? { x: ox / extent * 42, y: oy / extent * 42 } : { x: ox, y: oy };
            best = score;
          }
        }
        if (offset) { u._passageX = offset.x; u._passageY = offset.y; u._passageUntil = this.time + 1.4; }
      }
      if (this.time < (u._passageUntil || 0)) return { x: goal.x + u._passageX, y: goal.y + u._passageY };
      return goal;
    }

    _updateContactLanes(r, enemy) {
      const columns = r._combatColumns, t = TYPES[r.type];
      if (!r._laneAdvance || r._laneAdvance.length !== columns) r._laneAdvance = new Float64Array(columns);
      const depths = r._laneAdvance;
      depths.fill(Infinity);
      const pitch = r.frontageWidth / columns;
      const stand = Math.max(t.radius * 2 + 2, t.range * (t.contactStand || .78));
      // One enemy at the leading edge must not pin the whole contact line.
      // Each file follows the nearest real opposing body in its own lateral
      // sector, so casualties open reachable space for the following ranks.
      for (const id of enemy.unitIds) {
        const u = this.units[id];
        if (!u.alive || u.routed) continue;
        const dx = u.x - r.frontX, dy = u.y - r.frontY;
        const lateral = dx * r._frontTX + dy * r._frontTY;
        if (Math.abs(lateral) > r.frontageWidth * .5 + pitch) continue;
        const col = clamp(Math.floor((lateral / r.frontageWidth + .5) * columns), 0, columns - 1);
        const forward = dx * r._frontNX + dy * r._frontNY - stand;
        depths[col] = Math.min(depths[col], forward);
      }
      // Empty edge files can wrap toward the rear of the contact line. Interior
      // gaps follow adjacent occupied files instead of converging on a center.
      const limit = Math.min(280, Math.max(100, enemy.depth * 1.8 + 50));
      for (let col = 0; col < columns; col++) {
        if (Number.isFinite(depths[col])) continue;
        let value = 0;
        for (let distance = 1; distance < columns; distance++) {
          const a = col - distance, b = col + distance;
          const left = a >= 0 ? depths[a] : Infinity, right = b < columns ? depths[b] : Infinity;
          if (Number.isFinite(left) || Number.isFinite(right)) {
            value = Math.min(left, right) + Math.min(35, distance * pitch * .35);
            break;
          }
        }
        depths[col] = value;
      }
      for (let col = 0; col < columns; col++) depths[col] = clamp(depths[col], -60, limit);
    }

    _elasticGoal(u, r, goal, angle, nx, ny) {
      if (u._flex < .02 || r.order === 'hold' || r._traffic || r._bridge || TYPES[r.type].commander) return goal;
      const factor = r.formation === 'shieldwall' ? .7 : r.formation === 'loose' ? 3 : r.formation === 'wedge' ? 1.5 : 2;
      let firm = r._terrainPressure < .2 && r._contactDistance < TYPES[r.type].range + 120 && r.formation !== 'loose' ?
        (TYPES[r.type].rankStiffness || 0) * clamp(r.cohesion / 100, 0, 1) : 0;
      if (r._steadyBraceScale > 0) firm = Math.max(firm, .45 * clamp(r.cohesion / 100, 0, 1));
      const slack = r._spacing * factor * u._flex * (1 - firm);
      const px = ny === undefined ? -Math.sin(angle) : -ny, py = nx === undefined ? Math.cos(angle) : nx;
      const lateral = clamp((u.x - goal.x) * px + (u.y - goal.y) * py, -slack, slack);
      // Local rank positions are elastic under real pressure. A soldier can
      // remain in an opened gap instead of immediately fighting to reclaim
      // its exact file; the allowance contracts when the obstruction clears.
      return { x: goal.x + px * lateral, y: goal.y + py * lateral };
    }

    _combatGoal(u, r, rigid) {
      const slot = u._combatSlot >= 0 ? u._combatSlot : u._slot;
      const columns = r._combatColumns;
      const col = u._pressUntil > this.time && u._pressColumn >= 0 ? u._pressColumn : slot % columns;
      const row = Math.floor(slot / columns);
      const lateral = ((col + .5) / columns - .5) * r.frontageWidth;
      let rear = row * Math.max(u._radius * 2 + 3, r._combatSpacing * .94);
      if (u._pressUntil > this.time && (col === 0 || col === columns - 1) && row > 1) rear = -Math.min(110, row * r._spacing * .65);
      const advance = (r._laneAdvance ? r._laneAdvance[col] : 0) - rear;
      const goal = { x: r.frontX + r._frontTX * lateral + r._frontNX * advance, y: r.frontY + r._frontTY * lateral + r._frontNY * advance };
      return rigid ? goal : this._elasticGoal(u, r, goal, 0, r._frontNX, r._frontNY);
    }

    _canDeployForCombat(r) {
      if (this._inUnitStep) return r._deployCombat;
      if (!r._combatActive || r._traffic || r._bridge || r.order === 'face') return false;
      if (r._routeDetour && r._nearestEnemyDistance > TYPES[r.type].range + 180) return false;
      return r.formation !== 'wedge' || r._enemyDistance <= 180;
    }

    _seekContactGap(u, r) {
      const columns = r._combatColumns, slot = u._combatSlot >= 0 ? u._combatSlot : u._slot;
      if (columns < 3 || Math.floor(slot / columns) < 1) return;
      const current = slot % columns;
      const candidates = [current - 2, current + 2, current - 4, current + 4, 0, columns - 1];
      let best = current, score = Infinity;
      for (let k = 0; k < candidates.length; k++) {
        const col = candidates[(k + u.id) % candidates.length];
        if (col < 0 || col >= columns) continue;
        const lateral = ((col + .5) / columns - .5) * r.frontageWidth;
        const advance = (r._laneAdvance ? r._laneAdvance[col] : 0) - 15;
        const x = r.frontX + r._frontTX * lateral + r._frontNX * advance;
        const y = r.frontY + r._frontTY * lateral + r._frontNY * advance;
        if (this._bodyBlocked(x, y, u._radius + 2)) continue;
        const cell = clamp(Math.floor(y / CELL), 0, this._rows - 1) * this._cols + clamp(Math.floor(x / CELL), 0, this._cols - 1);
        const value = this._cellCounts[u.team][cell] * 1.5 + Math.abs(col - current) * .35 - Math.min(6, this._cellCounts[1 - u.team][cell]) * .5;
        if (value < score) { best = col; score = value; }
      }
      u._pressColumn = best; u._pressUntil = this.time + 4; u._jamTime = 0;
    }

    _strategize() {
      if (this.siege) this.siege.prepare();
      if (global.BattleTactics) {
        if (!this._tactics) this._tactics = new global.BattleTactics(this);
        this._tactics.update();
      }
      this._assignEngagements();
      for (const r of this.regiments) {
        if (r.alive <= 0) continue;
        const t = TYPES[r.type];
        const nearest = this.regiments[r._enemyId] || null;
        let gx = r.x, gy = r.y;
        if (r.fallingBack) {
          if (r._withdrawLeg && Math.hypot(r._centroidX - r._withdrawGoalX, r._centroidY - r._withdrawGoalY) < 32) {
            r._withdrawLeg = false;
            if (r._manualFallback) {
              // Continue from the ranks that actually withdrew. Restoring
              // their old forward-facing slots would swap front and rear
              // soldiers through one another as soon as breakoff finished.
              const center = this._currentCenter(r);
              r.x = r._centroidX = center.x; r.y = r._centroidY = center.y;
              r._path.length = 0; r._pathTimer = 0;
              r._travelSlots = true; r._approachBlendUntil = this.time + 2;
              for (const id of r.unitIds) {
                const u = this.units[id];
                if (!u.alive || u.routed) continue;
                u._orderOffsetX = u.x - center.x; u._orderOffsetY = u.y - center.y;
                u._moveSlot = -1;
              }
            }
          }
          const support = this._findSupport(r);
          r.supportRegimentId = support ? support.id : -1;
          r.regroupWith = support ? support.name : 'surviving allies';
          const rear = r.team === 0 ? -1 : 1;
          if (r._manualFallback && r._withdrawLeg) {
            gx = r._withdrawGoalX; gy = r._withdrawGoalY;
          } else if (support) {
            const behind = TYPES[support.type].commander ? 90 : Math.min(220, support.depth * .6 + 100);
            gx = support.x + (r._manualFallback ? r._retreatNX : rear) * behind;
            gy = support.y + (r._manualFallback ? r._retreatNY * behind : ((r.id % 3) - 1) * 115);
          } else {
            const distance = r._nearestEnemyDistance < 350 ? 300 : 0;
            gx = r.x + (r._manualFallback ? r._retreatNX : rear) * distance;
            gy = r.y + (r._manualFallback ? r._retreatNY * distance : 0);
          }
          r.targetX = gx; r.targetY = gy;
        } else if (r._rallyTime > 0) {
          gx = r._holdX; gy = r._holdY;
        } else if (r.order === 'face') {
          gx = r._holdX; gy = r._holdY;
        } else if (TYPES[r.type].commander && r.order === 'auto') {
          // Commanders follow viable friendly formations from behind. They do
          // not automatically lead the charge into the opposing battle line.
          let support = null, score = Infinity;
          for (const other of this.regiments) {
            if (other.team !== r.team || other.id === r.id || other.alive <= 0 || other.fallingBack || TYPES[other.type].commander || TYPES[other.type].siege) continue;
            const d = Math.hypot(other.x - r.x, other.y - r.y) - Math.min(180, other.alive * .6);
            if (d < score) { score = d; support = other; }
          }
          if (support) {
            gx = support.x + (r.team === 0 ? -210 : 210);
            gy = support.y + (r.id % 2 ? 65 : -65);
          } else if (nearest && r._enemyDistance < 240) {
            const dx = r.x - nearest.x, dy = r.y - nearest.y, d = Math.max(1, Math.hypot(dx, dy));
            gx = r.x + dx / d * 160; gy = r.y + dy / d * 160;
          }
        } else if (nearest) {
          const dx = nearest.x - r.x, dy = nearest.y - r.y;
          const distance = Math.max(1, Math.hypot(dx, dy));
          if (r.order !== 'auto' && r.order !== 'hold' && r.order !== 'face' &&
              !((t.brace || t.frontalShield || t.push) && this._canDeployForCombat(r))) r.facing = Math.atan2(dy, dx);
          const firingRange = t.range * (this.terrainAt(r.x, r.y).elevation ? 1.12 : 1);
          const stop = t.ranged ? Math.max(60, firingRange * .70 - nearest.depth * .12) : Math.max(12, t.range * .3);
          if (r.order === 'hold') { gx = r._holdX; gy = r._holdY; }
          else if (r.order === 'move' || this._advancingAttack(r)) {
            gx = r.targetX; gy = r.targetY;
            const atDestination = Math.hypot(r.x - gx, r.y - gy) < 28 && !r._traffic;
            if (atDestination && r._marchLayout && r.order === 'move') {
              // The standard reached the destination. Assign the closest
              // available files now, including for late or scattered troops.
              r._marchLayout = false; r.x = gx; r.y = gy;
              this._assignTravelSlots(r, Math.max(1, Math.round(r._adaptiveColumns)));
            }
            if (atDestination && r._arrivalFraction >= .88) {
              if (r.order === 'move') {
                // Arrival settles into the destination slots; a manual Hold
                // instead captures present positions in command(). Let the
                // remaining tail complete a passage before stopping.
                for (const id of r.unitIds) {
                  const u = this.units[id];
                  if (u.alive && !u.routed) {
                    u._holdPosition = u._friendlyOffsetX || u._friendlyOffsetY ? { x: u.x, y: u.y } : this._formationGoal(u, r);
                    u._friendlyOffsetX = u._friendlyOffsetY = 0; u._friendlyUntil = 0;
                    u._friendlySource = u._friendlySourceEpoch = u._friendlyEpoch = -1;
                  }
                }
                r.order = 'hold'; r._holdX = gx; r._holdY = gy; r._travelSlots = false;
              }
              else r._hasDestination = false;
            }
          } else {
            if (r.order === 'auto' && !r._combatActive && !r._missileActive) {
              // March the carried body toward the local enemy front. Do not
              // reserve combat files or flanking arcs several marches away.
              const nx = r._approachNX, ny = r._approachNY, tx = -ny, ty = nx;
              const across = this._projectFrontBounds(nearest, tx, ty), forward = this._projectFrontBounds(nearest, nx, ny);
              const lane = clamp(r._centroidX * tx + r._centroidY * ty, across.lo - r.width * .25, across.hi + r.width * .25);
              const normal = forward.lo - (t.ranged ? t.range * .72 : r.depth * .5 + t.range + 25);
              gx = normal * nx + lane * tx; gy = normal * ny + lane * ty;
            } else if (!t.ranged) {
              gx = r.frontX - r._frontNX * Math.max(r._spacing, r._combatRows * r._spacing * .45);
              gy = r.frontY - r._frontNY * Math.max(r._spacing, r._combatRows * r._spacing * .45);
              if (r.order === 'charge' && r._sharedChargeLane) {
                const lane = r._sharedChargeLane;
                const run = Math.max(0, ((gx - r._centroidX) * lane.x + (gy - r._centroidY) * lane.y) /
                  (lane.x * lane.x + lane.y * lane.y));
                gx = r._centroidX + run * lane.x;
                gy = r._centroidY + run * lane.y;
              }
            } else { gx = r.frontX; gy = r.frontY; }
            if (t.ranged && !t.missile && distance < stop * .75 && r.order !== 'charge' && !t.siege) {
              gx = r.x - dx / distance * 90; gy = r.y - dy / distance * 90;
            }
          }
        } else if (r.order === 'move' || (r.order === 'attack' && r._hasDestination)) {
          gx = r.targetX; gy = r.targetY;
        }
        const tacticalGoal = this._tactics && this._tactics.goal(r);
        if (tacticalGoal) { gx = tacticalGoal.x; gy = tacticalGoal.y; }
        const siegeGoal = this.siege && this.siege.goal(r, gx, gy);
        if (siegeGoal) { gx = siegeGoal.x; gy = siegeGoal.y; }
        const goal = this._nearestPassable(gx, gy);
        r._goalX = goal.x; r._goalY = goal.y;
        if (r.order !== 'move' && !(r.order === 'attack' && r._hasDestination)) { r.targetX = goal.x; r.targetY = goal.y; }
        if (r._pathTimer <= 0 || !r._path.length) {
          r._path = this._planRegimentRoute(r, goal.x, goal.y);
          r._pathTimer = 1.4 + (r.id % 7) * .12;
        }
      }
      this._updateManeuvers();
      for (const r of this.regiments) if (r.alive > 0) {
        if (r._travelSlots && r.unitIds.some(id => this.units[id].alive && !this.units[id].routed && this.units[id]._moveSlot < 0)) this._assignTravelSlots(r, Math.max(1, Math.round(r._adaptiveColumns)));
        this._commandFeedback(r);
        if (r.order === 'auto' && !r.fallingBack && r._rallyTime <= 0 && r._tactic) {
          r.tacticalRole = r._tactic.role;
          if (r._tactic.charge) {
            const target = this.regiments[r._tactic.targetId];
            if (target && target.alive > 0) { r.commandTargetId = target.id; r.commandTargetName = target.name; }
          }
        }
      }
      this._updateApproachFields();
    }

    _assignTravelSlots(r, columns) {
      const nx = Math.cos(r._travelAngle), ny = Math.sin(r._travelAngle), tx = -ny, ty = nx;
      const soldiers = r.unitIds.map(id => this.units[id]).filter(u => u.alive && !u.routed);
      soldiers.sort((a, b) => (a.x - b.x) * tx + (a.y - b.y) * ty || a.id - b.id);
      const lanes = Array.from({ length: columns }, () => []);
      for (let i = 0; i < soldiers.length; i++) lanes[Math.min(columns - 1, Math.floor(i * columns / Math.max(1, soldiers.length)))].push(soldiers[i]);
      for (let col = 0; col < columns; col++) {
        lanes[col].sort((a, b) => (b.x - a.x) * nx + (b.y - a.y) * ny || a.id - b.id);
        for (let row = 0; row < lanes[col].length; row++) lanes[col][row]._moveSlot = row * columns + col;
      }
      r._travelSlotColumns = columns; r._travelSlotCount = r.alive;
      r._travelSlotAngle = r._travelAngle;
    }

    _projectBounds(r, ax, ay) {
      const lo = ax * (ax >= 0 ? r._actualMinX : r._actualMaxX) + ay * (ay >= 0 ? r._actualMinY : r._actualMaxY);
      const hi = ax * (ax >= 0 ? r._actualMaxX : r._actualMinX) + ay * (ay >= 0 ? r._actualMaxY : r._actualMinY);
      return { lo, hi };
    }

    _projectFrontBounds(r, ax, ay) {
      const bounds = r._frontBounds;
      if (!bounds) return this._projectBounds(r, ax, ay);
      return {
        lo: ax * (ax >= 0 ? bounds.minX : bounds.maxX) + ay * (ay >= 0 ? bounds.minY : bounds.maxY),
        hi: ax * (ax >= 0 ? bounds.maxX : bounds.minX) + ay * (ay >= 0 ? bounds.maxY : bounds.minY)
      };
    }

    _clearTraffic(plan) {
      for (const id of [plan.a, plan.b]) {
        const r = this.regiments[id];
        if (r && r._traffic && r._traffic.plan === plan) {
          r._traffic = null; r.maneuver = 'reforming';
          for (const uid of r.unitIds) this.units[uid]._trafficSlot = -1;
          if (r.order === 'auto' || (plan.parallel && (r.order === 'move' || r.order === 'attack' || r.order === 'charge'))) {
            // A passage ends in the body that actually emerged. Returning to
            // cached pre-passage offsets would swap files through one another.
            this._captureMarchLayout(r);
          }
          if (r.order === 'auto' && r._tactic && r._tactic.hold) {
            for (const uid of r.unitIds) {
              const u = this.units[uid];
              if (u.alive && !u.routed) { u._tacticHoldX = u.x; u._tacticHoldY = u.y; }
            }
            r._tactic.goalX = r.x; r._tactic.goalY = r.y;
          }
        }
      }
      if (plan.yielding) this._crossingCooldowns.set(Math.min(plan.a, plan.b) + ':' + Math.max(plan.a, plan.b), this.time + 4);
      const index = this._trafficPlans.indexOf(plan);
      if (index !== -1) this._trafficPlans.splice(index, 1);
    }

    _assignTrafficSlots(r) {
      const traffic = r._traffic, plan = traffic.plan, lanes = traffic.lanes;
      const soldiers = r.unitIds.map(id => this.units[id]).filter(u => u.alive && !u.routed);
      // Preserve physical lateral ordering while opening lanes. Original slot
      // modulo would ask adjacent files to swap through each other and jam.
      soldiers.sort((a, b) => (-plan.ay * a.x + plan.ax * a.y) - (-plan.ay * b.x + plan.ax * b.y) || a.id - b.id);
      const columns = Array.from({ length: lanes }, () => []);
      for (let i = 0; i < soldiers.length; i++) columns[Math.min(lanes - 1, Math.floor(i * lanes / Math.max(1, soldiers.length)))].push(soldiers[i]);
      const forward = Math.cos(r._travelAngle) * plan.ax + Math.sin(r._travelAngle) * plan.ay >= 0 ? 1 : -1;
      let rows = 0;
      for (let lane = 0; lane < columns.length; lane++) {
        const column = columns[lane];
        column.sort((a, b) => ((b.x - a.x) * plan.ax + (b.y - a.y) * plan.ay) * forward || a.id - b.id);
        for (let row = 0; row < column.length; row++) column[row]._trafficSlot = row * lanes + lane;
        rows = Math.max(rows, column.length);
      }
      traffic.rows = rows;
    }

    _startParallelPass(a, b, ax, ay, crossing) {
      const canMarch = r => r.order === 'auto' || r.order === 'move' || r.order === 'charge' || this._advancingAttack(r);
      if (!canMarch(a) || (!canMarch(b) && !b._withdrawLeg) || a.fallingBack || (b.fallingBack && !b._withdrawLeg) || a._rallyTime > 0 || b._rallyTime > 0 ||
          a._missileActive || b._missileActive || a._bridge || b._bridge || a._terrainColumns < a._shapeColumns || b._terrainColumns < b._shapeColumns ||
          (!crossing && TYPES[a.type].speed < TYPES[b.type].speed * 1.12)) return false;
      const aa = this._projectBounds(a, ax, ay), ba = this._projectBounds(b, ax, ay);
      const at = this._projectBounds(a, -ay, ax), bt = this._projectBounds(b, -ay, ax);
      const width = Math.max(a.width, at.hi - at.lo), depth = Math.max(a.depth, aa.hi - aa.lo);
      const clearance = Math.hypot(width, depth) * .5 + TYPES[a.type].radius + 4;
      const across = (bt.lo + bt.hi) * .5, ownAcross = -ay * a.x + ax * a.y;
      const initialSide = Math.abs(ownAcross - across) > 20 ? Math.sign(ownAcross - across) : a._flankSign;
      for (const side of [initialSide, -initialSide]) {
        const origin = across + side * (Math.max(b.width, bt.hi - bt.lo) * .5 + width * .5 + 42);
        const along = a.x * ax + a.y * ay, end = ba.hi + depth + 180;
        const bend = { x: along * ax - origin * ay, y: along * ay + origin * ax };
        const finish = { x: end * ax - origin * ay, y: end * ay + origin * ax };
        if (!this._lineClear(a.x, a.y, bend.x, bend.y, clearance) || !this._lineClear(bend.x, bend.y, finish.x, finish.y, clearance)) continue;
        // A broad woodland route is still passable at its normal terrain
        // speed. Only real solid ground can require a narrow interleaving.
        if (this._trafficPlans.some(other => other.parallel && other.b === b.id &&
          Math.abs(other.origin - origin) < (width + this.regiments[other.a].width) * .5 + 24)) continue;
        const lanes = Math.max(1, a._shapeColumns), pitch = a._shapeSpacing;
        const plan = { a: a.id, b: b.id, ax, ay, origin, pitch, started: this.time, marching: true, parallel: true, bodyFacing: a.facing,
          intentA: a.order + ':' + a.fallingBack + ':' + (a._rallyTime > 0), intentB: b.order + ':' + b.fallingBack + ':' + (b._rallyTime > 0),
          epochA: a._commandEpoch, epochB: b._commandEpoch };
        this._captureMarchLayout(a);
        a._traffic = { plan, phase: 0, lanes, maxLanes: lanes, base: -(lanes - 1) / 2, hold: false, anchorX: a.x, anchorY: a.y };
        this._trafficPlans.push(plan);
        return true;
      }
      return false;
    }

    _trafficWaypoint(r) {
      // Traffic predicts the continuing route through a nearby corner. The
      // actual march still follows every path point and checks solid terrain.
      for (const point of r._path) if (Math.hypot(point.x - r.x, point.y - r.y) >= 70) return point;
      return { x: r._goalX, y: r._goalY };
    }

    _automaticCrossing(a, b, ax, ay, bdx, bdy, bd) {
      if (a.order !== 'auto' || b.order !== 'auto' || a.fallingBack || b.fallingBack || a._rallyTime > 0 || b._rallyTime > 0 ||
          a._combatActive || b._combatActive || a._missileActive || b._missileActive || a._bridge || b._bridge ||
          a._terrainColumns < a._shapeColumns || b._terrainColumns < b._shapeColumns || bd < 70) return false;
      const alignment = (bdx * ax + bdy * ay) / bd;
      return alignment > -.6 && alignment < .94;
    }

    _crossingFootprintsMeet(a, b, ax, ay, horizon) {
      const waypoint = this._trafficWaypoint(b);
      const bdx = waypoint.x - b.x, bdy = waypoint.y - b.y, bd = Math.hypot(bdx, bdy);
      const av = TYPES[a.type].speed * .88 * this._formationFactor(a, 'speed');
      const moving = b.order !== 'hold' && !(b.order === 'auto' && b._tactic && b._tactic.hold) && Math.hypot(b._goalX - b.x, b._goalY - b.y) > 70;
      const bv = moving && bd > 20 ? TYPES[b.type].speed * .88 * this._formationFactor(b, 'speed') : 0;
      const vx = (bd ? bdx / bd * bv : 0) - ax * av, vy = (bd ? bdy / bd * bv : 0) - ay * av;
      const margin = TYPES[a.type].radius + TYPES[b.type].radius + 6;
      let enter = 0, leave = horizon;
      for (const [alo, ahi, blo, bhi, speed] of [[a._actualMinX, a._actualMaxX, b._actualMinX, b._actualMaxX, vx],
        [a._actualMinY, a._actualMaxY, b._actualMinY, b._actualMaxY, vy]]) {
        if (Math.abs(speed) < .001) { if (bhi + margin < alo || blo - margin > ahi) return false; continue; }
        const first = (alo - bhi - margin) / speed, last = (ahi - blo + margin) / speed;
        enter = Math.max(enter, Math.min(first, last)); leave = Math.min(leave, Math.max(first, last));
        if (leave < enter) return false;
      }
      return leave >= enter;
    }

    _yieldAutomaticCrossing(a, b, ax, ay) {
      this._captureMarchLayout(a);
      const lanes = a._shapeColumns;
      const plan = { a: a.id, b: b.id, ax, ay, pitch: a._shapeSpacing, started: this.time, yielding: true, deadline: this.time + 2.6 };
      a._traffic = { plan, phase: 0, lanes, maxLanes: lanes, base: -(lanes - 1) / 2,
        hold: true, anchorX: a.x, anchorY: a.y };
      // This is temporary right of way, not a Hold order and not a request
      // for elephant-width lanes through a forty-row cavalry column.
      this._trafficPlans.push(plan);
    }

    _needsCombatRoom(r) {
      if (!r || r.alive <= 0 || r.fallingBack || r._rallyTime > 0 || r.order === 'move' || r.order === 'hold' || r.order === 'face') return false;
      const t = TYPES[r.type];
      if (t.commander) return false;
      if (t.ranged) return r.engagedSoldiers > 0 || r._contactDistance < Math.max(200, t.range * .95);
      return r._combatActive && (r._nearestEnemyDistance < 450 || r._contactDistance < 220);
    }

    _localMountedPass(a, b, ax, ay, bMoving, alignment, bx, by) {
      // A parked horse can give one file of room. Do not slow an entire
      // approaching company to thread two artificial columns through it.
      if (!this.impacts || !this.impacts.isHorse(a) || !this.impacts.isHorse(b) ||
          a.fallingBack || b.fallingBack || a._rallyTime > 0 || b._rallyTime > 0 ||
          a._marchTurnUntil > this.time || b._marchTurnUntil > this.time ||
          a._siegeJob || b._siegeJob || a._bridge || b._bridge ||
          a._terrainPressure >= .2 || b._terrainPressure >= .2 ||
          a._contactDistance < TYPES[a.type].range + 220 ||
          b._contactDistance < TYPES[b.type].range + 220) return false;
      const held = b.order === 'hold' || (b.order === 'auto' && b._tactic && b._tactic.hold);
      if (held ? b.alive > 2 : !bMoving || a.alive !== 1 || b.alive !== 1) return false;
      const radius = TYPES[b.type].radius, space = radius + TYPES[a.type].radius + 5;
      if (!held && alignment <= .9) {
        // This is a pair of individual moving horses, not two formed fronts.
        // Reserve sideways room on both actual courses through their projected
        // meeting. A wall, narrow crossing or boundary retains ordinary traffic.
        const u = this.units[a.unitIds.find(id => this.units[id].alive && !this.units[id].routed)];
        const v = this.units[b.unitIds.find(id => this.units[id].alive && !this.units[id].routed)];
        if (!u || !v || !Number.isFinite(bx) || !Number.isFinite(by)) return false;
        const sa = TYPES[a.type].speed, sb = TYPES[b.type].speed;
        const vx = ax * sa - bx * sb, vy = ay * sa - by * sb;
        const dx = v.x - u.x, dy = v.y - u.y;
        const rawTime = (dx * vx + dy * vy) / Math.max(1, vx * vx + vy * vy);
        const ahead = clamp(rawTime, 0, 2.4);
        if (!this._lineClear(u.x, u.y, u.x + ax * sa * ahead, u.y + ay * sa * ahead, u._radius + space) ||
            !this._lineClear(v.x, v.y, v.x + bx * sb * ahead, v.y + by * sb * ahead, v._radius + space)) return false;
        // Two independent trajectories can pass this rectangle at different
        // times. Such a near miss needs neither a formation plan nor a peer.
        if (rawTime < 0 || Math.hypot(dx-vx*ahead,dy-vy*ahead) > space * 2) return true;
        const deadline = this.time + ahead;
        const precedes = (r, otherId) => {
          const old = r._localRiderPass;
          if (!old || old.unitId === otherId || old.ownEpoch !== r._commandEpoch ||
              old.until < this.time || old.deadline < this.time || old.deadline > deadline) return false;
          const other = this.units[old.unitId];
          return other && other.alive && !other.routed && this.regiments[other.regiment]._commandEpoch === old.otherEpoch;
        };
        if (!precedes(a, v.id) && !precedes(b, u.id)) {
          const until = this.time + 1.1;
          a._localRiderPass = { unitId: v.id, ownEpoch: a._commandEpoch, otherEpoch: b._commandEpoch, deadline, until };
          b._localRiderPass = { unitId: u.id, ownEpoch: b._commandEpoch, otherEpoch: a._commandEpoch, deadline, until };
        }
        return true;
      }
      for (let side = -1; side <= 1; side += 2) {
        const sx = -ay * space * side, sy = ax * space * side;
        if (!this._lineClear(a.x, a.y, b.x + sx, b.y + sy, TYPES[a.type].radius + 2)) continue;
        let open = true;
        for (const id of b.unitIds) {
          const u = this.units[id];
          if (!u.alive || u.routed) continue;
          if (this._bodyBlocked(u.x + sx, u.y + sy, radius + 2) ||
              !this._lineClear(u.x, u.y, u.x + sx, u.y + sy, radius + 2)) { open = false; break; }
        }
        if (open) return true;
      }
      return false;
    }

    _passLancerCompany(a, b, ax, ay, bx, by) {
      if (a._lancerPass || b._lancerPass) return true;
      const dx = b._centroidX - a._centroidX, dy = b._centroidY - a._centroidY;
      const radius = TYPES.lancer.radius, margin = radius * 2 + 18;
      const halfX = (a._actualMaxX - a._actualMinX + b._actualMaxX - b._actualMinX) * .5;
      const halfY = (a._actualMaxY - a._actualMinY + b._actualMaxY - b._actualMinY) * .5;
      const speed = TYPES.lancer.speed * .88;
      const horizon = Math.min(4, Math.hypot(a._goalX - a.x, a._goalY - a.y) / speed);
      const bodyRadius = Math.hypot(a._actualMaxX - a._actualMinX, a._actualMaxY - a._actualMinY) * .5 + radius;
      // Sweep both companies at cruising speed. Choose the shallowest
      // forward course with room for their real ranks to pass one another.
      const clearance = (nx, ny) => {
        const vx = (nx - bx) * speed, vy = (ny - by) * speed, vv = vx * vx + vy * vy;
        if (vv < 1) return Infinity;
        const time = (dx * vx + dy * vy) / vv;
        if (time <= 0 || time > horizon) return Infinity;
        return (Math.abs(dx * vy - dy * vx) - halfX * Math.abs(vy) - halfY * Math.abs(vx)) / Math.sqrt(vv);
      };
      if (clearance(ax, ay) >= margin) return true;
      let best = null, bestGap = -Infinity;
      const cross = ax * by - ay * bx;
      const side = Math.abs(cross) > .1 ? -Math.sign(cross) : dx * ay - dy * ax < 0 ? -1 : 1;
      for (let angle = .12; angle <= 1.46; angle += .12) {
        for (const sign of [side, -side]) {
          const c = Math.cos(angle), s = Math.sin(angle) * sign;
          const nx = ax * c - ay * s, ny = ay * c + ax * s;
          const gap = clearance(nx, ny);
          if (gap <= bestGap || !this._lineClear(a.x, a.y, a.x + nx * speed, a.y + ny * speed, bodyRadius)) continue;
          best = { nx, ny }; bestGap = gap;
          if (gap >= margin) break;
        }
        if (bestGap >= margin) break;
      }
      a._lancerPass = best;
      return !!best;
    }

    _updateManeuvers() {
      for (const [pair, until] of this._crossingCooldowns) if (until <= this.time) this._crossingCooldowns.delete(pair);
      // Route-oriented, temporary columns keep a regiment's chosen formation
      // while adapting its current footprint to woodland and solid obstacles.
      for (const r of this.regiments) {
        r._lancerPass = null;
        if (r.alive <= 0) continue;
        if (r.order === 'face') { r._terrainPressure = 0; continue; }
        const t = TYPES[r.type], f = FORMATIONS[r.formation] || {};
        const nominal = r.formation === 'shieldwall' ? Math.ceil(r._columns * 1.25) : r.formation === 'wedge' ? Math.ceil((Math.sqrt(8 * r.alive + 1) - 1) / 2) : r._columns;
        r._shapeColumns = Math.max(1, nominal);
        const waypoint = r._path[0] || { x: r._goalX, y: r._goalY };
        const dx = waypoint.x - r.x, dy = waypoint.y - r.y, distance = Math.hypot(dx, dy);
        if (distance > 12) r._travelAngle = Math.atan2(dy, dx);
        const ax = Math.cos(r._travelAngle), ay = Math.sin(r._travelAngle), px = -ay, py = ax;
        const spacing = r._spacing * (f.spacing || 1);
        let desired = nominal, forest = this.terrainAt(r.x, r.y).type === 'forest';
        let obstacle = false;
        for (const region of this._regions) {
          if (region.type === 'forest' && inside(r.x + ax * Math.min(130, distance), r.y + ay * Math.min(130, distance), region, 25)) forest = true;
          if ((region.type === 'rock' || region.type === 'water') && r._actualMaxX + 80 > region.x && r._actualMinX - 80 < region.x + region.w && r._actualMaxY + 80 > region.y && r._actualMinY - 80 < region.y + region.h) obstacle = true;
        }
        if (r._terrainContacts > r.alive * .08) forest = true;
        const half = nominal * spacing * .5;
        let offsetLo = -half, offsetHi = half;
        // A route usually hugs one edge of a rock. Measure the whole usable
        // corridor and shift its temporary ranks into that room; twice the
        // smaller side clearance would turn a wide passage into one file.
        for (const look of [0, Math.min(100, distance), Math.min(210, distance)]) {
          const sx = r.x + ax * look, sy = r.y + ay * look;
          if (this._bodyBlocked(sx, sy, t.radius + 2)) continue;
          let left = half * 2, right = half * 2;
          for (let side = -1; side <= 1; side += 2) {
            let available = half * 2;
            for (let offset = Math.max(10, spacing * .5); offset <= half * 2 + 10; offset += 12) {
              if (this._bodyBlocked(sx + px * offset * side, sy + py * offset * side, t.radius + 3)) {
                available = Math.max(spacing * .5, offset - 12); obstacle = true; break;
              }
            }
            if (side < 0) left = available; else right = available;
          }
          const fit = Math.max(1, Math.min(nominal, Math.floor((left + right) / spacing)));
          desired = Math.min(desired, fit);
          const fitHalf = desired * spacing * .5;
          offsetLo = Math.max(offsetLo, fitHalf - left);
          offsetHi = Math.min(offsetHi, right - fitHalf);
        }
        r._terrainOffset = offsetLo <= offsetHi ? clamp(0, offsetLo, offsetHi) : (offsetLo + offsetHi) * .5;
        // Woods slow the individual bodies and squeeze existing files a
        // little. They are not a solid corridor: halving the column count
        // here repeatedly rebuilt a healthy formation into a long queue.
        r._forestCompression = forest ? Math.min(1, Math.max(.86, (t.radius * 2 + 2) / spacing)) : 1;
        if (desired < nominal) r._maneuverUntil = this.time + 2.5;
        if (this.time < r._maneuverUntil && desired === nominal) desired = Math.min(nominal, r._terrainColumns);
        r._terrainColumns = Math.max(1, desired);
        r._shapeSpacing = spacing;
        r._terrainPressure = Math.max(forest ? .28 : 0, desired < nominal ? 1 - desired / nominal : 0);
      }

      // A shared passing plan puts allied bodies into alternating physical
      // lanes. It expires only after the moving tail has cleared the other
      // formation, not merely after the banner passes its center.
      for (let i = this._trafficPlans.length - 1; i >= 0; i--) {
        const plan = this._trafficPlans[i], a = this.regiments[plan.a], b = this.regiments[plan.b];
        const pa = a ? this._projectBounds(a, plan.ax, plan.ay) : null;
        const pb = b ? this._projectBounds(b, plan.ax, plan.ay) : null;
        const combatDeployment = this._needsCombatRoom(a) || this._needsCombatRoom(b);
        const changedIntent = a && b && plan.intentA !== undefined && (plan.intentA !== a.order + ':' + a.fallingBack + ':' + (a._rallyTime > 0) ||
          plan.intentB !== b.order + ':' + b.fallingBack + ':' + (b._rallyTime > 0) ||
          (plan.epochA !== undefined && (plan.epochA !== a._commandEpoch || plan.epochB !== b._commandEpoch)));
        const yieldDone = plan.yielding && a && b && (this.time >= plan.deadline || a.order !== 'auto' || b.order !== 'auto' ||
          a.fallingBack || b.fallingBack || a._rallyTime > 0 || b._rallyTime > 0 || a._terrainColumns < a._shapeColumns || b._terrainColumns < b._shapeColumns ||
          !this._crossingFootprintsMeet(a, b, plan.ax, plan.ay, 3));
        const clear = !a || !b || a.alive <= 0 || b.alive <= 0 || a._nearestEnemyDistance < 200 || b._nearestEnemyDistance < 200 || combatDeployment ||
          changedIntent || yieldDone ||
          (plan.parallel && (a.order === 'hold' || b.order === 'hold' || a.fallingBack || (b.fallingBack && !b._withdrawLeg) || a._rallyTime > 0 || b._rallyTime > 0 ||
            a._terrainColumns < a._shapeColumns || b._terrainColumns < b._shapeColumns)) || (this.time > plan.started + 4 && pa.lo > pb.hi + 28);
        if (clear) {
          this._clearTraffic(plan);
        }
      }
      for (let i = 0; i < this.regiments.length; i++) {
        const a = this.regiments[i];
        // A single engine has no ranks to interleave with an army. Nearby
        // bodies still avoid its actual footprint through physical collision;
        // a parked ram must not stretch a whole company into one long file.
        if (a.alive <= 0 || a._traffic || TYPES[a.type].commander || TYPES[a.type].equipment || a.order === 'hold' || a.order === 'face' ||
            (a.order === 'auto' && a._tactic && a._tactic.hold) || a._nearestEnemyDistance < 250 || this._needsCombatRoom(a)) continue;
        const next = this._trafficWaypoint(a);
        const dx = next.x - a.x, dy = next.y - a.y, distance = Math.hypot(dx, dy);
        if (distance < 100) continue;
        const ax = dx / distance, ay = dy / distance;
        for (let j = 0; j < this.regiments.length; j++) {
          const b = this.regiments[j];
          if (i === j || b.team !== a.team || b.alive <= 0 || b._traffic || b.order === 'face' || TYPES[b.type].commander || TYPES[b.type].equipment || b._nearestEnemyDistance < 250 || this._needsCombatRoom(b)) continue;
          const ox = b.x - a.x, oy = b.y - a.y, along = ox * ax + oy * ay, lateral = Math.abs(-ox * ay + oy * ax);
          if (a.type === 'lancer' && b.type === 'lancer' && (a.alive > 1 || b.alive > 1) &&
              !a.fallingBack && !b.fallingBack && a._rallyTime <= 0 && b._rallyTime <= 0 &&
              !a._bridge && !b._bridge &&
              a._terrainPressure < .2 && b._terrainPressure < .2 &&
              (along > 0 || b._lancerPass) && Math.hypot(ox, oy) < (a.width + b.width) * .5 + 8 * TYPES.lancer.speed) {
            const goal = this._trafficWaypoint(b), dx = goal.x - b.x, dy = goal.y - b.y, d = Math.hypot(dx, dy);
            const held = b.order === 'hold' || (b.order === 'auto' && b._tactic && b._tactic.hold);
            const crossing = held ? b.alive > 2 : d > 70 && (ax * dx + ay * dy) / d < .94;
            if (crossing && this._passLancerCompany(a, b, ax, ay, held ? 0 : dx / d, held ? 0 : dy / d)) continue;
          }
          const lateralMiss = lateral > (a.width + b.width) * .40 + 12;
          // Individual moving riders may approach a crossing from different
          // files. Widen only their local lookahead, never the formation planner.
          if (along < 15 || along > Math.min(distance + 100, (a.depth + b.depth) * .5 + 240) ||
              (lateralMiss && !(lateral < 200 && a.alive === 1 && b.alive === 1 && b.order !== 'hold' &&
                this.impacts && this.impacts.isHorse(a) && this.impacts.isHorse(b)))) continue;
          const bnext = this._trafficWaypoint(b);
          const bdx = bnext.x - b.x, bdy = bnext.y - b.y, bd = Math.hypot(bdx, bdy);
          const alignment = bd > 20 ? (bdx * ax + bdy * ay) / bd : 0;
          const adx = a._goalX - a.x, ady = a._goalY - a.y, al = Math.max(1, Math.hypot(adx, ady));
          const bgx = b._goalX - b.x, bgy = b._goalY - b.y, bl = Math.max(1, Math.hypot(bgx, bgy));
          // A nearby path corner is not arrival. Treating the lead company
          // as stationary at each rock turn told it to hold indefinitely
          // while its equally fast followers tried to pass through it.
          const bMoving = b.order !== 'hold' && !(b.order === 'auto' && b._tactic && b._tactic.hold) && bl > 70;
          if (this._localMountedPass(a, b, ax, ay, bMoving, alignment, bd > 20 ? bdx/bd : NaN, bd > 20 ? bdy/bd : NaN) || lateralMiss) continue;
          const sameStream = (adx * bgx + ady * bgy) / (al * bl) > .96 && bgx * ox + bgy * oy > 0;
          if (bMoving && sameStream && TYPES[b.type].speed >= TYPES[a.type].speed * .94) continue;
          if (this._automaticCrossing(a, b, ax, ay, bdx, bdy, bd)) {
            const pair = Math.min(a.id, b.id) + ':' + Math.max(a.id, b.id);
            if (this._crossingCooldowns.has(pair) || !this._crossingFootprintsMeet(a, b, ax, ay, 3)) continue;
            this._yieldAutomaticCrossing(a, b, ax, ay);
            break;
          }
          const departingCommand = b.order === 'move' || b.order === 'charge' || this._advancingAttack(b) || (b.fallingBack && b._manualFallback);
          if (departingCommand && bMoving && bdx * ox + bdy * oy > 0) {
            // Let a commanded body leave a follower. Fast horses can close
            // that gap before the foot ranks finish turning; assigning a
            // new held passage then pulled them back against their order.
            // Ordinary swept collision still governs every actual crossing.
            continue;
          }
          if (a.order === 'auto' && b.order === 'auto' && bMoving && alignment > 0 && alignment < .94) {
            const relativeX = bdx / bd * TYPES[b.type].speed - ax * TYPES[a.type].speed;
            const relativeY = bdy / bd * TYPES[b.type].speed - ay * TYPES[a.type].speed;
            // Courses toward separate wings may overlap in projection while
            // already opening away from one another. A shared lane here
            // would pull one whole body along the other's opposite wing.
            if ((ox + relativeX * 2) ** 2 + (oy + relativeY * 2) ** 2 > ox * ox + oy * oy) continue;
          }
          if (bMoving && alignment > .75 && TYPES[b.type].speed >= TYPES[a.type].speed * .94 && along > (a.depth + b.depth) * .4) continue;
          if (bMoving && alignment > .94 && this._startParallelPass(a, b, ax, ay)) break;
          if (a.order === 'auto' && b.order === 'auto' && (alignment < -.6 || !bMoving) && this._startParallelPass(a, b, ax, ay, true)) break;
          const manualAdvance = a.order === 'move' || a.order === 'charge' || b.order === 'move' || b.order === 'charge';
          if (manualAdvance && bMoving && alignment > .94) {
            // A manual advance starts before distant, co-moving followers
            // ask for passing lanes. This also applies when only the leading
            // formation was commanded and its rear allies remain automatic.
            const front = this._projectBounds(a, ax, ay).hi;
            const rear = this._projectBounds(b, ax, ay).lo;
            if (rear - front > 48) continue;
          }
          // In open ground, two moving bodies on opposing courses do not
          // need to reform into interleaved columns. Their local moves still
          // collide, and the automatic formation can use a clear bypass.
          if (bMoving && alignment < .6 && (a.order === 'move' || b.order === 'move' || a._withdrawLeg || b._withdrawLeg) &&
              !a._bridge && !b._bridge && a._terrainColumns === a._shapeColumns && b._terrainColumns === b._shapeColumns) {
            if (a.order === 'auto' && this._startParallelPass(a, b, ax, ay, true)) break;
            if (b.order === 'auto' && bd > 20) this._startParallelPass(b, a, bdx / bd, bdy / bd, true);
            continue;
          }
          const spacing = Math.max(a._shapeSpacing, b._shapeSpacing);
          const pitch = Math.max(spacing * 2, (TYPES[a.type].radius + TYPES[b.type].radius) * 2 + 12);
          const plan = { a: a.id, b: b.id, ax, ay, origin: (-ay * a.x + ax * a.y + -ay * b.x + ax * b.y) / 2, pitch, started: this.time, marching: bMoving && alignment > .94,
            intentA: a.order + ':' + a.fallingBack + ':' + (a._rallyTime > 0), intentB: b.order + ':' + b.fallingBack + ':' + (b._rallyTime > 0) };
          const attach = (r, phase, hold) => {
            const usableWidth = Math.min(r.width, r._terrainColumns * r._shapeSpacing);
            const terrainLimit = r._terrainPressure > .01 ? Math.floor((usableWidth - pitch * .5) / pitch) : Infinity;
            const maxLanes = Math.max(1, Math.floor(r.width / (pitch * 1.2)));
            const lanes = Math.max(1, Math.min(maxLanes, terrainLimit));
            const center = -ay * r.x + ax * r.y;
            r._traffic = { plan, phase, lanes, maxLanes, base: Math.round((center - plan.origin) / pitch - (lanes - 1) / 2), hold, anchorX: r.x, anchorY: r.y };
            this._assignTrafficSlots(r);
            r.maneuver = 'threading'; r.trafficPressure = .65;
            r.cohesion = Math.max(20, r.cohesion - 8);
          };
          attach(a, -.25, false);
          attach(b, .25, !bMoving || Math.abs(alignment) < .6);
          this._trafficPlans.push(plan);
          break;
        }
      }
    }

    _stepRegiments(dt) {
      for (const r of this.regiments) {
        if (r.alive <= 0) continue;
        const marchStartX = r.x, marchStartY = r.y, marchStartAngle = r._marchAngle;
        // Strategy/command state is shared by the whole regiment during a
        // fixed step. Read the optional plan object once, not once per body.
        const activeTactic = r.order === 'auto' && !r.fallingBack && r._rallyTime <= 0 ? r._tactic : null;
        r._tacticalMovement = !!(activeTactic && (activeTactic.transit || activeTactic.hold));
        r._tacticalHold = !!(activeTactic && activeTactic.hold);
        if (r.order === 'face') {
          r._marchActive = false; r._deployCombat = false; r._deployMissiles = false;
          r._speedFactor = this._formationFactor(r, 'speed');
          const progress = clamp((this.time - r._faceStart - .35) / Math.max(.25, r._faceDuration - .35), 0, 1);
          r.facing = r._faceFrom + r._faceDelta * progress;
          r.footprintFacing = r.facing; r.commandTurnRemaining = Math.max(0, r._faceStart + r._faceDuration - this.time);
          if (progress >= 1 && r._faceError < Math.max(7, r._spacing * .6)) {
            r.order = 'hold'; r.facing = r.commandFacing; r._travelAngle = r.facing;
            for (const id of r.unitIds) { const u = this.units[id]; if (u.alive && !u.routed) u._holdPosition = { x: u.x, y: u.y }; }
            this._commandFeedback(r);
          }
          continue;
        }
        r._pathTimer -= dt;
        const t = TYPES[r.type];
        const terrain = this.terrainAt(r.x, r.y);
        const wanted = r._traffic ? Math.min(r._terrainColumns, r._traffic.lanes) : r._terrainColumns;
        r._adaptiveColumns += clamp(wanted - r._adaptiveColumns, -dt * 3, dt * 2);
        r._adaptiveOffset += clamp(r._terrainOffset - r._adaptiveOffset, -dt * t.speed * .7, dt * t.speed * .7);
        r._marchSqueeze += clamp(r._forestCompression - r._marchSqueeze, -dt * .16, dt * .12);
        // Commit once to the corridor's target file count. Re-sorting at
        // every rounded intermediate width caused repeated rank exchanges.
        const columns = Math.max(1, Math.round(wanted));
        if (r._travelSlots && !this._marchingBody(r) && (columns !== r._travelSlotColumns || r._travelSlotCount - r.alive > Math.max(4, r.alive * .1))) this._assignTravelSlots(r, columns);
        r.trafficPressure += ((r._traffic && !r._traffic.plan.yielding ? r._traffic.plan.parallel ? .16 : .65 : 0) - r.trafficPressure) * Math.min(1, dt * 1.8);
        if (r._traffic) {
          {
            const plan = r._traffic.plan;
            const limit = plan.parallel || plan.yielding ? r._terrainColumns : Math.max(1, Math.floor((r._terrainColumns * r._shapeSpacing - plan.pitch * .5) / plan.pitch));
            const nextLanes = Math.min(r._traffic.maxLanes || r._traffic.lanes, limit);
            if (nextLanes !== r._traffic.lanes) {
              r._traffic.lanes = nextLanes;
              r._traffic.base = plan.parallel || plan.yielding ? -(nextLanes - 1) / 2 : Math.round((-plan.ay * r.x + plan.ax * r.y - plan.origin) / plan.pitch - (nextLanes - 1) / 2);
              this._assignTrafficSlots(r);
            }
          }
          r.maneuver = r._traffic.plan.yielding || r._traffic.plan.parallel ? 'formed' : 'threading';
          r.footprintFacing = r._traffic.plan.yielding ? r.facing : r._traffic.plan.parallel ? r._traffic.plan.bodyFacing : Math.atan2(r._traffic.plan.ay, r._traffic.plan.ax);
          r.effectiveWidth = r._traffic.lanes * r._traffic.plan.pitch;
          r.effectiveDepth = Math.ceil(r.alive / r._traffic.lanes) * r._shapeSpacing;
        } else {
          r.effectiveWidth = columns * r._shapeSpacing * r._marchSqueeze;
          r.effectiveDepth = Math.ceil(r.alive / columns) * r._shapeSpacing;
          const adapted = r._terrainColumns < r._shapeColumns || r._terrainPressure > .2;
          r.maneuver = adapted ? 'narrowing' : Math.abs(r._adaptiveColumns - r._shapeColumns) > .1 || r.deformation > 2 ? 'reforming' : 'formed';
          r.footprintFacing = r.maneuver === 'formed' ? r.facing : r._travelAngle;
          if (r.maneuver === 'formed' && r.formation === 'wedge') r.effectiveDepth = r.effectiveWidth;
        }
        const deformationTarget = r._traffic ? r._traffic.plan.yielding || r._traffic.plan.parallel ? 0 : 48 : Math.max(0, (1 - columns / Math.max(1, r._shapeColumns)) * 80 + r._terrainPressure * 22 + (1 - r._marchSqueeze) * 28);
        r.deformation += (deformationTarget - r.deformation) * Math.min(1, dt * 1.3);
        r._movementScale = r._traffic ? r._traffic.plan.yielding ? 1 : r._traffic.plan.parallel ? .94 : .64 : r.maneuver === 'narrowing' ? .86 : 1;
        if (this._canDeployForCombat(r)) {
          r.effectiveWidth = r.frontageWidth; r.effectiveDepth = r._combatRows * r._combatSpacing;
          r.footprintFacing = Math.atan2(r._frontNY, r._frontNX);
          if (r._terrainPressure < .2) r.maneuver = 'formed';
          r._movementScale = r._terrainPressure > .2 ? .9 : 1;
        }
        if (t.brace || t.frontalShield || t.push) {
          // Contact slots may wheel a wing onto a flank. Its defenses follow
          // that coherent rank axis gradually, never a soldier's attack look.
          // Held ranks keep the direction the player accepted with Face.
          const turning = this._canDeployForCombat(r) ? Math.atan2(Math.sin(r.footprintFacing - r.facing), Math.cos(r.footprintFacing - r.facing)) : 0;
          r._frontTurning = Math.abs(turning) > .06;
          if (r._frontTurning) r.facing += clamp(turning, -dt * .8, dt * .8);
        }
        const tactic = r.order === 'auto' && !r.fallingBack && r._rallyTime <= 0 ? r._tactic : null;
        if (tactic && Number.isFinite(tactic.speedScale)) r._movementScale *= tactic.speedScale;
        if (tactic && tactic.hold && Number.isFinite(tactic.faceAngle)) {
          const turn = Math.atan2(Math.sin(tactic.faceAngle - r.facing), Math.cos(tactic.faceAngle - r.facing));
          r._frontTurning = Math.abs(turn) > .05;
          r.facing += clamp(turn, -dt * 2.4, dt * 2.4);
          r._marchAngle = r.facing; r.footprintFacing = r.facing;
        }
        const directMarch = r.order === 'move' || this._advancingAttack(r);
        const pace = directMarch && t.mounted && this._marchingBody(r) ? 1 : .88;
        let speed = t.speed * pace * terrain.speed * this._formationFactor(r, 'speed') * (r.fallingBack ? 1 : r._collisionSlow) * r._movementScale;
        if (directMarch) {
          // A new heading belongs to every soldier immediately. The footprint
          // keeps its world-space offsets until the destination is reached.
          r.facing = r._travelAngle; r.footprintFacing = r._marchAngle;
        } else if (this._marchingBody(r)) {
          let turn = Math.atan2(Math.sin(r._travelAngle - r._marchAngle), Math.cos(r._travelAngle - r._marchAngle));
          if (Math.abs(turn) > 2.1) {
            // An about-face changes which existing rank leads. It must not
            // send the old front and rear through one another to swap places.
            r._marchAngle += Math.sign(turn) * Math.PI; r._marchTurnUntil = this.time + .28;
            for (const id of r.unitIds) {
              const u = this.units[id];
              if (!u.alive || u.routed) continue;
              u._marchOX = -u._marchOX; u._marchOY = -u._marchOY;
              u._marchTurnFrom = u.angle; u._marchTurnDelta = Math.atan2(Math.sin(r._travelAngle - u.angle), Math.cos(r._travelAngle - u.angle));
              u._marchTurnStart = this.time; u._marchTurnEnd = r._marchTurnUntil;
            }
            turn = Math.atan2(Math.sin(r._travelAngle - r._marchAngle), Math.cos(r._travelAngle - r._marchAngle));
          }
          const amount = Math.min(Math.abs(turn), dt * Math.min(1.4, t.speed * .65 / Math.max(35, r._marchRadius)));
          r._marchAngle += Math.sign(turn) * amount; r.facing = r._marchAngle; r.footprintFacing = r.facing;
          speed *= clamp(1 - Math.max(0, r._marchLag - 22) / 95, .2, 1) * clamp(1 - Math.abs(turn) / Math.PI, .65, 1);
          if (this.time < r._marchTurnUntil) speed = 0;
        }
        if (Math.hypot(r.x - r._centroidX, r.y - r._centroidY) > Math.max(170, r.effectiveDepth * .65)) speed *= .35;
        r.rallyCooldown = Math.max(0, r.rallyCooldown - dt);
        r._rallyTime = Math.max(0, r._rallyTime - dt);
        if (r.fallingBack) r._fallbackTime += dt;
        r.commandTurnRemaining = r.fallingBack ? Math.max(0, r._retreatTurnUntil - this.time) : 0;
        if (r.fallingBack) r.commandStatus = r.commandTurnRemaining > 0 ? 'turning' : 'withdrawing';
        if (terrain.type === 'forest') speed *= Number.isFinite(t.forestSpeed) ? t.forestSpeed : t.mounted ? .62 : 1;
        // Mounted Charge slots must advance at the pace requested by their
        // riders. The real-body guide below still limits lead, tail and turns.
        if (r.order === 'charge') speed *= this.impacts && this.impacts.isHorse(r) && this._marchingBody(r) ? 1.15 / .88 : 1.12;
        if (r.order === 'auto' && this.config.armies[r.team].doctrine === 'aggressive') speed *= 1.07;
        let waypoint = r._path[0];
        while (waypoint && Math.hypot(waypoint.x - r.x, waypoint.y - r.y) < Math.max(18, speed * dt)) {
          r._path.shift(); waypoint = r._path[0];
        }
        if (r._lancerPass && waypoint && !r._traffic) {
          waypoint = { x: r.x + r._lancerPass.nx * 120, y: r.y + r._lancerPass.ny * 120 };
        }
        if (r._traffic && r._traffic.plan.parallel) {
          const plan = r._traffic.plan, along = r.x * plan.ax + r.y * plan.ay + 260;
          waypoint = { x: along * plan.ax - plan.origin * plan.ay, y: along * plan.ay + plan.origin * plan.ax };
        }
        if (waypoint && !(r._traffic && r._traffic.hold) && r.commandTurnRemaining <= 0) {
          const dx = waypoint.x - r.x, dy = waypoint.y - r.y;
          const distance = Math.hypot(dx, dy);
          if (distance > 1) {
            const amount = Math.min(distance, speed * dt);
            const nx = r.x + dx / distance * amount, ny = r.y + dy / distance * amount;
            if (!this._blocked(nx, ny)) { r.x = nx; r.y = ny; }
            else r._pathTimer = 0;
            if (!this._marchingBody(r) && (r.order === 'move' || this._advancingAttack(r) || r.fallingBack || r.maneuver === 'narrowing')) { r.facing = Math.atan2(dy, dx); r._marchAngle = r.facing; }
          }
        }
        r._bridge = null;
        for (const bridge of this._bridges) {
          const horizontal = bridge.w >= bridge.h;
          const along = horizontal ? r.x : r.y, side = horizontal ? r.y : r.x;
          const start = horizontal ? bridge.x : bridge.y, length = horizontal ? bridge.w : bridge.h;
          const middle = horizontal ? bridge.y + bridge.h / 2 : bridge.x + bridge.w / 2;
          if (along > start - r.depth * .6 - 80 && along < start + length + r.depth * .6 + 80 && Math.abs(side - middle) < Math.max(300, (horizontal ? bridge.h : bridge.w) / 2)) { r._bridge = bridge; break; }
        }
        // These properties are shared by every member during this fixed
        // step. Compute them once per regiment instead of thousands of
        // repeated predicate/formation/trigonometry calls in the hot loop.
        r._marchActive = this._marchingBody(r);
        r._deployCombat = this._canDeployForCombat(r); r._deployMissiles = this._canDeployMissiles(r);
        r._marchNX = Math.cos(r._marchAngle); r._marchNY = Math.sin(r._marchAngle);
        const adapted = r.maneuver !== 'formed' || Math.abs(r._adaptiveOffset) > 1;
        r._slotAngle = r._bridge ? r._bridge.w >= r._bridge.h ? (r._goalX >= r.x ? 0 : Math.PI) : (r._goalY >= r.y ? Math.PI / 2 : -Math.PI / 2) : adapted || r.order === 'move' || this._advancingAttack(r) ? r._travelAngle : r.facing;
        r._slotNX = Math.cos(r._slotAngle); r._slotNY = Math.sin(r._slotAngle);
        r._speedFactor = this._formationFactor(r, 'speed');
        r._steadyBraceScale = tactic && tactic.movingBrace && (r.formation === 'line' || r.formation === 'shieldwall') && !r._frontTurning && !r._traffic && !r._bridge && r._terrainPressure < .2 && r.cohesion >= 45 &&
          Math.abs(Math.atan2(Math.sin(r._travelAngle - r.facing), Math.cos(r._travelAngle - r.facing))) < .045 &&
          Math.abs(r._marchAngle - marchStartAngle) < .002 ? r.type === 'pikeman' ? .58 : r.type === 'hoplite' ? .52 : r.type === 'halberdier' ? .40 : .24 : 0;
        if (this.impacts) this.impacts.guideRegiment(r, dt, marchStartX, marchStartY, marchStartAngle);
        if (directMarch) r.facing = r._travelAngle;
        r._rigidAngleDelta = Math.abs(r._marchAngle - marchStartAngle);
      }
    }

    _stepClearMarches(dt) {
      this.fastMarchUnits = 0;
      if (this.units.length <= 20000) return;
      const neighborTravel = 162 * dt * 1.7;
      for (const r of this.regiments) {
        const t = TYPES[r.type];
        const horse = this.impacts && this.impacts.isHorse(r);
        if (r.alive <= 0 || r._rampartJob || (t.mounted && !horse) || t.siege || t.commander || !r._marchActive || r._combatActive || r._missileActive ||
            (r.order !== 'auto' && r.order !== 'move' && !(r.order === 'attack' && this._advancingAttack(r))) ||
            r._traffic || r._bridge || r.fallingBack || r._rallyTime > 0 || r._frontTurning || r._tacticalMovement || r._tacticalHold ||
            r._rigidAngleDelta > 1e-10 || r._marchTurnUntil > this.time || r._formationPressureUntil > this.time ||
            (r._tactic && r._tactic.movingBrace) ||
            r._terrainPressure > .001 || r._nearestEnemyDistance < Math.max(1700, t.range + r.depth + 400) ||
            r.cohesion < 80 || this.time < r._rigidCheckAfter) continue;
        const speed = t.speed * r._speedFactor * r._movementScale, limit = speed * dt;
        const cx = r._rigidCenterX / r.alive, cy = r._rigidCenterY / r.alive;
        const dx = r.x - cx, dy = r.y - cy, distance = Math.hypot(dx, dy);
        if (distance < .001 || limit <= 0) continue;
        let amount = Math.min(limit, distance), mx = dx / distance * amount, my = dy / distance * amount;
        const padding = amount + neighborTravel + 2;
        const left = r._rigidMinX - padding, right = r._rigidMaxX + padding, top = r._rigidMinY - padding, bottom = r._rigidMaxY + padding;
        if (left < 12 || top < 12 || right > this.width - 12 || bottom > this.height - 12) continue;
        let blocked = false;
        for (const region of this._regions) if (left < region.x + region.w && right > region.x && top < region.y + region.h && bottom > region.y) { blocked = true; break; }
        if (blocked) continue;
        for (const other of this.regiments) if (other !== r && other.alive > 0 && left < other._rigidMaxX && right > other._rigidMinX && top < other._rigidMaxY && bottom > other._rigidMinY) { blocked = true; break; }
        if (blocked) continue;
        const fresh = r._rigidCertifiedEpoch !== r._commandEpoch || r._rigidContinuityTick !== this._tick - 1 || r._rigidCount !== r.alive;
        const nx = r._marchNX, ny = r._marchNY;
        // With no projectiles or active forced motion, a continuously
        // certified body cannot acquire a new internal contact between these
        // identical translations. External swept bounds are still checked.
        const inspect = fresh || this.projectiles.length > 0 || (this.impacts && this.impacts.active.size > 0) || r._recentHits > 0;
        if (inspect) for (const id of r.unitIds) {
          const u = this.units[id];
          if (!u.alive || u.routed) continue;
          if (u.target >= 0 || u._contact || u._impactUntil > this.time || u._pushUntil > this.time || u._marchTurnEnd > this.time ||
              u._terrain.type !== 'plain' || u._portalGoal || u._localPath.length || u._jamTime > .1 ||
              (horse && (Math.abs(u._friendlyOffsetX) + Math.abs(u._friendlyOffsetY) > .01 || u._friendlyUntil > this.time)) ||
              (fresh && Math.hypot(u.x - cx - u._marchOX * nx + u._marchOY * r._marchSqueeze * ny, u.y - cy - u._marchOX * ny - u._marchOY * r._marchSqueeze * nx) > 3) ||
              (fresh && this._collideMovement(u, 0, 0, 0).blocked)) { blocked = true; break; }
        }
        if (blocked) { r._rigidCheckAfter = this.time + .5; continue; }
        const driveNX = dx / distance, driveNY = dy / distance;
        if (horse) {
          let drive = speed;
          for (const id of r.unitIds) {
            const u = this.units[id];
            if (!u.alive || u.routed) continue;
            this.impacts.beginUnit(u);
            drive = Math.min(drive, this.impacts.speedFor(u, speed, driveNX, driveNY, dt));
          }
          amount = Math.min(amount, drive * dt); mx = dx / distance * amount; my = dy / distance * amount;
        }
        // This translation preserves every internal distance. The expanded
        // swept bounds above exclude all external bodies, obstacles, terrain
        // transitions, and edges for this entire fixed step. Contacts always
        // return to the individual collision solver on the following step.
        const vx = mx / dt, vy = my / dt, heading = Math.atan2(my, mx);
        for (const id of r.unitIds) {
          const u = this.units[id];
          if (!u.alive || u.routed) continue;
          u.anim += dt; u.attackFlash = Math.max(0, u.attackFlash - dt * 3.7);
          u._cooldown -= dt; u._meleeCooldown -= dt; u._sense -= dt; u._terrainTimer -= dt; u._contactSense -= dt;
          u._localTimer -= dt;
          if (u._localTimer <= 0) u._localTimer = .7 + u._jitter * .55;
          if (u._sense <= 0) u._sense = .65 + u._jitter * .3;
          if (u._terrainTimer <= 0) u._terrainTimer = .4 + u._jitter * .25;
          u._flex = Math.max(0, u._flex - dt * .7); u._stuck = Math.max(0, u._stuck - dt * 2); u._jamTime = Math.max(0, u._jamTime - dt * 2);
          u.x += mx; u.y += my; u._vx = vx; u._vy = vy; u._contact = false;
          u._moveIntentNX = driveNX; u._moveIntentNY = driveNY; u._moveIntentTime = this.time;
          u._motionTick = this._tick; u._motionDistance = horse ? 0 : amount; u.travelDistance += amount;
          u._braceTime = 0; u._idleContactTime = 0; u.state = 'move';
          if (r.order === 'move') u.angle += clamp(Math.atan2(Math.sin(heading - u.angle), Math.cos(heading - u.angle)), -12 * dt, 12 * dt);
          else u.angle = heading;
          if (horse) this.impacts.afterMove(u, mx, my, dt, amount, false);
          else if (this.time >= u._chargeCheckedUntil) u._charge = Math.min(200, u._charge + amount);
          u._bulkStepTick = this._tick; this._bodyX[id] = u.x; this._bodyY[id] = u.y;
          this.fastMarchUnits++;
        }
        r._moveAttempts += r.alive; this._bodyHashDrift = Math.max(this._bodyHashDrift, amount);
        r._rigidCertifiedEpoch = r._commandEpoch; r._rigidContinuityTick = this._tick; r._rigidCount = r.alive;
      }
    }

    _formationGoal(u, r, rigid) {
      if(r._rampartJob)return u._onRampart&&!u._rampartPath.length&&!r._rampartJob.leaving?(r._rampartSlots[u._rampartSlot]||u):{x:u.x,y:u.y};
      const formation = FORMATIONS[r.formation] || {};
      if (r.order === 'hold' && !r._traffic && u._holdPosition) return u._holdPosition;
      const tacticalHold = this._inUnitStep ? r._tacticalHold : r.order === 'auto' && r._tactic && r._tactic.hold && !r.fallingBack && r._rallyTime <= 0;
      if (tacticalHold && !r._traffic && Number.isFinite(u._tacticHoldX)) return { x: u._tacticHoldX, y: u._tacticHoldY };
      if (r.order === 'face') {
        const angle = r.facing - r._faceFrom, nx = Math.cos(angle), ny = Math.sin(angle);
        return { x: r._holdX + u._faceOX * nx - u._faceOY * ny, y: r._holdY + u._faceOX * ny + u._faceOY * nx };
      }
      if (r.fallingBack && r._manualFallback && r._withdrawLeg && !r._traffic) return { x: r._withdrawGoalX + u._withdrawOffsetX, y: r._withdrawGoalY + u._withdrawOffsetY };
      if (this.phase !== 'deployment' && this._canDeployMissiles(r)) return this._missileGoal(u, r, rigid);
      if (this._canDeployForCombat(r) && this.phase !== 'deployment') return this._combatGoal(u, r, rigid);
      if (r._traffic) {
        const traffic = r._traffic, plan = traffic.plan;
        if (plan.yielding) return { x: traffic.anchorX + u._orderOffsetX, y: traffic.anchorY + u._orderOffsetY };
        if (plan.parallel) return { x: r.x + u._orderOffsetX, y: r.y + u._orderOffsetY };
        const columns = traffic.lanes, rows = traffic.rows || Math.ceil(r.alive / columns);
        const slot = u._trafficSlot >= 0 ? u._trafficSlot : u._slot;
        const row = Math.floor(slot / columns), lane = slot % columns;
        const forward = Math.cos(r._travelAngle) * plan.ax + Math.sin(r._travelAngle) * plan.ay >= 0 ? 1 : -1;
        const anchorX = traffic.hold ? traffic.anchorX : r.x, anchorY = traffic.hold ? traffic.anchorY : r.y;
        const along = anchorX * plan.ax + anchorY * plan.ay + ((rows - 1) / 2 - row) * r._shapeSpacing * forward;
        const across = plan.origin + (traffic.base + lane + traffic.phase) * plan.pitch;
        return { x: along * plan.ax - across * plan.ay, y: along * plan.ay + across * plan.ax };
      }
      if (this._marchingBody(r) && (r.order === 'move' || this._advancingAttack(r))) {
        const waypoint = r._path[0];
        const dx = (waypoint ? waypoint.x : r._goalX) - r.x, dy = (waypoint ? waypoint.y : r._goalY) - r.y;
        const lead = rigid ? 0 : Math.min(1, 24 / Math.max(1, Math.hypot(dx, dy)));
        return { x: r.x + u._orderOffsetX + dx * lead, y: r.y + u._orderOffsetY + dy * lead };
      }
      if (this._marchingBody(r)) {
        const nx = this._inUnitStep ? r._marchNX : Math.cos(r._marchAngle), ny = this._inUnitStep ? r._marchNY : Math.sin(r._marchAngle);
        const lateral = u._marchOY * r._marchSqueeze;
        const goal = { x: r.x + u._marchOX * nx - lateral * ny, y: r.y + u._marchOX * ny + lateral * nx };
        return rigid ? goal : this._elasticGoal(u, r, goal, r._marchAngle, nx, ny);
      }
      const adapted = (r.maneuver !== 'formed' || Math.abs(r._adaptiveOffset) > 1) && this.phase !== 'deployment';
      let columns = adapted ? Math.max(1, r._travelSlots ? r._travelSlotColumns : Math.round(r._adaptiveColumns)) : formation.files ? Math.min(r.alive, formation.files) : formation.ranks ? Math.ceil(r.alive / formation.ranks) : r.type === 'musketeer' && r.formation !== 'wedge' ? Math.ceil(r.alive / 3) : r.formation === 'shieldwall' ? Math.ceil(r._columns * 1.25) : r._columns;
      let spacing = adapted ? r._shapeSpacing : r._spacing * (formation.spacing || 1);
      if (r._bridge) columns = Math.max(1, Math.min(columns, Math.floor((Math.min(r._bridge.w, r._bridge.h) - 55) / spacing)));
      if (r._bridge) {
        const bridge = r._bridge, horizontal = bridge.w >= bridge.h;
        const start = horizontal ? bridge.x : bridge.y, length = horizontal ? bridge.w : bridge.h, middle = start + length / 2;
        const goal = horizontal ? r._goalX : r._goalY, along = horizontal ? u.x : u.y;
        const direction = goal > middle + 60 ? 1 : goal < middle - 60 ? -1 : 0;
        const notAcross = direction === 1 ? along < start + length + 90 : direction === -1 ? along > start - 90 : false;
        if (notAcross) {
          const lane = u._slot % columns;
          const across = (lane - (columns - 1) / 2) * spacing;
          return horizontal ? { x: direction === 1 ? bridge.x + bridge.w + 160 : bridge.x - 160, y: bridge.y + bridge.h / 2 + across } :
            { x: bridge.x + bridge.w / 2 - direction * across, y: direction === 1 ? bridge.y + bridge.h + 160 : bridge.y - 160 };
        }
      }
      const slot = r._travelSlots && u._moveSlot >= 0 ? u._moveSlot : u._slot;
      let rows = Math.ceil(r.alive / columns), row = Math.floor(slot / columns), col = slot % columns;
      if (r.formation === 'wedge' && !r._bridge && !adapted) {
        rows = Math.ceil((Math.sqrt(8 * r.alive + 1) - 1) / 2);
        row = Math.floor((Math.sqrt(8 * slot + 1) - 1) / 2);
        columns = row + 1; col = slot - row * (row + 1) / 2;
      }
      const ox = ((rows - 1) / 2 - row) * spacing;
      const oy = (col - (columns - 1) / 2) * spacing * (adapted ? r._marchSqueeze : 1) + (adapted ? r._adaptiveOffset : 0);
      const angle = this._inUnitStep ? r._slotAngle : r._bridge ? r._bridge.w >= r._bridge.h ? (r._goalX >= r.x ? 0 : Math.PI) : (r._goalY >= r.y ? Math.PI / 2 : -Math.PI / 2) : adapted || r.order === 'move' || this._advancingAttack(r) ? r._travelAngle : r.facing;
      const cos = this._inUnitStep ? r._slotNX : Math.cos(angle), sin = this._inUnitStep ? r._slotNY : Math.sin(angle);
      const goal = { x: r.x + ox * cos - oy * sin, y: r.y + ox * sin + oy * cos };
      if (r._travelSlots && this.time < r._approachBlendUntil && !r._bridge && r._terrainPressure < .2 && !(r.order === 'charge' && TYPES[r.type].ranged)) {
        // Start marching from the body the player can actually see, then
        // settle into the new ranks. Requiring an immediate perfect layout
        // around a recentered standard sent rear ranks backward first.
        // Combat, firing positions, passing lanes, and terrain adaptation
        // take precedence.
        const blend = clamp((r._approachBlendUntil - this.time) / 2, 0, 1);
        goal.x += (r.x + u._orderOffsetX - goal.x) * blend;
        goal.y += (r.y + u._orderOffsetY - goal.y) * blend;
      }
      return rigid ? goal : this._elasticGoal(u, r, goal, angle, cos, sin);
    }

    _marchingBody(r) {
      if (this._inUnitStep) return r._marchActive;
      return this.phase !== 'deployment' && (r.order === 'auto' || r.order === 'move' || r.order === 'attack' || r.order === 'charge') && r._marchLayout && !r.fallingBack && r._rallyTime <= 0 && !r._combatActive && !r._missileActive && !r._traffic && !r._bridge && r._terrainColumns === r._shapeColumns && Math.abs(r._adaptiveColumns - r._shapeColumns) < .5;
    }

    _waterGoal(u, gx, gy) {
      for (const water of this._waters) {
        if (water.h <= water.w) continue;
        const crosses = (u.x < water.x && gx > water.x) || (u.x > water.x + water.w && gx < water.x + water.w) || (u.x >= water.x && u.x <= water.x + water.w);
        if (!crosses) continue;
        let bridge = null, best = Infinity;
        for (const b of this._bridges) {
          if (b.x > water.x + water.w || b.x + b.w < water.x) continue;
          const score = Math.abs(u.y - (b.y + b.h / 2)) + Math.abs(gy - (b.y + b.h / 2)) * .25;
          if (score < best) { best = score; bridge = b; }
        }
        if (!bridge) continue;
        const direction = gx >= u.x ? 1 : -1;
        const safeY = clamp(gy, bridge.y + 30, bridge.y + bridge.h - 30);
        if (u.y < bridge.y + 22 || u.y > bridge.y + bridge.h - 22) {
          return { x: direction === 1 ? Math.min(u.x, water.x - 30) : Math.max(u.x, water.x + water.w + 30), y: safeY };
        }
        return { x: direction === 1 ? water.x + water.w + 45 : water.x - 45, y: safeY };
      }
      return { x: gx, y: gy };
    }

    _localWaypoint(u) {
      const path = u._localPath, corner = path[0];
      if (!corner || path.length < 2) return corner;
      if (u._portalCorner === corner && this.time < u._portalUntil) return u._portalGoal;
      const next = path[1], dx = next.x - corner.x, dy = next.y - corner.y;
      const length = Math.max(1, Math.hypot(dx, dy)), px = -dy / length, py = dx / length;
      const clearance = u._radius + 3;
      // Preserve the soldier's lateral approach to each turn. Different files
      // can use different parts of a broad portal and immediately fan out after
      // its exit, while the actual body collision and terrain tests still run.
      let offset = clamp((u.x - corner.x) * px + (u.y - corner.y) * py, -105, 105);
      const r = this.regiments[u.regiment];
      const slotOffset = ((u._slot % Math.max(2, r._columns)) / Math.max(1, r._columns - 1) - .5) * 100;
      if (Math.abs(offset) < 10) offset = slotOffset;
      for (let scale = 1; scale >= 0; scale -= .25) {
        const x = corner.x + px * offset * scale, y = corner.y + py * offset * scale;
        if (!this._bodyBlocked(x, y, clearance) && this._lineClear(u.x, u.y, x, y, clearance) && this._lineClear(x, y, next.x, next.y, clearance)) {
          u._portalCorner = corner; u._portalGoal = { x, y }; u._portalUntil = this.time + .6 + u._jitter * .3;
          return u._portalGoal;
        }
      }
      return corner;
    }

    _localNavigation(u, target, dt) {
      u._localTimer -= dt;
      if (u._localPath.length) {
        let waypoint = this._localWaypoint(u);
        while (waypoint && Math.hypot(waypoint.x - u.x, waypoint.y - u.y) < Math.max(16, u._radius * 2)) {
          u._localPath.shift(); waypoint = this._localWaypoint(u);
        }
        if (u._localPath.length && u._localTimer > 0) return waypoint;
      }
      if (u._localTimer > 0) return target;
      u._localTimer = .7 + u._jitter * .55;
      let nearby = false;
      for (const block of this._blocks) {
        if (inside(u.x, u.y, block, 180) || inside(target.x, target.y, block, u._radius + 6)) { nearby = true; break; }
      }
      if (!nearby) { u._localPath.length = 0; return target; }
      const safe = this._nearestPassable(target.x, target.y, u._radius + 2);
      if (this._lineClear(u.x, u.y, safe.x, safe.y, u._radius + 2)) { u._localPath.length = 0; return safe; }
      if (!u._localPath.length || Math.hypot(target.x - u._localGoalX, target.y - u._localGoalY) > 190 || u._stuck > 1.5) {
        u._localPath = this._findPath(u.x, u.y, safe.x, safe.y, u._radius + 3);
        u._localGoalX = target.x; u._localGoalY = target.y;
      }
      return this._localWaypoint(u) || safe;
    }

    _moveUnit(u, gx, gy, dt, speedScale) {
      const t = TYPES[u.type], impacts = this.impacts;
      const mountedImpact = impacts && (u.type === 'cavalry' || u.type === 'lancer' || u.type === 'horseArcher');
      const regiment = this.regiments[u.regiment];
      if (regiment._traffic && !regiment._traffic.plan.parallel) {
        const plan = regiment._traffic.plan;
        const lateral = (gx - u.x) * -plan.ay + (gy - u.y) * plan.ax;
        if (plan.marching && (regiment.id === plan.b || regiment.order === 'auto')) {
          // A moving lead formation can open lanes diagonally while pressing
          // forward. Rear ranks wait for room instead of marching backward
          // solely to stretch into the longer passing columns.
          const forward = (gx - u.x) * plan.ax + (gy - u.y) * plan.ay;
          if (forward < 0) { gx = u.x - plan.ay * lateral; gy = u.y + plan.ax * lateral; }
        } else if (Math.abs(lateral) > u._radius * .8) {
          // Vacate the shared lane first, then advance along it. The held
          // ranks visibly step aside; the incoming body is never exempted
          // from collision while its larger neighbors make room.
          gx = u.x - plan.ay * lateral; gy = u.y + plan.ax * lateral;
        }
      }
      if (!regiment._traffic && regiment._terrainColumns < regiment._shapeColumns && !regiment._combatActive && !regiment._missileActive &&
          !regiment.fallingBack && regiment.order !== 'hold' && regiment.order !== 'face') {
        const nx = Math.cos(regiment._travelAngle), ny = Math.sin(regiment._travelAngle);
        const forward = (gx - u.x) * nx + (gy - u.y) * ny;
        // A narrowing march changes which files follow, not the direction of
        // travel. Rear soldiers make room without backing up to build a tail.
        if (forward < 0) { gx -= nx * forward; gy -= ny * forward; }
      }
      if (mountedImpact) {
        const friendlyGoal = impacts.friendlyGoal(u, gx, gy);
        gx = friendlyGoal.x; gy = friendlyGoal.y;
      }
      if ((gx - u.x) * (gx - u.x) + (gy - u.y) * (gy - u.y) < 4) { u._vx = u._vy = 0; return false; }
      // An obstacle-free field has no water detour or local route to plan.
      // Keep navigation's timer phase identical while avoiding two temporary
      // points and their dispatch for every freely marching body. Solid-region
      // arrays retain breached structures, so their full solver stays active.
      let targetX = gx, targetY = gy;
      if (!this._blocks.length && !this._waters.length && !u._localPath.length && !u._portalGoal && !u._portalCorner && !u._detour) {
        u._localTimer -= dt;
        if (u._localTimer <= 0) u._localTimer = .7 + u._jitter * .55;
      } else {
        const target = this._localNavigation(u, this._waterGoal(u, gx, gy), dt);
        targetX = target.x; targetY = target.y;
      }
      const dx = targetX - u.x, dy = targetY - u.y;
      const distance = length2(dx, dy);
      if (distance < 2) { u._vx = u._vy = 0; return false; }
      const moveNX = dx / distance, moveNY = dy / distance;
      u._moveIntentNX = moveNX; u._moveIntentNY = moveNY; u._moveIntentTime = this.time;
      let speed = t.speed * (speedScale || 1) * Math.max(.2, u._terrain.speed) * (this._inUnitStep ? regiment._speedFactor : this._formationFactor(regiment, 'speed')) * regiment._movementScale;
      if (u._terrain.type === 'forest') speed *= Number.isFinite(t.forestSpeed) ? t.forestSpeed : t.mounted ? .62 : 1;
      if (mountedImpact) speed = impacts.speedFor(u, speed, moveNX, moveNY, dt);
      let amount = Math.min(distance, speed * dt);
      if (mountedImpact) {
        if (u.type !== 'horseArcher') impacts.beforeMove(u, moveNX, moveNY, amount);
        amount = Math.min(amount, u._mountDriveSpeed * dt);
      }
      if (impacts && !u.alive) return false;
      // Forced movement and voluntary motion share one per-step budget.
      // Reserve the solver's existing small separation allowance as well.
      if (impacts && u._motionTick === this._tick) amount = Math.min(amount, impacts.remainingMotion(u, dt) / 1.15);
      let mx = moveNX * amount, my = moveNY * amount;
      if (impacts && u._pressureTick === this._tick) {
        const constrained = impacts.constrainPressureMove(u, mx, my);
        if (constrained) { mx = constrained.x; my = constrained.y; amount = Math.hypot(mx, my); }
      }
      if (amount < .0001) { u._vx = u._vy = 0; return false; }
      if (mountedImpact) {
        // Courtesy steps finish before the ordinary collision query, whose
        // scratch buffers are shared. Both riders can make real lateral room.
        impacts.clearFriendly(u, moveNX, moveNY, amount, dt);
        const remaining = impacts.remainingMotion(u, dt) / 1.15;
        if (amount > remaining) { const scale = remaining / amount; mx *= scale; my *= scale; amount = remaining; }
        if (amount < .0001) { u._vx = u._vy = 0; return false; }
      }
      const collision = this._collideMovement(u, mx, my, amount);
      mx = collision.x; my = collision.y;
      regiment._moveAttempts++;
      if (collision.blocked) regiment._blockedMoves++;
      if (collision.ally) regiment._allyContacts++;
      if (collision.enemy) regiment._enemyContacts++;
      u._contact = collision.blocked;
      const oldX = u.x, oldY = u.y;
      const nx = clamp(u.x + mx, 12, this.width - 12), ny = clamp(u.y + my, 12, this.height - 12);
      const terrainBlocked = this._bodyBlocked(nx, ny, u._radius);
      if (!terrainBlocked) { u.x = nx; u.y = ny; }
      else if (!this._bodyBlocked(nx, u.y, u._radius)) {
        const slide = this._collideMovement(u, mx, 0, Math.abs(mx));
        if (!this._bodyBlocked(u.x + slide.x, u.y + slide.y, u._radius)) { u.x += slide.x; u.y += slide.y; }
      } else if (!this._bodyBlocked(u.x, ny, u._radius)) {
        const slide = this._collideMovement(u, 0, my, Math.abs(my));
        if (!this._bodyBlocked(u.x + slide.x, u.y + slide.y, u._radius)) { u.x += slide.x; u.y += slide.y; }
      }
      // Separation and terrain sliding can alter the requested direction.
      // Reject a trial step that would reclaim ground conceded this tick;
      // projecting it afterward would create a new, unchecked swept path.
      if (u._pressureTick === this._tick && (u.x - oldX) * u._pressureDX + (u.y - oldY) * u._pressureDY < -.000000001) {
        u.x = oldX; u.y = oldY;
      }
      if (terrainBlocked && Math.abs(u.x - oldX) + Math.abs(u.y - oldY) < amount * .25) {
        u._stuck += dt;
        if (u._stuck > .8) u._localTimer = 0;
      } else u._stuck = Math.max(0, u._stuck - dt * 2);
      const movedX = u.x - oldX, movedY = u.y - oldY, movedDistance = length2(movedX, movedY);
      u._vx = movedX / dt; u._vy = movedY / dt;
      if (impacts) {
        if (mountedImpact) impacts.afterMove(u, movedX, movedY, dt, movedDistance,
          (collision.ally || u._friendlyFlowTick === this._tick) && !collision.enemy && !terrainBlocked);
        else impacts.recordMove(u, movedDistance);
      } else { this._bodyX[u.id] = u.x; this._bodyY[u.id] = u.y; }
      u.travelDistance += movedDistance;
      if (collision.ally && movedDistance < amount * .28) u._jamTime += dt;
      else u._jamTime = Math.max(0, u._jamTime - dt * 2);
      if (Math.abs(u._vx) + Math.abs(u._vy) > 1) {
        const heading = Math.atan2(u._vy, u._vx);
        if (regiment.order === 'move' || regiment._manualFallback && regiment.fallingBack) {
          // Turning the sprite does not consume a soldier's movement step.
          const turn = Math.atan2(Math.sin(heading - u.angle), Math.cos(heading - u.angle));
          u.angle += clamp(turn, -12 * dt, 12 * dt);
        } else u.angle = heading;
        u.state = 'move';
        if (!mountedImpact && this.time >= u._chargeCheckedUntil) u._charge = Math.min(200, u._charge + movedDistance);
        return true;
      }
      return false;
    }

    _collideMovement(u, mx, my, amount) {
      // Radius-separated broad phase: ordinary ranks search nearby small
      // bodies, while rare large bodies occupy their own coarser grid. Cache
      // relative geometry once for both constraint passes and the final sweep.
      const ux=u.x,uy=u.y,radius=u._radius,unitId=u.id,unitTeam=u.team,units=this.units,next=this._solidNext,siege=this.structures.length>0;
      const xs=this._bodyX,ys=this._bodyY,radii=this._bodyRadius,live=this._bodyLive;
      const candidates=this._collisionCandidates,dxs=this._collisionDX,dys=this._collisionDY;
      const gaps=this._collisionGap,distances=this._collisionDistance2;
      let count=0,blocked=false,ally=false,enemy=false;
      for(let size=0;size<2;size++){
        const maxRadius=size?this._largeRadius:this._smallRadius;
        if(!maxRadius)continue;
        const cellSize=size?128:BODY_CELL,cols=size?this._largeBodyCols:this._bodyCols,rows=size?this._largeBodyRows:this._bodyRows;
        const padding=radius+maxRadius+Math.max(13,amount*2,(this._bodyHashDrift||0)+amount*1.15);
        const minX=Math.max(0,Math.floor((ux-padding)/cellSize)),maxX=Math.min(cols-1,Math.floor((ux+padding)/cellSize));
        const minY=Math.max(0,Math.floor((uy-padding)/cellSize)),maxY=Math.min(rows-1,Math.floor((uy+padding)/cellSize));
        const grid=size?this._largeSolidHeads:this._smallSolidHeads;
        for(let team=0;team<2;team++){
          const heads=grid[team];
          for(let cy=minY;cy<=maxY;cy++)for(let cell=cy*cols+minX,end=cy*cols+maxX;cell<=end;cell++){
            for(let id=heads[cell];id!==-1;id=next[id]){
              if(id===unitId)continue;
              if(!live[id])continue;
              // Legacy assault ramps are one continuous crowded walking
              // surface. Only a real rampart/stair body opens a separate layer.
              if(siege&&(u._rampartZ>0||units[id]._rampartZ>0)&&Math.abs(this.unitAltitude(u)-this._bodyZ[id])>18)continue;
              const ox=ux-xs[id],oy=uy-ys[id],gap=(radius+radii[id])*.96,reach=gap+amount*2;
              // A departing tower closes its ground footprint to newcomers
              // while walkers already beneath the raised deck finish exiting.
              const other=siege?units[id]:null;
              if(siege&&other._siegeDeployed){
                if(!other._siegeUndocking)continue;
                if(other._siegeOccupants&&other._siegeOccupants.has(unitId)){
                  if(ox*ox+oy*oy<(gap+.6)*(gap+.6))continue;
                  other._siegeOccupants.delete(unitId);
                }
              }
              if(Math.abs(ox)>reach||Math.abs(oy)>reach)continue;
              const distance2=ox*ox+oy*oy;if(distance2>reach*reach)continue;
              candidates[count]=id;dxs[count]=ox;dys[count]=oy;gaps[count]=gap;distances[count]=distance2;count++;
            }
          }
        }
      }
      if (!count) return { x: mx, y: my, blocked: false, ally: false, enemy: false };
      for (let pass = 0; pass < 2; pass++) {
        for (let i = 0; i < count; i++) {
          const other = units[candidates[i]];
          const ox=dxs[i],oy=dys[i],gap=gaps[i],dist2=distances[i];
          const motion2 = mx * mx + my * my;
          const along = motion2 > .00001 ? clamp(-(ox * mx + oy * my) / motion2, 0, 1) : 0;
          const nearX = ox + mx * along, nearY = oy + my * along;
          if (nearX * nearX + nearY * nearY >= gap * gap && dist2 >= gap * gap) continue;
          blocked = true;
          if (other.team === unitTeam) ally = true; else enemy = true;
          let distance = Math.sqrt(dist2), nx, ny;
          if (distance < .001) { const angle = ((unitId * 17 + other.id * 31) % 628) * .01; nx = Math.cos(angle); ny = Math.sin(angle); distance = .001; }
          else { nx = ox / distance; ny = oy / distance; }
          const normal = mx * nx + my * ny;
          if (normal < 0) { mx -= nx * normal; my -= ny * normal; }
          if (pass === 0 && mx * mx + my * my < amount * amount * .22) {
            const cross = nx * my - ny * mx;
            const side = Math.abs(cross) > .01 ? Math.sign(cross) : (u._jitter > .5 ? 1 : -1);
            mx += -ny * amount * .70 * side;
            my += nx * amount * .70 * side;
          }
          if (distance < gap) {
            const correction = Math.min(amount * .8 + .1, gap - distance + .02);
            mx += nx * correction; my += ny * correction;
          }
          const length = Math.sqrt(mx*mx+my*my);
          if (length > amount * 1.15) { mx *= amount * 1.15 / length; my *= amount * 1.15 / length; }
        }
        if (!blocked) break;
      }
      if (blocked) for (let i = 0; i < count; i++) {
        const gap=gaps[i],ox=dxs[i],oy=dys[i],old2=distances[i];
        const nx = ox + mx, ny = oy + my, new2 = nx * nx + ny * ny;
        if (new2 < gap * gap - .01 && new2 < old2 - .001) return { x: 0, y: 0, blocked: true, ally, enemy };
        const motion2 = mx * mx + my * my;
        const along = motion2 > .00001 ? clamp(-(ox * mx + oy * my) / motion2, 0, 1) : 0;
        const nearX = ox + mx * along, nearY = oy + my * along;
        if (old2 >= gap * gap && nearX * nearX + nearY * nearY < gap * gap - .01) return { x: 0, y: 0, blocked: true, ally, enemy };
      }
      return { x: mx, y: my, blocked, ally, enemy };
    }

    _prepareMountedStreams() {
      // Only free approaches take part. Enemy contact, turns, withdrawal, siege
      // work, narrow routes and assigned passing maneuvers keep their normal order.
      const streams = this._marchStreams;
      streams.length = 0;
      for (const r of this.regiments) {
        if (!r._marchActive || !this.impacts || !this.impacts.isHorse(r) ||
            r.alive <= 0 || r._contactDistance <= TYPES[r.type].range + 220 ||
            r._terrainPressure >= .2 || r._tacticalHold || r._siegeJob ||
            r._marchTurnUntil > this.time || r._formationPressureUntil > this.time ||
            (r.order !== 'move' && r.order !== 'charge' && r.order !== 'auto' && !this._advancingAttack(r))) continue;
        let s = r._mountedStream;
        if (!s || s.owner !== r.id) r._mountedStream = s = { owner: r.id, units: [] };
        s.parent = null; s.frame = this._tick; s.stepped = false; s.team = r.team;
        s.active = r.order === 'charge' || s.passageEpoch === r._commandEpoch;
        s.nx = r.order === 'charge' ? r._chargeNX : r._marchNX;
        s.ny = r.order === 'charge' ? r._chargeNY : r._marchNY;
        // During a gradual wheel these ranks still follow their present
        // formation axis. The waypoint's future heading can be far ahead
        // of that axis; disabling file release there bunches the rear ranks.
        s.minX = s.minY = Infinity; s.maxX = s.maxY = -Infinity;
        const members = s.units; members.length = 0;
        for (const id of r.unitIds) {
          const u = this.units[id];
          if (!u.alive || u.routed) continue;
          members.push(u);
          // A normal starting formation first opens a real passing lane.
          // After a visible brush, keep its following files flowing for this
          // order rather than reopening the same gap on alternating ticks.
          if (!s.active && u._friendlyOffsetX * u._friendlyOffsetX + u._friendlyOffsetY * u._friendlyOffsetY > u._radius * u._radius / 16) {
            s.active = true; s.passageEpoch = r._commandEpoch;
          }
          s.minX = Math.min(s.minX, u.x); s.maxX = Math.max(s.maxX, u.x);
          s.minY = Math.min(s.minY, u.y); s.maxY = Math.max(s.maxY, u.y);
        }
        if (members.length) streams.push(s);
      }
      // Sweep formation bounds rather than searching every rider for partners.
      // Adjacent, aligned companies share front-to-back release, even when
      // their physical rows are interleaved across regiment IDs.
      streams.sort((a, b) => a.minX - b.minX || a.owner - b.owner);
      const gap = 40;
      for (let i = 0; i < streams.length; i++) {
        const a = streams[i];
        for (let j = i + 1; j < streams.length; j++) {
          const b = streams[j];
          if (b.minX > a.maxX + gap) break;
          if (b.team !== a.team || b.minY > a.maxY + gap || a.minY > b.maxY + gap ||
              a.nx * b.nx + a.ny * b.ny < .98) continue;
          let root = a, other = b;
          while (root.parent) root = root.parent;
          while (other.parent) other = other.parent;
          if (root !== other && root.nx * other.nx + root.ny * other.ny >= .98) {
            other.parent = root; root.active = root.active || other.active;
          }
        }
      }
      for (const s of streams) if (s.parent) {
        let root = s.parent;
        while (root.parent) root = root.parent;
        s.parent = root;
        if (root.active) for (const u of s.units) root.units.push(u);
      }
    }

    _stepUnit(u, dt, orderedMarch) {
      if (u._bulkStepTick === this._tick) return;
      const t = TYPES[u.type], r = this.regiments[u.regiment], impacts = this.impacts;
      const mountedImpact = impacts && (u.type === 'cavalry' || u.type === 'lancer' || u.type === 'horseArcher');
      const stream = mountedImpact && r._mountedStream;
      const group = stream && stream.owner === r.id && stream.frame === this._tick && (stream.parent || stream);
      if (!orderedMarch && !u.routed && this._inUnitStep && group && group.active) {
        if (group.stepped) return;
        group.stepped = true;
        const nx = group.nx, ny = group.ny;
        group.units.sort((a, b) => (b.x - a.x) * nx + (b.y - a.y) * ny || a.id - b.id);
        // Every rider advances through the real body/terrain solver once.
        // A follower uses ground its actual leader has already left this tick.
        for (const member of group.units) if (member.alive && !member.routed) this._stepUnit(member, dt, true);
        return;
      }
      u.anim += dt;
      u.attackFlash = Math.max(0, u.attackFlash - dt * 3.7);
      if (impacts && (mountedImpact || u._impactUntil > this.time) && impacts.beginUnit(u)) return;
      if (u.routed) {
        if (!u.escaped) {
          const direction = u.team === 0 ? -1 : 1;
          this._moveUnit(u, direction === -1 ? 12 : this.width - 12, u.y, dt, 1.1);
          u.state = 'rout';
          u.angle = direction === 1 ? 0 : Math.PI;
          if (u.x < u._radius + 13 || u.x > this.width - u._radius - 13) { u.escaped = true; u.x = direction === -1 ? -100 : this.width + 100; }
        }
        return;
      }
      if (this._marchingBody(r) && u._marchTurnEnd > this.time) {
        const progress = clamp((this.time - u._marchTurnStart) / Math.max(.001, u._marchTurnEnd - u._marchTurnStart), 0, 1);
        u.angle = u._marchTurnFrom + u._marchTurnDelta * progress;
        u._vx = 0; u._vy = 0; u.state = 'turning';
        return;
      }
      u._cooldown -= dt; u._meleeCooldown -= dt; u._sense -= dt; u._terrainTimer -= dt; u._contactSense -= dt;
      if (u._pushUntil > this.time) this._resolveShieldPush(u, dt);
      if (t.brace) u._braceTime = !r._frontTurning && (u._vx * u._vx + u._vy * u._vy < t.speed * t.speed * .0324 || this._steadyMotion(u, r)) ? Math.min(2, u._braceTime + dt) : 0;
      if (u._terrainTimer <= 0) { u._terrain = this.terrainAt(u.x, u.y); u._terrainTimer = .4 + u._jitter * .25; }
      const pressure = r.order !== 'hold' && (u._contact || u._jamTime > .1 || r._terrainPressure > .2);
      u._flex = clamp(u._flex + dt * (pressure ? 3.2 : -.7), 0, 1);
      if (r.fallingBack) {
        this._setTarget(u, -1);
        const goal = this._formationGoal(u, r);
        this._moveUnit(u, goal.x, goal.y, dt, 1.04);
        u.state = 'fallBack';
        return;
      }
      if (r.order === 'face') {
        const goal = this._formationGoal(u, r), turning = clamp((this.time - r._faceStart) / .35, 0, 1);
        if (turning >= 1) this._moveUnit(u, goal.x, goal.y, dt, 1);
        else { u._vx = 0; u._vy = 0; u.state = 'turning'; }
        u.angle = u._faceFromAngle + u._faceHeadDelta * turning;
        return;
      }
      const current = this.units[u.target];
      const tacticalMovement = r._tacticalMovement;
      if (!current || !current.alive || current.routed || length2(current.x - u.x, current.y - u.y) > t.range + 260) this._setTarget(u, -1);
      if ((!t.ranged || u.type === 'musketeer') && (r.order === 'auto' || r.order === 'hold') && u._contactSense <= 0 && r._contactDistance < (u.type === 'musketeer' ? 42 : t.range) + this._maxRadius + 100) {
        const contact = this._contactTarget(u, u.type === 'musketeer' ? 32 : t.range * (u._terrain.elevation ? 1.12 : 1));
        if (contact !== -1) this._setTarget(u, contact);
        u._contactSense = .12 + u._jitter * .08;
      }
      if (u._sense <= 0) {
        if (r._nearestEnemyDistance < t.range + r.depth + 400 || r._contactDistance < t.range + 160 || u.target !== -1) this._setTarget(u, this._chooseTarget(u, r, t));
        u._sense = (u.target === -1 ? .65 : .32) + u._jitter * .3;
      }
      const enemy = this.units[u.target];
      u.state = u.attackFlash > .1 ? 'attack' : 'idle';
      const firmContact = t.rankStiffness > 0 && r.formation !== 'loose' && r._terrainPressure < .2 && r.cohesion > 45 && r._combatActive && r._contactDistance < t.range + 70;
      const canCirculate = !tacticalMovement && !firmContact && !t.ranged && !t.commander && r.order !== 'hold' && r.order !== 'move' && !this._advancingAttack(r) && r._rallyTime <= 0 && !r._traffic && !this._marchingBody(r) && (r._combatActive || r._contactDistance < 350);
      if (canCirculate && this.time - u._lastAttack > Math.max(3.5, t.cooldown * 2)) u._idleContactTime += dt;
      else u._idleContactTime = 0;
      if (canCirculate && (u._jamTime > 1.2 || u._idleContactTime > 3.5)) {
        u._circulateUntil = this.time + 7; u._idleContactTime = 0;
      }
      let goal = this._formationGoal(u, r);
      // A movement order owns the feet even while a soldier can defend itself.
      // Previously an in-range enemy returned before this movement ever ran.
      if (r.order === 'move' || this._advancingAttack(r) || tacticalMovement) {
        if (tacticalMovement && r._tactic.hold && !r._traffic && t.ranged && !t.siege) {
          goal = this._missilePassageGoal(u, r, goal);
          if (u._passageUntil > this.time) u._tacticYielding = true;
          else if (u._tacticYielding) {
            // Keep the gap that was physically opened for allied reserves;
            // do not rush back across their tail to reclaim an obsolete spot.
            u._tacticYielding = false; u._tacticHoldX = u.x; u._tacticHoldY = u.y;
            goal = { x: u.x, y: u.y };
          }
        }
        this._moveUnit(u, goal.x, goal.y, dt, 1);
        if (tacticalMovement && r._tactic.hold && Number.isFinite(r._tactic.faceAngle) && u._vx * u._vx + u._vy * u._vy < 1) {
          const turn = Math.atan2(Math.sin(r._tactic.faceAngle - u.angle), Math.cos(r._tactic.faceAngle - u.angle));
          u.angle += clamp(turn, -dt * 2.4, dt * 2.4);
          if (Math.abs(turn) > .05) u.state = 'turning';
        }
        if (enemy && enemy.alive && !enemy.routed && u._cooldown <= 0) {
          const reach = t.range * (u._terrain.elevation ? 1.12 : 1) + (t.ranged ? 0 : enemy._radius);
          if (length2(enemy.x - u.x, enemy.y - u.y) <= reach) this._attack(u, enemy);
        }
        return;
      }
      if (r.order === 'charge' && enemy && enemy.alive && !enemy.routed && enemy.regiment !== r._chargeTargetId) {
        // Opportunistic shots must not cancel the approach to the committed
        // formation. Target sensing switches to that enemy as soon as a
        // soldier can fire on it; nearby threats can still be shot en route.
        this._moveUnit(u, goal.x, goal.y, dt, 1.15);
        const reach = t.range * (u._terrain.elevation ? 1.12 : 1) + (t.ranged ? 0 : enemy._radius);
        if (u._cooldown <= 0 && length2(enemy.x - u.x, enemy.y - u.y) <= reach) this._attack(u, enemy);
        return;
      }
      if (t.ranged && this._canDeployMissiles(r)) {
        const profile = t.missile || {}, liveEnemy = enemy && enemy.alive && !enemy.routed;
        const dx = liveEnemy ? enemy.x - u.x : 0, dy = liveEnemy ? enemy.y - u.y : 0;
        const distance = liveEnemy ? length2(dx, dy) : Infinity;
        const reach = t.range * (u._terrain.elevation ? 1.12 : 1), inReach = liveEnemy && distance <= reach;
        // Every shooter has a physical place in a firing band. Following a
        // sampled enemy body instead collapsed the rear into the first file
        // that stopped to shoot. Explicit approaches still own movement.
        // A Charge reaches the assigned firing band even after its first
        // rank enters weapon range. Stopping each early shooter at the range
        // edge would trap the remaining files behind those stationary bodies.
        // The shared firing anchor keeps this approach from becoming kiting.
        if (inReach && r.order === 'attack') goal = { x: u.x, y: u.y };
        if (r.order === 'auto' && profile.reloadStep > 0 && this.time - u._lastAttack < t.cooldown * .88) {
          const phase = clamp((this.time - u._lastAttack) / (t.cooldown * .88), 0, 1);
          const excursion = Math.sin(phase * Math.PI) * profile.reloadStep;
          const side = (Math.floor((u._missileSlot >= 0 ? u._missileSlot : u._slot) % r._missileColumns / 5) + r.id) % 2 ? 1 : -1;
          goal = { x: goal.x + r._frontTX * excursion * side * .86 - r._frontNX * excursion * .32,
            y: goal.y + r._frontTY * excursion * side * .86 - r._frontNY * excursion * .32 };
        }
        if (r.order === 'auto' && inReach && profile.breakRange > 0 && distance < profile.breakRange) {
          const retreat = Math.min(70, profile.breakRange - distance + 15);
          const ex = u.x - dx / Math.max(1, distance) * retreat, ey = u.y - dy / Math.max(1, distance) * retreat;
          const ox = ex - goal.x, oy = ey - goal.y, extent = length2(ox, oy), bound = Math.max(70, profile.reloadStep || 0);
          goal = extent > bound ? { x: goal.x + ox / extent * bound, y: goal.y + oy / extent * bound } : { x: ex, y: ey };
        }
        if (r.order === 'auto' && distance > 65 && !t.siege) goal = this._missilePassageGoal(u, r, goal);
        if (length2(goal.x - u.x, goal.y - u.y) > 6) this._moveUnit(u, goal.x, goal.y, dt, r.order === 'charge' ? 1.15 : 1);
        else { u._vx = 0; u._vy = 0; }
        const firingDistance = liveEnemy ? length2(enemy.x - u.x, enemy.y - u.y) : Infinity;
        if (liveEnemy && firingDistance <= reach) {
          u._fireReachTime = this.time; u._fireReachEnemy = enemy.regiment;
          if (u._vx * u._vx + u._vy * u._vy < 1) u.angle = Math.atan2(dy, dx);
          if (u._cooldown <= 0) this._attack(u, enemy);
        }
        return;
      }
      // A running horse carries through weapon contact while its momentum
      // lasts. Body impacts open only collision-checked space and slow it;
      // once that run is spent the ordinary close-combat behavior resumes.
      const plunge = mountedImpact && u.type !== 'horseArcher' && impacts.plungeGoal(u, enemy);
      if (plunge) {
        if (enemy && u._cooldown <= 0 && length2(enemy.x - u.x, enemy.y - u.y) <= t.range + enemy._radius) this._attack(u, enemy);
        if (u.alive) this._moveUnit(u, plunge.x, plunge.y, dt, r.order === 'charge' ? 1.15 : 1);
        return;
      }
      if (enemy && enemy.alive && !enemy.routed) {
        const dx = enemy.x - u.x, dy = enemy.y - u.y, distance = length2(dx, dy);
        const reach = t.range * (u._terrain.elevation ? 1.12 : 1) + (t.ranged ? 0 : TYPES[enemy.type].radius);
        const heldTooFar = false; // Hold limits pursuit, never nearby self-defense.
        const movingAway = r.order === 'move' && distance > Math.max(55, t.range);
        const chargePast = r.order === 'charge' && enemy.regiment !== r._chargeTargetId && dx * r._chargeNX + dy * r._chargeNY < -u._radius;
        if (distance <= reach && !heldTooFar && !chargePast) {
          u.angle = Math.atan2(dy, dx);
          u._vx = u._vy = 0;
          if (u._cooldown <= 0) this._attack(u, enemy);
          if (!t.ranged && (r.order === 'auto' || (t.push && r.order !== 'hold')) && r._combatActive) {
            if (t.push && this._canShieldPress(u) && u._combatSlot >= 0 && u._combatSlot < r._combatColumns) {
              // A supported shield front follows the body directly ahead at
              // contact distance. Its lateral file remains assigned, but a
              // receding foe need not wait for the coarse lane refresh before
              // the front takes another small, collision-checked step.
              const along = dx * r._frontNX + dy * r._frontNY;
              const across = Math.abs(dx * r._frontTX + dy * r._frontTY);
              if (along > 0 && across < r._spacing * 1.3) {
                const desired = along - u._radius - enemy._radius - 1.5;
                const planned = (goal.x - u.x) * r._frontNX + (goal.y - u.y) * r._frontNY;
                if (desired > planned) goal = { x: goal.x + r._frontNX * (desired - planned), y: goal.y + r._frontNY * (desired - planned) };
              }
            }
            const forward = (goal.x - u.x) * r._frontNX + (goal.y - u.y) * r._frontNY;
            const contactDistance = length2(enemy.x - goal.x, enemy.y - goal.y);
            // Weapon reach starts a fight; it is not a wall that stops the
            // whole rank. Press into the assigned, body-checked contact space
            // so following ranks can use reachable gaps behind the front.
            if (forward > 1 && contactDistance < distance - 1) {
              this._moveUnit(u, goal.x, goal.y, dt, t.push && this._canShieldPress(u) ? .65 : .35);
              u.angle = Math.atan2(enemy.y - u.y, enemy.x - u.x);
              if (u.attackFlash > .1) u.state = 'attack';
            }
          }
          if (t.push) this._tryShieldPush(u, enemy);
          const skirmish = Number.isFinite(t.skirmishDistance) ? t.skirmishDistance : t.mounted ? 130 : 60;
          if (t.ranged && !t.siege && distance < skirmish && r.order !== 'hold' && r.order !== 'charge') {
            goal = { x: u.x - dx / Math.max(1, distance) * 90, y: u.y - dy / Math.max(1, distance) * 90 };
            this._moveUnit(u, goal.x, goal.y, dt, t.mounted ? 1 : .82);
          }
          return;
        }
        if (!heldTooFar && !movingAway && !chargePast && r.order !== 'hold' && !(t.commander && r.order === 'auto') && r._rallyTime <= 0) {
          if (t.ranged) {
            const stop = reach * .86;
            goal = { x: enemy.x - dx / Math.max(1, distance) * stop, y: enemy.y - dy / Math.max(1, distance) * stop };
          } else {
            const lateral = Math.abs((enemy.x - goal.x) * r._frontTX + (enemy.y - goal.y) * r._frontTY);
            if (distance < reach + 42 && (lateral < r._spacing * 2.3 || !r._combatActive) && this._targetClaims[enemy.id] <= 3) {
              goal = { x: enemy.x - dx / Math.max(1, distance) * reach * .83, y: enemy.y - dy / Math.max(1, distance) * reach * .83 };
            }
          }
        }
      }
      if (canCirculate && u._circulateUntil > this.time) goal = this._circulationGoal(u, r, goal);
      this._moveUnit(u, goal.x, goal.y, dt, r.order === 'charge' ? 1.15 : 1);
    }

    _meleeLineClear(a,b){const az=this.unitAltitude(a),bz=this.unitAltitude(b);return az>8||bz>8?!this._projectileBarrier(a.x,a.y,b.x,b.y,0,0,1,az+14,bz+14):this._lineClear(a.x,a.y,b.x,b.y);}

    _attack(attacker, target) {
      const t = TYPES[attacker.type];
      if(!t.ranged&&Math.abs(this.unitAltitude(attacker)-this.unitAltitude(target))>18)return;
      if (attacker.type === 'musketeer' && this._inUnitStep && !this.regiments[attacker.regiment]._volleyDispatch) return;
      const distance = Math.hypot(target.x - attacker.x, target.y - attacker.y);
      const bodyReach = t.ranged ? 0 : target._radius;
      if (distance > t.range + bodyReach) {
        // Movement terrain is cached, but a hill's extra weapon reach must
        // end when the soldier leaves that ground, even between cache ticks.
        attacker._terrain = this.terrainAt(attacker.x, attacker.y);
        if (distance > t.range * (attacker._terrain.elevation ? 1.12 : 1) + bodyReach) return;
      }
      const attackRegiment = this.regiments[attacker.regiment];
      if (attacker.type === 'trebuchet' && distance < 85) return;
      const ordnance = attacker.type === 'musketeer' || attacker.type === 'cannon' || attacker.type === 'trebuchet';
      attacker._cooldown = ordnance ? t.cooldown : t.cooldown * (.93 + this._random() * .14);
      attacker._lastAttack = this.time;
      attacker.attackFlash = 1;
      attacker.state = 'attack';
      if (t.ranged) {
        if (distance < 48 && !t.siege && attacker.type !== 'musketeer' && Math.abs(this.unitAltitude(attacker)-this.unitAltitude(target))<=18) {
          this._hit(attacker, target, Math.max(4, t.damage * .32), false, false);
          return;
        }
        if (t.siege && !t.equipment && !ordnance && distance < 85 && Math.abs(this.unitAltitude(attacker)-this.unitAltitude(target))<=18) {
          this._hit(attacker, target, 3, false, false);
          return;
        }
        const projectileSpeed = Number.isFinite(t.projectileSpeed) ? t.projectileSpeed : t.projectile === 'stone' ? 370 : t.projectile === 'bolt' ? 760 : 530;
        const duration = Math.max(attacker.type === 'musketeer' ? .025 : .20, distance / projectileSpeed);
        const spread = attacker.type === 'musketeer' ? 4 : attacker.type === 'trebuchet' ? 28 : t.projectile === 'stone' ? 28 : t.mounted ? 16 : 10;
        const tx = clamp(target.x + target._vx * duration * .8 + (this._random() - .5) * spread, 10, this.width - 10);
        const ty = clamp(target.y + target._vy * duration * .8 + (this._random() - .5) * spread, 10, this.height - 10);
        const projectile = {
          x: attacker.x, y: attacker.y, z: 0, tx, ty,
          sx: attacker.x, sy: attacker.y, team: attacker.team,
          kind: t.projectile || 'arrow', target: target.id,
          attacker: attacker.id, type: attacker.type, regiment: attacker.regiment,
          age: 0, duration, damage: t.damage * (attacker._terrain.elevation ? 1.1 : 1),
          arc: Number.isFinite(t.projectileArc) ? t.projectileArc : t.projectile === 'stone' ? Math.min(180, distance * .23) : Math.min(75, distance * .11)
        };
        this._launchProjectile(projectile);
      } else {
        const defense = TYPES[target.type], momentum = this.impacts && (attacker.type === 'cavalry' || attacker.type === 'lancer') ? this.impacts.weaponPower(attacker) : null;
        const chargePower = momentum === null ? (attacker._charge >= 90 ? 1 : 0) : momentum;
        const charging = t.charge > 0 && chargePower > .078;
        const setPikes = defense.mounted ? this._braceStrength(attacker, target) : 0;
        let damage = t.damage + (defense.mounted ? t.antiCavalry * (1 + setPikes * (t.brace ? t.brace.mountedDamage : 0)) : 0);
        if (setPikes > 0 && defense.charge > 0 && target._charge >= 90) {
          damage += setPikes * (t.antiCavalry * .5 + defense.charge * t.brace.chargeReturn);
          target._charge = 0; target._chargeCheckedUntil = this.time + 1.2;
          if (this.impacts) this.impacts.intercept(target, attacker, setPikes);
        }
        if (charging) {
          const braced = this._braceStrength(target, attacker);
          damage += t.charge * chargePower * this._formationFactor(attackRegiment, 'charge') * (1 - braced * (defense.brace ? defense.brace.chargeReduction : 0));
          if (braced > 0) this._hit(target, attacker, braced * (defense.antiCavalry * .5 + t.charge * defense.brace.chargeReturn), false, false);
          this._effect(attacker.x, attacker.y, 'charge', 24 + t.radius, attacker.team, .5);
        }
        if (t.sweep) {
          this._clubSweep(attacker, target, damage, t.sweep);
          return;
        }
        this._hit(attacker, target, damage, false, charging);
        if (t.splash > 0) this._splash(target.x, target.y, t.splash, attacker, damage * .55, target.id, false);
        attacker._charge = 0;
      }
    }

    _clubSweep(attacker, primary, damage, sweep) {
      const angle = Math.atan2(primary.y - attacker.y, primary.x - attacker.x);
      const nx = Math.cos(angle), ny = Math.sin(angle), edge = Math.cos(sweep.halfAngle);
      const radius = sweep.radius;
      attacker.angle = angle;
      const effect = this._effect(attacker.x, attacker.y, 'clubSweep', radius, attacker.team, .34);
      if (effect) Object.assign(effect, { angle, nx, ny, radius, halfAngle: sweep.halfAngle });
      const strike = (target, raw) => {
        if (!this._lineClear(attacker.x, attacker.y, target.x, target.y)) return;
        this._hit(attacker, target, raw, false, false, true);
        if (this.impacts) this.impacts.weaponImpulse(attacker, target, target.x - attacker.x, target.y - attacker.y, sweep.shove, .30, 0);
      };
      strike(primary, damage);
      const minX = Math.max(0, Math.floor((attacker.x - radius) / CELL)), maxX = Math.min(this._cols - 1, Math.floor((attacker.x + radius) / CELL));
      const minY = Math.max(0, Math.floor((attacker.y - radius) / CELL)), maxY = Math.min(this._rows - 1, Math.floor((attacker.y + radius) / CELL));
      for (let y = minY; y <= maxY; y++) for (let x = minX; x <= maxX; x++) {
        for (let id = this._heads[1 - attacker.team][y * this._cols + x]; id !== -1; id = this._next[id]) {
          const other = this.units[id];
          if (other === primary || !other.alive || other.routed) continue;
          const dx = other.x - attacker.x, dy = other.y - attacker.y, d = Math.hypot(dx, dy);
          if (d <= radius && dx * nx + dy * ny >= edge * d) strike(other, damage * sweep.secondaryDamage);
        }
      }
    }

    _hit(attacker, target, raw, ranged, charged, splash) {
      if (!target.alive || target.routed) return;
      const attackType = TYPES[attacker.type], defense = TYPES[target.type];
      const terrain = target._terrain || EMPTY_TERRAIN;
      const attackRegiment = this.regiments[attacker.regiment];
      const regiment = this.regiments[target.regiment];
      // Physical ranks restrict how many soldiers can engage at once. The
      // gentler damage curve leaves time to order a fallback and bring reserves.
      let damage = raw * .54 * (.90 + this._random() * .20) * this._formationFactor(attackRegiment, 'damage');
      damage *= .86 + .14 * clamp(attackRegiment.cohesion / 100, 0, 1);
      if (ranged) damage *= 1 - terrain.cover * .65;
      const angle = Math.atan2(attacker.y - target.y, attacker.x - target.x);
      // Formed polearms keep their rank's directional protection. Turning
      // to strike a flanker cannot rotate the whole line's defensive front.
      const formedBrace = defense.brace && (regiment.formation === 'line' || regiment.formation === 'shieldwall');
      const front = Math.cos(angle - (defense.frontalShield || formedBrace ? regiment.facing : target.angle));
      damage *= this._formationFactor(regiment, ranged ? 'incomingRanged' : 'incomingMelee');
      if (front > .25) damage *= this._formationFactor(regiment, ranged ? 'frontalRanged' : 'frontalMelee');
      if (splash) damage *= this._formationFactor(regiment, 'incomingSplash');
      if ((target.type === 'guardian' || target.type === 'swordsman') && front > .25) damage *= target.type === 'guardian' ? .72 : .90;
      else if (!ranged && front < -.4) { damage *= 1.16; regiment._flankHits++; }
      if (defense.frontalShield && front > .45 && !regiment._frontTurning && !regiment.fallingBack && regiment.order !== 'face') {
        const shield = defense.frontalShield[ranged ? 'ranged' : 'melee'];
        const strength = clamp(regiment.cohesion / 100, 0, 1) * (regiment.formation === 'loose' ? .25 : 1);
        damage *= 1 + (shield - 1) * strength;
      }
      if (!ranged && !defense.brace) damage *= 1 - this._steadyGuardStrength(target, attacker) * .3;
      if (!ranged && attackType.mounted && defense.brace) damage *= 1 - this._braceStrength(target, attacker) * (defense.brace.mountedDefense || 0);
      damage = Math.max(1.5, damage - defense.armor * (1 - (attackType.armorPierce || 0)));
      damage *= DATA.armorMultiplier(attacker.type, defense.armorClass);
      // More missile soldiers now find reachable targets. Apply their pacing
      // adjustment to finished damage so it cannot push distinct formation
      // defenses into the same pre-armor damage floor.
      if (ranged) damage *= .90;
      target.hp -= damage;
      regiment._recentHits += damage;
      if (charged) {
        regiment.morale = Math.max(0, regiment.morale - Math.min(.8, damage / Math.max(1, regiment.initial) * .06));
        regiment.cohesion = Math.max(0, regiment.cohesion - Math.min(1.6, damage / Math.max(1, regiment.initial) * .11));
      }
      if ((target.id + this._tick) % 7 === 0) this._effect(target.x, target.y, 'hit', 7, attacker.team, .25, target.id, front > .25 && (defense.frontalShield || defense.armor >= 5));
      if (target.hp <= 0) this._kill(target, attacker);
    }

    _splash(x, y, radius, attacker, damage, skipId, ranged) {
      const minX = clamp(Math.floor((x - radius) / CELL), 0, this._cols - 1);
      const maxX = clamp(Math.floor((x + radius) / CELL), 0, this._cols - 1);
      const minY = clamp(Math.floor((y - radius) / CELL), 0, this._rows - 1);
      const maxY = clamp(Math.floor((y + radius) / CELL), 0, this._rows - 1);
      let hits = 0;
      for (let yy = minY; yy <= maxY; yy++) for (let xx = minX; xx <= maxX; xx++) {
        for (let id = this._heads[1 - attacker.team][yy * this._cols + xx]; id !== -1; id = this._next[id]) {
          const target = this.units[id];
          if (id === skipId || !target.alive || target.routed) continue;
          const distance = Math.hypot(target.x - x, target.y - y);
          if (distance <= radius + TYPES[target.type].radius) {
            const strength = 1 - .5 * Math.min(1, distance / radius);
            this._hit(attacker, target, damage * strength, ranged, false, true);
            if (attacker.type === 'catapult' && this.impacts) this.impacts.weaponImpulse(attacker, target,
              distance > .1 ? (target.x - x) / distance : Math.cos(attacker.angle), distance > .1 ? (target.y - y) / distance : Math.sin(attacker.angle),
              245 * strength, 1.05 * (.6 + strength * .4), 85 * strength);
            if (++hits >= (attacker.type === 'catapult' ? 28 : 7)) return;
          }
        }
      }
    }

    _launchProjectile(p) {
      const source=this.units[p.attacker],aim=this.units[p.target],altitude=u=>u?(u._rampartZ||this.siege&&this.siege.heightAt(u.x,u.y)||0):0;
      if(p.launchZ===undefined)p.launchZ=altitude(source)+(p.type==='trebuchet'?87:p.type==='siegeTower'?116:p.type==='catapult'?40:p.type==='ballista'?25:20);
      if(p.targetZ===undefined)p.targetZ=altitude(aim)+14;
      p.rampartTrajectory=!!((source&&source._rampartZ>0)||(aim&&aim._rampartZ>0));
      const support=aim&&aim._onRampart&&this.siege&&this.siege.byId.get(aim._rampartId);
      let exterior=!!(source&&support&&(support.rampartSide==='west'?source.x<support.x+support.w/2:
        support.rampartSide==='east'?source.x>support.x+support.w/2:
        support.rampartSide==='north'?source.y<support.y+support.h/2:
        support.rampartSide==='south'?source.y>support.y+support.h/2:false));
      if(source&&support&&support.castleTower){const cx=support.x+support.w/2,cy=support.y+support.h/2;
        exterior=exterior||(Number.isFinite(support.courtyardX)&&(cx<support.courtyardX-8?source.x<cx:cx>support.courtyardX+8&&source.x>cx))||
          (Number.isFinite(support.courtyardY)&&(cy<support.courtyardY-8?source.y<cy:cy>support.courtyardY+8&&source.y>cy));}
      p.rampartMiss=!!(exterior&&this.unitAltitude(source)<this.unitAltitude(aim)-8&&this._random()>.52);
      const gun = p.type === 'musketeer' || p.type === 'cannon';
      p.ordnance = gun || p.type === 'trebuchet';
      if (p.ordnance) {
        const u = this.units[p.attacker];
        u.angle = Math.atan2(p.ty - p.sy, p.tx - p.sx);
        if (gun) {
          const flash = this._effect(u.x, u.y, p.type === 'cannon' ? 'cannonMuzzle' : 'musketMuzzle', p.type === 'cannon' ? 30 : 12, p.team, .25, u.id);
          if (flash) flash.angle = u.angle;
          if(flash)flash.z=u._rampartZ||0;
        }
      }
      if (this.projectiles.length < MAX_PROJECTILES) this.projectiles.push(p);
      else {
        if (p.ordnance) this._traceOrdnance(p, 0, 1);
        this._impact(p);
      }
    }

    _projectileBarrier(x, y, dx, dy, arc, from, to, launchZ=0, targetZ=0) {
      let first = null;
      const x0 = x + dx * from, x1 = x + dx * to, y0 = y + dy * from, y1 = y + dy * to;
      const left = Math.min(x0, x1), right = Math.max(x0, x1), top = Math.min(y0, y1), bottom = Math.max(y0, y1);
      for (const block of this._blocks) {
        if (block.breached || block.open || block.type === 'water' || block.x > right || block.x + block.w < left || block.y > bottom || block.y + block.h < top) continue;
        const span = segmentBox(x, y, dx, dy, block, 0);
        if (!span) continue;
        let entry = Math.max(span[0], from);
        const exit = Math.min(span[1], to);
        if (entry > exit) continue;
        // The stone clears the parapet only between its rising and falling crossings.
        const height = block.type === 'rock' ? 32 : block.structureHeight || 36;
        if(launchZ||targetZ){
          let hit=null;const steps=24;
          for(let i=0;i<=steps;i++){const at=entry+(exit-entry)*i/steps,z=launchZ+(targetZ-launchZ)*at+Math.sin(at*Math.PI)*arc;if(z<=height+.01){hit=at;break;}}
          if(hit===null)continue;entry=hit;
        } else if (arc > height) {
          const rise = Math.asin(height / arc) / Math.PI;
          if (entry > rise && entry < 1 - rise) entry = 1 - rise;
          if (entry > exit) continue;
        }
        if (!first || entry < first.at) first = { at: entry, block };
      }
      return first;
    }

    _ordnanceNeighbors(team, x0, y0, x1, y1, margin, visit) {
      // The spatial hash is rebuilt before movement each tick.
      const pad = margin + this._maxRadius + 24;
      const left = clamp(Math.floor((Math.min(x0, x1) - pad) / CELL), 0, this._cols - 1);
      const right = clamp(Math.floor((Math.max(x0, x1) + pad) / CELL), 0, this._cols - 1);
      const top = clamp(Math.floor((Math.min(y0, y1) - pad) / CELL), 0, this._rows - 1);
      const bottom = clamp(Math.floor((Math.max(y0, y1) + pad) / CELL), 0, this._rows - 1);
      for (let y = top; y <= bottom; y++) for (let x = left; x <= right; x++) {
        for (let id = this._heads[team][y * this._cols + x]; id !== -1; id = this._next[id]) {
          const u = this.units[id];
          if (u.alive && !u.routed) visit(u);
        }
      }
    }

    _traceOrdnance(p, from, to) {
      const dx = p.tx - p.sx, dy = p.ty - p.sy, length = Math.hypot(dx, dy);
      const barrier = this._projectileBarrier(p.sx, p.sy, dx, dy, p.arc, from, to,p.launchZ||0,p.targetZ||0);
      let stop = barrier ? barrier.at : Infinity, hit = null;
      if (p.ordnance && !p.rampartMiss && p.type !== 'trebuchet' && length > .001) {
        const nx = dx / length, ny = dy / length, shotRadius = p.type === 'cannon' ? 4 : 1;
        this._ordnanceNeighbors(1 - p.team, p.sx + dx * from, p.sy + dy * from, p.sx + dx * to, p.sy + dy * to, shotRadius, u => {
          const ox = u.x - p.sx, oy = u.y - p.sy, lateral = ox * ny - oy * nx;
          const radius = u._radius + shotRadius;
          if (Math.abs(lateral) > radius) return;
          const along = ox * nx + oy * ny, half = Math.sqrt(radius * radius - lateral * lateral);
          if ((along + half) / length < from) return;
          const entry = Math.max(from, (along - half) / length);
          const z=(p.launchZ||0)+((p.targetZ||0)-(p.launchZ||0))*entry+Math.sin(entry*Math.PI)*p.arc;
          if(z<this.unitAltitude(u)-3||z>this.unitAltitude(u)+28)return;
          if (entry <= to && entry < stop) { stop = entry; hit = u; }
        });
      }
      if (!Number.isFinite(stop)) return false;
      p.impactZ=(p.launchZ||0)+((p.targetZ||0)-(p.launchZ||0))*stop+Math.sin(stop*Math.PI)*p.arc;
      p.tx = p.sx + dx * stop; p.ty = p.sy + dy * stop;
      p.contact = hit ? hit.id : -1;
      // A projectile that met an intervening body or rock never damages the original wall.
      p.structureTarget = hit ? undefined : barrier.block.id;
      return true;
    }

    _ordnanceImpact(p, attacker) {
      const dx = p.tx - p.sx, dy = p.ty - p.sy, distance = Math.hypot(dx, dy) || 1;
      const nx = dx / distance, ny = dy / distance;
      if (p.type === 'musketeer') {
        const target = this.units[p.contact];
        if (target) {
          this._hit(attacker, target, p.damage, true, false);
          if (this.impacts) this.impacts.weaponImpulse(attacker, target, nx, ny, 74, .31, 0);
        }
        const effect=this._effect(p.tx, p.ty, 'musketHit', 10, p.team, .23);if(effect)effect.z=Math.max(0,(p.impactZ===undefined?p.targetZ:p.impactZ)-14);
        return;
      }
      const stone = p.type === 'trebuchet', radius = TYPES[p.type].splash;
      const wall = this.siege && this.siege.byId.get(p.structureTarget);
      // Start blast rays on the exposed face. Resolve cover before applying breach damage.
      const x = p.tx - nx * .05, y = p.ty - ny * .05;
      this._ordnanceNeighbors(1 - p.team, x, y, x, y, radius, target => {
        const ox = target.x - x, oy = target.y - y, d = Math.hypot(ox, oy);
        if (d > radius + target._radius || this._projectileBarrier(x, y, ox, oy, 0, 0, 1,p.targetZ||0,(target._rampartZ||0)+14)) return;
        const strength = 1 - .65 * Math.min(1, d / radius);
        this._hit(attacker, target, p.damage * strength, true, false, true);
        if (this.impacts) this.impacts.weaponImpulse(attacker, target, d > .1 ? ox / d : nx, d > .1 ? oy / d : ny,
          (stone ? 520 : 440) * strength, (stone ? 1.8 : 1.5) * (.55 + strength * .45), (stone ? 180 : 145) * strength);
      });
      if (wall) this.siege.damage(wall, p.damage * (stone ? 3.4 : 2.2), attacker);
      const effect=this._effect(p.tx, p.ty, stone ? 'trebuchetImpact' : 'cannonImpact', radius, p.team, stone ? 1.1 : .72);if(effect)effect.z=Math.max(0,(p.impactZ===undefined?p.targetZ:p.impactZ)-14);
    }

    _impact(p) {
      if (p.ordnance) { this._ordnanceImpact(p, this.units[p.attacker]); return; }
      if (p.structureTarget !== undefined && this.siege) { this.siege.impact(p); return; }
      const attacker = this.units[p.attacker];
      if (!attacker) return;
      const target = this.units[p.target];
      if(p.rampartMiss)return;
      if (TYPES[p.type].splash > 0) {
        this._splash(p.tx, p.ty, TYPES[p.type].splash, attacker, p.damage, -1, true);
        this._effect(p.tx, p.ty, 'explosion', TYPES[p.type].splash, p.team, .8);
      } else if (target && target.alive && !target.routed && Math.hypot(target.x - p.tx, target.y - p.ty) < TYPES[target.type].radius + 25) {
        this._hit(attacker, target, p.damage, true, false);
      }
    }

    _kill(u, attacker) {
      if (!u.alive || u.routed) return;
      u.alive = false; u.hp = 0; u.state = 'dead'; u.target = -1;
      this._bodyLive[u.id] = 0;
      u._pushUntil = 0;
      u._deathTime = this.time;
      this.deadUnitIds.push(u.id);
      const r = this.regiments[u.regiment];
      r.alive--;
      r.morale = Math.max(0, r.morale - 78 / Math.max(1, r.initial));
      const stats = this._stats[u.team];
      stats.alive--; stats.dead++;
      stats.power -= TYPES[u.type].cost;
      if (r.fallingBack) stats.fallingBack--;
      if (attacker && attacker.team !== u.team) {
        this._stats[attacker.team].kills++;
        this.regiments[attacker.regiment].kills++;
      }
      this._lastCasualty = this.time;
      if (r.alive === 0) {
        this._event(r.name + ' has been destroyed.', r.team, 'destroyed');
        if (this.regiments.some(other => other.order === 'charge' && other._chargeTargetId === r.id && other.alive > 0)) this._strategyTimer = 0;
      }
    }

    _findSupport(r) {
      let best = null, score = Infinity;
      for (const other of this.regiments) {
        if (other.id === r.id || other.team !== r.team || other.alive <= 0 || other._routed) continue;
        if (r._manualFallback && r.fallingBack && r._nearestEnemyDistance < 500) {
          const away = (other._centroidX - r._centroidX) * r._retreatNX + (other._centroidY - r._centroidY) * r._retreatNY;
          if (other._nearestEnemyDistance < 160 || (away < -65 && other._nearestEnemyDistance < r._nearestEnemyDistance + 100)) continue;
        }
        const distance = Math.hypot(other.x - r.x, other.y - r.y);
        const preference = (TYPES[other.type].commander ? -220 : 0) + (other.fallingBack ? 1400 : 0) + (other._nearestEnemyDistance < 160 ? 160 : 0);
        if (distance + preference < score) { score = distance + preference; best = other; }
      }
      return best;
    }

    _startFallback(r, manual) {
      if (r.alive <= 0 || r._routed) return false;
      if (r.fallingBack && (!manual || r._manualFallback)) return !!manual;
      const alreadyWithdrawing = r.fallingBack;
      if (r._traffic) this._clearTraffic(r._traffic.plan);
      if (manual) { this._resetCommandIntent(r); r._commandTime = this.time; }
      else if (this.impacts) { this.impacts.cancelPressure(r); this.impacts.cancelFriendly(r); }
      r._missileActive = false; r._fireLocked = false; r._fireLastInRange = -1000;
      r._approachBlendUntil = 0;
      r.fallingBack = true; r.status = 'fallingBack'; r.order = 'retreat';
      if (!alreadyWithdrawing) { r._fallbackTime = 0; r._unsupportedTime = 0; r._fallbackCount++; }
      r._manualFallback = !!manual; r._pathTimer = 0; r._path = [];
      r._rallyTime = 0;
      r.commandStatus = 'withdrawing'; r.commandTargetId = -1; r.commandTargetName = '';
      if (manual) {
        const center = this._currentCenter(r);
        r.x = center.x; r.y = center.y; r._centroidX = center.x; r._centroidY = center.y;
        let nx = 0, ny = 0, samples = 0;
        const stride = Math.max(1, Math.floor(r.unitIds.length / 24));
        for (let index = 0; index < r.unitIds.length; index += stride) {
          const u = this.units[r.unitIds[index]];
          if (!u.alive || u.routed) continue;
          const id = this._nearestEnemy(u, 360), enemy = this.units[id];
          if (!enemy) continue;
          const dx = u.x - enemy.x, dy = u.y - enemy.y, distance = Math.max(1, Math.hypot(dx, dy));
          const weight = 1 - Math.min(.95, distance / 400);
          nx += dx / distance * weight; ny += dy / distance * weight; samples++;
        }
        if (!samples || Math.hypot(nx, ny) < .1) {
          let threat = null, nearest = Infinity;
          for (const other of this.regiments) if (other.team !== r.team && other.alive > 0 && !other._routed) {
            const d = Math.hypot(other._centroidX - center.x, other._centroidY - center.y);
            if (d < nearest) { nearest = d; threat = other; }
          }
          nx = threat ? center.x - threat._centroidX : r.team === 0 ? -1 : 1;
          ny = threat ? center.y - threat._centroidY : 0;
        }
        const length = Math.hypot(nx, ny);
        r._retreatNX = length > .01 ? nx / length : r.team === 0 ? -1 : 1;
        r._retreatNY = length > .01 ? ny / length : 0;
        const goal = this._nearestPassable(center.x + r._retreatNX * 280, center.y + r._retreatNY * 280, TYPES[r.type].radius + 3);
        r._withdrawStartX = center.x; r._withdrawStartY = center.y;
        r._withdrawGoalX = goal.x; r._withdrawGoalY = goal.y; r._withdrawLeg = true;
        r.targetX = goal.x; r.targetY = goal.y;
        for (const id of r.unitIds) {
          const u = this.units[id];
          if (!u.alive || u.routed) continue;
          u._withdrawOffsetX = u.x - center.x; u._withdrawOffsetY = u.y - center.y;
          u._mountSpeed = u._mountDriveSpeed = u._charge = 0;
          u.attackFlash = 0; u.state = 'fallBack';
        }
      }
      const support = this._findSupport(r);
      r.supportRegimentId = support ? support.id : -1;
      r.regroupWith = support ? support.name : 'surviving allies';
      for (const id of r.unitIds) {
        const u = this.units[id];
        if (u.alive && !u.routed) { this._setTarget(u, -1); if (!manual) u.state = 'fallBack'; u._detour = null; u._holdPosition = null; }
      }
      if(this.ramparts)this.ramparts.onCommand(r,'retreat');
      this._event(r.name + ' falls back to regroup' + (support ? ' with ' + support.name : '') + '.', r.team, 'fallback');
      return true;
    }

    _recover(r, activeRally) {
      if (!r.fallingBack || r.alive <= 0) return false;
      // Passive regrouping and automatic commander pulses cannot overrule
      // a player's unfinished breakoff, including a fresh manual order on
      // an older automatic fallback. An explicit player rally can recover it.
      if (r._manualFallback && !activeRally && (r._withdrawLeg || r._nearestEnemyDistance <= 180)) return false;
      if (this.impacts) { this.impacts.cancelPressure(r); this.impacts.cancelFriendly(r); }
      if (r._traffic) this._clearTraffic(r._traffic.plan);
      r.fallingBack = false; r.status = 'rallying'; r._rallyTime = 8;
      r._retreatTurnUntil = 0; r.commandTurnRemaining = 0; r._withdrawLeg = false;
      r.commandStatus = 'regrouping';
      r.morale = Math.max(46, r.morale); r.cohesion = Math.max(48, r.cohesion);
      r._holdX = r.x; r._holdY = r.y;
      r.order = r._manualFallback && !activeRally ? 'hold' : 'auto';
      r._pathTimer = 0; r._path = []; r._unsupportedTime = 0;
      this._stats[r.team].rallied += r.alive;
      for (const id of r.unitIds) {
        const u = this.units[id];
        if (u.alive && !u.routed) { u.state = 'move'; u._sense = .1; u.target = -1; }
      }
      this._event(r.name + ' reforms with ' + r.alive + ' soldiers and returns to service.', r.team, 'rally');
      this._effect(r.x, r.y, 'rally', Math.max(60, r.width * .5), r.team, 1.5);
      return true;
    }

    _rally(commander, explicit) {
      if (!TYPES[commander.type].commander || commander.rallyCooldown > 0 || commander.alive <= 0) return 0;
      commander.rallyCooldown = TYPES[commander.type].rallyCooldown || 45;
      commander._rallyTime = Math.max(commander._rallyTime, 2);
      commander._holdX = commander.x; commander._holdY = commander.y;
      let recovered = 0;
      const radius = commander.rallyRadius || 620;
      for (const other of this.regiments) {
        if (other.team !== commander.team || other.alive <= 0 || Math.hypot(other.x - commander.x, other.y - commander.y) > radius) continue;
        other.morale = Math.min(TYPES[other.type].morale, other.morale + 28);
        other.cohesion = Math.min(100, other.cohesion + 35);
        if (other.fallingBack && other._fallbackTime >= 3 && this._recover(other, !!explicit || !other._manualFallback)) recovered += other.alive;
      }
      this._effect(commander.x, commander.y, 'rally', radius, commander.team, 1.8);
      this._event(commander.name + ' sounds the rally' + (recovered ? ': ' + recovered + ' soldiers return to their formations.' : ' and steadies nearby formations.'), commander.team, 'rally');
      return recovered;
    }

    _route(r, reason) {
      if (r._routed || r.alive <= 0) return;
      if (this.impacts) { this.impacts.cancelPressure(r); this.impacts.cancelFriendly(r); }
      let amount = 0;
      for (const id of r.unitIds) {
        const u = this.units[id];
        if (u.alive && !u.routed) { u.routed = true; u.state = 'rout'; u.target = -1; u._pushUntil = 0; this._bodyLive[u.id] = 0; amount++; }
      }
      r._routed = true; r.alive = 0; r.morale = 0; r.order = 'retreat'; r.fallingBack = false; r.status = 'routing';
      this._stats[r.team].alive -= amount; this._stats[r.team].routed += amount;
      this._event(r.name + ' ' + (reason || 'breaks and flees') + '.', r.team, 'rout');
      for (const other of this.regiments) {
        if (other.team === r.team && other.alive > 0 && Math.hypot(other.x - r.x, other.y - r.y) < 420) other.morale = Math.max(0, other.morale - 2);
      }
    }

    _moraleUpdate() {
      for (const r of this.regiments) {
        if (r.alive <= 0) continue;
        const t = TYPES[r.type];
        let slot = 0, cx = 0, cy = 0, displacement = 0, arrived = 0, forestCount = 0, engaged = 0, flexing = 0, braced = 0, pushing = 0;
        const xs = [], ys = [];
        r._actualMinX = r._actualMinY = Infinity; r._actualMaxX = r._actualMaxY = -Infinity;
        const arrivalRadius = Math.max(85, Math.hypot(r.width, r.depth) * .55 + 30);
        for (const id of r.unitIds) {
          const u = this.units[id];
          if (u.alive && !u.routed) {
            u._slot = slot++; cx += u.x; cy += u.y;
            xs.push(u.x); ys.push(u.y);
            r._actualMinX = Math.min(r._actualMinX, u.x); r._actualMaxX = Math.max(r._actualMaxX, u.x);
            r._actualMinY = Math.min(r._actualMinY, u.y); r._actualMaxY = Math.max(r._actualMaxY, u.y);
            if (Math.hypot(u.x - r.targetX, u.y - r.targetY) < arrivalRadius) arrived++;
            if (u._terrain.type === 'forest') forestCount++;
            if (u._lastAttack >= this.time - 1.5) engaged++;
            if (t.brace && this._braceReadiness(u) >= .5) braced++;
            if (u._lastPush >= this.time - .55) pushing++;
            const expected = this._formationGoal(u, r, true);
            const advance = r._combatActive ? Math.hypot(r.frontX - r.x, r.frontY - r.y) : 0;
            displacement += Math.min(350, Math.max(0, Math.hypot(u.x - expected.x, u.y - expected.y) - advance));
            if (u._flex > .2 && Math.hypot(u.x - expected.x, u.y - expected.y) - advance > r._spacing * .5) flexing++;
          }
        }
        if (slot) { cx /= slot; cy /= slot; displacement /= slot; }
        if (slot) {
          xs.sort((a, b) => a - b); ys.sort((a, b) => a - b);
          const trim = slot >= 12 ? Math.floor(slot * .08) : 0;
          r._frontBounds = { minX: xs[trim], maxX: xs[slot - 1 - trim], minY: ys[trim], maxY: ys[slot - 1 - trim] };
        }
        r._centroidX = cx; r._centroidY = cy;
        r._arrivalFraction = arrived / Math.max(1, slot);
        r._arrivalDistance = Math.hypot(cx - r.targetX, cy - r.targetY);
        r._terrainContacts = forestCount;
        r.engagedSoldiers = engaged;
        r.bracedSoldiers = braced; r.pushingSoldiers = pushing;
        if (r.order === 'face') r._faceError = displacement;
        if (this._marchingBody(r)) r._marchLag = displacement;
        r.flexingSoldiers = flexing;
        const obstruction = r._moveAttempts ? r._blockedMoves / r._moveAttempts : 0;
        const enemyObstruction = r._moveAttempts ? r._enemyContacts / r._moveAttempts : 0;
        r._collisionSlow = clamp(1 - enemyObstruction * 1.2, .08, 1);
        r._commandBlocked = r.order !== 'hold' && r.order !== 'auto' && obstruction > .85 && r.engagedSoldiers === 0;
        // Standards follow the actual troops when a battle line is blocked;
        // their commanded anchor cannot march through the enemy by itself.
        if (enemyObstruction > .18 || (r.fallingBack && !r._traffic) || r._nearestEnemyDistance < 150) {
          r.x += (cx - r.x) * .65; r.y += (cy - r.y) * .65;
        }
        const rankFirmness = r.formation !== 'loose' && r._terrainPressure < .2 ? (t.rankStiffness || 0) : 0;
        let targetCohesion = clamp(100 - displacement * .22 * (1 - rankFirmness * .45) - obstruction * (r.order === 'hold' ? 8 : 36) * (1 - rankFirmness * .5) - r._flankHits / Math.max(1, r.alive) * 38 - r.deformation * .28 - r.trafficPressure * 12, 8, 100);
        if(r._rampartJob){r.x=cx;r.y=cy;if(!r._rampartJob.leaving)targetCohesion=Math.max(80,100-r._recentHits/Math.max(1,r.alive*t.hp)*40);}
        r.cohesion += (targetCohesion - r.cohesion) * (targetCohesion < r.cohesion ? .16 : .08);
        r.morale = Math.max(0, r.morale - r._recentHits / Math.max(1, r.initial * t.hp) * 14 - r._flankHits / Math.max(1, r.initial) * 5);
        let commander = null;
        for (const candidate of this.regiments) {
          if (candidate.team === r.team && candidate.alive > 0 && !candidate.fallingBack && TYPES[candidate.type].commander && Math.hypot(candidate.x - r.x, candidate.y - r.y) <= (TYPES[candidate.type].supportRadius || 360)) { commander = candidate; break; }
        }
        if (commander) { r.morale = Math.min(t.morale, r.morale + .55); r.cohesion = Math.min(100, r.cohesion + .8); }
        if (r.fallingBack) {
          const support = this.regiments[r.supportRegimentId];
          const supported = !!commander || !!(support && support.alive > 0 && !support.fallingBack && Math.hypot(support.x - r.x, support.y - r.y) < 330);
          const safe = r._recentHits < Math.max(2, r.alive * .1) && r._nearestEnemyDistance > 160;
          if (supported && safe) { r.morale = Math.min(t.morale, r.morale + 1.4); r.cohesion = Math.min(100, r.cohesion + 1.8); r._unsupportedTime = 0; }
          else {
            r._unsupportedTime += .5;
            if (safe && r._fallbackTime > 18) { r.morale = Math.min(t.morale, r.morale + .6); r.cohesion = Math.min(100, r.cohesion + .9); }
          }
          if (r._fallbackTime >= 12 && r.morale >= 45 && r.cohesion >= 45 && (supported || (safe && r._fallbackTime >= 30))) this._recover(r, false);
          else if (r._unsupportedTime >= 45 && r.morale < 8 && r.alive / r.initial <= .2) this._route(r, 'breaks after an unsupported withdrawal');
        } else {
          if (r._recentHits < 1 && r._nearestEnemyDistance > t.range + 120) r.morale = Math.min(t.morale, r.morale + .25);
          if (!t.commander && r.alive / r.initial <= .2 && r.morale < 12 && engaged + r._recentHits > 0) this._route(r, 'breaks under devastating losses');
          else if (!t.commander && r.morale < 22 && r.alive / r.initial < .78 && r._rallyTime <= 0 && r.order !== 'move') this._startFallback(r, false);
          else r.status = r._rallyTime > 0 ? 'rallying' : r.morale < 42 || r.cohesion < 40 ? 'shaken' : 'steady';
          if (t.commander && r.rallyCooldown <= 0) {
            const distressed = this.regiments.some(other => other.team === r.team && other.fallingBack && other._fallbackTime > 20 && other.alive > 0 && Math.hypot(other.x - r.x, other.y - r.y) <= r.rallyRadius);
            if (distressed) this._rally(r);
          }
        }
        r._recentHits = 0; r._blockedMoves = 0; r._moveAttempts = 0; r._flankHits = 0; r._allyContacts = 0; r._enemyContacts = 0;
        this._commandFeedback(r);
      }
      this._refreshStats();
      this._checkVictory();
    }

    _refreshStats() {
      this._stats[0].morale = 0; this._stats[1].morale = 0;
      this._stats[0].power = 0; this._stats[1].power = 0;
      this._stats[0].fallingBack = 0; this._stats[1].fallingBack = 0;
      for (const r of this.regiments) {
        this._stats[r.team].morale += r.morale * r.alive;
        this._stats[r.team].power += r.alive * TYPES[r.type].cost;
        if (r.fallingBack) this._stats[r.team].fallingBack += r.alive;
      }
      for (const stats of this._stats) stats.morale = stats.alive ? stats.morale / stats.alive : 0;
    }

    getStats() {
      return { teams: this._stats.map(s => Object.assign({}, s)), time: this.time, winner: this.winner, reason: this.reason };
    }

    _checkVictory() {
      if (this.phase !== 'running') return;
      const left = this._stats[0].alive, right = this._stats[1].alive;
      if (!left || !right) {
        if (this.impacts && this.time < this.impacts.lastLanding) return;
        this.winner = !left && !right ? 'draw' : left ? 0 : 1;
        this.reason = this.winner === 'draw' ? 'Both armies have left the field.' : 'The opposing army has been defeated or routed.';
        this.phase = 'ended';
      } else if (this.time >= 900) {
        const power0 = this._stats[0].power, power1 = this._stats[1].power;
        this.winner = Math.abs(power0 - power1) < Math.max(power0, power1) * .04 ? 'draw' : power0 > power1 ? 0 : 1;
        this.reason = 'The 15-minute battle limit was reached. Remaining army strength decides the field.';
        this.phase = 'ended';
      }
      if (this.phase === 'ended') this._event(this.winner === 'draw' ? 'The battle ends in a draw.' : this.config.armies[this.winner].name + ' holds the field.', typeof this.winner === 'number' ? this.winner : null, 'victory');
    }

    _event(text, team, kind) {
      this.events.push({ time: this.time, text, team, kind });
      if (this.events.length > 100) this.events.splice(0, this.events.length - 100);
    }

    _effect(x, y, kind, size, team, life, unitId, blocked) {
      if (this.effects.length < MAX_EFFECTS) {
        const effect = { x, y, kind, size, team, age: 0, life: life || .5 };
        if (unitId !== undefined) { effect.unitId = unitId; effect.blocked = !!blocked; }
        this.effects.push(effect);
        return effect;
      }
    }

    step(dt) {
      if (this.phase !== 'running' || !Number.isFinite(dt) || dt <= 0) return;
      dt = Math.min(dt, .1);
      this.time += dt; this._tick++;
      this._strategyTimer -= dt; this._moraleTimer -= dt;
      if (this._strategyTimer <= 0) { this._strategize(); this._strategyTimer = .55; }
      this._rebuildHash(true);
      if (this.impacts) this.impacts.beginStep(dt);
      this._stepRegiments(dt);
      this._stepClearMarches(dt);
      this._prepareMountedStreams();
      this._inUnitStep = true;
      // Alternate traversal order to avoid granting one team the first strike every tick.
      const units = this.units, siegeStep = this.structures.length > 0 && this.siege;
      if (this._tick & 1) {
        for (let i = 0; i < units.length; i++) {
          const u = units[i];
          if (u.alive && !(siegeStep && siegeStep.stepUnit(u, dt))) this._stepUnit(u, dt);
        }
      } else {
        for (let i = units.length - 1; i >= 0; i--) {
          const u = units[i];
          if (u.alive && !(siegeStep && siegeStep.stepUnit(u, dt))) this._stepUnit(u, dt);
        }
      }
      this._inUnitStep = false;
      this._stepMusketVolleys();
      for (let i = this.projectiles.length - 1; i >= 0; i--) {
        const p = this.projectiles[i];
        const previous = p.age / p.duration;
        p.age += dt;
        const progress = Math.min(1, p.age / p.duration);
        p.x = p.sx + (p.tx - p.sx) * progress;
        p.y = p.sy + (p.ty - p.sy) * progress;
        p.z = Math.sin(progress * Math.PI) * p.arc;
        if (((p.ordnance||p.rampartTrajectory) && this._traceOrdnance(p, previous, progress)) || progress >= 1) {
          this._impact(p);
          this.projectiles[i] = this.projectiles[this.projectiles.length - 1];
          this.projectiles.pop();
        }
      }
      for (let i = this.effects.length - 1; i >= 0; i--) {
        const effect = this.effects[i]; effect.age += dt;
        if (effect.age >= effect.life) { this.effects[i] = this.effects[this.effects.length - 1]; this.effects.pop(); }
      }
      if (this._moraleTimer <= 0) { this._moraleUpdate(); this._moraleTimer = .5; }
      if (this.time - this._lastCasualty > 75 && this.time - this._lastStallEvent > 90) {
        for (const r of this.regiments) if (r.alive > 0 && r.order === 'auto') r._pathTimer = 0;
        this._lastStallEvent = this.time;
      }
    }
  }

  global.BattleEngine = BattleEngine;
})(typeof window !== 'undefined' ? window : globalThis);
