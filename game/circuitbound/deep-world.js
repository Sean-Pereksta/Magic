/* Deterministic Circuitbound terrain; save only changes to the generated world. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.CircuitDeepWorld = factory();
})(typeof globalThis === 'object' ? globalThis : this, function () {
  'use strict';

  const WIDTH = 2048;
  const HEIGHT = 512;
  const LEGACY_WIDTH = 220;
  const LEGACY_HEIGHT = 84;
  const CHUNK = 32;
  const FOG = 8;
  const CHUNK_COLUMNS = WIDTH / CHUNK;
  const FOG_COLUMNS = WIDTH / FOG;
  const FOG_BYTES = WIDTH * HEIGHT / FOG ** 2 / 8;
  const MAX_CACHED_CHUNKS = 96;
  const FORMAT = 1;

  const REGIONS = [
    {id:'scrapyard', name:'Surface Scrapyards', color:'#9bbd8b', start:0, end:41, tier:0},
    {id:'iron', name:'Iron Caverns', color:'#a4b6c4', start:42, end:53, tier:1},
    {id:'crystal', name:'Crystal Hollows', color:'#78e6e8', start:54, end:64, tier:2},
    {id:'industrial', name:'Forgotten Industrial Depths', color:'#d5a271', start:65, end:73, tier:3},
    {id:'abyss', name:'Reactor Abyss', color:'#dc83a7', start:74, end:83, tier:4},
    {id:'faultGalleries', name:'Fault Galleries', color:'#9bb5ba', start:84, end:119, tier:5},
    {id:'osmiumBasin', name:'Osmium Basin', color:'#ffc080', start:120, end:195, tier:6, ore:'osmiumOre'},
    {id:'gravityHollows', name:'Gravity Hollows', color:'#a5e5a9', start:196, end:279, tier:7, ore:'graviteOre'},
    {id:'nullCrystalVaults', name:'Null Crystal Vaults', color:'#baadff', start:280, end:367, tier:8, ore:'nullQuartzOre'},
    {id:'starcoreMantle', name:'Starcore Mantle', color:'#ffa4a8', start:368, end:511, tier:9, ore:'starcoreOre'}
  ].map(region => ({...region, depth:region.start / LEGACY_HEIGHT}));

  const SEAMS = [
    {tile:'OSMIUM_ORE', ore:'osmiumOre', name:'Osmium', depth:142, region:'osmiumBasin'},
    {tile:'GRAVITE_ORE', ore:'graviteOre', name:'Gravite', depth:225, region:'gravityHollows'},
    {tile:'NULL_QUARTZ_ORE', ore:'nullQuartzOre', name:'Null Quartz', depth:306, region:'nullCrystalVaults'},
    {tile:'STARCORE_ORE', ore:'starcoreOre', name:'Starcore', depth:394, region:'starcoreMantle'}
  ];
  const PLANS = [
    [['breachHammer','Borer Test Gallery'], ['shardScattergun','Shard Foundry'], ['anchorHarpoon','Harpoon Drop Shaft'], ['repeaterCarbine','Security Yard']],
    [['gravityMaul','Lift Test Chamber'], ['orbitDisc','Flywheel Workshop'], ['compressionProjector','Compression Lab'], ['kineticBow','Kinetic Firing Alley']],
    [['prismRifle','Prism Vault'], ['phaseSabers','Phase Annex'], ['interferenceWand','Signal Vault'], ['echoMortar','Echo Loading Bay']],
    [['starcoreLance','Mantle Rail Gallery'], ['coronaCaster','Corona Shield Room'], ['singularityLauncher','Gravity Furnace'], ['mantleBreaker','Mantle Guard Post']]
  ];
  const SHAFTS = [181, 380, 677, 963, 1260, 1580, 1900];
  const SURFACE = ['East Freight Yard', 'Collapsed Lark Mine', 'Wayfarer Launch Site', 'North Spur Freight Yard', 'Collapsed Meridian Mine', 'Terminus Launch Site'];
  const inside = (x, y) => Number.isInteger(x) && Number.isInteger(y) && x >= 0 && y >= 0 && x < WIDTH && y < HEIGHT;
  const keyAt = (x, y) => x + y * WIDTH;
  const clamp = (n, min, max) => Math.max(min, Math.min(max, n));

  function hash(x, y, seed) {
    let n = x * 374761393 + y * 668265263 + seed * 1442695041;
    n = (n ^ (n >> 13)) * 1274126177;
    return ((n ^ (n >> 16)) >>> 0) / 4294967295;
  }

  function terrainNoise(x, y, seed) {
    let n = Math.imul(x, 374761393) ^ Math.imul(y, 668265263) ^ Math.imul(seed, 1442695041);
    n = Math.imul(n ^ (n >>> 13), 1274126177);
    return ((n ^ (n >>> 16)) >>> 0) / 4294967295;
  }

  function groundAt(x, seed) {
    const legacySlope = Math.sin(x * .12) * 2 + Math.sin(x * .035) * 5;
    const eastSlope = Math.max(0, Math.min(1, (x - LEGACY_WIDTH) / 35)) * Math.sin(x * .009) * 4;
    return 31 + Math.floor(legacySlope + eastSlope + hash(x, 1, seed) * 2);
  }

  function regionAt(y) {
    for (let n = REGIONS.length - 1; n > 0; n--) if (y >= REGIONS[n].start) return REGIONS[n];
    return REGIONS[0];
  }

  function encode(bytes) {
    let binary = '';
    for (let offset = 0; offset < bytes.length; offset += 8192) {
      binary += String.fromCharCode(...bytes.subarray(offset, offset + 8192));
    }
    return btoa(binary);
  }

  function decode(data) {
    if (typeof data !== 'string') throw new Error('Invalid world data');
    return Uint8Array.from(atob(data), char => char.charCodeAt(0));
  }

  function readEdits(data, defs) {
    if (data?.format !== FORMAT) throw new Error('Unsupported world format');
    const bytes = decode(data.edits);
    const edits = new Map();
    if (bytes.length % 4 || bytes.length / 4 > WIDTH * HEIGHT) throw new Error('Invalid world edits');
    for (let offset = 0; offset < bytes.length; offset += 4) {
      const key = bytes[offset] + bytes[offset + 1] * 256 + bytes[offset + 2] * 65536;
      const id = bytes[offset + 3];
      if (key >= WIDTH * HEIGHT || !defs[id] || edits.has(key)) throw new Error('Invalid world tile');
      edits.set(key, id);
    }
    return edits;
  }

  function validSave(data, defs) {
    try {
      readEdits(data, defs);
      return decode(data.fog).length === FOG_BYTES;
    } catch {
      return false;
    }
  }

  function remapLegacyKey(key) {
    return key % LEGACY_WIDTH + Math.floor(key / LEGACY_WIDTH) * WIDTH;
  }

  function makeLayout(T, seed) {
    const features = new Map();
    const sites = [];
    const fixtures = [];
    function feature(x0, y0, x1, y1, paint) {
      const shape = {x0, y0, x1, y1, paint};
      for (let cy = Math.max(0, Math.floor(y0 / CHUNK)); cy <= Math.min(HEIGHT / CHUNK - 1, Math.floor(y1 / CHUNK)); cy++) {
        for (let cx = Math.max(0, Math.floor(x0 / CHUNK)); cx <= Math.min(CHUNK_COLUMNS - 1, Math.floor(x1 / CHUNK)); cx++) {
          const key = cx + cy * CHUNK_COLUMNS;
          if (!features.has(key)) features.set(key, []);
          features.get(key).push(shape);
        }
      }
    }
    function fill(x0, y0, x1, y1, id) {
      feature(x0, y0, x1, y1, () => id);
    }
    function passage(from, to, floor, headroom = 5) {
      const left = Math.min(from, to), right = Math.max(from, to);
      fill(left, floor - headroom, right, floor - 1, T.AIR);
      fill(left, floor, right, floor, T.STONE);
    }
    function fixture(x, y, tile, data) {
      if (tile !== undefined) fixtures.push({x, y, tile, data});
    }
    function landmark(site) {
      sites.push(site);
      return site;
    }

    // Side caverns break the shafts' symmetry, but designed expedition rooms clear these where they meet.
    for (let gx = 0; gx < WIDTH; gx += 56) for (let gy = 72; gy < HEIGHT - 26; gy += 32) {
      if (terrainNoise(gx, gy, seed) < .35) continue;
      const cx = gx + 14 + Math.floor(terrainNoise(gx + 7, gy, seed) * 30);
      const cy = gy + 9 + Math.floor(terrainNoise(gx, gy + 7, seed) * 18);
      const rx = 5 + Math.floor(terrainNoise(gx + 19, gy, seed) * 12);
      const ry = 3 + Math.floor(terrainNoise(gx, gy + 19, seed) * 7);
      feature(cx - rx, cy - ry, cx + rx, cy + ry, (x, y, id) => {
        if (x < LEGACY_WIDTH && y < LEGACY_HEIGHT) return id;
        return ((x - cx) / rx) ** 2 + ((y - cy) / ry) ** 2 < 1 ? T.AIR : id;
      });
    }

    for (let lane = 0; lane < SHAFTS.length; lane++) {
      const shaft = SHAFTS[lane];
      const name = lane === 0 ? 'Reactor Abyss Descent' : SURFACE[lane - 1];
      const mouth = lane === 0 ? LEGACY_HEIGHT : groundAt(shaft, seed);
      landmark({id:`entrance-${lane}`, x:shaft, y:mouth, kind:lane === 0 ? 'entrance' : 'surface', name, region:lane === 0 ? 'faultGalleries' : 'scrapyard'});
      if (lane > 0) {
        const deck = mouth + 1;
        fill(shaft - 9, deck - 6, shaft + 11, deck - 1, T.AIR);
        fill(shaft - 9, deck, shaft + 11, deck, T.STONE);
        const launch = lane % 3 === 0;
        const freight = lane % 3 === 1;
        for (let x = shaft - 8; x <= (launch ? shaft + 3 : shaft + 10); x += 3) fill(x, deck - 6, x, deck - 6, T.FRAME);
        fixture(shaft + 4, deck - 1, T.STORAGE, {rot:0, contents:{iron:5, steel:2, ...(launch ? {flightCell:2} : {})}, landmarkName:name});
        if (launch) {
          fixture(shaft + 8, deck, T.STARSHIP_PAD, {rot:0, landmarkName:name});
          fixture(shaft + 8, deck + 1, T.GENERATOR, {rot:0});
        } else if (freight) {
          fixture(shaft - 7, deck - 1, T.FREIGHT_STOP, {rot:0, freightName:name});
          fixture(shaft - 7, deck, T.GENERATOR, {rot:0});
          if (T.FREIGHT_RAIL !== undefined) fill(shaft - 11, deck - 1, shaft - 8, deck - 1, T.FREIGHT_RAIL);
        } else {
          fill(shaft - 10, deck - 2, shaft - 8, deck - 1, T.OBSIDIAN);
        }
      }

      // The expanded edge contains the original rare materials and their basic research route too.
      if (lane === 1) {
        const precursor = [
          ['metallurgy','East Works Metallurgy',52,T.COBALT_ORE,{cobaltOre:6,steel:3}],
          ['robotics','East Works Foundry',65,T.IRIDIUM_ORE,{iridiumOre:5,circuit:3}],
          ['aetherEngineering','East Works Reactor',79,T.AETHERIUM_ORE,{aetheriumOre:4,crystal:4}]
        ];
        for (const [schematic, title, floor, ore, contents] of precursor) {
          const cx = shaft - 25;
          passage(shaft, cx - 6, floor);
          if (ore !== undefined) fill(cx - 5, floor, cx + 4, floor + 4, ore);
          const site = landmark({id:`precursor-${schematic}`, x:cx - 5, y:floor - 1, kind:'schematic', name:title, region:regionAt(floor).id, schematic});
          fixture(site.x, site.y, T.STORAGE, {rot:0, contents, discovery:true, schematic, landmarkName:title});
        }
      }

      const faultFloor = 102 + lane % 3 * 3;
      const alcoveSide = lane % 2 === 0 ? 1 : -1;
      passage(shaft, shaft + alcoveSide * 24, faultFloor, 6);
      const survey = landmark({id:`survey-fault-${lane}`, x:shaft + alcoveSide * 16, y:faultFloor - 1, kind:'surveyStation', name:'Fault Galleries Survey Station', region:'faultGalleries'});
      fixture(survey.x, survey.y, T.STORAGE, {rot:0, surveyStation:true, surveySite:survey.id, surveyRadius:90, landmarkName:survey.name, contents:{ladder:5, repairKit:1}});
      fixture(shaft + alcoveSide * 7, faultFloor - 1, T.WINCH, {rot:0, ruin:true, disabled:true, landmarkName:'Abandoned Freight Winch'});

      for (let tier = 0; tier < SEAMS.length; tier++) {
        const seam = SEAMS[tier];
        const floor = seam.depth + (lane === 0 ? 0 : Math.floor(terrainNoise(lane, seam.depth, seed) * 17) - 6);
        const side = lane % 2 === 0 ? -1 : 1;
        const depositX = shaft + side * (17 + lane % 3 * 3);
        const halfWidth = lane === 0 ? 3 : lane % 3 === 2 ? 9 : 6;
        const height = lane === 0 ? 8 : lane % 3 === 2 ? 12 : 9;
        passage(shaft, depositX - side * halfWidth, floor, 6);
        passage(depositX - halfWidth - 2, depositX + halfWidth + 2, floor, 7);
        landmark({id:`seam-${lane}-${tier}`, x:depositX, y:floor, kind:'deposit', name:`${seam.name} ${lane === 0 ? 'Pocket' : lane % 3 === 2 ? 'Field' : 'Seam'}`, ore:seam.ore, region:seam.region, tier:tier + 1});
        feature(depositX - halfWidth, floor, depositX + halfWidth, floor + height - 1, (x, y, id) => {
          const distance = Math.abs(x - depositX) / (halfWidth + 1);
          const depth = (y - floor) / height;
          const solidCore = distance < .6 && depth < .7;
          return solidCore || terrainNoise(x, y, seed + 33) > .15 + distance * .5 + depth * .2 ? T[seam.tile] ?? T.STONE : id;
        });
        const surveyX = shaft + side * 4;
        const station = landmark({id:`survey-${lane}-${tier}`, x:surveyX, y:floor - 1, kind:'surveyStation', name:`${seam.name} Survey Station`, region:seam.region});
        fixture(station.x, station.y, T.STORAGE, {rot:0, surveyStation:true, surveySite:station.id, surveyRadius:110, landmarkName:station.name, contents:{iron:2, crystal:1}});
        if (lane < 4) {
          const [schematic, planName] = PLANS[tier][lane];
          const chamberFloor = floor + (lane === 0 ? 18 : 15);
          const planX = shaft - side * (33 + tier * 2);
          passage(shaft, planX + side * 5, chamberFloor, schematic === 'anchorHarpoon' ? 3 : 5);
          passage(planX - 8, planX + 8, chamberFloor, 7);
          if (schematic === 'prismRifle') {
            for (let step = 0; step < 6; step++) {
              fill(planX + side * (6 + step), chamberFloor - 5 - Math.floor(step / 2), planX + side * (6 + step), chamberFloor - 5 - Math.floor(step / 2), T.OBSIDIAN);
            }
            fill(planX - 3, chamberFloor - 2, planX - 3, chamberFloor - 1, T.OBSIDIAN);
            fill(planX + 4, chamberFloor - 3, planX + 4, chamberFloor - 2, T.OBSIDIAN);
          } else if (schematic === 'anchorHarpoon') {
            fill(planX + side * 5, chamberFloor - 12, planX + side * 7, chamberFloor - 4, T.AIR);
            fill(planX + side * 6, chamberFloor - 12, planX + side * 6, chamberFloor - 5, T.LADDER);
          }
          const plan = landmark({id:`plan-${schematic}`, x:planX, y:chamberFloor - 1, kind:'schematic', name:planName, region:seam.region, schematic});
          fixture(plan.x, plan.y, T.STORAGE, {rot:0, discovery:true, schematic, landmarkName:planName, contents:{scrap:3, ammo:2}});
        }
      }

      // Cut the main route last to keep passage floors from blocking the climbing lane.
      fill(shaft - 2, mouth, shaft + 2, HEIGHT - 8, T.AIR);
      fill(shaft, mouth, shaft, HEIGHT - 8, T.LADDER);
      if (lane > 0) fill(shaft + 3, mouth + 3, shaft + 3, HEIGHT - 8, T.MAGNETIC_RAIL ?? T.RAIL);
      for (let landing = faultFloor + 11; landing < HEIGHT - 9; landing += 25) {
        fill(shaft - 6, landing - 4, shaft - 1, landing - 1, T.AIR);
        fill(shaft - 6, landing, shaft - 1, landing, T.PLATFORM);
      }
    }
    return {features, sites, fixtures};
  }

  class World {
    constructor(T, defs, seed, saved) {
      Object.assign(this, {T, defs, seed, width:WIDTH, height:HEIGHT});
      this.chunks = new Map();
      this.edits = saved ? readEdits(saved, defs) : new Map();
      this.fog = saved ? decode(saved.fog) : new Uint8Array(FOG_BYTES);
      const layout = makeLayout(T, seed);
      Object.assign(this, layout);
      this.sitesById = new Map(this.sites.map(site => [site.id, site]));
      this.surveyed = new Map();
      for (const entry of Array.isArray(saved?.surveyed) ? saved.surveyed : []) {
        if (!Array.isArray(entry)) continue;
        const [id, precision] = entry;
        if (this.sitesById.get(id)?.kind === 'deposit') this.surveyed.set(id, precision === 2 ? 2 : 1);
      }
      this.markers = Array.isArray(saved?.markers) ? saved.markers.filter(marker => marker && typeof marker.id === 'string' && typeof marker.name === 'string' && inside(marker.x, marker.y)).slice(0, 128) : [];
      this.pinned = typeof saved?.pinned === 'string' ? saved.pinned : null;
      this.explorationRevision = 0;
    }

    natural(x, y, ground) {
      const T = this.T;
      if (y < ground) return T.AIR;
      if (y === ground) return T.GRASS;
      if (y < ground + 5) return T.DIRT;
      if (x < LEGACY_WIDTH && y < LEGACY_HEIGHT) {
        const r = hash(x, y, this.seed);
        if (y > LEGACY_HEIGHT - 7 && r > .45) return T.OBSIDIAN;
        if (y > ground + 14 && r > .973) return T.CRYSTAL;
        if (y > ground + 9 && r < .045) return T.IRON;
        return y > ground + 5 && r > .93 ? T.COPPER : T.STONE;
      }
      const r = terrainNoise(x, y, this.seed);
      if (y > 367 && r > .54 || y > 80 && r > .87) return T.OBSIDIAN;
      if (y > ground + 14 && r > .965) return T.CRYSTAL;
      if (y > ground + 9 && r < .06) return T.IRON;
      return y > ground + 5 && r < .11 ? T.COPPER : T.STONE;
    }

    base(x, y) {
      const cx = Math.floor(x / CHUNK), cy = Math.floor(y / CHUNK);
      const key = cx + cy * CHUNK_COLUMNS;
      let chunk = this.chunks.get(key);
      if (!chunk) {
        const x0 = cx * CHUNK, y0 = cy * CHUNK;
        chunk = new Uint8Array(CHUNK ** 2);
        for (let dx = 0; dx < CHUNK; dx++) {
          const ground = groundAt(x0 + dx, this.seed);
          for (let dy = 0; dy < CHUNK; dy++) chunk[dx + dy * CHUNK] = this.natural(x0 + dx, y0 + dy, ground);
        }
        for (const shape of this.features.get(key) || []) {
          for (let ty = Math.max(y0, shape.y0); ty <= Math.min(y0 + CHUNK - 1, shape.y1); ty++) {
            for (let tx = Math.max(x0, shape.x0); tx <= Math.min(x0 + CHUNK - 1, shape.x1); tx++) {
              const index = tx - x0 + (ty - y0) * CHUNK;
              chunk[index] = shape.paint(tx, ty, chunk[index]);
            }
          }
        }
        if (this.chunks.size === MAX_CACHED_CHUNKS) this.chunks.delete(this.chunks.keys().next().value);
        this.chunks.set(key, chunk);
      }
      return chunk[x % CHUNK + y % CHUNK * CHUNK];
    }

    get(x, y) {
      if (!inside(x, y)) return this.T.STONE;
      return this.edits.get(keyAt(x, y)) ?? this.base(x, y);
    }

    getIndex(key) {
      return this.get(key % WIDTH, Math.floor(key / WIDTH));
    }

    set(x, y, tile) {
      const key = keyAt(x, y);
      if (tile === this.base(x, y)) this.edits.delete(key);
      else this.edits.set(key, tile);
    }

    placedKeys() {
      return this.edits.keys();
    }

    fixturesInto(set) {
      for (const fixture of this.fixtures) set(fixture.x, fixture.y, fixture.tile, fixture.data);
    }

    importLegacy(tiles) {
      for (let y = 0; y < LEGACY_HEIGHT; y++) for (let x = 0; x < LEGACY_WIDTH; x++) {
        this.set(x, y, tiles[x + y * LEGACY_WIDTH]);
      }
    }

    reveal(x, y, radius = 13) {
      if (!Number.isFinite(x) || !Number.isFinite(y)) return false;
      let changed = false;
      for (let cy = Math.max(0, Math.floor((y - radius) / FOG)); cy <= Math.min(HEIGHT / FOG - 1, Math.floor((y + radius) / FOG)); cy++) {
        for (let cx = Math.max(0, Math.floor((x - radius) / FOG)); cx <= Math.min(FOG_COLUMNS - 1, Math.floor((x + radius) / FOG)); cx++) {
          const cell = cx + cy * FOG_COLUMNS;
          const bit = 1 << (cell % 8), index = cell >> 3;
          if (!(this.fog[index] & bit)) {
            this.fog[index] |= bit;
            changed = true;
          }
        }
      }
      if (changed) this.explorationRevision++;
      return changed;
    }

    isExplored(x, y) {
      if (!inside(Math.floor(x), Math.floor(y))) return false;
      const cell = Math.floor(x / FOG) + Math.floor(y / FOG) * FOG_COLUMNS;
      return !!(this.fog[cell >> 3] & 1 << (cell % 8));
    }

    *exploredCells() {
      for (let i = 0; i < this.fog.length; i++) {
        if (!this.fog[i]) continue;
        for (let bit = 0; bit < 8; bit++) if (this.fog[i] & 1 << bit) {
          const cell = i * 8 + bit;
          yield [cell % FOG_COLUMNS * FOG, Math.floor(cell / FOG_COLUMNS) * FOG];
        }
      }
    }

    surveyMarker(site, precision) {
      const offset = precision === 2 ? 0 : terrainNoise(site.x, site.y, this.seed) < .5 ? -8 : 8;
      return {id:site.id, x:site.x + offset, y:site.y, kind:'survey', name:precision === 2 ? site.name : `${site.name} area`, ore:site.ore, region:site.region, precise:precision === 2};
    }

    survey(x, y, radius = 30, precise = true) {
      const precision = precise ? 2 : 1;
      const found = [];
      for (const site of this.sites) {
        if (site.kind !== 'deposit' || (this.surveyed.get(site.id) || 0) >= precision || Math.hypot(site.x - x, site.y - y) > radius) continue;
        this.surveyed.set(site.id, precision);
        found.push(this.surveyMarker(site, precision));
      }
      return found;
    }

    get surveyMarkers() {
      return [...this.surveyed].map(([id, precision]) => this.surveyMarker(this.sitesById.get(id), precision));
    }

    export() {
      const bytes = new Uint8Array(this.edits.size * 4);
      let offset = 0;
      for (const [key, id] of this.edits) {
        bytes[offset++] = key & 255;
        bytes[offset++] = (key >>> 8) & 255;
        bytes[offset++] = key >>> 16;
        bytes[offset++] = id;
      }
      return {format:FORMAT, edits:encode(bytes), fog:encode(this.fog), surveyed:[...this.surveyed], markers:this.markers, pinned:this.pinned};
    }
  }

  return {World, WIDTH, HEIGHT, LEGACY_WIDTH, LEGACY_HEIGHT, CHUNK, FOG, REGIONS, SEAMS, validSave, remapLegacyKey, groundAt, hash, regionAt};
});
