import { ASSET_BASE_URL, ASSET_CATALOG } from './catalog.js';

export { ASSET_CATALOG };
export const BIOME_ALIASES = { galilee:'countryside', rome:'city', jerusalem:'ruins', hell:'desert', heaven:'storm' };
export const biomeKey = value => BIOME_ALIASES[value] || value || 'countryside';
export const HAZARD_ART = {
  spikes:'hazard-spike-trap', spike:'hazard-spike-trap', brambles:'hazard-brambles',
  fallingRock:'hazard-falling-rock', rock:'hazard-falling-rock', falling:'hazard-falling-rock',
  flameJet:'hazard-flame-jet', fireJet:'hazard-flame-jet', fire:'hazard-flame-jet', lava:'hazard-flame-jet',
  lightning:'hazard-lightning-strike', water:'hazard-water-surface', river:'hazard-water-surface',
  waterfall:'hazard-waterfall', wind:'hazard-wind-gust', windGust:'hazard-wind-gust', chase:'hazard-wind-gust',
};
const PLAYER_CORE = ['idle','walk','run','accelerate','stop','jump','double-jump','fall','land','wall-slide','wall-jump','dash','dodge','attack','hurt','death','recover','interact','victory'];
const CORE_FX = ['jump-dust','landing-dust','dash-trail','blessing-attack-arc','gold-impact','damage-burst','pickup-collect','checkpoint-activation','rubble-destruction','radial-orb','shield-pulse','bread-healing-aura'];
const ENEMY_STATES = ['idle','move','attack','hurt','death'];
const BOSS_STATES = [...ENEMY_STATES,'attack2'];
const POWER_ART = {
  water:['fx-water-splash','fx-wings-flight-aura'], wind:['fx-sandals-speed-trail'],
  sandals:['fx-sandals-speed-trail'], wings:['player.jesus.glide','fx-wings-flight-aura'],
  shield:['player.jesus.shield','fx-shield-pulse'], bread:['player.jesus.heal','fx-bread-healing-aura','fx-heal-recovery'],
  holyLight:['player.jesus.holy-light','fx-holy-light-judgment'], 'holy-light':['player.jesus.holy-light','fx-holy-light-judgment'],
  fireRain:['player.jesus.fire-rain','fx-fire-meteor','fx-fire-eruption'], 'fire-rain':['player.jesus.fire-rain','fx-fire-meteor','fx-fire-eruption'],
};
const ENEMY_FX = {
  javelin:['projectile-javelin'], zealot:['projectile-thorn'], 'temple-guard':['fx-ground-shockwave'],
  'leaping-scorpion':['projectile-thorn'], 'fire-skull':['fx-fire-meteor'], imp:['fx-fire-meteor'],
  'brim-hound':['fx-fire-eruption'], wraith:['fx-radial-orb'], 'tempest-sentinel':['projectile-lightning','fx-ground-shockwave'],
  'stone-guardian':['fx-ground-shockwave','projectile-stone'],
};
const BOSS_FX = {
  warBeast:['fx-boss-sweep-slam','fx-ground-shockwave','fx-radial-orb','projectile-stone'],
  templeJudge:['fx-boss-sweep-slam','fx-ground-shockwave','fx-radial-orb','fx-holy-light-judgment','fx-fire-eruption','projectile-tablet','projectile-stone','projectile-lightning'],
  pitLord:['fx-boss-sweep-slam','fx-ground-shockwave','fx-radial-orb','fx-fire-eruption','fx-fire-meteor'],
  celestialTrial:['fx-boss-sweep-slam','fx-ground-shockwave','fx-radial-orb','fx-holy-light-judgment','fx-fire-eruption','fx-fire-meteor','projectile-lightning','projectile-tablet'],
};

export class AssetLoadError extends Error {
  constructor(ids, cause) {
    super(`Graphics could not load. Check your connection and retry. (${ids.join(', ')})`);
    this.name='AssetLoadError'; this.failedIDs=ids; this.cause=cause; this.retriable=true;
  }
}

/** Animation geometry and durations are exact references into the stage-one manifests. */
export function sampleFrame(meta, seconds=0, name=meta?.defaultAnimation) {
  if (!meta) return null;
  const clip=meta.animations?.[name] || meta.animations?.[meta.defaultAnimation];
  if (!clip?.frames.length) return meta.frames[0];
  let ms=Math.max(0,seconds*1000);
  if (clip.loop) ms%=clip.durationMs;
  else ms=Math.min(ms,Math.max(0,clip.durationMs-0.001));
  for (const [index,duration] of clip.frames) {
    if (ms<duration) return meta.frames[index];
    ms-=duration;
  }
  return meta.frames[clip.frames.at(-1)[0]];
}
export function namedFrame(meta, name) {
  const index=meta?.namedFrames?.[name];
  return index==null ? null : meta.frames[index];
}

export class AssetManager {
  constructor({baseURL=ASSET_BASE_URL, imageFactory=()=>new Image(), concurrency=4, timeoutMs=20000}={}) {
    this.baseURL=baseURL.replace(/\/$/,''); this.imageFactory=imageFactory;
    this.concurrency=concurrency; this.timeoutMs=timeoutMs;
    this.cache=new Map(); this.pending=new Map(); this.pinned=new Set(); this.levelIDs=new Set();
    this.errors=new Map(); this.queue=[]; this.active=0; this.biome=null; this.levelKey=null; this.revision=0; this.aborters=new Map(); this.cancelEpoch=0;
  }
  async initialize() { return this; }
  getURL(id) { const m=ASSET_CATALOG[id]; if(!m) throw new Error(`Unknown graphics asset: ${id}`); return `${this.baseURL}/${m.path}`; }
  get(id) { const entry=this.cache.get(id); if(entry) entry.lastUsed=Date.now(); return entry || null; }
  metadata(id) { return ASSET_CATALOG[id] || null; }
  has(id) { return this.cache.has(id); }
  planLevel(world) {
    const biome=biomeKey(world.biome || world.region);
    const ids=[`${biome}-background-sky`,`${biome}-background-far`,`${biome}-background-near`,`${biome}-terrain`,`${biome}-blocks`,`${biome}-structures`,'collectibles-atlas'];
    ids.push(...PLAYER_CORE.map(s=>`player.jesus.${s}`), ...CORE_FX.map(s=>`fx-${s}`));
    const pool=world.enemyPool || [...new Set((world.enemies || []).map(e=>e.spriteType || e.type))];
    for(const item of pool) {
      const type=typeof item==='string'?item:item.spriteType||item.type;
      ids.push(...ENEMY_STATES.map(s=>`enemy-${type}-${s}`),...(ENEMY_FX[type] || []));
    }
    const hazardTypes=new Set([...(world.hazardTypes||[]),...(world.hazards||[]).map(h=>h.type)]);
    // Shared route modules may introduce gusts and rockfall later, beyond resident chunks.
    for(const chunk of world.chunkPlan || []) {
      const tags=chunk.tags || [];
      if(tags.includes('wind')||tags.includes('chase'))hazardTypes.add('wind');
      if(tags.includes('rockfall'))hazardTypes.add('fallingRock');
      if(tags.includes('lightning'))hazardTypes.add('lightning');
    }
    // Mountain/desert/storm route families contain gusts even when the first chunks are calm.
    if(['mountains','desert','storm'].includes(biome))hazardTypes.add('wind');
    for(const h of hazardTypes) ids.push(HAZARD_ART[h] || h);
    // Water is also decorative; its ripple animation is shared by all six worlds.
    ids.push('hazard-water-surface','fx-water-splash');
    if(biome==='storm') ids.push('fx-weather-rain','hazard-lightning-strike','projectile-lightning');
    if(biome==='desert') ids.push('fx-weather-sandstorm');
    for(const [power,value] of Object.entries(world.playerPowers || world.powers || {})) if(value) ids.push(...(POWER_ART[power] || []));
    // Resident pickups may grant a new action immediately. Decode its action and
    // effects before play resumes, so collecting it never reveals a blank frame.
    for(const pickup of world.pickups || []) if(!pickup.collected) ids.push(...this.powerIDs(pickup.type));
    // Boss sheets are requested at the arena approach, not at the beginning of a long level.
    if(world.loadBossNow && world.bossId) ids.push(...this.bossIDs(world.bossId));
    const unique=[...new Set(ids)],unknown=unique.filter(id=>!ASSET_CATALOG[id]);
    if(unknown.length)throw new AssetLoadError(unknown,new Error('The level references an unknown asset.'));
    return unique;
  }
  async loadLevel(world, onProgress=()=>{}) {
    const nextBiome=biomeKey(world.biome || world.region);
    const levelKey=world.id || nextBiome;
    const ids=this.planLevel(world);
    for(const id of PLAYER_CORE.map(s=>`player.jesus.${s}`)) this.pinned.add(id);
    // Keep only the intersection with the next plan. A same-biome arena can reuse
    // its prefetched guardian while shedding the preceding normal enemy pool.
    // Different-biome textures are also released before any new downloads begin.
    this.levelIDs=new Set(ids);
    const keep=new Set([...this.pinned,...ids]);
    this._retainOnly(keep);
    this.biome=nextBiome; this.levelKey=levelKey;
    let loaded=0;
    onProgress({loaded,total:ids.length,ratio:0,label:'Preparing world graphics'});
    const results=await Promise.allSettled(ids.map(async id=>{
      const entry=await this._load(id);
      onProgress({loaded:++loaded,total:ids.length,ratio:loaded/ids.length,label:ASSET_CATALOG[id].kind});
      return entry;
    }));
    const failed=ids.filter((id,i)=>results[i].status==='rejected');
    if(failed.length) throw new AssetLoadError(failed,results.find(r=>r.status==='rejected').reason);
    this.revision++;
    return this.stats();
  }
  async ensure(ids, {pin=false}={}) {
    ids=Array.isArray(ids)?ids:[ids];
    for(const id of ids) (pin?this.pinned:this.levelIDs).add(id);
    const results=await Promise.allSettled(ids.map(id=>this._load(id)));
    const failed=ids.filter((id,i)=>results[i].status==='rejected');
    if(failed.length) throw new AssetLoadError(failed,results.find(r=>r.status==='rejected').reason);
    this.revision++;
    return results.map(r=>r.value);
  }
  prefetch(ids) { return this.ensure(ids); }
  bossIDs(id) {
    const ids=[...BOSS_STATES.map(s=>`enemy-${id}-${s}`),...(BOSS_FX[id]||[])];
    const unknown=ids.filter(id=>!ASSET_CATALOG[id]);
    if(unknown.length)throw new AssetLoadError(unknown,new Error('Unknown boss graphics.'));
    return ids;
  }
  prefetchBoss(id) { return this.ensure(this.bossIDs(id)); }
  powerIDs(power) { return [...(POWER_ART[power] || [])]; }
  prefetchPowers(powers) { return this.ensure([...new Set(powers.flatMap(power=>this.powerIDs(power)))]); }
  ensurePlayerPower(power) { return this.ensure(POWER_ART[power] || [],{pin:true}); }
  _retainOnly(keep) {
    const error=new Error('Previous-world graphics loading cancelled'); error.name='AbortError';
    const remaining=[];
    for(const task of this.queue) {
      if(keep.has(task.id))remaining.push(task);
      else {this.pending.delete(task.id);task.reject(error);}
    }
    this.queue=remaining;
    for(const [id,abort] of [...this.aborters]) if(!keep.has(id)) {abort();this.pending.delete(id);}
    for(const [id,entry] of this.cache) if(!keep.has(id)) {entry.image.src='';this.cache.delete(id);}
    for(const id of this.errors.keys()) if(!keep.has(id))this.errors.delete(id);
  }
  cancelLoads() {
    this.cancelEpoch++;
    const error=new Error('Graphics loading cancelled'); error.name='AbortError';
    for(const task of this.queue.splice(0)) {this.pending.delete(task.id);task.reject(error);}
    for(const abort of this.aborters.values()) abort();
    this.pending.clear();
    this.releaseLevel();
  }
  releaseLevel() {
    this.levelIDs.clear(); this.biome=null;this.levelKey=null;
    for(const [id,entry] of this.cache) if(!this.pinned.has(id)) {
      entry.image.src=''; this.cache.delete(id);
    }
    this.errors.clear(); this.revision++;
  }
  stats() {
    let decodedBytes=0,networkBytes=0;
    for(const {meta} of this.cache.values()) { decodedBytes+=meta.width*meta.height*4; networkBytes+=meta.bytes || 0; }
    return {images:this.cache.size,pending:this.pending.size,decodedBytes,networkBytes,biome:this.biome,failed:[...this.errors.keys()]};
  }
  _load(id) {
    if(this.cache.has(id)) return Promise.resolve(this.cache.get(id));
    if(this.pending.has(id)) return this.pending.get(id);
    if(!ASSET_CATALOG[id]) return Promise.reject(new Error(`Unknown graphics asset: ${id}`));
    let resolve,reject;
    const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});
    this.pending.set(id,promise);
    this.queue.push({id,resolve,reject,promise}); this._drain();
    return promise;
  }
  _drain() {
    while(this.active<this.concurrency && this.queue.length) {
      const task=this.queue.shift(); this.active++;
      this._download(task.id).then(entry=>{
        // A late completion from the previous world must not repopulate its released cache.
        if(this.levelIDs.has(task.id)||this.pinned.has(task.id)) this.cache.set(task.id,entry);
        this.errors.delete(task.id); task.resolve(entry);
      },err=>{if(this.levelIDs.has(task.id)||this.pinned.has(task.id))this.errors.set(task.id,err);task.reject(err);}).finally(()=>{
        if(this.pending.get(task.id)===task.promise)this.pending.delete(task.id);
        this.active--; this._drain();
      });
    }
  }
  async _download(id) {
    let lastError; const epoch=this.cancelEpoch;
    for(let attempt=0;attempt<2;attempt++) {
      try {
        const image=this.imageFactory(); image.decoding='async';
        const meta=ASSET_CATALOG[id];
        await new Promise((resolve,reject)=>{
          const clean=()=>{clearTimeout(timer);image.onload=image.onerror=null;this.aborters.delete(id);};
          const timer=setTimeout(()=>{clean();image.src='';reject(new Error(`Graphics request timed out: ${id}`));},this.timeoutMs);
          image.onload=()=>{clean();resolve();};
          image.onerror=()=>{clean();reject(new Error(`Graphics request failed: ${id}`));};
          this.aborters.set(id,()=>{clean();image.src='';const error=new Error('Graphics loading cancelled');error.name='AbortError';reject(error);});
          // R2 public images can be drawn without CORS. No fetch() or crossOrigin is needed.
          image.src=this.getURL(id);
        });
        if(image.naturalWidth!==meta.width || image.naturalHeight!==meta.height) throw new Error(`Uploaded graphics dimensions differ from the manifest: ${id}`);
        if(image.decode) await image.decode();
        return {image,meta,lastUsed:Date.now()};
      } catch(error) { lastError=error; if(error.name==='AbortError'||epoch!==this.cancelEpoch) throw error; }
    }
    throw lastError;
  }
}
