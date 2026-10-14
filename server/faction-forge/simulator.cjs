'use strict';
// Runs the game's own checked-in functions in an isolated, deterministic clock.
// Client submissions never supply executable code, statistics, or outcomes.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createHash } = require('node:crypto');
const { pathToFileURL } = require('node:url');

const HTML_PATH = path.resolve(__dirname, '../../game/crowncouncil.html');
const DATA = ['R','M','DEFAULT_TROOP_VISUAL','TROOP_VISUALS','TROOPS','TROOP_IMAGE_VERSION','TROOP_IMAGE_PATHS','LEGENDARY_ABILITIES','TROOP_COUNTERS','COUNTER_ARCHETYPES','GUIDE_LINES','TROOP_ARCHETYPES','FACTIONS','ALL_FACTIONS'];
const FUNCTIONS = [
  'archetypesForKey','normalizeTag','evaluateDraftFit','rosterSummary','evaluateDraftCard',
  'manhattan','unitSize','rectDistance','nearestPointOnRectToUnit','armorTypeFor','unitStrongVs',
  'rectangleFree','canMoveTo','bandRows','spawnSlot','forgeUnitTier','spawnForgeIllusion','forgeCombatHooks','placeUnit','rebuildFieldFromRoster',
  'ownedTypes','weightedPick','unitEmoji','troopBodyClass','packFor','applyPickToState',
  'rosterAdd','rosterDoubleOneSpecific','rosterUpgradeOneSpecific','fxBeam','fxHeal',
  'fxBurst','fxTransport','fxImpact','fxStorm','fxStatus','pushSyncedFx',
  'auraDamageReduceFor','spawnAdjacentUnit','isBacklineUnit','frontBlocked','attemptViperLeap',
  'attemptBurrow','broodTick','reduceDamageForTarget','healScale','isSupportUnit','applyShockDebuff',
  'tilesOnLine','unitCenter','applyWardShields','nearestEnemy','strongestArmorTargetInRange',
  'targetPressure','chooseBalancedEnemy','pickTarget','nearestEnemyRangedPriority','manhattanUnits',
  'nearbyCrowdPenalty','stepToward','canStepNow','blinkIfNeeded','upsertTerrainTile','paintPoison',
  'addToxicSplash','addAcid','placeWard','applyTerrain','wardsHeal','wardsShoot','healAction',
  'wardAction','applyDamage','splash','tryKnock','chainLightning','attack','applyVguardRegen',
  'pruneBattleField','applySuddenDeath','resolveRoundLoser'
];
function extractFunction(source, name) {
  const re = new RegExp('^function ' + name + '\\([^\\n]*', 'm');
  const found = re.exec(source);
  if (!found) throw new Error('Missing shared combat function: '+name);
  const start = found.index;
  const lineEnd = source.indexOf('\n', start);
  if (found[0].includes('}')) return source.slice(start, lineEnd);
  const end = source.indexOf('\n}', start);
  if (end < 0) throw new Error('Unterminated shared function: '+name);
  return source.slice(start, end+2);
}
function extractData(source, name) {
  const start = source.search(new RegExp('^const '+name+'\\s*=', 'm'));
  if (start < 0) throw new Error('Missing combat data: '+name);
  const tail = source.slice(start);
  const next = /\n(?:const |let |function |\/\*|async function )/.exec(tail);
  return next ? tail.slice(0,next.index) : tail;
}
function rng(seed) {
  let state = parseInt(createHash('sha256').update(String(seed)).digest('hex').slice(0,8),16) >>> 0;
  return () => { state += 0x6D2B79F5; let t=state; t=Math.imul(t^(t>>>15),t|1); t^=t+Math.imul(t^(t>>>7),t|61); return ((t^(t>>>14))>>>0)/4294967296; };
}
const clone = value => JSON.parse(JSON.stringify(value));
let modulesPromise;
async function loadModules() {
  if (!modulesPromise) modulesPromise = Promise.all([
    import(pathToFileURL(path.resolve(__dirname,'../../game/faction-forge/units.js')).href),
    import(pathToFileURL(path.resolve(__dirname,'../../game/faction-forge/campaign.js')).href),
    import(pathToFileURL(path.resolve(__dirname,'../../game/faction-forge/drafting.js')).href),
    import(pathToFileURL(path.resolve(__dirname,'../../game/faction-forge/config.js')).href)
  ]).then(([units,campaign,drafting,config])=>({units,campaign,drafting,config}));
  return modulesPromise;
}
function createCombatAdapter({seed,modules,html=fs.readFileSync(HTML_PATH,'utf8')}) {
  let time=1000, serial=0;
  const random=rng(seed), math=Object.create(Math); math.random=random;
  class VirtualDate extends Date { static now() { return time; } }
  const context=vm.createContext({
    Math:math,Date:VirtualDate,crypto:{randomUUID:()=>`sim-${++serial}`},
    FFUnits:modules?.units || {}, FFCampaign:modules?.campaign || {},
    FFDraft:modules?.drafting || {}, FACTION_FORGE_CONFIG:modules?.config?.FACTION_FORGE_CONFIG || {},
    ...(modules?.campaign||{}),
    console:{warn(){},log(){}}, structuredClone:clone,
    ffProfile:null,ffRanked:false,
    // Presentation has no influence on seeded gameplay RNG.
    flashAt(){},emitLocalEffect(){},groundTickParticles(){},pulseCell(){},
    ffCombatEvent(){}, emitCombatEvent(){}, window:{},
  },{codeGeneration:{strings:false,wasm:false}});
  const setup=`let W=8,H=16,S=null,hostBattleStartedAt=1000,hostSuddenDeathAt=0,hostBattleTickId=0;
const PACE={tickMs:420,atkMult:1.85,moveSteps:1,poisonDotMs:900,poisonSlowMs:1050,toxicDotMs:1000};
const MOVE_MS=500,SUDDEN_DEATH_MS=60000,SUDDEN_DEATH_TICK_MS=1800;
const pKey=i=>i===0?'p0':'p1',inside=(x,y)=>x>=0&&x<W&&y>=0&&y<H;
const fieldEmpty=()=>({units:[],poison:[],toxic:[],acid:[],wards:[],syncedFx:[],startedAt:0,suddenDeathTickAt:0});
${DATA.map(name=>extractData(html,name)).join('\n')}
${FUNCTIONS.map(name=>extractFunction(html,name)).join('\n')}
globalThis.core={TROOPS,TROOP_COUNTERS,FACTIONS,LEGENDARY_ABILITIES,TROOP_VISUALS,TROOP_ARCHETYPES,packFor,applyPickToState,evaluateDraftCard,rebuildFieldFromRoster};`;
  vm.runInContext(setup,context,{timeout:1000});
  if (modules?.units?.installUnits) modules.units.installUnits(context.core);
  // New troops use the same hooks installed by the browser integration.
  context.__state=null;
  function run(source, timeout=5000) { return vm.runInContext(source,context,{timeout}); }
  return {
    version:['units.js','combos.js','evolutions.js','drafting.js','campaign.js','config.js'].reduce((hash,file)=>hash.update(fs.readFileSync(path.resolve(__dirname,'../../game/faction-forge',file))),createHash('sha256').update(setup)).digest('hex').slice(0,20),
    troops:clone(context.core.TROOPS),
    pack(state,side,unlocked) {
      context.__state=clone(state); context.__side=side;context.__unlocked=unlocked||[];
      return clone(run(`S=__state;(()=>{const faction=S.factions[__side],prior=FACTIONS[faction];FACTIONS[faction]=prior.filter(k=>__unlocked.includes(k));try{return packFor(S.roster[__side],faction)}finally{FACTIONS[faction]=prior}})()`));
    },
    pick(state,side,card) {
      context.__state=clone(state); context.__side=side;context.__card=clone(card);
      return clone(run('S=__state;applyPickToState(S,__side,__card);S'));
    },
    aiPick(pack,state,side,difficulty) {
      if (modules?.campaign?.chooseAIDraft) {
        const enemy=side==='p0'?'p1':'p0';
        return modules.campaign.chooseAIDraft(pack,{myRoster:state.roster[side],enemyRoster:state.roster[enemy],formation:state.formations[side],myTactics:state.tactics[side],myRelics:state.relics[side],enemyRelics:state.relics[enemy],evolutions:state.evolutions?.[side]||{},comboEnhancements:state.comboEnhancements?.[side]||[],enemyComboEnhancements:state.comboEnhancements?.[enemy]||[],aiStyle:state.aiStyle,name:state.aiName,difficulty},random,{evaluateDraftCard:context.core.evaluateDraftCard});
      }
      let best=0,score=-Infinity;
      pack.forEach((card,i)=>{const result=context.core.evaluateDraftCard(card,state.roster[side],state.roster[side==='p0'?'p1':'p0'],state.relics[side],[]);const next=typeof result==='number'?result:Number(result?.score)||0;if(next>score){best=i;score=next;}});
      return best;
    },
    battle(state,{forgeProfiles={},ranked=false,mission=null}={}) {
      context.__state=clone(state);context.__profiles=forgeProfiles;context.__ranked=ranked;context.__mission=mission;
      time=1000;
      run('S=__state;if(__mission&&__mission.id)S.missionId=__mission.id;S.ranked=__ranked;S.collections={p0:__profiles[0],p1:__profiles[1]};S.field=fieldEmpty();S.field.startedAt=1000;rebuildFieldFromRoster(S);');
      const frames=[];
      const capture=()=>{
        const field=context.__state.field;
        frames.push({at:time-1000,units:field.units.map(u=>{
          const keep=['id','p','type','x','y','hp','maxhp','dmg','range','lvl','battleRank','forgeTier','size','targetId','animUntil','animType','statusShockUntil','statusPoisonUntil','statusAcidUntil','woundUntil','illusion','startShield','summoned','revealedUntil','markedUntil','markedBySide','expiresAt','evolution','evolutionSignature','comboShield','comboShieldUntil','chainChargedUntil','bindUntil','suppressedUntil'];
          return Object.fromEntries(keep.filter(k=>u[k]!==undefined).map(k=>[k,u[k]]));
        }),poison:clone(field.poison),toxic:clone(field.toxic),acid:clone(field.acid),wards:clone(field.wards),syncedFx:clone(field.syncedFx),temporalFields:clone(field.temporalFields||[]),comboFields:clone(field.comboFields||[]),endlessPressure:clone(field.endlessPressure||null)});
      };
      capture();
      let lastLogic=0,winnerSide=null,health=[0,0];
      for (let tick=1;tick<=1100;tick++) {
        time=1000+tick*180;
        const logic=time-lastLogic>=420;
        if(logic)lastLogic=time;
        context.__now=time;context.__doLogic=logic;
        run(`(()=>{const st=S,units=st.field.units,now=__now,doLogic=__doLogic;
if(doLogic){pruneBattleField(st.field,now);applyTerrain(st.field,units);for(const u of units)blinkIfNeeded(u,units,st);wardsHeal(st,now);wardsShoot(st,now);broodTick(st,now);FFUnits.tickForgeAbilities(st,now,forgeCombatHooks(st));applySuddenDeath(st,now);}
for(let i=0;i<units.length&&i<192;i++){const u=units[i];if(!u||u.hp<=0)continue;if(doLogic){healAction(u,st,now);wardAction(u,st,now);applyVguardRegen(u,st,now);}const t=pickTarget(u,units,st.field);if(!t)continue;const attacked=attack(u,t,now,st);if(!attacked&&doLogic&&canStepNow(u,now)){const slowed=u.slowUntil&&u.slowUntil>now,bound=u.bindUntil&&u.bindUntil>now;const steps=bound?0:(slowed?1:PACE.moveSteps);for(let s=0;s<steps;s++){const t2=pickTarget(u,units,st.field);if(!t2)break;if(u.burrow)attemptBurrow(u,units,st,now);if(u.viperLeap&&isBacklineUnit(t2)&&frontBlocked(u,units)){if(attemptViperLeap(u,units,st)){if(rectDistance(u,t2)<=(u.range||1))attack(u,t2,now,st);continue;}}stepToward(u,t2,units);if(rectDistance(u,t2)<=(u.range||1)){attack(u,t2,now,st);break;}}}}
if(doLogic){for(const u of st.field.units.filter(u=>u.hp<=0)){FFUnits.noteForgeDeath(u,st,now,{distance:rectDistance});if(u.paints){paintPoison(st,u.x,u.y,7000,u.p);pushSyncedFx(st.field,fxBurst(u.x,u.y),now);}}st.field.units=st.field.units.filter(u=>u.hp>0);pruneBattleField(st.field,now);}return st.field;})()`);
        if (context.__state.field.units.length>192) throw new Error('Simulation summon cap exceeded');
        const nextHealth=[0,0];for(const u of context.__state.field.units)if(u.hp>0)nextHealth[u.p]+=u.hp;
        if(tick%2===0||nextHealth[0]===0||nextHealth[1]===0)capture();
        if(!nextHealth[0]||!nextHealth[1]) {
          winnerSide=nextHealth[0]>0?0:nextHealth[1]>0?1:health[0]===health[1]?tick%2:health[0]>health[1]?0:1;
          break;
        }
        health=nextHealth;
      }
      if(winnerSide===null) throw new Error('Simulation timed out; rewards refused');
      return clone({frames,durationMs:time-1000,clockOrigin:1000,winnerSide,survivingHealth:health});
    }
  };
}
module.exports={createCombatAdapter,loadModules,rng,extractFunction,extractData};
