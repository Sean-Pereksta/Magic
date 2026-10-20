import {createJourneySave,migrateJourney,JOURNEY_BY_ID} from "../world/journey.js";
import {cleanBoosts} from "../powers/boosts.js";
const JOURNEY_KEY="jesusRunnerJourneyV3",LEGACY_KEY="jesusRunJourneyV2",SCORE_KEY="jesusRunHighscores";
const POWERS=["shield","sandals","bread","holyLight","fireRain","wings"];
const BOSS_IDS=["warBeast","templeJudge","pitLord","celestialTrial"];
const record=value=>Boolean(value)&&typeof value==="object"&&!Array.isArray(value);
function bounded(value,fallback,min,max,integer=false){
  const numeric=typeof value==="number"||typeof value==="string"&&value.trim()!==""?Number(value):NaN;
  const result=Number.isFinite(numeric)?Math.max(min,Math.min(max,numeric)):fallback;
  return integer?Math.floor(result):result;
}
function checkpointStats(value){
  const raw=record(value)?value:{};
  return{
    time:bounded(raw.time,0,0,604800),
    collectibles:bounded(raw.collectibles,0,0,100000,true),
    secrets:bounded(raw.secrets,0,0,100000,true),
    damage:bounded(raw.damage,0,0,1000000,true),
    deaths:bounded(raw.deaths,0,0,1000000,true),
    totalSecrets:bounded(raw.totalSecrets,0,0,100000,true),
  };
}
function checkpointPlayer(value,challenge,permanentPowers){
  if(!record(value))return null;
  const rawPowers=record(value.powers)?value.powers:{};
  const powers={};
  for(const key of ["shield","sandals","bread","wings"])if(typeof rawPowers[key]==="boolean")powers[key]=rawPowers[key];
  const defaultCapacity=permanentPowers.includes("holyLight")?5:3;
  const capacity=bounded(value.holyLightCapacity,Math.max(defaultCapacity,bounded(rawPowers.holyLight,0,0,5,true)),3,5,true);
  if(rawPowers.holyLight!=null)powers.holyLight=bounded(rawPowers.holyLight,capacity,0,capacity,true);
  if(rawPowers.fireRain!=null)powers.fireRain=bounded(rawPowers.fireRain,0,0,3,true);
  return{powers,boosts:cleanBoosts(value.boosts),hearts:bounded(value.hearts,challenge?3:4,1,challenge?3:4,true),
    shieldUnlocked:value.shieldUnlocked===true||powers.shield===true||permanentPowers.includes("shield"),holyLightCapacity:capacity};
}
function checkpointWorld(value,checkpoint){
  if(!record(value))return null;
  if(value.version!=null&&value.version!==1&&value.version!==2)return null;
  if(value.seed!=null&&value.seed!==checkpoint.seed)return null;
  if(value.biome!=null&&value.biome!==checkpoint.nodeId)return null;
  // The generator builds Sets from these arrays. Reject a malformed snapshot
  // while retaining its valid checkpoint, rather than throwing during resume.
  for(const key of ["collected","dead","activated"])if(value[key]!=null&&!Array.isArray(value[key]))return null;
  const ids=key=>[...new Set((value[key]||[]).filter(id=>typeof id==="string"&&id.length>0&&id.length<=200))].slice(0,10000);
  const result={version:value.version===2?2:1,seed:checkpoint.seed,biome:checkpoint.nodeId,collected:ids("collected"),dead:ids("dead"),activated:ids("activated"),furthestX:bounded(value.furthestX,0,0,360000)};
  if(value.version===2){
    result.layoutVersion=[2,3].includes(value.layoutVersion)?value.layoutVersion:1;result.time=bounded(value.time,0,0,604800);
    result.dynamics=(Array.isArray(value.dynamics)?value.dynamics:[]).slice(0,2048).flatMap(entry=>{
      if(!Array.isArray(entry)||entry.length!==2||typeof entry[0]!=="string"||entry[0].length>200||!record(entry[1])||!record(entry[1].values))return[];
      const values={};
      for(const key of ["dead","shaking","triggered","active"])if(typeof entry[1].values[key]==="boolean")values[key]=entry[1].values[key];
      for(const key of ["collapseTimer","resetTimer"])if(entry[1].values[key]===null)values[key]=null;else if(Number.isFinite(entry[1].values[key]))values[key]=bounded(entry[1].values[key],0,-1,10);
      if(Number.isFinite(entry[1].values.x))values.x=bounded(entry[1].values.x,0,-1000,360000);
      return[[entry[0],{time:bounded(entry[1].time,result.time,0,result.time),values}]];
    });
  }
  return result;
}
function normalizeCheckpoint(value,permanentPowers){
  if(!record(value)||!JOURNEY_BY_ID[value.nodeId]||![value.x,value.y,value.seed].every(Number.isFinite)||value.x<0||value.x>360000||value.y< -700||value.y>1000||value.seed<1||value.seed>4294967295||!Number.isInteger(value.seed))return null;
  const checkpoint={nodeId:value.nodeId,x:value.x,y:value.y,seed:value.seed,boss:value.boss===true,
    bossId:BOSS_IDS.includes(value.bossId)?value.bossId:null,challenge:value.challenge===true,stats:checkpointStats(value.stats)};
  checkpoint.player=checkpointPlayer(value.player,checkpoint.challenge,permanentPowers);
  checkpoint.worldState=checkpointWorld(value.worldState,checkpoint);
  return checkpoint;
}
function get(key,fallback=null){try{return JSON.parse(localStorage.getItem(key)||"null")??fallback;}catch{return fallback;}}
function put(key,value){try{localStorage.setItem(key,JSON.stringify(value));return true;}catch{return false;}}
function scores(rows){return(Array.isArray(rows)?rows:[]).filter(r=>r&&Number.isFinite(Number(r.score))).map(r=>({name:String(r.name||"Anonymous").slice(0,20),score:Math.max(0,Math.floor(Number(r.score))),createdAt:Number(r.createdAt)||0})).sort((a,b)=>b.score-a.score||a.createdAt-b.createdAt).slice(0,10);}
export class GameStorage{
  loadJourney(){const raw=get(JOURNEY_KEY);if(!raw||raw.version!==3)return migrateJourney(get(LEGACY_KEY));const fresh=createJourneySave();const completed=(Array.isArray(raw.completed)?raw.completed:[]).filter(id=>JOURNEY_BY_ID[id]);const powers=(Array.isArray(raw.permanentPowers)?raw.permanentPowers:[]).filter(p=>POWERS.includes(p));const save={...fresh,...raw,completed,permanentPowers:powers,maxHearts:4,unlocked:Math.min(5,Math.max(0,Math.floor(Number(raw.unlocked)||0))),records:record(raw.records)?raw.records:{}};if(!JOURNEY_BY_ID[save.currentNode])save.currentNode="countryside";save.checkpoint=normalizeCheckpoint(raw.checkpoint,powers);return save;}
  saveJourney(save){return put(JOURNEY_KEY,{...save,version:3,updatedAt:Date.now()});}
  clearJourney(){const fresh=createJourneySave();this.saveJourney(fresh);return fresh;}
  endlessBest(save){return Math.max(bounded(save?.records?.endless?.bestDistance,0,0,Number.MAX_SAFE_INTEGER),(this.localScores()[0]?.score||0)/10);}
  updateEndlessRecord(save,run){
    save.records ||= {};
    const previousBest=this.endlessBest(save),distance=bounded(run.distance,0,0,Number.MAX_SAFE_INTEGER),old=record(save.records.endless)?save.records.endless:{};
    const result={previousBest,bestDistance:Math.max(previousBest,distance),newPersonalBest:distance>previousBest};
    save.records.endless={...old,bestDistance:result.bestDistance,runs:(Number(old.runs)||0)+1,
      bestRegions:Math.max(Number(old.bestRegions)||0,Number(run.regionsReached)||1),
      lastRun:{distance,regionsReached:run.regionsReached,enemiesDefeated:run.enemiesDefeated,collectibles:run.collectibles,time:run.time},updatedAt:Date.now()};
    this.saveJourney(save);return result;
  }
  localScores(){return scores(get(SCORE_KEY,[]));}
  async scores(){const local=this.localScores();try{const db=window.firebase?.apps?.length?window.firebase.firestore():null;if(!db)return local;const result=await db.collection("highscores").orderBy("score","desc").limit(10).get();return scores([...local,...result.docs.map(doc=>doc.data())]);}catch{return local;}}
  async saveScore(name,score){const row={name:String(name||"Traveler").slice(0,20),score:Math.floor(score),createdAt:Date.now()};put(SCORE_KEY,scores([row,...this.localScores()]));try{const db=window.firebase?.apps?.length?window.firebase.firestore():null;if(db){await db.collection("highscores").add(row);return"cloud";}}catch{}return"browser";}
  updateRecord(save,key,run){const old=save.records[key]||{};const bestTime=Number.isFinite(old.bestTime)?Math.min(old.bestTime,run.time):run.time;const record={bestTime,bestSecrets:Math.max(Number(old.bestSecrets)||0,run.secrets),noHit:Boolean(old.noHit||run.damage===0),allSecrets:Boolean(old.allSecrets||(run.totalSecrets>0&&run.secrets===run.totalSecrets)),targetTime:Boolean(old.targetTime||run.time<=run.targetTime),runs:(Number(old.runs)||0)+1};save.records[key]=record;return record;}
}
