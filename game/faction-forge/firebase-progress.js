/** Personal solo saves through Firebase Auth + Firestore. No HTTP service.
 * Client-played solo progress never writes competitive profiles or ratings. */
import {FORGE_COSTS,ProgressionError} from './progression.js';
import {getMission,createEndlessMission} from './campaign.js';
import {getForgePack,ROUND_WIN_CARDS,ROUND_WIN_DAILY_CAP,WIN_REWARDS} from './packs.js';

const clone=value=>JSON.parse(JSON.stringify(value));
const validId=value=>typeof value==='string'&&/^[A-Za-z0-9_-]{1,128}$/.test(value);
const fail=(ok,code,message)=>{if(!ok)throw new ProgressionError(code,message);};
export function newSoloProfile(uid,username,troops){
  return {schemaVersion:1,uid,username:String(username||'Commander').slice(0,80),forgeDust:0,
    troops:Object.fromEntries(Object.entries(troops).map(([key,troop])=>[key,{forgeTier:1,cards:0,unlocked:!troop.unlockable}])),
    campaign:{missions:{},endlessUnlocked:false,endlessBest:0},wins:0,losses:0,rewardTarget:null,
    rewardDay:'',rewardCounts:{},updatedAt:Date.now()};
}
export function soloMission(profile,mode,missionId){
  fail(['campaign','bot','endless'].includes(mode),'INVALID_MODE','Choose a solo mode.');
  if(mode==='bot')return null;
  if(mode==='endless'){
    fail(profile.campaign.endlessUnlocked,'MODE_LOCKED','Complete the campaign to unlock the Endless Crucible.');
    const stage=missionId?Number(String(missionId).replace(/^endless-/,'')):(profile.campaign.endlessBest||0)+1;
    fail(Number.isInteger(stage)&&stage>=1&&stage<=(profile.campaign.endlessBest||0)+1,'MISSION_LOCKED','Complete the preceding Crucible challenge first.');
    return createEndlessMission(stage);
  }
  const mission=getMission(missionId);
  fail(mission,'INVALID_MISSION','Unknown campaign mission.');
  const previous=mission.number>1?`forge-${String(mission.number-1).padStart(2,'0')}`:null;
  fail(!previous||profile.campaign.missions[previous]?.completed,'MISSION_LOCKED','Complete the previous mission first.');
  return mission;
}
export function settleSolo(profile,result){
  fail(validId(result.id),'INVALID_RESULT','Invalid solo match identifier.');
  const mission=soloMission(profile,result.mode,result.missionId),next=clone(profile),won=result.won===true;
  const receipt={id:result.id,uid:profile.uid,mode:result.mode,won,missionId:mission?.id||null,stars:0,cards:[],forgeDust:0,unlocks:[],completedAt:Date.now()};
  if(won)next.wins++;else next.losses++;
  const day=new Date().toISOString().slice(0,10);
  if(next.rewardDay!==day){next.rewardDay=day;next.rewardCounts={};}
  let kind=result.mode;
  if(won&&result.mode==='campaign'){
    const old=next.campaign.missions[mission.id]||{};kind=old.completed?'replay':mission.boss?'boss':'campaign';
    const lives=Math.max(0,Math.min(4,Number(result.lives)||0));receipt.stars=lives>=3?3:lives>=2?2:1;
    next.campaign.missions[mission.id]={completed:true,stars:Math.max(old.stars||0,receipt.stars),clears:(old.clears||0)+1};
    if(mission.unlockTroop&&next.troops[mission.unlockTroop]&&!next.troops[mission.unlockTroop].unlocked){next.troops[mission.unlockTroop].unlocked=true;receipt.unlocks.push(mission.unlockTroop);}
    if(mission.number===30)next.campaign.endlessUnlocked=true;
  }
  if(won&&result.mode==='endless')next.campaign.endlessBest=Math.max(next.campaign.endlessBest||0,mission.stage||mission.challengeLevel||1);
  const {cards,dust,cap}=WIN_REWARDS[kind];
  if(won&&(next.rewardCounts[kind]||0)<cap){
    const available=Object.keys(next.troops).filter(key=>next.troops[key].unlocked);
    const target=next.troops[next.rewardTarget]?.unlocked?next.rewardTarget:null;
    // Stable allocation makes an uncertain transaction retry repeat the same reward.
    let hash=2166136261;for(const char of result.id)hash=Math.imul(hash^char.charCodeAt(0),16777619)>>>0;
    const key=target||available[hash%available.length];
    if(key){next.troops[key].cards+=cards;receipt.cards.push({troopKey:key,quantity:cards});}
    next.forgeDust+=dust;receipt.forgeDust=dust;next.rewardCounts[kind]=(next.rewardCounts[kind]||0)+1;
  }
  next.updatedAt=Date.now();return {profile:next,receipt};
}
export function upgradeSolo(profile,troopKey){
  const unit=profile.troops[troopKey],cost=FORGE_COSTS[unit?.forgeTier];
  fail(unit?.unlocked,'TROOP_LOCKED','Unlock this troop first.');fail(cost,'MAX_TIER','This troop is already Elite.');
  fail(unit.cards>=cost.cards&&profile.forgeDust>=cost.dust,'INSUFFICIENT_RESOURCES','More matching cards and Forge Dust are required.');
  const next=clone(profile);next.forgeDust-=cost.dust;next.troops[troopKey].cards-=cost.cards;next.troops[troopKey].forgeTier++;next.updatedAt=Date.now();
  return {profile:next,transaction:{troopKey,fromTier:unit.forgeTier,toTier:unit.forgeTier+1,cardsSpent:cost.cards,dustSpent:cost.dust}};
}
export function buySoloPack(profile,packId){
  const item=getForgePack(packId);fail(item,'INVALID_PACK','Choose a card pack.');
  fail(!profile.activeSessionId,'MATCH_ACTIVE','Finish your current match before buying packs.');
  fail(item.cards.every(card=>profile.troops[card.troopKey]?.unlocked),'TROOP_LOCKED','Unlock the troops in this pack first.');
  fail(Number.isSafeInteger(profile.forgeDust)&&profile.forgeDust>=item.dust,'INSUFFICIENT_RESOURCES',`This pack requires ${item.dust} Forge Dust.`);
  const next=clone(profile);next.forgeDust-=item.dust;
  for(const card of item.cards)next.troops[card.troopKey].cards+=card.quantity;
  next.updatedAt=Date.now();return {profile:next,transaction:{packId:item.id,dustSpent:item.dust,cards:clone(item.cards)}};
}
export function settleSoloRoundWin(profile,result){
  fail(validId(result.id),'INVALID_RESULT','Invalid solo match identifier.');
  fail(Number.isInteger(result.round)&&result.round>=1&&result.round<=7,'INVALID_ROUND','Choose a completed round from this match.');
  soloMission(profile,result.mode,result.missionId);
  const next=clone(profile),day=new Date().toISOString().slice(0,10);
  if(next.rewardDay!==day){next.rewardDay=day;next.rewardCounts={};}
  const receipt={id:`${result.id}-round-${result.round}`,matchId:result.id,uid:profile.uid,mode:result.mode,missionId:result.missionId||null,round:result.round,cards:[],completedAt:Date.now()};
  if((next.rewardCounts.round||0)<ROUND_WIN_DAILY_CAP){
    const available=Object.keys(next.troops).filter(key=>next.troops[key].unlocked),target=next.troops[next.rewardTarget]?.unlocked?next.rewardTarget:null;
    let hash=2166136261;for(const char of receipt.id)hash=Math.imul(hash^char.charCodeAt(0),16777619)>>>0;
    const key=target||available[hash%available.length];
    if(key){next.troops[key].cards+=ROUND_WIN_CARDS;receipt.cards.push({troopKey:key,quantity:ROUND_WIN_CARDS});next.rewardCounts.round=(next.rewardCounts.round||0)+1;}
  }
  next.updatedAt=Date.now();return {profile:next,receipt};
}
export function createFirebaseProgress({auth,db,api,username,troops,storage=null,profilePath=null,queueNamespace='',isActiveMatch=()=>false}){
  try{if(storage===null)storage=globalThis.localStorage;}catch{}
  let currentProfile=null,disposed=false,subscription=null,status='Connecting to Firebase…';const listeners=new Set(),pending=new Map(),pendingRounds=new Map();
  const uid=auth.currentUser?.uid;
  fail(uid,'UNAUTHENTICATED','Sign in to save Faction Forge progress.');
  const accountUsername=String(username||'Commander').slice(0,80);
  profilePath=profilePath||['factionForgeSoloProfiles',uid];
  fail(Array.isArray(profilePath)&&profilePath[0]==='factionForgeSoloProfiles'&&profilePath[1]===uid&&(profilePath.length===2||profilePath.length===4&&profilePath[2]==='accounts'&&typeof profilePath[3]==='string'&&profilePath[3].length>0&&!profilePath[3].includes('/')),'INVALID_PROFILE','The save must belong to the signed-in account.');
  const profileRef=api.doc(db,...profilePath),queueKey='factionForge.solo.pending.'+uid+(queueNamespace?(queueNamespace.startsWith('.')?'':'.')+queueNamespace:''),roundQueueKey=queueKey+'.rounds';
  try{for(const result of JSON.parse(storage?.getItem(queueKey)||'[]'))if(validId(result.id))pending.set(result.id,result);}catch{}
  try{for(const result of JSON.parse(storage?.getItem(roundQueueKey)||'[]'))if(validId(result.id)&&Number.isInteger(result.round))pendingRounds.set(result.id+'-round-'+result.round,result);}catch{}
  const persistQueue=()=>{try{storage?.setItem(queueKey,JSON.stringify([...pending.values()]));}catch{}};
  const persistRounds=()=>{try{storage?.setItem(roundQueueKey,JSON.stringify([...pendingRounds.values()]));}catch{}};
  function assertIdentity(){fail(!disposed,'DISPOSED','The Firebase save connection has closed.');fail(auth.currentUser?.uid===uid,'ACCOUNT_CHANGED','Your account changed. Reopen the game to load its progress.');}
  function assertAccount(profile){fail(profile?.uid===uid&&profile.username===accountUsername,'ACCOUNT_CHANGED','Your account changed. Reopen the game to load its progress.');}
  function publish(profile){assertIdentity();assertAccount(profile);currentProfile=profile;status='Progress saved with Firebase';for(const listener of listeners)listener(profile);return profile;}
  function saveError(error){status=error.code==='permission-denied'?'Firebase save permissions need updating':'Firebase save unavailable · check your connection';return error;}
  async function transaction(callback){
    assertIdentity();try{return await api.runTransaction(db,async tx=>{assertIdentity();const guarded={get:async ref=>{const snap=await tx.get(ref);assertIdentity();return snap;},set:(...args)=>{assertIdentity();return tx.set(...args);}};const snap=await guarded.get(profileRef);const profile=snap.exists()?snap.data():newSoloProfile(uid,accountUsername,troops);assertAccount(profile);return callback(guarded,profile);});}catch(error){if(error instanceof ProgressionError)throw error;throw saveError(error);}
  }
  async function recordSoloResult(result){
    assertIdentity();fail(validId(result.id),'INVALID_RESULT','Invalid solo match identifier.');
    pending.set(result.id,clone(result));persistQueue();
    const ref=api.doc(db,...profilePath,'results',result.id);
    const response=await transaction(async(tx,profile)=>{
      const previous=await tx.get(ref);if(previous.exists())return {profile,receipt:previous.data()};
      const value=settleSolo(profile,result);tx.set(profileRef,value.profile);tx.set(ref,value.receipt);return value;
    });
    publish(response.profile);pending.delete(result.id);persistQueue();return response;
  }
  async function recordSoloRoundWin(result){
    assertIdentity();fail(validId(result.id),'INVALID_RESULT','Invalid solo match identifier.');
    fail(Number.isInteger(result.round)&&result.round>=1&&result.round<=7,'INVALID_ROUND','Choose a completed round from this match.');
    const id=result.id+'-round-'+result.round,ref=api.doc(db,...profilePath,'rounds',id);
    pendingRounds.set(id,clone(result));persistRounds();
    const response=await transaction(async(tx,profile)=>{const previous=await tx.get(ref);if(previous.exists()){const receipt=previous.data();fail(receipt.mode===result.mode&&receipt.missionId===(result.missionId||null),'REQUEST_CONFLICT','This round already belongs to a different match.');return {profile,receipt};}const value=settleSoloRoundWin(profile,result);tx.set(profileRef,value.profile);tx.set(ref,value.receipt);return value;});
    publish(response.profile);pendingRounds.delete(id);persistRounds();return response;
  }
  const upgradeRequests=new Map(),packRequests=new Map();
  return {
    get profile(){return currentProfile;},get configured(){return true;},get connectionStatus(){return status;},
    async loadProfile(){const profile=await transaction((tx,profile)=>{tx.set(profileRef,profile);return profile;});publish(profile);for(const result of [...pendingRounds.values()])await recordSoloRoundWin(result);for(const result of [...pending.values()])await recordSoloResult(result);return currentProfile;},
    subscribeProfile(listener){listeners.add(listener);if(currentProfile)listener(currentProfile);if(!subscription)subscription=api.onSnapshot(profileRef,snap=>{if(!disposed&&auth.currentUser?.uid===uid&&snap.exists())try{publish(snap.data());}catch(error){if(error.code==='ACCOUNT_CHANGED'){currentProfile=null;status='Account changed · reopen Faction Forge';for(const fn of listeners)fn(null);}else saveError(error);}},saveError);return ()=>listeners.delete(listener);},
    recordSoloResult,
    recordSoloRoundWin,
    async upgradeTroop(troopKey,{requestId}={}){
      // Keep one identifier across an uncertain retry, including a page reload.
      const key=queueKey+'.upgrade.'+troopKey;
      let retained;try{retained=storage?.getItem(key);}catch{}
      requestId=retained||upgradeRequests.get(key)||requestId||crypto.randomUUID();upgradeRequests.set(key,requestId);try{storage?.setItem(key,requestId);}catch{}
      fail(validId(requestId),'INVALID_REQUEST','Invalid upgrade request identifier.');
      const ref=api.doc(db,...profilePath,'upgrades',requestId);
      const response=await transaction(async(tx,profile)=>{const old=await tx.get(ref);if(old.exists()){fail(old.data().troopKey===troopKey,'REQUEST_CONFLICT','Use a new identifier for a different upgrade.');return {profile,transaction:old.data()};}const value=upgradeSolo(profile,troopKey);tx.set(profileRef,value.profile);tx.set(ref,{...value.transaction,uid,requestId,completedAt:Date.now()});return value;});
      publish(response.profile);upgradeRequests.delete(key);try{storage?.removeItem(key);}catch{}return response;
    },
    async buyPack(packId,{requestId}={}){
      assertIdentity();fail(!isActiveMatch(),'MATCH_ACTIVE','Finish your current match before buying packs.');
      const key=queueKey+'.pack.'+packId;let retained;try{retained=storage?.getItem(key);}catch{}
      requestId=retained||packRequests.get(key)||requestId||crypto.randomUUID();
      fail(validId(requestId),'INVALID_REQUEST','Invalid pack request identifier.');
      packRequests.set(key,requestId);try{storage?.setItem(key,requestId);}catch{}
      const clear=()=>{packRequests.delete(key);try{storage?.removeItem(key);}catch{}};
      try{
        const ref=api.doc(db,...profilePath,'purchases',requestId);
        const response=await transaction(async(tx,profile)=>{const previous=await tx.get(ref);if(previous.exists()){fail(previous.data().packId===packId,'REQUEST_CONFLICT','Use a new identifier for a different pack.');return {profile,transaction:previous.data()};}fail(!isActiveMatch(),'MATCH_ACTIVE','Finish your current match before buying packs.');const value=buySoloPack(profile,packId);tx.set(profileRef,value.profile);const receipt={...value.transaction,uid,requestId,completedAt:Date.now()};tx.set(ref,receipt);return {profile:value.profile,transaction:receipt};});
        publish(response.profile);clear();return response;
      }catch(error){if(['INVALID_PACK','MATCH_ACTIVE','TROOP_LOCKED','INSUFFICIENT_RESOURCES','REQUEST_CONFLICT','permission-denied','invalid-argument','failed-precondition'].includes(error.code))clear();throw error;}
    },
    async setRewardTarget(troopKey){const profile=await transaction((tx,profile)=>{fail(profile.troops[troopKey]?.unlocked,'TROOP_LOCKED','Choose an unlocked troop.');const next={...profile,rewardTarget:troopKey,updatedAt:Date.now()};tx.set(profileRef,next);return next;});publish(profile);return {profile};},
    dispose(){disposed=true;subscription?.();subscription=null;listeners.clear();}
  };
}

