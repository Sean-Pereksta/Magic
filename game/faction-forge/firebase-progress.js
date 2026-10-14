/** Personal solo saves through Firebase Auth + Firestore. No HTTP service.
 * Client-played solo progress never writes competitive profiles or ratings. */
import {FORGE_COSTS,ProgressionError} from './progression.js';
import {getMission,createEndlessMission} from './campaign.js';

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
  const rewards={campaign:[3,20,30],boss:[6,40,30],replay:[1,5,5],bot:[2,10,10],endless:[2,10,10]};
  const [cards,dust,cap]=rewards[kind];
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
export function createFirebaseProgress({auth,db,api,username,troops,storage=null}){
  try{if(storage===null)storage=globalThis.localStorage;}catch{}
  let currentProfile=null,disposed=false,subscription=null,status='Connecting to Firebase…';const listeners=new Set(),pending=new Map();
  const uid=auth.currentUser?.uid;
  fail(uid,'UNAUTHENTICATED','Sign in to save Faction Forge progress.');
  const profileRef=api.doc(db,'factionForgeSoloProfiles',uid),queueKey='factionForge.solo.pending.'+uid;
  try{for(const result of JSON.parse(storage?.getItem(queueKey)||'[]'))if(validId(result.id))pending.set(result.id,result);}catch{}
  const persistQueue=()=>{try{storage?.setItem(queueKey,JSON.stringify([...pending.values()]));}catch{}};
  function assertIdentity(){fail(!disposed,'DISPOSED','The Firebase save connection has closed.');fail(auth.currentUser?.uid===uid,'ACCOUNT_CHANGED','Your account changed. Reopen the game to load its progress.');}
  function publish(profile){assertIdentity();currentProfile=profile;status='Progress saved with Firebase';for(const listener of listeners)listener(profile);return profile;}
  function saveError(error){status=error.code==='permission-denied'?'Firebase save permissions need updating':'Firebase save unavailable · check your connection';return error;}
  async function transaction(callback){
    assertIdentity();try{return await api.runTransaction(db,async tx=>{assertIdentity();const snap=await tx.get(profileRef);const profile=snap.exists()?snap.data():newSoloProfile(uid,username,troops);return callback(tx,profile);});}catch(error){throw saveError(error);}
  }
  async function recordSoloResult(result){
    assertIdentity();fail(validId(result.id),'INVALID_RESULT','Invalid solo match identifier.');
    pending.set(result.id,clone(result));persistQueue();
    const ref=api.doc(db,'factionForgeSoloProfiles',uid,'results',result.id);
    const response=await transaction(async(tx,profile)=>{
      const previous=await tx.get(ref);if(previous.exists())return {profile,receipt:previous.data()};
      const value=settleSolo(profile,result);tx.set(profileRef,value.profile);tx.set(ref,value.receipt);return value;
    });
    publish(response.profile);pending.delete(result.id);persistQueue();return response;
  }
  const upgradeRequests=new Map();
  return {
    get profile(){return currentProfile;},get configured(){return true;},get connectionStatus(){return status;},
    async loadProfile(){const profile=await transaction((tx,profile)=>{tx.set(profileRef,profile);return profile;});publish(profile);for(const result of [...pending.values()])await recordSoloResult(result);return currentProfile;},
    subscribeProfile(listener){listeners.add(listener);if(currentProfile)listener(currentProfile);if(!subscription)subscription=api.onSnapshot(profileRef,snap=>{if(!disposed&&auth.currentUser?.uid===uid&&snap.exists())publish(snap.data());},saveError);return ()=>listeners.delete(listener);},
    recordSoloResult,
    async upgradeTroop(troopKey,{requestId}={}){
      // Keep one identifier across an uncertain retry, including a page reload.
      const key=queueKey+'.upgrade.'+troopKey;
      let retained;try{retained=storage?.getItem(key);}catch{}
      requestId=retained||upgradeRequests.get(key)||requestId||crypto.randomUUID();upgradeRequests.set(key,requestId);try{storage?.setItem(key,requestId);}catch{}
      fail(validId(requestId),'INVALID_REQUEST','Invalid upgrade request identifier.');
      const ref=api.doc(db,'factionForgeSoloProfiles',uid,'upgrades',requestId);
      const response=await transaction(async(tx,profile)=>{const old=await tx.get(ref);if(old.exists()){fail(old.data().troopKey===troopKey,'REQUEST_CONFLICT','Use a new identifier for a different upgrade.');return {profile,transaction:old.data()};}const value=upgradeSolo(profile,troopKey);tx.set(profileRef,value.profile);tx.set(ref,{...value.transaction,uid,requestId,completedAt:Date.now()});return value;});
      publish(response.profile);upgradeRequests.delete(key);try{storage?.removeItem(key);}catch{}return response;
    },
    async setRewardTarget(troopKey){const profile=await transaction((tx,profile)=>{fail(profile.troops[troopKey]?.unlocked,'TROOP_LOCKED','Choose an unlocked troop.');const next={...profile,rewardTarget:troopKey,updatedAt:Date.now()};tx.set(profileRef,next);return next;});publish(profile);return {profile};},
    dispose(){disposed=true;subscription?.();subscription=null;listeners.clear();}
  };
}

