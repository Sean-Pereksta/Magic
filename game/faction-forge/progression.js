/** Browser-side transport only. Currency/results/ownership remain server authority. */
export const FORGE_COSTS=Object.freeze({1:Object.freeze({cards:10,dust:100}),2:Object.freeze({cards:30,dust:350})});
export class ProgressionError extends Error {
  constructor(code,message,status=0){super(message);this.name='ProgressionError';this.code=code;this.status=status;}
}
export function createProgressionService({auth,db,apiBaseUrl='',fetchImpl=globalThis.fetch,firestoreApi=null,requestStorage=null}) {
  const configured=typeof apiBaseUrl==='string'&&apiBaseUrl.trim().length>0;
  let currentProfile=null,disposed=false,subscription=null,authSubscription=null,subscriptionUid=null;const listeners=new Set();
  try {if(requestStorage===null)requestStorage=globalThis.sessionStorage;}catch{requestStorage=null;}
  const pendingRequests=new Map(),replayCache=new Map();
  function retainedRequest(kind,provided) {const key='factionForge.request.'+(auth?.currentUser?.uid||'unknown')+'.'+kind;let id=pendingRequests.get(key);try{id=id||requestStorage?.getItem(key);}catch{}if(!id){id=provided||crypto.randomUUID();pendingRequests.set(key,id);try{requestStorage?.setItem(key,id);}catch{}}return id;}
  function clearRequest(kind,uid){const key='factionForge.request.'+uid+'.'+kind;pendingRequests.delete(key);try{requestStorage?.removeItem(key);}catch{}}
  async function mutation(kind,path,body,provided){const uid=auth?.currentUser?.uid||'unknown';body.requestId=retainedRequest(kind,provided);try{const data=await request(path,{method:'POST',body});clearRequest(kind,uid);return data;}catch(error){if(!['CONNECTION_FAILED','INVALID_RESPONSE','SERVICE_UNAVAILABLE','ACCOUNT_CHANGED','DISPOSED'].includes(error.code))clearRequest(kind,uid);throw error;}}
  const endpoint=apiBaseUrl.replace(/\/+$/,'');
  const publish=profile=>{currentProfile=profile;for(const fn of listeners)fn(profile);return profile;};
  function followIdentity(user){if(subscriptionUid===user?.uid)return;subscription?.();subscription=null;subscriptionUid=user?.uid||null;replayCache.clear();if(currentProfile?.uid!==user?.uid)publish(null);if(db&&firestoreApi?.onSnapshot&&user?.uid){const uid=user.uid;subscription=firestoreApi.onSnapshot(firestoreApi.doc(db,'factionForgeProfiles',uid),snap=>{if(!disposed&&auth.currentUser?.uid===uid&&snap.exists())publish({...snap.data(),upgradeCosts:currentProfile?.upgradeCosts||snap.data().upgradeCosts});},()=>{});}}
  async function request(path,{method='GET',body}={}) {
    if(disposed)throw new ProgressionError('DISPOSED','The progression connection has closed.');
    if(!configured)throw new ProgressionError('SERVICE_NOT_CONFIGURED','Cloud saves and rewards are available after the authoritative progression service is configured. Practice remains available.');
    if(!auth?.currentUser)throw new ProgressionError('UNAUTHENTICATED','Sign in to save your Faction Forge progress.');
    const uid=auth.currentUser.uid;
    const token=await auth.currentUser.getIdToken();
    const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),45000);
    let response;
    try {response=await fetchImpl(endpoint+path,{method,headers:{Authorization:'Bearer '+token,...(body?{'Content-Type':'application/json'}:{})},body:body?JSON.stringify(body):undefined,signal:controller.signal,credentials:'omit'});}catch(error){throw new ProgressionError('CONNECTION_FAILED',error.name==='AbortError'?'The server took too long. Retry safely.':'Could not reach the progression server. Your existing progress is safe.');}finally{clearTimeout(timer);}
    let data;try{data=await response.json();}catch{throw new ProgressionError('INVALID_RESPONSE','The progression server returned an invalid response.');}
    if(!response.ok)throw new ProgressionError(data.error?.code||'REQUEST_FAILED',data.error?.message||'Progress could not be validated.',response.status);
    if(auth.currentUser?.uid!==uid)throw new ProgressionError('ACCOUNT_CHANGED','Your account changed while this request was running. Refresh your profile.');
    if(data.session){const session=data.session,battle=session.battle;if(session.phase==='battle'&&battle){if(Array.isArray(battle.frames))replayCache.set(session.id,{replayId:battle.replayId,frames:battle.frames});else if(replayCache.get(session.id)?.replayId===battle.replayId)battle.frames=replayCache.get(session.id).frames;}else replayCache.delete(session.id);while(replayCache.size>3)replayCache.delete(replayCache.keys().next().value);}
    if(data.profile)publish(data.profile);return data;
  }
  return {
    get configured(){return configured;},get profile(){return currentProfile;},
    async loadProfile(){const data=await request('/profile');return publish(data.profile||data);},
    subscribeProfile(fn){listeners.add(fn);if(currentProfile)fn(currentProfile);
      if(!authSubscription&&typeof auth?.onAuthStateChanged==='function')authSubscription=auth.onAuthStateChanged(followIdentity);
      followIdentity(auth?.currentUser);return ()=>listeners.delete(fn);
    },
    startSolo(options){return request('/sessions/solo',{method:'POST',body:options});},
    joinPvp(options){return request('/sessions/pvp',{method:'POST',body:options});},
    getSession(id){const replay=replayCache.get(id);return request('/sessions/'+encodeURIComponent(id)+(replay?'?knownReplayId='+encodeURIComponent(replay.replayId):''));},
    pick(id,index,{sequence,requestId}={}){return mutation('pick.'+id+'.'+sequence+'.'+index,'/sessions/'+encodeURIComponent(id)+'/pick',{index,sequence},requestId);},
    upgradeTroop(troopKey,{requestId}={}){return mutation('upgrade.'+troopKey,'/upgrades',{troopKey},requestId);},
    getPendingPackRequest(packId){const key='factionForge.request.'+(auth?.currentUser?.uid||'unknown')+'.pack.'+packId;let id=pendingRequests.get(key);try{id=id||requestStorage?.getItem(key);}catch{}return id||null;},
    buyPack(packId,{requestId}={}){return mutation('pack.'+packId,'/packs',{packId},requestId);},
    setRewardTarget(troopKey){return request('/reward-target',{method:'POST',body:{troopKey}});},
    abandon(id){return request('/sessions/'+encodeURIComponent(id)+'/abandon',{method:'POST',body:{}});},
    dispose(){disposed=true;subscription?.();authSubscription?.();subscription=null;authSubscription=null;replayCache.clear();listeners.clear();}
  };
}
