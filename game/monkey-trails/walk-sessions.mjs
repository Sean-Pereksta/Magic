// A walking session is a recap boundary, never an expedition reset.
// No coordinates or route history are copied into these compact records.
const number=n=>Number.isFinite(n)?Math.max(0,n):0;
const counters=()=>({distance:0,discoveries:0,recruited:0,trainingXp:0,friendship:0,victories:0});
export function normalizeWalkSessions(s){
  const old=s.meta.walkSessions;
  if(!old||old.version!==1)s.meta.walkSessions={version:1,serial:0,active:null,last:null};
  const store=s.meta.walkSessions;store.serial=Math.max(0,Math.floor(number(store.serial)));
  if(store.active&&store.active.runId!==s.run?.id)store.active=null;
  return store;
}
export function beginWalkSession(s){
  if(!s.run||s.run.phase==='dead')throw Error('Begin a living expedition before starting a walk.');
  const store=normalizeWalkSessions(s);
  if(!store.active)store.active={id:`walk-${s.run.id}-${++store.serial}`,runId:s.run.id,startedAt:s.run.startedAt,...counters(),regions:{},regionalGoals:{}};
  return store.active;
}
export function recordWalkSession(s,progress={}){
  const session=beginWalkSession(s);
  for(const key of Object.keys(counters()))session[key]+=number(progress[key]);
  if(progress.landId){
    session.regionalGoals??={};const goals=session.regionalGoals[progress.landId]??={distance:0,discoveries:0,victories:0};
    for(const key of ['distance','discoveries','victories'])goals[key]+=number(progress[key]);
    if(progress.distance>0)session.regions[progress.landId]=number(session.regions[progress.landId])+progress.distance;
  }
  return session;
}
export function walkSessionPreview(s){
  const store=normalizeWalkSessions(s),session=store.active;
  return session?structuredClone(session):null;
}
export function finishWalkSession(s){
  const store=normalizeWalkSessions(s);
  if(!store.active)return store.last?structuredClone(store.last):null;
  // Clear only session bookkeeping. Party, pending encounter, supplies, distance,
  // phase, expedition identity, and regional progression are left untouched.
  store.last={...structuredClone(store.active),finishedAt:Date.now(),goal:structuredClone(s.meta.pinnedGoal||s.meta.regionChallenges?.tracked||null)};
  store.active=null;return structuredClone(store.last);
}
