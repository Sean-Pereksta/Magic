import test from 'node:test';
import assert from 'node:assert/strict';
import {newSoloProfile,settleSolo,soloMission,upgradeSolo,createFirebaseProgress} from '../firebase-progress.js';
const troops={rifleman:{},medic:{},signalhawk:{unlockable:true},chronoweaver:{unlockable:true},siphonleech:{unlockable:true},railbreaker:{unlockable:true},miragewarden:{unlockable:true},hiveherald:{unlockable:true}};
const storage=()=>{const map=new Map();return {getItem:key=>map.get(key)||null,setItem:(key,value)=>map.set(key,value),removeItem:key=>map.delete(key)};};
function firebase(){
 const docs=new Map();let serial=Promise.resolve(),failAfterCommit=false;
 const api={doc:(_db,...parts)=>parts.join('/'),onSnapshot:()=>()=>{},runTransaction:async(_db,fn)=>{
  const operation=serial.then(async()=>{const writes=[];const value=await fn({get:async ref=>({exists:()=>docs.has(ref),data:()=>structuredClone(docs.get(ref))}),set:(ref,data)=>writes.push([ref,structuredClone(data)])});for(const [ref,data] of writes)docs.set(ref,data);if(failAfterCommit){failAfterCommit=false;throw Object.assign(Error('Disconnected'),{code:'unavailable'});}return value;});serial=operation.catch(()=>{});return operation;
 }};
 return {api,docs,uncertain:()=>{failAfterCommit=true;}};
}
const result=(id='win')=>({id,mode:'campaign',missionId:'forge-01',won:true,lives:4});
function service(store,opts={}){return createFirebaseProgress({auth:{currentUser:{uid:'owner'}},db:{},api:store.api,username:'Commander',troops,storage:storage(),...opts});}

test('Firebase alone creates and reloads the same UID profile without any HTTP endpoint',async()=>{
 const store=firebase(),one=service(store);assert.equal(one.configured,true);const profile=await one.loadProfile();assert.equal(profile.uid,'owner');assert.equal(profile.troops.signalhawk.unlocked,false);assert.equal(one.connectionStatus,'Progress saved with Firebase');
 await one.recordSoloResult(result());one.dispose();const two=service(store);assert.equal((await two.loadProfile()).campaign.missions['forge-01'].stars,3);assert.ok([...store.docs.keys()].every(key=>key.startsWith('factionForgeSoloProfiles/owner')));
});
test('campaign wins unlock the next mission and award cards and dust once despite concurrent retries',async()=>{
 const store=firebase(),one=service(store);await one.loadProfile();await Promise.all([one.recordSoloResult(result()),one.recordSoloResult(result())]);assert.equal(one.profile.forgeDust,20);assert.equal(one.profile.wins,1);assert.equal(Object.values(one.profile.troops).reduce((n,t)=>n+t.cards,0),3);assert.equal(soloMission(one.profile,'campaign','forge-02').id,'forge-02');
});
test('unacknowledged rewards survive reload and recover without a duplicate grant',async()=>{
 const store=firebase(),cache=storage(),one=service(store,{storage:cache});await one.loadProfile();store.uncertain();await assert.rejects(one.recordSoloResult(result()),{code:'unavailable'});one.dispose();
 const two=service(store,{storage:cache});await two.loadProfile();assert.equal(two.profile.forgeDust,20);assert.equal(two.profile.wins,1);assert.deepEqual(JSON.parse(cache.getItem('factionForge.solo.pending.owner')),[]);
});
test('upgrade transaction deducts its cost once and rejects insufficient resources or a conflicting retry',async()=>{
 const store=firebase(),one=service(store);await one.loadProfile();let p=store.docs.get('factionForgeSoloProfiles/owner');p.forgeDust=100;p.troops.rifleman.cards=10;
 await Promise.all([one.upgradeTroop('rifleman',{requestId:'upgrade-1'}),one.upgradeTroop('rifleman',{requestId:'upgrade-1'})]);assert.equal(one.profile.forgeDust,0);assert.equal(one.profile.troops.rifleman.forgeTier,2);
 await assert.rejects(one.upgradeTroop('medic',{requestId:'upgrade-1'}),{code:'REQUEST_CONFLICT'});await assert.rejects(one.upgradeTroop('rifleman',{requestId:'upgrade-2'}),{code:'INSUFFICIENT_RESOURCES'});
});
test('uncertain upgrade retains its request ID across reload without spending twice',async()=>{
 const store=firebase(),cache=storage(),one=service(store,{storage:cache});await one.loadProfile();const p=store.docs.get('factionForgeSoloProfiles/owner');p.forgeDust=500;p.troops.rifleman.cards=50;
 store.uncertain();await assert.rejects(one.upgradeTroop('rifleman',{requestId:'confirmed-upgrade'}),{code:'unavailable'});one.dispose();const two=service(store,{storage:cache});await two.loadProfile();await two.upgradeTroop('rifleman',{requestId:'new-dialog-after-reload'});assert.equal(two.profile.forgeDust,400);assert.equal(two.profile.troops.rifleman.forgeTier,2);assert.equal(two.profile.troops.rifleman.cards,40);
});
test('all campaign milestones and Endless progression work with solo saves',()=>{
 let p=newSoloProfile('owner','Commander',troops);assert.throws(()=>soloMission(p,'campaign','forge-02'),{code:'MISSION_LOCKED'});assert.throws(()=>soloMission(p,'endless'),{code:'MODE_LOCKED'});
 for(let n=1;n<=30;n++){p=settleSolo(p,{...result('win-'+n),missionId:'forge-'+String(n).padStart(2,'0')}).profile;}
 assert.equal(p.campaign.endlessUnlocked,true);for(const [key,value] of Object.entries(troops))if(value.unlockable)assert.equal(p.troops[key].unlocked,true);
 p=settleSolo(p,{id:'endless-1-win',mode:'endless',missionId:'endless-1',won:true,lives:2}).profile;assert.equal(p.campaign.endlessBest,1);assert.equal(soloMission(p,'endless').stage,2);
});
test('losses award nothing; replay and bot reward caps preserve progress without competitive fields',()=>{
 let p=newSoloProfile('owner','Commander',troops);p=settleSolo(p,{...result('loss'),won:false}).profile;assert.equal(p.forgeDust,0);assert.equal(p.campaign.missions['forge-01'],undefined);
 p=settleSolo(p,result()).profile;for(let i=0;i<7;i++)p=settleSolo(p,result('replay-'+i)).profile;assert.equal(p.forgeDust,45);
 for(let i=0;i<12;i++)p=settleSolo(p,{id:'bot-'+i,mode:'bot',won:true}).profile;assert.equal(p.forgeDust,145);assert.equal(p.crowncouncilElo,undefined);assert.equal(p.activeSessionId,undefined);
});
test('reward targets affect the next grant and are limited to unlocked troops',async()=>{
 const one=service(firebase());await one.loadProfile();await assert.rejects(one.setRewardTarget('signalhawk'),{code:'TROOP_LOCKED'});await one.setRewardTarget('medic');await one.recordSoloResult(result());assert.equal(one.profile.troops.medic.cards,3);
});
test('a changed or disposed identity cannot write the previous UID profile',async()=>{
 const auth={currentUser:{uid:'owner'}},one=service(firebase(),{auth});await one.loadProfile();auth.currentUser={uid:'other'};await assert.rejects(one.recordSoloResult(result()),{code:'ACCOUNT_CHANGED'});one.dispose();await assert.rejects(one.loadProfile(),{code:'DISPOSED'});
});
test('permission errors accurately describe the required Firebase rules update',async()=>{
 const one=service({api:{doc:()=>'',runTransaction:async()=>{throw Object.assign(Error('Denied'),{code:'permission-denied'});}}});await assert.rejects(one.loadProfile(),{code:'permission-denied'});assert.equal(one.connectionStatus,'Firebase save permissions need updating');assert.equal(one.profile,null);
});

