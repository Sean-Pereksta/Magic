import test from 'node:test';
import assert from 'node:assert/strict';
import { FirebaseCampaign } from '../multiplayer-firebase.mjs';
import { houseIds } from '../multiplayer-rounds.mjs';

function fixture(){
  const campaign=new FirebaseCampaign({lobbyId:'test',username:'Ruler'});
  let deliver,unsubscribed=false;
  campaign.ref=()=>({});
  campaign.f={onSnapshot:(_ref,callback)=>{deliver=callback;return ()=>{unsubscribed=true;};}};
  return {campaign,receipt:(data,metadata={})=>deliver({exists:()=>true,data:()=>data,metadata}),unsubscribed:()=>unsubscribed};
}
for(const stateFirst of [false,true])test(`authoritative receipt waits for the applied view (state first: ${stateFirst})`,async()=>{
  const f=fixture(),{campaign}=f;let complete=false;
  const pending=campaign.waitForApplied('command').then(r=>{complete=true;return r;});
  if(stateFirst)campaign.lastVersion=4;
  f.receipt({status:'accepted',stateVersionApplied:4},{fromCache:true});
  await Promise.resolve();assert.equal(complete,false,'cached receipts cannot complete an action');
  f.receipt({status:'accepted',stateVersionApplied:4});
  if(!stateFirst){
    await Promise.resolve();assert.equal(complete,false,'receipt alone cannot advance the response queue');
    campaign.lastVersion=3;for(const waiter of campaign.appliedWaiters)waiter.check();
    await Promise.resolve();assert.equal(complete,false);
    campaign.lastVersion=4;for(const waiter of campaign.appliedWaiters)waiter.check();
  }
  assert.deepEqual(await pending,{ok:true});assert.equal(campaign.appliedWaiters.size,0);assert.equal(f.unsubscribed(),true);
});
test('rejected authoritative commands return the rejection and release the listener',async()=>{
  const f=fixture(),pending=f.campaign.waitForApplied('command');
  f.receipt({status:'rejected',error:'The terms changed.'});
  await assert.rejects(pending,/The terms changed/);assert.equal(f.campaign.appliedWaiters.size,0);assert.equal(f.unsubscribed(),true);
});

test('snapshots wake committed AI activations promptly and stop at the next human',async()=>{
  const {campaign}=fixture();
  campaign.uid='ruler';campaign.online=true;
  campaign.meta={phase:'resolving',lease:{uid:campaign.uid,token:campaign.token,expiresAt:Date.now()+30000}};
  let release,calls=0;
  const committed=new Promise(resolve=>{release=resolve;});
  campaign.advance=async()=>{
    calls++;
    if(calls===1){
      await campaign.pump(); // The new snapshot arrives while the commit is pending.
      await committed;
    }else {
      campaign.meta.phase='planning';campaign.meta.activeHouse='ashen';campaign.meta.ready={};campaign.meta.planningAt=Date.now();
      campaign.meta.seats=Object.fromEntries(houseIds.map(id=>[id,{kind:id==='ashen'?'human':'ai',uid:id==='ashen'?campaign.uid:null}]));
      campaign.presence={[campaign.uid]:{at:Date.now()}};
      campaign.state={sequential:{},tiles:{home:{id:'home',owner:'ashen',building:'city'}}};
      await campaign.pump(); // The committed snapshot hands control to a human.
    }
  };
  const pending=campaign.pump();
  await Promise.resolve();assert.equal(calls,1,'the next activation waits for the durable commit');
  release();await pending;
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(calls,2,'the next activation starts without the 1.5-second fallback interval');
  assert.equal(campaign.busy,false);
});
