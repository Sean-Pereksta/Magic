import test from 'node:test';
import assert from 'node:assert/strict';
import { FirebaseCampaign } from '../multiplayer-firebase.mjs';

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
