import test from 'node:test';
import assert from 'node:assert/strict';
import {NotificationQueue,recordActivity,activityRows,markActivityRead,evolutionMoment,rememberReveal,pendingReveal,acknowledgeReveal,renderEvolutionReveal,renderRecruitReveal,renderActivity,recruitMoment,renderSavedReveal,saveFeedback,renderWelcomeChoices,welcomeHelp,tutorialHint,dismissTutorial,playMomentSound} from './presentation.mjs';
const monkey={id:'m-1',species:'capuchin',nickname:'Moss',types:['leaf'],stage:0,level:3,training:24};
const save=()=>({rng:90210,meta:{claims:{keep:true}},collection:{'m-1':structuredClone(monkey)},run:{phase:'trail',party:['m-1'],distance:0,fruit:18,essence:{leaf:8}},mode:'gps'});

test('notification priority, stable FIFO and discovery coalescing do not create reward state',()=>{
  const q=new NotificationQueue();q.push({id:'a',kind:'discovery',combineKey:'fruit',amount:1});q.push({id:'b',kind:'discovery',combineKey:'fruit',amount:2});q.push({id:'guardian',kind:'reward'});q.push({id:'ultra',kind:'ultra'});q.push({id:'equal',kind:'ultra'});
  assert.equal(q.entries().length,4);assert.equal(q.next({blocked:true}),null);assert.equal(q.next().id,'guardian');assert.equal(q.next(),null);q.finish();assert.equal(q.next().id,'ultra');assert.equal(q.push({id:'ultra',kind:'ultra'}),false);q.finish();assert.equal(q.next().id,'equal');q.finish();assert.equal(q.next().amount,3);q.finish();assert.equal(q.next(),null);
});
test('activity identities deduplicate replay and survive JSON reload without modifying claims',()=>{
  let s=save();recordActivity(s,{id:'claim:a',kind:'reward',title:'Temple reward',important:true,time:123});recordActivity(s,{id:'claim:a',kind:'reward',title:'Repeat',time:124});
  s=JSON.parse(JSON.stringify(s));assert.equal(activityRows(s).length,1);assert.equal(activityRows(s)[0].title,'Temple reward');assert.deepEqual(s.meta.claims,{keep:true});markActivityRead(s,'claim:a');assert.equal(activityRows(s)[0].read,true);
});
test('activity stays compact and prefers unread important records',()=>{
  const s=save();recordActivity(s,{id:'important',kind:'reward',title:'Guardian',important:true});for(let i=0;i<250;i++)recordActivity(s,{id:`find:${i}`,kind:'discovery',title:'fruit'});
  assert.equal(activityRows(s).length,160);assert(activityRows(s).some(row=>row.id==='important'));
});
test('evolution replay uses actual committed stats and cannot spend resources or combat randomness',()=>{
  const s=save(),before=structuredClone(monkey),after={...monkey,stage:1},stats=p=>({hp:70+p.stage*20,atk:14+p.stage*6,def:12+p.stage*3,speed:18+p.stage*2,maxEnergy:12+p.stage*2});
  const original=JSON.stringify(s),moment=evolutionMoment(before,after,{stats,formName:()=>'<Elder Moss>',unlocked:p=>p.stage?['strike','vine']:['strike']});
  assert.deepEqual(moment.stats.atk,{before:14,after:20});assert.deepEqual(moment.abilities,['vine']);assert.equal(JSON.stringify(s),original);
  assert.equal(rememberReveal(s,moment),true);assert.equal(rememberReveal(s,moment),false);const reloaded=JSON.parse(JSON.stringify(s));assert.equal(pendingReveal(reloaded).id,moment.id);
  const stateBeforeRender=JSON.stringify(reloaded),markup=renderEvolutionReveal(pendingReveal(reloaded));assert.match(markup,/&lt;Elder Moss&gt;/);assert.match(markup,/Maximum energy/);assert.match(markup,/Continue · skip animation/);assert.equal(JSON.stringify(reloaded),stateBeforeRender);assert.equal(reloaded.run.fruit,18);assert.equal(reloaded.run.essence.leaf,8);assert.equal(reloaded.rng,90210);
  assert.match(renderActivity(reloaded),/data-action="reveal-replay"/);acknowledgeReveal(reloaded,moment.id);assert.equal(pendingReveal(reloaded),null);
});
test('recruit presentation stores its committed destination and handles renamed content safely',()=>{
  const s=save(),p={...monkey,nickname:'<img src=x>',id:'m-2'};s.collection[p.id]=p;
  const moment=recruitMoment(s,p,{duplicate:true});assert.equal(moment.destination,'sanctuary');s.run.party.push('m-2');const markup=renderSavedReveal(s,moment);assert.match(markup,/Joined your Sanctuary/);assert.match(markup,/&lt;img src=x&gt;/);assert.match(markup,/1 \/ 90 species/);assert.doesNotMatch(markup,/<img src=x>/);assert.equal(moment.duplicate,true);
  assert.match(renderRecruitReveal(s,p),/Joined your active team/);
});
test('save labels retain failed-device and recovery information',()=>{
  assert.equal(saveFeedback({kind:'saved',text:'Saved on this device and in Firestore.'}),'Cloud synced');assert.match(saveFeedback({kind:'local'}),/waiting to sync/);assert.equal(saveFeedback({kind:'conflict',text:'Export a backup or load the cloud copy.'}),'Export a backup or load the cloud copy.');assert.equal(saveFeedback({kind:'offline',text:'Not saved locally · cloud unavailable. Retry sync.'}),'Not saved locally · cloud unavailable. Retry sync.');assert.equal(saveFeedback({kind:'saved',text:'Cloud saved · device storage unavailable.'}),'Cloud saved · device storage unavailable.');
});
test('welcome choices retain credential rules and contextual tips can be dismissed',()=>{
  assert.match(renderWelcomeChoices(),/New Adventure/);assert.match(renderWelcomeChoices(),/Continue/);assert.match(welcomeHelp('continue'),/There is no password reset/);
  const s=save();assert.equal(tutorialHint(s).id,'first-discovery');dismissTutorial(s,'first-discovery');dismissTutorial(s,'first-discovery');assert.equal(s.meta.presentation.tutorial.length,1);s.run.pendingEncounter={id:'waiting'};assert.equal(tutorialHint(s).id,'battle-ready');
});
test('muted sound never initializes audio',()=>{let calls=0;class Context{constructor(){calls++;}}assert.equal(playMomentSound('ultra',{sound:false},{AudioContext:Context}),false);assert.equal(calls,0);assert.equal(playMomentSound('recruit',{sound:true},{}),false);});
