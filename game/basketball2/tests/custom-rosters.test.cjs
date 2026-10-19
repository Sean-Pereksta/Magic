const test=require('node:test');
const assert=require('node:assert/strict');
const Roster=require('../js/custom-rosters.js');
const Rise=require('../js/campaign/rise-to-glory.js');

const classic=Object.fromEntries(Array.from({length:10},(_,index)=>[`p${index}`,{name:`Classic ${index}`,archetype:'all_rounder',shot:1+index*.03,speed:1,passing:1}]));
function entries(){return Roster.catalog(classic,Rise,null);}
function ready(size=3){return Roster.fill({enabled:true,cap:250},size,entries());}

test('a 250 OVR cap is inclusive for both teams and counts every selected athlete',()=>{
  const pool=entries();pool.forEach((entry,index)=>entry.overall=[90,85,75,80,80,80,50,50,50,50][index]);
  const draft={enabled:true,cap:250,teams:[pool.slice(0,3).map(p=>p.id),pool.slice(3,6).map(p=>p.id)]};
  assert.deepEqual(Roster.validate(draft,3,pool),{ok:true,errors:[],totals:[250,240]});
  draft.cap=249;assert.match(Roster.validate(draft,3,pool).errors.join(' '),/Blue is 1 OVR over/);
  draft.cap=239;assert.equal(Roster.snapshot(draft,3,pool),null);
});

test('missing, unknown, duplicate, over-cap and malformed teams cannot start',()=>{
  const pool=entries();
  for(const mutate of [d=>d.teams[0].pop(),d=>d.teams[0]='invalid',d=>d.teams[1][2]='unknown',d=>d.teams[1][0]=d.teams[0][0],d=>d.cap=25,d=>d.cap=250.5,d=>d.cap=NaN,d=>d.cap=999]){
    const draft=ready();mutate(draft);assert.equal(Roster.validate(draft,3,pool).ok,false);assert.equal(Roster.snapshot(draft,3,pool),null);
  }
  assert.equal(Roster.validate(ready(),4,pool).ok,false);
});

test('automatic fill uses unique athletes and obeys both budgets at every supported team size',()=>{
  for(const size of [1,2,3]){
    const draft=ready(size),result=Roster.validate(draft,size,entries());
    assert.equal(result.ok,true);assert.equal(new Set(draft.teams.flatMap(ids=>ids.slice(0,size))).size,size*2);
    assert.ok(result.totals.every(total=>total<=250));
  }
  const impossible=Roster.fill({enabled:true,cap:25},3,entries());
  assert.equal(Roster.validate(impossible,3,entries()).ok,false);
  assert.deepEqual(impossible.teams,[['','',''],['','','']]);
});

test('joint fill leaves enough low OVR athletes for both teams and prefers both captains when feasible',()=>{
  const pool=[['classic:sky',70],['classic:flash',72],['classic:smooth',61],['classic:bruiser',64],['classic:claw',67],
    ['classic:rocket',68],['classic:crafty',70],['classic:tower',62],['classic:rimreaper',72],['classic:deeprange',62],
    ['rise:rtg-24-0',50],['rise:rtg-24-1',54],['rise:rtg-24-4',54]].map(([id,overall])=>({id,overall,profile:{name:id}}));
  for(const cap of [173,174,175,176,180,200,250]){
    const filled=Roster.fill({enabled:true,cap},3,pool,['classic:sky','classic:flash']);
    assert.equal(Roster.validate(filled,3,pool).ok,true,'two feasible rosters at cap '+cap);
    assert.deepEqual(filled,Roster.fill({enabled:true,cap},3,pool,['classic:sky','classic:flash']),'deterministic choices');
  }
  const preferred=Roster.fill({enabled:true,cap:200},3,pool,['classic:sky','classic:flash']);
  assert.equal(preferred.teams[0][0],'classic:sky');assert.equal(preferred.teams[1][0],'classic:flash');
  const core={enabled:true,cap:174,teams:[['classic:smooth','',''],['classic:deeprange','','']]};
  const preserved=Roster.fill(core,3,pool);assert.equal(Roster.validate(preserved,3,pool).ok,true);
  assert.equal(preserved.teams[0][0],'classic:smooth');assert.equal(preserved.teams[1][0],'classic:deeprange');
  const trapped={enabled:true,cap:174,teams:[['classic:sky','rise:rtg-24-0','rise:rtg-24-1'],['','','']]};
  assert.equal(Roster.validate(Roster.fill(trapped,3,pool,['classic:sky','classic:flash']),3,pool).ok,true,'fallback rebuilds an impossible preserved core');
});

test('Rise pool contains owned athletes with current ratings, skills, roles and real OVR',()=>{
  const state=Rise.create(),athlete=Rise.roster(state)[0];
  athlete.role='shooter';athlete.abilities={catchAndShoot:2,quickRelease:3};athlete.ratings.threePoint=87;
  athlete.overall=1; // A stale cached OVR is never trusted by the budget.
  const pool=Roster.catalog(classic,Rise,state),entry=pool.find(p=>p.id===`rise:${athlete.id}`);
  assert.equal(pool.length,10+state.roster.length);assert.equal(entry.overall,Rise.overall(athlete));
  assert.equal(entry.profile.ratings.threePoint,87);assert.equal(entry.profile.role,'shooter');
  assert.deepEqual(entry.profile.abilities,athlete.abilities);
  const before=entry.overall;for(const key of Rise.ATTRIBUTE_KEYS) athlete.ratings[key]=90;
  const upgraded=Roster.catalog(classic,Rise,state).find(p=>p.id===entry.id);
  assert.ok(upgraded.overall>before);assert.equal(upgraded.profile.ratings.threePoint,90);
  const draft=ready();draft.teams[0][0]=entry.id;
  assert.equal(Roster.snapshot(draft,3,pool).teams[0][0].abilities.quickRelease,3);
  state.roster=state.roster.filter(id=>id!==athlete.id);
  assert.equal(Roster.snapshot(draft,3,Roster.catalog(classic,Rise,state)),null,'released or replaced save identities must be reselected');
});

test('runtime snapshots preserve chosen identities and isolate exhibition changes from the franchise',()=>{
  const state=Rise.create(),pool=Roster.catalog(classic,Rise,state),athlete=Rise.roster(state)[0];
  const draft=ready();draft.teams[0][0]=`rise:${athlete.id}`;
  const before=JSON.stringify(state),snapshot=Roster.snapshot(draft,3,pool);
  assert.equal(snapshot.teams[0][0].campaignPlayerId,athlete.id);
  assert.equal(snapshot.teams[0][0].customOverall,Rise.overall(athlete));
  snapshot.teams[0][0].ratings.threePoint=99;snapshot.teams[0][0].abilities.quickRelease=3;
  assert.equal(JSON.stringify(state),before);
});

test('stored drafts keep IDs only and sanitize untrusted shape and cap',()=>{
  assert.deepEqual(Roster.normalize(null),{enabled:false,cap:250,teams:[['','',''],['','','']]});
  const value=Roster.normalize({enabled:true,cap:200,teams:[['classic:p1',7],['rise:r1']],ratings:{threePoint:99}});
  assert.deepEqual(value,{enabled:true,cap:200,teams:[['classic:p1','',''],['rise:r1','','']]});
  for(const cap of [-1,'abc',500]) assert.equal(Roster.normalize({cap}).cap,250);
  assert.ok(Roster.legacyOverall({shot:1.8})>Roster.legacyOverall({shot:.6}));
});
