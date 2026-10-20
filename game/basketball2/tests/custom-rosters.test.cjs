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

function seeded(seed){
  return ()=>{seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed/4294967296;};
}
function poolOf(ratings){return ratings.map((overall,index)=>({id:`fixture:${index}`,overall,profile:{name:`Fixture ${index}`}}));}
function assertLineups(draft,size,pool){
  const checked=Roster.validate(draft,size,pool);
  assert.equal(checked.ok,true,checked.errors.join(' '));
  assert.equal(new Set(draft.teams.flatMap(team=>team.slice(0,size))).size,2*size);
  return checked.totals;
}

test('Quick Play has all nine roles, four real OVR tiers and three stable variants without changing the legacy catalog',()=>{
  const before=JSON.stringify(classic),pool=Roster.quickPlayCatalog(classic,Rise);
  assert.deepEqual(Roster.QUICK_PLAY_TIERS,[60,70,80,90]);
  assert.equal(Roster.QUICK_PLAY_ROLES.length,9);
  assert.equal(pool.length,108);assert.equal(new Set(pool.map(entry=>entry.id)).size,108);
  assert.equal(new Set(pool.map(entry=>entry.name)).size,108,'tiers use distinct people, not four copies of one athlete');
  assert.deepEqual(pool,Roster.quickPlayCatalog(classic,Rise));
  assert.equal(JSON.stringify(classic),before);assert.equal(entries().length,10);
  for(const role of Roster.QUICK_PLAY_ROLES)for(const tier of Roster.QUICK_PLAY_TIERS){
    const variants=pool.filter(entry=>entry.role===role.id&&entry.tier===tier);
    assert.deepEqual(variants.map(entry=>entry.variant),[1,2,3]);
    assert.equal(new Set(variants.map(entry=>JSON.stringify(entry.profile.ratings))).size,3);
    for(const entry of variants){
      assert.equal(entry.id,`quick:${role.id}:${tier}:${entry.variant}`);
      assert.equal(entry.roleLabel,role.label);assert.equal(entry.archetype,role.id);
      assert.equal(entry.overall,tier);
      assert.equal(Rise.overall({archetype:role.id,ratings:entry.profile.ratings}),tier,'display OVR comes from live ratings');
      assert.equal(entry.profile.campaignArchetype,role.id);assert.equal(entry.profile.campaignPlayerId,undefined);
      assert.equal(entry.profile.quickPlayId,entry.id);assert.equal(entry.profile.role,role.runtimeRole);
      assert.deepEqual(Object.keys(entry.profile.ratings).sort(),Rise.ATTRIBUTE_KEYS.slice().sort());
      assert.ok(Object.values(entry.profile.ratings).every(value=>Number.isInteger(value)&&value>=25&&value<=99));
      const definition=Rise.ARCHETYPES[role.id],average=keys=>keys.reduce((sum,key)=>sum+entry.profile.ratings[key],0)/keys.length;
      assert.ok(average(definition.strengths)>average(definition.weaknesses),'specialist skill shape survives calibration');
      for(const [key,rank] of Object.entries(entry.profile.abilities)){
        const ability=Rise.ABILITIES[key];
        assert.ok(rank>=1&&rank<=3);
        assert.ok(Object.entries(ability.requirements).every(([stat,value])=>entry.profile.ratings[stat]>=value));
        if(ability.requires)assert.ok(entry.profile.abilities[ability.requires]);
        if(ability.requiresAny)assert.ok(ability.requiresAny.some(required=>entry.profile.abilities[required]));
      }
    }
  }
  assert.ok(new Set(pool.map(entry=>entry.profile.skin)).size>=4,'appearance varies across the roster');
  assert.ok(new Set(pool.map(entry=>entry.profile.height)).size>=9,'players use their real physical profiles');
  const low=pool.find(entry=>entry.id==='quick:sharpshooter:60:1'),high=pool.find(entry=>entry.id==='quick:sharpshooter:90:1');
  assert.ok(high.profile.shot>low.profile.shot,'live shooting modifier improves with actual OVR');
  assert.ok(high.profile.ratings.threePoint>low.profile.ratings.threePoint);
});

test('random teams hit both exact budgets at every size, are reproducible, and vary across seeds',()=>{
  const pool=[...entries(),...Roster.quickPlayCatalog(classic,Rise)],before=JSON.stringify(pool);
  for(const [size,cap] of [[1,60],[1,70],[1,80],[1,90],[2,120],[2,140],[2,160],[2,180],[3,180],[3,210],[3,240],[3,250],[3,270]]){
    const lineups=new Set();
    for(let seed=1;seed<=8;seed++){
      const raw={enabled:true,cap,teams:[['obsolete','',''],['obsolete','','']]},rawBefore=JSON.stringify(raw);
      const draft=Roster.rollRandom(raw,size,pool,seeded(seed));
      assert.deepEqual(assertLineups(draft,size,pool),[cap,cap]);
      assert.deepEqual(draft,Roster.rollRandom(raw,size,pool,seeded(seed)),'injected RNG completely determines the roll');
      assert.equal(JSON.stringify(raw),rawBefore);
      lineups.add(JSON.stringify(draft.teams));
    }
    assert.ok(lineups.size>=6,`roster variety for ${size}v${size}, cap ${cap}`);
  }
  assert.equal(JSON.stringify(pool),before);
});

test('joint random solving includes arbitrary integer ratings and does not consume the other team’s only exact lineup',()=>{
  const extras=poolOf([61,61,51,51,41,41]),pool=[...Roster.quickPlayCatalog(classic,Rise),...extras];
  for(const [size,cap] of [[1,61],[2,141],[3,221],[3,241],[3,231]]){
    const draft=Roster.rollRandom({enabled:true,cap},size,pool,seeded(42));
    assert.deepEqual(assertLineups(draft,size,pool),[cap,cap]);
  }
  const narrow=poolOf([50,30,20,60,25,15]);
  for(let seed=1;seed<=8;seed++){
    const draft=Roster.rollRandom({enabled:true,cap:100},3,narrow,seeded(seed));
    assert.deepEqual(assertLineups(draft,3,narrow),[100,100]);
  }
});

test('unreachable caps use the highest joint total with a balanced tie-break, and impossible rosters return validation errors',()=>{
  const quick=Roster.quickPlayCatalog(classic,Rise);
  for(const [size,cap,expected] of [[1,95,90],[2,165,160],[3,251,250],[3,297,270]]){
    const draft=Roster.rollRandom({enabled:true,cap},size,quick,seeded(1));
    assert.deepEqual(assertLineups(draft,size,quick),[expected,expected]);
  }
  const tied=poolOf([50,50,40,40]);
  assert.deepEqual(assertLineups(Roster.rollRandom({cap:100},2,tied,seeded(7)),2,tied),[90,90]);
  for(const [size,cap,pool] of [[3,25,quick],[3,250,poolOf([60,60,60,60,60])],[2,100,poolOf([50,50,70,70])]]){
    const draft=Roster.rollRandom({enabled:true,cap,teams:[['old'],['old']]},size,pool,seeded(1));
    assert.equal(Roster.validate(draft,size,pool).ok,false);
    assert.equal(Roster.snapshot(draft,size,pool),null);
    assert.deepEqual(draft.teams,[['','',''],['','','']]);
  }
  assert.equal(Roster.validate(Roster.rollRandom({cap:250},4,quick,seeded(1)),4,quick).ok,false);
});

test('joint DP matches exhaustive optimal rosters on small independent fixtures',()=>{
  // The slow oracle is deliberately restricted to eight athletes. Production
  // uses bounded sum/count states, never this combination cross-product.
  function optimum(pool,size,cap){
    const lineups=[];
    function choose(start,ids,sum){
      if(ids.length===size){if(sum<=cap)lineups.push({ids,sum});return;}
      for(let index=start;index<pool.length;index++)choose(index+1,[...ids,index],sum+pool[index].overall);
    }
    choose(0,[],0);
    let best=null;
    for(const blue of lineups)for(const coral of lineups){
      if(blue.ids.some(id=>coral.ids.includes(id)))continue;
      const score=[blue.sum+coral.sum,Math.min(blue.sum,coral.sum)];
      if(!best||score[0]>best[0]||score[0]===best[0]&&score[1]>best[1])best=score;
    }
    return best;
  }
  for(let seed=1;seed<=12;seed++){
    const rng=seeded(seed),pool=poolOf(Array.from({length:8},()=>8+Math.floor(rng()*22)));
    for(const size of [1,2,3]){
      const cap=25+seed*3,best=optimum(pool,size,cap),draft=Roster.rollRandom({cap},size,pool,seeded(seed+99));
      if(!best)assert.equal(Roster.validate(draft,size,pool).ok,false);
      else{
        const totals=assertLineups(draft,size,pool);
        assert.deepEqual([totals[0]+totals[1],Math.min(...totals)],best,`seed ${seed}, size ${size}`);
      }
    }
  }
});

test('large-pool fill preserves selected identities and captains and random fill remains bounded with 128 entries',()=>{
  const pool=[...Roster.quickPlayCatalog(classic,Rise),...poolOf(Array.from({length:20},(_,index)=>40+index))];
  assert.equal(pool.length,128);
  const initial={enabled:true,cap:250,teams:[['quick:playmaker:80:1','',''],['quick:rimProtector:90:3','','']]};
  const filled=Roster.fill(initial,3,pool,['quick:playmaker:80:1','quick:rimProtector:90:3']);
  assertLineups(filled,3,pool);assert.equal(filled.teams[0][0],initial.teams[0][0]);assert.equal(filled.teams[1][0],initial.teams[1][0]);
  assert.deepEqual(Roster.fill(filled,3,pool),filled);
  const preferred=Roster.fill({enabled:true,cap:250},3,pool,['quick:slasher:90:2','quick:sharpshooter:90:1']);
  assertLineups(preferred,3,pool);assert.equal(preferred.teams[0][0],'quick:slasher:90:2');assert.equal(preferred.teams[1][0],'quick:sharpshooter:90:1');
  for(const cap of [180,200,211,231,250,251,270,297]){
    let calls=0;
    const draft=Roster.rollRandom({enabled:true,cap},3,pool,()=>{calls++;return .37;});
    assertLineups(draft,3,pool);assert.equal(calls,127,'one bounded shuffle, no retry loop');
  }
  const duplicatePool=[...pool,...pool];
  assertLineups(Roster.rollRandom({cap:250},3,duplicatePool,seeded(9)),3,pool);
});

test('Quick Play snapshots carry real gameplay skills without franchise ownership or shared mutable ratings',()=>{
  const state=Rise.create(),before=JSON.stringify(state),pool=[...Roster.catalog(classic,Rise,state),...Roster.quickPlayCatalog(classic,Rise)];
  const selected=pool.filter(entry=>entry.id.startsWith('quick:')&&entry.overall===80).slice(0,6);
  const draft={enabled:true,cap:240,teams:[selected.slice(0,3).map(entry=>entry.id),selected.slice(3).map(entry=>entry.id)]};
  const match=Roster.snapshot(draft,3,pool);
  for(let team=0;team<2;team++)for(let slot=0;slot<3;slot++){
    const source=selected[team*3+slot],profile=match.teams[team][slot];
    assert.equal(profile.customRosterId,source.id);assert.equal(profile.customOverall,80);
    assert.equal(profile.campaignPlayerId,undefined);assert.deepEqual(profile.ratings,source.profile.ratings);
    assert.deepEqual(profile.abilities,source.profile.abilities);assert.equal(profile.role,source.profile.role);
    profile.ratings.threePoint=1;profile.abilities.customTest=3;
    assert.notEqual(source.profile.ratings.threePoint,1);assert.equal(source.profile.abilities.customTest,undefined);
  }
  assert.equal(JSON.stringify(state),before);
});
