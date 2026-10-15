'use strict';
const test=require('node:test');const assert=require('node:assert/strict');
const D=require('../../../server/faction-forge/domain.cjs');
const {createForgeService}=require('../../../server/faction-forge/service.cjs');
const {migrate}=require('../../../server/faction-forge/migrate-account.cjs');
const copy=v=>v===undefined?v:JSON.parse(JSON.stringify(v));
function memoryDb(initial={}) {
  const docs=new Map(Object.entries(initial).map(([k,v])=>[k,copy(v)]));let queue=Promise.resolve(),writes=0;
  const snap=ref=>({exists:docs.has(ref.path),data:()=>copy(docs.get(ref.path)),id:ref.path.split('/').at(-1)});
  const ref=path=>({path,get:async()=>snap({path}),collection:name=>({doc:id=>ref(path+'/'+name+'/'+id)}),update:async data=>{docs.set(path,{...docs.get(path),...copy(data)});writes++;}});
  const db={doc:ref,docs,get writes(){return writes;},runTransaction(fn){const task=queue.then(async()=>{
    const pending=[];let beganWriting=false;
    const tx={get:async r=>{assert.equal(beganWriting,false,'Firestore transactions must read before writes');return snap(r);},
      create(r,data){beganWriting=true;assert(!docs.has(r.path));pending.push(['set',r,copy(data)]);},
      set(r,data,options){beganWriting=true;pending.push([options?.merge?'merge':'set',r,copy(data)]);},
      update(r,data){beganWriting=true;assert(docs.has(r.path));pending.push(['merge',r,copy(data)]);}};
    const result=await fn(tx);for(const [op,r,data]of pending){docs.set(r.path,op==='merge'?{...docs.get(r.path),...data}:data);writes++;}return result;
  });queue=task.catch(()=>{});return task;}};return db;
}
const who={uid:'verified-user',username:'LegacyName'};
const modules={campaign:{getMission:id=>({id,aiFaction:'Helions',opponent:{name:'AshenWolf',roster:[{type:'hellspawn',count:1,lvl:1}],formation:'wide',relics:[],tactics:[]}}),practiceOpponent:()=>({name:'SteelHawk',estimatedMmr:1250,faction:'Aesir'}),createEndlessMission:()=>({opponent:{name:'Endless',roster:[],formation:'balanced'}})}};
function fixture({adapterFactory}={}){
  let stamp=100000;const db=memoryDb({'users/LegacyName':{crowncouncilElo:1824,wins:20,password:'legacy-public-value'}});
  const auth={verifyIdToken:async()=>({uid:who.uid}),getUser:async uid=>({uid,disabled:false})};
  const service=createForgeService({db,auth,now:()=>stamp,modules,adapterFactory});return {db,auth,service,advance:ms=>stamp+=ms};
}
const fakeAdapter=()=>({version:'test',pack:(_s,_side,unlocked)=>[{type:'troop',key:unlocked[0],name:'Test'},{type:'troop',key:unlocked[1],name:'Second'},{type:'troop',key:unlocked[2],name:'Third'}],pick(s,side,card){s=copy(s);s.roster[side].push({type:card.key,count:1,lvl:1});return s;},aiPick:()=>0,battle:()=>({frames:[{at:0,units:[]}],durationMs:100,clockOrigin:1000,winnerSide:0})});

test('all 26 original troops start owned; six blueprints start locked and ranks remain independent',()=>{
  const p=D.newProfile('u','Name',1789);assert.equal(p.crowncouncilElo,1789);assert.equal(Object.keys(p.troops).length,32);assert.equal(Object.values(p.troops).filter(u=>u.unlocked).length,26);assert(D.NEW_TROOPS.every(k=>!p.troops[k].unlocked));
});
test('upgrade consumes exact configurable costs once, rejects active-match/locked/max tier/insufficient resources',()=>{
  const p=D.newProfile('u','Name');p.troops.rifleman.cards=45;p.forgeDust=500;
  const t2=D.upgrade(p,'rifleman');assert.equal(t2.profile.troops.rifleman.forgeTier,2);assert.equal(t2.profile.troops.rifleman.cards,35);assert.equal(t2.profile.forgeDust,400);
  const t3=D.upgrade(t2.profile,'rifleman');assert.equal(t3.profile.troops.rifleman.forgeTier,3);assert.equal(t3.profile.forgeDust,50);
  assert.throws(()=>D.upgrade(t3.profile,'rifleman'),{code:'MAX_TIER'});
  assert.throws(()=>D.upgrade(p,'railbreaker'),{code:'TROOP_LOCKED'});
  assert.throws(()=>D.upgrade({...p,activeSessionId:'live'},'rifleman'),{code:'MATCH_ACTIVE'});
  assert.throws(()=>D.upgrade(D.newProfile('x','X'),'rifleman'),{code:'INSUFFICIENT_RESOURCES'});
});
test('migration requires independent operator identity and preserves history without legacy credential/UID trust',async()=>{
  const f=fixture();await assert.rejects(f.service.identity('token'),{code:'ACCOUNT_NOT_LINKED'});
  const linked=await migrate({db:f.db,auth:f.auth,...who,now:100000});assert.equal(linked.crowncouncilElo,1824);
  assert.equal((await f.service.identity('token')).username,'LegacyName');assert.equal(f.db.docs.get('users/LegacyName').password,'legacy-public-value');
  await assert.rejects(migrate({db:f.db,auth:f.auth,username:'LegacyName',uid:'attacker'}),{code:'OWNERSHIP_CONFLICT'});
});
test('transactional upgrades are idempotent under simultaneous retries and conflicts cannot reuse a receipt',async()=>{
  const f=fixture();const p=await f.service.profile(who);p.troops.rifleman.cards=50;p.forgeDust=1000;f.db.docs.set('factionForgeProfiles/'+who.uid,p);
  const [a,b]=await Promise.all([f.service.upgradeTroop(who,{troopKey:'rifleman',requestId:'same-request'}),f.service.upgradeTroop(who,{troopKey:'rifleman',requestId:'same-request'})]);
  assert.deepEqual(a.transaction,b.transaction);assert.equal(b.profile.forgeDust,900);assert.equal(b.profile.troops.rifleman.cards,40);
  await assert.rejects(f.service.upgradeTroop(who,{troopKey:'miner',requestId:'same-request'}),{code:'REQUEST_CONFLICT'});
});
test('campaign gates and milestone unlocks use canonical IDs; replay only grants reduced reward',()=>{
  const p=D.newProfile('u','Name');assert.equal(D.canStart(p,{mode:'campaign',missionId:'c1m1'}).missionId,'forge-01');assert.throws(()=>D.canStart(p,{mode:'campaign',missionId:'forge-02'}),{code:'MISSION_LOCKED'});
  const s={id:'verified',mode:'campaign',phase:'game-over',serverValidated:true,participants:['u',null],winnerSide:0,missionNumber:6,missionId:'forge-06',lives:[3,0]};
  const first=D.settle(p,s,0);assert.equal(first.receipt.forgeDust,40);assert.equal(first.receipt.stars,3);assert(first.profile.troops.signalhawk.unlocked);
  const replay=D.settle(first.profile,{...s,id:'second'},0);assert.equal(replay.receipt.forgeDust,5);assert.equal(replay.receipt.cards.reduce((n,c)=>n+c.quantity,0),2);
});
test('reported outcomes cannot settle; bot MMR is immutable; farm rewards are capped',()=>{
  const p=D.newProfile('u','Name',2100);const s={id:'server',mode:'bot',phase:'game-over',serverValidated:true,participants:['u',null],winnerSide:0,lives:[4,0]};
  assert.throws(()=>D.settle(p,{...s,serverValidated:false},0),{code:'RESULT_UNVERIFIED'});
  assert.throws(()=>D.settle(p,{...s,participants:['other',null]},0),{code:'PERMISSION_DENIED'});
  let result={profile:p};for(let i=0;i<11;i++)result=D.settle(result.profile,{...s,id:'server-'+i},0,{now:100000});
  assert.equal(result.profile.crowncouncilElo,2100);assert.equal(result.profile.forgeDust,100);assert.equal(result.receipt.forgeDust,0);assert.equal(result.receipt.reason,'Daily reward limit reached.');
});
test('solo sessions issue legal packs, reject strangers and stale/illegal picks; reads do not write and duplicate end events grant once',async()=>{
  const f=fixture({adapterFactory:fakeAdapter});await f.service.profile(who);
  await assert.rejects(f.service.startSolo(who,{mode:'campaign',missionId:'forge-01',faction:'Vanir',winner:who.username,forgeDust:1000}),{code:'INVALID_ARGUMENT'});
  let data=await f.service.startSolo(who,{mode:'campaign',missionId:'forge-01',faction:'Vanir'});const id=data.session.id;
  assert.equal(data.session.players[1],'AshenWolf');assert.equal(data.session.currentPacks.p1.length,0);assert.equal(data.session.seed,undefined);assert.equal(data.session.profiles,undefined);
  const before=f.db.writes;await f.service.getSession(who,id);assert.equal(f.db.writes,before);
  await assert.rejects(f.service.getSession({uid:'intruder'},id),{code:'PERMISSION_DENIED'});
  await assert.rejects(f.service.pick(who,id,{index:4,sequence:0,requestId:'bad'}),{code:'INVALID_PICK'});
  for(let round=0;round<4;round++) {
    if(data.session.phase==='loser-bonus'){data=await f.service.pick(who,id,{index:0,sequence:data.session.revision,requestId:'bonus-'+round});}
    for(let ix=0;ix<3;ix++) {
      const args={index:0,sequence:data.session.revision,requestId:'r'+round+'i'+ix};
      data=await f.service.pick(who,id,args);const repeat=await f.service.pick(who,id,args);assert.equal(repeat.session.revision,data.session.revision);
    }
    assert.equal(data.session.phase,'battle');assert.equal(data.receipt,undefined);const lean=await f.service.getSession(who,id,{knownReplayId:data.session.battle.replayId});assert.equal(lean.session.battle.frames,undefined);
    f.advance(21000);data=await f.service.getSession(who,id);
  }
  assert.equal(data.session.phase,'game-over');assert.equal(data.receipt.forgeDust,20);assert.equal(data.profile.crowncouncilElo,1824);assert.deepEqual(data.session.roundWins,[4,0]);assert.equal(data.receipt.roundCards,12);assert.equal(data.receipt.cards.reduce((n,card)=>n+card.quantity,0),18);
  const same=await f.service.getSession(who,id);assert.deepEqual(same.receipt,data.receipt);assert.equal(same.profile.forgeDust,data.profile.forgeDust);assert.equal(same.profile.wins,1);
});
test('PvP needs independent verified-seat inputs and cannot award from host-only winner snapshots',async()=>{
  const f=fixture({adapterFactory:fakeAdapter});const peer={uid:'peer-user',username:'Opponent'};
  f.db.docs.set('users/Opponent',{crowncouncilElo:1800});f.db.docs.set('lobbies/public-lobby',{gameType:'crowncouncil',host:who.username,hostUid:who.uid,members:{[who.uid]:who.username,[peer.uid]:peer.username},visibility:'public',players:[who.username,peer.username],winner:who.username,status:'started'});
  let host=await f.service.joinPvp(who,{lobbyId:'public-lobby',faction:'Vanir',ranked:true});assert.equal(host.session.phase,'waiting');
  let other=await f.service.joinPvp(peer,{lobbyId:'public-lobby',faction:'Aesir',ranked:true});assert.equal(other.session.phase,'draft');
  host=await f.service.getSession(who,host.session.id);const id=host.session.id;
  host=await f.service.pick(who,id,{index:0,sequence:host.session.revision,requestId:'host-only'});
  assert.equal(host.session.phase,'draft');assert.equal(host.session.draftIndex,0);assert.equal(host.receipt,undefined);
  await assert.rejects(f.service.pick(who,id,{index:0,sequence:host.session.revision,requestId:'host-twice'}),{code:'ALREADY_PICKED'});
  other=await f.service.pick(peer,id,{index:0,sequence:other.session.revision,requestId:'peer-input'});assert.equal(other.session.draftIndex,1);
  assert.equal((await f.service.profile(who)).crowncouncilElo,1824);
});

test('server timeout choices advance both verified seats without a client-reported winner',async()=>{
  const f=fixture({adapterFactory:fakeAdapter}),peer={uid:'timeout-peer',username:'TimeoutPeer'};f.db.docs.set('users/TimeoutPeer',{crowncouncilElo:1700});f.db.docs.set('lobbies/timeout-lobby',{gameType:'crowncouncil',host:who.username,hostUid:who.uid,members:{[who.uid]:who.username,[peer.uid]:peer.username},visibility:'public',players:[who.username,peer.username]});
  const host=await f.service.joinPvp(who,{lobbyId:'timeout-lobby',faction:'Vanir',ranked:true});await f.service.joinPvp(peer,{lobbyId:'timeout-lobby',faction:'Helions',ranked:true});f.advance(61000);const next=await f.service.getSession(who,host.session.id);assert.equal(next.session.draftIndex,1);assert.equal(next.receipt,undefined);assert(next.session.deadlineAt>161000);
});

test('complete authoritative PvP writes both legacy ratings exactly once; early voluntary forfeit cannot mint cards',async()=>{
  const f=fixture({adapterFactory:fakeAdapter}),peer={uid:'rated-peer',username:'RatedPeer'};f.db.docs.set('users/RatedPeer',{crowncouncilElo:1800});f.db.docs.set('lobbies/rated-lobby',{gameType:'crowncouncil',host:who.username,hostUid:who.uid,members:{[who.uid]:who.username,[peer.uid]:peer.username},visibility:'public',players:[who.username,peer.username]});
  let data=await f.service.joinPvp(who,{lobbyId:'rated-lobby',faction:'Vanir',ranked:true});const id=data.session.id;await f.service.joinPvp(peer,{lobbyId:'rated-lobby',faction:'Helions',ranked:true});data=await f.service.getSession(who,id);
  for(let round=0;round<4;round++){
    if(data.session.phase==='loser-bonus')data=await f.service.pick(peer,id,{index:0,sequence:data.session.revision,requestId:'rated-bonus-'+round});
    for(let i=0;i<3;i++){data=await f.service.pick(who,id,{index:0,sequence:data.session.revision,requestId:'rated-host-'+round+'-'+i});data=await f.service.pick(peer,id,{index:0,sequence:data.session.revision,requestId:'rated-peer-'+round+'-'+i});}
    f.advance(21000);data=await f.service.getSession(who,id);
  }
  assert.equal(data.session.phase,'game-over');assert.equal(data.profile.crowncouncilElo,1849);assert.equal((await f.service.profile(peer)).crowncouncilElo,1775);assert.equal(data.session.mmrChange[0],25);assert.equal(f.db.docs.get('users/LegacyName').crowncouncilElo,1849);assert.equal(f.db.docs.get('users/RatedPeer').crowncouncilElo,1775);
  const again=await f.service.getSession(peer,id);assert.equal(again.profile.crowncouncilElo,1775);assert.equal(again.profile.losses,1);assert.equal(again.receipt.forgeDust,0);
  let next=await f.service.joinPvp(who,{lobbyId:'rated-lobby',faction:'Vanir',ranked:true});await f.service.joinPvp(peer,{lobbyId:'rated-lobby',faction:'Helions',ranked:true});const forfeit=await f.service.abandon(peer,next.session.id);assert.equal(forfeit.receipt.won,false);const win=await f.service.getSession(who,next.session.id);assert.equal(win.receipt.won,true);assert.equal(win.receipt.forgeDust,0);assert.equal(win.receipt.cards.length,0);assert.equal(win.profile.wins,2);assert.equal(win.profile.crowncouncilElo,1849);assert.match(win.receipt.ratingReason,/no winner rating gain/);
});

test('unprotected name-only lobbies and private rooms cannot create ranked sessions',async()=>{
  const f=fixture({adapterFactory:fakeAdapter});f.db.docs.set('lobbies/name-only',{gameType:'crowncouncil',host:who.username,players:[who.username],visibility:'public'});
  await assert.rejects(f.service.joinPvp(who,{lobbyId:'name-only',faction:'Vanir',ranked:true}),{code:'VERIFIED_LOBBY_REQUIRED'});
  f.db.docs.set('lobbies/private-room',{gameType:'crowncouncil',host:who.username,hostUid:who.uid,members:{[who.uid]:who.username},players:[who.username],visibility:'private'});
  await assert.rejects(f.service.joinPvp(who,{lobbyId:'private-room',faction:'Vanir',ranked:true}),{code:'PRIVATE_PRACTICE_ONLY'});
});
test('rating cannot be minted at zero floor, from early forfeits, or through repeated opponent farming',()=>{
  const profiles=[D.newProfile('winner','Winner',1000,100000),D.newProfile('loser','Loser',0,100000)];
  const session={id:'rated',mode:'pvp',ranked:true,phase:'game-over',serverValidated:true,participants:['winner','loser'],winnerSide:0,createdAt:100000,completedBattles:4,lives:[4,0]};
  assert.deepEqual(D.ratingDecision(profiles,session,140000).delta,[0,0]);
  profiles[1].crowncouncilElo=9;assert.deepEqual(D.ratingDecision(profiles,session,140000).delta,[9,-9]);
  profiles[1].crowncouncilElo=1000;const early={...session,forfeitUid:'loser',completedBattles:0};assert.deepEqual(D.ratingDecision(profiles,early,100100).delta,[0,-25]);
  profiles[0].rewardDays['1970-01-01']={rated_opponent_loser:3};assert.deepEqual(D.ratingDecision(profiles,session,140000).delta,[0,0]);assert.equal(D.ratingDecision(profiles,session,140000).counted,false);
});

test('lobby-owned casual/ranked setting cannot be overridden by a participant input',async()=>{
  const f=fixture({adapterFactory:fakeAdapter});f.db.docs.set('lobbies/casual-room',{gameType:'crowncouncil',host:who.username,hostUid:who.uid,members:{[who.uid]:who.username},players:[who.username],visibility:'public',forgeRanked:false});
  const casual=await f.service.joinPvp(who,{lobbyId:'casual-room',faction:'Vanir',ranked:true});assert.equal(casual.session.ranked,false);
});

test('browser and server use identical fixed card sets, prices and boosted reward amounts',async()=>{
  const browser=await import('../packs.js');assert.deepEqual(D.FORGE_PACKS,browser.FORGE_PACKS);assert.deepEqual(D.REWARDS,browser.WIN_REWARDS);assert.equal(D.ROUND_WIN_CARDS,browser.ROUND_WIN_CARDS);assert.equal(D.ROUND_WIN_DAILY_CAP,browser.ROUND_WIN_DAILY_CAP);
});
test('server pack purchases are atomic and idempotent; inputs cannot override costs or ownership',async()=>{
  const f=fixture(),p=await f.service.profile(who);p.forgeDust=50;f.db.docs.set('factionForgeProfiles/'+who.uid,p);
  const [a,b]=await Promise.all([f.service.buyPack(who,{packId:'vanir',requestId:'pack-race'}),f.service.buyPack(who,{packId:'vanir',requestId:'pack-race'})]);
  assert.deepEqual(a.transaction,b.transaction);assert.equal(b.profile.forgeDust,0);assert.equal(b.profile.troops.rifleman.cards,5);assert.equal(b.transaction.cards.reduce((n,card)=>n+card.quantity,0),15);
  await assert.rejects(f.service.buyPack(who,{packId:'aesir',requestId:'pack-race'}),{code:'REQUEST_CONFLICT'});await assert.rejects(f.service.buyPack(who,{packId:'aesir',requestId:'overspend'}),{code:'INSUFFICIENT_RESOURCES'});
  await assert.rejects(f.service.buyPack(who,{packId:'vanir',requestId:'injected',dustSpent:0}),{code:'INVALID_ARGUMENT'});assert.throws(()=>D.buyPack({...p,activeSessionId:'battle'},'vanir'),{code:'MATCH_ACTIVE'});assert.throws(()=>D.buyPack(p,'invented'),{code:'INVALID_PACK'});
});
test('losing a completed match keeps rewards for rounds won and the round-card cap is independent of match wins',()=>{
  let p=D.newProfile('loser','Loser',1500,100000);const session={id:'round-cards',mode:'bot',phase:'game-over',serverValidated:true,participants:['loser',null],winnerSide:1,lives:[0,2],roundWins:[2,4],completedBattles:6,createdAt:100000};
  let result=D.settle(p,session,0,{now:200000});assert.equal(result.receipt.won,false);assert.equal(result.receipt.roundCards,6);assert.equal(result.receipt.cards.reduce((n,c)=>n+c.quantity,0),6);assert.equal(result.receipt.forgeDust,0);
  p=result.profile;p.rewardDays['1970-01-01'].round=69;result=D.settle(p,{...session,id:'last-capped-round'},0,{now:200000});assert.equal(result.receipt.roundCards,3);assert.equal(result.profile.rewardDays['1970-01-01'].round,70);
  result=D.settle(result.profile,{...session,id:'over-cap'},0,{now:200000});assert.equal(result.receipt.cards.length,0);assert.equal(result.profile.crowncouncilElo,1500);
});
