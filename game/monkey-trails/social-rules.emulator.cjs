const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{pathToFileURL}=require('node:url');
const {initializeTestEnvironment,assertSucceeds,assertFails}=require('@firebase/rules-unit-testing'),f=require('firebase/firestore');
(async()=>{
 const [host,port]=(process.env.FIRESTORE_EMULATOR_HOST||'127.0.0.1:8097').split(':'),env=await initializeTestEnvironment({projectId:'demo-monkey-trails',firestore:{host,port:Number(port),rules:fs.readFileSync(path.resolve(__dirname,'../../firestore.rules'),'utf8')}});
 const {SocialStore,createFirestoreSocialAdapter}=await import(pathToFileURL(path.join(__dirname,'social-store.mjs'))),C=await import(pathToFileURL(path.join(__dirname,'core.mjs'))),A=await import(pathToFileURL(path.join(__dirname,'social-combat.mjs')));
 const root='monkeyTrailsSocial/v1',dbs={},stores={};let serial=0,checks=0;
 const yes=async(p,label)=>{try{const r=await assertSucceeds(p);checks++;return r;}catch(e){throw Error(label+': '+e.message,{cause:e});}},no=async(p,label)=>{try{await assertFails(p);checks++;}catch(e){throw Error(label+': '+e.message,{cause:e});}};
 const blocked=async(p,label)=>{try{await assert.rejects(p);checks++;}catch(e){throw Error(label+': '+e.message,{cause:e});}};
 const make=async uid=>{const db=env.authenticatedContext(uid).firestore();dbs[uid]=db;const s=stores[uid]=new SocialStore(createFirestoreSocialAdapter({db,f}),{uid,id:()=>`${uid}-operation-${++serial}`});await yes(s.register({saveId:`save-${uid}`,name:`Player ${uid}`,team:A.snapshotTeam(C.newSave(`Player ${uid}`,'capuchin','gps',22))}),`register ${uid}`);return s;};
 try{
  await env.clearFirestore();const a=await make('alice'),b=await make('bob'),c=await make('charlie');
  await no(f.setDoc(f.doc(dbs.bob,root,'players','alice'),{uid:'bob'}),'other-player writes denied');
  await no(f.getDoc(f.doc(env.unauthenticatedContext().firestore(),root,'players','alice')),'signed-out reads denied');
  await yes(a.deposit('fruit',40,'deposit-alice'),'own earned deposit');await yes(b.deposit('fruit',40,'deposit-bob'),'second deposit');
  const tribe=await yes(a.createTribe({name:'Canopy Guardians',emblem:{shape:'shield',symbol:'leaf',color:'#477a51'}}),'create tribe');await yes(b.joinTribe(tribe.id),'join tribe');
  // New weekly shared objective: membership-scoped counts, exact delta totals,
  // immutable per-week claims and no second-tribe participation.
  const zero={meters:0,discoveries:0,challenges:0,raids:0,trades:0};
  const am=await yes(a.joinAdventure(zero),'opt into weekly adventure');
  await yes(b.joinAdventure({...zero,meters:1000}),'second member baseline');
  await yes(b.contributeAdventure({...zero,meters:1500}),'accepted contribution delta');
  await yes(b.contributeAdventure({...zero,meters:1500}),'same upload idempotent');
  await yes(a.contributeAdventure({...zero,meters:30000}),'complete shared weekly objective');
  const summary=await f.getDoc(f.doc(dbs.alice,root,'adventures',am.summaryId));assert.equal(summary.data().points,610);checks++;
  await no(f.updateDoc(f.doc(dbs.bob,root,'adventures',am.summaryId),{points:999999,updatedAt:Date.now()}),'summary cannot grow without own matching member delta');
  await no(f.updateDoc(f.doc(dbs.bob,root,'adventureMembers',am.id),{points:999999,updatedAt:Date.now()}),'other member contribution cannot be rewritten');
  await no(f.getDoc(f.doc(dbs.charlie,root,'adventures',am.summaryId)),'outsider cannot read weekly progress');
  await yes(a.adapter.list('adventureMembers',[['tribeId','==',tribe.id],['weekStart','==',am.weekStart]]),'weekly contribution roster query');
  const weeklyClaim=await yes(a.claimAdventure(),'weekly reward claim');
  assert.deepEqual(await yes(a.claimAdventure(),'repeated claim returns same immutable record'),weeklyClaim);checks++;
  await yes(a.adapter.list('adventureClaims',[['uid','==','alice']]),'own immutable reward claim query');
  await no(f.updateDoc(f.doc(dbs.alice,root,'adventureClaims',weeklyClaim.id),{'reward.fruit':999}),'claimed reward is immutable');
  await yes(b.leaveTribe(),'leave after contribution');
  await blocked(b.claimAdventure(),'departed member cannot claim');
  const other=await yes(b.createTribe({name:'Another Canopy'}),'create different tribe');
  await blocked(b.joinAdventure(zero),'one tribe objective per player per week');
  await yes(b.leaveTribe(),'leave temporary tribe');await yes(b.joinTribe(tribe.id),'return to original tribe');
  await yes(b.joinAdventure(zero),'same tribe opt-in returns prior baseline');
  await yes(b.contributeAdventure({...zero,meters:1500}),'rejoin does not duplicate contribution');
  await yes(b.claimAdventure(),'rejoined member can claim earned reward');
  const oldWeek=am.weekStart-604800000,oldMember={...am,id:`${oldWeek}-alice`,weekStart:oldWeek,weekEnd:am.weekStart,summaryId:`${oldWeek}-${tribe.id}`,joinedAt:oldWeek+1000,updatedAt:oldWeek+1000};
  await env.withSecurityRulesDisabled(async ctx=>{const db=ctx.firestore();await f.setDoc(f.doc(db,root,'adventureMembers',oldMember.id),oldMember);await f.setDoc(f.doc(db,root,'adventures',oldMember.summaryId),{id:oldMember.summaryId,tribeId:tribe.id,weekStart:oldWeek,weekEnd:am.weekStart,target:600,points:0,updatedAt:oldWeek+1000});});
  await yes(a.contributeAdventure({...zero,meters:30000},oldWeek),'previous enrolled week accepts offline catch-up');
  await yes(a.claimAdventure(oldWeek),'previous completed objective reward recovery');

  await yes(a.gift('bob','fruit',3),'atomic gift');assert.equal((await a.getPlayer()).wallet.fruit,37);assert.equal((await b.getPlayer()).wallet.fruit,43);
  await no(f.updateDoc(f.doc(dbs.charlie,root,'players','alice'),{'wallet.fruit':999}),'outsider cannot mint partner wallet');
  await yes(a.deposit('berry',3,'berry-alice'),'deposit item');const offer=await yes(a.offerTrade('bob',{asset:'berry',amount:1},{asset:'fruit',amount:2}),'escrow trade');await yes(b.acceptTrade(offer.id),'accept trade');assert.equal((await b.getPlayer()).wallet.items.berry,1);
  const cancel=await yes(a.offerTrade('bob',{asset:'berry',amount:1},{asset:'fruit',amount:2}),'second escrow trade');await yes(b.cancelTrade(cancel.id),'recipient cancels and refunds escrow');
  await yes(a.heartbeat(),'first presence');await yes(b.heartbeat(),'second presence');
  const challenge=await yes(a.challenge('bob'),'tribe challenge');const duel=await yes(b.acceptChallenge(challenge.id),'accept and escrow duel');
  await blocked(a.challenge('bob'),'busy players reject new challenges');
  await no(f.updateDoc(f.doc(dbs.charlie,root,'duels',duel.id),{revision:1}),'outsider cannot append turn');
  await blocked(b.act(duel.id,{type:'move',id:'strike'},0),'off-turn play denied');
  await yes(a.act(duel.id,{type:'move',id:'strike'},0),'first synchronized turn');
  await blocked(a.act(duel.id,{type:'move',id:'strike'},0),'stale sequence denied');
  await yes(b.surrender(duel.id,1),'finish and settle duel');await yes(a.returnToTrail(),'winner returns');await yes(b.returnToTrail(),'loser returns');
  await yes(a.heartbeat(),'availability restored');await yes(b.heartbeat(),'second availability restored');const repeat=await yes(a.challenge('bob'),'immediate rematch');await yes(b.declineChallenge(repeat.id),'decline rematch');
  const counterfeit=await yes(a.challenge('bob'),'challenge for escrow attack test');
  const bobBase=createFirestoreSocialAdapter({db:dbs.bob,f}),badEscrow={...bobBase,transaction:cb=>bobBase.transaction(async tx=>{const writes=[];const result=await cb({get:tx.get,set:(p,v)=>writes.push([p,structuredClone(v)]),delete:tx.delete});const operation=writes.find(([p,v])=>p.startsWith('operations/')&&v.kind==='duel-start')?.[1];if(operation){for(const uid of operation.participants){operation.details.escrow[uid]=-1;operation.details.wallets[uid].after.fruit=operation.details.wallets[uid].before.fruit+1;writes.find(([p])=>p===`players/${uid}`)[1].wallet.fruit=operation.details.wallets[uid].after.fruit;}writes.find(([p])=>p.startsWith('duels/'))[1].escrow=structuredClone(operation.details.escrow);}for(const [p,v]of writes)tx.set(p,v);return result;})};
  await no(new SocialStore(badEscrow,{uid:'bob'}).acceptChallenge(counterfeit.id),'negative escrow cannot mint either player fruit');await yes(b.declineChallenge(counterfeit.id),'close rejected malicious acceptance');
  await no(f.setDoc(f.doc(dbs.charlie,root,'receipts','alice','entries','fake'),{id:'fake',uid:'alice',type:'raid',title:'Forged',payload:{fruit:100,xp:100,items:{}},walletCredited:true,sourceId:'fake',createdAt:Date.now(),seen:false}),'outsider forged rewards denied');
  await yes(c.heartbeat({discoverable:true,location:{lat:40,lon:-74,accuracy:100,at:Date.now()}}),'nearby presence');await yes(a.heartbeat({discoverable:true,location:{lat:40,lon:-74,accuracy:100,at:Date.now()}}),'own nearby presence');
  const nearby=await yes(a.challenge('charlie'),'opted-in nearby challenge');await yes(c.declineChallenge(nearby.id),'nearby decline');
  await yes(c.heartbeat({discoverable:true,location:{lat:42,lon:-74,accuracy:100,at:Date.now()}}),'far presence');await blocked(a.challenge('charlie'),'one-mile boundary blocks far strangers');
  const raid=await yes(a.startRaid(),'hourly raid start');await yes(a.abandonRaid(raid.id),'raid abandonment clears busy');await yes(a.heartbeat(),'raid availability');await blocked(a.startRaid(),'server cooldown rejects second hourly attempt');
  for(let i=0;i<8;i++){const member=await make(`member${i}`);await yes(member.joinTribe(tribe.id),'join until cap');}
  await blocked(c.joinTribe(tribe.id),'eleventh member rejected');
  // Ten-member fanout executes under Firestore's real per-transaction access budget.
  await env.withSecurityRulesDisabled(async ctx=>{const db=ctx.firestore();await f.updateDoc(f.doc(db,root,'tribes',tribe.id),{'boss.hp':1});await f.updateDoc(f.doc(db,root,'players','alice'),{lastRaidAt:0});});
  await yes(a.heartbeat(),'boss fanout availability');const fanout=await yes(a.startRaid(),'ten-member raid start');
  const beforeFanout=await Promise.all([a,b,...Object.values(stores).filter(s=>s.uid.startsWith('member'))].map(s=>s.getPlayer()));
  await yes(a.submitRaid(fanout.id,Array.from({length:10},()=>({type:'move',id:'strike'}))),'ten-member boss defeat rewards');
  for(const previous of beforeFanout){const current=await stores[previous.uid].getPlayer();assert.equal(current.wallet.fruit,previous.wallet.fruit+10);assert.equal(current.wallet.items.berry,(previous.wallet.items.berry||0)+1);checks++;}
  // Mutate otherwise-valid adapter writes to test authorized-actor forgery attempts.
  const evilAdapter=mutate=>{const base=createFirestoreSocialAdapter({db:dbs.alice,f});return{...base,transaction:cb=>base.transaction(tx=>cb({...tx,set:(p,v)=>{const value=structuredClone(v);mutate(p,value,tx);tx.set(p,value);}}))};};
  const forgedTrade=new SocialStore(evilAdapter((p,v)=>{if(p.startsWith('offers/'))v.give.amount=100000;}),{uid:'alice',id:()=>`forged-offer-${++serial}`});
  await no(forgedTrade.offerTrade('bob',{asset:'fruit',amount:1},{asset:'berry',amount:1}),'offer payload must exactly match escrow operation');
  const forgedReward=new SocialStore(evilAdapter((p,v,tx)=>{if(p.startsWith('operations/')){const r={id:v.id,uid:'alice',type:'raid',title:'Forged XP',payload:{fruit:1000,xp:10000,items:{}},walletCredited:false,sourceId:v.id,createdAt:Date.now(),seen:false};v.details.receipts={alice:{type:r.type,title:r.title,payload:r.payload,walletCredited:r.walletCredited}};tx.set(`receipts/alice/entries/${v.id}`,r);}}),{uid:'alice'});
  await no(forgedReward.deposit('fruit',1,'forged-deposit-receipt'),'deposit cannot manufacture a reward receipt');
  console.log(JSON.stringify({checks,ownership:true,tribeCap:true,atomicTrading:true,duelTurns:true,nearby:true,cooldown:true},null,2));
 }finally{await env.cleanup();}
})().catch(e=>{console.error(e);process.exitCode=1;});
