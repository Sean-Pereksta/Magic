const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{pathToFileURL}=require('node:url');
const {initializeTestEnvironment,assertSucceeds,assertFails}=require('@firebase/rules-unit-testing'),f=require('firebase/firestore');
(async()=>{
 const [host,port]=(process.env.FIRESTORE_EMULATOR_HOST||'127.0.0.1:8097').split(':'),env=await initializeTestEnvironment({projectId:'demo-monkey-trails',firestore:{host,port:Number(port),rules:fs.readFileSync(path.resolve(__dirname,'../../firestore.rules'),'utf8')}});
 const {SocialStore,createFirestoreSocialAdapter}=await import(pathToFileURL(path.join(__dirname,'social-store.mjs'))),C=await import(pathToFileURL(path.join(__dirname,'core.mjs'))),A=await import(pathToFileURL(path.join(__dirname,'social-combat.mjs')));
 const root='monkeyTrailsSocial/v1',dbs={},stores={};let serial=0,checks=0;
 const yes=async(p,label)=>{try{const r=await assertSucceeds(p);checks++;return r;}catch(e){throw Error(label+': '+e.message,{cause:e});}},no=async(p,label)=>{try{await assertFails(p);checks++;}catch(e){throw Error(label+': '+e.message,{cause:e});}};
 const blocked=async(p,label)=>{try{await assert.rejects(p);checks++;}catch(e){throw Error(label+': '+e.message,{cause:e});}};
 const mutatedStore=(uid,mutate)=>{const base=createFirestoreSocialAdapter({db:dbs[uid],f});return new SocialStore({...base,transaction:cb=>base.transaction(tx=>cb({...tx,set:(p,v)=>{const value=structuredClone(v);mutate(p,value);tx.set(p,value);}}))},{uid,id:()=>`${uid}-version-${++serial}`});};
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
  assert.equal(duel.state.version,A.SOCIAL_COMBAT_VERSION);checks++;
  await blocked(a.challenge('bob'),'busy players reject new challenges');
  await no(f.updateDoc(f.doc(dbs.charlie,root,'duels',duel.id),{revision:1}),'outsider cannot append turn');
  await blocked(b.act(duel.id,{type:'move',id:'strike'},0),'off-turn play denied');
  await no(mutatedStore('alice',(p,v)=>{if(p.startsWith('duels/'))v.state.version=2;}).act(duel.id,{type:'move',id:'strike'},0),'equipment battle cannot change balance version mid-turn');
  await yes(a.act(duel.id,{type:'move',id:'strike'},0),'first synchronized turn');
  await blocked(a.act(duel.id,{type:'move',id:'strike'},0),'stale sequence denied');
  await yes(b.surrender(duel.id,1),'finish and settle duel');await yes(a.returnToTrail(),'winner returns');await yes(b.returnToTrail(),'loser returns');
  for(const version of [undefined,1,2]){
   await yes(a.heartbeat(),`legacy ${version===undefined?'unversioned':`v${version}`} challenger available`);await yes(b.heartbeat(),`legacy ${version===undefined?'unversioned':`v${version}`} recipient available`);
   const legacyChallenge=await yes(a.challenge('bob'),`legacy ${version===undefined?'unversioned':`v${version}`} challenge`);
   await yes(mutatedStore('bob',(p,v)=>{if(p.startsWith('duels/')){v.state=A.newDuel(...v.initialProfiles,v.seed,version??1);if(version===undefined)delete v.state.version;}}).acceptChallenge(legacyChallenge.id),`legacy ${version===undefined?'unversioned':`v${version}`} duel fixture`);
   const legacyTurn=await yes(a.act(legacyChallenge.id,{type:'move',id:'strike'},0),`legacy ${version===undefined?'unversioned':`v${version}`} duel continues`);assert.equal(legacyTurn.state.version,version??1);checks++;
   await yes(b.surrender(legacyChallenge.id,1),`legacy ${version===undefined?'unversioned':`v${version}`} duel settles`);await yes(a.returnToTrail(),`legacy ${version===undefined?'unversioned':`v${version}`} winner returns`);await yes(b.returnToTrail(),`legacy ${version===undefined?'unversioned':`v${version}`} loser returns`);
  }
  await yes(a.heartbeat(),'availability restored');await yes(b.heartbeat(),'second availability restored');const repeat=await yes(a.challenge('bob'),'immediate rematch');await yes(b.declineChallenge(repeat.id),'decline rematch');
  const counterfeit=await yes(a.challenge('bob'),'challenge for escrow attack test');
  const bobBase=createFirestoreSocialAdapter({db:dbs.bob,f}),badEscrow={...bobBase,transaction:cb=>bobBase.transaction(async tx=>{const writes=[];const result=await cb({get:tx.get,set:(p,v)=>writes.push([p,structuredClone(v)]),delete:tx.delete});const operation=writes.find(([p,v])=>p.startsWith('operations/')&&v.kind==='duel-start')?.[1];if(operation){for(const uid of operation.participants){operation.details.escrow[uid]=-1;operation.details.wallets[uid].after.fruit=operation.details.wallets[uid].before.fruit+1;writes.find(([p])=>p===`players/${uid}`)[1].wallet.fruit=operation.details.wallets[uid].after.fruit;}writes.find(([p])=>p.startsWith('duels/'))[1].escrow=structuredClone(operation.details.escrow);}for(const [p,v]of writes)tx.set(p,v);return result;})};
  await no(new SocialStore(badEscrow,{uid:'bob'}).acceptChallenge(counterfeit.id),'negative escrow cannot mint either player fruit');await yes(b.declineChallenge(counterfeit.id),'close rejected malicious acceptance');
  const futureChallenge=await yes(a.challenge('bob'),'future-version challenge fixture');
  await no(mutatedStore('bob',(p,v)=>{if(p.startsWith('duels/'))v.state.version=6;}).acceptChallenge(futureChallenge.id),'unknown duel balance version rejected');await yes(b.declineChallenge(futureChallenge.id),'close rejected future-version challenge');
  await no(f.setDoc(f.doc(dbs.charlie,root,'receipts','alice','entries','fake'),{id:'fake',uid:'alice',type:'raid',title:'Forged',payload:{fruit:100,xp:100,items:{}},walletCredited:true,sourceId:'fake',createdAt:Date.now(),seen:false}),'outsider forged rewards denied');
  await yes(c.heartbeat({discoverable:true,location:{lat:40,lon:-74,accuracy:100,at:Date.now()}}),'nearby presence');await yes(a.heartbeat({discoverable:true,location:{lat:40,lon:-74,accuracy:100,at:Date.now()}}),'own nearby presence');
  const nearby=await yes(a.challenge('charlie'),'opted-in nearby challenge');await yes(c.declineChallenge(nearby.id),'nearby decline');
  await yes(c.heartbeat({discoverable:true,location:{lat:42,lon:-74,accuracy:100,at:Date.now()}}),'far presence');await blocked(a.challenge('charlie'),'one-mile boundary blocks far strangers');
  await no(mutatedStore('alice',(p,v)=>{if(p.startsWith('raids/'))v.version=6;}).startRaid(),'unknown raid balance version rejected');
  const raid=await yes(a.startRaid(),'hourly raid start');assert.equal(raid.version,A.SOCIAL_COMBAT_VERSION);checks++;
  await no(f.updateDoc(f.doc(dbs.alice,root,'raids',raid.id),{version:2}),'raid balance version immutable');
  await yes(a.abandonRaid(raid.id),'raid abandonment clears busy');await yes(a.heartbeat(),'raid availability');await blocked(a.startRaid(),'server cooldown rejects second hourly attempt');
  for(const version of [undefined,1,2,3]){
   await env.withSecurityRulesDisabled(ctx=>f.updateDoc(f.doc(ctx.firestore(),root,'players','alice'),{lastRaidAt:0}));
   const legacyRaid=await yes(mutatedStore('alice',(p,v)=>{if(p.startsWith('raids/')){if(version===undefined)delete v.version;else v.version=version;}}).startRaid(),`legacy ${version===undefined?'unversioned':`v${version}`} raid fixture`);
   const legacyResult=await yes(a.submitRaid(legacyRaid.id,Array.from({length:10},()=>({type:'move',id:'strike'}))),`legacy ${version===undefined?'unversioned':`v${version}`} raid replay submits`);assert.equal(legacyResult.attempt.version,version);checks++;
   await yes(a.heartbeat(),`legacy ${version===undefined?'unversioned':`v${version}`} raid availability`);
  }
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
  // Rankings must order the complete remote collection before applying the
  // limit. The strongest fixtures deliberately occur after the old first 30.
  await env.withSecurityRulesDisabled(async ctx=>{const db=ctx.firestore();for(let i=0;i<35;i++){const id=`ranking-${String(i).padStart(2,'0')}`,level=i>=33?99:i+5;await f.setDoc(f.doc(db,root,'tribes',id),{...tribe,id,code:id,name:`Ranked Tribe ${i}`,members:['ranking-member'],ownerId:'ranking-member',boss:{level,hp:A.bossHealth(level),maxHp:A.bossHealth(level),revision:0},updatedAt:Date.now()});}});
  const rankings=await yes(c.listTribeRankings(),'authenticated non-member can query the actual top tribes');
  assert.equal(rankings.length,25);assert.deepEqual(rankings.slice(0,3).map(t=>[t.id,t.rank]),[['ranking-34',1],['ranking-33',1],['ranking-32',3]]);checks++;
  for(const [index,row] of rankings.entries()){assert.deepEqual(Object.keys(row).sort(),['id','name','emblem','memberCount','bossLevel','bossesDefeated','rank'].sort());assert.equal(row.bossesDefeated,row.bossLevel-1);assert.equal(row.memberCount,1);if(index)assert.ok(row.bossLevel<=rankings[index-1].bossLevel);}checks++;
  await no(f.getDocs(f.query(f.collection(env.unauthenticatedContext().firestore(),root,'tribes'),f.orderBy('boss.level','desc'),f.limit(25))),'rankings remain authenticated');
  // Public directory and approval mode must be enforced by rules, even when a
  // modified browser skips the normal application and membership checks.
  const leaderA=await make('leaderA'),leaderB=await make('leaderB'),applicant=await make('applicant'),outsider=await make('outsider');
  const approvalA=await yes(leaderA.createTribe({name:'Approval Canopy A',admissionMode:'approval'}),'create approval tribe A'),approvalB=await yes(leaderB.createTribe({name:'Approval Canopy B',admissionMode:'approval'}),'create approval tribe B');
  const requestA=await yes(applicant.requestTribe(approvalA.id),'request approval tribe A'),requestB=await yes(applicant.joinTribe(approvalB.id),'invite code respects leader approval');
  assert.equal((await applicant.getPlayer()).tribeId,null);checks++;
  await yes(applicant.adapter.list('applications',[['uid','==','applicant']]),'applicant can inspect own request statuses');
  await yes(leaderA.adapter.list('applications',[['tribeId','==',approvalA.id]]),'leader can inspect request inbox');
  await no(outsider.adapter.get(`applications/${requestA.id}`),'outsider cannot read another application');
  await no(f.setDoc(f.doc(dbs.applicant,root,'applications','applicant-3'),{...requestA,id:'applicant-3',slot:3}),'fourth application slot rejected by rules');
  await no(f.updateDoc(f.doc(dbs.applicant,root,'applications',requestA.id),{status:'accepted',decidedBy:'applicant',updatedAt:Date.now()}),'applicant cannot self approve');
  await no(f.updateDoc(f.doc(dbs.leaderA,root,'applications',requestA.id),{status:'accepted',decidedBy:'leaderA',updatedAt:Date.now()}),'approval requires atomic membership writes');
  await no(applicant.adapter.transaction(async tx=>{const p=await tx.get('players/applicant'),t=await tx.get(`tribes/${approvalA.id}`);t.members.push('applicant');t.updatedAt=Date.now();p.tribeId=t.id;p.version++;p.updatedAt=Date.now();tx.set(`tribes/${t.id}`,t);tx.set('players/applicant',p);}),'approval tribe rejects direct open join');
  await yes(applicant.heartbeat({busy:true}),'applicant enters ordinary battle');await blocked(leaderA.reviewApplication(requestA.id,true),'leader cannot interrupt an online applicant battle');
  const bypassBase=createFirestoreSocialAdapter({db:dbs.leaderA,f}),bypassBusy=new SocialStore({...bypassBase,transaction:cb=>bypassBase.transaction(tx=>cb({...tx,get:async path=>{const value=await tx.get(path);return path==='presence/applicant'?{...value,busy:false}:value;}}))},{uid:'leaderA'});
  await no(bypassBusy.reviewApplication(requestA.id,true),'rules enforce busy applicant even when browser forges its local read');await yes(applicant.heartbeat({busy:false}),'applicant returns before approval');
  const competing=await Promise.allSettled([leaderA.reviewApplication(requestA.id,true),leaderB.reviewApplication(requestB.id,true)]);assert.equal(competing.filter(r=>r.status==='fulfilled').length,1);checks++;
  const joined=await applicant.getPlayer(),otherLeader=joined.tribeId===approvalA.id?leaderB:leaderA,otherRequest=joined.tribeId===approvalA.id?requestB:requestA;
  await yes(otherLeader.reviewApplication(otherRequest.id,false),'other leader can reject after applicant joined elsewhere');
  const cap1=await make('cap1'),cap2=await make('cap2'),capA=await yes(cap1.requestTribe(approvalA.id),'first last-place applicant'),capB=await yes(cap2.requestTribe(approvalA.id),'second last-place applicant');
  await env.withSecurityRulesDisabled(async ctx=>{const db=ctx.firestore(),ref=f.doc(db,root,'tribes',approvalA.id),t=(await f.getDoc(ref)).data();while(t.members.length<9)t.members.push(`capacity-fixture-${t.members.length}`);await f.updateDoc(ref,{members:t.members});});
  const lastPlace=await Promise.allSettled([leaderA.reviewApplication(capA.id,true),leaderA.reviewApplication(capB.id,true)]);assert.equal(lastPlace.filter(r=>r.status==='fulfilled').length,1);assert.equal((await leaderA.adapter.get(`tribes/${approvalA.id}`)).members.length,10);checks++;
  const expires=await yes(outsider.requestTribe(approvalB.id),'expiring request');
  await env.withSecurityRulesDisabled(ctx=>{const now=Date.now();return f.updateDoc(f.doc(ctx.firestore(),root,'applications',expires.id),{createdAt:now-604800001,expiresAt:now-1});});
  await blocked(leaderB.reviewApplication(expires.id,true),'expired request cannot be approved');
  await yes(outsider.expireApplications([await outsider.adapter.get(`applications/${expires.id}`)]),'ordinary client persists expired status');
  assert.equal((await outsider.adapter.get(`applications/${expires.id}`)).status,'expired');checks++;
  await yes(outsider.requestTribe(approvalB.id),'expired slot can be reused');
  const remaining=joined.tribeId===approvalB.id?leaderB:leaderA;
  await yes(remaining.deposit('fruit',4,'lock-test-fruit'),'fund escrow lock test');const locked=await yes(remaining.offerTrade('applicant',{asset:'fruit',amount:1},{asset:'berry',amount:1}),'pending trade locks both memberships');
  await blocked(applicant.leaveTribe(),'incoming trade blocks leave');
  await no(applicant.adapter.transaction(async tx=>{const p=await tx.get('players/applicant'),t=await tx.get(`tribes/${p.tribeId}`);t.members=t.members.filter(uid=>uid!=='applicant');t.updatedAt=Date.now();p.tribeId=null;p.version++;p.updatedAt=Date.now();tx.set(`tribes/${t.id}`,t);tx.set('players/applicant',p);}),'rules reject bypassed pending trade leave');
  await yes(applicant.cancelTrade(locked.id),'cancel clears both membership locks');await yes(applicant.leaveTribe(),'leave after resolving escrow');
  await no(f.setDoc(f.doc(dbs.outsider,root,'tribePublicActivity',approvalA.id),{id:approvalA.id,weekStart:am.weekStart,points:999999,updatedAt:Date.now()}),'outsider cannot forge public weekly metrics');
  await yes(outsider.listDirectory({search:'approval',availableOnly:true}),'public directory search uses real tribe records');
  const publicProfile=await yes(outsider.getPublicTribe(approvalB.id),'view another public tribe crew');assert.doesNotMatch(JSON.stringify(publicProfile),/saveId|wallet|location|route/);checks++;
  // Encrypted legacy saves remain writable. Sale receipts stay immutable and
  // sale ids survive stale clients, using no permanent monkey data in plaintext.
  const saveId='a'.repeat(64),saleId='b'.repeat(64),saveRef=f.doc(dbs.alice,'games_rogueduel','monkey_trails_v1','saves',saveId),saleRef=f.doc(dbs.alice,'games_rogueduel','monkey_trails_v1','sale_operations',saleId);
  await yes(f.setDoc(saveRef,{format:1,salt:'encrypted',cipher:'encrypted',revision:1}),'legacy encrypted save remains accepted');
  await yes(f.runTransaction(dbs.alice,async tx=>{await tx.get(saveRef);tx.update(saveRef,{saleIds:[saleId],revision:2});tx.set(saleRef,{saveId,fingerprint:'c'.repeat(64),createdAt:f.serverTimestamp()});}),'sale receipt and encrypted checkpoint commit atomically');
  await no(f.updateDoc(saleRef,{fingerprint:'d'.repeat(64)}),'sale receipt cannot be rewritten');await no(f.deleteDoc(saleRef),'sale receipt cannot be deleted');
  await no(f.updateDoc(saveRef,{saleIds:[],revision:3}),'stale checkpoint cannot erase sale ids');
  await yes(f.updateDoc(saveRef,{cipher:'next encrypted revision',revision:3}),'ordinary encrypted checkpoint preserves sale ids');
  console.log(JSON.stringify({checks,ownership:true,tribeCap:true,atomicTrading:true,duelTurns:true,nearby:true,cooldown:true},null,2));
 }finally{await env.cleanup();}
})().catch(e=>{console.error(e);process.exitCode=1;});
