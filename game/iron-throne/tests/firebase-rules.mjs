// Runs only against the local demo emulator, never the user's Firebase project.
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {setupMeta,claimSeat,startCampaign} from '../multiplayer-rounds.mjs';
import {planFoundings} from '../founding.mjs';
import {applyCommand} from '../multiplayer-commands.mjs';
import {packCampaign,decodePayload,joinCampaign} from '../multiplayer-state.mjs';
const require=createRequire(process.env.IRON_FIREBASE_TEST_MODULES?`${process.env.IRON_FIREBASE_TEST_MODULES}/package.json`:import.meta.url);
const {initializeTestEnvironment,assertFails,assertSucceeds}=require('@firebase/rules-unit-testing');
const {doc,setDoc,getDoc,updateDoc,runTransaction,Timestamp,serverTimestamp}=require('firebase/firestore');
const env=await initializeTestEnvironment({projectId:'demo-iron-thrones',firestore:{host:'127.0.0.1',port:8089,rules:await readFile(new URL('../../../firestore.rules',import.meta.url),'utf8')}});
const lobby='rules-tests',root=`lobbies/${lobby}`,path=(collection,id)=>`${root}/${collection}/${id}`;
const db=uid=>env.authenticatedContext(uid).firestore();
try{
 await env.clearFirestore();
 const a=db('a'),b=db('b'),x=db('x');
 await assertSucceeds(setDoc(doc(a,root),{gameType:'ironthrone',hostUid:'a',host:'A',status:'waiting',players:['A'],members:{a:'A'}}));
 await assertSucceeds(updateDoc(doc(b,root),{members:{a:'A',b:'B'},players:['A','B']}));
 await assertFails(updateDoc(doc(b,root),{hostUid:'b'}));
 const meta=setupMeta({hostUid:'a'},Date.now());
 await assertSucceeds(setDoc(doc(a,path('iron_throne','meta')),meta));
 const claim=(database,uid,house)=>runTransaction(database,async tx=>{const ref=doc(database,path('iron_throne','meta')),snap=await tx.get(ref),m=snap.data();claimSeat(m,uid,uid.toUpperCase(),house);tx.set(ref,m);});
 const race=await Promise.allSettled([claim(a,'a','ashen'),claim(b,'b','ashen')]);assert.equal(race.filter(x=>x.status==='fulfilled').length,1);
 let actual=(await getDoc(doc(a,path('iron_throne','meta')))).data();
 const claimant=actual.seats.ashen.uid,claimDb=claimant==='a'?a:b;
 await assertSucceeds(updateDoc(doc(claimDb,path('iron_throne','meta')),{epoch:1,lease:{uid:claimant,token:'setup-controller',expiresAt:Timestamp.fromMillis(Date.now()+30000)}}));
 if(actual.seats.ashen.uid==='b'){await claim(b,'b','wintermere');await claim(a,'a','ashen');}else await claim(b,'b','wintermere');
 actual=(await getDoc(doc(a,path('iron_throne','meta')))).data();
 const hostile=structuredClone(actual);hostile.seats.ashen={kind:'human',uid:'b',name:'B',substitute:false};
 await assertFails(setDoc(doc(b,path('iron_throne','meta')),hostile));
 const state=startCampaign(actual,'a',Date.now());actual.epoch=2;actual.lease={uid:'a',token:'controller-a',expiresAt:Timestamp.fromMillis(Date.now()+30000)};
 const packed=await packCampaign(state,actual);
 await assertSucceeds(runTransaction(a,async tx=>{await tx.get(doc(a,path('iron_throne','meta')));tx.set(doc(a,path('iron_throne','meta')),actual);tx.set(doc(a,path('iron_throne','state')),packed.canonical);tx.set(doc(a,path('iron_throne','world')),packed.world);for(const [id,p]of Object.entries(packed.privateByHouse))tx.set(doc(a,path('iron_throne_private',id)),p);}));
 await assertSucceeds(getDoc(doc(b,path('iron_throne_private','wintermere'))));
 await assertFails(getDoc(doc(b,path('iron_throne_private','ashen'))));
 await assertFails(getDoc(doc(b,path('iron_throne','state'))));
 await assertFails(getDoc(doc(x,path('iron_throne','world'))));
 await assertFails(setDoc(doc(b,path('iron_throne','state')),{...packed.canonical,stateVersion:2}));
 await assertFails(setDoc(doc(b,path('iron_throne_private','ashen')),{...packed.privateByHouse.ashen,stateVersion:2}));
 const command={id:'valid-command',clientId:'browser-tab-1',sequence:1,uid:'b',actorHouseId:'wintermere',turn:0,stateVersion:1,epoch:2,type:'found',args:{tile:planFoundings(state)[0].capital},status:'pending',createdAt:serverTimestamp()};
 await assertSucceeds(getDoc(doc(b,path('iron_throne_commands',command.id))));
 await assertSucceeds(setDoc(doc(b,path('iron_throne_commands',command.id)),command));
 await assertFails(setDoc(doc(b,path('iron_throne_commands','forged-command')),{...command,id:'forged-command',actorHouseId:'ashen'}));
 await assertFails(updateDoc(doc(b,path('iron_throne_commands',command.id)),{args:{policy:'high'}}));

 // Gameplay is locked before the last capital exists; founding is the only command allowed.
 await assertFails(setDoc(doc(b,path('iron_throne_commands','early-ready')),{...command,id:'early-ready',type:'ready',args:{ready:true}}));
 const competing={...command,id:'rival-founding',uid:'a',actorHouseId:'ashen'};
 await assertSucceeds(setDoc(doc(a,path('iron_throne_commands',competing.id)),competing));
 async function processFounding(id){
  return runTransaction(a,async tx=>{
   const mr=doc(a,path('iron_throne','meta')),cr=doc(a,path('iron_throne_commands',id));
   const ms=await tx.get(mr),cs=await tx.get(cr),canonical=await tx.get(doc(a,path('iron_throne','state')));
   const privateDocs=await Promise.all(Object.keys(ms.data().seats).map(h=>tx.get(doc(a,path('iron_throne_private',h)))));
   const parts=Object.fromEntries(await Promise.all(privateDocs.map(async d=>[d.id,await decodePayload(d.data().payload)])));
   const current=joinCampaign(await decodePayload(canonical.data().payload),parts),m=ms.data();
   const result=applyCommand(current,m,cs.data(),{now:Date.now()});
   if(result.ok){m.stateVersion++;const packed=await packCampaign(current,m);tx.set(mr,m);tx.set(doc(a,path('iron_throne','state')),packed.canonical);tx.set(doc(a,path('iron_throne','world')),packed.world);for(const [h,p]of Object.entries(packed.privateByHouse))tx.set(doc(a,path('iron_throne_private',h)),p);}
   tx.update(cr,{status:result.ok?'accepted':'rejected',error:result.error||'',stateVersionApplied:m.stateVersion,resolvedAt:serverTimestamp()});
   return result.ok;
  });
 }
 const placements=await Promise.allSettled([processFounding(command.id),processFounding(competing.id)]);
 assert.equal(placements.filter(r=>r.status==='fulfilled'&&r.value).length,1,'conflicting capitals cannot both commit');
 // Rules can reject a stale full-snapshot commit before the SDK retries. The
 // command stays pending; the controller retries it against the committed world.
 for(let i=0;i<placements.length;i++)if(placements[i].status==='rejected'){
  assert.equal(placements[i].reason.code,'permission-denied');
  assert.equal(await processFounding([command.id,competing.id][i]),false);
 }
 for(const id of [command.id,competing.id])assert.notEqual((await getDoc(doc(a,path('iron_throne_commands',id)))).data().status,'pending');
 const after=(await getDoc(doc(a,path('iron_throne','meta')))).data();assert.equal(after.turn,0);assert.equal(after.phase,'founding');assert.equal(after.stateVersion,2);
 const shared=await decodePayload((await getDoc(doc(a,path('iron_throne','world')))).data().payload);
 assert.equal(Object.values(shared.founding.houses).filter(h=>h.founded).length,1);
 const remaining=shared.founding.houses.ashen.founded?'wintermere':'ashen',database=remaining==='ashen'?a:b,uid=remaining==='ashen'?'a':'b';
 const nextCommand={...command,id:'final-human-founding',uid,actorHouseId:remaining,sequence:2,stateVersion:2,args:{tile:planFoundings(shared).find(p=>p.owner===remaining).capital}};
 await assertSucceeds(setDoc(doc(database,path('iron_throne_commands',nextCommand.id)),nextCommand));
 assert.equal(await processFounding(nextCommand.id),true);
 const complete=(await getDoc(doc(a,path('iron_throne','meta')))).data();assert.equal(complete.turn,1);assert.equal(complete.phase,'planning');
 await assertFails(setDoc(doc(b,path('iron_throne_commands','late-found')),{...command,id:'late-found',turn:1,stateVersion:3}));
 const playingCommand={...command,id:'planning-tax',turn:1,stateVersion:3,type:'tax',args:{policy:'low'}};
 await assertSucceeds(setDoc(doc(b,path('iron_throne_commands',playingCommand.id)),playingCommand));
 await assertFails(getDoc(doc(a,path('iron_throne_private','unknown-house'))));
 await assertSucceeds(setDoc(doc(a,path('iron_throne_presence','a')),{uid:'a',name:'A',at:serverTimestamp()}));
 await assertFails(setDoc(doc(b,path('iron_throne_presence','a')),{uid:'a',name:'A',at:serverTimestamp()}));
 // Expire the old lease as a fixture, then acquire it as the other authenticated ruler.
 await env.withSecurityRulesDisabled(async ctx=>{await updateDoc(doc(ctx.firestore(),path('iron_throne','meta')),{lease:{...actual.lease,expiresAt:Timestamp.fromMillis(Date.now()-1)}});});
 const current=(await getDoc(doc(b,path('iron_throne','meta')))).data();
 await assertSucceeds(updateDoc(doc(b,path('iron_throne','meta')),{epoch:3,lease:{uid:'b',token:'controller-b',expiresAt:Timestamp.fromMillis(Date.now()+30000)}}));
 await assertFails(updateDoc(doc(a,path('iron_throne','meta')),{epoch:2,lease:actual.lease}));
 await assertFails(updateDoc(doc(a,path('iron_throne_commands',command.id)),{status:'accepted',error:'',stateVersionApplied:1,resolvedAt:serverTimestamp()}));
 await assertSucceeds(getDoc(doc(b,path('iron_throne','state'))));
 // The additive namespace restriction must leave an existing game working.
 await assertSucceeds(setDoc(doc(a,'lobbies/legacy-game'),{gameType:'turncraft',status:'started',players:['A']}));
 await assertSucceeds(setDoc(doc(a,'lobbies/legacy-game/turncraft/state'),{turn:1}));
 await assertSucceeds(setDoc(doc(a,'lobbies/legacy-without-type'),{status:'started'}));
 console.log('PASS atomic conflicting founding, phase gates, all-six transition and Firestore rules: UID membership, concurrent seats, private data, command forgery, controller fencing, legacy paths');
}finally{await env.cleanup();}
