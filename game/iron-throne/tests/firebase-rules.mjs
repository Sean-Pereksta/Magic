// Runs only against the local demo emulator, never the user's Firebase project.
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {setupMeta,claimSeat,startCampaign} from '../multiplayer-rounds.mjs';
import {packCampaign} from '../multiplayer-state.mjs';
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
 const command={id:'valid-command',clientId:'browser-tab-1',sequence:1,uid:'b',actorHouseId:'wintermere',turn:1,stateVersion:1,epoch:2,type:'tax',args:{policy:'low'},status:'pending',createdAt:serverTimestamp()};
 await assertSucceeds(getDoc(doc(b,path('iron_throne_commands',command.id))));
 await assertSucceeds(setDoc(doc(b,path('iron_throne_commands',command.id)),command));
 await assertFails(setDoc(doc(b,path('iron_throne_commands','forged-command')),{...command,id:'forged-command',actorHouseId:'ashen'}));
 await assertFails(updateDoc(doc(b,path('iron_throne_commands',command.id)),{args:{policy:'high'}}));
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
 console.log('PASS Firestore rules: UID membership, concurrent seats, private data, command forgery, controller fencing, legacy paths');
}finally{await env.cleanup();}
