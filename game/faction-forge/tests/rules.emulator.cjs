'use strict';
// Requires a running local Firestore emulator. Never points at production.
const fs=require('node:fs');const path=require('node:path');
const {initializeTestEnvironment,assertFails,assertSucceeds}=require('@firebase/rules-unit-testing');
const {doc,getDoc,setDoc,updateDoc,deleteDoc,collection,getDocs}=require('firebase/firestore');
(async()=>{
  if(!process.env.FIRESTORE_EMULATOR_HOST)throw new Error('Run through Firebase emulators:exec; FIRESTORE_EMULATOR_HOST is required.');
  const env=await initializeTestEnvironment({projectId:'demo-faction-forge',firestore:{rules:fs.readFileSync(path.resolve(__dirname,'../../../firestore.rules'),'utf8')}});
  try{
    await env.withSecurityRulesDisabled(async ctx=>{
      const db=ctx.firestore();
      await setDoc(doc(db,'factionForgeNames','Legacy'),{uid:'owner',username:'Legacy',verified:true});
      await setDoc(doc(db,'factionForgeAccounts','owner'),{uid:'owner',username:'Legacy',verified:true});
      await setDoc(doc(db,'factionForgeProfiles','owner'),{uid:'owner',username:'Legacy',forgeDust:100,crowncouncilElo:1750});
      await setDoc(doc(db,'factionForgeProfiles','owner','history','complete'),{sessionId:'complete',forgeDust:20});
      await setDoc(doc(db,'factionForgeMatches','complete'),{participants:['owner','peer'],winnerSide:0});
      await setDoc(doc(db,'users','Legacy'),{username:'Legacy',crowncouncilElo:1750,wins:9,bio:'Before'});
      await setDoc(doc(db,'users','Unmigrated'),{username:'Unmigrated',crowncouncilElo:1200,bio:'Original'});
      await setDoc(doc(db,'lobbies','secure-room'),{gameType:'crowncouncil',players:['Legacy'],host:'Legacy',hostUid:'owner',members:{owner:'Legacy'},visibility:'public',forgeRanked:true,status:'waiting'});
      await setDoc(doc(db,'lobbies','hosted'),{gameType:'crowncouncil',players:['Legacy','Peer'],host:'Legacy',status:'started'});
    });
    const owner=env.authenticatedContext('owner').firestore(),attacker=env.authenticatedContext('attacker').firestore(),guest=env.unauthenticatedContext().firestore();
    await assertSucceeds(getDoc(doc(owner,'factionForgeProfiles','owner')));
    await assertSucceeds(getDoc(doc(owner,'factionForgeProfiles','owner','history','complete')));
    await assertFails(getDoc(doc(attacker,'factionForgeProfiles','owner')));
    await assertFails(getDocs(collection(owner,'factionForgeProfiles')));
    await assertFails(getDoc(doc(guest,'factionForgeProfiles','owner')));
    await assertFails(updateDoc(doc(owner,'factionForgeProfiles','owner'),{forgeDust:999999}));
    await assertFails(updateDoc(doc(owner,'factionForgeProfiles','owner'),{crowncouncilElo:9999}));
    await assertFails(setDoc(doc(owner,'factionForgeProfiles','owner','history','fake'),{forgeDust:20}));
    await assertFails(setDoc(doc(owner,'factionForgeProfiles','owner','upgrades','fake'),{forgeTier:3}));
    await assertFails(setDoc(doc(owner,'factionForgeMatches','fake'),{participants:['owner'],winnerSide:0}));
    await assertFails(getDoc(doc(owner,'factionForgeMatches','complete')));
    await assertFails(updateDoc(doc(attacker,'factionForgeMatches','complete'),{winnerSide:1}));
    await assertFails(setDoc(doc(attacker,'factionForgeNames','Legacy'),{uid:'attacker'}));
    await assertFails(setDoc(doc(owner,'factionForgeAccounts','owner'),{verified:true,username:'Other'}));
    await assertFails(setDoc(doc(owner,'factionForgeLobbySessions','hosted'),{sessionId:'forged'}));
    await assertSucceeds(getDoc(doc(owner,'factionForgeAccounts','owner')));
    await assertFails(getDoc(doc(attacker,'factionForgeAccounts','owner')));
    await assertSucceeds(updateDoc(doc(owner,'users','Legacy'),{bio:'Updated by owner',favoriteGames:['crowncouncil']}));
    await assertFails(updateDoc(doc(attacker,'users','Legacy'),{bio:'Impersonated'}));
    await assertFails(updateDoc(doc(owner,'users','Legacy'),{crowncouncilElo:2500}));
    await assertFails(updateDoc(doc(attacker,'users','Unmigrated'),{crowncouncilElo:2500}));
    await assertFails(updateDoc(doc(attacker,'users','Legacy'),{password:'replacement-from-unrelated-session'}));
    await assertFails(deleteDoc(doc(owner,'users','Legacy')));
    await assertSucceeds(updateDoc(doc(attacker,'users','Unmigrated'),{bio:'Existing unrelated compatibility'}));
    await assertSucceeds(setDoc(doc(owner,'lobbies','hosted','crowncouncil','state'),{phase:'battle',winner:'Legacy'}));
    // Legacy hosted snapshots still work, but cannot write protected authority.
    await assertFails(updateDoc(doc(owner,'users','Legacy'),{crowncouncilLastDelta:50}));
    // Public UID seats may be claimed only by their own authenticated owner.
    await assertSucceeds(updateDoc(doc(attacker,'lobbies','secure-room'),{members:{owner:'Legacy',attacker:'Attacker'},players:['Legacy','Attacker'],updatedAt:1}));
    await assertFails(updateDoc(doc(attacker,'lobbies','secure-room'),{hostUid:'attacker',host:'Attacker'}));
    await assertFails(updateDoc(doc(attacker,'lobbies','secure-room'),{members:{owner:'Impersonated',attacker:'Attacker'},players:['Impersonated','Attacker']}));
    await assertFails(updateDoc(doc(attacker,'lobbies','secure-room'),{visibility:'private'}));
    await assertFails(updateDoc(doc(attacker,'lobbies','secure-room'),{forgeRanked:false}));
    await assertFails(updateDoc(doc(attacker,'lobbies','secure-room'),{gameType:'other-game'}));
    await assertFails(deleteDoc(doc(attacker,'lobbies','secure-room')));
    await assertSucceeds(updateDoc(doc(owner,'lobbies','secure-room'),{forgeRanked:false,updatedAt:2}));
    await assertSucceeds(updateDoc(doc(owner,'lobbies','secure-room'),{status:'started',updatedAt:3}));
    await assertFails(updateDoc(doc(owner,'lobbies','secure-room'),{forgeRanked:true,updatedAt:4}));
    await assertFails(updateDoc(doc(attacker,'lobbies','hosted'),{hostUid:'attacker',members:{attacker:'Legacy'}}));
    await assertFails(setDoc(doc(attacker,'lobbies','forged-room'),{gameType:'crowncouncil',hostUid:'owner',host:'Legacy',members:{owner:'Legacy'},players:['Legacy'],visibility:'public',status:'waiting'}));
    await assertSucceeds(setDoc(doc(attacker,'lobbies','own-room'),{gameType:'crowncouncil',hostUid:'attacker',host:'Attacker',members:{attacker:'Attacker'},players:['Attacker'],visibility:'public',status:'waiting'}));
    await assertSucceeds(setDoc(doc(owner,'lobbies','secure-room','crowncouncil','state'),{phase:'battle'}));
    const stranger=env.authenticatedContext('stranger').firestore();
    await assertFails(getDoc(doc(stranger,'lobbies','secure-room','crowncouncil','state')));
    await assertFails(updateDoc(doc(stranger,'lobbies','secure-room','crowncouncil','state'),{winner:'Stranger'}));
    console.log('Faction Forge emulator rules: 44 ownership, authority, compatibility and protected UID-lobby assertions passed.');
  }finally{await env.cleanup();}
})().catch(error=>{console.error(error);process.exitCode=1;});
