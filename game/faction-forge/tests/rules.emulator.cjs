'use strict';
// Requires a running local Firestore emulator. Never points at production.
const fs=require('node:fs');const path=require('node:path');
const {initializeTestEnvironment,assertFails,assertSucceeds}=require('@firebase/rules-unit-testing');
const {doc,getDoc,setDoc,updateDoc,deleteDoc,collection,getDocs}=require('firebase/firestore');
(async()=>{
  if(!process.env.FIRESTORE_EMULATOR_HOST)throw new Error('Run through Firebase emulators:exec; FIRESTORE_EMULATOR_HOST is required.');
  const env=await initializeTestEnvironment({projectId:'demo-faction-forge',firestore:{rules:fs.readFileSync(path.resolve(__dirname,'../../../firestore.rules'),'utf8')}});
  try{
    await env.clearFirestore();
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
    // Firebase-only solo saves use a separate, private UID namespace.
    const {newSoloProfile,createFirebaseProgress}=await import('../firebase-progress.js');
    const {ALL_TROOPS,NEW_TROOPS}=require('../../../server/faction-forge/domain.cjs');
    const troops=Object.fromEntries(ALL_TROOPS.map(key=>[key,{unlockable:NEW_TROOPS.includes(key)}]));
    const profile=newSoloProfile('owner','Commander',troops),solo=doc(owner,'factionForgeSoloProfiles','owner');
    await assertSucceeds(setDoc(solo,profile));await assertSucceeds(getDoc(solo));
    await assertFails(getDoc(doc(attacker,'factionForgeSoloProfiles','owner')));
    await assertFails(setDoc(doc(attacker,'factionForgeSoloProfiles','owner'),profile));
    await assertFails(getDoc(doc(guest,'factionForgeSoloProfiles','owner')));
    await assertFails(getDocs(collection(owner,'factionForgeSoloProfiles')));
    await assertFails(updateDoc(solo,{forgeDust:-1}));
    await assertFails(updateDoc(solo,{crowncouncilElo:9999}));
    const firestore=require('firebase/firestore');
    const progress=createFirebaseProgress({auth:{currentUser:{uid:'owner'}},db:owner,api:firestore,username:'Commander',troops,storage:{getItem:()=>null,setItem(){},removeItem(){}}});
    try{
      await progress.loadProfile();
      await assertSucceeds(progress.recordSoloResult({id:'solo-win',mode:'campaign',missionId:'forge-01',won:true,lives:4}));
      await assertSucceeds(progress.recordSoloResult({id:'solo-win',mode:'campaign',missionId:'forge-01',won:true,lives:4}));
      if(progress.profile.forgeDust!==20)throw Error('Duplicate solo rewards were granted.');
      await assertSucceeds(progress.setRewardTarget('rifleman'));
      await assertSucceeds(updateDoc(solo,{forgeDust:100,'troops.rifleman.cards':10}));
      await assertSucceeds(progress.upgradeTroop('rifleman',{requestId:'solo-upgrade'}));
      if(progress.profile.forgeDust!==0||progress.profile.troops.rifleman.forgeTier!==2)throw Error('Solo upgrade cost was not applied.');
      await assertSucceeds(updateDoc(solo,{forgeDust:50}));
      await assertSucceeds(progress.buyPack('vanir',{requestId:'solo-pack'}));await assertSucceeds(progress.buyPack('vanir',{requestId:'solo-pack'}));
      if(progress.profile.forgeDust!==0||progress.profile.troops.rifleman.cards!==5)throw Error('Solo pack purchase duplicated spending or cards.');
      await assertSucceeds(progress.recordSoloRoundWin({id:'solo-rounds',mode:'bot',round:1}));await assertSucceeds(progress.recordSoloRoundWin({id:'solo-rounds',mode:'bot',round:1}));
      if(progress.profile.troops.rifleman.cards!==8)throw Error('Round win rewards were not idempotent.');
    }finally{progress.dispose();}
    await assertFails(updateDoc(doc(owner,'factionForgeSoloProfiles','owner','results','solo-win'),{forgeDust:9000}));
    await assertFails(getDoc(doc(attacker,'factionForgeSoloProfiles','owner','results','solo-win')));
    await assertFails(setDoc(doc(owner,'factionForgeSoloProfiles','owner','results','fake'),{uid:'attacker',id:'fake',mode:'bot'}));
    await assertFails(updateDoc(doc(owner,'factionForgeSoloProfiles','owner','purchases','solo-pack'),{dustSpent:0}));
    await assertFails(getDoc(doc(attacker,'factionForgeSoloProfiles','owner','purchases','solo-pack')));
    await assertFails(updateDoc(doc(owner,'factionForgeSoloProfiles','owner','rounds','solo-rounds-round-1'),{cards:[]}));
    const scopedPath=['factionForgeSoloProfiles','owner','accounts','Sean'],scopedRef=doc(owner,...scopedPath);
    const scoped=createFirebaseProgress({auth:{currentUser:{uid:'owner'}},db:owner,api:firestore,username:'Sean',troops,profilePath:scopedPath,queueNamespace:'.name-Sean',storage:{getItem:()=>null,setItem(){},removeItem(){}}});
    try{
      await assertSucceeds(scoped.loadProfile());await assertSucceeds(updateDoc(scopedRef,{forgeDust:150,'troops.rifleman.cards':10}));
      await assertSucceeds(scoped.buyPack('aesir',{requestId:'scoped-pack'}));await assertSucceeds(scoped.upgradeTroop('rifleman',{requestId:'scoped-upgrade'}));
      await assertSucceeds(scoped.recordSoloRoundWin({id:'scoped-match',mode:'bot',round:1}));await assertSucceeds(scoped.recordSoloResult({id:'scoped-match',mode:'bot',won:false}));
      if(scoped.profile.forgeDust!==0||scoped.profile.username!=='Sean')throw Error('Scoped account transaction failed.');
      if((await getDoc(solo)).data().username!=='Commander')throw Error('Scoped account changed another save.');
    }finally{scoped.dispose();}
    await assertFails(getDoc(doc(attacker,...scopedPath)));await assertFails(updateDoc(doc(attacker,...scopedPath),{forgeDust:9999}));
    await assertFails(getDoc(doc(attacker,...scopedPath,'rounds','scoped-match-round-1')));await assertFails(getDocs(collection(owner,'factionForgeSoloProfiles','owner','accounts')));
    await assertFails(updateDoc(doc(owner,...scopedPath,'purchases','scoped-pack'),{dustSpent:0}));await assertFails(deleteDoc(scopedRef));
    // Random packs must save through both the private guest and stable hub paths.
    for(const [profilePath,username] of [[['factionForgeSoloProfiles','owner'],'Commander'],[['users','PackCommander','factionForge','profile'],'PackCommander']]){
      if(profilePath[0]==='users')await setDoc(doc(owner,'users',username),{username});
      const randomClient=createFirebaseProgress({auth:{currentUser:{uid:'owner'}},db:owner,api:firestore,username,troops,profilePath,storage:{getItem:()=>null,setItem(){},removeItem(){}}});
      try{
        await randomClient.loadProfile();await updateDoc(doc(owner,...profilePath),{forgeDust:50});
        const first=await randomClient.buyPack('random-reinforcements',{requestId:'random-pack'});
        const repeat=await randomClient.buyPack('random-reinforcements',{requestId:'random-pack'});
        if(first.transaction.cards.length!==3||first.transaction.cards.reduce((sum,card)=>sum+card.quantity,0)!==15)throw Error('Random pack contents are incomplete.');
        if(JSON.stringify(first.transaction.cards)!==JSON.stringify(repeat.transaction.cards)||repeat.profile.forgeDust!==0)throw Error('Random pack rerolled or charged twice.');
        await updateDoc(doc(owner,...profilePath),{forgeDust:50});
        const parallel=await Promise.all([randomClient.buyPack('random-reinforcements',{requestId:'random-parallel'}),randomClient.buyPack('random-reinforcements',{requestId:'random-parallel'})]);
        if(JSON.stringify(parallel[0].transaction.cards)!==JSON.stringify(parallel[1].transaction.cards)||randomClient.profile.forgeDust!==0)throw Error('Concurrent random purchase was not recovered exactly once.');
        await assertFails(updateDoc(doc(owner,...profilePath,'purchases','random-pack'),{cards:[]}));
        const invalid={...first.transaction,uid:'owner',requestId:'invalid-random',packId:'random-reinforcements',dustSpent:0,completedAt:Date.now()};
        await assertFails(setDoc(doc(owner,...profilePath,'purchases','invalid-random'),invalid));
      }finally{randomClient.dispose();}
    }
    await assertFails(deleteDoc(solo));
    console.log('Faction Forge rules: authority, private scoped accounts, rewards, packs, retry and upgrade assertions passed.');
  }finally{await env.cleanup();}
})().catch(error=>{console.error(error);process.exitCode=1;});


