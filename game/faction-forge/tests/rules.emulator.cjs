'use strict';
// Requires a running local Firestore emulator. Never points at production.
const fs=require('node:fs');const path=require('node:path');
const {initializeTestEnvironment,assertFails,assertSucceeds}=require('@firebase/rules-unit-testing');
const {doc,getDoc,setDoc,updateDoc,deleteDoc,deleteField,collection,getDocs}=require('firebase/firestore');
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
    await assertSucceeds(setDoc(doc(owner,'lobbies','hosted','crowncouncil','state'),{phase:'waiting',winner:'Legacy'}));
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
    await assertSucceeds(setDoc(doc(owner,'lobbies','secure-room','crowncouncil','state'),{phase:'waiting'}));
    const vanirDeck={id:'starter-vanir',name:'Iron Vanguard',faction:'Vanir',troops:['vguard','marksman','suppressor','vrocket','medic']},helionDeck={id:'starter-helions',name:'Blight Swarm',faction:'Helions',troops:['hellspawn','roach','defiler','spitter','blood']};
    const staticRef=doc(owner,'lobbies','secure-room','crowncouncil','state');
    const lockedState={sessionKey:'deck-rules',phase:'draft',factions:{p0:'Vanir',p1:'Helions'},decks:{p0:vanirDeck,p1:helionDeck},roster:{p0:[{type:'vguard',count:1,lvl:1},{type:'marksman',count:1,lvl:1}],p1:[{type:'hellspawn',count:1,lvl:1}]}};
    for(const count of [256,Number.MAX_SAFE_INTEGER])await assertFails(setDoc(staticRef,{...lockedState,roster:{...lockedState.roster,p0:[{type:'vguard',count,lvl:1}]}}));
    await assertSucceeds(setDoc(staticRef,lockedState));
    console.log('Faction Forge rules: initial roster growth guard passed.');
    await assertFails(updateDoc(staticRef,{'decks.p0':{...vanirDeck,name:'Changed during match'}}));
    await assertFails(updateDoc(doc(attacker,'lobbies','secure-room','crowncouncil','state'),{'decks.p1':{...helionDeck,troops:['hellspawn','roach','defiler','viper','blood']}}));
    for(const count of [6,11,16,19,24,29,34,39,44,49,54,59,64,66]){
      await assertSucceeds(updateDoc(staticRef,{'roster.p0':[{type:'vguard',count,lvl:1}]}));
      if(count===6)for(const giantCount of [256,Number.MAX_SAFE_INTEGER])await assertFails(updateDoc(staticRef,{'roster.p0':[{type:'vguard',count:giantCount,lvl:1}]}));
    }
    await assertFails(updateDoc(staticRef,{sessionKey:'oversized-reset','roster.p0':[{type:'vguard',count:66,lvl:1}]}));
    await assertFails(updateDoc(staticRef,{phase:'faction-select','roster.p0':[{type:'vguard',count:6,lvl:1}]}));
    await assertFails(updateDoc(staticRef,{'roster.p0':[{type:'vguard',count:-1,lvl:1}]}));
    await assertFails(updateDoc(staticRef,{'roster.p0':[{type:'miner',count:1,lvl:1}]}));
    await assertSucceeds(updateDoc(staticRef,{'roster.p0':[{type:'vguard',count:1,lvl:2},{type:'marksman',count:3,lvl:2},{type:'medic',count:1,lvl:1}]}));
    await assertSucceeds(updateDoc(staticRef,{roster:{p0:vanirDeck.troops.map(type=>({type,count:1,lvl:3})),p1:helionDeck.troops.map(type=>({type,count:1,lvl:3}))}}));
    console.log('Faction Forge rules: incremental recruitment and five-type roster checks passed.');
    const battleRef=doc(owner,'lobbies','secure-room','crowncouncil','battle'),legalUnits=[...vanirDeck.troops.map(type=>({type,p:0,hp:10})),...helionDeck.troops.map(type=>({type,p:1,hp:10}))];
    await assertSucceeds(setDoc(battleRef,{units:legalUnits,sessionKey:'deck-rules'}));
    await assertFails(updateDoc(battleRef,{units:[...legalUnits,{type:'vguard',p:0,hp:10}]}));
    await assertFails(updateDoc(battleRef,{units:legalUnits.map((unit,index)=>index===9?{type:'vguard',p:0,hp:10}:unit)}));
    await assertFails(updateDoc(battleRef,{units:[{type:'miner',p:0,hp:10}]}));
    await assertFails(updateDoc(doc(attacker,'lobbies','secure-room','crowncouncil','battle'),{units:legalUnits}));
    for(const [p0,p1] of [[10,10],[15,15],[20,20],[25,25],[30,30],[35,31]])await assertSucceeds(updateDoc(staticRef,{roster:{p0:[{type:'vguard',count:p0,lvl:1}],p1:[{type:'hellspawn',count:p1,lvl:1}]}}));
    const growingGroups={p0:{vguard:Array.from({length:35},(_,i)=>({id:'v-'+i,hp:10}))},p1:{hellspawn:Array.from({length:31},(_,i)=>({id:'h-'+i,hp:10}))}};
    await assertSucceeds(setDoc(battleRef,{unitGroups:growingGroups,sessionKey:'deck-rules'}));
    await assertFails(updateDoc(battleRef,{unitGroups:{...growingGroups,p0:{vguard:Array.from({length:256},(_,i)=>({id:'overflow-'+i,hp:10}))}}}));
    await assertFails(updateDoc(battleRef,{unitGroups:{...growingGroups,p0:{...growingGroups.p0,miner:[{id:'offdeck',hp:10}]}}}));
    await assertFails(updateDoc(battleRef,{unitGroups:{...growingGroups,p0:{vguard:{id:'not-a-list',hp:10}}}}));
    await assertFails(updateDoc(battleRef,{units:legalUnits}));
    await assertFails(updateDoc(doc(attacker,'lobbies','secure-room','crowncouncil','battle'),{unitGroups:growingGroups}));
    console.log('Faction Forge rules: grouped army snapshots and body budget checks passed.');
    const afterimageDeck={id:'afterimage-test',name:'Afterimage',faction:'Aesir',troops:['guardian','phaser','storm','runebinder','wardmaker']},broodDeck={id:'brood-test',name:'Brood',faction:'Helions',troops:['roach','matron','defiler','spitter','carrion']};
    await assertSucceeds(setDoc(staticRef,{...lockedState,sessionKey:'summon-rules',phase:'faction-select',factions:{p0:'Aesir',p1:'Helions'},decks:{p0:afterimageDeck,p1:broodDeck},roster:{p0:[],p1:[]}}));
    await assertSucceeds(setDoc(battleRef,{units:[{type:'phaser',p:0,hp:10},{type:'miragewarden',p:0,hp:4,dmg:0,illusion:true},{type:'matron',p:1,hp:10},{type:'hellspawn',p:1,hp:4,summoned:true}],sessionKey:'summon-rules'}));
    await assertFails(updateDoc(battleRef,{units:[{type:'miragewarden',p:0,hp:4,dmg:9,illusion:true}]}));
    await assertSucceeds(setDoc(battleRef,{unitGroups:{p0:{phaser:[{id:'caster',hp:10}],miragewarden:[{id:'decoy',hp:4,dmg:0}]},p1:{matron:[{id:'brood',hp:10}],hellspawn:[{id:'summon',hp:4}],blood:[{id:'explosive',hp:4}]}},sessionKey:'summon-rules'}));
    await assertFails(updateDoc(battleRef,{unitGroups:{p0:{miragewarden:'not a body list'},p1:{}}}));
    await assertFails(updateDoc(battleRef,{unitGroups:{p0:{blood:[{id:'wrong-summon-side',hp:4}]},p1:{}}}));
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
      const custom={id:'rules-custom',name:'Cloud Precision',faction:'Vanir',troops:['vguard','marksman','rifleman','vrocket','medic']};
      await assertSucceeds(progress.saveDeck(custom));await assertSucceeds(progress.selectDeck('starter-aesir'));
      const savedDecks=(await getDoc(solo)).data().decks;
      await assertFails(updateDoc(solo,{decks:[...savedDecks,{...custom,id:'rules-duplicate',troops:['vguard','vguard','rifleman','vrocket','medic']}]}));
      await assertFails(updateDoc(solo,{decks:[...savedDecks,{...custom,id:'rules-locked',troops:['vguard','marksman','railbreaker','vrocket','medic']}]}));
      const railDeck={...custom,id:'unlocked-selection',troops:['vguard','marksman','railbreaker','vrocket','medic']};
      await assertSucceeds(setDoc(staticRef,{...lockedState,sessionKey:'unlock-selection',phase:'faction-select',decks:{p0:null,p1:null},factions:{p0:null,p1:null},roster:{p0:[],p1:[]}}));
      await assertFails(updateDoc(staticRef,{'decks.p0':railDeck,'factions.p0':'Vanir'}));
      await assertSucceeds(updateDoc(solo,{'troops.railbreaker.unlocked':true}));
      await assertSucceeds(updateDoc(staticRef,{'decks.p0':railDeck,'factions.p0':'Vanir'}));
      for(let i=0;i<8;i++)await assertSucceeds(progress.saveDeck({...custom,id:'capacity-'+i,name:'Capacity '+i}));
      await assertSucceeds(progress.saveDeck({...custom,name:'Edited at capacity'}));
      if(progress.profile.decks.length!==12)throw Error('The deck library did not reach its expected capacity.');
      await assertSucceeds(setDoc(doc(owner,'users','DeckMigration','factionForge','profile'),{...progress.profile,username:'DeckMigration'}));
      try{await progress.saveDeck({...custom,id:'over-capacity'});throw Error('Deck overflow was accepted.');}catch(error){if(error.code!=='DECK_LIMIT')throw error;}
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
    // Exercise the actual client against both supported save locations. The
    // deployed pre-deck rules rejected these complete post-migration profiles.
    const deckDb=env.authenticatedContext('deck-owner').firestore();
    for(const [profilePath,username] of [[['factionForgeSoloProfiles','deck-owner'],'Deck Guest'],[['users','DeckCommander','factionForge','profile'],'DeckCommander']]){
      if(profilePath[0]==='users')await assertSucceeds(setDoc(doc(deckDb,'users',username),{username}));
      const ref=doc(deckDb,...profilePath),legacy=newSoloProfile('deck-owner',username,troops);
      delete legacy.decks;delete legacy.selectedDeckId;delete legacy.selectedDeckIndex;
      legacy.forgeDust=73;legacy.wins=5;legacy.troops.rifleman.cards=9;
      legacy.campaign.missions['forge-01']={completed:true,stars:3,clears:2};
      await assertSucceeds(setDoc(ref,legacy));
      await assertSucceeds(updateDoc(ref,{forgeDust:74}));
      const options={auth:{currentUser:{uid:'deck-owner'}},db:deckDb,api:firestore,username,troops,profilePath,storage:{getItem:()=>null,setItem(){},removeItem(){}}};
      const deckClient=createFirebaseProgress(options);
      const custom={id:'persistent-deck',name:'Saved Precision',faction:'Vanir',troops:['vguard','marksman','rifleman','vrocket','medic']};
      try{
        await assertSucceeds(deckClient.loadProfile());
        if(deckClient.profile.forgeDust!==74||deckClient.profile.wins!==5||deckClient.profile.troops.rifleman.cards!==9||deckClient.profile.campaign.missions['forge-01'].stars!==3)throw Error('Deck migration changed existing progress.');
        await assertSucceeds(deckClient.saveDeck(custom));
        await assertSucceeds(deckClient.saveDeck({...custom,name:'Edited Precision'}));
        for(const id of ['starter-vanir','starter-aesir','starter-helions',custom.id])await assertSucceeds(deckClient.selectDeck(id));
      }finally{deckClient.dispose();}
      const reloaded=createFirebaseProgress(options);
      try{
        await assertSucceeds(reloaded.loadProfile());
        if(reloaded.profile.selectedDeckId!==custom.id||reloaded.profile.selectedDeckIndex!==3||reloaded.profile.decks[3].name!=='Edited Precision')throw Error('Deck selection or edits did not survive reopening.');
      }finally{reloaded.dispose();}
      const saved=(await getDoc(ref)).data();
      await assertFails(updateDoc(ref,{decks:deleteField(),selectedDeckId:deleteField(),selectedDeckIndex:deleteField()}));
      await assertFails(updateDoc(ref,{selectedDeckId:'missing-deck'}));
      await assertFails(updateDoc(ref,{selectedDeckIndex:99}));
      await assertFails(updateDoc(ref,{decks:[{...saved.decks[0],name:'Overwritten starter'},...saved.decks.slice(1)]}));
      for(const [id,keys] of [['duplicate-troops',['vguard','vguard','rifleman','vrocket','medic']],['foreign-troop',['guardian','marksman','rifleman','vrocket','medic']],['locked-troop',['railbreaker','marksman','rifleman','vrocket','medic']]]){
        const invalid={...custom,id,troops:keys};
        await assertFails(setDoc(ref,{...saved,decks:[...saved.decks,invalid],selectedDeckId:id,selectedDeckIndex:saved.decks.length}));
      }
      await assertSucceeds(updateDoc(ref,{forgeDust:75}));
      const after=(await getDoc(ref)).data();
      if(after.decks.length!==4||after.selectedDeckId!==custom.id||after.forgeDust!==75)throw Error('Rejected deck writes changed saved progress.');
    }
    await assertFails(deleteDoc(solo));
    console.log('Faction Forge rules: authority, private scoped accounts, rewards, packs, retry, upgrades, legacy deck migration and persistent deck selections passed.');
  }finally{await env.cleanup();}
})().catch(error=>{console.error(error);process.exitCode=1;});


