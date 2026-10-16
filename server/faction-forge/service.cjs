'use strict';
const { randomUUID, randomBytes } = require('node:crypto');
const { gzipSync, gunzipSync } = require('node:zlib');
const D = require('./domain.cjs');
const { createCombatAdapter, loadModules } = require('./simulator.cjs');
const clone=value=>JSON.parse(JSON.stringify(value));
function createForgeService({db,auth,now=Date.now,modules,adapterFactory=createCombatAdapter}) {
  const profileRef=uid=>db.doc(`factionForgeProfiles/${uid}`);
  const sessionRef=id=>db.doc(`factionForgeMatches/${id}`);
  const receiptRef=(id,uid)=>db.doc(`factionForgeProfiles/${uid}/history/${id}`);
  let shared=modules?Promise.resolve(modules):null;
  const sharedContent=()=>shared||(shared=loadModules());
  const deckRules=async()=> (await sharedContent()).decks||(await loadModules()).decks;
  async function normalizeProfile(p){const rules=await deckRules(),decks=rules.normalizeDecks(p.decks),defaults=D.newProfile(p.uid,p.username).troops,selectedDeckIndex=Math.max(0,decks.findIndex(deck=>deck.id===p.selectedDeckId));return {...p,troops:Object.fromEntries(D.ALL_TROOPS.map(key=>[key,{...defaults[key],...p.troops?.[key]}])),decks,selectedDeckId:decks[selectedDeckIndex].id,selectedDeckIndex};}
  async function requestedDeck(request,p,{saving=false}={}){
    const rules=await deckRules(),decks=rules.normalizeDecks(p.decks);
    const supplied=request.deck||decks.find(deck=>deck.id===request.deckId)||(!request.deckId&&(request.faction?decks.find(deck=>deck.faction===request.faction):decks.find(deck=>deck.id===p.selectedDeckId)))||null;
    const checked=rules.validateDeck(supplied,{unlockedTypes:owned(p)});
    D.fail(checked.valid,'INVALID_DECK',checked.errors.join(' '));
    D.fail(!request.faction||request.faction===checked.deck.faction,'INVALID_DECK','The deck faction does not match the requested faction.');
    if(!saving&&!p.activeSessionId)D.fail(decks.some(deck=>JSON.stringify(deck)===JSON.stringify(checked.deck)),'DECK_NOT_SAVED','Save this deck before starting a match.');
    return clone(checked.deck);
  }
  async function normalizeSessionDecks(session){
    const rules=await deckRules();session.decks=session.decks||{};
    for(const side of ['p0','p1'])if(session.factions[side]){
      const prior=session.decks[side],fallback=rules.STARTER_DECKS.find(deck=>deck.faction===session.factions[side]);
      const checked=rules.validateDeck(prior||fallback,{unlockedTypes:owned(session.profiles[side]||D.newProfile('bot','AI'))});
      D.fail(checked.valid,'INVALID_DECK','The locked match deck is invalid. Start a new match.');
      session.decks[side]=clone(checked.deck);session.roster[side]=rules.sanitizeRoster(session.roster[side],checked.deck);
    }
    return session;
  }
  async function identity(token) {
    D.fail(typeof token==='string'&&token.length>0,'UNAUTHENTICATED','Sign in to save Faction Forge progress.',401);
    const decoded=await auth.verifyIdToken(token,true);
    const account=await db.doc(`factionForgeAccounts/${decoded.uid}`).get();
    D.fail(account.exists&&account.data().verified===true,'ACCOUNT_NOT_LINKED','This Firebase account needs a verified CatnMice ownership binding before progression can be saved.',403);
    const binding=account.data();D.safeUsername(binding.username);
    const name=await db.doc(`factionForgeNames/${binding.username}`).get();
    D.fail(name.exists&&name.data().uid===decoded.uid,'ACCOUNT_NOT_LINKED','Account ownership could not be verified.',403);
    return {uid:decoded.uid,username:binding.username};
  }
  async function profile(who) {
    return db.runTransaction(async tx=>{
      const ref=profileRef(who.uid),snap=await tx.get(ref);
      if(snap.exists){const next=await normalizeProfile(snap.data());if(JSON.stringify(next)!==JSON.stringify(snap.data()))tx.set(ref,next);return {...next,upgradeCosts:clone(D.FORGE_COSTS)};}
      const legacy=await tx.get(db.doc(`users/${who.username}`));
      const created=await normalizeProfile(D.newProfile(who.uid,who.username,legacy.data()?.crowncouncilElo,now()));tx.create(ref,created);return {...created,upgradeCosts:clone(D.FORGE_COSTS)};
    });
  }
  function owned(profile) {return D.ALL_TROOPS.filter(key=>profile.troops[key]?.unlocked);}
  async function engine(session) {return adapterFactory({seed:session.seed+':'+session.revision,modules:await sharedContent()});}
  function newState({id,mode,faction,deck,who,profile,missionNumber,missionId,mission}) {
    const created=now();
    return {schemaVersion:1,id,mode,missionNumber:missionNumber||null,missionId:missionId||null,seed:randomUUID(),ranked:false,revision:0,
      isAI:mode!=='pvp',aiDifficulty:mission?.difficulty||.65,aiStyle:mission?.aiStyle||mission?.opponent?.aiStyle||'early_combos',estimatedMmr:mission?.estimatedMmr||null,participants:[who.uid,null],players:[who.username,mission?.opponent?.name||'IronWarden'],
      profiles:{p0:clone(profile),p1:D.newProfile('bot','AI',profile.crowncouncilElo)},
      factions:{p0:faction,p1:mission?.aiFaction||mission?.opponent?.faction||mission?.faction||'Helions'},decks:{p0:clone(deck),p1:clone(mission?.opponent?.deck||mission?.deck||null)},roster:{p0:[],p1:[]},relics:{p0:[],p1:[]},tactics:{p0:[],p1:[]},formations:{p0:'balanced',p1:'balanced'},
      lives:[4,4],initialMmr:[profile.crowncouncilElo,profile.crowncouncilElo],mmr:[profile.crowncouncilElo,profile.crowncouncilElo],mmrChange:[0,0],currentPacks:{p0:[],p1:[]},selections:{},draftIndex:0,round:1,
      phase:'draft',bonusFor:null,phaseEndsAt:created+60000,deadlineAt:created+60000,createdAt:created,updatedAt:created,expiresAt:created+2*60*60*1000,battleReadyAt:0,battle:null,battleCount:0,completedBattles:0,roundWins:[0,0],serverValidated:false,processedRequests:{},winnerSide:null};
  }
  async function makePacks(session,adapter) {
    await normalizeSessionDecks(session);
    session.phaseEndsAt=now()+60000;session.deadlineAt=session.phaseEndsAt;
    for(const side of ['p0','p1']) {
      session.currentPacks[side]=session.bonusFor&&session.bonusFor!==side?[]:adapter.pack(session,side,owned(session.profiles[side]));
    }
  }
  async function startSolo(who,request) {
    D.onlyKeys(request,['mode','missionId','faction','deck','deckId']);
    D.fail(['campaign','bot','endless'].includes(request.mode),'INVALID_MODE','Choose a solo mode.');
    const content=await sharedContent();
    const initial=await profile(who);
    const deck=await requestedDeck(request,initial);request={...request,faction:deck.faction,deck};
    if(initial.activeSessionId) return getSession(who,initial.activeSessionId);
    const options=D.canStart(initial,request);
    const mission=options.missionNumber?content.campaign.getMission(options.missionId):request.mode==='endless'?content.campaign.createEndlessMission((initial.campaign.endlessBest||0)+1):content.campaign.practiceOpponent(initial.crowncouncilElo);
    if(request.mode==='bot'&&content.campaign.chooseAIDeck)mission.deck=content.campaign.chooseAIDeck(mission.faction||'Helions',deck,mission.difficulty||.65);
    const id=randomUUID();const session=newState({id,...request,...options,who,profile:initial,mission});
    if(request.mode==='endless'){session.challengeLevel=(initial.campaign.endlessBest||0)+1;session.missionId='endless-'+session.challengeLevel;}
    if(mission?.opponent){session.roster.p0=clone(content.campaign.playerStartingRoster?.(request.faction,mission,deck)||[]);session.roster.p1=clone(mission.opponent.roster);session.formations.p1=mission.opponent.formation;session.relics.p1=clone(mission.opponent.relics||[]);session.tactics.p1=clone(mission.opponent.tactics||[]);}
    else if(mission){session.players[1]=mission.name;session.factions.p1=mission.faction;session.mmr[1]=mission.estimatedMmr;}
    // Campaign introduces unlockables deliberately; bot/endless opponents may
    // use the complete roster. Player collection locks are independently enforced.
    const enemyUnlocks=mission?.opponent?.unlockedTroops||D.NEW_TROOPS;
    for(const key of enemyUnlocks)session.profiles.p1.troops[key].unlocked=true;
    const adapter=await engine(session);await makePacks(session,adapter);
    await db.runTransaction(async tx=>{
      const latest=await tx.get(profileRef(who.uid));D.fail(latest.exists&&!latest.data().activeSessionId,'MATCH_ACTIVE','Resume your active match.',409);
      tx.create(sessionRef(id),session);tx.update(profileRef(who.uid),{activeSessionId:id,updatedAt:now()});
    });
    return view(who,session);
  }
  async function joinPvp(who,request) {
    D.onlyKeys(request,['lobbyId','faction','ranked','deck','deckId']);
    D.safeId(request.lobbyId,'Lobby');
    await profile(who);const content=await sharedContent();
    const link=db.doc(`factionForgeLobbySessions/${request.lobbyId}`);
    const id=randomUUID();
    await db.runTransaction(async tx=>{
      const [lobbySnap,linkSnap,pSnap]=await Promise.all([tx.get(db.doc(`lobbies/${request.lobbyId}`)),tx.get(link),tx.get(profileRef(who.uid))]);
      const lobby=lobbySnap.data(),p=pSnap.data();
      const deck=await requestedDeck(request,p);const faction=deck.faction;
      D.fail(lobbySnap.exists&&['crowncouncil','factionforge'].includes(lobby.gameType),'LOBBY_UNAVAILABLE','This Faction Forge lobby is unavailable.',404);
      D.fail(lobby.visibility==='public','PRIVATE_PRACTICE_ONLY','Private hosted rooms remain practice matches. Ranked progression uses verified public lobbies.',409);
      D.fail(typeof lobby.hostUid==='string'&&lobby.members&&lobby.members[who.uid]===who.username&&Array.isArray(lobby.players)&&lobby.players.slice(0,2).includes(who.username),'VERIFIED_LOBBY_REQUIRED','This room needs protected Firebase UID seats. Create a new public Faction Forge lobby.',409);
      let existing=null,existingRef=null;
      if(linkSnap.exists){existingRef=sessionRef(linkSnap.data().sessionId);const s=await tx.get(existingRef);existing=s.exists?s.data():null;}
      if(existing&&existing.phase!=='game-over'&&existing.expiresAt>now()) {
        D.fail(existing.ranked===(lobby.forgeRanked!==false),'LOBBY_SETTINGS_CHANGED','The lobby match rules changed. Create a new lobby.',409);
        D.fail(existing.participants[0]===lobby.hostUid,'LOBBY_OWNERSHIP_CHANGED','This lobby belongs to a different match host. Create a new lobby.',409);
        const prior=existing.participants.indexOf(who.uid);
        if(prior>=0){await normalizeSessionDecks(existing);D.fail(JSON.stringify(existing.decks['p'+prior])===JSON.stringify(deck),'DECK_LOCKED','Your match deck is already locked.',409);return;}
        D.fail(existing.phase==='waiting'&&!existing.participants[1],'LOBBY_FULL','This match already has two verified players.',409);
        D.fail(!p.activeSessionId,'MATCH_ACTIVE','Finish your active match first.',409);
        existing.participants[1]=who.uid;existing.players[1]=who.username;existing.profiles.p1=clone(p);existing.factions.p1=faction;existing.decks=existing.decks||{};existing.decks.p1=deck;existing.initialMmr[1]=p.crowncouncilElo;existing.mmr[1]=p.crowncouncilElo;existing.phase='draft';existing.revision++;
        const adapter=adapterFactory({seed:existing.seed+':'+existing.revision,modules:content});await makePacks(existing,adapter);
        tx.set(existingRef,existing);tx.update(profileRef(who.uid),{activeSessionId:existing.id,updatedAt:now()});
      } else {
        D.fail(lobby.host===who.username&&lobby.hostUid===who.uid,'HOST_NOT_READY','The verified host must choose a deck first.',409);
        D.fail(!p.activeSessionId,'MATCH_ACTIVE','Finish your active match first.',409);
        const session=newState({id,mode:'pvp',faction,deck,who,profile:p});session.phase='waiting';session.phaseEndsAt=0;session.deadlineAt=0;session.ranked=lobby.forgeRanked!==false;session.lobbyId=request.lobbyId;session.players[1]='Waiting for player';session.profiles.p1=null;session.factions.p1=null;
        tx.create(sessionRef(id),session);tx.set(link,{sessionId:id,hostUid:who.uid,createdAt:now()});tx.update(profileRef(who.uid),{activeSessionId:id,updatedAt:now()});
      }
    });
    const linked=await link.get();return getSession(who,linked.data().sessionId);
  }
  async function getSession(who,id,{knownReplayId=null}={}) {
    D.safeId(id,'Session');
    const before=await sessionRef(id).get();
    D.fail(before.exists,'NOT_FOUND','Match not found.',404);
    const pending=before.data();D.fail(pending.participants.includes(who.uid),'PERMISSION_DENIED','This match belongs to another player.',403);
    if(['draft','loser-bonus'].includes(pending.phase)&&pending.deadlineAt<=now()&&pending.expiresAt>now()){
      const side=pending.bonusFor==='p1'?1:pending.bonusFor==='p0'?0:pending.selections.p0===undefined?0:1;
      const key='p'+side;
      if(pending.participants[side]&&pending.selections[key]===undefined){
        const adapter=await engine(pending);const index=adapter.aiPick(pending.currentPacks[key],pending,key,.6);
        await pick({uid:pending.participants[side],username:pending.players[side]},id,{index:Number.isInteger(index)&&index>=0&&index<3?index:0,sequence:pending.revision,requestId:'timeout-'+pending.revision+'-'+side},true);
        return getSession(who,id,{knownReplayId});
      }
    }
    let session,dirty=false;
    await db.runTransaction(async tx=>{
      const snap=await tx.get(sessionRef(id));D.fail(snap.exists,'NOT_FOUND','Match not found.',404);session=snap.data();
      D.fail(session.participants.includes(who.uid),'PERMISSION_DENIED','This match belongs to another player.',403);
      if(!session.decks){dirty=true;await normalizeSessionDecks(session);if(['draft','loser-bonus'].includes(session.phase)){session.revision++;session.selections={};await makePacks(session,await engine(session));}}
      if(session.phase!=='game-over'&&session.expiresAt<=now()){dirty=true;session.phase='game-over';session.abandoned=true;session.winnerSide=null;session.endedAt=now();session.serverValidated=false;}
      if(session.phase==='battle'&&session.battleReadyAt<=now()) {
        dirty=true;
        session.completedBattles++;const loser=1-session.battle.winnerSide;session.roundWins=session.roundWins||[0,0];session.roundWins[1-loser]++;session.lives[loser]=Math.max(0,session.lives[loser]-1);session.revision++;
        if(session.lives[loser]===0){session.phase='game-over';session.phaseEndsAt=0;session.deadlineAt=0;session.winnerSide=1-loser;session.winner=session.players[1-loser];session.endedAt=now();session.serverValidated=true;}
        else {session.phase='loser-bonus';session.bonusFor='p'+loser;session.draftIndex=0;session.currentPacks={p0:[],p1:[]};session.round++;session.selections={};const adapter=await engine(session);await makePacks(session,adapter);await autoBot(session,adapter);}
      }
      if(session.phase==='game-over') {
        const refs=session.participants.filter(Boolean).map(uid=>profileRef(uid));
        const profileSnaps=await Promise.all(refs.map(ref=>tx.get(ref)));
        const receipts=await Promise.all(session.participants.filter(Boolean).map(uid=>tx.get(receiptRef(id,uid))));
        if(session.serverValidated&&session.mode==='pvp'&&session.ranked&&!session.ratingDecision)session.ratingDecision=D.ratingDecision(profileSnaps.map(s=>s.data()),session,now());
        for(let i=0;i<profileSnaps.length;i++) {
          const p=profileSnaps[i].data();if(receipts[i].exists)continue;
          if(session.serverValidated){dirty=true;const settled=D.settle(p,session,i,{now:now(),opponentUid:session.mode==='pvp'?session.participants[1-i]:null});tx.set(refs[i],settled.profile);tx.create(receiptRef(id,p.uid),settled.receipt);session.mmr[i]=settled.profile.crowncouncilElo;session.mmrChange[i]=settled.receipt.mmrDelta;session.eloCommitted=session.mode==='pvp'&&session.ranked;
            if(session.mode==='pvp'&&session.ranked)tx.set(db.doc(`users/${p.username}`),{crowncouncilElo:settled.profile.crowncouncilElo,crowncouncilLastDelta:settled.receipt.mmrDelta,crowncouncilLastMatchAt:now()},{merge:true});
          }else if(p.activeSessionId===id)tx.update(refs[i],{activeSessionId:null,updatedAt:now()});
        }
      }
      if(dirty)tx.set(sessionRef(id),session);
    });
    return view(who,session,knownReplayId);
  }
  async function autoBot(session,adapter) {
    if(session.selections.p1!==undefined||session.mode==='pvp'||session.phase==='waiting'||session.phase==='battle'||session.phase==='game-over')return;
    if(session.bonusFor==='p0')return;
    const pack=session.currentPacks.p1;if(!pack?.length)return;
    const content=await sharedContent();const mission=session.missionNumber?content.campaign.getMission(session.missionId):session.mode==='endless'?content.campaign.createEndlessMission(session.challengeLevel):null;
    let index=adapter.aiPick(pack,session,'p1',mission?.difficulty||session.aiDifficulty||.65);
    if(typeof index==='object')index=pack.indexOf(index);if(!Number.isInteger(index)||index<0||index>=pack.length)index=0;
    const changed=adapter.pick(session,'p1',pack[index]);Object.assign(session,changed);session.selections.p1=index;
    if(session.phase==='loser-bonus'&&session.bonusFor==='p1'){session.phase='draft';session.bonusFor=null;session.selections={};await makePacks(session,adapter);}
  }
  async function pick(who,id,request,internal=false) {
    D.onlyKeys(request,['index','sequence','requestId']);
    D.safeId(id,'Session');D.safeId(request.requestId,'Request');D.fail(Number.isInteger(request.index)&&request.index>=0&&request.index<3,'INVALID_PICK','Choose one of the three draft cards.');
    D.fail(Number.isInteger(request.sequence)&&request.sequence>=0,'INVALID_SEQUENCE','Draft revision is required.');
    if(!internal)await getSession(who,id);
    const content=await sharedContent();
    await db.runTransaction(async tx=>{
      const snap=await tx.get(sessionRef(id)),session=snap.data();D.fail(snap.exists,'NOT_FOUND','Match not found.',404);
      await normalizeSessionDecks(session);
      const side=session.participants.indexOf(who.uid);D.fail(side>=0,'PERMISSION_DENIED','This is another player\'s match.',403);
      const requestKey=who.uid+'_'+request.requestId;
      if(session.processedRequests[requestKey]){const old=session.processedRequests[requestKey];D.fail(old.index===request.index&&old.sequence===request.sequence,'REQUEST_CONFLICT','Use a new identifier for a different choice.',409);return;}
      D.fail(session.revision===request.sequence,'STALE_REVISION','The draft changed. Refresh and choose again.',409);
      D.fail(['draft','loser-bonus'].includes(session.phase),'INVALID_PHASE','This match is not accepting draft choices.',409);
      const key='p'+side;D.fail(!session.bonusFor||session.bonusFor===key,'NOT_YOUR_PICK','Your opponent is choosing a bonus card.',409);
      D.fail(session.selections[key]===undefined,'ALREADY_PICKED','Wait for your opponent.',409);
      const pack=session.currentPacks[key];D.fail(pack&&pack[request.index],'INVALID_PICK','This card is not in your server-issued pack.');
      const adapter=adapterFactory({seed:session.seed+':'+session.revision,modules:content});
      Object.assign(session,adapter.pick(session,key,pack[request.index]));session.selections[key]=request.index;session.processedRequests[requestKey]={at:now(),index:request.index,sequence:request.sequence};
      if(session.phase==='loser-bonus') {
        session.phase='draft';session.bonusFor=null;session.selections={};session.revision++;await makePacks(session,adapter);await autoBot(session,adapter);
      }else {
        await autoBot(session,adapter);
        if(session.selections.p0!==undefined&&session.selections.p1!==undefined) {
          session.draftIndex++;session.selections={};session.revision++;
          if(session.draftIndex>=3) {
            const mission=session.missionNumber?content.campaign.getMission(session.missionId):session.mode==='endless'?content.campaign.createEndlessMission(session.challengeLevel):null;
            const battle=adapter.battle(session,{forgeProfiles:{0:session.profiles.p0,1:session.profiles.p1},ranked:session.mode==='pvp'&&session.ranked,mission});
            const compressed=gzipSync(Buffer.from(JSON.stringify(battle.frames))).toString('base64');
            D.fail(Buffer.byteLength(compressed)<700000,'REPLAY_TOO_LARGE','The battle replay exceeded the persistence limit.',503);
            session.battleCount++;session.battle={startedAt:now(),simulationStartedAt:1000,durationMs:battle.durationMs,clockOrigin:battle.clockOrigin,winnerSide:battle.winnerSide,replayId:String(session.battleCount)};
            session.phase='battle';session.phaseEndsAt=0;session.deadlineAt=0;session.battleReadyAt=now()+Math.max(20000,battle.durationMs);session.currentPacks={p0:[],p1:[]};session.draftIndex=0;
            tx.create(sessionRef(id).collection('replays').doc(String(session.battleCount)),{encoding:'gzip-base64',frames:compressed,createdAt:now(),engineVersion:adapter.version});
          } else await makePacks(session,adapter);
        }
      }
      session.updatedAt=now();tx.set(sessionRef(id),session);
    });
    return internal?null:getSession(who,id);
  }
  async function upgradeTroop(who,request) {
    D.onlyKeys(request,['troopKey','requestId']);const {troopKey,requestId}=request;
    D.safeId(requestId,'Request');const ref=profileRef(who.uid),receipt=ref.collection('upgrades').doc(requestId);let transaction;
    await profile(who);
    await db.runTransaction(async tx=>{
      const [p,old]=await Promise.all([tx.get(ref),tx.get(receipt)]);
      if(old.exists){D.fail(old.data().troopKey===troopKey,'REQUEST_CONFLICT','Use a new request identifier.',409);transaction=old.data();return;}
      const result=D.upgrade(p.data(),troopKey,now());transaction={...result.transaction,requestId,uid:who.uid};tx.set(ref,result.profile);tx.create(receipt,transaction);
    });return {profile:await profile(who),transaction};
  }
  async function saveDeck(who,request){
    D.onlyKeys(request,['deck']);await profile(who);const rules=await deckRules();
    let saved;
    await db.runTransaction(async tx=>{const ref=profileRef(who.uid),snap=await tx.get(ref),p=snap.data();D.fail(!p.activeSessionId,'MATCH_ACTIVE','Your match deck is locked.',409);D.safeId(request.deck?.id,'Deck');D.fail(!rules.STARTER_DECKS.some(deck=>deck.id===request.deck.id),'INVALID_DECK','Duplicate a starter deck before editing it.');saved=await requestedDeck(request,p,{saving:true});D.fail(p.decks.some(deck=>deck.id===saved.id)||p.decks.length<12,'DECK_LIMIT','Keep at most 12 decks.');const decks=rules.normalizeDecks(p.decks.some(deck=>deck.id===saved.id)?p.decks.map(deck=>deck.id===saved.id?saved:deck):[...p.decks,saved]);D.fail(decks.length<=12,'DECK_LIMIT','Keep at most 12 decks.');tx.update(ref,{decks,selectedDeckId:saved.id,selectedDeckIndex:decks.findIndex(deck=>deck.id===saved.id),updatedAt:now()});});
    return {profile:await profile(who),deck:saved};
  }
  async function selectDeck(who,request){
    D.onlyKeys(request,['deckId']);D.safeId(request.deckId,'Deck');await profile(who);
    await db.runTransaction(async tx=>{const ref=profileRef(who.uid),snap=await tx.get(ref),p=snap.data();D.fail(!p.activeSessionId,'MATCH_ACTIVE','Your match deck is locked.',409);const deck=await requestedDeck(request,p);tx.update(ref,{selectedDeckId:deck.id,selectedDeckIndex:p.decks.findIndex(item=>item.id===deck.id),updatedAt:now()});});return {profile:await profile(who)};
  }
  async function target(who,troopKey){D.fail(D.ALL_TROOPS.includes(troopKey),'INVALID_ARGUMENT','Unknown troop.');const p=await profile(who);D.fail(p.troops[troopKey].unlocked,'TROOP_LOCKED','Choose an unlocked troop.');await profileRef(who.uid).update({rewardTarget:troopKey});return {profile:await profile(who)};}
  async function buyPack(who,request){
    D.onlyKeys(request,['packId','requestId']);const {packId,requestId}=request;D.safeId(requestId,'Request');
    const ref=profileRef(who.uid),receipt=ref.collection('purchases').doc(requestId);let transaction;await profile(who);
    // The trusted service owns randomness; retain entropy across transaction retries.
    const entropy=packId==='random-reinforcements'?randomBytes(32).toString('hex'):undefined;
    await db.runTransaction(async tx=>{const [p,old]=await Promise.all([tx.get(ref),tx.get(receipt)]);if(old.exists){D.fail(old.data().packId===packId,'REQUEST_CONFLICT','Use a new request identifier.',409);transaction=old.data();return;}const result=D.buyPack(p.data(),packId,now(),entropy);transaction={...result.transaction,uid:who.uid,requestId};tx.set(ref,result.profile);tx.create(receipt,transaction);});
    return {profile:await profile(who),transaction};
  }
  async function abandon(who,id) {
    D.safeId(id,'Session');await db.runTransaction(async tx=>{
      const snap=await tx.get(sessionRef(id));D.fail(snap.exists&&snap.data().participants.includes(who.uid),'PERMISSION_DENIED','This match belongs to another player.',403);
      const session=snap.data();if(session.phase==='game-over')return;
      // An authenticated participant may surrender their own seat. This is a
      // server-owned forfeit, never a client-reported winner or opponent result.
      session.phase='game-over';session.abandoned=true;session.endedAt=now();session.phaseEndsAt=0;session.deadlineAt=0;
      if(session.mode==='pvp'&&session.participants[1]){const side=session.participants.indexOf(who.uid);session.serverValidated=true;session.forfeitUid=who.uid;session.winnerSide=1-side;session.winner=session.players[1-side];session.lives[side]=0;}
      else {session.serverValidated=false;session.winnerSide=null;}
      tx.set(sessionRef(id),session);
    });return getSession(who,id);
  }
  async function view(who,session,knownReplayId=null) {
    const side=session.participants.indexOf(who.uid);D.fail(side>=0,'PERMISSION_DENIED','This is another player\'s match.',403);
    const publicSession=clone(session);for(const key of ['seed','profiles','processedRequests','serverValidated'])delete publicSession[key];
    publicSession.meIndex=side;publicSession.currentPacks['p'+(1-side)]=[];publicSession.selections={p0:[],p1:[]};
    for(let seat=0;seat<2;seat++){const key='p'+seat;if(session.selections[key]!==undefined)publicSession.selections[key][session.draftIndex]=seat===side?session.selections[key]:0;}
    if(publicSession.phase==='battle'&&publicSession.battle&&publicSession.battle.replayId!==knownReplayId){const replay=await sessionRef(session.id).collection('replays').doc(publicSession.battle.replayId).get();if(replay.exists)publicSession.battle.frames=JSON.parse(gunzipSync(Buffer.from(replay.data().frames,'base64')).toString());}
    const receipt=await receiptRef(session.id,who.uid).get();
    return {session:publicSession,profile:await profile(who),...(receipt.exists?{receipt:receipt.data()}:{}),serverTime:now()};
  }
  return {identity,profile,startSolo,joinPvp,getSession,pick,upgradeTroop,buyPack,saveDeck,selectDeck,target,abandon};
}
module.exports={createForgeService};
