import test from 'node:test';
import assert from 'node:assert/strict';
import {newSoloProfile,settleSolo,soloMission,upgradeSolo,buySoloPack,settleSoloRoundWin,createFirebaseProgress} from '../firebase-progress.js';
import {FORGE_PACKS,ROUND_WIN_CARDS,ROUND_WIN_DAILY_CAP,packEligibility,forgePackCards} from '../packs.js';
import {selectSoloAccount} from '../solo-account.js';
import {FACTION_TROOPS,STARTER_DECKS} from '../decks.js';
const troops={...Object.fromEntries(FORGE_PACKS.flatMap(pack=>pack.cards.map(card=>[card.troopKey,{}]))),signalhawk:{unlockable:true},chronoweaver:{unlockable:true},siphonleech:{unlockable:true},railbreaker:{unlockable:true},miragewarden:{unlockable:true},hiveherald:{unlockable:true}};
const storage=()=>{const map=new Map();return {getItem:key=>map.get(key)||null,setItem:(key,value)=>map.set(key,value),removeItem:key=>map.delete(key)};};
function firebase(){
 const docs=new Map();let serial=Promise.resolve(),failAfterCommit=false;
 const api={doc:(_db,...parts)=>parts.join('/'),getDoc:async ref=>({exists:()=>docs.has(ref),data:()=>structuredClone(docs.get(ref))}),onSnapshot:()=>()=>{},runTransaction:async(_db,fn)=>{
  const operation=serial.then(async()=>{const writes=[];const value=await fn({get:async ref=>{assert.equal(writes.length,0,'Firestore transactions require every read before any write');return {exists:()=>docs.has(ref),data:()=>structuredClone(docs.get(ref))};},set:(ref,data)=>writes.push([ref,structuredClone(data)])});for(const [ref,data] of writes)docs.set(ref,data);if(failAfterCommit){failAfterCommit=false;throw Object.assign(Error('Disconnected'),{code:'unavailable'});}return value;});serial=operation.catch(()=>{});return operation;
 }};
 return {api,docs,uncertain:()=>{failAfterCommit=true;}};
}
const result=(id='win')=>({id,mode:'campaign',missionId:'forge-01',won:true,lives:4});
function service(store,opts={}){return createFirebaseProgress({auth:{currentUser:{uid:'owner'}},db:{},api:store.api,username:'Commander',troops,storage:storage(),...opts});}

test('deck migration adds canonical starters without changing currency, upgrades, campaign or receipts',async()=>{
 const store=firebase(),one=service(store);const original=await one.loadProfile();const legacy={...original,forgeDust:731,wins:19,campaign:{missions:{'forge-01':{completed:true,stars:3}},endlessUnlocked:false,endlessBest:0}};delete legacy.decks;delete legacy.selectedDeckId;legacy.troops.marksman.forgeTier=3;legacy.troops.marksman.cards=22;store.docs.set('factionForgeSoloProfiles/owner',legacy);store.docs.set('factionForgeSoloProfiles/owner/results/kept',{id:'kept'});
 const migrated=await one.loadProfile();assert.equal(migrated.decks.length,3);assert.deepEqual(migrated.decks.map(deck=>deck.id),STARTER_DECKS.map(deck=>deck.id));const {decks,selectedDeckId,...retained}=migrated;assert.deepEqual(retained,legacy);assert.deepEqual(store.docs.get('factionForgeSoloProfiles/owner/results/kept'),{id:'kept'});
});
test('custom decks persist across devices and reject duplicate, mixed, locked, preset and active-match edits',async()=>{
 const allTroops=Object.fromEntries(Object.values(FACTION_TROOPS).flat().map(key=>[key,{unlockable:key==='railbreaker'}]));let active=false;const store=firebase(),one=service(store,{troops:allTroops,isActiveMatch:()=>active});await one.loadProfile();
 const deck={id:'custom-iron',name:'Precision Guard',faction:'Vanir',troops:['vguard','marksman','vrocket','rifleman','medic']};await one.saveDeck(deck);one.dispose();const two=service(store,{troops:allTroops,isActiveMatch:()=>active});await two.loadProfile();assert.deepEqual(two.profile.decks.find(item=>item.id===deck.id),deck);assert.equal(two.profile.selectedDeckId,deck.id);await two.selectDeck('starter-aesir');assert.equal(two.profile.selectedDeckId,'starter-aesir');
 for(const bad of [{...deck,id:'starter-vanir'},{...deck,troops:['vguard','vguard','medic','marksman','vrocket']},{...deck,troops:['vguard','marksman','medic','storm','vrocket']},{...deck,troops:['vguard','marksman','medic','railbreaker','vrocket']}])await assert.rejects(two.saveDeck(bad),{code:'INVALID_DECK'});
 active=true;await assert.rejects(two.saveDeck(deck),{code:'MATCH_ACTIVE'});await assert.rejects(two.selectDeck('starter-vanir'),{code:'MATCH_ACTIVE'});
});
test('full deck libraries reject another deck before normalization and still allow existing deck edits',async()=>{
 const allTroops=Object.fromEntries(Object.values(FACTION_TROOPS).flat().map(key=>[key,{}])),store=firebase(),one=service(store,{troops:allTroops});await one.loadProfile();const base={name:'Library Deck',faction:'Vanir',troops:['vguard','marksman','vrocket','rifleman','medic']};for(let i=0;i<9;i++)await one.saveDeck({...base,id:'library-'+i});assert.equal(one.profile.decks.length,12);await assert.rejects(one.saveDeck({...base,id:'overflow'}),{code:'DECK_LIMIT'});assert.equal(one.profile.selectedDeckId,'library-8');await one.saveDeck({...base,id:'library-0',name:'Edited Existing'});assert.equal(one.profile.decks.length,12);assert.equal(one.profile.decks[3].name,'Edited Existing');
});
test('old partial collections gain missing troop defaults while keeping existing cards and upgrades',async()=>{
 const allTroops=Object.fromEntries(Object.values(FACTION_TROOPS).flat().map(key=>[key,{unlockable:key==='railbreaker'}])),store=firebase(),old=newSoloProfile('owner','Commander',allTroops);old.troops={marksman:{forgeTier:3,cards:91,unlocked:true}};delete old.decks;store.docs.set('factionForgeSoloProfiles/owner',old);const one=service(store,{troops:allTroops});const profile=await one.loadProfile();assert.equal(Object.keys(profile.troops).length,32);assert.equal(profile.troops.marksman.cards,91);assert.equal(profile.troops.marksman.forgeTier,3);assert.equal(profile.troops.vguard.unlocked,true);assert.equal(profile.troops.railbreaker.unlocked,false);
});

test('Firebase alone creates and reloads the same UID profile without any HTTP endpoint',async()=>{
 const store=firebase(),one=service(store);assert.equal(one.configured,true);const profile=await one.loadProfile();assert.equal(profile.uid,'owner');assert.equal(profile.troops.signalhawk.unlocked,false);assert.equal(one.connectionStatus,'Progress saved with Firebase');
 await one.recordSoloResult(result());one.dispose();const two=service(store);assert.equal((await two.loadProfile()).campaign.missions['forge-01'].stars,3);assert.ok([...store.docs.keys()].every(key=>key.startsWith('factionForgeSoloProfiles/owner')));
});
test('campaign wins unlock the next mission and award cards and dust once despite concurrent retries',async()=>{
 const store=firebase(),one=service(store);await one.loadProfile();await Promise.all([one.recordSoloResult(result()),one.recordSoloResult(result())]);assert.equal(one.profile.forgeDust,20);assert.equal(one.profile.wins,1);assert.equal(Object.values(one.profile.troops).reduce((n,t)=>n+t.cards,0),6);assert.equal(soloMission(one.profile,'campaign','forge-02').id,'forge-02');
});
test('unacknowledged rewards survive reload and recover without a duplicate grant',async()=>{
 const store=firebase(),cache=storage(),one=service(store,{storage:cache});await one.loadProfile();store.uncertain();await assert.rejects(one.recordSoloResult(result()),{code:'unavailable'});one.dispose();
 const two=service(store,{storage:cache});await two.loadProfile();assert.equal(two.profile.forgeDust,20);assert.equal(two.profile.wins,1);assert.deepEqual(JSON.parse(cache.getItem('factionForge.solo.pending.owner')),[]);
});
test('upgrade transaction deducts its cost once and rejects insufficient resources or a conflicting retry',async()=>{
 const store=firebase(),one=service(store);await one.loadProfile();let p=store.docs.get('factionForgeSoloProfiles/owner');p.forgeDust=100;p.troops.rifleman.cards=10;
 await Promise.all([one.upgradeTroop('rifleman',{requestId:'upgrade-1'}),one.upgradeTroop('rifleman',{requestId:'upgrade-1'})]);assert.equal(one.profile.forgeDust,0);assert.equal(one.profile.troops.rifleman.forgeTier,2);
 await assert.rejects(one.upgradeTroop('medic',{requestId:'upgrade-1'}),{code:'REQUEST_CONFLICT'});await assert.rejects(one.upgradeTroop('rifleman',{requestId:'upgrade-2'}),{code:'INSUFFICIENT_RESOURCES'});
});
test('uncertain upgrade retains its request ID across reload without spending twice',async()=>{
 const store=firebase(),cache=storage(),one=service(store,{storage:cache});await one.loadProfile();const p=store.docs.get('factionForgeSoloProfiles/owner');p.forgeDust=500;p.troops.rifleman.cards=50;
 store.uncertain();await assert.rejects(one.upgradeTroop('rifleman',{requestId:'confirmed-upgrade'}),{code:'unavailable'});one.dispose();const two=service(store,{storage:cache});await two.loadProfile();await two.upgradeTroop('rifleman',{requestId:'new-dialog-after-reload'});assert.equal(two.profile.forgeDust,400);assert.equal(two.profile.troops.rifleman.forgeTier,2);assert.equal(two.profile.troops.rifleman.cards,40);
});
test('all campaign milestones and Endless progression work with solo saves',()=>{
 let p=newSoloProfile('owner','Commander',troops);assert.throws(()=>soloMission(p,'campaign','forge-02'),{code:'MISSION_LOCKED'});assert.throws(()=>soloMission(p,'endless'),{code:'MODE_LOCKED'});
 for(let n=1;n<=30;n++){p=settleSolo(p,{...result('win-'+n),missionId:'forge-'+String(n).padStart(2,'0')}).profile;}
 assert.equal(p.campaign.endlessUnlocked,true);for(const [key,value] of Object.entries(troops))if(value.unlockable)assert.equal(p.troops[key].unlocked,true);
 p=settleSolo(p,{id:'endless-1-win',mode:'endless',missionId:'endless-1',won:true,lives:2}).profile;assert.equal(p.campaign.endlessBest,1);assert.equal(soloMission(p,'endless').stage,2);
});
test('losses award nothing; replay and bot reward caps preserve progress without competitive fields',()=>{
 let p=newSoloProfile('owner','Commander',troops);p=settleSolo(p,{...result('loss'),won:false}).profile;assert.equal(p.forgeDust,0);assert.equal(p.campaign.missions['forge-01'],undefined);
 p=settleSolo(p,result()).profile;for(let i=0;i<7;i++)p=settleSolo(p,result('replay-'+i)).profile;assert.equal(p.forgeDust,45);
 for(let i=0;i<12;i++)p=settleSolo(p,{id:'bot-'+i,mode:'bot',won:true}).profile;assert.equal(p.forgeDust,145);assert.equal(p.crowncouncilElo,undefined);assert.equal(p.activeSessionId,undefined);
});
test('reward targets affect the next grant and are limited to unlocked troops',async()=>{
 const one=service(firebase());await one.loadProfile();await assert.rejects(one.setRewardTarget('signalhawk'),{code:'TROOP_LOCKED'});await one.setRewardTarget('medic');await one.recordSoloResult(result());assert.equal(one.profile.troops.medic.cards,6);
});
test('a changed or disposed identity cannot write the previous UID profile',async()=>{
 const auth={currentUser:{uid:'owner'}},one=service(firebase(),{auth});await one.loadProfile();auth.currentUser={uid:'other'};await assert.rejects(one.recordSoloResult(result()),{code:'ACCOUNT_CHANGED'});one.dispose();await assert.rejects(one.loadProfile(),{code:'DISPOSED'});
});
test('permission errors accurately describe the required Firebase rules update',async()=>{
 const one=service({api:{doc:()=>'',runTransaction:async()=>{throw Object.assign(Error('Denied'),{code:'permission-denied'});}}});await assert.rejects(one.loadProfile(),{code:'permission-denied'});assert.equal(one.connectionStatus,'Firebase save permissions need updating');assert.equal(one.profile,null);
});

test('fixed packs show exact contents and spend their Dust once under concurrent retries',async()=>{
 const store=firebase(),one=service(store);await one.loadProfile();store.docs.get('factionForgeSoloProfiles/owner').forgeDust=50;
 const [first,again]=await Promise.all([one.buyPack('vanir',{requestId:'pack-1'}),one.buyPack('vanir',{requestId:'pack-1'})]);
 assert.deepEqual(first.transaction,again.transaction);assert.deepEqual(first.transaction.cards,FORGE_PACKS[0].cards);assert.equal(one.profile.forgeDust,0);
 for(const card of FORGE_PACKS[0].cards)assert.equal(one.profile.troops[card.troopKey].cards,5);
 await assert.rejects(one.buyPack('aesir',{requestId:'pack-1'}),{code:'REQUEST_CONFLICT'});
 await assert.rejects(one.buyPack('vanir',{requestId:'pack-2'}),{code:'INSUFFICIENT_RESOURCES'});
 assert.equal(store.docs.has('factionForgeSoloProfiles/owner/purchases/pack-2'),false);
});
test('two different pack purchases cannot overspend a shared balance',async()=>{
 const store=firebase(),one=service(store);await one.loadProfile();store.docs.get('factionForgeSoloProfiles/owner').forgeDust=50;
 const attempts=await Promise.allSettled([one.buyPack('vanir',{requestId:'first'}),one.buyPack('aesir',{requestId:'second'})]);
 assert.equal(attempts.filter(attempt=>attempt.status==='fulfilled').length,1);assert.equal(one.profile.forgeDust,0);assert.equal(Object.values(one.profile.troops).reduce((n,t)=>n+t.cards,0),15);
});
test('an uncertain pack response retains its receipt across reload without duplicate spending or cards',async()=>{
 const store=firebase(),cache=storage(),one=service(store,{storage:cache});await one.loadProfile();store.docs.get('factionForgeSoloProfiles/owner').forgeDust=100;
 store.uncertain();await assert.rejects(one.buyPack('vanir',{requestId:'persisted-pack'}),{code:'unavailable'});one.dispose();
 const two=service(store,{storage:cache});await two.loadProfile();const recovered=await two.buyPack('vanir',{requestId:'new-dialog'});
 assert.equal(recovered.transaction.requestId,'persisted-pack');assert.equal(two.profile.forgeDust,50);assert.equal(two.profile.troops.rifleman.cards,5);assert.equal(cache.getItem('factionForge.solo.pending.owner.pack.vanir'),null);
});
test('pack eligibility and transactions reject active matches, invalid packs, locked troops and insufficient Dust',async()=>{
 const p=newSoloProfile('owner','Commander',troops);p.forgeDust=90;
 assert.equal(packEligibility(p,'war-chest').available,true);assert.equal(buySoloPack(p,'war-chest').profile.forgeDust,0);assert.equal(p.forgeDust,90);
 assert.throws(()=>buySoloPack({...p,activeSessionId:'battle'},'vanir'),{code:'MATCH_ACTIVE'});assert.throws(()=>buySoloPack(p,'fake'),{code:'INVALID_PACK'});
 p.troops.medic.unlocked=false;assert.equal(packEligibility(p,'vanir').available,false);assert.throws(()=>buySoloPack(p,'vanir'),{code:'TROOP_LOCKED'});
 const store=firebase(),one=service(store,{isActiveMatch:()=>true});await one.loadProfile();await assert.rejects(one.buyPack('vanir'),{code:'MATCH_ACTIVE'});
});

test('random reinforcements use every unlocked faction, retain seed across Auth changes, and never include locked troops',()=>{
 const p=newSoloProfile('owner','Commander',troops);p.forgeDust=50;
 const cards=forgePackCards(p,'random-reinforcements','stable-roll');
 assert.equal(cards.length,3);assert.equal(new Set(cards.map(card=>card.troopKey)).size,3);assert.equal(cards.reduce((n,card)=>n+card.quantity,0),15);
 assert.ok(cards.every(card=>p.troops[card.troopKey].unlocked));
 assert.deepEqual(forgePackCards({...p,uid:'replacement-uid'},'random-reinforcements','stable-roll'),cards);
 const encountered=new Set(Array.from({length:200},(_,i)=>forgePackCards(p,'random-reinforcements','roll-'+i)).flat().map(card=>card.troopKey));
 assert.ok(encountered.has('rifleman')&&encountered.has('phaser')&&encountered.has('hellspawn'));
 assert.equal(encountered.has('signalhawk'),false);p.troops.signalhawk.unlocked=true;
 assert.ok(Array.from({length:200},(_,i)=>forgePackCards(p,'random-reinforcements','unlocked-'+i)).flat().some(card=>card.troopKey==='signalhawk'));
 for(const key of Object.keys(p.troops))p.troops[key].unlocked=key==='rifleman';
 assert.deepEqual(buySoloPack(p,'random-reinforcements','small-pool').transaction.cards,Array.from({length:3},()=>({troopKey:'rifleman',quantity:5})));
 p.troops.rifleman.unlocked=false;assert.equal(packEligibility(p,'random-reinforcements').available,false);
 assert.throws(()=>buySoloPack(p,'random-reinforcements','empty-pool'),{code:'TROOP_LOCKED'});
 assert.throws(()=>buySoloPack(p,'random-reinforcements'),{code:'INVALID_REQUEST'});
});

test('random pack concurrent retries and uncertain reload recover the exact receipt without reroll or double spending',async()=>{
 const store=firebase(),cache=storage(),one=service(store,{storage:cache});await one.loadProfile();store.docs.get('factionForgeSoloProfiles/owner').forgeDust=100;
 const [first,again]=await Promise.all([one.buyPack('random-reinforcements',{requestId:'random-once'}),one.buyPack('random-reinforcements',{requestId:'random-once'})]);
 assert.deepEqual(first.transaction,again.transaction);assert.equal(one.profile.forgeDust,50);assert.equal(Object.values(one.profile.troops).reduce((n,t)=>n+t.cards,0),15);
 store.uncertain();await assert.rejects(one.buyPack('random-reinforcements',{requestId:'random-reload'}),{code:'unavailable'});const committed=structuredClone(store.docs.get('factionForgeSoloProfiles/owner/purchases/random-reload'));one.dispose();
 const two=service(store,{storage:cache});await two.loadProfile();const recovered=await two.buyPack('random-reinforcements',{requestId:'different-dialog'});
 assert.deepEqual(recovered.transaction,committed);assert.equal(two.profile.forgeDust,0);assert.equal(Object.values(two.profile.troops).reduce((n,t)=>n+t.cards,0),30);
 assert.equal(cache.getItem('factionForge.solo.pending.owner.pack.random-reinforcements'),null);
});
test('round wins grant three cards exactly once, survive a match loss, and obey their own daily cap',async()=>{
 const store=firebase(),one=service(store);await one.loadProfile();await one.setRewardTarget('medic');const round={id:'lost-match',mode:'bot',round:1};
 await Promise.all([one.recordSoloRoundWin(round),one.recordSoloRoundWin(round)]);await one.recordSoloResult({id:'lost-match',mode:'bot',won:false});
 assert.equal(one.profile.troops.medic.cards,ROUND_WIN_CARDS);assert.equal(one.profile.losses,1);assert.equal(one.profile.forgeDust,0);
 const p=one.profile;p.rewardCounts.round=ROUND_WIN_DAILY_CAP;
 assert.deepEqual(settleSoloRoundWin(p,{...round,round:2}).receipt.cards,[]);assert.throws(()=>settleSoloRoundWin(p,{...round,round:8}),{code:'INVALID_ROUND'});
});
test('uncertain round rewards recover before match rewards without a duplicate grant',async()=>{
 const store=firebase(),cache=storage(),one=service(store,{storage:cache});await one.loadProfile();await one.setRewardTarget('medic');
 store.uncertain();await assert.rejects(one.recordSoloRoundWin({id:'saved-round',mode:'campaign',missionId:'forge-01',round:1}),{code:'unavailable'});one.dispose();
 const two=service(store,{storage:cache});await two.loadProfile();await two.recordSoloResult(result('saved-round'));assert.equal(two.profile.troops.medic.cards,9);assert.equal(two.profile.wins,1);assert.deepEqual(JSON.parse(cache.getItem('factionForge.solo.pending.owner.rounds')),[]);
});
test('scoped hub accounts isolate profiles, purchase receipts and pending request identifiers',async()=>{
 const store=firebase(),cache=storage(),commander=service(store,{storage:cache});await commander.loadProfile();store.docs.get('factionForgeSoloProfiles/owner').forgeDust=100;
 store.uncertain();await assert.rejects(commander.buyPack('vanir',{requestId:'commander-pack'}),{code:'unavailable'});
 const sean=service(store,{storage:cache,username:'Sean',profilePath:['factionForgeSoloProfiles','owner','accounts','Sean'],queueNamespace:'.name-Sean'});await sean.loadProfile();const path='factionForgeSoloProfiles/owner/accounts/Sean';store.docs.get(path).forgeDust=50;
 const purchase=await sean.buyPack('vanir',{requestId:'sean-pack'});assert.equal(purchase.transaction.requestId,'sean-pack');assert.equal(sean.profile.forgeDust,0);assert.equal(store.docs.get('factionForgeSoloProfiles/owner').forgeDust,50);
 assert.equal(cache.getItem('factionForge.solo.pending.owner.pack.vanir'),'commander-pack');assert.equal(store.docs.has(path+'/purchases/sean-pack'),true);
 await sean.recordSoloRoundWin({id:'scoped-round',mode:'bot',round:1});assert.equal(store.docs.has(path+'/rounds/scoped-round-round-1'),true);
 assert.throws(()=>service(store,{profilePath:['factionForgeSoloProfiles','intruder']}),{code:'INVALID_PROFILE'});
});
test('account switches during receipt reads and renamed stored accounts cannot write an old save',async()=>{
 const store=firebase(),auth={currentUser:{uid:'owner'}},one=service(store,{auth});await one.loadProfile();store.docs.get('factionForgeSoloProfiles/owner').forgeDust=100;
 const run=store.api.runTransaction;store.api.runTransaction=(_db,fn)=>run({},tx=>fn({...tx,get:async ref=>{const snap=await tx.get(ref);if(ref.includes('/purchases/'))auth.currentUser={uid:'other'};return snap;}}));
 await assert.rejects(one.buyPack('vanir',{requestId:'cross-account'}),{code:'ACCOUNT_CHANGED'});assert.equal(store.docs.get('factionForgeSoloProfiles/owner').forgeDust,100);assert.equal(store.docs.has('factionForgeSoloProfiles/owner/purchases/cross-account'),false);
 store.api.runTransaction=run;auth.currentUser={uid:'owner'};store.docs.get('factionForgeSoloProfiles/owner').username='Sean';await assert.rejects(one.buyPack('vanir'),{code:'ACCOUNT_CHANGED'});
});
test('a save connection closed after purchase commit retains its retry receipt',async()=>{
 const store=firebase(),cache=storage(),one=service(store,{storage:cache});await one.loadProfile();store.docs.get('factionForgeSoloProfiles/owner').forgeDust=50;
 const run=store.api.runTransaction;store.api.runTransaction=async(db,fn)=>{const response=await run(db,fn);one.dispose();return response;};
 await assert.rejects(one.buyPack('vanir',{requestId:'closed-after-commit'}),{code:'DISPOSED'});assert.equal(cache.getItem('factionForge.solo.pending.owner.pack.vanir'),'closed-after-commit');
 store.api.runTransaction=run;const two=service(store,{storage:cache});await two.loadProfile();await two.buyPack('vanir',{requestId:'new-dialog'});assert.equal(two.profile.forgeDust,0);assert.equal(two.profile.troops.rifleman.cards,5);
});

test('hub profile reopens with campaign, collection, packs and receipts after a new anonymous sign-in',async()=>{
 const store=firebase(),cache=storage();store.docs.set('users/Sean',{username:'Sean',gameStats:{anotherGame:{wins:7}}});
 const auth={currentUser:{uid:'first-browser'}};
 const selection=await selectSoloAccount({auth,db:{},api:store.api,username:'Sean',troops});
 const one=service(store,{auth,storage:cache,...selection});await one.loadProfile();await one.setRewardTarget('rifleman');
 await one.recordSoloRoundWin({id:'saved-match',mode:'campaign',missionId:'forge-01',round:1});await one.recordSoloResult(result('saved-match'));
 store.docs.get('users/Sean/factionForge/profile').forgeDust=150;store.docs.get('users/Sean/factionForge/profile').troops.rifleman.cards=20;
 await one.upgradeTroop('rifleman',{requestId:'saved-upgrade'});await one.buyPack('vanir',{requestId:'saved-pack'});one.dispose();
 auth.currentUser={uid:'second-browser'};
 const selected=await selectSoloAccount({auth,db:{},api:store.api,username:'Sean',troops});
 const two=service(store,{auth,storage:cache,...selected});await two.loadProfile();
 assert.equal(two.profile.uid,'second-browser');assert.equal(two.profile.forgeDust,0);assert.equal(two.profile.troops.rifleman.forgeTier,2);assert.equal(two.profile.troops.rifleman.cards,15);
 assert.equal(two.profile.campaign.missions['forge-01'].stars,3);assert.equal(two.profile.rewardTarget,'rifleman');assert.equal(two.profile.wins,1);
 await two.recordSoloResult(result('saved-match'));await two.upgradeTroop('rifleman',{requestId:'saved-upgrade'});await two.buyPack('vanir',{requestId:'saved-pack'});
 assert.equal(two.profile.forgeDust,0);assert.equal(two.profile.wins,1);assert.equal(two.profile.troops.rifleman.forgeTier,2);
 assert.deepEqual(store.docs.get('users/Sean'),{username:'Sean',gameStats:{anotherGame:{wins:7}}});
});

test('migration recovers unacknowledged legacy rewards and spending using the original receipts',async()=>{
 const store=firebase(),cache=storage(),auth={currentUser:{uid:'owner'}},legacy=service(store,{auth,storage:cache,username:'Sean'});await legacy.loadProfile();
 await legacy.setRewardTarget('rifleman');store.docs.get('factionForgeSoloProfiles/owner').forgeDust=150;store.docs.get('factionForgeSoloProfiles/owner').troops.rifleman.cards=20;
 store.uncertain();await assert.rejects(legacy.recordSoloRoundWin({id:'old-match',mode:'campaign',missionId:'forge-01',round:1}),{code:'unavailable'});
 store.uncertain();await assert.rejects(legacy.recordSoloResult(result('old-match')),{code:'unavailable'});
 store.uncertain();await assert.rejects(legacy.upgradeTroop('rifleman',{requestId:'old-upgrade'}),{code:'unavailable'});
 store.uncertain();await assert.rejects(legacy.buyPack('vanir',{requestId:'old-pack'}),{code:'unavailable'});legacy.dispose();
 const saved=structuredClone(store.docs.get('factionForgeSoloProfiles/owner'));store.docs.set('users/Sean',{username:'Sean'});
 const selected=await selectSoloAccount({auth,db:{},api:store.api,username:'Sean',troops,storage:cache});
 const current=service(store,{auth,storage:cache,...selected});await current.loadProfile();current.dispose();
 // Changing Firebase identity after migration must retain uncertain spending receipts too.
 auth.currentUser={uid:'new-sign-in'};
 const reopenedSelection=await selectSoloAccount({auth,db:{},api:store.api,username:'Sean',troops,storage:cache});
 const reopened=service(store,{auth,storage:cache,...reopenedSelection});await reopened.loadProfile();await reopened.upgradeTroop('rifleman',{requestId:'new-dialog'});await reopened.buyPack('vanir',{requestId:'new-dialog'});
 assert.deepEqual(reopened.profile,{...saved,uid:'new-sign-in'});assert.equal(cache.getItem('factionForge.solo.pending.hub.Sean.upgrade.rifleman'),null);assert.equal(cache.getItem('factionForge.solo.pending.hub.Sean.pack.vanir'),null);
 for(const [collection,id] of [['results','old-match'],['rounds','old-match-round-1'],['upgrades','old-upgrade'],['purchases','old-pack']])assert.equal(store.docs.has('users/Sean/factionForge/profile/'+collection+'/'+id),true);
});

test('same Firebase session switching hub profiles cannot mutate the previously selected save',async()=>{
 const store=firebase();let active=true;
 const one=service(store,{username:'Sean',profilePath:['users','Sean','factionForge','profile'],isCurrentAccount:()=>active});await one.loadProfile();active=false;
 await assert.rejects(one.recordSoloResult(result()),{code:'ACCOUNT_CHANGED'});
 assert.equal(store.docs.get('users/Sean/factionForge/profile').wins,0);
});

test('older rules denying scoped saves, round and pack receipts do not prevent canonical saves',async()=>{
 const store=firebase(),cache=storage(),auth={currentUser:{uid:'owner'}};
 store.docs.set('users/Sean',{username:'Sean'});const old=newSoloProfile('owner','Sean',troops);old.forgeDust=100;store.docs.set('factionForgeSoloProfiles/owner',old);
 cache.setItem('factionForge.solo.pending.owner.rounds',JSON.stringify([{id:'offline-round',mode:'bot',round:1}]));cache.setItem('factionForge.solo.pending.owner.pack.vanir','offline-pack');
 const get=store.api.getDoc;store.api.getDoc=async ref=>{if(/\/accounts\/|\/rounds\/|\/purchases\//.test(ref)&&ref.startsWith('factionForgeSoloProfiles/'))throw Object.assign(Error('Legacy path not allowed'),{code:'permission-denied'});return get(ref);};
 const selected=await selectSoloAccount({auth,db:{},api:store.api,username:'Sean',troops,storage:cache});
 const current=service(store,{auth,storage:cache,...selected});await current.loadProfile();await current.buyPack('vanir');await current.recordSoloRoundWin({id:'new-round',mode:'bot',round:1});
 assert.equal(current.profile.forgeDust,50);assert.equal(Object.values(current.profile.troops).reduce((n,unit)=>n+unit.cards,0),21);
 current.dispose();
 const run=store.api.runTransaction;store.api.runTransaction=(db,fn)=>run(db,tx=>fn({...tx,get:ref=>{assert.ok(!ref.startsWith('factionForgeSoloProfiles/'),'existing canonical profile must not read any legacy save');return tx.get(ref);}}));
 const again=await selectSoloAccount({auth,db:{},api:store.api,username:'Sean',troops,storage:cache});
 assert.deepEqual(again.legacyProfilePaths,[]);
});

