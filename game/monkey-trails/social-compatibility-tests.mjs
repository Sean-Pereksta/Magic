import test from 'node:test';
import assert from 'node:assert/strict';
import {newSave} from './core.mjs';
import {snapshotTeam} from './social-combat.mjs';
import {SocialStore,createMemorySocialAdapter,nearbyPlayers} from './social-store.mjs';

// Exercise the client against the original v1 collection and field contract.
// The separate emulator suite remains responsible for security enforcement.
const optional=/^(applications|tradeLocks|tribePublicActivity)\//;
const tribeKeys=['id','code','name','ownerId','emblem','members','boss','createdAt','updatedAt'].sort();
function legacyFixture(){
 const base=createMemorySocialAdapter(),attempts=[],now=1800000000000;
 let serial=0;
 const check=path=>{attempts.push(path);if(optional.test(path))throw Object.assign(Error('Missing or insufficient permissions.'),{code:'permission-denied'});};
 const adapter={...base,
  async get(path){check(path);return base.get(path);},
  async list(path,...args){check(`${path}/`);return base.list(path,...args);},
  transaction(callback){return base.transaction(tx=>callback({
   async get(path){check(path);return tx.get(path);},
   set(path,value){check(path);if(path.startsWith('tribes/'))assert.deepEqual(Object.keys(value).sort(),tribeKeys);if(path.startsWith('players/'))assert.ok(value.name.length<=30);if(path.startsWith('presence/')&&value.location){assert.ok(value.location.accuracy<=500);assert.deepEqual(Object.keys(value.location).sort(),['accuracy','at','lat','lon']);}tx.set(path,value);},
   delete(path){check(path);tx.delete(path);}
  }));}
 };
 const team=snapshotTeam(newSave('Fixture','capuchin','gps',12345));
 return{base,adapter,attempts,now,team,async player(uid,name=`Trail ${uid}`){const store=new SocialStore(adapter,{uid,now:()=>now,id:()=>`LEGACY${++serial}X`});await store.register({saveId:`save-${uid}`,name,team});await store.heartbeat();return store;}};
}

test('original v1 rules support open tribe creation, joining, public browsing and leaving',async()=>{
 const f=legacyFixture(),a=await f.player('a'),b=await f.player('b');
 const [caps,again]=await Promise.all([a.initializeCompatibility(),a.initializeCompatibility()]);
 assert.equal(caps,again);assert.deepEqual(caps,{tribeApplications:false,tradeLocks:false,publicActivity:false});
 const tribe=await a.createTribe({name:'Canopy Crew'});assert.deepEqual(Object.keys(tribe).sort(),tribeKeys);
 await b.joinTribe(tribe.id);assert.equal((await b.getPlayer()).tribeId,tribe.id);
 await a.updateTribe({name:'Updated Crew',acceptingApplications:true,emblem:{symbol:'crown'}});
 assert.equal((await a.listDirectory({search:'updated',availableOnly:true})).length,1);
 const profile=await b.getPublicTribe(tribe.id);assert.equal(profile.members.length,2);assert.equal(profile.tribe.weeklyContribution,null);
 await b.leaveTribe();await a.leaveTribe();assert.deepEqual((await f.base.get(`tribes/${tribe.id}`)).members,[]);
 await b.joinTribe(tribe.id);assert.equal((await f.base.get(`tribes/${tribe.id}`)).ownerId,'b');
 assert.deepEqual(f.attempts.filter(path=>optional.test(path)),['applications/a-0','applications/b-0']);
});

test('original v1 trades preserve escrow and pending-trade leave checks without newer lock paths',async()=>{
 const f=legacyFixture(),a=await f.player('a'),b=await f.player('b'),tribe=await a.createTribe({name:'Trade Crew'});await b.joinTribe(tribe.id);
 await a.deposit('fruit',20,'a-fund');await b.deposit('berry',3,'b-fund');await a.gift('b','fruit',2);
 const offer=await a.offerTrade('b',{asset:'fruit',amount:4},{asset:'berry',amount:1});
 await assert.rejects(a.leaveTribe(),/pending trades/);await assert.rejects(b.leaveTribe(),/pending trades/);
 await b.acceptTrade(offer.id);assert.equal((await a.getPlayer()).wallet.items.berry,1);assert.equal((await b.getPlayer()).wallet.fruit,6);
 const cancelled=await a.offerTrade('b',{asset:'fruit',amount:2},{asset:'berry',amount:1});await b.cancelTrade(cancelled.id);
 assert.equal((await a.getPlayer()).wallet.fruit,14);await b.leaveTribe();
 assert.equal(Object.keys(f.base.dump()).some(path=>path.startsWith('tradeLocks/')),false);
});

test('original v1 adventures contribute and claim without a public activity write',async()=>{
 const f=legacyFixture(),a=await f.player('a');await a.createTribe({name:'Adventure Crew'});
 const zero={meters:0,discoveries:0,challenges:0,raids:0,trades:0};await a.joinAdventure(zero);
 const member=await a.contributeAdventure({...zero,meters:30000});assert.equal(member.points,600);
 assert.equal((await a.claimAdventure()).reward.fruit,6);
 assert.equal(Object.keys(f.base.dump()).some(path=>path.startsWith('tribePublicActivity/')),false);
 await assert.rejects(a.listTribeRankings('weekly'),/weekly rankings are unavailable/);
 await assert.rejects(a.createTribe({name:'Approval Crew',admissionMode:'approval'}),/support open tribes/);
 await assert.rejects(a.updateTribe({acceptingApplications:false}),/support open tribes/);
 await assert.rejects(a.reviewApplication('b-0',true),/applications are unavailable/);
});

test('original v1 tribe challenges, duels and raid attempts continue to work',async()=>{
 const f=legacyFixture(),a=await f.player('a'),b=await f.player('b'),tribe=await a.createTribe({name:'Battle Crew'});await b.joinTribe(tribe.id);await a.heartbeat();await b.heartbeat();
 const challenge=await a.challenge('b'),duel=await b.acceptChallenge(challenge.id);await b.surrender(duel.id,0);
 assert.equal((await f.base.get(`duels/${duel.id}`)).settled,true);await a.returnToTrail();await b.returnToTrail();await a.heartbeat();
 const raid=await a.startRaid();assert.equal(raid.tribeId,tribe.id);await a.abandonRaid(raid.id);assert.equal((await a.getPlayer()).activeRaid,null);
});

test('social profile names and nearby presence fit the original v1 limits',async()=>{
 const f=legacyFixture(),a=await f.player('a','A'.repeat(32));assert.equal((await a.getPlayer()).name,'A'.repeat(30));
 const point={lat:40,lon:-74,accuracy:500,at:f.now,altitude:123};await a.heartbeat({discoverable:true,location:point});
 assert.deepEqual((await f.base.get('presence/a')).location,{lat:40,lon:-74,accuracy:500,at:f.now});
 await a.heartbeat({discoverable:true,location:{...point,accuracy:501}});assert.equal((await f.base.get('presence/a')).location,null);
 const presence={online:true,busy:false,discoverable:true,seenAt:f.now,expiresAt:f.now+90000,location:{...point,accuracy:501}};
 assert.deepEqual(nearbyPlayers([{...presence,uid:'a'},{...presence,uid:'b'}],'a',f.now),[]);
});

test('capability probing preserves newer features and propagates recoverable network errors',async()=>{
 const base=createMemorySocialAdapter(),modern=new SocialStore(base,{uid:'modern'});assert.deepEqual(await modern.initializeCompatibility(),{tribeApplications:true,tradeLocks:true,publicActivity:true});
 let requests=0;const adapter={...base,async get(path){if(!requests++)throw Object.assign(Error('Offline'),{code:'unavailable'});return base.get(path);}},store=new SocialStore(adapter,{uid:'retry'});
 await assert.rejects(store.initializeCompatibility(),/Offline/);assert.equal(store.capabilities,undefined);
 assert.equal((await store.initializeCompatibility()).tribeApplications,true);assert.equal(requests,2);
});
