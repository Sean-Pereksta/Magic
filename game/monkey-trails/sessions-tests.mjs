import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import {regionChallenges} from './challenges.mjs';
const save=()=>C.newSave('A longer walk','capuchin','practice',1234);
function invite(s){C.advance(s,s.run.nextEncounter-s.run.distance);assert.equal(s.run.phase,'trail');assert.ok(s.run.pendingEncounter);return s.run.pendingEncounter;}
function win(s){C.readyBattle(s);s.run.monkey.level=60;s.run.monkey.hp=C.stats(s.run.monkey).hp;for(const p of s.run.battle.enemies)p.hp=1;while(s.run.phase==='battle')C.turn(s,'strike');}

test('one frozen ordinary encounter waits while walking discoveries training and recovery continue',()=>{
 const s=save(),p=s.run.monkey;p.hp=10;const b=structuredClone(invite(s)),distance=s.run.distance,discoveries=s.run.discoveries,xp=p.training,hp=p.hp;
 C.advance(s,500,500,0);assert.equal(s.run.distance,distance+500);assert.ok(s.run.discoveries>discoveries);assert.ok(p.training>xp);assert.ok(p.hp>=hp);assert.deepEqual(s.run.pendingEncounter,b);assert.equal(s.run.battle,null);
 C.advance(s,5000);assert.equal(s.run.phase,'camp');assert.equal(s.run.distance,C.CONFIG.campFeet*C.FEET);assert.deepEqual(s.run.pendingEncounter,b);C.leaveCamp(s);C.advance(s,2000);assert.equal(s.run.phase,'camp');assert.equal(s.run.events.filter(e=>e.type==='encounter').length,0);assert.deepEqual(s.run.pendingEncounter,b);
});

test('pending encounter survives reload and regional travel and credits the origin region',()=>{
 let s=save();const original=structuredClone(invite(s));s.world.totalDistance=2000;s.world.unlocked.push('sunstone-savannah');C.selectLand(s,'sunstone-savannah');s=C.validate(structuredClone(s));assert.deepEqual(s.run.pendingEncounter,original);C.beginPendingEncounter(s);assert.equal(C.advance(s,50),0);assert.equal(s.run.battle.originLandId,'emerald-jungle');win(s);
 assert.equal(regionChallenges(s,'emerald-jungle').find(g=>g.kind==='victories').current,1);assert.equal(regionChallenges(s,'sunstone-savannah').find(g=>g.kind==='victories').current,0);
});

test('passing grants no battle rewards and resets the interval from current accepted distance',()=>{
 const s=save();invite(s);C.advance(s,100);const before=structuredClone(s),distance=s.run.distance;C.passPendingEncounter(s);assert.equal(s.run.nextEncounter,distance+C.ENCOUNTER_STEP);assert.equal(s.run.wins,before.run.wins);assert.equal(s.run.fruit,before.run.fruit);assert.deepEqual(s.run.essence,before.run.essence);assert.deepEqual(s.collection,before.collection);assert.equal(s.run.capture,null);assert.throws(()=>C.passPendingEncounter(s));
 C.advance(s,C.ENCOUNTER_STEP-1);assert.equal(s.run.pendingEncounter,null);C.advance(s,1);assert.ok(s.run.pendingEncounter);
});

test('a safe-route Ultra and coincident camp retain the ordinary invitation',()=>{
 const s=save();s.run.ordinaryWins=2;s.run.distance=5000*C.FEET;s.run.nextEncounter=6000*C.FEET;s.run.nextCamp=6000*C.FEET;C.advance(s,1000*C.FEET);assert.equal(s.run.phase,'camp');const pending=structuredClone(s.run.pendingEncounter);C.leaveCamp(s);C.safeUltraAlternative(s);s.run.ultra.challengeRemaining=0;s.run.ultra.status='ready';C.beginUltra(s);assert.equal(s.run.phase,'battle');assert.equal(s.run.battle.ultra,true);assert.deepEqual(s.run.pendingEncounter,pending);win(s);C.skipBefriend(s);assert.equal(s.run.phase,'trail');assert.deepEqual(s.run.pendingEncounter,pending);
});

test('finishing a walking session keeps the expedition and invitation intact and is repeatable safely',()=>{
 const s=save();C.beginWalkSession(s);invite(s);C.advance(s,100);const run=structuredClone(s.run),world=structuredClone(s.world),collection=structuredClone(s.collection);const recap=C.finishWalkSession(s);assert.equal(recap.distance,s.run.distance);assert.equal(recap.discoveries,s.run.discoveries);assert.ok(recap.trainingXp>0);assert.deepEqual(s.run,run);assert.deepEqual(s.world,world);assert.deepEqual(s.collection,collection);assert.deepEqual(C.finishWalkSession(s),recap);
 C.beginWalkSession(s);C.advance(s,10);assert.equal(C.walkSessionPreview(s).distance,10);assert.equal(C.walkSessionPreview(s).recruited,0);assert.equal(s.run.id,run.id);
});

test('legacy active battles remain active and missing pending/session fields migrate additively',()=>{
 const s=save();invite(s);C.beginPendingEncounter(s);C.readyBattle(s);const before=structuredClone(s.run.battle);delete s.run.pendingEncounter;delete s.meta.walkSessions;const loaded=C.validate(structuredClone(s));assert.deepEqual(loaded.run.battle,before);assert.equal(loaded.run.phase,'battle');assert.equal(loaded.run.pendingEncounter,null);assert.equal(loaded.meta.walkSessions.version,1);
});


test('one accepted GPS sample crosses an ordinary invitation without losing its remainder',()=>{
 const s=save(),meters=C.ENCOUNTER_STEP+125;assert.equal(C.advance(s,meters,meters,0),meters);assert.equal(s.run.phase,'trail');assert.ok(s.run.pendingEncounter);assert.equal(s.run.pendingEncounter.originDistance,C.ENCOUNTER_STEP);assert.equal(s.run.distance,meters);assert.equal(s.world.regions[s.world.activeLand].position.east,meters);
});
