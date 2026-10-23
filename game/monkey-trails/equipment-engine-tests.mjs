import test from 'node:test';
import assert from 'node:assert/strict';
import * as C from './core.mjs';
import * as S from './social-combat.mjs';
import {damagePreview} from './battle-guide.mjs';

function save(gear=[]){const s=C.newSave('Equipment events','capuchin','gps',881);s.run.monkey.loadout=['strike','brace','breathe'];for(const[i,id]of gear.entries()){s.meta.gear[id]=1;C.equip(s,id,undefined,['weapon','armor','charm'][i]);}return s;}
function reserve(s,gear=[]){const p=structuredClone(save().run.monkey);p.id=`reserve-${s.run.party.length}`;s.collection[p.id]=p;s.run.party.push(p.id);for(const[i,id]of gear.entries()){s.meta.gear[id]=1;C.equip(s,id,p.id,['weapon','armor','charm'][i]);}return p;}
function battle(s){const e=structuredClone(save().run.monkey);e.id='equipment-enemy';e.species='mandrill';e.types=['earth'];e.hp=10000;e.energy=100;e.gear={};s.run.phase='battle';s.run.battle={id:'equipment-engine',enemies:[e],enemy:e,enemyActive:0,round:1,turn:1,ready:true,intent:'breathe',enemyAction:{type:'move',id:'breathe'},rewarded:false,bondBonuses:{},pendingRally:null,enemyRally:null};return s;}
function act(s,id='breathe'){s.run.battle.enemyAction={type:'move',id:'breathe'};s.run.battle.intent='breathe';C.turn(s,id);}
function switched(s,id){s.run.battle.enemyAction={type:'move',id:'breathe'};s.run.battle.intent='breathe';C.switchMonkey(s,id);}
const dealt=s=>s.run.battle.hitFeedback.find(hit=>hit.side==='player')?.damage||0;
const unit=(b,p)=>b.equipmentBattle.units[p.equipmentBattleKey];
function members(gear=[]){return S.snapshotTeam(save(gear));}
function duel(gear=[],other=members()){return S.newDuel({id:'alice',team:members(gear)},{id:'bob',team:other},776);}
function socialAct(d,id='breathe'){return S.duelAction(d,d.players[d.turn],{type:'move',id});}

test('expedition first-attack equipment is consumed once and its locked rank survives battle JSON resume',()=>{
 const s=battle(save(['cinderOpener'])),plain=battle(save());act(s,'strike');act(plain,'strike');assert.ok(dealt(s)>dealt(plain));
 const resume=C.validate(JSON.parse(JSON.stringify(s)));C.validate(s);act(s,'strike');act(resume,'strike');act(plain,'strike');assert.equal(dealt(s),dealt(plain));assert.deepEqual(s,resume);
 assert.equal(unit(s.run.battle,s.run.monkey).used.firstAttackDamage,true);
});

test('expedition turn triggers count actions by the wearer and voluntary switches keep activation history',()=>{
 const s=save(['springbudLocket','dawnthreadBracelet','tempestSpurs']),p=s.run.monkey,q=reserve(s,['tidecallShell']);p.hp=5;q.hp=5;battle(s);
 act(s);const afterFirst=p.hp;assert.equal(afterFirst,5+Math.round(C.stats(p,C.equipped(s,p)).hp*.04));
 switched(s,q.id);assert.ok(q.hp>5);assert.equal(unit(s.run.battle,p).turns,2);assert.equal(unit(s.run.battle,q).turns,0);
 switched(s,p.id);assert.equal(unit(s.run.battle,p).turns,2);act(s);assert.equal(unit(s.run.battle,p).turns,3);assert.ok(p.hp>afterFirst);
 switched(s,q.id);const prior=q.hp;switched(s,p.id);switched(s,q.id);assert.equal(q.hp,prior);
});

test('expedition Beacon sends the revived active wearer to reserve and cancels its queued action',()=>{
 const s=save(),p=s.run.monkey,q=reserve(s,['beaconOfTheFirstDawn']);p.hp=1;p.species='mandrill';q.hp=C.stats(q,C.equipped(s,q)).hp;battle(s);
 const e=s.run.battle.enemy;e.species='macaque';e.level=60;e.types=['fire'];e.hp=10000;s.run.battle.enemyAction={type:'move',id:'strike'};s.run.battle.intent='strike';
 C.turn(s,'strike');assert.equal(s.run.activeId,q.id);assert.ok(p.hp>0);assert.equal(s.run.battle.hitFeedback.filter(row=>row.side==='player').length,0);
 assert.equal(s.run.battle.equipmentBattle.teamUsed[0].beacon,true);
});

test('a counter-knocked-out relay user revived by Beacon cannot perform a second voluntary switch',()=>{
 const s=save(),p=s.run.monkey,q=reserve(s,['beaconOfTheFirstDawn']),r=reserve(s);p.hp=1;p.learned.push('branchRelay');p.loadout=['strike','branchRelay','breathe'];battle(s);s.run.battle.enemy.counter=1;
 C.turn(s,'branchRelay',false,{targetId:r.id});assert.equal(s.run.activeId,q.id);assert.ok(p.hp>0);assert.equal(unit(s.run.battle,r).turns,0);
});

test('new equipment damage previews are pure and bound actual capped hits before and after activation',()=>{
 for(let seed=1;seed<=12;seed++){
  const s=save(['cinderOpener','duskglassLens','nightfangTooth']);s.rng=seed;preset(s);battle(s);s.run.battle.enemy.gear={duskBand:1,heartOfTheFirstMountain:1,frostButton:1};s.run.battle.enemy.shield=9;
  for(let turn=0;turn<2;turn++){
   const before=JSON.stringify(s),preview=damagePreview(s,'cinder');assert.equal(JSON.stringify(s),before);act(s,'cinder');const actual=s.run.battle.hitFeedback.find(hit=>hit.side==='player');assert.ok(actual.damage>=preview.min&&actual.damage<=(actual.critical?preview.criticalMax:preview.max));
  }
 }
 function preset(s){s.run.monkey.learned.push('cinder');s.run.monkey.types=['fire'];s.run.monkey.loadout=['strike','cinder','breathe'];}
 const d=duel(['cinderOpener']);socialAct(d,'strike');socialAct(d);const before=JSON.stringify(d),preview=damagePreview(null,'strike',{actor:d.teams[0][0],target:d.teams[1][0],socialVersion:4,battle:d});assert.equal(JSON.stringify(d),before);socialAct(d,'strike');assert.ok(d.lastHit.damage>=preview.min&&d.lastHit.damage<=(d.lastHit.critical?preview.criticalMax:preview.max));
});

test('social v4 applies entry shields and support hooks while v3 preserves released behavior',()=>{
 const source=members(['glacierAegis','dawnthreadBracelet']),old=S.newDuel({id:'alice',team:source},{id:'bob',team:members()},776,3),current=S.newDuel({id:'alice',team:source},{id:'bob',team:members()},776,4);
 assert.ok(current.teams[0][0].shield>0);assert.equal(old.teams[0][0].shield,0);
 old.teams[0][0].hp=current.teams[0][0].hp=5;socialAct(old);socialAct(current);assert.equal(old.teams[0][0].hp,5);assert.ok(current.teams[0][0].hp>5);
 const copy=JSON.parse(JSON.stringify(current));socialAct(current);socialAct(copy);assert.deepEqual(current,copy);
});

test('social v4 allows independent same-name copies across teammates and keeps three/four item capacity',()=>{
 const s=save(['dawnthreadBracelet']),p=reserve(s);p.gear={dawnthreadBracelet:1};const source=members(['dawnthreadBracelet']);source.push({...structuredClone(source[0]),id:'second'});
 const d=duel([],source);assert.equal(d.teams[1].length,2);assert.ok(d.teams[1].every(p=>p.gear.dawnthreadBracelet));
 assert.throws(()=>S.newDuel({id:'alice',team:members()},{id:'bob',team:source},776,3),/unique companion owner/);
 const allGear={bambooKnuckles:1,pebbleGuard:1,emberFang:1,breezeRibbon:1,riverglassShell:1};const standard=members()[0];standard.gear=allGear;
 assert.equal(Object.keys(S.newRaidAttempt([standard]).team[0].gear).length,3);
 standard.species=Object.keys(C.SPECIES).find(id=>C.SPECIES[id].rarity==='ultra');standard.learned=[];standard.loadout=['strike','brace','breathe'];
 assert.equal(Object.keys(S.newRaidAttempt([standard]).team[0].gear).length,4);
});

test('social knockout Beacon revival chooses a different active ally and raid effect state persists',()=>{
 const source=members();source.push({...structuredClone(source[0]),id:'beacon',gear:{beaconOfTheFirstDawn:1}});
 const d=duel([],source);d.teams[1][0].hp=1;socialAct(d,'strike');assert.equal(d.active[1],1);assert.ok(d.teams[1][0].hp>0);
 const raid=S.newRaidAttempt(members(['cinderOpener','springbudLocket'])),copy=JSON.parse(JSON.stringify(raid));raid.team[0].hp=copy.team[0].hp=5;
 for(let i=0;i<4;i++){S.raidAction(raid,{type:'move',id:i?'breathe':'strike'});S.raidAction(copy,{type:'move',id:i?'breathe':'strike'});}
 assert.deepEqual(raid,copy);assert.equal(unit(raid,raid.team[0]).turns,4);assert.equal(unit(raid,raid.team[0]).used.firstAttackDamage,true);assert.ok(raid.team[0].hp>5);
});
