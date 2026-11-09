import test from 'node:test';
import assert from 'node:assert/strict';
import {Game,TYPES} from '../engine.js';

function arena(){
 const game=new Game();game._think=Infinity;
 game.map.obstacles=[];game.map.plateaus=[];game.map.ramps=[];game.map.cliffs=[];
 game.entities=game.entities.filter(e=>e.type==='desk');
 game.canSee=()=>true;game.isVisible=()=>true;
 return game;
}
function advance(game,seconds){for(let elapsed=0;elapsed<seconds;elapsed+=.04)game.update(.04);}

test('all troop tiers and transports receive the higher health budget without changing worker or building health',()=>{
 const expected={rock:180,paper:180,scissors:180,sniper:150,airplane:140,mage:200,divider:280,rollingrock:640,shears:300,cd:320};
 for(const [type,hp] of Object.entries(expected))assert.equal(TYPES[type].hp,hp,type);
 assert.equal(TYPES.twig.hp,40);assert.equal(TYPES.desk.hp,850);
});

test('a matching duel survives its opening exchange long enough to retreat',()=>{
 const game=arena(),a=game._spawn('rock',0,1400,1400),b=game._spawn('rock',1,1440,1400);
 advance(game,3);assert(a.hp>0&&b.hp>0);
 const health=a.hp;game.command([a.id],{type:'move',x:1100,y:1400});
 advance(game,2);assert(a.x<1250,'retreat creates real separation during a longer engagement');
 assert(a.hp>0&&a.hp<=health);
});

test('a healthy Paper survives one Sweep; a damaged Paper can still be finished',()=>{
 const game=arena();game.teams[0].upgrades.scissors=true;
 const attacker=game._spawn('scissors',0,1400,1400),healthy=game._spawn('paper',1,1490,1400),damaged=game._spawn('paper',1,1480,1430);
 damaged.hp=100;
 assert(game.ability([attacker.id],1600,1400).ok);
 assert.equal(healthy.hp,45);assert(damaged.hp<=0);
});

test('combat impacts expose direction, family, strength and lethal state to the renderer',()=>{
 const game=arena(),source=game._spawn('rock',0,1400,1400),target=game._spawn('scissors',1,1440,1400);
 game._damage(target,55,source);
 const impact=game.effects.find(e=>e.type==='hit');
 assert.deepEqual({family:impact.family,sourceType:impact.sourceType,targetType:impact.targetType,fromX:impact.fromX,fromY:impact.fromY,damage:impact.damage,lethal:impact.lethal},
 {family:'rock',sourceType:'rock',targetType:'scissors',fromX:1400,fromY:1400,damage:55,lethal:false});
 game._damage(target,target.hp,source);assert.equal(game.effects.at(-1).lethal,true);
 advance(game,.5);assert(!game.effects.some(e=>e.type==='hit'),'short impact effects expire');
});

test('Launch emits a takeoff and a landing effect at the correct positions',()=>{
 const game=arena();game.teams[0].upgrades.rock=true;
 const rock=game._spawn('rock',0,1400,1400);
 assert(game.ability([rock.id],1600,1400).ok);
 const takeoff=game.effects.find(e=>e.type==='abilityCast');
 assert.equal(takeoff.ability,'launch');assert.equal(takeoff.x,1400);assert.equal(takeoff.toX,1600);
 advance(game,.7);const landing=game.effects.find(e=>e.type==='leap');
 assert(landing);assert.equal(landing.x,1600);assert.equal(landing.fromX,1400);
});
