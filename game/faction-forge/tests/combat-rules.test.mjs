import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import {createRequire} from 'node:module';
import * as units from '../units.js';
import {calculateCounterDamage,counterProfile,interceptTarget,chooseTacticalTarget,chooseCombatMove,keepFiringDistance,chooseFlankLanding} from '../combat-rules.js';
const require=createRequire(import.meta.url);
const {loadModules,createCombatAdapter,extractFunction}=require('../../../server/faction-forge/simulator.cjs');
const {BUILDS,stateFor}=require('../balance-benchmark.cjs');
const make=(type,props={})=>({id:type,type,p:0,hp:30,maxhp:30,x:3,y:8,size:1,range:1,dmg:5,armor:0,cd:1500,...props});
const distance=(a,b)=>Math.max(Math.abs(a.x-b.x),Math.abs(a.y-b.y));
const hooks={distance,width:8,canMoveTo:(u,all,x,y)=>x>=0&&x<8&&y>=0&&y<16&&!all.some(v=>v!==u&&v.hp>0&&v.x===x&&v.y===y)};

test('simultaneous combat priority alternates deterministically between seats',()=>{
  const all=[make('guardian',{p:0}),make('phaser',{p:0}),make('viper',{p:1}),make('roach',{p:1})];
  assert.deepEqual(units.combatTurnOrder(all,1180,{startedAt:1000}).map(u=>u.p),[1,1,0,0]);
  assert.deepEqual(units.combatTurnOrder(all,1360,{startedAt:1000}).map(u=>u.p),[0,0,1,1]);
  assert.deepEqual(all.map(u=>u.p),[0,0,1,1]);
});

test('precision penetrates actual armor; counter modifiers are bounded and fast swarms resist deliberate shots',()=>{
  const marksman=make('marksman',{range:6}),tank=make('guardian',{armor:3,p:1});
  const normal=calculateCounterDamage(make('rifleman',{range:6}),tank,6,{now:1000});
  const shot=calculateCounterDamage(marksman,tank,6,{now:1000});
  assert.equal(shot.penetration,.65);assert.equal(shot.damage,6);assert.equal(normal.damage,3);
  assert.equal(calculateCounterDamage({...marksman,evolution:'a'},tank,6,{now:1000}).penetration,.85);
  assert.equal(calculateCounterDamage(marksman,make('hellspawn',{p:1}),6).damage,5);
  for(const a of ['marksman','railbreaker','viper','storm','suppressor','guardian'])for(const b of ['guardian','medic','blood','viper'])assert.ok(counterProfile(make(a),make(b)).multiplier<=1.35);
  assert.equal(calculateCounterDamage(marksman,tank,0).damage,0);
});

test('corrosion strips armor temporarily, magic penetrates armor, and secondary hits do not multiply counter bonuses',()=>{
  const tank=make('guardian',{armor:3,p:1,corrosionUntil:2000}),phaser=make('phaser',{range:5});
  assert.ok(calculateCounterDamage(phaser,tank,4,{now:1000}).damage>calculateCounterDamage(phaser,tank,4,{now:3000}).damage);
  assert.equal(counterProfile(phaser,tank).penetration,.5);
  assert.equal(calculateCounterDamage(make('vrocket'),make('hellspawn'),10,{secondary:true}).damage,10);
  assert.equal(calculateCounterDamage(make('vrocket'),make('hellspawn'),10).damage,13);
  assert.equal(calculateCounterDamage(phaser,{...tank,corrosionUntil:500,statusAcidUntil:3000},5,{now:1000}).damage,calculateCounterDamage(phaser,tank,5,{now:1000}).damage);
});

test('guard interception requires a nearby facing lane, respects cooldown and control, and penetration bypasses it',()=>{
  const shooter=make('rifleman',{p:0,x:3,y:9,range:6}),medic=make('medic',{p:1,x:3,y:4,range:3}),guard=make('guardian',{p:1,x:3,y:5}),all=[shooter,medic,guard];
  assert.equal(interceptTarget(shooter,medic,all,1000,hooks),guard);
  assert.equal(interceptTarget(shooter,medic,all,1200,hooks),medic);
  assert.equal(interceptTarget(shooter,medic,all,3300,hooks),guard);
  guard.nextInterceptAt=0;guard.x=6;assert.equal(interceptTarget(shooter,medic,all,4000,hooks),medic);
  guard.x=3;guard.y=3;assert.equal(interceptTarget(shooter,medic,all,4000,hooks),medic);
  guard.y=5;guard.statusShockUntil=5000;assert.equal(interceptTarget(shooter,medic,all,4000,hooks),medic);
  guard.statusShockUntil=0;assert.equal(interceptTarget({...shooter,type:'marksman'},medic,all,4000,hooks),medic);
});

test('tactical targeting recognizes heavy armor, fragile supports, clusters and threats to allies',()=>{
  const marksman=make('marksman',{range:6}),tank=make('guardian',{p:1,x:4,y:5,armor:3}),light=make('rifleman',{p:1,x:3,y:6,range:6});
  assert.equal(chooseTacticalTarget(marksman,[marksman,tank,light],1000,hooks),tank);
  const assassin=make('viper'),support=make('medic',{p:1,x:3,y:5,range:3});
  assert.equal(chooseTacticalTarget(assassin,[assassin,tank,support],1000,hooks),support);
  const rocket=make('vrocket',{range:8}),isolated=make('hellspawn',{id:'isolated',p:1,x:3,y:6}),clump=make('hellspawn',{id:'clump',p:1,x:5,y:5}),neighbor=make('roach',{p:1,x:6,y:5,range:3});
  assert.equal(chooseTacticalTarget(rocket,[rocket,isolated,clump,neighbor],1000,hooks),clump);
  const guard=make('guardian'),carry=make('marksman',{id:'carry',x:4,y:9,range:6}),threat=make('viper',{p:1,x:4,y:8}),other=make('guardian',{id:'other',p:1,x:3,y:7});
  assert.equal(chooseTacticalTarget(guard,[guard,carry,threat,other],1000,hooks),threat);
});

test('movement is deterministic, avoids occupancy, holds useful range, and flanks within actual leap distance',()=>{
  const rocket=make('vrocket',{range:8}),target=make('guardian',{p:1,x:3,y:4});
  assert.equal(chooseCombatMove(rocket,target,[rocket,target],hooks),null);
  const threat=make('viper',{p:1,x:3,y:7}),all=[rocket,threat];
  assert.equal(keepFiringDistance(rocket,all,1000,hooks),true);assert.ok(distance(rocket,threat)>1);
  const position={x:rocket.x,y:rocket.y};assert.equal(keepFiringDistance(rocket,all,1100,hooks),false);assert.deepEqual({x:rocket.x,y:rocket.y},position);
  const assassin=make('viper',{x:0,y:9}),prey=make('medic',{p:1,x:3,y:5}),blocker=make('guardian',{p:1,x:2,y:7});
  const move=chooseFlankLanding(assassin,prey,[assassin,prey,blocker],2,hooks);
  assert.ok(move);assert.ok(distance(assassin,move)<=2);assert.ok(distance(move,prey)<distance(assassin,prey));assert.notDeepEqual({x:move.x,y:move.y},{x:blocker.x,y:blocker.y});
  assert.deepEqual(move,chooseFlankLanding(assassin,prey,[assassin,prey,blocker],2,hooks));
});

test('every summoned body and illusion consumes the same five active slots; dead bodies release slots',()=>{
  const parent=make('matron'),all=[parent,...Array.from({length:4},(_,i)=>make('hellspawn',{id:'s'+i,summoned:i<1,illusion:i===1}))],st={field:{units:all}};
  assert.equal(units.canSpawnSummon(st,parent),false);assert.equal(units.canSpawnSummon(st,parent,true),false);
  all[4].hp=0;assert.equal(units.canSpawnSummon(st,parent),true);
  parent.hp=0;assert.equal(units.canSpawnSummon(st,parent),false);
});

test('a full five-unit formation retains Mirage Warden identity through a temporary shield',()=>{
  const warden=make('miragewarden',{range:3}),ally=make('phaser',{hp:5,maxhp:20,x:4,y:8,range:5}),enemy=make('vguard',{p:1,x:3,y:5});
  const st={field:{units:[warden,ally,make('guardian'),make('storm'),make('runebinder'),enemy]}};
  const events=[];units.tickForgeAbilities(st,1000,{...hooks,event:event=>events.push(event)});
  assert.equal(st.field.units.length,6);assert.ok(ally.comboShield>0);assert.equal(ally.comboShieldUntil,3200);assert.ok(events.some(event=>event.kind==='mirage'));
});

test('actual damage clamps combined shield sources before absorption',()=>{
  const html=readFileSync(new URL('../../crowncouncil.html',import.meta.url),'utf8');
  class VirtualDate extends Date{static now(){return 1000;}}
  const context=vm.createContext({Date:VirtualDate,FFUnits:units,forgeCombatHooks:()=>({event(){}}),addToxicSplash(){}});
  vm.runInContext(extractFunction(html,'applyDamage')+'\nthis.hit=applyDamage;',context);
  const victim=make('guardian',{startShield:3,startShieldUntil:2000,comboShield:3,comboShieldUntil:2000});
  context.hit(victim,6,{field:{units:[victim]}});assert.equal(victim.hp,28);assert.equal(victim.startShield+victim.comboShield,0);
});

test('actual five-body army includes every starter troop and gives defenders lower personal DPS than carries',async()=>{
  const modules=await loadModules(),adapter=createCombatAdapter({seed:'body-identity',modules});
  const battle=adapter.battle(stateFor(BUILDS.vanguard,BUILDS.tempest,1));
  for(const p of [0,1])assert.equal(battle.frames[0].units.filter(u=>u.p===p).length,5);
  assert.equal(new Set(battle.frames[0].units.filter(u=>u.p===1).map(u=>u.type)).size,5);
  for(const [guard,carry] of [['vguard','marksman'],['guardian','phaser'],['golem','storm']]){
    const a=adapter.troops[guard].base,b=adapter.troops[carry].base;assert.ok(a.dmg/a.cd<b.dmg/b.cd,`${guard} must protect ${carry}`);
  }
  const again=createCombatAdapter({seed:'body-identity',modules}).battle(stateFor(BUILDS.vanguard,BUILDS.tempest,1));assert.deepEqual(battle,again);
});

test('all five formations keep legal mirrored deployments and generate meaningfully different layouts',async()=>{
  const modules=await loadModules(),layouts=[];
  for(const formation of ['balanced','wide','spearhead','protected_backline','ambush']){
    const battle=createCombatAdapter({seed:'formation-'+formation,modules}).battle(stateFor(BUILDS.assassins,BUILDS.assassins,1,formation,formation));
    const first=battle.frames[0].units;
    for(const p of [0,1]){const own=first.filter(u=>u.p===p);assert.equal(own.length,5);assert.equal(new Set(own.map(u=>`${u.x},${u.y}`)).size,5);assert.ok(own.every(u=>p===0?u.y>=8:u.y<8));}
    const bottom=first.filter(u=>u.p===0),top=first.filter(u=>u.p===1);for(let i=0;i<bottom.length;i++){assert.equal(bottom[i].x,top[i].x);assert.equal(bottom[i].y+top[i].y,15);}
    layouts.push(JSON.stringify(first.filter(u=>u.p===0).map(({x,y})=>({x,y}))));
  }
  assert.ok(new Set(layouts).size>=4);
});
