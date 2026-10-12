'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');

function arena(){
 const c=loadEngine(),g=new c.ATSGame('PRISON-CHAMPIONS');
 g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();g.world.ensure=()=>{};g.world.stream=()=>{};g.world.getSites=()=>[];
 g.world.terrain=()=>({biome:'forest',water:false});g.world.waterBlocked=()=>false;g.world.lineClear=()=>true;g.spawnSites=()=>{};
 g.nextDirectorAt=g.nextConvoyAt=g.heliTimer=1e9;g.king.x=-500;return {c,g};
}
function ape(g,species='gorilla',x=0,y=0){const a=g.makeApe(x,y,'follow');a.species=species;g.siege.balance(a,true);a.attackCD=0;return a}
function grids(g){g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);g.humanGrid.rebuild(g.humans);g.vehicleGrid.rebuild(g.vehicles);g.navigation.beginFrame(g.time);g.performance.beginStep(g)}
function promote(g,a,options={}){return g.champions.promote(a,{prisonChampion:true,sourceId:'high-security-cell',...options})}

test('prison champions retain each ordinary species with 5.5x normal health and no extra monkey form',()=>{
 const {g,c}=arena();
 for(const species of Object.keys(c.ATSSpeciesHealth)){
  const a=ape(g,species),base=a.maxHp;a.hp-=13;
  assert.equal(promote(g,a),a);assert.equal(a.species,species);assert.equal(a.elite,undefined);
  assert.equal(a.maxHp,base*5.5);assert.equal(a.maxHp-a.hp,13);assert.equal(a.champion.prisonChampion,true);
  assert.equal(a.champion.damageMultiplier,1.8);assert.equal(a.champion.passive,'Banner of freedom');
  const maximum=a.maxHp;promote(g,a);assert.equal(a.maxHp,maximum,'repeated promotion cannot stack health');
 }
});

test('cage champions bypass the named-champion cap without consuming its slots or becoming veterans',()=>{
 const {g}=arena();for(let i=0;i<30;i++){const a=ape(g);assert.equal(promote(g,a),a);assert.equal(a.veteran,undefined)}
 for(let i=0;i<24;i++){const a=ape(g);assert.equal(g.champions.promote(a),a)}
 const extra=ape(g);assert.equal(g.champions.promote(extra),null);assert.ok(extra.veteran);
 for(let i=0;i<2;i++){const a=ape(g);assert.equal(promote(g,a),a);assert.equal(a.veteran,undefined)}
 assert.equal(g.apes.filter(a=>a.champion?.prisonChampion).length,32);
});

test('promoting an already armored follower matches fitting its armor after promotion',()=>{
 const {g,c}=arena();
 for(const species of Object.keys(c.ATSSpeciesHealth)){
  const before=ape(g,species),after=ape(g,species);g.equipment.fitItem(before,'heavyArmor');before.hp-=19;
  promote(g,before);promote(g,after);g.equipment.fitItem(after,'heavyArmor');
  assert.equal(before.maxHp,after.maxHp);assert.equal(before.maxHp-before.hp,19);
  g.siege.balance(before,true);assert.equal(before.maxHp,after.maxHp);assert.equal(before.maxHp-before.hp,19);
 }
});

test('champion strength reaches real melee, structure and ranged attack damage',()=>{
 const {g}=arena(),ordinary=ape(g),champion=ape(g);promote(g,champion,{archetype:'tankBuster'});
 const human=g.makeHuman(20,0,null);human.hp=human.maxHp=10000;g.forces.armor=(_a,n)=>n;
 grids(g);let before=human.hp;assert.equal(g.siege.attack(ordinary,human,'human',.1),true);const melee=before-human.hp;
 before=human.hp;assert.equal(g.siege.attack(champion,human,'human',.1),true);assert.ok(Math.abs((before-human.hp)/melee-1.8)<1e-10);
 const structure={id:'generator-test',type:'generator',siteId:'no-site',x:20,y:0,r:12,hp:10000,maxHp:10000,solid:true};
 g.world.objects.set(structure.id,structure);g.world._indexObject(structure);ordinary.attackCD=champion.attackCD=0;
 before=structure.hp;g.siege.attack(ordinary,structure,'object',.1);const breach=before-structure.hp;
 before=structure.hp;g.siege.attack(champion,structure,'object',.1);assert.ok(Math.abs((before-structure.hp)/breach-1.8)<1e-10);
 const slinger=ape(g,'capuchin',0,40),rangedChampion=ape(g,'capuchin',0,40);promote(g,rangedChampion,{archetype:'skirmisher'});human.x=120;human.y=40;grids(g);
 assert.equal(g.siege.throwAt(slinger,human,'human'),true);assert.equal(g.siege.throwAt(rangedChampion,human,'human'),true);
 g.time=1;g.siege.releaseThrow(slinger);g.siege.releaseThrow(rangedChampion);
 assert.equal(g.siege.stones.length,2);assert.ok(Math.abs(g.siege.stones[1].damage/g.siege.stones[0].damage-1.8)<1e-10);
});

test('nearby followers get one bounded damage and movement boost that expires outside the banners',()=>{
 const {g}=arena(),friend=ape(g,'chimpanzee',40,0),distant=ape(g,'chimpanzee',400,0),rival=ape(g,'chimpanzee',60,0),unrecruited=ape(g,'chimpanzee',70,0),a=ape(g,'orangutan'),b=ape(g,'orangutan',0,30);
 rival.hordeOwner='rival';unrecruited.state='free';unrecruited.hordeOwner=null;
 promote(g,a,{archetype:'masterBuilder'});promote(g,b,{archetype:'masterBuilder'});
 const base=g.apeDamage(friend,20),farBase=g.apeDamage(distant,20);grids(g);g.time=2;g.champions.tick();
 assert.ok(Math.abs(g.apeDamage(friend,20)/base-1.15)<1e-10);assert.equal(g.apeDamage(distant,20),farBase);
 assert.equal(rival._prisonChampionAuraUntil,undefined);assert.equal(unrecruited._prisonChampionAuraUntil,undefined);
 g.move(friend,1000,0,100,.1);assert.ok(Math.abs(friend.x-51.2)<1e-8,'two banners still grant only 12% speed');
 a.hp=b.hp=0;g.time=3.21;g.champions.tick();assert.equal(g.apeDamage(friend,20),base);
 const x=friend.x;g.move(friend,1000,0,100,.1);assert.ok(Math.abs(friend.x-x-10)<1e-8);
});

test('banner refreshes are spatial, once per second, capped and require living recovered champions',()=>{
 const {g,c}=arena(),champion=ape(g,'orangutan');promote(g,champion,{archetype:'masterBuilder'});
 for(let i=0;i<200;i++)ape(g,'chimpanzee',i%20*4,Math.floor(i/20)*4);
 grids(g);const nearest=g.apeGrid.nearest.bind(g.apeGrid);let queries=0;g.apeGrid.nearest=(...args)=>{queries++;return nearest(...args)};
 g.time=2;g.champions.tick();assert.equal(queries,1);assert.equal(g.apes.filter(a=>a._prisonChampionAuraUntil>g.time).length,c.ATSPrisonChampionStats.auraLimit);
 for(let i=0;i<20;i++){g.time+=.02;g.champions.tick()}assert.equal(queries,1);
 champion.recoveryRemaining=20;g.time=4;g.champions.tick();assert.equal(queries,1);
 delete champion.recoveryRemaining;champion.hp=0;g.time=6;g.champions.tick();assert.equal(queries,1);
});

test('training, armor, scouting and repeated loads retain exactly one champion health multiplier',()=>{
 const {g,c}=arena(),champion=ape(g,'gorilla');promote(g,champion,{archetype:'tankBuster'});
 const species=c.ATSSpeciesHealth.gorilla;g.applyTraining(champion,2);assert.equal(champion.maxHp,(species+24)*5.5);
 g.equipment.fitItem(champion,'heavyArmor');const trained=(species+24)*5.5,armored=trained+Math.round(trained*.6);
 assert.equal(champion.maxHp,armored);champion.hp=champion.maxHp-73;
 for(let i=0;i<3;i++){g.siege.balance(champion,true);assert.equal(champion.maxHp,armored);assert.equal(champion.maxHp-champion.hp,73)}
 g.king.x=0;g.command('settle');g.commandCD=0;g.command('patrol');assert.equal(champion.state,'scout');
 const scout=(species+24+25)*5.5,maximum=scout+Math.round(scout*.6);assert.equal(champion.maxHp,maximum);assert.equal(champion.maxHp-champion.hp,73);
 let loaded=g;for(let i=0;i<3;i++)loaded=c.ATSGame.fromJSON(JSON.parse(JSON.stringify(loaded.serialize())));
 const copy=loaded.apesById.get(champion.id);assert.equal(copy.champion.prisonChampion,true);assert.equal(copy.maxHp,maximum);assert.equal(copy.maxHp-copy.hp,73);
 assert.equal(copy._prisonChampionAuraUntil,undefined);assert.equal(copy.champion.healthBonus,(species+24+25)*4.5);
});
