'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {loadEngine}=require('./performance-harness.cjs');
function setup(){
 const c=loadEngine(),g=new c.ATSGame('BLAST-FLIGHT');g.world.objects.clear();g.world._spatial.clear();g.world.sites.clear();
 g.world.ensure=()=>{};g.world.stream=()=>{};g.world.boundsReady=()=>true;g.world.terrain=()=>({biome:'farmland',road:true,water:false});g.world.lineClear=()=>true;
 g.spawnSites=()=>{};g.nextDirectorAt=g.nextConvoyAt=g.heliTimer=1000000;g.king.x=9000;return{c,g};
}
function burst(g,type){return{x:0,y:0,type,radius:100,damage:70,owner:'blast-test'}}
for(const type of ['shell','mortar','airstrike','grenade'])test(type+' explosion throws survivors, newly killed apes and existing corpses, then survivors get up',()=>{
 const {g}=setup(),alive=g.makeApe(30,0,'hold'),fragile=g.makeApe(-25,0,'hold'),old=g.makeApe(0,25,'hold');alive.hp=alive.maxHp=1000;fragile.hp=1;
 g.hurt(old,1000,{x:0,y:0});const oldBody=g.corpses[0];oldBody.age=3;
 g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);g.forces.blast(burst(g,type));
 assert.ok(g.effects.some(e=>e.type==='explosion'&&(e.blastKind===type||e.blastType===type)),'real colorful explosion effect');
 assert.ok(alive.hp<1000&&alive.hp>0);assert.equal(alive.blastReaction.stage,'flight');assert.ok(alive.blastReaction.height>0);assert.ok(alive.knockbackUntil>g.time&&alive.staggerUntil>g.time);
 assert.equal(oldBody.blastReaction.stage,'flight');assert.ok(oldBody.blastReaction.height>0);const newBody=g.corpses.find(c=>c.id===fragile.id);assert.equal(newBody.blastReaction.stage,'flight');
 const start=alive.x;let sawGettingUp=false;for(let i=0;i<35;i++){g.update(.1,{x:1});if(alive.blastReaction?.stage==='gettingUp'){sawGettingUp=true;assert.ok(alive.blastReaction.getUpAt<=g.time);assert.ok(alive.blastReaction.recoverAt>g.time)}}
 assert.ok(alive.x>start,'blast moves the ape away from the center');assert.equal(sawGettingUp,true);assert.equal(alive.blastReaction,undefined);assert.equal(alive.blastZ,0);assert.equal(oldBody.blastReaction.stage,'landed');assert.equal(newBody.blastReaction.stage,'landed');
});

test('flight and get-up interrupt commands, combat and worker movement, then release the actor',()=>{
 const {g}=setup(),ape=g.makeApe(35,0,'charge');g.king.x=0;ape.target={x:1000,y:0};ape.chargeTime=20;g.launchBlastReaction(ape,burst(g,'shell'),{radius:100});
 let moves=0;g.move=()=>{moves++};g.updateApe(ape,.1);assert.equal(moves,0);assert.equal(ape.moving,false);
 for(let i=0;i<12&&ape.blastReaction?.stage==='flight';i++){g.time+=.1;g.updateApe(ape,.1)}
 assert.equal(ape.blastReaction.stage,'gettingUp');const x=ape.x,y=ape.y;g.time+=.2;g.updateApe(ape,.2);assert.equal(ape.x,x);assert.equal(ape.y,y);assert.equal(moves,0);
 g.time=ape.blastReaction.recoverAt+.01;g.updateApe(ape,.1);assert.equal(ape.blastReaction,undefined);assert.ok(moves>0,'ordinary movement resumes after finite recovery');
 g.king.x=35;g.launchBlastReaction(g.king,burst(g,'grenade'),{radius:100,power:.7});assert.equal(g.command('charge'),false);const oldCD=g.attackCD;g.attack();assert.equal(g.attackCD,oldCD);
});

test('blast travel cannot sweep through solid cover or water and lands outside collision',()=>{
 const {g}=setup(),ape=g.makeApe(0,0,'hold'),wall={id:'blast-wall',type:'wall',x:55,y:0,w:18,h:180,r:10,collision:'rect',solid:true,hp:500};g.world.objects.set(wall.id,wall);g.world._indexObject(wall);
 g.launchBlastReaction(ape,{x:-20,y:0,type:'shell',radius:120},{radius:120,power:1.8});
 for(let i=0;i<25;i++){g.time+=.1;g.updateBlastReaction(ape,.1);assert.equal(g.navigation.blocked(ape.x,ape.y,10,'ape'),false);assert.ok(ape.x<=36,'swept body does not cross the live wall')}
 const swimmer=g.makeApe(-200,250,'hold');g.world.waterBlocked=(x,y,r)=>x>-145;g.launchBlastReaction(swimmer,{x:-220,y:250,type:'airstrike',radius:120},{radius:120,power:1.5});
 for(let i=0;i<25;i++){g.time+=.1;g.updateBlastReaction(swimmer,.1);assert.ok(swimmer.x<=-145,'launch cannot move a living ape into water')}
 assert.equal(swimmer.blastZ,0);assert.equal(swimmer.blastReaction,undefined);
});

test('saved airborne survivors and corpses retain momentum and recovery timing',()=>{
 const {c,g}=setup(),ape=g.makeApe(30,0,'hold'),dead=g.makeApe(-30,0,'hold');g.hurt(dead,1000,{x:0,y:0});g.launchBlastReaction(ape,burst(g,'airstrike'),{radius:100});g.blastBodies(burst(g,'airstrike'),100);g.time+=.2;g.updateBlastReaction(ape,.2);g.updateBlastReaction(g.corpses[0],.2);
 const snapshot=JSON.parse(JSON.stringify(g.serialize())),loaded=c.ATSGame.fromJSON(snapshot),actor=loaded.apesById.get(ape.id),body=loaded.corpses.find(x=>x.id===dead.id);
 assert.equal(actor.blastReaction.stage,'flight');assert.equal(actor.blastReaction.elapsed,ape.blastReaction.elapsed);assert.equal(actor.blastReaction.vx,ape.blastReaction.vx);assert.equal(actor.blastZ,ape.blastZ);assert.equal(body.blastReaction.stage,'flight');assert.equal(body.blastReaction.rotation,g.corpses[0].blastReaction.rotation);
 loaded.time+=.1;loaded.updateBlastReaction(actor,.1);assert.ok(actor.blastReaction.elapsed>ape.blastReaction.elapsed);assert.ok(Number.isFinite(actor.x)&&Number.isFinite(actor.y));
});

test('a fatal blast throws the King and corpse physics continues beneath the death screen',()=>{
 const {g}=setup();g.king.x=30;g.king.hp=1;g.apeGrid.rebuild([g.king]);g.forces.blast(burst(g,'shell'));assert.equal(g.ended,true);assert.equal(g.king.blastReaction.stage,'flight');assert.ok(g.king.blastZ>0);
 for(let i=0;i<20;i++)g.update(.1,{});assert.equal(g.king.blastReaction.stage,'landed');assert.equal(g.king.blastZ,0);assert.ok(g.deathTimer>=2);
});

test('dense corpse piles all react with bounded immediate collision work and keep actor records',()=>{
 const {g}=setup();for(let i=0;i<200;i++)g.corpses.push({id:'ape-dead-'+i,type:'ape',x:i%10,y:Math.floor(i/10),hp:0,life:20,age:2,dir:0});
 const clear=g.navigation.clearSegment.bind(g.navigation);let sweeps=0;g.navigation.clearSegment=(...args)=>{sweeps++;return clear(...args)};
 assert.equal(g.blastBodies(burst(g,'airstrike'),100),200);assert.equal(g.corpses.filter(c=>c.blastReaction).length,200);assert.ok(sweeps<=12,'collision integration is deferred for dense piles');assert.equal(g.corpses.length,200);
 for(const c of g.corpses){g.time+=.001;g.updateBlastReaction(c,.1);assert.ok(c.blastReaction.height>0)}
});

test('crowded battle effects retain colorful explosions within the effect limit',()=>{
 const {g}=setup();for(let i=0;i<400;i++)g.effects.push(g.effectPool.take({type:'muzzle',x:1000+i,y:0,life:1,maxLife:1}));
 g.forces.blast(burst(g,'airstrike'));assert.ok(g.effects.some(e=>e.type==='explosion'&&e.blastKind==='airstrike'));assert.ok(g.effects.length<=448);
});
