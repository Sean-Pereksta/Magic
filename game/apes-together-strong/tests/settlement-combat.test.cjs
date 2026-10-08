'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {loadEngine}=require('./performance-harness.cjs');
function arena(){
 const c=loadEngine();if(!c.ATSForces.prototype.structureTarget)vm.runInContext(fs.readFileSync(path.join(__dirname,'..','settlement-combat.js'),'utf8'),c);
 const g=new c.ATSGame('TOWER-RETALIATION'),w=g.world;w.objects.clear();w._spatial.clear();w.sites.clear();w.ensure=()=>{};w.getSites=()=>[];w.terrain=()=>({biome:'farmland',road:true,water:false});g.king.x=-1000;g.king.y=0;g.apeGrid.rebuild([g.king]);
 const tower={id:'village-defense',kind:'spearTower',x:220,y:0,stage:4,hp:500,maxHp:500,lastShot:0},s={id:'village',x:300,y:0,radius:180,huts:[],structures:[],facilities:[tower]};g.settlements.push(s);g.syncIndexes();w.syncSettlementBuildings(s);
 const h=g.makeHuman(0,0,null);g.forces.assign(h,null,'rifleman');Object.assign(h,{state:'search',raidTarget:s.id,dir:0,shootTimer:0,accuracyMultiplier:0});g.humanGrid.rebuild([h]);g.performance.beginStep(g);
 return{c,g,w,h,s,tower,proxy:w.objects.get(tower.id+':collision')};
}
function cover(w,x=110){const o={id:'opaque-wall',x,y:0,w:20,h:300,r:10,hp:300,solid:true,collision:'rect',type:'wall',height:100};w.objects.set(o.id,o);w._indexObject(o);w.navRevision++;return o}
function flight(g,seconds=.6){for(let i=0;i<seconds/.02;i++){g.time+=.02;g.updateBullets(.02)}}
test('a human aims at a tower and a traveling rifle bullet damages its real building on impact',()=>{
 const {g,h,tower,proxy}=arena();assert.equal(g.forces.update(h,.02),true);assert.equal(h.structureTarget,tower.id);assert.ok(g.bullets.length);assert.ok(g.bullets.every(b=>b.structureShot&&b.structureTarget===proxy.id));assert.equal(tower.hp,500,'no damage is applied before flight');
 flight(g);assert.ok(tower.hp<500);assert.equal(proxy.hp,tower.hp);assert.equal(g.bullets.length,0);
});
test('opaque cover blocks target acquisition and also stops a shot fired before cover appears',()=>{
 let {g,w,h,tower}=arena();cover(w);assert.equal(g.forces.updateStructureCombat(h,.02),false);assert.equal(g.bullets.length,0);assert.equal(tower.hp,500);
 ({g,w,h,tower}=arena());g.forces.updateStructureCombat(h,.02);assert.ok(g.bullets.length);cover(w);flight(g);assert.equal(tower.hp,500);assert.equal(g.bullets.length,0);
});
test('a live ape contact takes priority over structures and an ape crossing the shot absorbs the projectile',()=>{
 let {g,h,tower}=arena();const a=g.makeApe(140,0,'hold');g.syncIndexes();g.apeGrid.rebuild([g.king,a]);h.targetId=a.id;h.state='combat';h.lastSeenAt=g.time;assert.equal(g.forces.updateStructureCombat(h,.02),false);assert.equal(h.structureTarget,undefined);
 ({g,h,tower}=arena());g.forces.updateStructureCombat(h,.02);const blocker=g.makeApe(110,0,'hold'),hp=blocker.hp;g.syncIndexes();g.apeGrid.rebuild([g.king,blocker]);flight(g);assert.ok(blocker.hp<hp);assert.equal(tower.hp,500);
});
test('retreats and engineer jobs remain authoritative, while unseen structures behind patrols are ignored',()=>{
 const {g,h}=arena();const squad=g.forces.createSquad([h],{x:-200,y:0},{order:'Fallback'});squad.order='Fallback';assert.equal(g.forces.updateStructureCombat(h,.02),false);delete h.squadId;h.engineerJob={remaining:4};assert.equal(g.forces.updateStructureCombat(h,.02),false);delete h.engineerJob;h.state='patrol';h.raidTarget=null;h.dir=Math.PI;assert.equal(g.forces.updateStructureCombat(h,.02),false);
});
test('traveling structure attackers cannot also apply a direct settlement siege hit',()=>{
 const {g,h,s,tower}=arena();g.forces.updateStructureCombat(h,.02);g.colonies.siege(s,[h]);assert.equal(tower.hp,500);flight(g);const hp=tower.hp;g.colonies.siege(s,[h]);assert.equal(tower.hp,hp);
});
test('retaliation acquisition and sight work stay capped across a dense human wave',()=>{
 const {g,h}=arena(),humans=[h];for(let i=0;i<299;i++){const p=g.makeHuman(i%5,i%20-10,null);g.forces.assign(p,null,'rifleman');Object.assign(p,{state:'search',raidTarget:'village',dir:0,shootTimer:0});humans.push(p)}g.performance.beginStep(g);
 for(const p of humans)g.forces.updateStructureCombat(p,.02);assert.ok(g._structureBudget.acquisitions<=16);assert.ok(g._structureBudget.los<=32);assert.ok(g.performance.counters.aiThinks<=16);assert.ok(g.bullets.length<=16);
});
test('invading riflemen physically shoot down palisades and then target the next live structure',()=>{
 const {g,w,h,s,proxy}=arena(),b=w.createFortification({id:'invasion-palisade',team:'ape',settlementId:s.id,x:110,y:0,w:16,h:180,height:16,hp:55,maxHp:55});s.barriers=[b];
 for(let i=0;i<2;i++){g.performance.beginStep(g);h.shootTimer=0;assert.equal(g.forces.updateStructureCombat(h,.02),true);assert.equal(h.structureTarget,b.id);assert.ok(g.bullets.some(p=>p.structureTarget===b.id));const hp=b.hp;assert.ok(hp>0);flight(g,.35);assert.ok(b.hp<hp,'a real rifle projectile reaches the wooden section')}
 assert.equal(b.hp,0);assert.equal(b.dead,true);assert.equal(b.solid,false);assert.equal(w.objects.get(b.id),b);assert.equal(s.barriers[0],b,'the settlement keeps the actual destroyed world object for saves and repairs');
 g.performance.beginStep(g);h.shootTimer=0;assert.equal(g.forces.updateStructureCombat(h,.02),true);assert.equal(h._structureTargetId,proxy.id,'destroying the wall invalidates target cooldown immediately');
});
test('the nearest palisade beats a farther active tower or remembered ape, while a closer visible ape still wins',()=>{
 const {g,w,h,s,tower}=arena(),b=w.createFortification({id:'near-wall',team:'ape',settlementId:s.id,x:130,y:0,w:16,h:180,hp:300});tower.lastShot=g.time;
 const far=g.makeApe(185,0,'hold');g.syncIndexes();g.apeGrid.rebuild([g.king,far]);Object.assign(h,{state:'combat',targetId:far.id,lastSeenAt:g.time});
 assert.equal(g.forces.updateStructureCombat(h,.02),true);assert.equal(h.structureTarget,b.id);assert.ok(g.bullets.every(p=>p.structureTarget===b.id));
 const near=g.makeApe(60,0,'hold');g.syncIndexes();g.apeGrid.rebuild([g.king,far,near]);g.time+=.02;g.performance.beginStep(g);assert.equal(g.forces.updateStructureCombat(h,.02),false);assert.equal(h.targetId,near.id);assert.equal(h.structureTarget,undefined);
});
test('allied human barricades are never targets or collateral recipients of palisade fire',()=>{
 const {g,w,h,s}=arena(),human=w.createFortification({id:'friendly-human-wall',team:'human',x:65,y:0,w:16,h:180,hp:300}),ape=w.createFortification({id:'enemy-ape-wall',team:'ape',x:125,y:0,w:16,h:180,height:16,hp:55});
 g.performance.beginStep(g);assert.equal(g.forces.updateStructureCombat(h,.02),true);assert.equal(h.structureTarget,ape.id,'an ape barrier does not require a settlement ID to block an invasion');flight(g,.4);assert.ok(ape.hp<55);assert.equal(human.hp,300);assert.equal(human.dead,undefined);
});
test('a nearer live palisade constructed after acquisition invalidates the old tower target',()=>{
 const {g,w,h,s,proxy}=arena();assert.equal(g.forces.structureTarget(h),proxy);const wall=w.createFortification({id:'new-near-wall',team:'ape',settlementId:s.id,x:105,y:0,w:16,h:180,hp:300});assert.equal(g.forces.structureTarget(h),wall);
});
test('a fifth nearer visible ape wins after four closer apes are hidden by opaque cover',()=>{
 const {g,w,h,s}=arena();w.createFortification({id:'farther-palisade',team:'ape',settlementId:s.id,x:130,y:0,w:16,h:180,hp:300});const obstruction={id:'screening-wall',type:'wall',faction:'human',x:16,y:35,w:12,h:36,height:100,collision:'rect',solid:true,hp:300};w.objects.set(obstruction.id,obstruction);w._indexObject(obstruction);w.navRevision++;
 for(let i=0;i<4;i++)g.makeApe(30+i,50+2*i,'hold');const visible=g.makeApe(90,-20,'hold');g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);assert.equal(g.forces.updateStructureCombat(h,.02),false);assert.equal(h.targetId,visible.id);assert.equal(g.bullets.length,0);assert.ok(g._structureBudget.los<=32);
});
test('an exhausted nearer-ape sight budget defers wall fire instead of assuming an occluded sample is complete',()=>{
 const {g,w,h,s}=arena();w.createFortification({id:'budget-palisade',team:'ape',settlementId:s.id,x:130,y:0,w:16,h:180,hp:300});const obstruction={id:'crowd-screen',type:'wall',faction:'human',x:16,y:35,w:12,h:42,height:100,collision:'rect',solid:true,hp:300};w.objects.set(obstruction.id,obstruction);w._indexObject(obstruction);w.navRevision++;
 for(let i=0;i<40;i++)g.makeApe(30+i*.05,50+i*.05,'hold');const visible=g.makeApe(90,-20,'hold');g.syncIndexes();g.apeGrid.rebuild([g.king,...g.apes]);assert.equal(g.forces.updateStructureCombat(h,.02),false);assert.equal(g.bullets.length,0);assert.equal(g._structureBudget.los,32);assert.notEqual(h.structureTarget,'budget-palisade');assert.ok(g.siege.clearRay(h,visible),'a later visible candidate exists beyond the checked crowd');
});
test('the visible king takes ordinary combat priority when closer than an invasion palisade',()=>{
 const {g,w,h,s}=arena();w.createFortification({id:'king-palisade',team:'ape',settlementId:s.id,x:130,y:0,w:16,h:180,hp:300});g.king.x=60;g.king.y=-20;g.apeGrid.rebuild([g.king]);assert.equal(g.forces.updateStructureCombat(h,.02),false);assert.equal(h.targetId,'king');assert.equal(g.bullets.length,0);
});
