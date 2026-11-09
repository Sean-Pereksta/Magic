import test from 'node:test';
import assert from 'node:assert/strict';
import {Game} from '../engine.js';
import {FORCE_FIELD_RADIUS} from '../warfare.js';

function arena(){
 const g=new Game();g._think=Infinity;g.entities=g.entities.filter(e=>e.type==='desk');
 g.map.obstacles=[];g.map.plateaus=[];g.map.ramps=[];g.map.cliffs=[];g._collisionBuildings=null;
 g.canSee=()=>true;g.isVisible=()=>true;return g;
}
function advance(g,seconds){for(let t=0;t<seconds-1e-9;){const dt=Math.min(.04,seconds-t);g.update(dt);t+=dt;}}
const spawn=(g,type,team,x,y)=>g._spawn(type,team,x,y,{cooldown:100});

test('smaller domes push both armies and workers outside with separate clear footprints, leaving aircraft and buildings still',()=>{
 const g=arena(),mage=spawn(g,'mage',0,1500,1700),center={x:1800,y:1700};
 const troops=[spawn(g,'rock',0,1770,1700),spawn(g,'paper',1,1790,1720),spawn(g,'twig',1,1800,1670),spawn(g,'rollingrock',0,1770,1740)];
 const plane=spawn(g,'airplane',1,1800,1700),desk=spawn(g,'cabinet',0,1865,1700);
 // The structure remains in place and displacement routes around its footprint.
 const result=g.useAbility([mage.id],'forcefield',center);assert(result.ok);assert.equal(FORCE_FIELD_RADIUS,80);assert.equal(g.forceFields[0].r,80);
 assert.equal(plane.x,1800);assert.equal(plane.y,1700);assert.equal(desk.x,1865);assert.equal(desk.y,1700);
 for(const e of troops){assert(Math.hypot(e.x-center.x,e.y-center.y)>=80+e.r+4);assert(!g._blocked(e.x,e.y,e.r));}
 for(let i=0;i<troops.length;i++)for(let j=0;j<i;j++)assert(Math.hypot(troops[i].x-troops[j].x,troops[i].y-troops[j].y)>=troops[i].r+troops[j].r);
 assert(g.effects.some(e=>e.type==='forcefieldPush'));
});

test('a dome cannot push a troop through enclosing terrain and rejects atomically without a charge',()=>{
 const g=arena(),mage=spawn(g,'mage',0,1500,1700),troop=spawn(g,'rock',1,1800,1700);
 g.map.obstacles=[{x:1750,y:1650,w:100,h:10},{x:1750,y:1740,w:100,h:10},{x:1750,y:1660,w:10,h:80},{x:1840,y:1660,w:10,h:80}];
 const result=g.useAbility([mage.id],'forcefield',{x:1800,y:1700});assert(!result.ok);assert.equal(mage.mageCharges,2);assert.equal(g.forceFields.length,0);assert.equal(troop.x,1800);assert.equal(troop.y,1700);
});

test('a distant dome command moves only the nearest selected ready mage and spends its charge on arrival',()=>{
 const g=arena(),near=spawn(g,'mage',0,1000,1700),far=spawn(g,'mage',0,800,1700);
 const r=g.useAbility([far.id,near.id],'forcefield',{x:2000,y:1700});assert(r.ok&&r.queued);assert.equal(r.casterId,near.id);assert(near.pendingAbility);assert(!far.pendingAbility);assert.equal(near.mageCharges,2);assert.equal(near.abilityCooldown,0);
 advance(g,2);assert(near.x>1000);assert.equal(g.forceFields.length,0);assert.equal(near.mageCharges,2);
 advance(g,4);assert.equal(g.forceFields.length,1);assert.equal(near.mageCharges,1);assert(!near.pendingAbility);assert.equal(far.x,800);
});

test('distant ground casts scout a fogged coordinate and wait for visibility before casting',()=>{
 const g=arena(),mage=spawn(g,'mage',0,1000,1700),target={x:2000,y:1700};
 g.canSee=(team,x,y)=>Math.hypot(x-mage.x,y-mage.y)<200;
 assert(g.useAbility([mage.id],'forcefield',target).queued);advance(g,5.5);
 assert(mage.x>1580&&mage.x<1800,'the mage continues scouting after entering spell range');assert.equal(g.forceFields.length,0);assert.equal(mage.mageCharges,2);
 advance(g,2);assert.equal(g.forceFields.length,1);assert.equal(mage.mageCharges,1);
});

test('Division and Snipe approach their cast ranges and start their original casts at arrival',()=>{
 const g=arena(),divider=spawn(g,'divider',0,1000,1700);
 assert(g.useAbility([divider.id],'division',{x:2000,y:1700}).queued);assert.equal(divider.abilityCooldown,0);advance(g,4.5);
 assert.equal(g.divisions.length,1);assert(divider.abilityCooldown>0);assert(divider.divisionCast>0);
 const h=arena(),sniper=spawn(h,'sniper',0,1000,1700),paper=spawn(h,'paper',1,1900,1700);paper.order={type:'move',x:paper.x,y:paper.y};
 assert(h.useAbility([sniper.id],'snipe',{targetId:paper.id}).queued);assert.equal(sniper.abilityCooldown,0);advance(h,2);assert(sniper.aim);assert.equal(paper.hp,paper.maxHp);advance(h,1.5);assert(!h.getEntity(paper.id));
});

test('new orders and lost Snipe targets cancel pending casts without a charge or cooldown',()=>{
 for(const order of [{type:'stop'},{type:'move',x:900,y:1800}]){
  const g=arena(),mage=spawn(g,'mage',0,1000,1700);g.useAbility([mage.id],'forcefield',{x:2000,y:1700});g.command([mage.id],order);assert(!mage.pendingAbility);advance(g,7);assert.equal(g.forceFields.length,0);assert.equal(mage.mageCharges,2);
 }
 const g=arena(),sniper=spawn(g,'sniper',0,1000,1700),paper=spawn(g,'paper',1,1900,1700);
 assert(g.useAbility([sniper.id],'snipe',{targetId:paper.id}).queued);g.isVisible=()=>false;advance(g,.1);assert(!sniper.pendingAbility);assert.equal(sniper.abilityCooldown,0);assert(!sniper.aim);
});

test('pending casts revalidate charges, target life and terrain when they actually arrive',()=>{
 const g=arena(),mage=spawn(g,'mage',0,1000,1700);assert(g.useAbility([mage.id],'forcefield',{x:2000,y:1700}).queued);
 mage.mageCharges=0;advance(g,.1);assert(!mage.pendingAbility);assert.equal(g.forceFields.length,0);assert.equal(mage.abilityCooldown,0);
 const h=arena(),sniper=spawn(h,'sniper',0,1000,1700),paper=spawn(h,'paper',1,1900,1700);
 assert(h.useAbility([sniper.id],'snipe',{targetId:paper.id}).queued);paper.hp=0;advance(h,.1);assert(!sniper.pendingAbility);assert.equal(sniper.abilityCooldown,0);
 const k=arena(),caster=spawn(k,'mage',0,1000,1700),troop=spawn(k,'rock',1,2000,1700);troop.order={type:'move',x:troop.x,y:troop.y};
 assert(k.useAbility([caster.id],'forcefield',{x:2000,y:1700}).queued);
 k.map.obstacles=[{x:1950,y:1650,w:100,h:10},{x:1950,y:1740,w:100,h:10},{x:1950,y:1660,w:10,h:80},{x:2040,y:1660,w:10,h:80}];
 advance(k,6);assert(!caster.pendingAbility);assert.equal(k.forceFields.length,0);assert.equal(caster.mageCharges,2);assert.equal(troop.x,2000);assert.equal(troop.y,1700);
});

test('a fogged point with no route cancels without spending a charge',()=>{
 const g=arena(),mage=spawn(g,'mage',0,1000,1700);g.canSee=()=>false;
 g.map.obstacles=[{x:950,y:1650,w:100,h:10},{x:950,y:1740,w:100,h:10},{x:950,y:1660,w:10,h:80},{x:1040,y:1660,w:10,h:80}];
 assert(g.useAbility([mage.id],'forcefield',{x:2000,y:1700}).queued);advance(g,9);
 assert(!mage.pendingAbility);assert.equal(mage.mageCharges,2);assert.equal(g.forceFields.length,0);
});

test('transport unloading cannot place passengers in a dome interior',()=>{
 const g=arena(),mage=spawn(g,'mage',0,1700,1700),cd=spawn(g,'cd',0,1800,1900),troop=spawn(g,'paper',0,1790,1900);
 assert(g.useAbility([mage.id],'forcefield',{x:2000,y:1700}).ok);assert(g.loadTransport([troop.id],[cd.id]).ok);advance(g,.1);assert.equal(troop.transportedBy,cd.id);
 g.command([cd.id],{type:'move',x:2000,y:1700});advance(g,2);assert(g._unloadTransport(cd));assert(!troop.transportedBy);assert(Math.hypot(troop.x-2000,troop.y-1700)>=80+troop.r+2);assert(!g._blocked(troop.x,troop.y,troop.r));
});
