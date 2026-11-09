import test from 'node:test';
import assert from 'node:assert/strict';
import {Game,TYPES,familyOf,canTransport} from '../engine.js';

const advance=(g,seconds)=>{for(let t=0;t<seconds-1e-9;){const dt=Math.min(.04,seconds-t);g.update(dt);t+=dt;}};
function arena(){const g=new Game();g._think=Infinity;g.entities=g.entities.filter(e=>e.type==='desk');g.map.obstacles=[];g.map.plateaus=[];g.map.ramps=[];g._collisionBuildings=null;return g;}
const spawn=(g,type,team,x=1500,y=1400)=>g._spawn(type,team,x,y,{cooldown:100});
const stop=e=>e.order={type:'move',x:e.x,y:e.y};

test('specialist construction requires operational prerequisites, pays exact costs and uses weighted queued supply',()=>{
  const g=arena(),w=spawn(g,'twig',0,900,1000);g.teams[0].trash=3000;
  assert(!g.build([w.id],'workshop',1000,1000).ok);
  const printer=spawn(g,'printer',0,850,1100);printer.constructing=1;
  assert(!g.build([w.id],'workshop',1000,1000).ok);printer.constructing=0;
  const built=g.build([w.id],'workshop',1000,1000);assert(built.ok);assert.equal(g.teams[0].trash,2725);
  const workshop=g.getEntity(built.entityId);assert.equal(workshop.constructing,16);
  assert(!g.build([w.id],'laboratory',1200,1000).ok);assert(!g.train(workshop.id,'sniper').ok);
  advance(g,18);assert.equal(workshop.constructing,0);
  assert(g.build([w.id],'laboratory',1200,1000).ok);assert.equal(g.teams[0].trash,2325);
  assert(g.train(workshop.id,'sniper').ok);assert(g.train(workshop.id,'airplane').ok);assert(g.train(workshop.id,'mage').ok);
  assert.equal(g.getSupply(0).reserved,6);assert.equal(g.getSupply(0).used,7);
  assert.equal(g.teams[0].trash,1875);assert(!g.train(printer.id,'sniper').ok);
  const lab=g.entities.find(e=>e.type==='laboratory');lab.constructing=0;
  assert(!g.train(lab.id,'divider').ok,'six supply cannot fit into five remaining slots');
  spawn(g,'cabinet',0,700,1200);assert(g.train(lab.id,'divider').ok);assert.equal(g.getSupply(0).reserved,12);
});

test('every tier inherits family damage immunity and new transport sizes preserve CD capacity',()=>{
  const g=arena();for(const a of ['rock','mage','rollingrock','paper','airplane','divider','scissors','sniper','shears'])for(const b of ['rock','mage','rollingrock','paper','airplane','divider','scissors','sniper','shears']){
    const source=spawn(g,a,0,1500,1400),target=spawn(g,b,1,1540,1400),hp=target.hp;g._damage(target,5,source);
    const forbidden={rock:'paper',paper:'scissors',scissors:'rock'}[familyOf(a)]===familyOf(b);
    assert.equal(target.hp,hp-(forbidden?0:5),`${a} into ${b}`);source.hp=0;target.hp=0;
  }
  const cd=spawn(g,'cd',0,1500,1400),sniper=spawn(g,'sniper',0,1510,1400),mage=spawn(g,'mage',0,1510,1410),heavy=spawn(g,'rollingrock',0,1510,1420),plane=spawn(g,'airplane',0,1510,1430);
  assert(canTransport('mage'));assert(!canTransport('divider'));
  assert.equal(g.loadTransport([sniper.id,mage.id,heavy.id,plane.id],[cd.id]).assigned,2);assert.equal(cd.cargo.length,2);assert(!heavy.transportedBy);assert(!plane.transportedBy);
});

test('each snipe click chooses one nearest ready sniper, aims visibly, and interrupts on fog, cover or Crumple',()=>{
  const g=arena(),a=spawn(g,'sniper',0,1500,1400),b=spawn(g,'sniper',0,1460,1400),paper=spawn(g,'divider',1,1750,1400);stop(paper);
  assert(g.useAbility([a.id,b.id],'snipe',{targetId:paper.id}).ok);assert(a.aim);assert(!b.aim);advance(g,1.4);assert.equal(paper.hp,paper.maxHp);advance(g,.15);assert(!g.getEntity(paper.id));
  const target=spawn(g,'paper',1,1750,1400);stop(target);assert(g.useAbility([b.id],'snipe',{targetId:target.id}).ok);target.paperInvulnerable=3;advance(g,.1);assert(!b.aim);assert.equal(target.hp,target.maxHp);
  b.abilityCooldown=0;target.paperInvulnerable=0;assert(g.useAbility([b.id],'snipe',{targetId:target.id}).ok);g.map.obstacles.push({x:1600,y:1300,w:30,h:200});advance(g,.1);assert(!b.aim);assert.equal(target.hp,target.maxHp);
  g.map.obstacles=[];b.abilityCooldown=0;assert(g.useAbility([b.id],'snipe',{targetId:target.id}).ok);target.x=4000;advance(g,.1);assert(!b.aim);
});

test('airplanes intercept moving CDs using real darts and never automatically crash or attack Scissors aircraft',()=>{
  const g=arena(),plane=spawn(g,'airplane',0,1500,1400),cd=spawn(g,'cd',1,1750,1400);plane.cooldown=0;
  assert(g.useAbility([plane.id],'intercept',{targetId:cd.id}).ok);advance(g,.04);assert(g.projectiles.some(p=>p.type==='dart'));advance(g,.6);assert(cd.hp<cd.maxHp);assert(!plane.crash);assert(plane.hp>0);
  const invalid=spawn(g,'shears',1,1700,1400);TYPES.shears.flying=true;try{assert(!g.canTarget(plane,invalid),'Scissors aircraft remain immune to Paper aircraft');}finally{delete TYPES.shears.flying;}
  assert(g.useAbility([plane.id],'patrol',{points:[{x:1400,y:1200},{x:1800,y:1200},{x:1800,y:1600}]}).ok);assert.equal(plane.order.points.length,3);
});

test('thirty crash orders assign distinct impacts, consume surviving aircraft, spare friends and Scissors, and permit pre-dive cancellation',()=>{
  const g=arena(),planes=Array.from({length:30},(_,i)=>spawn(g,'airplane',0,1100+(i%6)*45,1100+Math.floor(i/6)*45));
  const impactsSeen=[],effect=g._effect.bind(g);g._effect=(type,x,y,extra)=>{if(type==='crash')impactsSeen.push({x,y});return effect(type,x,y,extra);};
  assert(g.useAbility(planes.map(e=>e.id),'crash',{x:1700,y:1500}).ok);
  const impacts=planes.map(e=>({...e.crash}));assert.equal(new Set(impacts.map(p=>`${p.x},${p.y}`)).size,30);
  assert(Math.max(...impacts.map(p=>p.x))-Math.min(...impacts.map(p=>p.x))>250);
  g.command([planes[0].id],{type:'stop'});assert(!planes[0].crash);planes[1].hp=0;
  const rock=spawn(g,'rollingrock',1,impacts[2].x,impacts[2].y),friend=spawn(g,'paper',0,impacts[2].x+10,impacts[2].y),scissors=spawn(g,'shears',1,impacts[2].x,impacts[2].y+15);[rock,friend,scissors].forEach(stop);
  advance(g,5);assert(rock.hp<rock.maxHp);assert.equal(friend.hp,friend.maxHp);assert.equal(scissors.hp,scissors.maxHp);
  assert.equal(impactsSeen.length,28,'an intercepted plane and a canceled plane produce no crash blast');
  assert(impactsSeen.every(p=>impacts.some(assigned=>Math.hypot(p.x-assigned.x,p.y-assigned.y)<.01)),'each aircraft impacts its own assigned destination');
  assert.equal(g.effects.filter(e=>e.type==='crash').length,0,'impact effects expire without growing unbounded');
  assert.equal(g.entities.filter(e=>e.type==='airplane').length,1,'only the canceled plane remains');
});

test('a final dive commits its aircraft and mouse anti-air can destroy it without a crash explosion',()=>{
  const g=arena(),plane=spawn(g,'airplane',0,1500,1400);let crashes=0;const effect=g._effect.bind(g);g._effect=(type,x,y,extra)=>{if(type==='crash')crashes++;return effect(type,x,y,extra);};
  assert(g.useAbility([plane.id],'crash',{x:1600,y:1400}).ok);advance(g,.04);assert(plane.crash.diving);
  g.command([plane.id],{type:'stop'});assert(plane.crash.diving,'a committed dive cannot be canceled');
  const mice=Array.from({length:7},(_,i)=>spawn(g,'mouse',1,1580+i*4,1425));for(const mouse of mice){mouse.cooldown=0;assert(g.canTarget(mouse,plane));}advance(g,.04);assert(g.projectiles.some(p=>p.type==='pointer'));advance(g,1);assert.equal(crashes,0);assert(!g.getEntity(plane.id));
});

test('one dome per click uses independent two-cast recharge cycles and solid overlapping ground boundaries',()=>{
  const g=arena(),a=spawn(g,'mage',0,1450,1400),b=spawn(g,'mage',0,1300,1400);
  assert(g.useAbility([a.id,b.id],'forcefield',{x:1650,y:1400}).ok);assert.equal(g.forceFields.length,1);assert.equal(a.mageCharges,1);assert.equal(b.mageCharges,2);
  assert(g.useAbility([a.id,b.id],'forcefield',{x:1700,y:1400}).ok);assert.equal(g.forceFields.length,2);assert.equal(a.mageCharges,0);
  assert.equal(g.forceFields[0].r,80);assert(g._blocked(1570,1400,18));assert(!g._lineFree(1480,1400,1800,1400,18));
  const trapped=spawn(g,'rock',0,1650,1400);g.command([trapped.id],{type:'move',x:1900,y:1400});advance(g,2);assert(g.forceFields.every(f=>Math.hypot(trapped.x-f.x,trapped.y-f.y)>=f.r+trapped.r));
  advance(g,3.1);assert.equal(a.mageCharges,2);assert.equal(b.mageCharges,2);
  advance(g,5.1);assert.equal(g.forceFields.length,0);advance(g,4);assert(trapped.x>1750,'pushed troops retain their movement order');
});

test('Triple Division continuously sorts a large mixed army and friendlies into free, separated sectors then releases orders',()=>{
  const g=arena(),caster=spawn(g,'divider',0,1220,1600),center={x:1700,y:1600},troops=[];
  for(const [type,count] of [['rock',40],['paper',25],['scissors',30],['mage',4],['sniper',3],['rollingrock',2]])for(let i=0;i<count;i++){
    const n=troops.length,angle=n*2.399963,rad=30+Math.sqrt(n/105)*270,e=spawn(g,type,n%7===0?0:1,center.x+Math.cos(angle)*rad,center.y+Math.sin(angle)*rad);e.order={type:'move',x:1700,y:1600};troops.push(e);
  }
  const plane=spawn(g,'airplane',1,1750,1600),cd=spawn(g,'cd',1,1700,1550);troops.push(plane);stop(cd);
  assert(g.useAbility([caster.id],'division',center).ok);assert.equal(g.divisions[0].casting,1.5);advance(g,1.4);assert(!troops.some(e=>e.divisionPull));advance(g,4);
  const field=g.divisions[0];assert(field);assert(troops.every(e=>e.divisionId===field.id),'unassigned: '+troops.filter(e=>e.divisionId!==field.id).map(e=>e.type+':'+e.id+':'+Math.round(Math.hypot(e.x-center.x,e.y-center.y))).join(','));assert(!cd.divisionId);
  for(const e of troops){assert(!e.divisionPull,`pull completes for ${e.type}`);const angle=(Math.atan2(e.y-center.y,e.x-center.x)+Math.PI/2+Math.PI*2)%(Math.PI*2),sector=Math.floor(angle/(Math.PI*2/3));assert.equal(sector,['rock','paper','scissors'].indexOf(familyOf(e)),`${e.type} section`);assert(Math.hypot(e.x-center.x,e.y-center.y)<350);if(e.type!=='airplane')assert(!g._blocked(e.x,e.y,e.r),`${e.type} on free ground`);}
  for(let i=0;i<troops.length;i++)for(let j=0;j<i;j++)if(troops[i].type!=='airplane'&&troops[j].type!=='airplane')assert(Math.hypot(troops[i].x-troops[j].x,troops[i].y-troops[j].y)>=troops[i].r+troops[j].r-1,'no stacked troops');
  const rock=troops.find(e=>familyOf(e)==='rock'),paper=troops.find(e=>familyOf(e)==='paper');assert(!g.canTarget(rock,paper));
  advance(g,6);assert.equal(g.divisions.length,0);assert(troops.every(e=>!e.divisionPull&&!e.divisionId));
});

test('Crushing Roll accelerates, damages multiple eligible enemies, protects Paper and stops at solid scenery',()=>{
  const g=arena(),rock=spawn(g,'rollingrock',0,1200,1400),targets=[spawn(g,'scissors',1,1400,1400),spawn(g,'rollingrock',1,1500,1400)],paper=spawn(g,'paper',1,1700,1450);[...targets,paper].forEach(stop);
  assert(g.useAbility([rock.id],'roll',{x:1950,y:1400}).ok);advance(g,.2);assert(rock.x>1200&&rock.x<1220);advance(g,2);assert(targets.every(t=>t.hp<t.maxHp));assert.equal(paper.hp,paper.maxHp);
  const other=spawn(g,'rollingrock',0,1200,1800);g.map.obstacles.push({x:1400,y:1750,w:60,h:100});assert(g.useAbility([other.id],'roll',{x:1900,y:1800}).ok);advance(g,3);assert(other.x<1400-other.r);assert(!other.roll);
});

test('division sorting routes around solid terrain and packs all reachable heavy and light troops clear of walls',()=>{
  const g=arena();g.map.obstacles=[{x:960,y:900,w:80,h:160,type:'book'}];
  const caster=spawn(g,'divider',0,700,1000),types=['rock','paper','scissors','mage','sniper','rollingrock','shears','airplane','divider'],troops=[];
  for(let y=760;y<=1240;y+=43)for(let x=760;x<=1240;x+=43){
    if(Math.hypot(x-1000,y-1000)>295||troops.length>=90)continue;
    const type=types[troops.length%types.length];if(g._blocked(x,y,TYPES[type].r))continue;
    const e=spawn(g,type,troops.length%2,x,y);stop(e);troops.push(e);
  }
  assert(g.useAbility([caster.id],'division',{x:1000,y:1000}).ok);advance(g,1.48);
  const eligible=troops.filter(e=>Math.hypot(e.x-1000,e.y-1000)<=350);advance(g,3);
  for(const e of eligible){assert(e.divisionId,`${e.type} is assigned`);assert(!e.divisionPull,`${e.type} completes a real route`);if(e.type!=='airplane')assert(!g._blocked(e.x,e.y,e.r),`${e.type} clears terrain and magical walls`);const angle=(Math.atan2(e.y-1000,e.x-1000)+Math.PI/2+Math.PI*2)%(Math.PI*2);assert.equal(Math.floor(angle/(Math.PI*2/3)),['rock','paper','scissors'].indexOf(familyOf(e)));}
});

test('Shadow Path follows bends, hits each target once, creates group lanes and stops at barriers',()=>{
  const g=arena(),a=spawn(g,'shears',0,1200,1300),b=spawn(g,'shears',0,1200,1350),paper=spawn(g,'divider',1,1450,1550),rock=spawn(g,'rollingrock',1,1460,1500);[paper,rock].forEach(stop);
  const route=[{x:1200,y:1550},{x:1600,y:1550},{x:1600,y:1800}];assert(g.useAbility([a.id,b.id],'shadowpath',{points:route}).ok);assert.notDeepEqual(a.shadow.points,b.shadow.points);
  advance(g,.2);assert(a.y>1300&&Math.abs(a.x-1200)<30,'starts down the drawn bend, not toward the final point');advance(g,1.4);assert(paper.hp<paper.maxHp);assert.equal(rock.hp,rock.maxHp);assert(!a.shadow);assert(!b.shadow);
  const c=spawn(g,'shears',0,1200,2200);g.forceFields.push({id:999,x:1500,y:2200,r:110,remaining:10,total:10,team:0});assert(g.useAbility([c.id],'shadowpath',{points:[{x:1800,y:2200}]}).ok);advance(g,2);assert(c.x<1390);assert(!c.shadow);
});

for(const difficulty of ['easy','normal','hard'])test(`${difficulty} bots construct and train every specialist tier using their own finite trash and supply`,()=>{
  const g=new Game({difficulty});g._think=Infinity;const team=1;g.teams[team].trash=20000;
  // Plenty of paid-equivalent storage isolates tier decisions from battle deaths.
  for(let i=0;i<10;i++)spawn(g,'cabinet',team,4000-i*85,2200);
  g.time=70;g._bot(team);const workshop=g.entities.find(e=>e.team===team&&e.type==='workshop');assert(workshop);assert(g.teams[team].trash<=20000-275);workshop.constructing=0;
  g.time=160;g._bot(team);const lab=g.entities.find(e=>e.team===team&&e.type==='laboratory');assert(lab);lab.constructing=0;
  const trained=new Set();let previous=g.teams[team].trash;
  for(let i=0;i<12;i++){g._bot(team);for(const building of [workshop,lab]){for(const q of building.queue){if(q.kind==='train')trained.add(q.type);}building.queue=[];}}
  for(const type of ['sniper','airplane','mage','divider','rollingrock','shears'])assert(trained.has(type),`${difficulty} trains ${type}`);
  assert(g.teams[team].trash<previous);assert(g.teams[team].trash>=0);assert(g.getSupply(team).used<=g.getSupply(team).cap);
});
