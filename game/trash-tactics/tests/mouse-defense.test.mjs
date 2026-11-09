import test from 'node:test';
import assert from 'node:assert/strict';
import {Game,TYPES} from '../engine.js';

function arena(){const g=new Game();g._think=Infinity;g.entities=g.entities.filter(e=>e.type==='desk');return g;}
function advance(g,seconds){for(let t=0;t<seconds-1e-9;){const dt=Math.min(.02,seconds-t);g.update(dt);t+=dt;}}
function enemy(g,type,x=920,y=800){const e=g._spawn(type,1,x,y);e.cooldown=100;return e;}

test('fortified mice retain their price and build time while covering farther lanes and firing faster',()=>{
  const g=arena(),mouse=g._spawn('mouse',0,700,800),cd=enemy(g,'cd',1040);
  assert.equal(mouse.hp,850);assert.equal(TYPES.mouse.cost,175);assert.equal(TYPES.mouse.buildTime,8);
  assert.equal(TYPES.mouse.range,360);assert.equal(TYPES.mouse.shotInterval,1.25);
  advance(g,.02);assert.equal(g.projectiles[0]?.targetId,cd.id,'the wider defense reaches beyond the former coverage edge');
  advance(g,1.3);assert(mouse.cooldown>0);assert(g.projectiles.some(p=>p.type==='pointer'),'the second pointer is already in flight');
  assert.equal(TYPES.mouse.impactDamage,28);assert.equal(TYPES.mouse.clickDps,6);assert.equal(TYPES.mouse.clickDuration,3);
});

test('workers build paid mouse defenses, which provide no supply and cannot fire unfinished',()=>{
  const g=arena();g.teams[0].trash=1000;
  const twig=g._spawn('twig',0,650,800),supply=g.getSupply(0).cap;
  const built=g.build([twig.id],'mouse',700,800);assert(built.ok);
  const mouse=g.getEntity(built.entityId),cd=enemy(g,'cd');
  assert.equal(g.teams[0].trash,1000-TYPES.mouse.cost);
  assert(!g.canTarget(mouse,cd));assert(!g.train(mouse.id,'cd').ok);
  advance(g,2);assert.equal(cd.hp,cd.maxHp);assert.equal(g.projectiles.length,0);
  advance(g,7);assert.equal(mouse.constructing,0);
  assert.equal(g.getSupply(0).cap,supply);assert(cd.hp<cd.maxHp);
});

test('mouse fires visible travelling pointer shots and prioritizes flying CDs over ground troops',()=>{
  const g=arena(),mouse=g._spawn('mouse',0,700,800),rock=enemy(g,'rock',850),cd=enemy(g,'cd',980);
  advance(g,.02);
  assert.equal(g.projectiles.length,1);const shot=g.projectiles[0];
  assert.equal(shot.type,'pointer');assert.equal(shot.targetId,cd.id);
  assert(shot.x>mouse.x&&shot.x<cd.x);assert.equal(cd.hp,cd.maxHp,'damage waits for pointer impact');
  const x=shot.x;advance(g,.1);assert(shot.x>x);
  advance(g,.5);assert.equal(cd.hp,cd.maxHp-TYPES.mouse.impactDamage);assert.equal(rock.hp,rock.maxHp);
  assert.equal(g.projectiles.length,0);assert.equal(cd.clickDamage.length,1);
});

test('pointer impact is followed by three seconds of damage, which ends without extra ticks',()=>{
  const g=arena(),mouse=g._spawn('mouse',0,700,800),cd=enemy(g,'cd',920);
  advance(g,.02);mouse.cooldown=100;
  advance(g,.5);assert.equal(cd.hp,cd.maxHp-TYPES.mouse.impactDamage);
  advance(g,.5);assert.equal(cd.hp,cd.maxHp-TYPES.mouse.impactDamage-TYPES.mouse.clickDps*.5);
  advance(g,2.6);assert.equal(cd.hp,cd.maxHp-TYPES.mouse.impactDamage-TYPES.mouse.clickDps*TYPES.mouse.clickDuration,'one hit deals its configured impact plus six half-second damage ticks');
  assert.equal(cd.clickDamage.length,0);
  advance(g,3);assert.equal(cd.hp,cd.maxHp-TYPES.mouse.impactDamage-TYPES.mouse.clickDps*TYPES.mouse.clickDuration,'expired click damage stops');
});

test('mouse defenses can damage each rock-paper-scissors type without a counter restriction',()=>{
  for(const type of ['rock','paper','scissors']){
    const g=arena(),mouse=g._spawn('mouse',0,700,800),troop=enemy(g,type,920);
    assert(g.canTarget(mouse,troop));advance(g,.02);mouse.cooldown=100;advance(g,.6);
    assert.equal(troop.hp,troop.maxHp-TYPES.mouse.impactDamage,`${type} receives pointer impact`);
    advance(g,3.1);assert.equal(troop.hp,troop.maxHp-TYPES.mouse.impactDamage-TYPES.mouse.clickDps*TYPES.mouse.clickDuration,`${type} receives lingering click damage`);
  }
});

test('defenses ignore friends, workers, buildings, passengers, and enemies outside range',()=>{
  const g=arena(),mouse=g._spawn('mouse',0,700,800);
  const friendly=g._spawn('cd',0,800,800),twig=enemy(g,'twig',800),building=enemy(g,'cabinet',850),far=enemy(g,'cd',1100);
  const passenger=enemy(g,'paper',1100,850),carrier=enemy(g,'cd',1100,850);
  assert(g.loadTransport([passenger.id],[carrier.id]).ok);advance(g,3);
  assert(passenger.transportedBy);
  for(const e of [friendly,twig,building,passenger])assert(!g.canTarget(mouse,e));
  assert.equal(g.projectiles.length,0);assert.equal(far.hp,far.maxHp);
  for(const e of [friendly,twig,building,passenger])assert.equal(e.hp,e.maxHp);
});

test('shots track a moving CD and invalid targets disappear without hitting passengers or crumpled paper',()=>{
  const g=arena(),mouse=g._spawn('mouse',0,700,800),cd=enemy(g,'cd',950);
  advance(g,.02);mouse.cooldown=100;g.command([cd.id],{type:'move',x:1200,y:900});
  advance(g,1.5);assert(cd.hp<cd.maxHp,'homing pointer catches a moving transport');
  const h=arena(),defense=h._spawn('mouse',0,700,800),paper=enemy(h,'paper',950),carrier=enemy(h,'cd',950,840);
  carrier.x=1200;advance(h,.02);defense.cooldown=100;
  carrier.x=paper.x;carrier.y=paper.y;assert(h.loadTransport([paper.id],[carrier.id]).ok);
  advance(h,.8);assert.equal(paper.hp,paper.maxHp);assert.equal(h.projectiles.length,0);
  const k=arena(),m=k._spawn('mouse',0,700,800),p=enemy(k,'paper',950);
  advance(k,.02);m.cooldown=100;p.paperInvulnerable=3;
  advance(k,1);assert.equal(p.hp,p.maxHp);assert.equal(k.projectiles.length,0);assert(!p.clickDamage?.length);
});

test('active click damage honors paper invulnerability and stops when troops embark',()=>{
  const g=arena(),mouse=g._spawn('mouse',0,700,800),paper=enemy(g,'paper',920);
  advance(g,.02);mouse.cooldown=100;advance(g,.5);assert.equal(paper.hp,paper.maxHp-TYPES.mouse.impactDamage);
  paper.paperInvulnerable=1.1;advance(g,1);assert.equal(paper.hp,paper.maxHp-TYPES.mouse.impactDamage);
  const cd=enemy(g,'cd',paper.x,paper.y);assert(g.loadTransport([paper.id],[cd.id]).ok);
  advance(g,2);assert.equal(paper.hp,paper.maxHp-TYPES.mouse.impactDamage);assert.equal(paper.clickDamage.length,0);
});

test('repeat clicks refresh one damage mark per mouse and still permit separate defensive mice',()=>{
  const g=arena(),a=g._spawn('mouse',0,700,800),b=g._spawn('mouse',0,700,900),cd=enemy(g,'cd',920,850);
  cd.hp=cd.maxHp=1000;advance(g,5);
  assert.equal(cd.clickDamage.length,2);assert.equal(new Set(cd.clickDamage.map(e=>e.sourceId)).size,2);
  assert(cd.clickDamage.every(mark=>mark.sourceId===a.id||mark.sourceId===b.id));
  assert(cd.hp<900,'successive pointer impacts and lingering ticks hurt the transport');
});

test('a mouse destroying a loaded CD releases passengers safely and records one transport kill',()=>{
  const g=arena(),mouse=g._spawn('mouse',0,700,800),cd=enemy(g,'cd',920),passenger=enemy(g,'rock',920,800);
  assert(g.loadTransport([passenger.id],[cd.id]).ok);cd.hp=10;
  advance(g,.8);assert(!g.getEntity(cd.id));assert(g.getEntity(passenger.id));assert(!passenger.transportedBy);
  assert(!g._blocked(passenger.x,passenger.y,passenger.r));assert.equal(g.stats.destroyed[0],1);
  assert(!g.projectiles.some(p=>p.targetId===cd.id));
});

test('bots pay for a mouse near a desk threatened by a hostile CD and avoid duplicate defenses',()=>{
  const g=new Game();g._think=Infinity;g.teams[1].trash=250;
  const home=g.starts[1],cd=g._spawn('cd',0,home.x-1100,home.y);
  g._bot(1);assert(!g.entities.some(e=>e.team===1&&e.type==='mouse'),'distant CDs do not trigger new defenses');
  const bank=g.teams[1].trash;assert.equal(bank,175,'ordinary worker and troop production still cost trash');
  cd.x=home.x-220;g._bot(1);
  const defenses=g.entities.filter(e=>e.team===1&&e.type==='mouse');assert.equal(defenses.length,1);
  assert(defenses[0].constructing>0);assert.equal(g.teams[1].trash,bank-TYPES.mouse.cost);
  const desk=g.entities.find(e=>e.team===1&&e.type==='desk');
  assert(Math.hypot(defenses[0].x-desk.x,defenses[0].y-desk.y)<350);
  g.teams[1].trash=1000;g._bot(1);
  assert.equal(g.entities.filter(e=>e.team===1&&e.type==='mouse').length,1,'an unfinished nearby mouse already covers this desk');
  const h=new Game();h._think=Infinity;h.time=50;h.teams[1].trash=2000;
  const worker=h.entities.find(e=>e.team===1&&e.type==='twig');
  h.entities=h.entities.filter(e=>e.team!==1||e.type!=='twig'||e.id===worker.id);
  h._spawn('cd',0,h.starts[1].x-220,h.starts[1].y);h._bot(1);
  const defense=h.entities.find(e=>e.team===1&&e.type==='mouse');
  assert.equal(worker.order.targetId,defense.id,'later production expansion must not take the defense builder');
});
