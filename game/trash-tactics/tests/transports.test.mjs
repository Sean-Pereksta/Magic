import test from 'node:test';
import assert from 'node:assert/strict';
import {Game,TYPES} from '../engine.js';

function arena(){
  const game=new Game();game._think=Infinity;
  game.entities=game.entities.filter(e=>e.type==='desk');
  return game;
}
function advance(game,seconds){for(let t=0;t<seconds-1e-9;){const dt=Math.min(.08,seconds-t);game.update(dt);t+=dt;}}
const spawn=(g,type,team,x,y)=>g._spawn(type,team,x,y);

test('CD readers are built by workers and summon CDs through a paid, supply-reserving queue',()=>{
  const g=arena(),twig=spawn(g,'twig',0,650,800);g.teams[0].trash=1000;
  const r=g.build([twig.id],'cdreader',700,800);assert(r.ok);
  const reader=g.getEntity(r.entityId);assert(!g.train(reader.id,'cd').ok);
  advance(g,11);assert.equal(reader.constructing,0);
  const before=g.getSupply(0).used;
  assert(g.train(reader.id,'cd').ok);assert.equal(g.getSupply(0).used,before+1);
  assert.equal(g.teams[0].trash,1000-TYPES.cdreader.cost-TYPES.cd.cost);
  assert(!g.train(reader.id,'rock').ok);
  advance(g,7.9);assert(!g.entities.some(e=>e.type==='cd'));
  advance(g,.2);const cd=g.entities.find(e=>e.type==='cd');assert(cd);
  assert.deepEqual(cd.cargo,[]);assert.equal(g.getSupply(0).used,before+1);
});

test('loading allows six allied combat troops, rejects twigs and enemies, and keeps passenger supply',()=>{
  const g=arena(),cd=spawn(g,'cd',0,700,800);
  const troops=Array.from({length:8},(_,i)=>spawn(g,['rock','paper','scissors'][i%3],0,700+i*3,800));
  const twig=spawn(g,'twig',0,710,800),enemy=spawn(g,'rock',1,710,800);
  const before=g.getSupply(0).used;
  assert(!g.loadTransport([twig.id,enemy.id],[cd.id]).ok);
  const r=g.loadTransport([...troops.map(e=>e.id),twig.id,enemy.id],[cd.id]);
  assert.equal(r.assigned,6);assert.equal(cd.cargo.length,6);
  assert.equal(troops.filter(e=>e.transportedBy===cd.id).length,6);
  assert(!twig.transportedBy);assert(!enemy.transportedBy);
  assert.equal(g.getSupply(0).used,before);
  assert(!g.loadTransport([troops[7].id],[cd.id]).ok);
});

test('distant selected troops walk to a CD and cargo ignores ground orders, abilities, and attacks',()=>{
  const g=arena(),cd=spawn(g,'cd',0,850,800),paper=spawn(g,'paper',0,600,800);
  const scissors=spawn(g,'scissors',1,850,800);scissors.cooldown=100;
  assert(g.loadTransport([paper.id],[cd.id]).ok);assert.equal(paper.order.type,'board');
  advance(g,3);assert.equal(paper.transportedBy,cd.id);
  assert(!g.canTarget(scissors,paper));assert(!g.canTarget(scissors,cd));
  g.teams[0].upgrades.paper=true;assert(!g.ability([paper.id],1000,800).ok);
  g.command([paper.id],{type:'move',x:1600,y:800});
  assert.equal(paper.order.type,'idle');
  g.teams[1].upgrades.scissors=true;g.ability([scissors.id],paper.x,paper.y);
  assert.equal(paper.hp,paper.maxHp,'sweep cannot hit passengers');
  g.command([cd.id],{type:'move',x:1000,y:800});advance(g,1);
  assert.equal(paper.x,cd.x);assert.equal(paper.y,cd.y);
  assert.equal(g.getSupply(0).units,2,'passenger remains in the supply ledger');
});

test('nearest loading reserves seats uniquely across multiple CDs and respects nearest eligible troops',()=>{
  const g=arena(),a=spawn(g,'cd',0,600,800),b=spawn(g,'cd',0,950,800);
  const nearA=Array.from({length:7},(_,i)=>spawn(g,'rock',0,610+i*2,800));
  const nearB=Array.from({length:7},(_,i)=>spawn(g,'paper',0,960+i*2,800));
  const twig=spawn(g,'twig',0,600,800);
  assert.equal(g.loadNearest([a.id,b.id]).assigned,12);
  assert.equal(a.cargo.length,6);assert.equal(b.cargo.length,6);
  assert(a.cargo.every(id=>nearA.some(e=>e.id===id)));
  assert(b.cargo.every(id=>nearB.some(e=>e.id===id)));
  assert.equal(new Set([...a.cargo,...b.cargo]).size,12);assert(!twig.transportedBy);
  assert(!g.loadNearest([a.id,b.id]).ok);
});

test('reserved boarding slots cannot overflow after repeated or overlapping load requests',()=>{
  const g=arena(),cd=spawn(g,'cd',0,950,800);
  const troops=Array.from({length:8},(_,i)=>spawn(g,'rock',0,600+i*45,1100));
  assert.equal(g.loadTransport(troops.slice(0,6).map(e=>e.id),[cd.id]).assigned,6);
  assert(!g.loadTransport([troops[6].id],[cd.id]).ok);
  assert.equal(g.loadTransport([troops[6].id,...troops.slice(0,6).map(e=>e.id)],[cd.id]).assigned,6);
  assert.equal(g._boarders(cd).length,6);
  advance(g,15);assert.equal(cd.cargo.length,6);assert.equal(g._boarders(cd).length,0);
});

test('a failed transfer into a full CD preserves a troop\'s existing boarding order',()=>{
  const g=arena(),a=spawn(g,'cd',0,950,800),b=spawn(g,'cd',0,950,1100),troop=spawn(g,'rock',0,600,800);
  const full=Array.from({length:6},()=>spawn(g,'paper',0,950,1100));
  assert(g.loadTransport(full.map(e=>e.id),[b.id]).ok);assert.equal(b.cargo.length,6);
  assert(g.loadTransport([troop.id],[a.id]).ok);const original={...troop.order};
  assert(!g.loadTransport([troop.id],[b.id]).ok);assert.deepEqual(troop.order,original);
  advance(g,4);assert.equal(troop.transportedBy,a.id);assert.equal(b.cargo.length,6);
});

test('multiple CDs fly over obstacles and unload every passenger into separate safe ground slots',()=>{
  const g=arena(),book=g.map.obstacles.find(o=>o.type==='book'),target={x:book.x+book.w/2,y:book.y+book.h/2},start={x:book.x-80,y:target.y};
  const cds=[spawn(g,'cd',0,start.x,start.y),spawn(g,'cd',0,start.x+30,start.y)];
  const troops=Array.from({length:12},(_,i)=>spawn(g,['rock','paper','scissors'][i%3],0,start.x+i*3,start.y));
  assert(g.loadTransport(troops.map(e=>e.id),cds.map(e=>e.id)).ok);
  const allIds=cds.flatMap(cd=>cd.cargo);assert.equal(allIds.length,12);
  assert(g._blocked(target.x,target.y,18),'drop target is inside the actual enlarged book');
  assert(g.dropTransports(cds.map(e=>e.id),target.x,target.y).ok,'drop target is inside a book');
  advance(g,1.2);assert(troops.every(e=>e.transportedBy),'cargo remains aboard while flying');
  assert(cds.some(cd=>g._blocked(cd.x,cd.y,cd.r)),'flight crosses impassable ground');
  advance(g,6);assert(cds.every(cd=>cd.cargo.length===0));
  assert(troops.every(e=>!e.transportedBy&&!g._blocked(e.x,e.y,e.r)));
  for(let i=0;i<troops.length;i++)for(let j=i+1;j<troops.length;j++)assert(Math.hypot(troops[i].x-troops[j].x,troops[i].y-troops[j].y)>=troops[i].r+troops[j].r+4.9);
  assert(!g.dropTransports(cds.map(e=>e.id),1000,800).ok,'empty CDs cannot drop');
});

test('a removed CD safely releases passengers and pending boarders recover instead of disappearing',()=>{
  const g=arena(),cd=spawn(g,'cd',0,700,800),a=spawn(g,'rock',0,710,800),b=spawn(g,'paper',0,500,800);
  assert(g.loadTransport([a.id,b.id],[cd.id]).ok);assert(a.transportedBy);assert.equal(b.order.type,'board');
  cd.hp=0;advance(g,.1);
  assert(!g.getEntity(cd.id));assert(!a.transportedBy);assert(g.getEntity(a.id));
  assert(!g._blocked(a.x,a.y,a.r));assert.equal(b.order.type,'idle');
});

test('direct orders, pursuit and automatic target selection all reject exact counters',()=>{
  for(const [type,counter] of [['rock','paper'],['paper','scissors'],['scissors','rock']]){
    const g=arena(),unit=spawn(g,type,0,700,800),enemy=spawn(g,counter,1,900,800);
    enemy.order={type:'move',x:1500,y:800};
    assert(!g.canTarget(unit,enemy));g.command([unit.id],{type:'attack',targetId:enemy.id});
    assert.equal(unit.order.type,'idle');const x=unit.x;advance(g,.5);assert.equal(unit.x,x);
    unit.order={type:'attack',targetId:enemy.id};advance(g,.1);assert.equal(unit.order.type,'idle');
    enemy.x=740;enemy.y=800;enemy.order={type:'idle'};enemy.cooldown=100;
    advance(g,.3);assert.equal(unit.combatTarget,undefined);assert.equal(enemy.hp,enemy.maxHp);
  }
});

test('combat attacks use the slower 1.25 second interval',()=>{
  const g=arena(),rock=spawn(g,'rock',0,700,800),building=spawn(g,'cabinet',1,740,800);
  g.command([rock.id],{type:'attack',targetId:building.id});advance(g,.01);
  assert.equal(building.hp,building.maxHp-24);
  advance(g,1.1);assert.equal(building.hp,building.maxHp-24);
  advance(g,.2);assert.equal(building.hp,building.maxHp-48);
});
