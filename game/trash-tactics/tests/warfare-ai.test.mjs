import test from 'node:test';
import assert from 'node:assert/strict';
import {Game,TYPES} from '../engine.js';

function productionArena(){
  const g=new Game();g._think=Infinity;g.entities=g.entities.filter(e=>e.type==='desk');g.time=200;
  const team=1,start=g.starts[team];g._bots[team].lastExpansion=200;g.teams[team].upgrades={rock:true,paper:true,scissors:true};
  for(let i=0;i<11;i++)g._spawn('twig',team,start.x+50+(i%4)*35,start.y+140+Math.floor(i/4)*35);
  const printer=g._spawn('printer',team,start.x-180,start.y-100),workshop=g._spawn('workshop',team,start.x-180,start.y+100),lab=g._spawn('laboratory',team,start.x-360,start.y);
  for(let i=0;i<6;i++)g._spawn('cabinet',team,start.x-100-i*90,start.y+350);
  return {g,printer,workshop,lab,team,start};
}

test('AI saves the next laboratory purchase instead of spending its trash and weighted supply on lower tiers',()=>{
  const {g,printer,workshop,lab,team,start}=productionArena();
  for(let i=0;i<40;i++)g._spawn('rock',team,start.x-600+(i%8)*42,start.y-500+Math.floor(i/8)*42);
  g.teams[team].trash=450;g._bot(team);
  assert.equal(g.teams[team].trash,450,'lower tiers leave the next 500-trash Divider purchase available');
  assert.equal(printer.queue.length,0);assert.equal(workshop.queue.length,0);assert.equal(lab.queue.length,0);
  // Five free supply cannot fit a Divider, even though an ordinary troop fits.
  for(let i=0;i<4;i++)g._spawn('paper',team,start.x-700+i*42,start.y-600);
  g.teams[team].trash=2000;const cap=g.getSupply(team).cap;assert.equal(cap-g.getSupply(team).used,5);g._bot(team);
  assert.equal(printer.queue.length,0);assert.equal(workshop.queue.length,0);assert.equal(lab.queue.length,0);
  const cabinet=g.entities.find(e=>e.team===team&&e.type==='cabinet'&&e.constructing);assert(cabinet,'the bot buys extra storage for its planned specialist');
  const constructionCost=g.entities.filter(e=>e.team===team&&e.constructing).reduce((sum,e)=>sum+TYPES[e.type].cost,0);
  assert.equal(g.teams[team].trash,2000-constructionCost);
  cabinet.constructing=0;g._bot(team);assert.equal(lab.queue[0]?.type,'divider');assert.equal(g.getSupply(team).reserved,6);
  assert.equal(printer.queue.length,0,'lower-tier production still leaves the next planned specialist slots');
});

test('AI alternates paid tier purchases and keeps independent workshop and laboratory troop rotations',()=>{
  const {g,workshop,lab,team}=productionArena();g.teams[team].trash=5000;
  const expected=[['sniper','divider'],['airplane','rollingrock'],['mage','shears']];
  for(const [tier2,tier3] of expected){
    const before=g.teams[team].trash;g._bot(team);
    assert.equal(workshop.queue[0]?.type,tier2);assert.equal(lab.queue[0]?.type,tier3);
    assert(g.teams[team].trash<=before-TYPES[tier2].cost-TYPES[tier3].cost);
    // Complete actual paid production without involving combat in this fixture.
    for(const b of [workshop,lab]){b.queue[0].remaining=.01;g._building(b,.02);}
    assert(g.entities.some(e=>e.team===team&&e.type===tier2));assert(g.entities.some(e=>e.team===team&&e.type===tier3));
  }
});
