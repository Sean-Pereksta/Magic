import assert from 'node:assert/strict';
import test from 'node:test';
import {createSimulation} from './simulation-harness.mjs';

const json=(h,s)=>JSON.parse(h.run(`JSON.stringify(${s})`));
const near=(a,b)=>assert.ok(Math.abs(a-b)<1e-8,`${a} != ${b}`);
function mission(launch=false){
  const h=createSimulation();
  h.run(`state.ships=[];state.p=owned(0)[0];state.target=state.planets.find(p=>p.owner===null);
    state.p.pop=1;state.p.orders=[];state.p.buildQueue=[];state.p.localProject=null;
    STX_IF_RESOURCES.forEach(r=>state.p.stock[r]=r==='trained'?1:1000);
    empire(0).credits=100;state.before={stock:{...state.p.stock},pop:state.p.pop,credits:empire(0).credits};
    startExpansionProject(empire(0),state.p,state.target,true);`);
  if(launch)h.run(`stxSDAllocatePlanet(state.p);state.p.expansionProject.volunteers=state.p.expansionProject.goal;
    tickExpansionProject(state.p,0);state.colony=state.ships.find(s=>s.type==='colony');`);
  return h;
}
test('claimed destinations cancel staged missions and refund only committed materials and paid credits',()=>{
  const h=mission();
  h.run(`state.p.stock.iron-=7;state.p.expansionProject.stxSupply={delivered:{iron:7},orderIds:{}};
    state.p.expansionProject.volunteers=.02;state.target.owner=1;stxCMCancelInvalid();stxCMCancelInvalid();`);
  assert.equal(h.run('state.p.expansionProject'),null);
  assert.deepEqual(json(h,'state.p.stock'),json(h,'state.before.stock'));
  near(h.run('state.p.pop'),1);near(h.run('empire(0).credits'),100);
});
test('in-flight missions cancel immediately and return the complete paid hull, crew, settlers and credits exactly once',()=>{
  const h=mission(true);
  assert.equal(h.run('state.ships.filter(s=>s.type==="colony").length'),1);
  assert.ok(h.run('state.p.pop')<1);
  h.run('state.target.owner=1;tickShips(0);stxCMCancelShip(state.colony);tickShips(0)');
  assert.equal(h.run('state.ships.length'),0);
  for(const r of Object.keys(json(h,'state.before.stock')))near(h.run(`state.p.stock.${r}`),h.run(`state.before.stock.${r}`));
  near(h.run('state.p.pop'),1);near(h.run('empire(0).credits'),100);
  assert.equal(h.run('state.target.owner'),1);
});
test('another friendly colony also invalidates the mission; successful neutral landings remain unchanged',()=>{
  const h=mission(true);
  h.run('state.target.owner=0;tickShips(0)');assert.equal(h.run('state.ships.length'),0);near(h.run('state.p.pop'),1);
  const live=mission(true);
  live.run('tickShips(10000)');
  assert.equal(live.run('state.target.owner'),0);
  near(live.run('state.p.pop+state.target.pop'),1);
  assert.ok(live.run('empire(0).credits')<100);
});
test('project freight is cancelled and refunded without removing unrelated cancelled ships',()=>{
  const h=mission();
  h.run(`state.p.expansionProject.stxProjectId='mission';state.p.expansionProject.stxSupply={delivered:{},orderIds:{iron:'order'}};
    state.p.orders=[{id:'order',status:'in transit',stxProjectId:'mission'}];state.p.stock.iron-=12;
    state.ships=[{id:'supply',owner:0,from:state.p.id,to:state.p.id,type:'freighter',cargo:{iron:12},orderId:'order',stxProjectId:'mission'},
      {id:'unrelated',type:'freighter',stxCancelled:true}];
    state.target.owner=1;stxCMCancelInvalid();stxCMCancelInvalid();`);
  assert.deepEqual(json(h,'state.ships.map(s=>s.id)'),['unrelated']);
  assert.equal(h.run('state.p.orders[0].status'),'cancelled');near(h.run('state.p.stock.iron'),1000);
});
test('refunds follow the original empire when its sponsor is captured, including no-world escrow',()=>{
  const h=mission(true);
  h.run(`owned(0).forEach(p=>p.owner=1);state.target.owner=1;stxCMCancelInvalid();`);
  near(h.run('empire(0).credits'),100);
  assert.equal(h.run('empire(0).stxColonyRefunds.length'),1);
  assert.ok(h.run('state.p.pop')<1);
  h.run('state.p.owner=0;stxCMCancelInvalid();stxCMCancelInvalid()');near(h.run('state.p.pop'),1);
  near(h.run('state.p.stock.components'),1000);
  assert.equal(h.run('empire(0).stxColonyRefunds.length'),0);
});
test('save/load preserves the paid launch ledger and cannot refund twice',()=>{
  const h=mission(true);
  h.run('saveGame(false);loadGame();state.planets.find(p=>p.id===state.target.id).owner=1;stxCMCancelInvalid();saveGame(false);loadGame();stxCMCancelInvalid()');
  near(h.run('empire(0).credits'),100);
  near(h.run('owned(0)[0].pop'),1);assert.equal(h.run('state.ships.length'),0);
});
test('legacy in-flight colony saves recover their paid recipe, passengers and recoverable launch fee',()=>{
  const h=mission(true);h.run('delete state.colony.stxColonyRefund;state.target.owner=1;stxCMCancelInvalid()');
  near(h.run('empire(0).credits'),100);near(h.run('state.p.stock.components'),1000);near(h.run('state.p.stock.trained'),1);near(h.run('state.p.pop'),1);
});
test('capacity-blocked launch keeps its project and refunds it on a later claim',()=>{
  const h=mission();
  h.run(`stxSDAllocatePlanet(state.p);state.p.expansionProject.volunteers=state.p.expansionProject.goal;
    state.ships=Array.from({length:280},(_,i)=>({id:'cap'+i}));tickExpansionProject(state.p,0);`);
  assert.ok(h.run('state.p.expansionProject'));near(h.run('state.p.pop'),1);
  h.run('state.target.owner=1;stxCMCancelInvalid()');near(h.run('empire(0).credits'),100);
  near(h.run('state.p.stock.trained'),1);assert.equal(h.run('state.ships.length'),280);
});
test('destroyed/intercepted colony ships never receive a cancellation refund',()=>{
  const h=mission(true);h.run('state.colony.stxIntercepted=true;state.target.owner=1;stxCMCancelInvalid()');
  assert.ok(h.run('empire(0).credits')<100);assert.ok(h.run('state.p.pop')<1);
});
