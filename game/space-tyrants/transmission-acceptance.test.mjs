import assert from 'node:assert/strict';
import test from 'node:test';
import {createSimulation} from './simulation-harness.mjs';
const json=(h,s)=>JSON.parse(h.run(`JSON.stringify(${s})`));
const near=(a,b)=>assert.ok(Math.abs(a-b)<1e-8,`${a} != ${b}`);
function lab(){
  const h=createSimulation();
  h.run(`state.ships=[];state.proposals=[];state.militaryRequests=[];state.wars=[];
    state.p=owned(0)[0];state.p.infra.shipyard=2;state.p.reserveVessels=100;state.p.localProject=null;state.p.buildQueue=[];
    state.empires.forEach(e=>e.credits=1000);state.planets.forEach(p=>{p.underAttack=false;p.orders=[];p.buildQueue=[];
      STX_IF_RESOURCES.forEach(r=>p.stock[r]=r==='trained'?1:1000)});
    state.rivalDiplomacy.requests=[];state.rivalDiplomacy.agreements=[];`);
  return h;
}
function military(h,id='commission',cost=30){h.run(`state.militaryRequests.push({id:'${id}',type:'fleet',planetId:state.p.id,creditCost:${cost},status:'pending',title:'Fleet commission',expiresAt:1000})`)}
function trade(h,{credits=30,resource=null,amount=20}={}){
  h.run(`state.proposals.push({id:'trade',kind:'trade',from:1,to:0,title:'Industrial exchange',status:'pending',expiresAt:1000,
    offer:{resource:'iron',amount:20},request:${JSON.stringify(resource?{resource,amount}:{credits})},duration:1})`);
}
test('Accept is routed to approval, records acceptance and charges a military commission once',()=>{
  const h=lab();military(h);
  assert.equal(h.run('stxTXAct("military","commission","accept")'),true);
  assert.equal(h.run('state.militaryRequests[0].status'),'accepted');assert.equal(h.run('state.fleets.filter(f=>f.owner===0).length'),1);
  near(h.run('empire(0).credits'),970);
  assert.equal(h.run('stxTXAct("military","commission","accept")'),false);
  assert.equal(h.run('state.fleets.filter(f=>f.owner===0).length'),1);
  assert.deepEqual(json(h,'empire(0).stxTransmissionHistory.map(x=>x.status)'),['accepted']);
});
test('delegated card clicks use explicit actions; unknown actions never decline requests',()=>{
  const h=lab();military(h);
  h.context.txButton={dataset:{txType:'military',txId:'commission',txAction:'accept'},disabled:false};
  h.run(`document.getElementById('transmissionList').dispatchEvent({type:'click',target:{closest:()=>txButton},preventDefault(){},stopImmediatePropagation(){}})`);
  assert.equal(h.run('state.militaryRequests[0].status'),'accepted');
  military(h,'other');assert.equal(h.run('stxTXAct("military","other","typo")'),false);
  assert.equal(h.run('state.militaryRequests[1].status'),'pending');
});
test('governor and capital requests start parallel projects immediately',()=>{
  const h=lab();h.run(`state.p.localProject={type:'mine'};state.proposals=[{id:'g',kind:'governor',to:0,planetId:state.p.id,project:'factory',cost:40,status:'pending',title:'Factory'}];state.militaryRequests=[{id:'c',type:'capital',capitalKind:'shipyard',planetId:state.p.id,creditCost:50,status:'pending',title:'Shipyard'}]`);assert.equal(h.run('stxTXAct("proposal","g","accept")'),true);assert.equal(h.run('stxTXAct("military","c","accept")'),true);assert.deepEqual(json(h,'state.p.stxLocalProjects.map(q=>q.type)'),['mine','factory','shipyard']);near(h.run('empire(0).credits'),904);
});
test('project acceptance requires authorization fee and grant but never enters diplomatic queue',()=>{
  const h=lab();h.run(`empire(0).credits=40;state.proposals=[{id:'g',kind:'governor',to:0,planetId:state.p.id,project:'factory',cost:40,status:'pending',title:'Factory'}]`);assert.equal(h.run('stxTXEnqueue("proposal","g")'),false);assert.equal(h.run('stxTXAct("proposal","g","accept")'),false);h.run('empire(0).credits=43');assert.equal(h.run('stxTXAct("proposal","g","accept")'),true);near(h.run('empire(0).credits'),0);
});
test('diplomatic trade queue retries with no reservation or duplicate spending',()=>{
  const h=lab();trade(h);h.run('empire(0).credits=0;stxTXEnqueue("proposal","trade");stxTXEnqueue("proposal","trade")');assert.equal(h.run('stxTXQueue().length'),1);h.run('empire(0).credits=30;stxTXProcessQueue();stxTXProcessQueue()');assert.equal(h.run('state.proposals[0].status'),'accepted');near(h.run('empire(0).credits'),0);assert.equal(h.run('state.ships.length'),1);assert.equal(h.run('stxTXQueue().length'),0);
});
test('queued diplomatic acceptance survives save/load and runs during normal simulation',()=>{
  const h=lab();trade(h);h.run('empire(0).credits=0;stxTXEnqueue("proposal","trade");saveGame(false);loadGame()');assert.equal(h.run('stxTXQueue().length'),1);h.run('empire(0).credits=100;simulate(.12)');assert.equal(h.run('state.proposals.find(q=>q.id==="trade").status'),'accepted');assert.equal(h.run('stxTXQueue().length'),0);
});
test('queued expired or invalidated trades cancel with history and manual unqueue leaves them pending',()=>{
  const h=lab();trade(h);h.run(`state.proposals=['expired','invalid','manual'].map((id,i)=>({...state.proposals[0],id,from:i===1?2:1}));empire(0).credits=0;state.proposals.forEach(q=>stxTXEnqueue('proposal',q.id));stxTXCancelQueue('proposal','manual');state.proposals[0].expiresAt=0;stxDSAddAgreement('embargo',2,0,100);stxTXProcessQueue()`);assert.equal(h.run('stxTXQueue().length'),0);assert.equal(h.run('state.proposals[2].status'),'pending');const statuses=json(h,'empire(0).stxTransmissionHistory.map(x=>x.status)');for(const s of ['expired','cancelled','unqueued'])assert.ok(statuses.includes(s));
});
test('trade acceptance sends real cargo and transfers the payment exactly once',()=>{
  const h=lab();trade(h);
  h.run('state.seller=owned(1)[0];state.ironBefore=owned(1).reduce((n,p)=>n+p.stock.iron,0)');
  assert.equal(h.run('stxTXAct("proposal","trade","accept")'),true);
  near(h.run('owned(1).reduce((n,p)=>n+p.stock.iron,0)'),h.run('state.ironBefore-20'));
  near(h.run('empire(0).credits'),970);near(h.run('empire(1).credits'),1029.96);
  assert.equal(h.run('state.ships[0].cargo.iron'),20);assert.equal(h.run('state.proposals[0].status'),'accepted');
  h.run('stxTXAct("proposal","trade","accept")');assert.equal(h.run('state.ships.length'),1);
});
test('material queue accepts once uncommitted resources arrive with no hidden player freight fee',()=>{
  const h=lab();trade(h,{resource:'components',amount:20});h.run('owned(0).forEach(p=>p.stock.components=0);empire(0).credits=0;stxTXEnqueue("proposal","trade")');assert.equal(h.run('state.proposals[0].status'),'pending');h.run('state.p.stock.components=1000;stxTXProcessQueue()');assert.equal(h.run('state.proposals[0].status'),'accepted');assert.equal(h.run('state.ships.length'),2);near(h.run('empire(0).credits'),0);
});
test('a failed second dispatch restores both parties stock, freight fees and random state',()=>{
  const h=lab();trade(h,{resource:'components'});
  h.run(`state.plan=stxTXTradePlan(1,0,'iron',20,{resource:'components',amount:20});
    state.before=JSON.stringify({stock:state.planets.map(p=>p.stock),credits:state.empires.map(e=>e.credits),rng:state.rngState});
    window.originalCreate=createShip;window.calls=0;createShip=function(...args){return ++window.calls===2?null:window.originalCreate(...args)};`);
  assert.equal(h.run('stxTXExecuteTrade(state.plan)'),false);
  assert.equal(h.run('JSON.stringify({stock:state.planets.map(p=>p.stock),credits:state.empires.map(e=>e.credits),rng:state.rngState})'),h.run('state.before'));
  assert.equal(h.run('state.ships.length'),0);
});
test('resource offers retain contact acceptance and fulfillment accounting',()=>{
  const h=lab();
  h.run(`state.resourceTradeEconomy.requests=[{id:'r',resource:'iron',status:'open',expiresAt:1000,desired:20,accepted:0,offerIds:['o','unused'],contacts:[{from:1,status:'offered'}]}];
    state.resourceTradeEconomy.offers=[{id:'o',requestId:'r',from:1,resource:'iron',amount:20,payment:{credits:30},status:'pending',expiresAt:1000},
      {id:'unused',requestId:'r',from:2,resource:'iron',amount:20,payment:{credits:40},status:'pending',expiresAt:1000}];
    stxTXAct('trade','o','accept');`);
  assert.equal(h.run('state.resourceTradeEconomy.requests[0].accepted'),20);
  assert.equal(h.run('state.resourceTradeEconomy.requests[0].status'),'fulfilled');
  assert.equal(h.run('state.resourceTradeEconomy.requests[0].contacts[0].status'),'accepted');
  assert.equal(h.run('state.resourceTradeEconomy.offers[1].status'),'closed');
});
test('diplomatic tribute can be queued and accepts without becoming a refusal',()=>{
  const h=lab();
  h.run(`state.rivalDiplomacy.requests=[{id:'aid',from:1,to:0,actionKind:'tribute',kind:'request',title:'Reparations',amount:25,status:'pending',expiresAt:1000}];
    empire(0).credits=0;stxTXEnqueue('diplomacy','aid');empire(0).credits=25;stxTXProcessQueue();`);
  assert.equal(h.run('state.rivalDiplomacy.requests[0].status'),'accepted');near(h.run('empire(0).credits'),0);
  assert.equal(h.run('stxRDPair(1,0).refusedRequests'),0);
});
test('migration is accepted only when actual passengers depart; dispatch failure cannot mint population',()=>{
  const h=lab();
  h.run(`state.proposals=[{id:'m',kind:'migration',from:1,to:0,planetId:state.p.id,amount:.005,title:'Refugees',status:'pending'}];
    state.popBefore=owned(1).reduce((n,p)=>n+p.pop,0);window.originalCreate=createShip;createShip=()=>null;`);
  assert.equal(h.run('stxTXAct("proposal","m","accept")'),false);near(h.run('owned(1).reduce((n,p)=>n+p.pop,0)'),h.run('state.popBefore'));
  h.run('createShip=window.originalCreate;stxTXAct("proposal","m","accept")');
  assert.equal(h.run('state.proposals[0].status'),'accepted');
  near(h.run('owned(1).reduce((n,p)=>n+p.pop,0)+state.ships[0].cargo.population'),h.run('state.popBefore'));
});
