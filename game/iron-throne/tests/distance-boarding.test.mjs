import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { emptyUnits } from '../economy.mjs';
import { orderArmy, parseSave, resolveMovement } from '../core.mjs';
import { armyArrivalTurns } from '../movement-timing.mjs';
import { boardingArrival, orderEmbark, resolveFleetMovement } from '../naval.mjs';
import { SHIPS, cargoCount, fleetCapacity } from '../naval-state.mjs';
import { armyShipClick } from '../ship-click.mjs';
import { drawOrderIndicators, mapOrders } from '../map-orders.mjs';
import { knowledgeView, refreshKnowledge } from '../fog.mjs';
import { splitCampaign, joinCampaign, playerView } from '../multiplayer-state.mjs';
import { encodePayload, decodePayload } from '../multiplayer-state.mjs';
import { applyCommand } from '../multiplayer-commands.mjs';
import { onlineGame, activateForTest } from './fixtures/online-game.mjs';

const base=createGame();
function world(q=10,count=25) {
  const s=structuredClone(base);s.fog={version:1,houses:{}};
  for(const t of Object.values(s.tiles))if(t.q>0&&t.q<15&&t.r>=8&&t.r<=15){
    t.terrain='plains';t.owner='ashen';t.road=false;t.river=false;
    if(t.levels)delete t.levels.road;
  }
  const a=s.armies[0];Object.assign(a,{tile:`${q},10`,units:{...emptyUnits(),levy:count}});
  const f={id:`fleet-${s.nextId++}`,owner:'ashen',tile:'0,10',node:'sea:0,10',ships:[{id:`ship-${s.nextId++}`,type:'transport',hp:SHIPS.transport.hull,crew:6,cargo:[]}],cargo:[],morale:1,path:[],target:null,order:'hold',landing:null,movementTurn:s.turn,movementSpent:0,resolvedTurn:0};
  s.fleets.push(f);refreshKnowledge(s);return {s,a,f};
}
const resolve=s=>{resolveFleetMovement(s,'ashen');resolveMovement(s,'ashen');refreshKnowledge(s);};
const indicator=(s,a)=>mapOrders(knowledgeView(s,'ashen')).find(o=>o.id===a.id);

test('March click from inland queues one persistent route, counts down 2/1/0, and boards exactly once',()=>{
  const {s,a,f}=world();
  assert.deepEqual(armyShipClick(knowledgeView(s,'ashen'),'ashen',a.id,f.tile,'move'),{type:'fleetEmbark',args:{army:a.id,fleet:f.id}});
  assert.equal(orderEmbark(s,'ashen',a.id,f.id).ok,true);
  assert.equal(a.embarkOrder.type,'board-transport');assert.equal(a.path.length,9);
  for(const turns of [2,1,0]){
    assert.equal(indicator(s,a).turns,turns);assert.equal(indicator(s,a).kind,'load');
    resolve(s);
    if(turns){assert.ok(s.armies.includes(a));assert.equal(cargoCount(f),0);assert.equal(a.embarkOrder.fleet,f.id);s.turn++;}
  }
  assert.equal(cargoCount(f),25);assert.ok(!s.armies.includes(a));assert.equal(f.cargo[0].id,a.id);
  resolve(s);assert.equal(cargoCount(f),25);assert.equal(f.cargo.length,1);
  assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));
});
test('adjacent and same-resolution march-and-board both show 0, never an extra boarding turn',()=>{
  for(const q of [1,4]){const {s,a,f}=world(q);orderEmbark(s,'ashen',a.id,f.id);assert.equal(indicator(s,a).turns,0);resolve(s);assert.equal(cargoCount(f),25);}
});
test('normal route ETA matches actual costs, roads, rivers, formation, general speed and costly first edges',()=>{
  for(const scenario of ['plain','river','road','defensive','siege','general','spent']){
    const {s,a}=world(10);const path=['9,10','8,10','7,10','6,10','5,10','4,10'];
    if(scenario==='river')s.tiles['8,10'].river=true;
    if(scenario==='road')for(const id of [a.tile,...path]){s.tiles[id].road=true;s.tiles[id].levels={...s.tiles[id].levels,road:3};}
    if(scenario==='defensive')a.formation='defensive';
    if(scenario==='siege')a.units={...emptyUnits(),trebuchet:1};
    if(scenario==='general')a.commandMove=2;
    if(scenario==='spent'){a.movementTurn=s.turn;a.movementSpent=2.5;}
    a.path=path;a.target=path.at(-1);a.order='move';
    const turns=armyArrivalTurns(knowledgeView(s,'ashen'),a);
    assert.equal(indicator(s,a).turns,turns);
    for(let i=0;i<=turns;i++){resolveMovement(s,'ashen');if(i<turns){assert.ok(a.path.length,scenario);s.turn++;}}
    assert.equal(a.tile,'4,10',scenario);assert.equal(a.path.length,0,scenario);
  }
});
test('ETA includes known zones of control, and already-used activations start at 1',()=>{
  const {s,a}=world(10);s.wars.push('ashen:wintermere');
  const enemy=s.armies.find(a=>a.owner==='wintermere');enemy.tile='8,11';
  a.path=['9,10','8,10','7,10'];a.target='7,10';a.order='move';
  const eta=armyArrivalTurns(s,a);assert.ok(eta>0);
  for(let i=0;i<=eta;i++){resolveMovement(s,'ashen');if(i<eta)s.turn++;}assert.equal(a.tile,'7,10');
  const t=world(4);t.a.path=['3,10'];t.a.target='3,10';t.a.resolvedTurn=t.s.turn;
  assert.equal(armyArrivalTurns(t.s,t.a),1);
});
test('transport movement repaths toward the same fleet ID instead of the old shoreline',()=>{
  const {s,a,f}=world(7);orderEmbark(s,'ashen',a.id,f.id);const old=a.embarkOrder.embarkPosition;
  f.tile='0,14';f.node='sea:0,14';refreshKnowledge(s);resolve(s);
  assert.equal(a.embarkOrder.fleet,f.id);assert.notEqual(a.embarkOrder.embarkPosition,old);assert.equal(cargoCount(f),0);
  for(let i=0;i<8&&s.armies.includes(a);i++){s.turn++;resolve(s);}assert.equal(cargoCount(f),25);
});
test('ETA forecasts sailing before land movement and follows a moving transport',()=>{
  const {s,a,f}=world(7);orderEmbark(s,'ashen',a.id,f.id);
  f.path=['sea:0,11','sea:0,12','sea:0,13','sea:0,14'];f.target=f.path.at(-1);f.order='move';
  const eta=boardingArrival(knowledgeView(s,'ashen'),a,f).turns;
  for(let i=0;i<=eta;i++){resolve(s);if(i<eta){assert.equal(cargoCount(f),0);s.turn++;}}
  assert.equal(cargoCount(f),25);assert.equal(f.tile,'0,14');
});
test('partial boarding honors preview and reservations, leaves 45 ashore, and cannot overbook',()=>{
  const {s,a,f}=world(4,70);
  assert.equal(orderEmbark(s,'ashen',a.id,f.id,26).ok,false);assert.equal(a.embarkOrder,undefined);
  assert.deepEqual(orderEmbark(s,'ashen',a.id,f.id,25),{ok:true,count:25,remaining:45});
  const b={...a,id:`army-${s.nextId++}`,units:{...emptyUnits(),levy:20},path:[],target:null,order:'hold'};delete b.embarkOrder;s.armies.push(b);
  assert.equal(orderEmbark(s,'ashen',b.id,f.id).ok,false);resolve(s);
  assert.equal(a.units.levy,45);assert.equal(cargoCount(f),fleetCapacity(f));assert.notEqual(a.id,f.cargo[0].id);
  assert.equal(a.embarkOrder,undefined);assert.equal(a.order,'hold');
});
test('capacity loss pauses and explains the order, then resumes when seats return',()=>{
  const {s,a,f}=world();orderEmbark(s,'ashen',a.id,f.id);
  f.ships[0].type='warship';resolve(s);
  assert.equal(a.tile,'10,10');assert.equal(a.embarkOrder.status,'paused');assert.match(a.boardingNotice,/capacity/);
  assert.equal(indicator(s,a),undefined);assert.ok(knowledgeView(s,'ashen').events.some(e=>e.message.includes('capacity')));
  f.ships[0].type='transport';s.turn++;resolve(s);assert.equal(a.embarkOrder.status,'active');assert.equal(a.boardingNotice,undefined);
});
test('destroyed or foreign transports cancel safely without converting boarding to an attack',()=>{
  for(const missing of [true,false]){
    const {s,a,f}=world();orderEmbark(s,'ashen',a.id,f.id);
    if(missing)s.fleets=[];else{f.owner='wintermere';s.wars.push('ashen:wintermere');}
    resolve(s);assert.equal(a.tile,'10,10');assert.equal(a.embarkOrder,undefined);assert.equal(a.order,'hold');assert.equal(a.units.levy,25);assert.ok(a.boardingNotice);
  }
});
test('blocked routes pause on valid land, and alternate routes avoid visible enemy troops',()=>{
  const {s,a,f}=world();orderEmbark(s,'ashen',a.id,f.id);
  const shore=Object.values(s.tiles).filter(t=>t.q===1);for(const t of shore)t.terrain='mountain';refreshKnowledge(s);
  resolve(s);assert.equal(a.tile,'10,10');assert.equal(a.embarkOrder.status,'paused');assert.match(a.boardingNotice,/route/);
  s.tiles['1,10'].terrain='plains';s.turn++;resolve(s);assert.equal(a.embarkOrder.status,'active');
  const t=world();t.s.wars.push('ashen:wintermere');const e=t.s.armies.find(a=>a.owner==='wintermere');e.tile='8,10';refreshKnowledge(t.s);
  orderEmbark(t.s,'ashen',t.a.id,t.f.id);assert.ok(!t.a.path.includes(e.tile));
});
test('save/load and serialized multiplayer reconnect preserve fleet, remaining route, and derived ETA',async()=>{
  const {s,a,f}=world();orderEmbark(s,'ashen',a.id,f.id);resolve(s);s.turn++;
  const loaded=parseSave(JSON.stringify(s)),army=loaded.armies.find(x=>x.id===a.id);
  assert.deepEqual(army.embarkOrder,a.embarkOrder);assert.deepEqual(army.path,a.path);assert.equal(indicator(loaded,army).turns,1);
  const parts=splitCampaign(loaded),restored=joinCampaign(await decodePayload(await encodePayload(parts.canonical)),parts.privateByHouse);
  assert.deepEqual(restored.armies.find(x=>x.id===a.id).embarkOrder,a.embarkOrder);
  const view=playerView(parts.world,parts.privateByHouse.ashen,'ashen');assert.equal(mapOrders(view).find(o=>o.id===a.id).turns,1);
  for(const turns of [1,0]){assert.equal(indicator(restored,a).turns,turns);resolve(restored);if(turns)restored.turn++;}
  assert.equal(cargoCount(restored.fleets.find(x=>x.id===f.id)),25);
  assert.ok(!parts.privateByHouse.wintermere.view.armies.some(x=>x.embarkOrder));
});
test('canonical multiplayer boarding command accepts a remote route and rejects stale preview counts',()=>{
  const {state:s,meta}=onlineGame();activateForTest(s,meta,'ashen');
  const t=world();s.tiles=t.s.tiles;s.armies=t.s.armies;s.fleets=t.s.fleets;s.nextId=t.s.nextId;
  const a=s.armies[0],f=s.fleets[0];refreshKnowledge(s);
  const cmd={id:'distance-board-01',clientId:'distance-board-client',sequence:4,uid:'u0',actorHouseId:'ashen',turn:s.turn,stateVersion:meta.stateVersion,epoch:meta.epoch,activationId:meta.activationId,type:'fleetEmbark',args:{army:a.id,fleet:f.id,count:26}};
  assert.equal(applyCommand(s,meta,cmd,{now:1000}).ok,false);cmd.id='distance-board-02';cmd.sequence++;cmd.args.count=25;
  assert.equal(applyCommand(s,meta,cmd,{now:1000}).ok,true);assert.ok(a.path.length);assert.equal(a.embarkOrder.fleet,f.id);
});
test('route badge draws one selected numeric ETA above routes, uses boarding tint, and clips to visible midpoint',()=>{
  const {s,a,f}=world();orderEmbark(s,'ashen',a.id,f.id);
  const texts=[],colors=[],positions=[];const c=new Proxy({},{get:(o,k)=>{
    if(k==='measureText')return ()=>({width:8});
    if(k==='fillText')return text=>texts.push(text);
    if(k==='stroke')return ()=>colors.push(o.strokeStyle);
    if(k==='translate')return (x,y)=>positions.push({x,y});return ()=>{};
  }});
  const map={armyId:a.id,zoom:1,width:200,height:200,x:400,y:100};
  drawOrderIndicators(map,c,knowledgeView(s,'ashen'),t=>({x:t.q*50,y:t.r*10}),()=>true);
  assert.deepEqual(texts,['2']);assert.ok(colors.includes('#8ee8ad'));assert.deepEqual(positions.at(-1),{x:400,y:100});
  texts.length=0;map.armyId=null;drawOrderIndicators(map,c,knowledgeView(s,'ashen'),t=>({x:t.q*50,y:t.r*10}),()=>true);assert.deepEqual(texts,[]);
  const normal=world(4);orderArmy(normal.s,'ashen',normal.a.id,'3,10');texts.length=0;map.armyId=normal.a.id;
  drawOrderIndicators(map,c,normal.s,t=>({x:t.q*50,y:t.r*10}),()=>true);assert.deepEqual(texts,['0']);assert.ok(colors.includes('#84ddff'));
  orderArmy(normal.s,'ashen',normal.a.id,normal.a.tile,'hold');assert.equal(indicator(normal.s,normal.a),undefined);
});
