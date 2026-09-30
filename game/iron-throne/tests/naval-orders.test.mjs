import { armyShipClick } from '../ship-click.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame } from './fixtures/legacy-game.mjs';
import { emptyUnits } from '../economy.mjs';
import { parseSave, orderArmy, resolveMovement, distance } from '../core.mjs';
import { SHIPS, cargoCount, fleetCapacity, fleetSpeed } from '../naval-state.mjs';
import { orderEmbark, embarkArmy, resolveEmbarkOrders, orderFleet, resolveFleetMovement, mergeFleets } from '../naval.mjs';
import { resolveNavalCombat } from '../naval-combat.mjs';
import { landNavalRange } from '../naval-ranged.mjs';
import { refreshKnowledge, knowledgeView } from '../fog.mjs';
import { mapOrders, drawOrderIndicators } from '../map-orders.mjs';
import { fleetPanel } from '../naval-ui.mjs';
const base=createGame();
function world(){const s=structuredClone(base);s.fog={version:1,houses:{}};s.wars.push('ashen:wintermere');s.armies[0].tile='1,6';s.armies[0].units={...emptyUnits(),levy:70};s.tiles['1,6'].owner='ashen';return s;}
function fleet(s,types=['transport'],owner='ashen',tile='0,6'){
  const f={id:`fleet-${s.nextId++}`,owner,tile,node:`sea:${tile}`,ships:types.map(type=>({id:`ship-${s.nextId++}`,type,hp:SHIPS[type].hull,crew:SHIPS[type].crew,cargo:[]})),cargo:[],morale:1,path:[],target:null,order:'hold',landing:null,movementTurn:s.turn,movementSpent:0,resolvedTurn:0};s.fleets.push(f);refreshKnowledge(s);return f;
}
test('boarding 70 into one transport queues 25 and leaves exactly 45 ashore after resolution',()=>{
  const s=world(),a=s.armies[0],f=fleet(s),before=JSON.stringify(a.units);
  assert.deepEqual(orderEmbark(s,'ashen',a.id,f.id),{ok:true,count:25,remaining:45});
  assert.equal(JSON.stringify(a.units),before);assert.equal(cargoCount(f),0);assert.equal(mapOrders(knowledgeView(s,'ashen'))[0].kind,'load');
  assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));resolveFleetMovement(s,'ashen');
  assert.equal(a.units.levy,45);assert.equal(cargoCount(f),25);assert.notEqual(f.cargo[0].id,a.id);
  resolveFleetMovement(s,'ashen');assert.equal(a.units.levy,45);assert.equal(cargoCount(f),25);
  assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));
});
test('three colocated transports combine for 75 seats, board all 70, and preserve the sailing order',()=>{
  const s=world(),a=s.armies[0],f=fleet(s);fleet(s);fleet(s);
  assert.match(fleetPanel(knowledgeView(s,'ashen'),f.tile,'ashen'),/0\/75 troops aboard/);
  assert.equal(orderFleet(s,'ashen',f.id,'0,15').ok,true);
  assert.equal(orderEmbark(s,'ashen',a.id,f.id).count,70);assert.equal(s.fleets.length,1);assert.equal(fleetCapacity(f),75);assert.equal(f.order,'move');
  resolveFleetMovement(s,'ashen');assert.equal(cargoCount(f),70);assert.equal(s.armies.includes(a),false);assert.notEqual(f.tile,'0,6');
  assert.deepEqual(f.ships.map(v=>v.cargo.reduce((n,x)=>n+x.count,0)),[25,25,20]);assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));
});
test('boarding reservations cannot overbook, repeat, or survive an explicit cancel',()=>{
  const s=world(),a=s.armies[0],f=fleet(s),b={...a,id:`army-${s.nextId++}`,units:{...emptyUnits(),archer:20},path:[]};s.armies.push(b);
  assert.equal(orderEmbark(s,'ashen',a.id,f.id).ok,true);assert.equal(orderEmbark(s,'ashen',a.id,f.id).ok,false);assert.equal(orderEmbark(s,'ashen',b.id,f.id).ok,false);
  assert.equal(orderArmy(s,'ashen',a.id,a.tile,'hold').ok,true);assert.equal(orderEmbark(s,'ashen',b.id,f.id).count,20);
  resolveEmbarkOrders(s,'ashen');assert.equal(cargoCount(f),20);assert.equal(a.units.levy,70);
});
test('moved transport retains its boarding target and repaths the army',()=>{
  const s=world(),a=s.armies[0],f=fleet(s);orderEmbark(s,'ashen',a.id,f.id);f.tile='0,20';f.node='sea:0,20';resolveEmbarkOrders(s,'ashen');
  assert.equal(cargoCount(f),0);assert.equal(a.units.levy,70);assert.equal(a.embarkOrder.fleet,f.id);assert.ok(a.path.length);
});
test('sailing averages exactly 2.5 times standard infantry with fractional distance carried forward',()=>{
  const s=world(),f=fleet(s);assert.equal(fleetSpeed(f),7.5);orderFleet(s,'ashen',f.id,'0,29');resolveFleetMovement(s,'ashen');
  assert.equal(distance(s.tiles['0,6'],s.tiles[f.tile]),7);s.turn++;resolveFleetMovement(s,'ashen');assert.equal(distance(s.tiles['0,6'],s.tiles[f.tile]),15);
  assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));
});
test('warships fire from two hexes without moving or taking out-of-range transport return fire',()=>{
  const s=world(),f=fleet(s,['warship']),e=fleet(s,['transport'],'wintermere','0,8'),hp=f.ships[0].hp;
  assert.equal(orderFleet(s,'ashen',f.id,e.tile,'attack').ok,true);assert.deepEqual(f.path,[]);
  assert.ok(mapOrders(knowledgeView(s,'ashen')).some(o=>o.kind==='attack'&&o.turns===0));
  resolveFleetMovement(s,'ashen');assert.equal(f.tile,'0,6');assert.equal(f.ships[0].hp,hp);assert.ok(e.ships[0].hp<70);
  const after=e.ships[0].hp;resolveFleetMovement(s,'ashen');assert.equal(e.ships[0].hp,after);assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));
  const t=world(),transport=fleet(t),target=fleet(t,['transport'],'wintermere','0,8');assert.equal(resolveNavalCombat(t,transport,target).ok,false);
});
test('transports approach to adjacent water while warships can sink fully loaded enemy transports at range',()=>{
  const s=world(),f=fleet(s),e=fleet(s,['transport'],'wintermere','0,8');orderFleet(s,'ashen',f.id,e.tile,'attack');assert.equal(f.path.length,1);resolveFleetMovement(s,'ashen');assert.equal(s.militaryEvents.at(-1).action,'naval');
  const t=world(),loaded=fleet(t,['transport','transport','transport']);embarkArmy(t,'ashen',t.armies[0].id,loaded.id);const warships=fleet(t,Array(6).fill('warship'),'wintermere','0,8');
  const result=resolveNavalCombat(t,warships,loaded,{contact:false});assert.equal(result.ok,true);assert.equal(cargoCount(loaded),0);assert.equal(result.troopLosses[1],70);assert.equal(t.armies.some(a=>a.owner==='ashen'),false);
});
test('warships bombard land armies and empty structures from two hexes without landing or capturing',()=>{
  for(const structure of [false,true]){
    const s=world(),f=fleet(s,['warship']),t=s.tiles['2,6'];Object.assign(t,{owner:'wintermere',terrain:'plains',building:structure?'farm':null,levels:structure?{farm:1}:{}});
    const a=s.armies.find(a=>a.owner==='wintermere');a.tile=structure?'17,4':t.id;a.units={...emptyUnits(),levy:20};refreshKnowledge(s);
    assert.equal(orderFleet(s,'ashen',f.id,t.id,'attack').ok,true);resolveFleetMovement(s,'ashen');
    assert.equal(f.tile,'0,6');assert.equal(t.owner,'wintermere');if(structure)assert.ok(t.structureDamage.farm>0);else assert.ok(a.units.levy<20);
    assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));
  }
});
test('all ranged land types can fire on ships; melee troops and battering rams cannot',()=>{
  for(const type of ['archer','veteranArcher','crossbow','catapult','trebuchet','siege']){
    const s=world(),a=s.armies[0],e=fleet(s,['warship'],'wintermere');a.tile=type==='trebuchet'?'2,7':'1,7';a.units={...emptyUnits(),[type]:2,scout:1};refreshKnowledge(s);
    assert.equal(landNavalRange(a),type==='trebuchet'?3:2);assert.equal(orderArmy(s,'ashen',a.id,e.tile,'ranged').ok,true);
    assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));resolveMovement(s,'ashen');assert.ok(e.ships[0].hp<150);assert.equal(a.tile,type==='trebuchet'?'2,7':'1,7');
    const hp=e.ships[0].hp;resolveMovement(s,'ashen');assert.equal(e.ships[0].hp,hp);assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));
  }
  for(const type of ['levy','ram']){const s=world(),a=s.armies[0],e=fleet(s,['transport'],'wintermere');a.units={...emptyUnits(),[type]:20};refreshKnowledge(s);assert.equal(orderArmy(s,'ashen',a.id,e.tile,'ranged').ok,false);}
  const river=world();river.tiles['1,6'].river=true;river.armies[0].tile='5,6';const ship=fleet(river,['warship'],'ashen','1,6');ship.node='river:1,6';const ram=river.armies.find(a=>a.owner==='wintermere');ram.tile='1,6';ram.units={...emptyUnits(),ram:20};refreshKnowledge(river);assert.equal(orderFleet(river,'ashen',ship.id,'1,6','attack').ok,true);resolveFleetMovement(river,'ashen');assert.equal(ship.ships[0].hp,150);
});
test('ranged targeting respects fog, range, war and mountains, and cancels when targets leave',()=>{
  const s=world(),a=s.armies[0],e=fleet(s,['transport'],'wintermere','0,6');a.units={...emptyUnits(),archer:20};a.tile='2,6';s.tiles['1,6'].terrain='mountain';refreshKnowledge(s);
  assert.equal(orderArmy(s,'ashen',a.id,e.tile,'ranged').ok,false);s.tiles['1,6'].terrain='plains';refreshKnowledge(s);
  assert.equal(orderArmy(s,'ashen',a.id,e.tile,'ranged').ok,true);e.tile='0,20';e.node='sea:0,20';refreshKnowledge(s);resolveMovement(s,'ashen');assert.equal(e.ships[0].hp,70);assert.equal(a.order,'hold');
  assert.equal(orderArmy(s,'ashen',a.id,e.tile,'ranged').ok,false);
});
test('orders stay visible without selection, distinguish loading and unloading, and hide enemy plans',()=>{
  const s=world(),a=s.armies[0],f=fleet(s);orderEmbark(s,'ashen',a.id,f.id);orderFleet(s,'ashen',f.id,'0,15');
  const view=knowledgeView(s,'ashen'),orders=mapOrders(view);assert.ok(orders.some(o=>o.kind==='sail'&&o.turns===1));assert.ok(orders.some(o=>o.kind==='load'));
  const calls=[],c=new Proxy({},{get:(o,k)=>k==='measureText'?()=>({width:80}):k==='fillText'?(text)=>calls.push(text):()=>{}});
  drawOrderIndicators({zoom:1,selected:'17,4'},c,view,t=>({x:t.q*40,y:t.r*40}),()=>true);assert.deepEqual(calls,[],'Map routes must not draw text banners');
  assert.equal(mapOrders(knowledgeView(s,'wintermere')).length,0);
  resolveEmbarkOrders(s,'ashen');orderFleet(s,'ashen',f.id,'1,6','unload');assert.ok(mapOrders(knowledgeView(s,'ashen')).some(o=>o.kind==='unload'&&o.turns===0));
  const t=world();t.armies[0].path=['1,7'];t.armies[0].target='1,7';t.armies[0].order='attack';assert.ok(mapOrders(t).some(o=>o.kind==='attack'&&o.turns===0));
});

test('ship clicks infer ranged fire or March boarding using visible ships only',()=>{
  const s=world(),a=s.armies[0],friendly=fleet(s);a.units={...emptyUnits(),archer:70};const enemy=fleet(s,['warship'],'wintermere','0,8');refreshKnowledge(s);
  let view=knowledgeView(s,'ashen');
  assert.deepEqual(armyShipClick(view,'ashen',a.id,enemy.tile),{type:'order',args:{army:a.id,tile:enemy.tile,order:'ranged'}});
  assert.equal(armyShipClick(view,'ashen',a.id,friendly.tile),null);
  assert.deepEqual(armyShipClick(view,'ashen',a.id,friendly.tile,'move'),{type:'fleetEmbark',args:{army:a.id,fleet:friendly.id}});
  assert.equal(armyShipClick(view,'ashen',a.id,'5,6','move'),null);
  enemy.tile='0,25';enemy.node='sea:0,25';refreshKnowledge(s);view=knowledgeView(s,'ashen');assert.equal(armyShipClick(view,'ashen',a.id,enemy.tile),null);
});
