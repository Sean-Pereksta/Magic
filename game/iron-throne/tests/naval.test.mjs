import test from 'node:test';
import assert from 'node:assert/strict';
import { createGame as legacyGame } from './fixtures/legacy-game.mjs';
import { onlineGame, activateForTest } from './fixtures/online-game.mjs';
import { createGame, parseSave, buildCheck, economyProjection, militaryArmiesOf, orderArmy, resolveMovement, sizeOf } from '../core.mjs';
import { emptyUnits, tileProduction, productionPlan } from '../economy.mjs';
import { SHIPS, cargoCount, fleetCapacity, distributeCargo, syncCargo } from '../naval-state.mjs';
import { navalGraph, navalNode, navalPath, shoreNodes, nodeTile } from '../naval-graph.mjs';
import { queueShip, shipBuildCheck, cancelShip, resolveShipConstruction, embarkArmy, orderFleet, resolveFleetMovement, mergeFleets, blockadeAt } from '../naval.mjs';
import { resolveNavalCombat, sinkShips } from '../naval-combat.mjs';
import { prepareNavalEconomy, directNavalForces } from '../naval-ai.mjs';
import { knowledgeView, refreshKnowledge } from '../fog.mjs';
import { applyCommand } from '../multiplayer-commands.mjs';
import { splitCampaign, joinCampaign } from '../multiplayer-state.mjs';
import { MAP_PROFILES } from '../map-profiles.mjs';
import { planFoundings } from '../founding.mjs';
import { fleetPanel, shipyardPanel } from '../naval-ui.mjs';
import { buildingInspection, economySummary } from '../expansion-ui.mjs';
const base=legacyGame();
function world() {
  const s=structuredClone(base);s.fog={version:1,houses:{}};
  const port=s.tiles['1,6'];Object.assign(port,{owner:'ashen',building:'town',terrain:'coast',shipyard:true,levels:{town:1,shipyard:1},river:false});
  for(const k of s.kingdoms){k.population=200;k.commands=8;for(const key of Object.keys(k.resources))k.resources[key]=2000;}
  s.armies[0].tile=port.id;s.armies[0].units={...emptyUnits(),levy:25};
  return s;
}
function fleet(s,types=['transport'],owner='ashen',tile='0,6') {
  const f={id:`fleet-${s.nextId++}`,owner,tile,node:navalNode(s,tile),ships:types.map(type=>({id:`ship-${s.nextId++}`,type,hp:SHIPS[type].hull,crew:SHIPS[type].crew,cargo:[]})),cargo:[],morale:1,path:[],target:null,order:'hold',landing:null,movementTurn:s.turn,movementSpent:0,resolvedTurn:0};s.fleets.push(f);return f;
}
const war=s=>s.wars.push('ashen:wintermere');
function cargo(s,f,n){const a=s.armies[0];a.units={...emptyUnits(),levy:n};assert.equal(embarkArmy(s,'ashen',a.id,f.id).ok,true);return a;}

for(const [type,turns] of [['warCanoe',1],['transport',1],['warship',2]])test(`${type} costs crew and resources once and launches after ${turns} turn(s)`,()=>{
  const s=world(),k=s.kingdoms[0],before=structuredClone(k);assert.equal(queueShip(s,'ashen','1,6',type).ok,true);
  assert.equal(k.population,before.population-SHIPS[type].crew);for(const [r,n]of Object.entries(SHIPS[type].cost))assert.equal(k.resources[r],before.resources[r]-n);
  resolveShipConstruction(s);resolveShipConstruction(s);assert.equal(s.fleets.length,turns===1?1:0);
  if(turns===2){s.turn++;resolveShipConstruction(s);}assert.equal(s.fleets[0].ships[0].type,type);assert.ok(navalGraph(s).has(s.fleets[0].node));assert.equal(s.shipQueues.length,0);
});
test('construction queue is serial, cancellation refunds once, and capture cannot grant free ships',()=>{
  const s=world(),before=s.kingdoms[0].population;queueShip(s,'ashen','1,6','warship');queueShip(s,'ashen','1,6','transport');
  resolveShipConstruction(s);assert.deepEqual(s.shipQueues.map(q=>q.remaining),[1,1]);const id=s.shipQueues[1].id;
  assert.equal(cancelShip(s,'ashen',id).ok,true);assert.equal(cancelShip(s,'ashen',id).ok,false);assert.equal(s.kingdoms[0].population,before-10);
  s.tiles['1,6'].owner='wintermere';s.turn++;resolveShipConstruction(s);assert.equal(s.fleets.length,0);assert.equal(s.shipQueues.length,0);
});
test('shipyard requires town/city water access; fishing docks require connected water',()=>{
  const s=world();assert.equal(buildCheck(s,'ashen','5,6','shipyard'),'Requires access to navigable ocean or river water.');
  s.tiles['1,6'].shipyard=false;delete s.tiles['1,6'].levels.shipyard;assert.equal(buildCheck(s,'ashen','1,6','shipyard'),null);
  const inland=s.tiles['6,6'];inland.building=null;inland.owner='ashen';inland.terrain='plains';assert.match(buildCheck(s,'ashen',inland.id,'fishingDock'),/navigable/);
});
test('capacity is exactly 25; 26 needs two transports; cargo exists exactly once',()=>{
  const s=world(),f=fleet(s),a=s.armies[0];a.units.levy=26;const before=JSON.stringify(s);
  assert.match(embarkArmy(s,'ashen',a.id,f.id).error,/2 Transports/);assert.equal(JSON.stringify(s),before);
  const other=fleet(s);assert.equal(mergeFleets(s,'ashen',f.id,other.id).ok,true);assert.equal(fleetCapacity(f),50);
  const accounting=economyProjection(s,'ashen').income;assert.equal(embarkArmy(s,'ashen',a.id,f.id).ok,true);
  assert.equal(s.armies.some(x=>x.id===a.id),false);assert.equal(militaryArmiesOf(s,'ashen').filter(x=>x.id===a.id).length,1);
  assert.deepEqual(f.ships.map(v=>v.cargo.reduce((n,x)=>n+x.count,0)),[25,1]);assert.deepEqual(economyProjection(s,'ashen').income,accounting);
  assert.equal(embarkArmy(s,'ashen',a.id,f.id).ok,false);assert.equal(orderArmy(s,'ashen',a.id,'1,7').ok,false);
});
test('ocean and reciprocal river navigation never changes land terrain or permits a land node',()=>{
  const s=world();for(const id of ['1,6','2,6','3,6'])Object.assign(s.tiles[id],{river:true,terrain:'plains'});
  const before=JSON.stringify(s.tiles),graph=navalGraph(s);const path=navalPath(s,'sea:0,6','river:3,6');
  assert.ok(path?.includes('river:1,6'));assert.ok(navalPath(s,'river:3,6','sea:0,6'));
  assert.equal(navalNode(s,'7,6'),null);assert.equal(navalPath(s,'sea:0,6','7,6'),null);assert.equal(JSON.stringify(s.tiles),before);
  for(const type of Object.keys(SHIPS)){const f=fleet(s,[type]);for(const id of ['1,6','2,6','3,6'])s.tiles[id].owner='ashen';assert.equal(orderFleet(s,'ashen',f.id,'3,6').ok,true);resolveFleetMovement(s,'ashen');assert.equal(f.node,'river:3,6');}
  for(const [node,edges]of graph)for(const edge of edges)assert.ok(graph.get(edge).includes(node));
});
test('ships move farther than infantry and cannot move twice in a turn',()=>{
  const s=world(),f=fleet(s,['transport']);assert.equal(orderFleet(s,'ashen',f.id,'0,20').ok,true);resolveFleetMovement(s,'ashen');assert.equal(f.tile,'0,15');
  const after=f.tile;resolveFleetMovement(s,'ashen');assert.equal(f.tile,after);assert.equal(orderFleet(s,'ashen',f.id,'0,21').ok,false);assert.equal(orderFleet(s,'ashen',f.id,'5,6').ok,false);
});
test('building beside a legacy river outlet cannot erase its navigable water position',()=>{
  const s=world(),node=[...navalGraph(s).keys()].find(n=>n.startsWith('river:')&&!s.tiles[n.slice(6)].river);
  assert.ok(node);const tile=node.slice(6),f=fleet(s,['transport'],'ashen',tile);
  Object.assign(s.tiles[tile],{building:'fishingDock',levels:{fishingDock:1},owner:'ashen'});
  assert.ok(navalGraph(s).has(f.node));assert.ok(navalPath(s,f.node,'sea:0,6'));assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));
});
test('unloading returns real troops to land while fleet stays at sea and consumes their activation',()=>{
  const s=world(),f=fleet(s),a=cargo(s,f,25);assert.equal(orderFleet(s,'ashen',f.id,'1,6','unload').ok,true);resolveFleetMovement(s,'ashen');
  assert.equal(f.tile,'0,6');assert.equal(a.tile,'1,6');assert.equal(f.cargo.length,0);assert.ok(s.armies.includes(a));assert.equal(a.embarkedFleetId,undefined);
  assert.equal(orderArmy(s,'ashen',a.id,'1,7').ok,true);resolveMovement(s,'ashen');assert.equal(a.tile,'1,6');assert.equal(orderFleet(s,'ashen',f.id,'1,6','unload').ok,false);
});
test('an amphibious landing fights defenders and captures land only through its army',()=>{
  const s=world(),f=fleet(s,['transport','transport','transport']),a=cargo(s,f,70);war(s);
  s.tiles['1,7'].owner='wintermere';s.tiles['1,7'].terrain='coast';s.tiles['1,7'].building='town';
  const defender=s.armies.find(a=>a.owner==='wintermere');defender.tile='1,7';defender.units={...emptyUnits(),levy:1};
  assert.equal(orderFleet(s,'ashen',f.id,'1,7','unload').ok,true);resolveFleetMovement(s,'ashen');
  assert.equal(s.tiles['1,7'].owner,'ashen');assert.equal(a.tile,'1,7');assert.equal(s.tiles[f.tile].terrain,'water');assert.ok(s.militaryEvents.some(e=>e.action==='battle'));
});
test('naval combat damages warships, sinks transports, and boarding needs contact',()=>{
  const s=world(),a=fleet(s,['warship']),d=fleet(s,['transport'],'wintermere','0,7');war(s);
  const hp=d.ships[0].hp;const first=resolveNavalCombat(s,a,d);assert.equal(first.ok,true);assert.ok(d.ships[0].hp<hp);assert.ok(a.ships[0].hp<150);
  for(let i=0;i<5&&s.fleets.includes(d);i++){d.node=a.node;d.tile=a.tile;resolveNavalCombat(s,a,d);}assert.equal(s.fleets.includes(d),false);
  const t=world(),af=fleet(t,['transport','warship']),df=fleet(t,['transport','warCanoe'],'wintermere','0,7');cargo(t,af,25);war(t);
  const noContact=resolveNavalCombat(t,af,df,{contact:false});assert.equal(noContact.boarding,false);assert.equal(cargoCount(af),25);
  df.node=af.node;df.tile=af.tile;
  const contact=resolveNavalCombat(t,af,df,{contact:true});assert.equal(contact.boarding,true);assert.ok(cargoCount(af)<25);
});
test('sinking losses use each transport manifest, transfer survivors only into free capacity, and never create water armies',()=>{
  const s=world(),f=fleet(s,['transport','transport','transport']);cargo(s,f,63);f.ships[0].hp=0;sinkShips(s,f);
  assert.equal(fleetCapacity(f),50);assert.equal(cargoCount(f),43);assert.equal(s.armies.some(a=>a.tile==='0,6'),false);
  f.ships.forEach(v=>v.hp=0);sinkShips(s,f);assert.equal(cargoCount(f),0);assert.equal(s.fleets.includes(f),false);
  const t=world(),full=fleet(t,['transport','transport','transport']);cargo(t,full,75);full.ships[0].hp=0;sinkShips(t,full);assert.equal(cargoCount(full),50);
});
test('fishing docks produce 14 food regardless of fertility and blockades suspend water production',()=>{
  const s=world(),t=s.tiles['1,7'];Object.assign(t,{owner:'ashen',terrain:'coast',resource:null,building:'fishingDock',levels:{fishingDock:1},quality:'exceptional'});
  assert.equal(tileProduction(t,'ashen').food,14);assert.equal(tileProduction({...t,terrain:'plains',building:'farm',levels:{farm:1}},'ashen').food,12);
  const before=productionPlan(s,'ashen').income.food,f=fleet(s,['warship'],'wintermere','0,7');war(s);f.order='blockade';
  assert.equal(blockadeAt(s,t.id,'ashen'),true);assert.equal(productionPlan(s,'ashen').income.food,before-14);
  refreshKnowledge(s);assert.match(buildingInspection(knowledgeView(s,'ashen'),t),/Naval blockade/);assert.match(economySummary(knowledgeView(s,'ashen')),/production sites are paused/);
});
test('fog hides fleets, cargo, queues and unseen changes; visible enemies only reveal vessels',()=>{
  const s=world(),f=fleet(s,['transport']);cargo(s,f,25);refreshKnowledge(s);
  assert.equal(knowledgeView(s,'wintermere').fleets.some(x=>x.id===f.id),false);
  const enemy=s.armies.find(a=>a.owner==='wintermere');enemy.tile='1,7';refreshKnowledge(s);
  const view=knowledgeView(s,'wintermere'),observed=view.fleets.find(x=>x.id===f.id);assert.equal(observed.cargoUnknown,true);assert.equal(observed.cargo,undefined);assert.deepEqual(observed.ships,[{type:'transport'}]);assert.equal(view.armies.some(a=>a.id===f.cargo[0].id),false);
  const own=knowledgeView(s,'ashen');assert.equal(cargoCount(own.fleets[0]),25);
  enemy.tile='17,4';refreshKnowledge(s);assert.equal(knowledgeView(s,'wintermere').lastSeenFleets[0].tile,'0,6');f.tile='0,8';f.node='sea:0,8';syncCargo(f);assert.equal(knowledgeView(s,'wintermere').lastSeenFleets[0].tile,'0,6');
});
test('AI constructs vessels and transports 72 troops using at least three transports into an invasion',()=>{
  const s=world();s.turn=10;s.difficulty='hard';s.tiles['1,6'].owner='wintermere';s.armies[0].owner='wintermere';s.armies[0].units.levy=72;
  const initial=s.kingdoms[1].population;prepareNavalEconomy(s,'wintermere');assert.equal(s.shipQueues[0].type,'warCanoe');assert.equal(s.kingdoms[1].population,initial-4);
  for(let i=0;i<5;i++){resolveShipConstruction(s);s.turn++;s.kingdoms[1].commands=8;prepareNavalEconomy(s,'wintermere');}
  assert.ok(s.fleets[0].ships.filter(v=>v.type==='transport').length>=3);
  Object.assign(s.tiles['1,8'],{owner:'ashen',building:'town',terrain:'coast'});war(s);refreshKnowledge(s);directNavalForces(s,'wintermere');
  const f=s.fleets[0];assert.equal(cargoCount(f),72);assert.ok(fleetCapacity(f)>=75);assert.equal(f.order,'unload');
  for(let i=0;i<5&&cargoCount(f);i++){resolveFleetMovement(s,'wintermere');s.turn++;}
  assert.equal(cargoCount(f),0);assert.ok(s.armies.some(a=>a.owner==='wintermere'&&sizeOf(a)>=72&&a.tile!=='1,6'));
});
test('new worlds have four playable exterior water rows and separated realms have viable starts',()=>{
  for(const profile of ['broken-coast','shattered-realms']){const s=createGame(42,profile);assert.ok(planFoundings(s));
    for(const t of Object.values(s.tiles))if(t.q<4||t.r<4||t.q>=s.width-4||t.r>=s.height-4)assert.equal(t.terrain,'water');
    assert.ok(navalPath(s,'sea:0,0',`sea:${s.width-1},${s.height-1}`));assert.ok(MAP_PROFILES[s.mapProfile].fragmented);
  }
});
test('save roundtrip preserves damage, cargo, construction and orders; old saves migrate',()=>{
  const s=world(),f=fleet(s,['transport','transport']);cargo(s,f,47);queueShip(s,'ashen','1,6','warship');f.ships[0].hp=40;orderFleet(s,'ashen',f.id,'0,18');refreshKnowledge(s);
  const loaded=parseSave(JSON.stringify(s));assert.deepEqual(loaded.fleets,s.fleets);assert.deepEqual(loaded.shipQueues,s.shipQueues);
  const old=world();delete old.fleets;delete old.shipQueues;delete old.navalVersion;assert.deepEqual(parseSave(JSON.stringify(old)).fleets,[]);
  for(const corrupt of [x=>x.fleets[0].node='sea:5,6',x=>x.fleets[0].cargo.push(x.fleets[0].cargo[0]),x=>x.fleets[0].cargo[0].units.levy=51,x=>x.fleets[0].ships[0].cargo[0].count=26,x=>x.fleets[0].path=['river:5,6'],x=>x.armies.push(x.fleets[0].cargo[0])]){const copy=structuredClone(s);corrupt(copy);assert.throws(()=>parseSave(JSON.stringify(copy)),/naval|army/i);}
});
test('multiplayer validates active owner, exact version and command replay for naval actions',()=>{
  const {state:s,meta:m}=onlineGame();activateForTest(s,m,'ashen');
  const port=Object.values(s.tiles).find(t=>t.owner==='ashen'&&t.building==='city');port.shipyard=true;port.levels.shipyard=1;port.river=true;
  const k=s.kingdoms[0];k.population=200;k.resources.wood=1000;
  const command={id:'naval-command',clientId:'naval-tests-001',sequence:10,uid:'u0',actorHouseId:'ashen',turn:s.turn,stateVersion:m.stateVersion,epoch:m.epoch,activationId:m.activationId,type:'shipBuild',args:{tile:port.id,ship:'transport'}};
  assert.equal(applyCommand(s,m,{...command,stateVersion:m.stateVersion-1}).ok,false);
  assert.equal(applyCommand(s,m,{...command,uid:'u1'}).ok,false);
  assert.equal(applyCommand(s,m,command).ok,true);assert.equal(applyCommand(s,m,command).ok,false);assert.equal(s.shipQueues.length,1);
  const split=splitCampaign(s),joined=joinCampaign(split.canonical,split.privateByHouse);assert.deepEqual(joined.shipQueues,s.shipQueues);assert.equal(split.world.shipQueues.length,0);
});
test('fleet and shipyard UI expose capacity, queue cost, blocked reasons and private cargo correctly',()=>{
  const s=world(),f=fleet(s);s.armies[0].units.levy=26;refreshKnowledge(s);const view=knowledgeView(s,'ashen');
  assert.match(fleetPanel(view,'0,6','ashen'),/2 Transports required/);assert.match(fleetPanel(view,'0,6','ashen'),/Current capacity: 25/);
  queueShip(s,'ashen','1,6','warship');assert.match(shipyardPanel(s,'1,6','ashen'),/2 turns until launch/);assert.match(shipyardPanel(s,'1,6','ashen'),/10 population/);
});
test('sinking survivors can be rescued by another friendly stack at the same naval location',()=>{
  const s=world(),f=fleet(s),rescue=fleet(s);cargo(s,f,25);f.ships[0].hp=0;sinkShips(s,f);
  assert.equal(cargoCount(rescue),5);assert.equal(s.fleets.includes(f),false);assert.equal(rescue.cargo[0].embarkedFleetId,rescue.id);
  assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));
});
test('failed amphibious landings retain surviving cargo and cannot duplicate or walk it onto land',()=>{
  const s=world(),f=fleet(s,['transport','transport']),a=cargo(s,f,30);war(s);
  const target=s.tiles['1,7'];Object.assign(target,{owner:'wintermere',building:'town',terrain:'coast'});
  const defender=s.armies.find(a=>a.owner==='wintermere');defender.tile=target.id;defender.units={...emptyUnits(),heavyInfantry:25};
  orderFleet(s,'ashen',f.id,target.id,'unload');resolveFleetMovement(s,'ashen');
  assert.equal(target.owner,'wintermere');assert.equal(s.armies.some(x=>x.id===a.id),false);
  assert.ok(f.cargo.every(a=>a.order==='embarked'&&a.tile===f.tile));assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));
});
test('naval combat refuses contact across disconnected waterways or distant tiles',()=>{
  const s=world(),a=fleet(s,['warship']),d=fleet(s,['transport'],'wintermere','0,20');war(s);const before=JSON.stringify(s);
  assert.equal(resolveNavalCombat(s,a,d).ok,false);assert.equal(JSON.stringify(s),before);
});
test('a second defender contests a landing even on the invader’s own shore',()=>{
  const s=world(),f=fleet(s,['transport','transport','transport']),a=cargo(s,f,70);war(s);
  const defenders=s.armies.filter(a=>a.owner!=='ashen').slice(0,2);
  for(const d of defenders){d.owner='wintermere';d.tile='1,6';d.units={...emptyUnits(),levy:1};}
  orderFleet(s,'ashen',f.id,'1,6','unload');resolveFleetMovement(s,'ashen');
  assert.ok(s.armies.some(d=>d.owner==='wintermere'&&d.tile==='1,6'));assert.ok(f.cargo.includes(a));assert.equal(s.armies.includes(a),false);
});
test('AI escorts a loaded convoy and waits for protection before launching transports',()=>{
  const s=world(),transport=fleet(s),guard=fleet(s,['warship'],'ashen','0,7');cargo(s,transport,25);war(s);refreshKnowledge(s);
  directNavalForces(s,'ashen');assert.equal(guard.order,'escort');assert.equal(guard.escort,transport.id);
  const t=world(),empty=fleet(t);war(t);refreshKnowledge(t);directNavalForces(t,'ashen');assert.equal(cargoCount(empty),0);
});
test('interception resolves along water edges and does not move the fleet twice',()=>{
  const s=world(),a=fleet(s,['warship']),d=fleet(s,['warCanoe'],'wintermere','0,8');war(s);d.order='intercept';
  orderFleet(s,'ashen',a.id,'0,12');resolveFleetMovement(s,'ashen');assert.equal(s.militaryEvents.filter(e=>e.action==='naval').length,1);
  assert.equal(a.node,'sea:0,7');assert.equal(a.path[0],'sea:0,8');assert.doesNotThrow(()=>parseSave(JSON.stringify(s)));
  assert.equal(a.movementSpent,1);const tile=a.tile;resolveFleetMovement(s,'ashen');assert.equal(a.tile,tile);
});
test('multiplayer embark and unload reject stale commands and preserve one army across private snapshots',()=>{
  const {state:s,meta:m}=onlineGame();activateForTest(s,m,'ashen');const a=s.armies.find(a=>a.owner==='ashen'),port=s.tiles[a.tile];port.river=true;a.units={...emptyUnits(),levy:26};
  const f=fleet(s,['transport','transport'],'ashen',port.id);refreshKnowledge(s);
  const command={id:'embark-01',clientId:'naval-multi-001',sequence:10,uid:'u0',actorHouseId:'ashen',turn:s.turn,stateVersion:m.stateVersion,epoch:m.epoch,activationId:m.activationId,type:'fleetEmbark',args:{army:a.id,fleet:f.id}};
  assert.equal(applyCommand(s,m,command).ok,true);m.stateVersion++;
  assert.equal(applyCommand(s,m,command).ok,false);
  assert.equal(applyCommand(s,m,{...command,id:'duplicate-02',stateVersion:m.stateVersion,sequence:11}).ok,false);
  const split=splitCampaign(s),joined=joinCampaign(split.canonical,split.privateByHouse);assert.equal(joined.fleets[0].cargo[0].id,a.id);assert.equal(joined.armies.some(x=>x.id===a.id),false);assert.equal(split.world.fleets.length,0);
  const unload={...command,id:'unload-03',type:'fleetOrder',stateVersion:m.stateVersion,sequence:12,args:{fleet:f.id,tile:port.id,order:'unload'}};
  assert.equal(applyCommand(s,m,unload).ok,true);resolveFleetMovement(s,'ashen');assert.equal(s.armies.filter(x=>x.id===a.id).length,1);resolveFleetMovement(s,'ashen');assert.equal(s.armies.filter(x=>x.id===a.id).length,1);assert.equal(f.cargo.length,0);
});
