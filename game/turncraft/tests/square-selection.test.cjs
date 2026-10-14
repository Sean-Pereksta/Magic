const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

function setup(faction='frontier'){
 const run=simulation(faction);
 run(`cam.x=53;cam.y=97;cam.zoom=1;W=1600;H=1200;globalThis.squareCommands=[];sendCommand=c=>{squareCommands.push(c);applyCommand({...c,slot:PLAYER})}`);
 return run;
}

for(const faction of ['frontier','ascendant','brood'])test(`${faction} workers select across their whole occupied square after another group was selected`,()=>{
 const run=setup(faction),r=run.plain(`(()=>{
  const worker=addEntity('unit',FACTIONS[players[0].factionId].worker,0,8,8),group=[addEntity('unit','rifleman',0,2,2),addEntity('unit','rifleman',0,3,2)],results=[];
  for(const zoom of [.55,1,1.8])for(const [x,y] of [[.01,.01],[.99,.01],[.01,.99],[.99,.99],[.5,.01],[.99,.5],[.5,.99],[.01,.5]]){
   cam.zoom=zoom;selectEntities(group);lastCanvasTap={at:0,entityId:null};lastStackTap={key:'',idx:0,at:0};testClock+=1000;
   const p=worldToScreen(worker.x+x,worker.y+y);handlePolishTap(p.x,p.y);results.push({selected:selectedIds.slice(),commands:squareCommands.length,zoom,x,y});
  }
  return {results,worker:worker.id,destinations:group.map(e=>e.moveDestination)};
 })()`);
 for(const click of r.results){assert.deepEqual(click.selected,[r.worker],JSON.stringify(click));assert.equal(click.commands,0,JSON.stringify(click))}
 assert.deepEqual(r.destinations,[null,null]);
});

test('a moving troop selects from both its logical occupied square and its full painted square',()=>{
 const run=setup(),r=run.plain(`(()=>{
  const mover=addEntity('unit','rifleman',0,12,8),group=[addEntity('unit','rifleman',0,2,2),addEntity('unit','rifleman',0,3,2)];mover.px=8.2;mover.py=8.3;const chosen=[];
  for(const [x,y] of [[12.01,8.01],[12.99,8.99],[8.21,8.31],[9.19,9.29]]){
   selectEntities(group);lastCanvasTap={at:0,entityId:null};testClock+=1000;const p=worldToScreen(x,y);handlePolishTap(p.x,p.y);chosen.push(selectedId);
  }
  return {chosen,mover:mover.id,commands:squareCommands.length};
 })()`);
 assert.deepEqual(r.chosen,Array(4).fill(r.mover));assert.equal(r.commands,0);
});

test('the troop logically occupying a square wins over a moving neighbor painted across its corner',()=>{
 const run=setup(),r=run.plain(`(()=>{
  const moving=addEntity('unit','rifleman',0,12,8),occupant=addEntity('unit','engineer',0,8,8),selected=addEntity('unit','rifleman',0,2,2);moving.px=8.2;moving.py=8.2;selectEntity(selected);
  const p=worldToScreen(8.99,8.99);handlePolishTap(p.x,p.y);return {selected:selectedId,expected:occupant.id,commands:squareCommands.length};
 })()`);
 assert.equal(r.selected,r.expected);assert.equal(r.commands,0);
});

test('ordinary clicks select an owned worker on minerals while armed Mine still orders the selected workers',()=>{
 const run=setup(),r=run.plain(`(()=>{
  addEntity('structure',FACTIONS.frontier.base,0,10,10);addNode('mineral',13,10,1800);const node=resourceNodes.at(-1),incumbent=addEntity('unit','engineer',0,13,10),group=[addEntity('unit','engineer',0,6,9),addEntity('unit','engineer',0,6,10)];
  selectEntities(group);const p=worldToScreen(13.99,10.99);handlePolishTap(p.x,p.y);const ordinary={selected:selectedIds.slice(),commands:squareCommands.length};
  selectEntities(group);interactionMode='mine';handlePolishTap(p.x,p.y);return {ordinary,incumbent:incumbent.id,command:squareCommands.at(-1),node:node.id,selected:selectedIds,group:group.map(e=>e.id),mode:interactionMode};
 })()`);
 assert.deepEqual(r.ordinary.selected,[r.incumbent]);assert.equal(r.ordinary.commands,0);
 assert.equal(r.command.type,'groupMine');assert.equal(r.command.nodeId,r.node);assert.deepEqual(r.selected,r.group);assert.equal(r.mode,null);
});

test('hidden, dead and transported troops are never picked by square coverage',()=>{
 const run=setup(),r=run.plain(`(()=>{
  const hidden=addEntity('unit','rifleman',0,8,8),dead=addEntity('unit','rifleman',0,10,8),transported=addEntity('unit','rifleman',0,12,8);dead.alive=false;transported.garrisonedIn=123;tileVisibleToFaction=(f,x,y)=>x!==8||y!==8;
  return [hidden,dead,transported].map(e=>{const p=worldToScreen(e.x+.99,e.y+.99);return selectableEntityAtScreen(p.x,p.y)?.id||null});
 })()`);
 assert.deepEqual(r,[null,null,null]);
});

test('square corner double-taps select the visible type without issuing a movement order',()=>{
 const run=setup(),r=run.plain(`(()=>{
  const troop=addEntity('unit','rifleman',0,8,8),same=addEntity('unit','rifleman',0,9,9),other=addEntity('unit','engineer',0,2,2);selectEntity(other);const p=worldToScreen(8.99,8.99);handlePolishTap(p.x,p.y);const first=selectedId;testClock+=100;handlePolishTap(p.x,p.y);return {first,ids:selectedIds,expected:[troop.id,same.id],commands:squareCommands.length};
 })()`);
 assert.equal(r.first,r.expected[0]);assert.deepEqual(r.ids,r.expected);assert.equal(r.commands,0);
});

test('stacked air and ground troops remain distinguishable at their displayed body centers',()=>{
 const run=setup(),r=run.plain(`(()=>{
  const ground=addEntity('unit','rifleman',0,8,8),air=addEntity('unit','wraith',0,8,8),other=addEntity('unit','engineer',0,2,2),chosen=[];
  for(const e of [ground,air]){selectEntity(other);lastCanvasTap={at:0,entityId:null};lastStackTap={key:'',idx:0,at:0};testClock+=1000;const p=entityScreenAnchor(e);handlePolishTap(p.x,p.y);chosen.push(selectedId)}
  return {chosen,expected:[ground.id,air.id],commands:squareCommands.length};
 })()`);
 assert.deepEqual(r.chosen,r.expected);assert.equal(r.commands,0);
});

test('explicit Move owns an occupied friendly square while an invalid armed Attack never becomes selection',()=>{
 const run=setup(),r=run.plain(`(()=>{
  const own=addEntity('unit','rifleman',0,2,2),friend=addEntity('unit','engineer',0,8,8);selectEntity(own);const p=worldToScreen(8.99,8.99);interactionMode='move';handlePolishTap(p.x,p.y);const move=squareCommands.at(-1),afterMove=selectedId;
  interactionMode='attack';const center=entityScreenAnchor(friend);handlePolishTap(center.x,center.y);return {move,afterMove,selected:selectedId,own:own.id,commands:squareCommands.length,mode:interactionMode};
 })()`);
 assert.equal(r.move.type,'move');assert.equal(r.move.x,8);assert.equal(r.move.y,8);assert.equal(r.afterMove,r.own);assert.equal(r.selected,r.own);assert.equal(r.commands,1);assert.equal(r.mode,'attack');
});

test('a captured ordinary tap selects its originally pressed troop after that troop moves away',()=>{
 const run=setup(),r=run.plain(`(()=>{
  const target=addEntity('unit','rifleman',0,8,8),group=[addEntity('unit','engineer',0,2,2),addEntity('unit','engineer',0,3,2)];selectEntities(group);const p=worldToScreen(8.99,8.99),capturedId=target.id;
  target.x=target.px=12;target.y=target.py=12;handlePolishTap(p.x,p.y,capturedId);return {selected:selectedIds,expected:[target.id],commands:squareCommands.length,destinations:group.map(e=>e.moveDestination)};
 })()`);
 assert.deepEqual(r.selected,r.expected);assert.equal(r.commands,0);assert.deepEqual(r.destinations,[null,null]);
});

test('a captured troop that dies, boards transport or becomes hidden consumes the tap without moving the selected group',()=>{
 for(const state of ['dead','transported','hidden']){
  const run=setup(),r=run.plain(`(()=>{
   const target=addEntity('unit','rifleman',0,8,8),group=[addEntity('unit','engineer',0,2,2),addEntity('unit','engineer',0,3,2)];selectEntities(group);const p=worldToScreen(8.99,8.99),capturedId=target.id;
   if('${state}'==='dead')target.alive=false;else if('${state}'==='transported')target.garrisonedIn=123;else tileVisibleToFaction=(f,x,y)=>x!==8||y!==8;
   handlePolishTap(p.x,p.y,capturedId);return {selected:selectedIds,expected:group.map(e=>e.id),commands:squareCommands.length,destinations:group.map(e=>e.moveDestination)};
  })()`);
  assert.deepEqual(r.selected,r.expected,state);assert.equal(r.commands,0,state);assert.deepEqual(r.destinations,[null,null],state);
 }
});

test('capturing the nearest pressed troop preserves deliberate cycling through its stationary stack',()=>{
 const run=setup(),r=run.plain(`(()=>{
  const first=addEntity('unit','engineer',0,8,8),second=addEntity('unit','rifleman',0,8,8),p=worldToScreen(8.5,8.5);
  handlePolishTap(p.x,p.y,first.id);const selectedFirst=selectedId;testClock+=400;handlePolishTap(p.x,p.y,first.id);return {selectedFirst,selectedSecond:selectedId,first:first.id,second:second.id,commands:squareCommands.length};
 })()`);
 assert.equal(r.selectedFirst,r.first);assert.equal(r.selectedSecond,r.second);assert.equal(r.commands,0);
});
