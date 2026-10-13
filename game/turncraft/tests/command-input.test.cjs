const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');
function setup(){const run=simulation();run(`screenToTile=(x,y)=>({x,y});globalThis.testCommands=[];globalThis.testOriginalSendCommand=sendCommand;sendCommand=c=>{testCommands.push(c);applyCommand({...c,slot:PLAYER})};worldToScreen=(x,y)=>({x:x*30,y:y*30});selectableEntityAtScreen=(x,y,cycle=false)=>selectableEntityAt(x,y,cycle);entityScreenHitScore=(e,x,y)=>e.x===x&&e.y===y?0:Infinity;W=1500;H=1500`);return run}

test('post-match spectating selects units and structures without issuing contextual or armed orders',()=>{
 const run=setup(),r=run.plain(`(()=>{const own=addEntity('unit','rifleman',0,2,2),enemy=addEntity('unit','zealot',1,8,8),building=addEntity('structure','barracks',0,4,4);selectEntity(own);gameOver=true;spectating=false;handlePolishTap(8,8);const closed=selectedId;spectating=true;interactionMode='attack';handlePolishTap(8,8);const inspectedEnemy=selectedId;interactionMode='build';interactionPayload='barracks';legalTiles=[{x:4,y:4}];handlePolishTap(4,4);const inspectedBuilding=selectedId;handlePolishTap(2,2);const inspectedOwn=selectedId;for(const mode of ['mine','rally','assistBuild','attackMove']){interactionMode=mode;handlePolishTap(2,2)}applyCommand({type:'move',entityId:own.id,x:12,y:12,slot:PLAYER});applyCommand({type:'attackMove',entityId:own.id,x:12,y:12,slot:PLAYER});const destination=own.moveDestination,sequence=clientSequence;testOriginalSendCommand({type:'move',entityId:own.id,x:12,y:12});const sentAfterEnd=clientSequence-sequence;handlePolishTap(10,10);return {closed,own:own.id,inspectedEnemy,enemy:enemy.id,inspectedBuilding,building:building.id,inspectedOwn,empty:selectedId,mode:interactionMode,commands:testCommands.length,destination,sentAfterEnd}})()`);
 assert.equal(r.closed,r.own);assert.equal(r.inspectedEnemy,r.enemy);assert.equal(r.inspectedBuilding,r.building);assert.equal(r.inspectedOwn,r.own);assert.equal(r.empty,null);assert.equal(r.mode,null);assert.equal(r.commands,0);assert.equal(r.destination,null);assert.equal(r.sentAfterEnd,0);
});

test('Attack ground selects the nearest visible legal enemy within four tiles and preserves selection',()=>{
 const run=setup(),r=run.plain(`(()=>{const u=addEntity('unit','engineer',0,2,2),friend=addEntity('unit','rifleman',0,9,10),hidden=addEntity('unit','zealot',1,10,11),air=addEntity('unit','phoenix',1,10,9),near=addEntity('unit','zealot',1,11,10);addEntity('unit','zealot',1,13,10);tileVisibleToFaction=(f,x,y)=>x!==hidden.x||y!==hidden.y;selectEntity(u);interactionMode='attack';handlePolishTap(10,10);return {target:u.targetId,expected:near.id,ids:selectedIds,mode:interactionMode,command:testCommands[0]}})()`);
 assert.equal(r.target,r.expected);assert.deepEqual(r.ids,[r.command.entityId]);assert.equal(r.mode,null);assert.equal(r.command.type,'attack');
});

test('empty Attack ground uses formation attack-move without seeing hidden or invalid air enemies',()=>{
 const run=setup(),r=run.plain(`(()=>{const units=[addEntity('unit','engineer',0,2,2),addEntity('unit','engineer',0,3,2)],hidden=addEntity('unit','zealot',1,10,11);addEntity('unit','phoenix',1,10,9);addEntity('unit','zealot',1,15,10);tileVisibleToFaction=(f,x,y)=>x!==hidden.x||y!==hidden.y;selectEntities(units);interactionMode='attack';handlePolishTap(10,10);return {command:testCommands[0],dest:units.map(e=>e.attackMoveDestination),distinct:new Set(units.map(e=>e.moveDestination.x+','+e.moveDestination.y)).size,selected:selectedIds,mode:interactionMode}})()`);
 assert.equal(r.command.type,'groupAttackMove');assert.equal(r.command.x,10);assert.equal(r.command.y,10);assert.ok(r.dest.every(Boolean));assert.equal(r.distinct,2);assert.deepEqual(r.selected,r.command.entityIds);assert.equal(r.mode,null);
});

test('exact enemy clicks never redirect invalid air or friendly targets into another attack or movement',()=>{
 const run=setup(),r=run.plain(`(()=>{const u=addEntity('unit','engineer',0,2,2);addEntity('unit','zealot',1,10,10);const specific=addEntity('unit','zealot',1,13,10);addEntity('unit','phoenix',1,11,10);addEntity('unit','rifleman',0,12,10);selectEntity(u);interactionMode='attack';handlePolishTap(13,10);const chosen=u.targetId;interactionMode='attack';handlePolishTap(11,10);handlePolishTap(12,10);return {chosen,expected:specific.id,commands:testCommands.length,mode:interactionMode,selected:selectedIds}})()`);
 assert.equal(r.chosen,r.expected);assert.equal(r.commands,1);assert.equal(r.mode,'attack');assert.deepEqual(r.selected,[1]);
});

test('mixed selections attack with owned units whose weapons can hit the chosen target',()=>{
 const run=setup(),r=run.plain(`(()=>{const ground=addEntity('unit','engineer',0,2,2),rifle=addEntity('unit','rifleman',0,3,2),foreign=addEntity('unit','rifleman',1,4,2),air=addEntity('unit','phoenix',1,10,10);selectEntities([ground,rifle,foreign]);interactionMode='attack';handlePolishTap(10,10);return {cmd:testCommands[0],ground:ground.targetId,rifle:rifle.targetId,foreign:foreign.targetId,enemy:air.id}})()`);
 assert.equal(r.cmd.type,'attack');assert.equal(r.cmd.entityId,2);assert.equal(r.rifle,r.enemy);assert.equal(r.ground,null);assert.equal(r.foreign,null);
});

test('armed abilities, construction, mining and rally own taps on friendly troops and invalid targets',()=>{
 const run=setup(),r=run.plain(`(()=>{const u=addEntity('unit','engineer',0,2,2);addEntity('unit','rifleman',0,8,8);selectEntity(u);interactionMode='ability';interactionPayload='psionicStorm';legalTiles=[];handlePolishTap(8,8);const ability={mode:interactionMode,selection:selectedId,count:testCommands.length};interactionMode='build';interactionPayload='barracks';legalTiles=[];handlePolishTap(8,8);const build={mode:interactionMode,selection:selectedId,count:testCommands.length};interactionMode='mine';handlePolishTap(8,8);const mine={mode:interactionMode,selection:selectedId,count:testCommands.length};interactionMode='rally';handlePolishTap(8,8);return {ability,build,mine,last:testCommands.at(-1),selected:selectedId}})()`);
 for(const [mode,state] of Object.entries({ability:r.ability,build:r.build,mine:r.mine})){assert.equal(state.mode,mode);assert.equal(state.selection,1);assert.equal(state.count,0)}assert.equal(r.last.type,'setRally');assert.equal(r.selected,1);
});

test('contextual taps retain selection after move/attack and switch or double-select visible friendly types',()=>{
 const run=setup(),r=run.plain(`(()=>{const u=addEntity('unit','rifleman',0,2,2),friend=addEntity('unit','rifleman',0,4,4),garrison=addEntity('unit','rifleman',0,5,4);addEntity('unit','rifleman',0,BOARD_W-1,4);const enemy=addEntity('unit','zealot',1,9,9);garrison.garrisonedIn=123;W=250;selectEntity(u);handlePolishTap(7,7);const moved=u.moveDestination!=null,moveSelection=selectedId;handlePolishTap(9,9);const attacked=u.targetId===enemy.id,attackSelection=selectedId;handlePolishTap(4,4);const switched=selectedId;testClock+=100;handlePolishTap(4,4);return {moved,attacked,moveSelection,attackSelection,switched,ids:selectedIds,commands:testCommands.length}})()`);
 assert.equal(r.moved,true);assert.equal(r.attacked,true);assert.equal(r.moveSelection,1);assert.equal(r.attackSelection,1);assert.equal(r.switched,2);assert.deepEqual(r.ids,[1,2]);assert.equal(r.commands,2);
});

test('nearest search excludes allied teams, neutral, dead and transported entities but neutral destructibles allow exact attacks',()=>{
 const run=setup(),r=run.plain(`(()=>{const u=addEntity('unit','rifleman',0,2,2);addEntity('unit','zealot',1,10,11);const enemy=addEntity('unit','zergling',2,13,10),neutral=addEntity('structure','neutralRock',-1,10,9),dead=addEntity('unit','zergling',2,11,10),transported=addEntity('unit','zergling',2,9,10);players[0].teamId='blue';players[1].teamId='blue';neutral.faction=-1;dead.alive=false;transported.garrisonedIn=123;selectEntity(u);const target=nearestCommandEnemy({x:10,y:10})?.id;interactionMode='attack';handlePolishTap(10,9);return {target,expected:enemy.id,exact:u.targetId,neutral:neutral.id}})()`);
 assert.equal(r.target,r.expected);assert.equal(r.exact,r.neutral);
});

test('authoritative commands still reject forged foreign ownership and hidden or invalid targets',()=>{
 const run=setup(),r=run.plain(`(()=>{const u=addEntity('unit','engineer',0,2,2),enemy=addEntity('unit','zealot',1,7,7),air=addEntity('unit','phoenix',1,8,7);applyCommand({type:'attack',entityId:u.id,targetId:enemy.id,slot:1});const forged=u.targetId;tileVisibleToFaction=()=>false;applyCommand({type:'attack',entityId:u.id,targetId:enemy.id,slot:0});const hidden=u.targetId;tileVisibleToFaction=()=>true;applyCommand({type:'attack',entityId:u.id,targetId:air.id,slot:0});return {forged,hidden,air:u.targetId}})()`);
 assert.equal(r.forged,null);assert.equal(r.hidden,null);assert.equal(r.air,null);
});


test('stacked troops cycle once per tap and double-tap selects the type that was actually selected',()=>{
 const run=setup(),r=run.plain(`(()=>{const first=addEntity('unit','rifleman',0,4,4),second=addEntity('unit','engineer',0,4,4),same=addEntity('unit','rifleman',0,5,5);handlePolishTap(4,4);const selectedType=selected().type,firstSelection=selectedId;testClock+=100;handlePolishTap(4,4);return {firstSelection,selectedType,ids:selectedIds,types:selectedEntities().map(e=>e.type),commands:testCommands.length}})()`);
 assert.equal(r.firstSelection,1);assert.equal(r.selectedType,'rifleman');assert.deepEqual(r.ids,[1,3]);assert.deepEqual(r.types,['rifleman','rifleman']);assert.equal(r.commands,0);
});
