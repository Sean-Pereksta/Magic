const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');
function setup(){const run=simulation();run(`cam.x=0;cam.y=0;cam.zoom=1;W=1600;H=1200;globalThis.hitCommands=[];sendCommand=c=>{hitCommands.push(c);applyCommand({...c,slot:PLAYER})}`);return run}

test('screen hit testing selects moving troop body and logical square while discriminating a nearby stationary neighbor',()=>{
 const run=setup(),r=run.plain(`(()=>{const moving=addEntity('unit','rifleman',0,8,4),neighbor=addEntity('unit','engineer',0,4,4);moving.px=3.35;moving.py=4;let p=entityScreenAnchor(moving);handlePolishTap(p.x,p.y);const first=selectedId;testClock+=100;p=entityScreenAnchor(neighbor);handlePolishTap(p.x,p.y);return {first,moving:moving.id,second:selectedId,neighbor:neighbor.id,commands:hitCommands.length,old:selectableEntityAtScreen(8.5*TILE,4.5*TILE)?.id}})()`);
 assert.equal(r.first,r.moving);assert.equal(r.second,r.neighbor);assert.equal(r.commands,0);assert.equal(r.old,r.moving,'the troop remains selectable throughout its logically occupied square while it animates');
});

test('exact attack targets a moving enemy body while movement still uses the authoritative clicked ground tile',()=>{
 const run=setup(),r=run.plain(`(()=>{const own=addEntity('unit','rifleman',0,2,4),enemy=addEntity('unit','zergling',1,9,4);enemy.px=5.2;enemy.py=4;selectEntity(own);interactionMode='attack';const p=entityScreenAnchor(enemy);handlePolishTap(p.x,p.y);const attack=hitCommands[0],target=own.targetId;handlePolishTap(7.5*TILE,8.5*TILE);return {attack,target,expected:enemy.id,move:hitCommands.at(-1),enemyX:enemy.x,enemyPx:enemy.px}})()`);
 assert.equal(r.attack.type,'attack');assert.equal(r.attack.targetId,r.expected);assert.equal(r.target,r.expected);assert.equal(r.move.type,'move');assert.equal(r.move.x,7);assert.equal(r.move.y,8);assert.equal(r.enemyX,9);assert.equal(r.enemyPx,5.2);
});

test('air and ground bodies use different displayed anchors without selecting oversized artwork over a neighbor',()=>{
 const run=setup(),r=run.plain(`(()=>{const ground=addEntity('unit','rifleman',0,4,4),air=addEntity('unit','wraith',0,4,4),neighbor=addEntity('unit','engineer',0,5,4);const g=entityScreenAnchor(ground),a=entityScreenAnchor(air),n=entityScreenAnchor(neighbor);return {offset:g.y-a.y,air:selectableEntityAtScreen(a.x,a.y,true)?.id,ground:selectableEntityAtScreen(g.x,g.y,true)?.id,neighbor:selectableEntityAtScreen(n.x,n.y,true)?.id,ids:[ground.id,air.id,neighbor.id]}})()`);
 assert.equal(r.offset,12);assert.equal(r.air,r.ids[1]);assert.equal(r.ground,r.ids[0]);assert.equal(r.neighbor,r.ids[2]);
});

test('visual hit rejects hidden logical or painted positions and ground attack requires logical visibility',()=>{
 const run=setup(),r=run.plain(`(()=>{const own=addEntity('unit','rifleman',0,2,4),enemy=addEntity('unit','zergling',1,9,4);enemy.px=5.2;enemy.py=4;selectEntity(own);const p=entityScreenAnchor(enemy);tileVisibleToFaction=(f,x)=>x!==5;const visual=selectableEntityAtScreen(p.x,p.y)?.id;tileVisibleToFaction=(f,x)=>x!==9;const logical=selectableEntityAtScreen(p.x,p.y)?.id;applyCommand({type:'attackGround',entityId:own.id,x:30,y:20,slot:0});return {visual,logical,target:own.targetId}})()`);
 assert.equal(r.visual,undefined);assert.equal(r.logical,undefined);assert.equal(r.target,null);
});

test('held rectangle selection uses cosmetic body anchors instead of logical destination coordinates',()=>{
 const run=setup(),r=run.plain(`(()=>{const own=addEntity('unit','rifleman',0,9,4),other=addEntity('unit','engineer',0,5,4),enemy=addEntity('unit','zergling',1,8,4);own.px=3.2;own.py=4;enemy.px=3.3;enemy.py=4;selectUnitsInBox({startX:3*TILE,startY:4*TILE,endX:4*TILE,endY:5*TILE});return {ids:selectedIds,own:own.id,x:own.x,px:own.px,commands:hitCommands.length}})()`);
 assert.deepEqual(r.ids,[r.own]);assert.equal(r.x,9);assert.equal(r.px,3.2);assert.equal(r.commands,0);
});

test('co-located body clicks cycle once and double-taps select the first chosen troop type',()=>{
 const run=setup(),r=run.plain(`(()=>{const one=addEntity('unit','rifleman',0,8,4),two=addEntity('unit','engineer',0,8,4),same=addEntity('unit','rifleman',0,6,4);one.px=two.px=3.2;one.py=two.py=4;const p=entityScreenAnchor(one);handlePolishTap(p.x,p.y);const first=selectedId;testClock+=100;handlePolishTap(p.x,p.y);const group=selectedIds.slice();testClock+=400;handlePolishTap(p.x,p.y);const next=selectedId;return {first,group,next,ids:[one.id,two.id,same.id]}})()`);
 assert.equal(r.first,r.ids[0]);assert.deepEqual(r.group,[r.ids[0],r.ids[2]]);assert.equal(r.next,r.ids[1]);
});
