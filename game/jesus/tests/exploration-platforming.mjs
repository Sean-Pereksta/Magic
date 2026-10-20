import assert from "node:assert/strict";
import {PHYSICS,VIEW} from "../js/config.js";
import {EXPLORATION_ROOM_LIBRARY,DYNAMIC_ROOM_LIBRARY,roomForId} from "../js/world/rooms.js";
import {CHAMBER_STYLES} from "../js/world/exploration-rooms.js";
import {validateRoom,canReachSurface} from "../js/world/room-validator.js";
import {createLevel,updateWorldStreaming,persistWorldState,restoreWorldState,layoutTemplate,BIOME_ORDER,CHUNK_WIDTH} from "../js/world/generator.js";
import {updateGateProgress} from "../js/world/gate-progress.js";
import {collectPickups} from "../js/powers/blessings.js";
import {createPlayer} from "../js/physics.js";

const clone=value=>JSON.parse(JSON.stringify(value));
assert.deepEqual(EXPLORATION_ROOM_LIBRARY.map(r=>r.chamberStyle),CHAMBER_STYLES);
let phases=0;
for(const room of EXPLORATION_ROOM_LIBRARY) {
  assert.equal(roomForId(room.id),room);
  const proof=validateRoom(room,PHYSICS,{dash:false,wallJump:false,wings:false,sandals:false});
  assert.ok(proof.valid,`${room.id}: ${proof.reason}`);
  assert.ok(proof.baseAbilities&&proof.exploration);
  assert.ok(proof.phaseProofs.every(p=>p.crossedGate&&p.activated.length===room.switches.length));
  assert.equal(proof.phaseProofs.length,(room.movers.length?4:1)*(room.alternateExplorationRoute?2:1));phases+=proof.phaseProofs.length;
  const gate=room.platforms.find(p=>p.mandatory),ceiling=room.platforms.find(p=>p.kind==="ceiling");
  assert.ok(gate.y<=-700&&gate.y+gate.h>=1000&&!gate.oneWay);
  assert.ok(ceiling.x<=gate.x&&ceiling.x+ceiling.w>=gate.x+gate.w&&!ceiling.oneWay);
  assert.ok(room.platforms.some(p=>p.kind==="ground"&&!p.oneWay&&p.x<=gate.x&&p.x+p.w>=gate.x+gate.w&&p.y<=VIEW.floorY&&p.y+p.h>=gate.frameBottom));
  assert.ok(room.enemySpawns.every(e=>e.guardian&&e.elite&&e.hp>=5&&e.guardBounds.right-e.guardBounds.left>=170),"guardians have broad dedicated fight space");
  const short=clone(room);short.platforms.find(p=>p.mandatory).h=400;
  assert.equal(validateRoom(short).valid,false,"a jumpable gate cannot obtain a route proof");
  const roofless=clone(room);roofless.platforms.find(p=>p.kind==="ceiling").oneWay=true;
  assert.equal(validateRoom(roofless).valid,false,"a decorative ceiling cannot obtain a route proof");
  const inaccessible=clone(room);inaccessible.switches[0].x=9000;
  for(const pickup of inaccessible.pickupSpawns)if(pickup.unlockLocal===0)pickup.x=9000;
  assert.equal(validateRoom(inaccessible).valid,false,"a key or switch outside its reachable alcove invalidates the full route");
}
const shaft=EXPLORATION_ROOM_LIBRARY.find(r=>r.chamberStyle==="shaft");
assert.equal(canReachSurface(shaft.platforms[0],shaft.platforms[shaft.guardianSurface.index],PHYSICS,{dash:false,wings:false}),false,"shaft requires its wall contacts and resting ledges");
const oldPortico=DYNAMIC_ROOM_LIBRARY.find(r=>r.id==="restored-portico"),newPortico=layoutTemplate(oldPortico);
assert.equal(layoutTemplate(oldPortico,2),oldPortico);
assert.ok(!oldPortico.platforms.some(p=>p.mandatory));
assert.ok(newPortico.platforms.some(p=>p.mandatory&&p.h===1700));
assert.ok(validateRoom(newPortico).valid,"new pressure door is proved with its full-height collider and opening delay");

let encounters=0,guardians=0,required=0;const styles=new Set();
for(let seed=1;seed<=100;seed++)for(const biome of BIOME_ORDER) {
  const w=createLevel({biome,seed,difficulty:4});
  assert.equal(w.layoutVersion,3);assert.ok(w.routeValid);
  const rooms=w.chunkPlan.filter(c=>c.gateEncounter);assert.equal(rooms.length,2);
  assert.ok(rooms.every(c=>!c.rest&&!c.final&&c.validation.exploration&&c.validation.valid));
  assert.notEqual(rooms[0].moduleId,rooms[1].moduleId,"exploration does not repeat its chamber within a short region");
  for(const c of rooms){encounters++;guardians+=Number(c.guardian);required+=Number(c.requiredGuardian);styles.add(c.chamberStyle);}
}
assert.equal(styles.size,8,"all eight authored chamber families appear in generated regions");
assert.ok(guardians/encounters>=.57&&guardians/encounters<=.63,`${guardians}/${encounters} encounters carry stronger guardians`);
assert.ok(required>0&&required<guardians,"some guardians must be defeated and others can be avoided");

const options={biome:"city",seed:138,difficulty:4},w=createLevel(options);
const encounter=w.chunkPlan.find(c=>c.gateEncounter);updateWorldStreaming(w,encounter.x+100);
const gate=w.platforms.find(g=>g.chunkIndex===encounter.index&&g.mandatory);
const mechanisms=w.switches.filter(s=>s.gateId===gate.id);
assert.ok(gate.unlockIds.every(id=>mechanisms.some(s=>s.id===id)),"gate identities bind only their own chamber mechanisms");
const p=createPlayer();
for(const sw of mechanisms) {
  if(sw.guardianId){const enemy=w.enemies.find(e=>e.id===sw.guardianId);enemy.dead=true;}
  const pickup=w.pickups.find(k=>k.unlockId===sw.id);
  p.x=sw.x;p.y=sw.y;
  updateGateProgress(w,p,0);if(pickup){p.x=pickup.x;p.y=pickup.y;collectPickups(w,p);}updateGateProgress(w,p,0);
  assert.ok(sw.activated);
}
assert.ok(gate.opening&&!gate.open,"activation begins a solid opening animation");
const snapshot=persistWorldState(w);
updateWorldStreaming(w,w.width-100);updateWorldStreaming(w,encounter.x+100);
assert.ok(w.platforms.find(g=>g.id===gate.id).open,"evicted completed mechanisms restore their gate open");
const restored=createLevel(options);restoreWorldState(restored,clone(snapshot),encounter.x+100);
assert.ok(restored.platforms.find(g=>g.id===gate.id).open,"JSON checkpoint restores the correct open gate");
assert.ok(restored.switches.filter(s=>s.gateId===gate.id).every(s=>s.activated));
for(const c of w.chunkPlan.filter(c=>c.gateEncounter&&c.index!==encounter.index)) {
  updateWorldStreaming(restored,c.x+100);
  assert.ok(restored.platforms.filter(g=>g.chunkIndex===c.index&&g.mandatory).every(g=>!g.open),"another room's key cannot open this gate");
}
console.log(`Eight chambers, ${phases} continuous base-controller route/phase proofs, sealed legacy-door upgrade, 600 seeded worlds, ${guardians}/${encounters} guardian balance, and scoped streamed checkpoint gates passed.`);
