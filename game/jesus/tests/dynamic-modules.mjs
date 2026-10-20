import assert from "node:assert/strict";
import { PHYSICS, VIEW } from "../js/config.js";
import { createPlayer, updateWorldEnvironment } from "../js/physics.js";
import { DYNAMIC_ROOM_LIBRARY, dynamicRoomsFor } from "../js/world/dynamic-rooms.js";
import { BIOME_ORDER } from "../js/world/regions.js";
import { validateRoom } from "../js/world/room-validator.js";

const dt = PHYSICS.fixedStep;
const copy = value => JSON.parse(JSON.stringify(value));
assert.ok(DYNAMIC_ROOM_LIBRARY.length >= 12, "at least twelve authored dynamic rooms are available");
assert.equal(new Set(DYNAMIC_ROOM_LIBRARY.map(r => r.id)).size, DYNAMIC_ROOM_LIBRARY.length);
for (const biome of BIOME_ORDER) {
  assert.ok(dynamicRoomsFor(biome, 2).length > 0, `${biome} introduces mandatory motion before tier three`);
  assert.ok(DYNAMIC_ROOM_LIBRARY.filter(r => r.region === biome).length >= 2, `${biome} has two different authored encounters`);
}
let phaseProofs = 0;
for (const room of DYNAMIC_ROOM_LIBRARY) {
  const entry = room.platforms.find(p => p.entry), exit = room.platforms.find(p => p.exit);
  assert.ok(entry && !entry.temporary && entry.x <= 100 && entry.x + entry.w > 100 + PHYSICS.playerWidth);
  assert.equal(entry.y, VIEW.floorY, `${room.id} checkpoint rests on the shared static entry`);
  assert.ok(exit && !exit.temporary && exit.x + exit.w === room.width);
  assert.equal(exit.y, VIEW.floorY, `${room.id} exit meets the next shared seam`);
  assert.ok(room.movers.length && room.movers.every(m => m.required && m.route));
  assert.ok(room.validationWaypoints.some(p => p.surface === "movers"));
  const result = validateRoom(room, PHYSICS, { dash: false, wallJump: false, wings: false, sandals: false });
  assert.ok(result.valid, `${room.id}: ${result.reason}`);
  assert.equal(result.phaseProofs.length, 4, `${room.id} can recover from different initial timings`);
  phaseProofs += result.phaseProofs.length;
  const bypass = validateRoom({ ...room, requiresDynamic: false, validationWaypoints: [], movers: [], hazards: [] }, PHYSICS,
    { dash: true, wallJump: true, wings: false, sandals: false });
  assert.equal(bypass.valid, false, `${room.id} cannot bypass its moving encounter using a static dash route`);
  for (const hazard of room.hazards) {
    if (hazard.type === "wind") {
      assert.ok(hazard.x >= entry.x + entry.w && hazard.x + hazard.w < exit.x, `${room.id} keeps its entry and exit calm`);
      assert.ok(hazard.force <= 130, `${room.id} gusts remain weaker than ordinary directional control`);
    } else {
      assert.ok(hazard.timed && hazard.warningDuration >= 1.2, `${room.id}/${hazard.type} telegraphs before activation`);
      assert.ok(hazard.period > hazard.warningDuration + hazard.activeDuration + 1, "hazards leave an unhurried safe interval");
    }
  }
  for (const spawn of room.enemySpawns) {
    const support = room.platforms.find(p => !p.temporary && p.kind !== "gate" && spawn.x >= p.x + 40 && spawn.x + 130 <= p.x + p.w && Math.abs(spawn.y + 52 - p.y) < 1);
    assert.ok(support, `${room.id} combat stays on a broad static landing`);
  }
}

// A real overlap with the sanctuary plate permanently opens its linked gate.
const sanctuary = copy(DYNAMIC_ROOM_LIBRARY.find(r => r.id === "restored-portico"));
const sw = sanctuary.switches[0]; sw.id = "sanctuary-plate";
for (const p of sanctuary.platforms) if (p.switchLocal === 0) p.switchId = sw.id;
const pressureGate = sanctuary.platforms.find(p => p.pressureGate);
const pressurePlayer = createPlayer(sw.x + 5, sw.y + 20 - PHYSICS.playerHeight);
const pressureWorld = { ...sanctuary, width: sanctuary.width, effects: [] };
updateWorldEnvironment(pressureWorld, pressurePlayer, dt, 0);
assert.ok(sw.activated && pressureGate.open, "the pressure plate physically opens its gate");
pressurePlayer.x = 0;
updateWorldEnvironment(pressureWorld, pressurePlayer, dt, dt);
assert.ok(sw.activated && pressureGate.open, "the sanctuary gate remains open after leaving the plate");

// The required mountain bridge crumbles underfoot and returns after its reset.
const spire = DYNAMIC_ROOM_LIBRARY.find(r => r.id === "thawing-spire");
const collapse = copy(spire.platforms.find(p => p.kind === "collapsing" && p.route));
assert.ok(collapse && collapse.visualKind === "bridge", "the spire has a required unstable wooden bridge");
collapse.id = "test-collapse";
const collapseWorld = { width: 3200, platforms: [collapse], movers: [], hazards: [], switches: [], effects: [] };
const collapsePlayer = createPlayer(collapse.x + 30, collapse.y - PHYSICS.playerHeight);
collapsePlayer.onGround = true; collapsePlayer.groundId = collapse.id;
for (let step = 0; step < 120; step++) updateWorldEnvironment(collapseWorld, collapsePlayer, dt, step * dt);
assert.ok(collapse.dead, "standing on the unstable bridge triggers collapse");
collapsePlayer.groundId = null;
for (let step = 0; step < 510; step++) updateWorldEnvironment(collapseWorld, collapsePlayer, dt, 1 + step * dt);
assert.ok(!collapse.dead && collapse.collapseTimer == null, "the required bridge recovers instead of permanently removing the route");

console.log(`${DYNAMIC_ROOM_LIBRARY.length} authored moving rooms replayed at ${phaseProofs} initial phases; static dash bypasses rejected; pressure gate and recovering bridge exercised.`);
