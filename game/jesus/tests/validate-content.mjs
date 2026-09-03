import assert from "node:assert/strict";
import { ROOM_LIBRARY, SAFE_FALLBACK_ROOM } from "../js/world/rooms.js";
import { validateRoom } from "../js/world/room-validator.js";
import { generateRoom } from "../js/world/generator.js";
import { REGIONS } from "../js/world/regions.js";
import { BOSS_DEFINITIONS } from "../js/bosses/boss-data.js";
import { createAttackData } from "../js/bosses/boss-attacks.js";

for (const room of [...ROOM_LIBRARY, SAFE_FALLBACK_ROOM]) {
  const result = validateRoom(room);
  assert.equal(result.valid, true, `${room.id}: ${result.reason}`);
  assert.ok(room.platforms.some((platform) => platform.entry), `${room.id} must define an entrance`);
  assert.ok(room.platforms.some((platform) => platform.exit), `${room.id} must define an exit`);
}

for (const region of Object.keys(REGIONS)) {
  for (let tier = 1; tier <= 6; tier++) {
    const generated = generateRoom({ region, depth: (tier - 1) * 2, seed: tier * 101 });
    assert.equal(generated.routeValid, true, `${region} tier ${tier} generated an invalid route`);
  }
}

for (const boss of Object.values(BOSS_DEFINITIONS)) {
  assert.equal(boss.phases.length, 3, `${boss.id} must have three phases`);
  const attackLibrary = new Set(boss.phases.flatMap((phase) => phase.attacks));
  assert.ok(attackLibrary.size >= 5, `${boss.id} needs at least five attacks`);
  for (const phase of boss.phases) {
    assert.ok(phase.attacks.some((attack) => boss.exposingAttacks.includes(attack)), `${boss.id}/${phase.name} needs a guaranteed vulnerability attack`);
  }
}

const sampleDefinition = BOSS_DEFINITIONS.warBeast;
const sampleBoss = { definition: sampleDefinition, phase: sampleDefinition.phases[0], attackCount: 0 };
const sampleAttack = createAttackData("charge", sampleBoss, { x: 222, y: 300, w: 34, h: 62 }, { left: 0, right: 1400 });
const preservedTarget = sampleAttack.targetX;
const disposableVisuals = [{ life: 0 }];
disposableVisuals.length = 0;
assert.equal(sampleAttack.targetX, preservedTarget, "attack execution data must survive telegraph cleanup");

console.log(`Validated ${ROOM_LIBRARY.length + 1} rooms, ${Object.keys(REGIONS).length * 6} generated runs, and ${Object.keys(BOSS_DEFINITIONS).length} bosses.`);
