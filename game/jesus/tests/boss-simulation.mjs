import assert from "node:assert/strict";
import { BossController, BOSS_STATES } from "../js/bosses/boss-controller.js";
import { BOSS_DEFINITIONS } from "../js/bosses/boss-data.js";
import { createPlayer, updateMovers, updatePlayer } from "../js/physics.js";
import { generateRoom } from "../js/world/generator.js";

for (const definition of Object.values(BOSS_DEFINITIONS)) {
  let defeated = false;
  const controller = new BossController(() => { defeated = true; });
  const world = { platforms: [], movers: [], hazards: [], enemies: [], pickups: [], props: [], bossHazards: [] };
  const player = createPlayer();
  player.hearts = 999;
  controller.start(definition.id, 3, world, player);
  const visited = new Set();
  let previousState = null;

  for (let step = 0; step < 12000 && !defeated; step++) {
    controller.update(world, player, 1 / 120, { kick() {} });
    const state = controller.boss?.state;
    visited.add(state);
    if (state === BOSS_STATES.VULNERABLE && previousState !== BOSS_STATES.VULNERABLE) controller.damage(4, "simulation");
    previousState = state;
  }

  assert.ok(visited.has(BOSS_STATES.TELL), `${definition.id} never telegraphed`);
  assert.ok(visited.has(BOSS_STATES.ATTACK), `${definition.id} never attacked`);
  assert.ok(visited.has(BOSS_STATES.VULNERABLE), `${definition.id} never became vulnerable`);
  assert.equal(defeated, true, `${definition.id} could not be completed through vulnerability windows`);
}

const room = generateRoom({ region: "galilee", depth: 0, seed: 42 });
const runner = createPlayer();
const held = new Set(["d"]);
let jumpQueued = false;
const input = {
  horizontal: () => 1,
  jumpPressed: () => { const value = jumpQueued; jumpQueued = false; return value; },
  jumpHeld: () => held.has("jump"),
  downHeld: () => false,
};
for (let step = 0; step < 240; step++) {
  if (step === 25) { jumpQueued = true; held.add("jump"); }
  if (step === 70) held.delete("jump");
  updateMovers(room, step / 120, 1 / 120);
  updatePlayer(runner, room, input, 1 / 120);
}
assert.ok(runner.x > 300, "player movement simulation did not advance across the room");

console.log(`Simulated ${Object.keys(BOSS_DEFINITIONS).length} completable boss fights and responsive player movement.`);
