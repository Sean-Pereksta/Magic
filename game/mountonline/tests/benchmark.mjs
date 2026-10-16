import { performance } from "node:perf_hooks";
import {
  createBattle,
  tickBattle,
  battleView,
  issueOrder,
} from "../battle.mjs";
import { battleArmy } from "../world.mjs";
const reports = [];
for (const size of [50, 200, 600]) {
  const b = createBattle({
    armyA: battleArmy(size, "crown"),
    armyB: battleArmy(size, "ember"),
    seed: 412,
  });
  const samples = [];
  issueOrder(
    b,
    0,
    b.units.filter((u) => u.team === 0).map((u) => u.id),
    "charge",
  );
  for (let i = 0; i < 800 && !b.finished; i++) {
    const begin = performance.now();
    tickBattle(b);
    samples.push(performance.now() - begin);
  }
  samples.sort((a, b) => a - b);
  const sum = samples.reduce((n, v) => n + v, 0);
  reports.push({
    troops: size * 2,
    steps: samples.length,
    meanStepMs: +(sum / samples.length).toFixed(2),
    p95StepMs: +samples[Math.floor(samples.length * 0.95)].toFixed(2),
    maxStepMs: +samples.at(-1).toFixed(2),
    snapshotBytes: Buffer.byteLength(JSON.stringify(battleView(b))),
    heapMB: +(process.memoryUsage().heapUsed / 1048576).toFixed(1),
  });
}
console.log(
  JSON.stringify(
    { runtime: process.version, simulationHz: 20, reports },
    null,
    2,
  ),
);
