import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import {
  buildCompactCardStatuses,
  transformationPresentation,
  transformationTimings
} from "../ui/card-presentation.js";

test("compact statuses keep exact Heat first and represent actions as symbols", () => {
  const statuses = buildCompactCardStatuses({
    heat: 5,
    charge: 3,
    transformReady: true,
    directAction: "play-card"
  });

  assert.deepEqual(statuses.map(status => status.kind), ["heat", "charge", "ready", "action"]);
  assert.equal(statuses[0].icon, "♨");
  assert.equal(statuses[0].value, "5");
  assert.equal(statuses.at(-1).icon, "▶");
  assert.equal(statuses.at(-1).value, "");
  assert.equal(statuses.at(-1).label, "Tap to play");
});

test("status combinations remain additive instead of replacing Heat", () => {
  for (const input of [
    { heat: 2 },
    { directAction: "play-card" },
    { heat: 4, directAction: "play-card" },
    { heat: 6, charge: 2 },
    { heat: 7, charge: 3, directAction: "play-card" },
    { heat: 8, charge: 4, transformReady: true, cooldown: 2, activated: true, disabled: true, directAction: "play-card" }
  ]) {
    const statuses = buildCompactCardStatuses(input);
    if (input.heat != null) {
      assert.equal(statuses[0].kind, "heat");
      assert.equal(statuses[0].value, String(input.heat));
    }
    assert.equal(new Set(statuses.map(status => status.kind)).size, statuses.length);
  }
});

test("transformation copy distinguishes evolution, hatching, and ascension", () => {
  assert.equal(transformationPresentation({ presentation: "evolution" }).complete, "EVOLUTION COMPLETE");
  assert.equal(transformationPresentation({ fromName: "War Egg", toName: "Sky Drake" }).complete, "HATCH COMPLETE");
  assert.equal(transformationPresentation({ method: "ownerTurnsElapsed" }).complete, "ASCENSION COMPLETE");
});

test("normal and reduced transformation sequences stay brief", () => {
  const total = timings => Object.values(timings).reduce((sum, value) => sum + value, 0);
  assert.equal(total(transformationTimings(false)), 2250);
  assert.equal(total(transformationTimings(true)), 680);
});

test("WarRealms shell uses wrapped trays, explicit inspection, and a queued cinematic", async () => {
  const source = await readFile(new URL("../../warrealms.html", import.meta.url), "utf8");
  assert.match(source, /\.cardStatusTray\{[^}]*flex-wrap:wrap/);
  assert.match(source, /\.gameCard\.directAction::after\{display:none\}/);
  assert.doesNotMatch(source, /DISABLED NEXT TURN/);
  assert.match(source, /data-act="card-info"/);
  assert.match(source, /async function animateCardTransformation/);
  assert.match(source, /visualEventChain = visualEventChain\.then/);
  assert.match(source, /id="commanderShield" hidden/);
});
