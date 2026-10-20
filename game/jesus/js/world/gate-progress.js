import { overlaps } from "../common.js";
import { emitCombatEffect } from "../enemies/enemies.js";

const livingGuardian = (world, mechanism) => mechanism.guardianId &&
  !world._state?.dead?.has(mechanism.guardianId) &&
  !(world.enemies || []).some(enemy => enemy.id === mechanism.guardianId && enemy.dead);

// Mechanisms share the existing activated-ID save set. IDs are scoped to their
// own room and gate, so a relic never unlocks an unrelated passage.
export function updateGateProgress(world, player, dt = 0) {
  const events = [];
  const mechanisms = (world.switches || []).filter(sw => sw.exploration);
  const pickups = (world.pickups || []).filter(p => p.unlockId);
  for (const pickup of pickups) {
    const mechanism = mechanisms.find(sw => sw.id === pickup.unlockId);
    pickup.locked = !mechanism || Boolean(livingGuardian(world, mechanism));
  }
  for (const mechanism of mechanisms) {
    if (mechanism.activated || livingGuardian(world, mechanism)) continue;
    const portable = mechanism.kind === "key" || mechanism.kind === "relic";
    const activated = portable ? pickups.some(p => p.unlockId === mechanism.id && p.collected) : overlaps(player, mechanism);
    if (!activated) continue;
    mechanism.activated = true;
    world._state?.activated?.add(mechanism.id);
    emitCombatEffect(world, "fx-platform-activation", mechanism.x + mechanism.w / 2, mechanism.y, { duration: .7, scale: .5 });
    events.push({ type: "mechanism", mechanismId: mechanism.id, title: portable ? "Relic recovered" : "Mechanism activated", detail: "The matching seal responds" });
  }
  for (const gate of (world.platforms || []).filter(p => p.kind === "gate" && p.exploration)) {
    if (gate.open) continue;
    const ids = gate.unlockIds || [];
    const ready = ids.length > 0 && ids.every(id => world._state?.activated?.has(id) || mechanisms.some(sw => sw.id === id && sw.activated));
    if (!ready) {
      if (!gate.hintShown && player.x + player.w > gate.x - 320 && player.x < gate.x) {
        gate.hintShown = true;
        const target = mechanisms.find(sw => ids.includes(sw.id) && !sw.activated);
        const direction = target && target.y > player.y + 100 ? "below" : "above";
        events.push({ type: "hint", title: "Passage sealed", detail: gate.hint || `Find the ${target?.kind === "switch" || target?.kind === "pressure" ? "mechanism" : "relic"} ${direction}` });
      }
      continue;
    }
    if (!gate.opening) {
      gate.opening = true;
      emitCombatEffect(world, gate.appearance === "rubble" || gate.appearance === "boulder" ? "fx-rubble-destruction" : "fx-shield-pulse", gate.x + gate.w / 2, player.y + player.h / 2, { duration: 1, scale: .9 });
      events.push({ type: "opening", title: "Passage opening", detail: "Return to the matching gate" });
    }
    gate.openProgress = Math.min(1, (gate.openProgress || 0) + Math.max(0, dt) / (gate.openDuration || 1));
    if (gate.openProgress >= 1) { gate.open = true; gate.opening = false; }
  }
  return events;
}

export const GATE_PROGRESS_PROOF_SIGNATURE = [livingGuardian, updateGateProgress]
  .map(fn => fn.toString().replace(/\s+/g, " ")).join("|");
