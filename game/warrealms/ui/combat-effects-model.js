// Pure presentation policy. No game rules, randomness, DOM, or network state.
export const COMBAT_EFFECTS_KEY = "warRealms.combatEffects";
export const EFFECT_LIMITS = Object.freeze({ queue: 64, nodes: 160, mobileNodes: 80, seen: 256, ghosts: 12 });
export const COMBAT_TYPES = new Set(["card-destroy", "card-disable", "card-enable", "base-damage", "base-repair", "health-loss", "health-gain", "shield-gain", "armor-gain", "stat-gain"]);
export function magnitude(amount) {
  const value = Math.abs(Number(amount) || 0);
  return value >= 10 ? "heavy" : value >= 5 ? "medium" : "minor";
}
export function effectPriority(event) {
  if (event.type === "card-transform") return 100;
  if (event.type === "card-destroy") return 90;
  if (event.lethal) return 85;
  if (event.shieldBroken) return 80;
  if (event.type === "card-disable" || event.type === "card-enable") return 70;
  if (["health-loss", "base-damage"].includes(event.type)) return magnitude(event.amount) === "heavy" ? 60 : 40;
  if (["health-gain", "base-repair", "shield-gain"].includes(event.type)) return 50;
  return 20;
}
export function effectTarget(event) {
  return String(event.instanceId || event.targetId || event.actorId || event.sourceId || "board");
}
// Choose the most important target, but resolve that target's earlier hits first.
// This preserves projectile -> impact -> destruction and disable -> restore order.
export function nextEffectIndex(queue) {
  let best = -1, priority = -1;
  queue.forEach((item, index) => {
    const score = effectPriority(item.event);
    if (score > priority) { best = index; priority = score; }
  });
  if (best < 0) return -1;
  const target = effectTarget(queue[best].event);
  return queue.findIndex(item => effectTarget(item.event) === target);
}
export function combatFeedback(event) {
  const amount = Math.max(0, Number(event.amount) || 0);
  const count = event.hits > 1 ? ` ×${event.hits}` : "";
  const absorbed = Math.max(0, Number(event.absorbed) || 0);
  switch (event.type) {
    case "health-loss": case "base-damage":
      return [
        ...(absorbed ? [{ kind: "shield-hit", amount: absorbed, label: `${event.type === "base-damage" ? "⬡" : "🛡"} −${absorbed}` }] : []),
        ...(event.shieldBroken ? [{ kind: "shield-break", amount: absorbed, label: "SHIELD BREAK" }] : []),
        ...(amount ? [{ kind: "damage", amount, label: `−${amount}${count}` }] : []),
        ...(event.lethal ? [{ kind: "death", amount, label: "REALM FALLEN" }] : [])
      ];
    case "health-gain": return amount ? [{ kind: "heal", amount, label: `+${amount}${count}` }] : [];
    case "base-repair": return amount ? [{ kind: "repair", amount, label: `+${amount}${count}` }] : [];
    case "shield-gain": return amount ? [{ kind: "shield-gain", amount, label: `🛡 +${amount}${count}` }] : [];
    case "armor-gain": return amount ? [{ kind: "shield-gain", amount, label: `⬡ +${amount}` }] : [];
    case "stat-gain": return amount ? [{ kind: event.stat === "combat" ? "combat" : "trade", amount, label: `${event.stat === "combat" ? "⚔" : "◆"} +${amount}` }] : [];
    case "card-disable": return [{ kind: "disable", amount: 1, label: "DISABLED" }];
    case "card-enable": return [{ kind: "enable", amount: 1, label: "REACTIVATED" }];
    case "card-destroy": return [{ kind: "raze", amount: event.strong ? 12 : 5, label: "RAZED" }];
    default: return [];
  }
}
