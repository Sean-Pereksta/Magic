// Serializable feedback shared by local and remote renderers.
export function bridgeCombatEvent(event) {
  const type = { BASE_DAMAGED: "base-damage", BASE_REPAIRED: "base-repair", SHIELD_GAINED: "shield-gain", AUTHORITY_GAINED: "health-gain" }[event.type];
  if (!type || !(event.amount > 0 || event.absorbed > 0)) return null;
  const cardTarget = type === "base-damage" || type === "base-repair";
  return {
    id: `combat_${event.id}`, type, targetId: String(event.targetId || event.ownerId || event.playerId || ""),
    actorId: String(event.actorId || ""), amount: Number(event.amount) || 0, absorbed: Number(event.absorbed) || 0,
    cardId: cardTarget ? String(event.cardId || "") : "", instanceId: cardTarget ? String(event.instanceId || "") : "",
    sourceInstanceId: String(event.sourceInstanceId || (!cardTarget ? event.instanceId : "") || ""),
    faction: String(event.faction || ""), method: String(event.method || ""), atMs: Date.now()
  };
}
