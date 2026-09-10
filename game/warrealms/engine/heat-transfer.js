const HEAT_TRANSFER_DELTA_OFFSET = 1000000;
let pendingTransferIntents = 0;
const registeredHeatCards = new Map();

function positiveInteger(value, fallback = 1) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(1, Math.floor(number)) : fallback;
}

function nonNegativeInteger(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(0, Math.floor(number)) : fallback;
}

export function encodeMoveHeatAmount(amount = 1) {
  return HEAT_TRANSFER_DELTA_OFFSET + positiveInteger(amount);
}

export function decodeHeatDelta(delta = 0) {
  const number = Number(delta);
  if (!Number.isFinite(number)) return 0;
  const whole = Math.trunc(number);
  if (whole <= -HEAT_TRANSFER_DELTA_OFFSET) {
    pendingTransferIntents += 1;
    return -positiveInteger(Math.abs(whole) - HEAT_TRANSFER_DELTA_OFFSET);
  }
  return whole;
}

export function consumeMoveHeatTransferIntent() {
  if (pendingTransferIntents <= 0) return false;
  pendingTransferIntents -= 1;
  return true;
}

export function clearMoveHeatTransferIntents() {
  pendingTransferIntents = 0;
}

export function clearRegisteredHeatCards() {
  registeredHeatCards.clear();
}

function normalizeMoveHeatConfig(value) {
  if (typeof value === "number") return { amount: positiveInteger(value) };
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  return {
    ...value,
    amount: positiveInteger(value.amount)
  };
}

function installAdapter(value, seen) {
  if (!value || typeof value !== "object" || seen.has(value)) return;
  seen.add(value);

  if (!Array.isArray(value) && typeof value.id === "string" && value.heat && typeof value.heat === "object") {
    registeredHeatCards.set(value.id, value);
  }

  if (!Array.isArray(value) && Object.prototype.hasOwnProperty.call(value, "moveHeat")) {
    const moveHeat = normalizeMoveHeatConfig(value.moveHeat);
    if (moveHeat && !Object.prototype.hasOwnProperty.call(value, "toJSON")) {
      Object.defineProperty(value, "toJSON", {
        enumerable: false,
        configurable: true,
        value() {
          const serialized = Object.fromEntries(Object.entries(this));
          if (!serialized.coolHeat) {
            serialized.coolHeat = {
              amount: encodeMoveHeatAmount(moveHeat.amount),
              targets: 1
            };
          }
          return serialized;
        }
      });
    }
  }

  if (Array.isArray(value)) {
    value.forEach(item => installAdapter(item, seen));
  } else {
    Object.values(value).forEach(item => installAdapter(item, seen));
  }
}

export function installMoveHeatRuntimeAdapter(root) {
  installAdapter(root, new WeakSet());
  return root;
}

function allPlayerEntries(player) {
  if (!player) return [];
  return ["draw", "discard", "hand", "played", "bases", "attachments"]
    .flatMap(zone => player[zone] || []);
}

function activeHeatEntries(player) {
  if (!player) return [];
  return ["played", "bases", "attachments"]
    .flatMap(zone => player[zone] || []);
}

function heatDefinition(entry) {
  return registeredHeatCards.get(String(entry?.id || ""))?.heat || null;
}

export function prepareMoveHeatDestination(game, input = {}) {
  if (String(input.type || "") !== "HEAT_SPENT" || !consumeMoveHeatTransferIntent()) {
    return { transfer: false, refunded: false };
  }

  const playerId = String(input.playerId || input.ownerId || input.actorId || "");
  const player = (game?.players || []).find(candidate => String(candidate.id || "") === playerId);
  const sourceInstanceId = String(input.instanceId || "");
  const source = allPlayerEntries(player).find(entry => String(entry.instanceId || "") === sourceInstanceId);
  const amount = nonNegativeInteger(input.amount);

  if (!player || !source || !amount) return { transfer: true, refunded: false };

  const targets = activeHeatEntries(player).filter(entry => {
    if (String(entry.instanceId || "") === sourceInstanceId) return false;
    const heat = heatDefinition(entry);
    if (!heat) return false;
    const maximum = Math.max(0, nonNegativeInteger(heat.max, 99));
    const current = nonNegativeInteger(entry.heat);
    return maximum - current >= amount;
  });

  if (!targets.length) {
    source.heat = nonNegativeInteger(input.before, source.heat);
    return { transfer: true, refunded: true };
  }

  player.pendingChoices = Array.isArray(player.pendingChoices) ? player.pendingChoices : [];
  player.pendingChoices.push({
    id: `choice_move_heat_${nonNegativeInteger(game?.turnSerial)}_${nonNegativeInteger(game?.eventSequence) + 1}_${player.pendingChoices.length}`,
    cardId: String(input.sourceCardId || input.cardId || source.id || ""),
    source: "moveHeat",
    title: "Move Heat",
    subtitle: `Choose another friendly Heat card to receive ${amount} Heat.`,
    createdAtMs: Date.now(),
    options: targets.map((entry, index) => {
      const card = registeredHeatCards.get(String(entry.id || ""));
      return {
        label: `${card?.name || "Heat card"} · Heat ${nonNegativeInteger(entry.heat)}`,
        description: `Move ${amount} Heat here.`,
        actionLabel: "MOVE HEAT",
        cardId: String(entry.id || ""),
        instanceId: String(entry.instanceId || ""),
        index,
        effect: {
          __heatChange: {
            instanceId: String(entry.instanceId || ""),
            delta: amount
          }
        }
      };
    })
  });
  input.method = "moveHeat-source";
  return { transfer: true, refunded: false };
}

export const MOVE_HEAT_RUNTIME_OFFSET = HEAT_TRANSFER_DELTA_OFFSET;
