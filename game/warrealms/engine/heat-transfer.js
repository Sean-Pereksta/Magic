const HEAT_TRANSFER_DELTA_OFFSET = 1000000;
let pendingTransferIntents = 0;

function positiveInteger(value, fallback = 1) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(1, Math.floor(number)) : fallback;
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

export const MOVE_HEAT_RUNTIME_OFFSET = HEAT_TRANSFER_DELTA_OFFSET;
