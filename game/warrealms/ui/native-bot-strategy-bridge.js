import {
  BASE_NATIVE_STRATEGY_IDS,
  EXPANDED_BOT_STRATEGY_IDS,
  NATIVE_BOT_STRATEGY_EXTENSIONS
} from "./bot-strategy-expansion.js";

const freeze = Object.freeze;

const SAFE_NATIVE_EXTENSIONS = freeze({
  ...NATIVE_BOT_STRATEGY_EXTENSIONS,
  reactor: freeze({
    ...NATIVE_BOT_STRATEGY_EXTENSIONS.reactor,
    // Heat targeting uses structured { amount, target } effects. The native
    // requirement matcher only counts scalar effects, so don't impose a quota
    // that it cannot legally validate. Strategy weights still prefer Heat
    // support while Test Lab uses the full recursive Heat detector.
    minimumAbilities: freeze([])
  }),
  architect: freeze({
    ...NATIVE_BOT_STRATEGY_EXTENSIONS.architect,
    // Construction and repair are structured effects in the live pack. Keep
    // the reliable Base quota here and let the strategy's Base/Attachment bias
    // and construction weights choose the support cards without false failures.
    minimumAbilities: freeze([
      freeze({ types: freeze(["base"]), min: 10 })
    ])
  })
});

function looksLikeNativeWarRealmsRegistry(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  if (EXPANDED_BOT_STRATEGY_IDS.some(id => Object.prototype.hasOwnProperty.call(value, id))) return false;
  if (!BASE_NATIVE_STRATEGY_IDS.every(id => Object.prototype.hasOwnProperty.call(value, id))) return false;
  return value.vanguard?.name === "Vanguard" && value.engine?.name === "Engine" && value.fleet?.name === "Fleet";
}

function extendNativeRegistry(registry) {
  const result = { ...registry };
  for (const strategyId of EXPANDED_BOT_STRATEGY_IDS) result[strategyId] = SAFE_NATIVE_EXTENSIONS[strategyId];
  return freeze(result);
}

let bridgeInstalled = false;

export function installNativeBotStrategyBridge() {
  if (bridgeInstalled || typeof Object.freeze !== "function") return;
  bridgeInstalled = true;
  const originalFreeze = Object.freeze;
  let active = true;

  function bridgedFreeze(value) {
    if (active && looksLikeNativeWarRealmsRegistry(value)) {
      active = false;
      if (Object.freeze === bridgedFreeze) Object.freeze = originalFreeze;
      return extendNativeRegistry(value);
    }
    return originalFreeze(value);
  }

  Object.freeze = bridgedFreeze;
  queueMicrotask(() => {
    active = false;
    if (Object.freeze === bridgedFreeze) Object.freeze = originalFreeze;
  });
}
