export function allyActivationState(activeFactionCards = 0) {
  const active = Math.max(0, Math.floor(Number(activeFactionCards) || 0));
  return {
    active,
    ally: active >= 2,
    doubleAlly: active >= 3,
    additionalFactionCards: Math.max(0, active - 1)
  };
}
