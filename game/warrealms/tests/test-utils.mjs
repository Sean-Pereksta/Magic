
let cardPackPromise;

export function loadCardPack() {
  if (!cardPackPromise) {
    cardPackPromise = import("../../warrealms-pack/warrealms-cards.js");
  }
  return cardPackPromise;
}

export function eventGame(overrides = {}) {
  return {
    turnSerial: 1,
    round: 1,
    resolutionQueue: [],
    resolutionWarnings: [],
    eventHistory: [],
    eventWarnings: [],
    players: [],
    ...overrides
  };
}
