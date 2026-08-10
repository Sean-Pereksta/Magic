import fs from "node:fs/promises";

let cardPackPromise;

export function loadCardPack() {
  if (!cardPackPromise) {
    cardPackPromise = (async () => {
      const url = new URL("../../warrealms-pack/warrealms-cards.js", import.meta.url);
      const source = await fs.readFile(url, "utf8");
      return import(`data:text/javascript;base64,${Buffer.from(source).toString("base64")}`);
    })();
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
