import fs from "node:fs/promises";

let cardPackPromise;

export function loadCardPack() {
  if (!cardPackPromise) {
    cardPackPromise = (async () => {
      const wrapperUrl = new URL("../../warrealms-pack/warrealms-cards.js", import.meta.url);
      const baseUrl = new URL("../../warrealms-pack/warrealms-cards-base.js", import.meta.url);
      const [wrapperSource, baseSource] = await Promise.all([
        fs.readFile(wrapperUrl, "utf8"),
        fs.readFile(baseUrl, "utf8")
      ]);
      const baseDataUrl = `data:text/javascript;base64,${Buffer.from(baseSource).toString("base64")}`;
      const source = wrapperSource.replaceAll("./warrealms-cards-base.js", baseDataUrl);
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
