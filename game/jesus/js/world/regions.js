export const REGIONS = {
  galilee: {
    name: "Galilee",
    lesson: "Movement and momentum",
    enemyPool: ["wanderer", "hound"],
    mechanics: ["wide-landings", "moving-platforms"],
  },
  rome: {
    name: "Rome",
    lesson: "Speed and horizontal traversal",
    enemyPool: ["legionary", "shield-bearer", "javelin"],
    mechanics: ["collapsing-arches", "charges", "rooflines"],
  },
  jerusalem: {
    name: "Jerusalem",
    lesson: "Verticality and precision",
    enemyPool: ["temple-guard", "zealot", "stone-guardian"],
    mechanics: ["stairs", "towers", "vertical-movers"],
  },
  hell: {
    name: "Hell",
    lesson: "Readable hazard mastery",
    enemyPool: ["imp", "brim-hound", "fire-skull"],
    mechanics: ["lava", "fire-jets", "timed-platforms"],
  },
  heaven: {
    name: "Heaven",
    lesson: "Movement mastery",
    enemyPool: ["wraith"],
    mechanics: ["clouds", "light-bridges", "vertical-ascent"],
  },
};

export function regionForDepth(depth) {
  const rotation = ["galilee", "rome", "jerusalem", "hell", "heaven"];
  return rotation[Math.floor(Math.max(0, depth) / 3) % rotation.length];
}

