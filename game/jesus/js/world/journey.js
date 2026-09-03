export const JOURNEY_NODES = [
  { id: "galilee", name: "Galilee Shore", region: "galilee", type: "standard", x: 8, y: 72, difficulty: 1, next: ["capernaum"] },
  { id: "capernaum", name: "Capernaum", region: "galilee", type: "standard", x: 21, y: 59, difficulty: 2, next: ["sea"] },
  { id: "sea", name: "Sea of Galilee", region: "galilee", type: "challenge", x: 34, y: 67, difficulty: 3, next: ["wilderness"] },
  { id: "wilderness", name: "Wilderness", region: "jerusalem", type: "checkpoint", x: 46, y: 52, difficulty: 3, next: ["miracle", "blessing"] },
  { id: "miracle", name: "Miracle Trial", region: "heaven", type: "trial", x: 57, y: 34, difficulty: 4, next: ["jerusalem"] },
  { id: "blessing", name: "Blessing Route", region: "jerusalem", type: "blessing", x: 59, y: 69, difficulty: 3, next: ["jerusalem"] },
  { id: "jerusalem", name: "Jerusalem", region: "jerusalem", type: "boss", boss: "templeJudge", x: 70, y: 51, difficulty: 4, next: ["gethsemane"] },
  { id: "gethsemane", name: "Gethsemane", region: "jerusalem", type: "story", x: 79, y: 68, difficulty: 4, next: ["rome"] },
  { id: "rome", name: "Roman Trial", region: "rome", type: "boss", boss: "warBeast", x: 87, y: 48, difficulty: 5, next: ["descent"] },
  { id: "descent", name: "Spiritual Descent", region: "hell", type: "challenge", x: 75, y: 26, difficulty: 5, next: ["hell"] },
  { id: "hell", name: "The Abyss", region: "hell", type: "boss", boss: "pitLord", x: 57, y: 15, difficulty: 6, next: ["ascension"] },
  { id: "ascension", name: "Ascension", region: "heaven", type: "trial", x: 37, y: 22, difficulty: 6, next: ["heaven"] },
  { id: "heaven", name: "Heaven", region: "heaven", type: "finale", boss: "celestialTrial", x: 18, y: 14, difficulty: 6, next: [] },
];

export const JOURNEY_BY_ID = Object.fromEntries(JOURNEY_NODES.map((node) => [node.id, node]));

export function createJourneySave() {
  return {
    version: 2,
    completed: [],
    currentNode: "galilee",
    checkpoint: null,
    permanentPowers: [],
    maxHearts: 4,
    updatedAt: Date.now(),
  };
}

export function reachableNodes(save) {
  const completed = new Set(save.completed || []);
  if (!completed.size) return ["galilee"];
  const reachable = new Set();
  for (const id of completed) {
    const node = JOURNEY_BY_ID[id];
    for (const next of node?.next || []) {
      if (!completed.has(next)) reachable.add(next);
    }
  }
  return [...reachable];
}

export function completeJourneyNode(save, nodeId) {
  const completed = new Set(save.completed || []);
  completed.add(nodeId);
  const next = JOURNEY_BY_ID[nodeId]?.next || [];
  return {
    ...save,
    completed: [...completed],
    currentNode: next[0] || nodeId,
    checkpoint: null,
    updatedAt: Date.now(),
  };
}

