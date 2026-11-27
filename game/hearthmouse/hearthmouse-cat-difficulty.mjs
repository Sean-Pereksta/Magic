import { registerFrameVisualStage } from "./hearthmouse-performance-manager.mjs";

export const CAT_DIFFICULTIES = Object.freeze({
  gentle: Object.freeze({
    label: "Gentle", description: "Cats rely on direct pursuit and give you more time to dodge.",
    detection: 0.78, ambushChance: 0, stalkChance: 0.12, interceptChance: 0.12,
    tacticCooldown: 14, stalkTime: 4, leadTime: 0, searches: 0, searchDistance: 0,
    habitRate: 0.65, watchTime: 0, distractionChance: 1.35,
    pounceRange: 1.35, pounceLead: 0.03, windupScale: 1.22, recovery: 0.83,
  }),
  standard: Object.freeze({
    label: "Standard", description: "Cats stalk, cut off a visible escape and search where they last saw you.",
    detection: 1.08, ambushChance: 0.22, stalkChance: 0.51, interceptChance: 0.88,
    tacticCooldown: 10, stalkTime: 4.8, leadTime: 0.4, searches: 1, searchDistance: 0.85,
    habitRate: 1, watchTime: 4, distractionChance: 1,
    pounceRange: 1.48, pounceLead: 0.075, windupScale: 1, recovery: 0.65,
  }),
  hard: Object.freeze({
    label: "Hard", description: "Cats use cover, probe escape routes and learn which food you use to distract them.",
    detection: 1.2, ambushChance: 0.29, stalkChance: 0.54, interceptChance: 0.94,
    tacticCooldown: 8, stalkTime: 4.1, leadTime: 0.49, searches: 2, searchDistance: 1.15,
    habitRate: 1.3, watchTime: 5, distractionChance: 0.73,
    pounceRange: 1.62, pounceLead: 0.11, windupScale: 0.86, recovery: 0.57,
  }),
  nightmare: Object.freeze({
    label: "Nightmare", description: "Cats plan cutoffs, check multiple escape routes and keep watch over familiar approaches.",
    detection: 1.36, ambushChance: 0.34, stalkChance: 0.58, interceptChance: 0.99,
    tacticCooldown: 6.5, stalkTime: 3.4, leadTime: 0.56, searches: 3, searchDistance: 1.45,
    habitRate: 1.65, watchTime: 6, distractionChance: 0.48,
    pounceRange: 1.78, pounceLead: 0.15, windupScale: 0.74, recovery: 0.49,
  }),
});

const DEFAULT_DIFFICULTY = "standard";
const STORAGE_KEY = "hearthmouse.cat-difficulty";
const menuPhases = new Set(["intro", "summary", "caught", "won", "lost"]);
const normalizeDifficulty = id => Object.hasOwn(CAT_DIFFICULTIES, id) ? id : DEFAULT_DIFFICULTY;

export function selectedCatDifficulty(engine) {
  if (!engine.catDifficultySelection) {
    let stored;
    try { stored = globalThis.localStorage?.getItem(STORAGE_KEY); }
    catch { /* A restricted browser can still use the in-memory preference. */ }
    engine.catDifficultySelection = normalizeDifficulty(stored);
  }
  return engine.catDifficultySelection;
}

export function applyCatDifficulty(engine) {
  engine.catDifficulty = selectedCatDifficulty(engine);
}

export function catDifficultyProfile(engine) {
  return CAT_DIFFICULTIES[normalizeDifficulty(engine.catDifficulty)];
}

function mountDifficultyControl(engine) {
  if (typeof document === "undefined" || !menuPhases.has(engine.snapshot?.phase)) return;
  const action = document.querySelector(".game-shell .title-copy .primary-action, .game-shell .summary-card .primary-action, .game-shell .end-card .primary-action");
  if (!action || action.parentNode.querySelector(".hearthmouse-cat-difficulty")) return;

  const label = document.createElement("label");
  const title = document.createElement("span");
  const select = document.createElement("select");
  const description = document.createElement("small");
  title.textContent = "Cat difficulty";
  label.className = "hearthmouse-cat-difficulty";
  select.setAttribute("aria-describedby", "hearthmouse-cat-difficulty-description");
  description.id = "hearthmouse-cat-difficulty-description";
  for (const [id, profile] of Object.entries(CAT_DIFFICULTIES)) {
    const option = document.createElement("option");
    option.value = id;
    option.textContent = profile.label;
    select.append(option);
  }
  select.value = selectedCatDifficulty(engine);
  description.textContent = CAT_DIFFICULTIES[select.value].description;
  select.addEventListener("change", () => {
    const id = normalizeDifficulty(select.value);
    engine.catDifficultySelection = id;
    description.textContent = CAT_DIFFICULTIES[id].description;
    try { globalThis.localStorage?.setItem(STORAGE_KEY, id); }
    catch { /* Keep the selection for this session. */ }
  });
  label.append(title, select);
  const control = document.createElement("div");
  control.className = "hearthmouse-cat-difficulty-control";
  control.append(label, description);
  action.before(control);
}

registerFrameVisualStage("cat-difficulty-menu", mountDifficultyControl, 0.15);
