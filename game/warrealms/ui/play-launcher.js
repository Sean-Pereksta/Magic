import "./armory-ui-polish.js?v=1";
import {
  showPlayLauncher as showCorePlayLauncher,
  showWarTable as showCoreWarTable
} from "./play-launcher-core.js?v=1";
import {
  EXPANDED_BOT_STRATEGIES,
  NATIVE_STRATEGY_ORDER,
  expandedStrategyById
} from "./bot-strategy-expansion.js?v=1";
import { installNativeBotStrategyBridge } from "./native-bot-strategy-bridge.js?v=1";

installNativeBotStrategyBridge();

const TEST_LAB_BUTTON_ID = "warrealmsTestLabButton";
const EXPANDED_STRATEGY_CLASS = "wrExpandedStrategy";
let injectionQueued = false;
let selectedExpandedStrategyId = "";

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, character => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[character]);
}

function openTestLab() {
  window.location.assign("./warrealms/test-lab.html");
}

function expandedStrategyCard(strategy, selected) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = `wrStrategyCard ${EXPANDED_STRATEGY_CLASS}${selected ? " active" : ""}`;
  button.dataset.wrStrategy = strategy.id;
  button.style.setProperty("--wr-strategy", strategy.color);
  button.style.setProperty("--wr-strategy2", strategy.secondary);
  button.innerHTML = `
    <span class="wrStrategyTop"><span class="wrStrategyIcon" aria-hidden="true">${escapeHtml(strategy.icon)}</span><span class="wrStrategyNames"><strong>${escapeHtml(strategy.name)}</strong><span>${escapeHtml(strategy.factions)}</span></span></span>
    <span class="wrStrategyDeck">${escapeHtml(strategy.deck)}</span><span class="wrStrategyDesc">${escapeHtml(strategy.description)}</span>
  `;
  return button;
}

function patchExpandedSelection(launcher) {
  const strategy = expandedStrategyById(selectedExpandedStrategyId);
  if (!strategy) return;
  const easy = launcher.querySelector('[data-wr-difficulty="easy"]');
  if (easy) easy.disabled = true;

  const difficultyGrid = launcher.querySelector(".wrDifficultyGrid");
  if (difficultyGrid && !launcher.querySelector(".wrStrategyNote")) {
    const note = document.createElement("p");
    note.className = "wrStrategyNote";
    note.textContent = "Easy intentionally uses random purchases, so named strategies begin at Medium.";
    difficultyGrid.after(note);
  }

  const summary = launcher.querySelector(".wrSelectionSummary");
  if (summary && !summary.dataset.expandedStrategySummary) {
    const difficultyName = launcher.querySelector(".wrDifficulty.active strong")?.textContent || "Medium";
    summary.dataset.expandedStrategySummary = strategy.id;
    summary.innerHTML = `<strong>${escapeHtml(strategy.name)} · ${escapeHtml(difficultyName)}</strong><br>${escapeHtml(strategy.factions)} — ${escapeHtml(strategy.deck)}`;
  }
}

function injectExpandedStrategies() {
  if (typeof document === "undefined") return;
  const launcher = document.getElementById("warrealmsPlayLauncher");
  if (!launcher || launcher.hidden) return;
  const grid = launcher.querySelector(".wrStrategyGrid");
  if (!grid) return;

  const moreButton = launcher.querySelector("[data-wr-more-strategies]");
  const expanded = moreButton?.textContent?.includes("Show fewer") === true;
  const wanted = expanded
    ? EXPANDED_BOT_STRATEGIES
    : EXPANDED_BOT_STRATEGIES.filter(strategy => strategy.id === selectedExpandedStrategyId);

  launcher.querySelectorAll(`.${EXPANDED_STRATEGY_CLASS}`).forEach(button => {
    if (!wanted.some(strategy => strategy.id === button.dataset.wrStrategy)) button.remove();
  });

  const randomCard = grid.querySelector('[data-wr-strategy="random"]');
  for (const strategy of wanted) {
    let button = grid.querySelector(`[data-wr-strategy="${strategy.id}"]`);
    if (!button) {
      button = expandedStrategyCard(strategy, strategy.id === selectedExpandedStrategyId);
      if (!expanded && randomCard) randomCard.after(button);
      else grid.appendChild(button);
    }
    button.classList.toggle("active", strategy.id === selectedExpandedStrategyId);
  }

  if (moreButton && !expanded) {
    const label = `More strategies · ${NATIVE_STRATEGY_ORDER.length}`;
    if (moreButton.textContent !== label) moreButton.textContent = label;
  }
  patchExpandedSelection(launcher);
}

function injectTestLabAction() {
  if (typeof document === "undefined") return;
  const launcher = document.getElementById("warrealmsPlayLauncher");
  if (!launcher || launcher.hidden) return;
  const warTableStack = launcher.querySelector(".wrWarTableStack");
  if (!warTableStack || document.getElementById(TEST_LAB_BUTTON_ID)) return;

  const button = document.createElement("button");
  button.type = "button";
  button.id = TEST_LAB_BUTTON_ID;
  button.className = "wrWarAction";
  button.setAttribute("aria-label", "Open War Realms Test Lab");
  button.innerHTML = `
    <strong>Test Lab</strong>
    <span>Run accelerated Warbot-v-Warbot balance simulations and watch purchased-card win rates move live.</span>
    <em>Balance simulator</em>
    <span class="wrWarActionIcon" aria-hidden="true">Σ</span>
  `;
  button.addEventListener("click", event => {
    event.preventDefault();
    event.stopPropagation();
    openTestLab();
  });
  warTableStack.appendChild(button);
}

function injectLauncherExtensions() {
  injectionQueued = false;
  injectTestLabAction();
  injectExpandedStrategies();
}

function queueLauncherInjection() {
  if (injectionQueued || typeof document === "undefined") return;
  injectionQueued = true;
  const schedule = typeof requestAnimationFrame === "function" ? requestAnimationFrame : callback => setTimeout(callback, 0);
  schedule(injectLauncherExtensions);
}

function forceExpandedStrategy(strategyId, run) {
  const index = NATIVE_STRATEGY_ORDER.indexOf(strategyId);
  if (index < 0) return run();
  const originalRandom = Math.random;
  const forcedValue = (index + .5) / NATIVE_STRATEGY_ORDER.length;
  let restored = false;
  const restore = () => {
    if (restored) return;
    restored = true;
    if (Math.random === forcedRandom) Math.random = originalRandom;
  };
  function forcedRandom() {
    restore();
    return forcedValue;
  }
  Math.random = forcedRandom;
  try {
    return run();
  } finally {
    queueMicrotask(restore);
  }
}

function startExpandedSinglePlayer(strategyId) {
  const nativeButton = document.getElementById("botChallengeBtn");
  if (!nativeButton) return;
  const launcher = document.getElementById("warrealmsPlayLauncher");
  const difficulty = launcher?.querySelector(".wrDifficulty.active")?.dataset.wrDifficulty || "medium";
  nativeButton.click();
  const nativeDifficulty = document.querySelector(`[data-act="start-bot"][data-difficulty="${difficulty === "easy" ? "medium" : difficulty}"]`);
  if (!(nativeDifficulty instanceof HTMLElement)) return;
  if (launcher) launcher.hidden = true;
  forceExpandedStrategy(strategyId, () => nativeDifficulty.click());
}

function handleExpandedStrategyClick(event) {
  if (!(event.target instanceof Element)) return;
  const strategyButton = event.target.closest("[data-wr-strategy]");
  if (strategyButton) {
    const strategyId = strategyButton.dataset.wrStrategy || "";
    selectedExpandedStrategyId = expandedStrategyById(strategyId) ? strategyId : "";
    queueLauncherInjection();
  }

  const startButton = event.target.closest("[data-wr-start-single]");
  if (startButton && selectedExpandedStrategyId) {
    event.preventDefault();
    event.stopPropagation();
    event.stopImmediatePropagation();
    startExpandedSinglePlayer(selectedExpandedStrategyId);
  }
}

function installLauncherBridge() {
  if (typeof document === "undefined") return;
  const observe = () => {
    if (!document.body) return;
    const observer = new MutationObserver(queueLauncherInjection);
    observer.observe(document.body, { childList: true, subtree: true });
    document.addEventListener("click", handleExpandedStrategyClick, true);
    queueLauncherInjection();
  };
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", observe, { once: true });
  else observe();
}

function showPlayLauncher(options = {}) {
  const result = showCorePlayLauncher(options);
  queueLauncherInjection();
  return result;
}

function showWarTable(options = {}) {
  const result = showCoreWarTable(options);
  queueLauncherInjection();
  return result;
}

installLauncherBridge();

export { showPlayLauncher, showWarTable };
