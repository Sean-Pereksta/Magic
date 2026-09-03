import {
  showPlayLauncher as showCorePlayLauncher,
  showWarTable as showCoreWarTable
} from "./play-launcher-core.js?v=1";

const TEST_LAB_BUTTON_ID = "warrealmsTestLabButton";
let injectionQueued = false;

function openTestLab() {
  window.location.assign("./warrealms/test-lab.html");
}

function injectTestLabAction() {
  injectionQueued = false;
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

function queueTestLabInjection() {
  if (injectionQueued || typeof document === "undefined") return;
  injectionQueued = true;
  const schedule = typeof requestAnimationFrame === "function" ? requestAnimationFrame : callback => setTimeout(callback, 0);
  schedule(injectTestLabAction);
}

function installTestLabLauncherBridge() {
  if (typeof document === "undefined") return;
  const observe = () => {
    if (!document.body) return;
    const observer = new MutationObserver(queueTestLabInjection);
    observer.observe(document.body, { childList: true, subtree: true });
    queueTestLabInjection();
  };
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", observe, { once: true });
  else observe();
}

function showPlayLauncher(options = {}) {
  const result = showCorePlayLauncher(options);
  queueTestLabInjection();
  return result;
}

function showWarTable(options = {}) {
  const result = showCoreWarTable(options);
  queueTestLabInjection();
  return result;
}

installTestLabLauncherBridge();

export { showPlayLauncher, showWarTable };
