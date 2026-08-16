const STORAGE_KEY = "hearthmouse.graphicsQuality";
const STYLE_ID = "hearthmouse-graphics-quality-style";
const MENU_CLASS = "hearthmouse-graphics-menu";

export const GRAPHICS_QUALITY_PROFILES = Object.freeze({
  low: Object.freeze({
    id: "low",
    label: "Low",
    maxPixelRatio: 1,
    shadowMapLimit: 256,
    shadows: false,
    description: "Best FPS · dynamic shadows off · 1× render scale",
  }),
  medium: Object.freeze({
    id: "medium",
    label: "Medium",
    maxPixelRatio: 1.35,
    shadowMapLimit: 1024,
    shadows: true,
    description: "Balanced · lighter shadow maps · reduced render scale",
  }),
  high: Object.freeze({
    id: "high",
    label: "High",
    maxPixelRatio: Infinity,
    shadowMapLimit: Infinity,
    shadows: true,
    description: "Full visuals · original Hearthmouse render settings",
  }),
});

const rendererBaselines = new WeakMap();
const shadowBaselines = new WeakMap();

export function normalizeGraphicsQuality(value) {
  const key = String(value ?? "").toLowerCase();
  return GRAPHICS_QUALITY_PROFILES[key] ? key : "high";
}

export function targetPixelRatioForQuality(quality, baselinePixelRatio = 1) {
  const profile = GRAPHICS_QUALITY_PROFILES[normalizeGraphicsQuality(quality)];
  const baseline = Number.isFinite(baselinePixelRatio) && baselinePixelRatio > 0 ? baselinePixelRatio : 1;
  return Math.min(baseline, profile.maxPixelRatio);
}

export function targetShadowMapSizeForQuality(quality, baselineSize = 1024) {
  const profile = GRAPHICS_QUALITY_PROFILES[normalizeGraphicsQuality(quality)];
  const baseline = Number.isFinite(baselineSize) && baselineSize > 0 ? baselineSize : 1024;
  return Math.min(baseline, profile.shadowMapLimit);
}

function getRendererBaseline(renderer) {
  let baseline = rendererBaselines.get(renderer);
  if (baseline) return baseline;
  baseline = {
    pixelRatio: renderer.getPixelRatio?.() ?? 1,
    shadowEnabled: !!renderer.shadowMap?.enabled,
    shadowAutoUpdate: renderer.shadowMap?.autoUpdate ?? true,
  };
  rendererBaselines.set(renderer, baseline);
  return baseline;
}

function getShadowBaseline(shadow) {
  let baseline = shadowBaselines.get(shadow);
  if (baseline) return baseline;
  baseline = {
    x: shadow.mapSize?.x ?? 1024,
    y: shadow.mapSize?.y ?? 1024,
  };
  shadowBaselines.set(shadow, baseline);
  return baseline;
}

function resizeShadowMap(shadow, quality) {
  if (!shadow?.mapSize) return;
  const baseline = getShadowBaseline(shadow);
  const targetX = targetShadowMapSizeForQuality(quality, baseline.x);
  const targetY = targetShadowMapSizeForQuality(quality, baseline.y);
  if (shadow.mapSize.x === targetX && shadow.mapSize.y === targetY) return;

  if (typeof shadow.mapSize.set === "function") shadow.mapSize.set(targetX, targetY);
  else {
    shadow.mapSize.x = targetX;
    shadow.mapSize.y = targetY;
  }
  if (shadow.map) {
    shadow.map.dispose?.();
    shadow.map = null;
  }
  shadow.needsUpdate = true;
}

function forEachShadowSource(engine, callback) {
  const roots = [
    engine?.world?.root,
    engine?.scene,
    engine?.playerView,
    ...(Array.isArray(engine?.world?.lights) ? engine.world.lights : []),
    ...(Array.isArray(engine?.lights) ? engine.lights : []),
  ].filter(Boolean);
  const seen = new Set();
  const visit = (object) => {
    if (!object || seen.has(object)) return;
    seen.add(object);
    if (object.shadow?.mapSize) callback(object.shadow, object);
  };

  for (const root of roots) {
    if (typeof root.traverse === "function") root.traverse(visit);
    else visit(root);
  }
}

export function applyGraphicsQuality(engine, requestedQuality) {
  const quality = normalizeGraphicsQuality(requestedQuality);
  const profile = GRAPHICS_QUALITY_PROFILES[quality];
  const renderer = engine?.renderer;
  if (!renderer) return quality;

  const baseline = getRendererBaseline(renderer);
  const pixelRatio = targetPixelRatioForQuality(quality, baseline.pixelRatio);
  if (typeof renderer.setPixelRatio === "function" && renderer.getPixelRatio?.() !== pixelRatio) {
    renderer.setPixelRatio(pixelRatio);
  }

  if (renderer.shadowMap) {
    const allowShadows = profile.shadows && baseline.shadowEnabled;
    renderer.shadowMap.enabled = allowShadows;
    renderer.shadowMap.autoUpdate = allowShadows ? baseline.shadowAutoUpdate : false;
    renderer.shadowMap.needsUpdate = allowShadows;
  }

  forEachShadowSource(engine, (shadow) => resizeShadowMap(shadow, quality));

  if (typeof document !== "undefined") document.body?.setAttribute("data-hearthmouse-graphics-quality", quality);
  return quality;
}

function readStoredQuality() {
  try {
    return normalizeGraphicsQuality(window.localStorage?.getItem(STORAGE_KEY));
  } catch {
    return "high";
  }
}

function storeQuality(quality) {
  try {
    window.localStorage?.setItem(STORAGE_KEY, quality);
  } catch {
    // Storage can be unavailable in private/restricted browser contexts.
  }
}

function gameIsOnMainMenu() {
  const engine = window.hearthmouseEngine;
  if (!engine || engine.disposed) return true;
  const phase = engine.snapshot?.phase;
  return phase !== "foraging" && phase !== "summary";
}

function installMenuStyles() {
  if (document.getElementById(STYLE_ID)) return;
  const style = document.createElement("style");
  style.id = STYLE_ID;
  style.textContent = `
    .${MENU_CLASS} {
      position: fixed;
      top: max(14px, env(safe-area-inset-top));
      right: max(14px, env(safe-area-inset-right));
      z-index: 10020;
      width: min(330px, calc(100vw - 28px));
      box-sizing: border-box;
      padding: 11px 12px 10px;
      border: 1px solid rgba(235, 214, 173, .28);
      border-radius: 12px;
      background: rgba(8, 13, 19, .88);
      box-shadow: 0 8px 28px rgba(0, 0, 0, .28);
      color: #f5ead5;
      font-family: inherit;
      backdrop-filter: blur(8px);
      -webkit-backdrop-filter: blur(8px);
    }
    .${MENU_CLASS}[hidden] { display: none !important; }
    .${MENU_CLASS}__heading {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 7px;
    }
    .${MENU_CLASS}__heading strong {
      font-size: 12px;
      letter-spacing: .14em;
      text-transform: uppercase;
    }
    .${MENU_CLASS}__heading span {
      font-size: 10px;
      opacity: .62;
      letter-spacing: .08em;
      text-transform: uppercase;
    }
    .${MENU_CLASS}__buttons {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 6px;
    }
    .${MENU_CLASS} button {
      min-height: 34px;
      border: 1px solid rgba(235, 214, 173, .22);
      border-radius: 8px;
      background: rgba(255, 255, 255, .055);
      color: inherit;
      font: inherit;
      font-size: 12px;
      font-weight: 700;
      cursor: pointer;
      touch-action: manipulation;
    }
    .${MENU_CLASS} button:hover,
    .${MENU_CLASS} button:focus-visible {
      border-color: rgba(245, 225, 187, .62);
      background: rgba(255, 255, 255, .1);
      outline: none;
    }
    .${MENU_CLASS} button[aria-pressed="true"] {
      border-color: rgba(245, 225, 187, .82);
      background: rgba(206, 167, 99, .2);
      box-shadow: inset 0 0 0 1px rgba(245, 225, 187, .12);
    }
    .${MENU_CLASS}__description {
      min-height: 14px;
      margin: 7px 1px 0;
      font-size: 10px;
      line-height: 1.35;
      opacity: .7;
    }
    @media (max-height: 540px), (max-width: 720px) {
      .${MENU_CLASS} {
        top: max(8px, env(safe-area-inset-top));
        right: max(8px, env(safe-area-inset-right));
        width: min(286px, calc(100vw - 16px));
        padding: 7px 8px;
        border-radius: 9px;
      }
      .${MENU_CLASS}__heading { margin-bottom: 5px; }
      .${MENU_CLASS}__heading strong { font-size: 10px; }
      .${MENU_CLASS}__heading span,
      .${MENU_CLASS}__description { display: none; }
      .${MENU_CLASS} button { min-height: 29px; font-size: 11px; }
    }
  `;
  document.head.appendChild(style);
}

function createGraphicsMenu() {
  const existing = document.querySelector(`.${MENU_CLASS}`);
  if (existing) return existing;

  installMenuStyles();
  const root = document.createElement("section");
  root.className = MENU_CLASS;
  root.setAttribute("aria-label", "Graphics quality settings");
  root.innerHTML = `
    <div class="${MENU_CLASS}__heading">
      <strong>Graphics Quality</strong>
      <span>Performance</span>
    </div>
    <div class="${MENU_CLASS}__buttons" role="group" aria-label="Graphics quality"></div>
    <p class="${MENU_CLASS}__description"></p>
  `;

  const buttons = root.querySelector(`.${MENU_CLASS}__buttons`);
  for (const profile of Object.values(GRAPHICS_QUALITY_PROFILES)) {
    const button = document.createElement("button");
    button.type = "button";
    button.dataset.graphicsQuality = profile.id;
    button.textContent = profile.label;
    button.setAttribute("aria-pressed", "false");
    buttons.appendChild(button);
  }
  document.body.appendChild(root);
  return root;
}

export function mountGraphicsQualityMenu() {
  if (typeof window === "undefined" || typeof document === "undefined") return null;
  const root = createGraphicsMenu();
  let selectedQuality = readStoredQuality();
  let lastEngine = null;
  let lastAppliedQuality = "";

  const renderSelection = () => {
    const profile = GRAPHICS_QUALITY_PROFILES[selectedQuality];
    root.querySelectorAll("[data-graphics-quality]").forEach((button) => {
      button.setAttribute("aria-pressed", String(button.dataset.graphicsQuality === selectedQuality));
    });
    const description = root.querySelector(`.${MENU_CLASS}__description`);
    if (description) description.textContent = profile.description;
  };

  const applyToCurrentEngine = (force = false) => {
    const engine = window.hearthmouseEngine;
    if (engine && !engine.disposed && (force || engine !== lastEngine || selectedQuality !== lastAppliedQuality)) {
      applyGraphicsQuality(engine, selectedQuality);
      lastEngine = engine;
      lastAppliedQuality = selectedQuality;
    }
    root.hidden = !gameIsOnMainMenu();
  };

  root.addEventListener("click", (event) => {
    const button = event.target.closest("[data-graphics-quality]");
    if (!button) return;
    selectedQuality = normalizeGraphicsQuality(button.dataset.graphicsQuality);
    storeQuality(selectedQuality);
    renderSelection();
    applyToCurrentEngine(true);
    window.dispatchEvent(new CustomEvent("hearthmouse:graphics-quality", { detail: { quality: selectedQuality } }));
  });

  window.HearthmouseGraphicsQuality = {
    profiles: GRAPHICS_QUALITY_PROFILES,
    get quality() { return selectedQuality; },
    set quality(value) {
      selectedQuality = normalizeGraphicsQuality(value);
      storeQuality(selectedQuality);
      renderSelection();
      applyToCurrentEngine(true);
    },
    apply() { applyToCurrentEngine(true); },
  };

  renderSelection();
  applyToCurrentEngine(true);
  window.setInterval(applyToCurrentEngine, 400);
  return root;
}

function bootGraphicsQualityMenu() {
  if (document.querySelector(`.${MENU_CLASS}`)) return;
  mountGraphicsQualityMenu();
}

if (typeof window !== "undefined" && typeof document !== "undefined") {
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", bootGraphicsQualityMenu, { once: true });
  else bootGraphicsQualityMenu();
}
