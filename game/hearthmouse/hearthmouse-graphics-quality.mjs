const STORAGE_KEY = "hearthmouse.graphicsQuality";
const CONTROLLER_STORAGE_KEY = "hearthmouse.controllerSettings.v1";
const STYLE_ID = "hearthmouse-graphics-quality-style";
const CONTROLLER_STYLE_ID = "hearthmouse-controller-settings-style";
const MENU_CLASS = "hearthmouse-graphics-menu";
const CONTROLLER_DIALOG_CLASS = "hearthmouse-controller-dialog";

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

const DEFAULT_BINDINGS = Object.freeze({
  moveAxes: Object.freeze([0, 1]),
  lookAxes: Object.freeze([2, 3]),
  jump: 0,
  interact: 2,
  rest: 3,
  sprint: 7,
  creep: 6,
  menuConfirm: 9,
});

export const DEFAULT_CONTROLLER_SETTINGS = Object.freeze({
  enabled: true,
  deadzone: 0.16,
  lookSensitivity: 1,
  preferredGamepadId: "",
  preferredGamepadIndex: null,
  bindings: DEFAULT_BINDINGS,
});

const CONTROLLER_ACTIONS = Object.freeze([
  Object.freeze({ id: "moveAxes", label: "Move", type: "axes" }),
  Object.freeze({ id: "lookAxes", label: "Look", type: "axes" }),
  Object.freeze({ id: "jump", label: "Jump / Menu Select", type: "button" }),
  Object.freeze({ id: "interact", label: "Interact / Use", type: "button" }),
  Object.freeze({ id: "rest", label: "Rest Until Dawn", type: "button" }),
  Object.freeze({ id: "sprint", label: "Sprint", type: "button" }),
  Object.freeze({ id: "creep", label: "Creep", type: "button" }),
  Object.freeze({ id: "menuConfirm", label: "Menu Select (alternate)", type: "button" }),
]);

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

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function normalizeButtonIndex(value, fallback) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= 0 && parsed <= 63 ? parsed : fallback;
}

function normalizeAxes(value, fallback) {
  if (!Array.isArray(value) || value.length !== 2) return [...fallback];
  const first = Number(value[0]);
  const second = Number(value[1]);
  if (!Number.isInteger(first) || !Number.isInteger(second) || first < 0 || second < 0 || first > 31 || second > 31 || first === second) {
    return [...fallback];
  }
  return [first, second];
}

export function normalizeControllerSettings(value = {}) {
  const bindings = value?.bindings ?? {};
  return {
    enabled: value?.enabled !== false,
    deadzone: clamp(Number.isFinite(Number(value?.deadzone)) ? Number(value.deadzone) : DEFAULT_CONTROLLER_SETTINGS.deadzone, 0.05, 0.45),
    lookSensitivity: clamp(
      Number.isFinite(Number(value?.lookSensitivity)) ? Number(value.lookSensitivity) : DEFAULT_CONTROLLER_SETTINGS.lookSensitivity,
      0.4,
      2,
    ),
    preferredGamepadId: typeof value?.preferredGamepadId === "string" ? value.preferredGamepadId : "",
    preferredGamepadIndex: Number.isInteger(value?.preferredGamepadIndex) && value.preferredGamepadIndex >= 0
      ? value.preferredGamepadIndex
      : null,
    bindings: {
      moveAxes: normalizeAxes(bindings.moveAxes, DEFAULT_BINDINGS.moveAxes),
      lookAxes: normalizeAxes(bindings.lookAxes, DEFAULT_BINDINGS.lookAxes),
      jump: normalizeButtonIndex(bindings.jump, DEFAULT_BINDINGS.jump),
      interact: normalizeButtonIndex(bindings.interact, DEFAULT_BINDINGS.interact),
      rest: normalizeButtonIndex(bindings.rest, DEFAULT_BINDINGS.rest),
      sprint: normalizeButtonIndex(bindings.sprint, DEFAULT_BINDINGS.sprint),
      creep: normalizeButtonIndex(bindings.creep, DEFAULT_BINDINGS.creep),
      menuConfirm: normalizeButtonIndex(bindings.menuConfirm, DEFAULT_BINDINGS.menuConfirm),
    },
  };
}

export function applyControllerDeadzone(x, y, deadzone = DEFAULT_CONTROLLER_SETTINGS.deadzone) {
  const safeX = Number.isFinite(Number(x)) ? Number(x) : 0;
  const safeY = Number.isFinite(Number(y)) ? Number(y) : 0;
  const zone = clamp(Number(deadzone) || DEFAULT_CONTROLLER_SETTINGS.deadzone, 0, 0.95);
  const magnitude = Math.min(1, Math.hypot(safeX, safeY));
  if (magnitude <= zone || magnitude <= 1e-6) return { x: 0, y: 0, magnitude: 0 };
  const scaledMagnitude = (magnitude - zone) / (1 - zone);
  const scale = scaledMagnitude / magnitude;
  return { x: safeX * scale, y: safeY * scale, magnitude: scaledMagnitude };
}

export function controllerAxisVector(gamepad, axes, deadzone = DEFAULT_CONTROLLER_SETTINGS.deadzone) {
  const pair = normalizeAxes(axes, DEFAULT_BINDINGS.moveAxes);
  return applyControllerDeadzone(gamepad?.axes?.[pair[0]] ?? 0, gamepad?.axes?.[pair[1]] ?? 0, deadzone);
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

function readControllerSettings() {
  try {
    const raw = window.localStorage?.getItem(CONTROLLER_STORAGE_KEY);
    return normalizeControllerSettings(raw ? JSON.parse(raw) : {});
  } catch {
    return normalizeControllerSettings({});
  }
}

function storeControllerSettings(settings) {
  try {
    window.localStorage?.setItem(CONTROLLER_STORAGE_KEY, JSON.stringify(settings));
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
      width: min(340px, calc(100vw - 28px));
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
    .${MENU_CLASS}__section-label {
      margin: 7px 1px 6px;
      font-size: 9px;
      font-weight: 800;
      letter-spacing: .14em;
      text-transform: uppercase;
      opacity: .58;
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
    .${MENU_CLASS}__controller-row {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px;
      align-items: center;
      margin-top: 9px;
      padding-top: 9px;
      border-top: 1px solid rgba(255, 255, 255, .09);
    }
    .${MENU_CLASS}__controller-row button {
      width: 100%;
      text-align: left;
      padding: 0 10px;
    }
    .${MENU_CLASS}__controller-status {
      max-width: 120px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      font: 700 9px/1.2 ui-sans-serif, system-ui, sans-serif;
      letter-spacing: .08em;
      text-transform: uppercase;
      opacity: .66;
    }
    .${MENU_CLASS}__controller-status[data-connected="true"] { color: #d9c17d; opacity: .9; }
    @media (max-height: 540px), (max-width: 720px) {
      .${MENU_CLASS} {
        top: max(8px, env(safe-area-inset-top));
        right: max(8px, env(safe-area-inset-right));
        width: min(300px, calc(100vw - 16px));
        padding: 7px 8px;
        border-radius: 9px;
      }
      .${MENU_CLASS}__heading { margin-bottom: 5px; }
      .${MENU_CLASS}__heading strong { font-size: 10px; }
      .${MENU_CLASS}__heading span,
      .${MENU_CLASS}__description,
      .${MENU_CLASS}__section-label { display: none; }
      .${MENU_CLASS} button { min-height: 29px; font-size: 11px; }
      .${MENU_CLASS}__controller-row { margin-top: 6px; padding-top: 6px; }
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
  root.setAttribute("aria-label", "Hearthmouse settings");
  root.innerHTML = `
    <div class="${MENU_CLASS}__heading">
      <strong>Settings</strong>
      <span>Graphics + Controls</span>
    </div>
    <div class="${MENU_CLASS}__section-label">Graphics Quality</div>
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

function installControllerStyles() {
  if (document.getElementById(CONTROLLER_STYLE_ID)) return;
  const style = document.createElement("style");
  style.id = CONTROLLER_STYLE_ID;
  style.textContent = `
    .${CONTROLLER_DIALOG_CLASS} {
      position: fixed;
      inset: 0;
      z-index: 10060;
      display: grid;
      place-items: center;
      padding: 18px;
      background: rgba(1, 4, 7, .72);
      backdrop-filter: blur(7px);
      -webkit-backdrop-filter: blur(7px);
      color: #f5ead5;
      font-family: Georgia, "Times New Roman", serif;
    }
    .${CONTROLLER_DIALOG_CLASS}[hidden] { display: none !important; }
    .${CONTROLLER_DIALOG_CLASS}__panel {
      width: min(680px, calc(100vw - 28px));
      max-height: min(760px, calc(100vh - 28px));
      overflow: auto;
      box-sizing: border-box;
      border: 1px solid rgba(235, 214, 173, .35);
      border-radius: 14px;
      background: linear-gradient(145deg, rgba(9, 14, 19, .98), rgba(19, 22, 22, .97));
      box-shadow: 0 26px 90px rgba(0, 0, 0, .62), inset 0 0 44px rgba(217, 179, 97, .025);
      padding: 20px;
    }
    .${CONTROLLER_DIALOG_CLASS}__header {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 14px;
    }
    .${CONTROLLER_DIALOG_CLASS}__eyebrow {
      margin: 0 0 5px;
      color: #d9b361;
      font: 800 9px/1.2 ui-sans-serif, system-ui, sans-serif;
      letter-spacing: .2em;
      text-transform: uppercase;
    }
    .${CONTROLLER_DIALOG_CLASS} h2 {
      margin: 0;
      font-size: clamp(1.65rem, 4vw, 2.45rem);
      font-weight: 400;
      letter-spacing: .03em;
    }
    .${CONTROLLER_DIALOG_CLASS}__close,
    .${CONTROLLER_DIALOG_CLASS} button,
    .${CONTROLLER_DIALOG_CLASS} select,
    .${CONTROLLER_DIALOG_CLASS} input { font: inherit; }
    .${CONTROLLER_DIALOG_CLASS} button,
    .${CONTROLLER_DIALOG_CLASS} select {
      border: 1px solid rgba(235, 214, 173, .24);
      border-radius: 8px;
      background: rgba(255, 255, 255, .055);
      color: #f5ead5;
    }
    .${CONTROLLER_DIALOG_CLASS} button {
      min-height: 36px;
      padding: 7px 11px;
      cursor: pointer;
    }
    .${CONTROLLER_DIALOG_CLASS} button:hover,
    .${CONTROLLER_DIALOG_CLASS} button:focus-visible,
    .${CONTROLLER_DIALOG_CLASS} select:focus-visible {
      outline: none;
      border-color: rgba(245, 225, 187, .7);
      background: rgba(255, 255, 255, .1);
    }
    .${CONTROLLER_DIALOG_CLASS}__close {
      min-width: 40px;
      font-size: 18px;
      line-height: 1;
    }
    .${CONTROLLER_DIALOG_CLASS}__connection {
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(170px, .65fr);
      gap: 10px;
      align-items: center;
      padding: 12px;
      border: 1px solid rgba(255, 255, 255, .09);
      border-radius: 10px;
      background: rgba(255, 255, 255, .025);
    }
    .${CONTROLLER_DIALOG_CLASS}__connection strong {
      display: block;
      margin-bottom: 3px;
      font: 800 10px/1.3 ui-sans-serif, system-ui, sans-serif;
      letter-spacing: .12em;
      text-transform: uppercase;
    }
    .${CONTROLLER_DIALOG_CLASS}__connection span {
      display: block;
      font-size: 12px;
      line-height: 1.35;
      opacity: .72;
    }
    .${CONTROLLER_DIALOG_CLASS}__connection select {
      width: 100%;
      min-height: 38px;
      padding: 5px 8px;
    }
    .${CONTROLLER_DIALOG_CLASS}__connection select option { color: #111; }
    .${CONTROLLER_DIALOG_CLASS}__toggle-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      margin: 12px 0 8px;
      padding: 10px 2px;
      font: 700 11px/1.3 ui-sans-serif, system-ui, sans-serif;
      letter-spacing: .08em;
      text-transform: uppercase;
    }
    .${CONTROLLER_DIALOG_CLASS}__toggle-row input { width: 20px; height: 20px; accent-color: #d9b361; }
    .${CONTROLLER_DIALOG_CLASS}__mapping {
      margin-top: 8px;
      border-top: 1px solid rgba(255, 255, 255, .1);
    }
    .${CONTROLLER_DIALOG_CLASS}__mapping-row {
      display: grid;
      grid-template-columns: minmax(150px, .7fr) minmax(190px, 1fr);
      gap: 12px;
      align-items: center;
      min-height: 51px;
      border-bottom: 1px solid rgba(255, 255, 255, .075);
    }
    .${CONTROLLER_DIALOG_CLASS}__mapping-row > span {
      font: 700 11px/1.3 ui-sans-serif, system-ui, sans-serif;
      letter-spacing: .055em;
      text-transform: uppercase;
      opacity: .84;
    }
    .${CONTROLLER_DIALOG_CLASS}__map-button {
      width: 100%;
      text-align: left;
      display: flex;
      justify-content: space-between;
      gap: 10px;
    }
    .${CONTROLLER_DIALOG_CLASS}__map-button[data-capturing="true"] {
      border-color: #d9b361;
      background: rgba(217, 179, 97, .15);
      animation: hearthmouse-controller-pulse 1s ease-in-out infinite alternate;
    }
    .${CONTROLLER_DIALOG_CLASS}__sliders {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
      margin: 15px 0;
    }
    .${CONTROLLER_DIALOG_CLASS}__slider {
      padding: 11px;
      border: 1px solid rgba(255, 255, 255, .08);
      border-radius: 9px;
    }
    .${CONTROLLER_DIALOG_CLASS}__slider label {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 8px;
      font: 700 10px/1.2 ui-sans-serif, system-ui, sans-serif;
      letter-spacing: .08em;
      text-transform: uppercase;
    }
    .${CONTROLLER_DIALOG_CLASS}__slider input { width: 100%; accent-color: #d9b361; }
    .${CONTROLLER_DIALOG_CLASS}__help {
      margin: 12px 0 0;
      color: rgba(235, 226, 208, .68);
      font-size: 11px;
      line-height: 1.5;
    }
    .${CONTROLLER_DIALOG_CLASS}__footer {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      margin-top: 16px;
    }
    .${CONTROLLER_DIALOG_CLASS}__footer button { min-width: 150px; }
    @keyframes hearthmouse-controller-pulse { from { box-shadow: 0 0 0 rgba(217,179,97,0); } to { box-shadow: 0 0 24px rgba(217,179,97,.16); } }
    @media (max-width: 620px), (max-height: 560px) {
      .${CONTROLLER_DIALOG_CLASS} { padding: 8px; }
      .${CONTROLLER_DIALOG_CLASS}__panel { max-height: calc(100vh - 16px); width: calc(100vw - 16px); padding: 13px; }
      .${CONTROLLER_DIALOG_CLASS}__connection { grid-template-columns: 1fr; }
      .${CONTROLLER_DIALOG_CLASS}__mapping-row { grid-template-columns: 1fr; gap: 5px; padding: 8px 0; }
      .${CONTROLLER_DIALOG_CLASS}__sliders { grid-template-columns: 1fr; }
      .${CONTROLLER_DIALOG_CLASS}__footer { flex-direction: column; }
      .${CONTROLLER_DIALOG_CLASS}__footer button { width: 100%; }
    }
  `;
  document.head.appendChild(style);
}

function gamepadButtonLabel(index) {
  const standard = {
    0: "A / Cross",
    1: "B / Circle",
    2: "X / Square",
    3: "Y / Triangle",
    4: "LB / L1",
    5: "RB / R1",
    6: "LT / L2",
    7: "RT / R2",
    8: "Back / Share",
    9: "Start / Options",
    10: "Left Stick Click",
    11: "Right Stick Click",
    12: "D-pad Up",
    13: "D-pad Down",
    14: "D-pad Left",
    15: "D-pad Right",
    16: "Home",
  };
  return standard[index] ?? `Button ${index}`;
}

function gamepadAxesLabel(axes) {
  const normalized = normalizeAxes(axes, DEFAULT_BINDINGS.moveAxes);
  if (normalized[0] === 0 && normalized[1] === 1) return "Left Stick";
  if (normalized[0] === 2 && normalized[1] === 3) return "Right Stick";
  return `Axes ${normalized[0]} + ${normalized[1]}`;
}

function connectedGamepads() {
  if (typeof navigator === "undefined" || typeof navigator.getGamepads !== "function") return [];
  try {
    return Array.from(navigator.getGamepads() ?? []).filter((gamepad) => gamepad?.connected !== false);
  } catch {
    return [];
  }
}

function gamepadButtonIsPressed(gamepad, index, threshold = 0.55) {
  const button = gamepad?.buttons?.[index];
  return !!button && (button.pressed || (Number(button.value) || 0) >= threshold);
}

function getPreferredGamepad(settings) {
  const pads = connectedGamepads();
  if (!pads.length) return null;
  if (Number.isInteger(settings.preferredGamepadIndex)) {
    const byIndex = pads.find((pad) => pad.index === settings.preferredGamepadIndex && (!settings.preferredGamepadId || pad.id === settings.preferredGamepadId));
    if (byIndex) return byIndex;
  }
  if (settings.preferredGamepadId) {
    const byId = pads.find((pad) => pad.id === settings.preferredGamepadId);
    if (byId) return byId;
  }
  return pads[0];
}

function trimGamepadName(id) {
  const text = String(id || "Controller").replace(/\s*\([^)]*STANDARD GAMEPAD[^)]*\)\s*/i, " ").trim();
  return text.length > 44 ? `${text.slice(0, 41)}…` : text;
}

function createControllerDialog() {
  const existing = document.querySelector(`.${CONTROLLER_DIALOG_CLASS}`);
  if (existing) return existing;
  installControllerStyles();
  const dialog = document.createElement("section");
  dialog.className = CONTROLLER_DIALOG_CLASS;
  dialog.hidden = true;
  dialog.setAttribute("role", "dialog");
  dialog.setAttribute("aria-modal", "true");
  dialog.setAttribute("aria-labelledby", "hearthmouse-controller-title");
  dialog.innerHTML = `
    <div class="${CONTROLLER_DIALOG_CLASS}__panel">
      <div class="${CONTROLLER_DIALOG_CLASS}__header">
        <div>
          <p class="${CONTROLLER_DIALOG_CLASS}__eyebrow">Settings · Controls</p>
          <h2 id="hearthmouse-controller-title">Controller Setup</h2>
        </div>
        <button class="${CONTROLLER_DIALOG_CLASS}__close" type="button" data-controller-close aria-label="Close controller settings">×</button>
      </div>
      <div class="${CONTROLLER_DIALOG_CLASS}__connection">
        <div>
          <strong data-controller-connection-title>No controller detected</strong>
          <span data-controller-connection-help>Connect by USB or Bluetooth, then press any controller button so the browser can detect it.</span>
        </div>
        <select data-controller-device aria-label="Controller device"><option value="">No controller detected</option></select>
      </div>
      <label class="${CONTROLLER_DIALOG_CLASS}__toggle-row">
        <span>Enable controller input</span>
        <input type="checkbox" data-controller-enabled>
      </label>
      <div class="${CONTROLLER_DIALOG_CLASS}__mapping" data-controller-mapping></div>
      <div class="${CONTROLLER_DIALOG_CLASS}__sliders">
        <div class="${CONTROLLER_DIALOG_CLASS}__slider">
          <label><span>Stick Deadzone</span><strong data-controller-deadzone-value></strong></label>
          <input type="range" min="0.05" max="0.35" step="0.01" data-controller-deadzone>
        </div>
        <div class="${CONTROLLER_DIALOG_CLASS}__slider">
          <label><span>Look Sensitivity</span><strong data-controller-look-value></strong></label>
          <input type="range" min="0.4" max="2" step="0.05" data-controller-look-sensitivity>
        </div>
      </div>
      <p class="${CONTROLLER_DIALOG_CLASS}__help">To remap a control, choose its mapping and then press the desired controller button or move the desired stick. Full stick movement already gives analog sprint speed; the mapped Sprint and Creep buttons can force those speeds at smaller stick deflections.</p>
      <div class="${CONTROLLER_DIALOG_CLASS}__footer">
        <button type="button" data-controller-reset>Reset Default Mapping</button>
        <button type="button" data-controller-done>Done</button>
      </div>
    </div>
  `;
  document.body.appendChild(dialog);
  return dialog;
}

function createControllerMenuRow(root) {
  let row = root.querySelector(`.${MENU_CLASS}__controller-row`);
  if (row) return row;
  row = document.createElement("div");
  row.className = `${MENU_CLASS}__controller-row`;
  row.innerHTML = `
    <button type="button" data-controller-open>Controller Settings</button>
    <span class="${MENU_CLASS}__controller-status" data-controller-menu-status>No controller</span>
  `;
  root.appendChild(row);
  return row;
}

function menuPrimaryAction() {
  const selectors = [
    ".title-screen .primary-action",
    ".summary-screen .primary-action",
    ".caught-screen .primary-action",
    ".end-screen .primary-action",
  ];
  for (const selector of selectors) {
    const button = document.querySelector(selector);
    if (button && !button.disabled && button.offsetParent !== null) return button;
  }
  return null;
}

function mountControllerSettings(root) {
  const row = createControllerMenuRow(root);
  const dialog = createControllerDialog();
  let settings = readControllerSettings();
  let capture = null;
  let previousButtons = [];
  let raf = 0;
  let lastUiRefresh = 0;
  let lastConnectedSignature = "";
  let lastAppliedGamepadIndex = null;

  const mappingRoot = dialog.querySelector("[data-controller-mapping]");
  for (const action of CONTROLLER_ACTIONS) {
    const mappingRow = document.createElement("div");
    mappingRow.className = `${CONTROLLER_DIALOG_CLASS}__mapping-row`;
    mappingRow.innerHTML = `
      <span>${action.label}</span>
      <button class="${CONTROLLER_DIALOG_CLASS}__map-button" type="button" data-controller-map="${action.id}" data-controller-map-type="${action.type}"></button>
    `;
    mappingRoot.appendChild(mappingRow);
  }

  const enabledInput = dialog.querySelector("[data-controller-enabled]");
  const deadzoneInput = dialog.querySelector("[data-controller-deadzone]");
  const lookInput = dialog.querySelector("[data-controller-look-sensitivity]");
  const deviceSelect = dialog.querySelector("[data-controller-device]");
  const menuStatus = row.querySelector("[data-controller-menu-status]");
  const connectionTitle = dialog.querySelector("[data-controller-connection-title]");
  const connectionHelp = dialog.querySelector("[data-controller-connection-help]");

  const save = () => storeControllerSettings(settings);

  const renderMappings = () => {
    dialog.querySelectorAll("[data-controller-map]").forEach((button) => {
      const id = button.dataset.controllerMap;
      const type = button.dataset.controllerMapType;
      const isCapturing = capture?.action === id;
      button.dataset.capturing = String(isCapturing);
      if (isCapturing) {
        button.textContent = type === "axes" ? "Move the desired stick…" : "Press the desired button…";
        return;
      }
      const value = settings.bindings[id];
      button.textContent = type === "axes" ? gamepadAxesLabel(value) : gamepadButtonLabel(value);
    });
    enabledInput.checked = settings.enabled;
    deadzoneInput.value = String(settings.deadzone);
    lookInput.value = String(settings.lookSensitivity);
    dialog.querySelector("[data-controller-deadzone-value]").textContent = `${Math.round(settings.deadzone * 100)}%`;
    dialog.querySelector("[data-controller-look-value]").textContent = `${settings.lookSensitivity.toFixed(2)}×`;
  };

  const renderDevices = (force = false) => {
    const pads = connectedGamepads();
    const signature = pads.map((pad) => `${pad.index}:${pad.id}`).join("|");
    if (!force && signature === lastConnectedSignature) return;
    lastConnectedSignature = signature;
    const active = getPreferredGamepad(settings);
    deviceSelect.innerHTML = "";
    if (!pads.length) {
      const option = document.createElement("option");
      option.value = "";
      option.textContent = typeof navigator !== "undefined" && typeof navigator.getGamepads === "function"
        ? "No controller detected"
        : "Gamepad API unavailable";
      deviceSelect.appendChild(option);
      deviceSelect.disabled = true;
      connectionTitle.textContent = "No controller detected";
      connectionHelp.textContent = typeof navigator !== "undefined" && typeof navigator.getGamepads === "function"
        ? "Connect by USB or Bluetooth, then press any controller button so the browser can detect it."
        : "This browser does not expose the Gamepad API required by Hearthmouse controller support.";
      menuStatus.textContent = "No controller";
      menuStatus.dataset.connected = "false";
      return;
    }
    deviceSelect.disabled = false;
    for (const pad of pads) {
      const option = document.createElement("option");
      option.value = String(pad.index);
      option.textContent = trimGamepadName(pad.id);
      option.selected = pad.index === active?.index;
      deviceSelect.appendChild(option);
    }
    connectionTitle.textContent = `${trimGamepadName(active?.id)} connected`;
    connectionHelp.textContent = active?.mapping === "standard"
      ? "Standard controller mapping detected. You can play immediately or remap any control below."
      : "Controller detected. Custom mapping is available below if its button layout differs.";
    menuStatus.textContent = settings.enabled ? "Connected" : "Disabled";
    menuStatus.dataset.connected = "true";
  };

  const setSettings = (next) => {
    settings = normalizeControllerSettings(next);
    save();
    renderMappings();
    renderDevices(true);
  };

  const selectGamepad = (gamepad) => {
    if (!gamepad) return;
    settings.preferredGamepadId = gamepad.id || "";
    settings.preferredGamepadIndex = gamepad.index;
    save();
    renderDevices(true);
  };

  const openDialog = () => {
    capture = null;
    renderMappings();
    renderDevices(true);
    dialog.hidden = false;
    dialog.querySelector("[data-controller-close]")?.focus({ preventScroll: true });
  };

  const closeDialog = () => {
    capture = null;
    renderMappings();
    dialog.hidden = true;
    row.querySelector("[data-controller-open]")?.focus({ preventScroll: true });
  };

  const beginCapture = (button) => {
    const gamepad = getPreferredGamepad(settings);
    if (!gamepad) {
      renderDevices(true);
      return;
    }
    const type = button.dataset.controllerMapType;
    capture = {
      action: button.dataset.controllerMap,
      type,
      startedAt: performance.now(),
      baselineAxes: Array.from(gamepad.axes ?? []),
      baselineButtons: Array.from(gamepad.buttons ?? [], (item) => Number(item?.value) || 0),
    };
    renderMappings();
  };

  const detectCapture = (gamepad) => {
    if (!capture || !gamepad || performance.now() - capture.startedAt < 120) return;
    if (capture.type === "button") {
      for (let index = 0; index < (gamepad.buttons?.length ?? 0); index++) {
        const value = Number(gamepad.buttons[index]?.value) || 0;
        const baseline = capture.baselineButtons[index] ?? 0;
        if (value >= 0.72 && baseline < 0.45) {
          settings.bindings[capture.action] = index;
          capture = null;
          save();
          renderMappings();
          return;
        }
      }
      return;
    }

    let strongestIndex = -1;
    let strongestChange = 0;
    for (let index = 0; index < (gamepad.axes?.length ?? 0); index++) {
      const value = Number(gamepad.axes[index]) || 0;
      const baseline = capture.baselineAxes[index] ?? 0;
      const change = Math.abs(value - baseline);
      if (change > strongestChange) {
        strongestChange = change;
        strongestIndex = index;
      }
    }
    if (strongestIndex < 0 || strongestChange < 0.58) return;
    const first = strongestIndex % 2 === 0 ? strongestIndex : strongestIndex - 1;
    if (first < 0 || first + 1 >= (gamepad.axes?.length ?? 0)) return;
    settings.bindings[capture.action] = [first, first + 1];
    capture = null;
    save();
    renderMappings();
  };

  const edgePressed = (gamepad, binding) => {
    const now = gamepadButtonIsPressed(gamepad, binding);
    const before = !!previousButtons[binding];
    return now && !before;
  };

  const rememberButtons = (gamepad) => {
    previousButtons = Array.from(gamepad?.buttons ?? [], (button) => !!button && (button.pressed || (Number(button.value) || 0) >= 0.55));
  };

  const applyControllerInput = (gamepad) => {
    const engine = window.hearthmouseEngine;
    if (!settings.enabled || !gamepad || !engine || engine.disposed) {
      if (lastAppliedGamepadIndex !== null && engine?.setTouch) {
        engine.setTouch({ moveX: 0, moveY: 0, lookX: 0, lookY: 0 });
      }
      lastAppliedGamepadIndex = null;
      rememberButtons(gamepad);
      return;
    }

    lastAppliedGamepadIndex = gamepad.index;
    if (capture || !dialog.hidden) {
      engine.setTouch?.({ moveX: 0, moveY: 0, lookX: 0, lookY: 0 });
      rememberButtons(gamepad);
      return;
    }

    const move = controllerAxisVector(gamepad, settings.bindings.moveAxes, settings.deadzone);
    const look = controllerAxisVector(gamepad, settings.bindings.lookAxes, settings.deadzone);
    let moveX = move.x;
    let moveY = move.y;

    const creepHeld = gamepadButtonIsPressed(gamepad, settings.bindings.creep);
    const sprintHeld = gamepadButtonIsPressed(gamepad, settings.bindings.sprint);
    if (move.magnitude > 0.001 && creepHeld) {
      const targetMagnitude = Math.min(0.3, move.magnitude);
      moveX = (move.x / move.magnitude) * targetMagnitude;
      moveY = (move.y / move.magnitude) * targetMagnitude;
    } else if (move.magnitude > 0.001 && sprintHeld && move.magnitude < 0.99) {
      moveX = move.x / move.magnitude;
      moveY = move.y / move.magnitude;
    }

    engine.setTouch?.({
      moveX,
      moveY,
      lookX: clamp(look.x * settings.lookSensitivity, -1, 1),
      lookY: clamp(look.y * settings.lookSensitivity, -1, 1),
    });

    const phase = engine.snapshot?.phase;
    const jumpEdge = edgePressed(gamepad, settings.bindings.jump);
    const menuEdge = edgePressed(gamepad, settings.bindings.menuConfirm);
    if (phase === "foraging") {
      if (!document.hidden) engine.paused = false;
      if (jumpEdge) engine.jumpQueued = true;
      if (edgePressed(gamepad, settings.bindings.interact)) engine.interactQueued = true;
      if (edgePressed(gamepad, settings.bindings.rest)) engine.restForNight?.();
    } else if ((jumpEdge || menuEdge) && gameIsOnMainMenu()) {
      menuPrimaryAction()?.click();
    }
    rememberButtons(gamepad);
  };

  const poll = (timestamp) => {
    const gamepad = getPreferredGamepad(settings);
    if (gamepad && (!settings.preferredGamepadId || settings.preferredGamepadIndex !== gamepad.index)) selectGamepad(gamepad);
    detectCapture(gamepad);
    applyControllerInput(gamepad);
    if (timestamp - lastUiRefresh > 350) {
      lastUiRefresh = timestamp;
      renderDevices();
      if (!gameIsOnMainMenu() && !dialog.hidden) closeDialog();
    }
    raf = window.requestAnimationFrame(poll);
  };

  row.querySelector("[data-controller-open]").addEventListener("click", openDialog);
  dialog.querySelector("[data-controller-close]").addEventListener("click", closeDialog);
  dialog.querySelector("[data-controller-done]").addEventListener("click", closeDialog);
  dialog.addEventListener("click", (event) => {
    if (event.target === dialog) closeDialog();
    const mappingButton = event.target.closest?.("[data-controller-map]");
    if (mappingButton) beginCapture(mappingButton);
  });
  window.addEventListener("keydown", (event) => {
    if (event.code === "Escape" && !dialog.hidden) closeDialog();
  });
  window.addEventListener("gamepadconnected", (event) => {
    if (!settings.preferredGamepadId) selectGamepad(event.gamepad);
    renderDevices(true);
  });
  window.addEventListener("gamepaddisconnected", () => renderDevices(true));

  enabledInput.addEventListener("change", () => {
    settings.enabled = enabledInput.checked;
    save();
    renderDevices(true);
  });
  deadzoneInput.addEventListener("input", () => {
    settings.deadzone = clamp(Number(deadzoneInput.value), 0.05, 0.45);
    save();
    renderMappings();
  });
  lookInput.addEventListener("input", () => {
    settings.lookSensitivity = clamp(Number(lookInput.value), 0.4, 2);
    save();
    renderMappings();
  });
  deviceSelect.addEventListener("change", () => {
    const selectedIndex = Number(deviceSelect.value);
    const gamepad = connectedGamepads().find((pad) => pad.index === selectedIndex);
    if (gamepad) selectGamepad(gamepad);
  });
  dialog.querySelector("[data-controller-reset]").addEventListener("click", () => {
    const preferredGamepadId = settings.preferredGamepadId;
    const preferredGamepadIndex = settings.preferredGamepadIndex;
    setSettings({ ...DEFAULT_CONTROLLER_SETTINGS, preferredGamepadId, preferredGamepadIndex });
  });

  renderMappings();
  renderDevices(true);
  raf = window.requestAnimationFrame(poll);

  window.HearthmouseController = {
    get settings() { return typeof structuredClone === "function" ? structuredClone(settings) : JSON.parse(JSON.stringify(settings)); },
    set settings(value) { setSettings(value); },
    get gamepad() { return getPreferredGamepad(settings); },
    openSettings: openDialog,
    closeSettings: closeDialog,
    reset() {
      const preferredGamepadId = settings.preferredGamepadId;
      const preferredGamepadIndex = settings.preferredGamepadIndex;
      setSettings({ ...DEFAULT_CONTROLLER_SETTINGS, preferredGamepadId, preferredGamepadIndex });
    },
    dispose() {
      if (raf) window.cancelAnimationFrame(raf);
    },
  };

  return dialog;
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

  mountControllerSettings(root);
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
