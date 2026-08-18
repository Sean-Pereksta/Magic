const STATUS_SELECTOR = "[data-controller-menu-status]";
const CONNECTION_TITLE_SELECTOR = "[data-controller-connection-title]";
const CONNECTION_HELP_SELECTOR = "[data-controller-connection-help]";
const DEVICE_SELECTOR = "[data-controller-device]";
const BRIDGE_FLAG = "__hearthmouseControllerTouchBridge";
const KEY_THRESHOLD = 0.24;
const LOOK_YAW_SPEED = 2.65;
const LOOK_PITCH_SPEED = 2.05;

const gamepadCache = new Map();

export function controllerDisplayName(id = "") {
  let text = String(id || "Controller")
    .replace(/\s*\([^)]*STANDARD GAMEPAD[^)]*\)\s*/ig, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (/xbox/i.test(text)) {
    if (/wireless/i.test(text)) return "Xbox Wireless Controller";
    if (/elite/i.test(text)) return "Xbox Elite Controller";
    if (/360/i.test(text)) return "Xbox 360 Controller";
    return "Xbox Controller";
  }

  if (!text) text = "Controller";
  return text.length > 48 ? `${text.slice(0, 45)}…` : text;
}

export function movementKeysForVector(moveX = 0, moveY = 0, threshold = KEY_THRESHOLD) {
  const x = Number.isFinite(Number(moveX)) ? Number(moveX) : 0;
  const y = Number.isFinite(Number(moveY)) ? Number(moveY) : 0;
  const t = Math.max(0.05, Math.min(0.8, Number(threshold) || KEY_THRESHOLD));
  return {
    KeyW: y < -t,
    KeyS: y > t,
    KeyA: x < -t,
    KeyD: x > t,
  };
}

function gamepadGetter(targetNavigator = globalThis.navigator) {
  if (!targetNavigator) return null;
  if (typeof targetNavigator.getGamepads === "function") return targetNavigator.getGamepads.bind(targetNavigator);
  if (typeof targetNavigator.webkitGetGamepads === "function") return targetNavigator.webkitGetGamepads.bind(targetNavigator);
  return null;
}

function liveGamepads() {
  const getter = gamepadGetter();
  const pads = [];
  if (getter) {
    try {
      for (const pad of Array.from(getter() ?? [])) {
        if (!pad || pad.connected === false) continue;
        pads.push(pad);
        gamepadCache.set(pad.index, pad);
      }
    } catch (error) {
      if (error?.name === "SecurityError") document.documentElement.dataset.hearthmouseGamepadBlocked = "true";
    }
  }

  for (const [index, pad] of gamepadCache) {
    if (!pad || pad.connected === false) {
      gamepadCache.delete(index);
      continue;
    }
    if (!pads.some((candidate) => candidate.index === index)) pads.push(pad);
  }
  return pads.sort((a, b) => a.index - b.index);
}

function currentPad() {
  const controller = window.HearthmouseController;
  const preferred = controller?.gamepad;
  if (preferred?.connected !== false) return preferred;

  const pads = liveGamepads();
  if (!pads.length) return null;

  const settings = controller?.settings;
  if (settings) {
    const exact = pads.find((pad) =>
      (Number.isInteger(settings.preferredGamepadIndex) && pad.index === settings.preferredGamepadIndex) ||
      (settings.preferredGamepadId && pad.id === settings.preferredGamepadId)
    );
    if (exact) return exact;
  }
  return pads[0];
}

function promotePad(pad) {
  if (!pad || !window.HearthmouseController) return;
  try {
    const settings = window.HearthmouseController.settings;
    if (!settings) return;
    if (settings.preferredGamepadIndex === pad.index && settings.preferredGamepadId === (pad.id || "")) return;
    window.HearthmouseController.settings = {
      ...settings,
      preferredGamepadIndex: pad.index,
      preferredGamepadId: pad.id || "",
    };
  } catch {
    // The existing controller module may still be mounting; the next poll retries.
  }
}

function renderDetectedController() {
  const pad = currentPad();
  const status = document.querySelector(STATUS_SELECTOR);
  const connectionTitle = document.querySelector(CONNECTION_TITLE_SELECTOR);
  const connectionHelp = document.querySelector(CONNECTION_HELP_SELECTOR);
  const deviceSelect = document.querySelector(DEVICE_SELECTOR);

  if (!pad) {
    if (document.documentElement.dataset.hearthmouseGamepadBlocked === "true") {
      if (status) {
        status.textContent = "Controller blocked";
        status.dataset.connected = "false";
      }
      if (connectionTitle) connectionTitle.textContent = "Controller access blocked";
      if (connectionHelp) connectionHelp.textContent = "The browser blocked Gamepad access. Open Hearthmouse directly in a secure tab or allow gamepad access for the embedded frame.";
    }
    return;
  }

  promotePad(pad);
  const name = controllerDisplayName(pad.id);
  if (status) {
    status.textContent = name;
    status.title = String(pad.id || name);
    status.dataset.connected = "true";
  }
  if (connectionTitle) connectionTitle.textContent = `${name} connected`;
  if (connectionHelp) {
    connectionHelp.textContent = pad.mapping === "standard"
      ? "Xbox/standard Gamepad mapping detected. Controller input is active."
      : "Controller detected. Use the mapping controls below if its layout differs.";
  }

  if (deviceSelect && !Array.from(deviceSelect.options).some((option) => Number(option.value) === pad.index)) {
    const option = document.createElement("option");
    option.value = String(pad.index);
    option.textContent = name;
    option.selected = true;
    deviceSelect.appendChild(option);
    deviceSelect.disabled = false;
  }
}

const keyNames = Object.freeze({
  KeyW: "w",
  KeyA: "a",
  KeyS: "s",
  KeyD: "d",
  ShiftLeft: "Shift",
  ControlLeft: "Control",
});

function dispatchSyntheticKey(code, pressed) {
  const event = new KeyboardEvent(pressed ? "keydown" : "keyup", {
    key: keyNames[code] ?? code,
    code,
    bubbles: true,
    cancelable: true,
  });
  window.dispatchEvent(event);
}

function installTouchBridge(engine) {
  if (!engine || engine.disposed || typeof engine.setTouch === "function" || engine[BRIDGE_FLAG]) return false;
  if (engine.isTouchOnlyDevice?.()) return false;

  const held = new Map();
  let lastLookTime = performance.now();

  const setHeld = (code, pressed) => {
    if (held.get(code) === pressed) return;
    held.set(code, pressed);
    dispatchSyntheticKey(code, pressed);
  };

  const releaseAll = () => {
    for (const [code, pressed] of held) if (pressed) dispatchSyntheticKey(code, false);
    held.clear();
  };

  engine.setTouch = ({ moveX = 0, moveY = 0, lookX = 0, lookY = 0 } = {}) => {
    const keys = movementKeysForVector(moveX, moveY);
    setHeld("KeyW", keys.KeyW);
    setHeld("KeyS", keys.KeyS);
    setHeld("KeyA", keys.KeyA);
    setHeld("KeyD", keys.KeyD);

    const controller = window.HearthmouseController;
    const pad = controller?.gamepad ?? currentPad();
    const settings = controller?.settings;
    const sprintBinding = settings?.bindings?.sprint ?? 7;
    const creepBinding = settings?.bindings?.creep ?? 6;
    const sprint = !!pad?.buttons?.[sprintBinding] && (pad.buttons[sprintBinding].pressed || Number(pad.buttons[sprintBinding].value) >= 0.55);
    const creep = !!pad?.buttons?.[creepBinding] && (pad.buttons[creepBinding].pressed || Number(pad.buttons[creepBinding].value) >= 0.55);
    setHeld("ShiftLeft", sprint && !creep);
    setHeld("ControlLeft", creep);

    const now = performance.now();
    const dt = Math.min(0.05, Math.max(0, (now - lastLookTime) / 1000));
    lastLookTime = now;
    const lx = Number.isFinite(Number(lookX)) ? Number(lookX) : 0;
    const ly = Number.isFinite(Number(lookY)) ? Number(lookY) : 0;
    if (Math.abs(lx) > 0.001) engine.yaw -= lx * LOOK_YAW_SPEED * dt;
    if (Math.abs(ly) > 0.001) {
      engine.pitch = Math.max(-1.05, Math.min(0.92, engine.pitch - ly * LOOK_PITCH_SPEED * dt));
    }
  };

  engine[BRIDGE_FLAG] = { releaseAll };
  return true;
}

function releaseBridge(engine) {
  engine?.[BRIDGE_FLAG]?.releaseAll?.();
}

function poll() {
  const pads = liveGamepads();
  if (pads.length) promotePad(currentPad() ?? pads[0]);
  renderDetectedController();

  const engine = window.hearthmouseEngine;
  if (engine && !engine.disposed) installTouchBridge(engine);
  else releaseBridge(engine);

  window.setTimeout(poll, 180);
}

function installLegacyGamepadAlias() {
  if (!navigator || typeof navigator.getGamepads === "function" || typeof navigator.webkitGetGamepads !== "function") return;
  try {
    Object.defineProperty(navigator, "getGamepads", {
      configurable: true,
      value: navigator.webkitGetGamepads.bind(navigator),
    });
  } catch {
    // Non-writable Navigator implementations are still handled by liveGamepads().
  }
}

export function installHearthmouseControllerReliability() {
  if (window.__hearthmouseControllerReliabilityInstalled) return;
  window.__hearthmouseControllerReliabilityInstalled = true;
  installLegacyGamepadAlias();

  window.addEventListener("gamepadconnected", (event) => {
    if (event.gamepad) {
      gamepadCache.set(event.gamepad.index, event.gamepad);
      promotePad(event.gamepad);
    }
    renderDetectedController();
  });
  window.addEventListener("gamepaddisconnected", (event) => {
    if (event.gamepad) gamepadCache.delete(event.gamepad.index);
    renderDetectedController();
  });

  window.addEventListener("blur", () => releaseBridge(window.hearthmouseEngine));
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) releaseBridge(window.hearthmouseEngine);
  });

  poll();
}

if (typeof window !== "undefined" && typeof document !== "undefined") {
  installHearthmouseControllerReliability();
}
