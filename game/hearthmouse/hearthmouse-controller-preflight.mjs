export function sanitizeGamepadList(value) {
  try {
    return Array.from(value ?? []).filter((gamepad) => !!gamepad && gamepad.connected !== false);
  } catch {
    return [];
  }
}

function replaceGetter(target, key, getter) {
  if (!target) return false;
  try {
    Object.defineProperty(target, key, {
      configurable: true,
      writable: true,
      value: getter,
    });
    return target[key] === getter;
  } catch {
    try {
      target[key] = getter;
      return target[key] === getter;
    } catch {
      return false;
    }
  }
}

export function installHearthmouseGamepadPreflight(targetNavigator = globalThis.navigator, targetGlobal = globalThis) {
  if (!targetNavigator) return false;

  const sourceGetter = typeof targetNavigator.getGamepads === "function"
    ? targetNavigator.getGamepads.bind(targetNavigator)
    : typeof targetNavigator.webkitGetGamepads === "function"
      ? targetNavigator.webkitGetGamepads.bind(targetNavigator)
      : null;
  if (!sourceGetter) return false;

  const safeGetGamepads = () => {
    try {
      return sanitizeGamepadList(sourceGetter());
    } catch {
      return [];
    }
  };

  // Prefer an own-property override when the browser allows Navigator expandos.
  if (replaceGetter(targetNavigator, "getGamepads", safeGetGamepads)) return true;

  // Some Chromium/WebView builds expose Navigator as non-extensible. Patch the
  // configurable prototype method instead so every subsequent lookup is safe.
  const prototype = Object.getPrototypeOf(targetNavigator);
  if (prototype && replaceGetter(prototype, "getGamepads", safeGetGamepads)) return true;

  // Last-resort fallback for hosts that lock both the Navigator instance and
  // prototype but allow the global navigator property to be redefined.
  if (targetGlobal && targetGlobal.navigator === targetNavigator && typeof Proxy === "function") {
    const safeNavigator = new Proxy(targetNavigator, {
      get(target, property, receiver) {
        if (property === "getGamepads") return safeGetGamepads;
        return Reflect.get(target, property, receiver);
      },
    });
    try {
      Object.defineProperty(targetGlobal, "navigator", {
        configurable: true,
        get: () => safeNavigator,
      });
      return targetGlobal.navigator === safeNavigator;
    } catch {
      // Nothing else can safely replace the host's Gamepad API.
    }
  }

  return false;
}

if (typeof navigator !== "undefined") {
  installHearthmouseGamepadPreflight(navigator, globalThis);
}
