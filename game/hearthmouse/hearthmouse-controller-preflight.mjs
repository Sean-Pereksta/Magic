export function sanitizeGamepadList(value) {
  try {
    return Array.from(value ?? []).filter((gamepad) => !!gamepad && gamepad.connected !== false);
  } catch {
    return [];
  }
}

export function installHearthmouseGamepadPreflight(targetNavigator = globalThis.navigator) {
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

  try {
    Object.defineProperty(targetNavigator, "getGamepads", {
      configurable: true,
      value: safeGetGamepads,
    });
    return true;
  } catch {
    try {
      targetNavigator.getGamepads = safeGetGamepads;
      return targetNavigator.getGamepads === safeGetGamepads;
    } catch {
      return false;
    }
  }
}

if (typeof navigator !== "undefined") {
  installHearthmouseGamepadPreflight(navigator);
}
