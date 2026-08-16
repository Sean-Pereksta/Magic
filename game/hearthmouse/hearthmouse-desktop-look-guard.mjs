export const DESKTOP_MOUSE_LOOK_MAX_DELTA = 96;
export const DESKTOP_MOUSE_LOOK_YAW_SENSITIVITY = 0.00235;
export const DESKTOP_MOUSE_LOOK_PITCH_SENSITIVITY = 0.002;

export function clampDesktopMouseDelta(
  deltaX,
  deltaY,
  maxMagnitude = DESKTOP_MOUSE_LOOK_MAX_DELTA,
) {
  const xIsFinite = Number.isFinite(deltaX);
  const yIsFinite = Number.isFinite(deltaY);
  const x = xIsFinite ? deltaX : 0;
  const y = yIsFinite ? deltaY : 0;

  if (!xIsFinite || !yIsFinite) return { x, y, clamped: true };

  const magnitude = Math.hypot(x, y);
  if (magnitude === 0 || magnitude <= maxMagnitude) return { x, y, clamped: false };

  const scale = maxMagnitude / magnitude;
  return { x: x * scale, y: y * scale, clamped: true };
}

export function applyDesktopMouseLook(engine, deltaX, deltaY) {
  const safe = clampDesktopMouseDelta(deltaX, deltaY);
  engine.yaw -= safe.x * DESKTOP_MOUSE_LOOK_YAW_SENSITIVITY;
  engine.pitch = Math.max(
    -1.05,
    Math.min(0.92, engine.pitch - safe.y * DESKTOP_MOUSE_LOOK_PITCH_SENSITIVITY),
  );
  return safe;
}

export function installDesktopMouseLookGuard(targetWindow, targetDocument) {
  if (!targetWindow || !targetDocument) return false;
  if (targetDocument.__hearthmouseDesktopMouseLookGuardInstalled) return true;

  targetDocument.__hearthmouseDesktopMouseLookGuardInstalled = true;
  targetDocument.addEventListener("mousemove", (event) => {
    const engine = targetWindow.hearthmouseEngine;
    if (!engine || engine.disposed || engine.isTouchOnlyDevice?.()) return;

    let deltaX = 0;
    let deltaY = 0;
    let fallbackLook = false;

    if (targetDocument.pointerLockElement === engine.canvas) {
      deltaX = event.movementX;
      deltaY = event.movementY;
    } else if (engine.fallbackLookActive && (event.buttons & 1) === 1) {
      deltaX = event.clientX - engine.fallbackLookX;
      deltaY = event.clientY - engine.fallbackLookY;
      fallbackLook = true;
    } else {
      return;
    }

    const safe = clampDesktopMouseDelta(deltaX, deltaY);
    if (!safe.clamped) return;

    // The generated engine's normal mousemove handler has no delta guard. Stop
    // only anomalously large/non-finite desktop events before they reach that
    // handler, then apply the same sensitivity with a bounded delta instead.
    event.stopImmediatePropagation();

    if (fallbackLook) {
      engine.fallbackLookX = event.clientX;
      engine.fallbackLookY = event.clientY;
    }

    engine.yaw -= safe.x * DESKTOP_MOUSE_LOOK_YAW_SENSITIVITY;
    engine.pitch = Math.max(
      -1.05,
      Math.min(0.92, engine.pitch - safe.y * DESKTOP_MOUSE_LOOK_PITCH_SENSITIVITY),
    );
  }, true);

  return true;
}

if (typeof window !== "undefined" && typeof document !== "undefined") {
  installDesktopMouseLookGuard(window, document);
}
