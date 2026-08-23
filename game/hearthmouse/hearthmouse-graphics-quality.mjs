// Safe Hearthmouse graphics/controller entrypoint.
// Keep controller Gamepad sanitization ahead of the existing menu module even
// when this file is imported directly by the page rather than via expansion.mjs.
import "./hearthmouse-controller-preflight.mjs";
import "./hearthmouse-performance-governor.mjs";
import "./hearthmouse-renderer-budget-guard.mjs";
export * from "./hearthmouse-graphics-quality-core.mjs";
