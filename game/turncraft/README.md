# TurnCraft presentation checks

The game remains `../turncraft.html`. `polish.css` is loaded after the legacy styles. No build step or new runtime dependency is required.

## Controls

- Drag to pan; wheel/pinch or Zoom buttons to zoom. WASD/arrows pan; F focuses selection.
- Touch taps select. Choose an order before tapping a target; gestures never issue orders.
- Shift-drag selects a group; double-click/tap selects the visible unit type.
- X arms Attack. Escape/Cancel Order cancels targeting. The numbered control groups retain tap/hold behavior.
- Mine uses existing automatic gathering on arrival. Stop suppresses gathering until a new mining/movement order.
- Build lists structures and lets adjacent human workers assist an unfinished building. Construction duration and cost are unchanged.

## Checks

From this directory, run `npm install`, `npx playwright install chromium`, then `npm test`.
Set `CHROMIUM_EXECUTABLE_PATH` to use an existing Chromium executable.

The headless checks load the real game source and CSS, replacing Firebase imports and the network boot with an isolated test entry point. They cover all three construction styles, snapshot fields, touch gestures, smooth camera controls, mining stop/resume and ownership, helper construction, ability/hit/destruction effects, bounded particles, and command geometry at 390×844, 844×390, and 1280×800. They do not contact Firebase or produce HTML previews.

Live multi-client synchronization and physical-device frame rates require an actual lobby/device check. New construction fields travel through the existing entity snapshot; presentation events are observed locally without a second simulation.

## Mining and input reliability update

- The existing mining loop handles all three factions. Friendly completed faction-specific extractors must remain connected to a completed base. Gas supports every adjacent worker and takes priority over minerals and automatic combat; explicit attacks, movement, construction, channels, and Stop keep priority over harvesting.
- New Move/Build/Assist orders clear Stop even if no movement step occurs. Finished or destroyed construction releases workers, and depletion releases every worker immediately. Selection never changes worker orders.
- `GLOBAL_HOTKEYS`, `resolveHotkeys`, and `dispatchHotkey` own keyboard allocation and dispatch. Camera, focus, pause, build, attack, and group keys are reserved. Ability buttons use the same resolver, with Shift and Alt+Shift fallbacks for large mixed selections. Inputs, contenteditable fields, modals, pause, and game-over states gate dispatch.
- Existing roster abilities already cover every unit; this pass adds no abilities or economy mechanics. Local bounded presentation events distinguish healing/repair, shields/cloak, mobility, deployment, teleport/scan, and explosive effects.
- Cancel remains visible outside scrolling command rows. Production cards explain resource, supply (including queued units), power, construction, prerequisite, and queue failures. Spawns avoid resources and reserved destinations, and favor the rally direction.

Run `node tests/turncraft-warfare-core.cjs` from the repository root for dependency-free simulation checks (192 assertions at this revision). The Playwright suite additionally checks keyboard dispatch, pointer ownership, cancellation, and desktop/portrait/landscape geometry, including 568×320. `.github/workflows/turncraft-regressions.yml` runs both suites on relevant PRs.

Local validation: simulation assertions and whitespace checks passed. Chromium and Headless Shell downloads returned invalid archives in the editing environment, so browser layout/input checks could not run locally. CI browser results and live lobby/device checks must be assessed separately.
