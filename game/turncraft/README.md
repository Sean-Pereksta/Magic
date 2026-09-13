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
