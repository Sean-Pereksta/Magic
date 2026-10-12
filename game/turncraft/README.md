# TurnCraft presentation checks

The game remains `../turncraft.html`. `polish.css` is loaded after the legacy styles. No build step or new runtime dependency is required.

## Controls

- Drag to pan; wheel/pinch or Zoom buttons to zoom. WASD/arrows pan; F focuses selection.
- Touch taps select. Choose an order before tapping a target; gestures never issue orders.
- Hold still for 380 ms, then drag a box to select friendly troops with mouse or touch. Shift-drag selects immediately; double-click/tap selects the visible unit type. Moving immediately still pans, and a second finger switches safely to pinch zoom.
- Ability buttons display Q/E/R/T and subsequent shortcuts, including on touch layouts. The same keys activate ready abilities or enter targeting; Escape cancels. Typing, modifier keys, pause, cooldowns and open dialogs suppress ability shortcuts.
- X arms Attack. Escape/Cancel Order cancels targeting. The numbered control groups retain tap/hold behavior.
- Mine uses existing automatic gathering on arrival. Stop suppresses gathering until a new mining/movement order.
- Build lists structures and lets adjacent human workers assist an unfinished building. Choose Refinery, Assimilator or Extractor and click a visible unoccupied gas vent at any distance: the worker travels to a reachable adjacent spot and constructs automatically. The cost is checked when ordering and charged once when construction begins. Replacement orders, unavailable vents, blocked routes and lost affordability cancel without a charge. Pending orders survive shared snapshots; Brood consumes its Drone only when construction begins. Ordinary buildings still require adjacent placement.

## Match setup

The faction cards use three original illustrated WebP assets in `assets/factions/`; their generation prompts and provenance are documented there. They load independently of optional battlefield sprites. The menu offers Easy, Medium, Hard and Pro buttons tied to the existing AI simulation tuning and saved preference. Solo opponent selection now creates the requested one or two opponents. Faction and difficulty controls keep keyboard focus while setup status updates.

## Checks

From this directory, run `npm install`, `npx playwright install chromium`, then `npm test`.
Set `CHROMIUM_EXECUTABLE_PATH` to use an existing Chromium executable.

The headless checks load the real game source and CSS, replacing Firebase imports and the network boot with an isolated test entry point. They cover all three construction styles, snapshot fields, hold/pan/pinch gestures, smooth camera controls, mining stop/resume and ownership, helper construction, ability hotkeys and effects, distant gas construction and cancellations, bounded particles, faction artwork, persistent difficulty choices, and menu/command geometry at 390×844, 844×390, and 1280×800. They do not contact Firebase. Set `TURNCRAFT_SCREENSHOT_DIR` to capture menu screenshots from the browser checks.

Live multi-client synchronization and physical-device frame rates require an actual lobby/device check. New construction fields travel through the existing entity snapshot; presentation events are observed locally without a second simulation.
