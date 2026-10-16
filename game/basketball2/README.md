# Basketball 2

`index.html` is the full game entry point. The legacy `game/Basketball 2.html` URL redirects here so existing lobby links continue to work.

## Structure

- `styles/main.css` — menus, clean score presentation, tour map, touch controls, and responsive layouts.
- `js/state.js` — game state and data definitions.
- `js/core/settings.js` — persistent controls, simulation/arcade presets, and adaptive quality.
- `js/core/input-manager.js` — shared keyboard, touch, and Web Gamepad action layer.
- `js/core/controller-config.js` — validated, saved per-player controller assignments, button/trigger/combo maps and stick settings.
- `js/core/controller-ui.js` / `styles/controller.css` — controller setup, input capture, manual mappings and live input testing in Controls & Settings.
- `js/campaign/national-tour.js` — cities, rivals, scouting, rewards, saves, XP, and map rendering.
- `js/ui.js` — menus, selection, tour flow, and game startup.
- `js/rendering.js` — arena, players, materials, shot meter, and procedural models.
- `js/input-gameplay.js` — action handling, modern 360-degree movement, game loop, and human control.
- `js/team-play.js` — individual/team membership, authoritative ownership, directional pass flight/interception, control switching, and roster lifecycle.
- `js/player-models.js` — cached GLB loading, isolated skeletons/materials, animation blending, hand/forearm attachment and arm IK; safe generated-player fallback.
- `assets/models/` — original guard/wing/center GLBs, each with 24 joints and 12 movement clips; `tools/build-models.cjs` reproducibly authors them without dependencies.
- `js/ai-reads.js` — deterministic shot, drive, passing-window and safe setup-space assessments with bounded movement prediction.
- `js/ai-coordination.js` — shared rim receiving pockets, weak-side spacing, drive containment, receiver tags and gap help.
- `js/ai.js` — playbooks, opponent decisions, action execution and difficulty behavior.
- `js/animation-contact.js` — animation blending, footwork, contests, steals, and body contact.
- `js/ball-physics.js` — dribbles, rim/backboard response, rebounds, bounds, and loose balls.
- `js/scoring.js` — shot outcomes, finishes, physical blocks, possession, and scoring.
- `js/presentation.js` — cameras, minimal HUD, audio, particles, quality application, and utilities.
- `js/core/integrations.js` — controls/settings UI, tutorial, pause flow, and touch presentation.
- `js/bootstrap.js` — startup order.

## Input actions

Every device feeds the same internal actions. Player 1 uses WASD to move, Space to hold/release Shoot, C to Pass, F to Block, G to Steal, and V to switch to an available AI teammate. Player 2 uses IJKL, Enter, Comma, Numpad 2, Numpad 1, and Period. Existing Q/E and U/O skills, sprint, protect, takeover, and controller skill-stick inputs remain available.

The default controller layout uses X / Square to Shoot, A / Cross to Pass, Y / Triangle to Block, B / Circle to Steal, RT / R2 to Sprint, LT / L2 to Protect, LB / L1 for the left skill, and Menu / Options to Pause. RB / R1 switches teammates in team games and performs the right skill in 1v1. Press both stick clicks for Takeover. Switching characters preserves the player's device assignment and custom controls.

Open **Controls & Settings → Controller setup**, connect an Xbox controller by USB or pair it through the device's Bluetooth settings, and press a controller button so the browser exposes it. Choose Player 1 or Player 2 and select the controller. Each controller is assigned to one player. Automatic assignment supports a controller for solo Player 1, one controller for local Player 2 alongside Player 1 keyboard/touch, or two controllers for independent local players. Surviving controllers retain their slots after a disconnect. Player 2 can be configured before starting a local match; the Settings shortcut opens two-player match setup.

Choose an action's **Edit** button, release held controls, then press the desired button, trigger or axis. Every gameplay action can be remapped, including Pause and Takeover; two-input combos and manual selection are also available. Conflicting inputs swap where possible, and any additional action needing a replacement is shown as Unassigned. Movement and skill sticks support separate axis selection, direction reversal, disabling and a radial deadzone. The live tester shows mapped actions and stick movement without affecting gameplay. Settings save separately under `basketball2.controllerSettings.v1`; each player can reset their defaults independently.

Controller settings are available from the main menu and the pause menu. Opening setup, changing assignments/bindings, disconnecting, losing focus or resuming cancels pending shots and requires held inputs to return to neutral. Configuration and testing cannot shoot, move, pass or resume the match. Keyboard and touch controls remain available, and controller haptics follow the assigned human player. Browser device indices and model IDs identify assignments; pairing and the initial device connection remain managed by the operating system and browser.

Mobile uses one movement/facing joystick and two action buttons: Shoot and Pass on offense, Contest and Steal on defense. Pushing the joystick past 82% automatically sprints. Touch opacity, scale, and mirroring remain adjustable. Touch controls stay hidden on desktop until touch input is used.

## Match systems

Quick Play supports 1v1, 2v2, and 3v3, with one or two humans. Local team matches can be cooperative or competitive, with side-by-side or top/bottom views. Teams have distinct blue and coral jerseys/indicators. The National Tour and tutorial remain 1v1; existing tour saves, settings, courts, characters, and cosmetic selections retain their storage keys. Team setup preferences use a separate `basketball2MatchV1` key.

`matchPlayers` holds individual players with immutable IDs, `teamId`, and optional `controllerSlot`. `p1`/`p2` refer to the current controlled characters, so systems must resolve ball ownership by ID rather than assuming those two characters are opponents. `takePossession` grants exactly one player ownership. Passes lead a moving receiver, travel through space, and can be intercepted. `lastTouchPlayerId` controls out-of-bounds possession separately from shot credit.

Gameplay runs at a fixed 60 Hz, with position/rotation interpolation for rendering. Cameras translate with the action using a constant court-relative angle and distance, including both split views. Split rendering shares the shadow update. AI uses the same movement, stamina, collision, and shooting systems as humans; difficulty changes reaction and tactical decisions. Off-ball roles include spacing, cutting, screens/rolls, assignments/help, and rebounding/outlets.

Bots assess the entire route to a finish, including help at the rim and moving defenders. An open rimward drive continues into a short running layup gather; a settled shooting specialist can take a good open jumper instead. A covered paint leads to an open teammate, a clear receiving pocket beside the rim, or a safe lateral/backout setup. Passes are checked again before release so a rotating defender can close the window. A guarded catch is not treated as an open rim feed. The finish owner leaves movement to the gather solver, and defenders continue contesting the gather rather than prematurely chasing a rebound.

Within 2.2 meters, a strong finisher can attack a smaller single defender using the actual height, width, finishing skill and defender reach matchup. Equal-sized rim protection, a well-timed airborne block, extra paint help or fatigue can remove that opportunity. These reads change the attempt decision; the existing release height, ball flight and collisions still decide the outcome. A clear pass to a big player sealing a smaller guard is a viable feed even when the catch is not completely unguarded, but actual passing-corridor clearance remains mandatory.

Direct drives compare the quality of the finish with the current jumper and nearby passing options. A committed clear run continues instead of stopping for a similar lateral midrange pass, while a useful rim feed or superior clean three can still win the decision. Help closing the lane triggers a new pass or setup read. Off-ball players prioritize a single useful seal and preserve the handler's driving corridor. Defenders beyond the takeoff point no longer falsely count as bodies occupying an empty lane; real rim protection and rotating help remain threats.

Jumper decisions include the shooter's range, pressure, fatigue and foot speed. Reads use the court's actual three-point arc and give a modest value advantage to a clean, in-range three; deep or covered threes receive no advantage. Passing compares the teammate's catch-and-shoot value with the handler's current look. AI passes preserve the intended receiver after the passer visibly turns, using the same pass flight and interception physics as human passes.

Each difficulty has separate court vision, stable spatial read error, bounded velocity look-ahead, pass advantage, drive commitment and decision time settings. Harder levels identify smaller openings sooner, coordinate one dump-off receiver with spaced outlets, contain projected drives, tag rim cutters and deny likely catches. Expert weak-side help shows in a beaten driving gap while preserving the existing two-player trap limit. Player speed, jumping, stamina and jumper timing variation remain shared across difficulty levels.

Each team can choose an offense in Quick Play: **Balanced** mixes reads, **Motion** uses shared cut/fill phases and pass-and-cut actions, **Pick & Roll** coordinates a screener, the handler's turn, a roll, and a weak-side outlet, and **Drive & Kick** spreads outlets before attacking and passing against help. These choices change AI targets, movement, and passing priorities. Human teammates remain under human control. The playbook adapts to 2v2/3v3 and resets after turnovers and inbounds.

Defenders maintain separate matchups. A shared defensive plan can send exactly one AI helper to a vulnerable sideline/corner handle or beaten paint drive while the third defender rotates into uncovered passing lanes. It accounts for nearby human help and open rim threats. Traps end on a pass, escape, or timeout and have a recovery cooldown; they are not permanent team-wide ball chasing. Only one defender pursues an interception while the others retain coverage.

The menu uses original illustrated artwork in `assets/menu-hero.webp` with live HTML controls and native SVG mode/tactic diagrams. See `assets/README.md` for its generation prompt and provenance. Courts, hoop, basketball and effects remain generated in code. Players load original skinned GLB models with run/sprint, shuffle/pivot, jump-shot, layup/dunk, pass/catch, block and crossover clips. Colors, selected jersey numbers and shoe colors are applied per player. The loader shares mesh buffers while each athlete has independent bones, animation and materials. Assets load during the menu from `https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/`, with bundled GLBs as a fallback if the host is unavailable; generated players remain available if both sources fail. See `assets/models/README.md` for hosting and CORS configuration. Covered court rendering and cosmetic menu previews redraw at 12 Hz while the HTML UI retains normal browser responsiveness.

Shot timing, distance, momentum, stamina, shot type and pressure determine repeatable physical aim error. Good open jumper timing has a forgiving band; standing close shots select a layup, and tapped open layups receive finishing touch while still launching a real ball. The gather keeps the release clear of the rim underside. Pressure, body contact and timed blocks still matter. Glass dimensions and rim tube radius come from one render/collision contract; swept slab collisions reflect once, torus contacts use their 3D normal, and scoring requires a downward plane crossing with clearance for the whole ball. Bank shots physically rebound off the board. Steals have a readable windup, a live reach interval, and recovery after a miss. Score/out-of-bounds delays advance with simulation, so pausing and restarting cannot trigger stale inbound callbacks.

## Validation

All JavaScript files can be syntax-checked with `node --check`. The scripts are classic browser scripts and must remain in the order listed in `index.html` because they share the preserved game state while keeping each system in a separate file.

Run the dependency-free regression suite:

```sh
node --test game/basketball2/tests/*.test.cjs
```

Run the real browser integration smoke test with the repository's Playwright development dependency:

```sh
node game/basketball2/tests/browser-smoke.cjs
node game/basketball2/tests/menu-layout.cjs
node game/basketball2/tests/player-gameplay-browser.cjs
node game/basketball2/tests/ai-gameplay-browser.cjs
node game/basketball2/tests/ai-mismatch-browser.cjs
node game/basketball2/tests/controller-browser.cjs
```

The browser tests serve source through request interception, without a development server. Set `BASKETBALL_CHROME` to use a system Chrome executable, `THREE_BUNDLE` to an existing Three.js 0.160 UMD bundle for offline testing, and `BASKETBALL_SCREENSHOTS` to an existing output directory to capture desktop/mobile views. Gameplay checks exercise all eight solo/local configurations, all four playbooks, continuous AI possessions, passes/interceptions, switching, scoring, pause/rematch/menu, resize/split layouts, and Tour save/load. Menu checks cover four desktop/mobile sizes, artwork decoding, keyboard focus/activation, and style persistence through reloads and Tour. Player gameplay checks decode all three GLBs in Three.js, verify skeletal movement, make 54 open layups/jumpers using keyboard events across the three builds and both sides of the basket, and verify split/mobile rendering and repeated model disposal. Runtime unit tests use `THREE_BUNDLE` when supplied; asset and shooting pipeline tests do not require a renderer.

Automated keyboard, touch-pointer, and single/dual gamepad-frame checks complement browser simulation. Physical touchscreen/gamepad feel and frame pacing on low-end devices still require on-device playtesting; AI tactics are lightweight heuristics, not a full basketball playbook.

AI browser checks run the actual bot, movement, passing, gather, GLB animation, collision and scoring systems. They verify close finishes across all three player builds, running drives from 5–6 meters, a specialist's open jumper and three, rejection of covered/out-of-range threes, completed rim feeds with nearly aligned teammates, a safe reset against packed paint, a real layup contest and more anticipatory expert defense. The court-read and coordination tests cover blocked paths, help defenders, predicted interceptors, guarded catches, stable perception error, single receiving reservations, spacing and recovery.

The mismatch browser check verifies immediate big-on-small finishes across twelve seeded possessions, caution against rim protection/help/timed blocks, sustained clear-lane running layups across four player builds, retention of a valuable open three, and a genuine gather collision that can still deflect a size-advantaged attempt.

Controller tests cover one/two Xbox-style input streams, independent player maps, hotplug, saved and malformed configuration, axes/triggers/combos, safe rebinding and neutral release gates. The browser check supplies virtual Gamepad API hardware while running the actual UI, input manager, animation, movement, shooting, passing, skills, takeover and pause logic; it also checks capture/manual input, persistence, disconnects and desktop/mobile setup layouts. Physical controller pairing, device-specific drivers and hardware feel require a real connected controller.

Browser gameplay tests use the bundled GLBs as hosted-response fixtures by default. Set `BASKETBALL_LIVE_MODELS=1` to request the actual R2 files; the player gameplay test then requires all three remote assets to load without fallback, testing the public host's browser CORS behavior as well as gameplay.
