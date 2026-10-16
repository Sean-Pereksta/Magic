# Basketball 2

`index.html` is the full game entry point. The legacy `game/Basketball 2.html` URL redirects here so existing lobby links continue to work.

## Structure

- `styles/main.css` — menus, clean score presentation, tour map, touch controls, and responsive layouts.
- `js/state.js` — game state and data definitions.
- `js/core/settings.js` — persistent controls, simulation/arcade presets, and adaptive quality.
- `js/core/input-manager.js` — shared keyboard, touch, and Web Gamepad action layer.
- `js/campaign/national-tour.js` — cities, rivals, scouting, rewards, saves, XP, and map rendering.
- `js/ui.js` — menus, selection, tour flow, and game startup.
- `js/rendering.js` — arena, players, materials, shot meter, and procedural models.
- `js/input-gameplay.js` — action handling, modern 360-degree movement, game loop, and human control.
- `js/team-play.js` — individual/team membership, authoritative ownership, directional pass flight/interception, control switching, and roster lifecycle.
- `js/ai.js` — opponent decision-making and difficulty behavior.
- `js/animation-contact.js` — animation blending, footwork, contests, steals, and body contact.
- `js/ball-physics.js` — dribbles, rim/backboard response, rebounds, bounds, and loose balls.
- `js/scoring.js` — shot outcomes, finishes, physical blocks, possession, and scoring.
- `js/presentation.js` — cameras, minimal HUD, audio, particles, quality application, and utilities.
- `js/core/integrations.js` — controls/settings UI, tutorial, pause flow, and touch presentation.
- `js/bootstrap.js` — startup order.

## Input actions

Every device feeds the same internal actions. Player 1 uses WASD to move, Space to hold/release Shoot, C to Pass, F to Block, G to Steal, and V to switch to an available AI teammate. Player 2 uses IJKL, Enter, Comma, Numpad 2, Numpad 1, and Period. Existing Q/E and U/O skills, sprint, protect, takeover, and controller skill-stick inputs remain available.

Controllers use X / Square to Shoot, A / Cross to Pass, Y / Triangle to Block, B / Circle to Steal, and RB / R1 to switch. One controller is assigned to Player 2 in local play alongside Player 1 keyboard/touch; two controllers are independently assigned to Players 1 and 2. Switching changes the controlled character without changing device assignment or team membership.

Mobile uses one movement/facing joystick and two action buttons: Shoot and Pass on offense, Contest and Steal on defense. Pushing the joystick past 82% automatically sprints. Touch opacity, scale, and mirroring remain adjustable. Touch controls stay hidden on desktop until touch input is used.

## Match systems

Quick Play supports 1v1, 2v2, and 3v3, with one or two humans. Local team matches can be cooperative or competitive, with side-by-side or top/bottom views. Teams have distinct blue and coral jerseys/indicators. The National Tour and tutorial remain 1v1; existing tour saves, settings, courts, characters, and cosmetic selections retain their storage keys. Team setup preferences use a separate `basketball2MatchV1` key.

`matchPlayers` holds individual players with immutable IDs, `teamId`, and optional `controllerSlot`. `p1`/`p2` refer to the current controlled characters, so systems must resolve ball ownership by ID rather than assuming those two characters are opponents. `takePossession` grants exactly one player ownership. Passes lead a moving receiver, travel through space, and can be intercepted. `lastTouchPlayerId` controls out-of-bounds possession separately from shot credit.

Gameplay runs at a fixed 60 Hz, with position/rotation interpolation for rendering. Cameras translate with the action using a constant court-relative angle and distance, including both split views. Split rendering shares the shadow update. AI uses the same movement, stamina, collision, and shooting systems as humans; difficulty changes reaction and tactical decisions. Off-ball roles include spacing, cutting, screens/rolls, assignments/help, and rebounding/outlets.

Shot timing, distance, momentum, stamina, shot type and pressure determine repeatable physical aim error. Layups release a ballistic shot, while close dunk gathers can be blocked. Steals have a readable windup, a live reach interval, and recovery after a miss. Score/out-of-bounds delays advance with simulation, so pausing and restarting cannot trigger stale inbound callbacks.

## Validation

All JavaScript files can be syntax-checked with `node --check`. The scripts are classic browser scripts and must remain in the order listed in `index.html` because they share the preserved game state while keeping each system in a separate file.

Run the dependency-free regression suite:

```sh
node --test game/basketball2/tests/*.test.cjs
```

Run the real browser integration smoke test with the repository's Playwright development dependency:

```sh
node game/basketball2/tests/browser-smoke.cjs
```

The browser test serves source through request interception, without a development server. Set `BASKETBALL_CHROME` to use a system Chrome executable, `THREE_BUNDLE` to an existing Three.js 0.160 UMD bundle for offline testing, and `BASKETBALL_SCREENSHOTS` to an existing output directory to capture desktop/mobile views. It exercises all eight solo/local configurations, continuous AI possessions, passes/interceptions, switching, scoring, pause/rematch/menu, resize/split layouts, and Tour save/load.

Automated keyboard, touch-pointer, and single/dual gamepad-frame checks complement browser simulation. Physical touchscreen/gamepad feel and frame pacing on low-end devices still require on-device playtesting; AI tactics are lightweight heuristics, not a full basketball playbook.
