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
- `js/ai.js` — opponent decision-making and difficulty behavior.
- `js/animation-contact.js` — animation blending, footwork, contests, steals, and body contact.
- `js/ball-physics.js` — dribbles, rim/backboard response, rebounds, bounds, and loose balls.
- `js/scoring.js` — shot outcomes, finishes, physical blocks, possession, and scoring.
- `js/presentation.js` — cameras, minimal HUD, audio, particles, quality application, and utilities.
- `js/core/integrations.js` — controls/settings UI, tutorial, pause flow, and touch presentation.
- `js/bootstrap.js` — startup order.

## Input actions

Every device feeds the same internal actions: movement, sprint, shoot/finish, block/contest, steal, left skill, right skill, skill-stick direction, protect/stance, takeover, and pause. Local multiplayer automatically assigns one connected controller to Player 2 alongside Player 1 keyboard/touch; with two controllers, they become Players 1 and 2.

## Validation

All JavaScript files can be syntax-checked with `node --check`. The scripts are classic browser scripts and must remain in the order listed in `index.html` because they share the preserved game state while keeping each system in a separate file.
