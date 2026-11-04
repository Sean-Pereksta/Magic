# Splinters — The Curious Table

A browser-local single-player card game against Mossback, Madame Moth, and Rook. Open `/game/Splinters.html` through the existing static host. No room, account, database, or network game service is used.

## Play

- Move the mouse to look when captured; hold and drag the table when the cursor is free. `E` switches cursor capture.
- Left click a center card to flip it for 4.5 seconds. Right click to take it, then click one of your floating hand cards or press `1`–`7` to return that card. Every exchange is exactly one for one. Escape cancels an unfinished exchange.
- On a touchscreen, drag to look, tap to flip, hold briefly to take, then tap a hand card to return it.
- Complete an exact recipe of four or five symbols, then use **Lock in** or `R`. Only the required cards burn. Choose a rival, and a specific hidden rival card where required, before a targeted recipe resolves. Global effects resolve immediately.
- `B` opens the 32-recipe book and pauses the rivals. Escape pauses the game. Fullscreen uses the browser's fullscreen API where supported.
- First to 60 embers wins. A game also ends after 80 complete rival rounds, with the highest score winning; equal scores share the win.

Eight symbol types support 32 selective recipes: 20 require five cards. There are no wildcard, arbitrary mixed-hand, or three-card recipes. A new hand begins with a discoverable near-match, rather than a free scoring set. The table maintains at least 12 center cards and every hand is replenished to seven; scattered center cards are capped at 24.

## Difficulty and forecasts

| Mode | Each bot's interval | Remembered center cards | Forecasts |
| --- | --- | --- | --- |
| Hearthside | 10 seconds | 4 | 3 |
| After Dark | 7 seconds | 9 | 1 |
| Witching Hour | 4.2 seconds | 24 | None |

Harder rivals inspect more cards, make fewer intentional mistakes, prioritize useful recipes and target leaders. Bots learn from revealed cards and their own discards; they do not evaluate hidden card symbols. Forecasts use only the player's hand and show exact missing symbols. Selecting a forecast marks useful hand cards. Rival clocks stop during pause, recipe browsing, a pending exchange, or spell targeting.

## Implementation and validation

- `engine.mjs`: deterministic rules, recipes, card ownership, effects, finite endings, and fair AI memory.
- `art.mjs`: locally drawn parchment, distinct colored symbol engravings, printed names, and a shared illustrated back.
- `scene.mjs`: timber cabin, modeled creature rivals, responsive 3D hand, card flips, falling refills, and ash particles. Reuses the repository's local Three.js distribution in `../receiver-window-qb/vendor/three.min.js`.
- `main.mjs` and `style.css`: controls, timed bot scheduling, forecasts, targeted casting, sound, fullscreen and responsive interface.

Run from the repository root:

```sh
node --test tests/splinters.test.mjs tests/splinters-ui.test.mjs
node --experimental-vm-modules --test lobby/tests/*.test.cjs lobby/tests/*.test.mjs
```

The rules tests cover every recipe and effect, atomic target selection and exchanges, replenishment and card ownership, reveal expiry, AI information limits, difficulty planning, selective match rates, and finite games. The lobby regression verifies direct solo launch even when a multiplayer room was previously remembered.
