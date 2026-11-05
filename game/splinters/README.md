# Splinters — The Curious Table

A browser-local single-player card game against Mossback, Madame Moth, and Rook. Open `/game/Splinters.html` through the existing static host. No room, account, database, or network game service is used.

## Play

- Move the mouse to look when captured; hold and drag the table when the cursor is free. `E` switches cursor capture.
- Left click a center card to flip it for 4.5 seconds. Right click to take it, then click one of your floating hand cards or press `1`–`7` to return that card. Every exchange is exactly one for one. Escape cancels an unfinished exchange.
- On a touchscreen, drag to look, tap to flip, hold briefly to take, then tap a hand card to return it.
- Complete an exact recipe of four or five symbols, then use **Lock in** or `R`. Only the required cards burn. Choose a rival, and a specific hidden rival card where required, before a targeted recipe resolves. Global effects resolve immediately.
- `B` opens the 32-recipe book and pauses the rivals. Escape pauses the game. Fullscreen uses the browser's fullscreen API where supported.
- Choose a game mode and difficulty before taking a seat. The mode sets the winning objective and table rules; difficulty sets rival skill and forecasts.

Eight symbol types support 32 selective recipes: 20 require five cards. There are no wildcard, arbitrary mixed-hand, or three-card recipes. A new hand begins with a discoverable near-match, rather than a free scoring set. The table maintains at least 12 center cards and every hand is replenished to seven; scattered center cards are capped at 24.

## Game modes

| Mode | Objective | Table rules |
| --- | --- | --- |
| Ember Race | First to 60 embers | Remember briefly revealed center cards and build scoring recipes. |
| Quickfire | First to 30 embers | Rivals act at 75% of their usual interval for a shorter, faster game. |
| Lanternlight | First to 60 embers | All center symbols stay face up, including newly dealt and returned cards. |
| Recipe Hunt | First to burn five different recipes | A recipe counts once toward discoveries. Repeating it still scores embers and casts its effect. |

All four modes are single player against the three creatures. The score display and results use the selected objective. A game also ends after 80 complete rival rounds, with the highest objective progress winning; equal progress shares the win. Starting another round retains the selected mode and difficulty. Choose **New evening** or **Change mode or difficulty** to return to setup.

## Difficulty and forecasts

| Difficulty | Each bot's base interval | Remembered center cards | Forecasts |
| --- | --- | --- | --- |
| Hearthside | 10 seconds | 4 | 3 |
| After Dark | 7 seconds | 9 | 1 |
| Witching Hour | 4.2 seconds | 24 | None |

Harder rivals inspect more cards, make fewer intentional mistakes, prioritize useful recipes and target leaders. Bots learn from revealed cards and their own discards; they do not evaluate hidden card symbols. Forecasts use only the player's hand and show exact missing symbols. Selecting a forecast marks useful hand cards. Rival clocks stop during pause, recipe browsing, a pending exchange, or spell targeting.

Game modes and difficulty combine independently. For example, Quickfire with After Dark uses a 5.25-second base interval instead of 7 seconds; Lanternlight still uses the selected difficulty's planning and forecasts.

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

The rules tests cover every recipe and effect, atomic target selection and exchanges, replenishment and card ownership, reveal expiry, AI information limits, difficulty planning, selective match rates, mode objectives, and finite games. UI tests exercise registered touch controls, pause and targeting guards, fullscreen, mode selection, Quickfire scheduling, Recipe Hunt progress, and restart behavior. The lobby regression verifies direct solo launch even when a multiplayer room was previously remembered.
