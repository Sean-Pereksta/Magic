# Validation

The focused rules, campaign, targeting, save, and catalog suite passes **101 tests**. The existing library and flagship regressions pass **44 tests**, including the same two catalog checks. Together these are **143 distinct passing checks**.

```text
node --test game/flag-capture/*.test.mjs lobby/tests/flag-capture-catalog.test.cjs
node --experimental-vm-modules --test lobby/tests/lobby-library.test.cjs lobby/tests/flagships.test.cjs lobby/tests/flag-capture-catalog.test.cjs
node --check game/flag-capture/app.mjs
git diff --check
```

The rules suite exercises all 21 abilities, independent cooldowns, terrain and movement distinctions, connected symmetric custom maps, scoring and contested zones, recruitment, all victory conditions, and completed AI matches. The campaign and storage suites exercise unlocks, replay, malformed data, every supported board/mode combination, and flight landing saves. Target tests verify blue movement, red damage, green healing only, gold support, and canonical skill actions.

Browser verification used the actual game UI: custom 11 × 9 Zone Control with wild terrain, campaign mission deployment on 7 × 5, paused planning, grid movement and flag capture, healing a wounded leader with a separate special cooldown, switching to red attack targets, and a drawn attack effect. Additional browser checks covered buying the entire starting budget, placing a purchased troop, summoning two controllable minions with visible effects, two-player top/bottom bars, Player 2 movement and a special attack, Player 1 healing with green particles and a health bar updating from 7 to 10, automatic countdown, and absence of the reinforcement shop in standard mode. Browser console reported no warnings or errors during those interactions. Physical simultaneous two-finger input was not available in the browser testing interface; the application uses independent pointer IDs and per-player selection.

The broader artwork inventory suite requires the repository's unrelated lobby asset collection, which is omitted from this sparse checkout; it was not part of the passing suite above.
