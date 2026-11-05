# Validation

The focused gameplay, controls, rendering, deployment, campaign, targeting, save and catalog suite passes **155 tests**. Existing library and flagship regressions pass **44 tests**, including the same two catalog checks: **197 distinct passing checks**.

```text
node --test game/flag-capture/*.test.mjs lobby/tests/flag-capture-catalog.test.cjs
node --experimental-vm-modules --test lobby/tests/lobby-library.test.cjs lobby/tests/flagships.test.cjs lobby/tests/flag-capture-catalog.test.cjs
node --check game/flag-capture/app.mjs
node game/flag-capture/playtest.mjs
git diff --check
```

Rules tests cover all 21 skills, five difficulty schedules, different faction movement, mode scoring and tiebreaks, drafting, summoning and Clash-only purchases. New scenarios cover water-endpoint attacks, pursuit around terrain, defenders holding zones, meaningful guarding, and seeded terrain stability across 27 map/size/density combinations.

Actual app input/scheduler tests cover slow frames, 0.55-second near-ready buffering, stale target cancellation, pause/countdown, simultaneous gesture ownership, drag/click suppression, deployment swaps and cleanup, and identical preview/start formations. Renderer tests exercise persistent keys, moving tokens, open dialogs, focus, cached artwork, and uninterrupted floating effects.

Browser verification covered the complete buy → arrange → countdown → battle flow, custom/local/short-campaign setup, exact placements for both armies, stable terrain, movement, contested zones, attack and healing targets/effects, restored health, pause and menu. Desktop and 390 × 844 layouts were inspected. The phone view had no horizontal overflow and retained both local docks. Browser warnings/errors were empty during those interactions. Physical simultaneous two-finger input was unavailable; actual-handler tests cover independent pointer IDs and snapshots.

The standalone HTML was regenerated, syntax checked and browser tested. See [PLAYTEST.md](PLAYTEST.md) for measured browser timing and 48 reproducible bot matches; those diagnostics establish specific improvements, not a claim of universal faction balance or human enjoyment.

The broader artwork inventory suite requires unrelated lobby assets omitted from this sparse checkout and was not included.
