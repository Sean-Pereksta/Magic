# Tiny Troops

`../roguecard.html` retains the existing emoji roster, stationary formation grid,
campaign regions, trait and formation mechanics, spells, mentors, relics, bench,
and 5–13 star progression. The new integration is loaded after that game data.

Every campaign is endless. Clearing a wave advances to the next wave; there is
no length setting or final-wave victory. Defeat records the army’s story in a
bounded, local history. Existing late regions and their enemy scaling continue
beyond wave 300. The fifth row retains its native wave/level-250 unlock.

## Files

- `core.js`: DOM-free rules, standardized role tags, soft counters, synergy
  requirements, specialization paths, commanders, modifiers, events, objectives,
  seeded drafting, and schema-3 save validation/migration. Its CommonJS export
  runs directly in Node tests.
- `lifecycle.js`: run-scoped timeouts. Reset, load, menu, and battle transitions
  invalidate stale callbacks.
- `runtime.js`: adapters around native mechanics, one fixed-step combat
  scheduler, pause/speed controls, exact wave previews, drafts and rewards,
  results/discovery/history, accessible dialogs, cached formation queries, and
  bounded visual effects. Combat speed changes wall-clock scheduling, while the
  simulation always advances by 0.25 seconds.
- `ui.css`: responsive battle-first layout, role/status indicators, restrained
  effects, keyboard focus, and reduced-motion handling.

The former `roguecard-polish.js` is no longer loaded. Do not load both runtime
adapters on the same page.

## Save behavior

The existing `tiny_troops_v1` profile format and local/cloud save identifiers are
retained. Schema 3 repairs missing or invalid fields and preserves progression,
stars, equipment, abilities, relics, and occupied fifth-row slots.

A combat save stores the pre-battle checkpoint, including its exact enemy
formation and already-consumed draft. Reloading replays that wave with the same
army and income. It grants neither another recruit nor another copy of combat
rewards. Pending boss relics, event choices, and unconsumed draft offers survive
reload. Incomplete native star/evolution choices are reopened from troop data.

Discoveries and the last 25 completed runs are stored locally per save name.
Unlocks add commanders and a draft modifier; they grant no permanent stat boosts.

## Verification

```sh
npm run test:tiny-troops
```

The browser suite needs Playwright and Chromium in the development environment:

```sh
npm install --no-save playwright
npx playwright install chromium
npm run test:tiny-troops:browser
```

An existing browser binary can be selected with `BROWSER_EXECUTABLE`.

The browser suite serves repository files on an ephemeral localhost port. It
checks start/restart, draft stability, pause and all three speeds, checkpoint
rollback, every valid recruit and effect, every battle ability and new path,
native 5–13 star choices for damage/support roles, boss relics, store purchases,
events, results/history, waves through 601, status deaths and invalid targets,
an 800-step large-army battle, and desktop/tablet/mobile layouts. Firebase SDK
requests are deliberately blocked to exercise the local-save fallback; live
cloud sync and leaderboard services are not covered by this offline suite.
