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
- `saves.js`: browser storage, recovery backups, save selection, and a bounded cloud write queue.
- `save-menu.js`: available saved runs, one-click loads, honest save status, and page-exit checkpoints.
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

The menu lists saved runs on this device with name, wave, army, coins, and saved time. Choosing a file fills its local save code and loads it. Cloud saves from another device can be loaded by name/code; successful loads are cached and then listed. New runs with an existing local save name receive a numbered name, preserving the earlier run and carrying over its local discoveries/unlocks when the code matches.

Every primary checkpoint keeps the last readable version as a recovery backup. Storage errors are reported accurately. Cloud writes retain their original profile identity, coalesce pending checkpoints, time out instead of blocking forever, and reject mismatched codes or older timestamps. A failed cloud read still permits a valid local load.

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

## Save-specific checks and Firestore rules

```sh
npm run test:tiny-troops:saves-browser
```

This browser suite verifies visible files, reload, separate run names, backups,
full browser storage, page-exit checkpoints, and cloud adapter failure/lookup
flows. Cloud adapter cases use a deterministic SDK mock.

The repository's prior Firestore rules denied Tiny Troops' save namespaces.
The updated root rules allow authenticated named-save lookup and validated
writes, reject code changes and stale checkpoints, retain the legacy owner
lookup, and support native run scores and the top-ten leaderboard. They do
not enable collection-wide save browsing.

Install development tools and run the rules against a local demo emulator:

```sh
npm install --no-save firebase-tools@13.11.2 @firebase/rules-unit-testing@3.0.2 firebase@10.12.5
firebase emulators:exec --only firestore --project demo-tiny-troops --config firebase.rules.json "npm run test:tiny-troops:rules"
```

For production cloud saving, deploy the updated rules to the configured
Firebase project using an authenticated Firebase CLI:

```sh
firebase deploy --only firestore:rules --project bible-game-246c0 --config firebase.rules.json
```

Rule changes in GitHub do not update the Firebase service by themselves.
