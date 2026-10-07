# Tiny Troops

`../roguecard.html` retains the existing emoji roster, stationary formation grid,
campaign regions, trait and formation mechanics, spells, mentors, relics, bench,
and 5–13 star progression. The new integration is loaded after that game data.

Every campaign is endless. Clearing a wave advances to the next wave; there is
no length setting or final-wave victory. Defeat records the army’s story in a
bounded, local history. Existing late regions and their enemy scaling continue
beyond wave 300. The fifth row retains its native wave/level-250 unlock.

## Troop placement and bench

The visible **🪑 Bench** button opens three numbered reserve slots. Recruits can
go directly into an empty slot. Exact duplicates grant **+2 star progress** to
the selected board or bench troop; **Star Training** grants **+1**. Bench troops
retain their identity, equipment, growth, evolutions, and 5–13 star choices.

Placing a different recruit onto an occupied board or bench slot asks whether
to **Move to bench**, **Sell**, or **Cancel**. A full bench disables only the
move option. Cancellation keeps the recruit selected and the occupant intact.
Troops can deploy from the bench to empty board squares freely between battles;
swapping with an occupied square uses the existing one post-boss switch.
Combat bench inspection pauses the battle and cannot change reserves.

Save slots retain empty positions. Checkpoints serialize owned troops before
queued upgrade references, and keep the currently open star choice by troop ID,
so reloading an upgrade cannot remove or train a detached copy of a reserve.

## Combat recovery

The shared aura pulse helper is available to all native evolution and spell
callbacks. Battle spell-field initialization skips empty formation slots, and
periodic support pulses use actual board positions. Core setup failures enter
recovery instead of silently continuing with a partly initialized army.
Board-size queries log only an actual size change, keeping long combat out of
the old per-query console flood.

Combat errors stop the scheduler and invalidate pending callbacks. A persistent
recovery panel offers **Retry wave**, **Restore preparation**, **Saved runs**, and
**Retry with basic combat**. Retry and preparation restore the exact pre-battle
army, enemy plan, RNG, consumed draft, and income. Partial rewards are rolled
back, including when an error occurs during the victory transition. Resume,
speed changes, and returning from the menu cannot restart a broken simulation.

Basic combat is an explicit, single-wave fallback for persistent native ability
failures. It uses base stats plus purchased stat boosts, front-line targeting,
normal attacks, healing, shields, and role counters; advanced abilities and
their triggers are disabled for that wave. It does not skip enemies or grant
an automatic victory. Normal kill/victory rewards and the boss relic/store
flow settle once; normal combat returns on the next wave. A checkpoint saved
during a basic battle retains this setting for its replay.

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

Required popups can be closed with Close, Escape, or the backdrop. The **Continue
choice** button reopens the same pending reward or upgrade; closing never selects
an option or rerolls its offers. Troop upgrades finish before an overlapping event,
including after reloading a checkpoint. Event popups also offer **Skip event · no
reward**. Stale popup buttons repair the current view before applying a choice,
and Close stays visible while scrolling on short screens. Saved runs and the run
menu remain reachable while a choice is pending.

The footer keeps a full-width next-step button visible after every round. It
starts the next wave, reopens a pending choice, finishes the boss store, or opens
recruitment/bench deployment when the board is empty. During card placement it
stays visible with the instruction to place the card; during combat it shows
the battle status and resumes a paused fight.

The menu lists saved runs on this device with name, wave, army, coins, and saved time. Choosing a file fills its local save code and loads it. Cloud saves from another device can be loaded by name/code; successful loads are cached and then listed. New runs with an existing local save name receive a numbered name, preserving the earlier run and carrying over its local discoveries/unlocks when the code matches.

Every primary checkpoint keeps the last readable version as a recovery backup. Storage errors are reported accurately. Cloud writes retain their original profile identity, coalesce pending checkpoints, time out instead of blocking forever, and reject mismatched codes or older timestamps. A failed cloud read still permits a valid local load.

New runs check both current and legacy cloud names before selecting a numbered
save name. If a different or newer cloud run is discovered later, a reachable
**Resolve cloud save** button opens **Load cloud run**, **Save this run
separately**, and **Keep playing on this device**. Cloud loading first preserves
a separate device copy. A new conflicting local file does not shadow the older
cloud account just because its timestamp is newer. Failed requests unlock the
controls, and device-only loading keeps the cloud-name guard until ownership is
confirmed. A separate copy can save to the cloud even if device storage is full.
Loading and separating a run invalidate queued transactions for the old profile.

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
npm run test:tiny-troops:recovery-browser
npm run test:tiny-troops:bench-browser
npm run test:tiny-troops:popups-browser
```

This browser suite verifies visible files, reload, separate run names, backups,
full browser storage, page-exit checkpoints, and cloud adapter failure/lookup
flows. Cloud adapter cases use a deterministic SDK mock.
They also exercise cloud-name conflicts, legacy account discovery, separate
copies, offline and full-storage recovery, and clickable recovery controls.

The bench suite uses the shipped page to verify replacement choices, sale
payouts, three fixed slots, merges, training and damage/support star paths,
post-boss swaps, combat inspection, saved-file reload, and mobile interactions.

The popup suite reproduces overlapping event/evolution rewards and checks all
nine event options with real clicks, dismissal and identical offers on resume,
stale phases/buttons, single rewards, skipping, obsolete events, evolution,
mastery, high stars, specialization, relics, menu access, touch interaction,
sticky Close on short screens, and real save/reload of overlapping rewards.
It also checks visible next-wave controls after normal, event, and boss victories,
store completion, empty-board recruitment/deployment, pause/resume, and duplicate
start protection.

The recovery suite sustains every native boss definition for 160 simulation
steps, all 243 valid recruits for repeated periodic abilities, exercises the
five-second spell cadence, and injects repeated combat,
initialization, and delayed-callback errors. It verifies rollback, saved-file
safety, menu behavior, single rewards, fallback reloads, return to normal
combat, and reachable recovery controls on desktop and mobile.

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
