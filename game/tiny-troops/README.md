# Tiny Troops

`../roguecard.html` retains the existing emoji roster, stationary formation grid,
campaign regions, trait and formation mechanics, spells, mentors, relics, bench,
and 5–13 star progression. The new integration is loaded after that game data.

Every campaign is endless. Clearing a wave advances to the next wave; there is
no length setting or final-wave victory. Defeat records the army’s story in a
bounded, local history. Existing late regions and their enemy scaling continue
beyond wave 300. The fifth row retains its native wave/level-250 unlock.

## Tactical battle and management update

The battlefield now has allied/enemy health and remaining-enemy indicators,
collapsible battle events, compact troop inspection, and a persistent reserve
strip. Selecting a troop keeps the battlefield available; **Inspect** opens its
complete native abilities, equipment, and progression. **Team** contains Troops,
Formation, Evolutions, Towers, and Inventory sections. Small screens scroll the
battle area and return the formation to view after a panel closes.

Enemies use a responsive multi-column grid on desktop, tablet, and phone. Its
own vertical scroll area shows at least two full rows and keeps every remaining
enemy reachable with wheel, keyboard, or touch. It overrides the older fixed
single-row height rather than clipping later rows inside the enemy box.

The Elemental Shrine offers eight lasting blessings plus its coin option:
Ember (Fire burn), Winter (Frost starting shield), Storm (chain chance), Venom
(poison), Dawn (healing), Arcane (Caster damage), Gale (Ranged tempo), and Valor
(Fast-striker first-hit damage). Existing blessing IDs and saved bonuses remain
compatible; postponing or closing the Shrine still spends nothing.

Tap a reserve and then an empty square, or drag a reserve/deployed troop onto a
legal highlighted square. Invalid drops retain the original formation. Deployed
troops can swap freely during preparation. Returning a troop to an empty bench
slot is free; occupied reserve swaps retain the original post-boss switch rule.
**Auto Arrange** favors defenders on the right, support in the middle, and ranged
troops on the left. Three formation presets store stable troop IDs and positions;
loading a preset preserves newly acquired troops and cannot bypass reserve swaps
or the fifth-row unlock.

At 5★ Evolution and 6★ Mastery, compare the actual native path statistics,
appearance, and ability descriptions before pressing **Confirm**. Previewing
uses a detached troop copy. Existing 7–13★ progression is retained. Closing an
upgrade, specialization, event, or relic stores the same decision under
**Choices** and allows preparation and later battles. Postponed decisions survive
saving/reloading, retain their offer IDs, and do not award or spend resources.
Confirming an evolution returns to preparation; the player starts the battle.
Canceling a selected draft returns the same offers without losing the consumed
recruit. An unfinished selected card also survives save/reload.

### Tower armory and loot

The original game has stationary roster defenders rather than a separate tower
placement subsystem. **Armory** equips those existing defenders, including ranged,
heavy, elemental, and support units, without changing their native targeting or
removing troop effects, spells, mentors, relics, or upgrades. Each defender has
one slot; the second unlocks at 5★. Every inventory copy has its own ID and can
occupy only one slot. Replace/unequip returns the old item. Selling or permanently
removing a defender returns its items; a temporary defeat retains them for the
native next-wave revival. Preparation-only changes are validated before mutation.

All 18 requested items are available, from Sharpened Bolts through Ancient Siege
Relic. The grid filters rarity, effect category, compatibility, and equipped
status. Preview shows exact bonuses and assignment before equipping. Because
native focus targeting has unrestricted reach, lens items provide an explicitly
described +8%/+15% chance to extend a shot to another enemy at 40% damage, rather
than inventing a tile-range limit. Damage, tempo, durability, burning, slowing,
poison, piercing, splash, shields, chain, echo, and periodic area effects affect
native combat. Frozen Barrage, Burning Front, Shield Wall, Poison Volley, and
Siege Breaker show their active requirements/bonuses in Armory.

The first victory guarantees Sharpened Bolts. Subsequent eligible normal/elite/
boss victories use 12%/35%/100% drop chances. Rarity weights are
52% common, 28% uncommon, 14% rare, 5% epic, 1% legendary. Seven consecutive misses
guarantee the next drop. A saved per-wave ledger settles loot once; rollback
restores the ledger, inventory, and seeded RNG together. Challenges can use an
explicit reward-specific transaction ID. These are starting balance values.

### Visual effects and safeguards

Full, Reduced, and Minimal settings are available in **Battle settings**. Full
shows melee lunges, directional impacts, traveling elemental projectiles,
ability labels, area bursts, defeat feedback, and distinct combat numbers.
Device reduced-motion preferences take priority. Effect nodes are reused, with
caps of 48 desktop / 28 mobile (18 Reduced, 8 Minimal), one animation scheduler,
and cleanup on pause, battle end, reset, menu, and page exit. Rendering errors
are isolated from combat calculations. Effects never resolve damage themselves.

The lifecycle reports Preparation, Running, Paused, Victory, Defeat, or Recovery
and validates battle tokens/targets before actions. Canceled queued timers cannot
run after cancellation. Detached or already defeated targets cannot grant damage
or rewards. Upgrade transactions roll back troop and shared-resource changes on
failure. Recovery retains the original preparation snapshot and exact wave;
pause/menu/speed controls cannot restart a broken simulation. Technical failures
are logged for debugging; existing backup and cloud-conflict recovery remain.

New adapter files are `equipment-rules.js`/`equipment.js`,
`management-rules.js`/`management.js`, `presentation.js`/`presentation.css`, and
`navigation.js` (loaded last). Browser Back dismisses one live panel, placement,
or inspection at a time. X/confirmation removes its temporary history entry;
when no temporary UI remains, Back can leave the game normally. History state
from the host page is preserved, including rapid close/open sequences.
They load after the native runtime; all added progress stays in validated `S.tt`
extensions. No new production dependency or server is needed.

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

Decision popups close with X, Escape, or the backdrop and move into the saved
**Choices** queue. Closing never selects an option or rerolls its offers. An open
upgrade takes precedence over an overlapping event until confirmed or postponed,
including after reloading a checkpoint. Event popups also offer **Skip event · no
reward**. Stale popup buttons repair the current view before applying a choice,
and X stays visible while scrolling on short screens. Saved runs and the run
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
npm run test:tiny-troops:equipment-browser
npm run test:tiny-troops:management-browser
npm run test:tiny-troops:lifecycle-browser
npm run test:tiny-troops:tactical-browser
npm run test:tiny-troops:navigation-browser
npm run test:tiny-troops:enemy-grid-browser
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

The equipment suite measures actual item effects, unique-copy transactions,
sale/defeat behavior, save migration, duplicate victory calls, and checkpoint
replay. The management suite exercises every valid native troop definition,
tap/drag/invalid placement, presets, reserve switches, native evolution/mastery
previews, confirmation/postponement, saved progress, and touch controls. Lifecycle
regressions cover selected-card persistence, individually canceled callbacks,
atomic upgrade rollback, stale targets, duplicate end, postponed milestones and
boss rewards, manual pause, and invalid-state recovery. Tactical presentation
checks include capped/reused effects, rendering failure isolation, reduced
motion, desktop/tablet/phone layout, and one-click dismissal. Browser tests use
emulated touch; physical-device performance and live cloud services still need
release smoke checks. Set `TT_SCREENSHOT_DIR` for optional tactical preview PNGs.
The navigation suite uses real browser Back/Forward, including stacked choices,
evolution postponement, reserve/selected-card cancellation, repeated X, rapid
replacement panels, private dismissal paths, and return to a previous page.

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
