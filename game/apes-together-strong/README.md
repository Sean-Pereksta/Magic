# Apes Together Strong

A solo, procedural horde survival game. The lobby registry launches
`/game/apes-together-strong.html` directly and offers that same self-contained
file as a download. Canvas artwork and Web Audio need no asset downloads,
accounts, multiplayer room, or server-side game state.

## Build and validation

Edit the modules and `shell.html` in this directory, then rebuild the checked-in
HTML. The build uses only Node's standard library.

```sh
npm run build:apes
node game/apes-together-strong/build.cjs --check
npm run test:apes
node --experimental-vm-modules --test lobby/tests/lobby-library.test.cjs
```

The optional browser check requires a local Playwright installation and its
Chromium browser. `CHROMIUM_PATH` can select an existing Chromium executable.
It exercises desktop and touch menus, movement, rescue, settlements, combat
rendering, settings, saved runs, and permadeath.

```sh
npm run test:apes:browser
```

## Design

- **Navigation:** apes and humans share a 28-unit clearance grid, bounded A*
  searches, cached routes, segment smoothing, and collision-aware sliding.
  Waypoints only advance when the next segment is clear. Destroyed obstacles
  invalidate routes. Broad tree canopies provide concealment; smaller solid
  trunks leave room for the horde. Old trail steering no longer pulls followers
  backward. Regression tests include enclosed walls, bridges, 100 mixed units
  in dense trees, and 100 followers in an actual generated forest.
- **Horde spacing:** nearby active apes gently make room even when holding,
  attacking, or already close to their destination. Exact overlaps separate in
  stable directions, children use smaller spacing, and followers leave room
  around the crown. Larger hordes keep wider, loose following offsets. Local
  spacing respects obstacles and water, preserves attacks and commands, and
  leaves distant sleeping settlements alone. Tests cover clumps, corridors,
  different update rates, and a moving 100-ape horde.
- **World:** frontier, watershed, orchard, highland, research, and military
  districts influence landscape and development. Major hubs occupy spaced dry
  slots between river bands. Smaller camps, checkpoints, and research outposts
  connect to roads. Compounds have front gates, service openings, and seeded
  layout variants. Distant garrisons retain their wounds and surviving members
  in the save without exhausting the active force budget.
- **Combat:** six ape attack poses, five fall variants, recoil, interruption,
  fading bodies, dropped weapons, and a falling king. Nine human roles include
  trackers, radio officers, shield guards, marksmen, grenadiers, medics, assault
  scouts, and heavy gunners. Marksmen lock a warned point before firing; grenade
  circles give 1.8 seconds to escape. Shield guards reward flanking or a swarm.
  Officers illuminate remembered positions with flares. Vehicles, searchlights,
  alarms, finite reinforcements, and helicopters remain part of the hunt.
- **Balance:** adults have 120 health (roughly 2–4 ordinary hits on Survival),
  scouts 150, and young 60. Marksmen enter tier 3+ forces and retain warned,
  lethal shots. Captive counts are substantially higher: checkpoints hold
  18–32, prisons 40–70, detention camps 80–120, and experimental camps 120–180.
  Military liberation awards 30 food per site tier once, and camp supplies and
  ape score rewards are larger. Rescue/population threat contributions are
  lower so one successful rescue does not immediately trigger a military hunt.
- **Settlements:** food patches, water, fertility, nearby timber, housing, and
  local human activity affect a camp. Adults forage, build, or guard. Work
  priorities favor growth, food, or defense. Construction produces shelters,
  gardens, lodges, stores, and defenses; population growth requires food, room,
  and safety. Visit to deliver food or recruit adults. Raids consume defenses
  and supplies, and scouts give advance warnings.
  Lodge targets advance one level per 12 inhabitants, reaching level 10 at 108;
  upgrades still require building work and timber. Construction and timber
  gathering are faster, shelters add 12 housing, and gardens produce more food.
  Each eight adults contribute one family work unit per second toward a birth
  every 30 units, subject to food, housing, safety, local capacity, and the
  600-ape limit. Young mature after 35 seconds, including while far from the king.
- **Presentation:** illustrated night forest, pines and hanging wetland trees,
  orchard trees, reeds, farm rows, landscape details, construction and garden
  visuals, region HUD, map management, and a seeded menu. Procedural audio adds
  footsteps, river ambience, varied impacts and falls, and distinct threat cues.
  Low detail, reduced motion, mute, and volume controls are available.

## Controls and persistence

WASD/arrows move; Shift sprints; Ctrl sneaks; click/Space attacks. Q calls,
E charges toward the pointer, R recalls, F holds, Z settles nearby followers,
Shift+Z settles all, and C assigns scouts. M/Tab opens the map and settlement
controls; Escape pauses. Touch uses a movement stick, strike button, and command
menu.

The original `ats-crown-save-v1` storage key and JSON save format are retained.
Older tree collision and settlement records migrate on load. Navigation caches
are rebuilt; they are not serialized. Autosave runs every 15 seconds, with
manual save and JSON export/import in Pause. The king's death removes the
living checkpoint and records a legacy score.

Balance revision 2 migrates older ape health proportionally, retains family
progress, and increases rewards in existing unbroken cages once. Cleared camps
and past rescue statistics retain their history.

The new regional layout applies to newly generated chunks. Already explored
areas in imported saves keep their existing structures.
