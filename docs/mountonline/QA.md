# Living Kingdoms verification — 2026-10-24

The overhaul was exercised on Windows with Node 24 and headless Chrome, using the actual static game and the local **demo-mountonline** Firestore emulator. Responsive results are browser emulation, not physical-phone or deployed-production measurements.

| Suite | Result |
| --- | --- |
| Shared simulation, campaign, saves, navigation, tactical combat, kingdom economy, diplomacy, motion and compression | **72 passing tests** |
| Original browser regressions | Desktop 1440 × 900, phone 390 × 844, landscape 844 × 390 pass: local saves, recruitment, promotions/reserves, travel, selection, groups, pinch/zoom, targeted orders, formations, abilities, field victory, siege breach and results; zero JavaScript errors or missing local assets |
| Living Kingdoms browser flow | Desktop and phone pass: all eight overlays, ten council cards, actual gift and counteroffer/acceptance effects, all twenty unique signature portraits, individual/group immediate attack and distributed targets, Shift+A targeted attack, Skirmish, Brace and overflow checks; all eleven training scenarios open and render |
| Movement browser regression | All three layouts pass irregular snapshots, burst updates, displayed-position selection and zero additional network requests |
| Firestore security | 62 allow/deny/transaction checks pass against each of the repository and complete supplied rule sets; unrelated supplied rule sections retain their original SHA-256 digest |
| Browser multiplayer | Real SDK/listeners/rules pass idempotent purchases and diplomatic agreements (including host-recovery persistence), protected profiles, shared battle, seat reconnect, abrupt host loss and restore, exactly one battle result, alliances, duplicate-tab ownership and verified legacy migration |
| Assets | All 34 troop definitions point to valid eight-frame rows; essential atlases plus hero total 2,350,478 bytes; versioned cache and manifest updated |

## Gameplay evidence

Scenario tests compare shield-covered and exposed projectile targets, frontal and rear strikes, splash falloff and physical obstruction, braced pikes and cavalry momentum, horse-archer firing/withdrawal/ammunition/stamina, terrain movement, group orders, and doctrine-based role assignment. Defensive objectives require actual friendly occupation; escort and raid objectives can resolve while enemies survive. Rearguards preserve escaped survivors and temporary reinforcement waves arrive once.

Kingdom tests verify counteroffer acceptance cannot spend twice, independent deterministic diplomacy/war, treaty expiry and betrayal, conquest and exhaustion, occupied settlement loyalty, gated recruitment, damaged production and desert supply costs, trade delivery payments, ransom and lord recovery, old/new campaign loads, and bounded terrain-free checkpoints.

Review regressions cover expired captive timers between strategic turns, duplicate delivery income after caravan defense, and an escaped rearguard incorrectly capturing a living pursuing lord. All are fixed. Allied reinforcement casualties return to the real lord's roster and never duplicate into the player's army. The expanded world retains every original settlement coordinate and original NPC identity.

## Measured budgets

- Sixteen players and 74 NPCs, 120 simulated seconds: mean world tick 0.436 ms; p95 1.175 ms; p99 3.091 ms on this host.
- A 1,200-troop battle measured mean 13.44 ms and p95 34.69 ms before final targeting-cache optimizations, inside the 50 ms simulation-step budget. These figures do not guarantee performance on every phone or the maximum aggregate battle load.
- Complete browser-flow frame p95: desktop 19.1 ms, phone 19.9 ms, landscape 26.2 ms. Canvas drawing averaged approximately 1.46–2.48 ms across those layouts.
- A 4,800-troop/16-player checkpoint and per-player views round-trip browser compression below the existing 850,000-character document guard. Terrain, props, navigation, and artwork are not transmitted through Firestore.

## Release checks

Publish the matching static game and Mountain Blade Firestore rules together. The pull request does not modify production Firebase rules or deploy the site. Check the deployed game with two real accounts and on physical mobile devices. Long campaigns still need balance testing for faction snowballing, recruitment prices, treaty pricing, supply pressure, and AI war outcomes. The existing peer-host trust model remains unchanged.

The earlier implementation records follow for historical context.

---

# Rebuild verification — 2026-10-16

Validation ran against the actual game source and optimized artwork, on Linux with Node v24.19.0 and headless Chromium 138. Desktop and mobile results below are browser emulation on this host, not measurements from physical phones or a production multiplayer deployment.

## Firestore-only correction

The production game now connects directly to Cloud Firestore. The Mountain Blade Node/Admin service, HTTP endpoints, Socket.IO namespace, service-account helper, start script, and API configuration have been removed. Existing Node services for other games are untouched.

| Suite | Result |
| --- | --- |
| Engine, progression, configuration and compression | 23 passing tests; exact supplied-rule preservation and 4,800-troop compressed checkpoint verified |
| Repository Firestore rules | 62 passing allow/deny/transaction checks |
| Complete rules based on the supplied deployed file | Same 62 checks passed; a SHA-256 check proves all other supplied game sections are unchanged |
| Browser multiplayer | Real Web SDK and real Firestore emulator rules: desktop/touch companies shared one battle; purchases retried concurrently committed once; direct unauthorized saves and lease theft failed; enemy orders were rejected; reconnect restored the correct battle seat; killing the host tab transferred authority and restored the battle; both profiles received exactly one result; movement, alliance membership/protection, and duplicate-tab ownership passed |
| Legacy migration | A URL name imported nothing; Console-style verified bindings imported gold, troops, reserve overflow, inventory, and a castle once; another reconnect did not duplicate troops |
| Capacity | One transaction wrote 16 profiles/views plus checkpoint/lease inside the security-rule access budget; a 17th company received a private full-realm message without replacing another player; a retired-server battle reference resumed its saved company without invented rewards |
| Responsive browser flow | 1440 × 900 desktop, 390 × 844 touch portrait, 844 × 390 touch landscape: zero JavaScript errors, no failed local artwork, no document overflow |

The browser flow also covers local saving, all 14 troop cards, settlement recruitment, promotions/reserves, map travel, halt, quality controls, desktop boxes/control groups, touch long-press/pinch, commands/formations, abilities, completed field battles, siege gate breaches, and results.

The Firestore test uses only `demo-mountonline`. In the browser fixture, test-only module responses supply emulated UID credentials to the real Web SDK. The rules, listeners, transactions, shared simulation, reconnection, and result settlement are real. Tests assert that **no `/api/mountonline` or Socket.IO requests occur**. The HTTP process serves static test files only. No production auth bypass, live rules publication, service-account access, production account writes, or production deployment is part of these tests.

The game is peer-hosted: the current elected browser is trusted to run the simulation. Rules fence host identity/epoch, ownership, private views, legal document shapes, immutable receipts and duplicate commits; they do not independently recompute battle outcomes or validate compressed simulation contents. When nobody hosts, the world pauses. An abrupt host disconnect takes up to the 20-second lease interval plus election/network time to hand over. Anonymous identities resume in the same browser; cross-device recovery requires the existing linked Firebase sign-in, rather than a display name alone.

## Continuous movement and lobby placement

- All 29 engine/configuration/compression/motion tests pass. Six new motion tests cover early/burst snapshots, duplicate position updates, correction bounds at 30/60/120 Hz, immutable gameplay data, reconnect/respawn/battle/identity resets, actor removal, and 1,200 reused troop poses.
- The actual Canvas app passes a dedicated irregular-snapshot browser test at desktop, phone portrait and phone landscape sizes. Commander/NPC/allied-company positions advance between saved steps; burst updates keep the current pose; battle troops glide; mouse and touch select a troop at its displayed position while its saved coordinate is several tiles ahead. Walking finishes at the destination. The fixture records zero extra network requests and zero JavaScript errors.
- The real Firestore browser multiplayer/recovery suite and both 62-check rules suites pass with the new presentation layer. Network schema, snapshot cadence, command persistence and rules are unchanged.
- Mountain Blade Online is promoted to the normal built-out games shelf and shared-world catalog above Experimental. Existing library/flagship regression suites pass (42 tests), and editorial metadata validation passes. Desktop/mobile catalog DOM checks confirm its normal card is visible above the collapsed Experimental group. Loader cache versions are bumped so existing browsers receive the updated categories.

## Performance samples

Seeded full-combat benchmark, including movement, targeting, projectiles, healing, collisions, and casualties; fixed simulation rate 20 Hz:

| Total troops | Steps sampled | Mean step | P95 step | Maximum step | Result snapshot |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 310 | 0.33 ms | 1.13 ms | 4.44 ms | 5.5 KB |
| 400 | 372 | 1.21 ms | 3.78 ms | 18.92 ms | 20.9 KB |
| 1,200 | 487 | 4.28 ms | 15.76 ms | 34.04 ms | 54.2 KB |

The 1,200-troop process reported approximately 19.8 MiB of used JS heap. This measures the simulation process, not total browser/GPU memory or end-to-end rendering latency. The shared simulation admission budget bounds simultaneous battle troops; compressed checkpoint guards reject oversized saves.

75 animation-frame samples during the 135-troop practice battle, after loading:

| Emulated viewport | Median frame interval | P95 interval | Mean sampled game work |
| --- | ---: | ---: | ---: |
| Desktop 1440 × 900 | 16.6 ms | 18.5 ms | 0.64 ms |
| Touch portrait 390 × 844 | 16.6 ms | 19.0 ms | 0.65 ms |
| Touch landscape 844 × 390 | 16.7 ms | 17.8 ms | 0.70 ms |

The sampled median is approximately 60 FPS on this host. Hardware, thermal throttling, browser scheduling, draw resolution, network conditions, and larger visible armies can change that result. There was no controlled old-versus-new hardware benchmark; these are direct measurements of the rebuilt game.

Essential bitmap payload is 1,697,856 bytes: two troop atlases, one terrain/prop atlas, and the existing menu backdrop. Native SVG icons and the manifest add only a few kilobytes. The client preloads/decodes the bitmaps before enabling game entry and reuses a versioned cache.

## Release validation still required

Publish the matching static game files and Firestore rules to the existing project. The supplied-rules replacement is `docs/mountonline/firestore-complete.rules`; no Node server, Cloud Function, Realtime Database, extra host, service account, API URL, or custom index is needed. Optional older name-based company imports use two verified binding documents entered in the Firebase Console.

Smoke-test real Firebase sign-in, two production identities, castle ownership/garrison defense, real mobile-network reconnects, and physical-phone frame pacing. Local emulator/browser results do not represent those production/physical-device checks. No production rules or records were changed while preparing this PR.
