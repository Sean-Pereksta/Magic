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
