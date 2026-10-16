# Rebuild verification — 2026-10-16

Validation ran against the actual game source and optimized artwork, on Linux with Node v24.19.0 and headless Chromium 138. Desktop and mobile results below are browser emulation on this host, not measurements from physical phones or a production multiplayer deployment.

## Functional results

| Suite | Result |
| --- | --- |
| Engine and progression | 21 passing tests |
| Mountain Blade Firestore security | 30 passing allow/deny checks against the full production rules file |
| Authoritative service | Verified name binding, legacy company/castle/inventory migration, late binding without losing newer progress, replay receipts, concurrent purchases, command ownership, forged-result rejection, PvP, restart recovery, connected-traveler recovery, atomic results, alliances, allied-castle protection, and lost-authority rollback passed |
| Two browser clients | Desktop and touch clients shared one real server battle; defender selection used the correct side; enemy troop orders were rejected; closing and reopening a client restored its seat; both profiles received one committed result; HTTP fallback, origin restrictions, and an existing game's health endpoint passed |
| Responsive browser flow | 1440 × 900 desktop, 390 × 844 touch portrait, and 844 × 390 touch landscape passed with zero JavaScript errors, no failed local assets, and no document overflow |
| Shared host | All three existing Faction Forge server-mount regression tests passed |
| Shared Firebase rules | Existing Faction Forge security and TurnCraft personal-save emulator suites passed with the updated rules |

The browser flow covers the loading screen, field guide, local campaign, settlement recruitment, promotions, reserves, map travel, halt, saving/reloading, all 14 troop cards, quality controls, single selection, additive selection, desktop drag boxes and control groups, touch long-press selection and pinch zoom, group orders, formations, rally, completed field battles, breached siege gates, and result screens. Screenshots of each viewport's menu/world/battle were inspected for layout and sprite visibility. The CI workflow uploads these screenshots as artifacts.

Engine tests additionally cover physical tree/cliff obstacles, bridge-only river crossings, unreachable paths, diagonal corner cuts, all nine order types, 600 distinct formation slots, self-follow handling, repeated hold stability, exhausted paths, projectile damage, deterministic battle outcomes, healer withdrawal, retreat survivors, cooldowns, contracts, invalid purchases, level-cap saves, unfinished-battle import, settlement income, assigned garrison troop classes, owner-controlled garrisons without field-army duplication, patrol attrition, caravan trade, and peace stopping hostile pressure.

The emulator uses only `demo-mountonline`. Browser authentication in the two-player fixture substitutes a token verifier/Auth SDK while exercising real HTTP, Socket.IO, the shared engine, Firestore reads/writes and transactions. The production server always calls Firebase Admin's token verifier. No production accounts, saves, Firebase rules, hosting, or account bindings were changed during validation.

## Performance samples

Seeded full-combat benchmark, including movement, targeting, projectiles, healing, collisions, and casualties; fixed simulation rate 20 Hz:

| Total troops | Steps sampled | Mean step | P95 step | Maximum step | Result snapshot |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 310 | 0.33 ms | 1.13 ms | 4.44 ms | 5.5 KB |
| 400 | 372 | 1.21 ms | 3.78 ms | 18.92 ms | 20.9 KB |
| 1,200 | 487 | 4.28 ms | 15.76 ms | 34.04 ms | 54.2 KB |

The 1,200-troop process reported approximately 19.8 MiB of used JS heap. This measures the simulation process, not total browser/GPU memory or end-to-end rendering latency. The server admission budget prevents unbounded simultaneous battles.

75 animation-frame samples during the 135-troop practice battle, after loading:

| Emulated viewport | Median frame interval | P95 interval | Mean sampled game work |
| --- | ---: | ---: | ---: |
| Desktop 1440 × 900 | 16.6 ms | 18.5 ms | 0.64 ms |
| Touch portrait 390 × 844 | 16.6 ms | 19.0 ms | 0.65 ms |
| Touch landscape 844 × 390 | 16.7 ms | 17.8 ms | 0.70 ms |

The sampled median is approximately 60 FPS on this host. Hardware, thermal throttling, browser scheduling, draw resolution, network conditions, and larger visible armies can change that result. There was no controlled old-versus-new hardware benchmark; these are direct measurements of the rebuilt game.

Essential bitmap payload is 1,697,856 bytes: two troop atlases, one terrain/prop atlas, and the existing menu backdrop. Native SVG icons and the manifest add only a few kilobytes. The client preloads/decodes the bitmaps before enabling game entry and reuses a versioned cache.

## Release validation still required

The implementation is ready for review. Before making the online release live, deploy the authoritative Node service and matching Firestore rules, configure its HTTPS API origin for the actual frontend, and verify trusted bindings for legacy accounts. Then smoke-test real Firebase sign-in, two production accounts, a player-owned castle defense, reconnecting over real mobile networks, and physical-device frame pacing. These release checks require the deployed service and real account identities; they are not represented by the local/emulator results above.
