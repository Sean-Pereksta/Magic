# Stoney's Relic — synchronized dungeon (protocol 2)

The existing player and Dungeon Master URLs, `gameId` / `username` query parameters, Firebase project, anonymous authentication, 21×21 seeded maze, five-minute cooperative escape, 15-second respawn, DM energy/costs, and four trap types are retained. The large inline scripts are replaced with modules for rules, transport, rendering, and input/HUD.

## Changes

- Firebase Firestore remains the transport; no Realtime Database provisioning, credentials, or rule relaxation is introduced.
- Position-only packets live in `lobbies/{gameId}/stoneyPoses/{uid}-{sessionId}`. Session, round, and life identifiers prevent old tabs or pre-respawn position writes from restoring obsolete gameplay state. Authoritative status remains in `players/{uid}`.
- Moving positions are coalesced at a 350 ms target cadence, stationary heartbeats at five seconds, with one write in flight, acknowledgment-based dirty checks, and capped retry backoff. This deliberately improves movement freshness versus the former 950 ms cadence; it does not promise lower total write volume during constant movement.
- A leased host publishes shared enemies to `stoneySync/state` (400 ms target while enemies move, two seconds idle). Transactions fence updates by owner, session, round, and epoch. Visible peers take over an expired 6.5-second lease; hidden hosts stop simulation. This is client-hosted authority, not a trusted anti-cheat server.
- Crown pickup, escape, timeout, death/crown-drop, and energy/spawn mutations are transaction guarded. Contact is revalidated against current server player state before death. Disconnected carriers release their crown after a 20-second grace period.
- Setup starts automatically after its deadline even if the DM leaves, provided an eligible player remains connected. Timeout is checked by the simulation host, not dependent on a new lobby snapshot.
- Ghosts track players through legal maze corridors. Demons anticipate a legal next corridor and warn before lunging. Target memory, crown priority, target-pressure scoring, deterministic patrols, shared positions, and bounded BFS caches avoid competing client simulations.
- Placement protects the entrance, living players, and crown, rejects overlapping spawns, caps active enemies at 12, and gives spawns a 1.3-second warning. Smart placement suggests a legal, useful maze cell. Automatic crown placement budgets its shortest-path round trip to at most 45% of the match at walking speed, leaving time for searching and encounters. Energy and the spawn commit together. Static player status and trap reads run in parallel; streamed movement is not added to the transaction's conflict set. These are cooperative-client fairness checks, not security-rule enforcement against malicious clients.
- Players see teammate names, distance, through-wall and screen-edge markers, and living/respawning/offline status. The crown carrier gets a persistent gold announcement, gold model/marker, roster crown, and DM map marker. Dropped crowns remain marked after discovery. User names use textContent, not injected HTML.
- The procedural dungeon is rebuilt using instanced walls, fixtures, and crates; a small fixed light budget; collision buckets; and adaptive pixel ratio. The entrance is framed with side posts and a lintel rather than an opaque slab. Dynamic resources, subscriptions, timers, and controls are cleaned up on exit.

## Deployment and compatibility

Deploy both HTML entrypoints and every file in this directory together. Reopen all Stoney tabs or start a fresh lobby after deploying: active legacy clients are explicitly detected and block a mixed-protocol match rather than silently diverging. Existing stale player documents stop blocking after presence expires.

The repository's existing authenticated lobby-subcollection rules cover the new documents. This change does not deploy Firebase rules or enable Anonymous Auth; those services must already be configured. Existing permissive lobby rules are unchanged and are not an anti-cheat boundary. First-time DM UID binding respects the lobby's assigned DM name; subsequent control requires the bound UID.

Movement remains local and responsive; remote actors interpolate committed snapshots. Firestore latency, browser suspension, and packet loss still affect remote freshness. No zero-lag or hard failover-time guarantee is made.

## Validation

Run from repository root with Node 22 or newer:

```sh
node --test tests/stoney.test.mjs
```

The tests cover deterministic maze connectivity and collision, safe placement, trap timing, enemy chase/memory/windup, write coalescing/backoff, concurrent crown acquisition, stale-session/life protection, atomic spawn/death mutations, host fencing, disconnect recovery, clock calibration, and listener cleanup. The network suite uses an in-memory transactional driver with reads-before-writes checks, **not** the Firebase Emulator Suite or a live Firebase project.

Local Chromium layout checks exercised both entrypoints at 1440×900, 390×844, and 844×390 with mocked match data. They cover HUD/map layout and JavaScript imports, not actual WebGL rendering or live network traffic.

Before merging/deployment, smoke-test two separate authenticated browsers/devices (then 8 players): start from the lobby, move/jump on desktop and dual-stick touch, pick up/drop/escape with the crown, spawn all powers and verify charge deductions, observe identical enemy targets/positions and deaths, background/close the host, disconnect/reconnect a carrier, reload while dead, and let the timer expire. Verify CDN/WebGL loading and physical mobile frame rate in that live test. Those live Firebase/WebGL/device checks were not run in the authoring environment.
