# Stoney’s Relic: shared simulation and party tracking

## Scope and compatibility

The existing `game/stoney.html` and `game/dungeonmaster.html` URLs and their
`gameId` / `username` parameters are retained. Both now load the same rules and
Firebase transport. The seeded maze algorithm, first-person controls, DM energy
model, five-minute escape objective, and 15-second respawn remain. Rendering is
still Three.js, with a textured 3D maze, crown, player bodies and hazard models.

Start from an existing site lobby. A mistyped/deleted room is reported instead of
silently creating an empty game. Existing seeded rounds are read with a stable
`seed:<seed>` compatibility key. New rounds have `runId` and a Firebase-authenticated
`dmUid`; a different DM-page visitor is a read-only viewer. Refresh restores a
player’s position and death timer rather than resetting their life state.

Load both updated routes for a match: old cached clients do not run the new shared
enemy protocol. No other game is modified and Firestore security rules are not
weakened. Deploy the entire `game/stoney-relic/` directory with the two HTML files;
HTML now reports module/CDN loading failures with a reconnect button.

## Synchronization design

The low-frequency objective remains at `lobbies/{gameId}.stoney`. Individual
players remain in `lobbies/{gameId}/players/{uid}` and traps in `traps/{id}`.
One new document, `lobbies/{gameId}/stoneyRuntime/state`, contains the enemy
snapshot and simulation-host lease. The repository’s existing authenticated
recursive lobby rule covers that path. A more restrictive deployed project must
be reviewed before release; permission failures are shown, not bypassed.

An active, visible client owns a 6.5-second transactional lease. The DM is preferred;
otherwise a deterministic eligible-player ordering supplies a replacement. The
lease carries a browser session and incrementing epoch. A superseded host cannot
publish over a new host. Handoffs resume the last shared enemy snapshot. There is
no catch-up simulation spike or local enemy simulation on every guest.

This is **cooperative client authority**, not a trusted anti-cheat server. The
existing backend rules still allow broad authenticated lobby access. UID/session
checks and transactions prevent normal-client races; they do not make malicious
clients trustworthy. Competitive anti-cheat would require a server and stricter
rules as a separate, carefully tested project.

Position writes are latest-only, at most one outstanding request per client.
Normal movement is limited to one attempt per 500 ms and idle presence to one per
2.5 seconds; important actions force a fresh position. Acknowledged writes alone
advance the baseline. Retries back off and use the newest pose rather than
replaying accumulated frames. Movement transactions preserve life state and
reject superseded browser sessions and pre-respawn positions. Listeners consume
changed documents, not repeated rebuilds of the whole scene. Server timestamps
calibrate the clock and age presence; dead, stale, hidden and wrong-round players
are distinguished.

The host publishes one shared enemy snapshot approximately every 350 ms while
enemies are active, or every two seconds otherwise. Round maintenance is bounded
to 1.5-second checks. Rendering never writes every frame. These are scheduling
limits, **not measured Firebase cost reductions or latency guarantees**: meaningful
moving enemies and faster position updates add work compared with the old
stationary-enemy implementation. Test read/write usage with the target party size.

Crown pickup, escape, death/drop and timeout revalidate current state in
transactions. Death and crown drop are one atomic commit. Spawn creation and
energy spending are also one atomic commit, with a captured trap type and a
single ID across transaction retries. Repeated input has a single-flight guard.
The round timer is checked independently of lobby snapshots. A disconnected crown
carrier drops the objective at their last safe position after the presence grace.

## Gameplay, visibility and rendering

Demons pursue corridor routes. Ghosts can intercept a moving player or get ahead
of the crown carrier. Target locks, route-distance scoring, last-known-position
searches and patrols avoid constant target switching and wall-crossing. Dead,
stale and respawn-protected players are not targeted. Active enemies are capped
by party size; total active traps are limited to 24.

Manual and smart placement use the same safety checks: clear/reachable cells,
protected entrance route, two-cell distance from living players and the crown,
spawn spacing, energy and active limits. New live-play traps warn for 1.2 seconds.
Setup traps arm at the actual start time, including an early Ready click. Respawns
have three seconds of protection. The crown cannot be relocated after play starts.

A persistent gold banner names the crown carrier for players and the DM. The
party list, color-matched radar, crown symbol and through-wall/directional player
labels show where teammates are, their distance and their life/connection status.
The carrier gets an exit marker. Notifications are no longer suppressed during
play. User-provided names are rendered as text, not HTML.

The first-person renderer instances maze walls and lamp bulbs and caps real point
lights at three on touch devices / five on desktop. Collision uses spatial wall
buckets and swept movement; interpolation is bounded and collision-aware. The
old solid decorative entrance block is now an open arch. GPU resources and
listeners are disposed on exit. Mobile move/look pointers remain independent,
are released on interruption, and do not overlap the essential HUD. The menu
pauses input, **not the multiplayer world**: it grants no damage immunity.

## Automated checks

Run without npm dependencies, using Node 22 or newer:

```sh
node --test tests/stoney-relic.test.mjs tests/stoney-network.test.mjs
for f in game/stoney-relic/*.mjs; do node --check "$f"; done
```

The 28 automated checks cover deterministic/connective maze generation, collision,
pathfinding, spawn fairness and timing, enemy pursuit, presence, interpolation,
write coalescing/retry, concurrent crown pickup, atomic death/drop, energy rollback,
respawn timing, deadline validation, host lease handoff and stale-host fencing.
Network tests use an atomic transaction double with read-before-write assertions;
they are not Firebase emulator or deployed-project tests.

Desktop and mobile HUD/menu smoke checks were also run locally in Chromium with
mocked networking and a renderer stub. They covered crown/party UI, inert name
markup, layout width, smart placement, menu closure/reopening and separate mobile
sticks. They do **not** validate real WebGL output, physical phones or live Firebase.

## Required release checks

Use a test lobby and distinct browser profiles/devices, not two tabs sharing the
same anonymous UID. Have the DM and two players join, and verify Ready and automatic
start, trap arming, enemy movement and crown ownership on all three screens.
Try simultaneous crown pickup, crown-carrier death, a refresh during respawn,
simultaneous spawn clicks and crossing the exit near the deadline. Only one crown
owner/result and one energy charge per successful placement should persist.

Disconnect or background the current host, then the crown carrier. Verify host
handoff after lease expiry, resumption from the shared snapshot, dropped-crown
recovery after the grace period, and no old-host overwrite on return. Interrupt
Wi-Fi while moving; verify visible connection feedback and latest-state recovery,
not playback of queued positions. Try an unauthorized DM view and blocked Firebase
permissions; neither should silently modify the game.

Finally play on a real desktop and phone using the production CDN imports. Check
pointer lock, fullscreen, held multi-touch controls across interruptions, portrait
and landscape, actual 3D visibility, frame time and Firestore usage with eight
players and the maximum enemy count. Live-project / physical-device checks were
not performed as part of the local automated validation.
