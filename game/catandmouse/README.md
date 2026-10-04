# Cat & Mouse combat, motion and sync

The public entrypoint remains `game/catandmouse.html`. It fetches and patches
`catandmouse-core.html`. Deploy both HTML files and the `catandmouse/` runtime
modules and stylesheet together:

- `sync.mjs`: existing write queues and multiplayer reconciliation.
- `motion.mjs`: local visual tracks and smooth projectile sampling.
- `tactics.mjs`: host target commitment, cached routes and tile reservations.
- `combat-presentation.mjs` and `combat-presentation.css`: persistent units,
  bounded effects, procedural animation and reduced-motion feedback.

The loader's exact/regex replacements are exercised against the real core.
The optimized patch version is `2026-10-04-combat-polish-v1`; its validation
includes the new runtime imports and the existing caps, buffered BFS and
self-scheduling movement loops. Tests compile both optimized and fallback
module scripts. No HTML previews are generated.

## Movement and decisions

Grid coordinates remain authoritative. A separate track copies each unit's
previous grid position, rendered position, target, timing and facing. One
demand-driven animation loop interpolates transforms and projectile travel;
it never runs AI, scans the board or writes animation frames to Firebase.
Changing facing replaces only the sprite body, preserving the unit wrapper,
nameplate and movement timeline. The camera follows the interpolated focus.

Local mouse movement retargets immediately, before the coalesced board redraw.
Remote snapshots retarget from the currently rendered position. Corrections
over 1.8 tiles catch up in 110 ms (65 ms locally); explicit teleports, revives
and corrections over five tiles snap rather than gliding through obstacles.
Travel is mostly linear with slight easing. Oxen and rat kings accelerate
more heavily, vultures glide, termites crawl, and rabbits and fleas hop.

The host director holds reasonable targets, releases dead targets, reuses
valid paths, invalidates routes when the grid or destination changes, and
reconsiders blocked routes. Short-lived origin/destination reservations and
local alternate steps prevent allies from swapping places and reduce crowd
oscillation. Unreachable targets get a short cooldown. Ordinary enemy plans
advance at most one adjacent tile per simulation tick.

Rats divide pressure between players, allies and defenses; stink rats favor
useful towers that are still active; oxen favor breaches; rat kings favor
defenses/support; vultures favor exposed players and economy; termites favor
structures, including damaged defenses. Existing rat-king damage support is
preserved. Difficulty and progression shorten reaction windows; the loader's
existing speed escalation and enemy count caps remain in place. Ox/king
contact attacks carry a small authoritative windup, and the existing cat
pounce destination is visible before impact. Core damage and unlock values
are retained.

## Friendly troops

Rabbit Dens and Flea Nests select a reachable, unoccupied open tile within
three steps of the producer. The bounded search cannot cross structures;
fully blocked producers retry on their existing tick. New units count toward
caps and occupancy immediately, emerge from the producer-facing edge of the
open tile, and then use the same motion system as other units.

Friendly targets persist and favor threats near players, the producer and
other infrastructure. Rabbits turn, recoil and fire smooth projectiles, with
a small golden pulse when the existing Rally Drum buff activates. The drum's
two-step travel validates both intervening cells. Fleas retain their existing
friendly role, lifetime and caps; towers and rabbits no longer mistake them
for enemies. The host collects troop decisions before submitting compatible
document updates in one batch, rather than awaiting a write per rabbit.
Failed producer batches release new local spawns for retry.

## Effects and performance

Turret/rabbit/acorn/web shots and rockets share local smooth travel and
impacts. Tesla arcs, laser beams, gust lanes, slime feedback, trapped-unit
webs, shield hit/break/recharge pulses, claws, spawn/death bursts and structure
damage/destruction use the same bounded presentation layer. Damaged
structures show a small crack. Major impacts alone add a board shake capped
at 2.2 pixels; reduced motion disables shake and minimizes hops, sweeping
particles and large transforms while retaining hits, statuses and warnings.

The effect store caps active effects at 128, projectiles/beams at 64,
particles at 48, trails at 12 and large effects at four. It recycles up to 96
DOM nodes and retains at most 12 brief death sprites. Critical projectiles
can displace decoration; keyed attack markers are independent of particle
caps. Offscreen effects are skipped, expired effects clean up automatically,
and sustained slow frames reduce quality from high to medium to low. Recovery
is slower than degradation; all levels retain important combat cues.

The optimized host shares at most five target decisions and ten path searches
per 200 ms window, with a 16-target shortlist. AI caches and reservations stay
host-local. The existing buffered BFS, simulation task locks, enemy/friendly
caps, spatial structure cache and coalesced rendering remain intact. There
are no per-frame network writes or extra movement timers.

Remote damage snapshots show hits and can infer a projectile from the closest
eligible tower. This is presentation feedback, not an authoritative record
of the shooter: exact attack events are not added to the network protocol.

## Sync behavior

- State assignments coalesce at a 250 ms minimum interval; player movement at
  120 ms. The interval starts when a write starts, so network acknowledgement
  time does not add an extra interval. There is at most one in-flight SDK write
  and one merged pending patch per writer.
- Only changed fields are sent. Dedupe follows successful writes or confirmed
  server snapshots. Transient errors retain failed fields, with newer values
  taking precedence, and retry up to five times with bounded backoff.
- Player movement updates position, facing, sequence and presence only. It
  cannot overwrite host-owned death/revive state, cheese or identity.
- Cat fields use dotted updates. Each objective is replaced independently;
  updating one objective cannot erase the others.
- Healthy host heartbeats come from the existing collection listener. Only
  the elected replacement attempts a transaction once the host is stale.
  Routine heartbeats no longer rewrite the host identity.
- Cached snapshots pause host simulation. Host changes cancel unsent state
  patches. Reattaching the listener removes entities absent from the first
  server snapshot. The HUD reports connection state and allows retrying a
  terminated listener. Back/forward cache restoration reattaches listeners.

## Rendering

Unchanged structures, terrain styles, desktop/mobile build controls and player
status labels retain their DOM nodes. Structure damage, generator activation,
shields, removal and asset fallback invalidate the relevant cached structure.
The existing isometric depth values remain shared across layers. Rendering is
coalesced through animation frames and suspended while the document is hidden;
simulation is not intentionally paused merely because a connected host hides
its tab. Browser timer throttling still applies.

## Validation

Run `npm run test:catandmouse` (Node 20+; no dependencies). The 62 tests cover
sync queues/retries, death/revive safety, host election/listener recovery,
loader application, interpolation/corrections, target persistence, reachable
spawns, cap enforcement, failed producer retries, route reuse/reservations,
AI budgets, effect limits/expiry and reduced-motion telegraphs. Runtime tests
also execute the full optimized core against a deterministic DOM and clock
fixture with mixed enemies, defensive structures, a den and a nest. They
exercise the real spawners, movement and tower attacks together; they are not
browser rendering or live Firebase tests.

For the optional real-browser smoke test, make Playwright available locally
and install its Chromium browser (`npx playwright install chromium`), then
run `npm run test:catandmouse:browser`. `CHROMIUM_EXECUTABLE` may point to an
existing browser executable. The runner serves the actual loader/core,
injects the mixed battlefield in memory, disables Firebase writes and checks
DOM motion, caps, feedback and cleanup in normal/reduced-motion modes. It
does not save preview HTML or screenshots. Chromium was unavailable in the
implementation environment, so this browser test has not been run there.

`window.__catMouseSyncStats()` exposes write/skip/retry counters and connection
state. `window.__catMouseBattleStats()` exposes unit/effect/pool counts,
quality, reduced-motion state, cached brains, reservations and AI budgets.

Before release, play the mixed-wave acceptance scenario on desktop and
mobile, including reduced motion. Check den/nest emergence, swarm spacing,
heavy windups, airborne motion, all tower effects, destruction and busy-wave
responsiveness. In a live two-player Firebase game, check remote interpolation
and fast corrections, deaths/revives, disconnect/reconnect, host replacement,
background/foreground restoration and friendly-unit cap consistency. Compare
write counters and frame responsiveness during a busy fight.

The automated fixtures do not measure real network latency or hardware frame
rate. Live two-player and desktop/mobile visual checks remain unverified.
Existing SDK writes already in flight cannot be cancelled by this client-only
change, and existing multi-document combat/economy paths are not made globally
transactional. No Firebase rules or backend services are changed.
