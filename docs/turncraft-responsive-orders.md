# TurnCraft responsive movement and ordered multiplayer commands

This change replaces long committed movement segments with legal adjacent grid
steps, adds immediate order feedback, and makes multiplayer command processing
durable and ordered. The required real Firebase lobby playtest remains
**unverified**; the work must not be treated as satisfying every acceptance
criterion until that playtest succeeds.

## Baseline and reproduced delay

The reference is commit `787753f316cacbeb7b5a8225c4da5fe26fd189b6`.
The original simulation moved a troop several tiles at once and set its full
movement cooldown. A replacement move changed the destination immediately but
could not execute until that cooldown ended. Reversing immediately after a move
reproduced delays of 1.44–2.53 seconds for the seven sampled units.

The following measurements use the actual inline simulation on unobstructed
ground. Arrival is a 100-tile straight trip sampled every 10 ms. Reversal is a
180-degree replacement issued 1 ms after the initial movement, sampled every
1 ms. Times exclude network delay and cosmetic interpolation.

| Unit | Original arrival (s) | Updated arrival (s) | Original reversal (ms) | Updated reversal (ms) |
| --- | ---: | ---: | ---: | ---: |
| Rifleman | 60.720 | 60.730 | 1,839 | 613 |
| Medic | 64.680 | 64.520 | 1,954 | 651 |
| Hellion | 41.520 | 42.700 | 1,724 | 431 |
| Dragoon | 75.900 | 75.900 | 2,299 | 766 |
| Phoenix | 32.870 | 34.160 | 1,724 | 344 |
| Zergling | 34.560 | 35.580 | 1,437 | 359 |
| Ultralisk | 83.490 | 83.500 | 2,529 | 843 |

A separate comparison of all 40 movable unit definitions, advancing at exact
movement deadlines, kept 100-tile arrival times within the documented **5%**
tolerance. The largest increase was Wraith at **4.21%**. The steady distance per
cooldown is unchanged. The original simulation granted its entire first
multi-tile burst instantaneously; the updated simulation grants one tile at
startup. That accounts for the arrival difference and can produce a larger
percentage difference on very short trips. Whole-segment rounding and polling
also affect the sampled arrival table.

## Movement and feedback behavior

Ground troops execute one orthogonal tile per step. Aircraft retain their legal
diagonal movement with a one-tile step. Step cooldown equals the effective
original movement cooldown divided by the original travel range. Ordinary frame
overrun carries into the next deadline, avoiding cumulative slowdown. Idle or
blocked troops do not accumulate a catch-up burst.

The shared navigation fields and bounded local traffic search remain in use.
An isolated troop can use a four-neighbor field lookup; nearby traffic retains
the local search that handles crowds and narrow passages. Terrain, occupancy,
siege/deployment locks, weapon range, and transport eligibility remain
authoritative. Charge retains one original boosted-distance budget; fungal
movement retains its one-tile cadence. Emergency Stabilization applies its
existing proportional mobility support to the shorter step.

Removing the free initial burst also shifts early mining income and seeded AI
purchase timing. Two small policy corrections preserve the existing economy
acceptance behavior without changing unit or difficulty values: Easy postpones
optional technology and defense purchases until its first attack wave, while
retaining workers, gas, supply and basic production; an AI with fewer than three
base purchases' worth of connected minerals reserves a paid replacement before
the deposits empty, even when its current live-base target is met. An already
constructing base suppresses another depletion-driven replacement request.
All existing Easy opening assertions pass after this correction, including the
first-wave deadline of 240 seconds and the smaller-than-Medium army checks. The
two previously failing 30-minute Brood scenarios also pass: Medium/balanced
completes two paid expansions and retains two active bases with 880 connected
minerals; Hard/cautious completes three paid expansions and retains three active
bases with 4,988 connected minerals. Their original assertions were preserved.

Move, attack, stop, and hold acknowledge immediately with a local marker/status.
Newer orders replace the affected troops' old feedback. Group feedback includes
compact destination stands and accepted/partial/rejected counts where available.
Pending feedback stays pending until a saved authoritative snapshot acknowledges
the command. Connection failure is shown as waiting with retry. Existing mouse,
touch, keyboard, selection, construction, mining, rally, ability and passenger
targeting paths remain in use; panning and pinching do not issue orders.

Sprites use the short movement timer and face their actual segment while moving,
including during an older attack animation. Stationary attacks face their
targets. Followers interpolate over the observed snapshot cadence, normally
750 ms, and reconstruct bounded legal cosmetic routes for skipped ground hops.
This adds up to one snapshot of presentation lag. Duplicate snapshots do not
restart movement or change the measured cadence. Interpolation changes neither
logical positions nor cooldowns and sends no animation frames to Firestore.

After a gap longer than 1.5 seconds, catch-up is bounded; a pause or a single
correction can remain visible. Large corrections retain the existing distance
limit and may snap. Cosmetic routes use current static geometry and can differ
from an exact crowd route that occurred between snapshots. These limits need
assessment in the real lobby playtest.

## Multiplayer ordering, recovery and compatibility

New command documents carry `clientStream`, `clientSeq`, `clientCommandId` and
`controllerEpochKnown`. A client stream belongs to a browser tab, commander and
seat. The controller buffers gaps and executes consecutive sequence numbers in
order. An older move therefore cannot run after a newer stop from that stream;
distinct attacks and abilities retain their position in the sequence. Duplicate
documents do not replay acknowledged effects. A missing command holds later
commands rather than silently skipping a distinct action.

Snapshots add `commandProtocol: 1`, `commandStreams` and a bounded
`commandResults` history alongside the existing acknowledgements. The cursor
and simulated result become durable in the same saved state. Command documents
are deleted only after that snapshot write succeeds, allowing takeover to
replay work that was applied but never saved. Controller epoch handling permits
a takeover with a lower tick and rejects snapshots from an older controller.
Older snapshots do not erase already durable cursors.

The per-tab outbox and sequence survive browser reload through `sessionStorage`;
retries reuse the command document identity. A duplicated tab creates a distinct
stream. Closing or discarding a tab can lose commands that never reached
Firestore; recovery of that unsynced outbox is not guaranteed. Simultaneous
devices or tabs controlling the same seat have independent streams, so there is
no cross-device total order for those competing inputs.

Legacy unsequenced commands retain their prior best-effort compatibility path
and expiry behavior. They do **not** receive the new ordering guarantee. Reload
every participant, including the controller, after deploying the complete
updated client. Do not rely on a mixed-version lobby for these guarantees.

The controller continues to validate ownership, visibility and action
eligibility. No rules, indexes, Cloud Functions or server migration are required
by these payloads under the repository's current local Firestore rules. This is
a compatibility assessment of those rules, not confirmation that production has
the same deployed rules. Existing deployment guidance remains in
[`turncraft-multiplayer-deployment.md`](turncraft-multiplayer-deployment.md).

## Payload and traffic accounting

These are controlled UTF-8 JSON measurements, excluding SDK framing, transport
overhead, billing and production listener traffic.

| Fixture | Original | Updated | Difference |
| --- | ---: | ---: | ---: |
| Representative move command | 201 B | 237 B | +36 B |
| 1,000-troop snapshot after 64 commands | 496,832 B | 501,092 B | +4,260 B (+0.86%) |

The existing 750 ms state-write throttle remains. Shorter movement does not
write per-frame positions. Commands issued by the controller now also use the
durable command-document path: each host order adds one command creation and a
later deletion, plus its normal inclusion in the next state snapshot. Retries
may add document reads. Remote orders already used command documents. Large-army
frame-rate and measured browser results should be read with the final validation
results supplied with this change.

## Large-army browser measurements

The reproducible fixture uses verified actual artwork, a 1360×900 headless
Chromium viewport, medium graphics, a 128×128 open battlefield and the existing
formation group command. Each army size resets its presentation state; 240 timed
frames include 30 warm-up frames, followed by 180 requestAnimationFrame samples
with 20 warm-up samples omitted. No other heavy simulation ran during these
measurements.

| Formation size | Original observed FPS (two runs) | Final optimized FPS | Original frame-work p95 | Final frame-work p95 |
| --- | ---: | ---: | ---: | ---: |
| 128 troops | 60.0 | 60.0 | 8.2–9.3 ms | 9.1 ms |
| 256 troops | 48.7–54.9 | 59.3 | 12.2–12.8 ms | 13.5 ms |

Headless FPS varied between runs; frame-work timings do not include all browser
composition/GPU work. The first updated formation runs were slower (45.7–47.1
FPS at 256 troops). Reviewing the draw path identified repeated curved shadow
and ownership-ring drawing as avoidable work. Those unchanged underlays now use
a bounded 4 MB raster cache; ordinary adjacent cosmetic hops also avoid route
searches, and unchanged feedback DOM attributes are no longer rewritten each
frame. The final observed frame rate improves while CPU timings remain similar;
this is laboratory evidence, not a physical-device or production guarantee.

The cache retains original geometry/colors and releases entries on zoom/DPR
changes. Selection rings, bodies and air offsets retain their existing paths.
Twenty-four ground/air/shadow/tint/size comparisons against original vector
rendering pass with at most 2/255 compositing quantization; reuse, eviction and
memory-cap checks pass, together with 11 desktop/touch selection cases and five
movement/remote/fallback animation cases. Unusual alpha/compositing states use
the original vector path.

Set `TURNCRAFT_BENCH_REAL_ART=1`, `TURNCRAFT_BENCH_FORMATION=1` and
`TURNCRAFT_ASSET_CACHE` to the verified artwork cache, then run
`node game/turncraft/tests/responsive-performance.bench.cjs`.
`TURNCRAFT_BENCH_SOURCE` selects baseline HTML; `TURNCRAFT_BENCH_BASE_REF` selects
its animation-runtime revision (defaults to the baseline commit above).
`TURNCRAFT_BENCH_OUTPUT` saves the JSON measurements. Leave the formation flag
unset to exercise individual lane commands instead.

## Reproduce the focused checks

Run from the repository root with Node.js. Browser tests also require Playwright
and an installed Chromium; `CHROMIUM_EXECUTABLE_PATH` can select an available
browser executable.

```sh
node game/turncraft/tests/movement-benchmark.cjs
node game/turncraft/tests/command-network-benchmark.cjs
node --test game/turncraft/tests/interruptible-movement.test.cjs game/turncraft/tests/movement-reconciliation.test.cjs game/turncraft/tests/command-ordering.test.cjs game/turncraft/tests/order-feedback.test.cjs
node --test game/turncraft/tests/order-feedback-browser.test.cjs
node --test game/turncraft/tests/strategic-navigation.test.cjs game/turncraft/tests/compact-formations.test.cjs game/turncraft/tests/animation-runtime.test.cjs
node --test game/turncraft/tests/easy-ai.test.cjs game/turncraft/tests/economic-horizon.test.cjs
```

Both benchmark scripts accept a path to the original `game/turncraft.html` as
their optional first argument. Use the baseline commit above and the current
test harness to reproduce the before/after comparison. The focused tests cover
short deadlines, reversal, exact travel ratios, slows/charge, constrained groups,
750 ms follower updates, duplicate/reordered commands, sequence gaps, failed
writes, reconnect/reload, takeover and immediate desktop/touch feedback.

`movement-benchmark.cjs --all [baseline-file]` measures all 40 movable units.
The final 10 ms sampling run's largest 100-tile arrival difference was 4.24%
(rounded up), within the 5% tolerance above.

## Final automated and local Firebase verification

The complete regression run passed **675 checks**. One unrelated artwork test
was explicitly excluded: the mirror-enemy tint pixel-edge assertion fails with
the same verified images and Chromium on the untouched baseline HTML and
animation runtime. The initial unfiltered run exposed it; it was reproduced
separately against the original commit. No tint code or pixel tolerance was
changed to conceal that failure.

The broad run used:

```sh
node --test --test-concurrency=2 --test-skip-pattern='^real Cloudflare troop artwork has a light mirror-enemy tint' game/turncraft/tests/*.test.cjs game/turncraft/shattered-front/tests/*.mjs game/turncraft/shattered-front/tests/*.cjs
```

After the final presentation optimization, 61 focused movement, navigation and
animation checks and four browser movement, snapshot and touch checks passed.
Ordinary adjacent cosmetic hops now avoid a route search; a failed reconstructed
remote route performs one presentation correction rather than crossing a wall.
The feedback panel also avoids setting unchanged DOM attributes on every frame.

The shared Firestore rules emulator passes with actual updated command envelopes
and serialized snapshots, including create-only command retry restrictions.
The separate two-browser integration uses the real Firebase SDK with local
Authentication and Firestore emulators. It passed actual desktop mouse and touch
orders, immediate pending feedback, rapid reversals and stop, offline move/hold
reconnect, same-context reload, and controller takeover to epoch 2. The final
troop was stopped with zero pending commands, and the private test room finished
successfully. No uncaught page exceptions occurred. The final offline/reconnect
run recorded one Firestore Listen-channel HTTP 400 diagnostic; the SDK recovered
and all state/acknowledgement assertions passed. Measured immediate feedback was
0.1–14.9 ms; the first touch order's authoritative acknowledgement took 693 ms.
These are local test measurements, not production latency guarantees.

Run that integration with:

```sh
npm --prefix game/turncraft run test:responsive:firebase
```

It requires the repository's Firebase/Playwright test dependencies and Java.
The emulator config now includes Auth and explicitly binds both emulators to
loopback. The test grants Chromium local-network access only to its local test
origin. A live project run requires `TURNCRAFT_LIVE_FIREBASE=1`; it creates a
private unranked QA room and suppresses player profile/rating writes.

## Real Firebase acceptance status

On **November 4, 2026**, two separate browser contexts successfully established
Firebase Authentication sessions against the live project. Creation of the
private test room was rejected with a Firestore permission error, so the clients
could not enter a real shared match. This is not a successful two-client lobby
playtest. Production room permissions/deployment must be resolved through the
existing authorized lobby flow before continuing.

The real Firebase desktop/touch lobby checks, delayed/reordered/duplicate
delivery during a live match, reconnect, controller takeover, visual correction
quality, and production traffic measurements remain **unverified**. Automated
simulation, injected-network tests and browser fixtures provide separate
evidence; they do not substitute for this acceptance requirement. The PR remains
a draft until the live lobby playtest and production performance/traffic checks
can be completed.
