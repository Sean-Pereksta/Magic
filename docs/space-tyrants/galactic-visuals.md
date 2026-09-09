# Galactic visuals and transmission reliability

The shipping loader ends with two gameplay fixes followed by three presentation
modules. No assets, build step, preview page, or new production dependencies are
required.

## Presentation

- `galaxy-visual-model.js` reads existing planets, infrastructure, fleets, cargo,
  wars, battles, bases and operations. Deterministic planet geology is independent
  of ownership. Eleven families retain their surface palette while ownership uses
  rims, infrastructure lighting and fleet engines.
- `galaxy-visual-art.js` provides cached 96 × 96 procedural planet textures,
  distinct ship silhouettes, station geometry and one canvas icon vocabulary.
- `galaxy-visual-renderer.js` replaces accumulated legacy drawing wrappers so
  each fleet is drawn once. It adds development tiers, capital halos, defense
  platforms, formations, battles, invasions, landing craft, cargo traffic,
  contextual routes, disruption markers, event pulses and map inspection.
- Fleet formations represent power rather than new simulation ship classes.
  Existing fleet names, flagships, veterans and admirals remain authoritative.
  Contested blockades use the existing space-control engagement state; they do
  not create a planetary invasion.
- Trade is gold and dashed, military movement uses faction-colored directional
  lines, logistics is thin cyan, and contextual agreements use violet dots.
  Trade corridors come from actual cargo movements. Briefly retained observed
  corridors can show an interruption after their traffic disappears.
- The Map Key uses the same canvas paths as the map. Hover/selection identifies
  the object, its role, force strength, destination and relevant activity.

| Fleet power | Tier | Maximum formation ships |
| --- | --- | ---: |
| 0–9 | Patrol | 2 |
| 10–34 | Small fleet | 4 |
| 35–99 | Task force | 9 |
| 100–249 | Battle fleet | 18 |
| 250–499 | Armada | 32 |
| 500+ | Doomstack | 48 |

Zoom and load reduce visible escorts while retaining the relative force scale.
Geology is cached; city illumination, construction, defenses and ownership react
to live simulation values. No procedural art consumes simulation randomness.
The original physical facility-position helper remains untouched because ship
launching and docking also use it.

## Rendering limits

| Item | Maximum |
| --- | ---: |
| Cached planet textures | 96 |
| New textures per frame | 1 |
| Detailed planet overlays per frame | 24 |
| Fleet silhouettes per frame | 360 |
| Civilian vessels, including orbital traffic | 110 |
| Orbital civilian traffic | 36 |
| Combat entities/effects across engagements | 240 |
| Station structures per frame | 100 |
| Route overlays per frame | 80 |
| Collision-managed labels per frame | 30 |
| Pooled event pulses | 48 |
| Retained commercial corridors | 320 |

Battle allocations are 18, 44, 78 or 100 entities according to current combined
strength, then reduced for distance/zoom and load. Selected engagements are
allocated first. Lower-priority engagements retain a strategic marker when the
global budget is exhausted. Cached textures pin the visible working set to
prevent overview cache churn. Offscreen work is culled, hidden documents stop
drawing, and adaptive quality reduces optional detail down to 40%. Pause and
reduced-motion settings stop decorative movement.

`SpaceTyrantsVisuals.diagnostics()` exposes quality, measured rendering cost and
budget counts for profiling. It does not expose hidden enemy information.

## Colony cancellation

`colony-mission-cancellation.js` cancels staged and in-flight colony missions
when their destination becomes claimed or unavailable. Refunds go to the original
empire's sponsor, capital or another controlled world. If that empire has lost
all worlds, its material/passenger refund is retained until a landing world is
available. Diplomatic withdrawal of a colonial claim uses the same refund path.

Only paid credits, committed materials, associated undelivered freight and
embarked passengers are refunded. Volunteer recruitment does not create a second
population refund. Refunded missions are removed exactly once. Intercepted or
destroyed cargo is excluded.

New launches preserve the exact paid credit/material ledger on the existing
ship save object. Older in-flight saves recover their known hull recipe, actual
passengers and recoverable launch fee. For old missions at the minimum settler
floor, the original variable credit fee was not saved and cannot be reconstructed
exactly: only the known fixed 8-credit payment is refunded. New missions do not
have that limitation. The save version remains unchanged.

## Transmission acceptance

`transmission-acceptance.js` centralizes explicit Accept/Decline action routing
for governor, trade, Admiralty, diplomatic and peace cards. Delegated handlers
survive card refreshes, and pointer interaction temporarily holds refreshes so
buttons cannot disappear between pressing and releasing them.

The acceptance layer reuses the shared action receipts, trade validation and
atomic rollback introduced by the action-responsiveness update on main.
Readiness covers the real commitment: current treasury, project authorization
fees, uncommitted stock, shipyard/project availability, receiving worlds, freight
capacity, war/embargo state and offer expiry. Projects are marked accepted only
after construction starts. Trade commits both physical cargo legs atomically;
a failed dispatch restores cargo, freight fees, fuel and random state. Existing
trade-request contact and fulfillment accounting is retained. Migration accepts
only after real passengers depart and conserves their population.

Queue acceptance stores request identifiers and retries after simulation ticks.
It reserves no resources, rechecks current terms, and accepts ready requests in
queue order without overspending. Original offer deadlines still apply. Players
can cancel automatic acceptance without declining the underlying request.
The queue (40 entries maximum) and recent response history (40 entries) persist
through the existing empire save object. The UI shows the blocking reason and
accepted, declined, queued, cancelled or expired history.

## Verification

Run `node --test game/space-tyrants/*.test.mjs`.

The new integration coverage checks simulation/RNG isolation, physical docking
isolation, planet families, scale at multiple zoom levels, selected-battle
priority, bounded effects and cache reuse, hidden fleets, event replay, routes,
colony race refunds, legacy/save-load compatibility, actual button routing,
atomic dispatch, queued acceptance and request fulfillment.

Native Canvas checks also exercised the shipping compositor with an intentionally
oversized scene (111 visible worlds, 800 civilian ships, 120 fleets and 10 major
battles), verifying bounded detail and adaptive quality. This is a stress check,
not a browser frame-rate guarantee. No HTML preview was generated.

After integration with the latest action-responsiveness changes on main, all
211 Space Tyrants tests pass. The existing inspector stability patch remains
after the last `renderPlanet` wrapper.
