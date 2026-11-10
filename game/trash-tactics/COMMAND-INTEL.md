# Command & Intel · 2.4

This update adds worker construction plans, minimap attack alerts, last-seen observations and post-match analysis. All seven maps and four difficulties remain available.

## Queue an entire base

1. Select one or more Twigs and choose a building card.
2. Place it on clear, currently visible ground. A Twig already building finishes its current paid job before the new one.
3. Hold **Shift** while placing on desktop, or enable **Queue builds** on desktop or touch, to keep placement active. Select other cards to mix cabinets, printers, desks, CD readers, defenses, walls and specialist buildings in the plan.
4. Review the **Build plan** for active and queued jobs. **Done placing** exits placement while workers continue working.

Payment happens once, when a blueprint is accepted. Starting a queued job costs nothing further. Prerequisites must already be complete when placing a blueprint: a queued Printer does not yet unlock a Workshop. Invalid, unaffordable or unreachable placement does not charge trash or alter the current plan. Proposed construction must leave the selected worker able to reach its paid jobs.

With multiple workers selected, assignment uses estimated remaining construction and travel, with the worker ID breaking ties. Each worker follows its own ordered plan. Walls retain automatic repeated placement and queue behavior.

**Stop / S**, a replacement movement or gathering command, or worker death releases active and queued assignments. It does not refund payment or erase blueprints. Unfinished buildings retain their progress and physical footprint. Select a Twig and right-click or tap an unfinished friendly blueprint to resume it; only a worker that can reach the site may take over. If none can reach, the existing builder and plan remain unchanged. Destroyed queued jobs are skipped. Dismantling a wall also provides no refund.

The simulation API is `game.build(workerIds,type,x,y,{queue:true})`. `game.getBuildQueue(workerId)` returns active-first snapshots with `id`, `type`, `x`, `y`, `remaining`, `buildTotal`, `status` and `workerId`. Player placement always preserves construction work; the UI toggle controls whether placement remains armed.

## Read the red minimap pulses

Actual damage to your own units or buildings creates an alert at the victim's position. Pointer damage over time also refreshes alerts. Damage elsewhere does not create a human-team alert.

- Hits within **260 world pixels** share a cluster.
- The minimap retains at most **five** clusters.
- A cluster expires **six simulation seconds after its latest hit**. Pausing freezes this clock.
- Repeated damage refreshes the cluster without restarting its pulse more often than every **0.8 seconds**.

Alerts do not expose a hidden attacker's position, move the camera, change selection or issue an order. Existing minimap click and drag navigation remains available.

## Scout, then remember

When an observed enemy or neutral object leaves sight, its battlefield image becomes a desaturated, motionless **LAST SEEN** observation. The minimap uses subdued dashed markers. Units, buildings, walls, CD transports and resource cans can be remembered; unexplored objects never generate an observation.

The observation preserves only what was visible: position, orientation, health, construction progress, current front production progress, resource amount and occupied CD seat count. It does not disclose future production orders or passenger identities. Animation, health, construction, resources, cargo and wall connections remain frozen while hidden, even if the live object moves or is destroyed.

Returning sight updates the observation. If an object is absent, its old image clears only after all fog cells covering its remembered footprint are currently visible. A partial glimpse of the old area cannot erase it. Observations are not selectable, targetable, physical obstacles or live enemy markers. Attack-move and scout to establish current information.

## Review the completed match

Victory or elimination opens a read-only report. **Overview** summarizes the battle; **Economy** compares gathering, spending, bank and difficulty grants; **Army** shows army value, size, workers, survivors, completed production and losses; **Timeline** shows retained key events. Focus on one team or compare all teams. Charts include exact-value tables and support keyboard and touch controls.

Worker gathering counts trash physically delivered. **Impossible** bonuses are separate: **+300 starting trash**, **double harvesting credit**, and **+2 trash per second**, with the passive stipend capped at **1,200 per bot**. The player's ordinary 300-trash starting bank is not gathering or a bonus. Spending counts successful unit, research and blueprint payments, including queued purchases. Trained counts completed production, excluding starting troops. Army and loss value use nominal production cost; health does not change that value. Army size includes embarked combat troops and CD transports, while workers and buildings are separate.

The engine records actual observations every **five simulation seconds**, plus the initial and final states. Brief changes between observations may not appear in curves. Each team retains at most **721 observations**; progressive thinning preserves whole-match coverage, its beginning and endpoint. Exact cumulative totals, first expansion, expansion count and separate army-count/value peaks survive thinning. Recent event details are bounded to **256 per team / 512 overall**; full-match totals are not truncated.

`game.getMatchReport()` returns `null` during active play. Once the human wins or is eliminated, it returns a deeply frozen plain snapshot. A free-for-all report can end at human elimination while surviving bots have no final winner yet. The lobby's **View last match report** reopens that snapshot until a new match starts. New matches reset construction, intelligence, alerts and accounting.

## Validation

Run `npm test` for the full suite. Focused coverage added for this update:

- `tests/building-queues.test.mjs`: mixed plans, payment, progress, interruption, death, takeover and reachability.
- `tests/match-telemetry.test.mjs`: real accounting, final snapshots, short matches, Impossible separation, peaks and bounded long histories.
- `tests/scout-memory.test.mjs`: observation-only snapshots, frozen hidden state and full-footprint re-scouting.
- `tests/intel-render.test.mjs`: remembered visuals and minimap markers.
- `tests/minimap-alerts.test.mjs`: damage clusters, lifetime, clipping and player-only alerts.
- `tests/match-report.test.mjs`: chart data, team comparison, exact summaries and report controls.

Existing wall queues, construction navigation and desktop/touch controls remain regression-tested.
