# Circuitbound: engineering foundations

This implements the **first expansion (priorities 1–5)** from the construction-language request. Players combine primitives; there are no finished elevator, car, factory, or miner blocks.

## Run and validate

Serve the repository with the existing server and open `/game/circuitbound.html`. The lobby route is unchanged. The entrypoint now loads readable `styles.css`, `engineering.js`, and `app.js`; the ten encoded payload parts are retired.

```sh
npm run test:circuitbound
node --check game/circuitbound/engineering.js
node --check game/circuitbound/app.js
```

Tests use Node's built-in test runner, the actual in-game component catalog, and the actual application inside a small DOM/canvas test harness. No added dependencies. The harness executes startup, input actions, simulation, movement, save/load, and drawing calls; it is **not** a real browser rendering or touch-layout test.

## Included

| Foundation | Behavior |
| --- | --- |
| Directional ports | Rotatable power, signal, mechanical, item, and structural ports. Input arrows point in; output arrows point out; squares are bidirectional. Fluid type/color reserved only. |
| Signals | AND, OR, NOT, XOR, NAND, NOR; signal splitter; sustained-input timer; repeating pulse generator; toggle; reset-dominant latch; counter with separate reset and target 1–15. |
| Transmission | Rear-to-front shafts, signal clutch, belt drive and pulley, 90° bevel, reciprocal speed/torque ratios, reversed small/large gears, bearings, hinges. |
| Structures | Separate assembly IDs, add/remove/inspect modes, disconnected-section splitting, weight, strength, stress colors, load-dependent stalls, non-destructive connection detachment under repeated extreme stress, anchored joints, rider-safe translation. |
| Items | Persistent loose item stacks, gravity, pickup, animated conveyor cargo, hopper, 64-item storage, alternating splitter, configurable filter, manual insertion/extraction, backpressure and full-container mining recovery. |

The Fabricator exposes every added component and its cost. Crafting selects the component. Advanced components must be crafted; this expansion does not yet add technology gating. New saves retain the existing starter kit and foundry mission.

## Controls

- **A/D**, arrows: move; **Space**: jump. Left mouse mines; right mouse places/configures.
- **E**: Fabricator; **H**: Field Manual. Both open normally; neither opens automatically at startup.
- **R**: rotate held component; **Shift+R**: rotate the aimed component within reach.
- **C**: Lens; **V** or View button: ports, power, signals, mechanics, structures, items.
- **0**: Link Tool. **L** cycles New / Add / Remove / Inspect. Drag a reachable rectangle. Inspect selects the structure that Add extends.
- **F**: take contents from aimed item machinery. **Shift+F**: insert one selected part.
- **X**: drop a selected part at the cursor. Walk near loose items to collect them.
- Gearbox right-click cycles torque / balanced / speed. Timer/clock cycles 0.25, 0.5, 1, 2, 5, 10 seconds. Counter cycles target 1–15. Filter cycles materials; Shift-right-click selects the held component as its filter.

The hotbar scrolls horizontally and displays component names. Rotate, Lens, Link, take, and drop buttons are also available on screen.

## Port conventions

Sides below are relative to an unrotated, right-facing component. Rotating a component rotates every port.

| Component | Rear / left | Top | Front / right | Bottom |
| --- | --- | --- | --- | --- |
| Motor | Power input | Optional enable | Rotation output | — |
| Shaft / gearbox / gears / pulley | Rotation input | — | Rotation output | — |
| Clutch | Rotation input | Required enable | Rotation output | — |
| Bevel | Rotation input | — | — | Rotation output |
| Piston / winch / joint | Rotation input | Optional control | Structural action / attachment | — |
| Binary gate | A input | B input | Signal output | — |
| Latch | SET | RESET (wins) | Stored output | — |
| Counter | COUNT | RESET | Target reached | — |
| Signal splitter | Input | — | Output | Output |
| Conveyor | Rotation + item input | Falling-item input | Rotation + item output | Rotation input |
| Hopper | Item input | Falling-item input | Item output | — |
| Storage / filter | Item input | — | Item output | — |
| Item splitter | Item input | — | First output | Second output |

Unconnected enable defaults ON for motors, pistons, winches and pulse generators. Clutches require an actual ON signal. Hinges use their input to select closed/open while their mechanical supply remains active. An unpowered piston holds its position; power plus LOW enable retracts it.

## Simulation details and deliberate limits

- **Fixed 50ms steps**, paused with game dialogs. Dirty topology rebuilds use indexed active components, never a full world scan every frame. Initial load/new-world indexing scans the world once. A topology edit rebuilds the active-component graph; incremental per-network invalidation is future optimization.
- Only passive signal wires form buses. Gate ports remain separate. Combinational gates settle synchronously (up to 64 passes), stateful gates advance once per step, then outputs settle again. Unstable or over-depth combinational networks are flagged. State-to-state propagation may take one step; wire length has no delay.
- Mechanical speed is signed. Ratios conserve speed × available torque. Branches share torque. Conflicting multiple drive sources are reported rather than summed. Extreme ratios are capped with a diagnostic. This is a gameplay transmission model, not a continuous rigid-body solver.
- **Joints are anchored, grid-quantized quarter-turns**, with a sampled sweep and collision checks. Not arbitrary-angle rotation, freely falling bodies, or nested articulated rigid-body physics. Maximum 128 blocks per rotating assembly. Hinges swing 0–90°; bearings repeat quarter-turns.
- Leave a clear sweep around rotating loads. A belt can remotely drive a facing bearing/hinge from up to seven tile-center distances (six intervening cells). Its drive runs behind that joint's own linked load so the load does not interrupt its own belt; unrelated obstacles block it. A directly adjacent solid drive can physically obstruct a full turn.
- Stress is a generous gameplay estimate of force distributed across the assembly, using material strength and lever distance. Three extreme stress attempts detach a connection, never delete materials. Stress is not yet a full beam, static-gravity, or finite-element simulation.
- Item buffers hold 8 items (storage 64). Each item crosses at most one component per tick, with a 0.25-second transfer cadence. Splitters alternate strictly: a blocked selected branch waits. Filters reject unmatched items rather than choosing a bypass route. No automatic production recipes yet.
- Loose item entities are bounded at 512 stacks. Identical nearby drops merge. Full budgets block extraction/mining instead of destroying resources. Stored items do not count against the loose-entity budget.

## Save compatibility

The existing `circuitbound-save-v1` key is retained; saves now include schema version 2 and an engineering payload. Old worlds, player inventory and progress load normally. Old connected `linked` flags migrate to distinct assembly IDs. Component orientation, configuration, memory/counters, contents, joint state and loose drops persist. Existing builds may need rotation/rewiring because arbitrary side adjacency is intentionally replaced by typed ports. Save or back up an old world before rebuilding it.

## Acceptance playtest

1. Start fresh: move immediately, open and close H/E, inspect the powered starter lamp.
2. Generator → motor → shaft → piston → separately linked two-block load. Add a torque gearbox to move a heavier iron load. Inspect a deliberately blocked load.
3. Run motor continuously; wire a player sensor into the piston's top enable. Check that the door extends and retracts as the player approaches/leaves.
4. Pulse generator → counter (target 3). Watch rising edges; reset from the top. Check AND/NOT outputs do not backfeed input wires.
5. Generator → motor → clutch → shaft, with a switch above the clutch facing down. Toggle drive. Rotate a bevel to turn the drive path vertically.
6. Drive a bearing remotely with a belt; attach a separately linked frame to the front. Verify all four quarter-turns, then add an obstruction. Repeat with a controlled hinge.
7. Drive a conveyor from its rear or bottom, drop a component with X, and send it through hopper → storage. Split into two correctly oriented bins; fill one and inspect backpressure. Add a filter and test a rejected material.
8. Mine a container with cargo, collect every drop, save/reload a moving assembly with cargo and a counter, then start a new world and verify all old state is cleared.
9. Check narrow and short viewports manually, including hotbar scrolling and touch controls. Browser rendering remains to be playtested.

## Deferred, following the requested order

Second expansion: wheels/vehicles, drills/saws, production chains and automatic crafting, batteries/resistance/transformers, magnetism, blueprints/modules. Third expansion: fluids, steam, hydraulic/pneumatic actuators, heat, robotics, numeric signals and advanced automation. Additional sensors, electrical crossings/relays, mechanical joints (sliders, ropes, springs), structural gravity and construction challenges can build on this foundation. They are not represented as implemented features here.
