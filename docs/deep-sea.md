# Deep Sea: Feeding Frenzy

`game/deep_sea.html` is a self-contained Canvas 2D game with a projected 3D ocean. Open it directly or serve it through the existing game server. No additional assets, dependencies, or network calls are needed.

## Playing

The fish swims forward automatically. Swim into smaller fish to eat them, grow, earn evolutions, and build a feeding chain. Meals within four seconds continue the chain, increase growth, and restore sprint energy. A jaw evolution increases the size of prey you can swallow.

- Desktop: click the water for mouse steering, or use I/J/K/L; A/D strafe; Shift sprints; Space or left click chomps. Q uses an unlocked burst and E uses an equipped special.
- Touch: the left stick steers, the right button sprints, and the special button uses an equipped ability.
- P or Escape pauses. The pause button and resume button work on both layouts. Losing focus also pauses.

The opening school sits directly in the swim lane at the player's depth. Eight seconds of initial protection begin when the starting evolution is chosen. Predator eyes and jaws flare during a 0.66-second windup, followed by a 0.58-second committed lunge and a 1.35-second recovery. Windup and recovery contact give the player an escape window. Larger fish also hunt and grow by eating other fish.

Evolution choices and pause freeze the simulation clock, preserving feeding chains and opening protection while reading. Restart clears the current run.

## Rendering and performance

Water lighting, seabed caustics, seeded coral gardens, leafy kelp, irregular rocky outcrops, marine particles, and predator silhouettes are procedural. Distant fish use inexpensive oriented sprites; close fish preserve the 3D mesh and visible evolutions. The render queue reuses records, culls objects outside the view, and sorts visible objects only.

Fish and obstacle broadphase grids limit local steering and collision searches. Fish population is capped at 150 including school and pack additions; trail bubbles are capped at 197 and bite effects at 160.

Auto quality adapts mesh detail and canvas resolution to measured frame cost and frame intervals. Vivid and Fast provide manual overrides. UI values update at 10 Hz, ability markup is cached, and hidden or paused views stop rendering. Actual frame rate depends on the device and browser.

## Validation

Run the deterministic gameplay suite with:

```sh
node --test tests/deep-sea-living.test.cjs
```

It covers opening food density and three-second first meals, population caps, size rules and jaw evolutions, NPC predation, growth and sprint recovery, pause and feeding-chain timing, attack phases and contact fairness, spatial boundaries, and a sustained ecosystem/restart scenario.

The browser check uses the repository's Playwright dependency:

```sh
node tests/deep-sea-browser.cjs
```

It checks desktop and touch layouts, steering and sprinting, pause/resume, quality controls, and reef/deep rendering while recording frame and render timings. The harness injects its test interface into an intercepted local page; production does not expose test globals. Optional `DEEP_SEA_PLAYWRIGHT`, `DEEP_SEA_BROWSER`, `DEEP_SEA_SOURCE`, and `DEEP_SEA_OUTPUTS` environment variables select an installed Playwright package, browser binary, comparison source, or screenshot/report destination. `--baseline` supports comparison with the previous game.

The completed local check passed all 16 gameplay tests and both browser layouts without runtime errors. Seeded headless scene comparisons measured:

| Viewport | Previous median drawing time | Updated median drawing time | Updated p95 |
| --- | ---: | ---: | ---: |
| Desktop, 1280 × 720, scale 1 | 17.0 ms | 2.1 ms | 4.4 ms |
| Touch, 390 × 844, scale 2 | 17.5 ms | 2.7 ms | 4.1 ms |

Median frame intervals were 16.7 ms in both updated profiles, with p95 of 33.4 ms. These are local scene measurements; device and browser differences affect frame rate.
