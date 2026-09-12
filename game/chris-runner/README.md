# Chris Runner upgrade

The existing `game/chrisrunner.html` entry point loads `upgrade.js`. No build step or new runtime dependency is required; both files must be deployed together. The current Three.js 0.160.0 dependency remains unchanged.

## Controls and characters

Movement and the upright follow camera are retained. Player 1 uses Space for the selected character's special and Shift for sprint. Player 2 uses O for special and Enter for sprint. On touch screens, tap the existing sprint button in under 180 ms for the special; hold it to sprint. Cancellation, lost pointer capture, focus loss, and hidden tabs release sprint. Low energy uses a vignette and a recovery threshold to avoid alternating run/sprint speeds each frame.

| Character | Ability | Energy | Tradeoff |
| --- | --- | --- | --- |
| Chris | Repulse: push and briefly stun nearby enemies | 30 | Efficient endurance; short reach |
| Dawit | Dash: fast, collision-checked movement | 30 | Fastest runner; higher sprint drain |
| Jay | Sidestep: directional dodge with a brief shield | 20 | Excellent lateral movement; slower forward pace |
| Anthony | Parkour: animated vault over marked obstacles | 50 | Athletic, agile; expensive ability |
| Kevon | Smash: destroy marked terrain and surge through | 25 | Heavy build and afro; slowest runner |

Amber braces mark breakable/vaultable obstacles. Solid capped obstacles remain impassable. Invalid targets or blocked landings spend no energy. Low hurdles in optional parkour routes automatically vault for every runner; Anthony traverses faster and can clear taller marked barriers. Specials activate once per press, with per-player cooldowns.

## Sections and environments

Six section patterns—parkour, slalom, encounter, narrow passage, shortcut, and recovery—reserve connected approaches with player-radius clearance. Optional routes offer gold pickups. Recovery sections have energy pickups and no new enemy spawns; existing pursuers can still follow. The planner's safety guarantee covers static terrain, not immunity to pursuing enemies or timed hazards.

Ten environments transition every six chunks (252 yards): forest, city, rooftops, construction, industrial, tunnel, ruins, snow, desert, and dream. Urban environments add windowed buildings, rooftop platforms, cranes, tanks/pipes, tunnel supports, and rubble. Color/fog/lighting blend using the existing sky system. Tunnel supports leave the chase camera's view open.

Enemy roles include ordinary chasers, windup/burst sprinters, barrier-breaking heavies, warning-before-activation ambushers, lateral path blockers, obstacle jumpers, ranged throwers, and rare elite pursuers. Existing dogs, police, desert stalkers, and dream threats remain.

## Resource management

Scenery boxes use per-chunk instancing. Removed chunks, enemies, projectiles, debris, and temporary effects dispose GPU resources. Mobile uses reduced pixel ratio, 512px shadows, fewer debris particles, and fewer outline draws. Gameplay generation does not change with graphics quality. Temporary debris uses cosmetic deterministic offsets instead of consuming gameplay randomness.

The replay build identifier changed because generation and input semantics changed. Older-build replay files are explicitly rejected rather than replayed against different terrain. New touch ability events are recorded. The pre-existing wall-clock/snapshot replay mechanism remains; exact deterministic replay at every frame rate is not claimed.

## Validation

Dependency-free syntax and 10,000-section route tests:

```sh
node --test game/chris-runner/upgrade.test.cjs
```

Full integration tests use the same Three.js browser build as the game:

```sh
curl -fL https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js -o /tmp/chris-runner-three.js
THREE_SOURCE=/tmp/chris-runner-three.js node --test game/chris-runner/upgrade.test.cjs
```

All ten tests passed during implementation, including abilities, blocked landings, automatic hurdles, touch cancellation, distinct enemy behavior, energy exhaustion, and 300 chunks / 12,600 yards of simulated cleanup. Tests use real Three.js scene objects with a simulated DOM; they do not validate GPU rendering or device frame rate. Browser rendering and physical-phone performance were not verified because a browser runtime could not be downloaded in the implementation environment. No HTML preview was generated.
