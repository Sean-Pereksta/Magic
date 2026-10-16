# Basketball 2 athlete models

These are original game assets, authored for Basketball 2. All three are real
glTF 2.0 binary models with embedded geometry, an articulated skin, inverse bind
matrices, PBR materials and twelve movement clips. They require no remote mesh,
texture, decoder or animation service.

## Delivery

The game first fetches `player-guard.glb`, `player-wing.glb` and
`player-center.glb` from the root of
`https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/`. Each file independently
falls back to the matching bundled `assets/models/` file if the remote request
fails, takes more than 6.5 seconds, returns an error, or contains an invalid model.
The timeout covers downloading the response body as well as opening the request.
If both sources fail,
the procedural player remains playable. Players already on court receive their
rigged model when loading completes; repeated starts reuse the cached assets.

The R2 bucket must permit cross-origin `GET` requests from the game's website
origin (or `*` for these public assets). Browsers require the response's
`Access-Control-Allow-Origin` header before JavaScript can read the GLB. No
credentials or custom request headers are used. Bundled models continue to
work when remote CORS is unavailable.

| Asset | Build | Distinctive features |
| --- | --- | --- |
| `player-guard.glb` | Agile | Narrower shoulders, shorter frame, high fade |
| `player-wing.glb` | Balanced | Medium frame, modeled textured curls |
| `player-center.glb` | Strong | Wider shoulders and legs, taller frame, crop |

The athletic silhouette has continuous, tapered arm and leg surfaces with soft
weights across elbows, wrists, hips and knees. Jerseys, shorts, collar/hem piping,
socks, wristband, shaped sneakers, outsole, laces, hair and facial features are
modeled into the mesh. Colors are runtime materials, so existing uniforms, player
skin/hair colors and shoe selections continue to work.

## Rebuild and verify

```sh
node game/basketball2/tools/build-models.cjs
node --test game/basketball2/tests/models.test.cjs
```

The authoring script has no package dependencies. Its output is deterministic.
`manifest.json` lists generated file sizes, dimensions, vertex/triangle counts,
bone count and available clips. The tests inspect embedded data, accessor bounds,
skin weights, inverse bind matrices and animation quaternions. They also evaluate
the actual hierarchy to verify shooting hands reach above the head and running
feet/hands travel through alternating strides.

## Runtime contract

- Units: meters, Y up, +Z forward, feet at floor zero. Body proportions vary;
  `asset.extras` and `AthleteRoot.extras` include `authoredHeight`, `authoredWidth`,
  `profileHeight` and `profileWidth` for fitting the existing gameplay bodies.
- One skinned mesh, twelve color-based material primitives, 24 shared bone names.
- Bind pose uses translation-only nodes. Four skin weights per vertex; up to
  four influences. All motion channels use standard LINEAR interpolation.
- Bone hierarchy: `root → hips → spine → chest → neck → head`;
  `chest → clavicle_L/R → upper_arm_L/R → forearm_L/R → hand_L/R → thumb_L/R`;
  `hips → thigh_L/R → shin_L/R → foot_L/R → toes_L/R`.
- `_L` is anatomically left (+X when facing +Z); `_R` is anatomically right (-X).
- Material names: `jersey`, `shorts`, `trim`, `skin`, `hair`, `sneaker`, `sole`,
  `sock`, `eyes`, `mouth`, `number`, `wristband`.
- Authored numbers are modeled fallback numerals; the game hides that primitive
  and adds individual jersey-number planes. Chest-local center is approximately
  `(0, -.07 × profileHeight, ±.157 × profileWidth)`.

| Clip | Duration | Behavior |
| --- | ---: | --- |
| `idle` | 2.00 s | Breathing, slight head motion, relaxed elbows |
| `run` | 0.72 s | Alternating strides, knee flex, counterbalancing arms |
| `sprint` | 0.54 s | Longer stride, stronger elbow swing, forward lean |
| `defensive_shuffle` | 0.80 s | Crouched lateral steps, wide arms |
| `pivot` | 0.60 s | Planted turn, torso twist, ball protection |
| `jump_shot` | 0.95 s | Gather, knee load, extension, wrist follow-through |
| `layup` | 0.90 s | One-knee gather and high one-hand release |
| `dunk` | 1.00 s | Two-hand gather, overhead extension, rim finish |
| `pass` | 0.55 s | Chest gather, arm extension, hand release |
| `catch` | 0.45 s | Reach then absorb into the chest |
| `block` | 0.80 s | Knee load and two-arm overhead contest |
| `crossover` | 0.65 s | Hip dip, torso change and low hand transfer |

Clips stay in place; game simulation owns world travel and jump height. The hips
have small local bob/crouch translations. One-shot clips return toward neutral;
the runtime blends them with locomotion and ball/defense hand positioning.
