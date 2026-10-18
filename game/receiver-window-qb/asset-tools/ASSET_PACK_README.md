# Receiver Window QB — Next Generation Football Models

This is the first asset milestone for `Sean-Pereksta/Magic`, game `game/receiver-window-qb/`. The archive contains actual loadable glTF 2.0 binary models. The gameplay overhaul and live R2 activation are later milestones.

These are original parametric athletes with shaped anatomical surfaces, fitted uniforms, engineered helmet shells and cage bars, pads, five-finger gloves, and cleats with laces and studs. The art direction is stylized realism. They are not scans, motion capture, or photoreal production characters. The source creates new assets independently of the existing game's procedural figures.

## Contents

- `models/quarterbacks/`: field general and mobile quarterback.
- `models/receivers/`: speed, route technician, possession, jump ball, physical, and slot receiver.
- `models/defenders/`: press corner, ball hawk corner, hybrid safety, and coverage linebacker.
- `models/other-positions/`: elusive and power running backs; receiving and blocking tight ends.
- Each of the 16 athletes has a full-detail `.glb` and a lower-detail `_lod1.glb` with the same rig, morph names, and animation library.
- `animations/shared_humanoid_clips.glb`: reusable animated skeleton; clips are also embedded in every player GLB.
- `animations/clip_library.json`: clip timing, looping, and root motion metadata.
- `equipment/`: helmet/cage/visor, articulated glove, cleat, and football GLBs.
- `textures/`: fabric color, normal, and roughness PNGs. Player textures are embedded in the GLBs as well.
- `source/build_football_assets.py`: complete Blender authoring/export source.
- `source/player-assets.js`: the prepared, inactive game-side loader and its isolated tests. It contains no R2 URL and is not connected to the game's rendering loop yet.
- `preview/`: offline-dependency browser viewer, shared physique adapter, local server, and Three.js r160 with its MIT license.
- `validation/`: format, skin, morph, material, animation, and import reports, plus reproducible validation tools.
- `manifest.json`: model IDs, paths, physical measurements, contact anchors, proportions, rig, morph semantics, LODs, and limitations.

## Inspect the actual exports

Extract the ZIP, then run this from the extracted directory with Python 3:

```text
python preview/serve.py
```

The script serves the extracted folder on `127.0.0.1:8765` and opens `/preview/`. It accepts only local connections. If that port is busy, use `python preview/serve.py --port 8766`. No package installation or remote graphics CDN is needed. Opening `index.html` directly with `file://` will fail because browsers restrict model and module loading in that mode.

Choose an archetype and animation; drag to orbit and scroll/pinch to zoom. The height, strength, shoulder, leg, wingspan, and hand controls modify the actual loaded GLB, including joint proportions. Team colors change separate material groups. Full detail/Lower detail switches between real exported LOD files.

## Rig, units, and physique

GLBs use meters, +Y up and +Z forward, with the feet close to ground level. Barehead height and helmet top are separate measurements. Contact anchors in the manifest are reference-pose points, not collision radii. Extended reach values are sums of actual rig and glove lengths and must not award catches by themselves.

The existing game's field uses two world units per yard, equivalent to approximately 2.18723 world units per meter. Its old avatars use a different visual scale. Integration must reconcile this scale across body dimensions, hand IK, collision shapes, ball attachment, camera, movement and field dimensions before replacing those avatars. Loading the GLB at an arbitrary visual scale while retaining old reach constants will not preserve physical alignment.

All athletes use the same 50-joint topology, including individual finger chains. Bone names in the GLB use `.L`/`.R`; Three.js sanitizes these to names such as `handL` while preserving the original name in `bone.userData.name`.

Nine additive shape targets are exported: `Muscularity`, `ShoulderWidth`, `ChestSize`, `ArmThickness`, `LegPower`, `BodyWeight`, `Wingspan`, `HandSize`, and `Reach`. Each target ranges from 0 to 1 relative to an archetype's existing build.

`preview/physique.js` implements the matching runtime rest-joint adaptation. It updates shape weights, bone rest offsets, inverse bind matrices, and animation translation tracks together. Create the controller before positioning a character in the game world, pass an independent skeleton and independent animation clips, apply a profile, then let an animation mixer resume. Later profile changes preserve the actor's placement, including nested rotation and scale. Height uniformly scales the entire model relative to its manifest `heightM`; do not separately stretch only the visible mesh. Strength 60 to 95 drives muscle, shoulder, chest, arm, and leg development. Weight is a separate volume parameter; mass in kilograms remains explicit gameplay data.

The viewer demonstrates height selection. It does not implement franchise rules. In realistic franchise play, adult height must remain fixed while jump/reach technique can develop; an arcade growth option can use this same profile system. All physical reach and glove contact must be measured from the resulting posed skeleton and visible glove geometry.

Authored clips cover walking, jogging, sprinting, acceleration, planting and cuts, jumps, high-point/one-hand/two-hand/back-shoulder/over-shoulder/diving/toe-tap catches, standing/jumping/trailing/diving defensive breakups, tip drills, snatch/undercut/diving interceptions, tackling, falling, recovery, celebrations, quarterback release, and defensive backpedaling. Root offsets are in meters. Runtime blending, grounding, ball-aware hand IK and contact adjudication remain engine work.

Default jersey numbers and nameplates are modeled typography. Arbitrary runtime number changes require replacing or isolating that geometry; team-color material edits already work. Helmet/facemask style selection and a full team builder are later integration features.

## Rebuild

Use Blender 5.2 in background mode. The script requires only Blender's bundled Python:

```text
blender --background --python source/build_football_assets.py -- --pack /absolute/path/to/rebuilt-pack
```

The source uses deterministic geometry and palettes. `--only qb_field_general` is useful for a single-archetype iteration. The interactive preview and validation tools are supplied alongside the generated files and should be preserved when rebuilding.

Reproduce checks with Node.js 20+ by running `npm ci` inside `source/validation/`, followed by the commands in that folder's README. The prepared loader's separate tests run with `node --test source/player-assets.test.cjs`. After all reports pass, `python source/package_assets.py --output /absolute/path/ReceiverWindowQB_NextGen_Football_Models.zip` creates a ZIP outside the pack, verifies it, and writes `SHA256SUMS.txt` for the unpacked contents. It refuses packaging when a validated GLB has changed.

## Upload to Cloudflare R2

Upload the archive's **contents** beneath one public asset directory, preserving the relative paths. `manifest.json` belongs at that directory's root alongside `models/`, `animations/`, `equipment/`, and `textures/`. Configure CORS to allow the game origin to fetch the manifest and GLBs. Serve `.glb` as `model/gltf-binary`, JSON as `application/json`, and PNG as `image/png`.

No R2 URL is embedded or assumed. Supply the public base URL after upload. The prepared game-side loader remains inactive until that configuration is provided and the new rig is connected to gameplay contact, animation, replay, and physique systems.

## Acceptance scope

Read `validation/` for results from the actual exported files. Format validation and import checks do not prove football behavior or frame rate. This pack does not by itself complete passing-window physics, contested catches, defender decisions, new modes, recruitment, training, save migrations, or live cloud integration. Desktop/mobile performance must be measured with these imported models in the final game. Authored keyframes require polish and integration testing before a production release.

The delivered build has zero Khronos validation errors and 334 nonfatal warnings: 300 concern tangent generation for normal maps, and 34 concern skinned meshes beneath their rig nodes. Three.js imports all 37 files successfully. The equipment football also has 124 near-zero triangles at its pointed pole caps; all player surfaces have no degenerate triangles. These details and exact asset hashes are retained in the per-file reports.
