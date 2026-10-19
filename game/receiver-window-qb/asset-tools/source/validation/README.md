# Reproduce the asset checks

Requirements: Node.js 20 or newer, npm, and access to the npm package registry when installing dependencies.

```sh
npm ci
node validate-pack.cjs /absolute/path/to/extracted/pack
node three-load-smoke.mjs /absolute/path/to/extracted/pack
node physique-compatibility-test.mjs /absolute/path/to/extracted/pack
node animation-semantics.cjs /absolute/path/to/extracted/pack
node player-loader-import.mjs /absolute/path/to/extracted/pack
```

The commands write JSON reports into `validation/` inside the extracted pack. A nonzero exit code means at least one check failed. The recorded SHA-256 values identify the exact GLBs checked. Running against modified assets replaces the reports with new results.

`validate-pack.cjs` invokes Khronos' glTF validator, then independently examines binary accessors, triangle indices, normals, weights, skeleton joints, inverse bind matrices, named physique targets, PNG texture dimensions, material categories, animation timing and animation-category coverage. It also checks the required archive directories and distinct player assets. Full Khronos messages are retained per asset, including warnings and informational messages.

`three-load-smoke.mjs` imports the GLBs using `GLTFLoader` from Three.js r160, decompresses PNG texture payloads, evaluates every named physique target, and samples every animation at five points in time while computing skinned vertex bounds and key joint positions. It checks that jump clips lift in the glTF vertical axis, that high-point hands reach above the standing helmet, and that prone/transition clips do not place core or hand joints deeply below ground. It records inverse-bind/rest matrix differences. This runs without a graphics device. The Node texture shim decompresses the PNG and supplies dimensions to the loader; browser rendering must verify actual texture appearance.

These checks do not prove artistic realism, animation quality, successful hand IK, glove contact, physically correct gameplay, save compatibility or device frame rate. Those require visual inspection and integration tests in the game. The archive is an asset milestone; the browser preview and validation reports are not a claim that the later gameplay phases are complete.

`physique-compatibility-test.mjs` checks the supplied physique helper against the actual imported athlete. It verifies whole-rig height scaling, proportional rest-bone adjustment, repeat application and reset, rejection of invalid height without mutation, and isolation of two cloned actors from each other and their cached template. Its JSON report is `validation/physique-compatibility.json`.

`animation-semantics.cjs` independently samples each player clip at 30 Hz, evaluating every skinned vertex for ground penetration and recording key joints, jump lift, overhead hand reach, prone endpoint core height and finger curl direction. Its report is `validation/animation-semantics.json`. This geometric check complements the browser's visual motion review; it cannot establish that every motion is artistically natural or ready for live ball interaction.

`player-loader-import.mjs` loads an actual exported GLB through the supplied configurable loader, then checks cached-template cloning, skeleton/inverse-bind and animation ownership, and resource disposal across independent actors. Its report is `validation/player-loader-import.json`. The supplied loader is prepared integration code; production game replacement waits for the uploaded asset URL and the later engineering work.
