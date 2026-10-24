# Battlefield artwork and asset delivery

The optional battlefield imports `battle-renderer.mjs` only when the player enters a battle. It uses WebGL 1 with instanced drawing when available, and a compatible ordinary-draw fallback. There is no third-party 3D runtime or CDN dependency.

The included `assets/battle/v1/pack.json` is original, articulated low-poly artwork: shaped human bodies, helmets, cloth, hand-held weapons, heraldic shields, horses, and crewed engines. All 15 current troop keys have distinct equipment, including the legacy `siege` model. The horse is also available independently. All 12 campaign Houses have a cloth palette and authored shield/banner emblem. A rider and horse still represent one simulation troop; attached engine crews are artwork only.

## Source and reusable package

`tools/battle-model-source.mjs` authors the geometry, materials, rig hierarchy, and clip metadata. `tools/generate-battle-assets.mjs` produces the compact runtime package and a reusable asset bundle:

```text
node game/iron-throne/tools/generate-battle-assets.mjs <output-directory>
```

The bundle contains `battle/v1/models/*.gltf` and their binary buffers, animation tracks, editable House heraldry SVGs, original WAV cues, a manifest with SHA-256 hashes, and a README. The glTF files import into standard editors. All original assets are CC0-1.0. Runtime animation applies the simulation's current walk, attack, block, shoot/reload, climbing, mount, and siege-action progress to the model hierarchy. The commander visibly equips the chosen weapon and dismounts when the simulation does.

## Existing R2 workflow

The default public asset base is `https://pub-47f679f65f034fbda4c4b2ee31b3818a.r2.dev`. Upload the bundle contents with their paths intact; the runtime model URL becomes `battle/v1/pack.json`. Enter Battle automatically loads this Cloudflare package and caches it for offline play. Serve JSON with CORS GET permission for the game origin. A remote failure falls back to the included local package. Set `globalThis.IRON_THRONES_BATTLE_ASSET_BASE` to another base to override the host, or to an empty string to use only bundled assets.

The settings download action saves the validated model pack in browser Cache Storage. Loading checks that cache before fetching; a failed load never silently substitutes generic placeholder soldiers. Retry clears the in-memory failed request and re-fetches. If both local and configured remote sources fail, the caller receives a recovery error while the saved encounter remains pending. A download reports failure if site storage cannot persist the pack. Clearing browser site data removes the offline model cache; the base campaign files still need to be available to open the game itself.

## Renderer contract

```js
const renderer = await createBattleRenderer(canvas, battle, {
  onProgress, signal, assetBase, retry: false, sound: true
});
renderer.render(battle, { mode: 'third', yaw, pitch, distance, target });
const point = renderer.pick(clientX, clientY); // { x, y, z, unitId } or null
renderer.resize();
renderer.setMuted(true);
renderer.dispose();
```

`mode: 'tactical'` uses an elevated camera. Coordinates match the simulation: Y is height, X/Z are the ground plane, and facing is `atan2(dx, dz)`. Hills use the same height function. Structures, bridges, trees, rocks, water, ladders, and wall damage are rendered from the simulation layout; breached walls immediately lose their visible blocking surface. Destination rings and arrows show ordered positions and facing. Corpses, shadows, impact particles, commander standards, and engine crew do not change troop counts. Audio starts only after a pointer gesture and closes on disposal.

The caller owns simulation time, input, checkpoints, and the animation loop. `dispose()` releases GPU buffers, the shader program, audio, observers, event handlers, and decoded model references. WebGL context loss throws a recoverable error. The caller must pause and retain its checkpoint, then offer recovery or automatic completion.

## Verification

`node game/iron-throne/tests/battle-renderer-browser.mjs` exercises all troop meshes, instancing, tactical/personal cameras, picking, safe disposal, offline cached loading, and model-fetch failure. It supports `IRON_THRONE_CHROMIUM` and `CODEX_PRIMARY_RUNTIME_NODE_MODULES` like the existing browser checks. Set `BATTLE_SCREENSHOT` to an absolute PNG path for a review image. Test geometry and glTF exports with `node --test game/iron-throne/tests/battle-assets.test.mjs`.

The rendering style is deliberately lightweight and stylized. Frame-rate targets need measurement on the intended range of desktop and mobile hardware; the browser smoke check verifies rendering behavior rather than promising a hardware-independent frame rate.
