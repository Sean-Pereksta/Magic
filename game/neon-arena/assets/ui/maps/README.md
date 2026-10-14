# Arena previews

These six 960×540 WebP images show the actual playable geometry and materials,
captured by the game renderer with actors, weapons, and HUD omitted. Camera poses
are recorded in `tools/capture-map-previews.cjs`; no concept art is used.

Regenerate from the repository root with Playwright and a WebGL-capable Chromium:

```sh
NEON_ASSET_FIXTURES=/path/to/existing-model-pack node game/neon-arena/tools/capture-map-previews.cjs
```

`CHROMIUM_PATH` can select an installed browser. The script replaces the six WebP
files here and saves full-size PNGs plus camera metadata under `work/map-previews`
(override with `NEON_PREVIEW_OUTPUT`). Existing hosted assets are used when no
local asset fixture is supplied.
