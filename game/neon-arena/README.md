# Neon Breach

Neon Breach is a dependency-free browser arena FPS with solo bots, two-player local split screen, destructible building, and five game modes.

## Run

Serve the repository root with any static HTTP server and open `/game/neon-arena/`. The game uses WebGL for the depth-buffered arena and Canvas 2D for actors, effects, first-person weapons, and the HUD.

## Mobile play

Phones and tablets are detected automatically and Player 1 switches to a touch input adapter. The left analog stick moves with full analog speed control, the right stick looks, and dedicated buttons handle jumping, building, weapon swaps, reloads, build mode, and upgrades. Auto-fire only locks enemies inside a small cone around the crosshair after a world ray confirms that the target is visible; target scans are throttled so touch assistance does not become a new per-frame performance bottleneck.

## Files

- `index.html` contains the menu, HUD, overlays, and accessibility-friendly controls.
- `neon-arena.css` contains presentation and responsive menu layout.
- `neon-arena.js` contains the game simulation, render pipeline, modes, bots, weapons, maps, and input adapters.

## Extension points

- Add or tune weapons in `WEAPONS`, then add their compact first-person dimensions in `gunSpec`. Optional muzzle and pickup silhouettes live in `MUZZLE_STYLE`, `drawMuzzleFlashes`, and `drawPickupGunIcon`.
- Add arenas through a `build…ArenaMap` function, register their label in `MAP_META`, and route them from `buildMap`.
- Add bot personalities in `BOT_ARCHETYPES`; weapon-distance behavior is centralized in `aiProfileForBot`.
- Add modes through the mode selector and isolate setup/update rules next to the existing Survivor, Siege, Frag, DM, and Team DM handlers.
- Extend mobile actions through `setupMobileControls`; aiming and visibility rules live in `mobileAimInfo` and `updateMobileAimAndFire`.

## Performance guardrails

- Static world geometry is compiled into cached GPU buffers and rebuilt only when structures change.
- A spatial hash limits movement, raycast, wall-contact, support, and collapse work to nearby structures.
- Active player builds are capped at 320 pieces to prevent runaway simulation and render cost.
- Auto graphics mode adjusts internal resolution and effect budgets from rolling frame time.
- Effects, labels, and pickups use visibility budgets and bounded particle/decal arrays.
- Split screen shares world caches while keeping separate player, controller, camera, inventory, score, health, and build state.

Keep new effects bounded, prefer cached world geometry for static additions, and use `blocksNear` or `blocksAlongRay` instead of scanning every structure in per-frame code.
