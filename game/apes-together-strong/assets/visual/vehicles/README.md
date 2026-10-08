# Vehicle and helicopter artwork

Four original RGBA PNG atlases contain 85 authored frames: 35 chassis views,
25 turret views, 15 helicopter body views, five rotor components and five wrecks.
They were created with the built-in imagegen tool. `prompts.json` records the
exact source prompts, including the correction to helicopter rear-quarter views.
The selected original PNGs retain their generated alpha channels unchanged.

The seven existing ground classes are jeep, command vehicle, armored patrol,
troop truck, APC, IFV and tank. Veteran, siege, ironclad and sentinel variants
share chassis art with cached material differences. Repeater, bombard and cyclone
use separately illustrated twin-barrel, mortar and rotary turrets. Aircraft cover
the existing recon, armed scout and gunship classes. No new gameplay class exists.

Five source views (south, southeast, east, northeast, north) plus horizontal
mirroring yield eight screen headings. World direction is projected through the
same 0.8 / 0.42 isometric transform as movement before choosing a heading. Turrets
resolve the existing `turretDir` independently. Crop bounds and attachment points
come from the actual images, not nominal grid cells. The gunship side view has a
crop polygon excluding a neighboring sprite; the original PNG remains untouched.
Run `scripts/build-vehicle-manifest.py` with Pillow to regenerate this metadata.

`vehicle-art.js` uses sprite blits and finite shared caches. It retains at most
96 material textures (four creations per frame) and 32 rotor animation textures.
Rotors keep spinning during hover and on Low/reduced-motion settings. Optional
bob, bank, suspension, wheel-hub glints, exhaust and dust follow those settings.
The essential rotor speed follows startup and explicit grounded/landed state.
Altitude uses a supplied `altitude` when present; existing launch/return behavior
has cosmetic ascent/descent and altitude-dependent shadow spread and opacity.

Health and component damage select operational, light, heavy or disabled
appearances. Authored smoke, dust, fire and muzzle sprites reuse the environment
atlas and share a bounded per-frame effect budget. Headlamp and brake glows are
small presentation effects; `getLights` / `drawLights` remain authoritative for
headlight beams, searchlight positions, visibility and detection. No art changes
attack ranges, damage, eligibility, movement, collision or navigation bounds.

World `vehicleWreck` objects are the canonical ground wrecks and use their existing
non-solid, bounded persistence behavior. Matching corpses are not drawn twice.
Legacy ground corpses and helicopter crashes use their existing simulation-owned
corpse lifetimes; a helicopter's preexisting ape corpse classification is intercepted
by ID so it cannot render as a fallen ape. Crash descent is visual only.

Validation: `node --test tests/vehicle-art.test.cjs` exercises class coverage,
direction and turret independence, animation clocks, decoded bounds, state
immutability, actual raster calls, cache limits and corpse deduplication.
`node tests/vehicle-art-browser.cjs` loads the complete source order in Chromium,
checks changing rotor pixels and renders all eight headings plus a scene with
28 vehicles and four helicopters. Set `QA_ARTIFACT_DIR` to retain screenshots
and render measurements. The broader gameplay benchmark is separate.
