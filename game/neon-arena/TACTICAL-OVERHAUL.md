# Neon Breach tactical overhaul

The update rebuilds arena routes and physical scenery, adds a tap-to-slide movement system, and replaces human bot decisions with perception-driven tactics. The five modes, two local players, weapon inventories and upgrades, building, Survival species/bosses/equipment, progression, profile synchronization, and existing hosted models remain in place.

## Maps and physical geometry

| Arena | Added layout and environmental detail |
| --- | --- |
| Circuit Citadel | Generator compounds, loading yards, connected rooms, supported service decks and broad ramps; authored generators, transformers and cargo. |
| Prism Garden | Offset research wings, laboratory courtyards and observation routes; experiment chamber, consoles and laboratory machinery. |
| Obsidian Reactor | Shielded central core, turbine zones, twin maintenance tunnels and ramped galleries; authored turbines, reactor machinery and rotating fans. |
| Skyrail Terminal | Staggered train cars, twin passenger platforms, a cross-concourse and linked service shops. |
| Ashfall Foundry | Asymmetric furnace yard, L-shaped maintenance gallery, lower loading gantry and turbine machinery. |
| Rift Outpost | Grounded cliff mesas, ancient arch, survey bridge and desert research shelters. |
| Breach Ring | Defensive checkpoints, barracks, watchtower routes and cargo fortifications. |
| Derelict Station | Ticket/newsstand interiors, gates and connected terminal routes around the existing train cars. |
| Xenofall Wastes | Survey shrine, crystal ridge and desert routes around the existing alien cliffs, arches and wreckage. |
| Black Ice Cryolab | Two containment laboratories and connected frozen research spaces. |
| The Living Hive | Ribbed nesting chamber, organic cover and connected raised routes. |
| Drowned Powerworks | Switchgear rooms, flooded turbine basin, machinery and fans. |
| Orbital Graveyard | Observation chamber and cargo routes around the existing docking wreckage. |
| The Swarm Heart | Brood gateway, vascular gallery and pulsating organic detail. |

All standard arenas reuse the authored Survival atlas and prefab infrastructure. Existing Survival landmarks, hazards, unlock requirements and map identities remain. Steam, sparks, warning lights, fans and organic pulses use a small animation buffer updated at a bounded rate.

The standing collider now accounts for the complete **2.39-unit human model**, rather than its 1.86-unit eye position. Accessible platform undersides require **2.57 units** of clearance. `validateGeometry` checks raised platforms, stacked surfaces and ramp corridors; `validateBlueprint` also checks prefab references, finite compound colliders and visual mesh coverage. Invalid authored geometry stops map loading. CI runs these checks before changes reach the default branch.

Prefab components receive appropriately sized compound collision shapes. Movement, hitscan, projectiles, line of sight, cover evaluation and navigation use the shared physical blocks. Destroying a compound prop removes all its collision components and its visible mesh together, and invalidates navigation/render state. Background objects and explicitly explained atmospheric decorations remain non-solid.

Ramp walking uses continuous slope support while collision treads retain physical solidity. Short, grounded transitions join ramps to decks without a jump or an uphill/downhill snap. Spawn selection rejects physical overlaps and scores candidates for enemy exposure, separation and nearby cover.

## Movement and controls

| Input | Slide |
| --- | --- |
| Player 1 keyboard | Tap Shift while moving |
| Player 2 keyboard | Tap Numpad Decimal while moving |
| Controller | Press left stick; configurable independently of jump/build |
| Touch | Tap the compact Slide button |

Slides last 0.72 seconds, preserve the initial movement direction, add a bounded momentum burst, allow limited steering and decay smoothly. A 0.60-second cooldown prevents repeated bursts. Walls stop the slide; jump transitions consume the existing buffered jump request once. Standing-height collision remains active during the lowered visual posture, so sliding cannot exploit low overhead gaps.

The camera, field of view, weapon pose, rigid character parts, aim origin, hitboxes, touch auto-aim and visibility checks follow the slide posture. Sliding increases shot spread by up to 32%. Existing jump buffering, coyote time, Volt Dash, grappling and independent split-screen input remain.

## Combat feedback

- Weapon-specific muzzle shapes, bounded glow, recoil, smoke and procedural firing sounds are synchronized with actual shots.
- Metal sparks, concrete dust/chips, glass fragments, organic reactions and shield rings use material metadata and the actual near-side collision point.
- Hitscan tracers terminate exactly on their collision point. Projectiles sweep from the firing position rather than appearing beyond nearby cover. Rays starting inside a solid stop immediately.
- Shield, armor, health, headshot and defeat feedback differ. Directional incoming-damage cues are separate for both local players. Hit reactions and weapon switch recovery add animation without hiding targets.
- Chained shots and explosive splash respect physical cover; the existing explicitly allowed rail penetration rules remain.
- Sound effects and quiet map-specific ambience have a persisted Sound effects setting. Sound output can be disabled without disabling AI hearing.

The Punch Gun now launches an authored solid 3D fist: four curled fingers,
knuckles, a wrapped thumb, palm and wrist cuff. It turns with the authoritative
flight/return direction and returns along an interlocking 3D chain. The Harpoon
launches a faceted spearhead, barbs and shaft on a thin 3D cable. Latched heads
stay at the real impact point or follow the hooked actor. Both renderers share
the world depth buffer, so heads and tethers are occluded by cover. These meshes
are generated locally once; the existing weapon GLBs and collision, damage,
pull/grapple and upgrade rules are retained.

Rocket, mortar, plasma and seismic blasts use distinct short-lived volumetric
flame lobes, hot cores, soft smoke, glowing fragments and ground shockwaves.
Nearby physical faces bound the visual geometry; the world depth buffer hides
effects behind cover. Splash damage still uses the existing combat visibility
checks. Effects expire after at most 1.4 seconds, pool at most sixteen records,
and batch into one draw per view, with ten/eight/six visible effects at high/
medium/low quality and fewer smoke, flame and fragment elements on low quality.

## Match setup

The menu presents mode → arena → settings, with six actual game-rendered arena
previews and a larger selected-map view. All six standard arenas support DM,
Team DM, Frag and Siege; Survivor retains eight locked/unlocked map cards and
its original progression. Arena choices and settings persist independently per
mode and recover safely from invalid saved values.

DM/Team DM accept 0–16 bots, Frag 0–1 (zero with a second local player), and Siege
2–16 attackers. Easy/Normal/Hard/Nightmare difficulty, DM/Team DM round duration,
DM/Team DM/Frag elimination targets, respawn delay, sensitivity, sound volume and
graphics controls are wired into the simulation. Mode-inapplicable controls are
hidden or disabled with a short explanation. Keyboard/controller menu navigation
and touch-sized map cards remain available.

## Tactical AI

Human bots use Aggressor, Flanker, Defender, Marksman and Balanced Soldier profiles, retaining their existing character models and preferred loadouts. They evaluate real cover and reachable peeks, sprint or slide to positions, strafe, seek elevated routes, retreat when hurt/reloading, flank exposed opponents and coordinate locally with teammates. Automatic weapons use short bursts and pauses.

Bots acquire targets through field of view and physical line of sight. Reaction times range from roughly 0.50 seconds on Easy to 0.14 seconds on Nightmare, with continuous imperfect tracking, recoil and increased moving/slide aim error. Losing sight immediately stops direct firing. Last observations expire; gunfire, steps and slides store position snapshots with uncertainty rather than retaining a hidden actor reference. Nearby teammates can communicate recent observations.

Recent defeats teach a bot to avoid its own dangerous lane and favor cover/flanks; successes modestly change its willingness to push. Learning is bounded, expires, survives respawns and clears with a new map/match. It never changes damage or grants perfect aim/awareness.

Survival keeps species-specific movement and boss abilities. Ground and flying routes consult actual physical geometry, closed routes stop instead of crossing walls, and breachers attack reachable faces of player defenses. Burrow/phase shortcuts remain restricted to their telegraphed species abilities.

## Compact HUD and allies

Match information uses one compact top-left panel for each local player's view. The large startup/help feed, redundant score bar and minimap no longer occupy the play area. Crosshairs, brief combat feedback and touch action buttons remain usable; menus, match settings, upgrades and results retain their existing functions.

Team Deathmatch allies receive a cyan rim and subtle body highlight in the normal depth-tested model pass, plus a small friendly marker when visible. Each local view evaluates its own team, enemies retain their normal appearance, and physical walls occlude the highlight. Verbose world-space name/weapon labels are removed.

## Asset inventory and performance

**No additional graphics upload is required.** New detail uses the checked-in map JSON, prefab definitions, common mesh JSON and existing atlas graphics:

```
assets/survival/maps/<map_id>.json
assets/survival/tilesets/<map_id>/prefabs.json
assets/survival/tilesets/<map_id>/prefabs/*.json
assets/survival/tilesets/<map_id>/atlas_color.webp
assets/survival/tilesets/<map_id>/atlas_color_mobile.webp
assets/survival/common/meshes/*.json
assets/survival/common/meshes/generate.py
assets/ui/maps/<arena_id>.webp
neon-projectile-models.js
neon-explosion-fx.js
```

The eight 2048×2048 desktop and 1024×1024 mobile atlases, padding and UV conventions are unchanged. Character, weapon and R2 asset references are unchanged. The common mesh generator reproduces the updated component collision definitions.

Static geometry remains chunked and cached across both split views. All eight Survival environments remain below the existing 160,000-vertex test budget with 36–37 cached chunks. Particles remain capped at 170/120/78 for high/medium/low; ambience uses at most 18/8 visual details for high/low. Impact decals, sound voices and navigation caches are bounded. Perception is staggered and paths are cached rather than recomputed for every frame.

## Validation and release checks

Run the dependency-free suites with `npm run test:neon`. Run browser checks with Playwright installed, `NEON_ASSET_FIXTURES` pointing to the existing weapon/character model pack, and optionally `CHROMIUM_PATH` pointing to Chrome:

```
npm run test:neon:browser
```

The browser checks cover all five modes and 14 weapons, all fourteen arenas, atlas pixels, render caching/destruction, Survival unlock/save/results flows, mobile portrait/landscape and split-screen layouts, saved match settings, actual slide input and ramp walking. Ramp cases exercise the production movement/collision functions without jumping, in both directions, for both players, at the center and two side lanes and at 60/30/10 movement updates per second.

The full ramp sweep passed **1,620 walking traversals across all 45 ramps on all fourteen maps**, plus **432 Frag traversals across the twelve new ramps**, with no movement failures, page errors or WebGL errors. Frag boundaries leave standing landing clearance around the new ramp toes.

The local regression run passed **184 tests with no failures or skips**, using the existing model fixtures. The compact HUD passed twelve desktop/mobile/split layouts, and the ally-rendering test verified both local team perspectives and zero highlighted pixels through a solid wall. Siege startup checks also validate both defenders and all three supplies on every standard map. Projectile tests render different camera angles and actual launch/return/latch states; projectile and explosion tests verify zero contributed pixels behind a solid wall and restored depth/blend state.

Browser rendering checks use real WebGL with SwiftShader. Mobile dimensions, touch input and controller button edges are automated; these are **not physical phone/controller tests or real-device FPS measurements**. Before release, measure sustained desktop 60 FPS and mobile 30–60 FPS on representative hardware, and playtest physical controllers/touch devices, map balance, weapon feel and audio mix. Those hardware and subjective acceptance checks remain open.
