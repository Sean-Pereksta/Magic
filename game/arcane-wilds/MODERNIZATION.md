# Arcane Wilds presentation modernization

This pass retains the existing spell catalog, auto-attacks, equipment, progression, enemy AI, village services, reactions, and save IDs. It adds presentation and action-input ownership around the existing simulation.

## Ownership and initialization

The entry document now declares the entire script order explicitly. `mobile-interaction.js` no longer injects four late scripts. Existing content loads before the performance governor, navigation, world/UI presentation, and final crash containment.

| Responsibility | Owner | Integration |
| --- | --- | --- |
| Keyboard, mouse, touch and gamepad actions | `input-manager.js` | 150 ms intent buffer, proportional sticks, mappings, device prompts, focus/disconnect cleanup |
| Camera, animation state, audio, preferences and quality | `presentation-core.js` | Frame hook before simulation; successful gameplay events; transient WeakMaps |
| Player and enemy poses | `entity-presentation.js` | Direct entity renderer hooks; modular gear; cosmetic recoil |
| Static room floor and telegraph shapes | `world-presentation.js` | One bounded room canvas; projected danger footprints; foreground fading |
| Spell effect dispatch | `AWPresentation.effectRenderers` | Ability visuals and the content pack register named handlers; no nested effect fallback wrappers |
| HUD, comparison, settings, dialogue portrait and loot labels | `modern-ui.js` | Stable spell nodes; 3–5 slots; semantic menu controls |
| Cosmetic budgets, viewport sizing and existing particles | `performance.js` | Uses presentation quality/preferences; keeps gameplay collections intact |
| Door geometry and entrance spawn safety | `navigation-clarity.js` | Retained; camera projection now has one owner |
| Inventory and learned spell selection | `inventory-ui.js` | Equipment/spells/journal tabs; existing assignment gate and save functions |
| Frame scheduling and crash containment | `runtime-stability.js` | Sole frame owner; independent input, presentation, simulation, HUD and render stages |

Removed duplicate generic cast feedback, the second equipment overlay, independent decal allocation, stale three-slot HUD caching, and overwritten atmosphere/simulation copies. Existing gameplay extension wrappers remain where they implement catalog, combat, progression or save behavior; this is not an engine rewrite.

## Behavior

- All five spells and dodge use the same 150 ms buffer. Requests expire across menus, focus loss, room transitions, loadout changes and player changes. A held key/controller button does not repeatedly cast.
- Maximum movement speed and dodge duration remain unchanged. Analog magnitude now controls movement speed. Facing, cloak, feet, arms and equipment respond to motion, attacks and casts.
- Gamepads use left/right sticks, A/Cross interaction, X/Square, Y/Triangle, B/Circle and shoulders for spells, right trigger dodge, View/Share inventory and Menu/Options pause. Menu navigation uses the D-pad and A/Cross; B/Circle activates available close controls. Nonstandard controllers expose numbered prompts and button remapping.
- Camera follow eases, combat crowds slightly widen the view, and heavy events pulse zoom/shake within caps. Boss introductions suspend the simulation for 1.75 seconds; phase and death feedback are cosmetic.
- Heavy hits use infrequent short hit pauses. Perfect dodge retains its existing gameplay reward and adds an outline, echoes, sound and announcement.
- Temporary impacts, lights, decals and death fragments use bounded reusable pools. Existing projectiles, hazards and telegraphs retain their gameplay ownership.
- Room floor caching works in world coordinates so camera movement does not rebuild static terrain. OffscreenCanvas has a normal canvas fallback. Tall foreground scenery fades near the hero.
- Combat loot appears on the ground and is inspected through the interaction action; shop/crafting comparisons retain their existing flow. Pending loot survives saving and follows the player into the next room rather than being lost.
- Spell icons have elemental silhouettes, cooldown sweeps, tier labels, ready flashes and highlights for actual supported reactions. The minimap hides undiscovered room types.
- Preferences use `arcaneWildsPresentationV1`, independently of local/cloud journey saves. The optional `pendingLoot` save field defaults to null for older saves. Existing spell IDs and mutations are unchanged.

## Validation

Run with Node 24:

```sh
npm ci --prefix game/arcane-wilds --ignore-scripts
npm test --prefix game/arcane-wilds
```

52 checks cover existing cloud/content/progression regression contracts, real input-buffer behavior, gamepad edge/disconnect handling, mapping collisions, pool reuse, whole-script parsing, desktop/mobile DOM initialization, five-slot node stability, settings, loot comparison, old/new save restoration, boss lifecycle and all spell/enemy render paths. The integration harness validates canvas coordinates with a test context; it does not rasterize images or measure GPU performance.

## Mobile freeze and menu follow-up

The regression harness reproduced a stationary world with responsive DOM menus when `navigator.getGamepads()` throws a permissions-policy `SecurityError`. Previously, the presentation frame polled it before simulation; the outer recovery loop repeatedly retried the same exception. The input manager now treats an inaccessible Gamepad API as unavailable for the page session while retaining touch and keyboard input.

A second injected failure in WebAudio node creation could prevent a boss introduction from expiring. Optional audio now shuts down cleanly on device failures. The runtime owns a single frame pipeline and independently contains optional input, animation, HUD and rendering errors, allowing simulation and future frames to continue. Failed animation frames release cosmetic freeze flags. Simulation errors remain separately logged, and gameplay objects are not culled for performance.

Inventory browsing now has Equipment, Spells and Journal tabs, larger mobile cards, a persistent close header, search by name/effect/mutation, equipped filters, explicit 3–5 slot assignment, and mutation details. Assignment uses the existing loadout gate and saves without resetting cooldowns, granting upgrades or discarding replaced learned spells. Evolution and replacement cards use the same elemental icons and readable cooldown labels. The obsolete inventory render copies were removed.

Additional integration checks cover restricted controller access, unavailable audio, optional frame failures, nested menu pause state, spellbook search, slot assignment, retained mutations/cooldowns and save restoration on desktop and touch configurations. These reproduce concrete failure paths; they do not establish which browser exception occurred on the reporting device without its console log.

## Remaining validation and limits

Chromium installation was unavailable in the implementation environment because its download timed out. Real browser visual review, hardware controllers, audio listening, touch ergonomics, fullscreen support, late-run GPU profiling and the requested desktop/mobile FPS targets remain unverified. No HTML previews were generated.

This is a substantial modernization pass, not a claim that every optional item in the design brief is finished. Animation is procedural and family based. Boss presentation reuses existing models and AI phases; there are no bespoke sprite sheets or new boss attack patterns. Music is lightweight synthesized ambience. Optional tap/hold spell charging, cosmetic breakable props, village animals/day-night simulation, complete migration of all legacy gameplay wrappers, and pooling of every legacy particle/text allocation remain outside this pass.
