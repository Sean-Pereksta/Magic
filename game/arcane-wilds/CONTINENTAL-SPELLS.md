# Continental spell expansion

Adds the 20 spells from the supplied design plus 10 additional spells, taking the live catalog from 52 to 82. This change covers the spell portion of that design.

| Region | Requested spells | Additional spells |
| --- | --- | --- |
| Verdant Reach | Briar Cage, Wildstep, Sporeburst, Guardian Treant, Predator’s Mark, Emerald Rain | Seed Sentry, Bramble Tether, Pollen Veil |
| Shattered Meridian | Thunderstep, Magnetic Field, Crystal Barricade, Prism Rebound, Avalanche, Storm Beacon, Mirror Walk | Cinder Mine, Hail Orbit, Fulgurite Lance, Ember Recall |
| Astral Gloam | Soul Chain, Umbral Passage, Astral Sentinel, Gravity Inversion, Eclipse, Constellation Spear, Rewind | Soul Harvest, Starweave, Hourglass Mine |

## Discovery and persistence

Town specialists sell regional spells. Spells assigned to named bosses, rulers, dungeons and shrines require those discoveries and do not appear in random level-up choices before ownership. Shop spells can also appear in level-up choices on their continent, subject to the existing rarity level gates. Spell descriptions name their source, and known world-map locations display their spell rewards.

Bonehaven and Emberfall gain Spell Scribes so their spell stock is accessible. Existing saves recover newly introduced rewards from already-cleared bosses/dungeons or already-claimed shrines when a room loads. Ownership, active slots, cooldowns and mutations use the existing save registries. No active-slot limits change.

## Combat behavior

- Briar Cage records boundary crossings, damages entering/exiting foes and restrains light foes trying to leave. Iron Thorns, Living Prison and Bloodroot are separate mutations.
- Wildstep supports a second charge with a shared recharge timer, thorn trails and a speed boost through mutations.
- Sporeburst launches a pod, creates stacking poison and optionally spreads smaller, non-recursive clouds on deaths.
- Treants have health, slow movement, melee attacks, collision and local aggression diversion. Crystal barricades have health and intercept projectile paths. Enemies can destroy either.
- Magnetic Field pulls ranged enemies and weakens each hostile projectile only once. It does not nullify all ranged attacks.
- Prism Rebound ricochets off room boundaries and redirects to unhit enemies, with bounded damage growth and bounce count. Existing scenery is decorative, so it does not add scenery collision.
- Mirror Walk and Ember Recall create moving/stationary decoys that attract local enemies. Emerald Rain heals the player and health-bearing summons and preserves poison duration.
- Soul Chain shares actual health damage once, without recursive sharing. Predator’s Mark amplifies weapon hits, influences summon targeting and refunds cooldown on a kill.
- Umbral Passage prevents attacks/casts and damage until its exit blast. Eclipse affects distant idle enemy tracking, shadow damage and celestial critical flares.
- Constellation Spear uses a 0.8-second visible aiming windup compatible with the existing tap/key cast controls; aim remains adjustable during charging. It does not require a new hold gesture.
- Rewind records health, shield and position. A second tap/key/controller press returns during its window despite the active cooldown. The original cooldown keeps running. Death, room travel and window expiry cancel the anchor; other spell cooldowns are never rewound.

All delayed effects advance with simulation time, stop during pauses and clear on room travel/death. Active custom fields and summons are bounded separately from cosmetic particle budgets. Each new spell has three implemented mutation paths, with additional bespoke Briar Cage, Wildstep and Sporeburst mutations.

## Validation

Run `npm ci --ignore-scripts --prefix game/arcane-wilds` and `npm test --prefix game/arcane-wilds`.

The suite covers all 30 casts with/without mutations in desktop/touch runtime configurations; bounded numeric render geometry; Rewind input, cooldown, expiry and death behavior; barrier interception/destruction; damage sharing; marks; poison/healing; phasing; charges; ricochets; projectile weakening; summon targeting; regional rewards; normal town/shrine flows; and save/load persistence. Rendering tests use the existing JSDOM/canvas harness; physical-device playtesting and balance tuning remain manual.
