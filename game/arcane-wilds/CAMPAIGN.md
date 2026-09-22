# Three-continent campaign

Arcane Wilds now uses a 63-location campaign around the existing room engine. Start or continue from `game/arcane-wilds.html`.

## Journey

| Continent | Threat | Settlements | Ruler |
| --- | --- | --- | --- |
| Verdant Reach | 1–8 | Sunmere Haven, Briarwatch, Greenharbor | The Thornheart Sovereign |
| Shattered Meridian | 9–17 | Prismhold, Stormrest, Emberfall | Vaelith, Tempest Dragon |
| Astral Gloam | 18–27 | Astral Sanctuary, Bonehaven, Redwatch Refuge | The Watching Dark |

Each continent has 21 connected locations, four required regional bosses, a five-room branching dungeon, a Portal City, optional shrines, mounts, landmarks, an event, a merchant camp and onward passage. Ordinary rooms use role-based regional squads. Optional dungeon relic and elite branches are separate from the guardian. Only the guardian is required to clear a dungeon.

Normal travel requires an adjacent destination and a cleared current location. Scouting reveals names without unlocking portals. Town Portals connect only physically visited settlements. Every regional boss must fall before the capital's physical portal can be entered; clicking a ruler on the map cannot bypass it. Defeating a ruler unlocks the next continent's ship/gate passage. Previously visited towns remain reachable.

The 12 regional bosses preserve the existing boss families. Continent rulers use the existing three-phase combat system with native summons, distinct arena layouts and additional themed telegraphs, rather than a second phase scheduler. Regional reward gear queues separately from ordinary pending loot, so simultaneous drops cannot overwrite it. Claim these rewards in Character.

## Towns and exploration

Settlements have different service rosters, local dialogue, residents, costumes, architecture, palettes and ambience. Regional stock is a separate catalog and is never inserted into the generic random merchant pool. Purchases validate location, gold, materials and unlock requirements in the action handler. Prismhold's Mirror Sigil requires the Crystal Vault; Cinderplate requires Emberfall's forge and local ore. Astral Codex grants two bonus spell slots, still capped at five.

Five mounts can be bought or discovered; continent rulers also reward companions. Mounted movement and camera distance change without stamina or feeding systems. Mounted heroes do not auto-attack; casting, dodging or receiving damage dismisses the mount. A terrain affinity supplies a small extra movement bonus.

Six new spells expand the catalog to 52: Grove Guardians, Storm Bridge, Mirror Bastion, Spring Snare, Prism Ricochet and Star Causeway. Each has six mutations. The first three require regional discovery or specialists until learned. The others receive region-weighted discovery chances. The old 46 spells, their signature mutations, reactions and loadouts are retained.

## Controls

- Journal: the menu button or the existing inventory/controller binding.
- Map: map button or **M**. Spellbook: **B**. Mount: **R**. User remaps take priority over these convenience shortcuts.
- Map: pointer drag, touch pinch, wheel or zoom buttons; arrow/WASD directional node navigation; Enter inspects; Escape closes. Travel always uses the inspected destination's explicit button.
- Spellbook: choose a spell, Equip, then a slot. Dragging is optional. Moving an equipped spell swaps slots; replacing a spell or unequipping it never removes learned status or mutations.
- Controller: existing D-pad menu focus, confirm and back. Slots 4–5 continue to require equipment bonuses.
- Dungeons: cardinal doors navigate internal rooms; labels identify the destination. Other exits open the World Map.

Mobile uses separate movement/aim sticks, adaptive 3–5 spell controls, contextual interaction and dodge, safe-area spacing, and a full-screen Character / Spellbook / Inventory / Map journal. Map DOM is built only when shown; the map has no animation loop. Combat simulation and unnecessary canvas rendering pause behind menus.

## Implementation and saves

- `campaign-data.js`: declarative continent, node, town, item and mount catalogs; pure discovery/travel rules; versioned normalization.
- `campaign.js`: one integration adapter for room generation, combat, services, travel, recovery and world rendering.
- `campaign-spells.js`: additions using the existing projectile, summon, hazard and mutation systems. New persistent spell jobs advance in simulation time and clear on room changes.
- `campaign-ui.js` and `campaign.css`: shared journal, map gestures, Spellbook assignment and town interfaces.

The existing base save contains `campaign` alongside its original character/room fields. The encrypted Firebase bundle already carries that base save and the expansion save, so no new cloud document, credentials or identity is created. Legacy saves start at Sunmere with level, XP, gold, spells, mutations, equipment, trinkets, materials and quests intact. Old coordinate rooms remain archived; they do not grant new campaign seals. Starting a new journey resets campaign state. Death restores the last visited settlement and retains discovery, seals and rewards.

Campaign coordinates are internal room-cache addresses, not the travel interface. Non-campaign room APIs remain available for old content and regression coverage. New named regional gear stays out of the random equipment pools.

## Validation

`npm ci --prefix game/arcane-wilds --ignore-scripts`

`npm test --prefix game/arcane-wilds`

70 passing regressions cover the existing game plus campaign graph reachability, adjacent travel, portal gating, complete three-continent progression, internal dungeons, legacy migration, cloud-bundle serialization, recovery, restricted purchases, Spellbook tap assignment/swapping, mounts, menu pause and all six spell casts.

Chromium smoke checks at 320×568, 390×844, 844×390 and 1280×800 verified actual map travel, Spellbook assignment, menu containment, no combat-control intersections and no page/console errors. No HTML previews or image assets are required. Live Firebase writes and physical handset/controller performance were not exercised; cloud payload compatibility was checked locally.
