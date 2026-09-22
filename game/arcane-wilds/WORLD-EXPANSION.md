# World expansion and Shadow Realms

The main journey contains 210 named locations: 56 in the Verdant Reach, 70 in the Shattered Meridian and 84 in the Astral Gloam. There are 16 regions and 24 settlements. The original location IDs, twelve boss seals, three rulers and earned rewards remain compatible with existing saves.

## Travel and exploration

Walking through a cardinal doorway immediately enters the connected destination from its opposite side. Missing roads have no usable exit. Undiscovered destinations say “Unknown Path”; discovered names persist. The ruler arena is reached through the capital's seal-gated portal.

The dedicated Map button and **M** open the planning map. Drag to pan; use the wheel, pinch gesture or buttons to zoom. Smaller markers distinguish location types, and the current location is labeled “YOU ARE HERE.” Hidden treasure nodes require discovery or scouting. Dungeon entrances remember arrival direction; internal rooms rotate consistently, with world roads accessible from the corresponding outer chambers.

Waystones require activation before fast travel. Old saves migrate previously visited settlements into activated waystones. New journeys start with Sunmere's waystone. Optional mount routes supplement the road graph; every required boss remains reachable on foot.

Regions supply their own materials, biome scenery and ambience. Additional settlements are distributed across regions, with merchants, alchemists, smiths, scribes and relic dealers. Resource deposits, rune puzzles, hidden vaults, shrines, mount quests and encounters provide different exploration rewards. Events include rescue objectives, shrine defense, elite hunts, destructible Rift Crystals and enemy waves.

## Regional content

- **40 named equipment items**, with functional combat or exploration effects, explicit regional sources, stock requirements and one-time reward claims. Existing saves receive earned boss/site items retroactively.
- **Verdant Elk:** discover Silverwood, then claim the Elk Sanctuary reward. Forest affinity, resistance to natural slowing and Forest Trails.
- **Stormclaw:** defeat the Storm Knight Regent, then claim Stormclaw Crag. Sustained movement builds a sprint; Storm Paths open.
- **Astral Gryphon:** defeat the Last Sun Warden, then visit its aerie. Flight Routes and an arrival movement boost.
- **20 regional enemies:** six Verdant, seven Meridian and seven Gloam. Behaviors include healing plants, destructible barriers, charging enemies with wall-stun counters, misleading copies, coordinated attacks, dodge-removable leeches, predictive strikes and ally-consuming devourers.
- Restorative and warding draughts, plus regional herb, moonstone, stardust and void-shard trade. The Herbalist's Satchel extends draught durations.

Mounts use the existing travel system. Attacking requires dismounting; casting, dodging or taking damage dismisses the mount. Equipment continues using the normal weapon, armor and trinket slots. Bonus active spell slots are capped at five.

## Shadow Realms

Defeating the Watching Dark opens the Shadow Realms. A portal in Astral Sanctuary begins an expedition. A stored seed generates an endless northern road with side branches, loops, twelve rotating environment themes and corrupted echoes of familiar places.

Depth increases health, damage, speed, encounter density and environmental pressure. Up to four of sixteen traits alter combat: Vampiric, Swift, Armored, Reflective, Explosive, Teleporting, Regenerating, Arcane, Frozen, Burning, Summoner, Berserker, Giant, Tiny, Shielded and Shadowed. Twelve enemy identities are exclusive to the Shadow Realms.

| Milestone | Encounter |
| --- | --- |
| Every 10 depths | Elite Guardian |
| Every 25 depths | Shadow Boss |
| Every 50 depths | Greater Shadow Lord |
| Every 100 depths | Legendary Realm Boss |

Bosses combine established boss patterns with Shadow traits and additional attacks. Boss rewards can activate a waystone. Sanctuaries appear at Depth 1 and after each 25-depth milestone. They offer healing, spell selection, return travel and checkpoint activation.

Branches include treasure, spell shrines, merchants, mount encounters, events and challenges. Greed routes raise enemy health 40% and loot chance/Glory 70%; Safety routes retain ordinary scaling. Events add rescue and defense objectives, crystals, elite hunts, three-wave challenges, timed collapse penalties and escorts that require staying near the merchant. Objective failure reduces rewards; it does not strand the player.

Glory comes from first clears, elites, bosses, discoveries, challenges and new depth records. It is permanent and never spent. The Glory journal records total Glory, highest depth, expedition earnings, milestones, titles and stored rewards.

| Glory | Unlock |
| --- | --- |
| 100 | Shadow Walker title |
| 600 | Rift Hunter title and starlit trail |
| 1,000 | Shadow Fireball |
| 1,800 | Voidbreaker title and Shadow merchants |
| 2,400 | Eclipse Lightning |
| 5,000 | Realm Conqueror title and Eclipse Gryphon |
| 15,000 | The Unending title |
| 40,000 | Lord of Shadows title |

Spell shrines and rare deeper loot can also reveal the two variants. Shadow Fireball leaves a damaging void pool; Eclipse Lightning adds shadow arcs. Both have mutation choices and remain outside ordinary unlearned spell rolls. The complete spell catalog now contains 84 spells.

Five exclusive equipment themes—Shadowsteel Reaver, Vestments of the Unseen, Riftwarden Armor, Eclipse Heart and Atlas of Lost Dimensions—use existing equipment mechanics. Drop rarity, frequency and power rise with depth. Legendary rewards have sound, effects and name announcements. A rare mount branch can earn the Eclipse Gryphon from Depth 25 with 1,800 Glory; it also unlocks through the Glory track and supports Flight Routes.

## Persistence and performance

The existing base save stores the additional campaign fields. Existing encrypted cloud bundles carry these fields without a new service or identity. Death returns to a Shadow checkpoint while keeping Glory, depth records, equipment and unlocks.

- Retain at most 39 nearby procedural node definitions; recreate distant definitions from the seed.
- Keep only the current Shadow room snapshot and at most 48 normal campaign room snapshots. Opened normal caches retain compact claim flags when rooms are evicted.
- Store discovery, clear and claim bits in up to 32 ledger chunks of 16 depths each. Older depths are archived as resolved, preventing repeated rewards without retaining the entire map.
- Keep the entrance and 79 recent Shadow waystones, plus 80 recent boss milestones. Permanent titles/unlocks and highest depth remain recorded separately.
- Preserve one inspected loot item and up to 12 queued Shadow items. Overflow is explicitly salvaged into gold and Void Shards; stored items are not silently overwritten.
- Bound hostile regional zones and reinforcement counts. Simulation timers pause behind menus. Maps build their DOM only when opened.

## Implementation and verification

`world-expansion-data.js` extends the authored geography before the campaign adapter initializes. `regional-content.js` and `regional-enemies.js` implement equipment, mounts and combat behaviors. `world-travel.js` owns cardinal travel, waystones, world objectives and cache retention. `shadow-realms.js` owns generation, progression, scaling and persistence; `shadow-ui.js` provides the portal, journal, sites and compact HUD.

Run `npm test --prefix game/arcane-wilds`. The 98 regression tests cover the full existing suite plus reciprocal geography, required routes without mounts, desktop/touch travel, dungeon junctions, all item sources, mount gates, enemy behaviors, event objectives, Shadow unlocks, boss tiers, spell variants, one-time rewards, save/cloud serialization, death recovery and bounded state beyond Depth 12,000. The runtime harness exercises canvas calls and rejects nonfinite geometry. Physical device frame rates, live Firebase writes and long-term balance are not measured by these automated checks.
