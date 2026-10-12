# Tribal Kings, outfitting, and living settlements

The three playable Kings now use separately illustrated body and action sheets.
Ironback, Shadowhand, and Emberwood keep their existing gameplay specializations;
their silhouettes, crowns, armor, lodges, towers, workshops, and palisades differ.
Tribe previews use the same King renderer as the game. Legacy kingdoms receive
the Ironback appearance without acquiring Ironback gameplay bonuses.

## Crafting and progression

Visit the first populated settlement and open the existing building panel to
use Camp Crafting. Completed workshops fit advanced equipment. The panel has
Armor, Weapons, Shields, and Utility tabs; recipients can be selected troops,
a species, a division, village defenders, or the King. Quantities are 1, 5, 10,
or all eligible recipients. Five supplied presets and up to twelve custom
presets combine one item per slot. Availability, stocks, assigned recipients,
equipped counts, remaining queues, and partial affordability are shown.

Each batch reserves real village wood and food once. Rare recipes also reserve
heartwood. Recipients travel to the actual station and complete timed fitting.
The shared queue holds at most sixteen batches of at most 1,000 recipients,
with at most four active stations and one fitting at each station. Combat,
recovery, destroyed stations, and new army or recall orders pause fitting.
Cancellation returns unused supplies, with work already performed deducted.

| Body armor | Unlock | Damage reduction | Movement factor |
| --- | --- | ---: | ---: |
| Vine-Wrapped Padding | First camp | 4% | 1.00 |
| Leafweave Vest | First camp | 6% | 1.00 |
| Basic Bark Armor | First camp | 9% | 0.98 |
| Hardwood Chestguard | 25 apes | 13% | 0.97 |
| Reinforced Shoulder Plates | 40 apes | 15% | 0.96 |
| Shell-Plate Harness | 60 apes, workshop | 18% | 0.93 |
| Resinwood Battle Armor | 80 apes, workshop | 17% | 0.99 |
| Ironwood Warplate | 100 apes, workshop | 24% | 0.88 |
| Royal Heavy Battle Armor | 200 apes, workshop | 28% | 0.84 |
| Elite Champion Armor | Rare heartwood, workshop | 30% | 0.88 |

Heavy armor also increases detectability and slows climbing. Body and mantle
protection combines multiplicatively and caps at 33%; species, tribe, shields,
and existing champion rules still apply. Weapons retain species restrictions.
Spears extend melee reach, slings improve capuchin ranged attacks, mauls damage
gates, cutters accelerate device sabotage, and climbing gear changes real
traversal time. Carry rigs change actual expedition capacity; builder and
lumber gear change existing work rates. Medical packs use real food, with up
to eight medics serviced every four seconds and no healing near enemy forces.
Broken shields stop adding their new equipment weight and can be refitted.

Woven Royal Guard (185 HP) and Timber Crown Harness (210 HP) precede the existing
240/350/500-HP royal upgrades. Fitting preserves the King's current HP deficit.
Advanced royal tiers and their existing bonuses are unchanged. Existing gear,
shield durability, champions, divisions, orders, and workshop jobs survive
loading. Version-2 outfit slots, custom presets, material balances, and batch
progress are saved separately from legacy equipment. Existing torch ownership
is preserved while active torch lights remain capped at twenty-four.

The first successful escort from each Blacksite or Transfer Fortress awards
four heartwood and a rescued ape wearing the player's tribal rare armor.
Facility reward markers survive saves and prevent repeated rewards. Rare armor,
ancient shields, royal mantles, elite shoulders, and siege gauntlets provide
specialist choices within the existing combat caps.

## Artwork and bounded rendering

Eleven original source sheets are retained under `assets/visual/outfits/source`.
Runtime sheets use lossless WebP. The manifests contain 384 King/worker body
frames, 48 species armor components, 30 royal armor components, 18 architectural
components, and 24 shield/tool components. Source prompts, crop rules, frame
anchors, attachment landmarks, and hashes are checked in. Ground support is
measured from opaque pixels; hand/body landmarks are authored for the frames.
Six older environment sheets are also packaged as lossless WebP, preserving
their original PNG masters and all visible RGBA pixels.

Five authored King facings plus validated mirroring cover eight displayed
directions. Independent tribal sheets include travel, sprint, attack, climbing,
carrying, hurt, command, victory, and collapse poses; a separate rear-action
sheet completes Ironback and Shadowhand's straight-back actions. Five royal
armor tiers change each tribe's silhouette. Followers retain all six species
bodies and use four species-specific armor grades, forearm/head protection,
shared held items, and front/rear layering with hand and face occlusion.

Complete repeated body/armor/item poses are cached as immutable ImageBitmaps
where supported, with a Canvas fallback. The full-loadout cache is bounded by
1,024 entries / 16 million pixels in High and 512 / 10 million in Low. A smaller
body-only fallback cache holds at most 160 entries / 2.2 million pixels.
Crowds use native-density composites and retain visible armor silhouettes.
After an appearance's first pose, up to eight additional ordinary movement
poses are composed per frame; the existing complete pose is reused briefly
while the cache warms. Kings, champions, combat, climbing, and workers bypass
that movement-pose budget. No actor simulation is skipped by this cache.

Workers use species-specific front, side, and rear sheets for chopping,
delivery, beam placement, hammering, gathering, cooking, and sawing. Work poses
stop when traveling or interrupted; tools attach to the working hands.
Construction stages and actual resource accounting remain authoritative.
Harvested trees rotate around their bases, reveal logs, and leave the original
stumps. Tree falls are capped at eight and worker impact accents at eighteen
per frame; Low and reduced motion omit these transient effects. Existing
visual-facing interpolation is also used for the King and equipped followers,
while work facing points at the actual job without changing combat direction.

Palisade segments use tribal timber illustrations within the existing barrier
cache and footprint. Intact, damaged, and open-gate artwork follows real state.
Lodges, towers, and workshops retain their construction, health, and interaction
logic. Missing artwork falls back through the existing rendering layers.

## Validation and limits

Validation passed: 644 Node tests; offline outfit, equipment, visual settings
and fallback, tribal opening, held-item, and touchscreen browser regressions.
Pixel checks distinguish all twelve King/armor previews and every follower
armor grade. Browser coverage draws 312 King state/direction combinations,
576 equipped follower poses, and 48 worker poses; all eleven new atlases decode.
The standalone build is 102.52 MB with no external requests.

See `benchmarks/visual-equipment-2026-10-12.json` for measurements and methodology.
The benchmark uses the production Canvas renderer and all decoded assets,
fixed graphics settings, unchanged actor counts, and deterministic scenarios.
Cold-start work is included. Mixed-equipment movement penalties are neutralized
only in the comparison fixture so the two versions execute equivalent movement;
gameplay tests cover the real penalties separately.


| High scenario | Mean before → after (ms) | Change | p95 before → after (ms) | Change |
| --- | ---: | ---: | ---: | ---: |
| 250 working residents | 61.81 → 63.33 | +2.46% | 81.80 → 84.60 | +3.42% |
| 250 residents + 120 commissions | 69.11 → 66.78 | -3.37% | 87.50 → 85.00 | -2.86% |
| 500 mixed-equipment apes | 63.84 → 61.11 | -4.28% | 76.70 → 72.40 | -5.61% |
| 750-ape combined-arms battle | 192.84 → 196.39 | +1.84% | 249.90 → 241.90 | -3.20% |
| 300-ape fortress siege | 108.67 → 108.52 | -0.14% | 129.90 → 134.10 | +3.23% |

All five High comparisons meet the roughly +3% mean / +5% p95 targets.
Low village medians use three fresh-process pairs: working residents
55.41 → 55.04 ms mean and 83.9 → 82.6 ms p95; commissioning residents
61.71 → 60.68 ms mean and 81.3 → 83.4 ms p95. The mixed army improves
in Low, and the siege changes by +2.73% mean / −0.19% p95. The extreme
Low repeat measures 190.72 ms mean / 235.9 ms p95, also below the original
stable baseline. Both versions showed occasional large timing spikes on this
host; the report retains those sweeps and explains every repeat selection.

These are authored key-pose animations with mirroring and modular armor, not
individually painted frames for every equipment combination. Several recipes
share an armor grade while retaining distinct gameplay effects. Headless
Chromium timings describe this test machine, not a mobile-device frame-rate
guarantee. The existing extreme combined-arms workload remains expensive.
