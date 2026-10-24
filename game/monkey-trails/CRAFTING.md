# Equipment, inventory and the trail forge

The catalog contains **135 equipment definitions**: the original five, the previous forty forge items, the previous **sixty additional items**, and **30 Living World items**. The earlier sixty-item expansion has ten items in each of Common, Uncommon, Rare, Epic, Legendary and Mythic, with 48 elemental items and 12 neutral items. All 105 participate in resource-mixing crafting. Regional victory challenges and final adventure keepsake milestones also give fixed, themed Rare equipment.

Rarity, upgrade rank and affinity are independent. Neutral equipment has no elemental affinity and creates no new monkey element. Affinity influences acquisition and crafting; it does not change the wearer or convert abilities. The new Legendary Leaf heart is named **Canopy Relay Heart** (`canopyHeart`) to distinguish it from the original **Canopy Heart** (`heart`). Existing IDs and collected upgrades remain save-stable.

## Inventory and slots

**Pack → Equipment · Consumables · Materials** opens to an owned-equipment grid. Search matches names, affinities and effect terms. A drawer filters rarity, affinity, purpose, status and owned/complete collection; sorting supports newest, rarity, name and rank. Accessible rarity labels and symbols accompany colors. Tiles group only copies of the same definition, rarity and rank, with quantity, assignment, favorite and New indicators.

An item opens a mobile bottom sheet or desktop side panel with exact bonuses, triggers, activation limits, assignments and acquisition hints. Select a monkey and slot to compare the complete resulting loadout, including power and separate effects gained/lost, then equip, replace or transfer that copy. Search, filters, selection and scroll survive these actions. Upgrade, favorite, protect and unequip are secondary actions.

Standard monkeys have **three generic equipment slots**; Ultras have a fourth **Ultra Slot**, including previously collected Ultras. Any equipment can use any usable slot. Item rarity does not restrict monkey classification. One monkey cannot wear the same named item twice; separate copies can belong to different monkeys. Assignments stay with individuals in the expedition and Sanctuary, and the team limit remains three. Change equipment anywhere outside an active battle or forced replacement, including post-battle recruitment. Increasing maximum HP never refills current HP; removing HP bonuses clamps only when necessary.

## Ingredients and camp advantages

Craft anywhere outside an active battle or forced replacement. Choose whole amounts, inspect all six rarity chances and **View Possible Items**, then craft and reveal one item. The offering needs at least **8 forge value** and at most **60 units**. Forge value describes existing ingredients, rather than an additional currency or charge. Each outcome shows its overall chance from the actual lottery. Results provide **View Item** and **Equip**.

At camp, each nonzero row costs `max(1, ceil(0.8 × offered amount))`: eight fruit cost seven, five essence cost four, and one ingredient still costs one. Camps add **0.10 quality**, capped at 1. Quality, affinity influence and limits use the selected amounts before discounts; all offered amounts must be owned. The discount cannot lower recipe quality. Full-roster arrival healing and existing resource-for-essence trades remain available.

| Ingredient | Value per unit | Quality |
| --- | ---: | ---: |
| Fruit | 1 | 0 |
| Elemental essence | 2 | 0.25 |
| Charm material | 3 | 0.35 |
| Healing Berry | 3 | 0.10 |
| Spark Tonic / Cleanse Balm | 4 | 0.20 |
| Bond Charm | 5 | 0.40 |
| Revival Seed | 10 | 0.55 |
| Trail Ribbon | 6 | 0.40 |
| Treasure Scent / Campfire Seed | 6 | 0.45 |
| Scout's Feather | 8 | 0.65 |
| Equipment fragment, any rarity | 4 | Common 0.45; Uncommon 0.60; Rare 0.75; Epic 0.85; Legendary 0.95; Mythic 1.00 |
| Common species bond material | 4 | 0.35 |
| Uncommon species bond material | 6 | 0.55 |
| Rare species bond material | 9 | 0.80 |
| Ultra species bond material | 12 | 1.00 |

Species materials are spent without removing a monkey. Ordinary fragment discoveries retain the original five equipment IDs.

Resource Lens and Supply Satchel introduce **uncommon crafting materials** in `run.forgeMaterials`, alongside existing fruit, essence, supplies, fragments and species materials. Keys name the base resource, such as `fruit` or `essence:leaf`; forge inputs add `quality:`, for example `quality:essence:leaf`. Their value is `max(base value + 1, ceil(base value × 1.5))`, quality is `max(0.45, base quality + 0.20)`, and affinity influence is retained. Uncommon essence has value 3 and quality 0.45. Materials appear in Pack and the forge and follow the ordinary supplies' expedition lifetime.

## Exact lottery

For offered counts `n`, unit values `v` and qualities `q`:

```text
V = sum(n × v)
R = sum(n × v × q) / V
B = clamp((V − 8) / 72, 0, 1)
Qbase = clamp(0.85 × R + 0.15 × B, 0, 1)
Q = clamp(Qbase + (at camp ? 0.10 : 0), 0, 1)
```

| Rarity | Ideal percentage weight | At Q = 0 | At Q = 1 |
| --- | --- | ---: | ---: |
| Common | `70 − 64Q` | 70% | 6% |
| Uncommon | `23 − 10Q` | 23% | 13% |
| Rare | `5.8 + 23Q` | 5.8% | 28.8% |
| Epic | `1 + 22Q` | 1% | 23% |
| Legendary | `0.18 + 20Q` | 0.18% | 20.18% |
| Mythic | `0.02 + 9Q` | 0.02% | 9.02% |

These weights sum to 100%. Each of the eight affinities **and Neutral** starts with weight 2. Essence adds 3 to its element; a fragment adds 2 to its item's affinity, including Neutral. A species material adds 3 divided between its native elements, or none for an untyped species. Fruit and supplies add none. Four Water essence therefore give Water `14 / 30`, about 46.67% of the ideal affinity share.

An item's ideal weight is its rarity weight times its affinity share, divided equally among items in that rarity/affinity bucket. All buckets are represented. Largest-remainder apportionment with stable tie ordering creates **1,000,000 tickets**; one ticket equals **0.0001 percentage points**. Item probabilities, rarity totals and affinity totals use those same tickets and sum to 100%. One deterministic ticket draw selects the item. Displayed item chances are overall probabilities, not conditional-on-rarity percentages. Previews never write inventory or RNG.

## Copies, upgrades and battle rules

Each copy has its own rank, flags and assignment identity. Acquisition automatically upgrades the highest-ranked eligible available copy below rank 12. **Assigned, protected and favorited copies are excluded.** If no copy is eligible, acquisition creates a rank-1 copy. If eligible copies are all rank 12, a forge result gives fragments. The explicit Upgrade action instead consumes an available, unprotected, unfavorited duplicate at the same rank; the selected copy advances one rank.

Forge salvage returns `max(1, floor(V × 0.4 / 4))` fragments, each worth 4 forge value, using offered value. Value 60 returns six fragments worth 24; at camp sixty fruit cost forty-eight. The receipt states the exact result. Original fragment recipes remain available; direct maximum-rank fragment upgrades reject before charging. Invalid mixes, unavailable resources, blocked phases, invalid ownership and full storage also reject before spending or changing RNG.

The original five items retain linear rank scaling. Equipment percentages and the earlier forge catalog's numeric bonuses use `1 + 0.08 × (rank − 1)`, up to 1.88× at rank 12. Expansion activation counts, discovery/refund intervals and camp reward quantity stay fixed. Stat percentages add within each category. Current battle equipment damage bonuses cap at **+40% per attack**, direct reduction at **30%**, and equipment-generated shield capacity at **30% maximum HP**. Locked loadouts and limited activations survive switches and saved-battle reloads. Triggered effects cannot recursively create equipment chains. Older social combat versions retain their previous formulas.

Power includes complete-loadout stats and a rank-scaled equipment/passive estimate, with base values 2 / 4 / 7 / 11 / 16 / 23 for Common through Mythic. Injuries do not lower that estimate; conditional triggers, abilities and elemental tactics still determine actual outcomes.

## Trail passives

These are rank-1 values. Team exploration/crafting effect families apply once at their strongest eligible value while a wearer belongs to the expedition.

| Item | Effect |
| --- | --- |
| Forager's Pouch | One extra common resource after every 12 ordinary discoveries, from that event's eligible pool. |
| Tracker's Knot | Active wearer's eligible capture chance ×1.05, retaining caps and guaranteed captures. |
| Campwalker Brooch | Wearer's accepted-distance recovery +10%; base 2% becomes 2.2%. |
| Fruitkeeper Clip | Training fruit grants wearer 5% extra training XP. |
| Resource Lens | 8% chance to promote one awarded common ingredient to uncommon crafting quality. |
| Trailfinder Compass | Chosen element gets 10% extra relative weight within the already eligible encounter pool. |
| Supply Satchel | Each newly reached camp grants one region-appropriate uncommon essence. |
| Trailblazer Medal | Ordinary discovery requirements −5%; region unlocks and distance challenges use unchanged meters. |
| Prism Compass | 15% chance to convert one awarded essence to an eligible chosen element, preserving quantity and quality. Choices follow the current directional essence pool. |
| Trailforger's Prism | One common ingredient refunded per five actually spent after camp discounts, rounded down. Essence and higher-quality ingredients are excluded. |

Saved discovery serials, forager progress and camp IDs in `run.equipmentTrail` prevent repeated bonuses after reopening or reloading. Bonus resources are not discoveries. Trailward Standard and Wanderking Crown are the other two neutral items; their effects operate in combat.

## Save contract and verification

Existing ownership and assignments migrate into `meta.equipmentCopies`; `meta.gear` remains the maximum-rank compatibility mirror. Monkey slots retain definition IDs in `equipment` and individual IDs in `equipmentCopies`. `meta.equipmentChoices` stores compass preferences. Ownership, ranks and assignments survive synchronization and expedition restart.

Before rolling, the UI chooses `forge-N`. Permanent `meta.forge` stores its sequence and latest receipt: offering, charged inputs, camp benefits, outcome/copy ID, exact chance, salvage and refunds. Replaying the same latest operation/offering returns the receipt without another payment, refund or roll; changed offerings, old IDs and skipped sequences fail. Older receipts remain valid. Another roll waits for the durable local checkpoint.

`equipment-data.mjs` owns definitions; `equipment-icons.mjs` and `inventory-ui.mjs` provide icons, grid, details and comparisons. `equipment-inventory.mjs` owns copies and assignments; `equipment-effects.mjs` and `equipment-battle.mjs` own scaling and saved combat triggers. `equipment-trail.mjs` handles discovery/camp effects, materials and refunds. `forge.mjs` owns probabilities, payments and receipts. `challenges.mjs` and `regional-adventures.mjs` supply fixed Rare rewards. `core.mjs`, `power.mjs` and `social-combat.mjs` integrate expedition, power and protected online copies.

New duels/raids default to combat version **5**. Existing unversioned/v1/v2/v3/v4 snapshots retain their saved rules. Runtime remains static browser modules and existing Firebase storage, with no Node server. From the repository root:

```text
npm run test:monkey-trails
node --test game/monkey-trails/forge-tests.mjs game/monkey-trails/equipment-trail-tests.mjs game/monkey-trails/equipment-inventory-tests.mjs game/monkey-trails/equipment-battle-tests.mjs game/monkey-trails/inventory-ui-tests.mjs
npm run test:monkey-trails:forge-browser
npm run test:monkey-trails:social-rules
```

The browser suite requires an installed Chromium executable. Node and the Firestore emulator are development verification tools only.

See [LIVING_WORLD.md](LIVING_WORLD.md) for the 30 new equipment triggers, Forge Reserve proceeds and the bounded Forgeheart quality benefit.
