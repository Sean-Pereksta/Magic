# Trail forge, camp bonuses and equipment

On the trail or at an earned camp, open **Pack**, choose whole amounts of resources, inspect the rarity and elemental chances, then forge one random equipment item. Crafting pauses during unresolved encounters. Any supported mixture works once the offering reaches **8 forge value**. Each craft accepts at most **60 offered resource units**. Forge value is a calculation over resources already owned, not a new currency or an additional fee.

**Camps make crafting cheaper and improve its odds.** Each selected ingredient row costs `ceil(0.8 × offered amount)`, with at least one unit charged. For example, offering eight fruit costs seven; offering five essence costs four; offering one material still costs one. The preview names the exact amounts charged. A camp also adds **0.10 quality**, capped at 1, raising Rare and Legendary chances. Value, elemental focus and the offering limits use the selected amounts before the discount, so both bonuses apply to the same planned mixture. All offered resources must be owned even when the camp will charge fewer of them.

Rarer ingredients improve the chances of Rare and Legendary equipment. More essence of an element shifts the equipment toward that element. An item's element describes its affinity; any monkey can equip it in the matching weapon, armor or charm slot. A Fire item can, for example, improve energy abilities regardless of the ability's element when its description says so.

The forge contains the original five items and **40 new items**. Finding an unowned item unlocks tier 1. Finding an owned item improves it by one tier, up to tier 12. A maximum-tier result explicitly returns fragments of that item. Ordinary fruit-based supply recipes remain separate and deterministic.

## Resource contributions

Each selected unit contributes the following value and quality. Quality is a fraction from 0 to 1. Elemental influence is calculated separately below, so a mixture can have both good rarity chances and a strong elemental preference.

| Resource | Value per unit | Quality |
| --- | ---: | ---: |
| Trail fruit | 1 | 0 |
| Any elemental essence | 2 | 0.25 |
| Charm material | 3 | 0.35 |
| Healing Berry | 3 | 0.10 |
| Spark Tonic | 4 | 0.20 |
| Cleanse Balm | 4 | 0.20 |
| Bond Charm | 5 | 0.40 |
| Revival Seed | 10 | 0.55 |
| Trail Ribbon | 6 | 0.40 |
| Treasure Scent | 6 | 0.45 |
| Scout's Feather | 8 | 0.65 |
| Campfire Seed | 6 | 0.45 |

| Equipment fragment rarity | Value per unit | Quality |
| --- | ---: | ---: |
| Common | 4 | 0.45 |
| Uncommon | 4 | 0.60 |
| Rare | 4 | 0.80 |
| Legendary | 4 | 1.00 |

| Species bond material rarity | Value per unit | Quality |
| --- | ---: | ---: |
| Common | 4 | 0.35 |
| Uncommon | 6 | 0.55 |
| Rare | 9 | 0.80 |
| Ultra | 12 | 1.00 |

These inputs use `run.fruit`, `run.essence`, `run.items`, `meta.fragments` and `meta.bondMaterials`. Bond materials are the existing duplicate-conversion resources; forging consumes those materials, never a monkey. Normal discovery fragment drops retain the original five equipment IDs.

## Rarity and elemental odds

For ingredient counts `n`, unit values `v` and unit qualities `q`:

```text
V = sum(n × v)
R = sum(n × v × q) / V
B = clamp((V − 8) / 72, 0, 1)
Qbase = clamp(0.85 × R + 0.15 × B, 0, 1)
Q = clamp(Qbase + (at camp ? 0.10 : 0), 0, 1)
```

`R` is the value-weighted ingredient quality of the offering, before discounts. `B` gives a small benefit to larger offerings, reaching its cap at value 80. Better ingredient quality at the same value increases `Qbase`; increasing the size of the same ingredient blend also increases it until that volume benefit caps. The trail uses `Qbase` directly. A camp adds 0.10, retaining the same upper bound of 1.

The ideal rarity weights are:

| Rarity | Percentage weight | At Q = 0 | At Q = 1 |
| --- | ---: | ---: | ---: |
| Common | `76 − 70Q` | 76% | 6% |
| Uncommon | `20` | 20% | 20% |
| Rare | `3.8 + 44Q` | 3.8% | 47.8% |
| Legendary | `0.2 + 26Q` | 0.2% | 26.2% |

Increasing `Q` raises both Rare and Legendary weights, reduces Common weight and preserves an Uncommon chance. There is always a chance of every item; even the best mix does not guarantee a particular rarity or item.

Every element starts with weight **2**. An essence unit adds **3** to its element. A fragment adds **2** to its item's element. A bond material adds **3**, divided equally between the species' native elements; a species without an element adds no elemental influence. Fruit, charm material and utility supplies add no elemental weight. Divide each element's final weight by all eight weights combined. For example, four Water essence add 12 to Water's starting weight 2; Water then has an ideal `14 / 28 = 50%` elemental share.

The original items count as Common Fire (Sunfang Wraps), Common Water (River-shell Vest), Common Leaf (Canopy Heart), Common Storm (Gale Anklet) and Uncommon Leaf (Forager Satchel). These affinities do not change their original effects.

Each rarity/element pair has at least one item. Items within the same pair share its probability equally. The engine apportions these ideal item weights over **1,000,000 lottery tickets**, using largest remainders and stable tie ordering. One ticket equals **0.0001 percentage points**. The actual displayed item chances and the rarity/element totals all come from these same tickets; each set sums to 100%. Tiny differences from the ideal formulas come only from this explicit ticket rounding. One deterministic draw selects a ticket; there are no hidden rerolls.

## Duplicates, upgrades and recovery

All ingredients are checked before any deduction or random-state change. A valid craft spends the preview's actual charged amounts once, then unlocks, upgrades or salvages the selected item. Invalid counts, missing resources, unknown IDs, more than 60 offered units, insufficient offered value and attempts during unresolved encounters spend nothing. The camp discount rounds each nonzero ingredient row up independently, so small amounts may receive no unit reduction and no row can become free.

New equipment bonuses and effects scale by `1 + 0.08 × (tier − 1)`, reaching **1.88×** at tier 12. The original five items retain their existing linear stat upgrades. Combined effects have caps: round healing 8% maximum health, energy-ability damage +60%, physical damage +45%, lifesteal 25%, round energy 3, round shield 18% maximum health, and elemental damage/resistance 50% for each element. Stat totals and recovery amounts use the shared combat rounding rules.

Physical damage means neutral damage. Energy-ability bonuses require a damaging move with a positive energy cost. Elemental bonuses and resistance average their contributions for mixed-element attacks. Lifesteal uses actual health damage, including the overkill limit. Between-round effects apply to the conscious active combatant; they never revive a monkey or recover a reserve. Shields refresh to the item's shield amount and do not accumulate every round. Passive recovery grants no contribution XP.

A tier-12 duplicate returns:

```text
fragment count = max(1, floor(V × 0.4 / 4))
returned forge value = 4 × fragment count
```

The receipt names the item, fragment count and returned value. The formula uses **offered value**, targeting 40% rounded down to whole fragments, with a minimum of one fragment: value 8 returns one fragment worth 4 (50% of the offering); value 19 returns one worth 4 (about 21.1%); value 60 returns six worth 24 (40%). A camp's lower ingredient cost changes the ratio to the amount actually paid: eight offered fruit cost seven and return four value (about 57.1% paid value), while sixty offered fruit cost forty-eight and return twenty-four value (50%). The exact return varies with rounding and remains below both the offered and charged value, so repeated recycling cannot generate free resource value.

## New equipment catalogue

These are tier-1 values. **Round healing, energy and shields below apply only while active and conscious.** HP means maximum health; energy means maximum energy unless the effect explicitly says recovery.

| Item | Element | Rarity | Slot | Tier-1 effect |
| --- | --- | --- | --- | --- |
| Mosstitch Vest | Leaf | Common | Armor | +5 HP. |
| Sapwell Pendant | Leaf | Uncommon | Charm | +4 HP; heal 1.5% HP between rounds. |
| Briar Knuckles | Leaf | Rare | Weapon | +2 attack; +6% physical damage; +6% Leaf damage. |
| Springheart Brooch | Leaf | Rare | Charm | +1 energy; heal 2.5% HP between rounds. |
| Worldroot Mantle | Leaf | Legendary | Armor | +10 HP; +1 defense; heal 3.5% HP between rounds; 10% less Leaf damage taken. |
| Embergrip Wraps | Fire | Common | Weapon | +1 attack; +4% Fire damage. |
| Kiln-fired Vest | Fire | Uncommon | Armor | +4 HP; +1 defense; 8% less Fire damage taken. |
| Furnace Core | Fire | Rare | Charm | +2 energy; +10% damage with energy abilities. |
| Cinder Spurs | Fire | Rare | Charm | +1 speed; +8% physical damage. |
| Phoenix Wraps | Fire | Legendary | Weapon | +2 attack; +12% damage with energy abilities; heal 2% HP between rounds. |
| Pearl Life-cord | Water | Common | Charm | +6 HP. |
| Streamshell Vest | Water | Uncommon | Armor | +1 defense; 8% less Water damage taken. |
| Tidesong Pendant | Water | Rare | Charm | +2 energy; recover 1 energy between rounds. |
| Siphon-fin Wraps | Water | Rare | Weapon | +1 attack; 6% lifesteal; +6% Water damage. |
| Oceanheart Mail | Water | Legendary | Armor | +8 HP; +1 defense; heal 2.5% HP and refresh a 4%-HP shield between rounds. |
| Static Wrist-wraps | Storm | Common | Weapon | +1 attack; +4% Storm damage. |
| Sky-runner Anklet | Storm | Uncommon | Charm | +2 speed. |
| Dynamo Loop | Storm | Rare | Charm | +2 energy; +10% damage with energy abilities. |
| Thunderfoil Knuckles | Storm | Rare | Weapon | +1 attack; +12% Storm damage; +4% physical damage. |
| Tempest Engine | Storm | Legendary | Charm | +2 speed; +2 energy; recover 1 energy between rounds; +12% damage with energy abilities. |
| Pebbleguard Vest | Earth | Common | Armor | +1 defense; +3 HP. |
| Basalt Clasp | Earth | Uncommon | Charm | +7 HP; refresh a 2.5%-HP shield between rounds. |
| Faultline Knuckles | Earth | Rare | Weapon | +2 attack; +10% physical damage. |
| Geode Wall-vest | Earth | Rare | Armor | +2 defense; 12% less Earth damage taken. |
| Atlas Shell | Earth | Legendary | Armor | +12 HP; +1 defense; refresh a 6%-HP shield between rounds. |
| Rimecoat | Ice | Common | Armor | +5 HP; 4% less Ice damage taken. |
| Frostbite Claws | Ice | Uncommon | Weapon | +1 attack; +8% Ice damage. |
| Glacier Locket | Ice | Rare | Charm | +2 energy; +5 HP. |
| Snowmend Mantle | Ice | Rare | Armor | +6 HP; heal 2.5% HP between rounds. |
| Everfrost Prism | Ice | Legendary | Charm | +1 defense; +2 energy; +12% Ice damage; refresh a 4%-HP shield between rounds. |
| Dawn Beads | Light | Common | Charm | +1 energy. |
| Sunlace Vest | Light | Uncommon | Armor | +6 HP; heal 1.5% HP between rounds. |
| Halo Fist-wraps | Light | Rare | Weapon | +1 attack; +10% damage with energy abilities; +6% Light damage. |
| Radiant Battery | Light | Rare | Charm | +3 energy; recover 1 energy between rounds. |
| Lifegiver Crown | Light | Legendary | Charm | +8 HP; heal 3.5% HP and recover 1 energy between rounds. |
| Dusk Mitts | Shadow | Common | Weapon | +1 attack; +4% Shadow damage. |
| Veilweave Vest | Shadow | Uncommon | Armor | +4 HP; 8% less Shadow damage taken. |
| Nightdrink Fang | Shadow | Rare | Weapon | +1 attack; 7.5% lifesteal. |
| Eclipse Seal | Shadow | Rare | Charm | +2 energy; +10% damage with energy abilities. |
| Moonreaper Wraps | Shadow | Legendary | Weapon | +2 attack; +12% Shadow damage; 6% lifesteal. |

## Module and save contract

`equipment-data.mjs` owns save-stable content IDs and base effects. `forge.mjs` exports `forgeInventory`, pure `previewForge`, `nextForgeOperationId`, `craftForge` and `validateForgeCheckpoint`. Mixes are objects whose keys are `fruit`, `essence:<element>`, `item:<item ID>`, `fragment:<gear ID>` or `bond:<species ID>`, and whose values are whole offered resource counts. `chance` fields are fractions from 0 to 1; `tickets` are integers.

Preview and new receipts retain `inputs` as the offering and `costInputs` as the actual deduction. `costSummary[].count` and `.value` describe the charge; `.offeredCount` and `.offeredValue` preserve the offering. `value` and `offeredUnits` describe the offering; `chargedValue` and `chargedUnits` describe payment. `campBonus` is a boolean, `discount` is 0.2 at camp or 0 elsewhere, `qualityBoost` is 0.10 at camp or 0 elsewhere, `baseQuality` is `Qbase`, and `quality` is the final capped `Q`. The advertised discount and quality boost remain nominal values even when unit rounding or the quality cap reduces their effect.

The UI obtains `forge-N` before the draw. A successful operation stores its sequence and full result in permanent `meta.forge.serial` and `meta.forge.last`. The operation fingerprint uses the normalized **offering**, not its discounted charge. Repeating the latest ID with the same offering returns the saved receipt without another deduction or RNG change, including after reload or moving between trail and camp. Its original discount and odds remain part of the receipt. Reusing that ID with a different offering fails. Older IDs and skipped sequence IDs fail without mutation. Only the latest receipt is retained, bounding save size; the permanent sequence prevents old operations from becoming usable again. The UI waits for its durable device checkpoint before permitting another roll.

`core.validate` accepts existing version-2 saves without `meta.forge`, validates the optional ledger when present and retains the latest receipt. Earlier forge receipts without the new charge/bonus fields remain valid and replay without a new deduction. New receipts validate their offering, actual charged map, per-row costs and camp benefits. No save namespace, currency, equipment slot or original gear ID changes. Gear ownership, fragments and the forge ledger survive expedition restart; fruit, essence and supplies retain their existing expedition lifetime. New protected social battles use equipment-aware combat rules while prior in-progress battles retain their original combat version.

Run `node --test game/monkey-trails/forge-tests.mjs game/monkey-trails/equipment-effects-tests.mjs` for pure odds, inventory, replay, reload, duplicate salvage and combat-effect checks.
