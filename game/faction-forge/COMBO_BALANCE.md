# Combo balance validation

The benchmark executes the production browser combat functions in the authoritative simulator. It uses fixed seeds, mirrored seats, ranked Forge normalization, and Rank I, II and III armies. Every army spends six recruitment cards; spawn counts and large-unit footprints are preserved. This tests actual damage, summons, cooldowns and healing rather than a separate approximation.

Run `node game/faction-forge/combo-benchmark.cjs 3 --json` for the full matrix. `npm run test:faction-forge` includes all combo/path mechanics, deterministic replays, 128 production battles covering every specialization at both upgraded ranks, and explicit Rocket Battery counterplay checks.

## Observations and changes

The initial six-build, three-seed matrix contained 270 battles. Arcane Fortress won all 90 of its matchups across ranks; Guided Artillery lost all 30 starting-rank matches. A source ablation that disabled both ward healing and ward shooting still left the initial Fortress winning every starting-rank opponent in a smaller mirrored probe. The imbalance therefore involved the existing troop economy as well as the new combo rules: Blade and Phaser recruitment each creates two high-damage bodies, and Guardian plus Golem provides substantial frontline health and ranged mitigation.

The resulting adjustments are explicit and limited:

| Troop / mechanic | Previous | Updated |
| --- | --- | --- |
| Blademasters, each of two bodies | 14 HP, 6 damage, 700 base cooldown | 12 HP, 4 damage, 850 base cooldown |
| Phasers, each of two bodies | 6 HP, 5 damage, 1100 base cooldown | 6 HP, 4 damage, 1250 base cooldown |
| Rockets | 8 HP, 6 damage, 2100 base cooldown | 11 HP, 8 damage, 1750 base cooldown |
| BloodBurst | 3 damage; no starting splash flag | 6 damage; starting splash enabled |
| Golem ranged mitigation | 2 | 1 |
| Ward healing | Multiple wards could heal the same recipient together | One ward heal per recipient per 2500 ms |

Cooldown values are before the existing global battle pace multiplier. The Rocket adjustment also compensates for correcting the old splash path that damaged the primary target twice. Rocket splash now excludes the primary target, and each primary or secondary impact still applies damage exactly once.

Signal Hawks prefer reachable support and artillery targets, so precision units can attack the engines behind defensive formations. Rocket impacts can damage enemy wards inside their actual one-tile impact area. A six-card **Rocket Battery** (`Vanir Guard`, `Shield Captain`, two `Rockets`, `Signal Hawk`, `Field Medic`) measures concentration of artillery against a magical fortress. **Assassin Swarm** (two `Vipers`, `Burrower`, `Hell Spawn`, `Brood Matron`, `Hive Herald`) tests the backline pressure that can punish that artillery investment. Both are included alongside the six general builds.

The counterplay regressions verify that Rocket Battery can defeat Fortress at starting and Legendary ranks, and that Rank II Assassin Swarm and Rocket Battery each win some of their six mirrored matchups. The latter observed result is three wins for each army using the committed benchmark seeds. This is useful evidence of counterplay, rather than an assumption based on role labels.

## Final production-engine sample

The frozen combat source completed **504 battles**: eight builds, three seeds, mirrored seats, and three battle ranks. Each build played 42 opponents at each rank. Runtime for this local benchmark was 54.5 seconds. The complete pairwise results, combo activation counts, peak populations and durations are saved in [`combo-balance-sample.json`](combo-balance-sample.json).

| Build | Rank I wins | Rank II wins | Rank III wins |
| --- | ---: | ---: | ---: |
| Static Tempest | 14 / 42 | 14 / 42 | 20 / 42 |
| Arcane Fortress | 31 / 42 | 25 / 42 | 32 / 42 |
| Blood Ambush | 11 / 42 | 10 / 42 | 6 / 42 |
| Brood Invasion | 15 / 42 | 13 / 42 | 9 / 42 |
| Guided Artillery | 8 / 42 | 20 / 42 | 20 / 42 |
| Precision Division | 27 / 42 | 20 / 42 | 30 / 42 |
| Rocket Battery | 35 / 42 | 39 / 42 | 38 / 42 |
| Assassin Swarm | 27 / 42 | 27 / 42 | 13 / 42 |

Every sampled build has wins and losses at every rank. Rocket Battery remains especially strong in this sample and deserves further tuning with additional opponents, draft orders and specialization paths; its three Rank II losses are the observed Assassin Swarm counter. The lower Helions results at Legendary rank also warrant broader follow-up testing. These figures establish bounded, functional counterplay and show a substantial improvement over the initial all-winning Fortress and all-losing starting artillery. They do not establish equal faction strength or universal balance.

## Safeguards

Draft recruitment has its own 65% category budget, separate from direct rank upgrades (15%), specializations (10%), combo enhancements (6%) and legacy attachments (4%). Every normal pack includes at least one actual troop, and combo-completing recruits count toward recruitment. Seeded distribution regressions sample 3,000 packs per faction with complete Rank I armies and require 65–70% actual troop choices. Additional empty, partial and fully upgraded roster samples verify that troops remain the majority, unavailable cards never appear, positions vary, and browser/server seeded packs agree. This changes draft availability; the fixed-army combat results above remain the same historical sample.

Basic combinations work at Rank I. All upgrades are in-battle choices; permanent Forge tiers remain normalized in ranked simulations. A combo enhancement can promote an assembled Basic combo to Advanced; Legendary combinations require a Rank III partner and an upgraded complementary partner.

Secondary attack damage shares a single budget of 8 HP at Ranks I–II and 12 HP at Rank III, even when a supporting caster or Railbreaker is the actual damage source. Individual secondary hits cap at 5, radius at two tiles, and targets at five. Cooldowns apply per side and combo; Blood Chain uses a once-per-body detonation budget and never forces another unit to detonate or recursively invokes an attack.

Combo fields cap at six per side, expire after at most three short ticks, and create no new damage from visual frames. Additional control caps at 450 ms with a per-target recovery window. Swarm spawns use the existing per-parent, per-side and total population caps. Healing caps at three per activation, respects wound, and cannot exceed maximum health. Defensive shields cap at four HP and expire.

## Scope of the evidence

The committed matrix is a reproducible regression sample, not a claim that every army, placement, specialization, draft order, campaign mission or player strategy is equally strong. Fortress investment remains effective against general mixed armies; the counterplay sample specifically tests concentrated rockets and flanking swarms. Future balance work should extend the seeds and test alternative specialization paths rather than infer universal win rates from these fixed compositions.
