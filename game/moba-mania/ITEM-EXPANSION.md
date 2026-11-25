# MOBA Mania: epic equipment expansion

**Status: implemented.** The shop has 45 permanent match upgrades: fifteen Basics, fifteen Upgrades, and fifteen Epics. Each item has a distinct code-drawn pixel icon. Existing recipes retain their IDs and components, so old saved inventories continue to work.

## Design goals

An epic should create a visible combat event and a reason to choose a build. Progression items reward personal last hits; status builds reinforce a hero’s existing burn, poison, or slows. All five new paths retain the existing additional-cost system, with ingredients staying active. An epic requires an Upgrade from its path and a separate Basic. A hero may eventually own several paths, but the extra investment delays immediate combat upgrades.

## Fifteen new items

Prices below are additional purchase costs, not a replacement price for earlier ingredients.

| Item | Tier | Gold | Required active items | Bonus from this purchase |
| --- | --- | ---: | --- | --- |
| Apprentice Lens | Basic | 140 | None | +15% personal kill XP |
| Hunter's Journal | Upgrade | 290 | Apprentice Lens | +20% personal kill XP · +8% mana |
| Ascendant Codex | Epic | 660 | Hunter's Journal + Spark Crystal | +25% personal kill XP · +15% ability damage |
| Copper Coin | Basic | 150 | None | +10% final-hit gold |
| Bounty Contract | Upgrade | 320 | Copper Coin | +15% final-hit gold · +4% damage |
| King's Ransom | Epic | 700 | Bounty Contract + Swift Boots | +20% final-hit gold · +12% attack speed |
| Ember Wick | Basic | 120 | None | +15% burn damage/sec |
| Cinder Brazier | Upgrade | 290 | Ember Wick | +25% burn damage/sec · +15% burn duration |
| Inferno Heart | Epic | 650 | Cinder Brazier + Spark Crystal | +35% burn damage/sec · +20% burn duration · +12% ability damage |
| Venom Vial | Basic | 120 | None | +15% poison damage/sec |
| Plague Mask | Upgrade | 290 | Venom Vial | +25% poison damage/sec · +15% poison duration |
| Pandemic Engine | Epic | 650 | Plague Mask + Spark Crystal | +35% poison damage/sec · +20% poison duration · +12% ability damage |
| Rime Shard | Basic | 120 | None | +10% slow strength |
| Frost Compass | Upgrade | 280 | Rime Shard | +15% slow strength · +15% slow duration |
| Winter's Dominion | Epic | 640 | Frost Compass + Blue Flask | +10% slow strength · +20% slow duration · +10% health |

## Complete paths and limits

| Path | Result when all its components are owned |
| --- | --- |
| Apprentice Lens → Hunter’s Journal → Ascendant Codex | +60% XP for your own finishing blows. Hero kills also grant a ward and limited Q/E/R cooldown refunds. |
| Copper Coin → Bounty Contract → King’s Ransom | +45% gold for your own finishing blows, rounded down per reward. Hero kills grant a temporary movement/attack haste burst. |
| Ember Wick → Cinder Brazier → Inferno Heart | +75% burn damage per second and +35% burn duration; a warned blast spreads a smaller burn to up to three other enemies. |
| Venom Vial → Plague Mask → Pandemic Engine | +75% poison damage per second and +35% poison duration; a warned plague burst infects up to three other enemies. |
| Rime Shard → Frost Compass → Winter’s Dominion | +35% slow strength and +35% slow duration; direct abilities trigger a warned frost burst. |

Bonuses add across ingredients and are capped independently. Kill XP applies only to the finishing hero’s existing XP reward; assist XP stays unchanged. Kill gold applies to the existing finishing-blow reward, including a shutdown bounty. No time-based gold, assist gold, shared team income, or extra rewards for merely applying a status are created. Owned summon, projectile, item, and damage-over-time finishing blows retain their owner credit even after that hero dies. A captured river beast has no personal owner and grants no passive income.

Status multipliers apply once to outgoing effects and do not scale initial spell impact damage. Burn and poison gear need a matching source of that status. A +35% slow-strength bonus turns a 30% slow into 40.5%, rather than adding 35 percentage points. Item-enhanced slows stop at 65%; naturally stronger abilities retain their existing value. Stuns, displacement distances, and crowd-control recovery do not gain item duration.

Projectiles and scheduled burn/poison/slow impacts snapshot their item bonuses when released. Later purchases do not retroactively increase those effects. A weaker same-owner burn or poison cannot overwrite a stronger one or extend its stronger damage with a weaker refresh. Spread damage is marked as an item proc and cannot recursively spread, trigger more damaging item procs, or mint more than one kill reward per defeated target.

## Every epic’s combat effect

Level-scaled effects below use the existing equipment progression multiplier, 1 + 0.055 × (hero level − 1). Shields, heals, and damage retain their ordinary combat validation and attribution.

| Epic | Effect |
| --- | --- |
| Sovereign Aegis | **Royal Bulwark:** A meaningful shield wards up to two other allied heroes within 240 for 90 scaled shielding and 15% damage reduction for 2.5s. Recharges in 7s. |
| Worldsplitter | **Earthcleaver:** Every third basic hit strikes its victim for 35% extra attack damage and cleaves a 175-radius cone for 75% attack damage. |
| Starfire Scepter | **Falling Star:** A direct ability hit warns a 135-radius impact, then deals 80 level-scaled damage after 0.45s. Recharges in 5s. |
| Eternity Hourglass | **Time Sequence:** Cast three different abilities within 6s to remove up to 2.5s from other active cooldowns, leaving at least 0.5s. Recharges in 12s. |
| Celestial Font | **Celestial Overflow:** Every third cast restores 10% maximum mana and heals nearby allies for 55 + 4 per level, with a 45 level-scaled ward. Radius 190; 5s recharge. |
| Tempest Wings | **Storm Arrival:** Movement abilities grant 25% movement and 15% attack haste for 3s. Your next basic hit within 4s deals 60 level-scaled bonus damage and slows 25% for 2s. |
| Thunderbolt Crossbow | **Thunder Chain:** Every third basic hit deals 35 level-scaled bonus damage and arcs 55 level-scaled damage to up to three other enemies within 230. |
| Phoenix Crown | **Rekindling:** A meaningful heal wards its hero recipient for 90 level-scaled shielding for 4s and heals the most injured nearby allied hero for 40 level-scaled health. Recharges in 5s. |
| Elderwood Staff | **Elder Guardian:** The first summon of each cast gains 40% health, 25% damage and a 100 level-scaled ward. Guardian attacks pulse 50% of their damage to nearby enemies; shared 3s recharge. |
| Crimson Relic | **Blood Ascendant:** A basic hit on a hero while below 60% health restores 10% health and grants an 85 level-scaled ward for 3s. Recharges in 7s. |
| Ascendant Codex | **Ascension:** Personal kills earn bonus XP. A hero finishing blow grants a 100 level-scaled ward for 4s and refunds up to 2s from Q/E/R, leaving 0.5s. Recharges in 12s; assists get no bonus. |
| King's Ransom | **Victor's Spoils:** Personal finishing blows earn bonus gold, including owned summons and damage over time. A hero finishing blow grants 25% movement and 20% attack haste for 4s; 10s recharge. No assist or passive income. |
| Inferno Heart | **Wildfire:** Your burn damage warns a 135-radius blast that deals 55 level-scaled fire damage and spreads half its DPS for up to 3s to three other enemies. Recharges in 6s; no chain reactions. |
| Pandemic Engine | **Plague Bloom:** Your poison damage warns a 150-radius burst that deals 65 level-scaled damage and infects three other enemies with 60% of its DPS for up to 3s. Recharges in 7s; no chain reactions. |
| Winter's Dominion | **Whiteout:** A direct damaging ability creates a warned 145-radius frost burst: 45 level-scaled damage and a 30% slow for 2.5s. Recharges in 7s. Slow bonuses never extend stuns. |

## Readability, saves, and bots

The shop labels the top tier Epics and shows named passives, recharge conditions, each ingredient, remaining recipe costs, and actual stat comparisons. Relevant personal reward and status bonuses appear in the comparison. Hit feedback, countdown areas, fire/poison/frost impacts, shields, and reward text use the existing bounded visual systems. No additional hosted assets or models are needed.

Item ownership, cooldown timestamps, counters, scheduled impacts, and status data remain plain serializable state. Bots buy through the same prerequisites and personal economy. Dragon prefers its burn path, Ranger its poison path, and Vela its gold path; the two new heroes use appropriate protection, control, and spell builds.

The numbers are initial game-balance values. Automated mechanics checks, rendered frames, and deterministic bot matches establish that the systems function; human matches remain the way to tune feel and competitive balance.
