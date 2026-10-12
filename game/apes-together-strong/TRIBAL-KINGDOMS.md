# Tribal Kingdoms, Infiltration and Fortress Warfare

Choose a tribe in the new-game menu. The first twelve nearby opening rescues
use that tribe's six-ape composition, repeated twice; population, captive count
and starting resources stay unchanged. Every tribe can rescue every species.

| Tribe | Permanent bonuses | Opening rescues | Exclusive controls and upgrades |
| --- | --- | --- | --- |
| Ironback | Heavy log shields +20%; heavy fortification damage +15%; gorillas take 10% less ordinary bullet damage; construction −10% | Gorilla, orangutan, gorilla, chimpanzee, orangutan, capuchin | Shieldwall, Battering Assault, Gatehouse |
| Shadowhand | Light climbing +20%; careful movement suspicion −25%; sabotage +20%; reinforced batteries, ballistas and lodge expansions cost +20% | Gibbon, capuchin, chimpanzee, gibbon, chimpanzee, capuchin | Ghost Entry, Silent Relay Cut, Master Distraction |
| Emberwood | Construction +20%; staffed workshop timber +15%; defensive durability +15% | Orangutan, chimpanzee, orangutan, capuchin, mandrill, gorilla | Holdfast Defense, Masterwork Towers, Engineer Reserve |

Existing saves without a tribe remain balanced legacy kingdoms. Tribe identity,
upgrades, elite identities, orders and cooldowns persist. Existing explored
prisons keep their original architecture; new strong prisons use the expansion.

## Army controls

**K** selects climbers; ordinary move/attack orders inside a fortress choose
climbing access automatically. Light apes compare usable climb routes and gates;
heavy troops use gates and breach closed ones. The original destination survives
the crossing. Double perimeters and inner lockdown walls are routed separately,
including return journeys. Blocked routes retry and interrupted climbs land
safely. Discovered access points show route lines, arrows and status text.

The existing advanced army panel adds **Infiltrate**, **Distract**, and a choice
of continue, fight or retreat when discovered. The five-command mobile wheel
stays unchanged. Infiltration persists after a completed route, slows movement,
and suppresses unsolicited combat. Explicit combat/recall orders clear it.

**Ghost Entry** favors covered entrances and orders sabotage of the nearest
known security device. **Silent Relay Cut** arms selected chimpanzees; click a
known radio or alarm to perform the interaction. The non-destructive outage
lasts 30 seconds, with a 40-second individual cooldown. Gate controls instead
open their linked gate and invalidate navigation. Unknown devices remain hidden.

**Distract** previews its noise radius and sends one eligible light ape toward
the chosen reachable point. On approach it attracts up to two nearby guards
(up to four with Master Distraction), capped at one third of eligible listeners.
Wall posts and prison response troops remain assigned. Guards investigate the
noise location, not an unseen King. Repeated nearby distractions raise suspicion.
The normal cooldown is 14 seconds; Master Slingers use nine seconds.

Species, nearby allies, cover, equipment, attacks and careful movement affect
visibility before existing LOS, weather and lighting checks. Detection remains
possible at close range. Shadowhand's suspicion reduction applies to careful
movement independently of those species differences.

**Shieldwall** uses the existing shield formation, with heavy logs in front and
lighter units behind. An intact frontal log can absorb 55% of a nearby ally's
incoming bullet damage, consuming actual shield durability. **Battering Assault**
requires two selected gorillas; for eight seconds coordinated nearby gorillas
receive +35% gate damage, with a 30-second cooldown.

## Village defenses

Friendly spear towers, batteries and ballistas acquire nearby hostiles without
scenery LOS checks. Their arcing projectiles ignore structural obstruction but
retain weapon range, reload, target collision/evasion and projectile budgets.
Threatened villages prioritize crews, including pulling other adult workers.
Empty or disabled towers are labeled. Distant villages keep their stations and
activate full combat simulation for nearby infantry or vehicles.

Every two seconds each settlement assigns adult guardians to towers, gates,
threatened walls, reserves and lodge protection using bounded nearby queries.
Defenders have local engagement leashes; manually recalled or commanded units
remain under player control. Holdfast brings wall defenders inward when enemies
penetrate the settlement. Builders with Engineer Reserve move to damaged
fortifications and spend two wood per 30 durability repaired every four seconds,
only when the village and nearby area are safe.

Visit the lodge to purchase upgrades; choose the particular gate or tower when
several are available. Each target can receive its upgrade once.

| Upgrade | Wood / food | Effect |
| --- | --- | --- |
| Ironback Gatehouse | 100 / 70 | Gate durability +65%; protected by gate assignments |
| Master Distraction | 50 / 70 | Noise radius 330 instead of 240; up to four listeners |
| Masterwork Tower | 100 / 70 | Selected tower durability +30%; reload time ×0.75 |
| Engineer Reserve | 60 / 50 | Enables safe, timber-funded builder repairs |

## Strong prisons and elites

New Blacksite Research Compounds and Transfer Fortresses have two perimeter
rings, separately controlled gates, 64–92-pace seeded service corridors, wall
access, elevated defenders, an internal lockdown section and reserve posts.
Finite barracks waves stage inside the compound. Some guards retain reserve
positions even during a breakout.

Every Blacksite/Transfer cage and every tier-4-or-higher prison cage holds
10–20 normal monkeys with a seeded chance of 0–2 champions. Champions use the
existing primate species, full heavy armor and a hand-drawn flag on their back.
They have 5.5× normal health and 1.8× strike damage; their nonstacking banner aura
grants nearby allies 15% damage and 12% movement speed within 180 paces.

Freed monkeys are individual followers that fight and obey orders immediately.
Escort each survivor beyond the walls for four safe seconds to secure their
rescue. Isolated captives can be recaptured and released again as the same
individuals. Saved actor IDs and award markers prevent duplicate recruits or
rerolled champions. Untouched saved high-level cells adopt the new composition
once; partially opened cells preserve their remaining stock. Legacy crowd
escorts migrate to individual followers; already awarded species-specific
elites remain compatible with older saves.
The first high-security cell still requires both power and cell controls
before opening. Scouting reveals champion counts and lock requirements.

Tribal paint, armor, king crown tiers, champion armor/insignia, banners and
fortification decorations reuse existing art with a bounded 128-entry overlay
cache. Rendering follows committed movement with dead zones and turn smoothing;
combat and shield direction remain independent. The standalone build embeds
all required assets and makes no external requests.

## Validation

Run from the repository root:

```sh
node --test game/apes-together-strong/tests/*.test.cjs
node game/apes-together-strong/build.cjs
node game/apes-together-strong/build.cjs --check
node game/apes-together-strong/tests/tribal-kingdoms-browser.cjs
node game/apes-together-strong/tests/browser-smoke.cjs
node game/apes-together-strong/tests/mobile-commands-browser.cjs
node game/apes-together-strong/tests/visual-overhaul-browser.cjs
node game/apes-together-strong/tests/performance-bench.cjs --scenario HIK --frames 180
```

Browser scripts require Playwright and Chromium and accept `CHROMIUM_PATH`.
Focused simulation coverage includes opening balance, legacy saves, finite
upgrades, stealth, distractions, silent outages, gate interaction, blocked and
interrupted climbs, champion award persistence, garrison recall and remote armor.
Existing prison, mixed-order, spear-defense, save and mobile regressions remain
part of the validation.

The accompanying benchmark JSON compares the same 180 cold-start simulation
ticks against base commit `f4faaeaa1a0cad85f5db32b44691d34f2de889f4` on the same
host, sequentially without concurrent browser/testing load. These are simulation
CPU timings, not browser frame rates. At 1,000 apes and at the large combined-arms
scenario, the pre-existing simulation already exceeds a 16.7 ms frame budget;
this change does not claim 60 FPS at those workloads. AI, navigation, active
projectile, population and overlay limits remain bounded. Human balance tuning
and long campaign play remain useful follow-up beyond the automated scenarios.

Final validation: **631/631 simulation tests passed**, plus tribal offline,
desktop/mobile smoke, mobile wheel, and visual-overhaul browser suites. Build
verification and whitespace checks passed.

| Workload | Baseline mean / p95 | Expansion mean / p95 |
| --- | --- | --- |
| H: 750 moving followers in dense forest | 29.14 / 39.82 ms | 29.59 / 40.25 ms |
| I: 1,000 real apes: 650 followers and 350 distant residents | 31.80 / 43.85 ms | 31.48 / 41.11 ms |
| K: extreme war: 750 apes, 320 humans, 6 tanks, 8 armored vehicles, helicopters and explosives | 44.78 / 52.56 ms | 46.62 / 58.45 ms |

The extreme combined-arms sample adds about 4% mean CPU time; the 1,000-ape
sample is essentially unchanged. Short cold-start samples include run-to-run
variation. Full counters and host details are in
[the benchmark report](benchmarks/tribal-kingdoms-2026-10-12.json).
