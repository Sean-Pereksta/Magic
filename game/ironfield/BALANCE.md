# IRONFIELD 2.5.1 — movement, armor, and counter reference

All values are fictional game balance. This reference uses the same troop and weapon data as the simulation and in-game field manual. A listed advantage describes a useful situation, not a guaranteed duel or equal-cost victory.

## Reading armor and damage

Every troop has an armor class, armor points, and a weapon profile. Armor penetration ignores a fraction of the defender’s armor points. The weapon-versus-class factor multiplies the damage that remains after armor-point protection, including the ordinary minimum-damage floor. Thus light and heavy targets can still take different damage from a weak hit. Ranged attacks retain their separate pacing adjustment.

Formation, cohesion, shields, facing, charge, cover, elevation, and ordinary damage variation also apply. The base mounted bonus is additional raw attack damage against mounted targets; it is not a percentage and is not the full prepared-brace bonus. It applies to horses and the game’s mounted beasts and leaders. Charge defense depends on actual preparation and direction.

**Light armor:** Cloth, leather, or limited protection. Arrows, sling stones, and fast cutting weapons are effective.
**Medium armor:** Mail, bronze protection, or reinforced equipment. A balance of protection and mobility.
**Heavy armor:** Plate, substantial barding, or a large armored body. Piercing bolts and poleaxes gain an advantage.

## Complete armor matrix

Each damage column is the attacking troop’s multiplier against that defending armor class. “Pierce” is the fraction of armor points ignored. These are separate mechanics.

| Attacking troop | Own armor | Armor points | Weapon profile | Vs light | Vs medium | Vs heavy | Pierce | Base mounted bonus |
| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| Levy Spearmen | Light | 1 | Spear thrust | ×1.08 | ×1.00 | ×0.90 | 0% | +4 |
| Shield Swordsmen | Medium | 5 | Cutting blade | ×1.08 | ×1.00 | ×0.88 | 0% | — |
| Pike Guard | Light | 3 | Long pike | ×1.04 | ×1.00 | ×0.92 | 0% | +24 |
| Bronze Hoplites | Medium | 6 | Spear thrust | ×1.08 | ×1.00 | ×0.90 | 12% | +10 |
| Crown Halberdiers | Medium | 7 | Piercing poleaxe | ×1.00 | ×1.08 | ×1.18 | 60% | +17 |
| Iron Guardians | Heavy | 12 | Cutting blade | ×1.08 | ×1.00 | ×0.88 | 0% | — |
| Berserkers | Light | 1 | War club | ×1.10 | ×1.04 | ×0.95 | 30% | — |
| Longbow Rangers | Light | 1 | Arrow | ×1.15 | ×0.98 | ×0.82 | 0% | — |
| Musketeers | Light | 2 | Musket ball | ×1.00 | ×1.08 | ×1.16 | 85% | — |
| Arbalest Company | Medium | 4 | Crossbow bolt | ×1.00 | ×1.08 | ×1.18 | 80% | — |
| Javelin Skirmishers | Light | 2 | Throwing spear | ×1.10 | ×1.04 | ×0.96 | 22% | — |
| Levy Slingers | Light | 0 | Sling stone | ×1.12 | ×0.95 | ×0.78 | 5% | — |
| Royal Cavalry | Medium | 7 | Cutting blade | ×1.08 | ×1.00 | ×0.88 | 0% | — |
| Banner Lancers | Heavy | 8 | Cavalry lance | ×1.08 | ×1.06 | ×0.94 | 0% | — |
| Steppe Outriders | Light | 2 | Arrow | ×1.15 | ×0.98 | ×0.82 | 0% | — |
| Field Cannons | Medium | 6 | Round shot | ×1.10 | ×1.10 | ×1.16 | 85% | — |
| Counterweight Trebuchets | Medium | 4 | Siege stone | ×1.00 | ×1.04 | ×1.08 | 85% | — |
| Stonebreak Catapults | Medium | 5 | Siege stone | ×1.00 | ×1.04 | ×1.08 | 45% | — |
| Ironwind Ballistae | Medium | 6 | Ballista bolt | ×1.00 | ×1.10 | ×1.18 | 95% | — |
| Assault Siege Tower | Heavy | 9 | Arrow | ×1.15 | ×0.98 | ×0.82 | 0% | — |
| Covered Battering Ram | Heavy | 9 | Crushing impact | ×1.12 | ×1.06 | ×0.96 | 25% | — |
| War Elephants | Heavy | 8 | Crushing impact | ×1.12 | ×1.06 | ×0.96 | 0% | — |
| Field Commander | Heavy | 12 | Cutting blade | ×1.08 | ×1.00 | ×0.88 | 15% | — |

## Weapons and reloads

| Weapon | Base range | Base damage | Reload | Hit area |
| --- | ---: | ---: | ---: | --- |
| War club | 31 | 34 | 1.65 s nominal | 144-degree forward sweep, 42 units from the attacker. Secondary hits use 68% damage. |
| Musket ball | 425 | 51 | 12 s | One enemy in the shot path. Push follows the direction of the ball. |
| Cannonball | 920 | 160 | 22 s | 64-unit blast around first impact. Lighter targets fly higher. |
| Trebuchet boulder | 1,450 | 320 | 24 s | 112-unit impact area. The high arc can clear intervening walls. |

Range and blast radii are in world units. Damage lists the weapon input before armor, shields, distance falloff, formation and other combat modifiers.

## Using prepared pikes

Pikes are the specialist frontal cavalry counter. A cohesive Battle line or Shield wall must settle and face the approaching mounted troops. Correctly set pikes gain mounted damage, can interrupt a charge at spear reach, and reduce a frontal charge’s effect. Moving, turning, charging, a loose or wedge formation, weak cohesion, and flank attacks reduce or remove the prepared defense. Terrain disruption also weakens preparation. Ordinary anti-mounted damage remains useful, but does not make an unprepared or surrounded pike formation invulnerable.

Facing comes from the regiment’s formation direction. An individual turning its head to strike a nearby enemy does not instantly aim the whole brace at that enemy. Use Face / T, allow the turn to finish, and hold the approach you want to defend. Longbows, javelins, and mounted archers can attack from outside pike reach; foot infantry avoid the mounted-damage bonus.

## Using hoplite ranks and shield pressure

Bronze Hoplites use medium armor and frontal shields. Cohesive line and shield-wall formations retain their ranks more firmly during contact. Supported hoplites can press lighter enemy bodies backward through real movement. Resistance, occupied ground, hard terrain, boundaries, and heavy bodies bound the displacement; the mechanic does not teleport soldiers or remove collision.

Physical pressure is conditional on formation, cohesion, support, contact, and facing. Loose or disrupted formations lose much of the advantage. Exposed sides lose frontal shield protection. Penetrating bolts, poleaxes, and bombardment provide useful answers. Move, Face, and Retreat remain authoritative: formation discipline and shield pressure do not keep a soldier attacking against a fresh movement order.

### Steady Advance and a walking brace

Steady Advance lets coherent Battle lines and Shield walls retain a partial frontal brace while their bodies actually walk forward together. The maximum moving strength is **0.58 for pikes, 0.52 for hoplites, and 0.40 for halberdiers**, compared with a fully settled brace of 1. Ordinary infantry can receive a **0.24 guard strength** that adds resistance to frontal melee and mounted push; this is a strength coefficient, not a 24% damage reduction, and it adds no pike damage or retaliation. Cohesion, formation facing, actual forward velocity, alignment, and settled preparation still scale the result. Turning, sprinting, sideways movement, crossing traffic, bridge compression, terrain disruption, knockback, or a fresh direct movement order removes the walking protection. Exposed flanks receive no frontal benefit. A planted, prepared polearm formation remains the stronger charge defense.

## Mounted movement speeds

| Troop | Previous base speed (2.2.1) | Current base speed (2.3.1) | Sustained open-ground Move pace in the release fixture |
| --- | ---: | ---: | ---: |
| Royal Cavalry | 92 | 150 | 132.00 |
| Banner Lancers | 87 | 132 | 116.16 |
| Steppe Outriders | 105 | 162 | 142.56 |
| Field Commander | 65 | 150 | 132.00 |

Speeds are world units per second. The movement fixture measures actual formation bodies after acceleration, not just the data constants. Lancers remain slower than the other horse troops and exceed twice the fastest infantry's measured sustained pace. Infantry, elephants, and siege equipment keep their existing speeds. Terrain, formation pace, turns, collision, and commands still affect real travel. Mounted commanders use their existing movement behavior with the higher base speed; the three cavalry types retain their acceleration controller.

## Friendly mounted clearance

Banner Lancers, Royal Cavalry, and Steppe Outriders can take bounded lateral courtesy steps when another friendly horse is approaching their occupied lane. Same-direction moving riders cooperate; held riders can yield a small sideways distance while keeping their facing and shifted individual hold position. Face and initial turn phases retain priority. Infantry, elephants, commanders, and siege equipment do not receive this mounted courtesy movement.

Clearance is committed before the requesting rider's ordinary collision pass. Each displaced body must have real free space and terrain clearance, and its lateral movement shares the existing per-step allowance with voluntary movement and enemy impacts. Temporary moving-slot offsets keep riders from immediately stepping back under the following file; they recover gradually once the nearby passage is clear. Layout captures absorb an existing offset once. Fresh commands discard old sender and recipient authority.

Assigned crossing lanes take priority over temporary courtesy offsets. Shock cavalry already deployed near a hostile front preserves its supporting combat files: the contact controller owns those positions through the plunge. The local melee guard also checks nearby enemies outside the regiment's selected target.

A chained sidestep cannot push a moving recipient backward against its own accepted course. This matters when nearby riders have slightly different directions: a sideways step for one must not countermand another's forward order.

Free, marching Charge cohorts advance their actual front riders before their rear files. This changes movement sequencing while preserving body and terrain collision; there is no assumed future vacancy. Near hostile contact, the existing alternating combat traversal resumes. Dense captured formations delay their combat-slot expansion until the actual opposing front is near, while open deployments keep their established preparation distance.

Mounted Charge guides use the same nominal pace as their riders; actual body progress and the existing rear-rank, lead-distance, terrain, and turn limits still determine the formation's advance.

A friendly brush can preserve drive effort only while a real gap is opening and the rider still makes forward progress. Actual forward displacement continues to determine mounted speed and charge power. A courtesy step changes neither facing nor impact energy; hostile contact, bracing, a wall, or a hard stop still removes momentum. Base troop speeds, acceleration rate, armor, weapon damage, and prepared-counter rules are unchanged from 2.3.0.

## Sustained whole-formation pressure

Connected companies immediately behind the front add supporting mass over the ranks they physically touch. Additional layers contribute less force. A gap between companies breaks that link. Pressure begins after sustained contact and builds toward the local limit. A held heavy reserve can brace the line against displacement.

A regiment remains eligible to recover until it has lost at least 80% of its troops. A devastated regiment can rout below 12 morale under fire or in melee. During an unsupported fallback it can rout below 8 morale after 45 seconds. Giving ground and an ordered withdrawal do not by themselves cause a rout.


An engaged force can gradually make a weaker connected enemy line give ground. Effective pressure uses actual nearby contacts, supporting ranks, mass, troop quality, cohesion, and formation discipline. A correctly facing brace increases resistance. Firm hoplite ranks transfer pressure efficiently; loose or disordered troops lose much of that ability. Ranged formations can be driven back but do not exert an invisible force through their missiles.

Pressure builds through sustained contact and is capped at **8 world units per second**, or **16% of the receiving troop's base speed** when that is lower. Rear ranks make room first. Every displacement uses the normal body and terrain constraints; no force can bypass a rock, occupied rear space, or a battlefield boundary. Disconnected stragglers are excluded from the connected group. Cavalry's short impact impulse remains separate from this slower sustained effect.

Actual body movement carries the relevant formation and hold anchors backward. Troops keep facing the threat and can fight as they yield; ordinary combat positioning cannot immediately reclaim the ground they conceded in the same step. A **GIVING GROUND** banner and distance-driven backsteps or sidesteps show real displacement. No displacement means no such status or animation.

Fresh Move, Face, Retreat, formation changes, splits, routing, recovery, and tactical phase changes invalidate the old pressure. A source given Hold stops its active push. Defenders holding position can still yield to an overpowering enemy. Both the force source and the receiver must remain valid and in real contact; the effect cannot persist on a stale command or on soldiers who have left the fight.

## Mounted momentum and rank resistance

Cavalry and lancers build their charge from actual speed. Acceleration is **0.62 times the type's normal speed per second**. Sharp turns, obstructed travel, and physical contacts remove speed. Closing-speed squared controls contact damage; backing mass, body mass, brace strength, and terrain control penetration and displacement. A **1.1-second per-rider/target contact cooldown** prevents continuous touching from applying free impact damage. For physical body contacts, closing speed is normalized against a shared 100-unit speed reference (or the type's base speed if lower), and the squared power remains capped at 1.44. Increasing top speed therefore does not weaken a contact at the same real speed. An explicit Charge keeps its committed approach through small file corrections when the rider is already aligned with it.

Unsupported light infantry may be briefly launched. Their horizontal movement still uses ordinary body collision and terrain clearance. An airborne victim loses **30% less horizontal impulse to drag**. Any detected rear support prevents the launch and instead transmits diminished ground pressure to supporting ranks. Formed hoplites are harder to move; prepared pikes can remove most of a charging horse's momentum before body contact. A flank, a shallow screen, or a disrupted line gives cavalry more room to penetrate than a deep frontal brace.

Horse archers accelerate but keep their missile role. They do not gain the cavalry/lancer plunge. Towers and rams cannot be launched or pushed by cavalry.

### Stronger penetration against weak ranks

Low-mass light foot troops receive an additional yielding response when hit by shock cavalry. Lancers receive the full mass-based factor; Royal Cavalry receives **90%** of it. Troops with a formal brace, mounted troops, siege equipment, and medium or heavy armor are excluded from this light-body bonus. Steady Advance's ordinary infantry guard increases physical resistance and reduces yielding while its movement and frontal-alignment conditions hold. Existing facing and cohesion rules continue to control polearm and shield resistance.

The response scales with physical closing speed. It strengthens the push, provides a limited outward deflection to clear the horse's corridor, extends recoil, and reduces the horse's momentum loss against weak bodies. In 2.2.0 more of that impulse acts forward, allowing riders to drive deeper through cleared space. Supporting ranks still contribute mass, and each transferred pulse weakens through its chain. Pressure can propagate through at most four rear links, sharing among at most three contacted bodies at each link. A rider that physically reaches the next rank can deliver a new direct impact there.

| Mounted pressure parameter | 2.1.1 | 2.2.0 |
| --- | ---: | ---: |
| Royal Cavalry share of the light-body yielding factor | 55% | 90% |
| Initial push-speed cap, relative to defender walking speed | 1.55× | 1.68× |
| Outward spill coefficient, multiplied by yielding | 0.85 | 0.65 |
| Yielding coefficient in the impulse decay rate | 3.00 | 3.60 |
| Extra lancer recoil duration, seconds multiplied by yielding | 0.65 | 1.40 |
| Extra Royal Cavalry recoil duration, seconds multiplied by yielding | 0.65 | 0.65 |
| Lancer forward carry after the last qualifying nearby target | 1.60 seconds | 2.50 seconds |
| Royal Cavalry forward carry after the last qualifying nearby target | 0.95 seconds | 1.75 seconds |

The active impulse's decay rate is `4.6 − 3.60 × yielding`, multiplied by `0.70` while the victim is airborne. Recoil duration is `0.56 + min(0.20, 0.15 × contactPower)` seconds, plus the type-specific yielding extension in the table. Royal Cavalry's push cap also accounts for backing with a denominator of `1 + 0.28 × backingMass × (1 − yielding)`; strong support still absorbs pressure. Lancers retain their separate light-body cap treatment, while the combined resistance and supporting mass continue to reduce the actual transferred force.

The **per-step force movement budget remains 1.7 times the displaced type's normal speed**, and every movement still passes through the swept body and terrain solvers. The higher initial cap cannot bypass that budget. The earlier 2.2.0 push tuning left troop health, armor, weapon damage, base speed, acceleration, and contact-damage formulas unchanged. The 2.3.0 mounted speeds and physical contact-speed reference are documented above; health, armor, and weapon damage remain unchanged. Fresh commands clear the forward carry; a former charge never authorizes movement through a wall, an occupied body, or a new retreat order.

### Final 2.3.0 mounted breakthrough measurements

The retained encounter uses 12 riders against 56 held levies in four ranks, a 650-unit run-up, and six seconds of observation after the first damage. A rider must deal damage and cross the original rear within the defenders' frontage to count; riders going around an end are excluded.

| Troop | Seed | Interior breakthroughs | Largest defender displacement | Deepest rider penetration |
| --- | ---: | ---: | ---: | ---: |
| Banner Lancers | 7137 | 8/12 | 93.859 | 490.629 |
| Banner Lancers | 4719 | 6/12 | 180.226 | 476.649 |
| Royal Cavalry | 7137 | 7/12 | 106.312 | 382.640 |
| Royal Cavalry | 4719 | 4/12 | 106.238 | 379.480 |

Displacements are world units. These measured encounters preserve the existing swept-body and terrain requirements. Short approaches and prepared or heavy defenders retain their separate counter checks; the table is not a prediction for every deployment.

### Historical 2.1.1 → 2.2.0 breakthrough comparison

These measurements belong to the earlier releases. See `tests/VALIDATION.md` for the final 2.3.0 results at the higher mounted speeds.

The fixture uses **12 mounted troops against 56 held levies in four ranks**, a **650-unit run-up**, and a six-second observation window after the first damage. A rider must damage a defender and cross the original rear within the formation's frontage; anyone who bypasses an end is excluded. Displacement and penetration are measured in world units. These are repeatable scenario results, not universal odds for every battle.

| Attacker | Seed | Interior breakthroughs: 2.1.1 → 2.2.0 | Maximum defender displacement: old → new | Maximum rider penetration: old → new |
| --- | ---: | ---: | ---: | ---: |
| Banner Lancers | 7137 | 4 → **8** | 46.014 → **74.271** | 241.456 → **358.452** |
| Banner Lancers | 4719 | 4 → **8** | 59.337 → **110.331** | 225.611 → **328.208** |
| Banner Lancers | 2531 | 0 → **5** | 65.426 → **73.971** | 96.310 → **352.181** |
| Banner Lancers | 1921 | 3 → **6** | 63.934 → **106.338** | 225.822 → **371.936** |
| Royal Cavalry | 7137 | 0 → **2** | 23.451 → **67.905** | 74.572 → **266.611** |
| Royal Cavalry | 4719 | 0 → **3** | 24.090 → **62.779** | 80.238 → **254.625** |
| Royal Cavalry | 2531 | 0 → **4** | 24.090 → **70.672** | 80.238 → **241.051** |
| Royal Cavalry | 1921 | 0 → **3** | 31.206 → **64.561** | 56.270 → **251.774** |

All eight updated comparisons retained a minimum swept body separation of **0.960 times combined radii**. The short-run-up, prepared pike, formed hoplite, and heavy-guardian comparison fixtures retained zero interior breakthroughs. Solid terrain still blocks pressure, and a fresh retreat interrupts the plunge after its brief turn.

Backing remains meaningful in the separate single-rider resistance fixture: an unsupported levy moves **43.348** units forward, while the lead levy with three supporting ranks moves **35.873**, or **82.8%** as far. The following ranks move **31.411, 24.785, and 18.050** units. A braced hoplite in the comparison moves **6.358** units and stays grounded. These checks are retained in `tests/impact-siege-test.cjs`; the four-rank breakthrough gates are in `tests/lancer-breakthrough-test.cjs` and `tests/mounted-push-test.cjs`.

## Siege balance

An Assault Siege Tower has 950 health, 9 armor points, heavy armor, and a
23-unit normal speed. It opens a 144-unit assault lane after physically
reaching a wall and deploying for four seconds. A Covered Battering Ram has
780 health, 9 armor points, heavy armor, and a 26-unit normal speed. Its
structural strike deals 95 damage to gates or 52 to walls, on a 2.2-second
cycle after arriving. Structural strikes are separate from ordinary troop
weapon damage and the armor matrix above.

Custom walls begin with 2,200 health and custom gates with 1,000. Imported
initial structure health is bounded from 200 to 10,000. Authored maps may use
stronger fortifications. Catapults and ballistas can bombard structures;
ordinary melee attacks contribute smaller structural damage at an entrance.
Established breaches and ramps remain passable. Each army is limited to
12 towers and 12 rams, with one machine per formation.

## Every troop’s targets and threats

### Levy Spearmen

A low-cost screen. Its modest mounted bonus is not a substitute for a braced pike line.

**Good targets and conditions**

- **Longbow Rangers:** Cheap spears can overrun unsupported bowmen if they reach them.
- **Steppe Outriders:** An inexpensive spear screen protects the troops behind it from mounted harassment at close range.

**Threats and counterplay**

- **Berserkers:** Sweeping clubs overwhelm lightly protected levies.
- **Longbow Rangers:** Bowmen outrange the spear screen and punish its light armor.

### Shield Swordsmen

Reliable line infantry; combine with pikes against cavalry and missiles against heavy defenders.

**Good targets and conditions**

- **Levy Spearmen:** Shields, mail, and better morale favor sustained close combat.
- **Longbow Rangers:** Once in contact, blades beat lightly armored bowmen.

**Threats and counterplay**

- **Banner Lancers:** An unbraced line is vulnerable to a clean lance charge.
- **Arbalest Company:** Piercing bolts bypass much of the mail protection.

### Pike Guard

Use line or shield wall, face the charge, and allow the ranks to settle. Moving, loose, or flanked pikes cannot fully brace.

**Good targets and conditions**

- **Royal Cavalry:** A settled cohesive front intercepts charges and adds strong mounted damage.
- **Banner Lancers:** Set pikes blunt lance momentum before the riders reach the line.

**Threats and counterplay**

- **Longbow Rangers:** Longbows outrange pikes and exploit their light armor.
- **Shield Swordsmen:** Infantry and attacks from the flank bypass the mounted bonus and frontal brace.

### Bronze Hoplites

A firm formation fighter. Shield pressure needs cohesive, supported ranks and space behind the opponent; heavy bodies and terrain can stop it.

**Good targets and conditions**

- **Levy Spearmen:** A cohesive shield front withstands light spears and can press lighter bodies backward.
- **Shield Swordsmen:** Supported frontal spear ranks can hold and exert limited pressure; flanked hoplites lose that advantage.

**Threats and counterplay**

- **Arbalest Company:** Piercing bolts exploit the slow advance and bypass bronze armor.
- **Crown Halberdiers:** Poleaxe penetration threatens the shield line, especially from its sides.
- **Stonebreak Catapults:** Packed ranks are vulnerable to long-range area damage.

### Crown Halberdiers

An armored and piercing alternative to pikes. Pikes provide the stronger dedicated charge defense.

**Good targets and conditions**

- **Iron Guardians:** Armor penetration and the heavy-armor modifier help against plate; avoid trading into an entire shielded front.
- **Royal Cavalry:** Long polearms add mounted damage and a limited frontal brace.

**Threats and counterplay**

- **Longbow Rangers:** Slow polearms cannot answer sustained long-range volleys.
- **Berserkers:** Heavy clubs can exploit exposed flanks and the slower polearm recovery.

### Iron Guardians

An expensive defensive anchor, not a fast pursuer. Heavy mass also resists shield pressure.

**Good targets and conditions**

- **Levy Spearmen:** Plate and a tower shield absorb weak frontal attacks.
- **Levy Slingers:** Light sling stones have little effect against heavy armor.

**Threats and counterplay**

- **Arbalest Company:** High penetration and a heavy-armor damage bonus defeat much of the protection.
- **Crown Halberdiers:** Piercing poleaxes remain effective against plate, particularly when the shield is bypassed.

### Berserkers

High damage after reaching the fight; screen the approach and attack disordered or exposed fronts.

**Good targets and conditions**

- **Levy Spearmen:** Rapid heavy blows punish lightly protected infantry.
- **Pike Guard:** Sweeping clubs exploit pikes that cannot use their mounted bonus.

**Threats and counterplay**

- **Longbow Rangers:** Light armor takes increased arrow damage before contact.
- **Banner Lancers:** A clean mounted charge punishes the unshielded approach.

### Longbow Rangers

Long range and frequent volleys favor light targets; support with piercing weapons against heavy armor.

**Good targets and conditions**

- **Pike Guard:** Long range and a light-armor modifier punish stationary pike fronts.
- **Berserkers:** Arrows are effective against exposed, lightly armored shock infantry.

**Threats and counterplay**

- **Royal Cavalry:** Fast riders can close the distance and defeat an exposed bow line.
- **Iron Guardians:** Heavy armor and frontal shields blunt ordinary arrows.

### Arbalest Company

A deliberate armor counter. Its numerical modifier does not remove the need for distance and a protective screen.

**Good targets and conditions**

- **Iron Guardians:** High armor penetration and a heavy-armor modifier make bolts useful against plate.
- **Banner Lancers:** Piercing bolts threaten armored riders while an infantry screen keeps them at range.

**Threats and counterplay**

- **Longbow Rangers:** Longbows outrange crossbows and fire more frequently.
- **Royal Cavalry:** The long reload leaves unsupported crews vulnerable to a close charge.

### Javelin Skirmishers

Mobile close-range support. Automatic bands reposition between throws; an explicit Charge occupies a firing band.

**Good targets and conditions**

- **Levy Spearmen:** Strong short-range throws and quick movement punish light infantry.
- **Pike Guard:** Throwers can damage settled pikes without entering their melee reach.

**Threats and counterplay**

- **Longbow Rangers:** Longbows outrange the throwing bands.
- **Royal Cavalry:** Riders can close the short throwing distance if no spear screen intervenes.

### Levy Slingers

Economical pressure against light troops. Cost efficiency does not make slings an armor-piercing weapon.

**Good targets and conditions**

- **Levy Spearmen:** Inexpensive, frequent stones work well against lightly armored levies.
- **Berserkers:** Lightly protected shock infantry are susceptible while crossing open ground.

**Threats and counterplay**

- **Iron Guardians:** Heavy armor strongly reduces the effect of small stones.
- **Royal Cavalry:** An exposed sling screen cannot withstand mounted contact.

### Royal Cavalry

Use speed to find open flanks or isolated missile troops; avoid charging into set pikes.

**Good targets and conditions**

- **Longbow Rangers:** Speed and charge impact exploit unsupported bow lines.
- **Levy Slingers:** Riders can overrun exposed light missile troops.

**Threats and counterplay**

- **Pike Guard:** A facing, settled pike formation punishes frontal charges.
- **Crown Halberdiers:** Piercing polearms add damage against mounted bodies.

### Banner Lancers

Strongest on the first impact. Flank braced polearms and avoid becoming trapped in sustained melee.

**Good targets and conditions**

- **Shield Swordsmen:** A clean run-up creates a powerful hit against infantry without a pike brace.
- **Arbalest Company:** An exposed, reloading bolt line is vulnerable if riders close the gap.

**Threats and counterplay**

- **Pike Guard:** Set pikes absorb lance momentum and punish the impact.
- **Ironwind Ballistae:** Heavy piercing bolts threaten expensive armored riders at long range.

### Steppe Outriders

Mobile harassment against targets it can keep at distance, not a frontal cavalry substitute.

**Good targets and conditions**

- **Pike Guard:** Mounted bowmen can harass a slow pike line without a frontal charge.
- **Levy Spearmen:** Speed and ranged fire exploit slow, lightly protected infantry.

**Threats and counterplay**

- **Longbow Rangers:** Longbows outrange mounted bows and punish their light protection.
- **Royal Cavalry:** Fast melee riders are dangerous if they catch the skirmishers.

### Stonebreak Catapults

Targets concentrations of troops; loose formations and mobile threats reduce its advantage.

**Good targets and conditions**

- **Bronze Hoplites:** Area impacts punish densely packed shield ranks.
- **Iron Guardians:** Long-range splash can pressure a slow defensive mass.

**Threats and counterplay**

- **Royal Cavalry:** Flanking riders can reach engines during their long reload.
- **Berserkers:** Fast infantry destroy an unprotected battery once in contact.

### Ironwind Ballistae

Slow single-target fire against valuable armored bodies; it needs protection from cheap or fast attackers.

**Good targets and conditions**

- **War Elephants:** Powerful, highly penetrating bolts threaten large armored beasts.
- **Banner Lancers:** Fire at expensive armored riders from behind a protective infantry screen.

**Threats and counterplay**

- **Royal Cavalry:** Mobile riders can flank and overrun exposed engines.
- **Levy Spearmen:** Numerous cheap infantry can overwhelm an unprotected single-target battery.

### Assault Siege Tower

An engineering unit: escort it to an enemy wall under Auto. It deploys a ramp after arriving; the captured passage then remains usable.

**Good targets and conditions**

- **Longbow Rangers:** A protected firing deck can support an assault while the tower approaches a defended wall.
- **Levy Spearmen:** The tower can establish a raised assault passage past a lightly defended wall.

**Threats and counterplay**

- **Stonebreak Catapults:** Heavy stones damage the large wooden structure before it can deploy.
- **Ironwind Ballistae:** Piercing siege bolts threaten the slow, expensive tower.

### Covered Battering Ram

An engineering unit: Auto drives to a defended gate and repeatedly batters it at physical contact range. Keep infantry beside it.

**Good targets and conditions**

- **Levy Spearmen:** Protected crews can withstand light attacks while breaking a gate.
- **Longbow Rangers:** The armored roof reduces weak missile damage during the slow approach.

**Threats and counterplay**

- **Stonebreak Catapults:** Heavy stones punish a slow engine and its packed escort.
- **Crown Halberdiers:** Piercing polearms can damage the engine if its infantry escort is defeated.

### War Elephants

Very expensive shock beasts. Their mass resists shield shoves, but does not make them immune to polearms or missiles.

**Good targets and conditions**

- **Levy Spearmen:** Large health reserves and sweeping impacts punish tightly packed light infantry.
- **Shield Swordsmen:** A clean impact and splash pressure a dense line without dedicated polearms.

**Threats and counterplay**

- **Pike Guard:** Long pikes bring several ranks of mounted damage to bear on a large body.
- **Ironwind Ballistae:** Heavy piercing bolts are effective against a valuable armored target.

### Field Commander

Its main counter to enemy pressure is allied morale support and rallying. Keep it protected rather than using it as a frontline fighter.

**Good targets and conditions**

- **Levy Spearmen:** A commander can survive scattered light spear attacks while supporting nearby allies; this is not a cost-efficient duel.
- **Levy Slingers:** Heavy protection resists weak sling hits while the commander rallies the line.

**Threats and counterplay**

- **Pike Guard:** Mounted commanders are vulnerable to the same set-pike defenses as other riders.
- **Ironwind Ballistae:** Piercing long-range bolts threaten an exposed, valuable commander.

## Tuning the game

Edit `ARMOR_CLASSES`, `ATTACK_PROFILES`, and `UNIT_TACTICS` in `src/data.js` to change the class system and reference text. Each troop’s own entry contains its base statistics and any brace, frontal shield, or pressure settings. `GameData.armorMultiplier` and `GameData.getMatchup` expose the shared values to combat and the interface. Rebuild with `python3 build.py` after editing source.

The strengths and threats are contextual tactical guidance. Automated checks exercise actual hits and representative matchups; they do not establish a universal ranking of every troop pairing, terrain, army size, or cost ratio. See `tests/VALIDATION.md` for the release’s measured scope.
