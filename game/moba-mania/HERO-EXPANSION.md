# MOBA Mania: Rivet, Brinehook, Vela, and Solstice

**Status: playable expansion.** All four heroes are selectable, expanding the roster from eight to twelve. This document records their abilities, talent trees, visual identities, and implementation contracts.

## Fit with the current game

The original eight heroes in [data.mjs](data.mjs) cover a control mage, burning bruiser, creature summoner, sustaining support, poison marksman, displacement tank, mobile assassin, and frost swordsman. The additions below give players four different decisions: where to establish a defensive position, which enemy to pull out of formation, when to spend a magazine, and when to protect an ally or accelerate a combination.

| Hero | Role and distinctive contribution | Main tradeoff |
| --- | --- | --- |
| **Rivet, Copper Engineer** | Lane defender who builds a small, temporary firing position | Strong preparation; weak pursuit and relocation |
| **Brinehook, Teal Corsair** | Catch tank who brings an exposed enemy toward his team | Valuable accuracy; an intercepted hook creates a long opening |
| **Vela, Ivory Gunslinger** | Sustained marksman whose six-round magazine rewards spacing | Excellent steady damage; readable reload vulnerability |
| **Solstice, Golden Chronomancer** | Reactive support who mitigates a dangerous moment and advances allied cooldowns | Strong timing; limited damage and no escape dash |

All four are human characters. Their silhouettes remain readable beside the existing brightly colored, code-drawn heroes. None uses a mouse, another angel silhouette, or a second animal-summoning army.

### Shared rules and seed values

These are baseline tuning values. Automated checks establish mechanical behavior; human playtesting is still needed to judge balance. Distances are world units and times are seconds. Active-ability damage uses the existing cast multiplier in [engine.mjs](engine.mjs): level growth, general damage, and ability damage. Shields and healing use their existing dedicated scaling. Values explicitly written as a percentage of basic-attack damage use that damage once, without multiplying by spell power again. Device stats are snapshotted when created, as with current summons.

Use the existing **Q / E / R / F** controls, including touch aim-and-release. Slots unlock at levels **1 / 2 / 4 / 6**. There are no extra recast keys, pet-selection modes, mandatory double taps, or separate ultimate meters. All four keep level 18 progression and one choice from each talent tier at **3, 5, 8, 10, 12, 15, and 18**. The three table columns describe build directions; players may mix columns freely.

| Hero | Health | Mana | Speed | Basic range | Basic damage | Attack interval |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Rivet | 740 | 330 | 174 | 245 | 36 | 0.90 s |
| Brinehook | 1,090 | 310 | 165 | 68 | 52 | 1.00 s |
| Vela | 650 | 285 | 193 | 280 | 42 | 0.66 s, with reloads |
| Solstice | 700 | 390 | 180 | 235 | 35 | 0.85 s |

## Rivet — Copper Engineer

**Play pattern:** establish two sentries along a safe approach, stand close enough to maintain them, and choose when to overclock the position. Rivet supports a tower defense or beast contest but must dismantle that advantage simply by moving elsewhere. Unlike Emerald Druid's pursuing creatures, his devices never follow, chase, or block a path.

### Passive: Field Mechanic

A landed basic attack repairs the most damaged allied sentry owned by Rivet within 260 units for **10 + 1 per hero level** health. Maximum one repair per second; repairs stop at maximum health. It cannot repair heroes, towers, cores, mines, another Rivet's devices, or captured beasts. Repair is device maintenance, not healing contribution for hero-kill assists.

### Abilities

| Slot / unlock | Skill | Seed cost / cooldown | Effect and readable warning |
| --- | --- | --- | --- |
| Q / 1 | **Screwshot** | 30 mana / 6 s | A visibly spinning bolt travels 430 units, radius 13, and deals 95 damage to the first valid enemy. It ricochets once to the nearest visible hostile within 120 units for 45 damage. The bounce cannot hit the original target or travel through terrain. A short copper trail identifies both legs. |
| E / 2 | **Bolt Sentry** | 45 / 10 s | Place a stationary, targetable sentry within 220 units after a 0.6 s assembly animation. It has 230 health, range 250, 26 damage, a 0.9 s attack interval, and an 18 s life. Maximum two sentries; a third replaces the oldest with a visible shutdown. Preserve ordinary summon scaling and damage reductions. |
| R / 4 | **Shock Mine** | 40 / 12 s | Place a clearly visible mine within 300 units. Its warning ring fills for 0.8 s before arming. The first currently visible hostile within 65 units triggers an 85-radius explosion: 80 damage and 35% slow for 2 s. Maximum two mines, 12 s lifetime; a third replaces the oldest. It is targetable, has 70 health, and supplies no continuous vision. |
| F / 6 | **Overclock** | 70 / 24 s | Rivet and his existing sentries within 300 units light up for 6 s. Sentries gain 50% attack speed; Rivet gains 20% attack speed and a 90 shield. A copper gear expands from Rivet, then bright cooling fins and sparks identify active sentries. New sentries placed afterward do not inherit the buff. |

Sentries use Rivet's explicitly focused target only while it is visible, valid, and within their own range. Otherwise they attack the nearest valid visible enemy. They stay in place when a target leaves range. Their sight uses the normal summon rules, including brush concealment; mine damage and triggered effects do not track concealed enemies. Destroying a device cancels its unlaunched attacks, and expiring a mine creates no damage burst.

### Talent choices

| Level | Artillery | Workshop | Fieldcraft |
| --- | --- | --- | --- |
| 3 | **Deep Threads:** Screwshot's first hit deals 20% more damage. | **Reinforced Casing:** sentries gain 20% maximum health. | **Rubber Soles:** gain 10% movement speed. |
| 5 | **Long Barrel:** gain 12% basic-attack range. | **Repair Routine:** Field Mechanic repairs 40% more health. | **Spare Cells:** gain 20% mana and 1.5 mana regeneration. |
| 8 | **Hot Rivets:** sentries gain 15% attack speed. | **Portable Workshop:** sentries live 5 s longer; the maximum stays two. | **Wide Tripwire:** mine trigger radius becomes 80 and explosion radius 100. |
| 10 | **Master Gunsmith:** gain 15% general damage. | **Insulated Apron:** gain 18% health and 15% shield strength. | **Quick Assembly:** reduce ability cooldowns by 12%. |
| 12 | **Crossfire:** a sentry's first hit on Rivet's explicitly focused enemy deals 30 bonus damage; 5 s recharge per sentry. | **Emergency Patch:** a sentry first falling below 35% health gains a 45 shield for 3 s, once during its lifetime. | **Survey Charge:** a mine's actual explosion reveals struck enemy heroes for 2 s. |
| 15 | **Double Thread:** Screwshot may bounce a second time for 30 damage to a different visible target; the same range and terrain rules apply. | **Shared Tools:** each successful Field Mechanic repair gives Rivet a 20 shield for 2 s, with a separate 3 s recharge. | **Exit Plan:** a triggered mine grants Rivet 20% movement speed for 2 s if he is within 300 units; 6 s recharge. |
| 18 | **Redline:** Overclock adds 20% sentry damage during its existing duration. | **Fortified Position:** Overclock also gives affected sentries a 100 shield for 6 s. | **Rapid Relocation:** Overclock removes up to 4 s from Bolt Sentry's remaining cooldown and grants Rivet 20% movement speed for 3 s. |

### Art, animation, and counterplay

Use a short, square human silhouette: copper welding mask with a bright cyan slit, cream work apron, dark boots, and one large shoulder toolbox. Suggested colors: copper `#C97A45`, cream `#F4D7A1`, charcoal `#283440`, accent cyan `#8BE8E5`. Keep the face visor smaller than Arc Rift's blue-flame silhouette. Sentries are low tripods with two chunky barrels, never miniature heroes.

Icons are a screw bolt, tripod, exposed coil, and toothed gear. Use four walking frames, a three-frame wrist-shot, four assembly frames, two sentry recoil frames, and a distinct broken-device puff. Sentry base rings show ownership independently of copper armor. Mine windup uses a filling ring, so color alone is unnecessary.

Enemies can clear the devices before committing, outrange the position, or rotate to another lane. Rivet has no dash or hard stun. His damage drops substantially when a fight moves away from prepared ground. Bots place sentries behind a healthy wave or their own frontline, maintain spacing between devices, and avoid putting both under a single warned enemy area attack. Veteran and Legend bots prepare a visible beast approach and save a mine for retreat; they never infer hidden enemies from mine placement.

## Brinehook — Teal Corsair

**Play pattern:** threaten a narrow angle, catch an exposed target, and survive while allies follow. Purple Knight pushes enemies away and leads charges; Brinehook is slower and specializes in bringing one target toward a favorable fight. He cannot drag bosses around their pit or move anyone through scenery.

### Passive: Saltwater Resolve

Successfully displacing an enemy hero grants Brinehook an **80 shield for 3 s**, with an **8 s** recharge. Displacing minions, failing against terrain, or applying only a slow does not trigger it. Multiple victims of the same cast grant one shield. The shield supports commitment but does not pay for a missed hook.

### Abilities

| Slot / unlock | Skill | Seed cost / cooldown | Effect and readable warning |
| --- | --- | --- | --- |
| Q / 1 | **Anchorline** | 38 mana / 9 s | Wind up for 0.25 s, then throw a large anchor 440 units along a narrow 18-radius path. It hits the first visible valid unit for 90 damage, pulls an eligible unit up to 130 units toward Brinehook, and stuns for 0.55 s. The windup, bright anchor head, and taut rope remain visible throughout. |
| E / 2 | **Sea Legs** | 42 / 14 s | Gain a 180 shield for 4 s and remove current movement slows. This does not remove stuns or provide immunity to a newly applied slow. Foam gathers around his boots and a visibly cracked shell tracks the shield. |
| R / 4 | **Undertow** | 48 / 13 s | Mark a 110-radius pool within 300 units. After 0.6 s, deal 80 damage, pull eligible enemies up to 65 units toward its center, and slow them 30% for 2.5 s. Converging arrows show the pull direction before the impact. |
| F / 6 | **Captain's Wake** | 75 / 24 s | After a 0.75 s warning, send a broad wave 360 units forward, width 100. It deals 145 damage and stuns for 0.9 s. Eligible enemies move up to 90 units toward the wave's center line, gathering them without carrying them toward the caster. The rectangular warning shows both travel direction and inward arrows. |

Structures and neutral or captured river beasts take permitted damage but resist these displacements. Hooked minions and ordinary camp creatures may move; camp movement must remain within the existing leash. All movement traces the complete path against terrain and clears navigation state. Hooks do not interrupt an enemy's input indefinitely or create a new persistent pursuit command for the player.

### Talent choices

| Level | Hookmaster | Captain | Breaker |
| --- | --- | --- | --- |
| 3 | **Barbed Anchor:** Anchorline deals 25% more damage. | **Thick Coat:** gain 18% maximum health. | **Deck Fighter:** gain 12% basic-attack damage. |
| 5 | **Long Cable:** Anchorline reaches 15% farther; pull distance is unchanged. | **Weatherproof:** Sea Legs shields 25% more. | **Sea Stride:** gain 10% movement speed. |
| 8 | **Tight Formation:** Undertow radius increases to 130. | **Steady Footing:** gain 15% tenacity, subject to the shared cap. | **Raking Tide:** Undertow damage increases 25%. |
| 10 | **Seasoned Hunter:** reduce cooldowns by 12%. | **Iron Captain:** gain 15% health and 20% shield strength. | **Boarding Axe:** gain 18% attack speed. |
| 12 | **Marked Catch:** a successful Anchorline hit reveals that target for 3 s, without revealing nearby units. | **Crew Cover:** Sea Legs also grants allied heroes within 130 units a 60 shield for 3 s. | **Heavy Landing:** the next basic attack within 3 s of pulling an enemy deals 35 bonus damage; one use per cast. |
| 15 | **Follow the Rope:** Anchorline grants 15% movement speed for 2 s after a hero hit. | **Calm Waters:** Saltwater Resolve also heals Brinehook for 35; keep its 8 s recharge. | **Rough Current:** Undertow leaves a 2 s pool that applies a 20% slow while occupied, without repeated pulls or damage. |
| 18 | **Grand Wake:** Captain's Wake is 20% wider, with unchanged stun and displacement. | **Safe Harbor:** Captain's Wake grants allied heroes in its warning area a 110 shield for 4 s when it launches. | **Boarding Party:** hitting an enemy hero with Captain's Wake hastens nearby allies' attacks by 20% for 4 s. |

### Art, animation, and counterplay

Give him a wide triangular coat, squared shoulders, sea-glass beard, and an oversized anchor dragged from one hand. Teal `#3FB8AC`, deep navy `#153C53`, pale foam `#D0F2E3`, and weathered brass `#B9A36B` keep him separate from the purple shield knight. His anchor, not another shield or sword, defines the silhouette.

Icons are an anchor with rope, boot in foam, whirlpool, and ship's bow. Show a heavy four-frame walk, three hook-windup frames, a two-frame cable recoil, and a wave built from stepped pixel crests. Sea Legs' foam persists only while its shield exists. Pull warnings use inward chevrons and outlined borders.

Minions intercept Anchorline. Dodging the visible throw leaves Brinehook without his best initiation for nine seconds. He has no gap-closing dash, low sustained ranged pressure, and limited value if his team cannot follow. Bots check the first body along the entire hook line, compare nearby team strength, and prefer peeling an attacker off a wounded ally. They do not pull a dangerous frontline enemy into their own fragile backline or chain all control onto an already disabled target. Aggressive tower hooks still use the existing dive evaluation.

## Vela — Ivory Gunslinger

**Play pattern:** fire a useful magazine, reposition during reload, then recommit. Dark Ranger centers on poison and aimed arrow skills; Vela's main threat is repeated basic attacks and the player's ability to keep a safe distance. Her abilities support that rhythm instead of providing an assassin's instantaneous kill combination.

### Passive: Six-Round Rhythm

Basic attacks spend one of **six rounds at launch**. The shot that empties the magazine, called the sixth round, deals **120% basic-attack damage**. Restoring a round delays this finishing shot; it does not create a separate six-hit counter. After emptying the magazine, Vela reloads for **1.4 s** before basic attacks resume. Moving and casting are allowed during reload; attacking is not. Reload begins automatically and needs no extra key. A six-pip counter sits beside her health bar; empty pips fill in sequence during reload. Shots cannot create another shot or consume ammunition through item-proc recursion. Death resets to a full magazine; saving preserves rounds and remaining reload time.

### Abilities

| Slot / unlock | Skill | Seed cost / cooldown | Effect and readable warning |
| --- | --- | --- | --- |
| Q / 1 | **Split Shot** | 30 mana / 6 s | Fire two visible bullets in a narrow fan to 360 units. Each hits the first valid enemy for 65 damage. If both hit the same victim, the second deals only 35. Uses no magazine rounds and does not advance the sixth-shot passive. The preview displays both paths. |
| E / 2 | **Sidestep** | 35 / 11 s | Dash up to 165 units toward the aim, stopping at terrain. Restore one spent round; if already reloading, instead remove up to 0.45 s of reload time. A brief ivory afterimage shows the path. No damage, invulnerability, or automatic attack reset. |
| R / 4 | **Steady Aim** | 45 / 14 s | For 4 s, gain 15% basic range and 20% attack speed, while moving 15% slower. The six-pip counter receives a sight bracket and the guns glow white. Reload still occurs, and the buff does not refund ammunition. |
| F / 6 | **Silver Volley** | 70 / 24 s | Warn along a fixed 480-unit, 24-radius line for 0.4 s, then channel six bullets over 1.2 s. Each deals 45 damage to the first valid target in its path. Stun or movement cancels unlaunched bullets. Aim is fixed at release, and the shot does not consume or reload her basic magazine. |

Sidestep cannot shorten the basic-attack timer; otherwise repeated ammunition restoration becomes an unintended burst reset. Silver Volley pauses basic attacks; casting another ability also cancels its unlaunched bullets. Its projectiles retain the owner's credit and normal structure protections. Enemies can intercept the volley with minions, move perpendicular to the marked line, or interrupt the channel. Its narrow path never turns to follow the victim.

### Talent choices

| Level | Deadeye | Duelist | Outrider |
| --- | --- | --- | --- |
| 3 | **Fine Powder:** Split Shot deals 20% more damage, including its reduced second hit. | **Quick Draw:** gain 12% attack speed. | **Travel Coat:** gain 15% maximum health. |
| 5 | **Long Sights:** gain 10% basic range. | **Fast Hands:** reload lasts 1.2 s instead of 1.4 s. | **Light Steps:** gain 10% movement speed. |
| 8 | **Measured Fire:** Steady Aim lasts 1 s longer. | **Clean Chambers:** Sidestep restores two rounds instead of one; its reload-time reduction is unchanged. | **Long Stride:** Sidestep travels 15% farther. |
| 10 | **Silver Pressure:** gain 15% basic-attack damage. | **Field Medic:** basic attacks provide 10% lifesteal, with ordinary damage-based accounting. | **Escape Artist:** reduce ability cooldowns by 12%. |
| 12 | **Heavy Sixth:** the sixth round deals 140% basic damage instead of 120%. | **Opening Pair:** the first two basic attacks after a completed reload have 15% shorter intervals; no free extra attack. | **Safe Landing:** Sidestep grants a 60 shield for 2 s. |
| 15 | **Patient Aim:** Steady Aim's movement penalty becomes 5%. | **Spare Magazine:** a hero finishing blow removes up to 0.5 s of an active reload; 6 s recharge. | **Covering Shot:** Split Shot's first hit slows its victim 20% for 1.5 s; no additional slow from the second bullet. |
| 18 | **Silver Storm:** Silver Volley deals 20% more damage without adding shots or shortening warnings. | **Perfect Rhythm:** the sixth round grants a 70 shield for 2 s when it actually hits an enemy hero; no shield on a miss. | **Never Cornered:** surviving damage below 30% health grants 25% speed for 2 s and removes up to 3 s from Sidestep; 30 s recharge. |

### Art, animation, and counterplay

Use a tall, narrow human silhouette with an ivory half-cape, dark trousers, swept silver hair, and two long-barreled pistols held apart. Ivory `#F4EFDC`, slate `#32435B`, desaturated plum `#806E9A`, and restrained silver-blue sparks differentiate her from Dark Ranger's bow and Orange Assassin's flowing cloak. Her walk and attack poses visibly lean away from danger.

Icons are paired bullets, a boot and diagonal streak, a sight ring, and six parallel shots. Give her four walk frames, alternating two-frame pistol recoil, a four-frame reload with visibly tipped barrels, and a locked wide stance during Silver Volley. The sixth muzzle flash is larger and square-edged; use a distinct reload click and dry chamber silhouette without making audio necessary.

She is fragile and has no hard stun, healing spell, or immunity. Her reload and channel give opponents clear windows, especially if Sidestep is spent offensively. Bots reserve Sidestep for threat spacing unless a safe finish is available, strafe during reload, and avoid starting Silver Volley behind a full enemy wave. Higher difficulties track ammunition and their own escape destination; they still use normal sight and reaction delays.

## Solstice — Golden Chronomancer

**Play pattern:** predict a dangerous exchange, protect the teammate about to take damage, then give the team a slightly earlier second round of spells. Pink Angel sustains a group through persistent healing and haste. Solstice is a fragile, reactive support whose value depends on short protective windows and cooldown timing.

### Passive: Perfect Timing

If **Time Ward** absorbs at least **30 damage within its first second**, restore **8 mana** and remove up to **1 s** from Solstice's Hourhand cooldown. This can occur once every **6 s**. Only actual absorbed enemy damage counts; recasting, self-damage, overwriting a shield, or an unused shield provides nothing. The protected ally receives a small hourglass flash when the timing succeeds.

### Abilities

| Slot / unlock | Skill | Seed cost / cooldown | Effect and readable warning |
| --- | --- | --- | --- |
| Q / 1 | **Hourhand** | 30 mana / 6 s | Fire a clock-hand projectile 410 units, radius 14. Its first valid hit deals 90 damage and slows 25% for 2 s. A thin gold trail and visibly rotating head support aiming and ordinary last hits. |
| E / 2 | **Time Ward** | 45 / 11 s | Aim within 330 units. Select the allied hero nearest the aim within 75 units, including Solstice; show that target before release. Grant a 120 shield for 3 s. At expiry, heal 25% of the health that target actually lost during the effect, capped at 70 base healing. A three-segment hourglass marks the remaining window. |
| R / 4 | **Unwind** | 60 / 16 s | Allies within a 125-radius circle, cast up to 320 units away, lose all current slow layers and have their current stun shortened by at most 0.5 s. Grant each a 60 shield for 2 s. A reverse-turning ring flashes immediately. It does not clear damage-over-time effects or make allies immune to new control. |
| F / 6 | **Borrowed Seconds** | 75 / 26 s | Mark a 150-radius circle within 320 units with a 0.7 s clock-face warning. At impact, enemies take 90 damage and are slowed 35% for 3 s. Each allied hero inside receives one refund on each of their first three ability cooldowns: at most 2 s or 25% of that ability's base cooldown, whichever is smaller. A refund cannot lower a timer below 0.6 s. |

Time Ward's delayed heal cannot resurrect, undo a kill, restore spent gold, return ammunition, or rewind position. Count actual health lost once per damage event after shields and mitigation, and cap healing by current missing health. A new Time Ward ends an older instance on the same target without paying its delayed heal, preventing stacking or shield-refresh farming. Death ends that ward. Unwind leaves the target's crowd-control recovery timestamp intact.

Borrowed Seconds never refunds the fourth ability slot, its own cooldown, item passive recharge timers, or a cooldown already at or below 0.6 s. It applies once at impact, not every frame while a hero stands in the circle. Refunds and cooldown reduction remain separately accounted for, following the existing Eternity Hourglass precedent without creating an infinite feedback loop.

### Talent choices

| Level | Timekeeper | Guardian | Sandweaver |
| --- | --- | --- | --- |
| 3 | **Measured Reserves:** gain 20% mana and 1.5 mana regeneration. | **Thicker Glass:** Time Ward's shield is 25% stronger. | **Sharpened Hand:** Hourhand deals 25% more damage. |
| 5 | **Long Reach:** targeted ability range increases 12%. | **Careful Steps:** gain 15% health. | **Clockwork Pace:** gain 12% movement speed. |
| 8 | **Precise Timing:** Perfect Timing restores 12 mana instead of 8. | **Gentle Return:** Time Ward's delayed-heal cap rises from 70 to 100. | **Heavy Sand:** Hourhand slow lasts 0.75 s longer. |
| 10 | **Balanced Mechanism:** reduce ability cooldowns by 12%. | **Golden Shelter:** gain 20% shield strength and 15% healing strength. | **Bright Hour:** gain 18% ability damage. |
| 12 | **Second Hand:** Perfect Timing removes up to 1.5 s from Hourhand instead of 1 s. | **Shared Moment:** a successful Perfect Timing trigger grants a different nearby allied hero a 35 shield for 2 s, within 160 units of the warded hero. | **Lingering Sand:** Hourhand leaves an 85-radius slow field at the impact for 2 s, applying 20% slow while occupied; no extra damage. |
| 15 | **Quick Recovery:** Unwind grants affected allies 15% movement speed for 2 s. | **Steady Hour:** Time Ward also grants 10% damage reduction for its first second. | **Glass Echo:** Hourhand makes a second warned 45-damage burst at its actual impact point after 0.6 s, radius 60. |
| 18 | **Generous Seconds:** Borrowed Seconds' refund limit becomes 2.5 s; retain the 25% limit, 0.6 s floor, and first-three-slots rule. | **Golden Interval:** Borrowed Seconds also grants allied heroes a 100 shield for 3 s at impact. | **Sunset Dial:** Borrowed Seconds' damage rises 25%; its slow becomes 40% with unchanged duration. |

### Art, animation, and counterplay

Solstice wears a dark, tall hourglass-shaped robe with a segmented golden shoulder dial, an open hood, and a suspended hourglass held between both hands. Gold `#E7B951`, midnight `#272B48`, pale sand `#F4DEA0`, and muted lavender `#9D91C4` avoid Arc Rift's gold armor, staff, and blue fire. The floating hourglass, not a halo, is the identifying shape.

Icons are a clock hand, half-full hourglass, reverse arrow around a boot, and three small clock faces. Use a restrained four-frame hovering walk, three casting poses, visibly falling sand during Time Ward, and stepped rotating ticks on Borrowed Seconds. The delayed heal travels toward a still-living target as a small gold grain arc. All countdowns use geometry as well as color.

Solstice has no dash, hard-disable ultimate, resurrection, or global time stop. Enemies can bait Time Ward, change targets, stagger their damage, or leave Borrowed Seconds before it resolves. His support spells provide no guaranteed farming income, so Hourhand and normal attacks must remain adequate for personal last hits. Bots ward allies facing visible incoming damage, reserve Unwind for actual impairment, and cast Borrowed Seconds where an ally with meaningful remaining cooldowns can safely benefit. Legend bots make better timing choices; they do not inspect hidden enemy cooldowns or future damage.

## Integration requirements

### Economy, combat, and fairness

The rules in [README.md](README.md), [engine.mjs](engine.mjs), and [strategy.mjs](strategy.mjs) remain the baseline. Heroes receive gold only for their own finishing blows. Rivet's devices and every delayed projectile retain owner IDs for attribution. Mines, turret repair, ammunition restoration, cleansing, cooldown refunds, and merely standing near an ally never mint gold. A captured beast remains a team attacker, so none of its later kills pays its capturer.

Damage contribution, effective healing, and shields that actually absorb damage may qualify for the current personal assist XP rules. Time Ward and Unwind use the existing support records; their effects never grant duplicate assists. Cleansing and cooldown refunds alone do not award XP. Rivet's device repair is excluded. Before balancing either support's farm damage, compare real last-hit opportunities against the existing Pink Angel/Purple Knight creature-damage assistance; do not silently extend that assistance to every new ability.

All targeting obeys the current nearest-valid-target, explicit-focus, visibility, protected-structure, and collision rules. Warning graphics do not reveal hidden heroes. Devices, bots, hooks, ricochets, and delayed blasts must never acquire hidden units through an omniscient search. Mines have no vision source; sentries use ordinary summon sight. New displacement cannot cross obstacles, displace structures or river beasts, or move ordinary camps outside their leash.

Use `applyStatus` for all stuns. The current implementation caps tenacity contribution at 35%, shortens a chained hero stun to 55% during its recovery window, and maintains that window for two seconds after the applied stun. Preserve those rules rather than assigning a raw status object. These kits propose no stun longer than 0.9 s before shared modifiers. New pulls should share a 0.5 s per-target displacement recovery so a simultaneous hook and pool cannot repeatedly move a helpless hero. This recovery applies to the new pulls and is covered by the expansion regressions. Slow/haste layers keep their own strength and lifetime. Talent and item cooldown reduction retain the shared 50% cap.

Devices must respect early structure fortification, reduced summon damage, tower retaliation, and protected inner towers/cores. Brinehook cannot relocate a tower or river boss; Vela cannot dash through terrain or use an attack-speed buff to skip reload. Solstice cannot reset the stun-chain history, refund fourth-slot abilities, or reverse a completed kill. All bots apply the current tower-risk and shared-vision rules on every difficulty.

### Implementation map and acceptance criteria

| Runtime area | Responsibilities |
| --- | --- |
| [data.mjs](data.mjs) | Add four definitions, passives, icon identities, and all 84 mutually exclusive talent choices; keep seven tiers and four slots per hero. |
| [engine.mjs](engine.mjs), [hero-skills.mjs](hero-skills.mjs) | Add stationary device behavior, traced pulls, Vela's magazine/channel state, Solstice's tracked ward and controlled cooldown refund; reuse damage, ownership, support, and status APIs. |
| [progression.mjs](progression.mjs) | Add passive/talent hooks with explicit recharge fields, damage-proc recursion guards, and deterministic delayed effects. |
| [strategy.mjs](strategy.mjs) | Assign catch, defend, marksman, and reactive-support jobs; add coherent existing-item recipes and visibility-safe cast priorities. |
| [renderer.mjs](renderer.mjs), [ui.mjs](ui.mjs), [style.css](style.css) | Add distinct code-drawn sprites, pixel icons, warnings, magazine/ward indicators, and compact selection information; preserve the four-button touch layout. |
| [save-state.mjs](save-state.mjs), [profile.mjs](profile.mjs) | Serialize all devices, reload/channel progress, per-target ward accounting, recharge timestamps, and new mastery identities; handle older saves explicitly. |

Focused acceptance should demonstrate: device kills credit the correct owner; targets in brush remain concealed; hooks cannot tunnel through a rock; failed hooks cannot trigger Saltwater Resolve; every Vela attack spends exactly one round; no reload or channel creates free attacks; expired/dead wards cannot heal; Borrowed Seconds cannot affect itself or stack repeatedly; stun recovery survives Unwind; and all twelve choices can start, save, resume, and finish on desktop and both touch orientations. Once mechanics pass, run representative bot matches across difficulties to tune gold access, objective capture, tower safety, and match length. Human playtesting still determines whether these four roles feel fair and enjoyable.
