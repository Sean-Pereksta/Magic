# Ferrite and Cadence: playable polarity and musical-combo heroes

**Status: implemented.** These two heroes join the existing twelve, bringing the selectable roster to fourteen. Both work with the four normal ability buttons, all seven talent levels, bots, personal kill rewards, saved matches, and the existing code-drawn pixel renderer. They need no separately hosted art.

## Ferrite — The Magnetic Colossus

Ferrite is a slow floating automaton with a bright core, separated charcoal body segments, red and cyan magnetic arms, and up to three visible orbiting scraps. He builds a resource by actually hitting enemies, changes magnetic polarity, and chooses whether to spend his stored metal on a shield or an attack.

The existing Purple Knight can protect and shove; Brinehook can catch an isolated enemy with a hook. Ferrite's distinguishing decision is **which direction an area effect will move enemies and when to spend a scarce, visible resource**. Spending metal to survive costs damage and displacement on his next ultimate. Polarity remains in effect until his next pulse, so switching at the wrong time can push a target out of a teammate's follow-up.

### Resource and controls

**Scrap Orbit:** basic damage and Rail Spike hits against non-structures collect one scrap, no more than once every 0.65 seconds. Maximum three. One Rail Spike can collect at most one scrap even after learning its piercing talent. Misses, damage from items, damage over time, allied damage, towers, and simply waiting create no scrap. Scrap resets on death. Polarity starts on **PUSH**; the first Polarity Pulse switches it to **PULL**.

| Button / unlock | Ability | Mana / cooldown | Effect before level and item scaling |
| --- | --- | --- | --- |
| Q / level 1 | Rail Spike | 30 / 6s | A visible 400-range projectile deals 90 physical damage to its first enemy and can collect one scrap. Intervening enemies and terrain block it. |
| E / level 2 | Scrap Barrier | 42 / 13s | Consume all scraps. Shield yourself for 100 plus 50 per scrap for 4s: 100/150/200/250 at zero through three scraps. |
| R / level 4 | Polarity Pulse | 45 / 10s | Switch polarity, then show a 0.35s warning around your current location. Deal 75 damage in a 155-radius circle and move each enemy once: up to 75 inward on PULL, or 100 outward on PUSH. |
| F / level 6 | Lodestone Storm | 78 / 25s | Consume all scraps and warn for 0.8s at a point within 330. Deal 150 plus 35 per scrap in a 155-radius circle, slow 30% for 2s, and move enemies in the current polarity by up to 70 plus 20 per scrap. |

Scrap is spent when the ability is released. Stunning Ferrite or killing him before a warning resolves cancels that impact; the spent resource is not refunded. Structures and river beasts can take ordinary permitted damage but cannot be moved. Displacement follows the entire path, stops at terrain and map edges, keeps ordinary camps inside their leash, and shares the existing 0.5s displacement-recovery rule.

**Example:** land Q and two spaced basic hits to reach three scraps. Use R to switch from PUSH to PULL and gather exposed enemies. Place F where they must either escape its visible warning or take the storm. If threatened before that commitment, E instead turns those same three scraps into a much larger shield.

### Bots and counterplay

Bots gather scrap with normal visible attacks and reserve the ultimate for at least two scraps and a useful hero, beast, or clustered-wave target. They prefer PULL when committing and PUSH when retreating or peeling an endangered ally. They use Scrap Barrier under visible pressure. Bot movement and cast positions keep the ordinary tower-risk checks.

Ferrite has no dash, no permanent crowd-control loop, and no undodgeable ranged initiation. His scraps and polarity are visible. Enemies can interrupt his warnings, make him spend his metal defensively, spread out, or fight from beyond his pulse radius.

## Cadence — The Wandering Resonator

Cadence is a coral-and-cream moth musician with a swept wing cape and a forked tuning staff. Her visible score records Q, E, and R in cast order. Her ultimate has three different behaviors, selected by the last note in that score.

The existing Pink Angel focuses on healing and Solstice on timing wards and cooldown refunds. Cadence **composes a sequence to choose an attack, a shield, or a persistent combat stage**. Her protection is shielding; she does not have a healing spell. The immediate ability she uses last also determines her next finale, creating a tradeoff between the best action now and the desired ultimate form.

### Score and controls

**Three-Note Score:** casting Q, E, or R appends its note to a three-entry score. Notes last 8s. Three different consecutive notes charge the next basic hit for 8s; that hit releases one 45-base-damage sound burst within 95 units of its victim. The charge cannot trigger itself or item-proc recursion. The score keeps its order after that basic hit; F consumes both the stored score and any remaining charged hit. Death clears the score.

| Button / unlock | Ability | Mana / cooldown | Effect before level and item scaling |
| --- | --- | --- | --- |
| Q / level 1 | Piercing Note | 28 / 5.8s | Fire a 420-range musical projectile through up to three enemies, dealing 75 damage to each. Record Q. |
| E / level 2 | Sheltering Chime | 40 / 10s | Grant allied heroes in a 115-radius circle within 310 a 75 shield for 3s. Record E. |
| R / level 4 | Skipbeat | 34 / 11s | Dash up to 140 toward your aim, stopping at terrain, and gain 20% movement speed for 2s. Record R. This dash has no contact damage or invulnerability. |
| F / level 6 | Grand Finale | 75 / 25s | Consume the score and warn for 0.65s within 340 range. Deal 120 damage in a 150-radius circle, then add the last note's effect below. A complete three-note score increases the radius 20%. |

| Last note | Finale form | Additional result |
| --- | --- | --- |
| Q | Shatter Finale | A second clearly warned explosion lands at the same place 0.55s later for 80 base damage. Enemies can leave before the repeat. |
| E | Shelter Finale | Allied heroes inside receive a 160 shield for 4s. No second damage burst or persistent field. |
| R | Dance Finale | Create a 4s stage. Allied heroes inside gain 20% movement and 25% attack speed; enemies inside are slowed 35%. Leaving the stage removes its benefit or penalty after a brief 0.15s refresh window, modified by slow-duration equipment. Only one stage per Cadence may exist. |

An empty score defaults to Shatter Finale at its ordinary radius. A complete score can be any ordering of Q/E/R, so **E → R → Q** prepares the double blast, **Q → R → E** prepares group protection, and **Q → E → R** prepares the dance field. Repeating a note does not charge Three-Note Score unless the final three stored notes are all different.

### Bots and counterplay

Bots use Piercing Note to farm and fight, shield allies under visible pressure, and use Skipbeat to escape close enemies or complete a useful dance setup. Before an ultimate, they prefer a shield ending for an endangered ally, a dance ending in a multi-hero fight, and a damage ending otherwise. They can take one useful setup cast when that desired note and enough mana are available. Their choices use current shared vision rather than hidden enemies.

Cadence is vulnerable when Skipbeat is on cooldown and must spend mana and time to compose a full score. The finale warning can be interrupted. Enemies can spread out, force an early defensive note, leave the second explosion, or fight outside the stage. Her support and area damage never grant passive gold: every finishing blow is attributed through the existing personal economy.

## Shared implementation contracts

- Both heroes have four abilities and seven exclusive three-choice talent tiers at levels 3, 5, 8, 10, 12, 15, and 18. The tables below are generated from the actual definitions.
- Damage, shielding, slow effects, item effects, and progression use the normal engine paths. Scraps and musical notes never award currency or XP by themselves.
- Area hits acquire only currently visible enemies and require an unobstructed line from their effect. New displacement and dashes trace terrain. No ability moves a protected structure or grants vision through hidden enemies.
- Projectile slows, echo availability, and damage are captured at release. Delayed storm and stage slow strength and duration are also captured at release. Buying equipment while these attacks travel does not retrofit a stronger cast.
- Extra chord damage and repeated finale/aftershock damage cannot recursively trigger the item-proc system. They retain their hero's kill credit, including already-created non-interruptible repeats after the hero dies.
- Resource arrays, scrap counts, recharge timestamps, projectile one-shot flags, pending impacts, and stages are deterministic JSON. Save validation rejects impossible resources and malformed variants. Older snapshots without the optional resource state receive empty defaults.
- A stronger existing guard is not weakened by Ferrite's Insulated Core talent. A weaker shield purchase does not create health or currency.
- Starting build plans are shield/control for Ferrite and personal farming XP, spell cooldowns, and shielding for Cadence. Bots still earn their own purchases and obey prerequisites.

## Validation

`node --test tests/moba-mania-resonance.test.mjs` exercises live engine casts, damage, positioning, AI decisions, save/reload, malformed saves, resource limits, fog, interruption, and each ultimate form. These are deterministic correctness checks. Human playtesting is still needed to tune long-term competitive balance and how the new resource loops feel.

## Ferrite: complete talent choices

| Level | Choice 1 | Choice 2 | Choice 3 |
| --- | --- | --- | --- |
| 3 | **Steel Point:** Rail Spike deals 25% more damage. | **Heavy Chassis:** Gain 18% maximum health. | **Tempered Scrap:** Scrap Barrier shields 20% more. |
| 5 | **Long Rails:** Rail Spike reaches 18% farther. | **Hover Drive:** Gain 10% movement speed. | **Grounding:** Gain 15% tenacity, subject to the shared cap. |
| 8 | **Wide Field:** Polarity Pulse covers 20% more area. | **Layered Alloy:** Each scrap adds 65 instead of 50 base shielding to Scrap Barrier. | **Industrious:** Gain 16% attack speed to collect scrap in combat. |
| 10 | **Hard Metal:** Gain 15% general damage. | **Short Circuit:** Reduce ability cooldowns by 12%. | **Deep Core:** Gain 10% health and 20% shield strength. |
| 12 | **Static Point:** Rail Spike also slows its target 20% for 1.5s. | **Shared Alloy:** Scrap Barrier also grants allied heroes within 150 a shield worth 40% of yours. | **Induction:** Polarity Pulse salvages one scrap if it damages a non-structure, once per cast. |
| 15 | **Through Line:** Rail Spike pierces two additional enemies. A shot still generates at most one scrap. | **Insulated Core:** Scrap Barrier reduces incoming damage by 15% for its first 2s. | **Lighten the Load:** Spending three scraps on Scrap Barrier grants 20% movement speed for 2s. |
| 18 | **Aftershock:** Lodestone Storm warns of a second blast after 0.6s: 55% damage, smaller radius, no extra movement. | **Monolith:** Lodestone Storm also grants you a 90 shield plus 35 per scrap for 4s. | **Mass Driver:** Lodestone Storm moves enemies 45 farther and its slow lasts 1s longer. |

## Cadence: complete talent choices

| Level | Choice 1 | Choice 2 | Choice 3 |
| --- | --- | --- | --- |
| 3 | **Clear Tone:** Piercing Note deals 25% more damage. | **Travel Cloak:** Gain 15% maximum health. | **Soft Chime:** Sheltering Chime shields 25% more. |
| 5 | **Far Song:** Targeted abilities reach 12% farther. | **Quickstep:** Gain 12% movement speed. | **Long Phrase:** Notes stay in the score for 10s instead of 8s. |
| 8 | **Loud Chord:** The Three-Note Score basic-hit burst deals 30% more damage. | **Broad Stage:** Sheltering Chime covers 20% more area. | **Long Step:** Skipbeat travels 20% farther. |
| 10 | **Fast Measure:** Reduce ability cooldowns by 12%. | **Bright Song:** Gain 18% ability damage. | **Deep Breath:** Gain 12% maximum mana and 20% shield strength. |
| 12 | **Reverberation:** Piercing Note warns of a 25-damage echo at its first hero hit; radius 65, after 0.4s. | **Shared Rhythm:** A charged basic hit also grants you 15% movement and attack speed for 2s. | **Soft Landing:** Skipbeat grants a 50 shield for 2s. |
| 15 | **Overtone:** Sheltering Chime also grants its allied heroes 15% attack speed for 2s. | **Long Refrain:** Skipbeat’s movement boost lasts 1.5s longer. | **Holding Note:** Piercing Note slows enemies 20% for 1.2s. |
| 18 | **Encore:** Shatter Finale’s second blast deals 120 instead of 80 base damage. | **Grand Shelter:** Shelter Finale shields 25% more and clears current slows from its recipients. | **Last Dance:** Dance Finale’s stage lasts 6s instead of 4s. |
