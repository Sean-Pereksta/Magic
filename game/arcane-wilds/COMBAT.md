# Enemy abilities and combat affinities

`combat-affinities.js` and `enemy-abilities.js` load after the existing combat,
regional, squad, spell, and presentation layers. The world danger adapters
follow them before runtime stability; see [WORLD-DANGER.md](WORLD-DANGER.md).
They extend the existing AI, damage pipeline, dodge rewards, and
effect renderer. Enemy health and base speed are unchanged by these modules.

## Movement attacks

| Attack | Behavior | Player response |
| --- | --- | --- |
| Leap Slam | Wolves and thorn hounds leap onto a marked landing circle. | Leave the circle or dodge the landing, then punish recovery. |
| Vaulting Strike | Divers and advanced assassins vault through the target and slash back into a marked cone. | Move outside the cone or dodge the landing strike. |
| Crosscut Dash | Blade dancers and rams dash along two frozen lanes, with a pause between them. | Sidestep each lane; the second leg cannot retarget. |
| Vault / Volley | Archers and wasps jump backward and fire three marked projectile lanes after landing. | Change direction or use the gaps between shots. |

Jump travel lasts 0.44 seconds, has a separate ground shadow, and causes no
airborne contact damage. Crosscut Dash has two 0.28-second legs; the second
starts 0.8 seconds after activation. Both lanes are visible from windup.

Other signatures include charges, sweeping cleaves, sequential ruptures and
meteors, leading shots, coordinated pincers, rotating beams, gravity collapses,
frost trails, flame waves, burrows, chain lightning, thorn cages, blink strikes,
projectile fans, counter stances, and summoning rituals. Catalog AI and regional
family select normal/advanced patterns; boss phases change the sequence.
Existing support AI retains healing, summoning, and summon/decoy aggro.

## Timing and geometry

- `definitions` controls windup, active time, recovery, cooldown, damage factor,
  and engagement range. Higher difficulty shortens windup by at most 18%, with
  floors of 0.55 seconds for leading shots and 0.7 seconds for major attacks.
- Observed world movement supplies a bounded lead of at most 0.36 seconds and
  1.35 world units. Targets and directions freeze when the warning starts.
- `shapes()` and `inShape()` share world-space circles, capsules, and cones with
  the warning renderer. Projectile warnings include the complete flight lane.
- Thorn cages leave an opening toward the arena interior. Off-room roots are
  omitted rather than clamped into a corner escape route.
- Crowd separation cannot move an enemy off its promised attack lane. Missed
  charges into a wall create a stagger window.
- Only actual attack/projectile overlap during dodge invulnerability triggers
  the new major perfect dodge: 18 momentum, the existing cooldown refunds and
  counter pulse, 0.8 seconds of tailwind, and a nearby attacker stagger where
  appropriate. A warning alone cannot award it. One dodge awards once.

## Elemental matchups

| Element | Target tags/status | Damage multiplier |
| --- | --- | --- |
| Fire | ICE or PLANT | 1.35 |
| Fire | UNDEAD / wet target | 1.25 / 0.75 |
| Frost | FIRE | 1.30 |
| Water | FIRE | 1.35 |
| Earth | ARMORED, CONSTRUCT, or a shield | 1.45 |
| Wind | Enemy currently owns an attack | 1.15 |
| Lightning | ARMORED / WET | 1.30 / 1.40 |
| Physical or impact | CONSTRUCT | 1.30 |
| Arcane | ARCANE with a magical shield | 1.35 |
| Solar/holy | UNDEAD or SHADOW | 1.40 |
| Shadow | ARCANE | 1.30 |
| Poison | BEAST or HUMANOID | 1.25 |
| Poison | CONSTRUCT or UNDEAD | 0.80 |
| Matching fire/frost/lightning/shadow | Matching elemental tag | 0.85 |

The resolver selects one matchup; multiple tags do not multiply bonuses.
Dynamic wet status and traits added after spawning participate in lookup.
Fire erodes frost shields, frost briefly suppresses fiery attackers and nearby
burning ground, lightning staggers conductive enemies, and shadow disrupts
casters. Fire spread and armored lightning arcs reach at most one neighbor per
proc, with a 1.1-second cooldown and recursion guard.

Projectile and ground-field `combatElement` metadata preserves a spell's
identity independently of sprite names and upgrade tags. `damageEnemy` accepts
an optional fifth source argument; `radialDamage` accepts an optional seventh
source argument. Summons and continental actors carry tags, and enemy ability
hits plus native actor/projectile damage use the same affinity resolver.

Weak hits keep damage numbers enabled/disabled according to existing settings.
Strong hits enlarge numbers and use short labels, elemental flashes, sound,
particles, and cosmetic decals. Physical cracks are presentation decals with
no damage behavior.

## Elite modifiers and effect budgets

Ordinary campaign elites now use the affixes in `danger-combat.js` instead of
the automatic legacy modifier below. Explicit legacy and Shadow encounters
retain this original behavior.

Echoing repeats geometry with a fresh warning; jumping/dashing echoes repeat
ground danger without moving the actor twice. Volatile and Frozen leave warned
ground hazards, Blinking repositions after an attack, Twin Cast advertises its
second fan, Bulwark periodically guards, Stormbound adds a warned lightning
strike, and Vengeful shortens cooldown. Existing Shadow speed, regeneration,
vampiric, and summoned-minion behavior is retained.

Gameplay records use `game.effects` kind `enemyAbility`, separate from cosmetic
effect kinds. They stay visible when particles are disabled or their budget is
full. Admission limits concurrent major warnings/attacks to three, or four at
difficulty 7+, spaces new starts by 0.3 seconds, accounts for legacy intensity
hazards, and limits records to 40. Secondary elite attacks share this admission
limit. Death/stun cancels owned attacks; changing rooms clears all records and
jump/hidden/counter state.

Run `npm test --prefix game/arcane-wilds` from the repository root. The affinity
and enemy-ability tests exercise the complete ordered HTML runtime, alongside
existing spell, regional, boss, mobile, presentation, save, and inventory tests.

