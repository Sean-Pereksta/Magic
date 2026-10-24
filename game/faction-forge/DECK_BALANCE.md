# Historical five-body combat balance sample

The October 24 recruitment correction keeps the five distinct troop types per deck and removes the field population cap. Armies can continue recruiting copies, and the board grows for their footprints. Summon and decoy abilities retain their own budgets. The matrix below records the earlier five-body setup; its win rates do not establish balance for larger armies. Current regression tests separately verify growing recruitment, mixed-footprint deployment, snapshot recovery and type validation.

The balance harness runs the actual functions from `crowncouncil.html` through
the same authoritative simulator used by the optional multiplayer service.
It does not approximate combat with a separate damage model.

## Reproduce

```sh
node game/faction-forge/balance-benchmark.cjs 5 --json
node --test game/faction-forge/tests/combat-rules.test.mjs game/faction-forge/tests/combos-evolutions.test.mjs
```

`combo-benchmark.cjs` remains a compatible entry point. The checked-in
`balance-results.json` records the complete matrix. Increase the seed argument
to sample more specialization choices; it accepts up to 30 seeds.

## Final decisions

- One recruited troop creates one body. Every live summon and illusion shares
  the five-body cap. Matron replaces casualties; with a full formation it gives
  a nearby brood a temporary shield and attack surge. Mirage Warden instead
  supplies a short mirror shield when no decoy slot is available.
- Defenders have more durability and substantially less personal damage than
  dedicated carries. Rank growth is +22% health and +18% damage per rank;
  permanent Forge upgrades remain separate and normalize in ranked play.
- Favorable counter multipliers are normally 1.20–1.30 and never exceed 1.35.
  Marksman removes 65% of actual armor, or 85% with Armor Piercer. Railbreaker
  removes 75%, magical attacks 50%, and corrosion attacks 35%. Percentage and
  flat penetration remove armor before mitigation; they do not add another
  raw-damage multiplier. Precision shots lose 20% effectiveness against swarms.
- These armor reductions can produce a larger *effective* advantage than 35%
  against heavy armor. That is deliberate: a protected Marksman must reliably
  wear down a defender, while its deliberate reload and fragile body leave it
  vulnerable to flanks. Rank and combo bonuses do not multiply this advantage.
- Guards intercept one attack every 2.2 seconds from a lane in front of a
  nearby ally, within two tiles. Flanks, precision penetration, disruption and
  area attacks defeat that protection. Only one ward mitigates a projectile;
  all temporary shields share a four-point capacity, and aura reduction never
  stacks beyond one point.
- Target decisions consider distance, usable range, armor, support value,
  health, clusters, nearby guards and threats to allied carries. Backline
  troops retain firing distance; assassins use legal, occupied-tile-aware
  flank landings. Detection and leap/burrow cooldowns constrain ambushes.
- Blood Chain's duplicate-unit requirement became Corrosive Chain Reaction
  (BloodBurst + Spitter). Combo partners must belong to the locked deck and
  actually be present. Fourteen combinations and all 64 specialization paths
  remain. Secondary damage has a shared per-attack budget and cannot recurse.
- Attack priority alternates between starting sides. Mirrored spawn placement
  and movement/target tie breaks remove directional preference while preserving
  deterministic replay and smooth presentation between simulation updates.

## Recorded production-engine sample

270 battles: nine matchups, ranks I/II/III, five seeds, both starting seats,
all five formations, and seeded alternative specialization paths. Decks contain
five distinct same-faction types; deployed duplicate copies still consume slots.

| Matchup (composition A vs B) | A wins, I | A wins, II | A wins, III |
| --- | ---: | ---: | ---: |
| Iron Vanguard vs Arcane Tempest | 2/10 | 7/10 | 6/10 |
| Iron Vanguard vs Blight Swarm | 8/10 | 3/10 | 5/10 |
| Arcane Tempest vs Blight Swarm | 8/10 | 5/10 | 7/10 |
| Balanced Arcane Tempest vs three tanks + two supports | 10/10 | 10/10 | 9/10 |
| Protected precision vs three tanks + two supports | 10/10 | 10/10 | 10/10 |
| Rocket battery vs concentrated brood | 10/10 | 10/10 | 10/10 |
| Ambush pressure vs exposed ranged line | 10/10 | 10/10 | 9/10 |
| Corrosive attrition vs three tanks + two supports | 10/10 | 10/10 | 10/10 |
| Magic without a frontline vs three tanks + two supports | 6/10 | 4/10 | 0/10 |

Peak live bodies were **five per side**, including summons and illusions.
Seat zero won **53.3%** overall and **51.1%** of starter matchups. Mean measured
engine time was **71.54 ms per complete battle**, with 19.37 seconds total for
the matrix on the development machine; this is a reproducibility sample, not a
cross-device performance promise.

Starter aggregates are 50% Vanir wins against Aesir, 53% Vanir against Helions,
and 67% Aesir against Helions. Rank-specific results remain composition- and
formation-sensitive. Concentrated swarm and unprotected gunline stress cases
intentionally lose heavily to their dedicated answers. Conversely, an upgraded
ward/healing stack can defeat an unsupported magical team, while a balanced
team with the same faction's control, frontline and combo partners wins 29/30.
Counterplay therefore depends on composition and protection together.

The sample covers representative extreme and starter matchups, rather than all
possible decks or permanent collection combinations. Regression tests also run
all 64 evolution paths at both ranks II and III through the actual simulator,
verify deterministic replay, real penetration and control, mirrored legal
formations, limited interception, shield consumption, combo eligibility, and
the shared five-body limit.
