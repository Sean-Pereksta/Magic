# Living Kingdoms and Tactical Warfare

This expansion upgrades the existing browser game and shared deterministic engine. The original 14 classes, progression, contracts, inventory, castle ownership, player alliances, Firebase identity, elected host, command receipts, recovery, and local campaigns remain in place.

## Commanding

Press **A** with battlefield troops selected to attack accessible nearby enemies immediately. Targets are distributed across the selection and replaced when defeated. **Shift+A** and **Target attack** retain the destination/target workflow. World-map WASD remains unchanged. Touch players have the same one-tap **Attack nearest** button.

The compact **More tactics** drawer contains Skirmish, Brace, Flank, Fall back, Focus fire, and Regroup. Skirmish accepts a fallback marker; horse archers fire, pull away while reloading, and approach again. Mounted archers have finite arrows and stamina. Brace needs time, facing, and cohesive ranks. Moving or flanking a braced line removes its advantage.

Shields intercept actual projectile segments for troops behind them. Frontal protection depends on facing, shield condition, stamina, spacing, and formation. Rear attacks bypass that protection. Cavalry builds momentum over distance, suffers against prepared pikes and difficult ground, and regroups after an impact. Splash attacks have radius, falloff, obstruction, and unit-specific friendly fire. Crossbows retain armor penetration.

Battle commanders plan at bounded intervals. They separate screens, missiles, support, mounted harassment, and reserves; react to visible enemies, casualties, morale, cohesion, and terrain; and withdraw when the army breaks. They use the same orders and resource limits as the player.

## A larger continent

The seeded continent is **320 × 224** tiles with **80 settlements**: ten capitals, twenty castles, twenty towns, and thirty villages. The original settlements retain their IDs and coordinates. The original three factions are joined by Steppe Khanate, Stonehold Clans, Suncoast League, Frostbound Jarldoms, Desert Sultanate, Iron Covenant, and Marsh Freeholds.

Each kingdom has two signature units, a leader, doctrine, economic interests, and diplomatic personality. Recruit these troops in their homeland after gaining reputation or an appropriate agreement. Elite troops require commander rank and recruitment development. Suncoast mercenary contracts provide another route to foreign specialties. Recruitment buttons explain unmet conditions.

Named lords recruit, patrol, campaign, recover, and return from captivity. Merchants complete routes between actual markets. Bandits raid villages and vulnerable caravans. Distant activity uses bounded strategic turns and path budgets. Terrain, roads, navigation, and render chunks are regenerated locally; generated maps are never written to Firestore.

## Diplomacy and the economy

The council shows strength, wealth, exhaustion, holdings, enemies, agreements, signature units, and relations for each kingdom. Proposals use trust, reputation, military strength, wealth, mutual enemies, losses, and war score. Decisions show reasons; counteroffers have persistent IDs and expire. Accepting an offer cannot spend its gold twice.

Gifts, pacts, trade, passage, defensive and military alliances, aid, war, ceasefires, peace, prisoners, tribute, territory, and protection have persistent effects. Treaties expire. Breaking promises damages trust across the continent. Player alliances remain a separate system. AI kingdoms also make agreements, declare wars, form coalitions, and seek peace independently.

Completed caravan deliveries fund kingdoms and trade partners. Raids damage production and recruitment. Sieges and blockades reduce settlement supplies. Garrisons and local loyalty affect occupation and rebellion. Armies consume more supplies on hostile and desert expeditions. Alliance support can commit a real lord's company to a reinforcement battle; surviving allied soldiers return to that lord rather than becoming free player troops.

## Encounters and training

Field battles, ambushes, sieges, village raids, defensive holds, caravan escorts, rearguard withdrawals, and reinforcement battles use the same combat engine. Campaign encounter choices appear in settlement and army inspection panels. Escorting a merchant requires nearby raiders; allied relief requires a treaty and an available nearby lord.

**Tactical scenarios** on the main menu opens eleven playable exercises: shield cover, flanking, horse archers, splash, combined arms, pass defense, ambush, caravan escort, supply raid, rearguard, and arriving reinforcements. Practice does not alter the saved campaign. Marked objective zones, wagon health, supply stores, holding progress, and withdrawal requirements can decide a battle before every enemy is eliminated.

## Saves, synchronization, and deployment

Profile schema remains version 2. Loading an older campaign overlays its holdings and companies onto the expanded generated continent. Added armies, factions, diplomacy, and kingdom state receive defaults. Original troop snapshot tuples retain their ten fields; tactical status is a separate compact array. Strategic checkpoints include mutable state, seed, and bounded history, not terrain or navigation arrays.

Durable diplomacy uses the existing transaction and immutable command receipt, together with profiles and the realm checkpoint. Host failover restores kingdom state and unfinished encounters. Snapshot/checkpoint cadence and the existing 16-player/4,800-troop realm limits remain; pending allied waves reserve admission capacity.

Publish the matching static files and updated Mountain Blade Firestore rules together. The rules now accept expanded coordinates, the 34-class roster, and validated diplomacy fields. Existing rules for other games are unchanged. This change does not publish production rules or deploy the site automatically. See [README](README.md) for the existing Firebase Console deployment procedure and [QA](QA.md) for validation results.

## Validation

```sh
npm run test:mountonline
npm run bench:mountonline
npm run test:mountonline:browser
npm run test:mountonline:rules
```

The simulation suite includes quantitative shield, rear-attack, projectile, brace, horse-archer, splash, AI, objective, diplomacy, world, capture, trade, save, and recovery scenarios. Browser tests exercise the actual app on desktop, touch portrait, and touch landscape. Emulator tests use only `demo-mountonline`.

Physical-device performance and production Firebase deployment remain manual checks. Strategic distant-army outcomes are an approximation; long campaign balance, treaty pricing, and faction snowballing should receive continued playtesting.
