# Tier 2 and Tier 3 warfare

The warfare expansion introduced six specialists, two production buildings and two additional battlefields. The Frontiers update now expands all seven maps to 9600 × 6400; see [Frontiers](FRONTIERS.md) for the map dropdown, rival strategies and Impossible resource rules. The original maps, economy, upgrades, fog, formations, groups, mouse defenses, and CD transports remain available.

## Production

Twigs construct the **Prototype Workshop** for **275 trash / 16 seconds**, after an operational Printer. The **Shredder Laboratory** costs **400 trash / 20 seconds** and requires both a completed Workshop and operational Printer. Multiple production buildings can operate at once, each with a six-order queue and rally point.

| Building | Troop | Family | Trash | HP | Supply | Training |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Workshop | Scissors Sniper | Scissors | 150 | 150 | 2 | 11s |
| Workshop | Paper Airplane | Paper | 130 | 140 | 2 | 9s |
| Workshop | Rock Mage | Rock | 170 | 200 | 2 | 12s |
| Laboratory | Paper Divider | Paper | 500 | 280 | 6 | 25s |
| Laboratory | Rolling Rock | Rock | 350 | 640 | 4 | 16s |
| Laboratory | Shadow Shears | Scissors | 325 | 300 | 3 | 15s |

Supply includes every queued unit at its full weight. A CD has six passenger seats: Tier 1 and Tier 2 ground troops may board; workers, airplanes, and Tier 3 troops cannot.

## Fortifications update

Mouse towers now have **850 HP**, **360 range**, and fire **28 damage** every **1.25s**, with **6 damage/s for 3s** after impact. Cost remains **175 trash**.

Twigs build **Wall blocks (Z)** for **20 trash**, with **1,200 HP** and a **3s** build time. The 48px grid connects square blocks into sealed ground barriers. Repeat click/tap to extend; Done placing or Esc ends placement while Twigs finish their queued jobs. Dismantle an owned block to open a passage with no refund. Aircraft and projectiles pass over the walls. Paper Airplanes now have **900px scouting vision**.

## Tactical pacing

Tier 1 Rock, Paper and Scissors have **180 HP**. Flying CDs have **320 HP**. Basic attack damage, attack cadence and counter restrictions remain intact, so favorable basic duels take four hits. Sweep deals **135 damage** instead of executing a whole Paper squad. Precision Snipe retains its single-target execution after a visible, interruptible **1.5s** aim.

## Commands

Select a Twig and use **N** to place a Workshop or **O** to place a Laboratory. At either production building, **Q / W / E** train the three troops in the table's order. Right-click a destination to set its rally point.

| Key | Ability | Use |
| --- | --- | --- |
| F | Precision Snipe | Click a visible enemy Paper troop; approach within 700 pixels, then aim for 1.5s. 18s cooldown. Cover, fog, or Crumple interrupts the shot. |
| I | Intercept | Click an enemy aircraft the Paper family can attack. |
| J | Patrol | Tap two or more waypoints, then Finish route / Enter. |
| K | Crash Landing | Choose an area; each selected airplane receives a separate impact point. 145 center damage, 75-pixel splash; consumes the plane. |
| G | Force Field Dome | Choose a location; approach within 420 pixels and sight. 80-pixel dome pushes ground troops outside its footprint and lasts 10s. Each mage has two charges per five-second recharge cycle. |
| T | Triple Division | Choose a location; approach within 600 pixels and sight; 1.5s cast, 350-pixel radius, 9s containment, 38s cooldown. |
| Y | Crushing Roll | Choose a destination up to 750 pixels away. Parallel lanes, 1s acceleration to 300px/s, 130 first contact / 40 repeated contact, 17s cooldown. |
| H | Shadow Path | Drag and release, or tap waypoints then Finish route / Enter. 900-pixel route, 650px/s, 115 damage once per target, 18s cooldown. |

Snipe, Force Field and Triple Division select **one nearest eligible caster per click** and remain armed for additional clicks. Intercept, Crash, Roll and Shadow Path apply to the ready selected group. Crash points and dash/roll lanes spread across formations. **Esc** cancels targeting before opening the menu; **X** clears selection. Far Force Field and Triple Division targets queue movement into range and sight before casting; a distant visible Snipe target queues approach before the 1.5s aim. Charges and cooldowns start only when the cast begins. Stop or a new command cancels a pending cast. **Stop / S** cancels a crash before its final dive. Once diving, the airplane is committed.

Square icon tiles provide touch equivalents, with clear keycaps and ready/recharge status. **A**, then either left- or right-click chooses an attack-move destination; the Attack tile follows the same flow. **F2** selects the fielded army. **Ctrl+1–4** or holding a group number assigns troops and/or buildings; tap to recall and double-tap to center. Shadow Path supports finger drawing. Original Q/W/E abilities, Shift/box selection, groups 1–4, minimap commands, attack-move, camera navigation and fullscreen battlefield controls retain their original behavior.

## Families and barriers

Rock damages Rock/Scissors, Paper damages Paper/Rock, and Scissors damages Scissors/Paper at every tier. Every splash/dash/roll victim is checked independently. Crumple still protects Paper. Control spells affect both sides and all families without dealing damage.

Domes push ground troops of either team beyond their smaller 80-pixel footprint, then block ground travel through their interior. Pushes respect walls, terrain, other fields and troop footprints. Aircraft, projectiles and vision pass over/through ordinary domes.

Triple Division sorts ground and airborne family troops into equal Rock/Paper/Scissors sectors using continuous displacement through reachable terrain. CDs are excluded. Terrain, building, body and wall clearances determine valid destinations; unreachable terrain pockets cannot teleport through obstacles. Containment walls block movement and attacks. Expiring barriers invalidate route caches and release the original orders.

## Maps

**Stapler Canyon** has a narrow northern ruler bridge, winding southern crossing, raised book/cardboard lookouts with ramps, and a neutral 1800-HP Stapler Gate. Destroying the gate permanently opens the center. Flying units cross the chasm directly.

**Conveyor Chaos** has a large outer loop, two central crossing belts, and side transfer belts. Traveling with a belt is 45% faster; against it is 25% slower; crossing produces slight drift. Aircraft ignore belts. Ground combat troops capture control stations after four uncontested seconds nearby. A captured station reverses its segment for 18s, with 24s recharge. Select the station to trigger a reversal when ready. Arrows and treads reverse together.

All AI difficulties buy the new buildings and troops with their own resources and weighted supply. Specialists use their abilities, bots scout crossings, attack visible gates, and capture discovered conveyor stations.

## Artwork and animation

The six painted troop designs use the existing Rock, Paper and Scissors illustrations as style references. Transparent character sheets contain sixteen frames each: four idle, four move, four attack, and four special poses. The renderer selects actual frames from state and timing, then adds heading/banking, recoil, shadows and trails. The VFX sheet contains six effect rows with three chronological frames each. Precise dome/division boundaries remain resolution-independent animated Canvas geometry. Artwork caches are shared across troops.

See [ARTWORK.md](ARTWORK.md), [animation metadata](assets/warfare-animation.json), and the [animated gallery](warfare-gallery.html).

## Validation

Run `npm test` for simulation, family/cost/supply, abilities, dynamic collision, map, transport, control, and rendering regressions. Run `npm run test:browser` with Playwright available for real desktop/mobile rendering and controls. `PLAYWRIGHT_MODULE`, `BROWSER_PATH`, `GAME_URL`, and `ARTIFACT_DIR` can point the browser check at existing local tools and output directories.
