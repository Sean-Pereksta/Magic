# Lead the Battle: offline battlefield expansion

Offline land encounters now pause when armies meet and offer **Resolve Battle** or **Enter Battle**. Orders still resolve at turn end. The campaign menu offers **Ask before battles** (default) or **Resolve automatically**. Online campaigns, online disconnections, naval combat, and amphibious resolution retain their existing paths.

The encounter shows the location, defenses, armies and commanders, visible opposition, nearby support, and a clearly labeled strategic casualty estimate. Closing it leaves a **Battle pending** button and keeps campaign orders frozen. An empty settlement still follows ordinary capture rules without manufacturing defenders.

## Custom Battle

Choose **Custom Battle** on the opening screen. **West Wall Assault** fields 72 soldiers and one ram against 48 defenders, including 16 archers. Send infantry to a ladder with **L**, or select infantry, press **B**, and choose the ram and its target. The ram has 2,400 HP. Four living infantry can roll it into position. Six push faster and recover sooner between blows. A section collapses where the head strikes, opening a route into the fort.

**Royal City Siege** fields 240 attacking soldiers and two wheeled rams against 200 defenders. The royal citadel has separate outer and inner walls, a chapel, a keep and defenders in both wards. Select **Defenses** to try an open market town, a timber stockade or palisade, a stone fort, or a larger upgraded city. Campaign battles use the actual settlement, fort and wall upgrades on the tile.

Both sides have editable rosters. Their deployment previews show how many soldiers and engines will fight immediately and how many will arrive in waves. **Deploy First** lets you position formations and assign a ram crew. **Fight Now** starts immediately. At the end you return to setup. **Rematch** restores the starting deployment and **New Field** changes the seed. Custom battles use their own in-memory state.

## Playing

Enter Battle opens deployment. Attackers and defenders begin in separate zones at opposite ends of the field, facing each other, even when their campaign approach directions coincide. Fort defenders start inside their defenses. Zone outlines show the starting areas; placement checks terrain and occupied space so troops cannot be stacked on another formation or placed among opponents. Select troop families or a particular player army, choose a formation, and drag on the ground to place the group and set its facing. Archers, crossbows and cavalry have separate selection cards with deployed and reserve counts. Choosing a formation immediately rearranges the selected troops. Begin Battle switches to third-person commander control.

Choose **Troops on the field** in campaign settings, at a new encounter, or in Custom Battle. Both armies share the selected limit of 100, 150, 200, 300, or 400 active troops. Extra soldiers arrive in waves. New campaigns default to 300. Existing campaigns keep their chosen limit. Saved battles retain their starting limit.

Save a selection under **Custom command groups** to name and recall its exact soldiers, including reserves. Up to twelve groups persist with the battlefield checkpoint; recalling a group selects its surviving members. Orders affect only the selected group. Line, column, wedge and square layouts complement the existing tactical formations.

| Control | Action |
| --- | --- |
| WASD / movement stick | Move the counted commander; WASD pans the tactical camera |
| Mouse / swipe | Aim and camera |
| Left click / Attack | Attack or shoot |
| Right click / Block | Directional defense |
| Space / Heavy | Heavy attack with stamina cost |
| Shift / Sprint | Sprint |
| E / Interact / Mount | Interact with a ladder or mount/dismount an existing mounted troop |
| Wheel / weapon selector | Change to an available battle weapon |
| 1 / 2 / 3 / 4 / 5 / 6 / 7 | Select all troops / infantry / spearmen / archers / crossbows / cavalry / siege |
| 8 / 9 / 0 | Select the first / second / third saved command group |
| F1 | Hold here: selected troops stop at their current positions |
| F2 | Follow the commander |
| F3 | Charge |
| F4 | Advance |
| F5 | Toggle fire / hold fire for the selection |
| F6 | Retreat the selected troops |
| G | Move the selection to the ground under the aim point |
| L | Assault a ladder |
| B | Assign infantry to the ram and choose a gate or exterior wall |
| V | Defend a wall |
| Q / Orders | Expand or collapse formation commands in the current camera |
| H / Hide UI / Show UI | Hide or restore the battlefield controls. Orders and movement still work while the controls are hidden. |
| Tab / Tactical view | Elevated command view |
| Escape / Pause | Cancel targeting, otherwise pause |

The compact on-field command guide shows the current selection and acknowledges orders. Number keys and function-key orders work directly in commander and tactical views without opening the orders panel, switching cameras, or changing time mode. Orders work while sprinting. Hotkeys respect text entry, browser shortcuts, and pause. Numbered troop selection covers your own armies; saved-group keys retain exact membership. Hold here cancels charge momentum while allowing nearby defensive attacks; reserves given this order hold at their entry position when they arrive.

Adjust **Mouse look sensitivity** in the campaign menu or battle pause panel, from 0.5× through 4×. A posted archer gains 10 accuracy points on an intact wall walk. Individual training and weapon type affect aim, and moving targets can dodge an arrow after it leaves the bow.

Tactical view runs in real time by default. Slow motion and pause remain available in the command panel. In real time, the commander follows formation orders while the player commands. A knocked-out commander stays knocked out; surviving troops remain controllable. Ranged loadouts require a ranged commander, and lances require a mounted commander. Weapon changes never refill a quiver or add troops.

Orders cover move/facing, hold, defend walls, advance, charge, follow, attack a selected enemy army, attack nearest, fire/hold fire, withdrawal and retreat. Defend walls places infantry and ranged troops on intact parapets during deployment; during combat they walk to the inside stairs and climb. Leaving a wall uses the same stairs. Elevated archers can shoot down over the parapet while ground troops need a route up to engage them. Formation spacing and behavior implement balanced, defensive line, spear wall, aggressive charge, skirmish and flanking. Allies accept or decline broad requests and retain their own command.

Engaged troops face their opponents and commit to a visible strike through contact and recovery. Explicit facing, movement and retreat orders take precedence. Weapon poses follow the simulation's strike timing. Confirmed damaging hits produce red blood particles near the camera; blocked attacks and impacts on defenses do not. Fallen troops remain on the field, with a separate bounded rendering budget for corpse detail.

## Campaign and recovery contract

`offline-encounters.mjs` is the campaign boundary. It snapshots exact rosters, commander identities, defenses, approach directions, seed and support decisions. `offlineTurn` records the movement phase after AI planning and fleet movement. Returning from a battle resumes the established army order using existing resolved-turn markers; economy, construction and diplomatic timers do not advance inside a battlefield.

Support uses a bounded breadth-first walk through legal land neighbors and access rules, up to two steps. Military relationships and existing wars must entitle an army to participate. Trade or non-aggression alone does not qualify. Committed armies cannot spend movement again; their incompatible orders are cleared. Surviving support normally stays on its source tile. Arrival timing is 0/25/50 seconds for zero/one/two steps. Tactical approach lanes retain the approach's flank while keeping each side's entry on its own end of the map; occupied entry points leave arrivals safely in reserve.

`battle-sim.mjs` is deterministic and independent of the DOM. Every soldier retains its army/type identity. A rider and horse count once. Ram pushers are individual infantry from the army roster. The ram stops when fewer than four remain at its grips. At most **400 troops total** are active, including the commander, with a new-campaign default limit of **300**. The deployment budget is distributed proportionally across sides and compositions, with remaining troops preserved in reserves. Reserves enter from legal approach edges in eight-second waves as space opens. Newly arriving support can enter at its scheduled arrival if space is available. Fallen troops do not use active slots. Victory checks remaining commitments, escaped units and reserves. Defenses begin at their saved integrity; gates and breached sections change navigation and persist back to the campaign.

The result validates every army before application. For each army and type, starting = surviving + lost. Escaped and undeployed survivors are disjoint subsets of survivors. Capture requires an active surviving occupying force and no hostile army still contesting the tile. The existing capture and political-history rules are reused. Manual damage replaces incidental assault damage; casualties are not fed through the original resolver again.

The battle closes automatically when the outcome is final. A single save records the survivors, casualties, capture, wall damage and cleared encounter. The map resumes the interrupted turn. View Report opens the latest result from the map banner. Checkpoints contain simulation RNG, attack phases, missiles, ammunition, morale, reserve arrival state and fortification damage. Save and Exit, refresh, graphics failure, and Finish Automatically preserve that progress. Automatic completion runs in a separate worker so the interface remains responsive; failure retains the last saved checkpoint. Save failures pause play and explain that progress could not be persisted.

Older checkpoints exceeding the 200-troop ceiling move overflow troops into reserves while preserving identity, health, ammunition, morale and casualties. Existing lower limits remain unchanged. The commander remains deployed when already active, and returning reserves keep their prior orders.

Unsafe overlapping or misplaced deployment saves are repaired within the corresponding starting zones without changing troop identities or health. Loading a battle already in combat preserves current positions; only future reinforcement entry lanes are corrected.

## Artwork and delivery

The renderer loads only on entering combat. The included articulated models cover the complete troop catalog and all twelve House emblems, with horses, weapons, engine crew, effects and audio. Enter Battle automatically loads the package from the configured Cloudflare R2 bucket and caches it for offline play; bundled assets remain available if the remote package cannot load. Campaign settings can also predownload the package. See [BATTLE_ASSETS.md](BATTLE_ASSETS.md) for the reusable glTF/animation/texture/sound package and existing remote-asset workflow.

## Validation and limits

Run `npm run test:iron-throne:battle` for tactical, accounting, recovery and asset tests; run `npm run test:iron-throne:battle-browser` for the campaign loop and WebGL checks. The browser suite supports the same optional `IRON_THRONE_CHROMIUM` and `CODEX_PRIMARY_RUNTIME_NODE_MODULES` settings as existing tests. `IRON_BATTLE_SCREENSHOTS` optionally records review images outside the repository.

Large battles share static scenery and route data. The renderer uses less animation detail for distant troops, and nearby soldiers retain their weapon poses. Active troop limits can be lowered independently of both army rosters.

Regression coverage includes legal support, online exclusion, defended and empty settlements, multi-defender capture, no extra troops, fractional integrity conversion to campaign save format, exact resumed turns, duplicate outcomes, checkpoint corruption, knockout, finite ammunition, physical defenses, breach navigation, and automatic continuation. The full interface is exercised at desktop and mobile viewport sizes, including save/reload during combat and after results. Real-device performance and combat balance still need playtesting; viewport and software-renderer tests do not establish a mobile frame-rate guarantee.
