# Lead the Battle: offline battlefield expansion

Offline land encounters now pause when armies meet and offer **Resolve Battle** or **Enter Battle**. Orders still resolve at turn end. The campaign menu offers **Ask before battles** (default) or **Resolve automatically**. Online campaigns, online disconnections, naval combat, and amphibious resolution retain their existing paths.

The encounter shows the location, defenses, armies and commanders, visible opposition, nearby support, and a clearly labeled strategic casualty estimate. Closing it leaves a **Battle pending** button and keeps campaign orders frozen. An empty settlement still follows ordinary capture rules without manufacturing defenders.

## Playing

Enter Battle opens deployment. Select troop families or a particular player army, choose a formation, and drag on the ground to place the group and set its facing. Deployment stays within the starting area. Begin Battle switches to third-person commander control.

| Control | Action |
| --- | --- |
| WASD / movement stick | Move the counted commander |
| Mouse / swipe | Aim and camera |
| Left click / Attack | Attack or shoot |
| Right click / Block | Directional defense |
| Space / Heavy | Heavy attack with stamina cost |
| Shift / Sprint | Sprint |
| E / Interact / Mount | Interact with a ladder or mount/dismount an existing mounted troop |
| Wheel / weapon selector | Change to an available battle weapon |
| Q / Orders | Formation commands |
| Tab / Tactical view | Elevated command view |
| A in tactical view | Attack nearest |
| Escape / Pause | Pause, save and exit, retreat, or finish automatically |

Tactical view pauses combat by default; slow motion and real time are optional. In real time, the commander follows formation orders while the player commands. A knocked-out commander stays knocked out; surviving troops remain controllable. Ranged loadouts require a ranged commander, and lances require a mounted commander. Weapon changes never refill a quiver or add troops.

Orders cover move/facing, hold, defend walls, advance, charge, follow, attack a selected enemy army, attack nearest, fire/hold fire, withdrawal and retreat. Defend walls places infantry and ranged troops on intact parapets during deployment; during combat they walk to the inside stairs and climb. Leaving a wall uses the same stairs. Elevated archers can shoot down over the parapet while ground troops need a route up to engage them. Formation spacing and behavior implement balanced, defensive line, spear wall, aggressive charge, skirmish and flanking. Allies accept or decline broad requests and retain their own command.

## Campaign and recovery contract

`offline-encounters.mjs` is the campaign boundary. It snapshots exact rosters, commander identities, defenses, approach directions, seed and support decisions. `offlineTurn` records the movement phase after AI planning and fleet movement. Returning from a battle resumes the established army order using existing resolved-turn markers; economy, construction and diplomatic timers do not advance inside a battlefield.

Support uses a bounded breadth-first walk through legal land neighbors and access rules, up to two steps. Military relationships and existing wars must entitle an army to participate. Trade or non-aggression alone does not qualify. Committed armies cannot spend movement again; their incompatible orders are cleared. Surviving support normally stays on its source tile. Arrival timing is 0/25/50 seconds for zero/one/two steps, from the corresponding edge.

`battle-sim.mjs` is deterministic and independent of the DOM. Every soldier retains its army/type identity. A rider and horse count once; engine crews are visual parts of the engine. The active deployment budget is distributed proportionally across sides and compositions, with remaining troops preserved in reserves. Victory checks remaining commitments, escaped units and reserves. Defenses begin at their saved integrity; gates and breached sections change navigation and persist back to the campaign.

The result validates every army before application. For each army and type, starting = surviving + lost. Escaped and undeployed survivors are disjoint subsets of survivors. Capture requires an active surviving occupying force and no hostile army still contesting the tile. The existing capture and political-history rules are reused. Manual damage replaces incidental assault damage; casualties are not fed through the original resolver again.

The UI saves a single campaign snapshot containing the applied marker and all results before showing the report. Reopening that report cannot apply it twice. Checkpoints contain simulation RNG, attack phases, missiles, ammunition, morale, reserve arrival state and fortification damage. Save and Exit, refresh, graphics failure, and Finish Automatically preserve that progress. Automatic completion runs in a separate worker so the interface remains responsive; failure retains the last saved checkpoint. Save failures pause play and explain that progress could not be persisted.

## Artwork and delivery

The renderer loads only on entering combat. The included articulated models cover the complete troop catalog and all twelve House emblems, with horses, weapons, engine crew, effects and audio. Browser Cache Storage can predownload the model package from campaign settings. The runtime uses the local package and optionally an R2 mirror; no remote upload is required to play this change. See [BATTLE_ASSETS.md](BATTLE_ASSETS.md) for the reusable glTF/animation/texture/sound package and existing remote-asset workflow.

## Validation and limits

Run `npm run test:iron-throne:battle` for tactical, accounting, recovery and asset tests; run `npm run test:iron-throne:battle-browser` for the campaign loop and WebGL checks. The browser suite supports the same optional `IRON_THRONE_CHROMIUM` and `CODEX_PRIMARY_RUNTIME_NODE_MODULES` settings as existing tests. `IRON_BATTLE_SCREENSHOTS` optionally records review images outside the repository.

Regression coverage includes legal support, online exclusion, defended and empty settlements, multi-defender capture, no extra troops, fractional integrity conversion to campaign save format, exact resumed turns, duplicate outcomes, checkpoint corruption, knockout, finite ammunition, physical defenses, breach navigation, and automatic continuation. The full interface is exercised at desktop and mobile viewport sizes, including save/reload during combat and after results. Real-device performance and combat balance still need playtesting; viewport and software-renderer tests do not establish a mobile frame-rate guarantee.
