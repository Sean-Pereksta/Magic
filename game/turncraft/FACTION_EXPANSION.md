# TurnCraft faction expansion

The expansion remains inside `../turncraft.html`, using the existing simulation, economy, research, doctrine, artwork loader, commands and controller-owned multiplayer state. No runtime dependency or build step is added. The map chooser uses `map-popout.css` and the existing six map definitions and terrain preview renderer.

| Faction | Unit | Production / technology | Cost (minerals / gas) | Supply | New tactical role |
| --- | --- | --- | --- | --- | --- |
| Frontier | Suppressor Exosuit | Factory | 150 / 100 | 2 | Four-tile cone suppresses ground attack recovery |
| Frontier | Pulse Operative | Barracks / Tactical Academy | 125 / 125 | 2 | Shield drain and temporary ability lock |
| Frontier | Forward Fabricator | Factory / Tactical Academy | 225 / 125 | 3 | Deployable forward Rifleman and Medic production |
| Ascendant | Lattice Skiff | Stargate / Forge | 175 / 150 | 2 | Pulls two ground reinforcements from a friendly Pylon |
| Ascendant | Chronoweaver | Gateway / Psionic Citadel | 125 / 175 | 2 | Temporal field slows attack and ability recovery |
| Ascendant | Void Lancer | Robotics Bay / Psionic Citadel | 200 / 175 | 3 | Shield-consuming charged anti-structure siege beam |
| Brood | Tunnel Maw | Larva / Hive | 200 / 125 | 3 | Temporary, destructible underground invasion route |
| Brood | Sporeback | Larva / Hydra Den + Evolution Nest | 150 / 100 | 2 | Air-only splash and persistent air-denial cloud |
| Brood | Brood Matriarch | Larva / Hive | 200 / 150 | 3 | Trades health for a temporary swarm |

## Controls

Select a new unit to see its ability and existing ordinary orders. The normal ability buttons and displayed keyboard shortcuts work for individual and group selections. Target previews show the affected cone, radius, structure, or Pylon reinforcement/landing plan. Disruption and charging states appear on the battlefield and command buttons; health and shield costs appear beside the applicable abilities.

Deploy a Fabricator using Field Muster, then select its training cards. Rally and Cancel Last work through the same command transport as ordinary production. Pack Up refunds 75% of unfinished queued orders at their paid cost, matching ordinary queue cancellation. Movement is unavailable while deployed; packing restores it.

A Tunnel Maw must stand on friendly creep. Target a visible open tile within eight tiles and wait three seconds. Select eligible combat troops next to the Maw and choose **Enter Tunnel**. The exit can be attacked; its destruction, expiration, five completed passages, or source destruction closes the connection. Stop/Hold interrupts an unfinished channel.

Click **Select Battlefield** to open the six-map comparison dialog. A selection updates and saves the menu immediately. Escape, the close button or backdrop closes it. Ready submission locks the map vote, including reconnects; majority and first-ready tie resolution are unchanged.

## Balance and lifecycle decisions

- Existing global game pacing and applicable research/doctrine modifiers still apply. Direct channel and field durations are stated in ability descriptions.
- Suppression and temporal recovery slow elapsed attack cooldown recovery to 70%; temporal also affects shared ability recovery. Their strongest penalty applies once, including to timers already running. Neither slows movement.
- Disruption drains at most 50 shields without health damage. A four-second ability lock grants immunity to another lock until nine seconds after the initial pulse. Ordinary attacks, movement, and production remain available.
- Void Spear spends 70 shields when charging begins. Interruption keeps the shield cost and cooldown, preventing cancellation/recast abuse. Its structure hit deals 75 damage after the visible two-second charge and beam impact; temporary barriers receive 40.
- One Fabricator per player, including unfinished orders. Field production costs 20% more and takes 25% longer than normal training, reserves supply and allows three queued items. Destroying the platform loses its unfinished queue.
- Warp and tunnel landings reject occupied or blocked tiles, resource nodes, protected objectives, and proximity within three tiles of an enemy command base. Workers, aircraft, massive troops, Relic carriers and temporary summons cannot travel.
- Tunnel Maws allow at most two active or digging connections per player, 15 seconds per exit and five passages. Individual or group commands move existing units to distinct validated landing tiles.
- Blood Clutch creates three Broodlings atomically only when three adjacent spaces and room under the six-summon cap are available. It costs 25 health and cannot kill its source. Broodlings last ten seconds and disappear on source death. They have no supply/resource refund, training, morphing, garrison, transport, gathering, or objective privileges.
- Sporeback ground damage is two. Its direct splash affects aircraft only. Overlapping Spore Bloom fields apply one six-damage-per-second rate, totaling 30 over five seconds.

AI composition uses visible opponents, existing queued commitments and army size. All difficulties use the shared validated abilities within their existing decision/command budgets; higher difficulty supplies more frequent decisions. Combat casts prioritize relevant clusters/structures, logistics chooses valid staging and arrival sites, and Fabricators reinforce safe pushes. Combat effects settle once per frame or affected moving/firing unit, avoiding army-wide rescans per shot.

## Verification

`npm test` in this directory runs the real game code under an isolated simulation harness and headless browser fixtures. `npm run test:warfare` runs the existing warfare scenarios. GitHub Actions installs Chromium and runs both commands for relevant changes. Set `CHROMIUM_EXECUTABLE_PATH` to reuse a local compatible Chrome/Chromium executable.

Checks cover all nine production paths, costs, supply, prerequisites, ownership, standard orders, research, refunds, unique abilities, interruptions, caps, safe placement, temporary cleanup, objective exclusions, adaptive AI, snapshot round trips and command deduplication. Browser checks cover menu interactions, focus, desktop/mobile geometry and command controls. Test network fixtures isolate Firebase: a live multi-client lobby and physical-device performance still require a separate playtest.

Original transparent unit assets and generation prompts are listed in `ASSET_EXPANSION.md`.
