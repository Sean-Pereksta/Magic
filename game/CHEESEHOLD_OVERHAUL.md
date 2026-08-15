# Cheesehold Navigation, Enemy, Summoner, and Terrain Overhaul

Implementation scope for `Cheesehold_3D_Progression_Upgrade.html`:

- Keep a fixed camera orientation instead of rotating/flipping the arena when nearby walls obstruct the player.
- Preserve mouse readability behind walls with a high-contrast silhouette/occlusion treatment.
- Add substantially larger late-round arena layouts while retaining clear navigable spines and safe spawn zones.
- Make cat powers mechanically visible and distinct, especially converting Pounce from a fast grid move into a true airborne leap with anticipation, arc, landing impact, and damage timing.
- Expand enemy behavior beyond stat variations with specialist roles such as flankers, structure hunters, chargers, support units, ranged pressure, and terrain-aware hunters.
- Add three summoner-focused structure evolution paths that trade normal range/output for allied-unit production.
- Provide six allied minion archetypes across those paths, including frontline, skirmisher, ranged, support, siege, and elite/guardian roles.
- Add later-round terrain features appropriate to each arena theme, including movement hazards, sticky/slow zones, breakable or interactive obstacles, and resource opportunities.

This branch is reserved for the Cheesehold gameplay overhaul and is intended to keep all implementation work isolated from `main` until the full feature set has been validated.
