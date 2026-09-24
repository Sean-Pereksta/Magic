# Regional worlds and founding

New campaigns use six map profiles: Heartlands, Great Divide, Highland Crown,
Verdant Kingdoms, Great Basin and Broken Coast. The default is Random map.
Online lobbies receive a fresh seed; profile selection and all generation retries
are deterministic from that seed. Selecting an explicit profile takes priority.
The campaign stores the original seed, resolved profile, generator version,
successful retry, geographic regions, terrain, deposits and rivers.

The generator builds broad biomes, coherent clearings, winding mountain spines,
planned passes and foothills before tracing highland rivers to the coast or basin.
Resource quality follows geography, never ownership. New-world production does
not use the legacy House industry multipliers. Directional river/coast artwork
continues to use the existing geography renderer.

Before a world is displayed, validation requires connected passable land,
sufficient terrain and minerals, sizable connected forest/plains/hill/mountain
regions, and a feasible set of all six starting packages at least eight hexes
apart. Failed worlds retry deterministically without carving corrective corridors.

## Found your kingdom

A new campaign starts at turn zero in `phase: 'founding'`. Click or keyboard-select
a tile to see Food, Wood, Stone and Iron outlooks for the surrounding four hexes,
terrain prevalence, and the planned starter structure locations. Confirming
**FOUND CITY** establishes the permanent starting capital. Gameplay orders,
round timers, economy and victory do not run during founding.

The shared legality check rejects water, mountains, occupied sites, foreign
territory, capitals at distance **less than 8**, and sites whose full starter
package cannot fit. Distance **exactly 8 is allowed**. The check also requires
that the remaining Houses can still fit; the witness positions are not reserved.
This prevents otherwise attractive human choices from stranding the final House.
The minimum distance never changes.

Starting influence follows usable land up to four hexes. Structures use nearest
legal reachable tiles within that radius; scarce deposits are allocated first.
All starter roads form connected paths. No terrain, river, deposit or quality is
changed, including underneath the city. A site needing structures beyond the
four-hex footprint is rejected instead of giving a remote, unowned producer.

Every House retains 80 population, its normal starting stockpile, barracks,
archery range, and its army of 20 levies, 6 archers and 2 cavalry. Each receives
farm/lumber/quarry/mine production. Existing extra starting structures are retained
(additional farms/lumber/mines/quarries/ranches; Sunspire and Vesper markets and
Vesper's workshop). These extras preserve the old starting inventory; no new
House-specific stockpile or deposit bonuses are introduced.

Humans confirm first. AI chooses from the remaining legal land using local food,
wood, minerals, expansion space, defense and diversity, with deterministic ties.
All six capitals must exist before **THE REALM IS FOUNDED** begins Turn 1.

## Firebase and saves

`found` is an authenticated command in the existing controller queue. Each accepted
claim writes canonical, public and private snapshots, metadata/version, and its
receipt in one Firestore transaction. A stale conflict cannot overwrite a committed
capital. A failed snapshot commit leaves its command pending for retry against the
current state. Each subsequent founding check uses the already committed world.
The public snapshot includes each House's founding status and capital; reconnects
restore these without regenerating the world. Round timers start after founding.

**Deploy the accompanying `firestore.rules` when deploying this change.** The new
rules allow turn-zero founding commands and reject ordinary orders during that
phase. No production Firebase deployment is performed by this pull request.
The existing client-controller trust model is unchanged (see MULTIPLAYER.md).

Legacy campaigns load their original tiles, buildings, resources, armies, territory
rules and industry bonuses. Loading never calls the generator. Schema 3 remains
supported; new-world/founding metadata is validated separately. Frozen legacy
fixtures keep the existing simulation regression suite exercising those saves.

## Modules and verification

- `map-profiles.mjs`: configuration and seeded profile selection.
- `world-generation.mjs`: regions, ranges/passes, rivers, resources and validation.
- `world-hex.mjs`: shared axial hex geometry and deterministic random streams.
- `founding.mjs`: site/footprint checks, outlook, AI selection, placement and phase.
- `founding-ui.mjs`: in-game outlook and founding controls.

Run `npm run test:iron-throne` for engine, save, geography and founding tests.
`npm run test:iron-throne:founding-browser` checks desktop and both mobile
orientations without generating previews. `npm run test:iron-throne:online`
checks actual Firestore rules, competing founding transactions, two- and
six-browser campaigns, reconnects and controller replacement using demo emulators.
