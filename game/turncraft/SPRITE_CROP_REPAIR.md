# Sprite crop repair

The production troop sheets do not consistently follow their advertised grid.
Both horizontal spacing and directional row spacing drift, particularly in the
Brood artwork. A nominal cell can cut off its troop and include a neighbor's body,
head, limb or weapon effect. Some sheets also contain fewer complete poses than
their advertised animation frame count.

The animation manifest measures all 127 troop sheets: 8,000 frame slots covering
every action and all eight directions for all 40 troop types, including separate
worker and deployment sheets. It preserves the original five troops' sixteen
manually repaired sheets. Source SHA-256 values identify the existing production
Cloudflare files; no new image uploads are needed.

| Faction | Troops covered |
| --- | --- |
| Frontier (14) | Engineer, Rifleman, Medic, Marauder, Reaper, Hellion, Siege Tank, Goliath, Wraith, Dropship, Battlecruiser, Suppressor Exosuit, Pulse Operative, Forward Fabricator |
| Ascendant (13) | Probe, Zealot, Dragoon, Sentinel, Sentry, Templar, Immortal, Disruptor, Phoenix, Carrier, Chronoweaver, Lattice Skiff, Void Lancer |
| Brood (13) | Drone, Zergling Pair, Roach, Hydralisk, Ravager, Lurker, Mutalisk, Defiler, Ultralisk, Tunnel Maw, Sporeback, Broodling, Brood Matriarch |

TextureCache repacks measured source rectangles once after decoding, before
quality downsampling. Each body occupies a transparent cell with gutters.
`destinationOffsets[row][column]` places a crop independently of its source row,
so a body can cross a nominal row boundary without including another direction.
`sourceFrameWidth` and `sourceFrameHeight` preserve native pixel scale when the
repaired cell needs extra horizontal or vertical padding. Older horizontal-only
repairs remain compatible.

The renderer centers additional vertical padding around the original nominal
cell. Ground/air anchors and logical hit bounds retain their existing values.
Static atlas previews use the same repaired source rectangles. Repaired canvases
participate in the existing texture budget, eviction and quality invalidation;
runtime drawing needs no cross-origin canvas pixel reads.

Ambiguous, clipped or beam-connected poses reuse an intact pose from the same
action and direction when available. If an entire action's source bodies are
clipped, an intact pose facing the same direction keeps the full body visible.
Death slots cannot borrow a living ability pose. These
substitutions preserve frame counts, animation clocks and gameplay timers.
Anatomy already clipped out of a source sheet cannot be recovered by a crop.
Troop facing now draws one pose immediately, retaining directional hysteresis
while removing the old two-body turn crossfade.

`scripts/verify-animation-assets.py` checks source identities, coverage, crop
bounds, placement gutters and cleanup rectangles. Runtime tests cover vertical
row recovery, native scale, pivots, hit bounds, downsampling, single-body turns
and safe fallback. Browser tests inspect every repaired frame at all three
graphics qualities and exercise real movement, attacks, worker activity,
deployment, death, previews and large desktop/mobile battles.
