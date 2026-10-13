# Sprite crop repair

Hellion, Marauder, Sentinel, Siege Tank and Zealot artwork uses the advertised
96 × 144 grid dimensions, but the painted subjects are not consistently centered
in those cells. Some frame selections contain a neighboring wheel, limb, barrel,
or an additional part of a collapsed unit. The issue is in the packed artwork,
not the movement timers.

The animation manifest contains measured crops for their fifteen locomotion,
combat and demise sheets, plus Siege Tank deployment: 1,008 frame slots across
eight directions. The source SHA-256 values identify the existing production
Cloudflare files. No replacement uploads or GitHub image assets are needed.

TextureCache repacks each sheet once after decoding. Each measured subject is
centered at its native pixel scale in a transparent 192 × 144 cell. Specific
small disconnected edge scraps are erased using measured local rectangles.
Repacking happens before Low/Medium quality downsampling, so filtering samples
transparent gutters instead of another unit. The renderer compensates for the
wider cells horizontally; vertical scale, ground anchors, hit bounds, action
clocks, simulation data and frame counts remain unchanged. Repaired canvases
participate in the existing texture budget, eviction and quality invalidation.
Canvas drawing requires no cross-origin pixel access.

The original sheets include incomplete edge poses and occasionally fewer intact
subjects than their advertised frame count. Those slots reuse the closest usable
pose in the same directional sheet, keeping the existing animation duration.
This repair cannot recover anatomy or effects already cut off in the source;
fresh art would be needed to improve those individual poses further.

`python3 scripts/verify-animation-assets.py` checks source identity, every measured
crop, cleanup rectangle and gutter. Runtime tests cover repacking, scale, hit
bounds and safe fallback. The browser suite includes an all-frame gutter and
nonempty-frame check at all three graphics qualities.
