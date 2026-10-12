# Dedicated armor bodies

Every normal species has four independently illustrated body sheets: bare,
basic, reinforced, and heavy. Ironback, Shadowhand, and Emberwood each retain
their identity across six King looks: base, Age 1, Age 2, command, power, and
shadow. The package contains 42 source PNGs and 42 runtime WebP atlases
(13.74 MiB). The complete self-contained game is 97,475,659 bytes (92.96 MiB).

## Progression

| Follower armor | Maximum health | Protection | Movement | Unlock |
| --- | --- | --- | --- | --- |
| Basic | +15% | 8% | unchanged | first camp |
| Reinforced | +35% | 17% | −4% | 100 apes, workshop |
| Heavy | +60% | 27% | −12% | 200 apes, workshop |

Health bonuses apply once to the species/training/elite health total. Fitting
preserves the existing missing-health amount, including after repeated saves
and training. Broken shields continue to lose their weight and protection.

| King set | Maximum HP | Protection | Attack multiplier | Specialization |
| --- | --- | --- | --- | --- |
| Age 1 Rootcrown | 210 | 8% | 1.10× | early camp armor |
| Age 2 Crownwarden | 320 | 15% | 1.28× | Age 2 milestone and workshop |
| Fleet Commander | 400 | 18% | 1.35× | +15% King speed; +10% troop speed within 240 paces; 35% shorter command recovery |
| Battle King | 600 | 28% | 1.70× | frontline durability and damage; −6% speed |
| Shadow Sovereign | 390 | 17% | 1.35× | 45% lower visibility factor; 35% less suspicion buildup; +8% speed |

The Warlord milestone unlocks the last three sets. The player confirms the
choice before reserving supplies. Cancellation before fitting completes leaves
all paths open. Completion locks the chosen path for that run. Existing
visibility cones, cover, weather, minimum proximity detection, and line-of-sight
budgets still apply to shadow armor.

## Artwork and rendering

Sources and exact prompts are in `assets/visual/armor-sprites/source/` and
`assets/visual/armor-sprites/prompts.json`. All artwork was generated with the
built-in `image_gen` tool. King reference images are the existing tribal sheets
under `assets/visual/outfits/source/`. Original generated PNGs remain unchanged.

`scripts/build-armor-sprites.py` uses Pillow, NumPy, and SciPy to measure each
body's connected silhouette and antialiased edge, retain its alpha, normalize
atlas cells, and encode WebP. Neighboring sprite fragments are excluded during
extraction. It never paints armor. Authored row maps account for source layout differences; unused
empty cells are excluded. Mirroring and explicit row mappings cover eight
display directions, with front/profile/rear source views. Some rear action
views use the authored rear three-quarter pose. Walking, running, carrying,
attacking, hurt, climbing, rally, fall/death, and worker activities select poses
on the currently equipped full-body sheet, including corpses.

Nine base appearances decode at startup. Armored variants load on demand into
a shared cache capped at 24 decoded variants. Every matching unit shares the
same atlas. There are no armor overlays or per-unit armor textures. Existing
held weapons/tools use the bounded shared loadout cache (1,024 entries / 16M
pixels on high, 512 / 10M on low). Original fallback character PNGs are retained;
WebP copies keep the offline HTML below GitHub's 100 MiB file limit.

To rebuild the sprites, install Pillow, NumPy, and SciPy in the development
Python environment and run `python scripts/build-armor-sprites.py` from this
directory. `--metadata-only` refreshes frame metadata without re-encoding.
Then run `node build.cjs` to rebuild the self-contained game.

## Save compatibility

Old follower body recipes map to basic/reinforced/heavy; old loadout presets
map the same way. Paid batches keep their original costs, duration, and
cancellation refunds, then fit the replacement tier at completion. Old camp
King armor maps to Age 1. Legacy royal level 1 maps to Age 2; levels 2–3 map to
Battle King. A redundant queued legacy upgrade refunds its unused reservation
once. Saves retain the Warlord path and the separately recorded follower health
bonus, preventing repeated load inflation.

## Validation

`tests/armor-progression.test.cjs` exercises fitting, wounds, training, migration,
permanent choices, troop/enemy movement, real visibility, and command recovery.
`tests/armor-art.test.cjs` validates every file, crop, mapped pose, and the lazy
cache. `tests/outfitting-browser.cjs` decodes all 42 atlases offline, renders
5,376 appearance/action/facing combinations, checks shared caching, and uses
the actual crafting UI and a 390-pixel mobile viewport. Performance comparisons
and final regression totals are recorded with the PR validation results.

Local regression validation: 667 game tests and 25 lobby tests passed. Browser
checks cover equipment purchases, held items, tribal openings, mobile commands,
graphics/fallback behavior, and the full desktop/mobile smoke flow. Set
`ARMOR_MOTION_QA=1` and `QA_ARTIFACT_DIR` when running the outfitting browser
check to capture both full appearance grids through twelve motion/action states.

### Performance comparison

Measured against the previous merged game (`56bb381`) in headless Chrome 155,
Windows 11, Ryzen 7 5825U, 1440×900 at DPR 1. Each scenario runs 180 real Canvas
frames, including cold frame-cache population, with the same seed, actors,
sustained combat health, movement workload, and navigation/lighting limits.
Baseline and new versions ran sequentially. Values are mean animation-frame
intervals in milliseconds; lower is better. These are local measurements, not
device-independent frame-rate guarantees.

| Scenario | Previous high | New high | Previous low | New low |
| --- | ---: | ---: | ---: | ---: |
| 250 working villagers | 19.46 | 19.18 | 19.27 | 18.99 |
| 250 villagers, 120 commissions | 23.84 | 23.56 | 21.79 | 22.72 |
| 500 apes with mixed equipment | 24.48 | 26.35 | 22.07 | 24.86 |
| 300 apes, 120 humans, fortress | 41.25 | 39.39 | 36.97 | 37.15 |
| 750 apes, 320 humans, vehicles | 81.94 | 82.12 | 77.47 | 81.01 |

Settlement workloads stay close to the previous version. The additional body
variety adds several milliseconds in the mixed-equipment stress case. Extreme
combined battles remain below smooth frame rates on this machine in both
versions; this change does not claim to solve that existing limitation. No
actors were removed, navigation stayed at at most three searches per frame,
and light/visibility work and shared image/loadout caches remained bounded.
