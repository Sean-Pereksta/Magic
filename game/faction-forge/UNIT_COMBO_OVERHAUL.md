# Unit development and battle effects

Faction Forge now drafts around **recruit → upgrade → specialize → combine**. This expands the existing game; campaign, Endless Crucible, the 32-unit collection, Firebase saves, ranked MMR, lobby entry, and permanent Forge costs use their existing systems.

## Draft and progression

`drafting.js` is used by both the browser and authoritative simulator. It samples categories before cards: 45% recruitment/direct rank upgrades, 25% specializations, 20% combo partners/enhancements, and 10% legacy relics/tactics/formations. Unavailable categories redistribute their weight. Opening armies receive at least two troop choices. Offers obey faction/unlock rules, upgrade only owned troops below III, and avoid duplicate choices or already selected specializations.

Every troop has two short, exclusive specialization paths in `evolutions.js`. Rank II activates path A by default; a specialization card selects A or B and raises a Rank I troop to II. A plain rank upgrade retains the selected path. Rank III strengthens that signature. Cards describe the current/resulting rank, actual mechanical change, tradeoff, and relevant combo unlocks. The artwork badge sits at the bottom-right. Draft art and battle sprites use the existing appropriate tier artwork, falling back to available lower tier artwork.

`lvl` / `battleRank` remain temporary match progression. `evolutions.p0/p1` and `comboEnhancements.p0/p1` belong only to the match. They survive all draft/bonus/round transitions. Permanent Firebase `forgeTier`, cards, dust, campaign rewards, and MMR are unchanged. Ranked combat still normalizes permanent Forge stat bonuses, while retaining earned cosmetics. Existing saves and previously issued troop/upgrade/double/strategy cards remain readable without a profile migration. Missing new match fields use empty defaults.

## Shared combo rules

`combos.js` owns the fourteen faction combinations, automatic trigger conditions, cooldowns, range checks, progression, secondary damage, targeting, and simulation events. Basic combos work with Rank I partners. All partners at II or a drafted combo enhancement unlock Advanced. A III participant with an upgraded complement unlocks Legendary. A single BloodBurst card produces two bodies, allowing its early chain interaction.

The compact tracker shows at most four relevant combinations and filters unavailable locked partners. Mobile starts with the tracker collapsed. Battle labels appear near the actual affected area and expire quickly.

Safeguards include team combo cooldowns, five secondary targets, radius at most two tiles, five bonus damage per target, an aggregate eight/ twelve damage budget for Rank I–II/III attack procs, control at most 450ms with a per-target recovery interval, six lingering fields per side with at most three ticks, bounded healing, and existing summon/decoy/occupancy caps. Secondary hits never recursively trigger attacks or new combos. Fields and cooldowns reset for each rebuilt army.

Spacing, backline assassins, splash against swarms, wound against sustain, EMP against defenses, and killing or disabling a required partner provide counterplay. Combo definitions expose descriptions and counterplay for review and further tuning.

## Combat events and presentation

The existing attack, damage, targeting, terrain, pathfinding, and occupancy functions remain authoritative. Both local play and the server execute the same hooks. `simulator.cjs` fingerprints the combat source and shared modules, and captures combo fields and events in deterministic replays.

Events include a stable ID, simulation timestamp, kind, unit type/rank, source and target IDs, source/impact coordinates, actual hit radius, and actual affected targets. No renderer callback applies damage, chooses targets, spawns logical units, or writes network state. Hosts and guests consume the same synchronized event list; IDs prevent replayed snapshots from repeating effects.

Rockets physically travel from the launcher to the recorded impact point, with muzzle flash, exhaust, smoke, blast, shockwave, and debris. Existing instant authoritative damage is preserved. A bounded presentation interval (at most 220ms) aligns displayed health, hit/death reactions, and explosion with projectile arrival. A finishing impact can complete its brief animation while the logical round has already advanced; this changes no combat timers. Storm arcs use recorded damaged targets; explosion footprints use the simulation radius. BloodBurst swelling and chain blasts use real detonation events. Binding, wards, temporal fields, toxic pools, melee sweeps, healing/drain, EMP, Railbreaker charge/beams, spawning, and deaths each have corresponding effects.

`effects.js` uses two reusable canvas layers, a pool of 480 objects, a normal active budget of 420, and a low-detail budget of 130. Completed effects return to the pool. Quality can fall automatically on slow frames; low detail preserves important impacts. `renderer.js` keeps the existing animated unit sheets, shows rank artwork, and uses actual loaded unit art for afterimages.

`audio.js` synthesizes distinct missile, explosion, lightning, melee, magic, swarm, healing, and toxic sounds with a master gain, ten-voice cap, and short event-rate limit. Browser audio unlock requires a gesture; audio failure never blocks combat.

The in-game **Effects** dialog controls Automatic/High/Low/Off detail, reduced motion, brief glow flashes, battle sounds, and volume. Preferences are local presentation settings, saved separately from progression. Reduced motion initially follows the OS preference; sounds and flashes start disabled.

## Asset integration and deployment

The configured R2 base remains:

`https://pub-8ce3fd69b69d4a58b3989d32f7a47e81.r2.dev`

The existing manifest, animation sheets, loader, and public paths are preserved. All new battle effects and sounds are procedural; **no new R2 upload, image ZIP, audio files, or hosted manifest is required**. No references to proposed but unavailable `effects/*.webp` files were added. Missing graphics, failed decodes, or unavailable audio use the existing nonblocking fallback path.

Deploy the game HTML and the complete `game/faction-forge/` JavaScript/CSS changes together. For the optional competitive service, deploy the updated `server/faction-forge/` implementation from the same commit. Existing Firebase rules and profile schema require no changes. Reload both participants after upgrading a hosted session so they run the same client rules; existing issued legacy cards remain supported. Authoritative replay frames remain server-owned.

## Verification

```sh
npm install
npm run test:faction-forge
npx playwright install chromium
npm run test:faction-forge:browser
npm run test:faction-forge:rules
npm run bench:faction-forge:combos -- 3 --json
```

The core suite includes every combo trigger/cooldown/missing-partner case, all 64 paths, 128 real simulator path/rank battles, deterministic replay equality, ranked normalization, draft distribution/legality/AI, pooled effects/timing/audio, campaign/collection, and server/progression regressions. Browser tests cover original menu/campaign/Firebase/online flows, multi-round evolution persistence, card/tracker accessibility and mobile layout, sprite cropping, actual effect rendering, intensity settings, cleanup, and missing assets. Firebase emulator checks use the demo project and verify ownership, transactional retries, replay persistence, and protected MMR.

For an optional live check against the existing public asset bucket, set `FACTION_FORGE_R2_URL` to the base above and run `npm run test:faction-forge:assets`. `CHROMIUM_PATH` can select an installed compatible browser.

`combo-benchmark.cjs` runs eight representative builds at Ranks I–III with mirrored seats, equal draft-card counts, balanced/wide formations, and reproducible seeds through the production simulator. It records wins, combo activations, peak units, and duration. Roster-card equality does not imply equal body count: some cards recruit multiple troops. See `COMBO_BALANCE.md` for the observed 504-battle sample and tuning. Mobile browser checks emulate screen size; these are not a guarantee of frame rate on every physical device.
