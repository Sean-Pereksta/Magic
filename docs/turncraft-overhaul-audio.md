# Turncraft controls, campaign and audio update

Turncraft still runs from `game/turncraft.html` with the existing static hosting, Firebase identity/Firestore multiplayer, sprite manifests and simulation. There is no new server, framework or build step.

## Player controls

- Friendly occupied grid squares select their units or building footprints before contextual movement. Hold Shift while clicking troops or dragging a selection box to add them to the current army selection without duplicates; releasing Shift after starting the box preserves that intent. Visible enemies receive validated attack orders. Explicit abilities, construction and rally targeting retain their own validation and never fall through to an ordinary order.
- Commands use compact fixed cells, atlas-cropped portraits, visible actual shortcuts, stable button identity and independent cooldown/resource/production updates. Selection, research and genuine layout changes can update available actions.
- Groups 1–9: tap a number to recall, hold for 500 ms to assign, double-tap to center. Ctrl/Meta plus a number remains supported. Empty selections, repeat events, lost focus, pause and blocking menus cannot accidentally assign groups. Local groups are scoped to the profile and battle and survive checkpoint timer rebasing.
- Single Player opens one graphical faction/map setup with 1–7 independently configurable opponents, all difficulty/temperament choices and the existing victory conditions. Start Battle launches directly. Preferences persist.

## Music and effects

The supplied `Conquest Signal.mp3` is preserved byte for byte at `game/turncraft/assets/audio/conquest-signal.mp3` (SHA-256 `96fc58b2e0947c97de7a411d6dabf792eadd8acbce35b6d0623bbc1d8364f584`). Publish it with `audio.js`, `audio.css` and the updated HTML. No external audio upload is required.

The theme loops at 35% volume after the first pointer or keyboard interaction, across menus and battles. The Sound control provides independent saved music/effects levels and mute. Switching to another tab pauses audio; returning resumes the same track position. Autoplay rejection and unavailable audio do not interrupt gameplay.

Original synthesized effects cover selection, orders, build/train/research confirmation, ballistic/energy/organic/melee/heavy weapons, shield/armor/organic impacts, healing, teleportation, abilities and destruction. Combat sounds follow the existing deduplicated visual events on hosts and snapshot clients, require visibility and an on-screen position, and do not change network messages or authoritative state. Per-family rate limits, a twelve-voice cap and a compressor bound large battles. Visual-only windups remain silent.

## Faction HUD overlays

Frontier uses riveted industrial armor and cyan lights; Ascendant uses ivory/gold crystal framing and violet energy; Brood uses organic chitin with green bioluminescent veins. Three locally shipped transparent WebP frames total 515,066 bytes. The command dock, resource strip and minimap use nine-slice decorations and faction surfaces, buttons, focus rings and a crest. Decorative layers accept no input and preserve the established control sizes and layout.

The overlay follows the local commander's faction after a new game, campaign launch or multiplayer snapshot/seat restoration. Inspecting enemies cannot switch it. Missing artwork retains readable faction colors and controls. Publish `faction-hud.css` and all three `assets/ui/frames/*.webp` files with the HTML. Built-in generation prompts and exact asset paths are in `game/turncraft/assets/ui/frames/README.md`.

## Economic AI

Every personality has an expanding economic horizon scaled by difficulty, map area and match duration. Depleted bases no longer count toward useful economic capacity. Essential production, paid expansion reserves and periodic technology recovery coexist with the existing difficulty tuning. Failed/dead/stalled/unsafe construction jobs expire and release workers; searches retry sites and rotate through alternative workers so an isolated miner cannot veto every expansion. Territorial stalemates trigger bounded reconnaissance and attack-plan reassessment.

The final twelve-case thirty-minute matrix retains an active mineral economy in every case, with one to five paid expansions. Eleven cases first expand at 3.7–8.7 minutes; Easy/cautious Frontier first expands at 23.4 minutes and subsequently maintains three active bases. This slower Easy opening remains a campaign/skirmish pacing consideration for human playtesting.

## The Shattered Front

The original six regions, 24 mission identities, story characters, stars, unlocks and save/profile paths remain. Missions now use authored tactical stages: exploration, limited patrols, safe-route escorts, divided defense, worker-operated devices, linked enemy installations, economic battles, evacuation, shifting gates and a phased final commander encounter. Five operations use ordinary enemy economic AI; scripted pressure is scoped to tactical stages instead of an obligatory mission-wide wave countdown.

The objective tracker retains its elements and expanded state. Radio messages expire automatically, queue within bounded limits, support immediate dismiss/skip, remain in a communications log and can be disabled without removing objectives. Briefings show forces/style/rewards; results explain stars, duration and objective completion. Optional-objective support is bounded and campaign-only. Old checkpoints migrate their preserved progress into the first unfinished tactical stage and materialize newly required targets.

## Verification and limits

Local verification: the full regression/browser suite passed 589/589 checks, excluding the separately run economic-horizon file (34/34 checks, including twelve thirty-minute matches). After the final menu/input and unload-readiness review fixes, both focused browser checks and 35 related runtime regressions passed. The subsequent Shift-selection change passed two focused browser tests and 55 related runtime/desktop/touch regressions. Warfare validation also passed 41 simulation and 45 browser assertions. All 178 existing production artwork files were hash-verified; the three new faction frames were visually inspected and checked for transparent centers.

The regression suite is `node --test game/turncraft/tests/*.test.cjs game/turncraft/shattered-front/tests/*.mjs game/turncraft/shattered-front/tests/*.cjs` after the existing artwork-cache preparation. The economic-horizon file includes twelve thirty-minute simulated matches and can run separately to avoid CPU contention with browser checks. Browser tests use Playwright; `CHROMIUM_EXECUTABLE_PATH` selects an installed Chrome when needed.

New checks exercise actual MP3 decoding/wraparound/preferences, desktop/touch audio placement and event deduplication; stable command DOM over three simulated minutes; selection footprints and number-key timing; all 24 real mission adapters, stage locks and checkpoint entities; campaign HUD/radio; direct skirmish launch; and paid AI expansion/retry/stalemate cases. Mission fixtures accelerate objective progress, so they are regression checks rather than full human campaign playthroughs. Physical-device performance, full campaign pacing/balance and production multi-device Firestore play require human release testing. No production deployment or Firebase rules change is part of this update.
