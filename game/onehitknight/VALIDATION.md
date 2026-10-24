# One Hit Knight verification — October 24, 2026

The final implementation passed 21 gameplay check groups, 25 real-browser check groups, and 13 Firestore emulator permission checks. Asset validation passed for 164 PNGs, 180 sprite IDs, and 1,479 declared frame entries, including aliases.

Verified behavior includes all eight weapons' anticipation/contact/recovery windows, four directional sectors, hit-once swings, repeated input, Perfect Strikes, all seven boss openings, 30-kill Knight Frenzy, independent appearance layers, all 30 pickup handlers, seven events, the three original modes, every-third-wave graphical choices, paused buffs, Mayhem expiration with shields, and canceled prior-run timers/death menus.

Browser checks covered direct file opening without a server, decoded image artwork, keyboard movement, settings/pause/resume, mobile analog stick/Slash/Dash, compact portrait layout, score payload/name handling through a deterministic Firebase adapter, and restart behavior. Hosted asset checks used intercepted HTTP responses: critical PNG/manifest boot, upcoming-arena loading, missing image recovery, retry while art remains visible, invalid/incomplete manifests, and renamed missing critical sheets.

Artwork checks verified every PNG path, SHA-256 digest, alpha channel, frame bounds, direction offset, nonempty animation entry, and positive frame duration. Contact sheets and synchronized weapon/equipment previews were inspected. The pack contains real ImageGen raster artwork; the exact prompt set and authored/reused frame provenance are included. Enemy movement has two poses and equipment has two poses per direction; these documented counts remain below the longer production animation targets.

With all 34 enemy types rendering together at 1280×800, headless Chromium on this host measured 60.0 average FPS, 16.7 ms median frame time, and 16.7 ms at the 95th percentile across 175 sampled frames. This measures this host and browser; it is not a physical-device benchmark across all mobile hardware. Mobile input/layout were checked through browser touch emulation.

Live production verification: Firebase anonymous sign-in succeeded. The original Firestore leaderboard read returned `Missing or insufficient permissions`. The included rules fix passed all 13 local emulator checks, including valid submission, forged owner/negative score/bad mode/oversized name/extra field/stale timestamp/out-of-range rejection, immutable scores, denied deletion, and denied broad-rule bypass. Production rules were not deployed and no production scores were submitted or deleted. Publish the reviewed rules to activate the online leaderboard.

Raw browser, artwork, permission, live-read, and performance JSON reports are included beside this document. The repository contains reproducible gameplay/browser/emulator test entry points and setup instructions under `game/onehitknight/README.md`.
