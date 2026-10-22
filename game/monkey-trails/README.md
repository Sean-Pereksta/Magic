# Monkey Trails · Compass Wilds

Entry: `/game/monkey-trails.html`. The game is linked from `lobby/lobby.html`.
Static HTML, CSS, native JavaScript modules and inline SVG portraits; no build
step, Node game server, Cloud Functions, map service, or paid assets required.
Keep these files in the existing Magic repository. The shared
`lobby/firebase-config.mjs` is deliberately imported rather than duplicated.

## Gameplay

Six starters (Capuchin, Macaque, Langur, Mandrill, Spider Monkey and Howler) have
different stats and traits. Accepted walking earns a cache every 250 feet and
an encounter every 2,000 feet. North favors Leaf, south Fire, east Storm, and
west Water. Resources use net accepted displacement from the previous fight;
the next enemy's elemental odds weight the distance credited in each sector.
The relative compass resets after victory. No absolute coordinates are saved.

Fruit buys levels; battle training discounts the cost. Evolve at levels 3, 6,
and 10. Choose a pure element or one of six hybrids: Wildfire, Steam, Plasma,
Monsoon, Tempest or Typhoon. Dual defensive multipliers combine; hybrid attacks
choose their better matchup. There are 17 abilities, four equipped slots,
energy, initiative, guarding, healing, status effects and consumables. The
first slot retains a free attack so an empty energy bar cannot soft-lock combat.

Every fifth rival is a guardian. Defeat ends the run, preserves all found
weapon/armor/charm equipment and grants legacy starting supplies. The last 25
completed runs are retained. Practice is an isolated device-only save with
simulated direction buttons; it never connects to Firestore or posts scores.

## Browser location and safety

Open the deployed HTTPS page, allow location and tap Start walking. This is
foreground-only tracking: hiding the page, locking the phone, using game menus,
pausing, or entering battle stops it. Resume manually with a new GPS baseline.
No hidden, paused or missing interval is backfilled. A safe-stop confirmation is
required before combat, including after reloading an encounter.

The filter rejects reported/derived speeds above 4.4704 m/s (10 mph), accuracy
worse than 25 m, stale readings, jumps and gaps over 15 seconds. Drift gates and
two-fix warmup intentionally undercount uncertain movement. GPS is approximate;
a browser cannot certify actual speed or prevent location spoofing. Optional
screen wake-lock is only a convenience, not background location permission.
Never play while driving or crossing roads. Compass sectors are resource hints,
not instructions to enter unsafe or private places.

## Existing Firebase integration

`storage.mjs` uses Firebase SDK 10.7.1 and imports the existing shared config
(project `bible-game-246c0`). It waits for the existing Auth session and signs in
anonymously only if no session exists. It does not replace or sign out the lobby
identity and does not require an email/password Auth provider.

Paths:

```
games_rogueduel/monkey_trails_v1/saves/{SHA256(normalizedName+mode)}
games_rogueduel/monkey_trails_v1/scores/{saveId}_{runId}
```

These fit the existing recursive authenticated `games_rogueduel/{gameId}` rule;
no rules changes are included or required when deployed rules match this repo.
Only `mountandblade_online` is excluded by that rule. Permission errors remain
visible and device saving continues. Production rules were not deployed or
verified by this change. The leaderboard uses a single-field score ordering.

## Autosaves and limitations

Create a 3–24 character creature name and a unique password of at least 10
characters; reopen with the same credentials. Names are case-insensitive.
There is no password-reset mechanism. PBKDF2-SHA256 (310,000 iterations) derives
an AES-256-GCM key. Random salt/IV and identity-bound authenticated encryption
protect checkpoints; plaintext passwords are not stored or sent to Firestore.

Checkpoints cover movement (at most five seconds between accepted-fix saves),
rewards, battle turns, upgrades, evolution and transitions. Encrypted device
storage is the baseline; cloud writes coalesce and normally throttle to 20
seconds, with urgent actions syncing sooner. An abrupt browser/OS termination
can lose the most recent unsaved seconds; a page-exit save is only best effort.
Offline play works once game code is loaded, not as an installable offline PWA.

Revision transactions reject conflicting saves rather than overwriting a newer
cloud copy. Export/restore encrypted JSON backups and explicit cloud loading
preserve recovery copies. Death scores use stable run IDs; retries do not
multiply records. The last 25 runs also bound the offline score retry history.
Wait for the explicit Firestore-saved status before switching devices.

IMPORTANT: existing shared rules are not owner-only. Encryption protects
readable save content, not deletion/replacement by another authenticated client.
Weak passwords remain vulnerable to offline guessing. Final creature names and
scores are readable by authenticated visitors, and scores/GPS are client-reported,
unverified casual results, not authoritative competitive or fitness records.
No latitude, longitude or route history is included in any checkpoint or score.

## Verification

From the repository root, with Node 22 or newer for development tests only:

```sh
node --test game/monkey-trails/test.mjs
```

24 tests passed with Node 22: milestone boundaries, evolution paths, battle
recovery, death/reward idempotency, GPS speed/drift/gap filtering, real WebCrypto
roundtrips, incorrect passwords, tampering, offline retries, revision conflicts,
practice isolation, storage quota failures and encrypted backup restoration.
Firebase integration tests use a deterministic adapter, not production writes.
All modules also pass `node --check`.

A full end-to-end browser pass was not completed: the available Chromium
blocked navigation by administrator policy. Real phone GPS, live Firestore,
and full mobile/desktop layout behavior still need deployment smoke testing.

Before release: open from the lobby on Android Chrome and iOS Safari; verify
permission allow/deny, a measured outdoor walk, stationary drift, screen locking
and resume, each game tab, a complete battle, death/restart, encrypted export
and restore, offline recovery, and two-device conflicting saves. Do not test
speed rejection by playing while driving; use simulated GPS in development.
