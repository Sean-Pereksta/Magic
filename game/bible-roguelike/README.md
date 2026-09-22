# Scripture Quest: solo and cooperative roguelike

The existing Bible Game's Scripture Quest now uses one deterministic rules engine for solo and co-op. Verse Rush, Reference Hunt, Book Sprint, and the competitive multiplayer page remain separate modes.

## Entry points

- Solo: choose Scripture Quest in Bible Game, or the **Bible Game — Roguelike** lobby card.
- Co-op: create a **Bible Game — Roguelike** lobby (`gameType: bibleroguelike`), invite up to five players, then start. Existing `gameId` and `username` routing launches `biblegame.html`.
- Solo resumes automatically. A v2 local run is migrated once into a separate v3 save. Legacy saves are retained. Solo/co-op highscores remain separate.

## Gameplay

Every valid verse deals damage. Category weaknesses add damage; resistance reduces damage by 20% without invalidating Scripture. The enemy responds to each successful attack. Invalid and duplicate references have no combat cost.

Each verse is shared once per encounter. Recall is the explicit exception: one reference may be released for the party, once per reference per encounter. Used references stay visible in the Finder. A new encounter clears the pool.

Normal enemy HP scales at **1 / 1.8 / 2.6 / 3.35 / 4** for one through five players; bosses use **1 / 1.85 / 2.7 / 3.5 / 4.25**. Party size is fixed when the run is created. Downed players cannot act; Second Wind and Restoration Oil can revive them, and winning an encounter revives every downed player. A total party defeat ends the run.

The existing 30-enemy roster and calling/class artwork are retained. Eleven archetypes, seven color variants, eight active abilities (three equipped slots), 17 consumables and 23 relics create different builds. Active cooldowns count the owner's successful verses. Swapping slots is only allowed between encounters.

Paths offer battles, elites, merchants, rest, treasure, Scripture challenges and risk/reward rooms. Every fifth floor is a boss. Any living teammate may choose a path; room rewards and six-offer shops are personal. Each player chooses Continue after finishing. A disconnected teammate is excluded from the readiness check after 60 seconds (or after closing their session), so the host is not a progression dependency.

Menus hold inventory, equipped abilities, modifiers, mastery, highscores and the last 40 combat events. Finder supports exact reference/chapter, words, book, category, recently viewed and favorites. Searching uses bounded result rendering and yields while scanning large category lists. Effects respect reduced motion.

## Shared state and transaction boundaries

- `lobbies/{gameId}/roguelike/run`: versioned run snapshot, fixed roster and UID bindings, enemy/room, per-player builds/HP, used references, seeded randomness, action IDs and recent events.
- `lobbies/{gameId}/roguelikePresence/{playerId}`: online flag and server-timestamped heartbeat.
- Attack, verse claim, damage, counterattack, defeat, loot and next-room availability commit in **one Firestore transaction**.
- Every transaction reads the current lobby and run, verifies membership and UID binding, and applies the pure reducer to that snapshot. Stale room actions are rejected; recent action IDs are idempotent; shop offer IDs change on reroll.
- Seeded rolls and inputs produce the same result on transaction retries. UI effects only run after a committed result/snapshot. No optimistic damage or offline combat writes.
- Simultaneous initialization and rematch requests converge on a single run.

This uses the repository's existing authenticated `lobbies` subcollection rule. It does not tighten those shared rules or add server-side anti-cheat: like the surrounding games, gameplay validation runs in the client. Do not mistake transactional consistency or client UID checks for server-authoritative security.

## Validation

`npm run test:bible-roguelike`

The Node suite covers same/different-verse races, competing finishing blows, duplicate rewards, path races, reconnects, offline failures, host departure, cooldowns, variants, shops, challenges, migration and bounded long-run snapshots.

Optional browser integration (requires Playwright and Chromium):

```sh
node game/bible-roguelike/browser-check.mjs
```

Set `BIBLE_TEST_CSV` to a local copy of the production `web.csv` to test all 31,098 verses. `BIBLE_TEST_CHROMIUM` optionally selects a browser executable. The browser harness uses isolated sessions and a mocked transactional Firebase adapter; it never writes to a production lobby. It checks solo resume, Finder/favorites, used-verse labels, viewport bounds, co-op races and reconnects. Asset requests use placeholders, so this is not a visual validation of remote artwork or a live Firebase deployment test.
