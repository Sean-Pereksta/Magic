# Scripture Quest: solo and cooperative roguelike

The existing Bible Game's Scripture Quest now uses one deterministic rules engine for solo and co-op. Verse Rush, Reference Hunt, Book Sprint, and the competitive multiplayer page remain separate modes.

## Entry points

- Solo: choose Scripture Quest in Bible Game, or the **Bible Game — Roguelike** lobby card.
- Co-op: create a **Bible Game — Roguelike** lobby (`gameType: bibleroguelike`), invite up to five players, then start. Existing `gameId` and `username` routing launches `biblegame.html`.
- Solo resumes automatically. A v2 or v3 local run is migrated once into a separate v4 save. Existing co-op v3 runs upgrade transactionally on reconnect; older clients must reload before playing. Legacy saves are retained. Solo/co-op highscores remain separate.

## Gameplay

Every valid verse deals damage. A verse matching any enemy weakness is a **correct answer** (including a ×1 category): it blocks the entire enemy response, including poison ticks, critical attacks, and boss pulses. It does not consume shield, guard, or Second Chance. Unmatched or resisted verses are incorrect and trigger the normal enemy response if the enemy survives. Invalid and duplicate references are input errors and have no combat cost. Battle knowledge trials also protect correct answers and retaliate on wrong answers; noncombat Scripture rooms retain their reward-only rules.

Each verse is shared once per encounter. Recall is the explicit exception: one reference may be released for the party, once per reference per encounter. Used references remain in combat history and the recall selector. A new encounter clears the pool.

Normal enemy HP scales at **1 / 1.8 / 2.6 / 3.35 / 4** for one through five players; bosses use **1 / 1.85 / 2.7 / 3.5 / 4.25**. Party size is fixed when the run is created. Downed players cannot act; Second Wind and Restoration Oil can revive them, and winning an encounter revives every downed player. A total party defeat ends the run.

The existing 30-enemy roster and calling/class artwork are retained. Eleven archetypes, seven color variants, eight active abilities (three equipped slots), 17 consumables and 23 relics create different builds. Active cooldowns count the owner's successful verses. Swapping slots is only allowed between encounters.

Paths offer battles, elites, merchants, rest, treasure, Scripture challenges and risk/reward rooms. Every fifth floor is a boss. Any living teammate may choose a path; room rewards and six-offer shops are personal. Each player chooses Continue after finishing. A disconnected teammate is excluded from the readiness check after 60 seconds (or after closing their session), so the host is not a progression dependency.

Menus hold inventory, equipped abilities, modifiers, mastery, highscores and the last 40 combat events. Verse Finder, its search interface, and answer-revealing hints are removed. Manual reference entry and mastery selection remain. Former hint consumables now grant 18 shield; Scroll of Context grants 8 shield per encounter and Verse Insight grants 12 shield once per encounter. This preserves saved inventory without revealing answers.

Committed answers carry player ID, reference, correctness, action ID and a 2.8-second expiry in the shared event log. Green “Protected · 0 damage” or red “Incorrect” feedback appears above the submitting player’s character. Co-op renders all party characters with reserved feedback lanes; text wraps inside each lane and never intercepts input. New answers replace that player’s previous feedback. Initial/reconnected snapshots do not replay old effects, repeated snapshots do not duplicate them, and exit/restart clears timers. Reduced motion uses fading without movement. Finishing-blow feedback remains visible before the room dialog opens.

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

Set `BIBLE_TEST_CSV` to a local copy of the production `web.csv` to test all 31,098 verses. `BIBLE_TEST_CHROMIUM` optionally selects a browser executable. The browser harness uses isolated sessions and a mocked transactional Firebase adapter; it never writes to a production lobby. It checks solo resume, Finder removal, correct/wrong damage, per-player synchronized answer feedback, expiry, viewport bounds, co-op races and reconnects. Asset requests use placeholders, so this is not a visual validation of remote artwork or a live Firebase deployment test.
