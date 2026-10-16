# Quip Clash

Quip Clash is an original phone-friendly party game launched from the existing
`lobby/lobby.html` catalog. Its dedicated page is `game/quip-clash.html`.

## Playing

Sign in through the normal game hub, create a room, and share its six-character
code or invitation link. Each player needs their own website account. The game
uses the account's display name. The host starts when 4–12 players have joined.

- Rounds 1 and 2 give every player exactly two different prompts and 90 seconds
  to answer. Responses allow up to 1,000 characters. Text saves as players type;
  locking in confirms the answers. Only text saved before the deadline is used.
- Paired matchups have two authors. Odd player counts produce two three-author
  matchups and pairs for the remaining questions, preserving two answers each.
- Each matchup has 20 seconds to vote. Normal-round authors sit out their own
  matchup. Voting ends early when every eligible voter has locked a vote.
- Round 3 uses one common prompt, 90 seconds of writing and 20 seconds of voting.
  Everyone may choose another player's response; self-voting is disabled.
- Each question splits a point pool according to valid votes: 1,000 points in
  round 1, 2,000 in round 2, and 3,000 in the finale. Whole-point rounding uses
  largest remainders with deterministic answer-ID tie breaking. No votes means
  no points; blank responses never earn points. There are no win bonuses.
- Results appear after each matchup, with a full scoreboard between rounds.
  Results advance after six seconds and scoreboards after twelve; the host can
  continue sooner. The final board supports tied winners and a rematch.

The bank contains 108 original normal prompts and 28 finale prompts. Questions
do not repeat within a match. Rematches use a fresh random seed.

## Integration and synchronization

`firebase.mjs` reuses the public lobby Firebase configuration and existing
Firebase Auth session. It requires a real `users/{username}` profile whose
`chatAuthUid` matches the session established by the hub. A remembered name or
arbitrary username query alone does not grant a game seat. Display names are
copied into the roster on joining and stay stable for the match.

All game data is stored in the parent `lobbies/{CODE}` document: member map,
roster, round, assigned questions, saved text, locked submissions, ballots,
phase timestamps, and scoring receipts. No new Firestore collection, index,
backend service, or security-rule deployment is required. Generic lobby browser
creation, invitation joins, and started-game reconnects route to the game page.

Every game mutation uses a Firestore transaction. Phase keys reject stale
requests, votes lock on first submission, and one receipt per question prevents
duplicate awards. Scores are recomputed from the receipt ledger. Server
timestamps and per-device clock probes align countdowns even if device clocks
are wrong. Any member can advance a completed or expired phase, so the host's
phone can disconnect without stopping an active game. Writing/voting deadlines
still require an open connected game client to apply the transition; all-offline
games catch up when a member reconnects.

Started-game seats remain fixed. Leaving the page during a match does not remove
the account or erase scores. Rejoining the same room restores its current phase.
In the waiting room, leaving removes the seat and transfers hosting to the next
player. A full reload restores saved text and local pending drafts. Firestore
transactions reject offline writes, avoiding votes queued into a later matchup.

### Existing rule boundary

The repository's generic authenticated `lobbies/{id}` rules permit this game
without changes, and were tested directly. Those rules allow authenticated
clients to edit generic lobby documents. Validation, anonymous-answer display,
and score-receipt immutability are application guarantees, not server-enforced
anti-cheat or answer secrecy. A malicious modified client could edit room data
or inspect answers. Stronger adversarial protection would require separately
deployed restricted rules and/or a trusted backend; this change does not weaken
or claim to replace the existing rules.

The transaction behavior follows the [Firestore transaction contract](https://firebase.google.com/docs/firestore/manage-data/transactions),
including retries on contention and rejection while offline. Connection display
uses [snapshot metadata](https://firebase.google.com/docs/firestore/manage-data/enable-offline).

## Validation

From the repository root:

```text
npm run test:quip-clash
npm run test:quip-clash:rules
```

The rules command starts an isolated `demo-quip-clash` emulator on port 8189,
loads the unchanged repository rules, and verifies a complete five-player match,
simultaneous votes/advances, exact ledger totals, reconnect/rematch, deadlines,
partial answers and rejection of unauthenticated reads/writes. It never accesses
the production database.

For browser coverage, serve the repository on port 8790 and keep the Firestore
emulator running, then run `npm run test:quip-clash:browser`. The test replaces
only the Firebase adapter with a test-account adapter to the emulator. Production
UI, app, network, reducer and Firebase SDK run unchanged in four independent
mobile browser contexts. Optional settings: `QUIP_PREVIEW_URL`,
`FIRESTORE_EMULATOR_HOST`, and `QUIP_BROWSER_CHANNEL=chrome` for an installed
Chrome browser. `QUIP_DEPENDENCIES` and `PLAYWRIGHT_PACKAGE` allow reuse of
workspace test dependencies.

Core/state tests cover all supported player counts, odd assignments, vote-share
rounding, ties, self/invalid votes, missing answers, full matches, stale requests,
receipt reuse and account switching on a shared anonymous Firebase UID.

Local validation also covered all UI screens at 320px, 390px and 1280px, 1,000
character normal/unbroken Unicode responses, escaped HTML, preserved input focus
and selection during live updates, locked controls, and no horizontal overflow.

## Deployment

Merge/publish the game page, `game/quip-clash/`, modified lobby files, and the new
lobby artwork/manifests through the site's usual GitHub publishing process. The
game uses existing Firestore rules as-is. No production test lobbies were created.
