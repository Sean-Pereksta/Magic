# The Game: multiplayer presentation and synchronization

Entry point: `game/thegame.html`. Classic and Game 2.0 remain the existing cooperative card game; there are no character, damage, resource, or world-position systems to synchronize.

## State audit

The single Firestore document `lobbies/{room}/thegame/state` contains players, host, phase/mode, deck, all hands, stack tops/directions/Bungee charges, votes/deadline, turn index/round/plays, Time Warp pass/debts, placed/total counts, and terminal outcome. These remain atomic; display effects and selection never write state. The URL's room ID and username rejoin the existing match roster, even if the lobby roster has changed.

- Creation is an existence-checked transaction. Restart checks the latest host and terminal phase inside a transaction.
- Each gameplay input captures its observed version and a unique action ID. Transaction retries cannot reinterpret it against a newer board, repeated power, later turn, or new match. Every accepted mutation increments a monotonic revision. Voting deliberately merges concurrent votes; vote resolution is phase-checked.
- Server timestamps are recorded for diagnostics, not used to order moves. Legacy unversioned rooms upgrade on their first accepted mutation. All participants should reload after deployment; older clients do not implement this protocol.
- One metadata-aware match listener replaces the live lobby listener. Cached data can show an initial read-only board, but cannot overwrite confirmed state. Pending local snapshots do not drive effects. Lower revisions are rejected. Offline/cache-only/error states disable writes; ordinary transient failures use Firebase's listener reconnect, terminal errors expose a Retry button. Listener replacement unsubscribes the previous listener.
- No offline move queue or local state upload: refresh/reconnect reads the current match. A delayed input can be rejected rather than silently applied to a changed turn. The last action receipt resolves immediate duplicate transaction callbacks; version checks reject older inputs after later commits.
- Logs are bounded to 120 entries. There are no polling writes, presence heartbeats, or per-frame Firebase operations. All participants still receive the whole small match document, including hands, as in the existing game.
- Consistency-related game fixes: last power card triggers victory; exhausted-deck turns skip empty hands; forward +20 plays do not consume a backward Bungee charge; a Swap cannot target the same pile twice.

## Presentation

Static felt texture, table lighting, layered cards, directional stack rails, player initials, Bungee highlighting, and a deck-clear progress rail. Committed snapshots animate stack changes, direction flips, hand changes, turn transitions, and outcomes for both viewers. At most eight short-lived sparks per changed stack (four on mobile); no continuous render loop, assets, new graphics dependency, or animation writes. Reduced-motion users skip effects. Narrow screens disable backdrop blur.

## Verification

Run deterministic handler/protocol tests (no dependencies):

```sh
node --test game/thegame/sync.test.mjs
```

The DOM/layout smoke test requires Playwright and its Chromium browser:

```sh
node --test game/thegame/layout.test.cjs
```

Tests use actual inline game handlers with an in-memory serialized transaction adapter, and the actual DOM/CSS with a Firebase stub. They are **not** Firebase emulator or production network tests. Browser checks cover 1440×900, 390×844, 320×568, and 844×390, control bounds, particle cleanup, and offline hand disabling. No HTML previews or screenshots are produced.

Before release, run a real two-browser match for both modes: concurrent inputs, each power, end turn, host refresh, guest refresh, brief disconnect, reconnect after another player's move, competing host tabs, victory/loss, and restart. Verify both boards, hands/counts, turn, and outcome converge after each step. The local tests do not establish production rules deployment, network latency, or mobile frame rate.

## Existing trust boundary

The checked-in Firestore rules allow any authenticated user to write lobby subcollections, and player identity here is a username from the URL rather than an enforced UID-to-seat mapping. This update improves cooperating-client reliability; it does not provide server-enforced anti-cheat or protect against modified/old clients. Securing that boundary requires coordinated lobby identity and rules changes. Production Firebase and its deployed rules were not accessed or changed during this work.

Firebase behavior references: [transactions](https://firebase.google.com/docs/firestore/manage-data/transactions), [listener metadata and errors](https://firebase.google.com/docs/firestore/query-data/listen).

Validation recorded for this change: 15 protocol/handler tests passed and inline JavaScript syntax passed. The browser suite could not run in the authoring environment: Chromium was absent and its download timed out. Its viewport assertions remain an unexecuted release check.
