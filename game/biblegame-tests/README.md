# Multiplayer regression tests

Run from the repository root:

```sh
node --test game/biblegame-tests/*.test.cjs
```

The suite executes the actual inline module from `biblegame-multiplayer.html` in
Node's VM, with authentication startup dormant, a small DOM, a controlled clock,
and in-memory Firestore snapshots and serial transactions. It requires no npm
installation, network access, or Firebase credentials.

The tests cover timer-driven team/co-op controls, reference drafts surviving
rerenders, one answer per team/player/question, stale answers and connection
ownership, exact deadlines, atomic round transitions and replay requests,
fallback advancement when a host page freezes, small co-op verse pools,
clock probe handling, rating receipts and recovery, and obsolete session listeners.
A transaction hook advances time or stored
state between the player's click and the transaction's reads.

Run the Firestore rules checks and two-browser integration smoke test with
Java 21, the npm dependencies, and Playwright Chromium installed:

```sh
npm install --ignore-scripts --no-audit --no-fund
npx playwright install --with-deps chromium
npm run test:biblegame:rules
```

The emulator command starts the local `demo-biblegame` project using
`firebase.biblegame-test.json`, runs the rules tests, and then runs
`browser.emulator.cjs` against the same emulator. Run these tests sequentially:
the rules suite uploads its rules before the browsers start.

The browser smoke uses two isolated Chromium sessions, the real Firebase SDK,
transactions, snapshots, and security rules. It checks room creation/joining,
team voting and rosters, concurrent team answers, early round advancement,
host disconnect/failover/reconnect, account rating receipts surviving
reload without double counting, and a closed player recovering a missed result
after the active session changes. It seeds unique fixture names and replaces
authentication and Bible data only inside its local test server. It connects
only to a localhost Firestore emulator and requires no production credentials.

For an already-running emulator, set `FIRESTORE_EMULATOR_HOST` and optionally
`GCLOUD_PROJECT`, then run:

```sh
node game/biblegame-tests/browser.emulator.cjs
```

`CHROME_PATH` selects an existing Chromium executable. Otherwise Playwright
uses its installed browser; on the Codex Windows runtime, the smoke can also
use the bundled Playwright package and installed Chrome.

The Bible Game workflow runs both suites with a 15-minute overall limit.
