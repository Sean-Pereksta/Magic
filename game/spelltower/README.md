# Spell Tower: the enchanted glade

Spell Tower is a turn-based cooperative tower defense game. Seven basic elements produce 28 fusions, for 35 spells. Twenty-three terrain reactions make placement and spell order matter. The campaign lasts ten waves, with bosses on waves five and ten.

## Launch modes

| Mode | Route | Behavior |
| --- | --- | --- |
| Solo | `/game/spelltower.html?mode=solo` | One mage; local autosave; no Firebase dependency for gameplay. |
| Hot seat | `/game/spelltower.html?mode=hotseat` | Two to four named mages on one screen; optional `players=2..4`; a handoff hides the next hand until the player is ready. |
| Online | `/game/spelltower.html?gameId=ROOM&username=NAME` | One to eight members of a started Spell Tower lobby; identity is verified using the lobby's Firebase Auth session. |

Direct visits show the mode chooser. An explicit local mode takes precedence over any stale online room query. The lobby exposes Solo, Hot Seat and Create Lobby. The desktop launcher installs the complete local game folder and passes the chosen local mode to its loopback server; Online uses the existing official lobby flow.

## Rules and presentation

- `engine.mjs` is deterministic and contains no DOM, network or clock calls. `applyAction` returns a new JSON-safe state or throws without modifying the old state. Seeded randomness survives reloads and Firestore transaction retries.
- Every mage receives all seven basic elements. Mana refills each turn. A card can be cast once per turn and has finite charges. Two unused base cards can be forged into an advanced spell. All unordered pairs, including duplicates, have a recipe.
- Enemies and terrain advance after the whole party has played. Late arrivals join the next round. Leaving cannot produce an empty or skipped turn. Blocked spawn entrances queue reinforcements instead of silently dropping them.
- The engine computes the targeting preview using the same spell resolution as the committed action. Previews never change the live state.
- `art.mjs` and `assets/glade.svg` are original local vector illustrations. `effects.mjs` renders bounded, short-lived particles, lightning, ripples, embers, shards and damage labels after committed changes. Sounds are synthesized locally and start muted. Reduced-motion preferences are respected.

## Online lifecycle

`network.mjs` is imported only for online rooms. It verifies the current name against the signed-in arcade profile. `session.mjs` reads the started lobby and keeps the new game snapshot at `lobbies/{room}/spelltower/state`, under the repository's existing authenticated lobby-subcollection rules. It does not write the old top-level `spelltower` collection.

Transactions recheck membership, game version, turn ownership and the expected revision. Visual effects run after a committed result, never from inside a transaction callback. Snapshot listeners do not write state back. A host departure transfers ownership. The last departure removes the room. A host can pass another player's turn after 90 seconds without an action. Restarting replaces the run for every connected player. Refreshing reconnects to the shared snapshot.

This follows the existing cooperative lobby's security model; it is not a ranked, server-authoritative anti-cheat service. No Firestore rule deployment is required by this change. Start a new party after updating from the legacy game so every device loads the same version.

## Verification

```sh
node --test game/spelltower/tests/*.test.mjs
node --experimental-vm-modules --test lobby/tests/solo-launch.test.cjs lobby/tests/lobby-library.test.cjs desktop-hub/test/spelltower-launch.test.cjs
node game/spelltower/tests/browser.cjs
```

The browser test requires Playwright and Chromium. `CHROMIUM_PATH` can select an existing browser executable; `SPELLTOWER_QA_OUTPUT` optionally records layout screenshots. The test runs the real UI at desktop, small desktop, phone and landscape dimensions; casts terrain combinations; forges spells; reloads saves; plays three-seat handoffs; connects two browser pages to an isolated transactional room fixture; restarts and transfers hosting; and builds/plays the generated launcher pack with network access disabled.

The isolated online fixture exercises client synchronization without modifying public rooms. A live production Firebase room and a packaged Windows installer are separate deployment checks.
