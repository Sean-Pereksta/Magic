# TurnCraft multiplayer, cloud profiles and ranked deployment

The implementation reuses the existing CatnMice Firebase project, Auth session,
`lobbies` collection and Node server. This change does not deploy production
Firebase rules, publish an API, recover accounts or change live ratings.

## Deploy the existing Node host

1. Deploy the repository's updated `server.js`, `server/turncraft/`,
   `game/turncraft.html` and `game/turncraft/maps.js` together. The authoritative
   runtime loads the actual checked-in RTS simulation from these game files.
   Keep the rest of the game's served modules and graphics in the web deployment.
2. Install root dependencies with `npm install` and run the existing `npm start`
   command. Its existing private `FIREBASE_SERVICE_ACCOUNT` environment value or
   `serviceAccountKey.json` supplies Firebase Admin credentials. No browser
   service-account credential or second Firebase project is needed.
3. The optional mount exposes `/api/turncraft` and requires the existing project
   ID `bible-game-246c0`. A failed mount leaves the other games running.
4. Expose this API through HTTPS. The default exact CORS origins are
   `https://catnmice.com` and `https://www.catnmice.com`.
   `TURNCRAFT_ALLOWED_ORIGINS` can override them with a comma-separated list.
   Include an exact localhost origin only for local testing.
5. On the game origin, set the API route base to the actual deployed endpoint:

   ```js
   localStorage.setItem('turncraft.apiBase', 'https://YOUR-NODE-HOST/api/turncraft');
   ```

   A hosting configuration may instead set `window.TURNCRAFT_API_BASE` before
   game modules load. The default `/api/turncraft` supports a same-origin proxy.
   The R2 artwork address is an asset host, and cannot serve the game API.
6. Verify `GET /api/turncraft/health` reports `custom: true` and `ranked: true`.
   Ranked capability becomes false if the actual simulation cannot initialize.
7. Review and deploy `firestore.rules` to the same existing Firebase project.
   Deploy updated web clients and the backend with these rules. New TurnCraft
   rooms are created and managed through authenticated backend endpoints.

No composite Firestore index is required. Ranked queue lookup uses the single
`expiresAt` field; the rating leaderboard uses the single `mmr` field.

## Existing accounts and profile ownership

The old Game Hub uses `users/{username}` documents. Its legacy plaintext password
and broadly authenticated writes are insufficient evidence of account ownership.
TurnCraft reuses the existing verified Faction Forge ownership bindings:

- `factionForgeAccounts/{FirebaseAuthUID}` with `verified: true` and `username`.
- `factionForgeNames/{username}` with the same Firebase Auth UID.

For an account already recovered and independently verified by an operator, the
existing Admin migration helper provisions those bindings:

```sh
node server/faction-forge/migrate-account.cjs 'ExactLegacyUsername' FIREBASE_AUTH_UID --verified-owner
```

Do not provision a binding from an unverified URL username or the old publicly
readable password. A signed-in Firebase account without a verified binding can
play custom multiplayer and retain UID-private personal progress, but cannot
enter ranked play or claim another user's shared profile. A durable Firebase
Auth UID must be reused to recover cloud progress on another device. An anonymous
browser identity alone does not provide cross-device recovery.

`turncraftProfiles/{uid}` is the canonical UID-owned TurnCraft profile. For a
verified CatnMice owner, the backend merges its TurnCraft data into the existing
`users/{verifiedUsername}.gameStats.turncraft` namespace. Other games, profile
fields and the existing Faction Forge rating remain intact. Clients cannot
create, alter or remove this protected TurnCraft namespace, rating documents,
match checkpoints, queue entries, invites or receipts.

## Multiplayer entry and custom matches

The CatnMice TurnCraft tile offers **Join Match**. Public lobby rows route to the
dedicated `game/turncraft-lobby.html` room instead of attempting legacy writes.
The game menu supports `?mode=quick`, `ranked`, `create`, `browse`, `code` and
`leaderboard`. Private invite links carry a 12-character code; opening the link
shows the join action before claiming a seat.

Custom rooms support the engine's two or three human seats, every existing
faction, six established multiplayer maps and the existing Annihilation,
Conquest and Central Relic modes. Team selection accurately shows Free for All.
Host setting changes reset readiness. Only the authenticated host may start,
and every present commander must be ready. Late seats are rejected after launch.
Leaving a waiting room transfers hosting to a remaining commander. Reconnecting
with the same UID restores the frozen seat and existing shared battlefield.

Custom matches retain the existing controller simulation and receive no MMR.
New UID rooms permit only the recorded controller to write snapshots and only
each seat's owner to create commands or presence. A completed custom match is
confirmed from its persisted terminal snapshot. The lobby displays its result
without relaunching it. A host rematch creates a fresh room and invitation;
remaining lobby viewers follow the new room, and all commanders ready again.
Old tabs retain the old finished room, so their snapshots cannot overwrite a
rematch. Room creation and rematch retries reuse the original created room.

Legacy version-zero TurnCraft rooms retain their existing unranked synchronization
permissions and game flow. Clients cannot promote those rooms into protected UID
rooms or ranked matches. Other games keep their established lobby behavior.

## Authoritative ranked play

Ranked matchmaking pairs two independently authenticated, verified CatnMice
owners. It starts near their current saved MMR and widens the search with wait
time. Each account has at most one active ranked match. Queue and match allocation
use Firestore transactions so concurrent retries cannot allocate duplicate matches.
Ranked settings are fixed to Classic Frontier, Annihilation, standard resources,
two humans and each player's selected existing faction. Campaign and survival
cosmetics confer no competitive statistics.

The server runs the real TurnCraft engine in an isolated VM with DOM, rendering,
browser boot and Firebase activity disabled. It advances combat, movement,
production, mining, research, logistics and victory detection using server time.
Clients send bounded gameplay commands and request IDs, never winners, scores,
rosters, timestamps, resources or rating deltas. The server assigns the UID's
frozen slot, verifies actor ownership, applies fog checks to targeted enemies and
rejects unknown authority fields. Responses hide fog-hidden enemies, opponent
economy, research and orders until the game ends.

Full server state, simulation clock and RNG state are compressed into an
Admin-only Firestore checkpoint after every accepted transaction. Another Node
instance can restore it after a restart. Physics uses 50 ms steps, economy uses
the established 250 ms cadence, and an individual request advances at most five
seconds of catch-up simulation. The browser polls at 750 ms while the match is
active. State polling has a separate 120/minute budget; commands have a separate
1,200/minute budget. Command groups are bounded to 200 entities.

The terminal engine state or an authenticated participant's own forfeit produces
the result. Repeated terminal reads and forfeit requests reuse its receipt. Both
profiles, the shared TurnCraft namespaces, public ranking rows, history receipts,
pair ledger and finished lobby state commit in one transaction. Elo starts at
1,000; the first ten matches use K=48, later matches K=24. A transfer uses the
larger provisional K when either player is provisional and cannot push a rating
below zero. Rank thresholds are Bronze <900, Silver 900, Gold 1,100, Platinum
1,300, Diamond 1,500 and Master 1,750. Never-played profiles display Unranked.

A player who forfeits receives the authenticated loss, including an early
forfeit. Leaving the browser does not clear the active-match pointer. After two
minutes without heartbeats, one absent player forfeits; if both have been absent
for two minutes, the result is a draw. These deadlines are evaluated on the next
authenticated match read/input, including a reconnect. A one-hour ranked match
limit also resolves a draw. Each opponent pairing can receive three rated results
per UTC day; it is then excluded from that day's matchmaking. This limits repeat
opponent farming without claiming to prevent every form of collusion.

Production concurrency and hosting cost must be measured on the deployed Node
runtime. Checkpoint transactions reconstruct the engine rather than relying on
one process's memory, trading more CPU/Firestore activity for restart safety.

## Personal campaign and survival saves

Cloud campaign saves validate the 24 mission IDs, sequential unlocks, best stars,
completed optional objectives and ratings by the engine's `easy`, `medium`, `hard`
and `pro` difficulties. Regional and survival milestone cosmetics are derived
from the saved progression. Survival records retain arena, highest wave, duration,
kills, faction, difficulty and server save date.

These personal progression records are explicitly `personal`/unverified. They
cannot change MMR or enter a public survival leaderboard. `globalSurvival` remains
false until an authoritative survival adjudicator is implemented. Local campaign
and survival remain playable when the cloud service is unavailable; the menu
keeps pending personal results for retry.

## API

All routes except `/health` require a Firebase ID token in
`Authorization: Bearer TOKEN`. Mutation bodies reject unknown fields.

| Method | Route | Purpose |
|---|---|---|
| GET | `/profile` | Current UID identity, saved profile and capabilities |
| POST | `/progress/campaign`, `/progress/survival` | Personal cloud progress |
| GET / POST | `/lobbies` | Browse public rooms / create a room |
| POST | `/lobbies/join` | Claim a public seat or use a private invite code |
| GET | `/lobbies/:id` | Read an allowed room and its member invite |
| POST | `/lobbies/:id/player`, `/configure` | Own faction/readiness / host settings |
| POST | `/lobbies/:id/start`, `/leave` | Host launch / authenticated departure |
| POST | `/lobbies/:id/complete`, `/rematch` | Custom terminal snapshot / fresh rematch |
| POST | `/ranked/queue`, `/ranked/cancel` | Verified 1v1 matchmaking |
| GET | `/matches/:id` | Advance/read your authoritative ranked match |
| POST | `/matches/:id/command`, `/forfeit` | Own gameplay input / authenticated surrender |
| GET | `/leaderboard` | Saved server-written top 50 ranked profiles |

## Verification

```sh
node --test game/turncraft/tests/authority.test.cjs server/turncraft/tests/http.test.cjs
node --experimental-vm-modules --test lobby/tests/lobby-library.test.cjs
firebase emulators:exec --project demo-turncraft --config firebase.turncraft-test.json --only firestore "node game/turncraft/tests/turncraft-rules.emulator.cjs"
```

The authority tests run the real RTS engine and exercise queue allocation,
checkpoint restore, authenticated commands, private custom rooms, readiness,
terminal results, rematches, atomic Elo receipts, repeat submission protection,
fog filtering and preservation of other games' fields. HTTP tests cover bearer
authentication, exact-origin CORS, invalid inputs and independent request budgets.
The demo-project emulator compiles the actual shared rules and checks profile
authority, nested namespace protection, UID seats, controller leases, ranked path
isolation and legacy compatibility. No test reads or writes production Firebase.

After deployment, finish one public custom match and its rematch, one private
invitation and one ranked match with two verified test accounts. Confirm the
same UID reconnects after a Node restart, both saved MMR receipts match the
displayed result, other game profile data remains unchanged, and Firebase rules
reject direct client rating writes. Live deployment validation remains an
operator step; local/emulator verification does not publish the service.
