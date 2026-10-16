# Faction Forge Firebase progression

## Default: Firebase Auth + Firestore, no Node server

Leave `progressionApiUrl` empty in `game/faction-forge/config.js`. Campaign and bot battles run in the browser, and personal progress saves directly to the existing `bible-game-246c0` Firebase project. No server URL, Firebase Functions deployment, Admin credentials or account-binding migration is required for this mode.

Publish the updated static game files and deploy the included Firestore rules to that existing project:

```sh
firebase deploy --only firestore:rules --project bible-game-246c0 --config firebase.rules.json
```

Firebase Console users can instead publish the repository's complete `firestore.rules` in Firestore Database → Rules. The rules support the hub profile save at `users/{username}/factionForge/profile`, its immutable operation receipts, and the original private UID guest saves. Test rules in the emulator before publishing.

The selected Game Hub profile (`gh.username`) determines the save, including when an old game URL supplies a different username. Sign into the same hub profile on another browser or device to reopen its campaign, cards, Forge Dust, tiers, reward preferences, wins and losses. Firebase Auth supplies the connection; changing its anonymous UID does not select a different hub save. The canonical save follows the existing username-profile access model used by the lobby. It does not replace the hub's authentication system.

On the first hub-profile launch, the game imports a matching legacy save from the currently connected UID, including its scoped account save. A legacy `Commander` or `Guest` label can be claimed by one named profile. The original save remains intact. Queued operations preserve their existing receipts when accessible, so uncertain rewards and purchases cannot apply twice during migration. Older rules that deny optional scoped saves or receipt paths do not block canonical saves. A previously lost anonymous UID cannot be searched or recovered by username alone; import its legacy save while that original browser identity is still available.

Hub profiles have separate Firestore documents and retry queues. Hub login/logout in another tab, returning focus, or making the game visible reloads it into the currently selected account. Every save checks that its selected hub account and Firebase connection are still current. Without an existing hub profile, the browser retains a private guest save at `factionForgeSoloProfiles/{uid}`; clearing guest browser data can change that identity. Live multiplayer state remains in the selected `lobbies/{lobbyId}/crowncouncil` room, separate from profile progress.

Deploy the updated Firestore rules with these static files for validated profile saves and immutable result, upgrade, purchase and round receipts. The Forge's **Buy packs** tab spends Forge Dust on displayed, fixed card sets: faction packs cost 50 Dust for 15 cards; the War Chest costs 90 Dust for 30 cards. Purchases use immutable receipts so an uncertain retry cannot charge twice. Active matches block purchases. A saved solo round win awards 3 cards, and completed-win card grants are doubled; existing match-reward caps remain, with a separate cap of 70 rewarded rounds per UTC day. Practice without a connected save still earns no collection rewards.

Solo wins persist campaign stars, mission unlocks, all six troop milestones, matching cards, Forge Dust and Endless challenge progress. Upgrades deduct cards/dust and increase a permanent tier in one Firestore transaction; tiers are frozen into the next local battle. Result and upgrade receipts make retries idempotent. Interrupted reward saves remain queued under the hub username on that device and retry on reload; the results screen also provides a retry button. Pending upgrades and purchases retain their request IDs across reloads and Firebase sign-in changes. An interrupted solo battle itself is not restored by this mode.

These are personal, client-played solo saves, not independently verified competitive results. Solo documents have no MMR field and never update `users/{username}.crowncouncilElo` or `factionForgeProfiles`. Hosted multiplayer remains available through the existing Firebase lobby path. The optional trusted competitive service below remains a separate mode; its currency, collection and ratings are not merged with client-owned solo saves.

The game reports `Progress saved with Firebase` after connecting. A permissions error specifically requests the Firebase rules update. A connection error retains pending results for retry.

## Optional: authoritative competitive service

The remaining sections apply only when an operator explicitly sets a non-empty `progressionApiUrl`. They are not needed for Firebase-only solo progression.

## Service

The existing repository `server.js` now automatically mounts `/api/faction-forge` using its already initialized Firebase Admin connection. Deploying the latest code to that existing Node host supplies the endpoint without a separate service account or a second Firebase initialization. Its existing private `FIREBASE_SERVICE_ACCOUNT` or `serviceAccountKey.json` remains on the server. The optional mount accepts only the existing `bible-game-246c0` project; initialization failures leave Grid Overlord's original HTTP and Socket.IO host running.

The shared host defaults to exactly `https://catnmice.com` and `https://www.catnmice.com` for progression CORS. `FACTION_FORGE_ALLOWED_ORIGINS` can explicitly override the allowlist. The original `/` health route remains unchanged. After deploying the Node runtime, verify its actual `/api/faction-forge/health` endpoint and set `progressionApiUrl` to that runtime's HTTPS origin plus `/api/faction-forge`. No live service address is assumed or invented. The R2 graphics address serves assets and is not the progression API.

A separately hosted service is also supported. Install the repository dependencies, then run:

```sh
npm run start:faction-forge
```

Required runtime settings:

- `GOOGLE_CLOUD_PROJECT`: the existing CatnMice Firebase project, `bible-game-246c0`.
- Firebase Admin Application Default Credentials for that project, supplied by the server runtime or a private credentials file through `GOOGLE_APPLICATION_CREDENTIALS`.
- `FACTION_FORGE_ALLOWED_ORIGINS`: comma-separated exact game origins, for example `https://catnmice.com,https://www.catnmice.com`.
- `PORT`: optional, defaults to 8788.

The service mounts `/api/faction-forge`. Expose it through HTTPS; set `progressionApiUrl` in `game/faction-forge/config.js` to the complete route base. Example: `https://YOUR-SERVICE/api/faction-forge`. Keep credentials out of GitHub and out of browser files. The dedicated service and the existing-host mount use the same progression implementation.

Deploy `firestore.rules` to the same existing project after reviewing the protected profile fields and verifying the game code is updated. No special Firestore indexes are required for progression: all authoritative operations use document paths and transactions.

## Account ownership and legacy history

The previous profile model uses username documents with publicly readable plaintext passwords and broadly authenticated writes. A URL username, a supplied old password, or `chatAuthUid` is consequently insufficient ownership evidence. None is accepted as a progression ownership claim.

The operator must independently recover/verify each CatnMice account, establish a durable Firebase Auth identity, and create its immutable binding:

```sh
node server/faction-forge/migrate-account.cjs 'ExactLegacyUsername' FIREBASE_AUTH_UID --verified-owner
```

This copies the existing `users/ExactLegacyUsername.crowncouncilElo` exactly into the UID-owned profile, without changing the old rating, password, other games, or other profile fields. Repeating it for the same owner is safe; claiming an already bound username for another UID is rejected. `factionForgeAccounts/{uid}` and `factionForgeNames/{username}` are Admin-only ownership records.

The existing browser reuses any already signed-in Firebase Auth account. For recovery on another device, authenticate as the same durable UID. An anonymous browser session by itself is not a cross-device account recovery method. Accounts without a verified binding can play practice; they cannot claim another username's progression or mint rewards.

For a migrated username, unrelated legacy profile updates remain possible for its bound owner. Its rating fields and all Faction Forge progression remain server-only. Unmigrated unrelated games keep their existing behavior, except direct writes to protected Faction Forge rating/progression fields. The old publicly readable legacy credential design is not used by this service; a full lobby authentication replacement is outside this change.

## Authority and retries

Clients submit only their own faction and an index from their issued draft pack, with a revision and request ID. They never submit a winner, mission stars, a roster, permanent tiers, currency, or MMR. Extra authority fields are rejected.

The service executes the actual checked-in tile combat and shared troop/campaign hooks with a seeded RNG and virtual clock. It freezes each player's collection for the match, normalizes permanent stats for ranked games, and stores compressed authoritative replays. Human sessions require two independently authenticated verified profile seats in a public lobby with protected `hostUid` and `members` metadata. Ranked versus casual is selected by the lobby host through protected `forgeRanked`; a participant request cannot override it. Legacy name-only rooms and private invitation rooms retain hosted practice, with no ranked progression, until a separately authorized server invitation workflow is available. Neither a legacy host snapshot nor participant agreement about a winner grants rewards.

Packs have a 60-second server deadline. Missing choices are selected legally by the server. Battle advancement waits for the recorded replay duration, with a 20-second minimum. Unchanged read requests do not write snapshots. The browser renderer performs no progression writes.

Match receipts and upgrade receipts are committed atomically. Concurrent/repeated requests cannot spend or earn twice. Browser mutations retain request IDs through uncertain network errors and page refreshes. Reconnecting with the same identity resumes `activeSessionId`, including the persisted battle replay.

A human may surrender only their own authenticated seat. A human ranked surrender is recorded as a forfeit, so abandoning cannot erase a losing rating result. Surrenders before a completed authoritative round or before 20 seconds award no cards/dust and give the winner no MMR; the quitter can receive a 25-point penalty. Ordinary ranked transfers are zero-sum and cannot exceed the loser's current rating. Each pair has at most three rated results per UTC day, limiting repeated opponent and zero-rating-floor farming. Solo abandon and unfilled lobby cancellation grant no rewards or rating changes.

Rewards are configurable in `server/faction-forge/domain.cjs`: bot/endless wins cap at ten reward grants per UTC day; campaign replays cap at five; human wins cap at ten, with at most two reward grants against the same opponent each day. Campaign first clears and bosses have separate bounded ledgers. These limits reduce automatic farming and do not claim to prevent all collusion.

## Data/API

- `GET /profile`
- `POST /sessions/solo`: `{mode, missionId?, faction}`
- `POST /sessions/pvp`: `{lobbyId, faction, ranked}`
- `GET /sessions/:id`
- `POST /sessions/:id/pick`: `{index, sequence, requestId}`
- `POST /sessions/:id/abandon`
- `POST /upgrades`: `{troopKey, requestId}`
- `POST /packs`: `{packId, requestId}`
- `POST /reward-target`: `{troopKey}`

All except `/health` require a Firebase ID token in the Bearer Authorization header. Ownership reads use server-only bindings. Profile documents live at `factionForgeProfiles/{uid}`; history and upgrade receipts are subcollections. Match state, seeds, frozen collections and compressed replays live under Admin-only `factionForgeMatches/{id}`.

Campaign IDs are `forge-01` through `forge-30`; `c1m1` through `c5m6` are accepted aliases. Rewards preserve the existing `crowncouncilElo` field for legacy profile/leaderboard readers. Bot estimated MMR is session metadata only and never creates a leaderboard profile.

## Local verification

```sh
npm run test:faction-forge
npm run test:faction-forge:rules
npm run test:faction-forge:browser
```

The rules command starts only a demo-project Firestore emulator. It tests private ownership reads, protected rating/currency/result/upgrade writes, cross-account denial, compatible unrelated profile updates and legacy hosted snapshots. Its Admin integration exercises ownership migration, simultaneous upgrade retries, cross-instance profile/session recovery and persistence of real deterministic combat replay. It does not access production Firebase.


