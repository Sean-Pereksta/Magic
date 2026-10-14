# TurnCraft personal progress and multiplayer rollout

TurnCraft uses the existing CatnMice Firebase Authentication session and Firestore
database in project `bible-game-246c0`. Campaign records, survival records,
achievements and custom multiplayer use the site's existing Firebase connection.
The required rollout is the updated website, shared Firestore rules and artwork.
There is no additional game server to configure.

## Publish the update

1. Merge the PR and let the site's existing web deployment publish the updated
   `game/turncraft.html`, `game/turncraft/`, `game/turncraft-lobby.html` and hub/lobby
   files. The game has no frontend build step.
2. Publish the updated root `firestore.rules` to the same existing Firebase
   project if your deployment does not already do that. These shared rules allow
   the new personal progress fields while keeping competitive fields protected.
   You can publish the complete file in **Firebase Console → Firestore Database
   → Rules**, or run the following from the repository root with the Firebase CLI:

   ```sh
   npx firebase login
   npx firebase deploy --project bible-game-246c0 --config firebase.rules.json --only firestore:rules
   ```

   This command publishes the shared Firestore rules only. Review the complete
   file because the database also serves the other CatnMice games.
3. Upload `TurnCraft_ShatteredFront_Assets.zip` to the existing R2 artwork bucket
   under `shattered-front/`, preserving its directories. The expected first
   object is `shattered-front/ui/main_menu/battlefield.webp`. The existing public
   artwork prefix is already configured in the game; the separate R2 setup guide
   covers another artwork location.
4. Sign in through the existing CatnMice account flow, open TurnCraft, complete
   the first campaign mission and reopen the game. Check that the mission stars
   and unlocked operation remain. Repeat from another browser signed into the
   same account, then test a public custom room and a private invitation with
   two browsers.

No API URL, Cloud Functions deployment, service-account key or new account
binding migration is required for personal progress. Merging the PR does not
itself publish Firebase rules unless the site's deployment already includes
that step.

## Personal account progress

The game saves only its personal fields under the existing account document:

```text
users/{username}.gameStats.turncraft.campaign
users/{username}.gameStats.turncraft.survival
users/{username}.gameStats.turncraft.achievements
```

Updates merge these fields instead of replacing the account document or another
game's statistics. The site supplies the existing username and Firebase Auth
session. Existing verified account bindings remain honored by the shared rules;
this update does not require creating new bindings for ordinary personal saves.
Use the same CatnMice account to restore its cloud records on another device.

Campaign progress retains completed mission records, best stars, optional
objectives and unlocks across the six regions. Survival progress retains personal
best waves and arena records. Cosmetics are derived from personal achievements.
These browser-reported records are personal progress, not trusted competitive
results.

Signed-out play and an unavailable Firestore connection keep progress locally.
Local battle checkpoints also stay in that browser. The menu can retry pending
personal records when the account connection becomes available; a local battle
checkpoint is separate from the account's completed-mission and survival records.
The profile uses one Firestore listener while it is being watched; saves happen
for completed progress, rather than on every rendered frame.

## Custom multiplayer

Custom matches use the existing Firestore lobby and shared-state approach. The
dedicated `game/turncraft-lobby.html` supports public rooms, private invitations,
host configuration, faction selection, readiness, direct joins and reconnects.
Lobby listeners update the room, and the selected browser controller publishes
the shared match snapshots. Custom matches do not change ranked ratings.

Use two browsers to check room creation and admission, host settings, readiness,
launch, reconnection, a completed match and a rematch. Check that old completed
rooms do not overwrite the new room and that the other games' lobby flows still
work.

## Competitive features

Ranked matchmaking and rating updates are disabled in this Firestore-only
deployment. A browser-controlled match cannot establish a trusted competitive
result, so the game does not grant Elo/MMR from a client winner claim. Protected
rating fields remain protected.

Survival records and campaign achievements remain personal. Public survival
rankings are disabled until a trusted adjudicator exists. Neither deploying the
rules nor changing a browser preference enables competitive result validation.

## Usage and verification

The existing Blaze plan can incur normal Firestore usage charges for document
reads, writes, storage and network usage. Profile/lobby listeners and multiplayer
snapshots use that database too. This update does not promise zero charges; review
the project's usage and budget alerts as players use the game. See
[Firebase's Firestore billing documentation](https://firebase.google.com/docs/firestore/pricing).

Install the root dependencies and the game package to run local checks:

```sh
npm install
npm --prefix game/turncraft install
npm --prefix game/turncraft test
npm --prefix game/turncraft run test:warfare
npm --prefix game/turncraft run test:expansion
npx firebase emulators:exec --project demo-turncraft --config firebase.turncraft-test.json --only firestore "node game/turncraft/tests/profile-store.emulator.cjs && node game/turncraft/tests/lobby-store.emulator.cjs && node game/turncraft/tests/turncraft-rules.emulator.cjs"
```

The emulator uses a demo project and does not change production Firebase. Local
checks cover the actual RTS engine, mission and survival progression, saved
battles, offline result retries, account-save transactions, custom lobby behavior
and the shared Firestore rules. Finish the live
account-save and two-browser checks after the web files and rules are published.
