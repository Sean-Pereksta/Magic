# Faction Forge: Firestore deployment and save recovery

Faction Forge stores profiles, decks, operation receipts and multiplayer room state in Cloud Firestore. Firebase Authentication supplies the identity used by Firestore rules. Cloudflare R2 supplies artwork. The game does not select an HTTP progression service, Realtime Database, Firebase Storage or Firebase Functions. The old optional server files are retained for historical compatibility and are not used by this browser entry point.

## Why the pasted rules break decks and campaign entry

The rules supplied on October 22, 2026 whitelist profile fields without `decks`, `selectedDeckId`, `selectedDeckIndex` or `deckImportAccountKey`. The current client includes deck defaults when creating or migrating a profile. Firestore therefore rejects the entire transaction, including a first profile load. Deck saves and selection fail for the same reason. The old menu hid this failure behind a generic connecting message, and it also allowed actions before the initial Auth callback finished.

Publish the complete checked-in `firestore.rules`, not a replacement rule allowing all authenticated writes. Both the private guest validator and the hub profile validator must call `forgeDeckFields` and allow those four fields. The shared deck validators near the top of the rules enforce three canonical starters, at most nine custom decks, five distinct troops from one faction, unlocked custom troops, a valid selected index/ID, and bounded edits. Once migrated, a stale write cannot remove the saved deck library. Multiplayer deck/body validation is also included. Existing unrelated game rules remain in this complete file.

## Deploy together

The October 24 recruitment update removes the accidental five-body restriction. Decks still contain five distinct troop types; armies can recruit further copies. Multiplayer battle writes now use `unitGroups` keyed by side and troop type, with board dimensions for growing armies. Publish the complete updated rules before the static game files so multiplayer accepts the new snapshots. Old flat snapshots remain readable. No saved-profile migration or graphics upload is needed.

1. Merge the game repair and publish the updated static files, including `game/crowncouncil.html`, the Faction Forge modules/styles and the new `profile-connection.js` module.
2. In Firebase Console, choose project `bible-game-246c0`, open Firestore Database > Rules, replace the rules with the complete `firestore.rules` file, and Publish. Alternatively use the command below from the repository root.
3. Ensure Firebase Authentication > Sign-in method > Anonymous is enabled. Authentication is still needed to authorize Firestore requests.
4. Reopen Faction Forge or choose **Retry profile connection**. Create and select a custom deck, clear the first campaign mission, then reload and confirm the same deck, stars, cards and Dust remain.

```sh
firebase deploy --only firestore:rules --project bible-game-246c0 --config firebase.rules.json
```

No rules have been published to production by the automated tests. Emulator checks use `demo-faction-forge` only. No additional Firestore indexes are needed for these document transactions.

## Save paths and consistency

A selected Game Hub profile saves to `users/{username}/factionForge/profile`. Reopening the same hub username selects the same document, including after an anonymous Auth UID changes. A guest without an existing hub profile saves privately to `factionForgeSoloProfiles/{uid}`; clearing browser identity can lose access to that guest save. Old matching UID/scoped saves migrate without deleting their source, balances, cards, upgrades, campaign progress or immutable receipts. Keep the original browser identity available until migration completes.

Deck saves, selection, rewards, upgrades and purchases use Firestore transactions. The menu waits for initial Auth and profile readiness, shares concurrent connection attempts, and gives a bounded timeout with a retry action. Permission, disabled sign-in and network errors identify the relevant fix. A temporary reconnect keeps the last confirmed profile visible. Account switches clear the previous account's view and reject stale writes.

An uncertain reward/purchase/upgrade keeps its original request identifier in a small device retry queue. Durable game data and operation receipts remain in Firestore; the queue is not a second cloud database. Receipts prevent duplicate spending or rewards. Failure to replay one queued reward keeps the loaded profile available and preserves that reward for retry. An interrupted battle itself is not resumed by the personal save mode.

## Deck graphics and Cloudflare

The deck stack artwork is drawn with CSS, and each deck displays five small portraits from the existing troop sprite sheets. The main menu also shows the selected deck and lineup. No new bitmap, manifest, R2 object or Cloudflare upload is required for this repair. Publish the CSS/JavaScript with the website. Existing artwork continues to load from the `assetBaseUrl` in `config.js`, with fallback icons if unavailable.

## Existing ownership limitation

The hub currently trusts the selected username and broadly authenticated access; its Forge documents are not restricted to a durable verified account owner. This compatibility behavior was already in the supplied rules and is preserved so existing users can reopen saves across anonymous sign-ins. It is not strong account security. Simply adding `request.auth.uid == profile.uid` would strand those existing cross-device saves. A future hub-wide migration must establish durable Firebase Auth sign-in and independently verified username bindings before tightening those permissions. Firestore can remain the only database during that migration. Private UID guest saves are already owner-scoped.

Personal client-played progress is not verified competitive authority and does not update protected ratings.

## Validation

```sh
npm run test:faction-forge
node --test game/faction-forge/tests/profile-connection.test.mjs
npm run test:faction-forge:browser
npm run test:faction-forge:rules
```

The rules suite exercises actual Firestore client transactions in the local emulator, including custom deck creation/editing, starter selection, legacy migration, reopen, malformed deck rejection and protection against erasing deck metadata. Browser tests cover connection delays/retry, no external service routing, campaign entry, deck building and mobile layouts.
