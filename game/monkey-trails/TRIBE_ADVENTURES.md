# Shared weekly tribe adventures

Tribe members may opt in from the existing Social tab. A shared objective needs 600 contribution points. Every 50 accepted meters is one point; discoveries earn 3; completed regional challenges and ten-turn boss attempts earn 25; completed trades earn 15. Practice progress never contributes. The opt-in baseline excludes activity already earned that week.

The week starts Monday at 00:00 UTC. Each member keeps one stable `weekStart-uid` record tied to their first tribe for that week. Retries take the maximum of monotonic counters and transactionally add only the new point difference to the shared total. Rejoining the same tribe returns the existing baseline; another tribe cannot start a second objective for that player during that week. Members who leave cannot submit or claim until they return to their original tribe.

Contributing members can claim a completed goal once, during the objective week or the following week. The reward is six fruit, one Trail Berry, a rotating banner/emblem/Sanctuary decoration, and contribution titles. Offline counts from an enrolled previous week can reconnect during the following week; historical opt-ins cannot be created. Claims are immutable Firestore records. The encrypted save keeps an applied-claim ledger, so reconnecting, reloading or clicking again does not grant additional supplies. A defeated expedition defers delivery until a new expedition starts. The Sanctuary displays earned decorations and titles. No companion health, expedition phase, equipment or walking state is changed by a shared reward.

## Deployment

1. Publish the repository's updated **complete `firestore.rules`** to the existing Firebase project before serving the updated client. All existing game rules remain present. Using an already-configured Firebase CLI checkout, `firebase deploy --only firestore:rules --project YOUR_EXISTING_PROJECT_ID` deploys rules; otherwise paste the complete file in Firebase Console → Firestore Database → Rules and Publish. Do not replace unrelated project rules with only the new block.
2. Serve the changed static Monkey Trails modules and CSS together. No Node runtime, Cloud Functions or game server is needed.
3. The new documents live under `monkeyTrailsSocial/v1/adventures`, `adventureMembers`, and `adventureClaims`. They are created on opt-in/contribution/claim. No seed data or scheduled server job is required. The member query filters `tribeId` and `weekStart`; Firestore's normal equality index merging covers it. If custom index exemptions disable these fields, enable indexing for them.
4. Check two production accounts in the same tribe: opt in, contribute, reconnect, leave/rejoin, complete the total, and claim. Each user should get one claim and one local delivery. Verify encrypted cloud saves sync before moving between devices.

The Firestore rules enforce authentication, current tribe membership, stable identities, unchanged baselines, monotonic counters, exact point deltas, fixed reward contents, and immutable claims. As with the existing static browser game, accepted movement/activity is client-reported; rules cannot independently validate a physical GPS walk without a trusted game service. No route history is uploaded by this feature.

## Verification

`node --test game/monkey-trails/tribe-adventures-tests.mjs` covers baselines, retry races, membership changes, reward idempotence, offline claim windows, practice isolation and protected companion health. `npm run test:monkey-trails:social-rules` runs the existing rules suite plus shared-adventure permissions, delta checks, outsider denial, immutable claims and leave/rejoin behavior against the Firestore emulator. Node is a development/test tool only.
