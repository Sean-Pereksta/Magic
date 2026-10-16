# TurnCraft profile saves and Firestore multiplayer

TurnCraft uses the existing Game Hub profile (`gh.username`) and Firebase
Authentication session. The selected hub profile takes precedence over an old
username in a game link. A battle keeps the profile that entered it; switching
profiles cannot give another profile its progress or result.

## Publish

Publish the updated website, including `game/turncraft.html`,
`game/turncraft-lobby.html`, `game/turncraft/` and the hub launch settings.

The Firestore rules supplied with the bug report already allow the new account
and lobby paths through recursive `users` and `lobbies` grants. The complete
positive save/ranked integration passed against those exact rules, so no new
permissions are needed if they are the deployed rules. The repository's stricter
rules also support these flows and enforce atomic ranked receipts and immutable
completed results. If the stricter repository rules are deployed, publish the
updated version with the website:

```sh
npx firebase deploy --project bible-game-246c0 --config firebase.rules.json --only firestore:rules
```

The website has no frontend build step. This change requires no Cloud Functions,
additional game server, service-account key or new username binding. The complete
shared rules also serve the other hub games. The updated file preserves the
supplied Gridbound durable-room and reward validators, as well as existing
username-scoped saves for other games. Merging website code alone does not
publish Firebase rules.

The supplied broad recursive grants permit more writes than the repository
rules. Firestore combines matching allow statements with OR, so adding a
restrictive block beside a broad grant does not enforce receipt protections.
Adopt the updated stricter ruleset to enforce those checks.

## Profile data

```text
users/{username}.gameStats.turncraft
users/{username}/turncraftHistory/{lobbyId}
users/{username}/turncraftSoloHistory/{runId}
users/{username}/turncraftSaves/active
```

The profile stores campaign stars/unlocks, survival records, achievements,
multiplayer totals and ranked MMR. Updates preserve the rest of the account and
other games' statistics. The profile screen lists recent campaign, survival,
custom and ranked games. Each multiplayer history document is also an immutable
receipt: retries, page reloads and concurrent tabs count the match once.

Campaign and survival checkpoints contain a packed battlefield snapshot under
the selected profile, with a separate local fallback for that profile. Reopening
loads the latest cloud checkpoint. Completed runs have stable IDs for history and
queued offline retries. Finishing an old queued run cannot erase a newer run's
checkpoint. Signed-out or disconnected play remains saved locally and retries
when its original profile reconnects.

## Lobbies and ranked play

```text
lobbies/{lobbyId}
lobbies/{lobbyId}/chess_starfront/state
lobbies/{lobbyId}/chess_starfront_presence/{player}
lobbies/{lobbyId}/chess_starfront_commands/{command}
lobbies/{lobbyId}/turncraft_private/invite
lobbies/{lobbyId}/turncraft_joins/{uid}
lobbies/{lobbyId}/turncraft_forfeits/{uid}
lobbies/turncraft/invites/{code}
```

All live session state and invitation metadata live under lobbies. Custom rooms
support two to four human commanders and optional AI fill. Ranked Match finds an
open ranked room or creates one waiting for an opponent. Ranked duels require
exactly two humans, public visibility and no AI. Both players select factions and
ready up before the host starts.

A win grants 25 MMR, a loss removes 25 down to a minimum of zero, and a draw grants
zero. Campaign, survival and custom games do not change MMR. Rating counters,
ranks and history save together in one transaction for each participant, based
on the immutable completed lobby. Reopening the command center retries pending
match receipts. Returning from an active ranked game records a forfeit; the
current controller publishes the final result. Controller leases allow the
remaining browser to take over after a disconnected host's lease expires.

Ranked battles are simulated by a leased participant browser. Firestore rules
validate membership, controller authority, finished results and receipt-based
rating changes; they do not independently replay combat or provide a trusted
anti-cheat adjudicator. Global survival rankings remain unavailable.

## Verification

```sh
node --test game/turncraft/tests/match-results.test.cjs game/turncraft/tests/profile-persistence.test.cjs game/turncraft/tests/shattered-progress.test.cjs game/turncraft/shattered-front/tests/campaign.test.mjs
node --test game/turncraft/shattered-front/tests/profile-browser.test.cjs
npx firebase emulators:exec --project demo-turncraft --config firebase.turncraft-test.json --only firestore "node game/turncraft/tests/profile-store.emulator.cjs && node game/turncraft/tests/lobby-store.emulator.cjs && node game/turncraft/tests/turncraft-rules.emulator.cjs && node game/turncraft/tests/game-profile-compat.emulator.cjs"
```

After deployment, select a hub profile, complete a campaign run and reopen it.
Check the same profile from a second device, switch to another hub profile and
verify isolation, then play a ranked duel in two authenticated browsers. Confirm
that each profile records its own outcome once, including after reconnecting,
and that a custom rematch leaves ranked MMR unchanged.

The compatibility integration also exercises Faction Forge's actual account
selection and migration, optional inaccessible legacy saves, same-profile
reopening under a different anonymous Auth UID, separate profile progress, and
reward/purchase/upgrade/round replay protection. To run the same positive checks
against another deployed rules file, set `GAME_PROFILE_RULES_FILE` to its path
inside the emulator command. The test always uses a local demo project.
