# Arcade Nexus 2.0 — functionality preservation audit

Audited on 2026-10-14 against the original repository checkout. This is a behavioral contract for the presentation redesign, not a claim that production Firestore interactions were exercised. No repository-level or ancestor `AGENTS.md` was found. Game logic, save formats, ranking formulas and backend rules are outside the visual redesign.

## Actual rendering and integration pipeline

1. Root `index.html` provides the outer brand/navigation bar, `contentFrame`, return/reload/fullscreen controls, public/private chat, lobby advertisements, unread notifications and its own Firebase connection.
2. `lobby/lobby.html` fetches `lobby-core.html` with `cache: no-store`, inserts missing registry entries, rewrites featured flags, applies `GameHubLibrary.upgradeHtml`, optionally applies `GameHubFlagships.upgradeHtml`, appends the deferred catalog-organization module, and writes the complete document.
3. The core has **65** registry entries. The effective loader adds `circuitbound`, `mtgcatalog`, and `fantasyfootball` for **68 unique entries**. Its Receiver Window QB insertion is a compatibility fallback; that key already exists in the core and must not duplicate.
4. `GameHubLibrary` upgrades the historical singleplayer view to the whole catalog, retains the view ID, renders mode-specific actions, and expands favorites/recents to online-only games.
5. `GameHubFlagships` adds Discover and four flagship cards using the same registry/cards/action dispatch, moves the original featured carousel rather than cloning it, adds search/library shortcuts and recent cards, and focuses the current lobby after successful create/join.
6. `lobby-catalog-organization.js` decorates and groups existing cards/select options through observers. Its tier headings, badges and prototype toggles are behavior, not merely styling.

A visual enhancement failure must leave the working core/library available. Both upgrade functions are idempotent and reject missing/ambiguous transformation anchors before writing. Preserve that atomic fallback. Loading the core directly does not represent the full catalog or the final user experience.

## Non-negotiable launch boundary

`GameHubLibrary.getActions(game)` is the card action authority. `runAction(game, action, hooks)` rejects unsupported actions, calls `playSingle(game, true)` for solo, `openCreateLobby(game.key)` for hosted multiplayer and `joinOnlineGame(game, true)` for direct online/matchmaking routes. Never put direct links around whole cards or invent solo actions for online-only entries.

`launchGameUrl(url, reason)` accepts only:

| Reason | Required path |
| --- | --- |
| `single-play` | Explicit user solo action through `playSingle` |
| `lobby-started` | Current lobby exists, has `status: started`, and contains the current username |
| `online-world-join` | Explicit account-gated persistent-world action |
| `matchmaking-join` | Explicit account-gated matchmaking/dedicated multiplayer action |

`launchingGame` prevents repeated navigation and repeated counting; starting a game also stops lobby polling. `playSingle` and `joinOnlineGame` reject calls without their `userInitiated` flag. `GameHubLibrary.createGameCard` disables all launch buttons while one asynchronous action is pending and restores them after an error. Favorite clicks stop propagation.

Solo launches use `singleRoute || route`, append an encoded username only when `autoUsername` is set, save `gh.lastGameType`, and **never await analytics**. Local recent history must update even when cloud writes stall or fail. A remembered multiplayer lobby must never change a solo action into joining or starting a game.

### Game-specific launch contracts

| Registry key | Required behavior |
| --- | --- |
| `apestogetherstrong` | Solo only, `/game/apes-together-strong.html`; retain download action. Never creates a lobby. |
| `cat` | Multiplayer only; Create Lobby, Find Lobby, then explicit host Start. No solo action. |
| `warrealms` | Solo `/game/warrealms.html?mode=singleplayer`; hosted multiplayer with 2–4 players. UI disables Start below 2 and handler independently rejects. Username behavior retained. |
| `ironthrone` | Solo plus 1–6 human persistent campaigns; membership/host keyed by auth UID. Started campaigns can be rejoined by existing members. Launch includes encoded `returnUrl`. Preserve saved-campaign section and `data-resume-iron` actions. Do not delete started campaigns when leaving the launcher. |
| `turncraft` | Solo `/game/turncraft.html`; Join Match `/game/turncraft-lobby.html?mode=browse&username=...`; create `?mode=create&username=...`; a `turncraftVersion: 1` room opens the dedicated page with `?lobbyId=...`. Do not substitute generic hosted-lobby launch. |
| `crowncouncil` | Faction Forge retains `/game/crowncouncil.html?mode=solo`; hosted multiplayer uses auth UID members, max 2, explicit Start, `mode=multi`, `ranked=0/1`. `forgeRankedLobby` default is checked. Preserve `forgeJoin` handoff validated by matching session `ff.handoffLobby`. |
| `gridbound` | Always-online world; direct account-gated entry with `realm=public-gridbound-realm`; no host, Create Lobby or Start. |
| `mountonline`, `crownlands`, `rock` | Direct realm entry with each existing route/action label. |
| `cardrealmsmulti` | Direct matchmaking action `Find Match`; no generic room. |
| `estate` | Starting joins/creates its shared active Estate Defense game; use `activeGameId` for launch. |
| `stoneyrelic` | Minimum 2; retain Dungeon Master selection and `/game/dungeonmaster.html` versus player route with `role=dm/player`. |
| `bibleroguelike` | Solo `?mode=roguelike`; 1–5 player hosted variant and encoded username. |
| `biblegame` | Existing solo plus up to 20-player hosted match. |
| `village` | Account-required solo game; defer through account gate and retain encoded username. |
| All remaining entries | Preserve exact route case/spaces, modes, `multiTitle`, min/max limits, flags, downloads and registry key. Do not rename persistence keys when changing display titles. |

## Navigation, discovery and saved preferences

- Preserve internal `singleplayer`, `multiplayer`, `profile`, and `discover` view names and `gh.lastTab` compatibility, even if visible labels change.
- `showView` updates all matching desktop/mobile/shortcut controls, persists the tab, renders profile when needed, and enables lobby-browser refresh only when appropriate.
- `GameHubFlagships.initialView` opens Discover for first/invalid saved views; invitation links take priority and open Multiplayer without auto-joining.
- All four flagship cards are directly available. Existing editorial order can be intentionally changed in the new configuration, but their registry entries/actions cannot change.
- Discover search delegates to the existing library input, resets category to All, renders, switches views and focuses the search field. Find Lobby sets the game filter, clears text, selects all visibility and open slots, renders and focuses browser search.
- Full library search matches normalized title, description and actual category. Preserve sort modes popular/title/recent. Popular uses real recorded counts with title tie-break; recent supports both legacy strings and `{key, playedAt}` entries.
- Categories derive from the complete registry, including Apps and Educational. Keep all prototype, legacy, tool and online-only entries discoverable.
- Recent history stores at most 12 unique entries, library strips show up to 8, flagship recents up to 4, profile recents up to 10. These are reopen actions, not guaranteed resume promises.
- Favorites require an account, persist locally and to `users/{username}.favoriteGames`, and sync across Discover, library, online cards, profile and featured carousel. Deleted or inherited registry keys are ignored by library strips.
- Preserve downloads: Apes Together Strong and Hearthmouse. TurnCraft uses one cached remote-cover probe per document and tracked faction artwork when its R2 cover is unavailable; no retry loops on searches or favorite rerenders.
- Existing featured carousel retains Play, Favorite, downloads, previous/next, dots, swipe, inactive-slide `inert`/`aria-hidden`, selected-dot state and reduced-motion/hidden-tab timer behavior unless replaced with equivalent accessible functionality.

### Catalog organization behavior

- Current tiers are `showcase`, `playable`, `tools`, `prototype`; labels/badges, counts, ordering and select optgroups are created dynamically.
- Clean `(Incomplete)` from visible titles while preserving a raw title for classification. `Mountain Blade Skirmish` is a prototype alias.
- Prototype cards remain in the DOM/catalog; `data-prototypes-open=false` hides them by default. A visible Show prototypes button expands them. Nonempty search opens prototype groups so a matching prototype can be found.
- Observe and regroup `singleGameGrid`, `lobbyGrid`, `onlineWorldGrid`, `newLobbyGame` and `lobbyGameFilter` after dynamic updates without observer loops or stale selected values.
- Decorate recent/favorite/profile cards without changing registry titles or their route identifiers. Tools must remain reachable; the original tool list is not exhaustive of Apps and may be improved by the centralized configuration.

## Multiplayer browser, rooms and continuation

- Browser data comes from Firestore waiting-room snapshots plus 4-second direct-read safety polling while Multiplayer is visible. Preserve source-order protection so stale pending reads cannot replace newer snapshots, listener error handling, retry on visibility/focus/pageshow, and no duplicate subscriptions.
- Filter by room/host/game text, game, public/private, and open slots; sort newest first. Empty/loading/offline states must remain actionable and truthful.
- Public rooms are visible to guests. Private rooms are visible only to their host, their players and invited users. Do not fabricate lobbies/player activity or reveal private details in Discover.
- Room cards retain title, game, host, player count/capacity, avatars/profile links, current-room state, capacity display, copy link, host Invite, and Join/Open Lobby/Full action. Full rooms are disabled except for existing participants reopening them.
- Creation requires a user, nonempty name, hosted-lobby mode, selected public/private visibility and the correct game-specific fields. It leaves an existing room before creating, writes waiting status, retains host/members data, saves keys and subscribes to the new room. Successful create/join opens Multiplayer and focuses the current panel; it never starts a game.
- Join is a transaction: validate existence/status/mode/capacity, retain existing UID-member campaign reentry, add membership once, clean consumed invites, subscribe and focus. Missing/full/started rooms show errors.
- Current room retains artwork/name/game/privacy, players with profile buttons, host badge, host/player role, waiting-for-host text, invite/copy/start/leave controls. Start is host-only, has an in-flight guard, validates minimums and starts transactionally.
- Started-room navigation occurs promptly for the host and listener/safety-read participants. Invite cleanup and analytics never delay redirect, including Mobile Safari. Current-room safety reads run at 1 second only while active/visible and stop on exit/launch.
- Leave clears local UI immediately, transactionally removes membership, transfers legacy host when necessary, deletes empty rooms and cleans invitations. Iron Thrones preserves started campaigns. UID-membership Faction Forge/Iron Thrones behavior must remain distinct.
- `gh.lastLobbyId` on a fresh load is **cleanup information only**. Never automatically resubscribe, restore membership or launch its game. Failed cleanup retries through `gh.staleLobbyCleanupId`.
- Legitimate continuation is explicit saved Iron campaign actions or the proof-checked Faction Forge handoff. Existing game save files/formats and game-specific cloud progress must remain untouched.
- `pagehide.persisted` (back-forward cache) preserves membership, then `pageshow` resets transient flags and reconnects. Permanent exit attempts best-effort removal and records stale cleanup. Game navigation preserves membership by setting `launchingGame`.

## Invites and invitation links

- Header invitation dropdown, count, user label, notification pulse, empty/guest states and live updates remain.
- Only a room host sends username invitations; reject self-invite/nonexistent/already-present players. Resolve exact username then case-insensitive matches. Preserve deterministic invite IDs and `toUsernameLower` subscription.
- Join/Decline actions and cleanup for closed/started/non-hosted rooms remain. Invitations allow otherwise-private rooms to appear in browser.
- Copy link retains current lobby origin/path and `?lobby=...`, with clipboard and manual-copy fallback.
- Incoming `?lobby=` remains a preview. It is consumed from the URL, requires account when needed, validates waiting hosted room, and shows explicit `linkLobbyJoinBtn` before any membership change. A chat advertisement uses this same preview flow.

## Accounts, profiles and member administration

- Preserve guests, login, signup, existing-account auto-login, logout and pending-action replay for join/preview/create/profile/solo/online. Username from URL is consumed after initialization; values remain encoded in game routes.
- Keep current validation, Enter-to-submit, busy/error handling, tab switching, close/backdrop behavior, account-modal focus trap, Escape handling and mobile visual-viewport fit.
- The production flagship upgrade already replaces a blocking first-visit creation prompt with an optional inline profile offer. Preserve optional signup plus the underlying onboarding compatibility function/keys; do not reintroduce mandatory signup for guest-playable games.
- Account changes sync invitation subscriptions, local/cloud favorites and recent history, profile caches, daily metrics and chat UID linkage. Logout leaves current room, locks admin, clears relevant keys/pending action and updates guest presentation.
- Profile retains display name, avatar (emoji and approved `/graphics/*.png` formats), bio, wins, favorites, recents, most played, ranks/ELO, username/category/account/invite details, edit and logout.
- Editing preserves display-name/bio limits, emoji/preset/image avatar selection, favorite category, save errors and refreshing related member/current-room views.
- Public profiles are available from leaderboard/member list/current-room/player previews and parent-chat messages. Keep lazy avatar hydration, profile cache invalidation and asynchronous render race protection.
- Rankings merge existing `users` / legacy `Users` and Bible records. Preserve Faction Forge, Chess Warlord and Bible ELO formulas/tiers, no-data labels and existing field fallbacks. Do not invent new ranks/achievements/activity counts.
- Leaderboard uses real wins, top 8 and profile links. Members menu shows the latest 50 by last login, profile icons and real stats. Daily registered/guest counts use America/New_York day boundaries and one session mark per day.
- Admin panel is available only for exact username `Sean` and an explicitly unlocked session; remembered name alone must not unlock. Password verification, clear-on-check, Lock/Logout, stale asynchronous result protection, complete member list (no 50 cap), search/sort/refresh/activity details and missing-field handling remain.
- Member activity transformation must not expose password or `chatAuthUid`. Existing legacy auth and Firestore architecture are preserved here; this audit does not claim the legacy client gate is a server-enforced role.

## Root-page feature contract

- Keep `contentFrame` with existing permissions and the surrounding top controls. The lobby must not duplicate the root chat/fullscreen/reload/return bar.
- `returnToLobby` exits fullscreen if needed, closes chat, clears `gh.lastLobbyId`, resets share state, shows loader and loads the existing lobby URL. `reloadLobby` reloads the actual current frame (including games), with lobby fallback. Frame load hides loader and syncs username.
- Fullscreen targets the iframe itself, including prefixed Safari APIs and options fallback. Keep `aria-pressed` and label state. Message handling checks `event.source === frame.contentWindow`, not the stale original frame src.
- Accepted child messages: `chess-warlord-fullscreen`, `game-fullscreen-request`, `game-fullscreen`; actions enter/exit/toggle. Parent emits `game-fullscreen-state`, and on failures `game-fullscreen-result` with `ok:false` and reason.
- Parent profile buttons send `{type:'game-hub-open-profile', username}`; core accepts only messages from its parent and opens the public profile. Preserve this exact hook.
- Chat drawer retains open/close FAB, inaccessible/inert closed state, focus, login note, input/send, Enter submit, messages, avatars/profile links, unread badge/count/accessible label and scroll-sticking behavior.
- Public chat and private `@username` / `@"Full Name"` messages remain; private recipient resolution, sender/recipient UID and participants filtering, self-message/missing-recipient errors, 500-character cap and username suggestions remain. No production chat writes are required to verify presentation.
- Public and private messages are merged/sorted, limited to the latest 100, retained for 24 hours, and cleaned in existing batches. Preserve subscription errors and private-subscription replacement on identity changes.
- Advertise is available only for a verified waiting public lobby containing the current user. Preserve per-lobby 30-second session cooldown, real room details, joinable advertisement cards and validation before opening invitation preview (closed/started/private/full failures).
- Identity/room changes sync by storage/focus/visibility events plus the existing 1.2-second same-tab check, since iframe same-tab storage changes do not fire the outer storage event.

## Persistence names and route/message hooks

| Name | Existing use |
| --- | --- |
| `gh.username` | Shared identity in lobby/root/chat |
| `gh.lastLobbyId` | Active room to root while live; cleanup-only on fresh core startup |
| `gh.staleLobbyCleanupId` | Retry failed membership cleanup |
| `gh.lastGameType` | Last game registry key |
| `gh.lastTab` | Historical navigation preference |
| `gh.nextRoute` | Declared legacy compatibility key |
| `gh.favorites` | Local saved game keys |
| `gh.recentGames` | String or timestamped recent entries |
| `gh.profilePromptSeen` | Optional onboarding memory |
| `gh.du.marked.YYYY-MM-DD` / `gh.dg.marked.YYYY-MM-DD` | Per-session daily metric marks |
| `ff.handoffLobby` | Session proof for Faction Forge handoff |
| `gh.chatAd.{lobbyId}` | Session advertisement cooldown |
| URL `lobby`, `forgeJoin`, `username` | Preview, proof-gated handoff, identity initialization |
| Game URL `gameId`, `username`, `realm`, `role`, `returnUrl`, `mode`, `ranked`, `lobbyId` | Preserve game-specific route contracts |

## Baseline validation and recommended preservation tests

Baseline command: `node --experimental-vm-modules --test lobby/tests/*.test.cjs lobby/tests/*.test.mjs`.

Baseline result before application edits: **57 passed, 1 failed (58 total)**. The one failure is the installed Apes route existence assertion, because this working copy initially uses sparse checkout (`.github`, `lobby`) and omits `game/apes-together-strong.html`. `git ls-tree HEAD game/apes-together-strong.html` confirms the route is tracked (blob `361991b7c558e3b5b5c5e0fd1220804dcb6ffa96`). Materialize required paths before the final route/asset verification; do not classify the sparse omission as a product regression or hide the failed assertion.

Existing useful suites:

- `lobby-library.test.cjs`: all mode/action mappings, unsupported action rejection, full-registry filter and persisted online favorites/recents, exact routes, idempotent upgrade, loader insertions, module syntax, TurnCraft artwork cache and button race recovery.
- `solo-launch.test.cjs`: immediate guest/member solo navigation while cloud writes stall, local history offline, explicit-user/account gates, encoded route params, duplicate suppression, Faction Forge solo isolation from remembered rooms.
- `flagships.test.cjs`: persisted view/invite precedence, registry/control ID preservation, unchanged functional launch handlers, atomic fallback/idempotency, real loader insertions, account-only refresh.
- `profile-onboarding-admin.test.mjs`: onboarding compatibility, admin password/name/lock/stale-response checks, all-members normalization/search/sort, secret-field exclusion.
- `flagships-browser.mjs`: real loader/card DOM with isolated Firebase fixture, desktop/mobile navigation/search/favorites/guest gates/create-without-start, solo and TurnCraft dedicated routes, remote-cover success/failure, reduced motion, no horizontal overflow and touch-target size.

Final tests should exercise behavior, not merely match rewritten implementation strings:

1. Run the real loader in a browser with mocked Firebase; compare baseline and final unique registry keys, routes/modes, required DOM hooks and action sets. Verify every effective entry has successfully decoded art and a visible fallback when requests fail.
2. Use a named fixture user and rooms for public/private/full/waiting/started states. Verify guest gates, allowed visibility, transactional joins, create/join focus, host-only Start, War Realms/Stoney minimums, route params and no navigation before Start.
3. Test startup with stale `gh.lastLobbyId`, a `?lobby` link and the Faction Forge handoff with matching/missing proof. Assert no auto-join/launch for stale state and previews; verify explicit Iron resume path and started-campaign preservation.
4. Force realtime listener errors, resolve a stale read after a newer snapshot, hide/show and focus/pageshow the tab; verify reconnect, no stale room replacement, no duplicate polling/subscriptions and correct cached-page membership.
5. Test login/signup/logout and pending actions with fixture records, favorites/recent sync across all surfaces, profile edits, ranking fallback data and admin race cancellation. Keep secrets out of rendered DOM.
6. Load root index with iframe fixture: return/reload, fullscreen state/request messages and source rejection, public/private chat rendering/unread and ad preview validation. Test presentation with fixtures; do not send real messages/invites or change production accounts.
7. Capture desktop/laptop/tablet/320–390px mobile/landscape and zoom states, long titles, modals, search empty state and missing art. Confirm 44px actions, keyboard/focus, no hover dependency, safe-area padding and no page overflow.
8. Record same-browser baseline and final image requests/bytes, initial DOM/script costs and repeated navigation. Check reduced motion, hidden-tab pause and bounded decorative sprites. Avoid presenting fixture/network-isolated timing as production performance.

The final handoff must report which live authenticated backend flows were verified and which were covered only through fixtures/source preservation. Automated visual/fixture coverage does not establish production account, chat, invitation or campaign writes.

## Exact original static DOM IDs

These IDs are script/test integration anchors. They may be moved and restyled, but must remain unique and reachable. Additional new IDs are allowed. The lists below are extracted from original HTML before the redesign.

### lobby/lobby-core.html

`accountButton`, `accountButtonText`, `accountGateModal`, `accountMessage`, `accountModal`, `accountModalTitle`, `adminLock`, `adminMemberRows`, `adminMembersContent`, `adminMemberSearch`, `adminMembersLocked`, `adminMembersMessage`, `adminMemberSort`, `adminMembersPanel`, `adminMembersTitle`, `adminMemberSummary`, `adminPassword`, `adminRefreshMembers`, `adminUnlock`, `avatarEditPreview`, `avatarPicker`, `categoryPills`, `continueSection`, `continueStrip`, `copyInviteBtn`, `createConfirm`, `createLobbyGameInfo`, `createLobbyMessage`, `createLobbyModal`, `createLobbySubmit`, `createPane`, `createPassword`, `createPasswordWarning`, `createSubmit`, `createUsername`, `currentLobbyArtwork`, `currentLobbyContent`, `currentLobbyCover`, `currentLobbyEmpty`, `currentLobbyGame`, `currentLobbyName`, `currentLobbyPanel`, `currentLobbyPlayers`, `currentLobbyRole`, `currentLobbyState`, `dailyGuestCount`, `dailyUsersCount`, `dailyUsersPill`, `editProfileBtn`, `editProfileModal`, `favoritesSection`, `favoritesStrip`, `featuredDots`, `featuredHero`, `featuredNextBtn`, `featuredPrevBtn`, `featuredTrack`, `forgeRankedLobby`, `gameSearch`, `gameSort`, `gateCreate`, `gateLogin`, `gateNotNow`, `inviteCount`, `inviteList`, `inviteMessage`, `inviteModal`, `invitePanel`, `invitePlayerBtn`, `inviteToggle`, `inviteUserLabel`, `inviteUsername`, `leaderboardEntries`, `leaveLobbyBtn`, `linkLobbyInfo`, `linkLobbyJoinBtn`, `linkLobbyMessage`, `linkLobbyModal`, `lobbyGameFilter`, `lobbyGrid`, `lobbySearch`, `lobbySlotFilter`, `lobbyVisibilityFilter`, `loginPane`, `loginPassword`, `loginPasswordWarning`, `loginSubmit`, `loginUsername`, `memberList`, `memberPanel`, `memberToggle`, `modalLayer`, `newLobbyGame`, `newLobbyName`, `onlineWorldGrid`, `onlineWorldSection`, `openCreateLobby`, `profileAvatar`, `profileAvatarEmojiInput`, `profileAvatarInput`, `profileBio`, `profileBioInput`, `profileCategoryInput`, `profileContent`, `profileDetails`, `profileDisplayNameInput`, `profileFavoriteCount`, `profileFavorites`, `profileGuest`, `profileLoginBtn`, `profileLogoutBtn`, `profileMessage`, `profileMostPlayed`, `profileName`, `profileRanks`, `profileRecent`, `profileRecentCount`, `profileWins`, `publicProfileAvatar`, `publicProfileBio`, `publicProfileContent`, `publicProfileLoading`, `publicProfileModal`, `publicProfileMostPlayed`, `publicProfileName`, `publicProfileRanks`, `saveProfileSubmit`, `sendInviteSubmit`, `showCreateTab`, `showLoginTab`, `singleGameGrid`, `startGameBtn`, `toastStack`, `view-multiplayer`, `view-profile`, `view-singleplayer`, `visibilityPrivate`, `visibilityPublic`, `waitingForHost`

### index.html

`activeLobbyHelp`, `activeLobbyLabel`, `chatContainer`, `chatFabIcon`, `chatLoginNote`, `chatMessage`, `chatMessages`, `chatUnreadBadge`, `chatUser`, `chatUserSuggestions`, `closeChat`, `contentFrame`, `frameLoader`, `fullscreenLobby`, `fullscreenLobbyIcon`, `fullscreenLobbyLabel`, `reloadLobby`, `returnToLobby`, `sendBtn`, `shareLobbyBtn`, `toast`, `toggleChat`

### Additional generated/module hooks

- Flagship/navigation: `view-discover`, `cnmDiscoverTitle`, `cnmSearchForm`, `cnmSearch`, `cnmFlagshipGrid`, `cnmRecent`, `cnmRecentTitle`, `cnmRecentGrid`, `cnmProfileOffer`, `cnmCreateProfile`, `cnmMore`, `cnmMoreTitle`, `cnmMoreMount`, `cnmLibraryPicks`, `cnmLibraryGrid`; `[data-view]`, `.cnm-find-lobby`.
- Cards: `[data-game-key]`, `[data-game-action=single|lobby|online]`, `.favorite-btn`, `.download-game-btn`, `.game-name`, `.game-description`, `.game-meta`, `.game-actions`, `.play-count`, `.game-art`; TurnCraft `.tc-portrait`, `.tc-cover`, `.tc-cover-ready`.
- Registry organization: `catalogOrganizationStyles`, `catalogLibraryNote`, `.catalog-shelf-heading`, `[data-catalog-shelf]`, `[data-catalog-status]`, `[data-prototypes-open]`, `.catalog-prototype-toggle`, `newLobbyGame` and `lobbyGameFilter` optgroups.
- Generated room/campaign: `forgeRankedLobby`, `ironSavedCampaigns`, `[data-resume-iron]`, `.join`, `.copy`, `.invite`, `.decline`, `.player-profile`, `[data-profile-name]`, `[data-avatar-for]`, `[data-profile-bound]`, `[data-avatar-loaded]`.
- Online worlds: `.online-world-card`, `.online-world-title`, `.online-world-desc`, `.online-world-badges`, `.join-online-world`.
- Featured carousel: `.featured-slide`, `.featured-play`, `.featured-favorite`, `[data-featured-key]`, `.featured-dot`, `[data-featured-index]`.
- Public module API: `GameHubLibrary.{getActions,runAction,createGameCard,upgradeHtml}`, `GameHubFlagships.{PICKS,MARKER,STARTUP,upgradeHtml,initialView,selectPicks,recentKeys,mount,refresh,filterLibrary,viewChanged,revealLobby}`; `window.logout`.

## Post-implementation preservation review

Reviewed the implemented loader, core, library, flagship module, centralized showcase/artwork modules, catalog organization and root page on 2026-10-14. Application files were inspected read-only by this audit.

The root `index.html` contains one inline JavaScript block; its contents are exactly unchanged from repository HEAD after normalizing line endings. This preserves the existing parent-frame navigation, return/reload/fullscreen messaging, public/private chat, unread tracking, advertisements and identity synchronization implementation.

The core registry is exactly unchanged when evaluated and compared as JSON. Independently executing the original and final loaders also yields exactly identical effective registry JSON: **68 unique games**, including the dynamically inserted entries. All **112 original named core functions** remain. The **40 behavioral source blocks** below were compared to HEAD and are unchanged:

`launchGameUrl`, `requireUser`, `playSingle`, `joinOnlineGame`, `requestJoinLobby`, `joinLobby`, `openCreateLobby`, `createLobby`, `startGame`, `redirectStartedGame`, `removePlayerFromLobby`, `leaveLobby`, `handleStartupLobbyState`, `handleAuthenticatedUser`, `beginExitLobbyCleanup`, `listenToCurrentLobby`, `readCurrentLobbySafety`, `refreshLobbyBrowser`, `applyLobbyBrowserSnapshot`, `login`, `createUser`, `autoLogin`, `afterUserLogin`, `logout`, `runPendingAction`, `toggleFavorite`, `logGamePlay`, `addRecentGame`, `sendInvite`, `listenForInvites`, `renderInvites`, `validateInvites`, `cleanupInvitesForLobby`, `getProfileBundle`, `ranksForBundle`, `renderProfile`, `canViewAdminMembers`, `unlockAdminMembers`, `lockAdminMembers`, `loadAdminMembers`.

Eight presentation/navigation helpers are intentionally adjusted: `gameImagePath` (an arrow helper), `gameVisual`, `renderLargeGameArtwork`, `showView`, `renderSingleLibrary`, `renderFeatured`, `showLobbyConnectionState` and `lobbyCard`. These changes cover artwork resolution/lazy images, library play-mode/featured sorting, truthful room metadata, missing-view navigation fallback and synchronized Discover connection errors. The implementation adds My Games, a configurable premium shelf, thumbnail search suggestions and real available-room previews. These reuse the existing registry, card action dispatch, visibility checks and room join controls. Library and flagship upgrades still preserve original control IDs and use atomic transformation/fallback behavior. GameHubLibrary's `getActions` and `runAction` remain unchanged.

The wrapper loads showcase configuration before the flagship module captures it. The graphical fallback resolves the manifest fallback through the configured asset base; emitted image dimensions match the generated 16:9 assets. Root toast/fullscreen/advertisement contrast corrections preserve the parent controls' readability. No game implementation or save-format changes are required by the redesign. The unrelated Windows `CatLanes`/lowercase-alias materialization collision discovered during work was restored and is absent from the final application diff.

Final rerun after artwork completion: the combined preservation/artwork suite reports **63 passing out of 63**, with no failures. This includes all effective catalog routes, centralized editorial references, ready/nonempty artwork for every game, CDN/fallback/escaping, launch dispatch, registry/control preservation, navigation, favorites/history, and account/admin checks. The initial sparse-checkout Apes route assertion passes after materialization. Browser/fixture results should be reported separately in the final PR validation.

### Limits to state in the PR

- Browser integration tests isolate Firebase and remote traffic. They exercise the real local loader and UI against fixture records; they do not establish successful production account, invitation, private-chat, lobby or campaign writes.
- Tracked route validation establishes that every registry route and alternate action targets a repository file. It does not exercise gameplay, each game's cloud saves or every remote production dependency.
- Source preservation covers the exact legacy auth, host/start, admin and private-room logic. This visual change does not migrate or strengthen those underlying backend/auth mechanisms.
- Desktop/mobile screenshots, missing-art and reduced-motion checks use a desktop browser with emulated viewports. Real iOS/Android keyboard, Safari back-forward cache, native fullscreen permissions and low-power hardware remain useful device checks.
- Before/after fixture measurements must be compared under the same browser and asset conditions; they are regression evidence, not public-site loading guarantees.
- Artwork readiness/asset assertions now pass for every effective catalog key and the fallback. Those checks establish coverage and files, while visual quality remains a separate screenshot/artwork review.


## Final curation follow-up

The user's follow-up requires the best and fully built-out games at the top, with other games grouped below. The centralized configuration now promotes **24 games**: four flagships, eight premium picks and twelve additional source-backed developed games. The remaining catalog partitions into **16 In Production**, **23 Experimental** and **5 Tools & Learning** entries, covering all 68 keys exactly once. See [catalog tier evidence](arcade-nexus-catalog-tiers.md) for the complete explicit lists, supporting source/docs/tests and the conservative lower-tier default. This is configurable editorial curation rather than a runtime quality score; gameplay categories, modes, routes, account gates and launch requirements remain unchanged.
