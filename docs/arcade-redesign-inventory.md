# Cat’N Mice redesign: existing functionality inventory

Audited before interface edits on 2026-11-04. Source: actual `lobby.html` loader, `lobby-core.html`, library, flagship, artwork and catalog modules, and their existing tests.

## Catalog and navigation

- 72 effective entries: 68 core entries plus Circuitbound, MTG Catalog, Fantasy Football and Monkey Trails injected by the loader. 50 support solo and 31 support multiplayer (some both).
- All 13 current categories remain searchable: Strategy, Adventure, Party, Survival, Apps, Cards, Action, Faith, Arcade, RPG, Educational, Builder and Sports.
- Existing internal view IDs and `gh.lastTab` values are `discover`, `singleplayer` (all-game library), `multiplayer`, `mygames` and `profile`; invalid saved tabs open Discover. Invite links take precedence and preview Multiplayer without automatically joining.
- Library searches title, description and category; filters category and solo/multiplayer; sorts featured, title, recent and play count. Experimental and in-production projects remain in the registry. Catalog organization supplies status badges/groups and expands prototypes on search.
- Existing editorial tiers derive from `lobby-showcase.js`; they are presentation metadata, never a second source of gameplay routes or permissions.

## Launch and route contracts

- Keep every registry field and actual route/alternate route/download route. War Realms solo uses `?mode=singleplayer`; Bible Roguelike uses `?mode=roguelike`; Faction Forge solo uses `?mode=solo`.
- GameHubLibrary action selection respects `modes`: solo, hosted lobby, persistent realm and matchmaking are distinct. No solo action for Cat & Mouse or other multiplayer-only games. Shared worlds must not gain hosted lobbies.
- Four persistent realms: Mountain Blade Online, Crownlands, Rock Paper Scissors MMO and Gridbound. Card Realms Multiplayer uses direct matchmaking. TurnCraft and Quip Clash use their dedicated lobby pages.
- `launchGameUrl` accepts only explicit `single-play`, `lobby-started`, `online-world-join` or `matchmaking-join` reasons. A lobby launch requires actual started membership. Duplicate launch guards remain.
- Solo launch is user initiated; guest play works except games requiring an account. Analytics or stalled cloud writes must never prevent solo launch.
- Preserve username encoding and selected single/multiplayer routes. TurnCraft/Quip Clash preserve dedicated create/browse/join mode, lobbyId, username, inviteId and room settings.
- Downloads remain available for IRONFIELD, Apes Together Strong and Hearthmouse.

## Multiplayer and social

- Hosted browser retains name/host/game search, game, public/private and open-slot filters. Private rooms are visible only to their host, players or invited users. Rooms sort newest first.
- Creating and joining retain account gates, player limits, public/private visibility and host-controlled Start Game. Creation must open current lobby and never start it automatically.
- Keep current-lobby membership, players, host status, leave, invite, copy-link and Start controls. Preserve realtime listeners, polling fallback, stale-room cleanup and Safari pagehide/pageshow recovery.
- Iron Thrones started campaigns persist with auth-UID members and resume through the current campaign list. Faction Forge ranked/casual setting remains.
- Invite URLs preview before joining. Accept/decline, username invitations, invite count, link copy and started-room membership guards remain.
- Parent shell owns chat, game frame, lobby return, fullscreen and reload. Its controls and messaging must remain connected to the existing game frame and lobby URL.

## Personal data and account contracts

- `gh.username`, `gh.lastLobbyId`, `gh.staleLobbyCleanupId`, `gh.lastGameType`, `gh.lastTab`, `gh.nextRoute`, `gh.favorites`, `gh.recentGames` and `gh.profilePromptSeen` retain meaning.
- Recents accept legacy strings and `{key, playedAt}` records. Ignore stale/deleted/inherited keys and retain order. Adding a recent updates locally before cloud work.
- Favorites require an optional profile, persist to local `gh.favorites` and cloud `users.favoriteGames`, and refresh every card instance when changed. Preserve removal and favorite pressed states.
- Game save data belongs to each game; do not rename origins, game URLs or game storage keys. Hub favorites/history do not imply cloud progress support in every game.
- Preserve login/create user, pending actions after account gates, remembered accounts, logout, display name, bio, avatar/category editing, own/public profile stats, rankings, member list, invites and password-gated admin member dashboard.
- Optional first-visit account offer remains nonblocking. Required multiplayer/account gates remain.

## Artwork and accessibility

- Existing manifest supplies tile/thumbnail/hero art and an original fallback. The initial audit found Monkey Trails missing from the manifest even though its native tile existed; the redesign repairs that omission. Preserve strong art, responsive source sets, lazy loading, explicit dimensions and layered image-error fallback. Do not preload game bundles.
- Retain accessible favorite labels/pressed state, action labels, focus visibility, 44px controls, reduced-motion support and usable narrow layouts.

## Verification plan

- Run existing unit suites with `node --experimental-vm-modules --test lobby/tests/*.test.cjs lobby/tests/*.test.mjs` (requires referenced game files for route assertions).
- Run existing `flagships-browser.mjs` and `arcade-nexus-browser.mjs` where browser runtime is available. These isolate Firebase and never write production data.
- Add meaningful checks for new status filters, result counts/clear, personal library navigation, empty-state actions and newly introduced keyboard behavior.
- Check 320/390/768/1440px layouts, visible focus, reduced motion, artwork fallback, loading and overflow.
- Distinguish fixture verification from live production Firebase, real multiplayer, real-device or per-game save persistence checks in the delivery report.
