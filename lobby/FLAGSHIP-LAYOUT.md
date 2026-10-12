# Cat’N Mice flagship-first layout

The root homepage already embeds `lobby/lobby.html`. This change redesigns the
shared hub instead of replacing the root shell or creating a second launcher.
The parent page, chat, fullscreen, reload, account logic, game registry, actual
routes, downloads, analytics, Firestore rules and game implementations are not
changed.

## Navigation and play

- Discover is the default for a new visitor or an invalid saved tab. Valid
  `gh.lastTab` values still restore Library, Multiplayer, Profile or Discover.
  Invitation links select Multiplayer and retain their existing confirmation.
- Four stationary flagship cards: Apes Together Strong, War Realms, Cat & Mouse,
  Iron Thrones. Apes has the large lead panel. All artwork comes from the registry;
  failed artwork retains the native fallback. No game bundles are prefetched.
- Buttons reuse `GameHubLibrary.createGameCard` through the original `gameCard`
  bridge. The original account gates, route variants and launch reasons remain.
  In particular, Cat & Mouse remains lobby-only; no nonexistent solo mode is added.
- Hosted games also have Find lobby: this selects the existing game and open-slot
  filters in Multiplayer. It does not create, join or launch a game.
- After successful lobby creation or joining, Multiplayer opens and the current
  lobby panel receives focus. The host must still press Start Game.
- The existing recommendation carousel moves below the four main cards on
  Discover. Its arrows, favorites and game actions are retained.
- Full Library, sort choices, categories, prototypes, tools, recent games,
  favorites, public/private lobbies, world entry, invitations, profiles, admin
  controls and saved Iron Thrones campaigns remain accessible.
- Search and category controls move above personal rows. Flagship shortcuts,
  recents and favorites collapse during a search/category filter, then return
  when filters clear. They are not deleted or removed from storage.
- Optional first-visit signup is an inline callout instead of an automatic modal.
  Create Profile opens the same account form. Required multiplayer/account gates
  have not changed.

## Integration and validation

`lobby-flagships.js` is a small, dependency-free presentation layer loaded by the
existing loader. `upgradeHtml` checks every integration anchor, returns a single
complete transformed string, and is idempotent. An unavailable script or rejected
transformation falls back to the existing library-enhanced hub. No original core
HTML, game code, API configuration or permissions are modified.

Run:

```sh
node --experimental-vm-modules --test lobby/tests/*.test.cjs lobby/tests/*.test.mjs
node lobby/tests/flagships-browser.mjs
```

The browser runner accepts `PLAYWRIGHT_MODULE` and `CHROMIUM_PATH`. It uses the
actual loader/core with isolated Firebase and artwork fixtures and blocks remote
traffic. It tests mobile widths, missing art, search, account gates, favorites,
solo routing and successful lobby creation. No screenshots or HTML previews are
produced. It is not a live multiplayer, production Firebase or real-device test.

Deploy the updated loader together with `lobby-flagships.js` and
`lobby-flagships.css`. A pull request is not a production deployment.
