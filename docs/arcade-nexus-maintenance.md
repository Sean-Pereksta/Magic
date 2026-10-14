# Arcade Nexus 2.0 — maintenance guide

## Source of truth

`lobby/lobby-core.html` owns game definitions and the existing account, launch, multiplayer, profile and persistence behavior. `lobby/lobby.html` is the public loader and also inserts compatible supplemental games before presentation upgrades run. Read the **effective** registry after loading, not just the core array, when auditing coverage.

The functionality preservation contract and original integration hooks are documented in [arcade-nexus-functionality-audit.md](arcade-nexus-functionality-audit.md).

## Promote a game

Edit `lobby/lobby-showcase.js`:

- `flagships` sets the primary collection and display order.
- `premium` sets the secondary collection and display order.
- `builtOut` lists additional developed games; flagships, premium and builtOut together form the top Best Games tier.
- `tools` and explicit `experimental` overrides control the lower groups. Unpromoted games default to In Production; original incomplete titles default to Experimental.
- See [catalog-tiers](arcade-nexus-catalog-tiers.md) for the 24/16/23/5 partition and source evidence. Curation is editorial metadata, not a runtime quality score.
- Each item references an existing `GAMES` key and can supply `label`, `pitch`, `color`, `badges` and a presentation-only `categoryLabel`.
- `artworkOverrides` can map any registry key to custom `tile`, `thumb` and `hero` paths without changing game definitions. The standard paths remain in the manifest.
- `assetBase` is the base URL for artwork paths from the asset manifest. It defaults to `/lobby/assets` and can be changed to an HTTPS CDN/R2 prefix.

Use editorial copy here and retain the existing registry's exact routes, modes and player limits. An online-only game does not gain a solo action by being promoted. Future game launch behavior must be defined in the registry and supported by `GameHubLibrary.getActions`, not duplicated in promotional cards.

## Add a new game

1. Add one unique registry definition in the established core/loader location. Supply the actual working route, display name, description, category and existing supported modes. Set `singleRoute`, dedicated `multiplayerRoute`, `launchMode`, UID membership behavior or limits only when the game supports them.
2. Verify case-sensitive route paths and query parameters against the game itself. Preserve existing launch dispatch/account gates. A shared world uses realm entry; matchmaking uses its own entry; hosted rooms wait for explicit host Start.
3. Add optimized tile art and a thumbnail under `lobby/assets`, then add the matching registry key to `lobby/assets/game-artwork.js` / its structured asset inventory. An optional hero variant is appropriate for showcased titles.
4. Leave title text in the card's semantic HTML; artwork should be a recognizable scene rather than a title on a generic background. Keep the main subject visible in portrait/landscape crops and at thumbnail size.
5. Optionally add its key to the showcase configuration. Unlisted games must still appear automatically in the complete searchable catalog and their actual category.
6. Run the loader/catalog/action checks, verify the new routes/art files exist, and inspect both desktop and narrow mobile cards. Exercise all supported actions through the real card handlers.

## Artwork resolution

`lobby/lobby-artwork.js` reads `GameHubArtworkManifest.games[game.key]` and uses `thumb`, `tile` or `hero`. Relative manifest and override paths resolve against `GameHubShowcase.assetBase`; explicit HTTPS paths remain intact. The fallback path also follows the manifest and asset base. Existing registry artwork is retained as a compatibility source. Keep the manifest graphical fallback available for failed requests; it follows the same asset base. Unknown future games get this illustrated arcade scene until dedicated art is supplied.

Use small WebP thumbnails for compact/search/room views, tiles for normal cards and hero images only where useful. Do not preload the full catalog. Above-the-fold flagship art can be eager; offscreen cards remain lazy. If moving assets to R2, upload and verify every manifest path before switching the base and retain existing Cloudflare URLs where required.

TurnCraft uses its optimized local cover when the artwork module is available. The existing shared remote-cover probe and tracked faction fallback remain available if that optional module fails. Preserve its dedicated multiplayer URL and offline/online actions when adjusting its presentation.

## Safe presentation changes

- Keep old internal view names and persistence keys. Visible Library naming can change while `singleplayer` remains compatible with saved tabs.
- Retain unique core IDs, original buttons/handlers, host/admin gates and parent `postMessage` contracts. Move existing nodes instead of creating duplicate controls or subscriptions.
- Use real Firestore counts/statuses. Display loading/empty/unavailable states instead of made-up activity.
- A recent game opens the game again; call it a resume action only when the game has a supported continuation mechanism. Iron campaign resume and Faction Forge handoff have specific existing behavior.
- Decorative chases belong behind the interface, cannot capture input, and should use bounded reusable SVG/CSS sprites and predetermined transforms. Respect reduced motion and pause on hidden tabs.
- Consolidate shared tokens and module styles; do not layer conflicting theme files. Maintain the working loader's fallback if a presentation enhancement fails.

## Validation

Run the Node preservation suites with VM-module support:

```text
node --experimental-vm-modules --test lobby/tests/*.test.cjs lobby/tests/*.test.mjs
```

The browser integration fixture in `lobby/tests/arcade-nexus-browser.mjs` runs the real local loader and presentation files with isolated Firebase data and remote writes blocked. It accepts `--root`, `--asset-root`, `--output`, `--label` and `--measure-only` for repeatable before/after checks. Set `PLAYWRIGHT_MODULE` / `CHROMIUM_PATH` only when using a known available runtime/browser.

Verify the following in a real browser: all catalog entries and artwork, direct flagship actions, full library search/category/play-mode/sort, prototype expansion, favorites/recents, guest/account states, create/join without auto-start, minimum-player gates, dedicated worlds/matchmaking, profile/admin views, root return/reload/fullscreen/chat, missing-art fallback, keyboard access, reduced motion and mobile overflow. Production account/chat/invitation/campaign changes are separate from isolated fixture validation; report their actual verification status clearly.

For performance comparisons use the same browser, viewport, assets and network conditions. Record image requests/bytes, render readiness, DOM size, long tasks and repeated navigation. Network-isolated fixture timings are useful for regressions and are not production site load claims.

## Design ownership

Shared dark colors, fields, buttons, game cards, room cards, profile panels, modal layouts and responsive library sizing live in the existing `lobby-core.html` stylesheet. `lobby-flagships.css` owns Discover sections, hero/premium/compact variants, navigation and ambient characters. Catalog organization uses the same tokens. The root shell palette is updated in `index.html`; its JavaScript, iframe permissions, chat, fullscreen, return and reload behavior remain intact. No extra theme stylesheet or runtime dependency was introduced.

`lobby-flagships.js` creates new presentation views and calls the original game card/lobby card renderers. Discover activity reads the current lobby cache and its existing privacy predicate; it creates no additional Firebase listener. `mygames` is a new presentation view over existing favorites/history. `singleplayer` remains the persisted ID for All Games.

There are six original SVG characters on desktop and two on mobile, with three transform paths and a two-step running gait. All decoration is noninteractive and hidden from assistive technology. Reduced-motion and low-power modes use static silhouettes; hidden tabs pause both movement and gait.

## Assets and package

The artwork inventory includes every core and loader-added key, actual title/category/routes/modes, tier, provenance, and tile/thumbnail/hero paths. `tooling/build-lobby-artwork.py` rebuilds optimized files, inventories and the optional R2 ZIP from preserved sources. Consult its usage and the artwork documentation for source locations. Generation prompts are tracked with the assets; original game title art remains untouched.
