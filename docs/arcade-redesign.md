# Cat’N Mice arcade redesign

The existing root page and lobby URLs remain the entry points. This redesign adds a
small, dependency-free presentation layer to the existing loader; the game registry,
launch guards, account services, lobby transactions, Firestore rules, and individual
game saves retain their existing behavior. No redirects or data migrations are needed.

## Interface

- Discover leads with original Cat’N Mice artwork and game search, then recent games
  when present, four flagship cards, and three collections. Additional editorial
  shelves and the existing spotlight remain available in an expandable section.
- All Games includes all 72 games, tools, and experiments. Development status combines
  with search, genre, mode, and sort. Results and active filters are announced, and
  Clear filters restores the catalog while preserving the chosen sort. Experimental
  entries are visible in the current library without an additional reveal step.
- Multiplayer separates hosted lobbies from direct online-world entry. Existing
  filters, current-room controls, invitation behavior, saved campaigns, and host
  Start remain. The leaderboard is expandable below the play paths.
- My Games keeps favorites and recents together, with actionable illustrated empty
  states and an explanation of game-managed saves. Profiles remain optional.
- The root shell integrates Chat into the lobby header, avoiding two branded headers.
  A game opens the compact Lobby/Fullscreen/Reload/Chat toolbar. Chat remains gated by
  the same account and service logic. An unavailable SDK leaves game controls usable.
- Five-item mobile navigation, visible focus, dialog focus trapping/return, accessible
  icon labels, image fallbacks, and reduced motion are included. The spotlight rotates
  only when expanded, on Discover, with the document visible and motion allowed.

## Assets

The SVG brand mark serves as the header logo and browser icon. The original generated
hero has 1920×640 and 1200×400 WebP sources, plus a 768×512 mobile crop. The wide
source is approximately 78 KB. Existing game art is retained; Monkey Trails now has
the missing optimized tile, thumbnail, hero, and manifest entry.

See [artwork provenance](arcade-artwork-provenance.txt) for the exact generation prompt
and output paths. Category, navigation, and empty-state illustrations use coordinated
code-native SVGs. No game bundles are prefetched.

## Verification

```sh
node --experimental-vm-modules --test lobby/tests/*.test.cjs lobby/tests/*.test.mjs
node lobby/tests/arcade-hub-browser.mjs
node lobby/tests/shell-browser.mjs
```

The browser runners use Playwright. `PLAYWRIGHT_MODULE` can select an installed module;
`CHROMIUM_PATH` can select an installed browser. `SHELL_QA_OUTPUT` optionally writes
desktop/mobile screenshots and a JSON report. CI runs all lobby unit tests and the
current arcade acceptance suite on pull requests affecting the lobby or root shell.

The older `flagships-browser.mjs` and `arcade-nexus-browser.mjs` explicitly disable the
new Arcade enhancement to verify the retained flagship fallback. Their design
expectations describe that fallback, rather than the current presentation.

Checks use the actual loader and core with isolated service fixtures. They cover
catalog preservation, filters/sorts, favorites, recents, profiles, invitation preview
and confirmation, lobby creation/join/Start, persistent-world and solo URLs, download
links, responsive layouts, keyboard behavior, reduced motion, and unavailable assets.
The shell suite covers chat, iframe navigation, reload, fullscreen, return, and an
unavailable root SDK.

These are browser and contract checks, not a live production Firebase session,
real-device test, or gameplay/save-load test of every game. Individual game save code
and storage keys are unchanged. A pull request does not deploy the production site.
