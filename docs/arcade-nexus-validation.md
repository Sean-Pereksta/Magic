# Arcade Nexus 2.0 validation

Validated on October 14, 2026 against the final artwork, 24-game curated collection, deferred Library rendering and mobile title fix.

## Results

- **63/63 unit and integration tests passed.** The original regression suite remains, with expectations updated for the requested flagship order and versioned script URL, plus My Games and Nexus coverage.
- **139/139 Nexus browser assertions passed**, with no uncaught browser errors.
- **11/11 legacy browser groups passed.** This fixture intentionally disables the optional new artwork enhancer to retain coverage of the original TurnCraft faction renderer, cached remote-cover fallback and launch actions.
- All **68 effective registry games** have tracked route destinations and ready, nonempty artwork files. This includes the three games inserted by the loader beyond the 65 core entries.
- The catalog contains **24 Built Out, 16 In Production, 23 Experimental and 5 Tools** entries. All 24 curated games appear in Discover; production and experimental projects remain accessible in the complete Library. Experimental games are collapsed initially and can be revealed.

## What was exercised

| Area | Evidence |
| --- | --- |
| Responsive layout | 1920×1080, 1440×1000, 768×1000, 844×390 landscape, 390×844 and 320×844; no horizontal page overflow. Logged-in Library, Multiplayer and Profile also fit at 320 and 390 pixels. |
| Artwork and mobile typography | Flagship artwork decoded at every viewport. Screenshots were captured after scrolling through the page and waiting for image decoding. Mobile hero titles exceed 30px; titles and action buttons remain inside their cards. Missing artwork reaches the graphical fallback and retains usable actions. |
| Discovery and catalog | Instant graphical suggestions, Escape dismissal, title/category searches, empty results, category changes, alphabetical/recent sorting, solo mode filtering and My Games quick access. Actual visual positions place Built Out cards above the other catalog tiers. |
| Deferred Library | Discover starts without building the hidden 68-card Library. Opening Library receives favorite changes made on Discover and the saved recent history. Repeated navigation preserves those controls and data. |
| Launch rules | Guest multiplayer/favorite gates, the Village account gate, Apes solo entry, War Realms solo parameters and two-player multiplayer requirement, Cat & Mouse multiplayer-only behavior, TurnCraft dedicated multiplayer entry, Faction Forge solo entry and Gridbound shared-world parameters. |
| Hosted rooms and campaigns | Create and join remain in the hub until the appropriate Start action. The host can Start; a nonhost waits. Host invitations, confirmed invitation URLs, private-room visibility, full-room restrictions and saved Iron Thrones campaign Resume were exercised with fixture data. |
| Accounts and administration | Fixture login, signup, favorite persistence, recent history, profile rendering/editing/logout, ordinary-member admin hiding, remembered-Sean locked state, wrong-password rejection, verified unlock, member filtering and Lock clearing member rows. |
| Activity and recovery | Honest empty-room presentation, private-room filtering shared with Discover, simulated authentication failure replacing Connecting with Unavailable, and usable solo actions after backend failure. A missing presentation script and remembered My Games tab fall back to a usable original view and full catalog. |
| Motion and navigation | Six decorative characters on normal desktop and two on narrow mobile; no pointer interception. Reduced motion and simulated constrained hardware stop decorative animation. Hidden-tab handling pauses both runners and their child animations and clears carousel/polling timers. Forty view changes retained exactly 2,130 live DOM elements before and after. |
| Parent website | Actual embedded lobby, iframe game navigation, Lobby return, Reload, native browser Fullscreen targeting the iframe, chat open/close, guest chat gate and lobby-ad rendering. Notification text contrast measured 14.23:1; active-fullscreen button text measured 9.00:1. |

The source preservation audit is recorded separately in [arcade-nexus-functionality-audit.md](arcade-nexus-functionality-audit.md).

## Before and after observations

The baseline lobby files were frozen before implementation. Both versions used the actual loader/core and local artwork, the same viewport sizes, a fresh browser context per scenario and the same isolated backend fixture. These are individual local observations from headless Edge 155 on Windows, without network or CPU throttling.

| Metric | Desktop before → after | 390px mobile before → after |
| --- | --- | --- |
| Initial image transfer | 18,788,959 → 1,517,232 bytes (**91.9% less**) | 18,788,959 → 986,452 bytes (**94.7% less**) |
| Live DOM elements at startup | 2,037 → 1,275 (**37.4% fewer**) | 2,037 → 1,275 |
| First contentful paint | 556 → 508ms | 184 → 172ms |
| Last pre-interaction largest-contentful-paint observation | 556 → 588ms | 304 → 304ms |
| Observed initial long-task duration, total | 92 → 182ms | 0 → 128ms |
| Idle task time over 1.5 seconds | 3 → 3ms | 4 → 4ms |
| Idle script/layout time, rounded | 0/0 → 0/0ms | 0/0 → 0/0ms |
| Sampled JavaScript heap | 7.60 → 6.37MB | 7.63 → 6.34MB |

Initial bytes and DOM size fell substantially. Paint observations and idle work remained similar. Startup long-task totals increased with the additional curated content and presentation work; the raw results retain this result. These samples do not establish production performance, physical-device frame rates or statistically significant timing differences. The runner's ready-wall value includes a fixed 350ms settling interval and is not presented as a production load-time measurement.

Paint, DOM and initial-transfer measurements are recorded before screenshot scrolling. Runtime samples are recorded after scrolling and decoding. The JSON also records image render dimensions, resource counts, animation state and view state.

## Reproduce

Install the repository's Playwright dependency and its Chromium browser, then run from the repository root:

```sh
node --experimental-vm-modules --test lobby/tests/*.test.cjs lobby/tests/*.test.mjs
node lobby/tests/flagships-browser.mjs
node lobby/tests/arcade-nexus-browser.mjs --label after --output ./work/qa-evidence
```

`PLAYWRIGHT_MODULE` can select an installed Playwright module. The legacy runner uses an importable module path or file URL; an absolute Windows module path should be expressed as a `file:///` URL. `CHROMIUM_PATH` can select a browser executable. The Nexus runner uses installed Edge by default on Windows and bundled Chromium on other platforms.

To compare a preserved pre-change checkout using the same artwork source:

```sh
node lobby/tests/arcade-nexus-browser.mjs --root <baseline-directory> --asset-root <checkout-directory> --label before --measure-only --output ./work/qa-evidence
```

`--functional-only` runs the behavioral scenarios without the six initial viewport captures. `--measure-only --widths 390,320` limits a responsive check to those two widths. The normal acceptance run omits `--allow-missing-art`; that flag is only for intermediate layout work before artwork is ready.

Evidence includes `before-results.json`, `after-results.json`, unit/legacy transcripts and final desktop, mobile, Library, Multiplayer, Profile, missing-art and parent-chat screenshots. CI uploads the configured evidence folder as `arcade-nexus-browser-evidence`.

## Scope and limits

The browser fixtures fulfill Firebase SDK requests with in-memory implementations and serve the parent site's `catnmice.com` resources from local files. Other remote traffic is blocked. Account and lobby writes affect fixture memory only. Game destinations are small local navigation stubs; their route parameters are checked, while the game engines themselves are outside this browser suite.

This verifies the real frontend, its event handlers and presentation integration. It does **not** verify production Firebase authentication, Firestore permissions, live multiplayer, real invitations or chat delivery, deployed hosting, physical phones or Safari behavior. Administrative checks establish the existing frontend's UI gates, not server-enforced authorization. No live account or production data was modified, and this validation does not constitute a deployment.
