> The economy and warfare expansion is documented in [EXPANSION.md](./EXPANSION.md), including regional production, construction tiers, contracts, recruitment and battle reports.
> [STRATEGY.md](./STRATEGY.md) explains player orders, autonomous rival turns, economic planning, campaigns and the Rival Turns reports.
> [MULTIPLAYER.md](./MULTIPLAYER.md) covers online campaigns, deployment, synchronization, population growth and emulator tests.

# The Iron Throne Engine

A kingdom strategy game for Cat'N Mice with local single player and persistent
online campaigns for one to six human rulers. Open `/game/iron-throne/index.html`
for single player, which needs neither Firebase nor a build step. Choose
**Iron Thrones → Online Multiplayer** in the shared lobby for House selection
and synchronized rounds; unclaimed Houses use the same existing AI engine.

## Play now

- Single-player campaigns offer 6, 8, 10 or 12 Houses on progressively larger seeded maps. Multiplayer retains its original six Houses and 40 × 30 world. See [world sizes and fog](FOG_OF_WAR.md) and [shared strategic operations](STRATEGIC_OPERATIONS.md).
- Found your kingdom before Turn 1: choose a natural starting region, keep capitals at least 8 hexes apart, and let AI settle the remaining land. See [FOUNDING.md](FOUNDING.md).
- Food, wood, stone, iron, gold, population, happiness and three tax policies.
- Farms, lumber camps, quarries, iron mines, roads, forts, towns, city upgrades,
  walls, markets and workshops. Locations, costs and construction time matter.
- Levies, archers, cavalry and siege stacks. Recruit, split, combine, march,
  hold/garrison, attack or retreat by ordering a destination. Roads, terrain,
  rivers, border access, enemy zones of control, walls and casualties affect orders.
- Five deterministic rival rulers build, expand, recruit, defend, wage war and
  conduct AI-to-AI diplomacy without making model requests.
- Alliances, peace, road trade, resource exchange, gifts, joint wars, defense,
  positioning, withdrawals, fort construction, frontier cessions, tribute,
  allegiance, future payments and treaty betrayal.
- Persistent, directional relationships: Opinion/Trust, respect, fear, wariness,
  grievance, trade dependence, reliability, generosity and perceived aggression.
  Each significant change records a cause; rulers remember deeds and public betrayals.
- Conversational promises need **Give My Word**. Conditional war guarantees,
  actual attacks, border withdrawals, defenses, fort construction, non-aggression
  and resource deliveries are verified against the board. Joint wars require new
  combat; defense requires two distinct turns on station.
- A dispatch strip opens compact chat over the map or the full council. Both use
  the same saved conversation, unread count, proposed terms and pledge ledger.
- Three outgoing dispatches per turn across Houses; Envoy Office raises this to
  four, Royal Chancery to five. An ambassador physically stationed at a foreign
  capital grants a separate ten-message allowance with that House.
- Civilian ambassadors travel faster than armies, can be recalled or reassigned,
  and are never combat casualties. Detention, expulsion and explicit execution
  have real diplomatic consequences. Execution requires a confirmation.
- Immediate/recurring resource exchanges, loans with repayment, military access,
  non-aggression and embargoes share the existing evaluator and ratification gate.
- Conquest: control at least 60% of current settlements. Crown Accord: maintain
  alliances/allegiance with a majority of surviving rival houses for three
  resolutions. Losing the player's last settlement ends the campaign.
- Local autosaves after actions and turns; export/import with version and data
  validation. Import and new-campaign replacement are explicit player actions.
- Desktop and touch controls, pinch zoom, keyboard map navigation, compact mobile
  panels, turn chronicle, treaty ledger and an introductory guide.

Start by building a farm or mustering troops at Emberkeep. Three construction or
recruitment orders are available each turn; workshops increase the limit. Armies
receive orders freely and march at turn end. Found towns on owned land or adjacent
neutral frontier tiles, at least four hexes from an existing settlement. Declare
war before invading; alliances grant passage. Armies stranded by peace or expired
access can still withdraw out of the former partner's territory.

## Gemini free-tier integration

**The static game is playable immediately. Live Gemini chat is disabled until a
site operator deploys the included Worker and configures it.** This repository
contains only public connection settings; private keys stay in Cloudflare.
When both connection settings are present, Gemini is enabled automatically on
new sessions, including resumed campaigns. Verification loads when a council
opens; starting a game never sends a model request.

This uses the **Gemini Developer API free tier from Google AI Studio**, not the
consumer Gemini chat website or a consumer chat subscription. A consumer chat
account is not an embeddable game API credential.

The design intentionally does not promise a universal free request allowance.
Google publishes project/model-specific active quotas in AI Studio. In particular,
the pricing page's search-grounding quota is not a general chat-generation quota.
Use a Free Tier project **without billing enabled** if zero paid usage is required.
Local caps cannot turn a paid-tier project into a free-tier project.

1. Create a Gemini API key in [Google AI Studio](https://aistudio.google.com/api-keys).
   Verify the project's tier, model availability and actual RPM/TPM/RPD limits at
   [AI Studio's rate-limit page](https://aistudio.google.com/rate-limit).
2. Create a Cloudflare **Turnstile** widget for `catnmice.com` and any other exact
   hostnames you use. The widget site key is public; its secret stays server-side.
3. Use a Cloudflare Workers account on its Free plan with SQLite Durable Objects.
   From `game/iron-throne/worker`, using a current Wrangler installation:

   ```sh
   npx wrangler secret put GEMINI_API_KEY
   npx wrangler secret put TURNSTILE_SECRET
   npx wrangler deploy
   ```

   Wrangler may require account login. Paste secrets into its interactive prompt,
   never into a source file, URL, commit, browser setting or shell command argument.
4. The deployment config and Worker fallback both use `gemini-3.5-flash`,
   the confirmed working model for this installation. Deploy the updated Worker
   to replace the old `gemini-2.5-flash-lite` runtime setting. The explicit
   `GEMINI_MODEL` in `worker/wrangler.toml` is the deployment source of truth;
   future model changes should update it and `DEFAULT_GEMINI_MODEL` together.
   `keep_vars = true` preserves unrelated dashboard variables.
   Set `ALLOWED_ORIGINS` to the exact production origins, with no paths or
   trailing slash. Keep `DAILY_LIMIT`, `REQUESTS_PER_MINUTE` and
   `CLIENT_PER_MINUTE` below your verified provider allowances. Supplied budgets
   of 20/day, 4/minute globally and 2/minute per IP are conservative application
   defaults, not Google quota claims. A zero daily limit disables upstream calls.
5. Update `game/iron-throne/config.json` with public configuration only:

   ```json
   {
     "diplomacyEndpoint": "https://YOUR-WORKER.YOUR-SUBDOMAIN.workers.dev/diplomacy",
     "turnstileSiteKey": "YOUR-PUBLIC-TURNSTILE-SITE-KEY"
   }
   ```

6. Publish that configuration through the site's normal GitHub workflow. Enter a
   ruler's council chamber, leave **Gemini conversation** enabled, complete Turnstile once to establish a diplomacy session,
   and send an envoy. Verify that the reply is marked Gemini, inspect the proposed
   terms, and ratify one. Check the pledge ledger and next turn's movement.

No private keys are accepted or stored by the browser. Disabling the Gemini checkbox
returns to the scripted council. The treaty desk remains available in every mode.

### Diagnose a failed conversation

If an attempted Gemini message uses local diplomacy, a **Diagnostics** button
appears beside **Send envoy** in both the compact chat and full council. It opens
the captured failure, including its stage, HTTP status, safe error code, suggested
next step, and separate results for `GEMINI_API_KEY`, `TURNSTILE_SECRET`, and
`BUDGET`. **Copy report** copies that information for troubleshooting; if clipboard
access is blocked, the report stays selectable for manual copying.

Deploy **both the game files and the Worker** for the full breakdown. In particular,
a `/session` 503 with `CONFIG_MISSING` lists each missing runtime setting. Add the
two secrets to the Worker’s runtime Variables and Secrets, and bind `BUDGET` to
`DiplomacyBudget` using the included Wrangler configuration. A successful build
alone does not prove these runtime settings are installed. Session establishment
does not call Gemini, so a failure there cannot establish a Google billing issue.
After a successful session, the report distinguishes Google-reported billing,
key/permission, model and quota failures from the game’s own request allowances.

“Present” means configured, not validated or funded. With an older Worker, blocked
connection or unreadable CORS response, unobservable settings are **Unknown**;
the report does not guess which secret or binding is missing. Reopen the council
to retry verification after fixing configuration. A successful Gemini reply hides
the button. Opening/copying a report never sends a verification/model request or
consumes a dispatch. Reports live only in page memory, contain no secret values,
tokens, IP addresses, message text, campaign data or raw provider error bodies,
and are not included in saves or exports.

### Cost and failure behavior

- Player messages and submitted structured offers may call Gemini. After a turn,
  at most one significant incoming dispatch can be voiced in the background when
  an authenticated session is already active. Its local text appears immediately;
  failures never block the map. Strategy, combat and rival-to-rival negotiations
  remain deterministic and make no model requests.
- Prompts contain the current message (600 characters), up to twelve recent
  messages, five retrieved memories, a bounded summary, priorities, legal offer
  reasons and a ruler-specific board snapshot. Foreign treasury balances and
  private conversations are excluded. Payloads remain below 22 KB before auth.
  Generated memory candidates and summaries are labelled unverified interpretations;
  they cannot replace board facts or grant resources, trust or completed oaths.
- Replies have a 2,048-token output ceiling and a validated JSON intent schema.
- A single SQLite Durable Object reserves attempts transactionally before each
  upstream call. Concurrent visitors cannot race a KV counter past the global cap.
- Server-derived, hashed IP identity controls short-term per-client limits.
  Turnstile is verified once at `/session`, against the expected hostname and
  action. The Worker issues an HMAC-SHA256 signed, origin/client-bound session.
  It expires after 30 minutes of inactivity and at most eight hours from issuance;
  active requests renew it. A consumed Turnstile token is never reused. Sessions
  live only in page memory, avoiding blocked third-party cookies and keeping
  credentials out of saves, exports and localStorage. Raw IPs are not persisted.
  Session issuance has its own transactional rate limit; obtaining another
  session cannot reset the IP-based model budget.
- Successful exact-context replies are cached for 30 minutes (at most 40 entries).
  Different state, history, memory, model or origin creates a different cache key.
- Failed attempts consume budget. No automatic retry, paid upgrade, grounding,
  other provider, model download or AI-to-AI generation is performed.
- Provider quota errors activate a shared cooldown. The client also backs off on
  errors. Timeout, rate limit, missing configuration, invalid JSON or interrupted
  network returns the scripted council without preventing turns or deals.
- Free-tier chat content may be used to improve Google's products. The council UI
  discloses this while Gemini is enabled. Messages and fictional state go to the
  configured Worker and Gemini for messages, structured offers and at most one eligible incoming dispatch per turn in that mode. There is no game telemetry.

The proxy limits spending attempts and validates the output shape; the **game
rules are authoritative**. This is a local singleplayer campaign, not a secure
competitive server. Editing one's own save or developer-tools state can cheat
one's own game. A multiplayer/leaderboard version would need server authority.

Official references, checked during implementation:

- [Gemini pricing](https://ai.google.dev/gemini-api/docs/pricing)
- [Gemini rate limits](https://ai.google.dev/gemini-api/docs/rate-limits)
- [Generating content](https://ai.google.dev/api/generate-content)
- [Structured output](https://ai.google.dev/gemini-api/docs/structured-output)
- [SQLite Durable Object storage](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Durable Objects on the free plan](https://developers.cloudflare.com/durable-objects/platform/pricing/)

## Code and checks

The existing site serves source files directly. Native ES modules and a Canvas 2D
hex renderer fit that setup without a new Phaser/Vite bundle or external CDN.
The simulation, diplomacy, renderer and network adapter are separate modules.

```sh
# Repository root; no dependency installation required for rules/proxy tests.
npm run test:iron-throne
node --experimental-vm-modules --test lobby/tests/lobby-library.test.cjs

# Optional functional browser checks, with Playwright and Chromium installed.
npm run test:iron-throne:browser
```

The browser test accepts `IRON_THRONE_CHROMIUM` for an existing browser binary and
`CODEX_PRIMARY_RUNTIME_NODE_MODULES` for the provided runtime dependencies. It
serves the real source on localhost and exercises desktop, mobile portrait and
mobile landscape. It creates no HTML preview or screenshot artifact.

For local play, serve the repository with any static HTTP server and open
`/game/iron-throne/index.html`. ES modules require HTTP(S), not `file://`.

The included GitHub workflow runs rule/proxy tests and the existing lobby tests.
Provider behavior is tested with mocked HTTP responses, including quota failure,
malformed output, concurrent reservations, caching and privacy boundaries. A live
Gemini/Turnstile/Cloudflare smoke test remains a post-configuration check.

## Boundaries of this playable release

The brief's core turn-based game, strategy AI, menu diplomacy and Gemini adapter
are implemented. Optional fog of war, long-distance supply attrition, WebLLM,
Groq/OpenRouter failovers, telemetry and external art/audio packs are omitted.
Visuals are original procedural canvas/CSS, with no external asset licensing or
runtime downloads. Verified ruler memory compresses locally. Optional model interpretations are collected from the same conversation response, without extra summary calls. Each exchange offers one resource in each direction; recurring shipments and loans have their own obligations. Road-trade revenue still requires connected roads. Territorial deals are
limited to adjacent, unoccupied noncapital settlements/forts; capitals and last
settlements cannot be purchased. General roleplay threats have no hidden power:
only reviewed, ratified supported intents enact resource, treaty or military obligations. Threats and insults can worsen relations, while courteous speech has a small lifetime influence ceiling.

## Map presentation

Terrain and buildings use native canvas artwork cached in a bounded sprite cache.
Six terrain types have deterministic surface textures; production sites, towns,
forts and cities have distinct artwork. Walls, markets, workshops and construction
progress reflect real tile state. Roads, bridges, coastlines and house frontiers
are layered beneath structures. Army formations use cached troop silhouettes, shields, bows, horses, siege engines and House banners while badges retain readable troop counts. Ambassadors have distinct map markers.

Only visible tiles and armies render. Detail reduces at world zoom, display scale
is capped at 2×, and selection pulses stop after 650 ms. Battle events drive two-second clash,
projectile, casualty, retreat and siege effects with at most six active encounters
and 72 particles. A brief result lists actual before/after counts and retreats;
input stays available. Reduced-effects mode and system reduced-motion settings
skip motion. Effects stop when tabs are hidden. There is no continuous idle
animation or external art download. The welcome citadel is a bundled SVG, and existing campaigns migrate automatically.

## Deploying the living diplomacy update

Deploy the updated Worker **and** the static files. The new browser expects the
`/session` endpoint; an older Worker will safely leave it in local diplomacy until
redeployed. From `game/iron-throne/worker`, run `npx wrangler deploy` through the
existing Cloudflare account. Existing `GEMINI_API_KEY`, `TURNSTILE_SECRET`, origin
allowlist and Durable Object binding remain in use. No Durable Object migration
or Firebase change is needed.

A dedicated signing key can optionally be installed with
`npx wrangler secret put SESSION_SECRET`. Without it, the existing server-only
Turnstile secret signs purpose-separated session credentials. Rotating either
signing source invalidates existing sessions. Never put secrets in config.json.
The public config continues to use the existing `/diplomacy` URL. A Worker root
URL is also normalized automatically by the client.

Gameplay message allowances do not raise the operator's Gemini budget. The
conservative 20/day, 4/minute global and 2/minute per-client defaults remain;
local diplomacy supplies responses after those budgets are exhausted.

Campaign format is version 3. Version 1/2 imports and the existing browser save key
migrate automatically, retaining the board, existing resources and conversations while defaulting new expansion fields.
New relations, causal history, trade, civilian units, confirmed oaths, proposals,
unread messages and model interpretations survive export/import. Authentication
credentials and transient battle particles are deliberately outside saves.

Session protocol references:
- [Turnstile tokens: single use and server validation](https://developers.cloudflare.com/turnstile/get-started/server-side-validation/)
- [Cloudflare Workers Web Crypto](https://developers.cloudflare.com/workers/runtime-apis/web-crypto/)


## Cloudflare PNG artwork

`asset-manifest.mjs` defines the R2 base URL and all 171 active relative PNG keys (including 41 directional geography overlays). Opening
Iron Thrones loads and decodes the entire manifest before enabling Begin/Resume,
with a progress meter, six concurrent requests, ten seconds per request, and a
45-second startup budget. The lumber camp is attempted first. Missing or stalled
images do not block the campaign: catalog items use bundled SVGs and terrain and
overlays use procedural drawing. Failed remote keys are retried on the next page
opening, so uploading a listed PNG needs no code change.

The page shares decoded images across canvas renderers and retains them for the
session; panning does not start new downloads. Terrain variants are coordinate
stable. Sprites preserve aspect ratio and alpha. Resource and catalog images are
decorative and do not capture input. Titles without supplied PNG keys remain SVG.
Set `globalThis.IRON_THRONES_ART_DEBUG = true` before loading to log successful
requests; failed requests always log their full URL. Change only
`IRON_THRONES_ASSET_BASE` to move the bucket to a custom domain.

Checks: `node --test game/iron-throne/tests/art.test.mjs` and, with Playwright and
Chromium installed, `node game/iron-throne/tests/art-browser.mjs`. The browser
check mocks PNG availability, including a missing file, on desktop and mobile.


### Repeated `GEMINI_MODEL` / Google 404 after a deployment

A Google 404 on `/diplomacy` means Google could not serve the selected model.
The game reaching this step already reached the Worker and its budget binding.
The old Wrangler file explicitly pinned `gemini-2.5-flash-lite`; redeploying that
file could overwrite a working dashboard model selection. The current config
and code fallback both use the user-confirmed working `gemini-3.5-flash`.
The diagnostics now report the
normalized model ID and whether it came from the runtime setting or the default.
The last expansion changed the diplomacy prompt/schema, not the selected model;
that does not prove which model the live Worker was using during an old failure.

Deploy the updated Worker to apply `GEMINI_MODEL=gemini-3.5-flash`. For an
immediate dashboard correction, set that same value under the Worker's runtime
Variables and Secrets and choose Deploy. If availability still fails, set
`GEMINI_API_KEY` in a local environment
using the same project key as the Worker and run from the repository root:

```sh
node game/iron-throne/worker/check-models.mjs
```

This calls Google's paginated model-list endpoint, filters for `generateContent`,
and prints IDs only. It never calls generation, changes the Worker, or prints
keys/provider error bodies. Select a compatible text model with an available
free tier in your project, update runtime `GEMINI_MODEL`, and deploy the Worker.
Do not paste API keys into chat or commit them. One normal in-game message after
the cooldown confirms actual generation; metadata alone does not verify quota,
billing, or response-schema compatibility. Model names with surrounding spaces
or Google's `models/` prefix are normalized. Invalid IDs fail before an upstream
request. There is no automatic retry or switch to another model/provider.


### Map artwork visibility

Ready terrain PNGs replace procedural ground; troop sprites replace procedural
formations, and construction-stage sprites replace procedural scaffolding.
Fallbacks run only when the corresponding image is unavailable. Road sprites
also replace their procedural road strokes. Army badges, ownership boundaries,
selection indicators and construction progress meters remain gameplay UI.

Constructed buildings, settlement improvements, roads, bridges, construction
stages and troops have an alpha-shaped faction-color contour with a thin dark
outer edge. The original artwork is drawn once over the contour. This follows
transparent PNG silhouettes instead of outlining their rectangular bounds.
Opaque-background assets must have transparent backgrounds for that effect.
Contours use canvas compositing without pixel readback, so remote R2 sprites
need no additional CORS headers. Small display-sized outline surfaces are cached
with a 192-entry limit; faction changes invalidate the color variant, and zoom
and display-density buckets keep the edge readable without rebuilding each frame.

### Gemini 3.5 reply speed and invalid replies

Gemini 3.5 Flash requests use `thinkingConfig.thinkingLevel = MINIMAL` and a
2,048-token output cap (previously 700). Google counts thinking tokens toward
that cap. The cap is a maximum, not a requested reply length. Sampling overrides
are omitted as recommended for Gemini 3.5. The 1,600-character visible-reply
limit and strict intent validation remain enforced.

An HTTP 200 from Google followed by `GEMINI_RESPONSE_INVALID` is a reply validation
failure, not proof of rate limiting. Diagnostics distinguish an output-limit stop,
other incomplete generation, empty reply, malformed JSON, and invalid schema.
Truncated responses get `GEMINI_RESPONSE_TRUNCATED`. Neither partial replies nor
invalid intents are accepted. These reply failures use a five-second manual retry
wait; there is no automatic extra request. Per-minute local limits now return the
actual seconds until their next window instead of an extra fixed minute. Genuine
Google quota errors retain their existing cooldown, and request allowances remain
2/client/minute, 4 globally/minute and 20/day unless explicitly configured otherwise.

Deploy both the Worker and game files for the shorter retry wait: older game code
imposes a minimum 30-second wait even when the Worker asks for less. Live Gemini
latency depends on Google; mocked regression tests do not measure production speed.

## Connected rivers and coastlines

The map now derives six-bit edge masks in clockwise E, SE, SW, W, NW, NE order.
`geography.mjs` derives reciprocal river links and coast masks from actual neighboring
terrain. Missing board neighbors count as the surrounding ocean. All land types
receive shores when they face water, even if they are not labeled `coast`.
Coast terrain uses adjacent mainland textures, with mountain neighbors mapped to
foothills. Terrain underfill removes the transparent gaps around photographic hexes.

`geography-art.mjs` draws transparent overlays at exact shared edge coordinates.
Straight channels, both bend angles, all fork/junction masks, springs, and flared
mouths share the same port width. Coast curves include single shores, corners,
disjoint inlets, peninsulas and islands, with beach and rocky cliff treatments.
Shared endpoints and endpoint tangents keep shore curves continuous across tiles.
The bounded 256-sprite cache draws before roads, bridges, buildings and units.
Topology is cached and invalidates on actual geography changes, including imports.

The graphics are delivered separately in `Iron_Thrones_Cloudflare_Geography.zip`:
41 transparent 1024×1024 PNG overlays. Extract the ZIP and upload the `geography`
folder at the root of the existing R2 bucket, preserving filenames. For example:
`https://pub-47f679f65f034fbda4c4b2ee31b3818a.r2.dev/geography/coast_beach_01.png`.
Do not upload the ZIP itself as an image. Reload the game after uploading.

`geography-assets.mjs` is the shared catalog for the ZIP paths and image selection.
The map loads Cloudflare images, rotates canonical masks in 60-degree steps and
composites shore, river and mouth layers. If any required layer is unavailable,
the entire tile overlay uses its connection-preserving native fallback. Uploaded
PNGs are preferred as soon as the full set is decoded; no mixed partial rivers.
Images are deliberately not committed to the PR. The code includes the original
vector drawing definitions and an SVG export script for reproducible PNG packaging:


```sh
node game/iron-throne/assets/generate-geography.mjs /absolute/output-directory
node --test game/iron-throne/tests/geography.test.mjs
```

Hexadecimal filenames encode canonical six-bit masks; `canonicalMask()` supplies
the clockwise rotation needed to reproduce any of the 64 configurations. The mouth
overlay opens toward E and rotates to any sea edge; it composes with ordinary
river assets so junctions also support sea mouths. The retired coast PNGs
and three river PNGs are no longer selected or included in the startup preload.

Existing saves are not rewritten. Their river booleans define the original graph.
One outlet per connected component prefers adjacent sea; a bounded search can
extend through at most two unoccupied coastal land tiles to repair the old
one-tile gap. The remaining endpoints are explicit springs/pools. Legacy data
contains no elevation: equal-length outlet routes use a stable southern preference.
This is a visual topology system, not a drainage/elevation simulation; river
crossing costs, resources and terrain remain unchanged.
