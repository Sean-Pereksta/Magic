# The Iron Throne Engine

A playable singleplayer kingdom strategy game for Cat'N Mice. Open
`/game/iron-throne/index.html`, or choose **The Iron Throne Engine → Play** in
the lobby's Game Library / Strategy category. It does not create a multiplayer
lobby, require Firebase, or need a build step.

## Play now

- Six original houses on a seeded 40 × 30 hex world; Crossroads and Highlands presets.
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
- Promises have deadlines, fulfillment checks, reputation consequences and ruler
  memory. Joint wars require combat; defense requires two turns on station.
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
4. In `worker/wrangler.toml`, set `GEMINI_MODEL` to an available text model with a
   free tier in your project. The supplied starting model is
   `gemini-2.5-flash-lite`; it is configurable because models and availability
   change. Set `ALLOWED_ORIGINS` to the exact production origins, with no paths or
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
   ruler's council chamber, leave **Gemini conversation** enabled, complete Turnstile,
   and send an envoy. Verify that the reply is marked Gemini, inspect the proposed
   terms, and ratify one. Check the pledge ledger and next turn's movement.

No private keys are accepted or stored by the browser. Disabling the Gemini checkbox
returns to the scripted council. The treaty desk remains available in every mode.

### Cost and failure behavior

- Only explicit player messages can call Gemini. Turn resolution, armies,
  economy, rival-to-rival deals, ruler memory and menu negotiations are local.
- Prompts contain the current message (600 characters), six recent exchanges,
  five retrieved memories, a bounded summary and compact fictional world facts.
- Replies have a 700-token output ceiling and a validated JSON intent schema.
- A single SQLite Durable Object reserves attempts transactionally before each
  upstream call. Concurrent visitors cannot race a KV counter past the global cap.
- Server-derived, hashed IP identity controls short-term per-client limits.
  Turnstile is verified server-side against the expected hostname and action.
  User-supplied session IDs cannot bypass limits. Raw IPs are not persisted.
- Successful exact-context replies are cached for 30 minutes (at most 40 entries).
  Different state, history, memory, model or origin creates a different cache key.
- Failed attempts consume budget. No automatic retry, paid upgrade, grounding,
  other provider, model download or AI-to-AI generation is performed.
- Provider quota errors activate a shared cooldown. The client also backs off on
  errors. Timeout, rate limit, missing configuration, invalid JSON or interrupted
  network returns the scripted council without preventing turns or deals.
- Free-tier chat content may be used to improve Google's products. The council UI
  discloses this while Gemini is enabled. Messages and fictional state go to the
  configured Worker and Gemini only when the player sends a message in that mode. There is no game telemetry.

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
runtime downloads. Ruler memory compresses locally rather than spending extra
model calls. Diplomacy offers one resource in each direction per transaction;
ongoing trade revenue requires actual connected roads. Territorial deals are
limited to adjacent, unoccupied noncapital settlements/forts; capitals and last
settlements cannot be purchased. General roleplay threats have no hidden power:
only the reviewed, ratified supported intent can change the simulation.

## Map presentation

Terrain and buildings use native canvas artwork cached in a bounded sprite cache.
Six terrain types have deterministic surface textures; production sites, towns,
forts and cities have distinct artwork. Walls, markets, workshops and construction
progress reflect real tile state. Roads, bridges, coastlines and house frontiers
are layered beneath structures; army badges retain readable troop counts.

Only visible tiles and armies render. Detail reduces at world zoom, display scale
is capped at 2×, and selection pulses stop after 650 ms (disabled for reduced
motion and hidden tabs). There is no continuous idle animation or external art
download. The welcome citadel is a bundled SVG, and existing saves are unchanged.
