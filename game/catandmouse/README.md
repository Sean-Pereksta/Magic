# Cat & Mouse sync and runtime update

The public entrypoint remains `game/catandmouse.html`. It fetches and patches
`catandmouse-core.html`, which imports `catandmouse/sync.mjs`. Deploy these files
together. Two outdated loader patterns previously made the whole performance
pass fall back; both now match the current core and are exercised by the tests.

## Sync behavior

- State assignments coalesce at a 250 ms minimum interval; player movement at
  120 ms. The interval starts when a write starts, so network acknowledgement
  time does not add an extra interval. There is at most one in-flight SDK write
  and one merged pending patch per writer.
- Only changed fields are sent. Dedupe follows successful writes or confirmed
  server snapshots. Transient errors retain failed fields, with newer values
  taking precedence, and retry up to five times with bounded backoff.
- Player movement updates position, facing, sequence and presence only. It
  cannot overwrite host-owned death/revive state, cheese or identity.
- Cat fields use dotted updates. Each objective is replaced independently;
  updating one objective cannot erase the others.
- Healthy host heartbeats come from the existing collection listener. Only
  the elected replacement attempts a transaction once the host is stale.
  Routine heartbeats no longer rewrite the host identity.
- Cached snapshots pause host simulation. Host changes cancel unsent state
  patches. Reattaching the listener removes entities absent from the first
  server snapshot. The HUD reports connection state and allows retrying a
  terminated listener. Back/forward cache restoration reattaches listeners.

## Rendering

Unchanged structures, terrain styles, desktop/mobile build controls and player
status labels retain their DOM nodes. Structure damage, generator activation,
shields, removal and asset fallback invalidate the relevant cached structure.
The existing isometric depth values remain shared across layers. Rendering is
coalesced through animation frames and suspended while the document is hidden;
simulation is not intentionally paused merely because a connected host hides
its tab. Browser timer throttling still applies.

## Validation

Run `npm run test:catandmouse` (Node 20+; no dependencies). Tests cover queues,
retry ordering, death/revive reconciliation, cached snapshots, host election,
listener restart, render-node reuse, and the real public loader. Both the
optimized and fallback module scripts are syntax checked without producing an
HTML preview. `window.__catMouseSyncStats()` exposes local write/skip/retry
counters and connection state for diagnostics.

A live two-player Firebase session and desktop/mobile visual playtest remain
required before release. These tests use deterministic transport and DOM
fixtures; they do not measure real network latency or hardware frame rate.
Existing SDK writes already in flight cannot be cancelled by this client-only
change, and existing multi-document combat/economy paths are not made globally
transactional. No Firebase rules or backend services are changed.
