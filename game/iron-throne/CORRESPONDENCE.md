# Chat-first diplomacy

This is a presentation enhancement of the existing council, not a replacement
negotiation engine. `app.mjs`, diplomacy evaluation, ratification, save formats,
Gemini, fog-of-war and multiplayer transport are unchanged.

## Assets

Upload the following PNGs under `portraits/` in the existing Iron Thrones R2
bucket. The manifest builds their URLs with `IRON_THRONES_ASSET_BASE` and
`ironThronesAsset()`, exposing `ART.portraits[houseId]`.

```
portraits/ashen.png
portraits/wintermere.png
portraits/thornwall.png
portraits/sunspire.png
portraits/vesper.png
portraits/redharbor.png
portraits/stormholt.png
portraits/goldmere.png
portraits/ravenfell.png
portraits/oakwarden.png
portraits/dawnreach.png
portraits/saltwynd.png
```

Portraits are optional, loaded on demand, and excluded from the blocking startup
art queue. A missing or invalid image reveals the existing House symbol. Display
sizes are 64px desktop, 52px compact desktop and 44px mobile, with pixelated
rendering. This change references assets; it does not upload the artwork.

## Presentation contract

`correspondence.mjs` installs `correspondence-ui.mjs`. The adapter enhances the
existing DOM, preserving original nodes, IDs and delegated handlers. If the
council is absent, it does nothing. Styling is scoped to the enhanced council.

The existing renderer supplies the local House in `YOU · HOUSE NAME` and the
foreign House in `#ruler-house`. These exact labels resolve to `CAMPAIGN_HOUSES`
and then to House-ID portrait URLs. There is no default-Ashen assumption or
unfiltered game-state lookup. Unknown identities use a generic symbol.

`#proposals` and `#council-records` move into the right-side Treaty Desk. Desktop
opening reduces the conversation width; on phones the desk overlays the
conversation, traps boundary Tab navigation and makes the background inert.
Escape closes only the drawer, and closing restores focus. Original form state
survives close/reopen. Quick Offer, Request and Promises keep their existing
behavior, with the desk opened after their original handlers run.

Conversation cards copy only visible text from active proposals. Review invokes
the original Modify handler; counter review invokes the original counter handler
before loading those exact terms. The adapter never clicks Ratify or multiplayer
Accept. A card is tied to its original node plus a fingerprint of terms/status and
button identifiers; it cannot silently resolve to a replacement proposal index.
Human offers retain their original receiving-ruler acceptance controls.

The independent conversation viewport avoids the original renderer's automatic
scroll-to-bottom while the reader is looking backward. Consecutive speaker
messages group visually, and system/council messages are centered without faces.

## History boundary

All existing written conversation history remains owned and persisted by the
unchanged game. Structured cards in this enhancement represent **active offers**.
They are not a new permanent archive of expired proposal snapshots: when the
engine clears an offer, its active card disappears, while existing speech and
council records remain. Historical exact-term snapshots would require an explicit
save/network schema extension; this UI change does not invent or reconstruct them.

## Verification

```
node --test game/iron-throne/tests/correspondence.test.mjs
python game/iron-throne/tests/correspondence-browser.py
```

The browser fixture requires Python Playwright and Chromium (`CHROMIUM` can name
an installed executable). It renders the repository council HTML/CSS offline and
models the original DOM/event contract. It covers all twelve player Houses,
missing and successful portraits, desktop/mobile layout, exact offer and counter
review, human acceptance separation, stale cards, keyboard/focus behavior,
scroll preservation, compact mode and court switching. Screenshots and a JSON
check summary are produced for inspection. These are isolated UI tests, not live
Gemini/Firebase, engine, production deployment, or real-image integration tests.

Before merging, smoke-test a saved campaign, an incoming trade offer, a discussed
marriage, a ratification, a non-Ashen online seat, and a live Gemini reply in the
complete game. Existing app/engine tests should also run in the repository.
