# NFL Radio Dial

Entry: `/game/nfl-radio.html`. Registered in `lobby/lobby-core.html`, loaded by
`lobby/lobby.html`, under Apps and Tools & Learning. No build or backend required.

## Implemented

- Current NFL scoreboard/week from ESPN's public scoreboard endpoint, refresh every
  minute while visible, explicit failures, and up to 24 hours of labeled offline cache.
- All 32 teams, city/nickname/abbreviation/opponent voice matching and shared-city
  clarification; saved rotation, large game buttons, home/away/national/Spanish
  switching, station selection and successful-playback team preferences.
- Persistent single-stream player, immediate `play()` inside tap handlers,
  cancellation of stale requests, bounded fallback and stall timeout, Media Session
  controls, and explicit autoplay-policy recovery.
- Hold/release speech recognition with spoken team confirmation. Browser support
  varies; HTTPS and microphone permission are required. Typed commands are always
  available. Browser speech recognition may send audio to its speech service.
- Responsive mobile layout and scoped microphone/autoplay delegation from the
  Catnmice outer iframe. No HTML previews were generated.

## Provider integration update

The app now offers official SiriusXM team listening links for all 32 teams, NFL+
login/live-audio links, and provider-issued iHeart station widgets for Detroit and
Philadelphia. See [PROVIDER-INTEGRATION.md](./PROVIDER-INTEGRATION.md) for the source
verification, supported behavior, comparison and prepared partnership inquiry.

**The exact nationwide instant in-app experience still needs a provider agreement.**
SiriusXM/NFL+ login and playback remain on their own service. iHeart widgets may
require a Play tap and local-market eligibility; they do not guarantee the station
is carrying this game. No direct approved feed has been issued, so `feeds.json` is
still empty. The UI labels these distinctions and never treats a loaded iframe or
opened login page as confirmed audio playback.

## Connecting approved feeds

Publish provider-approved HTTPS audio URLs in `feeds.json` only after establishing
embedding permission and game coverage. No secrets or subscriber credentials belong
in this public file. Each feed must contain:

```json
{
  "id": "provider-station-id",
  "name": "Station display name",
  "team": "DET",
  "kind": "flagship",
  "language": "en",
  "access": "direct",
  "authorized": true,
  "gameAudio": true,
  "gameIds": ["ESPN_GAME_ID"],
  "streamUrl": "https://PROVIDER_APPROVED_AUDIO_URL",
  "sourceUrl": "https://PROVIDER_AUTHORIZATION_AND_COVERAGE_SOURCE",
  "verifiedAt": "ISO_TIMESTAMP",
  "availableFrom": "ISO_TIMESTAMP",
  "availableUntil": "ISO_TIMESTAMP"
}
```

`kind` supports `flagship`, `affiliate`, `national`; national feeds have no `team`.
The time window must be current and gameIds must explicitly include the selected
game. The provider must enforce any geographic/entitlement restrictions at its
endpoint. Do not mark a provider-page URL or general sports-talk stream as direct
game audio. Use native-browser supported HTTPS audio; HLS only works where supported
natively (no HLS.js dependency is bundled). Playback latency depends on buffering;
a browser can require a recovery tap for voice-triggered playback.

A production provider should provide a secure server integration if it requires
entitlements or ephemeral URLs. That is not replaced by flags in a public manifest.

## Validation

Run `node --test game/nfl-radio/core.test.mjs game/nfl-radio/providers.test.mjs`.
Tests use fake audio for dispatch, cancellation, failure, and autoplay behavior;
they do not establish live broadcast availability. The ESPN endpoint returned HTTP
200 with an accessible JSON schedule and an `Access-Control-Allow-Origin: *` header
in this environment. It is an external dependency with no SLA for this app.

A Playwright DOM smoke check was attempted but could not run because this runtime
has no installed Chromium executable. Real-device microphone, audio, background
playback, and mobile layout verification remain release checks.

Provider follow-up validation: 15 core/provider tests pass. A simulated DOM integration
check passed for schedule rendering, command routing, widget replacement/stop,
provider handoffs, popup-blocked recovery, station listing and persistent broadcast
controls. This is not an end-to-end audio test. Installing Chromium was attempted
but the browser download timed out; real-device testing still remains.
