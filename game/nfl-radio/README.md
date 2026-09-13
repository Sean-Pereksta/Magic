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

## Release blocker: game audio provider

**This is not yet a working nationwide game-audio service.** `feeds.json` intentionally
contains no direct streams: no embeddable NFL game feeds were verified during this
implementation. Selecting a game currently shows an unavailable state. Official
provider destinations are available in Stations, but opening those destinations is
not the requested instant in-app playback experience. This remains a draft until
an authorized provider integration is supplied and tested.

The station catalog is a starter catalog, not complete national affiliate coverage.
It identifies verified provider destinations for Detroit, Green Bay (including two
major affiliates), Kansas City and Philadelphia (including Spanish), plus NFL+ and
Westwood One. All 32 teams have official-site discovery links. The remaining flagship
and affiliate inventory still needs source verification; affiliation alone does not
prove permission to embed live game audio. Buffalo's recent network changes are a
reason not to seed remembered station names as current facts.

Sources checked September 13, 2026:
- https://www.packers.com/video/radio-network
- https://www.chiefs.com/listen/96-5-the-fan-the-kansas-city-chiefs-radio-network-stream
- https://www.audacy.com/stations/971theticket
- https://www.audacy.com/94wip/how-to-listen-to-philadelphia-eagles-games-on-the-radio
- https://www.philadelphiaeagles.com/liveradio/
- https://support.nfl.com/hc/en-us/articles/35869715432468-What-games-can-I-listen-to-with-live-audio-on-NFL
- https://tunein.com/radio/NFL-c1736013/
- https://www.westwoodonesports.com/

NFL+ offers subscription home, away and national audio in its own ecosystem; there
is no NFL+ session extraction in this application. TuneIn documents local-market
limits for free team broadcasts. No proxy, login bypass, geo bypass, or scraped
subscriber stream URLs are included.

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

Run `node --test game/nfl-radio/core.test.mjs`.
Tests use fake audio for dispatch, cancellation, failure, and autoplay behavior;
they do not establish live broadcast availability. The ESPN endpoint returned HTTP
200 with an accessible JSON schedule and an `Access-Control-Allow-Origin: *` header
in this environment. It is an external dependency with no SLA for this app.

A Playwright DOM smoke check was attempted but could not run because this runtime
has no installed Chromium executable. Real-device microphone, audio, background
playback, and mobile layout verification remain release checks.
