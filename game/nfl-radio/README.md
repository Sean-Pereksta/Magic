# NFL Radio Dial

Entry: `/game/nfl-radio.html`. The app is mobile-first, framework-free, and uses the existing ESPN schedule integration plus browser audio, voice commands, and spoken game updates.

## Playback contract

`Play` means audio plays inside Catnmice. It must never mean “open a provider page.”

Automatic order:

1. Verified, authorized game-specific feeds from `feeds.json`.
2. Free/no-login public HTTPS station streams discovered from Radio Browser using mapped NFL flagship and alternate station names/call signs.
3. Only after every in-app candidate has failed, show the separate glowing **External options** button.

Provider pages, embeds, logins and subscriptions are not automatic playback. iHeartRadio/Audacy/TuneIn pages are not classified as an in-app success merely because an iframe or page loads. The main Play button never calls `window.open()`.

## Free station discovery

`station-discovery.mjs` contains search aliases for all 32 NFL teams. It searches the public Radio Browser API for each mapped alias and keeps only candidates that:

- were most recently marked online by the directory;
- expose an HTTPS resolved stream URL;
- do not report an SSL error;
- report a browser-oriented audio codec such as MP3/AAC/OGG/Opus.

Results are deduplicated by resolved stream URL and ordered by name match, directory votes, bitrate and codec preference. The browser audio element is still the final authority: if a candidate errors, stalls or times out, `RadioPlayer` advances to the next in-app stream.

A healthy station stream is **not** proof that an NFL game is available on that internet stream. Stations and leagues may substitute programming or apply local-market, blackout, schedule or rights restrictions. The app labels public directory streams as station streams, not verified game audio.

No protected provider URLs, subscriber sessions, credentials, geo bypasses or DRM workarounds are used.

## Single-tap behavior

Adding a game to the rotation starts warming both teams’ free station searches so likely candidates are already cached before the user presses Start or switches broadcasts. If discovery is still needed after a selection, the app searches the remaining in-app candidates and does not surface external options until that search has completed and the audio queue is exhausted.

## External options

External destinations remain available as a last resort, but only through the glowing **External options** button that appears after in-app exhaustion. Opening one is always a separate, explicit user action. This section can include official free provider pages as well as authenticated SiriusXM/NFL+ options.

## Verified game feeds

`feeds.json` remains the higher-confidence path for provider-approved game-specific streams. A feed should only be added when the provider has authorized embedding/direct playback and the game/time window is explicit:

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
  "sourceUrl": "https://PROVIDER_AUTHORIZATION_SOURCE",
  "verifiedAt": "ISO_TIMESTAMP",
  "availableFrom": "ISO_TIMESTAMP",
  "availableUntil": "ISO_TIMESTAMP"
}
```

## Validation

Run:

```bash
node --test game/nfl-radio/core.test.mjs game/nfl-radio/providers.test.mjs game/nfl-radio/station-discovery.test.mjs game/nfl-radio/game-updates.test.mjs
node --check game/nfl-radio/app-v2.mjs
node --check game/nfl-radio/station-discovery.mjs
```

Live stream availability is inherently time-dependent and should still be smoke-tested on the target mobile browser. The regression tests validate resolver ordering/filtering and player behavior, not broadcast rights or whether a specific station is carrying a specific game at test time.
