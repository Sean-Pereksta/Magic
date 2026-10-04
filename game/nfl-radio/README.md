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

## Personalized live companion

The current-game card and transcript follow a monitored game; use the game selector to inspect another game in your rotation. Switching radio games also switches that view. Radio routing, public stream discovery, in-app fallback order, media controls, and station preferences remain in `app-v2.mjs` / `RadioPlayer`.

- **Every Play**, **Touchdowns**, and **Selected Players** control new play calls. Tap a mode to enable speech, or use **Voice on** to pause announcements while the transcript keeps updating. The first snapshot loads history silently.
- **Selected Players** opens the searchable rosters for both teams. Selections are stored per game. Athlete IDs cover all participant roles; unique full/abbreviated names cover play records with no participant IDs. Ambiguous initials are never guessed.
- **Voice & alerts** contains the device voice selector, Normal/Excited style, player picker, radio ducking, current-radio-game inclusion, and periodic score settings. Choosing automatic ranks available English voices, preferring voices marked Natural/Neural/Premium/Enhanced. Missing or late-loading voices fall back safely.
- Automatic score summaries remain separate from play filters. Set their interval to **0** for play calls only. Detailed game updates use the same formatter and voice queue; when live play calls are enabled, automatic detailed recaps omit duplicate play details. Manual replay remains available.
- `play-formatter.mjs` rewrites supported passes, runs, sacks, and kicks from explicit ESPN facts. Penalties, reviews, return/fumble sequences, missed/blocked kicks, and unfamiliar forms keep a cleaned version of their original detail. It never infers the previous play's team from the next possession.
- `play-tracker.mjs` combines ESPN drive history with the scoreboard last play, sorts plays, and deduplicates by ESPN play ID across polls and corrected/replayed snapshots. Corrections update the transcript without another announcement. Each game retains 60 visible plays; seen IDs last for the monitored session so scrolling history out cannot cause replay.
- Scoreboard polling remains every five seconds. Drive history is fetched when the latest play changes or at least every 30 seconds, with request overlap prevented. Rosters are cached for an hour with bounded retry delays. Unchanged plays reuse their formatting. An unavailable summary falls back to the latest scoreboard play and explicitly shows reduced coverage; a connection interruption preserves the transcript and retries.
- Voice commands now settle cancellation through the shared speech queue. Microphone capture suspends announcements, and ending/canceling speech restores the radio's previous volume. Selecting a new station while ducked continues using the existing audio bridge.

### Companion validation

```bash
node --test game/nfl-radio/*.test.mjs
node game/nfl-radio/browser-smoke.mjs
```

The browser smoke script requires Playwright and an installed Chromium (`CHROMIUM_PATH` can point to a browser executable). It uses controlled ESPN, roster, station, speech, and audio fixtures; it checks mobile/desktop overflow, saved selections, speech/transcript consistency, mode changes, radio controls, ducking/restoration, and network recovery. Real voice quality, pronunciation, mobile background scheduling, and current station carriage still require listening on the target device.
