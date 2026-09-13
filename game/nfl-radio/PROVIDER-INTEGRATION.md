# NFL audio provider integration decision

Updated September 13, 2026.

## Corrected definition of “in app”

Catnmice now calls a source **in-app playable** only when the browser can attach a direct audio URL to its own audio element and attempt playback without navigation, a provider login, or another Play control.

An iHeartRadio, Audacy or TuneIn webpage/iframe is **not** treated as successful in-app playback. A provider widget can be displayed inside a page while still owning its own controls, ads, permission prompts, account state, location checks and autoplay behavior. That does not meet the NFL Radio Dial’s one-press playback requirement.

## Automatic playback order

1. Provider-approved game-specific direct feeds from `feeds.json`.
2. Public no-login HTTPS station streams discovered through Radio Browser for mapped NFL flagship/alternate station names and call signs.
3. If and only if every in-app candidate fails, reveal a separate glowing **External options** button.

The Play button itself never opens iHeartRadio, Audacy, TuneIn, SiriusXM, NFL+ or another website.

## iHeartRadio

iHeartRadio publicly promotes free pro-football play-by-play on participating station homes in its own service. That is useful evidence for which station names are worth searching, but it is **not** evidence that Catnmice can programmatically start iHeart’s player inside this app with one press.

Therefore:

- iHeart station names/call signs may be used as discovery aliases for independently public direct station streams.
- Catnmice does not scrape, extract or replay protected iHeart session URLs.
- iHeart pages/widgets are external-provider fallbacks only and stay hidden until the in-app queue is exhausted.

## Other providers

SiriusXM and NFL+ remain authenticated/subscription destinations and are external only. Audacy/TuneIn/provider pages are also external unless a station exposes a separate public direct HTTPS stream that can be played by the browser audio element.

## Rights and blackouts

A working station stream proves only that the station stream is reachable. It does not prove the selected NFL game is licensed on that internet stream. The broadcaster may replace game audio, geoblock it, limit it to a home market, or require another entitlement. Catnmice does not bypass those restrictions.

## What would enable guaranteed nationwide in-app game audio

A provider agreement/API that explicitly grants third-party web playback of NFL game audio, exposes supported event/team feeds, and preserves provider entitlement/territory controls would still be the cleanest path to guaranteed nationwide coverage. Until then, the app uses verified approved feeds first and free public direct station streams second, while labeling the difference clearly.
