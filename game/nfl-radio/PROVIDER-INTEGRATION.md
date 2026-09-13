# Authorized NFL audio: integration decision

Checked September 13, 2026.

## What works with the public interfaces

| Option | Coverage | Catnmice implementation | What remains with the provider |
| --- | --- | --- | --- |
| SiriusXM | Home/away calls for all 32 teams; national/Spanish where scheduled | 32 official team deep links, plus exact-match scheduled national and Spanish links | Login, subscription eligibility, audio, play/pause and background playback |
| NFL+ | Every game's home, away and national audio in the US | Official login/live-audio destination | Login, subscription, game selection and playback |
| iHeart web widget | Participating local stations; game availability depends on market and schedule | Provider-issued oEmbed players for 97.1 The Ticket and SportsRadio 94WIP | Player controls, ads, permission prompts, geolocation and game restrictions |
| TuneIn | Local team streams in their home territory; selected national games free in the US | Researched; not activated because the station embed endpoints could not be verified from this environment | Rights/availability and playback; partnership needed to establish supported integrated control |
| Direct approved feeds | Only explicitly authorized games and time windows | Existing instant-play manifest adapter remains supported | Provider must issue and enforce permitted URLs |

**Recommendation:** SiriusXM's official team links are the most precise immediately
available paid option for nationwide team selection. NFL+ is another verified source
of full game-audio coverage, but no public game-specific audio handoff was documented.
For listening inside Catnmice today, use the provider-issued iHeart widgets where
available. These are authorized station players, not a guarantee that the selected
NFL game is available to the listener.

None of the reviewed official public sources supplies consumer OAuth credentials
or a general third-party NFL playback SDK that we can simply turn on with a personal
subscription. Do not purchase a subscription expecting it to enable an in-app
Catnmice integration. A subscription enables listening on the provider's supported
player/app. The SiriusXM navigation links do not expose subscriber audio URLs.

## Shipped behavior

“Listening options” saves Best available / SiriusXM / NFL+ as a listening preference.
It never says an account is connected or authenticated: Catnmice cannot inspect the
provider session. Login links go to the actual provider origin. No password fields,
token imports, private APIs, forged client identities, proxies or playback extraction
are implemented.

Best available uses approved direct feeds first, a matching official iHeart widget
second, and a clearly labeled provider handoff otherwise. Selecting SiriusXM or NFL+
launches that official service when tapping a game. Voice still identifies and speaks
the team; browsers may block the subsequent external window, in which case a visible
provider link is shown. The external service may require its own Play tap. Selecting
or leaving a widget removes the previous iframe so it cannot continue playing under
the next widget. External app audio cannot be stopped or inspected by Catnmice.

The app does **not** claim “playing” for an iframe load or provider handoff. It does
not blindly fall through local stations to try to escape a provider blackout.

## Remaining step for the exact original experience

To implement “say any game and its authorized paid audio immediately plays within
Catnmice,” obtain a provider agreement that explicitly supports a third-party web
application, NFL content, user entitlements and programmatic playback. TuneIn's
published Device Partners contact is the most concrete integration inquiry route
found. This is an inquiry recommendation, not a claim that TuneIn will approve the
app or that its agreement will cover all out-of-market NFL games.

Prepared inquiry — NOT SENT:

To: partners@tunein.com
Subject: Catnmice NFL audio integration — supported player/API and rights inquiry

Hello TuneIn Partnerships,

I am developing NFL Radio Dial at https://catnmice.com, a mobile-first web app that
lets listeners choose a rotation of current NFL games and switch broadcasts using
team buttons or hold-to-talk voice commands.

Please advise whether you offer an approved web player SDK or partner API for this
use case, including home, away, national and Spanish game calls where licensed.
We need supported team/event discovery, authenticated playback where required,
play/pause and station switching, with your advertising, territory, blackout and
subscription controls fully preserved.

Could you confirm whether NFL audio rights are available for this type of third-party
web integration, including any out-of-market coverage? Please share the application
process, documentation, pricing/minimum commitments, allowed domains, supported user
login/entitlement flow, and whether your embed supports programmatic start/switch.
We will not extract consumer sessions or bypass any access restrictions.

Thank you.

## Sources

- SiriusXM coverage and provider-issued links: https://www.siriusxm.com/sports/nfl
- Example stable team destination: https://www.siriusxm.com/sports/nfl/detroit-lions
- NFL+ coverage: https://support.nfl.com/hc/en-us/articles/35869715432468-What-games-can-I-listen-to-with-live-audio-on-NFL
- iHeart widget instructions: https://help.iheart.com/hc/en-us/articles/115000879272-How-do-I-add-iHeartRadio-to-my-website
- iHeart game restrictions: https://help.iheart.com/hc/en-us/articles/9474717276429-Listening-to-Live-Sports-on-iHeartRadio
- Detroit widget metadata: https://www.iheart.com/oembed/?url=https%3A%2F%2Fwww.iheart.com%2Flive%2F971-the-ticket-10827%2F&format=json
- Philadelphia widget metadata: https://www.iheart.com/oembed/?url=https%3A%2F%2Fwww.iheart.com%2Flive%2Fsportsradio-94wip-10934%2F&format=json
- TuneIn embed instructions: https://cms.tunein.com/listen/embedded/
- TuneIn NFL territorial coverage: https://tunein.com/radio/NFL-c1736013/
- TuneIn partnership contact: https://cms.tunein.com/contact/
- Westwood One national coverage: https://www.westwoodone.com/programs/sports/nfl/the-nfl-on-westwood-one/

## Data maintenance and validation

`provider-data.mjs` contains public SiriusXM team/channel navigation links and dated
schedule entries, extracted from the provider's public NFL schedule page. It does not
contain audio manifests, credentials, cookies, or subscriber data. National/Spanish
links require the same teams and kickoff time as the current ESPN game; unmatched or
changed fixtures do not guess a broadcast. Team deep links are provider-owned channels,
not promises that a game is currently on air. Re-check the provider schedule when
updating this catalog, particularly after schedule/channel changes.

Both iHeart oEmbed endpoints returned station-specific embed code, and Detroit's
widget endpoint returned HTTP 200 without frame-denying response headers. The widget
metadata currently sets autoplay false, so no unsupported autoplay query or hidden
control bypass was added. End-to-end audibility and subscription playback have not
been tested on the user's account or device.
