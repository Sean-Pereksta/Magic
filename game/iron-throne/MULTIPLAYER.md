# Iron Thrones Online

The existing route, map, economy, movement, combat, diplomacy, espionage and
strategy engine serve both modes. A normal launch retains schema-3 local saves.
A `gameId` (or `lobbyId`) selects the Firebase campaign; online actions never
write or load the single-player localStorage save.

## Playing

Create Iron Thrones from the shared lobby, invite up to five other people, and
launch House selection. Each anonymous Firebase UID can claim one of six Houses.
The host sets the seed, map, optional 2/5/10-minute timer, and absent-ruler policy.
Unclaimed seats become AI when the host starts. A private one-human campaign is
also supported. The shared lobby lists this browser's saved online campaigns.

New campaigns begin with **Found your kingdom** before Turn 1. The default Random
map chooses one of six profiles from a fresh campaign seed. Each human confirms a
capital at least eight hexes from existing capitals; AI Houses then found in the
remaining regions. Turn timers start only after all six capitals exist. See
[FOUNDING.md](FOUNDING.md) for geography, starting packages and save compatibility.

Issue orders, then press **Ready**. The round advances when all eligible human
rulers are ready, or when its timer expires. A human who is connected retains
their orders even if they miss the timer. Disconnected seats stay reserved; their
readiness is waived after 90 seconds. The host may then request temporary or
permanent AI takeover. Temporary control changes happen at round boundaries;
the same UID regains its reserved House at the next boundary after reconnecting.
The campaign pauses when no living human ruler is connected.

Human chat is private and cannot execute treaties. A structured proposal must be
accepted by its recipient, with costs and legality checked at that moment.
Declarations of war are unilateral. AI conversations retain the existing rule
evaluator, optional Gemini voice, confirmed promises, military pledges and
scripted fallback. AI strategy runs for every AI House and temporary substitute,
never for an actively controlled human House. Each ruler has separate relations,
memories, dispatch allowances, intelligence and negotiations.

All Houses can win by territorial dominance or a Crown Accord. An Accord names
the leading House and its allied human/AI coalition. A fallen human House can
continue observing while other rulers finish the campaign.

## Authority and persistence

`lobby/firebase-config.mjs` supplies the same public Firebase web configuration
as the shared lobby. Anonymous authentication provides ownership; a display name
does not grant access. Keeping browser site data preserves the UID on refresh.
Clearing it or changing browser profiles does not recover that anonymous identity.

Every lobby uses these isolated paths:

| Path below `lobbies/{id}` | Purpose and read access |
| --- | --- |
| `iron_throne/meta` | Seats, options, phase, ready flags, lease, version and epoch; members |
| `iron_throne/world` | Public projection; members |
| `iron_throne/state` | Compressed canonical simulation; current controller |
| `iron_throne_private/{houseId}` | Court, reports, owned/captured spies and proposals; owner and controller |
| `iron_throne_commands/{id}` | Immutable request and final receipt; issuer and controller |
| `iron_throne_presence/{uid}` | Server-timestamped heartbeat; members |
| `iron_throne_snapshots/{0,1,2}` | Three rotating completed-round recovery snapshots; controller |

A transaction claims seats and starts the world exactly once. One browser holds
a 30-second simulation lease renewed every 10 seconds. Its random per-tab token
prevents two tabs of one UID from both resolving. Lease acquisition increments
the epoch; commands from older epochs require review and resubmission.
Administrative hosting can transfer after the original host's 90-second absence.

The controller loads the latest canonical/private snapshots in a transaction,
validates each command's UID, House, turn, sequence, version, epoch and arguments,
and calls the shared engine. State, private projections, version and receipt
commit atomically. Costs, resource totals, RNG and battle results come from the
engine. A stale or duplicate receipt cannot apply again. Invalid arguments are
rejected without saving partial mutations or stopping the queue.

Round resolution first commits a durable `resolving` phase. A successor can
resume that exact turn; the completed turn and its snapshot commit atomically.
Observers render only matching public/private versions. Network failure pauses
orders and shows **RECONNECTING…**; there is no local simulation fork.

This is a trusted-client controller architecture. Firestore rules protect seats,
commands and normal private access, but the controller necessarily reads the full
simulation. It is not intended to resist a malicious controller editing its own
client. Other players cannot casually read private records through the ordinary
member permissions.

Payloads use gzip when supported and remain below a conservative 750 KB document
guard. Expanded snapshots are bounded at 8 MB. Conversation/report histories and
proposals are bounded; private histories stay outside the public world. Command
receipts remain durable for replay protection and accumulate over a campaign.
An operator may archive/delete a finished lobby's entire namespace, including
its receipts, once that campaign will no longer be resumed.

## Deployment and recovery

1. Deploy the updated `firestore.rules` to the existing `bible-game-246c0`
   Firebase project before exposing Online Multiplayer. Old recursive lobby
   rules do not provide Iron Thrones' ownership and privacy guarantees.
2. Publish the static game modules and shared lobby changes together.
3. Deploy the existing Iron Thrones Gemini Worker update if live chat is enabled;
   it now accepts an explicit speaking House. Scripted diplomacy needs no Worker.

No new Firebase project, credentials, server or paid Gemini quota is required by
these changes. The emulator configuration is strictly for `demo-iron-thrones`;
the automated tests never connect to the production project.

For operator recovery from malformed data, first stop clients, export the current
lobby documents, and choose a valid rotating snapshot. Decode its `payload` with
`decodePayload`, use `packCampaign` to rebuild public/private documents, and
restore them together with metadata at a **new**, larger state version and epoch.
Set phase/turn to the restored planning boundary, clear ready flags, sequences
and the lease, and reject outstanding commands from the old epoch. Perform this
as an administrative batch; normal clients intentionally cannot roll versions
backward. There is no in-game destructive restore button.

## Population growth

Growth and capacity are separate and calculated identically for humans and AI.
The header and Realm panel use the same calculation as turn resolution. For
example, a city and two farm levels support a capacity of 166; a healthy realm
can show **Population: 80/166 · +6 next turn**.

| Contribution | Growth per turn | Capacity |
| --- | ---: | ---: |
| Each city | +3 | 150 |
| Each completed city upgrade (up to three per city) | +1 | +50 |
| Each town | +2 | 80 |
| Each farm level | No direct growth bonus | +8 |
| Net food income at least 5 / 15 / 35 | +1 / +2 / +3 | — |
| Happiness after taxes at least 65% / 85% | +1 / +2 | — |
| Low taxes | +2 | — |
| High taxes | −1 | — |
| Happiness from 35% through 49% | −1 | — |

Positive growth stops at capacity and is capped at 24 per turn. It needs a
surviving settlement, more than 20 food after upkeep, and at least 35% happiness.
The Realm breakdown identifies every bonus, penalty and blocker, including the
growth limit and remaining capacity. Forecasts include construction completing
next turn; new orders or battles can change them.

Cities also gain +4 food, +4 gold and +1 kingdom build/recruit order with each
upgrade. Orders remain capped at eight per kingdom, and mustering still pays
the normal troop and population costs. See [EXPANSION.md](./EXPANSION.md#three-city-upgrades)
for the four city levels.

Recruitment costs are unchanged (a levy still consumes eight population). The
existing food/gold-shortage population loss, civilian floor, happiness loss and
desertion remain. Over-cap populations in existing saves are retained and stop
growing until capacity is available. No save-schema migration is required.

## Tests

Engine and shared-lobby regression tests need only Node:

```sh
npm run test:iron-throne
node --experimental-vm-modules --test lobby/tests/lobby-library.test.cjs
```

The optional browser/emulator suite uses Node 22 and Java 17:

```sh
npm ci --prefix game/iron-throne/tests
cd game/iron-throne/tests
npx playwright install chromium
cd ../../..
npm run test:iron-throne:online
npm run test:iron-throne:browser
npm run test:iron-throne:founding-browser
npm run test:iron-throne:intelligence-browser
```

The online suite uses the real Firebase SDK with Auth/Firestore emulators and
independent browser contexts. It covers concurrent seat and capital claims, founding reconnects, private rules,
ownership, human diplomacy, recruitment, synchronized two- and six-human rounds,
offline UI, same-UID refresh, and actual controller-browser closure/failover.
Pure engine tests additionally cover timers, temporary/permanent takeover,
duplicate/stale orders, useful AI pledges, coalition victory, save compatibility,
private model context, compression and population forecasts. Tests require no
Gemini quota and generate no previews or screenshots.
