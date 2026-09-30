# Living realm commands

## Generals

Candidates appear only in owned cities. Their saved, seeded schedule offers at
most one candidate per House, starting in rounds 3–8 with 14–23 rounds between
opportunities. Offers last through their displayed deadline (four rounds after
appearance). The campaign has sixteen named general identities shared across all Houses. Opening a city, refreshing
or reconnecting cannot reroll a candidate. Quality probabilities are 65%, 23%,
8%, 3% and 1%, respectively; AI pays the same prices and upkeep.

| Quality | Hire gold | Gold per round | Command effectiveness |
| --- | ---: | ---: | ---: |
| Capable | 75 | 1 | +5% |
| Veteran | 125 | 1 | +10% |
| Distinguished | 200 | 2 | +15% |
| Renowned | 300 | 3 | +20% |
| Legendary | 450 | 4 | +25% |

Command effectiveness scales phase attack power once in the shared combat
calculation. It does not add another multiplier to armor, morale recovery or
casualty conversion. Strength estimates, battle previews and resolution use the
same normalized bonus. Bonuses are regenerated from the roster when loading;
imported army bonus fields cannot create stronger officers.

A movement specialist adds one movement point, or two at Renowned/Legendary
quality. A mustering specialist adds 2–5 levies at an eligible owned city or 1–2
at a town. This consumes population, proportional normal levy costs and one
build/recruit order, including normal local prerequisites and the civilian
floor. The weakest eligible detachment receives the command's single allowance
at the round boundary, after economy and order refresh. Failure consumes that
round's opportunity. Splits, reassignments and repeated visits cannot farm it.
Hiring a mustering general authorizes this disclosed, bounded recurring expense;
campaign objectives authorize no further autonomous spending or new wars.

Assign from an army card; **Chat** and **Orders** display the actual objective,
status, constraints and history. Discussing a plan creates no game action. Review
the interpreted objective, known locations (or observed friendly army to
reinforce), loss threshold and permission to split, then approve those exact
orders. A declaration of war remains a separate player decision.

The local planner works without Gemini or an open conversation. Aggressive
commanders favor confirmed advantages; cautious commanders require a stronger
assault estimate; methodical commanders use available bombardment against walls;
opportunists can use smaller viable detachments; protective commanders preserve
forces and cover nearby capital threats. Every commander respects the approved
loss limit. Splits need current observations, legal routes, useful separate
objectives and at least 24 or 32 troops per detachment. Quality bounds autonomous
detachments to two or three. Same-command forces can reunite; unrelated human
armies are never silently absorbed.

The primary army retains its commander after a manual split; new formations begin under manual control. Approved autonomous detachments retain command identity. Merging commanded forces asks which general will lead. Movement expenditure and
already-resolved flags stay attached to troops. Manual movement, hold, formation
and split/merge choices are protected for the current activation. Unassigning,
detaching or confirmed dismissal preserves every soldier and spent movement;
unassigned officers still cost upkeep, while dismissal stops future upkeep.

Gemini shares the existing client, verification/session, Worker, cache, timeouts
and request budget with ruler chat. The general dialog can establish that session
without opening a ruler conversation. It spends no diplomatic envoy. Only an
explicit message calls the model; malformed/unavailable responses use contextual
local dialogue. Engine validation and separate approval control every action.
An expired activation's reply cannot submit commands. General histories are
private and bounded to 40 entries; model context uses eight short recent entries,
owned forces and limited current/dated observations.

See [Army organization](ARMY_ORGANIZATION.md) for the Sort Army editor, commander transfers, portraits and save migration.

## Vassals

**Your Vassals** appears in Realm, diplomacy, the ruler conversation and military
planning. House colors remain distinct, with a linked-crown marker. Commands
cover attack, defense, rally, reinforcement, siege, withdrawal and frontier
holding. They link to the existing persistent strategic plans and take priority
over discretionary campaigns. Practical obstacles and capital emergencies are
reported without abandoning a still-relevant objective or promising movement
that did not occur.

AI vassals issue and execute orders only during their own activation. A human
vassal explicitly accepts the obligation and retains manual army control. The
liege's command itself never moves subordinate troops. Command status is one of
Preparing, Marching, Engaged, Holding, Blocked or Completed; messages are emitted
only when status/reason changes. Defense, frontier and moving-army reinforcement
commands remain active after arrival, holding their position until replaced or
released. Completion of a finite objective releases the linked plan slot.

Fealty derives from recorded relationship history. Rebellion needs sustained
serious abuse on distinct rounds, or a broken protection obligation plus a grave
incident; unresolved high grievance/low trust; a suitable ruler personality; and
a credible advantage over actually observed liege forces. It first produces a
three-round warning. Repairing the relationship removes the warning. AI war
against the liege is otherwise prohibited. There are no loyalty decay or routine
betrayal rolls, on any difficulty. Peaceful expiry/release is recorded as Released,
not Rebel. Fealty history and command details are visible only to participants.

## Persistence and authority

Difficulty defaults to Medium in existing saves. New commander, command,
movement, Fealty and sequential fields have bounds and import validation. Missing
commanders return their armies to manual control. Existing world progress is
retained. Online schema-1 migration preserves old army destinations for explicit
review and rejects old simultaneous requests. See [MULTIPLAYER.md](MULTIPLAYER.md)
for the active-House/activation-ID gate and trusted-controller limitations.

## Verification

- `living-command.test.mjs`: deterministic candidates, duplicate hires, actual
  upkeep, paid city/town mustering, viable splits, reinforcement tracking,
  protected manual orders, dismissal, privacy, save validation, vassal plans,
  rebellion warnings, sequential accounting and queued joint-war consent.
- `firebase-rules.mjs`: real Firestore rules deny waiting Houses, stale
  activations, forged ownership and expired controllers.
- `multiplayer-browser.mjs`: real Firebase SDK and local emulators, independent
  desktop/mobile clients, a full six-human round, consent windows, reconnects
  and actual controller closure/failover.
- `command-browser.mjs`: desktop/mobile candidate and vassal UI, own-general
  verification, model proposals versus approval, local fallback, overrides and
  dismissal. Model responses are mocked; no quota is consumed.
- `difficulty-benchmark.mjs`: identical seeds and all-AI campaigns across four
  difficulties; founding, captures, completion, solvency and stalled objectives.

All browser checks inspect behavior and DOM layout without generating previews
or screenshots. Live Firebase rules and the Gemini Worker must be deployed with
these modules by the site's operator; the tests use only the demo emulators.

## Recorded difficulty comparison

`npm run test:iron-throne:difficulty` runs 30 full rounds on the same seeded
initial world for each difficulty: Heartlands (8147), Great Divide (991), and
Highland Crown (3401), with all six Houses controlled by AI. No model calls or
difficulty bonuses are involved. These are observed results, not brittle
pass/fail thresholds. [Raw results and metric definitions](tests/fixtures/difficulty-benchmark.json)
are checked in for reproduction. Totals across the three campaigns:

| Difficulty | Founded | Captures | Completed objectives | Shortage House-rounds | Stalled plans | Final gold |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Easy | 32 | 1 | 3 | 0 | 4 | 4073 |
| Medium | 28 | 10 | 15 | 0 | 7 | 2963 |
| Hard | 58 | 24 | 8 | 0 | 9 | 17265 |
| Insane | 67 | 14 | 10 | 0 | 11 | 16201 |

Captures include settlements and military positions. Completed objectives count
recorded invasion, joint-war and military infrastructure plans. Shortages mean
zero food or gold at a living House's round ending, and stalled plans are still
active after at least 12 rounds. Founding counts distinct new settlement sites.

Hard and Insane establish substantially larger economies and take more positions
over this sample. Formal campaign completions and stalled-plan counts do not
improve monotonically: the same stronger policies also control their opponents,
and this 30-round sample does not establish long-run conquest superiority or
head-to-head win rates. The raw metrics retain those limitations rather than
using war declarations as a proxy for success.
