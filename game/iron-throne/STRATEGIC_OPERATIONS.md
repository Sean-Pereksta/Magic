# Strategic operations

The War Room connects shared campaigns to existing `jointWar` plans, army orders,
paid recruitment, resource ledgers, promises, and political memory. No model call
creates an operation, accepts an invitation, moves an army, or resolves a pledge.

## Playing

Open **War Room**, choose an objective and launch window, and invite allied Houses.
The leader takes the main assault; partners can flank, recruit siege equipment,
deliver food, defend the leader's territory, or hold a position. Each participant
has its own rally point and troop/equipment requirements. Invitations allow
acceptance, rejection, or a smaller troop counteroffer requiring the leader's
approval. Treaty negotiations similarly support twelve-turn agreements or
six-turn counteroffers.

Accepting binds real commitments. Players continue issuing their own army orders;
AI Houses use the normal movement, recruitment, economy, retreat, and capital
defense systems. Readiness requires troops and siege equipment at the rally,
completed siege recruitment, and delivered food. Food transfers exactly once from
the supplier's treasury. Existing siege engines count toward battlefield strength;
a promise to recruit new equipment still requires new, paid recruitment.

The attack window is the **launch window**. Ready combat participants declare war
together, subject to current treaties and legal routes. Combat commitments allow
eight additional turns beyond the window for travel and fighting. Arrival, actual
combat at the objective, recruitment, deliveries, and two turns on station are
verified from simulation state and events. Issuing an order alone does not fulfill
a promise. Tactical AI still avoids an unsafe assault and can bombard instead.

Broken obligations affect trust, reliability, reputation, negotiations, and can
end an alliance when trust collapses. Withdrawal breaks outstanding obligations.
Peace or an objective captured by an ally releases redundant commitments. If a
partner never completes preparation, prepared allies are released from an attack
that never launched. Secret oaths do not broadcast their terms to unrelated courts.

## Intelligence and politics

Exposure reflects real movement, siege recruitment/equipment, border musters,
participating courts, couriers, and the weakest participant's counterintelligence.
Ordinary frontier observations report visible mobilization without claiming to
know its objective. Strategic spies disclose progressively richer snapshots:

1. Preparing House and general military activity.
2. Confirmed participants and target House.
3. Operation, objective, roles, rally points, launch window, and observed status.

Counterintelligence reduces effective access and exposure, detects foreign
activity and its real mission, and uses existing capture, imprisonment, expulsion,
exchange, ransom, and execution mechanics. Reports remain dated snapshots when a
plan changes. Deliberate misinformation is not implemented.

AI political plans propose terms and wait for another court's decision. Scores
consider trust, reliability, grievances, fear, military balance, shared enemies,
trade dependence, geography, personality, and resource needs. A dominant regional
power can provoke coalition proposals, frontier defenses, support for its enemies,
alignment, or economic neutrality. Actual crossings, passes, road junctions,
production regions, forts, and connecting settlements influence objective scores.

## State and multiplayer

`cooperation` is an additive save field, initialized for older saves. Operations,
proposals, reports, and pledge histories have bounded retention and validated
references. Each accepted role links to a normal `jointWar` plan. Shared plans
are governed by the operation's lifecycle rather than independent launch logic.

Public multiplayer snapshots exclude operations, negotiations, and operation
pledges. Participant documents contain authorized operations/commitments; enemy
knowledge consists only of that player's intelligence reports. Controller
reassembly retains authoritative state through lease changes. This uses the
existing trusted-controller model: the simulation controller can read full state.
Gemini receives shared or discovered operation facts, never an operation command
API. All new commands use the existing authenticated seat, turn, epoch, readiness,
and replay checks.

Deploy the accompanying `firestore.rules` command allowlist with the game when
releasing multiplayer support. This change does not deploy rules or publish the
game automatically.

## Verification

```sh
npm run test:iron-throne
node game/iron-throne/tests/cooperation-browser.mjs
node game/iron-throne/tests/intrigue-browser.mjs
firebase emulators:exec --only firestore,auth --project demo-iron-thrones \
  --config firebase.iron-throne-test.json \
  "node game/iron-throne/tests/firebase-rules.mjs"
```

The browser scripts use Playwright and support `IRON_THRONE_CHROMIUM`. They check
controls and state at desktop, portrait, and landscape sizes without creating
previews or screenshots. The emulator script accepts `IRON_FIREBASE_TEST_MODULES`
when dependencies are installed outside the test directory.
