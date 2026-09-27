# Runtime contract

## Authority

`data.mjs` defines content, units and legal intent names. `core.mjs` owns the seeded
world, construction, paths, combat, economy and strategic orders. `diplomacy.mjs`
owns intent validation, deal scoring, atomic transfers, treaties, pledge checks
and the end-turn orchestrator. `app.mjs` holds the current campaign. The map only
renders and reports selected tiles. The Worker never takes a system prompt,
provider URL, model name or schema from a browser request.

Ruler dialogue is advisory. A response creates *proposals*. The same
`evaluateDeal` runs for model proposals and treaty-desk offers. `commitDeal`
re-evaluates current resources, relationships, target ownership and treaty state
before mutating anything. Only an explicit Ratify button calls it. Counteroffers
must be reviewed and ratified separately. Never call `commitDeal` automatically
from chat parsing. Unrecognized intent fields, unknown types and nonfinite,
negative or oversized amounts are rejected.

Resource direction is always from the human's perspective: `giveAmount` is paid
by House Ashen to the ruler; `receiveAmount` is paid by the ruler to House Ashen.
`PROMISE` reserves no resources and grants no immediate trust; delivery from the
ledger makes the real transfer. Other payments are upfront. A failed military
pledge costs trust and reputation; upfront compensation is not automatically
refunded. Gifts improve relations once per ruler per turn. Exchanges must balance
resource value regardless of friendship.

## Turns

1. Player diplomacy and orders may be prepared freely between turns.
2. Expire treaties, resolve local rival diplomacy, select rival strategy/orders.
3. Move stacks along validated adjacent paths, apply border access, terrain/road
   costs, zones of control, battles, siege damage and captures. Slow stacks can
   spend a full turn traversing one costly edge.
4. Complete construction, recalculate territory, produce income, pay upkeep,
   resolve shortages/population/taxes and reset construction orders.
5. Increment the turn, verify pledges, expire treaties and check victory/defeat.

Six fixed starting locations have guaranteed passable corridors. Resource rings
make every house viable. Seeded RNG is serialized, including combat randomness;
continuing a saved campaign produces the same simulation as uninterrupted play.
There are no simulation timers or provider requests during a turn.

Cities/towns project three hexes of territory; forts project two. Founding a town
on an adjacent neutral hex establishes a protected construction claim. Complete
towns/forts persist as ownership anchors. Road-only connected settlement pairs
produce trade income; multiple paths never count the pair twice. Alliances grant
military access. Trade alone grants only road-trade access. Expired military
access permits withdrawal, not renewed entry.

## Pledges and victory

Military strategy prioritizes pending promises. DEFEND requires arrival at a
still-friendly settlement and two resolutions on station. POSITION requires
arrival. WITHDRAW requires all debtor troops out of creditor territory.
BUILD_DEFENSES requires a completed owned fort. JOINT_WAR requires an actual
battle, siege or capture against the named house after the agreement. PROMISE
requires the actual paid delivery. Successful pledges increase the creditor's
trust/opinion; deadlines and betrayal can break pledges and affect other rulers.

Conquest checks player settlements / all settlements ≥ 60%, rounded up. The
Crown Accord counts active alliance/allegiance agreements with more than half
the surviving rivals for three consecutive resolutions. No remaining player
settlement means defeat. After an outcome, turns and mutating orders stop.

## Persistence and network

Campaign version 1 uses `catnmice.iron-throne.v1` in localStorage and JSON exports.
Every player mutation autosaves. Corrupt saves produce a visible warning and are
not overwritten until the player explicitly begins/imports a campaign. Storage
failures are visible and leave export available. Schema validation bounds map,
army, economy, treaty, memory and conversation content. Imported presentation and
personality data is replaced by the trusted house catalog.

Chat snapshots the campaign generation; a delayed reply is discarded if the
world changes before it arrives. Browser text is escaped; ruler replies never
become HTML. Model messages, recent history and state summaries are bounded.
Free-tier limits are operator configuration. The transactionally reserved global
budget counts attempts, including failures. Gemini is optional and never gates
the playable campaign.


## Interest-led trade negotiation

`TRADE` is a road-commerce charter, `EXCHANGE` is an immediate swap, and
`RECURRING` is a supply obligation. Exploring a neighbor's needs is not a request
for a paid audience and creates no charter, gift, loan or oath. A fair swap does
not require a separate charter. Real transport fees remain contract costs.

`trade-negotiation.mjs` evaluates commercial interests, cautious commitment and
specific concerns. Connected roads give a mutual-benefit reason to accept a
no-payment charter. A short charter, useful resource concession, smaller swap,
or shorter supply obligation can be a counteroffer; every advertised alternative
must independently pass `evaluateDeal`. Gold is a concession when it addresses
an actual need, not the automatic default for an empty offer. Severe grievances
and observable frontier threats cannot be paid away to secure a lasting charter.

Authorized player speech can record bounded arguments: mutual interest, relevant
supply, a limited trial, a ledger-verified kept promise/withdrawal, or acknowledgment
of an existing grievance. Arguments last through their turn and the next two
turns, contribute at most 10 commercial willingness points, and only apply while
their factual preconditions remain true. Repeating/paraphrasing an argument or
cycling previously offered shortage resources cannot refresh it. New speech
can earn at most 6 opinion and 3 trust over that directional relationship's
lifetime. Threats revoke active influence without resetting repetition limits.
Offers and apologies are not shipments or fulfilled obligations. These optional
save fields are bounded and validated; older saves need no migration.

Resource swaps retain an independent base/scarcity value floor: relationships
and persuasion may reduce a bargaining premium but can never create arbitrage.
All transfers still require explicit ratification and fresh resource, route,
embargo, strategic-trust and human-consent checks.

`trade-economy.mjs` holds the existing read-only route, infrastructure and needs
functions and is re-exported by `trade.mjs`. This keeps relationship speech from
introducing an evaluator import cycle. The model receives only voluntary coarse
imports/exports and concerns, never private quantities or plans. Multiplayer
publishes the same dated briefing to the relevant human court; projected zero
inventories must never be treated as real shortages. Single-player, scripted
fallback and authoritative multiplayer use the same evaluator and response guard.
Compatible model character voice is retained alongside the exact court ruling;
model output cannot manufacture relationship gains or execute a deal. There is
no additional model call. A scoped, single-use invitation to submit commercial
terms covers a charter, barter or recurring proposal without another envoy fee.

The Worker system prompt changes with this feature. Deploy the updated Worker
through the existing release process when releasing the updated game client;
opening/merging a code PR does not itself assert that the live Worker was deployed.
