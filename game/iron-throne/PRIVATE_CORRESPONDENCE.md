# Ruler knowledge, disclosure, and intelligence purchases

A ruler knowing something is separate from the current player being entitled to
see it. `ruler-knowledge.mjs` keeps bounded, authoritative correspondence facts,
disclosure decisions, quotations, and delivery receipts. `knowledgeView` strips
the private ledger and publishes only that recipient's approved quote metadata
and delivered reports. Gemini never receives the private ledger.

## What a ruler can know

Accepted, player-authored correspondence can record an expression of hostility,
a recognizable proposal to act against another House, or its withdrawal. A
ratified `JOINT_WAR` separately records the agreement. A proposal is not consent;
an agreement is not proof of an actual attack. Statements, evidence, participants,
and turns remain distinct. Questions, hearsay, negated attacks, and AI-generated
claims do not manufacture plots. The recognizer is deliberately conservative; it
is not a general-purpose natural-language fact extractor.

Old saves recover recognized facts once from retained player messages. Deleted
or rolled-off conversations cannot be reconstructed. Facts, offers, receipts,
and decisions have explicit size limits, and reports state their dates and limited
coverage rather than claiming perfect knowledge of everybody's intentions.

## Disclosure is a rule decision

Enquiries about hostile correspondence are evaluated from the ruler's own records.
Trust, relative alliances, active marriage bonds, and personality can produce a
limited disclosure, named disclosure, refusal, negotiated quotation, or deliberate
denial. A deliberate denial has real supporting facts and a private recorded
motive; lack of evidence instead produces an explicitly limited answer. Neither
answer establishes the absence of all conspiracies in the world.

The online controller recomputes this decision after accepting a command. It does
not trust a client/model's allegation, denial, proposed price, or memory summary.
The client can preview only an already approved report or a neutral pending reply.
Sensitive enquiries and their follow-ups work without Gemini or an API request.
Raw third-party messages, private decision motives, and undisclosed report contents
are excluded from player projections and prompt contexts.

## Real purchases

A quotation defines the actual seller, buyer, subject, anonymous/named scope,
fixed gold price, report, and expiry (three turns). Review it and use **Accept &
Ratify**. `INTELLIGENCE` refers to that specific quotation; it is not editable as a
generic gift or exchange. Ratification rechecks identity, exact terms, expiry,
hostilities, prior delivery, and funds before one debit, credit, and receipt.
Repeated commands, already delivered reports, forged prices, and unrelated gifts
cannot deliver or charge again. Free disclosures also create durable receipts so
a later quote cannot sell that same report back to the recipient.

Delivered reports are visible under Council Records / Disclosed correspondence
and survive save/reload and multiplayer controller reconstruction. Their dated
wording distinguishes approaches, hostility, withdrawals, and actual agreements.

## Authority boundary

This preserves the existing multiplayer trust model: the controller assembles
canonical simulation data and evaluates authenticated commands; ordinary player
views receive only their projections. It does not replace the trusted controller
with a hardened server or prevent the controller operator from inspecting its own
canonical state. No additional raw secrets are sent to other players or Gemini.

## Regression coverage

`ruler-knowledge.test.mjs` covers recognition, facts, deliberate denials, scope,
privacy, bounded history, and exact purchases. `court-knowledge-integration.test.mjs`
covers controller enforcement, split/join, non-host player views, prompt filtering,
save validation, real agreements, replay protection, and no-network replies.
`court-access-browser.mjs` exercises the visible desktop/mobile quote and marriage
controls without screenshots. `marriage-access.test.mjs` covers actual eligibility,
negotiation, ratification, payments, adult roles, and human-player consent.
