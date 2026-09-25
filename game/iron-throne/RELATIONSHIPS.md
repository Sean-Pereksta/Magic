# Personal relationships and negotiated royal marriage

Personal feelings sit alongside the existing political attitude. They are directional: one ruler can feel gratitude while the other feels protective, and an alliance can coexist with bitterness. The council shows descriptive feelings and rare bonds; it does not expose emotional scores.

## How relationships grow

`emotions.mjs` owns bounded per-relationship state. Only simulation events change it. Model interpretations remain unverified prose.

- Courteous conversation creates at most a little fondness; it cannot create affection, loyalty, or a named bond. Repeated messages and split gifts do not farm a turn.
- Completed promises, delivered aid, reliable shipments, private shared planning, and completed military campaigns accumulate history on distinct turns.
- Personality reuses honor, ambition, greed, and paranoia. Honor weights promises, ambition weights military admiration and humiliation, greed weights gifts, and paranoia weights suspicion.
- Saving an allied capital from a substantial attacking force can immediately create gratitude and a Life Debt. Capture, coercion, broken military commitments, and invasion can immediately cause humiliation, bitterness, and suspicion.
- Betrayed friendship requires personal closeness **before** the betrayal. Ordinary hostility does not invent a former friendship.
- Feelings influence Gemini context, local conversation openings, cooperation evaluations, and assistance priorities. Trade still requires fair resource value.

Rare bonds require evidence: Trusted Confidant needs a long relationship, high trust/reliability, several confidential operations, and multiple deed turns; Brothers/Sisters in Arms needs at least two completed shared campaigns; Life Debt needs an extraordinary rescue; Patron/Protected House requires sustained support; Personal Rival needs repeated clashes and respect. Trusted confidants can hear two current strategic concerns from that court, without hidden army orders or exact treasury figures. Positive bonds can lapse as trust collapses. Rescue history survives normal memory pruning.

## Marriage flow

1. Build a history of dependable actions. AI eligibility requires at least eight relationship turns, four distinct deed turns, sufficient trust/respect/reliability, limited grievances, and a blend of personal regard and political usefulness. High opinion or an enormous payment alone is insufficient.
2. Raise marriage in conversation, for example “Would you consider joining our families?”, “I seek the hand of your daughter,” or “Would you marry me?” Each court has three lightweight adult roles: ruler, adult daughter, adult son. There is no dynasty, succession, or child simulation.
3. Continue on a later turn. Ordinary follow-ups about terms, resources, defense, or shipments continue the discussion. Repeated messages in one turn do not replace deliberation. Discussions expire after 12 turns of silence and can be withdrawn.
4. Review the proposed settlement. The existing treaty desk exposes marriage terms only after this discussion. A different family match must be discussed first. Cost depends on the recipient’s wealth, personality, needs, personal relationship, and the power it has actually observed.
5. Ratify the exact terms. A quote or model reply never transfers resources, creates a marriage, or consumes a family member. Eligibility, family availability, and affordability are checked again before mutation. Human recipients must explicitly consent through the existing multiplayer proposal path.

Settlements support an upfront payment in gold, food, iron, or horses; optional scheduled shipments; 10–20 turns of mutual peace; optional mutual defense during that term; and an optional trade agreement. The existing ledger retains the partners, terms, status, and remaining scheduled shipments. AI does not initiate marriage proposals.

Shipments begin next turn, charge once per turn, and end on the agreed schedule. A missed installment is a breach, not an accumulating hidden debt. The family bond itself persists beyond the economic and peace terms.

## Family duties and failure

An intact marriage strengthens personal regard and trust, reduces border wariness, improves cooperation, and makes an existing ally more willing to share its own dated intelligence. It grants no omniscient information, free army movement, or automatic allegiance.

A defensive settlement creates a normal three-turn war pledge when the spouse’s House is attacked. It does **not** declare war automatically. A conflicting alliance remains the player’s decision; the existing pledge ledger judges the result. Attacks on a married House immediately break the bond and create a major grievance. Repeated missed promises, failed shipments, or humiliation strain and eventually break it. A strained marriage can recover when grievances subside and both courts restore confidence. Broken marriages remain in history and their participants remain unavailable; divorce and remarriage are outside this update.

## Persistence and visibility

Old schema-3 campaigns gain empty personal history and no fabricated marriages when loaded. New emotional and family records have explicit size, identity, range, and uniqueness validation. Battle projections skip relationship mutations. Multiplayer controller state retains canonical records; public and other-player views exclude private negotiations and third-party personal memories. Only public marriage identities/status are visible to unrelated Houses. AI decisions retain the existing House-specific knowledge projections.

## Validation

- `npm run test:iron-throne`
- `node --test game/iron-throne/tests/relationships.test.mjs`
- `node game/iron-throne/tests/relationships-browser.mjs` (Playwright; functional checks without screenshots or previews)

The new scenarios cover word farming, real promises, rescue/debt, betrayal prerequisites, personality, deliberation, settlement variability, family availability, atomic payment and replay protection, scheduled/defaulted shipments, conflicting defense duties, context privacy, migration/corrupt saves, battle projections, and multiplayer consent.
