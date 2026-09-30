# Army organization and named commanders

Select an owned land army and choose **Sort Army**. Two formation boxes open;
**Create another formation** adds up to eight boxes in one editing session.
The controller supports more than two results and the existing 500-army world
limit still applies. Troop cards come from `UNITS`.

Drag a card to move its whole stack. Select it for Move All, Split Evenly,
exact Move Amount / Keep Here inputs, or Prepare Partial Card. A partial card
appears beside its source troops. Every drag operation has a click/touch/keyboard
alternative through the destination selector and Move buttons. Quick Split uses
the same editor, with percentage and troop-family presets.

Names, formations, composition and commander assignments remain in a temporary
draft. Reset restores the opening draft; Cancel/Escape writes nothing. Confirm
validates every unit type, count, general and source snapshot before committing.
If the campaign changed, close and reopen the editor to start a fresh draft.
Empty new slots are discarded; an empty slot with a selected general must first
release that general. The original army ID stays with the first nonempty result.
No turn, resource, morale or movement-budget refresh is granted by sorting.

Issued movement, attacks and boarding are cleared on confirmation and explained
in the modal. Previously spent movement and resolved-turn markers are inherited.
Approved commander objectives remain for the next activation; the player override
protects the current activation. Invalid troop-dependent formations reset to
Balanced in the draft. Embarked troops must disembark before being sorted.

## Command rules

A manual split leaves its original commander with the primary army and creates
manual detached armies. Assign a different general to each new formation.
Transferring a commander from another force requires explicit Transfer Command
confirmation and releases the previous forces to manual control. Generals keep
their conversation, objective and loss-limit history.

Existing player-approved autonomous detachments remain part of a single command;
this exception preserves general AI. They do not create extra general characters.
The general cannot reclaim a new manually organized force or silently merge it
back into that command.

Merging adopts the sole commander when only one exists. With two or more
commanders the player must choose one; the other commanders become unassigned,
including their autonomous detachments. No troops or characters are discarded.

Kingdom → Generals shows names, portrait slots, specialties, assignments, troop
counts, status, View Army, Chat and Change Orders. Chat from an army opens the
same general ID and history. The default dialogue is compact; reviewing orders
expands the existing approval panel. Model context now includes current troop
composition, strength, morale, formation, losses, supplies, observed threats and
recent known battles. All external observations use the existing Fog of War view.
Local replies also describe actual composition and available siege support.

## Finite roster and artwork

`general-roster.mjs` defines exactly sixteen named identities across all Houses.
Candidate offers reserve identities, dismissed generals stay in a bounded retired
record, and expired offers can reappear as the same character. Recruitment cannot
create a seventeenth identity. Quality, upkeep, paid mustering and the existing
five tactical behavior profiles remain in use; the roster maps the new character
personalities to those profiles. A House may recruit any remaining available
characters; it is no longer capped at four.

The existing R2 manifest preloads these square portrait slots. Upload the final
head-and-shoulders portraits to the listed paths under `IRON_THRONES_ASSET_BASE`.
This code change does not supply new PNG artwork. Each slot displays a named,
accessible initials fallback until the matching portrait is available.

| General | Personality | Portrait path |
| --- | --- | --- |
| Garrick Rowan | Steadfast | portraits/generals/garrick_rowan.png |
| Edric Vale | Cautious | portraits/generals/edric_vale.png |
| Alaric Thorn | Bold | portraits/generals/alaric_thorn.png |
| Cedric Ashford | Honorable | portraits/generals/cedric_ashford.png |
| Roderic Blackwell | Ruthless | portraits/generals/roderic_blackwell.png |
| Tristan Marlowe | Opportunistic | portraits/generals/tristan_marlowe.png |
| Osric Fen | Protective | portraits/generals/osric_fen.png |
| Lucan Grey | Watchful | portraits/generals/lucan_grey.png |
| Theon Harrow | Methodical | portraits/generals/theon_harrow.png |
| Merek Stone | Unyielding | portraits/generals/merek_stone.png |
| Corvin Hale | Pragmatic | portraits/generals/corvin_hale.png |
| Dorian Veyne | Ambitious | portraits/generals/dorian_veyne.png |
| Aldren Pike | Disciplined | portraits/generals/aldren_pike.png |
| Kael Rivers | Restless | portraits/generals/kael_rivers.png |
| Bram Wycliff | Patient | portraits/generals/bram_wycliff.png |
| Reynard Crow | Cunning | portraits/generals/reynard_crow.png |

Legacy saves with at most sixteen current generals/candidates map once to the
named roster, retaining IDs, assignments, history, objectives and the old name in
`previousName`. A legacy save already exceeding sixteen fails with an explicit
migration message and remains untouched; it does not silently delete commanders.

## Multiplayer and checks

`reorganize` is one command through the existing authenticated controller and
Firestore transaction. Clients submit a draft, never replacement campaign state.
The controller checks ownership, activation, state version, source stamp, unit
conservation, assignment ownership/uniqueness and transfer consent. The existing
receipt sequence prevents replay; projections publish all resulting armies at
one version. Deploy the updated Firestore rules with the game to admit the new
command. No production rules are deployed by this PR.

Run:

```sh
npm run test:iron-throne
npm run test:iron-throne:army-sort-browser
npm run test:iron-throne:command-browser
firebase emulators:exec --only firestore --project demo-iron-thrones \
  --config firebase.iron-throne-test.json \
  "node game/iron-throne/tests/firebase-rules.mjs"
```

Browser checks cover desktop and 390px touch layouts without screenshots or HTML
previews. They exercise full/partial drag and touch moves, exact counts, Reset,
Cancel, three armies, independent commanders, shared dialogue, merge choice,
Quick Split and reload. Engine tests exercise hostile payloads, source changes,
movement retention, finite identities, save/load and multiplayer projections.
