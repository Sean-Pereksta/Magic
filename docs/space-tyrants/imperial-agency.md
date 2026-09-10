# Imperial construction and fleet agency

This change extends the existing living-planets, physical logistics, persistent
fleet records, action receipts and galaxy renderer. The shipping loader loads
three new modules before the final inspector scroll-stability adapter.

## Player controls

Select an owned planet and use **Imperial Development** to choose a development
plan, enable governor construction, and authorize a credit allowance. Manual
orders remain available regardless of plan. Governors also commit the normal
material recipes; the allowance limits authorization fees, not material spending.
Each project offers Emergency, High, Normal, Low or Suspended priority, a labor
weight, priced rush options and cancellation with an explicit unused-material
refund. Existing empire-wide supply priority retains its additional weight.

Capacity depends on workforce, industry, shipyards, damage, gravity and traits.
Projects share finite effort. Missing material continues to be handled by the
existing supply ledger. Suspended projects retain delivered materials. Cancelled
projects release unused delivered goods; cargo already traveling remains physical
and returns to the planet's inventory. Colony expeditions retain their existing
specialized cancellation controls.

Planet geography and traits are stable saved properties. New city capacity
reflects world size and housing traits, preserving existing population/capacity.
Mining, research, growth, construction and defense traits affect their respective
systems. Planet and fleet histories retain the last 50 recorded events.

**Fleet Designer** provides eight built-in templates and saved custom templates,
six composition categories, seven roles, five doctrines and a commission review.
The normal quick commissioning controls remain available. Commissions use staged
vessels, local personnel and credits. Replacements consume raw materials and crew
at the selected shipyard, finish construction and travel to the persistent fleet.
Names, admirals and veterans survive reinforcement.

Escorts are faster; heavy ships increase power and siege effectiveness; carriers
extend detection; transports improve planetary assault; support ships reduce
crossing-battle losses. Patrol, Intercept, Defend, Raid and Escort roles make
periodic assignments when idle. Reserve waits for commands; Assault uses explicit
campaign orders. Evasive and Hold Position suppress autonomous pursuit.

## Continuous encounters

Registered military transits use continuous straight-line coordinates. Civilian
movement retains its existing implementation. A swept bounding-box broad phase
reduces candidate pairs, followed by a relative-motion quadratic that solves for
the first same-time contact inside a detection radius. Merely intersecting route
lines is insufficient. War and doctrine checks precede engagement.

Battles keep both transit records at a physical contact point and serialize with
the player empire. The winner resumes its original route, while the loser
retreats or is destroyed. Explicit interception projects the target's current
velocity; changing orders can invalidate that prediction. Nearby friendly fleets
can be ordered to join, with an ETA shown in their fleet controls. Interceptions
are against visible hostile fleets, not unknown map contacts.

The existing renderer now uses composition-specific hull silhouettes, separate
progress rings for concurrent construction, deep-space battle locations and
replacement transports. It retains existing cached geology, city lights,
industrial and orbital infrastructure, economy-driven traffic, detail budgets
and reduced-motion support. This is an extension of that renderer, not a new art
asset pack. Size/gravity/atmosphere are additional strategic descriptions; this
change does not add a bespoke atmospheric shader for every atmosphere category.

## Cost integrity

Mandate cards show immediate credits, material commitments, personnel where
applicable, and maximum additional policy running costs. Selection shows the
combined immediate credit total. Changed quotes must be reviewed again.
Execution receipts compare spending and new project commitments against the
quote; excess spending restores the simulation state instead of approving it.
Policies retain a lifetime allowance so acquiring more worlds cannot silently
increase their previously authorized running budget.

Legacy fleet programs now commission staged fleets without deferred purchases;
distributed factory mandates use raw-material construction. Transmission requests
count commissioning charges toward their advertised total instead of charging
twice. Factory/shipyard requests no longer require a hidden authorization fee.

## Verification

The Node integration harness loads the same core and ordered extensions as the
shipping game. New tests cover allocation, suspension, refunds, rush costs,
templates, commission conservation, movement crossings, nonconcurrent routes,
peace/evasive behavior, save recovery, resumption, replacements and cost ceilings.
Existing simulation and observational-renderer tests also run.

Final result: `npm run test:space-tyrants` — **248 passed, 0 failed**.

Baseline failures were checked independently before editing their expectations:
a recipe test referenced a runtime variable outside the harness; another still
expected manufactured inputs for factories/shipyards; transmission tests exposed
double billing and obsolete authorization fees. Those expectations now follow
the current resource-only factory/shipyard rules. The city completion assertion
now accounts for the requested size/trait-dependent new housing capacity.

No HTML preview or browser playtest was generated. Browser interaction, balance
and visual polish should receive manual review before merging this broad change.
