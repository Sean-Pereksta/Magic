# Living planets and player-controlled mobilization

This update connects the existing population, infrastructure, project ledgers,
trained-personnel stocks, fleet registry, physical freight and campaign saves.
The shipping loader installs `living-planets.js` after transmission routing and
before the inspector stability wrapper.

| Area | Behavior |
| --- | --- |
| Construction | Local and legacy orbital projects have independent arrays, progress and material ledgers. Multiple cities and ground factories can run together. Shared industrial labor remains visible; upgrades of the same orbital tier retain their prerequisite. |
| Population | Growth approaches housing capacity without overshoot. Completed cities increase capacity without immediately adding inhabitants. Existing population and capacity are retained. |
| Industry | Each existing factory switches freely between Components, Equipment and War Production. Infrastructure sets capacity; workforce utilization, technology, policies and supply readiness affect output. |
| Military | `stock.trained` remains the authoritative personnel pool. `reserveVessels` includes fractional manufacturing work; only whole vessels can mobilize. Active fleets remain separate registry objects. |
| Fleet commissioning | Presets of 6, 15 and 30 vessels, or a custom 1–500. Each vessel requires 200 personnel. The fleet appears at the selected safe shipyard and records its source world. |
| Regional staging | Owned safe worlds within 900 map units can send the chosen vessel count and matching personnel by visible transport. Arrival is required before commissioning. Capture redirects the cargo to a safe friendly world; failed dispatch does not deduct reserves. |
| Emergency production | An explicit order spends 50 credits, 30 Components and 20 Equipment locally for 10 reserve hulls. It does not commission a fleet or grant personnel. |
| Transmissions | Sticky authoritative resource totals, freight capacity, and Available / Required trade costs. Domestic requests start projects upon acceptance and cannot enter the diplomatic queue. Trades and diplomatic payments can still auto-accept when funded. |
| Presentation | Population, capacity, cities, industry, personnel, vessels, fleets, construction and orbit are grouped at the top of the inspector. Deterministic night-side settlement clusters and orbital traffic increase with population and infrastructure, within existing rendering budgets. |

## Spending rules

Normal factory output, personnel recruitment, research, player colony development,
player freight service and player station upkeep do not consume stored resources.
Governors cannot open new paid player projects during background simulation.
Automatic shipyard job creation and garrison-to-patrol conversion are retired.
Existing named fleets still move and fight through the fleet-command system.

Approved construction still reserves and transports its stated materials. Paid
policies retain their explicit activation costs and any stated special-program
upkeep (for example accelerated mining or growth). Accepted diplomatic contracts
retain their agreed payments and cargo. Battle losses and interdiction remain.
Background merchant cargo selection no longer exports player stock without an
order; passenger traffic and explicitly authorized freight remain available.

## Compatibility and units

Population and personnel keep the game's existing **millions** unit. Reserves
are derived once from old industrial/shipyard development; existing named fleets
are preserved without deducting their strength or replacing them. Existing city
counts, population, capacity, project progress and delivered materials survive.
A city's initial capacity contribution is derived from that world's saved
capacity and city count. Later cities add that contribution.

Local and orbital legacy pointers are retained for older readers; migration
reunites their JSON copies with the corresponding project arrays by stable ID.
Already-paid military shipyard jobs finish as reserve vessels and return their
reserved crew to the available personnel pool. Completion receipts prevent
repeat delivery. Obsolete domestic acceptance-queue entries are removed, leaving
the underlying request pending for a deliberate decision.

AI worlds use the same population, factory modes, personnel and reserve-based
commissioning. Their strategic planners continue to authorize their own projects.
Independent deep-space bases remain logistics facilities, not separate hull
inventories; regional transfers currently originate and stage on planets.

## Verification

- All **228 tests pass** in the full shipping-loader simulation suite, including all existing trade, colony
  cancellation, combat, cloud-save, fleet-command and inspector suites.
- New integration coverage for parallel project completion, migration, free
  background activity, factory conversion, labor, reserve accounting, failed
  transfer rollback, capture returns and AI commissioning.
- DOM interaction check for repeated city orders, fleet sizes, mobilization,
  persistent disclosure controls and the transmission resource header.

Legacy assertions that required recurring upkeep, automatic fleet generation or
queued domestic construction were updated to the new requested behavior. No HTML
preview was generated. Browser rendering and long-campaign gameplay balance have
not been manually playtested.
