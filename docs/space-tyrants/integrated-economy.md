# Space Tyrants: integrated economy and fortresses

This extends the original economy/fortress PR to connect existing production, construction, freight, colonies, and independent bases. It does not create a separate inventory or save format.

## Player controls

- **Planet inspector → Integrated Economic Flow:** stock, commitments, actual inbound/outbound cargo, industrial load, colony development and the current bottleneck. Click a resource to cycle **BALANCED → LOCAL → EXPORT**. The persisted `project` routing value is displayed as LOCAL.
- **PRIORITIZE:** one Imperial project across ordinary construction, shipbuilding, physical infrastructure, and independent station upgrades. Replacing or clearing it uses the existing project controls.
- **Station inspector:** authorize Tier 2/3 upgrades, inspect required/delivered/staged/inbound modules, prioritize the upgrade, and see readiness, service capacity and recurring upkeep.
- **Transmissions → Empire Economy:** collapsible stock, recent production/consumption/net per command cycle, freight congestion and leading historical exporters/importers.
- **Economic Allocation → Heavy Mobilization:** higher Components/Equipment output with greater input consumption, weaker extraction, slower civilian growth and lower tax income.

## Physical accounting

Project materials leave planet stock when committed. Reserved material cannot be exported. A station keeps separate ledgers for modules staged at its sponsor and modules delivered to its site; incoming ships account for the difference. Destroyed freight removes its contribution to the supply calculation and reopens demand. Ship creation failure restores the proposed shipment's goods.

Domestic routing chooses nearby eligible suppliers and applies LOCAL/EXPORT reserves to every ordinary order. Routine projects yield to an Imperial priority. Essential first mines/factories/training infrastructure remain eligible to support recovery; routine upgrades are not exempt from priority. Manufacturing-input orders can requisition reserves when they are necessary to supply the priority.

Priority escalates after 12, 30, 50 and 80 simulation seconds: strategic reserves, production mobilization, personnel mobilization, then emergency freight. The final stage can redirect domestic commercial cargo from its current map position; it does not seize foreign trade commitments. Priority grants no stock and cannot overcome destroyed producers, exhausted deposits, hostile routes or unavailable population without a real recovery action.

Training converts civilian population into crew and consumes Equipment. Additional mobilization is capped by training infrastructure, available population, Equipment and the actual remaining shortage. Personnel still travel aboard transports. When there is no training or naval infrastructure, a paid Emergency Training Camp is constructed through the existing physical-project system before recruitment can begin.

Freight uses a finite empire capacity derived from starting logistics, yards, completed merchant hulls and supplied trade/logistics bases. Cargo dispatch pays a small credit charter fee and consumes available Helium; fuel-poor routes travel more slowly. Stations near a route reduce effective travel time. The existing 280 visible ship bound remains a technical safety limit.

## Expansion and capacity

A new colony mission spends Credits and commits Iron, Silicates, Titanium, Helium, Components, Equipment and technical crew. Settlers are deducted only after a colony vessel successfully launches. Landing no longer creates extra population or awards the target's untouched map stockpiles.

An active colony program ties up as much as 30% of factory, 18% of mining, 22% of training and 12% of research capacity; the burden falls as supplies accumulate and ends on departure. Other simultaneous construction also competes for a finite work budget. Priority receives a larger share, not instant completion. Infrastructure completed inside a scaled production tick is preserved.

New settlements progress through landing, outpost, young colony, established colony and developed world. Equipment, Components, Helium and Silicates support development. Poor supply reduces production and growth, pauses development, and eventually creates unrest and modest population loss. It does not instantly destroy the settlement. Established economies retain the original resource profiles, mining deposits and production chains.

## Stations

| Type | Tier 1 | Tier 2 | Tier 3 |
| --- | --- | --- | --- |
| Military | Frontier Bastion | Sector Fortress | System Citadel |
| Trade | Trade Anchorage | Orbital Exchange | Grand Commerce Ring |
| Logistics | Logistics Relay | Fleet Waystation | Strategic Logistics Nexus |
| Sensor | Listening Outpost | Deepwatch Array | Far-Reach Observatory |

Upgrades spend authorization Credits, stage actual modules, dispatch engineering convoys, and assemble only as materials arrive. A station remains operational while upgrading. Integrity, repair/service capacity, prepared-fleet benefits, logistics capacity and sensor coverage rise with tier through the existing base systems.

Maintenance consumes stored Helium, Equipment, Components and imperial Credits. Tier costs scale by 1.7 per tier, with modest extra network overhead above three stations. Missing any maintenance input lowers readiness over time. Repair and fleet preparation need paid maintenance and available service slots; an empty tender cannot restore readiness. Trade bases earn income when supplied. Logistics bases store larger maintenance reserves.

Station art adds pressure/habitat rings, docking arms, cargo modules, sensor arrays and military batteries. Upgrades show incomplete framework, a delivery ring, engineering traffic and welding lights. Military battles add battery and shield effects. Detail is culled offscreen and at distant zoom. Existing raids, blockades, interception, destruction/wrecks, independent fleet destinations and AI war objectives remain in use.

The former six-base gameplay ceiling is replaced with a 32-base technical safety bound. Capital, supply, freight and construction commitments provide the normal constraints.

## AI and persistence

AI factions choose a project priority, adjust output and routing every 20 simulation seconds, and use the same allocations, crew costs, colony commitments and physical upgrade pipeline. Existing economic/foreign-policy personalities influence project selection and expansion. Existing diplomacy, route warfare and expansion difficulty remain authoritative.

Routing, colony status, priority age, freight capacity, production totals, station staging, delivered modules and maintenance stock live on the saved planet/empire/base objects. Old saves default to balanced routing and Tier 1 bases where fields are absent. Existing paid goods and legacy physical-project freight remain credited. Planner caches and clocks are reset after loading or starting a new galaxy.

## Conservation defects repaired

- Orbital construction no longer produces input-free Components.
- Emergency Equipment production no longer has a free output floor with missing materials.
- Strategic reserve release requisitions actual stock or starts production rather than inventing goods/crew.
- Emergency procurement preserves fractional crew units instead of rounding them to millions.
- Tiny material/crew deficits retain supply orders; they cannot be silently dismissed before strict completion.
- Failed multi-leg trade restores freight fuel and charter fees as well as the original cargo transaction.
- A colony launch blocked by the technical ship bound keeps both its settlers and its paid project.

## Verification

Run `npm run test:space-tyrants` for the complete regression suite. The economy test file executes the shipping core and every ordered patch, rather than testing source-text snippets. It covers the requested priority, crew, routing, expansion, freight loss, station tiers/upkeep, AI, save/load and old-save behaviors, including a physical two-upgrade journey to Tier 3.

The shared harness now supplies the Web APIs needed by the existing cloud-save module and supports deterministic campaign seeds. Stale tests referring to the former four/five-choice command hands and pre-cloud-save loader order were updated to the current runtime behavior; command-hand implementation is unchanged.

Run `node game/space-tyrants/economy-campaigns.mjs 1800 docs/space-tyrants/economy-campaign-results.json` to reproduce three 30-minute campaigns. The runner uses the full simulation, including AI and combat. Its player controller only selects existing project priorities; it adds no resources, population, tech or speed advantages. It records phase snapshots, shortages, completions, population, production, freight, fleet power and station tiers.

Campaign findings and limitations are recorded in [economy-balance-results.md](economy-balance-results.md). These simulations validate accounting and expose balance pressure; they are not a substitute for human strategic play or browser performance testing. No browser/HTML preview, deployment or merge is part of this change.
