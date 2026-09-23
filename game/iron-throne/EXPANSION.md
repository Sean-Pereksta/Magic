# Deep economy, construction, trade and warfare

This expansion stays in the existing static ES-module/Canvas game. `core.mjs` remains the campaign authority; `diplomacy.mjs` still evaluates and ratifies every player agreement. No language-model call was added to the turn simulation. The existing configured Gemini session, diagnostics, ambassadors, promises, treaties and browser save key remain in place.

## Geography and production

Seeded regional biases follow the six capitals. Ashen has balanced land without exceptional opening deposits; Wintermere favors food and timber; Thornwall stone and iron; Sunspire commerce; Vesper manufacturing; Redharbor food and horses. Regional identity, deposit quality and production are visible in inspection. Starting supplies provide a buffer; geography and investment determine replenishment.

Poor, normal, rich and exceptional deposits multiply the site's output by 0.65, 1, 1.5 and 2. A matching House industry adds 12%; tiers multiply base output by 1, 1.65 and 2.3. Farms gain from fertile deposits; river plains favor agriculture. Workshops transform wood/iron into tools; armories and royal arsenals transform wood/iron into arms. Manufacture shares one input budget across the kingdom and pauses when supplies are missing. Upkeep and production use the same forecast shown in the UI.

Storage starts at 600 for materials; storehouses add 400 per level. Farms and great granaries add food storage. Gold retains the existing 99,999 limit. Imported old reserves are retained even above a new cap; extra production is held back until storage is available. Great granaries halve famine attrition.

## Construction and military infrastructure

`data.mjs` contains 28 construction families and 72 completed tier definitions. Most families have three levels. Town/city foundations and the four named great projects are single major projects. Inspection shows the exact next cost, remaining turns, production, requirements and tier artwork.

| System | Progression / behavior |
| --- | --- |
| Food, wood, stone, iron, horses | Three production levels, affected by deposit quality |
| Market | Market → Merchant Quarter → Grand Bazaar; gold and contract slots |
| Workshop | Three manufacturing levels; tools and additional construction orders |
| Armory | Three arms manufacturing levels; Royal Arsenal adds a larger production chain |
| Storehouse | Storehouse → Warehouse → Royal Granary; Great Granary adds 2,400 food storage |
| Trade Outpost | Trading Post → Merchant Outpost → Grand Exchange; 2/3/4 turns, gold, shipment and contract capacity |
| Road | Road → Stone Road → Royal Highway; 0.5/0.4/0.3 movement per edge |
| Barracks | Levies/spearmen → men-at-arms → heavy infantry; higher tiers muster infantry faster |
| Archery Range | Archers → veteran archers → heavy crossbowmen |
| Military Stables | Scouts/light cavalry → heavy cavalry → knights |
| Siege Works | Rams → catapults → trebuchets; Siege Foundry reduces siege recruitment costs by 20% |
| Fort / Walls | 45/100/180 intrinsic fort strength and 60/120/180 wall strength |
| Watchtower | Claims nearby ground, defensive bonus and adjacent enemy movement control |
| Envoy Office / Chancery | Existing message allowance preserved; upgraded offices improve envoy travel, chanceries add administration income |
| Harbor | Coastal commerce and port-to-port abstract shipping; no naval combat |
| Merchant Guild | Gold, additional contracts and regional commercial income |

Costs are paid once at the start, one project occupies a tile, and the existing tier operates until completion. A captured project is cancelled. Repairing maximum-level walls/forts costs 40% of the tier cost and two turns. Advanced timberworks/workshops shorten eligible projects. Royal Highway can upgrade an entire existing, owned road corridor between settlements in one paid project order; every tile completes its own remaining staged work before the corridor bonus applies.

Harbors, Envoy Offices and Chanceries require towns or cities, where their trade and diplomatic effects operate. Forts support military infrastructure. Watchtower tiers apply their 15% per-level defense multiplier to both phased-battle casualties and final combat power.

Select land for categorized construction and local recruitment. Locked options state their reason. Commercial connections display income and offer a reviewable highway project. Settlement improvements appear in inspection and as compact map art.

## Trade

Trade proposals use actual needs, outgoing obligations, production, partner surplus, relationship, safe route access, industrial goals and dependency. Ordinary offers are considered every other turn and use a four-turn House cooldown. Food crises use a two-turn cooldown. At most one unsolicited major trade offer is posted per turn, with one pending offer per House. Proposals expire after three turns and never spend player resources automatically. Rival-to-rival exchanges use the same actual treasuries and resource constraints.

Incoming Trade Dispatch cards provide review, counter, decline and negotiation. Opening an offer transfers nothing; **Accept & Ratify** re-evaluates the exact terms. Sustainable partners with outposts may propose recurring supplies.

| Terms | Rules |
| --- | --- |
| Immediate exchange | Safe passage and two real resource transfers |
| Gold purchase | One side must provide gold |
| Recurring contract | Rechecked each turn, limited by infrastructure and route capacity |
| Strategic supply | Requires 30 trust; 50% larger shipment capacity |
| Emergency shipment | Immediate; a seller with scarce supplies demands a premium |
| Preferential agreement | Alliance or 45 trust; transport fees waived |

Caravans cost up to two gold per party per recurring shipment. Outposts reduce the fee; connected roads remove it. Road tiers improve capacity and road income; ports support abstract coastal shipments. Contracted outposts and harbors are remembered, so their loss interrupts supplies. A blocked route pauses both transfers; after three disrupted turns the contract expires. Insolvency ends it without a partial transfer. War and embargo end shipments. Different contracts can share a partner, but an identical active shipment cannot be ratified twice.

Dependence is the largest resource share supplied by a partner over the recent trade window, divided by production plus imports from **all** suppliers. Diversification lowers that share and the existing political dependence statistic continues influencing wars and diplomacy. Foreign treasury totals are not exposed in the player UI; qualitative shortages and surpluses are public.

## Armies and combat

Every surviving rival takes a complete strategy turn when the player ends the round. [STRATEGY.md](./STRATEGY.md) documents the shared rules, economic reserves, ranked construction and recruitment, war decisions, coordinated military orders and visible six-round activity history.

There are 14 recruitable classes plus the old `siege` class retained for saves. Local building levels and materials unlock recruitment. Splits/merges retain every class. Mounted and siege families affect movement and upkeep. Formation controls support Balanced, Defensive Line, Spear Wall, Aggressive Charge, Skirmish and Flanking; spears/cavalry are required for the specialized formations.

Every field encounter records positioning, missile fire, charge, melee, flanking, morale and pursuit. Combat uses campaign RNG exclusively. Spears counter mounted charges; forests reduce volleys and mounted attacks; hills and cities favor defenders; undeveloped river crossings disrupt attacks. Armor, crossbows, scouts, elite morale, casualties, homeland defense, isolation and past retreats affect resolution. Rams have negligible field power. The losing side withdraws or routs, and mounted troops can pursue a rout. Reports under Realm show starting composition, losses by class and causal phase notes.

Walls and intrinsic fort strength are attacked before a defending army can be assaulted. Rams, catapults and trebuchets have distinct breach performance, especially against tier-III defenses. Garrison missiles can destroy equipment. Siege morale declines across turns, with exhausted weak garrisons able to surrender. A breach permits an assault on a later order resolution. Short result-driven effects respect reduced motion and never block simulation.

## Artwork, saves and validation

`asset-manifest.mjs` centralizes paths for 106 original repository SVG assets: 72 structures, 15 unit classes, eight resources, eight headers and three construction stages. `assets/generate-art.mjs` regenerates those exact vector assets. Loading is local, lazy and bounded to 128 images; existing Canvas art is retained as fallback. Map rendering still culls off-screen tiles and retains its bounded procedural cache. The existing lobby art reference is unchanged.

Schema **3** migrates versions 1 and 2 without rerolling geography or deleting diplomacy/history. Old structures become level I, older armies remain usable, new resource balances default to zero, and existing recurring agreements retain their old route behavior. Expanded fields are validated before a campaign loads.

Run `npm run test:iron-throne`, `node --experimental-vm-modules --test lobby/tests/lobby-library.test.cjs`, and `npm run test:iron-throne:browser`. Browser tests accept `IRON_THRONE_CHROMIUM` when an external Chromium executable is needed. They exercise desktop and mobile without generating preview files or using live Gemini quota. Deploy the static files and updated Worker together so the model schema recognizes the expanded resources and trade terms.
