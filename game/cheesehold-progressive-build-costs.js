// Cheesehold progressive structure pricing
// Each structure type becomes more expensive independently during a round.
// Per-type counters reset when a new round begins. Building upgrades keep their existing prices.
const BASE_STRUCTURE_COST_OVERRIDES={turret:17};
const STRUCTURE_COST_STEPS={wall:1,superwall:1};
const DEFAULT_STRUCTURE_COST_STEP=2;

function roundStructureBuildCounts(){
 if(!game)return {};
 if(!game.roundStructureBuildCounts||typeof game.roundStructureBuildCounts!=='object'||Array.isArray(game.roundStructureBuildCounts))game.roundStructureBuildCounts={};
 return game.roundStructureBuildCounts;
}
function roundStructuresBuilt(type){
 return Math.max(0,Number(roundStructureBuildCounts()[type])||0);
}
function structureCostStep(type){
 return STRUCTURE_COST_STEPS[type]??DEFAULT_STRUCTURE_COST_STEP;
}
function structureBuildInflation(type){
 return roundStructuresBuilt(type)*structureCostStep(type);
}
function structureDiscount(type){
 const discounts=game?.mods?.structureDiscounts;
 return Math.max(0,Math.min(.65,Number(discounts?.[type])||0));
}
function addStructureDiscount(mods,types,amount){
 if(!mods.structureDiscounts||typeof mods.structureDiscounts!=='object')mods.structureDiscounts={};
 for(const type of types)mods.structureDiscounts[type]=Math.min(.65,(Number(mods.structureDiscounts[type])||0)+amount);
}
const baseStructureCost=structureCost;
structureCost=function(type){
 const rawBase=BASE_STRUCTURE_COST_OVERRIDES[type]??BUILDINGS[type].cost;
 const globallyAdjusted=Math.max(1,Math.ceil(rawBase*(1-game.mods.buildDiscount)));
 const inflated=globallyAdjusted+structureBuildInflation(type);
 return Math.max(1,Math.ceil(inflated*(1-structureDiscount(type))));
};
function noteStructureBuilt(type){
 const counts=roundStructureBuildCounts();
 counts[type]=roundStructuresBuilt(type)+1;
}
function resetRoundStructurePricing(){
 game.roundStructureBuildCounts={};
}

// Economy artifacts create focused fortress builds rather than only broad stat boosts.
ARTIFACTS.push(
 {id:'foundersHoard',icon:'🧀',name:"Founder's Hoard",max:1,desc:'Start every future round with +20 cheese.',apply:m=>m.roundCheese+=20},
 {id:'perpetualGear',icon:'⚙',name:'Perpetual Generator Gear',max:1,desc:'Generators produce +3 cheese and cycle 20% faster.',apply:m=>{m.generatorYield+=3;m.generatorRate*=.80}},
 {id:'generatorPatent',icon:'⌁',name:'Generator Patent',max:1,desc:'Generators cost 35% less cheese, including their rising round cost.',apply:m=>addStructureDiscount(m,['generator'],.35)},
 {id:'masonLedger',icon:'▦',name:"Mason's Ledger",max:1,desc:'Walls and Super Walls cost 30% less cheese.',apply:m=>addStructureDiscount(m,['wall','superwall'],.30)},
 {id:'gunnerContract',icon:'⌖',name:"Gunner's Contract",max:1,desc:'Turrets cost 25% less cheese.',apply:m=>addStructureDiscount(m,['turret'],.25)},
 {id:'arcLicense',icon:'☇',name:'Arc License',max:1,desc:'Tesla towers cost 25% less cheese.',apply:m=>addStructureDiscount(m,['tesla'],.25)},
 {id:'trappersKit',icon:'✣',name:"Trapper's Kit",max:1,desc:'Slow and Poison structures cost 25% less cheese.',apply:m=>addStructureDiscount(m,['slow','poison'],.25)},
 {id:'phoenixCharter',icon:'🔥',name:'Phoenix Charter',max:1,desc:'Phoenix structures cost 30% less cheese.',apply:m=>addStructureDiscount(m,['phoenix'],.30)}
);

// Late rounds use progressively larger battlefields. COLS/ROWS are mutable in the loader.
function cheeseholdArenaSize(round=game?.round||1){
 if(round>=25)return [47,35];
 if(round>=20)return [43,33];
 if(round>=15)return [39,31];
 if(round>=10)return [35,27];
 if(round>=5)return [31,25];
 return [27,21];
}
function applyCheeseholdArenaSize(round=game?.round||1){
 const [cols,rows]=cheeseholdArenaSize(round);
 COLS=cols;ROWS=rows;
}
const economyAndArenaBeginRound=beginRound;
beginRound=function(first=false){
 applyCheeseholdArenaSize(game.round);
 return economyAndArenaBeginRound(first);
};
