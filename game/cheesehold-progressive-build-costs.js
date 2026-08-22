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
const baseStructureCost=structureCost;
structureCost=function(type){
 const rawBase=BASE_STRUCTURE_COST_OVERRIDES[type]??BUILDINGS[type].cost;
 const adjustedBase=Math.max(1,Math.ceil(rawBase*(1-game.mods.buildDiscount)));
 return adjustedBase+structureBuildInflation(type);
};
function noteStructureBuilt(type){
 const counts=roundStructureBuildCounts();
 counts[type]=roundStructuresBuilt(type)+1;
}
function resetRoundStructurePricing(){
 game.roundStructureBuildCounts={};
}
