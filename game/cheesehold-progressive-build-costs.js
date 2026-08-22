// Cheesehold progressive structure pricing
// New structures become more expensive as the fortress grows during a round.
// The counter resets when a new round begins. Building upgrades keep their existing prices.
const BASE_STRUCTURE_COST_OVERRIDES={turret:17};
const STRUCTURE_COST_STEP=2;
const STRUCTURE_COST_ACCEL=0.12;

function roundStructuresBuilt(){
 return Math.max(0,game?.roundStructuresBuilt||0);
}
function structureBuildInflation(){
 const n=roundStructuresBuilt();
 return n*STRUCTURE_COST_STEP+Math.floor(Math.pow(n,1.32)*STRUCTURE_COST_ACCEL);
}
const baseStructureCost=structureCost;
structureCost=function(type){
 const discountedBase=baseStructureCost(type);
 const rawBase=BASE_STRUCTURE_COST_OVERRIDES[type]??BUILDINGS[type].cost;
 const adjustedBase=Math.max(1,Math.ceil(rawBase*(1-game.mods.buildDiscount)));
 return adjustedBase+structureBuildInflation();
};
function noteStructureBuilt(){
 game.roundStructuresBuilt=roundStructuresBuilt()+1;
}
function resetRoundStructurePricing(){
 game.roundStructuresBuilt=0;
}
