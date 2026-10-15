const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

test('idle troops far from a flank waypoint retry it instead of skipping to the enemy base',()=>{
  const run=simulation(),result=run.plain(`(()=>{chosenMapId='fourfold-divide';setBoardDimensions();terrain=mapLayout().terrain;const b=mapLayout().starts[0];addEntity('structure','commandHub',0,b.x,b.y);
    const units=Array.from({length:8},(_,i)=>addEntity('unit',i%3?'rifleman':'medic',0,b.x+i%3,b.y+Math.floor(i/3))),ids=units.map(u=>u.id),wave={ids,approaches:[{ids,points:[{x:40,y:33},{x:81,y:81}],index:0}]};
    aiAdvanceApproaches(wave,0);const stayed=wave.approaches[0].index===0,reissued=units.every(u=>u.moveDestination||u.attackMoveDestination);
    units.forEach((u,i)=>{u.x=40+i%3;u.y=33+Math.floor(i/3);u.targetId=null;clearMoveDestination(u);u.attackMoveDestination=null});testClock+=4000;aiAdvanceApproaches(wave,0);
    return {stayed,reissued,advanced:wave.approaches[0].index===1};})()`);
  assert.deepEqual(result,{stayed:true,reissued:true,advanced:true});
});

test('a large initial formation builds the shared map field before individual routes',()=>{
  const run=simulation(),result=run.plain(`(()=>{chosenMapId='rift-dominion';setBoardDimensions();terrain=mapLayout().terrain;const home=mapLayout().starts[0],target=mapLayout().starts[4],spots=formationTargets(home.x,home.y,169).filter(p=>!groundBlocked(p.x,p.y));
    const units=spots.slice(0,24).map((p,i)=>addEntity('unit',i%4?'rifleman':'medic',0,p.x,p.y)),before=navigationCache.builds;orderFormation(units.map(u=>u.id),target.x,target.y,0);
    return {builds:navigationCache.builds-before,hits:navigationCache.hits,shared:units.every(u=>u.formationAnchor?.x===target.x&&u.formationAnchor?.y===target.y)};})()`);
  assert.equal(result.shared,true);assert.ok(result.builds<=2,JSON.stringify(result));assert.ok(result.hits>=20,JSON.stringify(result));
});

for(const faction of ['frontier','ascendant','brood'])test(faction+': Hard AI fortifies owned expansions then adds usable forward production',()=>{
  const run=simulation(faction),result=run.plain(`(()=>{chosenMapId='fourfold-divide';setBoardDimensions();terrain=mapLayout().terrain;const home=mapLayout().starts[0],natural=mapLayout().expansions.find(p=>p.naturalFor===0),fac=FACTIONS[players[0].factionId],prod={frontier:'barracks',ascendant:'gateway',brood:'broodColony'}[fac.id],defense={frontier:'missileTurret',ascendant:'photonCannon',brood:'sunkenColony'}[fac.id];
    players[0].ai=true;players[0].aiDifficulty='hard';players[0].minerals=5000;players[0].gas=5000;addEntity('structure',fac.base,0,home.x,home.y);addEntity('structure',fac.base,0,natural.x,natural.y);
    addEntity('structure',prod,0,home.x+1,home.y);if(fac.id==='frontier')addEntity('structure','academy',0,home.x+2,home.y);if(fac.id==='ascendant'){addEntity('structure','forge',0,home.x+2,home.y);addEntity('structure','pylon',0,home.x,home.y+2)}if(fac.id==='brood')addEntity('structure','evolutionNest',0,home.x+2,home.y);
    for(let i=0;i<4;i++)addEntity('unit',fac.worker,0,natural.x-2+i,natural.y-2);
    function settle(){for(let i=0;i<60;i++){testClock+=250;aiManageBuilders(0);updatePersistentOrders();updateProduction(.25)}}
    const first=aiFortifyForward(0);settle();testClock+=26000;const second=aiFortifyForward(0);settle();
    // Ascendant builds permanent power before its cannon, then the gateway.
    if(fac.id==='ascendant'){testClock+=26000;aiFortifyForward(0);settle()}
    const structures=playerStructures(0),defender=structures.find(e=>e.type===defense&&dist(e,natural)<8),production=structures.find(e=>e.type===prod&&dist(e,home)>12),safe=structures.filter(e=>dist(e,home)>12).every(e=>!mapLayout().chokes.some(c=>dist(e,c)<Math.max(2,c.width/2)+1));
    return {first,second,defended:!!defender,production:!!production,powered:!production||structurePowered(production),creep:fac.id!=='brood'||isCreepTile(0,production.x,production.y),safe};})()`);
  assert.equal(result.first,true,faction);assert.equal(result.second,true,faction);assert.equal(result.defended,true,faction);assert.equal(result.production,true,faction);assert.equal(result.powered,true,faction);assert.equal(result.creep,true,faction);assert.equal(result.safe,true,faction);
});

test('forward fortification respects difficulty, build reservations and nearby enemy danger',()=>{
  const run=simulation(),result=run.plain(`(()=>{chosenMapId='fourfold-divide';setBoardDimensions();terrain=mapLayout().terrain;const home=mapLayout().starts[0],natural=mapLayout().expansions.find(p=>p.naturalFor===0);addEntity('structure','commandHub',0,home.x,home.y);addEntity('structure','commandHub',0,natural.x,natural.y);addEntity('structure','academy',0,home.x+2,home.y);addEntity('unit','engineer',0,natural.x,natural.y-1);
    players[0].aiDifficulty='easy';const easy=aiFortifyForward(0);players[0].aiDifficulty='pro';players[0].strategy.buildJob={type:'barracks'};const reserved=aiFortifyForward(0);players[0].strategy.buildJob=null;addEntity('unit','dragoon',1,natural.x+3,natural.y+2);const threatened=aiFortifyForward(0);return {easy,reserved,threatened,job:players[0].strategy.buildJob};})()`);
  assert.deepEqual(result,{easy:false,reserved:false,threatened:false,job:null});
});

test('eight commanders stagger their first AI decisions after a long menu dwell and share one intel update per batch',()=>{
  const run=simulation('frontier',{factions:Array(8).fill('frontier')}),result=run.plain(`(()=>{let intel=0;const calls=[],updates=[];performance.now=()=>testClock;updateLastSeenIntel=()=>intel++;aiThink=(f,cfg,skipIntel)=>{calls.push({f,t:testClock});aiUpdateStrategy(f,cfg,skipIntel)};players.forEach((p,i)=>{p.ai=i>0;p.aiDifficulty='hard';p.lastAi=0;p.aiPhaseAt=null});testClock=600000;aiThinkAll();const first=calls.length;
    for(let i=0;i<9;i++){testClock+=250;const before=intel;aiThinkAll();updates.push(intel-before)}return {first,commanders:new Set(calls.map(c=>c.f)).size,phases:new Set(calls.map(c=>c.t)).size,maxIntel:Math.max(...updates)};})()`);
  assert.equal(result.first,0);assert.equal(result.commanders,7);assert.ok(result.phases>=4,JSON.stringify(result));assert.equal(result.maxIntel,1);
});

test('distance fields share bounded terrain masks while friendly shield passage remains faction specific',()=>{
  const run=simulation(),result=run.plain(`(()=>{const a=addEntity('unit','rifleman',0,5,5),b=addEntity('unit','dragoon',1,8,5),first=navigationField(a,30,20),other=navigationField(a,31,20);const shared=first.blocked===other.blocked;temporaryObstacles.push({id:'wall',kind:'shieldWall',x:12,y:12,faction:0,hp:100,endAt:testClock+5000});testClock+=600;const friendly=navigationField(a,30,20),enemy=navigationField(b,30,20),index=12*BOARD_W+12;return {shared,refreshed:first.blocked!==friendly.blocked,friendly:friendly.blocked[index],enemy:enemy.blocked[index],masks:navigationCache.masks.size};})()`);
  assert.equal(result.shared,true);assert.equal(result.refreshed,true);assert.equal(result.friendly,0);assert.equal(result.enemy,1);assert.ok(result.masks<=8);
});
