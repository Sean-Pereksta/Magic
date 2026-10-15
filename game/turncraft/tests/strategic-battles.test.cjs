const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

for(const [map,count]of [['fourfold-divide',4],['rift-dominion',8]])test(`${count}-commander free-for-all sustains combat, AI economies and serialization on its actual authored map`,()=>{
  const run=simulation(),result=run.plain(`(()=>{
    chosenMapId=${JSON.stringify(map)};chosenAiCount=${count-1};chosenDifficulty='hard';createGame(buildSoloSlots());
    performance.now=()=>testClock;const center=mapLayout().relic;tileVisibleToFaction=(f,x,y)=>{rebuildVisionCache();return !!visionCache.get(f)?.has(x+','+y)};
    for(const p of players){p.minerals=1400;p.gas=700;const types={frontier:['rifleman','marauder','medic'],ascendant:['zealot','dragoon','sentinel'],brood:['zergling','roach','hydralisk']}[p.factionId],angle=p.idx*Math.PI*2/players.length;
      for(let i=0;i<6;i++){const q=nearestFreeAround(Math.round(center.x+Math.cos(angle)*6),Math.round(center.y+Math.sin(angle)*6),null,6);if(q){const unit=addEntity('unit',types[i%types.length],p.idx,q.x,q.y);issueAttackMoveOrder(unit,center.x,center.y)}}
    }
    const initial=entities.filter(e=>e.kind==='unit').length;let bad=false,started=matchStartedAt;
    for(let tick=0;tick<600&&!gameOver;tick++){testClock+=100;updatePersistentOrders();updateCombat(.1);resolveProjectileImpacts();updateProduction(.1);updateRegeneration(.1);updateAreaEffects();updateObjectives(.1);aiThinkAll();
      bad||=entities.some(e=>e.alive!==false&&(!inBounds(e.x,e.y)||!Number.isFinite(e.hp)||!Number.isInteger(e.faction)));
    }
    const snapshot=JSON.parse(JSON.stringify(serializeState())),kills=players.reduce((sum,p)=>sum+p.kills,0),ai=players.filter(p=>p.ai);
    return {players:players.length,initial,kills,bad,aiTicks:ai.every(p=>p.lastAi>0),economies:ai.every(p=>Number.isFinite(p.minerals)&&Number.isFinite(p.gas)),bytes:JSON.stringify(snapshot).length,roundtrip:applyRemoteState(snapshot),width:BOARD_W,elapsed:testClock-started};
  })()`);
  assert.equal(result.players,count);assert.ok(result.initial>=count*10);assert.ok(result.kills>0,JSON.stringify(result));assert.equal(result.bad,false);assert.equal(result.aiTicks,true);assert.equal(result.economies,true);assert.equal(result.roundtrip,true);assert.equal(result.width,count===8?128:96);assert.ok(result.bytes<950000);
});
