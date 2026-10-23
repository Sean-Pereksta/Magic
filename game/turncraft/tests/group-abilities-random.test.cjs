const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

test('seeded crowded Blink assignments remain deterministic, legal and collision-free',()=>{
  for(let seed=1;seed<=40;seed++){
    const run=simulation('ascendant');
    const fixture=run.plain(`(()=>{let seed=${seed};const random=()=>{seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed/4294967296};
      const corner=${seed}%4,origin={x:corner===0?1:corner===1?BOARD_W-6:12,y:corner===2?1:corner===3?BOARD_H-5:12};
      for(const t of terrain)t.blocked=random()<.16;
      globalThis.army=Array.from({length:12},(_,i)=>{const x=origin.x+i%4,y=origin.y+Math.floor(i/4);terrain[y*BOARD_W+x].blocked=false;return addEntity('unit','dragoon',0,x,y)});
      army[10].abilityReadyAt=testClock+5000;army[11].abilityLockedUntil=testClock+5000;
      for(let i=0;i<35;i++){const x=Math.max(0,Math.min(BOARD_W-1,origin.x+Math.floor(random()*15)-5)),y=Math.max(0,Math.min(BOARD_H-1,origin.y+Math.floor(random()*13)-5));if(!entityAt(x,y)){terrain[y*BOARD_W+x].blocked=false;addEntity(i%4===0?'structure':'unit',i%4===0?'spawningPool':'zergling',i%3===0?0:1,x,y)}}
      const destination={x:Math.floor(random()*BOARD_W),y:Math.floor(random()*BOARD_H)},before=JSON.stringify(entities),plan=planGroupAbility(army,'phaseStep',destination),reverse=planGroupAbility(army.slice().reverse(),'phaseStep',destination),pure=before===JSON.stringify(entities);
      const illegal=plan.casts.filter(c=>{const e=entityById(c.casterId);return Math.hypot(c.x-e.x,c.y-e.y)>ABILITIES.phaseStep.range+.05||groundBlocked(c.x,c.y,e)||!lineClear(e.x,e.y,c.x,c.y,e.id)});
      globalThis.origins=new Map(entities.map(e=>[e.id,{x:e.x,y:e.y,cooldown:e.abilityReadyAt,ability:e.lastAbilityId}]));
      const foreign=entities.find(e=>e.faction===1&&e.kind==='unit');
      applyCommand({type:'groupAbility',slot:0,entityIds:[...army.map(e=>e.id),army[0].id,foreign?.id].filter(Boolean),abilityId:'phaseStep',...destination,clientCommandId:'random-${seed}'});
      const actual=army.filter(e=>origins.get(e.id).x!==e.x||origins.get(e.id).y!==e.y).map(e=>e.id).sort((a,b)=>a-b),expected=plan.casts.map(c=>c.casterId).sort((a,b)=>a-b);
      const duplicate=entities.filter(e=>e.alive!==false&&!e.garrisonedIn).map(e=>e.x+','+e.y),incorrectCooldown=entities.filter(e=>{const old=origins.get(e.id),cast=expected.includes(e.id);return cast?e.abilityReadyAt<=testClock:e.abilityReadyAt!==old.cooldown});
      return {pure,deterministic:JSON.stringify(plan)===JSON.stringify(reverse),illegal,actual,expected,duplicate,incorrectCooldown:incorrectCooldown.map(e=>e.id),disabledMoved:actual.includes(army[10].id)||actual.includes(army[11].id)};
    })()`);
    assert.equal(fixture.pure,true,`seed ${seed}: preview changed state`);
    assert.equal(fixture.deterministic,true,`seed ${seed}: input order changed assignment`);
    assert.deepEqual(fixture.illegal,[],`seed ${seed}: illegal landing`);
    assert.deepEqual(fixture.actual,fixture.expected,`seed ${seed}: planner and execution differ`);
    assert.equal(new Set(fixture.duplicate).size,fixture.duplicate.length,`seed ${seed}: collision`);
    assert.deepEqual(fixture.incorrectCooldown,[],`seed ${seed}: cooldown consumed incorrectly`);
    assert.equal(fixture.disabledMoved,false,`seed ${seed}: unavailable unit moved`);
  }
});
