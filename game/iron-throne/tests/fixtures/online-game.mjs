import assert from 'node:assert/strict';
import { setupMeta, claimSeat, startCampaign, houseIds } from '../../multiplayer-rounds.mjs';
import { planFoundings } from '../../founding.mjs';
import { applyCommand } from '../../multiplayer-commands.mjs';
const ready=new Map();
// Existing round/economy tests now enter through the real founding command path.
export function onlineGame(count=2) {
  if(!ready.has(count)){
    const meta=setupMeta({hostUid:'u0'},1000);
    for(let i=0;i<count;i++)claimSeat(meta,`u${i}`,`Ruler ${i}`,houseIds[i]);
    const state=startCampaign(meta,'u0',1000);meta.epoch=1;
    for(const actor of houseIds.slice(0,count)){
      const tile=planFoundings(state).find(p=>p.owner===actor).capital;
      const result=applyCommand(state,meta,{id:`found-${actor}`,clientId:'founding-fixture',sequence:1,uid:meta.seats[actor].uid,actorHouseId:actor,turn:0,stateVersion:meta.stateVersion,epoch:1,type:'found',args:{tile}},{now:1000});
      assert.equal(result.ok,true,result.error);meta.stateVersion++;
    }
    assert.equal(state.turn,1);assert.equal(meta.phase,'planning');ready.set(count,{state,meta});
  }
  return structuredClone(ready.get(count));
}
