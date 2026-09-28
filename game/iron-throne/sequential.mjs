import { hash } from './world-hex.mjs';
import { alive } from './core.mjs';
import { prepareGenerals } from './generals.mjs';

export const activeHouse=s=>s.sequential?.order[s.sequential.index]||null;
export function initializeSequential(s,meta,{migrate=false}={}) {
  if(s.sequential)return syncSequential(s,meta);
  const houses=s.kingdoms.map(k=>k.id),offset=hash(s.seed||s.rng,'first-house')%houses.length;
  s.sequential={version:1,order:[...houses.slice(offset),...houses.slice(0,offset)],index:0,id:1,round:s.turn};
  if(migrate){
    // Preserve the world; retain obsolete queued plans for explicit review.
    for(const a of s.armies){
      if(a.target)a.legacyOrder={target:a.target,order:a.order};
      a.path=[];a.target=null;a.order='hold';a.structureTarget=null;
    }
    meta.ready={};meta.sequences={};meta.migratedAtRound=s.turn;
  }
  while(!alive(s,activeHouse(s))&&s.sequential.index<houses.length-1)s.sequential.index++;
  syncSequential(s,meta);prepareGenerals(s,activeHouse(s));
}
export function syncSequential(s,meta){
  meta.schema=2;meta.turnOrder=[...s.sequential.order];meta.activeHouse=activeHouse(s);meta.activationId=s.sequential.id;
}
export function validateSequential(s){
  const q=s.sequential;if(!q)return;
  if(q.version!==1||!Array.isArray(q.order)||q.order.length!==s.kingdoms.length||new Set(q.order).size!==q.order.length||q.order.some(id=>!s.kingdoms.some(k=>k.id===id))||!Number.isInteger(q.index)||q.index<0||q.index>=q.order.length||!Number.isSafeInteger(q.id)||q.id<1||q.round!==s.turn)throw new Error('Damaged sequential activation state.');
}
