/* Current orders, derived from live selected units rather than the last button. */
(() => {
 'use strict';
 const names={charge:'Charge',follow:'Follow',hold:'Hold position',defend:'Defend',retreat:'Retreat',attack:'Attack target',movement:'Move'};
 const commandKinds={call:'follow',follow:'follow',charge:'charge',spreadCharge:'charge',nearestTarget:'charge',nearestHuman:'charge',attackNearest:'defend',recall:'retreat',recallField:'retreat',hold:'hold',shield:'defend',defend:'defend',regroup:'follow'};
 function summarize(game){
  const siege=game.siege;
  if(!siege?.selected.length)return null;
  const species=new Set(siege.selected),ids=siege.unitIds?new Set(siege.unitIds):null,counts=new Map();let total=0;
  for(const ape of game.apes){
   if(ape.hp<=0||!species.has(ape.species)||ids&&!ids.has(ape.id)||!['follow','charge','hold'].includes(ape.state))continue;
   const order=window.ATSCommandAnimation?.orderFor(ape,game);
   if(!order)continue;counts.set(order,(counts.get(order)||0)+1);total++;
  }
  if(!total)return null;
  const kind=counts.size===1?counts.keys().next().value:'mixed';
  return {kind,total,label:kind==='mixed'?'Mixed orders':names[kind]||'Ready',detail:[...counts].map(([order,count])=>count+' '+(names[order]||order).toLowerCase()).join(' · ')};
 }
 let last='';
 function update(game){
  const badge=document.getElementById('selectedCommandState');if(!badge)return;
  const state=summarize(game),key=state?state.kind+':'+state.total+':'+state.detail:'';
  if(key===last)return;last=key;badge.hidden=!state;
  if(state){badge.dataset.order=state.kind;badge.textContent=state.total+' selected · '+state.label;badge.title=state.detail;}
  for(const button of document.querySelectorAll('#commandPanel [data-command],#armyDock .army-commands [data-army-command]')){
   const kind=commandKinds[button.dataset.armyCommand||button.dataset.command];
   if(kind)button.setAttribute('aria-pressed',String(!!state&&kind===state.kind));
  }
 }
 window.ATSCommandHUD={summarize,update};
})();
