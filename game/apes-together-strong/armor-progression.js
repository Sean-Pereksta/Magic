/* Three species-fitted follower tiers and five royal sets. Health is accounted
 * once; fitting preserves wounds. The final path is committed on completion. */
(() => {
'use strict';
const G=ATSGame.prototype,E=ATSEquipment.prototype,S=ATSSiege.prototype,items=ATSOutfitItems;
const BODY=['basicArmor','reinforcedArmor','heavyArmor'],ROYAL=['age1Royal','age2Royal','commandRoyal','powerRoyal','shadowRoyal'];
const BODY_ALIASES={vine:0,leaf:0,bark:0,climbPadding:0,hardwood:1,shoulders:1,shell:1,resin:1,ironwood:2,royalHeavy:2,elite:2,warlordPlate:2,phantomWraps:2,mastercraft:2};
const ROYAL_ALIASES={wovenRoyal:'age1Royal',timberRoyal:'age1Royal'};
const canonical=id=>Object.hasOwn(BODY_ALIASES,id)?BODY[BODY_ALIASES[id]]:ROYAL_ALIASES[id]||id;
const royal=e=>items[e.g.king.outfit?.royal];
const clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
function health(a,savedBonus=a.outfit?.armorHealthBonus||0){
 if(a.id==='king'||a.state==='young')return;
 const spec=items[canonical(a.outfit?.body)],base=Math.max(1,a.maxHp-Math.max(0,savedBonus)),bonus=spec?.standardArmor?Math.round(base*spec.health):0;
 const deficit=Math.max(0,a.maxHp-a.hp);a.maxHp=base+bonus;a.hp=a.hp>0?Math.max(1,a.maxHp-deficit):0;
 if(a.outfit)a.outfit.armorHealthBonus=bonus;
}
E.armorPath=function(){return this.warlordPath||''};
Object.defineProperty(E,'armor',{configurable:true,get(){return royal(this)||null}});
const available=E.outfitAvailable;
E.outfitAvailable=function(it,station){
 const reason=available.call(this,it,station);if(reason)return reason;
 if(it.royalSet&&this.g.progression.tier<it.reign)return 'Unlocks in '+(it.reign===1?'Age 2 — King of the Jungle (100 apes).':'Warlord (200 apes).');
 if(it.path&&this.warlordPath&&this.warlordPath!==it.path)return 'Warlord path chosen: '+this.warlordPath+'. This choice lasts for the run.';
 return '';
};
const eligible=E.outfitEligible;
E.outfitEligible=function(a,it){
 if(it.royalSet){
  if(!a||a.id!=='king'||a.hp<=0||a.recoveryRemaining||a.kingdomMission)return false;
  const current=royal(this);if(this.warlordPath)return false;
  return !current||current.grade<it.grade;
 }
 if(it.standardArmor){const current=items[canonical(a?.outfit?.body)];if(current?.standardArmor&&current.grade>=it.grade)return false}
 return eligible.call(this,a,it);
};
const catalog=E.outfitCatalog;
E.outfitCatalog=function(...args){return catalog.apply(this,args).filter(it=>it.slot==='body'?it.standardArmor:it.king?it.royalSet:true).map(it=>it.king?{...it,equipped:this.g.king.outfit?.royal===it.id?1:0}:it)};
// Legacy royal jobs still finish/refund their original paid costs; no new
// legacy royal purchases can circumvent the permanent warlord choice.
const legacyCatalog=E.catalog;
E.catalog=function(...args){return legacyCatalog.apply(this,args).filter(it=>it.kind!=='armor')};
const order=E.orderBatch;
E.orderBatch=function(s,w,id,...args){return order.call(this,s,w,canonical(id),...args)};
const fit=E.fitItem;
E.fitItem=function(a,id){
 id=canonical(id);const it=items[id];if(!it||!a||a.hp<=0)return false;
 if(it.royalSet&&this.warlordPath&&it.path!==this.warlordPath)return false;
 const bonus=a.outfit?.armorHealthBonus||0,deficit=Math.max(0,a.maxHp-a.hp);
 if(it.royalSet&&a.id==='king'){if(a.equipment)delete a.equipment.armorLevel;const result=fit.call(this,a,id);if(!result)return false;a.maxHp=it.maxHp;a.hp=Math.max(1,it.maxHp-deficit);if(it.path)this.warlordPath=it.path;return true}
 const result=fit.call(this,a,id);if(result&&it.standardArmor)health(a,bonus);return result;
};
const finish=E.finish;
E.finish=function(job){if(job.kind==='armor'){this.fitItem(this.g.king,job.armorLevel>=2?'powerRoyal':'age2Royal');return}return finish.call(this,job)};
const balance=S.balance;
S.balance=function(a,force=false){
 const applied=a.outfit?.armorHealthBonus||0,deficit=Math.max(0,a.maxHp-a.hp),alive=a.hp>0,willBalance=a.id!=='king'&&a.state!=='young'&&(!a.speciesBalanceVersion||force);
 if(willBalance&&applied){const deficit=Math.max(0,a.maxHp-a.hp);a.maxHp=Math.max(1,a.maxHp-applied);a.hp=Math.max(1,a.maxHp-deficit);a.outfit.armorHealthBonus=0}
 const result=balance.call(this,a,force);if(willBalance&&items[a.outfit?.body]?.standardArmor){health(a,0);a.hp=alive?Math.max(1,a.maxHp-deficit):0}return result;
};
const training=G.applyTraining;
G.applyTraining=function(a,...args){const before=a.maxHp,result=training.call(this,a,...args);if(a.maxHp!==before&&a.outfit?.armorHealthBonus)health(a);return result};
const move=G.move;
G.move=function(a,dx,dy,speed,dt,...args){
 const kit=royal(this.equipment);let factor=1;
 if(a.id==='king')factor=kit?.speed||1;
 else if(kit?.auraSpeed&&this.king.hp>0&&a.hp>0&&this.apesById.get(a.id)===a&&!['free','idle','young'].includes(a.state)){const x=a.x-this.king.x,y=a.y-this.king.y;if(x*x+y*y<=240*240)factor=kit.auraSpeed}
 return move.call(this,a,dx,dy,speed*factor,dt,...args);
};
const command=G.command;
G.command=function(...args){const before=this.commandCD,result=command.apply(this,args);if(result&&royal(this.equipment)?.commandSpeed&&this.commandCD>0&&this.commandCD!==before)this.commandCD*=royal(this.equipment).commandSpeed;return result};
const stealth=G.stealthFactor;
G.stealthFactor=function(a){return clamp(stealth.call(this,a)*(a.id==='king'?royal(this.equipment)?.stealth||1:1),.22,2.2)};
const perceive=G.perceive;
G.perceive=function(...args){const r=perceive.apply(this,args),kit=royal(this.equipment);if(r?.seen?.id==='king'&&kit?.suspicion)r.suspicionGain=(r.suspicionGain??.32)*kit.suspicion;return r};
const save=E.serialize;
E.serialize=function(){const d=save.call(this);d.armorProgression={version:1,warlordPath:this.warlordPath||null};return d};
const load=ATSGame.fromJSON;
ATSGame.fromJSON=function(d,...args){
 const bonuses=new Map((d.apes||[]).map(a=>[a.id,a.outfit?.armorHealthBonus||0]));
 const savedKing={hp:d.king?.hp,maxHp:d.king?.maxHp},paidRoyal=(d.equipment?.queue||[]).filter(j=>j.kind==='armor'&&j.actorId==='king').map(j=>({...j}));
 const g=load.call(this,d,...args),e=g.equipment;
 e.warlordPath=['command','power','shadow'].includes(d.equipment?.armorProgression?.warlordPath)?d.equipment.armorProgression.warlordPath:null;
 for(const a of g.apes){if(a.outfit?.body)a.outfit.body=canonical(a.outfit.body);health(a,bonuses.get(a.id)||0)}
 const king=g.king,level=king.equipment?.armorLevel,id=level?(level>=2?'powerRoyal':'age2Royal'):canonical(king.outfit?.royal);
 if(items[id]?.royalSet){const deficit=Math.max(0,savedKing.maxHp-savedKing.hp),alive=savedKing.hp>0;king.outfit={...(king.outfit||{}),version:2,royal:id};if(king.equipment)delete king.equipment.armorLevel;king.maxHp=items[id].maxHp;king.hp=alive?Math.max(1,king.maxHp-deficit):0;if(items[id].path)e.warlordPath=items[id].path}
 // A migrated queue must survive a second save after its old armor-level field
 // has gone. Keep the paid recipe and timing, without reopening legacy sales.
 for(const j of paidRoyal){const spec=ATSKingArmor[j.armorLevel-1];if(!spec||!g.settlement(j.settlementId)||e.warlordPath||king.hp<=0||e.queue.some(q=>q.actorId==='king'))continue;const current=items[king.outfit?.royal];if(current?.grade>=3)continue;e.queue.push({...j,duration:spec.seconds,progress:clamp(Number(j.progress)||0,0,spec.seconds),cost:{wood:spec.wood,food:spec.food}})}
 // Old top-tier armor already migrates to the final Battle King set. Any
 // further paid legacy upgrade is redundant; return its unused reservation.
 if(e.warlordPath)for(const j of [...e.queue])if(j.kind==='armor')e.cancel(j.id);
 // Keep each in-flight recipe's price and duration for exact cancellation.
 // fitItem maps it to the new tier when its original paid work completes.
 for(const p of Object.values(e.loadouts||{}))p.items=p.items.map(canonical);
 return g;
};
window.ATSArmorProgression=Object.freeze({body:BODY,royal:ROYAL,canonical,health});
})();
