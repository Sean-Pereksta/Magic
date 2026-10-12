/* Tribal identity and finite upgrades. Save version 1 remains compatible: a
 * missing tribe is a balanced legacy kingdom, never an automatic conversion. */
(() => {
'use strict';
const G=ATSGame.prototype,S=ATSSiege.prototype,C=ATSSettlements.prototype;
const LIGHT=new Set(['chimpanzee','gibbon','capuchin']);
const HEAVY=new Set(['gorilla','orangutan']);
const DEFENSE=new Set(['spearTower','spearBattery','spearBallista']);
const FORTS=new Set(['wall','gate','riotGate','humanBarricade','heavyBarricade','fieldBarricade']);
const dist=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const TRIBES=Object.freeze({
 ironback:{name:'Ironback Clan',color:'#ca5954',dark:'#382d30',symbol:'◆',style:'Warriors & conquerors',
  description:'Heavy shields, coordinated gate assaults, and a protected advancing army.',
  bonuses:'+20% heavy log shield durability · +15% heavy breach damage · gorillas resist 10% of ordinary bullets',
  drawback:'Construction is 10% slower. Heavy opening rescues are conspicuous.',
  opening:['gorilla','orangutan','gorilla','chimpanzee','orangutan','capuchin'],
  abilities:['shieldwall','battering'],upgrade:'gatehouse'},
 shadowhand:{name:'Shadowhand Tribe',color:'#81b79a',dark:'#263634',symbol:'❖',style:'Infiltrators & saboteurs',
  description:'Quiet climbers, stronger decoys, and silent security shutdowns.',
  bonuses:'+20% light climbing speed · 25% less careful-movement suspicion · +20% sabotage speed',
  drawback:'A fragile opening army. Reinforced defenses cost 20% more.',
  opening:['gibbon','capuchin','chimpanzee','gibbon','chimpanzee','capuchin'],
  abilities:['ghost','relay'],upgrade:'decoys'},
 emberwood:{name:'Emberwood Kingdom',color:'#e3b75e',dark:'#39452d',symbol:'✦',style:'Builders & defenders',
  description:'Productive villages, disciplined defensive posts, and safe automatic repairs.',
  bonuses:'+20% construction speed · +15% workshop timber · +15% defensive durability',
  drawback:'Balanced workers have less early assault power; no infiltration bonuses.',
  opening:['orangutan','chimpanzee','orangutan','capuchin','mandrill','gorilla'],
  abilities:['holdfast','engineers'],upgrade:'masterwork'}
});
const UPGRADES={gatehouse:{tribe:'ironback',label:'Ironback Gatehouse',wood:100,food:70},decoys:{tribe:'shadowhand',label:'Master Distraction',wood:50,food:70},masterwork:{tribe:'emberwood',label:'Masterwork Towers',wood:100,food:70},engineers:{tribe:'emberwood',label:'Engineer Reserve',wood:60,food:50}};
Object.defineProperty(G,'tribe',{get(){return this._tribe||(this._tribe={version:1,id:'legacy',unlocks:[],opening:0})}});
G.chooseTribe=function(id){
 if(!TRIBES[id]||this.time>0||this.apes.length||this.tribe.id!=='legacy')return false;
 this._tribe={version:1,id,unlocks:[...TRIBES[id].abilities],opening:0};return true;
};
G.tribalConstruction=function(s){
 let n=this.tribe.id==='emberwood'?1.2:this.tribe.id==='ironback'?.9:1;
 if((s?._adults||[]).some(a=>a.elite?.type==='stoneheart'&&a.hp>0&&!a.recoveryRemaining&&dist(a,s)<s.radius))n*=1.15;
 return Math.min(1.4,n);
};
G.tribalUpgrade=function(id,settlementId,targetId){
 const spec=UPGRADES[id],s=this.settlement(settlementId);
 if(!spec||this.tribe.id!==spec.tribe||!s||!this.colonies.lodgeAt(this.king)||this.colonies.lodgeAt(this.king).id!==s.id)return false;
 if(s.attack||s.wood<spec.wood||s.food<spec.food)return false;
 let target;
 if(id==='masterwork')target=(s.facilities||[]).find(f=>f.id===targetId&&DEFENSE.has(f.kind)&&f.hp>0&&f.stage===4&&!f.masterwork);
 if(id==='gatehouse')target=(s.barriers||[]).find(f=>f.id===targetId&&f.gate&&f.hp>0&&!f.ironbackGatehouse);
 if(['masterwork','gatehouse'].includes(id)&&!target)return false;
 if(id==='engineers'&&s.engineerReserve||id==='decoys'&&this.tribe.unlocks.includes(id))return false;
 s.wood-=spec.wood;s.food-=spec.food;
 if(target){const extra=Math.round(target.maxHp*(id==='masterwork'?.3:.65));target.maxHp+=extra;target.hp+=extra;target[id==='masterwork'?'masterwork':'ironbackGatehouse']=true;this.world.syncSettlementBuildings?.(s,this);}
 if(id==='engineers')s.engineerReserve=true;
 if(!this.tribe.unlocks.includes(id))this.tribe.unlocks.push(id);
 this.siege.status=spec.label+' ready';return true;
};
const make=G.makeApe;
G.makeApe=function(...args){const a=make.apply(this,args),t=TRIBES[this.tribe.id];
 if(a&&t&&(args[2]===undefined||args[2]==='free')&&this.tribe.opening<12&&dist(a,{x:0,y:0})<1300){a.species=t.opening[this.tribe.opening++%6];this.siege.balance(a,true)}
 return a;
};
function shield(g,a){if(!a.shield||!HEAVY.has(a.species))return;
 const boost=(g.tribe.id==='ironback'?.2:0)+(a.elite?.type==='ironhide'?.3:0),old=a.shield.tribalBoost||0;
 if(boost<=old)return;const extra=a.shield.maxHp/(1+old)*(boost-old);a.shield.maxHp+=extra;a.shield.hp+=extra;a.shield.tribalBoost=boost;
}
const shields=S.prepareShields;S.prepareShields=function(){const r=shields.call(this);for(const a of this.selectedMembers())shield(this.g,a);return r};
const absorb=S.absorb;S.absorb=function(a,n,source){shield(this.g,a);return absorb.call(this,a,n,source)};
const hurt=G.hurt;G.hurt=function(a,n,source){
 if(source?.type==='bullet'&&this.tribe.id==='ironback'&&a.species==='gorilla')n*=.9;
 if(a.elite?.type==='ironhide')n*=.9;
 // A shieldwall only protects an ally behind an intact, correctly facing log.
 if(source?.type==='bullet'&&a.species&&this.tribe.id==='ironback'){
  const front=this.apeGrid.nearest(a.x,a.y,65,6,p=>p!==a&&p.shield?.hp>0&&p.ironbackShieldwall&&p.armyOrder);
  const dx=source.x-a.x,dy=source.y-a.y,length=Math.hypot(dx,dy)||1;
  const guard=front.find(p=>{const x=p.x-a.x,y=p.y-a.y;return (x*dx+y*dy)>0&&Math.abs(x*dy-y*dx)/length<24&&Math.cos(Math.atan2(source.y-p.y,source.x-p.x)-(p.dir||0))>.5});
  if(guard){const blocked=Math.min(n*.55,guard.shield.hp);guard.shield.hp-=blocked;n-=blocked}
 }
 return hurt.call(this,a,n,source);
};
G.batteringAssault=function(){
 if(this.tribe.id!=='ironback'||this.time<(this.tribe.batteringReady||0))return false;
 const apes=this.siege.selectedMembers().filter(a=>a.species==='gorilla');if(apes.length<2){this.siege.status='Select at least two gorillas';return false}
 for(const a of apes)a.batteringUntil=this.time+8;this.tribe.batteringReady=this.time+30;this.siege.status='Battering Assault · 8 seconds';return true;
};
G.ironbackShieldwall=function(point){if(this.tribe.id!=='ironback')return false;const ok=this.siege.immediate('shield',point||this.siege.center());if(ok)for(const a of this.siege.selectedMembers())a.ironbackShieldwall=true;return ok};
const clear=S.clearActor;S.clearActor=function(a){delete a.ironbackShieldwall;delete a.infiltration;delete a.routeStatus;return clear.call(this,a)};
const damage=G.damageObject;G.damageObject=function(o,n,a){
 if(this.tribe.id==='shadowhand'&&a?.species&&['gateControl','prisonControl','generator','radio','alarm'].includes(o.type))n*=1.2;
 if(a?.elite?.type==='shadowbreaker'&&['gateControl','prisonControl','generator','radio','alarm'].includes(o.type))n*=1.3;
 if(this.tribe.id==='ironback'&&HEAVY.has(a?.species)&&FORTS.has(o.type))n*=1.15;
 if(o.type==='gate'&&a?.species==='gorilla'&&a.batteringUntil>this.time&&this.apeGrid.nearest(a.x,a.y,90,2,p=>p!==a&&p.species==='gorilla'&&p.batteringUntil>this.time).length)n*=1.35;
 return damage.call(this,o,n,a);
};
const catalog=C.catalog;C.catalog=function(s){const rows=catalog.call(this,s);if(this.game.tribe.id!=='shadowhand')return rows;
 return rows.map(r=>{if(!['spearBattery','spearBallista','royalExpansion','warlordExpansion'].includes(r.kind))return r;const wood=Math.ceil(r.cost.wood*1.2),food=Math.ceil(r.cost.food*1.2);return{...r,wood,food,cost:{wood,food},available:r.available&&s.wood>=wood&&s.food>=food,reason:s.wood<wood||s.food<food?'Shadowhand reinforced defenses cost 20% more.':r.reason}});
};
// Commission reserves the original catalog cost internally; charge the surcharge
// only after a successful reservation, never on a failed or duplicate request.
const commission=C.commission;C.commission=function(id,kind){const s=this.game.settlement(id),quote=s&&this.catalog(s).find(r=>r.kind===kind),before=s&&{wood:s.wood,food:s.food},r=commission.call(this,id,kind);
 if(r?.ok&&quote&&this.game.tribe.id==='shadowhand'){const extraWood=Math.max(0,quote.cost.wood-(before.wood-s.wood)),extraFood=Math.max(0,quote.cost.food-(before.food-s.food));s.wood-=extraWood;s.food-=extraFood;const p=s.projects.find(p=>p.id===r.projectId);if(p?.paidCost){p.paidCost.wood+=extraWood;p.paidCost.food+=extraFood;const record=s.commissions.find(c=>c.projectId===p.id);if(record)record.cost={...p.paidCost}}}
 return r;
};
const tick=C.tick;C.tick=function(s,...args){const result=tick.call(this,s,...args),g=this.game;
 if(g.tribe.id==='emberwood')for(const o of [...(s.barriers||[]),...(s.facilities||[]).filter(f=>DEFENSE.has(f.kind))])if(!o.emberwoodDurability){const extra=Math.round(o.maxHp*.15);o.maxHp+=extra;if(o.hp>0)o.hp+=extra;o.emberwoodDurability=true}
 if(s.engineerReserve&&!s.attack&&g.time>=(s.nextEngineerRepair||0)){
  s.nextEngineerRepair=g.time+4;const damaged=[...(s.barriers||[]),...(s.facilities||[])].find(o=>o.hp>0&&o.hp<o.maxHp);
  const builders=(s._adults||[]).filter(a=>a.hp>0&&a.job==='builder'&&!a.kingdomMission&&!a.towerId);
  if(damaged&&s.wood>=2&&!g.humanGrid.nearest(s.x,s.y,s.radius+180,1).length&&!g.vehicleGrid.nearest(s.x,s.y,s.radius+180,1).length){
   for(const a of builders.slice(0,3))a.engineerRepairId=damaged.id;
   if(builders.some(a=>dist(a,damaged)<65)){s.wood-=2;damaged.hp=Math.min(damaged.maxHp,damaged.hp+30);g.world.syncSettlementBuildings?.(s,g)}
  }
 }
 return result;
};
const activity=C.activityTarget;C.activityTarget=function(a,s){if(a.engineerRepairId&&s.engineerReserve&&!s.attack&&a.job==='builder'){const target=(s.barriers||[]).concat(s.facilities||[]).find(o=>o.id===a.engineerRepairId&&o.hp>0&&o.hp<o.maxHp);if(target){a.activity='repairing defenses';return {...target,speed:a.speed*.8}}delete a.engineerRepairId}return activity.call(this,a,s)};
const transition=S.transition;S.transition=function(a,dt){return transition.call(this,a,dt*(this.g.tribe.id==='shadowhand'&&LIGHT.has(a.species)?1.2:1)*(a.elite?.type==='ghost'?1.2:1))};
const climb=ATSApeTactics.prototype.updateClimb;ATSApeTactics.prototype.updateClimb=function(a,dt){return climb.call(this,a,dt*(this.g.tribe.id==='shadowhand'&&LIGHT.has(a.species)?1.2:1)*(a.elite?.type==='ghost'?1.2:1))};
const save=G.serialize,restore=ATSGame.fromJSON;
G.serialize=function(){const d=save.call(this);d.tribe={...this.tribe,unlocks:[...this.tribe.unlocks]};return d};
ATSGame.fromJSON=function(d,hooks){const g=restore.call(this,d,hooks),t=d.tribe;
 if(t?.version===1&&TRIBES[t.id])g._tribe={version:1,id:t.id,unlocks:[...new Set([...TRIBES[t.id].abilities,...(Array.isArray(t.unlocks)?t.unlocks:[]).filter(k=>UPGRADES[k]?.tribe===t.id)])],opening:Math.max(0,Math.min(12,Number(t.opening)||0)),batteringReady:Math.max(0,Math.min(g.time+30,Number(t.batteringReady)||0))};
 return g;
};
window.ATSTribes=TRIBES;window.ATSTribalUpgrades=UPGRADES;
})();
