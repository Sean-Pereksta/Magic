/* Permanent civilization milestones, separate from fluctuating human threat. */
(() => {
'use strict';
const TIERS=[
 {name:'Underpowered King',threshold:0,track:'underpowered-king',crown:'The first crown'},
 {name:'King of the Jungle',threshold:100,track:'ceremonial-tom',crown:'The royal crown'},
 {name:'Warlord',threshold:300,track:'primal-roar',crown:'The war crown'}
];
const P=ATSGame.prototype;
Object.defineProperty(P,'progression',{configurable:true,get(){return this._progression||(this._progression={version:1,tier:0,acknowledged:0,peak:0})}});
P.recordReignPopulation=function(n){
 if(this.ended||this.king.hp<=0)return;
 const p=this.progression;
 p.peak=Math.max(p.peak,n);p.tier=Math.max(p.tier,n>=300?2:n>=100?1:0);
 this.king.crownTier=p.tier;
 return p;
};
P.checkProgression=function(){return this.recordReignPopulation(this.population)};
P.nextCoronation=function(){if(this.ended||this.king.hp<=0)return 0;const p=this.progression;return p.acknowledged<p.tier?p.acknowledged+1:0};
P.acknowledgeCoronation=function(tier){const p=this.progression;if(tier!==p.acknowledged+1||tier>p.tier)return false;p.acknowledged=tier;return true};
// makeApe records births/rescues immediately using its existing population count,
// so casualties in the same tick cannot erase a freshly earned milestone. The
// slower audit supports restores and retains existing finite invasion budgets.
const update=P.update;
P.update=function(dt,input){if(this.time>=(this._nextReignCheck||0)){this._nextReignCheck=this.time+.25;this.checkProgression()}return update.call(this,dt,input)};
const response=P.responsePackage;
P.responsePackage=function(site,target,regional=false,options={}){
 const result=response.call(this,site,target,regional,options),tier=this.progression.tier;
 if(tier){const scale=tier===2?1.22:1.12;result.people=Math.min(Math.floor(site.strength||0),Math.ceil(result.people*scale));if(regional)result.name=tier===2?'Warlord siege column':'Royal settlement invasion'}
 return result;
};
for(const [key,factors] of [['directorInterval',[1,.9,.82]],['reinforcementInterval',[1,.9,.84]]]){
 const getter=Object.getOwnPropertyDescriptor(P,key).get;
 Object.defineProperty(P,key,{configurable:true,get(){return Math.max(3,getter.call(this)*factors[this.progression.tier])}});
}
const save=P.serialize,restore=ATSGame.fromJSON;
P.serialize=function(){this.checkProgression();const d=save.call(this);d.progression={...this.progression};return d};
ATSGame.fromJSON=function(d,hooks){const g=restore.call(this,d,hooks),p=d.progression;
 if(p?.version===1){const tier=Math.max(0,Math.min(2,Math.floor(Number(p.tier)||0)));g._progression={version:1,tier,acknowledged:Math.max(0,Math.min(tier,Math.floor(Number(p.acknowledged)||0))),peak:Math.max(0,Math.min(1000,Number(p.peak)||0))}}
 // Old living kingdoms receive their milestone ceremonies on first resume.
 g.checkProgression();return g;
};
window.ATSReignTiers=TIERS;
})();
