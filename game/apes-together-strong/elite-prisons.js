/* New blueprints only. Elite awards travel in the existing saved escort cohort,
 * so breaking a cell or reloading it cannot award a second individual. */
(() => {
'use strict';
const FORMS=Object.freeze({
 gorilla:{type:'ironhide',name:'Ironhide Gorilla',color:'#cf7772',specialty:'10% damage resistance; 30% stronger log shields'},
 gibbon:{type:'ghost',name:'Ghost Gibbon',color:'#a2d5ce',specialty:'20% faster climbing and 20% lower visibility'},
 chimpanzee:{type:'shadowbreaker',name:'Shadowbreaker Chimpanzee',color:'#a8c87c',specialty:'30% faster security sabotage'},
 orangutan:{type:'stoneheart',name:'Stoneheart Orangutan',color:'#e7ba70',specialty:'15% village construction support while stationed'},
 mandrill:{type:'warcry',name:'Warcry Mandrill',color:'#a7b7ef',specialty:'Nearby allies receive a lasting defensive rally'},
 capuchin:{type:'slinger',name:'Master Slinger Capuchin',color:'#dbb1e3',specialty:'30% stronger ranged attacks; wider, faster distractions'}
});
const blueprint=ATSWorld.prototype._buildMilitarySite;
ATSWorld.prototype._buildMilitarySite=function(s,r,add){
 const objects=[],put=(type,x,y,data)=>{const o=add(type,x,y,data);objects.push(o);if(['gateControl','prisonControl','generator','radio','alarm'].includes(type))o.discovered=false;return o};
 blueprint.call(this,s,r,put);
 if(!s.prison||!['blacksite','transfer'].includes(s.prison.kind))return;
 s.prison.layoutVariant=ATSUtil.hash(this.seed+'|wing|'+s.id)%3;
 s.prison.version=2;
 const cells=objects.filter(o=>o.type==='cage'),species=Object.keys(FORMS),chosen=species[ATSUtil.hash(this.seed+'|elite|'+s.id)%6];
 if(cells[0]){cells[0].captiveSpecies=chosen;cells[0].eliteForm=chosen;cells[0].discovered=false;cells[0].prisonLock='dual';cells[0].prisonReward='champion'}
 if(cells[2]&&s.prison.layoutVariant===2){cells[2].eliteForm=cells[2].captiveSpecies;cells[2].discovered=false}
 const ex=s.extentX||s.radius-74,ey=s.extentY||s.radius-74,corridor=[64,80,92][s.prison.layoutVariant],outerX=ex+corridor,outerY=ey+corridor;
 const height=s.prison.kind==='blacksite'?3:2;
 // The second ring leaves a seeded 64–92-pace service corridor with front/rear gates
 // and aligned climbing access. All control machinery is on the inside.
 const wall=(x,y,w,h)=>put('wall',x,y,{w,h,r:Math.max(w,h)/2,collision:'rect',wallTier:height,defenseRing:2,walkable:true});
 for(let x=-outerX;x<=outerX;x+=38)if(Math.abs(x)>90){wall(x,outerY,39,22);wall(x,-outerY,39,22)}
 for(const side of[-1,1]){
  const route=s.climbRoutes.find(ids=>{const w=objects.find(o=>o.id===ids[0]);return w&&Math.sign(w.x-s.x)===side*(s.layout===1?-1:1)});
  const inner=route&&objects.find(o=>o.id===route[0]),accessY=inner?inner.y-s.y:0;
  let access=null;
  for(let y=-outerY+19;y<outerY;y+=38){const o=wall(side*outerX,y,22,39);if(!access||Math.abs(y-accessY)<Math.abs(access.y-s.y-accessY))access=o}
  if(access){access.climbAccess=access.climbable=true;access.accessKind='vines';access.walkHeight=access.visualHeight||80;s.walkways.push(access.id);if(route)route.unshift(access.id);else s.climbRoutes.push([access.id]);}
 }
 for(const sign of[-1,1]){
  const gate=put('gate',0,sign*outerY,{w:164,h:24,r:82,collision:'rect',gateState:'closed',defenseRing:2,hp:1300});gate.maxHp=gate.hp=1300;s.gates.push(gate.id);
  const control=put('gateControl',-64,sign*(outerY-42),{w:24,h:24,r:13,height:30,hp:240,gateId:gate.id,discovered:false});gate.controlId=control.id;
 }
 s.extentX=outerX;s.extentY=outerY;s.radius=Math.max(s.radius,Math.hypot(outerX,outerY)+35);
 s.prison.exit={x:s.x,y:s.y+outerY+120};s.serviceEntrance={x:s.x,y:s.y-outerY-44};
 // Seeded reserve posts vary the defended side without blocking a corridor.
 s.prison.reservePosts=[{x:s.x+(s.prison.layoutVariant===1?-1:1)*(ex+corridor/2),y:s.y+ey*.45},{x:s.x,y:s.y-ey*.7}];
 s.staging=[{x:s.x-ex*.6+52,y:s.y+ey*.62},...s.prison.reservePosts];
};
const P=ATSPrisonOperations.prototype;
const unlocked=P.unlocked;P.unlocked=function(o){if(o.prisonLock==='dual'){const p=this.site(o.siteId)?.prison;return !!(p?.controlsDown&&p?.powerDown)}return unlocked.call(this,o)};
const release=P.release;P.release=function(o){const n=release.call(this,o);if(n&&o.eliteForm){const c=this.site(o.siteId)?.prison.cohorts.find(c=>c.cageId===o.id);if(c)c.eliteForm=o.eliteForm}return n};
ATSGame.prototype.grantElite=function(a,species,sourceId){
 const form=FORMS[species];if(!form||!a||a.elite)return false;
 if(a.champion)a.champion.name=form.name+' · '+a.champion.name;
 a.elite={version:1,type:form.type,name:form.name,sourceId,identity:sourceId+':'+a.id};
 // At the champion cap this still remains an elite with its specialty and art.
 if(!a.champion){this.champions.veteran(a,{name:form.name});a.maxHp+=45;a.hp=Math.min(a.maxHp,a.hp+30);a.elite.healthBonus=45}
 if(species==='gorilla'&&!a.shield)a.shield={hp:220,maxHp:220};return true;
};
const reward=P.reward;P.reward=function(s,c){
 const pending=c.eliteForm&&!c.rewardClaimed,first=this.g.apes.length;
 reward.call(this,s,c);
 if(pending&&c.rewardClaimed){const a=this.g.apes[first];if(a)this.g.grantElite(a,c.eliteForm,c.cageId)}
};
const recon=P.recon;P.recon=function(value){const result=recon.call(this,value);if(!result)return result;const s=this.site(value);
 const cells=this.objects(s).filter(o=>o.eliteForm&&this.g.siege.known(o));
 result.elites=cells.map(o=>({name:FORMS[o.eliteForm].name,claimed:!!o.prisonRewardClaimed,specialty:FORMS[o.eliteForm].specialty}));
 result.objectives=result.objectives.filter(o=>this.g.siege.known(this.g.world.objects.get(o.id))).map(o=>{const cell=this.g.world.objects.get(o.id);return cell?.eliteForm?{...o,label:FORMS[cell.eliteForm].name+' cell · '+(this.unlocked(cell)?'released':cell.prisonLock==='dual'?'requires power + cell controls':'power lock')}:o});
 if(result.elites.length)result.hint='Elite wing needs both power and cell controls disabled. Escort survivors beyond both perimeters.';return result;
};
const make=ATSGame.prototype.makeHuman;ATSGame.prototype.makeHuman=function(x,y,s,...args){const h=make.call(this,x,y,s,...args);if(s?.prison?.version===2&&ATSUtil.hash(h.id)%5===0){h.eliteGuard=true;h.maxHp*=1.2;h.hp=h.maxHp}return h};
const damage=ATSGame.prototype.apeDamage;ATSGame.prototype.apeDamage=function(a,n){const result=damage.call(this,a,n);return a.elite?.type==='slinger'&&n<=8?result*1.3:result};
const balance=ATSSiege.prototype.balance;ATSSiege.prototype.balance=function(a,force){const change=a.id!=='king'&&a.state!=='young'&&(!a.speciesBalanceVersion||force),ratio=a.hp/Math.max(1,a.maxHp);balance.call(this,a,force);if(change&&a.elite?.healthBonus){a.maxHp+=a.elite.healthBonus;a.hp=a.maxHp*ratio}};
const rally=ATSSiege.prototype.updateRally;ATSSiege.prototype.updateRally=function(a){const before=this.rallyReady;rally.call(this,a);if(a.elite?.type==='warcry'&&this.rallyReady>before)for(const p of this.g.apeGrid.nearest(a.x,a.y,210,48,q=>q.hp>0)){p.rallyUntil=Math.max(p.rallyUntil||0,this.g.time+9);p._championGuardUntil=this.g.time+9}};
window.ATSEliteForms=FORMS;
})();
