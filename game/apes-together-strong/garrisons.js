/* One shared village plan every two seconds. Per-resident work only resolves
 * the cached post/target; manually recalled or ordered troops are never owned. */
(() => {
'use strict';
const C=ATSSettlements.prototype,T=ATSApeTactics.prototype;
const dist=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y),point=a=>({x:a.x,y:a.y});
C.planGarrison=function(s){
 const g=this.game;if(g.time<(s._garrisonAt||0))return;s._garrisonAt=g.time+2;
 const members=(s._adults||this.members(s)).filter(a=>a.hp>0&&a.state==='settled'&&!a.armyOrder&&!a.divisionId&&!a.kingdomMission&&!a.expeditionCargo);
 const humans=g.humanGrid.nearest(s.x,s.y,s.radius+240,32,h=>h.hp>0),vehicles=g.vehicleGrid.nearest(s.x,s.y,s.radius+240,8,v=>v.hp>0);
 const threats=humans.concat(vehicles),interior=threats.filter(h=>dist(h,s)<Math.max(80,s.radius*.55));
 const breached=(s.barriers||[]).filter(o=>o.hp<=0),gates=(s.entrances?.length?s.entrances:[s]).slice(0,12);
 const guards=members.filter(a=>a.job==='guardian'||a.towerId),counts={tower:0,gate:0,wall:0,reserve:0,emergency:0};
 const direction=threats[0]||{x:s.x+1,y:s.y};
 const walls=(s.barriers||[]).filter(o=>o.hp>0).sort((a,b)=>dist(a,direction)-dist(b,direction)),fieldCount=guards.filter(a=>!a.towerId).length;let index=0;
 for(const a of guards){
  if(a.towerId){a.garrisonPost={role:'tower',id:a.towerId};counts.tower++;continue}
  const slot=index++,fraction=slot/Math.max(1,fieldCount);
  let role=fraction<.32?'gate':fraction<.54?'wall':fraction<.82?'reserve':'emergency',post;
  if(role==='gate')post=gates[slot%gates.length];
  else if(role==='wall'){post=walls[slot%Math.max(1,Math.min(8,walls.length))]||gates[0];if(post!==s){const angle=Math.atan2(post.y-s.y,post.x-s.x);post={x:post.x-Math.cos(angle)*35,y:post.y-Math.sin(angle)*35}}}
  else if(role==='reserve')post=interior[0]||breached.find(b=>threats.some(h=>dist(h,b)<200))||threats.find(h=>dist(h,s)<s.radius+55)||s;
  else post=interior[0]||{x:s.x+(slot%3-1)*35,y:s.y+45};
  if(s.holdfast&&interior.length&&role==='wall'){role='reserve';post=interior[0]}
  const radius=role==='reserve'?150:role==='emergency'?90:95;
  const target=threats.filter(t=>dist(t,post)<radius).sort((a,b)=>dist(a,post)-dist(b,post))[0];
  a.garrisonPost={role,...point(post),targetId:target?.id,vehicle:!!target?.vehicleClass||target?.id?.startsWith('vehicle'),leash:radius};counts[role]++;
 }
 s.garrison={...counts,threats:threats.length,breaches:breached.length,phase:interior.length?'Protecting lodge':threats.length?'Defending approaches':'At posts'};
};
const tick=C.tick;C.tick=function(s,...args){const result=tick.call(this,s,...args);this.planGarrison(s);return result};
const activity=C.activityTarget;C.activityTarget=function(a,s){
 const post=a.garrisonPost;
 if(!a.towerId&&post&&post.role!=='tower'&&a.job==='guardian'&&a.state==='settled'&&!a.armyOrder&&!a.divisionId&&!a.kingdomMission){a.activity=s.attack&&post.role==='gate'?'defending entrance':'holding '+post.role+' post';return{...point(post),speed:a.speed*.8,activity:a.activity}}
 return activity.call(this,a,s);
};
const combat=T.combat;T.combat=function(a,dt,s){
 if(!s||a.state!=='settled'||a.armyOrder||a.divisionId||a.kingdomMission)return combat.call(this,a,dt,s);
 const g=this.g,post=a.garrisonPost;
 // Tower crew goes to its station even if a ground enemy is tempting it away.
 if(a.towerId){const tower=g.colonies.structure(s,a.towerId);if(tower?.hp>0){const goal=tower.station||tower;if(dist(a,goal)>8)g.move(a,goal.x-a.x,goal.y-a.y,a.speed*.85,dt);else a.moving=false;a.activity='crewing tower';return true}}
 if(!post||a.job!=='guardian')return combat.call(this,a,dt,s);
 const target=post.vehicle?g.vehicles.find(v=>v.id===post.targetId):g.humansById.get(post.targetId);
 if(target?.hp>0&&dist(target,post)<post.leash){
  if(!g.siege.attack(a,target,post.vehicle?'vehicle':'human',dt))g.move(a,target.x-a.x,target.y-a.y,a.speed,dt);
 }else if(dist(a,post)>20)g.move(a,post.x-a.x,post.y-a.y,a.speed*.8,dt);else a.moving=false;
 a.activity={gate:'guarding gate',wall:'holding perimeter',reserve:'mobile reserve',emergency:'protecting lodge'}[post.role];return true;
};
})();
