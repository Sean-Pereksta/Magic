/* Additional security layouts for high-security prison compounds. */
(() => {
'use strict';
const blueprint=ATSWorld.prototype._buildMilitarySite;
ATSWorld.prototype._buildMilitarySite=function(s,r,add){
 const objects=[],put=(type,x,y,data)=>{const o=add(type,x,y,data);objects.push(o);if(['gateControl','prisonControl','generator','radio','alarm'].includes(type))o.discovered=false;return o};
 blueprint.call(this,s,r,put);
 if(!s.prison||!['blacksite','transfer'].includes(s.prison.kind))return;
 s.prison.layoutVariant=ATSUtil.hash(this.seed+'|wing|'+s.id)%3;
 s.prison.version=2;
 const cells=objects.filter(o=>o.type==='cage');
 if(cells[0]){cells[0].discovered=false;cells[0].prisonLock='dual'}
 if(cells[2]&&s.prison.layoutVariant===2)cells[2].discovered=false;
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
const recon=P.recon;P.recon=function(value){const result=recon.call(this,value);if(!result)return result;const s=this.site(value);
 result.objectives=result.objectives.filter(o=>this.g.siege.known(this.g.world.objects.get(o.id))).map(o=>{const cell=this.g.world.objects.get(o.id);return cell?.prisonLock==='dual'&&!this.unlocked(cell)?{...o,label:o.label+' · requires power + cell controls'}:o});
 if(s.prison.version===2&&!s.prison.powerDown)result.hint='The secure wing needs both power and cell controls disabled. Lead survivors beyond both perimeters.';return result;
};
const make=ATSGame.prototype.makeHuman;ATSGame.prototype.makeHuman=function(x,y,s,...args){const h=make.call(this,x,y,s,...args);if(s?.prison?.version===2&&ATSUtil.hash(h.id)%5===0){h.eliteGuard=true;h.maxHp*=1.2;h.hp=h.maxHp}return h};
// Kept as an empty compatibility export for legacy cage art in old saves.
window.ATSEliteForms=Object.freeze({});
})();
