/* Human roles use readable tells and last observed positions. */
(function(){
'use strict';
const distance=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const ROLES={
 guard:{label:'Guard',hp:70,weapon:'pistol',range:230,speed:1},
 tracker:{label:'Tracker',hp:65,weapon:'rifle',range:330,speed:1.15},
 officer:{label:'Radio officer',hp:80,weapon:'pistol',range:300,speed:1},
 shield:{label:'Shield guard',hp:115,weapon:'shotgun',range:230,speed:.82},
 sniper:{label:'Marksman',hp:58,weapon:'sniper',range:510,speed:.8},
 grenadier:{label:'Grenadier',hp:85,weapon:'rifle',range:310,speed:.95},
 medic:{label:'Field medic',hp:65,weapon:'pistol',range:260,speed:1},
 flanker:{label:'Assault scout',hp:70,weapon:'assault',range:290,speed:1.22},
 gunner:{label:'Heavy gunner',hp:100,weapon:'machine',range:330,speed:.75}
};
class Forces {
 constructor(game){this.game=game;this.hazards=[]}
 assign(h,site,requested){if(h.role&&ROLES[h.role])return;const tier=Math.max(site?.tier||1,this.game.tier-1),n=ATSUtil.hash(h.id+this.game.seed)%100;let role=requested||'guard';if(!requested){if(tier<=1)role=n<20?'tracker':n<32?'officer':'guard';else if(tier===2)role=n<20?'shield':n<40?'tracker':n<55?'officer':n<70?'medic':'guard';else role=['shield','sniper','grenadier','medic','officer','flanker','gunner','tracker'][n%8]}
 const spec=ROLES[role];h.role=role;h.kind=spec.weapon;h.hp=h.maxHp=spec.hp;h.hasRadio=role==='officer'||h.hasRadio;h.specialAt=this.game.time+4+Math.random()*4;h.flankSide=ATSUtil.hash(h.id)%2?1:-1;h.label=spec.label;
 }
 range(h){return ROLES[h.role]?.range||(h.kind==='pistol'?230:300)}
 armor(h,damage,source){if(h.role!=='shield'||!source)return damage;const g=this.game,angle=Math.atan2(source.y-h.y,source.x-h.x);const frontal=Math.cos(angle-h.dir)>.45,swarm=g.apeGrid.near(h.x,h.y,52).length;if(frontal&&swarm<4){g.effect('text',h.x,h.y,{text:'BLOCK',life:.45,color:'#abc5cf'});return damage*.42}return damage}
 update(h,dt){const g=this.game;if(!h.role)this.assign(h,g.world.sites.get(h.siteId));h.hitTimer=Math.max(0,(h.hitTimer||0)-dt);if(h.hitTimer>.19){h.aiming=null;return true}
 if(h.role==='medic'&&g.time>h.specialAt&&g.time>=(h._roleThink||0)&&g.performance.think()){h._roleThink=g.time+.5;const ally=g.humanGrid.near(h.x,h.y,105).find(a=>a!==h&&a.hp>0&&a.hp<a.maxHp*.7);if(ally){ally.hp=Math.min(ally.maxHp,ally.hp+18);h.specialAt=g.time+6;h.animation={kind:'treat',start:g.time,duration:.5};g.effect('text',ally.x,ally.y,{text:'+18',color:'#92cdbb',life:1})}}
 if(h.role==='tracker'&&h.state==='patrol'&&g.time>=(h._roleThink||0)&&g.performance.think()){h._roleThink=g.time+.35;const sound=g.noiseGrid.near(h.x,h.y,1900).find(n=>distance(h,n)<n.radius*1.4);if(sound){h.state='investigate';h.lastX=sound.x;h.lastY=sound.y;h.searchTime=20}}
 if(h.state!=='combat'){h.aiming=null;return false}const target=h.targetId==='king'?g.king:g.apesById.get(h.targetId);if(!target||target.hp<=0)return false;
 if(h.role==='sniper'&&distance(h,target)>85&&distance(h,target)<520){const sight=g.lineVisible(h,target);if(sight===null)return true;if(!sight){h.aiming=null;h.state='search';h.searchTime=15;return false}h.dir=Math.atan2(target.y-h.y,target.x-h.x);h.shootTimer-=dt;
 if(!h.aiming&&h.shootTimer<=0){h.aiming={x:target.x,y:target.y,until:g.time+1.35};g.sound('warning',.45,h.x)}
 if(h.aiming&&g.time>=h.aiming.until){g.shoot(h,{x:h.aiming.x,y:h.aiming.y});h.aiming=null;h.shootTimer=2.8}h.moving=false;return true}
 if(h.role==='grenadier'&&g.time>h.specialAt&&distance(h,target)>105&&distance(h,target)<330&&g.world.lineClear(h.x,h.y,target.x,target.y)){h.specialAt=g.time+9;h.animation={kind:'throw',start:g.time,duration:.6};this.hazards.push({id:'grenade-'+g.nextId++,type:'grenade',x:target.x,y:target.y,fromX:h.x,fromY:h.y,start:g.time,fuse:1.8,radius:67,life:1.8});g.sound('grenade',.5,h.x)}
 if(h.role==='officer'&&g.time>h.specialAt&&g.exposure<.4){h.specialAt=g.time+24;this.hazards.push({id:'flare-'+g.nextId++,type:'flare',x:h.lastX,y:h.lastY,start:g.time,life:12,radius:150});g.noise(h.lastX,h.lastY,420,'flare');g.notify('A flare lights the last reported position.','red')}
 return false;
 }
 tick(dt){const g=this.game;for(const h of this.hazards){h.life-=dt;if(h.type==='grenade'&&h.life<=0){g.effect('wave',h.x,h.y,{life:.5,range:h.radius,color:'#efbd7d'});g.effect('smoke',h.x,h.y,{life:1.8,color:'#94705b'});g.sound('smash',1.6,h.x);for(const a of g.apeGrid.near(h.x,h.y,h.radius))if(g.world.lineClear(h.x,h.y,a.x,a.y))g.hurt(a,72*(1-distance(a,h)/h.radius*.45),h);for(const a of g.humanGrid.near(h.x,h.y,h.radius))if(g.world.lineClear(h.x,h.y,a.x,a.y))g.hurt(a,60,h)}}this.hazards=this.hazards.filter(h=>h.life>0)}
}
window.ATSForces=Forces;window.ATSHumanRoles=ROLES;
})();
