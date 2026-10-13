/* Temporary, bounded survival equipment. Purchases belong to the current run. */
(function(root){
'use strict';
function create(host,catalog){
  let devices=[],charges={},cooldowns={},selected='shockFence',serial=0;
  const find=id=>catalog.find(x=>x.id===id);
  function reset(){devices=[];charges={};cooldowns={};selected='shockFence';}
  function buy(id){const item=find(id);if(!item||!host.unlocked(item.unlockWave)||host.scrap()<item.cost)return false;const limit=item.maxCharges||3;if((charges[id]||0)>=limit)return false;host.spend(item.cost);charges[id]=(charges[id]||0)+(item.charges||1);selected=id;return true;}
  function deploy(actor,id=selected){const item=find(id);if(!item||actor.dead||!(charges[id]>0)||(cooldowns[id]||0)>0)return false;if(devices.filter(d=>d.id===id).length>=(item.maxActive||1))return false;
    const x=actor.x+Math.sin(actor.yaw)*3.4,z=actor.z+Math.cos(actor.yaw)*3.4,y=host.ground(x,z);
    if(!Number.isFinite(y)||y<-10||!host.clear(actor.x,actor.y-.5,actor.z,x,y+.5,z)){host.notice('CLEAR GROUND REQUIRED');return false;}
    charges[id]--;cooldowns[id]=item.cooldown||15;devices.push({id,serial:++serial,x,y,z,r:item.radius||6,hp:item.hp||100,life:item.duration||15,tick:0,owner:actor,item,dead:false,human:id==='decoy',name:'HOLOGRAM',vy:0,vx:0,vz:0,armor:0,maxHp:item.hp||100});host.notice(item.name+' DEPLOYED');return true;
  }
  function nearby(d){return host.aliens().filter(a=>Math.hypot(a.x-d.x,a.z-d.z)<d.r&&Math.abs(a.y-d.y)<12&&host.clear(d.x,d.y+1,d.z,a.x,a.y-.7,a.z));}
  function update(dt){for(const id of Object.keys(cooldowns))cooldowns[id]=Math.max(0,cooldowns[id]-dt);
    for(const d of devices){d.life-=dt;d.tick-=dt;if(d.dead||d.life<=0)continue;
      if(d.id==='decoy'){d.y=host.ground(d.x,d.z)+host.eyeHeight;continue;}
      if(d.id==='shieldBeacon')continue;
      if(d.tick>0)continue;d.tick=d.id==='sentry'?(d.item.fireInterval||.42):.5;const enemies=nearby(d);
      if(d.id==='shockFence'){for(const a of enemies){if(a.boss||(a.alienScale||1)>2.3){d.hp-=18;continue;}if(a.flying)continue;a.slowAmount=.55;a.slowTimer=Math.max(a.slowTimer||0,.75);host.damage(a,d.item.damage||7,d.owner);host.line(d.x,d.y+1,d.z,a.x,a.y-.7,a.z,'#78faff');}}
      if(d.id==='sentry'){const target=enemies.sort((a,b)=>Math.hypot(a.x-d.x,a.z-d.z)-Math.hypot(b.x-d.x,b.z-d.z))[0];if(target){host.damage(target,d.item.damage||12,d.owner);host.line(d.x,d.y+1.2,d.z,target.x,target.y-.7,target.z,'#ffdf86');}}
      if(d.id==='sentry'){for(const a of enemies)if(Math.hypot(a.x-d.x,a.z-d.z)<(a.meleeRange||2)+1)d.hp-=a.alienDamage*.4;}
      if(d.id==='empMine'&&enemies.length){for(const a of enemies){a.empUntil=host.time()+(d.item.disruptDuration||5);a.fireCd=Math.max(a.fireCd||0,3);a.chargeTimer=0;a.shieldFlash=.5;a.stagger=Math.max(a.stagger||0,a.boss?.3:1.2);host.damage(a,d.item.damage||20,d.owner);}host.burst(d.x,d.y+.5,d.z,'#9b9fff',30,.6);d.life=0;}
    }
    devices=devices.filter(d=>d.life>0&&!d.dead&&d.hp>0);
  }
  function absorb(victim,amount){for(const d of devices){if(d.id!=='shieldBeacon'||d.hp<=0||Math.hypot(victim.x-d.x,victim.z-d.z)>d.r||!host.clear(d.x,d.y+1,d.z,victim.x,victim.y-.7,victim.z))continue;const block=Math.min(d.hp,amount*.6);d.hp-=block;amount-=block;}return amount;}
  return {reset,buy,deploy,update,absorb,select(id){if(find(id))selected=id;},get selected(){return selected;},get devices(){return devices;},decoys:()=>devices.filter(d=>d.id==='decoy'&&!d.dead&&d.hp>0),state:()=>({selected,charges:{...charges},cooldowns:{...cooldowns},active:devices.length})};
}
const api={create};if(typeof module!=='undefined')module.exports=api;root.NeonSurvivalEquipment=api;
})(typeof globalThis!=='undefined'?globalThis:this);
