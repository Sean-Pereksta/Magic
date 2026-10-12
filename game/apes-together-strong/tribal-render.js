/* Bounded modular overlays on the authored character sheets. No new atlases,
 * per-ape textures, animation simulations, or visibility rays. */
(() => {
'use strict';
const P=ATSRenderer.prototype;
const line=(c,x,y,u,v,color,width=2)=>{c.beginPath();c.moveTo(x,y);c.lineTo(u,v);c.strokeStyle=color;c.lineWidth=width;c.stroke()};
const poly=(c,points,color)=>{c.beginPath();points.forEach(([x,y],i)=>i?c.lineTo(x,y):c.moveTo(x,y));c.closePath();c.fillStyle=color;c.fill()};
const draw=P.draw;P.draw=function(g,...args){this._tribalGame=g;return draw.call(this,g,...args)};
P.drawTribalOverlay=function(c,a,king=false){
 const id=a.tribeId||this._tribalGame?.tribe.id,t=ATSTribes[id],elite=a.elite,armor=king?a.equipment?.armorLevel||0:0;
 if(!t&&!elite||a.hp<=0||a._atlas)return;
 this._tribalOverlaySerial=(this._tribalOverlaySerial||0)+1;
 const pose=window.ATSCharacterArt?.resolve(a,this.time||0,{king}),rear=pose?.rear||false,flip=pose?.mirror?-1:1,cache=this.tribalOverlays||(this.tribalOverlays=new Map()),key=[id,a.species,king,armor,elite?.type,rear,this.detailLevel>=3].join(':');
 let art=cache.get(key);
 if(!art){art=document.createElement('canvas');art.width=128;art.height=128;const x=art.getContext('2d');x.translate(64,112);
  const color=t?.color||'#ddb779',dark=t?.dark||'#313f3a',detail=this.detailLevel<3;
  // Broad silhouettes make the three kingdoms readable even while moving.
  if(id==='ironback'){poly(x,[[-23,-47],[-11,-52],[-8,-35],[-24,-34]],dark);poly(x,[[9,-51],[24,-46],[25,-33],[9,-36]],dark);line(x,-23,-43,-12,-46,color,4);line(x,12,-46,24,-41,color,4)}
  else if(id==='shadowhand'){line(x,-15,-49,14,-22,dark,8);line(x,-15,-49,14,-22,color,2.5);line(x,14,-49,-12,-24,'#7d8461',2);if(detail){line(x,-18,-26,-20,-12,color,3);line(x,17,-28,22,-16,color,3)}}
  else{poly(x,[[-23,-44],[-14,-54],[-6,-44],[-11,-30],[-24,-33]],dark);poly(x,[[7,-44],[14,-54],[24,-43],[23,-31],[11,-30]],dark);line(x,-22,-41,-11,-44,color,3);line(x,10,-44,22,-40,color,3)}
  if(detail&&!rear){line(x,-8,-57,-3,-55,color,2);line(x,6,-57,11,-55,color,2)}
  if(king){
   const width=13+armor*2;poly(x,[[-width,-40],[0,-46],[width,-40],[width-3,-21],[0,-15],[-width+3,-22]],dark);
   line(x,-width,-37,width,-37,color,3);if(armor>0)for(const side of[-1,1]){line(x,side*22,-39,side*27,-23,dark,7+armor*2);line(x,side*22,-39,side*27,-23,color,2)}
   const crownY=-70;poly(x,[[-13,crownY+8],[-17,crownY-3-armor*2],[-5,crownY+2],[0,crownY-10-armor*2],[6,crownY+2],[17,crownY-3-armor*2],[13,crownY+8]],color);
  }
  if(elite){const form=Object.values(ATSEliteForms).find(f=>f.type===elite.type),accent=form?.color||'#e6d7a2';
   line(x,-18,-44,15,-20,accent,5);poly(x,[[0,-51],[6,-44],[0,-36],[-6,-44]],accent);
   if(detail){x.fillStyle=dark;x.fillRect(-1,-47,2,7);for(const side of[-1,1]){line(x,side*19,-38,side*22,-24,accent,4);line(x,side*12,-58,side*18,-55,accent,3)}}
  }
  if(cache.size>=128)cache.delete(cache.keys().next().value);cache.set(key,art);
 }
 c.save();c.translate(0,-(a.elevation||a.wallClimbHeight||0));const scale=(king?1.36:a.bodyScale||1)*(a.state==='young'?.63:1);c.scale(scale*flip,scale);
 if(a.siegeTransition||a.wallClimb)c.translate(0,-2);
 c.drawImage(art,-64,-112);c.restore();
};
const ape=P.drawApe;P.drawApe=function(c,a,king,...args){const simulationDir=a.dir;
 try{if(Number.isFinite(a.visualDir)&&!(a.attackTimer>0))a.dir=a.visualDir;ape.call(this,c,a,king,...args);this.drawTribalOverlay(c,a,king)}finally{a.dir=simulationDir}
};
const sprite=P.drawApeSprite;P.drawApeSprite=function(c,a){const before=this._tribalOverlaySerial||0,r=sprite.call(this,c,a);if(before===(this._tribalOverlaySerial||0))this.drawTribalOverlay(c,a,false);return r};
const human=P.drawHuman;P.drawHuman=function(c,a){human.call(this,c,a);if(a.eliteGuard){c.save();c.translate(0,-(a.elevation||0));poly(c,[[-12,-49],[0,-55],[12,-49],[8,-40],[-8,-40]],'#5b3434');line(c,-7,-50,7,-50,'#e5ad74',3);poly(c,[[0,-64],[4,-60],[0,-56],[-4,-60]],'#e9b96d');c.restore()}};
const prop=P.drawSettlementProp;P.drawSettlementProp=function(c,o){prop.call(this,c,o);const g=this._tribalGame,t=ATSTribes[g?.tribe.id];if(!t||o.hp<=0||(o.stage??4)<4||!['lodge','mainHut','main','greatLodge','spearTower','spearBattery','spearBallista'].includes(o.kind))return;
 line(c,-24,0,-24,-65,t.dark,3);poly(c,[[-24,-64],[3,-61],[-2,-49],[-24,-48]],t.color);line(c,-18,-58,-10,-53,t.dark,2);
 if(o.masterwork){line(c,-18,-20,-9,-50,t.color,4);line(c,18,-20,9,-50,t.color,4)}
};
const object=P.drawObject;P.drawObject=function(c,o){object.call(this,c,o);if(o.dead)return;
 if(o.eliteForm&&this._tribalGame?.siege.known(o)){const color=ATSEliteForms[o.eliteForm]?.color||'#e4c583';line(c,-23,-46,23,-46,color,4);poly(c,[[0,-62],[7,-54],[0,-46],[-7,-54]],color)}
 if(o.ironbackGatehouse){line(c,-25,0,-25,-37,'#b7615a',7);line(c,25,0,25,-37,'#b7615a',7);line(c,-25,-37,25,-37,'#4c3630',9)}
};
const siege=P.drawSiege;P.drawSiege=function(g){siege.call(this,g);const s=g.siege;if(!s)return;const c=this.ctx,z=this.camera.zoom;
 c.save();c.font='600 10px system-ui';c.textAlign='center';
 const devices=g.world.getObjects(g.king.x,g.king.y,g.viewRadius||900).filter(o=>['gateControl','prisonControl','generator','alarm','radio'].includes(o.type)&&s.known(o)).slice(0,36);
 for(const o of devices){const p=this.project(o.x,o.y,(o.height||30)+18);if(!this.visible(p,50))continue;
  const label={gateControl:'⚙ Gate Control',prisonControl:'▦ Cell Release',generator:'ϟ Power',alarm:'! Alarm',radio:'⌁ Radio'}[o.type];
  if(!s.selected.length&&o.id!==this.inspectDeviceId)continue;
  c.fillStyle='rgba(8,25,23,.9)';c.fillRect(p.x-45,p.y-11,90,27);c.fillStyle=o.dead||o.controlDisabled?'#a2a99e':'#e7d095';c.fillText(label,p.x,p.y);
  if(o.type==='gateControl'){const gate=g.world.objects.get(o.gateId),state=o.controlDisabled||o.dead?'Control Disabled':gate?.gateState==='open'?'Open':gate?.hp<gate?.maxHp?'Damaged':'Locked';c.fillText(state,p.x,p.y+11);
   if(gate&&s.known(gate)&&(this.inspectDeviceId===o.id||s.selected.some(sp=>s.drafts[sp]?.some(d=>d.targetId===o.id)))){const q=this.project(gate.x,gate.y);c.setLineDash([4,5]);line(c,p.x,p.y+15,q.x,q.y,'#daca83',1.5);c.setLineDash([]);c.strokeStyle='#daca83';c.strokeRect(q.x-27*z,q.y-12*z,54*z,24*z)}
  }
 }
 // One representative per selected species, cached until the route changes.
 const cache=this.tribalRoutePreview||(this.tribalRoutePreview=new Map());
 for(const species of s.selected){const group=s.groups[species],a=s.selectedMembers().find(a=>a.species===species),o=s.drafts[species]?.[0]||group?.orders[a?.armyOrder?.index||0];if(!a||!o)continue;
  const key=species+':'+o.id+':'+g.world.navRevision+':'+Math.floor(a.x/160)+','+Math.floor(a.y/160);let steps=a.siegeRoute?.steps;
  if(!steps){steps=cache.get(key);if(!steps){steps=s.routeFor(a,o);if(cache.size>24)cache.clear();cache.set(key,steps)}}
  let prev=this.project(a.x,a.y);for(const step of steps){if(!s.known({...step,type:'ground'}))continue;const wall=g.world.objects.get(step.wallId);if(wall&&!s.known(wall))continue;const p=this.project(step.x,step.y),color=step.blocked||group?.blocked?'#e78f77':'#91c7b0';c.setLineDash([3,6]);line(c,prev.x,prev.y,p.x,p.y,color,1.5);c.setLineDash([]);if(wall){const q=this.project(wall.x,wall.y,wall.visualHeight||60);line(c,p.x,p.y,q.x,q.y,color,1.5);poly(c,[[q.x,q.y-7],[q.x-4,q.y],[q.x+4,q.y]],color)}prev=p}
 }
 if(this.distractionPreview&&s.selected.length){const p=this.project(this.distractionPreview.x,this.distractionPreview.y),r=g.distractionRadius(s.selectedMembers()[0]);c.beginPath();c.ellipse(p.x,p.y,r*.8*z,r*.42*z,0,0,Math.PI*2);c.strokeStyle='#a1ccb0';c.setLineDash([6,5]);c.stroke();c.setLineDash([]);c.fillStyle='#d2e5d6';c.fillText('Noise area',p.x,p.y-12)}
 for(const home of g.settlements)for(const tower of home.facilities||[]){if(!['spearTower','spearBattery','spearBallista'].includes(tower.kind))continue;const a=g.apesById.get(tower.staffedBy),station=tower.station||tower,p=this.project(tower.x,tower.y,76);if(!this.visible(p,40))continue;const ready=a?.hp>0&&a.state==='settled'&&a.settlementId===home.id&&Math.hypot(a.x-station.x,a.y-station.y)<=42&&!g.blastActive(a);if(tower.hp<=0||(tower.stage??4)<4||!ready){c.fillStyle='#e4a776';c.fillText(tower.hp<=0?'Disabled':(tower.stage??4)<4?'Building':a?.hp>0?'Crew approaching':'Unstaffed',p.x,p.y)}}
 c.restore();
};
})();
