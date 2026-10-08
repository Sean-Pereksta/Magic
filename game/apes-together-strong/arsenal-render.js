/* Canvas-native weapon silhouettes and readable, word-free attack warnings. */
(() => {
'use strict';
const P=ATSRenderer.prototype,object=P.drawObject,vehicle=P.drawVehicle,human=P.drawHuman,humanSprite=P.drawHumanSprite,threats=P.drawThreats;
const line=(c,x,y,u,v,color,width=1)=>{c.beginPath();c.moveTo(x,y);c.lineTo(u,v);c.strokeStyle=color;c.lineWidth=width;c.lineCap='round';c.stroke()},ellipse=(c,x,y,w,h,color)=>{c.beginPath();c.ellipse(x,y,w,h,0,0,Math.PI*2);c.fillStyle=color;c.fill()},poly=(c,points,color,stroke)=>{c.beginPath();points.forEach((p,i)=>i?c.lineTo(...p):c.moveTo(...p));c.closePath();c.fillStyle=color;c.fill();if(stroke){c.strokeStyle=stroke;c.lineWidth=1;c.stroke()}},dir=(a,length)=>({x:(Math.cos(a)-Math.sin(a))*.8*length,y:(Math.cos(a)+Math.sin(a))*.42*length});
P.drawRotary=function(c,x,y,angle,length,phase,hot=false){
 const d=dir(angle,length),nx=-Math.sin(angle)*4,ny=Math.cos(angle)*2;
 line(c,x,y,x+d.x,y+d.y,'#172e32',13);for(let i=0;i<5;i++){const a=phase+i*Math.PI*2/5,ox=Math.cos(a)*4,oy=Math.sin(a)*3;line(c,x+ox,y+oy,x+d.x+ox,y+d.y+oy,hot?'#b99e73':i%2?'#afbab3':'#687d7d',2)}
 for(const t of[.25,.75,1])line(c,x+d.x*t+nx,y+d.y*t+ny,x+d.x*t-nx,y+d.y*t-ny,'#354d51',5);
 ellipse(c,x+d.x,y+d.y,5,4,'#24363c');for(let i=0;i<5;i++){const a=phase+i*Math.PI*2/5;ellipse(c,x+d.x+Math.cos(a)*3,y+d.y+Math.sin(a)*2,1,1,hot?'#efbe78':'#81908a')}
};
P.drawObject=function(c,o){
 if(o.dead||!['gatlingNest','mortarNest','powerRelay'].includes(o.type))return object.call(this,c,o);
 ellipse(c,0,4,38,15,'rgba(1,12,15,.4)');
 if(o.type==='powerRelay'){
  poly(c,[[-20,-8],[-20,-43],[4,-50],[23,-37],[23,-1],[0,9]],'#405957','#8d9b8c');poly(c,[[-20,-43],[4,-50],[23,-37],[0,-29]],'#899b8b');poly(c,[[0,-29],[23,-37],[23,-1],[0,9]],'#2d4245');
  for(let i=0;i<4;i++)line(c,-15,-32+i*5,-4,-29+i*5,'#1c3033',2);line(c,11,-34,11,-66,'#718b87',3);ellipse(c,11,-67,4,3,'#d7b671');
  poly(c,[[-10,-21],[-14,-10],[-8,-10],[-11,-2],[-2,-15],[-8,-15]],'#e5c277');ellipse(c,8,-21,3,3,'#9ec8ac');line(c,17,-30,17,-12,'#ceb57c',2);
 }else{
  // A low concrete plinth leaves the actual muzzle above the parapet.
  poly(c,[[-34,-7],[-31,-22],[1,-33],[35,-17],[32,1],[0,13]],'#556563','#92a093');poly(c,[[-34,-7],[0,9],[32,-2],[32,6],[0,18],[-34,1]],'#384f4d');
  for(let i=0;i<5;i++){const x=-26+i*13;line(c,x,-5+Math.abs(x)*.08,x+6,-2+Math.abs(x)*.08,'#b1ab8b',2)}
  const powered=o.powered!==false,phase=this.reducedMotion?0:this.time*(o.firePlan?24:2);
  line(c,0,-10,0,-32,'#344d50',10);ellipse(c,0,-33,17,9,'#8d9c8c');
  if(o.type==='gatlingNest'){
   poly(c,[[-14,-37],[-12,-50],[12,-45],[16,-29],[-11,-26]],'#607d77','#a9b6a0');this.drawRotary(c,0,-39,o.dir??o.mountDir,43,phase,o.gunFlashUntil>this.time);line(c,-15,-34,-24,-26,'#c0ae76',5);line(c,-24,-26,-20,-17,'#756746',5);
   poly(c,[[-30,-21],[-30,-39],[-17,-34],[-16,-18]],'#334d4f','#809485');ellipse(c,-25,-34,2,2,powered?'#d1b66f':'#596361');
  }else{for(const x of[-11,9]){line(c,x,-28,x-7,-57,'#293e43',13);line(c,x-2,-29,x-9,-57,'#9ea38c',3);ellipse(c,x-7,-58,7,4,'#1e3033')}line(c,-20,-16,18,-21,'#afa889',3)}
  if(!powered){line(c,-9,-37,9,-24,'#cb8a66',3);line(c,9,-37,-9,-24,'#cb8a66',3)}
 }
 if(o.hp<o.maxHp)this.health(c,o.hp/o.maxHp,-79,33,'#e1b582');
};
P.drawVehicle=function(c,v){
 const spec=ATSVehicleVariants[v.variant];if(!spec?.fireMode)return vehicle.call(this,c,v);
 const flip=Math.cos(v.dir||0)<0?-1:1,angle=v.turretDir??v.dir??0,moving=v.moving&&!this.reducedMotion;
 this.arsenalHulls=this.arsenalHulls||new Map();let hull=this.arsenalHulls.get(v.variant);
 if(!hull){hull=document.createElement('canvas');hull.width=240;hull.height=160;const sc=hull.getContext('2d');sc.translate(120,115);this.drawMilitaryHull(sc,'tank');poly(sc,[[-39,-41],[-8,-60],[54,-27],[27,-11]],spec.paint,'#b3bda9');for(let i=0;i<5;i++)line(sc,-23+i*11,-44+i*5,-12+i*11,-39+i*5,'#314849',2);this.arsenalHulls.set(v.variant,hull)}
 c.save();c.scale(flip,1);c.drawImage(hull,-120,-115);
 if(moving)for(let i=0;i<8;i++){const x=-67+(i*17+this.time*42)%132;line(c,x,4+x*.15,x+4,11+x*.15,'#a1a58a',1.7)}
 if(v.engineDamage>35)for(let i=0;i<3;i++){const t=this.reducedMotion?.5:(this.time*.7+i/3)%1;ellipse(c,-54-t*14,-29-t*38,6+t*12,4+t*12,`rgba(82,91,85,${(1-t)*.4})`)}
 c.restore();ellipse(c,0,-54,30,17,'#313f47');poly(c,[[-28,-53],[-25,-70],[3,-78],[29,-60],[21,-45],[-6,-41]],spec.paint,'#a8b5ac');line(c,-24,-69,3,-77,'#c3c7ad',2);
 if(spec.fireMode==='burst'){
  const d=dir(angle,78);for(const side of[-1,1]){const ox=-Math.sin(angle)*side*7,oy=Math.cos(angle)*side*4,recoil=v.cannonFlash>0?7:0;line(c,ox,-62+oy,d.x+ox-Math.cos(angle)*recoil,-62+d.y+oy,'#253c42',10);line(c,ox-1,-65+oy,d.x+ox-1,-65+d.y+oy,'#aebdb0',3);ellipse(c,d.x+ox,-62+d.y+oy,5,3,'#182c33')}ellipse(c,0,-76,9,5,'#9caa93');
 }else if(spec.fireMode==='mortar'){
  for(const x of[-16,1,18]){line(c,x,-54,x-17,-93,'#2b4044',15);line(c,x-3,-55,x-20,-91,'#bebc97',3);ellipse(c,x-17,-94,8,5,'#182d34');ellipse(c,x-17,-94,5,3,'#3f4e4d')}poly(c,[[-33,-51],[-36,-68],[-26,-72],[-17,-53]],'#897e5e');
 }else this.drawRotary(c,1,-64,angle,73,this.reducedMotion?0:this.time*(v.firePlan?30:1),v.gunFlashUntil>this.time);
 ellipse(c,18,-66,3,2,spec.fireMode==='mortar'?'#efc080':'#a8d9cd');if(v.weaponDamage>50){line(c,-16,-62,10,-50,'#202c30',4);line(c,10,-50,19,-56,'#d1a16e',2)}
 if(v.hp>0&&v.hp<v.maxHp)this.health(c,v.hp/v.maxHp,-110,70,'#d6b585');
};
P.drawHuman=function(c,h){
 human.call(this,c,h);if(!['breacher','spotter','bombardier','rotary'].includes(h.role))return;
 c.save();c.translate(0,-(h.elevation||0));c.scale(Math.cos(h.dir||0)<0?-1:1,1);
 if(h.role==='breacher'){poly(c,[[-11,-35],[8,-35],[11,-18],[-8,-15]],'#6c7471','#a7b1a0');line(c,-7,-42,8,-41,'#deb579',3);line(c,4,-25,27,-23,'#2b3d40',7);line(c,19,-26,27,-25,'#abb7a0',2)}
 if(h.role==='spotter'){line(c,-13,-28,-13,-64,'#a4b8ac',1.4);ellipse(c,-13,-64,2,2,'#d6b46e');poly(c,[[-15,-33],[-9,-36],[-7,-19],[-16,-18]],'#547574');for(const x of[0,7])ellipse(c,x,-41,3,2,'#8fbac3');line(c,-4,-39,11,-39,'#374c51',2)}
 if(h.role==='bombardier'){for(let i=0;i<4;i++){ellipse(c,-8+i*5,-29+i*2,2.5,4,'#c4a976');line(c,-8+i*5,-32+i*2,-8+i*5,-29+i*2,'#e5d3a2',1)}poly(c,[[-16,-33],[-10,-35],[-10,-16],[-17,-18]],'#8a8065')}
 if(h.role==='rotary'){poly(c,[[-15,-35],[-7,-37],[-5,-16],[-17,-18]],'#596c74','#a0b2a8');line(c,-8,-27,5,-19,'#c3ab78',5);line(c,6,-22,33,-21,'#243940',9);for(let i=0;i<3;i++)line(c,8,-25+i*3,35,-24+i*3,'#aab5a4',1.5);ellipse(c,35,-21,3,5,h.gunFlashUntil>this.time?'#e9c283':'#2f4148');line(c,-8,-44,8,-44,'#a7bdc1',3)}
 c.restore();
};
P.drawHumanSprite=function(c,h){if(h.firePlan)return this.drawHuman(c,h);return humanSprite.call(this,c,h)};
P.drawThreats=function(g){
 threats?.call(this,g);const c=this.ctx,owners=[...(g.forces.nearbyGuns||[]),...g.vehicles,...g.humans.filter(h=>h.firePlan)];
 for(const o of owners){if(o.dead||o.hp<=0||!g.siege.known(o))continue;const plan=o.firePlan;if(plan?.mode!=='rotary')continue;const origin=this.project(o.x,o.y),range=Math.min(distance(o,plan)+35,650),angle=plan.angle,points=[origin,...[-.12,.12].map(d=>this.project(o.x+Math.cos(angle+d)*range,o.y+Math.sin(angle+d)*range))];if(!points.some(p=>this.visible(p,100)))continue;
  c.save();const warming=this.time<plan.release;c.globalAlpha=warming?.16:.09;poly(c,points.map(p=>[p.x,p.y]),warming?'#f1c176':'#f08154');c.globalAlpha=.55;const aim=this.project(plan.x,plan.y);c.setLineDash(warming?[4,5]:[]);line(c,origin.x,origin.y,aim.x,aim.y,warming?'#e7bf80':'#db896b',1);c.setLineDash([]);c.restore();
 }
};
function distance(a,b){return Math.hypot(a.x-b.x,a.y-b.y)}
})();
