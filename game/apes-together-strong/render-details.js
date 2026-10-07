/* Additional illustrated poses, readable force equipment and landscape details. */
(function(){
'use strict';
const P=ATSRenderer.prototype,baseApe=P.drawApe,baseHuman=P.drawHuman,baseSettlement=P.drawSettlement,baseGround=P.drawGround,baseObject=P.drawObject;
const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
function ellipse(c,x,y,rx,ry,color,rotation=0){c.beginPath();c.ellipse(x,y,rx,ry,rotation,0,Math.PI*2);c.fillStyle=color;c.fill()}
function line(c,x,y,u,v,color,width=2){c.beginPath();c.moveTo(x,y);c.lineTo(u,v);c.strokeStyle=color;c.lineWidth=width;c.lineCap='round';c.stroke()}
function poly(c,points,color){c.beginPath();points.forEach(([x,y],i)=>i?c.lineTo(x,y):c.moveTo(x,y));c.closePath();c.fillStyle=color;c.fill()}
P.drawObject=function(c,o){
 if(o.type!=='tree'||o.dead||!['wetland','rocky','farmland'].includes(o.biome))return baseObject.call(this,c,o);
 const variant=(o.variant||0)%3,key=o.biome+variant;this.regionTrees=this.regionTrees||new Map();let sprite=this.regionTrees.get(key);
 if(!sprite){
   sprite=document.createElement('canvas');sprite.width=166;sprite.height=205;
   const t=sprite.getContext('2d');t.translate(83,185);
   ellipse(t,0,0,38,11,'rgba(0,9,12,.3)');
   poly(t,[[-7,0],[-5,-85],[4,-85],[9,1]],'#42564b');line(t,1,-4,0,-76,'#819075',2);
   if(o.biome==='rocky'){
     for(let i=0;i<4;i++){const y=-145+i*25,width=24+i*10+variant*2;poly(t,[[0,y-34],[-width,y+23],[-width*.5,y+17],[-width*.8,y+33],[0,y+25],[width,y+32],[width*.65,y+14],[width,y+19]],i%2?'#294d47':'#355d50');line(t,-width*.45,y+7,0,y-21,'rgba(139,166,131,.25)',2)}
   }else if(o.biome==='wetland'){
     for(let i=-3;i<=3;i++){const x=i*16,y=-102+Math.abs(i)*9;ellipse(t,x,y,27,29,i%2?'#385e50':'#2b514a');t.beginPath();t.moveTo(x-17,y+4);t.quadraticCurveTo(x-28,y+38,x-20,y+70+variant*4);t.strokeStyle='#4f7460';t.lineWidth=3;t.stroke();line(t,x+9,y+7,x+13,y+61,'#3a6556',3)}
   }else{
     for(let i=0;i<6;i++){const a=i*Math.PI/3,x=Math.cos(a)*30,y=-90+Math.sin(a)*24;ellipse(t,x,y,33,28,i%2?'#506748':'#3d5c43');for(let j=0;j<3;j++)ellipse(t,x-12+j*13,y+2-(j%2)*14,3,3,variant===1?'#b69360':'#8e9f69')}
   }
   this.regionTrees.set(key,sprite);
 }
 const size=clamp((o.r||27)/30,.58,1.22);c.drawImage(sprite,-83*size,-185*size,166*size,205*size);
};
P.drawApe=function(c,a,king,exposure){if(king&&a.hp<=0){const fall=this.reducedMotion?1:clamp((a.deathAge||0)/.8,0,1),ease=1-(1-fall)**3;c.save();c.translate(9*ease,0);c.rotate(ease*1.48);c.scale(1,1-ease*.18);baseApe.call(this,c,{...a,hp:1,maxHp:1,moving:false,attackTimer:0},true,0);c.restore();return}const anim=a.animation,t=anim?clamp((this.time-anim.start)/anim.duration,0,1):1,strike=t<1?Math.sin(Math.PI*clamp((t-.18)/.65,0,1)):0,kind=anim?.kind;
 c.save();const facing=Math.cos(a.dir||0)<0?-1:1,size=king?1:(a.bodyScale||1);c.scale(size,size);
 if(!this.reducedMotion){if(kind==='tackle'){c.translate(facing*strike*10,-strike*5);c.rotate(facing*strike*.23)}else if(kind==='slam'||kind==='overhead'){c.translate(0,-Math.sin(t*Math.PI)*6);c.scale(1+strike*.06,1-strike*.05)}else if(kind==='uppercut'){c.translate(0,-strike*9);c.rotate(-facing*strike*.13)}else if(kind==='backhand')c.rotate(-facing*strike*.16);else if(kind==='hook')c.rotate(facing*strike*.13);if(a.hitTimer>0)c.translate(-facing*Math.sin(a.hitTimer*24)*3,2)}
 baseApe.call(this,c,a,king,exposure);
 if(strike>.1&&a.hp>0){c.globalAlpha=strike*.55;const gold=king?'#e0c780':'#9aafa0';c.strokeStyle=gold;c.lineWidth=king?3:2;c.beginPath();if(kind==='slam'||kind==='overhead'){c.ellipse(7* facing,-1,14+strike*17,5+strike*5,0,0,Math.PI*2)}else{c.arc(facing*7,-29,21+strike*12,kind==='uppercut'?.4:-1.3,kind==='backhand'?2.9:1.2)}c.stroke()}
 if(a.carrying){ellipse(c,-12,-27,7,5,'#937a50');ellipse(c,-14,-31,3,3,'#a6b369');ellipse(c,-8,-30,3,3,'#c2a264')}c.restore();
};
P.drawHuman=function(c,h){const anim=h.animation,t=anim?clamp((this.time-anim.start)/anim.duration,0,1):1,strike=t<1?Math.sin(t*Math.PI):0,flip=Math.cos(h.dir||0)<0?-1:1;c.save();if(!this.reducedMotion){if(anim?.kind==='recoil')c.translate(-flip*strike*3,0);if(anim?.kind==='throw')c.rotate(flip*strike*.18);if(h.hitTimer>0)c.rotate(-flip*Math.sin(h.hitTimer*14)*.16)}baseHuman.call(this,c,h);c.scale(flip,1);
 if(h.role==='shield'){poly(c,[[7,-38],[24,-35],[26,-8],[17,-2],[5,-8]],'#586e72');poly(c,[[9,-35],[21,-32],[20,-23],[8,-25]],'#b3c0b2');line(c,16,-23,16,-8,'#91a7a0',2)}
 else if(h.role==='sniper'){poly(c,[[-13,-31],[-8,-48],[7,-50],[15,-30],[12,-14],[-12,-15]],'rgba(93,111,78,.42)');line(c,23,-27,51,-27,'#8fa49c',2);line(c,20,-32,31,-32,'#4a6055',4)}
 else if(h.role==='medic'){c.fillStyle='#c1cdbb';c.fillRect(-17,-30,9,15);line(c,-15,-23,-10,-23,'#ba6660',2);line(c,-12.5,-26,-12.5,-20,'#ba6660',2)}
 else if(h.role==='grenadier'){ellipse(c,-12,-23,4,7,'#8c8b54');ellipse(c,-12,-22,2,2,'#dac592');line(c,-6,-29,8,-15,'#afa179',3)}
 else if(h.role==='officer'){poly(c,[[-9,-49],[8,-49],[11,-44],[-10,-44]],'#b39964');line(c,-13,-31,-14,-58,'#93a997',2)}
 else if(h.role==='tracker'){poly(c,[[-10,-46],[9,-46],[14,-41],[-15,-41]],'#99896a');line(c,-11,-31,-13,-13,'#b5a178',4)}
 else if(h.role==='flanker'){line(c,-7,-43,7,-43,'#83b6ad',3)}c.restore();
};
P.drawCorpses=function(game){const c=this.ctx;for(const a of game.corpses||[]){const p=this.project(a.x,a.y);if(!this.visible(p,80))continue;const t=this.reducedMotion?1:clamp(a.age/.65,0,1),ease=1-(1-t)**3,variant=a.fallVariant||0;c.save();c.translate(p.x,p.y);c.scale(this.camera.zoom,this.camera.zoom);c.globalAlpha=clamp(a.life/2,0,.76);ellipse(c,0,2,22,7,'rgba(0,8,12,.3)');if(a.type==='vehicle'){poly(c,[[-38,-10],[-6,-25],[35,-7],[18,9],[-25,9]],'#3a4644');line(c,-25,-7,16,3,'#9b8a64',3);c.restore();continue}
 const sign=Math.cos(a.fallDir||0)<0?-1:1;c.translate(Math.cos(a.fallDir||0)*ease*(variant===3?22:9),Math.sin(a.fallDir||0)*ease*6);c.rotate((variant===1?-1:1)*sign*ease*(variant===2?2.7:1.48));c.scale(1,1-ease*(variant===4?.6:.18));const copy={...a,hp:1,maxHp:100,attackTimer:0,hitTimer:0,animation:null,moving:false,state:'fallen'};if(a.type==='human')baseHuman.call(this,c,copy);else baseApe.call(this,c,copy,false,0);c.restore();if(a.type==='human'){c.save();c.translate(p.x+sign*22*ease,p.y-9*(1-ease));c.rotate(sign*ease*1.7);line(c,-13,0,13,0,'#89978b',2);line(c,-7,0,-2,4,'#74694e',3);c.restore()}}};
P.drawThreats=function(game){const c=this.ctx;for(const h of game.humans){if(!h.aiming||h.hp<=0)continue;const a=this.project(h.x,h.y,26),b=this.project(h.aiming.x,h.aiming.y,12);c.save();c.setLineDash([4,5]);line(c,a.x,a.y,b.x,b.y,'rgba(238,130,104,.8)',1);c.setLineDash([]);ellipse(c,b.x,b.y,6,4,'rgba(240,147,114,.6)');c.restore()}
 for(const h of game.forces?.hazards||[]){const p=this.project(h.x,h.y),progress=clamp((game.time-h.start)/(h.fuse||1.8),0,1);c.save();c.translate(p.x,p.y);c.scale(this.camera.zoom,this.camera.zoom);if(h.type==='grenade'){ellipse(c,0,0,h.radius*.8,h.radius*.42,'rgba(231,116,64,.13)');c.beginPath();c.ellipse(0,0,h.radius*.8,h.radius*.42,0,0,Math.PI*2);c.strokeStyle='#e69c69';c.lineWidth=2;c.stroke();ellipse(c,0,0,h.radius*.8*progress,h.radius*.42*progress,'rgba(235,151,78,.2)');const flight=clamp(progress*2,0,1),from=this.project(h.fromX,h.fromY);ellipse(c,(from.x-p.x)/this.camera.zoom*(1-flight),(from.y-p.y)/this.camera.zoom*(1-flight)-Math.sin(flight*Math.PI)*75-8,4,5,'#e1c38b')}else{this.glow(c,0,-12,100,'221,126,111',.15);line(c,-5,-3,3,-10,'#dfb494',3);ellipse(c,4,-12,4,5,'#ffe2b1');for(let i=0;i<3;i++)ellipse(c,Math.sin(this.time+i)*5,-20-i*16,6+i*4,7+i*4,'rgba(177,109,95,.19)')}c.restore()}};
P.drawGround=function(world,king,radius){baseGround.call(this,world,king,radius);const c=this.ctx,z=this.camera.zoom;for(const site of world.getSites(king.x,king.y,radius)){const road=site.approach;if(!road)continue;c.save();c.lineWidth=19*z;c.strokeStyle='#303e3b';c.lineCap='round';c.beginPath();road.forEach((r,i)=>{const p=this.project(r.x,r.y);i?c.lineTo(p.x,p.y):c.moveTo(p.x,p.y)});c.stroke();c.lineWidth=1;c.setLineDash([5,13]);c.strokeStyle='rgba(183,179,138,.18)';c.stroke();c.restore()}
 const step=this.quality==='low'?190:112;for(let x=Math.floor((king.x-radius)/step)*step;x<king.x+radius;x+=step)for(let y=Math.floor((king.y-radius)/step)*step;y<king.y+radius;y+=step){if(Math.hypot(x-king.x,y-king.y)>radius)continue;const t=world.terrain(x,y),p=this.project(x,y);if(!this.visible(p,20)||t.road)continue;c.save();c.translate(p.x,p.y);c.scale(z,z);const n=ATSUtil.hash(x+','+y)%10;if(t.water){line(c,-12,1,17,-1,'rgba(109,154,165,.13)',1);if(!this.reducedMotion)line(c,-8,5+Math.sin(this.time+n)*2,10,4,'rgba(112,162,166,.09)',1)}else if(t.biome==='farmland'){for(let i=0;i<4;i++){line(c,-19+i*11,-2,-31+i*11,6,'#53613e',2);line(c,-18+i*11,-3,-16+i*11,-10,'#748454',1)}}else if(t.biome==='rocky'){poly(c,[[-15,2],[-8,-4],[6,-2],[12,5]],'#3a4b4c');line(c,-8,-4,6,-2,'#6a7971',1)}else if(t.biome==='wetland'){for(let i=0;i<4;i++)line(c,-8+i*5,2,-10+i*6,-12-i%2*7,'#64806d',1.4)}else if(n<5){for(let i=0;i<3;i++)line(c,-8+i*7,2,-5+i*6,-3-i%2*3,'rgba(110,135,106,.3)',1)}c.restore()}};
P.drawSettlement=function(c,s){baseSettlement.call(this,c,s);const gardens=Math.min(5,s.gardens||0);for(let i=0;i<gardens;i++){const x=50+(i%3)*30,y=35+Math.floor(i/3)*17;poly(c,[[x-14,y],[x,y-8],[x+17,y],[x+2,y+8]],'#594e32');for(let j=0;j<4;j++){ellipse(c,x-7+j*5,y-2,3,2,'#8f9b5e');line(c,x-7+j*5,y-2,x-7+j*5,y-6,'#a5b375',1)}}if(s.buildProgress>0){const x=-58,y=19;line(c,x,y,x,y-35,'#a99b70',3);line(c,x+27,y+7,x+27,y-29,'#a99b70',3);line(c,x,y-28,x+27,y-21,'#b9ae80',2);line(c,x+4,y-5,x+23,y-29,'#8c815b',1.5)}if(s.maxDefense>0)this.health(c,(s.defense||0)/s.maxDefense,30,55,'#99b4a0');};
})();
