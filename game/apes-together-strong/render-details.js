/* Additional illustrated poses, readable force equipment and landscape details. */
(function(){
'use strict';
const P=ATSRenderer.prototype,baseApe=P.drawApe,baseHuman=P.drawHuman,baseSettlement=P.drawSettlement,baseGround=P.drawGround,baseObject=P.drawObject;
const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
// A world-space circle follows both isometric axes, including their diagonal
// extent. All explosive tells must cover the same radius that causes damage.
const ISO_RADIUS_X=.8*Math.SQRT2,ISO_RADIUS_Y=.42*Math.SQRT2;
function idHash(id){let n=0;for(const c of String(id||''))n=(Math.imul(n,31)+c.charCodeAt(0))|0;return n;}
function ellipse(c,x,y,rx,ry,color,rotation=0){c.beginPath();c.ellipse(x,y,rx,ry,rotation,0,Math.PI*2);c.fillStyle=color;c.fill()}
function line(c,x,y,u,v,color,width=2){c.beginPath();c.moveTo(x,y);c.lineTo(u,v);c.strokeStyle=color;c.lineWidth=width;c.lineCap='round';c.stroke()}
function poly(c,points,color){c.beginPath();points.forEach(([x,y],i)=>i?c.lineTo(x,y):c.moveTo(x,y));c.closePath();c.fillStyle=color;c.fill()}
P.drawObject=function(c,o){
 if(o.type==='cage'&&o.rescueOpened&&o.count>0){baseObject.call(this,c,o);label(c,o.count+' CAPTIVES WAITING',0,-54,'#efd394');return}
 if(!o.dead&&(o.type==='rock'||o.type==='berry')){let sprite=this.propSprites.get(o.type);if(!sprite){sprite=document.createElement('canvas');sprite.width=100;sprite.height=86;const sc=sprite.getContext('2d');sc.translate(50,62);if(o.type==='rock')this.drawRock(sc,{r:24});else this.drawBerry(sc,{});this.cacheSet(this.propSprites,o.type,sprite,4);}const size=o.type==='rock'?clamp((o.r||25)/24,.7,1.8):1;c.save();c.scale(size,size);if(o.type==='berry'&&o.amount===0)c.globalAlpha*=.6;c.drawImage(sprite,-50,-62);c.restore();if(o.type==='rock'&&o.hp>0&&o.maxHp&&o.hp<o.maxHp)this.health(c,o.hp/o.maxHp,-60,34,'#d7ae6b');return;}
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
P.drawHumanSprite=function(c,h){const frame=this.actorFrame(h),key=(h.kind||'rifle')+':'+(h.role||'guard')+':'+frame;let sprite=this.humanSprites.get(key);if(!sprite){if(this.actorSpriteBudget<=0){this.drawHuman(c,h);return;}this.actorSpriteBudget--;sprite=document.createElement('canvas');sprite.width=112;sprite.height=86;const sc=sprite.getContext('2d');sc.translate(44,74);const time=this.time,reduced=this.reducedMotion;this.time=frame/6*Math.PI*2/10;this.reducedMotion=false;this.drawHuman(sc,{...h,id:'',dir:0,moving:frame!==6,hp:100,maxHp:100,state:'patrol',suspicion:0,animation:null,hitTimer:0,aiming:null});this.time=time;this.reducedMotion=reduced;this.cacheSet(this.humanSprites,key,sprite,160);}c.save();c.scale(Math.cos(h.dir||0)<0?-1:1,1);c.drawImage(sprite,-44,-74);c.restore();if((h.suspicion||0)>.08||h.state==='combat'){c.font='900 15px system-ui';c.textAlign='center';c.fillStyle=h.state==='combat'?'#ed9678':'#dfd3a1';c.fillText(h.state==='combat'?'!':'?',0,-59);}if(h.hp>0&&h.maxHp&&h.hp<h.maxHp)this.health(c,h.hp/h.maxHp,-68,23,'#ddaf88');};
P.drawCorpses=function(game){const c=this.ctx;for(const a of game.corpses||[]){const p=this.project(a.x,a.y);if(!this.visible(p,110*this.camera.zoom))continue;const fresh=a.age<.75,t=this.reducedMotion?1:clamp(a.age/.65,0,1),ease=1-(1-t)**3,variant=a.fallVariant||0;c.save();c.translate(p.x,p.y);c.scale(this.camera.zoom,this.camera.zoom);c.globalAlpha=clamp(a.life/2,0,.76);ellipse(c,0,2,22,7,'rgba(0,8,12,.3)');if(a.type==='vehicle'){poly(c,[[-38,-10],[-6,-25],[35,-7],[18,9],[-25,9]],'#3a4644');line(c,-25,-7,16,3,'#9b8a64',3);c.restore();continue}
 const sign=Math.cos(a.fallDir||0)<0?-1:1;c.translate(Math.cos(a.fallDir||0)*ease*(variant===3?22:9),Math.sin(a.fallDir||0)*ease*6);c.rotate((variant===1?-1:1)*sign*ease*(variant===2?2.7:1.48));c.scale(1,1-ease*(variant===4?.6:.18));
 // Fresh falls remain animated. After settling, reuse a body sprite instead of
 // rebuilding limbs, clothing and face geometry on every battlefield frame.
 const age=a.actorAge??240,young=age<20||a.actorState==='young',facing=Math.cos(a.dir||0)<(a.type==='human'?0:-.15)?-1:1,fur=a.type==='human'?'':typeof a.fur==='string'?a.fur:['#4f5b53','#596050','#485a53','#657064','#536368'][typeof a.fur==='number'?Math.min(4,Math.floor(clamp(a.fur,0,1)*5)):((idHash(a.id)>>>0)%5)],key=a.type+':'+(a.kind||'')+':'+fur+':'+young+':'+facing;let sprite=!fresh?this.corpseSprites.get(key):null,copy;if(!sprite)copy={...a,fur,hp:100,maxHp:100,attackTimer:0,hitTimer:0,animation:null,moving:false,state:young?'young':'fallen',age};
 if(!fresh&&!sprite&&this.actorSpriteBudget>0){this.actorSpriteBudget--;sprite=document.createElement('canvas');sprite.width=110;sprite.height=88;const sc=sprite.getContext('2d');sc.translate(50,75);if(a.type==='human')baseHuman.call(this,sc,copy);else baseApe.call(this,sc,copy,false,0);this.cacheSet(this.corpseSprites,key,sprite,64);}
 c.scale(a.bodyScale||1,a.bodyScale||1);if(sprite)c.drawImage(sprite,-50,-75);else if(a.type==='human')baseHuman.call(this,c,copy);else baseApe.call(this,c,copy,false,0);c.restore();if(a.type==='human'){c.save();c.translate(p.x+sign*22*ease,p.y-9*(1-ease));c.rotate(sign*ease*1.7);line(c,-13,0,13,0,'#89978b',2);line(c,-7,0,-2,4,'#74694e',3);c.restore()}}};
P.drawThreats=function(game){const c=this.ctx;for(const h of game.humans||[]){if(!h.aiming||h.hp<=0)continue;const a=this.renderPoint(h,26),b=this.project(h.aiming.x,h.aiming.y,12);if(!this.segmentVisible(a,b,20))continue;c.save();c.setLineDash([4,5]);line(c,a.x,a.y,b.x,b.y,'rgba(238,130,104,.8)',1);c.setLineDash([]);ellipse(c,b.x,b.y,6,4,'rgba(240,147,114,.6)');c.restore()}
 for(const h of game.forces?.hazards||[]){if(h.type!=='grenade'&&h.type!=='flare')continue;const p=this.project(h.x,h.y),progress=clamp((game.time-h.start)/(h.fuse||1.8),0,1),from=h.type==='grenade'?this.project(h.fromX,h.fromY):p;if(!this.segmentVisible(p,from,Math.max((h.radius||100)*ISO_RADIUS_X,95)*this.camera.zoom))continue;c.save();c.translate(p.x,p.y);c.scale(this.camera.zoom,this.camera.zoom);if(h.type==='grenade'){ellipse(c,0,0,h.radius*ISO_RADIUS_X,h.radius*ISO_RADIUS_Y,'rgba(231,116,64,.13)');c.beginPath();c.ellipse(0,0,h.radius*ISO_RADIUS_X,h.radius*ISO_RADIUS_Y,0,0,Math.PI*2);c.strokeStyle='#e69c69';c.lineWidth=2;c.stroke();ellipse(c,0,0,h.radius*ISO_RADIUS_X*progress,h.radius*ISO_RADIUS_Y*progress,'rgba(235,151,78,.2)');const flight=clamp(progress*2,0,1);ellipse(c,(from.x-p.x)/this.camera.zoom*(1-flight),(from.y-p.y)/this.camera.zoom*(1-flight)-Math.sin(flight*Math.PI)*75-8,4,5,'#e1c38b')}else{this.glow(c,0,-12,100,'221,126,111',.15);line(c,-5,-3,3,-10,'#dfb494',3);ellipse(c,4,-12,4,5,'#ffe2b1');for(let i=0;i<3;i++)ellipse(c,Math.sin(this.time+i)*5,-20-i*16,6+i*4,7+i*4,'rgba(177,109,95,.19)')}c.restore()}};
P.drawGround=function(world,king,radius){baseGround.call(this,world,king,radius);const c=this.ctx,z=this.camera.zoom;for(const site of world.getSites(king.x,king.y,radius)){const road=site.approach;if(!road)continue;c.save();c.lineWidth=19*z;c.strokeStyle='#303e3b';c.lineCap='round';c.beginPath();road.forEach((r,i)=>{const p=this.project(r.x,r.y);i?c.lineTo(p.x,p.y):c.moveTo(p.x,p.y)});c.stroke();c.lineWidth=1;c.setLineDash([5,13]);c.strokeStyle='rgba(183,179,138,.18)';c.stroke();c.restore()}
 const step=this.quality==='low'?190:this.detailLevel>=3?150:112;for(let x=Math.floor((king.x-radius)/step)*step;x<king.x+radius;x+=step)for(let y=Math.floor((king.y-radius)/step)*step;y<king.y+radius;y+=step){if((x-king.x)**2+(y-king.y)**2>radius*radius)continue;const t=this.terrain(world,x,y),p=this.project(x,y);if(!this.visible(p,35*z)||t.road)continue;c.save();c.translate(p.x,p.y);c.scale(z,z);const n=ATSUtil.hash(x+','+y)%10;if(t.water){line(c,-12,1,17,-1,'rgba(109,154,165,.13)',1);if(!this.reducedMotion&&this.detailLevel<4)line(c,-8,5+Math.sin(this.time+n)*2,10,4,'rgba(112,162,166,.09)',1)}else if(t.biome==='farmland'){for(let i=0;i<4;i++){line(c,-19+i*11,-2,-31+i*11,6,'#53613e',2);line(c,-18+i*11,-3,-16+i*11,-10,'#748454',1)}}else if(t.biome==='rocky'){poly(c,[[-15,2],[-8,-4],[6,-2],[12,5]],'#3a4b4c');line(c,-8,-4,6,-2,'#6a7971',1)}else if(t.biome==='wetland'){for(let i=0;i<4;i++)line(c,-8+i*5,2,-10+i*6,-12-i%2*7,'#64806d',1.4)}else if(n<5){for(let i=0;i<3;i++)line(c,-8+i*7,2,-5+i*6,-3-i%2*3,'rgba(110,135,106,.3)',1)}c.restore()}};
P.drawSettlement=function(c,s){baseSettlement.call(this,c,s);const gardens=Math.min(5,s.gardens||0);for(let i=0;i<gardens;i++){const x=50+(i%3)*30,y=35+Math.floor(i/3)*17;poly(c,[[x-14,y],[x,y-8],[x+17,y],[x+2,y+8]],'#594e32');for(let j=0;j<4;j++){ellipse(c,x-7+j*5,y-2,3,2,'#8f9b5e');line(c,x-7+j*5,y-2,x-7+j*5,y-6,'#a5b375',1)}}if(s.buildProgress>0){const x=-58,y=19;line(c,x,y,x,y-35,'#a99b70',3);line(c,x+27,y+7,x+27,y-29,'#a99b70',3);line(c,x,y-28,x+27,y-21,'#b9ae80',2);line(c,x+4,y-5,x+23,y-29,'#8c815b',1.5)}if(s.maxDefense>0)this.health(c,(s.defense||0)/s.maxDefense,30,55,'#99b4a0');};

// Hull geometry is cached separately from damage, doors, turret direction and
// lights. Armored columns therefore reuse four small sprites, even in war.
const civilianVehicle=P.drawVehicle,previousObject=P.drawObject,previousHuman=P.drawHuman,previousApe=P.drawApe,previousThreats=P.drawThreats,previousEffects=P.drawEffects,previousWall=P.drawWall,previousGate=P.drawGate;
const militaryVehicles=new Set(['tank','apc','ifv','truck']);
function vehicleKind(v){return v.vehicleClass||v.vehicleType||v.kind||'jeep'}
function label(c,text,x,y,color='#e7d7ac'){c.font='700 9px system-ui';c.textAlign='center';c.fillStyle=color;c.fillText(text,x,y)}
function projectedDirection(dir,length){return{x:(Math.cos(dir)-Math.sin(dir))*.8*length,y:(Math.cos(dir)+Math.sin(dir))*.42*length}}
// Fence faces follow the same axes as their collision rectangles. This keeps
// long side walls, wide supply gates and layered barricades visually connected.
P.drawWallFaces=function(c,o){
 if(!o.w||!o.h){previousWall.call(this,c,o);return}
 const hx=o.w/2,hy=o.h/2,height=clamp(o.height||35,18,50),iso=(x,y)=>[(x-y)*.8,(x+y)*.42],a=iso(-hx,-hy),b=iso(hx,-hy),d=iso(-hx,hy),e=iso(hx,hy),top=p=>[p[0],p[1]-height],barrier=!!o.barricade;
 ellipse(c,0,3,(hx+hy)*.8+3,(hx+hy)*.42+3,'rgba(0,7,10,.25)');
 poly(c,[d,e,top(e),top(d)],barrier?'#626d60':'#4b6052');poly(c,[e,b,top(b),top(e)],barrier?'#455b4e':'#354e43');poly(c,[top(a),top(b),top(e),top(d)],barrier?'#8c947c':'#748572');
 for(const [from,to]of [[d,e],[e,b]]){
  const length=Math.hypot(to[0]-from[0],to[1]-from[1]),count=Math.max(1,Math.ceil(length/(barrier?22:9)));
  for(let i=0;i<=count;i++){const t=i/count,x=from[0]+(to[0]-from[0])*t,y=from[1]+(to[1]-from[1])*t;line(c,x,y,x,y-height,barrier?'#344d40':'#809078',barrier?1.4:2);if(!barrier&&i<count)poly(c,[[x-2,y-height],[x,y-height-6],[x+3,y-height]],'#9ba487')}
  line(c,from[0],from[1]-height+4,to[0],to[1]-height+4,barrier?'#b0af8c':'#a0ad8e',2);
  if(barrier){line(c,from[0],from[1]-7,to[0],to[1]-7,'#869478',2);for(let i=0;i<count;i++){const t=(i+.5)/count,x=from[0]+(to[0]-from[0])*t,y=from[1]+(to[1]-from[1])*t;line(c,x-3,y-height+5,x+3,y-height+12,'#c1b27e',3)}}
  else line(c,from[0],from[1]-12,to[0],to[1]-12,'#798d73',2);
 }
};
P.drawWall=function(c,o){
 if(!o.w||!o.h){previousWall.call(this,c,o);return}
 const height=clamp(o.height||35,18,50),key=o.w+':'+o.h+':'+height+':'+!!o.barricade;
 this.wallSprites=this.wallSprites||new Map();let sprite=this.wallSprites.get(key);
 if(!sprite&&this.actorSpriteBudget>0){
  this.actorSpriteBudget--;const halfSpan=(o.w+o.h)*.4,halfDepth=(o.w+o.h)*.21,canvas=document.createElement('canvas');canvas.width=Math.ceil(halfSpan*2+12);canvas.height=Math.ceil(height+halfDepth*2+22);
  const x=canvas.width/2,y=height+halfDepth+10,sc=canvas.getContext('2d');sc.translate(x,y);this.drawWallFaces(sc,o);sprite={canvas,x,y};this.cacheSet(this.wallSprites,key,sprite,24);
 }
 if(sprite)c.drawImage(sprite.canvas,-sprite.x,-sprite.y);else this.drawWallFaces(c,o);
};
P.drawGate=function(c,o){
 if(!o.w||!o.h){previousGate.call(this,c,o);return}
 const half=o.w/2,axis=o.h>o.w?Math.PI/2:0,d=projectedDirection(axis,half),height=o.height||44;
 const l={x:-d.x,y:-d.y},r={x:d.x,y:d.y};ellipse(c,0,4,Math.abs(d.x)+10,Math.abs(d.y)+7,'rgba(0,8,10,.3)');
 poly(c,[[l.x,l.y],[r.x,r.y],[r.x,r.y-height],[l.x,l.y-height]],'#334d44');
 const count=Math.max(4,Math.ceil(o.w/10));for(let i=0;i<=count;i++){const t=i/count,x=l.x+(r.x-l.x)*t,y=l.y+(r.y-l.y)*t;line(c,x,y-3,x,y-height+3,'#8b9a80',2)}
 for(const lift of [height-6,12])line(c,l.x,l.y-lift,r.x,r.y-lift,'#a4ad8c',3);
 line(c,l.x,l.y-height+5,r.x,r.y-5,'#6d8970',2);line(c,r.x,r.y-height+5,l.x,l.y-5,'#6d8970',2);
 for(const p of [l,r]){line(c,p.x,p.y+4,p.x,p.y-height-10,'#657c63',7);ellipse(c,p.x,p.y-height-10,4,2,'#b5b590')}
 poly(c,[[-5,-26],[5,-23],[5,-13],[-5,-16]],'#d0bc7c');c.save();c.translate(0,-height-3);c.rotate(Math.atan2(d.y,d.x));label(c,'RESTRICTED',0,0,'#e1d7b1');c.restore();
};
P.drawMilitaryHull=function(c,kind){
 const tank=kind==='tank',truck=kind==='truck',w=tank?76:truck?67:57,h=tank?28:23;
 ellipse(c,0,7,w+10,h,'rgba(0,6,9,.5)');
 if(tank){
  for(const side of [-1,1]){const x=side<0?-13:10,y=side<0?-12:8;poly(c,[[-w+x,y-10],[w*.55+x,y+3],[w*.64+x,y+20],[-w+x,y+5]],'#172727');for(let i=0;i<7;i++)ellipse(c,-w+12+i*18+x,y+3+i*1.9,7,8,'#566052',-.32);line(c,-w+x,y-8,w*.6+x,y+8,'#959279',3)}
 }else{
  for(const [x,y] of [[-42,-5],[-3,8],[35,18],[45,-3]]){ellipse(c,x,y,10,12,'#152629',-.3);ellipse(c,x+1,y,4,6,'#728371',-.3)}
 }
 poly(c,[[-w,-18],[-w*.35,-h-23],[w,-8],[w-5,16],[-w*.38,20],[-w,1]],truck?'#556653':tank?'#626a4e':'#5a735d');
 poly(c,[[-w,-18],[-w*.35,-h-23],[w,-8],[w*.28,9]],truck?'#849078':tank?'#949476':'#899a7f');
 poly(c,[[w*.28,9],[w,-8],[w-5,16],[-w*.38,20]],'#354d3c');
 line(c,-w+2,-15,w*.26,11,'#b2b092',2);
 if(truck){
  poly(c,[[-63,-18],[-61,-51],[-22,-69],[13,-46],[13,-13],[-23,3]],'#71806a');
  poly(c,[[-61,-51],[-22,-69],[13,-46],[-23,-29]],'#9b9e7d');
  for(let i=0;i<5;i++)line(c,-55+i*12,-47+i*4,-55+i*12,-15+i*3,'#576b53',2);
  poly(c,[[15,-16],[14,-52],[33,-59],[58,-40],[66,-10],[42,2]],'#5e765e');
  poly(c,[[17,-48],[33,-55],[51,-40],[42,-28],[18,-39]],'#223d3d');
  poly(c,[[48,-29],[57,-36],[63,-17],[56,-13]],'#2a4846');line(c,20,-36,40,-25,'#b1b294',1.5);
  line(c,-62,-19,-62,-43,'#a7a78b',2);line(c,-60,-24,-25,-10,'#a7a78b',2);
 }else{
  const top=tank?-46:-53;
  poly(c,[[-w*.62,-24],[-w*.6,top],[-9,top-13],[w*.58,-29],[w*.56,-10],[-13,5]],tank?'#68775a':'#687f64');
  poly(c,[[-w*.6,top],[-9,top-13],[w*.58,-29],[-13,-19]],tank?'#a0a084':'#99a389');
  if(!tank){poly(c,[[20,-40],[33,-33],[35,-23],[21,-28]],'#253c37');for(let i=0;i<3;i++)line(c,-34+i*13,-31+i*4,-27+i*13,-28+i*4,'#394e3d',2)}
  if(kind==='ifv'){for(let i=0;i<4;i++)poly(c,[[-51+i*22,-15+i*4],[-35+i*22,-11+i*4],[-35+i*22,-1+i*4],[-52+i*22,-5+i*4]],'#778675');}
  for(let i=0;i<3;i++)line(c,-23+i*10,-39+i*3,-12+i*10,-36+i*3,'#495f45',2);
 }
 // Visible rear access and armor seams make flank attacks readable.
 line(c,-w,-17,-w,0,'#c3b78e',2);line(c,-w+2,-12,-w*.39,7,'#465945',2);
 ellipse(c,w-6,-3,3.2,4,'#e9e4ba');ellipse(c,w*.65,9,3,3.5,'#e9e4ba');
 ellipse(c,-w+2,-7,2.5,3,'#9d5647');
};
P.drawVehicle=function(c,v){
 const kind=vehicleKind(v);if(!militaryVehicles.has(kind)){civilianVehicle.call(this,c,v);return}
 const tank=kind==='tank',truck=kind==='truck',flip=Math.cos(v.dir||0)<0?-1:1,w=tank?76:truck?67:57;
 this.vehicleSprites=this.vehicleSprites||new Map();let sprite=this.vehicleSprites.get(kind);
 if(!sprite&&this.actorSpriteBudget>0){this.actorSpriteBudget--;sprite=document.createElement('canvas');sprite.width=230;sprite.height=150;const sc=sprite.getContext('2d');sc.translate(115,112);this.drawMilitaryHull(sc,kind);this.cacheSet(this.vehicleSprites,kind,sprite,8)}
 c.save();c.scale(flip,1);if(sprite)c.drawImage(sprite,-115,-112);else this.drawMilitaryHull(c,kind);
 const moving=v.moving&&!this.reducedMotion,phase=this.time*9+(v.phase||0);
 if(tank&&moving&&this.detailLevel<3){for(let i=0;i<7;i++){const x=-66+((i*18+phase*5)%126);line(c,x,5+x*.15,x+4,12+x*.15,'#9d9a78',1.5)}}
 if(v.dismounting||v.unloadTimer>0&&v.troops<v.capacity){const open=v.dismounting?1:.4;
  poly(c,[[-w,-18],[-w-17*open,-31],[-w-16*open,-8],[-w,1]],'#93a082');
  poly(c,[[-w,-18],[-w+13*open,-12],[-w+13*open,10],[-w,1]],'#778c71');
  poly(c,[[-w,-14],[-w+8,-10],[-w+8,2],[-w,0]],'#142d27');
  line(c,-w,1,-w-23,11,'#b6b293',5);label(c,'DISMOUNT',-w,-41,'#f0d28c');
 }
 if(!this.reducedMotion&&this.detailLevel<4&&(v.hp>0||!v.maxHp)){
  for(let i=0;i<(moving?2:1);i++){const t=(this.time*.7+i*.47)%1;ellipse(c,-w-4-t*14,-23-t*22,4+t*8,3+t*7,`rgba(114,123,105,${(1-t)*.16})`)}
 }
 const engine=clamp(v.engineDamage||v.components?.engine||0,0,100),mobility=clamp(v.mobilityDamage||v.components?.mobility||0,0,100),weapon=clamp(v.weaponDamage||v.components?.weapon||0,0,100);
 if(engine>20||v.overrun){const count=this.detailLevel>=3?2:4;for(let i=0;i<count;i++){const t=this.reducedMotion?i/count:(this.time*.6+i/count)%1;ellipse(c,-w*.7+Math.sin(t*6)*7,-30-t*52,6+t*16,5+t*15,`rgba(94,100,86,${(1-t)*(.22+engine/360)})`)}
  if(engine>=75)poly(c,[[-w*.65,-22],[-w*.65-4,-35],[-w*.65+3,-30],[-w*.65+6,-41],[-w*.65+12,-19]],'#e0a66b');
 }
 if(mobility>25){line(c,-54,1,-29,7,'#352f26',5);line(c,-41,-2,-34,9,'#b49e6e',2)}
 if((mobility>50||weapon>50||v.overrun)&&this.detailLevel<4){const t=this.reducedMotion ? .5 : (this.time*2.4)%1;for(let i=0;i<4;i++){const x=(i%2?-28:18),y=i<2?4:-45;line(c,x,y,x+(i-1.5)*15*t,y-15*t,'#eed397',1.5*(1-t)+.5)}}
 const headOn=v.lightActive!==false&&v.hp!==0;this.glow(c,w-6,-3,13,'238,225,163',headOn ? .28 : .04);
 ellipse(c,-w+2,-7,3,3.5,v.braking||!v.moving?'#f1a078':'#a56151');
 c.restore();
 if(!truck){const dir=v.turretDir??v.dir??0,barrel=projectedDirection(dir,tank?78:kind==='ifv'?48:34),baseX=3*flip,baseY=tank?-57:-56;
  ellipse(c,baseX,baseY, tank?28:16,tank?14:9,tank?'#8f9a72':'#9aa287');
  poly(c,[[baseX-19,baseY],[baseX-17,baseY-11],[baseX+7,baseY-16],[baseX+23,baseY-2],[baseX+19,baseY+7]],tank?'#667d57':'#557655');
  line(c,baseX,baseY-5,baseX+barrel.x,baseY-5+barrel.y,'#c0c0a0',tank?9:kind==='ifv'?6:4);
  line(c,baseX+barrel.x*.84,baseY-5+barrel.y*.84,baseX+barrel.x,baseY-5+barrel.y,'#304632',tank?10:6);
  line(c,-15*flip,baseY-7,-15*flip,baseY-40,'#b0b79b',1.5);ellipse(c,10*flip,baseY-10,6,3,'#c9d4ae');this.glow(c,10*flip,baseY-10,13,'238,225,163',.2);
  if(weapon>=70){line(c,baseX+6,baseY-12,baseX+18,baseY+4,'#352f2a',3);line(c,baseX+19,baseY-10,baseX+4,baseY,'#dbc183',1.5)}
  if(v.cannonFlash>0){this.glow(c,baseX+barrel.x,baseY-5+barrel.y,42,'255,205,114',.72);ellipse(c,baseX+barrel.x,baseY-5+barrel.y,12,7,'#ffeab9')}
 }
 if(v.overrun){label(c,'OVERRUN',0,-102,'#ffd49a');if(!this.reducedMotion)this.glow(c,0,-38,67,'203,123,62',.08)}
 else if(mobility>=100)label(c,'IMMOBILIZED',0,-102,'#e4b484');
 else if(weapon>=100)label(c,'GUN DISABLED',0,-102,'#e4b484');
 if(v.hp>0&&v.maxHp&&v.hp<v.maxHp)this.health(c,v.hp/v.maxHp,tank?-92:-88,tank?64:49,'#dcaf7e');
};
P.drawObject=function(c,o){
 if(o.type==='vehicle'&&o.vehicleType&&!o.dead){this.drawVehicle(c,o);return}
 previousObject.call(this,c,o);if(o.dead)return;
 if(o.commandCenter){
  line(c,6,-73,6,-123,'#b1bca8',3);ellipse(c,6,-120,24,6,'#8ba391',-.35);line(c,-11,-122,22,-132,'#d3c9a2',2);line(c,-26,-54,-26,-89,'#a5b39b',2);poly(c,[[-26,-89],[-5,-82],[-7,-70],[-26,-77]],'#c8ba83');
  poly(c,[[-23,-32],[-6,-27],[-6,-14],[-23,-18]],'#bcc39d');line(c,-18,-26,-10,-22,'#445e43',2);line(c,-14,-29,-14,-19,'#445e43',2);
 }else if(o.repairBay){
  line(c,-47,5,-47,-36,'#a7ac88',4);line(c,-47,-36,-20,-26,'#a7ac88',4);line(c,-20,-26,-20,-9,'#a7ac88',2);line(c,-26,-9,-14,-9,'#d4c392',3);
  for(let i=0;i<3;i++)ellipse(c,26+i*8,8+i*3,6,7,'#20332b');label(c,'REPAIR',-36,-44,'#dbce9b');
 }
};
P.drawHuman=function(c,h){
 previousHuman.call(this,c,h);const flip=Math.cos(h.dir||0)<0?-1:1;c.save();c.scale(flip,1);
 if(['rifleman','ranger','heavy','engineer','leader','mortar'].includes(h.role)){
  poly(c,[[-9,-45],[-6,-51],[7,-51],[10,-44],[8,-40],[-8,-41]],h.role==='ranger'?'#405e50':'#465d47');line(c,-7,-45,8,-45,'#a1ac88',2);
  poly(c,[[-11,-31],[10,-31],[11,-16],[-10,-16]],h.role==='ranger'?'#3a5d4f':'#445c43');line(c,-8,-25,7,-25,'#a7a885',1.5);line(c,-8,-21,7,-21,'#879575',1.5);
 }
 if(h.role==='rifleman'){line(c,17,-29,43,-28,'#a7b596',2);line(c,24,-31,33,-31,'#344832',3)}
 else if(h.role==='ranger'){poly(c,[[-10,-48],[10,-46],[11,-42],[-10,-43]],'#7a9271');line(c,-14,-28,-14,-14,'#879775',4);line(c,4,-40,9,-39,'#263e2f',2)}
 else if(h.role==='heavy'){poly(c,[[-15,-30],[-9,-32],[-8,-16],[-15,-14]],'#778b63');line(c,-11,-29,9,-17,'#c2b173',3);for(let i=0;i<4;i++)line(c,-8+i*5,-28+i*3,-10+i*5,-23+i*3,'#5d6448',1.3);line(c,23,-28,48,-27,'#95a27d',4);line(c,37,-25,35,-8,'#5e795d',2)}
 else if(h.role==='engineer'){poly(c,[[-16,-29],[-8,-26],[-8,-9],[-16,-13]],'#a39369');line(c,-18,-35,-10,-14,'#c7c69e',3);line(c,-22,-36,-14,-39,'#a4b59a',4);line(c,4,-25,10,-21,'#d0be7d',3)}
 else if(h.role==='leader'){line(c,-5,-47,7,-47,'#d5c583',3);poly(c,[[4,-28],[10,-26],[7,-22]],'#ddc787');line(c,-15,-28,-15,-57,'#a9b89a',2)}
 else if(h.role==='mortar'){ellipse(c,20,5,18,6,'#485c40');line(c,16,2,27,-34,'#a4ac86',7);line(c,14,2,3,-4,'#718361',3);line(c,17,2,35,7,'#718361',3);ellipse(c,27,-34,5,3,'#283c2e');poly(c,[[-18,-29],[-10,-27],[-10,-12],[-18,-15]],'#8a9067')}
 c.restore();
 if(h.constructing){this.health(c,1-clamp(h.constructing.remaining/3.5,0,1),-69,29,'#d1c38b');label(c,'BUILDING',0,-76,'#e2d2a5')}
};
P.drawApe=function(c,a,king,exposure){
 const climbing=!!a.climbingVehicleId&&a.climbUntil>this.time,stagger=a.staggerUntil>this.time||a.knockbackUntil>this.time;
 if(!climbing&&!stagger){previousApe.call(this,c,a,king,exposure);return}
 c.save();if(climbing){c.translate(0,-(king?27:32));if(!this.reducedMotion)c.rotate(Math.sin(this.time*9+(a.phase||0))*.08)}
 else{c.translate(0,7);c.rotate((Math.cos(a.dir||0)<0?-1:1)*.33)}
 previousApe.call(this,c,a,king,exposure);
 if(climbing){line(c,-12,-30,-16,-47,'#778977',5);line(c,12,-30,17,-45,'#788b76',5);ellipse(c,-16,-47,4,3,'#acb297');ellipse(c,17,-45,4,3,'#acb297')}
 c.restore();
};
P.drawHeli=function(c,h){
 const p=this.renderPoint(h,125),ground=this.renderPoint(h);if(!this.visible(p,240*this.camera.zoom))return;
 const kind=h.kind|| (h.armed?'scout':'recon'),gunship=kind==='gunship',size=gunship?1.32:kind==='scout'?1.08:1,flip=Math.cos(h.dir||0)<0?-1:1;
 c.save();c.translate(ground.x,ground.y);c.scale(this.camera.zoom,this.camera.zoom);ellipse(c,0,0,43*size,18*size,'rgba(1,10,15,.25)');c.restore();
 c.save();c.translate(p.x,p.y+(this.reducedMotion?0:Math.sin(this.time*3+(h.phase||0))*2));c.scale(this.camera.zoom*flip*size,this.camera.zoom*size);
 line(c,-40,-1,17,18,'#526c64',3);line(c,-33,8,24,27,'#8b9d8a',3);line(c,-29,4,-27,-6,'#8b9d8a',2);line(c,19,23,20,6,'#8b9d8a',2);
 poly(c,[[-29,-12],[-4,-27],[30,-13],[41,5],[29,17],[-10,8]],gunship?'#485e46':kind==='scout'?'#657864':'#536f65');
 poly(c,[[9,-22],[29,-13],[36,1],[12,-5]],'#253f45');line(c,10,-21,29,-12,'#bcc3a2',1.5);
 poly(c,[[-18,-12],[-85,-42],[-98,-38],[-95,-25],[-29,3]],'#5b7669');poly(c,[[-88,-38],[-96,-64],[-102,-55],[-99,-25]],'#7c8e77');line(c,-90,-30,-112,-22,'#9aa68a',3);line(c,-2,-25,-2,-41,'#819a87',4);
 const rot=this.reducedMotion?0:this.time*39;line(c,-2-Math.cos(rot)*76,-40-Math.sin(rot)*12,-2+Math.cos(rot)*76,-40+Math.sin(rot)*12,'rgba(171,193,181,.64)',3);ellipse(c,-2,-40,72,11,'rgba(150,180,170,.10)');
 ellipse(c,32,9,5,4,'#c3dcd2');this.glow(c,32,9,18,'151,209,216',.28);ellipse(c,-12,-18,4,4,gunship?'#dabe85':'#8dbdb1');
 if(kind==='recon'){ellipse(c,18,14,7,5,'#334e49');ellipse(c,20,15,3,3,'#accbb6');line(c,-17,-19,-17,-40,'#98b2a4',1.5)}
 else{line(c,20,16,49,25,'#b1b69c',4);poly(c,[[-16,-1],[4,4],[4,11],[-16,6]],'#273c33');if(gunship){poly(c,[[-24,-5],[12,8],[30,5],[-5,-10]],'#7f8b68');for(const [x,y]of [[-22,7],[12,17]]){ellipse(c,x,y,10,6,'#283d2e');for(let i=0;i<3;i++)ellipse(c,x-5+i*5,y,1.5,1.5,'#89977d')}line(c,28,16,48,21,'#d3c6a0',6)}
  if(h.attackTimer>0){this.glow(c,49,25,27,'255,199,114',.75);ellipse(c,49,25,8,4,'#ffe6ab')}
 }
 if(h.hp>0&&h.maxHp&&h.hp<h.maxHp)this.health(c,h.hp/h.maxHp,-73,gunship?53:38,'#dcaf7e');c.restore();
};
P.drawThreats=function(game){
 previousThreats.call(this,game);const c=this.ctx,z=this.camera.zoom,captions=[];
 const reserveCaption=(x,y)=>{if(captions.some(p=>Math.abs(p.x-x)<100*z&&Math.abs(p.y-y)<22*z))return false;captions.push({x,y});return true};
 for(const mortar of game.forces?.hazards||[]){if(mortar.type!=='mortar'&&mortar.type!=='airstrike')continue;const p=this.project(mortar.x,mortar.y),r=mortar.radius||100;if(!this.visible(p,(r*ISO_RADIUS_X+30)*z))continue;const progress=clamp((game.time-mortar.start)/(mortar.fuse||2.1),0,1);c.save();c.translate(p.x,p.y);c.scale(z,z);ellipse(c,0,0,r*ISO_RADIUS_X,r*ISO_RADIUS_Y,'rgba(242,139,75,.14)');c.beginPath();c.ellipse(0,0,r*ISO_RADIUS_X,r*ISO_RADIUS_Y,0,0,Math.PI*2);c.strokeStyle='#ffb56f';c.lineWidth=2.5;c.stroke();c.beginPath();c.ellipse(0,0,r*ISO_RADIUS_X,r*ISO_RADIUS_Y,0,-Math.PI*.5,-Math.PI*.5+Math.PI*2*progress);c.strokeStyle='#ffe4a5';c.lineWidth=4;c.stroke();line(c,-11,0,11,0,'#ffe4a5',2);line(c,0,-8,0,8,'#ffe4a5',2);label(c,mortar.type==='airstrike'?'AIRSTRIKE INCOMING':'MORTAR INCOMING',0,-r*ISO_RADIUS_Y-12,'#ffd79d');c.restore()}
 for(const v of game.vehicles||[]){const target=v.cannonTarget;if(!target||v.hp<=0)continue;const from=this.renderPoint(v,56),p=this.project(target.x,target.y),r=window.ATSVehicleSpecs?.[vehicleKind(v)]?.cannonRadius||(vehicleKind(v)==='ifv'?60:104);if(!this.segmentVisible(from,p,r*ISO_RADIUS_X*z))continue;
  const progress=clamp((game.time-target.start)/(target.duration||1.8),0,1);c.save();
  c.setLineDash([8,7]);line(c,from.x,from.y,p.x,p.y,`rgba(244,167,109,${.25+progress*.3})`,1.5*z);c.setLineDash([]);c.translate(p.x,p.y);c.scale(z,z);
  ellipse(c,0,0,r*ISO_RADIUS_X,r*ISO_RADIUS_Y,'rgba(234,128,79,.12)');c.beginPath();c.ellipse(0,0,r*ISO_RADIUS_X,r*ISO_RADIUS_Y,0,0,Math.PI*2);c.strokeStyle='#f1b176';c.lineWidth=2.2;c.stroke();
  c.beginPath();c.ellipse(0,0,r*ISO_RADIUS_X,r*ISO_RADIUS_Y,0,-Math.PI*.5,-Math.PI*.5+Math.PI*2*progress);c.strokeStyle='#ffe0a8';c.lineWidth=4;c.stroke();
  for(const side of [-1,1]){line(c,side*13,0,side*27,0,'#ffe0a8',2);line(c,0,side*6,0,side*14,'#ffe0a8',2)}
  if(reserveCaption(p.x,p.y-(r*ISO_RADIUS_Y+12)*z))label(c,'CANNON LOCK',0,-r*ISO_RADIUS_Y-12,'#ffd29d');c.restore();
 }
 for(const shell of game.forces?.hazards||[]){if(shell.type!=='shell')continue;const p=this.project(shell.x,shell.y,20),dir=Math.atan2(shell.targetY-shell.fromY,shell.targetX-shell.fromX),d=projectedDirection(dir,36),q={x:p.x-d.x*z,y:p.y-d.y*z};if(!this.segmentVisible(p,q,25*z))continue;
  c.save();c.globalCompositeOperation='screen';line(c,q.x,q.y,p.x,p.y,'rgba(255,192,98,.75)',3*z);ellipse(c,p.x,p.y,5*z,3*z,'#ffefc1');this.glow(c,p.x,p.y,14*z,'244,196,113',.55);c.restore();
 }
 // One order cue per visible squad, with a strict label budget. Cohesion loss
 // is visible at the leader's position without drawing N squared links.
 const shown=new Set();let budget=this.detailLevel>=3?4:10;
 for(const h of game.humans||[]){if(!budget||!h.squadId||shown.has(h.squadId)||h.hp<=0)continue;if(h.role!=='leader'&&h.role!=='officer'&&!(h.cohesionLossUntil>game.time))continue;const p=this.renderPoint(h,78);if(!this.visible(p,40)||!reserveCaption(p.x,p.y))continue;shown.add(h.squadId);budget--;c.save();c.translate(p.x,p.y);c.scale(z,z);
  const disrupted=h.cohesionLossUntil>game.time;label(c,disrupted?'REGROUP':String(h.squadOrder||'').replace(/([a-z])([A-Z])/g,'$1 $2').toUpperCase(),0,0,disrupted?'#edaa83':'#a8c5ab');c.restore();
 }
};
P.drawEffects=function(c,effects){
 const regular=[];let budget=this.detailLevel>=3?8:20;
 for(const e of effects){if(e.type==='siegeShot'){const from=this.project(e.x,e.y,24),to=this.project(e.targetX,e.targetY,12);if(this.segmentVisible(from,to,12))line(c,from.x,from.y,to.x,to.y,'rgba(255,210,119,.7)',1.5*this.camera.zoom);continue}if(e.type!=='tankImpact'&&e.type!=='cannonImpact'){regular.push(e);continue}if(budget<=0)continue;const r=e.radius||e.range||104,p=this.project(e.x,e.y),t=clamp(1-e.life/(e.maxLife||1),0,1);if(!this.visible(p,(r+60)*this.camera.zoom))continue;budget--;c.save();c.translate(p.x,p.y);c.scale(this.camera.zoom,this.camera.zoom);c.globalAlpha=1-t;
  ellipse(c,0,0,r*.8*(.3+t),r*.42*(.3+t),'rgba(153,131,92,.28)');if(t<.35){this.glow(c,0,-12,85*(1-t),'255,179,85',.6);ellipse(c,0,-13,28*(1-t),18*(1-t),'#ffd7a1')}
  const count=this.detailLevel>=3?4:8;for(let i=0;i<count;i++){const a=i/count*Math.PI*2,d=r*(.25+t*.8);ellipse(c,Math.cos(a)*d*.8,Math.sin(a)*d*.42-t*36,8+t*12,5+t*10,'rgba(150,144,118,.37)');line(c,Math.cos(a)*d*.65,Math.sin(a)*d*.33-t*26,Math.cos(a)*d*.85,Math.sin(a)*d*.44-t*29,'#d8c291',2)}c.restore();
 }
 previousEffects.call(this,c,regular);
};
})();
