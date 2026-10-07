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
 if(o.type==='cage'&&o.rescueOpened&&o.count>0){baseObject.call(this,c,o);return}
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
P.drawHumanSprite=function(c,h){const frame=this.actorFrame(h),key=(h.kind||'rifle')+':'+(h.role||'guard')+':'+frame;let sprite=this.humanSprites.get(key);if(!sprite){if(this.actorSpriteBudget<=0){this.drawHuman(c,h);return;}this.actorSpriteBudget--;sprite=document.createElement('canvas');sprite.width=112;sprite.height=86;const sc=sprite.getContext('2d');sc.translate(44,74);const time=this.time,reduced=this.reducedMotion;this.time=frame/6*Math.PI*2/10;this.reducedMotion=false;this.drawHuman(sc,{...h,id:'',dir:0,moving:frame!==6,hp:100,maxHp:100,state:'patrol',suspicion:0,animation:null,hitTimer:0,aiming:null});this.time=time;this.reducedMotion=reduced;this.cacheSet(this.humanSprites,key,sprite,160);}c.save();c.scale(Math.cos(h.dir||0)<0?-1:1,1);c.drawImage(sprite,-44,-74);c.restore();if((h.suspicion||0)>.08||h.state==='combat')this.drawAwarenessIcon(c,h);if(h.hp>0&&h.maxHp&&h.hp<h.maxHp)this.health(c,h.hp/h.maxHp,-68,23,'#ddaf88');};
function drawCorpseBody(r,c,a){const quality=r.quality,zoom=r.camera.zoom,detail=r.detailLevel,reduced=r.reducedMotion;r.quality='high';r.camera.zoom=1;r.detailLevel=0;r.reducedMotion=true;if(a.type==='human')baseHuman.call(r,c,a);else baseApe.call(r,c,a,false,0);r.quality=quality;r.camera.zoom=zoom;r.detailLevel=detail;r.reducedMotion=reduced;}
P.drawCorpses=function(game){const c=this.ctx;for(const a of game.corpses||[]){const p=this.project(a.x,a.y);if(!this.visible(p,110*this.camera.zoom))continue;const fresh=a.age<.75,t=this.reducedMotion?1:clamp(a.age/.65,0,1),ease=1-(1-t)**3,variant=a.fallVariant||0;c.save();c.translate(p.x,p.y);c.scale(this.camera.zoom,this.camera.zoom);c.globalAlpha=clamp(a.life/2,0,.76);ellipse(c,0,2,22,7,'rgba(0,8,12,.3)');if(a.type==='vehicle'){poly(c,[[-38,-10],[-6,-25],[35,-7],[18,9],[-25,9]],'#3a4644');line(c,-25,-7,16,3,'#9b8a64',3);c.restore();continue}
 const sign=Math.cos(a.fallDir||0)<0?-1:1;c.translate(Math.cos(a.fallDir||0)*ease*(variant===3?22:9),Math.sin(a.fallDir||0)*ease*6);c.rotate((variant===1?-1:1)*sign*ease*(variant===2?2.7:1.48));c.scale(1,1-ease*(variant===4?.6:.18));
 // Fresh falls remain animated. After settling, reuse a body sprite instead of
 // rebuilding limbs, clothing and face geometry on every battlefield frame.
 const age=a.actorAge??240,young=age<20||a.actorState==='young',facing=Math.cos(a.dir||0)<(a.type==='human'?0:-.15)?-1:1,look=a.type==='human'?null:this.primateLook(a),appearance=look?look.species+':'+look.coatVariant:(a.role||'guard'),key=a.type+':'+(a.kind||'')+':'+appearance+':'+young+':'+facing;let sprite=!fresh?this.corpseSprites.get(key):null,copy;if(!sprite)copy={...a,...(look?{species:look.species,coatVariant:look.coatVariant}:{}),hp:100,maxHp:100,attackTimer:0,hitTimer:0,animation:null,moving:false,_atlas:true,state:young?'young':'fallen',age};
 if(!fresh&&!sprite&&this.actorSpriteBudget>0){this.actorSpriteBudget--;sprite=document.createElement('canvas');sprite.width=140;sprite.height=132;const sc=sprite.getContext('2d');sc.translate(70,112);drawCorpseBody(this,sc,copy);this.cacheSet(this.corpseSprites,key,sprite,64);}
 c.scale(a.bodyScale||1,a.bodyScale||1);if(sprite)c.drawImage(sprite,-70,-112);else drawCorpseBody(this,c,copy);c.restore();if(a.type==='human'){c.save();c.translate(p.x+sign*22*ease,p.y-9*(1-ease));c.rotate(sign*ease*1.7);line(c,-13,0,13,0,'#89978b',2);line(c,-7,0,-2,4,'#74694e',3);c.restore()}}};
P.drawThreats=function(game){const c=this.ctx;for(const h of game.humans||[]){if(!h.aiming||h.hp<=0)continue;const a=this.renderPoint(h,26),b=this.project(h.aiming.x,h.aiming.y,12);if(!this.segmentVisible(a,b,20))continue;c.save();c.setLineDash([4,5]);line(c,a.x,a.y,b.x,b.y,'rgba(238,130,104,.8)',1);c.setLineDash([]);ellipse(c,b.x,b.y,6,4,'rgba(240,147,114,.6)');c.restore()}
 for(const h of game.forces?.hazards||[]){if(h.type!=='grenade'&&h.type!=='flare')continue;const p=this.project(h.x,h.y),progress=clamp((game.time-h.start)/(h.fuse||1.8),0,1),from=h.type==='grenade'?this.project(h.fromX,h.fromY):p;if(!this.segmentVisible(p,from,Math.max((h.radius||100)*ISO_RADIUS_X,95)*this.camera.zoom))continue;c.save();c.translate(p.x,p.y);c.scale(this.camera.zoom,this.camera.zoom);if(h.type==='grenade'){ellipse(c,0,0,h.radius*ISO_RADIUS_X,h.radius*ISO_RADIUS_Y,'rgba(231,116,64,.13)');c.beginPath();c.ellipse(0,0,h.radius*ISO_RADIUS_X,h.radius*ISO_RADIUS_Y,0,0,Math.PI*2);c.strokeStyle='#e69c69';c.lineWidth=2;c.stroke();ellipse(c,0,0,h.radius*ISO_RADIUS_X*progress,h.radius*ISO_RADIUS_Y*progress,'rgba(235,151,78,.2)');const flight=clamp(progress*2,0,1);ellipse(c,(from.x-p.x)/this.camera.zoom*(1-flight),(from.y-p.y)/this.camera.zoom*(1-flight)-Math.sin(flight*Math.PI)*75-8,4,5,'#e1c38b')}else{this.glow(c,0,-12,100,'221,126,111',.15);line(c,-5,-3,3,-10,'#dfb494',3);ellipse(c,4,-12,4,5,'#ffe2b1');for(let i=0;i<3;i++)ellipse(c,Math.sin(this.time+i)*5,-20-i*16,6+i*4,7+i*4,'rgba(177,109,95,.19)')}c.restore()}};
P.drawGround=function(world,king,radius){baseGround.call(this,world,king,radius);const c=this.ctx,z=this.camera.zoom;for(const site of world.getSites(king.x,king.y,radius)){const road=site.approach;if(!road)continue;c.save();c.lineWidth=19*z;c.strokeStyle='#303e3b';c.lineCap='round';c.beginPath();road.forEach((r,i)=>{const p=this.project(r.x,r.y);i?c.lineTo(p.x,p.y):c.moveTo(p.x,p.y)});c.stroke();c.lineWidth=1;c.setLineDash([5,13]);c.strokeStyle='rgba(183,179,138,.18)';c.stroke();c.restore()}
 const step=this.quality==='low'?190:this.detailLevel>=3?150:112;for(let x=Math.floor((king.x-radius)/step)*step;x<king.x+radius;x+=step)for(let y=Math.floor((king.y-radius)/step)*step;y<king.y+radius;y+=step){if((x-king.x)**2+(y-king.y)**2>radius*radius)continue;const t=this.terrain(world,x,y),p=this.project(x,y);if(!this.visible(p,35*z)||t.road)continue;c.save();c.translate(p.x,p.y);c.scale(z,z);const n=ATSUtil.hash(x+','+y)%10;if(t.water){line(c,-12,1,17,-1,'rgba(109,154,165,.13)',1);if(!this.reducedMotion&&this.detailLevel<4)line(c,-8,5+Math.sin(this.time+n)*2,10,4,'rgba(112,162,166,.09)',1)}else if(t.biome==='farmland'){for(let i=0;i<4;i++){line(c,-19+i*11,-2,-31+i*11,6,'#53613e',2);line(c,-18+i*11,-3,-16+i*11,-10,'#748454',1)}}else if(t.biome==='rocky'){poly(c,[[-15,2],[-8,-4],[6,-2],[12,5]],'#3a4b4c');line(c,-8,-4,6,-2,'#6a7971',1)}else if(t.biome==='wetland'){for(let i=0;i<4;i++)line(c,-8+i*5,2,-10+i*6,-12-i%2*7,'#64806d',1.4)}else if(n<5){for(let i=0;i<3;i++)line(c,-8+i*7,2,-5+i*6,-3-i%2*3,'rgba(110,135,106,.3)',1)}c.restore()}};
P.drawSettlement=function(c,s){baseSettlement.call(this,c,s);const gardens=Math.min(5,s.gardens||0);for(let i=0;i<gardens;i++){const x=50+(i%3)*30,y=35+Math.floor(i/3)*17;poly(c,[[x-14,y],[x,y-8],[x+17,y],[x+2,y+8]],'#594e32');for(let j=0;j<4;j++){ellipse(c,x-7+j*5,y-2,3,2,'#8f9b5e');line(c,x-7+j*5,y-2,x-7+j*5,y-6,'#a5b375',1)}}if(s.buildProgress>0){const x=-58,y=19;line(c,x,y,x,y-35,'#a99b70',3);line(c,x+27,y+7,x+27,y-29,'#a99b70',3);line(c,x,y-28,x+27,y-21,'#b9ae80',2);line(c,x+4,y-5,x+23,y-29,'#8c815b',1.5)}if(s.maxDefense>0)this.health(c,(s.defense||0)/s.maxDefense,30,55,'#99b4a0');};

// Hull geometry is cached separately from damage, doors, turret direction and
// lights. Base hulls and late-war variants share an eight-sprite budget.
const civilianVehicle=P.drawVehicle,previousObject=P.drawObject,previousHuman=P.drawHuman,previousApe=P.drawApe,previousThreats=P.drawThreats,previousEffects=P.drawEffects,previousWall=P.drawWall,previousGate=P.drawGate;
const militaryVehicles=new Set(['tank','apc','ifv','truck']);
const armoredLooks={
 veteran:{kind:'tank',scale:1.04,barrel:1.08,paint:'#60744d',top:'#9cab7b',edge:'#b9c49a',accent:'#ded39c'},
 siege:{kind:'tank',scale:1.1,barrel:1.28,paint:'#8b7756',top:'#b8a17a',edge:'#d4c49e',accent:'#ca7e56'},
 ironclad:{kind:'tank',scale:1.1,barrel:1.15,paint:'#40515d',top:'#768993',edge:'#a3b8bc',accent:'#d49a67'},
 sentinel:{kind:'ifv',scale:1.06,barrel:1.15,paint:'#4c6777',top:'#8fa5b0',edge:'#b2c5cc',accent:'#8cc4b9'}
};
function vehicleKind(v){return v.vehicleClass||v.vehicleType||v.kind||'jeep'}
function vehicleLook(v){const look=armoredLooks[v.variant];return look?.kind===vehicleKind(v)?look:null}
function projectedDirection(dir,length){return{x:(Math.cos(dir)-Math.sin(dir))*.8*length,y:(Math.cos(dir)+Math.sin(dir))*.42*length}}
// Fence faces follow the same axes as their collision rectangles. This keeps
// long side walls, wide supply gates and layered barricades visually connected.
function wallAppearance(o){const tier=clamp(Math.round(o.wallTier||1),1,4),height=clamp(o.visualHeight||o.height||[35,35,52,76,104][tier],18,140),ratio=o.hp===undefined?1:Math.max(0,o.hp)/(o.maxHp||100),damage=o.dead||ratio<=0?3:ratio<.3?2:ratio<.65?1:0;return{tier,height,damage}}
P.drawWallFaces=function(c,o){
 const hx=(o.w||70)/2,hy=(o.h||14)/2,{tier,height,damage}=wallAppearance(o),iso=(x,y)=>[(x-y)*.8,(x+y)*.42],a=iso(-hx,-hy),b=iso(hx,-hy),d=iso(-hx,hy),e=iso(hx,hy),top=p=>[p[0],p[1]-height],barrier=!!o.barricade,stone=tier>=3;
 ellipse(c,0,3,(hx+hy)*.8+4,(hx+hy)*.42+4,'rgba(0,7,10,.25)');
 if(damage===3){for(let i=0;i<9;i++){const t=i/8,x=d[0]+(e[0]-d[0])*t,y=d[1]+(e[1]-d[1])*t;poly(c,[[x-8,y],[x-3,y-9],[x+9,y-4],[x+6,y+5]],stone?'#73807a':'#736849');if(i%2===0)line(c,x-7,y-1,x+12,y+5,stone?'#a2aaa0':'#b2976a',3)}return}
 poly(c,[d,e,top(e),top(d)],stone?tier===4?'#697978':'#78877a':barrier?'#626d60':tier===2?'#625e47':'#4b6052');poly(c,[e,b,top(b),top(e)],stone?tier===4?'#425c5d':'#4f685f':barrier?'#455b4e':'#354e43');poly(c,[top(a),top(b),top(e),top(d)],stone?tier===4?'#a7b6aa':'#b0b4a0':barrier?'#8c947c':'#8e9776');
 for(const [from,to]of [[d,e],[e,b]]){
  const length=Math.hypot(to[0]-from[0],to[1]-from[1]),count=Math.max(1,Math.ceil(length/(stone?28:barrier?22:9)));
  if(stone){for(let lift=12;lift<height;lift+=14){line(c,from[0],from[1]-lift,to[0],to[1]-lift,tier===4?'#476160':'#536a60',1.4);const row=Math.floor(lift/14);for(let i=0;i<count;i++){const t=(i+(row%2?.5:0))/count,x=from[0]+(to[0]-from[0])*t,y=from[1]+(to[1]-from[1])*t;line(c,x,y-lift,x,y-Math.min(height,lift+14),tier===4?'#58716f':'#5f766a',1)}}}
  else for(let i=0;i<=count;i++){const t=i/count,x=from[0]+(to[0]-from[0])*t,y=from[1]+(to[1]-from[1])*t;line(c,x,y,x,y-height,tier===2?'#b0a079':barrier?'#344d40':'#809078',tier===2?3:barrier?1.4:2);if(!barrier&&i<count)poly(c,[[x-2,y-height],[x,y-height-6],[x+3,y-height]],tier===2?'#c2b28a':'#9ba487')}
  line(c,from[0],from[1]-height+4,to[0],to[1]-height+4,stone?'#cad0b5':'#b0af8c',tier===4?5:3);
  if(tier>=3){for(let i=0;i<=count;i++){const t=i/count,x=from[0]+(to[0]-from[0])*t,y=from[1]+(to[1]-from[1])*t;poly(c,[[x-5,y-height+1],[x-5,y-height-9],[x+5,y-height-9],[x+5,y-height+1]],tier===4?'#819790':'#9faa92');if(tier===4){line(c,x,y,x,y-height,'#9bac9f',3);for(const lift of[17,height-14])ellipse(c,x,y-lift,1.6,1.6,'#d1cfad')}}}
  else if(tier===2){line(c,from[0],from[1]-7,to[0],to[1]-7,'#c0aa7d',5);line(c,from[0],from[1]-height+10,to[0],to[1]-12,'#928362',4)}
  else line(c,from[0],from[1]-12,to[0],to[1]-12,'#798d73',2);
  if(damage){const x=(from[0]+to[0])*.5,y=(from[1]+to[1])*.5;line(c,x-7,y-height+6,x+3,y-height*.66,'#293e37',damage===2?4:2);line(c,x+3,y-height*.66,x-5,y-height*.38,'#293e37',damage===2?4:2);if(damage===2)poly(c,[[x-11,y-height],[x-7,y-height*.55],[x+8,y-height*.64],[x+12,y-height]],'#243b35')}
 }
};
P.drawWall=function(c,o){
 const {tier,height,damage}=wallAppearance(o),w=o.w||70,h=o.h||14,key=w+':'+h+':'+height+':'+tier+':'+!!o.barricade+':'+damage;
 this.wallSprites=this.wallSprites||new Map();let sprite=this.wallSprites.get(key);
 const width=Math.ceil((w+h)*.8+22),canvasHeight=Math.ceil(height+(w+h)*.42+34);
 if(!sprite&&this.actorSpriteBudget>0&&width*canvasHeight<=600000){this.actorSpriteBudget--;const canvas=document.createElement('canvas');canvas.width=width;canvas.height=canvasHeight;const x=width/2,y=height+(w+h)*.21+19,sc=canvas.getContext('2d');sc.translate(x,y);this.drawWallFaces(sc,o);sprite={canvas,x,y};this.cacheSet(this.wallSprites,key,sprite,24)}
 if(sprite)c.drawImage(sprite.canvas,-sprite.x,-sprite.y);else this.drawWallFaces(c,o);
};
P.drawGate=function(c,o){
 const length=Math.max(o.w||90,o.h||14),half=length/2,axis=(o.h||14)>(o.w||90)?Math.PI/2:0,d=projectedDirection(axis,half),{tier,height}=wallAppearance({...o,height:o.height||[44,44,58,80,108][clamp(o.wallTier||1,1,4)]}),stone=tier>=3;
 const l={x:-d.x,y:-d.y},r={x:d.x,y:d.y};ellipse(c,0,4,Math.abs(d.x)+10,Math.abs(d.y)+7,'rgba(0,8,10,.3)');
 poly(c,[[l.x,l.y],[r.x,r.y],[r.x,r.y-height],[l.x,l.y-height]],stone?'#344f51':'#334d44');
 const count=Math.max(4,Math.ceil(length/(stone?18:10)));for(let i=0;i<=count;i++){const t=i/count,x=l.x+(r.x-l.x)*t,y=l.y+(r.y-l.y)*t;line(c,x,y-3,x,y-height+3,stone?'#809c98':'#8b9a80',tier>=3?4:2)}
 for(const lift of [height-6,12])line(c,l.x,l.y-lift,r.x,r.y-lift,stone?'#adbcb0':'#a4ad8c',tier===4?6:3);
 line(c,l.x,l.y-height+5,r.x,r.y-5,'#6d8970',tier>=2?4:2);line(c,r.x,r.y-height+5,l.x,l.y-5,'#6d8970',tier>=2?4:2);
 for(const p of [l,r]){line(c,p.x,p.y+4,p.x,p.y-height-10,stone?'#92a69b':'#657c63',tier===4?13:7);ellipse(c,p.x,p.y-height-10,tier===4?7:4,2,'#b5b590')}
 poly(c,[[-5,-26],[5,-23],[5,-13],[-5,-16]],'#d0bc7c');if(o.hp>0&&o.maxHp&&o.hp<o.maxHp){line(c,-10,-height+5,8,-height*.55,'#223d34',3);line(c,8,-height*.55,-4,-15,'#223d34',2)}
};
P.drawMilitaryHull=function(c,kind,variant){
 const tank=kind==='tank',truck=kind==='truck',look=armoredLooks[variant]?.kind===kind?armoredLooks[variant]:null,paint=window.ATSVehicleVariants?.[variant]?.paint||look?.paint,w=tank?76:truck?67:57,h=tank?28:23;
 ellipse(c,0,7,w+10,h,'rgba(0,6,9,.5)');
 if(tank){
  for(const side of [-1,1]){const x=side<0?-13:10,y=side<0?-12:8;poly(c,[[-w+x,y-10],[w*.55+x,y+3],[w*.64+x,y+20],[-w+x,y+5]],'#172727');for(let i=0;i<7;i++)ellipse(c,-w+12+i*18+x,y+3+i*1.9,7,8,'#566052',-.32);line(c,-w+x,y-8,w*.6+x,y+8,'#959279',3)}
 }else{
  for(const [x,y] of [[-42,-5],[-3,8],[35,18],[45,-3]]){ellipse(c,x,y,10,12,'#152629',-.3);ellipse(c,x+1,y,4,6,'#728371',-.3)}
 }
 poly(c,[[-w,-18],[-w*.35,-h-23],[w,-8],[w-5,16],[-w*.38,20],[-w,1]],paint||(truck?'#556653':tank?'#626a4e':'#5a735d'));
 poly(c,[[-w,-18],[-w*.35,-h-23],[w,-8],[w*.28,9]],look?.top||(truck?'#849078':tank?'#949476':'#899a7f'));
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
  poly(c,[[-w*.62,-24],[-w*.6,top],[-9,top-13],[w*.58,-29],[w*.56,-10],[-13,5]],paint||(tank?'#68775a':'#687f64'));
  poly(c,[[-w*.6,top],[-9,top-13],[w*.58,-29],[-13,-19]],look?.top||(tank?'#a0a084':'#99a389'));
  if(!tank){poly(c,[[20,-40],[33,-33],[35,-23],[21,-28]],'#253c37');for(let i=0;i<3;i++)line(c,-34+i*13,-31+i*4,-27+i*13,-28+i*4,'#394e3d',2)}
  if(kind==='ifv'){for(let i=0;i<4;i++)poly(c,[[-51+i*22,-15+i*4],[-35+i*22,-11+i*4],[-35+i*22,-1+i*4],[-52+i*22,-5+i*4]],'#778675');}
  for(let i=0;i<3;i++)line(c,-23+i*10,-39+i*3,-12+i*10,-36+i*3,'#495f45',2);
 }
 if(look){
  // Armor skirts and frontal slabs distinguish variants even when zoomed out.
  const heavy=variant==='siege'||variant==='ironclad';
  for(let i=0;i<(heavy?5:4);i++){const x=-61+i*24,y=-10+i*4;poly(c,[[x,y-7],[x+20,y-3],[x+18,y+13],[x-2,y+8]],paint);line(c,x,y-6,x+19,y-2,look.edge,2);line(c,x+2,y+4,x+16,y+7,'#293a39',1.5)}
  poly(c,[[37,-33],[73,-18],[78,-4],[59,4],[34,-10]],paint);line(c,39,-30,71,-16,look.edge,3);
  if(variant==='veteran'){for(let i=0;i<3;i++)line(c,39+i*9,-22+i*3,42+i*9,-13+i*3,look.accent,3);poly(c,[[-12,-44],[-5,-41],[-12,-37],[-19,-40]],look.accent)}
  else if(variant==='siege'){poly(c,[[-74,-22],[-79,-36],[-44,-48],[-19,-36],[-20,-18]],paint);line(c,-76,-34,-44,-45,look.edge,3);for(let i=0;i<3;i++)line(c,44+i*8,-24+i*3,49+i*8,-17+i*3,look.accent,3);line(c,-29,-46,-14,-40,look.accent,5)}
  else if(variant==='ironclad'){for(let i=0;i<3;i++)poly(c,[[9+i*18,-38+i*5],[23+i*18,-34+i*5],[24+i*18,-25+i*5],[10+i*18,-29+i*5]],look.edge);line(c,-22,-45,-7,-39,look.accent,5);poly(c,[[-5,-37],[1,-40],[7,-35],[1,-31]],look.accent)}
  else {line(c,-36,-42,-14,-35,look.accent,4);poly(c,[[22,-38],[36,-33],[38,-26],[24,-31]],'#253d4b');line(c,25,-35,34,-32,look.accent,2);line(c,-51,-18,-25,-11,look.edge,3)}
 }
 // Visible rear access and armor seams make flank attacks readable.
 line(c,-w,-17,-w,0,'#c3b78e',2);line(c,-w+2,-12,-w*.39,7,'#465945',2);
 ellipse(c,w-6,-3,3.2,4,'#e9e4ba');ellipse(c,w*.65,9,3,3.5,'#e9e4ba');
 ellipse(c,-w+2,-7,2.5,3,'#9d5647');
};
P.drawVehicle=function(c,v){
 const kind=vehicleKind(v);if(!militaryVehicles.has(kind)){civilianVehicle.call(this,c,v);return}
 const tank=kind==='tank',truck=kind==='truck',flip=Math.cos(v.dir||0)<0?-1:1,w=tank?76:truck?67:57,look=vehicleLook(v),descriptor=look?window.ATSVehicleVariants?.[v.variant]:null,scale=look?clamp(descriptor?.visualScale||look.scale,1,1.1):1,key=look?kind+':'+v.variant:kind;
 this.vehicleSprites=this.vehicleSprites||new Map();let sprite=this.vehicleSprites.get(key);
 if(!sprite&&this.actorSpriteBudget>0){this.actorSpriteBudget--;sprite=document.createElement('canvas');sprite.width=230;sprite.height=150;const sc=sprite.getContext('2d');sc.translate(115,112);this.drawMilitaryHull(sc,kind,look?v.variant:null);this.cacheSet(this.vehicleSprites,key,sprite,8)}
 c.save();c.scale(scale,scale);c.save();c.scale(flip,1);if(sprite)c.drawImage(sprite,-115,-112);else this.drawMilitaryHull(c,kind,look?v.variant:null);
 const moving=v.moving&&!this.reducedMotion,phase=this.time*9+(v.phase||0);
 if(tank&&moving&&this.detailLevel<3){for(let i=0;i<7;i++){const x=-66+((i*18+phase*5)%126);line(c,x,5+x*.15,x+4,12+x*.15,'#9d9a78',1.5)}}
 if(v.dismounting||v.unloadTimer>0&&v.troops<v.capacity){const open=v.dismounting?1:.4;
  poly(c,[[-w,-18],[-w-17*open,-31],[-w-16*open,-8],[-w,1]],'#93a082');
  poly(c,[[-w,-18],[-w+13*open,-12],[-w+13*open,10],[-w,1]],'#778c71');
  poly(c,[[-w,-14],[-w+8,-10],[-w+8,2],[-w,0]],'#142d27');
  line(c,-w,1,-w-23,11,'#b6b293',5);
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
 if(!truck){const dir=v.turretDir??v.dir??0,barrel=projectedDirection(dir,(tank?78:kind==='ifv'?48:34)*(descriptor?.barrelScale||look?.barrel||1)),baseX=3*flip,baseY=tank?-57:-56,heavy=look&&v.variant!=='veteran'&&tank;
  ellipse(c,baseX,baseY,tank?(heavy?33:28):16,tank?(heavy?17:14):9,look?.top||(tank?'#8f9a72':'#9aa287'));
  poly(c,[[baseX-(heavy?25:19),baseY],[baseX-(heavy?23:17),baseY-11],[baseX+7,baseY-(heavy?21:16)],[baseX+(heavy?29:23),baseY-2],[baseX+19,baseY+7]],descriptor?.paint||look?.paint||(tank?'#667d57':'#557655'));
  if(look){line(c,baseX-16,baseY-11,baseX+7,baseY-16,look.edge,2.5);line(c,baseX+9,baseY-13,baseX+20,baseY-8,look.accent,3);if(v.variant==='siege'){poly(c,[[baseX-32,baseY-6],[baseX-29,baseY-18],[baseX-16,baseY-13],[baseX-17,baseY+3]],look.paint);line(c,baseX-28,baseY-16,baseX-18,baseY-12,look.edge,2)}if(v.variant==='ironclad'){poly(c,[[baseX-25,baseY],[baseX-22,baseY-15],[baseX-10,baseY-12],[baseX-10,baseY+7]],look.paint);ellipse(c,baseX-16,baseY-14,3,2,look.accent)}}
  line(c,baseX,baseY-5,baseX+barrel.x,baseY-5+barrel.y,look?.edge||'#c0c0a0',tank?(heavy?11:9):kind==='ifv'?6:4);
  line(c,baseX+barrel.x*.84,baseY-5+barrel.y*.84,baseX+barrel.x,baseY-5+barrel.y,'#304632',tank?(heavy?13:10):6);
  if(look&&v.variant==='ironclad')for(const t of [.38,.58,.78]){const length=Math.hypot(barrel.x,barrel.y)||1,nx=-barrel.y/length*6,ny=barrel.x/length*6,x=baseX+barrel.x*t,y=baseY-5+barrel.y*t;line(c,x-nx,y-ny,x+nx,y+ny,look.accent,3)}
  if(look&&v.variant==='siege'){const x=baseX+barrel.x*.92,y=baseY-5+barrel.y*.92;ellipse(c,x,y,7,5,look.paint,Math.atan2(barrel.y,barrel.x));line(c,x-2,y-4,x+2,y+4,look.accent,2)}
  line(c,-15*flip,baseY-7,-15*flip,baseY-40,'#b0b79b',1.5);ellipse(c,10*flip,baseY-10,6,3,'#c9d4ae');this.glow(c,10*flip,baseY-10,13,'238,225,163',.2);
  if(weapon>=70){line(c,baseX+6,baseY-12,baseX+18,baseY+4,'#352f2a',3);line(c,baseX+19,baseY-10,baseX+4,baseY,'#dbc183',1.5)}
  if(v.cannonFlash>0){this.glow(c,baseX+barrel.x,baseY-5+barrel.y,42,'255,205,114',.72);ellipse(c,baseX+barrel.x,baseY-5+barrel.y,12,7,'#ffeab9')}
 }
 if(v.overrun){if(!this.reducedMotion)this.glow(c,0,-38,67,'203,123,62',.08)}

 if(v.hp>0&&v.maxHp&&v.hp<v.maxHp)this.health(c,v.hp/v.maxHp,tank?-92:-88,tank?64:49,'#dcaf7e');
 c.restore();
};
P.drawObject=function(c,o){
 if(o.type==='vehicle'&&o.vehicleType&&!o.dead){this.drawVehicle(c,o);return}
 previousObject.call(this,c,o);if(o.dead)return;
 if(o.commandCenter){
  line(c,6,-73,6,-123,'#b1bca8',3);ellipse(c,6,-120,24,6,'#8ba391',-.35);line(c,-11,-122,22,-132,'#d3c9a2',2);line(c,-26,-54,-26,-89,'#a5b39b',2);poly(c,[[-26,-89],[-5,-82],[-7,-70],[-26,-77]],'#c8ba83');
  poly(c,[[-23,-32],[-6,-27],[-6,-14],[-23,-18]],'#bcc39d');line(c,-18,-26,-10,-22,'#445e43',2);line(c,-14,-29,-14,-19,'#445e43',2);
 }else if(o.repairBay){
  line(c,-47,5,-47,-36,'#a7ac88',4);line(c,-47,-36,-20,-26,'#a7ac88',4);line(c,-20,-26,-20,-9,'#a7ac88',2);line(c,-26,-9,-14,-9,'#d4c392',3);
  for(let i=0;i<3;i++)ellipse(c,26+i*8,8+i*3,6,7,'#20332b');
 }
};
P.drawHuman=function(c,h){
 const size=h.role==='juggernaut'?1.1:h.role==='assault'?1.04:1;c.save();c.scale(size,size);previousHuman.call(this,c,h);const flip=Math.cos(h.dir||0)<0?-1:1;c.save();c.scale(flip,1);
 if(['rifleman','ranger','heavy','engineer','leader','mortar','assault','commando','juggernaut'].includes(h.role)){
  const helmet=h.role==='juggernaut'?'#45545b':h.role==='assault'?'#2e493c':h.role==='commando'?'#394d3f':h.role==='ranger'?'#405e50':'#465d47';
  poly(c,[[-9,-45],[-6,-51],[7,-51],[10,-44],[8,-40],[-8,-41]],helmet);line(c,-7,-45,8,-45,'#a1ac88',2);
  poly(c,[[-11,-31],[10,-31],[11,-16],[-10,-16]],helmet);line(c,-8,-25,7,-25,'#a7a885',1.5);line(c,-8,-21,7,-21,'#879575',1.5);
 }
 if(h.role==='rifleman'){line(c,17,-29,43,-28,'#a7b596',2);line(c,24,-31,33,-31,'#344832',3)}
 else if(h.role==='ranger'){poly(c,[[-10,-48],[10,-46],[11,-42],[-10,-43]],'#7a9271');line(c,-14,-28,-14,-14,'#879775',4);line(c,4,-40,9,-39,'#263e2f',2)}
 else if(h.role==='heavy'){poly(c,[[-15,-30],[-9,-32],[-8,-16],[-15,-14]],'#778b63');line(c,-11,-29,9,-17,'#c2b173',3);for(let i=0;i<4;i++)line(c,-8+i*5,-28+i*3,-10+i*5,-23+i*3,'#5d6448',1.3);line(c,23,-28,48,-27,'#95a27d',4);line(c,37,-25,35,-8,'#5e795d',2)}
 else if(h.role==='engineer'){poly(c,[[-16,-29],[-8,-26],[-8,-9],[-16,-13]],'#a39369');line(c,-18,-35,-10,-14,'#c7c69e',3);line(c,-22,-36,-14,-39,'#a4b59a',4);line(c,4,-25,10,-21,'#d0be7d',3)}
 else if(h.role==='leader'){line(c,-5,-47,7,-47,'#d5c583',3);poly(c,[[4,-28],[10,-26],[7,-22]],'#ddc787');line(c,-15,-28,-15,-57,'#a9b89a',2)}
 else if(h.role==='mortar'){ellipse(c,20,5,18,6,'#485c40');line(c,16,2,27,-34,'#a4ac86',7);line(c,14,2,3,-4,'#718361',3);line(c,17,2,35,7,'#718361',3);ellipse(c,27,-34,5,3,'#283c2e');poly(c,[[-18,-29],[-10,-27],[-10,-12],[-18,-15]],'#8a9067')}
 else if(h.role==='assault'){poly(c,[[-14,-34],[-8,-36],[-6,-25],[-14,-23]],'#547260');poly(c,[[8,-35],[15,-31],[14,-22],[7,-25]],'#547260');poly(c,[[-8,-31],[8,-31],[7,-18],[-7,-18]],'#294438');line(c,-7,-29,6,-29,'#a4b69a',2);line(c,-7,-25,6,-25,'#738b6e',2);poly(c,[[-8,-46],[9,-45],[9,-39],[-6,-39]],'#233c33');line(c,1,-43,8,-42,'#b4c5a2',2);line(c,22,-28,45,-27,'#809482',3);line(c,-4,-16,-5,-8,'#536a56',4);line(c,6,-16,7,-8,'#536a56',4)}
 else if(h.role==='commando'){poly(c,[[-10,-46],[9,-46],[9,-42],[-10,-42]],'#4c624d');poly(c,[[-7,-40],[7,-40],[6,-35],[-5,-36]],'#2c4036');ellipse(c,5,-43,3,2,'#c3dbaa');poly(c,[[-14,-32],[-9,-30],[-10,-13],[-16,-16]],'#333f34');line(c,-9,-30,5,-17,'#8f9c71',2);line(c,25,-29,50,-28,'#a4b693',2);line(c,25,-33,36,-33,'#2f473b',3);ellipse(c,32,-33,3,2,'#c3dbaa');line(c,45,-28,52,-28,'#31463a',4);line(c,-14,-32,-14,-55,'#879d81',1.5)}
 else if(h.role==='juggernaut'){poly(c,[[-15,-33],[-8,-38],[9,-36],[16,-28],[13,-12],[-13,-12]],'#61747b');poly(c,[[-8,-32],[7,-31],[8,-19],[-8,-19]],'#3d535e');line(c,-7,-30,7,-29,'#abc0c4',2);poly(c,[[-18,-34],[-9,-36],[-9,-24],[-18,-23]],'#7d8c90');poly(c,[[9,-35],[18,-30],[18,-22],[10,-25]],'#7d8c90');poly(c,[[-10,-48],[-5,-54],[8,-53],[12,-45],[9,-37],[-8,-39]],'#44575d');poly(c,[[-5,-46],[10,-45],[9,-40],[-5,-41]],'#e1a267');line(c,-3,-44,8,-43,'#f3d0a0',1.5);line(c,-12,-16,-12,-6,'#75868b',5);line(c,11,-16,11,-6,'#75868b',5);line(c,-9,-31,9,-17,'#c4a777',4);for(let i=0;i<4;i++)line(c,-7+i*4,-30+i*3,-8+i*4,-25+i*3,'#e1c18b',1.5);line(c,21,-28,49,-27,'#9cadab',5);ellipse(c,21,-21,5,6,'#4b635d');line(c,37,-25,34,-6,'#778c89',2);line(c,46,-27,52,-27,'#384e4c',5)}
 c.restore();
 if(h.constructing){this.health(c,1-clamp(h.constructing.remaining/3.5,0,1),-69,29,'#d1c38b');}c.restore();
};
P.drawApe=function(c,a,king,exposure){
 const wallClimbing=!!a.climbingWallId&&a.wallClimbUntil>this.time,onWall=!!a.onWallId,climbing=!!a.climbingVehicleId&&a.climbUntil>this.time||wallClimbing||onWall,stagger=a.staggerUntil>this.time||a.knockbackUntil>this.time;
 if(!climbing&&!stagger){previousApe.call(this,c,a,king,exposure);return}
 c.save();if(climbing){c.translate(0,-(wallClimbing||onWall?clamp(a.wallClimbHeight||35,0,150):king?27:32));if(!this.reducedMotion&&!onWall)c.rotate(Math.sin(this.time*9+(a.phase||0))*.08)}
 else{c.translate(0,7);c.rotate((Math.cos(a.dir||0)<0?-1:1)*.33)}
 previousApe.call(this,c,a,king,exposure);
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
 previousThreats.call(this,game);const c=this.ctx,z=this.camera.zoom;
 for(const mortar of game.forces?.hazards||[]){if(mortar.type!=='mortar'&&mortar.type!=='airstrike')continue;const p=this.project(mortar.x,mortar.y),r=mortar.radius||100;if(!this.visible(p,(r*ISO_RADIUS_X+30)*z))continue;const progress=clamp((game.time-mortar.start)/(mortar.fuse||2.1),0,1);c.save();c.translate(p.x,p.y);c.scale(z,z);ellipse(c,0,0,r*ISO_RADIUS_X,r*ISO_RADIUS_Y,'rgba(242,139,75,.14)');c.beginPath();c.ellipse(0,0,r*ISO_RADIUS_X,r*ISO_RADIUS_Y,0,0,Math.PI*2);c.strokeStyle='#ffb56f';c.lineWidth=2.5;c.stroke();c.beginPath();c.ellipse(0,0,r*ISO_RADIUS_X,r*ISO_RADIUS_Y,0,-Math.PI*.5,-Math.PI*.5+Math.PI*2*progress);c.strokeStyle='#ffe4a5';c.lineWidth=4;c.stroke();line(c,-11,0,11,0,'#ffe4a5',2);line(c,0,-8,0,8,'#ffe4a5',2);c.restore()}
 for(const v of game.vehicles||[]){const target=v.cannonTarget;if(!target||v.hp<=0)continue;const from=this.renderPoint(v,56),p=this.project(target.x,target.y),r=(vehicleLook(v)&&window.ATSVehicleVariants?.[v.variant]?.cannonRadius)||window.ATSVehicleSpecs?.[vehicleKind(v)]?.cannonRadius||(vehicleKind(v)==='ifv'?60:104);if(!this.segmentVisible(from,p,r*ISO_RADIUS_X*z))continue;
  const progress=clamp((game.time-target.start)/(target.duration||1.8),0,1);c.save();
  c.setLineDash([8,7]);line(c,from.x,from.y,p.x,p.y,`rgba(244,167,109,${.25+progress*.3})`,1.5*z);c.setLineDash([]);c.translate(p.x,p.y);c.scale(z,z);
  ellipse(c,0,0,r*ISO_RADIUS_X,r*ISO_RADIUS_Y,'rgba(234,128,79,.12)');c.beginPath();c.ellipse(0,0,r*ISO_RADIUS_X,r*ISO_RADIUS_Y,0,0,Math.PI*2);c.strokeStyle='#f1b176';c.lineWidth=2.2;c.stroke();
  c.beginPath();c.ellipse(0,0,r*ISO_RADIUS_X,r*ISO_RADIUS_Y,0,-Math.PI*.5,-Math.PI*.5+Math.PI*2*progress);c.strokeStyle='#ffe0a8';c.lineWidth=4;c.stroke();
  for(const side of [-1,1]){line(c,side*13,0,side*27,0,'#ffe0a8',2);line(c,0,side*6,0,side*14,'#ffe0a8',2)}
  c.restore();
 }
 for(const shell of game.forces?.hazards||[]){if(shell.type!=='shell')continue;const p=this.project(shell.x,shell.y,20),dir=Math.atan2(shell.targetY-shell.fromY,shell.targetX-shell.fromX),d=projectedDirection(dir,36),q={x:p.x-d.x*z,y:p.y-d.y*z};if(!this.segmentVisible(p,q,25*z))continue;
  c.save();c.globalCompositeOperation='screen';line(c,q.x,q.y,p.x,p.y,'rgba(255,192,98,.75)',3*z);ellipse(c,p.x,p.y,5*z,3*z,'#ffefc1');this.glow(c,p.x,p.y,14*z,'244,196,113',.55);c.restore();
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
// Village geometry is shared across residents and cached by building state.
// Paths live on the ground layer; huts and work sites retain depth sorting.
const livingObject=P.drawObject,livingHut=P.drawHut,livingApe=P.drawApe,livingApeSprite=P.drawApeSprite,livingHuman=P.drawHuman,livingThreats=P.drawThreats;
P.drawSettlementGround=function(game,view){
 const c=this.ctx,z=this.camera.zoom;
 for(const s of game.settlements||[]){if(Math.hypot(s.x-game.king.x,s.y-game.king.y)>view+(s.radius||100))continue;
  const p=this.project(s.x,s.y),r=s.developedRadius||s.radius||100;c.save();c.translate(p.x,p.y);c.scale(z,z);
  ellipse(c,0,0,r*.8*Math.SQRT2*.82,r*.42*Math.SQRT2*.82,'rgba(124,107,65,.075)');
  ellipse(c,0,0,Math.min(r*.32,115)*1.13,Math.min(r*.32,115)*.59,'rgba(152,125,77,.12)');c.restore();
  c.save();c.lineCap='round';for(const path of s.paths||[]){const points=path.points||[path.from,path.to];if(!points[0]||!points[1])continue;c.beginPath();for(let i=0;i<points.length;i++){const q=this.project(points[i].x,points[i].y);i?c.lineTo(q.x,q.y):c.moveTo(q.x,q.y)}c.lineWidth=(path.width||12)*z;c.strokeStyle='rgba(142,119,76,.28)';c.stroke();c.lineWidth=2*z;c.strokeStyle='rgba(191,154,94,.14)';c.stroke();}c.restore();
 }
};
P.drawConstruction=function(c,project){
 const stage=clamp(project.stage||0,0,3),kind=project.kind||'hut',progress=clamp(project.progress||0,0,1),tower=kind==='spearTower',training=kind==='training';
 ellipse(c,0,2,tower?36:training?49:34,tower?16:training?23:14,'rgba(149,115,61,.22)');
 for(const x of [-23,23]){line(c,x,5,x,-9,'#c7af79',3);line(c,x,-8,x+7,-11,'#aa9565',2)}
 if(tower){
  for(let i=0;i<3;i++){line(c,-34+i*5,10-i*3,-8+i*4,20-i*3,'#8f7550',5);ellipse(c,-8+i*4,20-i*3,3,3,'#c4a674')}
  if(stage>=1){for(const [x,y]of[[-21,0],[21,0],[-10,-10],[10,-10]])line(c,x,y,x*.72,y-84,'#a9976a',5);for(const lift of[18,47,75])line(c,-21,-lift,21,-lift,'#bca77b',2)}
  if(stage>=2){line(c,-21,0,15,-80,'#7c7957',3);line(c,21,0,-15,-80,'#7c7957',3);poly(c,[[-29,-81],[0,-97],[29,-81],[0,-66]],'#a8996e');for(let i=0;i<8;i++)line(c,-5,-i*10,5,-i*10,'#d0b681',2);line(c,-29,-81,-29,-99,'#aa9569',3);line(c,29,-81,29,-99,'#aa9569',3)}
  if(stage>=3){poly(c,[[-34,-104],[0,-125],[34,-104],[0,-87]],'#8b9565');line(c,-29,-82,29,-82,'#d1bd83',3);line(c,-35,1,-35,-101,'#7c7958',2);line(c,35,1,35,-101,'#7c7958',2);for(const lift of[25,55,85])line(c,-35,-lift,35,-lift,'#998866',2)}
 }else if(training){
  poly(c,[[-47,0],[0,-23],[47,0],[0,24]],'#6c6542');
  if(stage>=1){for(const x of[-31,31])line(c,x,0,x,-50,'#a39469',4);line(c,-31,-48,31,-48,'#c8b47e',4);line(c,-31,0,27,-45,'#8b7f59',2)}
  if(stage>=2){poly(c,[[-40,-50],[0,-72],[40,-50],[0,-30]],'#81915c');line(c,-15,-2,-15,-31,'#aa9165',4);line(c,23,4,23,-27,'#aa9165',4);ellipse(c,-15,-28,9,13,'#b5a779')}
  if(stage>=3){for(const x of[-17,23]){ellipse(c,x,-29,11,14,'#c7b886');ellipse(c,x,-29,7,9,'#8c6b49');ellipse(c,x,-29,3,4,'#c5a768')}line(c,-44,7,-37,-31,'#b09a69',3);line(c,-37,-31,-34,-41,'#9eaf99',4)}
 }else{
  if(stage>=1){for(const [x,y]of [[-22,0],[0,13],[23,0],[0,-12]])line(c,x,y,x,y-29,'#aa9569',4);line(c,-22,-29,0,-16,'#d0b985',3);line(c,0,-16,23,-29,'#b29d71',3)}
  if(stage>=2){line(c,-22,-29,0,-48,'#c1aa78',3);line(c,0,-48,23,-29,'#c1aa78',3);line(c,0,-48,0,-16,'#c1aa78',3);line(c,-20,0,20,-27,'#857450',2)}
  if(stage>=3)poly(c,[[-29,-29],[0,-51],[29,-28],[0,-9]],'#79845a');
 }
 if((project.treeIds||[]).length)line(c,-30,5,-13,-4,'#b49a6b',4);
 this.health(c,progress,tower?26:training?32:20,38,'#b6c88a');
};
P.drawHut=function(c,h){
 if(h.stage!==undefined&&h.stage<4){this.drawConstruction(c,{...h,kind:'hut'});return;}
 const ratio=(h.hp||0)/(h.maxHp||100),damage=ratio<=0?3:ratio<.3?2:ratio<.65?1:0,variant=h.variant??(idHash(h.id)>>>0)%4,key=variant+':'+damage;
 this.hutSprites=this.hutSprites||new Map();let sprite=this.hutSprites.get(key);
 if(!sprite&&this.actorSpriteBudget>0){this.actorSpriteBudget--;sprite=document.createElement('canvas');sprite.width=104;sprite.height=104;const sc=sprite.getContext('2d');sc.translate(52,80);sc.scale(variant===1?1.12:variant===2?.92:1,variant===3?1.1:1);livingHut.call(this,sc,{id:'hut',hp:damage===3?0:100,maxHp:100});if(damage<3){if(variant===1){line(sc,-22,-25,0,-47,'#a89b6f',2);line(sc,0,-47,24,-25,'#b6a977',2)}if(variant===2){ellipse(sc,17,-14,5,4,'#c6b579');line(sc,17,-19,17,-10,'#384c35',1)}if(damage>0){line(sc,-11,-35,1,-25,'#4d3f2e',3);line(sc,1,-25,-3,-15,'#4d3f2e',2)}if(damage===2)poly(sc,[[4,-43],[16,-35],[9,-20],[-2,-29]],'#25392d');}this.cacheSet(this.hutSprites,key,sprite,20);}
 if(sprite)c.drawImage(sprite,-52,-80);else livingHut.call(this,c,h);
 if(ratio>0&&ratio<1)this.health(c,ratio,-64,33,ratio<.3?'#e6a07b':'#c4c894');
 if(this.time-(h.lastHit??-100)<.2){c.save();c.globalAlpha=.35;ellipse(c,0,-25,27,19,'#ecc48b');c.restore();}
};
P.drawSettlement=function(c,s){
 const level=clamp(Math.floor(s.level||1),1,10),scale=level>=7?1.28:level>=5?1.13:1;
 this.lodgeSprites=this.lodgeSprites||new Map();let sprite=this.lodgeSprites.get(level);
 if(!sprite&&this.actorSpriteBudget>0){this.actorSpriteBudget--;sprite=document.createElement('canvas');sprite.width=360;sprite.height=270;const sc=sprite.getContext('2d');sc.translate(180,225);baseSettlement.call(this,sc,{level,radius:45,food:0,known:false,attack:false});this.cacheSet(this.lodgeSprites,level,sprite,10);}
 c.save();c.scale(scale,scale);if(sprite)c.drawImage(sprite,-180,-225);else baseSettlement.call(this,c,{...s,radius:45,known:false,attack:false});c.restore();
 if(s.attack){const y=-(58+level*8)*scale;c.font='700 10px system-ui';c.textAlign='center';c.fillStyle='#eda282';c.fillText(s.name||'Ape village',0,y);c.fillText('UNDER ATTACK',0,y-15);this.glow(c,0,-35,100,'226,97,54',.12)}
 if(s.maxDefense>0)this.health(c,(s.defense||0)/s.maxDefense,26,55,'#9db899');
};
P.drawSettlementProp=function(c,o){
 if(o.stage!==undefined&&o.stage<4){this.drawConstruction(c,o);return}const kind=o.kind||o.type,variant=o.variant||0,ruin=o.hp<=0,key=kind+':'+variant+':'+ruin;
 this.villageSprites=this.villageSprites||new Map();let sprite=this.villageSprites.get(key);
 if(!sprite&&this.actorSpriteBudget>0){this.actorSpriteBudget--;sprite=document.createElement('canvas');sprite.width=184;sprite.height=208;const sc=sprite.getContext('2d');sc.translate(92,168);this.drawVillageGeometry(sc,kind,variant,ruin);this.cacheSet(this.villageSprites,key,sprite,48);}
 if(sprite)c.drawImage(sprite,-92,-168);else this.drawVillageGeometry(c,kind,variant,ruin);
 if(o.hp>0&&o.maxHp&&o.hp<o.maxHp)this.health(c,o.hp/o.maxHp,kind==='spearTower'?-140:kind==='training'?-85:-75,40,'#b7c895');if(kind==='spearTower'&&!ruin&&this.time-(o.lastShot??-10)<.2)this.glow(c,8,-91,24,'229,205,119',.25);if(kind==='cooking'&&!ruin&&this.detailLevel<3){const flicker=this.reducedMotion?0:Math.sin(this.time*9)*2;poly(c,[[-6,0],[-3,-13-flicker],[1,-9],[4,-20+flicker],[8,-2]],'#e4a151');ellipse(c,1,-4,4,5,'#f5d387');this.glow(c,1,-6,29,'236,169,77',.16);}
};
P.drawVillageGeometry=function(c,kind,variant,ruin){
 if(ruin){this.drawRubble(c,{id:kind+variant});return;}ellipse(c,0,3,32,13,'rgba(2,13,9,.25)');
 if(kind==='spearTower'){
  for(const [x,y]of[[-20,0],[20,0],[-10,-10],[10,-10]]){line(c,x,y,x*.72,y-86,'#8e7e56',6);line(c,x+1,y-4,x*.72+1,y-83,'#bcab7a',1.6)}
  line(c,-20,0,14,-83,'#67724b',4);line(c,20,0,-14,-83,'#67724b',4);poly(c,[[-30,-83],[0,-99],[30,-83],[0,-67]],'#b2a171');poly(c,[[-30,-83],[0,-67],[0,-59],[-30,-75]],'#766e47');poly(c,[[0,-67],[30,-83],[30,-75],[0,-59]],'#8f8154');
  for(let i=0;i<8;i++)line(c,-5,-8-i*10,6,-8-i*10,'#c3af77',2.3);for(const x of[-29,29]){line(c,x,-81,x,-104,'#c5b079',4);ellipse(c,x,-88,2.5,1.5,'#e1ca91')}
  line(c,-29,-95,29,-95,'#c4b280',3);poly(c,[[-37,-108],[0,-132],[37,-108],[0,-90]],'#7a8b53');poly(c,[[-37,-108],[0,-132],[0,-90]],'#a4ad70');line(c,0,-130,0,-101,'#bac285',1.4);
  for(const x of[-14,-6,9]){line(c,x,-78,x+8,-106,'#b9a36e',2);poly(c,[[x+8,-111],[x+5,-104],[x+11,-104]],'#cad3b4')}poly(c,[[27,-101],[40,-99],[37,-89],[27,-91]],'#90bfa3');line(c,27,-103,27,-87,'#b2a478',2);return;
 }
 if(kind==='training'){
  poly(c,[[-51,0],[0,-26],[52,0],[0,28]],'#796b42');ellipse(c,1,3,34,17,'#9a8852');c.beginPath();c.ellipse(1,3,34,17,0,0,Math.PI*2);c.strokeStyle='#c1ad76';c.lineWidth=2;c.stroke();
  for(const x of[-32,32])line(c,x,-4,x,-51,'#96825a',4);poly(c,[[-44,-51],[0,-76],[44,-51],[0,-30]],'#6c8654');poly(c,[[-44,-51],[0,-76],[0,-30]],'#9aaa6b');line(c,-32,-49,32,-49,'#c4b07b',3);
  for(const [x,y]of[[-17,-3],[23,6]]){line(c,x,y,x,y-36,'#a79163',4);line(c,x-13,y-27,x+13,y-27,'#bca97a',3);ellipse(c,x,y-30,10,13,'#d2c18a');ellipse(c,x,y-30,6.8,9,'#916b4c');ellipse(c,x,y-30,3,4,'#e3b982');line(c,x,y-45,x,y-40,'#a48a5a',3);ellipse(c,x,y-46,4.5,4.5,'#c6b486')}
  for(const x of[-47,-40,-33]){line(c,x,8,x+8,-31,'#b29b68',2);poly(c,[[x+9,-37],[x+5,-29],[x+12,-29]],'#b9c7a6')}
  ellipse(c,48,5,10,5,'#625341');ellipse(c,48,2,10,4,'#b69a67');line(c,42,1,48,-8,'#c4b589',3);return;
 }
 if(kind==='garden'){poly(c,[[-39,0],[0,-21],[40,0],[0,22]],'#665a39');for(let row=0;row<4;row++)for(let col=0;col<5;col++){const x=(col-2)*10+(row-1.5)*7,y=(col-2)*4-(row-1.5)*5;ellipse(c,x,y,5,3,row%2?'#839354':'#647e4d');line(c,x,y,x+2,y-9,'#b0b976',1.6);}return;}
 if(kind==='cooking'){for(let i=0;i<8;i++){const a=i*Math.PI/4;ellipse(c,Math.cos(a)*17,Math.sin(a)*7,5,3,'#8a8e76')}line(c,-15,-2,13,-8,'#9b7c50',5);for(const x of [-24,24])line(c,x,1,x*.4,-43,'#8f805d',3);line(c,-24,-27,24,-27,'#b7a16e',3);poly(c,[[-11,-20],[-8,-8],[9,-8],[12,-20]],'#334440');ellipse(c,0,-20,12,5,'#829182');ellipse(c,0,-21,8,2,'#ae9361');for(const x of[-38,38]){ellipse(c,x,3,9,6,'#aa8960');ellipse(c,x,-1,8,3,'#d0b878')}return;}
 if(kind==='wood'||kind==='lumber'){for(let row=0;row<3;row++)for(let col=0;col<4-row;col++){const x=(col-(3-row)/2)*13,y=4-row*8;line(c,x-16,y-7,x+12,y+7,'#87714e',7);ellipse(c,x+12,y+7,4,4,'#c5aa72');ellipse(c,x+12,y+7,2,2,'#8d7147')}return;}
 if(kind==='lookout'){for(const x of [-18,18]){line(c,x,0,x,-80,'#968466',5);line(c,x,0,-x,-74,'#716d4d',3)}poly(c,[[-26,-76],[0,-89],[26,-76],[0,-63]],'#b2a47b');for(let i=0;i<6;i++)line(c,-5,-i*10,6,-i*10,'#bba67a',2);line(c,-25,-76,-25,-93,'#978d68',3);line(c,25,-76,25,-93,'#978d68',3);poly(c,[[-29,-97],[0,-116],[29,-97],[0,-85]],'#758450');return;}
 if(kind==='communal'||kind==='gathering'){for(let i=0;i<6;i++){const a=i*Math.PI/3;line(c,Math.cos(a)*25-7,Math.sin(a)*12,Math.cos(a)*25+7,Math.sin(a)*12+3,'#8e7955',6)}return;}
 // Storehouses and work shelters share a timber frame, with different cargo.
 for(const x of[-30,30])line(c,x,0,x,-38,'#a29265',4);poly(c,[[-42,-39],[0,-67],[42,-39],[0,-15]],kind==='storage'?'#978b55':'#78834f');poly(c,[[-42,-39],[0,-67],[0,-15]],'#a4a36a');line(c,-30,-37,30,-37,'#b6a177',3);if(kind==='storage'||kind==='food'){for(const x of[-17,3,22]){ellipse(c,x,2,10,7,'#a48856');ellipse(c,x,-4,9,4,'#d0b572');for(let i=0;i<3;i++)ellipse(c,x-4+i*4,-5-(i%2)*3,2.8,2,'#aaba76')}}else{line(c,-23,-6,23,4,'#9a8256',6);line(c,-14,-10,-7,-21,'#b7aa79',3);}
};
P.drawObject=function(c,o){
 if(o.type==='tree'&&o.dead&&o.clearedBy){ellipse(c,0,2,13,5,'rgba(13,24,13,.24)');poly(c,[[-6,0],[-6,-8],[6,-8],[6,0]],'#6c6549');ellipse(c,0,-8,6,3,'#b2a273');line(c,0,-8,2,-7,'#6f714d',1);return;}
 if(o.type==='vehicleWreck'){c.save();c.globalAlpha*=.52;c.scale(Math.cos(o.dir||0)<0?-1:1,1);if(['tank','apc','ifv','truck'].includes(o.kind))this.drawMilitaryHull(c,o.kind);else civilianVehicle.call(this,c,{kind:o.kind,hp:0,dir:0});line(c,-27,-22,12,-7,'#241e19',6);line(c,-8,-34,20,-16,'#1c2621',5);c.restore();return;}
 if(o.fortification){if(o.kind==='searchlight'){line(c,-10,3,0,-49,'#9ea78e',3);line(c,10,3,0,-49,'#697d70',3);poly(c,[[-10,-54],[9,-58],[14,-48],[-8,-45]],o.dead?'#546258':'#a5b3a0');if(!o.dead)this.glow(c,9,-51,24,'238,226,169',.35);return;}if(o.kind==='observation'){this.drawVillageGeometry(c,'lookout',0,o.dead);return;}this.drawFieldBarrier(c,o);return;}
 return livingObject.call(this,c,o);
};
P.drawFieldBarrier=function(c,o){
 const damage=o.dead||o.hp<=0?3:o.hp/o.maxHp<.3?2:o.hp/o.maxHp<.65?1:0,w=o.w||74,h=o.h||16,key=o.team+':'+o.kind+':'+w+':'+h+':'+damage;
 this.barrierSprites=this.barrierSprites||new Map();let sprite=this.barrierSprites.get(key);
 if(!sprite&&this.actorSpriteBudget>0){this.actorSpriteBudget--;sprite=document.createElement('canvas');sprite.width=Math.ceil((w+h)*.8+32);sprite.height=Math.ceil((w+h)*.42+92);const sc=sprite.getContext('2d');sc.translate(sprite.width/2,sprite.height-24);this.drawBarrierGeometry(sc,o,damage);this.cacheSet(this.barrierSprites,key,sprite,72);}
 if(sprite)c.drawImage(sprite,-sprite.width/2,-sprite.height+24);else this.drawBarrierGeometry(c,o,damage);
 if(o.hp>0&&o.hp<o.maxHp)this.health(c,o.hp/o.maxHp,-51,42,damage===2?'#e9a17b':'#d4c596');
};
P.drawBarrierGeometry=function(c,o,damage){
 const w=o.w||74,h=o.h||16,axis=w>=h?0:Math.PI/2,length=Math.max(w,h),d=projectedDirection(axis,length/2),ape=o.team==='ape',height=o.kind==='heavy'?35:ape?38:28;
 ellipse(c,0,4,Math.abs(d.x)+11,Math.abs(d.y)+9,'rgba(0,10,9,.3)');
 if(damage===3){for(let i=0;i<5;i++){const t=i/4,x=(t-.5)*d.x*1.8,y=(t-.5)*d.y*1.8;line(c,x-9,y-4,x+12,y+4,ape?'#9c825c':'#798372',5);}return;}
 const count=Math.max(4,Math.ceil(length/12));for(let i=0;i<=count;i++){if(damage===2&&(i===2||i===3))continue;const t=i/count,x=-d.x+d.x*2*t,y=-d.y+d.y*2*t,top=height-(damage>0&&i%3===0?10:0);line(c,x,y,x+(ape?(i%2?2:-2):0),y-top,ape?'#a08b60':o.kind==='heavy'?'#91a08c':'#7f8c77',ape?7:10);if(ape)poly(c,[[x-4,y-top],[x,y-top-7],[x+4,y-top]],'#c3b482');else line(c,x-3,y-top+7,x+3,y-top+12,'#c7b879',2);}
 for(const lift of [8,height-8])line(c,-d.x,-d.y-lift,d.x,d.y-lift,ape?'#736f4b':'#b2b49a',3);
 if(damage>0){line(c,-8,-height+3,3,-height+13,'#35473a',2);line(c,3,-height+13,0,-8,'#35473a',2);}
};
P.drawSpears=function(game){
 const c=this.ctx,z=this.camera.zoom;let budget=128;
 for(const village of game.settlements||[]){const facilities=[...(village.facilities||[]),...(village.structures||[])];for(const spear of village.spears||[]){if(budget--<=0)return;if(spear.life<=0||![spear.x,spear.y,spear.fromX,spear.fromY,spear.targetX,spear.targetY].every(Number.isFinite))continue;
  const duration=Math.max(.05,spear.duration||Math.hypot(spear.targetX-spear.fromX,spear.targetY-spear.fromY)/(spear.speed||360)),elapsed=spear.elapsed??Math.max(0,game.time-(spear.start||0)),t=clamp(elapsed/duration,0,1),tower=facilities.find(f=>f.kind==='spearTower'&&(f.id===(spear.sourceId||spear.towerId)||Math.hypot(f.x-spear.fromX,f.y-spear.fromY)<3)),fromZ=spear.fromZ??(tower?92:27),targetZ=spear.targetZ??18,height=fromZ+(targetZ-fromZ)*t+Math.sin(t*Math.PI)*22,p=this.project(spear.x,spear.y,height),prior=this.project(spear.prevX??spear.fromX,spear.prevY??spear.fromY,height+2),from=this.project(spear.fromX,spear.fromY,fromZ),to=this.project(spear.targetX,spear.targetY,targetZ);
  let dx=p.x-prior.x,dy=p.y-prior.y;if(Math.hypot(dx,dy)<.1){dx=to.x-from.x;dy=to.y-from.y}const angle=Math.atan2(dy,dx),tail={x:p.x-Math.cos(angle)*25*z,y:p.y-Math.sin(angle)*25*z};if(!this.segmentVisible(p,tail,12*z))continue;c.save();c.translate(p.x,p.y);c.rotate(angle);c.scale(z,z);line(c,-25,0,-3,0,'#bda578',2.3);line(c,-23,-.6,-5,-.6,'#e1c798',.7);poly(c,[[5,0],[-3,-3.5],[-1,0],[-3,3.5]],'#dce0c1');line(c,-19,0,-24,-4,'#8db8a0',2);line(c,-19,0,-24,4,'#8db8a0',2);c.restore();
 }}
};
P.drawWorkerDetail=function(c,a){
 if(a.hp<=0)return;const activity=a.activity||'',working=/build|chop|clear|repair|garden|cook|breach/.test(activity),pulse=this.reducedMotion?0:Math.sin(this.time*7+(a.phase||0));
 if(a.carrying==='wood'){line(c,-18,-28,17,-20,'#806549',9);line(c,-18,-30,16,-22,'#b19767',3);ellipse(c,17,-20,4.5,4.2,'#dcc293',.2);ellipse(c,17,-20,2.5,2.5,'#927449',.2);ellipse(c,17,-20,1,1,'#d8be8d');line(c,-14,-35,14,-28,'#806c4e',6);ellipse(c,14,-28,3,3,'#c2a675');line(c,-14,-36,13,-30,'#b69764',1.3);}
 if(working&&this.detailLevel<3){line(c,12,-27,24+3*pulse,-36+5*pulse,'#b8a57a',3);line(c,19+3*pulse,-39+5*pulse,28+3*pulse,-34+5*pulse,'#809688',4);}
 if(activity==='grooming'||activity==='socializing')ellipse(c,19,-23,3,3,'#a4b394');
};
P.drawApe=function(c,a,king,exposure){c.save();if(!king&&(a.activity==='resting'||a.activity==='sleeping'||a.activity==='grooming')){c.translate(0,2);c.scale(1,.83)}if(a.animation?.kind==='vault'&&!this.reducedMotion){const t=clamp((this.time-a.animation.start)/a.animation.duration,0,1);c.translate(0,-Math.sin(t*Math.PI)*17)}livingApe.call(this,c,a.carrying==='wood'?{...a,carrying:false,_carryingWood:true}:a,king,exposure);if(!king)this.drawWorkerDetail(c,a);c.restore();};
P.drawApeSprite=function(c,a){if(livingApeSprite.call(this,c,a)!==false)this.drawWorkerDetail(c,a);};
P.drawHuman=function(c,h){livingHuman.call(this,c,h);if(h.engineerJob){line(c,12,-29,25,-19+Math.sin(this.time*9)*4,'#baa378',3);line(c,21,-21,29,-15,'#8a998a',4)}if(h.squadOrder==='Hold Line'&&this.camera.zoom>.9&&this.detailLevel<2)line(c,-8,7,8,7,'#9fb8a0',2);};
P.drawThreats=function(game){livingThreats.call(this,game);const c=this.ctx,z=this.camera.zoom;for(const h of game.humans||[]){const task=h.engineerJob||h.constructing;if(!task||h.hp<=0||!Number.isFinite(task.x)||!Number.isFinite(task.y))continue;const p=this.project(task.x,task.y);if(!this.visible(p,90*z))continue;const progress=clamp(1-(task.remaining||0)/(task.duration||5),0,.99);c.save();c.translate(p.x,p.y);c.scale(z,z);this.drawConstruction(c,{kind:'barrier',stage:Math.floor(progress*4),progress});c.restore();}};

// Blast motion uses the same actor art as ordinary play, with a ground shadow
// and a separate airborne body. Surviving apes visibly brace and stand again.
const groundApe=P.drawApe,groundCorpses=P.drawCorpses,ordinaryEffects=P.drawEffects;
P.drawApe=function(c,a,king,exposure){
 const reaction=a.blastReaction;if(!reaction||(a.hp<=0&&!(king&&reaction.stage==='flight')))return groundApe.call(this,c,a,king,exposure);
 const height=Math.max(0,reaction.height||a.blastZ||0);ellipse(c,0,3,Math.max(8,20-height*.05),6,'rgba(2,8,10,.35)');
 c.save();const bodyScale=king?1:(a.bodyScale||1);if(reaction.stage==='flight'){c.translate(0,-height);c.rotate(reaction.rotation||a.blastSpin||0);c.scale(bodyScale,bodyScale*.94);}
 else if(reaction.stage==='gettingUp'){const begin=reaction.getUpAt??reaction.start,end=reaction.recoverAt??begin+.8,t=clamp(Math.max(this.time-begin,reaction.recoveryElapsed||0)/Math.max(.01,reaction.recoveryDuration||end-begin),0,1),rise=t*t*(3-2*t);c.translate((1-rise)*12,5*(1-rise));c.rotate((1-rise)*1.24);c.scale(bodyScale,bodyScale*(.72+rise*.28));if(t>.3&&t<.8){const look=this.primateLook(a,king);line(c,15,-15,23,1,look.fur,look.arm*.65);this.drawPrimateHand(c,23,1,look,this.detailLevel>=3);}}
 baseApe.call(this,c,{...a,animation:null,attackTimer:0,hitTimer:0,moving:false,carrying:false},king,exposure);c.restore();
};
P.drawCorpses=function(game){
 const airborne=(game.corpses||[]).filter(a=>a.blastReaction?.stage==='flight'),ground=(game.corpses||[]).filter(a=>a.blastReaction?.stage!=='flight');groundCorpses.call(this,{...game,corpses:ground});
 const c=this.ctx,z=this.camera.zoom;for(const a of airborne){const p=this.project(a.x,a.y);if(!this.visible(p,230*z))continue;const reaction=a.blastReaction,height=Math.max(0,reaction.height||0);c.save();c.translate(p.x,p.y);c.scale(z,z);ellipse(c,0,2,Math.max(7,20-height*.05),6,'rgba(2,8,10,.32)');c.translate(0,-height);c.rotate(reaction.rotation||0);c.scale(a.bodyScale||1,a.bodyScale||1);c.globalAlpha=clamp(a.life/2,0,.95);const young=(a.actorAge??240)<20||a.actorState==='young',copy={...a,hp:100,maxHp:100,age:a.actorAge??240,state:young?'young':'fallen',moving:false,animation:null,attackTimer:0,hitTimer:0,carrying:false};
  if(a.type==='human'||a.type==='ape')drawCorpseBody(this,c,copy);else this.drawRubble(c,{id:a.id});c.restore();
 }
};
P.drawExplosion=function(c,e){
 const radius=e.radius||e.range||70,t=clamp(1-e.life/(e.maxLife||1.35),0,1),power=e.power||1,kind=e.blastKind||e.blastType||e.type,air=kind==='airstrike',grenade=kind==='grenade',count=this.detailLevel>=3?6:12,seed=idHash(e.id||kind+e.x+','+e.y),rx=radius*ISO_RADIUS_X,ry=radius*ISO_RADIUS_Y;
 const hot=air?'255,94,60':grenade?'255,198,72':'255,145,49',cool=air?'160,153,255':grenade?'125,215,171':'119,191,236';
 c.save();c.globalCompositeOperation='screen';this.glow(c,0,-14,Math.max(25,radius*(1.5-t)),hot,Math.max(0,.46*(1-t)));if(t<.32)this.glow(c,0,-18,radius*.6*(1-t),cool,.4*(1-t));
 const ring=.2+Math.min(1,t*2.3);c.beginPath();c.ellipse(0,0,rx*ring,ry*ring,0,0,Math.PI*2);c.strokeStyle=`rgba(${cool},${Math.max(0,(1-t)*.72)})`;c.lineWidth=Math.max(1,7*(1-t));c.stroke();c.restore();
 if(t<.7){const bloom=Math.sin(Math.PI*clamp(t/.7,0,1)),rise=16+t*radius*.4;for(let i=0;i<count;i++){const a=i/count*Math.PI*2,n=((seed>>>((i%4)*6))&63)/63,d=radius*(.1+t*.68)*(i%3===0?.9:.5),x=Math.cos(a)*d*.9,y=Math.sin(a)*d*.38-rise*(.4+n*.65),r=(12+radius*.16)*bloom*(.6+n*.4);ellipse(c,x,y,r,r*(.95+i%2*.2),i%3===0?'rgba(242,77,41,.78)':i%3===1?'rgba(255,162,46,.88)':'rgba(255,215,91,.88)',a*.1);if(t<.22)ellipse(c,x*.45,y*.6,r*.55,r*.7,'rgba(255,247,206,.96)');}}
 // Rising smoke remains colored by the fire below, while separate fast embers
 // trace the force of the blast. Their count obeys the current detail budget.
 for(let i=0;i<(this.detailLevel>=3?3:6);i++){const a=i*2.399+seed*.001,d=radius*.22*(.2+t),x=Math.cos(a)*d,y=Math.sin(a)*d*.32-25-t*(45+i*4),r=(10+radius*.13)*(.4+t);ellipse(c,x,y,r,r*.84,`rgba(${i%2?'117,102,104':'92,110,115'},${Math.min(.5,t*.8)*(1-t)})`);}
 for(let i=0;i<count;i++){const a=i/count*Math.PI*2+(seed%19)*.1,d=radius*(.25+t*1.4),x=Math.cos(a)*d*.85,y=Math.sin(a)*d*.42-Math.sin(t*Math.PI)*(18+(i%4)*10)*power;if(t<.85)line(c,x-Math.cos(a)*8*(1-t),y-Math.sin(a)*4,x,y,i%3===0?'#fff0b9':i%3===1?'#ffb44c':'#ff7359',Math.max(1,3*(1-t)));}
};
P.drawEffects=function(c,effects){
 const regular=[],colored=effects.filter(e=>e.type==='explosion');let budget=this.detailLevel>=3?8:20;
 for(const e of effects){if(!['explosion','treeFall','dust'].includes(e.type)){if((e.type==='tankImpact'||e.type==='cannonImpact')&&colored.some(b=>Math.abs(b.x-e.x)+Math.abs(b.y-e.y)<2))continue;regular.push(e);continue;}const p=this.project(e.x,e.y),r=e.radius||e.range||80;if(!this.visible(p,(r*1.8+120)*this.camera.zoom)||budget--<=0)continue;c.save();c.translate(p.x,p.y);c.scale(this.camera.zoom,this.camera.zoom);if(e.type==='explosion')this.drawExplosion(c,e);else{const t=clamp(1-e.life/(e.maxLife||1.2),0,1);c.globalAlpha=1-t;if(e.type==='dust'){for(let i=0;i<4;i++){const a=i/4*Math.PI*2;ellipse(c,Math.cos(a)*(8+t*20),Math.sin(a)*(4+t*8)-t*8,9+t*5,5+t*3,'rgba(182,171,141,.25)');}}else{c.rotate(t*.8);line(c,0,0,8,-60,'#8c8060',10);for(let i=0;i<3;i++)ellipse(c,-14+i*14,-61-i%2*12,20,15,'rgba(88,120,76,.6)');}}c.restore();}
 ordinaryEffects.call(this,c,regular);
};
})();
