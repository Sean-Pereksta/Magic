/* Original raster military library. Reads simulation state; never changes movement,
 * collision, detection, combat, corpse lifetime or save data. Load after other art. */
(() => {
  'use strict';
  const P=ATSRenderer.prototype,base={draw:P.draw,vehicle:P.drawVehicle,heli:P.drawHeli,object:P.drawObject,corpses:P.drawCorpses};
  const TAU=Math.PI*2,clamp=(v,a,b)=>Math.max(a,Math.min(b,v)),mix=(a,b,t)=>a+(b-a)*t;
  const art=()=>window.ATSVisualAssets,meta=()=>art()?.manifest?.vehicles;
  const headings=['east','southeast','south','southwest','west','northwest','north','northeast'];
  const sourceDirections=['east','southeast','south','southeast','east','northeast','north','northeast'];
  const widths={jeep:78,command:82,armored:88,truck:100,apc:104,ifv:112,tank:124,recon:136,scout:149,gunship:165};
  const filters={veteran:'sepia(.16) saturate(1.1)',siege:'sepia(.42) saturate(.82)',ironclad:'saturate(.32) brightness(.85)',sentinel:'hue-rotate(34deg) saturate(.7)',repeater:'hue-rotate(25deg)',bombard:'sepia(.38)',cyclone:'hue-rotate(50deg) saturate(.65)'};
  function direction(angle=0){
    const x=(Math.cos(angle)-Math.sin(angle))*.8,y=(Math.cos(angle)+Math.sin(angle))*.42;
    const sector=(Math.round(Math.atan2(y,x)/(Math.PI/4))+8)%8;
    return {sector,name:headings[sector],source:sourceDirections[sector],mirror:sector>=3&&sector<=5,x,y};
  }
  function damageState(a){
    if(a.hp<=0)return 'destroyed';
    if(a.engineDamage>=100||a.mobilityDamage>=100)return 'disabled';
    const ratio=a.hp/(a.maxHp||a.hp||1);
    if(ratio<.32||a.engineDamage>=65)return 'heavy';
    return ratio<.72||a.engineDamage>25||a.mobilityDamage>25?'light':'operational';
  }
  function resolve(a={},time=0,options={}){
    const heli=!!options.heli||a.id?.startsWith('heli'),kind=heli?(['recon','scout','gunship'].includes(a.kind)?a.kind:a.armed?'scout':'recon'):(a.vehicleClass||a.vehicleType||a.kind||'jeep');
    const d=direction(a.dir||0),state=damageState(a),turretKind=['repeater','bombard','cyclone'].includes(a.variant)?a.variant:kind==='ifv'?'ifv':kind==='tank'?'tank':null;
    const phase=Math.max(0,a.phase??5),startup=clamp(phase/2.4,0,1),landing=options.landing??1;
    const altitude=heli?(Number.isFinite(a.altitude)?Math.max(0,a.altitude):mix(22,125,startup)*landing):0;
    // Essential blade animation remains active on reduced-motion and Low presets.
    const rotorSpeed=clamp(a.rotorSpeed??(a.grounded?0:a.landed?.05:mix(.3,1,startup)),0,1);
    const rotorClock=time*(options.reducedMotion?7:17)*rotorSpeed,rotorFrame=((Math.floor(rotorClock+(a.phase||0))%16)+16)%16;
    return {kind,heli,direction:d,state,frame:(heli?'heli-':'')+kind+'-'+d.source,turret:turretKind?'turret-'+turretKind+'-'+direction(a.turretDir??a.dir??0).source:null,
      turretDirection:direction(a.turretDir??a.dir??0),width:widths[kind]||78,altitude,rotorFrame,tailFrame:(rotorFrame*3+Math.floor(time*5*rotorSpeed))%16,rotorSpeed,
      wheelPhase:options.reducedMotion||!a.moving?0:(time*(a.reversing?-11:11)+(a.phase||0))%TAU};
  }
  function frame(name){return meta()?.frames?.[name]}
  function ready(name){const f=frame(name);return f&&art()?.get(f.atlas)}
  function raster(r,name,variant,state){
    const f=frame(name),image=f&&art()?.get(f.atlas);if(!image)return null;
    const filter=[filters[variant]||'',state==='light'?'brightness(.87) saturate(.86)':state==='heavy'||state==='disabled'?'brightness(.61) saturate(.48)':''].join(' ').trim();
    if(!filter)return {image,f,sx:f.rect[0],sy:f.rect[1],sw:f.rect[2],sh:f.rect[3]};
    const cache=r.vehicleTextures||(r.vehicleTextures=new Map()),key=name+':'+(variant||'')+':'+state;
    let texture=cache.get(key);if(!texture){
      if(r.vehicleTextureBudget<=0)return {image,f,sx:f.rect[0],sy:f.rect[1],sw:f.rect[2],sh:f.rect[3]};
      r.vehicleTextureBudget--;
      const [x,y,w,h]=f.rect;texture=document.createElement('canvas');texture.width=w;texture.height=h;
      const c=texture.getContext('2d');c.filter=filter;c.drawImage(image,x,y,w,h,0,0,w,h);
      if(cache.size>=96)cache.delete(cache.keys().next().value);cache.set(key,texture);
    }
    return {image:texture,f,sx:0,sy:0,sw:f.rect[2],sh:f.rect[3]};
  }
  function stamp(r,c,name,scale=1,mirror=false,variant,state,opacity=1){
    const src=raster(r,name,variant,state);if(!src)return false;const {image,f,sx,sy,sw,sh}=src;
    c.save();c.globalAlpha*=opacity;c.scale(mirror?-scale:scale,scale);
    if(f.clip){c.beginPath();f.clip.forEach(([x,y],i)=>i?c.lineTo(x-f.anchor[0],y-f.anchor[1]):c.moveTo(x-f.anchor[0],y-f.anchor[1]));c.closePath();c.clip();}
    c.drawImage(image,sx,sy,sw,sh,-f.anchor[0],-f.anchor[1],sw,sh);c.restore();return true;
  }
  function motion(r,a,p){
    const states=r.vehicleMotion||(r.vehicleMotion=new WeakMap());let s=states.get(a),time=r.time||0;
    if(!s){s={frame:p.frame,mirror:p.direction.mirror,at:time,dir:a.dir||0,time,moving:!!a.moving,stopAt:-100,bank:0};states.set(a,s)}
    if(s.frame!==p.frame||s.mirror!==p.direction.mirror){s.previous=s.frame;s.previousMirror=s.mirror;s.frame=p.frame;s.mirror=p.direction.mirror;s.at=time}
    if(s.moving&&!a.moving)s.stopAt=time;
    const dt=clamp(time-s.time,0,.2),delta=Math.atan2(Math.sin((a.dir||0)-s.dir),Math.cos((a.dir||0)-s.dir));
    if(dt>0)s.bank=mix(s.bank,clamp(delta/dt,-1,1)*.075,1-Math.exp(-dt*6));
    s.dir=a.dir||0;s.time=time;s.moving=!!a.moving;return s;
  }
  function smoke(r,c,x,y,size,phase,fire=false){
    if(r.vehicleEffectBudget<=0)return;r.vehicleEffectBudget--;
    const name=fire?'fire':'smoke',f=art()?.manifest?.environment?.frames?.[name],im=f&&art()?.get(f.atlas);if(!im)return;
    const [sx,sy,w,h]=f.rect,t=((phase%1)+1)%1,scale=size/w;
    c.save();c.globalAlpha*=fire?.52:.27*(1-t);c.drawImage(im,sx,sy,w,h,x-f.anchor[0]*scale,y-f.anchor[1]*scale-t*19,w*scale,h*scale);c.restore();
  }
  function dust(r,c,x,y,size,phase){
    if(r.vehicleEffectBudget<=0)return;r.vehicleEffectBudget--;
    const f=art()?.manifest?.environment?.frames?.dust,im=f&&art()?.get(f.atlas);if(!im)return;
    const [sx,sy,w,h]=f.rect,t=((phase%1)+1)%1,s=size/w;
    c.save();c.globalAlpha*=.14*(1-t);c.drawImage(im,sx,sy,w,h,x-f.anchor[0]*s,y-f.anchor[1]*s,w*s,h*s);c.restore();
  }
  function shadow(r,c,width,opacity=.4,height=0){
    let image=r.vehicleShadow;if(!image){image=document.createElement('canvas');image.width=192;image.height=96;const s=image.getContext('2d');
      const g=s.createRadialGradient(96,48,4,96,48,83);g.addColorStop(0,'rgba(0,5,8,.8)');g.addColorStop(.55,'rgba(0,5,8,.48)');g.addColorStop(1,'rgba(0,5,8,0)');
      s.translate(0,24);s.scale(1,.5);s.fillStyle=g;s.fillRect(0,0,192,96);r.vehicleShadow=image;}
    const spread=1+height/230;c.save();c.globalAlpha*=opacity;c.drawImage(image,-width*spread*.65,-width*spread*.25,width*spread*1.3,width*spread*.65);c.restore();
  }
  function rotor(r,c,p,tail=false){
    const name=tail?'rotor-tail':'rotor-main',blur=tail?'rotor-tail-blur':'rotor-main-blur';
    if(!ready(name))return;
    const n=tail?p.tailFrame:p.rotorFrame,key=(tail?'tail:':'main:')+n,cache=r.vehicleRotors||(r.vehicleRotors=new Map());
    let image=cache.get(key);if(!image){
      image=document.createElement('canvas');image.width=image.height=192;const ctx=image.getContext('2d');ctx.translate(96,96);ctx.rotate(n/16*TAU);
      const f=frame(name);stamp(r,ctx,name,170/Math.max(f.rect[2],f.rect[3]));cache.set(key,image);
    }
    const size=tail?17:p.width*.87;
    c.save();if(!tail)c.scale(1,.49);
    if(p.rotorSpeed>.45){const f=frame(blur);if(f)stamp(r,c,blur,size/Math.max(f.rect[2],f.rect[3]),false,null,null,tail?.11:.08)}
    c.globalAlpha*=p.rotorSpeed>.45?.57:1;c.drawImage(image,-size*.5,-size*.5,size,size);c.restore();
  }
  function lights(r,c,a,p,size){
    if(r.vehicleLampBudget<=0||r.detailLevel>=4)return;
    const d=p.direction,front={x:d.x*size*.36,y:d.y*size*.36-10},braking=!a.moving&&a.hp>0;
    if(r.glow){r.vehicleLampBudget--;r.glow(c,front.x,front.y,6,'249,227,166',.58);
      if(braking&&p.state!=='disabled')r.glow(c,-front.x,-front.y-16,5,'244,65,37',.7);}
  }
  function firing(r,c,a,p,x,y){
    if(!(a.cannonFlash>0||a.gunFlashUntil>r.time||a.attackTimer>0)||r.reducedMotion)return;
    const d=p.turretDirection||p.direction,len=p.kind==='tank'?58:37,px=x+d.x*len,py=y+d.y*len;
    const f=art()?.manifest?.environment?.frames?.muzzle,im=f&&art()?.get(f.atlas);if(!im)return;
    c.save();c.translate(px,py);c.rotate(Math.atan2(d.y,d.x));const [sx,sy,w,h]=f.rect,s=(a.cannonFlash>0?41:24)/w;
    c.drawImage(im,sx,sy,w,h,-f.anchor[0]*s,-f.anchor[1]*s,w*s,h*s);c.restore();
  }
  P.draw=function(game,dt){
    this.vehicleGame=game;this.vehicleTextureBudget=4;this.vehicleEffectBudget=this.reducedMotion?0:this.detailLevel>=4?5:this.quality==='low'?10:30;this.vehicleLampBudget=this.quality==='low'?5:16;
    return base.draw.call(this,game,dt);
  };
  P.drawVehicle=function(c,a){
    const p=resolve(a,this.time||0,{reducedMotion:this.reducedMotion});if(!ready(p.frame)||p.turret&&!ready(p.turret))return base.vehicle.call(this,c,a);
    const f=frame(p.frame),side=frame(p.kind+'-east'),scale=p.width/(side?.rect[2]||f.rect[2]),s=motion(this,a,p),detail=this.detailLevel<3;
    shadow(this,c,p.width,.56);
    if(detail&&a.moving&&!this.reducedMotion){const d=p.direction;dust(this,c,-d.x*p.width*.4,-d.y*p.width*.35+5,46,this.time*.8+(a.phase||0));}
    c.save();if(a.moving&&!this.reducedMotion)c.translate(0,Math.sin(this.time*17+(a.phase||0))*.6);
    const transition=this.reducedMotion?1:clamp((this.time-s.at)/.1,0,1);
    if(transition<1&&s.previous)stamp(this,c,s.previous,scale,s.previousMirror,a.variant,p.state,1-transition);
    stamp(this,c,p.frame,scale,p.direction.mirror,a.variant,p.state,transition<1&&s.previous?transition:1);
    if(detail&&a.moving&&!this.reducedMotion&&f.wheels){
      c.save();c.scale(p.direction.mirror?-scale:scale,scale);c.translate(-f.anchor[0],-f.anchor[1]);
      c.strokeStyle='rgba(198,195,156,.4)';c.lineWidth=2;
      for(const [x,y,radius]of f.wheels)for(let i=0;i<3;i++){
        const angle=p.wheelPhase+i*TAU/3;c.beginPath();c.moveTo(x,y);c.lineTo(x+Math.cos(angle)*radius,y+Math.sin(angle)*radius*.86);c.stroke();
      }c.restore();
    }
    if(p.turret){
      const m=f.mount||[f.anchor[0],f.anchor[1]-f.rect[3]*.4],mx=(m[0]-f.anchor[0])*scale*(p.direction.mirror?-1:1),my=(m[1]-f.anchor[1])*scale;
      c.save();c.translate(mx,my);if(a.cannonFlash>0&&!this.reducedMotion)c.translate(-p.turretDirection.x*3,-p.turretDirection.y*3);
      // Specialist turrets carry their authored paint; other variants share filtered hull art.
      stamp(this,c,p.turret,scale*.86,p.turretDirection.mirror,['repeater','bombard','cyclone'].includes(a.variant)?null:a.variant,p.state);c.restore();firing(this,c,a,p,mx,my-11);
    }else firing(this,c,a,p,0,-26);
    if(detail)lights(this,c,a,p,p.width);
    if(p.state==='heavy'||p.state==='disabled'){
      for(let i=0;i<2;i++)smoke(this,c,-p.direction.x*p.width*.2,-22,40+i*14,this.time*.5+i*.43);
      if(p.state==='disabled')smoke(this,c,-p.direction.x*p.width*.2,-13,24,this.time*.5,true);
    }else if(detail&&!this.reducedMotion&&(a.moving||a.engineDamage>15))smoke(this,c,-p.direction.x*p.width*.35,-7,22,this.time*.7+(a.phase||0));
    c.restore();
    if(a.hp>0&&a.maxHp&&a.hp<a.maxHp)this.health(c,a.hp/a.maxHp,-Math.max(62,f.rect[3]*scale+18),p.width*.5,'#d6b585');
  };
  P.drawHeli=function(c,h){
    let landing=1;const baseSite=this.vehicleGame?.world?.sites?.get(h.siteId);
    if(h.life<15&&baseSite)landing=clamp((Math.hypot(h.x-baseSite.x,h.y-baseSite.y)-60)/220,.16,1);
    const p=resolve(h,this.time||0,{heli:true,reducedMotion:this.reducedMotion,landing});if(!ready(p.frame))return base.heli.call(this,c,h);
    const f=frame(p.frame),side=frame('heli-'+p.kind+'-east'),scale=p.width/side.rect[2],ground=this.renderPoint(h),point=this.renderPoint(h,p.altitude),z=this.camera.zoom;
    if(!this.visible(point,210*z))return;const s=motion(this,h,p);
    c.save();c.translate(ground.x+9*z,ground.y+4*z);c.scale(z,z);shadow(this,c,p.width*.64,clamp(.62-p.altitude/310,.18,.58),p.altitude);
    if(p.altitude<65&&!this.reducedMotion&&this.detailLevel<3)for(let i=0;i<2;i++)dust(this,c,(i?1:-1)*p.width*.23,4,78,this.time*.7+i*.5);c.restore();
    c.save();c.translate(point.x,point.y+(this.reducedMotion?0:Math.sin(this.time*2.2+(h.phase||0))*1.8)*z);c.scale(z,z);
    if(!this.reducedMotion)c.rotate(s.bank);
    stamp(this,c,p.frame,scale,p.direction.mirror,null,p.state);
    c.save();c.translate(f.tail[0]*scale*(p.direction.mirror?-1:1),f.tail[1]*scale);rotor(this,c,p,true);c.restore();rotor(this,c,p);
    if(h.armed)firing(this,c,h,{...p,turretDirection:p.direction},p.direction.x*38,32);
    if(p.state==='heavy'||p.state==='light'&&h.hp<h.maxHp*.55)for(let i=0;i<2;i++)smoke(this,c,-p.direction.x*18,12,46,this.time*.6+i*.5);
    if(h.hp<h.maxHp*.25)smoke(this,c,-p.direction.x*18,17,22,this.time*.7,true);
    // The actual spot location and beam are still drawn by getLights/drawLights.
    if(this.detailLevel<3&&this.glow)this.glow(c,p.direction.x*28,35,5,'196,226,232',.7);
    if(h.hp>0&&h.hp<h.maxHp)this.health(c,h.hp/h.maxHp,-25,p.width*.45,'#d6b585');c.restore();
  };
  function wreckKind(a){return a.id?.startsWith('heli')||a.kind==='heli'?'heli':['tank','ifv'].includes(a.vehicleClass||a.kind)?'tank':['apc','armored'].includes(a.vehicleClass||a.kind)?'apc':a.kind==='truck'?'truck':'jeep'}
  P.drawVehicleWreck=function(c,a,age=99){
    const kind=wreckKind(a),name='wreck-'+kind,f=frame(name);if(!f||!ready(name))return false;
    const width=widths[a.vehicleClass||a.kind]|| (kind==='heli'?147:kind==='tank'?124:88),scale=width/f.rect[2];
    shadow(this,c,width,.42);stamp(this,c,name,scale,direction(a.dir||0).mirror);
    if(age<7&&this.detailLevel<3){smoke(this,c,-8,-17,58,this.time*.5);if(age<2)smoke(this,c,5,-8,35,this.time*.8,true)}return true;
  };
  P.drawObject=function(c,o){if(o.type==='vehicleWreck'&&this.drawVehicleWreck(c,o,Math.max(0,this.time-(o.destroyedAt||0))))return;return base.object.call(this,c,o)};
  P.drawCorpses=function(game){
    if(!ready('wreck-jeep'))return base.corpses?.call(this,game);
    const ordinary=this.vehicleOrdinaryCorpses||(this.vehicleOrdinaryCorpses=[]);ordinary.length=0;
    for(const a of game.corpses||[]){
      const heli=a.id?.startsWith('heli'),vehicle=a.type==='vehicle';if(!heli&&!vehicle){ordinary.push(a);continue}
      if(vehicle&&game.world?.objects?.has(a.id+':wreck'))continue;
      const ground=this.project(a.x,a.y),z=this.camera.zoom;if(!this.visible(ground,200*z))continue;
      const c=this.ctx,age=a.age||0;c.save();c.translate(ground.x,ground.y);c.scale(z,z);c.globalAlpha*=clamp((a.life||0)/2,0,1);
      if(heli&&age<1.2&&!this.reducedMotion){
        const p=resolve(a,this.time,{heli:true}),f=frame(p.frame),side=frame('heli-'+p.kind+'-east');
        shadow(this,c,p.width*.6,.35,100*(1-age/1.2));c.translate(0,-125*(1-age/1.2));c.rotate(age*.65);
        if(f&&side)stamp(this,c,p.frame,p.width/side.rect[2],p.direction.mirror,null,'heavy');smoke(this,c,0,0,58,this.time*.5,true);
      }else this.drawVehicleWreck(c,a,age);c.restore();
    }
    // A renderer-only view avoids modifying the game's corpse collection or saves.
    if(!this.vehicleCorpseView||Object.getPrototypeOf(this.vehicleCorpseView)!==game)this.vehicleCorpseView=Object.create(game);
    this.vehicleCorpseView.corpses=ordinary;base.corpses?.call(this,this.vehicleCorpseView);
  };
  window.ATSVehicleArt={resolve,direction,damageState,frame,classes:Object.keys(widths).slice(0,7),rotorFrameCount:16,maxTextureEntries:96,maxRotorEntries:32};
})();
