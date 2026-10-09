/* Authored raster characters. This extension reads simulation state only.
 * Coordinates, navigation, damage, detection and corpse lifetime stay in sim.js. */
(() => {
  'use strict';
  const P=ATSRenderer.prototype,previousGeometry=P.drawPrimateGeometry,previousHuman=P.drawHuman,previousApe=P.drawApe,
    previousHumanSprite=P.drawHumanSprite,previousApeSprite=P.drawApeSprite,previousCorpses=P.drawCorpses;
  const TAU=Math.PI*2,clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
  const species=['gorilla','chimpanzee','orangutan','gibbon','mandrill','capuchin'];
  const sizes=[76,66,78,64,63,49],tempos=[1.45,2.05,1.25,1.75,1.95,2.45];
  const hash=s=>{let n=0;for(const c of String(s||''))n=(Math.imul(n,31)+c.charCodeAt(0))|0;return n>>>0};
  const ellipse=(c,x,y,w,h,color)=>{c.beginPath();c.ellipse(x,y,w,h,0,0,TAU);c.fillStyle=color;c.fill()};
  const line=(c,x,y,u,v,color,w=1)=>{c.beginPath();c.moveTo(x,y);c.lineTo(u,v);c.strokeStyle=color;c.lineWidth=w;c.lineCap='round';c.stroke()};
  const art=()=>window.ATSVisualAssets,metadata=()=>art()?.manifest?.characters;
  const DIRECTIONS=Object.freeze(['east','southeast','south','southwest','west','northwest','north','northeast'].map((name,sector)=>Object.freeze({sector,name,row:[2,1,0,1,2,3,4,3][sector],mirror:sector>2&&sector<6,rear:sector>=5})));
  const FRONT_STEP=[1,0,2,0],REAR_APE_STEP=[3,3,4,4],REAR_HUMAN_STEP=[5,5,6,6];
  const HEAVY_ROLES=new Set(['heavy','juggernaut','rotary','shield','breacher']),RANGED_ROLES=new Set(['sniper','ranger','tracker','flanker','spotter','commando']);
  const MELEE_ANIMATIONS=new Set(['hook','slam','backhand','tackle','overhead','uppercut']),COMMAND_STATES=new Set(['prepare','command','victory']),RUN_STATES=new Set(['run','sprint']);

  function frameGeometry(r,pose,slot){
    const registry=art(),data=metadata();if(!data)return null;
    let cache=r.characterFrameGeometry;
    if(!cache||cache.registry!==registry||cache.data!==data||cache.version!==data.version||cache.frames!==data.frames){
      cache=r.characterFrameGeometry={registry,data,version:data.version,frames:data.frames,atlases:new Map((data.atlases||[]).map(a=>[a.id,a])),entries:new Map()};
      r.characterCoats?.clear();r.characterCoatSources?.clear();
    }
    const key=pose.atlas+':'+pose.row+':'+pose.column+':'+slot;
    let value=cache.entries.get(key);if(value)return value;
    const atlas=cache.atlases.get('characters-'+pose.atlas),f=data.frames?.[pose.atlas]?.[pose.row+':'+pose.column];if(!atlas||!f)return null;
    const shadowUnit=slot/(atlas.width/atlas.columns),unit=shadowUnit*(pose.atlas==='kingactions'&&pose.row===0?.68:1),contacts=[];
    let left=Infinity,right=-Infinity;for(const contact of f.groundContacts||[]){left=Math.min(left,contact.x-contact.width/2);right=Math.max(right,contact.x+contact.width/2);contacts.push({x:(contact.x-f.anchorX)*shadowUnit,width:Math.max(2,contact.width*shadowUnit*.62)})}
    value={f,x:-f.anchorX*unit,y:-f.anchorY*unit,width:f.w*unit,height:f.h*unit,contacts,shadowX:contacts.length?((left+right)/2-f.anchorX)*shadowUnit:0,shadowWidth:contacts.length?(right-left)*shadowUnit*.54+3:0};
    if(cache.entries.size>=512)cache.entries.delete(cache.entries.keys().next().value);cache.entries.set(key,value);return value;
  }

  // World facing is first projected into the same isometric plane as movement.
  function direction(angle=0){
    const dx=(Math.cos(angle)-Math.sin(angle))*.8,dy=(Math.cos(angle)+Math.sin(angle))*.42;
    const sector=(Math.round(Math.atan2(dy,dx)/(Math.PI/4))+8)%8;
    return DIRECTIONS[sector];
  }
  function humanRow(a){
    if(HEAVY_ROLES.has(a.role)||a.kind==='machine')return 2;
    if(RANGED_ROLES.has(a.role)||a.kind==='sniper')return 3;
    return a.kind==='pistol'?0:1;
  }
  // Pure resolver, also used by QA and the sprite browser. No entity writes.
  function resolve(a={},time=0,options={}){
    const king=!!options.king||a.id==='king',human=!!options.human||a.type==='human',d=direction(a.dir||0),
      index=Math.max(0,species.indexOf(a.species||'gorilla')),seed=(a.phase??hash(a.id)%628/100),
      hz=options.animationHz||18,clock=options.reducedMotion?0:Math.floor(time*hz)/hz,
      urgent=a.state==='charge'||a.sprinting||a.urgent||a.state==='retreat',
      cycle=((clock*(human?1.9:tempos[index])*(urgent?1.35:1)+seed/TAU)%1+1)%1,
      beat=Math.floor(cycle*4),anim=a.animation,
      active=anim&&time>=anim.start&&time<anim.start+(anim.duration||.5),
      progress=active?clamp((time-anim.start)/(anim.duration||.5),0,1):0;
    let state='idle',column=0,event=null,phase=cycle;
    const rearStep=human?REAR_HUMAN_STEP:REAR_APE_STEP,frontStep=FRONT_STEP;
    const climbing=a.palisadeClimb||a.climbingWallId&&a.wallClimbUntil>time||a.climbingVehicleId&&a.climbUntil>time||a.siegeTransition&&!a.siegeTransition.stairs;
    if(a.hp<=0||a.state==='fallen'){state='death';column=human?7:king?5:5;phase=clamp((a.deathAge||a.age||0)/.65,0,1)}
    else if(a.blastReaction?.stage==='flight'||a.knockbackUntil>time){state='knockback';column=king?5:human?4:5}
    else if(a.blastReaction?.stage==='gettingUp'||a.staggerUntil>time||a.hitTimer>0){state='hurt';column=king?5:human?4:5}
    else if(climbing){state='climb';column=king?(beat%2?3:4):human?(beat%2?5:6):beat%2?5:(d.rear?3:1);event=beat%2?'gripRight':'gripLeft'}
    else if(human&&(a.gunFlashUntil>time||active&&anim.kind==='recoil'||a.attackTimer>0)){state='fire';column=progress<.45?4:3;phase=progress;event='muzzle'}
    else if(active&&(anim.kind==='rally'||a.rallyCast)||king&&!active&&a.attackTimer>0){state='command';column=king?3:5;phase=progress;event='command'}
    else if(active&&anim.kind==='throw'){
      state='throw';phase=progress;column=human?(progress<.52?3:4):king?(progress<.52?3:4):5;
      event=progress>=.525&&progress<.65?'release':progress<.525?'anticipation':'recover';
    }
    else if(active&&MELEE_ANIMATIONS.has(anim.kind)){
      // Melee damage is immediate in the existing game. The first frame is the
      // contact pose; follow-through/recovery never delays or predicts damage.
      state=['slam','overhead'].includes(anim.kind)?'heavyAttack':anim.kind==='tackle'?'chargeAttack':'attack';
      phase=progress;column=king?(progress<.28?4:progress<.62?5:0):progress<.68?5:0;event=progress<.28?'impact':'recover';
    }
    else if(human&&!a.moving&&(a.aiming||a.firePlan||a.state==='combat')){state='aim';column=3}
    else if(human&&a.state==='radio'){state='radio';column=3}
    else if(a.moving){state=urgent?'sprint':'run';column=d.rear&&!king?rearStep[beat]:frontStep[beat];event=beat===0?'footLeft':beat===2?'footRight':null}
    else if(!human&&a.attackCD>0&&a.attackCD<.14&&['charge','attack'].includes(a.state)){state='prepare';column=king?3:5;event='anticipation'}
    else if(a.hp>0&&a.hp<a.maxHp*.25){state='injured';column=king?5:0}
    else if(/rest|sleep|heal|groom/.test(a.activity||'')){state='rest';column=king?5:0}
    else if(a.carrying||a._carryingWood){state='carry';column=king?4:d.rear?3:0}
    else if(a.state==='victory'||a.celebrating){state='victory';column=king?3:5}
    else if(human&&d.rear){column=5}
    else if(!king&&d.rear){column=3}
    if(options.reducedMotion&&RUN_STATES.has(state)){column=d.rear&&!king?(human?5:3):0;event=null}
    let atlas=human?'humans':king?'king':'apes',row=human?humanRow(a):king?d.row:index;
    if(!human&&!king){
      if(state==='climb'){atlas='actions';column=beat%2?4:3}
      else if(state==='death'){atlas='actions';column=5}
      else if(state==='heavyAttack'||state==='chargeAttack'){atlas='actions';column=1}
      else if(state==='attack'){atlas='actions';column=progress<.68?2:1}
      else if(COMMAND_STATES.has(state)){atlas='actions';column=0}
      else if(state==='throw'){atlas='actions';column=progress<.525?0:2}
    }
    if(human&&(d.rear&&['fire','aim'].includes(state)||state==='radio'||state==='throw')){
      atlas='humanactions';column=state==='radio'?2:state==='throw'?3:state==='fire'?1:0;
    }
    if(human&&!options.reducedMotion&&RUN_STATES.has(state)){
      // Alternate the high-knee lift artwork with authored planted-foot passing
      // poses. Limbs and shoulders change shape; the whole cutout never bobs.
      if(beat===0||beat===2){atlas='humanwalk';column=(d.rear?2:0)+(beat===2?1:0)}
      else {atlas='humans';column=d.rear?(beat===1?5:6):(beat===1?1:2)}
    }
    if(king&&state==='climb'){atlas='kingactions';row=0;column=beat%2?1:0}
    if(king&&['command','victory'].includes(state)){atlas='kingactions';row=0;column=2}
    if(king&&state==='death'){atlas='kingactions';row=1;column=phase<.35?0:phase<.75?1:2}
    return {atlas,state,column,row,
      direction:d.sector,directionName:d.name,mirror:d.mirror,rear:d.rear,phase,event,beat,king,human,
      clipId:active?String(anim.start)+':'+anim.kind:state,sourceDirectionCount:king?8:4};
  }

  function atlasImage(r,name,a){
    const original=art()?.get('characters-'+name);if(!original)return null;
    if(!['apes','actions'].includes(name)||!a.coatVariant||r.quality==='low')return original;
    // Four shared coat atlases, never one cache entry per member of the horde.
    const variant=clamp(a.coatVariant|0,0,2);if(!variant)return original;r.characterCoats=r.characterCoats||new Map();
    const key=name+':'+variant;if(!r.characterCoatSources)r.characterCoatSources=new Map();let image=r.characterCoats.get(key);if(image&&r.characterCoatSources.get(key)===original)return image;
    image=document.createElement('canvas');image.width=original.naturalWidth||original.width;image.height=original.naturalHeight||original.height;
    const c=image.getContext('2d');c.filter=variant===1?'brightness(.82) sepia(.12)':'brightness(1.13) saturate(.82)';c.drawImage(original,0,0);c.filter='none';
    r.characterCoats.set(key,image);r.characterCoatSources.set(key,original);return image;
  }
  function frame(r,c,a,pose,slot){
    const geometry=frameGeometry(r,pose,slot),image=atlasImage(r,pose.atlas,a);
    if(!image||!geometry){const fallback=pose.king?'king':pose.human?'humans':'apes';if(pose.atlas!==fallback)return frame(r,c,a,{...pose,atlas:fallback,row:pose.king?direction(a.dir||0).row:pose.row,column:0},slot);return false}
    const f=geometry.f;c.drawImage(image,f.x,f.y,f.w,f.h,geometry.x,geometry.y,geometry.width,geometry.height);
    r.characterArtworkDraws=(r.characterArtworkDraws||0)+1;return true;
  }
  function stamp(r,a,pose){
    if(a._atlas||typeof a!=='object')return;
    r.characterEvents=r.characterEvents||new WeakMap();
    const token=pose.clipId+':'+pose.event+':'+pose.beat,prior=r.characterEvents.get(a);
    if(!prior||prior.token!==token){
      const event=pose.event&&(!prior||prior.event!==pose.event||prior.clipId!==pose.clipId);
      r.characterEvents.set(a,{token,event:pose.event,clipId:pose.clipId,at:r.time,pose:pose.state});
      if(event&&typeof r.onVisualAnimationEvent==='function')r.onVisualAnimationEvent({actorId:a.id,event:pose.event,time:r.time,state:pose.state});
    }
  }
  function cosmeticMotion(r,c,a,p){
    if(r.reducedMotion)return;
    if(['idle','aim','radio'].includes(p.state)){
      // Breathe around the planted-foot origin. Translating the whole cutout
      // would separate its contact foot from the ground and look like hovering.
      const breath=Math.sin(r.time*2+(a.phase||0));c.scale(1,1+breath*.006);
    }else if(p.state==='climb'){
      c.rotate(Math.sin(p.phase*TAU)*.045);
    }else if(p.state==='injured'||p.state==='rest')c.scale(1,.96);
  }
  P.usesPaintedPrimate=function(a={},king=false){return !!metadata()&&!!art()?.get(king||a.id==='king'?'characters-king':'characters-apes')};
  P.drawPrimateGeometry=function(c,a={},options={}){
    if(!art()?.get(options.king||a.id==='king'?'characters-king':'characters-apes')||!metadata())return previousGeometry.call(this,c,a,options);
    const p=resolve(a,this.time||0,{king:options.king,reducedMotion:this.reducedMotion,animationHz:this.graphicsProfile?.animationHz}),
      index=Math.max(0,species.indexOf(a.species||'gorilla')),slot=p.king?84:sizes[index],outerFlip=Math.cos(a.dir||0)<-.15?-1:1;
    stamp(this,a,p);c.save();
    // Existing outer body wrappers use world-axis mirroring. Cancel that here
    // before selecting the correctly projected authored facing.
    c.scale(outerFlip*(p.mirror?-1:1),1);cosmeticMotion(this,c,a,p);
    if(!['climb','knockback','death'].includes(p.state)){
      // The support locations are measured from the opaque feet/knuckles in
      // each authored frame. This stays aligned even when a stride spreads wide.
      const geometry=frameGeometry(this,p,slot);
      if(geometry){
        if(geometry.contacts.length)ellipse(c,geometry.shadowX,1.5,geometry.shadowWidth,3.2,'rgba(0,6,7,.18)');
        for(const contact of geometry.contacts)ellipse(c,contact.x,.35,contact.width,1.5,'rgba(0,5,6,.44)');
      }
    }
    const held=window.ATSHeldItemArt,items=held?.ready()&&held.hasEquipment(a,p)?held.resolve(a,p):null;
    if(items)held.draw(this,c,a,p,slot,'rear',items);
    frame(this,c,a,p,slot);
    if(items){held.draw(this,c,a,p,slot,'front',items);held.restoreHands(this,c,a,p,slot,items,atlasImage(this,p.atlas,a))}
    if(!options.mini&&this.detailLevel<3){
      const n=hash(a.id),rear=p.rear;
      if(!p.king&&!rear&&n%7===0){line(c,8,-46,11,-41,'rgba(205,178,146,.66)',.9);line(c,7,-45,9,-44,'#6a6554',.65)}
      if(a.state==='scout'&&!held?.ready()){line(c,-12,-35,11,-18,'#c8b47e',2);ellipse(c,10,-18,2,2,'#e4d09b')}
      if(p.king&&a.crownTier>0&&p.atlas==='king'){
        // The laurel is painted into every frame. Reign jewels attach to the
        // frame's actual head, so progression is visible without a floating crown.
        const crownY=[-64,-63,-63,-56,-54,-40][p.column];
        ellipse(c,p.rear?0:3,crownY,2.1,2.8,a.crownTier>1?'#f28e61':'#b0e5d8');
        if(a.crownTier>1){ellipse(c,-7,crownY+2,1.6,2,'#eac776');ellipse(c,11,crownY+2,1.6,2,'#eac776')}
      }
    }
    c.restore();return true;
  };
  // The painted sheet already batches every pose. Re-rasterizing actors into
  // old direction-agnostic crowd canvases would lose their rear views.
  P.drawApeSprite=function(c,a){if(!art()?.get('characters-apes'))return previousApeSprite.call(this,c,a);this.drawApe(c,a,false);return false};
  P.drawApe=function(c,a,king,exposure){
    if(!king||a.hp>0||!art()?.get('characters-kingactions'))return previousApe.call(this,c,a,king,exposure);
    // Three actual collapse poses settle the King with the laurel still painted
    // on his head; rotating a live standing frame cannot provide this silhouette.
    const p=resolve(a,this.time||0,{king:true,reducedMotion:this.reducedMotion});
    ellipse(c,0,3,36,8,'rgba(0,5,8,.38)');c.save();c.scale(p.mirror?-1.36:1.36,1.36);
    if(a.blastReaction?.stage==='flight'){c.translate(0,-(a.blastReaction.height||0));c.rotate(a.blastReaction.rotation||0)}
    frame(this,c,a,p,84);c.restore();
  };

  P.characterAttachment=function(a,king=false){
    const p=resolve(a,this.time||0,{king,reducedMotion:this.reducedMotion,animationHz:this.graphicsProfile?.animationHz});
    let left={x:-24,y:-15},right={x:24,y:-15};
    if(p.state==='climb'){left={x:-10,y:p.beat%2?-40:-65};right={x:12,y:p.beat%2?-65:-40}}
    else if(['prepare','command','victory'].includes(p.state)){left={x:-12,y:-64};right={x:16,y:-64}}
    else if(p.state==='throw'){left={x:-15,y:-28};right=p.phase<.525?{x:10,y:-65}:{x:33,y:-35}}
    else if(p.state==='attack'&&!king){left={x:-15,y:-25};right={x:29,y:-37}}
    else if(p.state==='heavyAttack'||p.state==='chargeAttack'){left={x:-17,y:-9};right={x:24,y:-7}}
    return {left,right,mirror:p.mirror,scale:king?1.36:(a.bodyScale||1),state:p.state};
  };
  const equipment=P.drawEquipment;
  if(equipment)P.drawEquipment=function(c,a,king=false){
    if(!art()?.get('characters-apes')||!a.equipment||a.hp<=0||a._atlas)return equipment.call(this,c,a,king);
    const gear=a.equipment,sockets=this.characterAttachment(a,king);
    // Torso armor remains on the original progression layer; items attached to
    // hands follow their authored pose independently of the chest.
    equipment.call(this,c,{...a,equipment:{...gear,cuffs:false,spear:false,torch:false}},king);
    if(window.ATSHeldItemArt?.ready())return;
    if(!gear.cuffs&&!gear.spear&&!gear.torch)return;
    c.save();c.translate(0,-(a.elevation||a.wallClimbHeight||0));c.scale(sockets.scale*(sockets.mirror?-1:1),sockets.scale);
    if(gear.cuffs)for(const hand of [sockets.left,sockets.right]){
      c.fillStyle='#aa8a4f';c.fillRect(hand.x-5,hand.y-5,10,7);line(c,hand.x-5,hand.y-4,hand.x+5,hand.y-4,'#e0c58a',1.5);
      for(let i=0;i<3;i++)line(c,hand.x-3+i*3,hand.y-4,hand.x-3+i*3,hand.y+1,'#615333',.8);
    }
    const hand=sockets.right;
    if(gear.spear){line(c,hand.x-3,hand.y+13,hand.x+7,hand.y-35,'#ba985c',2.5);c.beginPath();c.moveTo(hand.x+8,hand.y-43);c.lineTo(hand.x+3,hand.y-34);c.lineTo(hand.x+11,hand.y-32);c.closePath();c.fillStyle='#e4d8ad';c.fill()}
    if(gear.torch){const flicker=this.reducedMotion?0:Math.sin(this.time*13+(a.phase||0))*2;line(c,hand.x,hand.y+5,hand.x+5,hand.y-20,'#937343',3);ellipse(c,hand.x+5,hand.y-25,4,8+flicker,'#e88a38');ellipse(c,hand.x+5,hand.y-24,2.3,5,'#ffe39b');if(this.detailLevel<3)this.glow(c,hand.x+5,hand.y-25,21,'245,167,58',.24)}
    c.restore();
  };

  function humanEquipment(r,c,a,p){
    const role=a.role||'',rear=p.rear,moving=a.moving&&!r.reducedMotion,sway=moving?Math.sin(p.phase*TAU)*1.1:0;
    const held=window.ATSHeldItemArt?.ready();
    c.save();c.translate(0,sway);
    // Small role-specific equipment remains independent of the shared bodies.
    if(role==='medic'&&!held){c.fillStyle='#c4ccae';c.fillRect(-11,-31,7,11);line(c,-10,-26,-5,-26,'#ae5649',1.7);line(c,-7.5,-29,-7.5,-23,'#ae5649',1.7)}
    if(['officer','leader'].includes(role)){line(c,-5,-43,6,-43,'#d7bd7d',2);ellipse(c,6,-34,1.7,2,'#e4c377')}
    if(['grenadier','bombardier','mortar'].includes(role))for(let i=0;i<3;i++)ellipse(c,-9+i*4,-29+i*2,1.6,3,'#d0b275');
    if(['engineer','spotter'].includes(role)){line(c,-12,-25,-12,-55,'#95aaa0',1);ellipse(c,-12,-54,1.6,1.3,'#d3b371')}
    if(role==='shield'&&!rear&&!held){c.fillStyle='#485e63';c.fillRect(5,-36,13,29);c.fillStyle='#abbfb7';c.fillRect(6,-33,11,7);line(c,11,-23,11,-10,'#8aa49b',1.5)}
    if((a.engineerJob||a.constructing)&&!held){line(c,9,-27,19,-20+sway,'#bbaa7e',2.2);line(c,16,-24+sway,23,-20+sway,'#9aa99b',3)}
    if(p.state==='radio'){if(!held){ellipse(c,-8,-41,2,4,'#14282b');line(c,-8,-44,-8,-50,'#93ad9d',1)}r.drawSignal(c,-8,-54,a.radioTimer||0)}
    c.restore();
  }
  P.drawHuman=function(c,a){
    if(!art()?.get('characters-humans')||!metadata()||window.ATSHeldItemArt&&!window.ATSHeldItemArt.ready())return previousHuman.call(this,c,a);
    const p=resolve(a,this.time||0,{human:true,reducedMotion:this.reducedMotion,animationHz:this.graphicsProfile?.animationHz}),
      size=a.role==='juggernaut'?1.12:a.role==='assault'?1.04:1;
    stamp(this,a,p);ellipse(c,0,2,12*size,4.3,'rgba(0,5,8,.4)');c.save();c.translate(0,-(a.elevation||0));c.scale(size,size);
    c.save();c.scale(p.mirror?-1:1,1);cosmeticMotion(this,c,a,p);
    if(!this.reducedMotion&&a.hitTimer>0)c.rotate(Math.sin(a.hitTimer*18)*-.07);
    const held=window.ATSHeldItemArt,items=held?.ready()?held.resolve(a,p):null;
    if(items)held.draw(this,c,a,p,60,'rear',items);
    frame(this,c,a,p,60);
    if(items){held.draw(this,c,a,p,60,'front',items);held.restoreHands(this,c,a,p,60,items,atlasImage(this,p.atlas,a))}
    humanEquipment(this,c,a,p);c.restore();
    if(p.state==='fire'){
      const dx=(Math.cos(a.dir||0)-Math.sin(a.dir||0))*.8,dy=(Math.cos(a.dir||0)+Math.sin(a.dir||0))*.42,
        len=a.kind==='pistol'?24:a.kind==='machine'?39:33,mx=dx*len,my=-34+dy*len;
      if(!this.reducedMotion){this.glow(c,mx,my,13,'255,210,133',.36);line(c,mx,my,mx+dx*7,my+dy*7,'#ffe6ad',2);ellipse(c,mx,my,2.8,1.5,'#fff4d2')}
      if(this.detailLevel<2&&!this.reducedMotion){const t=p.phase;line(c,6+t*13,-28-t*12,8+t*13,-27-t*12,'#d1aa66',1.1)}
    }
    if((a.suspicion||0)>.08||a.state==='combat'||a.state==='radio')this.drawAwarenessIcon(c,a);
    if(a.hp>0&&a.maxHp&&a.hp<a.maxHp)this.health(c,a.hp/a.maxHp,-68,23,'#ddaf88');
    if(a.squadOrder==='Hold Line'&&this.camera.zoom>.9&&this.detailLevel<2)line(c,-8,7,8,7,'#9fb8a0',2);
    c.restore();
  };
  P.drawHumanSprite=function(c,a){if(!art()?.get('characters-humans'))return previousHumanSprite.call(this,c,a);this.drawHuman(c,a);return true};

  P.drawCorpses=function(game){
    if(!art()?.get('characters-apes')||!art()?.get('characters-humans')||!metadata())return previousCorpses.call(this,game);
    const other=(game.corpses||[]).filter(a=>a.type!=='ape'&&a.type!=='human');if(other.length)previousCorpses.call(this,{...game,corpses:other});
    const c=this.ctx,z=this.camera.zoom;
    for(const a of game.corpses||[]){
      if(a.type!=='ape'&&a.type!=='human')continue;const ground=this.project(a.x,a.y);if(!this.visible(ground,180*z))continue;
      const human=a.type==='human',p=resolve(a,this.time||0,{human,reducedMotion:this.reducedMotion}),
        t=this.reducedMotion?1:clamp((a.age||0)/.65,0,1),ease=1-(1-t)**3,sign=Math.cos(a.fallDir||0)<0?-1:1,
        airborne=a.blastReaction?.stage==='flight',height=airborne?Math.max(0,a.blastReaction.height||0):0;
      c.save();c.translate(ground.x,ground.y);c.scale(z,z);c.globalAlpha=clamp(a.life/2,0,.84);
      ellipse(c,0,2,22,6,'rgba(0,8,12,.28)');c.translate(Math.cos(a.fallDir||0)*ease*8,-height);
      c.scale((p.mirror?-1:1)*(a.bodyScale||1),a.bodyScale||1);
      if(airborne)c.rotate(a.blastReaction.rotation||0);
      else if(!human&&t<.72){p.atlas='actions';p.column=1;c.rotate((a.fallVariant===1?-1:1)*sign*ease*(a.fallVariant===2?2.65:1.48));c.scale(1,1-ease*.24)}
      else if(!human){p.atlas='actions';p.column=5;c.scale(1,.9)}
      else if(t<.72){c.rotate(sign*ease*1.45);p.column=4}
      else {p.column=7;c.scale(1,.82)}
      const index=Math.max(0,species.indexOf(a.species||'gorilla')),young=(a.actorAge??240)<20||a.actorState==='young';
      if(young)c.scale(.63,.63);frame(this,c,a,p,human?60:sizes[index]);c.restore();
    }
  };

  // Exposed without any game references so tools/tests can inspect the clips.
  window.ATSCharacterArt={version:1,species:[...species],direction,resolve,
    frameRect(atlas,row,column){return metadata()?.frames?.[atlas]?.[row+':'+column]||null},
    status(){return {ready:!!metadata()&&metadata().atlases.every(a=>!!art()?.get(a.id)),frames:metadata()?.atlases.reduce((n,a)=>n+a.frameCount,0)||0,directions:{king:8,crowd:4},cacheLimit:4}}};
})();
