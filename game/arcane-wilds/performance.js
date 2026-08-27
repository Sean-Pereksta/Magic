'use strict';

/* Arcane Wilds performance governor.
 * Loaded last so it can optimize the final wrapped runtime without changing combat rules.
 */
(function(){
  if(window.__arcaneWildsPerformanceLoaded)return;
  window.__arcaneWildsPerformanceLoaded=true;

  const perf={
    low:false,
    frameMs:16.7,
    slowFrames:0,
    fastFrames:0,
    particleCap:isTouch?130:210,
    dprCap:isTouch?1.35:1.7
  };
  window.arcaneWildsPerformance=perf;

  let cachedLight=null;
  let cachedSpellSignature='';
  let cachedMapSignature='';
  let lastHudPaint=0;

  function clearRenderCaches(){cachedLight=null;}
  function currentParticleCap(){return perf.low?(isTouch?90:145):perf.particleCap;}

  function applyResolutionCap(){
    const nextW=innerWidth,nextH=innerHeight;
    const nextDpr=Math.min(devicePixelRatio||1,perf.low?Math.min(perf.dprCap,1.35):perf.dprCap);
    const pixelW=Math.floor(nextW*nextDpr),pixelH=Math.floor(nextH*nextDpr);
    if(W===nextW&&H===nextH&&Math.abs(dpr-nextDpr)<.01&&canvas.width===pixelW&&canvas.height===pixelH)return;
    W=nextW;H=nextH;dpr=nextDpr;
    canvas.width=pixelW;canvas.height=pixelH;
    canvas.style.width=W+'px';canvas.style.height=H+'px';
    ctx.setTransform(dpr,0,0,dpr,0,0);
    ctx.imageSmoothingEnabled=true;
    mouse.x=W/2;mouse.y=H/2;
    clearRenderCaches();
  }

  addEventListener('resize',()=>requestAnimationFrame(applyResolutionCap));
  applyResolutionCap();

  /* The original HUD rebuilt three spell buttons and the full 5x5 minimap on every
     animation frame. Keep those nodes and only rebuild when their structure changes. */
  const baseRenderSpellBar=renderSpellBar;
  renderSpellBar=function(){
    const root=$('spells');
    const ids=game.player?.activeSpells||[];
    const signature=ids.slice(0,3).map(v=>v||'-').join('|');
    if(signature!==cachedSpellSignature||root.children.length!==3){
      cachedSpellSignature=signature;
      baseRenderSpellBar();
    }
    for(let i=0;i<3;i++){
      const id=ids[i],slot=root.children[i];
      if(!id||!slot)continue;
      const s=SPELLS[id],st=game.player.spellState[id]||(game.player.spellState[id]={cd:0});
      const max=s.cooldown*spellMods(id).cdr,ratio=max?clamp(st.cd/max,0,1):0;
      const shade=slot.querySelector('.cooldown'),text=slot.querySelector('.cooldown-text');
      if(shade)shade.style.transform=`scaleY(${ratio})`;
      if(text){const value=st.cd>.05?st.cd.toFixed(1):'';if(text.textContent!==value)text.textContent=value;}
    }
  };

  const baseRenderMinimap=renderMinimap;
  renderMinimap=function(){
    const parts=[game.room.x,game.room.y];
    for(let dy=-2;dy<=2;dy++)for(let dx=-2;dx<=2;dx++){
      const r=game.rooms[roomKey(game.room.x+dx,game.room.y+dy)];
      parts.push(r?(r.seen?'s':'-')+(r.cleared?'c':'-')+(r.town?'t':'-')+(r.boss?'b':'-'):'----');
    }
    const signature=parts.join('|');
    if(signature===cachedMapSignature)return;
    cachedMapSignature=signature;
    baseRenderMinimap();
  };

  /* mobile-interaction.js wraps updateHUD too, so throttle the final wrapper here. */
  const baseUpdateHUD=updateHUD;
  updateHUD=function(force=false){
    const now=performance.now();
    const interval=perf.low?125:80;
    if(!force&&running&&!paused&&!modalPause&&now-lastHudPaint<interval)return;
    lastHudPaint=now;
    baseUpdateHUD();
  };

  /* Math.hypot in every seeking-projectile target scan was avoidable. */
  nearestEnemy=function(origin,range=999,filter=()=>true){
    let best=null,bestSq=range*range;
    for(const e of game.enemies){
      if(e.dead||!filter(e))continue;
      const dx=e.x-origin.x,dy=e.y-origin.y,d2=dx*dx+dy*dy;
      if(d2<bestSq){bestSq=d2;best=e;}
    }
    return best;
  };

  /* Keep visual bursts from snowballing during multi-wave rooms and bosses. */
  const baseBurst=burst;
  burst=function(x,y,color,count=12,power=1,z=10){
    const available=currentParticleCap()-game.particles.length;
    if(available<=0)return;
    baseBurst(x,y,color,Math.min(count,available),power,z);
  };

  const baseFx=fx;
  const cosmeticFx=new Set(['explosion','deathBurst','castRing','shockRing','summon','riftOpen','frostNova','lightning','soulLink','gustCone','muzzle','dashEcho','slash','shieldHit']);
  fx=function(kind,x,y,life,color,extra={}){
    if(cosmeticFx.has(kind)&&game.effects.length>(perf.low?95:145))return;
    baseFx(kind,x,y,life,color,extra);
  };

  /* Replace gradient-heavy atmosphere passes. The previous stack could create dozens
     of radial gradients every frame before combat/projectile rendering even began. */
  drawAtmosphere=function(room,pal){
    ctx.save();
    const moteCount=perf.low?(isTouch?4:6):(isTouch?7:10);
    ctx.globalAlpha=perf.low?.09:.13;
    ctx.fillStyle=colorAlpha(pal.accent,.32);
    for(let i=0;i<moteCount;i++){
      const t=(elapsed*.018+i/moteCount)%1;
      const x=(i*173+room.x*71)%Math.max(1,W),y=((i*91+room.y*43)%Math.max(1,H)+t*H*.18)%Math.max(1,H);
      ctx.beginPath();ctx.arc(x,y,5+(i%4)*3,0,TAU);ctx.fill();
    }

    const horizon=H*.24;
    ctx.globalAlpha=perf.low?.08:.12;
    ctx.fillStyle=colorAlpha(pal.accent,.22);
    for(let layer=0;layer<(perf.low?1:2);layer++){
      ctx.beginPath();ctx.moveTo(0,horizon+70+layer*30);
      for(let x=0;x<=W+120;x+=120){
        const y=horizon+layer*34+Math.sin(x*.008+room.x*.7+layer)*18+Math.sin(x*.021+room.y)*7;
        ctx.lineTo(x,y);
      }
      ctx.lineTo(W,H*.48);ctx.lineTo(0,H*.48);ctx.closePath();ctx.fill();
      ctx.globalAlpha*=.62;
    }

    const weatherCount=perf.low?7:12;
    if(room.biome==='frost'||room.biome==='volcanic'||room.biome==='swamp'){
      ctx.globalAlpha=perf.low?.13:.2;
      ctx.fillStyle=room.biome==='frost'?'#dff8ff':room.biome==='volcanic'?'#ff875e':'#b9dd7c';
      for(let i=0;i<weatherCount;i++){
        const x=(i*97+elapsed*(room.biome==='volcanic'?18:8))%(W+50)-25;
        const y=(i*67+Math.sin(i*4.1)*80+elapsed*(room.biome==='frost'?30:-8))%(H+80)-40;
        ctx.beginPath();ctx.arc(x,y,room.biome==='frost'?1.1:1.6,0,TAU);ctx.fill();
      }
    }
    ctx.restore();
  };

  function roomLightGradient(room,pal){
    const baseY=H*.46-((ROOM_W+ROOM_H)*TILE_H*.25);
    const cx=((ROOM_W/2-ROOM_H/2)*TILE_W*.5)+W/2;
    const cy=((ROOM_W/2+ROOM_H/2)*TILE_H*.5)+baseY;
    const key=[room.biome,room.town?1:0,W,H,pal.accent].join('|');
    if(!cachedLight||cachedLight.key!==key){
      const radius=Math.min(W,H)*.62;
      const gradient=ctx.createRadialGradient(cx,cy,30,cx,cy,radius);
      gradient.addColorStop(0,colorAlpha(pal.accent,room.town?.08:.035));
      gradient.addColorStop(1,colorAlpha(pal.accent,0));
      cachedLight={key,gradient};
    }
    return cachedLight.gradient;
  }

  drawRoomLight=function(room,pal){
    ctx.save();
    ctx.globalCompositeOperation='screen';
    ctx.fillStyle=roomLightGradient(room,pal);
    ctx.fillRect(0,0,W,H);

    if(!perf.low){
      const lights=[];
      if(game.player?.armorGear?.aura){
        const a=game.player.armorGear.aura;
        lights.push({x:game.player.x,y:game.player.y,c:a==='ember'?'#ff7448':a==='frost'?'#9eeaff':a==='leaf'?'#6ddd7e':a==='void'?'#b86cff':a==='sun'?'#ffd365':'#76e5ff',r:54});
      }
      for(let i=0;i<game.projectiles.length&&lights.length<6;i+=Math.max(1,Math.ceil(game.projectiles.length/5))){
        const p=game.projectiles[i];lights.push({x:p.x,y:p.y,c:p.color,r:24+p.r*30});
      }
      for(const l of lights){
        const s=worldToScreen(l.x,l.y);ctx.fillStyle=colorAlpha(l.c,.045);ctx.beginPath();ctx.arc(s.x,s.y,l.r,0,TAU);ctx.fill();
      }
    }
    ctx.restore();
    if(!perf.low&&game.enemies.some(e=>e.boss)&&typeof drawBossArena==='function')drawBossArena();
    if(!perf.low&&typeof drawForegroundWeather==='function')drawForegroundWeather(room,pal);
  };

  drawParticles=function(){
    ctx.save();ctx.globalCompositeOperation='lighter';
    const glow=!isTouch&&!perf.low&&game.particles.length<90;
    for(const p of game.particles){
      const s=worldToScreen(p.x,p.y,p.z),a=clamp(p.life/p.maxLife,0,1);
      ctx.globalAlpha=a*(p.ambient?.35:1);ctx.fillStyle=p.color;
      if(glow&&p.soft){ctx.shadowBlur=5;ctx.shadowColor=p.color;}else ctx.shadowBlur=0;
      ctx.beginPath();ctx.arc(s.x,s.y,Math.max(.5,p.size*a),0,TAU);ctx.fill();
    }
    ctx.restore();
  };

  /* Final update wrapper: adapt cosmetic quality and prevent old trail particles from
     surviving beyond the renderer's useful budget. Gameplay entities are never culled. */
  const baseUpdate=update;
  update=function(dt){
    if(running&&!paused&&!modalPause&&!roomTransition){
      const ms=Math.min(40,Math.max(1,dt*1000));
      perf.frameMs=perf.frameMs*.94+ms*.06;
      if(perf.frameMs>23){perf.slowFrames++;perf.fastFrames=0;}
      else if(perf.frameMs<18.2){perf.fastFrames++;perf.slowFrames=Math.max(0,perf.slowFrames-2);}
      else{perf.slowFrames=Math.max(0,perf.slowFrames-1);perf.fastFrames=0;}
      if(!perf.low&&perf.slowFrames>24){perf.low=true;perf.slowFrames=0;applyResolutionCap();}
      else if(perf.low&&perf.fastFrames>220){perf.low=false;perf.fastFrames=0;applyResolutionCap();}
    }

    baseUpdate(dt);

    const cap=currentParticleCap();
    if(game.particles.length>cap)game.particles.splice(0,game.particles.length-cap);
  };
})();
