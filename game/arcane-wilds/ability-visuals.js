'use strict';

/*
 * Arcane Wilds — Ability Visual Identity Layer
 * Gives the Grand Update spells unique, readable silhouettes without changing
 * save IDs. Solar Lance is presented as Solar Beam and resolves as a true,
 * room-spanning instant beam instead of borrowing Glacial Lance visuals.
 */
(function(){
  if(window.__arcaneWildsAbilityVisualsLoaded)return;
  window.__arcaneWildsAbilityVisualsLoaded=true;
  if(typeof SPELLS==='undefined'||typeof SPELL_CASTS==='undefined'||typeof drawEffect!=='function')return;

  const inherited={
    frostnova:SPELL_CASTS.frostnova,
    thorns:SPELL_CASTS.thorns,
    missiles:SPELL_CASTS.missiles,
    gust:SPELL_CASTS.gust,
    ward:SPELL_CASTS.ward,
    chain:SPELL_CASTS.chain,
    poison:SPELL_CASTS.poison,
    chakram:SPELL_CASTS.chakram,
    spirits:SPELL_CASTS.spirits
  };

  function pushEffect(kind,x,y,life,color,extra={}){
    game.effects.push({kind,x,y,life,maxLife:life,color,...extra});
  }

  function rayToRoomEdge(x,y,d,pad=.45){
    const candidates=[];
    if(d.x>0)candidates.push((ROOM_W-pad-x)/d.x);
    if(d.x<0)candidates.push((pad-x)/d.x);
    if(d.y>0)candidates.push((ROOM_H-pad-y)/d.y);
    if(d.y<0)candidates.push((pad-y)/d.y);
    const travel=Math.max(.1,Math.min(...candidates.filter(v=>Number.isFinite(v)&&v>0)));
    return {x:x+d.x*travel,y:y+d.y*travel,travel};
  }

  function lineDistance(px,py,ox,oy,dx,dy,maxTravel){
    const rx=px-ox,ry=py-oy;
    const along=rx*dx+ry*dy;
    const side=Math.abs(rx*(-dy)+ry*dx);
    return {along,side,inside:along>=-.15&&along<=maxTravel+.15};
  }

  function castSolarBeam(id,s,m){
    const p=game.player,d=spellAim(),end=rayToRoomEdge(p.x,p.y,d,.38);
    const width=hasUpgrade(id,'triple')?.62:.44;
    for(const e of [...game.enemies]){
      if(e.dead)continue;
      const hit=lineDistance(e.x,e.y,p.x,p.y,d.x,d.y,end.travel);
      if(!hit.inside||hit.side>width+e.r)continue;
      let damage=s.damage*m.power;
      if(hasUpgrade(id,'execute')&&e.hp<e.maxHp*.38)damage*=1.35;
      damageEnemy(e,damage,'fire');
      burst(e.x,e.y,'#ffd86e',6,.55,12);
    }
    if(hasUpgrade(id,'explode')){
      radialDamage(end.x,end.y,.95,s.damage*.5*m.power,'fire');
      fx('explosion',end.x,end.y,.3,'#ffd65e',{r:1.05});
    }
    if(hasUpgrade(id,'trail')){
      for(const q of [.22,.45,.68,.86]){
        const x=lerp(p.x,end.x,q),y=lerp(p.y,end.y,q);
        groundEffect('fire',x,y,.28,1.55,'#ffbf4d',s.damage*.07*m.power,.55);
      }
    }
    pushEffect('solarBeam',p.x,p.y,.58,'#ffd257',{toX:end.x,toY:end.y,width});
    burst(p.x,p.y,'#fff0a3',18,1.1,18);
    burst(end.x,end.y,'#ffca4d',15,1.0,10);
    shake=Math.max(shake,7.5);
  }

  function castStormSpear(id,s,m){
    const d=spellAim(),shots=hasUpgrade(id,'triple')?[-.15,0,.15]:[0];
    for(const off of shots){
      const a=Math.atan2(d.y,d.x)+off,dd={x:Math.cos(a),y:Math.sin(a)};
      magicProjectile({
        vx:dd.x*12.4,vy:dd.y*12.4,r:.15,
        damage:s.damage*m.power*(shots.length>1?.72:1),
        color:'#8fefff',color2:'#f5ffff',kind:'sunLance',life:1.15,pierce:6,
        splash:hasUpgrade(id,'explode')?.72:0,
        tag:hasUpgrade(id,'execute')?'iceExecute':'lightning',
        onHit:(e,q)=>{
          e.stun=Math.max(e.stun||0,.22);
          fx('lightning',q.x-dd.x*.25,q.y-dd.y*.25,.14,'#9af5ff',{toX:q.x+dd.x*.4,toY:q.y+dd.y*.4,width:2});
          burst(q.x,q.y,'#baf8ff',7,.65,12);
          if(hasUpgrade(id,'trail'))groundEffect('lightningField',q.x,q.y,.38,1.9,'#73e9ff',s.damage*.09,.5);
        }
      });
      pushEffect('stormSpearCast',game.player.x,game.player.y,.34,'#89efff',{dir:dd,range:4.8});
    }
    shake=Math.max(shake,4.5);
  }

  function castEmberComet(id,s,m){
    const d=spellAim();
    magicProjectile({
      vx:d.x*5.7,vy:d.y*5.7,r:.34,damage:s.damage*m.power,
      color:'#ff6b35',color2:'#ffe08a',kind:'fireball',life:2.25,
      seek:hasUpgrade(id,'seek')?.7:0,
      splash:hasUpgrade(id,'huge')?1.8:1.18,
      knock:hasUpgrade(id,'huge')?.8:.28,
      trail:'spark',
      onHit:(e,q)=>{
        pushEffect('emberCometImpact',q.x,q.y,.46,'#ff713d',{r:hasUpgrade(id,'huge')?1.85:1.3});
        if(hasUpgrade(id,'burn'))groundEffect('fire',q.x,q.y,hasUpgrade(id,'huge')?1.55:1.1,3.6,'#ff5e32',s.damage*.16,.33);
        if(hasUpgrade(id,'echo'))setTimeout(()=>{if(running){radialDamage(q.x,q.y,.95,s.damage*.5*m.power,'fire');pushEffect('emberCometImpact',q.x,q.y,.34,'#ffbd62',{r:1})}},250);
        if(hasUpgrade(id,'split'))for(let i=-1;i<=1;i++){
          const a=Math.atan2(q.vy,q.vx)+i*.52;
          magicProjectile({x:q.x,y:q.y,z:10,vx:Math.cos(a)*5.1,vy:Math.sin(a)*5.1,r:.13,damage:s.damage*.42*m.power,color:'#ff9b52',kind:'ember',life:1.1,splash:.3});
        }
      }
    });
    pushEffect('emberCometLaunch',game.player.x,game.player.y,.44,'#ff9148',{dir:d,range:2.5});
    shake=Math.max(shake,4);
  }

  function castGlassWinter(id,s,m){
    inherited.frostnova(id,s,m);
    pushEffect('glassWinter',game.player.x,game.player.y,.88,'#c8f5ff',{r:hasUpgrade(id,'ring')?4.8:3.2});
  }

  function castBriarCrown(id,s,m){
    inherited.thorns(id,s,m);
    pushEffect('briarCrown',game.player.x,game.player.y,.92,'#89e38f',{r:2.65,spin:Math.random()*TAU});
  }

  function castNovaSwarm(id,s,m){
    inherited.missiles(id,s,m);
    pushEffect('novaSwarm',game.player.x,game.player.y,.68,'#b9adff',{count:hasUpgrade(id,'more')?12:8});
  }

  function castCycloneWall(id,s,m){
    const d=spellAim();
    inherited.gust(id,s,m);
    pushEffect('cycloneWall',game.player.x,game.player.y,.76,'#c9f5ff',{dir:d,range:hasUpgrade(id,'wide')?6.5:5.2,width:hasUpgrade(id,'wide')?1.5:1.05});
  }

  function castMirrorAegis(id,s,m){
    inherited.ward(id,s,m);
    pushEffect('mirrorAegis',game.player.x,game.player.y,1.15,'#a8e6ff',{follow:true,r:1.2});
  }

  function castHeavenCircuit(id,s,m){
    inherited.chain(id,s,m);
    pushEffect('heavenCircuit',game.player.x,game.player.y,.62,'#99f4ff',{r:2.1});
    shake=Math.max(shake,5);
  }

  function castRotBloom(id,s,m){
    const pt=aimPoint(4.2);
    inherited.poison(id,s,m);
    pushEffect('rotBloom',pt.x,pt.y,1.05,'#99e96d',{r:hasUpgrade(id,'grow')?1.85:1.45});
  }

  function castEclipseDisc(id,s,m){
    const d=spellAim();
    inherited.chakram(id,s,m);
    pushEffect('eclipseCast',game.player.x,game.player.y,.58,'#b99cff',{dir:d,r:1.0});
  }

  function castAncestorChoir(id,s,m){
    inherited.spirits(id,s,m);
    pushEffect('ancestorChoir',game.player.x,game.player.y,1.05,'#c8efff',{count:hasUpgrade(id,'more')?8:5});
  }

  if(SPELLS.solarLance){
    SPELLS.solarLance.name='Solar Beam';
    SPELLS.solarLance.desc='Blast a single solid beam of sunlight across the entire room, piercing everything in its line before it fizzles away.';
    SPELLS.solarLance.cast='solarBeam';
  }
  if(SPELLS.stormSpear)SPELLS.stormSpear.cast='stormSpear';
  if(SPELLS.emberComet)SPELLS.emberComet.cast='emberComet';
  if(SPELLS.glassWinter)SPELLS.glassWinter.cast='glassWinter';
  if(SPELLS.briarCrown)SPELLS.briarCrown.cast='briarCrown';
  if(SPELLS.novaSwarm)SPELLS.novaSwarm.cast='novaSwarm';
  if(SPELLS.cycloneWall)SPELLS.cycloneWall.cast='cycloneWall';
  if(SPELLS.mirrorAegis)SPELLS.mirrorAegis.cast='mirrorAegis';
  if(SPELLS.heavensCircuit)SPELLS.heavensCircuit.cast='heavenCircuit';
  if(SPELLS.rotBloom)SPELLS.rotBloom.cast='rotBloom';
  if(SPELLS.eclipseDisc)SPELLS.eclipseDisc.cast='eclipseDisc';
  if(SPELLS.ancestorChoir)SPELLS.ancestorChoir.cast='ancestorChoir';

  Object.assign(SPELL_CASTS,{
    solarBeam:castSolarBeam,
    stormSpear:castStormSpear,
    emberComet:castEmberComet,
    glassWinter:castGlassWinter,
    briarCrown:castBriarCrown,
    novaSwarm:castNovaSwarm,
    cycloneWall:castCycloneWall,
    mirrorAegis:castMirrorAegis,
    heavenCircuit:castHeavenCircuit,
    rotBloom:castRotBloom,
    eclipseDisc:castEclipseDisc,
    ancestorChoir:castAncestorChoir
  });

  const baseDrawEffect=drawEffect;

  function effectProgress(e){return clamp(1-e.life/e.maxLife,0,1)}
  function effectFade(e){return clamp(e.life/Math.min(e.maxLife,.45),0,1)}
  function centerFor(e,z=0){const q=e.follow&&game.player?game.player:e;return worldToScreen(q.x,q.y,z)}

  function drawSolarBeam(e){
    const a=worldToScreen(e.x,e.y,18),b=worldToScreen(e.toX,e.toY,8),t=effectProgress(e),fade=effectFade(e),fizzle=clamp((t-.52)/.48,0,1);
    ctx.save();ctx.globalCompositeOperation='lighter';ctx.lineCap='round';
    if(fizzle>.06)ctx.setLineDash([Math.max(3,18*(1-fizzle)),5+fizzle*20]);
    ctx.shadowBlur=28;ctx.shadowColor='#ffb52f';ctx.strokeStyle=`rgba(255,177,47,${.34*fade})`;ctx.lineWidth=30*(1-fizzle*.45);ctx.beginPath();ctx.moveTo(a.x,a.y-13);ctx.lineTo(b.x,b.y-8);ctx.stroke();
    ctx.shadowBlur=18;ctx.shadowColor='#ffe26f';ctx.strokeStyle=`rgba(255,220,92,${.82*fade})`;ctx.lineWidth=11*(1-fizzle*.35);ctx.beginPath();ctx.moveTo(a.x,a.y-13);ctx.lineTo(b.x,b.y-8);ctx.stroke();
    ctx.shadowBlur=8;ctx.shadowColor='#fff';ctx.strokeStyle=`rgba(255,255,235,${.96*fade})`;ctx.lineWidth=3.2;ctx.beginPath();ctx.moveTo(a.x,a.y-13);ctx.lineTo(b.x,b.y-8);ctx.stroke();
    ctx.setLineDash([]);
    for(let i=0;i<9;i++){
      const q=(i+.5)/9,phase=elapsed*10+i*2.2;
      const x=lerp(a.x,b.x,q)+Math.sin(phase)*5*fizzle,y=lerp(a.y,b.y,q)+Math.cos(phase*1.3)*3*fizzle;
      ctx.fillStyle=`rgba(255,239,148,${(.18+.42*fizzle)*fade})`;ctx.beginPath();ctx.arc(x,y,1.5+fizzle*2,0,TAU);ctx.fill();
    }
    ctx.restore();
  }

  function drawStormSpearCast(e){
    const a=worldToScreen(e.x,e.y,18),b=worldToScreen(e.x+e.dir.x*e.range,e.y+e.dir.y*e.range,10),fade=effectFade(e);
    ctx.save();ctx.globalCompositeOperation='lighter';ctx.strokeStyle=`rgba(150,247,255,${.78*fade})`;ctx.shadowBlur=16;ctx.shadowColor=e.color;ctx.lineWidth=3;ctx.beginPath();ctx.moveTo(a.x,a.y-10);for(let i=1;i<7;i++){const q=i/7;ctx.lineTo(lerp(a.x,b.x,q)+Math.sin(i*4.7+elapsed*20)*6,lerp(a.y,b.y,q)+Math.cos(i*3.1+elapsed*15)*4)}ctx.lineTo(b.x,b.y);ctx.stroke();ctx.restore();
  }

  function drawEmberCometLaunch(e){
    const a=centerFor(e,17),b=worldToScreen(e.x+e.dir.x*e.range,e.y+e.dir.y*e.range,8),fade=effectFade(e);
    ctx.save();ctx.globalCompositeOperation='lighter';const g=ctx.createLinearGradient(a.x,a.y,b.x,b.y);g.addColorStop(0,`rgba(255,239,161,${.8*fade})`);g.addColorStop(1,'rgba(255,72,31,0)');ctx.strokeStyle=g;ctx.shadowBlur=18;ctx.shadowColor='#ff6932';ctx.lineWidth=10;ctx.beginPath();ctx.moveTo(a.x,a.y-12);ctx.lineTo(b.x,b.y);ctx.stroke();ctx.restore();
  }

  function drawEmberCometImpact(e){
    const s=centerFor(e),t=effectProgress(e),fade=effectFade(e),r=(e.r||1.3)*TILE_W*.5*(.35+t*.85);
    ctx.save();ctx.globalCompositeOperation='lighter';const g=ctx.createRadialGradient(s.x,s.y,0,s.x,s.y,r);g.addColorStop(0,`rgba(255,255,215,${.9*fade})`);g.addColorStop(.32,`rgba(255,151,69,${.68*fade})`);g.addColorStop(1,'rgba(255,66,24,0)');ctx.fillStyle=g;ctx.beginPath();ctx.arc(s.x,s.y,r,0,TAU);ctx.fill();ctx.restore();
  }

  function drawGlassWinter(e){
    const s=centerFor(e),t=effectProgress(e),fade=effectFade(e),r=(e.r||3.2)*TILE_W*.5*t;
    ctx.save();ctx.globalCompositeOperation='lighter';ctx.strokeStyle=`rgba(205,249,255,${.82*fade})`;ctx.shadowBlur=13;ctx.shadowColor=e.color;ctx.lineWidth=2.4;for(let i=0;i<16;i++){const a=i/16*TAU+Math.sin(i*7.1)*.06,x=s.x+Math.cos(a)*r,y=s.y+Math.sin(a)*r*.5,len=14+((i*11)%13);ctx.beginPath();ctx.moveTo(x-Math.cos(a)*5,y-Math.sin(a)*3);ctx.lineTo(x+Math.cos(a)*len,y+Math.sin(a)*len*.55-8);ctx.stroke()}ctx.restore();
  }

  function drawBriarCrown(e){
    const s=centerFor(e,4),t=effectProgress(e),fade=effectFade(e),r=(e.r||2.6)*TILE_W*.42*(.55+t*.35);
    ctx.save();ctx.translate(s.x,s.y);ctx.globalCompositeOperation='lighter';ctx.strokeStyle=`rgba(125,232,139,${.78*fade})`;ctx.shadowBlur=10;ctx.shadowColor=e.color;ctx.lineWidth=2.2;for(let ring=0;ring<3;ring++){ctx.beginPath();for(let i=0;i<=24;i++){const a=i/24*TAU+e.spin+elapsed*(ring%2?-.9:.9),rr=r*(.72+ring*.12)+(i%2?7:-3),x=Math.cos(a)*rr,y=Math.sin(a)*rr*.47;i?ctx.lineTo(x,y):ctx.moveTo(x,y)}ctx.stroke()}ctx.fillStyle=`rgba(255,125,176,${.5*fade})`;for(let i=0;i<8;i++){const a=i/8*TAU+elapsed*.8;ctx.beginPath();ctx.ellipse(Math.cos(a)*r*.7,Math.sin(a)*r*.34,4,2,a,0,TAU);ctx.fill()}ctx.restore();
  }

  function drawNovaSwarm(e){
    const s=centerFor(e,12),t=effectProgress(e),fade=effectFade(e),count=e.count||8;
    ctx.save();ctx.globalCompositeOperation='lighter';for(let i=0;i<count;i++){const a=i/count*TAU+elapsed*4.2,r=(18+58*t)*(1+(i%3)*.08),x=s.x+Math.cos(a)*r,y=s.y+Math.sin(a)*r*.5;ctx.shadowBlur=12;ctx.shadowColor=e.color;ctx.fillStyle=`rgba(220,215,255,${.75*fade})`;ctx.beginPath();ctx.arc(x,y,2.5+(i%2),0,TAU);ctx.fill();}ctx.restore();
  }

  function drawCycloneWall(e){
    const t=effectProgress(e),fade=effectFade(e),travel=e.range*(.45+t*.5),cx=e.x+e.dir.x*travel,cy=e.y+e.dir.y*travel,perp={x:-e.dir.y,y:e.dir.x};
    ctx.save();ctx.globalCompositeOperation='lighter';ctx.strokeStyle=`rgba(204,248,255,${.55*fade})`;ctx.shadowBlur=12;ctx.shadowColor=e.color;ctx.lineWidth=2.2;for(let layer=-3;layer<=3;layer++){const off=layer*(e.width||1)*.32,a=worldToScreen(cx+perp.x*(off-1.2),cy+perp.y*(off-1.2),4+Math.abs(layer)*3),b=worldToScreen(cx+perp.x*(off+1.2),cy+perp.y*(off+1.2),28-Math.abs(layer)*2);ctx.beginPath();ctx.moveTo(a.x,a.y);ctx.quadraticCurveTo((a.x+b.x)/2+Math.sin(elapsed*9+layer)*12,(a.y+b.y)/2-28,b.x,b.y);ctx.stroke()}ctx.restore();
  }

  function drawMirrorAegis(e){
    const s=centerFor(e,14),fade=effectFade(e),r=(e.r||1.2)*TILE_W*.46;
    ctx.save();ctx.translate(s.x,s.y);ctx.globalCompositeOperation='lighter';ctx.strokeStyle=`rgba(185,239,255,${.78*fade})`;ctx.fillStyle=`rgba(133,211,255,${.08*fade})`;ctx.shadowBlur=14;ctx.shadowColor=e.color;ctx.lineWidth=2;for(let i=0;i<6;i++){const a=i/6*TAU+elapsed*.65,x=Math.cos(a)*r,y=Math.sin(a)*r*.58;ctx.save();ctx.translate(x,y);ctx.rotate(a);ctx.beginPath();ctx.moveTo(-10,-14);ctx.lineTo(8,-9);ctx.lineTo(11,8);ctx.lineTo(-7,14);ctx.closePath();ctx.fill();ctx.stroke();ctx.restore()}ctx.restore();
  }

  function drawHeavenCircuit(e){
    const s=centerFor(e,10),t=effectProgress(e),fade=effectFade(e),r=(e.r||2.1)*TILE_W*.42*(.7+t*.25);
    ctx.save();ctx.translate(s.x,s.y);ctx.globalCompositeOperation='lighter';ctx.strokeStyle=`rgba(157,247,255,${.7*fade})`;ctx.shadowBlur=16;ctx.shadowColor=e.color;ctx.lineWidth=2;const pts=[];for(let i=0;i<8;i++){const a=i/8*TAU+elapsed*.25;pts.push({x:Math.cos(a)*r,y:Math.sin(a)*r*.5})}for(let i=0;i<pts.length;i++){const a=pts[i],b=pts[(i+3)%pts.length];ctx.beginPath();ctx.moveTo(a.x,a.y);ctx.lineTo((a.x+b.x)/2+Math.sin(elapsed*18+i)*5,(a.y+b.y)/2+Math.cos(elapsed*15+i)*4);ctx.lineTo(b.x,b.y);ctx.stroke()}ctx.restore();
  }

  function drawRotBloom(e){
    const s=centerFor(e),t=effectProgress(e),fade=effectFade(e),r=(e.r||1.5)*TILE_W*.46*(.45+t*.55);
    ctx.save();ctx.translate(s.x,s.y);ctx.globalCompositeOperation='lighter';ctx.strokeStyle=`rgba(166,240,103,${.56*fade})`;ctx.fillStyle=`rgba(119,208,83,${.11*fade})`;ctx.shadowBlur=10;ctx.shadowColor=e.color;for(let i=0;i<9;i++){const a=i/9*TAU+elapsed*.2;ctx.save();ctx.rotate(a);ctx.beginPath();ctx.ellipse(r*.48,0,r*.42,r*.16,0,0,TAU);ctx.fill();ctx.stroke();ctx.restore()}for(let i=0;i<10;i++){const a=i/10*TAU-elapsed*.7,rr=r*(.35+(i%3)*.2);ctx.fillStyle=`rgba(205,255,135,${.45*fade})`;ctx.beginPath();ctx.arc(Math.cos(a)*rr,Math.sin(a)*rr*.48,2+(i%2),0,TAU);ctx.fill()}ctx.restore();
  }

  function drawEclipseCast(e){
    const s=centerFor(e,12),t=effectProgress(e),fade=effectFade(e),r=(e.r||1)*TILE_W*.45*(.8+t*.35),a=Math.atan2(e.dir.y,e.dir.x);
    ctx.save();ctx.translate(s.x,s.y);ctx.rotate(a*.45);ctx.globalCompositeOperation='lighter';ctx.shadowBlur=18;ctx.shadowColor='#8e6bff';ctx.strokeStyle=`rgba(190,168,255,${.74*fade})`;ctx.lineWidth=5;ctx.beginPath();ctx.arc(0,0,r,-1.15,1.15);ctx.stroke();ctx.strokeStyle=`rgba(18,10,35,${.9*fade})`;ctx.lineWidth=8;ctx.beginPath();ctx.arc(7,0,r*.78,-1.2,1.2);ctx.stroke();ctx.restore();
  }

  function drawAncestorChoir(e){
    const s=centerFor(e,16),t=effectProgress(e),fade=effectFade(e),count=e.count||5;
    ctx.save();ctx.globalCompositeOperation='lighter';for(let i=0;i<count;i++){const a=i/count*TAU+elapsed*.9,r=28+34*t,x=s.x+Math.cos(a)*r,y=s.y+Math.sin(a)*r*.45-10;ctx.strokeStyle=`rgba(210,246,255,${.5*fade})`;ctx.fillStyle=`rgba(185,222,255,${.12*fade})`;ctx.shadowBlur=10;ctx.shadowColor=e.color;ctx.beginPath();ctx.arc(x,y,7,0,TAU);ctx.fill();ctx.stroke();ctx.beginPath();ctx.moveTo(x-5,y+6);ctx.quadraticCurveTo(x,y+18+Math.sin(elapsed*5+i)*4,x+5,y+6);ctx.stroke()}ctx.restore();
  }

  drawEffect=function(e,front){
    if(e.kind==='solarBeam')return drawSolarBeam(e);
    if(e.kind==='stormSpearCast')return drawStormSpearCast(e);
    if(e.kind==='emberCometLaunch')return drawEmberCometLaunch(e);
    if(e.kind==='emberCometImpact')return drawEmberCometImpact(e);
    if(e.kind==='glassWinter')return drawGlassWinter(e);
    if(e.kind==='briarCrown')return drawBriarCrown(e);
    if(e.kind==='novaSwarm')return drawNovaSwarm(e);
    if(e.kind==='cycloneWall')return drawCycloneWall(e);
    if(e.kind==='mirrorAegis')return drawMirrorAegis(e);
    if(e.kind==='heavenCircuit')return drawHeavenCircuit(e);
    if(e.kind==='rotBloom')return drawRotBloom(e);
    if(e.kind==='eclipseCast')return drawEclipseCast(e);
    if(e.kind==='ancestorChoir')return drawAncestorChoir(e);
    return baseDrawEffect(e,front);
  };
})();
