'use strict';

/*
 * Arcane Wilds lightweight visual depth pass.
 * Adds stronger biome identity, silhouettes, equipment readability, pooled combat scars,
 * cheap projectile trails and richer town accents without increasing simulation load.
 */
(()=>{
  if(window.__arcaneWildsVisualDepthLoaded)return;
  window.__arcaneWildsVisualDepthLoaded=true;

  const VISUAL_DECAL_CAP=isTouch?7:10;
  const visualDecals=[];
  window.arcaneWildsVisualDecals=visualDecals;

  const BIOME_VISUALS={
    meadow:{motif:'grass',horizon:'softHills',weather:'pollen'},
    forest:{motif:'roots',horizon:'canopy',weather:'leaves'},
    ruins:{motif:'cracks',horizon:'ruins',weather:'dust'},
    swamp:{motif:'puddles',horizon:'canopy',weather:'spores'},
    frost:{motif:'ice',horizon:'peaks',weather:'snow'},
    desert:{motif:'dunes',horizon:'mesa',weather:'sand'},
    volcanic:{motif:'lava',horizon:'peaks',weather:'embers'},
    crypt:{motif:'runes',horizon:'ruins',weather:'ash'},
    town:{motif:'cobbles',horizon:'town',weather:'chimney'},
    thornwild:{motif:'thorns',horizon:'canopy',weather:'petals'},
    crystal:{motif:'facets',horizon:'crystal',weather:'shards'},
    stormlands:{motif:'wind',horizon:'mesa',weather:'storm'},
    gloam:{motif:'runes',horizon:'cavern',weather:'void'},
    celestial:{motif:'stars',horizon:'celestial',weather:'stars'},
    bloodroot:{motif:'roots',horizon:'twisted',weather:'redLeaves'}
  };

  function perfLow(){return !!window.arcaneWildsPerformance?.low}
  function visualProfile(room){return BIOME_VISUALS[room?.biome]||BIOME_VISUALS.meadow}
  function roomVisualSeed(room,salt){return hash2(room.x*31+salt,room.y*37-salt,room.seed+salt)}

  function ensureRoomVisualData(room){
    if(room._lightVisualData)return room._lightVisualData;
    const marks=[];
    const count=isTouch?22:30;
    for(let i=0;i<count;i++){
      marks.push({
        x:.55+roomVisualSeed(room,110+i*7)*(ROOM_W-1.1),
        y:.55+roomVisualSeed(room,111+i*7)*(ROOM_H-1.1),
        size:.55+roomVisualSeed(room,112+i*7)*1.15,
        rot:roomVisualSeed(room,113+i*7)*TAU,
        phase:roomVisualSeed(room,114+i*7)*TAU
      });
    }
    const horizon=[];
    for(let i=0;i<10;i++)horizon.push({
      x:roomVisualSeed(room,700+i*5),
      size:.65+roomVisualSeed(room,701+i*5)*.9,
      lift:roomVisualSeed(room,702+i*5)
    });
    room._lightVisualData={marks,horizon};
    return room._lightVisualData;
  }

  function drawGroundMark(mark,motif,pal,index){
    const s=worldToScreen(mark.x,mark.y),q=mark.size;
    ctx.save();ctx.translate(s.x,s.y);ctx.rotate(mark.rot);
    ctx.lineWidth=1;ctx.lineCap='round';ctx.lineJoin='round';
    ctx.strokeStyle=colorAlpha(pal.accent,.17);ctx.fillStyle=colorAlpha(pal.accent,.055);
    if(motif==='grass'||motif==='thorns'){
      const thorn=motif==='thorns';
      for(let k=-1;k<=1;k++){
        ctx.beginPath();ctx.moveTo(k*4*q,2*q);ctx.quadraticCurveTo((k*5+(thorn?3:0))*q,-4*q,(k*6+(thorn?5:1))*q,-9*q);ctx.stroke();
      }
      if(thorn){ctx.beginPath();ctx.moveTo(-6*q,-3*q);ctx.lineTo(6*q,2*q);ctx.stroke()}
    }else if(motif==='roots'){
      ctx.beginPath();ctx.moveTo(-11*q,0);ctx.bezierCurveTo(-4*q,-5*q,2*q,5*q,11*q,-1*q);ctx.stroke();
      ctx.beginPath();ctx.moveTo(-1*q,1*q);ctx.lineTo(5*q,7*q);ctx.stroke();
    }else if(motif==='cracks'||motif==='lava'||motif==='ice'){
      ctx.beginPath();ctx.moveTo(-10*q,1*q);ctx.lineTo(-3*q,-4*q);ctx.lineTo(2*q,3*q);ctx.lineTo(10*q,-2*q);ctx.stroke();
      if(motif==='lava'){ctx.strokeStyle='rgba(255,111,73,.25)';ctx.lineWidth=1.4;ctx.stroke()}
      if(motif==='ice'){ctx.beginPath();ctx.moveTo(2*q,3*q);ctx.lineTo(7*q,8*q);ctx.stroke()}
    }else if(motif==='puddles'){
      ctx.beginPath();ctx.ellipse(0,0,13*q,4*q,0,0,TAU);ctx.fill();ctx.stroke();
    }else if(motif==='dunes'||motif==='wind'){
      ctx.beginPath();ctx.moveTo(-14*q,1*q);ctx.quadraticCurveTo(-3*q,-4*q,7*q,0);ctx.quadraticCurveTo(11*q,2*q,15*q,-1*q);ctx.stroke();
      if(motif==='wind'&&index%3===0){ctx.beginPath();ctx.moveTo(-8*q,5*q);ctx.lineTo(9*q,3*q);ctx.stroke()}
    }else if(motif==='runes'||motif==='stars'){
      const star=motif==='stars';
      ctx.beginPath();
      if(star){ctx.moveTo(0,-7*q);ctx.lineTo(2*q,-2*q);ctx.lineTo(7*q,0);ctx.lineTo(2*q,2*q);ctx.lineTo(0,7*q);ctx.lineTo(-2*q,2*q);ctx.lineTo(-7*q,0);ctx.lineTo(-2*q,-2*q);ctx.closePath()}
      else{ctx.arc(0,0,7*q,0,TAU);ctx.moveTo(-5*q,0);ctx.lineTo(5*q,0);ctx.moveTo(0,-5*q);ctx.lineTo(0,5*q)}
      ctx.stroke();
    }else if(motif==='cobbles'){
      ctx.beginPath();ctx.ellipse(-5*q,0,7*q,3*q,0,0,TAU);ctx.ellipse(7*q,-1*q,6*q,3*q,0,0,TAU);ctx.stroke();
    }else if(motif==='facets'){
      ctx.beginPath();ctx.moveTo(0,-9*q);ctx.lineTo(8*q,-1*q);ctx.lineTo(4*q,7*q);ctx.lineTo(-6*q,5*q);ctx.lineTo(-9*q,-2*q);ctx.closePath();ctx.stroke();
      ctx.beginPath();ctx.moveTo(0,-9*q);ctx.lineTo(1*q,3*q);ctx.lineTo(8*q,-1*q);ctx.stroke();
    }
    ctx.restore();
  }

  function drawVisualDecals(){
    if(!visualDecals.length)return;
    for(const d of visualDecals){
      const s=worldToScreen(d.x,d.y),a=clamp(d.life/d.maxLife,0,1);
      ctx.save();ctx.translate(s.x,s.y);ctx.globalAlpha=.3*a;ctx.strokeStyle=d.color||'#d9e5ff';ctx.fillStyle=colorAlpha(d.color||'#d9e5ff',.08);ctx.lineWidth=1.2;
      if(d.kind==='slash'){
        ctx.beginPath();ctx.moveTo(-15,-4);ctx.lineTo(14,5);ctx.stroke();
      }else if(d.kind==='frostNova'){
        for(let i=0;i<6;i++){const t=i/6*TAU;ctx.beginPath();ctx.moveTo(Math.cos(t)*6,Math.sin(t)*3);ctx.lineTo(Math.cos(t)*19,Math.sin(t)*8);ctx.stroke()}
      }else{
        ctx.beginPath();ctx.ellipse(0,2,18+(1-a)*5,8+(1-a)*2,0,0,TAU);ctx.fill();ctx.stroke();
        if(d.kind==='deathBurst'){ctx.beginPath();ctx.moveTo(-10,-4);ctx.lineTo(9,6);ctx.moveTo(8,-5);ctx.lineTo(-7,7);ctx.stroke()}
      }
      ctx.restore();
    }
  }

  /* Replace the old per-tile floor repaint with a deterministic, richer and much smaller mark set. */
  drawFloorDetails=function(room,pal){
    const data=ensureRoomVisualData(room),profile=visualProfile(room),limit=perfLow()?12:(isTouch?18:data.marks.length);
    ctx.save();
    for(let i=0;i<limit;i++)drawGroundMark(data.marks[i],profile.motif,pal,i);
    const sparkleLimit=perfLow()?4:Math.min(room.deco.length,isTouch?8:12);
    ctx.strokeStyle=colorAlpha(pal.accent,.18);ctx.lineWidth=.8;
    for(let i=0;i<sparkleLimit;i++){
      const d=room.deco[i],s=worldToScreen(d.x,d.y);ctx.beginPath();ctx.arc(s.x,s.y,1.3+d.size*.35,0,TAU);ctx.stroke();
    }
    ctx.restore();
    if(room.biome==='town')drawTownRoads();
    drawVisualDecals();
  };

  function drawBiomeHorizon(room,pal){
    const data=ensureRoomVisualData(room),profile=visualProfile(room),h=H*.25;
    const count=perfLow()?4:(isTouch?6:8);
    ctx.save();ctx.fillStyle=colorAlpha(pal.accent,.10);ctx.strokeStyle=colorAlpha(pal.accent,.12);ctx.lineWidth=2;
    for(let i=0;i<count;i++){
      const item=data.horizon[i],x=item.x*W,size=item.size,base=h+34+item.lift*18;
      if(profile.horizon==='canopy'||profile.horizon==='twisted'){
        ctx.fillRect(x-2.5*size,base-48*size,5*size,48*size);
        ctx.beginPath();ctx.arc(x,base-52*size,16*size,0,TAU);ctx.fill();
        if(profile.horizon==='twisted'){ctx.beginPath();ctx.moveTo(x,base-42*size);ctx.lineTo(x-17*size,base-66*size);ctx.moveTo(x+2*size,base-38*size);ctx.lineTo(x+18*size,base-58*size);ctx.stroke()}
      }else if(profile.horizon==='ruins'||profile.horizon==='town'){
        const width=(12+(i%3)*7)*size,height=(25+(i%4)*11)*size;
        ctx.fillRect(x-width*.5,base-height,width,height);
        if(profile.horizon==='town'){ctx.beginPath();ctx.moveTo(x-width*.65,base-height);ctx.lineTo(x,base-height-12*size);ctx.lineTo(x+width*.65,base-height);ctx.closePath();ctx.fill()}
      }else if(profile.horizon==='crystal'||profile.horizon==='peaks'){
        const tall=(35+(i%4)*16)*size;
        ctx.beginPath();ctx.moveTo(x-18*size,base);ctx.lineTo(x,base-tall);ctx.lineTo(x+15*size,base);ctx.closePath();ctx.fill();
        if(profile.horizon==='crystal'){ctx.beginPath();ctx.moveTo(x,base-tall);ctx.lineTo(x+3*size,base-5*size);ctx.stroke()}
      }else if(profile.horizon==='mesa'||profile.horizon==='softHills'){
        ctx.beginPath();ctx.moveTo(x-30*size,base);ctx.quadraticCurveTo(x,base-(profile.horizon==='mesa'?22:14)*size,x+30*size,base);ctx.closePath();ctx.fill();
      }else if(profile.horizon==='celestial'){
        ctx.fillRect(x-4*size,base-55*size,8*size,55*size);
        ctx.beginPath();ctx.arc(x,base-56*size,10*size,Math.PI,TAU);ctx.stroke();
      }else if(profile.horizon==='cavern'){
        ctx.beginPath();ctx.moveTo(x-24*size,h-45*size);ctx.lineTo(x,h+item.lift*16);ctx.lineTo(x+22*size,h-45*size);ctx.closePath();ctx.fill();
      }
    }
    ctx.restore();
  }

  const baseDrawRoomBase=drawRoomBase;
  drawRoomBase=function(room,pal){drawBiomeHorizon(room,pal);baseDrawRoomBase(room,pal)};

  /* Keep the optimized performance governor's foreground-weather hook, but make every region distinct. */
  drawForegroundWeather=function(room,pal){
    if(perfLow())return;
    const kind=visualProfile(room).weather,count=isTouch?7:11;
    ctx.save();ctx.globalCompositeOperation='screen';ctx.lineCap='round';
    for(let i=0;i<count;i++){
      const phase=i*79+room.x*23-room.y*17;
      let x=(phase+elapsed*(kind==='storm'?52:kind==='sand'?28:kind==='snow'?20:10))%(W+60)-30;
      let y=(i*61+elapsed*(kind==='embers'?-24:kind==='snow'?34:kind==='stars'?24:8))%(H+80)-40;
      if(kind==='snow'){
        ctx.strokeStyle='rgba(224,249,255,.22)';ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(x,y);ctx.lineTo(x-4,y+9);ctx.stroke();
      }else if(kind==='embers'){
        y=H-((i*71+elapsed*35)%(H+60));ctx.fillStyle='rgba(255,131,78,.24)';ctx.beginPath();ctx.arc(x,y,1+(i%2),0,TAU);ctx.fill();
      }else if(kind==='storm'){
        ctx.strokeStyle='rgba(143,224,255,.17)';ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(x,y);ctx.lineTo(x+15,y+2);ctx.stroke();
        if(i===0&&Math.sin(elapsed*.72)>0.997){ctx.fillStyle='rgba(185,236,255,.07)';ctx.fillRect(0,0,W,H)}
      }else if(kind==='spores'||kind==='pollen'||kind==='petals'||kind==='void'||kind==='shards'||kind==='redLeaves'){
        const c=kind==='void'?'#d07cff':kind==='shards'?'#8ef4ff':kind==='redLeaves'?'#ff776e':kind==='petals'?'#f28eb3':kind==='spores'?'#b9dd7c':pal.accent;
        ctx.fillStyle=colorAlpha(c,.16);ctx.save();ctx.translate(x,y);ctx.rotate(elapsed*.25+i);ctx.beginPath();
        if(kind==='shards'){ctx.moveTo(0,-4);ctx.lineTo(2,0);ctx.lineTo(0,4);ctx.lineTo(-2,0);ctx.closePath()}
        else{ctx.ellipse(0,0,kind==='petals'||kind==='redLeaves'?3.5:2.2,1.5,0,0,TAU)}
        ctx.fill();ctx.restore();
      }else if(kind==='stars'){
        ctx.strokeStyle='rgba(255,230,175,.18)';ctx.beginPath();ctx.moveTo(x,y);ctx.lineTo(x-7,y+7);ctx.stroke();
      }else if(kind==='sand'||kind==='dust'||kind==='ash'){
        ctx.strokeStyle=colorAlpha(kind==='ash'?'#d6c6c0':pal.accent,.12);ctx.beginPath();ctx.moveTo(x,y);ctx.lineTo(x+9,y+1);ctx.stroke();
      }else if(kind==='leaves'||kind==='chimney'){
        ctx.fillStyle=colorAlpha(kind==='chimney'?'#d5c7b6':'#83b36d',.13);ctx.beginPath();ctx.arc(x,y,kind==='chimney'?3:2,0,TAU);ctx.fill();
      }
    }
    ctx.restore();
  };

  function drawTownPropAccent(p){
    if(!['house','market','lamp','banner','fountain'].includes(p.type))return;
    const s=worldToScreen(p.x,p.y),sc=p.scale||1;
    ctx.save();ctx.translate(s.x,s.y);ctx.lineWidth=1;
    if(p.type==='house'){
      ctx.fillStyle='rgba(255,224,145,.34)';ctx.fillRect(-10*sc,-28*sc,6*sc,8*sc);ctx.fillRect(4*sc,-25*sc,6*sc,7*sc);
      ctx.strokeStyle='rgba(60,39,25,.45)';ctx.strokeRect(-10*sc,-28*sc,6*sc,8*sc);ctx.strokeRect(4*sc,-25*sc,6*sc,7*sc);
    }else if(p.type==='market'){
      ctx.fillStyle='rgba(225,104,113,.35)';for(let i=-2;i<=2;i++)ctx.fillRect(i*5*sc,-24*sc,4*sc,5*sc);
    }else if(p.type==='lamp'){
      ctx.fillStyle='rgba(255,221,139,.55)';ctx.beginPath();ctx.arc(0,-24*sc,3*sc,0,TAU);ctx.fill();
    }else if(p.type==='banner'){
      ctx.strokeStyle='rgba(255,255,255,.16)';ctx.beginPath();ctx.moveTo(-3*sc,-30*sc);ctx.lineTo(7*sc,-27*sc);ctx.stroke();
    }else if(p.type==='fountain'){
      ctx.strokeStyle='rgba(145,225,255,.25)';ctx.beginPath();ctx.moveTo(0,-14*sc);ctx.quadraticCurveTo(8*sc,-22*sc,9*sc,-7*sc);ctx.moveTo(0,-14*sc);ctx.quadraticCurveTo(-8*sc,-22*sc,-9*sc,-7*sc);ctx.stroke();
    }
    ctx.restore();
  }

  const baseDrawProp=drawProp;
  drawProp=function(p,biome){baseDrawProp(p,biome);if(biome==='town')drawTownPropAccent(p)};

  function weaponClass(player){
    const w=player?.weapon||{},name=`${w.type||''} ${w.baseName||''} ${w.name||''}`.toLowerCase();
    if(name.includes('bow'))return 'bow';if(name.includes('staff')||name.includes('wand'))return 'staff';if(name.includes('axe'))return 'axe';
    if(name.includes('spear')||name.includes('lance'))return 'spear';if(name.includes('hammer')||name.includes('mace'))return 'hammer';if(name.includes('dagger'))return 'dagger';return 'blade';
  }

  function drawPlayerEquipment(player){
    const s=worldToScreen(player.x,player.y),kind=weaponClass(player),w=player.weapon||{};
    const c=rarityColors?.[w.rarity]||'#dfe9f5';
    ctx.save();ctx.translate(s.x+11,s.y-18);ctx.rotate(-.48);ctx.strokeStyle=colorAlpha(c,.82);ctx.fillStyle=colorAlpha(c,.58);ctx.lineWidth=2;ctx.lineCap='round';
    if(kind==='bow'){
      ctx.beginPath();ctx.arc(0,0,12,-1.15,1.15);ctx.stroke();ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(5,-11);ctx.lineTo(5,11);ctx.stroke();
    }else{
      ctx.beginPath();ctx.moveTo(0,8);ctx.lineTo(0,-12);ctx.stroke();
      if(kind==='staff'){ctx.beginPath();ctx.arc(0,-15,3.5,0,TAU);ctx.fill()}
      else if(kind==='axe'){ctx.beginPath();ctx.moveTo(0,-10);ctx.lineTo(8,-15);ctx.lineTo(8,-7);ctx.closePath();ctx.fill()}
      else if(kind==='hammer'){ctx.fillRect(-5,-16,10,6)}
      else if(kind==='spear'){ctx.beginPath();ctx.moveTo(0,-18);ctx.lineTo(4,-10);ctx.lineTo(-4,-10);ctx.closePath();ctx.fill()}
      else if(kind==='dagger'||kind==='blade'){ctx.beginPath();ctx.moveTo(0,-19);ctx.lineTo(3,-9);ctx.lineTo(-3,-9);ctx.closePath();ctx.fill()}
    }
    const armor=String(player.armorGear?.name||'').toLowerCase();
    ctx.rotate(.48);ctx.strokeStyle='rgba(224,236,248,.22)';ctx.lineWidth=2;
    if(armor.includes('plate')||armor.includes('guard')||armor.includes('knight')){ctx.beginPath();ctx.arc(-11,4,7,Math.PI,TAU);ctx.arc(9,4,7,Math.PI,TAU);ctx.stroke()}
    else if(armor.includes('robe')||armor.includes('cloth')){ctx.beginPath();ctx.moveTo(-7,5);ctx.lineTo(-12,15);ctx.moveTo(6,5);ctx.lineTo(11,15);ctx.stroke()}
    ctx.restore();
  }

  const baseDrawPlayer=drawPlayer;
  drawPlayer=function(player){baseDrawPlayer(player);drawPlayerEquipment(player)};

  const ENEMY_ACCENTS={
    thorn:new Set(['briarprowler','bloomhexer','rootjuggernaut','thorncolossus']),
    crystal:new Set(['prismscarab','glassoracle','shardram']),
    storm:new Set(['stormhound','skyraider','galeweaver','stormcaller']),
    void:new Set(['duskblade','voidmoth','abyssanchor','voideye','chainwarden']),
    celestial:new Set(['sunwarden','starcaller','astralknight','mirrormage']),
    horn:new Set(['marrowboar','charger','drake','executioner'])
  };

  function enemyAccentKind(type){for(const [k,set] of Object.entries(ENEMY_ACCENTS))if(set.has(type))return k;return null}
  function drawEnemyAccent(e){
    if(perfLow()&&!e.boss)return;
    const kind=enemyAccentKind(e.type);if(!kind&&!e.boss)return;
    const s=worldToScreen(e.x,e.y),r=Math.max(9,(e.r||.35)*29),c=e.proj||e.color||'#d8e7ff',bob=Math.sin(elapsed*3+(e.x+e.y))*1.2;
    ctx.save();ctx.translate(s.x,s.y-r*.72+bob);ctx.strokeStyle=colorAlpha(c,.62);ctx.fillStyle=colorAlpha(c,.30);ctx.lineWidth=1.5;ctx.lineCap='round';
    if(kind==='thorn'){
      for(let i=-1;i<=1;i++){ctx.beginPath();ctx.moveTo(i*r*.35,0);ctx.lineTo(i*r*.55+(i===0?3:0),-r*.55);ctx.lineTo(i*r*.18,-r*.28);ctx.stroke()}
    }else if(kind==='crystal'){
      for(let i=-1;i<=1;i++){ctx.beginPath();ctx.moveTo(i*r*.4,0);ctx.lineTo(i*r*.28,-r*(.55+Math.abs(i)*.1));ctx.lineTo(i*r*.08,-r*.15);ctx.closePath();ctx.fill()}
    }else if(kind==='storm'){
      ctx.beginPath();ctx.moveTo(-r*.7,-r*.1);ctx.lineTo(-r*.15,-r*.28);ctx.lineTo(-r*.35,r*.05);ctx.lineTo(r*.55,-r*.15);ctx.stroke();
    }else if(kind==='void'){
      ctx.setLineDash([3,3]);ctx.beginPath();ctx.ellipse(0,-r*.18,r*.72,r*.22,0,0,TAU);ctx.stroke();ctx.setLineDash([]);
    }else if(kind==='celestial'){
      ctx.beginPath();ctx.arc(0,-r*.25,r*.55,Math.PI,TAU);ctx.stroke();ctx.beginPath();ctx.moveTo(0,-r*.72);ctx.lineTo(0,-r*.35);ctx.moveTo(-r*.18,-r*.54);ctx.lineTo(r*.18,-r*.54);ctx.stroke();
    }else if(kind==='horn'){
      ctx.beginPath();ctx.moveTo(-r*.28,-r*.05);ctx.lineTo(-r*.65,-r*.5);ctx.lineTo(-r*.15,-r*.32);ctx.moveTo(r*.28,-r*.05);ctx.lineTo(r*.65,-r*.5);ctx.lineTo(r*.15,-r*.32);ctx.stroke();
    }
    if(e.boss){ctx.strokeStyle=colorAlpha(c,.38);ctx.beginPath();ctx.ellipse(0,-r*.12,r*.9,r*.28,0,0,TAU);ctx.stroke()}
    ctx.restore();
  }

  const baseDrawEnemy=drawEnemy;
  drawEnemy=function(e){baseDrawEnemy(e);drawEnemyAccent(e)};

  const baseDrawProjectiles=drawProjectiles;
  drawProjectiles=function(){
    if(!perfLow()){
      const list=game.projectiles||[],cap=isTouch?12:22,step=Math.max(1,Math.ceil(list.length/cap));
      ctx.save();ctx.lineCap='round';
      for(let i=0;i<list.length;i+=step){
        const p=list[i],vx=Number(p.vx||0),vy=Number(p.vy||0);if(vx*vx+vy*vy<.05)continue;
        const a=worldToScreen(p.x,p.y),b=worldToScreen(p.x-vx*.075,p.y-vy*.075);
        ctx.strokeStyle=colorAlpha(p.color||'#d8e7ff',.24);ctx.lineWidth=Math.max(1,Math.min(3,(p.r||.08)*12));ctx.beginPath();ctx.moveTo(b.x,b.y);ctx.lineTo(a.x,a.y);ctx.stroke();
      }
      ctx.restore();
    }
    baseDrawProjectiles();
  };

  const baseFx=fx;
  fx=function(kind,x,y,life,color,extra={}){
    if(['explosion','deathBurst','shockRing','frostNova','slash'].includes(kind)){
      visualDecals.push({kind,x,y,color:color||'#d8e7ff',life:kind==='slash'?1.8:3.6,maxLife:kind==='slash'?1.8:3.6});
      if(visualDecals.length>VISUAL_DECAL_CAP)visualDecals.splice(0,visualDecals.length-VISUAL_DECAL_CAP);
    }
    baseFx(kind,x,y,life,color,extra);
  };

  const baseUpdate=update;
  update=function(dt){
    baseUpdate(dt);
    if(!running||paused||modalPause||roomTransition||!visualDecals.length)return;
    for(let i=visualDecals.length-1;i>=0;i--){visualDecals[i].life-=dt;if(visualDecals[i].life<=0)visualDecals.splice(i,1)}
  };
})();
