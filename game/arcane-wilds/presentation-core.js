'use strict';
/* Presentation state is transient. The separate preferences key never changes journey saves. */
(() => {
  if(window.AWPresentation)return;
  const KEY='arcaneWildsPresentationV1';
  const defaults={version:1,quality:'auto',particles:'high',weather:'high',lighting:'high',shake:.5,damageNumbers:true,deadzone:.18,aimSensitivity:1,music:.18,effects:.55,keys:{},buttons:{}};
  let settings={...defaults};
  try{const saved=JSON.parse(localStorage.getItem(KEY)||'{}');settings={...defaults,...saved,version:1};}catch(_){}
  for(const key of ['shake','music','effects'])settings[key]=Number.isFinite(settings[key])?Math.max(0,Math.min(1,settings[key])):defaults[key];
  settings.deadzone=Number.isFinite(settings.deadzone)?Math.max(.05,Math.min(.4,settings.deadzone)):.18;
  settings.aimSensitivity=Number.isFinite(settings.aimSensitivity)?Math.max(.3,Math.min(3,settings.aimSensitivity)):1;
  for(const key of ['quality','particles','weather','lighting'])if(!['auto','high','medium','low','off'].includes(settings[key]))settings[key]=defaults[key];
  for(const key of ['keys','buttons'])if(!settings[key]||typeof settings[key]!=='object'||Array.isArray(settings[key]))settings[key]={};
  const poses=new WeakMap(),bosses=new WeakMap(),effectRenderers=new Map();
  let quality=isTouch?'medium':'high',frameAt=0,frameMs=16.7,slow=0,fast=0,roomRef=null,roomTitle=0,priorBiome=null,footstep=0;
  let intro=null,hitPause=0,lastImpact=0;
  const camera={x:0,y:0,zoom:1,ready:false,kick:0,shakeX:0,shakeY:0,
    project(x,y,z=0){return {x:W*.5+((x-y)-(this.x-this.y))*TILE_W*.5*this.zoom+this.shakeX,y:H*.53+((x+y)-(this.x+this.y))*TILE_H*.5*this.zoom-z*this.zoom+this.shakeY};},
    tick(dt){
      const p=game.player;if(!p)return;
      let tx=p.x,ty=p.y;
      if(intro){const t=intro.time;if(t>.45&&t<1.4){tx=lerp(tx,intro.boss.x,.82);ty=lerp(ty,intro.boss.y,.82);}}
      if(!this.ready){this.x=tx;this.y=ty;this.ready=true;}
      const blend=1-Math.exp(-dt*(game.enemies.length?13:9));
      this.x=lerp(this.x,tx,blend);this.y=lerp(this.y,ty,blend);
      const target=game.enemies.length>14?.93:game.enemies.some(e=>e.boss)?.96:1;
      this.kick=Math.max(0,this.kick-dt*.22);this.zoom=lerp(this.zoom,target+this.kick,1-Math.exp(-dt*6));
      this.shakeX=Math.sin(frameAt*.073)*Math.min(8,shake)*settings.shake;
      this.shakeY=Math.sin(frameAt*.097)*Math.min(5,shake)*settings.shake;
    }
  };
  class Pool{
    constructor(cap){this.items=Array.from({length:cap},()=>({life:0}));this.cursor=0;}
    add(data){let item=this.items.find(e=>e.life<=0);if(!item)item=this.items[this.cursor++%this.items.length];for(const k of Object.keys(item))delete item[k];Object.assign(item,data);item.maxLife=data.life;return item;}
    tick(dt){for(const e of this.items)if(e.life>0)e.life=Math.max(0,e.life-dt);}
    clear(){for(const e of this.items)e.life=0;}
  }
  const decals=new Pool(40),bursts=new Pool(80),lights=new Pool(12),deaths=new Pool(28);
  function colorFor(id=''){return /frost|ice|winter/i.test(id)?'#b1efff':/fire|ember|meteor|phoenix|furnace/i.test(id)?'#ff9958':/chain|lightning|storm|thunder/i.test(id)?'#8ff4ff':/thorn|briar|root|venom|poison|rot/i.test(id)?'#9be58c':/star|solar|sun|celestial|seraph/i.test(id)?'#ffe4a0':/spirit|soul|ancestor/i.test(id)?'#aee5e4':'#c3a6ff';}
  function pose(e){let s=poses.get(e);if(!s){s={state:'idle',until:0,x:e.x,y:e.y,stride:0,speed:0,recoilX:0,recoilY:0};poses.set(e,s);}return s;}
  function animate(e,state,duration=.25){if(!e)return;const s=pose(e);s.state=state;s.until=frameAt+duration*1000;}
  function ring(x,y,color,kind='cast',life=.4,size=1){return bursts.add({x,y,color,kind,life,size});}
  const audio={context:null,voices:0,last:new Map(),ambience:null,
    unlock(){
      try{if(!this.context){const Audio=window.AudioContext||window.webkitAudioContext;if(!Audio)return;this.context=new Audio();}if(this.context.state==='suspended')this.context.resume().catch(()=>{});}catch(_){}
    },
    play(kind,color=''){
      const ac=this.context;if(!ac||ac.state!=='running'||settings.effects<=0||this.voices>=12)return;
      const now=ac.currentTime;if(now-(this.last.get(kind)??-10)<(kind==='hit'?.055:.09))return;this.last.set(kind,now);
      const pitch={attack:230,cast:480,hit:130,hurt:95,heal:650,dodge:320,perfect:940,reaction:740,death:90,level:880,loot:720,interact:420,boss:65,phase:110,step:70}[kind]||300;
      const osc=ac.createOscillator(),gain=ac.createGain(),length=kind==='boss'?.65:kind==='level'?.4:.14;
      osc.type=['hit','hurt','death','step'].includes(kind)?'triangle':'sine';
      osc.frequency.setValueAtTime(pitch+(color.includes('ff99')?-50:0),now);osc.frequency.exponentialRampToValueAtTime(Math.max(30,pitch*(kind==='heal'||kind==='level'?1.8:.45)),now+length);
      gain.gain.setValueAtTime(0,now);gain.gain.linearRampToValueAtTime(settings.effects*(kind==='step'?.035:.09),now+.008);gain.gain.exponentialRampToValueAtTime(.0001,now+length);
      osc.connect(gain);gain.connect(ac.destination);this.voices++;osc.onended=()=>{this.voices--;osc.disconnect();gain.disconnect();};osc.start();osc.stop(now+length+.02);
    },
    ambient(biome){
      const ac=this.context;if(!ac||ac.state!=='running')return;
      if(this.ambience?.biome===biome){this.ambience.gain.gain.setTargetAtTime(settings.music*.022,ac.currentTime,.25);return;}
      if(this.ambience){this.ambience.gain.gain.setTargetAtTime(0,ac.currentTime,.4);for(const o of this.ambience.osc)o.stop(ac.currentTime+2);}
      const gain=ac.createGain();gain.gain.value=0;gain.connect(ac.destination);gain.gain.setTargetAtTime(settings.music*.022,ac.currentTime,.6);
      const base=/crypt|gloam|volcanic/.test(biome)?73.42:/town|meadow/.test(biome)?130.81:110;
      const osc=[1,1.5,2].map(f=>{const o=ac.createOscillator();o.type='sine';o.frequency.value=base*f;o.connect(gain);o.start();o.onended=()=>o.disconnect();return o;});
      this.ambience={biome,gain,osc};
    }
  };
  function event(kind,data={}){
    const p=game.player;if(!p)return;
    const e=data.entity,c=colorFor(data.id||data.tag||e?.ai||'');
    if(kind==='cast'){
      const heavy=(rarityRank[data.spell.rarity]||0)>=3;
      animate(p,heavy?'heavyCast':'cast',heavy?.42:.24);pose(p).cast=data.spell.cast;
      ring(p.x,p.y,c,'cast',.32,heavy?1.3:.75);lights.add({x:p.x,y:p.y,color:c,life:.3,size:heavy?130:75});if(heavy)camera.kick=.025;
    }else if(kind==='attack'){animate(p,'attack',.18);}
    else if(kind==='hit'&&!data.dot){
      const s=pose(e),d=norm(e.x-p.x,e.y-p.y);s.recoilX=(d.x-d.y)*Math.min(7,data.amount*.08);s.recoilY=(d.x+d.y)*Math.min(3,data.amount*.035);animate(e,'hit',.13);
      ring(e.x,e.y,c,'impact',.23,Math.min(1.7,.5+data.amount/100));
      if(data.amount>=45&&frameAt-lastImpact>220){lastImpact=frameAt;hitPause=Math.min(.035,.02+data.amount*.00004);camera.kick=.02;}
      if(data.tag)decals.add({x:e.x,y:e.y,color:c,life:4,kind:/frost|ice/.test(data.tag)?'frost':/fire/.test(data.tag)?'scorch':'crack',size:14});
    }else if(kind==='death'){
      deaths.add({x:e.x,y:e.y,color:e.color,life:e.boss?1.5:.55,size:e.r*45,kind:/skeleton|bone|crypt/.test(e.type)?'bones':/void|gloam/.test(e.ai)?'void':/frost|ice/.test(e.type)?'ice':'collapse'});
      if(e.boss){camera.kick=.04;ring(e.x,e.y,e.color,'impact',1.2,3);}
    }else if(kind==='dodge'||kind==='perfect'){
      animate(p,kind==='perfect'?'perfect':'dodge',kind==='perfect'?.42:.24);
      for(let i=0;i<4;i++)ring(p.x-p.facing.x*i*.22,p.y-p.facing.y*i*.22,kind==='perfect'?'#f6e8ff':p.armorGear.trim,'echo',.18+i*.06,1);
      if(kind==='perfect'){hitPause=.045;window.AWModernUI?.announce('PERFECT DODGE','Momentum +14');}
    }else if(kind==='reaction'){
      ring(p.x,p.y,'#eac8ff','reaction',.65,2);ring(p.x,p.y,'#9dedff','impact',.5,1.5);camera.kick=.03;window.AWModernUI?.announce(data.kind.replace(/([a-z])([A-Z])/g,'$1 $2').toUpperCase(),'Spell reaction');
    }else if(['heal','hurt','interact','level','loot'].includes(kind)){
      animate(p,{hurt:'hit',level:'levelUp',loot:'pickup'}[kind]||kind,kind==='level'?.75:.3);
      if(kind==='heal'||kind==='level')ring(p.x,p.y,kind==='heal'?'#96f3b0':'#ffe39c','cast',.6,1.4);
    }
    audio.play(kind,c);
  }
  function roomChanged(){camera.ready=false;roomRef=null;intro=null;hitPause=0;for(const pool of [decals,bursts,lights,deaths])pool.clear();window.AWWorld?.invalidate();}
  function frame(now){
    const raw=frameAt?Math.max(0,now-frameAt):16.7;frameAt=now;const dt=Math.min(.05,raw/1000);
    if(!running||paused||modalPause||roomTransition||document.hidden){window.AWInput?.poll();window.AWInput?.queue.clear();audio.ambience?.gain.gain.setTargetAtTime(0,audio.context.currentTime,.1);return;}
    // Real frame spacing, before simulation clamping. Hysteresis avoids quality oscillation.
    if(raw<250){frameMs=frameMs*.96+raw*.04;if(frameMs>23){slow+=dt;fast=0;}else if(frameMs<18.5){fast+=dt;slow=Math.max(0,slow-dt);}else{fast=0;slow=Math.max(0,slow-dt*.5);}}
    if(settings.quality==='auto'){
      if(slow>1.2){quality=quality==='high'?'medium':'low';slow=0;}
      if(fast>6){quality=quality==='low'?'medium':'high';fast=0;}
    }else quality=['high','medium','low'].includes(settings.quality)?settings.quality:'low';
    if(roomRef!==game.roomData){roomRef=game.roomData;roomTitle=2.1;const b=game.enemies.find(e=>e.boss&&!e.dead);if(b){intro={boss:b,time:0};audio.play('boss');window.AWModernUI?.announce(b.name,'Guardian of '+game.roomData.name);}priorBiome=roomRef?.biome;}
    audio.ambient(priorBiome||'meadow');
    if(intro){intro.time+=dt;if(intro.time>=1.75)intro=null;}
    hitPause=Math.max(0,hitPause-dt);roomTitle=Math.max(0,roomTitle-dt);
    for(const pool of [decals,bursts,lights,deaths])pool.tick(dt);
    for(const e of [game.player,...game.enemies]){
      if(!e)continue;const s=pose(e),distance=Math.hypot(e.x-s.x,e.y-s.y);s.speed=lerp(s.speed,Math.min(1,distance/(dt*3||1)),.6);s.stride+=distance*5;s.x=e.x;s.y=e.y;s.recoilX*=Math.exp(-dt*20);s.recoilY*=Math.exp(-dt*20);
      if(now>=s.until)s.state=e===game.player?(e.hp<=0?'death':e.dodgeTime>0?'dodge':s.speed>.08?'run':'idle'):(e.stun>0?'stun':e.telegraph?'windup':s.speed>.06?'run':'idle');
      if(e.boss&&!e.dead){const phase=e.hp/e.maxHp<.5?2:1,old=bosses.get(e)||1;if(phase>old){ring(e.x,e.y,e.color,'reaction',1,2.7);animate(e,'heavyCast',.6);window.AWModernUI?.announce(e.name,'PHASE II');audio.play('phase');}bosses.set(e,phase);}
    }
    const p=game.player,s=p&&pose(p);if(s?.speed>.25&&!intro){footstep+=dt;if(footstep>.22){footstep=0;decals.add({x:p.x,y:p.y,color:colorFor(priorBiome),life:1,kind:priorBiome==='swamp'?'ripple':'step',size:priorBiome==='swamp'?9:3});audio.play('step');}}
    camera.tick(dt);window.AWInput?.poll();window.AWModernUI?.tick();
  }
  const fx={decals,bursts,lights,deaths,
    drawGround(){
      const cap=quality==='low'?8:quality==='medium'?20:40;let n=0;
      for(const e of decals.items){if(e.life<=0||n++>=cap)continue;const s=worldToScreen(e.x,e.y),a=e.life/e.maxLife;ctx.save();ctx.globalAlpha=a*.3;ctx.strokeStyle=e.color;ctx.fillStyle=e.kind==='scorch'?'#15100c':e.color;ctx.lineWidth=1.3;ctx.beginPath();
        if(e.kind==='frost'||e.kind==='crack'){for(let i=0;i<6;i++){const angle=i*TAU/6;ctx.moveTo(s.x,s.y);ctx.lineTo(s.x+Math.cos(angle)*e.size,s.y+Math.sin(angle)*e.size*.5);}}
        else ctx.ellipse(s.x,s.y,e.size*(e.kind==='ripple'?2-a:1),e.size*.42,0,0,TAU);
        if(e.kind==='scorch'||e.kind==='step')ctx.fill();else ctx.stroke();ctx.restore();
      }
    },
    drawFront(){
      let count=0;const cap=settings.particles==='off'?0:quality==='low'||settings.particles==='low'?14:quality==='medium'||settings.particles==='medium'?36:80;
      for(const e of bursts.items){if(e.life<=0||count++>=cap)continue;const s=worldToScreen(e.x,e.y),t=1-e.life/e.maxLife,r=(10+t*28)*e.size;ctx.save();ctx.globalAlpha=1-t;ctx.strokeStyle=e.color;ctx.fillStyle=e.color;ctx.lineWidth=e.kind==='impact'?2.5:1.5;ctx.beginPath();
        if(e.kind==='echo'){ctx.globalAlpha*=.28;ctx.ellipse(s.x,s.y-20,12,23,-.2,0,TAU);ctx.fill();}
        else if(e.kind==='impact'||e.kind==='reaction'){for(let i=0;i<8;i++){const a=i*TAU/8+(e.kind==='reaction'?t*2:0);ctx.moveTo(s.x+Math.cos(a)*r*.5,s.y-12+Math.sin(a)*r*.5);ctx.lineTo(s.x+Math.cos(a)*r,s.y-12+Math.sin(a)*r);}ctx.stroke();}
        else{ctx.ellipse(s.x,s.y,r,r*.45,0,0,TAU);ctx.stroke();for(let i=0;i<4;i++){const a=i*TAU/4+t*2;ctx.fillRect(s.x+Math.cos(a)*r-2,s.y+Math.sin(a)*r*.45-2,4,4);}}
        ctx.restore();
      }
      for(const e of deaths.items){if(e.life<=0)continue;const s=worldToScreen(e.x,e.y),t=1-e.life/e.maxLife;ctx.save();ctx.globalAlpha=1-t;ctx.fillStyle=e.color;ctx.strokeStyle=e.color;
        for(let i=0;i<7;i++){const a=i*2.4,r=e.kind==='void'?e.size*(1-t):e.size*t,x=s.x+Math.cos(a)*r,y=s.y+Math.sin(a)*r*.4-Math.sin(t*Math.PI)*12;ctx.beginPath();if(e.kind==='bones'){ctx.moveTo(x-4,y-3);ctx.lineTo(x+4,y+3);ctx.lineWidth=3;ctx.stroke();}else if(e.kind==='ice'){ctx.moveTo(x,y-6);ctx.lineTo(x+4,y);ctx.lineTo(x-3,y+3);ctx.fill();}else{ctx.ellipse(x,y,Math.max(1,e.size*.23*(1-t)),Math.max(1,e.size*.18*(1-t)),0,0,TAU);ctx.fill();}}
        ctx.restore();
      }
      if(settings.lighting!=='off'&&settings.lighting!=='low'&&quality!=='low')for(const e of lights.items){if(e.life<=0)continue;const s=worldToScreen(e.x,e.y);ctx.save();ctx.globalCompositeOperation='screen';const g=ctx.createRadialGradient(s.x,s.y,0,s.x,s.y,e.size);g.addColorStop(0,colorAlpha(e.color,.17*e.life/e.maxLife));g.addColorStop(1,colorAlpha(e.color,0));ctx.fillStyle=g;ctx.fillRect(s.x-e.size,s.y-e.size,e.size*2,e.size*2);ctx.restore();}
    }
  };
  window.AWPresentation={settings,camera,fx,audio,pose,animate,event,frame,roomChanged,Pool,colorFor,effectRenderers,
    registerEffects(entries){for(const [kind,render] of Object.entries(entries))effectRenderers.set(kind,render);},
    saveSettings(){try{localStorage.setItem(KEY,JSON.stringify(settings));}catch(_){}},
    get quality(){return quality;},get frameMs(){return frameMs;},get cinematic(){return !!intro;},get frozen(){return !!intro||hitPause>0;},get intro(){return intro;},
    beforeRender(){if(game.player&&!camera.ready)camera.tick(.016);},
    afterRender(){if(roomTitle>0&&!intro&&game.roomData){ctx.save();ctx.globalAlpha=Math.min(1,roomTitle);ctx.textAlign='center';ctx.fillStyle='#e8dfcf';ctx.font='600 18px system-ui';ctx.fillText(game.roomData.name,W/2,H*.19);ctx.font='11px system-ui';ctx.fillStyle='#b9c9d4';ctx.fillText(`${biomePalette[game.roomData.biome]?.name||game.roomData.biome} · Threat ${game.roomData.difficulty}`,W/2,H*.19+21);ctx.restore();}}
  };
})();
