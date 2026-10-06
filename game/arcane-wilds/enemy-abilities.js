'use strict';

/* Signature attacks extend native enemy AI and gameplay effects. All hit shapes
 * are world-space geometry shared with their warnings. Effects with this kind
 * are gameplay records, so the cosmetic governor cannot evict them.
 */
(() => {
  if(window.AWEnemyCombat)return;
  const affinity=window.AWCombatAffinity;
  const KIND='enemyAbility',MAX_RECORDS=40;
  const definitions={
    charge:{name:'Line Break',wind:.85,active:.6,recovery:.65,cd:3.2,factor:1.35,range:8},
    cleave:{name:'Sweeping Cleave',wind:.8,active:.22,recovery:.65,cd:2.3,factor:1.2,range:2.5},
    rupture:{name:'Ground Rupture',wind:1,active:.8,recovery:.65,cd:3.4,factor:1.25,range:8},
    meteor:{name:'Meteor Barrage',wind:1.05,active:1,recovery:.65,cd:4,factor:1.2,range:12},
    predictive:{name:'Leading Shot',wind:.72,active:.15,recovery:.4,cd:2,factor:1.05,range:12},
    pincer:{name:'Pincer Rush',wind:.95,active:.6,recovery:.7,cd:4,factor:1.2,range:9},
    beam:{name:'Rotating Beam',wind:1.15,active:1.8,recovery:.8,cd:4.3,factor:.85,range:12},
    gravity:{name:'Pull / Collapse',wind:1.1,active:1.2,recovery:.75,cd:4.2,factor:1.3,range:10},
    frostTrail:{name:'Frost Trail',wind:.95,active:.62,recovery:.7,cd:3.5,factor:1.1,range:7},
    flameWave:{name:'Flame Wave',wind:1,active:1.2,recovery:.6,cd:3.8,factor:1.15,range:8},
    burrow:{name:'Burrow Ambush',wind:1.15,active:.25,recovery:.9,cd:3.8,factor:1.3,range:10},
    chain:{name:'Chain Lightning',wind:.95,active:.24,recovery:.65,cd:3.2,factor:1.15,range:10},
    cage:{name:'Thorn Cage',wind:1.1,active:.3,recovery:.6,cd:4.2,factor:.7,range:10},
    blink:{name:'Blink Strike',wind:.85,active:.2,recovery:.7,cd:3,factor:1.25,range:9},
    fan:{name:'Projectile Fan',wind:.85,active:.35,recovery:.55,cd:2.8,factor:.8,range:11},
    counter:{name:'Counter Stance',wind:.7,active:1.2,recovery:.5,cd:3.5,factor:.9,range:5},
    leapSlam:{name:'Leap Slam',wind:.95,active:.65,recovery:.85,cd:3.5,factor:1.3,range:7},
    vaultSlash:{name:'Vaulting Strike',wind:.9,active:.68,recovery:.75,cd:3.2,factor:1.15,range:7},
    doubleDash:{name:'Crosscut Dash',wind:.95,active:1.15,recovery:.8,cd:3.8,factor:1.1,range:8},
    leapVolley:{name:'Vault / Volley',wind:.9,active:.66,recovery:.65,cd:3.2,factor:.8,range:11},
    summon:{name:'Rally Ritual',wind:1.1,active:.2,recovery:.6,cd:5,factor:0,range:12},
    pool:{name:'Dangerous Ground',wind:.5,active:2.8,recovery:0,cd:0,factor:.3,range:0}
  };
  const eliteColors={Echoing:'#ddb5ff',Volatile:'#ffae67',Blinking:'#b694ff','Twin Cast':'#e3caff',Bulwark:'#bfe0ff',Stormbound:'#8cefff',Frozen:'#c2efff',Vengeful:'#ff8a96'};
  let roomRef=null,velocity={x:0,y:0},heading={x:0,y:0},steady=0,quiet=0;
  let serial=0,dodgeSerial=0,lastMajorDodge=-1,lastPerfectAt=-10,majorDodge=false;
  const targetIds=new WeakMap();let targetSerial=0;
  function targetId(p){if(!targetIds.has(p))targetIds.set(p,++targetSerial);return targetIds.get(p);}
  function actions(){return game.effects.filter(a=>a.kind===KIND&&a.life>0&&a.room===game.roomData);}
  function family(e){
    const t=ENEMY_TYPES[e.type]||{},b=t.biomes||[],tags=affinity.tags(e);
    if(t.region==='bloodroot'||b.length===1&&b[0]==='bloodroot'||/marrowboar|crimsonwitch|thorncolossus/i.test(e.type))return 'bloodroot';
    if(t.region==='celestial'||b.length===1&&b[0]==='celestial'||/starbound|astral|sunwarden|sunmoth/.test(e.type))return 'celestial';
    if(tags.has('FIRE'))return 'fire';
    if(tags.has('LIGHTNING'))return 'storm';
    if(tags.has('ICE'))return 'frost';
    if(tags.has('SHADOW'))return 'void';
    if(tags.has('PLANT'))return 'nature';
    if(tags.has('CONSTRUCT'))return 'stone';
    return 'neutral';
  }
  function profile(e){
    if(e.damage<=0||['regional_plant','regional_wall','regional_echo'].includes(e.ai))return null;
    const cacheKey=e.ai+'/'+(e.boss?(e.intensityPhase||1):0);
    if(e._combatProfileKey===cacheKey)return e._combatProfile;
    e._combatProfileKey=cacheKey;
    const f=family(e),ai=e.ai;
    if(e.boss){
      const phase=e.intensityPhase||1;
      const phases={
        fire:[['cleave','charge','leapSlam'],['flameWave','doubleDash','beam'],['beam','meteor','leapSlam']],
        frost:[['cleave','leapSlam'],['rupture','frostTrail','fan'],['beam','rupture','frostTrail']],
        storm:[['charge','chain'],['chain','doubleDash','rupture'],['beam','chain','leapVolley']],
        nature:[['cage','vaultSlash'],['cage','rupture','summon'],['rupture','cage','leapSlam']],
        void:[['blink','fan'],['gravity','beam','summon'],['beam','gravity','meteor']],
        celestial:[['fan','cleave'],['beam','meteor','counter'],['meteor','beam','blink']],
        bloodroot:[['cleave','charge'],['rupture','leapSlam','gravity'],['doubleDash','rupture','cleave']],
        stone:[['cleave','charge'],['rupture','counter','leapSlam'],['rupture','meteor','leapSlam']],
        neutral:[['cleave','charge'],['rupture','fan'],['rupture','beam','charge']]
      };
      if(ai==='bossNecro')return e._combatProfile=phase===1?['summon','cleave']:['summon','gravity','beam'];
      return e._combatProfile=(phases[f]||phases.neutral)[Math.min(2,phase-1)];
    }
    const mapping={
      melee:'cleave',ranged:'predictive',shield:'counter',orbiter:'fan',sniper:'predictive',charger:'charge',
      bomber:'meteor',summoner:'summon',spread:'fan',skirmish:'predictive',blinker:'blink',turret:'fan',
      diver:'vaultSlash',beam:'beam',shockwave:'rupture',frostmage:'frostTrail',vampire:'blink',
      phoenix:'flameWave',stormknight:'chain',voideye:'gravity',drake:'flameWave',
      bladeDancer:'doubleDash',mortarWitch:'meteor',chainWarden:'gravity',mirrorMage:'fan',
      burrower:'burrow',warPriest:'summon',stormCaller:'chain',executioner:'cleave',
      regionalPounce:'leapSlam',regionalBurst:'cage',regionalSlam:'rupture',regionalPrism:'fan',
      regionalBeam:'beam',regionalCharge:'charge',regionalArtillery:'meteor',
      regionalSpiral:'fan',regionalBlink:'blink',regionalGravity:'gravity',
      regional_beast:'vaultSlash',regional_ram:'doubleDash',regional_wasp:'leapVolley',regional_shaman:'summon',
      regional_moss:'counter',regional_briar:'cage',regional_alpha:'pincer',regional_spore:'cage',
      regional_mimic:'fan',regional_shard:'rupture',regional_frost:'frostTrail',
      regional_captain:'chain',regional_burrow:'burrow',regional_shepherd:'gravity',
      regional_archer:'predictive',regional_oracle:'meteor',regional_leech:'blink',
      regional_knight:'counter',regional_stalker:'blink',regional_devourer:'cleave',
      aw30_sunmoth:'fan',aw30_mimic:'cleave',aw30_cinderwheel:'flameWave',aw30_mirrorknight:'counter',
      aw30_boglantern:'cage',aw30_stormram:'charge',aw30_puppeteer:'gravity',
      aw30_burrower:'burrow',aw30_ashchoir:'meteor',aw30_behemoth:'rupture'
    };
    let first=mapping[ai]||'cleave';
    if(['wolf','awx_thornhound','briarprowler'].includes(e.type))first='leapSlam';
    if(first==='cage'&&!['nature','bloodroot'].includes(f))first=f==='void'?'gravity':'fan';
    const extra={fire:'flameWave',frost:'rupture',storm:'chain',nature:'cage',void:'blink',celestial:'beam',bloodroot:'charge',stone:'cleave'};
    const min=ENEMY_TYPES[e.type]?.min||1;
    if(['archer','awx_sunscout','astralArcher','skyraider'].includes(e.type))return e._combatProfile=[first,'leapVolley'];
    if(['assassin','awx_riftstalker','riftStalker','duskblade'].includes(e.type))return e._combatProfile=[first,'vaultSlash'];
    return e._combatProfile=min>=8&&extra[f]&&extra[f]!==first?[first,extra[f]]:[first];
  }
  function modifier(e){
    if(e.combatModifier)return e.combatModifier;
    const shadowMods={Teleporting:'Blinking',Burning:'Volatile',Arcane:'Echoing'};
    const inherited=[e.intensityTrait,e.trait,...(e.shadowTraits||[]).map(t=>shadowMods[t]||t)].find(t=>eliteColors[t]);
    if(inherited)return e.combatModifier=inherited;
    if(e.elite&&!e.boss){
      const options=['Echoing','Volatile','Blinking','Twin Cast','Bulwark','Frozen'];
      e.combatModifier=options[e.id%options.length];e.intensityColor=eliteColors[e.combatModifier];
    }
    return e.combatModifier||'';
  }
  function ensureRoom(){
    if(roomRef===game.roomData)return;
    roomRef=game.roomData;velocity={x:0,y:0};heading={x:0,y:0};steady=0;quiet=0;lastMajorDodge=-1;
    game.effects=game.effects.filter(a=>a.kind!==KIND);
    for(const e of game.enemies){e.combatAbility=null;e.combatCounter=null;e.combatJumpHeight=0;e.hidden=false;}
  }
  function point(p){return {x:clamp(p.x,.65,ROOM_W-.65),y:clamp(p.y,.65,ROOM_H-.65)};}
  function aim(e,lead=true){
    const ahead=lead?Math.min(.36,.14+steady*.055):0,vlen=Math.hypot(velocity.x,velocity.y)||1;
    const leadScale=Math.min(1,1.35/(vlen*Math.max(.01,ahead)));
    return point({x:game.player.x+velocity.x*ahead*leadScale,y:game.player.y+velocity.y*ahead*leadScale});
  }
  function distanceToSegment(p,a,b){
    const dx=b.x-a.x,dy=b.y-a.y,len=dx*dx+dy*dy;
    const t=len?clamp(((p.x-a.x)*dx+(p.y-a.y)*dy)/len,0,1):0;
    return Math.hypot(p.x-a.x-dx*t,p.y-a.y-dy*t);
  }
  function inShape(s,p,padding=p.r||.25){
    if(s.shape==='circle')return dist(s,p)<=s.r+padding;
    if(s.shape==='line')return distanceToSegment(p,s,s.to)<=s.width+padding;
    if(s.shape==='cone'){
      const dx=p.x-s.x,dy=p.y-s.y,d=Math.hypot(dx,dy),a=Math.atan2(dy,dx);
      const da=Math.abs(Math.atan2(Math.sin(a-s.angle),Math.cos(a-s.angle)));
      return d<=s.r+padding&&da<=s.arc+Math.asin(Math.min(1,padding/Math.max(.01,d)));
    }
    return false;
  }
  const circle=(p,r,delay=0)=>({shape:'circle',x:p.x,y:p.y,r,delay});
  function line(p,d,length,width,delay=0){
    let travel=length;
    if(d.x>0)travel=Math.min(travel,(ROOM_W-.4-p.x)/d.x);
    if(d.x<0)travel=Math.min(travel,(.4-p.x)/d.x);
    if(d.y>0)travel=Math.min(travel,(ROOM_H-.4-p.y)/d.y);
    if(d.y<0)travel=Math.min(travel,(.4-p.y)/d.y);
    travel=Math.max(0,travel);
    return {shape:'line',x:p.x,y:p.y,to:{x:p.x+d.x*travel,y:p.y+d.y*travel},width,delay};
  }
  function openDirection(center,r){
    let best={x:1,y:0},score=-Infinity;
    for(let i=0;i<16;i++){
      const d={x:Math.cos(i*TAU/16),y:Math.sin(i*TAU/16)},end={x:center.x+d.x*(r+1),y:center.y+d.y*(r+1)};
      if(end.x<.7||end.x>ROOM_W-.7||end.y<.7||end.y>ROOM_H-.7)continue;
      let value=Math.min(end.x,ROOM_W-end.x,end.y,ROOM_H-end.y);
      for(const a of actions())for(const s of shapes(a,true))if(inShape(s,end,.5))value-=8;
      if(value>score){score=value;best=d;}
    }
    return best;
  }
  function majorCount(){return actions().filter(a=>a.ability!=='pool'&&a.age<a.wind+a.active).length;}
  function hasCapacity(count=1){
    const cap=(game.roomData?.difficulty||1)>=7?4:3;
    const legacy=intensityState().hazards.filter(h=>h.time<1.5).length?1:0;
    return majorCount()+legacy+count<=cap&&actions().length+count+2<MAX_RECORDS;
  }
  function canStart(count=1){return quiet<=0&&hasCapacity(count);}
  function make(e,id,options={}){
    const def=definitions[id],f=family(e),hard=clamp(((game.roomData?.difficulty||1)-1)/20,0,1);
    const wind=Math.max(id==='predictive'?.55:.7,def.wind*(1-hard*.18));
    const target=options.target||aim(e),origin=point(e),dir=options.dir||norm(target.x-origin.x,target.y-origin.y);
    const a={kind:KIND,ability:id,enemy:e,id:++serial,x:origin.x,y:origin.y,z:0,origin,target,dir,
      angle:Math.atan2(dir.y,dir.x),wind,active:def.active,recovery:def.recovery,age:0,
      life:wind+def.active+def.recovery+.08,maxLife:wind+def.active+def.recovery+.08,
      color:({frost:'#a9eaff',fire:'#ff9362',storm:'#8ceeff',nature:'#a6dc86',void:'#ca94ff',celestial:'#ffe4a8',bloodroot:'#ff8b91',stone:'#d8b48d'})[f]||'#ffb499',
      element:({frost:'frost',fire:'fire',storm:'lightning',nature:'nature',void:'shadow',celestial:'solar',bloodroot:'physical',stone:'physical'})[f]||'physical',
      damage:e.damage*def.factor,room:game.roomData,hit:new Set(),fired:false,tick:0,modifier:modifier(e),marks:[],...options};
    if(id==='cleave')a.marks=[{shape:'cone',x:a.x,y:a.y,r:e.boss?3.5:2.5,angle:a.angle,arc:1.05,delay:0}];
    if(['charge','pincer','frostTrail'].includes(id))a.marks=[line(origin,dir,Math.min(def.range,11*def.active),e.r+.14)];
    if(id==='rupture')for(let i=1;i<=5;i++)a.marks.push(circle(point({x:a.x+dir.x*i*1.15,y:a.y+dir.y*i*1.15}),.55,i*.1));
    if(id==='meteor'){
      const travel=norm(velocity.x,velocity.y),side=Math.hypot(velocity.x,velocity.y)>.2?travel:dir;
      for(let i=0;i<3;i++)a.marks.push(circle(point({x:target.x+side.x*(i-1)*1.35,y:target.y+side.y*(i-1)*1.35}),.85,i*.3));
    }
    if(id==='gravity')a.marks=[circle(target,1.7,a.active-.12)];
    if(id==='burrow')a.marks=[circle(target,1.25)];
    if(id==='blink'){
      const back=point({x:target.x-dir.x*1.45,y:target.y-dir.y*1.45});
      a.blinkFrom=back;a.marks=[line(back,dir,3.2,.38)];
    }
    if(id==='cage'){
      a.gap=openDirection(target,2);const angle=Math.atan2(a.gap.y,a.gap.x);
      for(let i=2;i<=8;i++){const b=angle+i*TAU/10,p={x:target.x+Math.cos(b)*2,y:target.y+Math.sin(b)*2};
        // Clamping off-room roots onto the player can seal a corner escape.
        if(p.x>=.5&&p.x<=ROOM_W-.5&&p.y>=.5&&p.y<=ROOM_H-.5)a.marks.push(circle(p,.38));
      }
    }
    if(id==='chain'){
      a.chainTargets=[game.player];a.marks=[circle(target,.65)];
      let from=target;
      const seen=new Set(a.chainTargets);
      for(let i=0;i<3;i++){const next=allies().filter(s=>!seen.has(s)&&dist(s,from)<2.6).sort((s,t)=>dist(s,from)-dist(t,from))[0];if(!next)break;seen.add(next);a.chainTargets.push(next);a.marks.push(circle(point(next),.55));from=next;}
    }
    if(id==='counter')a.marks=[{shape:'cone',x:a.x,y:a.y,r:e.r+.65,angle:a.angle,arc:1.2,delay:0}];
    if(['leapSlam','vaultSlash','leapVolley'].includes(id)){
      a.flight=.44;
      a.jumpEnd=id==='leapSlam'?target:id==='vaultSlash'?point({x:target.x+dir.x*1.45,y:target.y+dir.y*1.45}):line(origin,{x:-dir.x,y:-dir.y},2.8,.1).to;
      if(id==='leapSlam')a.marks=[circle(a.jumpEnd,1.45,a.flight)];
      if(id==='vaultSlash'){
        const strike=norm(target.x-a.jumpEnd.x,target.y-a.jumpEnd.y);
        a.marks=[{shape:'cone',x:a.jumpEnd.x,y:a.jumpEnd.y,r:2.2,angle:Math.atan2(strike.y,strike.x),arc:.8,delay:a.flight}];
      }
      if(id==='leapVolley'){a.volleyOrigin={...a.jumpEnd};a.volleyDir=norm(target.x-a.jumpEnd.x,target.y-a.jumpEnd.y);}
    }
    if(id==='doubleDash'){
      const first=line(origin,dir,4.2,e.r+.14);
      const side=e.id%2?1:-1,nextTarget=point({x:target.x-dir.y*side*1.4,y:target.y+dir.x*side*1.4});
      const nextDir=norm(nextTarget.x-first.to.x,nextTarget.y-first.to.y);
      a.dashes=[first,line(first.to,nextDir,4.2,e.r+.14,.8)];a.marks=a.dashes;
    }
    if(id==='summon')a.marks=[circle(origin,1.25)];
    game.effects.push(a);e.combatAbility=a;e.combatManaged=true;e.combatCounter=null;
    e.state='abilityWind';e.stateTime=wind;e.facing={...dir};
    e.telegraph={kind:'ability',combatAbility:a,maxTime:wind,dir,range:def.range};
    if(id==='burrow')e.hidden=true;
    return a;
  }
  function start(e,id){
    if(!definitions[id]||!canStart())return false;
    let partner=null;
    if(id==='pincer'){
      partner=game.enemies.find(o=>o!==e&&!o.dead&&!o.combatAbility&&o.stun<=0&&o.attack<=.7&&dist(o,e)<10&&
        ['melee','charger','regional_beast','regional_alpha'].includes(o.ai)&&
        (o.x-game.player.x)*(e.x-game.player.x)+(o.y-game.player.y)*(e.y-game.player.y)<-1);
      if(!partner||!canStart(2))id='charge';
    }
    const a=make(e,id);if(partner){const b=make(partner,'pincer',{target:{...a.target}});a.partner=b.id;b.partner=a.id;}
    quiet=.3;return a;
  }
  function shapes(a,warning=false){
    const t=Math.max(0,a.age-a.wind);
    if(a.ability==='beam'){
      // The fan during windup also announces the direction/extent of the sweep.
      const angle=a.angle+(warning?0:t*.52*(a.enemy.id%2?1:-1));
      return [line(a.origin,{x:Math.cos(angle),y:Math.sin(angle)},12,.24)];
    }
    if(a.ability==='predictive')return [line(a.origin,a.dir,8*2.8,.12)];
    if(a.ability==='fan'){
      const n=(game.roomData?.difficulty||1)>=8?7:5,out=[],offsets=a.modifier==='Twin Cast'?[0,.125]:[a.offset||0];
      for(const offset of offsets)for(let i=0;i<n;i++){const angle=a.angle+(i-(n-1)/2)*.25+offset;out.push(line(a.origin,{x:Math.cos(angle),y:Math.sin(angle)},4.6*2.8,.07,offset===.125?.22:0));}
      return out;
    }
    if(a.ability==='flameWave'){
      if(warning)return [line(a.origin,a.dir,6,1.84)];
      const p={x:a.x+a.dir.x*(warning?0:t*5),y:a.y+a.dir.y*(warning?0:t*5)},side={x:-a.dir.y,y:a.dir.x};
      return [{shape:'line',x:p.x-side.x*1.6,y:p.y-side.y*1.6,to:{x:p.x+side.x*1.6,y:p.y+side.y*1.6},width:.24,delay:0}];
    }
    if(a.ability==='leapVolley'){
      const origin=a.volleyOrigin,d=a.volleyDir,out=[];
      for(let i=-1;i<=1;i++){const b=Math.atan2(d.y,d.x)+i*.25;out.push(line(origin,{x:Math.cos(b),y:Math.sin(b)},4.6*2.8,.08,a.flight));}return out;
    }
    return a.marks;
  }
  function allies(){
    return [...game.summons,...(window.AWContinentalSpells?.state().actors||[])].filter(s=>s.life>0&&(s.hp===undefined||s.hp>0));
  }
  function perfect(a){
    if(game.player.dodgeTime<=0||lastMajorDodge===dodgeSerial||elapsed-lastPerfectAt<.5)return;
    lastMajorDodge=dodgeSerial;lastPerfectAt=elapsed;majorDodge=true;
    try{intensityPerfectDodge();}finally{majorDodge=false;}
    game.player.tailwind=Math.max(game.player.tailwind||0,.8);
    const e=a.enemy||a.regionalSource;
    if(e&&!e.dead&&['charge','pincer','frostTrail','blink','cleave','leapSlam','vaultSlash','doubleDash'].includes(a.ability)&&dist(e,game.player)<3)e.stun=Math.max(e.stun,e.boss?.25:.65);
    fx('dashEcho',game.player.x,game.player.y,.4,'#eee1ff',{dir:game.player.dodgeDir});
  }
  function hurt(a,target,key,scale=1){
    if(a.hit.has(key))return;
    a.hit.add(key);
    if(target===game.player){
      if(game.player.dodgeTime>0){perfect(a);return;}
      const incoming=window.AWRegionalContent?.incoming;
      if(window.AWRegionalContent)AWRegionalContent.incoming=a.enemy;
      try{damagePlayer(a.damage*scale*(a.enemy?.combatSuppressed>0?.85:1));}finally{if(window.AWRegionalContent)AWRegionalContent.incoming=incoming;}
    }else{
      const tags=affinity.tags(target),result=affinity.resolve(a.element,target);
      if(target.hp===undefined)target.hp=target.maxHp=30;
      target.hp-=a.damage*scale*result.multiplier;
      if(target.hp<=0){target.life=0;burst(target.x,target.y,a.color,6,.5);}
      if(result.multiplier>1&&tags.size)fx('shieldHit',target.x,target.y,.22,a.color,{r:.5});
    }
  }
  function resolveShapes(a,list,ticking=false){
    const targets=[game.player,...allies()];
    for(let i=0;i<list.length;i++){
      const s=list[i],local=a.age-a.wind;
      if(!ticking&&(local<(s.delay||0)||local>(s.delay||0)+.18))continue;
      for(const p of targets)if(inShape(s,p))hurt(a,p,targetId(p)+'/'+i+(ticking?'/'+a.tickSerial:''));
    }
  }
  function patch(a,marks,element=a.element){
    if(actions().length>=MAX_RECORDS)return false;
    const wind=.55,life=wind+2.6;
    game.effects.push({...a,id:++serial,ability:'pool',element,wind,active:2.6,recovery:0,age:0,life,maxLife:life,marks,
      damage:a.damage*.25,hit:new Set(),tick:0,tickSerial:0,fired:false,modifier:'',detached:true});
    return true;
  }
  function finish(a){
    const e=a.enemy;if(e.combatAbility!==a)return;
    e.combatAbility=null;e.combatCounter=null;e.combatJumpHeight=0;e.telegraph=null;e.hidden=false;e.state='idle';
    const aggression=a.modifier==='Vengeful'?.82:1;
    e.attack=Math.max(e.attack,definitions[a.ability].cd*aggression);
  }
  function fireShots(a,offset=0){
    const n=a.ability==='predictive'?1:a.ability==='leapVolley'?3:(game.roomData?.difficulty||1)>=8?7:5;
    const speed=a.ability==='predictive'?8:4.6;
    for(let i=0;i<n;i++){
      const angle=a.angle+offset+(i-(n-1)/2)*.25;
      // The bounded lead is frozen at windup; shots never home in on the hero.
      enemyProjectile(a.enemy,{x:Math.cos(angle),y:Math.sin(angle)},speed,a.element==='fire'?'ember':'runeBolt',
        {r:a.ability==='predictive'?.075:.07,damage:a.damage*(a.enemy.combatSuppressed>0?.85:1),life:2.8,color:a.color});
      const q=game.projectiles.at(-1);q.x=a.origin.x;q.y=a.origin.y;q.combatAbilityId=a.id;q.ability=a.ability;q.combatElement=a.element;q.regionalSource=a.enemy;
    }
  }
  function activation(a){
    const e=a.enemy;
    if(['predictive','fan'].includes(a.ability))fireShots(a);
    if(a.ability==='blink'){e.x=a.blinkFrom.x;e.y=a.blinkFrom.y;burst(e.x,e.y,a.color,8,.6);}
    if(a.ability==='burrow'){e.x=a.target.x;e.y=a.target.y;e.hidden=false;burst(e.x,e.y,a.color,12,1);}
    if(a.ability==='counter')e.combatCounter={dir:{...a.dir},retaliate:false};
    if(a.ability==='cage')patch(a,a.marks,'nature');
    if(a.ability==='summon'){
      if(e.ai==='warPriest')for(const ally of game.enemies)if(!ally.dead&&dist(ally,e)<4)ally.hp=Math.min(ally.maxHp,ally.hp+ally.maxHp*.08);
      const shadowSummoner=e.shadowTraits?.includes('Summoner');
      const type=shadowSummoner?'voidStalker':e.ai==='regional_shaman'?'regionalHealPlant':family(e)==='nature'?'sporeling':family(e)==='void'?'wisp':'skeleton';
      if(game.enemies.length<(shadowSummoner?14:18))for(const side of [-1,1]){
        const m=spawnEnemy(type,point({x:e.x+side*1.25,y:e.y+.5}),false,.7);
        if(shadowSummoner){m.shadow=true;m.shadowMinion=true;m.shadowTraits=['Tiny'];m.shadowPulse=4;m.hp=m.maxHp*=.5;}
      }
      fx('summon',e.x,e.y,.5,a.color,{r:1.5});
    }
    fx('castRing',e.x,e.y,.28,a.color,{r:.8});
    window.AWPresentation?.audio.play('cast',a.color);
  }
  function tickAction(a,dt){
    const e=a.enemy;
    if(a.room!==game.roomData||a.life<=0)return;
    a.age+=dt;const t=a.age-a.wind;
    if(!a.detached&&(e.dead||e.stun>0&&!e.boss)){a.life=0;finish(a);return;}
    if(t<0)return;
    if(!a.fired){a.fired=true;if(!a.detached)e.state='abilityActive';activation(a);}
    if(t>a.active){
      if(e.combatAbility===a){e.state='abilityRecovery';e.telegraph=null;e.hidden=false;}
      if(a.age>=a.wind+a.active+a.recovery){a.life=0;finish(a);}
      return;
    }
    if(['leapSlam','vaultSlash','leapVolley'].includes(a.ability)){
      const progress=clamp(t/a.flight,0,1);
      e.x=lerp(a.x,a.jumpEnd.x,progress);e.y=lerp(a.y,a.jumpEnd.y,progress);
      e.combatJumpHeight=Math.sin(progress*Math.PI)*(a.ability==='leapSlam'?62:48);
      if(t>=a.flight&&!a.landed){
        a.landed=true;e.combatJumpHeight=0;
        fx('shockRing',e.x,e.y,.38,a.color,{r:a.ability==='leapSlam'?1.5:.75});
        burst(e.x,e.y,a.color,12,.9);shake=Math.max(shake,a.ability==='leapSlam'?5:2);
        if(a.ability==='leapVolley'){const original=a.origin,angle=a.angle;a.origin=a.volleyOrigin;a.angle=Math.atan2(a.volleyDir.y,a.volleyDir.x);fireShots(a);a.origin=original;a.angle=angle;}
      }
      if(a.ability!=='leapVolley')resolveShapes(a,a.marks);
    }else if(a.ability==='doubleDash'){
      const phase=t>=.8?1:0,s=a.dashes[phase],local=t-(phase?.8:0),progress=clamp(local/.28,0,1),from={x:e.x,y:e.y};
      e.x=lerp(s.x,s.to.x,progress);e.y=lerp(s.y,s.to.y,progress);
      if(local-dt<.28){const swept={shape:'line',...from,to:{x:e.x,y:e.y},width:s.width};
        for(const p of [game.player,...allies()])if(inShape(swept,p))hurt(a,p,'dash/'+phase+'/'+targetId(p));
      }
    }else if(['charge','pincer','frostTrail'].includes(a.ability)){
      const from={x:e.x,y:e.y},next={x:e.x+a.dir.x*11*dt,y:e.y+a.dir.y*11*dt};
      const wall=next.x<.4||next.x>ROOM_W-.4||next.y<.4||next.y>ROOM_H-.4;
      e.x=clamp(next.x,.4,ROOM_W-.4);e.y=clamp(next.y,.4,ROOM_H-.4);
      const swept={shape:'line',...from,to:{x:e.x,y:e.y},width:e.r+.14};
      for(const p of [game.player,...allies()])if(inShape(swept,p))hurt(a,p,p);
      if(wall){if(!a.hit.has(game.player))e.stun=Math.max(e.stun,e.boss?.4:.9);a.age=a.wind+a.active;}
      if(!a.trailMade&&t>=.4&&(a.ability==='frostTrail'||family(e)==='fire'&&e.boss&&(e.intensityPhase||1)>=2)){
        const marks=[];for(let i=0;i<5;i++)marks.push(circle(point({x:a.x+a.dir.x*i*1.1,y:a.y+a.dir.y*i*1.1}),.42));
        patch(a,marks,a.ability==='frostTrail'?'frost':'fire');a.trailMade=true;
      }
    }else if(a.ability==='gravity'){
      const p=game.player,d=dist(p,a.target);
      if(d<3.2&&d>.1&&p.dodgeTime<=0&&!(window.AWRegionalContent?.timers.anchored>0)){
        const pull=norm(a.target.x-p.x,a.target.y-p.y);
        p.x=clamp(p.x+pull.x*dt*.95,.3,ROOM_W-.3);p.y=clamp(p.y+pull.y*dt*.95,.3,ROOM_H-.3);
      }
      resolveShapes(a,a.marks);
    }else if(a.ability==='chain'){
      if(!a.chainFired){
        a.chainFired=true;let from=a.origin,connected=true;
        for(let i=0;i<a.chainTargets.length&&connected;i++){
          const target=a.chainTargets[i],mark=a.marks[i];connected=inShape(mark,target);
          fx('lightning',from.x,from.y,.25,a.color,{toX:mark.x,toY:mark.y,width:3});
          if(connected)hurt(a,target,target,i===0?1:.7);
          from=mark;
        }
      }
    }else if(a.ability==='counter'){
      if(t>a.active-.1&&e.combatCounter?.retaliate&&!a.counterQueued){e.combatCounter=null;a.counterQueued=true;
        // Retaliation has a fresh warning and a frozen direction.
        if(canStart()){const next=make(e,'cleave');next.wind=Math.max(.75,next.wind);next.life=next.maxLife=next.wind+next.active+next.recovery+.08;}
      }
    }else if(['beam','flameWave','pool'].includes(a.ability)){
      a.tick-=dt;
      if(a.tick<=0){a.tick=.38;a.tickSerial=(a.tickSerial||0)+1;const list=shapes(a);
        if(a.ability==='pool'&&a.element==='frost'&&list.some(s=>inShape(s,game.player))&&game.player.dodgeTime<=0)game.player.combatChill=.7;
        resolveShapes(a,list,true);
      }
      if(a.ability==='flameWave'&&!a.trailMade&&t>=.8){patch(a,[.8,2.3,3.8,5.3].map(v=>circle(point({x:a.x+a.dir.x*v,y:a.y+a.dir.y*v}),.55)),'fire');a.trailMade=true;}
    }else if(!['predictive','fan','summon'].includes(a.ability))resolveShapes(a,a.marks);
    if(a.modifier==='Twin Cast'&&a.ability==='fan'&&t>.22&&!a.twinFired){a.twinFired=true;fireShots(a,.125);}
    if(t>.12&&!a.modified){
      a.modified=true;
      if(a.modifier==='Volatile'||a.modifier==='Frozen')patch(a,[circle(a.target,.7)],a.modifier==='Frozen'?'frost':a.element);
      if(a.modifier==='Stormbound'&&hasCapacity()){
        const arc=makeDetached(a,'rupture',[circle(a.target,.65)],.8,.2,'lightning');game.effects.push(arc);
      }
    }
  }
  function makeDetached(a,id,marks,wind,active,element=a.element){
    return {...a,id:++serial,ability:id,marks,wind,active,recovery:0,age:0,life:wind+active+.08,maxLife:wind+active+.08,
      hit:new Set(),tick:0,fired:false,modifier:'',detached:true,element,damage:a.damage*.7};
  }
  function moveTactically(e,d,range,speed,dt){
    const ranged=['predictive','meteor','beam','fan','cage','gravity','summon','chain'].includes(profile(e)?.[0]);
    if(ranged){
      if(range<3.5)moveEnemy(e,{x:-d.x,y:-d.y},speed*.75,dt);
      else if(range>8)moveEnemy(e,d,speed*.7,dt);
      else if(steady>.7){const side=e.id%2?1:-1;moveEnemy(e,{x:-d.y*side,y:d.x*side},speed*.4,dt);}
      return;
    }
    let target=game.player;
    if(steady>.65&&range>2){target=aim(e);const side=e.id%2?1:-1;target=point({x:target.x-d.y*side*1.35,y:target.y+d.x*side*1.35});}
    if(profile(e)?.[0]==='counter'){
      const caster=game.enemies.find(o=>o!==e&&!o.dead&&o.campaignRole==='ranged');
      if(caster&&dist(e,caster)>2.2)target=point({x:caster.x+(game.player.x-caster.x)*.28,y:caster.y+(game.player.y-caster.y)*.28});
    }
    moveEnemy(e,norm(target.x-e.x,target.y-e.y),speed,dt);
  }
  const oldAI=updateEnemyAI;
  updateEnemyAI=function(e,d,range,speed,dt){
    ensureRoom();
    // Finish legacy in-flight states and retain attached leech / devourer mechanics.
    if(!e.combatAbility&&e.state!=='idle'&&!e.state.startsWith('ability'))return oldAI(e,d,range,speed,dt);
    if(e.ai==='regional_leech'&&e.regionalAttached)return oldAI(e,d,range,speed,dt);
    if(e.ai==='regional_devourer'&&!e.combatAbility){const attack=e.attack;e.attack=Math.max(.01,e.attack);oldAI(e,d,range,0,dt);e.attack=attack;}
    if(e.awDormant&&e.state==='dormant')return oldAI(e,d,range,speed,dt);
    const options=profile(e);
    if(!options)return oldAI(e,d,range,speed,dt);
    // Keep existing healing, summoning, and decoy aggro behavior in its native AI.
    const bait=window.AWContinentalSpells?.state().actors.find(s=>s.hp>0&&s.life>0&&['treant','mirror','ember'].includes(s.kind)&&dist(s,e)<(e.boss?2.5:5)*(hasUpgrade(s.id,'reach')?1.4:1));
    if(!e.combatAbility&&(bait&&e.state==='idle'||!e.boss&&options[0]==='summon'))return oldAI(e,d,range,speed,dt);
    e.combatManaged=true;
    if(e.shadow){
      const traits=e.shadowTraits||[];
      if(traits.includes('Swift')||traits.includes('Tiny'))speed*=1.18;
      if(traits.includes('Berserker')&&e.hp<e.maxHp*.4)speed*=1.35;
      if(traits.includes('Regenerating'))e.hp=Math.min(e.maxHp,e.hp+e.maxHp*.007*dt);
    }
    if(e.combatAbility){e.facing={...e.combatAbility.dir};return;}
    moveTactically(e,d,range,speed,dt);
    if(e.attack>0)return;
    let id=options[(e.combatSequence||0)%options.length],def=definitions[id];
    if(range>def.range||id==='cleave'&&range>3.5)return;
    if(e.shadowTraits?.includes('Summoner')&&(e.combatSequence||0)%3===2)id='summon';
    if(modifier(e)==='Bulwark'&&(e.combatSequence||0)%3===2)id='counter';
    const a=start(e,id);
    if(a){e.combatSequence=(e.combatSequence||0)+1;e.attack=1;}
    else e.attack=.18+(e.id%4)*.055;
  };
  const oldSeparate=separateEnemies;
  separateEnemies=function(){
    // Crowd separation may move ordinary foes, but cannot move a promised attack lane.
    const posed=game.enemies.filter(e=>e.combatAbility).map(e=>({e,x:e.x,y:e.y}));
    oldSeparate();for(const p of posed){p.e.x=p.x;p.e.y=p.y;}
  };
  const oldMove=playerMovement;
  playerMovement=function(dt){
    ensureRoom();const p=game.player;if(!p)return oldMove(dt);
    const from={x:p.x,y:p.y},base=p.speed,dodging=p.dodgeTime>0;
    if(p.combatChill>0&&!dodging)p.speed*=.72;
    try{oldMove(dt);}finally{p.speed=base;}
    if(dt<=0||roomRef!==game.roomData||dodging)return;
    const vx=(p.x-from.x)/dt,vy=(p.y-from.y)/dt,len=Math.hypot(vx,vy);
    if(len>base*2){velocity={x:0,y:0};steady=0;return;}
    velocity={x:lerp(velocity.x,vx,Math.min(1,dt*9)),y:lerp(velocity.y,vy,Math.min(1,dt*9))};
    const next=norm(vx,vy);
    if(len>.3){steady=next.x*heading.x+next.y*heading.y>.92?Math.min(4,steady+dt):0;heading=next;}
    else steady=Math.max(0,steady-dt*3);
  };
  const oldEffects=updateEnemyEffects;
  updateEnemyEffects=function(dt){
    oldEffects(dt);if(!running||paused||modalPause||roomTransition||!game.player)return;
    ensureRoom();quiet=Math.max(0,quiet-dt);
    game.player.combatChill=Math.max(0,(game.player.combatChill||0)-dt);
    for(const e of game.enemies){if(e.combatSuppressed>0)e.attack+=dt*.2;e.combatSuppressed=Math.max(0,(e.combatSuppressed||0)-dt);e.affinityFlash=Math.max(0,(e.affinityFlash||0)-dt);}
    for(const a of actions())tickAction(a,dt);
    // Echoes retain their original geometry and announce another complete windup.
    for(const a of actions())if(a.modifier==='Echoing'&&a.fired&&!a.echoQueued&&a.age>a.wind+a.active-.05&&!['pool','counter','summon','charge','pincer','frostTrail','blink','burrow','leapVolley'].includes(a.ability)&&hasCapacity()){
      // Repeating a landing or dash repeats its ground marks, never the actor's movement.
      const id=['leapSlam','vaultSlash','doubleDash'].includes(a.ability)?'rupture':a.ability;
      a.echoQueued=true;game.effects.push(makeDetached(a,id,a.marks.map(s=>({...s})),.85,a.active));
    }
    for(const a of actions())if(a.modifier==='Blinking'&&!a.blinked&&a.age>=a.wind+a.active){
      a.blinked=true;const e=a.enemy;if(!e.dead){const spot=point({x:e.x-a.dir.y*1.5,y:e.y+a.dir.x*1.5});e.x=spot.x;e.y=spot.y;burst(e.x,e.y,a.color,6,.4);}
    }
    game.summons=game.summons.filter(s=>s.life>0);
  };
  const oldDodge=dodge;
  dodge=function(){const before=game.player?.dodgeCd||0;oldDodge();if(game.player?.dodgeCd>before)dodgeSerial++;};
  const oldLoad=loadRoom;
  loadRoom=function(){roomRef=null;const result=oldLoad();ensureRoom();quiet=1;return result;};

  // The same sampled world polygon draws both the warning and the live attack.
  function drawShape(s,color,alpha,fill=true){
    let pts=[];
    if(s.shape==='circle')for(let i=0;i<32;i++){const a=i*TAU/32;pts.push({x:s.x+Math.cos(a)*s.r,y:s.y+Math.sin(a)*s.r});}
    if(s.shape==='line'){
      const angle=Math.atan2(s.to.y-s.y,s.to.x-s.x);
      for(let i=0;i<=12;i++){const a=angle-Math.PI/2+i/12*Math.PI;pts.push({x:s.to.x+Math.cos(a)*s.width,y:s.to.y+Math.sin(a)*s.width});}
      for(let i=0;i<=12;i++){const a=angle+Math.PI/2+i/12*Math.PI;pts.push({x:s.x+Math.cos(a)*s.width,y:s.y+Math.sin(a)*s.width});}
    }
    if(s.shape==='cone'){
      pts=[s];for(let i=0;i<=24;i++){const a=s.angle-s.arc+i/24*s.arc*2;pts.push({x:s.x+Math.cos(a)*s.r,y:s.y+Math.sin(a)*s.r});}
    }
    if(!pts.length)return;ctx.save();ctx.strokeStyle=color;ctx.fillStyle=colorAlpha(color,alpha);ctx.lineWidth=2;
    ctx.beginPath();pts.forEach((p,i)=>{const q=worldToScreen(p.x,p.y);if(i)ctx.lineTo(q.x,q.y);else ctx.moveTo(q.x,q.y);});ctx.closePath();if(fill)ctx.fill();ctx.stroke();ctx.restore();
  }
  function drawAction(a,warningOnly=false){
    const warming=a.age<a.wind,active=a.age>=a.wind&&a.age<a.wind+a.active;
    if(!warming&&!active||!warningOnly&&warming)return;
    const progress=clamp(a.age/a.wind,0,1),list=shapes(a,warming);
    if(warningOnly&&!warming&&!list.some(s=>(s.delay||0)>a.age-a.wind))return;
    for(let i=0;i<list.length;i++){
      const s=list[i],local=a.age-a.wind-(s.delay||0);
      const pending=warming||local<0;
      if(pending!==warningOnly)continue;
      if(!warming&&!['pool','beam','flameWave','counter'].includes(a.ability)&&local>.22)continue;
      drawShape(s,a.color,pending?.08+progress*.14:.25);
      if(pending&&s.shape==='circle'){const fill=warming?progress:clamp(1+local/Math.max(.1,s.delay||0),0,1);drawShape({...s,r:Math.max(.05,s.r*fill)},a.color,.08);}
      if(!warming&&['rupture','cage'].includes(a.ability)){
        const p=worldToScreen(s.x,s.y);ctx.save();ctx.fillStyle=a.color;ctx.beginPath();ctx.moveTo(p.x,p.y-24);ctx.lineTo(p.x+7,p.y+3);ctx.lineTo(p.x-7,p.y+3);ctx.closePath();ctx.fill();ctx.restore();
      }
    }
    if(a.ability==='beam'){
      if(warming){const side=a.enemy.id%2?1:-1;drawShape({shape:'cone',x:a.x,y:a.y,r:12,angle:a.angle+side*.46,arc:.47},a.color,.04);drawShape(line(a.origin,{x:Math.cos(a.angle+side*.94),y:Math.sin(a.angle+side*.94)},12,.08),a.color,.04);}
      else if(!warningOnly){const s=list[0],start=worldToScreen(s.x,s.y,10),end=worldToScreen(s.to.x,s.to.y,10);ctx.save();ctx.strokeStyle='#fff5e9';ctx.lineWidth=3;ctx.beginPath();ctx.moveTo(start.x,start.y);ctx.lineTo(end.x,end.y);ctx.stroke();ctx.restore();}
    }
    if(a.ability==='gravity'&&(!warningOnly||warming)){
      const p=worldToScreen(a.target.x,a.target.y);ctx.save();ctx.strokeStyle=a.color;ctx.lineWidth=2;
      for(let i=0;i<8;i++){const angle=i*TAU/8+a.age,r=(warming?34:42)*(1-(a.age*.75%1));ctx.beginPath();ctx.moveTo(p.x+Math.cos(angle)*r,p.y+Math.sin(angle)*r*.5);ctx.lineTo(p.x+Math.cos(angle)*r*.55,p.y+Math.sin(angle)*r*.28);ctx.stroke();}ctx.restore();
    }
    if(a.ability==='meteor'&&active&&!warningOnly)for(const s of list){const h=Math.max(0,(s.delay||0)-(a.age-a.wind))*150,p=worldToScreen(s.x,s.y);ctx.save();ctx.strokeStyle=a.color;ctx.lineWidth=4;ctx.beginPath();ctx.moveTo(p.x+h*.3,p.y-h-20);ctx.lineTo(p.x,p.y);ctx.stroke();ctx.restore();}
    if(a.ability==='burrow'&&warming){const p=worldToScreen(lerp(a.x,a.target.x,progress),lerp(a.y,a.target.y,progress));ctx.save();ctx.strokeStyle=a.color;ctx.lineWidth=2;ctx.beginPath();ctx.ellipse(p.x,p.y,12+Math.sin(a.age*20)*3,5,0,0,TAU);ctx.stroke();ctx.restore();}
    if(a.ability==='cage'&&warming&&a.gap){const d=a.gap,from=worldToScreen(a.target.x+d.x*.8,a.target.y+d.y*.8),to=worldToScreen(a.target.x+d.x*2.8,a.target.y+d.y*2.8);ctx.save();ctx.strokeStyle='#c2ffad';ctx.lineWidth=3;ctx.setLineDash([5,5]);ctx.beginPath();ctx.moveTo(from.x,from.y);ctx.lineTo(to.x,to.y);ctx.stroke();ctx.restore();}
    if(a.jumpEnd&&warningOnly&&(warming||a.age-a.wind<a.flight)){
      const from=worldToScreen(a.x,a.y),to=worldToScreen(a.jumpEnd.x,a.jumpEnd.y);ctx.save();ctx.strokeStyle=a.color;ctx.lineWidth=1.5;ctx.setLineDash([4,6]);ctx.beginPath();ctx.moveTo(from.x,from.y);ctx.quadraticCurveTo((from.x+to.x)/2,(from.y+to.y)/2-64,to.x,to.y);ctx.stroke();ctx.restore();
    }
    if(warming&&!a.detached){const p=worldToScreen(a.enemy.x,a.enemy.y,58);ctx.save();ctx.font='bold 11px system-ui';ctx.fillStyle=a.color;ctx.textAlign='center';ctx.fillText(definitions[a.ability].name,p.x,p.y);ctx.restore();}
  }
  const oldDrawEffect=drawEffect;
  drawEffect=function(e,front){if(e.kind===KIND){if(front)drawAction(e);return;}return oldDrawEffect(e,front);};
  const oldMarks=drawTelegraphs;
  drawTelegraphs=function(){oldMarks();for(const a of actions())drawAction(a,true);};
  const oldEnemyMark=drawEnemyTelegraph;
  drawEnemyTelegraph=function(e){if(e.telegraph?.combatAbility)return;return oldEnemyMark(e);};
  const oldDrawEnemy=drawEnemy;
  drawEnemy=function(e){
    const height=e.combatJumpHeight||0;
    if(height>0){
      const inheritedShadow=shadowAt;inheritedShadow(e.x,e.y,18+e.r*18,.22);ctx.save();ctx.translate(0,-height);
      shadowAt=()=>{};
      try{oldDrawEnemy(e);}finally{shadowAt=inheritedShadow;ctx.restore();}
    }else oldDrawEnemy(e);
    const mod=modifier(e);if(!mod||e.dead)return;
    const p=worldToScreen(e.x,e.y,8);ctx.save();ctx.strokeStyle=eliteColors[mod];ctx.lineWidth=2;ctx.setLineDash(mod==='Echoing'?[4,4]:[]);ctx.beginPath();ctx.ellipse(p.x,p.y,e.r*54,e.r*27,0,0,TAU);ctx.stroke();
    if(e.elite){ctx.font='bold 10px system-ui';ctx.fillStyle=eliteColors[mod];ctx.textAlign='center';ctx.fillText(mod,p.x,p.y-42);}ctx.restore();
  };
  window.AWEnemyCombat={
    definitions,profile,family,actions,start,shapes,inShape,distanceToSegment,drawAction,
    get majorDodge(){return majorDodge;},get movement(){return {velocity,steady};},
    extinguish(e,r){for(const a of actions())if(a.ability==='pool'&&a.element==='fire'&&a.marks.some(s=>dist(s,e)<r+s.r)){a.active=Math.min(a.active,a.age-a.wind+.05);a.life=Math.min(a.life,.1);fx('frostNova',e.x,e.y,.3,'#bff4ff',{r});}},
    projectileThreat(q,dt){return q.combatAbilityId&&distanceToSegment(game.player,{x:q.x-q.vx*dt,y:q.y-q.vy*dt},q)<q.r+game.player.r;},
    projectileHit(q){if(q.combatAbilityId&&game.player.dodgeTime>0)perfect(q);}
  };
})();
