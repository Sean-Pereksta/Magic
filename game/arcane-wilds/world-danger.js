'use strict';
/* World danger is a campaign adapter: seeded conditions, optional encounters,
 * objective problems and region mechanics. All clocks advance with simulation. */
(() => {
  const A=AWCampaign,D=AWCampaignData,C=AWDangerContent,B=AWEnemyCombat,K=AWDangerCombat,F=AWCombatAffinity;
  let room=null,environment=null,objects=[],jobs=[],activity=null,problem=null,spawnContext=null,spawnOrdinal=0;
  let legendLocations={},legendSeed=null;
  const active=()=>running&&!paused&&!modalPause&&!roomTransition&&room===game.roomData;
  const state=()=>A.state()?.danger;
  const has=id=>!!state()?.modifiers.includes(id);
  const night=()=>Math.floor((state()?.visits||0)/4)%2===1;
  const addObject=(action,x,y,label,extra={})=>{const o={type:'dangerObject',action,x,y,label,...extra};objects.push(o);game.interactables.push(o);return o;};
  function ensureState(){if(!A.state())return null;if(!state()||state().seed!==(game.seed>>>0))A.state().danger=C.fresh(game.seed);return state();}
  function syncLegends(){
    if(legendSeed!==game.seed){legendLocations=C.legendsFor(game.seed);legendSeed=game.seed;}
    for(const [node,id] of Object.entries(legendLocations))state().legends[node]||={id,discovered:false,defeated:false};
  }
  function family(){
    return ({frost:'frost',crystal:'void',volcanic:'fire',desert:'sand',swamp:'swamp',forest:'nature',thornwild:'nature',stormlands:'storm',ruins:'stone',gloam:'void',celestial:'void',bloodroot:'nature',crypt:'void'})[A.current()?.biome]||'nature';
  }
  function hint(text){toastMsg(text);window.AWInput?.clear();}
  function warn(at,r,element='earth',duration=.2,damage=9,extra={}){
    return B.hazard({id:0,x:at.x,y:at.y,r:.3,damage,color:F.colors[element]||'#dcc5ad',dangerEnvironment:true,...extra},[{shape:'circle',x:at.x,y:at.y,r,delay:0}],{wind:1,active:duration,damage,element,color:F.colors[element]||'#dcc5ad',slow:element==='frost'});
  }
  const baseSpawn=spawnEnemy;
  spawnEnemy=function(id,pos,elite=false,scale=1){
    if(spawnContext&&scale===1&&ENEMY_TYPES[id]?.damage>0&&!spawnContext.boss){
      const n=D.nodes[spawnContext.campaignNode],i=spawnOrdinal++,seed=C.hash(game.seed,n.id+'/'+i);
      if(n.threat>=3&&i%3===1){
        if(has('goblinUprising'))id=i%2?'dangerRaider':'archer';
        else if(has('undeadRising')&&night())id=i%2?'vampire':'skeleton';
        else if(has('longWinter')&&seed%3===0)id='frostwitch';
        else {const types=Object.keys(C.pursuits).filter(k=>ENEMY_TYPES[k].min<=n.threat+1);if(types.length&&(seed%2===0||has('monsterMigration')))id=types[seed%types.length];}
      }
    }return baseSpawn(id,pos,elite,scale);
  };
  const baseRoomSpawn=spawnRoomEnemies;
  spawnRoomEnemies=function(r){const previous=spawnContext;spawnContext=D.nodes[r.campaignNode]&&!D.nodes[r.campaignNode].shadow?r:null;spawnOrdinal=0;try{return baseRoomSpawn(r);}finally{spawnContext=previous;}};
  function bossProfile(e){
    const phase=e.intensityPhase||1,f=e.dangerFamily||B.family(e);
    const profiles={
      stone:[['charge','counter','cleave'],['leapSlam','rupture','charge'],['chainDash','rupture','pounceWave']],
      sand:[['burrow','hunterDash'],['hunterDash','rupture','burrow'],['chainDash','burrow','pounceWave']],
      frost:[['cleave','leapSlam'],['frostTrail','rupture','fan'],['chainDash','frostTrail','beam']],
      fire:[['charge','leapSlam'],['flameWave','doubleDash','meteor'],['beam','chainDash','pounceWave']],
      storm:[['hunterDash','chain'],['chainDash','chain','leapVolley'],['beam','chain','pounceWave']],
      nature:[['cage','vaultSlash'],['pounceWave','cage','rupture'],['chainDash','cage','pounceWave']],
      void:[['fan','blink'],['gravity','beam','shadowAmbush'],['beam','gravity','chainDash']]
    };
    e.dangerProfile=(profiles[f]||profiles.void)[Math.min(2,phase-1)];e._combatProfileKey=null;
  }
  const basePhase=intensityBossPhase;
  intensityBossPhase=function(e,phase){basePhase(e,phase);if(e.dangerBoss){bossProfile(e);if(environment)environment.next=Math.min(environment.next,2);}};
  function configureBoss(e,forced){
    e.dangerBoss=true;e.dangerFamily=forced||({swamp:'nature'})[family()]||family();bossProfile(e);K.arena(e,e.dangerFamily);
    if(e.dangerFamily==='void')e.dangerArenaArmor=true;
    if(e.dangerFamily==='fire'){
      for(const [x,y] of [[6,7],[12,7]])addObject('platform',x,y,'Activate stone refuge',{safeTime:0,r:.9});
      hint('Lava warns before spreading. Activate the stone refuges to cross dangerous ground.');
    }else if(e.dangerFamily==='frost')hint('Disrupt ice crystals to clear freezing ground.');
    else if(e.dangerArenaArmor)hint(e.dangerFamily==='sand'?'Lure the worm into the explosives.':'Lure charges into pillars or disrupt shield crystals.');
  }
  const problemKinds=['fire','mine','nest','seal','flood','caravan'];
  const problemNodes=Object.values(D.nodes).filter(n=>n.type==='event'&&!n.shadow&&!['waves','defend'].includes(n.event));
  problemNodes.forEach((n,i)=>{n.dangerProblem=problemKinds[i%problemKinds.length];});
  function setupProblem(n){
    if(!n.dangerProblem||A.state().cleared.includes(n.id))return;
    const r=state().problems[n.id]||={kind:n.dangerProblem,done:false,steps:[]};if(r.done)return;
    problem={kind:r.kind,record:r,time:0,next:9,water:0,channel:null,progress:0,wagonHp:100,eggTime:18,order:C.shuffled([0,1,2,3],C.hash(game.seed,n.id)),step:0};
    room.worldEvent=null;game.interactables=game.interactables.filter(o=>o.type!=='eventAlly');
    if(problem.kind==='seal'){problem.step=0;r.steps=[];}
    for(const e of game.enemies)if(e.type==='regionalCrystal'){e.dead=true;e.hp=0;}
    game.enemies=game.enemies.filter(e=>!e.dead);
    if(problem.kind==='fire'){
      [[5,4],[9,4],[13,4]].forEach(([x,y],i)=>addObject('fire',x,y,'Extinguish building '+(i+1),{index:i}));
      [[6,9],[12,9]].forEach(([x,y],i)=>addObject('rescue',x,y,'Rescue trapped villager',{index:i+3}));
      addObject('water',3,10,'Fill rescue bucket');
    }else if(problem.kind==='mine'){
      [[5,5],[13,5]].forEach(([x,y],i)=>addObject('debris',x,y,'Clear unstable debris',{index:i}));
      [[5,9],[13,9]].forEach(([x,y],i)=>addObject('mechanism',x,y,'Brace mine supports',{index:i+2}));
      addObject('miner',9,3,'Free the trapped miners',{index:4});
    }else if(problem.kind==='nest'){
      [[5,5],[13,5],[9,9]].forEach(([x,y],i)=>addObject('egg',x,y,'Destroy nest egg',{index:i}));
    }else if(problem.kind==='seal'){
      [[5,4],[13,4],[13,9],[5,9]].forEach(([x,y],i)=>addObject('sealRune',x,y,['Leaf','Flame','Moon','Sun'][i],{index:i}));
    }else if(problem.kind==='flood'){
      addObject('valve',4,9,'Drain the outer chamber',{index:0});addObject('valve',14,9,'Drain the inner chamber',{index:1});addObject('ruinVault',9,3,'Reach the submerged archive',{index:2});
    }else {
      problem.wagon=addObject('wagon',4,7,'Repair / escort the caravan',{index:0});
    }
    addObject('problemClue',9,12,'Read the local situation');
    hint(problemHint());
  }
  function problemHint(){
    if(!problem)return '';
    return ({
      fire:'Fill the bucket at the pump. Put out three fires and rescue two villagers while fighting the attackers.',
      mine:'Clear both debris piles, brace both supports, then rescue the miners. A falling-rock warning gives time to move.',
      nest:'Crush all three eggs before they hatch. Each egg needs a short uninterrupted interaction.',
      seal:'Repair the seal in this order: '+problem.order.map(i=>['Leaf','Flame','Moon','Sun'][i]).join(' → ')+'. Fight the portal creatures while visiting the runes.',
      flood:'Drain the outer valve, then the inner valve. The flood blocks the north chamber until both are open. Reach its archive and defeat the guardians.',
      caravan:'Stay within three paces to escort the wagon east. Enemies near it damage the wagon; use it to repair before continuing.'
    })[problem.kind];
  }
  function problemReady(){
    if(!problem)return true;
    const r=problem.record;
    if(problem.kind==='caravan')return problem.progress>=1&&problem.wagonHp>0;
    const count=problem.kind==='fire'||problem.kind==='mine'?5:problem.kind==='seal'?4:3;
    return Array.from({length:count},(_,i)=>r.steps[i]===true).every(Boolean);
  }
  function claim(id,item,gold=110){
    if(state().claims.includes(id))return false;state().claims.push(id);
    game.gold+=gold;addMaterial(A.current().material||'dust',6,true);
    AWInventory.receive(A.craftItem(item),{claim:id,node:A.current().id,label:A.current().name});
    saveGame();updateHUD(true);return true;
  }
  function optionalKind(n){
    const legend=state()?.legends[n.id];if(legend&&!legend.defeated)return 'legendary';
    if(['landmark','treasure'].includes(n.type)&&n.index%3===0)return 'trial';
    if(['shrine','resource'].includes(n.type))return 'risk';
    return null;
  }
  function setupEnvironment(n){
    const f=family();environment={family:f,next:7,conditionNext:10,patches:[],drift:{x:0,y:0}};
    if(n.town||n.homestead||n.shadow||n.type==='puzzle'){environment=null;return;}
    if(f==='frost'||has('longWinter'))environment.patches=[{x:6,y:6,r:1.15,stress:0,reset:0},{x:12,y:8,r:1.15,stress:0,reset:0}];
    if(f==='swamp'){addObject('gas',5,7,'Unstable swamp gas',{r:1.2,used:false});addObject('waterPool',13,7,'Conductive marsh water',{r:1.3});}
    if(['stone','sand','nature'].includes(f)&&n.threat>=3)addObject('barrel',5,8,'Detonate powder barrel',{used:false,r:1.5});
    if(f==='void'){
      addObject('portal',4,4,'Arcane portal → far corner',{to:{x:14,y:10}});
      addObject('portal',14,10,'Arcane portal → near corner',{to:{x:4,y:4}});
    }
  }
  const baseLoad=loadRoom;
  loadRoom=function(){
    room=null;environment=null;objects=[];jobs=[];activity=null;problem=null;K.reset();
    const result=baseLoad();room=game.roomData;
    if(!A.inCampaign()||!ensureState()||A.current().shadow)return result;
    syncLegends();const n=A.current();state().visits++;
    if(state().legends[n.id])state().legends[n.id].discovered=true;
    setupEnvironment(n);setupProblem(n);
    for(const e of game.enemies)if(e.boss)configureBoss(e);
    const optional=optionalKind(n);
    if(optional&&!state().claims.includes('optional:'+n.id))addObject('optional',9,10,optional==='legendary'?'Challenge '+C.legendary.find(l=>l.id===state().legends[n.id].id).name:optional==='trial'?'Enter an optional challenge':'Explore the dangerous site',{optional});
    refreshHomeObjects();
    saveGame();return result;
  };
  function refreshHomeObjects(){
    if(room!==game.roomData||!AWHome.atHome()||AWHome.inside())return;
    objects=objects.filter(o=>o.action!=='settlement');
    game.interactables=game.interactables.filter(o=>o.type!=='dangerObject'||o.action!=='settlement');
    addObject('settlement',7.5,11,'Settlement noticeboard');
  }
  function startBattle(kind){
    if(!active()||!room.cleared||game.enemies.some(e=>!e.dead)||activity||AWHome.pending())return false;
    if(kind==='settlement'&&(!AWHome.atHome()||AWHome.inside()||!AWHome.state().dangerEvent))return false;
    closeOverlay('npcPanel');AWCampaignUI.close();
    const n=A.current();
    activity={kind,id:kind==='settlement'?AWHome.state().dangerEvent.id:n.id,time:0,next:1.5,round:1,noHit:true,weakKills:0,finished:false,switches:[]};
    room.cleared=false;intensityState().encounter=null;intensityState().hazards=[];game.effects=game.effects.filter(e=>e.kind!=='enemyAbility');
    if(kind==='legendary'){
      const l=C.legendary.find(l=>l.id===state().legends[n.id]?.id);if(!l){activity=null;room.cleared=true;return false;}
      const e=spawnBoss({name:l.name,base:l.base,ai:l.ai,hp:3.1,damage:1.2,color:'#efc77c'},room);e.dangerLegend=l.id;activity.legend=l;configureBoss(e,l.family);
    }else if(kind==='trial'){
      const types=['survival','noHit','elemental','mobility','bossRush'];activity.trial=types[C.hash(game.seed,n.id)%types.length];
      if(activity.trial==='mobility'){
        [[4,4],[14,4],[9,8]].forEach(([x,y],i)=>addObject('trialSwitch',x,y,'Mobility checkpoint '+(i+1),{index:i}));
        addObject('trialFinish',14,10,'Finish the trap course');
      }else if(activity.trial==='elemental')for(const [i,id] of ['frostwitch','drake','skeleton'].entries()){const e=spawnEnemy(id,{x:4+i*5,y:4});e.dangerTrial=true;e.dangerElement=true;e.damage*=.75;}
      else trialWave();
      hint(({survival:'Survive 20 seconds. Dodge and manage the wave pressure.',noHit:'Complete the duel. Avoid all damage for the bonus relic.',elemental:'Use elemental weaknesses on all three enemies for the bonus.',mobility:'Visit all three checkpoints, then the finish beacon. Watch the trap warnings.',bossRush:'Defeat three affixed elites in succession.'})[activity.trial]);
    }else if(kind==='settlement'){
      for(const [i,id] of ['dangerRaider','wolf','archer'].entries()){const e=spawnEnemy(id,{x:4+i*5,y:3},i===0);e.dangerTrial=true;if(i===0)K.affix(e,['Commander']);}
      hint('Defend Hearthglade. Leave by a road to abandon and retry; your house is safe.');
    }else{
      for(const [i,id] of ['dangerHunter','dangerDasher'].entries()){const e=spawnEnemy(id,{x:5+i*7,y:4},true);e.dangerTrial=true;}
      hint('Optional dangerous encounter • defeat the two elites for a rare relic.');
    }
    return true;
  }
  function trialWave(){
    if(!activity)return;
    const count=activity.trial==='bossRush'?1:2;
    for(let i=0;i<count;i++){const type=activity.trial==='bossRush'?['dangerHunter','dangerBeast','dangerDasher'][activity.round-1]:'dangerHunter';const e=spawnEnemy(type,{x:5+i*7,y:4},activity.trial==='bossRush');e.dangerTrial=true;}
  }
  function finishActivity(){
    if(!activity||activity.finished)return false;
    activity.finished=true;room.cleared=true;
    if(activity.kind==='settlement'){hint('Hearthglade defended. Return to the noticeboard to collect the supplies.');return true;}
    if(activity.kind==='legendary'){state().legends[A.current().id].defeated=true;claim('optional:'+A.current().id,activity.legend.reward,180);hint(activity.legend.name+' defeated • legendary trophy recorded.');}
    else {
      const bonus=activity.kind==='trial'&&(activity.trial==='elemental'?activity.weakKills===3:activity.noHit);
      claim('optional:'+A.current().id,bonus?'stormRelic':'runeRelic',bonus?160:100);hint(bonus?'Challenge complete • bonus earned!':'Dangerous encounter complete • rare relic earned.');
    }saveGame();return true;
  }
  const baseClear=markRoomCleared;
  markRoomCleared=function(){
    if(room===game.roomData){
      if(activity&&!activity.finished){
        if(activity.kind==='trial'&&(activity.trial==='survival'&&activity.time<20||activity.trial==='mobility'||activity.trial==='bossRush'&&activity.round<3))return;
        if(game.enemies.some(e=>!e.dead))return;finishActivity();return;
      }
      if(problem&&!problemReady())return;
    }
    const was=game.roomData?.cleared,result=baseClear();
    if(!was&&room===game.roomData&&room.cleared&&problem&&!problem.record.done){
      problem.record.done=true;claim('problem:'+A.current().id,'groveRelic',90);hint('World problem solved • rescued supplies and rare relic earned.');
    }return result;
  };
  const baseDoor=doorOpen;doorOpen=function(dir){if(room===game.roomData&&activity&&!activity.finished){const exits=AWTravel.exits();return dir?!!exits[dir]:Object.keys(exits).length>0;}return baseDoor(dir);};
  const baseReason=D.travelReason;D.travelReason=function(s,id,mode='road'){
    if(s===A.state()&&room===game.roomData&&activity&&!activity.finished&&mode==='road'&&A.current()?.connections.includes(id))return s.unlocked.includes(D.nodes[id]?.continent)?'':'That continent is still locked.';
    return baseReason(s,id,mode);
  };
  const baseDamage=damageEnemy;
  damageEnemy=function(e,amount,tag='',dot=false,source=null){
    const element=source?.combatElement||F.element(tag);
    if(e?.dangerElement&&F.resolve(element,e).multiplier>1)e.dangerWeakHit=true;
    return baseDamage(e,amount,tag,dot,source);
  };
  const baseKill=killEnemy;killEnemy=function(e,...args){const alive=e&&!e.dead;const result=baseKill(e,...args);if(alive&&e.dead&&room===game.roomData&&activity&&e.dangerElement&&e.dangerWeakHit)activity.weakKills++;return result;};
  const baseHurt=damagePlayer;
  damagePlayer=function(amount,...args){
    const source=window.AWRegionalContent?.incoming;
    if(source?.dangerLava&&objects.some(o=>o.action==='platform'&&o.safeTime>0&&dist(o,game.player)<o.r))return;
    const hp=game.player?.hp,beforeRoom=game.roomData,result=baseHurt(amount,...args);
    if(beforeRoom===game.roomData&&game.player?.hp<hp){if(activity)activity.noHit=false;if(problem?.channel)problem.channel=null;}return result;
  };
  function channel(o,seconds){
    problem.channel={object:o,remaining:seconds,x:game.player.x,y:game.player.y,hp:game.player.hp};hint('Working • stay close and avoid damage.');return true;
  }
  function interactProblem(o){
    const p=problem;if(!p)return false;
    const s=p.record.steps;
    if(o.action==='problemClue'){openPanel('Local situation',problemHint(),[['Reset this situation',()=>{p.record.steps=[];saveGame();closeOverlay('npcPanel');A.enter(A.current().id,A.state().room);}]]);return true;}
    if(o.action==='water'){p.water=3;hint('Rescue bucket filled • three uses.');return true;}
    if(o.action==='fire'){if(s[o.index])return true;if(!p.water){hint('Fill your bucket at the pump first.');return true;}p.water--;s[o.index]=true;}
    else if(o.action==='rescue'){if(!s.slice(0,3).some(Boolean)){hint('Put out a fire to open a rescue path first.');return true;}s[o.index]=true;}
    else if(o.action==='debris'||o.action==='egg')return channel(o,o.action==='debris'?1.2:.8);
    else if(o.action==='mechanism')s[o.index]=true;
    else if(o.action==='miner'){if(![0,1,2,3].every(i=>s[i])){hint('Clear debris and brace both supports first.');return true;}s[o.index]=true;}
    else if(o.action==='sealRune'){
      if(o.index!==p.order[p.step]){p.step=0;p.record.steps=[];warn(game.player,.9,'arcane');hint('Wrong rune. Read the clue at the entrance.');}
      else {p.step++;s[o.index]=true;hint(p.step+'/4 seal runes repaired.');}
    }else if(o.action==='valve'){
      if(o.index===1&&!s[0]){hint('Drain the outer chamber first.');return true;}s[o.index]=true;hint('Water recedes; another passage opens.');
    }else if(o.action==='ruinVault'){if(!s[0]||!s[1])return false;s[2]=true;}
    else if(o.action==='wagon')return channel(o,1);
    else return false;
    saveGame();if(problemReady()&&!game.enemies.length)markRoomCleared();return true;
  }
  function openPanel(title,text,buttons){
    const body=$('npcBody');body.replaceChildren();$('npcName').textContent=title;
    const p=document.createElement('p');p.textContent=text;body.appendChild(p);
    for(const [label,fn,disabled] of buttons){const b=document.createElement('button');b.type='button';b.className='btn secondary';b.textContent=label;b.disabled=!!disabled;b.onclick=fn;body.appendChild(b);}
    showOverlay('npcPanel');
  }
  function optionalPanel(o){
    if(state().claims.includes('optional:'+A.current().id))return hint('This encounter reward was already earned.');
    openPanel(o.label,'A dangerous optional fight with a guaranteed rare relic. You can leave through the roads and retry. Bosses use three phases; charges, pillars, crystals and lava refuges change the arena.',[['Begin encounter',()=>{closeOverlay('npcPanel');startBattle(o.optional);},!room.cleared||!!game.enemies.length]]);
  }
  function settlementPanel(){
    const event=AWHome.state().dangerEvent,d=AWHomeCore.dangerEvents[event?.kind];
    if(!d)return openPanel('Hearthglade noticeboard','A new situation appears after every three newly completed expedition sites. House upgrades create additional ways to help.',[]);
    const station=AWHome.state().items.some(i=>!i.packed&&i.kind===d.station);
    const cost=Object.entries(d.cost).map(([k,v])=>v+' '+k).join(' + ')||'no supplies';
    const reward=Object.entries(d.reward).map(([k,v])=>v+' '+k).join(' + ');
    const resolve=async solution=>{
      if(AWHome.pending())return;
      $('npcBody').querySelectorAll('button').forEach(b=>b.disabled=true);
      const ok=await AWHome.action('resolveDangerEvent',{eventId:event.id,solution});
      if(ok){activity=null;settlementPanel();}else {settlementPanel();const p=document.createElement('p');p.textContent=AWHomeCloud.status();$('npcBody').appendChild(p);}
    };
    const buttons=[['Help with supplies • '+cost,()=>resolve('help'),AWHome.pending()]];
    buttons.push(['Use '+AWHomeCore.items[d.station].name,()=>resolve('prepared'),!station||AWHome.pending()]);
    if(d.raid)buttons.push([activity?.finished?'Collect defense rewards':'Defend the settlement',()=>activity?.finished?resolve('defend'):(closeOverlay('npcPanel'),startBattle('settlement')),AWHome.pending()]);
    openPanel(d.name,'Your homestead has a local problem to resolve. Rewards: '+reward+'. Upgrades count when placed in the world. '+(d.raid?'Defense is optional; roads let you retreat.':''),buttons);
  }
  function activate(o){
    if(!active()||o?.type!=='dangerObject'||dist(o,game.player)>1.45)return false;
    if(o.action==='optional'){optionalPanel(o);return true;}
    if(o.action==='settlement'){settlementPanel();return true;}
    if(problem&&interactProblem(o))return true;
    if(o.action==='platform'){o.safeTime=6;hint('Stone refuge active for six seconds.');return true;}
    if(o.action==='portal'){game.player.x=o.to.x;game.player.y=o.to.y;game.player.invuln=Math.max(game.player.invuln,.35);hint('Arcane portal crossed.');return true;}
    if(['barrel','gas'].includes(o.action)&&!o.used)return armExplosion(o);
    if(o.action==='trialSwitch'&&activity?.trial==='mobility'){activity.switches[o.index]=true;hint(activity.switches.filter(Boolean).length+'/3 checkpoints.');return true;}
    if(o.action==='trialFinish'&&activity?.trial==='mobility'&&activity.switches.filter(Boolean).length===3){finishActivity();return true;}
    return false;
  }
  function armExplosion(o){
    if(o.used)return false;
    o.used=true;if(warn(o,o.r,'fire',.2,12)){jobs.push({at:1,object:o,room,element:'fire'});hint('Explosion armed • leave the marked area.');}else o.used=false;return true;
  }
  const baseInteract=interact;interact=function(){const o=currentInteraction();if(o?.type==='dangerObject')return activate(o);return baseInteract();};
  const baseInteraction=currentInteraction;currentInteraction=function(){
    const previous=game.interactables;game.interactables=previous.filter(o=>o.life===undefined||o.life>0);
    try{return baseInteraction();}finally{game.interactables=previous;}
  };
  const baseMove=playerMovement;
  playerMovement=function(dt){
    const p=game.player;if(!p)return baseMove(dt);const from={x:p.x,y:p.y},before=game.roomData,speed=p.speed;
    if(environment&&active()&&p.dodgeTime<=0){
      if(environment.family==='sand'&&dist(p,{x:6,y:7})<1.2)p.speed*=.7;
      if(environment.family==='swamp'&&objects.some(o=>o.action==='waterPool'&&dist(o,p)<o.r))p.speed*=.75;
    }
    let result;try{result=baseMove(dt);}finally{p.speed=speed;}
    if(before!==room||before!==game.roomData)return result;
    if(problem?.kind==='flood'&&!(problem.record.steps[0]&&problem.record.steps[1])&&p.y<5)p.y=Math.max(5,from.y);
    if(environment?.patches.some(o=>o.reset<=0&&dist(o,p)<o.r)&&p.dodgeTime<=0&&!AWRegionalContent.has('frostwalk')&&dt>0){
      const d=environment.drift;d.x=lerp(d.x,(p.x-from.x)/dt,.24);d.y=lerp(d.y,(p.y-from.y)/dt,.24);
      p.x=clamp(p.x+d.x*dt*.2,.3,17.7);p.y=clamp(p.y+d.y*dt*.2,.3,13.7);
    }else if(environment)environment.drift={x:0,y:0};
    return result;
  };
  function elementalInteraction(element){
    const p=game.player,dir=spellAim();
    const reachable=o=>dist(o,p)<6&&((o.x-p.x)*dir.x+(o.y-p.y)*dir.y>0||dist(o,p)<2);
    for(const o of K.objects())if(o.life>0&&reachable(o))K.breakObject(o,element);
    for(const o of objects)if(reachable(o)){
      if(problem&&o.action==='fire'&&['frost','water'].includes(element))problem.record.steps[o.index]=true;
      if(problem&&o.action==='debris'&&element==='earth')problem.record.steps[o.index]=true;
      if(o.action==='gas'&&element==='fire')armExplosion(o);
    }
    if(environment&&element==='frost')for(const a of B.actions())if(a.enemy?.dangerLava&&a.marks.some(s=>dist(s,p)<5)){a.life=0;burst(p.x,p.y,'#b6e9ff',6,.6);}
    if(problem)saveGame();
  }
  const baseCast=castSpell;castSpell=function(slot){const p=game.player,id=p?.activeSpells[slot],cd=p?.spellState[id]?.cd||0,result=baseCast(slot);if(id&&(p.spellState[id]?.cd||0)>cd&&active())elementalInteraction(F.spellElement(id));return result;};
  function regionTick(dt){
    if(!environment||!active()||room.cleared&&!(activity&&!activity.finished)&&!(problem&&!problem.record.done))return;
    for(const o of environment.patches){
      o.reset=Math.max(0,o.reset-dt);
      if(o.reset<=0&&dist(o,game.player)<o.r&&game.player.dodgeTime<=0)o.stress+=dt;else o.stress=Math.max(0,o.stress-dt);
      if(o.stress>1.5&&warn(o,o.r,'frost',1.8,7)){o.reset=6;o.stress=0;hint('Ice cracking • leave the circle!');}
    }
    for(const o of objects)if(o.action==='waterPool')for(const e of game.enemies)if(dist(e,o)<o.r)e.regionalWet=1;
    environment.next-=dt;environment.conditionNext-=dt;
    if(environment.next<=0){
      let ok=false;const p={x:game.player.x,y:game.player.y},f=environment.family;
      if(f==='fire')ok=warn({x:game.player.x<9?5:13,y:clamp(p.y,3,11)},1.25,'fire',4,9,{dangerLava:true});
      else if(f==='stone'||f==='sand')ok=warn(p,.95,'earth',.2,11);
      else if(f==='swamp')ok=warn(p,1,'poison',2.5,5);
      else if(f==='void'||f==='storm')ok=warn(p,.8,f==='storm'?'lightning':'arcane',.2,9);
      if(ok)environment.next=room.boss?Math.max(2.8,5-(game.enemies.find(e=>e.boss)?.intensityPhase||1)*.6):7;
      else environment.next=1;
    }
    if(environment.conditionNext<=0){if(has('arcaneStorm'))warn(game.player,.7,'arcane',.2,8);environment.conditionNext=12;}
    for(const a of B.actions())if(a.dangerSlow&&a.age>=a.wind&&a.age<a.wind+a.active&&a.marks.some(s=>B.inShape(s,game.player))&&game.player.dodgeTime<=0)game.player.combatChill=.7;
  }
  function problemTick(dt){
    if(!problem||problem.record.done)return;const p=problem; p.time+=dt;
    if(p.channel){
      const c=p.channel;if(dist(game.player,c)>1||game.player.hp<c.hp)p.channel=null;
      else {c.remaining-=dt;if(c.remaining<=0){if(c.object.action==='wagon')p.wagonHp=100;else p.record.steps[c.object.index]=true;p.channel=null;saveGame();}}
    }
    if(p.kind==='caravan'){
      const enemies=game.enemies.filter(e=>!e.dead&&dist(e,p.wagon)<2.5);
      p.wagonHp=Math.max(0,p.wagonHp-dt*enemies.length*5);
      if(p.wagonHp>0&&dist(game.player,p.wagon)<3&&!p.channel){p.progress=Math.min(1,p.progress+dt/18);p.wagon.x=4+p.progress*10;}
    }
    if(p.kind==='nest'){
      p.eggTime-=dt;if(p.eggTime<=0){p.eggTime=999;
        for(const o of objects.filter(o=>o.action==='egg'&&!p.record.steps[o.index]))if(game.enemies.length<10){const e=spawnEnemy('dangerBeast',o,false,.7);e.xp=0;hint('An egg hatches • crush the remaining nests!');}
      }
    }
    if(p.kind==='seal'&&!problemReady()){p.next-=dt;if(p.next<=0){p.next=9;if(game.enemies.length<6){const e=spawnEnemy('wisp',randomEnemySpawn(),false,.7);e.xp=0;}}}
    if(p.kind==='fire'){p.next-=dt;if(p.next<=0){p.next=10;const fire=objects.find(o=>o.action==='fire'&&!p.record.steps[o.index]);if(fire)warn(fire,1.1,'fire',3,6);}}
    if(problemReady()&&!game.enemies.length)markRoomCleared();
  }
  function activityTick(dt){
    if(!activity||activity.finished)return;activity.time+=dt;
    const foes=game.enemies.filter(e=>!e.dead);
    if(activity.kind==='trial'){
      if(activity.trial==='survival'){
        if(activity.time>=20){for(const e of foes){B.cancel(e);e.dead=true;}game.enemies=[];finishActivity();return;}
        if(!foes.length){activity.next-=dt;if(activity.next<=0){trialWave();activity.next=1.5;}}
      }else if(activity.trial==='mobility'){
        activity.next-=dt;if(activity.next<=0){activity.next=2.6;warn({x:game.player.x,y:game.player.y},.9,'earth',.2,10);}
      }else if(activity.trial==='bossRush'&&!foes.length&&activity.round<3){
        activity.next-=dt;if(activity.next<=0){activity.round++;trialWave();activity.next=1.8;}
      }else if(!foes.length)finishActivity();
    }else if(!foes.length)finishActivity();
  }
  const baseUpdate=update;update=function(dt){
    const before=game.roomData;baseUpdate(dt);if(!active()||before!==game.roomData)return;
    for(const o of objects)if(o.safeTime>0)o.safeTime=Math.max(0,o.safeTime-dt);
    for(const job of jobs){job.at-=dt;if(job.at<=0&&job.room===room&&!job.done){job.done=true;radialDamage(job.object.x,job.object.y,job.object.r,30+room.difficulty*2,job.element);burst(job.object.x,job.object.y,'#ffb287',18,1);}}
    jobs=jobs.filter(j=>!j.done&&j.room===room);regionTick(dt);problemTick(dt);activityTick(dt);
  };
  const baseDraw=drawInteractable;
  drawInteractable=function(o){
    if(o.type!=='dangerObject')return baseDraw(o);
    const done=problem?.record.steps[o.index],p=worldToScreen(o.x,o.y,15);
    const symbol=({settlement:'📋',optional:'⚔',platform:'▰',gas:'☁',waterPool:'≈',barrel:'💥',portal:'◉',problemClue:'📜',fire:done?'✓':'🔥',rescue:done?'✓':'🙋',debris:done?'✓':'▥',mechanism:done?'✓':'⚙',miner:done?'✓':'⛏',egg:done?'✓':'🥚',sealRune:done?'✦':'◇',valve:done?'✓':'⟳',ruinVault:'📚',wagon:'🛒',trialSwitch:activity?.switches[o.index]?'✓':'◈',trialFinish:'⚑'})[o.action]||'◇';
    ctx.save();ctx.textAlign='center';ctx.fillStyle=o.safeTime>0?'#b9efa7':done?'#a5d6ad':'#ecd7a5';ctx.font='24px system-ui';ctx.fillText(symbol,p.x,p.y);
    if(dist(game.player,o)<2||['optional','settlement','sealRune'].includes(o.action)){ctx.font='10px system-ui';ctx.fillText(o.label,p.x,p.y-30);}
    if(o.action==='wagon'&&problem){ctx.font='10px system-ui';ctx.fillText(Math.ceil(problem.wagonHp)+' HP • '+Math.round(problem.progress*100)+'%',p.x,p.y-47);}
    ctx.restore();
  };
  const baseMarks=drawTelegraphs;drawTelegraphs=function(){
    baseMarks();if(room!==game.roomData)return;
    if(environment){for(const p of environment.patches){B.drawShape({shape:'circle',...p},p.reset>0?'#557997':'#b6efff',.12+Math.min(.2,p.stress*.1));}
      if(environment.family==='sand')B.drawShape({shape:'circle',x:6,y:7,r:1.2},'#dcc394',.13);
      if(environment.family==='swamp')for(const o of objects)if(['waterPool','gas'].includes(o.action))B.drawShape({shape:'circle',...o},o.action==='gas'?'#c9e790':'#82b8ba',.15);
    }
    for(const o of objects)if(o.action==='platform')B.drawShape({shape:'circle',...o},o.safeTime>0?'#b5efa1':'#d4b898',.26);
    if(problem?.channel){const p=worldToScreen(problem.channel.object.x,problem.channel.object.y,55);ctx.save();ctx.font='bold 12px system-ui';ctx.fillStyle='#ffeeac';ctx.textAlign='center';ctx.fillText('WORKING • '+Math.ceil(problem.channel.remaining*10)/10+'s',p.x,p.y);ctx.restore();}
    if(activity?.kind==='trial'&&!activity.finished){const p=worldToScreen(9,7,100);ctx.save();ctx.font='bold 12px system-ui';ctx.fillStyle='#f8e4af';ctx.textAlign='center';ctx.fillText(activity.trial==='survival'?Math.max(0,Math.ceil(20-activity.time))+'s remaining':activity.trial==='bossRush'?'Elite '+activity.round+'/3':activity.trial==='elemental'?'Elemental counters '+activity.weakKills+'/3':activity.noHit?'Clean run':'Bonus lost • finish for base reward',p.x,p.y);ctx.restore();}
  };
  function nodeInfo(n){
    const l=state()?.legends[n.id],d=l?.discovered&&C.legendary.find(v=>v.id===l.id);
    if(d)return (l.defeated?'✓ Defeated: ':'Optional legendary creature: ')+d.name;
    if(n.dangerPuzzle)return 'Interactive puzzle: '+n.dangerPuzzle+'. Rare relic and clean-solution bonus.';
    if(n.dangerProblem)return 'World problem: '+n.dangerProblem+'. Complete the objective and defeat its attackers.';
    return C.modifiers[state()?.modifiers[0]]?.name+' / '+C.modifiers[state()?.modifiers[1]]?.name+(night()?' • Night patrols':' • Day patrols');
  }
  function journal(parent){
    const card=document.createElement('section');card.className='exp-card';
    const title=document.createElement('h3');title.textContent='World conditions';card.appendChild(title);
    for(const id of state()?.modifiers||[]){const p=document.createElement('p');p.textContent=C.modifiers[id].name+' • '+C.modifiers[id].desc;card.appendChild(p);}
    const b=document.createElement('button');b.className='btn secondary';b.textContent='Elite and elemental guide';b.onclick=()=>{
      A.inCampaign()&&AWCampaignUI.close();
      const counters=Object.entries(C.affixes).map(([name,d])=>name+': '+d.counter).join('\n\n');
      openPanel('Read the danger','Fire melts ice, burns plants and sears undead. Frost slows fast foes and suppresses fire. Water quenches fire. Lightning conducts through wet or armored enemies. Earth breaks armor; wind interrupts attacks and disrupts projectiles. Poison works best on living foes.\n\n'+counters,[]);
      const p=$('npcBody').querySelector('p');p.style.whiteSpace='pre-line';
    };card.appendChild(b);parent.appendChild(card);
  }
  window.AWWorldDanger={
    state,has,night,nodeInfo,journal,activate,startBattle,optionalKind,problemHint,problemReady,elementalInteraction,settlementPanel,refreshHomeObjects,
    clearFrostAt:at=>{if(environment)for(const p of environment.patches)if(dist(p,at)<6){p.reset=20;p.stress=0;}},
    resourceBonus:()=>has('ageOfPlenty')?3:0,
    settlementVictory:()=>activity?.kind==='settlement'&&activity.finished&&room===game.roomData?activity.id:null,
    defenseActive:()=>activity?.kind==='settlement'&&!activity.finished&&room===game.roomData,
    environment:()=>environment,objects:()=>objects,activity:()=>activity,problem:()=>problem,
    conditions:()=>state()?.modifiers.map(k=>C.modifiers[k])||[]
  };
})();
