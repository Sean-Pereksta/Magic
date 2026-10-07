'use strict';
/* Seven physical, resettable puzzles. Interaction uses the shared E / USE /
 * controller binding; the clue dialog pauses combat, never puzzle timers. */
(() => {
  const A=AWCampaign,C=AWDangerContent,B=AWEnemyCombat;
  let room=null,puzzle=null,carrying=null,memoryTime=0,timer=0,guardian=null,hitTaken=false;
  const key=()=>A.current()?.id+':'+(A.state()?.room||0);
  const state=()=>A.state()?.danger;
  const allowed=()=>running&&!paused&&!modalPause&&!roomTransition&&room===game.roomData;
  function record(){return state()?.puzzles[key()];}
  function object(id,action,x,y,label,extra={}){const o={id,action,x,y,label,type:'challengeObject',...extra};game.interactables.push(o);return o;}
  function setup(){
    room=game.roomData;puzzle=null;carrying=null;timer=0;memoryTime=0;guardian=null;hitTaken=false;
    if(!A.inCampaign()||A.current().shadow||!state())return;
    const kind=C.puzzleFor(A.current(),A.state().room);if(!kind)return;
    const seed=C.hash(game.seed,key());
    const r=state().puzzles[key()]||={kind,solved:false,bonus:false,step:0,mistakes:0,seals:[false,false,false],rotation:[3,2,1],crates:[{x:4,y:7},{x:6,y:7}]};
    puzzle={kind,seed,objects:[],order:C.shuffled([0,1,2,3],seed),pillars:[]};
    object('clue','clue',9,12,r.solved?'Challenge complete':'Read the room inscriptions');
    if(r.solved)return;
    if(kind==='runes'||kind==='memory'){
      const names=kind==='memory'?['Moon','Star','Flame','Leaf']:['Seed','Rain','Bloom','Rest'];
      const positions=[[4,4],[14,4],[14,9],[4,9]];
      names.forEach((name,i)=>puzzle.objects.push(object('rune'+i,'rune',...positions[i],name,{index:i,name})));
      if(kind==='memory'){
        const length=4+Math.min(2,Math.floor(A.current().threat/10));puzzle.order=Array.from({length},(_,i)=>C.hash(seed,'beat'+i)%4);
        r.step=0;object('replay','replay',9,7,'Replay the memory pattern');
      }
    }else if(kind==='mirrors'){
      for(const [i,[x,y]] of [[6,3],[6,9],[13,9]].entries())puzzle.objects.push(object('mirror'+i,'mirror',x,y,'Rotate mirror '+(i+1),{index:i}));
      object('receiver','receiver',13,3,'Light the crystal');
    }else if(kind==='plates'){
      if(r.crates.length!==2)r.crates=[{x:4,y:7},{x:6,y:7}];
      r.crates.forEach((v,i)=>puzzle.objects.push(object('crate'+i,'crate',v.x,v.y,'Lift crate '+(i+1),{index:i})));
      puzzle.plates=[{x:9,y:4},{x:12,y:4}];
    }else if(kind==='elements'){
      puzzle.elements=['fire','frost','lightning'];puzzle.attuned=0;
      for(let i=0;i<3;i++)puzzle.objects.push(object('seal'+i,'seal',4+i*5,4,'Attune '+['flame','snowflake','lightning'][i]+' seal',{index:i,element:puzzle.elements[i]}));
      object('conduit','conduit',9,9,'Borrow an elemental charge');
    }else if(kind==='timing'){
      r.seals=[false,false,false];puzzle.limit=Math.max(10,14-A.current().threat*.12);
      for(let i=0;i<3;i++)puzzle.objects.push(object('lever'+i,'lever',...[[4,4],[14,4],[9,10]][i],'Activate linked mechanism '+(i+1),{index:i}));
    }else if(kind==='combat'){
      object('trial','trial',9,7,'Awaken the armored guardian');
      puzzle.pillars=[[5,5],[13,5],[9,10]].map(([x,y],index)=>object('pillar'+index,'pillar',x,y,'Charge-breaking pillar',{index,r:.5,broken:false}));
    }
    saveGame();
  }
  const clues={
    mirrors:'Turn the three mirrors to carry the beam from the western lamp to the northern crystal. Slash and backslash faces bend the light; an open face passes it, a dark face blocks it.',
    plates:'Lift a crate with E / USE. Walk it onto a diamond plate, then use E / USE again to set it down. Both plates must stay occupied at the same time.',
    elements:'Flame warms the left seal; frost cools the middle; lightning wakes the right. Aim a matching spell at a seal, or borrow a charge from the central conduit and use it at a seal. The conduit cycles through the three elements.',
    memory:'Use the center pedestal to see the pattern. Watch the runes light up, then visit them in the same order. Replay whenever you need.',
    timing:'Activate all three mechanisms before the first one loses power. The countdown advances only while playing. Dodge between distant switches.',
    combat:'The guardian is powered by three pillars. Stand beyond a pillar to bait a charge into it, then dodge sideways. Break all three pillars to expose the guardian. You can abandon and retry this optional trial.'
  };
  function clue(){
    if(!puzzle)return '';
    const r=record();if(r.solved)return 'Solved. Your reward is already in Inventory. Every challenge rewards once per journey.';
    if(puzzle.kind==='runes')return 'The inscription reads: '+puzzle.order.map(i=>puzzle.objects[i].name).join(' → ')+'. Visit the stones in that order. A mistake resets the sequence and warns a trap.';
    return clues[puzzle.kind];
  }
  function panel(){
    if(!puzzle)return;
    const body=$('npcBody');body.replaceChildren();$('npcName').textContent=A.current().name+' • Interactive challenge';
    const p=document.createElement('p');p.textContent=clue();body.appendChild(p);
    if(!record().solved){const b=document.createElement('button');b.className='btn secondary';b.textContent='Reset puzzle';b.onclick=()=>{closeOverlay('npcPanel');resetPuzzle();};body.appendChild(b);}
    const note=document.createElement('p');note.textContent='Rare relic for solving; extra gold for a clean solution. Close this panel to interact with the objects in the room. Roads stay available.';body.appendChild(note);
    showOverlay('npcPanel');
  }
  function resetPuzzle(){
    if(!puzzle||record().solved)return false;
    if(guardian){B.cancel(guardian);guardian.dead=true;game.enemies=game.enemies.filter(e=>e!==guardian);}
    const mistakes=record().mistakes+1;state().puzzles[key()]={kind:puzzle.kind,solved:false,bonus:false,step:0,mistakes,seals:[false,false,false],rotation:[3,2,1],crates:[{x:4,y:7},{x:6,y:7}]};
    game.interactables=game.interactables.filter(o=>o.type!=='challengeObject');setup();toastMsg('Puzzle reset.');return true;
  }
  function mistake(){
    const r=record();r.step=0;r.mistakes++;r.seals=[false,false,false];
    B.hazard({id:0,x:game.player.x,y:game.player.y,r:.3,damage:10,color:'#e7b4ff'},[{shape:'circle',x:game.player.x,y:game.player.y,r:.7,delay:0}],{wind:1,active:.18,damage:8,element:'arcane',color:'#e7b4ff'});
    saveGame();toastMsg('The mechanism resets. Move out of the warning.');
  }
  function solve(){
    const r=record();if(!r||r.solved)return false;
    r.solved=true;r.bonus=r.mistakes===0&&!hitTaken;
    const claim='puzzle:'+key();if(state().claims.includes(claim))return false;state().claims.push(claim);
    const id=({runes:'runeRelic',mirrors:'mirrorRelic',plates:'stoneRelic',elements:'stormRelic',memory:'winterRelic',timing:'emberRelic',combat:'groveRelic'})[puzzle.kind];
    AWInventory.receive(A.craftItem(id),{claim,node:A.current().id,label:A.current().name});
    game.gold+=r.bonus?100:60;addMaterial(A.current().material||'dust',r.bonus?6:4,true);
    if(A.current().type==='puzzle'&&!A.state().claimed.includes(A.current().id))A.state().claimed.push(A.current().id);
    game.interactables=game.interactables.filter(o=>o.type!=='challengeObject');object('clue','clue',9,12,'Solved challenge');
    carrying=null;timer=0;saveGame();updateHUD(true);toastMsg(r.bonus?'Challenge solved • clean solution bonus!':'Challenge solved • rare relic earned.');return true;
  }
  function trace(){
    if(!puzzle||puzzle.kind!=='mirrors')return {segments:[],lit:false};
    const rotations=record().rotation;let at={x:2,y:3},dir={x:1,y:0};const segments=[],seen=new Set(),goal={x:13,y:3};
    for(let bounce=0;bounce<8;bounce++){
      const candidates=[...puzzle.objects,goal].filter(o=>Math.abs((o.x-at.x)*dir.y-(o.y-at.y)*dir.x)<.01&&(o.x-at.x)*dir.x+(o.y-at.y)*dir.y>.01).sort((a,b)=>dist(a,at)-dist(b,at));
      const to=candidates[0]||{x:dir.x?dir.x>0?17:1:at.x,y:dir.y?dir.y>0?13:1:at.y};
      segments.push({from:at,to});if(to===goal)return {segments,lit:true};
      if(!candidates.length)return {segments,lit:false};
      const stamp=to.index+'/'+dir.x+'/'+dir.y;if(seen.has(stamp))break;seen.add(stamp);
      const rotation=rotations[to.index]||0;at={x:to.x,y:to.y};
      if(rotation===0)dir={x:-dir.y,y:-dir.x};else if(rotation===1)dir={x:dir.y,y:dir.x};else if(rotation===3)break;
    }return {segments,lit:false};
  }
  function activate(o){
    if(!allowed()||!puzzle||o?.type!=='challengeObject'||dist(game.player,o)>1.45)return false;
    const r=record();if(o.action==='clue'){panel();return true;}if(r.solved)return false;
    if(o.action==='rune'){
      if(puzzle.kind==='memory'&&memoryTime>0){toastMsg('Watch the pattern first.');return true;}
      if(o.index!==puzzle.order[r.step]){mistake();return true;}
      r.step++;if(r.step===puzzle.order.length)solve();else{saveGame();toastMsg(r.step+'/'+puzzle.order.length+' runes alight.');}
    }else if(o.action==='replay'){r.step=0;memoryTime=puzzle.order.length*.9+.3;toastMsg('Watch the rune sequence.');}
    else if(o.action==='mirror'){r.rotation[o.index]=(r.rotation[o.index]+1)%4;if(trace().lit)solve();else saveGame();}
    else if(o.action==='crate'){carrying=o.index;toastMsg('Carrying crate • E / USE to set down.');}
    else if(o.action==='conduit'){puzzle.attuned=(puzzle.attuned+1)%3;toastMsg('Borrowed '+puzzle.elements[puzzle.attuned]+' • use it near a seal.');}
    else if(o.action==='seal'){applyElement(puzzle.elements[puzzle.attuned],o);if(!r.seals[o.index])toastMsg('This seal needs '+o.element+'.');}
    else if(o.action==='lever'){
      if(!timer)timer=puzzle.limit;r.seals[o.index]=true;
      if(r.seals.every(Boolean))solve();else toastMsg('Mechanism live • '+Math.ceil(timer)+' seconds remain.');
    }else if(o.action==='trial'){
      if(guardian&&!guardian.dead)return false;
      guardian=spawnEnemy('dangerRaider',{x:9,y:3});guardian.dangerPuzzleGuard=true;guardian.damage*=.8;guardian.hp=guardian.maxHp*=1.4;guardian.name='Pillar-bound Guardian';guardian.attack=1.5;
      toastMsg('Bait its charge through all three pillars.');
    }
    return true;
  }
  function applyElement(element,o){
    if(!puzzle||puzzle.kind!=='elements'||record().solved||o.element!==element)return false;
    record().seals[o.index]=true;burst(o.x,o.y,AWCombatAffinity.colors[element],10,.7);
    if(record().seals.length===3&&record().seals.every(Boolean))solve();else saveGame();return true;
  }
  function elementalCast(element,origin,dir,radius=8){
    if(!puzzle||puzzle.kind!=='elements'||record().solved)return;
    for(const o of puzzle.objects){const d=norm(o.x-origin.x,o.y-origin.y);if(dist(o,origin)<radius&&(radius<=3||d.x*dir.x+d.y*dir.y>.72))applyElement(element,o);}
  }
  const oldInteract=interact;interact=function(){
    if(puzzle&&allowed()&&carrying!==null){
      const i=carrying,o=puzzle.objects.find(o=>o.action==='crate'&&o.index===i);record().crates[i]={x:clamp(game.player.x,1,17),y:clamp(game.player.y,1,13)};Object.assign(o,record().crates[i]);carrying=null;
      if(puzzle.plates.every(p=>record().crates.some(c=>dist(c,p)<.65)))solve();else saveGame();return;
    }
    const o=currentInteraction();if(o?.type==='challengeObject')return activate(o);return oldInteract();
  };
  const oldLoad=loadRoom;loadRoom=function(){const result=oldLoad();game.interactables=game.interactables.filter(o=>o.type!=='challengeObject');setup();return result;};
  const oldCast=castSpell;castSpell=function(slot){
    const p=game.player,id=p?.activeSpells[slot],before=p?.spellState[id]?.cd||0,result=oldCast(slot);
    if(id&&(p.spellState[id]?.cd||0)>before&&allowed())elementalCast(AWCombatAffinity.spellElement(id),p,spellAim(),id==='frostnova'?3:8);
    return result;
  };
  const oldDamage=damagePlayer;damagePlayer=function(...args){const hp=game.player?.hp,result=oldDamage(...args);if(room===game.roomData&&game.player?.hp<hp)hitTaken=true;return result;};
  const oldEffects=updateEnemyEffects;
  updateEnemyEffects=function(dt){
    const from=guardian&&{x:guardian.x,y:guardian.y};const result=oldEffects(dt);
    if(!allowed()||!puzzle||record().solved)return result;
    if(from&&guardian&&!guardian.dead&&guardian.combatAbility&&['charge','hunterDash'].includes(guardian.combatAbility.ability)&&guardian.combatAbility.age>=guardian.combatAbility.wind){
      const o=puzzle.pillars.find(o=>!o.broken&&B.distanceToSegment(o,from,guardian)<o.r+guardian.r);
      if(o){o.broken=true;B.cancel(guardian);guardian.stun=1.4;guardian.attack=1.8;burst(o.x,o.y,'#e8c495',14,1);toastMsg(puzzle.pillars.every(o=>o.broken)?'Armor gone! Defeat the guardian.':'Pillar broken • bait another charge.');}
    }
    if(guardian?.dead&&puzzle.pillars.every(o=>o.broken))solve();
    return result;
  };
  const oldUpdate=update;
  update=function(dt){
    const before=game.roomData;oldUpdate(dt);if(!allowed()||before!==game.roomData||!puzzle||record().solved)return;
    memoryTime=Math.max(0,memoryTime-dt);
    if(timer>0){timer-=dt;if(timer<=0){timer=0;mistake();toastMsg('Time expired. Reset switches and try a faster route.');}}
    if(carrying!==null){const o=puzzle.objects.find(o=>o.action==='crate'&&o.index===carrying);if(o){o.x=game.player.x;o.y=game.player.y;}}
  };
  const oldDraw=drawInteractable;
  drawInteractable=function(o){
    if(o.type!=='challengeObject')return oldDraw(o);
    if(!puzzle)return;const r=record(),p=worldToScreen(o.x,o.y,10);
    let color=r.solved?'#a3edaf':'#e6d7a6';
    if(o.action==='seal')color=AWCombatAffinity.colors[o.element];
    if(o.action==='rune'&&puzzle.kind==='memory'&&memoryTime>0){const i=Math.floor((puzzle.order.length*.9+.3-memoryTime)/.9);color=i>=0&&puzzle.order[i]===o.index?'#fff4a6':'#706b8e';}
    if(o.action==='pillar'&&o.broken)color='#747069';
    ctx.save();ctx.strokeStyle=color;ctx.fillStyle=color;ctx.textAlign='center';ctx.lineWidth=2;ctx.font='22px system-ui';
    const symbol=o.action==='mirror'?['╱','╲','○','■'][r.rotation[o.index]]:({clue:'📜',rune:'◆',crate:'📦',seal:r.seals[o.index]?'✦':'◇',conduit:'✧',lever:r.seals[o.index]?'●':'○',pillar:o.broken?'·':'▥',trial:'⚔',replay:'↻',receiver:'💎'})[o.action];
    ctx.fillText(symbol,p.x,p.y-12);if(dist(game.player,o)<2||o.action==='rune'){ctx.font='10px system-ui';ctx.fillText(o.label,p.x,p.y-40);}ctx.restore();
  };
  const oldMarks=drawTelegraphs;drawTelegraphs=function(){
    oldMarks();if(room!==game.roomData||!puzzle||record().solved)return;
    if(puzzle.kind==='mirrors')for(const segment of trace().segments){const a=worldToScreen(segment.from.x,segment.from.y,8),b=worldToScreen(segment.to.x,segment.to.y,8);ctx.save();ctx.strokeStyle='#ffeea6';ctx.lineWidth=3;ctx.beginPath();ctx.moveTo(a.x,a.y);ctx.lineTo(b.x,b.y);ctx.stroke();ctx.restore();}
    if(puzzle.plates)for(const p of puzzle.plates)B.drawShape({shape:'circle',...p,r:.6},record().crates.some(c=>dist(c,p)<.65)?'#b5e99c':'#dacb9a',.3);
    if(timer>0){const p=worldToScreen(9,7,90);ctx.save();ctx.font='bold 16px system-ui';ctx.textAlign='center';ctx.fillStyle='#ffe9a3';ctx.fillText(Math.ceil(timer)+'s • '+record().seals.filter(Boolean).length+'/3',p.x,p.y);ctx.restore();}
  };
  AWCampaignUI.openSite=((base)=>function(){if(puzzle&&A.current().type==='puzzle')return panel();return base();})(AWCampaignUI.openSite);
  window.AWChallengeRooms={setup,activate,panel,reset:resetPuzzle,clue,trace,applyElement,elementalCast,state:()=>puzzle,record,solve,guardExposed:()=>!!puzzle?.pillars.length&&puzzle.pillars.every(o=>o.broken),get timer(){return timer;},get memoryTime(){return memoryTime;}};
})();
