'use strict';

/* Mobile-only contextual interaction button. No combat, spell, HUD, or progression changes. */
(function(){
  const button=$('mobileInteract');
  if(!button)return;

  function interactionLabel(target){
    if(!target)return null;
    if(target.npc||target.type==='npc'){
      if(target.role==='Blacksmith')return '⚒ FORGE';
      if(target.role==='Merchant')return '🛒 SHOP';
      if(target.role==='Alchemist')return '⚗ SHOP';
      if(target.role==='Quest Keeper')return '❕ QUEST';
      return '💬 TALK';
    }
    if(target.type==='chest')return '📦 OPEN';
    if(target.type==='forge')return '⚒ FORGE';
    if(target.type==='well')return '💧 HEAL';
    if(target.type==='seer')return '🔮 SEER';
    return '✦ USE';
  }

  function refreshMobileInteraction(){
    if(!isTouch||!game.player||paused||modalPause){
      button.classList.add('hidden');
      return;
    }
    const target=currentInteraction();
    const label=interactionLabel(target);
    if(!label){
      button.classList.add('hidden');
      return;
    }
    button.classList.remove('hidden');
    button.textContent=label;
    button.title=target.label||label.replace(/^\S+\s*/, '');
    button.setAttribute('aria-label',button.title);
  }

  button.classList.add('hidden');
  Object.assign(button.style,{
    width:'88px',
    height:'48px',
    borderRadius:'16px',
    padding:'0 10px',
    whiteSpace:'nowrap',
    fontSize:'10px'
  });

  const baseUpdateHUD=updateHUD;
  updateHUD=function(){
    baseUpdateHUD();
    refreshMobileInteraction();
  };

  refreshMobileInteraction();
})();

/* Villages are checkpoints: death returns the hero to the most recently entered village. */
(function installVillageCheckpoint(){
  if(window.__arcaneVillageCheckpointLoaded)return;
  window.__arcaneVillageCheckpointLoaded=true;

  const CHECKPOINT_KEY='arcaneWilds:lastVillage:v1';
  const defaultCheckpoint=()=>({x:0,y:0,name:'Sunmere Haven'});

  function validCheckpoint(value){
    return value&&Number.isFinite(value.x)&&Number.isFinite(value.y);
  }

  function readCheckpoint(){
    try{
      const saved=JSON.parse(localStorage.getItem(CHECKPOINT_KEY)||'null');
      if(validCheckpoint(saved))return saved;
    }catch(err){
      console.warn('Arcane Wilds checkpoint read failed',err);
    }
    return defaultCheckpoint();
  }

  function rememberVillage(room){
    if(!room?.town)return;
    const checkpoint={x:room.x,y:room.y,name:room.name||'Village'};
    game.lastVillage=checkpoint;
    try{localStorage.setItem(CHECKPOINT_KEY,JSON.stringify(checkpoint));}
    catch(err){console.warn('Arcane Wilds checkpoint save failed',err);}
  }

  const baseLoadRoom=loadRoom;
  loadRoom=function(){
    baseLoadRoom();
    if(game.roomData?.town)rememberVillage(game.roomData);
  };

  const baseLoadGame=loadGame;
  loadGame=function(){
    const loaded=baseLoadGame();
    if(!loaded)return false;
    const current=getRoomData(game.room.x,game.room.y);
    if(current?.town)rememberVillage(current);
    else game.lastVillage=readCheckpoint();
    return true;
  };

  const baseStartNewGame=startNewGame;
  startNewGame=function(){
    try{localStorage.removeItem(CHECKPOINT_KEY);}catch(err){}
    game.lastVillage=defaultCheckpoint();
    return baseStartNewGame();
  };

  playerDeath=function(){
    paused=true;
    modalPause=true;
    const checkpoint=validCheckpoint(game.lastVillage)?{...game.lastVillage}:readCheckpoint();
    saveGame();
    setTimeout(()=>{
      alert(`Your journey ended at level ${game.level}. ${checkpoint.name||'The last village'} restores you.`);
      game.player.hp=game.player.maxHp;
      game.room={x:checkpoint.x,y:checkpoint.y};
      game.player.x=ROOM_W/2;
      game.player.y=ROOM_H/2;
      modalPause=false;
      paused=false;
      loadRoom();
      saveGame();
      toastMsg(`Returned to ${checkpoint.name||'your last village'}.`);
    },120);
  };

  game.lastVillage=readCheckpoint();
})();

/* Load the static/cached visual layer before the final performance wrappers capture it. */
(function loadArcaneVisualDepth(){
  if(document.querySelector('script[data-arcane-visual-depth]'))return;
  const script=document.createElement('script');
  script.src='arcane-wilds/visual-depth.js';
  script.async=false;
  script.dataset.arcaneVisualDepth='true';
  document.body.appendChild(script);
})();

/* Keep the optimization layer late in the script chain so it sees every cosmetic runtime wrapper. */
(function loadArcanePerformanceGovernor(){
  if(document.querySelector('script[data-arcane-performance]'))return;
  const script=document.createElement('script');
  script.src='arcane-wilds/performance.js';
  script.async=false;
  script.dataset.arcanePerformance='true';
  document.body.appendChild(script);
})();

/* Navigation clarity runs after the optional performance governor and owns final camera/door/spawn presentation. */
(function loadArcaneNavigationClarity(){
  if(document.querySelector('script[data-arcane-navigation-clarity]'))return;
  const script=document.createElement('script');
  script.src='arcane-wilds/navigation-clarity.js';
  script.async=false;
  script.dataset.arcaneNavigationClarity='true';
  document.body.appendChild(script);
})();
