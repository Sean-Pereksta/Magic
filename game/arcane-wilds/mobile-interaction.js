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

/* Final crash containment must wrap the completed render/update stack, not an earlier version. */
(function loadArcaneRuntimeStability(){
  if(document.querySelector('script[data-arcane-runtime-stability]'))return;
  const script=document.createElement('script');
  script.src='arcane-wilds/runtime-stability.js';
  script.async=false;
  script.dataset.arcaneRuntimeStability='true';
  document.body.appendChild(script);
})();
