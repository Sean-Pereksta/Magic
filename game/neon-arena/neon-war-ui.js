(function(root){
  'use strict';
  const $=id=>document.getElementById(id),key='neonBreachWarSelectionV1';
  let saved={};try{saved=JSON.parse(localStorage.getItem(key)||'{}')}catch{}
  let profile=()=>root.NeonSurvivalProgress?.store?.getCampaign?.()||root.NeonCampaign?.freshProgress();
  function selection(){return {faction:$('warFaction')?.value||'mice',character:$('warCharacter')?.value||'mouse_rifleman',weapon:$('warWeapon')?.value||'burrower',mission:$('warMission')?.value,rule:$('warVictory')?.value||'tickets',budget:Number($('warBudget')?.value||16),base:Number($('warBase')?.value||4),perFlag:Number($('warPerFlag')?.value||1),resume:!!$('warResume')?.checked,livery:!!$('warLivery')?.checked&&Object.keys(profile()?.completed||{}).length>0};}
  function characters(){const f=$('warFaction').value,previous=$('warCharacter').value||saved.character;$('warCharacter').innerHTML=root.NeonFactions.characters.filter(c=>c.faction===f).map(c=>'<option value="'+c.id+'">'+c.name+'</option>').join('');if([...$('warCharacter').options].some(o=>o.value===previous))$('warCharacter').value=previous;}
  function missions(){
    const list=root.NeonCampaign?.catalog($('warFaction').value)||[],previous=$('warMission').value||saved.mission,progress=profile();
    $('warMission').innerHTML=list.map((m,i)=>'<option value="'+m.id+'" '+(root.NeonCampaign.isUnlocked(progress,m.id)?'':'disabled')+'>'+String(i+1).padStart(2,'0')+' · '+m.name+(progress?.completed?.[m.id]?' · '+'★'.repeat(progress.completed[m.id].stars):'')+'</option>').join('');
    if([...$('warMission').options].some(o=>o.value===previous&&!o.disabled))$('warMission').value=previous;
  }
  function persist(){try{localStorage.setItem(key,JSON.stringify(selection()))}catch{}document.dispatchEvent(new CustomEvent('neon-war-selection',{detail:selection()}));}
  function refresh(){
    const mode=$('mode').value,war=['tdm','conquest','campaign'].includes(mode),campaign=mode==='campaign';$('warSetup').hidden=!war;$('campaignSetup').hidden=!campaign;$('conquestSetup').hidden=mode!=='conquest';
    $('characterLocker').hidden=war;
    if(campaign){const mission=root.NeonCampaign.mission($('warMission').value);if(mission){$('warBriefing').textContent=mission.briefing||mission.description||'';$('warMissionPreview').src='./'+mission.thumbnail;$('warMissionPreview').alt=mission.name+' objective route';$('map').value=mission.mapId;}}
    for(const card of $('arenaCards').querySelectorAll('[data-map]')){card.hidden=(mode==='conquest'&&!root.NeonWarMaps.get(card.dataset.map))||(campaign&&card.dataset.map!==$('map').value);}
    $('warPortrait').src='./assets/'+(root.NeonFactions.characters.find(c=>c.id===$('warCharacter').value)?.portrait||'');
    $('warWeaponPreview').src='./assets/'+(root.NeonFactions.weapons.find(w=>w.id===$('warWeapon').value)?.preview||'');
    $('warStory').textContent=$('warFaction').value==='cats'?'CAT DOMINION · Armored formations. Precision, discipline, and contested order.':'MOUSE RESISTANCE · Mobile infantry. Infiltration, ingenuity, and a fight for freedom.';
    $('secondPlayer').disabled=campaign;
    if(campaign)$('secondPlayer').value='off';
    const earned=Object.keys(profile()?.completed||{}).length;$('warLivery').disabled=!earned;$('warMedals').textContent=earned?earned+' campaign medals earned · faction weapon finish unlocked':'Complete a mission to unlock the faction weapon finish.';
  }
  function mount(){
    const section=document.createElement('section');section.id='warSetup';section.className='warSetup';section.hidden=true;
    section.innerHTML='<div class="sectionHeading"><h2>CHOOSE YOUR FACTION</h2><span>CATS × MICE</span></div><p id="warStory"></p><div class="warLoadout"><img id="warPortrait" alt="Selected tactical soldier"><div><label for="warFaction">Faction</label><select id="warFaction"><option value="mice">Mouse Resistance</option><option value="cats">Cat Dominion</option></select><label for="warCharacter">Soldier appearance</label><select id="warCharacter"></select><label for="warWeapon">Starting weapon</label><select id="warWeapon"></select></div><img id="warWeaponPreview" alt="Selected weapon"></div><div id="campaignSetup"><label for="warMission">Campaign mission</label><select id="warMission"></select><p id="warBriefing"></p><label class="warCheck"><input id="warResume" type="checkbox" checked> Resume saved checkpoint when available</label><p class="warHelp">F / pickup button: interact near marked objectives. C: crouch. G: aim down sights. Touch: hold Slide to crouch; hold Reload to aim. Rescue prisoners, then keep them close on the way to extraction.</p></div><div id="conquestSetup" class="warConquestOptions"><label>Victory rule<select id="warVictory"><option value="tickets">250 reinforcement tickets</option><option value="score">300 territory score</option></select></label><label>Active bot limit<select id="warBudget"><option value="8">8 · mobile</option><option value="12">12 · balanced</option><option value="16" selected>16 · large battle</option></select></label><label>Base team roster<select id="warBase"><option>3</option><option selected>4</option><option>5</option><option>6</option></select></label><label>Soldiers per flag<select id="warPerFlag"><option>0</option><option selected>1</option><option>2</option></select></label><p class="warHelp">Connected territory unlocks the next attack. Reinforcements arrive every 15 seconds. Lost flags stop replacements; surviving troops stay in the battle.</p></div>';
    $('advancedOptions').before(section);
    const preview=document.createElement('img');preview.id='warMissionPreview';$('warBriefing').before(preview);
    const rewards=document.createElement('div');rewards.innerHTML='<label class="warCheck"><input id="warLivery" type="checkbox"> Equip campaign weapon finish (cosmetic)</label><p class="warHelp" id="warMedals"></p>';section.appendChild(rewards);$('warLivery').checked=!!saved.livery;
    $('warWeapon').innerHTML=root.NeonFactions.weapons.map(w=>'<option value="'+w.id+'">'+w.name+'</option>').join('');
    for(const [id,value] of [['warFaction',saved.faction||'mice'],['warWeapon',saved.weapon||'burrower'],['warVictory',saved.rule||'tickets'],['warBudget',saved.budget||16],['warBase',saved.base||4],['warPerFlag',saved.perFlag??1]]){if([...$(id).options].some(o=>o.value===String(value)))$(id).value=String(value);}
    characters();missions();section.addEventListener('change',event=>{if(event.target.id==='warFaction'){characters();missions();}refresh();persist();$('map').dispatchEvent(new Event('change',{bubbles:true}));});
  }
  root.NeonWarUI={mount,selection,refresh,refreshProgress:missions,setProfile:fn=>{profile=fn;missions()}};
})(window);
