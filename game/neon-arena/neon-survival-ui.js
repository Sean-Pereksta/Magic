/* Graphical selector, loadout, nonblocking unlock notices, and survival results. */
(function(root){
  'use strict';
  const P=root.NeonSurvivalProgress;
  if(!P)return;
  const art=file=>'./assets/survival/ui/'+file;
  const escape=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const time=seconds=>{seconds=Math.floor(Math.max(0,seconds||0));return Math.floor(seconds/60)+':'+String(seconds%60).padStart(2,'0')};
  function create(options={}){
    const store=options.store||P.store;let selected='breach_ring',section=null,results=null,noticeTimer=null,warningTimer=null;
    const notice=document.createElement('div');notice.id='survivalUnlockNotice';notice.className='survivalUnlockNotice';notice.hidden=true;notice.setAttribute('role','status');notice.setAttribute('aria-live','polite');document.body.appendChild(notice);
    const warning=document.createElement('div');warning.id='survivalTacticalWarning';warning.className='survivalTacticalWarning';warning.hidden=true;warning.setAttribute('role','status');warning.setAttribute('aria-live','polite');document.body.appendChild(warning);
    function mount(parent){
      if(section)return section;
      section=document.createElement('section');section.id='survivalArenaSelector';section.className='survivalArenaSelector';section.hidden=true;section.setAttribute('aria-label','Endless Breach survival arenas');parent.appendChild(section);render();return section;
    }
    function render(){
      if(!section)return;
      const state=store.getState();if(!store.isUnlocked(selected))selected='breach_ring';
      // Re-render only on menu entry or a progression change, never every frame.
      const focusId=document.activeElement?.dataset?.survivalMap,focusReward=document.activeElement?.dataset?.survivalRewardKind,detailsOpen=section.querySelector('details')?.open||false;
      section.innerHTML='<div class="survivalRecords"><span><strong>'+state.highestWave+'</strong> BEST WAVE</span><span><strong>'+Math.floor(state.stats.kills).toLocaleString()+'</strong> ALIENS ELIMINATED</span><span><strong>'+state.unlockedMaps.length+'/8</strong> ARENAS</span></div><p class="survivalPromise">Every arena is endless. Clear waves to unlock new worlds for your next run.</p><div class="survivalArenaGrid" role="radiogroup" aria-label="Choose a survival arena">'+P.maps.map((map,index)=>{
        const unlocked=store.isUnlocked(map.id),record=state.arenas[map.id];
        return '<button type="button" class="survivalArena '+(unlocked?'':'locked ')+(selected===map.id?'selected':'')+'" data-survival-map="'+map.id+'" role="radio" aria-checked="'+(selected===map.id)+'" '+(!unlocked?'disabled':'')+' style="--arena-accent:'+map.accent+'" aria-label="'+escape(map.name)+'. '+(unlocked?'Unlocked. Personal best wave '+record.highestWave:'Locked. Clear Wave '+map.unlockWave+' in any arena')+'"><div class="survivalArenaPicture"><img loading="lazy" decoding="async" src="./assets/survival/tilesets/'+map.id+'/map_card.webp" alt="'+escape(map.name)+' environment"><span class="survivalArenaNumber">'+String(index+1).padStart(2,'0')+'</span><span class="survivalArenaLock">'+(unlocked?'AVAILABLE':'CLEAR WAVE '+map.unlockWave)+'</span></div><span class="survivalArenaTitle">'+escape(map.name)+'</span><span class="survivalArenaTag">'+escape(map.tag)+'</span><span class="survivalArenaDescription">'+escape(map.description)+'</span><span class="survivalArenaBest">'+(unlocked?'PERSONAL BEST <b>WAVE '+record.highestWave+'</b>':'UNLOCK IN ANY ARENA <b>WAVE '+map.unlockWave+'</b>')+'</span></button>';
      }).join('')+'</div>'+loadoutHTML(state,detailsOpen);
      section.querySelectorAll('[data-survival-map]').forEach(button=>button.addEventListener('click',()=>selectMap(button.dataset.survivalMap)));
      section.querySelectorAll('[data-survival-reward-kind]').forEach(select=>select.addEventListener('change',()=>{if(store.setSelection(select.dataset.survivalRewardKind,select.value))options.onRewardSelection?.(store.getSelection())}));
      section.querySelector('.survivalArenaGrid').addEventListener('keydown',event=>{
        const directions={ArrowLeft:-1,ArrowRight:1,ArrowUp:-2,ArrowDown:2};if(!(event.key in directions))return;
        const buttons=[...section.querySelectorAll('[data-survival-map]:not([disabled])')],index=buttons.indexOf(document.activeElement);if(index<0)return;event.preventDefault();buttons[(index+directions[event.key]+buttons.length)%buttons.length]?.focus();
      });
      if(focusId)section.querySelector('[data-survival-map="'+focusId+'"]')?.focus();
      else if(focusReward)section.querySelector('[data-survival-reward-kind="'+focusReward+'"]')?.focus();
    }
    function loadoutHTML(state,open){
      return '<details class="survivalLocker" '+(open?'open':'')+'><summary>AWARDS & APPEARANCE <span>'+state.unlockedRewards.length+' REWARDS · '+state.achievements.length+'/'+P.achievements.length+' CHALLENGES</span></summary><div class="survivalLoadout">'+[['finish','Weapon finish'],['armor','Armor appearance'],['title','Survivor title'],['effect','Elimination effect']].map(([kind,label])=>'<label>'+label+'<select data-survival-reward-kind="'+kind+'"><option value="default" '+(state.selected[kind]==='default'?'selected':'')+'>Standard</option>'+P.rewards.filter(r=>r.kind===kind).map(r=>'<option value="'+r.id+'" '+(state.selected[kind]===r.id?'selected':'')+' '+(!state.unlockedRewards.includes(r.id)?'disabled':'')+'>'+escape(r.name)+(state.unlockedRewards.includes(r.id)?'':' — Wave '+r.wave)+'</option>').join('')+'</select></label>').join('')+'</div><div class="survivalAwardGrid">'+[...P.rewards.filter(r=>['badge','trophy'].includes(r.kind)),...P.achievements].map(a=>{
        const unlocked=(a.wave?state.unlockedRewards:state.achievements).includes(a.id);
        return '<div class="survivalAward '+(unlocked?'earned':'')+'"><img src="'+art(a.art)+'" alt="" loading="lazy"><strong>'+escape(a.name)+'</strong><small>'+(a.wave?'Clear Wave '+a.wave:escape(a.description))+'</small><span>'+(unlocked?'EARNED':'LOCKED')+'</span></div>';
      }).join('')+'</div><div class="survivalEquipmentPreview"><h3>TACTICAL SALVAGE</h3><p>Scrap buys temporary equipment during a run. Carry up to three charges of each tool.</p>'+P.equipment.map(e=>'<span class="survivalEquipmentPreviewItem '+(store.isEquipmentUnlocked(e.id)?'available':'')+'"><img src="'+art(e.art)+'" alt=""><b>'+escape(e.name)+'</b><small>'+(store.isEquipmentUnlocked(e.id)?e.cost+' SCRAP':'CLEAR WAVE '+e.unlockWave)+'</small></span>').join('')+'</div></details>';
    }
    function selectMap(id){if(!store.isUnlocked(id))return false;selected=id;render();options.onSelect?.(id);return true}
    function setVisible(show){if(!section)return;section.hidden=!show;if(show)render();else hideNotice()}
    function hideNotice(){clearTimeout(noticeTimer);notice.hidden=true}
    function notifyUnlock(batch){
      if(!batch)return;
      const earned=[...(batch.maps||[]).map(id=>({name:P.maps.find(m=>m.id===id)?.name,sub:'NEW ARENA AVAILABLE',image:'./assets/survival/tilesets/'+id+'/map_card.webp'})),...(batch.rewards||[]).map(id=>{const r=P.rewards.find(r=>r.id===id);return {name:r?.name,sub:'PERMANENT '+(r?.kind||'REWARD').toUpperCase(),image:art(r?.art||'first_breach.svg')}}),...(batch.achievements||[]).map(id=>{const a=P.achievements.find(a=>a.id===id);return {name:a?.name,sub:'CHALLENGE COMPLETED',image:art(a?.art||'first_breach.svg')}})].filter(a=>a.name);
      if(!earned.length)return;
      notice.innerHTML='<div class="survivalUnlockKicker">BREACH MILESTONE · PERMANENT UNLOCKS</div><div class="survivalUnlockItems">'+earned.slice(0,3).map(a=>'<div><img src="'+a.image+'" alt=""><span><small>'+escape(a.sub)+'</small><strong>'+escape(a.name)+'</strong></span></div>').join('')+'</div><p>'+ (earned.length>3?'+'+(earned.length-3)+' more awards. ':'')+'Keep fighting. Your current arena stays endless.</p>';
      notice.hidden=false;clearTimeout(noticeTimer);noticeTimer=setTimeout(hideNotice,8500);
    }
    function warn(message,kind='hazard',duration=3500){warning.textContent=message;warning.dataset.kind=kind;warning.hidden=false;clearTimeout(warningTimer);warningTimer=setTimeout(()=>{warning.hidden=true},duration)}
    function showResults(summary,parent=document.getElementById('scoreboardPanel')){
      if(!summary||!parent)return;
      clearResults();hideNotice();warning.hidden=true;
      const map=P.maps.find(m=>m.id===summary.mapId)||P.maps[0];
      results=document.createElement('section');results.id='survivalResults';results.className='survivalResults';results.setAttribute('aria-label','Survival run results');
      results.innerHTML='<div class="survivalResultsHero" style="--arena-accent:'+map.accent+'"><img src="./assets/survival/tilesets/'+map.id+'/map_card.webp" alt="'+escape(map.name)+'"><span>ENDLESS BREACH</span><h3>'+escape(map.name)+'</h3><strong>'+summary.waves+'<small>WAVES CLEARED</small></strong></div><div class="survivalRunStats"><span><b>'+summary.kills.toLocaleString()+'</b> ELIMINATIONS</span><span><b>'+time(summary.seconds)+'</b> TIME SURVIVED</span><span><b>'+summary.bosses+'</b> BOSSES DEFEATED</span><span><b>'+summary.highestWave+'</b> ARENA RECORD</span></div>'+(summary.records?.length?'<p class="survivalResultRecord">NEW RECORD · '+summary.records.map(escape).join(' · ')+'</p>':'')+resultsAwards(summary)+'<div class="survivalResultsActions"><button type="button" data-survival-action="retry">RETRY '+escape(map.name).toUpperCase()+'</button><button type="button" data-survival-action="maps">MAP SELECTION</button><button type="button" data-survival-action="menu">MAIN MENU</button></div>';
      parent.appendChild(results);
      results.querySelector('[data-survival-action="retry"]').onclick=()=>options.onRetry?.(summary.mapId);
      results.querySelector('[data-survival-action="maps"]').onclick=()=>options.onMapSelection?.();
      results.querySelector('[data-survival-action="menu"]').onclick=()=>options.onMainMenu?.();
      const oldTable=document.getElementById('scoreboardTable');if(oldTable)oldTable.hidden=true;
      const oldActions=parent.querySelector('.overlayButtons');if(oldActions)oldActions.hidden=true;
      results.querySelector('button')?.focus();return results;
    }
    function resultsAwards(summary){
      const entries=[...(summary.earned?.maps||[]).map(id=>{const map=P.maps.find(m=>m.id===id);return {name:map?.name,image:'./assets/survival/tilesets/'+id+'/map_card.webp',type:'ARENA UNLOCKED'}}),...(summary.earned?.rewards||[]).map(id=>{const reward=P.rewards.find(r=>r.id===id);return {name:reward?.name,image:art(reward?.art||'first_breach.svg'),type:'REWARD EARNED'}}),...(summary.earned?.achievements||[]).map(id=>{const achievement=P.achievements.find(a=>a.id===id);return {name:achievement?.name,image:art(achievement?.art||'first_breach.svg'),type:'CHALLENGE COMPLETED'}})].filter(e=>e.name);
      if(!entries.length)return '<p class="survivalNextUnlock">'+nextUnlock(summary.highestWave)+'</p>';
      return '<h4 class="survivalResultsAwardTitle">UNLOCKED THIS RUN</h4><div class="survivalResultsAwards">'+entries.map(entry=>'<div><img src="'+entry.image+'" alt=""><span><small>'+entry.type+'</small><b>'+escape(entry.name)+'</b></span></div>').join('')+'</div>';
    }
    function nextUnlock(){const state=store.getState(),map=P.maps.find(m=>m.unlockWave>state.highestWave);return map?'Clear Wave '+map.unlockWave+' in any arena to unlock '+escape(map.name)+'.':'All arenas unlocked. Keep pushing your personal record.'}
    function clearResults(){results?.remove();results=null;const table=document.getElementById('scoreboardTable');if(table)table.hidden=false;const oldActions=document.querySelector('#scoreboardPanel > .overlayButtons');if(oldActions)oldActions.hidden=false}
    function renderEquipmentShop(parent,runState={}){
      if(!parent)return;
      const selectedEquipment=runState.selected||'shockFence';
      parent.innerHTML=P.equipment.map(e=>{
        const unlocked=store.isEquipmentUnlocked(e.id),count=runState.charges?.[e.id]||0,cooldown=runState.cooldowns?.[e.id]||0;
        return '<div class="survivalEquipmentShopItem"><img src="'+art(e.art)+'" alt=""><b>'+escape(e.name)+' · '+e.cost+' SCRAP</b><small>'+escape(e.description)+'</small><span>'+count+'/3 CHARGES'+(cooldown>0?' · '+Math.ceil(cooldown)+'s COOLDOWN':'')+'</span><div><button type="button" data-equipment-buy="'+e.id+'" '+(!unlocked||count>=3||(runState.scrap||0)<e.cost?'disabled':'')+'>'+(unlocked?'BUY CHARGE':'CLEAR WAVE '+e.unlockWave)+'</button><button type="button" data-equipment-select="'+e.id+'" aria-pressed="'+(e.id===selectedEquipment)+'" '+(!unlocked?'disabled':'')+'>'+(e.id===selectedEquipment?'SELECTED':'SELECT')+'</button></div></div>';
      }).join('');
      parent.querySelectorAll('[data-equipment-buy]').forEach(button=>button.onclick=()=>runState.onBuy?.(button.dataset.equipmentBuy));
      parent.querySelectorAll('[data-equipment-select]').forEach(button=>button.onclick=()=>runState.onSelect?.(button.dataset.equipmentSelect));
    }
    const unsubscribe=store.subscribe(()=>{if(section&&!section.hidden&&section.getClientRects().length)render()});
    return {mount,render,selectMap,get selectedMap(){return selected},setVisible,notifyUnlock,warn,showResults,clearResults,renderEquipmentShop,hideNotice,destroy(){unsubscribe();hideNotice();clearTimeout(warningTimer);section?.remove();notice.remove();warning.remove();clearResults()}};
  }
  root.NeonSurvivalUI={create,formatTime:time};
})(typeof globalThis!=='undefined'?globalThis:this);
