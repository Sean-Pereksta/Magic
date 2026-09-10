'use strict';
/* Final UI ownership: stable spell nodes, action prompts, compact preferences and comparisons. */
(() => {
  if(window.AWModernUI)return;
  const P=window.AWPresentation,I=window.AWInput;
  const escape=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  let signature='',noticeUntil=0,lastTick=0,previousFocus=null,settingsPaused=false,loot=null;
  function node(tag,id,cls,parent=document.body){const n=document.createElement(tag);if(id)n.id=id;if(cls)n.className=cls;parent.appendChild(n);return n;}
  const notice=node('div','awNotice','aw-notice');notice.setAttribute('role','status');
  const prompt=node('div','awPrompt','aw-prompt hidden');
  const settingsButton=node('button','awSettingsBtn','round-btn',$('buttons'));settingsButton.textContent='⚙';settingsButton.title='Settings';settingsButton.setAttribute('aria-label','Settings');settingsButton.onclick=openSettings;
  const panel=node('div','awSettings','overlay hidden');panel.innerHTML='<div class="panel aw-settings-panel" role="dialog" aria-modal="true" aria-labelledby="awSettingsTitle"><div class="overlay-head"><h2 id="awSettingsTitle">Settings</h2><button class="btn secondary" id="awCloseSettings">Close</button></div><div id="awSettingsFields"></div></div>';
  $('awCloseSettings').onclick=closeSettings;
  function announce(title,subtitle=''){notice.innerHTML=`<strong>${escape(title)}</strong><span>${escape(subtitle)}</span>`;notice.classList.add('show');noticeUntil=performance.now()+1700;}
  function openSettings(){previousFocus=document.activeElement;settingsPaused=paused;paused=true;I.clear();showOverlay('awSettings');refreshSettings();$('awCloseSettings').focus();}
  function closeSettings(){I.cancelBinding();panel.classList.add('hidden');const other=Array.from(document.querySelectorAll('.overlay')).some(n=>n!==panel&&!n.classList.contains('hidden'));modalPause=other;paused=settingsPaused;I.clear();previousFocus?.focus?.();}
  function refreshSettings(){
    const root=$('awSettingsFields');root.innerHTML='';
    const select=(key,label,options)=>{const row=node('label',null,'aw-setting',root);row.append(document.createTextNode(label));const el=node('select',null,null,row);for(const value of options){const option=node('option',null,null,el);option.value=value;option.textContent=capitalize(value);}el.value=P.settings[key];el.onchange=()=>{P.settings[key]=el.value;P.saveSettings();};};
    select('quality','Visual quality',['auto','high','medium','low']);select('particles','Particles',['high','medium','low','off']);select('weather','Weather',['high','medium','low','off']);select('lighting','Lighting',['high','medium','low','off']);
    for(const [key,label,min,max,step] of [['deadzone','Controller deadzone',.05,.4,.01],['aimSensitivity','Aim response',.3,3,.1],['shake','Screen shake',0,1,.05],['music','Ambient music',0,1,.05],['effects','Sound effects',0,1,.05]]){
      const row=node('label',null,'aw-setting',root);row.append(document.createTextNode(label));const value=node('output',null,null,row);value.textContent=P.settings[key].toFixed(2);const el=node('input',null,null,row);el.type='range';el.min=min;el.max=max;el.step=step;el.value=P.settings[key];el.oninput=()=>{P.settings[key]=Number(el.value);value.textContent=Number(el.value).toFixed(2);P.saveSettings();};
    }
    const label=node('label',null,'aw-setting',root);label.append(document.createTextNode('Damage numbers'));const check=node('input',null,null,label);check.type='checkbox';check.checked=P.settings.damageNumbers;check.onchange=()=>{P.settings.damageNumbers=check.checked;P.saveSettings();};
    const fullscreen=node('button',null,'btn secondary',root);fullscreen.textContent='Toggle fullscreen';fullscreen.onclick=async()=>{try{if(document.fullscreenElement)await document.exitFullscreen();else await document.documentElement.requestFullscreen();}catch(_){toastMsg('Fullscreen is unavailable in this browser.');}};
    const details=node('details',null,'aw-remap',root);const summary=node('summary',null,null,details);summary.textContent=I.binding?'Press the new key or controller button…':'Remap controls';details.open=!!I.binding;
    for(const [action,key] of Object.entries(I.mapping())){
      const row=node('div',null,'aw-setting',details);const title=node('span',null,null,row);title.textContent=action.replace(/spell(\d)/,'Spell $1');
      const keyButton=node('button',null,'btn secondary',row);keyButton.textContent=key.replace('Key','').replace('Digit','');keyButton.onclick=()=>{I.bind('keys',action);refreshSettings();};
      if(action in I.padMapping()){const button=node('button',null,'btn secondary',row);button.textContent=`Pad ${I.padMapping()[action]+1}`;button.onclick=()=>{I.bind('buttons',action);refreshSettings();};}
    }
    const reset=node('button',null,'btn secondary',details);reset.textContent='Reset control mappings';reset.onclick=()=>{P.settings.keys={};P.settings.buttons={};I.cancelBinding();P.saveSettings();refreshSettings();};
  }
  panel.addEventListener('keydown',e=>{if(e.key!=='Tab')return;const list=Array.from(panel.querySelectorAll('button,input,select,summary')).filter(n=>n.getClientRects().length);if(!list.length)return;const first=list[0],last=list[list.length-1];if(e.shiftKey&&document.activeElement===first){e.preventDefault();last.focus();}else if(!e.shiftKey&&document.activeElement===last){e.preventDefault();first.focus();}});
  function icon(id){const cast=SPELLS[id]?.cast||id,c=P.colorFor(cast);const shape=/chain|storm|lightning/i.test(cast)?'<path d="M23 3 10 20h10l-4 13L31 14H21Z"/>':/frost|ice|winter/i.test(cast)?'<path d="m20 3 9 15-9 17-9-17Z M20 3v32 M11 18h18"/>':/fire|ember|meteor|phoenix|furnace/i.test(cast)?'<path d="M20 3c4 10 13 14 10 23-3 12-24 8-22-3 1-6 7-10 8-16 3 6 6 9 4-4Z"/>':/thorn|root|briar|poison|rot/i.test(cast)?'<path d="M20 34V13 M20 23C3 22 6 7 6 7c13 1 16 9 14 16Zm0-7C34 18 35 4 35 4 22 4 18 8 20 16Z"/>':/ward|shield|aegis/i.test(cast)?'<path d="M20 3 33 9v11c0 8-13 15-13 15S7 28 7 20V9Z"/>':'<path d="m20 3 4 12 12 5-12 4-4 12-4-12-12-4 12-5Z"/>';
    return `<svg viewBox="0 0 40 40" aria-hidden="true" fill="${c}" fill-opacity=".2" stroke="${c}" stroke-width="2" stroke-linejoin="round">${shape}</svg>`;
  }
  renderSpellBar=function(){
    if(!game.player)return;const p=game.player,root=$('spells'),limit=typeof awSpellSlotLimit==='function'?awSpellSlotLimit():3;
    const next=limit+'|'+p.activeSpells.slice(0,limit).join('|');
    if(signature!==next||root.children.length!==limit){signature=next;root.innerHTML='';root.dataset.spellSlots=limit;root.classList.toggle('aw-expanded-spells',limit>3);
      for(let i=0;i<limit;i++){const id=p.activeSpells[i],s=SPELLS[id],b=node('button',null,'spell-slot',root);b.innerHTML=`<span class="spell-key"></span><span class="spell-icon">${id?icon(id):'+'}</span><span class="spell-name">${escape(s?.name||'Choose Spell')}</span><i class="cooldown"></i><span class="cooldown-text"></span><span class="aw-spell-tier"></span>`;b.onclick=()=>{I.useDevice(isTouch?'touch':'mouse');if(game.player.activeSpells[i])I.press('spell'+(i+1));else if(typeof awOpenEmptySpellSlot==='function')awOpenEmptySpellSlot(i);};b.title=s?.desc||'Equip a known spell';}
    }
    for(let i=0;i<limit;i++){const b=root.children[i],id=p.activeSpells[i],s=SPELLS[id],cd=p.spellState[id]?.cd||0,max=s?s.cooldown*spellMods(id).cdr:1;
      b.querySelector('.spell-key').textContent=I.prompt('spell'+(i+1));b.style.setProperty('--cooldown',`${clamp(cd/max,0,1)*360}deg`);b.querySelector('.cooldown-text').textContent=cd>.05?cd.toFixed(1):'';b.querySelector('.aw-spell-tier').textContent=id?`T${1+(p.upgrades[id]?.length||0)}`:'';b.setAttribute('aria-label',`${s?.name||'Empty slot'} · ${I.prompt('spell'+(i+1))}${cd>0?' · '+cd.toFixed(1)+' seconds':' · Ready'}`);
      const ready=!!id&&cd<=.05;if(ready&&b.dataset.ready==='false')b.animate?.([{filter:'brightness(1.8)'},{filter:'brightness(1)'}],{duration:260});b.dataset.ready=String(ready);b.classList.toggle('aw-unavailable',!!id&&cd>0);
      const st=typeof intensityState==='function'?intensityState():null;const reaction=st?.lastSpell?.time>0&&st.reactionLock<=0&&typeof INTENSITY_REACTIONS!=='undefined'&&INTENSITY_REACTIONS[intensityReactionKey(st.lastSpell.id,id)];b.classList.toggle('aw-reaction-ready',ready&&!!reaction);
    }
  };
  function queued(action){const b=/spell/.test(action)?$('spells').children[Number(action.slice(5))-1]:$('dodgeChip');b?.animate?.([{filter:'brightness(1.5)'},{filter:'brightness(1)'}],{duration:120});}
  let mapSignature='';
  renderMinimap=function(){
    const radius=W<760?2:3,parts=[game.room.x,game.room.y,radius];
    for(let y=-radius;y<=radius;y++)for(let x=-radius;x<=radius;x++){const r=game.rooms[roomKey(game.room.x+x,game.room.y+y)];parts.push(r?.seen?[r.cleared,r.town,r.boss,r.challenge,r.quest,r.portal].join(','):'?');}
    const next=parts.join('|');if(next===mapSignature)return;mapSignature=next;const root=$('minimapGrid');root.innerHTML='';root.style.gridTemplateColumns=`repeat(${radius*2+1},1fr)`;root.style.gridTemplateRows=`repeat(${radius*2+1},1fr)`;
    for(let y=-radius;y<=radius;y++)for(let x=-radius;x<=radius;x++){const r=game.rooms[roomKey(game.room.x+x,game.room.y+y)],c=node('div',null,'map-cell',root);if(r?.seen){c.classList.add('seen');if(r.cleared)c.classList.add('clear');if(r.town)c.classList.add('town');if(r.boss)c.classList.add('boss');c.textContent=r.boss?'◆':r.town?'⌂':r.challenge?'!':r.portal?'◉':r.quest?'✦':'';c.title=r.name+(r.cleared?' · Cleared':'');}else c.title='Unexplored';if(x===0&&y===0)c.classList.add('current');}
  };
  function compare(item,current){
    if(!item)return '';
    const rows=[];function delta(label,next,old,percent=false){if(!Number.isFinite(next)||!Number.isFinite(old))return;const n=next-old;if(Math.abs(n)<.0001)return;rows.push(`<span class="${n>0?'aw-up':'aw-down'}">${n>0?'+':''}${(n*(percent?100:1)).toFixed(percent||label==='Attack frequency'?1:0)}${percent?'%':''} ${label}</span>`);}
    if(item.slot==='weapon'){delta('Power',item.power*item.damage*(1+(item.forge||0)*.13),(current?.power||0)*(current?.damage||1)*(1+(current?.forge||0)*.13));delta('Attack frequency',item.attack,current?.attack||0);delta('Range',item.range,current?.range||0);}
    else if(item.slot==='armor'){delta('Bonus HP',item.hpBonus,current?.hpBonus||0);delta('HP multiplier',item.hp,current?.hp||1,true);delta('Armor',item.armorBonus,current?.armorBonus||0,true);delta('Movement',item.move,current?.move||1,true);}
    else for(const key of new Set([...Object.keys(item.mods||{}),...Object.keys(current?.mods||{})]))delta(capitalize(key),item.mods?.[key]||0,current?.mods?.[key]||0,true);
    const slots=(item.spellSlotBonus||0)-(current?.spellSlotBonus||0);if(slots)rows.unshift(`<span class="aw-special">${slots>0?'+':''}${slots} Spell Slot${Math.abs(slots)>1?'s':''} (maximum 5)</span>`);
    for(const text of [item.prefixText,item.suffixText])if(text)rows.push(`<span class="aw-special">${escape(text)}</span>`);
    return `<div class="aw-comparison">${rows.join('')||'<span>No stat change</span>'}</div>`;
  }
  function decorateComparison(){const item=game.loot;if(!item)return;const root=$('lootCompare');root.querySelectorAll('.aw-comparison').forEach(n=>n.remove());if(item.slot==='trinket'){const choices=root.querySelectorAll('.trinket-compare .gear-card');choices.forEach((n,i)=>n.insertAdjacentHTML('beforeend',compare(item,game.player.trinkets[i])));}else root.insertAdjacentHTML('afterbegin',compare(item,item.slot==='weapon'?game.player.weapon:game.player.armorGear));}
  function offerLoot(item){if(!item)return;loot={item,x:game.player.x+.65,y:game.player.y+.3,room:game.roomData};P.event('loot',{});if(rarityRank[item.rarity]>=3)announce(item.name,item.rarity+' '+(item.type||item.slot));}
  function inspectLoot(){loot=null;openLootOverlay();}
  function groundTarget(){if(loot&&game.loot===loot.item&&Math.hypot(game.player.x-loot.x,game.player.y-loot.y)<1.7)return {type:'groundLoot',x:loot.x,y:loot.y,label:loot.item.name};return null;}
  function drawLoot(){if(!loot||game.loot!==loot.item)return;const e=loot,s=worldToScreen(e.x,e.y),c=rarityColors[e.item.rarity],rank=rarityRank[e.item.rarity]||0;ctx.save();ctx.strokeStyle=c;ctx.fillStyle=c;ctx.lineWidth=rank>=2?3:1.5;ctx.globalAlpha=.6;ctx.beginPath();ctx.moveTo(s.x,s.y);ctx.lineTo(s.x,s.y-24-rank*12);ctx.stroke();ctx.globalAlpha=1;ctx.beginPath();ctx.moveTo(s.x,s.y-10);ctx.lineTo(s.x+6,s.y-5);ctx.lineTo(s.x,s.y);ctx.lineTo(s.x-6,s.y-5);ctx.closePath();ctx.fill();if(dist(e,game.player)<3){ctx.font='600 11px system-ui';ctx.textAlign='center';ctx.strokeStyle='#061019';ctx.lineWidth=4;ctx.strokeText(e.item.name,s.x,s.y-31-rank*12);ctx.fillText(e.item.name,s.x,s.y-31-rank*12);ctx.font='10px system-ui';ctx.fillText(`${e.item.rarity} ${e.item.type||e.item.slot}`,s.x,s.y-17-rank*12);}ctx.restore();}
  function tick(){
    const now=performance.now();if(now-noticeUntil>0)notice.classList.remove('show');if(now-lastTick<65)return;lastTick=now;renderSpellBar();
    if(loot&&game.loot!==loot.item)loot=null;if(game.loot&&!loot&&document.querySelectorAll('.overlay:not(.hidden)').length===0)offerLoot(game.loot);if(loot&&loot.room!==game.roomData){loot.room=game.roomData;loot.x=game.player.x+.65;loot.y=game.player.y+.3;}
    const target=groundTarget()||currentInteraction();prompt.classList.toggle('hidden',!target||paused||modalPause||P.cinematic);if(target)prompt.textContent=`${I.prompt('interact')} — ${target.npc?'Talk to '+target.name:target.label}`;
    const dodge=$('dodgeChip');dodge.classList.add('aw-dodge');dodge.style.setProperty('--cooldown',`${clamp(game.player.dodgeCd/1.25,0,1)*360}deg`);dodge.textContent='↯';dodge.title=`${I.prompt('dodge')} · ${game.player.dodgeCd>0?game.player.dodgeCd.toFixed(1)+'s':'Dodge ready'}`;dodge.setAttribute('aria-label',dodge.title);
  }
  function portrait(npc){
    document.querySelector('.aw-npc-portrait')?.remove();const portrait=document.createElement('canvas');portrait.className='aw-npc-portrait';portrait.width=65;portrait.height=65;portrait.setAttribute('aria-label',npc.name);const pc=portrait.getContext('2d');
    pc.fillStyle=npc.color;pc.beginPath();pc.moveTo(9,65);pc.lineTo(17,37);pc.lineTo(48,37);pc.lineTo(57,65);pc.fill();pc.fillStyle='#e4c1a3';pc.beginPath();pc.arc(32,27,12,0,Math.PI*2);pc.fill();pc.fillStyle='#49382f';pc.beginPath();pc.arc(32,23,13,Math.PI,Math.PI*2);pc.fill();$('npcName').parentNode.prepend(portrait);
  }
  function menuGamepad(buttons,previous){
    const overlay=Array.from(document.querySelectorAll('.overlay')).reverse().find(n=>!n.classList.contains('hidden'));
    if(!overlay)return false;
    const controls=Array.from(overlay.querySelectorAll('button,input,select,summary')).filter(n=>!n.disabled&&n.getClientRects().length);
    const index=controls.indexOf(document.activeElement);
    if(buttons[12]&&!previous[12]||buttons[14]&&!previous[14])controls[(index-1+controls.length)%controls.length]?.focus();
    if(buttons[13]&&!previous[13]||buttons[15]&&!previous[15])controls[(index+1)%controls.length]?.focus();
    if(buttons[0]&&!previous[0]){const target=controls[index<0?0:index];target?.focus();target?.click();}
    if(buttons[1]&&!previous[1]){if(overlay===panel)closeSettings();else{const close=overlay.querySelector('[data-close],[data-exp-close],#resumeBtn');close?.click();}}
    if(buttons[9]&&!previous[9]&&overlay.id==='pauseOverlay')$('resumeBtn').click();
    return true;
  }
  window.AWModernUI={portrait,menuGamepad,tick,announce,queued,openSettings,closeSettings,refreshSettings,compare,decorateComparison,offerLoot,groundTarget,inspectLoot,drawLoot};
  I.install();
})();
