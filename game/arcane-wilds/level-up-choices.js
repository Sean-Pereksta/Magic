/* Arcane Wilds level-up spell drafting.
   Normal level-ups reserve one choice for an equipped spell with a remaining mutation.
   The other two choices keep the normal weighted draft, including known spellbook spells.
   Known but unequipped spells retain their mutations and can be reassigned after evolving. */
function awCurrentSpellLimit(){
  return typeof window.awSpellSlotLimit==='function'?window.awSpellSlotLimit():3;
}

function awActiveSpellIds(){
  if(!game.player)return [];
  return [...new Set((game.player.activeSpells||[]).slice(0,awCurrentSpellLimit()).filter(Boolean))];
}

function awIsSpellActive(id){return awActiveSpellIds().includes(id)}

function awUpgradeableOwnedSpells(){
  return awActiveSpellIds().filter(id=>{
    const current=game.player.upgrades[id]||[];
    return (UPGRADE_POOLS[id]||[]).some(upgrade=>!current.includes(upgrade[0]));
  });
}

function awLevelSpellChoices(free=false){
  const pool=weightedSpellPool(),ids=[];
  if(!free){
    const upgradeable=awUpgradeableOwnedSpells();
    if(upgradeable.length)ids.push(upgradeable[irnd(upgradeable.length)]);
  }
  return ids.concat(sampleUnique(pool.filter(id=>!ids.includes(id)),3-ids.length));
}

function awFinishSpellFlow(){
  modalPause=false;
  saveGame();
  updateHUD();
  if(game.pendingLevelUps>0)setTimeout(openLevelChoice,100);
}

function awEquipSpellInSlot(id,slot){
  const active=game.player.activeSpells||(game.player.activeSpells=[]);
  for(let i=0;i<active.length;i++)if(i!==slot&&active[i]===id)active[i]=null;
  active[slot]=id;
}

function awOpenReassignChoice(id){
  const limit=awCurrentSpellLimit(),root=$('replaceCards'),overlay=$('replaceOverlay');
  const title=overlay.querySelector('h2'),hint=overlay.querySelector('.overlay-head p');
  game.selectedSpell=id;modalPause=true;root.innerHTML='';
  if(title)title.textContent=`Reassign ${SPELLS[id].icon} ${SPELLS[id].name}?`;
  if(hint)hint.textContent='This spell kept every mutation it had. Put it back into an active slot, or keep your current loadout.';
  for(let i=0;i<limit;i++){
    const old=game.player.activeSpells[i],oldSpell=old&&SPELLS[old];
    root.appendChild(choiceCard({
      rarity:SPELLS[id].rarity,
      icon:oldSpell?oldSpell.icon:'＋',
      name:oldSpell?`Replace ${oldSpell.name}`:`Fill empty slot ${i+1}`,
      desc:oldSpell?`Equip ${SPELLS[id].name} in slot ${i+1}. ${oldSpell.name} stays unlocked in your spellbook.`:`Equip ${SPELLS[id].name} in the open slot.`,
      tag:`Slot ${i+1}`
    },()=>{
      awEquipSpellInSlot(id,i);
      overlay.classList.add('hidden');
      toastMsg(`${SPELLS[id].name} equipped in slot ${i+1}.`);
      awFinishSpellFlow();
    }));
  }
  root.appendChild(choiceCard({
    rarity:'Common',icon:'📖',name:'Keep Current Loadout',
    desc:`Leave ${SPELLS[id].name} in the spellbook with its upgrades intact. You can bring it back later.`,
    tag:'No slot change'
  },()=>{overlay.classList.add('hidden');toastMsg(`${SPELLS[id].name} remains in your spellbook.`);awFinishSpellFlow()}));
  overlay.classList.remove('hidden');
}

openLevelChoice=function(free=false){
  if(!game.player||modalPause)return;
  if(!free&&game.pendingLevelUps<=0)return;
  if(!free)game.pendingLevelUps--;
  modalPause=true;
  $('levelTitle').textContent=free?'Arcane Seer':'Level Up • '+game.level;
  const hint=$('levelHint');
  if(hint)hint.textContent=free?'Choose from the Seer’s weighted spell visions.':'One choice is reserved for an equipped spell you can evolve; the other two are drawn normally.';
  const root=$('levelCards');root.innerHTML='';
  const ids=awLevelSpellChoices(free);
  for(const id of ids){
    const s=SPELLS[id],owned=game.player.unlocked.includes(id),active=awIsSpellActive(id);
    const activeIndex=active?game.player.activeSpells.indexOf(id):-1;
    const tag=active?`Evolve active spell • Slot ${activeIndex+1}`:owned?'Known spell • evolve & optionally reassign':'Unlock spell';
    root.appendChild(choiceCard({rarity:s.rarity,icon:s.icon,name:s.name,desc:s.desc,tag,owned},()=>selectSpellChoice(id)));
  }
  $('levelOverlay').classList.remove('hidden');
};

selectSpellChoice=function(id){
  $('levelOverlay').classList.add('hidden');
  if(game.player.unlocked.includes(id)){openUpgradeChoice(id);return}
  game.player.unlocked.push(id);
  const limit=awCurrentSpellLimit();
  let open=-1;
  for(let i=0;i<limit;i++)if(!game.player.activeSpells[i]){open=i;break}
  if(open>=0){
    awEquipSpellInSlot(id,open);
    toastMsg(`${SPELLS[id].name} unlocked in slot ${open+1}.`);
    awFinishSpellFlow();
  }else{
    game.selectedSpell=id;openReplaceChoice(id);
  }
};

openUpgradeChoice=function(id){
  game.selectedSpell=id;modalPause=true;
  $('upgradeTitle').textContent=`Evolve ${SPELLS[id].icon} ${SPELLS[id].name}`;
  const current=game.player.upgrades[id]||[],pool=(UPGRADE_POOLS[id]||[]).filter(u=>!current.includes(u[0]));
  const pick=sampleUnique(pool.map(u=>u[0]),3),root=$('upgradeCards');root.innerHTML='';
  const finish=(message)=>{
    $('upgradeOverlay').classList.add('hidden');
    toastMsg(message);saveGame();updateHUD();
    if(!awIsSpellActive(id)){setTimeout(()=>awOpenReassignChoice(id),60);return}
    awFinishSpellFlow();
  };
  for(const uid of pick){
    const u=UPGRADE_POOLS[id].find(v=>v[0]===uid);
    root.appendChild(choiceCard({rarity:SPELLS[id].rarity,icon:u[2],name:u[1],desc:u[3],tag:'Behavior mutation'},()=>{
      (game.player.upgrades[id]||(game.player.upgrades[id]=[])).push(uid);
      finish(`${SPELLS[id].name}: ${u[1]}`);
    }));
  }
  if(!pick.length){
    root.appendChild(choiceCard({rarity:SPELLS[id].rarity,icon:'✦',name:'Arcane Mastery',desc:'This spell has every mutation. Gain +12% weapon power instead.',tag:'Mastery'},()=>{
      game.player.weapon.power*=1.12;
      finish(`${SPELLS[id].name} is fully mastered.`);
    }));
  }
  $('upgradeOverlay').classList.remove('hidden');
};

openReplaceChoice=function(id){
  modalPause=true;
  const limit=awCurrentSpellLimit(),overlay=$('replaceOverlay'),root=$('replaceCards');root.innerHTML='';
  const title=overlay.querySelector('h2'),hint=overlay.querySelector('.overlay-head p');
  if(title)title.textContent=`Choose a Spell Slot • ${limit} Active`;
  if(hint)hint.textContent=`${SPELLS[id].name} is unlocked in your spellbook. Choose which active slot it should occupy.`;
  for(let i=0;i<limit;i++){
    const old=game.player.activeSpells[i],s=old&&SPELLS[old];
    root.appendChild(choiceCard({
      rarity:s?.rarity||SPELLS[id].rarity,
      icon:s?.icon||'＋',
      name:s?`Replace ${s.name}`:`Fill empty slot ${i+1}`,
      desc:s?`Put ${SPELLS[id].name} into active slot ${i+1}. ${s.name} remains unlocked with all of its mutations.`:`Put ${SPELLS[id].name} into active slot ${i+1}.`,
      tag:`Slot ${i+1}`
    },()=>{
      awEquipSpellInSlot(id,i);
      overlay.classList.add('hidden');
      toastMsg(`${SPELLS[id].name} equipped.`);
      awFinishSpellFlow();
    }));
  }
  overlay.classList.remove('hidden');
};
