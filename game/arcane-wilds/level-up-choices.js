/* Arcane Wilds level-up spell drafting.
   Normal level-ups reserve exactly one evolution/mastery choice for a currently equipped spell.
   The other choices cannot evolve active spells: they either unlock a new spell or equip/reassign a known inactive spell.
   Known inactive spells keep every mutation they earned when they return to the loadout. */
function awCurrentSpellLimit(){
  return typeof window.awSpellSlotLimit==='function'?window.awSpellSlotLimit():3;
}

function awActiveSpellIds(){
  if(!game.player)return [];
  return [...new Set((game.player.activeSpells||[]).slice(0,awCurrentSpellLimit()).filter(Boolean))];
}

function awActiveSpellSlot(id){
  if(!game.player)return -1;
  const active=game.player.activeSpells||[],limit=awCurrentSpellLimit();
  for(let i=0;i<limit;i++)if(active[i]===id)return i;
  return -1;
}

function awIsSpellActive(id){return awActiveSpellSlot(id)>=0}

function awFirstOpenSpellSlot(){
  if(!game.player)return -1;
  const active=game.player.activeSpells||[],limit=awCurrentSpellLimit();
  for(let i=0;i<limit;i++)if(!active[i])return i;
  return -1;
}

function awUpgradeableOwnedSpells(){
  return awActiveSpellIds().filter(id=>{
    const current=game.player.upgrades[id]||[];
    return (UPGRADE_POOLS[id]||[]).some(upgrade=>!current.includes(upgrade[0]));
  });
}

function awLevelSpellChoices(free=false){
  const pool=weightedSpellPool(),ids=[],activeIds=awActiveSpellIds(),active=new Set(activeIds);
  if(!free&&activeIds.length){
    const upgradeable=awUpgradeableOwnedSpells();
    const candidates=upgradeable.length?upgradeable:activeIds;
    ids.push(candidates[irnd(candidates.length)]);
  }
  const remainingPool=free?pool.filter(id=>!ids.includes(id)):pool.filter(id=>!ids.includes(id)&&!active.has(id));
  return ids.concat(sampleUnique(remainingPool,3-ids.length));
}

function awFinishSpellFlow(){
  modalPause=false;
  saveGame();
  updateHUD();
  if(game.pendingLevelUps>0)setTimeout(openLevelChoice,100);
}

function awEquipSpellInSlot(id,slot){
  const limit=awCurrentSpellLimit();
  if(!game.player||slot<0||slot>=limit)return false;
  const active=game.player.activeSpells||(game.player.activeSpells=[]);
  for(let i=0;i<active.length;i++)if(i!==slot&&active[i]===id)active[i]=null;
  active[slot]=id;
  return true;
}

/*
 * One gate for every level-up/select/upgrade path: an inactive spell must become
 * equipped before the flow can finish or before its upgrade picker can open.
 */
function awEquipSpellForFlow(id,onEquipped=null){
  const activeSlot=awActiveSpellSlot(id);
  if(activeSlot>=0){
    if(typeof onEquipped==='function')onEquipped(activeSlot);
    return true;
  }

  const open=awFirstOpenSpellSlot();
  if(open>=0){
    awEquipSpellInSlot(id,open);
    toastMsg(`${SPELLS[id].name} equipped in slot ${open+1}.`);
    if(typeof onEquipped==='function'){
      saveGame();
      updateHUD();
      onEquipped(open);
    }else awFinishSpellFlow();
    return true;
  }

  openReplaceChoice(id,onEquipped);
  return false;
}

openLevelChoice=function(free=false){
  if(!game.player||modalPause)return;
  if(!free&&game.pendingLevelUps<=0)return;
  if(!free)game.pendingLevelUps--;
  modalPause=true;
  $('levelTitle').textContent=free?'Arcane Seer':'Level Up • '+game.level;
  const hint=$('levelHint');
  if(hint)hint.textContent=free?'Choose a spell vision. Any spell you select will be equipped before this choice finishes.':'Exactly one choice evolves or masters a currently equipped spell; every other selected spell is equipped before the level-up finishes.';
  const root=$('levelCards');root.innerHTML='';
  const ids=awLevelSpellChoices(free),openSlot=awFirstOpenSpellSlot();
  for(const id of ids){
    const s=SPELLS[id],owned=game.player.unlocked.includes(id),active=awIsSpellActive(id);
    const activeIndex=active?awActiveSpellSlot(id):-1;
    const tag=active?`Evolve active spell • Slot ${activeIndex+1}`:
      owned&&openSlot>=0?`Equip known spell • fills Slot ${openSlot+1}`:
      owned?'Reassign known spell • upgrades retained':
      openSlot>=0?`Unlock & equip • fills Slot ${openSlot+1}`:'Unlock spell • choose active slot';
    root.appendChild(choiceCard({rarity:s.rarity,icon:s.icon,name:s.name,desc:s.desc,tag,owned:active},()=>selectSpellChoice(id)));
  }
  $('levelOverlay').classList.remove('hidden');
};

selectSpellChoice=function(id){
  $('levelOverlay').classList.add('hidden');
  if(game.player.unlocked.includes(id)){
    if(awIsSpellActive(id))openUpgradeChoice(id);
    else awEquipSpellForFlow(id);
    return;
  }
  game.player.unlocked.push(id);
  awEquipSpellForFlow(id);
};

openUpgradeChoice=function(id){
  /* Never mutate an inactive spell. Equip it first, using an empty bonus slot when available. */
  if(!awIsSpellActive(id))return awEquipSpellForFlow(id,()=>openUpgradeChoice(id));
  game.selectedSpell=id;modalPause=true;
  $('upgradeTitle').textContent=`Evolve ${SPELLS[id].icon} ${SPELLS[id].name}`;
  const current=game.player.upgrades[id]||[],pool=(UPGRADE_POOLS[id]||[]).filter(u=>!current.includes(u[0]));
  const pick=sampleUnique(pool.map(u=>u[0]),3),root=$('upgradeCards');root.innerHTML='';
  const finish=(message)=>{
    $('upgradeOverlay').classList.add('hidden');
    toastMsg(message);
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

openReplaceChoice=function(id,onEquipped=null){
  modalPause=true;
  const limit=awCurrentSpellLimit(),overlay=$('replaceOverlay'),root=$('replaceCards');root.innerHTML='';
  const title=overlay.querySelector('h2'),hint=overlay.querySelector('.overlay-head p');
  const known=game.player.unlocked.includes(id),upgrades=(game.player.upgrades[id]||[]).length;
  if(title)title.textContent=`Choose a Spell Slot • ${limit} Active`;
  if(hint)hint.textContent=known&&upgrades?`${SPELLS[id].name} keeps all ${upgrades} existing mutation${upgrades===1?'':'s'}. Choose which active slot it should occupy.`:`${SPELLS[id].name} is unlocked in your spellbook. Choose which active slot it should occupy.`;
  for(let i=0;i<limit;i++){
    const old=game.player.activeSpells[i],s=old&&SPELLS[old];
    root.appendChild(choiceCard({
      rarity:s?.rarity||SPELLS[id].rarity,
      icon:s?.icon||'＋',
      name:s?`Replace ${s.name}`:`Fill empty slot ${i+1}`,
      desc:s?`Put ${SPELLS[id].name} into active slot ${i+1}. ${s.name} remains learned with all of its mutations.`:`Put ${SPELLS[id].name} into active slot ${i+1}.`,
      tag:`Slot ${i+1}`
    },()=>{
      if(!awEquipSpellInSlot(id,i))return;
      overlay.classList.add('hidden');
      toastMsg(`${SPELLS[id].name} equipped in slot ${i+1}.`);
      if(typeof onEquipped==='function'){
        saveGame();
        updateHUD();
        onEquipped(i);
      }else awFinishSpellFlow();
    }));
  }
  overlay.classList.remove('hidden');
};
