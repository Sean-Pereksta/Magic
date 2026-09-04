/* Arcane Wilds level-up spell drafting.
   Normal level-ups reserve one choice for an owned spell with a remaining mutation.
   Non-level spell rolls (such as the Arcane Seer) keep the existing random draft. */
function awUpgradeableOwnedSpells(){
  return game.player.unlocked.filter(id=>{
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

openLevelChoice=function(free=false){
  if(!game.player||modalPause)return;
  if(!free&&game.pendingLevelUps<=0)return;
  if(!free)game.pendingLevelUps--;
  modalPause=true;
  $('levelTitle').textContent=free?'Arcane Seer':'Level Up • '+game.level;
  const root=$('levelCards');root.innerHTML='';
  const ids=awLevelSpellChoices(free);
  for(const id of ids){
    const s=SPELLS[id],owned=game.player.unlocked.includes(id);
    root.appendChild(choiceCard({rarity:s.rarity,icon:s.icon,name:s.name,desc:s.desc,tag:owned?'Choose to evolve':'Unlock spell',owned},()=>selectSpellChoice(id)));
  }
  $('levelOverlay').classList.remove('hidden');
};
