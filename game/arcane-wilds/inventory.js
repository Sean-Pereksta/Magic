'use strict';
/* One owned equipment collection. Equipped objects are references into this collection;
 * the existing material, seed and produce stores remain the source of stack quantities. */
(() => {
  const A=AWCampaign,D=AWCampaignData;
  const emptyWeapon=()=>({inventoryEmpty:true,name:'No weapon',icon:'✋',slot:'weapon',type:'unarmed',rarity:'Common',level:1,power:0,damage:1,attack:1,range:0,speed:1,color:'#cbbba1',forge:0});
  const emptyArmor=()=>({inventoryEmpty:true,name:'No armor',icon:'👕',slot:'armor',rarity:'Common',level:1,hpBonus:0,hp:1,move:1,armorBonus:0,color:'#69756d',trim:'#acb6a5',helm:'none'});
  const valid=i=>i&&!i.inventoryEmpty&&['weapon','armor','trinket'].includes(i.slot)&&typeof i.name==='string';
  const uid=()=>globalThis.crypto?.randomUUID?.()||'gear-'+Date.now().toString(36)+'-'+Math.random().toString(36).slice(2);
  function state(){if(!game.inventory||!Array.isArray(game.inventory.items))game.inventory={version:1,items:[],claims:[]};return game.inventory;}
  function normalize(){
    const s=state();s.version=1;s.claims=Array.isArray(s.claims)?[...new Set(s.claims.filter(x=>typeof x==='string'))]:[];
    const owned=new Map();
    for(const i of s.items.filter(valid)){i.id||=uid();if(!owned.has(i.id))owned.set(i.id,i);}
    // Equipment in old saves is authoritative, including forge/imbue changes.
    for(const i of equippedItems().filter(valid)){i.id||=uid();owned.set(i.id,i);}
    s.items=[...owned.values()];return s;
  }
  function equippedSlot(id){const p=game.player;if(p.weapon?.id===id)return 'weapon';if(p.armorGear?.id===id)return 'armor';const k=p.trinkets.findIndex(i=>i?.id===id);return k<0?null:'trinket'+k;}
  function receive(item,{node=A.current()?.id,label=A.current()?.name,claim,quiet=false}={}){
    if(!valid(item))return false;const s=state();s.claims||=[];
    if(claim&&s.claims.includes(claim))return false;
    item.id||=uid();if(s.items.some(i=>i.id===item.id)){if(claim)s.claims.push(claim);return false;}
    item.sourceNode=node||null;item.origin=label||item.origin||'Journey';s.items.push(item);if(claim)s.claims.push(claim);
    if(!quiet)toastMsg(`Picked up ${item.icon||'🎒'} ${item.name} · ${item.rarity} · In Inventory`);
    return true;
  }
  function sourceFor(id){return Object.values(D.nodes).find(n=>n.rewardItems?.includes(id))||D.nodes[D.continents.find(c=>c.reward===id)?.boss];}
  function collectRewards(){
    const s=A.state();if(!s?.rewards?.length)return 0;let count=0;
    s.rewards=s.rewards.filter(id=>{
      if(!D.items[id]?.slot)return true;
      const n=sourceFor(id),claim='region:'+id;
      // Older equipped named rewards already belong to this hero.
      if(state().items.some(i=>i.regionalId===id)&&!state().claims.includes(claim)){state().claims.push(claim);return false;}
      if(state().claims.includes(claim))return false;
      if(receive(A.craftItem(id),{node:n?.id,label:n?.name,claim}))count++;
      return false;
    });return count;
  }
  function collectPending(){const item=game.loot;if(!item)return false;const ok=receive(item);game.loot=null;closeOverlay('lootOverlay');closeOverlay('trinketSlotPanel');saveGame();updateHUD(true);return ok;}
  function restore(){
    normalize();collectRewards();
    if(game.loot)collectPending();
    const shadow=window.AWShadow?.state();if(shadow?.pendingLoot?.length){for(const i of shadow.pendingLoot)receive(i,{node:null,label:i.origin,quiet:true});shadow.pendingLoot=[];}
  }
  function canChange(){return !!game.player&&!AWHome.pending()&&!roomTransition;}
  function afterEquip(){recomputePlayerStats(true);AWInput.clear();saveGame();updateHUD(true);}
  function equip(id,slot){
    if(!canChange())return false;normalize();const item=state().items.find(i=>i.id===id);if(!item)return false;
    if(item.slot==='trinket'){
      if(!/^trinket[0-2]$/.test(slot||''))return false;
      const old=equippedSlot(id);if(old===slot)return true;if(old?.startsWith('trinket'))game.player.trinkets[Number(old.slice(7))]=null;
      game.player.trinkets[Number(slot.slice(7))]=item;
    }else {if(slot&&slot!==item.slot)return false;game.player[item.slot==='weapon'?'weapon':'armorGear']=item;}
    afterEquip();return true;
  }
  function unequip(slot){
    if(!canChange())return false;normalize();
    if(slot==='weapon')game.player.weapon=emptyWeapon();else if(slot==='armor')game.player.armorGear=emptyArmor();
    else if(/^trinket[0-2]$/.test(slot))game.player.trinkets[Number(slot.slice(7))]=null;else return false;
    afterEquip();return true;
  }
  function pickups(node){return state().items.filter(i=>i.sourceNode===node);}
  const save=saveGame;saveGame=function(){if(game.player)normalize();return save();};
  const load=loadGame;loadGame=function(){const ok=load();if(ok)normalize();return ok;};
  const begin=beginWorld;beginWorld=function(){normalize();const result=begin();restore();saveGame();return result;};
  const loadRoomBase=loadRoom;loadRoom=function(){const result=loadRoomBase();normalize();if(collectRewards())saveGame();return result;};
  const clear=markRoomCleared;markRoomCleared=function(){const result=clear();if(collectRewards())saveGame();return result;};
  const claimSite=A.claimSite;A.claimSite=function(){const ok=claimSite();if(ok){collectRewards();saveGame();}return ok;};
  const grant=AWRegionalContent.grantItems;AWRegionalContent.grantItems=function(){grant();collectRewards();saveGame();};
  A.claimReward=()=>{const count=collectRewards();saveGame();return !!count;};
  dropGearNow=function(source='enemy',opts={}){const ok=receive(makeRandomGear({source,...opts}));if(ok)saveGame();return ok;};
  openLootOverlay=collectPending;
  AWModernUI.offerLoot=function(item){const ok=receive(item);if(game.loot===item)game.loot=null;if(ok)saveGame();};
  AWModernUI.inspectLoot=collectPending;
  equipLoot=function(){const item=game.loot;if(!item)return;collectPending();AWCampaignUI.open('Inventory');window.AWInventoryUI?.select(item.id);};
  $('equipLootBtn').onclick=equipLoot;$('leaveLootBtn').onclick=collectPending;
  const attack=autoAttack;autoAttack=function(dt){if(!game.player.weapon?.inventoryEmpty)return attack(dt);};
  const drawWeapon=drawWeaponModel;drawWeaponModel=function(w,f){if(!w?.inventoryEmpty)return drawWeapon(w,f);};
  const imbue=imbueItem;imbueItem=function(slot){if(game.player[slot==='weapon'?'weapon':'armorGear']?.inventoryEmpty)return toastMsg('Equip an item before imbuing it.');return imbue(slot);};
  const improve=$('forgeService').onclick;$('forgeService').onclick=()=>game.player.weapon.inventoryEmpty?toastMsg('Equip a weapon before improving it.'):improve();
  window.AWInventory={state,normalize,receive,collectRewards,collectPending,equip,unequip,equippedSlot,pickups};
})();
