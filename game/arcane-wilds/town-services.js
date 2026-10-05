'use strict';
/* Shared source for town actions, world badges and discovered map summaries.
 * Cache static capabilities; quest availability is deliberately evaluated live. */
(() => {
  const D=AWCampaignData,cache=new WeakMap();
  const roles={
    Merchant:{shop:true},Weaponsmith:{shop:'weapon',forge:true,spells:true},
    Armorer:{shop:'armor',forge:true},Blacksmith:{shop:true,forge:true},
    'Relic Dealer':{shop:'relic'},Enchanter:{shop:'relic',forge:true,spells:true},
    Alchemist:{shop:'alchemy'},Arcanist:{spells:true},'Spell Scribe':{spells:true},
    Stablemaster:{mounts:true},'Quest Keeper':{quest:true},Cartographer:{scout:true}
  };
  const marks={shop:['⚖','Merchant','Shop'],weapons:['⚔','Weapons','Shop'],armor:['◇','Armor','Shop'],
    forge:['⚒','Forge','Forge'],alchemy:['⚗','Alchemy','Alchemy'],spells:['✦','Spells','View Spells'],
    supplies:['♧','Home Supplies & Seeds','Shop Supplies'],mounts:['♞','Mounts','View Mounts'],
    quest:['!','Quest','View Quest'],turnin:['?','Quest Turn-in','Turn In Quest'],
    scout:['⌖','Cartographer','Scout Paths'],portal:['◎','Portal','Use Portal'],talk:['…','Villager','Talk']};
  const badge=id=>({id,icon:marks[id][0],label:marks[id][1],action:marks[id][2]});
  const badges=Object.fromEntries(Object.keys(marks).map(id=>[id,badge(id)]));
  function supplierRole(town){
    return ['Merchant','Alchemist','Relic Dealer','Blacksmith','Weaponsmith','Armorer','Enchanter'].find(r=>town.roles.includes(r))||town.roles[0];
  }
  function capabilities(town,role){
    if(!town)return null;
    let byRole=cache.get(town);if(!byRole){byRole=new Map();cache.set(town,byRole);}
    if(byRole.has(role))return byRole.get(role);
    // New town roles can declare actions without changing the presentation layer.
    const spec=town.services?.[role]||roles[role]||{};
    const stock=spec.shop?(town.stock||[]).filter(id=>{const t=D.items[id];return t&&(spec.shop==='weapon'?t.slot==='weapon':spec.shop==='armor'?t.slot==='armor':spec.shop==='alchemy'?['heal','material','provision'].includes(t.kind):spec.shop==='relic'?t.slot==='trinket'||t.kind==='material':true);}):[];
    const result={stock,forge:!!spec.forge,spells:spec.spells?(town.spells||[]).filter(id=>SPELLS[id]):[],mounts:spec.mounts?(town.mounts||[]).filter(id=>D.mounts[id]):[],quest:!!spec.quest,scout:!!spec.scout,supplies:role===supplierRole(town),badges:[]};
    if(result.supplies)result.badges.push(badges.supplies);
    if(stock.length)result.badges.push(badges[spec.shop==='weapon'?'weapons':spec.shop==='armor'?'armor':spec.shop==='alchemy'?'alchemy':'shop']);
    if(result.forge)result.badges.push(badges.forge);
    if(result.spells.length)result.badges.push(badges.spells);
    if(result.mounts.length)result.badges.push(badges.mounts);
    if(result.quest)result.badges.push(badges.quest);
    if(result.scout)result.badges.push(badges.scout);
    byRole.set(role,result);return result;
  }
  function questBadge(){
    const quests=game.quests;
    if(!AWCampaign.state()||!game.roomData)return badges.quest;
    if(quests?.active.some(q=>q.ready&&!q.claimed))return badges.turnin;
    const q=questForVillage(game.roomData);
    if(!quests?.completed.includes(q.id)&&!quests?.active.some(a=>a.id===q.id)&&quests?.active.filter(a=>!a.claimed).length<3)return badges.quest;
    return badges.talk;
  }
  function forNPC(npc){
    const town=D.towns[npc.campaignTown],c=capabilities(town,npc.role);
    if(!c)return [badges[{Blacksmith:'forge',Merchant:'shop',Alchemist:'alchemy','Quest Keeper':'quest'}[npc.role]||'talk']];
    if(c.quest)return [questBadge(),...c.badges.filter(b=>b.id!=='quest')];
    return c.badges.length?c.badges:[badges.talk];
  }
  function forTown(node){
    const town=D.towns[node?.town];if(!town)return [];
    const found=new Map();for(const role of town.roles)for(const b of capabilities(town,role).badges)found.set(b.id,b);
    // Campaign towns always host an activated village portal.
    found.set('portal',badges.portal);return [...found.values()];
  }
  window.AWServices={capabilities,forNPC,forTown,supplierRole,badges};
})();
