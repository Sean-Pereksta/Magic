/* Villages, shops and quests. */
function seededNPC(room,salt){return hash2(room.x*41+salt,room.y*53-salt,room.seed+991+salt)}
function buildVillagePeople(room){
 const count=7+Math.min(6,room.villageLevel||1);const roles=['Blacksmith','Merchant','Quest Keeper','Alchemist'];
 for(let i=0;i<count;i++){
   const role=i<roles.length?roles[i]:'Villager',name=VILLAGER_NAMES[Math.floor(seededNPC(room,10+i)*VILLAGER_NAMES.length)],a=seededNPC(room,80+i)*TAU,rad=role==='Villager'?2.2+seededNPC(room,120+i)*3.5:2.8;
   const x=clamp(ROOM_W/2+Math.cos(a)*rad,1.2,ROOM_W-1.2),y=clamp(ROOM_H/2+Math.sin(a)*rad*.8,1.2,ROOM_H-1.2);
   game.interactables.push({type:'npc',npc:true,role,name,label:`${name} • ${role}`,x,y,homeX:x,homeY:y,targetX:x,targetY:y,walkTimer:seededNPC(room,200+i)*2.5,phase:seededNPC(room,250+i)*TAU,color:VILLAGER_COLORS[Math.floor(seededNPC(room,300+i)*VILLAGER_COLORS.length)]});
 }
}
function updateVillageLife(dt){
 if(!game.roomData?.town)return;for(const n of game.interactables){if(!n.npc)continue;n.walkTimer-=dt;if(n.walkTimer<=0){n.walkTimer=1.8+Math.random()*3.5;const roam=n.role==='Villager'?2.2:.8;n.targetX=clamp(n.homeX+rnd(roam,-roam),1.05,ROOM_W-1.05);n.targetY=clamp(n.homeY+rnd(roam,-roam),1.05,ROOM_H-1.05)}const dx=n.targetX-n.x,dy=n.targetY-n.y,l=Math.hypot(dx,dy);if(l>.05){const sp=(n.role==='Villager' ? .7 : .38)*dt;n.x+=dx/l*Math.min(sp,l);n.y+=dy/l*Math.min(sp,l);n.facing=dx>=0?1:-1}}
}
function questForVillage(room){
 const id=`${room.key}:quest`,h=hash2(room.x,room.y,room.seed+6021),tier=room.villageLevel||1;if(h<.27)return {id,type:'kill',title:'Thin the Roads',desc:`Defeat ${10+tier*3} enemies beyond the village.`,target:10+tier*3,progress:0,rewardGold:34+tier*12,rewardMat:'iron',rewardCount:2+tier};
 if(h<.5)return {id,type:'elite',title:'Break the Champions',desc:`Defeat ${2+tier} elite enemies.`,target:2+tier,progress:0,rewardGold:46+tier*14,rewardMat:'dust',rewardCount:2+tier};
 if(h<.74)return {id,type:'clear',title:'Open the Old Roads',desc:`Clear ${2+tier} hostile rooms of Threat ${Math.max(3,room.difficulty)} or higher.`,target:2+tier,minThreat:Math.max(3,room.difficulty),progress:0,rewardGold:42+tier*13,rewardMat:'hide',rewardCount:2+tier};
 const mats=['dust','iron','hide','frost','ember','bone'],material=mats[Math.floor(hash2(room.x,room.y,room.seed+6022)*mats.length)];return {id,type:'gather',material,title:`Gather ${MATERIALS[material].name}`,desc:`Collect ${6+tier*2} ${MATERIALS[material].name}.`,target:6+tier*2,progress:0,rewardGold:38+tier*11,rewardMat:'dust',rewardCount:2+tier};
}
function recordQuestEvent(type,data){
 ensureExpansionState();let changed=false;for(const q of game.quests.active){if(q.claimed||q.ready)continue;let add=0;if(q.type==='kill'&&type==='kill')add=1;else if(q.type==='elite'&&type==='kill'&&data.elite)add=1;else if(q.type==='clear'&&type==='clear'&&data.room.difficulty>=q.minThreat)add=1;else if(q.type==='gather'&&type==='gather'&&data.material===q.material)add=data.count||1;if(add){q.progress=Math.min(q.target,(q.progress||0)+add);q.ready=q.progress>=q.target;changed=true}}
 if(changed)saveExpansion();
}
function acceptQuest(q){ensureExpansionState();if(game.quests.completed.includes(q.id)||game.quests.active.some(a=>a.id===q.id))return toastMsg('That quest is already in your journal.');if(game.quests.active.filter(a=>!a.claimed).length>=3)return toastMsg('Your quest journal is full.');game.quests.active.push({...q});toastMsg(`Quest accepted: ${q.title}`);saveExpansion();renderNPCPanelQuestList()}
function claimQuest(id){const q=game.quests.active.find(x=>x.id===id);if(!q||!q.ready||q.claimed)return;q.claimed=true;game.gold+=q.rewardGold;addMaterial(q.rewardMat,q.rewardCount,true);game.quests.rep++;game.quests.completed.push(q.id);toastMsg(`Quest complete • +${q.rewardGold} gold`);if(Math.random()<.28)dropGearNow('quest');saveGame();closeOverlay('npcPanel')}
function renderNPCPanelQuestList(){const root=$('npcQuestList');if(!root)return;const room=game.roomData,q=questForVillage(room),done=game.quests.completed.includes(q.id),active=game.quests.active.find(a=>a.id===q.id);let html=`<div class="exp-card"><b>${q.title}</b><small>${q.desc}</small>`;if(done)html+='<span class="exp-good">Completed in this village</span>';else if(active)html+=`<span>${active.progress||0}/${active.target}${active.ready?' • Ready to claim':''}</span>${active.ready?`<button class="btn gold" data-quest-claim="${active.id}">Claim ${active.rewardGold} gold</button>`:''}`;else html+=`<span>Reward: 🪙 ${q.rewardGold} + ${MATERIALS[q.rewardMat].icon} ${q.rewardCount}</span><button class="btn" data-quest-accept="${q.id}">Accept Quest</button>`;html+='</div>';
 const others=game.quests.active.filter(a=>a.id!==q.id&&!a.claimed);if(others.length)html+=`<h4>Current journal</h4>${others.map(a=>`<div class="quest-row"><span>${a.title}</span><b>${a.progress||0}/${a.target}${a.ready?' ✓':''}</b>${a.ready?`<button class="btn small gold" data-quest-claim="${a.id}">Claim</button>`:''}</div>`).join('')}`;root.innerHTML=html;root.querySelector(`[data-quest-accept="${q.id}"]`)?.addEventListener('click',()=>acceptQuest(q));root.querySelectorAll('[data-quest-claim]').forEach(b=>b.onclick=()=>claimQuest(b.dataset.questClaim));
}
function spendGold(cost,fn){if(game.gold<cost)return toastMsg('Not enough gold.');game.gold-=cost;fn();saveGame();updateHUD()}
function merchantBuy(action){
 if(action==='materials')return spendGold(24+game.level,()=>{for(let i=0;i<3;i++)addMaterial(Object.keys(MATERIALS)[irnd(Object.keys(MATERIALS).length)],1,true);toastMsg('Material bundle purchased.')});
 if(action==='gear')return spendGold(62+game.level*4,()=>{dropGearNow('merchant');toastMsg('The merchant opens a sealed gear cache.')});
 if(action==='trinket')return spendGold(75+game.level*5,()=>{if(!game.loot)game.loot=makeRandomGear({source:'merchant',forceSlot:'trinket'});setTimeout(openLootOverlay,120)});
}
function alchemistBuy(action){if(action==='heal')return spendGold(15+game.level*2,()=>{healPlayer(game.player.maxHp*.45);toastMsg('Restorative draught consumed.')});if(action==='ward')return spendGold(22+game.level*2,()=>{game.player.shield=Math.max(game.player.shield,24+game.level*2);game.player.shieldTime=6;toastMsg('Runic ward tonic active.')});if(action==='haste')return spendGold(26+game.level*2,()=>{game.player.haste=Math.max(game.player.haste,12);game.player.tailwind=Math.max(game.player.tailwind,12);toastMsg('Quickstep tonic active.')})}
function openNPCPanel(npc){
 $('npcName').textContent=`${npc.name} • ${npc.role}`;const body=$('npcBody');
 if(npc.role==='Blacksmith'){body.innerHTML='<p class="exp-dialogue">“Bring me what the wilds shed. I can turn scraps into something worth naming.”</p><button class="btn gold blockish" id="npcForgeBtn">Open Forge & Crafting</button>';$('npcForgeBtn').onclick=()=>{closeOverlay('npcPanel');openForgePanel()}}
 else if(npc.role==='Merchant'){body.innerHTML=`<p class="exp-dialogue">“Gold travels farther than boots. Pick something useful.”</p><div class="exp-shop"><button class="exp-buy" data-buy="materials"><b>Prospector Bundle</b><span>3 mixed materials</span><strong>🪙 ${24+game.level}</strong></button><button class="exp-buy" data-buy="gear"><b>Sealed Gear Cache</b><span>Rare chance for an affixed item</span><strong>🪙 ${62+game.level*4}</strong></button><button class="exp-buy" data-buy="trinket"><b>Trinket Case</b><span>Always contains a trinket</span><strong>🪙 ${75+game.level*5}</strong></button></div>`;body.querySelectorAll('[data-buy]').forEach(b=>b.onclick=()=>merchantBuy(b.dataset.buy))}
 else if(npc.role==='Alchemist'){body.innerHTML=`<p class="exp-dialogue">“Nothing permanent. Just enough advantage to survive the next bad room.”</p><div class="exp-shop"><button class="exp-buy" data-tonic="heal"><b>Restorative Draught</b><span>Restore 45% health</span><strong>🪙 ${15+game.level*2}</strong></button><button class="exp-buy" data-tonic="ward"><b>Ward Tonic</b><span>Gain a temporary shield</span><strong>🪙 ${22+game.level*2}</strong></button><button class="exp-buy" data-tonic="haste"><b>Quickstep Tonic</b><span>Haste + movement for 12s</span><strong>🪙 ${26+game.level*2}</strong></button></div>`;body.querySelectorAll('[data-tonic]').forEach(b=>b.onclick=()=>alchemistBuy(b.dataset.tonic))}
 else if(npc.role==='Quest Keeper'){body.innerHTML='<p class="exp-dialogue">“The village survives because travelers keep the roads open.”</p><div id="npcQuestList"></div>';renderNPCPanelQuestList()}
 else body.innerHTML=`<p class="exp-dialogue">“${VILLAGER_LINES[irnd(VILLAGER_LINES.length)]}”</p><div class="exp-card"><b>${game.roomData.name}</b><small>Village tier ${game.roomData.villageLevel} • ${game.interactables.filter(n=>n.npc).length} residents moving through town.</small></div>`;
 showOverlay('npcPanel');
}

const CRAFT_RECIPES={
 weapon:{title:'Forge Weapon',slot:'weapon',gold:28,cost:{iron:6,dust:3}},
 armor:{title:'Forge Armor',slot:'armor',gold:28,cost:{iron:5,hide:4,dust:2}},
 trinket:{title:'Forge Trinket',slot:'trinket',gold:36,cost:{dust:4,iron:2,hide:2}}
};
function forgeRecipe(key){const r=CRAFT_RECIPES[key];if(!r)return;if(game.gold<r.gold)return toastMsg('Not enough gold.');if(!spendMaterials(r.cost))return toastMsg('You need more forging materials.');game.gold-=r.gold;game.loot=makeRandomGear({source:'crafted',crafted:true,forceSlot:r.slot});saveGame();updateExpansionHUD();closeOverlay('forgeCraftPanel');setTimeout(openLootOverlay,100)}
function imbueItem(slot){
 const item=slot==='weapon'?game.player.weapon:game.player.armorGear;if(!item)return;const cost={dust:5,ember:slot==='weapon'?2:0,iron:slot==='armor'?3:1};Object.keys(cost).forEach(k=>{if(!cost[k])delete cost[k]});if(item.prefixKey&&item.suffixKey)return toastMsg('That item already carries both affixes.');if(!spendMaterials(cost))return toastMsg('You need more materials to imbue this item.');if(game.gold<32){for(const [k,v] of Object.entries(cost))game.materials[k]+=v;return toastMsg('Not enough gold.')}game.gold-=32;
 const rates=item.prefixKey?{prefix:0,suffix:1}:item.suffixKey?{prefix:1,suffix:0}:{prefix:.55,suffix:.75};if(!item.prefixKey&&Math.random()<rates.prefix){const pool=PREFIXES.filter(x=>x.slots.includes(item.slot)),p=pool[irnd(pool.length)];item.prefixKey=p.key;item.prefixName=p.name;item.prefixText=p.text;p.apply(item)}if(!item.suffixKey&&Math.random()<rates.suffix){const pool=SUFFIXES.filter(x=>x.slots.includes(item.slot)),s=pool[irnd(pool.length)];item.suffixKey=s.key;item.suffixName=s.name;item.suffixText=s.text}item.baseName=item.baseName||item.name;item.name=`${item.prefixName?item.prefixName+' ':''}${item.baseName}${item.suffixName?' '+item.suffixName:''}`;recomputePlayerStats(true);saveGame();renderForgePanel();toastMsg('The item takes a new rune.')
}
function renderForgePanel(){ensureExpansionState();$('forgeMaterials').innerHTML=Object.entries(MATERIALS).map(([k,m])=>`<span>${m.icon} ${m.name} <b>${game.materials[k]||0}</b></span>`).join('');$('forgeRecipes').innerHTML=Object.entries(CRAFT_RECIPES).map(([k,r])=>`<button class="exp-recipe" data-recipe="${k}"><b>${r.title}</b><span>${materialCostText(r.cost)}</span><strong>🪙 ${r.gold}</strong></button>`).join('')+`<div class="exp-card"><b>Imbue equipped gear</b><small>Add a missing prefix or suffix. Not every found item begins with either.</small><div class="exp-inline"><button class="btn secondary" data-imbue="weapon">Weapon • ${materialCostText({dust:5,ember:2,iron:1})} + 🪙32</button><button class="btn secondary" data-imbue="armor">Armor • ${materialCostText({dust:5,iron:3})} + 🪙32</button></div></div>`;$('forgeRecipes').querySelectorAll('[data-recipe]').forEach(b=>b.onclick=()=>forgeRecipe(b.dataset.recipe));$('forgeRecipes').querySelectorAll('[data-imbue]').forEach(b=>b.onclick=()=>imbueItem(b.dataset.imbue))}
function openForgePanel(){renderForgePanel();showOverlay('forgeCraftPanel')}
const _baseOpenTownPanel=openTownPanel;
openTownPanel=function(service){_baseOpenTownPanel(service);$('forgeService').textContent='Open Material Forge';$('forgeService').onclick=()=>{closeOverlay('townPanel');openForgePanel()}};

const _baseLoadRoom=loadRoom;
loadRoom=function(){_baseLoadRoom();augmentRoom(game.roomData);if(game.roomData.town)buildVillagePeople(game.roomData);updateExpansionHUD();updateHUD()};
const _baseInteract=interact;
interact=function(){const o=currentInteraction();if(o?.npc)return openNPCPanel(o);if(o?.type==='forge')return openForgePanel();return _baseInteract()};

/* Save/load wrappers after the core state serializer. */
const _baseSaveGame=saveGame;
saveGame=function(){_baseSaveGame();saveExpansion()};
const _baseLoadGame=loadGame;
loadGame=function(){const ok=_baseLoadGame();if(ok){ensureExpansionState();loadExpansion();recomputePlayerStats(false)}return ok};
const _baseStartNewGame=startNewGame;
startNewGame=function(){localStorage.removeItem(EXPANSION_SAVE_KEY);game.materials=null;game.quests=null;game.expansion=null;_baseStartNewGame();ensureExpansionState();saveExpansion()};
