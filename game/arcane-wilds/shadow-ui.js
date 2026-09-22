'use strict';
(() => {
  const S=AWShadow,A=AWCampaign,U=AWCampaignUI,D=AWCampaignData;
  const button=(root,label,fn)=>{const b=document.createElement('button');b.type='button';b.className='btn secondary';b.textContent=label;b.onclick=fn;root.appendChild(b);return b;};
  const text=(root,value,tag='p')=>{const el=document.createElement(tag);el.textContent=value;root.appendChild(el);return el;};
  function journal(root){root.replaceChildren();const s=S.state();text(root,'The Shadow Realms','h2');if(!s?.unlocked){text(root,'Defeat the third continent ruler to open the endless roads.');return;}
    text(root,`Total Glory: ${s.glory.toLocaleString()} • Highest Shadow Depth: ${s.highest}`,'h3');text(root,`${s.title||'An uncharted journey'} • Current expedition: ${s.expedition.toLocaleString()} Glory`);
    text(root,'Glory is permanent. Titles and unlocks never spend it. Discoveries far behind your expedition are archived; their rewards remain banked.');
    for(const [n,title] of [[100,'Shadow Walker'],[600,'Rift Hunter + starlit trail'],[1000,'Shadow Fireball'],[1800,'Voidbreaker + Shadow merchants'],[2400,'Eclipse Lightning'],[5000,'Realm Conqueror + Eclipse Gryphon'],[15000,'The Unending'],[40000,'Lord of Shadows']])text(root,`${s.glory>=n?'✓':'◇'} ${title} · ${n.toLocaleString()} Glory`);
    text(root,`Boss milestones: ${s.milestones.slice(-12).join(', ')||'None yet'}`);text(root,`Activated Shadow waystones: ${s.waystones.join(', ')}`);
    if(A.current()?.shadow)button(root,'Return to Astral Sanctuary',()=>{if(!S.leave())toastMsg('Clear this realm before returning.');});
    else if(A.current()?.id==='gloam-city')button(root,'Begin Shadow Expedition',()=>S.enter());
    if(s.pendingLoot?.length)button(root,`Inspect stored Shadow reward (${s.pendingLoot.length})`,()=>{if(game.loot)return toastMsg('Inspect or leave pending loot first.');game.loot=s.pendingLoot.shift();U.close();openLootOverlay();saveGame();});
  }
  function site(){const n=A.current(),s=S.state(),root=$('npcBody');$('npcName').textContent=n.name;root.replaceChildren();
    text(root,`Shadow Depth ${n.depth} • ${s.glory} permanent Glory`);
    if(n.type==='sanctuary'){text(root,'A quiet refuge between unstable roads.');button(root,'Rest and heal',()=>{healPlayer(game.player.maxHp);saveGame();});button(root,'Activate / use waystone',()=>AWTravel.openWaystone());button(root,'Spellbook',()=>U.open('Spellbook'));button(root,'Return to Astral Sanctuary',()=>S.leave());}
    else if(n.type==='merchant'){if(!s.unlocks.includes('shadowMerchant'))text(root,'The merchant recognizes travelers with 1,800 Glory.');else for(const id of S.gearIds){const cost=450+n.depth*15;button(root,`${D.items[id].name} · ${cost} gold`,()=>{if(game.loot||game.gold<cost)return toastMsg('You need enough gold and an empty pending-loot slot.');game.gold-=cost;game.loot=A.craftItem(id);game.loot.shadowDepth=n.depth;U.close();openLootOverlay();saveGame();});}}
    else{text(root,n.type==='mount'?'A rare Eclipse Gryphon watches the horizon.':n.type==='shrine'?'The stars reveal a corrupted spell variant.':'An unstable vault guards equipment from another world.');const b=button(root,S.bit(n.id,'claimed')?'Discovery claimed':'Claim discovery',()=>{if(S.claim())site();});b.disabled=S.bit(n.id,'claimed');}
    showOverlay('npcPanel');
  }
  const baseInteract=interact;interact=function(){const o=currentInteraction();if(o?.type==='shadowPortal'){const body=$('npcBody');body.replaceChildren();$('npcName').textContent='THE SHADOW REALMS';text(body,'Beyond the third ruler lies an endless dimension. Glory is permanent; enemies and loot grow with depth.');button(body,'Enter the Shadow Realms',()=>S.enter());showOverlay('npcPanel');return;}if(o?.type==='shadowSite')return site();return baseInteract();};
  const baseDraw=drawInteractable;drawInteractable=function(o){if(!['shadowPortal','shadowSite'].includes(o.type))return baseDraw(o);const p=worldToScreen(o.x,o.y);ctx.save();ctx.translate(p.x,p.y);ctx.strokeStyle='#c59bef';ctx.fillStyle='rgba(35,11,58,.88)';ctx.lineWidth=4;ctx.beginPath();ctx.ellipse(0,-35,o.type==='shadowPortal'?34:20,45,0,0,TAU);ctx.fill();ctx.stroke();ctx.lineWidth=1.5;for(let i=0;i<3;i++){ctx.beginPath();ctx.ellipse(0,-35,12+i*6,22+i*6,elapsed*.3+i,0,TAU);ctx.stroke();}if(dist(game.player,o)<3){ctx.fillStyle='#efdfff';ctx.font='bold 11px system-ui';ctx.textAlign='center';ctx.fillText(o.label,0,-92);}ctx.restore();};
  const baseHUD=updateHUD;updateHUD=function(...args){baseHUD(...args);const n=A.current();let counter=document.getElementById('awShadowCounter');if(n?.shadow){if(!counter){counter=document.createElement('div');counter.id='awShadowCounter';$('hud').appendChild(counter);}const s=S.state(),threat=n.depth<20?'Rising':n.depth<50?'Severe':n.depth<100?'Extreme':'Mythic';counter.textContent=`Depth ${n.depth} · ${threat} · Glory ${s.glory} (+${s.expedition})`;counter.hidden=false;}else if(counter)counter.hidden=true;};
  const basePlayer=drawPlayer;drawPlayer=function(p){basePlayer(p);if(!S.state()?.unlocks.includes('trail'))return;const at=worldToScreen(p.x-p.facing.x*.6,p.y-p.facing.y*.6);ctx.save();ctx.fillStyle='#c6a1f1';ctx.globalAlpha=.5;for(let i=0;i<3;i++){ctx.beginPath();ctx.arc(at.x-i*5,at.y+Math.sin(elapsed*3+i)*4,2,0,TAU);ctx.fill();}ctx.restore();};
  const baseFront=drawEffectsFront;drawEffectsFront=function(){baseFront();if(!A.current()?.shadow)return;ctx.save();ctx.fillStyle='rgba(42,10,60,.10)';ctx.fillRect(0,0,W,H);ctx.restore();};
  window.AWShadowUI={journal,site};
})();
