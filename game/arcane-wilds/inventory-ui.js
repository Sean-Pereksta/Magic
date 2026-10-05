'use strict';
(() => {
  const I=AWInventory,C=AWHomeCore,D=AWCampaignData;
  let root=null,selected=null,filter='All',search='',page=0;
  const PAGE_SIZE=24,types={weapon:'⚔️',armor:'🛡️',trinket:'💍',material:'🧱',seed:'🌱',produce:'🥕',meal:'🍲',furnishing:'🪑'};
  const el=(tag,text,cls)=>{const e=document.createElement(tag);if(text!==undefined)e.textContent=text;if(cls)e.className=cls;return e;};
  const button=(parent,text,fn,cls='btn secondary')=>{const b=el('button',text,cls);b.type='button';b.onclick=fn;parent.append(b);return b;};
  function entries(){
    const out=I.state().items.map(item=>({key:item.id,name:item.name,icon:item.icon||types[item.slot],type:item.slot,rarity:item.rarity||'Common',item,count:1})),h=AWHome.snapshot();
    for(const [id,m]of Object.entries(MATERIALS))if(game.materials[id]>0)out.push({key:'material:'+id,name:m.name,icon:m.icon,type:'material',count:game.materials[id],description:m.desc||'Used at forges and in home blueprints.'});
    for(const [id,c]of Object.entries(C.crops)){
      if(h.seeds[id]>0)out.push({key:'seed:'+id,name:c.name+' '+(c.tree?'Sapling':'Seeds'),icon:c.tree?'🌳':'🌱',type:'seed',count:h.seeds[id],description:`Plant in ${c.tree?'an orchard plot':'a garden bed'} at Hearthglade. ${c.hours} hours of watered growth; harvest ${c.yield} produce.`});
      if(h.produce[id]>0)out.push({key:'produce:'+id,name:c.name,icon:c.tree?'🍎':'🥕',type:'produce',count:h.produce[id],description:`Use in recipes, compost, or sell at a village supplier for ${c.value} gold each.`});
    }
    for(const [id,r]of Object.entries(C.recipes))if(h.meals[id]>0)out.push({key:'meal:'+id,name:r.name,icon:'🍲',type:'meal',count:h.meals[id],description:`Restores ${Math.round(r.heal*100)}% health. Eat from your home or a town supplier.`,meal:id});
    for(const i of h.items){const d=C.items[i.kind];out.push({key:'furnishing:'+i.id,name:d.name,icon:d.icon,type:'furnishing',count:1,description:(i.packed?'Packed · ready to place. ':'Placed at Hearthglade. ')+(C.benefit?.(i.kind,i.level)||''),homeId:i.id});}
    return out;
  }
  function slot(parent,e,caption){
    const b=button(parent,'',()=>select(e.key),'aw-inv-slot');b.dataset.item=e.key;b.style.setProperty('--rarity',rarityColors[e.rarity]||'#87989d');
    b.title=`${e.name} · ${e.rarity||e.type}${e.item?.prefixKey?' · Prefix: '+(e.item.prefixName||e.item.prefixKey):''}${e.item?.suffixKey?' · Suffix: '+(e.item.suffixName||e.item.suffixKey):''}`;
    b.setAttribute('aria-label',b.title);b.setAttribute('aria-pressed',String(selected===e.key));
    b.append(el('span',e.icon,'aw-inv-icon'),el('span',e.name,'aw-inv-name'),el('span',caption||types[e.type]||'','aw-inv-type'));
    if(e.count>1)b.append(el('span',String(e.count),'aw-inv-count'));
    if(e.item?.prefixKey){const p=el('span','✨','aw-inv-prefix');p.title='Prefix: '+(e.item.prefixText||e.item.prefixKey);b.append(p);}
    if(e.item?.suffixKey){const s=el('span','🔮','aw-inv-suffix');s.title='Suffix: '+(e.item.suffixText||e.item.suffixKey);b.append(s);}
    return b;
  }
  function render(parent){
    I.normalize();root=parent;root.replaceChildren();root.dataset.inventoryRoot='true';
    root.append(el('h2','Inventory'),el('p','Select a square to inspect it. Swapping or removing equipment keeps the item in your backpack.','aw-inv-help'));
    const equipped=el('div',undefined,'aw-inv-equipped');equipped.setAttribute('aria-label','Equipped items');root.append(equipped);
    const all=entries();
    for(const [key,label,item]of [['weapon','Weapon',game.player.weapon],['armor','Armor',game.player.armorGear],...game.player.trinkets.map((i,k)=>['trinket'+k,'Trinket '+(k+1),i])]){
      const group=el('div',undefined,'aw-inv-equip-group');group.append(el('small',label));equipped.append(group);
      const e=item&&!item.inventoryEmpty?all.find(e=>e.key===item.id):null;
      if(e)slot(group,e,'Equipped');else {const empty=el('div',undefined,'aw-inv-slot aw-inv-empty');empty.append(el('span',types[key.startsWith('trinket')?'trinket':key],'aw-inv-icon'),el('span','Empty','aw-inv-name'));group.append(empty);}
    }
    const filters=el('div',undefined,'aw-inv-filters'),label=el('label','Show '),selectBox=el('select');
    for(const value of ['All','Weapons','Armor','Trinkets','Materials','Seeds','Produce','Meals','Furnishings']){const o=el('option',value);o.value=value;selectBox.append(o);}selectBox.value=filter;label.append(selectBox);filters.append(label);
    const searchLabel=el('label','Search '),input=el('input');input.type='search';input.placeholder='Name, prefix or suffix';input.value=search;searchLabel.append(input);filters.append(searchLabel);root.append(filters);
    const layout=el('div',undefined,'aw-inv-layout'),backpack=el('section'),grid=el('div',undefined,'aw-inv-grid');grid.id='awInventoryGrid';grid.setAttribute('aria-label','Backpack slots');backpack.append(grid);layout.append(backpack);
    const details=el('section',undefined,'aw-inv-detail');details.id='awInventoryDetail';details.setAttribute('aria-live','polite');layout.append(details);root.append(layout);
    const paging=el('div',undefined,'aw-inv-paging');paging.id='awInventoryPaging';backpack.append(paging);
    selectBox.onchange=()=>{filter=selectBox.value;page=0;paintGrid();};input.oninput=()=>{search=input.value;page=0;paintGrid();};
    paintGrid();detail();
  }
  function paintGrid(){
    if(!root?.isConnected)return;const grid=root.querySelector('#awInventoryGrid'),paging=root.querySelector('#awInventoryPaging');if(!grid)return;
    grid.replaceChildren();paging.replaceChildren();
    const category={Weapons:'weapon',Armor:'armor',Trinkets:'trinket',Materials:'material',Seeds:'seed',Produce:'produce',Meals:'meal',Furnishings:'furnishing'}[filter];
    const list=entries().filter(e=>(!e.item||!I.equippedSlot(e.key))&&(!category||e.type===category)&&[e.name,e.item?.prefixName,e.item?.prefixKey,e.item?.suffixName,e.item?.suffixKey].join(' ').toLowerCase().includes(search.toLowerCase()));
    const pages=Math.max(1,Math.ceil(list.length/PAGE_SIZE));page=Math.min(page,pages-1);
    for(const e of list.slice(page*PAGE_SIZE,(page+1)*PAGE_SIZE))slot(grid,e);
    for(let n=grid.children.length;n<PAGE_SIZE;n++){const empty=el('div','','aw-inv-slot aw-inv-empty');empty.setAttribute('aria-hidden','true');grid.append(empty);}
    const prev=button(paging,'← Previous',()=>{page--;paintGrid();});prev.disabled=page===0;paging.append(el('span',`${list.length} items · Page ${page+1}/${pages}`));const next=button(paging,'Next →',()=>{page++;paintGrid();});next.disabled=page===pages-1;
  }
  function select(id){selected=id;if(!root?.isConnected)return;for(const b of root.querySelectorAll('[data-item]'))b.setAttribute('aria-pressed',String(b.dataset.item===id));detail();}
  function detail(){
    const box=root?.querySelector('#awInventoryDetail');if(!box)return;box.replaceChildren();const e=entries().find(e=>e.key===selected);
    if(!e){box.append(el('h3','Your backpack'),el('p','Pickups stay here until you choose to equip them. Materials and home supplies show their current shared quantities.'),el('p','✨ Prefix · 🔮 Suffix. Inspect an item to read the exact effects.'));return;}
    box.append(el('h3',e.icon+' '+e.name),el('p',`${e.rarity||capitalize(e.type)}${e.count>1?' · '+e.count+' owned':''}`));
    const item=e.item;
    if(item){
      const slot=I.equippedSlot(item.id);box.append(el('p',slot?'Equipped in '+(slot.startsWith('trinket')?'Trinket '+(Number(slot.slice(7))+1):capitalize(slot)):'In backpack'));
      if(item.desc)box.append(el('p',item.desc));
      const stats=item.slot==='weapon'?[['Power',Math.round(item.power*item.damage*(1+(item.forge||0)*.13))],['Attacks / sec',Math.min(6.25,item.attack).toFixed(2)],['Range',item.range.toFixed(1)]]:item.slot==='armor'?[['Health bonus',Math.round(item.hpBonus)],['Health scale',Math.round(item.hp*100)+'%'],['Armor',Math.round(item.armorBonus*100)+'%'],['Movement',Math.round(item.move*100)+'%']]:Object.entries(item.mods||{}).map(([k,v])=>[capitalize(k),(v*100).toFixed(1)+'%']);
      for(const [name,value]of stats)box.append(el('p',name+': '+value,'aw-inv-stat'));
      if(item.spellSlotBonus)box.append(el('p','+'+item.spellSlotBonus+' spell slot(s) while equipped · maximum 5'));
      if(item.prefixKey)box.append(el('p','✨ Prefix — '+(item.prefixName||item.prefixKey)+': '+(item.prefixText||'')));
      if(item.suffixKey)box.append(el('p','🔮 Suffix — '+(item.suffixName||item.suffixKey)+': '+(item.suffixText||'')));
      if(item.origin)box.append(el('p','Picked up: '+item.origin,'aw-inv-help'));
      const update=fn=>{if(fn()){render(root);root.querySelector('#awInventoryDetail button')?.focus();}};
      if(slot)button(box,'Unequip to backpack',()=>update(()=>I.unequip(slot)));
      if(item.slot==='trinket')for(let k=0;k<3;k++)button(box,'Equip in Trinket '+(k+1),()=>update(()=>I.equip(item.id,'trinket'+k)));
      else if(!slot)button(box,'Equip '+capitalize(item.slot),()=>update(()=>I.equip(item.id)));
    }else{
      box.append(el('p',e.description));
      if(e.meal&&(AWHome.atHome()||D.isTown(AWCampaign.current())))button(box,'Eat · Heal '+Math.round(C.recipes[e.meal].heal*100)+'%',async()=>{if(await AWHome.action('eat',{recipe:e.meal}))render(root);});
      if(e.homeId&&AWHome.atHome())button(box,'Manage furnishing',()=>AWHomeUI.open('item',e.homeId));
      else if(['seed','produce','furnishing'].includes(e.type))box.append(el('p','Manage these at Hearthglade or a village home supplier.'));
    }
  }
  function region(parent,n){
    const box=el('section',undefined,'aw-region-pickups'),owned=I.pickups(n.id),rewards=[...(n.rewardItems||[]),...D.continents.filter(c=>c.boss===n.id).map(c=>c.reward)];
    if(!owned.length&&!rewards.length)return;
    box.append(el('h4',owned.length?'Picked up here':'Equipment in this area'));
    for(const i of owned)box.append(el('p',`${i.icon||types[i.slot]} ${i.name} · ${i.rarity} · In Inventory`));
    const unclaimed=rewards.filter(id=>!I.state().claims.includes('region:'+id));
    for(const id of unclaimed){const d=D.items[id];if(d)box.append(el('p',`${types[d.slot]} ${d.name} · ${d.rarity} · ${['boss','ruler','danger','dungeon'].includes(n.type)?'Clear this area to collect':'Explore this area to collect'}`));}
    if(unclaimed.length&&n.id===AWCampaign.current()?.id&&!['boss','ruler','danger','dungeon'].includes(n.type))button(box,'Collect area reward',()=>{if(AWCampaign.claimSite()){box.remove();region(parent,n);}else toastMsg('Complete this area’s objective first.');});
    if(owned.length)button(box,'View in Inventory',()=>{AWCampaignUI.open('Inventory');select(owned.at(-1).id);});
    parent.append(box);
  }
  window.AWInventoryUI={render,select,region};
})();
