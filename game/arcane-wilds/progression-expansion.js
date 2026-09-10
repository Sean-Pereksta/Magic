'use strict';

/*
 * Arcane Wilds — Spell-slot progression expansion
 * - 4 new spells: one early-game and three late-game abilities.
 * - 8 Rare/Legendary named items that can raise the active spell cap from 3 to 5.
 * - Clickable empty spell slots so known spells can fill newly unlocked slots immediately.
 * - Slightly denser villages plus a broader village-name pool.
 */
(function(){
  if(window.__arcaneWildsProgressionExpansionLoaded)return;
  window.__arcaneWildsProgressionExpansionLoaded=true;
  if(typeof SPELLS==='undefined'||typeof SPELL_CASTS==='undefined'||typeof game==='undefined')return;

  /* ---------- Spells ---------- */
  Object.assign(SPELLS,{
    runicNeedle:{name:'Runic Needle',icon:'🪡',rarity:'Common',desc:'Fire a razor-fast arcane needle that pierces a line of enemies.',cooldown:3.6,damage:28,cast:'runicNeedle'},
    celestialFurnace:{name:'Celestial Furnace',icon:'☀️',rarity:'Epic',desc:'Kindle a ring of miniature suns that flare in sequence before the whole pattern erupts.',cooldown:11.2,damage:42,cast:'celestialFurnace'},
    eventideGate:{name:'Eventide Gate',icon:'🌑',rarity:'Legendary',desc:'Open a vast gravity gate that drags enemies inward, tears at them, then collapses.',cooldown:16.5,damage:34,cast:'eventideGate'},
    seraphicArray:{name:'Seraphic Array',icon:'✴️',rarity:'Legendary',desc:'Call a late-game lattice of radiant judgments that hunts targets across the room.',cooldown:15.2,damage:74,cast:'seraphicArray'}
  });

  Object.assign(UPGRADE_POOLS,{
    runicNeedle:[
      ['runic_needle_triple','Threefold Script','🪡🪡','Fire three needles in a narrow fan.','triple'],
      ['runic_needle_pierce','Unbroken Line','➖','Needles pierce several additional enemies.','pierce'],
      ['runic_needle_burst','Runebreak','💠','Needles burst in a small arcane detonation on impact.','burst'],
      ['runic_needle_seek','Living Glyph','🎯','Needles bend gently toward nearby targets.','seek'],
      ['runic_needle_echo','Second Inscription','🔁','A weaker second volley repeats shortly after the first.','echo']
    ],
    celestialFurnace:[
      ['celestial_furnace_more','Five Suns','☀️☀️','The furnace forms five miniature suns instead of three.','more'],
      ['celestial_furnace_wide','Expanded Orbit','🪐','Suns form farther apart and their flare radius grows.','wide'],
      ['celestial_furnace_ground','Solar Scars','♨️','Each flare leaves a short-lived burning patch.','ground'],
      ['celestial_furnace_ward','Crown of Noon','🛡️','Casting grants a strong temporary ward.','ward'],
      ['celestial_furnace_finale','Helios Collapse','💥','The final eruption deals substantially more damage.','finale']
    ],
    eventideGate:[
      ['eventide_gate_twins','Twin Horizons','🌑🌑','Open two smaller Eventide Gates around the target area.','twins'],
      ['eventide_gate_pull','Irresistible Horizon','🌀','Greatly strengthen the gate’s pull.','pull'],
      ['eventide_gate_wide','Black Meridian','⭕','Increase the gate radius and collapse radius.','wide'],
      ['eventide_gate_long','Lasting Night','⌛','The gate persists longer and gains extra damage pulses.','long'],
      ['eventide_gate_collapse','Starless End','✦','The final collapse launches void shards in every direction.','collapse']
    ],
    seraphicArray:[
      ['seraphic_array_more','Ninefold Verdict','9️⃣','Call nine judgments instead of six.','more'],
      ['seraphic_array_wide','Broad Sigils','⭕','Each judgment strikes a larger area.','wide'],
      ['seraphic_array_chain','Choir Current','⚡','Judgments arc bonus lightning into nearby enemies.','chain'],
      ['seraphic_array_ward','Radiant Aegis','🛡️','Casting grants a powerful ward while the array resolves.','ward'],
      ['seraphic_array_finale','Final Canticle','🌟','Finish with a large radiant detonation at the aimed point.','finale']
    ]
  });

  function awLater(ms,fn){
    const key=game.roomData?.key;
    setTimeout(()=>{if(running&&game.player&&game.roomData?.key===key)fn()},ms);
  }

  function awCastRunicNeedle(id,s,m,scale=1){
    const d=spellAim(),base=Math.atan2(d.y,d.x),offs=hasUpgrade(id,'triple')?[-.13,0,.13]:[0];
    for(const off of offs){
      const a=base+off;
      magicProjectile({
        vx:Math.cos(a)*11.8,vy:Math.sin(a)*11.8,r:.085,
        damage:s.damage*m.power*scale*(offs.length>1?.76:1),
        color:'#a8c8ff',color2:'#f3f0ff',kind:'arcane',life:1.25,
        pierce:hasUpgrade(id,'pierce')?6:2,seek:hasUpgrade(id,'seek')?1.2:0,
        splash:hasUpgrade(id,'burst')?.46:0,trail:''
      });
    }
    fx('castRing',game.player.x,game.player.y,.22,'#a98cff',{r:.58});
  }

  function castRunicNeedle(id,s,m){
    awCastRunicNeedle(id,s,m,1);
    if(hasUpgrade(id,'echo'))awLater(210,()=>awCastRunicNeedle(id,s,m,.58));
  }

  function castCelestialFurnace(id,s,m){
    const pt=aimPoint(3.8),count=hasUpgrade(id,'more')?5:3,orbit=hasUpgrade(id,'wide')?2.05:1.55,hitR=hasUpgrade(id,'wide')?1.05:.82;
    for(let i=0;i<count;i++){
      const a=-Math.PI/2+i*TAU/count,x=clamp(pt.x+Math.cos(a)*orbit,.55,ROOM_W-.55),y=clamp(pt.y+Math.sin(a)*orbit,.55,ROOM_H-.55);
      telegraph('circle',x,y,hitR,.3+i*.12,'#ffd36d',{pulse:true});
      awLater(300+i*120,()=>{
        radialDamage(x,y,hitR,s.damage*.58*m.power,'fire');
        fx('explosion',x,y,.36,'#ffb347',{r:hitR});burst(x,y,'#ffd36d',9,.85,13);
        if(hasUpgrade(id,'ground'))groundEffect('fire',x,y,hitR*.9,2.8,'#ff8248',s.damage*.12*m.power,.38);
      });
    }
    if(hasUpgrade(id,'ward')){game.player.shield=Math.max(game.player.shield,32+game.level*2.2);game.player.shieldTime=Math.max(game.player.shieldTime||0,5.5)}
    awLater(430+count*120,()=>{
      const finale=hasUpgrade(id,'finale')?1.25:.72;
      radialDamage(pt.x,pt.y,orbit+hitR*.8,s.damage*finale*m.power,'fire');
      fx('explosion',pt.x,pt.y,.58,'#fff0a1',{r:orbit+hitR});burst(pt.x,pt.y,'#ffe59a',26,1.8,18);shake=Math.max(shake,8);
    });
  }

  function castEventideGate(id,s,m){
    const pt=aimPoint(4.5),twins=hasUpgrade(id,'twins')?2:1,r=hasUpgrade(id,'wide')?2.45:1.95,life=hasUpgrade(id,'long')?7.2:5.6;
    const spots=twins===2?[{x:clamp(pt.x-1.45,.8,ROOM_W-.8),y:clamp(pt.y+.65,.8,ROOM_H-.8)},{x:clamp(pt.x+1.45,.8,ROOM_W-.8),y:clamp(pt.y-.65,.8,ROOM_H-.8)}]:[pt];
    for(const spot of spots){
      groundEffect('void',spot.x,spot.y,r,life,'#7b46b5',s.damage*m.power*(twins===2?.72:1),.38,{pull:hasUpgrade(id,'pull')?.95:.58});
      fx('riftOpen',spot.x,spot.y,.7,'#b46cff',{r});burst(spot.x,spot.y,'#b07cff',18,1.0,12);
    }
    awLater(Math.round(life*1000)-80,()=>{
      for(const spot of spots){
        radialDamage(spot.x,spot.y,r*1.06,s.damage*1.65*m.power*(twins===2?.78:1),'soul',.35);
        fx('explosion',spot.x,spot.y,.5,'#bd7aff',{r:r*1.1});
        if(hasUpgrade(id,'collapse'))for(let i=0;i<10;i++){
          const a=i*TAU/10;
          magicProjectile({x:spot.x,y:spot.y,z:12,vx:Math.cos(a)*6.8,vy:Math.sin(a)*6.8,r:.09,damage:s.damage*.3*m.power,color:'#c88cff',kind:'arcane',life:.9,pierce:1,trail:''});
        }
      }
      shake=Math.max(shake,10);
    });
  }

  function castSeraphicArray(id,s,m){
    const pt=aimPoint(5),count=hasUpgrade(id,'more')?9:6,r=hasUpgrade(id,'wide')?1.05:.78;
    if(hasUpgrade(id,'ward')){game.player.shield=Math.max(game.player.shield,42+game.level*2.5);game.player.shieldTime=Math.max(game.player.shieldTime||0,6)}
    for(let i=0;i<count;i++){
      const live=[...game.enemies].filter(e=>!e.dead).sort((a,b)=>dist(a,game.player)-dist(b,game.player));
      const target=live.length?live[i%live.length]:null;
      const angle=i*2.399963229728653;
      const x=target?target.x:clamp(pt.x+Math.cos(angle)*(1.1+(i%3)*.5),.55,ROOM_W-.55),y=target?target.y:clamp(pt.y+Math.sin(angle)*(1.1+(i%3)*.5),.55,ROOM_H-.55);
      telegraph('circle',x,y,r,.32+i*.09,'#fff2ad',{pulse:true});
      awLater(320+i*90,()=>{
        radialDamage(x,y,r,s.damage*.55*m.power,'arcane');
        fx('explosion',x,y,.28,'#fff1a4',{r});burst(x,y,'#fff3bd',8,.6,16);
        if(hasUpgrade(id,'chain')){
          const first=nearestEnemy({x,y},3.4);
          if(first){fx('lightning',x,y,.18,'#e8f6ff',{toX:first.x,toY:first.y,width:2});damageEnemy(first,s.damage*.24*m.power,'lightning')}
        }
      });
    }
    if(hasUpgrade(id,'finale'))awLater(430+count*90,()=>{
      radialDamage(pt.x,pt.y,2.35,s.damage*1.15*m.power,'arcane',.25);
      fx('explosion',pt.x,pt.y,.6,'#fff5c7',{r:2.45});burst(pt.x,pt.y,'#fff6d2',30,1.8,18);shake=Math.max(shake,9);
    });
  }

  Object.assign(SPELL_CASTS,{runicNeedle:castRunicNeedle,celestialFurnace:castCelestialFurnace,eventideGate:castEventideGate,seraphicArray:castSeraphicArray});

  /* ---------- Rare / Legendary spell-slot gear ---------- */
  const AW_SLOT_GEAR=[
    {name:'Runebound Conductor',icon:'🪄',slot:'weapon',minRarity:'Rare',spellSlotBonus:1,type:'staff',damage:.98,attack:1.08,range:9.1,speed:10.4,color:'#8fb8ff',special:'awSlotConductor',desc:'Unlocks one additional active spell slot, up to five.'},
    {name:"Spellthief's Longbow",icon:'🏹',slot:'weapon',minRarity:'Rare',spellSlotBonus:1,type:'bow',damage:.91,attack:1.3,range:10.6,speed:13.4,color:'#b28cff',special:'awSlotLongbow',desc:'Unlocks one additional active spell slot, up to five.'},
    {name:'Twin-Sigil Scepter',icon:'⚡',slot:'weapon',minRarity:'Legendary',spellSlotBonus:2,type:'scepter',damage:1.08,attack:1.16,range:9.4,speed:11.2,color:'#f0c6ff',special:'awTwinSigil',desc:'Unlocks two additional active spell slots, allowing all five at once.'},
    {name:'Riftglass Vestments',icon:'🥋',slot:'armor',minRarity:'Rare',spellSlotBonus:1,hp:1.1,move:1.06,armor:.055,color:'#4c4b83',trim:'#d6c4ff',helm:'veil',aura:'void',special:'awRiftglass',desc:'Unlocks one additional active spell slot, up to five.'},
    {name:'Grand Arcanum Mantle',icon:'🧙',slot:'armor',minRarity:'Legendary',spellSlotBonus:2,hp:1.18,move:1.03,armor:.08,color:'#5b467f',trim:'#ffe4a3',helm:'crown',aura:'sun',special:'awGrandArcanum',desc:'Unlocks two additional active spell slots, allowing all five at once.'},
    {name:'Covenant Prism',icon:'🔷',slot:'trinket',minRarity:'Rare',spellSlotBonus:1,mods:{spell:.06,cdr:.03},special:'awCovenantPrism',desc:'Unlocks one additional active spell slot, plus spell potency and cooldown recovery.'},
    {name:'Fifth-Star Reliquary',icon:'🌟',slot:'trinket',minRarity:'Legendary',spellSlotBonus:2,mods:{spell:.1,cdr:.04,hp:.05},special:'awFifthStar',desc:'Unlocks two additional active spell slots and empowers high-end spell builds.'},
    {name:'Witchroad Signet',icon:'💍',slot:'trinket',minRarity:'Rare',spellSlotBonus:1,mods:{spell:.04,move:.04},special:'awWitchroad',desc:'Unlocks one additional active spell slot and improves spell potency and movement.'}
  ];

  function awEquippedSpellSlotGear(){
    const p=game.player;if(!p)return [];
    return [p.weapon,p.armorGear,...(p.trinkets||[])].filter(i=>i?.spellSlotBonus);
  }

  function awSpellSlotLimit(){
    const bonus=awEquippedSpellSlotGear().reduce((sum,item)=>sum+Math.max(0,Number(item.spellSlotBonus)||0),0);
    return Math.min(5,3+bonus);
  }
  window.awSpellSlotLimit=awSpellSlotLimit;

  function awBuildSlotGear(template,level,rarity,crafted=false){
    let item;
    if(template.slot==='weapon')item=makeWeapon(template,level,rarity);
    else if(template.slot==='armor')item=makeArmor(template,level,rarity);
    else item=makeTrinket(template,level,rarity);
    item.awSpellSlotGear=true;item.desc=template.desc;item.spellSlotBonus=template.spellSlotBonus;item.special=template.special;
    return typeof rollAffixes==='function'?rollAffixes(item,crafted):item;
  }

  if(typeof makeRandomGear==='function'){
    const baseMakeRandomGear=makeRandomGear;
    makeRandomGear=function(options={}){
      const regular=baseMakeRandomGear(options);if(!regular)return regular;
      const rank=rarityRank[regular.rarity]||0;if(rank<(rarityRank.Rare||2))return regular;
      const source=options?.source||'enemy';
      let chance=regular.rarity==='Legendary'?.5:.17;
      if(source==='boss')chance+=.12;else if(source==='quest')chance+=.07;else if(options?.crafted)chance+=.05;
      if(Math.random()>chance)return regular;
      const eligible=AW_SLOT_GEAR.filter(t=>t.slot===regular.slot&&(rarityRank[t.minRarity]||0)<=rank);
      if(!eligible.length)return regular;
      const template=eligible[irnd(eligible.length)],rarity=template.minRarity==='Legendary'?'Legendary':regular.rarity;
      return awBuildSlotGear(template,regular.level||Math.max(1,game.level),rarity,!!options?.crafted);
    };
  }

  /* ---------- Dynamic 3–5 spell loadout ---------- */
  function awAssignSpell(id,slot){
    if(typeof awEquipSpellInSlot==='function')return awEquipSpellInSlot(id,slot);
    const active=game.player.activeSpells||(game.player.activeSpells=[]);
    for(let i=0;i<active.length;i++)if(i!==slot&&active[i]===id)active[i]=null;
    active[slot]=id;
  }

  function awOpenEmptySpellSlot(slot){
    const limit=awSpellSlotLimit();if(slot>=limit)return;
    const active=new Set((game.player.activeSpells||[]).slice(0,limit).filter(Boolean));
    const known=(game.player.unlocked||[]).filter(id=>SPELLS[id]&&!active.has(id));
    if(!known.length){toastMsg('No inactive known spells are available for this slot yet.');return}
    const overlay=$('replaceOverlay'),root=$('replaceCards'),title=overlay.querySelector('h2'),hint=overlay.querySelector('.overlay-head p');
    modalPause=true;root.innerHTML='';
    if(title)title.textContent=`Fill Spell Slot ${slot+1}`;
    if(hint)hint.textContent='Choose any known inactive spell. Its existing mutations are preserved.';
    for(const id of known){
      const s=SPELLS[id],ups=game.player.upgrades[id]||[];
      root.appendChild(choiceCard({rarity:s.rarity,icon:s.icon,name:s.name,desc:s.desc,tag:`${ups.length} mutation${ups.length===1?'':'s'} retained`,owned:true},()=>{
        awAssignSpell(id,slot);overlay.classList.add('hidden');modalPause=false;toastMsg(`${s.name} equipped in slot ${slot+1}.`);saveGame();updateHUD();
      }));
    }
    root.appendChild(choiceCard({rarity:'Common',icon:'↩️',name:'Leave Slot Empty',desc:'Keep this bonus spell slot open for now.',tag:`Slot ${slot+1}`},()=>{overlay.classList.add('hidden');modalPause=false;updateHUD()}));
    overlay.classList.remove('hidden');
  }

  window.awOpenEmptySpellSlot=awOpenEmptySpellSlot;

  renderSpellBar=function(){
    if(!game.player)return;
    const root=$('spells'),limit=awSpellSlotLimit();root.innerHTML='';root.classList.toggle('aw-expanded-spells',limit>3);root.dataset.spellSlots=String(limit);
    for(let i=0;i<limit;i++){
      const id=game.player.activeSpells[i];
      if(!id){
        const b=document.createElement('button');b.className='spell-slot aw-empty-spell';b.innerHTML=`<span class="spell-key">${i+1}</span><span class="spell-icon">＋</span><span class="spell-name">Choose Spell</span>`;b.onclick=()=>awOpenEmptySpellSlot(i);root.appendChild(b);continue;
      }
      const s=SPELLS[id],st=game.player.spellState[id]||(game.player.spellState[id]={cd:0}),max=s.cooldown*spellMods(id).cdr,ratio=max?clamp(st.cd/max,0,1):0,b=document.createElement('button');
      b.className='spell-slot';b.style.borderColor=colorAlpha(rarityColors[s.rarity],.38);b.innerHTML=`<span class="spell-key">${i+1}</span><span class="spell-icon">${s.icon}</span><span class="spell-name">${s.name}</span><i class="cooldown" style="transform:scaleY(${ratio})"></i><span class="cooldown-text">${st.cd>.05?st.cd.toFixed(1):''}</span>`;b.onclick=()=>castSpell(i);root.appendChild(b);
    }
  };

  addEventListener('keydown',e=>{
    if(window.AWInput)return;
    if(e.repeat||e.ctrlKey||e.metaKey||e.altKey)return;
    const tag=e.target?.tagName?.toLowerCase();if(tag==='input'||tag==='textarea'||tag==='select')return;
    const n=Number(e.key);if(n<4||n>5||n>awSpellSlotLimit())return;
    e.preventDefault();castSpell(n-1);
  });

  if(typeof gearCard==='function'){
    const baseGearCard=gearCard;
    gearCard=function(item,label){
      const html=baseGearCard(item,label);if(!item?.spellSlotBonus)return html;
      const text=item.spellSlotBonus>=2?'✦ +2 Active Spell Slots • unlocks slots 4 & 5':'✦ +1 Active Spell Slot • up to 5';
      const line=`<div class="aw-slot-grant">${text}</div>`;
      return html.includes('<div class="stats">')?html.replace('<div class="stats">',line+'<div class="stats">'):html.replace('</div>',line+'</div>');
    };
  }

  if(typeof renderInventory==='function'){
    const baseRenderInventory=renderInventory;
    renderInventory=function(){
      baseRenderInventory();const root=$('inventoryContent');if(!root)return;
      const limit=awSpellSlotLimit(),gear=awEquippedSpellSlotGear();
      root.insertAdjacentHTML('afterbegin',`<div class="aw-slot-summary"><b>✨ Active Spell Capacity: ${limit} / 5</b><span>${limit===3?'Find Rare or Legendary spell-channeling equipment to unlock slots 4 and 5.':`Granted by ${gear.map(i=>i.name).join(' • ')}.`}</span></div>`);
    };
  }

  /* ---------- Slightly more villages ---------- */
  const AW_VILLAGE_NAMES=['Moonwell Refuge','Embercross','Lanternrest','Willowmere','Frostbell','Starfall Rest','Rookhaven','Cinderwatch'];
  if(typeof BIOME_NAMES!=='undefined'&&Array.isArray(BIOME_NAMES.town))for(const name of AW_VILLAGE_NAMES)if(!BIOME_NAMES.town.includes(name))BIOME_NAMES.town.push(name);
  isTownCoord=function(x,y){
    if(x===0&&y===0)return true;
    const d=Math.hypot(x,y);
    return d>2&&hash2(x,y,game.seed+71)>.925;
  };

  const style=document.createElement('style');style.id='awProgressionExpansionStyles';style.textContent=`
    .aw-slot-grant{margin-top:9px;padding:8px 10px;border-radius:10px;border:1px solid rgba(192,144,255,.3);background:rgba(143,91,220,.1);font-size:10px;font-weight:950;color:#e4c9ff}
    .aw-slot-summary{display:flex;flex-direction:column;gap:4px;margin-bottom:14px;padding:12px 14px;border-radius:14px;border:1px solid rgba(176,132,255,.24);background:linear-gradient(135deg,rgba(96,69,153,.16),rgba(255,210,105,.06))}
    .aw-slot-summary b{font-size:13px}.aw-slot-summary span{font-size:10px;color:#aebfd2;line-height:1.45}
    #spells.aw-expanded-spells{max-width:calc(100vw - 24px);gap:6px}
    #spells.aw-expanded-spells .spell-slot{width:72px}
    #spells .aw-empty-spell{border-style:dashed;border-color:rgba(198,158,255,.4);background:linear-gradient(180deg,rgba(80,54,119,.55),rgba(9,15,27,.96))}
    @media(max-width:620px){#spells.aw-expanded-spells{gap:3px;padding:5px;bottom:max(8px,env(safe-area-inset-bottom))}#spells.aw-expanded-spells .spell-slot{width:min(17vw,66px);height:58px;padding:4px}#spells.aw-expanded-spells .spell-icon{font-size:19px}#spells.aw-expanded-spells .spell-name{font-size:8px}}
  `;document.head.appendChild(style);
})();
