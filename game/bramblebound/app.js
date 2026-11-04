/* Browser shell for the standalone edition of Bramblebound. */
(() => {
  'use strict';
  const P=window.Progression,E=window.GameEngine,C=window.Catalog,A=window.ItemArt,J=window.Journal,S=window.TrailSound;
  const $=id=>document.getElementById(id);
  const glyphs={
    expand:'M3 9V3h6m6 0h6v6m0 6v6h-6M9 21H3v-6',collapse:'M9 3v6H3m12-6v6h6m0 6h-6v6M9 21v-6H3',
    road:'M4 19h6l4-6H9l3-8h5 M17 4l3 2-3 2',book:'M3 5q5-2 9 1 4-3 9-1v14q-5-2-9 1-4-3-9-1z M12 6v14',
    sprout:'M12 21v-8 M12 16Q3 16 4 7q9-1 8 9z M12 12Q12 3 21 3q1 8-9 9z',seed:'M6 20C1 9 9 5 19 3c2 9-1 18-13 17z M6 20l10-12',
    flag:'M5 21V3m0 1h13l-3 4 3 4H5',heart:'M12 20 4 12C-1 5 8 1 12 7c4-6 13-2 8 5z',
    target:'M12 2v4m0 12v4M2 12h4m12 0h4 M19 12a7 7 0 1 1-14 0 7 7 0 1 1 14 0z',
    swords:'M4 3l5 2 10 12-2 2L5 8z M14 18l5-4m-1 4 3 3M20 3l-5 2-3 4m-3 4-4 4 2 2 4-5 M4 14l5 5m-3-1-3 3',
    pack:'M6 9h12l2 4v7H4v-7zM8 9V6q0-3 4-3t4 3v3M7 13h10v4H7z',
    settings:'M4 6h6m5 0h5M4 12h2m5 0h9M4 18h9m5 0h2 M10 3v6m-4 0v6m7 0v6',
    close:'M5 5l14 14M19 5 5 19',play:'M7 4l13 8-13 8z',pause:'M7 5v14m10-14v14',
    speed:'M3 6l7 6-7 6m9-12 7 6-7 6',audio:'M3 9h4l5-4v14l-5-4H3z M15 9q4 3 0 6m3-10q7 7 0 14',
    mute:'M3 9h4l5-4v14l-5-4H3z M16 9l6 6m0-6-6 6',merge:'M4 4l8 8v8m8-16-8 8m-5 4 5 5 5-5',
    info:'M12 11v6m0-10v.4 M22 12a10 10 0 1 1-20 0 10 10 0 1 1 20 0z',chev:'M9 5l7 7-7 7',swap:'M3 8h16l-4-4m6 12H5l4 4',
    spark:'M12 2l2.5 7.5L22 12l-7.5 2.5L12 22l-2.5-7.5L2 12l7.5-2.5z',
    trash:'M4 7h16M9 7V4h6v3M6 7l1 14h10l1-14M10 10v7m4-7v7',check:'M3 12l6 6L21 5'
  };
  function icon(name){return '<svg aria-hidden="true" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="'+(glyphs[name]||glyphs.spark)+'"/></svg>';}
  function ink(root=document){root.querySelectorAll('i[data-glyph]:empty').forEach(n=>n.innerHTML=icon(n.dataset.glyph));}
  const esc=value=>String(value).replace(/[&<>"']/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
  const number=n=>Number(n).toLocaleString('en-US',{maximumFractionDigits:1});
  const pad=n=>String(n).padStart(2,'0'), lv=n=>E.levelText(n);
  const defs=Object.values(C.items), allItems=r=>[...r.active,...r.pack], byUid=(r,uid)=>allItems(r).find(x=>x.uid===Number(uid));
  const elName=id=>C.elements[id]?.name||id;
  const tile=(id,size=32)=>'<span class="icon-tile" data-rarity="'+C.items[id].rarity+'" style="--el:'+C.elements[C.items[id].element].color+'">'+A.svg(id,size)+'</span>';
  const KEY='bramblebound.profile.v1', defaultPrefs=()=>({audio:false,motion:!window.matchMedia('(prefers-reduced-motion: reduce)').matches});
  function freshKit(){const draft=E.freshProfile();E.start(draft);return draft;}
  let starter=freshKit(),campChanged=false;
  let profile=E.freshProfile(),prefs=defaultPrefs(),storageOK=true,damagedSave=null;
  function readProfile(raw){
    const data=JSON.parse(raw);
    if(!data||data.game!=='bramblebound'||data.format!==1)throw Error('This file is not a Bramblebound profile.');
    const p=E.validate(data.profile),s=defaultPrefs();
    const o=data.settings||{};
    if(typeof o.audio==='boolean')s.audio=o.audio;
    if(typeof o.motion==='boolean')s.motion=o.motion;
    const camp=!p.run&&data.camp?E.validate(data.camp):null;
    if(camp&&(!camp.run||camp.run.phase!=='walk'||camp.run.round!==1||camp.run.time!==0))throw Error('Invalid starting kit.');
    return {profile:p,prefs:s,camp};
  }
  try{const raw=localStorage.getItem(KEY);if(raw){try{const loaded=readProfile(raw);({profile,prefs}=loaded);if(loaded.camp){starter=loaded.camp;campChanged=true;}}catch{damagedSave=raw;}}}catch{storageOK=false;}
  const equipmentProfile=()=>profile.run?profile:starter;
  const displayRun=()=>equipmentProfile().run;
  let titleOpen=true;
  let page='expedition', guideTab='items',guideTier='all',query='',manualPause=!!profile.run&&['walk','fight'].includes(profile.run.phase);
  let effects=[],defeated=null,sceneClock=0, demoDistance=0,previousTime=null,hudTimer=0,saveTimer=0,structure='',toastTimer,pendingImport=null,encounterAge=1;
  let packPage=0,packQuery='',packTier='all',displayedDraft='',packPages=1;
  let selectedUid=null,partnerView=null,debriefedRun=null;
  const partners=new Map();
  const compactPack=window.matchMedia('(orientation:portrait), (max-height:680px)');
  const pageSize=()=>compactPack.matches?8:12;
  const modal=$('modal'); let dialogState=null,preDialogFocus=null;
  function serialize(){return JSON.stringify({game:'bramblebound',format:1,profile,settings:prefs,...(!profile.run&&campChanged?{camp:starter}:{})});}
  function save(){
    if(damagedSave)return;
    try{localStorage.setItem(KEY,serialize());storageOK=true;}catch{storageOK=false;}
    $('save-line').innerHTML='<b class="save-dot"></b>'+ (storageOK?'Saved on this device':'Local save unavailable · export in Profile');
  }
  function notify(message){const el=$('toast');el.textContent=message;el.classList.add('show');clearTimeout(toastTimer);toastTimer=setTimeout(()=>el.classList.remove('show'),2800);}
  function sound(kind,element){S?.play(kind,element);}
  function paused(){return titleOpen||manualPause||!!profile.run?.draft.length||page!=='expedition'||modal.open||document.hidden;}
  function titleMenu(open){
    titleOpen=open;$('title-screen').hidden=!open;document.body.classList.toggle('title-open',open);
    $('main').inert=open;document.querySelector('.mast').inert=open;previousTime=null;
    if(open){
      save();
      const alive=profile.run&&profile.run.phase!=='dead';
      $('title-play').textContent=alive?'Continue expedition →':'Begin expedition →';
      $('title-kit').textContent=alive?'Inspect your kit':'Prepare your kit';
      $('title-record').textContent=alive?'Round '+profile.run.round+' · '+C.regions[profile.run.region].name:profile.deaths?'Best round '+profile.best+' · '+profile.points+' trail seeds':'Your journey begins here.';
      $('title-play').focus({preventScroll:true});
    }
  }
  function enterFromTitle(prepare=false){
    if(damagedSave)return showDialog('recovery');
    titleMenu(false);closeDialog();
    if(prepare){manualPause=true;navigate('expedition');}
    else if(!profile.run||profile.run.phase==='dead')start();
    else{manualPause=false;navigate('expedition');}
    $('run-btn').focus({preventScroll:true});
  }
  function runCommand(name,...args){const owner=['equip','stow','swap','combine','discard'].includes(name)?equipmentProfile():profile;const res=E.act(owner,name,...args);if(!res.ok){notify(res.message);return false;}if(owner===starter)campChanged=true;save();structure='';return true;}
  function navigate(next){
    if(!['expedition','guide','legacy'].includes(next))return;
    closeDialog();page=next;document.body.classList.toggle('on-expedition',page==='expedition');document.querySelectorAll('.page').forEach(p=>p.hidden=p.id!==next+'-page');
    document.querySelectorAll('nav [data-nav]').forEach(b=>{const active=b.dataset.nav===page;b.classList.toggle('current',active);if(active)b.setAttribute('aria-current','page');else b.removeAttribute('aria-current');});
    if(next==='guide')renderGuide();else if(next==='legacy')renderLegacy();else update(true);
    window.scrollTo(0,0);
  }
  function renderGuide(){const pane=$('guide-page');pane.innerHTML=J.guide(profile,guideTab,query,guideTier);ink(pane);J.paintPortraits(pane);}
  function renderLegacy(){const pane=$('legacy-page');pane.innerHTML=J.legacy(profile);ink(pane);}
  function start(){
    const ready=profile.run===null;
    const result=E.start(profile);if(!result.ok)return notify(result.message);
    if(ready){
      const r=profile.run,loadout=starter.run;
      r.active=loadout.active.map(it=>({...it}));r.pack=loadout.pack.map(it=>({...it}));r.nextUid=loadout.nextUid;
      for(const id of starter.seenItems)if(!profile.seenItems.includes(id))profile.seenItems.push(id);
    }
    resetPack();displayedDraft='';manualPause=false;effects=[];defeated=null;lastHeld=null;heldFor=0;encounterAge=1;structure='';closeDialog();save();sound('start');navigate('expedition');
  }
  function onRunButton(){if(!profile.run)return start();if(profile.run.phase==='dead')return showDialog('debrief');if(profile.run.draft.length)return showDialog('draft');manualPause=!manualPause;structure='';update(true);save();}
  function exitPartnerView(){
    if(!partnerView)return;
    packQuery=partnerView.query;packPage=partnerView.page;packTier=partnerView.tier;$('pack-search').value=packQuery;$('pack-tier').value=packTier;partnerView=null;
  }
  function clearSelection(){
    exitPartnerView();selectedUid=null;partners.clear();
  }
  function refreshSelection(r){
    partners.clear();
    const source=selectedUid!==null&&byUid(r,selectedUid);
    if(!source||r.phase==='dead'){clearSelection();return;}
    for(const item of allItems(r)){
      if(item.uid===source.uid)continue;
      const result=E.previewPair(source,item);
      if(result.ok)partners.set(item.uid,result);
    }
  }
  function renderSelection(r){
    const strip=$('selected-item'),source=selectedUid!==null&&byUid(r,selectedUid);
    strip.hidden=!source;strip.closest('.kit-head').classList.toggle('has-selection',!!source);
    if(!source)return;
    $('sel-icon').innerHTML=tile(source.id,29);$('sel-name').textContent=C.items[source.id].name+(C.items[source.id].rarity==='common'?'':' · '+C.rarities[C.items[source.id].rarity].name)+' · Lv '+lv(source.level);
    const button=strip.querySelector('[data-selection="partners"]'),count=partners.size;
    button.textContent=partnerView?'Show all':count===1?'1 match':count+' matches';
    button.setAttribute('aria-pressed',String(!!partnerView));button.disabled=!count&&!partnerView;
    button.title=partnerView?'Show every pack slot':'Show compatible items across your backpack and equipment';
  }
  function selectGear(uid){
    const r=displayRun(),item=byUid(r,uid);if(!item)return;
    if(r.phase==='dead')return showDialog('item',{uid});
    if(selectedUid!==null&&uid!==selectedUid&&E.previewCombine(equipmentProfile(),selectedUid,uid).ok){
      showDialog('confirm-pair',{uid:selectedUid,other:uid,fromGrid:true});return;
    }
    exitPartnerView();
    selectedUid=uid===selectedUid?null:uid;
    renderGear(r);
    const slot=document.querySelector('.gear-slot[data-uid="'+uid+'"]');
    slot?.focus({preventScroll:true});
  }
  function selectionAction(action){
    const r=displayRun();if(selectedUid===null)return;
    if(action==='details')return showDialog('item',{uid:selectedUid});
    if(action==='clear'){
      const uid=selectedUid;clearSelection();renderGear(r);
      document.querySelector('.gear-slot[data-uid="'+uid+'"]')?.focus({preventScroll:true});return;
    }
    if(action==='partners'){
      if(partnerView)exitPartnerView();
      else{partnerView={page:packPage,query:packQuery,tier:packTier};packPage=0;packQuery='';packTier='all';$('pack-tier').value='all';$('pack-search').value='';}
      renderGear(r);
    }
  }
  function gearSlot(r,zone,index){
    const it=E.slotAt(r,zone,index),equipped=zone==='active',place=equipped?'equipped':'pack';
    const attrs='data-zone="'+zone+'" data-index="'+index+'"';
    const num='<span aria-hidden="true" class="slot-num">'+(equipped?'E':'P')+pad(index+1)+'</span>';
    if(!it)return '<div class="gear-slot empty" '+attrs+' aria-label="Empty '+place+' slot '+(index+1)+'">'+num+'<span class="empty-mark">+</span></div>';
    const d=C.items[it.id],name=esc(d.name),level=lv(it.level);
    const selected=it.uid===selectedUid,result=partners.get(it.uid);
    const verb=result?(it.id===byUid(r,selectedUid)?.id?'Upgrade':'Fuse'):'';
    const hint=result?' · '+verb+' → '+C.items[result.id].name+' · Lv '+lv(result.level):'';
    const state=selected?' is-selected':result?' fusion-partner':'';
    const tier=d.rarity==='common'?'':C.rarities[d.rarity].name;
    const stamp=tier?'<span class="gear-tier">'+tier+'</span>':num;
    const marker=result?'<span class="match-badge" aria-hidden="true">'+(verb==='Upgrade'?'+1':'✦')+'</span>':'';
    return '<button class="gear-slot'+state+'" '+attrs+' data-uid="'+it.uid+'" data-rarity="'+d.rarity+'" aria-pressed="'+selected+'" style="--el:'+C.elements[d.element].color+'" aria-label="'+name+(tier?', '+tier:'')+', level '+level+', '+place+' slot '+(index+1)+esc(hint)+'" title="'+name+(tier?' · '+tier:'')+' · Lv '+level+esc(hint)+'">'+stamp+marker+'<span class="level-tag">Lv '+level+'</span><span class="relic-picture">'+A.svg(it.id,58)+'</span><span class="slot-name">'+name+'</span>'+(equipped?'<span class="cd-text" data-clock="'+it.uid+'"></span><span class="cd-fill" data-fill="'+it.uid+'"></span>':'<span class="slot-type">'+elName(d.element)+' · '+J.styleLabel(d.style)+'</span>')+'</button>';
  }
  function resetPack(){clearSelection();packPage=0;packQuery='';packTier='all';$('pack-tier').value='all';$('pack-search').value='';}
  function renderPack(r){
    const size=pageSize(),terms=packQuery.toLowerCase().replace(/\b(?:level|lv)\s*(\d+(?:\.\d+)?)/g,'lv$1').trim().split(/\s+/).filter(Boolean);
    const matches=(terms.length||partnerView||packTier!=='all')?r.pack.filter(it=>{
      if(packTier!=='all'&&C.items[it.id].rarity!==packTier)return false;
      if(partnerView&&!partners.has(it.uid))return false;
      const fields=J.relicSearchText(C.items[it.id]);
      return terms.every(word=>/^lv\d/.test(word)?Number(word.slice(2))===it.level:/^p\d+$/.test(word)?Number(word.slice(1))===it.slot+1:fields.includes(word));
    }).sort((a,b)=>a.slot-b.slot):null;
    const total=matches?matches.length:E.packCapacity;
    packPages=Math.max(1,Math.ceil(total/size));packPage=Math.min(Math.max(0,packPage),packPages-1);
    const from=packPage*size,to=Math.min(total,from+size);
    const indices=matches?matches.slice(from,to).map(it=>it.slot):Array.from({length:to-from},(_,i)=>from+i);
    const grid=$('pack');grid.style.setProperty('--pack-rows',size/4);
    grid.innerHTML=indices.length?indices.map(i=>gearSlot(r,'pack',i)).join(''):'<p class="pack-miss">'+(partnerView&&!terms.length&&packTier==='all'&&partners.size?'All matches are in your equipped slots above.':'No matching relics in your backpack.')+'</p>';
    const pageHits=new Array(packPages).fill(0);
    if(selectedUid!==null)(matches||r.pack).forEach((it,i)=>{
      if(partners.has(it.uid))pageHits[Math.floor((matches?i:it.slot)/size)]++;
    });
    const picker=$('pack-page'),optionsKey=[size,packPages,!!matches,total,...pageHits].join('/');
    if(picker.dataset.key!==optionsKey){
      picker.innerHTML=Array.from({length:packPages},(_,i)=>'<option value="'+i+'" data-matches="'+pageHits[i]+'">'+(matches?'Page '+(i+1):'Slots '+(i*size+1)+'–'+Math.min(total,(i+1)*size))+(pageHits[i]?' · ✦ '+pageHits[i]:'')+'</option>').join('');
      picker.dataset.key=optionsKey;
    }
    picker.value=String(packPage);
    document.querySelector('[data-pack-step="-1"]').disabled=packPage===0;
    document.querySelector('[data-pack-step="1"]').disabled=packPage===packPages-1;
    $('pack-count').textContent=matches?total+' found · '+r.pack.length+' / '+E.packCapacity:r.pack.length+' / '+E.packCapacity;
    if(dragging?.moved)markPartners();
  }
  function renderGear(r){
    refreshSelection(r);renderSelection(r);
    $('gear-count').textContent=r.active.length+' / '+E.activeCapacity;
    $('equipped').innerHTML=Array.from({length:E.activeCapacity},(_,i)=>gearSlot(r,'active',i)).join('');
    renderPack(r);
  }
  function advancePage(step){packPage+=step;renderPack(displayRun());}
  function abilityRank(r,id){return r.skills[id]||0;}
  function skillTitle(u){return u.kind==='ability'?'Auto-cast ability':u.category;}
  function skillCard(r,id,mode){
    const u=P.upgrades[id],rank=abilityRank(r,id),next=mode==='draft',n=next?rank+1:Math.max(1,rank);
    return '<button class="skill-card" style="--el:'+C.elements[u.element].color+'" '+(next?'data-learn':'data-skill')+'="'+id+'">'+tile(u.icon,40)+'<span class="skill-copy"><small>'+esc(skillTitle(u))+(u.kind==='ability'?' · '+number(P.duration(r,id))+'s':'')+'</small><b>'+esc(u.name)+'</b><span>'+esc(P.describe(id,n))+'</span></span><span class="skill-rank">'+(next?(rank?'R'+rank+' → '+n:'RANK 1'):'RANK '+n)+'</span></button>';
  }
  function renderAbilities(r){
    const ids=P.abilities.filter(u=>abilityRank(r,u.id)>0);
    const el=$('ability-strip');el.hidden=!ids.length;
    el.innerHTML=ids.map(u=>'<button data-skill="'+u.id+'" class="ability-chip" style="--el:'+C.elements[u.element].color+'" title="'+esc(u.name)+'"><span class="ability-pic">'+A.svg(u.icon,24)+'</span><span><b>'+esc(u.name)+'</b><small data-ability-time="'+u.id+'"></small></span><span class="ability-fill" data-ability-fill="'+u.id+'"></span></button>').join('');
  }
  function updateGrowth(r){
    const need=E.xpNeeded(r.level),pending=r.draft.length>0;
    $('level-text').textContent='LV '+r.level;$('xp-text').textContent=Math.floor(r.xp)+' / '+need+' XP';
    $('xp-bar').style.width=Math.min(100,100*r.xp/need)+'%';
    $('growth-hint').textContent=pending?'Choose upgrade ›':'Run build ›';$('growth-btn').classList.toggle('awaiting',pending);
    for(const el of $('ability-strip').querySelectorAll('[data-ability-time]')){
      const id=el.dataset.abilityTime,time=r.abilityTimers[id]||0,period=P.duration(r,id);
      el.textContent=time>.05?time.toFixed(1)+'s':'Ready';
      el.closest('button').querySelector('.ability-fill').style.transform='scaleX('+Math.max(0,Math.min(1,1-time/period))+')';
    }
    if(pending&&!modal.open&&!dragging&&displayedDraft!==r.draft){showDialog('draft');}
  }
  let renderedDrop,renderedLocate;
  function renderFind(r){
    const drop=r.lastDrop,box=$('recent-find'),canLocate=!!(drop?.uid&&r.phase!=='dead'&&byUid(r,drop.uid));
    if(drop===renderedDrop&&canLocate===renderedLocate)return;
    renderedDrop=drop;renderedLocate=canLocate;
    box.dataset.rarity=drop?C.items[drop.id].rarity:'common';
    if(!drop){box.textContent='Fallen enemies can drop gear straight into your backpack.';return;}
    const d=C.items[drop.id],origin=drop.enemyName||C.enemies.find(e=>e.id===drop.enemyId)?.name;
    const status={packed:'Picked up',upgraded:'Duplicate upgraded',full:'Pack full · left behind'}[drop.outcome];
    const location=origin?' · '+esc(origin):'';
    box.innerHTML=A.svg(d.id,22)+'<span class="find-description">'+J.tierBadge(d)+' <b>'+esc(d.name)+'</b> · Lv '+lv(drop.level)+'<small>'+status+location+' · Wave '+drop.round+'</small></span>'+
      (canLocate?'<button type="button" class="find-gear" data-go-find="'+drop.uid+'" aria-label="Locate '+esc(d.name)+' in your kit">Locate ›</button>':'');
  }
  function locateFind(uid){
    const r=displayRun(),item=byUid(r,uid);if(!item)return;
    resetPack();if(r.pack.includes(item))packPage=Math.floor(item.slot/pageSize());
    selectedUid=uid;renderGear(r);
    const slot=document.querySelector('.gear-slot[data-uid="'+uid+'"]');
    slot?.focus({preventScroll:true});slot?.scrollIntoView({block:'nearest',inline:'nearest'});
  }
  function renderEncounter(r){
    const el=$('encounter'),e=r.enemy;
    el.classList.toggle('death-line',r.phase==='dead');
    if(r.phase==='dead'){el.innerHTML='<div><b>Back at camp.</b> Cleared '+r.kills+' rounds and earned '+r.lastAward+' trail seeds.</div><button class="text-link" data-dialog="debrief">Review run →</button>';return;}
    if(r.enemies.length>1){el.innerHTML='<div><b>'+r.enemies.length+' enemies closing in</b> · '+esc(C.regions[r.region].name)+'</div><button class="text-link" data-dialog="foes">Inspect group →</button>';return;}
    if(e){el.innerHTML='<div><b>'+esc(e.name)+'</b> · '+esc(e.trait)+' · Weak to '+elName(e.weak)+'</div><button class="text-link" data-species="'+e.id+'">Inspect enemy →</button>';return;}
    el.innerHTML='<div><b>'+(!profile.run?'At the trailhead':'On the road')+'</b> · '+esc(C.regions[r.region].subtitle)+'</div><button class="text-link" data-dialog="help">Handbook →</button>';
  }
  let previousGear='';
  function update(force=false){
    if(page!=='expedition')return;
    const r=displayRun(),active=!!profile.run;
    const sig=[active,r.round,r.phase,r.enemy?.uid,r.enemies.length,r.level,r.draft.join(','),r.lastDrop?.round,r.lastDrop?.targetUid,r.nextUid,r.pack.length,r.active.length,manualPause,prefs.audio,r.speed,profile.points,profile.best].join(':');
    if(force||sig!==structure){
      document.body.classList.toggle('reduced-art',!prefs.motion);
      structure=sig;$('region-cap').textContent='REGION '+pad(r.region+1);$('region-name').textContent=C.regions[r.region].name;$('round-chip').textContent='ROUND '+pad(r.round);
      $('best-count').textContent=pad(profile.best);$('point-count').textContent=number(profile.points);
      const walking=active&&r.phase!=='dead',bank=E.awardBreakdown(r).reduce((sum,row)=>sum+row.amount,0);
      const pending=$('pending-seeds');pending.hidden=!walking;pending.textContent=walking?'+'+bank:'';
      const wallet=pending.closest('button');
      wallet.title=profile.points+' banked trail seeds'+(walking?' · '+bank+' more when this run ends':'');
      wallet.setAttribute('aria-label','Legacy. '+wallet.title);
      const btn=$('run-btn'),label=!active?'Begin expedition':r.phase==='dead'?'Return to camp':r.draft.length?'Choose upgrade':manualPause?'Resume':'Pause';
      btn.innerHTML=icon(label==='Pause'?'pause':'play')+label;
      $('speed-btn').innerHTML=icon('speed')+r.speed+'× pace';$('audio-btn').innerHTML=icon(prefs.audio?'audio':'mute')+(prefs.audio?'Sound on':'Sound off');
      $('audio-btn').setAttribute('aria-pressed',String(prefs.audio));$('pause-glass').hidden=!(manualPause&&active&&['walk','fight'].includes(r.phase));
      $('world').setAttribute('aria-label','Pip in '+C.regions[r.region].name+(r.enemy?', facing '+r.enemy.name:'.'));
      const gear=r.active.concat(r.pack).map(it=>[it.uid,it.id,it.level,it.slot].join('/')).join('|')+';'+r.active.map(it=>it.uid).join('/');
      if(!dragging&&(gear!==previousGear||force||r.phase==='dead'&&selectedUid!==null)){renderGear(r);previousGear=gear;}
      renderFind(r);renderEncounter(r);renderAbilities(r);
    }
    if(!titleOpen&&active&&r.phase==='dead'&&debriefedRun!==r&&!modal.open&&!dragging)showDialog('debrief');
    updateGrowth(r);
    $('hp-num').textContent=Math.ceil(r.hp)+' / '+r.maxHp;$('hp-bar').style.width=100*r.hp/r.maxHp+'%';
    $('hero-sub').textContent=r.shield>.1?'Shield +'+Math.ceil(r.shield):'Trailwalker · '+(active?r.foes:0)+' defeated';
    const e=r.phase==='fight'?r.enemy:null;
    $('enemy-name').textContent=e?(r.enemies.length>1?r.enemies.length+' enemies':e.name):r.phase==='dead'?'Expedition ended':'Open trail';
    const hpTotal=r.enemies.reduce((n,foe)=>n+foe.hp,0),maxTotal=r.enemies.reduce((n,foe)=>n+foe.maxHp,0);
    $('enemy-num').textContent=e?Math.ceil(hpTotal)+' / '+maxTotal:'';
    $('enemy-bar').style.width=e?100*hpTotal/maxTotal+'%':r.phase==='walk'&&active?100*(1-r.walkLeft/E.walkDuration)+'%':'0%';
    $('enemy-sub').textContent=e?(r.enemies.length>1?'Lead foe · '+e.name:'Weak to '+elName(e.weak)+' · Resists '+elName(e.resist)):r.phase==='dead'?'Resting at camp':active?'Approaching encounter':'The road is quiet';
    for(const it of r.active){const clock=document.querySelector('[data-clock="'+it.uid+'"]'),bar=document.querySelector('[data-fill="'+it.uid+'"]');if(clock)clock.textContent=it.cd>.04&&e?it.cd.toFixed(1)+'s':'Ready';if(bar)bar.style.transform='scaleX('+(it.cd>.04&&e?Math.max(0,1-it.cd/E.itemCooldown(it,r)):1)+')';}
  }
  function closeDialog(){if(modal.open)modal.close();dialogState=null;pendingImport=null;}
  function showDialog(type,data={}){
    if(!modal.open)preDialogFocus=document.activeElement;
    dialogState={type,...data};let html='',kicker='FIELD NOTES',portrait=null;
    const r=displayRun(),live=r.phase!=='dead';
    if(type==='debrief'){
      if(r.phase!=='dead')return closeDialog();
      debriefedRun=r;kicker='EXPEDITION COMPLETE';html=J.debrief(profile);
    }else if(type==='draft'){
      if(!r.draft.length)return showDialog('build');
      displayedDraft=r.draft;kicker='LEVEL '+r.level+' / EXPEDITION UPGRADE';
      html='<h2 class="dialog-title">Choose an upgrade</h2><p class="body-copy">Pip reached level '+r.level+'. Pick one for this expedition. Picking it again adds another rank.</p><div class="skill-list">'+r.draft.map(id=>skillCard(r,id,'draft')).join('')+'</div><p class="mini-note">The encounter waits while you choose.</p>';
    }else if(type==='build'){
      kicker='PIP’S EXPEDITION BUILD';const entries=Object.keys(r.skills).filter(id=>r.skills[id]>0);
      html='<h2 class="dialog-title">Level '+r.level+' trailwalker</h2><p class="body-copy">Travel and combat earn XP. At each level, choose one of three upgrades. Your ranks last for this run.</p><div class="stat-row"><span class="stat-pill">'+Math.floor(r.xp)+' / '+E.xpNeeded(r.level)+' XP</span><span class="stat-pill">'+r.foes+' foes defeated</span></div>';
      if(r.draft.length)html+='<button data-dialog="draft" class="primary">Choose your upgrade →</button>';
      html+=entries.length?'<div class="skill-list">'+entries.map(id=>skillCard(r,id,'owned')).join('')+'</div>':'<p class="empty-note">Your first level-up opens three choices.</p>';
      html+='<button class="text-link" data-do="browse-skills">'+icon('book')+'Browse all '+Object.keys(P.upgrades).length+' upgrades →</button>';
    }else if(type==='skill'){
      const u=P.upgrades[data.id];if(!u)return closeDialog();const rank=abilityRank(r,u.id);
      kicker=skillTitle(u).toUpperCase();
      html='<div class="dialog-lead">'+tile(u.icon,52)+'<div><h2 class="dialog-title">'+esc(u.name)+'</h2><div class="eyebrow">'+elName(u.element)+' · '+(rank?'Rank '+rank:'Unlearned')+'</div></div></div><p class="body-copy">'+esc(u.description)+'</p>';
      if(rank)html+='<div class="subhead">Current rank '+rank+'</div><p class="body-copy">'+esc(P.describe(u.id,rank))+'</p>';
      html+='<div class="subhead">Rank '+(rank+1)+'</div><p class="body-copy">'+esc(P.describe(u.id,rank+1))+'</p>';
      if(u.kind==='ability'&&rank)html+='<div class="stat-row"><span class="stat-pill">'+number(P.duration(r,u.id))+'s cooldown</span></div>';
      html+='<button class="text-link" data-dialog="build">View expedition build →</button>';
    }else if(type==='foes'){
      kicker='ENCOUNTER / ROUND '+r.round;html='<h2 class="dialog-title">Enemies on the trail</h2><div class="pair-list">'+r.enemies.map(e=>'<button class="pair-option" data-species="'+e.id+'"><span><b>'+esc(e.name)+'</b><small>'+Math.ceil(e.hp)+' / '+e.maxHp+' HP · Weak to '+elName(e.weak)+' · '+Math.floor(100*e.attackProgress)+'% attack charge</small></span></button>').join('')+'</div>';
    }else if(['item','catalog'].includes(type)){
      const it=type==='item'?byUid(r,data.uid):{id:data.id,level:1},d=C.items[it?.id];if(!d)return closeDialog();
      const held=type==='item'&&r.active.some(i=>i.uid===it.uid);
      kicker=type==='catalog'?'RELIC DOSSIER':held?'EQUIPPED RELIC':'BACKPACK RELIC';
      html='<div class="dialog-lead">'+tile(d.id,55)+'<div style="--el:'+C.elements[d.element].color+'"><h2 class="dialog-title">'+esc(d.name)+'</h2><div class="eyebrow">'+elName(d.element)+' / '+esc(J.styleLabel(d.style))+' / Level '+lv(it.level)+'</div></div></div><p class="body-copy">'+esc(d.description)+'</p><div class="stat-row"><span class="stat-pill">'+number(E.itemPower(profile,it))+' base damage</span><span class="stat-pill">'+Number(E.itemCooldown(it,r).toFixed(2))+'s cooldown</span></div>';
      html+=J.tierBadge(d,true);
      if(d.recipe)html+='<div class="subhead">Fusion recipe</div><div class="chip-row">'+d.recipe.map(id=>'<button class="outline compact" data-catalog="'+id+'">'+esc(C.items[id].name)+'</button>').join(' + ')+'</div>';
      html+=J.relicSource(d);
      html+='<div class="subhead">Level effects</div><div class="milestones">'+[3,5,10].map(n=>'<div class="mile '+(it.level>=n?'live':'')+'"><b>LV '+n+'</b><span>'+esc(d.milestones[n])+'</span></div>').join('')+'</div>';
      if(type==='item'&&live){
        const last=held&&r.active.length===1,swap=held?r.pack.length>=E.packCapacity:r.active.length>=E.activeCapacity;
        const travel=swap?'swap':held?'stow':'equip',travelName=swap?'Swap gear':held?'Move to pack':'Equip';
        html+='<div class="dialog-actions"><button class="primary" data-gear="'+travel+'" '+(last&&travel==='stow'?'disabled title="Keep one relic equipped"':'')+'>'+icon(held?'pack':'swords')+travelName+'</button><button class="outline" data-gear="combine">'+icon('merge')+'Combine</button><button class="flat" data-gear="discard" '+(last?'disabled title="Keep one relic equipped"':'')+'>'+icon('trash')+'Discard</button></div>';
      }
      if(type==='catalog')html+='<button class="text-link" data-find-recipes="'+d.id+'">'+icon('book')+'Recipes using '+esc(d.name)+' →</button>';
    }else if(type==='pair'||type==='swap'){
      const it=byUid(r,data.uid);if(!it)return closeDialog();const d=C.items[it.id],swap=type==='swap';
      kicker=swap?'GEAR EXCHANGE':'TRAIL FORGE';html='<h2 class="dialog-title">'+(swap?'Swap gear':'Combine relics')+'</h2><p class="body-copy">'+(swap?'Choose a relic to exchange with ':'Choose a partner for ')+'<b>'+esc(d.name)+' · Lv '+lv(it.level)+'</b>.</p>';
      const list=swap?(r.active.includes(it)?r.pack:r.active):allItems(r).filter(y=>y.uid!==it.uid&&E.previewCombine(equipmentProfile(),it.uid,y.uid).ok);
      html+='<div class="pair-list">'+list.map(y=>{const result=swap?null:E.previewCombine(equipmentProfile(),it.uid,y.uid),out=swap?C.items[y.id].name:C.items[result.id].name;return '<button class="pair-option" data-'+(swap?'swap-with':'pair-with')+'="'+y.uid+'">'+tile(y.id,30)+'<span><b>'+esc(C.items[y.id].name)+' · Lv '+lv(y.level)+'</b><small>'+(swap?(r.active.includes(y)?'Equipped':'Backpack'):'Makes '+esc(out)+' · Lv '+lv(result.level))+'</small></span></button>';}).join('')+'</div>';
      if(!list.length)html+='<p class="empty-note">No compatible partner in your kit. Find an identical copy at the same level or match a fusion recipe.</p>';
      html+='<div class="dialog-actions"><button class="outline" data-uid="'+it.uid+'">Back</button><button class="flat" data-find-recipes="'+d.id+'">Find recipes →</button></div>';
    }else if(type==='confirm-pair'){
      const a=byUid(r,data.uid),b=byUid(r,data.other),res=E.previewCombine(equipmentProfile(),data.uid,data.other);if(!res.ok)return closeDialog();
      kicker=a.id===b.id?'LEVEL UPGRADE':'NEW FUSION';const out=C.items[res.id];html='<div class="dialog-lead">'+tile(out.id,56)+'<div><h2 class="dialog-title">'+esc(out.name)+'</h2><span class="level-tag">LEVEL '+lv(res.level)+'</span> '+J.tierBadge(out)+'</div></div><p class="body-copy">'+esc(out.description)+'</p><div class="stat-row"><span class="stat-pill">'+number(E.itemPower(profile,{id:out.id,level:res.level}))+' base damage</span><span class="stat-pill">'+out.cooldown+'s cooldown</span></div><p class="body-copy">Uses '+esc(C.items[a.id].name)+' (Lv '+lv(a.level)+') and '+esc(C.items[b.id].name)+' (Lv '+lv(b.level)+'). The result goes into your '+(r.active.includes(a)||r.active.includes(b)?'equipped slots':'backpack')+'.</p><div class="dialog-actions"><button class="primary" data-do="forge">'+icon('merge')+(a.id===b.id?'Upgrade item':'Fuse items')+'</button>'+(data.fromGrid?'<button class="outline" data-do="cancel-pair">Cancel</button>':'<button class="outline" data-gear="combine">Back</button>')+'</div>';
    }else if(type==='discard'){
      const it=byUid(r,data.uid);if(!it)return closeDialog();
      kicker='BACKPACK';html='<h2 class="dialog-title">Discard '+esc(C.items[it.id].name)+'?</h2><p class="body-copy">Pip will leave this level '+lv(it.level)+' item on the road.</p><div class="dialog-actions"><button class="danger" data-do="drop">Discard item</button><button class="outline" data-uid="'+it.uid+'">Keep item</button></div>';
    }else if(type==='species'){
      const e=C.enemies.find(x=>x.id===data.id);if(!e)return;
      kicker=e.boss?'GUARDIAN DOSSIER':'CREATURE DOSSIER';portrait=e;
      html='<div class="dialog-lead"><canvas id="detail-portrait" class="dialog-portrait" aria-hidden="true"></canvas><div><h2 class="dialog-title">'+esc(e.name)+'</h2><div class="eyebrow">'+esc(C.regions[e.region].name)+'</div></div></div><div class="stat-row"><span class="stat-pill">Weak to '+elName(e.weak)+' · 1.68×</span><span class="stat-pill">Resists '+elName(e.resist)+' · 0.60×</span></div><div class="subhead">'+esc(e.trait)+'</div><p class="body-copy">'+esc(J.traitText[e.trait])+'</p><div class="subhead">Relics to try</div><div class="chip-row">'+defs.filter(x=>!x.recipe&&x.element===e.weak).slice(0,3).map(d=>'<button class="outline compact" data-catalog="'+d.id+'">'+esc(d.name)+'</button>').join('')+'</div>';
    }else if(type==='region'){
      const i=data.index;if(!C.regions[i])return closeDialog();
      kicker='FIELD ATLAS / REGION '+pad(i+1)+' OF '+C.regions.length;html=J.regionDossier(r,i);
    }else if(type==='pace'){
      kicker='TRAIL MEMORY';html='<h2 class="dialog-title">Replay pace</h2><p class="body-copy">Spend trail seeds on Trail memory between runs to unlock 2×, 3×, and 4×. Bonus speed works on waves you cleared in this region and circuit. '+(!profile.run?'New ground runs at 1×.':E.isReplay(profile,r)?'This wave is eligible for replay speed.':'This is new ground and runs at 1×.')+'</p><div class="stat-row">'+[1,2,3,4].map(n=>'<button class="'+(n===(r.paceChoice||1)?'primary':'outline')+'" data-pace="'+n+'" '+(!live||n>E.maxSpeed(profile)?'disabled':'')+'>'+n+'×</button>').join('')+'</div><button class="text-link" data-nav="legacy">'+icon('sprout')+'Visit legacy →</button>';
    }else if(type==='route'){
      kicker='FIELD ATLAS / CIRCUIT '+pad(E.routeInfo(r).circuit);
      html=J.routeAtlas(r);
    }else if(type==='discard-picker'){
      kicker='PACK MANAGEMENT';html='<h2 class="dialog-title">Discard a relic</h2><p class="body-copy">Choose an item, or drag one onto Discard from the backpack.</p><div class="pair-list">'+allItems(r).map(it=>'<button class="pair-option" data-discard-id="'+it.uid+'" '+(!live||(r.active.includes(it)&&r.active.length===1)?'disabled':'')+'>'+tile(it.id,30)+'<span><b>'+esc(C.items[it.id].name)+'</b><small>Lv '+lv(it.level)+' · '+(r.active.includes(it)?'Equipped':'Backpack')+'</small></span></button>').join('')+'</div>';
    }else if(type==='help'){
      kicker='THE TRAIL HANDBOOK';html='<h2 class="dialog-title">Walking with Pip</h2>'+[
        ['On the road','Pip starts alone against one foe. The first guardian arrives with two escorts on wave 4. Groups reach four foes on wave 7 and five on wave 11. Every fourth wave is a guardian.'],
        ['Pack for the next fight','Four equipped relics attack on independent cooldowns. Your backpack holds 400 more. Search by name, element, or effect, or jump to a page by slot number. Drag an item to an empty slot to move it. Drop onto a matching recipe to fuse, or onto an unrelated item to swap. Select a relic to light up every valid partner. Tap a glowing item to preview the result, or choose Details to equip or discard.'],
        ['Trail finds','Every defeated enemy can drop a relic into your backpack during the fight. The chance of at least one drop across a full wave is 16%, or 37% in a guardian wave. When the pack is full, a matching copy at the same level upgrades automatically. Other finds stay on the road.'],
        ['Leveling up','Time on the trail and defeated enemies earn XP. At each level, choose one of three random upgrades. Passive bonuses stack, and learned abilities cast automatically on separate cooldowns. Expedition ranks reset when you start another run.'],
        ['Group encounters','Enemies approach together and charge their attacks independently. A green bar shows health, a gold bar shows windup, and a red warning marks an imminent strike. Area attacks can hit multiple foes.'],
        ['Relic rarity','Rare and mythic relics glow in your pack, on the road, and in Pip’s hand. The field guide shows drop unlocks and recipes. Guardians and later regions give better odds for higher tiers. The atlas lists your current drop odds.'],
        ['At the forge','Two matching copies at the same level gain one level, with no final tier. Different ingredients in a listed recipe give their exact average level. A level 2 and a level 3 make a level 2.5 relic.'],
        ['Growing stronger','Level 3 adds an echo strike, level 5 adds a shield ring, and level 10 adds a damage nova. All three effects stack.'],
        ['After a fall','Earn one seed for setting out, one per cleared wave, and one extra per four defeats. Spend seeds on health, damage, or replay speed in Legacy.'],
        ['Take your time','Gear details, the field guide, and settings pause battle. Space pauses or resumes on the road. Profiles save locally and can be exported from the top-right settings button.']
      ].map(([t,p])=>'<div class="dialog-section"><h3>'+t+'</h3><p class="body-copy">'+p+'</p></div>').join('');
    }else if(type==='profile'){
      kicker='TRAVELER’S RECORD';html='<h2 class="dialog-title">Profile & settings</h2><p class="body-copy">'+(storageOK?'Progress saves on this device. Export a copy to move Pip to another browser.':'This browser cannot save locally. Export a profile to keep your progress.')+'</p><div class="stat-row"><span class="stat-pill">Best round '+profile.best+'</span><span class="stat-pill">'+profile.deaths+' completed runs</span></div>'+
      '<div class="action-row"><button class="outline" data-do="export">Export profile</button><button class="outline" data-do="import">Import profile</button></div><div class="dialog-section"><div class="pref-row"><div><b>Sound effects</b><small>Elemental attacks and trail chimes.</small></div><button class="outline compact" data-do="sound" aria-pressed="'+prefs.audio+'">'+(prefs.audio?'On':'Off')+'</button></div><div class="pref-row"><div><b>Ambient motion</b><small>Moving scenery, particles, and idle animation.</small></div><button class="outline compact" data-do="motion" aria-pressed="'+prefs.motion+'">'+(prefs.motion?'On':'Reduced')+'</button></div></div><button class="text-link" data-dialog="help">'+icon('book')+'Read the trail handbook →</button>';
    }else if(type==='confirm-import'&&pendingImport){
      const p=pendingImport.profile;kicker='PROFILE TRANSFER';html='<h2 class="dialog-title">Use this profile?</h2><p class="body-copy">Replace the current profile with this traveler’s record. You can export the current one first.</p><div class="stat-row"><span class="stat-pill">Best round '+p.best+'</span><span class="stat-pill">'+p.points+' trail seeds</span><span class="stat-pill">'+(p.run?'Round '+p.run.round+' · '+p.run.phase:'At camp')+'</span></div><div class="dialog-actions"><button class="primary" data-do="confirm-import">Import profile</button><button class="outline" data-do="export">Export current</button><button class="flat" data-dialog="profile">Cancel</button></div>';
    }else if(type==='import-error'){
      kicker='PROFILE TRANSFER';html='<h2 class="dialog-title">Couldn’t import profile</h2><p class="body-copy">'+esc(data.message)+'</p><div class="dialog-actions"><button class="outline" data-do="import">Choose another file</button><button class="flat" data-dialog="profile">Back</button></div>';
    }else if(type==='recovery'){
      kicker='SAVE RECOVERY';html='<h2 class="dialog-title">Local profile could not load</h2><p class="body-copy">Download the original data before starting a fresh profile, or import an exported save.</p><div class="dialog-actions"><button class="outline" data-do="recover">Download original</button><button class="outline" data-do="import">Import profile</button><button class="danger" data-do="fresh">Start fresh</button></div>';
    }else return;
    $('dialog-kicker').textContent=kicker;$('dialog-body').innerHTML=html;ink($('dialog-body'));modal.classList.toggle('level-dialog',type==='draft');modal.classList.toggle('atlas-dialog',type==='region'||type==='route');modal.classList.toggle('debrief-dialog',type==='debrief');const title=modal.querySelector('.dialog-title');if(title){title.id='dialog-title';modal.setAttribute('aria-labelledby','dialog-title');}
    if(!modal.open)modal.showModal();modal.scrollTop=0;
    if(type==='region')WorldArt.regionPortrait($('region-art'),data.index);
    if(portrait)WorldArt.portrait($('detail-portrait'),portrait.species,portrait.color,portrait.boss);
  }
  function download(raw,file){
    const url=URL.createObjectURL(new Blob([raw],{type:'application/json'})),a=document.createElement('a');
    a.href=url;a.download=file;document.body.append(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(url),3000);
  }
  function toggleAudio(){prefs.audio=!prefs.audio;S?.setEnabled(prefs.audio,true);if(prefs.audio)sound('equip');save();structure='';}
  function handleAction(name){
    const d=dialogState||{};
    if(name==='start')return start();
    if(name==='browse-skills'){guideTab='upgrades';query='';navigate('guide');return;}
    if(name==='cancel-pair'){closeDialog();return;}
    if(name==='forge'){
      const owner=equipmentProfile(),result=E.act(owner,'combine',d.uid,d.other);
      if(!result.ok){notify(result.message);return;}
      if(owner===starter)campChanged=true;
      const item=byUid(owner.run,result.uid);
      if(selectedUid===d.uid||selectedUid===d.other){exitPartnerView();selectedUid=item.uid;}
      save();closeDialog();sound('combine',C.items[item.id].element);notify(result.message);update(true);return;
    }
    if(name==='drop'){if(runCommand('discard',d.uid)){closeDialog();notify('Item discarded.');update(true);}return;}
    if(name==='sound'){toggleAudio();showDialog('profile');return;}
    if(name==='motion'){prefs.motion=!prefs.motion;document.body.classList.toggle('reduced-art',!prefs.motion);save();showDialog('profile');return;}
    if(name==='export'){download(serialize(),'Bramblebound-profile.json');return;}
    if(name==='import'){$('import-input').value='';$('import-input').click();return;}
    if(name==='confirm-import'&&pendingImport){
      ({profile,prefs}=pendingImport);starter=pendingImport.camp||freshKit();campChanged=!!pendingImport.camp;damagedSave=null;S?.setEnabled(prefs.audio,true);
      closeDialog();manualPause=!!profile.run&&['walk','fight'].includes(profile.run.phase);
      resetPack();displayedDraft='';effects=[];defeated=null;encounterAge=1;lastHeld=null;heldFor=0;sceneClock=0;save();navigate('expedition');notify('Profile imported.');return;
    }
    if(name==='recover'&&damagedSave){download(damagedSave,'Bramblebound-recovery.json');return;}
    if(name==='fresh'){damagedSave=null;profile=E.freshProfile();starter=freshKit();campChanged=false;prefs=defaultPrefs();closeDialog();save();navigate('expedition');return;}
  }
  let dragging=null,blockDragClick=false;
  const dragNote='Drag to equip, swap, or fuse. Select a relic to light up its matches.';
  const kit=document.querySelector('.kit-col');
  function hitSlot(x,y){return document.elementFromPoint(x,y)?.closest('.gear-slot,#discard-zone')||null;}
  function dragIntent(target){
    const p=equipmentProfile(),r=p.run,uid=dragging.uid,source=byUid(r,uid);
    if(!target||!source)return {text:'Drop on a slot or on Discard.'};
    const sole=r.active.length===1&&r.active[0]===source;
    if(target.id==='discard-zone')return {text:sole?'Keep one relic equipped.':'Discard '+C.items[source.id].name+'…',denied:sole};
    const zone=target.dataset.zone,index=Number(target.dataset.index),it=E.slotAt(r,zone,index);
    const shown=Number(target.dataset.uid||0);
    if((it?.uid||0)!==shown)return {text:'This slot just changed. Try another slot.',denied:true};
    if(it===source)return {text:'Return to original slot.'};
    const fusion=it&&E.previewCombine(p,uid,it.uid);
    if(sole&&zone==='pack'&&(!it||fusion?.ok))return {text:'Keep one relic equipped.',denied:true};
    if(fusion?.ok)return {text:(it.id===source.id?'Upgrade':'Fuse')+' → '+C.items[fusion.id].name+' · Lv '+lv(fusion.level)};
    if(it)return {text:'Swap with '+C.items[it.id].name};
    return {text:'Move to '+(zone==='active'?'equipped':'pack')+' slot '+(index+1)};
  }
  function markPartners(){
    const p=equipmentProfile(),g=dragging;
    for(const slot of kit.querySelectorAll('.gear-slot[data-uid]')){
      const uid=Number(slot.dataset.uid);
      slot.classList.toggle('drag-source',uid===g.uid);
      slot.classList.toggle('compatible',E.previewCombine(p,g.uid,uid).ok);
    }
  }
  function dragPageAt(e,g){
    const arrow=document.elementFromPoint(e.clientX,e.clientY)?.closest('[data-pack-step]');
    if(g.arrow===arrow)return arrow;
    clearTimeout(g.turnPage);g.arrow=arrow;
    if(arrow&&!arrow.disabled){
      g.turnPage=setTimeout(()=>{
        if(dragging!==g||g.arrow!==arrow)return;
        advancePage(Number(arrow.dataset.packStep));$('drag-note').textContent='Drop on a slot on this page.';
      },420);
    }
    return arrow;
  }
  function trackDrag(e){
    const g=dragging;if(!g||g.pid!==e.pointerId)return;
    if(!g.moved){
      if(Math.hypot(e.clientX-g.x,e.clientY-g.y)<6)return;
      g.moved=true;g.node.classList.add('drag-source');document.body.classList.add('dragging-gear');
      kit.setPointerCapture(e.pointerId);
      const p=equipmentProfile(),item=byUid(p.run,g.uid);if(!item){endDrag(false);return;}
      markPartners();
      g.chip=document.createElement('div');g.chip.className='drag-chip';g.chip.setAttribute('aria-hidden','true');
      g.chip.dataset.rarity=C.items[item.id].rarity;g.chip.innerHTML=A.svg(item.id,31)+'<span>'+esc(C.items[item.id].name)+'</span> '+J.tierBadge(C.items[item.id]);document.body.append(g.chip);
    }
    e.preventDefault();
    const arrow=dragPageAt(e,g),target=hitSlot(e.clientX,e.clientY),intent=arrow?{text:'Hold over the arrow to turn the page.'}:dragIntent(target);
    if(g.target!==target){g.target?.classList.remove('drop-over','drop-denied');g.target=target;}
    if(target){target.classList.add('drop-over');target.classList.toggle('drop-denied',!!intent.denied);}
    if($('drag-note').textContent!==intent.text)$('drag-note').textContent=intent.text;
    const x=Math.max(3,Math.min(e.clientX+12,innerWidth-g.chip.offsetWidth-3)),y=Math.max(3,e.clientY-g.chip.offsetHeight-12);
    g.chip.style.transform='translate('+x+'px,'+y+'px)';
  }
  function endDrag(apply,target=null){
    const g=dragging;if(!g)return;
    const intent=g.moved&&apply&&target?dragIntent(target):null;
    clearTimeout(g.turnPage);dragging=null;
    if(!g.moved){structure='';return;}
    blockDragClick=true;g.chip?.remove();document.body.classList.remove('dragging-gear');
    kit.querySelectorAll('.compatible,.drop-over,.drag-source,.drop-denied').forEach(n=>n.classList.remove('compatible','drop-over','drag-source','drop-denied'));
    if(kit.hasPointerCapture(g.pid))kit.releasePointerCapture(g.pid);
    $('drag-note').textContent=dragNote;
    if(intent?.denied)notify(intent.text);
    else if(intent&&equipmentProfile().run.phase!=='dead'){
      if(target.id==='discard-zone')showDialog('discard',{uid:g.uid});
      else{
        const result=E.act(equipmentProfile(),'drop',g.uid,target.dataset.zone,Number(target.dataset.index));
        if(result.operation!=='none')notify(result.message);
        if(result.ok&&result.operation!=='none'){
          if(['upgrade','fuse'].includes(result.operation)&&(selectedUid===g.uid||selectedUid===Number(target.dataset.uid))){exitPartnerView();selectedUid=result.uid;}
          sound(['upgrade','fuse'].includes(result.operation)?'combine':'equip');
          if(!profile.run)campChanged=true;save();
        }
      }
    }
    structure='';update(true);
  }
  document.addEventListener('pointerdown',e=>{
    if(dragging||!e.isPrimary||e.button!==0)return;
    blockDragClick=false;
    const node=e.target.closest('.gear-slot[data-uid]');
    if(node&&page==='expedition'&&!modal.open&&displayRun().phase!=='dead'){
      dragging={pid:e.pointerId,uid:Number(node.dataset.uid),node,x:e.clientX,y:e.clientY,moved:false,chip:null,target:null};
    }
  });
  document.addEventListener('pointermove',trackDrag,{passive:false});
  document.addEventListener('pointerup',e=>{if(dragging?.pid===e.pointerId)endDrag(true,hitSlot(e.clientX,e.clientY));});
  document.addEventListener('pointercancel',e=>{if(dragging?.pid===e.pointerId)endDrag(false);});
  kit.addEventListener('lostpointercapture',e=>{if(e.target===kit&&dragging?.pid===e.pointerId)endDrag(false);});
  kit.addEventListener('contextmenu',e=>{if(e.target.closest('.gear-slot'))e.preventDefault();});
  window.addEventListener('blur',()=>{if(dragging)endDrag(false);});
  document.addEventListener('click',e=>{if(blockDragClick&&e.detail){e.preventDefault();e.stopImmediatePropagation();blockDragClick=false;}},true);

  let expanded=false;
  const nativeFull=()=>document.fullscreenElement||document.webkitFullscreenElement;
  function fullLabel(){
    const active=!!nativeFull()||expanded,btn=$('full-btn');
    const label=active?(expanded?'Exit expanded view':'Exit fullscreen'):'Enter fullscreen';
    btn.innerHTML=icon(active?'collapse':'expand')+'<span>'+(active?'Exit view':'Fullscreen')+'</span>';
    document.body.classList.toggle('focus-view',active);btn.setAttribute('aria-pressed',String(active));btn.setAttribute('aria-label',label);btn.title=label;
  }
  async function toggleFullscreen(){
    if(nativeFull()){const exit=document.exitFullscreen||document.webkitExitFullscreen;if(exit)try{await exit.call(document);}catch{notify('Use Escape to exit fullscreen.');}}
    else if(expanded){expanded=false;document.body.classList.remove('expanded-view');}
    else{
      const root=document.documentElement,request=root.requestFullscreen||root.webkitRequestFullscreen;
      try{if(!request)throw Error();await request.call(root);}
      catch{expanded=true;document.body.classList.add('expanded-view');notify('Expanded play view');}
    }
    fullLabel();
  }
  document.addEventListener('fullscreenchange',fullLabel);
  document.addEventListener('webkitfullscreenchange',fullLabel);
  document.addEventListener('keydown',e=>{if(e.key==='Escape'&&dragging){endDrag(false);e.preventDefault();}else if(e.key==='Escape'&&expanded&&!modal.open){expanded=false;document.body.classList.remove('expanded-view');fullLabel();}});

  document.addEventListener('click',event=>{
    const el=event.target.closest('button,a[data-nav],a[data-title]');if(!el||el.disabled)return;
    if(prefs.audio)S?.setEnabled(true,true);
    const ds=el.dataset,d=dialogState||{};
    if('title' in ds){event.preventDefault();closeDialog();titleMenu(true);return;}
    if(el.id==='title-play'){enterFromTitle();return;}
    if(el.id==='title-kit'){enterFromTitle(true);return;}
    if(el.id==='title-help'){showDialog('help');return;}
    if(el.id==='title-profile'){showDialog(damagedSave?'recovery':'profile');return;}
    if(ds.learn){if(runCommand('chooseUpgrade',ds.learn)){closeDialog();manualPause=false;displayedDraft='';sound('combine');notify(P.upgrades[ds.learn].name+' · rank '+displayRun().skills[ds.learn]);update(true);}return;}
    if(ds.skill){showDialog('skill',{id:ds.skill});return;}
    if(ds.packStep){advancePage(Number(ds.packStep));return;}
    if(el.id==='growth-btn'){showDialog(displayRun().draft.length?'draft':'build');return;}
    if(ds.nav){event.preventDefault();navigate(ds.nav);return;}
    if(ds.dialog){showDialog(ds.dialog);return;}
    if(ds.do){handleAction(ds.do);return;}
    if(ds.selection){selectionAction(ds.selection);return;}
    if('uid' in ds){if(el.classList.contains('gear-slot'))selectGear(Number(ds.uid));else showDialog('item',{uid:Number(ds.uid)});return;}
    if(ds.discardId){showDialog('discard',{uid:Number(ds.discardId)});return;}
    if(ds.catalog){showDialog('catalog',{id:ds.catalog});return;}
    if(ds.species){showDialog('species',{id:ds.species});return;}
    if('region' in ds){showDialog('region',{index:Number(ds.region)});return;}
    if(ds.findRecipes){guideTab='recipes';guideTier='all';query=C.items[ds.findRecipes].name;navigate('guide');return;}
    if(ds.goFind){locateFind(Number(ds.goFind));return;}
    if(ds.guideTier){guideTier=ds.guideTier;renderGuide();$('guide-page').querySelector('[data-guide-tier="'+guideTier+'"]')?.focus({preventScroll:true});return;}
    if(ds.gearRole){
      query=query.toLowerCase()===ds.gearRole.toLowerCase()?'':ds.gearRole;
      renderGuide();$('guide-page').querySelector('[data-gear-role="'+ds.gearRole+'"]')?.focus({preventScroll:true});return;
    }
    if(ds.guideTab){guideTab=ds.guideTab;query='';renderGuide();$('guide-page').querySelector('[data-guide-tab="'+guideTab+'"]')?.focus();return;}
    if(ds.gear){
      if(ds.gear==='combine')showDialog('pair',{uid:d.uid});
      else if(['swap','discard'].includes(ds.gear))showDialog(ds.gear,{uid:d.uid});
      else if(runCommand(ds.gear,d.uid)){sound('equip');closeDialog();update(true);}
      return;
    }
    if('pairWith' in ds){showDialog('confirm-pair',{uid:d.uid,other:Number(ds.pairWith)});return;}
    if('swapWith' in ds){if(runCommand('swap',d.uid,Number(ds.swapWith))){closeDialog();sound('equip');update(true);}return;}
    if(ds.buy){
      const inDebrief=d.type==='debrief';
      if(runCommand('buy',ds.buy)){
        sound('combine');if(inDebrief){showDialog('debrief');update(true);}else renderLegacy();
        const shop=inDebrief?$('dialog-body'):$('legacy-page');
        const button=shop.querySelector('[data-buy="'+ds.buy+'"]:not(:disabled)')||shop.querySelector('[data-buy]:not(:disabled),[data-do="start"]');
        button?.focus({preventScroll:true});notify(E.perks[ds.buy].name+' upgraded for future runs.');
      }return;
    }
    if(ds.pace){if(runCommand('speed',Number(ds.pace))){closeDialog();update(true);}return;}
    if(el.id==='full-btn')toggleFullscreen();
    if(el.id==='profile-btn')showDialog(damagedSave?'recovery':'profile');
    if(el.id==='run-btn')onRunButton();
    if(el.id==='audio-btn'){toggleAudio();update(true);}
    if(el.id==='speed-btn')showDialog('pace');
    if(el.id==='recipe-btn'){guideTab='recipes';guideTier='all';query='';navigate('guide');}
    if(el.id==='close-btn')closeDialog();
  });
  $('pack-tier').addEventListener('change',e=>{packTier=e.target.value;packPage=0;renderPack(displayRun());});
  $('pack-search').addEventListener('input',event=>{packQuery=event.target.value;packPage=0;renderPack(displayRun());});
  $('pack-page').addEventListener('change',event=>{packPage=Number(event.target.value);renderPack(displayRun());});
  compactPack.addEventListener('change',()=>{packPage=0;if(!dragging)renderPack(displayRun());});
  document.addEventListener('input',event=>{
    if(event.target.id!=='guide-search')return;
    query=event.target.value;const caret=event.target.selectionStart;renderGuide();
    const input=$('guide-search');input.focus({preventScroll:true});if(caret!=null)input.setSelectionRange(caret,caret);
  });
  $('import-input').addEventListener('change',async event=>{
    const f=event.target.files[0];if(!f)return;
    try{
      if(f.size>1024*1024)throw Error('The file exceeds the 1 MB profile limit.');
      pendingImport=readProfile(await f.text());showDialog('confirm-import');
    }catch{pendingImport=null;showDialog('import-error',{message:'Choose a valid Bramblebound profile JSON file (up to 1 MB). Your current traveler’s record is intact.'});}
  });
  modal.addEventListener('touchend',e=>{
    const btn=e.target.closest('[data-do="drop"]');if(!btn||e.touches.length||e.changedTouches.length!==1)return;
    const t=e.changedTouches[0];if(!btn.contains(document.elementFromPoint(t.clientX,t.clientY)))return;
    e.preventDefault();btn.click();
  },{passive:false});
  modal.addEventListener('click',e=>{if(e.target!==modal)return;const b=modal.getBoundingClientRect();if(e.clientX<b.left||e.clientX>b.right||e.clientY<b.top||e.clientY>b.bottom)closeDialog();});
  modal.addEventListener('close',()=>{if(modal.open)return;dialogState=null;pendingImport=null;if(preDialogFocus?.isConnected)preDialogFocus.focus({preventScroll:true});preDialogFocus=null;});
  document.addEventListener('keydown',e=>{if(e.code==='Space'&&!titleOpen&&!e.repeat&&!modal.open&&page==='expedition'&&!e.target.closest('input,select,textarea,button,a')){e.preventDefault();onRunButton();}});
  document.addEventListener('keydown',e=>{
    if(e.key==='Escape'&&selectedUid!==null&&!modal.open&&page==='expedition'&&!dragging&&!e.defaultPrevented){
      selectionAction('clear');e.preventDefault();
    }
  });
  window.addEventListener('pagehide',save);
  document.addEventListener('visibilitychange',()=>{previousTime=null;if(document.hidden)save();});
  let lastHeld=null,heldFor=0,lastStillFrame='';
  function frame(now){
    const dt=previousTime===null?0:Math.min(.18,(now-previousTime)/1000);previousTime=now;
    const running=!paused(),r=profile.run;
    if(page==='expedition'&&!titleOpen&&!document.hidden){
      const v=displayRun(),animating=running||(!r&&!modal.open);
      if(animating){
        sceneClock+=dt*(prefs.motion?(r?.speed||.65):0);
        if(!r&&prefs.motion)demoDistance+=dt*5;
        effects=effects.filter(e=>(e.age+=dt)<1.1);heldFor=Math.max(0,heldFor-dt);
      }
      if(r&&running){
        const events=E.tick(profile,dt);
        encounterAge+=dt*(r.speed||1);
        for(const ev of events){
          if(ev.kind==='round'&&ev.pattern==='encounter')encounterAge=0;
          if(ev.kind==='kill')defeated=ev.enemy||C.enemies.find(foe=>foe.id===ev.enemyId);
          if(['attack','enemy','hit','kill','heal','milestone','status','loot','ability'].includes(ev.kind)){
            if(ev.kind==='attack'){lastHeld=ev.itemId||r.active.find(it=>C.items[it.id].element===ev.element)?.id;heldFor=.7;}
            if(prefs.motion||['enemy','hit','attack','heal','milestone','ability'].includes(ev.kind))effects.push({...ev,age:prefs.motion?0:.24});
          }
          sound(ev.kind==='attack'&&ev.label?.includes('WEAK')?'crit':ev.kind,ev.element);
        }
        if(r.draft.length||events.some(ev=>['loot','dead','round','level'].includes(ev.kind)))save();
        if(r.phase==='walk'||r.phase==='fight'){saveTimer+=dt;if(saveTimer>=3){saveTimer=0;save();}}
      }
      hudTimer+=dt;if(hudTimer>=.10||!structure){update();hudTimer=0;}
      const standing=r?.phase==='fight'&&r.enemy?Math.min(1,r.enemy.attackProgress):0;
      if(effects.length>120)effects=effects.filter((ev,i)=>ev.kind==='loot'||i>=effects.length-120);
      const deathFX=effects.find(fx=>fx.kind==='kill'&&fx.age<.52);

      const view={time:sceneClock,reducedMotion:!prefs.motion,distance:prefs.motion?(r?v.distance:demoDistance):0,region:v.region,round:v.round,phase:r?v.phase:'walk',
        heroHP:v.hp,heroMax:v.maxHp,heroX:.30,enemyX:.69+.008*standing,enemy:r?v.enemy:null,enemies:r?v.enemies:[],waveSize:v.waveSize,encounterAge:prefs.motion?encounterAge:1,defeated:deathFX?defeated:null,defeatAge:deathFX?.age??1,effects,paused:!running,
        heldId:heldFor>0&&lastHeld?lastHeld:v.active[0]?.id};
      const still=!animating||(!prefs.motion&&!effects.length);
      const stillFrame=still?[v.region,v.phase,v.round,v.hp,v.enemies.map(e=>e.uid+'-'+Math.ceil(e.hp)+'-'+Math.floor(e.attackProgress*8)).join(),view.heldId,view.distance,innerWidth,innerHeight].join('|'):'';
      if(!still||stillFrame!==lastStillFrame)WorldArt.draw($('world'),view,dt);
      lastStillFrame=stillFrame;
    }
    requestAnimationFrame(frame);
  }
  for(const key of ['rare','mythic'])document.documentElement.style.setProperty('--'+key+'-light',C.rarities[key].color);
  ink();fullLabel();S?.setEnabled(prefs.audio);update(true);
  titleMenu(true);
  if(!storageOK)$('save-line').textContent='Local save unavailable · export in Profile';
  if(damagedSave)showDialog('recovery');
  requestAnimationFrame(frame);
})();
