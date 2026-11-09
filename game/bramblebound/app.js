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
    trash:'M4 7h16M9 7V4h6v3M6 7l1 14h10l1-14M10 10v7m4-7v7',check:'M3 12l6 6L21 5',
    shield:'M12 2 3 6v6q0 6 9 10 9-4 9-10V6z',
    droplet:'M12 2C10 7 4 10 4 15a8 8 0 0 0 16 0c0-5-6-8-8-13z M8 15q0 4 4 4',
    snowflake:'M12 2v20M3 7l18 10M3 17 21 7M8 4l4 3 4-3M8 20l4-3 4 3M3 11l4 1-1-5M21 13l-4-1 1 5',
    bolt:'M13 2 4 14h7l-1 8 10-13h-8z',clock:'M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18z M12 7v6l4 2',
    chill:'M10 3v11a4 4 0 1 0 4 0V3z M18 5h3m-3 4h3M12 8v9',
    flame:'M13 2q1 6-3 9-2-2-2-4-6 5-4 11c3 7 13 6 16 0 3-6-3-9-3-10q0 5-4 6 2-7 0-12z',
    mountain:'m2 20 7-13 4 7 3-5 6 11z M7 11l2 2 2-2',
    ghost:'M4 21V10a8 8 0 0 1 16 0v11l-4-3-4 3-4-3z M8 10v2m8-2v2',
    echo:'M6 6a8 8 0 1 1-1 11M2 3l4 3 3-4M12 7v6l3 1',
    vial:'M8 2h8M9 2v7L4 18q-1 4 3 4h10q4 0 3-4l-5-9V2M7 15h10'
  };
  function icon(name){return '<svg aria-hidden="true" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round"><path d="'+(glyphs[name]||glyphs.spark)+'"/></svg>';}
  function ink(root=document){root.querySelectorAll('i[data-glyph]:empty').forEach(n=>n.innerHTML=icon(n.dataset.glyph));}
  const esc=value=>String(value).replace(/[&<>"']/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
  const number=n=>Number(n).toLocaleString('en-US',{maximumFractionDigits:1});
  const pad=n=>String(n).padStart(2,'0'), lv=n=>E.levelText(n);
  const defs=Object.values(C.items), allItems=r=>[...r.active,...r.pack], byUid=(r,uid)=>allItems(r).find(x=>x.uid===Number(uid));
  const elName=id=>C.elements[id]?.name||id;
  function enemyRelics(e){const counters={corrosive:['purify','cleanse'],entangling:['unbinding','cleanse'],hexed:['purify','cleanse'],scorching:['blastguard','thaw','cleanse'],freezing:['thaw','cleanse'],shocking:['insulated','cleanse'],bulwark:['wardbreaker']};const keys=e.titan?({leaf:['unbinding','wardbreaker'],fire:['blastguard'],frost:['thaw','wardbreaker'],storm:['insulated'],stone:['purify','wardbreaker'],spirit:['purify']}[e.element]||[]):counters[e.affliction]||({venom:['antivenom','cleanse'],volatile:['blastguard'],warded:['wardbreaker'],armored:['wardbreaker']}[e.trait]||[]);return [...new Set([...defs.filter(x=>!x.recipe&&keys.includes(x.counter)).slice(0,3),...defs.filter(x=>!x.recipe&&x.element===e.weak).slice(0,3)])];}
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
  let packPage=0,packQuery='',packTier='all',packCombinable=false,packSort='slots',displayedDraft='',packPages=1,campArea='heartwood',campGroveView='masteries';
  let selectedUid=null,partnerView=null,debriefedRun=null;
  const partners=new Map();
  const compactPack=window.matchMedia('(orientation:portrait), (max-height:680px)');
  const pageSize=()=>compactPack.matches?8:12;
  const modal=$('modal'); let dialogState=null,preDialogFocus=null;
  const CLOUD_NAME_KEY='bramblebound.cloud.name.v1';
  let cloudName='',cloudSecret='',cloudBusy=false,cloudTimer=null,cloudStamp=0,cloudLastRaw='',cloudMessage='',cloudAutoPaused=false,pendingCloud=null,cloudService=null,cloudGeneration=0,cloudForgetPending=false;
  try{cloudName=localStorage.getItem(CLOUD_NAME_KEY)||'';}catch{}
  function forgetCloudSession(){if(cloudForgetPending&&cloudService&&!cloudService.status().busy&&!cloudService.status().pending){cloudService.forget();cloudForgetPending=false;}}
  function cloud(){forgetCloudSession();return cloudService||(cloudService=window.BrambleCloud.create({validate:readProfile}));}
  function cloudStatus(){
    if(cloudBusy)return 'Contacting Firebase… Local progress is safe.';
    if(cloudMessage)return cloudMessage;
    if(cloudName&&cloudSecret&&!cloudAutoPaused)return 'Saved to Firebase · '+cloudName+(cloudStamp?' · '+new Date(cloudStamp).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}):'')+' · autosave connected';
    if(cloudName)return 'Remembered cloud journey: '+cloudName+'. Reopen with its password to connect.';
    return 'Cloud not connected · local autosave is active.';
  }
  function renderCloudStatus(){
    document.querySelectorAll('[data-cloud-status]').forEach(el=>el.textContent=cloudStatus());
    if(dialogState?.type==='cloud')for(const el of modal.querySelectorAll('input,button'))el.disabled=cloudBusy;
    $('close-btn').disabled=cloudBusy&&dialogState?.type==='cloud';
  }
  function rememberCloud(name,password,stamp,raw){
    cloudName=name;cloudSecret=password;cloudStamp=stamp;cloudLastRaw=raw;cloudMessage='';cloudAutoPaused=false;
    try{localStorage.setItem(CLOUD_NAME_KEY,name);}catch{}
    renderCloudStatus();
  }
  function pauseCloud(){cloudGeneration++;clearTimeout(cloudTimer);cloudTimer=null;cloudSecret='';cloudAutoPaused=true;pendingCloud=null;cloudForgetPending=true;forgetCloudSession();cloudMessage='Cloud autosave paused. Reopen a cloud journey or save under a new name.';renderCloudStatus();}
  function queueCloud(){
    if(!cloudSecret||cloudAutoPaused||cloudBusy||pendingCloud||damagedSave||cloudTimer||serialize()===cloudLastRaw)return;
    cloudTimer=setTimeout(()=>{cloudTimer=null;if(document.hidden||modal.open){queueCloud();return;}void uploadCloud(true);},30000);
  }
  async function uploadCloud(automatic=false,name=cloudName,password=cloudSecret){
    if(cloudBusy)return;
    if(damagedSave){if(!automatic)showDialog('recovery');return;}
    cloudBusy=true;cloudMessage='';clearTimeout(cloudTimer);cloudTimer=null;renderCloudStatus();
    const raw=serialize(),generation=cloudGeneration;
    try{
      const result=await cloud().save(name,password,raw);
      if(generation!==cloudGeneration)return;
      rememberCloud(result.name,password,result.updatedAtMs,raw);
      if(!automatic){cloudBusy=false;closeDialog();notify('Journey saved to Firebase: '+result.name);}
    }catch(error){
      if(generation!==cloudGeneration)return;
      cloudAutoPaused=true;cloudMessage=(error?.message||'Cloud save could not be confirmed.')+' Local progress is safe.';
      if(!automatic)notify('Cloud save not confirmed. Your local journey is unchanged.');
    }finally{cloudBusy=false;forgetCloudSession();renderCloudStatus();queueCloud();}
  }
  async function submitCloud(event){
    event.preventDefault();if(cloudBusy||dialogState?.type!=='cloud')return;
    const mode=dialogState.mode||'save',name=$('cloud-name').value,password=$('cloud-password').value;
    $('cloud-password').value='';
    if(mode==='save')return uploadCloud(false,name,password);
    cloudBusy=true;cloudMessage='';clearTimeout(cloudTimer);cloudTimer=null;renderCloudStatus();
    try{
      const result=await cloud().load(name,password),loaded=readProfile(result.raw);
      pendingCloud={result,loaded,password};cloudAutoPaused=true;cloudMessage='Cloud journey downloaded and checked. Local progress has not changed.';cloudBusy=false;showDialog('cloud-confirm');
    }catch(error){cloudAutoPaused=true;cloudMessage=(error?.message||'Cloud journey could not be opened.')+' Local progress is unchanged.';}
    finally{cloudBusy=false;renderCloudStatus();}
  }
  function restoreCloud(){
    if(!pendingCloud||cloudBusy)return;
    const {result,loaded,password}=pendingCloud;
    const raw=JSON.stringify({game:'bramblebound',format:1,profile:loaded.profile,settings:loaded.prefs,...(loaded.camp?{camp:loaded.camp}:{})});
    try{
      const prior=damagedSave||serialize();
      localStorage.setItem('bramblebound.cloud.backup.v1',prior);
      localStorage.setItem(KEY,raw);
    }catch{cloudMessage='The cloud journey is ready, but this device could not keep a local backup. Export your current profile and free storage before restoring.';renderCloudStatus();return;}
    applyProfile(loaded);pendingCloud=null;rememberCloud(result.name,password,result.updatedAtMs,raw);
    save();titleMenu(true);notify('Cloud journey reopened. Continue when you are ready.');
  }
  function applyProfile(loaded){
    ({profile,prefs}=loaded);starter=loaded.camp||freshKit();campChanged=!!loaded.camp;damagedSave=null;
    S?.setEnabled(prefs.audio,true);document.body.classList.toggle('reduced-art',!prefs.motion);
    closeDialog();manualPause=!!profile.run&&['walk','fight'].includes(profile.run.phase);
    resetPack();displayedDraft='';effects=[];defeated=null;encounterAge=1;lastHeld=null;heldFor=0;sceneClock=0;structure='';navigate('expedition');
  }
  function serialize(){return JSON.stringify({game:'bramblebound',format:1,profile,settings:prefs,...(!profile.run&&campChanged?{camp:starter}:{})});}
  function save(){
    if(damagedSave)return;
    try{localStorage.setItem(KEY,serialize());storageOK=true;}catch{storageOK=false;}
    $('save-line').innerHTML='<b class="save-dot"></b>'+ (storageOK?'Saved on this device':'Local save unavailable · export in Profile');
    queueCloud();
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
  function renderLegacy(){const pane=$('legacy-page'),paths=new Map([...pane.querySelectorAll('[data-camp-path]')].map(el=>[el.dataset.campPath,el.open]));pane.innerHTML=window.CampUI?CampUI.render(profile,campArea,campGroveView):J.legacy(profile);for(const el of pane.querySelectorAll('[data-camp-path]'))if(paths.has(el.dataset.campPath))el.open=paths.get(el.dataset.campPath);ink(pane);}
  function campResult(result,focusId){if(!result?.ok){notify(result?.message||'This preparation is unavailable.');return false;}save();structure='';renderLegacy();sound('combine');notify(result.message);if(focusId){const node=$('legacy-page').querySelector('[data-upgrade="'+focusId+'"]');node?.classList.add('echo-purchased');node?.querySelector('button')?.focus({preventScroll:true});}return true;}
  function start(){
    const ready=profile.run===null;
    const result=E.start(profile);if(!result.ok)return notify(result.message);
    if(ready&&campChanged&&(profile.legacy?.selectedKit??-1)<0){
      const r=profile.run,loadout=starter.run;
      if(window.Legacy?.applyPrepared)Legacy.applyPrepared(profile,r,loadout);
      else {r.active=loadout.active.map(it=>({...it}));r.pack=loadout.pack.map(it=>({...it}));r.nextUid=loadout.nextUid;}
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
  const comparisonStats=[
    ['damage','swords','Base direct damage per activation; includes level and build bonuses. Enemy defenses and conditional effects vary.'],
    ['lifesteal','droplet','Health stolen as a percentage of damage; includes healing bonuses.'],
    ['heal','heart','Potential HP restored per activation, before the maximum health cap.'],
    ['shield','shield','Potential shield per activation, before the shield capacity cap.'],
    ['stun','bolt','Stun duration per activation in seconds.'],
    ['freeze','snowflake','Freeze duration per activation in seconds.'],
    ['chill','chill','Chill duration per activation in seconds.'],
    ['cooldown','clock','Cooldown in seconds with the current build; a shorter cooldown is better.']
  ];
  const comparisonNumber=n=>{
    const absolute=Math.abs(n);
    return absolute>0&&absolute<.01?'<0.01':absolute.toLocaleString('en-US',{notation:absolute>=1000?'compact':'standard',maximumFractionDigits:absolute<1?2:1});
  };
  function comparisonDelta(key,glyph,description,value,baseline,source,target){
    const delta=value-baseline;if(Math.abs(delta)<.0001)return '';
    const percent=key==='lifesteal',seconds=['stun','freeze','chill','cooldown'].includes(key),scale=percent?100:1;
    const signed=(delta>0?'+':'−')+comparisonNumber(delta*scale)+(percent?'%':seconds?'s':'');
    const exact=n=>Number((n*scale).toFixed(3)).toLocaleString('en-US')+(percent?'%':seconds?'s':'');
    const hint=description+' '+C.items[source.id].name+' (Lv '+lv(source.level)+'): '+exact(value)+'. '+C.items[target.id].name+' (Lv '+lv(target.level)+'): '+exact(baseline)+'. Difference '+(delta>0?'+':'−')+exact(Math.abs(delta))+(percent?' (percentage points).':'.');
    return '<span class="comparison-delta '+((key==='cooldown'?delta<0:delta>0)?'gain':'loss')+'" data-stat="'+key+'" data-delta="'+delta+'" tabindex="0" title="'+esc(hint)+'" aria-label="'+esc(hint)+'">'+icon(glyph)+'<b>'+signed+'</b></span>';
  }
  function comparisonEffect(key,glyph,description,color){
    return '<span class="comparison-effect" data-effect="'+esc(key)+'" tabindex="0" title="'+esc(description)+'" aria-label="'+esc(description)+'"'+(color?' style="--effect:'+color+'"':'')+'>'+icon(glyph)+'</span>';
  }
  function renderComparison(r){
    const box=$('gear-comparison'),source=selectedUid!==null&&r.pack.find(it=>it.uid===selectedUid);
    box.hidden=!source;if(!source){box.dataset.signature='';return;}
    const p=equipmentProfile(),d=C.items[source.id],stats=E.itemComparison(p,source);
    const targets=r.active.slice().sort((a,b)=>a.slot-b.slot).map(target=>{
      const baseline=E.itemComparison(p,target),hint=d.name+' · Lv '+lv(source.level)+' compared with equipped '+C.items[target.id].name+' · Lv '+lv(target.level)+' in slot '+(target.slot+1);
      const deltas=comparisonStats.map(([key,glyph,description])=>comparisonDelta(key,glyph,description,stats[key]||0,baseline[key]||0,source,target)).join('');
      return '<div class="comparison-target" data-compare-uid="'+target.uid+'" data-compare-slot="'+target.slot+'" style="grid-column:'+(target.slot+1)+'" role="group" aria-label="'+esc(hint)+'"><span class="comparison-item" title="'+esc(hint)+'" aria-label="'+esc(hint)+'">'+A.svg(target.id,21)+'</span><div class="comparison-values">'+(deltas||'<span class="comparison-equal" title="Same compared stats" aria-label="Same compared stats">=</span>')+'</div></div>';
    }).join('');
    const styleIcons={heal:'heart',mend:'heart',shield:'shield',guard:'shield',stun:'bolt',shock:'bolt',freeze:'snowflake',chill:'chill',drain:'droplet',burn:'flame',poison:'vial',rapid:'speed',pierce:'target',strike:'swords'};
    const elementIcons={leaf:'sprout',fire:'flame',frost:'snowflake',storm:'bolt',stone:'mountain',spirit:'ghost'};
    const utility=Object.hasOwn(C.utilityStyles,d.style);
    let effects=comparisonEffect('style-'+d.style,styleIcons[d.style]||'spark',J.styleLabel(d.style)+'. '+C.effects[d.style],C.elements[d.element].color);
    effects+=comparisonEffect('element-'+d.element,elementIcons[d.element],elName(d.element)+'. '+(utility?'Affinity for this utility relic; offensive elemental hit effects do not activate.':J.elementText[d.element]),C.elements[d.element].color);
    if(d.ancient)effects+=comparisonEffect('ancient-'+d.aura,'spark','Ancient power. '+d.ancientPower+' Character aura. '+d.auraDescription,'#80ffe2');
    if(d.special)effects+=comparisonEffect('special-'+d.special,'spark',d.special+'. '+d.specialDescription,'#f0cc83');
    if(d.counter)effects+=comparisonEffect('counter-'+d.counter,'shield',d.counter+'. '+d.counterDescription,'#9cdec8');
    for(const level of [3,5,10])if(source.level>=level)effects+=comparisonEffect('milestone-'+level,utility?styleIcons[d.style]:level===3?'echo':level===5?'shield':'spark','Level '+level+'. '+d.milestones[level],'#d4c6ee');
    const content='<div class="comparison-title"><span aria-hidden="true">'+A.svg(source.id,19)+'</span><b id="comparison-name">'+esc(d.name)+' · Lv '+lv(source.level)+'</b></div><div class="comparison-grid">'+targets+'</div><div id="comparison-effects" aria-label="Selected relic effects">'+effects+'</div>';
    if(box.dataset.signature!==content){box.innerHTML=content;box.dataset.signature=content;}
  }
  function selectGear(uid){
    const r=displayRun(),item=byUid(r,uid);if(!item)return;
    if(r.phase==='dead')return showDialog('item',{uid});
    if(selectedUid!==null&&uid!==selectedUid&&(!r.pack.includes(item)||partnerView)&&E.previewCombine(equipmentProfile(),selectedUid,uid).ok){
      showDialog('confirm-pair',{uid:selectedUid,other:uid,fromGrid:true});return;
    }
    exitPartnerView();
    selectedUid=uid===selectedUid?null:uid;
    renderGear(r);
    if(!$('gear-comparison').hidden)$('gear-comparison').scrollIntoView({block:'nearest'});
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
    const tier=C.rarities[d.rarity].name;
    const stamp=tier?'<span class="gear-tier">'+tier+'</span>':num;
    const marker=result?'<span class="match-badge" aria-hidden="true">'+(verb==='Upgrade'?'+1':'✦')+'</span>':'';
    return '<button class="gear-slot'+state+'" '+attrs+' data-uid="'+it.uid+'" data-rarity="'+d.rarity+'" aria-pressed="'+selected+'" style="--el:'+C.elements[d.element].color+'" aria-label="'+name+(tier?', '+tier:'')+', level '+level+', '+place+' slot '+(index+1)+esc(hint)+'" title="'+name+(tier?' · '+tier:'')+' · Lv '+level+esc(hint)+'">'+stamp+marker+'<span class="level-tag">Lv '+level+'</span><span class="relic-picture">'+A.svg(it.id,58)+'</span><span class="slot-name">'+name+'</span>'+(equipped?'<span class="cd-text" data-clock="'+it.uid+'"></span><span class="cd-fill" data-fill="'+it.uid+'"></span>':'<span class="slot-type">'+elName(d.element)+' · '+J.styleLabel(d.style)+'</span>')+'</button>';
  }
  function resetPack(){clearSelection();packPage=0;packQuery='';packTier='all';packCombinable=false;packSort='slots';$('pack-tier').value='all';$('pack-search').value='';$('pack-sort').value='slots';$('pack-combinable').setAttribute('aria-pressed','false');}
  let combinableKey='',combinableUids=new Set();
  function combinables(r){
    const items=allItems(r),key=items.map(it=>[it.uid,it.id,it.level].join('/')).join('|');
    if(key!==combinableKey){
      combinableKey=key;combinableUids=new Set();
      for(let i=0;i<items.length;i++)for(let j=i+1;j<items.length;j++){
        if(E.previewPair(items[i],items[j]).ok){combinableUids.add(items[i].uid);combinableUids.add(items[j].uid);}
      }
    }
    return combinableUids;
  }
  function strength(it){
    const stats=E.itemComparison(equipmentProfile(),it);
    // Compare sustained output. Control uses an eight-potency-per-second
    // equivalent; the sort is a convenience, while the comparison retains units.
    return (stats.damage*(1+stats.lifesteal)+stats.heal+stats.shield+8*(stats.stun+stats.freeze))/Math.max(.15,stats.cooldown);
  }
  function renderPack(r){
    const size=pageSize(),{terms,stages}=J.parseRelicSearch(packQuery.replace(/\b(?:level|lv)\s*(\d+(?:\.\d+)?)/gi,'lv$1'));
    const possible=packCombinable?combinables(r):null;
    const scores=packSort==='strongest'?new Map(r.pack.map(it=>[it.uid,strength(it)])):null;
    const matches=(terms.length||stages.size||partnerView||packCombinable||packSort!=='slots'||packTier!=='all')?r.pack.filter(it=>{
      if(packTier!=='all'&&C.items[it.id].rarity!==packTier)return false;
      if(!J.matchesFusionStage(C.items[it.id],stages))return false;
      if(partnerView&&!partners.has(it.uid))return false;
      if(possible&&!possible.has(it.uid))return false;
      const fields=J.relicSearchText(C.items[it.id]);
      return terms.every(word=>/^lv\d/.test(word)?Number(word.slice(2))===it.level:/^p\d+$/.test(word)?Number(word.slice(1))===it.slot+1:fields.includes(word));
    }).sort(scores?(a,b)=>scores.get(b.uid)-scores.get(a.uid)||a.slot-b.slot:(a,b)=>a.slot-b.slot):null;
    const total=matches?matches.length:E.packCapacity;
    packPages=Math.max(1,Math.ceil(total/size));packPage=Math.min(Math.max(0,packPage),packPages-1);
    const from=packPage*size,to=Math.min(total,from+size);
    const indices=matches?matches.slice(from,to).map(it=>it.slot):Array.from({length:to-from},(_,i)=>from+i);
    const grid=$('pack');grid.style.setProperty('--pack-rows',size/4);
    grid.innerHTML=indices.length?indices.map(i=>gearSlot(r,'pack',i)).join(''):'<p class="pack-miss">'+(partnerView&&!terms.length&&!stages.size&&packTier==='all'&&partners.size?'All matches are in your equipped slots above.':'No matching relics in your backpack.')+'</p>';
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
    $('pack-page-status').textContent='Backpack page '+(packPage+1)+' of '+packPages;
    if(dragging?.moved)markPartners();
  }
  function renderGear(r){
    refreshSelection(r);renderSelection(r);renderComparison(r);
    $('gear-count').textContent=r.active.length+' / '+E.activeCapacity;
    $('equipped').innerHTML=Array.from({length:E.activeCapacity},(_,i)=>gearSlot(r,'active',i)).join('');
    renderPack(r);
    renderSetSummary(r);
  }
  function advancePage(step){packPage+=step;renderPack(displayRun());}
  function abilityRank(r,id){return r.skills[id]||0;}
  function skillTitle(u){return u.kind==='ability'?'Auto-cast ability':u.category;}
  function skillCard(r,id,mode){
    const u=P.upgrades[id],rank=abilityRank(r,id),next=mode==='draft',n=next?rank+1:Math.max(1,rank);
    return '<button class="skill-card" data-talent-rarity="'+(u.rarity||'common')+'" style="--el:'+C.elements[u.element].color+'" '+(next?'data-learn':'data-skill')+'="'+id+'">'+tile(u.icon,40)+'<span class="skill-copy"><small>'+esc((u.rarity||'common')+' · '+skillTitle(u))+(u.kind==='ability'?' · '+number(P.duration(r,id))+'s':'')+'</small><b>'+esc(u.name)+'</b><span>'+esc(P.describe(id,n))+'</span></span><span class="skill-rank">'+(next?(rank?'R'+rank+' → '+n:'RANK 1'):'RANK '+n)+'</span></button>';
  }
  function renderAbilities(r){
    const ids=P.abilities.filter(u=>abilityRank(r,u.id)>0);
    const el=$('ability-strip');el.hidden=!ids.length;
    el.innerHTML=ids.map(u=>'<button data-skill="'+u.id+'" class="ability-chip" style="--el:'+C.elements[u.element].color+'" title="'+esc(u.name)+'"><span class="ability-pic">'+A.svg(u.icon,24)+'</span><span><b>'+esc(u.name)+'</b><small data-ability-time="'+u.id+'"></small></span><span class="ability-fill" data-ability-fill="'+u.id+'"></span></button>').join('');
  }
  const elementGlyph={leaf:'sprout',fire:'flame',frost:'snowflake',storm:'bolt',stone:'mountain',spirit:'ghost'};
  function setIcons(elements){return '<span class="set-icons">'+elements.map(id=>'<span style="color:'+C.elements[id].color+'">'+icon(elementGlyph[id])+'</span>').join('')+'</span>';}
  function renderSetSummary(r){
    const sets=window.Synergies?.describe(r)||[],active=sets.filter(s=>s.active);
    $('synergies-count').textContent=active.length?active.map(s=>s.name+(s.tier>=3?' III':' II')).join(' · '):'0 active · discover 3 sets';
    $('synergies-btn').classList.toggle('has-set',!!active.length);
  }
  function renderProvisions(r){
    const summary=window.Provisions?.summary(r);if(!summary)return;
    const active=!!profile.run&&r.phase!=='dead';
    $('provisions-count').textContent=!active?'Collect and use during runs':summary.count+' / '+summary.capacity+' supplies'+(summary.cooldown>.05?' · recovering':'');
    const content=summary.items.filter(d=>d.count).map(d=>'<span title="'+esc(d.name)+'">'+Provisions.icon(d.id,20)+'<small>'+d.count+'</small></span>').join('');
    const preview=$('provisions-preview');if(preview.dataset.signature!==content){preview.innerHTML=content;preview.dataset.signature=content;}
    $('provisions-btn').classList.toggle('has-supplies',active&&summary.count>0);
  }
  function provisionsDialog(r){
    const summary=Provisions.summary(r),active=!!profile.run&&r.phase!=='dead';
    return '<h2 class="dialog-title">Trail pouch</h2><p class="body-copy">Gather supplies on the road. Open the pouch to pause and choose; using a supply returns you to the trail. Supplies stack here, separate from relics, and last for this expedition.</p><div class="pouch-meta"><span>'+summary.count+' / '+summary.capacity+' packed</span><span>Potency ×'+number(summary.quality)+'</span></div>'+
      (!active?'<p class="pouch-note">Begin an expedition to receive your starting supplies.</p>':summary.cooldown>.05?'<p class="pouch-note">Pouch recovering · '+number(summary.cooldown)+'s of trail time. Close the pouch and resume to recharge.</p>':'<p class="pouch-note">Choose your moment. Combat waits while this screen is open.</p>')+
      '<div class="provision-list">'+summary.items.map(d=>'<article class="provision-card" style="--el:'+C.elements[d.element].color+'"><span class="provision-art">'+Provisions.icon(d.id,46)+'</span><div><h3>'+esc(d.name)+'</h3><p>'+esc(d.description)+'</p><small>'+(d.count?d.count+' packed':'Find on the trail')+'</small></div><button class="outline compact" data-use-consumable="'+d.id+'" '+(!active||!d.usable?'disabled':'')+' title="'+esc(!active?'Begin an expedition first':d.reason||'Use '+d.name)+'">Use<span class="sr-only"> '+esc(d.name)+'</span></button></article>').join('')+'</div><p class="mini-note">Upgrade Provision Pockets and Field Alchemy at the Wayfinder’s Lodge to begin with more supplies and improve their potency. A shared recovery prevents rapid use; it advances only while the run is moving.</p>';
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
    if(!titleOpen&&pending&&!modal.open&&!dragging&&displayedDraft!==r.draft){showDialog('draft');}
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
    const titan=r.enemies.find(e=>e.titanId);
    el.classList.toggle('titan-encounter',!!titan);
    if(titan){el.innerHTML='<div><b>'+esc(titan.name)+'</b> · '+(titan.titan.phase===2?'Enraged':'Titan')+' · '+(r.enemies.length-1)+' escorts</div><button class="text-link" data-species="'+titan.id+'">Plan a counter →</button>';return;}
    if(r.enemies.length>1){el.innerHTML='<div><b>'+r.enemies.length+' enemies closing in</b> · '+esc(C.regions[r.region].name)+'</div><button class="text-link" data-dialog="foes">Inspect group →</button>';return;}
    if(e){el.innerHTML='<div><b>'+esc(e.name)+'</b> · '+esc(e.trait)+' · Weak to '+elName(e.weak)+'</div><button class="text-link" data-species="'+e.id+'">Inspect enemy →</button>';return;}
    el.innerHTML='<div><b>'+(!profile.run?'At the trailhead':'On the road')+'</b> · '+esc(C.regions[r.region].subtitle)+'</div><button class="text-link" data-dialog="help">Handbook →</button>';
  }
  let previousGear='';
  function update(force=false){
    if(page!=='expedition')return;
    const r=displayRun(),active=!!profile.run;
    const sig=[active,r.round,r.phase,r.enemy?.uid,r.enemies.length,r.enemies.find(e=>e.titanId)?.titan?.phase,r.level,r.draft.join(','),r.lastDrop?.round,r.lastDrop?.targetUid,r.nextUid,r.pack.length,r.active.length,manualPause,prefs.audio,r.speed,profile.points,profile.best].join(':');
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
      const gear=r.active.concat(r.pack).map(it=>[it.uid,it.id,it.level,it.slot].join('/')).join('|')+';'+r.active.map(it=>it.uid).join('/')+(packSort==='strongest'?JSON.stringify(r.skills):'');
      if(!dragging&&(gear!==previousGear||force||r.phase==='dead'&&selectedUid!==null)){renderGear(r);previousGear=gear;}
      renderFind(r);renderEncounter(r);renderAbilities(r);
    }
    renderProvisions(r);
    if(!titleOpen&&active&&r.phase==='dead'&&debriefedRun!==r&&!modal.open&&!dragging)showDialog('debrief');
    updateGrowth(r);
    if(selectedUid!==null)renderComparison(r);
    const titan=r.phase==='fight'?r.enemies.find(e=>e.titanId):null,banner=$('titan-banner');
    banner.hidden=!titan;document.body.classList.toggle('titan-battle',!!titan);
    if(titan){
      const def=Titans.definitions.find(d=>d.id===titan.titanId),state=titan.titan,warning=state.warning,move=warning?def.moves[warning.move]:null;
      banner.style.setProperty('--titan-tone',def.color);banner.dataset.phase=state.phase;banner.classList.toggle('charging',!!warning);
      $('titan-name').textContent=def.name;$('titan-phase').textContent='ROUND '+r.round+' · TITAN · '+(state.phase===2?'PHASE II · ENRAGED':'PHASE I');
      $('titan-inspect').dataset.species=def.id;
      const warningText=move?move.name+' · '+move.description:def.title+' · Signature attack gathering';
      if($('titan-telegraph').textContent!==warningText)$('titan-telegraph').textContent=warningText;
      $('titan-countdown').textContent=warning?Math.max(0,warning.left).toFixed(1)+'s':'';
      $('titan-charge').style.width=(warning?100*(1-warning.left/warning.duration):0)+'%';
    }
    $('hp-num').textContent=Math.ceil(r.hp)+' / '+r.maxHp;$('hp-bar').style.width=100*r.hp/r.maxHp+'%';
    const afflicted=Object.entries(r.ailments||{}).filter(([,n])=>n>.1).map(([key])=>({corrosion:'Corroded',snare:'Entangled',hex:'Hexed',burn:'Scorched',chill:'Frostbitten',shock:'Static'}[key]));
    $('hero-sub').textContent=afflicted.length?afflicted.join(' · '):r.shield>.1?'Shield +'+Math.ceil(r.shield):'Trailwalker · '+(active?r.foes:0)+' defeated'+(r.active.some(it=>C.items[it.id].ancient)?' · Ancient awakened':'');
    const e=r.phase==='fight'?r.enemy:null;
    $('enemy-name').textContent=titan?titan.name:e?(r.enemies.length>1?r.enemies.length+' enemies':e.name):r.phase==='dead'?'Expedition ended':'Open trail';
    const hpTotal=r.enemies.reduce((n,foe)=>n+foe.hp,0),maxTotal=r.enemies.reduce((n,foe)=>n+foe.maxHp,0);
    $('enemy-num').textContent=e?Math.ceil(hpTotal)+' / '+maxTotal:'';
    $('enemy-bar').style.width=e?100*hpTotal/maxTotal+'%':r.phase==='walk'&&active?100*(1-r.walkLeft/E.walkDuration)+'%':'0%';
    $('enemy-sub').textContent=titan?(titan.titan.phase===2?'Enraged · ':'Titan · ')+'Weak to '+elName(titan.weak)+' · Signature powers in dossier':e?(e.affliction?e.affliction+' · Inspect to plan a counter':r.enemies.length>1?'Lead foe · '+e.name:'Weak to '+elName(e.weak)+' · Resists '+elName(e.resist)):r.phase==='dead'?'Resting at camp':active?'Approaching encounter':'The road is quiet';
    for(const it of r.active){const clock=document.querySelector('[data-clock="'+it.uid+'"]'),bar=document.querySelector('[data-fill="'+it.uid+'"]');if(clock)clock.textContent=it.cd>.04&&e?it.cd.toFixed(1)+'s':'Ready';if(bar)bar.style.transform='scaleX('+(it.cd>.04&&e?Math.max(0,1-it.cd/E.itemCooldown(it,r)):1)+')';}
  }
  function closeDialog(){if(cloudBusy&&dialogState?.type==='cloud')return;if(dialogState?.type==='cloud-confirm'){pendingCloud=null;cloudAutoPaused=true;}if(modal.open)modal.close();dialogState=null;pendingImport=null;}
  function showDialog(type,data={}){
    if(!modal.open)preDialogFocus=document.activeElement;
    dialogState={type,...data};let html='',kicker='FIELD NOTES',portrait=null;
    const r=displayRun(),live=r.phase!=='dead';
    if(type==='cloud'){
      const mode=data.mode||'save';kicker='FIREBASE CLOUD JOURNEY';
      html='<h2 class="dialog-title">'+(mode==='load'?'Reopen a cloud journey':'Save to Firebase')+'</h2><p class="body-copy">Use a save name and password on any device, just like Arcane Wilds. Your complete profile, current expedition, starting kit and settings travel together.</p><div class="cloud-modes"><button class="outline compact" data-cloud-mode="save" aria-pressed="'+(mode==='save')+'">Save journey</button><button class="outline compact" data-cloud-mode="load" aria-pressed="'+(mode==='load')+'">Reopen journey</button></div><form id="cloud-form"><label>Save name<input id="cloud-name" name="saveName" maxlength="48" minlength="2" required autocomplete="username" value="'+esc(data.name??cloudName)+'" placeholder="My Bramblebound journey"></label><label>Save password<input id="cloud-password" name="savePassword" type="password" minlength="4" required autocomplete="'+(mode==='load'?'current-password':'new-password')+'" placeholder="At least 4 characters"></label><p class="cloud-status" data-cloud-status role="status" aria-live="polite"></p><button id="cloud-submit" class="primary" type="submit">'+(mode==='load'?'Reopen from Firebase':'Save to Firebase')+'</button></form><p class="mini-note">The journey is encrypted before upload. Keep the password to reopen it; Bramblebound remembers only the save name on this device. Once connected, cloud autosaves run about every 30 seconds. Local autosaves continue.</p><p class="mini-note">To resume an existing cloud slot, reopen it first. Use a new name to keep this device’s journey separately.</p>';
    }else if(type==='cloud-confirm'&&pendingCloud){
      const p=pendingCloud.loaded.profile;kicker='CLOUD JOURNEY READY';
      html='<h2 class="dialog-title">Reopen '+esc(pendingCloud.result.name)+'?</h2><p class="body-copy">This replaces the journey on this device with the cloud journey. A local backup is kept. You can export your current profile first.</p><div class="stat-row"><span class="stat-pill">Best round '+p.best+'</span><span class="stat-pill">'+p.points+' trail seeds</span><span class="stat-pill">'+(p.run?'Round '+p.run.round+' · '+C.regions[p.run.region].name:'At camp')+'</span></div><p class="cloud-status" data-cloud-status role="status"></p><div class="dialog-actions"><button class="primary" data-do="cloud-restore">Reopen this journey</button><button class="outline" data-do="export">Export current</button><button class="flat" data-do="cloud-cancel">Keep local journey</button></div>';
    }else if(type==='debrief'){
      if(r.phase!=='dead')return closeDialog();
      debriefedRun=r;kicker='EXPEDITION COMPLETE';html=window.CampUI?CampUI.debrief(profile):J.debrief(profile);
    }else if(type==='draft'){
      if(!r.draft.length)return showDialog('build');
      displayedDraft=r.draft;kicker='LEVEL '+r.level+' / EXPEDITION UPGRADE';
      html='<h2 class="dialog-title">Choose an upgrade</h2><p class="body-copy">Pip reached level '+r.level+'. Pick one for this expedition. Picking it again adds another rank.</p><div class="skill-list">'+r.draft.map(id=>'<div class="skill-choice">'+skillCard(r,id,'draft')+((r.banishes||r.echoesLeft)?'<div class="draft-side-tools"><button type="button" data-draft-banish="'+id+'" '+(!r.banishes?'disabled':'')+' aria-label="Banish '+esc(P.upgrades[id].name)+'">Banish</button><button type="button" data-draft-echo="'+id+'" '+(!r.echoesLeft?'disabled':'')+' aria-label="Echo '+esc(P.upgrades[id].name)+'">Echo</button></div>':'')+'</div>').join('')+'</div><div class="draft-tools"><button class="outline compact" data-draft-reroll '+(!r.draftRerolls?'disabled':'')+'>Reroll · '+(r.draftRerolls||0)+' remaining</button><small>'+(r.banishes||0)+' banishes · '+(r.echoesLeft||0)+' echoes. Echo preserves an unchosen talent for two drafts. Combat waits while you choose.</small></div>';
    }else if(type==='provisions'){
      kicker='SUPPLIES FOR THE ROAD';html=provisionsDialog(r);
    }else if(type==='synergies'){
      kicker='ELEMENTAL SETS';const sets=Synergies.describe(r);
      html='<h2 class="dialog-title">Build a reaction</h2><p class="body-copy">Only equipped relics count. Equip both elements in a pair to awaken its set. A third relic from that pair strengthens it, or split your four slots between two sets.</p><div class="set-list">'+sets.map(s=>'<article class="set-card '+(s.active?'active':'')+'">'+setIcons(s.elements)+'<div class="set-title"><h3>'+esc(s.name)+'</h3><span>'+ (s.active?'ACTIVE · '+s.count+' PIECES':s.count+' PIECES')+'</span></div><p class="set-requirement">'+esc(s.requirement)+'</p><p>'+esc(s.description)+'</p><div class="set-bonus">'+esc(s.bonus)+'</div></article>').join('')+'</div><p class="mini-note">Carried relics do not activate sets. Healing, shielding, freeze and stun relics keep their support roles.</p>';
    }else if(type==='build'){
      kicker='PIP’S EXPEDITION BUILD';const entries=Object.keys(r.skills).filter(id=>r.skills[id]>0);
      html='<h2 class="dialog-title">Level '+r.level+' trailwalker</h2><p class="body-copy">Travel and combat earn XP. At each level, choose from your talent draft. Your ranks last for this run.</p><div class="stat-row"><span class="stat-pill">'+Math.floor(r.xp)+' / '+E.xpNeeded(r.level)+' XP</span><span class="stat-pill">'+r.foes+' foes defeated</span></div>';
      html+='<button class="text-link" data-dialog="synergies">'+icon('spark')+'View elemental sets →</button>';
      if(r.draft.length)html+='<button data-dialog="draft" class="primary">Choose your upgrade →</button>';
      html+=entries.length?'<div class="skill-list">'+entries.map(id=>skillCard(r,id,'owned')).join('')+'</div>':'<p class="empty-note">Your first level-up opens a talent draft.</p>';
      html+='<button class="text-link" data-do="browse-skills">'+icon('book')+'Browse all '+Object.keys(P.upgrades).length+' upgrades →</button>';
    }else if(type==='skill'){
      const u=P.upgrades[data.id];if(!u)return closeDialog();const rank=abilityRank(r,u.id);
      kicker=skillTitle(u).toUpperCase();
      html='<div class="dialog-lead">'+tile(u.icon,52)+'<div><h2 class="dialog-title">'+esc(u.name)+'</h2><div class="eyebrow">'+elName(u.element)+' · '+(rank?'Rank '+rank:'Unlearned')+'</div></div></div><p class="body-copy">'+esc(u.description)+'</p>';
      if(rank)html+='<div class="subhead">Current rank '+rank+'</div><p class="body-copy">'+esc(P.describe(u.id,rank))+'</p>';
      if(!u.maxRank||rank<u.maxRank)html+='<div class="subhead">Rank '+(rank+1)+'</div><p class="body-copy">'+esc(P.describe(u.id,rank+1))+'</p>';else html+='<p class="body-copy">Fully awakened for this expedition.</p>';
      if(u.kind==='ability'&&rank)html+='<div class="stat-row"><span class="stat-pill">'+number(P.duration(r,u.id))+'s cooldown</span></div>';
      html+='<button class="text-link" data-dialog="build">View expedition build →</button>';
    }else if(type==='foes'){
      kicker='ENCOUNTER / ROUND '+r.round;html='<h2 class="dialog-title">Enemies on the trail</h2><div class="pair-list">'+r.enemies.map(e=>'<button class="pair-option" data-species="'+e.id+'"><span><b>'+esc(e.name)+'</b><small>'+Math.ceil(e.hp)+' / '+e.maxHp+' HP · Weak to '+elName(e.weak)+' · '+Math.floor(100*e.attackProgress)+'% attack charge</small></span></button>').join('')+'</div>';
    }else if(['item','catalog'].includes(type)){
      const it=type==='item'?byUid(r,data.uid):{id:data.id,level:1},d=C.items[it?.id];if(!d)return closeDialog();
      const held=type==='item'&&r.active.some(i=>i.uid===it.uid);
      kicker=type==='catalog'?'RELIC DOSSIER':held?'EQUIPPED RELIC':'BACKPACK RELIC';
      html='<div class="dialog-lead">'+tile(d.id,55)+'<div style="--el:'+C.elements[d.element].color+'"><h2 class="dialog-title">'+esc(d.name)+'</h2><div class="eyebrow">'+elName(d.element)+' / '+esc(J.styleLabel(d.style))+' / Level '+lv(it.level)+'</div></div></div><p class="body-copy">'+esc(d.ancient?'An artifact older than the road itself. Forge two Mythics to awaken its power.':d.description)+'</p><div class="stat-row"><span class="stat-pill">'+esc(J.relicStat(equipmentProfile(),it))+'</span><span class="stat-pill">'+Number(E.itemCooldown(it,r).toFixed(2))+'s cooldown</span></div>';
      html+=J.tierBadge(d,true)+'<div class="stat-row"><span class="stat-pill">Rarity power ×'+C.rarities[d.rarity].powerScale+' · included above</span></div>'+J.relicEffects(d);
      if(d.recipe)html+='<div class="subhead">'+esc(J.fusionStage(d))+' recipe</div><div class="chip-row">'+d.recipe.map(id=>'<button class="outline compact" data-catalog="'+id+'">'+esc(C.items[id].name)+'</button>').join(' + ')+'</div>';
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
      const out=C.items[res.id];kicker=out.ancient&&a.id!==b.id?'ANCIENT AWAKENING':a.id===b.id?'LEVEL UPGRADE':'NEW FUSION';html='<div class="dialog-lead">'+tile(out.id,56)+'<div><h2 class="dialog-title">'+esc(out.name)+'</h2><span class="level-tag">LEVEL '+lv(res.level)+'</span> '+J.tierBadge(out)+'</div></div><p class="body-copy">'+esc(out.description)+'</p><div class="stat-row"><span class="stat-pill">'+esc(J.relicStat(equipmentProfile(),{id:out.id,level:res.level}))+'</span><span class="stat-pill">'+out.cooldown+'s cooldown</span></div><p class="body-copy">'+(out.ancient&&a.id!==b.id?'Two Mythics awaken one Ancient. ':'')+'Uses '+esc(C.items[a.id].name)+' (Lv '+lv(a.level)+') and '+esc(C.items[b.id].name)+' (Lv '+lv(b.level)+'). The result goes into your '+(r.active.includes(a)||r.active.includes(b)?'equipped slots':'backpack')+'.</p><div class="dialog-actions"><button class="primary" data-do="forge">'+icon('merge')+(out.ancient&&a.id!==b.id?'Awaken Ancient':a.id===b.id?'Upgrade item':'Fuse items')+'</button>'+(data.fromGrid?'<button class="outline" data-do="cancel-pair">Cancel</button>':'<button class="outline" data-gear="combine">Back</button>')+'</div>';
    }else if(type==='discard'){
      const it=byUid(r,data.uid);if(!it)return closeDialog();
      kicker='BACKPACK';html='<h2 class="dialog-title">Discard '+esc(C.items[it.id].name)+'?</h2><p class="body-copy">Pip will leave this level '+lv(it.level)+' item on the road.</p><div class="dialog-actions"><button class="danger" data-do="drop">Discard item</button><button class="outline" data-uid="'+it.uid+'">Keep item</button></div>';
    }else if(type==='species'){
      const e=C.enemies.find(x=>x.id===data.id);if(!e)return;
      kicker=e.titan?'TEN-ROUND TITAN':e.boss?'GUARDIAN DOSSIER':'CREATURE DOSSIER';portrait=e;
      html='<div class="dialog-lead"><canvas id="detail-portrait" class="dialog-portrait" aria-hidden="true"></canvas><div><h2 class="dialog-title">'+esc(e.name)+'</h2><div class="eyebrow">'+esc(e.titan?'Appears every 60 rounds · First encounter '+((Titans.definitions.indexOf(e)+1)*10):C.regions[e.region].name)+'</div></div></div><div class="stat-row"><span class="stat-pill">Weak to '+elName(e.weak)+' · 1.68×</span><span class="stat-pill">Resists '+elName(e.resist)+' · 0.60×</span></div><div class="subhead">'+esc(e.trait)+'</div><p class="body-copy">'+esc(e.titan?e.description:J.traitText[e.trait])+'</p>'+(e.affliction?'<div class="subhead">Enemy effect · '+esc(e.affliction)+'</div><p class="body-copy">'+esc(J.afflictionText[e.affliction])+'</p>':'')+'<div class="subhead">Relics to try</div><div class="chip-row">'+enemyRelics(e).map(d=>'<button class="outline compact" data-catalog="'+d.id+'">'+esc(d.name)+'</button>').join('')+'</div>';
      if(e.titan)html+='<section class="titan-dossier"><div class="subhead">Titan signature powers</div>'+e.moves.map(m=>'<article><b>'+esc(m.name)+'</b><p class="body-copy">'+esc(m.description)+'</p></article>').join('')+'<div class="subhead">Phase II · Enraged at 50% health</div><p class="body-copy">Signature powers charge more quickly and hit harder. Warnings pause while the Titan is frozen or stunned. Clear the Titan and its two escorts to recover 25% max HP. Titans drop an Epic cache with a 10% chance to upgrade to an eligible base Mythic. Every fiftieth round guarantees a Mythic to keep the Ancient forge within reach.</p></section>';
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
        ['On the road','Pip starts alone against one foe. The first guardian arrives with two escorts on wave 4. Groups reach four foes on wave 7 and five on wave 11. Every fourth wave is a guardian. Every tenth round brings a giant Titan with two escorts, two signature powers and an enraged second phase. Six Titans rotate, growing stronger on later cycles. Read its warning and dossier to plan your build.'],
        ['Elements with a purpose','Leaf poisons and roots; Fire burns and ignites poison; Frost chills and freezes; Storm disrupts and chains; Stone fractures defenses; Spirit marks foes and returns protective energy. The field guide explains the timings and cross-element reactions.'],
        ['Three elemental sets','Equip both elements of Fire/Frost, Leaf/Storm, or Stone/Spirit to awaken Emberglass, Bramble Circuit, or Soulstone. A third relic from that pair improves the reaction. Split four slots into two pairs for two active sets. Open Element sets below your equipment to see exact requirements and bonuses.'],
        ['Supplies at the right moment','Open Trail pouch below the scene to pause and choose a supply. Moss Tonic heals and cleanses, Bark Ward shields, Ember Bomb hits the group, Frost Bell freezes it, and Quicksilver recharges relics and abilities. Using one returns to the road. Supplies have their own pouch and a shared 1.25-second recovery that advances while the run moves. Gather more after battles; upgrade Provision Pockets and Field Alchemy at the Wayfinder’s Lodge for more and better starting supplies.'],
        ['Quick pack controls','Combinable shows relics with a valid partner anywhere in your kit. Strongest first ranks sustained damage or utility and keeps your saved slots intact. Combine either control with search and rarity. Swipe left or right across filled or empty pack slots to change pages. On touch, hold a relic briefly before dragging it to equip, swap or fuse.'],
        ['Support and control','Healing-only relics restore HP, shield-only relics build protection, stun-only relics interrupt a foe, and freeze-only relics encase a foe in ice. Stun and freeze stop enemy attack charge for their listed duration. These roles deal zero damage and do not trigger elemental hit effects. Pair them with an offensive relic to finish the fight.'],
        ['Pack a counter','New backpacks include a Dewglass Vial. Equip it to cleanse ailments whenever it activates. Other relics counter venom, static, frostbite, entanglement, hexes, corrosion, explosive death bursts and wards. Inspect a foe to see its effect and recommended counter relics.'],
        ['Signature relics','The expanded road adds twelve special weapon effects, including ricochets, executions, ailment ruptures, poison blooms and delayed aftershocks. Overload stores charge on its own relic and bursts every third activation. Search the field guide by signature or counter name.'],
        ['Pack for the next fight','Four equipped relics activate on independent cooldowns. Your backpack holds 400 more. Select a backpack relic to compare its damage, lifesteal and effects with every equipped relic; green numbers improve that stat and red numbers reduce it. Hover or focus an icon for details. Search by name, element, or effect, or jump to a page by slot number. Drag an item to an empty slot to move it. Drop onto a matching recipe to fuse, or onto an unrelated item to swap. Select a relic to light up every valid partner. Choose Matches, then tap a glowing partner to preview a fusion, or choose Details to equip or discard.'],
        ['Trail finds','Every defeated enemy can drop a relic into your backpack during the fight. The chance of at least one drop across a full wave is 24%, or 48% in a guardian wave. When the pack is full, a matching copy at the same level upgrades automatically. Other finds stay on the road.'],
        ['Leveling up','Time on the trail and defeated enemies earn XP. At each level, choose one of three random upgrades. Passive bonuses stack, and learned abilities cast automatically on separate cooldowns. Expedition ranks reset when you start another run.'],
        ['Group encounters','Enemies approach together and charge their attacks independently. A green bar shows health, a gold bar shows windup, and a red warning marks an imminent strike. Area attacks can hit multiple foes.'],
        ['Relic rarity','Common, Uncommon, Rare, Epic, Mythic and Ancient gear has ×1, ×1.12, ×1.28, ×1.5, ×1.8 and ×2.6 rarity power. Ancient relics are forged only by consuming two specific Mythics; they gain one level above the higher ingredient and awaken unique powers plus visible character auras. Stronger weapons and richer special effects sit in higher tiers. High tiers drop less often; guardian waves and later regions improve your odds. Rarity is rolled before duplicate or ingredient bonuses. The atlas lists exact local chances and the field guide filters every tier.'],
        ['At the forge','Two matching copies at the same level gain one level, with no final tier. Every listed fusion gains one level above its highest-level ingredient. Level 2 + level 3 makes level 4; level 2.5 + level 3.5 makes level 4.5. Fuse crafted relics again for second-stage and third-stage upgrades. Search the fusion guide by stage or ingredient to follow a crafting chain.'],
        ['Growing stronger','Offensive relics gain an echo strike at level 3, a shield ring at level 5 and a damage nova at level 10. Healing, shield, stun and freeze relics gain stronger utility at those milestones and continue to deal zero damage. Inspect a relic for its level effects.'],
        ['After a fall','Earn one seed for setting out, one per cleared wave, and one extra per four defeats. Spend seeds on health, damage, or replay speed in Legacy.'],
        ['Take your time','Gear details, the field guide, and settings pause battle. Space pauses or resumes on the road. Profiles save locally. Use Firebase cloud saves from the title menu or settings to reopen a password-protected journey on another device. Exported profiles remain available as backups.']
      ].map(([t,p])=>'<div class="dialog-section"><h3>'+t+'</h3><p class="body-copy">'+p+'</p></div>').join('');
    }else if(type==='camp-respec'){
      const spend=Object.values(Legacy.ensure(profile).spent).reduce((a,b)=>a+b,0);kicker='A NEW PATH THROUGH THE CAMP';html='<h2 class="dialog-title">Redistribute your legacy?</h2><p class="body-copy">Refund '+number(spend)+' Trail Seeds and rebuild permanent progression. Camp unlocks, discoveries, saved kit designs and expedition records stay with you. All purchased ranks, masteries, constellations and Oaths are refunded; Oaths and preferences are unequipped.</p><div class="dialog-actions"><button class="primary" data-do="camp-respec-confirm">Refund '+number(spend)+' seeds</button><button class="outline" data-nav="legacy">Keep this legacy</button></div>';
    }else if(type==='profile'){
      kicker='TRAVELER’S RECORD';html='<h2 class="dialog-title">Profile & settings</h2><p class="body-copy">'+(storageOK?'Progress saves on this device. Export a copy to move Pip to another browser.':'This browser cannot save locally. Export a profile to keep your progress.')+'</p><div class="stat-row"><span class="stat-pill">Best round '+profile.best+'</span><span class="stat-pill">'+profile.deaths+' completed runs</span></div>'+
      '<div class="action-row"><button class="outline" data-do="export">Export profile</button><button class="outline" data-do="import">Import profile</button></div><section class="dialog-section"><h3>Firebase cloud journey</h3><p class="cloud-status" data-cloud-status role="status"></p><div class="action-row"><button class="primary" data-do="cloud">Save to Firebase</button><button class="outline" data-do="cloud-open">Reopen cloud journey</button><button class="flat" data-do="cloud-forget">Disconnect cloud</button></div><p class="mini-note">Save name + password work across devices. Local progress remains available offline.</p></section><div class="dialog-section"><div class="pref-row"><div><b>Sound effects</b><small>Elemental attacks and trail chimes.</small></div><button class="outline compact" data-do="sound" aria-pressed="'+prefs.audio+'">'+(prefs.audio?'On':'Off')+'</button></div><div class="pref-row"><div><b>Ambient motion</b><small>Moving scenery, particles, and idle animation.</small></div><button class="outline compact" data-do="motion" aria-pressed="'+prefs.motion+'">'+(prefs.motion?'On':'Reduced')+'</button></div></div><button class="text-link" data-dialog="help">'+icon('book')+'Read the trail handbook →</button>';
    }else if(type==='confirm-import'&&pendingImport){
      const p=pendingImport.profile;kicker='PROFILE TRANSFER';html='<h2 class="dialog-title">Use this profile?</h2><p class="body-copy">Replace the current profile with this traveler’s record. You can export the current one first.</p><div class="stat-row"><span class="stat-pill">Best round '+p.best+'</span><span class="stat-pill">'+p.points+' trail seeds</span><span class="stat-pill">'+(p.run?'Round '+p.run.round+' · '+p.run.phase:'At camp')+'</span></div><div class="dialog-actions"><button class="primary" data-do="confirm-import">Import profile</button><button class="outline" data-do="export">Export current</button><button class="flat" data-dialog="profile">Cancel</button></div>';
    }else if(type==='import-error'){
      kicker='PROFILE TRANSFER';html='<h2 class="dialog-title">Couldn’t import profile</h2><p class="body-copy">'+esc(data.message)+'</p><div class="dialog-actions"><button class="outline" data-do="import">Choose another file</button><button class="flat" data-dialog="profile">Back</button></div>';
    }else if(type==='recovery'){
      kicker='SAVE RECOVERY';html='<h2 class="dialog-title">Local profile could not load</h2><p class="body-copy">Download the original data before starting a fresh profile, or import an exported save.</p><div class="dialog-actions"><button class="outline" data-do="recover">Download original</button><button class="outline" data-do="import">Import profile</button><button class="outline" data-do="cloud-open">Reopen cloud journey</button><button class="danger" data-do="fresh">Start fresh</button></div>';
    }else return;
    $('dialog-kicker').textContent=kicker;$('dialog-body').innerHTML=html;ink($('dialog-body'));modal.classList.toggle('level-dialog',type==='draft');modal.classList.toggle('atlas-dialog',type==='region'||type==='route');modal.classList.toggle('debrief-dialog',type==='debrief');const title=modal.querySelector('.dialog-title');if(title){title.id='dialog-title';modal.setAttribute('aria-labelledby','dialog-title');}
    if(!modal.open)modal.showModal();modal.scrollTop=0;
    if(type==='region')WorldArt.regionPortrait($('region-art'),data.index);
    if(portrait)WorldArt.portrait($('detail-portrait'),portrait.species,portrait.color,portrait.boss,portrait.titan?portrait.id:null);
    renderCloudStatus();
  }
  function download(raw,file){
    const url=URL.createObjectURL(new Blob([raw],{type:'application/json'})),a=document.createElement('a');
    a.href=url;a.download=file;document.body.append(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(url),3000);
  }
  function toggleAudio(){prefs.audio=!prefs.audio;S?.setEnabled(prefs.audio,true);if(prefs.audio)sound('equip');save();structure='';}
  function handleAction(name){
    const d=dialogState||{};
    if(name==='cloud'){if(cloudName&&cloudSecret&&!cloudAutoPaused)return void uploadCloud();showDialog('cloud',{mode:'save'});return;}
    if(name==='cloud-open'){showDialog('cloud',{mode:'load'});return;}
    if(name==='cloud-restore'){restoreCloud();return;}
    if(name==='cloud-cancel'){pauseCloud();cloudMessage='Cloud load cancelled. Your local journey is unchanged.';showDialog('profile');return;}
    if(name==='cloud-forget'){pauseCloud();cloudName='';cloudStamp=0;cloudMessage='Cloud disconnected. Your local journey and Firebase slot are unchanged.';try{localStorage.removeItem(CLOUD_NAME_KEY);}catch{}renderCloudStatus();notify('Cloud disconnected.');return;}
    if(name==='start')return start();
    if(name==='camp-respec-confirm'){const result=Legacy.respec(profile);closeDialog();campResult(result);return;}
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
      const loaded=pendingImport;pauseCloud();applyProfile(loaded);save();notify('Profile imported.');return;
    }
    if(name==='recover'&&damagedSave){download(damagedSave,'Bramblebound-recovery.json');return;}
    if(name==='fresh'){pauseCloud();damagedSave=null;profile=E.freshProfile();starter=freshKit();campChanged=false;prefs=defaultPrefs();closeDialog();save();navigate('expedition');return;}
  }
  let dragging=null,blockDragClick=false,packTouch=null;
  const dragNote='Swipe pages · Hold to drag · Select a relic to find matches.';
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
  function clearPackTouch(){if(packTouch)clearTimeout(packTouch.hold);packTouch=null;}
  $('pack').addEventListener('pointerdown',e=>{
    if(e.pointerType==='mouse'||!e.isPrimary||e.button!==0||modal.open||dragging)return;
    clearPackTouch();blockDragClick=false;
    const node=e.target.closest('.gear-slot[data-uid]');
    const gesture={pid:e.pointerId,x:e.clientX,y:e.clientY,lastX:e.clientX,lastY:e.clientY,swiping:false,hold:null};packTouch=gesture;
    if(node&&displayRun().phase!=='dead')gesture.hold=setTimeout(()=>{
      if(packTouch!==gesture)return;
      packTouch=null;dragging={pid:gesture.pid,uid:Number(node.dataset.uid),node,x:gesture.x,y:gesture.y,moved:false,chip:null,target:null};
    },280);
  });
  document.addEventListener('pointermove',e=>{
    const g=packTouch;if(!g||g.pid!==e.pointerId)return;
    g.lastX=e.clientX;g.lastY=e.clientY;
    const dx=e.clientX-g.x,dy=e.clientY-g.y;
    if(Math.hypot(dx,dy)>9)clearTimeout(g.hold);
    if(!g.swiping&&Math.abs(dy)>12&&Math.abs(dy)>Math.abs(dx)){clearPackTouch();return;}
    if(Math.abs(dx)>16&&Math.abs(dx)>Math.abs(dy)*1.35){g.swiping=true;e.preventDefault();}
  },{passive:false});
  document.addEventListener('pointerup',e=>{
    const g=packTouch;if(!g||g.pid!==e.pointerId)return;
    const dx=e.clientX-g.x,dy=e.clientY-g.y;
    clearPackTouch();
    if(g.swiping){
      blockDragClick=true;e.preventDefault();
      if(Math.abs(dx)>=42&&Math.abs(dx)>Math.abs(dy)*1.35){advancePage(dx<0?1:-1);}
    }
  });
  document.addEventListener('pointercancel',e=>{if(packTouch?.pid===e.pointerId)clearPackTouch();});
  document.addEventListener('pointerdown',e=>{
    if(dragging||!e.isPrimary||e.button!==0)return;
    if(e.pointerType!=='mouse'&&e.target.closest('#pack'))return;
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
  window.addEventListener('blur',()=>{clearPackTouch();if(dragging)endDrag(false);});
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
    if(ds.useConsumable){
      const result=E.act(profile,'useConsumable',ds.useConsumable);
      if(!result.ok){notify(result.message);showDialog('provisions');return;}
      for(const ev of result.events||[])effects.push({...ev,age:prefs.motion?0:.24});
      save();closeDialog();structure='';update(true);sound('ability',Provisions.definitions[ds.useConsumable]?.element);notify(result.message);return;
    }
    if(el.id==='pack-combinable'){packCombinable=!packCombinable;packPage=0;el.setAttribute('aria-pressed',String(packCombinable));renderPack(displayRun());return;}
    if(ds.campGroveView){campGroveView=ds.campGroveView;renderLegacy();$('legacy-page').querySelector('[data-camp-grove-view="'+campGroveView+'"]')?.focus({preventScroll:true});return;}
    if(ds.campArea){campArea=ds.campArea;renderLegacy();$('legacy-page').querySelector('[data-camp-area="'+campArea+'"]')?.focus({preventScroll:true});return;}
    if(ds.campBuy){campResult(Legacy.purchase(profile,ds.campBuy),ds.campBuy);return;}
    if('campRespec' in ds){showDialog('camp-respec');return;}
    if('campSelectKit' in ds){campResult(Legacy.selectKit(profile,Number(ds.campSelectKit)));return;}
    if(ds.campOath){const active=Legacy.ensure(profile).activeOaths.slice(),index=active.indexOf(ds.campOath);if(index>=0)active.splice(index,1);else if(Legacy.oathSlots(profile)===1)active.splice(0,active.length,ds.campOath);else if(active.length<Legacy.oathSlots(profile))active.push(ds.campOath);else return notify('Unequip an Oath to free a slot.');campResult(Legacy.selectOaths(profile,active));return;}
    if('draftReroll' in ds||ds.draftBanish||ds.draftEcho){const action=ds.draftBanish?'banishTalent':ds.draftEcho?'echoTalent':'rerollDraft';if(runCommand(action,ds.draftBanish||ds.draftEcho)){displayedDraft='';showDialog('draft');}return;}
    if(el.id==='title-camp'){titleMenu(false);navigate('legacy');return;}
    if(ds.cloudMode){showDialog('cloud',{mode:ds.cloudMode,name:$('cloud-name')?.value||cloudName});return;}
    if(el.id==='title-cloud'){showDialog('cloud',{mode:cloudName?'load':'save'});return;}
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
  $('pack-sort').addEventListener('change',e=>{packSort=e.target.value;packPage=0;renderPack(displayRun());});
  modal.addEventListener('submit',event=>{if(event.target.id==='cloud-form')void submitCloud(event);});
  document.addEventListener('submit',event=>{const form=event.target;if(form.id==='camp-config-form'){event.preventDefault();const data=new FormData(form),patch={};for(const key of ['favoredElement','style','build'])if(data.has(key))patch[key]=data.get(key);campResult(Legacy.configure(profile,patch));}else if(form.matches('[data-camp-kit-form]')){event.preventDefault();const data=new FormData(form),items=[0,1,2,3].map(i=>data.get('item'+i)).filter(Boolean);campResult(Legacy.saveKit(profile,Number(form.dataset.campKitForm),{name:data.get('kitName'),items}));}});
  modal.addEventListener('cancel',event=>{if(cloudBusy&&dialogState?.type==='cloud')event.preventDefault();else if(dialogState?.type==='cloud-confirm'){pendingCloud=null;cloudAutoPaused=true;}});
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
          if(['attack','enemy','hit','kill','heal','milestone','status','loot','ability','special','companion','boss','ancient','synergy','consumable','supply-find'].includes(ev.kind)){
            if(ev.kind==='attack'||ev.itemId&&['heal','shield','stun','freeze'].includes(C.items[ev.itemId]?.style)){lastHeld=ev.itemId||r.active.find(it=>C.items[it.id].element===ev.element)?.id;heldFor=.7;}
            if(prefs.motion||['enemy','hit','attack','heal','milestone','ability','special','boss','ancient','synergy','consumable','supply-find'].includes(ev.kind))effects.push({...ev,age:prefs.motion?0:.24});
          }
          sound(['boss','ancient','synergy','consumable'].includes(ev.kind)?'ability':ev.kind==='supply-find'?'loot':ev.kind==='attack'&&ev.label?.includes('WEAK')?'crit':ev.kind==='special'?'ability':ev.kind,ev.element);
        }
        if(r.draft.length||events.some(ev=>['loot','dead','round','level','supply-find'].includes(ev.kind)))save();
        if(r.phase==='walk'||r.phase==='fight'){saveTimer+=dt;if(saveTimer>=3){saveTimer=0;save();}}
      }
      hudTimer+=dt;if(hudTimer>=.10||!structure){update();hudTimer=0;}
      const standing=r?.phase==='fight'&&r.enemy?Math.min(1,r.enemy.attackProgress):0;
      if(effects.length>120)effects=effects.filter((ev,i)=>ev.kind==='loot'||i>=effects.length-120);
      const deathFX=effects.find(fx=>fx.kind==='kill'&&fx.age<.52);

      const view={time:sceneClock,reducedMotion:!prefs.motion,distance:prefs.motion?(r?v.distance:demoDistance):0,region:v.region,round:v.round,phase:r?v.phase:'walk',
        heroHP:v.hp,heroMax:v.maxHp,heroX:.30,enemyX:.69+.008*standing,enemy:r?v.enemy:null,enemies:r?v.enemies:[],waveSize:v.waveSize,encounterAge:prefs.motion?encounterAge:1,defeated:deathFX?defeated:null,defeatAge:deathFX?.age??1,effects,paused:!running,
        companions:v.living?.companions||[],skills:v.skills||{},oaths:v.legacy?.oaths||[],heldId:heldFor>0&&lastHeld?lastHeld:v.active[0]?.id,
        heldLevel:(v.active.find(it=>it.id===(heldFor>0&&lastHeld?lastHeld:v.active[0]?.id))||v.active[0])?.level||1,heroShield:v.shield,equipped:v.active.map(it=>({id:it.id,level:it.level})),heroAilments:v.ailments};
      const still=!animating||(!prefs.motion&&!effects.length);
      const stillFrame=still?[v.region,v.phase,v.round,v.hp,v.shield,v.enemies.map(e=>[e.uid,Math.ceil(e.hp),Math.floor(e.attackProgress*8),e.fracture,e.haunt,e.chill>0,e.rooted>0,e.stun>0,e.freeze>0,e.titan?.phase,e.titan?.warning?.move,Math.ceil((e.titan?.warning?.left||0)*4)].join('-')).join(),view.heldId,view.heldLevel,view.equipped.map(it=>it.id).join(),JSON.stringify(v.ailments),view.distance,innerWidth,innerHeight].join('|'):'';
      if(!still||stillFrame!==lastStillFrame)WorldArt.draw($('world'),view,dt);
      lastStillFrame=stillFrame;
    }
    requestAnimationFrame(frame);
  }
  for(const key of Object.keys(C.rarities))document.documentElement.style.setProperty('--'+key+'-light',C.rarities[key].color);
  ink();fullLabel();S?.setEnabled(prefs.audio);update(true);
  titleMenu(true);
  renderCloudStatus();
  if(!storageOK)$('save-line').textContent='Local save unavailable · export in Profile';
  if(damagedSave)showDialog('recovery');
  requestAnimationFrame(frame);
})();
