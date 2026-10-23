import * as C from './core.mjs';
import * as P from './presentation.mjs';
import * as Friends from './companions.mjs';
import * as CompanionActions from './companion-actions.mjs';
import * as FriendUI from './companions-ui.mjs';
import * as Regions from './regional-adventures.mjs';
import * as Forge from './forge.mjs';
import * as CampExchange from './camp-exchange.mjs';
import * as ForgeUI from './forge-ui.mjs';
import * as EquipmentInventory from './equipment-inventory.mjs';
import * as InventoryUI from './inventory-ui.mjs';
import {eligibleCompassElements} from './equipment-trail.mjs';
import {habitatUI,speciesHuntUI,regionAdventureUI,trackedRegionalGoalUI,refreshRegionalProgress} from './regional-ui.mjs';
import {renderAbilityCards,renderAbilityGuide,battleActionHints} from './battle-guide.mjs';
import {monkeyPower,teamPower,rankMonkeys,POWER_EXPLANATION} from './power.mjs';
import {SocialController} from './social-controller.mjs';
import {renderSocialView,renderSocialBanner,renderRewardNotices} from './social-ui.mjs';
import {TrailGPS} from './gps.mjs';
import {LocationAccess} from './location.mjs';
import {Vault,importBackup} from './storage.mjs';
import {portrait,battlefield,pickupIcon,hydrateHostedPortraits} from './art.mjs';
import {EncounterRadar} from './radar.mjs';
import {LANDS,landProgress,selectLand} from './lands.mjs';
import {REGION_CHALLENGES,regionChallenges,trackedChallenge,trackChallenge,claimChallenge} from './challenges.mjs';
import {landArt} from './lands-art.mjs';
import {terrainMap} from './terrain.mjs';
const $=id=>document.getElementById(id),esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const STARTERS=['capuchin','macaque','langur','mandrill','spider','howler'];
const challengeLandForId=new Map(REGION_CHALLENGES.map(row=>[row.id,row.landId]));
let state=null,vault=null,selected='capuchin',tab='walk',selectedId=null,busy=false,combatBusy=false,lastCheckpoint=0,toastTimer,scores=null,collectionPage=0,collectionFilter='owned',animation='idle',pendingMove=null,lastDiscovery=0,ultraConfirm=false,practiceExpanded=false;
let gpsInfo={reason:'Paused · choose Start walking when ready.',mph:null,accuracy:null};
let collectionSearch='',collectionElement='all',collectionRarity='all',collectionSort='number',selectedSpecies=null,abilityEditor=null,modalReturnFocus=null;
let mapZoom=1,practiceMotionTimer=null,menuProgressPending=false,landPreviewId=null;
let companionSection='train',sanctuaryScroll=0,welcomeMode='new',companionDetailOpen=false,detailReturn=null;
let forgeMix={},forgeBusy=false,forgeUnsaved=false,gearPicker=null,forgeOdds={rarity:'all',element:'all',page:0};
let packSection='equipment',equipmentSelection=null,equipmentScroll=0;
const inventoryPreferenceKey='monkey-trails-inventory-v1';
let inventoryPrefs=InventoryUI.inventoryPreferences();
try{inventoryPrefs=InventoryUI.inventoryPreferences(JSON.parse(localStorage.getItem(inventoryPreferenceKey)||'{}'));}catch{}
const discoveryQueue=new P.NotificationQueue();
let discoveryTimer=null;
const social=new SocialController({getState:()=>state,getVault:()=>vault,
  persist:async()=>{if(!vault||!state)throw Error('Open your saved trail first.');await vault.checkpoint(state,true);if(!vault.localOK)throw Error('Save your device checkpoint before moving online supplies.');},
  onUpdate:()=>updateSocialSurfaces(),
  onBattle:()=>{if(!state)return;pause('Friendly battle · your expedition is preserved.');closeModal();tab='social';render();},
  onReturn:()=>{tab='walk';render();window.scrollTo(0,0);}
});
function updateSocialSurfaces(){
  if(!$('social-banner'))return;
  $('social-banner').innerHTML=state?renderSocialBanner(social.model):'';
  if(state){
    const known=new Set(P.activityRows(state).map(row=>row.id)),uid=social.model.me?.uid||social.uid,events=[];
    for(const invite of social.model.challenges||[])if((invite.toId||invite.to)===uid&&invite.status==='pending')events.push({id:`invitation:${invite.id}`,kind:'invitation',title:'A friendly challenge is waiting',detail:'Open Tribe to review the invitation.',important:true});
    for(const receipt of social.model.receipts||[])events.push({id:`social-reward:${receipt.uid}:${receipt.id}`,kind:'reward',title:receipt.title||'Tribe supplies received',detail:'Open Tribe to review the saved reward.',important:true});
    for(const claim of social.model.adventureClaims||[])events.push({id:`tribe-adventure:${claim.id}`,kind:'reward',title:'Shared tribe adventure reward',detail:'A saved reward with a permanent decoration. Open Tribe for details.',important:true});
    let changed=false;for(const event of events)if(!known.has(event.id)){P.recordActivity(state,event);discoveryQueue.push({...event,label:event.title});changed=true;}
    if(changed){if(!discoveryTimer)showNextDiscovery();void checkpoint(true);}
  }
  const applied=state?.social?.applied||{};
  $('social-notices').innerHTML=state?renderRewardNotices(social.model.receipts.filter(r=>applied[`${r.uid}:${r.id}`])):'';
  syncSocialOverlay();
  if(state&&tab==='social'){
    // Snapshot listeners must not erase a half-written tribe name or trade offer.
    const focused=document.activeElement,fields=[...$('view').querySelectorAll('[data-social-form] input,[data-social-form] select')].map(el=>({form:el.closest('form').dataset.socialForm,name:el.name,value:el.value,focused:el===focused,start:el.selectionStart,end:el.selectionEnd}));
    const disclosureKey=d=>d.querySelector('[data-social-form]')?.dataset.socialForm||d.querySelector('summary')?.textContent||'';
    const open=new Map([...$('view').querySelectorAll('details')].map(d=>[disclosureKey(d),d.open]));
    $('view').innerHTML=renderSocialView(social.model);
    [...$('view').querySelectorAll('details')].forEach(d=>{if(open.has(disclosureKey(d)))d.open=open.get(disclosureKey(d));});
    for(const f of fields){const el=[...$('view').querySelectorAll('[data-social-form] input,[data-social-form] select')].find(el=>el.name===f.name&&el.closest('form').dataset.socialForm===f.form);if(el){el.value=f.value;if(f.focused){el.focus({preventScroll:true});if(f.start!==null&&['text','search','tel','url','password'].includes(el.type))el.setSelectionRange(f.start,f.end);}}}
    hydrateHostedPortraits($('view'),{reducedMotion:prefs.reducedMotion||prefs.battery});
  }
}
function syncSocialOverlay(){
  const overlay=$('social-menu-overlay');if(!overlay)return;
  const applied=state?.social?.applied||{},receipts=social.model.receipts.filter(r=>applied[`${r.uid}:${r.id}`]);
  const content=state?renderSocialBanner(social.model)+renderRewardNotices(receipts):'';
  const visible=$('inspect-dialog').open&&(receipts.length||Boolean(renderSocialBanner(social.model)));
  if(visible){if(overlay.parentElement!==$('inspect-dialog'))$('inspect-dialog').append(overlay);overlay.innerHTML=content;if(!overlay.matches(':popover-open'))overlay.showPopover();}
  else{if(overlay.matches(':popover-open'))overlay.hidePopover();if(overlay.parentElement!==document.body)document.body.append(overlay);}
}
const preferenceKey='monkey-trails-display-v2';
let prefs={reducedMotion:false,battery:false,sound:false,vibration:false,keepAwake:true};
try{prefs={...prefs,...JSON.parse(localStorage.getItem(preferenceKey)||'{}')};}catch{}
const radar=new EncounterRadar(),team=()=>C.team(state),collection=()=>C.sanctuary(state),active=()=>state.run.monkey;
const species=p=>C.SPECIES[p.species]||{},name=p=>p.nickname||species(p).name||'Companion',pstats=p=>C.stats(p,C.equipped(state,p));
const powerOf=p=>monkeyPower(p,C.equipped(state,p)),powerCount=n=>Math.max(0,n).toLocaleString();
const rosterRanks=()=>rankMonkeys(collection(),p=>C.equipped(state,p));
const startingPower=()=>teamPower(team(),p=>C.equipped(state,p));
function companionPower(p){
  const ranked=state.collection[p.id]===p?rosterRanks().find(row=>row.monkey.id===p.id):null;
  return `<div class="companion-power"><span>Power <strong data-monkey-power="${powerOf(p)}">${powerCount(powerOf(p))}</strong></span>${ranked?`<span data-roster-rank="${ranked.rank}">#${ranked.rank} of ${collection().length} in your sanctuary</span>`:''}</div>`;
}
function crewPowerSummary(){
  return `<section class="crew-power-summary" aria-label="Starting team power"><div class="crew-power-symbol" aria-hidden="true">${uiIcon('rival')}</div><div><span class="eyebrow">YOUR STARTING CREW</span><strong data-team-power="${startingPower()}">${powerCount(startingPower())}<small>team power</small></strong><p>The combined power of your ${team().length} selected ${team().length===1?'companion':'companions'}.</p></div><details><summary>How power works</summary><p>${esc(POWER_EXPLANATION)} Matchups and abilities still matter in battle.</p></details></section>`;
}
const feet=n=>Math.max(0,Math.ceil((Number(n)||0)/C.FEET-1e-6)).toLocaleString(),canPrepare=()=>['trail','camp'].includes(state.run.phase),canSwap=()=>CompanionActions.lineupPermission(state).allowed;
function applyPreferences(){document.body.classList.toggle('reduce-motion',prefs.reducedMotion);document.body.classList.toggle('battery-saver',prefs.battery);if(state){render();if($('inspect-dialog')?.open&&$('dialog-content').querySelector('img.monkey-art'))closeModal();}}
applyPreferences();
const locationAccess=new LocationAccess(()=>renderLocation());
const gps=new TrailGPS(fix=>{
  if(!state||state.run.phase!=='trail')return pause();
  if(ultraConfirm){radar.observe(fix,state);renderGPS();return;}
  const oldCache=state.run.discoveries??state.run.nextCache,oldPhase=state.run.phase;
  social.observeLocation(fix);
  C.advance(state,fix.meters,fix.east,fix.north,notifyDiscovery);radar.observe(fix,state);animation='walk';
  const discovered=(state.run.discoveries??state.run.nextCache)!==oldCache;
  if(state.run.phase!=='trail'){pause('Encounter ready · tracking paused. Stop somewhere safe.');closeModal();tab='walk';}
  if(discovered||oldPhase!==state.run.phase||Date.now()-lastCheckpoint>=5000)void checkpoint(discovered);
  // Keep inputs, filters and scroll stable while accepted fixes update the save.
  if(tab==='walk'&&!$('inspect-dialog').open)render();else{menuProgressPending=true;renderGPS();}
},info=>{gpsInfo=info;if(info.accuracy!=null&&locationAccess.status!=='ready')locationAccess.update('ready');renderGPS();},fix=>{
  if(!state||state.run.phase!=='trail')return;
  social.observeLocation(fix);
  const before=radar.view(state).withinRadius;radar.observe(fix,state);
  if(before!==radar.view(state).withinRadius){if(tab==='walk'&&!$('inspect-dialog').open)render();else menuProgressPending=true;}
},error=>locationAccess.reportError(error));
function locationPanel(){return `<section class="location-access" data-location-panel><div class="location-heading">${uiIcon('play')}<strong>Location for real walks</strong><span data-location-label></span></div><p data-location-message role="status" aria-live="polite"></p><button type="button" data-action="enable-location">Enable location</button><details data-location-help><summary>No permission prompt?</summary><ol><li>In Chrome, tap the site controls beside the address, then Permissions → Location. Allow access or reset the blocked permission.</li><li>On Android, check Settings → Apps → Chrome → Permissions → Location. On iPhone, check Settings → Privacy & Security → Location Services → Chrome. Allow access while using the app; turn on precise location.</li><li>Keep phone location services on. Open the HTTPS game directly in Chrome and try again.</li></ol><p>Already allowed? Chrome can use location without showing another prompt.</p><a href="./monkey-trails.html" target="_top">Open game directly</a></details></section>`;}
function renderLocation(){
  const s=locationAccess.status,copy={
    unknown:['Ready to ask','Tap Enable location to ask your browser for GPS access. A save is not needed for this check.'],
    prompt:['Ready to ask','Tap Enable location, then choose Allow in Chrome’s permission prompt.'],
    requesting:['Waiting for Chrome','Choose Allow if Chrome asks. Waiting for the phone to return a location…'],
    granted:['Access allowed','The browser already allows location, so it may not show a prompt. Check location to verify the phone’s GPS.'],
    ready:['Location ready',state?'Access confirmed. Tap Start walking when you’re ready.':'Access confirmed. Begin or reopen a regular trail, then tap Start walking.'],
    denied:['Location blocked','Chrome or your phone is blocking location. Check the site and phone permissions below, then try again.'],
    timeout:['No location returned','Chrome has not returned a location. If no prompt appeared, check the settings below. Otherwise move to open sky and retry.'],
    unavailable:['Location unavailable','Your phone could not provide a location. Turn on location services, check the settings below, and retry outside.'],
    insecure:['HTTPS needed','This page is not secure. Open the game with https:// so Chrome can ask for location.'],
    unsupported:['Location unavailable','This browser does not expose location here. Open the game directly in Chrome.'],
    policy:['Page blocks location','This page or its embedded browser blocks location. Open the game directly; an embedded page needs location access enabled by its host.'],
    hidden:['Return to the game','Keep the game visible, then tap Enable location again.']
  }[s]||['Ready to ask','Tap Enable location to ask your browser for GPS access.'];
  for(const panel of document.querySelectorAll('[data-location-panel]')){
    panel.hidden=Boolean(gps.active&&panel.closest('#view')&&['ready','granted'].includes(s));panel.dataset.locationState=s;panel.querySelector('[data-location-label]').textContent=copy[0];panel.querySelector('[data-location-message]').textContent=copy[1];
    const b=panel.querySelector('[data-action="enable-location"]');b.disabled=locationAccess.pending||['insecure','unsupported','policy'].includes(s);b.textContent=locationAccess.pending?'Waiting for location…':['granted','ready'].includes(s)?'Check location':['denied','timeout','unavailable'].includes(s)?'Try location again':'Enable location';
    const setup=panel.closest('.map-location-setup');if(setup){const error=['denied','timeout','unavailable','policy','insecure','unsupported'].includes(s);setup.hidden=(gps.active||['ready','granted'].includes(s))&&!error;if(error)setup.open=true;}const help=panel.querySelector('[data-location-help]');help.hidden=['prompt','granted','ready'].includes(s);if(['denied','timeout','unavailable','policy','insecure','unsupported'].includes(s))help.open=true;
  }
}
$('welcome-location').innerHTML=locationPanel();renderLocation();void locationAccess.refresh();
function toast(message){clearTimeout(toastTimer);$('toast').textContent=message;$('toast').hidden=false;toastTimer=setTimeout(()=>$('toast').hidden=true,4800);}
function notifyDiscovery(found){
  lastDiscovery=Date.now();feedback();
  const label=found.category==='essence'?`${C.ELEMENTS[found.id].name} essence`:found.category==='fruit'?'Training fruit':found.category==='fragment'?`${C.GEAR[found.id].name} fragment`:C.ITEMS[found.id]?.name||'Trail supply';
  const id=`discovery:${state.run.id}:${state.run.discoveries}`;
  P.recordActivity(state,{id,kind:'discovery',title:label,amount:found.amount});
  discoveryQueue.push({...found,resourceId:found.id,id,kind:'discovery',combineKey:`${found.category}:${found.id}`,label});
  if(!discoveryTimer)showNextDiscovery();
}
function showNextDiscovery(){
  const popup=$('discovery-popup');
  if($('inspect-dialog').open&&$('inspect-dialog').classList.contains('presentation-dialog')||['battle','replacement','befriend'].includes(state?.run.phase)){discoveryTimer=setTimeout(showNextDiscovery,600);return;}
  const found=discoveryQueue.next();
  if(!found){discoveryTimer=null;return;}
  const icon=found.category==='fragment'?'fragment':found.category==='fruit'?'fruit':found.resourceId;
  const artwork=found.kind!=='discovery'?'✦':found.category==='essence'?`<span style="color:${C.ELEMENTS[found.resourceId].color}">${C.ELEMENTS[found.resourceId].icon}</span>`:pickupIcon(icon);
  popup.innerHTML=`<span class="discovery-popup-icon" aria-hidden="true">${artwork}</span><span><small>${found.kind==='discovery'?'YOU FOUND':esc(found.kind.toUpperCase())}</small><strong>${esc(found.label)}${found.kind==='discovery'?` ×${found.amount}`:''}</strong></span>`;
  popup.hidden=false;popup.showPopover();
  discoveryTimer=setTimeout(()=>{popup.hidePopover();popup.hidden=true;discoveryQueue.finish();showNextDiscovery();},found.kind==='discovery'?2500:4000);
}
function observeMilestones(){
  if(!state)return;const presentation=P.normalizePresentation(state),u=state.run.ultra;
  const events=landProgress(state).unlocked.map(l=>({id:`region-unlocked:${l.id}`,kind:'unlock',label:`${l.name} is open`,detail:'Visit Regions to explore its habitats and adventure.'}));
  if(u&&!['claimed','battle'].includes(u.status))events.push({id:`ultra-arrival:${u.id}`,kind:'ultra',label:`${C.SPECIES[u.species]?.name||'An Ultra'} has arrived`,detail:'No countdown. Track it or choose the safe-route challenge.'});
  for(const e of events){if(presentation.activity.some(row=>row.id===e.id))continue;P.recordActivity(state,{...e,title:e.label,important:true});if(e.kind==='ultra'||state.run.distance>0){discoveryQueue.push(e);if(e.kind==='ultra')P.playMomentSound('ultra',prefs);}}
  if(!discoveryTimer&&discoveryQueue.entries().length)showNextDiscovery();
}
function showSavedReveal(moment){if(!moment)return;modal(P.renderSavedReveal(state,moment,{portrait}),'Adventure moment','presentation-dialog');if(moment.kind==='recruit'){$('dialog-content').querySelector('[data-action="close-dialog"]')?.setAttribute('data-reveal-id',moment.id);}P.playMomentSound(moment.kind==='recruit'&&C.SPECIES[moment.monkey.species]?.rarity==='ultra'?'ultra':moment.kind,prefs);}

function feedback(){if(prefs.vibration&&navigator.vibrate)navigator.vibrate(25);if(prefs.sound)try{const a=new (window.AudioContext||window.webkitAudioContext)(),o=a.createOscillator(),g=a.createGain();o.connect(g);g.connect(a.destination);o.frequency.value=660;g.gain.setValueAtTime(.035,a.currentTime);g.gain.exponentialRampToValueAtTime(.001,a.currentTime+.12);o.start();o.stop(a.currentTime+.13);o.onended=()=>a.close();}catch{}}
function saveStatus({text,kind}){$('save-status').textContent=P.saveFeedback({text,kind},state?.mode);$('save-dot').className=`save-dot ${kind}`;$('cloud-restore').hidden=kind!=='conflict';}
function starters(){$('starters').innerHTML=STARTERS.map(id=>{const p=C.SPECIES[id];return `<button type="button" class="starter ${selected===id?'selected':''}" data-starter="${id}" aria-pressed="${selected===id}">${portrait({species:id,types:[],stage:0})}<span>${esc(p.name)}</span></button>`;}).join('');$('starter-trait').textContent=`${C.SPECIES[selected].role||C.SPECIES[selected].title} · ${C.SPECIES[selected].trait}`;}
starters();$('starters').addEventListener('click',e=>{const b=e.target.closest('[data-starter]');if(b){selected=b.dataset.starter;starters();}});
function setWelcomeMode(mode){welcomeMode=mode;const creating=mode==='new';$('welcome-setup').hidden=false;$('starter-setup').hidden=!creating;$('welcome-setup-title').textContent=creating?'Choose your first companion':'Continue your adventure';document.querySelector('#save-form [value="create"]').hidden=!creating;document.querySelector('#save-form [value="open"]').hidden=creating;$('creature-password').autocomplete=creating?'new-password':'current-password';document.querySelectorAll('[data-action^="welcome-"]').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.action===`welcome-${creating?'new':'continue'}`)));$('welcome-setup-title').focus({preventScroll:true});}
function loginBusy(value){busy=value;document.querySelectorAll('#save-form button,#practice-button,#import-button').forEach(b=>b.disabled=value);}
function clearNotifications(){clearTimeout(discoveryTimer);discoveryTimer=null;discoveryQueue.clear();const popup=$('discovery-popup');if(popup.matches(':popover-open'))popup.hidePopover();popup.hidden=true;}
async function showGame(next,nextVault){clearNotifications();locationAccess.cancel();vault=nextVault;state=next;companionDetailOpen=false;detailReturn=null;forgeMix={};forgeBusy=false;forgeUnsaved=false;gearPicker=null;tab=['walk','team','sanctuary','pack','social'].includes(next.meta.interface?.tab)?next.meta.interface.tab:'walk';if(['battle','replacement','befriend','camp','dead'].includes(next.run.phase))tab='walk';selectedId=next.collection[next.meta.interface?.selectedId]?.id||active().id;await radar.bind(vault);$('creature-password').value='';$('welcome').hidden=true;$('game').hidden=false;$('login-message').textContent='';$('mode-label').textContent=state.mode==='practice'?'PRACTICE · UNRANKED':'GPS · FOREGROUND ONLY';if(state.run.battle)state.run.battle.ready=false;render();const reveal=P.pendingReveal(state);if(reveal)showSavedReveal(reveal);void social.start();window.scrollTo(0,0);}
async function enter(name,password,options){loginBusy(true);$('login-message').textContent='Opening your encrypted sanctuary…';const nextVault=new Vault(saveStatus);try{await showGame(await nextVault.open(name,password,options),nextVault);}catch(error){$('login-message').textContent=error.message;}finally{loginBusy(false);}}
$('save-form').addEventListener('submit',e=>{e.preventDefault();if(!busy)void enter($('creature-name').value,$('creature-password').value,{create:e.submitter?.value!=='open',species:selected,mode:'gps'});});
$('practice-button').addEventListener('click',async()=>{if(busy)return;loginBusy(true);$('login-message').textContent='Opening the separate practice sanctuary…';const v=new Vault(saveStatus),name='Trail Practice',password='practice-only-not-ranked';try{let s;try{s=await v.open(name,password,{mode:'practice'});}catch(e){if(!e.message.startsWith('No save found'))throw e;s=await v.open(name,password,{create:true,species:selected,mode:'practice'});}await showGame(s,v);}catch(e){$('login-message').textContent=e.message;}finally{loginBusy(false);}});
$('import-button').addEventListener('click',()=>{if(!busy)$('backup-file').click();});
$('backup-file').addEventListener('change',async e=>{const file=e.target.files[0];if(!file||busy)return;const name=$('creature-name').value,password=$('creature-password').value;if(!name.trim()||!password){$('login-message').textContent='Enter the backup creature name and password first.';e.target.value='';return;}if(file.size>1100000){$('login-message').textContent='Backup file is too large.';e.target.value='';return;}if(!confirm('Restore this encrypted backup on this device? Existing device progress will be kept as a recovery copy.')){e.target.value='';return;}loginBusy(true);try{const mode=await importBackup(name,password,await file.text());await enter(name,password,{mode});}catch(error){$('login-message').textContent=error.message;}finally{e.target.value='';loginBusy(false);}});
function pause(message='Paused · choose Start walking to resume.'){gps.stop();animation='idle';gpsInfo={reason:message,mph:null,accuracy:null};renderGPS();if(state&&vault)void checkpoint(true);}
async function checkpoint(urgent=false){if(!state||!vault)return;lastCheckpoint=Date.now();state.meta.interface={tab,selectedId,companionSection};try{await vault.checkpoint(state,urgent);}catch(e){saveStatus({text:e.message,kind:'error'});}}
function chip(type){const e=C.ELEMENTS[type];return e?`<span class="type-chip" style="--element:${e.color}">${e.icon} ${esc(e.name)}</span>`:'';}
function chips(p){return p.types?.length?p.types.map(chip).join(''):'<span class="type-chip adaptable">◇ Adaptable</span>';}
function bar(value,max,label,kind=''){const n=Math.max(0,Number(value)||0),m=Math.max(1,Number(max)||1);return `<div class="meter-label"><span>${esc(label)}</span><span>${Math.round(n)} / ${Math.round(m)}</span></div><div class="meter ${kind}" role="progressbar" aria-label="${esc(label)}" aria-valuenow="${Math.round(n)}" aria-valuemin="0" aria-valuemax="${Math.round(m)}"><i style="width:${C.clamp(n/m*100,0,100)}%"></i></div>`;}
function miniHealth(p){return `<div class="mini-health"><i style="width:${C.clamp(p.hp/pstats(p).hp*100,0,100)}%"></i></div>`;}
function resources(){const r=state.run;return `<div class="fruit-row"><span>${pickupIcon('fruit')} ${r.fruit} fruit</span><span>${r.items.bondCharm??0} Bond Charms</span></div><div class="resources">${Object.entries(C.ELEMENTS).map(([id,e])=>`<div class="resource" style="--element:${e.color}"><span>${e.icon} ${esc(e.name)}</span><b>${r.essence[id]??0}</b></div>`).join('')}</div>`;}
function journal(){return `<details class="panel journal"><summary>Trail journal <span>${state.log.length} recent moments</span></summary><ol class="log">${[...state.log].reverse().map(t=>`<li>${esc(t)}</li>`).join('')}</ol></details>`;}
function uiIcon(id){const paths={leaf:'<path d="M4 20Q1 4 21 3Q22 20 4 20ZM4 20 16 8M10 14V9m0 5h5"/>',rival:'<path d="m5 3 16 18M19 3 3 19M3 15l6 6m6-18 6 6M3 3l5 1-4 4-1-5Zm18 18-5-1 4-4 1 5Z"/>',camp:'<path d="m3 18 9-15 9 15H3ZM12 3v15m-4 3 8-3m-8 0 8 3"/>',arrow:'<path d="M5 12h14m-5-5 5 5-5 5"/>',play:'<path d="m9 5 10 7-10 7V5Z"/>',pack:'<path d="M8 7V5a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2M5 7h14l1 14H4L5 7ZM5 8v4l7 3 7-3V8"/>'};return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${paths[id]||paths.leaf}</svg>`;}
function practiceControls(){
  return `<div class="map-practice"><span>Practice walk<small>15 m per step</small></span><div class="map-step-buttons">${[['N','↑','north'],['W','←','west'],['S','↓','south'],['E','→','east']].map(([id,arrow,label])=>`<button type="button" data-action="map-step" data-id="${id}" aria-label="Walk ${label} in practice"><b>${arrow}</b><small>${id}</small></button>`).join('')}</div></div><details class="practice-tools" ${practiceExpanded?'open':''}><summary>${uiIcon('play')}<strong>More practice options</strong><span>＋</span></summary><div class="direction-buttons">${Object.entries(C.DIRECTIONS).map(([d,t])=>`<button data-action="practice-step" data-id="${d}"><b>${d}</b><span>${C.ELEMENTS[t].name}</span><small>+250 ft</small></button>`).join('')}</div><p>Separate practice save · no GPS or ranked progress</p><button class="text-button practice-gps-link" data-action="gps-setup">Set up a GPS trail →</button></details>`;
}
function compactLocationPanel(){
  const error=['denied','timeout','unavailable','insecure','unsupported','policy'].includes(locationAccess.status);
  const hidden=(gps.active||['ready','granted'].includes(locationAccess.status))&&!error;
  return `<details class="map-location-setup" ${error?'open':''} ${hidden?'hidden':''}><summary>Location setup & help</summary>${locationPanel()}</details>`;
}

function walkingBonuses(){const b=state.run.bonuses;return `<div class="active-bonuses">${[b.ribbon>0?`Trail Ribbon · ${feet(b.ribbon)} ft left`:'',b.scent>0?`Treasure Scent · ${b.scent} discoveries`:'',b.feather?'Scout Feather · next sighting':'',b.campSeed?'Campfire Seed · next camp':''].filter(Boolean).map(t=>`<span>${esc(t)}</span>`).join('')}</div>`;}
function upgradeReadiness(id){return social.locked?{train:false,evolve:false}:CompanionActions.upgradeReadiness(state,id);}
function upgradeBadges(info){return `${info.train?'<span class="upgrade-ready-badge training-ready">↑ Level up ready</span>':''}${info.evolve?'<span class="upgrade-ready-badge evolution-ready">✦ Evolution ready</span>':''}`;}
function upgradeClasses(info){return `${info.train?'is-training-ready ':''}${info.evolve?'is-evolution-ready':''}`;}
function openCompanionActions(id){
  const p=state.collection[id];if(!p)return;selectedId=id;const action=CompanionActions.companionAction(state,id),ready=upgradeReadiness(id),copies=collection().filter(x=>x.species===p.species);
  modal(`<section class="companion-action-sheet ${upgradeClasses(ready)}" data-upgrade-monkey="${esc(id)}"><div class="quick-companion-heading">${portrait(p,{state:p.hp<=0?'knockout':'idle'})}<div><span class="eyebrow">${action.inLineup?'IN YOUR LINEUP':'IN YOUR SANCTUARY'}</span><h2>${esc(name(p))}</h2><small>Lv ${p.level} · ${Math.round(p.hp)} / ${pstats(p).hp} HP</small></div></div><div class="upgrade-statuses">${upgradeBadges(ready)}</div>${copies.length>1?`<label for="companion-action-individual">Choose individual<select id="companion-action-individual">${copies.map(x=>`<option value="${esc(x.id)}" ${x.id===id?'selected':''}>${esc(name(x))} · Lv ${x.level}${state.run.party.includes(x.id)?' · In lineup':''}</option>`).join('')}</select></label>`:''}<div class="companion-quick-actions"><button type="button" class="primary" data-action="${action.inLineup?'companion-swap':'companion-add'}" data-id="${esc(id)}" ${action.allowed?'':'disabled'}><strong>${action.label}</strong><small>${action.inLineup?'Replace this lineup slot':action.full?'Choose who this monkey replaces':'Use an empty lineup slot'}</small></button><button type="button" data-action="companion-details" data-id="${esc(id)}"><strong>Details</strong><small>Upgrades, abilities, evolution & equipment</small></button></div>${action.reason?`<p class="fine">${esc(action.reason)}</p>`:''}${!action.inLineup&&(ready.train||ready.evolve)?'<p class="fine">Upgrade requirements are met. Add this companion to your lineup to train or evolve.</p>':''}</section>`,'Monkey actions','companion-actions-dialog');
}
function openReplacementPicker(outgoingId){
  const outgoing=state.collection[outgoingId],permission=CompanionActions.lineupPermission(state);if(!permission.allowed)throw Error(permission.reason);
  if(!outgoing||!state.run.party.includes(outgoingId))throw Error('Choose a current lineup companion.');
  const candidates=collection().filter(p=>!state.run.party.includes(p.id));
  modal(`<span class="eyebrow">SWAP LINEUP SLOT</span><h2>Replace ${esc(name(outgoing))}</h2><p class="fine">Choose a Sanctuary companion. Health, energy and equipment stay with each individual.</p><div class="switch-list">${candidates.map(p=>`<button type="button" data-action="lineup-replace" data-id="${esc(p.id)}" data-outgoing="${esc(outgoingId)}" ${p.hp>0?'':'disabled'}>${portrait(p,{state:p.hp>0?'idle':'knockout'})}<span><strong>${esc(name(p))}</strong><small>Lv ${p.level} · ${Math.round(p.hp)} HP${p.hp>0?'':' · Needs revival'}</small></span><b>Choose →</b></button>`).join('')||'<p>No other collected companions are available yet.</p>'}</div>`,'Swap companion');
}
function openLineupSlotPicker(incomingId){
  const p=state.collection[incomingId],permission=CompanionActions.lineupPermission(state);if(!permission.allowed)throw Error(permission.reason);
  if(!p||p.hp<=0||state.run.party.includes(incomingId))throw Error('Choose a conscious Sanctuary companion outside the lineup.');
  modal(`<span class="eyebrow">ADD TO LINEUP</span><h2>Make room for ${esc(name(p))}</h2><p class="fine">Your lineup holds three monkeys. Choose who returns to the Sanctuary.</p><div class="switch-list">${team().map(outgoing=>`<button type="button" data-action="lineup-replace" data-id="${esc(incomingId)}" data-outgoing="${esc(outgoing.id)}">${portrait(outgoing)}<span><strong>Replace ${esc(name(outgoing))}</strong><small>Lv ${outgoing.level} · ${Math.round(outgoing.hp)} HP</small></span><b>Choose →</b></button>`).join('')}</div>`,'Choose lineup slot');
}
function refreshUpgradeIndicators(){
  if(!state)return;
  for(const el of document.querySelectorAll('[data-upgrade-monkey]')){const info=upgradeReadiness(el.dataset.upgradeMonkey);el.classList.toggle('is-training-ready',info.train);el.classList.toggle('is-evolution-ready',info.evolve);const badges=el.querySelector(':scope > .upgrade-statuses');if(badges)badges.innerHTML=upgradeBadges(info);}
}
function teamStrip(action='monkey-actions',showPower=false){const members=team();return `<div class="team-strip">${Array.from({length:3},(_,i)=>{const p=members[i];return p?`<button class="team-slot ${p.id===state.run.activeId?'active':''} ${p.hp<=0?'knocked-out':''} ${upgradeClasses(upgradeReadiness(p.id))}" data-action="${action}" data-id="${esc(p.id)}" data-upgrade-monkey="${esc(p.id)}">${portrait(p,{state:p.hp<=0?'knockout':'idle'})}<strong>${esc(name(p))}</strong>${miniHealth(p)}<small>${p.hp<=0?'Knocked out':`Lv ${p.level} · ${Math.round(p.hp)} HP`}</small><span class="upgrade-statuses">${upgradeBadges(upgradeReadiness(p.id))}</span>${showPower?`<span class="team-power-badge" data-monkey-power="${powerOf(p)}">${powerCount(powerOf(p))} power</span>`:''}<span class="slot-elements">${(p.types||[]).map(t=>`<span style="color:${C.ELEMENTS[t].color}">${C.ELEMENTS[t].icon}</span>`).join('')||'◇'}</span></button>`:`<button class="team-slot empty" data-action="lineup-open-sanctuary" aria-label="Team slot ${i+1} empty"><span>＋</span><strong>Slot ${i+1}</strong><small>Recruit a companion</small></button>`;}).join('')}</div>`;}
function radarView(){
  const u=state.run.ultra,rv=radar.view(state)||{},bearing=Number(rv.bearing??u?.bearing??0),dist=Number(rv.distance??u?.challengeRemaining??0),ring=Math.min(92,Math.max(29,dist/(600*C.FEET)*92)),x=140+Math.sin(bearing*Math.PI/180)*ring,y=140-Math.cos(bearing*Math.PI/180)*ring,showing=u&&!['claimed','battle'].includes(u.status);
  const ticks=Array.from({length:32},(_,i)=>`<path d="M140 31v${i%4===0?6:3}" transform="rotate(${i*11.25} 140 140)"/>`).join('');
  return `<div class="radar-wrap"><div class="radar" role="img" aria-label="Encounter radar. North Leaf, east Storm, south Fire, west Water. Rings are not roads."><svg class="radar-grid" viewBox="0 0 280 280" aria-hidden="true"><defs><radialGradient id="radar-paper"><stop stop-color="#f9f6e9"/><stop offset="1" stop-color="#e7ead5"/></radialGradient><pattern id="radar-dots" width="10" height="10" patternUnits="userSpaceOnUse"><circle cx="1" cy="1" r=".55" fill="#668277" opacity=".22"/></pattern></defs><circle cx="140" cy="140" r="109" fill="url(#radar-paper)" stroke="#9cad92" stroke-width=".7"/><circle cx="140" cy="140" r="105" fill="url(#radar-dots)"/><g fill="none" stroke="#a9b89b" stroke-width=".7"><circle cx="140" cy="140" r="35"/><circle cx="140" cy="140" r="69"/><circle cx="140" cy="140" r="101"/><path d="M140 39v202M39 140h202" stroke-dasharray="2 6"/></g><g stroke="#6f8b76" stroke-width="1">${ticks}</g><g fill="#6d8677" font-size="8" font-family="Arial,sans-serif"><text x="169" y="127">200 ft</text><text x="193" y="101">400 ft</text><text x="213" y="77">600 ft</text></g><path d="m140 43-3 8h6Z" fill="#c28a35"/>${showing?`<g transform="translate(${x},${y})"><circle r="15" fill="#edd28d" stroke="#a87629"/><path d="M0-8 2-2 8 0 2 2 0 8-2 2-8 0-2-2Z" fill="#705225"/></g>`:''}</svg><span class="radar-bearing north"><b>N</b>Leaf</span><span class="radar-bearing east"><b>E</b>Storm</span><span class="radar-bearing south"><b>S</b>Fire</span><span class="radar-bearing west"><b>W</b>Water</span><div class="player-marker">${portrait(active(),{state:gps.active?animation:'idle'})}</div></div><span class="radar-caption">ENCOUNTER RADAR <i></i> YOUR ROUTE, YOUR CHOICE</span></div>`;
}
const landDistance=n=>n<1000?`${Math.ceil(Math.max(0,n))} m`:`${(n/1000).toLocaleString(undefined,{maximumFractionDigits:2})} km`;
function landTile(land,prefix='rail'){
  const progress=landProgress(state),unlocked=progress.unlocked.some(l=>l.id===land.id),here=progress.activeId===land.id;
  return `<button type="button" class="land-tile ${here?'is-current':''} ${unlocked?'is-unlocked':'is-locked'}" data-action="lands-preview" data-id="${land.id}" aria-label="${esc(land.name)} · ${here?'Current land':unlocked?'Unlocked':`Unlock at ${landDistance(land.unlock)}`}" ${here?'aria-current="location"':''}><span class="land-thumb">${landArt(land.id,{uid:`${prefix}-${land.id}`,compact:true})}<span class="land-tile-status">${here?'✓ HERE':unlocked?'VISIT':`${abilityIcon('lock')} ${landDistance(land.unlock)}`}</span></span><strong>${esc(land.name)}</strong></button>`;
}
function smallCompass(heading=0){
  const u=state.run.ultra,rv=radar.view(state),sighting=u&&!['claimed','battle'].includes(u.status),bearing=Number(rv?.bearing??u?.bearing??0)*Math.PI/180;
  const facing=['North','Northeast','East','Southeast','South','Southwest','West','Northwest'][Math.round(heading/45)%8];
  return `<button type="button" class="map-compass" data-action="compass" aria-label="Open encounter compass. Facing ${facing}${sighting?'. Ultra sighting nearby':''}"><svg viewBox="0 0 100 100" aria-hidden="true"><circle cx="50" cy="50" r="47" fill="#fffbe9" stroke="#c4c9a4"/><circle cx="50" cy="50" r="31" fill="none" stroke="#c4c9a4" stroke-dasharray="2 4"/><path d="M50 24v52M24 50h52" stroke="#b5c1a1"/><g fill="#476851" font-family="system-ui,sans-serif" font-size="12" font-weight="700" text-anchor="middle"><text x="50" y="18" fill="#a47736">N</text><text x="88" y="54">E</text><text x="50" y="92">S</text><text x="13" y="54">W</text></g><g transform="rotate(${heading} 50 50)"><path d="m50 29 9 27-9-4-9 4Z" fill="#ae813e"/><path d="m50 71-9-15 9 4 9-4Z" fill="#91a384"/></g><circle cx="50" cy="50" r="4" fill="#355c46"/>${sighting?`<circle cx="${50+Math.sin(bearing)*32}" cy="${50-Math.cos(bearing)*32}" r="5" fill="#e9c76f" stroke="#80622c"/>`:''}</svg></button>`;
}
function landscape(afterMap=''){
  const p=landProgress(state),land=p.active,heading=p.heading||0,percent=p.next?C.clamp(p.totalDistance/p.next.unlock*100,0,100):100;
  const pose=gps.active||state.mode==='practice'?animation:'idle';
  return `<section class="landscape terrain-exploration" aria-label="${esc(land.name)} exploration map">
    <div class="land-heading"><div><span class="eyebrow">EXPLORING</span><h2>${esc(land.name)}</h2></div><button type="button" class="land-world-button" data-action="lands" aria-label="Select region">${uiIcon('pack')}<span>Regions<b>${p.unlocked.length} / ${LANDS.length}</b></span></button></div>
    <div class="land-scene exploration-map ${gps.active||pose==='walk'?'is-walking':''}" data-region="${land.id}" style="--land-color:${land.color}">
      ${terrainMap(land.id,{uid:'main-map',position:p.position,trail:p.region.trail,heading,zoom:mapZoom})}
      <div class="map-avatar-direction" style="--heading:${heading}deg"><svg viewBox="0 0 50 50" aria-hidden="true"><path d="m25 2 7 13-7-3-7 3Z" fill="#f8df88" stroke="#496448" stroke-width="1.5"/></svg></div>
      <div class="land-traveler" data-pose="${pose}">${portrait(active(),{state:pose})}<span class="traveler-shadow"></span></div>
      ${smallCompass(heading)}
      <div class="map-zoom"><button type="button" data-action="map-zoom" data-id="in" aria-label="Zoom in on terrain" ${mapZoom>=1.5?'disabled':''}>+</button><button type="button" data-action="map-zoom" data-id="out" aria-label="Zoom out on terrain" ${mapZoom<=.75?'disabled':''}>−</button></div>
      <span class="map-location" aria-label="Virtual map position">${Math.round(Math.abs(p.position.east))} m ${p.position.east<0?'W':'E'} · ${Math.round(Math.abs(p.position.north))} m ${p.position.north<0?'S':'N'}</span>
    </div>
    ${afterMap}
    <div class="map-progress"><span>${p.next?`<b>${landDistance(p.remaining)}</b> to ${esc(p.next.name)}`:'<b>All ten lands unlocked</b>'}</span><small>${landDistance(p.totalDistance)} ${state.mode==='practice'?'practice':'total'}</small><div class="land-progress" role="progressbar" aria-label="${p.next?`Progress toward ${esc(p.next.name)}`:'All lands unlocked'}" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${Math.round(percent)}"><i style="width:${percent}%"></i></div></div>
  </section>`;
}

function challengeRewardText(reward={}){
  return [reward.fruit?`${reward.fruit} fruit`:'',...Object.entries(reward.items||{}).filter(([,amount])=>amount>0).map(([id,amount])=>`${amount} ${C.ITEMS[id]?.name||id}`),...Object.entries(reward.essence||{}).map(([id,amount])=>`${amount} ${C.ELEMENTS[id]?.name||id} essence`),...(reward.gear||[]).map(id=>`Rare ${C.GEAR[id]?.name||id}`),reward.cosmetic?'Permanent regional keepsake':''].filter(Boolean).join(' · ');
}
function challengeProgress(row){
  const progress=row.unit==='m'?`${landDistance(row.current)} / ${landDistance(row.goal)}`:`${Math.floor(row.current)} / ${row.goal} ${row.unit}`;
  return `<span class="challenge-count"><strong>${progress}</strong><small>${row.claimed?'Reward claimed':row.complete?'Complete · reward ready':`${Math.floor(row.percent)}% complete`}</small></span><span class="challenge-meter" role="progressbar" aria-label="${esc(row.title)} progress" aria-valuemin="0" aria-valuemax="${row.goal}" aria-valuenow="${Math.min(row.current,row.goal)}"><i style="width:${C.clamp(row.percent,0,100)}%"></i></span>`;
}
function regionChallengeSection(landId){
  const rows=regionChallenges(state,landId);
  return `<section class="region-challenges" aria-label="Regional challenges"><div class="regional-heading"><div><span class="eyebrow">MAKE THIS REGION YOUR OWN</span><h3>Regional challenges</h3></div><span data-region-challenge-count="${landId}">${rows.filter(r=>r.claimed).length} / ${rows.length} claimed</span></div><p class="fine">Progress comes from exploring this region. Track one goal on your trail, then claim its reward when complete.</p>${rows.map(row=>`<article class="region-challenge ${row.claimed?'is-claimed':row.complete?'is-complete':''}" data-challenge-id="${row.id}"><div class="challenge-card-title"><h4>${esc(row.title)}</h4><span>${row.claimed?'✓':row.kind==='distance'?'↟':row.kind==='victories'?'⚔':'✦'}</span></div><p>${esc(row.description)}</p><div class="challenge-progress-host">${challengeProgress(row)}</div><div class="challenge-reward">${pickupIcon(row.reward.fruit?'fruit':Object.keys(row.reward.items||{})[0]||'berry')}<span>${esc(challengeRewardText(row.reward))}</span></div><div class="challenge-actions"><button type="button" data-action="challenge-track" data-id="${row.id}" aria-pressed="${row.tracked}" ${!row.unlocked||row.claimed?'disabled':''}>${row.tracked?'Tracking · stop':'Track goal'}</button><button type="button" class="primary" data-action="challenge-claim" data-id="${row.id}" ${!row.unlocked||!row.complete||row.claimed||!['trail','camp'].includes(state.run.phase)?'disabled':''}>${row.claimed?'Claimed':row.complete?'Claim reward':'Goal in progress'}</button></div></article>`).join('')}</section>`;
}
function trackedChallengePanel(){
  const row=trackedChallenge(state),p=landProgress(state),land=LANDS.find(l=>l.id===(row?.landId||p.activeId));
  if(!row)return `<div class="tracked-region-challenge is-empty"><div><strong>Find your next regional goal</strong><small>Explore, win battles and discover supplies for extra rewards.</small></div><button type="button" data-action="challenge-open" data-id="${p.activeId}">Challenges →</button></div>`;
  return `<div class="tracked-region-challenge ${row.complete?'is-complete':''}" data-challenge-id="${row.id}"><div class="tracked-challenge-title"><span><small>TRACKED · ${esc(land?.name||'Regional challenge')}</small><strong>${esc(row.title)}</strong></span><button type="button" data-action="challenge-open" data-id="${row.landId}">${row.complete?'Claim →':'View →'}</button></div><div class="challenge-progress-host">${challengeProgress(row)}</div></div>`;
}
function refreshChallengeProgress(){
  if(!state)return;
  const nodes=[...document.querySelectorAll('[data-challenge-id]')],unlocks=[...document.querySelectorAll('[data-region-unlock-progress]')];
  if(!nodes.length&&!unlocks.length)return;
  const visibleLands=[...new Set(nodes.map(el=>challengeLandForId.get(el.dataset.challengeId)).filter(Boolean))],rows=visibleLands.flatMap(id=>regionChallenges(state,id)),byId=new Map(rows.map(row=>[row.id,row]));
  for(const el of nodes){
    const row=byId.get(el.dataset.challengeId);if(!row)continue;
    const host=el.querySelector('.challenge-progress-host');if(host)host.innerHTML=challengeProgress(row);
    el.classList.toggle('is-complete',row.complete);el.classList.toggle('is-claimed',row.claimed);
    const claim=el.querySelector('[data-action="challenge-claim"]');if(claim){claim.disabled=!row.unlocked||!row.complete||row.claimed||!['trail','camp'].includes(state.run.phase);claim.textContent=row.claimed?'Claimed':row.complete?'Claim reward':'Goal in progress';}
    const track=el.querySelector('[data-action="challenge-track"]');if(track){track.disabled=!row.unlocked||row.claimed;track.setAttribute('aria-pressed',String(row.tracked));track.textContent=row.tracked?'Tracking · stop':'Track goal';}
    const open=el.querySelector('[data-action="challenge-open"]');if(open)open.textContent=row.complete?'Claim →':'View →';
  }
  for(const el of document.querySelectorAll('[data-region-challenge-count]')){const local=rows.filter(row=>row.landId===el.dataset.regionChallengeCount);el.textContent=`${local.filter(row=>row.claimed).length} / ${local.length} claimed`;}
  const progress=landProgress(state);
  for(const el of unlocks){const land=LANDS.find(land=>land.id===el.dataset.regionUnlockProgress),unlocked=progress.unlocked.some(known=>known.id===land?.id);if(!land)continue;el.innerHTML=regionUnlockProgress(land,progress,unlocked);}
}
function regionUnlockProgress(land,progress,unlocked){
  const percent=unlocked?100:C.clamp(progress.totalDistance/Math.max(1,land.unlock)*100,0,100);
  return `<span class="challenge-count"><strong>${unlocked?'Region unlocked':`${landDistance(progress.totalDistance)} / ${landDistance(land.unlock)}`}</strong><small>${Math.floor(percent)}%</small></span><span class="challenge-meter" role="progressbar" aria-label="Unlock ${esc(land.name)}" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${Math.floor(percent)}"><i style="width:${percent}%"></i></span>`;
}
function openLands(id){
  landPreviewId=id||null;
  const p=landProgress(state),land=LANDS.find(l=>l.id===id),unlocked=land&&p.unlocked.some(l=>l.id===land.id);
  const content=land?`<button type="button" class="text-button land-back" data-action="lands">← All ten lands</button><div class="land-preview-art">${landArt(land.id,{uid:'preview-land'})}</div><span class="eyebrow">${unlocked?'YOUR NEXT ADVENTURE AWAITS':'A LITTLE FARTHER DOWN THE TRAIL'}</span><h2>${esc(land.name)}</h2><p class="land-preview-description">${esc(land.description)}</p><div class="land-preview-landmark">${uiIcon('leaf')}<span>${esc(land.landmark)}</span></div><p class="land-unlock-copy">${unlocked?'Unlocked · return whenever you like.':`Unlocks at ${landDistance(land.unlock)} total walking · ${landDistance(land.unlock-p.totalDistance)} to go.`}</p><div class="region-unlock-progress" data-region-unlock-progress="${land.id}">${regionUnlockProgress(land,p,unlocked)}</div>${unlocked?`<button type="button" class="primary wide" data-action="land-select" data-id="${land.id}" ${!['trail','camp'].includes(state.run.phase)?'disabled':''}>${p.activeId===land.id?'Back to the trail':`Visit ${esc(land.name)}`}${uiIcon('arrow')}</button>`:'<button type="button" class="primary wide" data-action="close-dialog">Keep exploring</button>'}${regionChallengeSection(land.id)}${regionAdventureUI(state,land.id)}`:`<span class="eyebrow">TEN LANDS. A WORLD OF WONDER.</span><h2>Where will your feet take you?</h2><p class="land-atlas-intro">Every walk opens up a little more of the world. Peek at any land, then revisit the places you’ve unlocked.</p><div class="land-atlas-total"><b>${landDistance(p.totalDistance)}</b><span>${state.mode==='practice'?'Separate practice distance':'Total walking distance'} · ${p.unlocked.length} of ${LANDS.length} lands unlocked</span></div><div class="land-atlas">${LANDS.map(l=>landTile(l,'atlas')).join('')}</div>`;
  modal(content,land?land.name:'World of Monkey Trails','lands-dialog');
  const heading=$('dialog-content').querySelector('h2');heading?.setAttribute('tabindex','-1');heading?.focus({preventScroll:true});$('inspect-dialog').scrollTop=0;
}
function updateLandMotion(){
  const scene=document.querySelector('.land-scene');if(!scene||!state)return;
  const pose=gps.active?animation:state.mode==='practice'?animation:'idle',traveler=scene.querySelector('.land-traveler');
  scene.classList.toggle('is-walking',gps.active||state.mode==='practice'&&pose==='walk');
  if(traveler&&traveler.dataset.pose!==pose){traveler.dataset.pose=pose;traveler.innerHTML=portrait(active(),{state:pose})+'<span class="traveler-shadow"></span>';hydrateHostedPortraits(traveler,{reducedMotion:prefs.reducedMotion||prefs.battery});}
}
function ultraCard(){const u=state.run.ultra;if(!u||['claimed','battle'].includes(u.status))return '';const p={species:u.species,types:C.SPECIES[u.species].types,stage:0},owned=collection().some(x=>x.species===u.species)||u.revealed||state.run.bonuses.feather>0,rv=radar.view(state)||{},eligible=u.status==='ready'||rv.withinRadius||u.safeAlternative&&u.challengeRemaining<=0;return `<section class="ultra-card"><div class="ultra-head"><div class="ultra-portrait ${owned?'':'silhouette'}">${portrait(p,{state:'arrival'})}</div><div><span class="eyebrow">ULTRA SIGHTING · ${esc(u.difficulty||'Challenging')}</span><h2>${owned?esc(C.SPECIES[u.species].name):'An extraordinary presence'}</h2><p>${u.safeAlternative?`${feet(u.challengeRemaining)} ft on any safe route`:rv.calibrating?'Calibrating position':`${feet(rv.distance)} ft · approx. ${Math.round(rv.bearing??u.bearing??0)}°`}</p></div></div><p class="fine">${u.safeAlternative?'Same Ultra, same rewards. Only accepted walking advances this challenge.':u.tracked?'Your selected target stays anchored. Radar cannot tell whether land is safe or public.':'Optional hunt. You can always choose a safe route instead.'}</p><div class="button-row">${!u.tracked?'<button data-action="ultra-track" class="primary">Track sighting</button>':''}${eligible?'<button data-action="ultra-preview" class="primary">Battle available</button>':''}${!u.safeAlternative?'<button data-action="ultra-safe">Cannot safely reach this location</button>':''}</div>${eligible?'<p class="fine">Stop safely, then confirm Begin Battle.</p>':''}</section>`;}
function discoveryText(){const d=state.run.lastDiscovery;if(!d)return 'Your next discovery is waiting.';if(d.category==='essence')return `${d.amount||1} ${C.ELEMENTS[d.id]?.name||''} essence added to your pack`;if(d.category==='fruit')return `${d.amount||2} training fruit added to your pack`;if(d.category==='fragment')return `${C.GEAR[d.id]?.name||'Equipment'} fragment found`;return `${C.ITEMS[d.id]?.name||'Trail supply'} added to your pack`;}
function pinnedGoalUI(){if(state.meta.pinnedGoal?.kind==='evolution')return FriendUI.renderEvolutionGoal(Friends.evolutionGoal(state,C.evolutionCost));return trackedRegionalGoalUI(state)||trackedChallengePanel();}
function hydrateAdventureSurfaces(){
  const p=state.collection[selectedId]||active();
  const fill=(selector,content)=>{const el=$('view').querySelector(selector);if(el)el.innerHTML=content;};
  fill('[data-companion-friendship]',FriendUI.renderFriendship(state,p));fill('[data-companion-memories]',FriendUI.renderMemories(p));fill('[data-saved-lineups]',FriendUI.renderLineups(state));
  const ready=Friends.readiness(state,p.id,{trainingCost:C.trainingCost,evolutionCost:C.evolutionCost,unlocked:C.unlocked});
  fill('[data-companion-readiness]',ready?`<button class="companion-readiness" data-action="companion-section" data-id="${ready.section}">${esc(ready.label)} →</button>`:'');
  FriendUI.observeCompanionScenes($('view'));
}
function pendingEncounterCard(){
  const p=state.run.pendingEncounter;if(!p)return '';const origin=LANDS.find(l=>l.id===p.originLandId),enemies=p.enemies||[];
  return `<section class="pending-encounter" aria-label="Encounter ready"><span class="eyebrow">ENCOUNTER READY</span><h3>A rival crew is waiting</h3><div class="pending-preview">${enemies.map(e=>portrait(e)).join('')}<p>${enemies.map(e=>esc(C.SPECIES[e.species]?.name)).join(' · ')}<small>${esc(origin?.name||'Trail')} · ${esc(p.habitat?.name||({trail:'Open trail',riverbank:'Riverbank',grove:'Grove',cave:'Cave formations',ruins:'Ancient ruins',landmark:'Landmark clearing'})[p.habitat]||'Open trail')} · ${esc(p.difficulty||state.difficulty)}</small></p></div><p class="fine">Keep walking for discoveries, XP and recovery. This same crew stays ready across saves and region changes.</p><div class="button-row"><button class="primary" data-action="pending-begin">Stop safely · Battle</button><button data-action="pending-pass">Pass encounter</button></div><small>Passing grants no battle XP, loot or recruitment.</small></section>`;
}
function walkRecap(row){
  if(!row)return '<h2>Walk finished</h2><p>No accepted walking has been recorded for this session yet.</p><p>Your expedition is ready to continue.</p>';
  return `<span class="eyebrow">WALK SESSION COMPLETE</span><h2>A little farther together.</h2><div class="recap-stats">${[[feet(row.distance),'accepted feet'],[row.discoveries,'discoveries'],[row.recruited,'monkeys befriended'],[row.trainingXp,'training XP'],[row.friendship,'friendship gained'],[row.victories,'victories']].map(([value,label])=>`<span><b>${value}</b>${label}</span>`).join('')}</div><h3>Regional exploration</h3><p>${Object.entries(row.regions||{}).map(([id,d])=>`${esc(LANDS.find(l=>l.id===id)?.name||id)}: ${feet(d)} ft${row.regionalGoals?.[id]?` · ${row.regionalGoals[id].discoveries||0} discoveries · ${row.regionalGoals[id].victories||0} victories`:''}`).join(' · ')||'Your next regional adventure is waiting.'}</p><div class="notice">Your expedition, supplies, injuries, and waiting encounter stay saved. Start walking whenever you are ready to continue.</div><div data-recap-goal></div><button class="primary wide" data-action="close-dialog">Back to the adventure</button>`;
}
function nextDiscovery(cacheLeft,progress){return `<div class="discovery ${Date.now()-lastDiscovery<1500?'discovered':''}"><div class="discovery-icon">${uiIcon('leaf')}</div><div class="discovery-copy"><span>NEXT DISCOVERY</span><strong>${feet(cacheLeft)}<small> ft away</small></strong></div><span class="discovery-spark">${uiIcon('pack')}</span><svg class="vine-meter" viewBox="0 0 320 25" preserveAspectRatio="none" aria-label="${Math.round(progress)} percent toward next discovery"><path class="vine-track" pathLength="100" d="M5 14C35 3 64 24 96 14S155 3 190 14 252 25 314 10"/><path class="vine-fill" pathLength="100" stroke-dasharray="${progress} 100" d="M5 14C35 3 64 24 96 14S155 3 190 14 252 25 314 10"/><path class="vine-leaves" d="M49 13q-9-12-15-5 2 8 15 5m74-5q5-11 13-6-1 8-13 6m107 9q8-12 17-7-3 9-17 7"/></svg></div>`;}
function walk(){
  const r=state.run;if(r.phase==='dead')return death();if(['battle','replacement'].includes(r.phase))return P.renderTutorial(state)+battle();if(r.phase==='befriend')return P.renderTutorial(state)+capture();if(r.phase==='camp')return camp();
  const interval=C.discoveryInterval(state),cacheLeft=Math.max(0,r.nextCache-r.distance),encounterLeft=Math.max(0,r.nextEncounter-r.distance),progress=C.clamp((1-cacheLeft/interval)*100,0,100);
  return `<div class="walk-layout"><section class="walk-main" data-mode="${state.mode}"><div class="tracking-controls"><button data-action="tracking" id="tracking-button" class="primary" ${state.mode==='practice'?'hidden':''}>Start walking</button>${state.mode==='practice'?practiceControls():''}<div class="gps-status-row"><p id="gps-status" class="gps-status"></p><p id="gps-meta" class="gps-meta"></p></div>${state.mode==='gps'?compactLocationPanel():''}</div><div class="trail-strip"><div class="distance-block"><span>EXPEDITION ${String(state.meta.runs).padStart(2,'0')}</span><strong>${feet(r.distance)}<small> ft</small></strong></div><div class="tracking-state"><b id="walking-state">${gps.active?'Walking':'Paused'}</b><span id="gps-quality">GPS waiting</span></div><span class="speed-limit"><b>10</b><small>MPH LIMIT</small></span></div>${landscape(nextDiscovery(cacheLeft,progress))}<div class="walk-goals" data-walk-goals>${pinnedGoalUI()}</div>${habitatUI(state)}${pendingEncounterCard()}<div class="team-heading"><span>ON YOUR TEAM</span><b>${team().length} / 3</b></div>${teamStrip()}${walkingBonuses()}<button class="text-button session-finish" data-action="finish-walk">Finish walk · view recap</button><div class="next-events"><span>${uiIcon('rival')}<i>Next rival<b>${r.pendingEncounter?'Encounter ready':`${feet(encounterLeft)} ft`}</b></i></span><span>${uiIcon('camp')}<i>Trail camp<b>${feet(r.nextCamp-r.distance)} ft</b></i></span></div><div class="recent-discovery">${uiIcon(r.lastDiscovery?'pack':'leaf')}<span>${esc(discoveryText())}</span></div></section><aside class="walk-aside">${ultraCard()}${P.renderTutorial(state)}<button class="wide activity-open" data-action="activity-open">Trail activity & saved moments</button>${journal()}</aside></div>`;
}
function statusList(p){return Object.entries(p.status||{}).map(([id,v])=>`<span class="status">${esc(id)} ${typeof v==='object'?v.turns??v.duration??'':v}r</span>`).join('');}
function fighter(p,isEnemy){const st=isEnemy?C.stats(p):pstats(p);return `<div class="fighter ${isEnemy?'enemy-fighter':''}">${portrait(p,{enemy:isEnemy,state:p.hp<=0?'knockout':isEnemy&&['attack','cast','signature'].includes(animation)?'hit':!isEnemy?animation:'idle'})}<h3>${esc(name(p))}</h3><div class="fighter-meta">Lv ${p.level} <span>${chips(p)}</span></div>${bar(p.hp,st.hp,'Health',isEnemy?'enemy':'')}${bar(p.energy,st.maxEnergy,'Energy','energy')}<div class="statuses">${statusList(p)}</div></div>`;}
function enemyStrip(enemies,current){return `<div class="enemy-strip" aria-label="Enemy team">${enemies.map(p=>`<div class="enemy-mini ${p===current?'active':''} ${p.hp<=0?'knocked-out':''}">${portrait(p,{enemy:true,state:p.hp<=0?'knockout':'idle'})}<span>${esc(species(p).name)}</span><i style="--hp:${Math.max(0,p.hp/C.stats(p).hp*100)}%"></i></div>`).join('')}</div>`;}
function moveTiming(p,id,b){const m=C.MOVES[id],cool=Math.max(0,(p.cooldowns?.[id]||0)-b.round),left=m.uses?Math.max(0,m.uses-(p.uses?.[id]||0)):null,relayBlock=m.switch&&(p.status.root||!team().some(x=>x.id!==p.id&&x.hp>0));return {blocked:cool>0||left===0||relayBlock,label:relayBlock?` · ${p.status.root?'Rooted':'Needs teammate'}`:cool?` · ${cool}r cooldown`:left!==null?` · ${left} uses left`:''};}
function effectivenessBadge(multiplier){
  const info=C.effectInfo(multiplier);
  return `<span class="effectiveness-badge is-${info.kind}" data-effectiveness="${info.kind}">${esc(info.label)} <b>×${Number(info.multiplier.toFixed(2))}</b></span>`;
}
function battleFeedback(hits=[]){
  const rows=(Array.isArray(hits)?hits:[]).filter(hit=>hit&&C.MOVES[hit.moveId]?.power).slice(-2);if(!rows.length)return '';
  return `<div class="battle-feedback" aria-live="polite" aria-label="Last battle actions">${rows.map(hit=>{const info=C.effectInfo(hit.multiplier),actor=hit.actorName||name(state.collection[hit.actorId]||{species:''});return `<div class="battle-hit is-${info.kind}" data-hit-side="${hit.side==='enemy'?'enemy':'player'}" data-effectiveness="${info.kind}"><span>${hit.side==='enemy'?'Rival':'Your crew'} · ${esc(actor)}</span><strong>${esc(C.MOVES[hit.moveId].name)} <b>${Math.max(0,Math.round(hit.damage||0))} damage${hit.critical?' · critical':''}</b></strong>${effectivenessBadge(hit.multiplier)}</div>`;}).join('')}</div>`;
}
function rarityDetails(p){
  const rarity=species(p).rarity||'common',bonus=C.CONFIG.rarity?.[rarity],boost=bonus?Math.round((bonus.stats-1)*100):0;
  return `<div class="rarity-strength ${rarity}" data-rarity="${rarity}"><strong>${esc(rarity[0].toUpperCase()+rarity.slice(1))} strength</strong><span>${boost?`+${boost}% maximum HP, attack and defense · +${Math.round((bonus.speed-1)*100)}% speed · +${bonus.energy} maximum energy`:'Base stats · adaptable strategy'}</span></div>`;
}
function evolutionEssence(offer){
  return `<span class="evolution-essence">${Object.entries(offer.cost||{}).map(([type,required])=>{const current=state.run.essence[type]||0,ready=current>=required,percent=C.clamp(current/Math.max(1,required)*100,0,100);return `<span class="essence-goal ${ready?'is-ready':'is-short'}" data-essence-type="${type}"><span><strong>${esc(C.ELEMENTS[type]?.name||type)} essence</strong><b><span data-essence-current>${current}</span> / <span data-essence-required>${required}</span></b></span><span class="essence-meter" role="progressbar" aria-label="${esc(C.ELEMENTS[type]?.name||type)} essence for evolution" aria-valuemin="0" aria-valuemax="${required}" aria-valuenow="${Math.min(current,required)}"><i style="width:${percent}%"></i></span><small>${ready?'Enough essence':`${required-current} more needed`}</small></span>`;}).join('')}</span>`;
}
function refreshEssenceProgress(){
  if(!state)return;
  for(const el of document.querySelectorAll('.essence-goal[data-essence-type]')){
    const required=Number(el.querySelector('[data-essence-required]')?.textContent)||0,current=state.run.essence[el.dataset.essenceType]||0,ready=current>=required;
    const value=el.querySelector('[data-essence-current]');if(value)value.textContent=current;
    el.classList.toggle('is-ready',ready);el.classList.toggle('is-short',!ready);
    const bar=el.querySelector('[role="progressbar"]');if(bar){bar.setAttribute('aria-valuenow',String(Math.min(current,required)));bar.querySelector('i').style.width=`${C.clamp(current/Math.max(1,required)*100,0,100)}%`;}
    const small=el.querySelector(':scope>small');if(small)small.textContent=ready?'Enough essence':`${required-current} more needed`;
  }
  const button=document.querySelector('#dialog-content [data-action="evolve"]'),p=state.collection[button?.dataset.monkey];
  if(button&&p){const offer=C.evolutionCost(state,button.dataset.id,p.id),ready=offer&&canPrepare()&&state.run.party.includes(p.id)&&p.hp>0&&p.level>=offer.level&&Object.entries(offer.cost).every(([t,n])=>(state.run.essence[t]||0)>=n);button.disabled=!ready;button.textContent=ready?'Commit to this evolution':'Gather essence and reach the required level';}
}
function evolutionGains(p,offer){
  const before=offer.beforeStats||pstats(p),after=offer.stats||C.stats(offer.monkey||{...p,stage:p.stage+1},C.equipped(state,p));
  return `<h3>Every stat grows</h3><div class="evolution-stat-gains">${[['hp','Max HP'],['atk','Attack'],['def','Defense'],['speed','Speed'],['maxEnergy','Max energy']].map(([key,label])=>`<span data-stat="${key}"><small>${label}</small><strong>${before[key]} → ${after[key]}</strong><b>+${after[key]-before[key]}</b></span>`).join('')}</div>`;
}
function battleLeadChoices(){
  return `<section class="battle-lead-picker" aria-label="Choose your first monkey"><h3>Who goes out first?</h3><p class="fine">Choose a conscious member of your current team. This opening choice costs no turn.</p><div class="team-strip battle-lead-choices" role="group" aria-label="Starting monkey">${team().map(p=>`<button type="button" class="team-slot ${p.id===state.run.activeId?'active':''} ${p.hp<=0?'knocked-out':''}" data-action="battle-lead" data-id="${esc(p.id)}" aria-pressed="${p.id===state.run.activeId}" ${p.hp<=0?'disabled':''}>${portrait(p,{state:p.hp<=0?'knockout':'idle'})}<strong>${esc(name(p))}</strong>${miniHealth(p)}<small>${p.hp<=0?'Knocked out':`${Math.round(p.hp)} / ${pstats(p).hp} HP · ${p.energy} EN`}</small><span class="lead-label">${p.hp<=0?'Needs revival':p.id===state.run.activeId?'Goes first':'Choose first'}</span></button>`).join('')}</div></section>`;
}
function battleOpening(){
  const opening=C.canChooseBattleLead(state);
  return `<div class="notice"><strong>Tracking is paused.</strong> Look up and stop in a safe place before playing.</div>${opening?battleLeadChoices():'<p class="fine">Your battle has already started. Use Switch during your turn to change monkeys.</p>'}<button class="primary wide" data-action="ready" ${active().hp>0?'':'disabled'}>${opening?`Begin battle with ${esc(name(active()))}`:'I have stopped safely · resume battle'}</button>`;
}
function battle(){
  const r=state.run,b=r.battle,p=active(),e=b.enemy,m=b.enemyAction?.type==='switch'?{name:'Switch teammate',cost:0,desc:'The rival spends its action bringing in a reserve.'}:C.MOVES[b.intent],replace=r.phase==='replacement';
  return `<section class="battle-panel"><div class="battle-title"><div><span class="eyebrow">${b.ultra?'ULTRA ENCOUNTER':esc(b.style||'WILD ENCOUNTER')}</span><h2>${b.ultra?esc(C.SPECIES[e.species].name):'A team on the trail'}</h2></div><span class="tag">ROUND ${b.round??b.turn??1}</span></div>${enemyStrip(b.enemies||[e],e)}<div class="battlefield">${battlefield(e.types)}${fighter(p,false)}<div class="versus">VS</div>${fighter(e,true)}</div><div class="player-bench">${teamStrip('monkey-actions')}</div>${battleFeedback(b.hitFeedback||[b.lastHit])}${!b.ready&&!replace?battleOpening():replace?`<div class="notice"><strong>${esc(name(p))} needs a rest.</strong> Choose a conscious teammate. This replacement gives the enemy no extra action.</div>${switchChoices(true)}`:`<div class="intent"><span>Enemy telegraph</span><strong>${esc(b.enemyAction?.intentLabel||m?.name||b.intent||'Preparing an action')}</strong><small>${esc(b.enemyAction?.intentDetail||(m?`${m.cost} energy · ${m.desc}`:''))}</small></div><div class="moves">${renderAbilityCards(state,{busy:combatBusy,escape:esc})}</div><div class="battle-actions"><button data-action="switch-menu" ${p.status.root?'disabled':''}>⇄ ${p.status.root?'Rooted':'Switch'}</button><button data-action="battle-heal" ${b.itemUsedRound===b.round?'disabled':''}>＋ ${b.itemUsedRound===b.round?'Supply used':'Supplies'}</button><button data-action="move-info">ⓘ Abilities</button></div><p class="fine battle-tip">${esc(battleActionHints(state).switch)} ${esc(battleActionHints(state).supply)}</p><p class="fine battle-tip">One supply per round, plus your attack or ability. Switching uses your action. Combat XP: +${b.contributions?.[p.id]?.xp||0} from ${Math.round(b.contributions?.[p.id]?.damage||0)} damage / ${Math.round(b.contributions?.[p.id]?.healing||0)} healing.</p>`}</section>${journal()}`;
}
function switchChoices(forced=false){const e=state.run.battle.enemy;return `<div class="switch-list">${team().filter(p=>p.id!==state.run.activeId).map(p=>`<button data-action="${forced?'replace':'switch-preview'}" data-id="${p.id}" ${p.hp<=0?'disabled':''}>${portrait(p,{state:p.hp<=0?'knockout':'idle'})}<span><strong>${esc(name(p))}</strong><small>${Math.round(p.hp)} / ${pstats(p).hp} HP · ${p.energy} EN</small><small>${e.types.map(t=>`${C.ELEMENTS[t].name} ×${C.effectiveness(t,p.types)}`).join(' · ')}</small><small>${statusList(p)||'No status effects'}${state.run.battle.hazard?` · Hazard: ${esc(state.run.battle.hazard.name||state.run.battle.hazard)}`:''}</small></span><b>${p.hp<=0?'KO':'→'}</b></button>`).join('')}</div>`;}
function profile(p){const st=pstats(p);return `<div class="profile-art">${portrait(p,{state:p.hp<=0?'knockout':Friends.friendshipInfo(p).tier.pose})}<div><span class="eyebrow">${esc(species(p).role||species(p).rarity||'COMPANION')}</span><h2>${esc(name(p))}</h2><p>${esc(C.formName(p))}</p><small>${esc(Friends.friendshipInfo(p).tier.title)}</small><div class="type-chips">${chips(p)}</div><span class="fine">Lv ${p.level} · Bond ${p.bond||0} · Stage ${p.stage}/3</span></div></div>${companionPower(p)}${rarityDetails(p)}<p class="muted">${esc(species(p).trait)}</p>${bar(p.hp,st.hp,'Health')}${bar(p.energy,st.maxEnergy,'Energy','energy')}<div class="stat-grid">${[['atk','Attack'],['def','Defense'],['speed','Speed'],['hp','Max HP']].map(([k,l])=>`<span><b>${st[k]}</b>${l}</span>`).join('')}</div>`;}
function evolutionOffers(p){return `<div class="evolution-grid">${Object.keys(C.ELEMENTS).map(t=>{let o;try{o=C.evolutionPreview(state,t,p.id);}catch{return '';}if(!o)return '';return `<button class="evolution" data-action="evolution-preview" data-id="${t}" data-monkey="${p.id}"><span style="color:${C.ELEMENTS[t].color}">${C.ELEMENTS[t].icon}</span><strong>${esc(o.name||o.formName||C.ELEMENTS[t].name)}</strong><small>Requires Lv ${o.level??o.requiredLevel??3} · boosts every stat</small>${evolutionEssence(o)}<b>Preview all stat gains →</b></button>`;}).join('')}</div>`;}
function abilityIcon(kind){
  const paths={fire:'<path d="M13 2c1 5-3 5-1 9 2-1 3-3 3-5 5 5 7 9 3 13-3 3-9 3-12 0C2 15 4 9 8 6c-1 4 1 5 2 5 1-4 2-5 3-9Z"/>',leaf:'<path d="M4 20C1 8 8 3 21 3c0 13-5 20-17 17Zm0 0L16 8M10 14v-5m0 5h5"/>',water:'<path d="M12 2C9 7 4 11 4 15a8 8 0 0 0 16 0c0-4-5-8-8-13ZM8 15c0 3 2 4 4 4"/>',storm:'<path d="m13 2-9 12h7l-1 8 10-13h-7l1-7Z"/>',earth:'<path d="m3 19 5-12 5 5 3-8 6 15H3Zm5-12 2 12m3-7 3 7M3 22h19"/>',ice:'<path d="M12 2v20M3 7l18 10M3 17 21 7M8 4l4 3 4-3M8 20l4-3 4 3M3 11l4-2V5m14 8-4 2v4M3 13l4 2v4m14-8-4-2V5"/>',light:'<circle cx="12" cy="12" r="4"/><path d="M12 1v3m0 16v3M1 12h3m16 0h3M4 4l2 2m12 12 2 2M4 20l2-2M18 6l2-2"/>',shadow:'<path d="M20 16A9 9 0 0 1 8 4a9 9 0 1 0 12 12Z"/><path d="m17 3 1 2 2 1-2 1-1 2-1-2-2-1 2-1 1-2Z"/>',strike:'<path d="m5 19 2-8 3 2 1-9 3 1v8l3-6 2 2-3 10-4 3H8l-5-5 2-2 3 2"/>',claw:'<path d="m6 3-3 17 6-5M13 2 9 20l6-6M20 4l-4 17 5-5"/>',shield:'<path d="m12 2 9 4v6c0 5-5 8-9 10-4-2-9-5-9-10V6l9-4Z"/><path d="m8 12 3 3 5-6"/>',heal:'<path d="M12 21 3 12C-2 6 5 0 12 7c7-7 14-1 9 5l-9 9Z"/><path d="M12 9v7m-3-3h6"/>',energy:'<path d="M8 3h8v3h3v16H5V6h3V3Z"/><path d="m13 8-4 6h4l-2 5 5-7h-4l1-4Z"/>',switch:'<path d="M3 7h16l-4-4m4 4-4 4M21 17H5l4 4m-4-4 4-4"/>',poison:'<path d="M9 2h6m-5 0v6L4 18c-1 2 0 4 3 4h10c3 0 4-2 3-4L14 8V2M7 15h10"/><circle cx="10" cy="18" r=".6"/><circle cx="14" cy="12" r=".6"/>',slow:'<circle cx="12" cy="13" r="8"/><path d="M12 8v5l-3 2M9 2h6m-3 0v3"/>',weaken:'<path d="M12 3v14m-5-5 5 5 5-5M4 21h16"/>',focus:'<path d="M7 3H3v4m14-4h4v4M3 17v4h4m14-4v4h-4"/><circle cx="12" cy="12" r="4"/>',cleanse:'<path d="m12 2 2 7 7 3-7 2-2 8-2-8-7-2 7-3 2-7Z"/>',counter:'<path d="M4 11a8 8 0 1 1 2 7M4 5v6h6"/>',lock:'<rect x="5" y="10" width="14" height="12" rx="2"/><path d="M8 10V6a4 4 0 0 1 8 0v4m-4 5v3"/>'};
  return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.65" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${paths[kind]||paths.strike}</svg>`;
}
function moveTypes(m){return m.types?.length?m.types:[m.type||'neutral'];}
function moveIcon(m){return m.switch?'switch':m.teamHeal||m.heal?'heal':m.shield||m.teamShield||m.guard?'shield':m.energy||m.drainEnergy?'energy':m.status==='poison'?'poison':m.counter?'counter':m.prepare||m.rally?'focus':m.cleanse?'cleanse':m.type==='neutral'?'strike':m.type;}
function moveEffects(m,p){
  const effects=[],duration=m.duration||2,pct=n=>Math.round(n*100),add=(icon,label)=>effects.push({icon,label});
  if(m.heal)add('heal',`${pct(m.heal)}% self heal`);if(m.teamHeal)add('heal',`${pct(m.teamHeal)}% team heal`);
  if(m.shield)add('shield',`${pct(m.shield)}% shield · ${duration}r`);if(m.teamShield)add('shield',`${pct(m.teamShield)}% incoming shield · ${duration}r`);
  if(m.guard)add('shield',`${p?.species==='mandrill'?70:55}% block · +2 EN`);if(m.counter)add('counter',`${pct(m.counter)}% next-hit counter`);
  if(m.status==='burn'||m.status==='poison')add(m.status==='burn'?'fire':'poison',`${m.status} · 4% HP × ${duration}r`);
  if(m.status==='slow')add('slow',`−35% speed · ${duration}r`);if(m.statusSelf==='speed')add('storm',`+20% speed · ${duration}r`);
  if(m.weaken)add('weaken',`−${pct(typeof m.weaken==='number'?m.weaken:.25)}% next attack`);
  if(m.root||m.status==='root')add('leaf',`No voluntary switch · ${typeof m.root==='number'?m.root:duration}r`);
  if(m.energy)add('energy',`Restore ${m.energy} EN`);if(m.drainEnergy)add('energy',`Steal up to ${m.drainEnergy} EN`);
  if(m.drain)add('heal',`Heal ${pct(m.drain)}% damage dealt`);if(m.cleanse)add('cleanse','Clear own effects');
  if(m.prepare)add('focus',`+${pct(m.prepare)}% next attack`);if(m.rally)add('focus',`+${pct(typeof m.rally==='number'?m.rally:.18)}% next entrant`);
  if(m.switch)add('switch','Switch teammate');if(m.transferSpeed)add('storm','Pass speed boost');if(m.entryEnergy)add('energy',`Incoming +${m.entryEnergy} EN`);
  if(m.priority)add(m.priority>0?'storm':'slow',`Priority ${m.priority>0?'+':''}${m.priority}`);if(m.breakShield)add('claw','Break shield & guard');
  if(m.bondBonus)add('heal',`+${pct(m.bondBonus)} capture points`);
  return effects;
}
function abilityCard(id,p,{action='',slot=null,disabled=false,label='',extra='',className='',battle=false}={}){
  const m=C.MOVES[id];if(!m)return `<button type="button" class="ability-card ability-empty" data-action="ability-open" data-monkey="${esc(p.id)}" data-ability-slot="${slot}"><span class="ability-slot">Ability ${Number(slot)+1}</span><strong>Open technique slot</strong><span>Choose an unlocked ability.</span></button>`;
  const types=moveTypes(m),colors=types.map(t=>C.ELEMENTS[t]?.color||'#a8b5aa'),tag=action?'button':'article',duration=m.duration||2;
  const effect=moveEffects(m,p),st=p?.id?pstats(p):null,heal=st&&m.heal?Math.round(st.hp*m.heal*(p.species==='langur'?1.25:1)):null,shield=st&&m.shield?Math.round(st.hp*m.shield):null;
  return `<${tag} class="ability-card ${m.signature?'signature':''} ${types.length>1?'hybrid':''} ${className}" style="--ability-color:${colors[0]};--ability-second:${colors[1]||colors[0]}" ${action?`type="button" data-action="${action}" data-id="${id}" data-monkey="${esc(p.id)}" ${slot!==null?`data-ability-slot="${slot}"`:''} ${disabled?'disabled':''}`:''} ${label?`aria-label="${esc(label)}"`:''}>
    <span class="ability-topline"><span class="ability-slot">${slot!==null?`ABILITY ${Number(slot)+1}`:m.signature?'SIGNATURE TECHNIQUE':'TECHNIQUE'}</span>${extra?`<span class="ability-state">${esc(extra)}</span>`:''}</span>
    <span class="ability-title"><span class="ability-art">${abilityIcon(moveIcon(m))}</span><strong>${esc(m.name)}</strong></span>
    <span class="ability-elements">${types.map(t=>`<span>${abilityIcon(t==='neutral'?'strike':t)}${esc(C.ELEMENTS[t]?.name||'Neutral')}</span>`).join('<span class="element-plus">+</span>')}${types.length>1?'<small>split elements</small>':''}</span>
    <span class="ability-values">${m.power?`<span><b>${m.power}</b> power</span>`:heal!==null?`<span><b>${heal}</b> HP heal</span>`:shield!==null?`<span><b>${shield}</b> shield</span>`:m.teamHeal?`<span><b>${Math.round(m.teamHeal*100)}%</b> team HP</span>`:m.energy?`<span><b>+${m.energy}</b> energy</span>`:'<span>Support</span>'}<span class="ability-energy">${abilityIcon('energy')}<b>${m.cost||0}</b> EN</span></span>
    <span class="ability-description">${esc(m.desc)}</span>
    ${effect.length?`<span class="ability-effects">${effect.map(x=>`<span>${abilityIcon(x.icon)}${esc(x.label)}</span>`).join('')}</span>`:''}
    <span class="ability-footer">${m.cooldown?`${m.cooldown}-round cooldown`:'No cooldown'}${m.uses?` · ${m.uses} use${m.uses===1?'':'s'} / encounter`:''}${battle?'':m.minStage?` · Stage ${m.minStage}`:''}</span>
  </${tag}>`;
}
function loadout(p){return `<div class="loadout ability-grid">${Array.from({length:4},(_,i)=>abilityCard(p.loadout[i],p,{action:'ability-open',slot:i,label:`Ability ${i+1}: ${C.MOVES[p.loadout[i]]?.name||'Open slot'}. ${i===0?'Required technique. View details.':canPrepare()?'Tap to replace.':'View details. Changes unlock between encounters.'}`,extra:i===0?'Required':canPrepare()?'Change ↗':'View details'})).join('')}</div><p class="loadout-hint">${abilityIcon('switch')} ${canPrepare()?'Tap a card to compare and instantly equip another technique.':'Ability changes are available between encounters.'} Palm Strike stays in slot 1.</p>`;}
function abilityPool(p){return [...new Set([...C.unlocked(p),...C.unlocked({...p,stage:3}),...(species(p).learnset||[]),...(p.learned||[])])].filter(id=>C.MOVES[id]);}
function comparisonSummary(current,next){const a=C.MOVES[current],b=C.MOVES[next];if(!a||!b)return 'Choose a technique below to compare.';const power=(b.power||0)-(a.power||0),cost=(b.cost||0)-(a.cost||0);return `${power?`${power>0?'+':''}${power} power`:'Same power'} · ${cost?`${cost>0?'+':''}${cost} energy cost`:'Same energy cost'}${moveTypes(b).join('+')!==moveTypes(a).join('+')?' · Different elemental coverage':''}. Power is a base value; final damage also uses stats and elemental matchups.`;}
function abilityComparison(p,slot,candidate){return `<div class="ability-compare-heading"><h3>Compare techniques</h3><span>Focus a card or tap Compare</span></div><div class="ability-comparison"><div><span class="comparison-label">CURRENT · SLOT ${slot+1}</span>${abilityCard(p.loadout[slot],p,{className:'comparison-card'})}</div><div id="ability-proposed"><span class="comparison-label">PROPOSED REPLACEMENT</span>${candidate?abilityCard(candidate,p,{className:'comparison-card'}):'<div class="comparison-placeholder">Palm Strike is required in this slot.</div>'}</div></div><p id="ability-comparison-summary" class="comparison-summary" aria-live="polite">${esc(comparisonSummary(p.loadout[slot],candidate))}</p>`;}
function openAbilities(monkeyId,slot){
  const p=state.collection[monkeyId];if(!p||slot<0||slot>3)return;const current=p.loadout[slot],available=C.unlocked(p),replaceable=canPrepare()&&slot!==0,candidate=replaceable?available.find(id=>!p.loadout.includes(id)):null;
  abilityEditor={monkeyId,slot,candidateId:candidate};
  const locked=abilityPool(p).filter(id=>!available.includes(id));
  modal(`<section class="ability-picker"><span class="eyebrow">${esc(name(p))} · TECHNIQUE LIBRARY</span><h2 id="ability-picker-title" tabindex="-1">${replaceable?`Replace ability ${slot+1}`:'Equipped technique'}</h2><p class="ability-picker-intro">${slot===0?'Palm Strike is your required zero-energy action. It always stays in slot 1.':canPrepare()?'Tap any available technique to equip it immediately. Changes save automatically.':'Inspect your techniques here. Loadout changes unlock between encounters.'}</p>${abilityComparison(p,slot,candidate)}<div class="ability-options-heading"><h3>Unlocked for ${esc(name(p))}</h3><span>${available.length} techniques</span></div><div class="ability-options ability-grid">${available.map(id=>{const equippedSlot=p.loadout.indexOf(id),isCurrent=id===current,blocked=!replaceable||equippedSlot>=0;return `<div class="ability-option ${isCurrent?'is-current':''}" data-compare-id="${id}">${abilityCard(id,p,{action:'ability-equip',disabled:blocked,className:isCurrent?'is-current':'',extra:isCurrent?'Current':equippedSlot>=0?`In slot ${equippedSlot+1}`:replaceable?'Tap to equip':'Unlocked'})}<button type="button" class="ability-compare-button" data-action="ability-compare" data-id="${id}" ${slot===0?'disabled':''}>Compare ${esc(C.MOVES[id].name)}</button></div>`;}).join('')}</div>${locked.length?`<details class="ability-locked"><summary>${abilityIcon('lock')} ${locked.length} future techniques · evolution unlocks</summary><div class="ability-grid">${locked.map(id=>`<div class="locked-technique">${abilityCard(id,p,{extra:`Unlock: stage ${C.MOVES[id].minStage||1}`})}<p>${esc(typeof C.abilityInfo==='function'?C.abilityInfo(p,id)?.lockedReason||`Reach evolution stage ${C.MOVES[id].minStage||1}.`:`Reach evolution stage ${C.MOVES[id].minStage||1}.`)}</p></div>`).join('')}</div></details>`:''}<div class="ability-picker-actions"><span>Four techniques. One distinct strategy.</span><button type="button" data-action="close-dialog">${replaceable?'Cancel · keep loadout':'Done'}</button></div></section>`,'Ability replacement','ability-dialog');
  $('ability-picker-title')?.focus();
}
function previewAbility(id){if(!abilityEditor||!C.MOVES[id])return;const p=state.collection[abilityEditor.monkeyId];if(!p)return;abilityEditor.candidateId=id;$('ability-proposed').innerHTML=`<span class="comparison-label">PROPOSED REPLACEMENT</span>${abilityCard(id,p,{className:'comparison-card'})}`;$('ability-comparison-summary').textContent=comparisonSummary(p.loadout[abilityEditor.slot],id);document.querySelectorAll('.ability-option').forEach(el=>el.classList.toggle('is-proposed',el.dataset.compareId===id));}
function levelProgress(p){
  if(p.level>=C.CONFIG.maxLevel)return '';
  const cost=C.trainingCost(p);
  return [['xp','XP',p.training??0,cost.xp],['fruit','Fruit',state.run.fruit,cost.fruit]].map(([key,label,amount,required])=>{
    const ready=amount>=required,percent=Math.floor(amount/required*1000)/10,filled=C.clamp(amount/required*100,0,100);
    return `<div class="level-resource ${ready?'is-ready':'is-short'}" data-level-resource="${key}"><div class="level-resource-label"><strong>${label}</strong><span>${amount.toLocaleString()} / ${required.toLocaleString()} · ${percent.toLocaleString()}%</span></div><div class="level-resource-track" role="progressbar" aria-label="${label} toward next level" aria-valuemin="0" aria-valuemax="${required}" aria-valuenow="${Math.min(amount,required)}" aria-valuetext="${amount} of ${required} ${label}, ${percent} percent, ${ready?'ready':`${required-amount} more needed`}"><i style="width:${filled}%"></i></div><small>${ready?'✓ Enough '+label:`${(required-amount).toLocaleString()} more ${label} needed`}</small></div>`;
  }).join('');
}
function trainingPanel(p){
  const max=p.level>=C.CONFIG.maxLevel,cost=C.trainingCost(p);
  return `<div class="training-row" data-training-monkey="${p.id}"><div class="training-copy"><strong>${max?'Maximum level reached':`Train to level ${p.level+1}`}</strong><small class="training-summary">${max?`Level ${p.level} · ${p.training??0} XP available`:`${cost.fruit} fruit + ${cost.xp} XP · ${p.training??0} XP available`}</small><div class="training-meters">${levelProgress(p)}</div><small>Walking heals 2% max HP / 100 ft · Walking + victory + actual damage/healing · ${C.xpMultiplier(p)}× XP at this level</small></div><button data-action="train" data-id="${p.id}" ${!canPrepare()||!state.run.party.includes(p.id)||p.hp<=0||max||state.run.fruit<cost.fruit||p.training<cost.xp?'disabled':''}>${max?'Max level':'Train'}</button></div>`;
}
function refreshTrainingProgress(){
  const row=document.querySelector('[data-training-monkey]'),p=state?.collection[row?.dataset.trainingMonkey];if(!row||!p)return;
  row.querySelector('.training-meters').innerHTML=levelProgress(p);
  const cost=C.trainingCost(p),max=p.level>=C.CONFIG.maxLevel;
  row.querySelector('.training-summary').textContent=max?`Level ${p.level} · ${p.training??0} XP available`:`${cost.fruit} fruit + ${cost.xp} XP · ${p.training??0} XP available`;
  row.querySelector('[data-action="train"]').disabled=!canPrepare()||!state.run.party.includes(p.id)||p.hp<=0||max||state.run.fruit<cost.fruit||p.training<cost.xp;
}
function equipmentPanel(p){return `<h2>Permanent equipment</h2>${ForgeUI.renderEquipmentSlots(state,p)}<button type="button" data-action="pack" class="text-button wide">Mix resources in the Canopy Forge →</button>`;}
function companionDetails(p){return `<section class="companion-detail-view"><div class="section-title"><h2>${esc(name(p))} · Details</h2><button type="button" data-action="companion-details-back">← Back</button></div><div class="companion-layout section-gap"><section class="panel companion-profile">${profile(p)}<div data-companion-friendship></div><details class="companion-details"><summary>Details · name & adventure records</summary><label for="monkey-nickname">Nickname</label><div class="input-row"><input id="monkey-nickname" maxlength="24" value="${esc(name(p))}"><button data-action="rename" data-id="${p.id}">Save</button></div><p class="fine">Highest discovered form: ${p.highestForm?esc(C.formName({...p,...p.highestForm})):esc(C.formName(p))}. Discovered paths remain after defeat.</p><div data-companion-memories></div></details></section><section class="panel companion-controls"><div class="companion-sections" role="group" aria-label="Companion actions">${[['train','Train'],['abilities','Abilities'],['evolve','Evolve'],['equipment','Equipment']].map(([id,label])=>`<button data-action="companion-section" data-id="${id}" aria-pressed="${companionSection===id}">${label}</button>`).join('')}</div><div data-companion-readiness></div>${companionSection==='train'?trainingPanel(p):companionSection==='abilities'?`<div class="ability-section-heading"><h2>Four equipped abilities</h2><span>${C.unlocked(p).length} unlocked</span></div>${loadout(p)}`:companionSection==='evolve'?`<h2>Evolution paths</h2><p class="muted">Preview every stat gain and exact essence requirements.</p>${evolutionOffers(p)}`:equipmentPanel(p)}</section></div>${speciesHuntUI(state,p.species)}</section>`;}
function teamView(){
  const p=state.collection[selectedId]||active();selectedId=p.id;
  return `<div class="section-title"><div><span class="eyebrow">EXPEDITION TEAM</span><h2>Three companions. One adventure.</h2></div><span class="tag">${team().length} / 3</span></div>${crewPowerSummary()}${teamStrip('monkey-actions',true)}<p class="fine">Tap a monkey for Swap or Details. ${canSwap()?'Choose who fills each lineup slot.':'Finish the encounter to change your lineup.'}</p>${canSwap()&&team().length<3?'<button type="button" data-action="lineup-open-sanctuary">Add a companion from the Sanctuary</button>':''}${companionDetailOpen?companionDetails(p):'<div class="panel companion-details-prompt"><h3>Choose a monkey</h3><p>Tap Details to train, change abilities, evolve or equip your companion.</p></div>'}<div data-saved-lineups></div>`;
}
function collectionDetail(id){
  const s=C.SPECIES[id];if(!s)return '';const individuals=collection().filter(p=>p.species===id),p=individuals.find(x=>x.id===selectedId)||individuals[0];
  if(p){selectedId=p.id;return `<section class="panel collection-detail" id="selected-companion" aria-label="Selected companion"><div class="collection-detail-heading"><span class="eyebrow">SELECTED COMPANION · ${esc(s.rarity)}</span><button type="button" class="text-button" data-action="collection-detail-close">Close details ×</button></div>${speciesHuntUI(state,id)}${individuals.length>1?`<div class="individual-tabs" aria-label="Your ${esc(s.name)} companions">${individuals.map(x=>`<button type="button" data-action="collection-individual" data-id="${x.id}" aria-pressed="${x.id===p.id}">${esc(name(x))} · Lv ${x.level}</button>`).join('')}</div>`:''}<div class="collection-detail-grid"><div class="selected-profile">${profile(p)}<p class="fine">Highest discovered form: ${esc(C.formName({...p,...p.highestForm}))}. ${team().some(x=>x.id===p.id)?'Traveling in your active team.':'Resting safely in your sanctuary.'}</p><button type="button" data-action="inspect-team" data-id="${p.id}" class="wide">Training, evolution & team →</button></div><div class="selected-loadout"><div class="ability-section-heading"><h3>Four equipped abilities</h3><span>${C.unlocked(p).length} unlocked</span></div>${loadout(p)}</div></div><details class="collection-evolutions"><summary>Explore evolution paths</summary>${evolutionOffers(p)}</details></section>`;}
  const preview={species:id,types:s.types||[],stage:0,level:1};return `<section class="panel collection-detail undiscovered-detail" id="selected-companion" aria-label="Undiscovered species"><div class="collection-detail-heading"><span class="eyebrow">FIELD GUIDE · ${esc(s.rarity)} · UNDISCOVERED</span><button type="button" class="text-button" data-action="collection-detail-close">Close details ×</button></div>${speciesHuntUI(state,id)}<div class="unknown-profile"><div class="species-hero">${portrait(preview)}<h2>${esc(s.name)}</h2><div class="type-chips">${chips(preview)}</div></div><div><span class="eyebrow">${esc(s.role)} · #${String(s.number).padStart(2,'0')}</span><h3>${esc(s.title||s.name)}</h3><p>${esc(s.trait)}</p>${rarityDetails(preview)}<p class="notice">${s.rarity==='ultra'?'An extraordinary boss companion. Look for a special Ultra map sighting and defeat its team to earn a guaranteed invitation.':'Explore the trail and befriend this species after a wild encounter to discover its forms and customize its techniques.'}</p><div class="stat-grid">${[['hp','Base HP'],['atk','Attack'],['def','Defense'],['speed','Speed']].map(([key,label])=>`<span><b>${C.stats(preview)[key]}</b>${label}</span>`).join('')}</div><h3>Species techniques</h3><p class="fine">${(s.learnset||[]).map(move=>esc(C.MOVES[move]?.name||move)).join(' · ')}</p></div></div></section>`;
}
function sanctuaryView(){
  const roster=Object.entries(C.SPECIES),companions=collection(),powerRanks=rosterRanks(),rankById=new Map(powerRanks.map(row=>[row.monkey.id,row])),strongestBySpecies=new Map([...powerRanks].reverse().map(row=>[row.monkey.species,row])),strongest=id=>strongestBySpecies.get(id),owned=new Set(companions.map(p=>p.species)),ranks={common:1,uncommon:2,rare:3,ultra:4},search=collectionSearch.trim().toLowerCase(),individuals=id=>companions.filter(p=>p.species===id),latest=id=>Math.max(-1,...companions.map((p,i)=>p.species===id?i:-1)),level=id=>Math.max(0,...individuals(id).map(p=>p.level));
  let filtered=roster.filter(([id,s])=>(collectionFilter!=='owned'||owned.has(id))&&(!search||`${s.name} ${individuals(id).map(name).join(' ')}`.toLowerCase().includes(search))&&(collectionRarity==='all'||s.rarity===collectionRarity)&&(collectionElement==='all'||(s.types||[]).includes(collectionElement)||individuals(id).some(p=>p.types.includes(collectionElement))));
  filtered.sort(([a,sa],[b,sb])=>collectionSort==='power'?(strongest(b)?.power||0)-(strongest(a)?.power||0)||sa.number-sb.number:collectionSort==='level'?level(b)-level(a)||sa.number-sb.number:collectionSort==='rarity'?(ranks[sb.rarity]||0)-(ranks[sa.rarity]||0)||sa.number-sb.number:collectionSort==='recent'?latest(b)-latest(a)||sa.number-sb.number:sa.number-sb.number);
  let entries=collectionFilter==='owned'?filtered.flatMap(([id,s])=>individuals(id).filter(p=>(!search||`${s.name} ${name(p)}`.toLowerCase().includes(search))&&(collectionElement==='all'||p.types.includes(collectionElement))).map(p=>({id,s,p}))):filtered.map(([id,s])=>({id,s,p:(collectionSort==='power'?strongest(id)?.monkey:individuals(id).sort((a,b)=>b.level-a.level)[0])||{species:id,types:s.types||[],stage:0}}));
  if(collectionFilter==='owned')entries.sort((a,b)=>collectionSort==='power'?powerOf(b.p)-powerOf(a.p)||a.s.number-b.s.number:collectionSort==='level'?b.p.level-a.p.level||a.s.number-b.s.number:collectionSort==='rarity'?ranks[b.s.rarity]-ranks[a.s.rarity]||a.s.number-b.s.number:collectionSort==='recent'?companions.indexOf(b.p)-companions.indexOf(a.p):a.s.number-b.s.number);
  const perPage=12,pages=Math.max(1,Math.ceil(entries.length/perPage));collectionPage=C.clamp(collectionPage,0,pages-1);
  return `<div class="section-title"><div><span class="eyebrow">PERMANENT SANCTUARY</span><h2>Your wild companions</h2><p>${owned.size} / ${roster.length} species · ${companions.length} individuals</p></div><span class="collection-badge">${owned.size}<small>discovered</small></span></div>${FriendUI.renderSanctuary(state,{portrait,reducedMotion:prefs.reducedMotion||prefs.battery})}<div class="collection-power-summary"><span>Your crew <strong>${powerCount(startingPower())} power</strong></span><span>${companions.length} companions ranked by permanent power</span></div><div class="collection-toolbar panel"><div class="segmented"><button data-action="collection-filter" data-id="owned" aria-pressed="${collectionFilter==='owned'}">Collected</button><button data-action="collection-filter" data-id="all" aria-pressed="${collectionFilter==='all'}">Field guide · ${roster.length}</button></div><label class="collection-search" for="collection-search">Search companions<input type="search" id="collection-search" placeholder="Search a species or nickname…" value="${esc(collectionSearch)}" autocomplete="off"></label><div class="collection-control-row"><label for="collection-element">Element<select id="collection-element" data-collection-control="element"><option value="all">All elements</option>${Object.entries(C.ELEMENTS).map(([key,e])=>`<option value="${key}" ${collectionElement===key?'selected':''}>${e.name}</option>`).join('')}</select></label><label for="collection-rarity">Rarity<select id="collection-rarity" data-collection-control="rarity">${[['all','All rarities'],['common','Common'],['uncommon','Uncommon'],['rare','Rare'],['ultra','Ultra']].map(([key,label])=>`<option value="${key}" ${collectionRarity===key?'selected':''}>${label}</option>`).join('')}</select></label><label for="collection-sort">Sort by<select id="collection-sort" data-collection-control="sort">${[['number','Field guide #'],['power','Highest power'],['level','Highest level'],['rarity','Rarest first'],['recent','Recently captured']].map(([key,label])=>`<option value="${key}" ${collectionSort===key?'selected':''}>${label}</option>`).join('')}</select></label></div><div class="collection-results"><span role="status">${entries.length} matching ${collectionFilter==='owned'?'companions':'species'}${collectionSearch?` for “${esc(collectionSearch)}”`:''}</span><button type="button" class="text-button" data-action="collection-reset">Reset filters</button></div></div>${selectedSpecies&&!owned.has(selectedSpecies)?collectionDetail(selectedSpecies):'<p class="collection-invitation">Tap a collected monkey for lineup options or Details.</p>'}<div class="collection-grid">${entries.slice(collectionPage*perPage,(collectionPage+1)*perPage).map(({id,s,p})=>{const members=individuals(id),ranked=rankById.get(p.id),ready=p.id?upgradeReadiness(p.id):{train:false,evolve:false},inLineup=state.run.party.includes(p.id);return `<button type="button" class="collection-card ${s.rarity==='ultra'?'ultra':''} ${members.length?'':'uncollected'} ${upgradeClasses(ready)}" data-action="${p.id?'monkey-actions':'species'}" data-id="${esc(p.id||id)}" data-species-id="${esc(id)}" ${p.id?`data-upgrade-monkey="${esc(p.id)}"`:''} aria-label="${esc(p.id?name(p):s.name)}, ${esc(s.rarity)}, ${p.id?`level ${p.level}, ${inLineup?'in lineup':'in Sanctuary'}`:'undiscovered'}"><span class="collection-number">${String(s.number).padStart(2,'0')}</span><span class="discovery-mark ${members.length?'discovered':''}">${members.length?'✓':'?'}</span>${portrait(p)}<strong>${esc(p.id?name(p):s.name)}</strong><small>${p.id?`Lv ${p.level} · ${inLineup?'In lineup':'Sanctuary'}`:'Undiscovered'}</small><span class="upgrade-statuses">${upgradeBadges(ready)}</span>${ranked?`<span class="collection-power" data-monkey-power="${ranked.power}" data-roster-rank="${ranked.rank}"><b>${powerCount(ranked.power)} power</b><small>#${ranked.rank} in sanctuary</small></span>`:''}<span class="rarity-label ${s.rarity}">${esc(s.rarity)}</span><span class="slot-elements">${(p.types||[]).map(t=>`<span style="color:${C.ELEMENTS[t].color}">${C.ELEMENTS[t].icon}</span>`).join('')||'◇'}</span></button>`;}).join('')}</div>${!entries.length?'<div class="collection-empty"><h3>No companions found</h3><p>Try another name, element, or rarity.</p><button type="button" data-action="collection-reset">Clear filters</button></div>':''}<div class="pagination"><button data-action="collection-page" data-id="-1" ${collectionPage===0?'disabled':''}>← Previous</button><span>${collectionPage+1} / ${pages}</span><button data-action="collection-page" data-id="1" ${collectionPage===pages-1?'disabled':''}>Next →</button></div><p class="fine">Power ranks compare your collected individuals; tied companions share a rank. Collected shows every individual, including duplicate species. Select Highest power to find your strongest companions. Your active expedition team holds up to three companions.</p>`;
}
function captureDisabled(c){return c.claimed||c.attempts>=c.maxAttempts||(!(c.ultra&&c.guaranteed)&&state.run.items.bondCharm<1);}
function capture(){const c=state.run.capture;if(!c)return '';return `${battleFeedback(state.run.lastBattleFeedback)}<section class="panel capture-panel"><span class="eyebrow">A NEW FRIENDSHIP?</span><h1>Befriend a wild companion.</h1><p>Recruit one opponent per encounter. Choose who to invite.</p><div class="capture-candidates">${c.candidates.map(p=>{const chance=C.captureChance(state,p.id),duplicates=collection().filter(x=>x.species===p.species).length;return `<article class="capture-card">${portrait(p,{state:'victory'})}<h2>${esc(species(p).name)}</h2><div class="type-chips">${chips(p)}</div><p>${esc(species(p).trait)}</p><div class="capture-odds"><strong>${Math.round(chance>1?chance:chance*100)}%</strong><span>${c.guaranteed?'Guaranteed invitation':`${Math.max(0,c.maxAttempts-c.attempts)} attempts left`}</span></div><label for="capture-name-${esc(p.id)}">Nickname <small>Optional · up to 24 characters</small></label><input id="capture-name-${esc(p.id)}" maxlength="24" placeholder="${esc(species(p).name)}">${duplicates?'<p class="fine">Duplicate species: keep the individual or choose species bond materials after recruitment.</p>':''}<button class="primary wide" data-action="befriend" data-id="${p.id}" ${captureDisabled(c)?'disabled':''}>${c.ultra&&c.guaranteed?'Invite Ultra · no charm needed':'Use 1 Bond Charm'}</button><p class="fine">${team().length<3?'An open team slot is available.':'Your team is full. New friends wait in your sanctuary; add them to your lineup after the encounter.'}</p></article>`;}).join('')}</div><p class="fine">Failures build persistent bad-luck protection. Combat bond bonuses improve the displayed chance.</p><button class="text-button" data-action="capture-skip">Continue without recruiting</button></section>`;}
function camp(){const r=state.run;return `<section class="panel camp-panel"><span class="eyebrow">A SAFE PLACE TO PREPARE</span><h1>Canopy camp</h1><p>${r.camp?.fullRecovery?'Your entire collection was restored to full health on arrival. ':''}${r.camp?.heals??0} camp recovery uses remain.</p>${r.camp?.equipmentReward?`<p class="notice">Supply Satchel · +${r.camp.equipmentReward.count} uncommon ${esc(C.ELEMENTS[r.camp.equipmentReward.element]?.name||'elemental')} essence.</p>`:''}${teamStrip('monkey-actions')}${team().map(p=>`<div class="item-card"><div><h3>${esc(name(p))}</h3><p>${Math.round(p.hp)} / ${pstats(p).hp} HP${p.hp<=0?' · Revive before replacing.':''}</p></div><button data-action="${p.hp<=0?'revive':'camp-heal'}" data-id="${p.id}" ${(p.hp<=0?!(r.items.revive>0):!(r.camp?.heals>0))?'disabled':''}>${p.hp<=0?`Revive (${r.items.revive||0})`:'Camp recovery'}</button></div>`).join('')}<div class="button-row section-gap"><button data-action="team">Manage team</button><button data-action="pack">Craft & prepare</button><button data-action="camp-leave" class="primary">Continue expedition</button></div><p class="fine">Your entire collection recovers on arrival. Change your lineup, use discounted forging, or trade fruit for essence before continuing.</p></section>${journal()}`;}
function pack(){
  const r=state.run,p=state.collection[selectedId]||active();
  const supplies=Object.entries(C.ITEMS).map(([id,item])=>`<div class="item-card"><span class="item-art">${pickupIcon(id)}</span><div><h3>${esc(item.name)} <b>×${r.items[id]??0}</b></h3><p>${esc(item.desc)}</p></div><button data-action="item" data-id="${id}" ${!(r.items[id]>0)||r.phase==='dead'||['befriend','replacement'].includes(r.phase)?'disabled':''}>${['trailRibbon','treasureScent','scoutFeather','campfireSeed'].includes(id)?'Activate':'Use'}</button></div>`).join('');
  const tabs=`<div class="pack-tabs" role="tablist" aria-label="Pack categories">${[['equipment','Equipment'],['consumables','Consumables'],['materials','Materials']].map(([id,label])=>`<button type="button" role="tab" data-action="pack-section" data-id="${id}" aria-selected="${packSection===id}" aria-controls="pack-content">${label}</button>`).join('')}</div>`;
  const materials=Forge.forgeInventory(state).filter(row=>row.owned>0&&(row.kind!=='item'||row.id==='item:charmMaterial'));
  const inventory=packSection==='equipment'?`${InventoryUI.renderInventory(state,inventoryPrefs,{portrait})}<details class="panel pack-equipment-current"><summary>Current loadout · ${esc(name(p))}</summary><label for="gear-target">Equip this individual</label><select id="gear-target">${collection().map(x=>`<option value="${x.id}" ${x.id===p.id?'selected':''}>${esc(name(x))}</option>`).join('')}</select>${ForgeUI.renderEquipmentSlots(state,p)}</details>`:packSection==='consumables'?`<section class="panel"><h2>Expedition consumables</h2><label for="supply-target">Use supplies on</label><select id="supply-target">${team().map(x=>`<option value="${x.id}" ${x.id===p.id?'selected':''}>${esc(name(x))} · ${Math.round(x.hp)} HP</option>`).join('')}</select>${supplies}<p class="fine">Temporary supplies reset after full-team defeat. Your permanent collection and equipment stay saved.</p></section>`:`<section class="panel"><h2>Crafting materials</h2><p class="muted">Gather ingredients along the trail and mix them in the forge below.</p><div class="pack-material-grid">${materials.map(row=>`<article><h3>${esc(row.name)} ×${row.owned}</h3><p>${esc(row.qualityLabel||row.rarity||'Common')} · ${row.value} forge value each</p></article>`).join('')||'<p>Your next discovery will bring more crafting materials.</p>'}</div>${resources()}</section>`;
  return `${tabs}<div id="pack-content" role="tabpanel">${inventory}</div><div class="pack-crafting two-col" id="pack-forge"><div class="stack"><div data-forge-result>${ForgeUI.renderForgeResult(state,p,{unsaved:forgeUnsaved})}</div>${ForgeUI.renderForge(state,forgeMix,{busy:forgeBusy,unsaved:forgeUnsaved})}</div><div class="stack">${essenceMarket()}<section class="panel"><h2>Supplies & exchange</h2><p class="muted">Fruit: ${r.fruit} · Charm materials: ${r.items.charmMaterial||0}.</p><div class="button-row"><button data-action="craft" data-id="charm">Bond Charm · 2 fruit + 1 material</button><button data-action="craft" data-id="revive">Revival Seed · 8 fruit + 4 materials</button></div><details class="pack-supplies"><summary>Exchange spare essence · 4 → 1</summary><div class="exchange-grid"><label>Give essence<select id="exchange-from">${Object.entries(C.ELEMENTS).map(([id,e])=>`<option value="${id}">${e.name} (${r.essence[id]??0})</option>`).join('')}</select></label><span>→</span><label>Receive essence<select id="exchange-to">${Object.entries(C.ELEMENTS).map(([id,e])=>`<option value="${id}" ${id==='earth'?'selected':''}>${e.name}</option>`).join('')}</select></label></div><button data-action="exchange" class="wide" ${canPrepare()?'':'disabled'}>Exchange essence</button></details><details class="pack-supplies"><summary>Original fragment recipes</summary><p class="fine">Use saved fragments for these original recipes or mix them in the forge.</p>${['fang','shell','heart','wind','satchel'].map(id=>`<button class="craft-gear" data-action="craft" data-id="${id}">Craft ${esc(C.GEAR[id].name)} <span>${state.meta.fragments?.[id]||0} / ${C.CONFIG.fragmentCost} fragments</span></button>`).join('')}</details></section></div></div>`;
}
function essenceMarket(){
  const offers=CampExchange.exchangeOffers(state);
  return `<section class="panel essence-market"><span class="eyebrow">ESSENCE TRADING</span><h2>Trade spare supplies.</h2><p class="muted">Choose any essence, including scarce Earth, Ice, Light and Shadow. Fruit trades cost less at camp.</p><label for="essence-offer">What to trade<select id="essence-offer">${offers.map(row=>`<option value="${esc(row.id)}">${esc(row.name)}</option>`).join('')}</select></label><div class="forge-filter-row"><label for="essence-element">Receive<select id="essence-element">${['earth','ice','light','shadow','leaf','fire','water','storm'].map(id=>`<option value="${id}">${C.ELEMENTS[id].name} essence</option>`).join('')}</select></label><label for="essence-quantity">Number of trades<input type="number" id="essence-quantity" value="1" min="1" max="25" step="1" inputmode="numeric"></label></div><div data-essence-trade-preview aria-live="polite"></div><button type="button" data-action="essence-buy" class="wide">Trade for essence</button></section>`;
}
function refreshEssenceTrade(){
  const panel=document.querySelector('.essence-market');if(!state||!panel)return;
  const offer=CampExchange.exchangeOffers(state).find(row=>row.id===$('essence-offer').value),quantity=Number($('essence-quantity').value),element=$('essence-element').value,valid=Number.isInteger(quantity)&&quantity>=1&&quantity<=25;
  const costs=offer?.costSummary||[],enough=valid&&costs.every(row=>row.owned>=row.count*quantity),allowed=['trail','camp'].includes(state.run.phase),amount=(offer?.amount||0)*(valid?quantity:1);
  const costText=costs.map(row=>`${row.count*(valid?quantity:1)} ${esc(row.name)} (${row.owned} owned)`).join(' + ');
  panel.querySelector('[data-essence-trade-preview]').innerHTML=`<p><strong>Receive ${amount} ${C.ELEMENTS[element]?.name||''} essence</strong><br>Pay ${costText}</p><p class="fine">${!allowed?'Finish the encounter to trade.':!valid?'Choose 1–25 whole trades.':!enough?'Gather enough supplies for this trade.':state.run.phase==='camp'&&offer?.id==='fruit'?'Camp discount applied to fruit cost.':'Trade only what you can spare for upgrades and recovery.'}</p>`;
  panel.querySelector('[data-action="essence-buy"]').disabled=!allowed||!enough||!offer;
}
function refreshForge(){
  const panel=document.querySelector('[data-forge-panel]');if(!state||!panel)return;
  panel.querySelector('[data-forge-preview]').innerHTML=ForgeUI.renderForgePreview(state,forgeMix);
  const resource=panel.querySelector('#forge-resource'),choice=resource.value;
  const markup=ForgeUI.renderIngredientOptions(state,forgeMix);if(resource.dataset.optionsMarkup!==markup){resource.innerHTML=markup;resource.dataset.optionsMarkup=markup;if([...resource.options].some(o=>o.value===choice))resource.value=choice;}
  const inventory=new Map(Forge.forgeInventory(state).map(row=>[row.id,row]));
  panel.querySelectorAll('[data-forge-available]').forEach(el=>{const row=inventory.get(el.dataset.forgeAvailable);el.textContent=`${row?.owned||0} owned · ${row?.qualityLabel||'Common'} · ${row?.value||0} value each${row?.element?' · '+C.ELEMENTS[row.element].name:''}`;});
  const preview=Forge.previewForge(state,forgeMix),button=panel.querySelector('[data-action="forge-roll"]');
  button.disabled=forgeBusy||(!forgeUnsaved&&!preview.canCraft);button.textContent=forgeBusy?'Saving crafted item…':forgeUnsaved?'Retry saving crafted item':'Craft 1 random item · spend mix';
  panel.querySelector('[data-action="forge-clear"]').disabled=!Object.keys(forgeMix).length||forgeBusy;
  panel.querySelectorAll('[data-action="forge-add"],[data-action="forge-remove"],[data-forge-quantity]').forEach(el=>el.disabled=forgeBusy||forgeUnsaved);
}
function updateForgeMix(){const panel=document.querySelector('[data-forge-panel]');if(!panel)return;panel.querySelector('[data-forge-mix]').innerHTML=ForgeUI.renderMix(state,forgeMix);refreshForge();}
function showGearPicker(){if(gearPicker)modal(ForgeUI.renderGearPicker(state,gearPicker),'Choose equipment','forge-dialog');}
function showForgeOdds(){modal(ForgeUI.renderForgeOdds(state,forgeMix,forgeOdds),'Crafting item probabilities','forge-dialog forge-odds-dialog');}
function saveInventoryPreferences(){try{localStorage.setItem(inventoryPreferenceKey,JSON.stringify(inventoryPrefs));}catch{}}
function refreshInventory(){
  const grid=document.querySelector('[data-inventory-grid]');if(!grid||!state)return;
  const rows=InventoryUI.inventoryGroups(state,inventoryPrefs),keys=new Set(rows.map(row=>row.key)),existing=new Map([...grid.children].filter(el=>el.dataset.inventoryKey).map(el=>[el.dataset.inventoryKey,el]));
  for(const child of [...grid.children])if(!keys.has(child.dataset.inventoryKey))child.remove();
  rows.forEach((row,index)=>{const old=existing.get(row.key);let node=old;if(!old||old.dataset.inventoryState!==InventoryUI.inventoryTileFingerprint(row)){const holder=document.createElement('template');holder.innerHTML=InventoryUI.renderInventoryTile(state,row,{portrait});node=holder.content.firstElementChild;if(old?.isConnected)old.replaceWith(node);}if(grid.children[index]!==node)grid.insertBefore(node,grid.children[index]||null);});
  if(!rows.length)grid.innerHTML=InventoryUI.renderInventoryGrid(state,inventoryPrefs,{portrait});
  const count=document.querySelector('[data-inventory-count]');if(count)count.textContent=`${rows.length} equipment groups · select an item to inspect and equip`;
  const summary=document.querySelector('.inventory-filters>summary');if(summary){const filters=inventoryPrefs.rarities.length+inventoryPrefs.affinities.length+inventoryPrefs.purposes.length+(inventoryPrefs.status!=='all')+(inventoryPrefs.collection!=='owned');summary.textContent=`Filters${filters?' · '+filters:''}`;}
  hydrateHostedPortraits(grid,{reducedMotion:true});
}
function showEquipmentItem({preserveScroll=true}={}){
  if(!equipmentSelection)return;const dialog=$('inspect-dialog'),scroll=preserveScroll&&dialog.classList.contains('inventory-detail-dialog')?dialog.scrollTop:0;
  modal(InventoryUI.renderItemDetails(state,equipmentSelection,{portrait,eligibleElements:eligibleCompassElements(state,equipmentSelection.itemId)}),'Equipment details','inventory-detail-dialog');
  const detail=$('dialog-content').querySelector('[data-equipment-detail]');if(detail){equipmentSelection.copyId=detail.dataset.copyId;equipmentSelection.monkeyId=detail.dataset.monkeyId;equipmentSelection.slot=detail.dataset.slot;}
  dialog.scrollTop=scroll;
}
function openEquipmentItem(itemId,{tier=0,copyId=null,monkeyId=null,slot=null,fromOdds=false}={}){
  if(!C.GEAR[itemId])return;equipmentScroll=window.scrollY;equipmentSelection={itemId,tier,copyId,monkeyId:monkeyId||selectedId,slot,fromOdds};showEquipmentItem({preserveScroll:false});
  if(equipmentSelection.copyId){EquipmentInventory.setEquipmentCopyFlag(state,equipmentSelection.copyId,'isNew',false);refreshInventory();void checkpoint(true);}
}
async function rollForge(){
  if(forgeBusy)return;forgeBusy=true;refreshForge();
  try{
    if(!forgeUnsaved){Forge.craftForge(state,forgeMix,Forge.nextForgeOperationId(state));forgeMix={};forgeUnsaved=true;}
    // Keep this result and block further rolls until its device checkpoint succeeds.
    vault.localOK=false;await checkpoint(true);
    if(vault.localOK){forgeUnsaved=false;toast('Equipment crafted and saved.');}
    else toast('Crafted item is held here. Retry saving before crafting again.');
  }finally{forgeBusy=false;render();}
}

function recordRows(records){return records?.length?records.map((h,i)=>`<div class="record"><span class="tag">${i+1}</span><div><strong>${esc(h.name)}</strong><small>${esc(h.form)} · ${Number(h.feet).toLocaleString()} ft · ${Number(h.wins)} wins${h.mode==='practice'?' · PRACTICE':''}</small></div><b>${Number(h.score).toLocaleString()}</b></div>`).join(''):'<p class="muted">Finish an expedition to record its story here.</p>';}
function records(){return `<h2>Your trail stories</h2><div class="stat-grid"><span><b>${state.meta.best.toLocaleString()}</b>Best score</span><span><b>${state.meta.runs}</b>Expeditions</span><span><b>${state.meta.legacy}</b>Legacy</span><span><b>${collection().length}</b>Companions</span></div>${recordRows(state.meta.history)}<h3 class="section-gap">Wild Companions leaderboard</h3><p class="fine">GPS expeditions only. Casual client-reported scores; no coordinates are published.</p><button class="wide" data-action="scores" ${state.mode==='practice'?'disabled':''}>Refresh leaderboard</button>${scores?recordRows(scores):''}`;}
function settings(){return `<h2>Make the trail yours</h2><p class="muted">Effects are optional. Tracking runs only in the foreground.</p><label for="difficulty-setting">Expedition difficulty<select id="difficulty-setting" ${state.run.distance>0&&state.run.phase!=='dead'?'disabled':''}>${[['relaxed','Relaxed'],['standard','Standard'],['challenging','Challenging']].map(([id,label])=>`<option value="${id}" ${state.difficulty===id?'selected':''}>${label}</option>`).join('')}</select><small>Choose before walking or after an expedition ends. Enemy depth stays tied to distance.</small></label>${[['reducedMotion','Reduced motion','Still portraits and simpler transitions.'],['battery','Battery saving','Limit ambient animations and effects.'],['sound','Sound effects','Quiet discovery and battle notes.'],['vibration','Vibration','A short tap when supplies arrive.'],['keepAwake','Keep screen awake','Optional while tracking; your browser may release it.']].map(([id,label,desc])=>`<label class="setting-row"><span><strong>${label}</strong><small>${desc}</small></span><input type="checkbox" data-pref="${id}" ${prefs[id]?'checked':''}></label>`).join('')}<div class="button-row section-gap"><button data-action="export">Export encrypted backup</button><button data-action="sync">Sync now</button></div><button class="text-button wide" data-action="activity-open">Trail activity & saved moments</button><button class="text-button wide" data-action="records">Trail stories & leaderboard</button>`;}
function death(){const h=state.meta.history[0];return `<section class="panel run-end"><span class="eyebrow">EVERY COMPANION COMES HOME</span><div class="end-art">${team().map(p=>portrait(p,{state:'knockout'})).join('')}</div><h1>A brave expedition.</h1><p>${feet(state.run.distance)} feet · ${state.run.wins} victories · ${h?.score?.toLocaleString()||0} points</p><div class="notice">Your ${collection().length} companions, equipment, bonds and evolution discoveries remain safe. The next expedition earns levels and active forms anew.</div><h3>Choose up to three for your next trail</h3><div class="restart-party">${collection().map((p,i)=>`<label><input type="checkbox" name="restart-member" value="${p.id}" ${i<3?'checked':''}>${portrait(p)}<span>${esc(name(p))}</span></label>`).join('')}</div><button class="primary wide" data-action="restart">Begin the next expedition</button><p class="fine">Temporary supplies and expedition injuries reset on a new expedition.</p></section>${journal()}`;}
function refreshCraftButtons(){if(!state)return;document.querySelectorAll('#view [data-action="craft"]').forEach(b=>{const recipe=b.dataset.id==='charm'?{fruit:2,material:1}:b.dataset.id==='revive'?{fruit:8,material:4}:null;b.disabled=!['trail','camp'].includes(state.run.phase)||(recipe?state.run.fruit<recipe.fruit||(state.run.items.charmMaterial||0)<recipe.material:(state.meta.fragments[b.dataset.id]||0)<C.CONFIG.fragmentCost||state.meta.gear[b.dataset.id]>=12);});}
function renderGPS(){refreshUpgradeIndicators();refreshForge();refreshEssenceTrade();refreshCraftButtons();if(state){refreshRegionalProgress(state);const goals=document.querySelector('[data-walk-goals]');if(goals&&tab!=='walk')goals.innerHTML=pinnedGoalUI();}refreshTrainingProgress();refreshChallengeProgress();refreshEssenceProgress();if(state)void social.heartbeat();updateLandMotion();renderLocation();if($('gps-status'))$('gps-status').textContent=state?.mode==='practice'?'':gpsInfo.reason;if($('gps-meta'))$('gps-meta').textContent=gpsInfo.mph==null?'':`${gpsInfo.mph.toFixed(1)} mph`;if($('tracking-button'))$('tracking-button').textContent=gps.active?'Pause walking':'Start walking';if($('walking-state'))$('walking-state').textContent=state?.mode==='practice'?'Practice':gps.active?'Walking':'Paused';if($('gps-quality'))$('gps-quality').textContent=state?.mode==='practice'?'Unranked trail':gpsInfo.accuracy==null?'GPS waiting':`GPS ±${Math.round(gpsInfo.accuracy)} m`;}
function render(){
  const oldMap=$('view').querySelector('.terrain-map'),camera=oldMap?{id:oldMap.dataset.land,east:Number(oldMap.dataset.mapEast),north:Number(oldMap.dataset.mapNorth),scale:Number(oldMap.dataset.mapScale)}:null;
  if(!state)return;observeMilestones();menuProgressPending=false;document.body.classList.add('in-game');document.body.classList.toggle('is-walk',tab==='walk');$('creature-label').textContent=`${state.name} · ${state.mode==='practice'?'Practice sanctuary':'Expedition '+state.meta.runs}`;document.querySelectorAll('[data-action="tab"]').forEach(b=>b.setAttribute('aria-current',b.dataset.id===tab?'page':'false'));
  $('view').innerHTML=tab==='walk'?walk():tab==='team'?teamView():tab==='sanctuary'?sanctuaryView():tab==='social'?renderSocialView(social.model):pack();
  refreshCraftButtons();
  if(state.run.phase==='battle'&&(!state.run.battle.ready||state.run.battle.itemUsedRound===state.run.battle.round))document.querySelectorAll('#view [data-action="item"]').forEach(b=>b.disabled=true);
  document.querySelectorAll('#view [data-action="item"][data-id="bondCharm"]').forEach(b=>{b.disabled=true;b.textContent='Befriend';});
  hydrateAdventureSurfaces();renderGPS();renderLocation();hydrateHostedPortraits($('view'),{reducedMotion:prefs.reducedMotion||prefs.battery});
  const map=$('view').querySelector('.terrain-map'),layer=map?.querySelector('.terrain-camera');
  if(camera&&layer&&typeof layer.animate==='function'&&camera.id===map.dataset.land&&camera.scale===Number(map.dataset.mapScale)&&!prefs.reducedMotion&&!prefs.battery&&!document.hidden&&!matchMedia('(prefers-reduced-motion: reduce)').matches){
    const x=(Number(map.dataset.mapEast)-camera.east)*camera.scale,y=(camera.north-Number(map.dataset.mapNorth))*camera.scale,distance=Math.hypot(x,y);
    if(distance>0&&distance<=75)layer.animate([{transform:`translate(${x}px,${y}px)`},{transform:'translate(0,0)'}],{duration:450,easing:'cubic-bezier(.2,.7,.3,1)'});
  }
}
function modal(content,label='Trail details',className=''){
  const dialog=$('inspect-dialog');if(!dialog.open){const el=document.activeElement;modalReturnFocus={element:el,monkeyId:el?.dataset?.monkey,slot:el?.dataset?.abilitySlot};}
  dialog.className=className;$('dialog-content').innerHTML=content;dialog.setAttribute('aria-label',label);if(!dialog.open)dialog.showModal();syncSocialOverlay();const popup=$('discovery-popup');if(popup.matches(':popover-open')){popup.hidePopover();if(className==='presentation-dialog')popup.hidden=true;else popup.showPopover();}hydrateHostedPortraits($('dialog-content'),{reducedMotion:prefs.reducedMotion||prefs.battery});
}
function closeModal(){
  const revealId=$('dialog-content').querySelector('[data-reveal-id]')?.dataset.revealId;if(revealId&&state){P.acknowledgeReveal(state,revealId);void checkpoint(true);}
  const inventoryDialog=$('inspect-dialog').classList.contains('inventory-detail-dialog'),closedItem=equipmentSelection?.itemId,landscapeDialog=$('inspect-dialog').classList.contains('lands-dialog')||$('inspect-dialog').classList.contains('compass-dialog');
  const stopped=ultraConfirm;ultraConfirm=false;$('inspect-dialog').close();syncSocialOverlay();pendingMove=null;abilityEditor=null;gearPicker=null;equipmentSelection=null;
  if(menuProgressPending)render();const focus=modalReturnFocus;modalReturnFocus=null;
  if(focus?.element?.isConnected)focus.element.focus({preventScroll:true});else if(inventoryDialog&&closedItem)document.querySelector(`[data-inventory-grid] [data-id="${CSS.escape(closedItem)}"]`)?.focus({preventScroll:true});else if(focus?.monkeyId&&focus.slot!==undefined)document.querySelector(`[data-action="ability-open"][data-monkey="${CSS.escape(focus.monkeyId)}"][data-ability-slot="${focus.slot}"]`)?.focus();
  if(inventoryDialog)window.scrollTo(0,equipmentScroll);if(landscapeDialog&&!focus?.element?.isConnected)document.querySelector('[data-action="lands"]')?.focus();if(stopped&&gps.active)pause('Ultra confirmation closed · Start walking to resume.');
}
function switchPreview(id){const p=state.collection[id],e=state.run.battle.enemy;modal(`<span class="eyebrow">${pendingMove?'RELAY TARGET':'SWITCH PREVIEW'}</span>${profile(p)}<div class="notice">${pendingMove?`${esc(C.MOVES[pendingMove].name)} will bring this teammate in.`:'Switching spends this turn. The incoming teammate may receive the enemy attack.'} Status effects remain attached.</div><h3>Incoming matchup</h3><div class="matchups">${e.types.map(t=>`<span style="color:${C.ELEMENTS[t].color}">${C.ELEMENTS[t].name} ×${C.effectiveness(t,p.types)}</span>`).join('')}</div><p class="fine">${state.run.battle.hazard?`Entry hazard: ${esc(state.run.battle.hazard.name||state.run.battle.hazard)}`:'No entry hazard is visible.'}</p><button class="primary wide" data-action="switch-confirm" data-id="${id}">${pendingMove?'Use relay ability':'Confirm switch · uses turn'}</button>`,'Switch teammate');}
function openEvolution(type,id){
  const p=state.collection[id],o=C.evolutionPreview(state,type,id);if(!o)return;
  const preview=o.monkey||o.preview||{...p,types:o.types||[type],stage:p.stage+1},level=o.level??o.requiredLevel??3,ready=canPrepare()&&state.run.party.includes(p.id)&&p.hp>0&&p.level>=level&&Object.entries(o.cost||{}).every(([t,n])=>(state.run.essence[t]||0)>=n);
  modal(`<span class="eyebrow">EVOLUTION PREVIEW · STAGE ${preview.stage}</span>${profile(preview)}${evolutionGains(p,o)}<h3>Essence needed</h3>${evolutionEssence(o)}<p class="fine">Requires level ${level} · your companion is level ${p.level}. ${preview.stage===1?'First evolution needs only 6 of each required essence.':''}</p><h3>${esc(o.specialization||C.ELEMENTS[type].name+' path')}</h3><p>${esc(o.desc||o.passive||'A new combat specialization, silhouette and elemental techniques.')}</p><div class="matchups">${Object.keys(C.ELEMENTS).map(t=>`<span style="color:${C.ELEMENTS[t].color}">${C.ELEMENTS[t].name} ×${C.effectiveness(t,preview.types)}</span>`).join('')}</div><h3 class="section-gap">New ability decisions</h3><p>${(o.moves||o.abilities||C.unlocked(preview).filter(x=>!C.unlocked(p).includes(x))).map(x=>esc(C.MOVES[typeof x==='string'?x:x.id]?.name||x)).join(' · ')||'Deepen this role and review your loadout.'}</p><button class="wide" data-action="evolution-track" data-id="${type}" data-monkey="${id}">Track this evolution requirement</button><button class="primary wide" data-action="evolve" data-id="${type}" data-monkey="${id}" ${ready?'':'disabled'}>${ready?'Commit to this evolution':'Gather essence and reach the required level'}</button>`,'Evolution preview');
}
document.addEventListener('input',e=>{if(state&&e.target.id==='equipment-search'){inventoryPrefs.search=e.target.value;saveInventoryPreferences();refreshInventory();}});
document.addEventListener('change',async e=>{
  if(!state)return;try{
    if(e.target.matches('[data-inventory-filter]')){const key=e.target.dataset.inventoryFilter,value=e.target.value;inventoryPrefs[key]=e.target.checked?[...new Set([...inventoryPrefs[key],value])]:inventoryPrefs[key].filter(id=>id!==value);saveInventoryPreferences();refreshInventory();}
    if(e.target.matches('[data-inventory-control]')){inventoryPrefs[e.target.dataset.inventoryControl]=e.target.value;saveInventoryPreferences();refreshInventory();}
    if(e.target.matches('[data-equipment-monkey]')&&equipmentSelection){equipmentSelection.monkeyId=e.target.value;equipmentSelection.slot=null;showEquipmentItem();document.querySelector('[data-equipment-monkey]')?.focus({preventScroll:true});}
    if(e.target.matches('[data-equipment-choice]')){EquipmentInventory.setEquipmentChoice(state,e.target.dataset.equipmentChoice,e.target.value);showEquipmentItem();await checkpoint(true);}
  }catch(error){toast(error.message);}
});
document.addEventListener('input',e=>{if(state&&e.target.id==='essence-quantity'){refreshEssenceTrade();return;}if(state&&e.target.matches('[data-forge-quantity]')&&!forgeBusy&&!forgeUnsaved){forgeMix[e.target.dataset.forgeQuantity]=e.target.value===''?NaN:Number(e.target.value);refreshForge();return;}if(e.target.id==='collection-search'&&state){const cursor=e.target.selectionStart;collectionSearch=e.target.value;collectionPage=0;render();const field=$('collection-search');field.focus();if(cursor!==null)field.setSelectionRange(cursor,cursor);}});
document.addEventListener('change',e=>{try{if(state&&['essence-offer','essence-element','essence-quantity'].includes(e.target.id)){refreshEssenceTrade();return;}if(state&&e.target.matches('[data-gear-filter]')&&gearPicker){const key=e.target.dataset.gearFilter;gearPicker[key]=e.target.value;gearPicker.page=0;showGearPicker();document.querySelector(`[data-gear-filter="${key}"]`)?.focus();return;}if(state&&e.target.matches('[data-odds-filter]')){const key=e.target.dataset.oddsFilter;forgeOdds[key]=e.target.value;forgeOdds.page=0;showForgeOdds();document.querySelector(`[data-odds-filter="${key}"]`)?.focus();return;}if(e.target.id==='companion-action-individual'&&state){openCompanionActions(e.target.value);$('companion-action-individual')?.focus();return;}if(e.target.matches('[data-collection-control]')&&state){const key=e.target.dataset.collectionControl;if(key==='element')collectionElement=e.target.value;if(key==='rarity')collectionRarity=e.target.value;if(key==='sort')collectionSort=e.target.value;collectionPage=0;render();}if(e.target.matches('[data-pref]')){prefs[e.target.dataset.pref]=e.target.checked;localStorage.setItem(preferenceKey,JSON.stringify(prefs));applyPreferences();if((prefs.battery||!prefs.keepAwake)&&gps.lock){gps.lock.release().catch(()=>{});gps.lock=null;}}if(e.target.id==='difficulty-setting'&&Object.keys(C.CONFIG.difficulty).includes(e.target.value)&&(state.run.distance===0||state.run.phase==='dead')){state.difficulty=e.target.value;void checkpoint(true);}if(e.target.id==='gear-target'){selectedId=e.target.value;render();}if(e.target.id==='supply-target')selectedId=e.target.value;if(e.target.name==='restart-member'&&document.querySelectorAll('[name="restart-member"]:checked').length>3){e.target.checked=false;toast('Choose at most three companions.');}}catch(error){toast(error.message);render();}});
document.addEventListener('focusin',e=>{const option=e.target.closest('.ability-option');if(option&&abilityEditor&&abilityEditor.slot!==0&&option.dataset.compareId!==abilityEditor.candidateId)previewAbility(option.dataset.compareId);});
document.addEventListener('pointerover',e=>{if(e.pointerType==='touch')return;const option=e.target.closest('.ability-option');if(option&&abilityEditor&&abilityEditor.slot!==0&&option.dataset.compareId!==abilityEditor.candidateId)previewAbility(option.dataset.compareId);});
document.addEventListener('click',async e=>{
  const b=e.target.closest('[data-action]');if(!b||b.disabled)return;const a=b.dataset.action,id=b.dataset.id;if(a==='close-dialog'){closeModal();return;}if(a==='enable-location'){locationAccess.request();return;}if(a==='welcome-new'||a==='welcome-continue'){setWelcomeMode(a==='welcome-new'?'new':'continue');return;}if(!state)return;
  try{
    if(social.locked&&!(a==='tab'&&id==='social')){toast('Return from your friendly battle before changing your expedition.');return;}
    if(combatBusy&&['move','switch-menu','switch-confirm','battle-heal','battle-item','ready','replace'].includes(a))return;
    if(a==='pack-section'){if(['equipment','consumables','materials'].includes(id)){packSection=id;render();}return;}
    if(a==='inventory-reset'){inventoryPrefs=InventoryUI.inventoryPreferences();saveInventoryPreferences();if(tab==='pack')render();return;}
    if(a==='inventory-item'){openEquipmentItem(id,{tier:Number(b.dataset.tier)||0,copyId:b.dataset.copy||null,monkeyId:b.dataset.monkey,slot:b.dataset.slot,fromOdds:b.dataset.fromOdds==='true'});if(b.dataset.equipStart==='true')document.querySelector('.equipment-equip-controls')?.scrollIntoView({block:'start'});return;}
    if(a==='inventory-back-odds'){equipmentSelection=null;showForgeOdds();return;}
    if(a==='inventory-copy'){if(equipmentSelection){equipmentSelection.copyId=id;equipmentSelection.slot=null;showEquipmentItem();}return;}
    if(a==='inventory-monkey'){if(equipmentSelection){equipmentSelection.monkeyId=id;equipmentSelection.slot=null;showEquipmentItem();}return;}
    if(a==='inventory-slot'){if(equipmentSelection){equipmentSelection.slot=id;showEquipmentItem();}return;}
    if(['inventory-equip','inventory-unequip','inventory-upgrade','inventory-flag'].includes(a)){
      if(!equipmentSelection)return;const copyId=equipmentSelection.copyId;
      if(a==='inventory-equip'){C.equip(state,id,b.dataset.monkey,b.dataset.slot);toast('Equipment saved. Power and loadout updated.');}
      if(a==='inventory-unequip'){C.unequip(state,id,b.dataset.monkey);toast('Equipment removed.');}
      if(a==='inventory-upgrade'){const upgraded=EquipmentInventory.upgradeEquipmentCopy(state,id);equipmentSelection.tier=upgraded.tier;toast(`Equipment upgraded to rank ${upgraded.tier}.`);}
      if(a==='inventory-flag'){const copy=EquipmentInventory.equipmentCopy(state,copyId);EquipmentInventory.setEquipmentCopyFlag(state,copyId,id,!copy[id]);}
      refreshInventory();showEquipmentItem();const current=document.querySelector('.pack-equipment-current .equipment-slots'),target=state.collection[selectedId]||active();if(current)current.outerHTML=ForgeUI.renderEquipmentSlots(state,target).replace(/<p class="fine">[\s\S]*$/,'');
      const result=document.querySelector('[data-forge-result]');if(result)result.innerHTML=ForgeUI.renderForgeResult(state,target,{unsaved:forgeUnsaved});await checkpoint(true);return;
    }
    if(a==='forge-add'){
      if(forgeBusy||forgeUnsaved)return;const resource=$('forge-resource').value,amount=Number($('forge-amount').value),row=Forge.forgeInventory(state).find(row=>row.id===resource);
      if(!row||!Number.isSafeInteger(amount)||amount<1)throw Error('Choose an ingredient and a whole amount of at least 1.');
      const quantity=(forgeMix[resource]||0)+amount;if(quantity>row.owned)throw Error(`Only ${row.owned} ${row.name.toLowerCase()} available.`);
      const total=Object.values(forgeMix).reduce((n,count)=>n+count,0)+amount;if(!Number.isFinite(total)||total>60)throw Error('Use at most 60 ingredients per craft.');
      forgeMix[resource]=quantity;updateForgeMix();return;
    }
    if(a==='forge-remove'){if(forgeBusy||forgeUnsaved)return;delete forgeMix[id];updateForgeMix();return;}
    if(a==='forge-clear'){if(forgeBusy||forgeUnsaved)return;forgeMix={};updateForgeMix();return;}
    if(a==='forge-roll'){await rollForge();return;}
    if(a==='forge-odds'){forgeOdds={rarity:'all',element:'all',page:0};showForgeOdds();return;}
    if(a==='forge-odds-page'){forgeOdds.page=Math.max(0,forgeOdds.page+Number(id));showForgeOdds();return;}
    if(a==='gear-picker'){gearPicker={monkeyId:b.dataset.monkey,slot:id,element:'all',rarity:'all',page:0};showGearPicker();return;}
    if(a==='gear-page'){if(gearPicker){gearPicker.page=Math.max(0,gearPicker.page+Number(id));showGearPicker();}return;}
    if(a==='hunt-info'){modal(speciesHuntUI(state,id),'Find this monkey');return;}
    if(a==='hunt-track'){Regions.trackHunt(state,id);modal(speciesHuntUI(state,id),'Monkey hunt');render();await checkpoint(true);return;}
    if(a==='hunt-begin'){pause('Targeted encounter · tracking paused.');C.beginRegionalHunt(state);closeModal();tab='walk';render();await checkpoint(true);return;}
    if(a==='adventure-open'){modal(regionAdventureUI(state,id),'Regional adventure');return;}
    if(a==='adventure-track'){Regions.trackAdventure(state,id);const row=Regions.REGION_ADVENTURES.flatMap(a=>a.steps).find(step=>step.id===id);modal(regionAdventureUI(state,row?.landId),'Regional adventure');render();await checkpoint(true);return;}
    if(a==='adventure-claim'){const landId=Regions.REGION_ADVENTURES.flatMap(a=>a.steps).find(step=>step.id===id)?.landId;const reward=Regions.claimAdventure(state,id);P.recordActivity(state,{id:`adventure:${id}`,kind:'reward',title:'Regional adventure reward claimed',detail:challengeRewardText(reward)||'Regional keepsake earned',important:true});modal(regionAdventureUI(state,landId),'Regional adventure');render();await checkpoint(true);return;}
    if(a==='guardian-begin'){pause('Regional guardian · tracking paused.');C.beginRegionalGuardian(state,id);closeModal();tab='walk';render();await checkpoint(true);return;}
    if(a==='friendship-spend'){Friends.spendBondMaterial(state,id);render();await checkpoint(true);toast('1 species material spent · +4 permanent Bond.');return;}
    if(a==='companion-favorite'){Friends.setFavorite(state,id);render();await checkpoint(true);return;}
    if(a==='lineup-save'){Friends.saveLineup(state,$('lineup-name').value);render();await checkpoint(true);return;}
    if(a==='lineup-apply'){Friends.applyLineup(state,id,C.setParty);render();await checkpoint(true);return;}
    if(a==='lineup-remove'){Friends.removeLineup(state,id);render();await checkpoint(true);return;}
    if(a==='evolution-track'){Friends.trackEvolution(state,b.dataset.monkey,id,C.evolutionCost);await checkpoint(true);closeModal();render();toast('Evolution requirement pinned on your trail.');return;}
    if(a==='evolution-goal-open'){selectedId=id;companionDetailOpen=true;companionSection='evolve';tab='team';render();return;}
    if(a==='duplicate-preview'){const offer=Friends.duplicateConversionPreview(state,id);modal(`<h2>Duplicate conversion</h2><p>${esc(offer.explanation)}</p>${offer.allowed?'':`<p class="notice">${esc(offer.reason)}</p>`}<p class="fine">This removes this new individual permanently. Existing companions and material balances remain safe.</p><button class="primary wide" data-action="convert-duplicate" data-id="${esc(id)}" ${offer.allowed?'':'disabled'}>Confirm conversion · +3 species materials</button><button class="wide" data-action="close-dialog">Keep this companion</button>`,'Review duplicate conversion');return;}
    if(a==='compass'){modal(`<span class="eyebrow">YOUR POCKET COMPASS</span><h2>Find your bearings</h2>${radarView()}<p class="fine">North stays up on the terrain map. The gold marker tracks an Ultra sighting when one is active.</p>`,'Encounter compass','compass-dialog');return;}
    if(a==='map-zoom'){mapZoom=C.clamp(Math.round((mapZoom+(id==='in'?.25:-.25))*100)/100,.75,1.5);render();document.querySelector(`[data-action="map-zoom"][data-id="${id}"]`)?.focus();return;}
    if(a==='lands'||a==='lands-preview'){openLands(a==='lands-preview'?id:null);return;}
    if(a==='challenge-open'){openLands(id);return;}
    if(a==='challenge-track'||a==='challenge-claim'){
      if(a==='challenge-track'){state.meta.pinnedGoal=null;const current=trackedChallenge(state);trackChallenge(state,current?.id===id?null:id);toast(current?.id===id?'Challenge untracked.':'Challenge tracked on the trail.');}
      else{const reward=claimChallenge(state,id);P.recordActivity(state,{id:`regional-challenge:${id}`,kind:'reward',title:'Regional challenge reward claimed',detail:challengeRewardText(reward),important:true});toast(`Challenge reward claimed: ${challengeRewardText(reward)}.`);}
      refreshChallengeProgress();
      await checkpoint(true);const scroll=$('inspect-dialog').scrollTop;render();refreshChallengeProgress();if($('inspect-dialog').open)$('inspect-dialog').scrollTop=scroll;return;
    }
    if(a==='land-select'){selectLand(state,id);closeModal();render();document.querySelector('[data-action="lands"]')?.focus();await checkpoint(true);feedback();return;}
    if(a==='ability-open'){openAbilities(b.dataset.monkey,Number(b.dataset.abilitySlot));return;}
    if(a==='ability-compare'){previewAbility(id);return;}
    if(a==='ability-equip'){if(!abilityEditor)return;const {monkeyId,slot}=abilityEditor,p=state.collection[monkeyId];if(!p||!canPrepare()||slot===0)return;C.setMove(state,slot,id,monkeyId);render();closeModal();feedback();toast(`${C.MOVES[id].name} equipped in slot ${slot+1}. Saving automatically.`);await checkpoint(true);return;}
    if(a==='collection-reset'){collectionSearch='';collectionElement='all';collectionRarity='all';collectionSort='number';collectionPage=0;render();return;}
    if(a==='collection-detail-close'){selectedSpecies=null;render();return;}
    if(a==='collection-individual'){openCompanionActions(id);return;}
    if(a==='tab'||['team','pack'].includes(a)){const next=a==='tab'?id:a;if(tab==='sanctuary')sanctuaryScroll=window.scrollY;tab=next;closeModal();render();window.scrollTo(0,next==='sanctuary'?sanctuaryScroll:0);await checkpoint(true);return;}
    if(a==='gps-setup'){clearNotifications();await social.stop();pause();await checkpoint(true);locationAccess.cancel();closeModal();state=null;vault=null;document.body.classList.remove('in-game','is-walk');$('game').hidden=true;$('welcome').hidden=false;$('mode-label').textContent='WALK · COLLECT · ADVENTURE';$('creature-label').textContent='WILD COMPANIONS';renderLocation();void locationAccess.refresh();window.scrollTo(0,0);return;}
    if(a==='battle-lead'){C.selectBattleLead(state,id);render();const picker=$('dialog-content').querySelector('.battle-lead-picker');if(picker)picker.outerHTML=battleLeadChoices();const chosen=document.querySelector(`#${$('inspect-dialog').open?'dialog-content':'view'} [data-action="battle-lead"][data-id="${CSS.escape(id)}"]`);chosen?.focus({preventScroll:true});await checkpoint(true);return;}
    if(a==='pending-begin'){pause('Battle · tracking paused.');C.beginPendingEncounter(state);closeModal();tab='walk';render();await checkpoint(true);return;}
    if(a==='pending-pass'){C.passPendingEncounter(state);render();await checkpoint(true);toast('Encounter passed. Your next rival is scheduled from this point.');return;}
    if(a==='finish-walk'){pause('Walk finished · your expedition is saved.');const recap=C.finishWalkSession(state);await checkpoint(true);modal(walkRecap(recap),'Walk recap');const goal=$('dialog-content').querySelector('[data-recap-goal]');if(goal)goal.innerHTML=`<h3>Next session’s goal</h3>${pinnedGoalUI()}`;return;}
    if(a==='tracking'){if(gps.active){pause();await checkpoint(true);}else if(state.mode==='gps'&&state.run.phase==='trail'&&tab==='walk'){locationAccess.cancel();C.beginWalkSession(state);try{await gps.start({wakeLock:prefs.keepAwake&&!prefs.battery});}catch(error){locationAccess.reportError(error);gpsInfo={reason:error.message,mph:null,accuracy:null};}}renderGPS();return;}
    if(a==='sync'){await checkpoint(true);void vault.sync();toast('Encrypted checkpoint queued. See save status below.');return;}
    if(a==='export'){await checkpoint(false);const data=await vault.export(),url=URL.createObjectURL(new Blob([data],{type:'application/json'})),link=document.createElement('a');link.href=url;link.download=`monkey-trails-${state.name.replace(/[^A-Za-z0-9_-]/g,'-')}.json`;link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);return;}
    if(a==='cloud'){pause();if(!confirm('Load the cloud checkpoint? An encrypted recovery copy of device progress will be kept.'))return;clearNotifications();state=await vault.loadCloud();await radar.bind(vault);void social.start();if(state.run.battle)state.run.battle.ready=false;render();return;}
    if(a==='settings'){modal(settings(),'Settings');return;}
    if(a==='records'){modal(records(),'Trail stories');return;}
    if(a==='scores'){b.disabled=true;b.textContent='Reading scores…';scores=await vault.leaderboard();modal(records(),'Trail stories');return;}
    if(a==='activity-open'){modal(P.renderActivity(state),'Trail activity');return;}
    if(a==='activity-read'){P.markActivityRead(state);modal(P.renderActivity(state),'Trail activity');await checkpoint(true);return;}
    if(a==='reveal-replay'){showSavedReveal(P.normalizePresentation(state).reveals.find(row=>row.id===id));return;}
    if(a==='reveal-done'){P.acknowledgeReveal(state,id);closeModal();await checkpoint(true);return;}
    if(a==='tutorial-dismiss'){P.dismissTutorial(state,id);render();await checkpoint(true);return;}
    if(a==='companion-section'){companionSection=id;render();await checkpoint(true);return;}
    if(a==='select-monkey'||a==='monkey-actions'||a==='inspect'){openCompanionActions(id);return;}
        if(a==='inspect-team'||a==='companion-details'){if(tab==='sanctuary')sanctuaryScroll=window.scrollY;detailReturn={tab,scroll:window.scrollY};selectedId=id;companionDetailOpen=true;tab='team';closeModal();render();document.querySelector('.companion-detail-view')?.scrollIntoView({block:'start'});return;}
    if(a==='companion-details-back'){companionDetailOpen=false;tab=detailReturn?.tab||'team';render();window.scrollTo(0,detailReturn?.scroll||0);detailReturn=null;return;}
    if(a==='companion-swap'){openReplacementPicker(id);return;}
    if(a==='companion-add'){if(state.run.party.length<3){CompanionActions.addLineupCompanion(state,id);closeModal();render();await checkpoint(true);toast('Companion added to your lineup.');}else openLineupSlotPicker(id);return;}
    if(a==='lineup-replace'){CompanionActions.replaceLineupCompanion(state,b.dataset.outgoing,id);closeModal();render();await checkpoint(true);toast('Lineup updated. Individual injuries are preserved.');return;}
    if(a==='lineup-open-sanctuary'){tab='sanctuary';render();return;}
    if(a==='collection-filter'){collectionFilter=id;collectionPage=0;render();return;}
    if(a==='collection-page'){collectionPage+=Number(id);render();return;}
    if(a==='species'){const owned=collection().find(p=>p.species===id);if(owned){openCompanionActions(owned.id);return;}selectedSpecies=id;render();$('selected-companion')?.scrollIntoView({behavior:prefs.reducedMotion?'instant':'smooth',block:'start'});return;}
    if(a==='party-menu'){const outgoing=state.run.party[Number(id)];if(outgoing){openReplacementPicker(outgoing);return;}modal(`<h2>Choose team slot ${Number(id)+1}</h2><p class="fine">Injuries stay with individuals. Knocked-out teammates require revival before replacement.</p><div class="switch-list">${collection().map(p=>`<button data-action="party-set" data-id="${p.id}" data-slot-index="${id}">${portrait(p,{state:p.hp<=0?'knockout':'idle'})}<span><strong>${esc(name(p))}</strong><small>${Math.round(p.hp)} HP · Lv ${p.level}${team().some(x=>x.id===p.id)?' · In team':''}</small></span><b>→</b></button>`).join('')}</div>`,'Team selection');return;}
    if(a==='evolution-preview'){openEvolution(id,b.dataset.monkey);return;}
    if(a==='switch-menu'){pendingMove=null;modal(`<h2>Switch teammate</h2><p class="fine">Switching spends this turn. Preview health, matchup and hazards first.</p>${switchChoices()}`,'Switch teammate');return;}
    if(a==='switch-preview'){switchPreview(id);return;}
    if(a==='move-guide'){modal(renderAbilityGuide(state,id),'Ability details');return;}
    if(a==='move-info'){modal(`<span class="eyebrow">${esc(name(active()))} · BATTLE TECHNIQUES</span><h2>Four equipped abilities</h2><div class="ability-grid">${active().loadout.map((id,slot)=>abilityCard(id,active(),{slot,battle:true})).join('')}</div><p class="fine">Power is a base value. Final damage depends on attack, defense and elemental matchups. Combat changes unlock between encounters.</p>`,'Abilities','ability-dialog');return;}
    if(a==='battle-heal'){modal(`<h2>Battle supplies</h2><p class="fine">Use one supply this round, then attack or use an ability. Choose any of your three teammates below.</p><label for="battle-supply-target">Teammate<select id="battle-supply-target">${team().map(p=>`<option value="${p.id}" ${p.id===active().id?'selected':''}>${esc(name(p))} · ${p.hp<=0?'KO':`${Math.round(p.hp)} HP`}</option>`).join('')}</select></label>${Object.entries(C.ITEMS).filter(([x])=>['berry','tonic','salve','revive'].includes(x)).map(([x,item])=>`<div class="item-card"><div><h3>${esc(item.name)} ×${state.run.items[x]||0}</h3><p>${esc(item.desc)}</p></div><button data-action="battle-item" data-id="${x}" ${!state.run.items[x]||state.run.battle.itemUsedRound===state.run.battle.round?'disabled':''}>Use</button></div>`).join('')}`,'Battle supplies');return;}
    if(a==='ultra-preview'){ultraConfirm=true;const u=state.run.ultra;modal(`<span class="eyebrow">AN ULTRA AWAITS</span><div class="species-hero">${portrait({species:u.species,types:C.SPECIES[u.species].types,stage:0},{state:'arrival'})}<h2>${esc(C.SPECIES[u.species].name)}</h2></div><p>A new Ultra joins your sanctuary after victory, with no rare capture item required.</p><div class="notice">Look up. Stop in a safe, permitted place before beginning. Expedition distance is paused while this confirmation is open.</div>${battleLeadChoices()}<button class="primary wide" data-action="ultra-begin">I have stopped safely · Begin Battle</button>`,'Begin Ultra battle');return;}
    if(a==='practice-step'||a==='map-step'){if(state.mode!=='practice'||state.run.phase!=='trail')return;const v={N:[0,1],S:[0,-1],E:[1,0],W:[-1,0]}[id];if(!v)return;const step=a==='map-step'?15:C.RESOURCE_STEP;C.advance(state,step,v[0]*step,v[1]*step,notifyDiscovery);animation='walk';clearTimeout(practiceMotionTimer);practiceMotionTimer=setTimeout(()=>{if(state?.mode==='practice'&&state.run.phase==='trail'){animation='idle';updateLandMotion();}},650);}
    else if(a==='ready')C.readyBattle(state);
    else if(a==='move'){if(combatBusy)return;const m=C.MOVES[id];if(m.switch&&team().filter(p=>p.hp>0).length>1){pendingMove=id;modal(`<h2>${esc(m.name)}</h2><p>${esc(m.desc)}</p>${switchChoices()}`,'Relay teammate');return;}C.turn(state,id);animation=m.signature?'signature':m.type!=='neutral'?'cast':m.power?'attack':'cast';combatBusy=true;feedback();setTimeout(()=>{combatBusy=false;animation='idle';render();},650);}
    else if(a==='switch-confirm'){
      const moveId=pendingMove,encounterState=state,battleId=state.run.battle?.id,actorId=active().id;closeModal();
      if(moveId){
        const m=C.MOVES[moveId];combatBusy=true;animation=m.signature?'signature':m.type!=='neutral'?'cast':m.power?'attack':'cast';render();feedback();
        try{await new Promise(resolve=>setTimeout(resolve,prefs.reducedMotion||prefs.battery?0:m.signature?950:650));if(state!==encounterState||state.run.battle?.id!==battleId||active().id!==actorId)return;C.turn(state,moveId,false,{targetId:id});}
        finally{combatBusy=false;animation='idle';}
      }else C.switchMonkey(state,id);
    }
    else if(a==='replace')C.chooseReplacement(state,id);
    else if(a==='item'||a==='battle-item'){const target=a==='battle-item'?($('battle-supply-target')?.value||active().id):($('supply-target')?.value||selectedId||active().id);if(['ribbon','scent','feather','seed','trailRibbon','treasureScent','scoutFeather','campfireSeed'].includes(id))C.activateBonus(state,id);else C.useItem(state,id,target);closeModal();}
    else if(a==='train')C.train(state,id);
    else if(a==='rename'){C.renameMonkey(state,id,$('monkey-nickname').value.trim());toast('Nickname saved.');}
    else if(a==='evolve'){const before=structuredClone(state.collection[b.dataset.monkey]);C.evolve(state,id,b.dataset.monkey);const moment=P.evolutionMoment(before,state.collection[b.dataset.monkey],{stats:p=>C.stats(p,C.equipped(state,p)),formName:C.formName,unlocked:C.unlocked});P.rememberReveal(state,moment);await checkpoint(true);closeModal();showSavedReveal(moment);}
    else if(a==='equip'){C.equip(state,id,b.dataset.monkey||$('gear-target')?.value||selectedId,b.dataset.slot||gearPicker?.slot);if(gearPicker&&$('inspect-dialog').open)showGearPicker();toast('Equipment saved.');}
    else if(a==='gear-unequip'){C.unequip(state,id,b.dataset.monkey);toast('Equipment removed.');}
    else if(a==='party-set'){const ids=team().map(p=>p.id);ids[Number(b.dataset.slotIndex)]=id;C.setParty(state,ids.filter(Boolean));closeModal();}
    else if(a==='camp-heal')C.campHeal(state,id);
    else if(a==='revive')C.useItem(state,'revive',id);
    else if(a==='camp-leave'){C.leaveCamp(state);tab='walk';pause('Camp complete · Start walking to resume.');}
    else if(a==='befriend'){const candidate=state.run.capture.candidates.find(p=>p.id===id),wasDuplicate=collection().some(p=>p.species===candidate.species),nickname=$(`capture-name-${id}`).value.trim(),result=C.befriend(state,id,{duplicate:'keep',nickname});if(result.success){const p=result.monkey,moment=P.recruitMoment(state,p,{duplicate:wasDuplicate});P.rememberReveal(state,moment);await checkpoint(true);showSavedReveal(moment);if(wasDuplicate)$('dialog-content').insertAdjacentHTML('beforeend',`<button class="wide" data-action="duplicate-preview" data-id="${p.id}">Review duplicate conversion</button>`);feedback();}else toast('They are still cautious. Bad-luck protection improves the next displayed chance.');}
    else if(a==='convert-duplicate'){C.convertDuplicate(state,id);closeModal();toast('Duplicate converted to species-specific bond materials.');}
    else if(a==='capture-skip')C.skipBefriend(state);
    else if(a==='ultra-track'){C.trackUltra(state);radar.view(state);toast('Ultra selected. Choose a safe route.');}
    else if(a==='ultra-safe'){C.safeUltraAlternative(state);toast('Same Ultra preserved. Walk the challenge on any safe route.');}
    else if(a==='ultra-begin'){C.beginUltra(state,radar.view(state)||{});pause('Ultra encounter · tracking paused.');C.readyBattle(state);closeModal();}
    else if(a==='craft'){C.craft(state,id);toast('Crafting complete.');}
    else if(a==='essence-buy'){CampExchange.buyEssence(state,{offerId:$('essence-offer').value,element:$('essence-element').value,quantity:Number($('essence-quantity').value)});toast('Essence trade complete.');}
    else if(a==='exchange'){C.exchangeEssence(state,$('exchange-from').value,$('exchange-to').value);toast('Essence exchanged.');}
    else if(a==='restart'){const ids=[...document.querySelectorAll('[name="restart-member"]:checked')].map(x=>x.value);if(!ids.length)throw Error('Choose at least one companion.');C.restart(state);C.setParty(state,ids);await social.queueReceipts();radar.clear();tab='walk';selectedId=active().id;pause();}
    else return;
    if(state.run.phase!=='trail'&&gps.active)pause('Tracking paused for the encounter.');render();if(a==='map-step')document.querySelector(`[data-action="map-step"][data-id="${id}"]`)?.focus({preventScroll:true});if(a==='restart')window.scrollTo(0,0);await checkpoint(true);
  }catch(error){toast(error.message);}
});
$('inspect-dialog').addEventListener('click',e=>{if(e.target===$('inspect-dialog'))closeModal();});
document.addEventListener('toggle',e=>{if(e.target.matches('.practice-tools'))practiceExpanded=e.target.open;},true);
$('inspect-dialog').addEventListener('cancel',e=>{e.preventDefault();closeModal();});
document.addEventListener('visibilitychange',()=>{document.body.classList.toggle('app-inactive',document.hidden);if(document.hidden){locationAccess.cancel();pause('Tracking paused while hidden. Resume manually; the gap earns no distance.');closeModal();void checkpoint(true);}else{renderGPS();void locationAccess.refresh();}});
window.addEventListener('pagehide',()=>{void social.stop();locationAccess.cancel();gps.stop();void checkpoint(true);});
window.addEventListener('focus',()=>{void locationAccess.refresh();});
window.addEventListener('online',()=>{if(vault)void vault.sync();if(state&&social.model.status==='offline')void social.start();});
window.addEventListener('offline',()=>{if(state)saveStatus({text:'Offline · encrypted device saves continue; Firestore will retry.',kind:'offline'});});
setInterval(()=>{if(vault&&state?.mode==='gps'&&vault.dirty&&!document.hidden)void vault.sync();},20000);







function socialFailure(error){social.model.error=error.message;social.emit();toast(error.message);}
document.addEventListener('submit',async e=>{
 const form=e.target.closest('[data-social-form]');if(!form||!state)return;e.preventDefault();const values=Object.fromEntries(new FormData(form)),kind=form.dataset.socialForm;
 try{await social.run(async()=>{
  if(!social.store)throw Error('Connect multiplayer first.');
  if(kind==='create-tribe'){await social.store.createTribe({name:values.name,emblem:{shape:values.shape,symbol:values.symbol,color:values.color}});await social.refreshTribeRankings();}
  else if(kind==='join-tribe'){await social.store.joinTribe(values.code);await social.refreshTribeRankings();}
  else if(kind==='update-emblem')await social.store.updateTribe({emblem:{shape:values.shape,symbol:values.symbol,color:values.color}});
  else if(kind==='deposit')await social.deposit(values.asset,Number(values.amount));
  else if(kind==='withdraw')await social.withdraw(values.asset,Number(values.amount));
  else if(kind==='gift'){await social.fund(values.asset,Number(values.amount));await social.store.gift(values.toId,values.asset,Number(values.amount));}
  else if(kind==='offer-trade'){await social.fund(values.giveAsset,Number(values.giveAmount));await social.store.offerTrade(values.toId,{asset:values.giveAsset,amount:Number(values.giveAmount)},{asset:values.wantAsset,amount:Number(values.wantAmount)});}
 });}catch(error){socialFailure(error);}
});
document.addEventListener('change',e=>{if(e.target.matches('[data-social-action="discoverability"]'))void social.run(()=>social.shareNearby(e.target.checked)).catch(socialFailure);});
document.addEventListener('click',async e=>{
 const b=e.target.closest('[data-social-action]');if(!b||b.disabled||!state)return;const a=b.dataset.socialAction,id=b.dataset.id;if(a==='discoverability')return;
 if(a==='open-social'){tab='social';closeModal();render();window.scrollTo(0,0);return;}
 try{
  if(['challenge','accept-challenge','start-raid'].includes(a)&&b.dataset.leadConfirmed!=='true'){
    social.requireFree();if(active().hp<=0){const first=team().find(p=>p.hp>0);if(first)C.selectBattleLead(state,first.id);}
    const label=a==='start-raid'?'Begin tribe boss attempt':a==='accept-challenge'?'Accept friendly challenge':'Send friendly challenge';
    modal(`<span class="eyebrow">STARTING CREW</span>${battleLeadChoices()}<p class="fine">Friendly battles use protected copies. Your expedition injuries stay saved.</p><button class="primary wide" data-social-action="${a}" data-id="${esc(id||'')}" data-lead-confirmed="true" ${team().some(p=>p.hp>0)?'':'disabled'}>${label}</button>`,'Choose your starting monkey');return;
  }
  if(b.dataset.leadConfirmed==='true')closeModal();
  await social.run(async()=>{
  if(a==='refresh-rankings')await social.refreshTribeRankings();
  else if(a==='connect'){await social.start();return;}
  if(!social.store)throw Error('Connect multiplayer first.');
  if(a==='join-adventure')await social.joinAdventure();
  else if(a==='claim-adventure')await social.claimAdventure(Number(id));
  else if(a==='leave-tribe'){await social.store.leaveTribe();await social.refreshTribeRankings();}
  else if(a==='challenge')await social.challenge(id);
  else if(a==='accept-challenge')await social.accept(id);
  else if(a==='decline-challenge')await social.store.declineChallenge(id);
  else if(a==='accept-trade'){const offer=social.model.offers.find(o=>o.id===id);if(!offer)throw Error('That trade is no longer available.');await social.fund(offer.want.asset,offer.want.amount);await social.store.acceptTrade(id);}
  else if(a==='decline-trade')await social.store.cancelTrade(id);
  else if(a==='start-raid')await social.startRaid();
  else if(a==='raid-move')await social.raidMove({type:'move',id,expectedSequence:Number(b.dataset.sequence)});
  else if(a==='raid-switch')await social.raidMove({type:'switch',index:Number(b.dataset.index),expectedSequence:Number(b.dataset.sequence)});
  else if(a==='submit-raid')await social.submitRaid();
  else if(a==='duel-move'||a==='duel-switch'){const d=social.model.duel;await social.store.act(d.id,a==='duel-move'?{type:'move',id}:{type:'switch',index:Number(b.dataset.index)},Number(b.dataset.sequence));}
  else if(a==='surrender')await social.store.surrender(social.model.duel.id,social.model.duel.revision);
  else if(a==='duel-timeout')await social.store.claimTimeout(social.model.duel.id);
  else if(a==='return')await social.returnToTrail();
  else if(a==='dismiss-reward')await social.dismissReceipt(id);
 });}catch(error){socialFailure(error);}
});

window.addEventListener('pageshow',event=>{if(event.persisted&&state){void social.start();renderGPS();}});
