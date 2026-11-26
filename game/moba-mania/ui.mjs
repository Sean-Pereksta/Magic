import { HEROES, DIFFICULTIES, WORLD, RULES } from './data.mjs';
import { Game } from './engine.mjs';
import { Renderer, drawPortrait, drawIcon } from './renderer.mjs';
import { resonanceResource } from './expansion-visuals.mjs';
import { pickEnemyAt } from './input.mjs';
import { captureMotion, renderMotion } from './render-motion.mjs';
import { applyOnlineSnapshot } from './online-match.mjs';
import { createOnlineLobby } from './online-ui.mjs';
import { ITEMS, ITEM_BY_ID, purchaseState, describeBonuses, drawItemIcon } from './items.mjs';
import { PROFILE_KEY, SAVE_KEY, COSMETICS, readProfile, writeProfile, heroCosmetics, equipCosmetic, recordMatch, readSavedMatch, saveMatch, clearSavedMatch, itemPlan, recommendedItems, previewItem, normalizeControls, matchBadges } from './profile.mjs';

const $ = id => document.getElementById(id);
const canvas = $('battlefield');
const keys = new Set();
const hotkeys = ['KeyQ','KeyE','KeyR','KeyF'];
const preferenceKey = 'moba-mania.preferences.v2';
let saved = {};
let storage=null;try{storage=window.localStorage;}catch{/* Storage is optional. */}
let profile=readProfile(storage);
try { saved = JSON.parse(localStorage.getItem(preferenceKey) || '{}') || {}; } catch { /* Storage may be unavailable in private tabs. */ }
let selectedHero = HEROES[saved.hero] ? saved.hero : 'arc';
let soundEnabled = saved.sound !== false;
let game = null;
let renderer = new Renderer(canvas);
let selectedSlot = -1;
let aim = { x: WORLD.width / 2, y: WORLD.height / 2 };
let moveTo = null;
let touchAttack = false;
let previousMotion=null, lastCombatCue=null, lastMotionGame=null;
let joystick = { x:0, y:0, pointerId:null };
let castDrag = null;
let pointer = { x:innerWidth / 2, y:innerHeight / 2, valid:false };
let lastFrame = 0, accumulator = 0, hudTimer = 0;
let lastEventId = null, lastLevel = 1, lastHealth = 1, lastShield=0, lastGoldSound=null;
let announcementUntil = 0, ended = false;
let endSequence = null;
const finaleSeconds = 1.2;
let helpReturn = null;
let audioContext = null, lastToneAt = 0;
let shopTier = 1, shopRenderKey = '', pinnedGoal=null;
let controls=normalizeControls(saved.controls),controlsReturn=null;
let saveTimer=0,savedMatchState={status:'empty'};
let minimapDrag=null,ignoreMapClick=false;
const abilities = [];
let animationFrame = 0;
let onlineMatch = false, snapshotAt = 0, snapshotSpan = 75, lastMoveSent = 0, lastNetworkInput = '';
let talentRenderKey = '';
let pendingPurchase = null;
const online = createOnlineLobby({
  getHero: () => selectedHero,
  onSnapshot: receiveOnline,
  onLobby: () => !onlineMatch,
  onStatus: showOnlineStatus,
  onExit: exitOnline
});

function savePreferences() {
  try { localStorage.setItem(preferenceKey, JSON.stringify({hero:selectedHero, difficulty:$('difficulty').value, sound:soundEnabled,controls})); } catch { /* The game does not require storage. */ }
}
function formatTime(seconds) {
  const value = Math.max(0, Math.floor(seconds || 0));
  return `${Math.floor(value / 60)}:${String(value % 60).padStart(2,'0')}`;
}
function setTouch() { document.body.classList.add('touch'); }
if (matchMedia('(pointer:coarse)').matches || navigator.maxTouchPoints > 0) setTouch();
window.addEventListener('pointerdown', event => { if (event.pointerType === 'touch') {setTouch();applyControls();} }, {passive:true});

function unlockAudio() {
  if (!soundEnabled) return;
  try {
    const Audio = window.AudioContext || window.webkitAudioContext;
    if (!Audio) return;
    audioContext ||= new Audio();
    if (audioContext.state === 'suspended') audioContext.resume().catch(()=>{});
  } catch { /* Audio is optional. */ }
}
function tone(kind='cast', slot=0) {
  if (!soundEnabled || !audioContext || audioContext.state !== 'running') return;
  const now = audioContext.currentTime;
  if (now-lastToneAt < .055) return;
  lastToneAt = now;
  const notes = kind === 'impact' ? [205,154] : kind === 'gold' ? [880,1320] : kind === 'block' ? [235,315] : kind === 'heavy' ? [125,78] : kind === 'frost' ? [988,740,1110] : kind === 'level' ? [523,659,784] : kind === 'win' ? [392,523,659,784] : kind === 'hurt' ? [110] : kind === 'error' ? [164] : [330 + slot*85, 495 + slot*65];
  notes.forEach((frequency,index)=>{
    const osc = audioContext.createOscillator();
    const gain = audioContext.createGain();
    osc.type = ['hurt','heavy','block','impact'].includes(kind) ? 'triangle' : kind==='frost'||kind==='gold'?'sine':'square';
    osc.frequency.setValueAtTime(frequency, now+index*.06);
    gain.gain.setValueAtTime(0,now+index*.06);
    gain.gain.linearRampToValueAtTime(kind === 'hurt' ? .017 : .014,now+index*.06+.008);
    gain.gain.exponentialRampToValueAtTime(.0001,now+index*.06+.13);
    osc.connect(gain); gain.connect(audioContext.destination);
    osc.start(now+index*.06); osc.stop(now+index*.06+.15);
    osc.onended=()=>{osc.disconnect();gain.disconnect();};
  });
}
function notify(message, duration=3000) {
  $('announcement').textContent = message;
  $('announcement').classList.add('visible');
  announcementUntil = performance.now() + duration;
}
function rewardPulse(element, className) {
  element.classList.remove(className);
  // Restart the brief CSS cue only on an earned reward, never every HUD frame.
  void element.offsetWidth;
  element.classList.add(className);
}
function drawMenu() {
  const grid=$('hero-grid'),scrollTop=grid.scrollTop,firstDraw=!grid.children.length;
  const focusedHero=document.activeElement?.dataset.hero,focusedCosmetic=document.activeElement?.dataset.cosmetic;
  grid.replaceChildren();$('hero-count').textContent=`${Object.keys(HEROES).length} HEROES`;
  Object.values(HEROES).forEach(hero=>{
    const button = document.createElement('button');
    button.className = `hero-card${hero.id === selectedHero ? ' selected' : ''}`;
    button.dataset.hero=hero.id;
    button.style.setProperty('--hero-color',hero.color);
    button.setAttribute('aria-pressed',String(hero.id === selectedHero));
    const portrait = document.createElement('canvas'); portrait.width=96; portrait.height=96; portrait.setAttribute('aria-hidden','true');
    const name = document.createElement('strong'); name.textContent=hero.name;
    const role = document.createElement('small'); role.textContent=hero.role;
    button.append(portrait,name,role);
    button.addEventListener('click',()=>{selectedHero=hero.id;drawMenu();savePreferences();unlockAudio();tone('cast');});
    grid.append(button);
    drawPortrait(portrait,hero.id,96,heroCosmetics(profile,hero.id));
  });
  const hero = HEROES[selectedHero];
  const detail = $('hero-detail'); detail.replaceChildren();
  const info=document.createElement('div');
  const title=document.createElement('h3');title.textContent=hero.title;
  const desc=document.createElement('p');desc.textContent=hero.description;
  info.append(title,desc);
  if(hero.specialty){
    const role=document.createElement('p');
    role.className='online-specialty';
    role.style.setProperty('--role-color',hero.specialty.color);
    role.textContent=hero.role+' · '+hero.specialty.description;
    info.append(role);
  }
  if(hero.passive){const passive=document.createElement('p');passive.className='hero-passive';passive.textContent=`${hero.passive.name} · ${hero.passive.description}`;info.append(passive);}
  const skills=document.createElement('div');skills.className='hero-skills';
  hero.abilities.forEach(ability=>{
    const item=document.createElement('div');item.className='hero-skill';item.title=`Level ${ability.unlock}: ${ability.description}`;
    const icon=document.createElement('canvas');icon.width=32;icon.height=32;icon.setAttribute('aria-hidden','true');
    const label=document.createElement('span');label.textContent=ability.name;
    item.append(icon,label);skills.append(item);drawIcon(icon,ability);
  });
  detail.append(info,skills);renderMastery();
  grid.scrollTop=scrollTop;
  // Replacing a card must not lose keyboard focus or jump the independent roster.
  if(focusedHero)grid.querySelector(`[data-hero="${focusedHero}"]`)?.focus({preventScroll:true});
  else if(focusedCosmetic)$('mastery-options').querySelector(`[data-cosmetic="${focusedCosmetic}"]`)?.focus({preventScroll:true});
  if(firstDraw)requestAnimationFrame(()=>{
    const card=grid.querySelector(`[data-hero="${selectedHero}"]`);if(!card)return;
    const bounds=grid.getBoundingClientRect(),selected=card.getBoundingClientRect();
    if(selected.bottom>bounds.bottom-5)grid.scrollTop+=selected.bottom-bounds.bottom+5;
    else if(selected.top<bounds.top+5)grid.scrollTop+=selected.top-bounds.top-5;
  });
}
function renderMastery(){
  const entry=profile.heroes[selectedHero],next=COSMETICS.find(c=>c.xp>entry.xp);
  $('mastery-title').textContent=`${HEROES[selectedHero].name} mastery`;
  $('mastery-progress').textContent=`${entry.xp} XP · ${entry.matches} matches${next?` · ${next.xp-entry.xp} to ${next.name}`:' · All cosmetics unlocked'}`;
  const options=$('mastery-options');options.replaceChildren();
  for(const cosmetic of COSMETICS){
    const button=document.createElement('button'),unlocked=entry.xp>=cosmetic.xp;
    button.dataset.cosmetic=cosmetic.id;button.disabled=!unlocked;button.setAttribute('aria-pressed',String(entry.equipped===cosmetic.id));
    button.textContent=`${entry.equipped===cosmetic.id?'✓ ':''}${cosmetic.name}${unlocked?'':` · ${cosmetic.xp} XP`}`;
    button.title=unlocked?'Equip armor color, trail, portrait frame and victory crest':`Finish matches with ${HEROES[selectedHero].name} to unlock this cosmetic set`;
    button.style.setProperty('--cosmetic-color',cosmetic.armorTint||HEROES[selectedHero].color);
    button.addEventListener('click',()=>{if(equipCosmetic(profile,selectedHero,cosmetic.id)){writeProfile(storage,profile);drawMenu();}});options.append(button);
  }
}
function refreshSavedMenu(){
  savedMatchState=readSavedMatch(storage,snapshot=>Game.restore(snapshot));
  const ready=savedMatchState.status==='ready';
  $('saved-match-panel').hidden=['empty','unavailable'].includes(savedMatchState.status);
  $('resume-saved-match').hidden=!ready;
  $('saved-match-title').textContent=ready?`${HEROES[savedMatchState.record.heroId].name} · Level ${savedMatchState.game.player.level}`:'Saved match needs a fresh start';
  $('saved-match-description').textContent=ready?`${formatTime(savedMatchState.game.time)} played · Resume safely paused. Saved only on this device.`:'This save could not be loaded. Starting a new match will ask before replacing it. Your mastery is safe.';
}

Object.values(DIFFICULTIES).forEach(difficulty=>{
  const option=document.createElement('option');option.value=difficulty.id;option.textContent=difficulty.name;$('difficulty').append(option);
});
$('difficulty').value=DIFFICULTIES[saved.difficulty] ? saved.difficulty : 'normal';
function updateDifficulty() {
  const difficulty=DIFFICULTIES[$('difficulty').value];
  $('difficulty-description').textContent=difficulty.description;
  $('match-format').textContent=`3 vs ${difficulty.enemyHeroes||3}`;
  savePreferences();
}
$('difficulty').addEventListener('change',updateDifficulty);
updateDifficulty();
drawMenu();refreshSavedMenu();applyControls();

function buildAbilities() {
  $('ability-bar').replaceChildren();abilities.length=0;
  HEROES[game.player.heroId].abilities.forEach((ability,slot)=>{
    const button=document.createElement('button');button.className='ability';button.dataset.slot=String(slot);
    button.style.setProperty('--ability-color',ability.color);
    button.title=`${hotkeys[slot].slice(3)} · ${ability.name}\n${ability.description}\n${ability.mana} mana · ${ability.cooldown}s cooldown · unlocks at level ${ability.unlock}`;
    button.setAttribute('aria-label',button.title.replaceAll('\n','. '));
    const key=document.createElement('span');key.className='ability-key';key.textContent=hotkeys[slot].slice(3);
    const icon=document.createElement('canvas');icon.width=32;icon.height=32;icon.setAttribute('aria-hidden','true');
    const name=document.createElement('span');name.className='ability-name';name.textContent=ability.name;
    const cost=document.createElement('span');cost.className='ability-cost';cost.textContent=`${ability.mana} MP`;
    const cooldown=document.createElement('span');cooldown.className='ability-cooldown';cooldown.setAttribute('aria-hidden','true');
    button.append(key,icon,name,cost,cooldown);$('ability-bar').append(button);drawIcon(icon,ability);
    abilities.push({button,cooldown,ability});
    button.addEventListener('pointerdown',event=>{
      if(!playing() || !canArm(slot))return;
      event.preventDefault();unlockAudio();button.setPointerCapture(event.pointerId);
      armAbility(slot);
      castDrag={id:event.pointerId,slot,startX:event.clientX,startY:event.clientY,dragged:false,pointerType:event.pointerType};
    });
    button.addEventListener('pointermove',event=>{
      if(!castDrag || castDrag.id!==event.pointerId)return;
      const dx=event.clientX-castDrag.startX,dy=event.clientY-castDrag.startY,distance=Math.hypot(dx,dy);
      if(distance<9 && !castDrag.dragged)return;
      castDrag.dragged=true;
      const effective=game.getAbility(game.player,slot);
      const range=effective.range||effective.radius||130;
      const reach=Math.min(1,distance/78)*range;
      aim={x:game.player.x+dx/(distance||1)*reach,y:game.player.y+dy/(distance||1)*reach};
      pointer.valid=false;
    });
    button.addEventListener('pointerup',event=>{
      if(!castDrag || castDrag.id!==event.pointerId)return;
      const drag=castDrag;castDrag=null;
      if(drag.dragged || ability.type==='self') castSelected();
    });
    button.addEventListener('pointercancel',()=>{castDrag=null;cancelAim();});
    // Keyboard / assistive activation has no pointer sequence.
    button.addEventListener('click',event=>{if(event.detail===0 && playing() && canArm(slot))armAbility(slot);});
  });
}
function playing() { return !!game && !game.paused && game.winner==null && !$('hud').hidden && (!onlineMatch || online.connected); }
function order(action, local) {
  return onlineMatch ? online.command(action) : local();
}
function clearInput() {
  renderer.peek=endSequence?.game===game&&!ended?endSequence.focus:null;minimapDrag=null;$('peek-hint').hidden=true;
  keys.clear();touchAttack=false;moveTo=null;castDrag=null;
  joystick.x=joystick.y=0;joystick.pointerId=null;
  if(onlineMatch){
    sendOnlineMove(performance.now(),true);
    if(game?.winner==null)online.command({type:'cancel'});
  }else if(game)game.cancelPlayerOrder();
  $('joystick-knob').style.transform='translate(0px,0px)';
  $('attack-button').classList.remove('held');
  cancelAim();
}
function startMatch() {
  let hasSave=false;try{hasSave=!!storage?.getItem(SAVE_KEY);}catch{/* No storage. */}
  if(hasSave&&(!game||ended)){
    $('replace-save-dialog').showModal();return;
  }
  beginMatch();
}
function beginMatch(restored=null,ui={},replace=false,networkGame=null) {
  document.querySelectorAll('dialog[open]').forEach(dialog=>dialog.close());
  clearInput();
  onlineMatch=Boolean(networkGame);
  if(!networkGame)selectedHero=restored?.player.heroId||(HEROES[selectedHero]?selectedHero:'arc');
  game=networkGame||restored||new Game({hero:selectedHero,difficulty:$('difficulty').value,seed:Math.floor(Math.random()*0x7fffffff)});
  if(!game.matchId)game.matchId=globalThis.crypto?.randomUUID?.()||`${Date.now()}-${Math.random().toString(36).slice(2)}`;
  const heroId=game.player.heroId;
  game.player.cosmetics=heroCosmetics(profile,heroId);
  ended=false;endSequence=null;document.body.classList.remove('core-finale');$('shop-button').classList.remove('gold-pulse');$('hero-level').classList.remove('level-pulse');lastEventId=null;lastLevel=game.player.level;lastHealth=game.player.hp;lastShield=game.player.shield||0;lastGoldSound=game.player.lastGoldGain?.time??null;accumulator=0;hudTimer=0;saveTimer=0;lastFrame=performance.now();
  shopTier=1;shopRenderKey='';pendingPurchase=null;pinnedGoal=ITEM_BY_ID[ui.pinnedGoal]?.tier===3?ui.pinnedGoal:null;$('shop-owned').open=false;
  $('menu-screen').hidden=true;$('hud').hidden=false;
  $('save-match').hidden=onlineMatch;
  $('quit-match').textContent=onlineMatch?'Leave online lobby':'Save & return to heroes';
  $('pause-title').textContent=onlineMatch?'Match options':'Rift paused';
  $('pause-button').setAttribute('aria-label',onlineMatch?'Match options':'Pause match');
  $('pause-button').title=onlineMatch?'Match options (Esc)':'Pause (Esc)';
  $('play-again').textContent=onlineMatch?'Return to lobby':'PLAY AGAIN';
  $('change-hero').textContent=onlineMatch?'Leave online lobby':'CHOOSE ANOTHER HERO';
  const captions=document.querySelectorAll('.map-caption span');
  captions[0].textContent=onlineMatch?'AZURE':'DAWN';
  captions[1].textContent=onlineMatch?'ROSE':'DUSK';
  showOnlineStatus('');
  $('hero-name').textContent=HEROES[heroId].name;
  drawPortrait($('hero-portrait'),heroId,48,game.player.cosmetics);
  aim={x:game.player.x+300,y:game.player.y};pointer.valid=false;
  buildAbilities();renderer.hasCamera=false;renderer.lastTime=null;renderer.peek=null;renderer.resize(innerWidth,innerHeight);
  refreshHUD();savePreferences();unlockAudio();applyControls();
  document.documentElement.dataset.catnmicePlaying='true';window.dispatchEvent(new Event('catnmice-game-started'));
  if(restored){
    setPaused(true);$('pause-message').textContent='Saved match restored. Press Back to the battle when you are ready.';$('pause-dialog').showModal();
  }else{
    game.paused=false;saveCurrent(false,replace);notify('Stay with your wave. The river beasts awaken soon.',4200);canvas.focus({preventScroll:true});
  }
}
function saveCurrent(manual=false,replace=false){
  if(onlineMatch||!game||ended||game.winner!=null)return false;
  const result=saveMatch(storage,game,{pinnedGoal},replace);saveTimer=0;
  if(manual)$('pause-message').textContent=result.ok?'Match saved on this device. It will resume paused.':result.reason;
  return result.ok;
}

function receiveOnline(state) {
  const now=performance.now();
  if(!onlineMatch || game.matchId!==state.matchId){
    const view=new Game({hero:'arc',seed:1});
    if(!applyOnlineSnapshot(view,state))return;
    previousMotion=null;
    snapshotSpan=75;
    snapshotAt=now;
    lastMoveSent=0;
    lastNetworkInput='';
    beginMatch(null,{},false,view);
    return;
  }
  const oldMotion=captureMotion(game);
  if(!applyOnlineSnapshot(game,state))return;
  previousMotion=oldMotion;
  snapshotSpan=Math.min(150,Math.max(33,now-snapshotAt));
  snapshotAt=now;
  game.player.cosmetics=heroCosmetics(profile,game.player.heroId);
}

function showOnlineStatus(message,notice=false) {
  if(notice && onlineMatch){notify(message);return;}
  const status=$('online-match-status');
  $('match-reconnect').hidden=!onlineMatch || online.connected;
  status.hidden=!message || !onlineMatch;
  status.textContent=message;
}

function exitOnline() {
  if(!onlineMatch)return;
  onlineMatch=false;
  game=null;endSequence=null;ended=false;previousMotion=null;
  document.body.classList.remove('core-finale');
  $('hud').hidden=true;$('menu-screen').hidden=false;
  document.querySelectorAll('dialog[open]').forEach(dialog=>dialog.close());
  clearInput();drawMenu();refreshSavedMenu();
  delete document.documentElement.dataset.catnmicePlaying;
}
$('start-match').addEventListener('click',startMatch);
$('keep-saved-match').addEventListener('click',()=>closeDialog('replace-save-dialog'));
$('replace-saved-match').addEventListener('click',()=>beginMatch(null,{},true));
$('resume-saved-match').addEventListener('click',()=>{
  refreshSavedMenu();if(savedMatchState.status==='ready')beginMatch(savedMatchState.game,savedMatchState.record.ui||{});
});
$('save-match').addEventListener('click',()=>saveCurrent(true));

function canArm(slot,quiet=false) {
  const hero=game?.player;
  if(!hero?.alive) { if(!quiet)notify('Your hero is returning to the healing well.'); return false; }
  const ability=game.getAbility(hero,slot);
  if(hero.level<ability.unlock) { if(!quiet)notify(`${ability.name} unlocks at level ${ability.unlock}.`);return false; }
  if((hero.cooldowns[slot]||0)>0) { if(!quiet)notify(`${ability.name} ready in ${Math.ceil(hero.cooldowns[slot])}s.`,1600);return false; }
  if(hero.mana<ability.mana) { if(!quiet)notify('Low mana. Recall to your well to recover.',2200);return false; }
  return true;
}
function armAbility(slot) {
  selectedSlot=slot;
  const ability=game.getAbility(game.player,slot);
  $('aim-hint').hidden=false;
  $('aim-hint').querySelector('span').textContent=`${ability.name} · ${ability.type==='self'?'cast around you':'aim & release / tap ground'}`;
  refreshAbilityHUD();
}
function cancelAim() {
  selectedSlot=-1;$('aim-hint').hidden=true;
  for(const {button} of abilities)button.classList.remove('selected');
}
function castSelected() {
  if(selectedSlot<0 || !playing())return;
  const slot=selectedSlot;
  if(order({type:'cast',slot,x:aim.x,y:aim.y},()=>game.cast(game.player,slot,aim))) {const id=HEROES[game.player.heroId].abilities[slot].id;tone(['winterfall','glacial-sweep'].includes(id)?'frost':['royal-quake','sky-cleave','execution-cut','shield-bash'].includes(id)?'heavy':'cast',slot);moveTo=null; }
  else if(canArm(slot)) notify('Cannot cast while stunned or recalling.',1500);
  cancelAim();refreshHUD();
}
$('cancel-aim').addEventListener('click',cancelAim);

canvas.addEventListener('contextmenu',event=>event.preventDefault());
canvas.addEventListener('pointermove',event=>{
  pointer={x:event.clientX,y:event.clientY,valid:true};
  if(!castDrag)aim=renderer.screenToWorld(event.clientX,event.clientY);
});
function enemyAt(clientX,clientY,pointerType) {
  const zoom=canvas.getBoundingClientRect().width/renderer.camera.width;
  return pickEnemyAt({units:game.units,team:game.player.team,point:{x:clientX,y:clientY},zoom,pointerType,
    worldToScreen:(x,y)=>renderer.worldToScreen(x,y),canSee:unit=>!game.canSee||game.canSee(game.player.team,unit)});
}
function attackNearest() {
  if(!playing()||!game.player.alive)return;
  moveTo=null;
  order({type:'attack'},()=>game.attackNearest());
  refreshHUD();
}
function movePlayerTo(point) {
  moveTo=null;
  order({type:'moveTo',x:point.x,y:point.y},()=>game.movePlayerTo(point));
  refreshHUD();
}
canvas.addEventListener('pointerdown',event=>{
  if(!playing()||!game.player.alive||![0,2].includes(event.button))return;
  event.preventDefault();unlockAudio();canvas.focus({preventScroll:true});
  aim=renderer.screenToWorld(event.clientX,event.clientY);
  pointer={x:event.clientX,y:event.clientY,valid:event.pointerType!=='touch'};
  if(event.button===2){cancelAim();movePlayerTo({...aim});return;}
  if(selectedSlot>=0){castSelected();return;}
  const target=enemyAt(event.clientX,event.clientY,event.pointerType);
  if(target){
    moveTo=null;
    if(game.isProtected(target)){
      order({type:'cancel'},()=>game.cancelPlayerOrder());
      notify(target.kind==='core'?'Break an inner tower to expose this crystal.':"Destroy this lane's outer tower first.");
    }else order({type:'attackTarget',targetId:target.id},()=>game.attackTarget(target.id));
    refreshHUD();
  }
  else attackNearest();
});
window.addEventListener('keydown',event=>{
  if(event.target.matches('input,select,textarea'))return;
  if(event.code==='Escape'){
    if(selectedSlot>=0){event.preventDefault();cancelAim();return;}
    if(document.querySelector('dialog[open]'))return;
    if(game&&!ended&&!$('hud').hidden){event.preventDefault();openPause();}return;
  }
  if(!playing())return;
  const controlled=['KeyW','KeyA','KeyS','KeyD','ArrowUp','ArrowDown','ArrowLeft','ArrowRight','Space','KeyQ','KeyE','KeyR','KeyF','KeyB','KeyT','KeyI','Tab'];
  if(!controlled.includes(event.code))return;
  event.preventDefault();unlockAudio();
  if(event.repeat)return;
  keys.add(event.code);
  const slot=hotkeys.indexOf(event.code);
  if(slot>=0&&canArm(slot))armAbility(slot);
  if(event.code==='KeyT')openTalents();
  if(event.code==='KeyI')openShop();
  if(event.code==='KeyB')recall();
  if(event.code==='Tab')openMap();
  if(event.code==='Space')attackNearest();
  if(['KeyW','KeyA','KeyS','KeyD','ArrowUp','ArrowDown','ArrowLeft','ArrowRight'].includes(event.code)){
    moveTo=null;
    if(onlineMatch)sendOnlineMove(performance.now(),true);
    else game.cancelPlayerOrder();
  }
});
window.addEventListener('keyup',event=>{
  keys.delete(event.code);
  if(onlineMatch&&playing())sendOnlineMove(performance.now());
  const slot=hotkeys.indexOf(event.code);
  if(slot>=0 && selectedSlot===slot && playing()){event.preventDefault();castSelected();}
});
window.addEventListener('blur',()=>{if(playing())openPause();else clearInput();saveCurrent();});
document.addEventListener('visibilitychange',()=>{if(document.hidden){if(playing())openPause();saveCurrent();}});

const stick=$('joystick');
function updateStick(event) {
  const rect=stick.getBoundingClientRect(),radius=rect.width*.34;
  const dx=event.clientX-rect.left-rect.width/2,dy=event.clientY-rect.top-rect.height/2,distance=Math.hypot(dx,dy),scale=Math.min(1,distance/radius);
  joystick.x=distance>5?dx/(distance||1)*scale:0;joystick.y=distance>5?dy/(distance||1)*scale:0;
  const drawRadius=stick.offsetWidth*.34;
  $('joystick-knob').style.transform=`translate(${joystick.x*drawRadius}px,${joystick.y*drawRadius}px)`;
  moveTo=null;
  if(!onlineMatch&&Math.hypot(joystick.x,joystick.y)>.05)game.cancelPlayerOrder();
}
stick.addEventListener('pointerdown',event=>{
  if(!playing()||joystick.pointerId!=null)return;
  event.preventDefault();unlockAudio();joystick.pointerId=event.pointerId;stick.setPointerCapture(event.pointerId);updateStick(event);
});
stick.addEventListener('pointermove',event=>{if(event.pointerId===joystick.pointerId)updateStick(event);});
function releaseStick(event){if(event.pointerId!==joystick.pointerId)return;joystick.x=joystick.y=0;joystick.pointerId=null;$('joystick-knob').style.transform='translate(0px,0px)';}
stick.addEventListener('pointerup',releaseStick);stick.addEventListener('pointercancel',releaseStick);
$('attack-button').addEventListener('pointerdown',event=>{
  if(!playing())return;event.preventDefault();unlockAudio();touchAttack=true;attackNearest();
  $('attack-button').setPointerCapture(event.pointerId);
});
$('attack-button').addEventListener('pointerup',()=>{touchAttack=false;});
$('attack-button').addEventListener('pointercancel',()=>{touchAttack=false;});
$('attack-button').addEventListener('click',event=>{if(event.detail===0){unlockAudio();attackNearest();}});
function recall(){if(playing()&&order({type:'recall'},()=>game.recall(game.player))){clearInput();notify('Recalling… stay still for 4 seconds.',2500);}else if(game?.player.alive)notify('Recall needs a safe moment.');}
$('recall-button').addEventListener('click',recall);

function setPaused(paused) {
  if(!game)return;
  game.paused=paused;
  clearInput();accumulator=0;lastFrame=performance.now();
}
function openDialog(id) {
  if(game?.winner!=null&&id!=='end-dialog')return;
  setPaused(true);
  const dialog=$(id);if(!dialog.open)dialog.showModal();
}
function closeDialog(id) { $(id).close(); }
function afterDialogClosed() {
  if(game && game.winner==null && !ended && !$('hud').hidden && !document.querySelector('dialog[open]')){setPaused(false);canvas.focus({preventScroll:true});}
}
for(const dialog of document.querySelectorAll('dialog'))dialog.addEventListener('close',afterDialogClosed);
function openPause(){
  if(!game||ended||$('hud').hidden)return;
  $('pause-message').textContent=onlineMatch?'The match continues while this menu is open.':'Your next great play can wait a moment.';
  $('match-reconnect').hidden=!onlineMatch||online.connected;
  openDialog('pause-dialog');
  saveCurrent();
}
$('pause-button').addEventListener('click',openPause);
$('match-reconnect').addEventListener('click',()=>{online.reconnect();$('match-reconnect').hidden=true;});
$('resume-match').addEventListener('click',()=>closeDialog('pause-dialog'));
$('quit-match').addEventListener('click',()=>{
  if(onlineMatch){online.leave();return;}
  if(!saveCurrent(true))return;
  $('hud').hidden=true;$('menu-screen').hidden=false;game=null;clearInput();
  document.querySelectorAll('dialog[open]').forEach(dialog=>dialog.close());drawMenu();refreshSavedMenu();
  delete document.documentElement.dataset.catnmicePlaying;
});
$('fullscreen-button').addEventListener('click',async()=>{
  try{if(document.fullscreenElement)await document.exitFullscreen();else await document.documentElement.requestFullscreen();}catch{notify('Fullscreen is unavailable on this device.');}
});
function openHelp(from=null){helpReturn=from;if(from)$(from).close();openDialog('help-dialog');}
$('menu-help').addEventListener('click',()=>openHelp());
$('pause-help').addEventListener('click',()=>openHelp('pause-dialog'));
$('close-help').addEventListener('click',()=>closeDialog('help-dialog'));
$('help-dialog').addEventListener('close',()=>{if(helpReturn){const target=helpReturn;helpReturn=null;openDialog(target);}});
$('sound-button').addEventListener('click',()=>{soundEnabled=!soundEnabled;updateSoundButton();savePreferences();if(soundEnabled){unlockAudio();tone('cast');}});
function updateSoundButton(){$('sound-button').textContent=soundEnabled?'♪':'×♪';$('sound-button').setAttribute('aria-label',soundEnabled?'Mute sound':'Enable sound');}
updateSoundButton();

function applyControls(){
  controls=normalizeControls(controls);
  const touch=document.body.classList.contains('touch'),bar=$('bottom-controls'),stick=$('joystick');
  const baseWidth=bar.offsetWidth||174,stickWidth=stick.offsetWidth||96;
  const scale=touch?Math.max(.65,Math.min(controls.scale,(innerWidth-32)/(baseWidth+stickWidth+12))):1;
  const inset=Math.max(0,Math.min(controls.inset,(innerWidth-(baseWidth+stickWidth)*scale-32)/2));
  document.documentElement.style.setProperty('--control-scale',String(scale));
  document.documentElement.style.setProperty('--control-lift',`${Math.min(controls.height,Math.max(0,innerHeight-(innerWidth>560?160:210)-Math.max(bar.offsetHeight||122,stick.offsetHeight||108)*scale-32))}px`);
  document.documentElement.style.setProperty('--control-inset',`${inset}px`);
  document.body.classList.toggle('left-controls',controls.handed==='left');
  $('control-size').value=String(Math.round(controls.scale*100));$('control-size-value').textContent=`${Math.round(controls.scale*100)}%`;
  $('control-height').value=String(controls.height);$('control-height-value').textContent=`${controls.height}px`;
  $('control-inset').value=String(controls.inset);$('control-inset-value').textContent=`${controls.inset}px`;
  $('control-handed').value=controls.handed;
}
function openControls(from=null){controlsReturn=from;if(from)$(from).close();applyControls();openDialog('controls-dialog');}
$('menu-controls').addEventListener('click',()=>openControls());
$('pause-controls').addEventListener('click',()=>openControls('pause-dialog'));
$('close-controls').addEventListener('click',()=>closeDialog('controls-dialog'));
$('controls-dialog').addEventListener('close',()=>{if(controlsReturn){const target=controlsReturn;controlsReturn=null;openDialog(target);}});
for(const id of ['control-size','control-height','control-inset','control-handed'])$(id).addEventListener('input',()=>{
  controls={scale:Number($('control-size').value)/100,height:Number($('control-height').value),inset:Number($('control-inset').value),handed:$('control-handed').value};applyControls();savePreferences();
});
for(const button of document.querySelectorAll('[data-control-preset]'))button.addEventListener('click',()=>{
  controls={...controls,...({default:{scale:1,height:0,inset:0},compact:{scale:.85,height:0,inset:0},large:{scale:1.2,height:30,inset:4}}[button.dataset.controlPreset])};applyControls();savePreferences();
});
function shopStateKey(hero) {
  return [pinnedGoal,hero.gold,(hero.items||[]).join(','),hero.level,Object.values(hero.talents).join(',')].join('|');
}
function itemIcon(item, size=40) {
  const icon=document.createElement('canvas');icon.width=size;icon.height=size;icon.setAttribute('aria-hidden','true');
  drawItemIcon(icon.getContext('2d'),item,0,0,size);return icon;
}
function openShop() {
  if(!game||ended||$('hud').hidden)return;
  $('shop-message').textContent='Choose an item. Its bonuses become active as soon as you buy it.';
  renderShop();openDialog('shop-dialog');
}
function showShopTier(tier, focusItem=null) {
  shopTier=tier;renderShop();
  if(focusItem){
    const card=[...$('shop-items').children].find(el=>el.dataset.itemId===focusItem);
    card?.scrollIntoView({block:'nearest'});card?.focus({preventScroll:true});
  }else $('shop-items').scrollTop=0;
}
function renderShop() {
  if(!game)return;
  const hero=game.player,owned=new Set(hero.items||[]);
  shopRenderKey=shopStateKey(hero);
  $('shop-balance').textContent=String(Math.floor(hero.gold||0));
  const stats=$('shop-stats');stats.replaceChildren();
  const spellBoost=((1+game.bonus(hero,'damage'))*(1+game.bonus(hero,'spellDamage'))-1)*100;
  for(const [value,label] of [[Math.round(hero.maxHp),'MAX HEALTH'],[hero.damage.toFixed(1),'ATTACK DAMAGE'],[`+${Math.round(spellBoost)}%`,'SPELL DAMAGE'],[`${Math.round(Math.min(.5,game.bonus(hero,'cooldown'))*100)}%`,'COOLDOWN CUT']]){
    const stat=document.createElement('div'),amount=document.createElement('strong'),caption=document.createElement('small');
    amount.textContent=String(value);caption.textContent=label;stat.append(amount,caption);stats.append(stat);
  }
  document.querySelectorAll('[data-shop-tier]').forEach(button=>{
    const tier=Number(button.dataset.shopTier),count=ITEMS.filter(item=>item.tier===tier).length;
    button.setAttribute('aria-pressed',String(tier===shopTier));
    button.querySelector('small').textContent=`${count} items · ${tier===1?'no requirements':tier===2?'needs a starter':'needs both tiers'}`;
  });
  renderBuildGoal();
  const recommended=new Set(recommendedItems(hero).map(item=>item.id));
  const list=$('shop-items'),scrollTop=list.scrollTop;list.replaceChildren();list.setAttribute('aria-label',`Tier ${shopTier} items`);
  for(const item of ITEMS.filter(item=>item.tier===shopTier)){
    const state=purchaseState(hero,item.id),isOwned=owned.has(item.id);
    const card=document.createElement('article');card.className=`shop-item ${item.tier===3?'epic ':''}${isOwned?'owned':state.ok?'ready':state.missing.length?'locked':'short'}`;
    card.dataset.itemId=item.id;card.dataset.itemState=isOwned?'owned':state.ok?'ready':state.missing.length?'locked':'short';card.tabIndex=-1;
    card.style.setProperty('--item-color',item.color);
    const heading=document.createElement('div');heading.className='shop-item-heading';
    const title=document.createElement('h3');title.textContent=item.name;
    const price=document.createElement('span');price.className='shop-price';price.textContent=`${item.cost} gold`;
    const titleBlock=document.createElement('div');titleBlock.append(title,price);heading.append(itemIcon(item),titleBlock);
    if(item.tier===3){
      const rarity=document.createElement('small');rarity.className='shop-rarity';rarity.textContent='EPIC';titleBlock.append(rarity);
      const pin=document.createElement('button');pin.className='shop-pin';pin.dataset.pinItem=item.id;pin.textContent=pinnedGoal===item.id?'◆':'◇';
      pin.setAttribute('aria-pressed',String(pinnedGoal===item.id));pin.setAttribute('aria-label',`${pinnedGoal===item.id?'Unpin':'Pin'} ${item.name} as build goal`);
      pin.addEventListener('click',()=>{pinnedGoal=pinnedGoal===item.id?null:item.id;renderShop();refreshHUD();saveCurrent();});heading.append(pin);
      if(recommended.has(item.id)){const tag=document.createElement('small');tag.className='shop-recommended';tag.textContent='RECOMMENDED';titleBlock.append(tag);}
    }
    const bonuses=document.createElement('p');bonuses.className='shop-bonuses';bonuses.textContent=describeBonuses(item);
    const description=document.createElement('p');description.className='shop-description';description.textContent=item.description;
    if(item.passive){const passiveName=document.createElement('strong');passiveName.textContent=item.passive.name;description.replaceChildren(passiveName,document.createTextNode(` · ${item.passive.description}`));}
    const compare=createItemComparison(hero,item);
    const requirements=document.createElement('div');requirements.className='shop-requirements';
    if(item.requires.length){
      const label=document.createElement('small');label.textContent='REQUIRES';requirements.append(label);
      for(const id of item.requires){
        const required=ITEM_BY_ID[id],hasItem=owned.has(id),button=document.createElement('button');
        button.className=`shop-requirement ${hasItem?'met':'missing'}`;button.dataset.requiresItem=id;
        button.title=`${required.name} · ${hasItem?'owned and active':'view required item'}`;button.setAttribute('aria-label',`${required.name}: ${hasItem?'owned':'required'}. View item.`);
        const name=document.createElement('span');name.textContent=`${hasItem?'✓ ':''}${required.name}`;
        button.append(itemIcon(required,24),name);button.addEventListener('click',()=>showShopTier(required.tier,id));requirements.append(button);
      }
    }else{const label=document.createElement('small');label.textContent='NO REQUIREMENTS';requirements.append(label);}
    const action=document.createElement('button');action.className='shop-buy';action.dataset.buyItem=item.id;action.disabled=!state.ok;
    action.textContent=isOwned?'✓ ACTIVE':state.ok?`BUY · ${item.cost} GOLD`:state.missing.length?'REQUIRES ITEMS':`NEED ${state.goldNeeded} MORE GOLD`;
    action.setAttribute('aria-label',isOwned?`${item.name} is owned and active`:`Buy ${item.name} for ${item.cost} gold. ${state.ok?'Ready to buy.':state.reason}`);
    action.title=state.ok?`${describeBonuses(item)} · Activates immediately`:state.reason;
    action.addEventListener('click',()=>{
      if(onlineMatch){
        if(online.command({type:'buy',id:item.id})){
          pendingPurchase={id:item.id,deadline:performance.now()+2500};
          $('shop-message').textContent='Purchase requested.';
        }else $('shop-message').textContent='Reconnecting to the host.';
        return;
      }
      if(!game.buyItem(hero,item.id)){$('shop-message').textContent=purchaseState(hero,item.id).reason;renderShop();return;}
      tone('level');$('shop-message').textContent=`${item.name} is active. ${describeBonuses(item)}.`;
      renderShop();refreshHUD();saveCurrent();
      // Keep keyboard focus near the purchased item after its button becomes disabled.
      const purchased=[...$('shop-items').children].find(el=>el.dataset.itemId===item.id);
      purchased?.scrollIntoView({block:'nearest',inline:'nearest'});purchased?.focus({preventScroll:true});
    });
    card.append(heading,bonuses,description,requirements,compare,action);list.append(card);
  }
  list.scrollTop=scrollTop;
  $('shop-owned-count').textContent=`${owned.size} / ${ITEMS.length}`;
  const inventory=$('shop-inventory');inventory.replaceChildren();
  for(const item of ITEMS.filter(item=>owned.has(item.id))){
    const badge=document.createElement('div');badge.className='shop-owned-item';badge.dataset.ownedItem=item.id;
    badge.title=`${item.name} · ${describeBonuses(item)}${item.passive?` · ${item.passive.name}: ${item.passive.description}`:''}`;
    const name=document.createElement('span');name.textContent=item.name;badge.append(itemIcon(item,28),name);inventory.append(badge);
  }
  if(!owned.size){const empty=document.createElement('p');empty.textContent='Your purchased items will appear here. All of them stay active.';inventory.append(empty);}
}
function renderBuildGoal(){
  const plan=itemPlan(game.player,pinnedGoal);
  $('shop-goal').hidden=!plan.goal;$('shop-recommendations').hidden=!!plan.goal;
  if(plan.goal){
    $('shop-goal-title').textContent=`◆ ${plan.goal.name}`;
    $('shop-goal-cost').textContent=plan.remaining.length?`${plan.cost} gold remaining · ${plan.remaining.length} purchases`:'Build complete · all items active';
    const next=$('shop-goal-next');next.hidden=!plan.next;
    if(plan.next){next.textContent=`${plan.affordable?'BUYABLE':'NEXT'} · ${plan.next.name} · ${plan.next.cost}G`;next.dataset.goalNext=plan.next.id;next.classList.toggle('affordable',plan.affordable);}
  }else{
    const recommendations=$('shop-recommendations');recommendations.replaceChildren();
    const label=document.createElement('span');label.textContent=`${HEROES[game.player.heroId].name} builds`;recommendations.append(label);
    for(const item of recommendedItems(game.player)){
      const button=document.createElement('button');button.dataset.recommendItem=item.id;button.textContent=item.name;button.title=`Pin this epic as your build goal. ${describeBonuses(item)}`;
      button.addEventListener('click',()=>{pinnedGoal=item.id;renderShop();refreshHUD();saveCurrent();});recommendations.append(button);
    }
  }
}
function createItemComparison(hero,item){
  const preview=previewItem(game,hero,item.id),details=document.createElement('details');details.className='shop-compare';
  const summary=document.createElement('summary');summary.textContent='Compare actual stats';details.append(summary);
  const table=document.createElement('table');table.setAttribute('aria-label',`${item.name} before and after stats`);
  const heading=document.createElement('tr');for(const text of ['Stat','Now','With item']){const cell=document.createElement('th');cell.textContent=text;heading.append(cell);}table.append(heading);
  const rows=[['Health','hp',0],['Mana','mana',0],['Attack','damage',1],['Move speed','speed',1],['Attack interval','attackInterval',2],['Spell power ×','spellMultiplier',2]];
  for(const [label,key,digits] of rows){
    const row=document.createElement('tr');for(const value of [label,preview.before[key].toFixed(digits),preview.after[key].toFixed(digits)]){const cell=document.createElement('td');cell.textContent=value;row.append(cell);}table.append(row);
  }
  const specialRows=[['Final-hit XP','killXp'],['Final-hit gold','killGold'],['Burn damage','burnPower'],['Burn duration','burnDurationPct'],['Poison damage','poisonPower'],['Poison duration','poisonDurationPct'],['Slow strength','slowPower'],['Slow duration','slowDurationPct']];
  let hasSpecial=false;
  for(const [label,key] of specialRows){
    const before=preview.before[key]||0,after=preview.after[key]||0;
    if(!(before>0||after>0))continue;hasSpecial=true;
    const row=document.createElement('tr');for(const value of [label,`+${Math.round(before*100)}%`,`+${Math.round(after*100)}%`]){const cell=document.createElement('td');cell.textContent=value;row.append(cell);}table.append(row);
  }
  preview.before.abilities.forEach((ability,index)=>{
    const row=document.createElement('tr');for(const value of [ability.name,`${ability.cooldown.toFixed(2)}s`,`${preview.after.abilities[index].cooldown.toFixed(2)}s`]){const cell=document.createElement('td');cell.textContent=value;row.append(cell);}table.append(row);
  });details.append(table);
  const cooldown=document.createElement('p');cooldown.textContent=`Cooldown reduction: ${(preview.before.cooldownCut*100).toFixed(0)}% → ${(preview.after.cooldownCut*100).toFixed(0)}%.`;
  if(preview.capped){cooldown.className='cap-warning';cooldown.textContent+=' 50% cap reached; extra reduction adds no benefit.';}details.append(cooldown);
  if(hasSpecial){const limits=document.createElement('p');limits.className='muted';limits.textContent='Item bonuses are capped: kill XP 60%, kill gold 45%, burn/poison damage 75%, effect duration 35%, slow strength 35%. Reward bonuses apply to your finishing blows; assists keep their ordinary XP.';details.append(limits);}
  if(item.passive){const passive=document.createElement('p');passive.textContent=`${item.passive.name}: ${item.passive.description}`;details.append(passive);}
  const note=document.createElement('p');note.className='muted';note.textContent='Shows this item at your current level. Other missing ingredients add their own bonuses. Active cooldowns and existing casts keep their original values.';details.append(note);
  return details;
}
$('shop-goal-next').addEventListener('click',()=>{const next=itemPlan(game.player,pinnedGoal).next;if(next)showShopTier(next.tier,next.id);});
$('shop-goal-clear').addEventListener('click',()=>{pinnedGoal=null;renderShop();refreshHUD();saveCurrent();});
$('shop-button').addEventListener('click',openShop);
$('close-shop').addEventListener('click',()=>closeDialog('shop-dialog'));
for(const button of document.querySelectorAll('[data-shop-tier]'))button.addEventListener('click',()=>showShopTier(Number(button.dataset.shopTier)));

function openTalents() {
  if(!game||ended)return;
  renderTalents();openDialog('talent-dialog');
}
function renderTalents() {
  const hero=game.player,definition=HEROES[hero.heroId];
  talentRenderKey=[hero.level,...Object.values(hero.talents)].join('|');
  $('talent-title').textContent=`${definition.name} · Skill tree`;
  const tree=$('talent-tree');tree.replaceChildren();
  definition.talents.forEach(tier=>{
    const chosen=hero.talents[tier.level];
    const unlocked=hero.level>=tier.level;
    const section=document.createElement('section');section.className=`talent-tier${unlocked?'':' locked'}`;
    const title=document.createElement('h3');title.textContent=`LEVEL ${tier.level}`;
    const state=document.createElement('span');state.textContent=chosen?'Path chosen':unlocked?'Choose your path':`Reach level ${tier.level}`;title.append(state);
    const choices=document.createElement('div');choices.className='talent-options';
    tier.choices.forEach(choice=>{
      const button=document.createElement('button');button.className=`talent-option${chosen===choice.id?' chosen':''}`;
      button.dataset.talentChoice=choice.id;button.dataset.tier=String(tier.level);button.disabled=!unlocked||!!chosen;
      const name=document.createElement('strong');name.textContent=choice.name;
      const heading=document.createElement('span');heading.className='talent-heading';
      const icon=document.createElement('canvas');icon.width=32;icon.height=32;icon.setAttribute('aria-hidden','true');
      heading.append(icon,name);
      const desc=document.createElement('small');desc.textContent=choice.description;
      button.append(heading,desc);drawIcon(icon,{...choice,color:choice.color||definition.color});
      button.addEventListener('click',()=>{
        if(onlineMatch){online.command({type:'talent',level:tier.level,id:choice.id});return;}
        if(game.chooseTalent(hero,tier.level,choice.id)){tone('level');renderTalents();refreshHUD();saveCurrent();}
      });
      choices.append(button);
    });section.append(title,choices);tree.append(section);
  });
}
$('talent-button').addEventListener('click',openTalents);
$('close-talents').addEventListener('click',()=>closeDialog('talent-dialog'));
function openMap(){if(!game||ended)return;renderer.drawMinimap($('tactical-map'),game);openDialog('map-dialog');}
$('map-button').addEventListener('click',event=>{if(ignoreMapClick){ignoreMapClick=false;event.preventDefault();return;}openMap();});
$('map-orders-button').addEventListener('click',openMap);
function peekAt(event){
  pointer.valid=false;
  const rect=$('minimap').getBoundingClientRect();
  renderer.peek={x:Math.max(0,Math.min(WORLD.width,(event.clientX-rect.left)/rect.width*WORLD.width)),y:Math.max(0,Math.min(WORLD.height,(event.clientY-rect.top)/rect.height*WORLD.height))};
  $('peek-hint').hidden=false;
}
$('map-button').addEventListener('pointerdown',event=>{
  if(!playing())return;event.preventDefault();$('map-button').setPointerCapture(event.pointerId);
  minimapDrag={id:event.pointerId,x:event.clientX,y:event.clientY,started:performance.now(),active:false};
});
$('map-button').addEventListener('pointermove',event=>{
  if(!minimapDrag||event.pointerId!==minimapDrag.id)return;
  if(Math.hypot(event.clientX-minimapDrag.x,event.clientY-minimapDrag.y)>5||performance.now()-minimapDrag.started>160){minimapDrag.active=true;peekAt(event);}
  minimapDrag.x=event.clientX;minimapDrag.y=event.clientY;
});
function releasePeek(event){
  if(!minimapDrag||event.pointerId!==minimapDrag.id)return;
  ignoreMapClick=minimapDrag.active;minimapDrag=null;renderer.peek=null;$('peek-hint').hidden=true;setTimeout(()=>{ignoreMapClick=false;},350);
}
$('map-button').addEventListener('pointerup',releasePeek);$('map-button').addEventListener('pointercancel',releasePeek);
function refreshBeastChoice(){
  const pending=game.pendingBeastChoice,visible=pending&&pending.team===game.player.team&&pending.expiresAt>game.time;
  $('beast-choice').hidden=!visible;if(!visible)return;
  $('beast-choice-label').textContent=`BEAST LANE · ${Math.ceil(pending.expiresAt-game.time)}s`;
  for(const button of document.querySelectorAll('[data-beast-lane]'))button.setAttribute('aria-pressed',String(Number(button.dataset.beastLane)===pending.lane));
}
for(const button of document.querySelectorAll('[data-beast-lane]'))button.addEventListener('click',()=>{
  if(onlineMatch){online.command({type:'beastLane',lane:Number(button.dataset.beastLane),beastId:game.pendingBeastChoice?.beastId});return;}
  if(game?.chooseBeastLane(Number(button.dataset.beastLane))){notify(`Your beast marches ${WORLD.lanes[Number(button.dataset.beastLane)].name}.`);refreshHUD();saveCurrent();}
});
$('close-map').addEventListener('click',()=>closeDialog('map-dialog'));
for(const button of document.querySelectorAll('[data-command]'))button.addEventListener('click',()=>{
  const lane=Number($('order-lane').value),command=button.dataset.command;
  order({type:'ping',order:command,lane},()=>game.command(command,lane));
  closeDialog('map-dialog');notify(button.dataset.command==='beast'?'Called a river beast hunt.':`Called ${button.dataset.command} on ${$('order-lane').selectedOptions[0].textContent}.`,2500);
});

function refreshAbilityHUD() {
  if(!game)return;
  const hero=game.player;
  abilities.forEach(({button,cooldown,ability},slot)=>{
    const remaining=hero.cooldowns[slot]||0,locked=hero.level<ability.unlock,ready=!locked&&remaining<=0&&hero.mana>=ability.mana&&hero.alive;
    button.classList.toggle('locked',locked);button.classList.toggle('cooling',!locked&&remaining>0);button.classList.toggle('ready',ready);button.classList.toggle('selected',selectedSlot===slot);
    button.setAttribute('aria-disabled',String(!ready));
    cooldown.textContent=locked?`LV ${ability.unlock}`:remaining>=1?String(Math.ceil(remaining)):remaining>0?remaining.toFixed(1):'';
    const evolution=HEROES[hero.heroId].talents.flatMap(tier=>tier.choices.filter(choice=>choice.id===hero.talents[tier.level]&&choice.evolution?.slot===slot));
    button.title=[ability.description,...evolution.map(choice=>choice.description)].join('\n');
    button.setAttribute('aria-label',`${ability.name}. ${locked?`Unlocks at level ${ability.unlock}`:remaining>0?`${Math.ceil(remaining)} second cooldown`:hero.mana<ability.mana?'Not enough mana':'Ready'}. ${button.title}`);
    if(ability.id==='polarity-pulse'||ability.id==='grand-finale'){
      const suffix=ability.id==='polarity-pulse'?(hero.resonance?.polarity==='pull'?'PUSH':'PULL'):['SHATTER','SHELTER','DANCE'][resonanceResource(hero,game.time)?.last||0];
      button.querySelector('.ability-name').textContent=ability.id==='polarity-pulse'?`Pulse · ${suffix}`:`Finale · ${suffix}`;
      button.setAttribute('aria-label',button.getAttribute('aria-label')+` Next cast: ${suffix.toLowerCase()}.`);
    }
  });
}
function refreshHUD() {
  if(!game)return;
  const hero=game.player;
  const resource=resonanceResource(hero,game.time),heroName=HEROES[hero.heroId].name;
  $('hero-name').textContent=resource?`${heroName} · ${resource.short}`:heroName;
  $('hero-name').title=resource?.detail||heroName;
  const percent=(v,max)=>`${Math.max(0,Math.min(100,v/(max||1)*100))}%`;
  $('health-fill').style.width=percent(hero.hp,hero.maxHp);$('health-text').textContent=`${Math.max(0,Math.ceil(hero.hp))} / ${Math.ceil(hero.maxHp)}`;
  $('mana-fill').style.width=percent(hero.mana,hero.maxMana);$('mana-text').textContent=`${Math.floor(hero.mana)} / ${Math.floor(hero.maxMana)}`;
  const maxLevel=hero.level>=RULES.maxLevel;
  $('xp-fill').style.width=maxLevel?'100%':percent(hero.xp,hero.xpNext);
  $('xp-fill').parentElement.title=maxLevel?`Level ${RULES.maxLevel} · MAX LEVEL`:`Level ${hero.level} · ${Math.floor(hero.xp)} / ${hero.xpNext} XP · Personal kills and assists`;
  $('hero-level').title=maxLevel?'Maximum level reached':`Level ${hero.level} of ${RULES.maxLevel}`;
  $('hero-level').textContent=String(hero.level);$('kda').textContent=`${hero.kills||0} / ${hero.deaths||0} / ${hero.assists||0}`;
  const gold=Math.floor(hero.gold||0);$('shop-gold').textContent=String(gold);
  const plan=itemPlan(hero,pinnedGoal);$('shop-ready').hidden=!plan.affordable;$('shop-button').classList.toggle('goal-ready',plan.affordable);
  $('shop-ready').title=plan.next?`${plan.next.name} is affordable`:'';
  $('shop-button').setAttribute('aria-label',`Open item shop, ${gold} gold`);
  $('shop-button').title=`Item shop (I) · ${gold} gold · Final hits earn gold`;
  const goldGain=hero.lastGoldGain,showGold=goldGain&&game.time-goldGain.time>=0&&game.time-goldGain.time<2;
  $('gold-earned').hidden=!showGold;
  if(showGold){$('gold-earned').textContent=`+${goldGain.amount} GOLD`;if(goldGain.time!==lastGoldSound){lastGoldSound=goldGain.time;tone('gold');rewardPulse($('shop-button'),'gold-pulse');}}
  if(game.combatCue&&game.combatCue!==lastCombatCue){lastCombatCue=game.combatCue;tone(game.combatCue.kind);}
  else if((hero.shield||0)<lastShield-5&&game.time-(hero.lastDamageAt??-100)<.2)tone('block');
  lastShield=hero.shield||0;
  if(pendingPurchase){
    const item=ITEM_BY_ID[pendingPurchase.id];
    if(hero.items?.includes(item.id)){
      tone('level');
      $('shop-message').textContent=item.name+' is active. '+describeBonuses(item)+'.';
      pendingPurchase=null;
    }else if(performance.now()>=pendingPurchase.deadline){
      $('shop-message').textContent=purchaseState(hero,item.id).reason||'Purchase could not be confirmed. Try again.';
      pendingPurchase=null;
    }
  }
  if($('shop-dialog').open&&shopRenderKey!==shopStateKey(hero))renderShop();
  if(onlineMatch&&$('talent-dialog').open&&talentRenderKey!==[hero.level,...Object.values(hero.talents)].join('|'))renderTalents();
  $('blue-score').textContent=String(game.stats?.kills?.[0]||0);$('red-score').textContent=String(game.stats?.kills?.[1]||0);$('match-clock').textContent=formatTime(game.time);
  const cores=game.units.filter(unit=>unit.kind==='core');
  for(const core of cores){
    const bar=$(core.team===0?'ally-core':'enemy-core');
    bar.style.setProperty('--hp',percent(core.hp,core.maxHp));
    bar.title=core.team===hero.team?'Your crystal':'Enemy crystal';
  }
  const tiers=game.availableTalents(hero),points=tiers.length;
  $('talent-count').hidden=points===0;$('talent-count').textContent=String(points);$('talent-button').classList.toggle('available',points>0);
  $('talent-button').setAttribute('aria-label',points?`Skill tree: ${points} talent choices available`:'View skill tree');
  const river=game.units.filter(unit=>unit.alive&&unit.kind==='beast'&&unit.team===-1);
  $('objective-status').textContent=river.length?`✦ RIVER BEAST${river.length>1?'S':''} · TIER ${Math.max(...river.map(b=>b.tier||b.level||1))}`:`✦ Beasts in ${formatTime(game.beastIn)} · Wave ${Math.ceil(game.waveIn||0)}s`;
  const attacking=!!game.attackOrder&&hero.alive;
  const target=attacking&&game.units.find(unit=>unit.alive&&unit.id===hero.targetId);
  $('attack-button').classList.toggle('held',attacking||touchAttack);
  $('attack-button').setAttribute('aria-pressed',String(attacking));
  $('attack-button').setAttribute('aria-label',target?'Attacking automatically. Press to select the nearest nearby enemy.':attacking?'Ready to attack. No nearby enemy.':'Attack the nearest nearby enemy');
  $('attack-button').querySelector('small').textContent=target?'ACTIVE':attacking?'READY':'ATTACK';
  $('target-status').hidden=!attacking;
  $('target-status').textContent=target?`ATTACKING · ${target.name||({footman:'Footman',archer:'Archer',summon:'Summon'}[target.kind])||'Enemy'}`:'ATTACK · No nearby enemy';
  $('respawn-status').hidden=hero.alive;
  if(!hero.alive){$('respawn-status').replaceChildren();const headline=document.createElement('span');headline.textContent=`Return in ${Math.ceil(hero.respawnIn||0)}s`;const caption=document.createElement('small');caption.textContent='Your allies are still fighting.';$('respawn-status').append(headline,caption);}
  if(hero.level>lastLevel){
    tone('level');rewardPulse($('hero-level'),'level-pulse');
    for(const {button,ability} of abilities)if(ability.unlock>lastLevel&&ability.unlock<=hero.level)rewardPulse(button,'ability-unlocked');
    notify(`LEVEL ${hero.level}${points?' · Choose a new talent ✦':' · More power, new possibilities'}`,3400);lastLevel=hero.level;
  }
  if(hero.hp<lastHealth-28&&hero.alive)tone('hurt');lastHealth=hero.hp;
  const events=game.events||[];
  const latest=events[events.length-1];
  if(latest&&latest.id!==lastEventId){lastEventId=latest.id;if(latest.text)notify(latest.text,latest.type==='beast'?3500:2600);}
  refreshBeastChoice();
  refreshAbilityHUD();renderer.drawMinimap($('minimap'),game);
  if(game.winner!=null&&!ended)beginEndSequence();
}
function beginEndSequence(now=performance.now()) {
  if(!game||game.winner==null||ended||endSequence?.game===game)return;
  setPaused(true);
  const core=game.units.find(u=>u.kind==='core'&&!u.alive&&u.team!==game.winner);
  // Resolve persistence immediately, even if the tab closes during the finale.
  const reward=recordMatch(profile,game),persisted=writeProfile(storage,profile);
  clearSavedMatch(storage,game.matchId);
  endSequence={game,startedAt:now,coreId:core?.id,focus:core?{x:core.x,y:core.y}:null,reward,persisted};
  document.querySelectorAll('dialog[open]').forEach(dialog=>dialog.close());
  document.body.classList.add('core-finale');
  if(core){renderer.peek=endSequence.focus;renderer.hasCamera=false;}
}
function endMatch() {
  ended=true;document.body.classList.remove('core-finale');setPaused(true);cancelAim();clearSavedMatch(storage,game.matchId);
  const won=game.winner===game.player.team,hero=game.player,m=hero.metrics||{},variant=hero.cosmetics?.victory||'classic';
  $('end-dialog').dataset.victory=variant;
  $('end-title').textContent=won?'Victory!':'Defeat';
  $('end-crest').textContent=({classic:won?'✦':'◇',verdant:'❧',astral:'✧',sovereign:'♛'})[variant]||'✦';
  $('end-description').textContent=won?'Their crystal is shattered. The Wild Rift belongs to your team.':'Your crystal has fallen. A new strategy awaits in the next rift.';
  $('end-stats').replaceChildren();
  for(const [value,label] of [[`${hero.kills||0}/${hero.deaths||0}/${hero.assists||0}`,'KILLS / DEATHS / ASSISTS'],[hero.level,'HERO LEVEL'],[formatTime(game.time),'MATCH TIME'],[Math.round(m.damageDealt||0),'TOTAL DAMAGE'],[Math.round(m.damageToHeroes||0),'HERO DAMAGE'],[Math.round(m.damageToStructures||0),'STRUCTURE DAMAGE'],[Math.round(m.healingDone||0),'HEALING'],[Math.round(m.shieldingDone||0),'SHIELDING'],[Math.round(m.goldEarned??hero.goldEarned??0),'GOLD EARNED'],[m.lastHits||0,'FINAL HITS'],[m.minionLastHits||0,'MINION FINAL HITS'],[m.jungleLastHits||0,'JUNGLE FINAL HITS'],[m.towersDestroyed||0,'TOWERS'],[m.beastsCaptured||0,'BEASTS CAPTURED'],[m.bestStreak||0,'BEST STREAK'],[m.shutdowns||0,'SHUTDOWNS']]){
    const block=document.createElement('div'),strong=document.createElement('strong'),small=document.createElement('small');strong.textContent=String(value);small.textContent=label;block.append(strong,small);$('end-stats').append(block);
  }
  $('end-badges').replaceChildren();for(const text of matchBadges(hero)){const badge=document.createElement('span');badge.textContent=text;$('end-badges').append(badge);}
  const reward=endSequence?.game===game?endSequence.reward:recordMatch(profile,game),persisted=endSequence?.game===game?endSequence.persisted:writeProfile(storage,profile);
  $('end-mastery').textContent=`+${reward.earned} ${HEROES[hero.heroId].name} mastery XP · ${profile.heroes[hero.heroId].xp} total${reward.unlocks.length?` · Unlocked ${reward.unlocks.join(', ')}`:''}${persisted?'':' · Available this session; browser storage is unavailable.'}`;
  $('end-dialog').showModal();if(won)tone('win');
}
$('play-again').addEventListener('click',()=>{
  if(!onlineMatch){startMatch();return;}
  exitOnline();
  online.returnToLobby();
});
$('change-hero').addEventListener('click',()=>{
  if(onlineMatch){online.leave();return;}
  game=null;endSequence=null;clearInput();$('hud').hidden=true;$('menu-screen').hidden=false;$('end-dialog').close();drawMenu();refreshSavedMenu();delete document.documentElement.dataset.catnmicePlaying;
});
$('end-dialog').addEventListener('cancel',event=>event.preventDefault());
function input() {
  let x=Number(keys.has('KeyD')||keys.has('ArrowRight'))-Number(keys.has('KeyA')||keys.has('ArrowLeft'))+joystick.x;
  let y=Number(keys.has('KeyS')||keys.has('ArrowDown'))-Number(keys.has('KeyW')||keys.has('ArrowUp'))+joystick.y;
  const length=Math.hypot(x,y);if(length>1){x/=length;y/=length;}
  if(moveTo&&Math.hypot(game.player.x-moveTo.x,game.player.y-moveTo.y)<12)moveTo=null;
  // Attack is a persistent command issued on press; holding a control must not
  // reselect targets or resurrect an order canceled by movement or a spell.
  return {move:{x,y},aim,...(moveTo?{moveTo}: {})};
}
function sendOnlineMove(now,force=false) {
  if(!game||game.winner!=null)return;
  const {move:{x,y}}=input();
  const signature=x.toFixed(2)+'|'+y.toFixed(2);
  if(!force&&signature===lastNetworkInput&&((!x&&!y)||now-lastMoveSent<100))return;
  if(!force&&(x||y)&&now-lastMoveSent<40)return;
  if(online.command({type:'move',x,y,aim})){lastNetworkInput=signature;lastMoveSent=now;}
}
function battleView(now=performance.now()) {
  const view={aim,selectedSlot,cameraAim:selectedSlot>=0?aim:null};
  if(endSequence?.game===game)view.presentationTime=game.time+Math.min(finaleSeconds,Math.max(0,(now-endSequence.startedAt)/1000));
  if(game?.effects.some(e=>e.type==='reward-gold'&&e.ownerId===game.playerId)){
    const rect=$('shop-gold').getBoundingClientRect();view.rewardTarget={x:rect.left+rect.width/2,y:rect.top+rect.height/2};
  }
  return view;
}
function resize(){applyControls();renderer.resize(innerWidth,innerHeight);if(game)renderer.draw(game,battleView());}
window.addEventListener('resize',resize);resize();
function frame(now) {
  animationFrame=requestAnimationFrame(frame);
  const delta=Math.min(.1,Math.max(0,(now-lastFrame)/1000));lastFrame=now;
  if(!game)return;
  if(minimapDrag&&!minimapDrag.active&&now-minimapDrag.started>=160){minimapDrag.active=true;peekAt({clientX:minimapDrag.x,clientY:minimapDrag.y});}
  if(lastMotionGame!==game){lastMotionGame=game;previousMotion=null;accumulator=0;lastCombatCue=null;}
  if(onlineMatch){
    if(playing()){
      if(pointer.valid&&!castDrag)aim=renderer.screenToWorld(pointer.x,pointer.y);
      sendOnlineMove(now);
    }
  }else if(playing()) {
    accumulator=Math.min(.15,accumulator+delta);
    if(pointer.valid&&!castDrag)aim=renderer.screenToWorld(pointer.x,pointer.y);
    while(accumulator>=1/30){
      previousMotion=captureMotion(game);
      game.update(1/30,input());accumulator-=1/30;if(game.winner!=null)break;
    }
  }else{previousMotion=null;accumulator=0;}
  if(game.winner!=null&&!ended)beginEndSequence(now);
  const interpolation=onlineMatch?Math.min(1,(now-snapshotAt)/snapshotSpan):accumulator*30;
  renderer.draw(renderMotion(game,previousMotion,interpolation),{...battleView(now),frameDelta:delta});
  if(endSequence?.game===game&&!ended&&now-endSequence.startedAt>=finaleSeconds*1000)endMatch();
  if(!ended){saveTimer+=delta;if(saveTimer>=10)saveCurrent();}
  hudTimer+=delta;if(hudTimer>.09){hudTimer=0;refreshHUD();}
  if(announcementUntil&&now>announcementUntil){$('announcement').classList.remove('visible');announcementUntil=0;}
}
requestAnimationFrame(frame);
online.launch();
window.addEventListener('pagehide',()=>{if(onlineMatch)clearInput();saveCurrent();cancelAnimationFrame(animationFrame);audioContext?.suspend().catch(()=>{});});
window.addEventListener('pageshow',event=>{if(event.persisted){lastFrame=performance.now();animationFrame=requestAnimationFrame(frame);if(game)openPause();}});

