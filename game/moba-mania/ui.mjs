import { HEROES, DIFFICULTIES, WORLD, RULES } from './data.mjs';
import { Game } from './engine.mjs';
import { Renderer, drawPortrait, drawIcon } from './renderer.mjs';
import { ITEMS, ITEM_BY_ID, purchaseState, describeBonuses, drawItemIcon } from './items.mjs';

const $ = id => document.getElementById(id);
const canvas = $('battlefield');
const keys = new Set();
const hotkeys = ['KeyQ','KeyE','KeyR','KeyF'];
const preferenceKey = 'moba-mania.preferences.v2';
let saved = {};
try { saved = JSON.parse(localStorage.getItem(preferenceKey) || '{}') || {}; } catch { /* Storage may be unavailable in private tabs. */ }
let selectedHero = HEROES[saved.hero] ? saved.hero : 'arc';
let soundEnabled = saved.sound !== false;
let game = null;
let renderer = new Renderer(canvas);
let selectedSlot = -1;
let aim = { x: WORLD.width / 2, y: WORLD.height / 2 };
let moveTo = null;
let touchAttack = false;
let joystick = { x:0, y:0, pointerId:null };
let castDrag = null;
let pointer = { x:innerWidth / 2, y:innerHeight / 2, valid:false };
let lastFrame = 0, accumulator = 0, hudTimer = 0;
let lastEventId = null, lastLevel = 1, lastHealth = 1;
let announcementUntil = 0, ended = false;
let helpReturn = null;
let audioContext = null, lastToneAt = 0;
let shopTier = 1, shopRenderKey = '';
const abilities = [];
let animationFrame = 0;

function savePreferences() {
  try { localStorage.setItem(preferenceKey, JSON.stringify({hero:selectedHero, difficulty:$('difficulty').value, sound:soundEnabled})); } catch { /* The game does not require storage. */ }
}
function formatTime(seconds) {
  const value = Math.max(0, Math.floor(seconds || 0));
  return `${Math.floor(value / 60)}:${String(value % 60).padStart(2,'0')}`;
}
function setTouch() { document.body.classList.add('touch'); }
if (matchMedia('(pointer:coarse)').matches || navigator.maxTouchPoints > 0) setTouch();
window.addEventListener('pointerdown', event => { if (event.pointerType === 'touch') setTouch(); }, {passive:true});

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
  const notes = kind === 'level' ? [523,659,784] : kind === 'win' ? [392,523,659,784] : kind === 'hurt' ? [110] : kind === 'error' ? [164] : [330 + slot*85, 495 + slot*65];
  notes.forEach((frequency,index)=>{
    const osc = audioContext.createOscillator();
    const gain = audioContext.createGain();
    osc.type = kind === 'hurt' ? 'triangle' : 'square';
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
function drawMenu() {
  $('hero-grid').replaceChildren();
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
    $('hero-grid').append(button);
    drawPortrait(portrait,hero.id,96);
  });
  const hero = HEROES[selectedHero];
  const detail = $('hero-detail'); detail.replaceChildren();
  const info=document.createElement('div');
  const title=document.createElement('h3');title.textContent=hero.title;
  const desc=document.createElement('p');desc.textContent=hero.description;
  info.append(title,desc);
  const skills=document.createElement('div');skills.className='hero-skills';
  hero.abilities.forEach(ability=>{
    const item=document.createElement('div');item.className='hero-skill';item.title=`Level ${ability.unlock}: ${ability.description}`;
    const icon=document.createElement('canvas');icon.width=32;icon.height=32;icon.setAttribute('aria-hidden','true');
    const label=document.createElement('span');label.textContent=ability.name;
    item.append(icon,label);skills.append(item);drawIcon(icon,ability);
  });
  detail.append(info,skills);
}
Object.values(DIFFICULTIES).forEach(difficulty=>{
  const option=document.createElement('option');option.value=difficulty.id;option.textContent=difficulty.name;$('difficulty').append(option);
});
$('difficulty').value=DIFFICULTIES[saved.difficulty] ? saved.difficulty : 'normal';
function updateDifficulty() { $('difficulty-description').textContent=DIFFICULTIES[$('difficulty').value].description;savePreferences(); }
$('difficulty').addEventListener('change',updateDifficulty);
updateDifficulty();
drawMenu();

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
function playing() { return !!game && !game.paused && game.winner==null && !$('hud').hidden; }
function clearInput() {
  keys.clear();touchAttack=false;moveTo=null;castDrag=null;
  if(game)game.cancelPlayerOrder();
  joystick.x=joystick.y=0;joystick.pointerId=null;
  $('joystick-knob').style.transform='translate(0px,0px)';
  $('attack-button').classList.remove('held');
  cancelAim();
}
function startMatch() {
  document.querySelectorAll('dialog[open]').forEach(dialog=>dialog.close());
  clearInput();
  selectedHero=HEROES[selectedHero] ? selectedHero : 'arc';
  game=new Game({hero:selectedHero,difficulty:$('difficulty').value,seed:Math.floor(Math.random()*0x7fffffff)});
  ended=false;lastEventId=null;lastLevel=1;lastHealth=game.player.hp;accumulator=0;hudTimer=0;lastFrame=performance.now();
  shopTier=1;shopRenderKey='';$('shop-owned').open=false;
  $('menu-screen').hidden=true;$('hud').hidden=false;
  $('hero-name').textContent=HEROES[selectedHero].name;
  drawPortrait($('hero-portrait'),selectedHero,48);
  aim={x:game.player.x+300,y:game.player.y};pointer.valid=false;
  buildAbilities();renderer.hasCamera=false;renderer.lastTime=null;renderer.resize(innerWidth,innerHeight);
  refreshHUD();savePreferences();unlockAudio();
  notify('Stay with your wave. The river beasts awaken soon.',4200);
  document.documentElement.dataset.catnmicePlaying='true';
  window.dispatchEvent(new Event('catnmice-game-started'));
  canvas.focus({preventScroll:true});
}
$('start-match').addEventListener('click',startMatch);

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
  if(game.cast(game.player,slot,aim)) { tone('cast',slot); moveTo=null; }
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
  // Sprites rise above their ground anchor. Include heads and tower crystals,
  // then add a screen-space touch margin that stays useful at every zoom.
  const zoom=canvas.getBoundingClientRect().width/renderer.camera.width;
  const padding=pointerType==='touch'?14:7;
  return game.units.filter(unit=>unit.alive&&unit.team!==game.player.team)
    .map(unit=>{
      const [width,height,lift]=({hero:[22,35,31],tower:[32,55,48],core:[64,80,42],footman:[17,23,17],archer:[17,23,17]})[unit.kind]
        ||[Math.max(22,unit.r*1.35),Math.max(26,unit.r*1.5),Math.max(20,unit.r*1.2)];
      const point=renderer.worldToScreen(unit.x,unit.y-lift);
      const distance=Math.hypot((clientX-point.x)/(width*zoom+padding),(clientY-point.y)/(height*zoom+padding));
      return {unit,distance};
    }).filter(hit=>hit.distance<=1)
    .sort((a,b)=>a.distance-b.distance||a.unit.id-b.unit.id)[0]?.unit;
}
function attackNearest() {
  if(!playing()||!game.player.alive)return;
  moveTo=null;
  game.attackNearest();
  refreshHUD();
}
function movePlayerTo(point) {
  moveTo=null;
  game.movePlayerTo(point);
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
      game.cancelPlayerOrder();
      notify(target.kind==='core'?'Break an inner tower to expose this crystal.':"Destroy this lane's outer tower first.");
    }else game.attackTarget(target.id);
    refreshHUD();
  }
  else movePlayerTo({...aim});
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
  if(['KeyW','KeyA','KeyS','KeyD','ArrowUp','ArrowDown','ArrowLeft','ArrowRight'].includes(event.code)){moveTo=null;game.cancelPlayerOrder();}
});
window.addEventListener('keyup',event=>{
  keys.delete(event.code);
  const slot=hotkeys.indexOf(event.code);
  if(slot>=0 && selectedSlot===slot && playing()){event.preventDefault();castSelected();}
});
window.addEventListener('blur',()=>{if(playing())openPause();else clearInput();});
document.addEventListener('visibilitychange',()=>{if(document.hidden&&playing())openPause();});

const stick=$('joystick');
function updateStick(event) {
  const rect=stick.getBoundingClientRect(),radius=rect.width*.34;
  const dx=event.clientX-rect.left-rect.width/2,dy=event.clientY-rect.top-rect.height/2,distance=Math.hypot(dx,dy),scale=Math.min(1,distance/radius);
  joystick.x=distance>5?dx/(distance||1)*scale:0;joystick.y=distance>5?dy/(distance||1)*scale:0;
  $('joystick-knob').style.transform=`translate(${joystick.x*radius}px,${joystick.y*radius}px)`;
  moveTo=null;
  if(Math.hypot(joystick.x,joystick.y)>.05)game.cancelPlayerOrder();
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
function recall(){if(playing()&&game.recall(game.player)){clearInput();notify('Recalling… stay still for 4 seconds.',2500);}else if(game?.player.alive)notify('Recall needs a safe moment.');}
$('recall-button').addEventListener('click',recall);

function setPaused(paused) {
  if(!game)return;
  game.paused=paused;
  clearInput();accumulator=0;lastFrame=performance.now();
}
function openDialog(id) {
  setPaused(true);
  const dialog=$(id);if(!dialog.open)dialog.showModal();
}
function closeDialog(id) { $(id).close(); }
function afterDialogClosed() {
  if(game && !ended && !$('hud').hidden && !document.querySelector('dialog[open]')){setPaused(false);canvas.focus({preventScroll:true});}
}
for(const dialog of document.querySelectorAll('dialog'))dialog.addEventListener('close',afterDialogClosed);
function openPause(){if(game&&!ended&&!$('hud').hidden)openDialog('pause-dialog');}
$('pause-button').addEventListener('click',openPause);
$('resume-match').addEventListener('click',()=>closeDialog('pause-dialog'));
$('quit-match').addEventListener('click',()=>{
  $('hud').hidden=true;$('menu-screen').hidden=false;game=null;clearInput();
  document.querySelectorAll('dialog[open]').forEach(dialog=>dialog.close());drawMenu();
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

function shopStateKey(hero) {
  return [hero.gold,(hero.items||[]).join(','),hero.level,Object.values(hero.talents).join(',')].join('|');
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
  document.querySelectorAll('[data-shop-tier]').forEach(button=>button.setAttribute('aria-pressed',String(Number(button.dataset.shopTier)===shopTier)));
  const list=$('shop-items'),scrollTop=list.scrollTop;list.replaceChildren();list.setAttribute('aria-label',`Tier ${shopTier} items`);
  for(const item of ITEMS.filter(item=>item.tier===shopTier)){
    const state=purchaseState(hero,item.id),isOwned=owned.has(item.id);
    const card=document.createElement('article');card.className=`shop-item ${isOwned?'owned':state.ok?'ready':state.missing.length?'locked':'short'}`;
    card.dataset.itemId=item.id;card.dataset.itemState=isOwned?'owned':state.ok?'ready':state.missing.length?'locked':'short';card.tabIndex=-1;
    card.style.setProperty('--item-color',item.color);
    const heading=document.createElement('div');heading.className='shop-item-heading';
    const title=document.createElement('h3');title.textContent=item.name;
    const price=document.createElement('span');price.className='shop-price';price.textContent=`${item.cost} gold`;
    const titleBlock=document.createElement('div');titleBlock.append(title,price);heading.append(itemIcon(item),titleBlock);
    const bonuses=document.createElement('p');bonuses.className='shop-bonuses';bonuses.textContent=describeBonuses(item);
    const description=document.createElement('p');description.className='shop-description';description.textContent=item.description;
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
      if(!game.buyItem(hero,item.id)){$('shop-message').textContent=purchaseState(hero,item.id).reason;renderShop();return;}
      tone('level');$('shop-message').textContent=`${item.name} is active. ${describeBonuses(item)}.`;
      renderShop();refreshHUD();
      // Keep keyboard focus near the purchased item after its button becomes disabled.
      const purchased=[...$('shop-items').children].find(el=>el.dataset.itemId===item.id);
      purchased?.scrollIntoView({block:'nearest',inline:'nearest'});purchased?.focus({preventScroll:true});
    });
    card.append(heading,bonuses,description,requirements,action);list.append(card);
  }
  list.scrollTop=scrollTop;
  $('shop-owned-count').textContent=`${owned.size} / ${ITEMS.length}`;
  const inventory=$('shop-inventory');inventory.replaceChildren();
  for(const item of ITEMS.filter(item=>owned.has(item.id))){
    const badge=document.createElement('div');badge.className='shop-owned-item';badge.dataset.ownedItem=item.id;
    badge.title=`${item.name} · ${describeBonuses(item)}`;
    const name=document.createElement('span');name.textContent=item.name;badge.append(itemIcon(item,28),name);inventory.append(badge);
  }
  if(!owned.size){const empty=document.createElement('p');empty.textContent='Your purchased items will appear here. All of them stay active.';inventory.append(empty);}
}
$('shop-button').addEventListener('click',openShop);
$('close-shop').addEventListener('click',()=>closeDialog('shop-dialog'));
for(const button of document.querySelectorAll('[data-shop-tier]'))button.addEventListener('click',()=>showShopTier(Number(button.dataset.shopTier)));

function openTalents() {
  if(!game||ended)return;
  renderTalents();openDialog('talent-dialog');
}
function renderTalents() {
  const hero=game.player,definition=HEROES[hero.heroId];
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
      button.addEventListener('click',()=>{if(game.chooseTalent(hero,tier.level,choice.id)){tone('level');renderTalents();refreshHUD();}});
      choices.append(button);
    });section.append(title,choices);tree.append(section);
  });
}
$('talent-button').addEventListener('click',openTalents);
$('close-talents').addEventListener('click',()=>closeDialog('talent-dialog'));
function openMap(){if(!game||ended)return;renderer.drawMinimap($('tactical-map'),game);openDialog('map-dialog');}
$('map-button').addEventListener('click',openMap);
$('close-map').addEventListener('click',()=>closeDialog('map-dialog'));
for(const button of document.querySelectorAll('[data-command]'))button.addEventListener('click',()=>{
  game.command(button.dataset.command,Number($('order-lane').value));
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
    button.setAttribute('aria-label',`${ability.name}. ${locked?`Unlocks at level ${ability.unlock}`:remaining>0?`${Math.ceil(remaining)} second cooldown`:hero.mana<ability.mana?'Not enough mana':'Ready'}. ${ability.description}`);
  });
}
function refreshHUD() {
  if(!game)return;
  const hero=game.player;
  const percent=(v,max)=>`${Math.max(0,Math.min(100,v/(max||1)*100))}%`;
  $('health-fill').style.width=percent(hero.hp,hero.maxHp);$('health-text').textContent=`${Math.max(0,Math.ceil(hero.hp))} / ${Math.ceil(hero.maxHp)}`;
  $('mana-fill').style.width=percent(hero.mana,hero.maxMana);$('mana-text').textContent=`${Math.floor(hero.mana)} / ${Math.floor(hero.maxMana)}`;
  const maxLevel=hero.level>=RULES.maxLevel;
  $('xp-fill').style.width=maxLevel?'100%':percent(hero.xp,hero.xpNext);
  $('xp-fill').parentElement.title=maxLevel?`Level ${RULES.maxLevel} · MAX LEVEL`:`Level ${hero.level} · ${Math.floor(hero.xp)} / ${hero.xpNext} XP · Personal kills and assists`;
  $('hero-level').title=maxLevel?'Maximum level reached':`Level ${hero.level} of ${RULES.maxLevel}`;
  $('hero-level').textContent=String(hero.level);$('kda').textContent=`${hero.kills||0} / ${hero.deaths||0} / ${hero.assists||0}`;
  const gold=Math.floor(hero.gold||0);$('shop-gold').textContent=String(gold);
  $('shop-button').setAttribute('aria-label',`Open item shop, ${gold} gold`);
  $('shop-button').title=`Item shop (I) · ${gold} gold · Final hits earn gold`;
  const goldGain=hero.lastGoldGain,showGold=goldGain&&game.time-goldGain.time>=0&&game.time-goldGain.time<2;
  $('gold-earned').hidden=!showGold;
  if(showGold)$('gold-earned').textContent=`+${goldGain.amount} GOLD`;
  if($('shop-dialog').open&&shopRenderKey!==shopStateKey(hero))renderShop();
  $('blue-score').textContent=String(game.stats?.kills?.[0]||0);$('red-score').textContent=String(game.stats?.kills?.[1]||0);$('match-clock').textContent=formatTime(game.time);
  const cores=game.units.filter(unit=>unit.kind==='core');
  for(const core of cores)$(core.team===0?'ally-core':'enemy-core').style.setProperty('--hp',percent(core.hp,core.maxHp));
  const tiers=game.availableTalents(hero),points=tiers.length;
  $('talent-count').hidden=points===0;$('talent-count').textContent=String(points);$('talent-button').classList.toggle('available',points>0);
  $('talent-button').setAttribute('aria-label',points?`Skill tree: ${points} talent choices available`:'View skill tree');
  const river=game.units.filter(unit=>unit.alive&&unit.kind==='beast'&&unit.team===-1);
  $('objective-status').textContent=river.length?`✦ RIVER BEAST${river.length>1?'S':''} · TIER ${Math.max(...river.map(b=>b.tier||b.level||1))}`:`✦ Beasts in ${formatTime(game.beastIn)} · Wave ${Math.ceil(game.waveIn||0)}s`;
  const attacking=!!game.attackOrder&&hero.alive;
  const target=attacking&&game.units.find(unit=>unit.alive&&unit.id===hero.targetId);
  $('attack-button').classList.toggle('held',attacking||touchAttack);
  $('attack-button').setAttribute('aria-pressed',String(attacking));
  $('attack-button').setAttribute('aria-label',attacking?'Attacking automatically. Press to select the nearest enemy.':'Attack the nearest enemy');
  $('attack-button').querySelector('small').textContent=attacking?'ACTIVE':'ATTACK';
  $('target-status').hidden=!attacking;
  $('target-status').textContent=target?`ATTACKING · ${target.name||({footman:'Footman',archer:'Archer',summon:'Summon'}[target.kind])||'Enemy'}`:'ATTACK · Looking for an enemy';
  $('respawn-status').hidden=hero.alive;
  if(!hero.alive){$('respawn-status').replaceChildren();const headline=document.createElement('span');headline.textContent=`Return in ${Math.ceil(hero.respawnIn||0)}s`;const caption=document.createElement('small');caption.textContent='Your allies are still fighting.';$('respawn-status').append(headline,caption);}
  if(hero.level>lastLevel){tone('level');notify(`LEVEL ${hero.level}${points?' · Choose a new talent ✦':' · More power, new possibilities'}`,3400);lastLevel=hero.level;}
  if(hero.hp<lastHealth-28&&hero.alive)tone('hurt');lastHealth=hero.hp;
  const events=game.events||[];
  const latest=events[events.length-1];
  if(latest&&latest.id!==lastEventId){lastEventId=latest.id;if(latest.text)notify(latest.text,latest.type==='beast'?3500:2600);}
  refreshAbilityHUD();renderer.drawMinimap($('minimap'),game);
  if(game.winner!=null&&!ended)endMatch();
}
function endMatch() {
  ended=true;setPaused(true);cancelAim();
  const won=game.winner===0;
  $('end-title').textContent=won?'Victory!':'Defeat';
  $('end-crest').textContent=won?'✦':'◇';
  $('end-description').textContent=won?'Their crystal is shattered. The Wild Rift belongs to your team.':'Your crystal has fallen. A new strategy awaits in the next rift.';
  $('end-stats').replaceChildren();
  for(const [value,label] of [[`${game.player.kills||0} / ${game.player.deaths||0}`,'KILLS / DEATHS'],[game.player.level,'HERO LEVEL'],[formatTime(game.time),'MATCH TIME']]){
    const block=document.createElement('div'),strong=document.createElement('strong'),small=document.createElement('small');strong.textContent=String(value);small.textContent=label;block.append(strong,small);$('end-stats').append(block);
  }
  $('end-dialog').showModal();if(won)tone('win');
}
$('play-again').addEventListener('click',startMatch);
$('change-hero').addEventListener('click',()=>{
  game=null;clearInput();$('hud').hidden=true;$('menu-screen').hidden=false;$('end-dialog').close();drawMenu();delete document.documentElement.dataset.catnmicePlaying;
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
function resize(){renderer.resize(innerWidth,innerHeight);if(game)renderer.draw(game,{aim,selectedSlot});}
window.addEventListener('resize',resize);resize();
function frame(now) {
  animationFrame=requestAnimationFrame(frame);
  const delta=Math.min(.1,Math.max(0,(now-lastFrame)/1000));lastFrame=now;
  if(!game)return;
  if(playing()) {
    accumulator=Math.min(.15,accumulator+delta);
    if(pointer.valid&&!castDrag)aim=renderer.screenToWorld(pointer.x,pointer.y);
    while(accumulator>=1/30){game.update(1/30,input());accumulator-=1/30;if(game.winner!=null)break;}
  }
  renderer.draw(game,{aim,selectedSlot});
  hudTimer+=delta;if(hudTimer>.09){hudTimer=0;refreshHUD();}
  if(announcementUntil&&now>announcementUntil){$('announcement').classList.remove('visible');announcementUntil=0;}
}
requestAnimationFrame(frame);
window.addEventListener('pagehide',()=>{cancelAnimationFrame(animationFrame);audioContext?.suspend().catch(()=>{});});
window.addEventListener('pageshow',event=>{if(event.persisted){lastFrame=performance.now();animationFrame=requestAnimationFrame(frame);if(game)openPause();}});
