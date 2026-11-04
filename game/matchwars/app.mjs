import { Board, ELEMENTS } from './board.mjs?v=4';
import { Battle, TROOPS, ENEMIES, ELEMENT_INFO, UPGRADES, CHAMPION_AFFIXES, upgradeCost, elementMultiplier, difficultyForStage } from './combat.mjs?v=4';
import { loadProgress, saveProgress } from './progress.mjs?v=4';
import { STAGES, CHAPTERS, TOTAL_STAGES, getChapter, getStage } from './campaign.mjs?v=4';
import { CLASSES, CLASS_CHOICES, DEFAULT_LOADOUT, COMBOS, getTroop, normalizeLoadout, isClassUnlocked } from './classes.mjs?v=4';
import { SPECIALIZATIONS, masteryRank, masteryProgress, calculateMasteryAwards, applyMasteryAwards, selectSpecialization } from './mastery.mjs?v=4';
import { createDaily, seededRng, recordDailyResult } from './daily.mjs?v=4';
import { drawSpellEffect, drawCastingRune } from './vfx.mjs?v=3';

const $ = id => document.getElementById(id);
const boardEl = $('board'), canvas = $('battlefield'), ctx = canvas.getContext('2d');
const mobileScreen=matchMedia('(max-width:760px)');
const laneY=lane=>(220+lane*120)*canvas.height/530;
function fitBattlefield(){canvas.height=mobileScreen.matches?420:530;}
fitBattlefield();mobileScreen.addEventListener('change',fitBattlefield);
const backdrop = $('modal-backdrop'), modal = $('modal'), content = $('modal-content');
const atlases=Object.fromEntries(['troops','classes','enemies'].map(key=>{const img=new Image();img.src=new URL(`./assets/${key}.png`,import.meta.url).href;return [key,img];}));
const sprite=atlases.troops;
const colors = Object.fromEntries(ELEMENTS.map(k => [k, ELEMENT_INFO[k].color]));
const RELICS = {
  fury:{name:'Fury',symbol:'↑',color:'#ffac81',detail:'+30% attack'},
  aegis:{name:'Aegis',symbol:'◇',color:'#eadd9a',detail:'+30 shield'},
  haste:{name:'Haste',symbol:'ϟ',color:'#89dbe1',detail:'+25% speed / +20% attack rate'},
  vitality:{name:'Vitality',symbol:'+',color:'#a9d7a4',detail:'+35% health'},
};
const roman = number => {let rest=number,result='';for(const [n,s] of [[50,'L'],[40,'XL'],[10,'X'],[9,'IX'],[5,'V'],[4,'IV'],[1,'I']])while(rest>=n){result+=s;rest-=n;}return result;};
const difficultyName = stage => stage<=12?'Initiate':stage<=24?'Veteran':stage<=36?'Elite':stage<=48?'Mythic':stage<=60?'Ascendant':stage<=72?'Legendary':stage<=84?'Worldbreaker':stage<=108?'Eternal':'Cataclysm';
let progress = loadProgress(), board, battle, lane = 1, selected = null, busy = false;
let started = false, session = 0, lastFrame = 0, uiTimer = 0, resolved = false;
let charge = 0, overdrive = 0, matches = 0, bestChain = 1, summonCount = 0;
let lastFocus = null, modalKind = '', rootDialog = null, toastTimer, bannerTimer, hintTimer;
let particles = [], floats = [], motionTime = 0, soundContext, lastSound = 0, lastCastSound=0;
let pointer = null, suppressClick = false;
let dailyConfig = null, eventCount = 0;
const todayKey = () => new Date().toISOString().slice(0,10);
const tileNodes = new Map();
const reduced = () => progress.settings.reducedMotion || matchMedia('(prefers-reduced-motion: reduce)').matches;
const clamp = (v,a,b) => Math.max(a,Math.min(b,v));
const delay = ms => new Promise(resolve => setTimeout(resolve, reduced() ? Math.min(40,ms) : ms));
const escape = text => String(text).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const activeTroop=key=>battle?.troopFor?.(key)||getTroop(key,progress.loadout[key]);
const classPortrait=(def,css='portrait')=>`<canvas class="${css}" data-portrait="${def.key}" data-class="${def.id}" width="180" height="180"></canvas>`;
async function waitForPlay(token) {
  while(token===session && (!backdrop.hidden || document.hidden)) await new Promise(resolve=>setTimeout(resolve,100));
  return token===session;
}

function rune(key) {
  const paths = {
    ember:'<path d="M26 3c1 11 13 12 10 25-1 8-7 13-14 13C9 41 4 26 13 18c0 7 4 8 5 5 2-5 0-11 8-20Z"/><path d="M23 25c6 7 7 10 1 14-7-2-7-7-1-14Z" fill="#ffebbd"/>',
    tide:'<path d="M24 3C20 12 8 23 8 30a16 16 0 0 0 32 0C40 22 29 11 24 3Z"/><path d="M14 30c0 6 4 10 10 10" fill="none" stroke="#d8f5ff" stroke-width="3" stroke-linecap="round"/>',
    grove:'<path d="M40 6C18 6 5 14 9 29c3 12 21 13 27-1 3-7 2-14 4-22Z"/><path d="m8 42 22-23M19 31l-1-11m1 11 12-2" fill="none" stroke="#e0f7c6" stroke-width="2.5" stroke-linecap="round"/>',
    storm:'<path d="M28 3 9 28h13l-3 17 21-28H26l2-14Z"/><path d="m26 8-2 12h10L22 36l3-12H15Z" fill="#fff2bf"/>',
    sun:'<path d="M24 2 28 10 36 6 36 16 46 20 38 26 42 36 31 35 24 46 18 36 7 39 10 27 2 21 12 16 11 6 21 10Z"/><circle cx="24" cy="24" r="9" fill="#fff4d5"/>',
    void:'<path d="M35 4C18 2 5 14 8 28c4 17 26 21 36 6-16 5-30-14-9-30Z"/><path d="m35 15 2 6 6 2-6 2-2 6-2-6-6-2 6-2Z" fill="#efe0ff"/>',
  };
  return `<svg class="rune" viewBox="0 0 48 48" fill="currentColor" aria-hidden="true">${paths[key]}</svg>`;
}

function persist() {
  const okay = saveProgress(progress);
  $('save-status').textContent = okay ? 'Progress saved on this device' : 'Saving unavailable · keep this tab open';
  $('gold').textContent = Math.floor(progress.gold);
  $('forge-dot').hidden = !UPGRADES.some(u => (progress.upgrades[u.id] || 0) < u.max && progress.gold >= upgradeCost(u.id,progress.upgrades[u.id] || 0));
}

function toast(message) {
  clearTimeout(toastTimer); $('toast').textContent = message; $('toast').classList.add('show');
  toastTimer = setTimeout(() => $('toast').classList.remove('show'),2800);
}

function banner(message, duration=2600) {
  clearTimeout(bannerTimer); $('battle-banner').textContent = message; $('battle-banner').classList.add('show');
  bannerTimer = setTimeout(() => $('battle-banner').classList.remove('show'),duration);
}

function sound(kind='match', chain=1) {
  if (!progress.settings.sound) return;
  try {
    soundContext ||= new (window.AudioContext || window.webkitAudioContext)();
    if (soundContext.state === 'suspended') soundContext.resume();
    const now = soundContext.currentTime;
    if (kind === 'hit' && now-lastSound < .12) return;
    lastSound = now;
    const tones = kind === 'match' ? [262,330,392].map(f => f*(1+Math.min(chain-1,6)*.12)) : kind === 'spell' ? [196,392,523] : kind === 'win' ? [330,440,554,660] : kind === 'lose' ? [220,196,147] : [110];
    tones.forEach((frequency,i) => {
      const osc = soundContext.createOscillator(), gain = soundContext.createGain();
      osc.type = kind === 'hit' ? 'triangle' : 'sine'; osc.frequency.value = frequency;
      gain.gain.setValueAtTime(0,now+i*.055); gain.gain.linearRampToValueAtTime(kind==='cast'?.009:.035,now+i*.055+.01); gain.gain.exponentialRampToValueAtTime(.0001,now+i*.055+.2);
      osc.connect(gain); gain.connect(soundContext.destination); osc.start(now+i*.055); osc.stop(now+i*.055+.22);
    });
  } catch { /* Audio is optional on browsers that block Web Audio. */ }
}

function eventMark(event) {
  const paths={supply:'<path d="M7 17h34v24H7zM6 12h36v9H6z"/><path d="M20 12v29h8V12M17 12V8h14v4"/>',frost:'<path d="m24 3 17 11v21L24 45 7 35V14Z"/><path d="M24 8v32M10 17l28 16M10 33l28-16M18 13l6 6 6-6M18 35l6-6 6 6"/>',curse:'<path d="m24 4 18 20-18 20L6 24Z"/><path d="m17 16 14 16m0-16L17 32"/>'};
  return `<span class="event-mark event-${event.type}"><svg viewBox="0 0 48 48" fill="none" stroke="currentColor" stroke-width="3" aria-hidden="true">${paths[event.type]||''}</svg>${event.type==='curse'?`<b>${event.fuse}</b>`:''}</span>`;
}

function applyBoardEvents(events,targetLane) {
  for(const event of events) {
    if(event.action==='spawned'){banner({supply:'RARE FIND · Match the supply cache for gold and charge.',frost:'FROZEN RUNE · Match beside it to thaw.',curse:'CURSED RUNE · Clear it within 4 moves!'}[event.type]);continue;}
    if(event.action==='collected'){eventCount++;battle.gold+=event.gold||20;charge+=8;burst(dailyConfig?'SUPPLY CACHE · CHARGE + SCORE':'SUPPLY CACHE · +20 GOLD');}
    if(event.action==='purified'){eventCount++;charge+=8;overdrive=Math.max(overdrive,5);burst('CURSE PURIFIED · EMPOWERED SUMMONS');}
    if(event.action==='thawed'){eventCount++;burst('ICE SHATTERED');}
    if(event.action==='reinforcement'){battle.reinforce(event.key,{lane:targetLane});banner(`CURSE UNLEASHED · An enemy joins lane ${targetLane+1}.`);}
  }
}

function renderBoard(snapshot=board.snapshot(), animate=false) {
  const ids = new Set();
  snapshot.forEach((row,r) => row.forEach((tile,c) => {
    ids.add(tile.id);
    let node = tileNodes.get(tile.id);
    const fresh = !node;
    if (fresh) { node = document.createElement('button'); node.className='tile'; node.type='button'; node.setAttribute('role','gridcell'); tileNodes.set(tile.id,node); boardEl.append(node); }
    node.dataset.id = tile.id; node.dataset.r = r; node.dataset.c = c;
    node.dataset.key = tile.key; node.dataset.special = tile.special || '';
    node.dataset.event=tile.event?.type||'';
    const signature = `${tile.key}:${tile.special}:${tile.relic}:${tile.event?.type}:${tile.event?.fuse}`;
    if (node.dataset.signature !== signature) {
      node.dataset.signature = signature;
      node.style.setProperty('--element', colors[tile.key]);
      node.innerHTML = `<span class="tile-inner">${rune(tile.key)}${tile.special ? '<span class="special-mark"></span>' : ''}${tile.relic ? `<span class="relic-mark" style="--relic-color:${RELICS[tile.relic].color}">${RELICS[tile.relic].symbol}</span>` : ''}${tile.event?eventMark(tile.event):''}</span>`;
    }
    node.classList.remove('clearing','hint','invalid');
    node.classList.toggle('selected', !!selected && selected.r===r && selected.c===c);
    node.setAttribute('aria-label',`${activeTroop(tile.key).name}${tile.special ? ', '+tile.special+' special' : ''}${tile.relic ? ', '+RELICS[tile.relic].name+' relic' : ''}${tile.event?', '+({supply:'supply cache',frost:'frozen: clear a neighbor to thaw',curse:'cursed rune, '+tile.event.fuse+' moves left'}[tile.event.type]):''}. Row ${r+1}, column ${c+1}`);
    node.setAttribute('aria-rowindex',r+1); node.setAttribute('aria-colindex',c+1);
    node.setAttribute('aria-selected',String(!!selected && selected.r===r && selected.c===c));
    node.tabIndex = r===0 && c===0 ? 0 : -1;
    if (fresh && animate) {
      node.style.transition='none'; node.style.setProperty('--c',c); node.style.setProperty('--r',-1-(r%3));
      node.offsetWidth; node.style.transition='';
    }
    node.style.setProperty('--r',r); node.style.setProperty('--c',c);
  }));
  for (const [id,node] of tileNodes) if (!ids.has(id)) { node.remove(); tileNodes.delete(id); }
}

function markSelection() {
  for (const node of tileNodes.values()) {
    const active = selected && +node.dataset.r===selected.r && +node.dataset.c===selected.c;
    node.classList.toggle('selected',!!active); node.setAttribute('aria-selected',String(!!active));
  }
}

function setLane(value) {
  lane = clamp(Math.floor(value),0,2);
  document.querySelectorAll('[data-lane]').forEach(button => { const active = +button.dataset.lane === lane; button.classList.toggle('active',active); button.setAttribute('aria-pressed',String(active)); });
  if (started && !busy) $('board-message').textContent=`New troops will enter the ${['upper','middle','lower'][lane]} lane.`;
}

function inputReady() { return started && !busy && backdrop.hidden && battle.status==='playing' && !document.hidden; }
function selectCell(cell) {
  if (!inputReady()) return;
  clearHint();
  if (selected && Math.abs(cell.r-selected.r)+Math.abs(cell.c-selected.c)===1) { const from = selected; selected=null; swap(from,cell); }
  else { selected = selected && selected.r===cell.r && selected.c===cell.c ? null : cell; markSelection(); }
}

async function swap(from,to) {
  if (!inputReady()) return;
  const token=session, targetLane=lane, focusWasBoard=boardEl.contains(document.activeElement);
  busy=true; selected=null; boardEl.setAttribute('aria-busy','true'); markSelection();
  const old = board.snapshot();
  const swapped = old.map(row=>row.map(t=>({...t})));
  [swapped[from.r][from.c],swapped[to.r][to.c]]=[swapped[to.r][to.c],swapped[from.r][from.c]];
  renderBoard(swapped);
  await delay(180);
  if(!await waitForPlay(token)) return;
  const result=board.trySwap(from,to);
  if(!result.valid) {
    renderBoard(old);
    [from,to].forEach(p=>tileNodes.get(old[p.r][p.c].id)?.classList.add('invalid'));
    $('board-message').textContent=result.reason==='frozen'?'Frozen rune: match beside it to thaw, or clear it in a line or blast.':'Make a line of 3, or swap a special rune. Try again.';
    await delay(220);
  } else {
    for(const step of result.steps) {
      if(!await waitForPlay(token)) return;
      renderBoard(step.before);
      step.cleared.forEach(({tile})=>tileNodes.get(tile.id)?.classList.add('clearing'));
      matches += step.groups.length; bestChain=Math.max(bestChain,step.chain);
      $('combo').textContent=`×${(1+(step.chain-1)*(.14+(battle.upgrades.combo||0)*.06)).toFixed(2).replace(/0$/,'').replace(/\.0$/,'')}`;
      $('combo-label').textContent=step.chain>1 ? `CASCADE ${step.chain}` : 'COMBO POWER';
      document.querySelector('.combo-display').classList.remove('ignite');
      void $('combo').offsetWidth; document.querySelector('.combo-display').classList.add('ignite');
      if(step.chain>1 || step.created.length) burst(step.chain>1 ? `${step.chain}× CASCADE` : step.created[0].tile.special==='wild' ? 'WILD RUNE' : 'SPECIAL FORGED');
      sound('match',step.chain);
      await delay(190);
      if(!await waitForPlay(token)) return;
      for(const group of step.groups) {
        const spawned=battle.summon(group.key,{lane:targetLane,chain:step.chain,count:group.count,buffs:group.buffs,elite:group.elite,power:overdrive>0 ? 1.25 : 1});
        summonCount+=spawned.length;
      }
      charge += step.cleared.length;
      if(charge>=50 && overdrive<=0) { charge-=50; overdrive=10; banner('OVERDRIVE · Summons gain +25% power for 10 seconds'); burst('OVERDRIVE'); }
      renderBoard(step.after,true);
      const buffs = [...new Set(step.groups.flatMap(g=>g.buffs))];
      const names = step.groups.map(g=>activeTroop(g.key).name).slice(0,2).join(' + ');
      $('board-message').textContent = `${names} → lane ${targetLane+1}${buffs.length ? ' · '+buffs.map(k=>RELICS[k].name).join(' + ') : step.chain>1 ? ' · empowered cascade' : ''}`;
      await delay(240);
    }
    if(!await waitForPlay(token)) return;
    applyBoardEvents(result.events||[],targetLane);
    renderBoard(board.snapshot(),true);
    if(result.reshuffled) { renderBoard(board.snapshot(),true); toast('No swaps left — the runes have reshuffled.'); }
  }
  if(token!==session) return;
  busy=false; boardEl.setAttribute('aria-busy','false');
  if(focusWasBoard) { const node=boardEl.querySelector(`[data-r="${to.r}"][data-c="${to.c}"]`); if(node){node.tabIndex=0;node.focus({preventScroll:true});} }
  updateUI();
  if(battle.status!=='playing') finishBattle();
}

function burst(text) { const el=$('board-burst'); el.textContent=text; el.classList.remove('show'); void el.offsetWidth; el.classList.add('show'); }
function clearHint() { clearTimeout(hintTimer); for(const node of tileNodes.values())node.classList.remove('hint'); }
function showHint() {
  if(!inputReady()) return;
  clearHint(); selected=null; markSelection();
  const move=board.hint();
  if(move) {
    [move.from,move.to].forEach(p=>tileNodes.get(board.grid[p.r][p.c].id)?.classList.add('hint'));
    $('board-message').textContent='Swap the two glowing runes. Specials also activate by swapping.';
    hintTimer=setTimeout(clearHint,4000);
  }
}

boardEl.addEventListener('pointerdown',event=>{
  const tile=event.target.closest('.tile'); if(!tile || !inputReady() || event.button>0)return;
  pointer={id:event.pointerId,x:event.clientX,y:event.clientY,r:+tile.dataset.r,c:+tile.dataset.c};
  boardEl.setPointerCapture(event.pointerId);
});
boardEl.addEventListener('pointerup',event=>{
  if(!pointer || pointer.id!==event.pointerId)return;
  const down=pointer; pointer=null;
  const dx=event.clientX-down.x,dy=event.clientY-down.y;
  suppressClick=true;
  if(Math.hypot(dx,dy)>18) {
    const to={r:down.r+(Math.abs(dy)>Math.abs(dx)?Math.sign(dy):0),c:down.c+(Math.abs(dx)>=Math.abs(dy)?Math.sign(dx):0)};
    if(to.r>=0&&to.r<7&&to.c>=0&&to.c<7){selected=null;swap({r:down.r,c:down.c},to);}
  } else selectCell({r:down.r,c:down.c});
  setTimeout(()=>suppressClick=false,0);
});
boardEl.addEventListener('pointercancel',()=>pointer=null);
boardEl.addEventListener('click',event=>{
  if(suppressClick)return;
  const tile=event.target.closest('.tile'); if(tile)selectCell({r:+tile.dataset.r,c:+tile.dataset.c});
});
boardEl.addEventListener('keydown',event=>{
  const tile=event.target.closest('.tile');if(!tile)return;
  const moves={ArrowUp:[-1,0],ArrowDown:[1,0],ArrowLeft:[0,-1],ArrowRight:[0,1]};
  if(moves[event.key]) {
    event.preventDefault();const [dr,dc]=moves[event.key];
    const r=clamp(+tile.dataset.r+dr,0,6),c=clamp(+tile.dataset.c+dc,0,6);
    tile.tabIndex=-1;const next=boardEl.querySelector(`[data-r="${r}"][data-c="${c}"]`);next.tabIndex=0;next.focus({preventScroll:true});
  }
});

function openModal(title,html,kind='menu',kicker='WAR COUNCIL') {
  if(!backdrop.hidden && (modalKind==='intro'||modalKind==='result') && kind!==modalKind) {
    rootDialog={title:$('modal-title').textContent,html:content.innerHTML,kind:modalKind,kicker:$('modal-kicker').textContent};
  }
  lastFocus = backdrop.hidden ? document.activeElement : lastFocus;
  modalKind=kind; $('modal-title').textContent=title; $('modal-kicker').textContent=kicker;
  content.innerHTML=html; backdrop.hidden=false;
  document.body.style.overflow='hidden';
  document.querySelector('.app-shell').inert=true;
  modal.scrollTop=0; modal.focus();
  $('modal-close').hidden=kind==='intro'||kind==='result';
  drawPortraits();
}

function closeModal() {
  if(modalKind==='intro'||modalKind==='result')return;
  if(!started){showIntro();return;}
  if(battle.status!=='playing'&&rootDialog?.kind==='result'){
    const saved=rootDialog;rootDialog=null;openModal(saved.title,saved.html,saved.kind,saved.kicker);
    const forgeButton=content.querySelector('[data-action="forge"]');if(forgeButton)forgeButton.textContent=`Upgrade forge · ◆ ${progress.gold}`;
    return;
  }
  backdrop.hidden=true; document.querySelector('.app-shell').inert=false; modalKind='';
  document.body.style.overflow='';
  lastFocus?.focus({preventScroll:true});lastFrame=performance.now();
}
function dismissModal() { modalKind='';rootDialog=null;backdrop.hidden=true;document.body.style.overflow='';document.querySelector('.app-shell').inert=false;lastFrame=performance.now(); }
$('modal-close').onclick=closeModal;
backdrop.addEventListener('click',e=>{if(e.target===backdrop)closeModal();});
document.addEventListener('keydown',event=>{
  if(!backdrop.hidden) {
    if(event.key==='Escape'){event.preventDefault();closeModal();}
    if(event.key==='Tab') {
      const all=[...modal.querySelectorAll('button:not(:disabled):not([hidden]),a,input')].filter(x=>x.getClientRects().length);
      if(!all.length){event.preventDefault();return;}
      if(event.shiftKey&&(document.activeElement===all[0]||document.activeElement===modal)){event.preventDefault();all.at(-1).focus();}
      else if(!event.shiftKey&&(document.activeElement===all.at(-1)||document.activeElement===modal)){event.preventDefault();all[0].focus();}
    }
    return;
  }
  if(event.ctrlKey||event.metaKey||event.altKey)return;
  if(['1','2','3'].includes(event.key)){event.preventDefault();setLane(+event.key-1);}
  if(event.key.toLowerCase()==='h'){event.preventDefault();showHint();}
  if(event.key.toLowerCase()==='p'||event.key==='Escape'){event.preventDefault();showMenu();}
  const spell={q:'meteor',w:'rally',e:'heal'}[event.key.toLowerCase()];if(spell){event.preventDefault();cast(spell);}
});

function showIntro() {
  openModal(progress.seenIntro?'Your army awaits.':'Every match turns the tide.',`
    <p class="guide-intro">${progress.seenIntro?`Continue at battle ${progress.stage}: ${STAGES[progress.stage-1][0]}. Your upgrades and campaign progress are saved.`:'The rift is open. Match elemental runes to summon an army, choose where it fights, and turn cascading combos into overwhelming power.'}</p>
    ${progress.legacyMigrated?'<p class="legacy-note">Your original Color Clash gold, levels, and earned upgrades have been carried into Match Wars.</p>':''}
    <div class="feature-grid"><div class="feature-card"><b>01 · Match & summon</b><p>Tap two neighboring runes or swipe. Lines of 3 summon your equipped class. Longer matches forge special runes.</p></div><div class="feature-card"><b>02 · Build a formation</b><p>Troops cast their own spells. Pair colors in the same lane for healing covenants, frozen shatters, or armies of the dead.</p></div><div class="feature-card"><b>03 · Choose your classes</b><p>Unlock 12 additional classes at campaign milestones. Earn mastery XP and choose from 36 specializations.</p></div><div class="feature-card"><b>04 · Conquer the frontiers</b><p>Fight across ${TOTAL_STAGES} battles. Spend gold on 18 upgrade paths and adapt to ${Object.keys(ENEMIES).length} enemy types.</p></div></div>
    <div class="modal-actions"><button class="primary-button" data-action="begin">${progress.seenIntro?'Return to the front':'Begin the campaign'} →</button><button class="secondary-button" data-action="army">Choose classes</button><button class="secondary-button" data-action="forge">Upgrade forge · ◆ ${progress.gold}</button></div>
    <p class="muted">PC: arrows + Enter to match · 1 / 2 / 3 lanes · Q / W / E spells · H hint · P pause. On mobile, tap or swipe.</p>`, 'intro','THE RUNEBORN FRONT');
}

function begin() {
  progress.seenIntro=true; started=true;persist();dismissModal();sound('match');banner('The first wave arrives in 5 seconds. Match your army!');
}

function showMenu() {
  openModal('Take a breath.',`<p>Battle is paused. Plan your next move.</p><div class="menu-grid"><button data-action="resume">↗ Return to battle</button><button data-action="army">✦ Troop classes</button><button data-action="forge">◆ Upgrade forge</button><button data-action="campaign">✧ Campaign map</button><button data-action="daily">✦ Daily challenge</button><button data-action="guide">⌘ Field guide</button><button data-action="bonds">◇ Formation bonds</button><button data-action="how">? How to play</button><button data-action="restart">↻ Restart this battle</button></div><label class="setting-row">Sound effects<input id="sound-setting" type="checkbox" ${progress.settings.sound?'checked':''}></label><label class="setting-row">Reduce visual effects<input id="motion-setting" type="checkbox" ${progress.settings.reducedMotion?'checked':''}></label><div class="modal-actions"><a class="secondary-button" href="/lobby/">‹ Back to the library</a></div>`, 'menu');
}

function rareGuide() { return '<h3>Rare battlefield finds</h3><p>From battle 5, very rare supply caches grant battle gold and Overdrive charge. Frozen runes cannot be swapped: match beside one to thaw it, or clear it in a match or special blast. Clear a cursed rune within four successful swaps for five seconds of empowered summons; letting it expire calls one enemy reinforcement. Only one board event can be present at a time, with at least eight successful moves between new events.</p><h3>Master your classes</h3><p>Campaign troops earn mastery XP through deployment and useful spells. At mastery 3, choose one of two specializations for each class. Choices are free to change for the next battle. Daily challenges use fixed mastery and award medals instead of class XP.</p>'; }

function showHow() {
  openModal('A commander’s primer.',`<div class="feature-grid"><div class="feature-card"><b>Matches become troops</b><p>Each match summons one soldier. Matches of 5 or more summon extra troops. Choose your lane first; every cascade uses that lane. Invalid swaps return safely.</p></div><div class="feature-card"><b>Forge special runes</b><p>Match 4 for a line blast, 5 for a wild rune, and a T or L for a nova. Swap a special with any neighbor to activate it. Wilds clear the other rune’s element. Special blasts chain.</p></div><div class="feature-card"><b>Cascades & Overdrive</b><p>Falling runes can match again. Each cascade gives new troops +14% power, increased by Battle Rhythm. Clear 50 runes for 10 seconds of +25% summon power.</p></div><div class="feature-card"><b>Relics empower summons</b><p>Small colored marks are relics. Match them to buff that element’s new soldiers: ↑ Fury, ◇ Aegis, ϟ Haste, + Vitality. Forge talents strengthen these buffs.</p></div><div class="feature-card"><b>Build a formation</b><p>Sun Paladins hold the front while Tide Rangers and Ember Mages attack from behind. Grove Wardens sustain them. Storm Lancers hunt flyers; Reapers punish weakened tanks. Summon the same element 4 / 8 / 12 times to increase its resonance.</p></div><div class="feature-card"><b>Win the battle</b><p>Destroy the rift or defeat every wave. Later chapters bring more waves, denser formations, and elite enemies. Defend your citadel with troops and spells. Chapter finales have a boss ward: defeat the chapter boss to break it. Losses grant some earned gold so you can adapt and retry.</p></div></div><h3>Elemental counters</h3><p>Ember → Grove → Storm → Tide → Ember. A counter deals 1.5× damage; fighting into your weakness deals 0.7×. Sun and Void both deal 1.35× to each other.</p><p>Spells target your selected lane. Meteor damages enemies, Rally grants attack speed and shields, Renew heals troops and repairs your citadel. They recharge during battle.</p>${rareGuide()}<p>Progress saves on this browser and device after battles and purchases. Refreshing restarts the current battle. The forge pauses combat; new class upgrades affect future summons.</p><div class="modal-actions"><button class="primary-button" data-action="resume">${started?'Back to battle':'Start battle'} →</button></div>`, 'how','MATCH · COMMAND · CONQUER');
}

function troopGuide(key) {
  const troop=activeTroop(key), info=ELEMENT_INFO[key];
  const resonance=Math.min(3,Math.floor((battle.synergy[key]||0)/4));
  openModal(troop.name,`${classPortrait(troop,'dialog-portrait')}<p style="color:${info.color}">${troop.role} · ${info.name}</p><p>${troop.description}</p><p>Health <b>${troop.hp}</b> · Attack <b>${troop.attack}</b> · Range <b>${troop.range}</b></p><div class="spell-detail"><b>${troop.spell.name}</b><small>Automatic spell · ${troop.spell.cooldown}s recharge</small><p>${troop.spell.description}</p></div><p>Strong against ${ELEMENT_INFO[info.strong].name}${key==='sun'||key==='void'?' (mutual counter)':' · vulnerable to '+ELEMENT_INFO[info.weak].name}.</p><p>Battle resonance: ${resonance} / 3. Every four summons adds +5% power, up to +15%.</p><h3>Formation partners</h3>${COMBOS.filter(c=>c.elements.includes(key)).map(c=>`<p><b>${c.name}</b> · ${c.elements.map(k=>ELEMENT_INFO[k].name).join(' + ')}<br>${c.description}</p>`).join('')}<div class="modal-actions"><button class="primary-button" data-army="${key}">Choose ${info.name} class →</button><button class="secondary-button" data-action="forge" data-tab="${key}">Train talents</button><button class="secondary-button" data-action="resume">Return</button></div>`,'troop','ELEMENTAL ARMORY');
}

function masteryPanel(def,unlocked) {
  const record=progress.mastery[def.id]||{xp:0,specialization:null},info=masteryProgress(record.xp);
  return `<div class="mastery-panel"><div class="mastery-heading"><b>MASTERY ${info.rank} / 6</b><small>${info.rank===6?'MASTERED':`${record.xp} / ${info.nextXp} XP`}</small></div><div class="mastery-track" role="progressbar" aria-label="${escape(def.name)} mastery" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${Math.round(info.progress*100)}"><i style="width:${info.progress*100}%"></i></div><p>${info.rank<3?'Use this class in campaign battles. Choose a specialization at mastery 3.':'Choose one specialization. You can change it freely for your next battle.'}</p><div class="specialization-list">${SPECIALIZATIONS[def.id].map(spec=>`<button data-specialize="${spec.id}" data-class-id="${def.id}" class="specialization ${record.specialization===spec.id?'chosen':''}" ${!unlocked||info.rank<3?'disabled':''} aria-pressed="${record.specialization===spec.id}"><b>${record.specialization===spec.id?'✦ ':''}${escape(spec.name)}</b><span>${escape(spec.description)}</span></button>`).join('')}</div></div>`;
}

function showArmory(key='ember') {
  if(dailyConfig){showDailyArmy();return;}
  if(!CLASS_CHOICES[key])key='ember';
  const chosen=progress.loadout[key],pending=started&&battle.status==='playing'&&battle.loadout[key]!==chosen;
  const earned=Object.values(CLASSES).filter(c=>isClassUnlocked(c.id,progress.unlocked)).length;
  openModal('Your colors. Your classes.',`<p>Equip one class for each color. Troops cast spells automatically when suitable targets are in reach. <b>${earned} / ${Object.keys(CLASSES).length}</b> classes unlocked.</p><p class="armory-rule">${started&&battle.status==='playing'?'Changes are saved for your next battle. Your current formation keeps its equipped classes.':'Choose your formation before the first wave. Class choices are free once unlocked.'}</p><div class="tabs class-tabs">${ELEMENTS.map(k=>`<button data-army="${k}" class="${k===key?'active':''}" style="${k===key?'':'color:'+colors[k]}">${ELEMENT_INFO[k].name}</button>`).join('')}</div><div class="class-grid">${CLASS_CHOICES[key].map(def=>{
    const unlocked=isClassUnlocked(def.id,progress.unlocked),selected=def.id===chosen;
    return `<article class="class-card ${selected?'equipped':''} ${unlocked?'':'locked'}" style="--element:${def.color}"><div class="class-portrait-wrap">${classPortrait(def,'class-portrait')}<span>${selected?'EQUIPPED':unlocked?'AVAILABLE':`BATTLE ${def.unlockLevel}`}</span></div><h3>${def.name}</h3><small class="class-role">${def.role}</small><p>${def.description}</p><div class="class-statline"><span>${def.hp} HP</span><span>${def.attack} ATK</span><span>${def.range} RANGE</span></div><div class="spell-detail"><b>${def.spell.name}</b><small>Auto cast · ${def.spell.cooldown}s</small><p>${def.spell.description}</p></div>${masteryPanel(def,unlocked)}<button class="${selected?'secondary-button':'primary-button'}" data-equip="${def.id}" ${!unlocked||selected?'disabled':''}>${selected?'Equipped':unlocked?'Equip class':`Reach battle ${def.unlockLevel} to unlock`}</button></article>`;
  }).join('')}</div><p class="class-swipe-hint">Swipe across the cards to compare all three classes.</p>${pending?'<p class="pending-note">New class equipped for the next battle.</p>':''}<h3>Build a bond</h3><p>${COMBOS.filter(c=>c.elements.includes(key)).map(c=>`<b>${c.name}</b>: ${c.elements.map(k=>ELEMENT_INFO[k].name).join(' + ')}. ${c.description}`).join('<br>')}</p><div class="modal-actions"><button class="primary-button" data-action="resume">${started?'Return to the front':'Start with this army'} →</button>${started&&battle.status==='playing'?'<button class="secondary-button" data-action="restart">Restart with this army</button>':''}<button class="secondary-button" data-action="bonds">All formation bonds</button></div>`,'army','THE CLASS ARMORY');
  if(mobileScreen.matches){const grid=content.querySelector('.class-grid'),card=content.querySelector('.class-card.equipped');if(grid&&card)grid.scrollLeft+=card.getBoundingClientRect().left-grid.getBoundingClientRect().left;}
}

function equipClass(id) {
  if(dailyConfig)return;
  const def=CLASSES[id];if(!def||!isClassUnlocked(id,progress.unlocked))return;
  progress.loadout[def.key]=id;progress.loadout=normalizeLoadout(progress.loadout,progress.unlocked);persist();
  if(!dailyConfig&&!started&&battle.time===0)startBattle(progress.stage);
  showArmory(def.key);sound('match');
}

function equipSpecialization(classId,id) {
  if(dailyConfig)return;
  const def=CLASSES[classId];if(!def||!isClassUnlocked(classId,progress.unlocked))return;
  const scroll=modal.scrollTop;
  progress.mastery=selectSpecialization(progress.mastery,classId,id);persist();
  if(!dailyConfig&&!started&&battle.time===0)startBattle(progress.stage);
  showArmory(def.key);modal.scrollTop=scroll;toast(started?'Specialization saved for your next campaign battle.':'Specialization ready.');
}

function showBonds() {
  const active=new Set((battle.activeCombos?.(lane)||[]).map(c=>c.id));
  openModal('Stronger together.',`<p>Keep both colors alive in the same lane to activate a formation bond. Choose classes that supply the healing, shields, or status effects the bond needs.</p><div class="guide-grid">${COMBOS.map(c=>`<div class="guide-card bond-card"><span class="bond-status ${active.has(c.id)?'on':''}">${active.has(c.id)?'ACTIVE IN LANE '+(lane+1):'FORMATION BOND'}</span><h3>${c.name}</h3><div class="bond-colors">${c.elements.map(k=>`<span style="color:${colors[k]}">${ELEMENT_INFO[k].name}</span>`).join(' + ')}</div><p>${c.description}</p><small>${c.condition}</small></div>`).join('')}</div><div class="modal-actions"><button class="primary-button" data-action="army">Build your army →</button><button class="secondary-button" data-action="resume">Return</button></div>`,'bonds','LANE FORMATIONS');
}

function showDailyArmy() {
  openModal('The challenge army.',`<p>This army is fixed for the shared daily challenge, with forge talents at rank 2 and class mastery at rank 3. Campaign class choices and specializations do not affect it.</p><div class="guide-grid">${ELEMENTS.map(key=>{const def=battle.troopFor(key),record=dailyConfig.mastery[def.id],spec=SPECIALIZATIONS[def.id].find(s=>s.id===record.specialization);return `<article class="guide-card">${classPortrait(def)}<h3>${escape(def.name)}</h3><p>${escape(def.role)}</p><div class="spell-detail"><b>${escape(def.spell.name)}</b><p>${escape(def.spell.description)}</p></div><p><b>${escape(spec.name)}</b><br>${escape(spec.description)}</p></article>`;}).join('')}</div><div class="modal-actions"><button class="primary-button" data-action="resume">Return to challenge →</button><button class="secondary-button" data-action="daily">Daily briefing</button><button class="secondary-button" data-action="bonds">Formation bonds</button></div>`,'army','DAILY · FIXED ARMY');
}

function showGuide(tab='troops') {
  const entries=tab==='enemies'?Object.values(ENEMIES):Object.values(CLASSES);
  const tabs=`<div class="tabs"><button data-guide="troops" class="${tab==='troops'?'active':''}">${Object.keys(CLASSES).length} troop classes</button><button data-guide="enemies" class="${tab==='enemies'?'active':''}">${Object.keys(ENEMIES).length} enemy types</button><button data-guide="relics" class="${tab==='relics'?'active':''}">Relics & buffs</button><button data-action="bonds">Formation bonds</button></div>`;
  const relics=`<div class="guide-grid">${Object.values(RELICS).map(r=>`<div class="guide-card"><b style="color:${r.color}">${r.symbol} ${r.name}</b><p>${r.detail} for troops summoned by a match containing this relic. Relic types stack; Relic Attunement improves all four.</p></div>`).join('')}</div><p>Gold shield rings, orange Fury sparks, cyan Haste trails, and green Vitality crosses identify empowered troops. Roots, frost, curses, and regeneration have distinct effects.</p>`;
  const cards=`<div class="guide-grid">${entries.map(t=>`<div class="guide-card">${tab==='enemies'?`<canvas class="portrait" data-portrait="${t.element}" data-enemy="${t.key}" width="180" height="180"></canvas>`:classPortrait(t)}<b style="color:${colors[t.element]}">${t.name}</b><p>${t.role}</p><p>${t.description||`${ELEMENT_INFO[t.element].name} · ${t.hp} health · ${t.attack} attack${t.boss?' · BOSS':''}`}</p>${t.spell?`<p><b>${t.spell.name}</b> · ${t.spell.cooldown}s<br>${t.spell.description||''}</p>`:''}<div class="counter-list"><span>Counter: ${ELEMENT_INFO[Object.keys(TROOPS).find(k=>elementMultiplier(k,t.element)>1)||'storm'].name}</span>${t.unlockLevel?`<span>${isClassUnlocked(t.id,progress.unlocked)?'Unlocked':'Reach battle '+t.unlockLevel}</span>`:''}</div></div>`).join('')}</div>`;
  openModal('Know your battlefield.',`<p>Pair a frontline, a ranged attacker, and a support. Enemy spellcasters and undead create new threats to counter.</p>${tabs}${tab==='enemies'?`<div class="champion-guide"><h3>Champions of the Eternal Front (85+)</h3>${Object.values(CHAMPION_AFFIXES).map(a=>`<p><b style="color:${a.color}">${escape(a.name)}</b> · ${escape(a.description)}</p>`).join('')}</div>`:''}${tab==='relics'?relics:cards}`,'guide','THE FIELD GUIDE');
}

function showForge(tab='command') {
  if(dailyConfig){openModal('A level field.',`<p>Daily challenges use a fixed army, mastery, and forge so every player faces the same challenge. Campaign purchases and class choices do not affect this run.</p><div class="modal-actions"><button class="primary-button" data-action="resume">Return to challenge</button><button class="secondary-button" data-action="daily">Daily briefing</button></div>`,'forge','DAILY CHALLENGE');return;}
  openModal('The upgrade forge.',`<p>Build around the army you want to play. <b style="color:var(--gold)">◆ ${progress.gold}</b> available. Purchased troop talents empower future summons; keep and spell talents apply immediately.</p><div class="tabs"><button data-forge="command" class="${tab==='command'?'active':''}">Command</button>${ELEMENTS.map(k=>`<button data-forge="${k}" class="${tab===k?'active':''}" style="${tab!==k?'color:'+colors[k]:''}">${ELEMENT_INFO[k].name}</button>`).join('')}</div><div class="upgrade-grid">${UPGRADES.filter(u=>u.group===tab).map(u=>{const rank=progress.upgrades[u.id]||0,cost=upgradeCost(u.id,rank);return `<div class="upgrade-card"><h3>${u.name}</h3><small>RANK ${rank} / ${u.max}</small><p>${u.description}</p><div class="progress-dots">${Array.from({length:u.max},(_,i)=>`<i class="${i<rank?'filled':''}"></i>`).join('')}</div><button data-buy="${u.id}" ${rank>=u.max||progress.gold<cost?'disabled':''}>${rank>=u.max?'Fully trained':`Train rank ${rank+1} · ◆ ${cost}`}</button></div>`;}).join('')}</div><div class="modal-actions"><button class="primary-button" data-action="resume">${started?'Return to the front':'Start battle'} →</button></div>`,'forge','CRAFT YOUR STRATEGY');
}

function buyUpgrade(id) {
  if(dailyConfig)return;
  const def=UPGRADES.find(u=>u.id===id);if(!def)return;
  const rank=progress.upgrades[id]||0,cost=upgradeCost(id,rank);
  if(rank>=def.max||progress.gold<cost)return;
  progress.gold-=cost;progress.upgrades[id]=rank+1;
  battle.upgrades[id]=rank+1;
  if(id==='fortress'){battle.playerMax+=40;battle.playerHp=Math.min(battle.playerMax,battle.playerHp+40);}
  persist();sound('match');showForge(def.group);updateUI();
}

function showCampaign(chapterNumber=getChapter(progress.stage).number) {
  const index=clamp(Number(chapterNumber)||1,1,CHAPTERS.length)-1,chapter=CHAPTERS[index];
  const first=chapter.startStage, stages=STAGES.slice(first-1,chapter.endStage), cleared=progress.completed.filter(n=>n>=first&&n<=chapter.endStage).length;
  openModal('Beyond the first rift.',`<p>${TOTAL_STAGES} battles across ${CHAPTERS.length} chapters. Each frontier brings stronger armies, tighter waves, and a new boss showdown. ${progress.completed.length} / ${TOTAL_STAGES} cleared.</p>
    <div class="chapter-navigation"><button class="secondary-button" data-chapter="${chapter.number-1}" aria-label="Previous chapter" ${index===0?'disabled':''}>‹</button><label for="campaign-chapter" class="sr-only">Choose a campaign chapter</label><select id="campaign-chapter">${CHAPTERS.map(c=>`<option value="${c.number}" ${c.number===chapter.number?'selected':''}>${roman(c.number)} · ${escape(c.title)}${progress.unlocked<c.startStage?' · Locked':''}</option>`).join('')}</select><button class="secondary-button" data-chapter="${chapter.number+1}" aria-label="Next chapter" ${index===CHAPTERS.length-1?'disabled':''}>›</button></div>
    <div class="chapter-summary" style="--chapter-color:${chapter.color}"><span class="eyebrow">CHAPTER ${roman(chapter.number)} · ${difficultyName(first).toUpperCase()} · ${cleared} / ${chapter.stageCount} CLEARED</span><h3>${escape(chapter.title)}</h3><p>${escape(chapter.description)}</p><small>Chapter boss: ${escape(chapter.bossName)}</small></div>
    <div class="campaign-grid chapter-stages">${stages.map(([name,description],i)=>{const stage=first+i,difficulty=difficultyForStage(stage);return `<button class="stage-card ${stage===progress.stage?'current':''}" data-stage="${stage}" ${stage>progress.unlocked?'disabled':''}><span class="stage-number">${String(stage).padStart(2,'0')} ${progress.completed.includes(stage)?'✦':getStage(stage).boss?'♜':''}</span><b>${escape(name)}</b><small>${escape(description)}</small><span class="stage-difficulty">${stage>progress.unlocked?'Locked · ':''}${difficulty.totalWaves} waves${getStage(stage).boss?' · Boss ward':''}</span></button>`;}).join('')}</div>
    <div class="modal-actions"><button class="primary-button" data-stage="${progress.unlocked}">Continue · battle ${progress.unlocked} →</button><button class="secondary-button" data-action="army">Troop classes</button><button class="secondary-button" data-action="resume">Return</button></div>`, 'campaign',`THE ${TOTAL_STAGES}-BATTLE CAMPAIGN`);
}

function showDaily(date=todayKey()) {
  const config=createDaily(date);if(!config){toast('Choose a challenge from the past 30 days.');return;}
  const saved=progress.daily.records[config.date],targets=config.medalTargets;

  openModal('One day. One shared battlefield.',`<div class="daily-hero"><span class="eyebrow">UTC DAILY · ${config.date}${config.date!==todayKey()?' · PRACTICE':''}</span><h3>${escape(getStage(config.stage).name)}</h3><p>Everyone gets the same starting board, army, mastery, and enemy waves. Earn medals and beat your personal best.</p><div class="daily-targets"><span>✦ GOLD <b>${targets.gold.toLocaleString()}</b></span><span>◇ SILVER <b>${targets.silver.toLocaleString()}</b></span><span>○ BRONZE <b>${targets.bronze.toLocaleString()}</b></span></div></div><div class="daily-army">${ELEMENTS.map(k=>{const def=getTroop(k,config.loadout[k]);return `<div>${classPortrait(def)}<b>${escape(def.name)}</b></div>`;}).join('')}</div><p>Clear waves quickly, protect your citadel, and build cascades to score. ${config.date===todayKey()?`First victory today earns ◆ ${config.reward}${saved?.claimed?' (already claimed)':''}.`:'Past challenges are practice; no gold reward.'} Daily runs do not change your campaign or class mastery.</p><div class="daily-best"><span>YOUR BEST</span><b>${saved?saved.score.toLocaleString():'—'}</b><small>${saved?`${saved.medal.toUpperCase()} · ${saved.attempts} completed attempt${saved.attempts===1?'':'s'}`:'No completed attempts yet'}</small></div><div class="modal-actions"><button class="primary-button" data-daily="${config.date}">${saved?'Try again':'Start challenge'} →</button><button class="secondary-button" data-share-daily="${config.date}">Copy challenge link</button><button class="secondary-button" data-action="campaign-return">Return to campaign</button></div><label class="daily-date">Practice a recent day<input type="date" id="daily-date" min="${new Date(Date.now()-29*86400000).toISOString().slice(0,10)}" max="${todayKey()}" value="${config.date}"></label><p class="muted">Personal scores are saved on this device. Starting a challenge restarts any unfinished battle without rewards.</p>`,'daily','THE DAILY FRONT');
}

function finishDaily() {
  const result=recordDailyResult(progress.daily,dailyConfig,{won:battle.status==='won',time:battle.time,kills:battle.kills,matches,bestChain,playerHp:battle.playerHp,playerMax:battle.playerMax,events:eventCount});
  progress.daily=result.daily;progress.gold+=result.reward;persist();sound(result.won?'win':'lose');
  openModal(result.won?'Daily front secured.':'Another attempt awaits.',`<div class="result-icon medal-${result.medal}">${result.medal==='gold'?'✦':result.medal==='silver'?'◇':result.medal==='bronze'?'○':'♜'}</div><p class="result-intro">${dailyConfig.date} · ${result.medal==='none'?'Clear the battlefield to earn a medal.':`${result.medal.toUpperCase()} MEDAL`}${result.improved?' · New personal best!':''}</p><div class="result-stats"><div><b>${result.score.toLocaleString()}</b><small>SCORE</small></div><div><b>${bestChain}×</b><small>BEST CASCADE</small></div><div><b>+${result.reward}</b><small>GOLD</small></div></div><p>Campaign progress and mastery are unchanged. ${result.reward?'Today’s first-win reward has been saved.':'Replay to improve your score.'}</p><div class="modal-actions"><button class="primary-button" data-daily="${dailyConfig.date}">Try the same challenge →</button><button class="secondary-button" data-action="daily">Daily briefing</button><button class="secondary-button" data-action="campaign-return">Campaign</button></div>`,'result','THE DAILY FRONT');
}

async function shareDaily(date) {
  const url=new URL(location.href);url.search='';url.searchParams.set('daily',date);url.hash='';
  try{await navigator.clipboard.writeText(url.href);toast('Challenge link copied.');}catch{openModal('Share this battlefield.',`<p>Copy this link to share the same challenge and fixed army.</p><input class="share-link" value="${escape(url.href)}" readonly aria-label="Daily challenge link"><div class="modal-actions"><button class="primary-button" data-action="daily">Back to briefing</button></div>`,'daily');content.querySelector('input').select();}
}

function confirmRestart(stage=progress.stage) {
  if(!started || battle.status!=='playing' || battle.time<1){startBattle(stage);begin();return;}
  openModal(stage===progress.stage?'Restart this battle?':'Change your battlefield?',`<p>This will restart the battle. Your purchased talents, gold, and cleared campaign battles are saved. Unfinished battle rewards are earned only when you win or lose.</p><div class="modal-actions"><button class="primary-button" data-restart="${stage}">Start battle ${stage} →</button><button class="secondary-button" data-action="resume">Keep playing</button></div>`,'confirm','CAMPAIGN');
}

function startBattle(stage=progress.stage,challenge=null) {
  session++;dailyConfig=challenge;started=false;
  if(!challenge)progress.stage=clamp(stage,1,progress.unlocked);
  const current=challenge?.stage||progress.stage;
  board=new Board({events:true,stage:current,...(challenge?{rng:seededRng(challenge.boardSeed)}:{})});
  battle=new Battle({stage:current,upgrades:challenge?.upgrades||progress.upgrades,loadout:challenge?.loadout||progress.loadout,mastery:challenge?.mastery||progress.mastery,...(challenge?{rng:seededRng(challenge.combatSeed)}:{}),onEvent:combatEvent});
  selected=null;busy=false;resolved=false;charge=0;overdrive=0;matches=0;bestChain=1;summonCount=0;eventCount=0;particles=[];floats=[];
  tileNodes.clear();boardEl.replaceChildren();boardEl.setAttribute('aria-busy','false');renderBoard();setLane(1);
  $('stage-name').textContent=STAGES[current-1][0];
  $('stage-counter').textContent=challenge?`Daily · ${challenge.date}`:`Battle ${current} / ${TOTAL_STAGES}`;
  $('chapter-label').textContent=challenge?'THE DAILY FRONT · FIXED ARMY':`CHAPTER ${roman(getChapter(current).number)} · ${STAGES[current-1][2]}`;
  $('stage-details').textContent=`${challenge?'Shared challenge':`Battle ${current} / ${TOTAL_STAGES}`} · ${difficultyName(current)} · ${battle.totalWaves} waves${battle.enemyBarrier?' · Boss battle':''}`;
  document.body.style.setProperty('--chapter-color',getChapter(current).color);
  $('combo').textContent='×1';$('combo-label').textContent='COMBO POWER';
  $('board-message').textContent='Swap adjacent runes to summon troops.';
  makeRoster();paintScenery();persist();updateUI();drawBattle();
}

function showClassUnlocks(definitions) {
  const panel=document.createElement('div');
  panel.innerHTML=definitions.map(def=>`<div class="class-unlock">${classPortrait(def)}<div><span class="eyebrow">NEW CLASS UNLOCKED</span><h3>${def.name}</h3><p>${def.role} · ${def.spell.name}</p></div><button class="text-button" data-army="${def.key}">Choose class →</button></div>`).join('');
  content.prepend(panel);drawPortraits();
}

function showMasteryGains(awards,before) {
  const panel=document.createElement('div');panel.className='mastery-gains';
  panel.innerHTML=`<h3>Class mastery earned</h3>${awards.map(({classId,xp})=>{const record=progress.mastery[classId],oldRank=masteryRank(before[classId]?.xp||0),rank=masteryRank(record.xp);return `<span><b>${escape(CLASSES[classId].name)}</b> +${xp} XP${rank>oldRank?` · MASTERY ${rank}${oldRank<3&&rank>=3?' · SPECIALIZATIONS UNLOCKED':''}`:''}</span>`;}).join('')}<button class="text-button" data-action="army">Choose specializations →</button>`;
  content.append(panel);
}

function finishBattle() {
  if(resolved||busy||battle.status==='playing')return;
  if(dailyConfig){resolved=true;finishDaily();return;}
  resolved=true;const won=battle.status==='won',stage=progress.stage;
  const priorMastery=progress.mastery;
  const awards=calculateMasteryAwards(battle.classStats,{won,stage});
  progress.mastery=applyMasteryAwards(progress.mastery,awards);
  const firstClear=won&&!progress.completed.includes(stage);
  const previousUnlock=progress.unlocked;
  const reward=Math.round(battle.reward())+(firstClear?35:0);
  progress.gold+=reward;
  if(won){if(firstClear)progress.completed.push(stage);progress.unlocked=Math.min(TOTAL_STAGES,Math.max(progress.unlocked,stage+1));}
  const newClasses=won?Object.values(CLASSES).filter(def=>def.unlockLevel>previousUnlock&&def.unlockLevel<=progress.unlocked):[];
  persist();sound(won?'win':'lose');
  openModal(won?(stage===TOTAL_STAGES?'The final rift is sealed.':'The front is yours.'):'The line has fallen.',`<div class="result-icon">${won?'✦':'♜'}</div><p class="result-intro">${won?(stage===TOTAL_STAGES?`You defeated ${escape(getStage(stage).bossName)} and completed all ${TOTAL_STAGES} battles of the Runeborn campaign. Replay any chapter to master a different army.`:stage===12?'The Eclipse Lord has fallen, but more frontiers await. Battle 13 is now unlocked.':`${STAGES[stage-1][0]} is secure. Train your army and prepare for the next battle.`):'Scout the enemy elements, protect your ranged attackers, and spend your earned gold before trying again.'}</p><div class="result-stats"><div><b>+${reward}</b><small>GOLD${firstClear?' · FIRST CLEAR +35':''}</small></div><div><b>${battle.kills}</b><small>ENEMIES DEFEATED</small></div><div><b>${bestChain}×</b><small>BEST CASCADE</small></div></div><div class="modal-actions"><button class="primary-button" data-next="${won?Math.min(TOTAL_STAGES,stage+1):stage}">${won&&stage<TOTAL_STAGES?'Next battle':'Play again'} →</button><button class="secondary-button" data-action="forge">Upgrade forge · ◆ ${progress.gold}</button><button class="secondary-button" data-action="campaign">Campaign map</button></div>`,'result',won?'VICTORY':'REGROUP & RETURN');
  if(newClasses.length)showClassUnlocks(newClasses);
  if(awards.length)showMasteryGains(awards,priorMastery);
}

function cast(spell) {
  if(!started||!backdrop.hidden||battle.status!=='playing')return;
  if(battle.cast(spell,lane)){sound('spell');banner(`${{meteor:'Meteor strike',rally:'Rally the line',heal:'Renewal'}[spell]} · lane ${lane+1}`);updateUI();}
}

content.addEventListener('click',event=>{
  const button=event.target.closest('button');if(!button||button.disabled)return;
  if(button.dataset.buy){buyUpgrade(button.dataset.buy);return;}
  if(button.dataset.forge){showForge(button.dataset.forge);return;}
  if(button.dataset.guide){showGuide(button.dataset.guide);return;}
  if(button.dataset.army){showArmory(button.dataset.army);return;}
  if(button.dataset.equip){equipClass(button.dataset.equip);return;}
  if(button.dataset.specialize){equipSpecialization(button.dataset.classId,button.dataset.specialize);return;}
  if(button.dataset.daily){const config=createDaily(button.dataset.daily);if(config){startBattle(config.stage,config);begin();}return;}
  if(button.dataset.shareDaily){shareDaily(button.dataset.shareDaily);return;}
  if(button.dataset.chapter){showCampaign(+button.dataset.chapter);return;}
  if(button.dataset.stage){confirmRestart(+button.dataset.stage);return;}
  if(button.dataset.restart){startBattle(+button.dataset.restart);begin();return;}
  if(button.dataset.next){startBattle(+button.dataset.next);begin();return;}
  switch(button.dataset.action){
    case 'begin':begin();break;
    case 'resume':if(battle.status!=='playing'){closeModal();}else if(!started)begin();else dismissModal();break;
    case 'forge':showForge(button.dataset.tab||'command');break;
    case 'campaign':showCampaign();break;
    case 'guide':showGuide();break;
    case 'army':showArmory(button.dataset.key||'ember');break;
    case 'bonds':showBonds();break;
    case 'daily':showDaily(dailyConfig?.date||todayKey());break;
    case 'campaign-return':if(!dailyConfig){closeModal();}else{startBattle(progress.stage);begin();}break;
    case 'how':showHow();break;
    case 'restart':if(dailyConfig){showDaily(dailyConfig.date);}else confirmRestart();break;
  }
});
content.addEventListener('change',event=>{
  if(event.target.id==='daily-date'){showDaily(event.target.value);return;}
  if(event.target.id==='campaign-chapter'){showCampaign(+event.target.value);return;}
  if(event.target.id==='sound-setting'){progress.settings.sound=event.target.checked;persist();}
  if(event.target.id==='motion-setting'){progress.settings.reducedMotion=event.target.checked;persist();}
});
document.querySelectorAll('[data-lane]').forEach(button=>button.onclick=()=>setLane(+button.dataset.lane));
document.querySelectorAll('[data-spell]').forEach(button=>button.onclick=()=>cast(button.dataset.spell));
$('menu-button').onclick=showMenu;$('forge-button').onclick=()=>showForge();$('campaign-button').onclick=()=>showCampaign();
$('how-button').onclick=showHow;$('codex-button').onclick=()=>showGuide();$('hint-button').onclick=showHint;
$('army-button').onclick=()=>showArmory();$('synergy-button').onclick=showBonds;
$('daily-button').onclick=()=>showDaily();
canvas.addEventListener('click',event=>{const rect=canvas.getBoundingClientRect(),y=(event.clientY-rect.top)/rect.height*530;setLane(clamp(Math.round((y-220)/120),0,2));});

function updateUI() {
  $('player-hp').textContent=`${Math.ceil(battle.playerHp)} / ${battle.playerMax}`;
  $('enemy-hp').textContent=`${Math.ceil(battle.enemyHp)} / ${battle.enemyMax}${battle.enemyBarrier?' · WARD':''}`;
  $('player-health').style.width=`${100*battle.playerHp/battle.playerMax}%`;
  $('enemy-health').style.width=`${100*battle.enemyHp/battle.enemyMax}%`;
  $('wave').textContent=battle.wave?`WAVE ${battle.wave} / ${battle.totalWaves}`:'PREPARE';
  $('battle-time').textContent=`${String(Math.floor(battle.time/60)).padStart(2,'0')}:${String(Math.floor(battle.time%60)).padStart(2,'0')}`;
  const bonds=battle.activeCombos?.(lane)||[];
  $('formation-bonds').innerHTML=bonds.length?bonds.map(c=>`<span class="bond-chip" title="${escape(c.description)}">${c.name}</span>`).join(''):'<span class="bond-hint">Combine colors in this lane</span>';
  $('formation-lane').textContent=`LANE ${lane+1} BONDS`;
  const upcoming=battle.forecast().slice(0,3);
  $('forecast-list').innerHTML=upcoming.length?upcoming.map(e=>`<span class="forecast-token" title="${escape(e.name+' · '+e.role+' · Counter: '+ELEMENT_INFO[e.counter].name)}"><span style="color:${colors[e.element]}">●</span> L${e.lane+1} ${escape(e.name)} · ${Math.ceil(e.time)}s</span>`).join(''):'<span>All waves deployed. Hold the line.</span>';
  for(const spell of Object.keys(battle.cooldowns)){
    const button=$('spell-'+spell),cooldown=battle.cooldowns[spell];
    button.disabled=!started||cooldown>0||battle.status!=='playing';
    button.querySelector('b').textContent=cooldown>0?`${{meteor:'Meteor',rally:'Rally',heal:'Renew'}[spell]} · ${Math.ceil(cooldown)}s`:{meteor:'Meteor',rally:'Rally',heal:'Renew'}[spell];
    button.querySelector('.cooldown').style.transform=`scaleX(${clamp(cooldown/battle.spellDurations[spell],0,1)})`;
    button.setAttribute('aria-label',`${spell} in lane ${lane+1}${cooldown>0?', ready in '+Math.ceil(cooldown)+' seconds':', ready'}`);
  }
  document.querySelector('.board-panel').classList.toggle('overdrive',overdrive>0);
  $('charge-fill').style.width=`${overdrive>0?overdrive*10:Math.min(100,charge*2)}%`;
  $('charge-label').textContent=overdrive>0?'OVERDRIVE · +25% SUMMON POWER':'OVERDRIVE';
  $('charge-count').textContent=overdrive>0?`${Math.ceil(overdrive)}s`:`${Math.min(charge,50)} / 50`;
  if(!busy&&battle.status!=='playing')finishBattle();
}

function combatEvent(event) {
  if(event.type==='wave'){banner(`WAVE ${event.wave} / ${battle.totalWaves} · ${event.wave===battle.totalWaves&&battle.enemyBarrier?'The boss approaches.':'Watch all three lanes.'}`);sound('spell');}
  if(event.type==='damage'&&event.amount>=1&&floats.length<55)floats.push({x:event.x,y:laneY(event.lane)-67,text:String(Math.ceil(event.amount)),color:event.effective?'#ffe5a0':event.side==='player'?'#ffb5a7':'#e5eee2',life:.8});
  if(event.type==='heal'&&event.amount>=2&&floats.length<55)floats.push({x:event.x,y:laneY(event.lane)-70,text:'+'+Math.ceil(event.amount),color:'#b7efba',life:.8});
  if(event.type==='spawn'&&event.unit.boss)banner(`${event.unit.name} has entered the middle lane!`,4200);
  if(event.type==='spawn'&&event.unit.affix&&!event.unit.boss)banner(`${event.unit.tag||'CHAMPION'} · ${event.unit.name} · lane ${event.unit.lane+1}`);
  if(event.type==='boss-phase')banner(`${event.name} · SECOND WIND! Break the empowered formation.`,3600);
  if(event.type==='troop-spell'){
    spawnParticles(event.x,laneY(event.lane)-35,colors[event.key]||'#d9c2f2',5);
    if(floats.filter(f=>f.spell).length<9)floats.push({x:event.x,y:laneY(event.lane)-82,text:event.name,color:colors[event.key]||'#e3d3f7',life:1.2,spell:true});
    if(performance.now()-lastCastSound>400){lastCastSound=performance.now();sound('cast');}
  }
  if(event.type==='death'){spawnParticles(event.x,laneY(event.lane)-28,event.side==='enemy'?'#c9a7df':'#a5d5be',10);if(event.boss)banner('BOSS DEFEATED · The rift’s ward is broken.');}
}

function spawnParticles(x,y,color,count=8) {
  if(reduced())return;
  for(let i=0;i<count&&particles.length<160;i++)particles.push({x,y,vx:(Math.random()-.5)*90,vy:-20-Math.random()*70,life:.45+Math.random()*.3,color});
}

function makeRoster() {
  $('roster').innerHTML=ELEMENTS.map(k=>{const troop=activeTroop(k);return `<button class="troop-card" data-troop="${k}" style="--element:${colors[k]}">${classPortrait(troop)}<div><h3>${troop.name}</h3><p>${troop.role}</p><small>${troop.spell.name}</small></div></button>`;}).join('');
  $('roster').onclick=e=>{const button=e.target.closest('[data-troop]');if(button)troopGuide(button.dataset.troop);};
  drawPortraits();
}

function drawPortraits() {
  document.querySelectorAll('[data-portrait]').forEach(el=>{
    const paint=el.getContext('2d');paint.clearRect(0,0,el.width,el.height);
    const enemy=el.dataset.enemy?ENEMIES[el.dataset.enemy]:null;
    const def=enemy||(el.dataset.class?CLASSES[el.dataset.class]:activeTroop(el.dataset.portrait));
    const atlas=atlases[def?.spriteAtlas||'troops'],row=(def?.spriteRow??ELEMENTS.indexOf(el.dataset.portrait))%6,col=def?.spriteColumn??(enemy?4:0);
    if(atlas?.complete&&atlas.naturalWidth){const cell=atlas.naturalWidth/8;paint.imageSmoothingEnabled=true;paint.drawImage(atlas,col*cell,row*cell,cell,cell,0,0,el.width,el.height);}
  });
}
for(const atlas of Object.values(atlases)){
  atlas.onload=()=>{drawPortraits();drawBattle();};
  atlas.onerror=()=>toast('Sprite artwork could not load. Reload to restore the animation.');
}

// Paint the scenery once. Animated layers reuse this surface every frame.
const scenery=document.createElement('canvas');scenery.width=1000;scenery.height=530;
const bg=scenery.getContext('2d');
function paintScenery() {
  const palettes={
    forest:{sky:['#263849','#62766c','#9a9b76'],mountains:['#3b5057','#3a5351','#2e4948'],ground:['#395044','#273d36'],trees:'#1c3537',moon:'#d2c69b'},
    frost:{sky:['#26394c','#6c8f9c','#b8c5bc'],mountains:['#486879','#557b84','#3b626c'],ground:['#627d7a','#3d5b62'],trees:'#2e4f5b',moon:'#e0e9e0'},
    storm:{sky:['#252e4b','#586776','#9a9a86'],mountains:['#39475e','#344d61','#2c4458'],ground:['#3e555b','#263d4e'],trees:'#263c51',moon:'#ddd7b6'},
    ash:{sky:['#362f3b','#886c65','#c09b76'],mountains:['#5d4d54','#624d49','#483f41'],ground:['#635243','#3e3835'],trees:'#372e36',moon:'#e2ad7e'},
    sun:{sky:['#41414c','#998d6e','#dbc490'],mountains:['#6f6a5d','#7c7258','#5d6251'],ground:['#6e7052','#414d3e'],trees:'#3b4b42',moon:'#fff0b7'},
    void:{sky:['#24243d','#5b526f','#9c809c'],mountains:['#46435f','#534663','#373c54'],ground:['#484859','#2b3548'],trees:'#262c46',moon:'#d3b5e4'},
  };
  const palette=palettes[getChapter(battle.stage).theme]||palettes.forest;
  const sky=bg.createLinearGradient(0,0,0,300);sky.addColorStop(0,palette.sky[0]);sky.addColorStop(.65,palette.sky[1]);sky.addColorStop(1,palette.sky[2]);bg.fillStyle=sky;bg.fillRect(0,0,1000,530);
  bg.fillStyle=palette.moon;bg.beginPath();bg.arc(720,64,27,0,Math.PI*2);bg.fill();
  for(let layer=0;layer<3;layer++){
    bg.fillStyle=palette.mountains[layer];bg.beginPath();bg.moveTo(0,190+layer*30);
    for(let x=0;x<=1100;x+=70)bg.lineTo(x,100+layer*44+Math.sin(x*.008+layer*2)*25+Math.cos(x*.023)*15);
    bg.lineTo(1000,350);bg.lineTo(0,350);bg.fill();
  }
  const ground=bg.createLinearGradient(0,155,0,530);ground.addColorStop(0,palette.ground[0]);ground.addColorStop(1,palette.ground[1]);bg.fillStyle=ground;bg.fillRect(0,176,1000,354);
  const rand=(()=>{let n=7351;return()=>{n=(n*1664525+1013904223)>>>0;return n/4294967296;};})();
  for(let l=0;l<3;l++){
    const y=220+l*120;
    bg.fillStyle='#7a776033';bg.beginPath();bg.roundRect(45,y-16,910,44,20);bg.fill();
    bg.strokeStyle='#9c998131';bg.lineWidth=1;bg.beginPath();bg.moveTo(70,y+25);bg.lineTo(935,y+25);bg.stroke();
    for(let i=0;i<85;i++){const x=rand()*1000,sy=y-8+rand()*35;bg.fillStyle=rand()>.5?'#b7ac8133':'#172c2950';bg.fillRect(x,sy,3+rand()*8,1+rand()*3);}
  }
  for(let i=0;i<350;i++){
    const x=rand()*1000,y=175+rand()*355;
    if([220,340,460].some(l=>Math.abs(y-l)<24))continue;
    bg.strokeStyle=rand()>.5?'#8eab7155':'#1c302f99';bg.beginPath();bg.moveTo(x,y);bg.lineTo(x-2,y-4-rand()*6);bg.moveTo(x,y);bg.lineTo(x+4,y-3-rand()*5);bg.stroke();
  }
  // Distant ruins and forest silhouettes frame the play area.
  for(const x of [12,148,268,812,972]){
    bg.fillStyle=palette.trees;bg.fillRect(x,109,6,79);
    for(let i=0;i<3;i++){bg.beginPath();bg.moveTo(x-21+i*3,150-i*19);bg.lineTo(x+3,84-i*8);bg.lineTo(x+29-i*2,150-i*19);bg.fill();}
  }
  castle(bg,65,false);castle(bg,935,true);
  const vignette=bg.createRadialGradient(500,280,120,500,280,600);vignette.addColorStop(0,'#071a1400');vignette.addColorStop(1,'#071a1470');bg.fillStyle=vignette;bg.fillRect(0,0,1000,530);
}

function castle(p,x,enemy) {
  p.save();p.translate(x,0);p.fillStyle='#112a2a88';p.beginPath();p.ellipse(0,460,68,18,0,0,Math.PI*2);p.fill();
  const wall=p.createLinearGradient(-50,0,50,0);wall.addColorStop(0,enemy?'#393745':'#69746b');wall.addColorStop(.5,enemy?'#59515c':'#94a18d');wall.addColorStop(1,enemy?'#302e3c':'#4c655c');
  p.fillStyle=wall;p.fillRect(-40,235,80,211);p.fillRect(-52,196,25,255);p.fillRect(27,196,25,255);p.fillRect(-30,152,60,118);
  p.fillStyle=enemy?'#5b3f56':'#344d4f';for(const tx of [-52,27]){p.beginPath();p.moveTo(tx-6,196);p.lineTo(tx+12,164);p.lineTo(tx+31,196);p.fill();}
  p.beginPath();p.moveTo(-36,152);p.lineTo(0,108);p.lineTo(36,152);p.fill();
  p.strokeStyle='#162d2e55';for(let y=210;y<440;y+=20){p.beginPath();p.moveTo(-49,y);p.lineTo(49,y);p.stroke();}
  p.fillStyle='#142c2e';for(const y of [230,350,430]){p.beginPath();p.roundRect(-14,y-28,28,44,[14,14,0,0]);p.fill();}
  p.fillStyle=enemy?'#b480ae':'#b7d3ad';p.fillRect(-2,80,3,32);p.beginPath();p.moveTo(1,82);p.lineTo(28,86);p.lineTo(20,103);p.lineTo(1,99);p.fill();
  p.fillStyle=enemy?'#b58cbd':'#e4ce8c';p.fillRect(-15,164,30,42);p.fillStyle=enemy?'#5c3e63':'#657e65';p.font='25px serif';p.textAlign='center';p.fillText(enemy?'◇':'✧',0,194);
  p.restore();
}

function drawBattle() {
  if(!battle)return;
  ctx.clearRect(0,0,1000,canvas.height);ctx.drawImage(scenery,0,0,1000,canvas.height);
  const y=laneY(lane);
  ctx.fillStyle='#ead3930c';ctx.fillRect(110,y-20,780,46);ctx.strokeStyle='#f0d79855';ctx.setLineDash([8,10]);ctx.lineWidth=1;ctx.strokeRect(112,y-20,776,46);ctx.setLineDash([]);
  for(const corpse of battle.corpses||[]){
    ctx.save();ctx.globalAlpha=.5;ctx.strokeStyle='#a9a4bc';ctx.lineWidth=2;const cy=laneY(corpse.lane);ctx.beginPath();ctx.moveTo(corpse.x-7,cy-3);ctx.lineTo(corpse.x+7,cy-3);ctx.moveTo(corpse.x,cy-9);ctx.lineTo(corpse.x,cy+3);ctx.stroke();ctx.restore();
  }
  if(battle.enemyBarrier){ctx.strokeStyle='#c29cde66';ctx.lineWidth=3;ctx.beginPath();ctx.ellipse(930,325*canvas.height/530,56,137*canvas.height/530,0,0,Math.PI*2);ctx.stroke();}
  for(const effect of battle.effects)if(['summon','heal','rally','meteor'].includes(effect.type))drawEffect(effect);
  const units=[...battle.units].sort((a,b)=>a.lane-b.lane||a.x-b.x);
  for(const u of units)drawUnit(u);
  for(const p of battle.projectiles){
    const py=laneY(p.lane)-31;ctx.strokeStyle=p.color;ctx.lineWidth=p.element==='tide'?3:5;ctx.beginPath();ctx.moveTo(p.x-(p.side==='player'?13:-13),py);ctx.lineTo(p.x,py);ctx.stroke();ctx.fillStyle='#f4e8c0';ctx.beginPath();ctx.arc(p.x,py,p.element==='ember'?5:2.5,0,Math.PI*2);ctx.fill();
  }
  for(const effect of battle.effects)if(!['summon','heal','rally','meteor'].includes(effect.type))drawEffect(effect);
  for(const p of particles){ctx.globalAlpha=clamp(p.life*2,0,1);ctx.fillStyle=p.color;ctx.fillRect(p.x,p.y,3,3);}
  ctx.globalAlpha=1;ctx.textAlign='center';
  for(const f of floats){ctx.globalAlpha=clamp(f.life*2,0,1);ctx.font=f.spell?'bold 12px system-ui':'bold 16px system-ui';ctx.strokeStyle='#14282d';ctx.lineWidth=3;const fx=clamp(f.x,75,925);ctx.strokeText(f.text,fx,f.y);ctx.fillStyle=f.color;ctx.fillText(f.text,fx,f.y);}
  ctx.globalAlpha=1;
  // Small drifting motes give the empty front a little life.
  if(!reduced()){ctx.fillStyle='#e9ddb588';for(let i=0;i<9;i++){const x=(i*173+motionTime*(8+i%3))%1000,fy=(130+(i*47)%370)*canvas.height/530+Math.sin(motionTime+i)*8;ctx.globalAlpha=.25+Math.sin(motionTime*1.2+i)*.2;ctx.fillRect(x,fy,2,2);}ctx.globalAlpha=1;}
}

function drawUnit(u) {
  const baseline=laneY(u.lane)+((u.id%3)-1)*5-(u.trait.flying?10+Math.sin(motionTime*5+u.id)*3:0);
  const size=u.boss?112:u.elite?87:79;const walking=u.state==='walk';
  const frame=reduced()?0:u.casting?3:walking?Math.floor(u.anim*8)%3:u.anim<.3?3:1;
  const row=(u.spriteRow??TROOPS[u.key].spriteRow)%6,col=(u.spriteColumn??(u.side==='enemy'?4:0))+frame;
  const sheet=atlases[u.spriteAtlas||'troops'];
  drawCastingRune(ctx,u,baseline,motionTime,reduced());
  ctx.save();ctx.translate(u.x,baseline);ctx.fillStyle='#071f2366';ctx.beginPath();ctx.ellipse(0,1,size*.28,6,0,0,Math.PI*2);ctx.fill();
  if(u.buffs.includes('haste')){ctx.strokeStyle='#8ce2e888';ctx.lineWidth=2;for(let i=0;i<3;i++){ctx.beginPath();ctx.moveTo(-31, -20-i*10);ctx.lineTo(-44-i*4,-20-i*10);ctx.stroke();}}
  if(u.shield>0){ctx.strokeStyle='#f4d998aa';ctx.fillStyle='#e9d3900a';ctx.lineWidth=2;ctx.beginPath();ctx.ellipse(0,-size*.4,size*.38,size*.46,0,0,Math.PI*2);ctx.fill();ctx.stroke();}
  if(sheet?.complete&&sheet.naturalWidth){
    const cell=sheet.naturalWidth/8;ctx.save();if(u.side==='enemy')ctx.scale(-1,1);
    if(u.undead){ctx.shadowColor='#bd9de7';ctx.shadowBlur=9;}
    if(u.hitFlash>0&&!reduced())ctx.globalAlpha=.62;
    ctx.drawImage(sheet,col*cell,row*cell,cell,cell,-size/2,-size*.9,size,size);ctx.restore();
  }else{ctx.fillStyle=colors[u.key];ctx.beginPath();ctx.arc(0,-28,18,0,Math.PI*2);ctx.fill();ctx.fillStyle='#fff';ctx.font='16px serif';ctx.textAlign='center';ctx.fillText('✧',0,-23);}
  if(u.buffs.includes('fury')){ctx.fillStyle='#ffa36f';for(let i=0;i<3;i++){const a=motionTime*2+i*2;ctx.fillRect(Math.cos(a)*23,-30+Math.sin(a)*18,3,3);}}
  if(u.buffs.includes('vitality')){ctx.fillStyle='#b5e3a6';ctx.font='bold 13px system-ui';ctx.fillText('+',19,-size*.57);}
  if(u.statuses.burn){ctx.fillStyle='#ff9765';for(let i=0;i<3;i++){ctx.beginPath();ctx.moveTo(-12+i*10,-15);ctx.lineTo(-8+i*10,-30-Math.sin(motionTime*8+i)*4);ctx.lineTo(-4+i*10,-15);ctx.fill();}}
  if(u.statuses.poison){ctx.fillStyle='#9cd980';for(let i=0;i<3;i++){ctx.beginPath();ctx.arc(-13+i*12,-54,2,0,Math.PI*2);ctx.fill();}}
  if(u.statuses.slow){ctx.strokeStyle='#9be2ff99';ctx.beginPath();ctx.ellipse(0,-8,24,7,0,0,Math.PI*2);ctx.stroke();}
  if(u.statuses.root||u.statuses.freeze){ctx.strokeStyle=u.statuses.freeze?'#c4edff':'#a6d38c';ctx.lineWidth=2;ctx.beginPath();ctx.moveTo(-19,-7);ctx.lineTo(-11,-23);ctx.lineTo(0,-9);ctx.lineTo(13,-24);ctx.lineTo(20,-6);ctx.stroke();}
  if(u.statuses.curse){ctx.strokeStyle='#df9dee';ctx.lineWidth=1.5;ctx.beginPath();ctx.moveTo(0,-76);ctx.lineTo(8,-66);ctx.lineTo(0,-56);ctx.lineTo(-8,-66);ctx.closePath();ctx.stroke();}
  if(u.statuses.regeneration){ctx.fillStyle='#b1e8ac';ctx.font='bold 14px system-ui';ctx.fillText('+',-22,-48-Math.sin(motionTime*2)*5);}
  if(u.statuses.rally||u.elite){ctx.fillStyle='#fbe4a6';ctx.textAlign='center';ctx.font='14px serif';ctx.fillText(u.elite?'✦':'↑',0,-size*.93);}
  if(u.affix){ctx.strokeStyle=CHAMPION_AFFIXES[u.affix]?.color||'#f2c38d';ctx.lineWidth=2;ctx.beginPath();ctx.moveTo(-24,-size*.82);ctx.lineTo(-15,-size*.97);ctx.lineTo(0,-size*.86);ctx.lineTo(15,-size*.97);ctx.lineTo(24,-size*.82);ctx.stroke();}
  const hpWidth=u.boss?64:38,hpY=-size*.88;
  if(u.hp<u.maxHp||u.side==='enemy'||u.elite){ctx.fillStyle='#10212ac9';ctx.fillRect(-hpWidth/2,hpY,hpWidth,4);ctx.fillStyle=u.side==='enemy'?'#df9a98':'#a8d4b0';ctx.fillRect(-hpWidth/2,hpY,hpWidth*clamp(u.hp/u.maxHp,0,1),4);if(u.shield){ctx.fillStyle='#e9cf91';ctx.fillRect(-hpWidth/2,hpY-2,hpWidth*clamp(u.shield/u.maxHp,0,1),2);}}
  ctx.fillStyle=colors[u.key];ctx.beginPath();ctx.arc(0,8,3,0,Math.PI*2);ctx.fill();
  const unitSpell=u.spell||u.trait?.spell||(u.classId?CLASSES[u.classId]?.spell:null);
  if(unitSpell&&Number.isFinite(u.spellCooldown)){
    ctx.fillStyle='#0b1d2a';ctx.fillRect(-19,12,38,2);ctx.fillStyle=colors[u.key];ctx.fillRect(-19,12,38*clamp(1-u.spellCooldown/unitSpell.cooldown,0,1),2);
  }
  if(u.boss){ctx.font='bold 12px system-ui';ctx.textAlign='center';ctx.fillStyle='#f4e2b8';ctx.fillText(u.name,0,-size-5);}
  else if(u.side==='enemy'&&(u.trait.healer||u.trait.summoner||u.trait.armor>.15||u.trait.flying)){ctx.fillStyle='#e9e7d7';ctx.font='bold 12px system-ui';ctx.textAlign='center';ctx.fillText(u.trait.healer?'+':u.trait.summoner?'✧':u.trait.flying?'⌃':'◇',21,-51);}
  ctx.restore();
}

function drawEffect(e) {
  if(drawSpellEffect(ctx,e,laneY(e.lane),motionTime,reduced()))return;
  if(reduced()&&e.type!=='meteor')return;
  const y=laneY(e.lane),t=clamp(e.age/e.maxLife,0,1);
  ctx.save();ctx.globalAlpha=(1-t)*.8;ctx.strokeStyle=e.color;ctx.fillStyle=e.color;ctx.lineWidth=2;
  if(e.type==='lightning'){
    ctx.beginPath();ctx.moveTo(e.x,y-30);for(let i=1;i<=6;i++)ctx.lineTo(e.x+(e.toX-e.x)*i/6,y-30+(i%2?8:-8));ctx.stroke();
  }else if(e.type==='meteor'){
    for(let i=0;i<5;i++){const mx=200+i*140,my=y-150+t*140;ctx.lineWidth=7;ctx.beginPath();ctx.moveTo(mx-25,my-60);ctx.lineTo(mx,my);ctx.stroke();ctx.beginPath();ctx.arc(mx,my,9,0,Math.PI*2);ctx.fill();}
  }else if(e.type==='rally'||(e.type==='heal'&&e.x===400)){
    ctx.fillRect(120,y-60,760,82);ctx.font='32px serif';ctx.textAlign='center';for(let i=0;i<6;i++)ctx.fillText(e.type==='heal'?'+':'↑',180+i*130,y-10-t*20);
  }else if(e.type==='slash'){
    ctx.lineWidth=3;ctx.beginPath();ctx.arc(e.x,y-34,20+t*20,-1.1,1);ctx.stroke();
  }else{
    ctx.beginPath();ctx.ellipse(e.x,y-15,10+t*36,(e.type==='summon'?8:20)+t*15,0,0,Math.PI*2);ctx.stroke();
    if(e.type==='heal'){ctx.font='22px system-ui';ctx.textAlign='center';ctx.fillText('+',e.x,y-30-t*20);}
  }
  ctx.restore();
}

function frame(time) {
  const dt=Math.min(.05,Math.max(0,(time-lastFrame)/1000));lastFrame=time;
  const playing=started&&backdrop.hidden&&!document.hidden&&battle.status==='playing';
  if(playing){
    battle.update(dt);motionTime+=dt;overdrive=Math.max(0,overdrive-dt);
    for(const p of particles){p.x+=p.vx*dt;p.y+=p.vy*dt;p.vy+=100*dt;p.life-=dt;}
    for(const f of floats){f.y-=24*dt;f.life-=dt;}
    particles=particles.filter(p=>p.life>0);floats=floats.filter(f=>f.life>0);
  }
  uiTimer+=dt;const uiDue=uiTimer>.15;if(uiDue){uiTimer=0;updateUI();}
  if(!document.hidden&&(playing||uiDue))drawBattle();requestAnimationFrame(frame);
}

document.addEventListener('visibilitychange',()=>{lastFrame=performance.now();if(document.hidden&&started&&backdrop.hidden&&battle.status==='playing')showMenu();});
window.addEventListener('pagehide',persist);
makeRoster();startBattle(progress.completed.includes(progress.stage)&&progress.stage<progress.unlocked?progress.unlocked:progress.stage);showIntro();const linkedDaily=new URLSearchParams(location.search).get('daily');if(linkedDaily&&createDaily(linkedDaily))showDaily(linkedDaily);requestAnimationFrame(frame);
