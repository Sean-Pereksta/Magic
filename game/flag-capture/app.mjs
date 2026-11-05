import {FACTIONS, MAPS, createGame, unitDef, at, legalActions, applyAction, recruit, advance} from './engine.mjs';
import {CAMPAIGN, readProgress, completeLevel, createCampaignBattle} from './campaign.mjs';
import {actionColor, displayTargets} from './targeting.mjs';
import {parseBattleSave} from './save.mjs';
import {abilityIcon} from './icons.mjs';
import {createDraft,resizeDraft,renderDraft,applyDraftAction} from './draft-ui.mjs';

const $ = q => document.querySelector(q);
const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const roles = ['leader','scout','brute','mystic'];
const factionKeys = ['bloom','chaos','choir'];
const sigils = {bloom:'✧',chaos:'◈',choir:'◎'};
const traits = {bloom:'Living groves · healing · strange shortcuts',chaos:'Warp strikes · curses · beautiful mayhem',choir:'Healing · shields · celestial teamwork'};
const summaries = {bloom:'Curious cosmic creatures. Plant living light, leap through spores, and grow stronger together.',chaos:'Impossible beasts from a broken dream. Bend space, bite sideways, and make a glorious mess.',choir:'Wings, watchful rings, and living bells. Mend your allies and turn harmony into a weapon.'};
const flagSVG = '<svg viewBox="0 0 64 70" aria-hidden="true"><ellipse cx="31" cy="61" rx="19" ry="5" fill="#36534b" opacity=".25" stroke="none"/><path d="M22 60V11M23 13c13-9 18 10 30 1v26c-12 9-17-10-30-1"/><path d="M16 60h14" fill="none"/></svg>';
const STORAGE = 'riftbound-battle-v2';
let scene='setup', chosen='bloom', opponent='chaos', difficulty='easy', map='glade';
let setupMode='custom', battleMode='beacons', boardSize='9x7', terrain='balanced', campaignLevel=1, progress=readProgress(null);
let opponentMode='ai', activeSeat='player', seatSelections={player:null,ai:null}, seatModes={player:'auto',ai:'auto'};
let selectedSkillIndex=0, seatSkills={player:0,ai:0};
let startingMoney=24;
const draftCache=new Map();
const consumedPointers=new Map(),heldPointers=new Set();
let deferredEvents=[];
let countdownRemainingMs=0, countdownNumber=0;
const pointers=new Map();
let game=null, selectedId=null, mode='auto', paused=false, logOpen=false, inspectResult=false, saved=null;
let sound=false, audio=null, toastTimer=null, lastFrame=0, accumulated=0, lastReadySignature='';
const motionReduced=matchMedia('(prefers-reduced-motion: reduce)').matches;

function sprite(faction,role,cls='') {
  return `<span class="sprite ${role==='minion'?'minion':''} ${cls}" style="background-position:${roles.indexOf(role==='minion'?'scout':role)*100/3}% ${factionKeys.indexOf(faction)*50}%" aria-hidden="true"></span>`;
}
function coordinate(x,y){return `${String.fromCharCode(65+x)}${(game?.height||7)-y}`;}
function alive(side){return game.units.filter(u=>u.hp>0&&(!side||u.side===side));}
function localGame(){return game?.opponentMode==='local';}
function current(side=activeSeat){const id=localGame()?seatSelections[side]:selectedId;return game?.units.find(u=>u.id===id&&u.hp>0);}
function commandMode(side=activeSeat){return localGame()?seatModes[side]:mode;}
function abilities(u){const d=unitDef(u);return d.abilities||[d.ability];}
function skillIndex(side=activeSeat){return localGame()?seatSkills[side]:selectedSkillIndex;}
function selectedSkill(u,side=activeSeat){return abilities(u)[skillIndex(side)]||abilities(u)[0];}
function setMode(next,side=activeSeat){activeSeat=side;if(localGame())seatModes[side]=next;else mode=next;}
function selectUnit(unit){activeSeat=localGame()?unit.side:'player';selectedId=unit.id;mode='auto';if(localGame()){seatSelections[unit.side]=unit.id;seatModes[unit.side]='auto';}}
function remaining(u,special=false,index=0){return Math.max(0,(special?(u.abilityReadyAts?.[index]??(index===0?u.abilityReadyAt:0)):u.readyAt)-game.time);}
function cooldownLabel(value){return value>0?`${value.toFixed(1)}s`:'Ready';}
function announce(message){$('#announcer').textContent=message;}
function toast(message){$('#toast').textContent=message;$('#toast').classList.add('show');clearTimeout(toastTimer);toastTimer=setTimeout(()=>$('#toast').classList.remove('show'),2600);announce(message);}
function tone(type='move') {
  if(!sound)return;
  try{
    audio??=new AudioContext();audio.resume();
    const oscillator=audio.createOscillator(),gain=audio.createGain(),now=audio.currentTime;
    const f={move:340,damage:135,heal:680,capture:820,ability:530,shield:590,win:900,recruit:420}[type]||280;
    oscillator.type=type==='damage'?'triangle':'sine';oscillator.frequency.setValueAtTime(f,now);oscillator.frequency.exponentialRampToValueAtTime(f*1.4,now+.13);
    gain.gain.setValueAtTime(.045,now);gain.gain.exponentialRampToValueAtTime(.001,now+.2);oscillator.connect(gain).connect(audio.destination);oscillator.start(now);oscillator.stop(now+.21);
  }catch{/* Sound is optional when a browser has no audio output. */}
}
function persist(){
  if(!game)return;
  if(game.winner==='player'&&game.campaignId){progress=completeLevel(progress,game.campaignId);try{localStorage.setItem('riftbound-campaign-v1',JSON.stringify(progress));}catch{}}
  saved={version:2,game};
  try{localStorage.setItem(STORAGE,JSON.stringify(saved));}catch{/* Battle remains playable if storage is unavailable. */}
}
try{
  progress=readProgress(localStorage.getItem('riftbound-campaign-v1'));
  saved=parseBattleSave(localStorage.getItem(STORAGE));
}catch{/* An old or interrupted save starts at the council. */}

function factionSetupHTML(){
  return `<section class="setup"><div class="setup-intro"><div><span class="eyebrow">THREE WORLDS. ONE VERY STRANGE WAR.</span><h1>Choose your side<br>of the <em>impossible.</em></h1></div><p>Wild armies. Tiny battlefields. Big decisions.<br>Outsmart an AI commander, capture the rift flags, and let your weird side win.</p></div><div class="faction-grid" aria-label="Choose your faction">${factionKeys.map((key,i)=>{const f=FACTIONS[key];return `<button class="faction-card ${chosen===key?'selected':''}" style="--faction:${f.color}" data-faction="${key}" aria-pressed="${chosen===key}"><span class="choice-mark" aria-hidden="true">${chosen===key?'✓':''}</span><div class="faction-art">${sprite(key,'scout','art-left')}${sprite(key,'brute','art-right')}${sprite(key,'leader','art-main')}</div><div class="faction-content"><div class="faction-number">0${i+1} / ${key==='bloom'?'THE ALIENS':key==='chaos'?'THE CHAOS BEASTS':'THE CELESTIALS'}</div><h2>${f.name}</h2><p>${summaries[key]}</p><div class="faction-trait"><span>${sigils[key]}</span>${traits[key]}</div></div></button>`;}).join('')}</div><div class="setup-bottom"><div class="setting-fields"><label>${opponentMode==='local'?'Player 2 faction':'AI faction'}<select id="opponent">${factionKeys.map(k=>`<option value="${k}" ${opponent===k?'selected':''}>${FACTIONS[k].name}</option>`).join('')}</select></label><label>Challenge<select id="difficulty"><option value="story" ${difficulty==='story'?'selected':''}>Story · very gentle</option><option value="easy" ${difficulty==='easy'?'selected':''}>Relaxed · slow AI</option><option value="normal" ${difficulty==='normal'?'selected':''}>Balanced</option><option value="hard" ${difficulty==='hard'?'selected':''}>Tactician</option><option value="expert" ${difficulty==='expert'?'selected':''}>Riftlord</option></select></label><label>Battlefield<select id="map">${Object.values(MAPS).map(m=>`<option value="${m.key}" ${map===m.key?'selected':''}>${m.name}</option>`).join('')}</select></label></div><button class="primary start-button" data-command="start">Into the rift <span aria-hidden="true">→</span></button></div><div class="setup-foot"><span>Piece cooldowns · 3 capture flags · 12 unique troops · Pause anytime</span><span>YOUR NEXT BEAUTIFULLY WEIRD SKIRMISH</span></div>${saved?`<div class="resume-note">An expedition is waiting on this device.<button data-command="resume">Resume battle →</button></div>`:''}</section>`;
}
function councilTabs(){return `<nav class="council-tabs" aria-label="Battle type"><button data-setup="custom" class="${setupMode==='custom'?'active':''}">Custom skirmish</button><button data-setup="campaign" class="${setupMode==='campaign'?'active':''}">The Little Rift · Campaign <span>${progress.completed.length}/5</span></button></nav>`;}
function customSettingsHTML(){return `<div class="custom-settings"><div><span class="eyebrow">YOUR BATTLE, YOUR RULES</span><p>Fine-tune the chaos.</p></div><div class="setting-fields"><label>Opponent<select id="opponent-mode"><option value="ai" ${opponentMode==='ai'?'selected':''}>AI commander</option><option value="local" ${opponentMode==='local'?'selected':''}>2 players · same screen</option></select></label><label>Game mode<select id="battle-mode"><option value="beacons" ${battleMode==='beacons'?'selected':''}>Flag Capture</option><option value="control" ${battleMode==='control'?'selected':''}>Zone Control</option><option value="elimination" ${battleMode==='elimination'?'selected':''}>Last Army</option><option value="reinforcements" ${battleMode==='reinforcements'?'selected':''}>Reinforcement Clash</option></select></label><label>Starting money<select id="starting-money">${[12,18,24,30,40].map(n=>`<option value="${n}" ${startingMoney===n?'selected':''}>${n} coins</option>`).join('')}</select></label><label>Board size<select id="board-size"><option value="7x5" ${boardSize==='7x5'?'selected':''}>Pocket · 7 × 5</option><option value="9x7" ${boardSize==='9x7'?'selected':''}>Classic · 9 × 7</option><option value="11x9" ${boardSize==='11x9'?'selected':''}>Expanse · 11 × 9</option></select></label><label>Terrain<select id="terrain"><option value="sparse" ${terrain==='sparse'?'selected':''}>Open</option><option value="balanced" ${terrain==='balanced'?'selected':''}>Balanced</option><option value="wild" ${terrain==='wild'?'selected':''}>Wild</option></select></label></div></div>`;}
function campaignHTML(){
  const level=CAMPAIGN[campaignLevel-1]||CAMPAIGN[0];
  return `<section class="setup">${councilTabs()}<div class="setup-intro"><div><span class="eyebrow">A FIVE-BATTLE CAMPAIGN</span><h1>The <em>Little Rift.</em></h1></div><p>One curious expedition. Five increasingly strange battles. Learn each army's tricks as you put a broken little world back together.</p></div><div class="campaign-levels">${CAMPAIGN.map((l,i)=>{const locked=progress.unlocked<i+1,done=progress.completed.includes(l.id);return `<button class="level-card ${campaignLevel===i+1?'active':''} ${done?'complete':''}" data-level="${i+1}" ${locked?'disabled':''}><span>${locked?'⌑':done?'✓':String(i+1).padStart(2,'0')}</span><strong>${esc(l.title)}</strong><small>${locked?'Complete the previous battle':done?'Completed · replay anytime':esc(l.subtitle)}</small></button>`;}).join('')}</div><div class="campaign-briefing" style="--faction:${FACTIONS[level.playerFaction].color}"><div class="campaign-art">${sprite(level.playerFaction,'leader')}${sprite(level.aiFaction,'brute','rival')}</div><div><span class="eyebrow">EXPEDITION ${campaignLevel} / 5</span><h2>${esc(level.title)}</h2><p>${esc(level.briefing)}</p><div class="mission-tags"><span>${FACTIONS[level.playerFaction].name}</span><span>${level.mode==='control'?'Zone Control':level.mode==='elimination'?'Last Army':'Flag Capture'}</span><span>${level.width} × ${level.height}</span><span>${({story:'Story',easy:'Relaxed',normal:'Balanced',hard:'Tactician',expert:'Riftlord'})[level.difficulty]}</span></div><button class="primary" data-command="start">${progress.completed.includes(level.id)?'Replay expedition':'Begin expedition'} <span>→</span></button><p class="campaign-reward">${esc(level.rewardText)}</p></div></div>${saved?'<div class="resume-note">Your current battle is saved.<button data-command="resume">Resume battle →</button></div>':''}</section>`;
}
function draftFor(side){
  const campaign=setupMode==='campaign',level=CAMPAIGN[campaignLevel-1],size=boardSize.split('x').map(Number);
  const options={side,faction:campaign?(side==='player'?level.playerFaction:level.aiFaction):(side==='player'?chosen:opponent),width:campaign?level.width:size[0],height:campaign?level.height:size[1],money:campaign?24:startingMoney};
  const key=`${campaign?level.id:'custom'}:${side}`;
  if(!draftCache.has(key))draftCache.set(key,createDraft(options));else resizeDraft(draftCache.get(key),options);
  return draftCache.get(key);
}
function setupHTML(){
  if(setupMode==='campaign')return campaignHTML().replace('<div class="campaign-briefing"',`${renderDraft(draftFor('player'))}<div class="campaign-briefing"`);
  return factionSetupHTML().replace('<section class="setup">',`<section class="setup">${councilTabs()}`).replace('<div class="setup-bottom">',`${customSettingsHTML()}${renderDraft(draftFor('player'))}${opponentMode==='local'?renderDraft(draftFor('ai')):''}<div class="setup-bottom">`);
}
function terrainHTML(terrain,x,y){
  if(terrain==='rock')return '<svg class="terrain-art" viewBox="0 0 70 70" aria-hidden="true"><ellipse cx="35" cy="58" rx="28" ry="7" fill="#354f49" opacity=".4"/><path d="M9 49l9-26 22-12 22 17 3 24-29 9z" fill="#a2b2a1" stroke="#4c6c62" stroke-width="3"/><path d="M18 23l16 20 28-15M34 43l2 18" fill="none" stroke="#718e7c" stroke-width="3"/><path d="M22 23l16-6 12 9" fill="none" stroke="#c7d3b8" stroke-width="3"/></svg>';
  if(terrain==='grove')return '<svg class="terrain-art" viewBox="0 0 70 70" aria-hidden="true"><ellipse cx="35" cy="58" rx="28" ry="7" fill="#365e4a" opacity=".4"/><path d="M32 57V30m15 28V37M18 53V36" fill="none" stroke="#405d45" stroke-width="5"/><path d="M9 35c-4-12 8-25 15-15 1-17 18-20 22-4 14-4 22 15 12 23-3 8-17 8-22 1-8 9-19 3-19-1z" fill="#b1cb87" stroke="#527854" stroke-width="2.5"/><path d="M21 23l8 2m12-6l8 6m-9 8l8 2" stroke="#d6e5a8" stroke-width="4" stroke-linecap="round"/></svg>';
  if(terrain==='water')return '<svg class="terrain-art" viewBox="0 0 70 70" aria-hidden="true"><g fill="none" stroke="#b2dcd5" stroke-width="3" stroke-linecap="round"><path d="M8 24q9-7 18 0t18 0t18 0M13 40q9-7 18 0t18 0M8 54q9-7 18 0t18 0t18 0"/></g></svg>';
  return (x+y)%3===0?'<svg class="grass-tuft" viewBox="0 0 20 15" aria-hidden="true"><path d="M3 13l-1-5 6 5-2-11 7 10 3-6v8" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>':'';
}
function colorFor(action,u){
  return actionColor(action,u,unitDef);
}
function targetActions(side=activeSeat){
  const u=current(side),chosenMode=commandMode(side);
  if(!u||(!localGame()&&u.side!=='player')||game.winner)return [];
  const actions=legalActions(game,u.id).filter(a=>chosenMode==='auto'?['move','attack'].includes(a.type):a.type===chosenMode&&(chosenMode!=='ability'||(a.skillIndex||0)===skillIndex(side)));
  if(chosenMode!=='ability')return actions;
  return displayTargets(game,u,actions,unitDef);
}
function tileHTML(tile,hints){
  const {x,y,terrain}=tile,u=at(game,x,y),f=game.flags.find(f=>f.x===x&&f.y===y),action=hints.find(a=>(a.displayX??a.x)===x&&(a.displayY??a.y)===y);
  const d=u&&unitDef(u),color=action&&colorFor(action,game.units.find(v=>v.id===action.unitId));
  const zone=game.mode==='control'?game.flags.find(f=>Math.abs(f.x-x)+Math.abs(f.y-y)<=1):null;
  const isSelected=u&&(localGame()?Object.values(seatSelections).includes(u.id):selectedId===u.id);
  const classes=['tile',terrain,y===game.height-1?'home-player':y===0?'home-ai':'',u?'has-unit':'',u?.side==='ai'?'enemy-unit':'',u&&remaining(u)>0?'spent':'',isSelected?'selected':'',action?`hint-${color}`:'',zone?`zone-${zone.contested?'contested':zone.owner||'neutral'}`:''].filter(Boolean).join(' ');
  const label=[coordinate(x,y),terrain,u?`${u.side==='player'?'Player 1':localGame()?'Player 2':'Enemy'} ${d.name}, ${u.hp} of ${u.maxHp} health, ${cooldownLabel(remaining(u))}`:'',f?`${f.name} flag, ${f.contested?'contested':f.owner||'unclaimed'}`:'',zone?'Control zone':'',action?`${action.type==='ability'?abilities(game.units.find(v=>v.id===action.unitId))[action.skillIndex||0].name:action.type} target`:''].filter(Boolean).join(', ');
  return `<button class="${classes}" data-cell="${x},${y}" ${u?`data-unit="${u.id}"`:''} aria-label="${esc(label)}" ${selectedId===u?.id?'aria-pressed="true"':''}><span class="tile-coord" aria-hidden="true">${coordinate(x,y)}</span>${terrainHTML(terrain,x,y)}${f?`<span class="flag-marker ${f.owner||''}">${flagSVG}<span>${f.name.replace('light','').toUpperCase()}</span></span>`:''}${u?`<span class="unit-base" aria-hidden="true"></span>${sprite(u.faction,u.role)}${u.role==='leader'?'<span class="leader-crown" aria-hidden="true">★</span>':''}${u.shield?`<span class="shield-badge" aria-hidden="true">◇ ${u.shield}</span>`:''}<span class="unit-health" aria-hidden="true"><i style="width:${u.hp/u.maxHp*100}%"></i></span><span class="unit-cooldown" aria-hidden="true"><i style="width:${(1-Math.min(1,remaining(u)/d.moveCooldown))*100}%"></i></span>`:''}</button>`;
}
function resultHTML(){
  const won=game.winner==='player',draw=game.winner==='draw';
  const results=game.mode==='elimination'?{player:alive('player').reduce((n,u)=>n+u.hp,0),ai:alive('ai').reduce((n,u)=>n+u.hp,0)}:game.scores;
  const hp={player:alive('player').reduce((n,u)=>n+u.hp,0),ai:alive('ai').reduce((n,u)=>n+u.hp,0)};
  const outcome=game.time>=game.duration?(game.scores.player===game.scores.ai?`Time limit · surviving health ${hp.player} — ${hp.ai} HP`:'Time limit · higher light wins'):!alive('player').length||!alive('ai').length?'An entire army was defeated.':'The light goal was reached.';
  if(localGame())return `<dialog id="victory" class="guide-dialog result-card" aria-labelledby="result-title"><span class="eyebrow">LOCAL BATTLE COMPLETE</span>${sprite(game.factions[game.winner==='ai'?'ai':'player'],'leader')}<h2 id="result-title">${draw?'A perfect balance.':won?'Player 1 wins!':'Player 2 wins!'}</h2><p>${draw?'An even match between two strange armies.':FACTIONS[game.factions[game.winner]].name+' claim this rift.'}</p><p class="outcome-reason">${outcome}</p><div class="result-score">${results.player} — ${results.ai} ${game.mode==='elimination'?'HP':'light'}</div><button class="primary" data-command="council">New battle →</button><button class="quiet-button" data-command="inspect-result">Inspect battlefield</button></dialog>`;
  return `<dialog id="victory" class="guide-dialog result-card" aria-labelledby="result-title"><span class="eyebrow">EXPEDITION COMPLETE</span>${sprite(game.factions[won?'player':'ai'],'leader')}<h2 id="result-title">${draw?'A perfect balance.':won?'Beautifully played.':'The rift fights back.'}</h2><p>${draw?'Even impossible worlds sometimes agree to disagree.':won?`${FACTIONS[game.factions.player].name} have claimed this corner of the impossible.`:'Every strange army has another trick to learn. Try another faction or a new route through the flags.'}</p><p class="outcome-reason">${outcome}</p><div class="result-score">${results.player} <span class="muted">—</span> ${results.ai}${game.mode==='elimination'?' HP':' light'}</div><button class="primary" data-command="council">Another expedition →</button><button class="quiet-button" data-command="inspect-result">Inspect battlefield</button></dialog>`;
}
function abilityDockHTML(side){
  const u=current(side),d=u&&unitDef(u),all=u?abilities(u):[],selectedMode=commandMode(side),local=localGame(),keys=side==='ai'?['Q','W','E','R','F','T']:['1','2','3','4','5','G'],disabled=!u||game.winner||(!local&&u.side!=='player');
  const actionButton=(type,label,key)=>`<button class="dock-command ${type}-command ${selectedMode===type||(type==='move'&&selectedMode==='auto')?'active':''}" ${type==='guard'?'data-command="guard"':`data-mode="${type}"`} data-side="${side}" ${disabled?'disabled':''} aria-label="${label}">${abilityIcon(type)}<kbd>${key}</kbd><span>${label}</span><small data-command-timer="${type}">${u?cooldownLabel(remaining(u)):'—'}</small></button>`;
  return `<section class="command-dock icon-dock ${side==='ai'?'p2-dock':'p1-dock'} ${activeSeat===side?'active-seat':''}" data-side="${side}" aria-label="${local?(side==='player'?'Player 1':'Player 2'):'Your'} commands"><div class="dock-identity">${u?sprite(u.faction,u.role):'<span class="dock-empty">⌖</span>'}<div><span class="dock-player">${local?(side==='player'?'PLAYER 1':'PLAYER 2'):paused?'PAUSED · MENU TO RESUME':'YOUR COMMANDS'}</span><strong>${d?d.name:'Select a troop on the board'}</strong></div>${u?`<div class="dock-health-wrap"><span>${u.hp} / ${u.maxHp} HP</span><div class="dock-health" role="progressbar" aria-label="${d.name} health" aria-valuenow="${u.hp}" aria-valuemin="0" aria-valuemax="${u.maxHp}"><i style="width:${u.hp/u.maxHp*100}%"></i></div></div>`:''}</div><div class="dock-actions" style="grid-template-columns:repeat(${all.length+3},minmax(0,1fr))">${actionButton('move','Move',keys[0])}${actionButton('attack','Attack',keys[1])}${all.map((skill,index)=>`<button class="dock-command special-command skill-${colorFor({type:'ability',skillIndex:index},u)} ${selectedMode==='ability'&&skillIndex(side)===index?'active':''}" data-mode="ability" data-skill-index="${index}" data-side="${side}" ${disabled?'disabled':''} aria-label="${esc(skill.name)}. ${esc(skill.description)}" title="${esc(skill.name)} · ${esc(skill.description)} · ${skill.cooldown}s recharge">${abilityIcon(skill.kind)}<kbd>${keys[index+2]}</kbd><span>${esc(skill.name)}</span><small data-command-timer="ability" data-skill-index="${index}">${cooldownLabel(remaining(u,true,index))}</small><i class="skill-fill" data-skill-index="${index}" style="transform:scaleX(${1-Math.min(1,remaining(u,true,index)/skill.cooldown)})"></i></button>`).join('')}${actionButton('guard','Guard',keys[5])}</div></section>`;
}
function countdownHTML(){const count=Math.ceil(countdownRemainingMs/1000);return count>0?`<div class="battle-countdown" aria-live="assertive"><span>${count}</span><small>GET READY</small></div>`:'';}
function compactBattleHTML(){
  const local=localGame(),p=FACTIONS[game.factions.player],p2=FACTIONS[game.factions.ai],hints=targetActions();
  const scores=game.mode==='elimination'?{player:alive('player').reduce((n,u)=>n+u.hp,0),ai:alive('ai').reduce((n,u)=>n+u.hp,0)}:game.scores;
  return `<section class="compact-battle ${local?'local-battle':''}"><div class="compact-shell" style="--cols:${game.width};--rows:${game.height}">${local?abilityDockHTML('ai'):''}<div class="arena-panel"><div class="compact-scorebar"><div class="compact-army" style="--faction:${p.color}"><b>${scores.player}</b><span>${local?'P1 · ':''}${p.name}</span></div><div class="compact-clock"><strong id="clock">${formatTime(game.duration-game.time)}</strong><small>${game.mode==='elimination'?'ARMY HP':`GOAL ${game.goal}`}</small></div><div class="compact-army army-two" style="--faction:${p2.color}"><b>${scores.ai}</b><span>${local?'P2':'AI'} · ${p2.name}</span></div><button class="menu-button" data-command="menu" aria-label="Open game menu">☰ <span>Menu</span></button></div><div class="arena"><div class="board" style="--columns:${game.width};--rows:${game.height}" aria-label="Battlefield, ${game.width} columns and ${game.height} rows">${game.tiles.map(t=>tileHTML(t,hints)).join('')}</div>${countdownHTML()}</div></div>${abilityDockHTML('player')}<span class="sr-only" id="ready-count"></span>${game.winner&&!inspectResult?resultHTML():''}</div></section>`;
}
function menuHTML(){
  const local=localGame();
  return `<form method="dialog"><button class="close-button" aria-label="Close game menu">×</button></form><span class="eyebrow">${local?'TWO PLAYERS · SAME SCREEN':'SOLO EXPEDITION'}</span><h2>Battle menu</h2><p class="menu-battle-info">${game.campaignId?esc(CAMPAIGN.find(l=>l.id===game.campaignId)?.title):MAPS[game.map].name} · ${game.mode==='control'?'Zone Control':game.mode==='elimination'?'Last Army Standing':game.mode==='reinforcements'?'Reinforcement Clash':'Flag Capture'} · ${game.width} × ${game.height}</p><button class="primary" data-command="resume-live">${game.time===0?'Begin battle':'Resume battle'} <span>▶</span></button><div class="menu-actions"><button class="secondary" data-command="details" ${current()?'':'disabled'}>Creature details</button><button class="secondary" data-command="help">Field guide</button><button class="secondary" data-command="restart">New battle</button><button class="secondary" data-command="sound-toggle">Sound ${sound?'on':'off'}</button></div>${game.mode==='reinforcements'?`<details class="menu-reinforcements"><summary>Reinforcements · ${local?'both armies':`${game.supplies.player} coins`}</summary>${(local?['ai','player']:['player']).map(side=>{const faction=game.factions[side];return `<h3>${local?(side==='player'?'Player 1':'Player 2')+' · ':''}${FACTIONS[faction].name} <small>◇ ${game.supplies[side]} · ${alive(side).length}/8</small></h3><div class="recruits">${['scout','brute','mystic'].map(role=>{const d=FACTIONS[faction].units[role];return `<button class="recruit-button" data-recruit="${role}" data-side="${side}" ${game.winner||game.supplies[side]<d.cost||alive(side).length>=8?'disabled':''}>${sprite(faction,role)}<span><strong>${d.name}</strong><small>${role}</small></span><b>${d.cost} ◇</b></button>`;}).join('')}</div>`;}).join('')}</details>`:''}<details class="menu-journal"><summary>Battle journal</summary>${game.log.slice(0,12).map(t=>`<p>${esc(t)}</p>`).join('')}</details><p class="menu-shortcuts">Blue: move · Red: damage · Green: healing · Gold: support.<br>${local?'P1: 1 2 3 4 5 G · P2: Q W E R F T · Drag your own troops with separate fingers.':'Keys 1 2 3 4 5 G · Tap a troop, then a target. Dragging also works.'}<br>Space pauses both the battle and cooldowns.</p>`;
}
function creatureHTML(){
  const u=current();if(!u)return '<h2>Select a creature first.</h2>';
  const d=unitDef(u),f=FACTIONS[u.faction];
  return `<form method="dialog"><button class="close-button" aria-label="Close creature details">×</button></form><span class="eyebrow">${f.name}</span><div class="creature-heading">${sprite(u.faction,u.role,'portrait')}<div><h2>${d.name}</h2><p>${u.hp}/${u.maxHp} HP · ${d.damage} attack · ${d.range} range</p></div></div><p>${d.description}</p>${abilities(u).map(skill=>`<div class="ability-box ability-detail">${abilityIcon(skill.kind)}<div><h4>${skill.name}</h4><p>${skill.description} Recharges in ${skill.cooldown}s.</p></div></div>`).join('')}<h3>${d.movement}</h3><p>${d.movementDescription}</p><h3>Faction doctrine</h3><p>${f.passive}</p><button class="primary" data-command="resume-live">Resume battle ▶</button>`;
}
for(const [id,cls] of [['battle-menu','compact-menu'],['creature-info','creature-dialog']]){const el=document.createElement('dialog');el.id=id;el.className=`guide-dialog ${cls}`;document.body.append(el);}
function render(){
  const previous=new Map();
  if(!motionReduced&&scene==='battle')document.querySelectorAll('.tile[data-unit]').forEach(el=>{const r=el.querySelector('.sprite')?.getBoundingClientRect();if(r)previous.set(el.dataset.unit,r);});
  const active=document.activeElement,focusCell=active?.dataset.cell,focusMode=active?.dataset.mode,focusCommand=active?.dataset.command,focusSide=active?.dataset.side,focusSkill=active?.dataset.skillIndex;
  const details=$('.log-panel');if(details)logOpen=details.open;
  document.body.classList.toggle('playing',scene==='battle');
  $('#app').innerHTML=scene==='setup'?setupHTML():compactBattleHTML();
  if(scene==='battle'){
    if(focusCell)$(`[data-cell="${focusCell}"]`)?.focus({preventScroll:true});
    else if(focusMode)$(`[data-mode="${focusMode}"]${focusSide?`[data-side="${focusSide}"]`:''}${focusSkill!==undefined?`[data-skill-index="${focusSkill}"]`:''}`)?.focus({preventScroll:true});
    else if(focusCommand)$(`#app [data-command="${focusCommand}"]${focusSide?`[data-side="${focusSide}"]`:''}`)?.focus({preventScroll:true});
    if($('#victory'))$('#victory').showModal();
    if(!motionReduced)document.querySelectorAll('.tile[data-unit]').forEach(el=>{const image=el.querySelector('.sprite'),old=previous.get(el.dataset.unit);if(!old||!image)return;const r=image.getBoundingClientRect(),dx=old.x-r.x,dy=old.y-r.y;if(Math.abs(dx)+Math.abs(dy)>2&&Math.abs(dx)+Math.abs(dy)<1800){el.classList.add('moving');setTimeout(()=>el.classList.remove('moving'),390);image.animate([{transform:`translate(${dx}px,${dy}px)`},{transform:'translate(0,0)'}],{duration:370,easing:'cubic-bezier(.2,.7,.2,1)'});const ring=el.querySelector('.unit-base');ring?.animate([{transform:`translate(${dx}px,${dy}px)`},{transform:'translate(0,0)'}],{duration:370,easing:'ease-out'});}});
  }
  lastReadySignature=readySignature();
}
function formatTime(t){return `${Math.floor(Math.max(0,t)/60)}:${String(Math.floor(Math.max(0,t)%60)).padStart(2,'0')}`;}
function drawAttack(event){
  if(motionReduced)return;
  const source=game.units.find(u=>u.id===event.sourceId),from=source&&$(`[data-unit="${source.id}"]`),to=$(`[data-cell="${event.x},${event.y}"]`);
  if(!from||!to)return;
  const a=from.getBoundingClientRect(),b=to.getBoundingClientRect(),sx=a.x+a.width/2+scrollX,sy=a.y+a.height*.4+scrollY,tx=b.x+b.width/2+scrollX,ty=b.y+b.height*.4+scrollY;
  const bend=Math.min(55,Math.hypot(tx-sx,ty-sy)*.22),mx=(sx+tx)/2,my=(sy+ty)/2-bend;
  const path=source.faction==='chaos'?`M${sx},${sy} L${mx-8},${my+12} L${mx+9},${my-4} L${tx},${ty}`:`M${sx},${sy} Q${mx},${my} ${tx},${ty}`;
  const color=source.faction==='chaos'?'#ff9e9b':source.faction==='choir'?'#ffe6a0':'#97f0da';
  const pageWidth=document.documentElement.scrollWidth,pageHeight=document.documentElement.scrollHeight;
  const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.classList.add('combat-trace');svg.setAttribute('aria-hidden','true');svg.setAttribute('viewBox',`0 0 ${pageWidth} ${pageHeight}`);svg.style.width=`${pageWidth}px`;svg.style.height=`${pageHeight}px`;
  svg.innerHTML=`<path d="${path}" fill="none" stroke="#18373c" stroke-width="9" stroke-linecap="round"/><path class="attack-stroke" d="${path}" fill="none" stroke="${color}" stroke-width="5" stroke-linecap="round"/><circle r="7" fill="${color}" stroke="#effde0" stroke-width="2"><animateMotion dur=".3s" path="${path}" fill="freeze"/></circle><g class="impact-star" transform="translate(${tx} ${ty})"><path d="M0-20L5-7 16-13 9-1 21 5 7 7 7 20 0 11-11 19-7 5-21 1-9-4-14-17-3-10Z" fill="${color}" stroke="#18373c" stroke-width="2"/></g>`;
  document.body.append(svg);setTimeout(()=>svg.remove(),650);
}
function readySignature(){return !game?'':game.units.filter(u=>u.hp>0).map(u=>`${u.id}:${remaining(u)===0}:${abilities(u).map((_,i)=>remaining(u,true,i)===0).join(',')}:${u.rootedUntil>game.time}`).join('|');}
function updateTimers(){
  if(scene!=='battle'||!game)return;
  $('#clock').textContent=formatTime(game.duration-game.time);
  const ready=alive('player').filter(u=>remaining(u)===0).length;
  $('#ready-count').textContent=`${ready} troops ready · Next light pulse in ${Math.max(0,game.nextPulseAt-game.time).toFixed(1)}s`;
  for(const u of alive()){
    const tile=$(`[data-unit="${u.id}"]`);if(!tile)continue;
    const fill=tile.querySelector('.unit-cooldown i');if(fill)fill.style.width=`${(1-Math.min(1,remaining(u)/unitDef(u).moveCooldown))*100}%`;
  }
  document.querySelectorAll('.command-dock').forEach(dock=>{const u=current(dock.dataset.side);if(!u)return;dock.querySelectorAll('[data-command-timer]').forEach(el=>el.textContent=cooldownLabel(remaining(u,el.dataset.commandTimer==='ability',Number(el.dataset.skillIndex)||0)));dock.querySelectorAll('.skill-fill').forEach(fill=>{const index=Number(fill.dataset.skillIndex)||0;fill.style.transform=`scaleX(${1-Math.min(1,remaining(u,true,index)/abilities(u)[index].cooldown)})`;});});
}
function drawSpecial(event){
  const target=$(`[data-cell="${event.x},${event.y}"]`);if(!target)return;
  const caster=game.units.find(u=>u.id===(event.sourceId||event.unitId));
  const skill=caster&&abilities(caster)[event.skillIndex||0];
  const semantic=event.type==='heal'?'heal':event.type==='shield'?'support':event.type==='summon'?'support':caster?colorFor({type:'ability',skillIndex:event.skillIndex||0},caster):'support';
  const kind=event.type==='heal'?'nectar':event.type==='shield'?'sanctuary':event.type==='summon'?(event.kind||({bloom:'broodlings',chaos:'pocketmaws',choir:'bellmotes'})[caster?.faction]||'broodlings'):event.kind||skill?.kind||'sanctuary';
  const r=target.getBoundingClientRect(),size=Math.max(45,r.width*.85),centerX=r.x+r.width/2+scrollX,centerY=r.y+r.height*.45+scrollY;
  const effect=document.createElement('div');effect.className=`special-fx fx-${semantic} fx-${kind}`;effect.style.left=`${centerX}px`;effect.style.top=`${centerY}px`;effect.style.setProperty('--fx-size',`${size}px`);
  const sign=event.type==='heal'?'+':event.type==='shield'?'◇':'✧';
  effect.innerHTML=`<i class="fx-halo"></i><i class="fx-halo second"></i><span class="fx-glyph">${abilityIcon(kind)}</span>${Array.from({length:7},(_,i)=>`<b class="fx-particle" style="--angle:${i*360/7}deg;--delay:${i*.035}s">${sign}</b>`).join('')}`;
  if(motionReduced){effect.classList.add('fx-reduced');effect.querySelectorAll('.fx-particle,.fx-halo').forEach(e=>e.remove());}
  document.body.append(effect);setTimeout(()=>effect.remove(),1050);
}
function showEvents(events=[]){
  if(!events.length)return;
  const message=events.filter(e=>e.text).at(-1)?.text;if(message)announce(message);
  for(const e of events){
    if(e.type==='win')continue;
    const tile=$(`[data-cell="${e.x},${e.y}"]`);if(!tile)continue;
    if(['damage','heal','capture','shield','ability','recruit','summon'].includes(e.type)){
      const el=document.createElement('span');el.className=`effect ${e.type}`;el.textContent=e.type==='damage'?`−${e.value??''}`:e.type==='heal'?`+${e.value??''}`:e.type==='shield'?`◇ +${e.value??''}`:e.type==='capture'?'CLAIMED':e.type==='recruit'?'NEW!':'✦';tile.append(el);
      const burst=document.createElement('i');burst.className='burst';tile.append(burst);
      setTimeout(()=>{el.remove();burst.remove();},900);
    }
    if(['ability','damage'].includes(e.type)){tile.classList.add('flash');setTimeout(()=>tile.classList.remove('flash'),400);}
    if(e.type==='damage')drawAttack(e);
    if(['ability','heal','shield','summon'].includes(e.type))drawSpecial(e);
  }
  tone(events.find(e=>['damage','heal','capture','ability'].includes(e.type))?.type||events[0].type);
}
function perform(action){
  if(countdownRemainingMs>0){toast('The battle begins after the countdown.');return;}
  if(paused){toast('Resume the battle to issue this command · Space');return;}
  if(game?.winner)return;
  const result=applyAction(game,action);
  if(!result.ok){toast(result.error||'That command is not available yet.');return;}
  if(action.type==='ability')setMode('auto',game.units.find(u=>u.id===action.unitId)?.side||activeSeat);
  persist();render();showEvents(result.events);updateTimers();
}
function chooseMode(next,side=activeSeat,index=0){
  activeSeat=side;
  const u=current(side);if(!u||(!localGame()&&u.side!=='player')){toast('Select one of your troops first.');return;}
  if(next==='ability'&&!abilities(u)[index]){toast('This creature has one special ability.');return;}
  if(localGame())seatSkills[side]=index;else selectedSkillIndex=index;
  setMode(next,side);render();
  if(remaining(u,next==='ability',index)>0)toast(`${next==='ability'?abilities(u)[index].name:u.role==='scout'?'Scout':'Troop'} recharging · ${cooldownLabel(remaining(u,next==='ability',index))}`);
  else if(!targetActions().length)toast(next==='ability'?'No valid targets for that skill yet.':next==='attack'?'No enemy in attack range.':'No open squares in movement range.');
}
function start(){
  const level=CAMPAIGN[campaignLevel-1];
  const [width,height]=boardSize.split('x').map(Number);
  const draft=draftFor('player'),otherDraft=opponentMode==='local'&&setupMode!=='campaign'?draftFor('ai'):null;
  const draftOptions={startingMoney:draft.money,playerArmy:[...draft.army],playerDeployment:draft.positions.map(p=>({...p})),...(otherDraft?{aiArmy:[...otherDraft.army],aiDeployment:otherDraft.positions.map(p=>({...p}))}:{})};
  game=createGame(setupMode==='campaign'?{...createCampaignBattle(level.id),...draftOptions}:{playerFaction:chosen,aiFaction:opponent,difficulty,map,mode:battleMode,width,height,terrain,opponentMode,seed:Date.now(),...draftOptions});
  if(setupMode==='campaign')game.campaignId=level.id;
  scene='battle';activeSeat='player';seatSelections={player:game.units.find(u=>u.side==='player'&&u.role==='scout')?.id||null,ai:game.units.find(u=>u.side==='ai'&&u.role==='scout')?.id||null};selectedId=seatSelections.player;seatModes={player:'auto',ai:'auto'};mode='auto';deferredEvents=[];heldPointers.clear();pointers.clear();selectedSkillIndex=0;seatSkills={player:0,ai:0};paused=false;countdownRemainingMs=3000;countdownNumber=3;inspectResult=false;accumulated=0;persist();render();window.scrollTo({top:0,behavior:'instant'});announce('Get ready. Battle begins in three seconds.');
}
function council(){
  $('#restart').close();$('#victory')?.close();$('#battle-menu').close();$('#creature-info').close();paused=true;scene='setup';selectedId=null;render();window.scrollTo({top:0,behavior:'instant'});
}
document.addEventListener('click',event=>{
  if(event.target.closest('[data-cell]')&&(consumedPointers.get(event.pointerId)||0)>performance.now())return;
  const draftButton=event.target.closest('[data-draft-action]');
  if(draftButton){const data=draftButton.dataset,result=applyDraftAction(draftFor(data.side),{type:data.draftAction,role:data.role,index:data.index,x:data.x,y:data.y});if(!result.ok)toast(result.error);else render();return;}
  const tab=event.target.closest('[data-setup]');if(tab){setupMode=tab.dataset.setup;render();return;}
  const levelButton=event.target.closest('[data-level]');if(levelButton){campaignLevel=Number(levelButton.dataset.level);render();return;}
  const factionButton=event.target.closest('[data-faction]');
  if(factionButton){chosen=factionButton.dataset.faction;if(opponent===chosen)opponent=factionKeys.find(k=>k!==chosen);render();$(`[data-faction="${chosen}"]`).focus({preventScroll:true});return;}
  const commandButton=event.target.closest('[data-command]');
  if(commandButton){
    const c=commandButton.dataset.command;
    if(c==='menu'){paused=true;render();$('#battle-menu').innerHTML=menuHTML();$('#battle-menu').showModal();}
    if(c==='resume-live'){$('#battle-menu').close();$('#creature-info').close();paused=false;render();}
    if(c==='details'){$('#battle-menu').close();$('#creature-info').innerHTML=creatureHTML();$('#creature-info').showModal();}
    if(c==='sound-toggle'){$('#sound').click();$('#battle-menu').innerHTML=menuHTML();}
    if(c==='help'){$('#battle-menu').close();if(game&&scene==='battle'){paused=true;render();}$('#guide').showModal();}
    if(c==='start')start();
    if(c==='pause'){paused=!paused;persist();render();announce(paused?'Battle paused.':'Battle resumed.');}
    if(c==='restart'){$('#battle-menu').close();paused=true;render();$('#restart').showModal();}
    if(c==='council')council();
    if(c==='resume'&&saved){game=structuredClone(saved.game);chosen=game.factions.player;opponent=game.factions.ai;map=game.map;difficulty=game.difficulty;opponentMode=game.opponentMode;activeSeat='player';seatSelections={player:game.units.find(u=>u.side==='player'&&u.hp>0)?.id||null,ai:game.units.find(u=>u.side==='ai'&&u.hp>0)?.id||null};selectedId=seatSelections.player;seatModes={player:'auto',ai:'auto'};scene='battle';paused=false;countdownRemainingMs=game.winner?0:3000;countdownNumber=3;inspectResult=false;render();window.scrollTo({top:0,behavior:'instant'});}
    if(c==='inspect-result'){inspectResult=true;$('#victory').close();render();}
    if(c==='guard'){activeSeat=commandButton.dataset.side||activeSeat;const u=current();if(u&&(u.side==='player'||localGame()))perform({type:'guard',unitId:u.id,x:u.x,y:u.y});else toast('Select one of your troops first.');}
    return;
  }
  const modeButton=event.target.closest('[data-mode]');if(modeButton){chooseMode(modeButton.dataset.mode,modeButton.dataset.side||activeSeat,Number(modeButton.dataset.skillIndex)||0);return;}
  const recruitButton=event.target.closest('[data-recruit]');if(recruitButton){const result=recruit(game,recruitButton.dataset.recruit,recruitButton.dataset.side||'player');if(!result.ok)toast(result.error);else{persist();render();showEvents(result.events);if($('#battle-menu').open)$('#battle-menu').innerHTML=menuHTML();}return;}
  const cell=event.target.closest('[data-cell]');
  if(cell&&game){
    const [x,y]=cell.dataset.cell.split(',').map(Number),u=at(game,x,y);
    if(localGame()&&u&&u.side!==activeSeat&&commandMode()==='auto'){selectUnit(u);render();return;}
    const action=targetActions().find(a=>(a.displayX??a.x)===x&&(a.displayY??a.y)===y);
    if(action){const {displayX,displayY,...canonical}=action;perform(canonical);return;}
    if(u){selectUnit(u);render();announce(`${unitDef(u).name} selected. ${u.hp} health. ${u.side==='player'||localGame()?'Choose a blue square or red target.':'Enemy troop.'}`);}
    else if(selectedId&&!paused)toast(mode==='ability'?'Choose a highlighted skill target.':'Choose a blue move square or a red attack target.');
  }
});
document.addEventListener('change',event=>{if(event.target.id==='opponent'){opponent=event.target.value;render();}if(event.target.id==='difficulty')difficulty=event.target.value;if(event.target.id==='map')map=event.target.value;});
document.addEventListener('change',event=>{if(event.target.id==='battle-mode')battleMode=event.target.value;if(event.target.id==='board-size'){boardSize=event.target.value;render();}if(event.target.id==='terrain')terrain=event.target.value;if(event.target.id==='starting-money'){startingMoney=Number(event.target.value);render();}});
document.addEventListener('change',event=>{if(event.target.id==='opponent-mode'){opponentMode=event.target.value;render();}});
$('#sound').addEventListener('click',()=>{sound=!sound;$('#sound').textContent=sound?'Sound on':'Sound off';$('#sound').setAttribute('aria-pressed',sound);$('#sound').setAttribute('aria-label',`${sound?'Disable':'Enable'} sound`);if(sound)tone('capture');});
document.addEventListener('keydown',event=>{
  if(scene!=='battle'||event.repeat||event.ctrlKey||event.metaKey||event.altKey||document.querySelector('dialog[open]')||['INPUT','SELECT','TEXTAREA'].includes(event.target.tagName))return;
  if(event.code==='Space'){event.preventDefault();if(!game.winner){paused=!paused;render();announce(paused?'Battle paused.':'Battle resumed.');}return;}
  if(event.key==='Escape'){if(localGame())seatSelections[activeSeat]=null;selectedId=null;setMode('auto');render();return;}
  if(game.winner)return;
  const modes={'1':'move','2':'attack','3':'ability'};
  if(modes[event.key]){event.preventDefault();chooseMode(modes[event.key],'player');}
  if(event.key==='4'){event.preventDefault();chooseMode('ability','player',1);}
  if(event.key==='5'){event.preventDefault();chooseMode('ability','player',2);}
  if(event.key.toLowerCase()==='g'){event.preventDefault();activeSeat='player';const u=current();if(u?.side==='player')perform({type:'guard',unitId:u.id,x:u.x,y:u.y});}
  if(localGame()){
    const p2modes={q:'move',w:'attack',e:'ability'},key=event.key.toLowerCase();
    if(p2modes[key]){event.preventDefault();chooseMode(p2modes[key],'ai');}
    if(key==='r'){event.preventDefault();chooseMode('ability','ai',1);}
    if(key==='f'){event.preventDefault();chooseMode('ability','ai',2);}
    if(key==='t'){event.preventDefault();activeSeat='ai';const u=current('ai');if(u)perform({type:'guard',unitId:u.id,x:u.x,y:u.y});}
  }
});
document.addEventListener('pointerdown',event=>{
  heldPointers.add(event.pointerId);
  const tile=event.target.closest?.('.tile[data-unit]');if(!tile||scene!=='battle'||event.button!==0||game.winner)return;
  const u=game.units.find(v=>v.id===tile.dataset.unit&&v.hp>0);if(!u||(!localGame()&&u.side!=='player'))return;
  pointers.set(event.pointerId,{unitId:u.id,side:u.side,x:event.clientX,y:event.clientY,dragging:false});
});
document.addEventListener('pointermove',event=>{
  const p=pointers.get(event.pointerId);if(!p||p.dragging||Math.hypot(event.clientX-p.x,event.clientY-p.y)<9)return;
  p.dragging=true;const u=game.units.find(v=>v.id===p.unitId&&v.hp>0);if(!u)return;
  if(current(p.side)?.id!==u.id)selectUnit(u);else activeSeat=p.side;
  $('#app').setPointerCapture(event.pointerId);render();event.preventDefault();
},{passive:false});
document.addEventListener('pointerup',event=>{
  heldPointers.delete(event.pointerId);
  const p=pointers.get(event.pointerId);pointers.delete(event.pointerId);if(!p?.dragging)return;
  event.preventDefault();consumedPointers.set(event.pointerId,performance.now()+500);setTimeout(()=>consumedPointers.delete(event.pointerId),550);
  const board=$('.board');if(!board)return;const r=board.getBoundingClientRect(),gap=parseFloat(getComputedStyle(board).gap)||0;
  if(event.clientX<r.left||event.clientX>=r.right||event.clientY<r.top||event.clientY>=r.bottom)return;
  const x=Math.floor((event.clientX-r.left)/((r.width+gap)/game.width)),y=Math.floor((event.clientY-r.top)/((r.height+gap)/game.height));
  activeSeat=p.side;
  const action=targetActions(p.side).find(a=>a.unitId===p.unitId&&(a.displayX??a.x)===x&&(a.displayY??a.y)===y);
  if(action){const {displayX,displayY,...canonical}=action;perform(canonical);}else toast('Drag to a highlighted destination or target.');
},{passive:false});
document.addEventListener('pointercancel',event=>{pointers.delete(event.pointerId);heldPointers.delete(event.pointerId);});
document.addEventListener('visibilitychange',()=>{if(document.hidden&&game&&scene==='battle'){paused=true;persist();render();}});
window.addEventListener('pagehide',persist);
function frame(ms){
  const elapsedMs=lastFrame?Math.min(1000,ms-lastFrame):0,dt=elapsedMs/1000;lastFrame=ms;
  if(scene==='battle'&&game&&!paused&&!game.winner&&!document.hidden&&!document.querySelector('dialog[open]')){
    if(countdownRemainingMs){
      countdownRemainingMs=Math.max(0,countdownRemainingMs-elapsedMs);
      const count=Math.ceil(countdownRemainingMs/1000),countEl=$('.battle-countdown span');
      if(count>0){if(count!==countdownNumber){countdownNumber=count;if(countEl)countEl.textContent=count;announce(`Get ready. ${count}`);}requestAnimationFrame(frame);return;}
      countdownRemainingMs=0;countdownNumber=0;if(countEl){countEl.textContent='GO!';countEl.parentElement.classList.add('countdown-go');const overlay=countEl.parentElement;setTimeout(()=>overlay.remove(),450);}announce('Go! Capture the rift.');
    }
    accumulated+=dt;
    if(accumulated>=.1){
      const events=[];
      while(accumulated>=.1&&!game.winner){events.push(...advance(game,.1));accumulated-=.1;}
      if(events.length)deferredEvents.push(...events);
      if(!heldPointers.size&&(deferredEvents.length||readySignature()!==lastReadySignature)){if(!current()){if(localGame())seatSelections[activeSeat]=null;selectedId=null;}render();showEvents(deferredEvents);if(deferredEvents.length)persist();deferredEvents=[];}
      updateTimers();
    }
  }
  requestAnimationFrame(frame);
}
render();requestAnimationFrame(frame);
