import { SYMBOLS, RECIPES, DIFFICULTIES, createGame, availableRecipes, pursueRecipes, flipCard, grabCard, discardCard, cancelGrab, lockRecipe, botStep } from './engine.mjs';
import { cardImage, backImage } from './art.mjs';
import { createScene } from './scene.mjs';

const $ = id => document.getElementById(id);
const canvas = $('table');
const panels = ['intro', 'pause', 'results', 'book'];
const modeNames = {easy:'Hearthside', normal:'After Dark', hard:'Witching Hour'};
const touchMode=()=>window.matchMedia('(pointer:coarse)').matches||window.innerWidth<560;
let difficulty = 'normal', state = createGame({difficulty}), started = false;
let paused = true, overlay = 'intro', targetRecipe = null, targetPlayer = null;
let nextBots = [], pauseAt = Date.now(), lastFrame = 0, hover = null, showingResults = false;
let soundEnabled = false, audio = null, view, explicitRelease = false, wasLocked = false;
let renderStamp = '', lastHoverStamp = '', forecastChoice = null, resultsTimer = null;
const messages = [];

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}
function button(text, action, className='') {
  const node = el('button', className, text); node.type='button'; node.addEventListener('click', action); return node;
}
function releaseMouse() { if (document.pointerLockElement) { explicitRelease=true;document.exitPointerLock(); } }
function lockMouse() {
  if (paused || !started || targetRecipe || state.pendingGrab) return;
  try { const pending = canvas.requestPointerLock?.(); pending?.catch?.(() => {}); } catch {}
}
function showPanel(name) {
  overlay = name;
  if (name) {
    if (!paused) pauseAt = Date.now();
    paused = true; releaseMouse();
  } else {
    const delta = Date.now() - pauseAt;
    state.center.forEach(card => { if (card.faceUntil > pauseAt) card.faceUntil += delta; });
    nextBots = nextBots.map(time => time + delta);
    paused = false;
  }
  $('modal').hidden = !name;
  for (const id of panels) $(id).hidden = id !== name;
  $('aimHint').textContent = '';
  lastHoverStamp='';if(!name)onHover(hover);
}
function tell(message) {
  if (!message) return;
  messages.unshift(message); messages.length = Math.min(messages.length, 4);
  $('activity').replaceChildren(...messages.map(text => el('div', '', text)));
}
function playSound(kind='flip') {
  if (!soundEnabled) return;
  try {
    audio ||= new (window.AudioContext || window.webkitAudioContext)(); audio.resume();
    const start = audio.currentTime;
    const frequencies = kind === 'burn' ? [196, 294, 392, 587] : kind === 'swap' ? [260, 350] : [430];
    frequencies.forEach((f,i) => {
      const osc=audio.createOscillator(), gain=audio.createGain();
      osc.type=kind==='burn'?'triangle':'sine'; osc.frequency.setValueAtTime(f,start+i*.07);
      osc.frequency.exponentialRampToValueAtTime(f*.65,start+i*.07+.22);
      gain.gain.setValueAtTime(.0001,start+i*.07); gain.gain.exponentialRampToValueAtTime(.035,start+i*.07+.01);
      gain.gain.exponentialRampToValueAtTime(.0001,start+i*.07+.4);
      osc.connect(gain).connect(audio.destination); osc.start(start+i*.07); osc.stop(start+i*.07+.45);
    });
  } catch { soundEnabled=false; }
}
function act(result, quiet=false) {
  if (!result) return;
  if (!quiet || !result.ok) tell(result.message);
  if (result.events?.some(event=>event.type==='burn')) playSound('burn');
  else if (result.events?.some(event=>event.type==='swap')) playSound('swap');
  else if (result.events?.some(event=>event.type==='flip')) playSound('flip');
  for (const event of result.events||[]) {
    if (event.type==='burn' && event.player!==0) {
      const recipe=RECIPES.find(value=>value.id===event.recipe);
      tell(`${state.players[event.player].name} burned ${recipe?.name||'a recipe'}.`);
    }
    if (event.type==='scuttle') tell(`${state.players[event.target].name} lost ${event.cards.length} cards to the table.`);
    if (event.type==='blocked') tell(`${state.players[event.player].name}’s ward caught the spell.`);
    if (event.type==='reveal') tell('A glimpse of everything. Remember the table!');
    if (event.type==='trade') tell(`${state.players[event.player].name} borrowed a card from ${state.players[event.target].name}.`);
  }
  view?.sync(state, result.events||[]);
  render(true);
  if (state.status==='finished' && !showingResults) {
    showingResults=true;
    // Let the final cards finish burning before the results appear.
    const finishedState=state;resultsTimer=setTimeout(()=>{if(state===finishedState)showResults();},1250);
  }
}
function currentHints() {
  return pursueRecipes(state,0,DIFFICULTIES.find(value=>value.id===difficulty).hints);
}
function render(force=false) {
  const stamp=JSON.stringify([state.players.map(p=>[p.hand.map(c=>c.id),p.score,p.shield]),state.pendingGrab,targetRecipe?.id,forecastChoice,difficulty]);
  if (!force && stamp===renderStamp) return;
  renderStamp=stamp;
  $('scores').replaceChildren(...state.players.map((p,i)=> {
    const node=el('div','score'+(i===0?' you':''));
    node.append(el('span','',p.name),el('strong','',`${p.score}`));
    if (p.shield) node.append(el('span','shield','Warded'));
    return node;
  }));
  $('handCount').textContent=state.players[0].hand.length;
  document.body.classList.toggle('discarding',Boolean(state.pendingGrab));
  const hints=currentHints();
  const highlighted=hints.find(value=>value.recipe.id===forecastChoice)||hints[0];
  const keep=new Set(highlighted?.keep||[]);
  view?.setHandHints?.([...keep]);
  $('hand').replaceChildren(...state.players[0].hand.map((card,i)=> {
    const node=button('',()=>chooseHand(card.id),'handCard'+(keep.has(card.id)?' keep':''));
    node.style.backgroundImage=`url("${cardImage(card.type)}")`;
    node.style.setProperty('--tilt',`${(i-3)*2}deg`);
    node.title=`${i+1} · ${SYMBOLS.find(s=>s.id===card.type).name}${state.pendingGrab?' · return this card':''}`;
    node.setAttribute('aria-label',node.title);
    node.tabIndex=-1;
    node.append(el('span','num',`${i+1}`));
    return node;
  }));
  $('instruction').textContent=state.pendingGrab?(touchMode()?'Tap a hand card to return it':'Click a hand card or press 1–7 to return it · Esc cancels'):targetRecipe?'Choose a rival for your recipe':touchMode()?'Drag · look  /  Tap · flip  /  Hold · take':'Left · flip  /  Right · take  /  1–7 · return a hand card';
  $('difficultyLabel').textContent=`${modeNames[difficulty]} · First to ${state.goal}`;
  const forecast=$('forecast'); forecast.replaceChildren();
  if (hints.length) {
    forecast.append(el('div','forecastHeading','A LITTLE FORECAST'));
    for(const hint of hints) {
      const item=button('',()=>{forecastChoice=hint.recipe.id;render(true);},'forecastItem');
      item.append(el('b','',hint.recipe.name),el('small','',`${hint.owned}/${hint.recipe.cards.length} held · ${hint.recipe.points} embers${hint.recipe.effect!=='points'?' + '+effectName(hint.recipe.effect):''}`));
      const missing=el('div','needs','Find ');
      hint.missing.forEach(type=>{
        const img=el('img');img.src=cardImage(type);img.alt=type;img.title=type;missing.append(img);
      });
      item.append(missing); item.title=`${hint.recipe.description} Click to mark cards worth keeping.`;
      forecast.append(item);
    }
  }
  const ready=availableRecipes(state);
  $('castPanel').replaceChildren();
  if (ready.length) for(const recipe of ready.slice(0,2)) {
    const card=el('div','readyRecipe');
    card.append(el('div','eyebrow','RECIPE READY'),el('h3','',recipe.name),el('p','',recipe.description));
    const cast=button(`Lock in · ${recipe.points} embers${recipe===ready[0]?'  [R]':''}`,()=>beginCast(recipe));
    cast.disabled=Boolean(state.pendingGrab)||Boolean(targetRecipe)||state.status!=='playing';card.append(cast);$('castPanel').append(card);
  } else {
    const note=el('div','quietRecipe');note.append(el('b','','A good hand takes intention.'),el('span','','Find exact sets in the recipe book.'));$('castPanel').append(note);
  }
  if (state.pendingGrab) {
    $('castPanel').append(button('Cancel exchange',()=>{act(cancelGrab(state));},'cancelTarget'));
  }
  $('waitBtn').disabled=Boolean(state.pendingGrab)||Boolean(targetRecipe)||state.status!=='playing';
}
function effectName(effect) {
  return ({scuttle:'scuttle',trade:'trade',reveal:'reveal',ward:'ward',drain:'steal embers',gust:'scatter all',renew:'fresh table',feast:'shared feast'})[effect]||effect;
}
function chooseHand(id) {
  if (paused||targetRecipe||state.status!=='playing') return;
  if (state.pendingGrab) {
    act(discardCard(state,id));
    nextBots=nextBots.map(time=>Math.max(time,Date.now()+700));
  } else tell('Right click a center card to take it, then choose a card to return.');
}
function beginCast(recipe) {
  if (paused||state.pendingGrab||targetRecipe||state.status!=='playing') return;
  if (recipe.target==='player'||recipe.target==='card') {
    targetRecipe=recipe;targetPlayer=null;releaseMouse();renderTargets();render(true);
  } else act(lockRecipe(state,recipe.id));
}
function cancelTarget() { targetRecipe=null;targetPlayer=null;$('targetPanel').hidden=true;render(true); }
function selectTarget(player,card) {
  if (!targetRecipe || player===0) return;
  if (targetRecipe.target==='card' && !card) { targetPlayer=player;renderTargets();return; }
  const recipe=targetRecipe;cancelTarget();act(lockRecipe(state,recipe.id,{player,card}));
  nextBots=nextBots.map(time=>Math.max(time,Date.now()+700));
}
function renderTargets() {
  const node=$('targetPanel');node.hidden=false;node.replaceChildren();
  node.append(el('div','eyebrow','CHOOSE BEFORE YOU BURN'),el('h3','',targetRecipe.name));
  node.append(el('p','',targetPlayer===null?'Choose a creature at the table or use their name below.':'Choose one of their face-down cards. Its symbol is a secret.'));
  const choices=el('div','targetChoices');
  if (targetPlayer===null) {
    state.players.forEach((p,i)=>{if(i)choices.append(button(`${p.name} · ${p.score}${p.shield?' · Warded':''}`,()=>selectTarget(i)));});
  } else {
    state.players[targetPlayer].hand.forEach((card,i)=>{
      const b=button('',()=>selectTarget(targetPlayer,card.id),'targetCard');
      b.style.backgroundImage=`url("${backImage()}")`;b.title=`Hidden card ${i+1}`;b.setAttribute('aria-label',b.title);choices.append(b);
    });
    choices.append(button('Other creature',()=>{targetPlayer=null;renderTargets();}));
  }
  node.append(choices,button('Cancel · no cards will burn',cancelTarget,'cancelTarget'));
}
function interact(hit,mouseButton) {
  if(!started||paused||state.status!=='playing'||!hit)return;
  if(targetRecipe) {
    if(hit.player>0&&(hit.kind==='player'||hit.kind==='opponentCard')) selectTarget(hit.player,hit.kind==='opponentCard'?hit.id:undefined);
    return;
  }
  if(state.pendingGrab) { if(hit.kind==='hand')chooseHand(hit.id);else tell('Choose one of your seven cards below to complete the exchange.');return; }
  if(hit.kind==='center') {
    if(mouseButton===2) {act(grabCard(state,hit.id));if(state.pendingGrab)releaseMouse();}
    else if(mouseButton===0) act(flipCard(state,hit.id));
  } else if(hit.kind==='hand') tell('Right click a center card to exchange it with a hand card.');
}
function onHover(hit) {
  hover=hit;
  const stamp=JSON.stringify([hit?.kind,hit?.id,targetRecipe?.id,state.pendingGrab?.cardId,paused]);
  if(stamp===lastHoverStamp)return;lastHoverStamp=stamp;
  $('crosshair').classList.toggle('hot',Boolean(hit)&&!paused);
  let text='';
  if(!paused&&started) {
    if(state.pendingGrab) text=hit?.kind==='hand'?'Return this card':'Choose a hand card below';
    else if(targetRecipe&&hit?.player>0) text=`Choose ${state.players[hit.player].name}`;
    else if(hit?.kind==='center') text=touchMode()?'Tap · flip    Hold · take':'Left · flip    Right · take';
    else if(hit?.kind==='player') text=state.players[hit.player].name;
  }
  $('aimHint').textContent=text;
}
function openBook() {
  if(overlay==='book'){showPanel(null);return;}
  if(!started||state.status!=='playing')return;
  const ready=new Set(availableRecipes(state).map(recipe=>recipe.id));
  $('recipeList').replaceChildren(...RECIPES.map(recipe=>{
    const entry=el('article','recipe'+(ready.has(recipe.id)?' ready':''));
    const head=el('h3','',recipe.name);head.append(el('span','',`+${recipe.points}`));
    const symbols=el('div','recipeSymbols');
    recipe.cards.forEach(type=>{const img=el('img');img.src=cardImage(type);img.alt=type;img.title=type;symbols.append(img);});
    entry.append(head,symbols,el('div','recipeKind',recipe.effect==='points'?'Embers only':effectName(recipe.effect)),el('p','',recipe.description));
    if(ready.has(recipe.id))entry.append(button('Ready · return & cast',()=>{showPanel(null);beginCast(recipe);}));
    return entry;
  }));
  showPanel('book');
}
function showResults() {
  if(state.status!=='finished')return;
  const win=state.winners.includes(0), tied=state.winners.length>1;
  $('results').replaceChildren(el('div','eyebrow','THE LAST EMBER'),el('h2','',win?(tied?'A shared victory.':'The table is yours.'):`${state.players[state.winners[0]].name} wins.`));
  const ranking=[...state.players].sort((a,b)=>b.score-a.score);
  ranking.forEach(p=>$('results').append(el('p','',`${p.name} · ${p.score} embers`)));
  $('results').append(button('Another round',()=>newGame(false),'primary'),button('Change difficulty',()=>newGame(true)));
  showPanel('results');
}
function startGame() {
  clearTimeout(resultsTimer);resultsTimer=null;
  started=true;showingResults=false;targetRecipe=null;targetPlayer=null;$('targetPanel').hidden=true;
  state=createGame({difficulty});
  const setting=DIFFICULTIES.find(v=>v.id===difficulty);
  const now=Date.now();nextBots=[0,now+setting.thinkMs,now+setting.thinkMs*1.34,now+setting.thinkMs*1.67];
  messages.length=0;forecastChoice=null;renderStamp='';
  pauseAt=now;showPanel(null);view.sync(state);render(true);
  tell('Welcome to the Hollow Inn. Three embers in hand… find the fourth.');
  lockMouse();
}
function newGame(changeDifficulty=false) {
  clearTimeout(resultsTimer);resultsTimer=null;
  cancelTarget();if(state.pendingGrab)act(cancelGrab(state),true);
  if(changeDifficulty){started=false;showPanel('intro');}else startGame();
}
function wait() {
  if(paused||state.pendingGrab||targetRecipe||state.status!=='playing')return;
  const i=nextBots.slice(1).indexOf(Math.min(...nextBots.slice(1)))+1;
  act(botStep(state,i),true);tell(`${state.players[i].name} takes a turn.`);
  nextBots[i]=Date.now()+DIFFICULTIES.find(v=>v.id===difficulty).thinkMs;
}
function frame(now) {
  requestAnimationFrame(frame);
  if(now-lastFrame>1000/50) {
    lastFrame=now;view.tick(now);
    if(started&&!paused&&!state.pendingGrab&&!targetRecipe&&state.status==='playing') {
      const time=Date.now(),setting=DIFFICULTIES.find(v=>v.id===difficulty);
      for(let i=1;i<4;i++) if(time>=nextBots[i]) {
        act(botStep(state,i,time),true);nextBots[i]=time+setting.thinkMs;
        if(state.status!=='playing')break;
      }
    }
  }
}
function registerControls() {
  $('startBtn').addEventListener('click',startGame);
  document.querySelectorAll('[data-difficulty]').forEach(node=>node.addEventListener('click',()=>{
    difficulty=node.dataset.difficulty;
    document.querySelectorAll('[data-difficulty]').forEach(b=>{b.classList.toggle('selected',b===node);b.setAttribute('aria-pressed',String(b===node));});
  }));
  $('bookBtn').addEventListener('click',openBook);$('closeBook').addEventListener('click',()=>showPanel(null));
  $('pauseBtn').addEventListener('click',()=>{if(started)showPanel(overlay==='pause'?null:'pause');});
  $('resumeBtn').addEventListener('click',()=>{showPanel(null);lockMouse();});
  $('newBtn').addEventListener('click',()=>newGame(true));$('waitBtn').addEventListener('click',wait);
  $('soundBtn').addEventListener('click',()=>{soundEnabled=!soundEnabled;$('soundBtn').textContent=soundEnabled?'Sound on':'Sound off';$('soundBtn').setAttribute('aria-pressed',String(soundEnabled));playSound('flip');});
  $('fullscreenBtn').addEventListener('click',async()=>{
    try {
      if(document.fullscreenElement)await document.exitFullscreen();
      else if(document.documentElement.requestFullscreen)await document.documentElement.requestFullscreen();
      else if(document.documentElement.webkitRequestFullscreen)document.documentElement.webkitRequestFullscreen();
      else tell('Fullscreen is unavailable in this browser. Try opening the game in Chrome or Safari.');
    } catch { tell('This browser could not enter fullscreen. Open the game in its own browser tab to try again.'); }
  });
  document.addEventListener('fullscreenchange',()=>{
    const full=Boolean(document.fullscreenElement);$('fullscreenBtn').textContent=full?'Exit fullscreen':'Fullscreen';$('fullscreenBtn').setAttribute('aria-pressed',String(full));view.resize();
  });
  document.addEventListener('pointerlockchange',()=>{
    const locked=document.pointerLockElement===canvas;
    document.body.classList.toggle('cursor',!locked);view.setLooking(locked);
    if(wasLocked&&!locked&&!explicitRelease&&!paused&&!targetRecipe&&!state.pendingGrab&&state.status==='playing')showPanel('pause');
    explicitRelease=false;wasLocked=locked;
  });
  document.addEventListener('mousemove',event=>{
    if(document.pointerLockElement===canvas&&!paused)view.look(event.movementX,event.movementY);
  });
  canvas.addEventListener('contextmenu',event=>event.preventDefault());
  window.addEventListener('resize',()=>{view.resize();render(true);});
  document.addEventListener('keydown',event=>{
    if(event.repeat)return;
    const key=event.key.toLowerCase();
    if(key==='escape') {
      if(targetRecipe)cancelTarget();
      else if(state.pendingGrab)act(cancelGrab(state));
      else if(started&&state.status==='playing')showPanel(overlay?null:'pause');
    }
    if(key==='b'){event.preventDefault();openBook();}
    if(paused||!started)return;
    if(key==='e'){event.preventDefault();if(document.pointerLockElement)releaseMouse();else lockMouse();}
    if(key==='r'){const recipe=availableRecipes(state)[0];if(recipe)beginCast(recipe);}
    if(/^[1-7]$/.test(key)){event.preventDefault();const card=state.players[0].hand[Number(key)-1];if(card)chooseHand(card.id);}
    if(key===' '){event.preventDefault();wait();}
  });
  document.addEventListener('visibilitychange',()=>{if(document.hidden&&started&&!paused&&state.status==='playing')showPanel('pause');});
  // Touch alternatives use the same exchange/flip rules without requiring a right mouse button.
  let touchStart=null;
  canvas.addEventListener('touchstart',event=>{
    if(event.touches.length===1)touchStart={x:event.touches[0].clientX,y:event.touches[0].clientY,time:Date.now()};
  },{passive:true});
  canvas.addEventListener('touchmove',event=>{
    if(!touchStart||paused)return;
    const t=event.touches[0];view.look((t.clientX-touchStart.x)*.7,(t.clientY-touchStart.y)*.7);touchStart.x=t.clientX;touchStart.y=t.clientY;touchStart.moved=true;
  },{passive:true});
  canvas.addEventListener('touchend',()=>{
    if(touchStart&&!touchStart.moved)view.interactAt?.(touchStart.x,touchStart.y,Date.now()-touchStart.time>420?2:0);touchStart=null;
  });
}
try {
  if(!window.THREE)throw new Error('The local 3D library could not be loaded.');
  view=createScene(canvas,{onInteract:interact,onHover});view.sync(state);render(true);registerControls();requestAnimationFrame(frame);
} catch(error) {
  console.error(error);$('modal').hidden=false;panels.forEach(id=>$(id).hidden=true);$('fatal').hidden=false;$('fatal').textContent=`The table could not open: ${error.message}. Please reload in a browser with WebGL enabled.`;
}
