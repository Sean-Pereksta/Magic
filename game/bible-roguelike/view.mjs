import {createRules, refKey, displayRef, normalizeName, VERSION} from './core.mjs';
import {ABILITIES, VARIANTS, ROOM_NAMES, ROOM_HINTS} from './content.mjs';
import {createCoopStore} from './sync.mjs';

const ART = 'https://pub-15a649bcaae84f2ca6c610e6ef0dde51.r2.dev';
const escape = value => String(value ?? '').replace(/[&<>"']/g,c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const button = (text,action,disabled = false,extra = '') => `<button type="button" data-act="${action}" ${disabled ? 'disabled' : ''} ${extra}>${escape(text)}</button>`;
const randomId = () => globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random().toString(36).slice(2)}`;
function read(key,fallback) { try { return JSON.parse(localStorage.getItem(key)) ?? fallback; } catch { return fallback; } }

export function installRoguelike({root,verses,enemies,books,classes,legacyItems,legacyRelics,conceptKeys,conceptLabel,matchVerseConcept,classifyVerseKeys,username,db,api,getAuthUser,onExit,onMastery,onRunEnd}) {
  const rules = createRules({verses,enemies,books,classes,legacyItems,legacyRelics,conceptKeys,matchVerseConcept,classifyVerseKeys});
  const prefix = `bibleSolo:${normalizeName(username)}:`, saveKey = `${prefix}rpgRunSave:v4`;
  let activeGameId = '', startGeneration = 0;
  let state = null, me = 'p0', store = null, busy = false, connected = true, coop = false, running = false;
  let lastSeq = 0, lastRun = '', completedRun = '', selected = null, panel = '', previousFocus = null;
  let targetAction = null, saveWarning = false;
  const effectTimers = new Set();
  const seenMastery = new Set();
  const $ = id => root.querySelector(`#${id}`);
  const player = () => state?.players.find(p => p.id === me);
  function saveValue(key,value) {
    try { localStorage.setItem(key,JSON.stringify(value)); return true; }
    catch { if (!saveWarning) { saveWarning = true; notice('Storage is full or unavailable. This solo run cannot be saved on this device.'); } return false; }
  }
  function newRun() { return {id:randomId(),seed:crypto.getRandomValues(new Uint32Array(1))[0] || 17,now:Date.now()}; }
  root.innerHTML = `
    <div class="rq-shell">
      <header class="rq-head"><div><small id="rqZone">SCRIPTURE QUEST</small><strong id="rqFloor">Floor 1</strong></div><span id="rqGold" class="rq-gold">◉ 25</span><span id="rqConnection" class="rq-connection">Solo</span>${button('☰ Menu','menu')}</header>
      <div id="rqParty" class="rq-party" aria-label="Party health"></div>
      <section class="rq-arena" id="rqArena" aria-label="Battlefield">
        <div class="rq-enemy-hud"><div><strong id="rqEnemyName"></strong><span id="rqEnemyHP"></span></div><div class="rq-meter"><i id="rqEnemyBar"></i></div><small id="rqEnemyType"></small></div>
        <div id="rqWeaknesses" class="rq-weaknesses"></div>
        <div class="rq-actors"><div id="rqPlayerActors" class="rq-player-actors"></div><figure id="rqEnemyFigure" class="rq-enemy"><img id="rqEnemyArt" alt="Enemy"><figcaption id="rqIntent"></figcaption></figure></div>
        <div id="rqRoomPrompt" class="rq-room-prompt" hidden><strong id="rqRoomTitle"></strong><p id="rqRoomHint"></p>${button('Open room','room','', 'id="rqOpenRoom"')}</div>
        <div id="rqEffects" class="rq-effects" aria-hidden="true"></div>
      </section>
      <footer class="rq-controls">
        <div id="rqSelected" class="rq-selected">Match an enemy weakness to block all damage. Other verses trigger an attack.</div>
        <form id="rqAttackForm" class="rq-attack-row"><label class="rq-sr" for="rqReference">Bible reference</label><input id="rqReference" autocomplete="off" placeholder="John 3:16" inputmode="text"><button id="rqAttack" type="submit">Attack</button></form>
        <div class="rq-actions"><div id="rqHotbar" class="rq-hotbar"></div>${button('🎒','inventory',false,'aria-label="Inventory and relics"')}<button type="button" id="rqTrialButton" data-act="quiz" hidden>Trial</button>${button('↺','history',false,'aria-label="Recent combat history"')}</div>
        <div id="rqNotice" role="status" class="rq-notice"></div><div id="rqRecent" class="rq-recent" aria-live="polite"></div>
      </footer>
    </div>
    <dialog id="rqDialog" aria-labelledby="rqDialogTitle"><div class="rq-dialog-head"><h2 id="rqDialogTitle"></h2>${button('✕','close',false,'aria-label="Close panel"')}</div><div id="rqDialogBody" class="rq-dialog-body"></div><div id="rqDialogNotice" class="rq-notice" role="status"></div></dialog>`;
  const dialog = $('rqDialog');
  function notice(message) { $('rqNotice').textContent = message; $('rqDialogNotice').textContent = message; }
  function openPanel(name) {
    if (!state) {
      panel = 'connection'; $('rqDialogTitle').textContent = 'Connect to Scripture Quest';
      $('rqDialogBody').innerHTML = '<p>Your run has not loaded yet. Reconnect or return to the lobby.</p>' + button('Reconnect','reconnect') + button('Return to lobby','exit');
      if (!dialog.open) dialog.showModal(); return;
    }
    panel = name; previousFocus = document.activeElement;
    renderPanel();
    if (!dialog.open) dialog.showModal();
  }
  function closePanel() {
    panel = ''; dialog.close();
    if (previousFocus?.isConnected) previousFocus.focus();
  }
  dialog.addEventListener('cancel',() => { panel = ''; });
  dialog.addEventListener('click',event => { if (event.target === dialog) { const r = dialog.getBoundingClientRect(); if (event.clientX < r.left || event.clientX > r.right || event.clientY < r.top || event.clientY > r.bottom) closePanel(); } });
  function persist() { if (!coop && state) saveValue(saveKey,{version:VERSION,state,savedAt:Date.now()}); }
  function accept(next) {
    if (!running) return;
    if (state?.id === next.id && state.revision > next.revision) return;
    const changedRoom = state?.room?.id !== next.room.id || state?.phase !== next.phase;
    if (lastRun !== next.id) { lastSeq = next.eventSeq; lastRun = next.id; selected = null; clearEffects(); }
    state = next;
    const p = player();
    if (!p) { notice('You are spectating this run.'); render(); return; }
    for (const key of p.mastered) if (!seenMastery.has(key)) { seenMastery.add(key); const v = rules.verseMap.get(key); if (v) onMastery?.(displayRef(v)); }
    const fresh = state.events.filter(e => e.seq > lastSeq); lastSeq = state.eventSeq;
    persist(); render();
    // Render each committed answer exactly once, on its own player actor.
    for (const event of fresh.filter(event=>event.effect)) effect(event);
    if (state.phase === 'ended' && completedRun !== state.id) {
      completedRun = state.id; onRunEnd?.({id:state.id,score:state.score,floor:state.floor,level:p.level,date:new Date().toISOString(),coop});
    }
    if (changedRoom && $('rqChallengeRef')) $('rqChallengeRef').value = '';
    if (changedRoom && ['path','shop','rest','treasure','scripture','risk','ended'].includes(state.phase)) {
      if (fresh.some(e => e.effect === 'answer')) {
        // Keep the final answer visible before opening rewards or defeat.
        if (dialog.open) closePanel();
        $('rqRoomPrompt').hidden = true;
        const room = state.room.id, phase = state.phase, run = state.id;
        later(() => {
          if (running && state.id === run && state.room.id === room && state.phase === phase) {
            render(); if (!dialog.open) openPanel('room');
          }
        },2800);
      } else openPanel('room');
    }
    else if (changedRoom && panel === 'room') closePanel();
    else if (dialog.open) {
      if (panel === 'quiz' && !p.quiz) closePanel();
      else renderPanel();
    }
  }
  async function send(type,data = {}) {
    if (!state || busy || (coop && !connected)) { if (!connected) notice('Reconnecting — wait for the party to synchronize.'); return false; }
    const action = {id:randomId(),runId:state.id,roomId:state.room.id,playerId:me,now:Date.now(),type,...data};
    busy = true; notice(''); renderBusy();
    try {
      const next = coop ? await store.dispatch(action) : rules.reduce(state,action);
      accept(next);
      return true;
    } catch (error) { notice(error.message || 'Action could not be saved. Try again.'); return false; }
    finally { busy = false; renderBusy(); if (dialog.open) renderPanel(); }
  }
  function renderBusy() {
    const p = player(), canAttack = !!p && p.hp > 0 && state.phase === 'battle' && !busy && (!coop || connected);
    $('rqAttack').disabled = !canAttack;
    $('rqAttack').textContent = busy ? '…' : p?.hp <= 0 ? 'Downed' : 'Attack';
    root.classList.toggle('rq-pending',busy || (coop && !connected));
    $('rqHotbar').querySelectorAll('button').forEach(b => { b.disabled = !canAttack || b.dataset.available !== 'yes'; });
    $('rqConnection').textContent = coop ? connected ? '● Co-op' : '↻ Reconnecting' : 'Solo';
  }
  function image(el,src,alt,fallback) {
    if (el.dataset.src === src) return;
    el.dataset.src = src; el.alt = alt; el.style.visibility = '';
    el.onerror = () => { if (el.dataset.fallback !== src && fallback) { el.dataset.fallback = src; el.src = fallback; } else { el.onerror = null; el.style.visibility = 'hidden'; } };
    el.src = src;
  }
  function weaknessesVisible(p) { return state.enemy.revealed || (state.enemy.variant !== 'deceiver' && !p.relics.includes('testing-faith')); }
  function render() {
    if (!state) return;
    const p = player(), e = state.enemy, battle = state.phase === 'battle'; if (!p) return;
    $('rqFloor').textContent = `Floor ${state.floor}${e?.boss && battle ? ' · Boss' : e?.elite && battle ? ' · Elite' : ''}`;
    $('rqGold').textContent = `◉ ${p.gold}`;
    $('rqZone').textContent = ['THE WILDERNESS','THE KINGDOM ROADS',"THE PROPHETS’ WATCH",'THE GOSPEL WAY','THE EARLY CHURCH','THE LAST THINGS'][Math.min(5,Math.floor((state.floor-1)/5))];
    const party = $('rqParty'), signature = state.players.map(ally=>`${ally.id}:${ally.name}`).join('|');
    party.style.setProperty('--party-count',state.players.length);
    if (party.dataset.roster !== signature) {
      party.dataset.roster = signature;
      party.innerHTML = state.players.map(ally => `<div class="rq-party-member" data-player="${ally.id}"><div><b>${escape(ally.name)}</b><span class="rq-hp-value"></span></div><div class="rq-meter"><i></i></div><small></small></div>`).join('');
    }
    for (const ally of state.players) {
      const node = party.querySelector(`[data-player="${ally.id}"]`);
      node.classList.toggle('rq-me',ally.id===me); node.classList.toggle('rq-downed',ally.hp<=0);
      node.querySelector('.rq-hp-value').innerHTML = ally.hp<=0 ? 'Downed' : `♥ ${ally.hp}<span class="rq-max-hp">/${ally.maxHp}</span>`;
      node.querySelector('.rq-hp-value').setAttribute('aria-label',`${ally.hp} of ${ally.maxHp} health`);
      node.querySelector('.rq-meter i').style.width = `${Math.max(0,100*ally.hp/ally.maxHp)}%`;
      node.querySelector('small').textContent = [ally.shield ? `⛨ ${ally.shield}` : '',ally.poison ? `Poison ${ally.poison}` : '',ally.chill ? `Chill ${ally.chill}` : '',ally.guard ? 'Braced' : ''].filter(Boolean).join(' · ') || `Lv ${ally.level}`;
    }
    $('rqEnemyName').textContent = `${VARIANTS[e.variant].name} ${e.name}`.trim();
    $('rqEnemyHP').textContent = `${e.hp} / ${e.maxHp}`;
    $('rqEnemyBar').style.width = `${Math.max(0,100*e.hp/e.maxHp)}%`;
    $('rqEnemyType').textContent = `${e.archetype} · ${e.armor ? 'Armored · ' : ''}${e.silenced ? 'Special silenced · ' : ''}${e.turn % 3 === 2 ? 'Vulnerable: bonus damage now' : 'Correct verse = 0 damage'}`;
    $('rqWeaknesses').innerHTML = (weaknessesVisible(p) ? e.weaknesses : e.weaknesses.filter(w => p.battle.discovered?.includes(w.concept))).slice(0,6).map(w => `<span>${escape(conceptLabel(w.concept))} ×${w.multiplier.toFixed(2)}</span>`).join('') || '<span>Weaknesses hidden · use Discernment</span>';
    $('rqEnemyFigure').style.setProperty('--variant',VARIANTS[e.variant].color);
    $('rqEnemyFigure').dataset.variant = e.variant;
    $('rqIntent').textContent = VARIANTS[e.variant].hint;
    const actors = $('rqPlayerActors');
    $('rqArena').classList.toggle('rq-coop-arena',state.players.length > 1);
    actors.style.setProperty('--party-count',state.players.length);
    if (actors.dataset.roster !== signature) {
      actors.dataset.roster = signature;
      actors.innerHTML = state.players.map(ally => `<figure class="rq-player" data-actor="${ally.id}"><div class="rq-answer-slot" aria-live="polite"></div><img alt=""><figcaption></figcaption></figure>`).join('');
    }
    for (const ally of state.players) {
      const actor = actors.querySelector(`[data-actor="${ally.id}"]`);
      actor.classList.toggle('rq-downed',ally.hp <= 0);
      actor.querySelector('figcaption').textContent = state.players.length > 1 ? ally.name : classes[ally.classId]?.name || 'Word Warden';
      image(actor.querySelector('img'),`${ART}/players/${classes[ally.classId]?.art || 'wordwarden'}.png`,`${ally.name} · ${classes[ally.classId]?.name || 'Word Warden'}`,`${ART}/players/wordwarden.png`);
    }
    image($('rqEnemyArt'),`${ART}/enemies/${e.art || e.id}.png`,e.name,`${ART}/enemies/dread-wraith.png`);
    $('rqArena').classList.toggle('rq-between',!battle);
    $('rqRoomPrompt').hidden = battle || state.events.some(e => e.effect === 'answer' && e.expiresAt > Date.now());
    $('rqRoomTitle').textContent = state.phase === 'path' ? 'The road divides' : state.phase === 'ended' ? 'The run is complete' : ROOM_NAMES[state.phase];
    $('rqRoomHint').textContent = state.phase === 'path' ? 'Choose your next destination.' : state.phase === 'ended' ? `Score ${state.score} · ${state.bosses} bosses defeated` : p.roomDone ? 'Ready. Open the room to continue when teammates finish.' : ROOM_HINTS[state.phase];
    const context = [];
    if (p.inspiration) context.push(`${conceptLabel(p.inspiration.category)} +30% (${p.inspiration.turns} verses)`);
    if (p.challenge) context.push(p.challengeBroken ? 'Challenge lost: healing received' : 'Challenge: no healing · +40% gold');
    if (selected && !rules.verseMap.has(refKey(selected))) selected = null;
    $('rqSelected').textContent = selected ? `${displayRef(selected)} — ${selected.text}` : context.join(' · ') || (p.hp <= 0 ? 'Downed — wait for a teammate’s revival or victory.' : 'Match a weakness: correct = protected; wrong = enemy attack.');
    $('rqSelected').title = [selected?.text,...context].filter(Boolean).join('\n');
    $('rqHotbar').innerHTML = p.equipped.map((id,index) => {
      const a = ABILITIES[id], remaining = Math.max(0,(p.cooldowns[id] || 0) - p.turn);
      const spent = id === 'recall' ? p.battle.recallUsed : id === 'discernment' ? p.battle.discernmentUsed : id === 'challenge' ? p.challenge || p.battle.attacks > 0 : false;
      return button(`${a.icon} ${a.name}${remaining ? ` · ${remaining}` : spent ? ' · ✓' : ''}`,'ability',remaining > 0 || spent,`data-id="${id}" data-available="${!remaining && !spent ? 'yes' : 'no'}" title="${escape(a.desc)} · shortcut ${index+1}"`);
    }).join('');
    $('rqTrialButton').hidden = !p.quiz || !battle;
    $('rqRecent').innerHTML = state.events.slice(-2).map(event => `<div>${escape(event.text)}</div>`).join('');
    renderBusy();
  }
  function card(title,description,actions = '',extra = '') { return `<article class="rq-card" ${extra}><strong>${escape(title)}</strong><p>${escape(description)}</p>${actions}</article>`; }
  function renderPanel() {
    const p = player(); if (!p) return;
    const body = $('rqDialogBody'), scroll = body.scrollTop;
    const focused = document.activeElement, focusedId = body.contains(focused) && focused.id;
    const draft = $('rqChallengeRef')?.value;
    const titles = {menu:'Your journey',inventory:'Inventory & relics',abilities:'Three active slots',modifiers:'Run modifiers',history:'Recent combat history',room:'Choose your path',quiz:'Scripture trial',target:'Choose a target',class:'Choose your calling',scores:'Highscores',mastery:'Mastery library'};
    $('rqDialogTitle').textContent = titles[panel] || 'Scripture Quest';
    let html = '';
    if (panel === 'menu') {
      html = `<p>Level ${p.level} · ${p.xp}/${24+(p.level-1)*13} XP · Score ${state.score} · ${p.mastered.length} verses</p><div class="rq-card-grid">${button('Inventory & relics','inventory')}${button('Active abilities','abilities')}${button('Run modifiers','modifiers')}${button('Combat history','history')}${button('Highscores','scores')}${button('Mastery library','mastery')}${!coop ? button('Start a fresh solo run','restart') : ''}${button('Calling & character','class')}${button('Room / path','room',state.phase === 'battle')}${coop ? button('Reconnect','reconnect') : ''}${button('Return to lobby','exit')}</div><p class="rq-help">${coop ? 'The shared run saves after each committed action. Reopen this lobby to reconnect. A disconnected player does not block room progression after 60 seconds.' : 'Your run saves automatically on this device, including rooms and shops.'}</p><details><summary>How to play</summary><p>Choose a verse → submit. A verse matching any enemy weakness is correct and blocks the entire enemy response, including poison and boss attacks. Unmatched or resisted verses trigger the normal enemy response. Each verse is shared once per encounter. Weaknesses add damage; resisted verses still hit. Active abilities recharge through your successful verse attacks. Defeating an enemy revives downed teammates. If everyone falls, the run ends.</p><p>Any living teammate may choose the next path. Shops and rewards belong to each player. Choose Continue when finished. Use keys 1–3 for abilities outside text fields.</p></details><label class="rq-toggle"><input type="checkbox" id="rqReduceMotion" ${read(`${prefix}reduceMotion`,false) ? 'checked' : ''}> Reduce combat motion</label>`;
    } else if (panel === 'inventory') {
      const counts = new Map(); p.inventory.forEach(id => counts.set(id,(counts.get(id) || 0) + 1));
      html = `<p>◉ ${p.gold} gold · ${p.shield} shield</p><div class="rq-card-grid">${[...counts].map(([id,count]) => card(`${rules.items[id].icon} ${rules.items[id].name} ×${count}`,rules.items[id].desc,button('Use','item',busy || p.hp <= 0,`data-id="${id}"`))).join('') || '<p>No consumables yet. Battles and merchants provide more.</p>'}</div><h3>Relics</h3><div class="rq-card-grid">${p.relics.map(id => card(`${rules.relics[id].icon} ${rules.relics[id].name}`,rules.relics[id].desc,id === 'verse-insight' ? button('Gain 12 shield','insight',state.phase !== 'battle' || p.battle.insightUsed) : '')).join('') || '<p>No relics yet. Elite victories guarantee one.</p>'}</div>${p.quiz ? button('Take knowledge trial','quiz') : ''}`;
    } else if (panel === 'abilities') {
      html = `<p>Three equipped abilities. Change slots between encounters. Cooldowns count your successful verses.</p><div class="rq-card-grid">${p.abilities.map(id => card(`${ABILITIES[id].icon} ${ABILITIES[id].name}${p.upgrades[id] ? ` +${p.upgrades[id]}` : ''}`,ABILITIES[id].desc + (p.upgrades[id] ? id === 'second-wind' ? ` Upgraded healing: ${18+4*p.upgrades[id]}%.` : ` Recharge: ${Math.max(2,ABILITIES[id].cooldown-p.upgrades[id])} verses.` : ''),[0,1,2].map(slot => button(`Slot ${slot+1}${p.equipped[slot] === id ? ' ✓' : ''}`,'equip',state.phase === 'battle' || busy,`data-id="${id}" data-slot="${slot}"`)).join(''))).join('')}</div>`;
    } else if (panel === 'modifiers') {
      html = `<div class="rq-card-grid">${p.relics.filter(id => rules.relics[id].risk || id === 'many-books').map(id => card(rules.relics[id].name,rules.relics[id].desc)).join('') || '<p>No risk modifiers. Try a risk/reward room or Trial merchant.</p>'}</div><p>Enemy health: ${state.players.length} player${state.players.length === 1 ? '' : 's'} · roster locked for this run. Abilities, gold, health and relics belong to each player.</p>`;
    } else if (panel === 'history') {
      html = `<p class="rq-help">Last ${state.events.length} events · Score ${state.score}</p>${[...state.events].reverse().map(e => `<p>${escape(e.text)}</p>`).join('')}`;
    } else if (panel === 'scores') {
      html = ['rpgHighscores','coopHighscores'].map((key,index) => `<h3>${index ? 'Cooperative runs' : 'Solo runs'}</h3>${read(`${prefix}${key}`,[]).map((record,i) => card(`#${i+1} · ${record.score} points`,`Floor ${record.floor} · Level ${record.level} · ${new Date(record.date).toLocaleDateString()}`)).join('') || '<p>No completed runs yet.</p>'}`).join('');
    } else if (panel === 'mastery') {
      const mastered = read(`${prefix}mastery`,[]).slice().reverse();
      html = `<p>${mastered.length} remembered verses</p><div class="rq-card-grid">${mastered.map(reference => { const verse = rules.parseReference(reference); return verse ? card(reference,verse.text,button('Select verse','select-verse',!!state.used[refKey(verse)],`data-key="${escape(refKey(verse))}"`)) : card(reference,''); }).join('') || '<p>Use a verse successfully to remember it here.</p>'}</div>`;
    } else if (panel === 'class') {
      const options = rules.classOptions(p);
      html = card(classes[p.classId].name,classes[p.classId].desc) + `<p>Callings unlock at levels 5, 10 and 15.</p><div class="rq-card-grid">${options.map(id => card(classes[id].name,classes[id].desc,button('Choose calling','class-pick',busy,`data-id="${id}"`))).join('')}</div>`;
    } else if (panel === 'room') html = renderRoom(p);
    else if (panel === 'quiz') html = p.quiz ? challengeHTML(p.quiz,'quiz') : '<p>No challenge is available this encounter.</p>';
    else if (panel === 'target') html = targetHTML(p);
    body.innerHTML = html; body.scrollTop = scroll;
    if (draft != null && $('rqChallengeRef')) $('rqChallengeRef').value = draft;
    if (focusedId && $(focusedId)) $(focusedId).focus({preventScroll:true});
  }
  function challengeHTML(q,type) {
    return `<p>${escape(q.prompt)}</p>${q.deadline ? `<p class="rq-help">45-second challenge · deadline ${escape(new Date(q.deadline).toLocaleTimeString())}. You have one attempt.</p>` : ''}${q.text ? `<blockquote>${escape(q.text)}</blockquote>` : ''}<div class="rq-card-grid">${q.options.map(option => button(q.kind === 'theme' ? conceptLabel(option) : option,'answer',busy,`data-type="${type}" data-answer="${escape(option)}"`)).join('')}</div>${!q.options.length ? `<form id="rqChallengeForm" data-type="${type}" class="rq-attack-row"><input id="rqChallengeRef" aria-label="Challenge verse reference" placeholder="Verse reference" required><button type="submit">Submit</button></form>` : ''}`;
  }
  function renderRoom(p) {
    const phase = state.phase;
    $('rqDialogTitle').textContent = phase === 'path' ? 'Choose your path' : phase === 'ended' ? 'Your journey ends here' : ROOM_NAMES[phase] || 'Battle in progress';
    if (phase === 'battle') return '<p>Finish this encounter to open the next room.</p>';
    if (phase === 'ended') {
      const examples = verses.filter(v => state.enemy.weaknesses.some(w => rules.tags(v).includes(w.concept))).slice(0,6);
      return `<p>Floor ${state.floor} · Score ${state.score} · ${state.bosses} bosses defeated</p>${button('Begin a new run','restart',busy)}${button('Return to lobby','exit')}<h3>Verses to remember</h3>${examples.map(v => card(displayRef(v),v.text)).join('')}`;
    }
    if (phase === 'path') return `<p>${coop ? 'Any living teammate may choose. The party travels together.' : 'Choose where your journey leads.'}</p><div class="rq-card-grid">${state.paths.map(path => card(ROOM_NAMES[path],ROOM_HINTS[path],button('Take this path','path',busy || p.hp <= 0,`data-path="${path}"`))).join('')}</div>${rules.classOptions(p).length ? button('New calling available','class') : ''}`;
    let html = `<p>${escape(ROOM_HINTS[phase])}</p>`;
    if (p.roomDone) html += '<p>Your choices are saved. Waiting for teammates. If someone disconnects, press Continue again after 60 seconds.</p>';
    else if (phase === 'shop') {
      html += `<p><b>${escape(p.shop.merchant)}</b> · ◉ ${p.gold} gold${p.discount ? ' · 15% discount' : ''}</p><div class="rq-card-grid">${p.shop.offers.map(offer => {
        const def = offer.kind === 'item' ? rules.items[offer.id] : offer.kind === 'relic' ? rules.relics[offer.id] : ['ability','upgrade'].includes(offer.kind) ? ABILITIES[offer.id] : null;
        const title = offer.kind === 'upgrade' ? `Upgrade ${def.name}` : def?.name || ({training:'Verse training',replace:'Replace one relic',mystery:'Mystery parcel'})[offer.kind];
        const description = offer.kind === 'upgrade' ? offer.id === 'second-wind' ? 'Restore 4% more maximum health.' : 'Recharge one verse sooner (minimum 2).' : def?.desc || ({training:'Permanently gain 2 base damage this run.',replace:'Trade one owned relic for a different random relic.',mystery:'40% chance of a relic; otherwise an uncommon or better consumable.'})[offer.kind];
        return card(`${offer.sold ? '✓ ' : ''}${title}`,description,button(offer.sold ? 'Sold' : `Buy · ${offer.cost} gold`,'buy',busy || offer.sold || p.gold < offer.cost,`data-offer="${offer.key}"`));
      }).join('')}</div>${button(`Reroll · ${10+p.shop.rerolls*5} gold`,'reroll',busy || p.gold < 10+p.shop.rerolls*5)}${button('Inventory / merchant token','inventory')}`;
    } else if (!p.roomClaimed) {
      if (phase === 'rest') html += `<div class="rq-card-grid">${button('Rest · restore 28% HP','claim',busy,'data-choice="heal"')}${button('Study · next verse +15%','claim',busy,'data-choice="study"')}</div>`;
      if (phase === 'treasure') html += card(p.treasure ? rules.relics[p.treasure].name : 'Gold cache',p.treasure ? rules.relics[p.treasure].desc : 'No unowned relic remains.',button('Take relic','claim',busy,'data-choice="relic"') + button('Take 32 gold','claim',busy,'data-choice="gold"'));
      if (phase === 'risk') html += card(p.riskOffer ? rules.relics[p.riskOffer].name : 'A quiet road',p.riskOffer ? rules.relics[p.riskOffer].desc : 'You already own all the challenge relics.',button('Accept relic +25 gold','claim',busy || !p.riskOffer,'data-choice="accept"') + button('Pass safely','claim',busy,'data-choice="pass"'));
      if (phase === 'scripture') html += challengeHTML(state.room.challenge,'answer');
    } else html += `<p>${escape(p.roomResult || 'Reward claimed.')}</p>`;
    html += `<div class="rq-room-footer">${button(p.roomDone ? 'Continue when party is ready' : 'Continue / skip remaining choices','continue',busy)}${button('Equip abilities','abilities')}</div>`;
    return html;
  }
  function targetHTML(p) {
    if (!targetAction) return '<p>Choose an ability or item first.</p>';
    const {kind} = targetAction;
    if (kind === 'recall') return `<p>Release one verse for the whole team. Each verse can be recalled only once per encounter.</p><div class="rq-card-grid">${Object.entries(state.used).filter(([key]) => !state.recalled.includes(key)).map(([key,value]) => button(`${displayRef(rules.verseMap.get(key))} · ${value.name}`,'target',busy,`data-value="${escape(key)}"`)).join('') || '<p>No eligible used verses.</p>'}</div>`;
    if (kind === 'revive') return `<div class="rq-card-grid">${state.players.filter(ally => ally.hp <= 0 || (targetAction.type === 'ability' && ally.id === me)).map(ally => button(`${ally.name} · ${ally.hp <= 0 ? 'Revive' : `Heal ${ally.hp}/${ally.maxHp}`}`,'target',busy,`data-value="${ally.id}"`)).join('') || '<p>No teammates are downed.</p>'}</div>`;
    if (kind === 'recharge') return `<div class="rq-card-grid">${p.equipped.filter(id => ABILITIES[id].cooldown && p.cooldowns[id] > p.turn).map(id => button(ABILITIES[id].name,'target',busy,`data-value="${id}"`)).join('') || '<p>No abilities are recharging.</p>'}</div>`;
    if (kind === 'replace') return `<p>Choose the relic to replace.</p><div class="rq-card-grid">${p.relics.map(id => button(rules.relics[id].name,'target',busy,`data-value="${id}"`)).join('')}</div>`;
    return '';
  }
  function later(callback,ms) {
    const timer = setTimeout(() => { effectTimers.delete(timer); callback(); },ms);
    effectTimers.add(timer);
  }
  function clearEffects() {
    for (const timer of effectTimers) clearTimeout(timer);
    effectTimers.clear();
    $('rqEffects')?.replaceChildren();
    root.querySelectorAll('.rq-answer-slot').forEach(slot => slot.replaceChildren());
  }
  function answerEffect(event) {
    // Initial/reconnected snapshots are baselined in accept(); stale events never replay.
    const remaining = Math.min(2800,event.expiresAt - Date.now());
    if (!(remaining > 0) || !state.players.some(p => p.id === event.playerId)) return;
    const slot = root.querySelector(`[data-actor="${event.playerId}"] .rq-answer-slot`);
    if (!slot) return;
    const fx = document.createElement('div');
    fx.className = `rq-answer ${event.correct ? 'rq-answer-correct' : 'rq-answer-wrong'}`;
    fx.dataset.action = event.actionId;
    const reference = document.createElement('strong'), result = document.createElement('small');
    reference.textContent = event.reference.toUpperCase();
    result.textContent = event.correct ? '✓ Protected · 0 damage' : '✕ Incorrect';
    fx.append(reference,result); slot.replaceChildren(fx);
    fx.style.animationDuration = `${remaining}ms`;
    later(() => fx.remove(),remaining);
  }
  function effect(event) {
    if (!event.effect || !running) return;
    if (event.effect === 'answer') { answerEffect(event); return; }
    const colors = {faith:'#ffe29a',holy:'#ffe29a',truth:'#ffffff',wisdom:'#78c7ff',courage:'#ffbb62',healing:'#8cf2b9',poison:'#9be165',fire:'#ff8557',fear:'#bf9ee8',frost:'#a4e8ff',victory:'#ffda75'};
    const color = colors[event.effect] || '#ffe29a', layer = $('rqEffects');
    if (layer.children.length >= 10) layer.firstElementChild.remove();
    const fx = document.createElement('div');
    fx.className = `rq-fx ${event.target === 'enemy' ? 'rq-on-enemy' : 'rq-on-player'} ${event.effect === 'healing' ? 'rq-heal-fx' : event.effect === 'truth' ? 'rq-truth-fx' : event.effect === 'fear' ? 'rq-fear-fx' : event.effect === 'fire' ? 'rq-fire-fx' : event.effect === 'poison' ? 'rq-poison-fx' : 'rq-ring-fx'}`;
    fx.style.setProperty('--fx',color);
    fx.textContent = event.amount != null ? event.target === 'enemy' ? `${event.amount}` : `${event.amount > 0 ? '+' : ''}${event.amount}` : event.effect === 'victory' ? 'VICTORY' : '✦';
    const figure = event.target === 'enemy' ? $('rqEnemyFigure') : root.querySelector(`[data-actor="${event.target}"]`);
    if (figure) {
      const area = layer.getBoundingClientRect(), actor = figure.querySelector('img').getBoundingClientRect();
      fx.style.left = `${actor.left + actor.width / 2 - area.left}px`;
      fx.style.top = `${actor.top + actor.height / 2 - area.top}px`;
    }
    layer.appendChild(fx); later(() => fx.remove(),1000);
    if (figure) { figure.classList.remove('rq-hit'); void figure.offsetWidth; figure.classList.add('rq-hit'); }
    if (event.critical) { $('rqArena').classList.remove('rq-critical'); void $('rqArena').offsetWidth; $('rqArena').classList.add('rq-critical'); }
  }
  async function activate(type,id) {
    if (type === 'ability') {
      if (id === 'recall') { targetAction = {type,ability:id,kind:'recall'}; openPanel('target'); return; }
      if (id === 'second-wind' && state.players.some(ally => ally.hp <= 0)) { targetAction = {type,ability:id,kind:'revive'}; openPanel('target'); return; }
      await send('ability',{ability:id}); return;
    }
    const kind = rules.items[id].kind;
    if (['refresh','great-recall','revive','recharge'].includes(kind)) {
      targetAction = {type:'item',item:id,kind:['refresh','great-recall'].includes(kind) ? 'recall' : kind}; openPanel('target'); return;
    }
    await send('item',{item:id});
  }
  root.addEventListener('click',async event => {
    const el = event.target.closest('[data-act]'); if (!el || el.disabled) return;
    const act = el.dataset.act;
    if (act === 'close') { closePanel(); return; }
    if (['menu','inventory','abilities','modifiers','history','room','quiz','class','scores','mastery'].includes(act)) { openPanel(act); return; }
    if (act === 'select-verse') {
      selected = rules.verseMap.get(el.dataset.key); if (!selected || state.used[refKey(selected)]) return;
      $('rqReference').value = displayRef(selected);
      closePanel(); render(); $('rqAttack').focus(); return;
    }
    if (act === 'ability' || act === 'item') { await activate(act,el.dataset.id); return; }
    if (act === 'target') {
      const action = {...targetAction}; delete action.kind; const type = action.type; delete action.type;
      const kind = targetAction.kind;
      action[kind === 'recall' ? 'reference' : kind === 'recharge' ? 'ability' : kind === 'replace' ? 'relic' : 'target'] = el.dataset.value;
      if (await send(type,action)) { targetAction = null; if (state.phase === 'battle') closePanel(); else openPanel('room'); } return;
    }
    if (act === 'buy') {
      const offer = player().shop.offers.find(o => o.key === el.dataset.offer);
      if (offer?.kind === 'replace') { targetAction = {kind:'replace',type:'buy',offer:offer.key}; openPanel('target'); return; }
      await send('buy',{offer:el.dataset.offer}); return;
    }
    if (act === 'path') await send('path',{path:el.dataset.path});
    else if (act === 'equip') await send('equip',{ability:el.dataset.id,slot:+el.dataset.slot});
    else if (act === 'class-pick') await send('class',{classId:el.dataset.id});
    else if (act === 'claim') await send('claim',{choice:el.dataset.choice});
    else if (act === 'answer') await send(el.dataset.type,{answer:el.dataset.answer});
    else if (['reroll','continue','insight'].includes(act)) await send(act);
    else if (act === 'restart') await restart();
    else if (act === 'reconnect') await start({gameId:activeGameId});
    else if (act === 'exit') { stop(); onExit?.(coop); }
  });
  root.addEventListener('input',event => {
    const id = event.target.id;
    if (id === 'rqReference') { selected = rules.parseReference(event.target.value); render(); return; }
    if (id === 'rqReduceMotion') { saveValue(`${prefix}reduceMotion`,event.target.checked); root.classList.toggle('rq-reduce-motion',event.target.checked); }
  });
  root.addEventListener('submit',async event => {
    if (!['rqAttackForm','rqChallengeForm'].includes(event.target.id)) return;
    event.preventDefault();
    if (event.target.id === 'rqChallengeForm') { await send(event.target.dataset.type,{answer:$('rqChallengeRef').value}); return; }
    const reference = $('rqReference').value;
    if (await send('attack',{reference})) { selected = null; $('rqReference').value = ''; render(); }
  });
  root.addEventListener('keydown',event => {
    if (event.key === 'Escape' && dialog.open) { event.preventDefault(); closePanel(); return; }
    if (dialog.open || event.ctrlKey || event.metaKey || event.altKey || /INPUT|SELECT|TEXTAREA/.test(event.target.tagName)) return;
    if (/^[123]$/.test(event.key)) { event.preventDefault(); const id = player()?.equipped[+event.key-1]; if (id) void activate('ability',id); }
  });
  async function restart() {
    if (busy) return;
    busy = true;
    try {
      closePanel();
      const next = coop ? await store.restart(state.id) : rules.create({...newRun(),names:[username]});
      accept(next);
    } catch (error) { notice(error.message); }
    finally { busy = false; renderBusy(); }
  }
  async function start({gameId = '',resume = true} = {}) {
    stop(); const generation = ++startGeneration; activeGameId = gameId; running = true; coop = !!gameId; connected = !coop; completedRun = ''; state = null; lastRun = ''; me = 'p0';
    root.classList.remove('hidden'); root.classList.add('rq-view'); root.classList.toggle('rq-reduce-motion',read(`${prefix}reduceMotion`,false));
    notice(coop ? 'Connecting to your party…' : '');
    if (coop) {
      try {
        const authUser = await getAuthUser();
        if (!running || generation !== startGeneration) return;
        store = createCoopStore({api,db,gameId,username,uid:authUser.uid,rules,newRun});
        const connectingStore = store;
        const joined = await connectingStore.connect(next => {
          if (!running || generation !== startGeneration) return;
          const mine = next.players.find(p => normalizeName(p.name) === normalizeName(username)); if (mine) me = mine.id;
          accept(next);
        },(ok,message) => { if (!running || generation !== startGeneration) return; connected = ok; renderBusy(); if (!ok) notice(message); else notice(''); });
        if (!running || generation !== startGeneration) { connectingStore.close(); return; }
        me = joined.playerId;
        render();
      } catch (error) { if (!running || generation !== startGeneration) return; notice(error.message); $('rqConnection').textContent = 'Connection unavailable'; $('rqAttack').disabled = true; }
      return;
    }
    let next = null;
    if (resume) {
      const saved = read(saveKey,null) || read(`${prefix}rpgRunSave:v3`,null);
      if ([3,VERSION].includes(saved?.version) && [3,VERSION].includes(saved.state?.version) && saved.state?.players?.length === 1 && saved.state?.room && saved.state?.enemy) next = rules.upgradeRun(saved.state);
      else next = rules.migrateLegacy(read(`${prefix}rpgRunSave:v2`,null),{...newRun(),name:username});
    }
    accept(next || rules.create({...newRun(),names:[username]}));
  }
  function stop() {
    persist(); startGeneration++; running = false; store?.close(); store = null; clearEffects();
    if (dialog.open) closePanel();
    root.classList.remove('rq-view'); root.classList.add('hidden');
  }
  window.addEventListener('pagehide',stop);
  window.addEventListener('online',() => { if (running && coop && !connected) notice('Connection restored. Waiting for the shared run…'); });
  return {start,stop,rules,get state() { return state; }};
}
