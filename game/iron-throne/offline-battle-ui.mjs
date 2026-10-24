import { UNITS, FORMATIONS } from './data.mjs';
import { configureOfflineBattles, pendingEncounter, commitEncounterSupport, beginEncounter, saveEncounterCheckpoint, resolveEncounterAutomatically, applyEncounterResult, returnFromEncounter, encounterPreview } from './offline-encounters.mjs';

const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const total = units => Object.values(units || {}).reduce((sum,n) => sum + n, 0);
const composition = units => units ? Object.entries(units).filter(([,n])=>n).map(([id,n])=>`${n} ${UNITS[id]?.name || id}`).join(' · ') || 'No troops' : 'Composition unknown';
const groups = [['all','All troops'],['infantry','Infantry'],['spearmen','Spearmen'],['ranged','Ranged'],['cavalry','Cavalry'],['siege','Siege']];
const orders = [['move','Move and face'],['hold','Hold position'],['walls','Defend walls'],['advance','Advance'],['charge','Charge'],['follow','Follow me'],['attack','Attack selected formation'],['nearest','Attack nearest'],['fire','Fire'],['holdFire','Hold fire'],['withdraw','Withdraw'],['retreat','Retreat']];

// This controller owns only presentation and input. Campaign writes always go
// through the same validated encounter API and one durable campaign snapshot.
export function installOfflineBattleUI({getState,replaceState,save,render,continueTurn,toast,isOnline=()=>false}) {
  if(isOnline()) return null;
  const ui = document.createElement('div'); ui.dataset.battleUi = '';
  ui.innerHTML = `<button id="battle-pending" class="battle-pending" hidden>Battle pending · Resume encounter</button>
    <dialog id="battle-encounter" class="battle-dialog" aria-labelledby="battle-title"><header class="dialog-header"><div><span class="eyebrow">OFFLINE CAMPAIGN · LEAD THE BATTLE</span><h2 id="battle-title">Encounter</h2></div><button data-battle-action="close" aria-label="Pause this battle decision">×</button></header><div id="battle-brief" class="dialog-body"></div><p id="battle-notice" role="status" class="battle-notice"></p><footer id="battle-choices" class="battle-choices"></footer></dialog>
    <dialog id="battlefield" class="battlefield" aria-label="Lead the battle"><canvas id="battle-canvas" tabindex="0" aria-label="3D battlefield. WASD move, mouse aim, click attack, right click block, Q commands, Tab tactical view, Escape pause."></canvas>
      <header class="battle-hud"><div><span class="eyebrow" id="battle-mode">DEPLOYMENT</span><strong id="battle-location"></strong></div><div id="battle-counts"></div><div class="battle-hud-actions"><button data-battle-action="camera">Tactical view · Tab</button><button data-battle-action="commands">Orders · Q</button><button data-battle-action="pause">Pause · Esc</button></div></header>
      <div id="battle-deployment" class="battle-deployment"><h2>Choose your ground</h2><p>Select a formation, then drag on the field to place it and set its facing. Your army must deploy within its starting area.</p><button class="primary" data-battle-action="begin">Begin Battle</button></div>
      <aside id="battle-command-panel" class="battle-command-panel" hidden><div class="battle-group-row">${groups.map(([id,label])=>`<button data-battle-group="${id}" aria-pressed="${id==='all'}">${label}</button>`).join('')}</div><label>Army <select id="battle-army"><option value="">All your armies</option></select></label><label>Formation <select id="battle-formation">${Object.entries(FORMATIONS).map(([id,f])=>`<option value="${id}">${esc(f.name)}</option>`).join('')}</select></label><div class="battle-order-row">${orders.map(([id,label])=>`<button data-battle-order="${id}">${label}</button>`).join('')}</div><p id="battle-order-status" role="status">Move and face: drag on the field.</p><label>Allied army <select id="battle-ally"></select></label><div class="battle-order-row"><button data-battle-request="flank">Protect a flank</button><button data-battle-request="assault">Support assault</button><button data-battle-request="withdraw">Cover withdrawal</button></div><label>Tactical view <select id="battle-command-time"><option value="pause">Pause combat</option><option value="slow">Slow motion</option><option value="real">Real time</option></select></label></aside>
      <div id="battle-event" class="battle-event" role="status" aria-live="polite"></div><div id="battle-reticle" class="battle-reticle" aria-hidden="true">+</div>
      <footer class="battle-player"><span id="battle-player-status"></span><label>Weapon <select id="battle-weapon"><option value="sword">Sword & shield</option><option value="spear">Spear & shield</option><option value="axe">Two-handed axe</option><option value="mace">Mace</option><option value="bow">Bow & sidearm</option><option value="crossbow">Crossbow & sidearm</option><option value="lance">Mounted lance</option></select></label><span class="battle-help">WASD move · Mouse aim · Click attack · Right click block · Space heavy attack · Shift sprint · E interact / mount · Tab command</span></footer>
      <div class="battle-touch" id="battle-touch"><div id="battle-stick" aria-label="Movement stick"><span></span></div><div class="battle-touch-buttons"><button data-battle-touch="sprint">Sprint</button><button data-battle-touch="block">Block</button><button data-battle-touch="attack">Attack</button><button data-battle-touch="heavy">Heavy</button><button data-battle-touch="interact">Interact / Mount</button><button data-battle-action="commands">Orders</button></div></div>
      <div id="battle-pause" class="battle-pause" hidden><section><span class="eyebrow">CAMPAIGN PAUSED</span><h2>Battle paused</h2><p id="battle-pause-note">Progress is saved on this device. Your campaign waits here.</p><button class="primary" data-battle-action="resume">Resume</button><button data-battle-action="exit">Save and Exit</button><button data-battle-action="retreat">Retreat</button><button data-battle-action="finish">Finish Automatically</button></section></div>
    </dialog>`;
  document.body.append(ui);
  const $ = id => ui.querySelector(`#${id}`), encounterDialog=$('battle-encounter'), field=$('battlefield'), canvas=$('battle-canvas');
  const menu=document.querySelector('#menu .menu-actions');
  const settings=document.createElement('div'); settings.dataset.battleUi=''; settings.className='battle-settings';
  settings.innerHTML=`<label>Offline battles<select id="offline-battle-preference"><option value="ask">Ask before battles</option><option value="auto">Resolve automatically</option></select></label><button id="battle-download" type="button">Download battle assets for offline play</button><p id="battle-download-status" class="fine" role="status"></p>`;
  menu?.prepend(settings);
  let sim, renderer, battle=null, frame=0, lastTime=0, lastCheckpoint=0, lastEvent='', lastMilestone='', loading=false, paused=false, finishing=false, graphicsError=false, handledCommanderExit=false;
  let group='all', order='move', drag=null, pointer=null, stickOrigin=null, camera={mode:'tactical',yaw:0,pitch:.8,distance:90};
  let keys=new Set(), touch={x:0,z:0}, input={attack:false,block:false,sprint:false}, sceneAbort=null;
  const mode=()=>battle?.phase==='deployment'?'deployment':camera.mode;
  const active=()=>!isOnline() && pendingEncounter(getState());
  function resetInput(){keys.clear();touch={x:0,z:0};input={attack:false,block:false,sprint:false};drag=pointer=stickOrigin=null;$('battle-stick').firstElementChild.style.transform='';}
  function notice(message){$('battle-notice').textContent=message||'';}
  function transact(operation){
    if(isOnline())throw new Error('Battle control is available only in offline campaigns.');
    const before=getState(), next=structuredClone(before), result=operation(next);
    if(result?.ok===false)throw new Error(result.error || 'The battle could not be updated.');
    replaceState(next);
    if(!save()){replaceState(before);throw new Error('The campaign could not be saved. Free device storage or export your save before continuing.');}
    return result;
  }
  function ownSupport(){const own=new Set((active()?.supportOptions||[]).filter(p=>p.playerControlled&&p.available).map(p=>p.armyId));return [...ui.querySelectorAll('[data-battle-support]:checked')].map(el=>el.dataset.battleSupport).filter(id=>own.has(id));}
  function lockSupport(){if(!active()?.commitmentsLocked)transact(s=>commitEncounterSupport(s,ownSupport()));}
  function displayArmy(p){return `<article class="battle-army"><strong>${esc(p.name||p.armyName||p.armyId||'Unidentified army')}</strong><span>${esc(p.commander?.name||p.commander||'Army captain')}</span><p>${esc(composition(p.units))}</p><small>${p.distance?`${p.distance} legal hex steps · ${esc(p.entryDirection||'approach')} · ${p.arrivalSeconds||p.distance*25}s arrival`:'Initial deployment'}${p.isSupport?' · Supporting army':''}</small></article>`;}
  function showEncounter(){
    if(!active()||loading)return;
    disposeScene();
    const e=active(), view=encounterPreview(getState())||e;
    $('battle-title').textContent=e.status==='applied'?'Battle aftermath':`${e.playerSide===0?'Attack':'Defense'} at ${view.tile?.name||view.tile?.id||'the frontier'}`;
    if(e.status==='applied'){
      const result=view.result||{}, rows=result.armies||[];
      $('battle-brief').innerHTML=`<p class="battle-summary">${result.winner===null?'Both sides withdrew.':result.winner===e.playerSide?'Your side holds the field.':'Your side has withdrawn.'}</p><div class="battle-report-scroll"><table><thead><tr><th>Army / troops</th><th>Started</th><th>Lost</th><th>Survived</th></tr></thead><tbody>${rows.map(row=>{const p=view.participants.find(p=>p.armyId===row.armyId)||{}, survivors=row.units||row.survivors||{}, start=p.units||row.starting||{};return `<tr><th colspan="4">${esc(p.name||row.armyId)}</th></tr>${Object.entries(start).filter(([,n])=>n).map(([id,n])=>`<tr><td>${esc(UNITS[id]?.name||id)}</td><td>${n}</td><td>${n-(survivors[id]||0)}</td><td>${survivors[id]||0}</td></tr>`).join('')}`;}).join('')}</tbody></table></div><p>Remaining walls: ${Math.round(result.walls??result.fortifications?.walls??e.tile.walls??0)} · Fort integrity: ${Math.round(result.fortIntegrity??result.fortifications?.fortIntegrity??e.tile.fortIntegrity??0)}</p><p class="fine">Escaped troops and troops that never deployed are included in survivors. All participating armies have been updated together.</p>`;
      $('battle-choices').innerHTML='<button class="primary" data-battle-action="return">Return to Board</button>';
    }else{
      const participants=view.participants||[], support=view.supportOptions||[];
      $('battle-brief').innerHTML=`<p>${esc(view.tile?.terrain||'Field')} · ${esc(view.tile?.building||'Open ground')} · Walls ${Math.round(view.tile?.walls||0)} · Fort ${Math.round(view.tile?.fortIntegrity||0)}</p><div class="battle-sides">${[e.playerSide,1-e.playerSide].map((side,i)=>`<section><h3>${i?'Known opposition':'Your side'}</h3>${participants.filter(p=>p.side===side).map(displayArmy).join('')||'<p>Army details are not visible.</p>'}</section>`).join('')}</div>${support.length?`<h3>Nearby support</h3><div class="battle-support">${support.map(p=>`<label><input type="checkbox" data-battle-support="${esc(p.armyId)}" ${p.available===false||p.owner!==e.playerOwner||e.commitmentsLocked?'disabled':''} ${p.joining||p.committed||p.available!==false&&p.owner===e.playerOwner?'checked':''}> <strong>${esc(p.name||p.armyId)}</strong> · ${p.distance??'?'} steps · ${esc(p.entryDirection||'approach')}<small>${esc(p.reason||p.status||(p.available===false?'Unavailable':p.owner===e.playerOwner?'Available':'Allied commander decides'))}</small><span>${esc(composition(p.units))}</span></label>`).join('')}</div>`:''}<p id="battle-forecast" class="battle-forecast">${forecastText(view)}</p><p class="fine">Automatic casualty estimates describe the strategic resolver. They do not predict the outcome of personally fighting. Both choices use the same committed armies and existing defenses.</p>${e.checkpoint?'<p class="battle-summary">Saved battle progress is available. Continue fighting or finish automatically from your current survivors and damage.</p>':''}`;
      $('battle-choices').innerHTML=`<button data-battle-action="auto">${e.checkpoint?'Finish Automatically':'Resolve Battle'}</button><button class="primary" data-battle-action="enter">${e.checkpoint?'Resume Battle':'Enter Battle'}</button><button data-battle-action="close">Decide later</button>`;
    }
    notice(''); if(!encounterDialog.open)encounterDialog.showModal(); sync();
  }
  function forecastText(view){const f=view.automaticForecast||view.forecast;if(typeof f==='string')return f;if(f?.label)return f.label; if(f?.armies)return `Automatic resolution estimate: ${f.armies.map(a=>`${a.name||a.armyId}: ${total(a.losses)} losses`).join(' · ')}`;return 'Automatic resolution estimate will be calculated from the committed forces.';}
  async function loadSimulation(){sim ||= await import('./battle-sim.mjs');return sim;}
  const milestone=()=>battle?`${battle.armies.filter(a=>a.arrived).length}:${battle.layout.navRevision||0}:${battle.layout.gate?.hp===0}`:'';
  function checkpoint(){if(!battle)return;transact(s=>saveEncounterCheckpoint(s,structuredClone(battle)));lastCheckpoint=battle.time;lastMilestone=milestone();}
  function selection(){const army=$('battle-army').value;return army?battle.units.filter(u=>u.armyId===army&&(group==='all'||group==='spearmen'?group==='all'||u.type==='spearman':group==='cavalry'?u.family==='mounted':u.family===group)).map(u=>u.id):group;}
  function sendOrder(params={}){
    if(!battle||!sim||finishing)return;
    const result=sim.issueOrder(battle,selection(),order,{formation:$('battle-formation').value,...params});
    $('battle-order-status').textContent=result?.message||result?.error||result?.reason||`${orders.find(([id])=>id===order)?.[1]||order} ordered${params.x!==undefined?` to ${Math.round(params.x)}, ${Math.round(params.z)}`:''}.`;
  }
  async function enter(){
    if(loading||!active()||active().status==='applied')return;loading=true;
    try{
      const pending=active(), ownIds=pending.commitmentsLocked?pending.participants.filter(p=>p.playerControlled).map(p=>p.armyId):[...pending.participants.filter(p=>p.playerControlled&&!p.isSupport).map(p=>p.armyId),...ownSupport()];
      if(!ownIds.length)throw new Error('Include one of your armies to lead this battle.');
      lockSupport();transact(s=>beginEncounter(s));notice('Preparing the battlefield…');
      await loadSimulation();const e=active();
      battle=e.checkpoint?structuredClone(e.checkpoint):sim.createBattle(e);
      const commander=player();$('battle-weapon').value=commander?.weapon||'sword';
      for(const option of $('battle-weapon').options)option.disabled=['bow','crossbow'].includes(option.value)?commander?.family!=='ranged':option.value==='lance'?commander?.family!=='mounted':false;
      const validation=sim.validateBattleCheckpoint(battle,e);if(validation===false||validation?.ok===false)throw new Error(validation?.error||'The saved battle checkpoint is invalid. Finish automatically to recover.');
      const {createBattleRenderer}=await import('./battle-renderer.mjs');sceneAbort=new AbortController();
      encounterDialog.close();field.showModal();
      renderer=await createBattleRenderer(canvas,battle,{signal:sceneAbort.signal,onProgress:p=>{$('battle-event').textContent=typeof p==='string'?p:'Loading battlefield models…';}});
      camera={mode:battle.phase==='deployment'?'tactical':'third',yaw:commander?.facing||0,pitch:battle.phase==='deployment'?.8:.35,distance:battle.phase==='deployment'?90:10};
      $('battle-location').textContent=e.tile.name||e.tile.id;
      $('battle-army').innerHTML='<option value="">All your armies</option>'+e.participants.filter(p=>p.owner===e.playerOwner).map(p=>`<option value="${esc(p.armyId)}">${esc(p.name||p.armyId)}</option>`).join('');
      $('battle-ally').innerHTML=e.participants.filter(p=>p.side===e.playerSide&&p.owner!==e.playerOwner).map(p=>`<option value="${esc(p.armyId)}">${esc(p.name||p.armyId)}</option>`).join('');
      $('battle-command-time').value=getState().offlineBattles?.tacticalTime||'pause';
      $('battle-command-panel').hidden=battle.phase!=='deployment';paused=false;graphicsError=false;handledCommanderExit=false;$('battle-pause').hidden=true;$('battle-pause-note').textContent='Progress is saved on this device. Your campaign waits here.';
      lastTime=0;lastCheckpoint=battle.time;lastEvent='';checkpoint();canvas.focus();frame=requestAnimationFrame(tick);
    }catch(error){disposeScene();loading=false;showEncounter();notice(`The battlefield could not open. ${error.message} Retry Enter Battle or choose Resolve Battle.`);}finally{loading=false;sync();}
  }
  function player(){return battle?.units.find(u=>u.id===battle.commanderId);}
  function tick(now){
    if(!battle||!renderer)return;const dt=Math.min(.05,lastTime?(now-lastTime)/1000:0);lastTime=now;
    const p=player(), tactical=camera.mode==='tactical';
    const forward=(keys.has('KeyW')?1:0)-(keys.has('KeyS')?1:0)-touch.z, strafe=(keys.has('KeyD')?1:0)-(keys.has('KeyA')?1:0)+touch.x;
    const commandTime=$('battle-command-time').value, timeScale=paused||finishing?0:tactical?(commandTime==='pause'?0:commandTime==='slow'?.18:1):1;
    if(tactical&&p){camera.target||={x:p.x,z:p.z};camera.target.x+=strafe*dt*24;camera.target.z+=forward*dt*24;}
    if(!paused&&!finishing&&battle.phase==='combat'){
      sim.stepBattle(battle,dt*timeScale,tactical?{commanderAI:true}:{...input,moveX:Math.sin(camera.yaw)*forward+Math.cos(camera.yaw)*strafe,moveZ:Math.cos(camera.yaw)*forward-Math.sin(camera.yaw)*strafe,facing:camera.yaw,attack:input.attack||input.heavy||keys.has('Space'),heavy:input.heavy||keys.has('Space'),block:input.block,sprint:input.sprint||keys.has('ShiftLeft')||keys.has('ShiftRight'),weapon:$('battle-weapon').value,aimX:p?p.x+Math.sin(camera.yaw)*70:0,aimZ:p?p.z+Math.cos(camera.yaw)*70:0});
      input.interact=false;
    }
    if(p?.status==='reserve'&&!tactical){const escort=battle.units.find(u=>u.side===battle.playerSide&&u.status==='active');if(escort){camera.target={x:escort.x,y:escort.y,z:escort.z};camera.spectating=true;}}
    else if(camera.spectating){delete camera.target;delete camera.spectating;if(p)camera.yaw=p.facing;}
    if(p&&['dead','escaped'].includes(p.status)&&!handledCommanderExit){handledCommanderExit=true;camera.mode='tactical';camera.distance=90;camera.pitch=.8;$('battle-command-panel').hidden=false;}
    try{renderer.render(battle,camera);}catch(error){graphicsError=true;setPause(true);$('battle-pause-note').textContent=`${error.message} Choose Resume to reload the battlefield, or Finish Automatically to preserve your progress.`;return;}updateHUD();
    if(battle.phase==='complete'){finishResult();return;}
    if(battle.time-lastCheckpoint>=5||milestone()!==lastMilestone){try{checkpoint();}catch(error){setPause(true);$('battle-pause-note').textContent=error.message;}}
    frame=requestAnimationFrame(tick);
  }
  function updateHUD(){
    const p=player(), e=active(), side=battle.playerSide??e?.playerSide??0;
    field.classList.toggle('is-deploying',battle.phase==='deployment');
    $('battle-mode').textContent=battle.phase==='deployment'?'DEPLOYMENT':paused?'PAUSED':camera.mode==='tactical'?`TACTICAL COMMAND · ${$('battle-command-time').selectedOptions[0].textContent.toUpperCase()}`:'LEAD THE BATTLE';
    $('battle-deployment').hidden=battle.phase!=='deployment';$('battle-reticle').hidden=camera.mode==='tactical';
    const friendly=battle.units.filter(u=>u.side===side),enemy=battle.units.filter(u=>u.side!==side&&u.status==='active');
    $('battle-counts').textContent=`Your side ${friendly.filter(u=>u.status!=='dead').length} · ${friendly.filter(u=>u.status==='reserve').length} reserves | Enemy deployed ${enemy.length}`;
    $('battle-player-status').textContent=p?.status==='active'?`Commander · Health ${Math.ceil(p.hp)}/${p.maxHp} · Stamina ${Math.round(p.stamina)}${Number.isFinite(p.ammo)?` · Ammo ${p.ammo}`:''}`:p?.status==='reserve'?'Your commander is approaching with reinforcements.':p?.status==='escaped'?'Your commander has withdrawn · Your army continues.':'Commander incapacitated · Your army continues under tactical command';
    const event=battle.events.at(-1), message=typeof event==='string'?event:event?.message||event?.text;
    if(message&&message!==lastEvent){lastEvent=message;$('battle-event').textContent=message;}
  }
  function setPause(value){if(finishing&&!value)return;paused=value;resetInput();$('battle-pause').hidden=!value;if(value){document.exitPointerLock?.();try{checkpoint();}catch(error){$('battle-pause-note').textContent=error.message;}}lastTime=0;}
  function cameraToggle(){if(!battle)return;camera.mode=camera.mode==='tactical'?'third':'tactical';camera.distance=camera.mode==='tactical'?90:10;camera.pitch=camera.mode==='tactical'?.8:.35;delete camera.target;$('battle-command-panel').hidden=camera.mode!=='tactical';document.exitPointerLock?.();resetInput();}
  function finishResult(){
    if(!battle)return;
    try{transact(s=>applyEncounterResult(s,sim.battleResult(battle)));disposeScene();render();showEncounter();}
    catch(error){setPause(true);$('battle-pause-note').textContent=error.message;}
  }
  async function finishSavedBattle(){
    if(finishing)return;finishing=true;
    try{
      if(battle){setPause(true);checkpoint();}
      const encounter=structuredClone(active());
      notice('Finishing from your saved battle progress…');$('battle-pause-note').textContent='Finishing from your saved battle progress…';
      const result=await new Promise((resolve,reject)=>{
        const worker=new Worker(new URL('./battle-finish-worker.mjs',import.meta.url),{type:'module'});
        const timer=setTimeout(()=>{worker.terminate();reject(new Error('Automatic completion timed out. Your saved battle remains available to retry.'));},120000);
        const done=()=>{clearTimeout(timer);worker.terminate();};
        worker.onerror=event=>{event.preventDefault();done();reject(new Error('Automatic completion could not start. Your saved battle is unchanged.'));};
        worker.onmessage=({data})=>{done();data.error?reject(new Error(data.error)):resolve(data.result);};
        worker.postMessage({encounter,checkpoint:encounter.checkpoint});
      });
      transact(s=>applyEncounterResult(s,result));disposeScene();render();showEncounter();
    }catch(error){notice(error.message);$('battle-pause-note').textContent=error.message;toast(error.message);}finally{finishing=false;}
  }
  function disposeScene(){cancelAnimationFrame(frame);frame=0;sceneAbort?.abort();sceneAbort=null;renderer?.dispose();renderer=null;battle=null;resetInput();document.exitPointerLock?.();if(field.open)field.close();}
  async function action(name){
    try{
      if(finishing)return;
      if(name==='close'){encounterDialog.close();sync();return;}
      if(name==='enter'){await enter();return;}
      if(name==='auto'){lockSupport();if(active()?.checkpoint){await finishSavedBattle();return;}transact(s=>resolveEncounterAutomatically(s));render();showEncounter();return;}
      if(name==='return'){const result=transact(s=>returnFromEncounter(s));encounterDialog.close();sync();render();if(result.resumeTurn)await continueTurn();return;}
      if(name==='begin'){sim.beginBattle(battle);camera.mode='third';camera.distance=10;camera.pitch=.35;delete camera.target;$('battle-command-panel').hidden=true;checkpoint();canvas.focus();return;}
      if(name==='camera'){cameraToggle();return;}
      if(name==='commands'){$('battle-command-panel').hidden=!$('battle-command-panel').hidden;if(!$('battle-command-panel').hidden&&camera.mode!=='tactical')cameraToggle();return;}
      if(name==='pause'){setPause(true);return;}
      if(name==='resume'){if(graphicsError){disposeScene();await enter();}else{setPause(false);canvas.focus();}return;}
      if(name==='exit'){checkpoint();disposeScene();showEncounter();return;}
      if(name==='retreat'){order='retreat';sim.issueOrder(battle,'all','retreat',{});setPause(false);camera.mode='third';checkpoint();return;}
      if(name==='finish'){await finishSavedBattle();return;}
    }catch(error){if(battle){setPause(true);$('battle-pause-note').textContent=error.message;}notice(error.message);toast(error.message);}
  }
  ui.addEventListener('click',event=>{const el=event.target.closest('button');if(!el)return;if(el.dataset.battleAction){event.preventDefault();action(el.dataset.battleAction);}if(el.dataset.battleGroup){group=el.dataset.battleGroup;ui.querySelectorAll('[data-battle-group]').forEach(b=>b.setAttribute('aria-pressed',String(b===el)));}if(el.dataset.battleOrder){order=el.dataset.battleOrder;if(['move','withdraw','attack'].includes(order))$('battle-order-status').textContent=order==='attack'?'Choose an enemy formation on the field.':'Drag on the field to choose position and facing.';else sendOrder();}if(el.dataset.battleRequest&&battle){const armyId=$('battle-ally').value;const r=sim.requestAlliedOrder?.(battle,armyId,el.dataset.battleRequest==='withdraw'?'cover':el.dataset.battleRequest);$('battle-order-status').textContent=r?.message||r?.error||r?.reason||(r?.accepted?'Allied commander accepts the request.':'No allied formation is available for that request.');}});
  ui.addEventListener('click',event=>{if(finishing){event.preventDefault();event.stopImmediatePropagation();}},true);
  $('battle-pending').onclick=showEncounter;
  ui.addEventListener('change',event=>{if(event.target.dataset.battleSupport){const preview=structuredClone(getState());try{commitEncounterSupport(preview,ownSupport());$('battle-forecast').textContent=forecastText(encounterPreview(preview));}catch(error){notice(error.message);}}});
  encounterDialog.addEventListener('cancel',()=>{});field.addEventListener('cancel',event=>{event.preventDefault();setPause(!paused);});
  field.addEventListener('keydown',event=>{
    if(finishing){event.preventDefault();event.stopPropagation();return;}
    if(event.target.matches('select,input'))return;
    if(['Tab','Escape','KeyQ','KeyW','KeyA','KeyS','KeyD','ShiftLeft','ShiftRight','KeyE','Space'].includes(event.code))event.preventDefault();event.stopPropagation();
    if(event.repeat){keys.add(event.code);return;}
    if(event.code==='Tab')cameraToggle();else if(event.code==='Escape')setPause(!paused);else if(event.code==='KeyQ')action('commands');else if(event.code==='KeyA'&&camera.mode==='tactical'){order='nearest';sendOrder();}else if(event.code==='KeyE')input.interact=true;else keys.add(event.code);
  });
  field.addEventListener('keyup',event=>{keys.delete(event.code);event.stopPropagation();});
  canvas.addEventListener('contextmenu',e=>e.preventDefault());
  canvas.addEventListener('pointerdown',event=>{
    if(paused)return;canvas.focus();pointer={x:event.clientX,y:event.clientY,id:event.pointerId};canvas.setPointerCapture(event.pointerId);
    if(camera.mode==='tactical'){drag=renderer.pick(event.clientX,event.clientY);if(order==='attack'&&drag?.unitId){const target=battle.units.find(u=>u.id===drag.unitId);if(target)sendOrder({targetArmyId:target.armyId});}}
    else if(event.pointerType==='mouse'){if(event.button===0)input.attack=true;if(event.button===2)input.block=true;}
  });
  canvas.addEventListener('pointermove',event=>{
    if(camera.mode==='tactical'||paused)return;
    const dx=pointer?event.clientX-pointer.x:event.movementX,dy=pointer?event.clientY-pointer.y:event.movementY;
    if(event.pointerType==='mouse'||pointer){camera.yaw+=dx*.006;camera.pitch=Math.max(-.4,Math.min(1.05,camera.pitch+dy*.004));}
    if(pointer){pointer.x=event.clientX;pointer.y=event.clientY;}
  });
  const release=event=>{if(drag&&renderer&&camera.mode==='tactical'){const end=renderer.pick(event.clientX,event.clientY);if(end&&order!=='attack')sendOrder({x:drag.x,z:drag.z,facing:Math.hypot(end.x-drag.x,end.z-drag.z)>1?Math.atan2(end.x-drag.x,end.z-drag.z):battle.playerSide===0?0:Math.PI});}drag=null;pointer=null;input.attack=input.block=false;};
  canvas.addEventListener('pointerup',release);canvas.addEventListener('pointercancel',()=>{drag=pointer=null;input.attack=input.block=false;});
  canvas.addEventListener('wheel',event=>{event.preventDefault();if(camera.mode==='tactical')camera.distance=Math.max(35,Math.min(160,camera.distance+Math.sign(event.deltaY)*8));else{const select=$('battle-weapon'),available=[...select.options].filter(o=>!o.disabled),index=available.findIndex(o=>o.selected);select.value=available[(index+Math.sign(event.deltaY)+available.length)%available.length].value;}},{passive:false});
  const stick=$('battle-stick');
  stick.addEventListener('pointerdown',event=>{event.preventDefault();stick.setPointerCapture(event.pointerId);const r=stick.getBoundingClientRect();stickOrigin={x:r.x+r.width/2,y:r.y+r.height/2};});
  stick.addEventListener('pointermove',event=>{if(!stickOrigin)return;const x=event.clientX-stickOrigin.x,z=event.clientY-stickOrigin.y,len=Math.max(34,Math.hypot(x,z));touch={x:x/len,z:z/len};stick.firstElementChild.style.transform=`translate(${touch.x*27}px,${touch.z*27}px)`;});
  for(const type of ['pointerup','pointercancel'])stick.addEventListener(type,()=>{stickOrigin=null;touch={x:0,z:0};stick.firstElementChild.style.transform='';});
  ui.querySelectorAll('[data-battle-touch]').forEach(button=>{button.addEventListener('pointerdown',event=>{event.preventDefault();button.setPointerCapture(event.pointerId);input[button.dataset.battleTouch]=true;});for(const type of ['pointerup','pointercancel'])button.addEventListener(type,()=>{input[button.dataset.battleTouch]=false;});});
  $('battle-command-time').onchange=()=>{try{transact(s=>{configureOfflineBattles(s);s.offlineBattles.tacticalTime=$('battle-command-time').value;return {ok:true};});}catch(error){toast(error.message);}};
  settings.querySelector('#offline-battle-preference').onchange=event=>{try{transact(s=>{configureOfflineBattles(s,{preference:event.target.value});return {ok:true};});}catch(error){toast(error.message);}sync();};
  settings.querySelector('#battle-download').onclick=async()=>{const button=settings.querySelector('#battle-download'),status=settings.querySelector('#battle-download-status');button.disabled=true;try{const assets=await import('./battle-assets.mjs');await assets.downloadBattleAssets(p=>{status.textContent=typeof p==='string'?p:'Downloading battle assets…';});status.textContent='Battle assets are saved on this device for offline play.';}catch(error){status.textContent=`Download failed: ${error.message}`;}finally{button.disabled=false;}};
  function sync(){const e=active();$('battle-pending').hidden=!e||field.open||encounterDialog.open;settings.querySelector('#offline-battle-preference').value=getState().offlineBattles?.preference||'ask';}
  const onVisibility=()=>{if(document.hidden&&battle)setPause(true);};document.addEventListener('visibilitychange',onVisibility);
  const onBlur=resetInput;window.addEventListener('blur',onBlur);
  const onUnload=()=>{if(battle)try{checkpoint();}catch{}};window.addEventListener('pagehide',onUnload);
  encounterDialog.addEventListener('close',sync);field.addEventListener('close',sync);
  configureOfflineBattles(getState());sync();
  return {sync,show:showEncounter,isPending:()=>!!active(),dispose(){disposeScene();document.removeEventListener('visibilitychange',onVisibility);window.removeEventListener('blur',onBlur);window.removeEventListener('pagehide',onUnload);ui.remove();settings.remove();}};
}
