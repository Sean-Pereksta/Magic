// Stable mission controls and nonblocking, bounded radio communications.
export function createCampaignHud(host, { document = globalThis.document, storage = globalThis.localStorage, portrait = () => '', nextWave = () => {} } = {}) {
  const make = (tag, text) => {const node=document.createElement(tag); if(text)node.textContent=text; return node;};
  host.replaceChildren();
  const row=make('div'), title=make('b'), status=make('span'), next=make('button','Next wave'), health=make('progress'), details=make('details'), summary=make('summary'), list=make('div'), log=make('details'), logTitle=make('summary','Communications log'), logRows=make('div'), radio=make('aside'), image=make('img'), words=make('div'), speaker=make('b'), speech=make('p'), close=make('button','×'), skip=make('button','Skip dialogue'), mute=make('input'), label=make('label','Radio messages ');
  row.className='sfWaveRow'; status.className='sfStageStatus'; title.className='sfMissionTitle'; next.id='sfNextWave'; next.type=close.type=skip.type='button'; next.onclick=nextWave; health.max=1; health.setAttribute('aria-label','Protected target health'); details.append(summary,list); log.append(logTitle,logRows); mute.type='checkbox'; mute.checked=storage?.getItem('turncraft.radioMessages')!=='false'; label.append(mute); log.append(label); image.alt=''; radio.id='sfDialogue'; radio.className='sfRadio'; radio.setAttribute('aria-live','polite'); radio.hidden=true; close.setAttribute('aria-label','Dismiss radio message'); words.append(speaker,speech); radio.append(image,words,close,skip); row.append(title,next); host.append(row,status,health,details,log,radio);
  const entries=new Map(), queue=[], history=[];
  let timer=null, active=null, lastRun='';
  function dismiss(clear=false) { if(timer)clearTimeout(timer);timer=null;active=null;radio.hidden=true;if(clear)queue.length=0;showNext(); }
  function showNext(){
    if(active || !queue.length || !mute.checked)return;
    active=queue.shift(); speaker.textContent=active.speaker.name;speech.textContent=active.text;image.src=portrait(active.speaker.id);image.hidden=false;image.onerror=()=>image.hidden=true;radio.hidden=false;
    timer=setTimeout(()=>dismiss(),Math.min(12000,Math.max(4500,active.text.length*45)));
  }
  close.onclick=()=>dismiss();skip.onclick=()=>dismiss(true);mute.onchange=()=>{storage?.setItem('turncraft.radioMessages',String(mute.checked));if(!mute.checked)dismiss(true);else showNext();};
  function reset(){dismiss(true);history.length=0;logRows.replaceChildren();}
  function dialogue(data){
    if(!data?.text)return;
    if(history.at(-1)?.text===data.text)return;
    history.push(data);if(history.length>80)history.shift();
    const line=make('p',`${data.speaker.name}: ${data.text}`);logRows.append(line);while(logRows.children.length>80)logRows.firstChild.remove();
    if(mute.checked){queue.push(data);if(queue.length>6)queue.splice(0,queue.length-6);showNext();}
  }
  function render(state, started=true){
    host.hidden=!state||!started;if(host.hidden){dismiss(true);return;}
    mute.checked=storage?.getItem('turncraft.radioMessages')!=='false';if(!mute.checked&&active)dismiss(true);
    const run=state.missionId||state.arenaId;
    if(lastRun && lastRun!==run)reset();lastRun=run;
    title.textContent=state.title;
    const tactical=!!state.operation, preparing=state.phase==='preparation';
    status.textContent=tactical?`Stage ${Math.min(state.operation.stage+1,state.totalStages||state.operation.stage+1)} / ${state.totalStages||'?'} · ${state.stageTitle||'Operation complete'}`:`Wave ${state.wave}${state.totalWaves?` / ${state.totalWaves}`:' · ENDLESS'} · ${preparing?`${Math.ceil(state.remaining)}s preparation`:state.phase==='assault'?state.wavePlan?.label||'Incoming attack':state.phase}`;
    next.hidden=tactical;next.disabled=!preparing;next.textContent=preparing?'Next wave':'In battle';health.value=state.protectedHealth;
    const visible=state.objectives.filter(o=>!o.unavailable&&(!tactical||!(o.activateStage>state.operation.stage))&&(o.stage<0||!tactical||o.stage<=state.operation.stage));
    summary.textContent=visible.find(o=>!o.optional&&!o.completed&&o.type!=='protect')?.label||visible.find(o=>!o.completed)?.label||'Mission objectives secured';
    const ids=new Set();
    for(const objective of visible){ids.add(objective.id);let node=entries.get(objective.id);if(!node){node=make('div');node.className='sfObjective';entries.set(objective.id,node);list.append(node);}node.textContent=`${objective.completed?'✓':objective.failed?'✕':'◆'} ${objective.label}${objective.optional?' (optional)':''}${!objective.completed&&objective.progress>0?` · ${Math.round(objective.progress*100)}%`:''}${objective.blocked?' · route unsafe':''}`;}
    for(const [id,node]of entries)if(!ids.has(id)){node.remove();entries.delete(id);}
  }
  return {render,dialogue,reset,dispose(){if(timer)clearTimeout(timer);host.replaceChildren();}};
}
