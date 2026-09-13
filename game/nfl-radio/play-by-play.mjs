const STORAGE_KEY='nfl-dial:livePlayByPlay';
const ROTATION_KEY='nfl-dial:rotation';
const SCOREBOARD_URL='https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard';
export const PLAY_POLL_MS=5000;

const clean=value=>String(value??'').replace(/\s+/g,' ').trim();
const canonical=value=>({WSH:'WAS',JAC:'JAX',LA:'LAR'}[value]||value);

export function readPlayByPlaySettings(storage=globalThis.localStorage){
  try{
    const saved=JSON.parse(storage?.getItem(STORAGE_KEY)||'null')||{};
    return {enabled:!!saved.enabled};
  }catch{return {enabled:false};}
}

export function selectedGameIds(storage=globalThis.localStorage){
  try{
    const value=JSON.parse(storage?.getItem(ROTATION_KEY)||'[]');
    return Array.isArray(value)?value.filter(id=>typeof id==='string'):[];
  }catch{return [];}
}

function competition(event){return event?.competitions?.[0]||null;}
function competitors(event){return competition(event)?.competitors||[];}
function teamById(event,id){return competitors(event).find(c=>String(c.id)===String(id)||String(c.team?.id)===String(id));}
function teamLabel(entry){return entry?.team?.shortDisplayName||entry?.team?.name||entry?.team?.displayName||entry?.team?.abbreviation||'';}
function replaceAbbreviations(text,event){
  let out=clean(text);
  for(const c of competitors(event)){
    const raw=c.team?.abbreviation,aliases=[raw,canonical(raw)].filter(Boolean);
    for(const abbr of new Set(aliases))out=out.replace(new RegExp(`\\b${abbr}\\b`,'g'),teamLabel(c)||abbr);
  }
  return out.replace(/\s*&\s*/g,' and ');
}
function gameState(event){return competition(event)?.status?.type?.state||event?.status?.type?.state||'pre';}
function playIdentity(play){
  const text=clean(play?.text||play?.shortText||play?.type?.text);
  const id=clean(play?.id);
  const period=play?.period?.number||play?.period||'';
  const clock=play?.clock?.displayValue||'';
  return `${id||`${period}:${clock}`}|${text}`;
}

export function latestPlayAnnouncement(event){
  const c=competition(event);
  if(!event||!c||gameState(event)!=='in')return null;
  const situation=c.situation||{};
  const play=situation.lastPlay;
  const text=replaceAbbreviations(play?.text||play?.shortText||play?.type?.text,event);
  if(!play||!text)return null;
  const explicitTeamId=play.team?.id??play.teamId??play.possession;
  const playTeam=teamById(event,explicitTeamId)||teamById(event,situation.possession);
  const teamName=teamLabel(playTeam);
  return {
    gameId:String(event.id),
    key:playIdentity({...play,text}),
    team:teamName,
    play:text,
    speech:teamName?`${teamName}. ${text}`:text
  };
}

export function collectNewPlayAnnouncements(events,selectedIds,seenKeys=new Map(),{announceInitial=false}={}){
  const selected=new Set((selectedIds||[]).map(String));
  const announcements=[];
  for(const event of events||[]){
    const gameId=String(event?.id??'');
    if(!selected.has(gameId))continue;
    const announcement=latestPlayAnnouncement(event);
    if(!announcement)continue;
    const previous=seenKeys.get(gameId);
    if((previous&&previous!==announcement.key)||(!previous&&announceInitial))announcements.push(announcement);
    seenKeys.set(gameId,announcement.key);
  }
  return announcements;
}

async function fetchJson(url,{timeout=10000}={}){
  const controller=new AbortController();
  const timer=setTimeout(()=>controller.abort(),timeout);
  try{
    const response=await fetch(url,{cache:'no-store',signal:controller.signal});
    if(!response.ok)throw new Error(`HTTP ${response.status}`);
    return await response.json();
  }finally{clearTimeout(timer);}
}

function defaultVoice(){
  if(!('speechSynthesis' in globalThis))return null;
  const voices=speechSynthesis.getVoices?.()||[];
  return voices.find(v=>v.default&&/^en/i.test(v.lang))||voices.find(v=>/^en/i.test(v.lang))||voices.find(v=>v.default)||null;
}

function speakQueued(text,onEnd){
  if(!text||!('speechSynthesis' in globalThis)||typeof SpeechSynthesisUtterance==='undefined')return false;
  const utterance=new SpeechSynthesisUtterance(text);
  utterance.lang='en-US';utterance.rate=1.08;
  const voice=defaultVoice();if(voice)utterance.voice=voice;
  let finished=false;
  const finish=()=>{if(finished)return;finished=true;onEnd?.();};
  utterance.onend=finish;utterance.onerror=finish;
  speechSynthesis.speak(utterance);
  return true;
}

function element(tag,text,attrs={}){
  const node=document.createElement(tag);
  if(text!=null)node.textContent=text;
  for(const [key,value] of Object.entries(attrs)){
    if(key==='class')node.className=value;
    else if(key==='for')node.htmlFor=value;
    else if(key in node)node[key]=value;
    else node.setAttribute(key,value);
  }
  return node;
}

export function installLivePlayByPlay(){
  if(typeof document==='undefined'||!document.querySelector('.toolbar')||document.getElementById('playByPlayButton'))return;
  const toolbar=document.querySelector('.toolbar');
  const open=element('button','📣 Play-by-play',{id:'playByPlayButton'});
  toolbar.insertBefore(open,document.getElementById('gameUpdatesButton')||document.getElementById('refresh'));

  const dialog=element('dialog',null,{id:'playByPlayDialog'});
  const title=element('div',null,{class:'sectionTitle'});
  title.append(element('h2','Live play-by-play voice'),element('button','✕',{id:'closePlayByPlay','aria-label':'Close live play-by-play'}));
  dialog.append(title);
  dialog.append(element('p','For every live game in your rotation, check for a different latest play about every 5 seconds. Each new play is spoken as “Team. Play…” and added behind anything already in the voice queue.',{class:'availability playByPlayIntro'}));

  const controls=element('div',null,{class:'playByPlayControls'});
  const enabledLabel=element('label',null,{class:'playByPlayToggle'});
  const enabled=element('input',null,{id:'playByPlayEnabled',type:'checkbox'});
  enabledLabel.append(enabled,document.createTextNode(' Automatic live play-by-play'));
  controls.append(enabledLabel);
  controls.append(element('p','This is a separate automatic-update mode. Turning it on switches the scheduled full-summary voice mode off so announcements do not overlap.',{class:'availability playByPlayNote'}));
  dialog.append(controls);

  const actions=element('div',null,{class:'playByPlayActions'});
  const speakLatest=element('button','Speak latest plays now',{id:'speakLatestPlays'});
  actions.append(speakLatest);dialog.append(actions);
  const status=element('p','Play-by-play is off.',{id:'playByPlayStatus',class:'availability','aria-live':'polite'});dialog.append(status);
  document.body.append(dialog);

  let settings=readPlayByPlaySettings();
  let timer=null,polling=false,speaking=false,currentItem=null,generation=0;
  const seenKeys=new Map();
  const queue=[];

  const save=()=>{try{localStorage.setItem(STORAGE_KEY,JSON.stringify(settings));}catch{}};
  const queueLabel=()=>queue.length?` ${queue.length} more queued.`:'';
  const liveCountLabel=count=>`${count} selected live game${count===1?'':'s'}`;

  const paint=message=>{
    const count=selectedGameIds().length;
    enabled.checked=!!settings.enabled;
    open.textContent=settings.enabled?'📣 Play-by-play ON':'📣 Play-by-play';
    open.classList.toggle('active',!!settings.enabled);
    if(message){status.textContent=message;return;}
    status.textContent=settings.enabled
      ?`Watching ${count} selected game${count===1?'':'s'} for new plays about every 5 seconds.${queueLabel()}`
      :'Play-by-play is off.';
  };

  const stopSchedule=()=>{clearInterval(timer);timer=null;generation++;};
  const clearPending=()=>{queue.length=0;currentItem=null;};

  const drainQueue=()=>{
    if(speaking)return;
    while(queue.length){
      const next=queue.shift();
      if(!selectedGameIds().includes(next.gameId))continue;
      speaking=true;currentItem=next;
      paint(`Speaking ${next.team||'latest'} play.${queueLabel()}`);
      const started=speakQueued(next.speech,()=>{
        speaking=false;currentItem=null;
        if(settings.enabled)paint(`Play spoken.${queueLabel()}`);else paint();
        drainQueue();
      });
      if(started)return;
      speaking=false;currentItem=null;
      paint('Browser speech is unavailable on this device.');
      return;
    }
    if(settings.enabled)paint();
  };

  const enqueue=announcement=>{
    if(!announcement?.speech)return;
    if(currentItem?.gameId===announcement.gameId&&currentItem?.key===announcement.key)return;
    if(queue.some(item=>item.gameId===announcement.gameId&&item.key===announcement.key))return;
    queue.push(announcement);drainQueue();
  };

  const poll=async({prime=false,manual=false,token=generation}={})=>{
    if(polling&&!manual)return;
    const ids=selectedGameIds();
    if(!ids.length){paint('Add games to your rotation first.');return;}
    polling=true;
    if(manual)paint('Getting latest plays…');
    try{
      const board=await fetchJson(SCOREBOARD_URL,{timeout:10000});
      if(token!==generation&&!manual)return;
      const events=(board.events||[]).filter(event=>ids.includes(String(event.id)));
      const live=events.filter(event=>gameState(event)==='in');
      if(!live.length){paint('None of your selected games are live right now.');return;}
      if(manual){
        for(const event of live){
          const announcement=latestPlayAnnouncement(event);
          if(!announcement)continue;
          seenKeys.set(announcement.gameId,announcement.key);
          enqueue(announcement);
        }
      }else{
        const announcements=collectNewPlayAnnouncements(live,ids,seenKeys,{announceInitial:false});
        if(!prime)for(const announcement of announcements)enqueue(announcement);
      }
      if(prime&&!speaking)paint(`Ready. Watching ${liveCountLabel(live.length)} for the next new play.`);
      else if(!speaking&&!queue.length)paint(`Watching ${liveCountLabel(live.length)}. No new play yet.`);
    }catch{
      paint('Could not check live plays. Automatic play-by-play will retry on the next 5-second check.');
    }finally{polling=false;}
  };

  const disableSummaryUpdates=()=>{
    const summaryToggle=document.getElementById('updatesEnabled');
    if(summaryToggle?.checked){
      summaryToggle.checked=false;
      summaryToggle.dispatchEvent(new Event('change',{bubbles:true}));
    }
  };

  const schedule=()=>{
    stopSchedule();clearPending();
    if(!settings.enabled){paint();return;}
    const token=generation;
    poll({prime:true,token});
    timer=setInterval(()=>poll({token}),PLAY_POLL_MS);
    paint();
  };

  const setEnabled=value=>{
    settings={enabled:!!value};save();
    if(settings.enabled)disableSummaryUpdates();
    schedule();
  };

  enabled.onchange=()=>setEnabled(enabled.checked);
  open.onclick=()=>{settings=readPlayByPlaySettings();enabled.checked=settings.enabled;paint();dialog.showModal();};
  document.getElementById('closePlayByPlay').onclick=()=>dialog.close();
  speakLatest.onclick=()=>poll({manual:true,token:generation});

  document.addEventListener('change',event=>{
    if(event.target?.id==='updatesEnabled'&&event.target.checked&&settings.enabled){
      settings={enabled:false};save();stopSchedule();clearPending();enabled.checked=false;
      paint('Play-by-play turned off because scheduled full game updates were enabled.');
    }
  });
  document.addEventListener('visibilitychange',()=>{if(!document.hidden&&settings.enabled)poll({token:generation});});
  window.addEventListener('pagehide',()=>{stopSchedule();clearPending();});

  enabled.checked=settings.enabled;paint();schedule();
}

if(typeof document!=='undefined')installLivePlayByPlay();
