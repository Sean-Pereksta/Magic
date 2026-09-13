import {
  browserSpeechAvailable,
  enqueueBrowserSpeech,
  getBrowserSpeechQueueState,
  removeQueuedSpeech,
  unlockBrowserSpeech
} from './speech-queue.mjs';
import {setRadioDucked} from './radio-audio-bridge.mjs';

const STORAGE_KEY='nfl-dial:livePlayByPlay';
const ROTATION_KEY='nfl-dial:rotation';
const SCOREBOARD_URL='https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard';
export const PLAY_POLL_MS=5000;

const clean=value=>String(value??'').replace(/\s+/g,' ').trim();
const canonical=value=>({WSH:'WAS',JAC:'JAX',LA:'LAR'}[value]||value);

export function readPlayByPlaySettings(storage=globalThis.localStorage){
  try{
    const saved=JSON.parse(storage?.getItem(STORAGE_KEY)||'null')||{};
    return {enabled:!!saved.enabled,duckRadio:saved.duckRadio!==false};
  }catch{return {enabled:false,duckRadio:true};}
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
function eventMatchup(event){
  const away=competitors(event).find(c=>c.homeAway==='away');
  const home=competitors(event).find(c=>c.homeAway==='home');
  return [canonical(away?.team?.abbreviation),canonical(home?.team?.abbreviation)].filter(Boolean);
}
export function radioLabelMatchup(label){
  const match=clean(label).match(/^([A-Z]{2,3})\s+vs\s+([A-Z]{2,3})\b/);
  return match?[canonical(match[1]),canonical(match[2])]:[];
}
export function matchupMatchesRadioLabel(matchup,label){
  const current=radioLabelMatchup(label);
  return current.length===2&&matchup?.length===2&&current[0]===canonical(matchup[0])&&current[1]===canonical(matchup[1]);
}
export function eventMatchesRadioLabel(event,label){return matchupMatchesRadioLabel(eventMatchup(event),label);}

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
    matchup:eventMatchup(event),
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
  dialog.append(element('p','For every other live game in your rotation, check for a different latest play about every 5 seconds. The game currently selected on the radio is automatically excluded.',{class:'availability playByPlayIntro'}));

  const controls=element('div',null,{class:'playByPlayControls'});
  const enabledLabel=element('label',null,{class:'playByPlayToggle'});
  const enabled=element('input',null,{id:'playByPlayEnabled',type:'checkbox'});
  enabledLabel.append(enabled,document.createTextNode(' Automatic live play-by-play'));
  controls.append(enabledLabel);

  const duckLabel=element('label',null,{class:'playByPlayToggle'});
  const duckRadio=element('input',null,{id:'playByPlayDuckRadio',type:'checkbox'});
  duckLabel.append(duckRadio,document.createTextNode(' Quiet radio while play-by-play speaks'));
  controls.append(duckLabel);
  controls.append(element('p','When radio ducking is on, the station drops to a low background level only while a play-by-play announcement is speaking, then returns to its exact previous volume. Scheduled game updates and play-by-play still share one FIFO browser-voice queue.',{class:'availability playByPlayNote'}));
  dialog.append(controls);

  const actions=element('div',null,{class:'playByPlayActions'});
  const speakLatest=element('button','Speak latest plays now',{id:'speakLatestPlays'});
  actions.append(speakLatest);
  dialog.append(actions);
  const status=element('p','Play-by-play is off.',{id:'playByPlayStatus',class:'availability','aria-live':'polite'});
  dialog.append(status);
  document.body.append(dialog);

  let settings=readPlayByPlaySettings();
  let timer=null,polling=false,generation=0;
  const seenKeys=new Map();

  const currentRadioLabel=()=>document.getElementById('nowGame')?.textContent||'';
  const save=()=>{try{localStorage.setItem(STORAGE_KEY,JSON.stringify(settings));}catch{}};
  const liveCountLabel=count=>`${count} other selected live game${count===1?'':'s'}`;
  const hydrate=()=>{enabled.checked=!!settings.enabled;duckRadio.checked=settings.duckRadio!==false;};

  const paint=message=>{
    const count=selectedGameIds().length;
    hydrate();
    open.textContent=settings.enabled?'📣 Play-by-play ON':'📣 Play-by-play';
    open.classList.toggle('active',!!settings.enabled);
    if(message){status.textContent=message;return;}
    if(!browserSpeechAvailable()){
      status.textContent='This browser does not expose text-to-speech.';
      return;
    }
    const voice=getBrowserSpeechQueueState();
    status.textContent=settings.enabled
      ?`Watching ${count} selected game${count===1?'':'s'} about every 5 seconds; the current radio game is excluded. Radio ducking ${settings.duckRadio?'ON':'OFF'}. Shared voice queue: ${voice.queued}${voice.speaking?' + 1 speaking':''}.`
      :'Play-by-play is off.';
  };

  const stopSchedule=()=>{
    clearInterval(timer);
    timer=null;
    generation++;
  };

  const enqueue=announcement=>{
    if(!announcement?.speech)return false;
    const speechKey=`play-by-play:${announcement.gameId}:${announcement.key}`;
    const currentGameIsThis=()=>matchupMatchesRadioLabel(announcement.matchup,currentRadioLabel());
    return enqueueBrowserSpeech(announcement.speech,{
      source:'play-by-play',
      key:speechKey,
      rate:1.08,
      shouldPlay:()=>settings.enabled&&selectedGameIds().includes(announcement.gameId)&&!currentGameIsThis(),
      onStart:()=>{
        if(settings.duckRadio)setRadioDucked(true);
        paint(`Speaking ${announcement.team||'latest'} play${settings.duckRadio?' over quieted radio':''}.`);
      },
      onEnd:()=>{
        setRadioDucked(false);
        paint('Play spoken. Watching for the next new play.');
      },
      onError:()=>{
        setRadioDucked(false);
        paint('Browser voice could not speak that play. New plays will keep queuing normally.');
      },
      onSkip:()=>{setRadioDucked(false);paint();}
    });
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

      const radioLabel=currentRadioLabel();
      const suppressed=live.filter(event=>eventMatchesRadioLabel(event,radioLabel));
      const eligible=live.filter(event=>!eventMatchesRadioLabel(event,radioLabel));

      for(const event of suppressed){
        const announcement=latestPlayAnnouncement(event);
        if(announcement)seenKeys.set(announcement.gameId,announcement.key);
      }

      if(!eligible.length){
        paint('The only selected live game is the one currently on the radio, so play-by-play is staying silent for it.');
        return;
      }

      let added=0;
      if(manual){
        for(const event of eligible){
          const announcement=latestPlayAnnouncement(event);
          if(!announcement)continue;
          seenKeys.set(announcement.gameId,announcement.key);
          if(enqueue(announcement))added++;
        }
      }else{
        const announcements=collectNewPlayAnnouncements(eligible,ids,seenKeys,{announceInitial:false});
        if(!prime)for(const announcement of announcements)if(enqueue(announcement))added++;
      }

      if(prime)paint(`Ready. Watching ${liveCountLabel(eligible.length)}; current radio game excluded.`);
      else if(added){
        const voice=getBrowserSpeechQueueState();
        paint(`${added} new play${added===1?'':'s'} added to the shared voice queue. ${voice.queued} waiting${voice.speaking?' + 1 speaking now':''}.`);
      }else{
        paint(`Watching ${liveCountLabel(eligible.length)}. No new play yet; current radio game excluded.`);
      }
    }catch{
      paint('Could not check live plays. Automatic play-by-play will retry on the next 5-second check.');
    }finally{
      polling=false;
    }
  };

  const schedule=()=>{
    stopSchedule();
    if(!settings.enabled){paint();return;}
    const token=generation;
    poll({prime:true,token});
    timer=setInterval(()=>poll({token}),PLAY_POLL_MS);
    paint();
  };

  const setEnabled=value=>{
    settings={...settings,enabled:!!value};
    save();
    if(settings.enabled)unlockBrowserSpeech();
    else{
      removeQueuedSpeech(item=>item.source==='play-by-play');
      setRadioDucked(false);
    }
    schedule();
  };

  const setDuckRadio=value=>{
    settings={...settings,duckRadio:!!value};
    save();
    const voice=getBrowserSpeechQueueState();
    setRadioDucked(!!settings.duckRadio&&voice.current?.source==='play-by-play');
    paint();
  };

  const rearm=()=>{if(settings.enabled)unlockBrowserSpeech();};
  document.addEventListener('pointerdown',rearm,{passive:true});
  document.addEventListener('keydown',rearm);

  enabled.onchange=()=>setEnabled(enabled.checked);
  duckRadio.onchange=()=>setDuckRadio(duckRadio.checked);
  open.onclick=()=>{
    settings=readPlayByPlaySettings();
    hydrate();
    if(settings.enabled)unlockBrowserSpeech();
    paint();
    dialog.showModal();
  };
  document.getElementById('closePlayByPlay').onclick=()=>dialog.close();
  speakLatest.onclick=()=>{
    unlockBrowserSpeech();
    poll({manual:true,token:generation});
  };

  document.addEventListener('visibilitychange',()=>{
    if(!document.hidden&&settings.enabled)poll({token:generation});
  });
  window.addEventListener('pagehide',()=>{clearInterval(timer);setRadioDucked(false);});

  hydrate();
  paint();
  schedule();
}

if(typeof document!=='undefined')installLivePlayByPlay();
