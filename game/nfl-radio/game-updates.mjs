import {
  browserSpeechAvailable,
  enqueueBrowserSpeech,
  getBrowserSpeechQueueState,
  unlockBrowserSpeech
} from './speech-queue.mjs';

const STORAGE_KEY='nfl-dial:spokenUpdates';
const ROTATION_KEY='nfl-dial:rotation';
const SCOREBOARD_URL='https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard';
const SUMMARY_URL=id=>`https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event=${encodeURIComponent(id)}`;

export const DEFAULT_UPDATE_SETTINGS={
  enabled:false,
  minutes:1,
  scoring:true,
  lastPlay:true,
  leaders:true
};

const clean=value=>String(value??'').replace(/\s+/g,' ').trim();
const canonical=value=>({WSH:'WAS',JAC:'JAX',LA:'LAR'}[value]||value);

export function readSettings(storage=globalThis.localStorage){
  try{
    const saved=JSON.parse(storage?.getItem(STORAGE_KEY)||'null')||{};
    const minutes=[1,2,3,5,10,15].includes(Number(saved.minutes))?Number(saved.minutes):1;
    return {...DEFAULT_UPDATE_SETTINGS,...saved,minutes};
  }catch{return {...DEFAULT_UPDATE_SETTINGS};}
}

export function selectedGameIds(storage=globalThis.localStorage){
  try{
    const value=JSON.parse(storage?.getItem(ROTATION_KEY)||'[]');
    return Array.isArray(value)?value.filter(id=>typeof id==='string'):[];
  }catch{return [];}
}

function competition(event){return event?.competitions?.[0]||null;}
function competitors(event){return competition(event)?.competitors||[];}
function participant(event,homeAway){return competitors(event).find(c=>c.homeAway===homeAway);}
function teamLabel(entry){return entry?.team?.shortDisplayName||entry?.team?.name||entry?.team?.displayName||entry?.team?.abbreviation||'Team';}
function teamById(event,id){return competitors(event).find(c=>String(c.id)===String(id));}
function replaceAbbreviations(text,event){
  let out=clean(text);
  for(const c of competitors(event)){
    const raw=c.team?.abbreviation,aliases=[raw,canonical(raw)].filter(Boolean);
    for(const abbr of new Set(aliases))out=out.replace(new RegExp(`\\b${abbr}\\b`,'g'),teamLabel(c));
  }
  return out.replace(/\s*&\s*/g,' and ');
}
function leaderByName(event,name){return (competition(event)?.leaders||[]).find(group=>group.name===name)?.leaders?.[0]||null;}
function leaderPhrase(event,name,label){
  const item=leaderByName(event,name);
  const athlete=item?.athlete?.displayName||item?.athlete?.fullName;
  const yards=Number(item?.value);
  if(!athlete||!Number.isFinite(yards))return '';
  return `${label} leader ${athlete}, ${Math.round(yards)} yards`;
}
function scorePlayId(play,index){return String(play?.id??`${play?.period?.number||play?.period||0}:${play?.clock?.displayValue||''}:${index}:${play?.text||''}`);}

export function buildGameUpdate(event,summary=null,options={}){
  const settings={...DEFAULT_UPDATE_SETTINGS,...options};
  const c=competition(event);
  if(!event||!c)return {speech:'',scoreIds:[],state:'unknown'};
  const away=participant(event,'away'),home=participant(event,'home');
  const awayName=teamLabel(away),homeName=teamLabel(home);
  const awayScore=clean(away?.score||'0'),homeScore=clean(home?.score||'0');
  const state=c.status?.type?.state||event.status?.type?.state||'pre';
  const status=clean(c.status?.type?.shortDetail||event.status?.type?.shortDetail||c.status?.type?.detail||event.status?.type?.detail);
  const parts=[`${awayName} ${awayScore}, ${homeName} ${homeScore}.`];
  if(status)parts.push(`${status}.`);

  const situation=c.situation;
  if(state==='in'&&situation){
    const possessing=teamById(event,situation.possession);
    if(possessing){
      const spot=replaceAbbreviations(situation.downDistanceText||situation.possessionText||'',event);
      parts.push(`${teamLabel(possessing)} has the ball${spot?`, ${spot}`:''}.`);
    }else if(situation.downDistanceText){
      parts.push(`${replaceAbbreviations(situation.downDistanceText,event)}.`);
    }
    if(settings.lastPlay&&situation.lastPlay?.text)parts.push(`Last play: ${clean(situation.lastPlay.text)}`);
  }else if(state==='post'){
    parts.push('Final.');
  }

  const seen=options.seenScoreIds instanceof Set?options.seenScoreIds:new Set(options.seenScoreIds||[]);
  const scoringPlays=Array.isArray(summary?.scoringPlays)?summary.scoringPlays:[];
  const allScoreIds=scoringPlays.map(scorePlayId);
  if(settings.scoring&&scoringPlays.length){
    const fresh=scoringPlays.map((play,index)=>({play,id:scorePlayId(play,index)})).filter(x=>!seen.has(x.id)).slice(-2);
    if(fresh.length){
      const text=fresh.map(({play})=>clean(play.text||play.shortText||play.type?.text)).filter(Boolean).join(' Next, ');
      if(text)parts.push(`Recent scoring: ${text}`);
    }
  }else if(settings.scoring&&situation?.lastPlay?.scoreValue>0&&situation.lastPlay?.text){
    parts.push(`Scoring play: ${clean(situation.lastPlay.text)}`);
  }

  if(settings.leaders){
    const leaders=[
      leaderPhrase(event,'passingYards','Passing'),
      leaderPhrase(event,'rushingYards','Rushing'),
      leaderPhrase(event,'receivingYards','Receiving')
    ].filter(Boolean);
    if(leaders.length)parts.push(`${leaders.join('. ')}.`);
  }
  return {speech:parts.join(' ').replace(/\s+/g,' ').trim(),scoreIds:allScoreIds,state};
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

async function summaryFor(id){
  try{return await fetchJson(SUMMARY_URL(id),{timeout:9000});}catch{return null;}
}

export function speakBrowserDefault(text,{onStart,onEnd,onError,key}={}){
  return enqueueBrowserSpeech(text,{
    source:'game-update',
    key:key||`game-update:${text}`,
    rate:1.04,
    onStart,
    onEnd,
    onError
  });
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

export function installSpokenUpdates(){
  if(typeof document==='undefined'||!document.querySelector('.toolbar')||document.getElementById('gameUpdatesButton'))return;
  const toolbar=document.querySelector('.toolbar');
  const open=element('button','🔊 Game updates',{id:'gameUpdatesButton'});
  toolbar.insertBefore(open,document.getElementById('refresh'));

  const dialog=element('dialog',null,{id:'gameUpdatesDialog'});
  const title=element('div',null,{class:'sectionTitle'});
  title.append(element('h2','Spoken game updates'),element('button','✕',{id:'closeGameUpdates','aria-label':'Close game updates'}));
  dialog.append(title);

  const intro=element('p','Speak live score, clock, possession, field position, recent scoring, last play, and passing/rushing/receiving leaders for games in your rotation. Uses the browser’s default voice and shares one audio queue with live play-by-play.');
  intro.className='availability updateIntro';
  dialog.append(intro);

  const controls=element('div',null,{class:'updateControls'});
  const enabledLabel=element('label',null,{class:'updateToggle'});
  const enabled=element('input',null,{id:'updatesEnabled',type:'checkbox'});
  enabledLabel.append(enabled,document.createTextNode(' Speak automatic updates'));
  controls.append(enabledLabel);

  const intervalLabel=element('label','How often');
  const interval=element('select',null,{id:'updateInterval'});
  for(const value of [1,2,3,5,10,15])interval.append(element('option',value===1?'Every minute':`Every ${value} minutes`,{value:String(value)}));
  intervalLabel.append(interval);
  controls.append(intervalLabel);

  for(const [id,text] of [['updatesScoring','Include recent scoring plays'],['updatesLastPlay','Include last play'],['updatesLeaders','Include passing, rushing, and receiving leaders']]){
    const label=element('label',null,{class:'updateToggle'}),input=element('input',null,{id,type:'checkbox'});
    label.append(input,document.createTextNode(` ${text}`));
    controls.append(label);
  }
  dialog.append(controls);

  const actions=element('div',null,{class:'updateActions'});
  const speakNow=element('button','Speak update now',{id:'speakUpdateNow'});
  actions.append(speakNow);
  dialog.append(actions);
  const status=element('p','Updates are off.',{id:'gameUpdateStatus',class:'availability','aria-live':'polite'});
  dialog.append(status);
  document.body.append(dialog);

  let settings=readSettings();
  let timer=null;
  let fetching=false;
  const seenScores=new Map();

  const saveSettings=()=>{
    settings={
      enabled:enabled.checked,
      minutes:Number(interval.value)||1,
      scoring:document.getElementById('updatesScoring').checked,
      lastPlay:document.getElementById('updatesLastPlay').checked,
      leaders:document.getElementById('updatesLeaders').checked
    };
    try{localStorage.setItem(STORAGE_KEY,JSON.stringify(settings));}catch{}
    if(settings.enabled)unlockBrowserSpeech();
    schedule();
    paint();
  };

  const hydrate=()=>{
    enabled.checked=!!settings.enabled;
    interval.value=String(settings.minutes);
    document.getElementById('updatesScoring').checked=settings.scoring!==false;
    document.getElementById('updatesLastPlay').checked=settings.lastPlay!==false;
    document.getElementById('updatesLeaders').checked=settings.leaders!==false;
  };

  const paint=message=>{
    const count=selectedGameIds().length;
    open.textContent=settings.enabled?`🔊 Updates ON · ${settings.minutes}m`:'🔊 Game updates';
    open.classList.toggle('active',!!settings.enabled);
    if(message){status.textContent=message;return;}
    if(!browserSpeechAvailable()){
      status.textContent='This browser does not expose text-to-speech.';
      return;
    }
    const voice=getBrowserSpeechQueueState();
    status.textContent=settings.enabled
      ?`Checking ${count} selected game${count===1?'':'s'} every ${settings.minutes===1?'minute':`${settings.minutes} minutes`}. Shared voice queue: ${voice.queued}${voice.speaking?' + 1 speaking':''}.`
      :'Updates are off. Interval defaults to one minute when enabled.';
  };

  const schedule=()=>{
    clearInterval(timer);
    timer=null;
    if(settings.enabled)timer=setInterval(()=>announce(false),settings.minutes*60000);
  };

  const announce=async manual=>{
    if(fetching)return;
    const ids=selectedGameIds();
    if(!ids.length){paint('Add games to your rotation first.');return;}
    fetching=true;
    paint('Getting live game updates…');
    try{
      const board=await fetchJson(SCOREBOARD_URL,{timeout:10000});
      const events=(board.events||[]).filter(event=>ids.includes(String(event.id)));
      const live=events.filter(event=>(competition(event)?.status?.type?.state||event.status?.type?.state)==='in');
      const targets=live.length?live:(manual?events:[]);
      if(!targets.length){paint('None of your selected games are live right now.');return;}

      const summaries=await Promise.all(targets.map(event=>settings.scoring?summaryFor(event.id):Promise.resolve(null)));
      const spoken=[];
      targets.forEach((event,index)=>{
        const seen=seenScores.get(String(event.id))||new Set();
        const update=buildGameUpdate(event,summaries[index],{...settings,seenScoreIds:seen});
        if(update.speech)spoken.push(update.speech);
        seenScores.set(String(event.id),new Set(update.scoreIds));
      });
      const text=spoken.join(' Next game. ');
      if(!text){paint('Live data is temporarily unavailable.');return;}

      const key=`game-update:${targets.map(event=>event.id).join(',')}:${text}`;
      const queued=speakBrowserDefault(text,{
        key,
        onStart:()=>paint(`Speaking ${targets.length} game${targets.length===1?'':'s'}…`),
        onEnd:()=>paint(`Last spoken update ${new Date().toLocaleTimeString([],{hour:'numeric',minute:'2-digit'})}.`),
        onError:()=>paint('Browser voice could not speak that update. Future checks will keep retrying normally.')
      });
      if(queued){
        const voice=getBrowserSpeechQueueState();
        paint(voice.speaking&&voice.queued?`Game update queued. ${voice.queued} item${voice.queued===1?'':'s'} waiting behind the current voice.`:'Game update added to the voice queue.');
      }else{
        paint('That exact update is already queued or browser speech is unavailable.');
      }
    }catch{
      paint('Could not load live game updates. The next scheduled update will retry.');
    }finally{
      fetching=false;
    }
  };

  hydrate();
  paint();
  schedule();

  const rearm=()=>{if(settings.enabled)unlockBrowserSpeech();};
  document.addEventListener('pointerdown',rearm,{passive:true});
  document.addEventListener('keydown',rearm);

  open.onclick=()=>{
    settings=readSettings();
    hydrate();
    if(settings.enabled)unlockBrowserSpeech();
    paint();
    dialog.showModal();
  };
  document.getElementById('closeGameUpdates').onclick=()=>dialog.close();
  for(const control of controls.querySelectorAll('input,select'))control.onchange=saveSettings;
  speakNow.onclick=()=>{
    unlockBrowserSpeech();
    announce(true);
  };
  window.addEventListener('pagehide',()=>clearInterval(timer));
}

if(typeof document!=='undefined')installSpokenUpdates();
