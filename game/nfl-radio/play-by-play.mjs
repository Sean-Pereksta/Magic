import {browserSpeechAvailable,enqueueBrowserSpeech,getBrowserSpeechQueueState,removeQueuedSpeech,unlockBrowserSpeech,onBrowserSpeechQueueState} from './speech-queue.mjs';
import {setRadioDucked} from './radio-audio-bridge.mjs';
import {formatPlay,mergePlayers,summaryPlayers,rosterPlayers,shouldAnnouncePlay} from './play-formatter.mjs';
import {createPlayTracker,playIdentity,playsFromSummary} from './play-tracker.mjs';
import {STORAGE_KEY,readPlayByPlaySettings,settingsForGame,selectedGameIds,normalizeScoreInterval} from './play-settings.mjs';
import {readVoiceSettings,saveVoiceSettings,englishVoices,playVoiceOptions} from './voice-settings.mjs';
export {readPlayByPlaySettings,selectedGameIds,normalizeScoreInterval,DEFAULT_SCORE_INTERVAL_MINUTES,MAX_SCORE_INTERVAL_MINUTES} from './play-settings.mjs';
export const PLAY_POLL_MS=5000;
const SCOREBOARD_URL='https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard';
const API='https://site.api.espn.com/apis/site/v2/sports/football/nfl';
const searchKey=value=>String(value??'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/[^a-z0-9]+/g,' ').trim();
const clean=value=>String(value??'').replace(/\s+/g,' ').trim();
const canonical=value=>({WSH:'WAS',JAC:'JAX',LA:'LAR'}[value]||value);
const scoreNumber=value=>Number.isFinite(Number(value))?Number(value):0;
const competition=event=>event?.competitions?.[0]||null;
const competitors=event=>competition(event)?.competitors||[];
const teamLabel=entry=>entry?.team?.shortDisplayName||entry?.team?.name||entry?.team?.displayName||entry?.team?.abbreviation||'';
const gameState=event=>competition(event)?.status?.type?.state||event?.status?.type?.state||'pre';
function eventMatchup(event){return ['away','home'].map(role=>canonical(competitors(event).find(c=>c.homeAway===role)?.team?.abbreviation)).filter(Boolean);}
export function radioLabelMatchup(label){const match=clean(label).match(/^([A-Z]{2,3})\s+vs\s+([A-Z]{2,3})\b/);return match?[canonical(match[1]),canonical(match[2])]:[];}
export function matchupMatchesRadioLabel(matchup,label){const current=radioLabelMatchup(label);return current.length===2&&matchup?.length===2&&current[0]===canonical(matchup[0])&&current[1]===canonical(matchup[1]);}
export function eventMatchesRadioLabel(event,label){return matchupMatchesRadioLabel(eventMatchup(event),label);}
export function latestPlayAnnouncement(event,{players=[]}={}){
  const raw=competition(event)?.situation?.lastPlay;
  if(!raw||gameState(event)!=='in')return null;
  const formatted=formatPlay(raw,{event,players:mergePlayers(players,summaryPlayers(null,event))});
  return formatted.text?{...formatted,gameId:String(event.id),key:playIdentity(raw),matchup:eventMatchup(event),raw,play:formatted.text,speech:formatted.text}:null;
}
export function collectNewPlayAnnouncements(events,selectedIds,seenKeys=new Map(),{announceInitial=false,settings={},players=[]}={}){
  const selected=new Set((selectedIds||[]).map(String)),announcements=[];
  for(const event of events||[]){
    if(!selected.has(String(event.id)))continue;
    const out=latestPlayAnnouncement(event,{players});if(!out)continue;
    const previous=seenKeys.get(out.gameId);
    const seen=previous instanceof Set?previous:new Set(previous?[previous]:[]);
    const fresh=!seen.has(out.key);seen.add(out.key);seenKeys.set(out.gameId,seen);
    if(fresh&&(previous||announceInitial)&&shouldAnnouncePlay(out.raw,settingsForGame(settings,out.gameId),{event,players,formatted:out}))announcements.push(out);
  }
  return announcements;
}
function scoreStatusLabel(event){
  const c=competition(event);
  const state=gameState(event);
  const detail=clean(c?.status?.type?.shortDetail||c?.status?.type?.detail||event?.status?.type?.shortDetail||event?.status?.type?.detail);
  if(state==='post')return 'Final';
  if(/half/i.test(detail))return 'Halftime';
  return '';
}

export function scoreLine(event){
  const state=gameState(event);
  if(state==='pre')return '';
  const away=competitors(event).find(c=>c.homeAway==='away');
  const home=competitors(event).find(c=>c.homeAway==='home');
  const awayName=teamLabel(away);
  const homeName=teamLabel(home);
  if(!awayName||!homeName)return '';
  const prefix=scoreStatusLabel(event);
  const line=`${awayName} ${scoreNumber(away?.score)}, ${homeName} ${scoreNumber(home?.score)}.`;
  return prefix?`${prefix}: ${line}`:line;
}

export function buildScoreUpdateSpeech(events,{selectedIds=[],scope='rotation'}={}){
  const selected=new Set((selectedIds||[]).map(String));
  const lines=(events||[])
    .filter(event=>gameState(event)!=='pre')
    .filter(event=>scope==='all'||selected.has(String(event?.id??'')))
    .map(scoreLine)
    .filter(Boolean);
  return lines.length?`NFL score update. ${lines.join(' ')}`:'';
}

async function fetchJson(url){
  const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),9000);
  try{const response=await fetch(url,{cache:'no-store',signal:controller.signal});if(!response.ok)throw new Error(`HTTP ${response.status}`);return await response.json();}finally{clearTimeout(timer);}
}
function element(tag,text,attrs={}){
  const node=document.createElement(tag);if(text!=null)node.textContent=text;
  for(const [key,value] of Object.entries(attrs))if(key==='class')node.className=value;else if(key in node)node[key]=value;else node.setAttribute(key,value);
  return node;
}
export function installLivePlayByPlay(){
  if(typeof document==='undefined'||!document.getElementById('activeGame'))return;
  const $=id=>document.getElementById(id);
  let settings=readPlayByPlaySettings(),voiceSettings=readVoiceSettings();
  let gameOptionsKey='',playerPaintKey='';
  let events=[],viewedGame='',polling=false,nextScoreAt=Date.now()+settings.scoreIntervalMinutes*60000,disposed=false,dataFailed=false;
  const tracker=createPlayTracker(),summaries=new Map(),rosters=new Map(),playersByGame=new Map(),historyStatus=new Map();
  const liveGames=new Set();
  const save=()=>{try{localStorage.setItem(STORAGE_KEY,JSON.stringify(settings));}catch{}};
  const currentRadioId=()=>$('nowGame')?.dataset.gameId||'';
  const monitored=()=>selectedGameIds();
  const selectedPlayers=id=>settings.selectedPlayersByGame[String(id)]||[];
  const currentEvent=()=>events.find(e=>String(e.id)===viewedGame);
  const allowed=entry=>{
    const event=events.find(e=>String(e.id)===entry.gameId);
    const latest=tracker.history(entry.gameId).find(p=>p.key===entry.key);
    if(latest&&latest.text!==entry.text)return false;
    return settings.enabled&&monitored().includes(entry.gameId)&&(settings.includeRadioGame||currentRadioId()!==entry.gameId)&&
      shouldAnnouncePlay(entry.raw,settingsForGame(settings,entry.gameId),{event,players:playersByGame.get(entry.gameId)||[],formatted:entry});
  };
  const status=message=>{$('playByPlayStatus').textContent=message;};
  const paintControls=()=>{
    for(const button of document.querySelectorAll('[data-play-mode]'))button.setAttribute('aria-pressed',String(button.dataset.playMode===settings.mode));
    $('announcementsToggle').textContent=settings.enabled?'Voice on':'Enable voice';
    $('announcementsToggle').setAttribute('aria-pressed',String(settings.enabled));
    $('playByPlayEnabled').checked=settings.enabled;$('playByPlayDuckRadio').checked=settings.duckRadio;
    $('includeRadioGame').checked=settings.includeRadioGame;$('playByPlayScoreInterval').value=settings.scoreIntervalMinutes;$('playByPlayScoreScope').value=settings.scoreScope;
    $('voiceStyle').value=voiceSettings.style;
    const followed=monitored().flatMap(id=>selectedPlayers(id));
    const count=followed.length;
    const names=[...new Set(followed.map(p=>p.name))];
    $('selectedPlayerSummary').textContent=names.slice(0,4).join(' · ')+(names.length>4?` · +${names.length-4} more`:'');
    $('selectedPlayerSummary').hidden=settings.mode!=='players'||!count;
    $('selectedPlayerCount').textContent=count?`${count} selected`:'Choose players';
    $('choosePlayers').hidden=settings.mode!=='players';
    $('announcementHint').textContent=!browserSpeechAvailable()?'Voice is unavailable in this browser. Live transcripts still work.':
      !settings.enabled?'Voice is paused. Your live transcript keeps updating.':settings.mode==='players'&&!count?'Choose players to hear their plays.':
      settings.mode==='touchdowns'?'Touchdown calls from your monitored games.':settings.mode==='players'?'Calls when your selected players are involved.':'Every new play from your monitored games.';
  };
  const paintVoiceList=()=>{
    const voices=englishVoices(globalThis.speechSynthesis?.getVoices?.()||[]),select=$('voiceSelect');
    select.replaceChildren(element('option','Automatic · best available English',{value:''}));
    for(const voice of voices)select.append(element('option',`${voice.name} · ${voice.lang}`,{value:voice.voiceURI}));
    if(voiceSettings.voiceURI&&!voices.some(v=>v.voiceURI===voiceSettings.voiceURI))select.append(element('option','Saved voice unavailable · using automatic',{value:voiceSettings.voiceURI}));
    select.value=voiceSettings.voiceURI;
  };
  const paintGameOptions=()=>{
    const ids=monitored(),available=events.filter(e=>ids.includes(String(e.id)));
    tracker.retain(ids);
    for(const cache of [summaries,playersByGame,historyStatus])for(const id of cache.keys())if(!ids.includes(id))cache.delete(id);
    for(const id of liveGames)if(!ids.includes(id))liveGames.delete(id);
    if(!available.some(e=>String(e.id)===viewedGame))viewedGame=available.find(e=>String(e.id)===currentRadioId())?.id||available[0]?.id||'';
    viewedGame=String(viewedGame);
    const optionsKey=available.map(e=>`${e.id}:${eventMatchup(e).join('-')}`).join('|');
    if(optionsKey===gameOptionsKey){$('transcriptGame').value=viewedGame;return;}
    gameOptionsKey=optionsKey;
    for(const id of ['transcriptGame','playerGame']){
      const select=$(id),previous=select.value;
      select.replaceChildren();
      if(!available.length)select.append(element('option','Add a game to your rotation',{value:''}));
      for(const event of available)select.append(element('option',eventMatchup(event).join(' vs '),{value:String(event.id)}));
      select.value=id==='transcriptGame'?viewedGame:available.some(e=>String(e.id)===previous)?previous:viewedGame;
    }
  };
  function paintHero(){
    const event=currentEvent(),c=competition(event),hero=$('gameScore');hero.replaceChildren();
    $('gameLiveState').textContent=dataFailed?'RECONNECTING':event?gameState(event)==='in'?'LIVE':gameState(event)==='post'?'FINAL':'UPCOMING':'YOUR DIAL';
    $('gameLiveState').classList.toggle('isLive',!!event&&gameState(event)==='in'&&!dataFailed);
    if(!event){hero.append(element('p','Pick your games. We’ll follow the action.',{class:'gameEmpty'}));$('gameSituation').textContent='Add games below to start your live companion.';paintTranscript();return;}
    const gameStatus=c?.status||event.status||{};
    for(const role of ['away','home']){
      const entry=competitors(event).find(c=>c.homeAway===role),team=entry?.team||{};
      const side=element('div',null,{class:`scoreTeam ${role}`});
      const logo=team.logo||team.logos?.[0]?.href;
      if(/^https:\/\//.test(logo||'')){const img=element('img',null,{src:logo,alt:'',width:72,height:72});img.onerror=()=>{img.hidden=true;};side.append(img);}
      const name=element('div',null,{class:'teamIdentity'});name.append(element('span',team.location||'',{class:'teamCity'}),element('strong',team.name||team.shortDisplayName||team.abbreviation||'Team'));
      if(gameState(event)==='in'&&String(c.situation?.possession)===String(team.id||entry.id))name.append(element('span','● Possession',{class:'possession'}));
      side.append(name,element('b',gameState(event)==='pre'?'—':entry?.score??'—',{class:'scoreValue'}));hero.append(side);
    }
    const clock=gameStatus.displayClock,period=gameStatus.period;
    const time=gameState(event)==='in'&&clock&&period?`${period>4?'OT':`Q${period}`} · ${clock}`:gameStatus.type?.shortDetail||gameStatus.type?.detail||'Scheduled';
    $('gameSituation').textContent=[time,c?.situation?.downDistanceText].filter(Boolean).join('  ·  ');
    paintTranscript();
  }
  function paintTranscript(){
    const history=tracker.history(viewedGame),latest=history.at(-1),list=$('transcriptList');
    $('latestPlay').textContent=latest?.text||'Waiting for the next play.';
    $('latestPlayLabel').textContent=latest?.label||'LIVE PLAY CALL';
    $('latestCall').dataset.kind=latest?.kind||'play';
    $('transcriptCount').textContent=String(history.length);
    $('transcriptMeta').textContent=historyStatus.get(viewedGame)||'Recent plays appear here when ESPN posts them.';
    list.replaceChildren();
    for(const entry of history.slice().reverse()){
      const row=element('li',null,{class:`transcriptPlay ${entry.important?'important':''}`});row.dataset.kind=entry.kind;
      const meta=element('div',null,{class:'playMeta'});meta.append(element('strong',entry.label),element('span',[entry.period?`Q${entry.period}`:'',entry.clock].filter(Boolean).join(' · ')));
      row.append(meta,element('p',entry.text));list.append(row);
    }
    if(!history.length)list.append(element('li','No plays yet for this game.',{class:'emptyState'}));
  }
  async function ensureRoster(team){
    if(!team?.id)return [];
    const id=String(team.id),existing=rosters.get(id);
    if(existing?.pending)return existing.pending;
    if(existing&&Date.now()<existing.expires)return existing.players;
    const entry={players:existing?.players||[],expires:Date.now()+60000};
    entry.pending=fetchJson(`${API}/teams/${encodeURIComponent(id)}/roster`).then(data=>{entry.players=rosterPlayers(data,team);entry.expires=Date.now()+3600000;entry.failed=false;return entry.players;}).catch(()=>{entry.failed=true;return entry.players;}).finally(()=>{entry.pending=null;});
    rosters.set(id,entry);return entry.pending;
  }
  async function loadPlayers(event,summary){
    const lists=await Promise.all(competitors(event).map(c=>ensureRoster(c.team)));
    const id=String(event.id);
    const saved=selectedPlayers(id).map(p=>({id:p.id,displayName:p.name,team:competitors(event).find(c=>c.team.abbreviation===p.team)?.team||{abbreviation:p.team}}));
    const players=mergePlayers(saved,...lists,summaryPlayers(summary,event));playersByGame.set(id,players);
    return players;
  }
  function paintPlayers(){
    const id=$('playerGame').value,event=events.find(e=>String(e.id)===id),selected=selectedPlayers(id),query=searchKey($('playerSearch').value);
    const players=playersByGame.get(id)||[],list=$('playerList'),tags=$('selectedPlayers');
    const renderKey=JSON.stringify([id,query,selected,players,competitors(event).map(c=>{const r=rosters.get(String(c.team.id));return [!!r?.pending,!!r?.failed];})]);
    if(renderKey===playerPaintKey)return;playerPaintKey=renderKey;list.replaceChildren();tags.replaceChildren();
    const toggle=player=>{
      const existing=selectedPlayers(id),has=existing.some(p=>p.id===player.id);
      settings.selectedPlayersByGame[id]=has?existing.filter(p=>p.id!==player.id):[...existing,{id:player.id,name:player.name,team:player.team}];
      save();paintPlayers();paintControls();removeQueuedSpeech(item=>item.source==='play-by-play'&&!settings.enabled);
    };
    for(const player of selected){const chip=element('button',`${player.name} ×`,{class:'playerChip','aria-label':`Remove ${player.name}`});chip.onclick=()=>toggle(player);tags.append(chip);}
    if(!selected.length)tags.append(element('p','No players selected for this game.',{class:'emptyState'}));
    for(const competitor of competitors(event)){
      const team=competitor.team,group=element('section',null,{class:'rosterGroup'});group.append(element('h3',team.displayName||team.shortDisplayName||team.abbreviation));
      const matches=players.filter(p=>(p.teamId===String(team.id)||p.team===team.abbreviation)&&p.id&&searchKey(`${p.name} ${p.position}`).includes(query)).sort((a,b)=>a.name.localeCompare(b.name));
      for(const player of matches){const b=element('button',null,{class:'playerOption','aria-pressed':String(selected.some(p=>p.id===player.id))});b.append(element('span',player.name),element('small',player.position||team.abbreviation));b.onclick=()=>toggle(player);group.append(b);}
      if(!matches.length)group.append(element('p',query?'No matching players.':rosters.get(String(team.id))?.pending?'Loading roster…':'Roster temporarily unavailable. Try again.',{class:'emptyState'}));
      list.append(group);
    }
    $('rosterStatus').textContent=!id?'Add a game to your rotation first.':competitors(event).some(c=>rosters.get(String(c.team.id))?.failed)?'Some roster data could not load. Available players are shown; retry to load the rest.':`${players.length} players available · select anyone on either team`;
  }
  function enqueuePlay(entry,{manual=false}={}){
    if(!entry?.text||(!manual&&!allowed(entry)))return false;
    return enqueueBrowserSpeech(entry.text,{
      source:'play-by-play',key:`play-by-play:${entry.gameId}:${entry.key}`,...playVoiceOptions(entry,voiceSettings.style),
      shouldPlay:()=>manual?monitored().includes(entry.gameId):allowed(entry),
      onStart:()=>{if(settings.duckRadio)setRadioDucked(true);status(`Speaking: ${entry.label}`);},
      onEnd:()=>{setRadioDucked(false);status('Ready for the next play.');},
      onError:()=>{setRadioDucked(false);status('Voice stopped. New announcements will keep queuing.');},
      onSkip:()=>setRadioDucked(false)
    });
  }
  function enqueueScores({manual=false}={}){
    const speech=buildScoreUpdateSpeech(events,{selectedIds:monitored(),scope:settings.scoreScope});
    if(!speech){status('No started games available for a score update.');return;}
    enqueueBrowserSpeech(speech,{source:'score-update',key:`score-update:${speech}`,rate:1.08,shouldPlay:()=>manual||settings.enabled,
      onStart:()=>{if(settings.duckRadio)setRadioDucked(true);},onEnd:()=>setRadioDucked(false),onError:()=>setRadioDucked(false)});
  }
  async function poll(){
    if(polling||disposed||!monitored().length)return;
    polling=true;
    try{
      const board=await fetchJson(SCOREBOARD_URL);if(disposed)return;
      if(!Array.isArray(board?.events))throw new Error('Invalid ESPN scoreboard');
      events=board.events;dataFailed=false;paintGameOptions();paintHero();
      $('liveDataStatus').textContent=`Updated ${new Date().toLocaleTimeString([],{hour:'numeric',minute:'2-digit'})}`;
      const targets=events.filter(e=>monitored().includes(String(e.id)));
      await Promise.all(targets.map(async event=>{
        const id=String(event.id),state=gameState(event),last=competition(event)?.situation?.lastPlay;
        if(state==='pre'){await loadPlayers(event,null);return;}
        const old=summaries.get(id),lastKey=last?playIdentity(last):'';
        let summary=old?.data||null,complete=false;
        if(!old||old.key!==lastKey||Date.now()-old.at>30000){
          try{summary=await fetchJson(`${API}/summary?event=${encodeURIComponent(id)}`);complete=!!summary?.drives||Array.isArray(summary?.plays);summaries.set(id,{data:summary,key:lastKey,at:Date.now()});}
          catch{historyStatus.set(id,'Latest play only · retrying ESPN play history.');}
        }else complete=!!summary?.drives||Array.isArray(summary?.plays);
        const players=await loadPlayers(event,summary);
        if(disposed||!monitored().includes(id))return;
        const result=tracker.ingest(event,playsFromSummary(summary,last),{players,settings:settingsForGame(settings,id),complete});
        if(complete)historyStatus.set(id,'Latest 60 plays · updated from ESPN');
        if(state==='in'||liveGames.has(id))for(const entry of result.announcements)enqueuePlay(entry);
        if(state==='in')liveGames.add(id);
      }));
      tracker.retain(monitored());
      if(settings.enabled&&settings.scoreIntervalMinutes&&Date.now()>=nextScoreAt){enqueueScores();nextScoreAt=Date.now()+settings.scoreIntervalMinutes*60000;}
      paintHero();if($('playByPlayDialog').open)paintPlayers();
    }catch{dataFailed=true;$('liveDataStatus').textContent='Live data interrupted · retrying';status('ESPN is unavailable. Saved plays remain visible; live updates will retry.');paintHero();}
    finally{polling=false;}
  }
  const setEnabled=value=>{
    settings.enabled=!!value;save();
    if(settings.enabled){unlockBrowserSpeech();nextScoreAt=Date.now()+settings.scoreIntervalMinutes*60000;void poll();}
    else removeQueuedSpeech(item=>['play-by-play','score-update'].includes(item.source));
    paintControls();
  };
  const openSettings=(players=false)=>{
    paintVoiceList();paintGameOptions();paintPlayers();$('playByPlayDialog').showModal();
    if(players){$('playerSearch').focus();$('playerSelection').scrollIntoView({block:'start'});}
  };
  for(const button of document.querySelectorAll('[data-play-mode]'))button.onclick=()=>{settings.mode=button.dataset.playMode;setEnabled(true);if(settings.mode==='players')openSettings(true);};
  $('announcementsToggle').onclick=()=>setEnabled(!settings.enabled);$('playByPlayEnabled').onchange=()=>setEnabled($('playByPlayEnabled').checked);
  $('playByPlayButton').onclick=()=>openSettings();$('choosePlayers').onclick=()=>openSettings(true);$('closePlayByPlay').onclick=()=>$('playByPlayDialog').close();
  $('playByPlayDuckRadio').onchange=()=>{settings.duckRadio=$('playByPlayDuckRadio').checked;save();setRadioDucked(settings.duckRadio&&['play-by-play','score-update','game-update','voice-preview'].includes(getBrowserSpeechQueueState().current?.source));};
  $('includeRadioGame').onchange=()=>{settings.includeRadioGame=$('includeRadioGame').checked;save();};
  $('playByPlayScoreInterval').onchange=()=>{settings.scoreIntervalMinutes=normalizeScoreInterval($('playByPlayScoreInterval').value);save();nextScoreAt=Date.now()+settings.scoreIntervalMinutes*60000;paintControls();};
  $('playByPlayScoreScope').onchange=()=>{settings.scoreScope=$('playByPlayScoreScope').value;save();};
  $('voiceSelect').onchange=()=>{voiceSettings.voiceURI=$('voiceSelect').value;saveVoiceSettings(voiceSettings);};
  $('voiceStyle').onchange=()=>{voiceSettings.style=$('voiceStyle').value;saveVoiceSettings(voiceSettings);};
  $('previewVoice').onclick=()=>{unlockBrowserSpeech();enqueueBrowserSpeech('Your NFL radio companion is ready. Choose your games and follow the action.',{source:'voice-preview',key:'voice-preview',...playVoiceOptions({},voiceSettings.style),onStart:()=>{if(settings.duckRadio)setRadioDucked(true);},onEnd:()=>setRadioDucked(false),onError:()=>setRadioDucked(false)});};
  $('speakLatestPlays').onclick=()=>{unlockBrowserSpeech();const latest=tracker.history(viewedGame).at(-1);if(latest&&shouldAnnouncePlay(latest.raw,settingsForGame(settings,viewedGame),{event:currentEvent(),players:playersByGame.get(viewedGame)||[],formatted:latest}))enqueuePlay(latest,{manual:true});else status('No latest play matches your announcement filter.');};
  $('speakScoresNow').onclick=()=>{unlockBrowserSpeech();enqueueScores({manual:true});};
  $('transcriptGame').onchange=()=>{viewedGame=$('transcriptGame').value;paintHero();};
  $('playerGame').onchange=()=>paintPlayers();$('playerSearch').oninput=()=>paintPlayers();
  $('retryRoster').onclick=async()=>{
    const event=events.find(e=>String(e.id)===$('playerGame').value);if(!event)return;
    for(const c of competitors(event)){const entry=rosters.get(String(c.team.id));if(entry&&!entry.pending)entry.expires=0;}
    const work=loadPlayers(event,summaries.get(String(event.id))?.data);paintPlayers();await work;paintPlayers();
  };
  const updates=$('gameUpdatesButton');if(updates)$('extraVoiceActions').append(updates);
  onBrowserSpeechQueueState(state=>{
    $('speechIndicator').textContent=state.speaking?`Speaking${state.queued?` · ${state.queued} queued`:''}`:state.queued?`${state.queued} queued`:'Voice ready';
    $('speechIndicator').classList.toggle('speaking',state.speaking);
  });
  globalThis.speechSynthesis?.addEventListener?.('voiceschanged',paintVoiceList);
  window.addEventListener('nfl-radio:scoreboard',e=>{if(!events.length){events=e.detail.events||[];paintGameOptions();paintHero();}void poll();});
  window.addEventListener('nfl-radio:rotation',()=>{paintGameOptions();paintHero();paintControls();void poll();});
  window.addEventListener('nfl-radio:selection',e=>{viewedGame=String(e.detail.gameId);paintGameOptions();paintHero();});
  window.addEventListener('storage',e=>{if(e.key===STORAGE_KEY){settings=readPlayByPlaySettings();paintControls();}if(e.key==='nfl-dial:rotation'){paintGameOptions();paintHero();void poll();}});
  document.addEventListener('pointerdown',()=>{if(settings.enabled)unlockBrowserSpeech();},{passive:true});
  document.addEventListener('visibilitychange',()=>{if(!document.hidden)void poll();});
  const timer=setInterval(poll,PLAY_POLL_MS);
  window.addEventListener('pagehide',()=>{disposed=true;clearInterval(timer);setRadioDucked(false);});
  paintControls();paintVoiceList();paintGameOptions();paintHero();void poll();
}
if(typeof document!=='undefined')installLivePlayByPlay();
