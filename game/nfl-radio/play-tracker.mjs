import {formatPlay,shouldAnnouncePlay} from './play-formatter.mjs';
const list=value=>Array.isArray(value)?value:[];
const clean=value=>String(value??'').replace(/\s+/g,' ').trim();
export const TRANSCRIPT_LIMIT=60;
export function playIdentity(play){
  if(play?.id!=null&&String(play.id))return `id:${play.id}`;
  if(play?.sequenceNumber!=null)return `sequence:${play.sequenceNumber}`;
  return `fallback:${play?.period?.number||play?.period||''}:${play?.clock?.displayValue||''}:${clean(play?.text||play?.shortText||play?.type?.text)}`;
}
export function playsFromSummary(summary,lastPlay){
  const byKey=new Map();
  const add=(play,driveTeam)=>{
    if(!play||!(play.text||play.shortText))return;
    const id=playIdentity(play),old=byKey.get(id);
    byKey.set(id,{...old,...play,driveTeam:driveTeam||old?.driveTeam,start:{...old?.start,...play.start},end:{...old?.end,...play.end}});
  };
  for(const drive of [...list(summary?.drives?.previous),summary?.drives?.current].filter(Boolean))for(const play of list(drive.plays))add(play,drive.team);
  for(const play of list(summary?.plays))add(play);
  // Scoring summaries can abbreviate the same full drive play: keep the detailed version.
  for(const play of list(summary?.scoringPlays))if(!byKey.has(playIdentity(play)))add(play);
  add(lastPlay);
  const clock=p=>{const [m,s]=String(p?.clock?.displayValue||'0:00').split(':').map(Number);return m*60+s;};
  return [...byKey.values()].sort((a,b)=>{
    if(a.sequenceNumber!=null&&b.sequenceNumber!=null)return Number(a.sequenceNumber)-Number(b.sequenceNumber);
    if(a.period?.number&&b.period?.number)return a.period.number-b.period.number||clock(b)-clock(a);
    return 0;
  });
}
export function createPlayTracker({limit=TRANSCRIPT_LIMIT}={}){
  const games=new Map();
  function ingest(event,plays,{players=[],settings={},prime=false,complete=false}={}){
    const id=String(event?.id||'');
    let state=games.get(id);
    const initial=!state;
    if(!state){state={seen:new Set(),fingerprints:new Map(),history:[],complete:false,lastKey:null};games.set(id,state);}
    const keys=plays.map(playIdentity);
    const anchor=state.lastKey?keys.indexOf(state.lastKey):-1;
    const firstComplete=complete&&!state.complete;
    const announcements=[];
    const rosterVersion=players.map(p=>[p.id,p.name,p.teamId,p.aliases]).flat().join('|');
    for(let index=0;index<plays.length;index++){
      const raw=plays[index],key=keys[index],fresh=!state.seen.has(key);
      const fingerprint=JSON.stringify([raw.text,raw.shortText,raw.type,raw.start,raw.end,raw.team,raw.scoreValue,raw.scoringType,raw.isTurnover,raw.isPenalty,raw.participants,raw.athletesInvolved,raw.period,raw.clock,rosterVersion]);
      if(!fresh&&state.fingerprints.get(key)===fingerprint)continue;
      state.fingerprints.set(key,fingerprint);
      if((initial||prime)&&index<plays.length-limit){state.seen.add(key);continue;}
      if(!fresh&&!state.history.some(p=>p.key===key))continue;
      const formatted=formatPlay(raw,{event,players});
      if(!formatted.text)continue;
      const entry={...formatted,key,gameId:id,raw,play:formatted.text,speech:formatted.text,period:raw.period?.number||null,clock:raw.clock?.displayValue||'',matchup:(event?.competitions?.[0]?.competitors||[]).slice().sort((a,b)=>(a.homeAway==='away'?-1:1)-(b.homeAway==='away'?-1:1)).map(c=>c.team?.abbreviation)};
      const existing=state.history.findIndex(p=>p.key===key);
      if(existing>=0)state.history[existing]=entry; // ESPN corrections update the transcript silently.
      else if(fresh)state.history.push(entry);
      state.seen.add(key);
      const historical=initial||prime||(firstComplete&&(anchor<0?index<plays.length-1:index<=anchor));
      if(fresh&&!historical&&shouldAnnouncePlay(raw,settings,{event,players,formatted}))announcements.push(entry);
    }
    // Recovered full history may have been inserted after the scoreboard's latest play.
    const order=new Map(keys.map((key,index)=>[key,index]));
    state.history.sort((a,b)=>(order.get(a.key)??-1)-(order.get(b.key)??-1));
    state.history=state.history.slice(-limit);
    if(keys.length&&(initial||!state.lastKey||keys.includes(state.lastKey)||announcements.length))state.lastKey=keys.at(-1);
    if(complete)state.complete=true;
    return {announcements,history:state.history};
  }
  return {ingest,history:id=>games.get(String(id))?.history||[],retain:ids=>{const keep=new Set(ids.map(String));for(const id of games.keys())if(!keep.has(id))games.delete(id);}};
}
