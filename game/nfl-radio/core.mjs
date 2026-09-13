// Framework-free domain logic shared by the page and regression tests.
export const TEAMS = [
 ['ARI','Arizona','Cardinals'],['ATL','Atlanta','Falcons'],['BAL','Baltimore','Ravens'],['BUF','Buffalo','Bills'],
 ['CAR','Carolina','Panthers'],['CHI','Chicago','Bears'],['CIN','Cincinnati','Bengals'],['CLE','Cleveland','Browns'],
 ['DAL','Dallas','Cowboys'],['DEN','Denver','Broncos'],['DET','Detroit','Lions'],['GB','Green Bay','Packers'],
 ['HOU','Houston','Texans'],['IND','Indianapolis','Colts'],['JAX','Jacksonville','Jaguars'],['KC','Kansas City','Chiefs'],
 ['LAC','Los Angeles','Chargers'],['LAR','Los Angeles','Rams'],['LV','Las Vegas','Raiders'],['MIA','Miami','Dolphins'],
 ['MIN','Minnesota','Vikings'],['NE','New England','Patriots'],['NO','New Orleans','Saints'],['NYG','New York','Giants'],
 ['NYJ','New York','Jets'],['PHI','Philadelphia','Eagles'],['PIT','Pittsburgh','Steelers'],['SEA','Seattle','Seahawks'],
 ['SF','San Francisco','49ers'],['TB','Tampa Bay','Buccaneers'],['TEN','Tennessee','Titans'],['WAS','Washington','Commanders']
].map(([id,city,nickname])=>({id,city,nickname,name:`${city} ${nickname}`}));
export const team = id => TEAMS.find(t=>t.id===id);
const normalize=s=>String(s).toLowerCase().replace(/[^a-z0-9 ]/g,' ').replace(/\s+/g,' ').trim();
const extras={SF:['niners','forty niners','forty niners'],TB:['bucs','tampa'],JAX:['jags','jac'],NE:['pats','boston'],WAS:['wash'],LAR:['la rams'],LAC:['la chargers']};
export function resolveVoice(input,games,current){
 const text=normalize(input), padded=` ${text} `;
 if(/^(play |switch to |put on )?(home|away|national|spanish)( broadcast| station| feed)?$/.test(text)) return {kind:'feed',role:text.match(/home|away|national|spanish/)[0]};
 if(/^(next|previous)( game)?$/.test(text))return {kind:'cycle',direction:text.startsWith('next')?1:-1};
 if(text==='next station')return {kind:'nextStation'};
 if(/^(pause|stop)( audio| radio)?$/.test(text))return {kind:'pause'};
 if(/^(resume|play)$/.test(text))return {kind:'resume'};
 const mentioned=TEAMS.filter(t=>[t.id,t.name,t.nickname,t.city,...(extras[t.id]||[])].some(a=>padded.includes(` ${normalize(a)} `)));
 // A city shared by two teams must never silently pick one, even on a bye.
 for(const city of ['New York','Los Angeles']){
  const pair=TEAMS.filter(t=>t.city===city);
  if(padded.includes(` ${city.toLowerCase()} `)&&!pair.some(t=>padded.includes(` ${normalize(t.nickname)} `)||padded.includes(` ${t.id.toLowerCase()} `))){
   const others=mentioned.filter(t=>!pair.includes(t));
   const matching=games.filter(g=>others.some(t=>[g.home,g.away].includes(t.id))&&pair.some(t=>[g.home,g.away].includes(t.id)));
   if(matching.length!==1)return {kind:'ambiguous',message:`${pair.map(t=>t.nickname).join(' or ')}?`};
   return {kind:'game',game:matching[0],team:pair.find(t=>[matching[0].home,matching[0].away].includes(t.id)).id};
  }
 }
 // Nicknames/abbreviations override a shared city match.
 const explicit=mentioned.filter(t=>[t.id,t.nickname,...(extras[t.id]||[])].some(a=>padded.includes(` ${normalize(a)} `)));
 const wanted=explicit.length?mentioned.filter(t=>!['New York','Los Angeles'].includes(t.city)||explicit.includes(t)):mentioned;
 if(!wanted.length)return {kind:'unknown',message:'Say a team, matchup, or broadcast.'};
 if(/broadcast|station|feed/.test(text)&&wanted.length===1&&current&&[current.home,current.away].includes(wanted[0].id))return {kind:'feed',role:current.home===wanted[0].id?'home':'away'};
 const matches=games.filter(g=>wanted.every(t=>[g.home,g.away].includes(t.id)));
 if(!matches.length)return {kind:'unknown',message:`No current game for ${wanted.map(t=>t.name).join(' and ')}.`};
 const live=matches.filter(g=>g.state==='in');
 const candidates=live.length?live:matches.filter(g=>g.state!=='post');
 const chosen=(candidates.length?candidates:matches).sort((a,b)=>Date.parse(a.date)-Date.parse(b.date))[0];
 return {kind:'game',game:chosen,team:wanted[0].id,matchup:wanted.length>1};
}
export function parseSchedule(data){
 if(!data||!Array.isArray(data.events))throw new Error('Invalid schedule response');
 return data.events.flatMap(e=>{
  const c=e.competitions?.[0], a=c?.competitors?.find(t=>t.homeAway==='away'),h=c?.competitors?.find(t=>t.homeAway==='home');
  const canonical=id=>({WSH:'WAS',JAC:'JAX',LA:'LAR'}[id]||id);
  const home=canonical(h?.team?.abbreviation),away=canonical(a?.team?.abbreviation);
  if(!team(home)||!team(away)||!e.id||!Number.isFinite(Date.parse(e.date)))return [];
  return [{id:String(e.id),home,away,date:e.date,state:e.status?.type?.state||'pre',status:e.status?.type?.shortDetail||'Scheduled'}];
 }).sort((a,b)=>(a.state==='in'?-1:0)-(b.state==='in'?-1:0)||Date.parse(a.date)-Date.parse(b.date));
}
export function playable(feed,game,now=Date.now()){
 return !!(feed && game && feed.access==='direct' && feed.gameIds?.includes(game.id) &&
 feed.authorized===true && feed.gameAudio===true && /^https:\/\//.test(feed.streamUrl||'') &&
 /^https:\/\//.test(feed.sourceUrl||'') && Number.isFinite(Date.parse(feed.verifiedAt)) &&
 Date.parse(feed.availableFrom)<=now && Date.parse(feed.availableUntil)>now);
}
export function feedQueue(feeds,game,teamId,preferred,role,now=Date.now()){
 const allowed=feeds.filter(f=>playable(f,game,now));
 const target=role==='home'?game.home:role==='away'?game.away:teamId;
 const candidates=allowed.filter(f=>role==='spanish'?f.language==='es':role==='national'?f.kind==='national':f.team===target||f.kind==='national');
 const rank=f=>f.id===preferred?0:f.kind==='flagship'?1:f.kind==='affiliate'?2:f.kind==='national'?3:4;
 return candidates.sort((a,b)=>rank(a)-rank(b));
}
export function cycle(rotation,current,direction){
 if(!rotation.length)return null;
 const index=rotation.indexOf(current);
 return rotation[(index<0?(direction>0?0:rotation.length-1):(index+direction+rotation.length)%rotation.length)];
}
export class RadioPlayer{
 constructor(createAudio,onState,{timeout=12000}={}){this.createAudio=createAudio;this.onState=onState;this.timeout=timeout;this.serial=0;this.audio=null;this.queue=[];this.index=0;this.timer=null;}
 stop(){this.serial++;clearTimeout(this.timer);if(this.audio){this.audio.pause();this.audio.removeAttribute('src');this.audio.load();this.audio=null;}}
 select(queue){this.stop();this.queue=queue;this.index=0;if(!queue.length){this.onState('unavailable',null);return;}this.attempt(this.serial);}
 attempt(serial){
  if(serial!==this.serial)return;
  const feed=this.queue[this.index];if(!feed){this.onState('unavailable',null);return;}
  const audio=this.createAudio();this.audio=audio;audio.preload='none';audio.src=feed.streamUrl;
  let failed=false;
  const valid=()=>serial===this.serial&&this.audio===audio;
  const fail=()=>{if(!valid()||failed)return;failed=true;clearTimeout(this.timer);audio.pause();audio.removeAttribute('src');audio.load();this.index++;this.onState('fallback',feed);this.attempt(serial);};
  const arm=()=>{clearTimeout(this.timer);this.timer=setTimeout(fail,this.timeout);};
  audio.addEventListener('error',fail);
  audio.addEventListener('ended',fail);
  audio.addEventListener('waiting',()=>{if(valid())arm();});
  audio.addEventListener('stalled',()=>{if(valid())arm();});
  audio.addEventListener('playing',()=>{if(valid()){clearTimeout(this.timer);this.onState('playing',feed);}});
  audio.addEventListener('pause',()=>{if(valid()&&!failed){clearTimeout(this.timer);this.onState('paused',feed);}});
  this.onState('loading',feed);arm();
  // Called synchronously from a tap; never await schedule/network before play().
  const result=audio.play();
  result?.catch(error=>{if(!valid())return;if(error.name==='NotAllowedError'){clearTimeout(this.timer);this.onState('blocked',feed);}else if(error.name!=='AbortError')fail();});
 }
 pause(){this.serial++;clearTimeout(this.timer);if(this.audio){this.audio.pause();this.onState('paused',this.queue[this.index]);}}
 resume(){if(this.audio)this.select(this.queue.slice(this.index));}
}
