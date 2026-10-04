// Deterministic calls: only rephrase facts present in ESPN's play and athlete data.
const clean=value=>String(value??'').replace(/\s+/g,' ').trim();
const key=value=>clean(value).normalize('NFKD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/[^a-z0-9]/g,'');
const escape=value=>value.replace(/[.*+?^${}()|[\]\\]/g,'\\$&');
const list=value=>Array.isArray(value)?value:[];
export const ANNOUNCEMENT_MODES=['every','touchdowns','players'];
export const normalizeAnnouncementMode=value=>ANNOUNCEMENT_MODES.includes(value)?value:'every';

export function normalizePlayer(value,team={}){
  const athlete=value?.athlete||value||{};
  const name=clean(athlete.displayName||athlete.fullName||[athlete.firstName,athlete.lastName].filter(Boolean).join(' '));
  if(!name)return null;
  const first=athlete.firstName||name.split(' ')[0];
  const last=athlete.lastName||name.slice(first.length).trim();
  const playerTeam=athlete.team||team;
  const baseLast=last.replace(/\s+(?:Jr\.?|Sr\.?|II|III|IV|V)$/i,'');
  return {
    id:String(athlete.id||''),name,
    teamId:String(playerTeam.id||team.id||''),team:playerTeam.abbreviation||team.abbreviation||'',
    position:clean(athlete.position?.abbreviation||athlete.position?.displayName||(typeof athlete.position==='string'?athlete.position:'')),
    aliases:[...new Set([name,athlete.fullName,athlete.shortName,last&&`${first[0]}. ${last}`,baseLast&&`${first[0]}. ${baseLast}`].map(clean).filter(Boolean))]
  };
}
export function mergePlayers(...groups){
  const players=new Map();
  for(const group of groups)for(const raw of list(group)){
    const player=raw?.name&&Array.isArray(raw.aliases)?raw:normalizePlayer(raw);
    if(!player)continue;
    const id=player.id||key(player.name),previous=players.get(id);
    players.set(id,{...previous,...player,teamId:player.teamId||previous?.teamId||'',team:player.team||previous?.team||'',position:player.position||previous?.position||'',aliases:[...new Set([...(previous?.aliases||[]),...player.aliases])]});
  }
  return [...players.values()];
}
export function rosterPlayers(data,team=data?.team||{}){
  return mergePlayers(list(data?.athletes).flatMap(group=>list(group.items).length?group.items:[group]).map(p=>normalizePlayer(p,team)).filter(Boolean));
}
export function summaryPlayers(summary,event){
  const groups=[];
  for(const entry of list(summary?.boxscore?.players))for(const stat of list(entry.statistics))groups.push(list(stat.athletes).map(p=>normalizePlayer(p,entry.team)).filter(Boolean));
  for(const entry of list(summary?.rosters))groups.push(list(entry.roster).map(p=>normalizePlayer(p,entry.team)).filter(Boolean));
  const c=event?.competitions?.[0];
  for(const group of list(c?.leaders))groups.push(list(group.leaders).map(p=>normalizePlayer(p,p.team)).filter(Boolean));
  groups.push(involvedAthletes(c?.situation?.lastPlay).map(p=>normalizePlayer(p)).filter(Boolean));
  return mergePlayers(...groups);
}
function involvedAthletes(play){
  return [...list(play?.participants),...list(play?.athletesInvolved),...list(play?.athletes)].map(p=>p?.athlete||p).filter(Boolean);
}
function aliasPattern(alias){
  return alias.split(/[.\s’'\-]+/).filter(Boolean).map(escape).join('[.\\s’\'\\-]*');
}
function nameMatches(text,alias){
  if(key(alias).length<3)return false;
  return new RegExp(`(?:^|[^\\p{L}\\p{N}])${aliasPattern(alias)}(?=$|[^\\p{L}\\p{N}])`,'iu').test(text);
}
function playerContext(play,players){
  const involved=involvedAthletes(play);
  const all=mergePlayers(players,involved.map(p=>normalizePlayer(p)).filter(Boolean));
  const ids=new Set(involved.map(p=>String(p.id||'')).filter(Boolean));
  const owners=new Map();
  for(const p of all)for(const alias of p.aliases){const k=key(alias);const set=owners.get(k)||new Set();set.add(p.id||key(p.name));owners.set(k,set);}
  const usable=(p,alias)=>owners.get(key(alias))?.size===1||
    (ids.has(p.id)&&all.filter(other=>ids.has(other.id)&&other.aliases.some(a=>key(a)===key(alias))).length===1);
  return {all,ids,usable};
}
export function playInvolvesSelectedPlayer(play,selected=[],players=[]){
  const {all,ids,usable}=playerContext(play,players);
  const wanted=new Set(list(selected).map(p=>String(typeof p==='object'?p.id:p)));
  if([...ids].some(id=>wanted.has(id)))return true;
  const text=clean(play?.text||play?.shortText);
  return all.some(p=>wanted.has(p.id)&&p.aliases.some(alias=>usable(p,alias)&&nameMatches(text,alias)));
}
export function expandPlayerNames(text,play,players=[]){
  const {all,usable}=playerContext(play,players);
  const aliases=all.flatMap(p=>p.aliases.filter(a=>usable(p,a)).map(alias=>({alias,name:p.name}))).sort((a,b)=>b.alias.length-a.alias.length);
  // Placeholders prevent a later alias from replacing part of an expanded name.
  const replacements=[];
  for(const {alias,name} of aliases){
    const pattern=new RegExp(`(^|[^\\p{L}\\p{N}])${aliasPattern(alias)}(?=$|[^\\p{L}\\p{N}])`,'giu');
    text=text.replace(pattern,(_,prefix)=>{const index=replacements.push(name)-1;return `${prefix}\uE000${index}\uE001`;});
  }
  return text.replace(/\uE000(\d+)\uE001/g,(_,index)=>replacements[Number(index)]);
}
function tidy(text,event){
  text=clean(text).replace(/\b(?:Yds?|yards)\b/gi,'yards').replace(/\bfor 1 yards\b/gi,'for 1 yard')
    .replace(/\b(?:ob|oob)\b/gi,'out of bounds').replace(/\bFUMBLES\b/g,'fumbles').replace(/\bRECOVERED\b/g,'recovered')
    .replace(/\bINTERCEPTED\b/g,'intercepted').replace(/\bTOUCHDOWN\b/g,'touchdown').replace(/\bPENALTY\b/g,'Penalty')
    .replace(/\bNO GOOD\b/g,'no good').replace(/\bGOOD\b/g,'good').replace(/\bNULLIFIED\b/g,'nullified').replace(/\bNo Huddle\b/gi,'No huddle').replace(/\bpass incomplete\b/gi,'pass is incomplete')
    .replace(/\b1ST DOWN\b/gi,'first down').replace(/\b2ND\b/g,'2nd').replace(/\b3RD\b/g,'3rd').replace(/\b4TH\b/g,'4th')
    .replace(/\b([A-Z]{1,2})\.(?=[A-Za-z])/g,'$1. ').replace(/\s*&\s*/g,' and ');
  for(const c of list(event?.competitions?.[0]?.competitors)){
    const team=c.team||{};
    if(team.abbreviation)text=text.replace(new RegExp(`\\b${escape(team.abbreviation)}\\b`,'g'),team.shortDisplayName||team.displayName||team.abbreviation);
  }
  return clean(text);
}
function explicitTeam(play,event,touchdown,turnover){
  const id=play?.team?.id||play?.teamId||(touchdown&&turnover?play?.end?.team?.id:null)||play?.start?.team?.id||play?.driveTeam?.id;
  return list(event?.competitions?.[0]?.competitors).find(c=>String(c.team?.id||c.id)===String(id))?.team||null;
}
function conversionNote(text){
  const kick=text.match(/^(.+?) Kick$/i);
  return kick?`Extra point by ${kick[1]}.`:`Conversion: ${text}.`;
}
function yardPhrase(yards){return yards<0?`for a loss of ${Math.abs(yards)} ${Math.abs(yards)===1?'yard':'yards'}`:yards===0?'for no gain':`for ${yards} ${yards===1?'yard':'yards'}`;}
export function formatPlay(play,{event=null,players=[]}={}){
  const raw=clean(play?.text||play?.shortText||play?.type?.text);
  if(!raw)return {text:'',kind:'play',touchdown:false,turnover:false,important:false,label:'PLAY',playerIds:[]};
  const expanded=expandPlayerNames(raw,play,players);
  const body=expanded.replace(/^(?:\([^)]*(?:Shotgun|No Huddle|\d+:\d+)[^)]*\)\s*)+/gi,'');
  const type=clean(play?.type?.text);
  const negated=/\b(no play|no touchdown|nullified|overturned|reversed)\b/i.test(raw);
  const interception=!negated&&(/intercept/i.test(type)||/\bintercepted\b/i.test(raw));
  const fumble=/\bfumble[sd]?\b/i.test(raw+' '+type);
  const startTeam=play?.start?.team?.id,endTeam=play?.end?.team?.id;
  const turnover=!negated&&(play?.isTurnover===true||interception||(fumble&&startTeam&&endTeam&&String(startTeam)!==String(endTeam)));
  const touchdown=!negated&&(/\btouchdown\b/i.test(raw+' '+type)||Number(play?.scoreValue)===6||/^(TD|touchdown)$/i.test(play?.scoringType?.abbreviation||play?.scoringType?.name||''));
  const sack=/\bsack(?:ed)?\b/i.test(raw+' '+type);
  const fieldGoal=/field goal/i.test(raw+' '+type);
  const penalty=/\bpenalty\b/i.test(raw+' '+type)||play?.isPenalty===true;
  const firstDown=!negated&&!turnover&&!touchdown&&(/\bfirst down\b/i.test(raw)||
    (!penalty&&Number(play?.start?.down)>=1&&Number(play?.end?.down)===1&&startTeam&&String(startTeam)===String(endTeam)&&/pass|rush|run/i.test(type)));
  const yardMatch=raw.match(/\bfor (-?\d+) yards?\b/i);
  const yards=yardMatch?Number(yardMatch[1]):(/\bfor no gain\b/i.test(raw)?0:null);
  const playTeam=explicitTeam(play,event,touchdown,turnover);
  const teamName=playTeam?.location||playTeam?.shortDisplayName||playTeam?.displayName||'';
  const teamAbbr=playTeam?.abbreviation||'';
  const complex=penalty||negated||fumble||/\byards?\.\s*\S/i.test(raw)||/\b(lateral|backward|aborted|challenge|review|replay|safety|two.point|2.point|kick|conversion)\b/i.test(raw);
  let text=tidy(body,event),kind=penalty?'penalty':'play';
  let match;
  if(!negated&&!penalty&&touchdown&&(match=body.match(/^(.+?) (\d+) (?:Yd|yard) pass from (.+?)(?: \(([^()]*)\))?$/i))){
    text=`${match[3]} finds ${match[1]} for ${match[2]} yards — touchdown${teamName?` ${teamName}`:''}!${match[4]?` ${conversionNote(match[4])}`:''}`;kind='touchdown';
  }else if(!negated&&!penalty&&touchdown&&(match=body.match(/^(.+?) (\d+) (?:Yd|yard) (?:rush|run)(?: \(([^()]*)\))?$/i))){
    text=`${match[1]} runs for ${match[2]} yards — touchdown${teamName?` ${teamName}`:''}!${match[3]?` ${conversionNote(match[3])}`:''}`;kind='touchdown';
  }else if(!complex&&!interception&&(match=body.match(/^(.+?) pass incomplete(?: (short|deep))?(?: (left|right|middle))?(?: to (.+?))?(?: \(([^()]*)\)| \[([^\]]*)\])?\.?$/i))){
    const direction=match[3]?({middle:' over the middle',left:' to the left',right:' to the right'}[match[3].toLowerCase()]):'';
    text=`${match[1]}’s${match[2]?` ${match[2].toLowerCase()}`:''} pass${match[4]?` intended for ${match[4]}`:''}${direction} is incomplete${match[5]||match[6]?`. Defended by ${match[5]||match[6]}`:''}`;kind='pass';
  }else if(!complex&&!interception&&!fieldGoal&&!sack&&(match=body.match(/^(.+?) pass (?:complete )?(?:(?:short|deep) )?(?:(left|right|middle) )?to (.+?)(?= to [A-Z]{2,3} \d| for | pushed | ran | out of bounds)/i))&&yards!==null){
    const direction=match[2]?({middle:' over the middle',left:' to the left',right:' to the right'}[match[2].toLowerCase()]):'';
    text=`${match[1]} finds ${match[3]}${direction} ${yardPhrase(yards)}`;kind='pass';
  }else if(!complex&&!interception&&!fieldGoal&&!sack&&(match=body.match(/^(.+?) (left (?:end|tackle|guard)|right (?:end|tackle|guard)|up the middle|scrambles|rushes|runs)\b/i))&&yards!==null){
    const direction=/^left/i.test(match[2])?' left':/^right/i.test(match[2])?' right':/middle/i.test(match[2])?' up the middle':'';
    text=`${match[1]} ${/scrambles/i.test(match[2])?'scrambles':'runs'}${direction} ${yardPhrase(yards)}`;kind='run';
  }else if(!complex&&sack&&(match=body.match(/^(.+?) sacked\b/i))&&yards!==null){
    const defender=body.match(/\(([^()]*)\)\.?$/)?.[1];
    text=`${match[1]} is sacked${defender?` by ${defender.replace(/;\s*/g,' and ')}`:''} ${yardPhrase(yards)}`;kind='sack';
  }else if(!complex&&fieldGoal&&(match=body.match(/^(.+?) (\d+) (?:yards?|yd) field goal (is )?(GOOD|NO GOOD|BLOCKED|no good|good|blocked)/i))){
    const outcome=match[4].toLowerCase();
    // Keep misses/blocks and their return details verbatim after cleaning.
    if(outcome==='good')text=`${match[1]} makes a ${match[2]}-yard field goal`;
    kind='field-goal';
  }else if(interception){
    text=`Interception! ${tidy(body.replace(/^(.+?) pass(?: intended for (.+?))? intercepted by/i,(_,passer,receiver)=>`${passer}’s pass${receiver?` intended for ${receiver}`:''} is intercepted by`),event)}`;kind='interception';
  }else if(fumble){kind='fumble';}
  else if(fieldGoal){kind='field-goal';}
  else if(sack){kind='sack';}
  else if(/\bpunt\w*\b/i.test(raw+' '+type)){kind='punt';}
  else if(/pass/i.test(type+' '+raw)){kind='pass';}
  else if(/rush|run/i.test(type)){kind='run';}
  text=tidy(text,event).replace(/[.\s]+$/,'');
  if(touchdown){
    // The complex fallback retains all return/recovery/penalty details.
    if(!/touchdown/i.test(text))text+=` — touchdown${teamName?` ${teamName}`:''}!`;
    else if(!/[.!?]$/.test(text))text+='!';
    kind='touchdown';
  }else{
    if(turnover&&!interception&&!fumble&&!/turnover|on downs/i.test(text))text+=/downs/i.test(type)?'. Turnover on downs':'. Turnover';
    if(firstDown&&!/first down/i.test(text))text+=' and a first down';
    if(!/[.!?]$/.test(text))text+='.';
  }
  const down=clean(play?.start?.shortDownDistanceText||play?.start?.downDistanceText).split(/ at /i)[0]||
    (play?.start?.down?`${['','1st','2nd','3rd','4th'][play.start.down]||play.start.down} & ${play.start.distance??'?'}`:'');
  const heading=touchdown?'TOUCHDOWN':turnover?'TURNOVER':sack?'SACK':penalty?'PENALTY':fieldGoal?'FIELD GOAL':down||kind.toUpperCase();
  const {all,ids,usable}=playerContext(play,players);
  const playerIds=all.filter(p=>ids.has(p.id)||p.aliases.some(a=>usable(p,a)&&nameMatches(raw,a))).map(p=>p.id).filter(Boolean);
  return {text,kind,touchdown,turnover:!!turnover,important:!!(touchdown||turnover||sack||(yards!==null&&yards>=20)),yards,firstDown:!!firstDown,team:teamName,teamAbbr,label:[heading,teamAbbr].filter(Boolean).join(' — '),playerIds};
}
export function shouldAnnouncePlay(play,{mode='every',selectedPlayerIds=[]}={},context={}){
  const formatted=context.formatted||formatPlay(play,context);
  if(!formatted.text)return false;
  switch(normalizeAnnouncementMode(mode)){
    case 'touchdowns':return formatted.touchdown;
    case 'players':return playInvolvesSelectedPlayer(play,selectedPlayerIds,context.players);
    default:return true;
  }
}
