// Box-score totals are authoritative: never add individual play yardage to them.
const list=v=>Array.isArray(v)?v:[];
const number=v=>v!==null&&v!==undefined&&String(v).trim()!==''&&/^[-+]?\d+(?:\.\d+)?$/.test(String(v).replaceAll(',',''))?Number(String(v).replaceAll(',','')):null;
const skill=p=>/^(QB|RB|FB|HB|WR|TE)$/i.test(p||'');
export function gamePlayerStats(summary,roster=[]){
 const rows=new Map();
 for(const p of roster)if(p.id&&skill(p.position))rows.set(String(p.id),{...p,categories:{},hasStats:false});
 for(const team of list(summary?.boxscore?.players))for(const group of list(team.statistics)){
  if(!['passing','rushing','receiving','fumbles','kickReturns','puntReturns'].includes(group.name))continue;
  for(const entry of list(group.athletes)){
   const a=entry.athlete||{},id=String(a.id||'');if(!id)continue;
   const p=rows.get(id)||{id,name:a.displayName||a.fullName||'Player',position:a.position?.abbreviation||'',team:team.team?.abbreviation||'',teamId:String(team.team?.id||''),categories:{},hasStats:false};
   // Labels align with values even when ESPN's keys contain an omitted QBR column.
   const labels=list(group.labels).length===list(entry.stats).length?group.labels:group.keys;
   p.categories[group.name]=Object.fromEntries(list(labels).map((label,i)=>[label,String(entry.stats?.[i]??'—')]));
   p.hasStats=true;rows.set(id,p);
  }
 }
 return [...rows.values()].filter(p=>skill(p.position)||['passing','rushing','receiving'].some(k=>p.categories[k]));
}
const aliases={YDS:['passingYards','rushingYards','receivingYards'],TD:['passingTouchdowns','rushingTouchdowns','receivingTouchdowns','kickReturnTouchdowns','puntReturnTouchdowns'],REC:['receptions'],CAR:['rushingAttempts'],TGTS:['receivingTargets'],INT:['interceptions'],LOST:['fumblesLost']};
export function stat(p,category,label){const c=p.categories[category]||{};return number(c[label]??(aliases[label]||[]).map(k=>c[k]).find(v=>v!==undefined));}
export function fantasyBreakdown(p,mode='ppr'){
 const n=(c,k)=>stat(p,c,k)??0;
 const parts=[['Passing yards',n('passing','YDS')*.04],['Passing TDs',n('passing','TD')*4],['Interceptions',n('passing','INT')*-2],['Rushing yards',n('rushing','YDS')*.1],['Rushing TDs',n('rushing','TD')*6],['Receiving yards',n('receiving','YDS')*.1],['Receiving TDs',n('receiving','TD')*6],['Receptions',mode==='ppr'?n('receiving','REC'):0],['Fumbles lost',n('fumbles','LOST')*-2],['Return TDs',(n('kickReturns','TD')+n('puntReturns','TD'))*6]];
 return {parts,total:Math.round(parts.reduce((sum,[,v])=>sum+v,0)*100)/100};
}
export function statLeaders(players,category){
 const eligible=players.filter(p=>stat(p,category,'YDS')!==null);if(!eligible.length)return [];
 const best=Math.max(...eligible.map(p=>stat(p,category,'YDS')));return eligible.filter(p=>stat(p,category,'YDS')===best);
}
export function playStatFooter(entry,players){
 if(!entry)return '';
 const ids=new Set(entry.playerIds||[]),matched=players.filter(p=>ids.has(p.id));
 const raw=entry.raw?.text||entry.text||'';
 const passing=/pass|reception/i.test(raw+' '+entry.raw?.type?.text)||entry.kind==='pass';
 const running=!passing&&(/rush|run|scrambl|left tackle|right tackle|up the middle|left guard|right guard|left end|right end/i.test(raw)||entry.kind==='run');
 const parts=[];
 for(const p of matched){
  // Only annotate actors named before the tackle/defender suffix, not every participant.
  const body=String(entry.text||'').split(/\(|Defended by|tackled by/i)[0];
  if(!body.includes(p.name))continue;
  if(passing&&p.categories.receiving&&/finds| to |pass from/i.test(body)){
   const receiver=body.includes(`finds ${p.name}`)||body.includes(`to ${p.name}`)||body.includes(`${p.name} `)&&body.includes('pass from');
   if(receiver)parts.push(`${p.name}: ${stat(p,'receiving','REC')??'—'} rec · ${stat(p,'receiving','YDS')??'—'} receiving yd`);
  }
  if(running&&p.categories.rushing&&body.indexOf(p.name)<body.search(/runs|rush|scrambl|left|right|up the middle/i))parts.push(`${p.name}: ${stat(p,'rushing','YDS')??'—'} rushing yd`);
  if(passing&&p.categories.passing&&(body.includes(`${p.name} finds`)||body.includes(`${p.name}’s`)||body.includes(`${p.name} pass`)))parts.push(`${p.name}: ${stat(p,'passing','YDS')??'—'} passing yd`);
 }
 return parts.length?`${parts.join(' · ')} · Latest game totals`:'';
}
