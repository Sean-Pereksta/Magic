import {storagePrefix} from './league.mjs';
import {fantasyBreakdown,stat,statLeaders} from './player-stats.mjs';
const node=(tag,text,cls)=>{const n=document.createElement(tag);if(text!=null)n.textContent=text;if(cls)n.className=cls;return n;};
export function installStatsView(){
 const root=document.getElementById('gamePlayerStats');
 const heading=node('summary','Players · game leaders & fantasy');root.append(heading);
 const controls=node('div',null,'statsControls'),mode=document.createElement('select'),position=document.createElement('select');
 let saved='ppr';try{saved=localStorage.getItem(`${storagePrefix}:fantasy`)==='standard'?'standard':'ppr';}catch{}
 for(const [value,text] of [['ppr','PPR · 1 point per catch'],['standard','Standard · no reception points']]){const o=node('option',text);o.value=value;mode.append(o);}mode.value=saved;mode.setAttribute('aria-label','Fantasy scoring');
 for(const value of ['ALL','QB','RB','WR','TE','FB']){const o=node('option',value==='ALL'?'All skill players':value);o.value=value;position.append(o);}position.setAttribute('aria-label','Player position');
 controls.append(mode,position);root.append(controls);
 const status=node('p',null,'settingHint'),leaders=node('div',null,'statsLeaders'),players=node('div',null,'statsPlayers');root.append(status,leaders,players);
 root.append(node('p','Fantasy estimates: 1/25 pass yd, 4/pass TD, −2/INT, 1/10 rush or receiving yd, 6/rush, receiving or return TD, −2/fumble lost; PPR adds 1/catch. Excludes two-point conversions, yardage bonuses, return yards, kicking and defense. Missing categories count as zero in estimates; — means no reported stat.','settingHint'));
 let state={rows:[],label:'',status:''},key='';
 const paint=()=>{
  const next=JSON.stringify([state,mode.value,position.value]);if(next===key)return;key=next;
  heading.textContent=`Players · ${state.label||'game leaders & fantasy'}`;status.textContent=state.status;
  const opened=new Set([...players.querySelectorAll('details[open]')].map(n=>n.dataset.playerId));
  leaders.replaceChildren();players.replaceChildren();
  for(const [category,title] of [['passing','Top passer'],['rushing','Top rusher'],['receiving','Top receiver']]){
   const box=node('div',null,'statLeader');box.append(node('strong',title));
   const best=statLeaders(state.rows,category);
   box.append(node('p',best.length?best.map(p=>`${p.name} (${p.team}) · ${stat(p,category,'YDS')} yd${category==='receiving'?` · ${stat(p,category,'REC')??'—'} rec`:''} · ${stat(p,category,'TD')??'—'} TD`).join(' / '):'Not reported yet'));leaders.append(box);
  }
  const rows=state.rows.filter(p=>position.value==='ALL'||p.position===position.value).sort((a,b)=>Number(b.hasStats)-Number(a.hasStats)||fantasyBreakdown(b,mode.value).total-fantasyBreakdown(a,mode.value).total||a.name.localeCompare(b.name));
  for(const p of rows){
   const box=node('details',null,'statPlayer');box.dataset.playerId=p.id;box.open=opened.has(p.id);const summary=node('summary');
   const score=fantasyBreakdown(p,mode.value);summary.append(node('strong',`${p.name} · ${p.team} ${p.position}`),node('span',p.hasStats?`${score.total.toFixed(2)} ${mode.value.toUpperCase()} pts*`:'No reported stats','fantasyTotal'));box.append(summary);
   const lines=[];
   for(const [c,title] of [['passing','Pass'],['rushing','Rush'],['receiving','Rec']])if(p.categories[c])lines.push(`${title}: ${stat(p,c,'YDS')??'—'} yd · ${stat(p,c,'TD')??'—'} TD${c==='receiving'?` · ${stat(p,c,'REC')??'—'} catches / ${stat(p,c,'TGTS')??'—'} targets`:c==='rushing'?` · ${stat(p,c,'CAR')??'—'} carries`:''}`);
   // Summary remains visible when the full stat breakdown is collapsed.
   summary.append(node('small',lines.join(' | ')||'Roster player · no box-score entry yet','statLine'));
   for(const [category,values] of Object.entries(p.categories)){
    const section=node('section');section.append(node('h4',category.replace(/([A-Z])/g,' $1')));
    const dl=node('dl',null,'statValues');for(const [label,value] of Object.entries(values)){dl.append(node('dt',label),node('dd',value));}section.append(dl);box.append(section);
   }
   if(p.hasStats){box.append(node('h4','Fantasy points breakdown (estimate)'));const dl=node('dl',null,'statValues');for(const [label,value] of score.parts)dl.append(node('dt',label),node('dd',value.toFixed(2)));box.append(dl);}
   players.append(box);
  }
  if(!rows.length)players.append(node('p','Player stats are not available for this selection yet.','emptyState'));
 };
 mode.onchange=()=>{try{localStorage.setItem(`${storagePrefix}:fantasy`,mode.value);}catch{}paint();};position.onchange=paint;
 return {render(rows,label,message){state={rows,label,status:message};paint();},open(){root.open=true;root.scrollIntoView({block:'start',behavior:'smooth'});}};
}
