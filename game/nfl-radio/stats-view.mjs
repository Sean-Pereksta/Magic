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
 const search=node('input');search.type='search';search.placeholder='Find player or team';search.setAttribute('aria-label','Find a player or team in stats');
 const scope=node('select');scope.setAttribute('aria-label','Players to display');
 for(const [value,text] of [['all','All players'],['selected','My selected players']]){const o=node('option',text);o.value=value;scope.append(o);}
 const selected=new Set();let enabled=['passing','rushing','receiving'];
 try{const saved=JSON.parse(localStorage.getItem(`${storagePrefix}:stats-display`)||'null');if(saved){for(const id of saved.players||[])selected.add(String(id));if(Array.isArray(saved.leaders))enabled=saved.leaders;scope.value=saved.scope==='selected'?'selected':'all';}}catch{}
 const save=()=>{try{localStorage.setItem(`${storagePrefix}:stats-display`,JSON.stringify({players:[...selected],leaders:enabled,scope:scope.value}));}catch{}};
 controls.append(search,position,scope,mode);root.append(controls);
 const choices=node('fieldset',null,'leaderChoices');choices.append(node('legend','Show game leaders'));
 for(const [category,title] of [['passing','Top passer'],['rushing','Top rusher'],['receiving','Top receiver']]){const label=node('label'),input=node('input');input.type='checkbox';input.checked=enabled.includes(category);input.onchange=()=>{enabled=input.checked?[...enabled,category]:enabled.filter(c=>c!==category);save();paint();};label.append(input,document.createTextNode(title));choices.append(label);}root.append(choices);
 const picker=node('details',null,'statPicker');picker.append(node('summary','Choose players to track'));
 const pickerList=node('div',null,'statPickerList');picker.append(node('p','Stat selections are separate from voice announcement filters. Use the search and position filters to find players.','settingHint'),pickerList);root.append(picker);
 const status=node('p',null,'settingHint'),leaders=node('div',null,'statsLeaders'),players=node('div',null,'statsPlayers');root.append(status,leaders,players);
 root.append(node('p','Fantasy estimates: 1/25 pass yd, 4/pass TD, −2/INT, 1/10 rush or receiving yd, 6/rush, receiving or return TD, −2/fumble lost; PPR adds 1/catch. Excludes two-point conversions, yardage bonuses, return yards, kicking and defense. Missing categories count as zero in estimates; — means no reported stat.','settingHint'));
 let state={rows:[],label:'',status:''},key='';
 const paint=()=>{
  const next=JSON.stringify([state,mode.value,position.value,search.value,scope.value,[...selected],enabled]);if(next===key)return;key=next;
  heading.textContent=`Players · ${state.label||'game leaders & fantasy'}`;status.textContent=state.status;
  const opened=new Set([...players.querySelectorAll('details[open]')].map(n=>n.dataset.playerId));
  leaders.replaceChildren();players.replaceChildren();pickerList.replaceChildren();
  const matches=p=>(position.value==='ALL'||p.position===position.value)&&`${p.name} ${p.team}`.toLowerCase().includes(search.value.trim().toLowerCase());
  for(const p of state.rows.filter(matches)){
   const label=node('label'),input=node('input');input.type='checkbox';input.checked=selected.has(String(p.id));
   input.onchange=()=>{if(input.checked)selected.add(String(p.id));else selected.delete(String(p.id));save();paint();};
   label.append(input,document.createTextNode(`${p.name} · ${p.team} ${p.position}`));pickerList.append(label);
  }
  if(!pickerList.childElementCount)pickerList.append(node('p','No matching players available yet.','emptyState'));
  picker.querySelector('summary').textContent=`Choose players to track · ${state.rows.filter(p=>selected.has(String(p.id))).length} selected in this game`;

  for(const [category,title] of [['passing','Top passer'],['rushing','Top rusher'],['receiving','Top receiver']]){
   if(!enabled.includes(category))continue;
   const box=node('div',null,'statLeader');box.append(node('strong',title));
   const best=statLeaders(state.rows,category);
   box.append(node('p',best.length?best.map(p=>`${p.name} (${p.team}) · ${stat(p,category,'YDS')} yd${category==='receiving'?` · ${stat(p,category,'REC')??'—'} rec`:''} · ${stat(p,category,'TD')??'—'} TD`).join(' / '):'Not reported yet'));leaders.append(box);
  }
  const rows=state.rows.filter(p=>matches(p)&&(scope.value!=='selected'||selected.has(String(p.id)))).sort((a,b)=>Number(b.hasStats)-Number(a.hasStats)||fantasyBreakdown(b,mode.value).total-fantasyBreakdown(a,mode.value).total||a.name.localeCompare(b.name));
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
 mode.onchange=()=>{try{localStorage.setItem(`${storagePrefix}:fantasy`,mode.value);}catch{}paint();};position.onchange=paint;search.oninput=paint;scope.onchange=()=>{save();paint();};
 return {render(rows,label,message){state={rows,label,status:message};paint();},open(){root.open=true;root.scrollIntoView({block:'start',behavior:'smooth'});}};
}
