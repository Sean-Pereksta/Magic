let customRosterDraft=BasketballCustomRosters.normalize(null);
let customMatchRoster=null;
let customRosterAutoBudget=true;
const customRosterAutoBudgetKey=BasketballCustomRosters.SAVE_KEY+'.autoBudget';
const customRosterPickerState={team:null,slot:null,role:'all',overall:'all',search:'',page:0};
const customRosterCatalogCache={static:null,campaignKey:null,campaign:[],storedKey:null,storedState:null};
let customRosterRollNotice=null;

function loadCustomRosterDraft(){
  let saved=null;
  try{
    saved=localStorage.getItem(BasketballCustomRosters.SAVE_KEY);
    customRosterDraft=BasketballCustomRosters.normalize(JSON.parse(saved||'null'));
    const flag=localStorage.getItem(customRosterAutoBudgetKey);
    customRosterAutoBudget=flag===null?!saved:flag==='true';
  }catch(_error){customRosterDraft=BasketballCustomRosters.normalize(null);customRosterAutoBudget=!saved;}
  customRosterRollNotice=null;customRosterPickerState.team=null;
  return syncCustomRosterBudget();
}
function syncCustomRosterBudget(){
  if(!customRosterAutoBudget)return false;
  const cap=({1:80,2:160,3:250})[teamSize]||250,changed=customRosterDraft.cap!==cap;
  customRosterDraft.cap=cap;return changed;
}
function saveCustomRosterDraft(){
  try{
    localStorage.setItem(BasketballCustomRosters.SAVE_KEY,JSON.stringify(customRosterDraft));
    localStorage.setItem(customRosterAutoBudgetKey,String(customRosterAutoBudget));
  }catch(_error){}
}
function customRosterCatalog(){
  const cache=customRosterCatalogCache;
  if(!cache.static)cache.static=[...BasketballCustomRosters.quickPlayCatalog(profiles,RiseToGlory),...BasketballCustomRosters.catalog(profiles,RiseToGlory,null)];
  let franchise=riseState;
  if(!franchise){
    let stored='';
    try{stored=[RiseToGlory.SAVE_KEY,RiseToGlory.BACKUP_KEY,'basketballRiseToGloryV2','basketballRiseToGloryV1'].map(key=>localStorage.getItem(key)||'').join('|');}catch(_error){}
    if(stored!==cache.storedKey){cache.storedKey=stored;cache.storedState=stored?RiseToGlory.load():null;}
    franchise=cache.storedState;
  }
  // Cache generated athletes and normalized portraits. Snapshot the small owned
  // roster so direct upgrades and imported ratings still invalidate the cache.
  const key=franchise?JSON.stringify([franchise.team,franchise.roster,franchise.roster.map(id=>franchise.players[id])]):'';
  if(key!==cache.campaignKey){
    cache.campaignKey=key;
    cache.campaign=franchise?BasketballCustomRosters.catalog(profiles,RiseToGlory,franchise).filter(entry=>entry.id.startsWith('rise:')):[];
  }
  return [...cache.static,...cache.campaign];
}
function customRosterRole(entry){
  const direct=entry?.role||entry?.identity||entry?.profile?.campaignArchetype||entry?.profile?.archetype||'allRounder';
  if(BasketballCustomRosters.QUICK_PLAY_ROLES.some(role=>role.id===direct))return direct;
  const aliases={shooter:'sharpshooter',sharp_shooter:'sharpshooter',midrange_specialist:'midrange',post_scorer:'interior',rebounder:'interior',rim_protector:'rimProtector',stretch_big:'stretchBig',all_rounder:'allRounder',balanced:'allRounder',anchor:'rimProtector',stopper:'defender',primary:'midrange',sixth_man:'allRounder'};
  return aliases[direct]||'allRounder';
}
function customRosterRoleLabel(entry){
  if(entry?.roleLabel)return entry.roleLabel;
  const role=customRosterRole(entry);
  return BasketballCustomRosters.QUICK_PLAY_ROLES.find(item=>item.id===role)?.label||'All-Rounder';
}
function customRosterHuman(team,slot){return team===0&&slot===0?'Player 1':twoPlayerMode&&(localTeamMode==='cooperative'?team===0&&slot===1:team===1&&slot===0)?'Player 2':'AI';}
function customRosterSignature(){return JSON.stringify([teamSize,customRosterDraft.cap,customRosterDraft.teams]);}
function buildCustomRosterSetup(){
  loadCustomRosterDraft();
  document.getElementById('customRosterCap').oninput=event=>{
    customRosterAutoBudget=false;customRosterDraft.cap=Number(event.target.value);customRosterRollNotice=null;
    saveCustomRosterDraft();refreshCustomRosterSetup(false);
  };
  document.getElementById('customRosterFill').onclick=()=>{
    if(!Number.isInteger(customRosterDraft.cap)||customRosterDraft.cap<25||customRosterDraft.cap>297)return;
    customRosterDraft=BasketballCustomRosters.rollRandom(customRosterDraft,teamSize,customRosterCatalog());
    const result=BasketballCustomRosters.validate(customRosterDraft,teamSize,customRosterCatalog());
    customRosterRollNotice={signature:customRosterSignature(),text:!result.ok?'No complete matchup fits this MAX. Raise it or choose fewer players.':result.totals.every(total=>total===customRosterDraft.cap)?'Rolled both teams to the exact MAX.':'Rolled the closest available totals under MAX.'};
    customRosterPickerState.team=null;saveCustomRosterDraft();refreshCustomRosterSetup();
  };
  for(const [id,key] of [['customRosterRoleFilter','role'],['customRosterOverallFilter','overall']])document.getElementById(id).onchange=event=>{
    customRosterPickerState[key]=event.target.value;customRosterPickerState.page=0;renderCustomRosterPicker();
  };
  document.getElementById('customRosterPlayerSearch').oninput=event=>{
    customRosterPickerState.search=event.target.value;customRosterPickerState.page=0;renderCustomRosterPicker();
  };
  document.getElementById('customRosterPickerClose').onclick=closeCustomRosterPicker;
  document.getElementById('customRosterPicker').addEventListener('keydown',event=>{if(event.key==='Escape'){event.preventDefault();closeCustomRosterPicker();}});
  document.getElementById('customRosterPickerPrev').onclick=()=>{customRosterPickerState.page--;renderCustomRosterPicker();};
  document.getElementById('customRosterPickerNext').onclick=()=>{customRosterPickerState.page++;renderCustomRosterPicker();};
  const settings=document.getElementById('quickPlayMatchSettings');
  for(const event of ['click','change','toggle'])settings.addEventListener(event,()=>queueMicrotask(refreshQuickPlaySettingsSummary));
  refreshCustomRosterSetup();
}
function refreshQuickPlaySettingsSummary(){
  const label=document.getElementById('quickPlaySettingsSummary');if(!label)return;
  const difficulty=String(selectedDifficulty).replace(/^./,char=>char.toUpperCase());
  label.textContent=`${courts[selectedCourt]?.name||'Classic Arena'} · ${difficulty} · First to ${targetScore}`;
}
function closeCustomRosterPicker(){
  const {team,slot}=customRosterPickerState;customRosterPickerState.team=null;renderCustomRosterPicker();
  document.querySelector(`[data-roster-browse][data-team="${team}"][data-slot="${slot}"]`)?.focus({preventScroll:true});
}
function openCustomRosterPicker(team,slot){
  Object.assign(customRosterPickerState,{team,slot,page:0});renderCustomRosterPicker();
  document.getElementById('customRosterPicker').scrollIntoView({block:'nearest'});
  document.getElementById('customRosterRoleFilter').focus({preventScroll:true});
}
function chooseCustomRosterPlayer(team,slot,id){
  customRosterDraft.teams[team][slot]=id;customRosterRollNotice=null;customRosterPickerState.team=null;
  saveCustomRosterDraft();refreshCustomRosterSetup();
  document.querySelector(`[data-roster-browse][data-team="${team}"][data-slot="${slot}"]`)?.focus({preventScroll:true});
}
function renderCustomRosterPicker(entries=customRosterCatalog()){
  const panel=document.getElementById('customRosterPicker');if(!panel)return;
  const picker=customRosterPickerState;panel.hidden=picker.team===null||picker.slot>=teamSize||!customRosterDraft.enabled;
  document.querySelectorAll('[data-roster-browse]').forEach(button=>button.setAttribute('aria-expanded',String(!panel.hidden&&Number(button.dataset.team)===picker.team&&Number(button.dataset.slot)===picker.slot)));
  if(panel.hidden)return;
  document.getElementById('customRosterPickerTitle').textContent=`Choose ${picker.team===0?'Blue':'Coral'} athlete ${picker.slot+1}`;
  document.getElementById('customRosterPickerSubtitle').textContent=`${customRosterHuman(picker.team,picker.slot)} · Every OVR is the athlete’s actual rating.`;
  const roleSelect=document.getElementById('customRosterRoleFilter'),roles=new Map();
  entries.forEach(entry=>{const role=customRosterRole(entry);if(!roles.has(role))roles.set(role,customRosterRoleLabel(entry));});
  const oldRole=picker.role;roleSelect.replaceChildren(new Option('All roles','all'));
  [...roles].sort((a,b)=>a[1].localeCompare(b[1])).forEach(([role,label])=>roleSelect.add(new Option(label,role)));
  picker.role=roles.has(oldRole)?oldRole:'all';roleSelect.value=picker.role;
  document.getElementById('customRosterOverallFilter').value=picker.overall;
  document.getElementById('customRosterPlayerSearch').value=picker.search;
  const search=picker.search.trim().toLowerCase();
  const filtered=entries.filter(entry=>(picker.role==='all'||customRosterRole(entry)===picker.role)&&(picker.overall==='all'||entry.overall===Number(picker.overall))&&(!search||`${entry.name} ${customRosterRoleLabel(entry)} ${entry.source}`.toLowerCase().includes(search)));
  const pageSize=6,pageCount=Math.max(1,Math.ceil(filtered.length/pageSize));picker.page=Math.max(0,Math.min(picker.page,pageCount-1));
  const results=document.getElementById('customRosterPickerResults');results.replaceChildren();
  const current=customRosterDraft.teams[picker.team][picker.slot];
  const others=customRosterDraft.teams[picker.team].slice(0,teamSize).filter((_,slot)=>slot!==picker.slot),teammatesTotal=BasketballCustomRosters.total(others,entries);
  const used=new Set(customRosterDraft.teams.flatMap((ids,team)=>ids.slice(0,teamSize).filter((_,slot)=>team!==picker.team||slot!==picker.slot)));
  for(const entry of filtered.slice(picker.page*pageSize,(picker.page+1)*pageSize)){
    const button=document.createElement('button');button.type='button';button.className='custom-roster-player';
    button.dataset.rosterPlayer=entry.id;button.dataset.role=customRosterRole(entry);button.dataset.overall=String(entry.overall);button.disabled=used.has(entry.id);
    button.classList.toggle('selected',entry.id===current);button.setAttribute('aria-pressed',String(entry.id===current));
    const rating=document.createElement('span');rating.className='custom-roster-rating';rating.textContent=entry.overall;
    const ratingLabel=document.createElement('small');ratingLabel.textContent='OVR';rating.append(ratingLabel);
    const text=document.createElement('span');text.className='custom-roster-player-copy';
    const name=document.createElement('strong');name.textContent=entry.name;
    const role=document.createElement('span');role.textContent=customRosterRoleLabel(entry);
    const detail=document.createElement('small'),over=teammatesTotal+entry.overall-customRosterDraft.cap;
    detail.textContent=used.has(entry.id)?'Already selected':entry.id===current?'Current selection':over>0?`${over} over team MAX`:entry.source;
    detail.classList.toggle('over-budget',over>0&&!used.has(entry.id));text.append(name,role,detail);button.append(rating,text);
    button.onclick=()=>chooseCustomRosterPlayer(picker.team,picker.slot,entry.id);results.append(button);
  }
  if(!filtered.length){const empty=document.createElement('p');empty.className='custom-roster-empty';empty.textContent='No players match. Try another role or OVR.';results.append(empty);}
  document.getElementById('customRosterPickerCount').textContent=`${filtered.length} player${filtered.length===1?'':'s'} · Page ${picker.page+1} of ${pageCount}`;
  document.getElementById('customRosterPickerPrev').disabled=picker.page===0;document.getElementById('customRosterPickerNext').disabled=picker.page>=pageCount-1;
}
function refreshCustomRosterSetup(rebuild=true){
  const enabled=document.getElementById('customRosterEnabled');if(!enabled)return;
  if(syncCustomRosterBudget()&&customRosterDraft.enabled){
    customRosterDraft=BasketballCustomRosters.fill(customRosterDraft,teamSize,customRosterCatalog(),[`classic:${selectedP1}`,`classic:${selectedP2}`]);saveCustomRosterDraft();rebuild=true;
  }
  enabled.checked=customRosterDraft.enabled;
  document.getElementById('quickPlayScreen').classList.toggle('custom-roster-active',customRosterDraft.enabled);
  if(rebuild)document.getElementById('customRosterCap').value=customRosterDraft.cap;
  const entries=customRosterCatalog(),byId=new Map(entries.map(entry=>[entry.id,entry]));
  if(rebuild&&customRosterDraft.enabled){
    const grid=document.getElementById('customRosterTeams');grid.replaceChildren();
    for(let team=0;team<2;team++){
      const panel=document.createElement('section');panel.className=`custom-roster-team ${team===0?'blue':'coral'}`;
      const header=document.createElement('div');header.className='custom-roster-team-header';
      const heading=document.createElement('h3');heading.textContent=`${team===0?'Blue':'Coral'} team`;
      const total=document.createElement('p');total.id=`customRosterTotal${team}`;total.className='custom-roster-total';header.append(heading,total);panel.append(header);
      const meter=document.createElement('meter');meter.id=`customRosterMeter${team}`;meter.min=0;meter.setAttribute('aria-label',`${team===0?'Blue':'Coral'} team OVR budget`);panel.append(meter);
      const remaining=document.createElement('p');remaining.id=`customRosterRemaining${team}`;remaining.className='custom-roster-remaining';panel.append(remaining);
      for(let slot=0;slot<teamSize;slot++){
        const row=document.createElement('div');row.className='custom-roster-slot';
        const top=document.createElement('div');top.className='custom-roster-slot-top';
        const human=customRosterHuman(team,slot),entry=byId.get(customRosterDraft.teams[team][slot]);
        const number=document.createElement('span');number.className='custom-roster-slot-number';number.textContent=String(slot+1).padStart(2,'0');
        const assignment=document.createElement('span');assignment.className='custom-roster-assignment';assignment.textContent=human;
        const role=document.createElement('span');role.className='custom-roster-role';role.textContent=entry?customRosterRoleLabel(entry):'Open spot';
        const rating=document.createElement('span');rating.className='custom-roster-slot-ovr';rating.textContent=entry?`${entry.overall} OVR`:'— OVR';
        top.append(number,assignment,role,rating);row.append(top);
        const select=document.createElement('select');select.id=`customRosterTeam${team}Slot${slot}`;select.setAttribute('aria-label',`${team===0?'Blue':'Coral'} athlete ${slot+1} · ${human}`);select.add(new Option('Choose a player',''));
        const groups=new Map();
        for(const optionEntry of entries){
          const groupName=optionEntry.source==='Quick Play'?`Quick Play · ${customRosterRoleLabel(optionEntry)}`:optionEntry.source;
          let group=groups.get(groupName);if(!group){group=document.createElement('optgroup');group.label=groupName;groups.set(groupName,group);select.append(group);}
          const option=new Option(`${optionEntry.name} · ${optionEntry.overall} OVR`,optionEntry.id);
          option.disabled=customRosterDraft.teams.some((ids,t)=>ids.slice(0,teamSize).some((id,s)=>id===optionEntry.id&&(t!==team||s!==slot)));group.append(option);
        }
        select.value=customRosterDraft.teams[team][slot]||'';select.onchange=()=>chooseCustomRosterPlayer(team,slot,select.value);
        select.hidden=true;select.tabIndex=-1;select.setAttribute('aria-hidden','true');row.append(select);
        const chosen=document.createElement('span');chosen.className='custom-roster-chosen-name';chosen.textContent=entry?.name||'Choose your player';row.append(chosen);
        const browse=document.createElement('button');browse.type='button';browse.className='custom-roster-browse';browse.dataset.rosterBrowse='';browse.dataset.team=team;browse.dataset.slot=slot;
        browse.textContent=entry?'Change player':'Choose player';browse.setAttribute('aria-label',`${entry?'Change':'Choose'} ${team===0?'Blue':'Coral'} athlete ${slot+1}`);browse.setAttribute('aria-expanded',String(customRosterPickerState.team===team&&customRosterPickerState.slot===slot));browse.setAttribute('aria-controls','customRosterPicker');browse.onclick=()=>openCustomRosterPicker(team,slot);row.append(browse);panel.append(row);
      }
      grid.append(panel);
    }
  }
  const result=BasketballCustomRosters.validate(customRosterDraft,teamSize,entries),budget=customRosterDraft.cap;
  const validBudget=Number.isInteger(budget)&&budget>=25&&budget<=297;
  document.getElementById('customRosterFill').disabled=!validBudget;
  for(let team=0;team<2;team++){
    const label=document.getElementById(`customRosterTotal${team}`);if(!label)continue;
    const spent=result.totals[team],available=budget-spent;label.textContent=`${spent} / ${budget} OVR`;label.classList.toggle('over-budget',available<0);
    const meter=document.getElementById(`customRosterMeter${team}`);meter.max=Math.max(1,budget||1);meter.value=spent;meter.classList.toggle('over-budget',available<0);
    const remaining=document.getElementById(`customRosterRemaining${team}`);remaining.textContent=!validBudget?'Set a MAX from 25 to 297':available<0?`${-available} over MAX`:available===0?'Exact MAX':`${available} OVR available`;remaining.classList.toggle('over-budget',available<0);
  }
  const status=document.getElementById('customRosterStatus'),notice=customRosterRollNotice?.signature===customRosterSignature()?customRosterRollNotice.text:'';
  status.textContent=result.ok?(notice||'Both teams are ready.')+` Maximum ${budget} total OVR per team.`:[notice,...result.errors].filter(Boolean).join(' ');status.classList.toggle('invalid',!result.ok);
  const start=document.getElementById('startBtn');start.disabled=customRosterDraft.enabled&&!result.ok;start.setAttribute('aria-describedby',customRosterDraft.enabled?'customRosterStatus':'matchSetupSummary');
  renderCustomRosterPicker(entries);
  refreshQuickPlaySettingsSummary();
}
function prepareCustomMatchRoster(){
  customMatchRoster=null;if(!customRosterDraft.enabled)return true;
  customMatchRoster=BasketballCustomRosters.snapshot(customRosterDraft,teamSize,customRosterCatalog());
  if(!customMatchRoster){refreshCustomRosterSetup();return false;}return true;
}
function customMatchProfile(team,slot){return customMatchRoster?.teams?.[team]?.[slot]?JSON.parse(JSON.stringify(customMatchRoster.teams[team][slot])):null;}
