let customRosterDraft=BasketballCustomRosters.normalize(null);
let customMatchRoster=null;

function loadCustomRosterDraft(){
  try{customRosterDraft=BasketballCustomRosters.normalize(JSON.parse(localStorage.getItem(BasketballCustomRosters.SAVE_KEY)||'null'));}
  catch(_error){customRosterDraft=BasketballCustomRosters.normalize(null);}
}
function saveCustomRosterDraft(){
  try{localStorage.setItem(BasketballCustomRosters.SAVE_KEY,JSON.stringify(customRosterDraft));}catch(_error){}
}
function customRosterCatalog(){
  const franchise=riseState||RiseToGlory.load();
  return BasketballCustomRosters.catalog(profiles,RiseToGlory,franchise);
}
function buildCustomRosterSetup(){
  loadCustomRosterDraft();
  document.getElementById('customRosterEnabled').onchange=event=>{
    customRosterDraft.enabled=event.target.checked;
    customRosterDraft=BasketballCustomRosters.fill(customRosterDraft,teamSize,customRosterCatalog(),[`classic:${selectedP1}`,`classic:${selectedP2}`]);
    saveCustomRosterDraft();refreshCustomRosterSetup();
  };
  document.getElementById('customRosterCap').oninput=event=>{
    customRosterDraft.cap=Number(event.target.value);
    saveCustomRosterDraft();refreshCustomRosterSetup(false);
  };
  document.getElementById('customRosterFill').onclick=()=>{
    customRosterDraft.teams=[[],[]];
    customRosterDraft=BasketballCustomRosters.fill(customRosterDraft,teamSize,customRosterCatalog(),[`classic:${selectedP1}`,`classic:${selectedP2}`]);
    saveCustomRosterDraft();refreshCustomRosterSetup();
  };
  refreshCustomRosterSetup();
}
function refreshCustomRosterSetup(rebuild=true){
  const enabled=document.getElementById('customRosterEnabled');if(!enabled)return;
  enabled.checked=customRosterDraft.enabled;
  document.getElementById('quickPlayScreen').classList.toggle('custom-roster-active',customRosterDraft.enabled);
  document.getElementById('customRosterBuilder').hidden=!customRosterDraft.enabled;
  if(rebuild) document.getElementById('customRosterCap').value=customRosterDraft.cap;
  const entries=customRosterCatalog();
  if(rebuild&&customRosterDraft.enabled){
    const grid=document.getElementById('customRosterTeams');grid.replaceChildren();
    for(let team=0;team<2;team++){
      const panel=document.createElement('section');panel.className=`custom-roster-team ${team===0?'blue':'coral'}`;
      const heading=document.createElement('h3');heading.textContent=`${team===0?'Blue':'Coral'} team`;panel.append(heading);
      const total=document.createElement('p');total.id=`customRosterTotal${team}`;total.className='custom-roster-total';panel.append(total);
      for(let slot=0;slot<teamSize;slot++){
        const label=document.createElement('label');
        const human=team===0&&slot===0?'Player 1':twoPlayerMode&&(localTeamMode==='cooperative'?team===0&&slot===1:team===1&&slot===0)?'Player 2':'AI';
        const title=document.createElement('span');title.textContent=`${slot+1} · ${human}`;label.append(title);
        const select=document.createElement('select');select.id=`customRosterTeam${team}Slot${slot}`;select.setAttribute('aria-label',`${team===0?'Blue':'Coral'} athlete ${slot+1} · ${human}`);
        select.add(new Option('Choose an athlete',''));
        const groups=new Map();
        for(const entry of entries){
          let group=groups.get(entry.source);if(!group){group=document.createElement('optgroup');group.label=entry.source;groups.set(entry.source,group);select.append(group);}
          const option=new Option(`${entry.name} · ${entry.overall} OVR`,entry.id);
          const selectedElsewhere=customRosterDraft.teams.some((ids,t)=>ids.slice(0,teamSize).some((id,s)=>id===entry.id&&(t!==team||s!==slot)));
          option.disabled=selectedElsewhere;group.append(option);
        }
        select.value=customRosterDraft.teams[team][slot]||'';
        select.onchange=()=>{customRosterDraft.teams[team][slot]=select.value;saveCustomRosterDraft();refreshCustomRosterSetup();};
        label.append(select);
        const entry=entries.find(item=>item.id===select.value),detail=document.createElement('small');
        detail.textContent=entry?`${entry.source} · ${String(entry.identity).replaceAll('_',' ').replace(/([a-z])([A-Z])/g,'$1 $2')}`:'Select a player for this spot.';
        label.append(detail);panel.append(label);
      }
      grid.append(panel);
    }
  }
  const result=BasketballCustomRosters.validate(customRosterDraft,teamSize,entries);
  for(let team=0;team<2;team++){
    const label=document.getElementById(`customRosterTotal${team}`);if(!label)continue;
    label.textContent=`${result.totals[team]} / ${customRosterDraft.cap} OVR`;
    label.classList.toggle('over-budget',result.totals[team]>customRosterDraft.cap);
  }
  const status=document.getElementById('customRosterStatus');
  status.textContent=result.ok?`Both teams are ready. At most ${customRosterDraft.cap} total OVR per team.`:result.errors.join(' ');
  status.classList.toggle('invalid',!result.ok);
  const start=document.getElementById('startBtn');
  start.disabled=customRosterDraft.enabled&&!result.ok;
  start.setAttribute('aria-describedby',customRosterDraft.enabled?'customRosterStatus':'matchSetupSummary');
}
function prepareCustomMatchRoster(){
  customMatchRoster=null;
  if(!customRosterDraft.enabled) return true;
  customMatchRoster=BasketballCustomRosters.snapshot(customRosterDraft,teamSize,customRosterCatalog());
  if(!customMatchRoster){refreshCustomRosterSetup();return false;}
  return true;
}
function customMatchProfile(team,slot){
  return customMatchRoster?.teams?.[team]?.[slot] ? JSON.parse(JSON.stringify(customMatchRoster.teams[team][slot])) : null;
}
