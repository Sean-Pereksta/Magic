import { BATTLE_SIZE_OPTIONS, createBattle } from './battle-sim.mjs';

const roles = [['menAtArms','Infantry'],['spearman','Spears'],['archer','Archers'],['crossbow','Crossbows'],['lightCavalry','Cavalry']];
const engines = [['ram','Battering rams'],['catapult','Catapults'],['trebuchet','Trebuchets']];
const scenarioArmies = [
  {menAtArms:32,spearman:16,archer:12,crossbow:6,lightCavalry:6,ram:1},
  {menAtArms:12,spearman:16,archer:16,crossbow:4,lightCavalry:0,ram:0}
];
const armyNames = ['Westmarch Company','Stonewatch Garrison'];
const soldierCount = units => roles.reduce((n,[type])=>n+units[type],0);
const terrains = {plains:'Open plains',hills:'Rolling hills',forest:'Woodland',river:'River crossing',pass:'Mountain pass'};
// Use the same settlement, fort and wall levels as a campaign tile.
const defenses = {
  open:{name:'Open market town',building:'town',levels:{town:1},walls:0,fortIntegrity:0},
  settlement:{name:'Frontier stockade',building:null,levels:{wall:1},walls:35,fortIntegrity:0},
  town:{name:'Town palisade',building:'town',levels:{town:1,wall:1},walls:45,fortIntegrity:0},
  fort:{name:'West Wall Assault',building:'fort',levels:{fort:1,wall:1},walls:60,fortIntegrity:45},
  fortress:{name:'Stone fortress',building:'fort',levels:{fort:2,wall:2},walls:120,fortIntegrity:100},
  great:{name:'Great fortress',building:'fort',levels:{fort:3,wall:3},walls:180,fortIntegrity:180},
  city:{name:'Grand city',building:'city',levels:{city:3,wall:2},walls:120,fortIntegrity:0},
  citadel:{name:'Royal city citadel',building:'city',levels:{city:4,wall:3},walls:180,fortIntegrity:0}
};

export function westWallAssault(){
  return {mode:'attack',defenses:'fort',playerSide:0,seed:8147,terrain:'plains',limit:150,armies:structuredClone(scenarioArmies)};
}
export function royalCitySiege(){
  const settings=westWallAssault();
  return {...settings,defenses:'citadel',limit:400,armies:[balancedArmy(240,0,2),balancedArmy(200,1,0)]};
}
export function balancedArmy(soldiers, side, engines){
  const weights=scenarioArmies[side], total=soldierCount(weights);
  const shares=roles.map(([type])=>({type,exact:soldiers*weights[type]/total}));
  const army=Object.fromEntries(shares.map(({type,exact})=>[type,Math.floor(exact)]));
  const remaining=soldiers-soldierCount(army);
  shares.sort((a,b)=>(b.exact%1)-(a.exact%1));
  for(const {type} of shares.slice(0,remaining))army[type]++;
  return {...army,ram:engines};
}
export function customDeployment(settings){
  const {armies,seed,limit,mode,playerSide,terrain}=settings;
  if(!['field','attack','defend'].includes(mode)||![0,1].includes(playerSide)||!Object.hasOwn(terrains,terrain)||!BATTLE_SIZE_OPTIONS.includes(limit))throw new Error('Choose a valid battlefield and active limit.');
  const castle=defenses[settings.defenses??'fort'];
  if(!castle)throw new Error('Choose a settlement or castle to defend.');
  const encounter={id:`custom-${seed}`,seed,playerSide,playerOwner:playerSide===0?'ashen':'wintermere',
    tile:{id:'custom',terrain,...(mode==='field'?{name:'Battle of the Marches',building:null,walls:0,fortIntegrity:0}:structuredClone(castle))},
    participants:armies.map((units,side)=>{
      const siege=engines.map(([type])=>units[type]??0);
      if([...roles.map(([type])=>units[type]),...siege].some(n=>!Number.isSafeInteger(n)||n<0)||soldierCount(units)<1||soldierCount(units)>1000||siege.reduce((sum,n)=>sum+n,0)>20)throw new Error('Each side needs 1–1,000 soldiers and can bring up to 20 engines.');
      return {armyId:`custom-side-${side}`,owner:side===0?'ashen':'wintermere',name:armyNames[side],side,playerControlled:side===playerSide,morale:1,units:{...units}};
    })};
  return {encounter,battle:createBattle(encounter,{maxActive:limit,autoGarrison:true})};
}

export function installCustomBattle(start,getPreferredLimit=()=>null){
  const button=document.createElement('button');
  button.type='button';button.id='custom-battle';button.textContent='Custom Battle';
  document.querySelector('#start-game').after(button);
  const dialog=document.createElement('dialog');dialog.id='custom-setup';dialog.dataset.battleUi='';
  const options=(items)=>Object.entries(items).map(([value,label])=>`<option value="${value}">${label}</option>`).join('');
  const armyFields=side=>`<fieldset class="custom-army"><legend>${side?'Defenders':'Attackers'}</legend>
    <label>Soldiers <input type="number" required min="1" max="1000" id="custom-total-${side}"></label>
    <details><summary>Army composition</summary>${[...roles,...engines.slice(1)].map(([type,label])=>`<label>${label}<input type="number" required min="0" max="${roles.some(([role])=>role===type)?1000:20}" data-side="${side}" data-unit="${type}"></label>`).join('')}
    <button type="button" data-balance="${side}">Balanced mix</button></details>
    <label>Battering rams <input type="number" required min="0" max="20" data-side="${side}" data-unit="ram"></label>
    <p data-estimate="${side}" class="fine"></p></fieldset>`;
  dialog.innerHTML=`<header class="dialog-header"><div><span class="eyebrow">OFFLINE · CUSTOM BATTLE</span><h2>Muster your armies</h2></div><button type="button" id="custom-close" aria-label="Back to main menu">×</button></header>
    <form class="dialog-body"><p id="custom-result" role="status"></p><div class="button-row" aria-label="Ready armies"><button type="button" data-scenario="west">West Wall Assault</button><button type="button" data-scenario="royal">Royal City Siege</button></div><div class="custom-options">
    <label>Battle <select name="mode">${options({field:'Field Battle',attack:'Attack a Fort',defend:'Defend a Fort'})}</select></label>
    <label>Command <select name="side">${options({0:'Attackers',1:'Defenders'})}</select></label>
    <label>Terrain <select name="terrain">${options(terrains)}</select></label>
    <label>Defenses <select name="defenses">${options(Object.fromEntries(Object.entries(defenses).map(([id,site])=>[id,id==='fort'?'Stone fort':site.name])))}</select></label>
    <label>Combined active limit <select name="limit">${options(Object.fromEntries(BATTLE_SIZE_OPTIONS.map(n=>[n,n])))}</select></label></div>
    <div class="custom-armies">${armyFields(0)}${armyFields(1)}</div><p id="custom-field-note"></p>
    <div class="button-row"><button type="submit" class="primary" value="fight">Fight Now</button><button type="submit" value="deploy">Deploy First</button><button type="button" id="custom-rematch" hidden>Rematch</button><button type="button" id="custom-new">New Field</button></div></form>`;
  document.body.append(dialog);
  const form=dialog.querySelector('form'), get=id=>dialog.querySelector(`#${id}`);
  let settings=westWallAssault(), last=null, prepared=null, customLimit=null;
  function fill(){
    for(const key of ['mode','terrain','limit','defenses'])form.elements[key].value=settings[key];
    form.elements.defenses.disabled=settings.mode==='field';
    form.elements.side.value=settings.playerSide;
    for(const [side,army] of settings.armies.entries()){
      get(`custom-total-${side}`).value=soldierCount(army);
      for(const el of form.querySelectorAll(`[data-side="${side}"]`))el.value=army[el.dataset.unit]??0;
    }
    preview();
  }
  function preview(){
    prepared=null;
    const note=get('custom-field-note');
    try{
      if(!form.checkValidity())throw new Error('Enter a whole number within the displayed range.');
      prepared=customDeployment(settings);
      for(const side of [0,1]){
        const troops=prepared.battle.units.filter(u=>u.side===side),soldiers=troops.filter(u=>u.family!=='siege');
        const engines=troops.length-soldiers.length;
        const deployed=troops.filter(u=>u.status==='active');
        const engineLabel=n=>`${n} ${n===1?'engine':'engines'}`;
        dialog.querySelector(`[data-estimate="${side}"]`).textContent=`${soldiers.length} soldiers · ${engineLabel(engines)}. ${deployed.length} start on field (${engineLabel(deployed.filter(u=>u.family==='siege').length)}) · ${troops.length-deployed.length} in reserve.`;
      }
      const castle=prepared.battle.layout.castle;
      const walls=castle?(castle.inner?'Outer and inner walls. ':castle.enclosed?'One defended perimeter. ':'Open approaches. '):'';
      note.textContent=`${prepared.encounter.tile.name} · Field ${settings.seed}. ${walls}Reinforcements enter as spaces open.`;
    }catch(error){note.textContent=error.message;for(const el of dialog.querySelectorAll('[data-estimate]'))el.textContent='';}
    for(const el of form.querySelectorAll('[type=submit]'))el.disabled=!prepared;
  }
  form.oninput=({target})=>{
    if(target.tagName!=='INPUT')return;
    const side=Number(target.dataset.side??target.id.split('-').at(-1));
    if(target.dataset.unit){
      settings.armies[side][target.dataset.unit]=target.valueAsNumber;
      get(`custom-total-${side}`).value=soldierCount(settings.armies[side]);
    }else if(target.validity.valid&&Number.isInteger(target.valueAsNumber)){
      Object.assign(settings.armies[side],balancedArmy(target.valueAsNumber,side,settings.armies[side].ram));
      for(const el of form.querySelectorAll(`[data-side="${side}"]`))el.value=settings.armies[side][el.dataset.unit]??0;
    }
    preview();
  };
  form.onchange=({target})=>{
    if(target.tagName!=='SELECT')return;
    const {name,value}=target;
    if(name==='side'){
      settings.playerSide=Number(value);
      if(settings.mode!=='field')settings.mode=settings.playerSide?'defend':'attack';
    }else{
      settings[name]=name==='limit'?Number(value):value;
      if(name==='limit')customLimit=settings.limit;
      if(name==='mode'&&value!=='field')settings.playerSide=Number(value==='defend');
    }
    fill();
  };
  form.onclick=({target})=>{
    if(target.dataset.scenario){
      settings=target.dataset.scenario==='west'?westWallAssault():royalCitySiege();
      settings.limit=customLimit??getPreferredLimit()??settings.limit;
      get('custom-result').textContent='';fill();return;
    }
    if(!target.hasAttribute('data-balance'))return;
    const side=Number(target.dataset.balance), n=soldierCount(settings.armies[side]);
    if(Number.isSafeInteger(n)&&n>0){Object.assign(settings.armies[side],balancedArmy(n,side,settings.armies[side].ram));fill();}
  };
  function launch(deploy,rematch=false){
    const source=rematch?last:prepared;
    if(!source)return;
    if(rematch){settings=structuredClone(last.settings);fill();}
    const session={settings:structuredClone(settings),encounter:structuredClone(source.encounter),initial:structuredClone(source.battle),deploy};
    dialog.close();start(session);
  }
  form.onsubmit=e=>{e.preventDefault();launch(e.submitter?.value==='deploy');};
  get('custom-rematch').onclick=()=>launch(false,true);
  get('custom-new').onclick=()=>{const seed=crypto.getRandomValues(new Uint32Array(1))[0];settings.seed=seed===settings.seed?(seed+1)>>>0:seed;get('custom-result').textContent='';fill();};
  get('custom-close').onclick=()=>dialog.close();
  function show(){fill();if(!dialog.open)dialog.showModal();}
  button.onclick=()=>{settings.limit=customLimit??getPreferredLimit()??settings.limit;show();};
  return {finish(session,result,message){
    if(result){
      last={settings:session.settings,encounter:session.encounter,battle:session.initial};
      get('custom-rematch').hidden=false;
      message=result.winner===null?'Draw':result.winner===session.encounter.playerSide?'Victory':'Defeat';
      message+=` at ${session.encounter.tile.name}. Choose Rematch to replay the same deployment.`;
    }
    get('custom-result').textContent=message;show();
  },dispose(){dialog.remove();button.remove();}};
}
