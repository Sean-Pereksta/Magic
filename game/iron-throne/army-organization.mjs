import {UNITS} from './data.mjs';
import {sizeOf} from './core.mjs';
import {formationCheck} from './warfare.mjs';
import {generalForArmy,markPlayerOverride,syncCommanders} from './command-state.mjs';
import {allArmies} from './naval-state.mjs';
const fail=error=>({ok:false,error});
const empty=()=>Object.fromEntries(Object.keys(UNITS).map(u=>[u,0]));
export const armyName=a=>a.name||`Army ${a.id.replace('army-','')}`;
// Compare only actor-visible state. Enemy/private updates cannot invalidate or leak into a draft.
export function organizationStamp(s,a){
  return JSON.stringify([s.turn,s.sequential?.id??null,a,allArmies(s).filter(x=>x.owner===a.owner&&x.commandId).map(x=>[x.id,x.commandId]).sort(),(s.commanders?.roster||[]).filter(g=>g.owner===a.owner).map(g=>g.id).sort()]);
}
export function createArmyDraft(s,owner,id){
  const a=s.armies.find(x=>x.id===id&&x.owner===owner);if(!a)return null;
  return {army:id,stamp:organizationStamp(s,a),transfers:[],formations:[{id:a.id,name:a.name||'',units:{...a.units},generalId:generalForArmy(s,a)?.id||null,formation:a.formation},{name:'',units:empty(),generalId:null,formation:'balanced'}]};
}
export function addDraftFormation(draft){draft.formations.push({name:'',units:empty(),generalId:null,formation:'balanced'});}
export function moveDraftUnits(draft,from,to,unit,count){
  const a=draft.formations[from],b=draft.formations[to];
  if(!a||!b||from===to||!Object.hasOwn(UNITS,unit)||!Number.isSafeInteger(count)||count<1||count>(a.units[unit]||0))return fail('Choose a whole number within the available troops.');
  a.units[unit]-=count;b.units[unit]=(b.units[unit]||0)+count;
  for(const f of [a,b])if(formationCheck(f,f.formation))f.formation='balanced';
  return {ok:true};
}
export function applyDraftPreset(draft,preset){
  const a=draft.formations[0];
  for(const [u,n] of Object.entries(a.units)){
    const family={archers:'ranged',cavalry:'mounted',siege:'siege'}[preset];
    const count=family?(UNITS[u].family===family?n:0):preset==='quarter'?Math.ceil(n*.25):Math.ceil(n*.5);
    if(count)moveDraftUnits(draft,0,1,u,count);
  }
}
export function validateArmyDraft(s,owner,draft){
  if(s.outcome)return fail('This campaign has ended.');
  const a=s.armies.find(x=>x.id===draft?.army&&x.owner===owner);
  if(!a)return fail('Select one of your land armies. Disembark transported troops first.');
  if(typeof draft.stamp!=='string'||draft.stamp!==organizationStamp(s,a))return fail('This army or its commanders changed. Close and reopen Sort Army before confirming.');
  if(!Array.isArray(draft.formations)||draft.formations.length<1||draft.formations.length>100||!Array.isArray(draft.transfers)||draft.transfers.length>16)return fail('Invalid formations.');
  const totals=empty(),generals=new Set(),roster=s.commanders?.roster||[];
  for(const [i,f] of draft.formations.entries()){
    if(!f||f.id!==undefined&&(i!==0||f.id!==a.id)||typeof f.name!=='string'||f.name.length>60||!f.units||Array.isArray(f.units)||Object.keys(f.units).some(u=>!Object.hasOwn(UNITS,u)))return fail('Invalid army name, identity or troop type.');
    for(const u of Object.keys(UNITS)){
      const n=f.units[u]??0;if(!Number.isSafeInteger(n)||n<0||n>100000)return fail('Troop quantities must be nonnegative whole numbers.');totals[u]+=n;
    }
    if(formationCheck(f,f.formation)&&sizeOf(f))return fail(formationCheck(f,f.formation));
    if(f.generalId!==null&&f.generalId!==undefined){
      const g=roster.find(g=>g.id===f.generalId&&g.owner===owner);
      if(!g||['dead','wounded','unavailable'].includes(g.status))return fail('This general is unavailable to your House.');
      if(generals.has(g.id))return fail('Each general can command only one resulting formation.');generals.add(g.id);
      if(!sizeOf(f))return fail('Move troops into this formation or remove its general.');
      const elsewhere=allArmies(s).filter(x=>x.commandId===g.commandId&&x.id!==a.id);
      // Keeping the original command preserves approved autonomous detachments.
      if(elsewhere.length&&!(i===0&&a.commandId===g.commandId)&&!draft.transfers.includes(g.id))return fail(`Confirm Transfer Command for ${g.name}.`);
    }
  }
  if(Object.keys(UNITS).some(u=>totals[u]!==a.units[u]))return fail('Every troop type must exactly match the original army.');
  const formations=draft.formations.filter(f=>sizeOf(f)>0);
  if(!formations.length||s.armies.length-1+formations.length>500)return fail('The army limit would be exceeded.');
  return {ok:true,source:a,formations};
}
export function reorganizeArmy(s,owner,draft){
  const checked=validateArmyDraft(s,owner,draft);if(!checked.ok)return checked;
  const a=checked.source,originalSize=sizeOf(a),baseline=a.commandBaseline||originalSize;
  // Validation finishes before any mutation; publication/receipts use one existing controller transaction.
  let nextId=s.nextId;const used=new Set(allArmies(s).map(x=>x.id));
  const allocate=()=>{let id;do{id=`army-${nextId++}`;}while(used.has(id));used.add(id);return id;};
  const results=checked.formations.map((f,i)=>{
    const b={...a,id:i===0?a.id:allocate(),name:f.name.trim(),units:{...empty(),...f.units},formation:f.formation,path:[],target:null,order:'hold',structureTarget:null};
    for(const key of ['commandId','commandBonus','commandMove','embarkOrder','legacyOrder','regrouping'])delete b[key];
    const g=s.commanders?.roster.find(g=>g.id===f.generalId&&g.owner===owner);
    if(g)b.commandId=g.commandId;
    b.commandBaseline=Math.max(sizeOf(b),Math.ceil(baseline*sizeOf(b)/originalSize));
    markPlayerOverride(s,b);return b;
  });
  for(const f of checked.formations){
    const g=s.commanders?.roster.find(g=>g.id===f.generalId);
    if(g&&draft.transfers.includes(g.id))for(const x of allArmies(s))if(x.id!==a.id&&x.commandId===g.commandId){delete x.commandId;markPlayerOverride(s,x);x.path=[];x.target=null;x.order=x.embarkedFleetId?'embarked':'hold';x.structureTarget=null;delete x.embarkOrder;}
  }
  s.armies=s.armies.flatMap(x=>x===a?results:[x]);s.nextId=nextId;syncCommanders(s);
  return {ok:true,armyId:results[0].id,armyIds:results.map(x=>x.id)};
}
