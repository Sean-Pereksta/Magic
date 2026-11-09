/* Deterministic tactical-roster exercise; both bots use the same command rules. */
import {FACTIONS,MODES,DRAFT_ROLES,createGame,chooseAIAction,applyAction,advance,unitDef,formationStatus} from './engine.mjs';
const swap=side=>side==='player'?'ai':side==='ai'?'player':side;
const timings=[],skillUses={},groundUses={},roleUses={},formationOrders={bloom:0,chaos:0,choir:0};
function choose(s,side){
  const view=structuredClone(s);view.opponentMode='ai';
  if(side==='player'){
    const point=p=>({...p,x:s.width-1-p.x,y:s.height-1-p.y});
    view.units=view.units.map(u=>({...point(u),side:swap(u.side)}));view.tiles=view.tiles.map(point).sort((a,b)=>a.y-b.y||a.x-b.x);
    view.flags=view.flags.map(f=>({...point(f),owner:swap(f.owner)}));view.groundEffects=view.groundEffects.map(e=>({...point(e),side:swap(e.side)}));
    view.telegraphs=(view.telegraphs||[]).map(mark=>({...mark,side:swap(mark.side),x:s.width-1-mark.x,y:s.height-1-mark.y,fromX:s.width-1-mark.fromX,fromY:s.height-1-mark.fromY}));
    for(const key of ['factions','scores','supplies','armies','orders'])view[key]={player:s[key].ai,ai:s[key].player};
  }
  const started=performance.now(),action=chooseAIAction(view);timings.push(performance.now()-started);s.rng=view.rng;
  if(action&&side==='player'){if(action.type==='recruit')action.side='player';else {action.x=s.width-1-action.x;action.y=s.height-1-action.y;}}
  return action;
}
function run(mode,p,a,seed){
  const s=createGame({mode,playerFaction:p,aiFaction:a,seed,startingMoney:40,playerArmy:DRAFT_ROLES,aiArmy:DRAFT_ROLES,opponentMode:'local',difficulty:'normal',terrain:seed%2?'wild':'balanced',map:['glade','fracture','sanctuary'][seed%3]});
  const next={player:0,ai:0};let commands=0,hits=0,heals=0,maxFields=0;
  while(!s.winner&&s.time<s.duration){
    for(const side of seed%2?['player','ai']:['ai','player'])if(!s.winner&&s.time>=next[side]){
      next[side]=s.time+1.8;const action=choose(s,side);if(!action)continue;
      const unit=s.units.find(u=>u.id===action.unitId);if(unit){roleUses[unit.role]=(roleUses[unit.role]||0)+1;if(formationStatus(s,unit).active)formationOrders[unit.faction]++;if(action.type==='ability'){const kind=unitDef(unit).abilities[action.skillIndex||0].kind;skillUses[kind]=(skillUses[kind]||0)+1;}}
      const result=applyAction(s,action);if(!result.ok)throw Error(`Illegal tactical order: ${JSON.stringify(action)}`);commands++;
      for(const event of result.events)if(event.type==='ground')groundUses[event.kind]=(groundUses[event.kind]||0)+1;
    }
    const events=advance(s,.2);for(const event of events){if(event.type==='damage')hits+=event.value;if(event.type==='heal')heals+=event.value;}
    maxFields=Math.max(maxFields,s.groundEffects.length);
  }
  if(!s.winner)throw Error('Tactical match did not resolve');
  return {mode,player:p,opponent:a,seed,winner:s.winner,time:s.time,commands,periodicDamage:hits,periodicHealing:heals,maxFields};
}
const rows=[],factions=Object.keys(FACTIONS);
for(const mode of Object.keys(MODES))for(let p=0;p<factions.length;p++)for(let a=0;a<factions.length;a++)if(p!==a)rows.push(run(mode,factions[p],factions[a],p*3+a+1));
timings.sort((a,b)=>a-b);const percentile=q=>Math.round(timings[Math.floor((timings.length-1)*q)]*100)/100;
console.log(JSON.stringify({description:'24 deterministic matched-cadence bot battles; each side fields all seven faction roles at40coins. Diagnostics exercise legality, fields and formations, not human enjoyment or overall balance.',matches:rows.length,finishedBeforeLimit:rows.filter(r=>r.time<180).length,skillUses,groundUses,roleUses,formationOrders,decisionMs:{median:percentile(.5),p95:percentile(.95)},rows},null,2));
