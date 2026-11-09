/* Reproducible headless playtest. Bots use the same legal commands as humans. */
import {pathToFileURL} from 'node:url';
import {resolve} from 'node:path';

const args=Object.fromEntries(process.argv.slice(2).map(a=>a.replace(/^--/,'').split('=')));
const E=await import(args.engine?pathToFileURL(resolve(args.engine)).href:'./engine.mjs');
const swap=side=>side==='player'?'ai':side==='ai'?'player':side;
const point=(s,p)=>({...p,x:s.width-1-p.x,y:s.height-1-p.y});
const alive=(s,side)=>s.units.filter(u=>u.hp>0&&u.side===side);
const rosters={balanced:['leader','scout','scout','brute','mystic'],swift:['leader',...Array(6).fill('scout')],bulwark:['leader','brute','brute','brute'],coven:['leader','mystic','mystic','mystic','mystic']};
const decisionTimes=[];

function commandFor(s,side,recruitAt){
  const view=structuredClone(s);view.opponentMode='ai';
  if(side==='player'){
    view.units=view.units.map(u=>({...point(s,u),side:swap(u.side)}));
    view.tiles=view.tiles.map(t=>point(s,t)).sort((a,b)=>a.y-b.y||a.x-b.x);
    view.flags=view.flags.map(f=>({...point(s,f),owner:swap(f.owner)}));
    view.groundEffects=(view.groundEffects||[]).map(effect=>({...point(s,effect),side:swap(effect.side)}));
    view.telegraphs=(view.telegraphs||[]).map(mark=>({...mark,side:swap(mark.side),x:s.width-1-mark.x,y:s.height-1-mark.y,fromX:s.width-1-mark.fromX,fromY:s.height-1-mark.fromY}));
    for(const name of ['factions','scores','supplies','armies','orders'])if(s[name])view[name]={player:s[name].ai,ai:s[name].player};
  }
  view.aiRecruitAt=recruitAt;
  const before=performance.now();
  const action=E.chooseAIAction(view);
  decisionTimes.push(performance.now()-before);
  s.rng=view.rng;
  if(action&&side==='player'){
    if(action.type==='recruit')action.side='player';
    else Object.assign(action,point(s,action));
  }
  return {action,recruitAt:view.aiRecruitAt};
}

function runCase({mode,playerFaction,aiFaction,map,seed,roster}){
  const s=E.createGame({mode,playerFaction,aiFaction,map,seed,playerArmy:rosters[roster],aiArmy:rosters[roster],opponentMode:'local',difficulty:'normal'});
  const next={player:0,ai:0},recruit={player:0,ai:0};
  const result={mode,playerFaction,aiFaction,map,seed,roster,actions:0,moves:0,attacks:0,specials:0,guards:0,emptyGuards:0,netZoneLosses:0,damage:0,healing:0,captures:0,firstDamage:null,firstCapture:null};
  const note=events=>{for(const e of events){if(e.type==='damage'){result.damage+=e.value;if(result.firstDamage===null)result.firstDamage=s.time;}if(e.type==='heal')result.healing+=e.value;if(e.type==='capture'){result.captures++;if(result.firstCapture===null)result.firstCapture=s.time;}}};
  while(!s.winner&&s.time<s.duration){
    for(const side of seed%2?['player','ai']:['ai','player'])if(!s.winner&&s.time+1e-6>=next[side]){
      const choice=commandFor(s,side,recruit[side]);recruit[side]=choice.recruitAt;next[side]=s.time+1.8;
      if(choice.action){
        const heldBefore=s.flags.filter(f=>f.owner===side).length;
        const response=E.applyAction(s,choice.action);if(!response.ok)throw Error(`Illegal playtest action ${JSON.stringify(choice.action)}`);
        if(mode==='control'&&choice.action.type==='move')result.netZoneLosses+=Math.max(0,heldBefore-s.flags.filter(f=>f.owner===side).length);
        result.actions++;if(choice.action.type==='move')result.moves++;if(choice.action.type==='attack')result.attacks++;if(choice.action.type==='ability')result.specials++;
        if(choice.action.type==='guard'){result.guards++;if(!response.events.some(e=>e.type==='brace'||e.type==='shield'))result.emptyGuards++;}
        note(response.events);
      }
    }
    if(!s.winner)note(E.advance(s,.2));
  }
  if(!s.winner)throw Error(`Unresolved match: ${mode}/${playerFaction}/${aiFaction}/${seed}`);
  return {...result,time:s.time,winner:s.winner,timeout:s.time>=s.duration,scores:s.scores,remaining:{player:alive(s,'player').length,ai:alive(s,'ai').length}};
}

const factions=Object.keys(E.FACTIONS),maps=Object.keys(E.MAPS),seeds=Math.max(1,Number(args.seeds)||2),rows=[];
for(const mode of Object.keys(E.MODES))for(let p=0;p<factions.length;p++)for(let a=0;a<factions.length;a++)if(p!==a)for(let seed=1;seed<=seeds;seed++){
  const pair=p*2+a,roster=Object.keys(rosters)[(pair+seed)%4];
  rows.push(runCase({mode,playerFaction:factions[p],aiFaction:factions[a],map:maps[(pair+seed)%maps.length],seed,roster}));
}
const mean=(items,key)=>Math.round(items.reduce((n,r)=>n+r[key],0)/items.length*10)/10;
const summary=Object.fromEntries(Object.keys(E.MODES).map(mode=>{const cases=rows.filter(r=>r.mode===mode);return[mode,{matches:cases.length,time:mean(cases,'time'),timeouts:cases.filter(r=>r.timeout).length,draws:cases.filter(r=>r.winner==='draw').length,damage:mean(cases,'damage'),captures:mean(cases,'captures'),actions:mean(cases,'actions'),guards:mean(cases,'guards'),emptyGuards:mean(cases,'emptyGuards'),netZoneLosses:mean(cases,'netZoneLosses'),moves:mean(cases,'moves'),specials:mean(cases,'specials')}];}));
decisionTimes.sort((a,b)=>a-b);
const decisionMs=Object.fromEntries([['median',.5],['p95',.95],['p99',.99]].map(([name,q])=>[name,Math.round(decisionTimes[Math.floor((decisionTimes.length-1)*q)]*100)/100]));
process.stdout.write(JSON.stringify({description:`${rows.length} deterministic equal-cadence (1.8 seconds per side) bot matches across faction pairs, maps, modes, and four roster styles. Automated results diagnose rules/AI; they do not measure human enjoyment. Decision timing measures this Node host, not browser rendering.`,summary,decisionMs,rows},null,2)+'\n');
