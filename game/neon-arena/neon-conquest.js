/* Deterministic territory simulation. Coordinates are supplied in world units. */
(function(root){
  'use strict';
  const clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
  const living=a=>a&&!a.dead&&a.active!==false;
  const config=value=>({base:clamp(Math.round(value?.base??4),1,8),perFlag:clamp(Math.round(value?.perFlag??1),0,3),botBudget:clamp(Math.round(value?.botBudget??16),0,24),waveSeconds:clamp(value?.waveSeconds??15,5,60),captureSeconds:clamp(value?.captureSeconds??8,1,60),tickets:clamp(value?.tickets??250,25,2000),scoreLimit:clamp(value?.scoreLimit??300,25,2000),rule:value?.rule==='score'?'score':'tickets'});
  function create(map,settings={}){
    const c=config(settings),sectors=map.sectors.map((s,i)=>({...s,id:s.id||String.fromCharCode(65+i),owner:s.owner===1||s.owner===2?s.owner:0,progress:0,capturing:0,contested:false,radius:s.radius||6,links:[...(s.links||[])]}));
    const ids=new Set(sectors.map(s=>s.id));
    if(ids.size!==sectors.length||sectors.some(s=>!Number.isFinite(s.x+s.z)||s.links.some(id=>!ids.has(id)&&!['cat_hq','mouse_hq'].includes(id))))throw Error('Invalid Conquest sector graph');
    return {version:1,config:c,sectors,hq:map.hq||{1:{x:-50,z:0},2:{x:50,z:0}},elapsed:0,nextWave:c.waveSeconds,tickets:{1:c.tickets,2:c.tickets},score:{1:0,2:0},winner:0,wave:0,events:[]};
  }
  function connected(state,team){
    const result=new Set(),hq=team===1?'cat_hq':'mouse_hq',queue=state.sectors.filter(s=>s.owner===team&&s.links.includes(hq));
    while(queue.length){const s=queue.shift();if(result.has(s.id))continue;result.add(s.id);for(const next of state.sectors)if(next.owner===team&&!result.has(next.id)&&(next.links.includes(s.id)||s.links.includes(next.id)))queue.push(next);}
    return result;
  }
  function canCapture(state,sector,team){const owned=connected(state,team),hq=team===1?'cat_hq':'mouse_hq';return sector.links.includes(hq)||sector.links.some(id=>owned.has(id))||state.sectors.some(s=>owned.has(s.id)&&s.links.includes(sector.id));}
  const controlled=(state,team)=>state.sectors.filter(s=>s.owner===team).length;
  const rosterTarget=(state,team)=>state.config.base+controlled(state,team)*state.config.perFlag;
  function reinforcements(state,actors){
    const live=actors.filter(living),counts={1:live.filter(a=>a.team===1).length,2:live.filter(a=>a.team===2).length},spawn={1:0,2:0};
    let budget=Math.max(0,state.config.botBudget-live.filter(a=>!a.human).length);
    // Alternate which faction is considered first so a shared cap cannot starve one side.
    const order=state.wave%2?[2,1]:[1,2];
    while(budget>0){let filled=false;for(const team of order){if(counts[team]+spawn[team]<rosterTarget(state,team)&&budget>0){spawn[team]++;budget--;filled=true;}}if(!filled)break;}
    return spawn;
  }
  const gap=(a,b)=>Math.hypot((a.x||0)-(b.x||0),(a.z||0)-(b.z||0));
  const linked=(a,b)=>a.links.includes(b.id)||b.links.includes(a.id);
  function strategy(state){
    return state.strategy||(state.strategy={teams:{},rosters:{1:[],2:[]},owners:Object.fromEntries(state.sectors.map(s=>[s.id,s.owner])),losses:{1:null,2:null}});
  }
  function planTeam(state,team,clear=()=>true){
    const command=strategy(state),old=command.teams[team],now=state.elapsed,connectedIds=connected(state,team);
    const owned=state.sectors.filter(s=>connectedIds.has(s.id)),front=state.sectors.filter(s=>s.owner!==team&&canCapture(state,s,team));
    const roster=command.rosters[team],soldiers=roster.filter(a=>!a.human),fallback=!roster.length;
    const units=fallback?Array.from({length:6},(_,i)=>({id:'slot-'+i,...state.hq[team]})):soldiers.slice().sort((a,b)=>a.id.localeCompare(b.id));
    const loss=command.losses[team],counter=loss&&loss.until>now?front.find(s=>s.id===loss.sector):null;
    const previous=front.find(s=>s.id===old?.primary);
    // Commit to a frontier until it is won, cut off, or a recently lost flag needs a counterattack.
    // Only public flag state and friendly positions enter this plan; no enemy actor is inspected.
    const rank=s=>(s.capturing===team?45+s.progress*30:0)+(s.owner===3-team?10:0)+state.sectors.filter(n=>n.owner!==team&&linked(s,n)).length*7-Math.min(...[state.hq[team],...owned].map(p=>gap(s,p)))*.3;
    const primary=counter||(previous&&(now<old.commitUntil||previous.capturing===team)?previous:front.slice().sort((a,b)=>rank(b)-rank(a)||a.id.localeCompare(b.id))[0]);
    const danger=owned.filter(s=>s.contested||s.capturing&&s.capturing!==team).sort((a,b)=>Number(b.links.includes(team===1?'cat_hq':'mouse_hq'))-Number(a.links.includes(team===1?'cat_hq':'mouse_hq'))||b.progress-a.progress);
    const defense=danger[0]||owned.slice().sort((a,b)=>{
      const border=s=>state.sectors.some(n=>n.owner!==team&&linked(s,n));
      return Number(border(b))-Number(border(a))+(primary?(gap(a,primary)-gap(b,primary))*.01:0);
    })[0];
    const signature=state.sectors.map(s=>s.id+':'+s.owner+':'+Number(s.contested)+':'+s.capturing).join('|');
    let guards=defense?(primary?(danger.length?Math.min(units.length,Math.max(2,Math.ceil(units.length*.6))):units.length>=6?2:units.length>=4?1:0):units.length):0;
    if(!danger.length&&primary&&roster.some(a=>a.human&&gap(a,defense||state.hq[team])<(defense?.radius||6)))guards=Math.max(0,guards-1);
    const guardIds=new Set(units.slice().sort((a,b)=>{
      const prior=a=>defense&&old?.orders[a.id]?.sector===defense.id&&['defend','overwatch','reinforce'].includes(old.orders[a.id].role)?12:0;
      return gap(a,defense||state.hq[team])-prior(a)-gap(b,defense||state.hq[team])+prior(b)||a.id.localeCompare(b.id);
    }).slice(0,guards).map(a=>a.id));
    const assault=units.filter(unit=>!guardIds.has(unit.id));
    const origin=primary?owned.filter(s=>s.id!==primary.id&&linked(s,primary)).sort((a,b)=>gap(a,primary)-gap(b,primary))[0]||state.hq[team]:state.hq[team];
    const occupies=primary&&assault.some(unit=>gap(unit,primary)<primary.radius&&Math.abs((unit.y||1.86)-(primary.y||1.86))<2.5&&clear(unit,primary));
    let operation=primary&&old?.primary===primary.id?old.operation:null;
    if(primary&&!operation){
      const length=gap(origin,primary)||1,advance=Math.max(0,length-Math.max(primary.radius+8,length*.35));
      const rally={x:origin.x+(primary.x-origin.x)*advance/length,y:origin.y||1.86,z:origin.z+(primary.z-origin.z)*advance/length,radius:8};
      operation={id:team+':'+primary.id+':'+now.toFixed(2),phase:!counter&&!occupies&&assault.length>=4&&length>24?'assemble':'assault',started:now,deadline:now+8,strength:units.length,rally,regrouped:false};
    }
    if(operation){
      const ready=assault.filter(unit=>gap(unit,operation.rally)<operation.rally.radius&&Math.abs((unit.y||1.86)-operation.rally.y)<2.5&&clear(unit,operation.rally)).length;
      // A rally is a short rendezvous, never a permanent gate on forward movement.
      if(operation.phase==='assemble'&&ready)operation.deadline=Math.min(operation.deadline,now+2.4);
      if(occupies||counter||now>=operation.deadline||operation.phase==='assemble'&&ready>=Math.ceil(assault.length*.6)||operation.phase==='regroup'&&units.length>=operation.strength*.75)operation.phase='assault';
      if(operation.phase==='assault'&&!counter&&!occupies&&!operation.regrouped&&operation.strength>=4&&units.length<=operation.strength*.5){
        operation.phase='regroup';operation.regrouped=true;operation.deadline=now+4.5;
      }
    }
    const plan={primary:primary?.id||null,defense:defense?.id||null,commitUntil:old&&old.primary===primary?.id?old.commitUntil:now+18,nextPlan:now+2,signature,orders:{},fallback,operation};
    let guardIndex=0,attackIndex=0;
    for(const unit of units){
      const guarding=guardIds.has(unit.id),slot=guarding?guardIndex++:attackIndex++;
      const target=guarding&&!primary&&!danger.length&&owned.length?owned[slot%owned.length]:guarding?defense:primary||defense;
      const role=guarding?(danger.length?'reinforce':slot%2?'overwatch':'defend'):primary?(slot%3===2?'flank':'attack'):slot%2?'overwatch':'defend';
      const point=target||state.hq[team],radius=target?Math.max(2,target.radius-1):5;
      const approach=target?owned.filter(s=>s.id!==target.id&&linked(s,target)).sort((a,b)=>gap(a,target)-gap(b,target))[0]||state.hq[team]:state.hq[team];
      plan.orders[unit.id]={x:point.x,y:point.y||1.86,z:point.z,role,radius,sector:target?.id,strategic:true,intent:guarding?(danger.length?'reinforce':'hold'):counter?'counterattack':primary?'capture':'hold',squad:team+':'+(target?.id||'hq'),slot,approachYaw:Math.atan2(point.x-approach.x,point.z-approach.z),commitUntil:plan.commitUntil,
        ...(!guarding&&operation?{operation:operation.id,phase:operation.phase,phaseUntil:operation.deadline,rally:{...operation.rally},fireteam:slot%2,squadSize:assault.length,leader:assault[0]?.id}: {})};
    }
    command.teams[team]=plan;return plan;
  }
  function updateStrategy(state,actors,clear){
    const command=strategy(state);
    for(const sector of state.sectors){
      const previous=command.owners[sector.id];
      if(previous&&previous!==sector.owner)command.losses[previous]={sector:sector.id,until:state.elapsed+24};
      command.owners[sector.id]=sector.owner;
    }
    for(const team of [1,2]){
      // Copy friendly scalars so a saved order cannot retain a live opponent reference.
      command.rosters[team]=actors.filter(a=>living(a)&&a.team===team).map((a,i)=>({id:String(a.entityId||a.name||'slot-'+i),x:a.x,y:a.y,z:a.z,human:!!a.human}));
      const old=command.teams[team],signature=state.sectors.map(s=>s.id+':'+s.owner+':'+Number(s.contested)+':'+s.capturing).join('|');
      if(!old||state.elapsed>=old.nextPlan||old.signature!==signature)planTeam(state,team,clear);
    }
  }
  function step(state,dt,actors,clear=()=>true){
    state.events=[];if(state.winner||!Number.isFinite(dt)||dt<=0)return state.events;
    strategy(state);
    dt=Math.min(dt,1);state.elapsed+=dt;state.nextWave-=dt;
    for(const s of state.sectors){
      const inside=actors.filter(a=>living(a)&&!(a.spawnGrace>0)&&Math.hypot(a.x-s.x,a.z-s.z)<=s.radius&&Math.abs((a.y||1.86)-(s.y||1.86))<2.5&&clear(a,s));
      const counts={1:inside.filter(a=>a.team===1).length,2:inside.filter(a=>a.team===2).length};s.contested=counts[1]>0&&counts[2]>0;
      if(s.contested)continue;
      const team=counts[1]?1:counts[2]?2:0;
      if(!team||team===s.owner||!canCapture(state,s,team)){s.progress=Math.max(0,s.progress-dt*.14);if(!s.progress)s.capturing=0;continue;}
      if(s.capturing&&s.capturing!==team){s.progress=Math.max(0,s.progress-dt/state.config.captureSeconds);if(!s.progress)s.capturing=team;continue;}
      s.capturing=team;s.progress+=dt/state.config.captureSeconds*Math.min(2,1+(counts[team]-1)*.25);
      if(s.progress>=1){const previous=s.owner;s.owner=previous?0:team;s.progress=0;s.capturing=0;state.events.push({type:previous?'neutralized':'captured',sector:s.id,team});}
    }
    const one=controlled(state,1),two=controlled(state,2),majority=Math.floor(state.sectors.length/2)+1;
    if(state.config.rule==='tickets'){if(one>=majority)state.tickets[2]=Math.max(0,state.tickets[2]-dt*(one-two)*.35);if(two>=majority)state.tickets[1]=Math.max(0,state.tickets[1]-dt*(two-one)*.35);}
    else {state.score[1]+=one*dt;state.score[2]+=two*dt;}
    if(state.config.rule==='tickets'&&(state.tickets[1]<=0||state.tickets[2]<=0))state.winner=state.tickets[1]===state.tickets[2]?3:state.tickets[1]>state.tickets[2]?1:2;
    if(state.config.rule==='score'&&(state.score[1]>=state.config.scoreLimit||state.score[2]>=state.config.scoreLimit))state.winner=state.score[1]===state.score[2]?3:state.score[1]>state.score[2]?1:2;
    if(state.nextWave<=0&&!state.winner){state.wave++;state.nextWave+=state.config.waveSeconds;state.events.push({type:'reinforcements',counts:reinforcements(state,actors)});}
    updateStrategy(state,actors,clear);
    return state.events;
  }
  function casualty(state,team){if(state.config.rule==='tickets'&&state.tickets[team]!=null)state.tickets[team]=Math.max(0,state.tickets[team]-1);}
  function spawnCandidates(state,team){const owned=connected(state,team);return [...state.sectors.filter(s=>owned.has(s.id)&&!s.contested&&!s.capturing).map(s=>({...s,forward:true})),{...state.hq[team],forward:false}];}
  function assignment(state,actor,index=0){
    const team=actor.team,command=strategy(state);
    let plan=command.teams[team];
    if(!plan||state.elapsed>=plan.nextPlan)plan=planTeam(state,team);
    const id=String(actor.entityId||actor.name||'slot-'+index);
    if(plan.orders[id])return {...plan.orders[id]};
    if(plan.fallback&&plan.orders['slot-'+index%6])return {...plan.orders['slot-'+index%6]};
    // A replacement joining between planning ticks immediately joins the committed assault.
    const attack=Object.values(plan.orders).find(order=>order.intent==='counterattack'||order.intent==='capture')||Object.values(plan.orders)[0];
    return attack?{...attack,slot:index,role:attack.intent==='hold'?'defend':'reinforce'}:{...state.hq[team],role:'defend',radius:5,strategic:true};
  }
  const api={create,config,connected,canCapture,controlled,rosterTarget,reinforcements,step,casualty,spawnCandidates,assignment};root.NeonConquest=api;if(typeof module==='object')module.exports=api;
})(typeof globalThis==='object'?globalThis:this);
