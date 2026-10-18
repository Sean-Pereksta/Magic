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
  function step(state,dt,actors,clear=()=>true){
    state.events=[];if(state.winner||!Number.isFinite(dt)||dt<=0)return state.events;
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
    return state.events;
  }
  function casualty(state,team){if(state.config.rule==='tickets'&&state.tickets[team]!=null)state.tickets[team]=Math.max(0,state.tickets[team]-1);}
  function spawnCandidates(state,team){const owned=connected(state,team);return [...state.sectors.filter(s=>owned.has(s.id)&&!s.contested&&!s.capturing).map(s=>({...s,forward:true})),{...state.hq[team],forward:false}];}
  function assignment(state,actor,index=0){
    const team=actor.team,connectedIds=connected(state,team),owned=state.sectors.filter(s=>connectedIds.has(s.id)),front=state.sectors.filter(s=>s.owner!==team&&canCapture(state,s,team)),threatened=owned.filter(s=>s.contested||s.capturing);
    let role='attack',target;
    if(threatened.length&&index%3!==2){role='reinforce';target=threatened[index%threatened.length];}
    else if(owned.length&&index%5===0){role='defend';target=owned.at(-1);}
    else if(owned.length&&index%5===1){role='overwatch';target=owned[index%owned.length];}
    else {role=index%3===2?'flank':'attack';target=front[index%Math.max(1,front.length)]||owned.at(-1);}
    if(!target)return {...state.hq[team],role:'defend',radius:5};
    return {x:target.x,y:target.y||1.86,z:target.z,role,radius:Math.max(2,target.radius-1),sector:target.id};
  }
  const api={create,config,connected,canCapture,controlled,rosterTarget,reinforcements,step,casualty,spawnCandidates,assignment};root.NeonConquest=api;if(typeof module==='object')module.exports=api;
})(typeof globalThis==='object'?globalThis:this);
