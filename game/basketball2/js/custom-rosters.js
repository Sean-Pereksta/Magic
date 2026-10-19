/* Exhibition budgets use the same athlete ratings that enter the court. */
(function(root,factory){
  const api=factory();
  if(typeof module==='object'&&module.exports) module.exports=api;
  else root.BasketballCustomRosters=api;
})(typeof globalThis!=='undefined'?globalThis:this,function(){
  'use strict';
  const SAVE_KEY='basketball2CustomRosterV1',DEFAULT_CAP=250;
  const clamp=(value,min,max)=>Math.max(min,Math.min(max,value));
  function legacyOverall(profile){
    // Legacy modifiers use 1 as an average player. Include all live skills,
    // then give the strongest four the same specialist weight as campaign OVR.
    const ratings=['speed','sprint','jump','layup','handle','shot','block','steal','hook','passing']
      .map(key=>clamp(60+((Number(profile?.[key])||1)-1)*90,25,95));
    const average=values=>values.reduce((sum,n)=>sum+n,0)/values.length;
    return Math.round(average(ratings)*.62+average(ratings.slice().sort((a,b)=>b-a).slice(0,4))*.38);
  }
  function catalog(profiles,riseAPI,state){
    const entries=Object.entries(profiles).map(([key,profile])=>({id:`classic:${key}`,name:profile.name,
      overall:legacyOverall(profile),source:'Classic',identity:profile.archetype||'all_rounder',profile:{...profile,profileKey:key}}));
    if(state&&riseAPI) for(const athlete of riseAPI.roster(state)){
      const overall=riseAPI.overall(athlete);
      entries.push({id:`rise:${athlete.id}`,name:athlete.name,overall,source:state.team.name,
        identity:athlete.role==='balanced'?athlete.archetype:athlete.role,
        profile:{...(profiles[athlete.profileKey]||{}),...riseAPI.toProfile(athlete,state.team)}});
    }
    return entries;
  }
  function normalize(raw){
    const cap=Number(raw?.cap);
    return {enabled:raw?.enabled===true,cap:Number.isInteger(cap)&&cap>=25&&cap<=297?cap:DEFAULT_CAP,
      teams:[0,1].map(team=>Array.from({length:3},(_,slot)=>typeof raw?.teams?.[team]?.[slot]==='string'?raw.teams[team][slot].slice(0,100):''))};
  }
  function total(ids,entries){
    const byId=new Map(entries.map(entry=>[entry.id,entry]));
    return ids.reduce((sum,id)=>sum+(byId.get(id)?.overall||0),0);
  }
  function validate(draft,size,entries){
    const byId=new Map(entries.map(entry=>[entry.id,entry])),errors=[],used=new Set();
    if(![1,2,3].includes(size)) return {ok:false,errors:['Choose 1, 2 or 3 players per team.'],totals:[0,0]};
    if(!Number.isInteger(draft?.cap)||draft.cap<25||draft.cap>297) errors.push('Set a whole-number team cap from 25 to 297 OVR.');
    const totals=[0,0];
    for(let team=0;team<2;team++){
      const ids=Array.isArray(draft?.teams?.[team])?draft.teams[team].slice(0,size):[];
      if(ids.length!==size||ids.some(id=>!byId.has(id))) errors.push(`${team===0?'Blue':'Coral'} needs ${size} selected athlete${size===1?'':'s'}.`);
      for(const id of ids){
        if(!id||!byId.has(id)) continue;
        if(used.has(id)) errors.push('Each athlete can appear only once in this matchup.');
        used.add(id);totals[team]+=byId.get(id).overall;
      }
      if(totals[team]>draft.cap) errors.push(`${team===0?'Blue':'Coral'} is ${totals[team]-draft.cap} OVR over the team cap.`);
    }
    return {ok:errors.length===0,errors:[...new Set(errors)],totals};
  }
  function fill(draft,size,entries,preferred=[]){
    const result=normalize(draft),used=new Set(),byId=new Map(entries.map(entry=>[entry.id,entry]));
    if(![1,2,3].includes(size))return result;
    for(let team=0;team<2;team++) for(let slot=0;slot<size;slot++){
      const id=result.teams[team][slot];
      if(!byId.has(id)||used.has(id)) result.teams[team][slot]='';
      else used.add(id);
    }
    function complete(base){
      const fixed=new Set(base.teams.flatMap(ids=>ids.slice(0,size)).filter(Boolean));
      const available=entries.map((entry,index)=>({...entry,index})).filter(entry=>!fixed.has(entry.id));
      const missing=2*size-fixed.size,fixedTotal=total([...fixed],entries);
      if(available.length<missing||fixedTotal+available.map(entry=>entry.overall).sort((a,b)=>a-b).slice(0,missing).reduce((sum,n)=>sum+n,0)>2*base.cap)return null;
      const options=[0,1].map(team=>{
        const ids=base.teams[team].slice(0,size),needed=ids.filter(id=>!id).length,current=total(ids,entries),choices=[];
        function combinations(start,chosen,sum){
          if(chosen.length===needed){
            if(current+sum>base.cap)return;
            const lineup=ids.slice(),remaining=chosen.map(entry=>entry.id);
            if(!lineup[0]&&remaining.includes(preferred[team])){lineup[0]=preferred[team];remaining.splice(remaining.indexOf(preferred[team]),1);}
            for(let slot=0;slot<size;slot++)if(!lineup[slot])lineup[slot]=remaining.shift();
            choices.push({lineup,preferred:lineup[0]===preferred[team]?1:0,indices:chosen.map(entry=>entry.index),mask:chosen.reduce((mask,entry)=>mask|(1<<entry.index),0)});
            return;
          }
          for(let index=start;index<=available.length-(needed-chosen.length);index++){
            const entry=available[index];if(current+sum+entry.overall>base.cap)continue;
            combinations(index+1,[...chosen,entry],sum+entry.overall);
          }
        }
        combinations(0,[],0);return choices;
      });
      // At most 20 live athletes and three slots per side: compare team combinations,
      // not six-slot permutations. Prefer both requested captains when they fit.
      const maxPreference=options.reduce((sum,choices)=>sum+Math.max(0,...choices.map(choice=>choice.preferred)),0);
      for(let preference=maxPreference;preference>=0;preference--)for(const blue of options[0])for(const coral of options[1]){
        if(blue.preferred+coral.preferred!==preference)continue;
        const overlap=entries.length<=30?(blue.mask&coral.mask)!==0:blue.indices.some(index=>coral.indices.includes(index));
        if(overlap)continue;
        const completed=normalize(base);completed.teams[0].splice(0,size,...blue.lineup);completed.teams[1].splice(0,size,...coral.lineup);return completed;
      }
      return null;
    }
    const preserved=complete(result);if(preserved)return preserved;
    const fresh=normalize(result);for(let team=0;team<2;team++)for(let slot=0;slot<size;slot++)fresh.teams[team][slot]='';
    return complete(fresh)||result;
  }
  function snapshot(draft,size,entries){
    const validation=validate(draft,size,entries);
    if(!validation.ok) return null;
    const byId=new Map(entries.map(entry=>[entry.id,entry]));
    return {cap:draft.cap,size,totals:validation.totals,teams:draft.teams.map(ids=>ids.slice(0,size).map(id=>{
      const entry=byId.get(id);
      return {...JSON.parse(JSON.stringify(entry.profile)),customRosterId:entry.id,customOverall:entry.overall};
    }))};
  }
  return Object.freeze({SAVE_KEY,DEFAULT_CAP,legacyOverall,catalog,normalize,total,validate,fill,snapshot});
});
