/* Exhibition budgets use the same athlete ratings that enter the court. */
(function(root,factory){
  const api=factory();
  if(typeof module==='object'&&module.exports) module.exports=api;
  else root.BasketballCustomRosters=api;
})(typeof globalThis!=='undefined'?globalThis:this,function(){
  'use strict';
  const SAVE_KEY='basketball2CustomRosterV1',DEFAULT_CAP=250;
  const QUICK_PLAY_TIERS=Object.freeze([60,70,80,90]);
  const QUICK_PLAY_ROLES=Object.freeze([
    ['playmaker','Playmaker','playmaker'],['sharpshooter','Shooter','shooter'],
    ['midrange','Shot Creator','primary'],['slasher','Slasher','slasher'],
    ['interior','Power Big','anchor'],['defender','Defender','stopper'],
    ['rimProtector','Rim Protector','anchor'],['stretchBig','Stretch Big','shooter'],
    ['allRounder','All-Rounder','balanced']
  ].map(([id,label,runtimeRole])=>Object.freeze({id,label,runtimeRole})));
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
  function quickPlayCatalog(profiles,riseAPI){
    if(!riseAPI?.generateMarket||!riseAPI?.overall||!riseAPI?.toProfile)return [];
    const templates=riseAPI.generateMarket(2407,108),entries=[];
    for(const role of QUICK_PLAY_ROLES){
      const variants=templates.filter(athlete=>athlete.archetype===role.id);
      for(let tierIndex=0;tierIndex<QUICK_PLAY_TIERS.length;tierIndex++)for(let index=0;index<3;index++){
        const target=QUICK_PLAY_TIERS[tierIndex],template=variants[tierIndex*3+index],variant=index+1,id=`quick:${role.id}:${target}:${variant}`;
        const athlete={...template,id,role:role.runtimeRole,abilities:{},ratings:{}};
        // Market veterans can share capped ratings. Small deterministic skill
        // differences keep the three exhibition variants distinct at every tier.
        const baseline=Object.fromEntries(riseAPI.ATTRIBUTE_KEYS.map((key,statIndex)=>
          [key,template.ratings[key]+(statIndex*7+index*11+tierIndex*3)%5-2]));
        // Shift the whole skill curve, keeping the specialist strengths and
        // weaknesses. A one-point shift changes weighted OVR by at most one.
        let low=-99,high=99;
        const setRatings=offset=>{
          for(const key of riseAPI.ATTRIBUTE_KEYS)athlete.ratings[key]=clamp(baseline[key]+offset,25,99);
        };
        while(low<high){
          const middle=Math.floor((low+high)/2);setRatings(middle);
          if(riseAPI.overall(athlete)<target)low=middle+1;else high=middle;
        }
        setRatings(low);
        const definition=riseAPI.ARCHETYPES[role.id],rank=target>=90?3:target>=80?2:1;
        // Only award skills whose real rating requirements are met. Resolve
        // prerequisite skills first instead of attaching decorative badges.
        for(let pass=0;pass<3;pass++)for(const key of definition.abilities){
          const ability=riseAPI.ABILITIES[key];
          if(Object.entries(ability.requirements||{}).every(([stat,value])=>athlete.ratings[stat]>=value)&&
            (!ability.requires||athlete.abilities[ability.requires])&&
            (!ability.requiresAny||ability.requiresAny.some(required=>athlete.abilities[required])))athlete.abilities[key]=rank;
        }
        const converted=riseAPI.toProfile(athlete);
        delete converted.campaignPlayerId;
        const profile={...(profiles[athlete.profileKey]||{}),...converted,quickPlayId:id};
        delete profile.campaignPlayerId;
        entries.push({id,name:athlete.name,overall:riseAPI.overall(athlete),source:'Quick Play',
          identity:role.id,role:role.id,roleLabel:role.label,archetype:role.id,tier:target,variant,profile});
      }
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
  function uniqueEntries(entries){
    const seen=new Set();
    return entries.filter(entry=>{
      if(!entry||typeof entry.id!=='string'||!entry.id||seen.has(entry.id)||
        !Number.isInteger(entry.overall)||entry.overall<1||entry.overall>297)return false;
      seen.add(entry.id);return true;
    });
  }
  function clearActive(draft,size){
    const result=normalize(draft);
    for(const team of result.teams)for(let slot=0;slot<size;slot++)team[slot]='';
    return result;
  }
  function completeTeams(base,size,entries,maximize){
    const fixed=new Set(base.teams.flatMap(ids=>ids.slice(0,size)).filter(Boolean));
    const needed=base.teams.map(ids=>ids.slice(0,size).filter(id=>!id).length);
    const remaining=base.teams.map(ids=>base.cap-total(ids.slice(0,size),entries));
    if(remaining.some(cap=>cap<0))return null;
    if(!needed[0]&&!needed[1])return normalize(base);
    // At most six athletes can be selected. Extra athletes at an identical
    // OVR add identities, but cannot add any reachable sums. Input ordering
    // chooses the representatives (shuffled ordering for a random roll).
    const multiplicity=new Map(),missing=needed[0]+needed[1];
    const available=entries.filter(entry=>{
      if(fixed.has(entry.id))return false;
      const count=multiplicity.get(entry.overall)||0;
      multiplicity.set(entry.overall,count+1);return count<missing;
    });
    if(available.length<missing)return null;
    const sorted=available.map(entry=>entry.overall).sort((a,b)=>a-b);
    if(sorted.slice(0,missing).reduce((sum,value)=>sum+value,0)>remaining[0]+remaining[1]||
      needed.some((count,team)=>sorted.slice(0,count).reduce((sum,value)=>sum+value,0)>remaining[team]))return null;
    const maxWeight=sorted[sorted.length-1],minWeight=sorted[0];
    const caps=remaining.map((cap,team)=>Math.min(cap,needed[team]*maxWeight));
    const columns=caps[1]+1,words=(columns+31)>>>5,rows=caps[0]+1,groups=(needed[0]+1)*(needed[1]+1);
    const group=(blue,coral)=>blue*(needed[1]+1)+coral;
    const bits=Array.from({length:groups},()=>new Uint32Array(rows*words));
    // One signed item index per newly reached state is enough to reconstruct.
    // Descending slot counts guarantee its predecessor used earlier items.
    const parents=Array.from({length:groups},()=>new Int32Array(rows*columns));
    bits[0][0]=1;
    const lastMask=columns%32?0xffffffff>>>(32-columns%32):0xffffffff;
    function add(target,row,word,value,parent){
      if(word===words-1)value&=lastMask;
      const address=row*words+word,fresh=(value&~bits[target][address])>>>0;
      if(!fresh)return;
      bits[target][address]|=fresh;
      let pending=fresh;
      while(pending){
        const bit=31-Math.clz32(pending&-pending);
        parents[target][row*columns+word*32+bit]=parent;
        pending=(pending&(pending-1))>>>0;
      }
    }
    const finalGroup=group(...needed);
    let chosen=null;
    for(let index=0;index<available.length;index++){
      const weight=available[index].overall,wordShift=weight>>>5,bitShift=weight%32;
      for(let blue=needed[0];blue>=0;blue--)for(let coral=needed[1];coral>=0;coral--){
        if(!blue&&!coral||blue+coral>index+1)continue;
        const target=group(blue,coral);
        const minRow=blue*minWeight,maxRow=Math.min(caps[0],blue*maxWeight);
        if(blue){
          const source=bits[group(blue-1,coral)];
          for(let row=Math.max(minRow,weight);row<=maxRow;row++)for(let word=0;word<words;word++){
            const value=source[(row-weight)*words+word];
            if(value)add(target,row,word,value,index+1);
          }
        }
        if(coral&&wordShift<words){
          const source=bits[group(blue,coral-1)];
          for(let row=minRow;row<=maxRow;row++)for(let word=wordShift;word<words;word++){
            let value=source[row*words+word-wordShift]<<bitShift;
            if(bitShift&&word>wordShift)value|=source[row*words+word-wordShift-1]>>>(32-bitShift);
            if(value)add(target,row,word,value,-(index+1));
          }
        }
      }
      const final=bits[finalGroup];
      if(final[caps[0]*words+(caps[1]>>>5)]&(1<<(caps[1]%32))){chosen=caps.slice();break;}
      if(!maximize){
        const address=final.findIndex(value=>value!==0);
        if(address>=0){chosen=[Math.floor(address/words),(address%words)*32+31-Math.clz32(final[address]&-final[address])];break;}
      }
    }
    if(!chosen){
      // When both caps cannot be reached, maximize combined OVR, then prefer
      // the more balanced split. All candidates still obey each team's cap.
      let best=-1,balance=-1;
      for(let blue=0;blue<=caps[0];blue++)for(let coral=0;coral<=caps[1];coral++){
        if(!(bits[finalGroup][blue*words+(coral>>>5)]&(1<<(coral%32))))continue;
        const sum=blue+coral,even=Math.min(blue,coral);
        if(sum>best||sum===best&&even>balance){best=sum;balance=even;chosen=[blue,coral];}
      }
    }
    if(!chosen)return null;
    const selected=[[],[]],counts=needed.slice(),sums=chosen.slice();
    while(counts[0]||counts[1]){
      const parent=parents[group(...counts)][sums[0]*columns+sums[1]],team=parent>0?0:1;
      const entry=available[Math.abs(parent)-1];
      selected[team].push(entry.id);counts[team]--;sums[team]-=entry.overall;
    }
    const result=normalize(base);
    for(let team=0;team<2;team++){
      selected[team].reverse();
      for(let slot=0;slot<size;slot++)if(!result.teams[team][slot])result.teams[team][slot]=selected[team].shift();
    }
    return result;
  }
  function fill(draft,size,entries,preferred=[]){
    const result=normalize(draft),pool=uniqueEntries(entries),used=new Set(),byId=new Map(pool.map(entry=>[entry.id,entry]));
    if(![1,2,3].includes(size))return result;
    for(let team=0;team<2;team++)for(let slot=0;slot<size;slot++){
      const id=result.teams[team][slot];
      if(!byId.has(id)||used.has(id))result.teams[team][slot]='';else used.add(id);
    }
    function complete(base){
      const tried=new Set();
      for(const mask of [3,1,2,0]){
        const candidate=normalize(base),taken=new Set(candidate.teams.flatMap(ids=>ids.slice(0,size)).filter(Boolean));
        for(let team=0;team<2;team++){
          const id=preferred[team];
          if(mask&(1<<team)&&!candidate.teams[team][0]&&byId.has(id)&&!taken.has(id)){
            candidate.teams[team][0]=id;taken.add(id);
          }
        }
        const key=JSON.stringify(candidate.teams);
        if(tried.has(key))continue;tried.add(key);
        const filled=completeTeams(candidate,size,pool,false);if(filled)return filled;
      }
      return null;
    }
    return complete(result)||complete(clearActive(result,size))||result;
  }
  function rollRandom(draft,size,entries,rng=Math.random){
    if(![1,2,3].includes(size))return normalize(draft);
    const result=clearActive(draft,size),pool=uniqueEntries(entries);
    for(let index=pool.length-1;index>0;index--){
      const chosen=Math.min(index,Math.floor(Math.max(0,Number(rng())||0)*(index+1)));
      [pool[index],pool[chosen]]=[pool[chosen],pool[index]];
    }
    return completeTeams(result,size,pool,true)||result;
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
  return Object.freeze({SAVE_KEY,DEFAULT_CAP,QUICK_PLAY_TIERS,QUICK_PLAY_ROLES,legacyOverall,catalog,quickPlayCatalog,normalize,total,validate,fill,rollRandom,snapshot});
});
