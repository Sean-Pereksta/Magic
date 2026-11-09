/* Expedition supplies live outside the relic pack. All timing and finds use
   the road simulation; opening the satchel never advances combat. */
(() => {
  'use strict';
  const ids=['moss_tonic','bark_ward','ember_bomb','frost_bell','quicksilver'];
  const definitions=Object.freeze({
    moss_tonic:Object.freeze({id:'moss_tonic',name:'Moss Tonic',element:'leaf',role:'Recover',description:'Restore 28% maximum HP and cleanse every ailment, including venom.'}),
    bark_ward:Object.freeze({id:'bark_ward',name:'Bark Ward',element:'stone',role:'Protect',description:'Gain 24% maximum HP as shield, up to the 40% shield cap.'}),
    ember_bomb:Object.freeze({id:'ember_bomb',name:'Ember Bomb',element:'fire',role:'Burst',description:'Blast every living foe. Damage grows with the round; each blast is capped at 60% of that foe’s maximum HP.'}),
    frost_bell:Object.freeze({id:'frost_bell',name:'Frost Bell',element:'frost',role:'Control',description:'Freeze every living foe for 1.8 seconds, interrupting their attack charge.'}),
    quicksilver:Object.freeze({id:'quicksilver',name:'Quicksilver',element:'storm',role:'Recharge',description:'Advance every equipped relic by 1.6 seconds and automatic abilities by 2.4 seconds.'})
  });
  const rank=(r,id)=>r?.legacy?.ranks?.[id]||0;
  const capacity=r=>6+2*rank(r,'provision_pockets');
  const quality=r=>1+.2*rank(r,'field_alchemy');
  const empty=()=>({version:1,counts:Object.fromEntries(ids.map(id=>[id,0])),cooldown:0,dryWaves:0,found:0,used:0,lastFound:null});
  const count=r=>ids.reduce((sum,id)=>sum+(r?.provisions?.counts?.[id]||0),0);
  function start(r){
    r.provisions=empty();
    const supplies=['moss_tonic','bark_ward','ember_bomb','frost_bell','quicksilver','moss_tonic'];
    for(const id of supplies.slice(0,2+rank(r,'provision_pockets')))r.provisions.counts[id]++;
    return r.provisions;
  }
  function tick(r,dt){if(r.provisions)r.provisions.cooldown=Math.max(0,r.provisions.cooldown-dt);}
  const foes=r=>(r?.enemies||[]).filter(e=>e.hp>0);
  function reason(r,id){
    if(!definitions[id])return 'Choose a supply.';
    if(!r||!['walk','fight'].includes(r.phase))return 'Begin or continue an expedition first.';
    if(r.draft?.length)return 'Choose your talent first.';
    if(!(r.provisions?.counts[id]>0))return 'Find more on the road.';
    if(r.provisions.cooldown>0)return 'Supplies are recharging. Close the satchel to continue the road.';
    if(id==='moss_tonic'&&r.hp>=r.maxHp&&!(r.venom>0)&&!Object.values(r.ailments||{}).some(v=>v>0))return 'Health is full and no ailments need cleansing.';
    if(id==='bark_ward'&&r.shield>=r.maxHp*.4-1e-8)return 'Your shield is already full.';
    if(['ember_bomb','frost_bell'].includes(id)&&(!foes(r).length||r.phase!=='fight'))return 'Use this when enemies are on the trail.';
    if(id==='frost_bell'&&foes(r).every(e=>(e.freeze||0)>=1.8*quality(r)))return 'Every foe is already frozen for longer.';
    if(id==='quicksilver'&&!r.active.some(it=>it.cd>0)&&!Object.values(r.abilityTimers||{}).some(v=>v>0))return 'Every relic and ability is ready.';
    return '';
  }
  const canUse=(r,id)=>!reason(r,id);
  function describe(r,id){
    const q=quality(r),pct=n=>Math.round(n*q)+'%';
    if(id==='moss_tonic')return `Restore ${pct(28)} maximum HP and cleanse every ailment, including venom.`;
    if(id==='bark_ward')return `Gain ${pct(24)} maximum HP as shield, up to the 40% shield cap.`;
    if(id==='frost_bell')return `Freeze every living foe for ${(1.8*q).toFixed(1)} seconds and interrupt their attack charge.`;
    if(id==='quicksilver')return `Advance all relics by ${(1.6*q).toFixed(1)} seconds and automatic abilities by ${(2.4*q).toFixed(1)} seconds.`;
    return definitions[id]?.description||'';
  }
  function summary(r){return {count:count(r),capacity:capacity(r),quality:quality(r),cooldown:r?.provisions?.cooldown||0,items:ids.map(id=>({...definitions[id],count:r?.provisions?.counts?.[id]||0,description:describe(r,id),usable:canUse(r,id),reason:reason(r,id)}))};}
  function event(ev,id,label,amount=0,enemy){
    const d=definitions[id],e={kind:'consumable',pattern:id,supplyId:id,element:d.element,age:0,strength:1,label,amount};
    if(enemy)Object.assign(e,{targetUid:enemy.uid,enemyId:enemy.id,lane:enemy.lane});
    ev.push(e);return e;
  }
  function use(r,id,ev=[],hooks={}){
    const why=reason(r,id);if(why)return {ok:false,message:why};
    const q=quality(r),state=r.provisions;
    state.counts[id]--;state.used++;state.cooldown=1.25;
    if(id==='moss_tonic'){
      const amount=r.maxHp*.28*q,before=r.hp;
      if(hooks.heal)hooks.heal(amount);else{
        const healing=globalThis.Progression?.healing?.(r,amount)??amount;
        r.hp=Math.min(r.maxHp,r.hp+healing);globalThis.Progression?.afterHealing?.(r,healing,r.hp-before);
      }
      r.venom=0;for(const key of Object.keys(r.ailments||{}))r.ailments[key]=0;
      event(ev,id,'+'+Math.round(r.hp-before)+' · Cleansed',r.hp-before).strength=q;
    }else if(id==='bark_ward'){
      const before=r.shield;r.shield=Math.min(r.maxHp*.4,r.shield+r.maxHp*.24*q);
      event(ev,id,'Ward +'+Math.round(r.shield-before),r.shield-before).strength=q;
    }else if(id==='ember_bomb'){
      const damage=(18+5*r.round+.12*r.round*r.round)*q;
      for(const e of foes(r)){const amount=Math.min(e.hp,e.maxHp*.6,damage);e.hp-=amount;event(ev,id,'−'+Math.ceil(amount),amount,e).strength=q;}
    }else if(id==='frost_bell'){
      for(const e of foes(r)){e.freeze=Math.max(e.freeze||0,Math.min(8,1.8*q));e.attackProgress=Math.max(0,e.attackProgress-.2);event(ev,id,'Frozen',e.freeze,e).strength=q;}
    }else if(id==='quicksilver'){
      for(const it of r.active)it.cd=Math.max(0,it.cd-1.6*q);
      for(const key of Object.keys(r.abilityTimers||{}))r.abilityTimers[key]=Math.max(0,r.abilityTimers[key]-2.4*q);
      event(ev,id,'Relics recharged',1.6*q).strength=q;
    }
    return {ok:true,message:definitions[id].name+' used.'};
  }
  function afterWave(r,boss,random,ev=[]){
    const state=r.provisions||(r.provisions=empty());
    state.dryWaves++;
    if(random()>=(boss?.5:.24)&&state.dryWaves<4)return null;
    state.dryWaves=0;
    const id=ids[Math.min(ids.length-1,Math.floor(random()*ids.length))],packed=count(r)<capacity(r);
    if(packed){state.counts[id]++;state.found++;}
    state.lastFound={id,round:r.round,outcome:packed?'packed':'full'};
    const e=event(ev,id,packed?definitions[id].name+' found':'Supply satchel full');
    e.kind='supply-find';e.outcome=state.lastFound.outcome;return state.lastFound;
  }
  function validate(raw,r){
    if(raw===undefined)return empty(); // Existing expeditions never receive a mid-run refill.
    const bad=()=>{throw Error('Invalid expedition supplies.');};
    const obj=x=>{if(!x||typeof x!=='object'||Array.isArray(x))bad();return x;};
    const int=(x,max=Number.MAX_SAFE_INTEGER)=>{if(!Number.isSafeInteger(x)||x<0||x>max)bad();return x;};
    obj(raw);if(raw.version!==1)bad();const out=empty(),counts=obj(raw.counts);
    if(Object.keys(counts).some(id=>!ids.includes(id)))bad();
    for(const id of ids)out.counts[id]=int(counts[id]??0,capacity(r));
    if(ids.reduce((sum,id)=>sum+out.counts[id],0)>capacity(r))bad();
    if(typeof raw.cooldown!=='number'||!Number.isFinite(raw.cooldown)||raw.cooldown<0||raw.cooldown>1.25)bad();out.cooldown=raw.cooldown;
    out.dryWaves=int(raw.dryWaves,3);out.found=int(raw.found);out.used=int(raw.used);
    if(raw.lastFound!==null){const found=obj(raw.lastFound);if(!ids.includes(found.id)||!['packed','full'].includes(found.outcome))bad();out.lastFound={id:found.id,round:int(found.round,r.round),outcome:found.outcome};if(out.lastFound.round<1)bad();}
    return out;
  }
  function icon(id,size=40){
    const art={
      moss_tonic:'<path fill="#c19158" d="M15 4h10v6H15z"/><path fill="#3f785b" d="M16 11h8v6l7 12q3 7-4 8H13q-7-1-4-8l7-12z"/><path fill="#a9d980" d="M11 28h18l2 4q1 3-4 3H13q-5 0-4-3z"/><path fill="#eff6bd" d="M19 29q-9-9-2-12 7 2 4 9 6-8 9-3-1 7-11 6z"/><path d="M17 13v5l-5 9"/>',
      bark_ward:'<path fill="#8b663f" d="m20 4 13 6-2 16q-4 8-11 11-9-4-11-11L7 10z"/><path fill="#b8d89d" d="m20 9 8 4-1 11q-2 5-7 8-5-3-7-8l-1-11z"/><path fill="#43705a" d="m20 13 7 12-7-3-7 3z"/><path d="M20 14v14m-8-17L9 9m19 2 3-2"/>',
      ember_bomb:'<path fill="#6b4748" d="m16 10 3-5 7 4-3 5"/><circle fill="#b24f38" cx="19" cy="25" r="13"/><path fill="#f6c16b" d="M19 34q-11-5-3-13-1 5 3 6-1-7 5-11 0 8 4 11 0 7-9 7z"/><path d="m25 10 4-5m-2-2 4 4m2-5 1 4m0 4 4-1M11 18l-2 4"/>',
      frost_bell:'<path fill="#c2ebed" d="M18 5h4v7h-4z"/><path fill="#7495b6" d="M20 10q-10 0-10 13v5l-4 5h28l-4-5v-5q0-13-10-13z"/><path fill="#dbf5f2" d="M9 28h22l3 5H6z"/><path fill="#e9d69d" d="M16 34h8q-1 5-4 5t-4-5z"/><path d="M20 15v11m-5-8 10 6m-10 0 10-6M17 7l3-3 3 3"/>',
      quicksilver:'<path fill="#d6c894" d="M15 4h10v6H15z"/><path fill="#6d87b0" d="M16 11h8v7q10 6 8 13-1 7-12 7T8 31q-2-7 8-13z"/><path fill="#badfda" d="M10 27h20v5q-2 4-10 4t-10-4z"/><path fill="#fff2b6" d="m22 14-9 12h7l-2 8 10-13h-7z"/><path d="M13 21l-2 3m16-9 2 2"/>'
    };
    return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 40 42" width="${Number(size)||40}" height="${Number(size)||40}" aria-hidden="true" fill="none" stroke="#f2edce" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">${art[id]||art.moss_tonic}</svg>`;
  }
  globalThis.Provisions={version:1,ids,definitions,capacity,quality,count,start,tick,afterWave,use,validate,summary,canUse,describe,icon};
})();
