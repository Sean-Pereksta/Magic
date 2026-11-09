/* Forge-only Ancient powers. Secondary strikes never re-enter primary relic
   hooks. All effects resolve on an activation, so no wall-clock timers or new
   save state are required. Load after Living Combat and before the engine. */
(() => {
  'use strict';
  const C=globalThis.Catalog,P=globalThis.Progression,old={...P};
  const ids=Object.values(C.items).filter(d=>d.ancient).map(d=>d.id);
  const equipped=r=>(r?.active||[]).map(it=>C.items[it.id]).filter(d=>d?.ancient);
  const owns=(r,aura)=>equipped(r).some(d=>d.aura===aura);
  const living=enemies=>enemies.filter(e=>e.hp>0);
  function event(ev,d,label,e=null,extra={}){
    if(!ev||ev.length>=180)return;
    ev.push({kind:'ancient',element:d.element,pattern:'ancient_'+d.aura,itemId:d.id,
      label,age:0,strength:8,hero:!e,...(e?{targetUid:e.uid,enemyId:e.id,lane:e.lane}:{}),...extra});
  }
  function restore(r,n,ev,d){
    if(r.hp<=0)return 0;
    const amount=P.healing(r,n),before=r.hp;r.hp=Math.min(r.maxHp,r.hp+amount);
    P.afterHealing?.(r,amount,r.hp-before);
    if(r.hp>before)event(ev,d,'+'+Math.ceil(r.hp-before),null,{recovery:true});
    return r.hp-before;
  }
  function shield(r,n,ev,d){
    const before=r.shield;r.shield=Math.min(r.maxHp*.4,r.shield+n);
    if(r.shield>before)event(ev,d,'Shield +'+Math.ceil(r.shield-before),null,{recovery:true});
  }
  function hit(e,n,d,ev){
    if(!e||e.hp<=0)return 0;
    const match=tag=>Array.isArray(tag)?tag.includes(d.element):tag===d.element;
    n*=match(e.weak)?1.68:match(e.resist)?.6:1;
    const removed=Math.min(e.hp,Math.max(0,n));e.hp=Math.max(0,e.hp-n);
    event(ev,d,String(Math.ceil(removed)),e);return removed;
  }
  function fallbackPower(r,d,it){
    const n=(it?.level||1)-1;
    return d.power*C.rarities.ancient.powerScale*(1+.31*n+.009*n*n)
      *(1+(r.legacy?.ranks?.mastercraft||0)*.03)*(1+(r.legacy?.ranks?.celestial_canopy||0)*.05);
  }
  function controlDuration(r,d,it,power){
    const level=it?.level||1,milestones=1+(level>=3?.4:0)+(level>=5?.2:0)+(level>=10?.65:0);
    return Math.min(8,d.duration*Math.pow(power/d.power,.25)*Math.pow(P.utilityMultiplier(r,d),.25)*milestones);
  }
  P.onSpawn=(r,ev=[])=>{
    old.onSpawn?.(r,ev);
    for(const d of [...new Map(equipped(r).map(d=>[d.id,d])).values()])event(ev,d,d.name,null,{awakening:true});
  };
  P.incoming=(r,n,e,ev,rng)=>old.incoming(r,n,e,ev,rng)*(owns(r,'titan')?.7:1);
  P.hitMultiplier=(r,d,e,count)=>old.hitMultiplier(r,d,e,count)*(e?.boss&&owns(r,'void')?1.5:1);
  P.onActivation=(r,enemies,e,d,damage,ev,rng,source,potency)=>{
    old.onActivation?.(r,enemies,e,d,damage,ev,rng);
    if(!d.ancient)return;
    const it=source||(r.active||[]).find(it=>it.id===d.id);
    // The engine supplies the exact source potency, including fractional levels
    // and permanent bonuses; direct hook callers retain a deterministic fallback.
    const power=Number.isFinite(potency)&&potency>=0?potency:fallbackPower(r,d,it);
    const targets=()=>living(enemies);
    const strike=(target,factor)=>hit(target,power*factor,d,ev);
    if(d.aura==='tidal'){
      for(const t of targets())strike(t,1.2);
      restore(r,r.maxHp*.08,ev,d);shield(r,r.maxHp*.08,ev,d);
      if(r.ailments){r.ailments.corrosion=0;r.ailments.snare=0;}
    }else if(d.aura==='worldroot'){
      for(const t of targets()){
        strike(t,.9);if(t.hp<=0)continue;
        t.poison+=power*1.2;t.rooted=Math.min(8,Math.max(t.rooted||0,1.8));
      }
      restore(r,r.maxHp*.06,ev,d);
    }else if(d.aura==='sunfire'){
      for(const t of targets()){
        const burning=t.burn>0;strike(t,1.6+(burning?.8:0));
        if(t.hp>0)t.burn=Math.max(t.burn||0,power*1.6);
      }
    }else if(d.aura==='winter'||d.aura==='hourglass'){
      const style=d.aura==='winter'?'freeze':'stun',duration=controlDuration(r,d,it,power);
      for(const t of targets()){t[style]=Math.min(8,Math.max(t[style]||0,duration));event(ev,d,style==='freeze'?'Frozen':'Time stopped',t,{duration});}
      if(d.aura==='winter')shield(r,r.maxHp*.1,ev,d);
      else for(const gear of r.active)if(gear!==it)gear.cd=Math.max(0,gear.cd-.8);
    }else if(d.aura==='storm'){
      for(let i=0;i<6;i++){
        const ts=targets();if(!ts.length)break;const t=ts[i%ts.length];
        strike(t,.75);t.attackProgress=Math.max(0,t.attackProgress-.18);t.stun=Math.max(t.stun||0,.35);
      }
      shield(r,r.maxHp*.08,ev,d);
    }else if(d.aura==='titan'){
      shield(r,r.maxHp*.4,ev,d);
    }else if(d.aura==='eclipse'){
      let stolen=0;for(const t of targets())stolen+=strike(t,1.4);
      restore(r,stolen*.3,ev,d);
    }else if(d.aura==='starfall'){
      for(let i=0;i<3;i++){
        const ts=targets();if(!ts.length)break;const t=ts[i%ts.length];
        t.ward=0;t.fracture=Math.min(5+2*P.rankOf(r,'fracture_point'),(t.fracture||0)+3);strike(t,1.3);
      }
    }else if(d.aura==='everdawn'){
      restore(r,r.maxHp*.18,ev,d);shield(r,r.maxHp*.15,ev,d);
      r.venom=0;if(r.ailments)for(const key of Object.keys(r.ailments))r.ailments[key]=0;
    }else if(d.aura==='void'){
      for(const t of targets()){t.ward=0;strike(t,2.2);if(t.hp>0)t.rooted=Math.min(8,Math.max(t.rooted||0,2.5));}
    }
    event(ev,d,d.name,null,{burst:true});
  };
  globalThis.AncientArtifacts=Object.freeze({ids:Object.freeze(ids),equipped,aura:r=>equipped(r).map(d=>d.aura)});
})();
