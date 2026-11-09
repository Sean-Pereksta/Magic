/* Three equipment sets. Reactions resolve only from primary relic activations. */
(() => {
  'use strict';
  const root=globalThis, C=()=>root.Catalog;
  const support=new Set(['heal','shield','stun','freeze']);
  const sets=Object.freeze([
    {id:'emberglass',name:'Emberglass',elements:['fire','frost'],requirement:'Equip at least one Fire and one Frost relic.',
      description:'2 pieces: Fire consumes a target’s chill or freeze for a 35% hit across up to three foes and a 0.35s interrupt. Once per 1.5s.',
      bonus:'3 pieces: the shatter deals 65% damage and interrupts for 0.6s.'},
    {id:'bramble_circuit',name:'Bramble Circuit',elements:['leaf','storm'],requirement:'Equip at least one Leaf and one Storm relic.',
      description:'2 pieces: Storm deals 20% more damage to poisoned foes. A Storm activation spreads 40% of their poison to other foes and advances Leaf relics by 0.3s, once per 1.5s.',
      bonus:'3 pieces: 35% bonus damage, 65% poison spread and 0.55s Leaf recharge.'},
    {id:'soulstone',name:'Soulstone',elements:['stone','spirit'],requirement:'Equip at least one Stone and one Spirit relic.',
      description:'2 pieces: Stone activations grant 2% maximum HP as shield. While shielded, Spirit activations advance Stone relics by 0.45s. Each effect has a 1.5s cooldown.',
      bonus:'3 pieces: gain 4% maximum HP as shield and advance Stone relics by 0.8s.'}
  ]);
  const timerIds=['emberglass','bramble_circuit','soulstone_stone','soulstone_spirit'];
  const fresh=()=>({timers:Object.fromEntries(timerIds.map(id=>[id,0]))});
  const state=r=>r.synergies??=fresh();
  function describe(r){
    const gear=(r?.active||[]).map(it=>C().items[it.id]).filter(Boolean);
    return sets.map(d=>{
      const count=gear.filter(it=>d.elements.includes(it.element)).length;
      const active=d.elements.every(el=>gear.some(it=>it.element===el));
      return {...d,count,tier:active?(count>=3?3:2):0,active};
    });
  }
  const tier=(r,id)=>describe(r).find(d=>d.id===id).tier;
  function emit(ev,id,element,secondaryElement,label,e,extra={}){
    if(ev.length>=180)return;
    const v={kind:'synergy',pattern:id,setId:id,element,secondaryElement,label,age:0,strength:1,...extra};
    if(e)Object.assign(v,{targetUid:e.uid,enemyId:e.id,lane:e.lane});ev.push(v);
  }
  function tick(r,dt){const s=state(r);for(const id of timerIds)s.timers[id]=Math.max(0,s.timers[id]-dt);}
  function hitMultiplier(r,d,e){
    if(support.has(d.style)||d.element!=='storm'||!(e.poison>0))return 1;
    const n=tier(r,'bramble_circuit');return n>=3?1.35:n?1.2:1;
  }
  function onActivation(r,e,d,damage,ev){
    const timers=state(r).timers, set=describe(r), get=id=>set.find(v=>v.id===id).tier;
    const ember=get('emberglass');
    if(ember&&d.element==='fire'&&!support.has(d.style)&&damage>0&&(e.chill>0||e.freeze>0)&&timers.emberglass===0){
      timers.emberglass=1.5;e.chill=0;e.freeze=0;
      const targets=[e,...r.enemies.filter(t=>t!==e)].filter(t=>t.hp>0).slice(0,3),amount=damage*(ember>=3?.65:.35);
      for(const foe of targets){const removed=Math.min(foe.hp,amount);foe.hp=Math.max(0,foe.hp-removed);foe.stun=Math.min(8,Math.max(foe.stun||0,ember>=3?.6:.35));emit(ev,'emberglass','fire','frost','Steam shatter '+Math.ceil(removed),foe,{amount:removed,strength:ember>=3?2:1});}
    }
    const circuit=get('bramble_circuit');
    if(circuit&&d.element==='storm'&&!support.has(d.style)&&damage>0&&e.poison>0&&timers.bramble_circuit===0){
      timers.bramble_circuit=1.5;
      for(const foe of r.enemies)if(foe!==e&&foe.hp>0)foe.poison=Math.max(foe.poison,Math.min(foe.maxHp*.08,e.poison*(circuit>=3?.65:.4)));
      for(const it of r.active)if(C().items[it.id].element==='leaf')it.cd=Math.max(0,it.cd-(circuit>=3?.55:.3));
      emit(ev,'bramble_circuit','storm','leaf','Poison circuit',e,{strength:circuit>=3?2:1});
    }
    const soul=get('soulstone');if(!soul)return;
    if(d.element==='stone'&&timers.soulstone_stone===0){
      timers.soulstone_stone=1.5;const before=r.shield;r.shield=Math.min(r.maxHp*.4,r.shield+r.maxHp*(soul>=3?.04:.02));
      emit(ev,'soulstone','stone','spirit','Soulstone ward +'+Math.ceil(r.shield-before),null,{strength:soul>=3?2:1});
    }
    if(d.element==='spirit'&&r.shield>0&&timers.soulstone_spirit===0){
      timers.soulstone_spirit=1.5;
      for(const it of r.active)if(C().items[it.id].element==='stone')it.cd=Math.max(0,it.cd-(soul>=3?.8:.45));
      emit(ev,'soulstone','spirit','stone','Soulstone recharge',null,{strength:soul>=3?2:1});
    }
  }
  function validate(raw){
    if(raw===undefined)return fresh();
    const fail=()=>{throw Error('Invalid elemental set state.');};
    if(!raw||typeof raw!=='object'||Array.isArray(raw)||!raw.timers||typeof raw.timers!=='object'||Array.isArray(raw.timers))fail();
    const s=fresh();
    for(const id of timerIds){const value=raw.timers[id]??0;if(typeof value!=='number'||!Number.isFinite(value)||value<0||value>1.5)fail();s.timers[id]=value;}
    return s;
  }
  root.Synergies={sets,describe,hitMultiplier,onActivation,tick,validate};
})();
