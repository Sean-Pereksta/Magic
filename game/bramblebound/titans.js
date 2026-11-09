/* Ten-round titan encounters. State and wind-ups use the simulation clock so
   pausing, replay speed and saving never skip a warning or repeat a cast. */
(() => {
  'use strict';
  const C=globalThis.Catalog;
  const definitions=[
    {id:'titan_worldroot',name:'Yggra, Worldroot Behemoth',title:'The forest remembers',species:'stag',color:'#8ce9a0',element:'leaf',weak:'fire',resist:'leaf',vitality:3.1,force:1.05,
      description:'A colossal antlered forest spirit. Thorn eruptions bind Pip while its living canopy restores the formation.',
      moves:[{name:'Thousand-Thorn Uprising',pattern:'titan-thorns',description:'Thorns erupt beneath Pip, dealing a heavy hit and slowing relic recharge. Unbinding counters the roots.'},{name:'Living Canopy',pattern:'titan-canopy',description:'The titan restores its health and its escorts, then raises a ward. Fire and ward-breaking relics keep the grove in check.'}]},
    {id:'titan_sun_eater',name:'Pyraxis, the Sun Eater',title:'An eclipse of cinders',species:'dragon',color:'#ff9c58',element:'fire',weak:'frost',resist:'fire',vitality:2.85,force:1.1,
      description:'An immense ember-winged dragon carrying a miniature sun. Falling stars leave lingering fire on Pip.',
      moves:[{name:'Solar Collapse',pattern:'titan-sunfall',description:'A collapsing sun crashes into Pip and ignites a persistent burn. Blastguard greatly reduces the afterburn.'},{name:'Phoenix Meteor Rain',pattern:'titan-meteors',description:'Three flaming meteors strike in succession. Frost, shields and healing help weather the impact.'}]},
    {id:'titan_winter_empress',name:'Isolde, Winter Empress',title:'The last wingbeat of winter',species:'moth',color:'#a2f3ff',element:'frost',weak:'storm',resist:'frost',vitality:2.9,force:1,
      description:'A cathedral-sized moth with crystalline wings. Frost prisons slow Pip, while mirror ice restores her defenses.',
      moves:[{name:'Cathedral of Absolute Winter',pattern:'titan-glacier',description:'An ice cathedral closes around Pip with a crushing hit and four seconds of chilled relic recharge. A thaw counter prevents the chill.'},{name:'Mirror-Ice Aegis',pattern:'titan-mirrors',description:'Mirror crystals restore the empress and raise a ward while a freezing gust hits Pip. Wardbreak removes the aegis.'}]},
    {id:'titan_tempest_roc',name:'Raikora, Tempest Roc',title:'The sky takes its throne',species:'bird',color:'#ffe88a',element:'storm',weak:'stone',resist:'storm',vitality:2.75,force:1.1,
      description:'A vast thunderbird whose feathers orbit like lightning rods. Repeated bolts and hurricane wings pressure the whole build.',
      moves:[{name:'Seven-Sky Judgment',pattern:'titan-lightning',description:'Three lightning bolts strike Pip and briefly disrupt automatic abilities. Insulated relics soften the bolts and prevent shock.'},{name:'Hurricane Crown',pattern:'titan-hurricane',description:'A sweeping hurricane strikes Pip and speeds up the roc’s next ordinary attack. Control can interrupt its follow-up.'}]},
    {id:'titan_mountain_heart',name:'Gravemaw, Mountain Heart',title:'The road rises against you',species:'golem',color:'#e3b994',element:'stone',weak:'leaf',resist:'stone',vitality:3.4,force:1.08,
      description:'A walking mountain of ancient armor and molten fault lines. Earthquakes crack defenses while stone plates regrow.',
      moves:[{name:'Worldbreaker Fault',pattern:'titan-fault',description:'A massive fault line hits Pip and corrodes defenses. Purify prevents corrosion; shields absorb the initial impact.'},{name:'Continental Bastion',pattern:'titan-bastion',description:'Stone plates restore the titan and raise a ward before a smaller shockwave. Poison, fracture and wardbreak help break through.'}]},
    {id:'titan_eclipse_sovereign',name:'Nyxara, Eclipse Sovereign',title:'Every shadow bows',species:'wisp',color:'#dcadff',element:'spirit',weak:'leaf',resist:'spirit',vitality:3,force:1.05,
      description:'An enormous crowned specter wrapped around a dark moon. Soul eclipses drain life, and spectral tides consume protection.',
      moves:[{name:'Eclipse of the Second Soul',pattern:'titan-eclipse',description:'An eclipse drains Pip’s life to restore Nyxara and hexes relic potency. Purify blocks the hex.'},{name:'Spectral Coronation',pattern:'titan-soultide',description:'A wave of spirits consumes part of Pip’s shield before striking. Nyxara restores health from the protection it steals.'}]}
  ].map(d=>Object.freeze({...d,titan:true,boss:true,region:-1,trait:'titan',affliction:null,moves:Object.freeze(d.moves.map(m=>Object.freeze(m)))}));
  const byId=new Map(definitions.map(d=>[d.id,d]));
  for(const d of definitions)if(!C.enemies.some(e=>e.id===d.id))C.enemies.push(d);
  const bossForRound=round=>Number.isSafeInteger(round)&&round>0&&round%10===0?definitions[(round/10-1)%definitions.length]:null;
  const state=()=>({phase:1,clock:3.2,warning:null,casts:0});
  function emit(ev,e,pattern,label,extra={}){
    const d=byId.get(e.id);ev.push({kind:'boss',element:d.element,pattern,label,bossId:d.id,targetUid:e.uid,sourceUid:e.uid,enemyId:e.id,lane:e.lane,strength:e.titan.phase===2?3:2,age:0,...extra});
  }
  function arrive(e,ev){emit(ev,e,'titan-arrival',e.name);}
  const carries=(r,key)=>r.active.some(it=>C.items[it.id]?.counter===key);
  function resolve(r,e,move,ev,ctx){
    const d=byId.get(e.id),s=e.titan,m=d.moves[move],rage=s.phase===2?1.28:1;
    let dealt=0;
    const strike=scale=>{if(r.hp>0&&e.hp>0)dealt+=ctx.damage(e.damage*scale*rage,d.element,e,ev);};
    const mend=portion=>{e.hp=Math.min(e.maxHp,e.hp+e.maxHp*portion);e.ward=1;};
    if(d.element==='leaf'){
      if(move===0){strike(1.7);if(!carries(r,'unbinding'))r.ailments.snare=3;}
      else {mend(.055);for(const other of r.enemies)if(other!==e&&other.hp>0)other.hp=Math.min(other.maxHp,other.hp+other.maxHp*.14);strike(.6);}
    }else if(d.element==='fire'){
      if(move===0){strike(1.65);r.ailments.burn+=e.damage*rage*(carries(r,'blastguard')?.12:carries(r,'thaw')?.22:.7);}
      else for(let i=0;i<3;i++)strike(.67);
    }else if(d.element==='frost'){
      if(move===0){strike(1.6);if(!carries(r,'thaw'))r.ailments.chill=4;}
      else {mend(.045);strike(.9);if(!carries(r,'thaw'))r.ailments.chill=2;}
    }else if(d.element==='storm'){
      if(move===0){for(let i=0;i<3;i++)strike(carries(r,'insulated')?.48:.65);if(!carries(r,'insulated'))r.ailments.shock=.8;}
      else {strike(1.5);e.attackProgress=Math.min(.99,e.attackProgress+.38);}
    }else if(d.element==='stone'){
      if(move===0){strike(1.85);if(!carries(r,'purify'))r.ailments.corrosion=5;}
      else {mend(.065);strike(.8);}
    }else{
      if(move===0){strike(1.55);if(e.hp>0)e.hp=Math.min(e.maxHp,e.hp+dealt*.8);if(!carries(r,'purify'))r.ailments.hex=4;}
      else {const stolen=r.shield*.45;r.shield-=stolen;e.hp=Math.min(e.maxHp,e.hp+stolen);strike(1.35);}
    }
    emit(ev,e,'titan-cast',m.name,{move,abilityPattern:m.pattern,amount:dealt});
  }
  function advance(r,e,dt,ev,ctx){
    if(!e.titanId||e.hp<=0||r.hp<=0)return;
    const s=e.titan,d=byId.get(e.id);
    if(s.phase===1&&e.hp<=e.maxHp*.5){s.phase=2;s.clock=Math.min(s.clock,1.5);e.ward=1;emit(ev,e,'titan-enrage','ENRAGED · '+d.title);}
    if(dt<=0)return;
    if(s.warning){
      s.warning.left=Math.max(0,s.warning.left-dt);
      if(s.warning.left<=0){const move=s.warning.move;s.warning=null;s.casts++;s.clock=s.phase===2?4.8:6.5;resolve(r,e,move,ev,ctx);}
    }else{
      s.clock=Math.max(0,s.clock-dt);
      if(!s.clock){const move=s.casts%2,duration=s.phase===2?1.35:1.8;s.warning={move,left:duration,duration};emit(ev,e,'titan-warning',d.moves[move].name,{move,abilityPattern:d.moves[move].pattern,duration});}
    }
  }
  function validateState(raw){
    const bad=()=>{throw Error('Invalid titan encounter state.');};
    const num=(v,max)=>{if(typeof v!=='number'||!Number.isFinite(v)||v<0||v>max)bad();return v;};
    if(!raw||typeof raw!=='object'||Array.isArray(raw)||![1,2].includes(raw.phase))bad();
    if(!Number.isSafeInteger(raw.casts)||raw.casts<0)bad();
    const s={phase:raw.phase,clock:num(raw.clock,6.5),casts:raw.casts,warning:null};
    if(raw.warning!=null){const w=raw.warning;if(![0,1].includes(w.move)||![1.35,1.8].includes(w.duration))bad();s.warning={move:w.move,left:num(w.left,w.duration),duration:w.duration};}
    return s;
  }
  globalThis.Titans={definitions,bossForRound,byId:id=>byId.get(id),state,arrive,advance,validateState,defeated:(e,ev)=>emit(ev,e,'titan-defeated',e.name+' defeated')};
})();
