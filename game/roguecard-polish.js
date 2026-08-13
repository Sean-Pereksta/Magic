/* Tiny Troops / Emoji Squad additive polish layer.
   Hooks the existing game without replacing its roster, balance tables, or save format. */
(function(){
  const TP=window.TinyTroopsPolish=window.TinyTroopsPolish||{};
  TP.speed=Number(localStorage.getItem('ttBattleSpeed')||1);
  if(![1,2,3].includes(TP.speed))TP.speed=1;
  TP.selectedIndex=null;TP.moveFrom=null;TP.undoSquad=null;TP.focusTarget=null;
  TP.stats=new Map();TP.enemyThreat={};TP.lastRecap=null;TP.lastBossBattle=null;TP.newEnemies=new Set();
  try{TP.seen=new Set(JSON.parse(localStorage.getItem('ttSeenUnits')||'[]'))}catch(_){TP.seen=new Set()}
  const saveSeen=()=>{try{localStorage.setItem('ttSeenUnits',JSON.stringify([...TP.seen].slice(-900)))}catch(_){}};
  const esc=s=>String(s??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
  const q=(s,r=document)=>r.querySelector(s),qa=(s,r=document)=>[...r.querySelectorAll(s)];

  function ensureUi(){
    const main=q('.main');if(!main)return;
    if(!q('#ttIntel')){
      const d=document.createElement('div');d.id='ttIntel';d.className='tt-intel';
      d.innerHTML='<div class="tt-region"><div><strong id="ttRegionName">Battlefield</strong><span id="ttRegionMeta"></span></div><div class="tt-speed"><button data-sp="1">1×</button><button data-sp="2">2×</button><button data-sp="3">3×</button></div></div><div id="ttNext" class="tt-next">Next wave preview</div>';
      main.insertBefore(d,main.firstChild);
      qa('[data-sp]',d).forEach(b=>b.onclick=()=>setSpeed(+b.dataset.sp));
    }
    const wrap=q('.gridWrap');
    if(wrap&&!q('#ttUnitRibbon')){
      const r=document.createElement('div');r.id='ttUnitRibbon';r.className='tt-unit-ribbon';wrap.appendChild(r);
    }
    const footer=q('footer');
    if(footer&&!q('#ttRecap')){const r=document.createElement('div');r.id='ttRecap';r.className='tt-recap';footer.insertBefore(r,footer.firstChild)}
  }
  function setSpeed(n){TP.speed=n;localStorage.setItem('ttBattleSpeed',String(n));postRender()}
  function intentIcon(e){
    const a=(e.attack||'').toLowerCase(),f=(e.focus||'').toLowerCase();
    if(a.includes('fire')||a.includes('burn'))return'🔥';if(a.includes('poison'))return'☠';if(a.includes('frost'))return'❄';if(a.includes('storm'))return'⚡';if(a.includes('shadow'))return'🌘';if(a.includes('holy'))return'✨';if(a.includes('thorn'))return'🌿';
    if(f.includes('back'))return'↩';if(f.includes('weak')||f.includes('wound'))return'🎯';if(f.includes('strong')||f.includes('healthy'))return'💥';if(f.includes('armor'))return'🛡';if(f.includes('support')||f.includes('caster'))return'🌀';return e.kind==='ranged'?'🏹':'⚔';
  }
  function regionPreview(){
    if(typeof region!=='function')return;
    const rg=region(),name=q('#ttRegionName'),meta=q('#ttRegionMeta'),next=q('#ttNext');
    if(name)name.textContent=`${rg.e||''} ${rg.n||'Battlefield'}`;
    if(meta)meta.textContent=`Wave ${S.round}${S.round%5===0?' • 👑 BOSS':''} • Threat ${(S.lastThreat||enemyScale()).toFixed(2)}×`;
    if(!next)return;
    let defs=[];
    if(S.phase==='battle'&&S.enemies?.length)defs=S.enemies.filter(e=>!e.dead).slice(0,5);
    else if(window.enemyDefs||typeof enemyDefs!=='undefined')defs=enemyDefs.filter(e=>(rg.main||[]).includes(e.n)).filter((e,i,a)=>a.findIndex(x=>x.n===e.n)===i).slice(0,5);
    const icons=defs.map(e=>e.e).join(' ');
    next.innerHTML=`<b>${S.phase==='battle'?'Facing':'Next'}:</b> ${icons||'❔'} ${S.round%5===0?' • Boss wave':''}`;
  }
  function reorderTraits(){
    const el=q('#traits');if(!el||typeof counts!=='function')return;const c=counts();
    qa('.synergyTag',el).forEach(d=>{
      const txt=d.textContent.trim(),key=Object.keys(TE||{}).find(t=>txt.includes(` ${t} `)||txt.includes(`${t} `));if(!key)return;
      const n=c[key]||0;d.classList.toggle('tt-active',n>=2);d.classList.toggle('tt-near',n===1);d.classList.toggle('tt-dim',n===0);
      d.textContent=`${TE[key]||''} ${key} ${n>=2?`${n}★`:`${n}/2`}`;
    });
  }
  function selectUnit(i){TP.selectedIndex=i;updateUnitRibbon();qa('.unit.tt-selected').forEach(x=>x.classList.remove('tt-selected'));const c=q(`.cell[data-i="${i}"] .unit`);if(c)c.classList.add('tt-selected')}
  function updateUnitRibbon(){
    const r=q('#ttUnitRibbon');if(!r)return;const i=TP.selectedIndex,u=Number.isInteger(i)?S.squad[i]:null;
    if(!u){r.classList.remove('show');r.innerHTML='';return}
    const b=typeof bonus==='function'?bonus(u):{},hp=Math.max(0,Math.round(u.hp||0)),mh=Math.max(1,Math.round(u.maxHp||1)),atk=Math.round((u.atk||0)+(b.atk||0));
    const moving=TP.moveFrom===i;
    r.innerHTML=`<div class="who">${u.e} <b>${esc(typeof heroTitle==='function'?heroTitle(u):u.n)}</b> · ★${u.star||1} · ${hp}/${mh} HP · ${atk} ATK · ${(u.t||[]).slice(0,2).join(' / ')}</div><div class="miniActions"><button class="secondary" id="ttMoveBtn">${moving?'Cancel':'Move'}</button>${TP.undoSquad?'<button class="secondary" id="ttUndoBtn">Undo</button>':''}</div>`;
    r.classList.add('show');q('#ttMoveBtn',r).onclick=()=>{if(S.phase!=='recruit'||S.placing||S.placingEffect||S.placingUpgrade)return msg('Reposition between battles before choosing/placing a card.');TP.moveFrom=TP.moveFrom===i?null:i;postRender()};
    const ub=q('#ttUndoBtn',r);if(ub)ub.onclick=()=>{if(!TP.undoSquad)return;S.squad=TP.undoSquad;TP.undoSquad=null;TP.moveFrom=null;render()};
  }
  function decorateGrid(){
    qa('.cell').forEach(c=>c.classList.toggle('tt-move-target',TP.moveFrom!=null&&S.phase==='recruit'));
    if(TP.selectedIndex!=null){const u=S.squad[TP.selectedIndex];if(u)q(`.cell[data-i="${TP.selectedIndex}"] .unit`)?.classList.add('tt-selected');else TP.selectedIndex=null}
  }
  function decorateEnemies(){
    qa('#enemyRow .enemy').forEach((d,i)=>{const e=S.enemies[i];if(!e)return;d.classList.toggle('tt-focus',TP.focusTarget===e&&!e.dead);
      if(!q('.intentBadge',d)){const s=document.createElement('span');s.className='intentBadge';s.title=`Intent: ${e.attack||e.kind}; target ${e.focus||e.kind}`;s.textContent=intentIcon(e);d.appendChild(s)}
      if(!q('.focusPin',d)&&!e.dead){const b=document.createElement('button');b.className='focusPin'+(TP.focusTarget===e?' on':'');b.textContent='🎯';b.title='Prefer this enemy for flexible attackers';b.onclick=ev=>{ev.stopPropagation();TP.focusTarget=TP.focusTarget===e?null:e;postRender()};d.appendChild(b)}
      if(TP.newEnemies.has(e.n)&&!q('.tt-new',d)){const n=document.createElement('span');n.className='tt-new';n.textContent='NEW';d.appendChild(n)}
    });
  }
  function decorateChoices(){
    const body=q('#body');if(!body)return;const cards=qa('.choice',body);
    if(S.phase==='recruit'&&Array.isArray(S.choices))cards.forEach((d,i)=>{const c=S.choices[i];if(!c)return;
      if(c.type==='recruit'){
        const u=c.unit,owned=S.squad.some(x=>x&&x.n===u.n),ct=typeof counts==='function'?counts():{},activates=(u.t||[]).find(t=>(ct[t]||0)===1),supports=(u.t||[]).find(t=>(ct[t]||0)>=2);
        const mid=d.children[1];if(mid&&!q('.tt-choice-hint',mid)){const h=document.createElement('span');h.className='tt-choice-hint'+(owned?' up':'');h.textContent=owned?'⬆ Star training':activates?`⚡ Activates ${activates}`:supports?`✦ Strengthens ${supports}`:'New squad option';mid.appendChild(h)}
        if(!TP.seen.has(u.n)&&!q('.tt-new',d)){const n=document.createElement('span');n.className='tt-new';n.textContent='NEW';d.appendChild(n)}
      }
    });
    if(S.phase==='shop'&&Array.isArray(S.items))cards.forEach((d,i)=>{const it=S.items[i],mid=d.children[1];if(!it||!mid||q('.tt-choice-hint',mid))return;const txt=`${it[0]||it.n||''} ${it[3]||it.desc||''}`;let hit=Object.keys(TE||{}).filter(t=>txt.includes(t)),benefit=S.squad.filter(u=>u&&hit.some(t=>u.t?.includes(t))).length;if(benefit){const h=document.createElement('span');h.className='tt-choice-hint';h.textContent=`✦ Benefits ${benefit} squad unit${benefit===1?'':'s'}`;mid.appendChild(h)}});
  }
  function bossCallout(){
    if(S.phase!=='battle'||!S.battle||TP.lastBossBattle===S.battle.id)return;const boss=S.enemies.find(e=>e.boss&&!e.dead);if(!boss)return;TP.lastBossBattle=S.battle.id;
    const d=document.createElement('div');d.className='tt-boss-callout';d.innerHTML=`<span>👑 BOSS INCOMING</span><b>${boss.e} ${esc(boss.n)}</b>`;document.body.appendChild(d);setTimeout(()=>d.remove(),1550);
  }
  function recapHtml(){
    const r=TP.lastRecap;if(!r)return'';const cells=r.top.map(x=>`<div class="tt-recap-cell"><b>${x.e} ${esc(x.n)}</b><span>${Math.round(x.damage)} dmg · ${Math.round(x.heal)} heal · ${Math.round(x.block)} block</span></div>`).join('');
    return `<div class="tt-recap-head"><span>${r.win?'✅ Victory':'💀 Defeat'} · Wave ${r.round}</span><span>${r.duration?`${r.duration.toFixed(1)}s`:''}</span></div><div class="tt-recap-grid">${cells||'<div class="tt-recap-cell"><b>Squad</b><span>No tracked stats</span></div>'}</div>${r.threat?`<div class="tt-loss-note">Biggest threat: ${r.threat.e} ${esc(r.threat.n)} · ${Math.round(r.threat.damage)} damage dealt to your squad.</div>`:''}`;
  }
  function showRecap(){const d=q('#ttRecap');if(!d)return;if(!TP.lastRecap){d.classList.remove('show');return}d.innerHTML=recapHtml();d.classList.add('show')}
  function postRender(){ensureUi();regionPreview();reorderTraits();decorateGrid();decorateEnemies();decorateChoices();updateUnitRibbon();showRecap();bossCallout();qa('[data-sp]').forEach(b=>b.classList.toggle('on',+b.dataset.sp===TP.speed));q('#app')?.classList.toggle('tt-combat-busy',S.phase==='battle')}
  TP.postRender=postRender;

  // Tap-to-move formation editing while safely between battles.
  document.addEventListener('click',ev=>{const cell=ev.target.closest?.('.cell');if(!cell)return;if(TP.moveFrom!=null&&S.phase==='recruit'&&!S.placing&&!S.placingEffect&&!S.placingUpgrade){ev.preventDefault();ev.stopImmediatePropagation();const to=+cell.dataset.i,from=TP.moveFrom;if(to===from){TP.moveFrom=null;return postRender()}TP.undoSquad=S.squad.slice();[S.squad[from],S.squad[to]]=[S.squad[to],S.squad[from]];TP.selectedIndex=to;TP.moveFrom=null;msg('Formation adjusted. Undo is available until the next move.');render();return}const i=+cell.dataset.i;if(Number.isInteger(i)&&S.squad[i])selectUnit(i)},true);

  // Preserve the original renderer and add the information layer after every core redraw.
  const coreRender=render;render=function(){coreRender();postRender()};

  // Flexible attackers can honor the player's marked focus target; specialist targeting stays untouched.
  const corePickEnemy=pickEnemy;pickEnemy=function(u){const t=TP.focusTarget,forced=new Set(['weak','strong','back','burn','poison','ranged','support','boss','armor','random','wounded','healthy','caster','elite','duel','sniper']);if(t&&!t.dead&&S.enemies.includes(t)&&!forced.has(String(u?.focus||'').toLowerCase()))return t;return corePickEnemy(u)};

  function st(u){if(!u||u.team!=='ally')return null;if(!TP.stats.has(u))TP.stats.set(u,{damage:0,heal:0,block:0});return TP.stats.get(u)}
  const coreDmg=dmg;dmg=function(src,tgt,amt,type){if(!tgt||tgt.dead)return coreDmg(src,tgt,amt,type);const bh=tgt.hp||0,bs=tgt.shield||0,r=coreDmg(src,tgt,amt,type),hpLoss=Math.max(0,bh-(tgt.hp||0)),shieldLoss=Math.max(0,bs-(tgt.shield||0));if(src?.team==='ally'){const x=st(src);if(x)x.damage+=hpLoss+shieldLoss}if(tgt.team==='ally'){const x=st(tgt);if(x)x.block+=shieldLoss;if(src?.team==='enemy'){const k=src.n||'Enemy';TP.enemyThreat[k]=TP.enemyThreat[k]||{n:k,e:src.e||'👾',damage:0};TP.enemyThreat[k].damage+=hpLoss+shieldLoss}}return r};
  const coreHeal=heal;heal=function(src,t,amt){const before=t?.hp||0,r=coreHeal(src,t,amt),gain=Math.max(0,(t?.hp||0)-before);if(src?.team==='ally'){const x=st(src);if(x)x.heal+=gain}return r};

  const coreFight=fight;fight=function(){TP.stats=new Map();TP.enemyThreat={};TP.lastRecap=null;TP.focusTarget=null;TP.undoSquad=null;TP.moveFrom=null;TP.battleStarted=performance.now();const r=coreFight();TP.newEnemies=new Set((S.enemies||[]).map(e=>e.n).filter(n=>!TP.seen.has(n)));TP.newEnemies.forEach(n=>TP.seen.add(n));saveSeen();postRender();return r};
  const coreChoose=choose;choose=function(i){const c=S.choices?.[i];if(c?.type==='recruit'){TP.seen.add(c.unit.n);saveSeen()}return coreChoose(i)};window.choose=choose;
  const coreEnd=end;end=function(win){const round=S.round,elapsed=TP.battleStarted?(performance.now()-TP.battleStarted)/1000:0,top=S.squad.filter(Boolean).map(u=>{const x=TP.stats.get(u)||{damage:0,heal:0,block:0};return{n:typeof heroTitle==='function'?heroTitle(u):u.n,e:u.e,...x,total:x.damage+x.heal+x.block*.65}}).sort((a,b)=>b.total-a.total).slice(0,3),threat=Object.values(TP.enemyThreat).sort((a,b)=>b.damage-a.damage)[0]||null;TP.lastRecap={win,round,duration:elapsed,top,threat:win?null:threat};const r=coreEnd(win);setTimeout(postRender,30);return r};

  // Same combat logic, adjustable presentation speed only.
  loop=function(battleId){if(S.phase!=='battle'||!S.battle)return;if(battleId&&S.battle.id&&battleId!==S.battle.id)return;let a=liveA(),e=liveE();if(!a.length)return end(false);if(!e.length)return end(true);S.battle.tick++;if(S.battle.tick%3===0)statuses();regionTick();evoPulse();a.forEach(u=>{let b=bonus(u);u.cd=(u.cd||0)-.25;let sp=Math.max(.35,u.spd+(b.spd||0));if(hasMixed('bloodpack')&&hasTag(u,'Blood','Beast')&&u.hp/u.maxHp<.55)sp+=.12;if(hasMixed('stormfreeze')&&hasTag(u,'Frost')&&region().id==='water')sp+=.08;if(u.cd<=0){allyAtk(u);u.cd=Math.max(.38,1.75/sp)}});e.forEach(u=>{u.cd=(u.cd||0)-.25;let sp=Math.max(.35,u.spd-(u.slow||0));if(u.cd<=0){enemyAtk(u);u.cd=Math.max(.45,1.9/sp)}});render();setTimeout(()=>loop(battleId),Math.max(80,Math.round(260/TP.speed)))};

  // Extend help text so the new controls explain themselves.
  const coreHelp=help;help=function(){coreHelp();setTimeout(()=>{const body=q('#body');if(!body)return;body.insertAdjacentHTML('beforeend','<p><b>Polish controls:</b> tap a troop to inspect it and use <b>Move</b> between battles to rearrange formation. Tap 🎯 on an enemy to make flexible attackers prefer it. The top bar previews the next enemy pool and offers 1×/2×/3× battle speed. After a fight, the recap shows your top damage/healing/blocking performers.</p>')},0)};

  ensureUi();postRender();
})();
