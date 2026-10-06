import {WAR_MODES, WAR_RULES, enemyCapitalsHeld, bannersHeld, capitalLost, capitalKingExposed} from './objectives.mjs';

export function initWarModePicker(select,description,onChange){
  for(const [id,mode] of Object.entries(WAR_MODES)){
    const option=document.createElement('option');option.value=id;option.textContent=mode.name;select.append(option);
  }
  select.value='grand';
  const update=()=>{description.textContent=WAR_MODES[select.value].description;onChange(select.value);};
  select.addEventListener('change',update);update();
}

export class ObjectiveHUD {
  constructor(element,focus){
    this.element=element;this.signature='';this.rows=new Map();this.notices=[];this.seen=new Set();
    element.addEventListener('click',event=>{
      const button=event.target.closest('[data-objective]');
      if(button){const objective=this.war?.objectives.find(o=>o.id===button.dataset.objective);if(objective)focus(objective);}
    });
  }
  reset(){this.signature='';this.rows.clear();this.notices=[];this.seen.clear();this.element.replaceChildren();}
  notify(text,time){
    if(this.notices.some(n=>n.text===text&&time-n.time<10))return;
    this.notices.push({text,time});this.notices=this.notices.slice(-4);
  }
  update(war,factions,player,time){
    this.war=war;
    if(!war || war.mode==='regicide'){this.element.hidden=true;return;}
    this.element.hidden=false;
    const signature=`${war.mode}|${factions.map(f=>f.idx+':'+f.short).join('|')}`;
    if(signature!==this.signature){
      this.signature=signature;this.element.replaceChildren();this.rows.clear();
      const title=document.createElement('strong');title.className='objectiveTitle';
      title.textContent=war.mode==='capital'?`CAPITAL CONQUEST · Hold ${war.capitalTarget} enemy capitals`:`DOMINION · First to ${war.target}${war.mode==='grand'?` · or ${war.capitalTarget} enemy capitals`:''}`;
      this.element.append(title);
      const table=document.createElement('div');table.className='objectiveScores';
      for(const faction of factions){
        const row=document.createElement('div');row.className='objectiveScore';row.style.setProperty('--faction',faction.color);
        const name=document.createElement('span');name.textContent=`${faction.icon} ${faction.short}`;
        const value=document.createElement('b');const detail=document.createElement('small');
        row.append(name,value,detail);table.append(row);this.rows.set(faction.idx,{row,value,detail});
      }
      this.element.append(table);
      const buttons=document.createElement('div');buttons.className='objectiveLinks';
      for(const o of war.objectives){
        const b=document.createElement('button');b.type='button';b.dataset.objective=o.id;
        b.textContent=o.kind==='capital'?'♜':o.central?'⚑★':'⚑';buttons.append(b);
      }
      this.element.append(buttons);
      const help=document.createElement('small');help.className='objectiveHelp';help.textContent=`${WAR_MODES[war.mode].dominion?`Banners: ${WAR_RULES.bannerSeconds}s · center +2 / 5s · other +1 / 5s. `:''}Capitals: ${WAR_RULES.capitalSeconds}s · lost capital halves production and reveals king. Click a marker to focus.`;
      this.element.append(help);
      this.notice=document.createElement('div');this.notice.className='objectiveNotice';this.notice.setAttribute('role','status');this.element.append(this.notice);
    }
    for(const f of factions){
      const r=this.rows.get(f.idx);r.row.classList.toggle('eliminated',!f.alive);r.row.classList.toggle('yours',f.idx===player);
      const score=war.mode==='capital'?`${enemyCapitalsHeld(war,f.idx)}/${war.capitalTarget}`:`${war.scores[f.idx]||0}/${war.target}`;
      if(r.value.textContent!==score)r.value.textContent=score;
      const detail=`⚑ ${bannersHeld(war,f.idx)} · ♜ ${enemyCapitalsHeld(war,f.idx)}${capitalLost(war,f.idx)?' · HOME LOST':''}`;
      if(r.detail.textContent!==detail)r.detail.textContent=detail;
      if(f.alive && WAR_MODES[war.mode].dominion && war.scores[f.idx]>=108 && !this.seen.has(`leader:${f.idx}`)){
        this.seen.add(`leader:${f.idx}`);this.notify(`⚠ ${f.short} approaching Dominion victory (${war.scores[f.idx]}/${war.target})`,time);
      }
    }
    for(const b of this.element.querySelectorAll('[data-objective]')){
      const o=war.objectives.find(o=>o.id===b.dataset.objective);
      b.style.color=o.owner==null?'#d4deeb':factions.find(f=>f.idx===o.owner)?.color||'#fff';
      b.classList.toggle('contested',o.status==='contested');
      b.classList.toggle('capturing',o.status==='capturing');
      const label=`${o.name}: ${o.status}${o.claimant!=null?` ${factions[o.claimant]?.short} ${Math.round(o.progress*100)}%`:''}${o.owner!=null?` · ${factions[o.owner]?.short}`:''}`;
      b.title=label;b.setAttribute('aria-label',label);
    }
    const capital=war.objectives.find(o=>o.kind==='capital'&&o.home===player);
    const warning=capital&&(capital.claimant!=null||capital.status==='contested')?'🚨 Your capital is under attack':capitalLost(war,player)?'♜ Retake your capital to restore production and conceal your king':null;
    const recent=this.notices.filter(n=>time-n.time<8).at(-1);
    const leader=factions.filter(f=>f.alive&&(war.scores[f.idx]||0)>=108).sort((a,b)=>war.scores[b.idx]-war.scores[a.idx])[0];
    const text=warning||(leader?`⚠ ${leader.short}: ${war.scores[leader.idx]}/${war.target} Dominion`:recent?.text)||'';if(this.notice.textContent!==text)this.notice.textContent=text;
  }
}

export function warVictoryText(war,factions,winner,territory=0,captures=0){
  const reason=war?.victory?.reason||'conquest',f=factions[winner],stats=war?.stats[winner]||{};
  const title={dominion:'Dominion Victory',imperial:'Imperial Victory',conquest:'Total Conquest'}[reason];
  const explanation=reason==='dominion'?`${f.short} reached ${war.scores[winner]} Dominion points.`:reason==='imperial'?`${f.short} controls ${enemyCapitalsHeld(war,winner)} enemy capitals.`:'All rival kings have fallen.';
  return {title,description:`${explanation} Banners captured: ${stats.banners||0} · Capitals captured: ${stats.capitals||0} · Kings defeated: ${stats.kings||0} · Final territory: ${territory} · Enemy pieces captured: ${captures}`};
}

export function drawWarObjectives(ctx,war,factions,cell,time,{upright=()=>{},reduceMotion=false,bounds=null}={}){
  if(!war)return;
  for(const o of war.objectives){
    if(bounds&&(o.x<bounds.left-4||o.x>bounds.right+4||o.y<bounds.top-4||o.y>bounds.bottom+4))continue;
    const f=factions[o.owner],color=f?.color||'#b2c1d6',contested=o.status==='contested';
    const pulse=reduceMotion?1:.75+.25*Math.sin(time*4+o.x);
    const x=(o.x+.5)*cell,y=(o.y+.5)*cell,r=o.radius*cell;
    ctx.save();ctx.fillStyle=color;ctx.globalAlpha=.07;ctx.beginPath();ctx.arc(x,y,r,0,Math.PI*2);ctx.fill();
    ctx.globalAlpha=contested?pulse:.6;ctx.strokeStyle=contested?'#ff7272':color;ctx.lineWidth=contested||o.general&&o.tower?4:2;
    ctx.setLineDash(contested?[10,7]:[]);ctx.beginPath();ctx.arc(x,y,r,0,Math.PI*2);ctx.stroke();ctx.setLineDash([]);
    if(o.progress>0){ctx.strokeStyle=factions[o.claimant]?.color||'#fff';ctx.lineWidth=5;ctx.beginPath();ctx.arc(x,y,r,-Math.PI/2,-Math.PI/2+Math.PI*2*o.progress);ctx.stroke();}
    ctx.globalAlpha=1;ctx.translate(x,y);upright(ctx);
    if(o.general||o.tower){ctx.fillStyle='#182334';ctx.strokeStyle=color;ctx.lineWidth=3;ctx.fillRect(-23,-8,46,15);ctx.strokeRect(-23,-8,46,15);if(o.general&&o.tower){ctx.fillStyle=color;for(const xx of [-23,-8,8,20])ctx.fillRect(xx,-15,5,8);}}
    if(o.kind==='capital'){
      ctx.fillStyle='#101b2b';ctx.strokeStyle=color;ctx.lineWidth=3;ctx.fillRect(-20,-31,40,36);ctx.strokeRect(-20,-31,40,36);
      for(const xx of [-24,12]){ctx.fillStyle=color;ctx.fillRect(xx,-39,12,42);for(let i=0;i<3;i++)ctx.fillRect(xx+i*4,-44,3,7);}
      ctx.fillStyle=color;ctx.font='21px serif';ctx.textAlign='center';ctx.fillText('♛',0,-11);
    }
    const pole=o.kind==='capital'?-58:-48;
    ctx.strokeStyle='#e5eaf3';ctx.lineWidth=3;ctx.beginPath();ctx.moveTo(0,3);ctx.lineTo(0,pole);ctx.stroke();
    const flutter=reduceMotion?0:Math.sin(time*4+o.y)*4;
    ctx.fillStyle=color;ctx.beginPath();ctx.moveTo(1,pole);ctx.lineTo(29,pole+4+flutter);ctx.lineTo(24,pole+17+flutter);ctx.lineTo(1,pole+13);ctx.closePath();ctx.fill();
    if(o.central){ctx.fillStyle='#fff';ctx.font='12px serif';ctx.fillText('★',14,pole+12);}
    ctx.fillStyle='rgba(4,8,16,.86)';ctx.fillRect(-53,10,106,19);ctx.font='10px system-ui';ctx.textAlign='center';ctx.fillStyle=contested?'#ff9494':color;
    ctx.fillText(contested?'⚔ CONTESTED':o.claimant!=null?`${Math.round(o.progress*100)}% CAPTURING`:o.kind==='capital'?`${factions[o.home]?.short} CAPITAL`:o.central?'CENTER · +2':'BANNER · +1',0,23);
    ctx.restore();
  }
}

export function drawExposedKings(ctx,war,pieces,factions,cell,time,{upright=()=>{},reduceMotion=false}={}){
  for(const p of pieces){
    if(!p.alive||!capitalKingExposed(war,p))continue;
    ctx.save();ctx.translate((p.x+.5)*cell,(p.y+.5)*cell);upright(ctx);
    ctx.strokeStyle='#ffbc70';ctx.lineWidth=3;ctx.globalAlpha=reduceMotion?1:.65+.35*Math.sin(time*3)**2;
    ctx.beginPath();ctx.arc(0,0,cell*.62,0,Math.PI*2);ctx.stroke();
    ctx.font='bold 11px system-ui';ctx.fillStyle='#ffce93';ctx.textAlign='center';ctx.fillText('KING EXPOSED',0,cell*.94);ctx.restore();
  }
}

export function drawWarMinimap(ctx,war,factions,sx,sy,time){
  if(!war)return;
  ctx.save();ctx.globalAlpha=1;
  for(const o of war.objectives){
    const x=(o.x+.5)*sx,y=(o.y+.5)*sy;
    ctx.fillStyle=o.owner==null?'#d1dbea':factions[o.owner]?.color||'#fff';ctx.strokeStyle='#07101e';ctx.lineWidth=2;
    if(o.kind==='capital'){ctx.fillRect(x-4,y-4,8,8);ctx.strokeRect(x-4,y-4,8,8);}
    else {ctx.beginPath();ctx.moveTo(x,y-5);ctx.lineTo(x+4,y);ctx.lineTo(x,y+5);ctx.lineTo(x-4,y);ctx.closePath();ctx.fill();ctx.stroke();}
    if(o.status==='contested'||o.status==='capturing'){ctx.strokeStyle=o.status==='contested'?'#ff7070':'#fff';ctx.lineWidth=1.5;ctx.beginPath();ctx.arc(x,y,7,0,Math.PI*2);ctx.stroke();}
  }
  ctx.restore();
}
