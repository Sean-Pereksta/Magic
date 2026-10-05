'use strict';
/* Presentation only: one reusable HUD line and canvas badges, no per-NPC DOM. */
(() => {
  const S=AWServices,models=new WeakMap(),placed=[];
  const services=document.createElement('div');services.id='awNearbyServices';services.className='aw-nearby-services hidden';$('roomSub').after(services);
  let summary='',summaryNode=null,lastSummary=0;
  function actionName(o){
    if(!o)return '';
    if(o.npc)return S.forNPC(o)[0].action;
    if(o.type==='homeObject'){
      const h=AWHome.state();
      if(o.homeKind==='house')return h.tier?'Enter Cottage':'Build Cottage';
      if(o.homeKind==='board')return h.tier?'Homestead Plans':'Build Cottage';
      if(o.homeKind==='plot'){
        const source=h.plots.find(p=>p.id===o.homeId),p=source?AWHomeCore.clone(source):null;if(p)AWHomeCore.grow(p,AWHome.now());
        return p&&AWHomeCore.ready(p)?'Harvest':p?.plant&&p.plant.wateredUntil<=AWHome.now()?'Water':p?.plant?'Tend Crop':'Plant Seeds';
      }
      return {well:'Refill Water',exit:'Leave Cottage',return:'Return to Adventure',item:'Inspect Furnishing'}[o.homeKind]||'View Plans';
    }
    return {groundLoot:'Inspect Loot',chest:'Open Chest',forge:'Forge',well:'Heal',seer:'View Spells',villagePortal:'Use Portal',continentPortal:'Use Portal',shadowPortal:'Use Portal',waystone:'Use Waystone',campaignSite:'Explore',shadowSite:'Explore'}[o.type]||o.label||'Interact';
  }
  const describe=o=>o.npc?`${actionName(o)} · ${o.name}`:o.homeKind==='house'&&!AWHome.state().tier?'Build Cottage · Future Cottage':actionName(o);
  function refreshServices(){
    if(!running||!game.player)return;
    const now=performance.now(),n=AWCampaign.current();if(n===summaryNode&&now-lastSummary<250)return;summaryNode=n;lastSummary=now;
    const list=n?.town?S.forTown(n):[];
    // Local quest markers reflect completed/accepted/ready work, just like the NPC.
    const keeper=game.interactables.find(o=>o.npc&&S.capabilities(AWCampaignData.towns[o.campaignTown],o.role)?.quest);
    const quest=n&&keeper?S.forNPC(keeper)[0]:null;
    const next=list.filter(b=>b.id!=='quest'||quest?.id!=='talk').map(b=>b.id==='quest'&&quest?quest:b).map(b=>b.icon+' '+(b.id==='supplies'?'Supplies':b.label)).join(' · ');
    if(next!==summary){summary=next;services.textContent=next?'Services: '+next:'';services.title=list.map(b=>b.label).join(' · ');}
    services.classList.toggle('hidden',!next);
  }
  function model(n){
    let m=models.get(n);if(!m){m={npc:n,at:-Infinity,badges:[],x:0,y:0,w:0,h:24};models.set(n,m);}
    if(performance.now()-m.at>250){m.badges=S.forNPC(n);m.at=performance.now();}
    return m;
  }
  function overlaps(a,b){return Math.abs(a.x-b.x)<(a.w+b.w)/2+5&&Math.abs(a.y-b.y)<(a.h+b.h)/2+4;}
  function drawBadges(target){
    placed.length=0;ctx.save();ctx.textAlign='center';ctx.textBaseline='middle';
    for(let index=-1;index<game.interactables.length;index++){
      const n=index===-1?target:game.interactables[index];if(!n?.npc||index>=0&&n===target)continue;const p=worldToScreen(n.x,n.y);if(p.x<-30||p.x>W+30||p.y<-30||p.y>H+80)continue;
      const m=model(n),selected=target===n,near=dist(n,game.player)<3,shown=m.badges.slice(0,2);
      const symbols=shown.map(b=>b.icon).join(' '),label=near?m.badges.map(b=>b.label).join(' / '):'';
      ctx.font='11px system-ui';m.w=Math.max(28,Math.min(260,ctx.measureText(label).width+18),shown.length*22);m.h=near?43:26;m.x=clamp(p.x,m.w/2+5,W-m.w/2-5);m.y=clamp(p.y-77,m.h/2+5,H-m.h/2-5);
      // Resolve crowded badges in screen space, reconnecting displaced badges to their NPC.
      const preferredY=m.y;let below=false;
      for(let pass=0;pass<placed.length*2+2;pass++){
        const hit=placed.find(b=>overlaps(m,b));if(!hit)break;
        m.y=hit.y+(below?1:-1)*((hit.h+m.h)/2+5);
        if(m.y-m.h/2<5){below=true;m.y=preferredY;}
      }
      placed.push(m);ctx.globalAlpha=selected?1:near?.93:.52;
      if(m.y<p.y-80){ctx.strokeStyle='#c8d6bd';ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(p.x,p.y-48);ctx.lineTo(m.x,m.y+m.h/2);ctx.stroke();}
      ctx.fillStyle=selected?'#294e49':'#102327';ctx.strokeStyle=selected?'#f1dda1':'#617e70';ctx.lineWidth=selected?2:1;
      ctx.beginPath();ctx.roundRect(m.x-m.w/2,m.y-m.h/2,m.w,m.h,7);ctx.fill();ctx.stroke();
      ctx.fillStyle='#f3e9c7';ctx.font=(selected?'bold 19px':'17px')+' system-ui';ctx.fillText(symbols,m.x,m.y-(near?8:0));
      if(near){ctx.fillStyle='#e0e9d9';ctx.font='11px system-ui';ctx.fillText(label,m.x,m.y+12,m.w-12);}
    }
    ctx.restore();
  }
  function drawTarget(o){
    if(!o)return;const p=worldToScreen(o.x,o.y);ctx.save();ctx.strokeStyle='#f4dfa0';ctx.fillStyle='rgba(244,223,160,.12)';ctx.lineWidth=2;
    ctx.beginPath();ctx.ellipse(p.x,p.y,25,11,0,0,TAU);ctx.fill();ctx.stroke();ctx.restore();
  }
  const baseHUD=updateHUD;updateHUD=function(...args){const result=baseHUD(...args);refreshServices();return result;};
  const baseRender=render;render=function(){const result=baseRender();if(!running||!game.player||paused||modalPause||AWPresentation.cinematic)return result;
    const target=AWModernUI.groundTarget()||currentInteraction();drawTarget(target);drawBadges(target);return result;
  };
  // The same explicit Close action serves Escape and controller Back for service menus.
  document.addEventListener('keydown',e=>{
    if(e.key!=='Escape'||AWInput.binding)return;
    const overlay=[...document.querySelectorAll('.overlay:not(.hidden)')].at(-1);
    const close=overlay?.querySelector('[data-close],[data-exp-close]');
    if(close){e.preventDefault();e.stopImmediatePropagation();close.click();}
  },true);
  window.AWUsability={actionName,describe,refreshServices};
})();
