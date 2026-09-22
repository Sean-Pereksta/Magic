/* Load the isolated screen tuning before the player can enter or start gameplay. */
(()=>{
  'use strict';
  const source='receiver-window-qb/screen-playability.js';
  if(document.querySelector(`script[src="${source}"]`))return;
  const gates=['startBtn','continueBtn','snapBtn'].map(id=>document.getElementById(id)).filter(Boolean).map(el=>({el,disabled:el.disabled}));
  gates.forEach(({el})=>{el.disabled=true;});
  const script=document.createElement('script');script.src=source;script.async=false;
  const release=()=>gates.forEach(({el,disabled})=>{el.disabled=disabled;});script.onload=release;script.onerror=release;
  const anchor=document.currentScript;
  if(anchor?.parentNode)anchor.parentNode.insertBefore(script,anchor.nextSibling);else document.head.appendChild(script);
})();

/* Keyboard navigation is scoped to the top visible menu, never live aiming. */
(()=>{
  'use strict';
  const visible=el=>el&&!el.hidden&&el.getClientRects().length&&getComputedStyle(el).visibility!=='hidden';
  function scope(){
    for(const id of ['franchiseLayer','saveLayer','managerLayer','startLayer','replayBar','audiblePanel']){const el=document.getElementById(id);if(visible(el))return el;}
    return document.body.dataset.phase==='call'?document.getElementById('hud'):null;
  }
  const controls=root=>[...root.querySelectorAll('button:not(:disabled),a[href],input,select,summary,[data-roster],[data-audible-receiver]')].filter(visible);
  function focus(el){el?.focus({preventScroll:true});el?.scrollIntoView({block:'nearest',inline:'nearest'});}
  // Restore the same logical control after training or roster selection rebuilds cards.
  document.addEventListener('keydown',e=>{
    const root=scope();if(!root||!['ArrowLeft','ArrowRight','ArrowUp','ArrowDown','Enter','Tab'].includes(e.key))return;
    if(root.id==='replayBar'&&e.key.startsWith('Arrow'))return;
    const list=controls(root);if(!list.length)return;
    const current=document.activeElement;
    if(['INPUT','SELECT'].includes(current?.tagName)&&e.key!=='Tab')return;
    e.preventDefault();e.stopImmediatePropagation();
    const index=list.indexOf(current);
    if(e.key==='Enter'){
      if(e.repeat)return;
      const target=index<0?list[0]:current,oldIndex=Math.max(0,index);target.click();
      const next=scope();if(next){const items=controls(next);if(!items.includes(document.activeElement))focus(items[Math.min(oldIndex,items.length-1)]);}
      return;
    }
    if(index<0){focus(list[0]);return;}
    if(e.key==='Tab'){focus(list[(index+(e.shiftKey?-1:1)+list.length)%list.length]);return;}
    const r=current.getBoundingClientRect(),cx=r.x+r.width/2,cy=r.y+r.height/2;
    const horizontal=e.key==='ArrowLeft'||e.key==='ArrowRight',sign=['ArrowLeft','ArrowUp'].includes(e.key)?-1:1;
    const options=list.filter(el=>el!==current).map(el=>{const b=el.getBoundingClientRect(),dx=b.x+b.width/2-cx,dy=b.y+b.height/2-cy;return {el,forward:(horizontal?dx:dy)*sign,across:Math.abs(horizontal?dy:dx)};}).filter(p=>p.forward>4).sort((a,b)=>(a.forward+a.across*3)-(b.forward+b.across*3));
    focus(options[0]?.el||list[(index+sign+list.length)%list.length]);
  },true);
  // Audible receiver rows are also keyboard targets.
  const observer=new MutationObserver(()=>{
    document.querySelectorAll('[data-audible-receiver]').forEach(el=>{el.tabIndex=0;el.setAttribute('role','button');});
  });observer.observe(document.getElementById('routes'),{childList:true});
})();
