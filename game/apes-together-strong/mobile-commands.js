/* Touch UI only: commands are dispatched by app.js through the existing game API. */
(() => {
'use strict';
const PRIMARY=[['nearestTarget','⚔️','All Charge'],['hold','✋','Hold Position'],['call','📣','Call Apes'],['recall','↩️','Recall'],['settlement','🏕️','Settlement']];
const SCOPES=[['recall','↩️','Nearby · R'],['recallField','↩️','All field · T'],['recallAll','🏘️','All + residents']];
class MobileCommands {
 constructor({canvas,hud,enabled,blocked,command,tap,drag,unlock,settlement,army,callHold}) {
  Object.assign(this,{canvas,hud,enabled,blocked,command,tap,drag,unlock,settlement,army,callHold});
  this.pointers=new Map();this.gesture=null;this.timer=null;this.scopeTimer=null;
  this.wheel=document.createElement('div');this.wheel.id='mobileCommandWheel';this.wheel.hidden=true;this.wheel.setAttribute('role','status');this.wheel.setAttribute('aria-live','polite');hud.append(this.wheel);
  this.panel=document.createElement('div');this.panel.id='mobileSettlementActions';this.panel.hidden=true;this.panel.setAttribute('role','group');this.panel.setAttribute('aria-label','Settlement and army actions');hud.append(this.panel);
  const actions=[['build','🏕️ Manage nearby hut / workshop'],['overview','Map & settlement management'],['settle','Found settlement · Z'],['settleAll','Settle all followers · Shift Z'],['patrol','Assign scouts · C'],['finder','Find settlements · V'],['army','More army controls'],['close','Close']];
  for(const [id,label] of actions){const b=document.createElement('button');b.type='button';b.className='button';b.textContent=label;b.dataset.mobileAction=id;b.addEventListener('click',()=>{this.panel.hidden=true;if(id!=='close')settlement(id)});this.panel.append(b)}
  this.base=document.createElement('button');this.base.id='mobileSettlementButton';this.base.className='button';this.base.hidden=true;this.base.setAttribute('aria-haspopup','dialog');this.base.addEventListener('click',()=>settlement('build'));hud.append(this.base);
  this.closeArmy=document.createElement('button');this.closeArmy.type='button';this.closeArmy.id='mobileCloseArmy';this.closeArmy.className='button';this.closeArmy.textContent='Close army controls';this.closeArmy.addEventListener('click',()=>this.toggleArmy(false));army.root.prepend(this.closeArmy);
  const style=document.createElement('style');style.textContent=`
   #gameCanvas,#touchControls{-webkit-touch-callout:none;user-select:none;-webkit-user-select:none;touch-action:none}
   #mobileCommandWheel{position:fixed;width:224px;height:224px;border-radius:50%;z-index:40;background:#0a211ff2;border:1px solid #c7b475;box-shadow:0 4px 20px #0008;pointer-events:none;color:#f2e7bf;font:11px/1.2 system-ui}
   #mobileCommandWheel[hidden],#mobileSettlementActions[hidden],#mobileSettlementButton[hidden]{display:none!important}
   .mobile-wheel-sector{position:absolute;inset:0;border-radius:50%;background:#213d35}.mobile-wheel-sector.selected{background:#777144}
   .mobile-wheel-label{position:absolute;width:72px;text-align:center;transform:translate(-50%,-50%);font-weight:600}.mobile-wheel-label b{display:block;font-size:22px;margin-bottom:3px}.mobile-wheel-label.selected{color:#fff}
   .mobile-wheel-center{position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);width:56px;height:56px;border-radius:50%;background:#091b1c;display:grid;place-content:center;text-align:center;font-size:10px;color:#c8d5ca}
   .mobile-wheel-hint{position:absolute;left:0;right:0;top:100%;margin-top:5px;padding:5px;border-radius:6px;background:#091b1cf2;text-align:center}
   #mobileSettlementActions{position:absolute;z-index:42;right:max(12px,env(safe-area-inset-right));bottom:110px;width:min(290px,calc(100vw - 24px));max-height:calc(100dvh - 160px);overflow:auto;padding:10px;border:1px solid #ad9c65;border-radius:12px;background:#0a211ff5;pointer-events:auto;display:grid;gap:6px}
   #mobileSettlementActions button{min-height:44px;white-space:normal}#mobileSettlementButton{display:none;position:absolute;right:max(16px,env(safe-area-inset-right));bottom:118px;z-index:8;min-height:44px;pointer-events:auto}#armyDock #mobileCloseArmy{display:none}
   @media(pointer:coarse),(max-width:800px){
    #armyDock:not(.mobile-open){display:none!important}#armyDock.mobile-open{max-height:calc(100dvh - 150px);overflow:auto;bottom:118px;width:min(360px,calc(100vw - 16px));background:#0a211ff5;padding:6px;border-radius:10px}#armyDock.mobile-open .army-commands{display:flex}#mobileCloseArmy{display:block!important;width:auto!important;min-height:44px!important}
    #mobileSettlementButton:not([hidden]){display:block}#mapButton{display:none}#joystick{bottom:max(18px,env(safe-area-inset-bottom))!important}#attackButton{bottom:max(26px,env(safe-area-inset-bottom))!important}
   }
  `;document.head.append(style);
  // A canceled captured touch can synthesize a click on a newly opened menu.
  // Suppress that click even after pause/rotation, but allow the next fresh tap.
  document.addEventListener('pointerdown',e=>{if(e.pointerType==='touch')this.suppressClick=false},true);
  document.addEventListener('click',e=>{if(this.suppressClick&&(e.pointerType==='touch'||e.sourceCapabilities?.firesTouchEvents)){e.preventDefault();e.stopImmediatePropagation();this.suppressClick=false}},true);
  for(const name of ['pointerdown','pointermove','pointerup','pointercancel','lostpointercapture'])canvas.addEventListener(name,e=>this.event(name,e),true);
  canvas.addEventListener('click',e=>{if(e.pointerType==='touch'){e.preventDefault();e.stopImmediatePropagation()}},true);
  for(const el of [canvas,document.getElementById('touchControls')])el.addEventListener('contextmenu',e=>e.preventDefault());
 }
 event(type,e){
  if(e.pointerType!=='touch')return;
  if(type==='pointerdown'){
   if(!this.enabled())return;this.suppressClick=true;e.preventDefault();e.stopImmediatePropagation();this.unlock();document.activeElement?.blur();this.canvas.setPointerCapture(e.pointerId);
   const point={pointerId:e.pointerId,clientX:e.clientX,clientY:e.clientY,button:0,pointerType:'touch'};this.pointers.set(e.pointerId,point);
   if(this.blocked()||!this.panel.hidden||this.army.root.classList.contains('mobile-open')){this.cancelGesture();return}
   if(this.pointers.size>1){const g=this.gesture;this.cancelGesture();if(g&&!g.open&&!g.dragging)this.drag('down',g.start);this.drag('down',point);return}
   this.gesture={id:e.pointerId,start:point,x:e.clientX,y:e.clientY,open:false,dragging:false,selected:-1,items:PRIMARY,offset:-Math.PI/2};
   this.timer=setTimeout(()=>{const g=this.gesture;if(!g||g.dragging||!this.enabled()||this.blocked())return;g.open=true;this.army.cancel();this.showWheel(g)},250);return;
  }
  if(!this.pointers.has(e.pointerId))return;
  e.preventDefault();e.stopImmediatePropagation();const g=this.gesture;
  if(type==='pointermove'){
   if(!this.enabled()||this.blocked()){this.cancel();return}
   if(g?.id===e.pointerId){g.x=e.clientX;g.y=e.clientY;if(g.open)this.select(g);else if(g.dragging||Math.hypot(g.x-g.start.clientX,g.y-g.start.clientY)>12){if(!g.dragging){clearTimeout(this.timer);g.dragging=true;this.drag('down',g.start)}this.drag('move',e)}}else this.drag('move',e);return;
  }
  this.pointers.delete(e.pointerId);
  if(type!=='pointerup'){this.cancelGesture();this.army.cancel();return}
  if(g?.id===e.pointerId){
   let action=null;if(g.open){g.x=e.clientX;g.y=e.clientY;this.select(g);if(g.selected>=0)action=g.items[g.selected][0];if(action==='nearestTarget'&&performance.now()-g.selectedAt>=280)action='nearestHuman'}
   const heldCall=action==='call'&&!!this.callHold;
   if(heldCall&&g.callToken&&this.enabled()&&!this.blocked()){this.callHold.release(g.callToken);delete g.callToken}
   this.cancelGesture();
   if(!this.enabled()||this.blocked())return;
   if(action==='settlement'){this.openSettlement()}else if(action&&!heldCall)this.command(action,g.start);else if(!g.open){if(g.dragging)this.drag('up',e);else this.tap(g.start,e)}
  }else this.drag('up',e);
 }
 showWheel(g){
  const r=112,pad=8;g.cx=Math.max(r+pad,Math.min(innerWidth-r-pad,g.start.clientX));g.cy=Math.max(r+pad,Math.min(innerHeight-r-45,g.start.clientY));
  this.wheel.style.left=(g.cx-r)+'px';this.wheel.style.top=(g.cy-r)+'px';this.wheel.hidden=false;this.render(g);
 }
 select(g){
  const dx=g.x-g.cx,dy=g.y-g.cy,n=g.items.length,step=Math.PI*2/n;
  const center=Math.hypot(dx,dy)<29||Math.hypot(g.x-g.start.clientX,g.y-g.start.clientY)<14;
  const selected=center?-1:Math.floor(((Math.atan2(dy,dx)-g.offset+step/2+Math.PI*4)%(Math.PI*2))/step);
  if(selected===g.selected)return;if(g.callToken){this.callHold.cancel(g.callToken);delete g.callToken}g.selected=selected;g.selectedAt=performance.now();clearTimeout(this.scopeTimer);this.render(g);
  if(g.items===PRIMARY&&selected>=0&&g.items[selected][0]==='call'&&this.callHold){const token='wheel:'+g.id;if(this.callHold.press(token))g.callToken=token}
  if(g.items===PRIMARY&&selected>=0&&g.items[selected][0]==='recall')this.scopeTimer=setTimeout(()=>{
   if(this.gesture!==g||g.selected!==selected)return;
   // Keep Nearby in the same direction: opening scopes never escalates a recall.
   g.offset=g.offset+selected*step;g.items=SCOPES;g.selected=0;this.render(g);
  },600);
 }
 render(g){
  this.wheel.replaceChildren();const n=g.items.length,step=Math.PI*2/n,r=112;
  g.items.forEach(([id,icon,label],i)=>{
   const angle=g.offset+i*step,points=['50% 50%'];for(let a=angle-step/2+.025;a<=angle+step/2-.025;a+=.035)points.push((50+50*Math.cos(a))+'% '+(50+50*Math.sin(a))+'%');
   const sector=document.createElement('div');sector.className='mobile-wheel-sector'+(i===g.selected?' selected':'');sector.style.clipPath='polygon('+points.join(',')+')';sector.dataset.command=id;this.wheel.append(sector);
   const text=document.createElement('div');text.className='mobile-wheel-label'+(i===g.selected?' selected':'');text.style.left=(r+76*Math.cos(angle))+'px';text.style.top=(r+76*Math.sin(angle))+'px';const b=document.createElement('b');b.textContent=icon;text.append(b,document.createTextNode(label));this.wheel.append(text);
  });
  const center=document.createElement('div');center.className='mobile-wheel-center';center.textContent='Cancel';this.wheel.append(center);
  const hint=document.createElement('div');hint.className='mobile-wheel-hint';const id=g.items[g.selected]?.[0];hint.textContent=g.items===SCOPES?'Choose recall scope · release to call':id==='call'?'Hold here to grow the call circle · 1.2s calls the entire horde':id==='recall'?'Release: nearby · hold here: recall scopes':id==='nearestTarget'?'Release: E · hold 0.28s: troops & vehicles':'Slide to choose · release to command';this.wheel.append(hint);
 }
 openSettlement(){this.cancelGesture();this.panel.hidden=false;this.panel.querySelector('[data-mobile-action=build]').disabled=this.base.hidden}
 toggleArmy(open){this.panel.hidden=true;const wasOpen=this.army.root.classList.contains('mobile-open');this.army.root.classList.toggle('mobile-open',open);if(open||wasOpen)this.army.root.classList.toggle('expanded',open)}
 update({near,workshop,playing}){this.base.hidden=!playing||!near&&!workshop;this.base.textContent=near?'🏕️ Settlement':'🛠️ Workshop';if(!playing)this.cancel()}
 cancelGesture(){clearTimeout(this.timer);clearTimeout(this.scopeTimer);if(this.gesture?.callToken)this.callHold?.cancel(this.gesture.callToken);this.gesture=null;this.wheel.hidden=true}
 cancel(){this.cancelGesture();this.pointers.clear();this.panel.hidden=true;this.toggleArmy(false)}
}
window.ATSMobileCommands=MobileCommands;
})();
