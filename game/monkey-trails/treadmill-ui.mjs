// Direction is a heading, never a movement command. Only detected steps earn distance.
const DIRECTIONS=[
  ['N','North',0,1],['NE','Northeast',Math.SQRT1_2,Math.SQRT1_2],
  ['E','East',1,0],['SE','Southeast',Math.SQRT1_2,-Math.SQRT1_2],
  ['S','South',0,-1],['SW','Southwest',-Math.SQRT1_2,-Math.SQRT1_2],
  ['W','West',-1,0],['NW','Northwest',-Math.SQRT1_2,Math.SQRT1_2]
];
const controllers=new WeakMap();
const stepCount=steps=>Math.max(0,Math.floor(Number.isFinite(steps)?steps:0));

export function normalizeHeading(east=0,north=1){
  const length=Math.hypot(east,north);
  return Number.isFinite(length)&&length>0?{east:east/length,north:north/length}:{east:0,north:1};
}

export function compassHeading(east=0,north=1){
  const heading=normalizeHeading(east,north);
  const degrees=(Math.atan2(heading.east,heading.north)*180/Math.PI+360)%360;
  const [short,label]=DIRECTIONS[Math.round(degrees/45)%8];
  return {short,label,degrees};
}

export function treadmillControls({east=0,north=1,steps=0,active=false}={}){
  const heading=normalizeHeading(east,north),direction=compassHeading(heading.east,heading.north);
  return `<section class="treadmill-controls" data-treadmill-controls aria-label="Treadmill direction and steps"><div class="treadmill-steering"><div class="treadmill-stick" data-treadmill-stick tabindex="0" role="group" aria-label="Direction stick, ${direction.label}. Slide or use arrow keys to choose a heading." style="--treadmill-east:${heading.east};--treadmill-north:${heading.north};--treadmill-angle:${direction.degrees}deg"><span class="treadmill-bearing treadmill-north" aria-hidden="true">N</span><span class="treadmill-bearing treadmill-east" aria-hidden="true">E</span><span class="treadmill-bearing treadmill-south" aria-hidden="true">S</span><span class="treadmill-bearing treadmill-west" aria-hidden="true">W</span><span class="treadmill-stick-ring" aria-hidden="true"></span><span class="treadmill-stick-knob" aria-hidden="true"><svg viewBox="0 0 24 24" focusable="false"><path d="m12 4 6 15-6-3-6 3Z"/></svg></span></div><div class="treadmill-readout"><span class="treadmill-eyebrow">WALK TOWARD</span><strong data-treadmill-heading>${direction.label}<small>${direction.short}</small></strong><span class="treadmill-step-line"><b data-treadmill-steps>${stepCount(steps).toLocaleString()}</b> steps this session</span><span class="treadmill-tracking" data-treadmill-active>${active?'Counting your steps':'Ready when you are'}</span></div></div><div class="treadmill-directions" aria-label="Choose a direction">${DIRECTIONS.filter((_,i)=>i%2===0).map(([short,label,x,y])=>`<button type="button" data-treadmill-direction="${short}" data-east="${x}" data-north="${y}" aria-label="Walk ${label}" aria-pressed="${direction.short===short}">${short}</button>`).join('')}</div><p class="treadmill-help">Slide to choose a heading. Each step moves you that way. Carry your phone as you walk; keep the game visible.</p></section>`;
}

export function refreshTreadmillControls(root,{east=0,north=1,steps,active}={}){
  const heading=normalizeHeading(east,north),direction=compassHeading(heading.east,heading.north);
  for(const card of root.querySelectorAll('[data-treadmill-controls]')){
    const stick=card.querySelector('[data-treadmill-stick]');
    stick.style.setProperty('--treadmill-east',heading.east);
    stick.style.setProperty('--treadmill-north',heading.north);
    stick.style.setProperty('--treadmill-angle',`${direction.degrees}deg`);
    stick.setAttribute('aria-label',`Direction stick, ${direction.label}. Slide or use arrow keys to choose a heading.`);
    const label=card.querySelector('[data-treadmill-heading]');
    // Keep heading text independent of the step readout and tracking status.
    label.firstChild.textContent=direction.label;
    label.querySelector('small').textContent=direction.short;
    if(steps!==undefined)card.querySelector('[data-treadmill-steps]').textContent=stepCount(steps).toLocaleString();
    if(active!==undefined)card.querySelector('[data-treadmill-active]').textContent=active?'Counting your steps':'Ready when you are';
    for(const button of card.querySelectorAll('[data-treadmill-direction]'))button.setAttribute('aria-pressed',String(button.dataset.treadmillDirection===direction.short));
  }
}

export function attachTreadmillControls(root,{getHeading=()=>({east:0,north:1}),onHeading=()=>{}}={}){
  // A stable root owns pointer capture because walking may replace card markup.
  const existing=controllers.get(root);
  if(existing){existing.getHeading=getHeading;existing.onHeading=onHeading;return existing.detach;}
  const controller={getHeading,onHeading,pointer:null,geometry:null,detach:null};
  const apply=(east,north)=>{
    const heading=normalizeHeading(east,north);
    controller.onHeading(heading);
    refreshTreadmillControls(root,heading);
  };
  const choose=event=>{
    const geometry=controller.geometry;
    if(!geometry)return;
    const east=event.clientX-geometry.x,north=geometry.y-event.clientY;
    // Touching the center preserves the current heading instead of jumping north.
    if(Math.hypot(east,north)<8)return;
    apply(east,north);
  };
  const down=event=>{
    const stick=event.target.closest?.('[data-treadmill-stick]');
    if(!stick||!root.contains(stick)||controller.pointer!==null||event.isPrimary===false||event.button!==0)return;
    event.preventDefault();
    const bounds=stick.getBoundingClientRect();
    controller.pointer=event.pointerId;controller.geometry={x:bounds.left+bounds.width/2,y:bounds.top+bounds.height/2};
    stick.focus({preventScroll:true});
    try{root.setPointerCapture(event.pointerId);}catch{}
    choose(event);
  };
  const move=event=>{
    if(event.pointerId!==controller.pointer)return;
    event.preventDefault();choose(event);
  };
  const end=event=>{
    if(event.pointerId!==controller.pointer)return;
    const pointer=controller.pointer;controller.pointer=null;controller.geometry=null;
    try{if(root.hasPointerCapture(pointer))root.releasePointerCapture(pointer);}catch{}
    // The selected heading deliberately stays in place after release/cancellation.
  };
  const key=event=>{
    if(!event.target.closest?.('[data-treadmill-stick]'))return;
    const row={ArrowUp:[0,1],ArrowRight:[1,0],ArrowDown:[0,-1],ArrowLeft:[-1,0]}[event.key];
    if(!row)return;
    event.preventDefault();apply(...row);
  };
  const click=event=>{
    const button=event.target.closest?.('[data-treadmill-direction]');
    if(!button||!root.contains(button))return;
    event.preventDefault();apply(Number(button.dataset.east),Number(button.dataset.north));
  };
  const listeners=[['pointerdown',down],['pointermove',move],['pointerup',end],['pointercancel',end],['lostpointercapture',end],['keydown',key],['click',click]];
  for(const [type,listener]of listeners)root.addEventListener(type,listener);
  controller.detach=()=>{
    if(controller.pointer!==null)end({pointerId:controller.pointer});
    for(const [type,listener]of listeners)root.removeEventListener(type,listener);
    controllers.delete(root);
  };
  controllers.set(root,controller);
  refreshTreadmillControls(root,controller.getHeading());
  return controller.detach;
}
