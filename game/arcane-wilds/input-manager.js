'use strict';
/* Action input owns device arbitration and short-lived intent, never spell ownership. */
(() => {
  if(window.AWInput)return;
  const defaults={up:'KeyW',down:'KeyS',left:'KeyA',right:'KeyD',dodge:'Space',interact:'KeyE',spell1:'Digit1',spell2:'Digit2',spell3:'Digit3',spell4:'Digit4',spell5:'Digit5',inventory:'KeyI',pause:'Escape'};
  const padDefaults={interact:0,spell1:2,spell2:3,spell3:1,spell4:4,spell5:5,dodge:7,inventory:8,pause:9};
  const held=new Set(),queue=new Map();
  let priorButtons=[],padIndex=null,binding=null,installed=false,device='keyboard',family='xbox';
  const move={x:0,y:0},aim={x:0,y:0,active:false};
  const settings=()=>window.AWPresentation.settings;
  function useDevice(next){device=next;if(next!=='mouse')mouse.active=false;}
  function blocked(){return !running||!game.player||paused||modalPause||roomTransition||document.hidden||window.AWPresentation.cinematic;}
  function clear(){held.clear();keys.clear();queue.clear();move.x=move.y=0;aim.x=aim.y=0;aim.active=false;for(const stick of [moveStick,aimStick]){stick.active=false;stick.x=stick.y=0;stick.pointer=null;}for(const id of ['moveKnob','aimKnob']){const el=$(id);if(el)el.style.transform='translate(-50%,-50%)';}}
  function radial(x,y,deadzone){const length=Math.hypot(x,y);if(length<=deadzone)return {x:0,y:0};const amount=Math.min(1,(length-deadzone)/(1-deadzone));return {x:x/length*amount,y:y/length*amount};}
  function mapping(){return {...defaults,...settings().keys};}
  function padMapping(){return {...padDefaults,...settings().buttons};}
  function press(action){
    window.AWPresentation.audio.unlock();
    if(action==='pause'){clear();if(document.getElementById('awSettings')&&!document.getElementById('awSettings').classList.contains('hidden')){window.AWModernUI?.closeSettings();return;}togglePause();return;}
    if(blocked())return;
    if(action==='inventory'){clear();renderInventory();showOverlay('inventoryOverlay');return;}
    if(action==='interact'){window.AWPresentation.event('interact',{});if(window.AWModernUI?.groundTarget())window.AWModernUI.inspectLoot();else interact();return;}
    if(action!=='dodge'&&!/^spell[1-5]$/.test(action))return;
    const slot=Number(action.slice(5))-1;
    const id=action==='dodge'?null:game.player.activeSpells[slot];
    if(action!=='dodge'&&!id)return;
    // Holding cannot extend a stale intent; each physical press replaces it once.
    queue.set(action,{expires:performance.now()+150,id,player:game.player,room:game.roomData});
    window.AWModernUI?.queued(action);
    flush();
  }
  function flush(){
    if(blocked()){queue.clear();return;}
    const now=performance.now();
    for(const [action,intent] of queue){
      if(now>intent.expires||intent.player!==game.player||intent.room!==game.roomData){queue.delete(action);continue;}
      const slot=Number(action.slice(5))-1;
      if(action==='dodge'){
        if(game.player.dodgeCd<=0){queue.delete(action);dodge();}
      }else{
        if(game.player.activeSpells[slot]!==intent.id){queue.delete(action);continue;}
        if((game.player.spellState[intent.id]?.cd||0)<=0){queue.delete(action);castSpell(slot);}
      }
    }
  }
  function poll(){
    const pads=typeof navigator.getGamepads==='function'?navigator.getGamepads():[];
    const pad=Array.from(pads||[]).find(p=>p&&p.connected!==false);
    let pm={x:0,y:0},pa={x:0,y:0};
    if(pad){
      if(padIndex!==pad.index){priorButtons=[];padIndex=pad.index;}
      family=/playstation|dualsense|dualshock|054c/i.test(pad.id)?'playstation':pad.mapping==='standard'?'xbox':'generic';
      pm=radial(pad.axes[0]||0,pad.axes[1]||0,settings().deadzone);
      pa=radial(pad.axes[2]||0,pad.axes[3]||0,settings().deadzone);
      const buttons=pad.buttons.map(b=>!!b.pressed||b.value>.55);
      if(pm.x||pm.y||pa.x||pa.y||buttons.some((v,i)=>v&&!priorButtons[i]))useDevice('gamepad');
      const previous=priorButtons;priorButtons=buttons;
      if(binding?.kind==='buttons'){
        const index=buttons.findIndex((v,i)=>v&&!previous[i]);if(index>=0){remap('buttons',binding.action,index);binding=null;window.AWModernUI?.refreshSettings();}
      }else if(!window.AWModernUI?.menuGamepad(buttons,previous))for(const [action,index] of Object.entries(padMapping()))if(buttons[index]&&!previous[index])press(action);
    }else{padIndex=null;priorButtons=[];}
    if(blocked()){move.x=move.y=0;aim.active=false;queue.clear();return;}
    const map=mapping();
    const kx=Number(held.has(map.right)||held.has('ArrowRight'))-Number(held.has(map.left)||held.has('ArrowLeft'));
    const ky=Number(held.has(map.down)||held.has('ArrowDown'))-Number(held.has(map.up)||held.has('ArrowUp'));
    const km=radial(kx,ky,0);
    const chosen=moveStick.active?radial(moveStick.x,moveStick.y,.06):(device==='gamepad'?(pm.x||pm.y?pm:km):km);
    move.x=chosen.x;move.y=chosen.y;
    const a=aimStick.active?radial(aimStick.x,aimStick.y,.08):(device==='gamepad'?pa:{x:0,y:0});
    const blend=clamp(settings().aimSensitivity*.35,.1,1);
    aim.x=lerp(aim.x,a.x,blend);aim.y=lerp(aim.y,a.y,blend);aim.active=!!(a.x||a.y);
  }
  function remap(kind,action,value){
    const map=kind==='keys'?mapping():padMapping();
    if(!(action in map))return;
    const old=map[action],other=Object.keys(map).find(k=>k!==action&&map[k]===value);
    if(other)map[other]=old;map[action]=value;settings()[kind]=map;window.AWPresentation.saveSettings();clear();
  }
  function prompt(action){
    if(device==='touch')return action==='interact'?'USE':action==='dodge'?'DODGE':action.replace('spell','');
    if(device==='gamepad'){
      const i=padMapping()[action],labels=family==='playstation'?['Cross','Circle','Square','Triangle','L1','R1','L2','R2','Share','Options']:['A','B','X','Y','LB','RB','LT','RT','View','Menu'];
      return family==='generic'?`B${i+1}`:labels[i]||`B${i+1}`;
    }
    return (mapping()[action]||action).replace('Key','').replace('Digit','').replace('Arrow','');
  }
  function install(){
    if(installed)return;installed=true;
    addEventListener('keydown',e=>{
      if(e.ctrlKey||e.metaKey||e.altKey)return;
      if(binding?.kind==='keys'){e.preventDefault();if(!e.repeat){remap('keys',binding.action,e.code);binding=null;window.AWModernUI?.refreshSettings();}return;}
      if(/INPUT|SELECT|TEXTAREA/.test(e.target?.tagName)||e.target?.isContentEditable)return;
      const map=mapping(),action=Object.keys(map).find(a=>map[a]===e.code);
      if(action||e.code.startsWith('Arrow')){e.preventDefault();held.add(e.code);useDevice('keyboard');if(!e.repeat&&action&&!['up','down','left','right'].includes(action))press(action);}
    });
    addEventListener('keyup',e=>held.delete(e.code));
    addEventListener('blur',clear);document.addEventListener('visibilitychange',clear);
    addEventListener('gamepaddisconnected',()=>{priorButtons=[];padIndex=null;clear();});
    canvas.addEventListener('mousemove',e=>{mouse.x=e.clientX;mouse.y=e.clientY;mouse.active=true;device='mouse';});
    canvas.addEventListener('mouseleave',()=>mouse.active=false);
    canvas.addEventListener('contextmenu',e=>{e.preventDefault();press('dodge');});
    document.addEventListener('pointerdown',()=>window.AWPresentation.audio.unlock(),{passive:true});
    if(typeof requestAnimationFrame==='function'){const menuPoll=()=>{if(!running)poll();requestAnimationFrame(menuPoll);};requestAnimationFrame(menuPoll);}
  }
  window.AWInput={move,aim,press,poll,flush,clear,install,prompt,useDevice,radial,remap,mapping,padMapping,queue,get device(){return device;},get binding(){return binding;},bind(kind,action){clear();binding={kind,action};},cancelBinding(){binding=null;}};
})();
