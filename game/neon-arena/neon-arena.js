(() => {
'use strict';
const canvas=document.getElementById('game'), ctx=canvas.getContext('2d');
// The arena itself is rendered into an offscreen WebGL depth buffer, then composited
// beneath the 2D HUD and effects. A real z-buffer removes the impossible painter-sort
// cases caused by intersecting floors, walls, platforms, and animated actors.
const worldCanvas=document.createElement('canvas'), gl=worldCanvas.getContext('webgl',{alpha:false,antialias:true,premultipliedAlpha:false});
if(!gl)throw new Error('WebGL is required for the depth-buffered arena renderer.');
const mm=document.getElementById('minimap'), mctx=mm.getContext('2d');
const hud={score:el('score'),ammo:el('ammo'),timer:el('timer'),botsLeft:el('botsLeft'),mode:el('modeLabel'),msg:el('msg'),feed:el('feed'),death:el('death'),healthText:el('healthText'),healthFill:el('healthFill'),armorText:el('armorText'),armorFill:el('armorFill'),upgradePoints:el('upgradePoints'),reloadHint:el('reloadHint'),weaponName:el('weaponName'),pickupPrompt:el('pickupPrompt')};
function el(id){return document.getElementById(id)}
let W=innerWidth,H=innerHeight,BASE_DPR=Math.min(2,devicePixelRatio||1),DPR=BASE_DPR;
const renderer={scale:1,avgMs:16.7,nextTune:0,stableTicks:0,minScale:.62,maxScale:1};
function resize(){W=innerWidth;H=innerHeight;DPR=BASE_DPR*renderer.scale;canvas.width=Math.max(1,Math.round(W*DPR));canvas.height=Math.max(1,Math.round(H*DPR));worldCanvas.width=Math.max(1,Math.round(W*DPR));worldCanvas.height=Math.max(1,Math.round(H*DPR));canvas.style.width=W+'px';canvas.style.height=H+'px';ctx.setTransform(DPR,0,0,DPR,0,0);gl.viewport(0,0,worldCanvas.width,worldCanvas.height)}
addEventListener('resize',resize); resize();
const TAU=Math.PI*2, GRAV=34, PLAYER_H=1.86, HEAD_CLEAR=.18, WORLD_SCALE=1.45, MAP_LIMIT=69;
const HUMAN_VISUAL_HEIGHT=1.15, HUMAN_VISUAL_WIDTH=1.10, ALIEN_SIZE_MULT=1.50;
const RAPTOR_VISUAL_SCALE=1.30, RAPTOR_KINDS=new Set(['blue','yellow','red','green','stripe','purple','climber']);
const ARENA_TILESETS={grid:'breach_ring',pastel:'black_ice_cryolab',reactor:'drowned_powerworks',terminal:'derelict_station',foundry:'drowned_powerworks',outpost:'xenofall_wastes'};
const JUMP_SPEED=23, JUMP_COYOTE=.11, JUMP_BUFFER=.14, GROUND_SNAP=.10, COLLISION_SKIN=.025;
const keys=Object.create(null);
const mobilePointerMedia=globalThis.matchMedia?.('(pointer: coarse)')||null;
function detectMobileDevice(){const ua=String(navigator.userAgent||''),mobileUA=!!navigator.userAgentData?.mobile||/Android|iPhone|iPad|iPod|Mobile|Silk|Kindle/i.test(ua),touch=(navigator.maxTouchPoints||0)>0,coarse=!!mobilePointerMedia?.matches,compact=Math.min(globalThis.screen?.width||innerWidth,globalThis.screen?.height||innerHeight)<=1024;return mobileUA||(touch&&coarse&&compact)}
const mobileInput={detected:detectMobileDevice(),visible:false,moveX:0,moveForward:0,lookX:0,lookY:0,lockTarget:null,aimThink:0,buildPointer:null,lockHudKey:'',stickResetters:[]};
addEventListener('keydown',e=>{
  if(game.shopOpen){if(e.code==='Escape'||e.code==='Enter'){e.preventDefault();closeSurvivorShop(true)}return;}
  if(e.code==='Tab'){if(!game.started&&el('controllerOverlay')?.style.display!=='flex')return;e.preventDefault();if(el('controllerOverlay')?.style.display==='flex'){setControllerTab(activeControllerTab===1?2:1);return;}if(game.started&&!e.repeat)toggleUpgradeTree();return;}
  if(e.code==='KeyQ'&&game.started&&!game.upgradesOpen&&!e.repeat){e.preventDefault();toggleBuildType(player);return;}
  if(e.code==='KeyR'&&game.started&&!e.repeat&&game.p1Input!=='controller'){e.preventDefault();manualReload(player);return;}
  if(e.code==='KeyF'&&game.started&&!e.repeat&&!game.upgradesOpen&&game.p1Input!=='controller'){e.preventDefault();interactWeapon(player);return;}
  if(e.code==='Space'&&game.started&&game.p1Input==='keyboard'){e.preventDefault();if(!e.repeat)requestHumanJump(player)}
  if(e.code==='Numpad0'&&game.started&&game.p2Input==='keyboard'&&player2){e.preventDefault();if(!e.repeat)requestHumanJump(player2)}
  if((e.code==='ShiftLeft'||e.code==='ShiftRight')&&game.started&&game.p1Input==='keyboard'){e.preventDefault();if(!e.repeat)requestHumanSlide(player)}
  if(e.code==='NumpadDecimal'&&game.started&&game.p2Input==='keyboard'&&player2){e.preventDefault();if(!e.repeat)requestHumanSlide(player2)}
  keys[e.code]=true;
});
addEventListener('keyup',e=>{if(e.code==='Tab'||e.code==='KeyR')e.preventDefault();keys[e.code]=false});
let pointer=false, mouseDX=0, mouseDY=0, fireHeld=false, controllerFireHeld=false, buildHeld=false, buildPreview=null;
function makePadState(){return {pad:null,moveX:0,moveZ:0,lookX:0,lookY:0,prev:{},label:'UNASSIGNED'}}
const controller={connected:false,players:{1:makePadState(),2:makePadState()},settings:{shoot:5,build:6,toggle:4,reload:2,pickup:1,jump:0,slide:10,tab:3,lookSensitivity:3.0,deadzone:.14,p1Pad:'auto',p2Pad:'auto'}};
function setControllerTab(n){activeControllerTab=n;el('controllerTabP1')?.classList.toggle('active',n===1);el('controllerTabP2')?.classList.toggle('active',n===2)}
function connectedGamepads(){return Array.from(navigator.getGamepads?.()||[]).filter(Boolean)}
function refreshControllerOptions(){const pads=connectedGamepads();for(const [id,key,fallback] of [['p1Pad','p1Pad','Auto — first available'],['p2Pad','p2Pad','Auto — next available']]){const select=el(id);if(!select)continue;const wanted=String(controller.settings[key]??'auto');select.innerHTML='';const auto=document.createElement('option');auto.value='auto';auto.textContent=fallback;select.appendChild(auto);for(const pad of pads){const option=document.createElement('option');option.value=String(pad.index);option.textContent='Controller '+(pad.index+1)+' — '+String(pad.id||'Gamepad').slice(0,46);select.appendChild(option)}select.value=[...select.options].some(o=>o.value===wanted)?wanted:'auto'}}
function loadControllerSettings(){try{const saved=JSON.parse(localStorage.getItem('neonArenaController')||'{}');Object.assign(controller.settings,saved);if(saved.p1Input)el('p1Input').value=saved.p1Input;if(saved.p2Input){el('p2Input').value=saved.p2Input;const sp=el('secondPlayer');if(sp)sp.value=saved.p2Input}}catch{};refreshControllerOptions();for(const [id,key] of [['padShoot','shoot'],['padBuild','build'],['padToggle','toggle'],['padReload','reload'],['padPickup','pickup'],['padJump','jump'],['padSlide','slide'],['padTab','tab'],['padLookSensitivity','lookSensitivity'],['padDeadzone','deadzone'],['p1Pad','p1Pad'],['p2Pad','p2Pad']]){const node=el(id);if(node)node.value=String(controller.settings[key]??node.value)}}
function saveControllerSettings(){for(const [id,key] of [['padShoot','shoot'],['padBuild','build'],['padToggle','toggle'],['padReload','reload'],['padPickup','pickup'],['padJump','jump'],['padSlide','slide'],['padTab','tab']])controller.settings[key]=+el(id).value;controller.settings.lookSensitivity=+el('padLookSensitivity').value;controller.settings.deadzone=+el('padDeadzone').value;controller.settings.p1Pad=el('p1Pad')?.value||'auto';controller.settings.p2Pad=el('p2Pad')?.value||'auto';controller.settings.p1Input=el('p1Input')?.value||'keyboard';controller.settings.p2Input=el('p2Input')?.value||'off';const sp=el('secondPlayer');if(sp)sp.value=controller.settings.p2Input;localStorage.setItem('neonArenaController',JSON.stringify(controller.settings));if(game.started){game.p1Input=controller.settings.p1Input;game.p2Input=controller.settings.p2Input;game.secondPlayer=game.p2Input!=='off';game.secondInput=game.p2Input;if(game.secondPlayer&&!player2){player2=makeHumanPlayer('PLAYER 2','#ff9d2e','p2');player2.upgrades=createUpgradeState();spawn(player2);bots.push(player2)}else if(!game.secondPlayer&&player2){bots=bots.filter(b=>b!==player2);player2=null}}if(game.p1Input!=='keyboard')document.exitPointerLock?.();updateMobileControlVisibility(true);msg('PLAYER INPUTS SAVED')}
function openControllerSettings(){const second=el('secondPlayer').value;loadControllerSettings();el('secondPlayer').value=second;el('p2Input').value=second;setControllerTab(activeControllerTab||1);el('controllerOverlay').style.display='flex';configureMobileInput();document.exitPointerLock?.()}
function closeControllerSettings(){el('controllerOverlay').style.display='none';updateMobileControlVisibility(true);if(game.started&&!game.over&&!game.upgradesOpen&&game.p1Input==='keyboard')try{canvas.requestPointerLock?.()?.catch?.(()=>{})}catch{}}
function stick(v){const d=controller.settings.deadzone;return Math.abs(v)<d?0:(v>0?(v-d)/(1-d):(v+d)/(1-d))}
function padDown(pad,index){const b=pad?.buttons?.[index];return !!b&&(b.pressed||b.value>.55)}
function currentMenuFocusables(){const selector='#survivorShop button,'+'#menu select,#menu input,#menu button,#menu summary,#controllerOverlay select,#controllerOverlay input,#controllerOverlay button,#scoreboardOverlay button,#upgradeOverlay button';return [...document.querySelectorAll(selector)].filter(n=>!n.disabled&&!n.hidden&&n.getClientRects().length>0)}
function moveMenuFocus(dir){const focusables=currentMenuFocusables();if(!focusables.length)return;let idx=focusables.indexOf(document.activeElement);if(idx<0)idx=dir>0?-1:0;idx=(idx+dir+focusables.length)%focusables.length;focusables[idx].focus();focusables[idx].scrollIntoView?.({block:'nearest',inline:'nearest'})}
function pollMenuController(pad=connectedGamepads()[0]){
  if(!pad)return;const x=stick(pad.axes[0]||0),y=stick(pad.axes[1]||0),a=padDown(pad,0),b=padDown(pad,1),tab=padDown(pad,controller.settings.tab??3),left=padDown(pad,14),right=padDown(pad,15),up=padDown(pad,12),down=padDown(pad,13),axisMove=Math.abs(y)>.55||Math.abs(x)>.72,dpadMove=up||down||left||right;
  if((axisMove||dpadMove)&&!menuAxisLock){const active=document.activeElement,horizontal=(Math.abs(x)>.72||left||right)&&Math.abs(y)<.55&&!up&&!down,direction=(x>.72||right)?1:-1;if(active?.tagName==='SELECT'&&horizontal){const options=[...active.options].filter(o=>!o.disabled),index=options.indexOf(active.selectedOptions[0]),next=(index+direction+options.length)%options.length;if(options[next]){active.value=options[next].value;active.dispatchEvent(new Event('change',{bubbles:true}))}}else if(active?.tagName==='INPUT'&&active.type==='range'&&horizontal){active.value=String(clamp(Number(active.value)+direction*(Number(active.step)||1),Number(active.min)||0,Number(active.max)||100));active.dispatchEvent(new Event('input',{bubbles:true}));active.dispatchEvent(new Event('change',{bubbles:true}))}else moveMenuFocus((y>.55||x>.72||down||right)?1:-1);menuAxisLock=true}else if(Math.abs(y)<.25&&Math.abs(x)<.35&&!dpadMove)menuAxisLock=false;
  if(a&&!menuPadPrev.a){const active=document.activeElement;if(active?.tagName==='SELECT')active.focus();else active?.click?.()}
  if(tab&&!menuPadPrev.tab){if(el('controllerOverlay').style.display==='flex')setControllerTab(activeControllerTab===1?2:1);else if(game.upgradesOpen)toggleUpgradeTree(false);else if(game.started)toggleUpgradeTree(true)}
  if(b&&!menuPadPrev.b){if(game.upgradesOpen)toggleUpgradeTree(false);else if(el('controllerOverlay').style.display==='flex')closeControllerSettings();else if(el('scoreboardOverlay').style.display==='flex')el('nextRound').click();else{const details=document.activeElement?.closest('details[open]');if(details){details.open=false;details.querySelector('summary')?.focus()}else document.activeElement?.blur?.()}}
  menuPadPrev={a,b,tab,up,down,left,right}
}
function choosePlayerPad(slot,pads,used){const wanted=String(controller.settings[slot===1?'p1Pad':'p2Pad']??'auto');let pad=wanted!=='auto'?pads.find(p=>String(p.index)===wanted&&!used.has(p.index)):null;if(!pad)pad=pads.find(p=>!used.has(p.index))||null;if(pad)used.add(pad.index);return pad}
function readPlayerPad(slot,ent,pad){const state=controller.players[slot],s=controller.settings;state.pad=pad;state.label=pad?'CONTROLLER '+(pad.index+1):'UNASSIGNED';state.moveX=state.moveZ=state.lookX=state.lookY=0;if(!pad){ent.controllerFireHeld=false;state.prev={};return}state.moveX=stick(pad.axes[0]||0);state.moveZ=-stick(pad.axes[1]||0);state.lookX=stick(pad.axes[2]||0);state.lookY=stick(pad.axes[3]||0);const deploy=padDown(pad,11),cycle=padDown(pad,8);if(game.mode==='survivor'){if(deploy&&!state.prev.deploy)deploySurvivalEquipment(ent);if(cycle&&!state.prev.cycle)cycleSurvivalEquipment();}if(warMode()){const aim=padDown(pad,9);if(aim&&!state.prev.aim)ent.aiming=!ent.aiming;if(cycle&&!state.prev.cycle)ent.crouching=!ent.crouching;}const shoot=padDown(pad,s.shoot)||padDown(pad,7)&&s.shoot===5,rawBuild=padDown(pad,s.build)||padDown(pad,6)&&s.build===4,reload=padDown(pad,s.reload),pickup=padDown(pad,s.pickup??1),jump=padDown(pad,s.jump),slide=padDown(pad,s.slide??10),modeToggle=padDown(pad,s.toggle??4),tab=padDown(pad,s.tab??3),build=rawBuild&&!(modeToggle&&s.build===s.toggle)&&!pickup;if(tab&&!state.prev.tab){toggleUpgradeTree(true);state.prev={...state.prev,tab};return}handleHumanButtons(ent,{shoot,build,reload,pickup,jump,slide,modeToggle},state.prev);state.prev={shoot,build,reload,pickup,jump,slide,modeToggle,tab,deploy,cycle,aim:padDown(pad,9)};if(slot===1)controllerFireHeld=!!shoot}
function updateControllerStatus(pads){const st=el('controllerStatus');if(!st)return;const p1=controller.players[1].label,p2=controller.players[2].label;st.textContent=pads.length?pads.length+' connected · P1 '+p1+' · P2 '+p2:'No controllers detected'}
function pollController(){const pads=connectedGamepads();controller.connected=pads.length>0;for(const state of Object.values(controller.players)){state.pad=null;state.moveX=state.moveZ=state.lookX=state.lookY=0;state.label='UNASSIGNED'}if(!pads.length){player.controllerFireHeld=false;if(player2)player2.controllerFireHeld=false;controllerFireHeld=false;updateControllerStatus(pads);return}if(!game.started||game.over||game.upgradesOpen||game.shopOpen||el('controllerOverlay').style.display==='flex'||el('scoreboardOverlay').style.display==='flex'){player.controllerFireHeld=false;if(player2)player2.controllerFireHeld=false;controllerFireHeld=false;pollMenuController(pads[0]);updateControllerStatus(pads);return}const used=new Set();if(game.p1Input!=='controller'){player.controllerFireHeld=false;controllerFireHeld=false}if(game.p1Input==='controller')readPlayerPad(1,player,choosePlayerPad(1,pads,used));if(player2&&game.p2Input!=='controller')player2.controllerFireHeld=false;if(game.secondPlayer&&player2&&game.p2Input==='controller')readPlayerPad(2,player2,choosePlayerPad(2,pads,used));updateControllerStatus(pads)}
addEventListener('gamepadconnected',e=>{controller.connected=true;refreshControllerOptions();feed('Controller '+(e.gamepad.index+1)+' connected')});
addEventListener('gamepaddisconnected',e=>{refreshControllerOptions();feed('Controller '+(e.gamepad.index+1)+' disconnected')});

function mobileCanAct(){return mobileInput.visible&&game.started&&!game.over&&!game.upgradesOpen&&!game.shopOpen&&!player.dead}
function setMobileButtonLabel(id,text){const node=el(id);if(node)node.setAttribute('aria-label',text)}
function updateMobileControlLabels(){const points=player.upgradePoints||0,button=el('mobileUpgrade'),badge=el('mobileUpgradeCount');if(button){button.classList.toggle('available',points>0);button.setAttribute('aria-label',points>0?'Weapon upgrades, '+points+' points available':'Weapon upgrades')}if(badge)badge.textContent=points>0?String(points):'';for(const [id,type] of [['mobileWall','wall'],['mobilePlatform','platform']]){const selected=buildTypeFor(player)===type;el(id)?.classList.toggle('selected',selected);el(id)?.setAttribute('aria-pressed',String(selected))}}
function updateMobileLockHud(target=null){const locked=!!target&&!target.dead,key=locked?'locked':'ready';if(mobileInput.lockHudKey===key)return;mobileInput.lockHudKey=key;el('mobileControls')?.classList.toggle('auto-locked',locked)}
function closeMobileBuildPicker(){const picker=el('mobileBuildPicker');if(picker)picker.hidden=true;el('mobileBuild')?.setAttribute('aria-expanded','false')}
function resetMobileControls(){mobileInput.moveX=mobileInput.moveForward=mobileInput.lookX=mobileInput.lookY=0;mobileInput.swiping=false;for(const resetStick of mobileInput.stickResetters)resetStick();mobileInput.cancelBuild?.();closeMobileBuildPicker();mobileInput.buildPointer=null;buildHeld=false;buildPreview=null;for(const button of document.querySelectorAll('#mobileControls .mobileAction'))button.classList.remove('active');clearMobileAutoFire(player);updateMobileLockHud(null)}
function updateMobileControlVisibility(force=false){const overlayOpen=game.upgradesOpen||game.shopOpen||el('controllerOverlay')?.style.display==='flex'||el('scoreboardOverlay')?.style.display==='flex',show=game.started&&!game.over&&game.p1Input==='touch'&&!overlayOpen,node=el('mobileControls');if(!node)return;const split=show&&game.secondPlayer&&!!player2,wide=split&&W>=H,tall=split&&W<H;node.classList.toggle('split-wide',wide);node.classList.toggle('split-tall',tall);document.body.classList.toggle('touch-playing',show);document.body.classList.toggle('touch-split-wide',wide);document.body.classList.toggle('touch-split-tall',tall);if(force||show!==mobileInput.visible){mobileInput.visible=show;node.style.display=show?'block':'none';node.setAttribute('aria-hidden',show?'false':'true');if(!show)resetMobileControls();else updateMobileControlLabels()}}
function configureMobileInput(){mobileInput.detected=detectMobileDevice();document.body?.classList.toggle('mobile-device',mobileInput.detected);const targetDpr=Math.min(mobileInput.detected?1.5:2,devicePixelRatio||1);if(Math.abs(BASE_DPR-targetDpr)>.01){BASE_DPR=targetDpr;resize()}const select=el('p1Input');if(select){if(mobileInput.detected&&select.value!=='controller')select.value='touch';else if(!mobileInput.detected&&select.value==='touch')select.value='keyboard';controller.settings.p1Input=select.value}updateMobileControlVisibility(true)}
function bindMobileStick(baseId,nubId,onMove){const base=el(baseId),nub=el(nubId);if(!base||!nub)return;let activePointer=null;const reset=()=>{const id=activePointer;activePointer=null;if(id!==null&&base.hasPointerCapture?.(id))try{base.releasePointerCapture(id)}catch{}nub.style.transform='translate(-50%,-50%)';onMove(0,0)};const update=e=>{const r=base.getBoundingClientRect(),radius=Math.max(1,Math.min(r.width,r.height)*.36),dx=e.clientX-(r.left+r.width/2),dy=e.clientY-(r.top+r.height/2),distance=Math.hypot(dx,dy),scale=distance>radius?radius/distance:1,x=dx*scale/radius,y=dy*scale/radius,dead=.055,filteredX=Math.abs(x)<dead?0:x,filteredY=Math.abs(y)<dead?0:y;nub.style.transform='translate(-50%,-50%) translate('+(x*radius).toFixed(1)+'px,'+(y*radius).toFixed(1)+'px)';onMove(filteredX,filteredY)};base.addEventListener('pointerdown',e=>{if(!mobileCanAct()||activePointer!==null)return;e.preventDefault();closeMobileBuildPicker();activePointer=e.pointerId;base.setPointerCapture?.(e.pointerId);update(e)});base.addEventListener('pointermove',e=>{if(e.pointerId!==activePointer)return;e.preventDefault();update(e)});const finish=e=>{if(activePointer===null||e.pointerId!==activePointer)return;e.preventDefault();reset()};base.addEventListener('pointerup',finish);base.addEventListener('pointercancel',finish);base.addEventListener('lostpointercapture',finish);mobileInput.stickResetters.push(reset)}
function bindMobileLook(){const area=el('mobileLookArea');if(!area)return;let activePointer=null,lastX=0,lastY=0;const reset=()=>{const id=activePointer;activePointer=null;mobileInput.swiping=false;mobileInput.lookX=mobileInput.lookY=0;if(id!==null&&area.hasPointerCapture?.(id))try{area.releasePointerCapture(id)}catch{}};area.addEventListener('pointerdown',e=>{if(!mobileCanAct()||activePointer!==null)return;e.preventDefault();closeMobileBuildPicker();activePointer=e.pointerId;lastX=e.clientX;lastY=e.clientY;mobileInput.swiping=true;area.setPointerCapture?.(e.pointerId)});area.addEventListener('pointermove',e=>{if(e.pointerId!==activePointer||!mobileCanAct())return;e.preventDefault();const sensitivity=(controller.settings.lookSensitivity||3)/3*(game.lookSensitivity||1),dx=e.clientX-lastX,dy=e.clientY-lastY;lastX=e.clientX;lastY=e.clientY;player.yaw+=dx*.0045*sensitivity;player.pitch=clamp(player.pitch+dy*.0040*sensitivity,-1.25,1.25)});const finish=e=>{if(e.pointerId===activePointer){e.preventDefault();reset()}};area.addEventListener('pointerup',finish);area.addEventListener('pointercancel',finish);area.addEventListener('lostpointercapture',finish);mobileInput.stickResetters.push(reset)}
function bindMobileAction(id,action){const button=el(id);if(!button)return;let activePointer=null;button.addEventListener('pointerdown',e=>{e.preventDefault();if(!mobileCanAct()||activePointer!==null)return;closeMobileBuildPicker();activePointer=e.pointerId;button.classList.add('active');button.setPointerCapture?.(e.pointerId);action(e)});const release=e=>{if(e.pointerId!==activePointer)return;activePointer=null;if(button.hasPointerCapture?.(e.pointerId))try{button.releasePointerCapture(e.pointerId)}catch{}button.classList.remove('active')};button.addEventListener('pointerup',release);button.addEventListener('pointercancel',release);button.addEventListener('lostpointercapture',release);button.addEventListener('click',e=>{if(e.detail===0&&mobileCanAct())action(e)});mobileInput.stickResetters.push(()=>{const id=activePointer;activePointer=null;if(id!==null&&button.hasPointerCapture?.(id))try{button.releasePointerCapture(id)}catch{}button.classList.remove('active')})}
function setupMobileControls(){
  bindMobileStick('mobileMoveStick','mobileMoveNub',(x,y)=>{mobileInput.moveX=x;mobileInput.moveForward=-y});bindMobileLook();bindMobileAction('mobileJump',()=>requestHumanJump(player));bindMobileAction('mobileSlide',()=>requestHumanSlide(player));
  const buildButton=el('mobileBuild'),picker=el('mobileBuildPicker');let holdTimer=null,heldOpen=false;
  const cancelBuild=()=>{clearTimeout(holdTimer);holdTimer=null;const id=mobileInput.buildPointer;mobileInput.buildPointer=null;heldOpen=false;buildHeld=false;buildPreview=null;buildButton?.classList.remove('active');if(id!==null&&buildButton?.hasPointerCapture?.(id))try{buildButton.releasePointerCapture(id)}catch{}};
  mobileInput.cancelBuild=cancelBuild;
  const openPicker=()=>{if(!mobileCanAct()||!picker)return;heldOpen=true;picker.hidden=false;buildButton?.setAttribute('aria-expanded','true');buildHeld=false;buildPreview=null;updateMobileControlLabels()};
  if(buildButton){
    buildButton.addEventListener('pointerdown',e=>{e.preventDefault();if(!mobileCanAct()||mobileInput.buildPointer!==null)return;closeMobileBuildPicker();mobileInput.buildPointer=e.pointerId;heldOpen=false;buildButton.classList.add('active');buildButton.setPointerCapture?.(e.pointerId);buildHeld=true;buildPreview=getBuildPreviewFor(player);holdTimer=setTimeout(openPicker,330)});
    buildButton.addEventListener('pointerup',e=>{if(e.pointerId!==mobileInput.buildPointer)return;e.preventDefault();clearTimeout(holdTimer);const r=buildButton.getBoundingClientRect(),inside=e.clientX>=r.left&&e.clientX<=r.right&&e.clientY>=r.top&&e.clientY<=r.bottom;if(!heldOpen&&inside&&mobileCanAct())placeBuildFor(player,buildPreview||getBuildPreviewFor(player));cancelBuild()});
    const cancel=e=>{if(e.pointerId!==mobileInput.buildPointer)return;e.preventDefault();cancelBuild();closeMobileBuildPicker()};buildButton.addEventListener('pointercancel',cancel);buildButton.addEventListener('lostpointercapture',cancel);
    buildButton.addEventListener('click',e=>{if(e.detail===0&&mobileCanAct()){if(e.shiftKey)openPicker();else placeBuildFor(player,getBuildPreviewFor(player))}});
    buildButton.addEventListener('contextmenu',e=>{e.preventDefault();openPicker()});
  }
  for(const [id,type] of [['mobileWall','wall'],['mobilePlatform','platform']]){const button=el(id);if(!button)continue;button.addEventListener('click',()=>{if(!mobileCanAct())return;setBuildOverrideFor(player,type);closeMobileBuildPicker();updateMobileControlLabels()});button.addEventListener('pointerdown',e=>e.stopPropagation())}
  bindMobileAction('mobileSwap',()=>interactWeapon(player));bindMobileAction('mobileReload',()=>manualReload(player));bindMobileAction('mobileUpgrade',()=>toggleUpgradeTree(true));
  addEventListener('blur',resetMobileControls);document.addEventListener('visibilitychange',()=>{if(document.hidden)resetMobileControls()});mobilePointerMedia?.addEventListener?.('change',configureMobileInput);addEventListener('orientationchange',configureMobileInput);configureMobileInput()
}
function setupGraphicalMenu(){
  for(const map of window.NeonWarMaps?.catalog()||[]){const option=document.createElement('option');option.value=map.id;option.textContent=map.name;el('map').appendChild(option)}
  window.NeonWarUI?.mount();
  for(const card of el('modeCards').querySelectorAll('[data-mode]'))card.addEventListener('click',()=>{el('mode').value=card.dataset.mode;el('mode').dispatchEvent(new Event('change',{bubbles:true}))});
  const bindNavigation=(group,selector)=>group.addEventListener('keydown',event=>{const cards=[...group.querySelectorAll(selector)].filter(c=>!c.disabled),index=cards.indexOf(document.activeElement);let next;if(['ArrowRight','ArrowDown'].includes(event.key))next=(index+1)%cards.length;else if(['ArrowLeft','ArrowUp'].includes(event.key))next=(index+cards.length-1)%cards.length;else if(event.key==='Home')next=0;else if(event.key==='End')next=cards.length-1;else return;if(!cards[next])return;event.preventDefault();cards[next].click();cards[next].focus()});
  bindNavigation(el('modeCards'),'[data-mode]');bindNavigation(el('arenaCards'),'[data-map]');el('arenaCards').addEventListener('click',event=>{const card=event.target.closest('[data-map]');if(!card)return;el('map').value=card.dataset.map;el('map').dispatchEvent(new Event('change',{bubbles:true}))});
  el('arenaCards').innerHTML=NeonMatchSettings.MAPS.map((id,i)=>'<button class="arenaCard" type="button" role="radio" aria-checked="false" tabindex="-1" data-map="'+id+'"><img loading="lazy" decoding="async" src="'+(MAP_META[id].preview||'./assets/ui/maps/'+id+'.webp')+'" alt="'+MAP_META[id].name+' in-game environment"><span class="arenaNumber">'+String(i+1).padStart(2,'0')+'</span><span class="arenaCaption">'+MAP_META[id].name+'<small>'+MAP_META[id].tag+'</small></span></button>').join('');
  for(const image of el('arenaCards').querySelectorAll('img'))image.addEventListener('error',()=>image.closest('button').classList.add('preview-missing'));
  loadMatchPreferences();for(const id of [...MATCH_FIELD_IDS,'secondPlayer','lookSensitivity','soundVolume','visualQuality'])el(id)?.addEventListener(id==='lookSensitivity'||id==='soundVolume'?'input':'change',()=>{saveMatchPreferencesFromFields();updateMenuSummary()});
}
canvas.addEventListener('click',()=>{if(game.started&&!game.upgradesOpen&&game.p1Input==='keyboard') try{canvas.requestPointerLock?.()?.catch?.(()=>{})}catch{}});
document.addEventListener('pointerlockchange',()=>pointer=document.pointerLockElement===canvas);
document.addEventListener('mousemove',e=>{if(pointer){mouseDX+=e.movementX; mouseDY+=e.movementY}});
document.addEventListener('contextmenu',e=>{if(game.started)e.preventDefault()});
canvas.addEventListener('mousedown',e=>{if(!game.started||game.upgradesOpen||game.p1Input!=='keyboard')return;if(e.button===0){if(detonateRemoteGrenades(player)){fireHeld=false;return}fireHeld=true;if(player.weapon!=='plasma'&&player.weapon!=='laser')tryFire(player,true)}else if(e.button===2){e.preventDefault();buildHeld=true;buildPreview=getBuildPreview()}});
addEventListener('mouseup',e=>{if(e.button===0){if(game.started&&!game.upgradesOpen&&game.p1Input==='keyboard'){if(player.weapon==='plasma')releasePlayerPlasma();if(player.mortarActive)releaseMortar(player)}fireHeld=false}else if(e.button===2){e.preventDefault();if(game.started&&!game.upgradesOpen&&game.p1Input==='keyboard'&&buildHeld)placeBuild(buildPreview||getBuildPreview());buildHeld=false;buildPreview=null}});
function clamp(v,a,b){return Math.max(a,Math.min(b,v))} function rand(a,b){return a+Math.random()*(b-a)} function pick(a){return a[(Math.random()*a.length)|0]} function hypot2(x,z){return Math.hypot(x,z)}
function msg(t){hud.msg.textContent=t;hud.msg.style.opacity=1;clearTimeout(msg.t);msg.t=setTimeout(()=>hud.msg.style.opacity=0,950)}
function feed(t){const d=document.createElement('div');d.textContent=t;hud.feed.prepend(d);while(hud.feed.children.length>8)hud.feed.lastChild.remove()}
const tier3CueTimes=new Map();
function tier3Cue(text,cooldown=2.5){const now=performance.now()/1000,last=tier3CueTimes.get(text)||-Infinity;if(now-last<cooldown)return false;tier3CueTimes.set(text,now);feed(text);return true}
const WEAPONS={
  pistol:{name:'SIX-SHOOTER PISTOL',damage:31,range:72,cool:.145,spread:.018,mag:6,reload:1.55,tracers:1,color:'#d9f7ff',falloff:.34,kind:'hitscan',semi:true},
  assault:{name:'VOLTLINE ASSAULT',damage:46,range:104,cool:.085,spread:.014,mag:36,reload:2.35,tracers:1,color:'#00eaff',falloff:.28,kind:'hitscan'},
  sniper:{name:'WHITEOUT RAIL SNIPER',damage:95,head:999,range:165,cool:.82,spread:.002,mag:1,reload:.75,tracers:1,color:'#ffffff',falloff:.02,kind:'hitscan'},
  smg:{name:'YELLOWJACKET SMG',damage:32,range:62,cool:.055,spread:.045,mag:42,reload:1.85,tracers:1,color:'#ffea00',falloff:.72,kind:'hitscan'},
  shotgun:{name:'DRAGONBREACH SHOTGUN',damage:46,range:38,cool:.62,spread:.125,mag:8,reload:4.10,tracers:8,color:'#ff7a2b',falloff:1.05,kind:'shotgun'},
  burst:{name:'TRIDENT BURST CARBINE',damage:29,range:88,cool:.235,spread:.010,mag:30,reload:2.25,tracers:3,color:'#7cffd5',falloff:.26,kind:'burst'},
  arc:{name:'TEMPEST ARC BLASTER',damage:52,range:72,cool:.34,spread:.018,mag:18,reload:2.65,tracers:1,color:'#48c7ff',falloff:.18,kind:'arc'},
  rocket:{name:'COMET ROCKET LAUNCHER',damage:124,range:95,cool:.82,spread:.006,mag:1,reload:3.65,tracers:1,color:'#ff4a30',falloff:.12,kind:'rocket'},
  mortar:{name:'SCARLET SIEGE MORTAR',damage:156,range:74,cool:.4,spread:0,mag:1,reload:3.2,tracers:1,color:'#ff204e',falloff:.35,kind:'mortar',radius:9,targetSpeed:16,maxHold:2.8,minHold:.25,semi:true},
  punch:{name:'MAGENTA PUNCH GUN',damage:92,range:21.75,cool:.11,spread:.004,mag:1,reload:1.0,tracers:1,color:'#ff4fd8',falloff:.05,kind:'punch',knockback:27,wallDamage:150},
  harpoon:{name:'ANCHORLINE HARPOON',damage:58,range:58,cool:.45,spread:.002,mag:1,reload:1.7,tracers:1,color:'#bdfcff',falloff:.05,kind:'harpoon',semi:true,pullStrength:90,pullDuration:1.3,pullVelocityMult:1.65,wallPullStrength:30},
  plasma:{name:'NOVA PLASMA BLASTER',damage:66,range:118,cool:.22,spread:.006,mag:999,reload:0,tracers:1,color:'#9b5cff',falloff:.08,kind:'plasma',heat:.18,coolRate:.48,chargeRate:.92},
  laser:{name:'PRISM PLASMA LASER',damage:25,range:112,cool:.035,spread:.001,mag:999,reload:0,tracers:1,color:'#ff2bff',falloff:.04,kind:'laser',heatRate:1.28,coolRate:.62},
  mender:{name:'LIFELINE MENDER',damage:.28,range:24,cool:.04,spread:0,mag:999,reload:0,color:'#8ffaff',kind:'mender',heatRate:.20,coolRate:.55,recoveryHeat:.40,repairRate:28,patchRepairRate:38,healRate:18,medicalHealRate:24,triageHealRate:35,armorHealRate:8}
};
for(const weapon of window.NeonFactions?.weapons||[])WEAPONS[weapon.id]={...weapon.stats,name:weapon.name,...Object.fromEntries(Object.entries(weapon).filter(([key])=>!['id','model','preview','stats'].includes(key)))};
const UPGRADE_TREE={
  harpoon:{
    barbedTip:{tier:1,cost:1,name:'Barbed Tip',desc:'Harpoon hits deal more damage and briefly bleed/stagger enemies.'},
    quickWinch:{tier:1,cost:2,name:'Quick Winch',desc:'Reloads faster and pulls you toward walls more quickly.'},
    tetherTrap:{tier:2,cost:3,requires:'barbedTip',name:'Tether Trap',desc:'Harpooned enemies are briefly chained near the impact point or nearest friendly wall.'},
    skyhook:{tier:2,cost:3,requires:'quickWinch',name:'Skyhook',desc:'Wall grapples launch you slightly upward and improve platform repositioning.'},
    breachAnchor:{tier:3,cost:5,requires:'tetherTrap',name:'Breach Anchor',desc:'Harpooned enemies attacking walls are ripped away from the breach and take impact damage if slammed into structures.'},
    momentumLine:{tier:3,cost:5,requires:'skyhook',name:'Momentum Line',desc:'After grappling to a wall or platform, gain a brief speed boost and empower your next shot.'}},
  assault:{
    electric:{tier:1,cost:1,name:'Electric Bolts',desc:'Hits briefly slow targets with a neon shock.'},
    stabilizer:{tier:1,cost:2,name:'Stabilizer',desc:'Cuts spread for accurate long-lane fire.'},
    chain:{tier:2,cost:3,requires:'electric',name:'Chain Spark',desc:'Electric hits arc damage to one nearby enemy.'},
    overdrive:{tier:2,cost:4,requires:'stabilizer',name:'Overdrive Barrel',desc:'Fires 22% faster; reloads take slightly longer.'},
    stormConductor:{tier:3,cost:5,requires:'chain',name:'Storm Conductor',desc:'Chain Spark jumps through up to 3 enemies, with extra reach around contested walls.'},
    momentumClip:{tier:3,cost:5,requires:'overdrive',name:'Momentum Clip',desc:'Accurate sustained hits build momentum for faster fire and reload; kills briefly lock it.'}},
  smg:{
    static:{tier:1,cost:1,name:'Static Rounds',desc:'Close hits apply a brief electric slow.'},
    lightweight:{tier:1,cost:2,name:'Lightweight Frame',desc:'Move 10% faster while holding the SMG.'},
    swarm:{tier:2,cost:3,requires:'static',name:'Arc Swarm',desc:'Rapid hits build a stronger slowing charge.'},
    closeBurst:{tier:2,cost:3,requires:'lightweight',name:'Close Quarters Burst',desc:'Deal 30% bonus damage within 10 meters.'},
    voltDash:{tier:3,cost:5,requires:'swarm',name:'Volt Dash',desc:'Close damage earns one short jump-dash charge; dashing after a kill recovers ammo.'},
    slipstream:{tier:3,cost:5,requires:'closeBurst',name:'Slipstream Runner',desc:'Gain mobility with the SMG, speed bursts on damage, and quicker post-kill reloads.'}},
  shotgun:{
    breacher:{tier:1,cost:1,name:'Breacher',desc:'Pellets knock enemies back and bite into armor.'},
    armorBreaker:{tier:1,cost:2,name:'Armor Breaker',desc:'Greatly increases pellet damage to armor.'},
    breacher2:{tier:2,cost:3,requires:'breacher',name:'Breacher II',desc:'Stronger knockback plus a short stagger.'},
    shatter:{tier:2,cost:4,requires:'armorBreaker',name:'Shatter Shells',desc:'Armor breaks burst into damaging neon shards.'},
    thunderBreach:{tier:3,cost:5,requires:'breacher2',name:'Thunder Breach',desc:'Near damaged friendly walls, shots emit a cone that repels and interrupts attackers while shielding the wall briefly.'},
    dragonShells:{tier:3,cost:5,requires:'shatter',name:'Dragon Shells',desc:'Close shots ignite a short cone, strongest directly in front of the barrel.'}},
  sniper:{
    rail:{tier:1,cost:2,name:'Rail Lance',desc:'Shots pierce through one enemy.'},
    precision:{tier:1,cost:2,name:'Precision Chamber',desc:'Faster shot recovery and tactical reload.'},
    rail2:{tier:2,cost:4,requires:'rail',name:'Rail Lance II',desc:'Pierces up to three enemies with better damage retention.'},
    critical:{tier:2,cost:3,requires:'precision',name:'Critical Trace',desc:'Headshots leave a short damaging energy trace.'},
    phaseRail:{tier:3,cost:5,requires:'rail2',name:'Phase Rail',desc:'Pierce one friendly built wall at reduced damage; enemies hit beyond it take 15% more teammate damage for 2 seconds.'},
    thermalScope:{tier:3,cost:5,requires:'critical',name:'Thermal Scope',desc:'Highlights enemies hidden behind exactly one wall near your aim.'}},
  burst:{
    tungsten:{tier:1,cost:1,name:'Tungsten Burst',desc:'Carbine bursts hit harder and chew cover better.'},
    quickCycle:{tier:1,cost:2,name:'Quick-Cycle Receiver',desc:'Shortens burst delay and reload time for aggressive mid-range duels.'},
    marksman:{tier:2,cost:3,requires:'tungsten',name:'Marksman Rifling',desc:'Tightens burst grouping and rewards headshots with bonus damage.'},
    suppressor:{tier:2,cost:3,requires:'quickCycle',name:'Suppressive Rhythm',desc:'Landing a full burst briefly slows the target.'},
    deadeye:{tier:3,cost:5,requires:'marksman',name:'Deadeye Burst',desc:'A full same-target burst empowers its final bullet, especially after a headshot.'},
    tacticalRollout:{tier:3,cost:5,requires:'suppressor',name:'Tactical Rollout',desc:'Full bursts briefly improve handling and acceleration; boost kills partially reload.'}},
  arc:{
    capacitor:{tier:1,cost:1,name:'Overcharged Capacitor',desc:'Primary arcs deal more damage and slow longer.'},
    fork:{tier:1,cost:2,name:'Forked Conductor',desc:'Arc shots can jump to a second nearby target.'},
    storm:{tier:2,cost:4,requires:'capacitor',name:'Storm Cell',desc:'Repeated hits build a stun-like stagger pulse.'},
    web:{tier:2,cost:4,requires:'fork',name:'Arc Web',desc:'Chain arcs jump farther and can tag two extra enemies.'},
    stormAnchor:{tier:3,cost:5,requires:'storm',name:'Storm Anchor',desc:'Repeated hits anchor one priority enemy as a temporary lightning hub; its death releases a small arc burst.'},
    lightningNetwork:{tier:3,cost:6,requires:'web',name:'Lightning Network',desc:'Chains prioritize wall attackers, stun the final target after 3 links, and can Overload bosses.'}},
  mortar:{
    biggerPayload:{tier:1,cost:1,name:'Bigger Payload',desc:'Increases explosion radius and wall damage.'},
    fasterTargeting:{tier:1,cost:2,name:'Faster Targeting',desc:'The red target circle travels faster and max hold time is slightly longer.'},
    bunkerBuster:{tier:2,cost:3,requires:'biggerPayload',name:'Bunker Buster',desc:'Deals bonus damage to bosses, armor, and enemies attacking walls.'},
    guidedDrift:{tier:2,cost:3,requires:'fasterTargeting',name:'Guided Drift',desc:'While holding, the target circle can be slightly steered with aim or movement.'},
    breachMortar:{tier:3,cost:5,requires:'bunkerBuster',name:'Breach Mortar',desc:'Explosions near damaged friendly walls push enemies away and briefly reinforce the wall.'},
    meteorCommand:{tier:3,cost:5,requires:'guidedDrift',name:'Meteor Command',desc:'After release, the falling rocket slightly tracks toward the largest enemy cluster inside the target circle.'}},
  rocket:{
    blast:{tier:1,cost:2,name:'Bigger Blast',desc:'Greatly increases the already-large explosion radius and cover-breaking damage.'},
    grenade:{tier:1,cost:2,name:'Grenade Launcher Kit',desc:'Rockets become bouncy grenades that explode on enemy contact or after a short fuse.'},
    cluster:{tier:2,cost:4,requires:'blast',name:'Cluster Payload',desc:'Rocket explosions release extra micro-blasts around the impact.'},
    velocity:{tier:2,cost:3,requires:'grenade',name:'Smart Fuse Springs',desc:'Bouncy grenades travel faster, keep more speed after bounces, and fuse sooner.'},
    siegeWarhead:{tier:3,cost:6,requires:'cluster',name:'Siege Warhead',desc:'Direct boss hits crack armor for 4 seconds, increasing damage from all players by 18%.'},
    remoteDetonator:{tier:3,cost:5,requires:'velocity',name:'Remote Detonator',desc:'Press fire again or reload to remotely detonate active bouncy grenade rockets.'}},
  punch:{
    heavyKnuckles:{tier:1,cost:1,name:'Heavy Knuckles',desc:'Increases punch damage and adds slightly stronger knockback.'},
    longerChain:{tier:1,cost:2,name:'Longer Chain',desc:'Moderately extends the chain while keeping the Punch Gun short-range.'},
    wallBreaker:{tier:2,cost:3,requires:'heavyKnuckles',name:'Wall Breaker',desc:'Greatly increases damage to walls, structures, and enemy cover.'},
    snapbackChain:{tier:2,cost:3,requires:'longerChain',name:'Snapback Chain',desc:'Retracts the fist faster and shortens Punch Gun reload time.'},
    seismicPunch:{tier:3,cost:5,requires:'wallBreaker',name:'Seismic Punch',desc:'Impacts release a small shockwave that damages nearby enemies and structures.'},
    grappleImpact:{tier:3,cost:5,requires:'snapbackChain',name:'Grapple Impact',desc:'Tugs then launches enemies; collisions damage wallside targets, while bosses resist but can be interrupted.'}},
  plasma:{
    gravity:{tier:1,cost:1,name:'Gravity Core',desc:'Charged plasma subtly pulls nearby enemies.'},
    larger:{tier:1,cost:2,name:'Larger Core',desc:'Charged projectiles have a larger collision core.'},
    singularity:{tier:2,cost:4,requires:'gravity',name:'Singularity Burst',desc:'Full charges pull hard and detonate in a gravity blast.'},
    supernova:{tier:2,cost:4,requires:'larger',name:'Supernova Core',desc:'Charged explosions gain radius and damage.'},
    eventHorizon:{tier:3,cost:6,requires:'singularity',name:'Event Horizon',desc:'Full charges pull before detonation and leave slowing plasma residue beside damaged walls and breaches.'},
    plasmaMine:{tier:3,cost:5,requires:'supernova',name:'Plasma Mine',desc:'Full charges stick to surfaces or enemies, then detonate on proximity or after a short delay.'}},
  laser:{
    reflector:{tier:1,cost:2,name:'Reflector',desc:'The beam reflects once from arena geometry.'},
    heatSink:{tier:1,cost:1,name:'Heat Sink',desc:'Reduces laser heat buildup by 28%.'},
    reflector2:{tier:2,cost:4,requires:'reflector',name:'Reflector II',desc:'Adds a second bounce with less damage loss.'},
    focus:{tier:2,cost:3,requires:'heatSink',name:'Focus Beam',desc:'Damage ramps while the beam remains on one target.'},
    prismSplitter:{tier:3,cost:5,requires:'reflector2',name:'Prism Splitter',desc:'The first reflection splits once into two capped, weaker beams at diverging angles.'},
    cuttingBeam:{tier:3,cost:6,requires:'focus',name:'Cutting Beam',desc:'Ramps harder on bosses and armor, suppresses wall attackers, and accelerates armor breaks; switching targets resets it.'}},
  mender:{
    patchCurrent:{tier:1,cost:1,name:'Patch Current',desc:'Raises structure repair from 28 to 38 HP/sec.'},
    medicalLens:{tier:1,cost:1,name:'Medical Lens',desc:'Raises teammate healing from 18 to 24 HP/sec and can restore armor slowly.'},
    fortifySeal:{tier:2,cost:3,requires:'patchCurrent',name:'Fortify Seal',desc:'Fully repaired structures gain temporary bonus armor for a short time.'},
    triageLink:{tier:2,cost:3,requires:'medicalLens',name:'Triage Link',desc:'Heals teammates below 40% health at 35 HP/sec and briefly boosts them at full health.'},
    armorBoost:{tier:3,cost:5,requires:'fortifySeal',name:'Armor Boost',desc:'Beam a fully repaired structure to build 45 bonus HP/sec, or 58 with Patch Current, up to 500. Boosted walls resist boss impacts.'},
    guardianNetwork:{tier:3,cost:5,requires:'triageLink',name:'Guardian Network',desc:'The beam chains to one nearby damaged wall or wounded teammate.'}}
};
function createUpgradeState(){const state={};for(const [weapon,defs] of Object.entries(UPGRADE_TREE)){state[weapon]={};for(const key of Object.keys(defs))state[weapon][key]=false}return state}
function hasUpgrade(weapon,key,ent=player){return ent===player&&!!ent.upgrades?.[weapon]?.[key]}
function toggleUpgradeTree(force){if(!game.started)return;game.upgradesOpen=force===undefined?!game.upgradesOpen:force;el('upgradeOverlay').style.display=game.upgradesOpen?'flex':'none';if(game.upgradesOpen&&player.mortarActive)releaseMortar(player);fireHeld=false;updateMobileControlVisibility(true);if(game.upgradesOpen){document.exitPointerLock?.();renderUpgradeTree();setTimeout(()=>moveMenuFocus(1),0)}else if(game.p1Input==='keyboard')try{canvas.requestPointerLock?.()?.catch?.(()=>{})}catch{}}
function renderUpgradeTree(){const weapon=player.weapon,defs=UPGRADE_TREE[weapon]||{},state=player.upgrades?.[weapon]||{};el('upgradeTitle').textContent=WEAPONS[weapon].name+' UPGRADES';el('upgradePointLabel').textContent=player.upgradePoints+' POINT'+(player.upgradePoints===1?'':'S');const grid=el('upgradeGrid');grid.innerHTML='';const tiers=[...new Set(Object.values(defs).map(u=>u.tier))].sort((a,b)=>a-b);for(const tier of tiers){const label=document.createElement('div');label.className='tierLabel tier'+tier;label.textContent='TIER '+tier+(tier===3?' · SPECIALIZATION':'');grid.appendChild(label);for(const [key,u] of Object.entries(defs).filter(([,v])=>v.tier===tier)){const bought=!!state[key],prereq=!u.requires||state[u.requires],affordable=player.upgradePoints>=u.cost,canBuy=!bought&&prereq&&affordable,card=document.createElement('div');card.className='upgradeCard tier'+tier+' '+(bought?'bought':(!prereq?'locked':''));const req=u.requires?' · Requires '+defs[u.requires].name:'',status=bought?'PURCHASED':(!prereq?'LOCKED'+req:(!affordable?u.cost+' POINTS · NEED '+(u.cost-player.upgradePoints):u.cost+' POINTS'));card.innerHTML='<h3>'+u.name+'</h3><p>'+u.desc+'</p><button '+(canBuy?'':'disabled')+' aria-disabled=+(!canBuy)+>'+status+'</button>';card.querySelector('button').onclick=()=>buyUpgrade(weapon,key);grid.appendChild(card)}}}
function buyUpgrade(weapon,key){const u=UPGRADE_TREE[weapon]?.[key],state=player.upgrades?.[weapon];if(!u||!state||state[key]||(u.requires&&!state[u.requires])||player.upgradePoints<u.cost)return;player.upgradePoints-=u.cost;state[key]=true;msg(u.name.toUpperCase()+' ONLINE');feed('Upgrade purchased: '+u.name);renderUpgradeTree();updateHud()}
function manualReload(ent,allowRemote=true){if(ent?.mortarActive)releaseMortar(ent);if(allowRemote&&detonateRemoteGrenades(ent))return;if(!game.started||ent.dead||ent.reload>0)return;const w=WEAPONS[ent.weapon];if(!w||w.kind==='mender')return;const ready=getUltimateReady(ent),active=ultimateActive(ent,ent.weapon);if((w.kind==='plasma'||w.kind==='laser'||w.kind==='mender')){if(!ready&&!active)return;const time=w.kind==='plasma'?1.45:1.35;ent.reload=time;ent.reloadTotal=time;if(typeof tacticalController!=='undefined')tacticalController?.hear({kind:'reload',x:ent.x,y:ent.y,z:ent.z,team:ent.team,source:ent.entityId||ent.name,range:5});ent.reloadAnimType=ent.weapon;ent.cool=Math.max(ent.cool,.18);if(ent===player){msg((ready?'LOADING ULTIMATE ':'COOLING ')+w.name);fireHeld=false}return;}if(ent.ammo>=w.mag&&!ready&&!active)return;let time=w.reload;if(ent===player&&ent.weapon==='sniper'&&hasUpgrade('sniper','precision'))time*=.72;if(ent===player&&ent.weapon==='assault'&&hasUpgrade('assault','overdrive'))time*=1.16;if(ent===player&&ent.weapon==='assault'&&hasUpgrade('assault','momentumClip')&&(ent.assaultMomentum||0)>=.72)time*=.78;if(ent===player&&ent.weapon==='smg'&&hasUpgrade('smg','slipstream')&&(ent.slipstreamKillWindow||0)>0)time*=.72;if(ent===player&&ent.weapon==='burst'&&hasUpgrade('burst','tacticalRollout')&&(ent.tacticalTimer||0)>0)time*=.78;if(ent===player&&ent.weapon==='punch'&&hasUpgrade('punch','snapbackChain'))time*=.68;if(ent===player&&ent.weapon==='harpoon'&&hasUpgrade('harpoon','quickWinch'))time*=.8;if(ent===player&&(ent.harpoonMomentumTimer||0)>0)time*=.85;ent.reload=time;ent.reloadTotal=time;if(typeof tacticalController!=='undefined')tacticalController?.hear({kind:'reload',x:ent.x,y:ent.y,z:ent.z,team:ent.team,source:ent.entityId||ent.name,range:5});ent.reloadAnimType=ent.weapon;ent.cool=Math.max(ent.cool,.18);if(ent===player){msg((ready?'LOADING ULTIMATE ':'RELOADING ')+w.name);fireHeld=false}}
function applySlow(ent,amount,time){ent.slowAmount=Math.max(ent.slowAmount||0,amount);ent.slowTimer=Math.max(ent.slowTimer||0,time);burst(ent.x,ent.y-.75,ent.z,'#65f7ff',7,.3)}
function nearActiveWall(ent,radius=5){return blocksNear(ent.x,ent.z,radius+1).some(b=>{const w=b.structure;if(b.kind!=='wall'||w?.active===false||Math.abs(ent.y-w.y)>=w.h/2+3)return false;const dx=Math.max(0,Math.abs(ent.x-w.x)-w.w/2),dz=Math.max(0,Math.abs(ent.z-w.z)-w.d/2);return Math.hypot(dx,dz)<radius})}
function addArcVisual(from,to,color='#65f7ff',width=3.5){const dist=hypot2(to.x-from.x,to.z-from.z)||1;shots.push({x:from.x,y:from.y,z:from.z,dx:(to.x-from.x)/dist,dy:0,dz:(to.z-from.z)/dist,life:.16,maxLife:.16,len:dist,color,width,tier3Visual:true})}
function activeArcAnchor(owner=player){const target=owner?.arcAnchorTarget;if(target&&!target.dead&&target.arcAnchorOwner===owner&&(target.arcAnchorUntil||0)>performance.now()/1000)return target;if(target?.arcAnchorOwner===owner){target.arcAnchorOwner=null;target.arcAnchorUntil=0}if(owner)owner.arcAnchorTarget=null;return null}
function plantArcAnchor(owner,target){const old=activeArcAnchor(owner);if(old&&old!==target){old.arcAnchorOwner=null;old.arcAnchorUntil=0}target.arcAnchorOwner=owner;target.arcAnchorUntil=performance.now()/1000+4.5;owner.arcAnchorTarget=target;particles.push({kind:'ring',x:target.x,y:target.y-.75,z:target.z,vx:0,vy:0,vz:0,life:.32,maxLife:.32,c:'#65f7ff',size:26});if(owner===player)msg('STORM ANCHOR PLANTED')}
function releaseArcAnchor(target){const owner=target?.arcAnchorOwner;if(!owner)return;const charged=(target.arcAnchorUntil||0)>performance.now()/1000;target.arcAnchorOwner=null;target.arcAnchorUntil=0;if(owner.arcAnchorTarget===target)owner.arcAnchorTarget=null;if(!charged)return;const nearby=targetsFor(owner,owner===player).filter(t=>t!==target&&!t.dead&&hypot2(t.x-target.x,t.z-target.z)<6.5).sort((a,b)=>hypot2(a.x-target.x,a.z-target.z)-hypot2(b.x-target.x,b.z-target.z)).slice(0,3);burst(target.x,target.y-.7,target.z,'#65f7ff',8,.3);for(const t of nearby){addArcVisual({x:target.x,y:target.y-.7,z:target.z},{x:t.x,y:t.y-.7,z:t.z},'#65f7ff',3);damage(t,24,owner,false,{x:t.x,y:t.y-.7,z:t.z},{weapon:'arc',armorMult:1.15})}}
function enemyAttackingWall(t){if(!t||t.dead)return false;if(t.bossTarget?.destructible&&t.bossTarget.active!==false)return true;return !!(t.alien&&nearActiveWall(t,2.8)&&(t.attackAnim||0)>.05)}
function bossInterruptible(t){return !!(t?.boss&&(t.bossState==='windup'||t.bossState==='charge'||t.bossState==='burrowWindup'||t.attackAnim>.22))}
function interruptBoss(t,time=.28){if(!bossInterruptible(t))return false;if(t.bossState==='windup'||t.bossState==='charge'){t.bossState='stunned';t.bossTimer=Math.max(t.bossTimer||0,time)}t.stagger=Math.max(t.stagger||0,time);t.vx*=.25;t.vz*=.25;return true}
function arcBossOverload(target,amount,owner){if(!target?.boss)return;target.arcOverload=Math.min(1,(target.arcOverload||0)+amount);target.arcOverloadTimer=2.6;if(target.arcOverload<1)return;target.arcOverload=0;damage(target,34,owner,false,{x:target.x,y:target.y-.7,z:target.z},{weapon:'arc',armorMult:1.4,noUltimate:true});target.stagger=Math.max(target.stagger||0,.38);interruptBoss(target,.42);addTier3Fx({type:'storm',x:target.x,y:target.y-.7,z:target.z,color:'#baffff',size:7,life:.28});tier3Cue('BOSS OVERLOADED',2.4)}
function arcChainShot(shooter,primary,baseDamage,isPlayer){const web=isPlayer&&hasUpgrade('arc','web'),network=isPlayer&&hasUpgrade('arc','lightningNetwork'),anchor=isPlayer&&hasUpgrade('arc','stormAnchor')?activeArcAnchor(shooter):null,used=new Set([primary]);let from=primary,chainHits=1,finalTarget=primary;const candidates=targetsFor(shooter,isPlayer),jumps=Math.min(MAX_CHAIN_JUMPS,network?5:(web?3:((isPlayer&&hasUpgrade('arc','fork'))?2:1)));if(network)arcBossOverload(primary,.24,shooter);for(let jump=0;jump<jumps;jump++){const breach=network&&nearActiveWall(from,5.5),range=network?(breach?14:12):(web?11:7.5);let next=null,best=Infinity;for(const t of candidates){if(used.has(t)||t.dead||t.spawnGrace>0||Math.abs(t.y-from.y)>=4)continue;const fromDist=hypot2(t.x-from.x,t.z-from.z);if(fromDist>=range||!lineClear(from.x,from.y-.7,from.z,t.x,t.y-.7,t.z))continue;const score=network?fromDist+(enemyAttackingWall(t)?-8:0)+(t.boss?-1.5:0):fromDist;if(score<best){best=score;next=t}}if(!next)break;used.add(next);chainHits++;finalTarget=next;const scale=network?[.55,.46,.38,.31,.25][jump]:.55;addArcVisual({x:from.x,y:from.y-.7,z:from.z},{x:next.x,y:next.y-.7,z:next.z},'#65f7ff',network?2.2:3.8);damage(next,baseDamage*scale,shooter,false,{x:next.x,y:next.y-.7,z:next.z},{weapon:'arc',armorMult:1.2});applySlow(next,.12,.55);if(network)arcBossOverload(next,.22,shooter);from=network?next:primary}
  if(network&&chainHits>=3&&finalTarget){finalTarget.stagger=Math.max(finalTarget.stagger||0,finalTarget.boss?.18:.48);if(finalTarget.boss)interruptBoss(finalTarget,.18)}
  if(anchor&&anchor!==primary&&hypot2(anchor.x-primary.x,anchor.z-primary.z)<12&&!used.has(anchor)){used.add(anchor);addArcVisual({x:primary.x,y:primary.y-.7,z:primary.z},{x:anchor.x,y:anchor.y-.7,z:anchor.z},'#baffff',3);damage(anchor,baseDamage*.38,shooter,false,{x:anchor.x,y:anchor.y-.7,z:anchor.z},{weapon:'arc',armorMult:1.25})}
  if(anchor){const hubTargets=candidates.filter(t=>t!==anchor&&!used.has(t)&&!t.dead&&hypot2(t.x-anchor.x,t.z-anchor.z)<9).sort((a,b)=>(enemyAttackingWall(b)?1:0)-(enemyAttackingWall(a)?1:0)||hypot2(a.x-anchor.x,a.z-anchor.z)-hypot2(b.x-anchor.x,b.z-anchor.z)).slice(0,network?2:1);for(const t of hubTargets){addArcVisual({x:anchor.x,y:anchor.y-.7,z:anchor.z},{x:t.x,y:t.y-.7,z:t.z},'#baffff',2.2);damage(t,baseDamage*.28,shooter,false,{x:t.x,y:t.y-.7,z:t.z},{weapon:'arc',armorMult:1.15});if(network)arcBossOverload(t,.18,shooter)}}
}
function applyDragonBurn(target,dps,time,owner){target.burnDps=Math.max(target.burnDps||0,dps);target.burnTimer=Math.max(target.burnTimer||0,time);target.burnOwner=owner;target.burnTick=Math.min(target.burnTick||.2,.2)}
function updateCombatUpgradeEffects(dt){
  for(const ent of new Set([player,player2,...bots])){if(!ent)continue;ent.menderSpeedBoost=Math.max(0,(ent.menderSpeedBoost||0)-dt);ent.armorCrackedTimer=Math.max(0,(ent.armorCrackedTimer||0)-dt);ent.phaseMarkedTimer=Math.max(0,(ent.phaseMarkedTimer||0)-dt);ent.arcOverloadTimer=Math.max(0,(ent.arcOverloadTimer||0)-dt);if(ent.arcOverloadTimer<=0)ent.arcOverload=Math.max(0,(ent.arcOverload||0)-dt*.45);ent.wallDamageCutTimer=Math.max(0,(ent.wallDamageCutTimer||0)-dt);ent.harpoonMomentumTimer=Math.max(0,(ent.harpoonMomentumTimer||0)-dt);if(ent.harpoonBleedTimer>0&&!ent.dead){ent.harpoonBleedTimer-=dt;ent.harpoonBleedTick=(ent.harpoonBleedTick||.25)-dt;if(ent.harpoonBleedTick<=0){ent.harpoonBleedTick=.25;damage(ent,(ent.harpoonBleedDps||0)*.25,ent.harpoonBleedOwner,false,{x:ent.x,y:ent.y-.7,z:ent.z},{weapon:'harpoon',armorMult:.75,noUltimate:true,minimalFx:true})}if(ent.harpoonBleedTimer<=0){ent.harpoonBleedDps=0;ent.harpoonBleedOwner=null}}if(ent.harpoonTether){const h=ent.harpoonTether;h.timer-=dt;const dx=ent.x-h.x,dz=ent.z-h.z,dist=Math.hypot(dx,dz)||1;if(dist>h.r){const excess=dist-h.r;ent.vx-=dx/dist*(10+excess*8)*dt;ent.vz-=dz/dist*(10+excess*8)*dt;ent.x-=dx/dist*Math.min(excess,.18);ent.z-=dz/dist*Math.min(excess,.18)}if(h.timer<=0||ent.dead)ent.harpoonTether=null}if(ent.harpoonPull){const h=ent.harpoonPull;h.timer-=dt;if(!h.owner||h.owner.dead||ent.dead)h.timer=0;else{const dx=h.owner.x-ent.x,dy=(h.owner.y-.6)-(ent.y-.7),dz=h.owner.z-ent.z,dist=Math.hypot(dx,dy,dz)||1;ent.vx+=dx/dist*h.strength*dt*2.15;ent.vy+=dy/dist*h.strength*dt*.36;ent.vz+=dz/dist*h.strength*dt*2.15;const wall=harpoonImpactStructure(ent),other=targetsFor(h.owner,isHumanActor(h.owner)).find(t=>t!==ent&&!t.dead&&Math.hypot(t.x-ent.x,t.z-ent.z)<(t.r||.6)+(ent.r||.6)+.25);if(!h.hit&&(wall||other)){h.hit=true;const amount=h.impact?42:24;damage(ent,amount,h.owner,false,{x:ent.x,y:ent.y-.7,z:ent.z},{weapon:'harpoon',armorMult:1.35});if(other)damage(other,h.impact?28:16,h.owner,false,{x:other.x,y:other.y-.7,z:other.z},{weapon:'harpoon'});burst(ent.x,ent.y-.7,ent.z,'#bdfcff',12,.34);h.timer=0}}if(h.timer<=0)ent.harpoonPull=null}if(ent.harpoonGrapple){const h=ent.harpoonGrapple;h.timer-=dt;const dx=h.x-ent.x,dy=h.y-(ent.y-.5),dz=h.z-ent.z,dist=Math.hypot(dx,dy,dz)||1;if(dist<=h.stop||!lineClear(ent.x,ent.y-.5,ent.z,h.x,h.y,h.z)){ent.vx*=.35;ent.vz*=.35;h.timer=0}else{ent.vx+=dx/dist*h.strength*dt*2.2;ent.vy+=dy/dist*h.strength*dt*.9;ent.vz+=dz/dist*h.strength*dt*2.2}if(h.timer<=0){if(h.friendly&&h.skyhook)ent.vy=Math.max(ent.vy,6.5);if(h.friendly&&h.momentum){ent.harpoonMomentumTimer=3;ent.harpoonMomentumShot=1;tier3Cue('MOMENTUM LINE',2.4)}ent.harpoonGrapple=null}}if(ent.grappleLaunch){ent.grappleLaunch.timer-=dt;if(ent.grappleLaunch.timer<=0){const g=ent.grappleLaunch;ent.vx+=g.dx*g.force;ent.vy=Math.max(ent.vy,5);ent.vz+=g.dz*g.force;ent.grappleImpact={owner:g.owner,life:.65,lastX:ent.x,lastZ:ent.z};ent.grappleLaunch=null}}if(ent.grappleImpact){const g=ent.grappleImpact;g.life-=dt;const wall=[...new Set(blocksNear(ent.x,ent.z,(ent.r||.6)+1).map(block=>block.structure))].find(w=>w.active!==false&&w.destructible&&Math.abs(ent.x-w.x)<w.w/2+(ent.r||.6)&&Math.abs(ent.z-w.z)<w.d/2+(ent.r||.6));if(wall){damage(ent,38,g.owner,false,{x:ent.x,y:ent.y-.7,z:ent.z},{weapon:'punch',armorMult:1.5});particles.push({kind:'ring',x:ent.x,y:ent.y-.7,z:ent.z,vx:0,vy:0,vz:0,life:.18,maxLife:.18,c:'#ff9ee5',size:22});g.life=0}else{const other=targetsFor(g.owner,isHumanActor(g.owner)).find(t=>t!==ent&&!t.dead&&Math.hypot(t.x-ent.x,t.z-ent.z)<(t.r||.6)+(ent.r||.6)+.5);if(other){damage(ent,24,g.owner,false,{x:ent.x,y:ent.y-.7,z:ent.z},{weapon:'punch'});damage(other,32,g.owner,false,{x:other.x,y:other.y-.7,z:other.z},{weapon:'punch'});g.life=0}}if(g.life<=0)ent.grappleImpact=null}if(ent.burnTimer>0&&!ent.dead){ent.burnTimer-=dt;ent.burnTick=(ent.burnTick||.2)-dt;if(ent.burnTick<=0){ent.burnTick=.25;damage(ent,(ent.burnDps||0)*.25,ent.burnOwner,false,{x:ent.x,y:ent.y-.7,z:ent.z},{weapon:'shotgun',armorMult:1.1})}if(ent.burnTimer<=0){ent.burnDps=0;ent.burnOwner=null}}}
  for(const r of plasmaResidues){r.life-=dt;for(const t of targetsFor(r.owner,isHumanActor(r.owner)))if(!t.dead&&Math.hypot(t.x-r.x,t.z-r.z)<r.r){t.slowAmount=Math.max(t.slowAmount||0,.32);t.slowTimer=Math.max(t.slowTimer||0,.18)}}plasmaResidues=plasmaResidues.filter(r=>r.life>0);
  player.momentumLock=Math.max(0,(player.momentumLock||0)-dt);player.momentumGrace=Math.max(0,(player.momentumGrace||0)-dt);if(hasUpgrade('assault','momentumClip')&&player.momentumLock<=0&&player.momentumGrace<=0)player.assaultMomentum=Math.max(0,(player.assaultMomentum||0)-dt*.16);
  player.voltDashCooldown=Math.max(0,(player.voltDashCooldown||0)-dt);player.voltKillWindow=Math.max(0,(player.voltKillWindow||0)-dt);player.slipstreamTimer=Math.max(0,(player.slipstreamTimer||0)-dt);player.slipstreamKillWindow=Math.max(0,(player.slipstreamKillWindow||0)-dt);player.tacticalTimer=Math.max(0,(player.tacticalTimer||0)-dt);
}
function tryVoltDash(ent){if(ent!==player||ent.weapon!=='smg'||!hasUpgrade('smg','voltDash')||!ent.voltDashCharge||(ent.voltDashCooldown||0)>0)return false;const speed=Math.hypot(ent.vx,ent.vz);if(speed<3)return false;const nx=ent.vx/speed,nz=ent.vz/speed;ent.vx=nx*29;ent.vz=nz*29;ent.vy=Math.max(ent.vy,4);ent.voltDashCharge=0;ent.voltDashMeter=0;ent.voltDashCooldown=2.5;if((ent.voltKillWindow||0)>0){const w=WEAPONS.smg;if(ent.reload>0){ent.reload*=.72;ent.reloadTotal=Math.max(ent.reload,ent.reloadTotal*.72)}else ent.ammo=Math.min(w.mag,ent.ammo+5)}addTier3Fx({type:'dash',x:ent.x,y:ent.y-.9,z:ent.z,dx:-nx,dy:0,dz:-nz,len:4.8,color:'#65f7ff',size:5,life:.24});particles.push({kind:'ring',x:ent.x,y:ent.y-1,z:ent.z,vx:0,vy:0,vz:0,life:.18,maxLife:.18,c:'#ffea00',size:20});return true}
function registerUpgradeKill(killer){if(killer!==player)return;if(hasUpgrade('assault','momentumClip')&&player.weapon==='assault'){player.momentumLock=.9;player.momentumGrace=1.1}if(hasUpgrade('smg','voltDash')&&player.weapon==='smg')player.voltKillWindow=1.5;if(hasUpgrade('smg','slipstream')&&player.weapon==='smg')player.slipstreamKillWindow=2;if(player.weapon==='burst'&&hasUpgrade('burst','tacticalRollout')&&(player.tacticalTimer||0)>0)player.ammo=Math.min(WEAPONS.burst.mag,player.ammo+5)}
const DIFF={easy:{aim:.28,react:.52,move:10.8,fire:.82,strategy:.55,lockRate:.18},normal:{aim:.44,react:.34,move:12.8,fire:.60,strategy:.75,lockRate:.32},hard:{aim:.70,react:.18,move:15.3,fire:.36,strategy:1.22,lockRate:.55},nightmare:{aim:.88,react:.10,move:17.6,fire:.24,strategy:1.58,lockRate:.82}};
const FRAG_KILL_LIMIT=10;
const game={started:false,map:'pastel',mode:'dm',difficulty:'normal',botCount:6,time:300,timeLeft:300,over:false,upgradesOpen:false,secondPlayer:false,secondInput:'off',dmItemSpawnTimer:15};
const survivalProgress=window.NeonSurvivalProgress?.store;
const SURVIVAL_NEW_KINDS=['leaper','stalker','screecher','artillery','lurker','warder'];
let survivalMapRuntime=window.NeonSurvivalMaps||null,survivalController=null,survivalInterface=null,survivalStarting=false;
let tacticalController=null;
let warMap=null,conquest=null,campaignRun=null,warClock=0,warActorId=0,warInteract=false,warEscort=null,warSightingCache=new Map();
const survivalEquipment=window.NeonSurvivalEquipment?.create({
  eyeHeight:PLAYER_H,unlocked:wave=>(survivalProgress?.getState().highestWave||0)>=wave,
  scrap:()=>game.scrap||0,spend:cost=>{game.scrap-=cost;},ground:(x,z)=>topAtPoint(x,z),
  clear:lineClear,notice:msg,aliens:survivorAliveAliens,time:()=>game.survivalElapsed||0,
  damage:(a,n,owner)=>damage(a,n,owner,false,null,{weapon:'equipment',minimalFx:true}),burst,
  line:(x,y,z,tx,ty,tz,color)=>{const len=Math.hypot(tx-x,ty-y,tz-z)||1;shots.push({x,y,z,dx:(tx-x)/len,dy:(ty-y)/len,dz:(tz-z)/len,len,color,life:.14,maxLife:.14,width:2});}
},window.NeonSurvivalProgress?.equipment||[]);
const MAP_META={
  grid:{name:'Circuit Citadel',desc:'Generator compounds, loading yards, connected rooms, and raised service routes.',tag:'Industrial · Flanking routes'},
  pastel:{name:'Prism Garden',desc:'Research wings, laboratory courtyards, observation bridges, and broad garden ramps.',tag:'Research · Fast rotations'},
  reactor:{name:'Obsidian Reactor',desc:'Turbine chambers, a shielded core, twin maintenance tunnels, and ramped galleries.',tag:'Reactor · Vertical combat'},
  terminal:{name:'Skyrail Terminal',desc:'A skyrail concourse with staggered trains, raised passenger platforms and linked service shops.',tag:'Transit hub'},
  foundry:{name:'Ashfall Foundry',desc:'An asymmetric furnace yard with turbine halls, loading bays and connected maintenance galleries.',tag:'Industrial works'},
  outpost:{name:'Rift Outpost',desc:'A desert research outpost with cliff approaches, ancient arches and a raised survey route.',tag:'Alien frontier'},
  survivor:{name:'Breach Ring',desc:'A wide defensive floor surrounded by a destructible perimeter that the alien squads must tear open.',tag:'Survival-only arena'}
};
for(const map of window.NeonWarMaps?.catalog()||[])MAP_META[map.id]={name:map.name,desc:map.desc,tag:map.tag,preview:map.preview};
const MATCH_FIELD_IDS=['botCount','difficulty','matchLen','scoreTarget','respawnDelay'];
let matchPreferences=NeonMatchSettings.preferences(),menuActiveMode='dm',menuSettingsReady=false,menuRestoring=false,menuSurvivalVisible=null;
function readModeFields(mode=el('mode').value){return NeonMatchSettings.modeSettings(mode,Object.fromEntries(MATCH_FIELD_IDS.map(id=>[id,el(id)?.value])))}
function applyModeFields(mode){const values=matchPreferences.modes[mode];for(const id of MATCH_FIELD_IDS)if(el(id))el(id).value=String(values[id])}
function persistMatchPreferences(){try{localStorage.setItem('neonArenaMatchSettingsV1',JSON.stringify(matchPreferences))}catch{}}
function loadMatchPreferences(){
  let raw={common:{secondPlayer:el('secondPlayer').value,visualQuality:el('visualQuality').value}};try{raw=JSON.parse(localStorage.getItem('neonArenaMatchSettingsV1')||'null')||raw}catch{}
  matchPreferences=NeonMatchSettings.preferences(raw);menuActiveMode=matchPreferences.mode;el('mode').value=menuActiveMode;el('map').value=matchPreferences.maps[menuActiveMode]||'grid';applyModeFields(menuActiveMode);
  const common=matchPreferences.common;el('secondPlayer').value=common.secondPlayer;el('lookSensitivity').value=common.lookSensitivity;el('soundVolume').value=common.volume;el('visualQuality').value=common.visualQuality;game.lookSensitivity=common.lookSensitivity;game.volume=common.volume/100;menuSettingsReady=true;
}
function saveMatchPreferencesFromFields(){
  const mode=NeonMatchSettings.MODES.includes(el('mode').value)?el('mode').value:'dm';matchPreferences.mode=mode;matchPreferences.modes[mode]=readModeFields(mode);
  matchPreferences.maps[mode]=mode==='survivor'?(survivalInterface?.selectedMap||matchPreferences.maps.survivor):el('map').value;
  matchPreferences.common={secondPlayer:el('secondPlayer').value,lookSensitivity:el('lookSensitivity').value,volume:el('soundVolume').value,visualQuality:el('visualQuality').value};matchPreferences=NeonMatchSettings.preferences(matchPreferences);game.lookSensitivity=matchPreferences.common.lookSensitivity;game.volume=matchPreferences.common.volume/100;persistMatchPreferences();return matchPreferences.modes[mode];
}
function restoreSurvivalMenuMap(){if(!survivalInterface)return;const id=matchPreferences.maps.survivor;menuRestoring=true;survivalInterface.selectMap(id);menuRestoring=false;matchPreferences.maps.survivor=survivalInterface.selectedMap}
function combatVolume(){return clamp(Number(game.volume??1),0,1)}
function fragKillLimit(){return game.mode==='frag'?(game.scoreTarget||FRAG_KILL_LIMIT):FRAG_KILL_LIMIT}
function updateMenuSummary(){
  if(menuRestoring)return;const mode=el('mode')?.value||'dm',survivor=mode==='survivor',changed=mode!==menuActiveMode;
  if(menuSettingsReady&&changed){matchPreferences.modes[menuActiveMode]=readModeFields(menuActiveMode);if(menuActiveMode!=='survivor')matchPreferences.maps[menuActiveMode]=el('map').value;menuActiveMode=mode;matchPreferences.mode=mode;applyModeFields(mode);if(!survivor)el('map').value=matchPreferences.maps[mode];else restoreSurvivalMenuMap()}
  window.NeonWarUI?.refresh();
  const selected=survivalInterface?.selectedMap||matchPreferences.maps.survivor,survivalMeta=window.NeonSurvivalProgress?.maps.find(m=>m.id===selected),id=survivor?selected:el('map')?.value||'grid',meta=survivor?{name:survivalMeta?.name||'Breach Ring',desc:survivalMeta?.description||MAP_META.survivor.desc,tag:survivalMeta?.tag||'Endless survival'}:MAP_META[id]||MAP_META.grid;
  el('map').disabled=survivor;el('arenaCards').hidden=survivor;el('survivorArenaCard').hidden=!survivor;if(survivalInterface&&menuSurvivalVisible!==survivor){survivalInterface.setVisible(survivor);menuSurvivalVisible=survivor}
  for(const card of document.querySelectorAll('[data-mode]')){const on=card.dataset.mode===mode;card.classList.toggle('selected',on);card.setAttribute('aria-checked',String(on));card.tabIndex=on?0:-1}for(const card of el('arenaCards').querySelectorAll('[data-map]')){const on=card.dataset.map===id;card.classList.toggle('selected',on);card.setAttribute('aria-checked',String(on));card.tabIndex=on?0:-1}
  const description={conquest:'Capture connected sectors, hold the front, and earn reinforcements every 15 seconds.',campaign:'Choose a faction and mission. Infiltrate, sabotage, rescue, and extract with saved checkpoints.',dm:'Every fighter for themselves. Choose your arena, opponents and elimination target.',tdm:'Fight together across connected routes. The first team to your target wins.',frag:'Sniper-only 1v1 duels, with a bot or a second local player.',siege:'Fortify the selected arena for 60 seconds, then repel the raiders.',survivor:'Endless alien waves. Clear milestones to unlock eight distinctive environments.'};el('modeDescription').textContent=description[mode];el('arenaTag').textContent=(survivor?'8 SURVIVAL ENVIRONMENTS':'12 ARENAS')+' · '+mode.toUpperCase();el('arenaHeading').textContent='02 · CHOOSE AN ARENA';el('mapDescription').textContent=meta.desc;el('selectedMapName').textContent=meta.name;el('selectedMapType').textContent=meta.tag;el('selectedMapMode').textContent=({campaign:'STORY CAMPAIGN',conquest:'TERRITORIAL CONQUEST',dm:'DEATHMATCH',tdm:'TEAM DEATHMATCH',frag:'SNIPER DUEL',siege:'SIEGE DEFENSE',survivor:'ENDLESS SURVIVAL'})[mode];
  const image=el('selectedMapImage'),src=survivor?'./assets/survival/tilesets/'+id+'/map_card.webp':(MAP_META[id].preview||'./assets/ui/maps/'+id+'.webp');if(image.getAttribute('src')!==src)image.src=src;image.alt=meta.name+(window.NeonWarMaps.get(id)?' tactical layout':' in-game environment');
  const hasSecond=el('secondPlayer').value!=='off';for(const option of el('botCount').options)option.disabled=mode==='frag'?+option.value>1:mode==='siege'?+option.value<2:false;
  const visibility={botCount:!survivor&&!['campaign','conquest'].includes(mode),matchLen:mode==='dm'||mode==='tdm',scoreTarget:['dm','tdm','frag'].includes(mode),respawnDelay:['dm','tdm','frag'].includes(mode)};for(const [field,show]of Object.entries(visibility)){el(field).closest('.fieldCard').hidden=!show;el(field).disabled=!show||(field==='botCount'&&mode==='frag'&&hasSecond)}
  if(mode==='frag'&&hasSecond)el('botCount').value='0';else if(mode==='siege'&&+el('botCount').value<2)el('botCount').value='2';else if(mode==='frag'&&+el('botCount').value>1)el('botCount').value='1';
  el('difficultyLabel').textContent=survivor?'Alien skill':'Bot skill';el('difficulty').disabled=!['survivor','conquest','campaign'].includes(mode)&&(+el('botCount').value===0||(mode==='frag'&&hasSecond));el('settingsModeNote').textContent=survivor?'Alien counts follow each wave. Survival has no score or time limit.':mode==='siege'?'Siege uses 2–16 raiders, five rounds and limited lives.':mode==='frag'?(hasSecond?'Your second player is the opponent; bots are off.':'One bot for a duel, or zero for sniper practice.'):'Choose zero bots for practice, or add up to 16 opponents.';
  el('lookSensitivityValue').textContent=Number(el('lookSensitivity').value).toFixed(2)+'×';el('soundVolumeValue').textContent=el('soundVolume').value+'%';const difficulty=el('difficulty').value,botSummary=survivor?'Alien waves':mode==='frag'&&hasSecond?'Local duel':el('botCount').value+' bots';el('settingsSummary').textContent=botSummary+' · '+difficulty+(visibility.matchLen?' · '+(+el('matchLen').value/60)+' min':'');
  if(menuSettingsReady){matchPreferences.mode=mode;matchPreferences.maps[mode]=id;persistMatchPreferences()}
}
const player={entityId:'p1',name:'YOU',color:'#ffffff',human:true,x:0,y:PLAYER_H,z:24,vx:0,vy:0,vz:0,yaw:Math.PI,pitch:0,onGround:false,r:.50,team:1,score:0,deaths:0,dead:false,hp:100,maxHp:100,armor:50,maxArmor:100,weapon:'assault',ammo:30,reload:0,cool:0,heat:0,spawnGrace:0,upgradePoints:0,killStreak:0,upgrades:null,slowTimer:0,slowAmount:0,controllerFireHeld:false};
function makeHumanPlayer(name,color,entityId='p2'){return {entityId,name,x:0,y:PLAYER_H,z:24,vx:0,vy:0,vz:0,yaw:0,pitch:0,onGround:false,r:.50,team:2,score:0,deaths:0,dead:false,hp:100,maxHp:100,armor:50,maxArmor:100,weapon:'pistol',ammo:6,reload:0,cool:0,heat:0,menderOverheated:false,spawnGrace:0,upgradePoints:0,killStreak:0,upgrades:null,slowTimer:0,slowAmount:0,color,human:true,faceYaw:0,walk:0,controllerFireHeld:false,buildHeld:false,buildPreview:null,buildCharges:BUILD_MAX,buildRecharge:0,buildFeedback:0,buildType:'wall'};}
let player2=null,menuPadPrev={},menuAxisLock=false,activeControllerTab=1,p2KeyPrev={},viewEnt=player,viewRect={x:0,y:0,w:W,h:H};
let bots=[],platforms=[],walls=[],activeBlocks=[],activeStructuresCache=[],nav=[],pickups=[],shots=[],projectiles=[],particles=[],lasers=[],impacts=[],muzzleFlashes=[],decals=[],ultimateFx=[],plasmaResidues=[],buildPieces=[],blocksDirty=true,worldGeometryDirty=true;
const BLOCK_BUCKET=8,blockSpatial=new Map();
const gridSize=4,WALL_H=4,WALL_T=.55,PLATFORM_CELLS=1,PLATFORM_SIZE=gridSize*PLATFORM_CELLS,PLATFORM_H=.55,BUILD_LEVEL=WALL_H,BUILD_MAX=10,BUILD_RECHARGE=.8,BUILD_CELL_LIMIT=2,BUILD_RANGE=26,BUILD_MAX_STACK=8,MAX_ACTIVE_BUILDS=320,PLATFORM_EDGE_HINT=8.75,PLATFORM_LOOKDOWN_EDGE_HINT=12.5;
// Build intent rules:
// - Assisted ground aim builds cover; wall-top aim stacks the matching wall.
// - Upper wall sides and elevated platform edges extend a flush standing surface.
// - Looking just above a wall stacks a wall even when the ray misses the top face.
// - Platforms cannot be placed at floor level; they must attach to a wall or elevated structure.
let spawnIndex=0,edgeSpawns=[];
let buildCharges=BUILD_MAX,buildRecharge=0,structureId=0,activeBuildCount=0,buildFeedback=0,selectedBuildType='wall';const buildOccupancy=new Map();
let mapTheme={sky:[.78,.88,.96],floorA:'#a9bbcf',floorB:'#b5c7d9',grid:'#526a85',gridSoft:'#71869d'};
const MAX_PARTICLES=170,MAX_IMPACTS=70,MAX_MUZZLE_FLASHES=28,MAX_DECALS=60,ARENA_SAFE=64.3,MAX_CHAIN_JUMPS=5,MAX_PLASMA_MINES=3,MAX_REMOTE_GRENADES=2,MAX_PRISM_SPLITS=2;
// Rendering presets only change decorative density and draw detail. Simulation counts,
// hit detection, damage, AI, projectile updates, and wave logic never consult these values.
const VISUAL_PRESETS={
  high:{particleScale:1,decalLife:1,glow:1,maxParticles:170,maxImpacts:70,maxMuzzles:28,maxDecals:60,maxTier3:28,tier3Lines:26,minePulses:5,near:30,mid:58,far:92,streaks:3},
  medium:{particleScale:.62,decalLife:.68,glow:.62,maxParticles:120,maxImpacts:52,maxMuzzles:24,maxDecals:50,maxTier3:24,tier3Lines:20,minePulses:4,near:25,mid:50,far:84,streaks:2},
  low:{particleScale:.34,decalLife:.42,glow:.30,maxParticles:78,maxImpacts:36,maxMuzzles:20,maxDecals:45,maxTier3:20,tier3Lines:14,minePulses:3,near:20,mid:40,far:72,streaks:1}
};
let visualMode='auto',visualQuality='high',minimapTimer=0,minimapNeedsRedraw=true,visualFrame={tier3Lines:0,minePulses:new Map(),occlusion:new Map(),occlusionChecks:0};
function visualPreset(){return VISUAL_PRESETS[visualQuality]||VISUAL_PRESETS.high}
function qualityForScale(scale){return scale<.73?'low':(scale<.90?'medium':'high')}
function applyVisualMode(mode,persist=true){visualMode=['auto','high','medium','low'].includes(mode)?mode:'auto';if(visualMode==='auto')visualQuality=qualityForScale(renderer.scale);else{visualQuality=visualMode;renderer.scale=visualMode==='low'?.72:(visualMode==='medium'?.86:1);resize()}if(persist)try{localStorage.setItem('neonArenaVisualQuality',visualMode)}catch{}}
function updateAdaptiveQuality(frameMs,now){if(visualMode!=='auto'||!Number.isFinite(frameMs)||frameMs<=0)return;renderer.avgMs=renderer.avgMs*.93+Math.min(80,frameMs)*.07;if(now<renderer.nextTune)return;renderer.nextTune=now+1800;const fps=1000/renderer.avgMs,old=renderer.scale;if(fps<47){renderer.scale=Math.max(renderer.minScale,renderer.scale-.10);renderer.stableTicks=0}else if(fps>57){renderer.stableTicks++;if(renderer.stableTicks>=2){renderer.scale=Math.min(renderer.maxScale,renderer.scale+.06);renderer.stableTicks=0}}else renderer.stableTicks=0;if(Math.abs(old-renderer.scale)>.001){visualQuality=qualityForScale(renderer.scale);resize()}}
function scaledVisualCount(count,min=1){return Math.max(min,Math.round(count*visualPreset().particleScale))}
function trimVisualArray(list,max){if(list.length>max)list.splice(0,list.length-max)}
function visualDistance(x,y,z,ent=viewEnt||player){return Math.hypot(x-ent.x,y-(ent.y-.6),z-ent.z)}
function visualDetailAt(x,y,z){const d=visualDistance(x,y,z),v=visualPreset();return d<=v.near?2:(d<=v.mid?1:(d<=v.far?0:-1))}
function pointInView(q,pad=48){return !!q&&q.x>=viewRect.x-pad&&q.x<=viewRect.x+viewRect.w+pad&&q.y>=viewRect.y-pad&&q.y<=viewRect.y+viewRect.h+pad}
function beginVisualView(){visualFrame.tier3Lines=0;visualFrame.minePulses.clear();visualFrame.occlusion.clear();visualFrame.occlusionChecks=0}
function visualVisible(x,y,z,ent=viewEnt||player){const key=Math.round(x*.5)+':'+Math.round(y*.5)+':'+Math.round(z*.5),cached=visualFrame.occlusion.get(key);if(cached!==undefined)return cached;const limit=visualQuality==='high'?52:(visualQuality==='medium'?36:24),distance=Math.hypot(x-ent.x,y-ent.y,z-ent.z);if(visualFrame.occlusionChecks>=limit){const fallback=distance<4.5;visualFrame.occlusion.set(key,fallback);return fallback}visualFrame.occlusionChecks++;const eye=actorEyePosition(ent),visible=lineClear(eye.x,eye.y-.05,eye.z,x,y,z);visualFrame.occlusion.set(key,visible);return visible}
function takeTier3Lines(count=1){const cap=visualPreset().tier3Lines;if(visualFrame.tier3Lines+count>cap)return false;visualFrame.tier3Lines+=count;return true}
function takeMinePulse(owner){const key=owner||player,n=(visualFrame.minePulses.get(key)||0)+1;if(n>visualPreset().minePulses)return false;visualFrame.minePulses.set(key,n);return true}
function makeStructurePiece(type,o={}){const built=!!o.builtBy,safety=!!o.safety,area=(o.w||1)*(o.d||1);let maxHp=o.maxHp||(type==='platform'?(built?1500:(area>300?2200:1850)):(built?1200:(area>180?1900:1450)));if(type==='wall')maxHp=Math.max(80,Math.round(maxHp*.5));return {...o,id:++structureId,type,hp:safety?Infinity:maxHp,maxHp:safety?Infinity:maxHp,bonusHp:0,maxBonusHp:500,armorBoosted:false,fortifySealTimer:0,destructible:!safety,active:true,createdAt:performance.now()/1000}}
function addPlat(x,z,w,d,y,h,c,safety=false){x*=WORLD_SCALE;z*=WORLD_SCALE;w*=WORLD_SCALE;d*=WORLD_SCALE;platforms.push(makeStructurePiece('platform',{x,z,w,d,y,h,c,safety}));markBlocksDirty();nav.push({x,z,y:y+h+PLAYER_H,r:Math.max(2.2,Math.min(6,Math.max(w,d)*.34))})}
function addTopPlat(x,z,w,d,top,h,c){addPlat(x,z,w,d,top-h,h,c)}
function addWall(x,z,w,d,h,c,y=0,safety=false){x*=WORLD_SCALE;z*=WORLD_SCALE;w*=WORLD_SCALE;d*=WORLD_SCALE;walls.push(makeStructurePiece('wall',{x,z,w,d,y:y+h/2,h,c,safety}));markBlocksDirty();}
function addNav(x,z,r=2.5){x*=WORLD_SCALE;z*=WORLD_SCALE;r*=WORLD_SCALE;const top=topAtPoint(x,z);if(top>-20)nav.push({x,z,y:top+PLAYER_H,r})}
function addPickup(type,x,z){if(game.mode==='frag')return;x*=WORLD_SCALE;z*=WORLD_SCALE;const top=game.mode==='survivor'?Math.max(0,topAtPoint(x,z)):0,oneShot=game.mode==='survivor';pickups.push({kind:'weapon',type,x,z,y:top+.7,spin:rand(0,TAU),taken:0,respawn:oneShot?0:10,oneShot,gone:false})}
function addArmorPickup(size,x,z){if(game.mode==='frag')return;x*=WORLD_SCALE;z*=WORLD_SCALE;const top=game.mode==='survivor'?Math.max(0,topAtPoint(x,z)):0,oneShot=game.mode==='survivor';pickups.push({kind:'armor',size,amount:size==='large'?50:25,x,z,y:top+.55,spin:rand(0,TAU),taken:0,respawn:oneShot?0:(size==='large'?18:12),oneShot,gone:false})}
function addHealthPickup(x,z){if(game.mode==='frag')return;x*=WORLD_SCALE;z*=WORLD_SCALE;const top=game.mode==='survivor'?Math.max(0,topAtPoint(x,z)):0,oneShot=game.mode==='survivor';pickups.push({kind:'health',size:'small',amount:10,x,z,y:top+.55,spin:rand(0,TAU),taken:0,respawn:oneShot?0:14,oneShot,gone:false})}
function addArenaBounds(){
  const c=game.map==='reactor'?['#124b5a','#55243f','#183a50','#49243d']:['#00eaff','#ff2bbd','#00ffa8','#ffea00'];
  addWall(0,-46,96,2.2,22,c[0],0,true);addWall(0,46,96,2.2,22,c[1],0,true);
  addWall(-46,0,2.2,96,22,c[2],0,true);addWall(46,0,2.2,96,22,c[3],0,true);
}
const SURVIVOR_BREACH_RING=39, SURVIVOR_SPAWN_EDGE=43;
function survivorBreachHpForWave(wave){return wave>=14?180:(wave>=9?110:50)}
function addSurvivorEdgeWall(x,z,w,d,hp=50,c='#ff496c'){
  addWall(x,z,w,d,4,c,0,false);
  const piece=walls[walls.length-1];
  piece.survivorEdgeWall=true;
  piece.maxHp=piece.hp=hp;
  piece.c=c;
  return piece;
}
function addSurvivorBreachRing(wave=1){
  const hp=survivorBreachHpForWave(wave),c=wave>=14?'#7dffb2':(wave>=9?'#ffb347':'#ff496c'),r=SURVIVOR_BREACH_RING;
  for(let v=-36;v<=36;v+=4){
    addSurvivorEdgeWall(v,-r,3.45,.65,hp,c);addSurvivorEdgeWall(v,r,3.45,.65,hp,c);
    addSurvivorEdgeWall(-r,v,.65,3.45,hp,c);addSurvivorEdgeWall(r,v,.65,3.45,hp,c);
  }
}
function reinforceSurvivorBreaches(wave){
  // Survivor mode must not spawn a second or replacement wall set mid-run.
  // The only perimeter walls are the original weak red breach walls created at map start.
  return;
}
function insideSurvivorRing(ent){const lim=(SURVIVOR_BREACH_RING-1.1)*WORLD_SCALE;return Math.abs(ent.x)<lim&&Math.abs(ent.z)<lim;}
function survivorBreachSpawnPoint(){
  const e=SURVIVOR_SPAWN_EDGE*WORLD_SCALE,r=(SURVIVOR_BREACH_RING-3)*WORLD_SCALE,side=(Math.random()*4)|0,v=rand(-r,r);
  if(side===0)return {x:v,z:-e}; if(side===1)return {x:v,z:e}; if(side===2)return {x:-e,z:v}; return {x:e,z:v};
}
function survivorWallSideFromPoint(x,z){
  if(Math.abs(x)>Math.abs(z))return x<0?'west':'east';
  return z<0?'north':'south';
}
function survivorWallSide(w){
  if(Math.abs(w.x)>Math.abs(w.z))return w.x<0?'west':'east';
  return w.z<0?'north':'south';
}
function survivorOpeningsForSide(side){
  const holes=[];
  for(const w of walls){
    if(!w.survivorEdgeWall||w.active!==false||survivorWallSide(w)!==side)continue;
    holes.push({x:w.x,z:w.z,side,w});
  }
  return holes;
}
function survivorInwardWaypoint(side,x,z,extra=5.5){
  const r=SURVIVOR_BREACH_RING*WORLD_SCALE;
  if(side==='north')return {x,z:-r+extra};
  if(side==='south')return {x,z:r-extra};
  if(side==='west')return {x:-r+extra,z};
  return {x:r-extra,z};
}
function survivorBreachPointForSide(side,h){
  const r=SURVIVOR_BREACH_RING*WORLD_SCALE,span=(SURVIVOR_BREACH_RING-3.0)*WORLD_SCALE;
  const hx=clamp(h?.x||0,-span,span),hz=clamp(h?.z||0,-span,span);
  if(side==='north')return {x:hx,z:-r};
  if(side==='south')return {x:hx,z:r};
  if(side==='west')return {x:-r,z:hz};
  return {x:r,z:hz};
}
function survivorWallCoveringPoint(side,pt,pad=1.25){
  let best=null,bd=1e9;
  for(const w of walls){
    if(w.active===false||!w.survivorEdgeWall||survivorWallSide(w)!==side)continue;
    const covers=Math.abs(pt.x-w.x)<=w.w/2+pad&&Math.abs(pt.z-w.z)<=w.d/2+pad;
    const d=Math.hypot(pt.x-w.x,pt.z-w.z);
    if((covers||d<bd)&&d<bd){bd=d;best=w;if(covers)break;}
  }
  if(best&&Math.abs(pt.x-best.x)<=best.w/2+pad&&Math.abs(pt.z-best.z)<=best.d/2+pad)return best;
  return null;
}
function survivorNearestWallOnSide(side,pt){
  let best=null,bd=1e9;
  for(const w of walls){
    if(w.active===false||!w.survivorEdgeWall||survivorWallSide(w)!==side)continue;
    const d=Math.hypot(pt.x-w.x,pt.z-w.z);
    if(d<bd){bd=d;best=w;}
  }
  return best;
}
function survivorRouteThroughHole(a,h){
  if(!h||insideSurvivorRing(a))return null;
  const side=survivorWallSideFromPoint(a.x,a.z),holes=survivorOpeningsForSide(side);
  let best=null,bs=1e9;
  for(const hole of holes){
    const da=Math.hypot(a.x-hole.x,a.z-hole.z),dh=Math.hypot(h.x-hole.x,h.z-hole.z),score=da+dh*.28;
    if(score<bs){bs=score;best={...hole,distFromAlien:da,score};}
  }
  return best;
}
function survivorBreachRoute(a,h){
  if(game.mode!=='survivor'||game.survivalMap!=='breach_ring'||!a.alien||a.flying||insideSurvivorRing(a)||!h)return null;
  const side=survivorWallSideFromPoint(a.x,a.z);
  const desired=survivorBreachPointForSide(side,h);
  const hole=survivorRouteThroughHole(a,h),maxHoleWalk=22*WORLD_SCALE;

  // If a nearby hole already exists, commit through it and keep moving inward before normal chase resumes.
  if(hole&&hole.distFromAlien<=maxHoleWalk)return {waypoint:survivorInwardWaypoint(side,hole.x,hole.z,7.5),hole};

  // Otherwise make a hole on this side, preferably at the point closest to the survivor's current position.
  // This prevents aliens from freezing at the edge or walking around the whole map to a distant gap.
  let wall=survivorWallCoveringPoint(side,desired,1.45)||survivorNearestWallOnSide(side,desired);
  if(wall)return {chew:wall};

  // If that side has no intact wall left, step inward through the closest available breach lane.
  return {waypoint:survivorInwardWaypoint(side,desired.x,desired.z,7.5)};
}
function survivorBreachTarget(a){
  const route=survivorBreachRoute(a,a.enemy||nearestHuman(a).e);
  return route?.chew||null;
}
function buildMap(){
  if(typeof tacticalController!=='undefined')tacticalController?.clear();
  if(typeof explosionSystem!=='undefined')explosionSystem?.clear();
  arenaRamps=[];platforms=[];walls=[];activeBlocks.length=0;blocksDirty=true;nav=[];pickups=[];buildPieces=[];buildOccupancy.clear();structureId=0;activeBuildCount=0;edgeSpawns=[];spawnIndex=0;
  if(game.mode==='survivor'){
    if(!survivalMapRuntime?.loaded)throw Error('The selected survival arena has not finished loading.');
    survivalMapRuntime.install({addPlatform:(...args)=>{addPlat(...args);return platforms.at(-1)},addWall:(...args)=>{addWall(...args);return walls.at(-1)},addSpawn:s=>edgeSpawns.push(s),addNav:n=>nav.push({...n,mapHeight:true}),setTheme:t=>{mapTheme=t;}});
  }else if(globalThis.NeonWarMaps?.get(game.map)){
    arenaProps=[];arenaAmbient=[];globalThis.NeonWarMaps.build(game.map,{arenaWall,arenaDeck,arenaRamp,arenaProp,arenaSurface,addPlat:(...args)=>{addPlat(...args);return platforms.at(-1)},addNavPoint:p=>nav.push({...p,x:p.x*WORLD_SCALE,z:p.z*WORLD_SCALE}),addArenaBounds,arenaNavAndSpawns,arenaLoadoutPickups,setTheme:t=>{mapTheme=t},addAmbient:a=>arenaAmbient.push(a)});
    warMap=window.NeonWarMaps.worldMetadata(game.map,WORLD_SCALE);
  }else if(game.map==='pastel')buildPastelArenaMap();
  else if(game.map==='reactor')buildReactorArenaMap();
  else if(game.map==='terminal')buildTerminalArenaMap();
  else if(game.map==='foundry')buildFoundryArenaMap();
  else if(game.map==='outpost')buildOutpostArenaMap();
  else buildStarterGridArenaMap();
  if(game.mode!=='survivor')survivalMapRuntime?.installScenery?.(arenaProps,{addWall:(...args)=>{addWall(...args);return walls.at(-1)}},arenaAmbient);
  nav=nav.filter(n=>n.mapHeight);
  const geometryReport=survivalMapRuntime?.validateGeometry?.({id:game.mode==='survivor'?game.survivalMap:game.map,walls,platforms,ramps:game.mode==='survivor'?[]:arenaRamps,worldCoordinates:true});
  if(geometryReport&&!geometryReport.valid)throw Error('Invalid arena geometry: '+geometryReport.errors.map(e=>e.message).join('; '));
  registerStaticGridOccupancy();
  rebuildActiveBlocks();
  for(const n of nav)if(!n.mapHeight)n.y=topAtPoint(n.x,n.z)+PLAYER_H;
}
function buildSurvivorBreachMap(){
  mapTheme={sky:[.70,.88,.98],floorA:'#d8eef5',floorB:'#c9e3ec',grid:'#6eaec2',gridSoft:'#92bfcb'};
  // Survivor starts clean: one flat arena floor and one weak red 50 HP breach ring.
  // The normal grid-map cover walls and the outer safety wall set are intentionally skipped here.
  addPlat(0,0,89.8,89.8,-.7,.7,'#cfe6ee',true);
  const spawnPts=[[-18,-18],[18,-18],[-18,18],[18,18],[0,0],[-24,0],[24,0],[0,-24],[0,24]];
  spawnPts.forEach(([x,z])=>{edgeSpawns.push({x:x*WORLD_SCALE,z:z*WORLD_SCALE,y:PLAYER_H,r:2.8});addNav(x,z,3)});
  for(let x=-28;x<=28;x+=14)for(let z=-28;z<=28;z+=14)addNav(x,z,3);
}
// Authored standard arenas use the same padded atlas surfaces as Survival.
// Coordinates are logical horizontal units; all heights are full world units.
let arenaRamps=[],arenaProps=[],arenaAmbient=[];
function arenaProp(prefab,x,z,w,d,h,tile='prop_02',y=0,material='metal'){arenaProps.push({prefab,x,z,w,d,h,tile,y,yaw:0,material});}
function arenaSurface(piece,tile,material='metal'){piece.surfaceTile=tile;piece.surfaceTopTile=tile.startsWith('floor_')?tile:'upper_00';piece.material=material;return piece;}
function arenaWall(x,z,w,d,h,tile='wall_01',y=0,safety=false,material='metal'){
  addWall(x,z,w,d,h,'#53697d',y,safety);return arenaSurface(walls.at(-1),tile,material);
}
function arenaDeck(x,z,w,d,top,tile='floor_04',h=.55){
  addTopPlat(x,z,w,d,top,h,'#65899b');const p=arenaSurface(platforms.at(-1),tile);p.authoredDeck=true;
  // Structural columns stand at the edges, leaving wide, fully upright passages.
  for(const sx of [-1,1])for(const sz of [-1,1])arenaWall(x+sx*(w/2-.55),z+sz*(d/2-.55),.65,.65,top-h,'trim_00');
  return p;
}
function arenaRamp(x,z,w,d,axis,fromHeight,toHeight,tile='floor_03'){
  const r={x,z,w,d,axis,fromHeight,toHeight,tile};arenaRamps.push(r);
  const steps=Math.max(12,Math.ceil(Math.abs(toHeight-fromHeight)/.14)),length=axis==='x'?w:d;
  for(let i=0;i<steps;i++){
    const t=(i+.5)/steps,top=fromHeight+(toHeight-fromHeight)*t;
    if(top<.02)continue;
    const s={x,z,w,d};s[axis]+=(t-.5)*length;s[axis==='x'?'w':'d']=length/steps+.012;
    // A solid staircase plinth has no misleading low void beneath its treads.
    addPlat(s.x,s.z,s.w,s.d,0,top,'#536e7d',true);arenaSurface(platforms.at(-1),tile);platforms.at(-1).rampCollision=true;
  }
  for(let i=0,n=Math.ceil(length/1.8);i<=n;i++){const t=i/n;nav.push({x:(x+(axis==='x'?(t-.5)*w:0))*WORLD_SCALE,z:(z+(axis==='z'?(t-.5)*d:0))*WORLD_SCALE,y:fromHeight+(toHeight-fromHeight)*t+PLAYER_H,r:2.2,mapHeight:true,ramp:true});}
  return r;
}
function arenaRampHeight(x,z){let h=-Infinity;for(const r of arenaRamps){const rx=r.x*WORLD_SCALE,rz=r.z*WORLD_SCALE,w=r.w*WORLD_SCALE,d=r.d*WORLD_SCALE;if(Math.abs(x-rx)>w/2+.001||Math.abs(z-rz)>d/2+.001)continue;const t=clamp((r.axis==='x'?(x-rx)/w:(z-rz)/d)+.5,0,1);h=Math.max(h,r.fromHeight+(r.toHeight-r.fromHeight)*t);}return h;}
function arenaRampSupport(actor){const h=arenaRampHeight(actor.x,actor.z);return Number.isFinite(h)&&Math.abs(actor.y-PLAYER_H-h)<.22;}
function followArenaRamp(actor,old){
  if(game.mode==='survivor'||actor.flying||actor.climbing||actor.vy>1)return false;
  const now=arenaRampHeight(actor.x,actor.z),before=arenaRampHeight(old.x,old.z),feet=old.y-PLAYER_H;
  if(!Number.isFinite(now))return false;
  const walking=old.grounded&&(Math.abs(feet-(Number.isFinite(before)?before:now))<.4||Math.abs(feet-now)<.3),landing=actor.vy<=0&&feet>=now-.08&&actor.y-PLAYER_H<=now+.08;
  if(!walking&&!landing)return false;actor.y=now+PLAYER_H;actor.vy=0;actor.onGround=true;return true;
}
function arenaRoom(x,z,w,d,tile='wall_01',height=5.4,material='metal'){
  // Two opposite, five-unit doors keep rooms useful as flanks, never traps.
  const door=5,segment=(w-door)/2;
  for(const side of [-1,1]){
    arenaWall(x+side*(door/2+segment/2),z-d/2,segment,.65,height,tile,0,false,material);
    arenaWall(x+side*(door/2+segment/2),z+d/2,segment,.65,height,tile,0,false,material);
  }
  arenaWall(x-w/2,z,.65,d,height,tile,0,false,material);arenaWall(x+w/2,z,.65,d,height,tile,0,false,material);
  arenaWall(x,z-d/2,door,.65,height-3.25,tile,3.25,false,material);arenaWall(x,z+d/2,door,.65,height-3.25,tile,3.25,false,material);
  addTopPlat(x,z,w,d,height+.4,.4,'#5f7282',true);const roof=arenaSurface(platforms.at(-1),'upper_00');roof.architecturalRoof=true;
  // Wall panels and cabinets use existing authored atlas control materials.
  arenaWall(x-w/2+.8,z-d/2+2,1,3,1.25,'prop_01',0,false,material);
}
function arenaCover(x,z,w,d,h=1.5,tile='wall_04',material='metal'){return arenaWall(x,z,w,d,h,tile,0,false,material);}
function arenaNavAndSpawns(points){
  const valid=(x,z,feet)=>!walls.some(w=>w.active!==false&&Math.abs(x-w.x)<w.w/2+.7&&Math.abs(z-w.z)<w.d/2+.7&&feet<w.y+w.h/2-.05&&feet+2.39>w.y-w.h/2+.05);
  for(let x=-40;x<=40;x+=5)for(let z=-40;z<=40;z+=5){const wx=x*WORLD_SCALE,wz=z*WORLD_SCALE;if(valid(wx,wz,0))nav.push({x:wx,z:wz,y:PLAYER_H,r:3.4,mapHeight:true});}
  for(const p of platforms.filter(p=>p.authoredDeck))for(let x=p.x-p.w/2+1.4;x<p.x+p.w/2-1;x+=3.4)for(let z=p.z-p.d/2+1.4;z<p.z+p.d/2-1;z+=3.4)if(valid(x,z,p.y+p.h))nav.push({x,z,y:p.y+p.h+PLAYER_H,r:2.4,mapHeight:true});
  edgeSpawns=points.map(([x,z])=>({x:x*WORLD_SCALE,z:z*WORLD_SCALE,y:PLAYER_H,r:1.7}));
}
function arenaLoadoutPickups(points){const weapons=['assault','smg','shotgun','sniper','plasma','laser','mender','burst','arc','rocket','mortar','punch','harpoon'];weapons.forEach((w,i)=>{const p=points[i%points.length];addPickup(w,...p);});for(const p of [points[0],points[3],points[6]])addArmorPickup('small',...p);addArmorPickup('large',...points.at(-1));}
function buildStarterGridArenaMap(){
  arenaRamps=[];arenaProps=[];arenaAmbient=[];mapTheme={sky:[.14,.23,.32],floorA:'#263e50',floorB:'#354f61',grid:'#5db6c9',gridSoft:'#385b6a'};
  addPlat(0,0,89.8,89.8,-.7,.7,'#324756',true);arenaSurface(platforms.at(-1),'floor_01');addArenaBounds();
  // Circuit Citadel: west loading yard, center generators, east service compound.
  arenaRoom(-25,-21,17,15,'wall_02');arenaRoom(25,20,17,17,'wall_01');arenaRoom(24,-23,16,13,'wall_03');
  arenaDeck(-24,13,18,10,4.2,'floor_04');arenaRamp(-24,27,7,18,'z',4.2,0);
  arenaDeck(12,1,21,7,4.2,'floor_04');arenaRamp(29,1,13,7,'x',4.2,0);
  // Open generator room, heavy machinery and interrupted cargo sightlines.
  arenaProp('generator',-5,-5,7,6,5.8,'prop_02');arenaProp('transformer',6,14,6,5,4.7,'prop_03');
  for(const [x,z,w,d,h] of [[-26,-1,11,4,3.1],[-34,22,5,9,3.1],[29,-4,9,4,3.1],[7,-27,7,4,3.1],[-9,31,9,4,3.1],[29,34,8,3,1.6]])arenaProp('cargo_container',x,z,w,d,h,'wall_04');
  for(const [x,z,w,d] of [[-10,-18,7,1.2],[9,-10,1.2,7],[-10,8,1.2,6],[9,28,8,1.2],[-38,-33,1,9],[38,32,1,9]])arenaCover(x,z,w,d,2.4,'wall_05');
  arenaAmbient.push({kind:'warning',x:6,z:14,y:5.1},{kind:'steam',x:-5,z:-5,y:5.8});
  arenaNavAndSpawns([[-39,-38],[0,-39],[39,-38],[-39,38],[0,39],[39,38],[-39,0],[39,12]]);
  arenaLoadoutPickups([[-25,-21],[-24,13],[25,20],[24,-23],[-14,0],[13,0],[0,24],[0,-24],[-34,32],[34,-34],[-10,-30],[10,32],[0,7]]);
}
function buildPastelArenaMap(){
  arenaRamps=[];arenaProps=[];arenaAmbient=[];mapTheme={sky:[.64,.81,.88],floorA:'#a6cbd0',floorB:'#91bdbd',grid:'#b2f6dd',gridSoft:'#5d9e9e'};
  addPlat(0,0,89.8,89.8,-.7,.7,'#a7ced0',true);arenaSurface(platforms.at(-1),'floor_05');addArenaBounds();
  // Prism Garden: two research wings frame offset greenhouse courtyards.
  arenaRoom(-25,-22,19,17,'wall_05',5.8,'glass');arenaRoom(26,21,18,18,'wall_05',5.8,'glass');
  arenaRoom(27,-24,16,13,'wall_01',5.5,'glass');
  arenaDeck(-24,15,19,10,4.3,'floor_06');arenaRamp(-24,29,8,18,'z',4.3,0,'floor_03');
  arenaDeck(11,0,22,7,4.3,'floor_06');arenaRamp(29,0,14,7,'x',4.3,0,'floor_03');
  // Staggered planter beds channel three routes without blocking crossover lanes.
  for(const [x,z,w,d] of [[-5,-9,8,6],[5,11,8,6],[-28,-3,11,4],[28,5,8,4],[-9,29,9,4],[8,-28,8,4]]){
    arenaCover(x,z,w,d,1.15,'wall_06','concrete');
    for(const side of [-1,1])arenaWall(x+side*w*.27,z,.65,.65,2.2,'prop_04',1.15,false,'organic');
  }
  arenaProp('experiment_chamber',0,-2,3.4,3.4,9,'prop_02',0,'glass');
  arenaProp('research_console',-29,-25,3.8,2,1.5,'prop_03');arenaProp('lab_machine',29,22,3,3,3.6,'prop_03');
  for(const [x,z] of [[-37,29],[36,-8],[-11,-30],[10,30]])arenaCover(x,z,6,1,2.1,'wall_04','glass');
  arenaAmbient.push({kind:'pulse',x:0,z:-2,y:8.8},{kind:'spore',x:5,z:11,y:2.5});
  arenaNavAndSpawns([[-39,-38],[0,-39],[39,-38],[-39,38],[0,39],[39,38],[-39,0],[39,10]]);
  arenaLoadoutPickups([[-25,-22],[-24,15],[26,21],[27,-24],[-12,0],[12,0],[0,23],[0,-23],[-34,32],[34,-34],[-10,-30],[10,32],[0,7]]);
}
function buildReactorArenaMap(){
  arenaRamps=[];arenaProps=[];arenaAmbient=[];mapTheme={sky:[.025,.045,.07],floorA:'#172d37',floorB:'#213b44',grid:'#23a5a6',gridSoft:'#284c55'};
  addPlat(0,0,89.8,89.8,-.8,.8,'#182c38',true);arenaSurface(platforms.at(-1),'floor_00');addArenaBounds();
  // Obsidian Reactor: west turbines, east controls, and a central shielded core.
  arenaRoom(-28,-32,16,13,'wall_02');arenaRoom(27,32,18,13,'wall_03');
  arenaProp('reactor_core',0,0,9,9,11,'prop_02');
  for(const [x,z] of [[-25,4],[25,-7]]){arenaProp('turbine',x,z,9,9,6.4,'prop_03');arenaAmbient.push({kind:'fan',x,z:z-4.32,y:3.8,r:2});}
  // Maintenance galleries are full standing height below, with four broad entries.
  arenaDeck(0,-18,48,7,4.6,'floor_04');arenaDeck(0,19,48,7,4.6,'floor_04');
  arenaRamp(-31,-18,14,7,'x',0,4.6);arenaRamp(31,-18,14,7,'x',4.6,0);
  arenaRamp(-31,19,14,7,'x',0,4.6);arenaRamp(31,19,14,7,'x',4.6,0);
  // Twin sheltered maintenance tunnels connect across the reactor without dead ends.
  for(const x of [-11,11]){arenaWall(x,0,.7,13,4,'wall_01');arenaWall(x+(x<0?-6:6),0,.7,13,4,'wall_01');}
  for(const [x,z,w,d] of [[-30,34,10,1.2],[27,-32,10,1.2],[-7,-31,6,3],[8,32,7,3],[-37,0,1,9],[37,8,1,9]])arenaCover(x,z,w,d,2.2,'wall_04');
  arenaAmbient.push({kind:'pulse',x:0,z:0,y:10.5},{kind:'warning',x:27,z:32,y:5});
  arenaNavAndSpawns([[-39,-38],[0,-39],[39,-38],[-39,38],[0,39],[39,38],[-39,12],[39,0]]);
  arenaLoadoutPickups([[-28,-32],[0,-18],[27,32],[0,19],[-14,0],[14,0],[-25,18],[25,19],[-34,32],[34,-34],[-10,-30],[10,32],[0,11]]);
}

function buildTerminalArenaMap(){
  arenaRamps=[];arenaProps=[];arenaAmbient=[];mapTheme={sky:[.14,.20,.30],floorA:'#283440',floorB:'#445264',grid:'#accadf',gridSoft:'#455765'};
  addPlat(0,0,89.8,89.8,-.7,.7,'#45505d',true);arenaSurface(platforms.at(-1),'floor_02');addArenaBounds();
  // Two long passenger platforms connect through a raised crossing concourse.
  // Trains stagger along the lower tracks, leaving a broad continuous center lane.
  arenaDeck(-20,0,9,46,4.6,'floor_06');arenaDeck(20,0,9,46,4.6,'floor_06');arenaDeck(0,0,49,9,4.6,'floor_04');
  for(const x of [-20,20]){arenaRamp(x,-31.5,8,17,'z',0,4.6);arenaRamp(x,31.5,8,17,'z',4.6,0);}
  arenaRoom(-32,-17,13,14,'wall_03',6.3);arenaRoom(32,18,13,15,'wall_04',6.3);
  arenaProp('train_carriage',-7,-24,7,20,4.8,'wall_04');arenaProp('train_carriage',7,24,7,20,4.8,'wall_04');
  arenaProp('subway_tunnel',0,-38,18,7,9,'wall_03');arenaProp('subway_tunnel',0,38,18,7,9,'wall_03');
  arenaProp('platform_canopy',-20,-14,12,16,5.8,'wall_02',4.6);arenaProp('platform_canopy',20,14,12,16,5.8,'wall_02',4.6);
  arenaProp('vending_machine',-35,-17,2,1.6,2.8,'prop_02');arenaProp('vending_machine',35,18,2,1.6,2.8,'prop_02');
  arenaProp('station_sign',0,0,7,.35,1.6,'prop_05',8.3);
  for(const [x,z] of [[-7,11],[7,-11],[-31,7],[31,-7]])arenaProp('ticket_gate',x,z,4.5,1.5,1.35,'prop_03');
  arenaAmbient.push({kind:'flicker',x:-20,z:-14,y:8.8},{kind:'flicker',x:20,z:14,y:8.8},{kind:'steam',x:-7,z:-29,y:4.7},{kind:'spark',x:7,z:29,y:3.5});
  arenaNavAndSpawns([[-39,-38],[0,-41],[39,-38],[-39,38],[0,41],[39,38],[-39,2],[39,-2]]);
  arenaLoadoutPickups([[-32,-17],[-20,0],[32,18],[20,0],[0,-10],[0,10],[-30,30],[30,-30],[-8,34],[8,-34],[-11,-2],[11,2],[0,0]]);
}
function buildFoundryArenaMap(){
  arenaRamps=[];arenaProps=[];arenaAmbient=[];mapTheme={sky:[.22,.105,.07],floorA:'#4a342c',floorB:'#654439',grid:'#d78b4e',gridSoft:'#765344'};
  addPlat(0,0,89.8,89.8,-.8,.8,'#4c3931',true);arenaSurface(platforms.at(-1),'floor_01');addArenaBounds();
  // An asymmetric L-shaped maintenance gallery overlooks the furnace yard;
  // a separate lower loading gantry offers a short flank through the east bay.
  arenaDeck(-24,0,10,38,5.2,'floor_04');arenaDeck(-5,-19,48,8,5.2,'floor_04');arenaDeck(25,15,11,18,3.9,'floor_03');
  arenaRamp(-24,28,8,18,'z',5.2,0);arenaRamp(28,-19,18,8,'x',5.2,0);
  arenaRamp(25,32.5,8,17,'z',3.9,0);arenaRamp(14,15,11,8,'x',0,3.9);
  arenaRoom(-4,31,18,14,'wall_02',7);arenaRoom(29,-33,17,12,'wall_03',6.5);
  arenaProp('reactor_core',9,-3,10,10,11.2,'prop_02');arenaProp('turbine',-9,12,9,9,6.8,'prop_03');
  arenaProp('pump',-5,-30,7,5,4.8,'prop_03');arenaProp('transformer',30,1,7,5,4.5,'wall_04');
  arenaProp('industrial_pipe',-38,-3,3,3,12,'wall_02');arenaProp('industrial_pipe',38,29,3,3,10,'wall_02');
  arenaProp('conduit_cluster',-2,29,4,4,6,'prop_02');arenaProp('floodgate',-6,-1,5,2,2.2,'wall_05');
  for(const [x,z,w,d] of [[-13,-9,7,1.2],[8,27,8,1.2],[24,-8,1.2,6],[-34,9,1.2,6]])arenaCover(x,z,w,d,2,'wall_05');
  arenaAmbient.push({kind:'fan',x:-9,z:7.68,y:4,r:2},{kind:'steam',x:9,z:-3,y:10.9},{kind:'warning',x:30,z:1,y:4.8},{kind:'spark',x:-5,z:-30,y:4.1});
  arenaNavAndSpawns([[-40,-39],[0,-40],[40,-39],[-40,39],[0,40],[40,39],[-40,0],[40,10]]);
  arenaLoadoutPickups([[-9,33],[-24,0],[29,-33],[-5,-19],[25,15],[0,7],[-12,-25],[16,29],[-34,21],[34,-23],[-16,-7],[20,5],[0,18]]);
}
function arenaMesa(x,z,w,d,top){
  // A natural mesa is visibly solid down to the desert floor, rather than a
  // thin floating slab. Its exact render box is also its physical plinth.
  addPlat(x,z,w,d,0,top,'#8b755b',true);const p=arenaSurface(platforms.at(-1),'floor_02','concrete');p.surfaceSideTile='wall_06';p.authoredDeck=true;return p;
}
function buildOutpostArenaMap(){
  arenaRamps=[];arenaProps=[];arenaAmbient=[];mapTheme={sky:[.45,.32,.24],floorA:'#8c765c',floorB:'#a38c69',grid:'#91bea1',gridSoft:'#887758'};
  addPlat(0,0,89.8,89.8,-.7,.7,'#9b8468',true);arenaSurface(platforms.at(-1),'floor_00','concrete');addArenaBounds();
  // Grounded cliff mesas and one narrow survey bridge create a frontier layout
  // around an ancient open arch, with two offset research buildings below.
  arenaMesa(-26,-23,18,17,4.8);arenaMesa(27,17,16,18,4);
  arenaDeck(-2,-23,30,6,4.8,'floor_06');
  arenaRamp(-26,-6.5,9,16,'z',4.8,0,'floor_07');arenaRamp(20.5,-23,15,6,'x',4.8,0,'floor_07');
  arenaRamp(12,17,14,9,'x',0,4,'floor_07');arenaRamp(27,33.5,9,15,'z',4,0,'floor_07');
  arenaRoom(-24,15,17,16,'wall_01',6,'concrete');arenaRoom(26,-4,15,13,'wall_03',6.5,'concrete');
  arenaProp('rock_arch',0,6,20,9,11.5,'wall_05',0,'concrete');arenaProp('meteor_formation',2,-7,8,8,3.5,'wall_06',0,'concrete');
  arenaProp('monolith',4,29,4,4,12,'prop_02',0,'concrete');arenaProp('meteor_formation',-6,-31,9,7,5,'wall_06',0,'concrete');
  arenaProp('crystal_cluster',-9,27,6,5,7,'prop_03',0,'concrete');arenaProp('crystal_cluster',34,-26,6,5,9,'prop_03',0,'concrete');
  arenaProp('crystal_cluster',-29,-23,4,4,3.2,'prop_03',4.8,'concrete');arenaProp('meteor_formation',30,21,5,5,2.8,'wall_06',4,'concrete');
  arenaProp('alien_cactus',-37,3,3,3,6,'prop_04',0,'organic');arenaProp('alien_cactus',37,34,3,3,7,'prop_04',0,'organic');
  arenaAmbient.push({kind:'dust',x:2,z:2,y:.9},{kind:'pulse',x:-9,z:27,y:5},{kind:'spore',x:0,z:6,y:6},{kind:'warning',x:26,z:-4,y:6.2});
  arenaNavAndSpawns([[-40,-39],[0,-40],[40,-39],[-40,39],[0,40],[40,39],[-40,-6],[40,3]]);
  arenaLoadoutPickups([[-24,15],[-26,-23],[26,-4],[27,17],[-9,10],[9,10],[-16,30],[19,-32],[-34,29],[34,-37],[-12,-17],[15,26],[0,20]]);
}
function applyFragArena(){
  // Frag mode is intentionally tighter: four new inner boundary walls shorten the long outer lanes
  // without destroying the cube-grid build system or the central wall square.
  const limit=['terminal','foundry','outpost'].includes(game.map)?43:38,thick=1.4,h=14,c='#0d2336';
  addWall(0,-limit,limit*2,thick,h,c,0,true);addWall(0,limit,limit*2,thick,h,c,0,true);
  addWall(-limit,0,thick,limit*2,h,c,0,true);addWall(limit,0,thick,limit*2,h,c,0,true);
  edgeSpawns=[{x:-30*WORLD_SCALE,z:-30*WORLD_SCALE,y:PLAYER_H,r:2.4},{x:30*WORLD_SCALE,z:30*WORLD_SCALE,y:PLAYER_H,r:2.4},{x:30*WORLD_SCALE,z:-30*WORLD_SCALE,y:PLAYER_H,r:2.4},{x:-30*WORLD_SCALE,z:30*WORLD_SCALE,y:PLAYER_H,r:2.4}];
  nav=nav.filter(n=>Math.abs(n.x)<=limit*WORLD_SCALE&&Math.abs(n.z)<=limit*WORLD_SCALE);
  [[-28,-28],[28,28],[28,-28],[-28,28],[0,-28],[0,28],[-28,0],[28,0],[0,0]].forEach(([x,z])=>addNav(x,z,2.8));
}
function enforceFragLoadout(ent){
  if(game.mode!=='frag'||!ent||ent.alien)return;
  ent.maxHp=100;ent.hp=100;ent.maxArmor=0;ent.armor=0;
  resetWeaponInventory(ent,'sniper');
}
function setupFragLoadout(){
  pickups=[];
  [player,player2,...bots].filter(Boolean).forEach((e,i)=>{
    if(e.alien)return;
    enforceFragLoadout(e);e.upgradePoints=0;e.buildCharges=BUILD_MAX;e.buildRecharge=0;e.buildType='wall';e.fragBuildCd=rand(.6,1.4);
    if(!e.human){e.style='marksman';applyBotPersonality(e);e.accuracyBonus=(DIFF[game.difficulty]?.aim||.4)*.18;e.aggression=.82;e.pickupBias=.05;e.aimLock=0;e.lockTarget=null;}
  });
  if(!player.dead)equip(player,'sniper',false);
  if(player2&&!player2.dead)equip(player2,'sniper',false);
}

function markBlocksDirty(){blocksDirty=true;worldGeometryDirty=true}
function blockCell(v){return Math.floor(v/BLOCK_BUCKET)}
function blockCellKey(x,z){return x+':'+z}
function addBlockToSpatial(block){const x0=blockCell(block.x-block.w/2),x1=blockCell(block.x+block.w/2),z0=blockCell(block.z-block.d/2),z1=blockCell(block.z+block.d/2);for(let gx=x0;gx<=x1;gx++)for(let gz=z0;gz<=z1;gz++){const key=blockCellKey(gx,gz),list=blockSpatial.get(key);if(list)list.push(block);else blockSpatial.set(key,[block])}}
function rebuildActiveBlocks(){
  activeBlocks.length=0;activeStructuresCache.length=0;blockSpatial.clear();
  for(const p of platforms)if(p.active!==false){const block={x:p.x,z:p.z,w:p.w,d:p.d,y:p.y+p.h/2,h:p.h,c:p.c,kind:'platform',structure:p};activeBlocks.push(block);activeStructuresCache.push(p);addBlockToSpatial(block)}
  for(const w of walls)if(w.active!==false){const block={...w,kind:'wall',structure:w};activeBlocks.push(block);activeStructuresCache.push(w);addBlockToSpatial(block)}
  blocksDirty=false;
  return activeBlocks;
}
function blocks(){return blocksDirty?rebuildActiveBlocks():activeBlocks}
function blocksInBounds(x,z,w=0,d=0,pad=0){blocks();const x0=blockCell(x-w/2-pad),x1=blockCell(x+w/2+pad),z0=blockCell(z-d/2-pad),z1=blockCell(z+d/2+pad),found=new Set();for(let gx=x0;gx<=x1;gx++)for(let gz=z0;gz<=z1;gz++)for(const block of blockSpatial.get(blockCellKey(gx,gz))||[])found.add(block);return [...found]}
function blocksNear(x,z,r=1){return blocksInBounds(x,z,r*2,r*2,0)}
function structuresNearPiece(piece,pad=1){return [...new Set(blocksInBounds(piece.x,piece.z,piece.w||0,piece.d||0,pad).map(b=>b.structure).filter(Boolean))]}
function blocksAlongRay(o,d,range,inflate=0){blocks();const found=new Set(),step=Math.max(2,BLOCK_BUCKET*.38),side=inflate+1;for(let t=0;t<=range+step;t+=step){const x=o.x+d.x*Math.min(t,range),z=o.z+d.z*Math.min(t,range);for(const block of blocksNear(x,z,side))found.add(block)}return [...found]}
function topAtPoint(x,z){let best=typeof survivalMapRuntime!=='undefined'&&game.mode==='survivor'?survivalMapRuntime?.rampHeight(x,z)??-999:arenaRampHeight(x,z);for(const b of blocksNear(x,z,.1))if(b.kind==='platform'&&Math.abs(x-b.x)<=b.w/2&&Math.abs(z-b.z)<=b.d/2)best=Math.max(best,b.y+b.h/2);return best}
function actorOverSurface(x,z,b,r){
  const dx=x-clamp(x,b.x-b.w/2,b.x+b.w/2),dz=z-clamp(z,b.z-b.d/2,b.z+b.d/2);
  return dx*dx+dz*dz<=r*r;
}
// Keep the collider as tall as the complete authored human model (NeonModels.HUMAN_HEIGHT),
// including during a slide or crouch. Posture lowers the aim/hit volumes, while
// every traversable passage still supports standing without ceiling exploits.
function actorStandingHeight(ent){return ent.alien?PLAYER_H:2.39}
function actorRampHeight(ent){
  if(game.mode==='survivor')return typeof survivalMapRuntime!=='undefined'?survivalMapRuntime?.rampHeight(ent.x,ent.z)??-Infinity:-Infinity;
  return typeof arenaRampHeight==='function'?arenaRampHeight(ent.x,ent.z):-Infinity;
}
function resetTacticalSlide(ent){ent.slideSoundPending=false;ent.slideTimer=0;ent.slideCooldown=0;ent.slideRequest=0;ent.slidePose=0;ent.slideDirX=0;ent.slideDirZ=0;ent.slideStartSpeed=0;ent.footstepDistance=0}
function requestHumanSlide(ent){
  if(!ent||ent.dead||!ent.onGround||(ent.slideCooldown||0)>0||(ent.slideTimer||0)>0||!game.started||game.over||game.upgradesOpen||game.shopOpen)return false;
  // A short request buffer handles Shift and movement pressed in the same frame.
  ent.slideRequest=.14;return true;
}
function endTacticalSlide(ent,blocked=false){
  if(!(ent.slideTimer>0))return;
  ent.slideTimer=0;ent.slideCooldown=Math.max(ent.slideCooldown||0,.60);
  if(blocked){ent.vx*=.22;ent.vz*=.22;ent.slideRequest=0}
}
function updateTacticalSlide(ent,dt,wishX,wishZ,moveSpeed=18.4){
  ent.slideCooldown=Math.max(0,(ent.slideCooldown||0)-dt);
  if(ent.dead){endTacticalSlide(ent);ent.slideRequest=0;return false}
  const wishLength=Math.hypot(wishX,wishZ),actualSpeed=Math.hypot(ent.vx,ent.vz);
  if(ent.slideRequest>0){
    ent.slideRequest=Math.max(0,ent.slideRequest-dt);
    if(ent.onGround&&!(ent.slideCooldown>0)&&!(ent.slideTimer>0)&&(actualSpeed>2||wishLength>.16)){
      ent.slideDirX=actualSpeed>2?ent.vx/actualSpeed:wishX/wishLength;ent.slideDirZ=actualSpeed>2?ent.vz/actualSpeed:wishZ/wishLength;
      ent.slideStartSpeed=Math.min(34,Math.max(actualSpeed,moveSpeed*1.50));ent.slideTimer=.72;ent.slideRequest=0;ent.slideSoundPending=true;
    }
  }
  if(!(ent.slideTimer>0))return false;
  if(!ent.onGround&&ent.vy>1){endTacticalSlide(ent);return false}
  // Turn at most 50 degrees per second, retaining the actual direction of travel.
  if(wishLength>.16){const current=Math.atan2(ent.slideDirX,ent.slideDirZ),wanted=Math.atan2(wishX,wishZ),delta=Math.atan2(Math.sin(wanted-current),Math.cos(wanted-current)),yaw=current+clamp(delta,-.88*dt,.88*dt);ent.slideDirX=Math.sin(yaw);ent.slideDirZ=Math.cos(yaw)}
  const progress=clamp(1-ent.slideTimer/.72,0,1),speed=ent.slideStartSpeed*(1-.54*progress);
  ent.vx=ent.slideDirX*speed;ent.vz=ent.slideDirZ*speed;ent.slideTimer=Math.max(0,ent.slideTimer-dt);
  if(ent.slideTimer===0)ent.slideCooldown=Math.max(ent.slideCooldown||0,.60);
  return true;
}
function updateSlideView(ent,dt){
  const target=ent.slideTimer>0?1:0,blend=1-Math.exp(-dt*(target?17:12));ent.slidePose=(ent.slidePose||0)+(target-(ent.slidePose||0))*blend;
  if(ent.slidePose<.001)ent.slidePose=0;
  const crouchTarget=!ent.dead&&ent.crouching?1:0;ent.crouchPose=(ent.crouchPose||0)+(crouchTarget-(ent.crouchPose||0))*(1-Math.exp(-dt*14));if(ent.crouchPose<.001)ent.crouchPose=0;
  if(ent.slideSoundPending){if(typeof tacticalController!=='undefined')tacticalController?.hear({kind:'slide',x:ent.x,y:ent.y,z:ent.z,team:ent.team,source:ent.entityId||ent.name,range:12});if(typeof combatAudio==='function')combatAudio('slide',ent.x,ent.y-PLAYER_H,ent.z);ent.slideSoundPending=false}
  const speed=Math.hypot(ent.vx,ent.vz);if(!ent.dead&&ent.onGround&&ent.slidePose<.2&&speed>2){ent.footstepDistance=(ent.footstepDistance||0)+speed*dt;if(ent.footstepDistance>5.3){ent.footstepDistance%=5.3;const crouched=ent.crouching&&ent.crouchPose>.5,sprinting=!crouched&&speed>12;if(typeof tacticalController!=='undefined')tacticalController?.hear({kind:sprinting?'sprint':'footstep',x:ent.x,y:ent.y,z:ent.z,team:ent.team,source:ent.entityId||ent.name,range:crouched?3.5:sprinting?15:8});if(typeof combatAudio==='function')combatAudio('step',ent.x,ent.y-PLAYER_H,ent.z)}}else ent.footstepDistance=0;
}
function actorEyePosition(ent){
  const slide=ent.slidePose||0,crouch=ent.crouchPose??(ent.crouching?1:0),forward=slide*.10,yaw=ent.yaw||0;
  return {x:ent.x+Math.sin(yaw)*forward,y:ent.y-Math.max(slide*.78,crouch*.65)-(ent.landingImpact||0)*.05,z:ent.z+Math.cos(yaw)*forward};
}
function slideSpreadMultiplier(ent){return 1+(ent.slidePose||0)*.32}
function pushOutXZ(ent){
  const r=ent.r||.45,feet=ent.y-PLAYER_H+HEAD_CLEAR,head=ent.y-PLAYER_H+actorStandingHeight(ent),rampHeight=actorRampHeight(ent),onRamp=Number.isFinite(rampHeight)&&Math.abs(ent.y-PLAYER_H-rampHeight)<.25;
  // Two short passes resolve intersecting walls; rounded corner normals retain
  // tangential velocity so fast actors slide around cover instead of sticking.
  for(let pass=0;pass<2;pass++)for(const b of blocksNear(ent.x,ent.z,r+1)){
    if(onRamp&&b.structure?.rampCollision)continue;
    const b0=b.y-b.h/2,b1=b.y+b.h/2;if(feet>=b1-COLLISION_SKIN||head<=b0+COLLISION_SKIN)continue;
    const left=b.x-b.w/2,right=b.x+b.w/2,back=b.z-b.d/2,front=b.z+b.d/2;
    const dx=ent.x-clamp(ent.x,left,right),dz=ent.z-clamp(ent.z,back,front),distance=Math.hypot(dx,dz);
    if(distance>=r-1e-6)continue;
    let nx,nz,depth;
    if(distance>1e-6){nx=dx/distance;nz=dz/distance;depth=r-distance}
    else{
      const exits=[{d:ent.x-left,nx:-1,nz:0},{d:right-ent.x,nx:1,nz:0},{d:ent.z-back,nx:0,nz:-1},{d:front-ent.z,nx:0,nz:1}];
      const exit=exits.reduce((best,item)=>item.d<best.d?item:best);nx=exit.nx;nz=exit.nz;depth=exit.d+r;
    }
    ent.x+=nx*(depth+1e-6);ent.z+=nz*(depth+1e-6);
    const inward=ent.vx*nx+ent.vz*nz;if(inward<0){ent.vx-=inward*nx;ent.vz-=inward*nz;if(ent.slideTimer>0&&inward<-1)endTacticalSlide(ent,true)}
  }
  // Keep even externally accelerated actors inside the inner boundary faces.
  const lim=ARENA_SAFE-r;ent.x=clamp(ent.x,-lim,lim);ent.z=clamp(ent.z,-lim,lim);if(Math.abs(ent.x)>=lim-.001){ent.vx=0;if(ent.slideTimer>0)endTacticalSlide(ent,true)}if(Math.abs(ent.z)>=lim-.001){ent.vz=0;if(ent.slideTimer>0)endTacticalSlide(ent,true)}
}
function verticalCollide(ent,oldY,oldX=ent.x,oldZ=ent.z,wasGrounded=false){
  ent.onGround=false;
  const r=ent.r||.45,footRadius=r*.72+COLLISION_SKIN,oldFeet=oldY-PLAYER_H,newFeet=ent.y-PLAYER_H,fall=oldFeet-newFeet;
  const middleX=(oldX+ent.x)/2,middleZ=(oldZ+ent.z)/2,travel=Math.hypot(ent.x-oldX,ent.z-oldZ);
  const nearby=blocksNear(middleX,middleZ,r+travel/2+1),continuousRamp=actorRampHeight(ent),followingRamp=Number.isFinite(continuousRamp)&&Math.abs(newFeet-continuousRamp)<.25;
  if(ent.vy<=0){
    let top=-Infinity;
    for(const b of nearby){
      const surface=b.y+b.h/2;
      // Stair plinths make the underside solid; their treads are an approximation
      // of the continuous ramp, and must not pull descending feet uphill again.
      if(followingRamp&&surface<continuousRamp+.24)continue;
      // Test the feet at the exact time they cross a top plane. Only an already
      // grounded actor may snap a few centimetres down across a platform seam.
      const crossing=oldFeet>=surface-COLLISION_SKIN&&newFeet<=surface+COLLISION_SKIN;
      const downSnap=wasGrounded&&surface<=oldFeet&&newFeet>=surface&&newFeet-surface<=GROUND_SNAP;
      // A continuous ramp can end between substeps. Lift onto its flush deck
      // instead of dropping a few centimetres below the deck's collision face.
      const upStep=wasGrounded&&surface>oldFeet&&surface-oldFeet<=.20&&newFeet<=surface;
      if(!crossing&&!downSnap&&!upStep)continue;
      const t=upStep?1:fall>1e-8?clamp((oldFeet-surface)/fall,0,1):1;
      if(!actorOverSurface(oldX+(ent.x-oldX)*t,oldZ+(ent.z-oldZ)*t,b,footRadius))continue;
      if(surface>top)top=surface;
    }
    if(top>-Infinity){ent.y=top+PLAYER_H;ent.vy=0;ent.onGround=true}
  }else{
    let underside=Infinity;const rise=ent.y-oldY,headOffset=actorStandingHeight(ent)-PLAYER_H,oldHead=oldY+headOffset,newHead=ent.y+headOffset;
    for(const b of nearby){
      const surface=b.y-b.h/2;if(oldY-PLAYER_H>=surface-COLLISION_SKIN||newHead<surface-COLLISION_SKIN)continue;
      const t=rise>1e-8?clamp((surface-oldHead)/rise,0,1):1;
      if(actorOverSurface(oldX+(ent.x-oldX)*t,oldZ+(ent.z-oldZ)*t,b,r)&&surface<underside)underside=surface;
    }
    if(underside<Infinity){ent.y=underside-headOffset-COLLISION_SKIN;ent.vy=0}
  }
}
function hasGroundSupport(ent){
  const feet=ent.y-PLAYER_H,r=(ent.r||.45)*.72+COLLISION_SKIN;
  if(typeof survivalMapRuntime!=='undefined'&&game.mode==='survivor'&&survivalMapRuntime?.rampSupport(ent,PLAYER_H))return true;
  if(game.mode!=='survivor'&&typeof arenaRampSupport==='function'&&arenaRampSupport(ent))return true;
  return blocksNear(ent.x,ent.z,r+1).some(b=>Math.abs(feet-(b.y+b.h/2))<=COLLISION_SKIN&&actorOverSurface(ent.x,ent.z,b,r));
}
function resetHumanJump(ent){ent.jumpBuffer=0;ent.coyoteTime=ent.onGround?JUMP_COYOTE:0;ent.landingImpact=0;ent.damageCue=null;ent.combatHit=null;ent.hitReactUntil=0;ent.hitReactYaw=0;ent.recoilPitch=ent.recoilYaw=ent.recoilRoll=0;ent.weaponSwitchTimer=0;resetTacticalSlide(ent)}
function requestHumanJump(ent){
  if(!ent||ent.dead||!game.started||game.over||game.upgradesOpen||game.shopOpen)return false;
  ent.jumpBuffer=JUMP_BUFFER;return true;
}
function consumeHumanJump(ent){
  if(!isHumanActor(ent)||ent.dead||game.over||game.upgradesOpen||game.shopOpen||!(ent.jumpBuffer>0)||(!ent.onGround&&!(ent.coyoteTime>0)))return false;
  ent.jumpBuffer=0;ent.coyoteTime=0;ent.onGround=false;
  if(ent.slideTimer>0)endTacticalSlide(ent);
  if(!tryVoltDash(ent)){ent.vy=JUMP_SPEED;burst(ent.x,ent.y-1,ent.z,'#fff',5,.25)}
  return true;
}
function physics(ent,dt){
  if(!Number.isFinite(ent.x+ent.y+ent.z+ent.vx+ent.vy+ent.vz)||Math.abs(ent.x)>MAP_LIMIT+8||Math.abs(ent.z)>MAP_LIMIT+8||ent.y<-8){recoverActor(ent);return}
  if(!Number.isFinite(dt)||dt<=0)return;
  const human=isHumanActor(ent),r=ent.r||.45;
  // Time and horizontal travel bounds avoid tunnelling at low FPS and during a
  // dash/grapple. Vertical top/head intersections remain swept inside each step.
  const steps=Math.min(64,Math.max(1,Math.ceil(dt*90),Math.ceil(Math.hypot(ent.vx,ent.vz)*dt/Math.max(.18,r*.55)))),step=dt/steps;
  ent.landingImpact=Math.max(0,(ent.landingImpact||0)-dt*4);
  for(let i=0;i<steps;i++){
    if(human){
      if(ent.vy>0){ent.onGround=false;ent.coyoteTime=0}
      else if(ent.onGround)ent.coyoteTime=JUMP_COYOTE;
      consumeHumanJump(ent);
      if(!ent.onGround)ent.coyoteTime=Math.max(0,(ent.coyoteTime||0)-step);
    }
    const oldY=ent.y,oldX=ent.x,oldZ=ent.z,wasGrounded=ent.onGround;
    ent.vy-=GRAV*step*(typeof survivalMapRuntime!=='undefined'&&game.mode==='survivor'?(survivalMapRuntime?.groundModifier(ent,game.survivalElapsed||0).gravity||1):1);const impactSpeed=-ent.vy;
    const terrainSpeed=typeof survivalMapRuntime!=='undefined'&&game.mode==='survivor'?survivalMapRuntime?.groundModifier(ent,game.survivalElapsed||0).speed||1:1;ent.x+=ent.vx*step*terrainSpeed;ent.z+=ent.vz*step*terrainSpeed;ent.y+=ent.vy*step;
    // Descending feet must catch an accessible top before side collision can
    // shove the actor away. Rising heads use their own underside intersection.
    if(typeof survivalMapRuntime!=='undefined'&&game.mode==='survivor')survivalMapRuntime?.followRamp(ent,{x:oldX,z:oldZ,y:oldY,grounded:wasGrounded},PLAYER_H);
    if(typeof followArenaRamp==='function')followArenaRamp(ent,{x:oldX,z:oldZ,y:oldY,grounded:wasGrounded});
    verticalCollide(ent,oldY,oldX,oldZ,wasGrounded);pushOutXZ(ent);if(typeof survivalMapRuntime!=='undefined'&&game.mode==='survivor'&&survivalMapRuntime?.rampSupport(ent,PLAYER_H)&&ent.vy<=0){ent.onGround=true;ent.vy=0;}
    if(game.mode!=='survivor'&&typeof arenaRampSupport==='function'&&arenaRampSupport(ent)&&ent.vy<=0){ent.onGround=true;ent.vy=0}
    if(ent.onGround&&!hasGroundSupport(ent))ent.onGround=false;
    if(human){
      if(ent.onGround){
        ent.coyoteTime=JUMP_COYOTE;
        if(!wasGrounded&&impactSpeed>7){ent.landingImpact=Math.min(.65,(impactSpeed-7)/24);burst(ent.x,ent.y-PLAYER_H+.04,ent.z,'#65f7ff',4,.20)}
        consumeHumanJump(ent);
      }
      ent.jumpBuffer=Math.max(0,(ent.jumpBuffer||0)-step);
    }
  }
  if(ent.y<-8)recoverActor(ent);
}
function recoverActor(ent){const wasDead=ent.dead;spawn(ent);ent.dead=wasDead;if(ent===player&&!wasDead)msg('RETURNED TO ARENA')}
function safeSpawnCandidate(ent,x,z,feet=0){
  const radius=(ent.r||.5)+.12,height=typeof actorStandingHeight==='function'?actorStandingHeight(ent):2.39;
  if(Math.abs(x)>ARENA_SAFE-radius||Math.abs(z)>ARENA_SAFE-radius)return null;
  let support=-Infinity;
  for(const block of blocksNear(x,z,radius))if(block.kind==='platform'&&Math.abs(x-block.x)<=block.w/2&&Math.abs(z-block.z)<=block.d/2){const top=block.y+block.h/2;if(top<=feet+.28)support=Math.max(support,top);}
  const ramp=game.mode==='survivor'?survivalMapRuntime?.rampHeight(x,z):arenaRampHeight(x,z);if(Number.isFinite(ramp)&&ramp<=feet+.28)support=Math.max(support,ramp);
  if(!Number.isFinite(support))return null;
  if(blocksNear(x,z,radius).some(b=>b.structure?.active!==false&&Math.abs(x-b.x)<b.w/2+radius&&Math.abs(z-b.z)<b.d/2+radius&&support<b.y+b.h/2-.06&&support+height>b.y-b.h/2+.03))return null;
  return {x,z,y:support+PLAYER_H};
}
function chooseSafeSpawn(ent){
  const points=edgeSpawns.length?edgeSpawns:nav,offset=spawnIndex++%Math.max(1,points.length),actors=[player,player2,...bots].filter(e=>e&&e!==ent&&!e.dead),enemies=actors.filter(e=>game.mode==='survivor'?!!e.alien:game.mode!=='tdm'||e.team!==ent.team);
  let best=null;
  for(let i=0;i<points.length;i++){
    const p=points[(i+offset)%points.length];
    for(let attempt=0;attempt<3;attempt++){
      const jitter=attempt?Math.min(1.2,(p.r||1)*.35):0,c=safeSpawnCandidate(ent,p.x+rand(-jitter,jitter),p.z+rand(-jitter,jitter),Math.max(0,(p.y||PLAYER_H)-PLAYER_H));if(!c)continue;
      let distance=80,visible=0,occupancy=0;
      for(const enemy of enemies){const d=Math.hypot(enemy.x-c.x,enemy.z-c.z);distance=Math.min(distance,d);if(d<70&&lineClear(c.x,c.y-.08,c.z,enemy.x,enemy.y-.45,enemy.z))visible+=1+(d<20?2:0);}
      for(const other of actors)if(Math.hypot(other.x-c.x,other.z-c.z)<3.5)occupancy++;
      const score=Math.min(65,distance)-visible*23-occupancy*35-i*.015;if(!best||score>best.score)best={...c,score};
    }
  }
  if(!best&&points!==nav)for(const p of nav){const c=safeSpawnCandidate(ent,p.x,p.z,Math.max(0,p.y-PLAYER_H));if(c){best=c;break;}}
  if(!best)throw Error('Arena has no clear standing spawn for '+(ent.name||'actor'));
  return best;
}
function spawn(ent){
  const sp=chooseWarSpawn(ent)||chooseSafeSpawn(ent),x=sp.x,z=sp.z;
  ent.crouching=false;ent.crouchPose=0;ent.aiming=false;ent.aimPose=0;ent.warRecoilPitch=ent.warRecoilYaw=ent.warShotIndex=ent.warBurstIdle=0;ent.x=x;ent.z=z;ent.y=sp.y;ent.vx=ent.vy=ent.vz=0;ent.onGround=true;resetHumanJump(ent);tacticalController?.reset(ent);ent.dead=false;ent.hp=ent.maxHp||100;ent.armor=Math.min(ent.maxArmor||100,ent===player?50:rand(25,60));ent.slowTimer=0;ent.slowAmount=0;ent.burnTimer=0;ent.burnDps=0;ent.burnOwner=null;ent.assaultMomentum=0;ent.momentumLock=0;ent.momentumGrace=0;ent.voltDashCharge=0;ent.voltDashMeter=0;ent.voltDashCooldown=0;ent.voltKillWindow=0;ent.slipstreamTimer=0;ent.slipstreamKillWindow=0;ent.tacticalTimer=0;ent.harpoonMomentumTimer=0;ent.harpoonMomentumShot=0;ent.mortarActive=null;ent.harpoonGrapple=null;ent.harpoonVisualAnchor=null;ent.harpoonPull=null;ent.harpoonTether=null;ent.harpoonBleedTimer=0;ent.spawnGrace=1.15;ent.target=pick(nav);ent.walk=0;ent.faceYaw=ent.yaw||0;pushOutXZ(ent);if(game.mode==='frag')enforceFragLoadout(ent);else resetWeaponInventory(ent,warMode()?(ent.warLoadout||window.NeonWarUI.selection().weapon):'pistol')
}
function ensureWeaponInventory(ent){
  if(!Array.isArray(ent.weaponSlots))ent.weaponSlots=[ent.weapon||'pistol',null];
  if(!Array.isArray(ent.weaponSlotState))ent.weaponSlotState=[null,null];
  if(ent.activeSlot!==0&&ent.activeSlot!==1)ent.activeSlot=0;
  for(let i=0;i<2;i++)if(ent.weaponSlots[i]&&!ent.weaponSlotState[i]){const w=WEAPONS[ent.weaponSlots[i]]||WEAPONS.pistol;ent.weaponSlotState[i]=initWeaponState(ent.weaponSlots[i]);}
}
function saveActiveWeaponState(ent){ensureWeaponInventory(ent);const i=ent.activeSlot||0;if(!ent.weaponSlots[i])return;ent.weaponSlotState[i]={ammo:ent.ammo,reload:ent.reload||0,reloadTotal:ent.reloadTotal||0,reloadAnimType:ent.reloadAnimType||null,heat:ent.heat||0,menderOverheated:!!ent.menderOverheated,charge:ent.charge||0,ult:ent.ult||0,ultReady:!!ent.ultReady,ultClipShots:ent.ultClipShots||0,ultClipWeapon:ent.ultClipWeapon||null,ultimateShot:!!ent.ultimateShot,ultimateWeapon:ent.ultimateWeapon||null};}
function loadActiveWeaponState(ent){ensureWeaponInventory(ent);let i=ent.activeSlot||0;if(!ent.weaponSlots[i]){i=ent.weaponSlots[0]?0:(ent.weaponSlots[1]?1:0);ent.activeSlot=i;}const type=ent.weaponSlots[i]||'pistol',w=WEAPONS[type]||WEAPONS.pistol,st=ent.weaponSlotState[i]||{};ent.weapon=type;ent.ammo=Number.isFinite(st.ammo)?st.ammo:w.mag;ent.reload=st.reload||0;ent.reloadTotal=st.reloadTotal||0;ent.reloadAnimType=st.reloadAnimType||null;ent.heat=st.heat||0;ent.menderOverheated=type==='mender'&&!!st.menderOverheated;ent.charge=st.charge||0;ent.ult=st.ult||0;ent.ultReady=!!st.ultReady;ent.ultClipShots=st.ultClipWeapon===type?(st.ultClipShots||0):0;ent.ultClipWeapon=ent.ultClipShots>0?type:null;ent.ultimateShot=st.ultimateWeapon===type&&!!st.ultimateShot;ent.ultimateWeapon=ent.ultimateShot?type:null;ent.cool=Math.min(ent.cool||.12,.12);if(ent===player){styleGun(type);renderWeaponSlots(ent)}}
function resetWeaponInventory(ent,type='pistol'){const w=WEAPONS[type]||WEAPONS.pistol;ent.weaponSlots=[type,null];ent.weaponSlotState=[initWeaponState(type),null];ent.activeSlot=0;ent.weapon=type;ent.ammo=w.mag;ent.reload=0;ent.reloadTotal=0;ent.reloadAnimType=null;ent.cool=.12;ent.heat=0;ent.menderOverheated=false;ent.charge=0;ent.ult=0;ent.ultReady=false;ent.ultClipShots=0;ent.ultClipWeapon=null;ent.ultimateShot=false;ent.ultimateWeapon=null;if(ent===player){styleGun(type);renderWeaponSlots(ent)}}
function equip(ent,type,announce=true,opts={}){ensureWeaponInventory(ent);saveActiveWeaponState(ent);const w=WEAPONS[type]||WEAPONS.pistol;let slot=ent.activeSlot||0;if(opts.forceSlot===0||opts.forceSlot===1)slot=opts.forceSlot;else if(opts.addToEmpty&&ent.weaponSlots[0]&& !ent.weaponSlots[1]&&ent.weaponSlots[0]!==type)slot=1;ent.activeSlot=slot;ent.weaponSlots[slot]=type;ent.weaponSlotState[slot]=initWeaponState(type);loadActiveWeaponState(ent);ent.weaponSwitchTimer=.18;ent.cool=.12;if(ent===player&&announce)msg('PICKED UP '+w.name)}
function switchWeapon(ent){if(ent?.mortarActive)releaseMortar(ent);ensureWeaponInventory(ent);const other=(ent.activeSlot||0)===0?1:0;if(!ent.weaponSlots[other]){if(ent===player)msg('NO SECOND WEAPON');return false;}saveActiveWeaponState(ent);ent.activeSlot=other;loadActiveWeaponState(ent);ent.weaponSwitchTimer=.18;ent.cool=Math.max(ent.cool||0,.10);if(ent===player){msg('SWAPPED TO '+(WEAPONS[ent.weapon]?.name||ent.weapon).toUpperCase());feed('Swapped to '+(WEAPONS[ent.weapon]?.name||ent.weapon));}return true;}
function nearestWeaponPickup(ent){let best=null,bd=Infinity;if(game.mode==='frag')return null;for(const p of pickups){if(p.gone||p.taken>0||p.kind!=='weapon')continue;const d=hypot2(ent.x-p.x,ent.z-p.z),dy=Math.abs(ent.y-(p.y+1.1));if(d<2.7&&dy<3.8&&d<bd&&lineClear(actorEyePosition(ent).x,actorEyePosition(ent).y-.05,actorEyePosition(ent).z,p.x,p.y+.28,p.z)){best=p;bd=d}}return best;}
function takeWeaponPickup(ent,p){if(!p||p.gone||p.taken>0)return false;equip(ent,p.type,ent===player,{addToEmpty:true});if(p.oneShot)p.gone=true;else p.taken=p.respawn;if(ent===player)feed('Picked up '+WEAPONS[p.type].name+' into slot '+((ent.activeSlot||0)+1));else ent.target=pick(nav);return true;}
function pickupButtonLabelFor(ent=player){
  if(ent===player&&game.p1Input==='touch')return 'SWAP';
  if(ent===player&&game.p1Input==='controller')return padButtonLabel(controller.settings.pickup??1);
  if(ent===player2&&game.p2Input==='controller')return padButtonLabel(controller.settings.pickup??1);
  return ent===player2?'NUM5':'F';
}
function padButtonLabel(i){return ({0:'A / Cross',1:'B / Circle',2:'X / Square',3:'Y / Triangle',4:'LB / L1',5:'RB / R1',6:'LT / L2',7:'RT / R2',8:'View / Select',9:'Menu / Start'})[i]||('Button '+i)}
function updatePickupPrompt(){
  if(!hud.pickupPrompt)return;
  if(!game.started||game.over||game.upgradesOpen||player.dead||game.mode==='frag'){hud.pickupPrompt.style.display='none';return;}
  const p=nearestWeaponPickup(player);
  if(!p){hud.pickupPrompt.style.display='none';return;}
  const name=WEAPONS[p.type]?.name||String(p.type).toUpperCase();
  hud.pickupPrompt.innerHTML=game.p1Input==='touch'?'⇄ '+compactWeaponName(p.type):'<b>'+pickupButtonLabelFor(player)+'</b> · '+compactWeaponName(p.type);
  const area=splitRects()[0];hud.pickupPrompt.style.left=(area.x+area.w/2)+'px';hud.pickupPrompt.style.top=(area.y+area.h/2+48)+'px';hud.pickupPrompt.style.display='block';
}
function interactWeapon(ent){if(game.mode==='campaign'&&ent===player&&(campaignInteractionAvailable()||campaignOptionalAvailable())){warInteract=true;return;}if(!ent||ent.dead||game.mode==='frag')return;const p=nearestWeaponPickup(ent);if(p){takeWeaponPickup(ent,p);return;}switchWeapon(ent)}
function compactWeaponName(type){
  if(!type)return 'EMPTY';
  const name=(WEAPONS[type]?.name||String(type)).toUpperCase();
  return name.replace('ASSAULT RIFLE','RIFLE').replace('CARBINE RIFLE','CARBINE').replace('ROCKET LAUNCHER','ROCKET').replace('PLASMA BLASTER','PLASMA').replace('ARC BLASTER','ARC').replace('PLASMA LASER','LASER').replace('MENDER BEAM','MENDER');
}

const ULTIMATE_NAMES={pistol:'DEADEYE ROUND',assault:'THUNDER MAG',sniper:'RAIL JUDGMENT',smg:'OVERCLOCK SPRAY',shotgun:'NOVA BUCKSHOT',burst:'LOCK-ON VOLLEY',arc:'CHAIN STORM',rocket:'WARHEAD',mortar:'METEOR SALVO',punch:'CHAIN HAYMAKER',harpoon:'LEVIATHAN LINE',plasma:'SUPERNOVA SHOT',laser:'ION SWEEP'};
const ULTIMATE_GAIN={pistol:.145,assault:.052,sniper:.34,smg:.038,shotgun:.034,burst:.05,arc:.062,rocket:.28,mortar:.24,plasma:.075,laser:.035};
const CLIP_ULTIMATE_WEAPONS=new Set(['assault','smg','burst','arc','laser']);
const CLIP_ULTIMATE_WEAK_MULT={assault:1.16,smg:1.13,burst:1.18,arc:1.20,laser:1.34};
function isClipUltimateWeapon(weapon){return CLIP_ULTIMATE_WEAPONS.has(weapon)}
function activeWeaponState(ent){ensureWeaponInventory(ent);return ent.weaponSlotState?.[ent.activeSlot||0]||null}
function initWeaponState(type){const w=WEAPONS[type]||WEAPONS.pistol;return {ammo:w.mag,reload:0,reloadTotal:0,reloadAnimType:null,heat:0,menderOverheated:false,charge:0,ult:0,ultReady:false,ultClipShots:0,ultClipWeapon:null,ultimateShot:false,ultimateWeapon:null}}
function getUltimateCharge(ent){const st=activeWeaponState(ent);return st?Math.max(ent.ult||0,st.ult||0):(ent.ult||0)}
function getUltimateReady(ent){const st=activeWeaponState(ent);return !!(ent.ultReady||st?.ultReady||getUltimateCharge(ent)>=1)}
function announceUltimate(ent,weapon){if(ent===player){msg('ULTIMATE READY: RELOAD '+(WEAPONS[weapon]?.name||weapon));feed((ULTIMATE_NAMES[weapon]||'Ultimate')+' charged — reload this weapon to load it into the next clip.')}else if(ent===player2){feed('PLAYER 2 ultimate ready: reload '+(WEAPONS[weapon]?.name||weapon));}}
function addUltimateCharge(ent,weapon,amount){if(!isHumanActor(ent)||!weapon||game.over)return;ensureWeaponInventory(ent);const st=activeWeaponState(ent);if(!st||st.ultReady)return;const mult=ULTIMATE_GAIN[weapon]||.06;const gain=clamp((amount||1)*mult/30,.01,.22);const before=st.ult||0;st.ult=clamp(before+gain,0,1);ent.ult=st.ult;if(before<1&&st.ult>=1){st.ultReady=true;ent.ultReady=true;announceUltimate(ent,weapon)}}
function addUltimateClipLoadVisual(ent,weapon){const d=dirFrom(ent,0),m=muzzlePoint(ent,d),c=ultimateColor(weapon);addUltimateFx({type:'clipLoad',weapon,x:m.x-d.x*.34,y:m.y-.12,z:m.z-d.z*.34,dx:d.x,dy:d.y,dz:d.z,life:.34,maxLife:.34,color:c,size:weapon==='rocket'?3.8:weapon==='sniper'?3.4:3.0});}
function consumeUltimate(ent){ensureWeaponInventory(ent);const st=activeWeaponState(ent),weapon=ent.weapon,w=WEAPONS[weapon]||WEAPONS.pistol;if(!st||!getUltimateReady(ent))return false;st.ult=0;st.ultReady=false;ent.ult=0;ent.ultReady=false;if(isClipUltimateWeapon(weapon)){const shots=weapon==='laser'?75:Math.max(1,w.mag||ent.ammo||1);st.ultClipShots=shots;st.ultClipWeapon=weapon;ent.ultClipShots=shots;ent.ultClipWeapon=weapon;ent.ultimateShot=false;ent.ultimateWeapon=null;}else{st.ultimateShot=true;st.ultimateWeapon=weapon;ent.ultimateShot=true;ent.ultimateWeapon=weapon;ent.ultClipShots=0;ent.ultClipWeapon=null;}addUltimateClipLoadVisual(ent,weapon);if(ent===player||ent===player2){const who=ent===player?'YOU':'PLAYER 2';msg((ULTIMATE_NAMES[weapon]||'ULTIMATE')+' LOADED!');feed(who+' loaded '+(ULTIMATE_NAMES[weapon]||'Ultimate')+' into the next '+(isClipUltimateWeapon(weapon)?'clip':'shot'));}saveActiveWeaponState(ent);return true}
function endUltimateShot(ent){if(ent.ultClipShots>0)return;ent.ultimateShot=false;ent.ultimateWeapon=null;saveActiveWeaponState(ent)}
function ultimateClipActive(ent,weapon){return !!ent.ultClipShots&&ent.ultClipWeapon===weapon}
function ultimateActive(ent,weapon){return (!!ent.ultimateShot&&ent.ultimateWeapon===weapon)||ultimateClipActive(ent,weapon)}
function spendClipUltimateShot(ent){if(!ent||!ent.ultClipShots)return;const st=activeWeaponState(ent);ent.ultClipShots=Math.max(0,(ent.ultClipShots||0)-1);if(st){st.ultClipShots=ent.ultClipShots;st.ultClipWeapon=ent.ultClipShots>0?ent.ultClipWeapon:null}if(ent.ultClipShots<=0){const finished=ent.ultClipWeapon;ent.ultClipWeapon=null;ent.ultimateShot=false;ent.ultimateWeapon=null;if(ent===player||ent===player2)feed((ULTIMATE_NAMES[finished]||'Ultimate')+' clip spent');}saveActiveWeaponState(ent)}
function ultimateColor(weapon){return ({pistol:'#fff44f',assault:'#65f7ff',sniper:'#ffffff',smg:'#ffea00',shotgun:'#ff7a2b',burst:'#7cffd5',arc:'#65f7ff',rocket:'#ff2b80',mortar:'#ff335f',plasma:'#ff2bff',laser:'#ff2bff'})[weapon]||'#fff44f'}
function addUltimateFx(fx){
  fx.life=fx.life??.7;fx.maxLife=fx.maxLife??fx.life;fx.color=fx.color||ultimateColor(fx.weapon);ultimateFx.push(fx);trimVisualArray(ultimateFx,Math.max(visualPreset().maxTier3,24));
}
function addTier3Fx(fx){fx.tier3=true;fx.life=fx.life??.28;fx.maxLife=fx.maxLife??fx.life;fx.color=fx.color||'#65f7ff';ultimateFx.push(fx);trimVisualArray(ultimateFx,visualPreset().maxTier3)}
function addUltimateSparkCloud(x,y,z,weapon,count=6,power=.55){const c=ultimateColor(weapon);count=scaledVisualCount(Math.min(count,8));for(let i=0;i<count;i++){const a=rand(0,TAU),r=rand(.2,2.2*power),up=rand(-.35,.65);particles.push({kind:'spark',x:x+Math.sin(a)*r*.15,y:y+up,z:z+Math.cos(a)*r*.15,vx:Math.sin(a)*rand(1.2,4.2)*power,vy:rand(.8,4.4)*power,vz:Math.cos(a)*rand(1.2,4.2)*power,life:rand(.14,.36),maxLife:.36,c:i%3?'#ffffff':c,size:rand(1.6,3.5)*power})}trimVisualArray(particles,visualPreset().maxParticles)}
function addUltimateMuzzleVisual(shooter,weapon,d){
  const m=muzzlePoint(shooter,d),c=ultimateColor(weapon);addUltimateFx({type:'muzzle',weapon,x:m.x,y:m.y,z:m.z,dx:d.x,dy:d.y,dz:d.z,life:.20,maxLife:.20,color:c,size:2.4});
  addUltimateFx({type:'ring',weapon,x:m.x,y:m.y,z:m.z,life:.24,maxLife:.24,color:c,size:weapon==='shotgun'?3.2:weapon==='rocket'?3.8:2.4});
  if(weapon==='shotgun')addUltimateFx({type:'cone',weapon,x:m.x,y:m.y,z:m.z,dx:d.x,dy:d.y,dz:d.z,life:.18,maxLife:.18,color:c,len:12,spread:.52});
  if(weapon==='smg')addUltimateFx({type:'spray',weapon,x:m.x,y:m.y,z:m.z,dx:d.x,dy:d.y,dz:d.z,life:.20,maxLife:.20,color:c,len:10});
  addUltimateSparkCloud(m.x,m.y,m.z,weapon,weapon==='shotgun'?5:3,.35);
}
function addUltimateImpactVisual(weapon,x,y,z,dir=null,target=null){
  const c=ultimateColor(weapon),big=weapon==='rocket'||weapon==='plasma'||weapon==='shotgun';
  addUltimateFx({type:'ring',weapon,x,y,z,life:big?.36:.26,maxLife:big?.36:.26,color:c,size:weapon==='rocket'?7:weapon==='plasma'?6:weapon==='shotgun'?5:3.5});
  if(weapon==='pistol')addUltimateFx({type:'deadeye',weapon,x,y,z,life:.34,maxLife:.34,color:c,size:4});
  if(weapon==='sniper')addUltimateFx({type:'railMark',weapon,x,y,z,life:.42,maxLife:.42,color:c,size:5.2});
  if(weapon==='burst'||weapon==='pistol')addUltimateFx({type:'reticle',weapon,x,y,z,life:.30,maxLife:.30,color:c,size:4});
  if(weapon==='assault'||weapon==='arc')addUltimateFx({type:'storm',weapon,x,y,z,life:.32,maxLife:.32,color:c,size:weapon==='arc'?5.5:4.2});
  if(weapon==='smg')addUltimateFx({type:'spiral',weapon,x,y,z,life:.28,maxLife:.28,color:c,size:3.7});
  if(weapon==='shotgun')addUltimateFx({type:'shockwave',weapon,x,y,z,life:.34,maxLife:.34,color:c,size:5.2});
  if(weapon==='rocket')addUltimateFx({type:'warhead',weapon,x,y,z,life:.40,maxLife:.40,color:c,size:7});
  if(weapon==='plasma')addUltimateFx({type:'supernova',weapon,x,y,z,life:.42,maxLife:.42,color:c,size:6.5});
  if(weapon==='laser')addUltimateFx({type:'ionBurst',weapon,x,y,z,life:.26,maxLife:.26,color:c,size:4.2});
  addUltimateSparkCloud(x,y,z,weapon,weapon==='rocket'||weapon==='plasma'?8:weapon==='shotgun'?6:4,weapon==='rocket'||weapon==='plasma'?.55:.38);
}
function addUltimateBeamVisual(weapon,x,y,z,dx,dy,dz,len){
  const c=ultimateColor(weapon);addUltimateFx({type:weapon==='sniper'?'railBeam':'beamAura',weapon,x,y,z,dx,dy,dz,len,life:weapon==='sniper'?.20:.16,maxLife:weapon==='sniper'?.20:.16,color:c});
  if(weapon==='burst')addUltimateFx({type:'reticle',weapon,x:x+dx*len*.55,y:y+dy*len*.55,z:z+dz*len*.55,life:.22,maxLife:.22,color:c,size:4});
}

function renderWeaponSlots(ent=player){return ent;}
function styleGun(type){const w=WEAPONS[type]||WEAPONS.assault;if(hud.weaponName)hud.weaponName.textContent=w.name;}

function siegeHumans(){return [player,player2].filter(h=>h&&!h.dead)}
function siegeEnemies(){return bots.filter(b=>b.siegeEnemy)}
function siegeAliveEnemies(){return siegeEnemies().filter(b=>!b.dead)}
function siegeRoundActive(){return game.mode==='siege'&&game.siegePhase==='fight'&&!game.siegeRoundOver&&!game.siegeMatchOver}
function siegeBuildActive(){return game.mode==='siege'&&game.siegePhase==='build'&&!game.siegeRoundOver&&!game.siegeMatchOver}
function makeSiegeEnemy(i){
  const cfg=DIFF[game.difficulty]||DIFF.normal,names=['BREACH','RAM','SAPPER','VANDAL','RAIDER','REAVER','CRUSH','PIKE','WRECKER','BOLT'],styles=['rifler','rusher','heavy','skirmisher','marksman'];
  const style=styles[i%styles.length],weapon=pick(['assault','smg','shotgun','burst','arc','rocket','mortar','harpoon']);
  return applyBotPersonality({name:names[i%names.length]+' '+(i+1),style,siegeLoadout:weapon,x:0,y:PLAYER_H,z:0,vx:0,vy:0,vz:0,r:.54,yaw:0,pitch:0,onGround:false,team:2,score:0,deaths:0,dead:false,hp:115,maxHp:115,armor:rand(35,70),maxArmor:100,weapon,ammo:(WEAPONS[weapon]||WEAPONS.assault).mag,reload:0,cool:0,heat:0,spawnGrace:0,think:0,fireCd:rand(.35,1.1),target:null,enemy:null,strafe:Math.random()<.5?-1:1,strafeT:0,color:i%2?'#ff3f5f':'#ffb02e',walk:0,faceYaw:0,slowTimer:0,slowAmount:0,stagger:0,aggression:style==='rusher'?1.28:style==='marksman'?.82:style==='heavy'?.98:1,accuracyBonus:(style==='marksman'?.12:style==='rifler'?.06:0)+(cfg.aim||.4)*.05,pickupBias:0,aimLock:0,lockTarget:null,siegeEnemy:true,siegeLives:2,siegeTotalLives:2});
}
function setActorAt(ent,x,z,yaw=0,eyeY=null){ent.x=x;ent.z=z;ent.y=Number.isFinite(eyeY)?eyeY:topAtPoint(x,z)+PLAYER_H;if(ent.y<-20)ent.y=PLAYER_H;ent.vx=ent.vy=ent.vz=0;ent.onGround=true;resetHumanJump(ent);ent.dead=false;ent.hp=ent.maxHp||100;ent.armor=Math.min(ent.maxArmor||100,ent===player?70:60);ent.spawnGrace=1.0;ent.yaw=yaw;ent.pitch=0;ent.faceYaw=yaw;ent.target=pick(nav);pushOutXZ(ent)}

function siegeOpeningLayout(){
  // Keep the fortification phase central while respecting each arena's landmark.
  const layouts={
    grid:{defenders:[[-3,0],[3,0]],supplies:[[-2.8,0],[2.8,0],[0,2.8]]},
    pastel:{defenders:[[-4,5],[4,5]],supplies:[[-6,5],[0,7],[6,5]]},
    reactor:{defenders:[[-4,10],[4,10]],supplies:[[-5.5,10],[0,12],[5.5,10]]},
    terminal:{defenders:[[-3,0],[3,0]],supplies:[[-2.8,0],[2.8,0],[0,2.8]]},
    foundry:{defenders:[[-3,0],[1,4]],supplies:[[-4,3],[-1,3],[3,5]]},
    outpost:{defenders:[[-3,1],[3,1]],supplies:[[-4,3],[0,4],[4,3]]}
  },layout=layouts[game.map]||layouts.grid;
  const clear=([x,z])=>{const p=safeSpawnCandidate({r:.5,human:true},x*WORLD_SCALE,z*WORLD_SCALE,0);if(!p)throw Error('Siege opening site is obstructed in '+game.map);return p;};
  return {defenders:layout.defenders.map(clear),supplies:layout.supplies.map(clear)};
}

function spawnSiegeOpeningWeapons(){
  if(game.mode!=='siege')return;
  const pool=['assault','smg','shotgun','sniper','burst','arc','plasma','laser','mender','rocket','mortar','harpoon'];
  const choices=pool.slice().sort(()=>Math.random()-.5).slice(0,3);
  const spots=siegeOpeningLayout().supplies;
  for(let i=0;i<choices.length;i++){
    addPickup(choices[i],spots[i].x/WORLD_SCALE,spots[i].z/WORLD_SCALE);
    const p=pickups[pickups.length-1];
    if(p){p.y=spots[i].y-PLAYER_H+.7;p.oneShot=true;p.respawn=0;p.siegeOpeningWeapon=true;}
  }
  feed('SIEGE SUPPLY DROP: 3 weapons are waiting in the middle before the breach starts.');
}
function spawnSiegeEnemies(){
  if(game.mode!=='siege'||game.siegeEnemiesSpawned)return;
  game.siegeEnemiesSpawned=true;bots=bots.filter(b=>b.human);
  const count=Math.max(2,Math.min(16,Number(game.botCount)||2));
  for(let i=0;i<count;i++){const b=makeSiegeEnemy(i);spawn(b);b.team=2;b.siegeLives=2;b.siegeTotalLives=2;if(i%3===0)b.siegeLoadout='shotgun';else if(i%3===1)b.siegeLoadout='assault';else b.siegeLoadout='smg';equip(b,b.siegeLoadout,false);bots.push(b)}
  feed('SIEGE: enemies are breaching now. Each enemy has 2 lives. Hold the castle and wipe them out.');
  msg('SIEGE BEGINS — DEFEND!');
}
function setupSiegeRound(keepScore=true){
  buildMap();pickups=[];shots=[];projectiles=[];particles=[];lasers=[];impacts=[];muzzleFlashes=[];decals=[];ultimateFx=[];plasmaResidues=[];buildPieces=[];buildOccupancy.clear();registerStaticGridOccupancy();
  buildCharges=BUILD_MAX;buildRecharge=0;buildFeedback=0;selectedBuildType='wall';buildHeld=false;buildPreview=null;
  if(!keepScore){game.siegeRound=1;game.siegePlayers=0;game.siegeEnemies=0;game.siegeMatchOver=false;player.score=0;if(player2)player2.score=0;}
  game.siegePhase='build';game.siegeBuildTime=60;game.siegeRoundOver=false;game.siegeEnemiesSpawned=false;game.timeLeft=9999;
  spawnSiegeOpeningWeapons();
  const opening=siegeOpeningLayout();
  player.team=1;player.dead=false;player.deaths=player.deaths||0;player.upgradePoints=0;player.killStreak=0;player.upgrades=createUpgradeState();resetWeaponInventory(player,'pistol');setActorAt(player,opening.defenders[0].x,opening.defenders[0].z,Math.PI/2,opening.defenders[0].y);
  bots=[];
  if(game.secondPlayer){
    if(!player2)player2=makeHumanPlayer('PLAYER 2','#ff9d2e');
    player2.team=1;player2.dead=false;player2.deaths=player2.deaths||0;player2.upgradePoints=0;player2.killStreak=0;player2.upgrades=createUpgradeState();resetWeaponInventory(player2,'pistol');setActorAt(player2,opening.defenders[1].x,opening.defenders[1].z,-Math.PI/2,opening.defenders[1].y);bots.push(player2);
  }else player2=null;
  game.started=true;game.over=false;styleGun(player.weapon);
  feed('SIEGE ROUND '+game.siegeRound+': 60 seconds of FREE building. Build a castle in the center — charges will not be spent until the breach starts. Grab the 3 middle weapons before enemies arrive.');
  msg('SIEGE ROUND '+game.siegeRound+' — BUILD YOUR CASTLE');
}
function finishSiegeRound(playersWon,reason){
  if(game.siegeRoundOver||game.siegeMatchOver)return;
  game.siegeRoundOver=true;game.siegePhase='roundover';
  if(playersWon)game.siegePlayers=(game.siegePlayers||0)+1;else game.siegeEnemies=(game.siegeEnemies||0)+1;
  const matchDone=game.siegePlayers>=3||game.siegeEnemies>=3||game.siegeRound>=5;
  game.siegeMatchOver=matchDone;game.over=true;fireHeld=false;controllerFireHeld=false;buildHeld=false;buildPreview=null;toggleUpgradeTree(false);document.exitPointerLock?.();
  const title=matchDone?((game.siegePlayers>game.siegeEnemies)?'SIEGE WON':'SIEGE LOST'):(playersWon?'ROUND WON':'ROUND LOST');
  el('scoreboardTitle').textContent=title;
  el('scoreboardReason').textContent=reason+' · Score: Defenders '+game.siegePlayers+' - '+game.siegeEnemies+' Raiders · Round '+game.siegeRound+' of 5';
  let html='<tr><th>Side</th><th>Round Wins</th><th>KOs</th><th>Lives Rule</th></tr>';
  const humanKos=(player.score||0)+(player2?.score||0), enemyKos=siegeEnemies().reduce((a,b)=>a+(b.score||0),0);
  html+='<tr class="you"><td>DEFENDERS</td><td>'+game.siegePlayers+'</td><td>'+humanKos+'</td><td>1 life each</td></tr>';
  html+='<tr><td>RAIDERS</td><td>'+game.siegeEnemies+'</td><td>'+enemyKos+'</td><td>2 lives each</td></tr>';
  el('scoreboardTable').innerHTML=html;
  el('nextRound').textContent=matchDone?'NEW SIEGE MATCH':'NEXT SIEGE ROUND';
  el('scoreboardOverlay').style.display='flex';
  feed(title+' — '+reason);
}
function updateSiege(dt){
  if(game.mode!=='siege'||game.siegeRoundOver||game.siegeMatchOver)return;
  if(siegeBuildActive()){
    game.siegeBuildTime=Math.max(0,(game.siegeBuildTime||0)-dt);buildCharges=BUILD_MAX;buildRecharge=0;if(player2){player2.buildCharges=BUILD_MAX;player2.buildRecharge=0;}
    if(game.siegeBuildTime<=0){game.siegePhase='fight';spawnSiegeEnemies();}
    return;
  }
  if(game.siegePhase==='fight'){
    const aliveHumans=siegeHumans().length,aliveEnemies=siegeAliveEnemies().length,totalEnemyLives=siegeEnemies().reduce((sum,b)=>sum+Math.max(0,b.siegeLives||0)+(b.dead?0:1),0);
    if(!aliveHumans)finishSiegeRound(false,'All defenders were eliminated');
    else if(game.siegeEnemiesSpawned&&aliveEnemies===0&&totalEnemyLives<=0)finishSiegeRound(true,'All raiders were wiped out');
  }
}
function nextSiegeRoundOrMatch(){
  el('scoreboardOverlay').style.display='none';el('nextRound').textContent='NEXT ROUND';
  if(game.siegeMatchOver){game.siegePlayers=0;game.siegeEnemies=0;game.siegeRound=1;setupSiegeRound(false);}else{game.siegeRound=(game.siegeRound||1)+1;setupSiegeRound(true);}
  updateMobileControlVisibility(true);if(game.p1Input==='keyboard')try{canvas.requestPointerLock?.()?.catch?.(()=>{})}catch{};
}
function initBots(){tacticalController?.clear();bots=[];if(game.secondPlayer&&player2){spawn(player2);equip(player2,game.mode==='frag'?'sniper':'pistol',false);bots.push(player2)}if(game.mode==='survivor'){game.survivalElapsed=0;game.survivalCleared=0;game.encounterHistory=[];game.survivorWave=0;game.waveDelay=30;survivalEquipment?.reset();survivalController?.invalidate();survivalProgress?.beginRun(game.survivalMap||'breach_ring',{teamSize:player2?2:1});game.waveSpawning=false;game.waveKills=0;game.waveKilled=0;game.waveTotal=0;game.waveSpawned=0;game.nextSquadTimer=0;game.survivorOrder=null;game.survivorDropTimer=4;game.scrap=0;game.shopOpen=false;game.shopPending=false;game.survivorBuildBonus=0;game.survivorRechargeMult=1;game.wallThorns=false;game.thornsQueued=false;game.acidZones=[];game.bossWarnings=[];el('survivorShop').style.display='none';feed('SURVIVOR BREACH: 30 seconds to build before Wave 1. Climbers join from wave 4, bosses attack every 5th wave, and cleared waves open the salvage shop. Aliens enter from the outer edge and tear through the perimeter before reaching your fort.');msg('30 SECONDS TO BUILD');return}const names=['VOLT','NOVA','RAZOR','GLITCH','ONYX','PULSE','HEX','ECHO','RIFT','BLITZ'],styles=['rifler','rusher','marksman','skirmisher','heavy'];for(let i=0;i<game.botCount;i++){const style=styles[i%styles.length],b={entityId:'bot-'+i,name:names[i%names.length],style,x:0,y:PLAYER_H,z:0,vx:0,vy:0,vz:0,r:.52,yaw:0,pitch:0,onGround:false,team:game.mode==='tdm'?(i%2)+1:10+i,score:0,deaths:0,dead:false,hp:100,maxHp:100,armor:rand(25,60),maxArmor:100,weapon:'assault',ammo:30,reload:0,cool:0,heat:0,spawnGrace:0,think:0,fireCd:rand(.2,1),target:null,enemy:null,strafe:Math.random()<.5?-1:1,strafeT:0,color:i%2?'#ff2bbd':'#00eaff',walk:0,faceYaw:0,slowTimer:0,slowAmount:0,stagger:0,aggression:style==='rusher'?1.22:style==='marksman'?.78:style==='heavy'?.92:1,accuracyBonus:style==='marksman'?.12:style==='rifler'?.06:0,pickupBias:style==='rusher'?1.18:style==='heavy'?1.10:1,aimLock:0,lockTarget:null};applyBotPersonality(b);spawn(b);bots.push(b)}}
function reset(){
  conquest=null;campaignRun=null;warMap=null;warEscort=null;warInteract=false;warClock=0;warActorId=0;warSightingCache.clear();
  survivalInterface?.clearResults();game.map=el('map')?.value||'grid';game.mode=NeonMatchSettings.MODES.includes(el('mode').value)?el('mode').value:'dm';game.survivalMap=survivalInterface?.selectedMap||'breach_ring';game.difficulty=el('difficulty').value;applyVisualMode(el('visualQuality')?.value||'auto');minimapTimer=0;minimapNeedsRedraw=true;
  game.p1Input=el('p1Input')?.value||controller.settings.p1Input||'keyboard';
  game.secondInput=el('secondPlayer')?.value||el('p2Input')?.value||'off';game.p2Input=game.secondInput;game.secondPlayer=game.p2Input!=='off';
  const options=saveMatchPreferencesFromFields();game.difficulty=options.difficulty;game.botCount=game.mode==='frag'&&game.secondPlayer?0:options.botCount;game.scoreTarget=options.scoreTarget;game.respawnDelay=options.respawnDelay;menuActiveMode=game.mode;
  game.time=(game.mode==='conquest'||game.mode==='campaign'||game.mode==='survivor'||game.mode==='frag'||game.mode==='siege')?9999:options.matchLen;
  game.over=false;game.weaponSafe=game.mode==='campaign';game.upgradesOpen=false;game.shopOpen=false;el('survivorShop').style.display='none';el('upgradeOverlay').style.display='none';hud.feed.innerHTML='';el('nextRound').textContent='NEXT ROUND';
  player.deaths=0;player.score=0;player.buildOverride='auto';
  if(game.mode==='siege'){
    player2=game.secondPlayer?makeHumanPlayer('PLAYER 2','#ff9d2e'):null;
    game.siegeRound=1;game.siegePlayers=0;game.siegeEnemies=0;game.siegeMatchOver=false;
    setupSiegeRound(false);
    hud.mode.textContent='SIEGE';
    return;
  }
  buildMap();if(game.mode==='frag')applyFragArena();if(game.mode==='survivor'||game.mode==='frag')pickups=[];
  buildCharges=BUILD_MAX;buildRecharge=0;buildFeedback=0;selectedBuildType='wall';buildHeld=false;buildPreview=null;game.timeLeft=game.time;game.dmItemSpawnTimer=15;
  player.team=game.mode==='survivor'?1:(warMode()?(window.NeonWarUI.selection().faction==='cats'?1:2):0);player.dead=false;player.upgradePoints=0;player.killStreak=0;player.upgrades=createUpgradeState();player.yaw=Math.PI;player.pitch=0;resetWeaponInventory(player,'pistol');spawn(player);
  player2=game.secondPlayer?makeHumanPlayer('PLAYER 2','#ff9d2e'):null;if(player2){player2.team=game.mode==='survivor'?1:(warMode()?3-player.team:99);player2.upgrades=createUpgradeState();player2.yaw=0;player2.pitch=0;}
  if(warMode())initWarMatch();else initBots();if(game.mode==='frag')setupFragLoadout();shots=[];projectiles=[];particles=[];lasers=[];impacts=[];muzzleFlashes=[];decals=[];ultimateFx=[];plasmaResidues=[];
  hud.mode.textContent=game.mode==='survivor'?'SURVIVOR WAVE 0':(game.mode==='frag'?'FRAG 1V1':(game.mode==='tdm'?'TEAM DM':'DEATHMATCH'));
  game.started=true;styleGun(player.weapon);
  feed(warMode()?factionName(player.team)+' deployed. Use your selected loadout, follow the objective HUD, and stay near cover.':game.mode==='frag'?'FRAG loaded: sniper-only, no pickups, one-shot duels. The selected elimination target wins. The enemy keeps sniper distance and builds more strategically on higher difficulty.':((game.mode==='survivor'?MAP_META.survivor.name:(MAP_META[game.map]?.name||MAP_META.grid.name))+' loaded. '+(game.secondPlayer?'Split screen enabled with independent player entities. ':'')+'Everyone starts on the edge with a 6-shot pistol. Press F or B/Circle over floor guns to pick them up; press it away from guns to swap between your two weapon slots. Armor and health still auto-collect.'));
}
el('start').onclick=()=>startArenaMatch();
function toggleFullScreen(){const root=document.documentElement;if(!document.fullscreenElement){root.requestFullscreen?.();el('fullscreenBtn').textContent='EXIT FULL SCREEN'}else{document.exitFullscreen?.();el('fullscreenBtn').textContent='FULL SCREEN'}}
try{visualMode=localStorage.getItem('neonArenaVisualQuality')||'auto'}catch{};if(!['auto','high','medium','low'].includes(visualMode))visualMode='auto';applyVisualMode(visualMode,false);if(el('visualQuality'))el('visualQuality').value=visualMode;
el('controllerSettings').onclick=openControllerSettings;el('fullscreenBtn').onclick=toggleFullScreen;document.addEventListener('fullscreenchange',()=>{const b=el('fullscreenBtn');if(b)b.textContent=document.fullscreenElement?'EXIT FULL SCREEN':'FULL SCREEN'});el('controllerTabP1').onclick=()=>setControllerTab(1);el('controllerTabP2').onclick=()=>setControllerTab(2);el('mode')?.addEventListener('change',updateMenuSummary);el('map')?.addEventListener('change',updateMenuSummary);setTimeout(()=>el('start')?.focus(),0);el('saveController').onclick=()=>{saveControllerSettings();saveMatchPreferencesFromFields();updateMenuSummary();closeControllerSettings()};el('closeController').onclick=closeControllerSettings;el('nextRound').onclick=()=>{if(game.mode==='siege'){nextSiegeRoundOrMatch();return;}el('scoreboardOverlay').style.display='none';el('menu').style.display='flex';startArenaMatch();};el('returnMenu').onclick=()=>{survivalInterface?.clearResults();survivalMapRuntime?.dispose();el('scoreboardOverlay').style.display='none';el('menu').style.display='flex';game.started=false;game.over=true;game.dmItemSpawnTimer=15;updateMobileControlVisibility(true);updateMenuSummary()};loadControllerSettings();setupMobileControls();setupGraphicalMenu();updateMenuSummary();
function isHumanActor(e){return e===player||e===player2||!!e?.human}
function sameTeam(a,b){if(game.mode==='survivor')return (!!a?.alien&&!!b?.alien)||(isHumanActor(a)&&isHumanActor(b));if(game.mode==='siege')return (isHumanActor(a)&&isHumanActor(b))||(!!a?.siegeEnemy&&!!b?.siegeEnemy);return warMode()&&a.team===b.team}

function humanBuildState(ent){if(ent===player)return {held:buildHeld,preview:buildPreview,charges:buildCharges,recharge:buildRecharge,feedback:buildFeedback};return {held:ent.buildHeld,preview:ent.buildPreview,charges:ent.buildCharges,recharge:ent.buildRecharge,feedback:ent.buildFeedback}}
function setHumanBuildPreview(ent,p){if(ent===player)buildPreview=p;else ent.buildPreview=p}
function buildTypeFor(ent){return ent===player||ent===player2||ent?.human?(ent.buildOverride||'auto'):(ent?.buildType||'wall')}
function setBuildOverrideFor(ent,type){
  if(!ent||!['auto','wall','platform'].includes(type))return false;
  ent.buildOverride=type;setHumanBuildPreview(ent,null);
  if(type!=='auto'){ent.buildType=type;if(ent===player)selectedBuildType=type;}
  return true;
}
function toggleBuildType(ent){const mode=buildTypeFor(ent),next=mode==='auto'?'wall':mode==='wall'?'platform':'auto';setBuildOverrideFor(ent,next);if(ent===player)msg('BUILD: '+(next==='auto'?'ASSISTED':next.toUpperCase()))}
function setHumanBuildHeld(ent,v){if(ent===player)buildHeld=v;else ent.buildHeld=v}
function setHumanBuildFeedback(ent,v){if(ent===player)buildFeedback=v;else ent.buildFeedback=v}
function humanCharges(ent){return ent===player?buildCharges:ent.buildCharges}
function setHumanCharges(ent,v){if(ent===player)buildCharges=v;else ent.buildCharges=v}
function humanRecharge(ent){return ent===player?buildRecharge:ent.buildRecharge}
function setHumanRecharge(ent,v){if(ent===player)buildRecharge=v;else ent.buildRecharge=v}
function handleHumanButtons(ent,now,prev){if(now.modeToggle&&!prev.modeToggle)toggleBuildType(ent);if(now.pickup&&!prev.pickup)interactWeapon(ent);if(now.reload&&!prev.reload)manualReload(ent);if(now.jump&&!prev.jump)requestHumanJump(ent);if(now.slide&&!prev.slide)requestHumanSlide(ent);if(now.build&&!prev.build){setHumanBuildHeld(ent,true);setHumanBuildPreview(ent,getBuildPreviewFor(ent))}else if(now.build){setHumanBuildPreview(ent,getBuildPreviewFor(ent))}else if(!now.build&&prev.build){placeBuildFor(ent,humanBuildState(ent).preview||getBuildPreviewFor(ent));setHumanBuildHeld(ent,false);setHumanBuildPreview(ent,null)}if(now.shoot&&!prev.shoot){ent.controllerFireHeld=true;if(ent.weapon==='plasma'){}else if(!detonateRemoteGrenades(ent))tryFire(ent,ent===player)}else if(now.shoot){ent.controllerFireHeld=true}else{if(prev.shoot&&ent.weapon==='plasma')releaseHumanPlasma(ent);if(prev.shoot&&ent.mortarActive)releaseMortar(ent);ent.controllerFireHeld=false}}
function updateHumanMovement(ent,dt,moveX,moveForward,lookX=0,lookY=0){
  ent.spawnGrace=Math.max(0,ent.spawnGrace-dt);ent.slowTimer=Math.max(0,(ent.slowTimer||0)-dt);if(ent.slowTimer<=0)ent.slowAmount=0;
  if(ent.dead){updateSlideView(ent,dt);return}
  ent.yaw+=lookX*controller.settings.lookSensitivity*(game.lookSensitivity||1)*dt;ent.pitch=clamp(ent.pitch+lookY*controller.settings.lookSensitivity*(game.lookSensitivity||1)*dt,-1.25,1.25);ent.faceYaw=ent.yaw;
  const fX=Math.sin(ent.yaw),fZ=Math.cos(ent.yaw),rX=Math.cos(ent.yaw),rZ=-Math.sin(ent.yaw),inputMag=Math.min(1,Math.hypot(moveX,moveForward)),ax=moveX*rX+moveForward*fX,az=moveX*rZ+moveForward*fZ,l=Math.hypot(ax,az)||1;
  const slow=1-(ent.slowAmount||0),light=ent.weapon==='smg'&&hasUpgrade('smg','lightweight',ent)?1.10:1,runner=ent.weapon==='smg'&&hasUpgrade('smg','slipstream',ent)?1.06:1,boost=ent.weapon==='smg'&&(ent.slipstreamTimer||0)>0?1.10:1,triage=(ent.menderSpeedBoost||0)>0?1.12:1,harpoonBoost=(ent.harpoonMomentumTimer||0)>0?1.18:1,speed=18.4*slow*light*runner*boost*triage*harpoonBoost*(ent.crouching?.52:1)*(ent.aiming?.8:1);
  if(!updateTacticalSlide(ent,dt,ax,az,speed)){
    const targetVX=ax/l*speed*inputMag,targetVZ=az/l*speed*inputMag,accel=(ent.onGround?(inputMag?25:32):8)*(ent.weapon==='burst'&&(ent.tacticalTimer||0)>0?1.18:1),blend=1-Math.exp(-accel*dt);
    ent.vx+=(targetVX-ent.vx)*blend;ent.vz+=(targetVZ-ent.vz)*blend;
  }
  physics(ent,dt);updateSlideView(ent,dt);updateWeaponTimers(ent,dt);updateMortarTarget(ent,dt,moveX,moveForward);pickupCheck(ent);updateBuildChargesFor(ent,dt);
  if(humanBuildState(ent).held)setHumanBuildPreview(ent,getBuildPreviewFor(ent));
  const held=ent===player&&game.p1Input==='keyboard'?fireHeld:ent.controllerFireHeld;
  if(held&&!ent.dead&&!game.upgradesOpen){if(ent.weapon==='plasma')chargeHumanPlasma(ent,dt);else if(ent.weapon==='laser')tryFire(ent,ent===player);else if(!WEAPONS[ent.weapon]?.semi)tryFire(ent,ent===player)}
}
function chargeHumanPlasma(ent,dt){if(ent.cool>0||ent.reload>0||ent.heat>.92)return;ent.charge=clamp((ent.charge||0)+WEAPONS.plasma.chargeRate*dt,0,1)}
function releaseHumanPlasma(ent){if(ent.weapon!=='plasma'||ent.dead||game.upgradesOpen)return;if((ent.charge||0)>.05)tryFire(ent,ent===player,ent.charge);ent.charge=0}
function survivorBuildMax(){return BUILD_MAX+(game.mode==='survivor'?(game.survivorBuildBonus||0):0)}
function survivorBuildRecharge(){return BUILD_RECHARGE*(game.mode==='survivor'?(game.survivorRechargeMult||1):1)}
function updateBuildChargesFor(ent,dt){if(ent===player){updateBuildCharges(dt);return}const max=survivorBuildMax(),rate=survivorBuildRecharge();if(siegeBuildActive()){ent.buildCharges=BUILD_MAX;ent.buildRecharge=0;return}if(ent.buildCharges>=max){ent.buildRecharge=0;return}ent.buildRecharge+=dt;while(ent.buildRecharge>=rate&&ent.buildCharges<max){ent.buildRecharge-=rate;ent.buildCharges++;if(ent.buildCharges===max)ent.buildRecharge=0}}

function updatePlayer(dt){
  pollController();if(game.shopOpen){if(player.mortarActive)releaseMortar(player);if(player2?.mortarActive)releaseMortar(player2);clearMobileAutoFire(player);mouseDX=mouseDY=0;fireHeld=false;controllerFireHeld=false;return;}
  if(game.p1Input==='controller'){const input=controller.players[1];mouseDX=mouseDY=0;updateHumanMovement(player,dt,input.moveX,input.moveZ,input.lookX,input.lookY)}
  else if(game.p1Input==='touch'){mouseDX=mouseDY=0;updateHumanMovement(player,dt,mobileInput.moveX,mobileInput.moveForward,mobileInput.lookX*.92,mobileInput.lookY*.82);updateMobileAimAndFire(player,dt)}
  else{if(!player.dead){player.yaw+=mouseDX*.00225*(game.lookSensitivity||1);player.pitch=clamp(player.pitch+mouseDY*.00225*(game.lookSensitivity||1),-1.25,1.25)}mouseDX=mouseDY=0;updateHumanMovement(player,dt,(keys.KeyD?1:0)-(keys.KeyA?1:0),(keys.KeyW?1:0)-(keys.KeyS?1:0))}
  if(game.secondPlayer&&player2){
    if(game.p2Input==='controller'){const input=controller.players[2];updateHumanMovement(player2,dt,input.moveX,input.moveZ,input.lookX,input.lookY)}
    else if(game.p2Input==='keyboard'){const now={shoot:!!keys.Numpad1,build:!!keys.Numpad2,reload:!!keys.Enter,pickup:!!keys.Numpad5,jump:!!keys.Numpad0,modeToggle:!!keys.Numpad3};handleHumanButtons(player2,now,p2KeyPrev);p2KeyPrev=now;const mx=(keys.Numpad6?1:0)-(keys.Numpad4?1:0),mf=(keys.ArrowUp?1:0)-(keys.ArrowDown?1:0),lookX=((keys.ArrowRight?1:0)-(keys.ArrowLeft?1:0))*1.55;updateHumanMovement(player2,dt,mx,mf,lookX,0)}
  }
}
function updateWeaponTimers(ent,dt){ent.warRecoilPitch=(ent.warRecoilPitch||0)*Math.exp(-dt*(WEAPONS[ent.weapon]?.recovery||7));ent.warRecoilYaw=(ent.warRecoilYaw||0)*Math.exp(-dt*(WEAPONS[ent.weapon]?.recovery||7));ent.warBurstIdle=(ent.warBurstIdle||0)+dt;if(ent.warBurstIdle>.45)ent.warShotIndex=0;ent.weaponSwitchTimer=Math.max(0,(ent.weaponSwitchTimer||0)-dt);ent.mortarImpactTimer=Math.max(0,(ent.mortarImpactTimer||0)-dt);ent.harpoonTetherTimer=Math.max(0,(ent.harpoonTetherTimer||0)-dt);if(ent.cool>0)ent.cool-=dt;if(ent.reload>0){ent.reload-=dt;if(ent.reload<=0){const w=WEAPONS[ent.weapon];if(w.kind!=='plasma'&&w.kind!=='laser'&&w.kind!=='mender')ent.ammo=w.mag;else ent.heat=0;ent.reload=0;ent.reloadTotal=0;ent.reloadAnimType=null;if(getUltimateReady(ent))consumeUltimate(ent);else if(ent===player)msg(w.name+' READY');saveActiveWeaponState(ent)}}if(ent.weapon==='plasma')ent.heat=Math.max(0,(ent.heat||0)-WEAPONS.plasma.coolRate*dt);else if(ent.weapon==='laser')ent.heat=Math.max(0,(ent.heat||0)-WEAPONS.laser.coolRate*dt);else if(ent.weapon==='mender'){const w=WEAPONS.mender;ent.heat=Math.max(0,(ent.heat||0)-w.coolRate*dt);if(ent.menderOverheated&&ent.heat<=w.recoveryHeat){ent.menderOverheated=false;if(ent===player)msg('MENDER BEAM READY');saveActiveWeaponState(ent)}}}
function pickupCheck(ent){if(game.mode==='frag')return;for(const p of pickups){if(p.gone||p.taken>0)continue;const dy=Math.abs(ent.y-(p.y+1.1));if(hypot2(ent.x-p.x,ent.z-p.z)<2.2&&dy<3.8&&lineClear(actorEyePosition(ent).x,actorEyePosition(ent).y-.05,actorEyePosition(ent).z,p.x,p.y+.28,p.z)){if(p.kind==='armor'){if(ent.armor>=ent.maxArmor)continue;ent.armor=Math.min(ent.maxArmor,ent.armor+p.amount);if(p.oneShot)p.gone=true;else p.taken=p.respawn;if(ent===player){msg('+'+p.amount+' ARMOR');feed('Collected '+p.size+' armor plate')}else ent.target=pick(nav)}else if(p.kind==='health'){if(ent.hp>=ent.maxHp)continue;ent.hp=Math.min(ent.maxHp,ent.hp+p.amount);if(p.oneShot)p.gone=true;else p.taken=p.respawn;if(ent===player){msg('+'+p.amount+' HP');feed('Collected small health drop')}else ent.target=pick(nav)}else{if(isHumanActor(ent))continue;takeWeaponPickup(ent,p)}}}}
function chargeColor(tier){return tier>=4?'#ff2bff':tier>=3?'#ffea00':tier>=2?'#00ffa8':'#9b5cff'}
function chargeTier(charge){return charge>.92?4:charge>.62?3:charge>.32?2:1}
function chargePlayerPlasma(dt){if(player.cool>0||player.reload>0||player.heat>.92)return;player.charge=clamp((player.charge||0)+WEAPONS.plasma.chargeRate*dt,0,1);}
function releasePlayerPlasma(){if(player.weapon!=='plasma'||player.dead||game.upgradesOpen)return;if((player.charge||0)>.05)tryFire(player,true,player.charge);player.charge=0;styleGun(player.weapon)}
function tryFire(shooter,isPlayer,charge=0){if(shooter.dead||shooter.cool>0||shooter.reload>0||(isPlayer&&game.upgradesOpen))return;const w=WEAPONS[shooter.weapon];if(w.kind==='mortar')return beginMortar(shooter,isPlayer);if(w.kind==='laser'){const wasClip=ultimateClipActive(shooter,'laser'),wasShot=!!shooter.ultimateShot,r=fireLaserBeam(shooter,isPlayer);if(wasClip)spendClipUltimateShot(shooter);else if(wasShot)endUltimateShot(shooter);return r}if(w.kind==='mender')return fireMenderBeam(shooter,isPlayer);if(w.kind!=='plasma'&&shooter.ammo<=0){manualReload(shooter,false);return}const wasClip=ultimateClipActive(shooter,shooter.weapon),wasShot=!!shooter.ultimateShot;if(w.kind==='plasma'){const tier=chargeTier(charge||.08),heatCost=.10+.09*tier;if((shooter.heat||0)+heatCost>.99)return;shooter.heat=(shooter.heat||0)+heatCost;shooter.cool=w.cool+.08*tier;}else{shooter.ammo--;let coolMult=1;if(isPlayer&&shooter.weapon==='assault'&&hasUpgrade('assault','overdrive'))coolMult=.78;if(isPlayer&&shooter.weapon==='assault'&&hasUpgrade('assault','momentumClip')&&(shooter.assaultMomentum||0)>=.72)coolMult*=.90;if(isPlayer&&shooter.weapon==='burst'&&hasUpgrade('burst','quickCycle'))coolMult=.82;if(isPlayer&&shooter.weapon==='rocket'&&hasUpgrade('rocket','velocity'))coolMult=.88;shooter.cool=w.cool*coolMult;}const momentumShot=isPlayer&&(shooter.harpoonMomentumShot||0)>0&&shooter.weapon!=='harpoon',baseDamage=w.damage,baseSpread=w.spread;if(momentumShot){w.damage*=1.18;w.spread*=.35;shooter.harpoonMomentumShot=0;tier3Cue('MOMENTUM SHOT',2.2)}if(w.kind==='plasma')firePlasma(shooter,isPlayer,charge||.08);else if(w.kind==='rocket')fireRocket(shooter,isPlayer);else if(w.kind==='punch')firePunchGun(shooter,isPlayer);else if(w.kind==='harpoon')fireHarpoon(shooter,isPlayer);else fireHitscan(shooter,isPlayer,w);w.damage=baseDamage;w.spread=baseSpread;if(wasClip)spendClipUltimateShot(shooter);else if(wasShot)endUltimateShot(shooter);if(w.kind!=='plasma'&&shooter.ammo<=0)manualReload(shooter,false);}
function dirFrom(shooter,spread=0){spread*=typeof slideSpreadMultiplier==='function'?slideSpreadMultiplier(shooter):1;const warWeapon=WEAPONS[shooter.weapon];if(warWeapon?.pattern)spread*=1+Math.min(1.5,Math.hypot(shooter.vx||0,shooter.vz||0)/10)*(warWeapon.movementPenalty||1)+(shooter.onGround===false?.7:0);if(shooter.aiming)spread*=.4;if(shooter.crouching)spread*=.7;let yaw=shooter.yaw+(shooter.warRecoilYaw||0)+rand(-spread,spread), pitch=(shooter.pitch||0)-(shooter.warRecoilPitch||0)+rand(-spread,spread);const sy=Math.sin(yaw),cy=Math.cos(yaw),cp=Math.cos(pitch),sp=Math.sin(pitch);return {x:sy*cp,y:-sp,z:cy*cp}}
function muzzlePoint(shooter,d){
  const yaw=isHumanActor(shooter)?(shooter.yaw||0):(shooter.faceYaw??shooter.yaw??0),fx=Math.sin(yaw),fz=Math.cos(yaw),sx=Math.cos(yaw),sz=-Math.sin(yaw);
  if(isHumanActor(shooter)){const eye=actorEyePosition(shooter),desired={x:eye.x+sx*.28+d.x*.58,y:eye.y-.14+d.y*.58,z:eye.z+sz*.28+d.z*.58},delta={x:desired.x-eye.x,y:desired.y-eye.y,z:desired.z-eye.z},len=Math.hypot(delta.x,delta.y,delta.z),ray={x:delta.x/len,y:delta.y/len,z:delta.z/len},hit=rayWorld(eye,ray,len);return hit?{x:eye.x+ray.x*Math.max(0,hit.dist-.03),y:eye.y+ray.y*Math.max(0,hit.dist-.03),z:eye.z+ray.z*Math.max(0,hit.dist-.03)}:desired;}
  const g=gunSpec(shooter.weapon),forward=.72+g.body[2]/2+g.barrel[2];
  return {x:shooter.x+fx*forward+sx*.18,y:shooter.alien?alienTargetY(shooter,shooter.y-.99):humanTargetY(shooter,'body')-.20,z:shooter.z+fz*forward+sz*.18};
}
let combatAudioContext=null,combatNoise=null,combatVoices=0,lastImpactSound=0;
function enableCombatAudio(){
  try{if(localStorage.getItem('neonArenaSound')==='off')return;const Audio=globalThis.AudioContext||globalThis.webkitAudioContext;if(!Audio)return;combatAudioContext= combatAudioContext||new Audio();if(combatAudioContext.state==='suspended')combatAudioContext.resume().catch(()=>{});if(!combatNoise){combatNoise=combatAudioContext.createBuffer(1,Math.ceil(combatAudioContext.sampleRate*.28),combatAudioContext.sampleRate);const data=combatNoise.getChannelData(0);for(let i=0;i<data.length;i++)data[i]=Math.random()*2-1;}}catch{}
}
document.addEventListener('pointerdown',enableCombatAudio,{passive:true});document.addEventListener('keydown',enableCombatAudio,{passive:true});
const soundSetting=el('soundEnabled');if(soundSetting){try{soundSetting.checked=localStorage.getItem('neonArenaSound')!=='off'}catch{}soundSetting.addEventListener('change',()=>{try{localStorage.setItem('neonArenaSound',soundSetting.checked?'on':'off')}catch{}if(soundSetting.checked)enableCombatAudio();else combatAudioContext?.suspend().catch(()=>{})})}
function combatAudio(type,x,y,z){
  const ac=combatAudioContext;if(!ac||ac.state!=='running'||combatVoices>=10||soundSetting?.checked===false||combatVolume()<=0)return;
  const ears=[player,game.secondPlayer?player2:null].filter(e=>e&&!e.dead),distance=Math.min(...ears.map(e=>Math.hypot(e.x-x,(e.y-y)*.35,e.z-z))),volume=NeonCombatFeedback.attenuation(distance,type==='step'?16:54);if(volume<.006)return;
  const now=ac.currentTime,impact=type.startsWith('impact:');if(impact&&now-lastImpactSound<.055)return;if(impact)lastImpactSound=now;
  const material=type.split(':')[1],surface=NeonCombatFeedback.SURFACES[material],profile=type.startsWith('blast:')?{frequency:type==='blast:plasma'?95:42,decay:.32}:NeonCombatFeedback.FIRING[type],slide=type==='slide',step=type==='step',hit=type==='hit'||type==='headshot';
  const duration=profile?.decay||(slide?.24:step?.07:hit?.06:.10),frequency=profile?.frequency||surface?.sound||(slide?260:step?130:hit?1350:180);
  try{const gain=ac.createGain(),filter=ac.createBiquadFilter(),noise=ac.createBufferSource(),tone=ac.createOscillator();filter.type=surface?'bandpass':'lowpass';filter.frequency.value=surface?frequency:profile?frequency*14:700;filter.Q.value=surface?2:.7;gain.gain.setValueAtTime(Math.max(.0001,volume*combatVolume()*(hit?.025:step?.022:slide?.035:profile?.07:.035)),now);gain.gain.exponentialRampToValueAtTime(.0001,now+duration);filter.connect(gain);gain.connect(ac.destination);noise.buffer=combatNoise;noise.connect(filter);tone.type=hit?'sine':profile&&frequency>300?'sine':'triangle';tone.frequency.setValueAtTime(frequency,now);tone.frequency.exponentialRampToValueAtTime(Math.max(30,frequency*(hit?1.4:.45)),now+duration);tone.connect(gain);combatVoices++;noise.onended=()=>{combatVoices--;noise.disconnect();tone.disconnect();filter.disconnect();gain.disconnect()};noise.start(now);noise.stop(now+duration);tone.start(now);tone.stop(now+duration)}catch{}
}
function recordCombatFeedback(victim,attacker,head,armorDamage,hpDamage){
  if(armorDamage<=0&&hpDamage<=0)return;const now=performance.now()/1000;
  victim.hitReactUntil=now+.18;victim.hitReactYaw=attacker?Math.atan2(attacker.x-victim.x,attacker.z-victim.z):0;
  if(isHumanActor(victim)&&attacker&&attacker!==victim)victim.damageCue={until:now+.65,yaw:Math.atan2(attacker.x-victim.x,attacker.z-victim.z),strength:clamp((armorDamage+hpDamage)/75,.18,.65)};
  if(isHumanActor(attacker)&&attacker!==victim){const kind=NeonCombatFeedback.feedbackKind({head,armor:armorDamage,shield:victim.shieldFlash>0,killed:victim.hp<=0});attacker.combatHit={until:now+(kind==='kill'?.32:.19),kind,head};combatAudio(head?'headshot':'hit',attacker.x,attacker.y,attacker.z)}
}
let ambientAudio=null,ambientNext=0;
function updateAmbientAudio(){
  const ac=combatAudioContext;if(!ac||ac.state!=='running'||ac.currentTime<ambientNext)return;ambientNext=ac.currentTime+.25;
  const playing=game.started&&!game.over&&!game.upgradesOpen&&!game.shopOpen;if(!ambientAudio&&!playing)return;
  try{if(!ambientAudio){const gain=ac.createGain(),filter=ac.createBiquadFilter(),noise=ac.createBufferSource(),tone=ac.createOscillator();filter.type='lowpass';filter.frequency.value=160;noise.buffer=combatNoise;noise.loop=true;noise.connect(filter);tone.type='sine';tone.frequency.value=48;tone.connect(filter);filter.connect(gain);gain.gain.value=0;gain.connect(ac.destination);noise.start();tone.start();ambientAudio={gain,filter,tone}}
    const map=game.mode==='survivor'?game.survivalMap:game.map,organic=/hive|heart/.test(map),garden=map==='pastel'||/xenofall|ice/.test(map);ambientAudio.filter.frequency.setTargetAtTime(organic?95:garden?280:150,ac.currentTime,.4);ambientAudio.tone.frequency.setTargetAtTime(organic?37:garden?62:map==='reactor'?45:54,ac.currentTime,.4);ambientAudio.gain.gain.setTargetAtTime(playing?.004*combatVolume():0,ac.currentTime,.15);
  }catch{}
}
function surfaceImpact(hit,weapon='assault'){
  if(!hit?.normal||hit.surfaceFx)return;hit.surfaceFx=true;const material=NeonCombatFeedback.surfaceFor(hit.block||{material:'concrete'},game.mode==='survivor'?game.survivalMap:ARENA_TILESETS[game.map]),style=NeonCombatFeedback.SURFACES[material],n=hit.normal;
  // Offset toward the shooter: fragments and decals never originate behind cover.
  const x=hit.x+n.x*.025,y=hit.y+n.y*.025,z=hit.z+n.z*.025,count=scaledVisualCount(style.count);
  if(typeof tacticalController!=='undefined')tacticalController?.hear({kind:'impact',x,y,z,range:12});
  addImpact(hit,style.color,material==='concrete'?.21:.13);
  for(let i=0;i<count;i++){const life=rand(style.life*.55,style.life),speed=style.speed;particles.push({kind:style.kind,x,y,z,vx:n.x*speed+rand(-speed*.45,speed*.45),vy:n.y*speed+rand(.2,speed*.7),vz:n.z*speed+rand(-speed*.45,speed*.45),life,maxLife:style.life,c:style.color,size:material==='concrete'?rand(3,6):rand(1.2,2.8)})}
  trimVisualArray(particles,visualPreset().maxParticles);combatAudio('impact:'+material,x,y,z);
}
function drawCombatFeedback(ent){
  const now=performance.now()/1000,cx=viewRect.x+viewRect.w/2,cy=viewRect.y+viewRect.h/2,hit=ent.combatHit,cue=ent.damageCue;
  ctx.save();if(cue?.until>now){const angle=cue.yaw-(ent.yaw||0),radius=Math.min(viewRect.w,viewRect.h)*.28,x=cx+Math.sin(angle)*radius,y=cy-Math.cos(angle)*radius;ctx.translate(x,y);ctx.rotate(angle);ctx.globalAlpha=clamp((cue.until-now)*2,0,cue.strength);ctx.fillStyle='#ff4269';ctx.beginPath();ctx.moveTo(0,-12);ctx.lineTo(9,6);ctx.lineTo(0,2);ctx.lineTo(-9,6);ctx.closePath();ctx.fill();ctx.setTransform(DPR,0,0,DPR,0,0)}
  if(hit?.until>now){const colors={health:'#ffffff',armor:'#ffc567',shield:'#68cfff',headshot:'#ff9b85',kill:'#ff527b'},size=hit.kind==='kill'?12:9,gap=hit.head?5:4;ctx.globalAlpha=clamp((hit.until-now)*8,0,1);ctx.strokeStyle=colors[hit.kind];ctx.lineWidth=hit.head?2.6:1.8;ctx.beginPath();for(const [x,y] of [[-1,-1],[1,-1],[-1,1],[1,1]]){ctx.moveTo(cx+x*gap,cy+y*gap);ctx.lineTo(cx+x*size,cy+y*size)}ctx.stroke();if(hit.kind==='shield'){ctx.beginPath();ctx.arc(cx,cy,15,Math.PI*1.15,Math.PI*1.85);ctx.stroke()}if(hit.head){ctx.fillStyle=colors[hit.kind];ctx.font='900 10px Arial';ctx.textAlign='center';ctx.fillText(hit.kind==='kill'?'HEADSHOT KO':'HEADSHOT',cx,cy-23)}}ctx.restore();
}
const MUZZLE_STYLE={
  pistol:{color:'#dff8ff',size:.72,life:.07},assault:{color:'#c9f7ff',size:.78,life:.065},smg:{color:'#fff4a8',size:.68,life:.05},shotgun:{color:'#ff9a3d',size:1.48,life:.105},sniper:{color:'#ffffff',size:1.18,life:.085},burst:{color:'#58ffd6',size:.82,life:.115},arc:{color:'#48c7ff',size:1.02,life:.09},rocket:{color:'#ff4a30',size:1.28,life:.11},mortar:{color:'#ff204e',size:1.34,life:.12},punch:{color:'#ff58dc',size:1.05,life:.10},harpoon:{color:'#bdfcff',size:.92,life:.105},plasma:{color:'#d85cff',size:1.08,life:.10},laser:{color:'#ff55d8',size:.72,life:.055},mender:{color:'#8ffaff',size:.56,life:.05}
};
const CAMERA_KICK={pistol:[.010,.003],assault:[.006,.0025],smg:[.004,.004],shotgun:[.035,.009],sniper:[.040,.006],burst:[.010,.003],arc:[.014,.005],rocket:[.052,.010],mortar:[.060,.011],punch:[.025,.008],harpoon:[.020,.004],plasma:[.018,.005],laser:[.0025,.003],mender:[.001,.0015]};
function addCameraKick(shooter,type,charge=0){
  if(!isHumanActor(shooter))return;const k=CAMERA_KICK[type]||CAMERA_KICK.pistol,scale=type==='plasma'?(.7+charge*.65):1;
  shooter.recoilPitch=clamp((shooter.recoilPitch||0)+k[0]*scale,0,.075);shooter.recoilYaw=clamp((shooter.recoilYaw||0)+rand(-k[1],k[1]),-.025,.025);shooter.recoilRoll=clamp((shooter.recoilRoll||0)+rand(-k[1]*.7,k[1]*.7),-.018,.018);
}
function updateCameraKick(ent,dt){if(!ent)return;const decay=Math.exp(-18*dt);ent.recoilPitch=(ent.recoilPitch||0)*decay;ent.recoilYaw=(ent.recoilYaw||0)*decay;ent.recoilRoll=(ent.recoilRoll||0)*decay;}
function addMuzzleFlash(shooter,isPlayer,d,color,charge=0){
  const type=shooter.weapon,w=WEAPONS[type]||WEAPONS.assault;warShot(shooter,w);const style=MUZZLE_STYLE[type]||MUZZLE_STYLE[w.kind==='shotgun'?'shotgun':type==='squeakshot'?'sniper':type==='purr90'?'smg':'assault'],m=muzzlePoint(shooter,d),life=style.life;
  muzzleFlashes.push({x:m.x,y:m.y,z:m.z,dx:d.x,dy:d.y,dz:d.z,yaw:shooter.yaw||0,color:color||style.color||w.color,size:(w.suppressed?.30:style.size)+(type==='plasma'?charge*.65:0),life,maxLife:life,type,owner:shooter,pulse:type==='burst'?((shooter.muzzlePulse=(shooter.muzzlePulse||0)+1)%3):0});
  trimVisualArray(muzzleFlashes,visualPreset().maxMuzzles);addCameraKick(shooter,type,charge);
  if(typeof tacticalController!=='undefined')tacticalController?.hear({x:shooter.x,y:shooter.y,z:shooter.z,team:shooter.team,source:shooter.entityId,kind:w.suppressed?'suppressed':'gunfire',range:w.hearingRange||(type==='sniper'||type==='rocket'?58:38)});
  combatAudio(type,m.x,m.y,m.z);const smoke=NeonCombatFeedback.FIRING[type]?.smoke||0;
  if(visualQuality!=='low')for(let i=0;i<smoke;i++)particles.push({kind:'smoke',x:m.x,y:m.y,z:m.z,vx:d.x*.8+rand(-.2,.2),vy:.7+d.y*.5,vz:d.z*.8+rand(-.2,.2),life:.22,maxLife:.22,c:'#a1a5ac',size:rand(2,4)});
  trimVisualArray(particles,visualPreset().maxParticles);
}
function addFloorDecal(hit,type='bullet',size=0){
  surfaceImpact(hit,type);
  if(!hit||!hit.normal||hit.normal.y<.55)return;if(visualQuality==='low'&&visualDistance(hit.x,hit.y,hit.z,player)>visualPreset().near)return;if(type==='laser'){const recent=decals[decals.length-1];if(recent?.type==='laser'&&Math.hypot(recent.x-hit.x,recent.z-hit.z)<.4&&recent.life>recent.maxLife-.14)return;}const style={bullet:['#17222b',.16,16],shotgun:['#1d2025',.13,13],rocket:['#23150f',1.65,24],plasma:['#6e238e',.72,18],arc:['#087f91',.42,15],laser:['#5b163f',.34,12],punch:['#2a1c36',.62,20],heavy:['#302118',2.1,26]}[type]||['#17222b',.16,16],life=(style[2]+rand(-2,2))*visualPreset().decalLife;
  decals.push({x:hit.x,y:hit.y+.018,z:hit.z,type,color:style[0],size:size||style[1],angle:rand(0,TAU),life,maxLife:life});trimVisualArray(decals,visualPreset().maxDecals);
}
function rayBox(o,d,b,maxDist){let near=0,far=maxDist,normal={x:0,y:0,z:0};const mins={x:b.x-b.w/2,y:b.y-b.h/2,z:b.z-b.d/2},maxs={x:b.x+b.w/2,y:b.y+b.h/2,z:b.z+b.d/2};for(const axis of ['x','y','z']){if(Math.abs(d[axis])<1e-7){if(o[axis]<mins[axis]||o[axis]>maxs[axis])return null;continue}let t1=(mins[axis]-o[axis])/d[axis],t2=(maxs[axis]-o[axis])/d[axis],sign=-1;if(t1>t2){[t1,t2]=[t2,t1];sign=1}if(t1>near){near=t1;normal={x:0,y:0,z:0};normal[axis]=sign}far=Math.min(far,t2);if(near>far)return null}if(far<0||near>maxDist)return null;if(near===0){const axis=['x','y','z'].sort((a,c)=>Math.abs(d[c])-Math.abs(d[a]))[0];normal[axis]=-Math.sign(d[axis])}return {dist:near,x:o.x+d.x*near,y:o.y+d.y*near,z:o.z+d.z*near,normal,block:b}}
function rayWorld(o,d,range,includeFloor=true,inflate=0){let best=null;for(const b of blocksAlongRay(o,d,range,inflate)){if(!includeFloor&&b.kind==='platform'&&b.y<.2)continue;const shape=inflate?{...b,w:b.w+inflate*2,h:b.h+inflate*2,d:b.d+inflate*2}:b,hit=rayBox(o,d,shape,range);if(hit&&(!best||hit.dist<best.dist))best=hit}return best}
function structureDamageMultiplier(weapon,source){let m={assault:1,smg:.75,burst:1.05,arc:.9,shotgun:1.2,sniper:1.5,plasma:1.4,laser:1.1,mender:.1,rocket:2.35,mortar:1.45,punch:2.15}[weapon]||1;if(source===player&&weapon==='shotgun'&&hasUpgrade('shotgun','breacher'))m*=1.45;if(source===player&&weapon==='shotgun'&&hasUpgrade('shotgun','breacher2'))m*=1.3;return m}
function damageStructure(piece,amount,source,weapon='assault',hit=null){if(!piece||piece.active===false||!piece.destructible)return false;if(game.mode==='survivor'&&game.wallThorns&&source?.alien&&piece.builtBy&&!source.dead){source.hp-=Math.min(28,Math.max(5,amount*.10));source.stagger=Math.max(source.stagger||0,.18);if(source.hp<=0)kill(source,piece.builtBy,false);}if(performance.now()/1000-piece.createdAt<.2&&piece.builtBy===source)return false;const armorBoosted=(piece.bonusHp||0)>0;let incomingDamage=amount*structureDamageMultiplier(weapon,source);if((piece.fortifySealTimer||0)>0)incomingDamage*=.8;if((piece.thunderGuardTimer||0)>0)incomingDamage*=.68;if(source?.wallDamageCutTimer>0)incomingDamage*=.55;if(armorBoosted&&weapon==='acid')incomingDamage*=.55;if(armorBoosted&&weapon==='burrow')incomingDamage*=.65;if((piece.bonusHp||0)>0){const absorbed=Math.min(piece.bonusHp,incomingDamage);piece.bonusHp-=absorbed;incomingDamage-=absorbed;piece.armorBoosted=piece.bonusHp>0;}const hpDamage=Math.min(piece.hp,incomingDamage);piece.hp-=incomingDamage;if(weapon==='acid'&&hpDamage>0)piece.acidDamage=Math.min(piece.maxHp,(piece.acidDamage||0)+hpDamage);piece.flash=.12;if(piece.compoundPieces)for(const part of piece.compoundPieces){part.hp=piece.hp;part.flash=piece.flash}worldGeometryDirty=true;if(hit)surfaceImpact(hit,weapon);if(piece.hp<=0)destroyStructure(piece,source);return true}
function destroyStructure(piece,source){if(!piece||piece.active===false||!piece.destructible)return;if(typeof game!=='undefined'&&game.mode==='survivor'&&typeof survivalProgress!=='undefined'){if(isHumanActor(source))survivalProgress?.recordEvent('structuresDestroyed');survivalController?.invalidate();survivalMapRuntime?.environmentStateChanged();}if(piece.builtBy)activeBuildCount=Math.max(0,activeBuildCount-1);survivalMapRuntime?.environmentStateChanged();piece.active=false;if(piece.compoundPieces)for(const part of piece.compoundPieces)part.active=false;markBlocksDirty();unregisterBuildPiece(piece);burst(piece.x,piece.type==='platform'?piece.y+piece.h:piece.y,piece.z,piece.builtBy?'#65f7ff':piece.c,16,.55);for(let i=0;i<10;i++)particles.push({kind:'debris',x:piece.x+rand(-piece.w/3,piece.w/3),y:(piece.type==='platform'?piece.y+piece.h:piece.y)+rand(-.4,.4),z:piece.z+rand(-piece.d/3,piece.d/3),vx:rand(-5,5),vy:rand(2,8),vz:rand(-5,5),life:rand(.3,.7),maxLife:.7,c:shadeColor(piece.c,-35),size:rand(3,6)});if(typeof tacticalController!=='undefined')tacticalController?.hear({kind:'breach',x:piece.x,y:piece.y,z:piece.z,team:source?.team,source:source?.entityId,range:35});if(source===player)feed('Cover destroyed');for(const b of bots){b.route=null;b.think=0}collapseUnsupportedBuilds()}
function damageStructuresInRadius(x,y,z,radius,amount,source,weapon){for(const piece of [...new Set(blocksNear(x,z,radius+1).map(b=>b.structure))]){if(!piece||piece.active===false||!piece.destructible)continue;const py=piece.type==='platform'?piece.y+piece.h/2:piece.y,dx=Math.max(0,Math.abs(x-piece.x)-piece.w/2),dy=Math.max(0,Math.abs(y-py)-piece.h/2),dz=Math.max(0,Math.abs(z-piece.z)-piece.d/2),dist=Math.hypot(dx,dy,dz);if(dist<radius)damageStructure(piece,amount*(1-dist/radius),source,weapon)}}
function addImpact(hit,color='#ffffff',size=.18){if(!hit)return;const n=hit.normal,eps=.012;impacts.push({x:hit.x+n.x*eps,y:hit.y+n.y*eps,z:hit.z+n.z*eps,nx:n.x,ny:n.y,nz:n.z,size:size*rand(.78,1.22),life:rand(3.4,5.2)*visualPreset().decalLife,maxLife:5.2*visualPreset().decalLife,color});trimVisualArray(impacts,visualPreset().maxImpacts)}
function bloodBurst(x,y,z,head=false,damage=0){const count=scaledVisualCount(Math.min(24,head?16+Math.round(damage/20):9+Math.round(damage/28)));for(let i=0;i<count;i++)particles.push({kind:'blood',x,y,z,vx:rand(-4.8,4.8),vy:rand(1.1,7.2)+(head?1.8:0),vz:rand(-4.8,4.8),life:rand(.28,.72),maxLife:.72,c:i%3?'#d7194b':'#7d0928',size:rand(2.2,4.6)});trimVisualArray(particles,visualPreset().maxParticles)}
function armorSpark(x,y,z,count=10){count=scaledVisualCount(count);for(let i=0;i<count;i++)particles.push({kind:'spark',x,y,z,vx:rand(-5.5,5.5),vy:rand(0,7.4),vz:rand(-5.5,5.5),life:rand(.16,.48),maxLife:.5,c:i%2?'#8ffaff':'#237dff',size:rand(2.5,5.2)});trimVisualArray(particles,visualPreset().maxParticles)}
function applyHitUpgrade(shooter,victim,weapon,head,hitPoint,dir,dist,dealt=0){if(shooter!==player||!victim)return;const hp=hitPoint||{x:victim.x,y:victim.y-.7,z:victim.z};
  if((weapon==='assault'&&hasUpgrade('assault','electric'))||(weapon==='smg'&&hasUpgrade('smg','static')&&dist<18))applySlow(victim,weapon==='smg'&&hasUpgrade('smg','swarm')?.42:.28,weapon==='smg'?1.2:1.05);
  if(weapon==='assault'&&hasUpgrade('assault','chain')&&Math.random()<.55){const storm=hasUpgrade('assault','stormConductor'),used=new Set([victim]);let from=victim,fromPoint=hp,jumps=storm?3:1;for(let jump=0;jump<jumps;jump++){const range=storm&&nearActiveWall(from,5)?9.5:7.5;let other=null,bd=range;for(const t of targetsFor(shooter,true)){if(used.has(t)||t.dead)continue;const d=hypot2(t.x-from.x,t.z-from.z);if(d<bd){bd=d;other=t}}if(!other)break;used.add(other);const chainDamage=(storm?[16,12,9][jump]:[18][jump]);damage(other,chainDamage,shooter,false,{x:other.x,y:other.y-.7,z:other.z},{weapon:'assault',armorMult:.75});addArcVisual(fromPoint,other,'#65f7ff',storm?3:3.5);from=other;fromPoint={x:other.x,y:other.y-.7,z:other.z}}}
  if(weapon==='assault'&&hasUpgrade('assault','momentumClip')){shooter.assaultMomentum=clamp((shooter.assaultMomentum||0)+Math.min(.16,.045+dealt/520),0,1);shooter.momentumGrace=.55}
  if(weapon==='smg'&&dist<12&&hasUpgrade('smg','voltDash')&&!shooter.voltDashCharge){shooter.voltDashMeter=(shooter.voltDashMeter||0)+dealt;if(shooter.voltDashMeter>=150&&(shooter.voltDashCooldown||0)<=0){shooter.voltDashCharge=1;shooter.voltDashMeter=150;msg('VOLT DASH READY')}}
  if(weapon==='smg'&&hasUpgrade('smg','slipstream')){if((shooter.slipstreamTimer||0)<=0)tier3Cue('SLIPSTREAM',2.4);shooter.slipstreamTimer=.8;}
  if(weapon==='shotgun'&&(hasUpgrade('shotgun','breacher')||hasUpgrade('shotgun','breacher2'))){const k=hasUpgrade('shotgun','breacher2')?10.5:6.5;victim.vx+=dir.x*k;victim.vz+=dir.z*k;if(hasUpgrade('shotgun','breacher2'))victim.stagger=Math.max(victim.stagger||0,.38)}
  if(weapon==='sniper'&&head&&hasUpgrade('sniper','critical')){burst(hp.x,hp.y,hp.z,'#ffffff',18,.55);shots.push({x:hp.x,y:hp.y,z:hp.z,dx:dir.x,dy:dir.y,dz:dir.z,life:.22,maxLife:.22,len:18,color:'#ffffff',width:6})}
  if(weapon==='burst'){if(hasUpgrade('burst','suppressor'))applySlow(victim,.16,.55);if(head&&hasUpgrade('burst','marksman')){damage(victim,18,shooter,true,hp,{weapon:'burst',armorMult:.7,bypassArmor:true});burst(hp.x,hp.y,hp.z,'#7cffd5',7,.25)}}
  if(weapon==='arc'){if(hasUpgrade('arc','capacitor'))applySlow(victim,.34,1.35);if(hasUpgrade('arc','storm')){victim.arcBuild=(victim.arcBuild||0)+1;if(victim.arcBuild>=3){victim.arcBuild=0;victim.stagger=Math.max(victim.stagger||0,victim.boss?.12:.48);burst(hp.x,hp.y,hp.z,'#65f7ff',victim.boss?8:18,.45)}}if(hasUpgrade('arc','stormAnchor')){victim.arcAnchorBuild=(victim.arcAnchorBuild||0)+1;if(victim.arcAnchorBuild>=4){victim.arcAnchorBuild=0;plantArcAnchor(shooter,victim)}}}
}
function shotgunTier3Cone(shooter,d){if(shooter!==player)return;const thunder=hasUpgrade('shotgun','thunderBreach'),dragon=hasUpgrade('shotgun','dragonShells');if(!thunder&&!dragon)return;const radius=9.5,damagedWalls=thunder?walls.filter(w=>w.active!==false&&w.builtBy&&(w.builtBy===shooter||sameTeam(w.builtBy,shooter))&&w.hp<w.maxHp&&Math.hypot(w.x-shooter.x,w.z-shooter.z)<8.5):[],breachWall=damagedWalls.sort((a,b)=>Math.hypot(a.x-shooter.x,a.z-shooter.z)-Math.hypot(b.x-shooter.x,b.z-shooter.z))[0],breachActive=!!breachWall;if(thunder&&!breachActive&&!dragon)return;addTier3Fx({type:'cone',x:shooter.x+d.x*.8,y:shooter.y-.12+d.y*.8,z:shooter.z+d.z*.8,dx:d.x,dy:d.y,dz:d.z,len:dragon?7.5:9.5,spread:dragon?.62:.55,color:dragon?'#ff7a2b':'#8ffaff',size:5,life:dragon?.20:.18});particles.push({kind:'ring',x:shooter.x+d.x*2.2,y:shooter.y-.75,z:shooter.z+d.z*2.2,vx:0,vy:0,vz:0,life:.16,maxLife:.16,c:thunder?'#ff9a3d':'#ff7a2b',size:28});if(breachWall){breachWall.thunderGuardTimer=Math.max(breachWall.thunderGuardTimer||0,1.4);breachWall.flash=.16}for(const t of targetsFor(shooter,true)){if(t.dead)continue;const dx=t.x-shooter.x,dz=t.z-shooter.z,dist=Math.hypot(dx,dz);if(dist<=.2||dist>radius||Math.abs(t.y-shooter.y)>4)continue;const facing=(dx*d.x+dz*d.z)/dist;if(facing<.62||!lineClear(shooter.x,shooter.y-.2,shooter.z,t.x,t.y-.7,t.z))continue;if(thunder&&breachActive){const wallThreat=nearActiveWall(t,4.8),resist=t.boss?.28:((t.alienScale||1)>1.3?.55:1),force=(wallThreat?20:12)*(1-dist/radius)*resist;t.vx+=dx/dist*force;t.vz+=dz/dist*force;t.stagger=Math.max(t.stagger||0,t.boss?.18:(wallThreat?.48:.30));if(t.alien&&!t.boss&&wallThreat){t.fireCd=Math.max(t.fireCd||0,.55);t.attackAnim=0}if(t.boss)interruptBoss(t,.18)}if(dragon&&dist<8){const close=1-dist/8;applyDragonBurn(t,8+24*close,1.25,shooter)}}}

function rayBlockersBetween(origin,dir,range){
  const blockers=[];
  for(const block of blocksAlongRay(origin,dir,range)){
    const hit=rayBox(origin,dir,block,range);
    if(hit)blockers.push({...hit,block});
  }
  blockers.sort((a,b)=>a.dist-b.dist);
  return blockers;
}
function tracerToHit(muzzle,origin,dir,distance){
  const dx=origin.x+dir.x*distance-muzzle.x,dy=origin.y+dir.y*distance-muzzle.y,dz=origin.z+dir.z*distance-muzzle.z,len=Math.hypot(dx,dy,dz),inv=1/(len||1);
  return {x:muzzle.x,y:muzzle.y,z:muzzle.z,dx:dx*inv,dy:dy*inv,dz:dz*inv,len};
}
function fireHitscan(shooter,isPlayer,w){
  const weapon=shooter.weapon,ult=ultimateActive(shooter,weapon),clipUlt=ultimateClipActive(shooter,weapon),pellets=ult?(weapon==='shotgun'?16:(weapon==='smg'?(clipUlt?3:7):(weapon==='burst'?(clipUlt?4:5):1))):(weapon==='burst'?3:(w.kind==='shotgun'?w.tracers:1)),spread=(ult?(weapon==='shotgun'?w.spread*.72:w.spread*.35):w.spread)*((isPlayer&&weapon==='assault'&&hasUpgrade('assault','stabilizer'))?.48:(isPlayer&&weapon==='burst'&&hasUpgrade('burst','marksman')?.62:1)),pierceMax=ult?(weapon==='sniper'?99:(weapon==='pistol'?4:0)):((isPlayer&&weapon==='sniper')?(hasUpgrade('sniper','rail2')?3:(hasUpgrade('sniper','rail')?1:0)):0),phaseRail=isPlayer&&weapon==='sniper'&&hasUpgrade('sniper','phaseRail'),burstTrack={target:null,hits:0,head:false};
  let shotHit=false;
  if(isPlayer&&weapon==='shotgun'){const coneDir=dirFrom(shooter,0);shotgunTier3Cone(shooter,coneDir)}
  for(let i=0;i<pellets;i++){
    const burstOffset=weapon==='burst'?(i-1)*.006:0,d=dirFrom(shooter,spread*(w.kind==='shotgun'?rand(.45,1.35):1)+Math.abs(burstOffset));
    if(weapon==='burst'){
      d.x+=Math.sin(shooter.yaw+Math.PI/2)*burstOffset;d.z+=Math.cos(shooter.yaw+Math.PI/2)*burstOffset;
      const dl=Math.hypot(d.x,d.y,d.z)||1;d.x/=dl;d.y/=dl;d.z/=dl;
    }
    const origin={...actorEyePosition(shooter),y:actorEyePosition(shooter).y-.05},muzzle=muzzlePoint(shooter,d),worldHit=rayWorld(origin,d,w.range),worldDist=worldHit?.dist??w.range,blockers=phaseRail?rayBlockersBetween(origin,d,w.range):[];
    if(i===0){addMuzzleFlash(shooter,isPlayer,d,ult?ultimateColor(weapon):w.color,ult?.42:.2);if(ult&&!clipUlt)addUltimateMuzzleVisual(shooter,weapon,d);}
    let hits=[];
    for(const t of targetsFor(shooter,isPlayer)){
      if(t.spawnGrace>0)continue;
      const res=rayEntity(origin,d,t,w.range,weapon==='arc'?.08:0);
      if(!res)continue;
      const blockersBefore=phaseRail?blockers.filter(b=>b.dist<res.dist-.04):[],phaseWall=blockersBefore[0]?.block?.structure,wallPenetrated=blockersBefore.length===1&&blockersBefore[0].block.kind==='wall'&&phaseWall?.builtBy&&(phaseWall.builtBy===shooter||sameTeam(phaseWall.builtBy,shooter));
      if(res.dist<worldDist||wallPenetrated)hits.push({t,dist:res.dist,head:res.head,wallsBefore:wallPenetrated?1:0,pt:{x:origin.x+d.x*res.dist,y:origin.y+d.y*res.dist,z:origin.z+d.z*res.dist}});
    }
    hits.sort((a,b)=>a.dist-b.dist);
    if(!pierceMax)hits=hits.slice(0,1);else hits=hits.slice(0,pierceMax+1);
    const firstDist=hits[0]?.dist??worldDist,tracer=tracerToHit(muzzle,origin,d,firstDist),traceLen=tracer.len,piercedWall=phaseRail&&hits.some(h=>h.wallsBefore>0);
    shots.push({...tracer,life:w.kind==='shotgun'?.18:(weapon==='burst'?.20:.15),maxLife:w.kind==='shotgun'?.18:(weapon==='burst'?.20:.15),color:piercedWall?'#dffcff':(ult?ultimateColor(weapon):w.color),width:ult?(weapon==='sniper'?10:(weapon==='arc'?9:6)):(w.kind==='shotgun'?2.1:(weapon==='sniper'?4.4:(weapon==='arc'?5.2:2.8)))});
    if(ult&&!clipUlt)addUltimateBeamVisual(weapon,muzzle.x,muzzle.y,muzzle.z,tracer.dx,tracer.dy,tracer.dz,traceLen);
    if(piercedWall&&worldHit){
      const entry={...worldHit,x:worldHit.x+worldHit.normal.x*.08,y:worldHit.y+worldHit.normal.y*.08,z:worldHit.z+worldHit.normal.z*.08};
      addImpact(entry,'#dffcff',.32);burst(entry.x,entry.y,entry.z,'#8feaff',12,.42);addTier3Fx({type:'railBeam',x:muzzle.x,y:muzzle.y,z:muzzle.z,dx:d.x,dy:d.y,dz:d.z,len:traceLen,color:'#dffcff',size:4,life:.22});
    }
    if(hits.length){
      shotHit=true;
      hits.forEach((h,idx)=>{
        let dmg=w.damage;
        if(ult){const clip=ultimateClipActive(shooter,weapon);dmg*=clip?(CLIP_ULTIMATE_WEAK_MULT[weapon]||1.2):(weapon==='pistol'?3.2:weapon==='assault'?1.85:weapon==='sniper'?1.45:weapon==='smg'?1.55:weapon==='shotgun'?1.55:weapon==='burst'?1.72:weapon==='arc'?1.9:1.6);}
        if(h.head&&weapon==='sniper')dmg=ult?999:w.head;
        if(isPlayer&&weapon==='burst'&&hasUpgrade('burst','tungsten'))dmg*=1.16;
        if(isPlayer&&weapon==='burst'&&h.head&&hasUpgrade('burst','marksman'))dmg*=1.28;
        if(isPlayer&&weapon==='arc'&&hasUpgrade('arc','capacitor'))dmg*=1.18;
        if(isPlayer&&weapon==='smg'&&hasUpgrade('smg','closeBurst')&&h.dist<10)dmg*=1.30;if(isPlayer&&weapon==='burst'){if(i===0)burstTrack.target=h.t;if(h.t===burstTrack.target)burstTrack.hits++;burstTrack.head=burstTrack.head||h.head;if(i===pellets-1&&burstTrack.hits===pellets&&hasUpgrade('burst','deadeye')){dmg*=burstTrack.head?1.62:1.38;addTier3Fx({type:'deadeye',x:h.pt.x,y:h.pt.y,z:h.pt.z,color:'#7cffd5',size:6,life:.30});tier3Cue('DEADEYE',2.2);}}
        const fall=clamp(1-(h.dist/w.range)*w.falloff,.18,1),enemyPierce=idx?(hasUpgrade('sniper','rail2')?.72:.48):1,wallPierce=h.wallsBefore>0?.5:1,armorMult=weapon==='shotgun'?(hasUpgrade('shotgun','armorBreaker')?2.2:(hasUpgrade('shotgun','breacher')?1.55:1.15)):(weapon==='sniper'?.65:(weapon==='arc'?1.45:1));
        const result=damage(h.t,dmg*fall*enemyPierce*wallPierce,shooter,h.head,h.pt,{weapon,armorMult,bypassArmor:(h.head&&weapon==='sniper')||ult});
        if(h.wallsBefore>0){h.t.phaseMarkedTimer=2;h.t.phaseMarkedMult=1.15;h.t.phaseMarkedOwner=shooter;burst(h.pt.x,h.pt.y,h.pt.z,'#ff4f6d',5,.24);}
        if(ult){
          burst(h.pt.x,h.pt.y,h.pt.z,ultimateColor(weapon),clipUlt?2:(weapon==='arc'?8:6),clipUlt?.12:.28);if(!clipUlt)addUltimateImpactVisual(weapon,h.pt.x,h.pt.y,h.pt.z,d,h.t);
          if(weapon==='assault'||weapon==='arc'){
            for(const extra of targetsFor(shooter,isPlayer).filter(t=>t!==h.t&&!t.dead&&hypot2(t.x-h.t.x,t.z-h.t.z)<13&&lineClear(h.pt.x,h.pt.y,h.pt.z,t.x,t.y-.7,t.z)).slice(0,weapon==='arc'?5:3)){
              shots.push({x:h.pt.x,y:h.pt.y,z:h.pt.z,dx:(extra.x-h.pt.x)/(hypot2(extra.x-h.pt.x,extra.z-h.pt.z)||1),dy:0,dz:(extra.z-h.pt.z)/(hypot2(extra.x-h.pt.x,extra.z-h.pt.z)||1),life:.24,maxLife:.24,len:hypot2(extra.x-h.pt.x,extra.z-h.pt.z),color:ultimateColor(weapon),width:5});
              damage(extra,dmg*(clipUlt?.32:.55),shooter,false,{x:extra.x,y:extra.y-.7,z:extra.z},{weapon,armorMult:1.5,bypassArmor:weapon==='arc'});applySlow(extra,clipUlt?.28:.42,1.4);
            }
          }
          if(weapon==='shotgun'||weapon==='smg')applySlow(h.t,.35,.8);
        }
        if(weapon==='arc'){
          applySlow(h.t,.18,.75);
          arcChainShot(shooter,h.t,dmg,isPlayer);
        }
        applyHitUpgrade(shooter,h.t,weapon,h.head,h.pt,d,h.dist,(result?.armorDamage||0)+(result?.hpDamage||0));
        if(result?.armorBroken&&shooter===player&&weapon==='shotgun'&&hasUpgrade('shotgun','shatter')){burst(h.pt.x,h.pt.y,h.pt.z,'#8ffaff',18,.5);for(const t of targetsFor(shooter,isPlayer))if(t!==h.t&&hypot2(t.x-h.t.x,t.z-h.t.z)<4.5)damage(t,16,shooter,false,{x:t.x,y:t.y-.7,z:t.z},{weapon:'shotgun',armorMult:1.5});}
      });
    }else if(worldHit){
      if(ult)addUltimateImpactVisual(weapon,worldHit.x+worldHit.normal.x*.08,worldHit.y+worldHit.normal.y*.08,worldHit.z+worldHit.normal.z*.08,d,null);
      addFloorDecal(worldHit,weapon==='shotgun'?'shotgun':(weapon==='arc'?'arc':'bullet'));
      damageStructure(worldHit.block.structure,w.damage*(weapon==='burst'?.62:1)*(ult?2.2:1),shooter,weapon);surfaceImpact(worldHit,weapon);
    }
  }
  if(isPlayer&&weapon==='assault'&&hasUpgrade('assault','momentumClip')&&!shotHit){shooter.assaultMomentum=Math.max(0,(shooter.assaultMomentum||0)-.22);shooter.momentumGrace=0}
  if(isPlayer&&weapon==='burst'&&burstTrack.hits===pellets){if(hasUpgrade('burst','suppressor'))applySlow(burstTrack.target,.20,.75);if(hasUpgrade('burst','tacticalRollout')){shooter.tacticalTimer=1.25;addTier3Fx({type:'dash',x:shooter.x,y:shooter.y-.9,z:shooter.z,dx:-Math.sin(shooter.yaw),dy:0,dz:-Math.cos(shooter.yaw),len:3.2,color:'#7cffd5',size:4,life:.22})}}
}


function fireHarpoon(shooter,isPlayer){
  const w=WEAPONS.harpoon,d=dirFrom(shooter,w.spread),m=muzzlePoint(shooter,d),ult=ultimateActive(shooter,'harpoon'),barbed=isPlayer&&hasUpgrade('harpoon','barbedTip'),quick=isPlayer&&hasUpgrade('harpoon','quickWinch'),tether=isPlayer&&hasUpgrade('harpoon','tetherTrap'),skyhook=isPlayer&&hasUpgrade('harpoon','skyhook'),breach=isPlayer&&hasUpgrade('harpoon','breachAnchor'),momentum=isPlayer&&hasUpgrade('harpoon','momentumLine');
  addMuzzleFlash(shooter,isPlayer,d,ult?ultimateColor('harpoon'):w.color,.28);if(ult)addUltimateMuzzleVisual(shooter,'harpoon',d);
  projectiles.push({kind:'harpoon',ultimate:ult,owner:shooter,...actorEyePosition(shooter),px:actorEyePosition(shooter).x,py:actorEyePosition(shooter).y,pz:actorEyePosition(shooter).z,vx:d.x*82,vy:d.y*82,vz:d.z*82,dx:d.x,dy:d.y,dz:d.z,life:w.range/82+.12,color:ult?ultimateColor('harpoon'):w.color,damage:w.damage*(barbed?1.2:1)*(ult?1.35:1),r:.22,barbed,quick,tether,skyhook,breach,momentum,pulse:0});
}
function friendlyHarpoonStructure(piece,owner){return !!(piece?.builtBy&&piece.active!==false&&(piece.builtBy===owner||sameTeam(piece.builtBy,owner)))}
function harpoonImpactStructure(ent){return [...new Set(blocksNear(ent.x,ent.z,(ent.r||.55)+1.5).map(block=>block.structure))].find(piece=>piece.active!==false&&Math.abs(ent.x-piece.x)<piece.w/2+(ent.r||.55)+.12&&Math.abs(ent.z-piece.z)<piece.d/2+(ent.r||.55)+.12&&Math.abs((ent.y-.7)-(piece.type==='platform'?piece.y+piece.h/2:piece.y))<piece.h/2+1.1)}
function latchHarpoonEnemy(p,target){
  const owner=p.owner,large=!!target.boss||(target.alienScale||1)>1.35||(target.r||0)>.78,attackingWall=enemyAttackingWall(target),hp={x:p.x,y:p.y,z:p.z};
  damage(target,p.damage,owner,false,hp,{weapon:'harpoon',armorMult:1.15});owner.harpoonTetherTimer=0.55;burst(p.x,p.y,p.z,p.color,p.barbed?11:7,.28);target.stagger=Math.max(target.stagger||0,large?.18:(p.barbed?.48:.34));
  owner.harpoonVisualAnchor={target,offset:{x:p.x-target.x,y:p.y-target.y,z:p.z-target.z},dx:p.dx,dy:p.dy,dz:p.dz,color:p.color,ultimate:p.ultimate,until:performance.now()/1000+Math.max(.55,WEAPONS.harpoon.pullDuration||1.3)};
  if(p.barbed){target.harpoonBleedTimer=Math.max(target.harpoonBleedTimer||0,1.5);target.harpoonBleedDps=Math.max(target.harpoonBleedDps||0,7);target.harpoonBleedOwner=owner;target.harpoonBleedTick=.25}
  if(large){if(target.boss&&interruptBoss(target,!isHumanActor(owner)?0.72:(p.ultimate?0.55:(p.breach&&attackingWall?0.48:0.24))))tier3Cue('ANCHOR INTERRUPT',2.1);const dx=target.x-owner.x,dz=target.z-owner.z,dist=Math.hypot(dx,dz)||1,tug=owner.onGround?2.2:5.5;owner.vx+=dx/dist*tug;owner.vz+=dz/dist*tug;return}
  const dx=owner.x-target.x,dy=(owner.y-.6)-(target.y-.7),dz=owner.z-target.z,dist=Math.hypot(dx,dy,dz)||1,force=WEAPONS.harpoon.pullStrength*(p.breach&&attackingWall?1.45:1)*(p.ultimate?1.18:1),pullImpulse=force*(WEAPONS.harpoon.pullVelocityMult||1);target.vx+=dx/dist*pullImpulse;target.vy+=dy/dist*pullImpulse*.32;target.vz+=dz/dist*pullImpulse;target.slowAmount=Math.max(target.slowAmount||0,.56);target.slowTimer=Math.max(target.slowTimer||0,.9);target.harpoonPull={owner,timer:p.breach&&attackingWall?Math.max(.9,WEAPONS.harpoon.pullDuration||.74):(WEAPONS.harpoon.pullDuration||.74),strength:force*(p.ultimate?1.45:1),impact:p.breach||p.ultimate,hit:false};
  if(attackingWall){target.fireCd=Math.max(target.fireCd||0,p.breach?.9:.45);target.attackAnim=0}
  if(p.tether)target.harpoonTether={x:target.x,y:target.y,z:target.z,r:4.2,timer:1.75,owner};
}
function latchHarpoonWorld(p,worldHit){
  const owner=p.owner,piece=worldHit.block?.structure,friendly=friendlyHarpoonStructure(piece,owner),normal=worldHit.normal||{x:0,y:0,z:0},stop=1.35;
  owner.harpoonTetherTimer=0.45;addImpact(worldHit,p.color,.25);burst(p.x,p.y,p.z,p.color,friendly?9:5,.25);particles.push({kind:'ring',x:p.x+normal.x*.05,y:p.y+normal.y*.05,z:p.z+normal.z*.05,vx:0,vy:0,vz:0,life:.18,maxLife:.18,c:p.color,size:friendly?14:9});
  owner.harpoonVisualAnchor={x:p.x,y:p.y,z:p.z,dx:p.dx,dy:p.dy,dz:p.dz,color:p.color,ultimate:p.ultimate,until:performance.now()/1000+.5};
  const tx=p.x+normal.x*stop,ty=p.y+normal.y*stop+PLAYER_H*.2,tz=p.z+normal.z*stop,dx=tx-owner.x,dy=ty-(owner.y-.5),dz=tz-owner.z,dist=Math.hypot(dx,dy,dz)||1,strength=friendly?WEAPONS.harpoon.wallPullStrength*(p.quick?1.25:1):10;
  owner.harpoonGrapple={x:tx,y:ty,z:tz,timer:friendly?.48:.22,strength,friendly,skyhook:p.skyhook,momentum:p.momentum,stop:1.2};
  if(friendly&&p.skyhook){owner.vy=Math.max(owner.vy,7.5);owner.onGround=false}
}
function updateHarpoonProjectile(p,dt){
  if(!p.owner||p.owner.dead){p.life=0;return}p.life-=dt;p.pulse=(p.pulse||0)+dt;const ox=p.x,oy=p.y,oz=p.z,step=Math.hypot(p.vx,p.vy,p.vz)*dt,d={x:p.dx,y:p.dy,z:p.dz},worldHit=rayWorld({x:ox,y:oy,z:oz},d,step,true,p.r);let target=null,hitDist=worldHit?.dist??step;
  for(const t of targetsFor(p.owner,isHumanActor(p.owner))){if(t.dead||t.spawnGrace>0)continue;const hit=rayEntity({x:ox,y:oy,z:oz},d,t,step,p.r);if(hit&&hit.dist<hitDist){target=t;hitDist=hit.dist}}
  p.px=ox;p.py=oy;p.pz=oz;p.x=ox+d.x*hitDist;p.y=oy+d.y*hitDist;p.z=oz+d.z*hitDist;
  if(target){latchHarpoonEnemy(p,target);p.life=0}else if(worldHit){latchHarpoonWorld(p,worldHit);p.life=0}
}
function firePunchGun(shooter,isPlayer){
  const w=WEAPONS.punch,d=dirFrom(shooter,w.spread),ult=ultimateActive(shooter,'punch'),m=muzzlePoint(shooter,d),heavy=isPlayer&&hasUpgrade('punch','heavyKnuckles'),longer=isPlayer&&hasUpgrade('punch','longerChain'),wallBreaker=isPlayer&&hasUpgrade('punch','wallBreaker'),snapback=isPlayer&&hasUpgrade('punch','snapbackChain'),seismic=isPlayer&&hasUpgrade('punch','seismicPunch'),grapple=isPlayer&&hasUpgrade('punch','grappleImpact'),damage=w.damage*(heavy?1.25:1)*(ult?1.85:1),knock=w.knockback*(heavy?1.12:1)*(grapple?1.28:1)*(ult?1.45:1);
  addMuzzleFlash(shooter,isPlayer,d,ult?ultimateColor('punch'):w.color,.35);if(ult)addUltimateMuzzleVisual(shooter,'punch',d);
  projectiles.push({kind:'punch',owner:shooter,...actorEyePosition(shooter),px:actorEyePosition(shooter).x,py:actorEyePosition(shooter).y,pz:actorEyePosition(shooter).z,vx:d.x*54,vy:d.y*54,vz:d.z*54,dx:d.x,dy:d.y,dz:d.z,life:1.8,color:ult?ultimateColor('punch'):w.color,damage,knock,wallDamage:w.wallDamage*(heavy?1.12:1)*(wallBreaker?2.05:1),range:w.range*(longer?1.32:1)*(ult?1.22:1),traveled:0,r:ult?1.05:.82,state:'out',hit:false,pulse:0,ultimate:ult,snapback,seismic,grapple,wallBreaker});
}
function punchSeismicShockwave(p,directTarget=null,directStructure=null){
  const radius=5.25,baseDamage=32;emitExplosion('seismic',p,p.x,p.y,p.z,radius);
  particles.push({kind:'ring',x:p.x,y:p.y,z:p.z,vx:0,vy:0,vz:0,life:.28,maxLife:.28,c:p.color,size:34});
  burst(p.x,p.y,p.z,p.color,6,.24);
  for(const t of targetsFor(p.owner,isHumanActor(p.owner))){if(t===directTarget||t.dead||t.spawnGrace>0)continue;const dist=Math.hypot(t.x-p.x,(t.y-.7)-p.y,t.z-p.z);if(dist>=radius)continue;damage(t,baseDamage*(1-dist/radius),p.owner,false,{x:t.x,y:t.y-.7,z:t.z},{weapon:'punch',armorMult:1.2});}
  for(const piece of [...walls,...platforms]){if(piece===directStructure||piece.active===false||!piece.destructible)continue;const py=piece.type==='platform'?piece.y+piece.h/2:piece.y,dx=Math.max(0,Math.abs(p.x-piece.x)-piece.w/2),dy=Math.max(0,Math.abs(p.y-py)-piece.h/2),dz=Math.max(0,Math.abs(p.z-piece.z)-piece.d/2),dist=Math.hypot(dx,dy,dz);if(dist<radius)damageStructure(piece,42*(p.wallBreaker?1.4:1)*(1-dist/radius),p.owner,'punch');}
}
function updatePunchProjectile(p,dt){
  if(!p.owner||p.owner.dead){p.life=0;return;}
  const d={x:p.dx,y:p.dy,z:p.dz},m=muzzlePoint(p.owner,d),speedOut=68,speedBack=p.snapback?128:92;
  p.pulse=(p.pulse||0)+dt;p.life-=dt;
  if(p.state==='out'){
    const step=speedOut*dt,origin={x:p.x,y:p.y,z:p.z};let targetHit=null,targetDist=step,worldHit=rayWorld(origin,d,step,true,p.r*.45);
    if(worldHit)targetDist=worldHit.dist;
    for(const t of targetsFor(p.owner,isHumanActor(p.owner))){if(t.dead||t.spawnGrace>0)continue;const hit=rayEntity(origin,d,t,step,p.r*.72);if(hit&&hit.dist<targetDist){targetHit=t;targetDist=hit.dist;}}
    p.x=origin.x+d.x*targetDist;p.y=origin.y+d.y*targetDist;p.z=origin.z+d.z*targetDist;p.traveled=(p.traveled||0)+targetDist;
    if(targetHit){
      const hp={x:p.x,y:p.y,z:p.z};damage(targetHit,p.damage,p.owner,false,hp,{weapon:'punch',armorMult:1.45,bypassArmor:p.ultimate});
      if(p.grapple){addTier3Fx({type:'grapple',x:p.owner.x,y:p.owner.y-.55,z:p.owner.z,tx:targetHit.x,ty:targetHit.y-.7,tz:targetHit.z,color:'#ff9ee5',size:4,life:.22});const pullX=p.owner.x-targetHit.x,pullZ=p.owner.z-targetHit.z,pullDist=Math.hypot(pullX,pullZ)||1,resist=targetHit.boss?.22:1;targetHit.vx+=pullX/pullDist*6.5*resist;targetHit.vz+=pullZ/pullDist*6.5*resist;targetHit.grappleLaunch={timer:.09,dx:d.x,dz:d.z,force:p.knock*(targetHit.boss?.28:1),owner:p.owner};if(targetHit.boss&&interruptBoss(targetHit,.32))tier3Cue('BOSS INTERRUPTED',2.2)}else{targetHit.vx+=d.x*p.knock;targetHit.vy+=Math.max(3.5,d.y*p.knock*.5+5.5);targetHit.vz+=d.z*p.knock;}targetHit.stagger=Math.max(targetHit.stagger||0,p.grapple?(targetHit.boss?.18:.44):.34);
      burst(p.x,p.y,p.z,p.color,18,.48);if(p.seismic)punchSeismicShockwave(p,targetHit);if(p.ultimate)addUltimateImpactVisual('punch',p.x,p.y,p.z,d,targetHit);p.state='back';p.hit=true;
    }else if(worldHit){
      addFloorDecal(worldHit,'punch');damageStructure(worldHit.block.structure,p.wallDamage,p.owner,'punch',worldHit);burst(p.x,p.y,p.z,p.color,12,.36);if(p.seismic)punchSeismicShockwave(p,null,worldHit.block.structure);p.state='back';p.hit=true;
    }else if(p.traveled>=p.range){p.state='back';}
  }else{
    const dx=m.x-p.x,dy=m.y-p.y,dz=m.z-p.z,dist=Math.hypot(dx,dy,dz);
    if(dist<.55){p.life=0;return;}
    const step=Math.min(dist,speedBack*dt),inv=1/(dist||1);p.x+=dx*inv*step;p.y+=dy*inv*step;p.z+=dz*inv*step;
  }
}

function firePrismSplit(origin,dir,remaining,shooter,isPlayer,damageScale,color){
  addTier3Fx({type:'prism',x:origin.x,y:origin.y,z:origin.z,dx:dir.x,dy:dir.y,dz:dir.z,color:'#ff8cff',size:5,life:.20});
  for(const angle of [-.095,.095].slice(0,MAX_PRISM_SPLITS)){const ca=Math.cos(angle),sa=Math.sin(angle),splitDir={x:dir.x*ca-dir.z*sa,y:dir.y,z:dir.x*sa+dir.z*ca},worldHit=rayWorld(origin,splitDir,remaining),worldDist=worldHit?.dist??remaining;let hit=null,hitDist=worldDist,hitPoint=null;for(const t of targetsFor(shooter,isPlayer)){if(t.spawnGrace>0)continue;const res=rayEntity(origin,splitDir,t,remaining);if(res&&res.dist<hitDist){hit=t;hitDist=res.dist;hitPoint={x:origin.x+splitDir.x*res.dist,y:origin.y+splitDir.y*res.dist,z:origin.z+splitDir.z*res.dist}}}lasers.push({...tracerToHit(origin,origin,splitDir,hitDist),life:.10,maxLife:.10,color,width:4});if(hit)damage(hit,WEAPONS.laser.damage*damageScale,shooter,false,hitPoint,{weapon:'laser',armorMult:1.25});else if(worldHit){addFloorDecal(worldHit,'laser');damageStructure(worldHit.block.structure,WEAPONS.laser.damage*damageScale,shooter,'laser',worldHit)}}
}
function fireLaserBeam(shooter,isPlayer){
  const w=WEAPONS.laser,ult=ultimateActive(shooter,'laser');if(shooter.dead||shooter.reload>0)return;if(!ult&&(shooter.heat||0)>.96){shooter.reload=1.35;shooter.reloadTotal=1.35;shooter.reloadAnimType='laser';shooter.heat=.82;if(isPlayer)msg('LASER OVERHEATED — COOLING');return}
  const heatScale=isPlayer&&hasUpgrade('laser','heatSink')?.72:1,cutting=isPlayer&&hasUpgrade('laser','cuttingBeam'),prism=isPlayer&&hasUpgrade('laser','prismSplitter');shooter.cool=ult?w.cool*1.8:w.cool;shooter.heat=ult?Math.max(0,(shooter.heat||0)-.35):clamp((shooter.heat||0)+w.heatRate*w.cool*heatScale,0,1.05);const initial=dirFrom(shooter,w.spread);addMuzzleFlash(shooter,isPlayer,initial,ult?ultimateColor('laser'):w.color,ult?.35:1);if(ult&&!ultimateClipActive(shooter,'laser'))addUltimateMuzzleVisual(shooter,'laser',initial);let origin={...actorEyePosition(shooter),y:actorEyePosition(shooter).y-.05},muzzle=muzzlePoint(shooter,initial),dir=initial,remaining=w.range,damageScale=1,bounces=isPlayer?(hasUpgrade('laser','reflector2')?2:(hasUpgrade('laser','reflector')?1:0)):0,hitThisCycle=false,splitDone=false;
  for(let segment=0;segment<=bounces&&remaining>2;segment++){const worldHit=rayWorld(origin,dir,remaining),worldDist=worldHit?.dist??remaining;let hit=null,hitDist=worldDist,hitPoint=null;for(const t of targetsFor(shooter,isPlayer)){if(t.spawnGrace>0)continue;const res=rayEntity(origin,dir,t,remaining);if(res&&res.dist<hitDist){hit=t;hitDist=res.dist;hitPoint={x:origin.x+dir.x*res.dist,y:origin.y+dir.y*res.dist,z:origin.z+dir.z*res.dist}}}lasers.push({...tracerToHit(segment?origin:muzzle,origin,dir,hitDist),life:.11,maxLife:.11,color:ult?ultimateColor('laser'):w.color,width:ult?16:(isPlayer?(cutting?7+Math.min(3.5,(shooter.focusTime||0)*3.2):7):4)});if(ult&&!ultimateClipActive(shooter,'laser'))addUltimateBeamVisual('laser',segment?origin.x:muzzle.x,segment?origin.y:muzzle.y,segment?origin.z:muzzle.z,dir.x,dir.y,dir.z,Math.max(.05,hitDist-(segment?0:.48)));if(hit){hitThisCycle=true;let focus=1;if(isPlayer&&hasUpgrade('laser','focus')){if(shooter.focusTarget===hit)shooter.focusTime=(shooter.focusTime||0)+w.cool;else{shooter.focusTarget=hit;shooter.focusTime=0}if(cutting&&(shooter.focusTime||0)>=.45)tier3Cue('CUTTING BEAM',2.8);focus=1+Math.min(cutting?1.35:.75,(shooter.focusTime||0)*(cutting?1.75:1.4))}const fall=clamp(1-(hitDist/w.range)*w.falloff,.35,1),armored=(hit.armor||0)>0,bossScale=cutting&&(hit.boss||armored)?1.10+Math.min(.32,(shooter.focusTime||0)*.28):1;if(cutting&&enemyAttackingWall(hit))hit.wallDamageCutTimer=Math.max(hit.wallDamageCutTimer||0,1.1);damage(hit,w.damage*fall*damageScale*focus*bossScale*(ult?(ultimateClipActive(shooter,'laser')?CLIP_ULTIMATE_WEAK_MULT.laser:4.2):1),shooter,false,hitPoint,{weapon:'laser',armorMult:ult?(ultimateClipActive(shooter,'laser')?1.55:2.2):(cutting?(hit.boss||armored?2.75:2.2):1.35),bypassArmor:ult});if(ult){burst(hitPoint.x,hitPoint.y,hitPoint.z,ultimateColor('laser'),ultimateClipActive(shooter,'laser')?2:8,ultimateClipActive(shooter,'laser')?.16:.55);if(!ultimateClipActive(shooter,'laser'))addUltimateImpactVisual('laser',hitPoint.x,hitPoint.y,hitPoint.z,dir,hit);for(const extra of targetsFor(shooter,isPlayer).filter(t=>t!==hit&&!t.dead&&hypot2(t.x-hit.x,t.z-hit.z)<10&&lineClear(hitPoint.x,hitPoint.y,hitPoint.z,t.x,t.y-.7,t.z)).slice(0,4))damage(extra,w.damage*(ultimateClipActive(shooter,'laser')?1.05:2.2),shooter,false,{x:extra.x,y:extra.y-.7,z:extra.z},{weapon:'laser',armorMult:1.4})}break}if(!worldHit)break;addFloorDecal(worldHit,'laser');damageStructure(worldHit.block.structure,w.damage*damageScale,shooter,'laser',worldHit);if(segment>=bounces)break;remaining-=worldHit.dist;const dot=dir.x*worldHit.normal.x+dir.y*worldHit.normal.y+dir.z*worldHit.normal.z;dir={x:dir.x-2*dot*worldHit.normal.x,y:dir.y-2*dot*worldHit.normal.y,z:dir.z-2*dot*worldHit.normal.z};origin={x:worldHit.x+dir.x*.06,y:worldHit.y+dir.y*.06,z:worldHit.z+dir.z*.06};damageScale*=hasUpgrade('laser','reflector2')?.82:.62;remaining*=.72;if(prism&&!splitDone){splitDone=true;firePrismSplit(origin,dir,remaining,shooter,isPlayer,damageScale*.42,ult?ultimateColor('laser'):w.color)}}
  if(isPlayer&&hasUpgrade('laser','focus')&&!hitThisCycle){shooter.focusTime=Math.max(0,(shooter.focusTime||0)-w.cool*(cutting?4:2));if(shooter.focusTime<=0)shooter.focusTarget=null}
}
function friendlyMenderStructure(piece,shooter){return !!(piece?.builtBy&&piece.active!==false&&(piece.builtBy===shooter||sameTeam(piece.builtBy,shooter)))}
function menderStructurePoint(piece){return {x:piece.x,y:piece.type==='platform'?piece.y+piece.h:piece.y,z:piece.z}}
function menderCanSupportStructure(piece,shooter,armorBoost=false){return friendlyMenderStructure(piece,shooter)&&(piece.hp<piece.maxHp||(armorBoost&&piece.hp>=piece.maxHp&&(piece.bonusHp||0)<(piece.maxBonusHp||500)))}
const MENDER_TARGET_COLORS={repair:'#20f6ff',boost:'#9eeeff',heal:'#43ffd5',enemy:'#e7fbff',invalid:'#39758d'};
function menderAimInfo(shooter){
  const w=WEAPONS.mender,dir=dirFrom(shooter,0),origin={...actorEyePosition(shooter),y:actorEyePosition(shooter).y-.05},worldHit=rayWorld(origin,dir,w.range),worldDist=worldHit?.dist??w.range;
  let friendly=null,friendlyDist=worldDist,enemy=null,enemyDist=worldDist;
  for(const target of [player,player2,...bots]){if(!target||target===shooter||target.dead||target.spawnGrace>0)continue;const hit=rayEntity(origin,dir,target,w.range,.06);if(!hit||hit.dist>=worldDist)continue;if(isHumanActor(target)&&sameTeam(shooter,target)){if(hit.dist<friendlyDist){friendly=target;friendlyDist=hit.dist}}else if(!sameTeam(shooter,target)&&hit.dist<enemyDist){enemy=target;enemyDist=hit.dist}}
  const structure=worldHit?.block?.structure,armorBoost=shooter===player&&hasUpgrade('mender','armorBoost'),damagedStructure=friendlyMenderStructure(structure,shooter)&&structure.hp<structure.maxHp,boostableStructure=friendlyMenderStructure(structure,shooter)&&armorBoost&&structure.hp>=structure.maxHp&&(structure.bonusHp||0)<(structure.maxBonusHp||500);
  if((damagedStructure||boostableStructure)&&!friendly&&!enemy)return {kind:damagedStructure?'repair':'boost',color:MENDER_TARGET_COLORS[damagedStructure?'repair':'boost'],dir,origin,worldHit,worldDist,structure,armorBoost};
  if(friendly&&friendlyDist<=enemyDist&&friendly.hp<friendly.maxHp)return {kind:'heal',color:MENDER_TARGET_COLORS.heal,dir,origin,worldHit,worldDist,target:friendly,targetDist:friendlyDist,armorBoost};
  if(enemy&&enemyDist<friendlyDist)return {kind:'enemy',color:MENDER_TARGET_COLORS.enemy,dir,origin,worldHit,worldDist,target:enemy,targetDist:enemyDist,armorBoost};
  return {kind:'invalid',color:MENDER_TARGET_COLORS.invalid,dir,origin,worldHit,worldDist,armorBoost};
}
function menderHeatStatus(ent){const heat=clamp(ent.heat||0,0,1);if(ent.menderOverheated)return heat>=.90?'OVERHEATED':'COOLING';return heat<=.005?'READY':'HEAT '+Math.round(heat*100)+'%'}

function menderSupportFx(kind,x,y,z,color,bright=false){
  if(visualQuality==='low'&&Math.random()<(bright?.5:.7))return;
  if(kind==='repair'){
    const count=bright?3:2;
    for(let i=0;i<count;i++)particles.push({kind:'spark',x:x+rand(-.12,.12),y:y+rand(-.12,.12),z:z+rand(-.12,.12),vx:rand(-.8,.8),vy:rand(.4,bright?2.25:1.8),vz:rand(-.8,.8),life:rand(.10,bright?.27:.22),maxLife:bright?.27:.22,c:bright?'#c9ffff':color,size:bright?2.8:2.2});
    particles.push({kind:'ring',x,y,z,vx:0,vy:0,vz:0,life:.13,maxLife:.13,c:bright?'#b8ffff':color,size:bright?8.5:7});
  }else if(kind==='heal')particles.push({kind:'ring',x,y:y-.65,z,vx:0,vy:0,vz:0,life:.16,maxLife:.16,c:'#8fffd1',size:9});
  else particles.push({kind:'spark',x,y,z,vx:rand(-.5,.5),vy:rand(.2,1.1),vz:rand(-.5,.5),life:.12,maxLife:.12,c:color,size:2});
  trimVisualArray(particles,visualPreset().maxParticles);
}
function applyMenderStructureSupport(piece,shooter,scale=1){
  const w=WEAPONS.mender,patch=shooter===player&&hasUpgrade('mender','patchCurrent'),fortify=shooter===player&&hasUpgrade('mender','fortifySeal'),armorBoost=shooter===player&&hasUpgrade('mender','armorBoost');
  if(!menderCanSupportStructure(piece,shooter,armorBoost))return false;
  if(piece.hp<piece.maxHp){const before=piece.hp,rate=(patch?w.patchRepairRate:w.repairRate)*scale,repair=Math.min(piece.maxHp-before,rate*w.cool);piece.hp+=repair;piece.acidDamage=Math.max(0,(piece.acidDamage||0)-repair);worldGeometryDirty=true;if((piece.breachMarked||0)>0){piece.breachMarked=piece.hp>=piece.maxHp?0:Math.max(0,piece.breachMarked-w.cool*1.5*scale);if(piece.breachMarked<=0)tier3Cue('BREACH MARK CLEARED',2.2)}if(before<piece.maxHp&&piece.hp>=piece.maxHp&&fortify){piece.fortifySealTimer=8;piece.fortifySealMax=8;}}
  else if(armorBoost){piece.maxBonusHp=500;const rate=(patch?58:45)*scale;piece.bonusHp=Math.min(piece.maxBonusHp,(piece.bonusHp||0)+rate*w.cool);piece.armorBoosted=piece.bonusHp>0;worldGeometryDirty=true;if((piece.breachMarked||0)>0){piece.breachMarked=Math.max(0,piece.breachMarked-w.cool*2.5*scale);if(piece.breachMarked<=0)tier3Cue('BREACH MARK CLEARED',2.2)}}
  piece.flash=.08;return true;
}
function applyMenderActorSupport(target,shooter,scale=1){
  const w=WEAPONS.mender,medical=shooter===player&&hasUpgrade('mender','medicalLens'),triage=shooter===player&&hasUpgrade('mender','triageLink'),beforeHp=target.hp,critical=beforeHp/target.maxHp<.4;
  const healRate=(triage&&critical?w.triageHealRate:(medical?w.medicalHealRate:w.healRate))*scale;
  target.hp=Math.min(target.maxHp,target.hp+healRate*w.cool);
  if(medical&&(target.armor||0)<(target.maxArmor||100))target.armor=Math.min(target.maxArmor||100,(target.armor||0)+w.armorHealRate*.45*scale*w.cool);
  if(triage&&beforeHp<target.maxHp&&target.hp>=target.maxHp){target.menderSpeedBoost=2;target.menderSpeedBoostMax=2;if(target===player)msg('TRIAGE SPEED BOOST');}
  return target.hp>beforeHp||(medical&&(target.armor||0)<(target.maxArmor||100));
}
function guardianMenderTarget(shooter,primary,from,armorBoost){
  let best=null,bestDist=9.5;
  for(const piece of [...walls,...platforms]){if(piece===primary||!menderCanSupportStructure(piece,shooter,armorBoost))continue;const point=menderStructurePoint(piece),dist=Math.hypot(point.x-from.x,point.y-from.y,point.z-from.z);if(dist<bestDist&&lineClear(from.x,from.y,from.z,point.x,point.y,point.z)){best={kind:'repair',target:piece,point};bestDist=dist}}
  for(const target of [player,player2,...bots]){if(!target||target===shooter||target===primary||target.dead||!isHumanActor(target)||!sameTeam(shooter,target)||target.hp>=target.maxHp)continue;const point={x:target.x,y:target.y-.7,z:target.z},dist=Math.hypot(point.x-from.x,point.y-from.y,point.z-from.z);if(dist<bestDist&&lineClear(from.x,from.y,from.z,point.x,point.y,point.z)){best={kind:'heal',target,point};bestDist=dist}}
  return best;
}
function fireMenderBeam(shooter,isPlayer){
  const w=WEAPONS.mender;if(shooter.dead||shooter.reload>0||shooter.menderOverheated)return false;
  if((shooter.heat||0)>=1){shooter.heat=1;shooter.menderOverheated=true;if(isPlayer)msg('MENDER OVERHEATED — COOLING');saveActiveWeaponState(shooter);return false}
  shooter.cool=w.cool;shooter.heat=clamp((shooter.heat||0)+(w.heatRate+w.coolRate)*w.cool,0,1);
  const aim=menderAimInfo(shooter),{dir,origin,worldHit,worldDist,armorBoost}=aim,muzzle=muzzlePoint(shooter,dir);
  let targetKind='none',primary=null,hitDist=worldDist,hitPoint=worldHit?{x:worldHit.x,y:worldHit.y,z:worldHit.z}:null;
  if(aim.kind==='repair'||aim.kind==='boost'){applyMenderStructureSupport(aim.structure,shooter);primary=aim.structure;targetKind='repair'}
  else if(aim.kind==='heal'){hitDist=aim.targetDist;hitPoint={x:origin.x+dir.x*hitDist,y:origin.y+dir.y*hitDist,z:origin.z+dir.z*hitDist};applyMenderActorSupport(aim.target,shooter);primary=aim.target;targetKind='heal'}
  else if(aim.kind==='enemy'){hitDist=aim.targetDist;hitPoint={x:origin.x+dir.x*hitDist,y:origin.y+dir.y*hitDist,z:origin.z+dir.z*hitDist};damage(aim.target,w.damage,shooter,false,hitPoint,{weapon:'mender',armorMult:.65,noUltimate:true,minimalFx:true});primary=aim.target;targetKind='damage'}
  lasers.push({x:muzzle.x,y:muzzle.y,z:muzzle.z,dx:dir.x,dy:dir.y,dz:dir.z,life:.075,maxLife:.075,len:Math.max(.05,hitDist-.48),color:aim.color,width:1.45,support:true,targetKind});
  const guardian=shooter===player&&hasUpgrade('mender','guardianNetwork');
  if(guardian&&primary&&targetKind!=='damage'&&hitPoint){const chain=guardianMenderTarget(shooter,primary,hitPoint,armorBoost);if(chain){if(chain.kind==='repair')applyMenderStructureSupport(chain.target,shooter,.7);else applyMenderActorSupport(chain.target,shooter,.7);const dx=chain.point.x-hitPoint.x,dy=chain.point.y-hitPoint.y,dz=chain.point.z-hitPoint.z,len=Math.hypot(dx,dy,dz)||1;lasers.push({x:hitPoint.x,y:hitPoint.y,z:hitPoint.z,dx:dx/len,dy:dy/len,dz:dz/len,life:.075,maxLife:.075,len,color:'#bffcff',width:.8,support:true,targetKind:chain.kind});}}
  shooter.menderFxCd=Math.max(0,(shooter.menderFxCd||0)-w.cool);if(targetKind!=='none'&&hitPoint&&shooter.menderFxCd<=0){menderSupportFx(targetKind,hitPoint.x,hitPoint.y,hitPoint.z,aim.color,shooter===player&&hasUpgrade('mender','patchCurrent')&&targetKind==='repair');shooter.menderFxCd=.12}
  if(shooter.heat>=1){shooter.heat=1;shooter.menderOverheated=true;if(isPlayer)msg('MENDER OVERHEATED — COOLING');saveActiveWeaponState(shooter)}
  return true;
}
const explosionSystem=window.NeonExplosionFX?.createSystem()||null;
let explosionRenderer=null;
function emitExplosion(kind,p,x,y,z,radius){
  if(typeof tacticalController!=='undefined')tacticalController?.hear({kind:'explosion',x,y,z,team:p?.owner?.team,source:p?.owner?.entityId,range:80});
  if(!explosionSystem)return;const bounds={minX:x-radius,maxX:x+radius,minY:y-radius,maxY:y+radius,minZ:z-radius,maxZ:z+radius};
  for(const [axis,sign,key]of [['x',1,'maxX'],['x',-1,'minX'],['y',1,'maxY'],['y',-1,'minY'],['z',1,'maxZ'],['z',-1,'minZ']]){const d={x:0,y:0,z:0};d[axis]=sign;const hit=rayWorld({x,y,z},d,radius,true);if(hit)bounds[key]=({x,y,z})[axis]+sign*Math.max(.015,hit.dist-.02);}
  explosionSystem.emit(kind,x,y,z,{radius,bounds});combatAudio('blast:'+kind,x,y,z);
}
function renderExplosionEffects(){
  if(!explosionSystem?.active.length)return;explosionRenderer ||= window.NeonExplosionFX.createRenderer(gl,explosionSystem);explosionRenderer.render(importedModelCamera(),visualQuality);
}
function blastCanReach(x,y,z,target){
  const feet=target.y-PLAYER_H,body=target.alien?alienTargetY(target,target.y-.8):humanTargetY(target,'body');
  return lineClear(x,y,z,target.x,body,target.z)||lineClear(x,y,z,target.x,feet+.3,target.z);
}
function plasmaExplosion(p,x,y,z,world=false){let radius=3.8+(p.tier||1)*.8;if(p.supernova)radius*=1.45;if(p.singularity)radius*=1.35;emitExplosion('plasma',p,x,y,z,radius);if(p.ultimate)addUltimateImpactVisual('plasma',x,y,z,null,null);for(const t of targetsFor(p.owner,p.owner===player)){if(t.dead||t.spawnGrace>0)continue;const dx=x-t.x,dy=y-(t.y-.8),dz=z-t.z,dist=Math.hypot(dx,dy,dz);if(dist>radius||!blastCanReach(x,y,z,t))continue;const scale=1-dist/radius;if(p.singularity){const resist=t.boss?.22:(((t.alienScale||1)>1.3||(t.r||0)>.75)?.5:1);t.vx+=dx/(dist||1)*10*scale*resist;t.vz+=dz/(dist||1)*10*scale*resist}const coreBonus=p.eventHorizon&&dist<radius*.38?1.35:1;damage(t,(p.damage*(p.supernova?.48:.34))*scale*coreBonus,p.owner,false,{x:t.x,y:t.y-.7,z:t.z},{weapon:'plasma',armorMult:1.28})}damageStructuresInRadius(x,y,z,radius,p.damage*.45,p.owner,'plasma');if(p.eventHorizon){const wall=[...walls,...platforms].find(w=>w.active!==false&&w.destructible&&w.hp<w.maxHp&&Math.hypot(w.x-x,w.z-z)<radius+3);if(wall){const y0=topAtPoint(x,z)+.05;plasmaResidues.push({x,z,y:y0,r:4.2,life:4,maxLife:4,owner:p.owner});if(plasmaResidues.length>6)plasmaResidues.shift();addTier3Fx({type:'gravityRing',x,y:y0,z,color:'#bd65ff',size:8,life:.32})}}if(world)burst(x,y,z,'#ffffff',8,.35)}
function firePlasma(shooter,isPlayer,charge=.08){const w=WEAPONS.plasma,d=dirFrom(shooter,w.spread),ult=ultimateActive(shooter,'plasma');if(ult)charge=1;const tier=chargeTier(charge),larger=isPlayer&&hasUpgrade('plasma','larger'),gravity=isPlayer&&hasUpgrade('plasma','gravity'),singularity=isPlayer&&hasUpgrade('plasma','singularity')&&tier>=4,supernova=(isPlayer&&hasUpgrade('plasma','supernova'))||ult,eventHorizon=isPlayer&&hasUpgrade('plasma','eventHorizon')&&tier>=4,plasmaMine=isPlayer&&hasUpgrade('plasma','plasmaMine')&&tier>=4,size=[0,.34,.48,.66,.9][tier]*(larger?1.32:1),speed=(ult?38:[0,58,55,51,46][tier]),color=ult?ultimateColor('plasma'):chargeColor(tier),damage=(w.damage+[0,0,48,108,204][tier])*(supernova?1.12:1)*(ult?1.55:1);if(plasmaMine){const active=projectiles.filter(p=>p.owner===shooter&&p.plasmaMine&&!p.exploded);if(active.length>=MAX_PLASMA_MINES){const oldest=active[0];plasmaExplosion(oldest,oldest.x,oldest.y,oldest.z);oldest.exploded=true}}addMuzzleFlash(shooter,isPlayer,d,color,charge);if(ult)addUltimateMuzzleVisual(shooter,'plasma',d);if(eventHorizon)addTier3Fx({type:'spiral',x:shooter.x+d.x*1.2,y:shooter.y-.05+d.y*1.2,z:shooter.z+d.z*1.2,color:'#d67cff',size:6,life:.34});projectiles.push({ultimate:ult,owner:shooter,...actorEyePosition(shooter),px:actorEyePosition(shooter).x,py:actorEyePosition(shooter).y,pz:actorEyePosition(shooter).z,vx:d.x*speed,vy:d.y*speed,vz:d.z*speed,life:2.6,color,damage,r:ult?size*1.55:size,tier,gravity:gravity||ult,singularity:singularity||ult,supernova,eventHorizon,plasmaMine,pulse:0});if(eventHorizon)tier3Cue('EVENT HORIZON',3.2)}
function mortarTargetPoint(active){const angle=Math.atan2(active.dirX,active.dirZ)+(active.steerAngle||0),dirX=Math.sin(angle),dirZ=Math.cos(angle),x=clamp(active.x+dirX*active.travel,-ARENA_SAFE+1,ARENA_SAFE-1),z=clamp(active.z+dirZ*active.travel,-ARENA_SAFE+1,ARENA_SAFE-1);return {x,z,y:topAtPoint(x,z)+.08,dirX,dirZ}}
function beginMortar(shooter,isPlayer){const w=WEAPONS.mortar;if(shooter.mortarActive||shooter.dead||shooter.cool>0||shooter.reload>0||shooter.ammo<=0)return false;const faster=isPlayer&&hasUpgrade('mortar','fasterTargeting',shooter),bigger=isPlayer&&hasUpgrade('mortar','biggerPayload',shooter),ultimate=ultimateActive(shooter,'mortar'),color=ultimate?ultimateColor('mortar'):w.color,botPoint=!isHumanActor(shooter)?shooter.botMortarPoint:null,dx=botPoint?botPoint.x-shooter.x:Math.sin(shooter.yaw),dz=botPoint?botPoint.z-shooter.z:Math.cos(shooter.yaw),botDist=Math.hypot(dx,dz)||1,d={x:dx/botDist,y:0,z:dz/botDist};shooter.ammo--;shooter.cool=w.cool;shooter.mortarActive={x:shooter.x,z:shooter.z,dirX:d.x,dirZ:d.z,travel:Math.max(4,w.targetSpeed*w.minHold),holdTime:0,maxHold:w.maxHold+(faster?.5:0),radius:w.radius*(bigger?1.2:1)*(ultimate?1.35:1),damage:w.damage*(ultimate?1.8:1),targetSpeed:w.targetSpeed*(faster?1.25:1),rocketY:shooter.y-.1,steerAngle:0,color,ultimate,isPlayer,desiredTravel:botPoint?Math.min(w.range,botDist):null,botReleaseAt:null};if(botPoint){const a=shooter.mortarActive;a.botReleaseAt=clamp(.48+botDist*.006,.52,.9);a.travel=Math.max(4,a.desiredTravel-a.targetSpeed*a.botReleaseAt)}shooter.botMortarPoint=null;addMuzzleFlash(shooter,isPlayer,{x:0,y:1,z:0},color,.3);if(ultimate)addUltimateMuzzleVisual(shooter,'mortar',{x:0,y:1,z:0});burst(shooter.x,shooter.y-.1,shooter.z,color,ultimate?11:7,.28);if(ultimate)endUltimateShot(shooter);return true}
function updateMortarTarget(ent,dt,moveX=0,moveForward=0){const a=ent?.mortarActive;if(!a)return;a.holdTime+=dt;a.travel=Math.min(a.desiredTravel??WEAPONS.mortar.range,a.travel+a.targetSpeed*dt);a.rocketY=Math.min(ent.y+25,a.rocketY+31*dt);if(a.isPlayer&&hasUpgrade('mortar','guidedDrift',ent)){const original=Math.atan2(a.dirX,a.dirZ),current=ent.yaw||original,aimDelta=Math.atan2(Math.sin(current-original),Math.cos(current-original)),moveSteer=clamp(moveX,-1,1)*.18,target=clamp(aimDelta*.42+moveSteer,-25*Math.PI/180,25*Math.PI/180);a.steerAngle+=(target-a.steerAngle)*clamp(dt*3.5,0,1)}const target=mortarTargetPoint(a);a.targetX=target.x;a.targetY=target.y;a.targetZ=target.z;if(a.holdTime>=(a.botReleaseAt??a.maxHold))releaseMortar(ent)}
function meteorMortarTarget(owner,x,z,radius){const candidates=targetsFor(owner,owner===player).filter(t=>!t.dead&&t.spawnGrace<=0&&hypot2(t.x-x,t.z-z)<=radius+4.5);if(!candidates.length)return {x,z};let best=null,bestScore=-Infinity;for(const center of candidates){const dx=center.x-x,dz=center.z-z,dist=Math.hypot(dx,dz);if(dist>radius+4.5)continue;let score=center.boss?8:0;for(const t of candidates){const d=hypot2(t.x-center.x,t.z-center.z);if(d<5.2)score+=(t.boss?4:1)*(1-d/6)}if(enemyAttackingWall(center))score+=2.5;score-=dist*.08;if(score>bestScore){bestScore=score;best=center}}if(!best)return {x,z};const dx=best.x-x,dz=best.z-z,d=Math.hypot(dx,dz)||1,shift=Math.min(4.5,d);return {x:x+dx/d*shift,z:z+dz/d*shift}}
function releaseMortar(shooter){const a=shooter?.mortarActive;if(!a)return false;const w=WEAPONS.mortar,base=mortarTargetPoint(a),meteor=a.isPlayer&&hasUpgrade('mortar','meteorCommand',shooter),corrected=meteor?meteorMortarTarget(shooter,base.x,base.z,a.radius):base,impactY=topAtPoint(corrected.x,corrected.z)+.08;projectiles.push({kind:'mortarDrop',owner:shooter,x:base.x,y:Math.max(base.y+30,a.rocketY+7),z:base.z,targetX:corrected.x,targetZ:corrected.z,impactY,vx:0,vy:-64,vz:0,r:a.ultimate?.55:.42,radius:a.radius,damage:a.damage,life:1.2,color:a.color,pulse:0,ultimate:a.ultimate,bigger:a.isPlayer&&hasUpgrade('mortar','biggerPayload',shooter),bunker:a.isPlayer&&hasUpgrade('mortar','bunkerBuster',shooter),breach:a.isPlayer&&hasUpgrade('mortar','breachMortar',shooter),meteor});shooter.mortarActive=null;shooter.mortarImpactTimer=1.2;if(shooter.ammo<=0)manualReload(shooter,false);return true}
function reinforceMortarWall(p,x,z){const friendly=walls.filter(w=>w.active!==false&&w.destructible&&w.builtBy&&(w.builtBy===p.owner||sameTeam(w.builtBy,p.owner))&&w.hp<w.maxHp&&hypot2(w.x-x,w.z-z)<p.radius+3).sort((a,b)=>hypot2(a.x-x,a.z-z)-hypot2(b.x-x,b.z-z))[0];if(!friendly)return;friendly.hp=Math.min(friendly.maxHp,friendly.hp+50);friendly.acidDamage=Math.max(0,(friendly.acidDamage||0)-50);friendly.fortifySealTimer=Math.max(friendly.fortifySealTimer||0,6);friendly.flash=.24;worldGeometryDirty=true;for(const t of targetsFor(p.owner,p.owner===player)){const dx=t.x-friendly.x,dz=t.z-friendly.z,dist=Math.hypot(dx,dz)||1;if(dist>p.radius+3)continue;const resist=t.boss?.25:((t.alienScale||1)>1.3?.55:1),force=18*(1-dist/(p.radius+3))*resist;t.vx+=dx/dist*force;t.vz+=dz/dist*force;t.stagger=Math.max(t.stagger||0,t.boss?.16:.42)}addTier3Fx({type:'ring',weapon:'mortar',x:friendly.x,y:friendly.y+friendly.h/2+.1,z:friendly.z,life:.45,maxLife:.45,color:'#ff6b6b',size:8});tier3Cue('BREACH REINFORCED',2.4)}
function mortarExplosion(p,x,y,z){const radius=p.radius,targets=targetsFor(p.owner,p.owner===player);emitExplosion('mortar',p,x,y,z,radius);addFloorDecal({x,y:y+.01,z,normal:{x:0,y:1,z:0}},'rocket',radius*.24);for(const t of targets){if(t.dead||t.spawnGrace>0)continue;const dist=Math.hypot(t.x-x,(t.y-.75)-y,t.z-z);if(dist>radius||!blastCanReach(x,y,z,t))continue;const falloff=clamp(1-dist/radius,.18,1),priority=p.bunker&&(t.boss||(t.armor||0)>0||enemyAttackingWall(t))?1.25:1;damage(t,p.damage*falloff*priority,p.owner,false,{x:t.x,y:t.y-.7,z:t.z},{weapon:'mortar',armorMult:p.bunker?1.9:1.45});const dx=t.x-x,dz=t.z-z,d=Math.hypot(dx,dz)||1;t.vx+=dx/d*7*falloff;t.vz+=dz/d*7*falloff;t.vy+=4.2*falloff}damageStructuresInRadius(x,y,z,radius,p.damage*.62*(p.bigger?1.25:1),p.owner,'mortar');if(p.breach)reinforceMortarWall(p,x,z);if(isHumanActor(p.owner)){const close=hypot2(p.owner.x-x,p.owner.z-z)<28;if(close)addCameraKick(p.owner,'mortar',p.bigger?.5:0)}}
function rocketExplosion(p,x,y,z,world=false,directTarget=null){let radius=p.radius||8.4,explosionDamage=p.damage||124;if(p.bigBlast)radius*=1.42;if(p.siege)radius*=1.08;emitExplosion('rocket',p,x,y,z,radius);if(p.ultimate)addUltimateImpactVisual('rocket',x,y,z,null,null);const explosionTargets=targetsFor(p.owner,p.owner===player);if(p.siege&&directTarget&&(directTarget.boss||(directTarget.alienScale||1)>1.3||(directTarget.r||0)>.75)){addTier3Fx({type:'warhead',x,y,z,color:'#fff4a8',size:8,life:.24});const heavy=directTarget.boss?.72:.68;if(directTarget.boss){directTarget.armorCrackedTimer=4;directTarget.armorCrackedMult=1.18;tier3Cue('ARMOR CRACKED',2.4)}damage(directTarget,explosionDamage*heavy,p.owner,false,{x:directTarget.x,y:directTarget.y-.7,z:directTarget.z},{weapon:'rocket',armorMult:2.15})}for(const t of explosionTargets){if(t.dead||t.spawnGrace>0)continue;const dx=x-t.x,dy=y-(t.y-.75),dz=z-t.z,dist=Math.hypot(dx,dy,dz);if(dist>radius||!blastCanReach(x,y,z,t))continue;const scale=clamp(1-dist/radius,.12,1);damage(t,explosionDamage*scale,p.owner,false,{x:t.x,y:t.y-.7,z:t.z},{weapon:'rocket',armorMult:1.6});t.vx+=(-dx/(dist||1))*8.5*scale;t.vz+=(-dz/(dist||1))*8.5*scale;t.vy+=5.5*scale}damageStructuresInRadius(x,y,z,radius,explosionDamage*(p.siege?1.9:(p.bigBlast?1.05:.78)),p.owner,'rocket');if(p.cluster){for(let i=0;i<5;i++){const a=i*TAU/5+rand(-.22,.22),rr=rand(2.2,radius*.72),cx=x+Math.sin(a)*rr,cz=z+Math.cos(a)*rr,cy=y+rand(-.4,.8);emitExplosion('rocket',p,cx,cy,cz,2.1);for(const t of explosionTargets){const d=Math.hypot(cx-t.x,cy-(t.y-.75),cz-t.z);if(!t.dead&&d<3.2&&blastCanReach(cx,cy,cz,t))damage(t,34*(1-d/3.2),p.owner,false,{x:t.x,y:t.y-.7,z:t.z},{weapon:'rocket',armorMult:1.25})}damageStructuresInRadius(cx,cy,cz,3.2,p.siege?68:42,p.owner,'rocket')}}if(world)burst(x,y,z,'#ffffff',8,.35)}
function fireRocket(shooter,isPlayer){const w=WEAPONS.rocket,d=dirFrom(shooter,w.spread),ult=ultimateActive(shooter,'rocket'),grenade=isPlayer&&hasUpgrade('rocket','grenade'),fast=isPlayer&&hasUpgrade('rocket','velocity'),bigBlast=isPlayer&&hasUpgrade('rocket','blast'),cluster=isPlayer&&hasUpgrade('rocket','cluster'),siege=isPlayer&&hasUpgrade('rocket','siegeWarhead'),remote=isPlayer&&hasUpgrade('rocket','remoteDetonator')&&grenade,speed=grenade?(fast?49:39):64,life=grenade?(fast?2.15:2.85):2.35,r=grenade?.36:.42,color=ult?ultimateColor('rocket'):(grenade?'#ffea00':w.color);if(remote){const active=projectiles.filter(p=>p.owner===shooter&&p.remote&&!p.exploded);if(active.length>=MAX_REMOTE_GRENADES){rocketExplosion(active[0],active[0].x,active[0].y,active[0].z);active[0].exploded=true}}addMuzzleFlash(shooter,isPlayer,d,color,.25);if(ult)addUltimateMuzzleVisual(shooter,'rocket',d);projectiles.push({ultimate:ult,kind:'rocket',owner:shooter,...actorEyePosition(shooter),px:actorEyePosition(shooter).x,py:actorEyePosition(shooter).y,pz:actorEyePosition(shooter).z,vx:d.x*speed,vy:d.y*speed+(grenade?6.5:0),vz:d.z*speed,life,color,damage:w.damage*(ult?2.15:1),r:ult?r*1.35:r,tier:ult?4:(grenade?2:1),bouncy:grenade,bounces:grenade?4:0,bigBlast:bigBlast||ult,cluster:cluster||ult,siege,remote,radius:ult?14.5:8.4,pulse:0})}
function detonateRemoteGrenades(ent){if(!game.started||game.upgradesOpen||ent?.dead||ent?.weapon!=='rocket'||ent!==player||!hasUpgrade('rocket','remoteDetonator',ent))return false;const active=projectiles.filter(p=>p.owner===ent&&p.remote&&!p.exploded);if(!active.length)return false;for(const p of active){rocketExplosion(p,p.x,p.y,p.z);p.exploded=true;p.life=0}msg('REMOTE DETONATION');tier3Cue('REMOTE DETONATION',2.2);return true}
function targetsFor(shooter,isPlayer){if(game.mode==='survivor'){if(shooter?.alien)return [player,player2].filter(h=>h&&!h.dead);return bots.filter(b=>b.alien&&!b.dead)}const actors=[...new Set([player,player2,...bots].filter(Boolean))];return actors.filter(e=>e!==shooter&&!e.dead&&!sameTeam(shooter,e))}
// Aim samples follow the authored head/chest after fitting from the soles.
function humanTargetY(actor,kind='body'){
  const model=neonModelRenderer?.get(actorModelPath(actor)),anchor=model?.anchors?.[kind==='head'?'head':'core'];
  const posture=1-.27*Math.max(actor.slidePose||0,actor.crouchPose??(actor.crouching?1:0));
  if(anchor){const fit=window.NeonModels.actorFit(model,actor,PLAYER_H);return actor.y-PLAYER_H+(fit.offset[1]+anchor[1]*fit.scale[1])*posture;}
  return actor.y-PLAYER_H+(PLAYER_H-(kind==='head'?.24:.86))*HUMAN_VISUAL_HEIGHT*posture;
}
function alienVisualFactor(actor){return Number.isFinite(actor.alienVisualScale)&&actor.alienVisualScale>0?actor.alienVisualScale:1;}
function alienTargetY(actor,legacyY){const feet=actor.y-PLAYER_H;return feet+(legacyY-feet)*alienVisualFactor(actor);}
function rayEntity(o,d,t,range,radius=0){
  const vx=t.x-o.x,vy=t.y-o.y,vz=t.z-o.z,dist=vx*d.x+vy*d.y+vz*d.z;if(dist<0||dist>range)return null;
  const px=o.x+d.x*dist,py=o.y+d.y*dist,pz=o.z+d.z*dist,visualScale=t.alien?alienVisualFactor(t):1,wide=(t.alien?(t.r||.6)*1.10*visualScale:(t.r||.50)*1.28*HUMAN_VISUAL_WIDTH),bodyCenter=t.alien?alienTargetY(t,t.y-.95*(t.alienScale||1)):humanTargetY(t,'body'),headCenter=t.alien?alienTargetY(t,t.y-.45*(t.alienScale||1)):humanTargetY(t,'head'),headMiss=Math.hypot(t.x-px,headCenter-py,t.z-pz);
  if(t.alien){const headDist=vx*d.x+(headCenter-o.y)*d.y+vz*d.z;if(headDist>=0&&headDist<=range&&Math.hypot(t.x-o.x-d.x*headDist,headCenter-o.y-d.y*headDist,t.z-o.z-d.z*headDist)<wide*.62+radius)return {dist:headDist,head:true};}
  else{const posture=1-.27*Math.max(t.slidePose||0,t.crouchPose??(t.crouching?1:0)),rx=wide*.62+radius,ry=(posture<1?Math.max(.10,Math.min(wide*.62*posture,t.y-PLAYER_H+2.39*posture-headCenter)):wide*.62)+radius,denom=(d.x*d.x+d.z*d.z)/(rx*rx)+d.y*d.y/(ry*ry),headDist=((vx*d.x+vz*d.z)/(rx*rx)+(headCenter-o.y)*d.y/(ry*ry))/(denom||1);if(headDist>=0&&headDist<=range){const side=(t.x-o.x-d.x*headDist)/rx,vertical=(headCenter-o.y-d.y*headDist)/ry,forward=(t.z-o.z-d.z*headDist)/rx;if(side*side+vertical*vertical+forward*forward<1)return {dist:headDist,head:true}}}
  if(t.alien){
    // Tower shots come in at a steep downward angle. A single center-line body test could pass
    // between the alien's upper and lower body, especially while it is pressed against a wall.
    // Sweep several body bands and keep the closest valid impact so top-down fire is reliable.
    const scale=t.alienScale||1,yaw=t.faceYaw??t.yaw??0,fx=Math.sin(yaw),fz=Math.cos(yaw),rx=Math.cos(yaw),rz=-Math.sin(yaw);
    const bands=[.38,.62,.86,1.10,1.34,1.58,1.82].map(v=>alienTargetY(t,t.y-v*scale));
    let best=null;
    for(const cy of bands){
      const bandDist=((t.x-o.x)*d.x+(cy-o.y)*d.y+(t.z-o.z)*d.z);
      if(bandDist<0||bandDist>range)continue;
      const bx=o.x+d.x*bandDist,by=o.y+d.y*bandDist,bz=o.z+d.z*bandDist,relX=bx-t.x,relZ=bz-t.z;
      const forward=relX*fx+relZ*fz,side=relX*rx+relZ*rz,back=Math.max(0,-forward),front=Math.max(0,forward),vertical=by-cy;
      const sideRadius=wide+radius+.08*scale*visualScale,frontRadius=wide+radius+.10*scale*visualScale,backRadius=wide+.82*scale*visualScale+radius,verticalRadius=wide*.88+radius+.06*scale*visualScale;
      const bodyScore=(side/sideRadius)*(side/sideRadius)+(vertical/verticalRadius)*(vertical/verticalRadius)+(front?((front/frontRadius)*(front/frontRadius)):((back/backRadius)*(back/backRadius)));
      if(bodyScore<1&&(!best||bandDist<best.dist))best={dist:bandDist,head:false};
    }
    if(best)return best;
  }else{
    // Keep exposed knees and shins hittable, including the low mobile aim sample.
    // Project each band separately so steep shots use the correct impact depth.
    let best=null;const feet=t.y-PLAYER_H,posture=1-.27*Math.max(t.slidePose||0,t.crouchPose??(t.crouching?1:0));
    for(const [cy,bodyRadius] of [[bodyCenter,wide],[feet+.86*posture,wide*.55],[feet+.36*posture,wide*.55]]){
      const rx=bodyRadius+radius,ry=bodyRadius*posture+radius,denom=(d.x*d.x+d.z*d.z)/(rx*rx)+d.y*d.y/(ry*ry),bandDist=((vx*d.x+vz*d.z)/(rx*rx)+(cy-o.y)*d.y/(ry*ry))/(denom||1);if(bandDist<0||bandDist>range)continue;
      const side=(t.x-o.x-d.x*bandDist)/rx,vertical=(cy-o.y-d.y*bandDist)/ry,forward=(t.z-o.z-d.z*bandDist)/rx;
      if(side*side+vertical*vertical+forward*forward<1&&(!best||bandDist<best.dist))best={dist:bandDist,head:false};
    }
    if(best)return best;
  }
  return null
}
function lineClear(x1,y1,z1,x2,y2,z2){const dx=x2-x1,dy=y2-y1,dz=z2-z1,dist=Math.hypot(dx,dy,dz);if(dist<.02)return true;const hit=rayWorld({x:x1,y:y1,z:z1},{x:dx/dist,y:dy/dist,z:dz/dist},Math.max(0,dist-.08),false);return !hit}
function visibleToPlayer(ent,viewer=player){
  if(ent.dead)return false;
  // Do not draw enemy bodies, labels, or guns unless at least one meaningful point is actually visible.
  // This prevents enemies from visually bleeding through the opaque pastel walls and platforms.
  const eye=typeof actorEyePosition==='function'?actorEyePosition(viewer):viewer,eyeY=eye.y-.05;
  const ys=ent.alien?[.45,.86,1.34].map(offset=>alienTargetY(ent,ent.y-offset*(ent.alienScale||1))):[humanTargetY(ent,'head'),humanTargetY(ent,'body'),ent.y-PLAYER_H+.55],pts=ys.map(y=>[ent.x,y,ent.z]);
  for(const pt of pts){if(lineClear(eye.x,eyeY,eye.z,pt[0],pt[1],pt[2]))return true;}
  return false;
}
function angleDelta(target,current){return Math.atan2(Math.sin(target-current),Math.cos(target-current))}
function mobileAimInfo(ent,target,yawCone,pitchCone){if(!target||target.dead||target.spawnGrace>0||target.burrowed)return null;const weapon=WEAPONS[ent.weapon]||WEAPONS.pistol,maxRange=Math.max(8,(weapon.range||90)*1.08),eye=typeof actorEyePosition==='function'?actorEyePosition(ent):ent,eyeY=eye.y-.05,dx=target.x-eye.x,dz=target.z-eye.z,horizontal=Math.hypot(dx,dz),desiredYaw=Math.atan2(dx,dz),yawError=angleDelta(desiredYaw,ent.yaw);if(horizontal<.01||horizontal>maxRange||Math.abs(yawError)>yawCone)return null;const scale=target.alienScale||1,aimYs=target.alien?[.42,.86,1.34].map(offset=>alienTargetY(target,target.y-offset*scale)):[humanTargetY(target,'head'),humanTargetY(target,'body'),target.y-PLAYER_H+.55],candidates=[];for(const aimY of aimYs){const dy=eyeY-aimY,dist=Math.hypot(horizontal,dy),desiredPitch=Math.atan2(dy,horizontal),pitchError=desiredPitch-(ent.pitch||0);if(dist>maxRange||Math.abs(pitchError)>pitchCone)continue;const score=Math.hypot(yawError/yawCone,pitchError/pitchCone)+dist/maxRange*.08;candidates.push({target,aimY,dist,horizontal,desiredYaw,desiredPitch,yawError,pitchError,score})}candidates.sort((a,b)=>a.score-b.score);for(const info of candidates)if(lineClear(eye.x,eyeY,eye.z,target.x,info.aimY,target.z))return info;return null}
function findMobileAimTarget(ent,yawCone,pitchCone){let best=null;for(const target of targetsFor(ent,true)){const info=mobileAimInfo(ent,target,yawCone,pitchCone);if(info&&(!best||info.score<best.score))best=info}return best}
function clearMobileAutoFire(ent=player,resetThink=true){mobileInput.lockTarget=null;if(resetThink)mobileInput.aimThink=0;if(ent?.weapon==='plasma')ent.charge=0;if(ent?.mortarActive)releaseMortar(ent);updateMobileLockHud(null)}
function updateMobileAimAndFire(ent,dt){if(game.mode==='campaign'&&game.weaponSafe){clearMobileAutoFire(ent);return;}if(!mobileCanAct()||ent.dead){clearMobileAutoFire(ent);return}const weapon=WEAPONS[ent.weapon]||WEAPONS.pistol,wideWeapon=weapon.kind==='shotgun'||weapon.kind==='punch',yawCone=wideWeapon ? .225 : .175,pitchCone=wideWeapon ? .18 : .15;mobileInput.aimThink-=dt;let info=mobileAimInfo(ent,mobileInput.lockTarget,yawCone*1.42,pitchCone*1.42);if(mobileInput.aimThink<=0){const candidate=findMobileAimTarget(ent,yawCone,pitchCone);if(candidate||!info)info=candidate;mobileInput.aimThink=.09}if(!info){clearMobileAutoFire(ent,false);return}mobileInput.lockTarget=info.target;updateMobileLockHud(info.target);const manualLook=mobileInput.swiping?1:Math.hypot(mobileInput.lookX,mobileInput.lookY),assist=clamp(dt*(manualLook>.14?7.5:12.5),0,1);ent.yaw+=angleDelta(info.desiredYaw,ent.yaw)*assist;ent.pitch=clamp(ent.pitch+(info.desiredPitch-ent.pitch)*assist,-1.25,1.25);const yawError=Math.abs(angleDelta(info.desiredYaw,ent.yaw)),pitchError=Math.abs(info.desiredPitch-ent.pitch),fireCone=wideWeapon ? .13 : .105,aligned=yawError<=fireCone&&pitchError<=.10;if(weapon.kind==='plasma'){chargeHumanPlasma(ent,dt);if(aligned&&(ent.charge||0)>=.48)releaseHumanPlasma(ent);return}if(!aligned)return;if(weapon.kind==='mortar'){if(!ent.mortarActive)beginMortar(ent,true);if(ent.mortarActive){ent.mortarActive.desiredTravel=clamp(info.horizontal,4,weapon.range);if(ent.mortarActive.holdTime>=.55)releaseMortar(ent)}return}tryFire(ent,true)}
function visibleOnMinimap(ent){if(['tdm','conquest','campaign'].includes(game.mode))return ent.team===player.team;return visibleToPlayer(ent,player) || hypot2(ent.x-player.x,ent.z-player.z)<7.5;}
function damage(victim,amount,attacker,head=false,hitPoint=null,opts={}){if(game.mode==='survivor'){if(victim.human&&victim.name==='HOLOGRAM'){victim.hp-=amount;victim.dead=victim.hp<=0;return {hpDamage:amount,armorDamage:0};}if(victim.alien)amount*=survivalController?.damageMultiplier(victim)||1;if(isHumanActor(victim))amount=survivalEquipment?.absorb(victim,amount)??amount;}if((victim.armorCrackedTimer||0)>0&&isHumanActor(attacker))amount*=victim.armorCrackedMult||1.18;if((victim.phaseMarkedTimer||0)>0&&isHumanActor(attacker)&&attacker!==victim.phaseMarkedOwner)amount*=victim.phaseMarkedMult||1.15;if(victim.dead||victim.spawnGrace>0||victim.burrowed)return {armorBroken:false};if(game.mode==='survivor'&&!survivalController&&victim.alien&&victim.bossType!=='shield'&&attacker){const carrier=survivorAliveAliens().find(a=>a.bossType==='shield'&&Math.hypot(a.x-victim.x,a.z-victim.z)<9);if(carrier){const ax=attacker.x-carrier.x,az=attacker.z-carrier.z,ad=Math.hypot(ax,az)||1,vx=victim.x-carrier.x,vz=victim.z-carrier.z,vd=Math.hypot(vx,vz)||1;if((ax/ad)*(vx/vd)+(az/ad)*(vz/vd)<-.15)amount*=.45;}}if(game.mode==='survivor'&&!(victim.empUntil>(game.survivalElapsed||0))&&victim.bossType==='shield'&&attacker){const dx=attacker.x-victim.x,dz=attacker.z-victim.z,d=Math.hypot(dx,dz)||1,front=Math.sin(victim.faceYaw||0)*dx/d+Math.cos(victim.faceYaw||0)*dz/d,weapon=opts.weapon||attacker.weapon;if(front>.12){const reduction=weapon==='arc'?.62:.22;amount*=reduction;victim.shieldFlash=.18;burst(victim.x+Math.sin(victim.faceYaw||0)*1.7,victim.y-.8,victim.z+Math.cos(victim.faceYaw||0)*1.7,'#69c8ff',weapon==='arc'?10:5,.22)}else if(front<-.35)amount*=1.35;}if(game.mode==='survivor'&&victim.bossType==='rammer'&&(victim.bossState==='windup'||victim.bossState==='charge')&&attacker&&(attacker.weapon==='punch'||(attacker.weapon==='shotgun'&&amount>=70))){victim.bossState='stunned';victim.bossTimer=attacker.weapon==='punch'?2.1:1.25;victim.stagger=victim.bossTimer;victim.vx=victim.vz=0;feed((attacker.weapon==='punch'?'Punch Gun':'Shotgun')+' interrupted the Rammer!');msg('CHARGE INTERRUPTED')}if(game.mode==='survivor'&&victim.climbing)amount*=victim.boss?1.2:1.35;if(game.mode==='frag'&&attacker&&attacker.weapon==='sniper'){amount=999;opts={...opts,bypassArmor:true};}const hp=hitPoint||{x:victim.x,y:victim.y-(head?.22:.82),z:victim.z};let hpDamage=amount,armorBroken=false,armorHit=0;if((victim.armor||0)>0&&!opts.bypassArmor&&!head){armorHit=Math.min(victim.armor,amount*(opts.armorMult||1));victim.armor-=armorHit;hpDamage=Math.max(0,amount-armorHit*.82);if(!opts.minimalFx)armorSpark(hp.x,hp.y,hp.z,8);if(victim.armor<=0){armorBroken=true;victim.armor=0;if(!opts.minimalFx)armorSpark(hp.x,hp.y,hp.z,20);if(victim===player)msg('ARMOR BROKEN')}}else if(head&&(victim.armor||0)>0){armorHit=Math.min(victim.armor,amount*.12);victim.armor-=armorHit;hpDamage=amount*.94}victim.hp-=hpDamage;recordCombatFeedback(victim,attacker,head,armorHit,hpDamage);if(game.mode==='survivor'&&isHumanActor(victim))survivalProgress?.recordEvent('damageTaken',Math.max(0,hpDamage)+armorHit);if(hpDamage>0&&!opts.minimalFx)bloodBurst(hp.x,hp.y,hp.z,head,hpDamage);if(victim===player||victim===player2)msg((victim===player2?'P2 ':'')+(armorHit?'-'+Math.round(armorHit)+' ARMOR ':'')+(hpDamage?'-'+Math.round(hpDamage)+' HP':''));if(attacker&&attacker!==victim&&!opts.noUltimate&&(armorHit>0||hpDamage>0))addUltimateCharge(attacker,opts.weapon||attacker.weapon,(armorHit+hpDamage));if(victim.hp<=0)kill(victim,attacker,head);return {armorBroken,armorDamage:armorHit,hpDamage}}
function kill(victim,killer,head=false){
  if(victim.dead)return;tacticalController?.recordOutcome(victim,killer);neonModelRenderer?.rememberDeath(actorModelPath(victim),victim);if(victim.mortarActive)releaseMortar(victim);victim.dead=true;releaseArcAnchor(victim);victim.deaths=(victim.deaths||0)+1;if(killer&&killer!==victim)registerUpgradeKill(killer);
  const victimHuman=isHumanActor(victim), killerHuman=isHumanActor(killer);
  if(['conquest','campaign'].includes(game.mode)){warCasualty(victim,killer);return;}
  if(game.mode==='survivor'){
    if(victim.alien){
      survivalProgress?.recordEvent('kills');if(victim.boss)survivalProgress?.recordEvent('bosses');if(victim.flying)survivalProgress?.recordEvent('flyingKills');if(killerHuman&&buildPieces.some(p=>p.active!==false&&Math.hypot(p.x-victim.x,p.z-victim.z)<12))survivalProgress?.recordEvent('defenseKills');const fx=survivalReward('effect');if(fx)burst(victim.x,victim.y,victim.z,fx.palette[0],18,.5);
      if(killerHuman){killer.score=(killer.score||0)+1;killer.killStreak=(killer.killStreak||0)+1;feed((killer===player?'YOU':killer.name)+' killed '+victim.name);}
      game.waveKills++;game.waveKilled=(game.waveKilled||0)+1;game.scrap=(game.scrap||0)+(victim.boss?15:2);
    }else if(victimHuman){
      victim.killStreak=0;feed((victim===player?'YOU':victim.name)+' was mauled by '+(killer?.name||'ALIEN'));
      if(victim===player){toggleUpgradeTree(false);hud.death.style.display='grid';setTimeout(()=>{hud.death.style.display='none'},900)}
    }
    burst(victim.x,victim.y,victim.z,victim.color||'#ff2b80');
    return;
  }
  if(game.mode==='siege'){
    if(victimHuman){
      if(killer)killer.score=(killer.score||0)+1;
      victim.killStreak=0;toggleUpgradeTree(false);
      feed((victim===player?'YOU':victim.name)+' fell in Siege Mode — no respawn this round.');
      if(victim===player){hud.death.style.display='grid';setTimeout(()=>{hud.death.style.display='none'},900)}
      burst(victim.x,victim.y,victim.z,victim.color||'#ff2b80');
      setTimeout(()=>updateSiege(0),250);
      return;
    }
    if(victim.siegeEnemy){
      if(killerHuman){killer.score=(killer.score||0)+1;killer.killStreak=(killer.killStreak||0)+1;const reward=head?2:1;killer.upgradePoints=(killer.upgradePoints||0)+reward;feed((killer===player?'YOU':killer.name)+' dropped '+victim.name+' — raider lives left: '+Math.max(0,(victim.siegeLives||1)-1));}
      victim.siegeLives=Math.max(0,(victim.siegeLives||1)-1);
      burst(victim.x,victim.y,victim.z,victim.color||'#ff2b80');
      if(victim.siegeLives>0){setTimeout(()=>{if(game.mode==='siege'&&!game.over&&!game.siegeRoundOver){spawn(victim);victim.team=2;victim.siegeEnemy=true;victim.siegeLives=Math.max(1,victim.siegeLives||1);equip(victim,victim.siegeLoadout||'assault',false);feed(victim.name+' re-entered the siege with '+victim.siegeLives+' life left.')}},950)}
      else setTimeout(()=>updateSiege(0),250);
      return;
    }
  }
  if(killer===player){player.score++;player.killStreak=(player.killStreak||0)+1;if(game.mode==='frag'){feed('YOU fragged '+victim.name+' — '+player.score+'/'+fragKillLimit());msg('FRAG! '+player.score+'/'+fragKillLimit())}else{const pts=head?2:1,bonus=player.killStreak>2?1:0;player.upgradePoints+=pts+bonus;feed('YOU dropped '+victim.name+(head?' with a headshot':'')+' +'+(pts+bonus)+' upgrade point'+(pts+bonus===1?'':'s'));msg(head?'HEADSHOT! +2 PTS':'+1 FRAG POINT')}}else if(victim===player){if(killer)killer.score++;player.killStreak=0;toggleUpgradeTree(false);feed((killer?.name||'ENEMY')+' eliminated YOU with '+(WEAPONS[killer?.weapon]?.name||'SNIPER'));hud.death.style.display='grid';setTimeout(()=>{hud.death.style.display='none';if(!game.over)spawn(player)},Math.max(.2,game.respawnDelay||1)*1000)}else{if(killer){killer.score++;feed(killer.name+' dropped '+victim.name+(game.mode==='frag'?' — '+killer.score+'/'+fragKillLimit():''));}}checkWinCondition();burst(victim.x,victim.y,victim.z,victim.color||'#ff2b80');setTimeout(()=>{if(victim!==player&&!game.over)spawn(victim)},Math.max(.2,game.respawnDelay||1)*1000)}
function burst(x,y,z,c,count=18,maxLife=.7){count=scaledVisualCount(count);for(let i=0;i<count;i++)particles.push({kind:'spark',x,y,z,vx:rand(-6,6),vy:rand(0,8),vz:rand(-6,6),life:rand(.18,maxLife),maxLife,c,size:4});trimVisualArray(particles,visualPreset().maxParticles)}
function tryBotHarpoonGrapple(bot,dt){bot.harpoonGrappleCd=Math.max(0,(bot.harpoonGrappleCd||0)-dt);if(bot.weapon!=='harpoon'||bot.enemy||bot.reload>0||bot.cool>0||bot.ammo<=0||bot.harpoonGrappleCd>0||Math.random()>dt*.018)return false;const goal=bot.route||bot.target;if(!goal)return false;let best=null,score=Infinity;for(const piece of [...walls,...platforms]){if(piece.active===false)continue;const d=hypot2(piece.x-bot.x,piece.z-bot.z);if(d<9||d>38)continue;const toward=hypot2(piece.x-goal.x,piece.z-goal.z);if(toward<score){best=piece;score=toward}}if(!best)return false;const dx=best.x-bot.x,dz=best.z-bot.z,dist=Math.hypot(dx,dz)||1;bot.yaw=Math.atan2(dx,dz);bot.pitch=Math.atan2(bot.y-(best.type==='platform'?best.y+best.h:best.y),dist);tryFire(bot,false);bot.pitch=0;bot.harpoonGrappleCd=rand(12,20);return true}
const BOT_WEAPON_VALUE={pistol:1,smg:3.7,shotgun:3.9,burst:4.15,assault:4.25,arc:4.45,punch:4.40,harpoon:4.5,rocket:4.55,mortar:4.6,plasma:4.55,laser:4.65,mender:2.2,sniper:4.8};
const BOT_ARCHETYPES={
  rusher:{label:'BREACHER',weapons:['shotgun','smg','punch'],range:.68,speed:1.12,strafe:.82,fire:1.10,flank:.35,retreat:.68},
  marksman:{label:'SENTINEL',weapons:['sniper','burst','harpoon'],range:1.34,speed:.92,strafe:.88,fire:.84,flank:.44,retreat:1.28},
  rifler:{label:'VANGUARD',weapons:['assault','burst','laser'],range:1.04,speed:1,strafe:1,fire:1,flank:.52,retreat:1},
  skirmisher:{label:'FLANKER',weapons:['smg','arc','plasma'],range:.92,speed:1.08,strafe:1.32,fire:.96,flank:.92,retreat:.96},
  heavy:{label:'DEMOLISHER',weapons:['rocket','mortar','plasma','arc'],range:1.12,speed:.88,strafe:.62,fire:.78,flank:.22,retreat:1.12}
};
function applyBotPersonality(bot){const p=BOT_ARCHETYPES[bot.style]||BOT_ARCHETYPES.rifler;bot.tacticalProfile=window.NeonTacticalAI?.profileName({style:bot.style})||'balanced';bot.tacticLabel=window.NeonTacticalAI?.PROFILES[bot.tacticalProfile]?.label||p.label;bot.preferredWeapons=p.weapons;bot.rangeMult=p.range;bot.speedMult=p.speed;bot.strafeMult=p.strafe;bot.fireTempo=p.fire;bot.flankBias=p.flank;bot.retreatMult=p.retreat;bot.flankSign=Math.random()<.5?-1:1;bot.flankTimer=rand(1.4,3.2);return bot}
function botWeaponValue(type,bot){let v=BOT_WEAPON_VALUE[type]||1;if(bot?.preferredWeapons?.includes(type))v+=1.05-Math.max(0,bot.preferredWeapons.indexOf(type))*.16;if(bot?.style==='heavy'&&(type==='rocket'||type==='mortar'))v+=.35;if(bot?.style==='skirmisher'&&(type==='arc'||type==='plasma'))v+=.28;return v}
function nearestPickup(bot){let best=null,bd=999;for(const p of pickups){if(p.gone||p.taken>0||(p.kind==='armor'&&bot.armor>=bot.maxArmor)||(p.kind==='health'&&bot.hp>=bot.maxHp))continue;const d=hypot2(p.x-bot.x,p.z-bot.z);let priority=d;if(p.kind==='health')priority=bot.hp<55?d*.28:(bot.hp<82?d*.62:d*1.35);else if(p.kind==='armor')priority=bot.armor<35?d*.35:(bot.armor<70?d*.72:d*1.2);else if(p.kind==='weapon'){const gain=botWeaponValue(p.type,bot)-botWeaponValue(bot.weapon,bot);priority=gain>.15?d/(1+gain*1.15):d*2.8;if((bot.weapon==='pistol'||bot.ammo<=2)&&gain>0)priority*=.42}priority/=bot.pickupBias||1;if(priority<bd){bd=priority;best=p}}return {p:best,d:best?hypot2(best.x-bot.x,best.z-bot.z):999,priority:bd}}
function aiProfile(type){return {
  pistol:{min:10,ideal:20,max:38,strafe:.58,advance:1.08,retreat:.84,pickups:.48,jump:.04,fireBias:1.18},
  sniper:{min:32,ideal:48,max:82,strafe:.38,advance:.72,retreat:1.05,pickups:.24,jump:.08,fireBias:1.55},
  shotgun:{min:4,ideal:10,max:22,strafe:.30,advance:1.62,retreat:.55,pickups:.30,jump:.06,fireBias:1.35},
  burst:{min:16,ideal:30,max:58,strafe:.56,advance:1.06,retreat:.88,pickups:.32,jump:.05,fireBias:1.42},
  arc:{min:10,ideal:22,max:44,strafe:.50,advance:1.14,retreat:.78,pickups:.34,jump:.05,fireBias:1.24},
  punch:{min:5,ideal:12,max:20,strafe:.36,advance:1.55,retreat:.55,pickups:.36,jump:.07,fireBias:1.42},
  harpoon:{min:16,ideal:30,max:52,strafe:.50,advance:1.02,retreat:.92,pickups:.34,jump:.04,fireBias:1.44},
  mortar:{min:18,ideal:36,max:68,strafe:.40,advance:.82,retreat:1.12,pickups:.32,jump:.03,fireBias:1.32},
  smg:{min:8,ideal:17,max:34,strafe:.66,advance:1.26,retreat:.74,pickups:.34,jump:.05,fireBias:1.32},
  assault:{min:14,ideal:27,max:52,strafe:.52,advance:1.12,retreat:.86,pickups:.30,jump:.05,fireBias:1.30},
  plasma:{min:15,ideal:29,max:56,strafe:.46,advance:1.02,retreat:.84,pickups:.32,jump:.05,fireBias:1.18},
  laser:{min:17,ideal:31,max:60,strafe:.40,advance:.94,retreat:.95,pickups:.30,jump:.04,fireBias:1.38}
}[type]||{min:14,ideal:25,max:44,strafe:.55,advance:1,retreat:1,pickups:.34,jump:.06,fireBias:1}}
function botSeparation(bot){let sx=0,sz=0;const war=['tdm','conquest','campaign'].includes(game.mode),neighbors=war?[...new Set([player,player2,...bots].filter(Boolean))]:bots;for(const o of neighbors){if(o===bot||o.dead||war&&Math.abs(o.y-bot.y)>2.5)continue;const dx=bot.x-o.x,dz=bot.z-o.z,d=Math.hypot(dx,dz);if(d>0&&d<3.2){const push=(3.2-d)/3.2*(war&&o.human?4:1);sx+=dx/d*push;sz+=dz/d*push}}return {x:sx,z:sz}}
const ALIEN_MAX_ALIVE=25;
function survivorHumans(){return [player,player2].filter(h=>h&&!h.dead)}
function survivorAliveAliens(){return bots.filter(b=>b.alien&&!b.dead)}
function survivorWaveBudget(wave){
  const total=Math.min(48,8+Math.floor(wave*2.6));
  let yellow=wave>=3?Math.min(9,1+Math.floor((wave-3)*.55)):0;
  let red=wave>=6?Math.min(9,1+Math.floor((wave-6)*.50)):0;
  let green=wave>=10?Math.min(7,1+Math.floor((wave-10)*.42)):0;
  let purple=wave>=12?Math.min(6,1+Math.floor((wave-12)*.35)):0;
  let stripe=wave>=15?Math.min(7,1+Math.floor((wave-15)*.45)):0;
  let flyer=wave>=15?Math.min(5,1+Math.floor((wave-15)*.28)):0;
  let climber=wave>=4?Math.min(8,1+Math.floor((wave-4)*.42)):0;
  if(wave>10){const shift=Math.min(total-4,Math.floor((wave-10)*.75));yellow+=Math.floor(shift*.28);red+=Math.floor(shift*.24);green+=Math.floor(shift*.16);purple+=Math.floor(shift*.12);if(wave>=15){stripe+=Math.floor(shift*.13);flyer+=Math.floor(shift*.07)}}
  let specials=yellow+red+green+purple+stripe+flyer+climber;
  const counts={yellow,red,green,purple,stripe,flyer,climber};
  while(specials>total-1){for(const k of ['yellow','red','green','purple','stripe','flyer','climber']){if(specials<=total-1)break;if(counts[k]>0){counts[k]--;specials--;}}}
  yellow=counts.yellow;red=counts.red;green=counts.green;purple=counts.purple;stripe=counts.stripe;flyer=counts.flyer;climber=counts.climber;
  let blue=Math.max(0,total-yellow-red-green-purple-stripe-flyer-climber);
  return {blue,red,yellow,green,purple,stripe,flyer,climber,total:blue+red+yellow+green+purple+stripe+flyer+climber}
}
function makeAlien(kind,wave){const inputWave=wave;wave=window.NeonSurvivalAI?.balanceWave(wave,kind.endsWith('Boss'))||Math.min(wave,60);const specs=window.NeonSurvivalAI?.speciesSpec(kind,inputWave)||{
  blue:{name:'BLUE RAPTOR',color:'#1b8cff',r:.56,hp:34+wave*4,damage:10+Math.floor(wave/5)*2,speed:12.4+Math.min(2.4,wave*.16),range:1.55,cool:.62,scale:.92,jumpMin:17.0,jumpMax:20.0},
  yellow:{name:'YELLOW PLASMA RAPTOR',color:'#ffe433',r:.62,hp:82+wave*8,damage:14+Math.floor(wave/4)*2,speed:13.8+Math.min(2.0,wave*.12),range:1.65,cool:.56,scale:1.00,jumpMin:17.5,jumpMax:20.5,ranged:true,ideal:18},
  red:{name:'RED BRUTE',color:'#ff3131',r:.78,hp:210+wave*22,damage:22+Math.floor(wave/3)*3,speed:10.6+Math.min(1.0,wave*.05),range:1.85,cool:.82,scale:1.28,jumpMin:17.0,jumpMax:20.0},
  green:{name:'GREEN RUNNER',color:'#28ff6a',r:.54,hp:76+wave*7,damage:13+Math.floor(wave/5)*2,speed:17.2,range:1.45,cool:.46,scale:.86,jumpMin:14.8,jumpMax:17.4,pathing:true},
  stripe:{name:'GREEN STRIPED RUNNER',color:'#7cff38',r:.55,hp:98+wave*8,damage:15+Math.floor(wave/5)*2,speed:21.4+Math.min(1.8,(wave-15)*.12),range:1.45,cool:.40,scale:.90,jumpMin:15.8,jumpMax:18.4,pathing:true,striped:true},
  purple:{name:'PURPLE GUNNER',color:'#b04cff',r:.66,hp:145+wave*12,damage:12+Math.floor(wave/6)*2,speed:13.4,range:1.7,cool:.24,scale:1.05,rangedGun:true,ideal:20,jumpMin:19.0,jumpMax:22.0},
  flyer:{name:'PTERODACTYL',color:'#ff7a38',r:.72,hp:62+wave*5,damage:18+Math.floor(wave/5)*3,speed:18.5,range:1.65,cool:1.45,scale:1.05,flying:true},
  climber:{name:'CYAN WALL CLIMBER',color:'#27f5df',r:.60,hp:96+wave*9,damage:15+Math.floor(wave/5)*2,speed:11.2,range:1.65,cool:.68,scale:1.03,jumpMin:10,jumpMax:12,climber:true},
  rammerBoss:{name:'RAMMER',color:'#ff5a20',r:1.18,hp:980+wave*92,damage:40+wave*2,speed:8.0,range:2.45,cool:1.05,scale:1.92,jumpMin:12,jumpMax:14,boss:true,bossType:'rammer',wallDamageMult:2.4},
  spitterBoss:{name:'SPITTER TITAN',color:'#b9e92c',r:1.10,hp:900+wave*86,damage:25+wave*2,speed:7.2,range:2.2,cool:.9,scale:1.82,jumpMin:11,jumpMax:13,boss:true,bossType:'spitter',ranged:true,ideal:30},
  matriarchBoss:{name:'CLIMBER MATRIARCH',color:'#b34cff',r:1.08,hp:940+wave*88,damage:31+wave*2,speed:9.2,range:2.25,cool:.76,scale:1.82,jumpMin:9,jumpMax:11,boss:true,bossType:'matriarch',climber:true},
  shieldBoss:{name:'SHIELD CARRIER',color:'#258cff',r:1.16,hp:1100+wave*96,damage:34+wave*2,speed:7.6,range:2.4,cool:.88,scale:1.90,jumpMin:11,jumpMax:13,boss:true,bossType:'shield'},
  burrowerBoss:{name:'BURROWER',color:'#c07838',r:1.12,hp:1020+wave*92,damage:38+wave*2,speed:8.6,range:2.35,cool:.82,scale:1.86,jumpMin:10,jumpMax:12,boss:true,bossType:'burrower'},
  commanderBoss:{name:'BREACH COMMANDER',color:'#ff2748',r:1.05,hp:980+wave*90,damage:28+wave*2,speed:8.4,range:2.15,cool:.72,scale:1.78,jumpMin:12,jumpMax:14,boss:true,bossType:'commander',rangedGun:true,ideal:22}
}[kind]||{};return {name:specs.name||'RAPTOR',x:0,y:PLAYER_H,z:0,vx:0,vy:0,vz:0,r:specs.r*ALIEN_SIZE_MULT,yaw:0,pitch:0,onGround:false,team:66,score:0,deaths:0,dead:false,hp:specs.hp,maxHp:specs.hp,armor:0,maxArmor:0,weapon:specs.rangedGun?'smg':'alien',ammo:999,reload:0,cool:0,heat:0,spawnGrace:.65,target:null,enemy:null,think:0,fireCd:rand(.25,1.0),strafe:Math.random()<.5?-1:1,strafeT:0,color:specs.color,walk:0,faceYaw:0,slowTimer:0,slowAmount:0,stagger:0,alien:true,alienKind:kind,alienScale:(specs.scale||1)*ALIEN_SIZE_MULT,alienVisualScale:RAPTOR_KINDS.has(kind)?RAPTOR_VISUAL_SCALE:1,alienDamage:specs.damage,alienSpeed:specs.speed,meleeRange:specs.range*ALIEN_SIZE_MULT,meleeCool:specs.cool,ranged:!!specs.ranged,rangedGun:!!specs.rangedGun,idealRange:specs.ideal||0,smartPathing:!!specs.pathing,jumpMin:specs.jumpMin,jumpMax:specs.jumpMax,flying:!!specs.flying,flyState:specs.flying?'circle':'ground',circleTimer:rand(1.8,3.7),diveTimer:0,flyAngle:rand(0,TAU),diveDir:null,chargeTimer:0,attackAnim:0,tailPhase:rand(0,TAU),striped:!!specs.striped,climber:!!specs.climber,boss:!!specs.boss,bossType:specs.bossType||'',wallDamageMult:specs.wallDamageMult||1,bossTimer:rand(2.8,4.5),bossState:'hunt',bossTarget:null,bossPhase:0,broodTimer:specs.bossType==='matriarch'?rand(4,6):0,broodSpawned:0,climbThink:0,climbTarget:null,climbing:false}}
function spawnAlienAtEdge(kind,wave,spawnPoint=null){
  if(survivorAliveAliens().length>=ALIEN_MAX_ALIVE)return null;
  const a=makeAlien(kind,wave),sp=spawnPoint||(game.mode==='survivor'?(survivalMapRuntime?.spawnPoint(a.flying?'fly':kind==='stalker'?'burrow':'ground',Math.random)||survivorBreachSpawnPoint()):(edgeSpawns.length?pick(edgeSpawns):{x:rand(-ARENA_SAFE,ARENA_SAFE),z:rand(-ARENA_SAFE,ARENA_SAFE)}));
  a.x=clamp(sp.x+(game.mode==='survivor'?0:rand(-2.2,2.2)),-ARENA_SAFE+2,ARENA_SAFE-2);a.z=clamp(sp.z+(game.mode==='survivor'?0:rand(-2.2,2.2)),-ARENA_SAFE+2,ARENA_SAFE-2);a.y=topAtPoint(a.x,a.z)+PLAYER_H;
  if(a.flying){a.y=Math.max(a.y+12,12+rand(0,8));a.onGround=false;a.flyAngle=Math.atan2(a.x,a.z)+rand(-.7,.7);a.flyState='circle';a.circleTimer=rand(2.2,4.2)}else a.onGround=true;
  a.faceYaw=Math.atan2(-a.x,-a.z);a.yaw=a.faceYaw;bots.push(a);burst(a.x,a.y-.8,a.z,a.color,14,.55);return a;
}
function survivorWaveLabel(b){return b.blue+' blue'+(b.yellow?', '+b.yellow+' yellow':'')+(b.red?', '+b.red+' red':'')+(b.green?', '+b.green+' green':'')+(b.purple?', '+b.purple+' purple gunners':'')+(b.stripe?', '+b.stripe+' striped runners':'')+(b.flyer?', '+b.flyer+' pterodactyl flyers':'')+(b.climber?', '+b.climber+' wall climbers':'')}
function spawnSurvivorSquad(force=false){
  if(game.over||game.mode!=='survivor'||!game.survivorOrder)return;
  const alive=survivorAliveAliens().length;
  if(!force&&alive>=ALIEN_MAX_ALIVE)return;
  const remaining=game.survivorOrder.length-(game.waveSpawnIndex||0);
  if(remaining<=0){game.waveSpawning=false;return;}
  const desired=Math.min(remaining,game.encounterPlan?.squadSize||4);
  let spawned=0;const members=[],anchor=survivalMapRuntime.spawnPoint('ground',Math.random);
  for(let n=0;n<desired&&survivorAliveAliens().length<ALIEN_MAX_ALIVE;n++){
    const kind=game.survivorOrder[game.waveSpawnIndex++];
    const angle=n*2.4,point={x:anchor.x+Math.cos(angle)*1.3,z:anchor.z+Math.sin(angle)*1.3};const info=survivalMapRuntime.pointInfo(point.x,point.z);const alien=spawnAlienAtEdge(kind,game.survivorWave,info.walkable&&!info.obstacle?point:anchor);if(alien){members.push(alien);spawned++;game.waveSpawned=(game.waveSpawned||0)+1;if(game.encounterPlan?.eliteEvery&&game.waveSpawned%game.encounterPlan.eliteEvery===0&&!alien.boss){alien.name='ELITE '+alien.name;alien.hp*=1.18;alien.maxHp=alien.hp;alien.alienDamage*=1.12;alien.elite=true;}}
  }
  survivalController?.assignSquad(members,{wave:game.survivorWave,route:(game.waveSpawned||0)%4});
  if(spawned>0&&game.waveSpawned<=desired){feed('Wave '+game.survivorWave+' breach squad: '+spawned+' aliens entered from the edge. '+Math.max(0,game.survivorOrder.length-game.waveSpawnIndex)+' held back.');msg('EDGE BREACH');}
  game.waveSpawning=(game.waveSpawnIndex||0)<game.survivorOrder.length;
  game.nextSquadTimer=game.encounterPlan?.spawnDelay||10;
}
function survivorBossForWave(wave){const fixed=['rammerBoss','spitterBoss','matriarchBoss','shieldBoss','burrowerBoss'];if(wave<=25)return fixed[Math.max(0,Math.floor(wave/5)-1)];const elites=['commanderBoss',...fixed];return wave===30?'commanderBoss':elites[Math.floor(Math.random()*elites.length)]}
function spawnSurvivorWave(){
  game.survivorWave++;if(game.survivalMap==='breach_ring')reinforceSurvivorBreaches(game.survivorWave);
  const plan=window.NeonSurvivalAI.planWave(game.survivorWave,{map:game.survivalMap,difficulty:game.difficulty,history:game.encounterHistory||[],performance:survivalController?.observedPerformance()||{}});
  game.encounterPlan=plan;game.encounterHistory=(game.encounterHistory||[]).concat(plan.encounter).slice(-4);game.waveSpawning=true;
  game.wallThorns=!!game.thornsQueued;game.thornsQueued=false;game.survivorBudget=plan.budget;game.survivorOrder=plan.order;game.waveSpawnIndex=0;game.waveSpawned=0;game.waveKilled=0;game.waveTotal=plan.total;game.nextSquadTimer=0;
  msg((plan.bosses.length?'BOSS WAVE ':'WAVE ')+game.survivorWave+' · '+plan.name.toUpperCase());if(plan.bosses.length)survivalInterface?.warn('BOSS ARRIVAL · '+plan.bosses.map(k=>makeAlien(k,game.survivorWave).name).join(' + '),'boss');
  feed('Wave '+game.survivorWave+' · '+plan.name+' · '+plan.total+' enemies, arriving in coordinated squads.');spawnSurvivorSquad(true);
}
function survivorPickupCount(kind){return pickups.filter(p=>!p.gone&&p.taken<=0&&(!kind||p.kind===kind)).length}
function spawnSurvivorPickup(forceArmor=false){if(game.mode!=='survivor')return;if(survivorPickupCount('weapon')>=4&&!forceArmor)return;if(survivorPickupCount('armor')>=1&&forceArmor)return;const n=survivalMapRuntime?.pickupPoint()||pick(nav)||{x:0,z:0},x=clamp(n.x+(survivalMapRuntime?.loaded?0:rand(-3,3)),-ARENA_SAFE+3,ARENA_SAFE-3)/WORLD_SCALE,z=clamp(n.z+(survivalMapRuntime?.loaded?0:rand(-3,3)),-ARENA_SAFE+3,ARENA_SAFE-3)/WORLD_SCALE;if(forceArmor||Math.random()<.14){addArmorPickup(Math.random()<.2?'large':'small',x,z);feed('Rare armor dropped somewhere on the arena.')}else{addPickup(pick(['assault','smg','shotgun','sniper','plasma','laser','mender','burst','arc','rocket','mortar','punch','harpoon']),x,z);feed('Weapon pickup dropped somewhere on the arena.')}}
function spawnSurvivorHealthDrop(){if(game.mode!=='survivor')return;if(survivorPickupCount('health')>=2)return;const n=survivalMapRuntime?.pickupPoint()||pick(nav)||{x:0,z:0},x=clamp(n.x+(survivalMapRuntime?.loaded?0:rand(-3,3)),-ARENA_SAFE+3,ARENA_SAFE-3)/WORLD_SCALE,z=clamp(n.z+(survivalMapRuntime?.loaded?0:rand(-3,3)),-ARENA_SAFE+3,ARENA_SAFE-3)/WORLD_SCALE;addHealthPickup(x,z);feed('Small +10 HP health drop appeared somewhere on the arena.')}

function activeLooseItemCount(){return pickups.filter(p=>!p.gone&&p.taken<=0).length}
function deathmatchPlayerCount(){
  if(!(game.mode==='dm'||game.mode==='tdm'))return 0;
  return Math.max(1,1+(game.secondPlayer&&player2?1:0)+(game.botCount||0));
}
function validDeathmatchItemPosition(x,z,placed=[]){
  const edgePadding=2.5,ground=topAtPoint(x,z);
  if(Math.abs(x)>ARENA_SAFE-edgePadding||Math.abs(z)>ARENA_SAFE-edgePadding||ground<-20)return false;
  if(walls.some(w=>w.active!==false&&Math.abs(x-w.x)<w.w/2+1&&Math.abs(z-w.z)<w.d/2+1&&ground<w.y+w.h/2+.2))return false;
  const actors=[player,player2,...bots].filter((a,i,list)=>a&&!a.dead&&list.indexOf(a)===i);
  if(actors.some(a=>hypot2(x-a.x,z-a.z)<3.2))return false;
  if(pickups.some(p=>!p.gone&&p.taken<=0&&hypot2(x-p.x,z-p.z)<3.2))return false;
  return !placed.some(p=>hypot2(x-p.x,z-p.z)<5);
}
function findDeathmatchItemPosition(placed=[]){
  // Deathmatch/Team Deathmatch supply drops should feel like contested middle spawns,
  // not a full arena stocked with gear. Try a ring around center first, then a
  // slightly wider middle fallback if walls/players already occupy the first spots.
  for(let i=0;i<70;i++){
    const angle=rand(0,TAU),radius=rand(4,24);
    const x=Math.cos(angle)*radius,z=Math.sin(angle)*radius;
    if(validDeathmatchItemPosition(x,z,placed))return {x,z,y:topAtPoint(x,z)};
  }
  for(let i=0;i<45;i++){
    const angle=rand(0,TAU),radius=rand(24,34);
    const x=Math.cos(angle)*radius,z=Math.sin(angle)*radius;
    if(validDeathmatchItemPosition(x,z,placed))return {x,z,y:topAtPoint(x,z)};
  }
  return null;
}
function spawnDeathmatchItems(count){
  if(!(game.mode==='dm'||game.mode==='tdm')||count<=0)return 0;
  const itemPool=['assault','smg','shotgun','sniper','plasma','laser','mender','burst','arc','rocket','mortar','punch','harpoon'],placed=[];
  for(let i=0;i<count;i++){
    const spot=findDeathmatchItemPosition(placed);if(!spot)break;
    const before=pickups.length,roll=Math.random(),x=spot.x/WORLD_SCALE,z=spot.z/WORLD_SCALE;
    if(roll<.18)addHealthPickup(x,z);else if(roll<.36)addArmorPickup(Math.random()<.22?'large':'small',x,z);else addPickup(pick(itemPool),x,z);
    if(pickups.length>before){const p=pickups[pickups.length-1];p.y=spot.y+(p.kind==='weapon'?.7:.55);p.dmDrop=true;p.respawn=0;p.oneShot=true;placed.push(spot);}
  }
  if(placed.length)feed('Arena supply drop: '+placed.length+' item'+(placed.length===1?'':'s')+' spawned around the middle.');
  return placed.length;
}
function updateDeathmatchItemSpawns(dt){
  if(!(game.mode==='dm'||game.mode==='tdm')||!game.started||game.over)return;
  game.dmItemSpawnTimer=(game.dmItemSpawnTimer??15)-dt;
  if(game.dmItemSpawnTimer>0)return;
  game.dmItemSpawnTimer=15;
  if(activeLooseItemCount()>8)return;
  spawnDeathmatchItems(deathmatchPlayerCount());
}
function updateSurvivorItems(dt){if(game.mode!=='survivor')return;game.survivorDropTimer=(game.survivorDropTimer||5)-dt;if(game.survivorDropTimer<=0){spawnSurvivorPickup(false);game.survivorDropTimer=rand(17,26)}}
function repairSurvivorBuilds(percent){let repaired=0;for(const p of [...walls,...platforms])if(p.active!==false&&p.builtBy&&p.hp<p.maxHp){const before=p.hp;p.hp=Math.min(p.maxHp,p.hp+p.maxHp*percent);repaired+=p.hp-before;}if(repaired>0)worldGeometryDirty=true;return repaired}
const SURVIVOR_SHOP_ITEMS=[
  {id:'repair',name:'FORT REPAIR',cost:18,desc:'Repair all active player-built walls and platforms by 35%.',buy(){return repairSurvivorBuilds(.35)>0}},
  {id:'charges',name:'+2 BUILD CAPACITY',cost:28,desc:'Permanently add two maximum build charges (cap +8).',buy(){if((game.survivorBuildBonus||0)>=8)return false;game.survivorBuildBonus=Math.min(8,(game.survivorBuildBonus||0)+2);buildCharges=Math.min(survivorBuildMax(),buildCharges+2);if(player2)player2.buildCharges=Math.min(survivorBuildMax(),(player2.buildCharges||0)+2);return true}},
  {id:'recharge',name:'FASTER FABRICATOR',cost:32,desc:'Permanently improve build recharge speed by 10% (up to 30%).',buy(){if((game.survivorRechargeMult||1)<=.7)return false;game.survivorRechargeMult=Math.max(.7,(game.survivorRechargeMult||1)-.1);return true}},
  {id:'armor',name:'ARMOR PLATES',cost:16,desc:'Give each living survivor 35 armor.',buy(){let changed=false;for(const h of survivorHumans()){const before=h.armor||0;h.armor=Math.min(h.maxArmor||100,before+35);changed=changed||h.armor>before}return changed}},
  {id:'health',name:'MED GEL',cost:14,desc:'Restore 40 health to each living survivor.',buy(){let changed=false;for(const h of survivorHumans()){const before=h.hp;h.hp=Math.min(h.maxHp,h.hp+40);changed=changed||h.hp>before}return changed}},
  {id:'weapon',name:'RANDOM WEAPON DROP',cost:22,desc:'Spawn one random weapon pickup near the arena interior.',buy(){spawnSurvivorPickup(false);return true}},
  {id:'thorns',name:'WALL THORNS',cost:34,desc:'Built walls and platforms retaliate against chewing aliens next wave.',buy(){if(game.thornsQueued)return false;game.thornsQueued=true;return true}}
];
function renderSurvivorShop(){if(game.mode!=='survivor')return;el('shopScrap').textContent='SCRAP '+Math.floor(game.scrap||0);el('shopGrid').innerHTML=SURVIVOR_SHOP_ITEMS.map(item=>'<div class="shopItem"><b>'+item.name+' · '+item.cost+'</b><small>'+item.desc+'</small><button type="button" data-shop="'+item.id+'" '+((game.scrap||0)<item.cost?'disabled':'')+'>BUY</button></div>').join('');for(const button of el('shopGrid').querySelectorAll('button'))button.onclick=()=>buySurvivorShopItem(button.dataset.shop);let gear=el('survivalEquipmentShop');if(!gear){gear=document.createElement('div');gear.id='survivalEquipmentShop';gear.className='survivalEquipmentShop';el('shopGrid').after(gear);}survivalInterface?.renderEquipmentShop(gear,{...survivalEquipment.state(),scrap:game.scrap,onBuy:id=>{if(survivalEquipment.buy(id))renderSurvivorShop();},onSelect:id=>{survivalEquipment.select(id);renderSurvivorShop();}});}
function buySurvivorShopItem(id){if(game.mode!=='survivor'||!game.shopOpen)return;const item=SURVIVOR_SHOP_ITEMS.find(x=>x.id===id);if(!item||(game.scrap||0)<item.cost)return;if(!item.buy()){msg('UPGRADE MAXED OR NOT NEEDED');return}game.scrap-=item.cost;feed('Salvage purchased: '+item.name+'.');renderSurvivorShop()}
function openSurvivorShop(){if(game.mode!=='survivor'||game.over)return;game.shopOpen=true;fireHeld=false;buildHeld=false;buildPreview=null;toggleUpgradeTree(false);el('survivorShop').style.display='flex';renderSurvivorShop();document.exitPointerLock?.()}
function closeSurvivorShop(startWave=false){if(!game.shopOpen)return;game.shopOpen=false;el('survivorShop').style.display='none';updateMobileControlVisibility(true);if(startWave&&game.mode==='survivor'&&!game.over){game.waveDelay=0;game.shopPending=false;if(survivorAliveAliens().length===0)spawnSurvivorWave();if(game.p1Input==='keyboard')try{canvas.requestPointerLock?.()?.catch?.(()=>{})}catch{}}}
el('shopContinue').onclick=()=>closeSurvivorShop(true);
function updateSurvivor(dt){if(game.mode!=='survivor')return;if(!survivorHumans().length){endMatch('The alien swarm wiped out the survivors at wave '+game.survivorWave);return;}if(game.shopOpen)return;game.survivalElapsed=(game.survivalElapsed||0)+dt;updateSurvivalMapEvents(dt);survivalProgress?.addTime(dt);survivalEquipment?.update(dt);bots=bots.filter(b=>!b.alien||!b.dead);updateSurvivorItems(dt);updateBossHazards(dt);const alive=survivorAliveAliens().length;if(!survivorHumans().length){endMatch('The alien swarm wiped out the survivors at wave '+game.survivorWave);return}if(game.waveDelay>0){game.waveDelay-=dt;if(game.waveDelay<=0&&alive===0)spawnSurvivorWave();return}if(game.waveSpawning){game.nextSquadTimer=(game.nextSquadTimer||12)-dt;if((alive===0&&game.waveSpawnIndex<game.survivorOrder.length)||game.nextSquadTimer<=0)spawnSurvivorSquad(alive===0);return}if((game.waveKilled||0)>=(game.waveTotal||0)&&alive===0&&!game.shopPending){const reward=1,clearScrap=10+game.survivorWave*2+(game.survivorWave%5===0?15:0);game.scrap=(game.scrap||0)+clearScrap;game.wallThorns=false;game.shopPending=true;game.survivalCleared=game.survivorWave;const unlocked=survivalProgress?.clearWave(game.survivorWave,{alivePlayers:survivorHumans().length});survivalInterface?.notifyUnlock(unlocked);feed('Wave '+game.survivorWave+' cleared. +'+clearScrap+' scrap and +1 upgrade point each.');msg('WAVE CLEAR · SALVAGE SHOP');player.upgradePoints=(player.upgradePoints||0)+reward;if(player2)player2.upgradePoints=(player2.upgradePoints||0)+reward;if(game.survivorWave>0&&game.survivorWave%3===0)spawnSurvivorHealthDrop();if(Math.random()<.35)spawnSurvivorPickup(true);game.waveDelay=15;game.survivorOrder=null;openSurvivorShop()}}
function nearestHuman(a){let best=null,bd=999;for(const h of survivorHumans()){const d=hypot2(h.x-a.x,h.z-a.z)+Math.abs(h.y-a.y)*.25;if(d<bd){bd=d;best=h}}return {e:best,d:bd}}
function resolveAlienCollision(a,target){if(!target||target.dead)return;const dx=target.x-a.x,dz=target.z-a.z,d=Math.hypot(dx,dz)||.001,min=(a.r||.6)+(target.r||.46)+.08;if(d<min&&Math.abs((target.y-.85)-(a.y-.85))<2.9){const push=(min-d)*.52,nx=dx/d,nz=dz/d;target.x+=nx*push;target.z+=nz*push;a.x-=nx*push*.7;a.z-=nz*push*.7;target.vx+=nx*2.4;target.vz+=nz*2.4}}
function alienMelee(a,target){if(a.fireCd>0||!target||target.dead)return;const dist=hypot2(target.x-a.x,target.z-a.z),vertical=Math.abs((target.y-.9)-(a.y-.85));if(dist<=a.meleeRange&&vertical<2.75){a.attackAnim=.32;a.fireCd=a.meleeCool;const dx=(target.x-a.x)/(dist||1),dz=(target.z-a.z)/(dist||1);target.vx+=dx*5.2;target.vz+=dz*5.2;damage(target,a.alienDamage,a,false,{x:target.x,y:target.y-.75,z:target.z},{bypassArmor:false,armorMult:1.0});burst(a.x+dx*.8,a.y-.95,a.z+dz*.8,a.color,5,.22)}}
function alienShootOrb(a,target){const dx=target.x-a.x,dy=(target.y-.75)-(a.y-.72),dz=target.z-a.z,len=Math.hypot(dx,dy,dz)||1,dir={x:dx/len,y:dy/len,z:dz/len};projectiles.push({owner:a,x:a.x+dir.x*.9,y:a.y-.72+dir.y*.9,z:a.z+dir.z*.9,px:a.x,py:a.y-.72,pz:a.z,vx:dir.x*34,vy:dir.y*34,vz:dir.z*34,life:3.4,color:'#ffe433',damage:a.alienDamage*1.35,r:.58,tier:3,pulse:0});addMuzzleFlash(a,false,dir,'#ffe433',.8);burst(a.x+dir.x*.75,a.y-.72,a.z+dir.z*.75,'#ffe433',10,.35)}
function alienShootGun(a,target){const dx=target.x-a.x,dy=(target.y-.72)-(a.y-.05),dz=target.z-a.z,dist=Math.hypot(dx,dz)||1;a.yaw=Math.atan2(dx,dz)+rand(-.045,.045);a.pitch=Math.atan2(-dy,Math.max(.01,dist))+rand(-.025,.025);a.weapon='smg';a.ammo=999;const oldDamage=WEAPONS.smg.damage;WEAPONS.smg.damage=a.alienDamage;tryFire(a,false,0);WEAPONS.smg.damage=oldDamage;a.pitch=0}

function findChewTargetForAlien(a,h){
  if(!h||h.dead)return null;
  const marked=bossStructures().filter(p=>(p.breachMarked||0)>0).sort((p,q)=>Math.hypot(a.x-p.x,a.z-p.z)-Math.hypot(a.x-q.x,a.z-q.z))[0];
  if(marked&&Math.hypot(a.x-marked.x,a.z-marked.z)<32)return marked;
  const elevated=(h.y-a.y>2.8)||(h.y-topAtPoint(h.x,h.z)>PLAYER_H+1.2);
  if(!elevated)return null;
  let best=null,bd=1e9;
  const hx=h.x,hz=h.z,feet=h.y-PLAYER_H;

  // First priority: chew the wall directly under or beside the elevated survivor.
  for(const block of blocksNear(hx,hz,20)){
    const w=block.structure;
    if(block.kind!=='wall')continue;
    if(w.active===false||!w.destructible||w.survivorEdgeWall)continue;
    const top=w.y+w.h/2;
    if(top>feet+1.2)continue;
    const under=Math.abs(hx-w.x)<=w.w/2+4.8&&Math.abs(hz-w.z)<=w.d/2+4.8;
    const dUnder=Math.hypot(hx-w.x,hz-w.z);
    if(under){
      const d=Math.hypot(a.x-w.x,a.z-w.z)+dUnder*.25+Math.max(0,feet-top)*.12;
      if(d<bd){bd=d;best=w;}
    }
  }
  if(best)return best;

  // Second priority: if the player is on a built platform with no obvious wall beneath, chew the closest destructible support/structure near that footprint.
  const structures=[...new Set(blocksNear(hx,hz,20).map(block=>block.structure))].filter(p=>p?.active!==false&&p?.destructible&&!p?.survivorEdgeWall);
  for(const p of structures){
    const py=p.type==='platform'?p.y+p.h:p.y+p.h/2;
    if(py>feet+1.4)continue;
    const nearPlayer=Math.hypot(hx-p.x,hz-p.z);
    if(nearPlayer>18)continue;
    const d=Math.hypot(a.x-p.x,a.z-p.z)*.45+nearPlayer+Math.max(0,feet-py)*.10;
    if(d<bd){bd=d;best=p;}
  }
  return best;
}
function alienChewStructure(a,target,dt){
  if(!target||target.active===false)return false;
  // Navigate to the nearest real face. A large cargo wall's center can be
  // metres behind its surface and is not a reachable melee destination.
  const faceX=clamp(a.x,target.x-target.w/2,target.x+target.w/2),faceZ=clamp(a.z,target.z-target.d/2,target.z+target.d/2);
  let dx=faceX-a.x,dz=faceZ-a.z,dist=Math.hypot(dx,dz);
  if(dist<.001){dx=target.x-a.x;dz=target.z-a.z;dist=Math.hypot(dx,dz)||1;}
  const nx=dx/dist,nz=dz/dist,attackRange=(a.r||.6)+Math.max(.8,(a.meleeRange||1.6)*.72);
  a.faceYaw=Math.atan2(nx,nz);a.yaw=a.faceYaw;
  if(dist>attackRange){const speed=a.alienSpeed*.88,sep=botSeparation(a);a.vx+=(nx*speed+sep.x*speed*.5-a.vx)*clamp(9*dt,0,1);a.vz+=(nz*speed+sep.z*speed*.5-a.vz)*clamp(9*dt,0,1);}
  else{
    a.vx*=.45;a.vz*=.45;
    const bottom=target.type==='platform'?target.y:target.y-target.h/2,hitY=clamp(a.y-.85,bottom+.1,bottom+target.h-.1);
    if(a.fireCd<=0&&lineClear(a.x,a.y-.65,a.z,faceX-nx*.12,hitY,faceZ-nz*.12)){
      a.attackAnim=.32;a.fireCd=(a.meleeCool||.62)*((target.breachMarked||0)>0?.55:1);
      damageStructure(target,a.alienDamage*3.7*(a.wallDamageMult||1)*((target.breachMarked||0)>0?1.65:1)*(a.bossType==='commander'?1.25:1),a,'alien',{x:faceX,y:hitY,z:faceZ,normal:{x:-nx,y:0,z:-nz}});
      burst(faceX,hitY,faceZ,a.color,5,.2);
    }
  }
  const mv=Math.hypot(a.vx,a.vz);if(mv>.2)a.walk+=mv*dt*5.8;physics(a,dt);return true;
}
function alienNaturalWallContact(a,dt){
  if(!a||!a.alien||a.flying||a.dead)return false;
  const ar=a.r||.6,feet=a.y-PLAYER_H*.92,head=a.y+.28;
  const attackRange=Math.max(1.35,(a.meleeRange||2.35)*.78);
  let best=null,bs=1e9,bhit=null,bnormal={x:0,z:1};

  // No pathfinding conditions here: if an alien is within target range of any destructible wall,
  // it stops and attacks that wall at its normal melee cooldown. This is intentionally blunt so
  // aliens reliably chew walls they are standing against instead of trying to route around them.
  for(const block of blocksNear(a.x,a.z,attackRange+ar+1.5)){
    const w=block.structure;
    if(block.kind!=='wall')continue;
    if(w.active===false||!w.destructible)continue;
    const bottom=w.y-w.h/2,top=w.y+w.h/2;
    if(head<bottom-.35||feet>top+1.15)continue;
    const hx=clamp(a.x,w.x-w.w/2,w.x+w.w/2),hz=clamp(a.z,w.z-w.d/2,w.z+w.d/2);
    const ox=a.x-hx,oz=a.z-hz,edgeDist=Math.hypot(ox,oz);
    const inside=Math.abs(a.x-w.x)<=w.w/2&&Math.abs(a.z-w.z)<=w.d/2;
    const gap=inside?0:Math.max(0,edgeDist-ar);
    if(!inside&&gap>attackRange)continue;
    const wallScore=gap+Math.hypot(a.x-hx,a.z-hz)*.01;
    if(wallScore<bs){
      bs=wallScore;best=w;bhit={x:hx,z:hz};
      const nd=edgeDist||1;
      bnormal={x:edgeDist?ox/nd:Math.sin(a.yaw||0),z:edgeDist?oz/nd:Math.cos(a.yaw||0)};
    }
  }
  if(!best||!bhit)return false;

  const dx=bhit.x-a.x,dz=bhit.z-a.z,d=Math.hypot(dx,dz)||1;
  a.faceYaw=Math.atan2(dx/d,dz/d);a.yaw=a.faceYaw;
  a.vx*=.12;a.vz*=.12;
  a.attackAnim=Math.max(a.attackAnim||0,.18);

  if(a.fireCd<=0){
    a.attackAnim=.32;
    a.fireCd=a.meleeCool||.62;
    const hitY=clamp(a.y-.85,best.y-best.h/2+.15,best.y+best.h/2-.15);
    damageStructure(best,a.alienDamage*3.7,a,'alien',{x:bhit.x,y:hitY,z:bhit.z,normal:{x:bnormal.x,y:0,z:bnormal.z}});
    burst(bhit.x,hitY,bhit.z,a.color,6,.22);
  }
  return true;
}

function alienCutoffPoint(a,h,dist){
  const lead=clamp(dist/Math.max(8,a.alienSpeed||15),.15,1.15),hx=h.x+(h.vx||0)*lead,hz=h.z+(h.vz||0)*lead;
  const allies=survivorAliveAliens().filter(o=>o!==a&&!o.flying&&hypot2(o.x-h.x,o.z-h.z)<22).length;
  if(allies<2)return {x:hx,z:hz};
  const dx=hx-a.x,dz=hz-a.z,d=Math.hypot(dx,dz)||1,side=Math.sign(Math.sin((a.tailPhase||0)+a.x*.07+a.z*.11))||1,spread=clamp(allies*1.15,2.5,7.0);
  return {x:hx+(-dz/d)*side*spread,z:hz+(dx/d)*side*spread};
}
function updateFlyingAlien(a,dt){
  a.spawnGrace=Math.max(0,a.spawnGrace-dt);a.stagger=Math.max(0,(a.stagger||0)-dt);a.fireCd=Math.max(0,a.fireCd-dt);a.attackAnim=Math.max(0,(a.attackAnim||0)-dt);a.tailPhase+=dt*9;if(a.dead)return;
  const decision=survivalController?.acquire(a);a.enemy=decision?.focus||nearestHuman(a).e;if(!a.enemy)return;const h=a.enemy;
  const dx=h.x-a.x,dz=h.z-a.z,dist=Math.hypot(dx,dz)||1;
  if(a.flyState==='circle'){
    a.circleTimer-=dt;a.flyAngle+=(.9+Math.sin(a.tailPhase)*.16)*dt;
    const radius=clamp(15+dist*.18,13,24),alt=10+Math.sin(a.tailPhase*.55)*3.2,tx=h.x+Math.cos(a.flyAngle)*radius,tz=h.z+Math.sin(a.flyAngle)*radius,ty=Math.max(topAtPoint(h.x,h.z)+PLAYER_H+alt,10);
    const ddx=tx-a.x,ddy=ty-a.y,ddz=tz-a.z,len=Math.hypot(ddx,ddy,ddz)||1,speed=a.alienSpeed*.82;
    a.vx+=(ddx/len*speed-a.vx)*clamp(4.2*dt,0,1);a.vy+=(ddy/len*speed-a.vy)*clamp(3.0*dt,0,1);a.vz+=(ddz/len*speed-a.vz)*clamp(4.2*dt,0,1);
    a.faceYaw=Math.atan2(a.vx,a.vz);a.yaw=a.faceYaw;
    if((!decision||decision.visible)&&a.circleTimer<=0&&dist<44&&lineClear(a.x,a.y,a.z,h.x,h.y-.7,h.z)){
      const aim={x:h.x+(h.vx||0)*.38,y:h.y-.75,z:h.z+(h.vz||0)*.38},vx=aim.x-a.x,vy=aim.y-a.y,vz=aim.z-a.z,l=Math.hypot(vx,vy,vz)||1;
      a.diveDir={x:vx/l,y:vy/l,z:vz/l};a.flyState='dive';a.diveTimer=1.25;a.attackAnim=.55;feed('A pterodactyl dives — shoot it down!');
    }
  }else{
    a.diveTimer-=dt;const dir=a.diveDir||{x:dx/dist,y:-.2,z:dz/dist},speed=a.alienSpeed*2.35;
    const steer=.9*dt,aim={x:h.x-a.x,y:(h.y-.85)-a.y,z:h.z-a.z},l=Math.hypot(aim.x,aim.y,aim.z)||1;
    dir.x+=(aim.x/l-dir.x)*steer;dir.y+=(aim.y/l-dir.y)*steer*.45;dir.z+=(aim.z/l-dir.z)*steer;const dl=Math.hypot(dir.x,dir.y,dir.z)||1;dir.x/=dl;dir.y/=dl;dir.z/=dl;a.diveDir=dir;
    a.vx=dir.x*speed;a.vy=dir.y*speed;a.vz=dir.z*speed;a.faceYaw=Math.atan2(dir.x,dir.z);a.yaw=a.faceYaw;
    if((!decision||decision.visible)&&dist<1.8&&Math.abs(a.y-(h.y-.8))<2.4&&a.fireCd<=0){a.fireCd=a.meleeCool||1.35;a.attackAnim=.5;damage(h,a.alienDamage,a,false,{x:h.x,y:h.y-.7,z:h.z},{bypassArmor:false});h.vx+=dir.x*7;h.vz+=dir.z*7;h.vy=Math.max(h.vy,6);burst(h.x,h.y-.7,h.z,a.color,10,.35);a.flyState='recover';a.diveTimer=.55}
    if(a.diveTimer<=0||a.y<2.6||Math.abs(a.x)>ARENA_SAFE-2||Math.abs(a.z)>ARENA_SAFE-2){a.flyState='recover';a.diveTimer=.7}
  }
  if(a.flyState==='recover'){
    a.diveTimer-=dt;const awayX=a.x-h.x,awayZ=a.z-h.z,ad=Math.hypot(awayX,awayZ)||1,ty=Math.max(topAtPoint(a.x,a.z)+PLAYER_H+13,12);
    a.vx+=(awayX/ad*a.alienSpeed*.9-a.vx)*clamp(4*dt,0,1);a.vz+=(awayZ/ad*a.alienSpeed*.9-a.vz)*clamp(4*dt,0,1);a.vy+=((ty-a.y)*2.0-a.vy)*clamp(3*dt,0,1);a.faceYaw=Math.atan2(a.vx,a.vz);a.yaw=a.faceYaw;
    if(a.diveTimer<=0){a.flyState='circle';a.circleTimer=rand(2.4,4.4);a.flyAngle=Math.atan2(a.x-h.x,a.z-h.z)}
  }
  const detour=survivalController?.flightWaypoint?.(a,{x:a.x+a.vx*.4,y:a.y+a.vy*.4,z:a.z+a.vz*.4});
  if(detour){const dx=detour.x-a.x,dy=detour.y-a.y,dz=detour.z-a.z,length=Math.hypot(dx,dy,dz),speed=a.alienSpeed*.78;a.vx=length>0?dx/length*speed:0;a.vy=length>0?dy/length*speed:0;a.vz=length>0?dz/length*speed:0;if(a.flyState==='dive'){a.flyState='recover';a.diveTimer=.7;}}
  const steps=Math.max(1,Math.ceil(Math.hypot(a.vx,a.vy,a.vz)*dt/.45)),step=dt/steps;for(let i=0;i<steps;i++){const oldX=a.x,oldY=a.y,oldZ=a.z;a.x+=a.vx*step;a.y+=a.vy*step;a.z+=a.vz*step;verticalCollide(a,oldY,oldX,oldZ,false);pushOutXZ(a);a.onGround=false;if(a.flyState==='dive'&&Math.hypot(a.x-oldX,a.y-oldY,a.z-oldZ)<.05){a.flyState='recover';a.diveTimer=.7;}}a.x=clamp(a.x,-ARENA_SAFE+1,ARENA_SAFE-1);a.z=clamp(a.z,-ARENA_SAFE+1,ARENA_SAFE-1);a.y=clamp(a.y,3.2,32);a.walk+=dt*8;
}
function findClimbWall(a,h){
  if(!a.climber||!h||h.dead||h.y-a.y<2.4||(game.survivalMap==='breach_ring'&&!insideSurvivorRing(a)))return null;
  let best=null,score=1e9;
  for(const block of blocksNear(a.x,a.z,11)){
    const w=block.structure;if(block.kind!=='wall'||w.active===false||!w.destructible||w.survivorEdgeWall)continue;
    const top=w.y+w.h/2;if(top<a.y-.5||top>h.y+2.5)continue;
    const nearHuman=Math.max(Math.abs(h.x-w.x)-w.w/2,Math.abs(h.z-w.z)-w.d/2);if(nearHuman>5.5)continue;
    const dx=Math.max(0,Math.abs(a.x-w.x)-w.w/2),dz=Math.max(0,Math.abs(a.z-w.z)-w.d/2),d=Math.hypot(dx,dz),s=d+nearHuman*.35;
    if(s<score&&d<10){score=s;best=w}
  }
  return best;
}
function updateClimberMovement(a,dt){const h=a.enemy;if(!a.climber||!h)return false;a.climbThink=(a.climbThink||0)-dt;if(a.climbThink<=0){a.climbThink=rand(.28,.48);a.climbTarget=findClimbWall(a,h);if(!a.climbTarget)a.climbing=false}const w=a.climbTarget;if(!w||w.active===false)return false;const top=w.y+w.h/2,ox=a.x-w.x,oz=a.z-w.z,alongX=w.w>=w.d,targetX=alongX?clamp(a.x,w.x-w.w/2+.35,w.x+w.w/2-.35):w.x+Math.sign(ox||1)*(w.w/2+a.r*.35),targetZ=alongX?w.z+Math.sign(oz||1)*(w.d/2+a.r*.35):clamp(a.z,w.z-w.d/2+.35,w.z+w.d/2-.35),d=Math.hypot(targetX-a.x,targetZ-a.z);if(!a.climbing){if(d>1.2){const nx=(targetX-a.x)/(d||1),nz=(targetZ-a.z)/(d||1),speed=a.alienSpeed*.72;a.vx+=(nx*speed-a.vx)*clamp(8*dt,0,1);a.vz+=(nz*speed-a.vz)*clamp(8*dt,0,1);a.faceYaw=Math.atan2(nx,nz);physics(a,dt);return true}a.climbing=true;a.vx=a.vz=0;a.vy=0}if(a.climbing){const climbSpeed=a.boss?4.6:3.25;a.x+=(targetX-a.x)*clamp(8*dt,0,1);a.z+=(targetZ-a.z)*clamp(8*dt,0,1);a.y=Math.min(top+PLAYER_H,a.y+climbSpeed*dt);a.walk+=dt*4;a.onGround=false;a.faceYaw=Math.atan2(h.x-a.x,h.z-a.z);if(a.y>=top+PLAYER_H-.12||h.y<a.y-1.2){a.climbing=false;a.climbTarget=null;a.onGround=true;a.y=Math.max(a.y,top+PLAYER_H);a.vx=Math.sin(a.faceYaw)*2.5;a.vz=Math.cos(a.faceYaw)*2.5}return true}return false}
function bossStructures(){return [...walls,...platforms].filter(p=>p.active!==false&&p.destructible&&p.builtBy)}
function strongestBossWall(a){if(survivalController)return a.tacticalBreachTarget?.active!==false?a.tacticalBreachTarget:null;let best=null,score=-1;for(const p of bossStructures()){const d=Math.hypot(p.x-a.x,p.z-a.z),cluster=bossStructures().filter(q=>Math.hypot(q.x-p.x,q.z-p.z)<8).length,s=(p.hp+(p.bonusHp||0))*(1+cluster*.18)-d*8;if(s>score){score=s;best=p}}return best}
function addBossWarning(x,z,type,color,life=1.5,r=3){game.bossWarnings??=[];if(game.bossWarnings.length>=32)game.bossWarnings.shift();game.bossWarnings.push({x,z,type,color,life,maxLife:life,r,angle:0});}
function spawnAcidZone(a,x,z){game.acidZones??=[];const y=topAtPoint(x,z);if(game.acidZones.length>=12)game.acidZones.shift();game.acidZones.push({x,z,y,life:7,maxLife:7,r:3.7,tick:0,owner:a});addBossWarning(x,z,'acid','#b8ff38',7,3.7);burst(x,y+.2,z,'#caff38',18,.55)}
function markCommanderWalls(a){const choices=bossStructures().sort((p,q)=>Math.hypot(p.x-a.x,p.z-a.z)-Math.hypot(q.x-a.x,q.z-a.z)).slice(0,3);for(const p of choices)p.breachMarked=7;feed('BREACH COMMANDER MARKED THE WALLS!');msg('BREACH COMMANDER MARKED THE WALLS!');for(const p of choices)addBossWarning(p.x,p.z,'mark','#ff304c',7,Math.max(2,Math.min(p.w,p.d)+1))}
function updateMatriarchBoss(a,dt){if(a.bossType!=='matriarch')return;a.broodTimer-=dt;if(a.broodTimer>0||a.broodSpawned>=12||survivorAliveAliens().length>ALIEN_MAX_ALIVE-3)return;a.broodTimer=rand(5,7);const count=Math.min(3,12-a.broodSpawned);for(let i=0;i<count;i++){const m=makeAlien('climber',game.survivorWave);m.name='MATRON CLIMBER';m.hp*=.72;m.maxHp=m.hp;m.alienScale*=.78;m.r*=.78;m.x=a.x+rand(-2.2,2.2);m.z=a.z+rand(-2.2,2.2);m.y=topAtPoint(m.x,m.z)+PLAYER_H;m.onGround=true;bots.push(m);a.broodSpawned++;game.waveTotal++;game.waveSpawned++;burst(m.x,m.y-.8,m.z,'#b34cff',6,.3)}feed('The Climber Matriarch screeches and calls climbers!');msg('MATRIARCH SCREECH')}
function updateBossSpecial(a,dt){
  if(!a.boss)return false;a.bossTimer=(a.bossTimer||0)-dt;
  if(a.bossType==='matriarch'){updateMatriarchBoss(a,dt);if(a.bossTimer<=0&&a.enemy&&a.enemy.y>a.y+2){a.bossTimer=rand(3.5,5);a.vy=18;a.vx=(a.enemy.x-a.x)*1.4;a.vz=(a.enemy.z-a.z)*1.4;addBossWarning(a.enemy.x,a.enemy.z,'leap','#b34cff',1.1,3);feed('Climber Matriarch screeches before leaping!')}}
  if(a.bossType==='spitter'&&a.bossTimer<=0&&a.enemy){a.bossTimer=rand(3.6,5.0);const t=a.enemy,lead=.55,x=clamp(t.x+(t.vx||0)*lead+rand(-2,2),-ARENA_SAFE+2,ARENA_SAFE-2),z=clamp(t.z+(t.vz||0)*lead+rand(-2,2),-ARENA_SAFE+2,ARENA_SAFE-2);a.spitTarget={x,z,t:1.05};addBossWarning(x,z,'acidArc','#d7ff38',1.15,3.7);feed('Spitter Titan lobs acid over the fort!')}
  if(a.spitTarget){a.spitTarget.t-=dt;if(a.spitTarget.t<=0){spawnAcidZone(a,a.spitTarget.x,a.spitTarget.z);a.spitTarget=null}}
  if(a.bossType==='commander'&&a.bossTimer<=0){a.bossTimer=rand(6,8);markCommanderWalls(a)}
  if(a.bossType==='burrower'){
    if(a.bossState==='hunt'&&a.bossTimer<=0&&a.enemy){a.bossState='burrow';a.bossTimer=1.5;a.burrowed=true;a.bossTarget={x:a.enemy.x,z:a.enemy.z};addBossWarning(a.bossTarget.x,a.bossTarget.z,'crack','#d29a55',1.5,4.5);feed('Cracked ground — the Burrower is tunneling inside!')}
    if(a.bossState==='burrow'){a.vx=a.vz=0;if(a.bossTarget){a.x+=(a.bossTarget.x-a.x)*clamp(3.5*dt,0,1);a.z+=(a.bossTarget.z-a.z)*clamp(3.5*dt,0,1)}if(a.bossTimer<=0){a.burrowed=false;a.bossState='hunt';a.bossTimer=rand(5,7);a.y=topAtPoint(a.x,a.z)+PLAYER_H;damageStructuresInRadius(a.x,a.y-PLAYER_H,a.z,6.5,220+game.survivorWave*7,a,'burrow');for(const h of survivorHumans())if(Math.hypot(h.x-a.x,h.z-a.z)<5.2)damage(h,a.alienDamage*1.35,a,false,{x:h.x,y:h.y-.8,z:h.z});burst(a.x,a.y-1,a.z,'#d29a55',30,.8);addBossWarning(a.x,a.z,'impact','#ffb45c',.7,6.5)}return true}
  }
  if(a.bossType==='rammer'){
    if(a.bossState==='stunned'){a.vx*=.75;a.vz*=.75;physics(a,dt);if(a.bossTimer<=0){a.bossState='hunt';a.bossTimer=rand(3.5,5)}return true}
    if(a.bossState==='hunt'&&a.bossTimer<=0){const target=strongestBossWall(a)||a.enemy;if(target){a.bossTarget=target;const dx=target.x-a.x,dz=target.z-a.z,d=Math.hypot(dx,dz)||1;a.chargeDir={x:dx/d,z:dz/d};a.faceYaw=Math.atan2(a.chargeDir.x,a.chargeDir.z);a.bossState='windup';a.bossTimer=1.15;addBossWarning(a.x+a.chargeDir.x*10,a.z+a.chargeDir.z*10,'charge','#ff5a20',1.2,10);feed('Rammer backs up — clear the orange charge lane!')}}
    if(a.bossState==='windup'){a.vx=-a.chargeDir.x*3.2;a.vz=-a.chargeDir.z*3.2;physics(a,dt);if(a.bossTimer<=0){a.bossState='charge';a.bossTimer=1.25}return true}
    if(a.bossState==='charge'){a.vx=a.chargeDir.x*31;a.vz=a.chargeDir.z*31;a.walk+=dt*22;physics(a,dt);for(const h of survivorHumans())if(Math.hypot(h.x-a.x,h.z-a.z)<a.r+(h.r||.5)&&Math.abs(h.y-a.y)<3&&lineClear(a.x,a.y-.5,a.z,h.x,h.y-.7,h.z)){damage(h,a.alienDamage*1.8,a,false,{x:h.x,y:h.y-.8,z:h.z});h.vx+=a.chargeDir.x*18;h.vz+=a.chargeDir.z*18;a.bossState='hunt';a.bossTimer=rand(4,5.5);burst(h.x,h.y-.8,h.z,'#ff6b2b',18,.5);return true}for(const p of [...walls,...platforms])if(p.active!==false&&p.destructible&&Math.abs(a.x-p.x)<p.w/2+a.r&&Math.abs(a.z-p.z)<p.d/2+a.r&&a.y>pieceBottomY(p)&&a.y-PLAYER_H<pieceTopY(p)){const bonusBefore=p.bonusHp||0,boosted=bonusBefore>0,stunsRammer=bonusBefore>=300;damageStructure(p,boosted?330+game.survivorWave*6:520+game.survivorWave*10,a,'alien');burst(a.x,a.y-1,a.z,'#ff8b32',26,.75);addBossWarning(a.x,a.z,'impact','#ff6b2b',.65,6);a.bossState=stunsRammer?'stunned':'hunt';a.bossTimer=stunsRammer?2.1:rand(4.5,6);a.stagger=stunsRammer?2.1:.25;if(stunsRammer){feed('A 300+ Armor Boost stunned the Rammer!');msg('RAMMER STUNNED')}return true}if(a.bossTimer<=0){a.bossState='hunt';a.bossTimer=rand(4,5.5)}return true}
  }
  return false
}
function updateBossHazards(dt){
  game.bossWarnings=(game.bossWarnings||[]).filter(w=>(w.life-=dt)>0);for(const p of [...walls,...platforms])p.breachMarked=Math.max(0,(p.breachMarked||0)-dt);
  for(const z of game.acidZones||[]){z.life-=dt;z.tick-=dt;if(z.tick<=0){z.tick=.55;for(const h of survivorHumans())if(Math.hypot(h.x-z.x,h.z-z.z)<z.r&&Math.abs((h.y-PLAYER_H)-z.y)<2.4&&lineClear(z.x,z.y+.35,z.z,h.x,h.y-.8,h.z))damage(h,9,z.owner,false,{x:h.x,y:h.y-.8,z:h.z},{minimalFx:true});for(const p of [...walls,...platforms])if(p.active!==false&&Math.hypot(p.x-z.x,p.z-z.z)<z.r+Math.max(p.w,p.d)*.35)damageStructure(p,22,z.owner,'acid')}}game.acidZones=(game.acidZones||[]).filter(z=>z.life>0)
}
function updateAlien(a,dt){
  if(game.shopOpen)return;
  if(a.flying)return updateFlyingAlien(a,dt);
  a.spawnGrace=Math.max(0,a.spawnGrace-dt);a.stagger=Math.max(0,(a.stagger||0)-dt);a.fireCd=Math.max(0,a.fireCd-dt);a.cool=Math.max(0,(a.cool||0)-dt);a.reload=Math.max(0,(a.reload||0)-dt);a.attackAnim=Math.max(0,a.attackAnim-dt);a.shieldFlash=Math.max(0,(a.shieldFlash||0)-dt);a.chargeTimer=Math.max(0,(a.chargeTimer||0)-dt);a.tailPhase+=dt*7;
  if(a.dead)return;
  const decision=survivalController?.acquire(a);a.enemy=decision?.focus||nearestHuman(a).e;if(!a.enemy)return;
  if(survivalController?.updateSpecial(a,dt,decision))return;
  if(a.bossType!=='burrower'&&(!(a.empUntil>(game.survivalElapsed||0)))&&(!decision||decision.visible||decision.memory&&(game.survivalElapsed||0)-decision.memory.time<3)&&updateBossSpecial(a,dt))return;

  const rawDx=a.enemy.x-a.x,rawDz=a.enemy.z-a.z,dist=Math.hypot(rawDx,rawDz)||1;
  if(updateClimberMovement(a,dt))return;
  a.survivalChewTarget=decision?.chew||null;
  const breachRoute=survivorBreachRoute(a,a.enemy);if(decision?.chew){alienChewStructure(a,decision.chew,dt);return;}
  if(breachRoute?.chew){alienChewStructure(a,breachRoute.chew,dt);return;}

  if(game.mode==='survivor'&&game.survivalMap==='breach_ring'&&!insideSurvivorRing(a)){
    const last=a.lastBreachPos||{x:a.x,z:a.z},moved=Math.hypot(a.x-last.x,a.z-last.z);
    a.breachStuckTime=moved<.08?(a.breachStuckTime||0)+dt:0;
    if(moved>=.08)a.lastBreachPos={x:a.x,z:a.z};
    if(a.breachStuckTime>.85){const forced=survivorBreachTarget(a);if(forced){alienChewStructure(a,forced,dt);return;}}
  }else{a.breachStuckTime=0;a.lastBreachPos=null;}

  let moveGoal=breachRoute?.waypoint||decision?.goal||alienCutoffPoint(a,a.enemy,dist);
  const dx=moveGoal.x-a.x,dz=moveGoal.z-a.z,cutDist=Math.hypot(dx,dz)||1,nx=dx/cutDist,nz=dz/cutDist;
  a.faceYaw=Math.atan2(rawDx,rawDz);a.yaw=a.faceYaw;

  const chew=decision?null:findChewTargetForAlien(a,a.enemy);
  const canSee=(!decision||decision.visible)&&!(a.empUntil>(game.survivalElapsed||0))&&lineClear(a.x,a.y-.65,a.z,a.enemy.x,a.enemy.y-.8,a.enemy.z);
  if(chew&&(!a.rangedGun)&&(!canSee||a.enemy.y-a.y>7.5)){alienChewStructure(a,chew,dt);return}
  const shouldShoot=a.ranged&&dist>10&&dist<44&&canSee,shouldGun=a.rangedGun&&dist>8&&dist<46&&canSee;
  let speed=a.alienSpeed*(1-(a.slowAmount||0))*(decision?.speedMultiplier||1);
  if(shouldShoot&&a.fireCd<=0){a.chargeTimer=1.05;a.chargeFired=false;a.fireCd=rand(2.25,3.15);burst(a.x,a.y-.7,a.z,'#ffe433',14,.42)}
  if(shouldGun&&a.fireCd<=0){alienShootGun(a,a.enemy);a.fireCd=rand(.16,.30)}
  if(a.chargeTimer>0){speed=0;if((a.glowPulse||0)<=0){burst(a.x,a.y-.72,a.z,'#fff05a',8,.18);a.glowPulse=.18}else a.glowPulse-=dt;if(canSee&&a.chargeTimer<.08&&!a.chargeFired){a.chargeFired=true;alienShootOrb(a,a.enemy)}}
  let rangeMove=1;
  if(decision)rangeMove=cutDist<1.2?0:1;
  else if(breachRoute?.waypoint)rangeMove=1;
  else if(a.ranged||a.rangedGun){const ideal=a.idealRange||18;if(dist<ideal-4)rangeMove=-.82;else if(dist>ideal+5)rangeMove=.72;else rangeMove=.02}
  else if(dist<a.meleeRange*.72)rangeMove=.2;
  let wantX=nx*speed*rangeMove,wantZ=nz*speed*rangeMove;
  if(!decision&&a.smartPathing&&!canSee&&dist<18&&!breachRoute?.waypoint){const side=Math.sign(Math.sin((a.tailPhase||0)+a.x*.13+a.z*.17))||1;wantX+=-nz*speed*.55*side;wantZ+=nx*speed*.55*side}
  const sep=botSeparation(a);wantX+=sep.x*speed*1.3;wantZ+=sep.z*speed*1.3;
  if(!decision&&(a.ranged||a.rangedGun)&&Math.abs(dist-(a.idealRange||18))<8&&!breachRoute?.waypoint){wantX+=-nz*a.strafe*speed*(a.rangedGun?.55:.82);wantZ+=nx*a.strafe*speed*(a.rangedGun?.55:.82)}
  a.vx+=(wantX-a.vx)*clamp(9*dt,0,1);a.vz+=(wantZ-a.vz)*clamp(9*dt,0,1);
  const mv=Math.hypot(a.vx,a.vz);if(mv>.2)a.walk+=mv*dt*5.8;
  const aheadX=a.x+nx*1.2,aheadZ=a.z+nz*1.2,gHere=topAtPoint(a.x,a.z),gAhead=topAtPoint(a.x+nx*2.2,a.z+nz*2.2),nearWall=blocksNear(aheadX,aheadZ,(a.r||.6)+1).some(block=>{const w=block.structure;return block.kind==='wall'&&w?.active!==false&&Math.abs(aheadX-w.x)<w.w/2+a.r+.35&&Math.abs(aheadZ-w.z)<w.d/2+a.r+.35&&a.y<w.y+w.h/2+1.1});
  if(alienNaturalWallContact(a,dt)){physics(a,dt);return}
  a.jumpCd=(a.jumpCd||0)-dt;if(a.onGround&&a.jumpCd<=0&&(!a.climber||a.enemy.y<=a.y+2.0)&&(gAhead>gHere+.4||nearWall||a.enemy.y>a.y+2.0)){a.vy=rand(a.jumpMin||21.0,a.jumpMax||25.5);a.jumpCd=rand(.55,1.05)}
  physics(a,dt);
  // Post-physics contact pass: if collision resolution leaves an alien pressed against a wall,
  // let it chew at the normal melee rate instead of waiting for pathfinding to pick a target.
  if(alienNaturalWallContact(a,dt))return;
  if(!decision||decision.visible)resolveAlienCollision(a,a.enemy);if(canSee&&!a.rangedGun&&!a.ranged)alienMelee(a,decision?.target||a.enemy)
}

function rechargeBotBuilds(b,dt){
  if(!b||b.human||b.alien)return;
  if(b.buildCharges==null)b.buildCharges=BUILD_MAX;
  if(b.buildCharges<BUILD_MAX){b.buildRecharge=(b.buildRecharge||0)+dt;if(b.buildRecharge>=BUILD_RECHARGE){b.buildRecharge=0;b.buildCharges=Math.min(BUILD_MAX,b.buildCharges+1)}}
}
function botCandidateScore(c,b,enemy){
  if(!c||!enemy)return -999;
  const de=Math.hypot(c.x-enemy.x,c.z-enemy.z),db=Math.hypot(c.x-b.x,c.z-b.z),cfg=DIFF[game.difficulty]||DIFF.normal;
  let score=0;
  if(c.type==='wall'){score+=28-Math.abs(de-9)*1.2;score+=24-db;score+=cfg.strategy*8;}
  else{score+=22-Math.abs(de-18);score+=18-db*.7;score+=cfg.strategy*12;}
  if(lineClear(b.x,b.y-.1,b.z,enemy.x,enemy.y-.55,enemy.z))score-=c.type==='wall'?4:-4;
  return score+rand(-2,2);
}
function tryBotBuildMove(b,dt){
  if(game.mode!=='frag'||b.human||b.alien||b.dead||!b.enemy)return;
  rechargeBotBuilds(b,dt);
  b.fragBuildCd=(b.fragBuildCd||0)-dt;
  const cfg=DIFF[game.difficulty]||DIFF.normal;
  if(b.fragBuildCd>0||b.buildCharges<=0)return;
  const enemy=b.enemy,dist=hypot2(enemy.x-b.x,enemy.z-b.z),skill=cfg.strategy||1;
  if(Math.random()>.42*skill && dist>18)return;
  const dx=enemy.x-b.x,dz=enemy.z-b.z,d=Math.hypot(dx,dz)||1,nx=dx/d,nz=dz/d,sx=-nz,sz=nx,gy=heightToBuildLayer(Math.max(0,topAtPoint(b.x,b.z)));
  const options=[];
  const makeWall=(x,z,orient)=>{const c=wallFromGrid(Math.round(x/gridSize),gy,Math.round(z/gridSize),orient,{face:'bot-frag'});c.distance=Math.hypot(c.x-b.x,c.y-b.y,c.z-b.z);return c};
  const makePlat=(x,z)=>{const c=platformFromGrid(Math.round(x/gridSize),gy+1,Math.round(z/gridSize),{face:'bot-frag'});c.distance=Math.hypot(c.x-b.x,c.y-b.y,c.z-b.z);return c};
  // Easy: occasional panic wall. Normal: side wall/peek. Hard+: wall plus platform jumps. Nightmare: flank/height combinations.
  options.push(makeWall(b.x+nx*2.1,b.z+nz*2.1,Math.abs(nx)>Math.abs(nz)?'z':'x'));
  if(skill>.7){options.push(makeWall(b.x+sx*2.4,b.z+sz*2.4,Math.abs(sx)>Math.abs(sz)?'z':'x'));options.push(makeWall(b.x-sx*2.4,b.z-sz*2.4,Math.abs(sx)>Math.abs(sz)?'z':'x'));}
  if(skill>1.0){options.push(makePlat(b.x-sx*3.5,b.z-sz*3.5));options.push(makePlat(b.x+sx*3.5,b.z+sz*3.5));}
  if(skill>1.25){options.push(makeWall(b.x-nx*2.3+sx*2.5,b.z-nz*2.3+sz*2.5,Math.abs(nx)>Math.abs(nz)?'z':'x'));}
  let best=null,bs=-999;
  for(const c of options){b.buildType=c.type;const old=buildActor;buildActor=b;const support=supportForCandidate(c);if(support&&support!==true)c.target=support;const v=validateBuildPlacement(c);buildActor=old;if(v.valid){const sc=botCandidateScore(c,b,enemy);if(sc>bs){bs=sc;best=c}}}
  if(best){b.buildType=best.type;placeBuildFor(b,best);if(skill>1.0&&b.onGround&&best.type==='platform'&&Math.random()<.65){b.vy=22;b.jumpCd=.8}b.fragBuildCd=rand(skill>1.15?1.4:2.0,skill>1.15?2.6:4.2);}
  else b.fragBuildCd=rand(.8,1.6);
}

// Test navigation against the same compound collision blocks used by movement and bullets.
function tacticalPointInfo(x,z,actor={}){
  const reference=(actor.y??PLAYER_H)-PLAYER_H,r=actor.r||.52;
  let floor=-Infinity;
  for(const block of blocksNear(x,z,r+.2)){
    const top=block.y+block.h/2;
    if(block.kind==='platform'&&!block.structure?.rampCollision&&top<=reference+.7&&Math.abs(x-block.x)<=block.w/2&&Math.abs(z-block.z)<=block.d/2)floor=Math.max(floor,top);
  }
  const ramp=game.mode==='survivor'?survivalMapRuntime?.rampHeight(x,z):arenaRampHeight(x,z);
  if(Number.isFinite(ramp)&&ramp<=reference+.9)floor=Math.max(floor,ramp);
  if(!Number.isFinite(floor)||Math.abs(x)>ARENA_SAFE-r||Math.abs(z)>ARENA_SAFE-r)return {walkable:false,height:floor};
  const body=actorStandingHeight(actor);
  const obstacle=blocksNear(x,z,r+.2).find(b=>{
    const top=b.y+b.h/2,step=b.structure?.rampCollision ? .24 : .20;
    if(b.kind==='platform'&&top-floor<=step)return false;
    return floor<top-.1&&floor+body>b.y-b.h/2+.08&&actorOverSurface(x,z,b,r+.06);
  });
  return {walkable:!obstacle,height:floor,onRamp:Number.isFinite(ramp)&&Math.abs(floor-ramp)<.15,obstacle:obstacle?.structure||null};
}
function tacticalCanTraverse(from,to,actor){
  const distance=Math.hypot(to.x-from.x,to.z-from.z),steps=Math.max(1,Math.ceil(distance/.65));
  let previous=from.y-PLAYER_H;
  for(let i=1;i<=steps;i++){
    const t=i/steps,x=from.x+(to.x-from.x)*t,z=from.z+(to.z-from.z)*t,expected=from.y+(to.y-from.y)*t;
    const info=tacticalPointInfo(x,z,{...actor,y:expected});
    if(!info.walkable||Math.abs(info.height-previous)>.72||info.height-previous>.22&&!info.onRamp)return false;
    previous=info.height;
  }
  return Math.abs(previous-(to.y-PLAYER_H))<.8;
}
function humanBotController(){
  if(tacticalController)return tacticalController;
  tacticalController=window.NeonTacticalAI?.createController({
    actors:()=>[...new Set([player,player2,...bots].filter(Boolean))],sameTeam,lineClear,
    eye:a=>a.entityId||a.human||a.style?actorEyePosition(a):a,
    aimHeight:a=>a.entityId||a.human||a.style?humanTargetY(a,'body'):a.y-.55,
    difficulty:()=>game.difficulty,time:()=>warMode()?warClock:performance.now()/1000,navigation:()=>nav,
    objective:warObjective,patrolRoute:a=>a.patrolRoute,onDetection:warDetection,visionRange:a=>game.mode==='campaign'?42:72,
    revision:()=>blocks().length+':'+structureId,pointInfo:tacticalPointInfo,canTraverse:tacticalCanTraverse,
    coverBlocks:a=>blocksNear(a.x,a.z,24),idealRange:a=>aiProfile(a.weapon).ideal,
    pickupGoal:a=>{const found=nearestPickup(a),p=found.p;if(!p||found.d>46)return null;const info=tacticalPointInfo(p.x,p.z,{...a,y:p.y||a.y});return info.walkable?{x:p.x,y:info.height+PLAYER_H,z:p.z}:null;}
  });
  return tacticalController;
}
function updateBot(b,dt){
  if(b.alien)return updateAlien(b,dt);if(b.human)return;
  b.spawnGrace=Math.max(0,b.spawnGrace-dt);b.slowTimer=Math.max(0,(b.slowTimer||0)-dt);b.stagger=Math.max(0,(b.stagger||0)-dt);if(b.slowTimer<=0)b.slowAmount=0;
  if(b.dead){tacticalController?.reset(b);return;}
  const cfg=DIFF[game.difficulty]||DIFF.normal,controller=humanBotController();if(!controller)return;
  updateWeaponTimers(b,dt);updateMortarTarget(b,dt);pickupCheck(b);rechargeBotBuilds(b,dt);b.fireCd=Math.max(0,(b.fireCd||0)-dt);
  const decision=controller.update(b,dt),speed=(decision.wantCrouch?.52:1)*cfg.move*(b.speedMult||1)*decision.speedMultiplier*(1-(b.slowAmount||0))*(b.stagger>0?.42:1);
  b.crouching=!!decision.wantCrouch;b.guardState=decision.guardState;b.suspicion=decision.suspicion;b.enemy=decision.enemy;b.enemyDist=b.enemy?hypot2(b.enemy.x-b.x,b.enemy.z-b.z):Infinity;
  b.target=decision.goal;b.route=decision.waypoint;b.faceYaw=decision.yaw;b.yaw=decision.yaw;b.pitch=decision.pitch;
  let wantX=decision.move.x*speed,wantZ=decision.move.z*speed;
  const sep=botSeparation(b);wantX+=sep.x*speed*.55;wantZ+=sep.z*speed*.55;
  if(decision.wantSlide)requestHumanSlide(b);
  if(!updateTacticalSlide(b,dt,wantX,wantZ,speed)){b.vx+=(wantX-b.vx)*clamp(10*dt,0,1);b.vz+=(wantZ-b.vz)*clamp(10*dt,0,1);}
  const mv=Math.hypot(b.vx,b.vz);if(mv>.2)b.walk+=mv*dt*4.2;
  tryBotBuildMove(b,dt);physics(b,dt);updateSlideView(b,dt);
  if(!b.enemy)tryBotHarpoonGrapple(b,dt);
  const w=WEAPONS[b.weapon];
  if(b.reload<=0&&w.kind!=='plasma'&&w.kind!=='laser'&&b.ammo>0&&b.ammo<=Math.max(1,w.mag*.22)&&(!b.enemy||decision.tactic==='cover'||decision.tactic==='retreat'))manualReload(b);
  // No weapon bypasses perception. Mortars can lob over geometry only after a real sighting.
  if(decision.aim?.fire&&b.enemy&&!b.mortarActive&&b.reload<=0&&b.fireCd<=0&&b.cool<=0){
    const origin=actorEyePosition(b),targetY=humanTargetY(b.enemy,'body');
    if(lineClear(origin.x,origin.y-.05,origin.z,b.enemy.x,targetY,b.enemy.z)){
      if(b.weapon==='mortar'){
        if(b.enemyDist<15)return;
        const lead=.18;b.botMortarPoint={x:b.enemy.x+(b.enemy.vx||0)*lead,z:b.enemy.z+(b.enemy.vz||0)*lead,count:1};
      }
      const before=b.cool;tryFire(b,false,b.weapon==='plasma'?rand(.35,.82):0);
      if(b.cool>before){controller.recordShot(b);b.fireCd=Math.max(.045,w.cool*1.06)/(b.fireTempo||1);}
    }
  }
}
function updateProjectiles(dt){for(const p of projectiles){if(p.exploded)continue;if(p.kind==='mortarDrop'){p.life-=dt;p.pulse=(p.pulse||0)+dt;const correction=clamp(dt*4.2,0,1);p.x+=(p.targetX-p.x)*correction;p.z+=(p.targetZ-p.z)*correction;p.y+=p.vy*dt;p.vy-=18*dt;if(p.pulse>.045){p.pulse=0;particles.push({kind:'spark',x:p.x,y:p.y,z:p.z,vx:rand(-.35,.35),vy:rand(-2,1),vz:rand(-.35,.35),life:.24,maxLife:.24,c:p.color,size:rand(3,5)})}if(p.y<=p.impactY||p.life<=0){p.x=p.targetX;p.z=p.targetZ;p.y=p.impactY;mortarExplosion(p,p.x,p.y,p.z);p.exploded=true}continue}if(p.kind==='punch'){updatePunchProjectile(p,dt);continue;}if(p.kind==='harpoon'){updateHarpoonProjectile(p,dt);continue;}const projectileTargets=targetsFor(p.owner,p.owner===player);p.life-=dt;p.pulse=(p.pulse||0)+dt;if(p.plasmaMine&&p.stuck){if(p.mineHost&&!p.mineHost.dead){p.x=p.mineHost.x;p.y=p.mineHost.y-.65;p.z=p.mineHost.z}else if(p.mineHost?.dead)p.life=0;p.arm=Math.max(0,(p.arm||0)-dt);let trigger=p.life<=0;if(!trigger&&p.arm<=0){for(const t of projectileTargets){if(t===p.mineHost||t.dead||t.spawnGrace>0)continue;if(Math.hypot(t.x-p.x,(t.y-.7)-p.y,t.z-p.z)<4.6){trigger=true;break}}}if(p.pulse>.24){p.pulse=0;particles.push({kind:'ring',x:p.x,y:p.y,z:p.z,vx:0,vy:0,vz:0,life:.18,maxLife:.18,c:p.color,size:16})}if(trigger){plasmaExplosion(p,p.x,p.y,p.z);p.exploded=true}continue}if(p.kind==='rocket'){p.vy-=GRAV*(p.bouncy?.42:.08)*dt;if(p.pulse>.055){p.pulse=0;particles.push({kind:'spark',x:p.x,y:p.y,z:p.z,vx:rand(-1.2,1.2),vy:rand(-.3,1.3),vz:rand(-1.2,1.2),life:.18,maxLife:.22,c:p.color,size:rand(2.5,4.5)});if(p.ultimate){addUltimateFx({type:'trailRing',weapon:'rocket',x:p.x,y:p.y,z:p.z,life:.26,maxLife:.26,color:p.color,size:4.2});addUltimateSparkCloud(p.x,p.y,p.z,'rocket',3,.45)}}}else if(p.gravity){const pullRadius=7+(p.tier||1)*1.2+(p.eventHorizon?2.4:0);for(const t of projectileTargets){if(t.dead)continue;const dx=p.x-t.x,dy=p.y-(t.y-.8),dz=p.z-t.z,dist=Math.hypot(dx,dy,dz);if(dist<pullRadius&&dist>.2){const resist=t.boss?.22:(((t.alienScale||1)>1.3||(t.r||0)>.75)?.5:1),pull=(p.eventHorizon?22:(p.singularity?16:5.5))*(1-dist/pullRadius)*resist;t.vx+=dx/dist*pull*dt;t.vz+=dz/dist*pull*dt;t.vy+=dy/dist*pull*.25*dt}}if(p.pulse>.11){p.pulse=0;particles.push({kind:'ring',x:p.x,y:p.y,z:p.z,vx:0,vy:0,vz:0,life:.24,maxLife:.24,c:p.color,size:p.r*(p.eventHorizon?21:18)});if(p.ultimate)addUltimateFx({type:'supernovaCore',weapon:'plasma',x:p.x,y:p.y,z:p.z,life:.34,maxLife:.34,color:p.color,size:p.r*16})}}const ox=p.x,oy=p.y,oz=p.z;p.px=ox;p.py=oy;p.pz=oz;let dx=p.vx*dt,dy=p.vy*dt,dz=p.vz*dt,step=Math.hypot(dx,dy,dz),dir=step?{x:dx/step,y:dy/step,z:dz/step}:{x:0,y:0,z:0},collisionRadius=p.r*.72,worldHit=step?rayWorld({x:ox,y:oy,z:oz},dir,step,true,collisionRadius):null;let targetHit=null,targetDist=worldHit?.dist??step;for(const t of projectileTargets){if(t.dead||t.spawnGrace>0)continue;const hit=rayEntity({x:ox,y:oy,z:oz},dir,t,step,collisionRadius);if(hit&&hit.dist<targetDist){targetHit=t;targetDist=hit.dist}}if(targetHit){p.x=ox+dir.x*targetDist;p.y=oy+dir.y*targetDist;p.z=oz+dir.z*targetDist;if(p.kind==='rocket'){rocketExplosion(p,p.x,p.y,p.z,false,targetHit);p.life=0;p.exploded=true}else if(p.plasmaMine){damage(targetHit,p.damage*.12,p.owner,false,{x:p.x,y:p.y,z:p.z},{weapon:'plasma',armorMult:1.28});p.stuck=true;p.mineHost=targetHit;p.vx=p.vy=p.vz=0;p.life=2.8;p.arm=.3;p.pulse=0}else{damage(targetHit,p.damage,p.owner,false,{x:p.x,y:p.y,z:p.z},{weapon:'plasma',armorMult:1.28});plasmaExplosion(p,p.x,p.y,p.z);p.life=0}}else if(worldHit){p.x=worldHit.x;p.y=worldHit.y;p.z=worldHit.z;const surfaceHit={...worldHit,x:worldHit.x-worldHit.normal.x*collisionRadius,y:worldHit.y-worldHit.normal.y*collisionRadius,z:worldHit.z-worldHit.normal.z*collisionRadius};if(p.kind==='rocket'&&p.bouncy&&p.bounces>0){damageStructure(worldHit.block.structure,p.damage*(p.siege?.42:.18),p.owner,'rocket',surfaceHit);addImpact(surfaceHit,p.color,.32);burst(p.x,p.y,p.z,p.color,p.siege?4:7,.25);const dot=p.vx*worldHit.normal.x+p.vy*worldHit.normal.y+p.vz*worldHit.normal.z,keep=p.bounces>2?.76:.62;p.vx=(p.vx-2*dot*worldHit.normal.x)*keep;p.vy=(p.vy-2*dot*worldHit.normal.y)*keep+.75;p.vz=(p.vz-2*dot*worldHit.normal.z)*keep;p.x+=worldHit.normal.x*(collisionRadius+.08);p.y+=worldHit.normal.y*(collisionRadius+.08);p.z+=worldHit.normal.z*(collisionRadius+.08);p.bounces--;p.life=Math.min(p.life,2.2)}else if(p.kind==='rocket'){addFloorDecal(surfaceHit,'rocket',p.bigBlast?2.15:1.65);damageStructure(worldHit.block.structure,p.damage*(p.siege?2.35:.75),p.owner,'rocket',surfaceHit);rocketExplosion(p,p.x,p.y,p.z,true);p.life=0;p.exploded=true}else if(p.plasmaMine){p.stuck=true;p.mineHost=null;p.vx=p.vy=p.vz=0;p.life=2.8;p.arm=.3;p.pulse=0;p.x=surfaceHit.x+worldHit.normal.x*.08;p.y=surfaceHit.y+worldHit.normal.y*.08;p.z=surfaceHit.z+worldHit.normal.z*.08;addImpact(surfaceHit,p.color,.28)}else{p.life=0;addFloorDecal(surfaceHit,'plasma',.55+(p.tier||1)*.16);damageStructure(worldHit.block.structure,p.damage,p.owner,'plasma',surfaceHit);plasmaExplosion(p,p.x,p.y,p.z,true)}}else{p.x+=dx;p.y+=dy;p.z+=dz}if(p.kind==='rocket'&&p.life<=0&&!p.exploded){rocketExplosion(p,p.x,p.y,p.z);p.exploded=true}if(p.kind!=='rocket'&&p.life<=0&&!p.plasmaMine&&p.singularity&&!targetHit&&!worldHit)plasmaExplosion(p,p.x,p.y,p.z)}projectiles=projectiles.filter(p=>p.life>0&&!p.exploded)}
function snapToGrid(value){return Math.round(value/gridSize)*gridSize}
function worldToGrid(x,z){return {gx:Math.floor(x/gridSize),gz:Math.floor(z/gridSize)}}
function gridToWorld(gx,gz){return {x:gx*gridSize,z:gz*gridSize}}
function heightToBuildLayer(y){return Math.round(y/BUILD_LEVEL)}
function layerToHeight(layer){return layer*BUILD_LEVEL}
function makeBuildKey(type,gx,gy,gz,orientation='h'){return type+':'+gx+':'+gy+':'+gz+':'+orientation}
function buildKeysFor(p){
  if(p.type==='wall')return [makeBuildKey('wall',p.gx,p.gy,p.gz,p.orientation)];
  return [makeBuildKey('platform',p.gx,p.gy,p.gz,'h')];
}
function isGridOccupied(type,gx,gy,gz,orientation='h'){return buildOccupancy.has(makeBuildKey(type,gx,gy,gz,orientation))}
function registerBuildPiece(piece,keys){piece.buildKeys=[...keys];for(const key of keys)buildOccupancy.set(key,piece)}
function unregisterBuildPiece(piece){for(const key of piece?.buildKeys||[])if(buildOccupancy.get(key)===piece)buildOccupancy.delete(key)}
function registerStaticGridOccupancy(){
  // Build placement is now a real cube grid: platforms occupy cube cells, walls occupy cube edges.
  // We only mark exact grid keys as occupied. Adjacent pieces, corner-touching walls, and platforms
  // sitting between walls no longer get rejected by loose geometry overlap.
  for(const piece of [...walls,...platforms]){
    if(!piece||piece.active===false)continue;
    if(piece.grid){registerBuildPiece(piece,buildKeysFor(piece.grid));continue;}
    const keys=[];
    if(piece.type==='wall'){
      const orientation=wallOrientation(piece),gy=heightToBuildLayer(piece.y-piece.h/2);
      if(orientation==='x'){
        const gz=Math.round(piece.z/gridSize),gx0=Math.floor((piece.x-piece.w/2+.02)/gridSize),gx1=Math.floor((piece.x+piece.w/2-.02)/gridSize);
        for(let gx=gx0;gx<=gx1;gx++)keys.push(makeBuildKey('wall',gx,gy,gz,'x'));
        piece.grid={type:'wall',gx:gx0,gy,gz,orientation:'x'};
      }else{
        const gx=Math.round(piece.x/gridSize),gz0=Math.floor((piece.z-piece.d/2+.02)/gridSize),gz1=Math.floor((piece.z+piece.d/2-.02)/gridSize);
        for(let gz=gz0;gz<=gz1;gz++)keys.push(makeBuildKey('wall',gx,gy,gz,'z'));
        piece.grid={type:'wall',gx,gy,gz:gz0,orientation:'z'};
      }
    }else if(piece.type==='platform'){
      const gy=Math.max(0,heightToBuildLayer(piece.y+piece.h));
      const gx0=Math.floor((piece.x-piece.w/2+.02)/gridSize),gx1=Math.floor((piece.x+piece.w/2-.02)/gridSize);
      const gz0=Math.floor((piece.z-piece.d/2+.02)/gridSize),gz1=Math.floor((piece.z+piece.d/2-.02)/gridSize);
      for(let gx=gx0;gx<=gx1;gx++)for(let gz=gz0;gz<=gz1;gz++)keys.push(makeBuildKey('platform',gx,gy,gz,'h'));
      piece.grid={type:'platform',gx:gx0,gy,gz:gz0,orientation:'h'};
    }
    if(keys.length)registerBuildPiece(piece,keys);
  }
}
function wallGridFromPoint(x,z,orientation){
  if(orientation==='x')return {gx:Math.floor(x/gridSize),gz:Math.round(z/gridSize)};
  return {gx:Math.round(x/gridSize),gz:Math.floor(z/gridSize)};
}
function snapBuildHeight(y){return layerToHeight(heightToBuildLayer(y))}
function wallOrientation(piece){if(piece?.grid?.orientation)return piece.grid.orientation;return piece&&piece.w>=piece.d?'x':'z'}
function wallFromGrid(gx,gy,gz,orientation,extra={}){
  const base=gridToWorld(gx,gz),alongX=orientation==='x',bottom=extra.baseY??layerToHeight(gy);
  return {type:'wall',gx,gy,gz,orientation,x:base.x+(alongX?gridSize/2:0),z:base.z+(alongX?0:gridSize/2),y:bottom+WALL_H/2,w:alongX?gridSize:WALL_T,d:alongX?WALL_T:gridSize,h:WALL_H,...extra}
}
function platformFromGrid(gx,gy,gz,extra={}){
  // Grid height is the standing surface, not the bottom of the slab. Previews,
  // collision blocks and adjacent bridges therefore share precisely the same top.
  const base=gridToWorld(gx,gz),top=extra.surfaceY??layerToHeight(gy);
  return {type:'platform',gx,gy,gz,orientation:'h',x:base.x+gridSize/2,z:base.z+gridSize/2,y:top-PLATFORM_H/2,w:PLATFORM_SIZE,d:PLATFORM_SIZE,h:PLATFORM_H,...extra}
}
function alignedPlatformCandidate(x,z,top,extra={}){const g=worldToGrid(x-gridSize/2+.001,z-gridSize/2+.001);return platformFromGrid(g.gx,Math.max(top>.08?1:0,heightToBuildLayer(top)),g.gz,{...extra,x,z,surfaceY:top})}
let buildActor=player;function getBuildPreviewFor(ent){const old=buildActor;buildActor=ent;try{return getBuildPreview()}finally{buildActor=old}}function placeBuildFor(ent,p){const old=buildActor;buildActor=ent;try{return placeBuild(p)}finally{buildActor=old}}function dedicatedBuildRaycast(){const actor=buildActor||player,eye=typeof actorEyePosition==='function'?actorEyePosition(actor):actor,origin={x:eye.x,y:eye.y-.05,z:eye.z},dir=dirFrom(actor,0),hit=rayWorld(origin,dir,BUILD_RANGE,true);if(!hit)return {origin,dir,hit:null};const n=hit.normal,s=hit.block.structure,face=Math.abs(n.y)>.55?(n.y>0?'top':'bottom'):'side';return {origin,dir,hit:{...hit,piece:s,face,grid:worldToGrid(hit.x,hit.z),orientation:s?.type==='wall'?wallOrientation(s):'h'}}}
function currentSupportPlatform(){const actor=buildActor||player,feet=actor.y-PLAYER_H;let best=null,bd=.75;for(const block of blocksNear(actor.x,actor.z,(actor.r||.5)+1)){const p=block.structure;if(block.kind!=='platform'||!p||p.active===false)continue;const top=p.y+p.h;if(Math.abs(feet-top)>bd)continue;if(Math.abs(actor.x-p.x)<=p.w/2+actor.r*.75&&Math.abs(actor.z-p.z)<=p.d/2+actor.r*.75){best=p;bd=Math.abs(feet-top)}}return best}
function platformEdgeLookCandidate(hit){const support=currentSupportPlatform();const actor=buildActor||player;if(!support)return null;const target=hit?{x:hit.x,z:hit.z}:{x:actor.x+Math.sin(actor.yaw)*9,z:actor.z+Math.cos(actor.yaw)*9};const dx=target.x-support.x,dz=target.z-support.z,nearX=Math.abs(Math.abs(dx)-support.w/2)<PLATFORM_LOOKDOWN_EDGE_HINT,nearZ=Math.abs(Math.abs(dz)-support.d/2)<PLATFORM_LOOKDOWN_EDGE_HINT,lookingForEdge=actor.pitch>.14||(!hit&&actor.pitch>-0.42);if(!lookingForEdge||(!nearX&&!nearZ))return null;let gx,gz;if(nearX&&Math.abs(dx)>=Math.abs(dz)){const sign=dx>=0?1:-1,edge=support.x+sign*support.w/2;gx=sign>0?Math.floor(edge/gridSize):Math.floor(edge/gridSize)-1;gz=Math.floor(clamp(target.z,support.z-support.d/2+.05,support.z+support.d/2-.05)/gridSize)}else{const sign=dz>=0?1:-1,edge=support.z+sign*support.d/2;gz=sign>0?Math.floor(edge/gridSize):Math.floor(edge/gridSize)-1;gx=Math.floor(clamp(target.x,support.x-support.w/2+.05,support.x+support.w/2-.05)/gridSize)}return attachedPlatformCandidate({x:target.x,z:target.z,piece:support,normal:{x:nearX&&Math.abs(dx)>=Math.abs(dz)?Math.sign(dx):0,z:nearX&&Math.abs(dx)>=Math.abs(dz)?0:Math.sign(dz)}})}
function closestWallEdgePlatformCandidate(origin,dir){
  const actor=buildActor||player;let best=null,bestD=1.8;
  for(const s of walls){
    if(!s||s.active===false)continue;
    const orientation=wallOrientation(s),top=s.y+s.h/2,samples=13;
    const tryPoint=(px,pz,nx,nz)=>{
      const vx=px-origin.x,vy=top-origin.y,vz=pz-origin.z,t=vx*dir.x+vy*dir.y+vz*dir.z;
      if(t<.8||t>BUILD_RANGE) return;
      const rx=origin.x+dir.x*t,ry=origin.y+dir.y*t,rz=origin.z+dir.z*t;
      const vertical=ry-top;
      if(vertical>.65||vertical<-2.8) return;
      const d=Math.hypot((px-rx)*.95,(top-ry)*.55,(pz-rz)*.95);
      if(d<bestD){
        bestD=d;
        best={x:px,y:clamp(ry,top-3.2,top+.35),z:pz,piece:s,block:{x:s.x,y:s.y,z:s.z,w:s.w,d:s.d,h:s.h,structure:s},face:'side',normal:{x:nx,y:0,z:nz},grid:worldToGrid(px,pz),orientation};
      }
    };
    if(orientation==='x'){
      for(let i=0;i<samples;i++){const a=i/(samples-1)-.5,px=s.x+a*s.w;tryPoint(px,s.z-s.d/2,0,-1);tryPoint(px,s.z+s.d/2,0,1)}
    }else{
      for(let i=0;i<samples;i++){const a=i/(samples-1)-.5,pz=s.z+a*s.d;tryPoint(s.x-s.w/2,pz,-1,0);tryPoint(s.x+s.w/2,pz,1,0)}
    }
  }
  return best?platformAdjacentToWall(best):null;
}
function floorWallCandidate(hit,dir){const orientation=Math.abs(dir.x)>Math.abs(dir.z)?'z':'x',g=wallGridFromPoint(hit.x,hit.z,orientation),surface=hit.block.y+hit.block.h/2,gy=Math.max(0,heightToBuildLayer(surface));return wallFromGrid(g.gx,gy,g.gz,orientation,{target:hit.piece,face:'floor',baseY:surface})}
function stackedWallCandidate(hit){
  const s=hit.piece,orientation=wallOrientation(s),top=hit.block.y+hit.block.h/2;
  // Map walls may span several cells or have a non-grid height. Use the aimed
  // segment while preserving their actual top, rather than their first grid key.
  const g=s?.builtBy&&s.grid?.type==='wall'?s.grid:wallGridFromPoint(hit.x,hit.z,orientation);
  return wallFromGrid(g.gx,heightToBuildLayer(top),g.gz,orientation,{target:s,face:'top',baseY:top,...(orientation==='x'?{z:s.z}:{x:s.x})})
}
function extendedWallCandidate(hit,dir){
  const s=hit.piece,orientation=wallOrientation(s);let gx,gz,gy;
  if(s?.grid?.type==='wall'){gx=s.grid.gx;gz=s.grid.gz;gy=s.grid.gy}else{const g=wallGridFromPoint(hit.x,hit.z,orientation);gx=g.gx;gz=g.gz;gy=heightToBuildLayer(hit.block.y-hit.block.h/2)}
  if(orientation==='x')gx+=(hit.x>=s.x?1:-1);else gz+=(hit.z>=s.z?1:-1);
  return wallFromGrid(gx,gy,gz,orientation,{target:s,face:'side'})
}
function platformAdjacentToWall(hit){
  const s=hit.piece,n=hit.normal||{x:0,z:0},orientation=wallOrientation(s),top=hit.block.y+hit.block.h/2;
  let x,z;
  if(orientation==='x'){
    const sign=Math.abs(n.z)>.5?n.z:(hit.z>=s.z?1:-1);
    x=s.builtBy&&s.grid?s.x:Math.floor(hit.x/gridSize)*gridSize+gridSize/2;
    z=s.z+sign*gridSize/2;
  }else{
    const sign=Math.abs(n.x)>.5?n.x:(hit.x>=s.x?1:-1);
    z=s.builtBy&&s.grid?s.z:Math.floor(hit.z/gridSize)*gridSize+gridSize/2;
    x=s.x+sign*gridSize/2;
  }
  return alignedPlatformCandidate(x,z,top,{target:s,face:'edge'});
}
function attachedPlatformCandidate(hit){
  const s=hit.piece;if(s?.type==='wall')return platformAdjacentToWall(hit);
  const n=hit.normal||{x:0,z:0},top=s.y+s.h,dx=hit.x-s.x,dz=hit.z-s.z;
  const alongX=Math.abs(n.x)>.5||(!Math.abs(n.z)&&Math.abs(dx)/(s.w||1)>Math.abs(dz)/(s.d||1));
  let x,z;
  if(alongX){const sign=Math.abs(n.x)>.5?n.x:(dx>=0?1:-1);x=s.x+sign*(s.w/2+PLATFORM_SIZE/2);z=clamp(Math.floor(hit.z/gridSize)*gridSize+gridSize/2,s.z-s.d/2+Math.min(PLATFORM_SIZE,s.d)/2,s.z+s.d/2-Math.min(PLATFORM_SIZE,s.d)/2);}
  else{const sign=Math.abs(n.z)>.5?n.z:(dz>=0?1:-1);z=s.z+sign*(s.d/2+PLATFORM_SIZE/2);x=clamp(Math.floor(hit.x/gridSize)*gridSize+gridSize/2,s.x-s.w/2+Math.min(PLATFORM_SIZE,s.w)/2,s.x+s.w/2-Math.min(PLATFORM_SIZE,s.w)/2);}
  return alignedPlatformCandidate(x,z,top,{target:s,face:'edge'});
}
function fallbackBuildCandidate(origin,dir){const horizontal=Math.hypot(dir.x,dir.z)||1,t=10/horizontal,x=origin.x+dir.x*t,z=origin.z+dir.z*t,g=worldToGrid(x,z),orientation=Math.abs(dir.x)>Math.abs(dir.z)?'z':'x',top=topAtPoint(x,z),gy=Math.max(0,heightToBuildLayer(top));return wallFromGrid(g.gx,gy,g.gz,orientation,{face:'floor'})}
function wallTopPlatformIntent(hit){
  const s=hit.piece;if(!s||s.type!=='wall'||hit.face!=='top')return false;
  const actor=buildActor||player,orientation=wallOrientation(s),along=orientation==='x'?Math.abs(hit.x-s.x):Math.abs(hit.z-s.z),cross=orientation==='x'?Math.abs(hit.z-s.z):Math.abs(hit.x-s.x);
  const len=orientation==='x'?s.w:s.d;
  const nearEnd=along>len/2-PLATFORM_EDGE_HINT*.72;
  const nearLip=cross>WALL_T*.22;
  const lookingDownToLip=actor.pitch>-.08&&cross>WALL_T*.16;
  return nearEnd||nearLip||lookingDownToLip;
}
function wallSideUpperEdgeIntent(hit){
  const s=hit?.piece;if(!s||s.type!=='wall'||hit.face!=='side')return false;
  const top=hit.block.y+hit.block.h/2,actor=buildActor||player;
  return hit.y>=top-4.25&&hit.y<=top+1.0&&actor.pitch>=-.65;
}
function wallSideAboveStackIntent(hit){
  const s=hit?.piece;if(!s||s.type!=='wall'||hit.face!=='side')return false;
  const top=hit.block.y+hit.block.h/2,actor=buildActor||player;
  return hit.y>top+.35&&actor.pitch<.10;
}
function closestWallTopAimCandidate(origin,dir){
  const actor=buildActor||player;
  if(actor.pitch>.20)return null;
  let best=null,bestD=1.35;
  for(const s of walls){
    if(!s||s.active===false)continue;
    const orientation=wallOrientation(s),top=s.y+s.h/2,samples=11;
    for(let i=0;i<samples;i++){
      const a=samples===1?0:(i/(samples-1)-.5),px=orientation==='x'?s.x+a*s.w:s.x,pz=orientation==='z'?s.z+a*s.d:s.z;
      const vx=px-origin.x,vy=top-origin.y,vz=pz-origin.z,t=vx*dir.x+vy*dir.y+vz*dir.z;
      if(t<1.0||t>BUILD_RANGE)continue;
      const rx=origin.x+dir.x*t,ry=origin.y+dir.y*t,rz=origin.z+dir.z*t,d=Math.hypot(px-rx,(top-ry)*.48,pz-rz);
      if(d<bestD){
        const fake={x:px,y:top,z:pz,piece:s,block:{x:s.x,y:s.y,z:s.z,w:s.w,d:s.d,h:s.h,structure:s},face:'top',normal:{x:0,y:1,z:0},grid:worldToGrid(px,pz),orientation};
        bestD=d;best=fake;
      }
    }
  }
  return best?stackedWallCandidate(best):null;
}

function actorBuildGrid(actor){return worldToGrid(actor.x,actor.z)}
function clampTargetGridToActor(g,actor){const a=actorBuildGrid(actor);return {gx:clamp(g.gx,a.gx-BUILD_CELL_LIMIT,a.gx+BUILD_CELL_LIMIT),gz:clamp(g.gz,a.gz-BUILD_CELL_LIMIT,a.gz+BUILD_CELL_LIMIT)}}
function localBuildDistanceCells(p,actor){const a=actorBuildGrid(actor);return Math.max(Math.abs(p.gx-a.gx),Math.abs(p.gz-a.gz))}
function withinLocalBuildLimit(p,actor){return localBuildDistanceCells(p,actor)<=BUILD_CELL_LIMIT}
function yawWallOrientation(actor){
  // Wall orientation is player-facing: the wall's broad flat face is placed in
  // front of the camera, so its thin axis follows the player's forward axis.
  // Looking mostly north/south creates an east-west wall; looking mostly
  // east/west creates a north-south wall.
  return Math.abs(Math.cos(actor.yaw))>=Math.abs(Math.sin(actor.yaw))?'x':'z';
}
function buildAimGrid(cast,actor){
  const dir=cast.dir;
  if(cast.hit){
    const h=cast.hit,n=h.normal||{x:0,z:0};
    return clampTargetGridToActor(worldToGrid(h.x+n.x*.35,h.z+n.z*.35),actor);
  }
  const dist=gridSize*1.55;
  return clampTargetGridToActor(worldToGrid(actor.x+Math.sin(actor.yaw)*dist,actor.z+Math.cos(actor.yaw)*dist),actor);
}
function nextFreeWallLayer(gx,gz,orientation,baseGy){
  let gy=Math.max(0,baseGy|0),guard=0;
  while(guard++<BUILD_MAX_STACK&&isGridOccupied('wall',gx,gy,gz,orientation))gy++;
  return gy;
}
function topBuildLayerAtCell(gx,gz){
  const c=gridToWorld(gx,gz),top=topAtPoint(c.x+gridSize/2,c.z+gridSize/2);
  return Math.max(0,heightToBuildLayer(top));
}
function candidateAimPoint(cast){
  const actor=buildActor||player;
  if(cast?.hit)return {x:cast.hit.x,y:cast.hit.y,z:cast.hit.z};
  const t=gridSize*1.65;
  return {x:actor.x+cast.dir.x*t,y:actor.y+cast.dir.y*t,z:actor.z+cast.dir.z*t};
}
function rayScoreForCandidate(p,cast,aim){
  const actor=buildActor||player,origin=cast.origin,dir=cast.dir;
  const vx=p.x-origin.x,vy=p.y-origin.y,vz=p.z-origin.z;
  const t=Math.max(0,vx*dir.x+vy*dir.y+vz*dir.z);
  const rx=origin.x+dir.x*t,ry=origin.y+dir.y*t,rz=origin.z+dir.z*t;
  const rayDist=Math.hypot(p.x-rx,(p.y-ry)*.55,p.z-rz);
  const aimDist=Math.hypot(p.x-aim.x,(p.y-aim.y)*.4,p.z-aim.z);
  const cellPenalty=localBuildDistanceCells(p,actor)*.08;
  return rayDist*1.4+aimDist*.72+cellPenalty;
}
function localBuildCells(actor){
  const a=actorBuildGrid(actor),cells=[];
  for(let dx=-BUILD_CELL_LIMIT;dx<=BUILD_CELL_LIMIT;dx++)for(let dz=-BUILD_CELL_LIMIT;dz<=BUILD_CELL_LIMIT;dz++)cells.push({gx:a.gx+dx,gz:a.gz+dz});
  return cells;
}
function addCandidate(list,c){
  if(!c)return;
  const actor=buildActor||player;
  c.distance=Math.hypot(c.x-actor.x,c.y-actor.y,c.z-actor.z);
  list.push(c);
}
function wallCandidateAtCell(gx,gz,orientation,baseGy=0,extra={}){
  const gy=nextFreeWallLayer(gx,gz,orientation,baseGy),point=wallFromGrid(gx,baseGy,gz,orientation),top=topAtPoint(point.x,point.z),surface=extra.baseY??(top>=-.05&&heightToBuildLayer(top)===baseGy?top:layerToHeight(baseGy));
  return wallFromGrid(gx,gy,gz,orientation,{...extra,baseY:surface+(gy-baseGy)*BUILD_LEVEL});
}
function explicitWallBuildCandidate(cast){
  const actor=buildActor||player,aim=candidateAimPoint(cast),preferred=yawWallOrientation(actor),list=[];
  // WALL MODE ONLY: never call platform candidate helpers here.
  if(cast.hit?.piece?.type==='wall'&&cast.hit.piece.grid&&withinLocalBuildLimit(cast.hit.piece.grid,actor)){
    const s=cast.hit.piece;
    addCandidate(list,wallCandidateAtCell(s.grid.gx,s.grid.gz,preferred,s.grid.gy+1,{target:s,face:'wall-mode-stack',baseY:pieceTopY(s)}));
  }
  const aimEdge=wallGridFromPoint(aim.x,aim.z,preferred),aimGrid=buildAimGrid(cast,actor);
  const directBase=topBuildLayerAtCell(Math.floor(aim.x/gridSize),Math.floor(aim.z/gridSize));
  addCandidate(list,wallCandidateAtCell(aimEdge.gx,aimEdge.gz,preferred,directBase,{face:'wall-mode-edge'}));
  for(const cell of localBuildCells(actor)){
    const nearAim=Math.max(Math.abs(cell.gx-aimGrid.gx),Math.abs(cell.gz-aimGrid.gz));
    if(nearAim>BUILD_CELL_LIMIT)continue;
    const edge=preferred==='x'?{gx:cell.gx,gz:Math.round(aim.z/gridSize)}:{gx:Math.round(aim.x/gridSize),gz:cell.gz};
    const baseGy=topBuildLayerAtCell(cell.gx,cell.gz);
    addCandidate(list,wallCandidateAtCell(edge.gx,edge.gz,preferred,baseGy,{face:'wall-mode'}));
  }
  return chooseBuildCandidate(list,'wall',cast,aim);
}
function platformCandidateFromNearbyWall(gx,gz,actor){
  const target=gridToWorld(gx,gz);target.x+=gridSize/2;target.z+=gridSize/2;
  let best=null,bd=999;
  for(const s of walls){
    if(!s||s.active===false)continue;
    const sg=s.grid||worldToGrid(s.x,s.z);
    if(Math.max(Math.abs(sg.gx-actorBuildGrid(actor).gx),Math.abs(sg.gz-actorBuildGrid(actor).gz))>BUILD_CELL_LIMIT)continue;
    const orientation=wallOrientation(s),dx=target.x-s.x,dz=target.z-s.z;
    let normal;
    if(orientation==='x')normal={x:0,y:0,z:dz>=0?1:-1};else normal={x:dx>=0?1:-1,y:0,z:0};
    const hit={x:target.x,y:s.y+s.h/2,z:target.z,piece:s,block:{x:s.x,y:s.y,z:s.z,w:s.w,d:s.d,h:s.h,structure:s},face:'side',normal,grid:sg,orientation};
    const p=platformAdjacentToWall(hit);p.distance=Math.hypot(p.x-actor.x,p.y-actor.y,p.z-actor.z);
    const d=Math.hypot(p.x-target.x,p.z-target.z)+Math.abs(p.gy-heightToBuildLayer(s.y+s.h/2))*2;
    if(withinLocalBuildLimit(p,actor)&&d<bd){bd=d;best=p;}
  }
  return best;
}
function explicitPlatformBuildCandidate(cast){
  const actor=buildActor||player,aim=candidateAimPoint(cast),list=[];
  // PLATFORM MODE ONLY: never call wall candidate helpers here.
  if(cast.hit?.piece?.type==='wall')addCandidate(list,platformAdjacentToWall(cast.hit));
  if(cast.hit?.piece?.type==='platform')addCandidate(list,attachedPlatformCandidate(cast.hit));
  const aimGrid=buildAimGrid(cast,actor);
  for(const cell of localBuildCells(actor)){
    const nearAim=Math.max(Math.abs(cell.gx-aimGrid.gx),Math.abs(cell.gz-aimGrid.gz));
    if(nearAim>BUILD_CELL_LIMIT)continue;
    addCandidate(list,platformCandidateFromNearbyWall(cell.gx,cell.gz,actor));
    const base=Math.max(1,topBuildLayerAtCell(cell.gx,cell.gz));
    for(let gy=Math.max(1,base-1);gy<=Math.min(BUILD_MAX_STACK,base+2);gy++)addCandidate(list,platformFromGrid(cell.gx,gy,cell.gz,{face:'platform-mode'}));
  }
  return chooseBuildCandidate(list,'platform',cast,aim);
}
function chooseBuildCandidate(list,type,cast,aim){
  const actor=buildActor||player,unique=new Map();
  for(const c of list){
    if(!c||c.type!==type||!withinLocalBuildLimit(c,actor))continue;
    const key=buildKeysFor(c).join('|');if(!unique.has(key))unique.set(key,c);
  }
  const candidates=[...unique.values()];
  // Assistance is local to the aimed surface. An invalid preview must not jump
  // to another wall on the far side of the player just because it has support.
  const anchor=candidates[0]?.target?candidates[0]:candidates.reduce((best,c)=>!best||rayScoreForCandidate(c,cast,aim)<rayScoreForCandidate(best,cast,aim)?c:best,null);
  let bestValid=null,bestValidScore=Infinity,bestInvalid=null,bestInvalidScore=Infinity,bestInvalidResult=null;
  for(const c of candidates){
    if(anchor&&(Math.hypot(c.x-anchor.x,c.z-anchor.z)>gridSize*1.5||Math.abs(c.y-anchor.y)>BUILD_LEVEL+.02))continue;
    c.distance=Math.hypot(c.x-actor.x,c.y-actor.y,c.z-actor.z);c.selectedType=type;
    const score=rayScoreForCandidate(c,cast,aim),raw=validateBuildPlacementRaw(c);
    if(raw.valid){if(score<bestValidScore){bestValidScore=score;bestValid=c;}}
    else if(score<bestInvalidScore){bestInvalidScore=score;bestInvalid=c;bestInvalidResult=raw;}
  }
  if(bestValid)return bestValid;
  if(bestInvalid){bestInvalid.invalidHint=bestInvalidResult;return bestInvalid;}
  const g=buildAimGrid(cast,actor);
  return type==='wall'?wallFromGrid(g.gx,0,g.gz,yawWallOrientation(actor),{face:'wall-mode-empty'}):platformFromGrid(g.gx,1,g.gz,{face:'platform-mode-empty'});
}
function buildIntentFor(cast,actor){
  const hit=cast?.hit,s=hit?.piece;if(!s)return 'wall';
  const top=hit.block.y+hit.block.h/2;
  if(top<=.08)return 'wall';
  if(s.type==='wall'){
    if(hit.face==='top')return 'stack';
    if(hit.face==='side'&&(pieceBottomY(s)>.08||hit.y>=top-Math.min(2.8,s.h*.75)))return 'platform';
    return 'wall';
  }
  if(s.type==='platform'){
    const edgeX=s.w/2-Math.abs(hit.x-s.x),edgeZ=s.d/2-Math.abs(hit.z-s.z);
    if(hit.face==='side'||hit.face==='top'&&Math.min(edgeX,edgeZ)<.8)return 'platform';
  }
  return 'wall';
}
function assistedBuildCandidate(cast){
  const actor=buildActor||player,hit=cast.hit,intent=buildIntentFor(cast,actor);
  let primary;
  if(intent==='stack')primary=stackedWallCandidate(hit);
  else if(intent==='platform')primary=hit.piece.type==='wall'?platformAdjacentToWall(hit):attachedPlatformCandidate(hit);
  else if(hit&&hit.face==='top')primary=floorWallCandidate(hit,cast.dir);
  else if(hit?.piece?.type==='wall')primary=extendedWallCandidate(hit,cast.dir);
  else if(!hit){
    const hint=actor.pitch<-.08?closestWallTopAimCandidate(cast.origin,cast.dir):closestWallEdgePlatformCandidate(cast.origin,cast.dir);
    if(hint&&withinLocalBuildLimit(hint,actor)&&lineClear(cast.origin.x,cast.origin.y,cast.origin.z,hint.x,pieceTopY(hint)+.06,hint.z))primary=hint;
  }
  if(!primary)primary=fallbackBuildCandidate(cast.origin,cast.dir);
  primary.distance=Math.hypot(primary.x-actor.x,primary.y-actor.y,primary.z-actor.z);
  if(validateBuildPlacementRaw(primary).valid)return primary;
  const list=[primary];
  // Try just the neighboring cells on the same surface; every candidate still
  // passes occupancy, actor collision, range, support and collapse validation.
  for(const delta of [-1,1]){
    if(primary.type==='wall'){
      const alongX=primary.orientation==='x';
      addCandidate(list,wallFromGrid(primary.gx+(alongX?delta:0),primary.gy,primary.gz+(alongX?0:delta),primary.orientation,{target:primary.target,face:'assisted-snap',baseY:pieceBottomY(primary),...(alongX?{z:primary.z}:{x:primary.x})}));
    }else{
      const target=primary.target,alongX=target?.type==='wall'?wallOrientation(target)==='x':Math.abs(primary.z-(target?.z||0))>=Math.abs(primary.x-(target?.x||0));
      addCandidate(list,alignedPlatformCandidate(primary.x+(alongX?delta*gridSize:0),primary.z+(alongX?0:delta*gridSize),pieceTopY(primary),{target,face:'assisted-snap'}));
    }
  }
  if(primary.type==='platform'&&primary.target){
    const target=primary.target,dx=primary.x-target.x,dz=primary.z-target.z,alongX=Math.abs(dx)>Math.abs(dz);
    addCandidate(list,alignedPlatformCandidate(primary.x+(alongX?Math.sign(dx)*gridSize:0),primary.z+(alongX?0:Math.sign(dz)*gridSize),pieceTopY(primary),{face:'bridge-snap'}));
  }
  return chooseBuildCandidate(list,primary.type,cast,candidateAimPoint(cast));
}
function getBuildPreview(){
  const cast=dedicatedBuildRaycast(),actor=buildActor||player,mode=buildTypeFor(actor);
  const p=mode==='auto'?assistedBuildCandidate(cast):(mode==='platform'?explicitPlatformBuildCandidate(cast):explicitWallBuildCandidate(cast));
  p.distance=Math.hypot(p.x-actor.x,p.y-actor.y,p.z-actor.z);
  const result=p.invalidHint||validateBuildPlacement(p);p.valid=result.valid;p.reason=result.reason;p.keys=buildKeysFor(p);p.hit=cast.hit;p.selectedType=p.type;p.assisted=mode==='auto';p.localCells=localBuildDistanceCells(p,actor);
  if(actor===player)selectedBuildType=p.type;else if(actor.human)actor.buildType=p.type;
  return p;
}

function pieceBottomY(p){return p.type==='platform'&&(p.builtBy||p.id)?p.y:p.y-p.h/2}
function pieceTopY(p){return p.type==='platform'&&(p.builtBy||p.id)?p.y+p.h:p.y+p.h/2}
function allActiveStructures(){blocks();return activeStructuresCache}
function pieceIsGroundAnchor(p){
  if(!p||p.active===false)return false;
  if(pieceBottomY(p)<=.08)return true;
  // Map-authored structures are treated as anchored while they still exist. If one is destroyed,
  // anything only connected through it will fail this same graph check and collapse.
  if(!p.builtBy)return true;
  return false;
}
function xzOverlap(a,b,pad=.18){return Math.abs(a.x-b.x)<=(a.w+b.w)/2+pad&&Math.abs(a.z-b.z)<=(a.d+b.d)/2+pad}
function verticalOverlap(a,b,pad=.18){return Math.min(pieceTopY(a),pieceTopY(b))-Math.max(pieceBottomY(a),pieceBottomY(b))>=-pad}
function structureContacts(a,b){
  if(!a||!b||b.active===false)return false;
  const aBot=pieceBottomY(a),aTop=pieceTopY(a),bBot=pieceBottomY(b),bTop=pieceTopY(b);
  // Stacked support: wall-on-wall, wall-on-platform, platform-on-wall, or platform-on-platform.
  if(Math.abs(aBot-bTop)<=.85&&xzOverlap(a,b,.45))return true;
  if(Math.abs(bBot-aTop)<=.85&&xzOverlap(a,b,.45))return true;
  // Same-level side attachment lets platforms tie into walls and lets connected platforms share support.
  if(verticalOverlap(a,b,.35)){
    const touchX=Math.abs(Math.abs(a.x-b.x)-(a.w+b.w)/2)<=.85&&Math.abs(a.z-b.z)<=(a.d+b.d)/2+.35;
    const touchZ=Math.abs(Math.abs(a.z-b.z)-(a.d+b.d)/2)<=.85&&Math.abs(a.x-b.x)<=(a.w+b.w)/2+.35;
    if(touchX||touchZ)return true;
  }
  return false;
}
function connectedToGround(piece,extra=null){
  if(!piece)return false;
  if(pieceBottomY(piece)<=.08)return true;
  const seen=new Set(),stack=[piece];
  while(stack.length){
    const cur=stack.pop();
    if(!cur||seen.has(cur.id||cur))continue;
    seen.add(cur.id||cur);
    if(pieceIsGroundAnchor(cur))return true;
    const local=structuresNearPiece(cur,1.05);if(extra&&structureContacts(cur,extra))local.push(extra);
    for(const other of local){
      if(other===cur||seen.has(other.id||other))continue;
      if(structureContacts(cur,other))stack.push(other);
    }
  }
  return false;
}
function directSupportForCandidate(p){
  if(!p)return null;
  if(pieceBottomY(p)<=.08)return {floor:true};
  let best=null,bd=999;
  for(const s of structuresNearPiece(p,1.05)){
    if(!structureContacts(p,s))continue;
    if(!connectedToGround(s))continue;
    const d=Math.abs(pieceBottomY(p)-pieceTopY(s))*.4+Math.hypot(p.x-s.x,p.z-s.z);
    if(d<bd){bd=d;best=s;}
  }
  return best;
}
function wallBelowForCandidate(p){if(!p||p.type!=='wall')return null;return directSupportForCandidate(p)}
function connectedWallForPlatform(p){return directSupportForCandidate(p)}
function supportForCandidate(p){return directSupportForCandidate(p)}
function candidateHasSupport(p){return !!supportForCandidate(p)}
function builtPieceStable(piece){if(!piece||piece.active===false||!piece.builtBy||!piece.grid)return true;return connectedToGround(piece)}
function collapseStructure(piece){if(!piece||piece.active===false||!piece.destructible)return;if(piece.builtBy)activeBuildCount=Math.max(0,activeBuildCount-1);piece.active=false;markBlocksDirty();unregisterBuildPiece(piece);const y=piece.type==='platform'?piece.y+piece.h:piece.y;burst(piece.x,y,piece.z,'#ffb02e',20,.75);for(let i=0;i<22;i++)particles.push({kind:'debris',x:piece.x+rand(-piece.w/2,piece.w/2),y:y+rand(-.25,.55),z:piece.z+rand(-piece.d/2,piece.d/2),vx:rand(-3.5,3.5),vy:rand(-2,2),vz:rand(-3.5,3.5),life:rand(.65,1.25),maxLife:1.25,c:shadeColor(piece.c||'#65f7ff',-25),size:rand(3,7)});for(const b of bots){b.route=null;b.think=0}}
function collapseUnsupportedBuilds(){const all=allActiveStructures().slice(),supported=new Set(),queue=[];for(const piece of all)if(pieceIsGroundAnchor(piece)){supported.add(piece.id||piece);queue.push(piece)}while(queue.length){const cur=queue.pop();for(const other of structuresNearPiece(cur,1.05)){const key=other.id||other;if(other===cur||supported.has(key)||!structureContacts(cur,other))continue;supported.add(key);queue.push(other)}}const unsupported=buildPieces.filter(piece=>piece.active!==false&&piece.builtBy&&piece.grid&&!supported.has(piece.id||piece));for(const piece of unsupported)collapseStructure(piece)}

function boxesOverlap(a,b,pad=0){
  // Generic center-based AABB overlap used by build validation. Candidates,
  // blocks, actors and pickups all pass x/y/z plus w/h/d in world units.
  if(!a||!b)return false;
  const aw=a.w||0,ah=a.h||0,ad=a.d||0,bw=b.w||0,bh=b.h||0,bd=b.d||0;
  return Math.abs((a.x||0)-(b.x||0)) <= (aw+bw)/2 + pad &&
         Math.abs((a.y||0)-(b.y||0)) <= (ah+bh)/2 + pad &&
         Math.abs((a.z||0)-(b.z||0)) <= (ad+bd)/2 + pad;
}
function wallsMeetAtCorner(a,b,pad=.18){
  // Allow two walls to share/touch a corner without treating that tiny contact
  // as a blocking overlap. This keeps wall-mode from rejecting clean grid corners.
  if(!a||!b||a.type!=='wall'||b.kind!=='wall')return false;
  if(Math.abs((a.y||0)-(b.y||0))>(a.h+b.h)/2+.35)return false;
  const ax1=a.x-a.w/2,ax2=a.x+a.w/2,az1=a.z-a.d/2,az2=a.z+a.d/2;
  const bx1=b.x-b.w/2,bx2=b.x+b.w/2,bz1=b.z-b.d/2,bz2=b.z+b.d/2;
  const xTouch=Math.min(Math.abs(ax1-bx1),Math.abs(ax1-bx2),Math.abs(ax2-bx1),Math.abs(ax2-bx2))<=pad;
  const zTouch=Math.min(Math.abs(az1-bz1),Math.abs(az1-bz2),Math.abs(az2-bz1),Math.abs(az2-bz2))<=pad;
  const xOverlap=Math.min(ax2,bx2)-Math.max(ax1,bx1)>pad;
  const zOverlap=Math.min(az2,bz2)-Math.max(az1,bz1)>pad;
  return (xTouch&&zOverlap)||(zTouch&&xOverlap)||(xTouch&&zTouch);
}

function findNearestValidBuildCandidate(p){
  if(!p)return null;const actor=buildActor||player;
  for(let r=0;r<=1;r++)for(let dx=-r;dx<=r;dx++)for(let dz=-r;dz<=r;dz++){
    if(Math.max(Math.abs(dx),Math.abs(dz))!==r)continue;
    const c=p.type==='wall'?wallFromGrid(p.gx+dx,p.gy,p.gz+dz,p.orientation,{face:'snap',baseY:pieceBottomY(p),x:p.x+dx*gridSize,z:p.z+dz*gridSize}):alignedPlatformCandidate(p.x+dx*gridSize,p.z+dz*gridSize,pieceTopY(p),{face:'snap'});
    c.distance=Math.hypot(c.x-actor.x,c.y-actor.y,c.z-actor.z);
    const support=supportForCandidate(c);if(support&&!support.floor)c.target=support;
    if(validateBuildPlacementRaw(c).valid)return c;
  }
  return null;
}
function candidateOverlapsActor(p,ent){
  if(!ent||ent.dead)return false;
  const bottom=ent.y-PLAYER_H+.025,top=ent.y+HEAD_CLEAR;
  if(pieceTopY(p)<=bottom||pieceBottomY(p)>=top)return false;
  const dx=Math.max(0,Math.abs(p.x-ent.x)-p.w/2),dz=Math.max(0,Math.abs(p.z-ent.z)-p.d/2),radius=(ent.r||.5)+.035;
  return dx*dx+dz*dz<radius*radius;
}
function candidateBlockedByStructure(p,b){
  const px=Math.min(p.x+p.w/2,b.x+b.w/2)-Math.max(p.x-p.w/2,b.x-b.w/2),pz=Math.min(p.z+p.d/2,b.z+b.d/2)-Math.max(p.z-p.d/2,b.z-b.d/2),py=Math.min(pieceTopY(p),b.y+b.h/2)-Math.max(pieceBottomY(p),b.y-b.h/2);
  if(px<=.015||py<=.015||pz<=.015)return false;
  // A floor slab attaches halfway over the wall cap. That deliberate overlap
  // closes the seam while leaving the actual standing top exactly level.
  if(p.type==='platform'&&b.kind==='wall'&&Math.abs(pieceTopY(p)-(b.y+b.h/2))<.04){const cross=b.w>=b.d?pz:px,thin=Math.min(b.w,b.d);if(cross<=thin/2+.06&&py<=PLATFORM_H+.025)return false;}
  if(p.type==='wall'&&b.kind==='wall'&&wallOrientation(p)!==wallOrientation(b.structure)&&wallsMeetAtCorner(p,b,.3))return false;
  return true;
}
function validateBuildPlacementRaw(p){
  if(!p||!['wall','platform'].includes(p.type))return {valid:false,reason:'NO SUPPORT'};
  const actor=buildActor||player,selected=buildTypeFor(actor);
  if(selected!=='auto'&&p.type!==selected)return {valid:false,reason:'WRONG BUILD MODE'};if(typeof survivalMapRuntime!=='undefined'&&game.mode==='survivor'&&!survivalMapRuntime.canBuild(p.x,p.z,p.y))return {valid:false,reason:'NO BUILD ZONE'};
  if(!siegeBuildActive()&&humanCharges(actor)<=0)return {valid:false,reason:'NO CHARGES'};
  if(!withinLocalBuildLimit(p,actor))return {valid:false,reason:'2 SPACES MAX'};
  const distance=Math.hypot(p.x-actor.x,p.y-actor.y,p.z-actor.z);
  if(distance>BUILD_RANGE+BUILD_LEVEL*BUILD_MAX_STACK)return {valid:false,reason:'TOO FAR'};
  if(![p.x,p.y,p.z,p.w,p.d,p.h].every(Number.isFinite)||Math.abs(p.x)+p.w/2>ARENA_SAFE-1||Math.abs(p.z)+p.d/2>ARENA_SAFE-1||pieceBottomY(p)<-.05)return {valid:false,reason:'BLOCKED'};
  if(p.type==='wall'&&(p.gy<0||p.gy>=BUILD_MAX_STACK)||p.type==='platform'&&(p.gy<0||p.gy>BUILD_MAX_STACK)||pieceTopY(p)>BUILD_LEVEL*BUILD_MAX_STACK+.08)return {valid:false,reason:'MAX HEIGHT'};
  if(p.type==='platform'&&(p.gy<=0||pieceTopY(p)<=.08))return {valid:false,reason:'ATTACH TO WALL'};
  if(activeBuildCount>=MAX_ACTIVE_BUILDS)return {valid:false,reason:'STRUCTURE LIMIT'};
  if(buildKeysFor(p).some(key=>buildOccupancy.has(key)))return {valid:false,reason:'OCCUPIED'};
  if([player,player2,...bots].some(ent=>candidateOverlapsActor(p,ent)))return {valid:false,reason:'ACTOR IN THE WAY'};
  if(blocksInBounds(p.x,p.z,p.w,p.d,.02).some(b=>candidateBlockedByStructure(p,b)))return {valid:false,reason:'BLOCKED'};
  if(!candidateHasSupport(p))return {valid:false,reason:'NO SUPPORT'};
  return {valid:true,reason:p.type.toUpperCase()};
}
function validateBuildPlacement(p){return validateBuildPlacementRaw(p)}
function isBuildPlacementValid(p){return validateBuildPlacement(p).valid}
function rejectBuildPlacement(actor,p,reason){setHumanBuildFeedback(actor,.3);actor.buildFailure={reason,p,until:performance.now()+950};return false;}
function placeBuild(p){
  const actor=buildActor||player,freeBuild=siegeBuildActive(),mode=buildTypeFor(actor);
  if(!p||mode!=='auto'&&p.type!==mode)return rejectBuildPlacement(actor,p,'WRONG BUILD MODE');
  const result=validateBuildPlacement(p);if(!result.valid)return rejectBuildPlacement(actor,p,result.reason);
  if(!freeBuild&&humanCharges(actor)<=0)return rejectBuildPlacement(actor,p,'NO CHARGES');
  const support=supportForCandidate(p),grid={type:p.type,gx:p.gx,gy:p.gy,gz:p.gz,orientation:p.orientation},piece=makeStructurePiece(p.type,{x:p.x,z:p.z,w:p.w,d:p.d,h:p.h,y:p.type==='platform'?p.y-p.h/2:p.y,c:p.type==='wall'?'#176b87':'#5f2d86',builtBy:actor,grid,orientation:p.orientation,supportId:support&&support.id});
  if(p.type==='wall')walls.push(piece);else platforms.push(piece);
  activeBuildCount++;markBlocksDirty();buildPieces.push(piece);if(typeof game!=='undefined'&&game.mode==='survivor'&&typeof survivalProgress!=='undefined'){survivalProgress?.recordEvent('structuresBuilt');survivalController?.invalidate();}registerBuildPiece(piece,buildKeysFor(p));
  if(freeBuild){setHumanCharges(actor,BUILD_MAX);setHumanRecharge(actor,0);}else{setHumanCharges(actor,humanCharges(actor)-1);if(humanCharges(actor)===BUILD_MAX-1)setHumanRecharge(actor,0);}
  actor.buildFailure=null;burst(p.x,p.y,p.z,p.type==='wall'?'#00eaff':'#d36bff',8,.25);for(const b of bots){b.route=null;b.think=0;}
  return true;
}
function updateBuildCharges(dt){const max=survivorBuildMax(),rate=survivorBuildRecharge();if(siegeBuildActive()){buildCharges=BUILD_MAX;buildRecharge=0;return}if(buildCharges>=max){buildRecharge=0;return}buildRecharge+=dt;while(buildRecharge>=rate&&buildCharges<max){buildRecharge-=rate;buildCharges++;if(buildCharges===max)buildRecharge=0}}

function drawUltimateReadyAura(ent,label=''){
  if(!ent||ent.dead||ent.alien)return;
  const ready=getUltimateReady(ent),clip=ent.ultClipShots>0;
  if(!ready&&!clip)return;
  if(!visualVisible(ent.x,ent.y-.85,ent.z))return;
  const q=project(ent.x,ent.y-.86,ent.z);if(!q)return;
  const weapon=ent.ultClipWeapon||ent.weapon,color=ultimateColor(weapon),pulse=.72+.28*Math.sin(performance.now()/115),r=Math.max(22,Math.min(54,q.s*.085))*pulse;
  ctx.save();ctx.globalCompositeOperation='lighter';ctx.globalAlpha=clip?.72:.48;ctx.strokeStyle=color;ctx.fillStyle=color;ctx.shadowColor=color;ctx.shadowBlur=26;
  ctx.lineWidth=clip?4:3;ctx.beginPath();ctx.ellipse(q.x,q.y,r*.92,r*1.35,0,0,TAU);ctx.stroke();
  ctx.globalAlpha=clip?.28:.18;ctx.beginPath();ctx.ellipse(q.x,q.y,r*1.18,r*1.68,0,0,TAU);ctx.fill();
  ctx.globalAlpha=.85;ctx.font='900 10px Arial';ctx.textAlign='center';ctx.fillStyle='#fff';ctx.shadowBlur=12;ctx.fillText(clip?'ULT CLIP':'ULT READY',q.x,q.y-r*1.55);
  ctx.restore();
}
function drawUltimateAurasForView(ent){
  for(const b of bots)if(!b.dead&&b!==ent)drawUltimateReadyAura(b);
  if(ent!==player&&!player.dead)drawUltimateReadyAura(player,'P1');
  if(player2&&ent!==player2&&!player2.dead)drawUltimateReadyAura(player2,'P2');
}
function drawBuildIntentZones(p){
  const hit=p?.hit,s=hit?.piece,mode=p?.selectedType||p?.type||buildTypeFor(viewEnt||buildActor||player);
  if(!hit||!s||s.active===false)return;
  ctx.save();ctx.globalCompositeOperation='lighter';ctx.lineWidth=2.5;ctx.shadowBlur=18;
  if(s.type==='wall'){
    const top=s.y+s.h/2+.035,orientation=wallOrientation(s);
    if(mode==='platform'){
      const platformColor='rgba(211,107,255,.28)';ctx.shadowColor='#d36bff';
      if(orientation==='x'){
        drawPoly([[s.x-s.w/2,top,s.z-s.d/2-.18],[s.x+s.w/2,top,s.z-s.d/2-.18],[s.x+s.w/2,top,s.z-s.d/2+.55],[s.x-s.w/2,top,s.z-s.d/2+.55]],platformColor,'#d36bff',2.5);
        drawPoly([[s.x-s.w/2,top,s.z+s.d/2-.55],[s.x+s.w/2,top,s.z+s.d/2-.55],[s.x+s.w/2,top,s.z+s.d/2+.18],[s.x-s.w/2,top,s.z+s.d/2+.18]],platformColor,'#d36bff',2.5);
      }else{
        drawPoly([[s.x-s.w/2-.18,top,s.z-s.d/2],[s.x-s.w/2+.55,top,s.z-s.d/2],[s.x-s.w/2+.55,top,s.z+s.d/2],[s.x-s.w/2-.18,top,s.z+s.d/2]],platformColor,'#d36bff',2.5);
        drawPoly([[s.x+s.w/2-.55,top,s.z-s.d/2],[s.x+s.w/2+.18,top,s.z-s.d/2],[s.x+s.w/2+.18,top,s.z+s.d/2],[s.x+s.w/2-.55,top,s.z+s.d/2]],platformColor,'#d36bff',2.5);
      }
    }else{
      const wallColor='rgba(0,234,255,.24)';ctx.shadowColor='#00eaff';
      if(orientation==='x')drawPoly([[s.x-s.w/2+.35,top+.05,s.z-.20],[s.x+s.w/2-.35,top+.05,s.z-.20],[s.x+s.w/2-.35,top+.05,s.z+.20],[s.x-s.w/2+.35,top+.05,s.z+.20]],wallColor,'#00eaff',2.5);
      else drawPoly([[s.x-.20,top+.05,s.z-s.d/2+.35],[s.x+.20,top+.05,s.z-s.d/2+.35],[s.x+.20,top+.05,s.z+s.d/2-.35],[s.x-.20,top+.05,s.z+s.d/2-.35]],wallColor,'#00eaff',2.5);
    }
  }else if(s.type==='platform'&&mode==='platform'){
    const top=s.y+s.h+.04,edge='rgba(211,107,255,.22)';ctx.shadowColor='#d36bff';
    drawPoly([[s.x-s.w/2,top,s.z-s.d/2],[s.x+s.w/2,top,s.z-s.d/2],[s.x+s.w/2,top,s.z-s.d/2+.7],[s.x-s.w/2,top,s.z-s.d/2+.7]],edge,'#d36bff',2);
    drawPoly([[s.x-s.w/2,top,s.z+s.d/2-.7],[s.x+s.w/2,top,s.z+s.d/2-.7],[s.x+s.w/2,top,s.z+s.d/2],[s.x-s.w/2,top,s.z+s.d/2]],edge,'#d36bff',2);
    drawPoly([[s.x-s.w/2,top,s.z-s.d/2],[s.x-s.w/2+.7,top,s.z-s.d/2],[s.x-s.w/2+.7,top,s.z+s.d/2],[s.x-s.w/2,top,s.z+s.d/2]],edge,'#d36bff',2);
    drawPoly([[s.x+s.w/2-.7,top,s.z-s.d/2],[s.x+s.w/2,top,s.z-s.d/2],[s.x+s.w/2,top,s.z+s.d/2],[s.x+s.w/2-.7,top,s.z+s.d/2]],edge,'#d36bff',2);
  }
  ctx.restore();
}
function drawBuildPreview(p){
  if(!p)return;const pts=orientedPoints(p.x,p.y,p.z,p.w,p.h,p.d,0),faces=[[4,5,6,7],[0,4,7,3],[1,2,6,5],[0,1,5,4],[2,3,7,6]],valid=p.valid,fill=valid?'rgba(35,240,220,.12)':'rgba(165,64,80,.12)',edge=valid?(p.type==='platform'?'#bf8cff':'#65f7ff'):'#bb6d7a';
  ctx.save();ctx.globalAlpha=.9;for(const f of faces)drawPoly(f.map(i=>pts[i]),fill,edge,1.6);
  // The highlighted top is the exact surface that vertical collision will land on.
  drawPoly([[p.x-p.w/2,pieceTopY(p),p.z-p.d/2],[p.x+p.w/2,pieceTopY(p),p.z-p.d/2],[p.x+p.w/2,pieceTopY(p),p.z+p.d/2],[p.x-p.w/2,pieceTopY(p),p.z+p.d/2]],valid?'rgba(90,255,240,.18)':'rgba(165,64,80,.12)',edge,2);
  if(!valid){const q=project(p.x,pieceTopY(p)+.2,p.z);if(q){ctx.font='600 10px system-ui';ctx.textAlign='center';ctx.lineWidth=3;ctx.strokeStyle='rgba(4,8,20,.85)';ctx.strokeText(p.reason||'BLOCKED',q.x,q.y);ctx.fillStyle='#e5a5af';ctx.fillText(p.reason||'BLOCKED',q.x,q.y);}}
  ctx.restore();
}
function drawBuildFeedback(){drawBuildFeedbackFor(player)}
function drawStructureHealth(){const hit=rayWorld(actorEyePosition(player),dirFrom(player,0),BUILD_RANGE,true),s=hit?.block?.structure;if(!s||!s.destructible||!Number.isFinite(s.hp))return;const ratio=clamp(s.hp/s.maxHp,0,1),w=150,x=W/2-w/2,y=H/2+55;ctx.save();ctx.fillStyle='rgba(5,10,24,.75)';ctx.fillRect(x-4,y-19,w+8,30);ctx.fillStyle=ratio<.25?'#ff315d':ratio<.5?'#ffb02e':'#65f7ff';ctx.fillRect(x,y,w*ratio,7);ctx.strokeStyle='rgba(255,255,255,.7)';ctx.strokeRect(x,y,w,7);ctx.fillStyle='#fff';ctx.font='bold 12px system-ui';ctx.textAlign='center';ctx.fillText(s.type.toUpperCase()+' '+Math.ceil(s.hp)+' / '+s.maxHp+((s.bonusHp||0)>0?' + '+Math.ceil(s.bonusHp)+' BOOST':''),W/2,y-5);ctx.restore()}
function shadeColor(hex,amt){const n=parseInt(hex.slice(1),16),r=clamp((n>>16)+amt,0,255)|0,g=clamp(((n>>8)&255)+amt,0,255)|0,b=clamp((n&255)+amt,0,255)|0;return '#'+((1<<24)+(r<<16)+(g<<8)+b).toString(16).slice(1)}
function mixColor(a,b,t){const A=parseInt(a.slice(1),16),B=parseInt(b.slice(1),16),ch=s=>[(s>>16)&255,(s>>8)&255,s&255],aa=ch(A),bb=ch(B),v=aa.map((n,i)=>Math.round(n+(bb[i]-n)*clamp(t,0,1)));return '#'+((1<<24)+(v[0]<<16)+(v[1]<<8)+v[2]).toString(16).slice(1)}
function cameraPoint(x,y,z){const cam=viewEnt||player,eye=actorEyePosition(cam),dx=x-eye.x,dy=y-eye.y,dz=z-eye.z,yaw=cam.yaw+(cam.recoilYaw||0),pitch=cam.pitch-(cam.recoilPitch||0);const sy=Math.sin(yaw),cy=Math.cos(yaw);let x1=dx*cy-dz*sy,z1=dx*sy+dz*cy;const cp=Math.cos(-pitch),sp=Math.sin(-pitch),y1=dy*cp-z1*sp;z1=dy*sp+z1*cp;const roll=(cam.recoilRoll||0)+(cam.slidePose||0)*.018,cr=Math.cos(roll),sr=Math.sin(roll),fov=(1-(cam.slidePose||0)*.065)*(1+(cam.aimPose||0)*.28);return {x:(x1*cr-y1*sr)*fov,y:(x1*sr+y1*cr)*fov,z:z1}}
function projectCam(p){const f=viewRect.h*.72/p.z;return {x:viewRect.x+viewRect.w/2+p.x*f,y:viewRect.y+viewRect.h/2-p.y*f,z:p.z,s:f}}
function project(x,y,z){const p=cameraPoint(x,y,z);if(p.z<.08)return null;return projectCam(p)}
function clipCamPoly(poly,near=.08){let out=[];for(let i=0;i<poly.length;i++){const a=poly[i],b=poly[(i+1)%poly.length],ain=a.z>=near,bin=b.z>=near;if(ain&&bin)out.push(b);else if(ain&&!bin){const t=(near-a.z)/(b.z-a.z);out.push({x:a.x+(b.x-a.x)*t,y:a.y+(b.y-a.y)*t,z:near})}else if(!ain&&bin){const t=(near-a.z)/(b.z-a.z);out.push({x:a.x+(b.x-a.x)*t,y:a.y+(b.y-a.y)*t,z:near});out.push(b)}}return out}
function drawPoly(points,fill,stroke,line=2){let cam=points.map(p=>cameraPoint(p[0],p[1],p[2]));cam=clipCamPoly(cam);if(cam.length<3)return;const ps=cam.map(projectCam);ctx.beginPath();ctx.moveTo(ps[0].x,ps[0].y);for(let i=1;i<ps.length;i++)ctx.lineTo(ps[i].x,ps[i].y);ctx.closePath();ctx.fillStyle=fill;ctx.fill();if(stroke){ctx.strokeStyle=stroke;ctx.lineWidth=line;ctx.shadowBlur=0;ctx.stroke()}}
function hexRgb(hex){const n=parseInt(hex.slice(1),16);return [((n>>16)&255)/255,((n>>8)&255)/255,(n&255)/255]}
function compileShader(type,source){const sh=gl.createShader(type);gl.shaderSource(sh,source);gl.compileShader(sh);if(!gl.getShaderParameter(sh,gl.COMPILE_STATUS))throw new Error(gl.getShaderInfoLog(sh));return sh}
const worldProgram=gl.createProgram();
gl.attachShader(worldProgram,compileShader(gl.VERTEX_SHADER,'attribute vec3 aPos;attribute vec3 aColor;uniform vec3 uCamera;uniform vec3 uRight;uniform vec3 uUp;uniform vec3 uForward;uniform float uAspect;varying vec3 vColor;void main(){vec3 delta=aPos-uCamera;vec3 camera=vec3(dot(delta,uRight),dot(delta,uUp),dot(delta,uForward));float near=.055;float far=180.0;float A=(far+near)/(far-near);float B=-2.0*far*near/(far-near);gl_Position=vec4(camera.x*1.44*uAspect,camera.y*1.44,A*camera.z+B,camera.z);vColor=aColor;}'));
gl.attachShader(worldProgram,compileShader(gl.FRAGMENT_SHADER,'precision mediump float;varying vec3 vColor;void main(){gl_FragColor=vec4(vColor,1.0);}'));
gl.linkProgram(worldProgram);if(!gl.getProgramParameter(worldProgram,gl.LINK_STATUS))throw new Error(gl.getProgramInfoLog(worldProgram));
const worldBuffer=gl.createBuffer(),cachedSolidBuffer=gl.createBuffer(),cachedLineBuffer=gl.createBuffer(),worldPos=gl.getAttribLocation(worldProgram,'aPos'),worldColor=gl.getAttribLocation(worldProgram,'aColor'),worldCamera=gl.getUniformLocation(worldProgram,'uCamera'),worldRight=gl.getUniformLocation(worldProgram,'uRight'),worldUp=gl.getUniformLocation(worldProgram,'uUp'),worldForward=gl.getUniformLocation(worldProgram,'uForward'),worldAspect=gl.getUniformLocation(worldProgram,'uAspect');
const worldCache={solidCount:0,lineCount:0,lastBuild:0,valid:false};
let solidTriangles=[],lineSegments=[];
let neonModelRenderer=null;try{neonModelRenderer=window.NeonModels?.createRenderer(gl,{resolve:path=>path.startsWith('items/')?'./assets/survival/common/'+path:/^characters\/alien_(leaper|stalker|screecher|artillery|lurker|warder)\.glb$/.test(path)?'./assets/survival/common/aliens/'+path.split('/').pop():window.NeonAssets?.url(path)||''})||null;}catch(error){console.warn('Neon Breach models unavailable; using procedural graphics.',error);}
let projectileModelRenderer=null;try{projectileModelRenderer=window.NeonProjectileModels?.createRenderer(gl)||null;}catch(error){console.warn('Neon Breach projectile meshes unavailable',error);}
let modelWorldActors=[],modelHeldWeapons=[];
const renderedModelGuns=new WeakMap();
function actorModelPath(b){if(warMode()&&b.factionCharacter)return window.NeonFactions.characters.find(c=>c.id===b.factionCharacter)?.model||'characters/player_one.glb';if(b.alien)return 'characters/alien_'+b.alienKind+'.glb';if(b===player||b.entityId==='p1')return survivalProgress?.getEquippedCharacterModelPath?.()||'characters/player_one.glb';if(b===player2||b.entityId==='p2')return 'characters/player_two.glb';return 'characters/'+({rusher:'bot_breacher',marksman:'bot_sentinel',rifler:'bot_vanguard',skirmisher:'bot_flanker',heavy:'bot_demolisher'}[b.style]||'bot_vanguard')+'.glb';}
function importedModelCamera(){const c=viewEnt||player,eye=actorEyePosition(c),x=cameraPoint(eye.x+1,eye.y,eye.z),y=cameraPoint(eye.x,eye.y+1,eye.z),z=cameraPoint(eye.x,eye.y,eye.z+1);return {position:[eye.x,eye.y,eye.z],right:[x.x,y.x,z.x],up:[x.y,y.y,z.y],forward:[x.z,y.z,z.z],aspect:viewRect.h/viewRect.w,fovScale:(1-(c.slidePose||0)*.065)*(1+(c.aimPose||0)*.28)};}
function modelWeaponState(ent){const w=WEAPONS[ent.weapon]||WEAPONS.assault;return {recoil:clamp((ent.cool||0)/(w.cool||.1),0,1),reload:ent.reload>0?clamp(1-ent.reload/(ent.reloadTotal||w.reload||1.5),0,1):0,charge:ent.charge||0,finishTint:warMode()&&ent.human&&window.NeonWarUI.selection().livery?hexRgb(ent.team===1?'#e8b66c':'#82dfbd'):game.mode==='survivor'&&survivalReward('finish')?hexRgb(survivalReward('finish').palette[1]):null};}
function importedGunPose(ent){
  const w=WEAPONS[ent.weapon]||WEAPONS.assault,rld=reloadVisualState(ent,w),speed=Math.hypot(ent.vx,ent.vz),now=performance.now(),bob=ent.onGround?Math.sin(now/95)*Math.min(.035,speed*.0024):0,idle=Math.sin(now/900)*.004,kick=ent.reload>0?0:Math.min(.12,(ent.cool||0)/(w.cool||.1)*.10),reloadKick=rld.active?Math.sin(Math.PI*rld.progress)*.10:0,swap=Math.min(1,(ent.weaponSwitchTimer||0)/.18);
  const pose={bx:Math.min(.43,viewRect.w/viewRect.h*.28)-(rld.active?.05*Math.sin(Math.PI*rld.progress):0)+idle,by:-.54+bob*(1-(ent.slidePose||0))-kick*.35+reloadKick*.55-(ent.landingImpact||0)*.04-(ent.slidePose||0)*.07-swap*.13,bz:.86-kick-reloadKick*.25-(ent.slidePose||0)*.035,tilt:(rld.tilt||0)+(ent.slidePose||0)*.16+swap*.10,scale:.68};
  const aim=ent.aimPose||0;pose.bx*=1-aim;pose.by=pose.by*(1-aim)-.18*aim;pose.bz+=.10*aim;return pose;
}
function importedGunMatrix(pose,camera){const basis=window.NeonModels.compose(camera.position);for(let i=0;i<3;i++){basis[i]=camera.right[i]/(camera.fovScale||1);basis[i+4]=camera.up[i]/(camera.fovScale||1);basis[i+8]=camera.forward[i];}return window.NeonModels.multiply(basis,window.NeonModels.multiply(window.NeonModels.compose([0,0,0],[-pose.tilt,0,0]),window.NeonModels.compose([pose.bx,pose.by,pose.bz],[0,0,0],[pose.scale,pose.scale,pose.scale])));}
function allyHighlightForView(actor,viewer=viewEnt||player){return !!(['tdm','conquest','campaign'].includes(game.mode)&&actor&&viewer&&actor!==viewer&&!actor.dead&&!actor.alien&&sameTeam(viewer,actor));}
function renderProjectileModels(camera){
  if(!projectileModelRenderer)return;
  const renderer=projectileModelRenderer,now=performance.now()/1000,limit=visualQuality==='low'?16:32;
  renderer.beginView(camera,{quality:visualQuality});let drawn=0;
  for(const p of projectiles){
    if(p.life<=0||!p.owner||p.owner.dead||!['punch','harpoon'].includes(p.kind))continue;
    if(drawn++>=limit)break;
    const muzzle=muzzlePoint(p.owner,{x:p.dx,y:p.dy,z:p.dz});
    if(p.kind==='punch')renderer.drawPunch(p,{muzzle});else renderer.drawHarpoon(p,{muzzle});
  }
  // Keep a hit spear attached to the actual impact point while the existing
  // grapple/pull state resolves. These records are visual-only snapshots.
  const actors=[...new Set([player,player2,...bots].filter(Boolean))];
  for(const owner of actors){
    const hit=owner.harpoonVisualAnchor;if(!hit||owner.dead||hit.until<=now||hit.target?.dead)continue;
    const point=hit.target?{x:hit.target.x+hit.offset.x,y:hit.target.y+hit.offset.y,z:hit.target.z+hit.offset.z}:hit;
    const muzzle=muzzlePoint(owner,{x:hit.dx,y:hit.dy,z:hit.dz});
    renderer.drawHarpoon({...point,kind:'harpoon',dx:hit.dx,dy:hit.dy,dz:hit.dz,color:hit.color,ultimate:hit.ultimate,life:1},{muzzle});
  }
  for(const ent of actors){
    const state=ent.harpoonPull||ent.harpoonTether,owner=state?.owner;
    if(!state||ent.dead||!owner||owner.dead||owner.harpoonVisualAnchor?.until>now)continue;
    const point={x:ent.x,y:ent.y-.7,z:ent.z},dx=point.x-owner.x,dy=point.y-owner.y,dz=point.z-owner.z,len=Math.hypot(dx,dy,dz)||1;
    renderer.drawCable(muzzlePoint(owner,{x:dx/len,y:dy/len,z:dz/len}),point,{color:'#bdfcff'});
  }
}
function renderImportedModels(){
  renderProjectileModels(importedModelCamera());
  if(!neonModelRenderer)return;const now=performance.now()/1000,camera=importedModelCamera(),v=visualPreset(),budget=visualQuality==='low'?4:(visualQuality==='medium'?8:12),animatedRange=visualQuality==='low'?v.near*.65:v.near;
  neonModelRenderer.beginView(camera,now);modelWorldActors.sort((a,b)=>a.distance-b.distance);for(const item of modelWorldActors)neonModelRenderer.drawActor(item.path,item.actor,{now,animate:item.distance<animatedRange||item.actor.boss,animationBudget:budget,eyeHeight:PLAYER_H,allyHighlight:allyHighlightForView(item.actor),teamTint:game.mode==='survivor'&&!item.actor.alien&&survivalReward('armor')?hexRgb(survivalReward('armor').palette[1]):null});
  for(const item of modelHeldWeapons){const b=item.actor,yaw=b.faceYaw??b.yaw??0,fx=Math.sin(yaw),fz=Math.cos(yaw),sx=Math.cos(yaw),sz=-Math.sin(yaw),rld=reloadVisualState(b,WEAPONS[b.weapon]),sc=b.alien?.50*alienVisualFactor(b):.70;const m=window.NeonModels.compose([b.x+fx*(b.factionCharacter?.43:.50)+sx*(b.factionCharacter?.32:.52),(b.alien?alienTargetY(b,b.y-.86):humanTargetY(b,'body')-.10),b.z+fz*(b.factionCharacter?.43:.50)+sz*(b.factionCharacter?.32:.52)],[-(b.pitch||0)-rld.tilt*.65,yaw,0],[sc,sc,sc]);neonModelRenderer.drawWeapon(item.path,m,modelWeaponState(b));}
  renderSalvageModels(now);
  for(const corpse of neonModelRenderer.recentDeaths(now)){const b=corpse.actor,cam=cameraPoint(b.x,b.y-.7,b.z);if(cam.z>.1&&cam.z<v.mid&&Math.abs(cam.x)<cam.z*viewRect.w/(1.44*viewRect.h)+4)neonModelRenderer.drawActor(corpse.path,b,{now,animate:false,eyeHeight:PLAYER_H,deathProgress:clamp((now-corpse.time)/.62,0,1)});}
  const ent=viewEnt||player;renderedModelGuns.delete(ent);if(game.started&&!ent.dead){const path='weapons/'+ent.weapon+'.glb',model=neonModelRenderer.get(path),pose=importedGunPose(ent);if(model&&neonModelRenderer.drawWeapon(path,importedGunMatrix(pose,camera),modelWeaponState(ent),true))renderedModelGuns.set(ent,{model,pose});}
}
function cameraClipPoint(p){const c=cameraPoint(p[0],p[1],p[2]),near=.055,far=180,A=(far+near)/(far-near),B=-2*far*near/(far-near);return [c.x*1.44*viewRect.h/viewRect.w,c.y*1.44,A*c.z+B,c.z]}
function addTriangle(a,b,c,color){const rgb=hexRgb(color);solidTriangles.push({p:[a,b,c],rgb})}
function addQuad(a,b,c,d,color,edge='#17253a'){addTriangle(a,b,c,color);addTriangle(a,c,d,color);if(edge){addLine(a,b,edge);addLine(b,c,edge);addLine(c,d,edge);addLine(d,a,edge)}}
function addLine(a,b,color){lineSegments.push({p:[a,b],rgb:hexRgb(color)})}
function addBoxGeometry(o,color,line=1){const x=o.x,z=o.z,hw=o.w/2,hd=o.d/2,y0=o.y-o.h/2,y1=o.y+o.h/2,A=[x-hw,y0,z-hd],B=[x+hw,y0,z-hd],C=[x+hw,y0,z+hd],D=[x-hw,y0,z+hd],E=[x-hw,y1,z-hd],F=[x+hw,y1,z-hd],G=[x+hw,y1,z+hd],Hh=[x-hw,y1,z+hd];addQuad(E,F,G,Hh,shadeColor(color,28));addQuad(D,C,B,A,shadeColor(color,-44));addQuad(A,E,Hh,D,shadeColor(color,-28));addQuad(B,C,G,F,shadeColor(color,-36));addQuad(A,B,F,E,shadeColor(color,-15));addQuad(C,D,Hh,G,shadeColor(color,-22))}
function orientedPoints(x,y,z,w,h,d,yaw){const sx=Math.cos(yaw),sz=-Math.sin(yaw),fx=Math.sin(yaw),fz=Math.cos(yaw),hw=w/2,hd=d/2,y0=y-h/2,y1=y+h/2,pt=(a,b,yy)=>[x+sx*a+fx*b,yy,z+sz*a+fz*b];return [pt(-hw,-hd,y0),pt(hw,-hd,y0),pt(hw,hd,y0),pt(-hw,hd,y0),pt(-hw,-hd,y1),pt(hw,-hd,y1),pt(hw,hd,y1),pt(-hw,hd,y1)]}
function addOrientedBox(x,y,z,w,h,d,yaw,color){const p=orientedPoints(x,y,z,w,h,d,yaw);addQuad(p[4],p[5],p[6],p[7],shadeColor(color,18));addQuad(p[3],p[2],p[1],p[0],shadeColor(color,-42));addQuad(p[0],p[4],p[7],p[3],shadeColor(color,-30));addQuad(p[1],p[2],p[6],p[5],shadeColor(color,-36));addQuad(p[0],p[1],p[5],p[4],shadeColor(color,-18));addQuad(p[2],p[3],p[7],p[6],shadeColor(color,-24))}
function addFloorTiles(){const y=.012,size=gridSize,min=-44,max=44;for(let z=min,zi=0;z<max;z+=size,zi++)for(let x=min,xi=0;x<max;x+=size,xi++){const x1=Math.min(max,x+size),z1=Math.min(max,z+size),major=xi%4===0||zi%4===0,base=(xi+zi)&1?mapTheme.floorA:mapTheme.floorB;addQuad([x,y,z],[x1,y,z],[x1,y,z1],[x,y,z1],base,major?mapTheme.grid:mapTheme.gridSoft)}}
function addBuildGridGuide(){const bs=humanBuildState(viewEnt||player);if(!bs.held)return;const actor=viewEnt||player,radius=24,gx0=Math.floor((actor.x-radius)/gridSize),gx1=Math.ceil((actor.x+radius)/gridSize),gz0=Math.floor((actor.z-radius)/gridSize),gz1=Math.ceil((actor.z+radius)/gridSize),top=BUILD_MAX_STACK*BUILD_LEVEL,col=mapTheme.gridSoft;for(let gx=gx0;gx<=gx1;gx++){const x=gx*gridSize;for(let gz=gz0;gz<=gz1;gz++){const z=gz*gridSize;addLine([x,0.05,z],[x,top,z],col)}}for(let gy=1;gy<=BUILD_MAX_STACK;gy++){const y=gy*BUILD_LEVEL+.018;for(let gx=gx0;gx<gx1;gx+=2){const x=gx*gridSize;addLine([x,y,gz0*gridSize],[x,y,gz1*gridSize],col)}for(let gz=gz0;gz<gz1;gz+=2){const z=gz*gridSize;addLine([gx0*gridSize,y,z],[gx1*gridSize,y,z],col)}}}
function addBotGeometryByDistance(b){
  if(b.burrowed)return;const cam=cameraPoint(b.x,b.y-.7,b.z),v=visualPreset(),radius=b.alien?Math.max(2.5,(b.alienScale||1)*2.5)*alienVisualFactor(b):2;
  if(cam.z<-radius||cam.z>v.far)return;const halfW=Math.max(0,cam.z)*viewRect.w/(1.44*viewRect.h),halfH=Math.max(0,cam.z)/1.44;
  if(Math.abs(cam.x)>halfW+radius||Math.abs(cam.y)>halfH+radius)return;
  const path=actorModelPath(b),distance=Math.hypot(cam.x,cam.y,cam.z);
  if(neonModelRenderer?.get(path)){modelWorldActors.push({path,actor:b,distance});if(!b.alien||b.rangedGun)addHeldGunGeometry(b,b.faceYaw??b.yaw??0);}
}
function addHeldGunGeometry(b,yaw){const path='weapons/'+b.weapon+'.glb';if(neonModelRenderer?.get(path))modelHeldWeapons.push({path,actor:b});}

function renderSalvageModels(now){
  for(const p of pickups){if(p.gone||p.taken>0||visualDistance(p.x,p.y,p.z)>visualPreset().far)continue;
    const path=p.kind==='weapon'?'weapons/'+p.type+'.glb':'items/'+p.kind+'.glb',scale=p.kind==='weapon'?.85:1.3;
    const matrix=window.NeonModels.compose([p.x,p.y+.28+Math.sin(now*2+p.x)*.12,p.z],[0,now*.65,0],[scale,scale,scale]);
    neonModelRenderer.drawWeapon(path,matrix,{});
  }
  if(game.mode!=='survivor')return;
  for(const d of survivalEquipment?.devices||[]){const y=d.id==='decoy'?d.y-PLAYER_H:d.y,matrix=window.NeonModels.compose([d.x,y,d.z],[0,0,0],[1.6,1.6,1.6]);neonModelRenderer.drawWeapon('items/'+d.id+'.glb',matrix,{});
    if(d.id==='decoy')neonModelRenderer.drawActor(actorModelPath(player),{...player,x:d.x,y:y+PLAYER_H,z:d.z,hp:d.hp,armor:0,faceYaw:now*.4},{now,animate:false,teamTint:hexRgb('#65faff')});
  }
}

function survivalReward(kind){if(game.mode!=='survivor')return null;const selected=survivalProgress?.getSelection();return window.NeonSurvivalProgress?.rewards.find(r=>r.id===selected?.[kind])||null;}

async function startArenaMatch(){
  if(survivalStarting)return;survivalStarting=true;const button=el('start'),label=button.querySelector('span'),original=label?.textContent||'ENTER THE ARENA';button.disabled=true;
  const mode=el('mode').value,arena=survivalInterface?.selectedMap||'breach_ring',mapId=el('map')?.value||'grid',materialMap=mode==='survivor'?arena:window.NeonWarMaps?.get(mapId)?.tileset||ARENA_TILESETS[mapId]||ARENA_TILESETS.grid,characterPath=actorModelPath(player);
  try{
    if(!neonModelRenderer)throw Error('The model renderer is unavailable.');
    if(!survivalMapRuntime?.load)throw Error('The arena tilesets are unavailable.');
    const paths=['tdm','campaign','conquest'].includes(mode)?[...window.NeonFactions.characters.map(c=>c.model),...window.NeonFactions.weapons.map(w=>'weapons/'+w.id+'.glb'),...(mode==='tdm'?Object.values(window.NeonAssets.weapons):[]),'items/health.glb','items/armor.glb']:[...Object.values(window.NeonAssets.weapons),...window.NeonModels.HUMANS.map(id=>'characters/'+id+'.glb'),characterPath,...(mode==='survivor'?window.NeonModels.ALIENS.map(id=>'characters/alien_'+id+'.glb'):[]),'items/health.glb','items/armor.glb'];
    if(mode==='survivor'){if(!survivalProgress?.isUnlocked(arena))throw Error('Clear the required wave to unlock this arena.');for(const kind of SURVIVAL_NEW_KINDS)paths.push('characters/alien_'+kind+'.glb');for(const gear of window.NeonSurvivalProgress.equipment)paths.push('items/'+gear.id+'.glb');}
    await Promise.all([neonModelRenderer.preload(paths,{retry:true,onProgress:(ready,total)=>{if(label)label.textContent='LOADING ASSETS '+ready+'/'+total;}}),survivalMapRuntime.load(materialMap)]);
    if(el('mode').value!==mode||actorModelPath(player)!==characterPath||mode==='survivor'&&survivalInterface.selectedMap!==arena||mode!=='survivor'&&el('map').value!==mapId)throw Error('Selection changed while loading. Press Enter the Arena again.');
    el('scoreboardOverlay').style.display='none';el('menu').style.display='none';reset();updateMobileControlVisibility(true);if(game.p1Input==='keyboard')try{canvas.requestPointerLock?.()?.catch?.(()=>{})}catch{};
  }catch(error){console.error('Neon Breach asset loading:',error);el('menu').style.display='flex';el('mapDescription').textContent='ASSET LOAD FAILED · '+error.message+' Check the model host and try again.';}
  finally{survivalStarting=false;button.disabled=false;if(label)label.textContent=original;}
}

function initializeSurvival(){
  survivalController=window.NeonSurvivalAI?.createController({humans:survivorHumans,aliens:survivorAliveAliens,structures:allActiveStructures,lineClear,physics,topAtPoint,damage,damageStructure,
    eyeHeight:PLAYER_H,time:()=>game.survivalElapsed||0,bounds:()=>ARENA_SAFE,decoys:()=>survivalEquipment?.decoys()||[],map:()=>game.survivalMap||'breach_ring',navigation:()=>survivalMapRuntime.navigation(),revision:()=>survivalMapRuntime.revision,
    pointInfo:(x,z,a)=>{const info=survivalMapRuntime.pointInfo(x,z,a),physical=tacticalPointInfo(x,z,a);return {...info,...physical,destructible:!!physical.obstacle?.destructible};},
    canTraverse:(from,to,a)=>{
      if(a.flying){const dx=to.x-from.x,dy=to.y-from.y,dz=to.z-from.z,length=Math.hypot(dx,dy,dz)||1;return !rayWorld({x:from.x,y:from.y-.65,z:from.z},{x:dx/length,y:dy/length,z:dz/length},length,false,(a.r||.6)*.8);}
      return survivalMapRuntime.canTraverse(from,to,a)&&tacticalCanTraverse(from,to,a);
    },
    warn:(x,z,type,color,life,r)=>addBossWarning(x,z,type,color,life,r),burst,
    acid:zone=>{game.acidZones??=[];if(game.acidZones.length>=12)game.acidZones.shift();game.acidZones.push({...zone,y:Number.isFinite(zone.y)?zone.y:topAtPoint(zone.x,zone.z),tick:0,maxLife:zone.life||7});}
  });
  survivalInterface=window.NeonSurvivalUI?.create({onSelect:()=>updateMenuSummary(),onRetry:id=>{survivalInterface.selectMap(id);el('mode').value='survivor';startArenaMatch();},onMapSelection:()=>{el('returnMenu').click();el('mode').value='survivor';updateMenuSummary();},onMainMenu:()=>{el('returnMenu').click();el('mode').value='dm';updateMenuSummary();}});
  survivalInterface?.mount(el('survivorArenaCard'));survivalInterface?.mountCharacters(el('characterLocker'));if(menuActiveMode==='survivor')restoreSurvivalMenuMap();updateMenuSummary();
  const gear=document.createElement('div');gear.id='survivalGear';gear.hidden=true;gear.innerHTML='<button type="button" id="survivalDeploy" aria-label="Deploy equipment" title="Deploy equipment">⚙</button><button type="button" id="survivalCycle" aria-label="Select next tactical equipment" title="Next equipment">↔</button><span id="survivalGearStatus"></span>';el('hud').appendChild(gear);el('survivalDeploy').onclick=()=>deploySurvivalEquipment(player);el('survivalCycle').onclick=cycleSurvivalEquipment;
  addEventListener('keydown',event=>{if(game.mode!=='survivor'||!game.started||game.over||game.shopOpen||event.repeat)return;if(event.code==='KeyE')deploySurvivalEquipment(player);if(event.code==='Numpad3'&&player2)deploySurvivalEquipment(player2);if(event.code==='KeyC')cycleSurvivalEquipment();});
  addEventListener('pagehide',()=>{if(game.mode==='survivor')survivalProgress?.checkpoint();});
}

function deploySurvivalEquipment(actor){if(game.mode!=='survivor'||game.over||game.shopOpen||game.upgradesOpen)return;if(!survivalEquipment?.deploy(actor))msg('NO CHARGE, COOLDOWN, OR DEPLOYMENT LIMIT');}
function cycleSurvivalEquipment(){const items=window.NeonSurvivalProgress.equipment.filter(e=>survivalProgress.isEquipmentUnlocked(e.id));const index=items.findIndex(e=>e.id===survivalEquipment.selected);survivalEquipment.select(items[(index+1)%items.length].id);}
function updateSurvivalGearHud(){
  const gear=el('survivalGear');if(!gear)return;gear.hidden=game.mode!=='survivor'||!game.started||game.over||game.shopOpen;
  const area=splitRects()[0],w=Math.min(game.p1Input==='touch'||area.w<500?178:198,area.w-20);gear.style.left=(area.x+w-50)+'px';gear.style.top=(area.y+76)+'px';
  if(game.hudViewWidth!==area.w){document.documentElement.style.setProperty('--local-view-width',area.w+'px');game.hudViewWidth=area.w}
  const state=survivalEquipment?.state();if(!state)return;const item=window.NeonSurvivalProgress.equipment.find(e=>e.id===state.selected),count=state.charges[item.id]||0,cool=Math.ceil(state.cooldowns[item.id]||0);
  el('survivalGearStatus').textContent=item.name+' · '+count+' CHARGES'+(cool?' · '+cool+'s':'');el('survivalDeploy').setAttribute('aria-label','Deploy '+item.name+', '+count+' charges'+(cool?', ready in '+cool+' seconds':''));el('survivalDeploy').disabled=!count||cool>0;
}

function updateSurvivalMapEvents(dt){
  const result=survivalMapRuntime?.updateEvents(game.survivalElapsed,game.survivorWave);if(!result)return;
  if(result.changed){markBlocksDirty();survivalController?.invalidate();}
  game.environmentEvents=result.events;game.environmentStates??={};game.environmentTick=(game.environmentTick||0)-dt;
  for(const event of result.events){if(event.state==='warning'&&game.environmentStates[event.index]!=='warning'){survivalInterface?.warn(({electric:'ELECTRICAL SURGE · LEAVE THE LIT CHANNEL',acid:'ACID VENT · LEAVE THE MARKED GROUND',freeze:'COOLING FAILURE · FROST FOG INCOMING',pulse:'HEART PULSE · LEAVE THE GLOWING VEINS',growth:'LIVING PASSAGE · GROWTH INCOMING'})[event.kind]||'ENVIRONMENTAL WARNING','hazard',4000);addBossWarning(event.x,event.z,'hazard',event.kind==='acid'?'#caff38':'#86eaff',event.warning||4,Math.min(event.w,event.d)/2);}game.environmentStates[event.index]=event.state;
    if(event.state!=='active'||!event.damage||game.environmentTick>0)continue;
    for(const human of survivorHumans()){const ground=human.y-PLAYER_H;if(Math.abs(human.x-event.x)<=event.w/2&&Math.abs(human.z-event.z)<=event.d/2&&Math.abs(ground-(event.y||0))<2.4)damage(human,event.damage,null,false,null,{minimalFx:true});}
  }
  if(game.environmentTick<=0)game.environmentTick=.75;
}

function compileWorldItems(items){const arr=new Float32Array(items.reduce((sum,item)=>sum+item.p.length*6,0));let at=0;for(const item of items)for(const p of item.p){arr[at++]=p[0];arr[at++]=p[1];arr[at++]=p[2];arr[at++]=item.rgb[0];arr[at++]=item.rgb[1];arr[at++]=item.rgb[2]}return arr}
function uploadWorldBuffer(buffer,items,usage){const arr=compileWorldItems(items);gl.bindBuffer(gl.ARRAY_BUFFER,buffer);gl.bufferData(gl.ARRAY_BUFFER,arr,usage);return arr.length/6}
function bindWorldBuffer(buffer){gl.bindBuffer(gl.ARRAY_BUFFER,buffer);gl.enableVertexAttribArray(worldPos);gl.vertexAttribPointer(worldPos,3,gl.FLOAT,false,24,0);gl.enableVertexAttribArray(worldColor);gl.vertexAttribPointer(worldColor,3,gl.FLOAT,false,24,12)}
function drawWorldBuffer(buffer,count,mode){if(!count)return;bindWorldBuffer(buffer);gl.drawArrays(mode,0,count)}
function setWorldCamera(){const c=importedModelCamera();gl.uniform3fv(worldCamera,c.position);gl.uniform3fv(worldRight,c.right);gl.uniform3fv(worldUp,c.up);gl.uniform3fv(worldForward,c.forward);gl.uniform1f(worldAspect,c.aspect)}
function uploadAndDraw(items,mode){if(!items.length)return;const count=uploadWorldBuffer(worldBuffer,items,gl.DYNAMIC_DRAW);drawWorldBuffer(worldBuffer,count,mode)}
function structureColor(p){if(!Number.isFinite(p.hp))return p.c;const damage=1-clamp(p.hp/p.maxHp,0,1),flash=p.flash>0?45:0,base=shadeColor(p.c,flash-Math.round(damage*72)),boost=clamp((p.bonusHp||0)/(p.maxBonusHp||500),0,1),seal=(p.fortifySealTimer||0)>0?.16+.06*Math.sin(performance.now()*.008):0;return mixColor(base,'#9eeeff',Math.max(seal,.18+.62*boost)*((boost>0||seal>0)?1:0))}
function addStructureSupportGlow(p){if((p.breachMarked||0)>0){const y=p.type==='platform'?p.y+p.h+.08:p.y+p.h/2+.08,r=Math.max(1.2,Math.min(4,Math.max(p.w,p.d)*.35));for(let i=0;i<8;i++){const a=i*TAU/8,b=(i+1)*TAU/8;addLine([p.x+Math.cos(a)*r,y,p.z+Math.sin(a)*r],[p.x+Math.cos(b)*r,y,p.z+Math.sin(b)*r],'#ff304c')}addLine([p.x,y+.2,p.z],[p.x,y+3.3,p.z],'#ff304c')}const boost=clamp((p.bonusHp||0)/(p.maxBonusHp||500),0,1),seal=(p.fortifySealTimer||0)>0;if(!seal&&boost<=0)return;const y0=p.type==='platform'?p.y:p.y-p.h/2,y1=p.type==='platform'?p.y+p.h:p.y+p.h/2,x0=p.x-p.w/2,x1=p.x+p.w/2,z0=p.z-p.d/2,z1=p.z+p.d/2,color=boost>.55?'#d7faff':'#75eaff';addLine([x0,y1+.025,z0],[x1,y1+.025,z0],color);addLine([x1,y1+.025,z0],[x1,y1+.025,z1],color);addLine([x1,y1+.025,z1],[x0,y1+.025,z1],color);addLine([x0,y1+.025,z1],[x0,y1+.025,z0],color);if(seal){addLine([x0,y0,z0],[x0,y1,z0],color);addLine([x1,y0,z1],[x1,y1,z1],color)}}
function addStructureDamage(p){if(!Number.isFinite(p.hp)||p.hp/p.maxHp>.5)return;const ratio=p.hp/p.maxHp,y0=p.type==='platform'?p.y+p.h+.012:p.y-p.h/2+.15,y1=p.type==='platform'?y0:p.y+p.h/2-.15,span=Math.min(p.w,p.d)*.32,count=ratio<.25?7:3,color=ratio<.25?'#05040a':'#160f1d';for(let i=0;i<count;i++){const t=(p.id*13+i*7)%19/19,xx=p.x+(t-.5)*p.w*.75,zz=p.z+(((t*11)%1)-.5)*p.d*.75;addLine([xx-span*.3,y0+(y1-y0)*t,zz-span],[xx+span*.25,y0+(y1-y0)*clamp(t+.18,0,1),zz+span],color)}}
function addBossWorldGeometry(){for(const r of plasmaResidues){const a=clamp(r.life/r.maxLife,0,1),y=r.y+.03;for(let i=0;i<10;i++){const a0=i*TAU/10,a1=(i+1)*TAU/10,rr=r.r*(.78+.08*Math.sin(i*2.4));addLine([r.x+Math.cos(a0)*rr,y,r.z+Math.sin(a0)*rr],[r.x+Math.cos(a1)*rr,y,r.z+Math.sin(a1)*rr],a>.35?'#9b5cff':'#633887')}}for(const w of game.bossWarnings||[]){const y=topAtPoint(w.x,w.z)+.08,pulse=.82+.18*Math.sin(performance.now()*.012),r=w.r*pulse,segments=w.type==='charge'?2:16;if(w.type==='charge'){const boss=survivorAliveAliens().find(a=>a.bossType==='rammer'&&(a.bossState==='windup'||a.bossState==='charge'));if(boss?.chargeDir){const sx=boss.x-boss.chargeDir.x*3,sz=boss.z-boss.chargeDir.z*3,ex=boss.x+boss.chargeDir.x*24,ez=boss.z+boss.chargeDir.z*24;addLine([sx,topAtPoint(sx,sz)+.12,sz],[ex,topAtPoint(ex,ez)+.12,ez],w.color);addLine([sx-boss.chargeDir.z*.7,y,sz+boss.chargeDir.x*.7],[ex-boss.chargeDir.z*.7,y,ez+boss.chargeDir.x*.7],w.color)}}else for(let i=0;i<segments;i++){const a=i*TAU/segments,b=(i+1)*TAU/segments;addLine([w.x+Math.cos(a)*r,y,w.z+Math.sin(a)*r],[w.x+Math.cos(b)*r,y,w.z+Math.sin(b)*r],w.color)}if(w.type==='crack'){addLine([w.x-r,y,w.z],[w.x+r,y,w.z],w.color);addLine([w.x,y,w.z-r],[w.x,y,w.z+r],w.color)}}for(const z of game.acidZones||[]){const y=z.y+.10;for(let i=0;i<12;i++){const a=i*TAU/12,b=(i+1)*TAU/12,rr=z.r*(.82+.12*Math.sin(i*2.7));addLine([z.x+Math.cos(a)*rr,y,z.z+Math.sin(a)*rr],[z.x+Math.cos(b)*rr,y,z.z+Math.sin(b)*rr],'#b8ff38')}}}
function rebuildCachedWorldGeometry(now=performance.now()){
  if(!worldGeometryDirty&&worldCache.valid&&now-worldCache.lastBuild<900)return;
  if(worldGeometryDirty&&worldCache.valid&&now-worldCache.lastBuild<70)return;
  const textured=!!survivalMapRuntime?.canRenderSurfaces;solidTriangles=[];lineSegments=[];
  for(const p of platforms)if(p.active!==false){if(!p.survivalEnvironment&&!textured)addBoxGeometry({x:p.x,z:p.z,w:p.w,d:p.d,y:p.y+p.h/2,h:p.h},structureColor(p));addStructureSupportGlow(p);addStructureDamage(p)}
  if(game.mode!=='survivor'&&!textured)addFloorTiles();
  for(const w of walls)if(w.active!==false){if(!w.survivalEnvironment&&!textured)addBoxGeometry(w,structureColor(w));addStructureSupportGlow(w);addStructureDamage(w)}
  worldCache.solidCount=uploadWorldBuffer(cachedSolidBuffer,solidTriangles,gl.STATIC_DRAW);worldCache.lineCount=uploadWorldBuffer(cachedLineBuffer,lineSegments,gl.STATIC_DRAW);worldCache.lastBuild=now;worldCache.valid=true;worldGeometryDirty=activeStructuresCache.some(p=>(p.flash||0)>0||(p.fortifySealTimer||0)>0||(p.thunderGuardTimer||0)>0||(p.bonusHp||0)>0);
}
function renderDepthWorld(){rebuildCachedWorldGeometry();solidTriangles=[];lineSegments=[];modelWorldActors=[];modelHeldWeapons=[];addBuildGridGuide();addBossWorldGeometry();addWarGeometry();for(const b of bots)if(!b.dead&&b!==viewEnt)addBotGeometryByDistance(b);if(viewEnt!==player&&!player.dead)addBotGeometryByDistance(player);gl.enable(gl.DEPTH_TEST);gl.depthFunc(gl.LEQUAL);gl.disable(gl.BLEND);gl.viewport(viewRect.x*DPR,(H-viewRect.y-viewRect.h)*DPR,viewRect.w*DPR,viewRect.h*DPR);gl.enable(gl.SCISSOR_TEST);gl.scissor(viewRect.x*DPR,(H-viewRect.y-viewRect.h)*DPR,viewRect.w*DPR,viewRect.h*DPR);gl.clearColor(mapTheme.sky[0],mapTheme.sky[1],mapTheme.sky[2],1);gl.clear(gl.COLOR_BUFFER_BIT|gl.DEPTH_BUFFER_BIT);gl.disable(gl.SCISSOR_TEST);survivalMapRuntime?.render(gl,importedModelCamera(),{time:game.mode==='survivor'?game.survivalElapsed||0:performance.now()/1000,wave:game.survivorWave||1,quality:visualQuality,fog:mapTheme.sky});survivalMapRuntime?.renderSurfaces?.(gl,importedModelCamera(),[...platforms,...walls].filter(p=>!p.survivalEnvironment&&p.active!==false),{time:performance.now()/1000,quality:visualQuality,fog:mapTheme.sky,environment:game.mode==='survivor'});gl.useProgram(worldProgram);setWorldCamera();drawWorldBuffer(cachedSolidBuffer,worldCache.solidCount,gl.TRIANGLES);uploadAndDraw(solidTriangles,gl.TRIANGLES);gl.enable(gl.POLYGON_OFFSET_FILL);gl.polygonOffset(-1,-1);gl.lineWidth(1);drawWorldBuffer(cachedLineBuffer,worldCache.lineCount,gl.LINES);uploadAndDraw(lineSegments,gl.LINES);gl.disable(gl.POLYGON_OFFSET_FILL);renderImportedModels();renderExplosionEffects()}
function splitRects(){if(!(game.secondPlayer&&player2))return [{ent:player,x:0,y:0,w:W,h:H,label:'P1'}];const p1Label=game.p1Input==='controller'?'CONTROLLER':(game.p1Input==='touch'?'TOUCH':'KEYBOARD'),l1='P1 '+p1Label,l2='P2 '+(game.p2Input==='controller'?'CONTROLLER':'KEYBOARD');if(W>=H)return [{ent:player,x:0,y:0,w:Math.floor(W/2),h:H,label:l1},{ent:player2,x:Math.floor(W/2),y:0,w:W-Math.floor(W/2),h:H,label:l2}];const hh=Math.floor(H/2);return [{ent:player,x:0,y:0,w:W,h:hh,label:l1},{ent:player2,x:0,y:hh,w:W,h:H-hh,label:l2}]}
function drawMortarCircle(x,y,z,radius,alpha=1,flash=0){ctx.save();ctx.globalCompositeOperation='lighter';ctx.lineWidth=2.6+flash*2;ctx.strokeStyle=flash?'#ffffff':'#ff3333';ctx.shadowBlur=18;ctx.shadowColor='#ff3333';ctx.globalAlpha=alpha;ctx.beginPath();let started=false;for(let i=0;i<=40;i++){const a=i*TAU/40,q=project(x+Math.cos(a)*radius,y+.035,z+Math.sin(a)*radius);if(!q)continue;if(!started){ctx.moveTo(q.x,q.y);started=true}else ctx.lineTo(q.x,q.y)}if(started){ctx.closePath();ctx.stroke()}for(let i=0;i<4;i++){const a=i*TAU/4,q1=project(x+Math.cos(a)*radius*.72,y+.04,z+Math.sin(a)*radius*.72),q2=project(x+Math.cos(a)*radius,y+.04,z+Math.sin(a)*radius);if(q1&&q2){ctx.beginPath();ctx.moveTo(q1.x,q1.y);ctx.lineTo(q2.x,q2.y);ctx.stroke()}}ctx.restore()}
function drawMortarTargeting(){const now=performance.now()*.008;for(const ent of [player,...bots]){const a=ent?.mortarActive;if(!a)continue;const target=mortarTargetPoint(a),pulse=.72+.18*Math.sin(now),radius=a.radius*(.96+.04*Math.sin(now));drawMortarCircle(target.x,target.y,target.z,radius,pulse);const q1=project(ent.x,ent.y-.1,ent.z),q2=project(ent.x,a.rocketY,ent.z);if(q1&&q2){ctx.save();ctx.globalCompositeOperation='lighter';ctx.strokeStyle='#ff4b4b';ctx.lineWidth=4;ctx.shadowBlur=20;ctx.shadowColor='#ff3333';ctx.globalAlpha=.78;ctx.beginPath();ctx.moveTo(q1.x,q1.y);ctx.lineTo(q2.x,q2.y);ctx.stroke();ctx.restore()}}for(const p of projectiles)if(p.kind==='mortarDrop'&&!p.exploded){const warning=clamp(1-(p.y-p.impactY)/32,0,1),groundY=topAtPoint(p.x,p.z)+.08;drawMortarCircle(p.x,groundY,p.z,p.radius,.72+warning*.25,warning>.72?warning:0);const top=project(p.x,p.y+7,p.z),bottom=project(p.x,Math.max(p.impactY,p.y-8),p.z);if(top&&bottom){ctx.save();ctx.globalCompositeOperation='lighter';ctx.strokeStyle=warning>.72?'#ffffff':'#ff3333';ctx.lineWidth=3+warning*4;ctx.shadowBlur=24;ctx.shadowColor='#ff3333';ctx.globalAlpha=.85;ctx.beginPath();ctx.moveTo(top.x,top.y);ctx.lineTo(bottom.x,bottom.y);ctx.stroke();ctx.restore()}}}
function drawView(rect){viewEnt=rect.ent;viewRect={x:rect.x,y:rect.y,w:rect.w,h:rect.h};renderDepthWorld();}
function draw(){ctx.clearRect(0,0,W,H);gl.viewport(0,0,worldCanvas.width,worldCanvas.height);gl.clearColor(mapTheme.sky[0],mapTheme.sky[1],mapTheme.sky[2],1);gl.clear(gl.COLOR_BUFFER_BIT|gl.DEPTH_BUFFER_BIT);const rects=splitRects();for(const r of rects)drawView(r);ctx.drawImage(worldCanvas,0,0,W,H);for(const r of rects){viewEnt=r.ent;viewRect={x:r.x,y:r.y,w:r.w,h:r.h};beginVisualView();ctx.save();ctx.beginPath();ctx.rect(r.x,r.y,r.w,r.h);ctx.clip();const bs=humanBuildState(r.ent);if(bs.held&&bs.preview){drawBuildIntentZones(bs.preview);drawBuildPreview(bs.preview);}drawBuildFeedbackFor(r.ent);drawStructureHealthFor(r.ent);drawMortarTargeting();
  for(const p of projectiles)drawPlasmaProjectile3D(p);drawDecals();drawUltimateFx();drawImpacts();drawPickups();for(const s of shots)if(!s.tier3Visual||takeTier3Lines())drawBeamTrail(s,false);for(const l of lasers)drawBeamTrail(l,true);drawMuzzleFlashes(false);drawUltimateAurasForView(r.ent);drawTier3Signatures(r.ent);drawThermalScopeHighlights(r.ent);for(const b of bots)if(!b.dead&&b!==r.ent)drawBotLabelIfVisible(b);if(r.ent!==player&&!player.dead)drawBotLabelIfVisible({...player,name:'PLAYER 1',color:'#ffffff'});
  for(let pi=0;pi<particles.length;pi++){const p=particles[pi],detail=visualDetailAt(p.x,p.y,p.z);if(detail<0||(visualQuality==='low'&&detail===0&&pi%3))continue;if(!visualVisible(p.x,p.y,p.z,r.ent))continue;const q=project(p.x,p.y,p.z);if(pointInView(q,24)){const a=clamp(p.life/(p.maxLife||.7),0,1),sz=(p.size||4)*(p.kind==='blood'?.72+q.s*.012:1);ctx.globalAlpha=a;ctx.strokeStyle=ctx.fillStyle=p.c;ctx.shadowBlur=p.kind==='blood'?0:12*visualPreset().glow;ctx.shadowColor=p.c;if(p.kind==='smoke'||p.kind==='dust'){ctx.globalAlpha=a*(p.kind==='smoke'?.12:.30);ctx.shadowBlur=0;ctx.beginPath();ctx.arc(q.x,q.y,sz*(1.7-a),0,TAU);ctx.fill()}else if(p.kind==='ring'){ctx.lineWidth=detail>0?2.5:1;ctx.beginPath();ctx.arc(q.x,q.y,sz*(1.35-a),0,TAU);ctx.stroke()}else ctx.fillRect(q.x-sz/2,q.y-sz/2,detail>0?sz:Math.min(2,sz),detail>0?sz:Math.min(2,sz));ctx.globalAlpha=1;ctx.shadowBlur=0}}
  drawMuzzleFlashes(true);drawPlayerCrosshairFor(r.ent);drawCombatFeedback(r.ent);ctx.restore();if(rects.length>1){ctx.strokeStyle='rgba(255,255,255,.30)';ctx.lineWidth=1;ctx.strokeRect(r.x+.5,r.y+.5,r.w-1,r.h-1)}drawPlayerMiniHud(r.ent,r.label);drawWarHud(r.ent)}}
function playerCrosshairColor(ent){
  return ent===player2?'#ff9d2e':'#00eaff';
}
function drawPlayerCrosshairFor(ent){
  const mobileLocked=ent===player&&mobileInput.visible&&!!mobileInput.lockTarget&&!mobileInput.lockTarget.dead,cx=viewRect.x+viewRect.w/2,cy=viewRect.y+viewRect.h/2,baseColor=mobileLocked?'#fff44f':(ent.weapon==='mender'?menderAimInfo(ent).color:playerCrosshairColor(ent)),readyGlow=ent.weapon!=='mender'&&(getUltimateReady(ent)||ent.ultClipShots>0),color=readyGlow?ultimateColor(ent.ultClipWeapon||ent.weapon):baseColor,enemyColor=ent===player2?'#ffcf86':'#bdfcff';
  const wp=WEAPONS[ent.weapon]||WEAPONS.pistol;
  const reloadPulse=ent.reload>0?Math.sin(performance.now()/95)*2:0;
  const spread=clamp((wp.spread||.015)*330,4,18)+(ent.cool>0?Math.min(8,ent.cool*22):0)+(ent.reload>0?7:0);
  ctx.save();
  if(readyGlow){ctx.globalAlpha=.12;ctx.fillStyle=color;ctx.fillRect(viewRect.x,viewRect.y,viewRect.w,viewRect.h);ctx.globalAlpha=1;}
  ctx.globalCompositeOperation='lighter';
  ctx.lineCap='round';
  ctx.lineJoin='round';
  ctx.shadowBlur=14;
  ctx.shadowColor=color;
  ctx.strokeStyle=color;
  ctx.fillStyle=color;
  ctx.lineWidth=2.2;
  const gap=8+spread*.35+reloadPulse,len=10;
  ctx.beginPath();
  ctx.moveTo(cx-gap-len,cy);ctx.lineTo(cx-gap,cy);
  ctx.moveTo(cx+gap,cy);ctx.lineTo(cx+gap+len,cy);
  ctx.moveTo(cx,cy-gap-len);ctx.lineTo(cx,cy-gap);
  ctx.moveTo(cx,cy+gap);ctx.lineTo(cx,cy+gap+len);
  ctx.stroke();
  ctx.shadowBlur=7;
  ctx.beginPath();ctx.arc(cx,cy,2.2,0,TAU);ctx.fill();
  ctx.globalCompositeOperation='source-over';
  ctx.shadowBlur=0;
  ctx.strokeStyle='rgba(255,255,255,.82)';ctx.lineWidth=1;
  ctx.beginPath();ctx.arc(cx,cy,spread+12,0,TAU);ctx.stroke();
  ctx.fillStyle=enemyColor;ctx.font='900 10px Arial';ctx.textAlign='center';
  if(game.secondPlayer&&!mobileInput.visible)ctx.fillText('',cx,cy+spread+29);
  if(ent.reload>0){
    const pct=clamp(1-ent.reload/(ent.reloadTotal||wp.reload||1),0,1),rw=54,rh=5,rx=cx-rw/2,ry=cy+spread+35;
    ctx.fillStyle='rgba(5,10,24,.72)';ctx.fillRect(rx-2,ry-2,rw+4,rh+4);
    ctx.fillStyle='rgba(255,255,255,.16)';ctx.fillRect(rx,ry,rw,rh);
    ctx.fillStyle=color;ctx.fillRect(rx,ry,rw*pct,rh);
    ctx.strokeStyle='rgba(255,255,255,.5)';ctx.strokeRect(rx,ry,rw,rh);
  }
  ctx.restore();
}
function drawBuildModeIcon(x,y,type,scale=1){
  const isPlatform=type==='platform',s=scale;
  ctx.save();
  ctx.translate(x,y);
  ctx.lineWidth=Math.max(1,1.4*s);
  ctx.strokeStyle=isPlatform?'#ff9ee5':'#9ffaff';
  ctx.fillStyle=isPlatform?'rgba(255,43,189,.30)':'rgba(0,234,255,.30)';
  ctx.shadowBlur=8*s;
  ctx.shadowColor=isPlatform?'#ff2bbd':'#00eaff';
  if(isPlatform){
    ctx.beginPath();
    ctx.moveTo(0,3*s);ctx.lineTo(14*s,3*s);ctx.lineTo(18*s,7*s);ctx.lineTo(4*s,7*s);
    ctx.closePath();ctx.fill();ctx.stroke();
    ctx.shadowBlur=0;
    ctx.beginPath();
    ctx.moveTo(4*s,7*s);ctx.lineTo(4*s,12*s);
    ctx.moveTo(14*s,7*s);ctx.lineTo(14*s,12*s);
    ctx.stroke();
  }else{
    ctx.beginPath();
    ctx.rect(2*s,0,13*s,13*s);
    ctx.fill();ctx.stroke();
    ctx.shadowBlur=0;
    ctx.beginPath();
    ctx.moveTo(2*s,4.3*s);ctx.lineTo(15*s,4.3*s);
    ctx.moveTo(2*s,8.7*s);ctx.lineTo(15*s,8.7*s);
    ctx.moveTo(6.3*s,0);ctx.lineTo(6.3*s,13*s);
    ctx.moveTo(10.7*s,0);ctx.lineTo(10.7*s,13*s);
    ctx.stroke();
  }
  ctx.restore();
}
function drawWeaponSlotText(x,y,ent,w){
  ensureWeaponInventory(ent);
  const active=ent.activeSlot||0, names=[compactWeaponName(ent.weaponSlots[0]),compactWeaponName(ent.weaponSlots[1])], widths=[ctx.measureText(names[0]).width,ctx.measureText(names[1]).width], slashW=ctx.measureText('/').width,total=widths[0]+slashW+widths[1]+10;
  let sx=x;
  if(total>w)sx=x;
  for(let i=0;i<2;i++){
    const tx=sx;
    if(i===active&&ent.weaponSlots[i]){ctx.fillStyle='rgba(0,234,255,.30)';ctx.strokeStyle='rgba(0,234,255,.55)';ctx.beginPath();ctx.roundRect(tx-3,y-10,widths[i]+6,13,5);ctx.fill();ctx.stroke();ctx.fillStyle='#fff';}
    else{ctx.fillStyle=ent.weaponSlots[i]?'#c9fbff':'rgba(210,220,230,.62)';}
    ctx.fillText(names[i],tx,y);
    sx+=widths[i];
    if(i===0){ctx.fillStyle='rgba(255,255,255,.55)';ctx.fillText('/',sx+4,y);sx+=slashW+10;}
  }
}
function playerHudLayout(ent){
  const compact=game.p1Input==='touch'||viewRect.w<500,w=Math.min(compact?178:198,viewRect.w-20),x=viewRect.x+10,y=viewRect.y+10,survival=game.mode==='survivor';
  let match;
  if(survival)match='W'+(game.survivorWave||0)+' · '+Math.max(0,(game.waveTotal||0)-(game.waveKilled||0))+' LEFT';
  else if(game.mode==='siege')match='R'+(game.siegeRound||1)+' · '+(siegeBuildActive()?Math.ceil(game.siegeBuildTime||0)+'s':'SIEGE');
  else if(game.mode==='frag')match='FRAG · '+fragKillLimit();
  else if(game.mode==='conquest')match='CONQUEST · '+factionName(ent.team);
  else if(game.mode==='campaign')match='CAMPAIGN · MISSION '+(NeonCampaign.mission(campaignRun?.missionId)?.chapter||1);
  else{const time=Math.max(0,game.timeLeft|0);match=(game.mode==='tdm'?'TEAM':'DM')+' · '+Math.floor(time/60)+':'+String(time%60).padStart(2,'0')}
  if(game.secondPlayer)match=(ent===player2?'P2':'P1')+' · '+match;
  return {x,y,w,h:survival?94:74,match,survival};
}
function drawPlayerMiniHud(ent,label){drawCompactTouchHud(ent,label)}
function drawCompactTouchHud(ent,label){
  if(!game.started||game.over)return;
  const box=playerHudLayout(ent),{x,y,w,h}=box,wp=WEAPONS[ent.weapon]||WEAPONS.pistol,bs=humanBuildState(ent),hp=clamp(ent.hp/(ent.maxHp||100),0,1),armor=clamp((ent.armor||0)/(ent.maxArmor||100),0,1),energy=['plasma','laser','mender'].includes(ent.weapon),ammo=ent.dead?'—':ent.reload>0?'↻':energy?Math.round((ent.heat||0)*100)+'%':ent.ammo+'/'+wp.mag,points=ent.upgradePoints||0,ultimate=getUltimateReady(ent)||ent.ultClipShots>0?'ULT READY':'ULT '+Math.round(getUltimateCharge(ent)*100)+'%';
  ctx.save();ctx.shadowBlur=0;ctx.fillStyle='rgba(4,12,22,.64)';ctx.beginPath();ctx.roundRect(x,y,w,h,6);ctx.fill();ctx.textAlign='left';ctx.font='600 9px Arial';ctx.fillStyle='#b8cbd7';ctx.fillText(box.match,x+8,y+14,w-56);ctx.textAlign='right';ctx.fillStyle='#ecfaff';ctx.fillText((ent.score||0)+' KO',x+w-8,y+14,44);
  ctx.font='700 11px Arial';ctx.textAlign='left';ctx.fillStyle='#eaf8ff';ctx.fillText(ent.dead?'RESPAWNING':compactWeaponName(ent.weapon),x+8,y+29,w-62);ctx.textAlign='right';ctx.fillStyle=ent.reload>0||ent.menderOverheated?'#ffe18c':'#ffffff';ctx.fillText(ammo,x+w-8,y+29,48);
  ctx.font='600 9px Arial';ctx.textAlign='left';ctx.fillStyle='#9ae7c5';ctx.fillText('HP '+Math.max(0,Math.round(ent.hp)),x+8,y+43,w/2-12);ctx.textAlign='right';ctx.fillStyle='#98d7ed';ctx.fillText('AR '+Math.max(0,Math.round(ent.armor||0)),x+w-8,y+43,w/2-12);
  const bar=(w-20)/2;ctx.fillStyle='rgba(255,255,255,.13)';ctx.fillRect(x+8,y+48,bar,3);ctx.fillRect(x+12+bar,y+48,bar,3);ctx.fillStyle='#70dbb3';ctx.fillRect(x+8,y+48,bar*hp,3);ctx.fillStyle='#79cbe8';ctx.fillRect(x+12+bar,y+48,bar*armor,3);
  ctx.textAlign='left';ctx.font='600 9px Arial';ctx.fillStyle='#b5cdd8';ctx.fillText('▧ '+Math.floor(bs.charges)+'  ✦ '+points+(box.survival?'  S '+Math.floor(game.scrap||0):''),x+8,y+64,w-86);ctx.textAlign='right';ctx.fillStyle=getUltimateReady(ent)?'#ffe18c':'#cbb2de';ctx.fillText(ultimate,x+w-8,y+64,75);ctx.fillStyle=getUltimateReady(ent)?'#ffe18c':'#a686bd';ctx.fillRect(x+8,y+70,(w-16)*clamp(getUltimateCharge(ent),0,1),1);
  if(box.survival){const state=survivalEquipment?.state(),item=window.NeonSurvivalProgress?.equipment.find(e=>e.id===state?.selected),count=state?.charges?.[item?.id]||0,cool=Math.ceil(state?.cooldowns?.[item?.id]||0);ctx.textAlign='left';ctx.fillStyle='#b8cbd7';ctx.fillText((item?.name||'EQUIPMENT')+' ×'+count+(cool?' · '+cool+'s':''),x+8,y+84,w-70)}
  ctx.restore();
}

function drawBuildFeedbackFor(ent){
  const failed=ent?.buildFailure;if(!failed||failed.until<performance.now()||humanBuildState(ent).held)return;
  ctx.save();ctx.globalAlpha=Math.min(1,(failed.until-performance.now())/240);ctx.fillStyle='#e5a5af';ctx.strokeStyle='rgba(4,8,20,.85)';ctx.lineWidth=3;ctx.font='600 10px system-ui';ctx.textAlign='center';const x=viewRect.x+viewRect.w/2,y=viewRect.y+viewRect.h/2+28;ctx.strokeText(failed.reason,x,y);ctx.fillText(failed.reason,x,y);ctx.restore();
}
function drawStructureHealthFor(ent){if(!humanBuildState(ent).held&&ent.weapon!=='mender')return;const old=viewEnt;viewEnt=ent;const hit=rayWorld(actorEyePosition(ent),dirFrom(ent,0),BUILD_RANGE,true),s=hit?.block?.structure;if(!s||!s.destructible||!Number.isFinite(s.hp)){viewEnt=old;return}const ratio=clamp(s.hp/s.maxHp,0,1),w=Math.min(150,viewRect.w*.55),x=viewRect.x+viewRect.w/2-w/2,y=viewRect.y+viewRect.h/2+55;ctx.save();ctx.fillStyle='rgba(5,10,24,.75)';ctx.fillRect(x-4,y-19,w+8,30);ctx.fillStyle=ratio<.25?'#ff315d':ratio<.5?'#ffb02e':'#65f7ff';ctx.fillRect(x,y,w*ratio,7);ctx.strokeStyle='rgba(255,255,255,.7)';ctx.strokeRect(x,y,w,7);ctx.fillStyle='#fff';ctx.font='bold 12px system-ui';ctx.textAlign='center';ctx.fillText(s.type.toUpperCase()+' '+Math.ceil(s.hp)+' / '+s.maxHp+((s.bonusHp||0)>0?' + '+Math.ceil(s.bonusHp)+' BOOST':''),viewRect.x+viewRect.w/2,y-5);ctx.restore();viewEnt=old}

function drawPlasmaProjectile3D(p){if(p.kind==='punch'||p.kind==='harpoon')return;
  const viewer=viewEnt||player;if(p.kind!=='mortarDrop'&&!lineClear(actorEyePosition(viewer).x,actorEyePosition(viewer).y-.05,actorEyePosition(viewer).z,p.x,p.y,p.z))return;
  const detail=visualDetailAt(p.x,p.y,p.z),q=project(p.x,p.y,p.z);if(detail<0||!pointInView(q,30))return;
  const speed=Math.hypot(p.vx,p.vy,p.vz)||1,dx=p.vx/speed,dy=p.vy/speed,dz=p.vz/speed,tier=p.tier||1,r=p.r||.34,tailLen=1.15+tier*.38;
  const tail=project(p.x-dx*tailLen,p.y-dy*tailLen,p.z-dz*tailLen),mid=project(p.x-dx*tailLen*.45,p.y-dy*tailLen*.45,p.z-dz*tailLen*.45);
  const sx=project(p.x+r,p.y,p.z),sy=project(p.x,p.y+r,p.z),sz=project(p.x,p.y,p.z+r),radius=Math.max(5.5,Math.max(sx?Math.hypot(sx.x-q.x,sx.y-q.y):0,sy?Math.hypot(sy.x-q.x,sy.y-q.y):0,sz?Math.hypot(sz.x-q.x,sz.y-q.y):0,q.s*r*.72));
  ctx.save();ctx.globalCompositeOperation='lighter';ctx.lineCap='round';
  if(tail){const g=ctx.createLinearGradient(tail.x,tail.y,q.x,q.y);g.addColorStop(0,'rgba(255,255,255,0)');g.addColorStop(.42,p.color);g.addColorStop(1,'#ffffff');ctx.globalAlpha=.28+.07*tier;ctx.strokeStyle=p.color;ctx.lineWidth=radius*(1.18+.08*tier);ctx.shadowBlur=radius*1.9*visualPreset().glow;ctx.shadowColor=p.color;ctx.beginPath();ctx.moveTo(tail.x,tail.y);if(mid)ctx.quadraticCurveTo(mid.x,mid.y,q.x,q.y);else ctx.lineTo(q.x,q.y);ctx.stroke();ctx.globalAlpha=.88;ctx.strokeStyle=g;ctx.lineWidth=Math.max(2,radius*.34);ctx.beginPath();ctx.moveTo(tail.x,tail.y);ctx.lineTo(q.x,q.y);ctx.stroke()}
  const glow=ctx.createRadialGradient(q.x-radius*.22,q.y-radius*.25,radius*.08,q.x,q.y,radius*1.35);glow.addColorStop(0,'#ffffff');glow.addColorStop(.18,'#ffffff');glow.addColorStop(.46,p.color);glow.addColorStop(1,'rgba(255,255,255,0)');ctx.globalAlpha=.96;ctx.fillStyle=glow;ctx.shadowBlur=radius*2.2*visualPreset().glow;ctx.shadowColor=p.color;ctx.beginPath();ctx.arc(q.x,q.y,radius*1.35,0,TAU);ctx.fill();
  for(const slice of [-.42,0,.42]){const c=project(p.x+dx*r*slice,p.y+dy*r*slice,p.z+dz*r*slice);if(!c)continue;const sliceRadius=radius*Math.sqrt(1-slice*slice)*(.76+.08*tier);ctx.globalAlpha=slice===0?.72:.34;ctx.strokeStyle=slice===0?'#ffffff':p.color;ctx.lineWidth=slice===0?Math.max(1.5,radius*.12):Math.max(1,radius*.08);ctx.beginPath();ctx.ellipse(c.x,c.y,sliceRadius,sliceRadius*.82,Math.atan2(q.y-(tail?.y??q.y),q.x-(tail?.x??q.x)),0,TAU);ctx.stroke()}
  ctx.globalAlpha=1;ctx.fillStyle='#ffffff';ctx.shadowBlur=radius*.9;ctx.beginPath();ctx.ellipse(q.x-radius*.24,q.y-radius*.28,radius*.28,radius*.20,-.5,0,TAU);ctx.fill();
  if(p.remote){const blink=Math.sin(performance.now()*.018)>-.15;ctx.shadowBlur=0;ctx.fillStyle=blink?'#ffffff':'#ff315d';ctx.globalAlpha=blink?1:.45;ctx.beginPath();ctx.arc(q.x,q.y-radius*.72,Math.max(2.5,radius*.18),0,TAU);ctx.fill()}
  if(p.plasmaMine&&p.stuck&&takeMinePulse(p.owner)){const pulse=.82+Math.sin(performance.now()*.012)*.18;ctx.shadowBlur=0;ctx.globalAlpha=.72;ctx.strokeStyle='#e7b3ff';ctx.lineWidth=1.6;ctx.beginPath();ctx.arc(q.x,q.y,radius*(1.55+pulse*.32),0,TAU);ctx.stroke();ctx.globalAlpha=.26;ctx.beginPath();ctx.arc(q.x,q.y,radius*(2.05+pulse*.48),0,TAU);ctx.stroke()}
  if(p.eventHorizon&&!p.stuck&&takeTier3Lines()){ctx.shadowBlur=0;ctx.globalAlpha=.50*visualPreset().glow;ctx.strokeStyle='#d67cff';ctx.lineWidth=1.4;ctx.beginPath();const swirlSteps=visualQuality==='high'?12:(visualQuality==='medium'?8:5);for(let i=0;i<swirlSteps;i++){const t=i/(swirlSteps-1),ang=t*TAU*1.35+performance.now()*.004,rr=radius*(.45+t*1.4),xx=q.x+Math.cos(ang)*rr,yy=q.y+Math.sin(ang)*rr*.55;if(i)ctx.lineTo(xx,yy);else ctx.moveTo(xx,yy)}ctx.stroke()}
  ctx.restore();
}


function drawUltimateFx(){
  for(const fx of ultimateFx){
    const detail=visualDetailAt(fx.x,fx.y,fx.z);if(detail<0||(fx.tier3&&visualQuality==='low'&&detail===0))continue;if(!visualVisible(fx.x,fx.y,fx.z))continue;
    const q=project(fx.x,fx.y,fx.z);if(!q)continue;
    const a=clamp(fx.life/(fx.maxLife||fx.life||.1),0,1),c=fx.color||ultimateColor(fx.weapon),sz=(fx.size||4)*(1.0-a*.25)*(q.s*.04+1);
    ctx.save();ctx.globalCompositeOperation='lighter';ctx.globalAlpha=Math.min(.55,.55*a);ctx.strokeStyle=c;ctx.fillStyle=c;ctx.shadowColor=c;ctx.shadowBlur=8*visualPreset().glow;ctx.lineCap='round';ctx.lineJoin='round';
    if(fx.type==='ring'||fx.type==='trailRing'||fx.type==='supernovaCore'){ctx.lineWidth=1.5;ctx.beginPath();ctx.arc(q.x,q.y,sz*(1.0+(1-a)*1.2),0,TAU);ctx.stroke();}
    else if(fx.type==='muzzle'){ctx.translate(q.x,q.y);const e=project(fx.x+fx.dx,fx.y+fx.dy,fx.z+fx.dz),ang=e?Math.atan2(e.y-q.y,e.x-q.x):0;ctx.rotate(ang);ctx.fillStyle='#ffffff';ctx.beginPath();ctx.moveTo(sz*1.55,0);ctx.lineTo(-sz*.34,-sz*.48);ctx.lineTo(-sz*.12,0);ctx.lineTo(-sz*.34,sz*.48);ctx.closePath();ctx.fill();}
    else if(fx.type==='clipLoad'){ctx.translate(q.x,q.y);const e=project(fx.x+fx.dx,fx.y+fx.dy,fx.z+fx.dz),ang=e?Math.atan2(e.y-q.y,e.x-q.x):0;ctx.rotate(ang);ctx.lineWidth=1.5;ctx.strokeStyle=c;ctx.globalAlpha=.45*a;ctx.strokeRect(-sz*.85,-sz*.35,sz*1.7,sz*.7);ctx.fillStyle='#ffffff';ctx.globalAlpha=.55*a;ctx.fillRect(-sz*.35,-sz*.10,sz*.7,sz*.20);}
    else if(fx.type==='deadeye'){ctx.lineWidth=1.5;for(let i=0;i<4;i++){const ang=i*Math.PI/2+(1-a)*.45;ctx.beginPath();ctx.moveTo(q.x+Math.cos(ang)*sz*.75,q.y+Math.sin(ang)*sz*.75);ctx.lineTo(q.x+Math.cos(ang)*sz*1.55,q.y+Math.sin(ang)*sz*1.55);ctx.stroke()}}
    else if(fx.type==='reticle'){ctx.lineWidth=1.2;ctx.translate(q.x,q.y);ctx.strokeRect(-sz*.7,-sz*.7,sz*1.4,sz*1.4);}
    else if(fx.type==='railMark'){ctx.lineWidth=1.8;ctx.beginPath();ctx.arc(q.x,q.y,sz*(1+(1-a)*.55),0,TAU);ctx.stroke();}
    else if(fx.type==='storm'){ctx.lineWidth=1.1;for(let i=0;i<(visualQuality==='high'?3:1);i++){if(!takeTier3Lines())break;const ang=i*TAU/3+(1-a)*1.6,r1=sz*.35,r2=sz*1.2;ctx.beginPath();ctx.moveTo(q.x+Math.sin(ang)*r1,q.y+Math.cos(ang)*r1);ctx.lineTo(q.x+Math.sin(ang+.25)*r2,q.y+Math.cos(ang+.25)*r2);ctx.stroke();}}
    else if(fx.type==='spiral'){ctx.lineWidth=1.3;ctx.beginPath();for(let i=0;i<10;i++){const t=i/9,ang=t*TAU*1.25+(1-a)*2.2,r=sz*t*1.25,x=q.x+Math.cos(ang)*r,y=q.y+Math.sin(ang)*r;if(i)ctx.lineTo(x,y);else ctx.moveTo(x,y)}ctx.stroke();}
    else if(fx.type==='shockwave'||fx.type==='warhead'||fx.type==='supernova'||fx.type==='ionBurst'){ctx.lineWidth=2;ctx.beginPath();ctx.arc(q.x,q.y,sz*(1.0+(1-a)*1.4),0,TAU);ctx.stroke();}
    else if(fx.type==='cone'||fx.type==='spray'||fx.type==='screenSweep'){if(!takeTier3Lines()){}else{const e=project(fx.x+fx.dx*(fx.len||12),fx.y+fx.dy*(fx.len||12),fx.z+fx.dz*(fx.len||12));if(e){ctx.lineWidth=1.2;const baseAng=Math.atan2(e.y-q.y,e.x-q.x),spread=fx.spread||.35;for(let i=-1;i<=1;i++){const ang=baseAng+i*spread/3,len=Math.hypot(e.x-q.x,e.y-q.y)*(1-Math.abs(i)*.12);ctx.beginPath();ctx.moveTo(q.x,q.y);ctx.lineTo(q.x+Math.cos(ang)*len,q.y+Math.sin(ang)*len);ctx.stroke()}}}}
    else if(fx.type==='railBeam'||fx.type==='beamAura'){if(!takeTier3Lines()){}else{const e=project(fx.x+fx.dx*fx.len,fx.y+fx.dy*fx.len,fx.z+fx.dz*fx.len);if(e){ctx.lineWidth=fx.type==='railBeam'?5:3;ctx.globalAlpha=.18*a;ctx.beginPath();ctx.moveTo(q.x,q.y);ctx.lineTo(e.x,e.y);ctx.stroke();ctx.lineWidth=1.2;ctx.globalAlpha=.55*a;ctx.strokeStyle='#ffffff';ctx.beginPath();ctx.moveTo(q.x,q.y);ctx.lineTo(e.x,e.y);ctx.stroke();}}}
    else if(fx.type==='dash'){if(!takeTier3Lines()){}else{const e=project(fx.x+fx.dx*fx.len,fx.y+fx.dy*fx.len,fx.z+fx.dz*fx.len);if(e){ctx.globalAlpha=.36*a;ctx.lineWidth=6;ctx.beginPath();ctx.moveTo(q.x,q.y);ctx.lineTo(e.x,e.y);ctx.stroke();ctx.globalAlpha=.72*a;ctx.lineWidth=1.5;ctx.strokeStyle='#ffffff';ctx.stroke();}}}
    else if(fx.type==='prism'){ctx.lineWidth=1.4;for(const side of (visualQuality==='low'?[1]:[-1,1])){if(!takeTier3Lines())break;const e=project(fx.x+fx.dx*4+side*1.2,fx.y+fx.dy*4,fx.z+fx.dz*4+side*1.2);if(e){ctx.beginPath();ctx.moveTo(q.x,q.y);ctx.lineTo(e.x,e.y);ctx.stroke();}}}
    else if(fx.type==='grapple'){const e=project(fx.tx,fx.ty,fx.tz);if(e){ctx.globalAlpha=.68*a;ctx.lineWidth=3;ctx.setLineDash([7,4]);ctx.beginPath();ctx.moveTo(q.x,q.y);ctx.lineTo(e.x,e.y);ctx.stroke();ctx.setLineDash([]);}}
    ctx.restore();
  }
}

function drawTier3Signatures(ent){
  const now=performance.now()*.001;
  // Continuous cues are computed from current state, so they create no persistent per-frame objects.
  if(ent===player&&!ent.dead){
    let color='',strength=0;
    if(ent.weapon==='assault'&&hasUpgrade('assault','momentumClip')&&(ent.assaultMomentum||0)>.32){color='#65f7ff';strength=ent.assaultMomentum||0}
    else if(ent.weapon==='smg'&&hasUpgrade('smg','slipstream')&&(ent.slipstreamTimer||0)>0){color='#baffff';strength=.72}
    else if(ent.weapon==='burst'&&hasUpgrade('burst','tacticalRollout')&&(ent.tacticalTimer||0)>0){color='#7cffd5';strength=.8}
    if(color){const speed=Math.hypot(ent.vx,ent.vz),inv=1/(speed||1),dx=speed>2?-ent.vx*inv:-Math.sin(ent.yaw),dz=speed>2?-ent.vz*inv:-Math.cos(ent.yaw);ctx.save();ctx.strokeStyle=color;ctx.lineCap='round';ctx.lineWidth=1.5;ctx.globalAlpha=.18+.24*strength;for(let i=0,streaks=visualPreset().streaks;i<streaks;i++){if(!takeTier3Lines())break;const lane=i-(streaks-1)/2,side=lane*.48,phase=((now*4+lane*.37)%1)*1.5,a=project(ent.x+Math.cos(ent.yaw)*side+dx*phase,ent.y-1.05+lane*.12,ent.z-Math.sin(ent.yaw)*side+dz*phase),b=project(ent.x+Math.cos(ent.yaw)*side+dx*(phase+1.6),ent.y-1.05+lane*.12,ent.z-Math.sin(ent.yaw)*side+dz*(phase+1.6));if(a&&b){ctx.beginPath();ctx.moveTo(a.x,a.y);ctx.lineTo(b.x,b.y);ctx.stroke()}}ctx.restore()}
  }
  const anchor=activeArcAnchor(player);if(anchor&&!anchor.dead&&takeTier3Lines(2)){const q=project(anchor.x,anchor.y-.72,anchor.z);if(q&&lineClear(ent.x,ent.y-.05,ent.z,anchor.x,anchor.y-.72,anchor.z)){const pulse=7+Math.sin(now*8)*1.5;ctx.save();ctx.strokeStyle='#baffff';ctx.globalAlpha=.78;ctx.lineWidth=1.8;ctx.setLineDash([5,4]);ctx.beginPath();ctx.arc(q.x,q.y,pulse+q.s*.15,0,TAU);ctx.stroke();ctx.setLineDash([]);ctx.beginPath();ctx.moveTo(q.x-pulse*.45,q.y);ctx.lineTo(q.x+pulse*.45,q.y);ctx.moveTo(q.x,q.y-pulse*.45);ctx.lineTo(q.x,q.y+pulse*.45);ctx.stroke();ctx.restore()}}
  for(const t of bots){if(t.dead||((t.armorCrackedTimer||0)<=0&&(t.phaseMarkedTimer||0)<=0)||!takeTier3Lines(2))continue;const q=project(t.x,t.y-(t.alienScale||1)*1.65,t.z);if(!q)continue;ctx.save();ctx.strokeStyle=(t.armorCrackedTimer||0)>0?'#ffb14f':'#dffcff';ctx.globalAlpha=.82;ctx.lineWidth=1.5;const r=7+Math.sin(now*9)*1.2;ctx.beginPath();ctx.arc(q.x,q.y,r,0,TAU);ctx.stroke();ctx.beginPath();ctx.moveTo(q.x-r*.8,q.y-r*.2);ctx.lineTo(q.x-r*.2,q.y-r*.75);ctx.lineTo(q.x+r*.25,q.y-r*.15);ctx.lineTo(q.x+r*.75,q.y-r*.65);ctx.stroke();ctx.restore()}
}

function drawDecals(){
  for(const d of decals){const detail=visualDetailAt(d.x,d.y,d.z);if(detail<0||(visualQuality==='low'&&detail===0))continue;if(!visualVisible(d.x,d.y+.03,d.z))continue;const a=clamp(d.life/d.maxLife,0,1),r=d.size,c=Math.cos(d.angle),sn=Math.sin(d.angle),pt=(x,z)=>[d.x+(x*c-z*sn)*r,d.y,d.z+(x*sn+z*c)*r];ctx.save();ctx.globalAlpha=.72*a;ctx.lineWidth=1;
    if(d.type==='rocket'||d.type==='heavy'){const ring=[];for(let i=0;i<10;i++){const ang=i*TAU/10,rr=.72+((i*7)%3)*.12;ring.push(pt(Math.cos(ang)*rr,Math.sin(ang)*rr));}drawPoly(ring,'rgba(24,13,10,.48)',d.color,1);for(let i=0;i<5;i++){const ang=i*TAU/5+.28;drawPoly([pt(Math.cos(ang)*.18,Math.sin(ang)*.18),pt(Math.cos(ang)*.98,Math.sin(ang)*.98),pt(Math.cos(ang+.12)*.55,Math.sin(ang+.12)*.55)],'rgba(0,0,0,0)',d.color,1);}}
    else if(d.type==='plasma'){const outer=[];for(let i=0;i<12;i++){const ang=i*TAU/12;outer.push(pt(Math.cos(ang),Math.sin(ang)));}drawPoly(outer,'rgba(78,18,92,.28)',d.color,1.5);}
    else if(d.type==='arc'){for(let i=0;i<3;i++){const ang=d.angle+i*TAU/3;drawPoly([pt(0,0),pt(Math.cos(ang)*.45,Math.sin(ang)*.45),pt(Math.cos(ang+.28),Math.sin(ang+.28))],'rgba(0,0,0,0)',d.color,1.2);}}
    else if(d.type==='laser'){drawPoly([pt(-1,-.10),pt(1,-.04),pt(1,.08),pt(-1,.05)],d.color,'rgba(20,5,18,.8)',1);}
    else if(d.type==='punch'){for(let i=0;i<4;i++){const ang=i*TAU/4+.35;drawPoly([pt(0,0),pt(Math.cos(ang)*.52,Math.sin(ang)*.52),pt(Math.cos(ang+.15),Math.sin(ang+.15))],'rgba(0,0,0,0)',d.color,1.2);}}
    else{const pts=[];for(let i=0;i<8;i++){const ang=i*TAU/8,rr=i%2?1:.72;pts.push(pt(Math.cos(ang)*rr,Math.sin(ang)*rr));}drawPoly(pts,d.color,'rgba(5,9,12,.65)',.8);}ctx.restore();
  }
}

function drawImpacts(){for(const m of impacts){if(visualDetailAt(m.x,m.y,m.z)<0)continue;if(!visualVisible(m.x+m.nx*.04,m.y+m.ny*.04,m.z+m.nz*.04))continue;const n={x:m.nx,y:m.ny,z:m.nz},up=Math.abs(n.y)>.8?{x:1,y:0,z:0}:{x:0,y:1,z:0},ux=n.y*up.z-n.z*up.y,uy=n.z*up.x-n.x*up.z,uz=n.x*up.y-n.y*up.x,ul=Math.hypot(ux,uy,uz)||1,u={x:ux/ul,y:uy/ul,z:uz/ul},v={x:n.y*u.z-n.z*u.y,y:n.z*u.x-n.x*u.z,z:n.x*u.y-n.y*u.x},r=m.size,a=clamp(m.life/.65,0,1)*clamp(m.life/(m.maxLife||5),0,1),pts=[[-1,-.45],[.05,-1],[1,-.25],[.55,.25],[.85,.9],[-.1,.62],[-.8,1],[-.55,.15]].map(([i,j])=>[m.x+u.x*i*r+v.x*j*r,m.y+u.y*i*r+v.y*j*r,m.z+u.z*i*r+v.z*j*r]);ctx.save();ctx.globalAlpha=.82*a;drawPoly(pts,shadeColor(m.color,-105),'rgba(7,16,24,.7)',1);ctx.restore()}}

function drawMuzzleFlashes(firstPerson=false){
  for(const f of muzzleFlashes){const ownView=f.owner===viewEnt;if(ownView!==firstPerson)continue;let q,dirEnd;
    if(firstPerson){const g=gunSpec(f.type),bob=(viewEnt.onGround?Math.sin(performance.now()/95)*Math.min(.035,Math.hypot(viewEnt.vx,viewEnt.vz)*.0024):0),kick=(viewEnt.recoilPitch||0)*1.9,bx=Math.min(.43,viewRect.w/viewRect.h*.28),by=-.54+bob-kick*.35,bz=.86-kick,tip=fpPoint(bx,by+.03,bz+g.body[2]*.52+g.barrel[2]*1.22),axes=fpAxes();q=project(tip[0],tip[1],tip[2]);dirEnd=q&&project(tip[0]+axes.F.x*.45,tip[1]+axes.F.y*.45,tip[2]+axes.F.z*.45)}
    else{if(visualDetailAt(f.x,f.y,f.z)<0||!visualVisible(f.x,f.y,f.z))continue;q=project(f.x,f.y,f.z);dirEnd=project(f.x+f.dx*.45,f.y+f.dy*.45,f.z+f.dz*.45)}if(!q)continue;
    const a=clamp(f.life/f.maxLife,0,1),base=(firstPerson?18:Math.max(5,q.s*.19))*f.size,ang=dirEnd?Math.atan2(dirEnd.y-q.y,dirEnd.x-q.x):0,type=f.type;ctx.save();ctx.translate(q.x,q.y);ctx.rotate(ang);ctx.globalAlpha=a;ctx.globalCompositeOperation='lighter';ctx.fillStyle=f.color;ctx.strokeStyle=f.color;ctx.lineCap='round';
    const halo=base*(1.45+(NeonCombatFeedback.FIRING[type]?.halo||.5)),glow=ctx.createRadialGradient(0,0,0,0,0,halo);glow.addColorStop(0,f.color);glow.addColorStop(1,'rgba(0,0,0,0)');ctx.globalAlpha=a*.12*visualPreset().glow;ctx.fillStyle=glow;ctx.fillRect(-halo,-halo,halo*2,halo*2);ctx.globalAlpha=a;ctx.fillStyle=f.color;
    if(type==='shotgun'){ctx.beginPath();ctx.moveTo(base*1.45,0);ctx.lineTo(-base*.15,-base*.82);ctx.lineTo(-base*.42,0);ctx.lineTo(-base*.15,base*.82);ctx.closePath();ctx.fill();}
    else if(type==='sniper'){ctx.lineWidth=Math.max(1.5,base*.12);ctx.beginPath();ctx.moveTo(-base*.35,0);ctx.lineTo(base*2.2,0);ctx.stroke();ctx.beginPath();ctx.arc(0,0,base*.32,0,TAU);ctx.fill();}
    else if(type==='burst'){for(let i=0;i<3;i++){const off=(i-f.pulse)*base*.28;ctx.globalAlpha=a*(1-i*.18);ctx.beginPath();ctx.moveTo(base*(.85+i*.18),off);ctx.lineTo(-base*.18,off-base*.22);ctx.lineTo(-base*.05,off+base*.22);ctx.closePath();ctx.fill();}}
    else if(type==='arc'){ctx.lineWidth=Math.max(1.3,base*.11);for(const off of [-.28,.28]){ctx.beginPath();ctx.moveTo(-base*.15,off*base);ctx.lineTo(base*.35,-off*base*.4);ctx.lineTo(base*.72,off*base*.65);ctx.lineTo(base*1.28,off*base*.15);ctx.stroke();}}
    else if(type==='rocket'){ctx.beginPath();ctx.ellipse(base*.28,0,base*1.05,base*.62,0,0,TAU);ctx.fill();ctx.fillStyle='#fff0b5';ctx.beginPath();ctx.ellipse(base*.12,0,base*.42,base*.28,0,0,TAU);ctx.fill();}
    else if(type==='mortar'){for(const [x,ry] of [[.05,.72],[.45,.48],[.86,.28]]){ctx.beginPath();ctx.ellipse(base*x,0,base*(.52-x*.16),base*ry,0,0,TAU);ctx.fill()}ctx.strokeStyle='#fff0f4';ctx.lineWidth=Math.max(1.5,base*.11);ctx.beginPath();ctx.arc(base*.2,0,base*.68,0,TAU);ctx.stroke();}
    else if(type==='punch'){ctx.lineWidth=Math.max(1.4,base*.12);for(let i=-1;i<=1;i++){ctx.beginPath();ctx.moveTo(-base*.25,i*base*.24);ctx.lineTo(base*.18,-i*base*.08);ctx.lineTo(base*.62,i*base*.2);ctx.lineTo(base*1.15,i*base*.04);ctx.stroke();}}
    else if(type==='harpoon'){ctx.lineWidth=Math.max(1.3,base*.10);ctx.beginPath();ctx.moveTo(-base*.28,0);ctx.lineTo(base*1.35,0);ctx.stroke();ctx.beginPath();ctx.moveTo(base*1.38,0);ctx.lineTo(base*.70,-base*.38);ctx.lineTo(base*.86,0);ctx.lineTo(base*.70,base*.38);ctx.closePath();ctx.fill();}
    else if(type==='plasma'){ctx.beginPath();ctx.arc(base*.2,0,base*.62,0,TAU);ctx.fill();ctx.fillStyle='#fff';ctx.globalAlpha=a*.8;ctx.beginPath();ctx.arc(base*.2,0,base*.24,0,TAU);ctx.fill();}
    else if(type==='mender'){ctx.lineWidth=Math.max(1,base*.07);ctx.beginPath();ctx.moveTo(-base*.62,0);ctx.lineTo(base*.72,0);ctx.stroke();ctx.beginPath();ctx.arc(base*.05,0,base*.24,0,TAU);ctx.stroke();ctx.beginPath();ctx.moveTo(base*.05,-base*.14);ctx.lineTo(base*.05,base*.14);ctx.moveTo(-base*.09,0);ctx.lineTo(base*.19,0);ctx.stroke();}
    else if(type==='laser'){ctx.lineWidth=Math.max(1.2,base*.09);ctx.beginPath();ctx.moveTo(-base*.8,0);ctx.lineTo(base*.8,0);ctx.stroke();ctx.beginPath();ctx.moveTo(0,-base*.48);ctx.lineTo(0,base*.48);ctx.stroke();ctx.beginPath();ctx.arc(0,0,base*.27,0,TAU);ctx.fill();}
    else if(type==='pistol'){ctx.lineWidth=Math.max(1.2,base*.09);for(let i=0;i<6;i++){const a=i*TAU/6;ctx.beginPath();ctx.moveTo(Math.cos(a)*base*.12,Math.sin(a)*base*.12);ctx.lineTo(Math.cos(a)*base*.92,Math.sin(a)*base*.56);ctx.stroke();}}
    else{const tight=type==='assault',strobe=type==='smg';ctx.beginPath();ctx.moveTo(base*(tight?1.55:1.28),0);ctx.lineTo(-base*.18,-base*(strobe?.52:.34));ctx.lineTo(-base*.05,-base*.10);ctx.lineTo(-base*.4,0);ctx.lineTo(-base*.05,base*.10);ctx.lineTo(-base*.18,base*(strobe?.52:.34));ctx.closePath();ctx.fill();}
    ctx.fillStyle='#ffffff';ctx.globalAlpha=Math.min(1,a*1.25);ctx.beginPath();ctx.ellipse(0,0,base*.34,base*.18,0,0,TAU);ctx.fill();ctx.restore();
  }
}
function drawBeamTrail(s,isLaser){const a=clamp(s.life/(s.maxLife||s.life||.1),0,1),support=!!s.support,steps=Math.max(3,Math.min(8,Math.ceil(s.len/10))),segments=[];let run=null;for(let i=0;i<=steps;i++){const t=i/steps,x=s.x+s.dx*s.len*t,y=s.y+s.dy*s.len*t,z=s.z+s.dz*s.len*t,q=project(x,y,z),visible=q&&visualVisible(x,y,z);if(visible){if(!run)run=[q,q];else run[1]=q}else if(run){segments.push(run);run=null}}if(run)segments.push(run);if(!segments.length)return;ctx.save();ctx.lineCap='round';for(const seg of segments){ctx.globalAlpha=support?.78*a:(isLaser?.95*a:.78*a);ctx.strokeStyle=s.color;ctx.lineWidth=(s.width||3)*(support?1.15:(isLaser?2.4:1.2));ctx.shadowBlur=support?10:(isLaser?28:18);ctx.shadowColor=s.color;ctx.beginPath();ctx.moveTo(seg[0].x,seg[0].y);ctx.lineTo(seg[1].x,seg[1].y);ctx.stroke();ctx.globalAlpha=support?.72*a:(isLaser?1:a);ctx.strokeStyle=support?'#dfffff':'#fff';ctx.lineWidth=support?.7:(isLaser?3.2:1.2);ctx.shadowBlur=support?5:(isLaser?16:0);ctx.beginPath();ctx.moveTo(seg[0].x,seg[0].y);ctx.lineTo(seg[1].x,seg[1].y);ctx.stroke()}if(isLaser){const end=segments[segments.length-1][1];ctx.fillStyle=s.color;ctx.shadowBlur=support?14:22;ctx.beginPath();ctx.arc(end.x,end.y,(support?4:7)+(support?4:8)*a,0,TAU);ctx.fill()}ctx.restore()}
function faceDepth(pts){let d=0;for(const p of pts)d+=cameraPoint(p[0],p[1],p[2]).z;return d/pts.length}
function drawPickups(){
  for(const p of pickups){if(p.gone||p.taken>0)continue;const y=p.y+.4;if(!visualVisible(p.x,y,p.z))continue;const q=project(p.x,y,p.z);if(!q)continue;const near=visualDistance(p.x,y,p.z)<8;if(!near)continue;ctx.save();ctx.font='bold 11px Arial';ctx.fillStyle='#ecfaff';ctx.shadowColor='#000';ctx.shadowBlur=4;ctx.textAlign='center';ctx.fillText(p.kind==='health'?'HEALTH +'+p.amount:p.kind==='armor'?'ARMOR +'+p.amount:WEAPONS[p.type].name,q.x,q.y-24);ctx.restore();}
}

function gunSpec(type){return {
  pistol:{body:[.24,.17,.40],barrel:[.08,.08,.32],stock:[.12,.13,.12],grip:[.14,.30,.14],accent:.12},
  assault:{body:[.34,.22,.82],barrel:[.13,.13,.78],stock:[.34,.24,.34],grip:[.18,.46,.20],accent:.28},
  sniper:{body:[.28,.20,1.08],barrel:[.08,.08,1.12],stock:[.28,.22,.46],grip:[.14,.38,.18],accent:.42},
  smg:{body:[.38,.25,.56],barrel:[.13,.13,.44],stock:[.24,.2,.22],grip:[.20,.46,.18],accent:.22},
  shotgun:{body:[.42,.25,.78],barrel:[.16,.16,.88],stock:[.36,.26,.38],grip:[.18,.44,.18],accent:.24},
  burst:{body:[.32,.21,.72],barrel:[.10,.10,.92],stock:[.28,.22,.30],grip:[.16,.42,.18],accent:.30},
  arc:{body:[.50,.34,.54],barrel:[.22,.22,.68],stock:[.18,.22,.20],grip:[.20,.42,.20],accent:.40},
  plasma:{body:[.54,.40,.60],barrel:[.24,.24,.55],stock:[.24,.24,.24],grip:[.20,.42,.20],accent:.36},
  laser:{body:[.34,.26,.86],barrel:[.12,.12,1.10],stock:[.26,.22,.34],grip:[.16,.42,.18],accent:.38},
  mender:{body:[.38,.28,.72],barrel:[.10,.10,.92],stock:[.28,.22,.30],grip:[.16,.42,.18],accent:.34},
  rocket:{body:[.48,.34,.92],barrel:[.26,.26,1.05],stock:[.30,.24,.32],grip:[.20,.46,.20],accent:.44},
  mortar:{body:[.52,.38,.82],barrel:[.30,.30,.90],stock:[.32,.25,.34],grip:[.20,.46,.20],accent:.46},
  punch:{body:[.42,.30,.62],barrel:[.24,.24,.34],stock:[.22,.22,.22],grip:[.22,.46,.20],accent:.36},
  harpoon:{body:[.34,.25,.78],barrel:[.12,.12,1.02],stock:[.30,.23,.34],grip:[.18,.44,.19],accent:.34}
}[type]||{body:[.34,.22,.82],barrel:[.13,.13,.78],stock:[.34,.24,.34],grip:[.18,.46,.20],accent:.28}}
function drawThermalScopeHighlights(ent){
  if(ent!==player||ent.weapon!=='sniper'||!hasUpgrade('sniper','thermalScope')||ent.dead)return;
  const origin=actorEyePosition(ent),forward=dirFrom(ent,0),maxRange=72;
  for(const target of targetsFor(ent,true)){
    if(target.dead||target.spawnGrace>0)continue;
    const tx=target.x,ty=target.alien?alienTargetY(target,target.y-.72):humanTargetY(target,'body'),tz=target.z,dx=tx-origin.x,dy=ty-origin.y,dz=tz-origin.z,dist=Math.hypot(dx,dy,dz);
    if(dist<2||dist>maxRange)continue;
    const inv=1/dist,dir={x:dx*inv,y:dy*inv,z:dz*inv};
    if(dir.x*forward.x+dir.y*forward.y+dir.z*forward.z<.72)continue;
    const blockers=rayBlockersBetween(origin,dir,Math.max(0,dist-.18));
    if(blockers.length!==1||blockers[0].block.kind!=='wall')continue;
    const head=project(target.x,target.alien?alienTargetY(target,target.y-.12):humanTargetY(target,'head')+.2,target.z),feet=project(target.x,target.y-PLAYER_H+.1,target.z);
    if(!head||!feet)continue;
    const height=clamp(Math.abs(feet.y-head.y)*1.18,18,150),width=clamp(height*.48,12,72),cx=(head.x+feet.x)/2,cy=(head.y+feet.y)/2;
    ctx.save();ctx.globalAlpha=.82;ctx.strokeStyle='#ff2f55';ctx.lineWidth=2;ctx.shadowBlur=10;ctx.shadowColor='#ff2f55';ctx.setLineDash([7,4]);ctx.strokeRect(cx-width/2,cy-height/2,width,height);ctx.setLineDash([]);ctx.globalAlpha=.16;ctx.fillStyle='#ff2f55';ctx.fillRect(cx-width/2,cy-height/2,width,height);ctx.restore();
  }
}
function drawBotLabelIfVisible(b){
  if(!allyHighlightForView(b))return;
  const eye=actorEyePosition(viewEnt||player),feet=b.y-PLAYER_H,top=feet+actorStandingHeight(b)*(1-(b.slidePose||0)*.27),markerY=top+.24;
  // The tiny marker needs an actual visible teammate and its own clear ray;
  // the model's cyan rim remains in the ordinary depth-tested world pass.
  if(![humanTargetY(b,'head'),humanTargetY(b,'body')].some(y=>lineClear(eye.x,eye.y-.05,eye.z,b.x,y,b.z))||!lineClear(eye.x,eye.y-.05,eye.z,b.x,markerY,b.z))return;
  const marker=project(b.x,markerY,b.z);if(!marker)return;
  ctx.save();ctx.fillStyle='rgba(18,232,255,.38)';ctx.strokeStyle='#65f4ff';ctx.lineWidth=1.5;ctx.shadowColor='#00dfff';ctx.shadowBlur=4;
  ctx.beginPath();ctx.moveTo(marker.x,marker.y-4);ctx.lineTo(marker.x+4,marker.y);ctx.lineTo(marker.x,marker.y+4);ctx.lineTo(marker.x-4,marker.y);ctx.closePath();ctx.fill();ctx.stroke();ctx.restore();
}

function fpAxes(){
  const ent=viewEnt||player,yaw=ent.yaw,pitch=ent.pitch,sy=Math.sin(yaw),cy=Math.cos(yaw),sp=Math.sin(pitch),cp=Math.cos(pitch);
  const F={x:sy*cp,y:-sp,z:cy*cp}, R={x:cy,y:0,z:-sy}, U={x:sy*sp,y:cp,z:cy*sp};
  return {F,R,U};
}
function fpPoint(rx,ry,rz){const a=fpAxes(),eye=actorEyePosition(viewEnt||player);return [eye.x+a.R.x*rx+a.U.x*ry+a.F.x*rz,eye.y+a.R.y*rx+a.U.y*ry+a.F.y*rz,eye.z+a.R.z*rx+a.U.z*ry+a.F.z*rz]}
function reloadVisualState(ent,w){
  if(!ent||ent.reload<=0||!w||w.kind==='plasma')return {active:false,progress:1,tilt:0};
  const total=Math.max(.05,ent.reloadTotal||w.reload||ent.reload),progress=clamp(1-ent.reload/total,0,1),ease=progress<.5?progress*2:(1-progress)*2;
  return {active:true,progress,tilt:.58*Math.sin(Math.PI*progress),settle:ease};
}
function drawMinimap(dt=0){if(game.p1Input==='touch')return;minimapTimer-=dt;if(!minimapNeedsRedraw&&minimapTimer>0)return;const crowdedSurvivor=game.mode==='survivor'&&bots.filter(b=>b.alien&&!b.dead).length>15;minimapTimer=1/(crowdedSurvivor?6:9);minimapNeedsRedraw=false;mctx.clearRect(0,0,178,178);mctx.fillStyle='rgba(8,12,28,.82)';mctx.fillRect(0,0,178,178);const sc=178/(ARENA_SAFE*2+8),ox=89,oz=89;mctx.strokeStyle='rgba(255,255,255,.25)';for(const p of platforms)if(p.active!==false)mctx.strokeRect(ox+(p.x-p.w/2)*sc,oz+(p.z-p.d/2)*sc,p.w*sc,p.d*sc);function dot(x,z,c,r=3){mctx.fillStyle=c;mctx.beginPath();mctx.arc(ox+x*sc,oz+z*sc,r,0,TAU);mctx.fill()}dot(player.x,player.z,'#fff',4);for(const b of bots)if(!b.dead&&visibleOnMinimap(b))dot(b.x,b.z,b.color,3);for(const p of pickups)if(!p.gone&&p.taken<=0){mctx.fillStyle=p.kind==='health'?'#ff315d':p.kind==='armor'?'#237dff':WEAPONS[p.type].color;mctx.fillRect(ox+p.x*sc-1.5,oz+p.z*sc-1.5,3,3);}}
initializeSurvival();
let last=performance.now(),lastFrameDt=0;function loop(now){const rawMs=Math.max(.1,now-last),dt=Math.min(.033,rawMs/1000);last=now;lastFrameDt=dt;updateAdaptiveQuality(rawMs,now);updateMobileControlVisibility();if(mobileInput.visible)updateMobileControlLabels();if(!game.started||game.over)pollController();if(game.started&&!game.over){if(game.mode!=='survivor'&&game.mode!=='frag'&&game.mode!=='siege'&&game.mode!=='conquest'&&game.mode!=='campaign')game.timeLeft-=dt;warClock+=dt;updateWar(dt);updatePlayer(dt);for(const b of bots)updateBot(b,dt);updateSurvivor(dt);updateSiege(dt);updateDeathmatchItemSpawns(dt);updateProjectiles(dt);explosionSystem?.update(dt);updateCombatUpgradeEffects(dt);updateCameraKick(player,dt);updateCameraKick(player2,dt);buildFeedback=Math.max(0,buildFeedback-dt);if(player2)player2.buildFeedback=Math.max(0,(player2.buildFeedback||0)-dt);for(const p of [...walls,...platforms]){p.flash=Math.max(0,(p.flash||0)-dt);p.fortifySealTimer=Math.max(0,(p.fortifySealTimer||0)-dt);p.thunderGuardTimer=Math.max(0,(p.thunderGuardTimer||0)-dt);p.armorBoosted=(p.bonusHp||0)>0;}for(const s of shots)s.life-=dt;shots=shots.filter(s=>s.life>0);for(const l of lasers)l.life-=dt;lasers=lasers.filter(l=>l.life>0);for(const m of impacts)m.life-=dt;impacts=impacts.filter(m=>m.life>0);for(const d of decals)d.life-=dt;decals=decals.filter(d=>d.life>0);for(const u of ultimateFx)u.life-=dt;ultimateFx=ultimateFx.filter(u=>u.life>0);for(const f of muzzleFlashes)f.life-=dt;muzzleFlashes=muzzleFlashes.filter(f=>f.life>0);for(const p of pickups)if(!p.oneShot&&p.taken>0)p.taken-=dt;pickups=pickups.filter(p=>!p.gone);for(const p of particles){p.life-=dt;if(p.kind!=='ring'){p.x+=p.vx*dt;p.y+=p.vy*dt;p.z+=p.vz*dt;p.vy-=(p.kind==='smoke'?-1.2:p.kind==='dust'?3:18)*dt}}particles=particles.filter(p=>p.life>0);if(game.mode!=='survivor'&&game.mode!=='frag'&&game.mode!=='siege'&&game.mode!=='conquest'&&game.mode!=='campaign'&&game.timeLeft<=0)endMatch('Time expired');updateHud()}updateAmbientAudio();draw();requestAnimationFrame(loop)}requestAnimationFrame(loop);
function updateHud(){updateSurvivalGearHud();
  const aliveAliens=survivorAliveAliens().length;
  if(game.mode==='siege'){
    const alive=siegeAliveEnemies().length,total=siegeEnemies().length,buildLeft=Math.ceil(game.siegeBuildTime||0);
    hud.score.textContent='DEF '+(game.siegePlayers||0)+' - RAID '+(game.siegeEnemies||0)+' · R'+(game.siegeRound||1)+'/5';
    hud.mode.textContent='SIEGE';
    hud.timer.textContent=siegeBuildActive()?('BUILD '+buildLeft+'s'):'BREACH';
    hud.botsLeft.textContent=siegeBuildActive()?'FREE BUILDS':('RAIDERS '+alive+'/'+total);
  }else if(game.mode==='survivor'){
    hud.score.textContent='WAVE '+game.survivorWave+' · SCRAP '+Math.floor(game.scrap||0)+' · KOS '+player.score;
    hud.mode.textContent='WAVE '+game.survivorWave+' · SCRAP '+Math.floor(game.scrap||0);
    hud.botsLeft.textContent='REMAINING '+Math.max(0,(game.waveTotal||0)-(game.waveKilled||0))+' · FIELD '+aliveAliens;
    hud.timer.textContent=game.shopOpen?'SHOP OPEN':(game.waveSpawning?('NEXT SQUAD '+Math.max(0,game.nextSquadTimer||0).toFixed(1)+'s'):(game.waveDelay>0?'NEXT WAVE '+game.waveDelay.toFixed(1)+'s':'FIGHT'));
  }else if(game.mode==='frag'){
    const rival=scoreRows().find(r=>!r.you);
    hud.score.textContent='YOU '+player.score+'/'+fragKillLimit()+' · D '+player.deaths;
    hud.mode.textContent='FRAG';
    hud.timer.textContent='FIRST TO '+fragKillLimit();
    hud.botsLeft.textContent=rival?(rival.name+' '+rival.score+'/'+fragKillLimit()):'SNIPER DUEL';
  }else{
    hud.score.textContent='YOU '+player.score+' / D '+player.deaths;
    hud.mode.textContent=game.mode==='tdm'?'TEAM DM':'DEATHMATCH';
    const t=Math.max(0,game.timeLeft|0),m=(t/60)|0,sec=(t%60+'').padStart(2,'0');
    hud.timer.textContent=m+':'+sec;
    hud.botsLeft.textContent='BOTS '+bots.filter(b=>!b.dead&&!b.human).length;
  }
  const w=WEAPONS[player.weapon]||WEAPONS.pistol;
  let ammoText;if(player.weapon==='harpoon')ammoText=(player.reload>0?'WINCHING · ':'')+player.ammo+' / '+w.mag+((player.harpoonTetherTimer||0)>0?' · TETHERED':'');else if(player.weapon==='mortar')ammoText=player.mortarActive?'GUIDING STRIKE':((player.mortarImpactTimer||0)>0?'IMPACT INCOMING':(player.reload>0?'RELOADING '+player.reload.toFixed(1)+'s':'MORTAR READY'));else ammoText=(player.weapon==='plasma')?'HEAT '+Math.round((player.heat||0)*100)+'%  CHARGE '+Math.round((player.charge||0)*100)+'%':(player.weapon==='mender'?menderHeatStatus(player):(player.weapon==='laser'?(player.reload>0?'COOLING '+player.reload.toFixed(1)+'s':'LASER HEAT '+Math.round((player.heat||0)*100)+'%'):(player.reload>0?'RELOADING '+player.reload.toFixed(1)+'s · R':player.ammo+'/'+w.mag+' · R RELOAD')));
  if(player.weapon==='assault'&&hasUpgrade('assault','momentumClip'))ammoText+=' · MOMENTUM '+Math.round((player.assaultMomentum||0)*100)+'%';if(player.weapon==='smg'&&hasUpgrade('smg','voltDash'))ammoText+=player.voltDashCharge?' · DASH READY':' · DASH '+Math.round(clamp((player.voltDashMeter||0)/150,0,1)*100)+'%';if(player.weapon==='burst'&&hasUpgrade('burst','tacticalRollout')&&(player.tacticalTimer||0)>0)ammoText+=' · HANDLING BOOST';hud.ammo.textContent=ammoText;hud.ammo.style.color=player.reload>0?'#ffea00':'#fff';
  hud.healthText.textContent=Math.max(0,Math.round(player.hp));hud.healthFill.style.width=clamp(player.hp/player.maxHp,0,1)*100+'%';hud.armorText.textContent=Math.max(0,Math.round(player.armor||0));hud.armorFill.style.width=clamp((player.armor||0)/(player.maxArmor||100),0,1)*100+'%';hud.upgradePoints.textContent=player.upgradePoints||0;hud.reloadHint.textContent=player.weapon==='mender'?'NO RELOAD · RELEASE FIRE TO COOL':(player.reload>0?'RELOADING — KEEP MOVING':'F / PICKUP BUTTON: PICK UP OR SWAP · R: RELOAD · TAB/Y: UPGRADES');hud.weaponName.textContent=w.name;
  updatePickupPrompt();
  if(game.upgradesOpen)el('upgradePointLabel').textContent=(player.upgradePoints||0)+' POINT'+(player.upgradePoints===1?'':'S')
}
function scoreRows(){const humanRows=[{name:'YOU',score:player.score,deaths:player.deaths,weapon:WEAPONS[player.weapon]?.name||'PISTOL',you:true,team:player.team}];if(player2)humanRows.push({name:'PLAYER 2',score:player2.score,deaths:player2.deaths||0,weapon:WEAPONS[player2.weapon]?.name||'PISTOL',team:player2.team});if(game.mode==='survivor')return humanRows.sort((a,b)=>b.score-a.score||a.deaths-b.deaths);return [...humanRows,...bots.filter(b=>!b.human).map(b=>({name:b.name,score:b.score,deaths:b.deaths||0,weapon:WEAPONS[b.weapon]?.name||'',team:b.team}))].sort((a,b)=>b.score-a.score||a.deaths-b.deaths)}
function teamScores(){const totals={};for(const r of scoreRows()){const key=game.mode==='tdm'?factionName(r.team):r.name;totals[key]=(totals[key]||0)+r.score}return Object.entries(totals).sort((a,b)=>b[1]-a[1])}
function checkWinCondition(){if(!game.started||game.over||game.mode==='survivor'||game.mode==='siege'||game.mode==='conquest'||game.mode==='campaign')return;const limit=game.scoreTarget||(game.mode==='tdm'?30:game.mode==='frag'?fragKillLimit():15);if(game.mode==='tdm'){const top=teamScores()[0];if(top&&top[1]>=limit)endMatch(top[0]+' reached '+limit+' eliminations')}else{const leader=scoreRows()[0];if(leader&&leader.score>=limit)endMatch(leader.name+' reached '+limit+(game.mode==='frag'?' frags':' eliminations'))}}
function endMatch(reason='Time expired'){if(game.over)return;if(game.mode==='survivor')survivalProgress?.checkpoint();game.over=true;game.shopOpen=false;el('survivorShop').style.display='none';fireHeld=false;controllerFireHeld=false;buildHeld=false;buildPreview=null;toggleUpgradeTree(false);document.exitPointerLock?.();const rows=scoreRows(),winner=game.mode==='campaign'?(campaignRun?.status==='complete'?'MISSION COMPLETE':'MISSION FAILED'):game.mode==='conquest'?(conquest?.winner===3?'DRAW':factionName(conquest?.winner||1)):game.mode==='survivor'?'SURVIVORS':(game.mode==='tdm'?teamScores()[0][0]:rows[0].name);feed(game.mode==='survivor'?'SURVIVOR ENDED — wave '+game.survivorWave:('ROUND OVER — winner: '+winner));msg(game.mode==='survivor'?'SURVIVOR ENDED — WAVE '+game.survivorWave:('ROUND OVER — '+winner+' WINS'));el('scoreboardTitle').textContent=game.mode==='survivor'?('SURVIVED TO WAVE '+game.survivorWave):(winner+' WINS');el('scoreboardReason').textContent=reason;if(warMode()){el('scoreboardTitle').textContent=game.mode==='campaign'?winner:((game.mode==='conquest'?conquest.winner==player.team:teamScores()[0]?.[0]===factionName(player.team))?'VICTORY · ':'DEFEAT · ')+factionName(player.team);if(game.mode==='campaign')el('nextRound').textContent='RETRY / CHECKPOINT';}let html='<tr><th>Rank</th><th>Player</th><th>KOs</th><th>Deaths</th><th>Weapon</th></tr>';rows.forEach((r,i)=>{html+='<tr class="'+(r.you?'you':'')+'"><td>#'+(i+1)+'</td><td>'+r.name+(game.mode==='tdm'?' · T'+r.team:'')+'</td><td>'+r.score+'</td><td>'+r.deaths+'</td><td>'+r.weapon+'</td></tr>'});el('scoreboardTable').innerHTML=html;el('scoreboardOverlay').style.display='flex';if(game.mode==='survivor')survivalInterface?.showResults(survivalProgress?.finishRun());}
// Cats vs Mice: reuse the authoritative collision, damage and renderer pipeline.
function warMode(){return ['tdm','conquest','campaign'].includes(game.mode)}
function factionName(team){return team===1?'CAT DOMINION':team===2?'MOUSE RESISTANCE':'DRAW'}
function warActors(){return [...new Set([player,player2,...bots].filter(Boolean))]}
function assignFaction(actor,index=0){
  const faction=actor.team===1?'cats':'mice',choices=window.NeonFactions.characters.filter(c=>c.faction===faction),selected=window.NeonWarUI.selection(),character=actor===player?choices.find(c=>c.id===selected.character)||choices[0]:choices[index%choices.length];
  actor.faction=faction;actor.factionCharacter=character.id;actor.warLoadout=actor.human?selected.weapon:character.weapon;actor.color=actor.team===1?'#f4bc69':'#64dccd';
  if(!actor.human)actor.name=character.name.toUpperCase()+' '+(index+1);
}
function chooseWarSpawn(ent){
  if(!warMode())return null;
  const team=ent.team||1,hq=warMap?.hq?.[team],friendly=warMap?.factionSpawns?.[team];
  let origins=conquest?NeonConquest.spawnCandidates(conquest,team):friendly?.length?friendly:hq?[hq]:edgeSpawns.filter(p=>team===1?p.x<0:p.x>=0);
  origins=origins.flatMap(origin=>origin.spawns?.length?[origin,...origin.spawns.map(p=>({...p,forward:origin.forward}))]:[origin]);
  if(game.mode==='campaign'&&!ent.human&&ent.patrolRoute?.length)origins=ent.patrolRoute;
  const enemies=warActors().filter(a=>!a.dead&&a!==ent&&a.team!==team),allies=warActors().filter(a=>a!==ent&&!a.dead);let best=null;
  for(const origin of origins)for(const [dx,dz] of [[0,0],[-3.5,0],[3.5,0],[0,-3.5],[0,3.5],[-4,-4],[4,4]]){
    const p=safeSpawnCandidate(ent,origin.x+dx,origin.z+dz,Math.max(0,(origin.y||PLAYER_H)-PLAYER_H));if(!p)continue;
    const nearest=Math.min(90,...enemies.map(a=>Math.hypot(a.x-p.x,a.z-p.z))),exposed=enemies.some(a=>Math.hypot(a.x-p.x,a.z-p.z)<45&&lineClear(p.x,p.y,p.z,a.x,a.y-.5,a.z)),crowded=allies.some(a=>Math.abs(a.y-p.y)<2.5&&Math.hypot(a.x-p.x,a.z-p.z)<3.1);
    if(crowded)continue;
    if(origin.forward&&(nearest<16||exposed))continue;
    const score=nearest-(exposed?70:0)+(origin.forward?22:0);if(!best||score>best.score)best={...p,score};
  }
  return best;
}
function newWarBot(team,index=0,patrolRoute=null){
  const styles=['rifler','rusher','skirmisher','marksman','heavy','rifler'],b={entityId:'war-'+(++warActorId),name:'SOLDIER',style:styles[index%6],human:false,x:0,y:PLAYER_H,z:0,vx:0,vy:0,vz:0,r:.5,team,score:0,deaths:0,dead:false,hp:100,maxHp:100,armor:50,maxArmor:100,yaw:team===1?Math.PI/4:-Math.PI*.75,pitch:0,onGround:true,weapon:'whisker',ammo:30,reload:0,cool:0,heat:0,spawnGrace:2,fireCd:.3,walk:0,slowTimer:0,slowAmount:0,stagger:0,strafe:index%2?1:-1,pickupBias:1,patrolRoute};
  applyBotPersonality(b);assignFaction(b,index);spawn(b);equip(b,b.warLoadout,false);bots.push(b);return b;
}
function initWarMatch(){
  bots=[];if(game.mode!=='tdm')pickups=pickups.filter(p=>p.kind!=='weapon');assignFaction(player);spawn(player);player.yaw=player.team===1?Math.PI/4:-Math.PI*.75;player.faceYaw=player.yaw;
  if(player2){assignFaction(player2,1);spawn(player2);bots.push(player2);}
  if(game.mode==='conquest'){
    const options=window.NeonWarUI.selection();conquest=NeonConquest.create(warMap,{...options,botBudget:options.budget});
    for(const [team,count] of Object.entries(NeonConquest.reinforcements(conquest,warActors())))for(let i=0;i<count;i++)newWarBot(+team,i);
  }else if(game.mode==='campaign'){
    const selected=window.NeonWarUI.selection(),progress=survivalProgress?.getCampaign?.()||NeonCampaign.freshProgress(),checkpoint=selected.resume?progress.checkpoints?.[selected.mission]:null;
    campaignRun=NeonCampaign.createRun(selected.mission,{difficulty:game.difficulty,checkpoint});
    if(checkpoint?.player){const p=checkpoint.player,safe=safeSpawnCandidate(player,p.x,p.z,Math.max(0,p.y-PLAYER_H));if(safe){Object.assign(player,safe);player.yaw=p.yaw??player.yaw;player.hp=p.hp??100;player.armor=p.armor??50;if(WEAPONS[p.weapon])equip(player,p.weapon,false);}}
    const mission=NeonCampaign.mission(selected.mission),count=campaignRun.guardCount||mission.guardCount||8,patrols=warMap.patrolRoutes||[];
    for(let i=0;i<count;i++){const b=newWarBot(3-player.team,i,patrols[i%Math.max(1,patrols.length)]||null);b.style=mission.guardTypes[i%mission.guardTypes.length];applyBotPersonality(b);}
    campaignRun.resumedEscort=!!checkpoint?.escort?.rescued;
    feed(mission.briefing||mission.name);saveWarCheckpoint();
  }else for(let i=0;i<game.botCount;i++)newWarBot((i%2)+1,i);
  // Preserve arena weapons and add the new arsenal at reachable authored nav points.
  for(const [i,w] of window.NeonFactions.weapons.entries()){const p=nav[Math.floor((i+1)*nav.length/8)];if(p)addPickup(w.id,p.x/WORLD_SCALE,p.z/WORLD_SCALE);}
}
function warObjective(a){
  if(!warMode())return null;
  const index=bots.filter(b=>!b.human&&b.team===a.team).indexOf(a);
  if(conquest)return NeonConquest.assignment(conquest,a,Math.max(0,index));
  if(game.mode==='campaign'){
    if(a===warEscort)return {x:player.x,y:player.y,z:player.z,role:'defend',radius:2.5};
    if(a.assaultOrder)return a.assaultOrder;
    if(campaignRun?.alarm?.level>0){const p=campaignRun.alarm.lastKnown;if(p&&campaignRun.elapsed-p.time<10)return {...p,role:index%3===0?'defend':'reinforce',radius:4};}
    return null;
  }
  // TDM squads use common map positions, never the hidden enemy position.
  const sectors=warMap?.sectors||nav.filter((_,i)=>i%Math.max(1,Math.floor(nav.length/5))===0).slice(0,5),target=sectors[(Math.floor(warClock/20)+Math.floor(index/3))%Math.max(1,sectors.length)];
  return target?{...target,role:['attack','overwatch','flank','defend'][Math.max(0,index)%4],radius:4}:null;
}
function warDetection(a,event){
  if(!warMode()||!event.position)return;
  if(event.confirmed)warSightingCache.set(a.team+':'+(event.position.id||'contact'),{...event.position,team:a.team,until:warClock+5});
  if(game.mode==='campaign'&&a.team!==player.team&&event.confirmed)NeonCampaign.raiseAlarm(campaignRun,{...event.position,source:event.kind});
}
function campaignPoint(){if(!campaignRun||!warMap)return null;return NeonCampaign.objectivePoint(campaignRun,warMap.missionSites?.[campaignRun.missionId]?.anchors||warMap.anchors)}
function campaignInteractionAvailable(){const p=campaignPoint(),eye=actorEyePosition(player);return p&&!player.dead&&Math.hypot(p.x-player.x,p.z-player.z)<4&&Math.abs((p.y||PLAYER_H)-player.y)<3&&lineClear(eye.x,eye.y,eye.z,p.x,(p.y||PLAYER_H)-.3,p.z)}
function campaignOptionalAvailable(){const p=warMap?.anchors?.security,eye=actorEyePosition(player);return !!(campaignRun?.alarm?.enabled&&p&&!player.dead&&Math.hypot(player.x-p.x,player.z-p.z)<3.8&&Math.abs(player.y-p.y)<3&&lineClear(eye.x,eye.y,eye.z,p.x,p.y,p.z));}
function saveWarCheckpoint(){
  if(!campaignRun)return;const progress=survivalProgress?.getCampaign?.()||NeonCampaign.freshProgress();progress.checkpoints||={};progress.checkpoints[campaignRun.missionId]=NeonCampaign.checkpoint(campaignRun,player);survivalProgress?.saveCampaign?.(progress);
}
function updateWar(dt){
  const trigger=el('warTrigger');if(trigger){trigger.hidden=!(game.started&&!game.over&&game.mode==='campaign'&&game.p1Input==='touch');trigger.textContent=game.weaponSafe?'WEAPON SAFE · TAP TO ARM':'WEAPON LIVE · TAP TO SAFE';trigger.setAttribute('aria-pressed',String(!game.weaponSafe));}
  if(!warMode())return;for(const actor of [player,player2].filter(Boolean))actor.aimPose=(actor.aimPose||0)+((actor.aiming?1:0)-(actor.aimPose||0))*Math.min(1,dt*12);
  if(conquest){
    for(const event of NeonConquest.step(conquest,dt,warActors(),(a,s)=>lineClear(a.x,a.y-.2,a.z,s.x,(s.y||PLAYER_H)-.2,s.z))){
      if(event.type==='reinforcements')for(const [team,count]of Object.entries(event.counts))for(let i=0;i<count;i++){
        const dead=bots.find(b=>!b.human&&b.dead&&b.team===+team);if(dead)spawn(dead);else newWarBot(+team,warActorId);
      }else feed(factionName(event.team)+' '+event.type+' sector '+event.sector);
    }
    for(const actor of [player,player2].filter(Boolean))if(actor.dead&&actor.warRespawnAt<=warClock)spawn(actor);
    if(conquest.winner)endMatch('Territory battle finished');
  }
  if(campaignRun&&!game.over){
    const before=campaignRun.index,objective=NeonCampaign.currentObjective(campaignRun),point=campaignPoint();
    if(campaignRun.escort.rescued&&!warEscort){const p=campaignRun.resumedEscort?{x:player.x+2,y:player.y,z:player.z}:warMap.anchors.prison||point;campaignRun.resumedEscort=false;warEscort=newWarBot(player.team,0);warEscort.escort=true;warEscort.name='RESCUED SOLDIER';if(p){const safe=safeSpawnCandidate(warEscort,p.x,p.z,Math.max(0,(p.y||PLAYER_H)-PLAYER_H));if(safe)Object.assign(warEscort,safe);}warEscort.spawnGrace=3;}
    const detected=bots.some(b=>b.team!==player.team&&!b.dead&&b.enemy===player&&['alert','engage'].includes(b.guardState));
    const nearby=bots.filter(b=>!b.dead&&b.team!==player.team&&point&&Math.hypot(b.x-point.x,b.z-point.z)<(objective?.radius||4)+2&&Math.abs(b.y-point.y)<3&&lineClear(b.x,b.y,b.z,point.x,point.y,point.z)).length;
    const usable=campaignInteractionAvailable(),nearSecurity=campaignOptionalAvailable();
    if(warInteract)player.warUseUntil=warClock+4.25;
    NeonCampaign.step(campaignRun,dt,{player,interact:(keys.KeyF||player.warUseUntil>warClock)&&usable,canInteract:usable,objectivePosition:point,anchors:warMap.anchors,optionalInteract:nearSecurity&&(keys.KeyF||player.warUseUntil>warClock),canOptionalInteract:nearSecurity,escort:warEscort,enemyCount:nearby,detected});warInteract=false;
    const reinforcement=NeonCampaign.consumeReinforcements(campaignRun);
    if(reinforcement){
      const count=typeof reinforcement==='number'?reinforcement:reinforcement.count||2,reinforcementObjective=NeonCampaign.currentObjective(campaignRun),reinforcementPoint=campaignPoint();
      const order=(b,i)=>{if(reinforcementObjective?.type==='defend'&&reinforcementPoint)b.assaultOrder={...reinforcementPoint,role:i%3===2?'flank':'attack',radius:reinforcementObjective.radius||5};};
      // A full roster still receives the counterattack orders; the cap limits additions, not the assault.
      bots.filter(b=>!b.dead&&!b.human&&b.team!==player.team).slice(0,Math.max(4,count)).forEach(order);
      for(let i=0;i<count&&bots.filter(b=>!b.dead&&!b.human).length<16;i++)order(newWarBot(3-player.team,warActorId),i);
    }
    if(before!==campaignRun.index){saveWarCheckpoint();feed('OBJECTIVE UPDATED');player.warUseUntil=0;}
    if(campaignRun.status==='complete'){const progress=NeonCampaign.completeProgress(survivalProgress?.getCampaign?.()||NeonCampaign.freshProgress(),campaignRun);survivalProgress?.saveCampaign?.(progress);window.NeonWarUI.refreshProgress();endMatch((NeonCampaign.mission(campaignRun.missionId)?.debrief||'Mission completed.')+' Progress and cosmetic rewards saved.');}
    if(campaignRun.status==='failed')endMatch('The operation failed. Retry from the last checkpoint.');
  }
  for(const [id,s]of warSightingCache)if(s.until<warClock)warSightingCache.delete(id);
}
function warCasualty(victim,killer){
  if(killer)killer.score=(killer.score||0)+1;if(campaignRun&&killer?.team===player.team&&victim.team!==player.team)campaignRun.stats.kills++;
  burst(victim.x,victim.y,victim.z,victim.color||'#d5c7aa');
  humanBotController()?.hear({kind:'fallen-ally',x:victim.x,y:victim.y,z:victim.z,team:victim.team,source:victim.entityId,range:12});
  if(conquest){NeonConquest.casualty(conquest,victim.team);if(victim.human)victim.warRespawnAt=warClock+Math.max(1,game.respawnDelay||1);}
  else if(victim===player||victim===warEscort){campaignRun.status='failed';endMatch(victim===player?'You were lost in action. Retry from the saved checkpoint.':'The rescued soldier was lost. Retry the escort checkpoint.');}
}
function warShot(shooter,w){
  if(!w.pattern)return;shooter.warBurstIdle=0;
  const sequence=w.pattern,index=(shooter.warShotIndex||0)%sequence.length,kick=sequence[index];shooter.warShotIndex=(shooter.warShotIndex||0)+1;
  shooter.warRecoilPitch=clamp((shooter.warRecoilPitch||0)+.009*w.recoil,0,.10);shooter.warRecoilYaw=clamp((shooter.warRecoilYaw||0)+kick*.035*w.recoil,-.04,.04);
  if(visualQuality==='low')return;
  const y=shooter.y-.3,sx=Math.cos(shooter.yaw),sz=-Math.sin(shooter.yaw);
  particles.push({kind:'casing',x:shooter.x+sx*.4,y,z:shooter.z+sz*.4,vx:sx*3,vy:2,vz:sz*3,life:.65,maxLife:.65,c:'#dfbd75',size:2});
}
function addWarGeometry(){
  if(!game.started||!warMode())return;
  for(const sector of conquest?.sectors||[]){const y=(sector.y||PLAYER_H)-PLAYER_H,color=sector.contested?'#ffffff':sector.owner===1?'#efbb71':sector.owner===2?'#69d7ca':'#9aa8b7';addOrientedBox(sector.x,y+2,sector.z,.08,4,.08,0,'#adb8c2');addOrientedBox(sector.x+.7,y+3.4,sector.z,1.4,.8,.04,0,color);addOrientedBox(sector.x,y+.08,sector.z,1.1,.16,1.1,0,'#465563');}
  if(campaignRun){const p=campaignPoint();if(p){const y=(p.y||PLAYER_H)-PLAYER_H;addOrientedBox(p.x,y+.5,p.z,.8,1,.6,0,'#87b8b4');}}
}
function drawWarHud(ent){
  if(!game.started||game.over||!warMode())return;
  const x=viewRect.x+8,y=viewRect.y+96,w=Math.min(278,viewRect.w-16),color=ent.team===1?'#efbb71':'#69d7ca';ctx.save();ctx.fillStyle='rgba(6,14,22,.85)';ctx.fillRect(x,y,w,campaignRun?88:conquest?126:20);ctx.fillStyle=color;ctx.font='bold 10px system-ui';ctx.textAlign='left';
  if(conquest){
    const value=conquest.config.rule==='tickets'?conquest.tickets:conquest.score;ctx.fillText('CATS '+Math.ceil(value[1])+'  :  '+Math.ceil(value[2])+' MICE · WAVE '+Math.ceil(conquest.nextWave)+'s',x+7,y+15,w-14);
    const cell=(w-14)/5;for(const [i,s]of conquest.sectors.entries()){ctx.fillStyle=s.contested?'#ffffff':s.owner===1?'#efbb71':s.owner===2?'#69d7ca':'#9aa8b7';ctx.fillText(s.id+(s.contested?' !':''),x+7+i*cell,y+32);ctx.fillRect(x+7+i*cell,y+36,(cell-8)*s.progress,3);}
    const ox=x+w/2,oy=y+81,scale=Math.min(w/136,60/130),dot=(p,c,r)=>{ctx.fillStyle=c;ctx.beginPath();ctx.arc(ox+p.x*scale,oy+p.z*scale,r,0,TAU);ctx.fill()};
    for(const s of conquest.sectors){for(const link of s.links){const b=conquest.sectors.find(f=>f.id===link);if(b){ctx.strokeStyle='#52616b';ctx.beginPath();ctx.moveTo(ox+s.x*scale,oy+s.z*scale);ctx.lineTo(ox+b.x*scale,oy+b.z*scale);ctx.stroke();}}dot(s,s.owner===1?'#efbb71':s.owner===2?'#69d7ca':'#8996a4',4);}
    for(const a of warActors())if(!a.dead&&a.team===ent.team)dot(a,'#c6f6ee',2);for(const sight of warSightingCache.values())if(sight.team===ent.team)dot(sight,'#ff707d',2);dot(ent,'#fff',3);
  }else if(campaignRun){const o=NeonCampaign.currentObjective(campaignRun),p=campaignPoint(),distance=p?Math.round(Math.hypot(p.x-ent.x,p.z-ent.z)):0;ctx.fillText((o?.title||'MISSION OBJECTIVE')+' · '+distance+'m',x+7,y+16,w-14);ctx.fillStyle='#c7d6de';ctx.fillText('ALARM '+(campaignRun.alarm?.level>0?'ACTIVE':'QUIET')+' · '+(bots.some(b=>b.enemy===player&&!b.dead)?'DETECTED':'UNSEEN'),x+7,y+33,w-14);ctx.fillText('F / PICKUP: '+(campaignOptionalAvailable()&&!campaignInteractionAvailable()?'SILENCE ALARM':campaignInteractionAvailable()?'INTERACT '+Math.round(campaignRun.progress/Math.max(.1,o.duration)*100)+'%':'APPROACH OBJECTIVE'),x+7,y+50,w-14);ctx.fillText('C crouch · G aim · '+(ent.crouching?'CROUCHED':'STANDING'),x+7,y+67,w-14);
    if(p){const angle=Math.atan2(p.x-ent.x,p.z-ent.z)-ent.yaw,cx=viewRect.x+viewRect.w/2,cy=viewRect.y+24;ctx.save();ctx.translate(cx,cy);ctx.rotate(angle);ctx.fillStyle='#b1f5dc';ctx.beginPath();ctx.moveTo(0,-7);ctx.lineTo(5,5);ctx.lineTo(-5,5);ctx.fill();ctx.restore();}
  }else ctx.fillText(factionName(ent.team),x+7,y+14,w-14);
  ctx.restore();
}
const warTrigger=document.createElement('button');warTrigger.id='warTrigger';warTrigger.type='button';warTrigger.hidden=true;warTrigger.onclick=()=>{game.weaponSafe=!game.weaponSafe};el('mobileControls').appendChild(warTrigger);
addEventListener('keydown',event=>{if(!game.started||game.over||event.repeat||!warMode())return;if(event.code==='KeyC'){player.crouching=!player.crouching;event.preventDefault();}if(event.code==='KeyG'){player.aiming=!player.aiming;event.preventDefault();}});
for(const [id,key]of [['mobileSlide','crouching'],['mobileReload','aiming']]){const button=el(id);let held;button?.addEventListener('pointerdown',()=>{if(warMode())held=setTimeout(()=>{player[key]=!player[key];msg(key==='crouching'?(player.crouching?'CROUCHED':'STANDING'):(player.aiming?'AIMING':'HIP FIRE'));},450)});for(const event of ['pointerup','pointercancel','lostpointercapture'])button?.addEventListener(event,()=>clearTimeout(held));}
addEventListener('pagehide',()=>{if(campaignRun&&campaignRun.status==='active')saveWarCheckpoint();});

if(!CanvasRenderingContext2D.prototype.roundRect){CanvasRenderingContext2D.prototype.roundRect=function(x,y,w,h,r){this.moveTo(x+r,y);this.arcTo(x+w,y,x+w,y+h,r);this.arcTo(x+w,y+h,x,y+h,r);this.arcTo(x,y+h,x,y,r);this.arcTo(x,y,x+w,y,r);return this}}
})();

