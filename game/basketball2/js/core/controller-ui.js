/* Controller setup stays independent of gameplay so testing an input never fires it. */
function mountControllerSettings(host){
  if(!host) return null;
  if(host.basketballControllerSettings) return host.basketballControllerSettings;
  const actions = [
    {key:"shoot",label:"Shoot / finish",help:"Hold, then release to shoot or finish at the rim.",group:"Attack"},
    {key:"pass",label:"Pass",help:"Face the teammate you want to find.",group:"Attack"},
    {key:"switchPlayer",label:"Switch teammate",help:"Take control of another teammate in team games.",group:"Attack"},
    {key:"block",label:"Block / contest",help:"Jump to challenge a shot.",group:"Defend"},
    {key:"steal",label:"Steal",help:"Reach for an exposed dribble.",group:"Defend"},
    {key:"sprint",label:"Sprint",help:"Hold while moving to accelerate.",group:"Movement"},
    {key:"protect",label:"Protect / stance",help:"Protect your dribble or settle into a defensive stance.",group:"Movement"},
    {key:"leftSkill",label:"Left skill",help:"Create space with a move to the left.",group:"Skills"},
    {key:"rightSkill",label:"Right skill",help:"Create space with a move to the right.",group:"Skills"},
    {key:"takeover",label:"Takeover",help:"Activate a full Takeover meter.",group:"Skills"},
    {key:"pause",label:"Pause",help:"Open the pause menu.",group:"Game"}
  ];
  const buttonNames = ["A","B","X","Y","LB","RB","LT","RT","View","Menu","Left stick click","Right stick click","D-pad up","D-pad down","D-pad left","D-pad right","Home"];
  let player = "p1", visible = false, disposed = false, raf = 0, lastTick = 0, capture = null;
  let padFingerprint = "", lastTester = "", lastMessage = "";
  let customAxesRequested = {move:false,skill:false};
  const playerName = slot => slot === "p1" ? "Player 1" : "Player 2";
  host.classList.add("controller-settings");
  host.hidden = true;
  host.innerHTML = `
    <div class="controller-heading">
      <div><span class="controller-eyebrow">MAKE IT YOURS</span><h2>Controller setup</h2></div>
      <span id="controllerConnectionBadge" class="controller-connection-badge">Waiting for a controller</span>
    </div>
    <p class="controller-intro">Connect a controller by USB or Bluetooth, then press a button so your browser can find it. Choose how each player plays and set their inputs below.</p>
    <div id="controllerDevices" class="controller-devices" aria-label="Connected controllers"></div>
    <div class="controller-player-tabs" role="tablist" aria-label="Choose a player to configure">
      <button type="button" id="controllerTabP1" role="tab" aria-controls="controllerPlayerPanel" aria-selected="true" data-controller-player="p1">Player 1</button>
      <button type="button" id="controllerTabP2" role="tab" aria-controls="controllerPlayerPanel" aria-selected="false" tabindex="-1" data-controller-player="p2">Player 2</button>
    </div>
    <section id="controllerPlayerPanel" role="tabpanel" aria-labelledby="controllerTabP1">
      <p class="controller-local-note">Player 2 controls are used in Local 2-player matches. Set up both players here before you start.</p>
      <div class="controller-mode-card">
        <label class="controller-field" for="controllerInputMode">Play using
          <select id="controllerInputMode" aria-describedby="controllerModeHint"><option value="auto">Automatic</option><option value="keyboard">Keyboard &amp; touch</option><option value="controller">Controller</option></select>
        </label>
        <div class="controller-mode-state"><strong id="controllerModeState">Automatic input</strong><p id="controllerModeHint" class="controller-muted"></p></div>
      </div>
      <div class="controller-assignment-row">
        <label class="controller-field" for="controllerAssignment">Controller for <span id="controllerSelectedPlayer">Player 1</span>
          <select id="controllerAssignment" aria-describedby="controllerAssignmentHint"><option value="auto">Choose automatically</option><option value="none">No controller</option></select>
        </label>
        <div><p id="controllerAssignmentHint" class="controller-muted"></p><p class="controller-small">Each controller belongs to one player. You can change its inputs in any play mode.</p></div>
      </div>
      <div class="controller-content-grid">
        <div class="controller-bindings-card">
          <div class="controller-section-top"><div><h3>Buttons &amp; triggers</h3><p>Choose an action, then press its new input.</p></div></div>
          <label class="controller-combo-option"><input id="controllerComboCapture" type="checkbox"> Record a two-input combo</label>
          <div id="controllerCapturePanel" class="controller-capture-panel" hidden>
            <span class="controller-listening-dot" aria-hidden="true"></span>
            <p id="controllerCaptureHint"></p><button id="controllerCancelCapture" type="button">Cancel</button>
          </div>
          <div class="controller-binding-list">${actions.map(action=>`<div class="controller-binding-row">
            <span class="controller-action-copy"><strong>${action.label}</strong><small>${action.help}</small></span>
            <button type="button" class="controller-bind-button" data-bind-action="${action.key}" aria-label="Change ${action.label} input"><span data-binding-label="${action.key}">—</span><span class="controller-bind-edit" aria-hidden="true">Edit</span></button>
          </div>`).join("")}</div>
          <p class="controller-small">Occupied inputs swap where possible; any action needing a new input is shown as Unassigned. Default RB switches teammates in team games and performs the right skill in 1v1.</p>
          <details class="controller-manual"><summary>Choose an input manually</summary>
            <p class="controller-small">Useful for a controller with a different button layout. Button names follow the Xbox layout.</p>
            <div class="controller-manual-grid"><label class="controller-field">Action<select id="controllerManualAction">${actions.map(action=>`<option value="${action.key}">${action.label}</option>`).join("")}</select></label>
              <label class="controller-field">Input<select id="controllerManualInput"></select></label>
              <button id="controllerManualApply" type="button" class="controller-outline-button">Use input</button>
            </div>
          </details>
        </div>
        <div class="controller-side-column">
          <div class="controller-sticks-card"><h3>Sticks &amp; feel</h3><p class="controller-muted">Tune movement and skill moves separately.</p>
            ${["move","skill"].map(kind=>`<fieldset class="controller-stick-fieldset"><legend>${kind==="move"?"Movement stick":"Skill stick"}</legend>
              <label class="controller-field">Stick<select data-stick-preset="${kind}"><option value="left">Left stick</option><option value="right">Right stick</option><option value="custom">Custom axes</option><option value="off">Disabled</option></select></label>
              <div class="controller-custom-axes" data-custom-axes="${kind}" hidden>
                <label class="controller-field">Horizontal<select data-axis-kind="${kind}" data-axis-component="x"></select></label>
                <label class="controller-field">Vertical<select data-axis-kind="${kind}" data-axis-component="y"></select></label>
              </div>
              <div class="controller-invert-options"><label><input type="checkbox" data-invert-kind="${kind}" data-invert-component="invertX"> Reverse left / right</label><label><input type="checkbox" data-invert-kind="${kind}" data-invert-component="invertY"> Reverse up / down</label></div>
            </fieldset>`).join("")}
            <label class="controller-deadzone-label" for="controllerDeadzone">Stick deadzone <output id="controllerDeadzoneValue" for="controllerDeadzone">16%</output></label>
            <input id="controllerDeadzone" class="controller-deadzone" type="range" min="0.05" max="0.4" step="0.01" value="0.16">
            <p class="controller-small">Raise this if your player moves while the stick is resting.</p>
          </div>
          <div class="controller-tester-card"><h3>Try your controls</h3><p class="controller-muted">Inputs show here without affecting the game.</p>
            <div class="controller-stick-tests">${["move","skill"].map(kind=>`<div><div class="controller-stick-test" role="img" aria-label="${kind==="move"?"Movement":"Skill"} stick at rest"><i data-stick-dot="${kind}"></i></div><span>${kind==="move"?"Movement":"Skill"}</span></div>`).join("")}</div>
            <div id="controllerTestActions" class="controller-test-actions"><span class="controller-test-empty">Press a button or move a stick</span></div>
            <details class="controller-input-readout"><summary>Live button &amp; axis details</summary><output id="controllerRawInputs">No controller assigned.</output></details>
          </div>
        </div>
      </div>
      <div class="controller-settings-footer"><button id="controllerReset" type="button" class="controller-outline-button">Reset Player 1 controls</button><span>Changes save automatically on this device.</span></div>
    </section>
    <p id="controllerLiveStatus" class="controller-live-status" role="status" aria-live="polite" aria-atomic="true"></p>`;
  const find = selector => host.querySelector(selector);
  const all = selector => Array.from(host.querySelectorAll(selector));
  const assignment = find("#controllerAssignment"), inputMode = find("#controllerInputMode"), capturePanel = find("#controllerCapturePanel");
  const config = () => controllerConfig.getPlayer(player);
  const pads = () => inputManager.getConnectedGamepads();
  const assignedPad = () => inputManager.getAssignments()[player];
  function say(message){
    if(message === lastMessage) return;
    lastMessage = message;
    find("#controllerLiveStatus").textContent = message;
  }
  function buttonLabel(index,pad){
    if(pad?.mapping === "standard") return buttonNames[index] || `Button ${index+1}`;
    return `Button ${index+1}${index<buttonNames.length?` (${buttonNames[index]})`:""}`;
  }
  function labelBinding(binding){
    if(!binding) return "Unassigned";
    return controllerConfig.labelBinding(binding);
  }
  function padName(pad){ return pad.id || `Controller ${pad.index+1}`; }
  function option(select,value,label){
    const node = document.createElement("option");
    node.value = value; node.textContent = label; select.appendChild(node); return node;
  }
  function updateDevices(force=false){
    const connected = pads(), assignments = inputManager.getAssignments();
    const fingerprint = JSON.stringify({pads:connected.map(pad=>[pad.index,pad.id,pad.mapping,pad.buttons.length,pad.axes.length]),assignments:Object.values(assignments).map(pad=>pad?.index??null),player,assignment:config().assignment,inputMode:config().inputMode});
    if(!force && fingerprint===padFingerprint) return;
    padFingerprint = fingerprint;
    const devices = find("#controllerDevices"); devices.replaceChildren();
    if(!connected.length){
      const empty = document.createElement("p"); empty.className="controller-empty";
      empty.textContent = navigator.getGamepads ? "No controller detected yet. Connect it, then press A or another button." : "This browser does not expose controller input. Keyboard and touch controls are available.";
      devices.appendChild(empty);
    }
    for(const pad of connected){
      const card = document.createElement("div"); card.className="controller-device-card";
      const number = document.createElement("span"); number.className="controller-device-number"; number.textContent=String(pad.index+1);
      const copy = document.createElement("div");
      const title = document.createElement("strong"); title.textContent=padName(pad); title.title=padName(pad);
      const detail = document.createElement("span");
      const owner = Object.keys(assignments).find(slot=>assignments[slot]?.index===pad.index);
      detail.textContent=owner ? `Connected · ${playerName(owner)}` : "Connected · Available";
      const use=document.createElement("button");use.type="button";use.className="controller-use-device";use.dataset.useController=String(pad.index);
      const selected=assignments[player]?.index===pad.index && config().inputMode==="controller";
      use.textContent=selected?`Using for ${playerName(player)}`:`Use for ${playerName(player)}`;
      use.disabled=selected;use.setAttribute("aria-label",`${selected?"Using":"Use"} Controller ${pad.index+1} for ${playerName(player)}`);
      copy.append(title,detail); card.append(number,copy,use); devices.appendChild(card);
    }
    const badge = find("#controllerConnectionBadge");
    badge.textContent=connected.length ? `${connected.length} controller${connected.length===1?"":"s"} connected` : "Waiting for a controller";
    badge.classList.toggle("is-connected",connected.length>0);
    const setting = config().assignment;
    assignment.replaceChildren(); option(assignment,"auto","Choose automatically"); option(assignment,"none","No controller");
    for(const pad of connected) option(assignment,`pad:${pad.index}`,`Controller ${pad.index+1} · ${padName(pad)}`);
    if(setting && typeof setting==="object" && !connected.some(pad=>pad.index===setting.index && pad.id===setting.id)){
      option(assignment,"disconnected",`Disconnected · ${setting.id || "Saved controller"}`);
      assignment.value="disconnected";
    }else assignment.value=typeof setting==="object"?`pad:${setting.index}`:setting;
    const active = assignments[player];
    find("#controllerAssignmentHint").textContent = active ? `Controller ${active.index+1} is assigned to ${playerName(player)}.` : setting==="none" ? "No controller is assigned. Choose one to test or set its inputs." : "Waiting for the selected controller.";
    refreshInputMode(active);
    const maxAxes=Math.max(4,...connected.map(pad=>pad.axes.length));
    for(const select of all("[data-axis-kind]")){
      const current = select.value; select.replaceChildren(); option(select,"-1","Disabled");
      for(let i=0;i<Math.min(32,maxAxes);i++) option(select,String(i),`Axis ${i+1}${i<4?` · ${["Left horizontal","Left vertical","Right horizontal","Right vertical"][i]}`:""}`);
      select.value=current;
    }
    const manual=find("#controllerManualInput"), previous=manual.value; manual.replaceChildren();
    const maxButtons=Math.max(17,...connected.map(pad=>pad.buttons.length));
    for(let i=0;i<Math.min(64,maxButtons);i++) option(manual,`button:${i}`,buttonLabel(i,active || {mapping:"standard"}));
    for(let i=0;i<Math.min(32,maxAxes);i++){
      option(manual,`axis:${i}:1`,`Axis ${i+1} · positive`); option(manual,`axis:${i}:-1`,`Axis ${i+1} · negative`);
    }
    option(manual,"unassigned","Unassigned"); manual.value=previous || "button:0";
    refreshValues();
  }
  function refreshValues(){
    const settings=config();
    inputMode.value=settings.inputMode || "auto";
    find("#controllerSelectedPlayer").textContent=playerName(player);
    find("#controllerReset").textContent=`Reset ${playerName(player)} controls`;
    for(const tab of all("[data-controller-player]")){
      const selected=tab.dataset.controllerPlayer===player;
      tab.setAttribute("aria-selected",String(selected)); tab.tabIndex=selected?0:-1;
    }
    find("#controllerPlayerPanel").setAttribute("aria-labelledby",player==="p1"?"controllerTabP1":"controllerTabP2");
    for(const action of actions){
      find(`[data-binding-label="${action.key}"]`).textContent=labelBinding(settings.bindings[action.key]);
      const button=find(`[data-bind-action="${action.key}"]`);
      button.classList.toggle("is-listening",capture?.action===action.key);
      button.setAttribute("aria-label",`Change ${action.label} input, currently ${labelBinding(settings.bindings[action.key])}`);
    }
    for(const kind of ["move","skill"]){
      const axes=settings.axes[kind];
      const preset=customAxesRequested[kind]?"custom":axes.x<0 && axes.y<0?"off":axes.x===0 && axes.y===1?"left":axes.x===2 && axes.y===3?"right":"custom";
      find(`[data-stick-preset="${kind}"]`).value=preset;
      find(`[data-custom-axes="${kind}"]`).hidden=preset!=="custom";
      for(const component of ["x","y"]) find(`[data-axis-kind="${kind}"][data-axis-component="${component}"]`).value=String(axes[component]);
      // Game coordinates point up; the ordinary controller Y axis points down.
      find(`[data-invert-kind="${kind}"][data-invert-component="invertX"]`).checked=!!axes.invertX;
      find(`[data-invert-kind="${kind}"][data-invert-component="invertY"]`).checked=!axes.invertY;
    }
    find("#controllerDeadzone").value=String(settings.deadzone);
    find("#controllerDeadzoneValue").textContent=`${Math.round(settings.deadzone*100)}%`;
  }
  function refreshInputMode(pad){
    const mode=config().inputMode || "auto",state=find("#controllerModeState"),hint=find("#controllerModeHint");
    inputMode.value=mode;
    if(mode==="keyboard"){
      state.textContent="Keyboard & touch active";
      hint.textContent="Play with keyboard or touch. Controller inputs below can still be tested and changed.";
    }else if(mode==="controller"){
      state.textContent=pad?`Controller ${pad.index+1} active`:"Waiting for a controller";
      hint.textContent=pad?"Keyboard and touch gameplay are off; Esc still pauses. Use the inputs below or choose your own.":"Connect a controller, press a button, then choose it below. Esc still pauses while you wait.";
    }else{
      state.textContent=pad?`Controller ${pad.index+1} ready`:"Keyboard & touch ready";
      hint.textContent="Automatic accepts keyboard, touch, and your assigned controller. Connect a controller at any time.";
    }
    state.parentElement.classList.toggle("is-waiting",mode==="controller"&&!pad);
  }
  function refresh(){ if(disposed) return; updateDevices(true); updateTester(); }
  function readStick(pad,axes,deadzone){
    const x=(pad?.axes[axes.x] || 0)*(axes.invertX?-1:1);
    const y=(pad?.axes[axes.y] || 0)*(axes.invertY?-1:1);
    const magnitude=Math.hypot(x,y);
    if(magnitude<=deadzone) return {x:0,y:0};
    const scale=Math.min(1,(magnitude-deadzone)/(1-deadzone))/magnitude;
    return {x:x*scale,y:y*scale};
  }
  function bindingActive(pad,binding){
    if(!pad || !binding) return false;
    if(binding.context==="team" && typeof teamSize!=="undefined" && teamSize<2) return false;
    if(binding.context==="single" && typeof teamSize!=="undefined" && teamSize>1) return false;
    if(binding.type==="button") return (pad.buttons[binding.index]?.value || (pad.buttons[binding.index]?.pressed?1:0))>=(binding.threshold??.25);
    if(binding.type==="axis") return (pad.axes[binding.index] || 0)*(binding.direction || 1)>=(binding.threshold??.55);
    if(binding.type==="chord") return binding.inputs ? binding.inputs.every(input=>bindingActive(pad,input)) : binding.buttons?.every(index=>bindingActive(pad,{type:"button",index}));
    return false;
  }
  function updateTester(){
    const pad=assignedPad(),settings=config();
    for(const kind of ["move","skill"]){
      const value=readStick(pad,settings.axes[kind],settings.deadzone),dot=find(`[data-stick-dot="${kind}"]`);
      dot.style.transform=`translate(${(value.x*29).toFixed(1)}px,${(-value.y*29).toFixed(1)}px)`;
      const label=kind==="move"?"Movement":"Skill";
      dot.parentElement.setAttribute("aria-label",Math.hypot(value.x,value.y)>.05?`${label} stick moved`:`${label} stick at rest`);
    }
    const active=actions.filter(action=>bindingActive(pad,settings.bindings[action.key]));
    const fingerprint=active.map(action=>action.key).join(",")+(pad?"connected":"missing");
    if(fingerprint!==lastTester){
      lastTester=fingerprint; const holder=find("#controllerTestActions"); holder.replaceChildren();
      if(!active.length){const empty=document.createElement("span");empty.className="controller-test-empty";empty.textContent=pad?"Press a button or move a stick":"Choose a connected controller to test";holder.appendChild(empty);}
      for(const action of active){const chip=document.createElement("span");chip.className="controller-test-chip";chip.textContent=action.label;holder.appendChild(chip);}
    }
    const raw=find("#controllerRawInputs");
    if(pad){
      const held=Array.from(pad.buttons).flatMap((button,index)=>button.pressed || button.value>.1?[`${buttonLabel(index,pad)} ${Math.round(button.value*100)}%`]:[]);
      raw.textContent=`${held.length?held.join(" · "):"No buttons held"}\n${Array.from(pad.axes).map((axis,index)=>`Axis ${index+1}: ${Number(axis).toFixed(2)}`).join(" · ")}`;
    }else raw.textContent="No controller assigned.";
  }
  function cancelCapture(message="",restoreFocus=true){
    if(!capture) return;
    const origin=capture.origin; capture=null; capturePanel.hidden=true; refreshValues();
    if(message) say(message);
    if(restoreFocus && visible && origin?.isConnected){origin.focus({preventScroll:true});origin.scrollIntoView({block:"nearest"});}
  }
  function startCapture(action,origin){
    cancelCapture("",false);
    const pad=assignedPad();
    if(!pad){say("Connect and choose a controller before changing an input.");assignment.focus({preventScroll:true});return;}
    const name=actions.find(item=>item.key===action)?.label || action;
    capture={action,origin,index:pad.index,id:pad.id,start:performance.now(),neutralSince:0,armed:false,combo:find("#controllerComboCapture").checked,inputs:[],firstInputAt:0,axisRest:Array.from(pad.axes)};
    capturePanel.hidden=false;
    find("#controllerCaptureHint").textContent=`Release all controls. Then press the new input for ${name}.`;
    say(`Changing ${name}. Release all controls first. Press Escape to cancel.`);
    refreshValues();find("#controllerCancelCapture").focus({preventScroll:true});capturePanel.scrollIntoView({block:"nearest"});
  }
  function captureInputs(pad){
    const buttons=Array.from(pad.buttons).flatMap((button,index)=>button.pressed || button.value>.65?[{type:"button",index,threshold:.25}]:[]);
    if(buttons.length && !capture.combo) return buttons;
    const axes=Array.from(pad.axes).flatMap((value,index)=>{
      const rest=capture.axisRest[index] || 0;
      return Math.abs(value)>.7 && Math.abs(value-rest)>.55 ? [{type:"axis",index,direction:value>0?1:-1,threshold:.55}] : [];
    });
    return buttons.concat(axes);
  }
  function saveBinding(action,binding){
    const before=config().bindings,name=actions.find(item=>item.key===action).label;
    if(!controllerConfig.setBinding(player,action,binding)) return `${name} could not be changed. Choose another input.`;
    const after=config().bindings;
    const unassigned=actions.filter(item=>item.key!==action && before[item.key] && !after[item.key]).map(item=>item.label);
    return `${name} is now ${labelBinding(binding)}. Saved.${unassigned.length?` ${unassigned.join(", ")} ${unassigned.length===1?"needs":"need"} a new input.`:""}`;
  }
  function commitCapture(binding){
    const message=saveBinding(capture.action,binding);
    cancelCapture(message);
  }
  function pollCapture(now){
    if(!capture) return;
    const pad=pads().find(item=>item.index===capture.index && item.id===capture.id);
    if(!pad || assignedPad()?.index!==capture.index){cancelCapture("Input change cancelled because the controller disconnected or changed.");return;}
    if(now-capture.start>15000){cancelCapture("Input change timed out. Choose the action to try again.");return;}
    if(!capture.armed){
      const settings=config();
      const usedAxes=new Set([settings.axes.move.x,settings.axes.move.y,settings.axes.skill.x,settings.axes.skill.y].filter(index=>index>=0));
      const neutral=Array.from(pad.buttons).every(button=>!button.pressed && (button.value || 0)<.25) && Array.from(pad.axes).every((value,index)=>Math.abs(value)<.4 || (!usedAxes.has(index) && Math.abs(value-capture.axisRest[index])<.18));
      if(!neutral){capture.neutralSince=0;return;}
      if(!capture.neutralSince) capture.neutralSince=now;
      if(now-capture.neutralSince<180) return;
      capture.armed=true;capture.axisRest=Array.from(pad.axes);
      const name=actions.find(item=>item.key===capture.action).label;
      find("#controllerCaptureHint").textContent=`${capture.combo?"Press two inputs together":"Press a button, trigger, or move an axis"} for ${name}. Esc cancels.`;
      say(`Ready. ${capture.combo?"Press your combo":"Press the new input"} for ${name}.`);
      return;
    }
    const inputs=captureInputs(pad);
    if(!capture.combo){if(inputs.length) commitCapture(inputs[0]);return;}
    for(const input of inputs){
      if(!capture.inputs.some(item=>item.type===input.type && item.index===input.index && item.direction===input.direction)) capture.inputs.push(input);
      if(!capture.firstInputAt) capture.firstInputAt=now;
      if(capture.inputs.length===2) break;
    }
    if(capture.inputs.length===2){commitCapture({type:"chord",inputs:capture.inputs});return;}
    if(capture.inputs.length===1 && !inputs.length && now-capture.firstInputAt>80) commitCapture(capture.inputs[0]);
  }
  function frame(now){
    raf=0;
    if(!visible || disposed || document.hidden) return;
    if(now-lastTick>75){lastTick=now;updateDevices();updateTester();}
    pollCapture(now);
    raf=requestAnimationFrame(frame);
  }
  function startPolling(){if(visible && !disposed && !document.hidden && !raf) raf=requestAnimationFrame(frame);}
  function stopPolling(){if(raf) cancelAnimationFrame(raf);raf=0;}
  function onConfigChanged(){refresh();}
  function onPadsChanged(){if(visible){updateDevices(true);pollCapture(performance.now());}}
  function onVisibility(){
    if(document.hidden){cancelCapture("Input change cancelled while the page was hidden.",false);stopPolling();}
    else if(visible){refresh();startPolling();}
  }
  function onBlur(){cancelCapture("Input change cancelled when the window lost focus.",false);stopPolling();}
  function onFocus(){if(visible){refresh();startPolling();}}
  function onEscape(event){
    if(visible && capture && event.code==="Escape"){
      // Keep the held-key guard informed even though capture consumes this event.
      inputManager.onKeyDown(event);
      event.preventDefault();event.stopImmediatePropagation();cancelCapture("Input change cancelled.");
    }
  }
  function onClick(event){
    const tab=event.target.closest("[data-controller-player]");
    if(tab){cancelCapture("",false);player=tab.dataset.controllerPlayer;customAxesRequested={move:false,skill:false};lastTester="";refresh();return;}
    const use=event.target.closest("[data-use-controller]");
    if(use){
      const pad=pads().find(item=>item.index===Number(use.dataset.useController));
      if(!pad) return;
      cancelCapture("",false);
      controllerConfig.setAssignment(player,{index:pad.index,id:pad.id});
      controllerConfig.setInputMode(player,"controller");
      refresh();say(`${playerName(player)} is set to play with Controller ${pad.index+1}. Saved.`);return;
    }
    const bind=event.target.closest("[data-bind-action]");
    if(bind){startCapture(bind.dataset.bindAction,bind);return;}
    if(event.target.closest("#controllerCancelCapture")){cancelCapture("Input change cancelled.");return;}
    if(event.target.closest("#controllerReset")){
      cancelCapture("",false);customAxesRequested={move:false,skill:false};controllerConfig.resetPlayer(player);refresh();say(`${playerName(player)} controller defaults restored. Saved.`);return;
    }
    if(event.target.closest("#controllerManualApply")){
      cancelCapture("",false);
      const action=find("#controllerManualAction").value,value=find("#controllerManualInput").value.split(":");
      const binding=value[0]==="button"?{type:"button",index:Number(value[1]),threshold:.25}:value[0]==="axis"?{type:"axis",index:Number(value[1]),direction:Number(value[2]),threshold:.55}:null;
      say(saveBinding(action,binding));
    }
  }
  function onChange(event){
    const target=event.target;
    if(target===inputMode){
      const mode=target.value;
      cancelCapture("",false);
      if(mode==="controller" && config().assignment==="none") controllerConfig.setAssignment(player,"auto");
      controllerConfig.setInputMode(player,mode);
      refresh();say(`${playerName(player)} play mode saved: ${target.selectedOptions[0].textContent}.`);
    }else if(target===assignment){
      cancelCapture("",false);
      if(target.value.startsWith("pad:")){
        const pad=pads().find(item=>item.index===Number(target.value.slice(4)));
        if(pad) controllerConfig.setAssignment(player,{index:pad.index,id:pad.id});
      }else if(target.value==="auto" || target.value==="none") controllerConfig.setAssignment(player,target.value);
      refresh();say(`${playerName(player)} controller assignment saved.`);
    }else if(target.dataset.stickPreset){
      const kind=target.dataset.stickPreset,preset=target.value;
      cancelCapture("",false);
      customAxesRequested[kind]=preset==="custom";
      if(preset==="custom"){find(`[data-custom-axes="${kind}"]`).hidden=false;return;}
      const start=preset==="right"?2:preset==="off"?-1:0;
      if(controllerConfig.setAxes(player,kind,{x:start,y:start<0?-1:start+1})) say(`${kind==="move"?"Movement":"Skill"} stick saved.`);
    }else if(target.dataset.axisKind){
      cancelCapture("",false);
      if(!controllerConfig.setAxes(player,target.dataset.axisKind,{[target.dataset.axisComponent]:Number(target.value)})){
        refreshValues();say("Choose different horizontal and vertical axes, or disable one axis first.");
      }
    }else if(target.dataset.invertKind){
      controllerConfig.setAxes(player,target.dataset.invertKind,{[target.dataset.invertComponent]:target.dataset.invertComponent==="invertY"?!target.checked:target.checked});
    }
  }
  function onInput(event){
    if(event.target.id==="controllerDeadzone") controllerConfig.setDeadzone(player,Number(event.target.value));
  }
  function onTabKey(event){
    const tab=event.target.closest("[data-controller-player]");
    if(!tab || !["ArrowLeft","ArrowRight","Home","End"].includes(event.code)) return;
    event.preventDefault();
    const next=event.code==="Home"?"p1":event.code==="End"?"p2":tab.dataset.controllerPlayer==="p1"?"p2":"p1";
    const button=find(`[data-controller-player="${next}"]`);button.click();button.focus();
  }
  host.addEventListener("click",onClick);host.addEventListener("change",onChange);host.addEventListener("input",onInput);host.addEventListener("keydown",onTabKey);
  window.addEventListener("keydown",onEscape,true);
  window.addEventListener("basketball-controller-config",onConfigChanged);
  window.addEventListener("basketball-gamepads-changed",onPadsChanged);
  window.addEventListener("blur",onBlur);window.addEventListener("focus",onFocus);
  document.addEventListener("visibilitychange",onVisibility);
  const api={
    setVisible(value){
      if(disposed || visible===!!value) return;
      visible=!!value;host.hidden=!visible;
      inputManager.setConfigurationMode(visible);
      if(visible){refresh();startPolling();}else{cancelCapture("",false);stopPolling();}
    },
    refresh,
    destroy(){
      if(disposed) return;
      api.setVisible(false);disposed=true;
      host.removeEventListener("click",onClick);host.removeEventListener("change",onChange);host.removeEventListener("input",onInput);host.removeEventListener("keydown",onTabKey);
      window.removeEventListener("keydown",onEscape,true);window.removeEventListener("basketball-controller-config",onConfigChanged);window.removeEventListener("basketball-gamepads-changed",onPadsChanged);window.removeEventListener("blur",onBlur);window.removeEventListener("focus",onFocus);document.removeEventListener("visibilitychange",onVisibility);
      delete host.basketballControllerSettings;host.replaceChildren();
    }
  };
  host.basketballControllerSettings=api;
  return api;
}
