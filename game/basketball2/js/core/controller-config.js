// The browser pairs devices; these profiles only choose and map connected pads.
const CONTROLLER_SETTINGS_KEY = "basketball2.controllerSettings.v1";
const CONTROLLER_ACTIONS = ["sprint","shoot","pass","switchPlayer","block","steal","leftSkill","rightSkill","takeover","pause","protect"];
const CONTROLLER_BUTTON_NAMES = ["A","B","X","Y","LB","RB","LT","RT","View","Menu","LS","RS","D-pad up","D-pad down","D-pad left","D-pad right","Xbox"];

function defaultControllerProfile(){
  return {
    assignment:"auto",deadzone:.16,
    axes:{move:{x:0,y:1,invertX:false,invertY:true},skill:{x:2,y:3,invertX:false,invertY:true}},
    bindings:{
      sprint:{type:"button",index:7,threshold:.25},shoot:{type:"button",index:2,threshold:.25},
      pass:{type:"button",index:0,threshold:.25},switchPlayer:{type:"button",index:5,threshold:.25,context:"team"},
      block:{type:"button",index:3,threshold:.25},steal:{type:"button",index:1,threshold:.25},
      leftSkill:{type:"button",index:4,threshold:.25},rightSkill:{type:"button",index:5,threshold:.25,context:"single"},
      takeover:{type:"chord",buttons:[10,11]},pause:{type:"button",index:9,threshold:.25},
      protect:{type:"button",index:6,threshold:.25}
    }
  };
}

function controllerCopy(value){ return JSON.parse(JSON.stringify(value)); }
function validControllerIndex(value,max){ return Number.isInteger(value) && value>=0 && value<=max; }
function validControllerAxis(value){ return value===-1 || validControllerIndex(value,31); }
function validateControllerBinding(value,allowChord=true){
  if(value===null) return null;
  if(!value || typeof value!=="object") return undefined;
  const context = value.context==="team" || value.context==="single" ? {context:value.context} : {};
  if(value.type==="button" && validControllerIndex(value.index,63)){
    return {type:"button",index:value.index,threshold:Number.isFinite(value.threshold)?Math.max(.1,Math.min(.95,value.threshold)):.25,...context};
  }
  if(value.type==="axis" && validControllerIndex(value.index,31) && [1,-1].includes(value.direction)){
    return {type:"axis",index:value.index,direction:value.direction,threshold:Number.isFinite(value.threshold)?Math.max(.2,Math.min(.95,value.threshold)):.55,...context};
  }
  if(value.type==="chord" && allowChord){
    if(Array.isArray(value.buttons) && value.buttons.length>=2 && value.buttons.length<=4 && value.buttons.every(index=>validControllerIndex(index,63))){
      const buttons=[...new Set(value.buttons)];
      if(buttons.length>=2) return {type:"chord",buttons,...context};
    }
    if(Array.isArray(value.inputs) && value.inputs.length>=2 && value.inputs.length<=4){
      const inputs=value.inputs.map(input=>validateControllerBinding(input,false));
      if(inputs.every(Boolean) && new Set(inputs.map(input=>`${input.type}:${input.index}:${input.direction || 0}`)).size===inputs.length) return {type:"chord",inputs,...context};
    }
  }
  return undefined;
}

function validateControllerAssignment(value){
  if(value==="auto" || value==="none") return value;
  if(value && typeof value==="object" && validControllerIndex(value.index,31) && typeof value.id==="string" && value.id.length<=512){
    return {index:value.index,id:value.id};
  }
  return undefined;
}

function controllerBindingsConflict(a,b){
  if(!a || !b) return false;
  if(a.context && b.context && a.context!==b.context) return false;
  const components=binding=>(binding.type==="chord" ? binding.inputs || binding.buttons?.map(index=>({type:"button",index})) || [] : [binding]).map(input=>`${input.type}:${input.index}:${input.direction || 0}`);
  const left=components(a),right=components(b);
  // A chord must not also fire a one-button action while it is being entered.
  // Different chords may still share a modifier, such as LT + X / LT + A.
  return left.every(input=>right.includes(input)) || right.every(input=>left.includes(input));
}

function sanitizeControllerBindings(bindings,priority=CONTROLLER_ACTIONS){
  const used=[];
  for(const action of priority){
    const binding=bindings[action];
    if(!binding) continue;
    if(used.some(other=>controllerBindingsConflict(binding,other))) bindings[action]=null;
    else used.push(binding);
  }
}

class BasketballControllerConfiguration {
  constructor(){
    this.players={p1:defaultControllerProfile(),p2:defaultControllerProfile()};
    try{
      const saved=JSON.parse(localStorage.getItem(CONTROLLER_SETTINGS_KEY));
      if(saved?.version===1){
        for(const slot of ["p1","p2"]){
          const value=saved.players?.[slot];
          if(!value || typeof value!=="object") continue;
          const profile=this.players[slot];
          const assignment=validateControllerAssignment(value.assignment);
          if(assignment!==undefined) profile.assignment=assignment;
          if(Number.isFinite(value.deadzone)) profile.deadzone=Math.max(.05,Math.min(.5,value.deadzone));
          for(const type of ["move","skill"]){
            const axes=value.axes?.[type];
            if(axes && validControllerAxis(axes.x) && validControllerAxis(axes.y) && (axes.x!==axes.y || axes.x===-1)){
              profile.axes[type]={x:axes.x,y:axes.y,invertX:axes.invertX===true,invertY:axes.invertY===true};
            }
          }
          for(const action of CONTROLLER_ACTIONS){
            if(!Object.hasOwn(value.bindings || {},action)) continue;
            const binding=validateControllerBinding(value.bindings[action]);
            if(binding!==undefined) profile.bindings[action]=binding;
          }
          sanitizeControllerBindings(profile.bindings);
        }
        this.preventDuplicateAssignments("p1");
      }
    }catch(_error){ /* Private browsing and malformed saves keep safe defaults. */ }
  }

  getPlayer(slot){ return ["p1","p2"].includes(slot) ? controllerCopy(this.players[slot]) : null; }
  serialize(){ return controllerCopy({version:1,players:this.players}); }
  preventDuplicateAssignments(preferred){
    const other=preferred==="p1"?"p2":"p1";
    const a=this.players[preferred].assignment,b=this.players[other].assignment;
    if(typeof a==="object" && typeof b==="object" && a.index===b.index) this.players[other].assignment="none";
  }
  changed(slot,reason){
    try{ localStorage.setItem(CONTROLLER_SETTINGS_KEY,JSON.stringify(this.serialize())); }catch(_error){}
    window.dispatchEvent(new CustomEvent("basketball-controller-config",{detail:{slot,reason}}));
    return true;
  }
  setAssignment(slot,value){
    const assignment=validateControllerAssignment(value);
    if(!["p1","p2"].includes(slot) || assignment===undefined) return false;
    this.players[slot].assignment=assignment;
    this.preventDuplicateAssignments(slot);
    return this.changed(slot,"assignment");
  }
  setBinding(slot,action,value){
    const binding=validateControllerBinding(value);
    if(!["p1","p2"].includes(slot) || !CONTROLLER_ACTIONS.includes(action) || binding===undefined) return false;
    const bindings=this.players[slot].bindings,previous=bindings[action],displaced=[];
    for(const other of CONTROLLER_ACTIONS){
      if(other===action || !controllerBindingsConflict(binding,bindings[other])) continue;
      displaced.push(other);
      const displacedContext=bindings[other].context;
      const replacement=previous ? controllerCopy(previous) : null;
      if(replacement) delete replacement.context;
      if(replacement && displacedContext) replacement.context=displacedContext;
      bindings[other]=replacement && !controllerBindingsConflict(binding,replacement) ? replacement : null;
    }
    bindings[action]=binding;
    sanitizeControllerBindings(bindings,[action,...displaced,...CONTROLLER_ACTIONS.filter(other=>other!==action && !displaced.includes(other))]);
    return this.changed(slot,"binding");
  }
  setAxes(slot,type,value){
    if(!["p1","p2"].includes(slot) || !["move","skill"].includes(type) || !value || typeof value!=="object") return false;
    const axes={...this.players[slot].axes[type],...value};
    if(!validControllerAxis(axes.x) || !validControllerAxis(axes.y) || (axes.x===axes.y && axes.x!==-1)) return false;
    this.players[slot].axes[type]={x:axes.x,y:axes.y,invertX:axes.invertX===true,invertY:axes.invertY===true};
    return this.changed(slot,"axes");
  }
  setDeadzone(slot,value){
    if(!["p1","p2"].includes(slot) || !Number.isFinite(value)) return false;
    this.players[slot].deadzone=Math.max(.05,Math.min(.5,value));
    return this.changed(slot,"deadzone");
  }
  resetPlayer(slot){
    if(!["p1","p2"].includes(slot)) return false;
    this.players[slot]=defaultControllerProfile();
    return this.changed(slot,"reset");
  }
  labelBinding(binding){
    if(!binding) return "Unbound";
    let label="Unbound";
    if(binding.type==="button") label=CONTROLLER_BUTTON_NAMES[binding.index] || `Button ${binding.index+1}`;
    else if(binding.type==="axis") label=`Axis ${binding.index+1} ${binding.direction<0?"−":"+"}`;
    else if(binding.type==="chord") label=(binding.inputs || binding.buttons?.map(index=>({type:"button",index})) || []).map(input=>this.labelBinding(input)).join(" + ");
    if(binding.context) label+=binding.context==="team"?" · teams":" · 1v1";
    return label;
  }
}

const controllerConfig = new BasketballControllerConfiguration();
