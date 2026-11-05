import {legalActions, skillReadyAt, unitDef} from './engine.mjs';

const SIDES = new Set(['player','ai']);
const MODES = new Set(['auto','move','attack','ability','guard']);
const alive = (state,id) => state?.units?.find(unit => unit.id===id && unit.hp>0);
const skillNumber = action => action.type==='ability' ? (action.skillIndex??0) : 0;
const sameAction = (action,requested) => action.type===requested.type && action.unitId===requested.unitId && action.x===requested.x && action.y===requested.y &&
  (requested.targetId===undefined || requested.targetId===action.targetId) && skillNumber(action)===skillNumber(requested);
const stamp = unit => ({id:unit.id,side:unit.side,faction:unit.faction,x:unit.x,y:unit.y});
const sameStamp = (unit,snapshot) => unit && unit.id===snapshot.id && unit.side===snapshot.side && unit.faction===snapshot.faction && unit.x===snapshot.x && unit.y===snapshot.y;

/** Show real legal geometry even while its command is recovering. No battle state is changed. */
export function previewActions(state,unitId) {
  const unit=alive(state,unitId);
  if(!unit || state.winner)return [];
  const copy={...unit,readyAt:state.time,abilityReadyAt:state.time,abilityReadyAts:unitDef(unit).abilities.map(()=>state.time)};
  const preview={...state,units:state.units.map(value=>value===unit?copy:value)};
  return legalActions(preview,unitId);
}

class InputController {
  constructor({bufferSeconds=.55,dragThreshold=8}={}) {
    this.bufferSeconds=Number.isFinite(bufferSeconds)?Math.max(0,Math.min(1,bufferSeconds)):.55;
    this.dragThreshold=Number.isFinite(dragThreshold)?Math.max(2,Math.min(30,dragThreshold)):8;
    this.intents=new Map();
    this.pointers=new Map();
  }

  /** A fresh valid order replaces that troop's previous buffered order. */
  offer(state,requested,{side='player',paused=false,countdown=false}={}) {
    if(paused||countdown||state?.winner){this.clear();return {status:'rejected',reason:paused?'paused':countdown?'countdown':'complete'};}
    if(!requested||typeof requested!=='object'||!SIDES.has(side))return {status:'rejected',reason:'invalid'};
    const unit=alive(state,requested.unitId);
    if(!unit||unit.side!==side||(side==='ai'&&state.opponentMode!=='local'))return {status:'rejected',reason:'ownership'};
    const action=previewActions(state,unit.id).find(candidate=>sameAction(candidate,requested));
    if(!action)return {status:'rejected',reason:'target'};
    const readyAt=action.type==='ability'?skillReadyAt(unit,skillNumber(action)):unit.readyAt;
    if(readyAt<=state.time){this.intents.delete(unit.id);return {status:'ready',action:{...action}};}
    if(readyAt-state.time>this.bufferSeconds+1e-7)return {status:'rejected',reason:'recovering',readyAt};
    const target=action.targetId&&alive(state,action.targetId);
    const intent={action:{...action},side,source:stamp(unit),target:target?stamp(target):null,readyAt,expiresAt:state.time+this.bufferSeconds};
    this.intents.set(unit.id,intent);
    return {status:'queued',action:{...action},readyAt,expiresAt:intent.expiresAt};
  }

  /** Return each ready order once. Movement, death, changed teams, and stale targets cancel it. */
  drain(state,{paused=false,countdown=false}={}) {
    if(paused||countdown||state?.winner){this.clear();return [];}
    const result=[];
    for(const [id,intent] of this.intents){
      const unit=alive(state,id),target=intent.target&&alive(state,intent.target.id);
      if(state.time>intent.expiresAt+1e-7 || !sameStamp(unit,intent.source) || unit.side!==intent.side || (intent.side==='ai'&&state.opponentMode!=='local') ||
        (intent.target&&!sameStamp(target,intent.target))){this.intents.delete(id);continue;}
      const candidate=previewActions(state,id).find(action=>sameAction(action,intent.action));
      if(!candidate){this.intents.delete(id);continue;}
      const ready=legalActions(state,id).find(action=>sameAction(action,intent.action));
      if(ready){this.intents.delete(id);result.push({...ready});}
    }
    return result;
  }

  pending() {return [...this.intents.values()].map(({action,side,readyAt,expiresAt})=>({action:{...action},side,readyAt,expiresAt}));}
  clear(unitId) {if(unitId===undefined)this.intents.clear();else this.intents.delete(unitId);}

  /** Capture a gesture's owner and command once, independently of later selection changes. */
  beginPointer(pointerId,{unitId,side,mode='auto',skillIndex=0,x,y}={}) {
    if(!Number.isInteger(pointerId)||typeof unitId!=='string'||!unitId||!SIDES.has(side)||!Number.isFinite(x)||!Number.isFinite(y))return null;
    const pointer={pointerId,unitId,side,mode:MODES.has(mode)?mode:'auto',skillIndex:Number.isInteger(skillIndex)&&skillIndex>=0?skillIndex:0,x,y,currentX:x,currentY:y,dragging:false};
    this.pointers.set(pointerId,pointer);
    return {...pointer};
  }
  movePointer(pointerId,{x,y}={}) {
    const pointer=this.pointers.get(pointerId);
    if(!pointer||!Number.isFinite(x)||!Number.isFinite(y))return null;
    pointer.currentX=x;pointer.currentY=y;
    pointer.dragging ||= Math.hypot(x-pointer.x,y-pointer.y)>=this.dragThreshold;
    return {...pointer};
  }
  endPointer(pointerId) {const pointer=this.pointers.get(pointerId);this.pointers.delete(pointerId);return pointer?{...pointer}:null;}
  cancelPointer(pointerId) {this.pointers.delete(pointerId);}
  clearPointers() {this.pointers.clear();}
  reset() {this.clear();this.clearPointers();}
}

export function createInputController(options) {return new InputController(options);}
