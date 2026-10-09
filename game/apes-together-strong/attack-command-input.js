/* Frame-driven E gestures: tap to target, double tap to fan out, hold to hunt. */
(() => {
'use strict';
const HOLD_MS=280,DOUBLE_TAP_MS=240;
// Store the initiating aim rather than reading a moving pointer on a later frame.
function snapshot(value){
 if(Array.isArray(value))return value.map(snapshot);
 if(value&&typeof value==='object'){const copy={};for(const key of Object.keys(value))copy[key]=snapshot(value[key]);return copy}
 return value;
}
class AttackCommandInput{
 constructor(commit){this.commit=commit;this.active=null;this.pending=null}
 press(now,aim,repeat=false){
  if(repeat||this.active||!Number.isFinite(now))return false;
  this.expireTap(now);
  // A second press reserves the first tap until this press releases or holds.
  this.active={started:now,aim:snapshot(aim),second:!!this.pending,held:false};
  return true;
 }
 release(now){
  const g=this.active;if(!g||!Number.isFinite(now))return false;
  this.active=null;
  if(g.held)return true;
  if(now-g.started>=HOLD_MS){this.pending=null;this.commit('nearestHuman',g.aim)}
  else if(g.second){this.pending=null;this.commit('spreadCharge',g.aim)}
  else this.pending={aim:g.aim,deadline:now+DOUBLE_TAP_MS};
  return true;
 }
 tick(now){
  if(!Number.isFinite(now))return;
  const g=this.active;
  if(g&&!g.held&&now-g.started>=HOLD_MS){g.held=true;this.pending=null;this.commit('nearestHuman',g.aim)}
  this.expireTap(now);
 }
 expireTap(now){
  // The second press must begin before the deadline. Expiring at >= in both
  // press and tick makes the exact boundary independent of event/frame order.
  if(this.pending&&!this.active?.second&&now>=this.pending.deadline){const pending=this.pending;this.pending=null;this.commit('nearestTarget',pending.aim)}
 }
 cancel(){this.active=null;this.pending=null}
}
AttackCommandInput.holdMs=HOLD_MS;AttackCommandInput.doubleTapMs=DOUBLE_TAP_MS;
window.ATSAttackCommandInput=AttackCommandInput;
window.ATSAttackCommands=Object.freeze({holdMs:HOLD_MS,doubleTapMs:DOUBLE_TAP_MS});
})();
