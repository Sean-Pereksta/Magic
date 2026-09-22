// A momentary pursuit-planner gap must not make a receiver forget a ball he just tracked.
function receiverCatchAware(a){return a.trackingBall||throwTime<(a.catchTrackingUntil||0);}
function receiverCatchControl(a,speed,twoHands,contested=false){
  const rating=Math.min(1,P.effective(a.profile.catching)/100),athletic=Math.min(1,P.effective(a.profile.athleticism)/100);
  const signature=a.signature==='Sure Hands'?.025:0;
  return THREE.MathUtils.clamp(.90+rating*.085+athletic*.020+(twoHands?.025:0)+spiralQuality*.015+signature
    -V.weather(matchWeather().name).hands*.40-Math.max(0,speed-38)*.0015
    -(a.stagger>0?.035:0)-(a.comebackActive?(a.underthrowDifficulty||0)*.08:0)
    +(throwBobbles>0?.025:0)-(contested?.12+currentSkill()*.06:0),.55,contested?.94:.995);
}
function tryReceiverGather(hit,aware,contested){
  const a=hit.actor;
  if(!a.profile||!aware||contested||pendingCatch||a.gatherUsed)return false;
  const index=a.mesh.userData.forearms.indexOf(hit.limb.mesh);
  if(index<0)return false;
  const hand=a.mesh.userData.hands[index].getWorldPosition(new THREE.Vector3());
  // Only a real wrist-side forearm impact qualifies, not the chest, upper arm or nearby air.
  const scale=a.mesh.getWorldScale(new THREE.Vector3());
  if(hit.point.distanceTo(hand)>.30*Math.max(scale.x,scale.y,scale.z))return false;
  const relative=ballVel.clone().sub(a.velocity),inward=relative.dot(hit.normal);
  if(inward<0)relative.addScaledVector(hit.normal,-inward*.90);
  ballVel.copy(relative.multiplyScalar(.55)).add(a.velocity);
  // One brief cushion per receiver per throw. No attraction or possession is awarded here.
  a.gatherUsed=true;a.gatherUntil=throwTime+.075;a.catchTrackingUntil=Math.max(a.catchTrackingUntil||0,throwTime+.16);a.catchPose=1;
  ballPrev.copy(ball.position);return true;
}
function deflectBall(hit,contested=false){
  const soft=!!hit.actor.profile&&hit.limb.hand&&!contested&&receiverCatchAware(hit.actor);
  // Preserve moving-glove impulses, but bound the animation's extra kick on attempted catches.
  const surface=soft?hit.actor.velocity.clone().add(hit.velocity.clone().sub(hit.actor.velocity).clampLength(0,3)):hit.velocity;
  const relative=ballVel.clone().sub(surface),normalSpeed=relative.dot(hit.normal);
  const restitution=soft?.12:hit.limb.hand?.30:.46;
  if(normalSpeed<0)relative.addScaledVector(hit.normal,-(1+restitution)*normalSpeed);
  relative.multiplyScalar(soft?.48:hit.limb.hand?.64:.82);
  ballVel.copy(relative).add(surface.clone().clampLength(0,12));
  ballVel.clampLength(.6,48);ball.position.addScaledVector(hit.normal,.006);
  ballTumble=Math.min(soft?8:22,4+Math.abs(normalSpeed)*.32);spiralQuality=Math.min(spiralQuality,soft?.70:.35);
  hit.actor.contactLock=true;pendingCatch=null;throwBobbles++;receiverDrop=!!hit.actor.profile;
  if(playLog&&hit.actor.profile&&hit.limb.hand&&!contested)playLog.dropped=hit.actor.profile.id;
  ballPrev.copy(ball.position);flashResult(contested?'CONTESTED TIP':hit.actor.profile?'BOBBLE!':'PASS TIPPED',false,450);
}
function checkBallContact(){
  if(!ballLive)return false;
  const contacts=[],holder=pendingCatch?.actor;
  if(holder&&holder.profile&&!pendingCatch.twoHands){
    // A second glove may arrive on a later physics step; it must actually touch the ball.
    const second=playerBallContacts(holder).some(c=>c.limb.hand&&c.limb!==pendingCatch.limb);
    if(second){pendingCatch.twoHands=true;pendingCatch.remaining=Math.min(pendingCatch.remaining,.020);holder.catchOneHand=false;}
  }
  for(const actor of [...receivers,...defenders]){
    if(actor===holder||Math.abs(actor.mesh.position.x)>25.6)continue;
    // Teammates cannot steal or knock out a ball already being secured in a receiver's hands.
    if(holder?.profile&&actor.profile)continue;
    contacts.push(...playerBallContacts(actor));
  }
  contacts.sort((a,b)=>a.time-b.time);
  const hit=contacts[0];if(!hit)return false;
  const actor=hit.actor,isDefense=!actor.profile;
  // A held ball stays on its glove when a receiver wins a real, once-per-contact contest.
  if(holder?.profile){
    const control=receiverCatchControl(holder,ballVel.clone().sub(hit.velocity).length(),pendingCatch.twoHands,true);
    if(Math.random()<control){actor.contactLock=true;return true;}
    ball.position.lerpVectors(ballPrev,ball.position,hit.time);
    deflectBall(hit,true);holder.contactLock=true;holder.placement=null;return true;
  }
  ball.position.lerpVectors(ballPrev,ball.position,hit.time);
  // Defense-first contact still wins chronological priority; no late receiver is promoted ahead of it.
  const opponent=contacts.find(c=>!!c.actor.profile!==!!actor.profile&&(c.time-hit.time)*contactStep<.006);
  const aware=isDefense?actor.ballSeen:receiverCatchAware(actor);
  if(tryReceiverGather(hit,aware,!!opponent))return true;
  const rating=isDefense?.52+currentSkill()*.23+(actor.hands||0):Math.min(1,P.effective(actor.profile.catching)/100);
  const athletic=isDefense?currentSkill():Math.min(1,P.effective(actor.profile.athleticism)/100);
  const handWindow=isDefense?.006:.040;
  const speed=ballVel.clone().sub(hit.velocity).length(),twoHands=contacts.some(c=>c.actor===actor&&c.limb.hand&&c.limb!==hit.limb&&(c.time-hit.time)*contactStep<handWindow);
  const signature=actor.signature==='Sure Hands'?.025:0;
  // Defensive interception odds are intentionally unchanged; forgiveness belongs to receivers.
  const control=isDefense?THREE.MathUtils.clamp(.64-V.weather(matchWeather().name).hands+signature+rating*.30+athletic*.07+(twoHands?.09:0)+spiralQuality*.04-Math.max(0,speed-24)*.006-(actor.stagger>0?.14:0)-(actor.comebackActive?(actor.underthrowDifficulty||0)*.3:0),.18,.99)
    :receiverCatchControl(actor,speed,twoHands,!!opponent);
  if(hit.limb.hand&&aware&&(!opponent||!isDefense)&&!pendingCatch&&Math.random()<control){
    if(!isDefense){actor.placement=catchPlacement(actor);actor.catchOneHand=!twoHands;}
    actor.mesh.updateMatrixWorld(true);
    pendingCatch={actor,isDefense,limb:hit.limb,offset:hit.limb.mesh.worldToLocal(ball.position.clone()),twoHands,remaining:isDefense?(twoHands?.045:.075):(twoHands?.020:.040)};
    if(opponent)opponent.actor.contactLock=true;
    ballVel.copy(actor.velocity);actor.catchPose=1;
  }else{
    deflectBall(hit,!!opponent||!!holder);
    if(holder){holder.contactLock=true;holder.placement=null;}
    if(opponent){opponent.actor.contactLock=true;opponent.actor.placement=null;}
  }
  return true;
}
