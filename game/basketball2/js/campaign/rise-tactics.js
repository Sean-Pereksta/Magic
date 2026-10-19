/* Destinations and decisions express roster chemistry; makes receive no team bonus. */
function risePreferredSpot(char,brain,plan,handler){
  if(typeof aiPlayerTendencies!=='function') return false;
  const r=char.profile.ratings||{},tendencies=aiPlayerTendencies(char);
  const action=aiPlanAction(plan),side=char.group.position.x<0?-1:1;
  if(['screen','screen_setup','cut','roll','post','break'].includes(brain.role)&&!handler) return false;
  const mid=tendencies.midrange>.3&&tendencies.midrange>tendencies.perimeter;
  const deep=tendencies.perimeter>.38&&!mid;
  if(mid&&(action==='motion'||action==='give_go'||action==='isolation')){
    brain.target.set(side*3,0,HOOP_POS.z+4.8);
    if(!handler) brain.role='elbow';
    return true;
  }
  if(deep&&!handler&&['space','fill','outlet','clear','pop'].includes(brain.role)){
    const corner=char.profile.abilities?.cornerSpecialist;
    // Keep an already empty perimeter window. Running away from a pending
    // human pass would discard the very catch-and-shoot look being created.
    const radius=flatDistance(char.group.position,HOOP_POS);
    const open=aiNearestDistance(char)>2.4;
    if(radius>7.25&&radius<8.15&&open) brain.target.copy(char.group.position);
    else brain.target.set(side*(corner?6.9:5.7),0,HOOP_POS.z+(corner?2.4:5));
    brain.role='shooting_outlet';return true;
  }
  if(tendencies.drive>.5&&!handler&&action==='fast_break'){
    brain.target.set(side*1.4,0,HOOP_POS.z+2.2);brain.role='roll';return true;
  }
  if(tendencies.post>.5&&!handler&&['space','fill','outlet'].includes(brain.role)&&!['pick_pop','give_go'].includes(action)){
    const rimActor=aiRoster().some(other=>other!==char&&other.teamId===char.teamId&&
      ['cut','roll','post','break'].includes(other.aiBrain?.role));
    if(!rimActor){brain.target.set(side*2.2,0,HOOP_POS.z+2.6);brain.role='post';return true;}
  }
  return false;
}
