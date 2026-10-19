/* Destinations and decisions express roster chemistry; makes receive no team bonus. */
function risePreferredSpot(char,brain,plan,handler){
  if(typeof riseMatch==='undefined'||!riseMatch||!char.profile.ratings) return false;
  const r=char.profile.ratings,role=char.profile.role;
  const action=aiPlanAction(plan),side=char.group.position.x<0?-1:1;
  if(['screen','screen_setup','cut','roll','post','break'].includes(brain.role)&&!handler) return false;
  const mid=r.midrange>r.threePoint+8&&r.midrange>r.dunking;
  const deep=r.threePoint>=r.midrange&&r.threePoint>60;
  if(mid&&(action==='motion'||action==='give_go'||action==='isolation')){
    brain.target.set(side*3,0,HOOP_POS.z+4.8);
    if(!handler) brain.role='elbow';
    return true;
  }
  if(deep&&!handler&&['space','fill','outlet','clear','pop'].includes(brain.role)){
    brain.target.set(side*(char.profile.abilities?.cornerSpecialist?6.8:5.6),0,HOOP_POS.z+(char.profile.abilities?.cornerSpecialist?2.8:6.1));
    brain.role='shooting_outlet';return true;
  }
  if(role==='slasher'&&!handler&&action==='fast_break'){
    brain.target.set(side*1.4,0,HOOP_POS.z+2.2);brain.role='roll';return true;
  }
  if(role==='shooter'&&!handler&&brain.role==='space'){
    brain.target.set(side*5.8,0,HOOP_POS.z+5.8);brain.role='shooting_outlet';return true;
  }
  return false;
}
