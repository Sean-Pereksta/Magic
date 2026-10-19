/* Court-side development rules. These change launch vectors, reach and movement;
 * the ball/hoop collision solver remains the only judge of a jump shot. */
const BasketballDevelopment=(()=>{
  const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));
  const aliases={closeShot:['close','closeShooting'],midrange:['midRange'],threePoint:['three','threePointShot'],layups:['layup'],dunking:['dunk'],jumping:['jump','vertical'],steals:['steal'],blocking:['block'],basketballIQ:['iq'],passingAccuracy:['passAccuracy']};
  function rating(char,key,fallback=60){
    const ratings=char?.profile?.ratings || char?.ratings || {};
    const value=ratings[key] ?? (aliases[key]||[]).map(id=>ratings[id]).find(v=>v!=null);
    return Number.isFinite(Number(value))?clamp(Number(value),20,99):fallback;
  }
  function ability(char,id){
    const abilities=char?.profile?.abilities || char?.abilities || [];
    const value=Array.isArray(abilities)?abilities.find(a=>(typeof a==='string'?a:a.id)===id):abilities[id];
    if(typeof value==='string'||value===true) return 1;
    return clamp(Number(typeof value==='object'?value?.tier ?? value?.level ?? 1:value)||0,0,3);
  }
  function legacyRating(char,key='shot'){
    return clamp(60+((Number(char?.profile?.[key])||1)-1)*90,25,95);
  }
  function shotProfile(char,{distance=5.5,type='setshot',contest=0,timingError=0,halfWindow=.09,now=0,clutch=false}={}){
    const close=['layup','runner','floater','hook'].includes(type)||distance<3.6;
    const three=distance>7.15;
    const r=rating(char,close?(type==='layup'?'layups':'closeShot'):three?'threePoint':'midrange',legacyRating(char,close?'layup':'shot'));
    const stability=rating(char,'releaseStability',legacyRating(char));
    const consistency=rating(char,'consistency',60);
    const composure=rating(char,'composure',60);
    const speed=char.velocity?.length?.()||0;
    const fatigue=(1-clamp(char.stamina??1,0,1))*(1.24-rating(char,'stamina',60)/180);
    const recentCatch=Number.isFinite(char.lastCatchTime)&&now-char.lastCatchTime>=0&&now-char.lastCatchTime<1.35;
    const catchQuality=recentCatch?clamp(char.lastPassQuality??.75,0,1):0;
    const corner=three&&Math.abs(char.group?.position?.x||0)>5.5&&Math.abs((char.group?.position?.z||0)+10.8)<3.8;
    const active=[];
    const use=(id,condition)=>{const tier=condition?ability(char,id):0;if(tier)active.push(id);return tier;};
    const mid=use('midrangeAssassin',!close&&!three);
    const deadeye=use('deadeye',contest>.12&&contest<.85);
    const catchShoot=use('catchAndShoot',recentCatch&&speed<2.1&&catchQuality>.45&&!close);
    const pullup=use('pullUpSpecialist',(type==='pullup'||(char.moveMeta?.approachSpeed||0)>2.4)&&speed<3.6&&!close);
    const stepback=use('stepBackCreator',type==='stepback'&&(char.stepBackQuality||0)>.55);
    const corners=use('cornerSpecialist',corner&&recentCatch&&speed<2.1);
    const deep=use('deepRange',three);
    const clutchTier=use('clutchShooter',clutch);
    const quick=use('quickRelease',!close);
    const contestSkill=rating(char,'contestedShooting',r)*.006;
    const pressure=contest*(1-clamp(contestSkill*.23+deadeye*.13,0,.42));
    const movement=Math.max(0,speed-(pullup?.95:0))*(stepback?.48:1);
    const contact=char.contactTimer>.02?Math.max(0,.08-(rating(char,'contactFinishing',r)-50)*.001):0;
    const control=rating(char,'aerialControl',r);
    const offBalance=(type==='fadeaway'?.085:type==='hook'?.035:type==='stepback'?.045:0)*(1-stepback*.2)+(char.stumbledTimer>0?.2:0);
    // Gaussian aim dispersion in court metres. These values are calibrated by
    // stepping the real rim, backboard and swept scoring collisions in tests.
    let spread=(close?.115:.375)-(r-60)*(close?.0009:.001);
    spread+=(60-stability)*.0003+(60-consistency)*.0002;
    spread-=mid*.012+catchShoot*.014*catchQuality+pullup*.012+stepback*.013+corners*.018;
    spread+=pressure*.32+Math.max(0,pressure-.6)*.55+movement*.031+fatigue*.13+offBalance+contact;
    spread+=Math.max(0,distance-(three?7.15+deep*.7:6.5))*(three?.058:.012);
    if(close)spread+=(60-control)*.0006;
    const timingTolerance=1+(stability-60)*.004+(composure-60)*.002+clutchTier*.13+(clutch?(rating(char,'clutchShooting',r)-60)*.003:0);
    const error=timingError/Math.max(.65,timingTolerance);
    const outside=Math.max(0,Math.abs(error)-halfWindow);
    const timingBias=-Math.sign(error)*(Math.min(Math.abs(error),halfWindow)*.23+outside*(1.1+Math.max(0,outside-.035)*24))*(close?.45:1);
    spread+=outside*.24;
    return {rating:r,spread:clamp(spread,close?.035:.135,.95),timingBias,pressure,
      movementBias:movement*.012,depthBias:-pressure*.17-fatigue*.055,
      active,quickRelease:quick>0,releaseDuration:quick?Math.max(.28,.46-.055*quick):.46};
  }
  function meterProfile(type,char,contest,base){
    const quick=ability(char,'quickRelease');
    const stable=rating(char,'releaseStability',legacyRating(char));
    const composure=rating(char,'composure',60);
    const center=(base.greenStart+base.greenEnd)*.5;
    const half=clamp((base.greenEnd-base.greenStart)*.5+(stable-60)*.00055+(composure-60)*.0002-quick*.007,.035,.145);
    return {...base,greenStart:center-half,greenEnd:center+half,meterFillSpeed:base.meterFillSpeed*(1+quick*.13)};
  }
  function dunkProfile(char,{momentum=0,angle=1,defender=null}={}){
    const dunk=rating(char,'dunking',legacyRating(char,'layup'));
    const jump=rating(char,'jumping',legacyRating(char,'jump'));
    const strength=rating(char,'strength',legacyRating(char,'width'));
    const standing=rating(char,'standingDunk',dunk-8);
    const aerial=rating(char,'aerialControl',dunk);
    const contact=rating(char,'contactFinishing',dunk);
    const power=ability(char,'powerFinisher'),flyer=ability(char,'highFlyer'),specialist=ability(char,'contactSpecialist'),acrobat=ability(char,'aerialAcrobat'),poster=ability(char,'posterizer');
    const height=char.profile?.height||1, width=char.profile?.width||1;
    const fatigue=clamp(char.stamina??1,0,1);
    const launch=clamp(momentum/5,0,1);
    const qualified=height>1.08?dunk>=53&&jump>=44:dunk>=64&&jump>=64;
    const standingQualified=height>1.04&&standing>=75&&strength>=65;
    const reach=height*2.22+(.94+(jump-60)*.007+flyer*.085)*(.7+.3*fatigue);
    const range=clamp(1.12+(height-1)*.9+(dunk-60)*.009+launch*.27+flyer*.09,.8,2.15);
    const canLaunch=qualified&&reach>=3.12&&fatigue>.2&&angle>(acrobat?.05:.32)&&(momentum>.65||standingQualified);
    let style=standingQualified&&momentum<1?'standing':width>1.09||power?'twohand':flyer?'tomahawk':'quick';
    if(acrobat&&angle<.65&&aerial>=70)style='reverse';
    else if(acrobat&&aerial>=80&&launch>.5)style='windmill';
    else if(poster&&defender&&dunk>=82&&jump>=78&&strength>=72&&launch>.55&&angle>.72)style='poster';
    else if(specialist&&defender&&contact>=70)style='contact';
    const resistance=clamp(.34+(contact-60)*.006+(strength-60)*.004+power*.045*launch+specialist*.065+poster*.035, .18,.82);
    return {canLaunch,range,reach,style,power,flyer,specialist,acrobat,poster,resistance,momentum,angle,
      force:clamp(.85+(strength-60)*.007+power*.12+launch*.17,.65,1.65),
      control:clamp(.5+(aerial-60)*.008+acrobat*.1,.25,.94),
      finish:clamp((dunk*.4+contact*.24+strength*.17+jump*.19)/100+.1*launch-.16*(1-fatigue),.3,.98)};
  }
  function canDunk(char,hoop,defender){
    if(!char?.hasBall||!char.group) return false;
    const dx=hoop.x-char.group.position.x,dz=hoop.z-char.group.position.z,dist=Math.hypot(dx,dz);
    const vx=char.velocity?.x||0,vz=char.velocity?.z||0;
    const angle=dist>.001?(Math.sin(char.angle||0)*dx+Math.cos(char.angle||0)*dz)/dist:1;
    const momentum=dist>.001?(vx*dx+vz*dz)/dist:0;
    const profile=dunkProfile(char,{momentum,angle,defender});
    // Eligibility comes from the attacker's actual reach and gather. A defender
    // can block or disrupt the attempted finish rather than veto its animation.
    return dist<=profile.range&&profile.canLaunch;
  }
  function sampleAim(spread,random=Math.random){
    const radius=Math.sqrt(-2*Math.log(Math.max(.000001,random())))*spread;
    const theta=random()*Math.PI*2;
    return {side:Math.cos(theta)*radius,depth:Math.sin(theta)*radius};
  }
  return Object.freeze({rating,ability,legacyRating,shotProfile,meterProfile,dunkProfile,canDunk,sampleAim});
})();
if(typeof module!=='undefined'&&module.exports)module.exports=BasketballDevelopment;
