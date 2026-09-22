/* Pure formation-audible rules shared by gameplay and regression tests. */
(function(root,factory){
  'use strict';
  const api=factory();
  if(typeof module!=='undefined'&&module.exports)module.exports=api;
  if(root)root.QBFormationAudibles=api;
})(typeof globalThis!=='undefined'?globalThis:this,function(){
  'use strict';
  const clamp=(n,a,b)=>Math.max(a,Math.min(b,Number(n)||0));
  const lerp=(a,b,t)=>a+(b-a)*t;
  const smooth=t=>{t=clamp(t,0,1);return t*t*(3-2*t)};
  const normalize=value=>String(value||'').toLowerCase().replace(/[^a-z0-9]+/g,' ').trim();

  const BASE_PLAYS=[
    {name:'Four Verticals',routes:['Go','Go','Go','Go'],category:'Deep Shots',xs:[-18,-6,6,18]},
    {name:'Mesh',routes:['Drag','Post','Corner','Drag'],category:'Intermediate',xs:[-18,-6,6,18]},
    {name:'Levels',routes:['Dig','Drag','Dig','Go'],category:'Intermediate',xs:[-18,-6,6,18]},
    {name:'Smash',routes:['Curl','Corner','Corner','Curl'],category:'Intermediate',xs:[-18,-6,6,18]},
    {name:'Dagger',routes:['Double Move','Dig','Post','Go'],category:'Deep Shots',xs:[-18,-6,6,18]},
    {name:'Drive',routes:['Dig','Drag','Post','Out'],category:'Intermediate',xs:[-18,-6,6,18]},
    {name:'Crossfire',routes:['Post','Slant','Slant','Corner'],category:'Intermediate',xs:[-18,-6,6,18]},
    {name:'Sideline',routes:['Fade','Wheel','Wheel','Fade'],category:'Deep Shots',xs:[-18,-6,6,18]}
  ];

  const GROUP_ORDER=['Recommended','Screens','Spread','Trips','Bunch / Stack','Motion','Shot Plays'];

  function hashString(value){
    let h=2166136261;
    for(const c of String(value)){h^=c.charCodeAt(0);h=Math.imul(h,16777619)}
    return h>>>0;
  }
  function seededRandom(seed){
    let a=(Number(seed)>>>0)||0x9e3779b9;
    return function(){
      a|=0;a=a+0x6D2B79F5|0;
      let t=Math.imul(a^a>>>15,1|a);t=t+Math.imul(t^t>>>7,61|t)^t;
      return ((t^t>>>14)>>>0)/4294967296;
    };
  }

  function isScreenPlay(play){
    const routes=Array.isArray(play?.routes)?play.routes:[];
    return Number.isInteger(play?.screen)||play?.family==='Screens'||play?.category==='Screens'||
      /screen|bubble|tunnel|slip/i.test(play?.name||'')||routes.some(route=>/bubble|tunnel|slip|screen choice/i.test(route));
  }

  function formationShape(play){
    const family=String(play?.family||'');
    if(family==='Bunch')return 'Bunch';
    if(family==='Stack')return 'Stack';
    if(family==='Trips')return 'Trips';
    const xs=(Array.isArray(play?.xs)&&play.xs.length===4?play.xs:[-18,-6,6,18]).map(Number);
    const sorted=[...xs].sort((a,b)=>a-b);
    const duplicate=sorted.some((x,i)=>i&&Math.abs(x-sorted[i-1])<=1.1);
    if(duplicate)return 'Stack';
    const left=xs.filter(x=>x<0).length,right=xs.filter(x=>x>0).length;
    const sameSide=left>=3||right>=3;
    const tightestThree=Math.min(sorted[2]-sorted[0],sorted[3]-sorted[1]);
    if(tightestThree<=7.5)return 'Bunch';
    if(sameSide)return 'Trips';
    return 'Spread';
  }

  function playMeta(play,index=0){
    const shape=formationShape(play),screen=isScreenPlay(play),motion=!!play?.motion||play?.family==='Motion'||play?.category==='Motion';
    const category=String(play?.category||play?.family||'Intermediate');
    const shot=!screen&&(/deep|shot/i.test(category)||Array.isArray(play?.routes)&&play.routes.filter(r=>/go|fade|post|corner|wheel|double move|tunnel go/i.test(r)).length>=3);
    const group=screen?'Screens':motion?'Motion':shape==='Trips'?'Trips':(['Bunch','Stack'].includes(shape)?'Bunch / Stack':shot?'Shot Plays':'Spread');
    return {...play,index,shape,screen,motion,shot,group,setLabel:screen?(shape==='Spread'?'Screen':`${shape} Screen`):(motion?`${shape} Motion`:shape)};
  }

  function buildCatalog(concepts=[]){
    return [...BASE_PLAYS,...(Array.isArray(concepts)?concepts:[])].map((play,index)=>playMeta(play,index));
  }

  function setMismatch(fromPlay,toPlay){
    const from=fromPlay?.shape?fromPlay:playMeta(fromPlay||{}),to=toPlay?.shape?toPlay:playMeta(toPlay||{});
    let value=0;
    if(from.shape!==to.shape)value+=.42;
    if(from.screen!==to.screen)value+=.45;
    if(from.motion!==to.motion)value+=.15;
    if(from.group!==to.group)value+=.12;
    const fromXs=Array.isArray(from.xs)?from.xs:[-18,-6,6,18],toXs=Array.isArray(to.xs)?to.xs:[-18,-6,6,18];
    const travel=fromXs.reduce((sum,x,i)=>sum+Math.abs(Number(x)-Number(toXs[i]??x)),0)/48;
    return clamp(value+travel*.22,0,1);
  }

  function coverageTraits(name){
    const text=String(name||'').toUpperCase();
    return {
      man:/MAN|PRESS|BRACKET/.test(text),
      zone:/ZONE|TWO HIGH|ROBBER|MATCH/.test(text),
      pressure:/PRESSURE|BLITZ|ALL OUT/.test(text),
      disguise:/DISGUISE|MATCH/.test(text),
      deep:/DEEP|TWO HIGH/.test(text)
    };
  }

  function defenseResponsePlan({fromPlay,toPlay,coverageName='',repeatCount=0,audibleCount=1,seed=1,defenderCount=6}={}){
    const from=fromPlay?.shape?fromPlay:playMeta(fromPlay||{}),to=toPlay?.shape?toPlay:playMeta(toPlay||{});
    const traits=coverageTraits(coverageName),mismatch=setMismatch(from,to),passToScreen=!from.screen&&to.screen,screenToShot=from.screen&&!to.screen;
    const learned=clamp(repeatCount*.115+Math.max(0,audibleCount-1)*.045,0,.36);
    let surprise=.18+mismatch*.48+(passToScreen?.18:0)+(screenToShot?.08:0)+(traits.disguise?.05:0)+(traits.pressure&&passToScreen?.10:0)-learned;
    if(traits.zone&&passToScreen)surprise-=.045;
    surprise=clamp(surprise,.08,.82);
    const random=seededRandom(seed);
    const defenders=[];
    let busts=0;
    for(let i=0;i<defenderCount;i++){
      const corner=i<Math.min(4,defenderCount),safety=!corner;
      let delay=.12+surprise*(corner?.72:.93)+random()*.32;
      if(passToScreen&&traits.man&&corner)delay+=.14;
      if(passToScreen&&safety)delay+=.12;
      if(screenToShot&&traits.deep&&safety)delay-=.10;
      delay=clamp(delay,.10,1.45);
      let bustChance=.025+surprise*.19+(passToScreen&&traits.man&&corner?.075:0)+(traits.pressure?.035:0)-learned*.18;
      if(safety)bustChance*=.72;
      bustChance=clamp(bustChance,.015,.31);
      const busted=random()<bustChance;
      if(busted)busts++;
      let assignmentIndex=i;
      if(busted&&corner){
        const direction=random()<.5?-1:1;
        assignmentIndex=(i+direction+4)%4;
      }
      const side=(i%2===0?-1:1)*(random()<.5?-1:1);
      const wrongX=busted?side*lerp(1.25,3.8,random()):0;
      const wrongZ=busted?(passToScreen?-lerp(1.2,3.3,random()):screenToShot?lerp(1.0,2.8,random()):lerp(-1.8,1.8,random())):0;
      const speed=lerp(5.15,7.35,random())+(traits.zone?.15:0);
      const postSnapHold=clamp((busted?.28:.035)+surprise*(busted?.55:.17)+random()*(busted?.22:.10),.03,.88);
      const correctionAt=busted?clamp(delay+.82+surprise*.72+random()*.78,.78,3.15):delay;
      defenders.push({index:i,role:corner?'corner':'safety',delay,speed,bustChance,busted,assignmentIndex,wrongX,wrongZ,postSnapHold,correctionAt});
    }
    const averageDelay=defenders.reduce((sum,d)=>sum+d.delay,0)/Math.max(1,defenders.length);
    const label=busts>=2?'LATE CHECK':busts===1?'ASSIGNMENT CHANGE':averageDelay>.68?'DEFENSE SHIFTING':'DEFENSE COMMUNICATING';
    return {from,to,mismatch,surprise,learned,passToScreen,screenToShot,busts,averageDelay,label,defenders,
      offenseBaseSeconds:clamp(.48+mismatch*.20,.48,.72)};
  }

  function defensiveAdjustmentElapsed({now,startedAt,committedAt=0,countdownScale=.22}){
    const pre=Math.max(0,((committedAt||now)-startedAt)/1000);
    if(!committedAt)return pre;
    return pre+Math.max(0,(now-committedAt)/1000)*clamp(countdownScale,0,1);
  }

  function moveTowards(position,target,maxDistance){
    const dx=target.x-position.x,dy=(target.y||0)-(position.y||0),dz=target.z-position.z;
    const distance=Math.hypot(dx,dy,dz);
    if(distance<=maxDistance||distance<1e-6)return {x:target.x,y:target.y||0,z:target.z,reached:true,distance};
    const scale=maxDistance/distance;
    return {x:position.x+dx*scale,y:(position.y||0)+dy*scale,z:position.z+dz*scale,reached:false,distance};
  }

  function recommendedPlays(catalog,current){
    const otherScreen=!current?.screen;
    const score=play=>{
      let value=0;
      if(otherScreen&&play.screen)value+=10;
      if(!otherScreen&&!play.screen)value+=7;
      if(play.shape!==current?.shape)value+=3;
      if(play.motion)value+=1.4;
      if(play.name===current?.name)value-=100;
      if(['WR Bubble','Tunnel Screen','Slip Screen','Bunch Screen','Jet Bubble','Slant / Flat','Mesh','Bunch Flood','Stack Switch','Dagger','Four Verticals'].includes(play.name))value+=2;
      return value;
    };
    return [...catalog].sort((a,b)=>score(b)-score(a)||a.index-b.index).slice(0,12);
  }

  return {clamp,lerp,smooth,normalize,BASE_PLAYS,GROUP_ORDER,hashString,seededRandom,isScreenPlay,formationShape,playMeta,buildCatalog,setMismatch,coverageTraits,defenseResponsePlan,defensiveAdjustmentElapsed,moveTowards,recommendedPlays};
});
