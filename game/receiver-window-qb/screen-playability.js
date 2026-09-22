/* Screen-play tuning layered onto QBVariety so normal route and runner logic remains unchanged. */
(function(root){
  'use strict';
  const V=root.QBVariety||(typeof require==='function'?require('./variety.js'):null);
  if(!V)return;

  const clamp=(n,a,b)=>Math.max(a,Math.min(b,Number(n)||0));
  const baseLane=V.lane;
  const baseCatchAnimation=V.catchAnimation;

  V.screenOutlet=function screenOutlet({anchor,losZ,defenders=[],qb={x:0,z:losZ+14},awareness=60}){
    let best=null;const iq=clamp(awareness,1,100)/100;
    // A screen needs a real receiving window, not a short shuffle with the corner still attached.
    // Test wider landmarks and a few depths, while keeping the throw behind the line and in bounds.
    for(const dx of [0,-2.2,2.2,-4.2,4.2,-6.2,6.2])for(const dz of [0,.9,-.8,1.8]){
      const x=clamp(anchor.x+dx,-23.4,23.4),z=clamp(anchor.z+dz,losZ-1,losZ+5);
      let crowd=0,laneRisk=0,nearest=12;
      for(const d of defenders){
        const distance=Math.hypot(x-d.x,z-d.z);nearest=Math.min(nearest,distance);
        crowd+=Math.max(0,5.1-distance)*1.35+Math.max(0,2.9-distance)*1.8;
        const vx=x-qb.x,vz=z-qb.z,t=clamp(((d.x-qb.x)*vx+(d.z-qb.z)*vz)/(vx*vx+vz*vz||1),0,1);
        if(t>.2&&t<.98)laneRisk+=Math.max(0,2.05-Math.hypot(qb.x+vx*t-d.x,qb.z+vz*t-d.z))*(1.2-.25*t);
      }
      const travel=Math.abs(dx)*.095+Math.abs(dz)*.08;
      const score=Math.min(nearest,7)*(.20+iq*.08)-crowd*(.82+iq*.48)-laneRisk*(1+iq*.45)-travel;
      if(!best||score>best.score)best={x,z,score};
    }
    return best;
  };

  V.screenRead=function screenRead({x,z,elapsed=0,blockers=[],defenders=[],awareness=60}){
    const available=defenders.filter(d=>!d.blocked),nearest=available.reduce((best,d)=>{
      const distance=Math.hypot(d.x-x,d.z-z);return !best||distance<best.distance?{...d,distance}:best;
    },null),threat=nearest?.distance??99;

    // Never idle behind a blocker when a free hitter is already at the catch point.
    if(nearest&&threat<3.1){
      const side=Math.sign(x-nearest.x)||Math.sign(x)||1;
      return {phase:'HOT CUT',pace:1,lead:{x:clamp(x+side*3.8,-23,23),z:z-3.2,weight:.62}};
    }

    const leads=blockers.filter(b=>b.z<z-.35&&b.z>z-12&&Math.abs(b.x-x)<11.5);
    let chosen=null;
    for(const b of leads){
      const target=defenders.find(d=>d.id===b.target);
      const side=target?Math.sign(b.x-target.x)||Math.sign(x-b.x)||1:Math.sign(x-b.x)||1;
      const gate={x:clamp(b.x+side*1.75,-23,23),z:b.z+.9};
      let risk=0;
      for(const d of defenders)risk+=Math.max(0,3.2-Math.hypot(gate.x-d.x,gate.z-d.z))*(d.blocked?.14:1);
      const score=-Math.hypot(gate.x-x,gate.z-z)*.22-risk+(b.engaged?1.05:0);
      if(!chosen||score>chosen.score)chosen={...gate,score,blocker:b};
    }
    if(!chosen||threat>10.5)return {phase:'BURST',pace:1,lead:null};

    // Let the runner press a potential block briefly, then demand an upfield decision.
    const prepare=chosen.score>-2&&elapsed<.55&&!chosen.blocker.engaged&&z-chosen.blocker.z<3.6&&threat>3.2&&threat<7.5;
    return {
      phase:prepare?'PRESS BLOCK':chosen.score<-2?'CUTBACK':'FOLLOW SEAL',
      pace:prepare?.86:1,
      lead:{x:chosen.x,z:Math.min(z-.3,chosen.z),weight:chosen.score<-2?.18:.42+clamp(awareness,1,100)*.003}
    };
  };

  V.lane=function lane(options){
    if(!options?.screen)return baseLane(options);
    const {x,z,defenders=[],style,bestZ=z,goalZ=-60,markerZ=-10,lead=null,awareness=60,blockers=[],previous=null}=options;
    const iq=(clamp(awareness,1,100)-60)/500;
    const near=Math.min(Math.abs(z-markerZ),Math.abs(z-goalZ))<8,limit=near?1.2:1.6;
    const wide=style==='YAC Specialist';
    const angles=[0,-.32,.32,-.65,.65,-1,1,-1.35,1.35,-Math.PI/2,Math.PI/2];
    let best=null;
    for(const a of angles){
      const dx=Math.sin(a),dz=-Math.cos(a);let risk=0;
      for(const d of defenders)for(const t of [.2,.5,.85]){
        const px=x+dx*6*t,pz=z+dz*6*t;
        const dist=Math.hypot(px-d.x-(d.vx||0)*t*(.5+iq),pz-d.z-(d.vz||0)*t*(.5+iq));
        risk+=Math.max(0,2.7-dist)*(d.blocked?.14:1);
      }
      const endX=x+dx*4,forward=-dz,back=dz>0;
      let score=forward*(near?5:3.85)-risk*.95-Math.max(0,Math.abs(endX)-(23-iq))*2-(back?3.5+(z-bestZ)*2:0);
      if(!wide)score-=Math.abs(dx)*.18;
      if(previous)score+=(dx*previous.x+dz*previous.z)*.35;
      for(const b of blockers){
        const t=clamp(((b.x-x)*dx+(b.z-z)*dz)/3,0,1);
        const gap=Math.hypot(x+dx*3*t-b.x,z+dz*3*t-b.z);
        if(b.engaged){
          // Run off the blocker shoulder. Only the blocker's body itself remains an obstacle.
          score+=Math.max(0,1.45-Math.abs(gap-1.55))*.78;
          if(gap<.55)score-=(.55-gap)*1.8;
        }else score-=Math.max(0,1.15-gap)*.72;
      }
      if(lead&&!back)score-=Math.hypot(x+dx*3-lead.x,z+dz*3-lead.z)*(lead.weight||.15);
      if(!best||score>best.score)best={x:dx,z:dz,score,risk,limit};
    }
    return best;
  };

  V.catchAnimation=function catchAnimation(options){
    const animation=baseCatchAnimation(options);
    return options?.screen&&animation.name==='SCREEN TURN-UP'?{...animation,duration:.42}:animation;
  };

  if(typeof module!=='undefined')module.exports=V;
})(typeof globalThis!=='undefined'?globalThis:this);
