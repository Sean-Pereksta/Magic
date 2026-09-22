/* Physical pre-snap adjustments and strength battles. No renderer or play-name reads. */
(function(root){
  'use strict';
  const clamp=(n,a,b)=>Math.max(a,Math.min(b,Number.isFinite(n)?n:a));
  const point=a=>a.mesh?.position||a;
  const distance=(a,b)=>Math.hypot(a.x-b.x,a.z-b.z);
  function formation(play,losZ){
    return play.routes.map((route,i)=>({x:clamp((play.xs||[-18,-6,6,18])[i],-23,23),z:losZ-.9+(play.depths?.[i]??(i%2)*.35),route,screenTarget:play.screen===i}));
  }
  // Arrival braking, acceleration and turn limits apply to both teams during a shift.
  function advance(actor,target,dt,speed=5.6){
    dt=clamp(dt,0,.05);const p=point(actor),v=actor.velocity,h=actor.heading;
    const dx=target.x-p.x,dz=target.z-p.z,gap=Math.hypot(dx,dz);
    if(gap<.045&&Math.hypot(v.x,v.z)<.35){p.x=target.x;p.z=target.z;v.x=0;v.z=0;return true;}
    const desired=Math.atan2(dx,dz),current=Math.atan2(h.x,h.z);
    const angle=Math.atan2(Math.sin(desired-current),Math.cos(desired-current));
    const turn=clamp(angle,-7*dt,7*dt),facing=current+turn;h.x=Math.sin(facing);h.z=Math.cos(facing);
    const pace=Math.min(speed,Math.sqrt(Math.max(0,2*16*gap)))*Math.max(.2,Math.cos(angle));
    const vx=h.x*pace-v.x,vz=h.z*pace-v.z,len=Math.hypot(vx,vz),scale=Math.min(1,18*dt/(len||1));
    v.x+=vx*scale;v.z+=vz*scale;
    const step=Math.hypot(v.x,v.z)*dt;
    if(gap<step+.02&&(v.x*dx+v.z*dz)>0&&gap<.15){p.x=target.x;p.z=target.z;v.x=0;v.z=0;return true;}
    p.x=clamp(p.x+v.x*dt,-24.5,24.5);p.z+=v.z*dt;return false;
  }
  function reaction({now,reaction=.2,discipline=.5,random=.5,previous=null}){
    const delay=260+clamp(reaction,.06,.6)*1000+clamp(random,0,1)*(740-380*clamp(discipline,0,1));
    // Repeated audibles cannot keep restarting an already-pending read.
    return previous&&previous.readyAt>now?previous:{readyAt:now+delay,nextReadAt:now+delay,error:(clamp(random,0,1)-.5)*(1.8-1.4*clamp(discipline,0,1))};
  }
  function defensiveLandmark({kind,mode,target=0,zoneX,homeZ,observed,error=0}){
    const selected=observed[target]||observed[0];if(!selected)return {x:zoneX,z:homeZ};
    const zone=mode==='zone'||mode==='deepzone';
    if(kind==='corner'&&!zone)return {x:clamp(selected.x+error,-24,24),z:selected.z-(mode==='press'||mode==='allout'?1.6:5.5)};
    const sameSide=observed.filter(r=>Math.sign(r.x||1)===Math.sign(zoneX||1));
    const group=sameSide.length?sameSide:observed,mean=group.reduce((s,r)=>s+r.x,0)/group.length;
    // A zone shell shades visible spacing, not the offense's hidden route or screen label.
    return {x:clamp(zoneX+clamp(mean-zoneX,-6,6)*(kind==='corner'?.48:.23)+error,-24,24),z:homeZ};
  }
  function blockContest({strength=60,defenseStrength=60,alignment=1,technique=.5}){
    const edge=clamp((strength-defenseStrength)/100,-1.4,1.4),leverage=clamp(alignment,0,1);
    const duration=clamp(1.25+Math.max(0,edge)*2.5-Math.max(0,-edge)*.65+leverage*.35,.6,3.8);
    const shedRate=clamp(.34-edge*.85+clamp(technique,0,1)*.12+(1-leverage)*.3,.025,2.2);
    // Even/losing strength never gets a free bull rush into the protected carrier.
    const driveSpeed=Math.abs(edge)<=.06?0:Math.sign(edge)*Math.min(2.3,(Math.abs(edge)-.06)*3.2);
    return {edge,duration,shedRate,driveSpeed,reach:clamp(.12-Math.min(0,edge)*.70,.12,.65)};
  }
  function engagement(contest,random=.5){
    return {...contest,elapsed:0,hazard:0,threshold:-Math.log(1-clamp(random,.000001,.999999))};
  }
  function advanceEngagement(state,dt,leverage=1){
    const before=state.elapsed;state.elapsed+=Math.max(0,dt);
    const vulnerable=Math.max(0,state.elapsed-.18)-Math.max(0,before-.18);
    state.hazard+=state.shedRate*(1+Math.max(0,state.elapsed-1)*.35+(1-clamp(leverage,0,1)))*vulnerable;
    return state.elapsed>=state.duration||state.hazard>=state.threshold;
  }
  // True only when the blocker physically lies between the defender and runner.
  function shielded(defender,runner,blocker,radius=.68){
    const dx=runner.x-defender.x,dz=runner.z-defender.z,den=dx*dx+dz*dz;
    if(den<.0001||distance(defender,blocker)>2.05)return false;
    const t=((blocker.x-defender.x)*dx+(blocker.z-defender.z)*dz)/den;
    return t>.06&&t<.98&&Math.hypot(defender.x+dx*t-blocker.x,defender.z+dz*t-blocker.z)<radius;
  }
  function tackleRadius({defenderSize=1,runnerSize=1,technique=.5,evasion=.5,fooled=false,diving=false,blockReach=1}){
    const body=.4*(defenderSize+runnerSize);
    return body+(fooled?0:(.36+.14*technique-.06*clamp(evasion,0,1)+(diving?.28:0))*blockReach);
  }
  function closest(ax,az,bx,bz){
    const dx=bx-ax,dz=bz-az,t=clamp(-(ax*dx+az*dz)/(dx*dx+dz*dz||1),0,1);
    return Math.hypot(ax+dx*t,az+dz*t);
  }
  // Score tackle envelopes rather than distance to a defender's center alone.
  function safeLane(options,base){
    const {x,z,defenders=[],blockers=[],speed=8,awareness=60,previous=null,lead=null,screen=false,goalZ=-60,markerZ=-10}=options;
    if(!defenders.length)return base;
    const near=Math.min(Math.abs(z-goalZ),Math.abs(z-markerZ))<8;
    const angles=[Math.atan2(base.x,-base.z),0,-.32,.32,-.65,.65,-1,1,-1.35,1.35];
    const margin=.45+clamp(awareness,1,100)*.004;
    let best=null;
    for(const angle of angles){
      const dx=Math.sin(angle),dz=-Math.cos(angle);let risk=0;
      // Forecast the runner's acceleration instead of assuming an instant full-speed cut.
      for(const d of defenders){
        let px=x,pz=z,ddx=d.x,ddz=d.z;
        const blocker=blockers.find(b=>b.engaged&&b.target===d.id);
        for(const t of [.16,.36,.64]){
          const blend=Math.min(1,t*4),travel=speed*t;
          const nx=x+dx*travel*blend+(options.vx||0)*t*(1-blend),nz=z+dz*travel*blend+(options.vz||0)*t*(1-blend);
          const ex=d.x+(d.vx||0)*t,ez=d.z+(d.vz||0)*t;
          const gap=closest(px-ddx,pz-ddz,nx-ex,nz-ez);
          const covered=blocker&&shielded({x:ex,z:ez},{x:nx,z:nz},{x:blocker.x+(blocker.vx||0)*t,z:blocker.z+(blocker.vz||0)*t},blocker.radius||.68);
          const envelope=(d.tackleRadius||1.25)+margin+(d.diving?.25:0);
          risk+=Math.max(0,envelope+.65-gap)*(covered?.14:1)*2;
          risk+=Math.max(0,(d.tackleRadius||1.25)+.12-gap)*(covered?.08:1)*5;
          px=nx;pz=nz;ddx=ex;ddz=ez;
        }
      }
      const endX=x+dx*Math.min(5,speed*.64);
      let score=-dz*(near?5:screen?4:3.4)-risk-Math.max(0,Math.abs(endX)-23.3)*3;
      if(dz>0)score-=5;
      if(previous)score+=(dx*previous.x+dz*previous.z)*.5;
      score+=(dx*base.x+dz*base.z)*.30;
      for(const b of blockers){
        const gap=closest(x-b.x,z-b.z,x+dx*3-b.x,z+dz*3-b.z);
        score-=Math.max(0,.8-gap)*3;
        if(b.engaged)score+=Math.max(0,1.2-Math.abs(gap-1.45))*.6;
      }
      if(lead&&dz<=0)score-=Math.hypot(x+dx*3-lead.x,z+dz*3-lead.z)*(lead.weight||.15);
      if(!best||score>best.score)best={x:dx,z:dz,score,risk,limit:base.limit};
    }
    return best;
  }
  const api={formation,advance,reaction,defensiveLandmark,blockContest,engagement,advanceEngagement,shielded,tackleRadius,safeLane};
  if(typeof module!=='undefined')module.exports=api;
  root.QBTactics=api;
})(typeof globalThis!=='undefined'?globalThis:this);
