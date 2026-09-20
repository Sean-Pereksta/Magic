/* Football variety rules. Pure, bounded, and shared with the regression suite. */
(function(root){
  'use strict';
  const clamp=(n,a,b)=>Math.max(a,Math.min(b,Number(n)||0));
  const rating=(p,k)=>{const n=clamp(p[k],1,Number.MAX_SAFE_INTEGER);return (n<=100?n:100+40*(1-Math.exp(-(n-100)/80)))/100;};
  const identities=[
    {name:'Conservative',schemes:['off','twohigh','deep'],speed:0,strength:0,hands:0,depth:2},
    {name:'Fast Secondary',schemes:['off','match','twohigh'],speed:.35,strength:-5,hands:0,depth:0},
    {name:'Physical Secondary',schemes:['press','pressure','bracket'],speed:-.15,strength:12,hands:0,depth:0},
    {name:'Aggressive Man',schemes:['press','pressure','off'],speed:.1,strength:4,hands:0,depth:-1},
    {name:'Zone Heavy',schemes:['underzone','deep','match','disguise'],speed:0,strength:0,hands:.02,depth:1},
    {name:'Ball Hawks',schemes:['robber','bracket','disguise'],speed:-.1,strength:-2,hands:.07,depth:0},
    {name:'Short Route Hunters',schemes:['underzone','robber','press'],speed:0,strength:3,hands:.02,depth:-2},
    {name:'Deep Ball Patrol',schemes:['deep','twohigh','bracket'],speed:.1,strength:0,hands:.02,depth:4}
  ];
  const identity=round=>identities[(Math.max(1,Math.floor(round))-1)%identities.length];
  function signature(p){
    const scores=[['Deep Threat',rating(p,'speed')],['Route Artist',(rating(p,'cutting')+rating(p,'turning'))/2],
      ['Sure Hands',rating(p,'catching')],['Contact Balance',rating(p,'strength')],
      ['Sideline Specialist',(rating(p,'catching')+rating(p,'turning'))/2],
      ['YAC Specialist',(rating(p,'evasion')+rating(p,'tricks'))/2]];
    scores.sort((a,b)=>b[1]-a[1]);
    const earned=(p.prestige||0)>0||Object.values(p.trainingByStat||{}).some(n=>n>=5);
    return earned&&scores[0][1]>=.85?scores[0][0]:null;
  }
  function movement(p){
    return {speed:6.05+2.65*rating(p,'speed'),accel:14+12*rating(p,'cutting'),
      turn:4.8+5*rating(p,'turning'),cutLoss:.48-.29*clamp(rating(p,'cutting'),0,1.4)};
  }
  function cleanHistory(raw){
    return (Array.isArray(raw)?raw:[]).slice(-12).filter(p=>p&&Number.isFinite(p.depth)&&Number.isInteger(p.target)&&p.target>=0&&p.target<4)
      .map(p=>({depth:clamp(p.depth,-10,50),target:p.target,route:String(p.route||'').slice(0,16)}));
  }
  function tendencies(raw){
    const history=cleanHistory(raw),n=history.length,targets=[0,0,0,0],routes=Array.from({length:4},()=>Object.create(null));
    let deep=0,short=0;
    for(const p of history){if(p.depth>=18)deep++;if(p.depth<=7)short++;targets[p.target]++;routes[p.target][p.route]=(routes[p.target][p.route]||0)+1;}
    // No reaction to the first two attempts; a long run of one tendency caps at a modest adjustment.
    const evidence=clamp((n-2)/8,0,1),bias=count=>clamp((count/Math.max(1,n)-.45)*2,0,1)*evidence;
    return {depth:3*bias(deep)-2*bias(short),help:targets.indexOf(Math.max(...targets)),
      attention:bias(Math.max(...targets)),anticipation:routes.map(r=>.65*bias(Math.max(0,...Object.values(r))))};
  }
  function remember(raw,attempt){return cleanHistory([...cleanHistory(raw),attempt]);}
  function chooseCoverage(profile,memory,previous,random=Math.random){
    const pool=profile.schemes.map(id=>({id,weight:id===previous?.4:1}));
    for(const entry of pool){if(memory.depth>1&&['deep','twohigh'].includes(entry.id))entry.weight+=.7;if(memory.depth<-.5&&['underzone','robber'].includes(entry.id))entry.weight+=.7;}
    let roll=clamp(random(),0,.999999)*pool.reduce((sum,p)=>sum+p.weight,0);
    for(const p of pool){roll-=p.weight;if(roll<0)return p.id;}return pool[0].id;
  }
  function placement({dx,dz,height,headingX,headingZ,speed,contest=99,defenderX=0,defenderZ=0,jump=0,sideline=false,timing=false,bobbled=false}){
    const along=dx*headingX+dz*headingZ,lateral=Math.abs(dx*headingZ-dz*headingX);
    const away=contest<2.5&&(dx*defenderX+dz*defenderZ)<-.12;
    const stride=speed>2&&along>=-.12&&along<1.3&&lateral<.65&&height>.8&&height<2.15&&!jump;
    let kind=bobbled?'BOBBLE RECOVERY':sideline?'TOE TAP':jump>.18?'HIGH POINT':height<.9?'LOW CATCH':
      along<-.3?'BACK SHOULDER':away?'OUTSIDE SHOULDER':contest<1.45?'CONTACT CATCH':stride&&along>.45?'OVER THE SHOULDER':'CATCH AND TURN';
    const feedback=bobbled?kind:contest<1.45?(jump>.18?'JUMP BALL':'CONTESTED CATCH'):sideline?kind:away?'GREAT BALL PLACEMENT':stride?(timing?'PERFECT TIMING':'IN STRIDE'):kind;
    return {kind,feedback,stride,away,
      retention:clamp((stride?.98:along<-.3?.53:height<.9?.60:.76)-(contest<1.45?.1:0)-(bobbled?.14:0),.4,1),
      hands:clamp((away?.055:0)+(stride?.025:0)-(along<-.3?.065:0)-(height<.9?.04:0),-.11,.08)};
  }
  // Solve |relative position + carrier velocity*t| = defender speed*t. No speed boost.
  function pursuitTime(dx,dz,vx,vz,speed){
    const a=vx*vx+vz*vz-speed*speed,b=2*(dx*vx+dz*vz),c=dx*dx+dz*dz;
    let t;
    if(Math.abs(a)<.001)t=b<-.001?-c/b:0;
    else {const disc=b*b-4*a*c;if(disc>=0){const r=Math.sqrt(disc),roots=[(-b-r)/(2*a),(-b+r)/(2*a)].filter(n=>n>=0);t=roots.length?Math.min(...roots):0;}}
    return clamp(t||0,0,1.25);
  }
  const api={identities,identity,signature,movement,cleanHistory,tendencies,remember,chooseCoverage,placement,pursuitTime};
  if(typeof module!=='undefined')module.exports=api;root.QBVariety=api;
})(typeof globalThis!=='undefined'?globalThis:this);
