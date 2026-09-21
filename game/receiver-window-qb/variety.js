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
    return {speed:4.85+3.65*rating(p,'speed'),accel:11+16*rating(p,'cutting'),
      turn:3.8+6*rating(p,'turning'),cutLoss:.48-.29*clamp(rating(p,'cutting'),0,1.4)};
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
  function tackleTechnique(round,skill){
    return clamp(.18+clamp(skill,0,1)*.55+.27*(1-Math.exp(-Math.max(0,round-1)/18)),.18,1);
  }
  // Closest point during a frame of relative motion; prevents contact tunneling.
  function sweptContact(x0,z0,x1,z1,radius=0){
    const dx=x1-x0,dz=z1-z0,den=dx*dx+dz*dz;
    const time=den>1e-8?clamp(-(x0*dx+z0*dz)/den,0,1):0;
    const distance=Math.hypot(x0+dx*time,z0+dz*time),startSquared=x0*x0+z0*z0;
    let entry=time;
    if(startSquared<=radius*radius)entry=0;
    else if(den>1e-8&&distance<=radius){const dot=x0*dx+z0*dz;entry=clamp((-dot-Math.sqrt(Math.max(0,dot*dot-den*(startSquared-radius*radius))))/den,0,1);}
    return {time,distance,entry};
  }
  // Leverage and closing momentum matter; no automatic wins or permanent holds.
  function blockOutcome({strength,size,defenseStrength,defenseSize,alignment,momentum}){
    const edge=(strength-defenseStrength)*.007+(size-defenseSize)*.005+clamp(alignment,-1,1)*.24+clamp(momentum,-5,5)*.035;
    return {duration:clamp(.32+edge*.65,.12,.85),slow:clamp(.76-edge*.45,.35,.94)};
  }
  function physique(p){
    const size=clamp(rating(p,'size'),0,1.4),strength=clamp(rating(p,'strength'),0,1.4);
    return {x:.82+size*.22+strength*.12,y:.88+size*.23,z:.86+size*.16+strength*.10};
  }
  const concepts=[
    {name:'Bunch Flood',family:'Bunch',xs:[-18,8,10.5,13],depths:[0,2,0,3],routes:['Go','Flat','Out','Corner'],hint:'Read the right flat → out → corner. Turning and catching win.'},
    {name:'Bunch Rub',family:'Bunch',xs:[-18,7,9.5,12],depths:[0,2,0,3],routes:['Post','Wheel','Drag','Slant'],hint:'Cross releases create traffic; quick cuts beat man coverage.'},
    {name:'Trips Sail',family:'Trips',xs:[-18,5,12,20],routes:['Dig','Flat','Corner','Go'],hint:'Three right-side depths stretch zone coverage.'},
    {name:'Quick Stick',family:'Quick',xs:[-19,-7,7,19],routes:['Slant','Stick','Flat','Fade'],hint:'Throw early to the settling slot or the flat.'},
    {name:'Stack Switch',family:'Stack',xs:[-12,-12,12,12],depths:[0,3,0,3],routes:['Slant','Wheel','Slant','Wheel'],hint:'Stacked releases free speed down the sideline.'},
    {name:'Deep Scissors',family:'Shots',xs:[-17,-6,6,17],routes:['Post','Corner','Corner','Post'],hint:'Cross deep landmarks; speed and high-point athleticism matter.'},
    {name:'WR Bubble',family:'Screens',xs:[-18,7,12,18],depths:[0,3,0,0],routes:['Go','Bubble','Lead','Lead'],screen:1,hint:'H catches behind Y/Z. Release early; size and strength lead the way.'},
    {name:'Tunnel Screen',family:'Screens',xs:[-19,-12,-6,18],depths:[2,0,0,0],routes:['Tunnel','Lead','Lead','Post'],screen:0,hint:'X slips inside behind H/Y. Late throws let the defense close.'},
    {name:'Motion Flood',family:'Motion',xs:[-18,-4,8,18],routes:['Post','Flat','Out','Go'],motion:{slot:1,to:4},hint:'H motions right before the snap; read the three-level flood.'},
    {name:'Motion Cross',family:'Motion',xs:[-18,-8,4,18],routes:['Go','Drag','Wheel','Dig'],motion:{slot:2,to:-4},hint:'Y motions into a wheel while crossers attack underneath.'}
  ];
  const api={concepts,blockOutcome,physique,identities,identity,signature,movement,cleanHistory,tendencies,remember,chooseCoverage,placement,pursuitTime,tackleTechnique,sweptContact};
  if(typeof module!=='undefined')module.exports=api;root.QBVariety=api;
})(typeof globalThis!=='undefined'?globalThis:this);
