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
  const categories=['Quick Game','Intermediate','Deep Shots','Screens','Bunch / Stack','Motion','Trick Plays'];
  const category=p=>p.category||({Quick:'Quick Game',Bunch:'Bunch / Stack',Stack:'Bunch / Stack',Trips:'Intermediate',Shots:'Deep Shots'}[p.family])||(['Screens','Motion'].includes(p.family)?p.family:'Intermediate');
  const add=(name,category,routes,xs,hint,extra={})=>concepts.push({name,category,routes,xs,hint,...extra});
  add('Slant / Flat','Quick Game',['Slant','Flat','Stick','Fade'],[-19,-7,6,18],'Inside leverage opens the flat; outside leverage opens the slant.');
  add('Spacing','Quick Game',['Stick','Flat','Stick','Curl'],[-18,-6,5,19],'Three short windows; catch, turn and protect the first down.');
  add('Quick Outs','Quick Game',['Out','Stick','Flat','Out'],[-15,-5,5,15],'Sideline spacing with an inside outlet.');
  add('Choice Stick','Quick Game',['Slant','Option','Flat','Go'],[-19,-6,6,18],'H reads leverage at five yards: curl, go, in or out.');
  add('Shallow Mesh','Intermediate',['Drag','Dig','Go','Drag'],[-18,-8,7,19],'Crossers at different depths create a natural rub.',{depths:[0,1,0,3]});
  add('Drive Over','Intermediate',['Dig','Drag','Corner','Curl'],[-19,-5,7,19],'Drag below the dig; curl holds the opposite corner.');
  add('Levels Switch','Intermediate',['Dig','Stick','Dig','Post'],[-20,-8,5,16],'Layer the middle at three depths.',{depths:[0,3,0,2]});
  add('Smash Choice','Intermediate',['Curl','Corner','Option','Fade'],[-20,-10,6,19],'Curl/corner high-low left; Y adjusts to leverage.');
  add('Seam Verticals','Deep Shots',['Fade','Go','Go','Fade'],[-20,-7,7,20],'Wide fades stretch safeties away from the seams.');
  add('Slot Fade','Deep Shots',['Curl','Fade','Post','Out'],[-20,-9,6,19],'Outside curl draws the corner under the slot fade.');
  add('Post / Wheel','Deep Shots',['Post','Wheel','Drag','Go'],[-17,-8,6,19],'Post clears inside while the wheel climbs outside.');
  add('Switch Verticals','Deep Shots',['Wheel','Post','Post','Wheel'],[-16,-9,9,16],'Cross the release lanes, then attack vertical space.');
  add('Slip Screen','Screens',['Go','Slip','Lead','Lead'],[-19,3,8,14],'H slips behind two moving blockers; throw early.',{screen:1});
  add('Left Bubble','Screens',['Lead','Lead','Bubble Left','Go'],[-18,-12,-7,19],'Y widens left behind X/H; wait for the lane.',{screen:2});
  add('Middle Tunnel','Screens',['Go','Lead','Tunnel','Lead'],[-19,-4,8,15],'Y folds inside while H/Z seal pursuit.',{screen:2});
  add('Bunch Cross','Bunch / Stack',['Go','Drag','Dig','Slant'],[-19,6,8.5,11],'Staggered releases separate underneath crossers.',{depths:[0,3,0,2]});
  add('Stack Choice','Bunch / Stack',['Fade','Option','Go','Drag'],[-12,-12,12,12],'Back slot reads the coverage after the stack release.',{depths:[0,3,0,3]});
  add('Bunch Screen','Bunch / Stack',['Post','Bubble','Lead','Lead'],[-19,5,9,13],'Compact spacing lets two strong receivers lead H.',{screen:1,depths:[0,3,0,1]});
  add('Motion Mesh','Motion',['Drag','Dig','Drag','Fade'],[-18,-7,5,19],'Y crosses before the snap then joins the shallow mesh.',{motion:{slot:2,to:-3}});
  add('Jet Bubble','Motion',['Lead','Bubble Left','Lead','Go'],[-18,5,-10,19],'H motions left into a screen behind X/Y.',{motion:{slot:1,to:-5},screen:1});
  add('Motion Seam','Motion',['Out','Go','Option','Post'],[-18,-8,6,19],'H tightens into the seam; Y reads the underneath leverage.',{motion:{slot:1,to:-3}});
  add('Fake Bubble → Wheel','Trick Plays',['Go','Bubble Wheel','Lead','Post'],[-19,5,11,18],'H sells a bubble before accelerating upfield. Pump once.',{trick:true});
  add('Fake Tunnel → Shot','Trick Plays',['Tunnel Go','Lead','Corner','Post'],[-18,-10,5,19],'X flashes inside, then climbs vertically.',{trick:true});
  add('Sluggo','Trick Plays',['Double Move','Flat','Dig','Fade'],[-18,-7,6,19],'Slant-and-go against route jumpers. Repeats lose surprise.',{trick:true});
  add('Fake Quick Screen','Trick Plays',['Bubble Wheel','Lead','Dig','Go'],[-10,-18,6,19],'Show the screen, then attack the vacated sideline.',{trick:true});
  add('Motion Misdirection','Trick Plays',['Post','Return','Wheel','Dig'],[-19,-7,7,19],'H motions right then reverses across the formation.',{motion:{slot:1,to:5},trick:true});
  function playstyle(p){
    const map={'Burner':'Deep Threat','Route Tech':'Route Technician','Comeback Artist':'Route Technician','Open-Field':'YAC Specialist','Raw Athlete':'YAC Specialist','Power Slot':'Power Receiver','Possession':'Possession Receiver','Sure Hands':'Possession Receiver','Blocking':'Blocking Receiver'};
    return map[p.archetype]||((p.strength+p.size)/2>p.speed?'Blocking Receiver':'Route Technician');
  }
  function pumpChance(memory,target,concept,discipline=.5){
    const h=Array.isArray(memory)?memory:[],same=h.filter(x=>x.target===target).length,route=h.filter(x=>x.concept===concept).length;
    return clamp((.55-discipline*.4)*Math.exp(-h.length*.22-same*.35-route*.3),0,.55);
  }
  function weather(name){return {cut:name==='Light Rain'?.96:1,hands:name==='Light Rain'?.025:0,wind:name==='Windy'?.32:0,contact:name==='Cold'?1.035:1};}
  // Score sampled forward corridors, not summed repulsion. Retreat is exceptional.
  function lane({x,z,defenders,style,bestZ=z,goalZ=-60,markerZ=-10,lead=null}){
    const near=Math.min(Math.abs(z-markerZ),Math.abs(z-goalZ))<8,limit=near?1.2:3.2;
    const wide=style==='YAC Specialist',power=style==='Power Receiver'||style==='Possession Receiver';
    const angles=[0,-.32,.32,-.65,.65,-1,1,-1.35,1.35,-Math.PI/2,Math.PI/2];
    if(z-bestZ<limit&&!near&&!power)angles.push(-1.8,1.8);
    let best=null;
    for(const a of angles){const dx=Math.sin(a),dz=-Math.cos(a);let risk=0;
      for(const d of defenders){for(const t of [.2,.5,.85]){const px=x+dx*6*t,pz=z+dz*6*t,dist=Math.hypot(px-d.x-(d.vx||0)*t*.5,pz-d.z-(d.vz||0)*t*.5);risk+=Math.max(0,2.7-dist)*(d.blocked?.3:1);}}
      const endX=x+dx*4,forward=-dz,back=dz>0;
      let score=forward*(near?5:power?3.6:2.6)-risk*(power?.85:1.3)-Math.max(0,Math.abs(endX)-23)*2-(back?3.5+(z-bestZ)*2:0);
      if(!wide)score-=Math.abs(dx)*.18;
      if(lead&&!back)score-=Math.hypot(x+dx*3-lead.x,z+dz*3-lead.z)*.15;
      if(!best||score>best.score)best={x:dx,z:dz,score,risk,limit};
    }
    return best;
  }
  identities.push(
    {name:'Gamblers',schemes:['robber','press','pressure'],speed:.1,strength:0,hands:.03,depth:-1,discipline:.15},
    {name:'Bullies',schemes:['press','bracket'],speed:-.15,strength:14,hands:0,depth:0,discipline:.5},
    {name:'Track Team',schemes:['match','twohigh'],speed:.45,strength:-12,hands:0,depth:0,discipline:.45},
    {name:'Veterans',schemes:['disguise','match','deep'],speed:-.2,strength:2,hands:.03,depth:1,discipline:.95},
    {name:'Heavy Hitters',schemes:['pressure','underzone'],speed:-.2,strength:16,hands:-.02,depth:0,discipline:.55});
  identities[5].discipline=.25;
  const api={categories,category,playstyle,pumpChance,weather,lane,concepts,blockOutcome,physique,identities,identity,signature,movement,cleanHistory,tendencies,remember,chooseCoverage,placement,pursuitTime,tackleTechnique,sweptContact};
  if(typeof module!=='undefined')module.exports=api;root.QBVariety=api;
})(typeof globalThis!=='undefined'?globalThis:this);
