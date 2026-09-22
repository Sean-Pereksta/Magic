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
  // Append concepts so existing saves keep their selected-play indices.
  add('Double Stick Choice','Quick Game',['Stick','Choice','Flat','Slant'],[-19,-6,6,19],'H chooses a soft pocket or breaks away from leverage.',{primary:1});
  add('Whip / Flat','Quick Game',['Whip','Flat','Stick','Fade'],[-17,-6,6,19],'X sells inside before snapping out; H clears the flat.',{primary:0});
  add('Pivot Spacing','Quick Game',['Curl','Pivot','Choice','Out'],[-19,-5,6,18],'Pivot against tight man; settle in uncovered space against zone.',{primary:1});
  add('Fast Choice Cross','Quick Game',['Slant','Choice','Drag','Fade'],[-18,-7,5,19],'Cross the shallow defender and let H choose the clean window.',{primary:1});
  add('Mesh Sit Choice','Intermediate',['Drag','Choice','Stick','Drag'],[-19,-7,6,19],'H settles between zones while X/Z cross beneath.',{primary:1});
  add('Pivot Drive','Intermediate',['Dig','Pivot','Drag','Go'],[-18,-5,6,19],'The pivot changes direction underneath the dig.',{primary:1});
  add('Seam Read Levels','Intermediate',['Dig','Seam Read','Stick','Post'],[-19,-6,6,19],'H bends away from deep help; X works the intermediate window.',{primary:1});
  add('Whip Sail','Intermediate',['Post','Whip','Out','Corner'],[-19,4,11,19],'The whip holds the underneath defender below the sail.',{primary:2});
  add('Double Seam Read','Deep Shots',['Fade','Seam Read','Seam Read','Fade'],[-20,-7,7,20],'Both seams read deep help independently.',{primary:1});
  add('Switch Choice Shot','Deep Shots',['Wheel','Seam Read','Choice','Post'],[-17,-8,7,19],'A wheel clears the boundary for a seam read inside.',{primary:1});
  add('Post Pivot Shot','Deep Shots',['Post','Pivot','Seam Read','Corner'],[-18,-7,6,19],'Underneath pivot invites a deep opening; take the outlet if capped.',{primary:2});
  add('Read Screen Right','Screens',['Go','Screen Choice','Lead','Lead'],[-19,4,10,16],'H chooses bubble or tunnel from the visible edge leverage.',{screen:1});
  add('Read Screen Left','Screens',['Lead','Lead','Screen Choice','Post'],[-17,-10,-4,19],'Y reads the left edge and follows the first useful seal.',{screen:2});
  add('Stack Slip Screen','Screens',['Slip','Lead','Go','Lead'],[-13,-13,8,-5],'The trailing screen waits for X-side leverage before turning upfield.',{screen:0,depths:[3,0,0,1]});
  add('Wide Tunnel Convoy','Screens',['Tunnel','Lead','Lead','Fade'],[-21,-14,-6,19],'X folds inside behind two separated lead blockers.',{screen:0});
  add('Return Slip Screen','Screens',['Post','Slip','Lead','Lead'],[-19,6,12,19],'H shows width, returns, then slips behind the two leads.',{screen:1,motion:{slot:1,to:5,via:[{x:11,dz:2}],start:0,end:.9}});
  add('Bunch Choice Pivot','Bunch / Stack',['Go','Choice','Pivot','Corner'],[-19,6,9,12],'H reads space behind a pivot release.',{primary:1,depths:[0,3,0,2]});
  add('Stack Whip Switch','Bunch / Stack',['Whip','Go','Seam Read','Drag'],[-12,-12,12,12],'Whip beneath one stack; read the seam from the other.',{primary:0,depths:[3,0,0,3]});
  add('Bunch Read Screen','Bunch / Stack',['Post','Screen Choice','Lead','Lead'],[-19,5,9,13],'Read the edge instead of forcing the bubble into contain.',{screen:1,depths:[0,3,0,1]});
  add('Twin Shift Choice','Bunch / Stack',['Fade','Choice','Seam Read','Whip'],[-20,-8,8,20],'Two receivers shift inward before the route reads begin.',{primary:1,motions:[{slot:0,to:-13,end:.65},{slot:3,to:13,start:.25,end:.9}]});
  add('Jet Read Screen','Motion',['Lead','Screen Choice','Lead','Go'],[-18,7,-10,19],'H jets across, reads contain and follows X/Y.',{screen:1,motion:{slot:1,to:-5,start:.18,kind:'jet'}});
  add('Orbit Slip','Motion',['Go','Slip','Lead','Lead'],[-19,-6,8,15],'H arcs behind the formation into a slip screen.',{screen:1,motion:{slot:1,to:4,via:[{x:-2,dz:5},{x:3,dz:5}],kind:'orbit'}});
  add('Return Choice','Motion',['Post','Choice','Drag','Fade'],[-19,-8,6,19],'H returns after showing a crossing motion, then reads leverage.',{primary:1,motion:{slot:1,to:-5,via:[{x:4,dz:1}],kind:'return'}});
  add('Jet Wheel Flood','Motion',['Dig','Wheel','Out','Corner'],[-19,-8,7,19],'H jets right before climbing into the wheel.',{primary:1,motion:{slot:1,to:4,start:.15,kind:'jet'}});
  add('Orbit Seam Read','Motion',['Whip','Seam Read','Choice','Post'],[-18,7,-5,19],'H orbits left, then reads the seam after the snap.',{primary:1,motion:{slot:1,to:-4,via:[{x:3,dz:4},{x:-3,dz:4}],kind:'orbit'}});
  add('Trade Motion Mesh','Motion',['Drag','Choice','Drag','Fade'],[-18,-8,7,19],'A staggered two-receiver trade changes the mesh releases.',{primary:1,motions:[{slot:1,to:4,via:[{x:-2,dz:3}],end:.7},{slot:2,to:-4,start:.35,end:1}]});
  add('Orbit Fake Wheel','Trick Plays',['Go','Bubble Wheel','Lead','Choice'],[-19,-7,8,18],'Orbit into a bubble look; H climbs if the defender bites.',{primary:1,motion:{slot:1,to:4,via:[{x:0,dz:4}],kind:'orbit'},trick:true});
  add('Return Whip Shot','Trick Plays',['Double Move','Whip','Seam Read','Fade'],[-18,-7,6,19],'Return motion and a whip create different short/deep answers.',{primary:2,motion:{slot:1,to:-5,via:[{x:5,dz:1}],kind:'return'},trick:true});
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
  function lane({x,z,defenders,style,bestZ=z,goalZ=-60,markerZ=-10,lead=null,awareness=60,screen=false,blockers=[],previous=null}){
    const iq=(clamp(awareness,1,100)-60)/500;
    const near=Math.min(Math.abs(z-markerZ),Math.abs(z-goalZ))<8,limit=near?1.2:screen?1.6:3.2;
    const wide=style==='YAC Specialist',power=style==='Power Receiver'||style==='Possession Receiver';
    const angles=[0,-.32,.32,-.65,.65,-1,1,-1.35,1.35,-Math.PI/2,Math.PI/2];
    if(z-bestZ<limit&&!near&&!power&&!screen)angles.push(-1.8,1.8);
    let best=null;
    for(const a of angles){const dx=Math.sin(a),dz=-Math.cos(a);let risk=0;
      for(const d of defenders){for(const t of [.2,.5,.85]){const px=x+dx*6*t,pz=z+dz*6*t,dist=Math.hypot(px-d.x-(d.vx||0)*t*(.5+iq),pz-d.z-(d.vz||0)*t*(.5+iq));risk+=Math.max(0,2.7-dist)*(d.blocked?.3:1);}}
      const endX=x+dx*4,forward=-dz,back=dz>0;
      let score=forward*(near?5:power?3.6:2.6)-risk*(power?.85:1.3)-Math.max(0,Math.abs(endX)-(23-iq))*2-(back?3.5+(z-bestZ)*2:0);
      if(!wide)score-=Math.abs(dx)*.18;
      if(previous)score+=(dx*previous.x+dz*previous.z)*.35;
      for(const b of blockers){const t=clamp(((b.x-x)*dx+(b.z-z)*dz)/3,0,1);score-=Math.max(0,1.25-Math.hypot(x+dx*3*t-b.x,z+dz*3*t-b.z))*1.4;}
      if(lead&&!back)score-=Math.hypot(x+dx*3-lead.x,z+dz*3-lead.z)*(lead.weight||.15);
      if(!best||score>best.score)best={x:dx,z:dz,score,risk,limit};
    }
    return best;
  }
  const optionDepths={'Option':7,'Choice':8,'Seam Read':15,'Screen Choice':2.5};
  const isOption=route=>Object.hasOwn(optionDepths,route);
  const motions=(play,override=null)=>override?[override]:(play.motions|| (play.motion?[play.motion]:[]));
  function motionPath(m,start){
    return [start,...(m.via||[]).map(p=>({x:clamp(p.x,-23,23),z:start.z+clamp(p.dz||0,0,6)})),{x:clamp(m.to,-23,23),z:start.z+(m.dz||0)}];
  }
  function motionSample(m,start,progress){
    const points=motionPath(m,start),t=clamp((progress-(m.start||0))/Math.max(.1,(m.end??1)-(m.start||0)),0,1);
    if(t===1)return {...points[points.length-1],done:true};
    // Smooth starts/ends retain the same bounded, distance-based motion path.
    const u=t*t*(3-2*t),lengths=points.slice(1).map((p,i)=>Math.hypot(p.x-points[i].x,p.z-points[i].z));
    let distance=lengths.reduce((a,b)=>a+b,0)*u;
    for(let i=0;i<lengths.length;i++){if(distance<=lengths[i]||i===lengths.length-1){const part=lengths[i]?clamp(distance/lengths[i],0,1):1;return {x:points[i].x+(points[i+1].x-points[i].x)*part,z:points[i].z+(points[i+1].z-points[i].z)*part,done:t===1};}distance-=lengths[i];}
    return {...start,done:true};
  }
  function optionDecision({kind,x,z,defenders=[],awareness=60,toGo=25}){
    const nearest=[...defenders].sort((a,b)=>Math.hypot(a.x-x,a.z-z)-Math.hypot(b.x-x,b.z-z))[0];
    if(!nearest)return kind==='Screen Choice'?(x<0?'Bubble Left':'Bubble'):kind==='Seam Read'?'Go':'Stick';
    const side=Math.sign(x)||1,deep=nearest.z<z-4,under=nearest.z>z-1,inside=Math.abs(nearest.x)<Math.abs(x);
    if(kind==='Screen Choice'){
      const outside=defenders.filter(d=>(d.x-x)*side>1&&Math.abs(d.x-x)<9&&Math.abs(d.z-z)<9).length;
      const inward=defenders.filter(d=>(d.x-x)*side<0&&Math.abs(d.x-x)<7&&Math.abs(d.z-z)<8).length;
      return outside>inward?'Tunnel':x<0?'Bubble Left':'Bubble';
    }
    if(kind==='Seam Read'){
      const cap=defenders.filter(d=>d.z<z-2&&d.z>z-22&&Math.abs(d.x-x)<6);
      if(!cap.length)return 'Go';
      const middle=defenders.some(d=>Math.abs(d.x)<5&&d.z<z-2&&d.z>z-20);
      return middle?'Out':'Post';
    }
    if(kind==='Choice'){
      if(deep||defenders.filter(d=>Math.hypot(d.x-x,d.z-z)<7).length>1)return 'Stick';
      if(awareness>65&&toGo<=8&&Math.hypot(nearest.x-x,nearest.z-z)<4)return 'Whip';
      return inside?'Out':'Dig';
    }
    return deep?'Curl':under?'Go':inside?'Out':'Dig';
  }
  function screenOutlet({anchor,losZ,defenders=[],qb={x:0,z:losZ+14},awareness=60}){
    let best=null;
    for(const dx of [0,-1.8,1.8,-3,3])for(const dz of [0,-.7]){
      const x=clamp(anchor.x+dx,-23.4,23.4),z=clamp(anchor.z+dz,losZ-1,losZ+5);
      let risk=0;for(const d of defenders){risk+=Math.max(0,3.7-Math.hypot(x-d.x,z-d.z))*2;
        const vx=x-qb.x,vz=z-qb.z,t=clamp(((d.x-qb.x)*vx+(d.z-qb.z)*vz)/(vx*vx+vz*vz||1),0,1);
        if(t>.3&&t<.95)risk+=Math.max(0,1.5-Math.hypot(qb.x+vx*t-d.x,qb.z+vz*t-d.z));}
      const score=-risk*(.7+clamp(awareness,1,100)*.006)-Math.abs(dx)*.22-Math.abs(dz)*.12;
      if(!best||score>best.score)best={x,z,score};
    }
    return best;
  }
  function screenRead({x,z,elapsed=0,blockers=[],defenders=[],awareness=60}){
    const threat=defenders.filter(d=>!d.blocked).reduce((n,d)=>Math.min(n,Math.hypot(d.x-x,d.z-z)),99);
    const leads=blockers.filter(b=>b.z<z-.5&&b.z>z-10&&Math.abs(b.x-x)<9);
    let chosen=null;
    for(const b of leads){const target=defenders.find(d=>d.id===b.target),side=target?Math.sign(b.x-target.x)||Math.sign(x-b.x)||1:Math.sign(x-b.x)||1;
      const gate={x:clamp(b.x+side*1.45,-23,23),z:b.z+1.4};
      let risk=0;for(const d of defenders)risk+=Math.max(0,3-Math.hypot(gate.x-d.x,gate.z-d.z))*(d.blocked?.18:1);
      const score=-Math.hypot(gate.x-x,gate.z-z)*.25-risk+(b.engaged?.7:0);
      if(!chosen||score>chosen.score)chosen={...gate,score,blocker:b};
    }
    if(!chosen||threat>9)return {phase:'BURST',pace:1,lead:null};
    const prepare=chosen.score>-2&&elapsed<1.1&&!chosen.blocker.engaged&&z-chosen.blocker.z<3.2&&threat>3.2&&threat<7;
    return {phase:prepare?'PRESS BLOCK':chosen.score<-2?'CUTBACK':'FOLLOW SEAL',pace:prepare?.86:1,lead:{x:chosen.x,z:Math.min(z-.3,chosen.z),weight:chosen.score<-2?.12:.32+clamp(awareness,1,100)*.003}};
  }
  function evasionMove({distance,side=1,closing=0,lateral=0,crowded=false,screen=false,nearSideline=false,style='',athleticism=60,last=''}){
    let name=distance<2.1?'SPIN':distance>3.5?'HESITATION':'HARD CUT';
    if(nearSideline&&distance>=2.1)name='SPEED CUT';
    else if(crowded&&style==='Power Receiver')name='SHOULDER DIP';
    else if(distance>=2.1&&distance<3.5&&Math.abs(lateral)>2)name='DEAD LEG';
    else if(screen&&distance>3.3)name='STUTTER GO';
    else if(distance>=2.1&&closing>4&&athleticism>=70)name='ROCKER STEP';
    if(last===name&&distance>=2.1&&!nearSideline)name=name==='HARD CUT'?'DEAD LEG':'HARD CUT';
    const shape={SPIN:[.62,.86,1],HESITATION:[.56,.68,.75],'HARD CUT':[.5,.86,1],'SPEED CUT':[.38,.97,.65],'SHOULDER DIP':[.42,.94,.45],'DEAD LEG':[.52,.83,.85],'STUTTER GO':[.58,.8,.6],'ROCKER STEP':[.6,.8,.9]}[name];
    return {name,side,duration:shape[0],retention:shape[1],impulse:shape[2]};
  }
  function catchAnimation({kind,screen=false,oneHand=false,catching=60,side=1}){
    const name=kind==='TOE TAP'?'SIDELINE DRAG':kind==='HIGH POINT'?'HIGH-POINT CLAMP':kind==='LOW CATCH'?'LOW SCOOP':
      kind==='CONTACT CATCH'?'BODY SHIELD':oneHand&&catching>=75?'REACH & TUCK':screen?'SCREEN TURN-UP':
      kind==='BACK SHOULDER'?'SPIN & SECURE':kind==='OVER THE SHOULDER'?'BASKET CATCH':'SNATCH & TUCK';
    return {name,side,duration:name==='SIDELINE DRAG'?.38:name==='BODY SHIELD'?.48:.6};
  }
  identities.push(
    {name:'Gamblers',schemes:['robber','press','pressure'],speed:.1,strength:0,hands:.03,depth:-1,discipline:.15},
    {name:'Bullies',schemes:['press','bracket'],speed:-.15,strength:14,hands:0,depth:0,discipline:.5},
    {name:'Track Team',schemes:['match','twohigh'],speed:.45,strength:-12,hands:0,depth:0,discipline:.45},
    {name:'Veterans',schemes:['disguise','match','deep'],speed:-.2,strength:2,hands:.03,depth:1,discipline:.95},
    {name:'Heavy Hitters',schemes:['pressure','underzone'],speed:-.2,strength:16,hands:-.02,depth:0,discipline:.55});
  identities[5].discipline=.25;
  // Evasion controls recognition; the move's own rating controls execution.
  function escapeOdds(p,kind,defenderStrength=65,technique=.5,screen=false){
    const skill=clamp(rating(p,kind==='hurdle'?'athleticism':'strength'),0,1.4),evade=clamp(rating(p,'evasion'),0,1.4);
    const defense=kind==='hurdle'?clamp(technique,0,1):clamp(defenderStrength/100,0,1.4);
    return {timing:clamp(.22+evade*.64+(screen?.20:0),.22,.97),
      success:clamp(.18+skill*.68-defense*.23+(screen?.28:0),.10,.94)};
  }
  const api={optionDepths,isOption,motions,motionPath,motionSample,optionDecision,screenOutlet,screenRead,evasionMove,catchAnimation,escapeOdds,categories,category,playstyle,pumpChance,weather,lane,concepts,blockOutcome,physique,identities,identity,signature,movement,cleanHistory,tendencies,remember,chooseCoverage,placement,pursuitTime,tackleTechnique,sweptContact};
  api.tactics=root.QBTactics||(typeof require==='function'?require('./tactics.js'):null);
  if(typeof module!=='undefined')module.exports=api;root.QBVariety=api;
})(typeof globalThis!=='undefined'?globalThis:this);
