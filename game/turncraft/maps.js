(function(root){
  'use strict';
  const WIDTH=42,HEIGHT=30;
  const palette=(accent,ground,rock,lane,extra={})=>({accent,background:'#070d17',ground,sand:ground,dust:ground,grass:ground,moss:ground,ruins:lane,lane,expansion:lane,rock,crystal:rock,start:'#f5f7ff',objective:'#ffd166',...extra});
  const definitions=[
    {id:'classic',name:'Classic Frontier',subtitle:'The original battlefield',description:'Broad central lanes, familiar natural expansions and an open three-way contest.',theme:'frontier',palette:palette('#77c6ff','#24372d','#586271','#405365')},
    {id:'ember-rift',name:'Ember Rift',subtitle:'Volcanic bridges',description:'A diagonal volcanic fault divides the battlefield. Cross the basalt bridges or secure a flank.',theme:'volcanic',palette:palette('#ff9367','#3c2325','#8f4938','#69504a',{sand:'#59352c',dust:'#472326',crystal:'#c46348',expansion:'#72523e'})},
    {id:'frostline-basin',name:'Frostline Basin',subtitle:'Frozen ring and basin',description:'A frozen ridge encircles the central basin, with broad gateways and a fast outer circuit.',theme:'ice',palette:palette('#9de7ff','#294b5f','#8cb9c9','#507f96',{sand:'#426378',dust:'#344e69',crystal:'#a4e5ef',expansion:'#477b86'})},
    {id:'verdant-divide',name:'Verdant Divide',subtitle:'Winding jungle passes',description:'A twisting forest divide shelters expansions. Three cut-throughs link the long jungle flanks.',theme:'jungle',palette:palette('#9fda78','#234c37','#315d3b','#71815a',{grass:'#2e603e',moss:'#1c3c31',crystal:'#4a8051',expansion:'#627849'})},
    {id:'shattered-dunes',name:'Shattered Dunes',subtitle:'Ridges and open flats',description:'Weathered ridges break up a wide desert. Sweep the open flats or contest the rich central oasis.',theme:'desert',palette:palette('#f4d397','#796344','#a6916a','#aa9162',{sand:'#8f7851',dust:'#67543c',crystal:'#c7a774',expansion:'#87905b'})},
    {id:'orbital-foundry',name:'Orbital Foundry',subtitle:'Industrial rooms and halls',description:'Linked manufacturing halls create defined fronts, alternate corridors and protected expansion rooms.',theme:'industrial',palette:palette('#b0b8ff','#303c51','#171f30','#63718a',{ruins:'#48536c',dust:'#2c3448',crystal:'#455879',expansion:'#667365'})}
  ];
  const point=(x,y)=>({x,y});
  const site=(x,y,rotation,naturalFor=null,rich=false)=>({x,y,rotation,naturalFor,rich});
  const neutral=(x,y,type='neutralWatchtower')=>({x,y,type});
  const conquest=(x,y,label)=>({x,y,label,name:label});
  // Legacy dimensions and spawn coordinates are intentionally unchanged. Campaigns
  // and old multiplayer snapshots still use the original 42 x 30 layouts.
  for(const definition of definitions)Object.assign(definition,{width:WIDTH,height:HEIGHT,capacity:3,playerCount:3,category:'3-player',legacy:true});
  const strategicDefinitions=[
    {id:'ashen-causeway',name:'Ashen Causeway',subtitle:'Causeway and two volcanic flanks',description:'Opposing fortified calderas face a central basalt crossing; northern and southern passes bypass its defenses.',theme:'volcanic',width:64,capacity:2,palette:palette('#ff9c70','#382428','#984b40','#765447',{dust:'#422529',sand:'#654432',crystal:'#c76543',expansion:'#86704c'})},
    {id:'ghostwood-passage',name:'Ghostwood Passage',subtitle:'Three winding forest trails',description:'Sheltered woodland bases open into winding trails, remote side clearings and a broad central battlefield.',theme:'jungle',width:64,capacity:2,palette:palette('#b3db8a','#243e31','#315039','#697c56',{grass:'#335a3c',moss:'#233b30',crystal:'#59815c',expansion:'#8a8e62'})},
    {id:'triad-basin',name:'Triad Basin',subtitle:'Triangular fronts and outer circuit',description:'Three mountain strongholds surround a broad basin. Pairwise outer roads reward scouting and indirect attacks.',theme:'desert',width:80,capacity:3,palette:palette('#f2d397','#766045','#a08d6b','#aa9569',{sand:'#8b7553',dust:'#655441',crystal:'#c2a476',expansion:'#939965'})},
    {id:'obsidian-crown',name:'Obsidian Crown',subtitle:'Plateaus around a broken crown',description:'Three volcanic plateaus descend to a fortified inner ring, with rich central resources and a separate outer circuit.',theme:'volcanic',width:80,capacity:3,palette:palette('#f8a077','#37272c','#80605a','#735a4d',{sand:'#544034',dust:'#422a31',crystal:'#aa6950',expansion:'#89734e'})},
    {id:'fourfold-divide',name:'Fourfold Divide',subtitle:'Four corners and mountain gateways',description:'Spacious corner strongholds, open staging fields, cross-shaped mountain divides and neighboring flank roads.',theme:'frontier',width:96,capacity:4,palette:palette('#93d7ef','#2c443d','#63767b','#73856f',{grass:'#385448',moss:'#273c35',crystal:'#8caaa6',expansion:'#a19c6a'})},
    {id:'rift-bastion',name:'Rift Bastion',subtitle:'Military sectors, bridges and halls',description:'Four fortified compounds connect through assembly halls, narrow bridges and a second perimeter road around the control hub.',theme:'industrial',width:96,capacity:4,palette:palette('#b3beff','#303c51','#1c2637','#73819b',{ruins:'#515e78',dust:'#354054',crystal:'#566d84',expansion:'#83917c'})},
    {id:'ashen-confluence',name:'Ashen Confluence',subtitle:'Six commanders around a volcanic basin',description:'Six roomy caldera bases, a continuous flank circuit and six contested bridgehead expansions surround a large central basin.',theme:'volcanic',width:112,capacity:6,palette:palette('#f4a775','#402b2b','#8b574c','#8a6a50',{sand:'#68513c',dust:'#4e3030',crystal:'#c77d50',expansion:'#a38c58'})},
    {id:'rift-dominion',name:'Rift Dominion',subtitle:'Eight sectors and two orbital circuits',description:'Eight isolated compounds feed broad staging halls and two connected circuits, with four rich inner assembly sectors.',theme:'industrial',width:128,capacity:8,palette:palette('#bac6ff','#344155','#1f293a','#7888a0',{ruins:'#54627b',dust:'#3e485b',crystal:'#668197',expansion:'#93a18a'})}
  ];
  for(const definition of strategicDefinitions){definition.height=definition.width;definition.playerCount=definition.capacity;definition.category=definition.capacity>4?'expanded':definition.capacity+'-player';definition.legacy=false;definitions.push(definition)}
  const layouts={
    classic:{starts:[point(6,6),point(35,6),point(21,24)],expansions:[site(14,11,.35,0),site(28,11,2.78,1),site(21,17,-1.55,2),site(8,22,.2),site(34,22,2.95),site(21,7,1.55,null,true)],conquestSites:[conquest(21,14,'Central Impact'),conquest(8,20,'West Impact'),conquest(34,20,'East Impact')],neutralSites:[neutral(21,9),neutral(21,20),neutral(9,15,'mineralCache'),neutral(33,15,'gasCache'),neutral(17,14,'neutralRock'),neutral(25,14,'neutralCrystal'),neutral(21,17,'neutralRock')],relic:point(21,14)},
    'ember-rift':{starts:[point(6,5),point(35,5),point(21,24)],expansions:[site(10,12,0,0),site(31,12,Math.PI,1),site(21,18,-Math.PI/2,2),site(6,23,0),site(35,23,Math.PI),site(21,5,Math.PI/2,null,true)],conquestSites:[conquest(15,9,'West Bridge'),conquest(27,9,'East Bridge'),conquest(21,14,'Rift Crossing')],neutralSites:[neutral(14,18),neutral(28,18),neutral(6,16,'mineralCache'),neutral(35,16,'gasCache'),neutral(17,11,'neutralRock'),neutral(25,11,'neutralCrystal'),neutral(18,15,'neutralRock')],relic:point(21,14)},
    'frostline-basin':{starts:[point(6,6),point(35,23),point(6,23)],expansions:[site(12,10,0,0),site(29,19,Math.PI,1),site(12,19,0,2),site(32,6,Math.PI),site(21,5,Math.PI/2),site(21,25,-Math.PI/2,null,true)],conquestSites:[conquest(21,10,'North Gate'),conquest(16,15,'West Basin'),conquest(26,15,'East Basin')],neutralSites:[neutral(10,14),neutral(32,14),neutral(15,5,'mineralCache'),neutral(27,25,'gasCache'),neutral(18,19,'neutralRock'),neutral(24,19,'neutralCrystal'),neutral(21,17,'neutralRock')],relic:point(21,14)},
    'verdant-divide':{starts:[point(6,6),point(35,6),point(35,24)],expansions:[site(11,12,0,0),site(29,11,Math.PI,1),site(29,20,Math.PI,2),site(7,23,0),site(20,23,Math.PI),site(21,5,Math.PI/2,null,true)],conquestSites:[conquest(15,8,'Canopy Pass'),conquest(21,14,'Central Cut'),conquest(24,20,'South Pass')],neutralSites:[neutral(14,18),neutral(33,15),neutral(6,16,'mineralCache'),neutral(25,6,'gasCache'),neutral(17,11,'neutralRock'),neutral(24,12,'neutralCrystal'),neutral(17,20,'neutralRock')],relic:point(21,14)},
    'shattered-dunes':{starts:[point(6,15),point(35,6),point(35,24)],expansions:[site(13,15,0,0),site(27,9,Math.PI,1),site(27,21,Math.PI,2),site(10,5,0),site(10,25,-Math.PI/2),site(21,15,Math.PI/2,null,true)],conquestSites:[conquest(19,7,'North Flat'),conquest(23,15,'Oasis'),conquest(19,23,'South Flat')],neutralSites:[neutral(18,11),neutral(18,19),neutral(7,9,'mineralCache'),neutral(34,15,'gasCache'),neutral(24,5,'neutralRock'),neutral(25,25,'neutralCrystal'),neutral(30,15,'neutralRock')],relic:point(21,14)},
    'orbital-foundry':{starts:[point(6,6),point(35,6),point(21,24)],expansions:[site(10,13,0,0),site(31,13,Math.PI,1),site(21,18,-Math.PI/2,2),site(6,24,0),site(35,24,Math.PI),site(21,5,Math.PI/2,null,true)],conquestSites:[conquest(15,14,'West Assembly'),conquest(27,14,'East Assembly'),conquest(21,11,'Control Hall')],neutralSites:[neutral(16,20),neutral(26,20),neutral(13,5,'mineralCache'),neutral(29,5,'gasCache'),neutral(16,9,'neutralRock'),neutral(26,9,'neutralCrystal'),neutral(21,15,'neutralRock')],relic:point(21,14)}
  };
  function clone(value){return JSON.parse(JSON.stringify(value))}
  function hash(seed,x,y){let n=(seed^Math.imul(x+1,374761393)^Math.imul(y+1,668265263))>>>0;n=Math.imul(n^(n>>>13),1274126177);return ((n^(n>>>16))>>>0)/4294967296}
  function strategicMap(definition,seed){
    const {id,width,height,capacity}=definition,s=Number(seed)>>>0,center=point(Math.floor(width/2),Math.floor(height/2));
    const result={id,width,height,capacity,playerCount:capacity,category:definition.category,topologyVersion:2,terrain:[],starts:[],bases:[],expansions:[],conquestSites:[],neutralSites:[],relic:{...center},chokes:[],routes:[],forwardPositions:[]};
    const floors=definition.theme==='industrial'?['ruins','dust','ground']:definition.theme==='jungle'?['grass','moss','ground']:definition.theme==='desert'?['sand','dust','ground']:['ground','sand','dust'];
    // The seed changes surface variation, never barriers or strategic geometry.
    // Thus every peer receives the same collision field even with a zero seed.
    for(let y=0;y<height;y++)for(let x=0;x<width;x++)result.terrain.push({x,y,type:hash(s,x,y)>.88?'crystal':'rock',blocked:true});
    const tile=(x,y)=>x>=0&&y>=0&&x<width&&y<height?result.terrain[y*width+x]:null;
    const paint=(x,y,type,blocked=false)=>{const t=tile(x,y);if(t){t.type=type==='floor'?floors[Math.floor(hash(s,x,y)*floors.length)]:type;t.blocked=blocked}};
    const disk=(p,r,type='floor',blocked=false)=>{for(let y=Math.floor(p.y-r);y<=Math.ceil(p.y+r);y++)for(let x=Math.floor(p.x-r);x<=Math.ceil(p.x+r);x++)if((x-p.x)**2+(y-p.y)**2<=r*r)paint(x,y,type,blocked)};
    const rect=(left,top,right,bottom,type='ruins',blocked=false)=>{for(let y=top;y<=bottom;y++)for(let x=left;x<=right;x++)paint(x,y,type,blocked)};
    const line=(a,b,width=5,type='lane')=>{const half=Math.floor(width/2),steps=Math.max(Math.abs(b.x-a.x),Math.abs(b.y-a.y))*2||1;for(let i=0;i<=steps;i++){const x=Math.round(a.x+(b.x-a.x)*i/steps),y=Math.round(a.y+(b.y-a.y)*i/steps);rect(x-half,y-half,x+half,y+half,type)}};
    const path=(name,role,points,width=5)=>{const route={id:name,role,width,points:points.map(p=>({...p}))};result.routes.push(route);for(let i=1;i<points.length;i++)line(points[i-1],points[i],width);return route};
    const offset=(p,angle,r)=>point(Math.round(p.x+Math.cos(angle)*r),Math.round(p.y+Math.sin(angle)*r));
    const lerp=(a,b,t)=>point(Math.round(a.x+(b.x-a.x)*t),Math.round(a.y+(b.y-a.y)*t));
    const choke=(name,p,role='pass',width=5,angle=0,owner=null)=>{const q={id:name,...p,role,width,owner,staging:[offset(p,angle,5),offset(p,angle+Math.PI,5)]};result.chokes.push(q);return q};
    const expansion=(p,owner=null,rich=false,rotation=0)=>{const q={...site(p.x,p.y,rotation,owner,rich),role:owner===null?'contested':'natural'};result.expansions.push(q);disk(p,4.5,'expansion');return q};
    const objective=(p,label)=>{result.conquestSites.push(conquest(p.x,p.y,label));disk(p,3,'ruins')};
    const base=(p,index)=>{const angle=Math.atan2(center.y-p.y,center.x-p.x),entrances=[-1,1].map((side,n)=>{const bearing=angle+side*Math.PI*.28,gate=offset(p,bearing,11),staging=offset(p,bearing,18);return {...gate,id:'base-'+index+'-gate-'+n,width:5,role:n?'flank':'main',staging,bearing}});const b={...p,player:index,radius:8.5,entrances};result.starts.push({...p,rotation:angle+Math.PI});result.bases.push(b);return b};
    const connectBase=(b)=>{for(const e of b.entrances)path(e.id,e.role,[b,e.staging],e.width)};
    const ring=(name,radius,width,vertices=24,angle=0)=>{const points=[];for(let i=0;i<=vertices;i++)points.push(offset(center,angle+i*Math.PI*2/vertices,radius));return path(name,'flank',points,width)};
    if(id==='ashen-causeway'){
      const a=base(point(12,32),0),b=base(point(51,32),1);result.bases.forEach(connectBase);
      // A single ridge has three crossings. The outer crossings are long flanks,
      // and the exposed central causeway has a broad army assembly area.
      disk(center,8,'dust');disk(point(27,32),5);disk(point(37,32),5);
      path('north-pass','flank',[a.entrances[0].staging,point(25,12),point(38,12),b.entrances[1].staging],3);
      path('south-pass','flank',[a.entrances[1].staging,point(25,51),point(38,51),b.entrances[0].staging],5);
      path('central-causeway','main',[a.entrances[0].staging,point(27,25),point(27,32),center,point(37,32),point(37,25),b.entrances[1].staging],5);
      path('west-muster','main',[a.entrances[1].staging,point(27,40),point(27,32)],5);
      path('east-muster','main',[b.entrances[0].staging,point(37,40),point(37,32)],5);
      expansion(point(26,19),0,false,-Math.PI/2);expansion(point(37,19),1,false,-Math.PI/2);
      path('north-economy','expansion',[point(26,19),point(26,12),point(37,12),point(37,19)],5);
      for(const p of [point(26,50),point(37,50)])expansion(p);expansion(center,null,true);
      choke('basalt-causeway',center,'main',5);choke('northern-lava-pass',point(29,12),'flank',3);choke('southern-lava-pass',point(29,51),'flank',5);
      objective(point(32,12),'North Basalt Pass');objective(point(32,51),'South Basalt Pass');objective(center,'Ashen Crossing');
      result.forwardPositions.push(point(27,27),point(37,37));
    }else if(id==='ghostwood-passage'){
      const a=base(point(12,12),0),b=base(point(51,51),1);result.bases.forEach(connectBase);disk(center,10,'grass');
      path('canopy-trail','flank',[a.entrances[0].staging,point(43,10),point(53,20),b.entrances[1].staging],3);
      path('moss-trail','flank',[a.entrances[1].staging,point(10,43),point(20,53),b.entrances[0].staging],5);
      path('clearing-road','main',[a.entrances[0].staging,point(29,22),center,point(41,34),b.entrances[1].staging],7);
      path('western-switchback','main',[a.entrances[1].staging,point(22,29),center],5);
      path('eastern-switchback','main',[b.entrances[0].staging,point(34,41),center],5);
      expansion(point(29,12),0,false,0);expansion(point(34,51),1,false,Math.PI);expansion(point(10,43));expansion(point(53,20));expansion(center,null,true);
      choke('canopy-narrows',point(46,13),'flank',3,Math.PI/4);choke('moss-bend',point(14,47),'flank',5,Math.PI/4);choke('west-clearing-mouth',point(22,29),'main',5,Math.atan2(3,10));choke('east-clearing-mouth',point(41,34),'main',7,0);
      objective(center,'Ghostwood Clearing');objective(point(10,43),'Moss Clearing');objective(point(53,20),'Canopy Clearing');result.forwardPositions.push(point(25,25),point(39,39));
    }else if(capacity===3){
      const startRadius=29,angles=[-Math.PI/2,Math.PI/6,Math.PI*5/6],crown=id==='obsidian-crown';
      angles.forEach((angle,i)=>base(offset(center,angle,startRadius),i));result.bases.forEach(connectBase);
      if(crown){ring('crown-inner-ring',13,5,18,-Math.PI/2);disk(center,6,'dust')}else disk(center,13,'sand');
      for(const b of result.bases){
        const angle=Math.atan2(center.y-b.y,center.x-b.x),left=b.entrances[0].staging,right=b.entrances[1].staging;
        const natural=offset(left,angle,4),inner=offset(center,angle+Math.PI,crown?13:9);
        expansion(natural,b.player,false,angle+Math.PI);path('natural-'+b.player,'expansion',[left,natural,inner],5);
        path('basin-front-'+b.player,'main',[right,offset(inner,angle+Math.PI/2,6),inner],crown?3:7);
        if(crown)path('crown-descent-'+b.player,'main',[inner,center],3);
        const next=result.bases[(b.player+1)%3],outer=offset(center,angles[b.player]+Math.PI/3,30);path('outer-link-'+b.player,'flank',[left,outer,next.entrances[1].staging],5);
        const contested=offset(center,angle+Math.PI+Math.PI/3,23);expansion(contested,null,true);objective(offset(center,angle+Math.PI,crown?13:9),['North Front','Southeast Front','Southwest Front'][b.player]);
        const bend=offset(inner,angle+Math.PI/2,6);choke('basin-gateway-'+b.player,lerp(right,bend,.5),'main',crown?3:7,Math.atan2(bend.y-right.y,bend.x-right.x));result.forwardPositions.push({...inner,player:b.player,role:'basin-staging'});
      }
      if(crown)expansion(center,null,true);
    }else if(capacity===4){
      const limit=width-1,industrial=id==='rift-bastion',rotate=(p,n)=>{for(let i=0;i<n;i++)p=point(limit-p.y,p.x);return p};
      for(let n=0;n<4;n++)base(rotate(point(14,14),n),n);result.bases.forEach(connectBase);
      if(industrial){rect(39,39,56,56);for(let n=0;n<4;n++){const a=rotate(point(27,27),n),b=rotate(point(68,27),n);path('sector-circuit-'+n,'flank',[a,b],5)}}else disk(center,13,'grass');
      for(let n=0;n<4;n++){
        const tr=p=>rotate(p,n),b=result.bases[n];
        if(industrial){for(const box of [[27,9,39,23],[9,27,23,39],[29,29,38,38]]){const corners=[tr(point(box[0],box[1])),tr(point(box[2],box[3]))];rect(Math.min(corners[0].x,corners[1].x),Math.min(corners[0].y,corners[1].y),Math.max(corners[0].x,corners[1].x),Math.max(corners[0].y,corners[1].y))}}
        else disk(tr(point(33,33)),10,'floor');
        path('east-sector-'+n,'main',[b.entrances[0].staging,tr(point(33,20)),tr(point(33,33))],7);
        path('south-sector-'+n,'main',[b.entrances[1].staging,tr(point(20,33)),tr(point(33,33))],7);
        path('neighbor-flank-'+n,'flank',[tr(point(32,14)),tr(point(47,14)),tr(point(63,14))],5);
        path('central-bridge-'+n,'main',[tr(point(33,33)),tr(point(47,33)),center],industrial?5:7);
        if(industrial)path('secondary-hall-'+n,'flank',[tr(point(20,33)),tr(point(27,33)),tr(point(27,47)),tr(point(33,47)),center],5);
        expansion(tr(point(33,20)),n,false,n*Math.PI/2);expansion(tr(point(47,14)),null,true);expansion(tr(point(33,33)));
        choke('inner-bridge-'+n,tr(point(43,33)),'main',industrial?5:7,n*Math.PI/2);choke('outer-pass-'+n,tr(point(47,14)),'flank',5,n*Math.PI/2);
        objective(tr(point(47,33)),['North Bridge','East Bridge','South Bridge','West Bridge'][n]);result.forwardPositions.push({...tr(point(35,35)),player:n,role:'forward-production'});
      }
      expansion(center,null,true);
    }else{
      const industrial=capacity===8,startRadius=capacity===6?42:49,angles=Array.from({length:capacity},(_,i)=>-Math.PI/2+i*Math.PI*2/capacity);
      angles.forEach((angle,i)=>base(offset(center,angle,startRadius),i));result.bases.forEach(connectBase);ring('inner-command-circuit',capacity===6?20:23,7,capacity*4,-Math.PI/2);
      if(industrial)rect(center.x-10,center.y-10,center.x+10,center.y+10);else disk(center,13,'dust');
      for(const b of result.bases){
        const angle=angles[b.player],inward=angle+Math.PI,next=result.bases[(b.player+1)%capacity],stage=b.entrances[1].staging,natural=offset(b.entrances[0].staging,inward,capacity===8?(b.player%2?6:2):4),inner=offset(center,angle,capacity===6?20:23),outer=offset(center,angle+Math.PI/capacity,startRadius);
        expansion(natural,b.player,false,angle);disk(inner,industrial?7:8);path('sector-front-'+b.player,'main',[b.entrances[0].staging,natural,inner,center],7);path('sector-flank-'+b.player,'flank',[stage,inner],5);
        // Clockwise outer roads connect neighbors without forcing all armies to
        // cross the middle; every sector has access to both strategic circuits.
        path('perimeter-road-'+b.player,'flank',[b.entrances[0].staging,outer,next.entrances[1].staging],5);expansion(outer,null,true);
        choke('sector-bridge-'+b.player,lerp(stage,inner,.55),'main',5,Math.atan2(inner.y-stage.y,inner.x-stage.x));result.forwardPositions.push({...inner,player:b.player,role:'command-staging'});
        if(b.player%2===0)objective(inner,'Command Sector '+(b.player+1));
      }
      if(industrial)for(let i=0;i<4;i++)expansion(offset(center,Math.PI/4+i*Math.PI/2,13),null,true);else expansion(center,null,true);
    }
    if(id==='ashen-causeway'){rect(30,19,33,44,'rock',true);line(point(27,32),point(37,32),5)}
    // Author the base walls last so adjacent clearings cannot accidentally add
    // a third entrance. The 17-tile interior holds economy, production and armies.
    for(const b of result.bases){disk(b,11.5,'rock',true);disk(b,b.radius,'floor');for(const e of b.entrances){line(b,e.staging,e.width);const q=choke(e.id,e,'base',e.width,e.bearing,b.player);q.staging=[lerp(b,e,.58),{...e.staging}]}}
    // Objective structures sit off the traffic line; there are no destructible
    // neutral blockers hidden inside the narrow passages.
    for(const p of result.forwardPositions){disk(p,3.5,'floor');const q=point(p.x+1,p.y-2);if(tile(q.x,q.y)&&!tile(q.x,q.y).blocked)result.neutralSites.push(neutral(q.x,q.y))}
    // Close any decorative fragments disconnected by a base wall. Authored
    // routes are tested with both 1 x 1 and 2 x 2 terrain footprints.
    const first=result.starts[0],seen=new Uint8Array(width*height),queue=[first.y*width+first.x];seen[queue[0]]=1;
    for(let i=0;i<queue.length;i++){const key=queue[i],x=key%width,y=Math.floor(key/width);for(const [dx,dy] of [[1,0],[-1,0],[0,1],[0,-1]]){const t=tile(x+dx,y+dy);if(!t||t.blocked)continue;const k=t.y*width+t.x;if(!seen[k]){seen[k]=1;queue.push(k)}}}
    for(let i=0;i<result.terrain.length;i++)if(!result.terrain[i].blocked&&!seen[i]){result.terrain[i].type='rock';result.terrain[i].blocked=true}
    return result;
  }
  function generate(id='classic',seed=1){
    const strategic=strategicDefinitions.find(map=>map.id===id);if(strategic)return strategicMap(strategic,seed);
    if(!layouts[id])id='classic';
    const layout=clone(layouts[id]),result={id,width:WIDTH,height:HEIGHT,capacity:3,playerCount:3,category:'3-player',terrain:null,...layout};
    if(id==='classic')return result;
    const terrain=[],s=Number(seed)>>>0;
    const floors=id==='ember-rift'?['ground','dust','sand']:id==='frostline-basin'?['ground','sand','dust']:id==='verdant-divide'?['grass','moss','ground']:id==='shattered-dunes'?['sand','dust','ground']:['ruins','ground','dust'];
    for(let y=0;y<HEIGHT;y++)for(let x=0;x<WIDTH;x++){
      const noise=hash(s,x,y),type=id==='orbital-foundry'?'rock':floors[Math.min(floors.length-1,Math.floor(noise*floors.length))];
      terrain.push({x,y,type,blocked:type==='rock'});
    }
    const tile=(x,y)=>x>=0&&y>=0&&x<WIDTH&&y<HEIGHT?terrain[y*WIDTH+x]:null;
    const paint=(x,y,type,blocked=false)=>{const t=tile(x,y);if(t){t.type=type;t.blocked=blocked}};
    const circle=(x,y,r,type='lane',blocked=false)=>{for(let yy=Math.floor(y-r);yy<=Math.ceil(y+r);yy++)for(let xx=Math.floor(x-r);xx<=Math.ceil(x+r);xx++)if(Math.hypot(xx-x,yy-y)<=r)paint(xx,yy,type,blocked)};
    const rect=(left,top,right,bottom,type='ruins',blocked=false)=>{for(let y=top;y<=bottom;y++)for(let x=left;x<=right;x++)paint(x,y,type,blocked)};
    const ellipse=(x,y,rx,ry,type='rock')=>{for(let yy=Math.floor(y-ry);yy<=Math.ceil(y+ry);yy++)for(let xx=Math.floor(x-rx);xx<=Math.ceil(x+rx);xx++)if((xx-x)**2/rx**2+(yy-y)**2/ry**2<=1)paint(xx,yy,type,true)};
    // Join diagonal raster steps orthogonally, preserving a full three-tile front.
    const route=(...points)=>{for(let n=1;n<points.length;n++){const a=points[n-1],b=points[n],steps=Math.max(Math.abs(a.x-b.x),Math.abs(a.y-b.y))*2||1;let previous=null;for(let i=0;i<=steps;i++){const x=Math.round(a.x+(b.x-a.x)*i/steps),y=Math.round(a.y+(b.y-a.y)*i/steps);if(previous&&previous.x!==x&&previous.y!==y)rect(x-1,previous.y-1,x+1,previous.y+1,'lane');rect(x-1,y-1,x+1,y+1,'lane');previous={x,y}}}};
    const [a,b,c]=layout.starts,[na,nb,nc,west,east,rich]=layout.expansions;
    if(id==='ember-rift'){
      for(let y=0;y<HEIGHT;y++){const x=Math.round(16+y*.3);rect(x-3,y,x+3,y,(y%4===0?'crystal':'rock'),true)}
      ellipse(3,15,2.7,3);ellipse(38,16,2.5,3,'crystal');ellipse(14,25,2.8,2);
      route(a,na,point(15,9),point(24,9),nb,b);route(na,point(13,18),point(22,19),nc,c);route(na,west,point(13,24),c,point(29,24),east,nb);route(rich,point(25,8),point(27,14),nb);route(point(24,9),layout.relic,nc);
    }else if(id==='frostline-basin'){
      for(let y=0;y<HEIGHT;y++)for(let x=0;x<WIDTH;x++){const r=((x-21)/10)**2+((y-14)/7)**2;if(r>.55&&r<1.6)paint(x,y,(x+y)%4?'rock':'crystal',true)}
      ellipse(3,14,2,3,'crystal');ellipse(38,14,2,3,'crystal');ellipse(21,1,4,1.5);
      route(a,na,point(13,6),rich,west,point(34,12),nb,b);route(na,point(15,14),layout.relic,point(27,14),nb);route(na,nc,c);route(c,point(12,25),east,point(30,25),b);route(rich,point(21,10),layout.relic,east);route(nc,point(16,15),layout.relic);
    }else if(id==='verdant-divide'){
      for(let y=0;y<HEIGHT;y++){const x=Math.round(20+Math.sin(y*.34)*4);rect(x-4,y,x+4,y,(y%5?'rock':'crystal'),true)}
      ellipse(4,17,2,3);ellipse(36,16,2,3);ellipse(14,25,2,2.5);
      route(a,na,point(14,18),west,east,nc,c);route(b,nb,nc);route(na,point(15,8),rich,nb);route(na,point(14,14),layout.relic,point(29,14),nb);route(east,point(24,20),nc);route(west,point(11,25),east);
    }else if(id==='shattered-dunes'){
      for(const [x,y,rx,ry] of [[16,3,3,1.5],[24,4,3,1.5],[17,26,3,1.5],[25,27,3,1.5],[20,10,4.5,1.7],[20,20,4.5,1.7],[3,4,2,3],[39,15,1.5,4]])ellipse(x,y,rx,ry,x%2?'crystal':'rock');
      route(a,na,rich,nb,b);route(rich,nc,c);route(a,west,point(19,7),nb);route(a,east,point(19,23),nc);route(west,point(17,5),point(24,7),b);route(east,point(18,26),point(28,25),c);route(layout.relic,point(30,15));
    }else{
      for(const room of [[2,2,12,10],[29,2,39,10],[15,21,27,28],[5,10,15,17],[26,10,36,17],[15,10,26,19],[3,20,10,27],[31,20,38,27],[17,2,25,8]])rect(...room);
      route(a,point(10,8),na,point(15,14),layout.relic,point(26,14),nb,point(31,8),b);route(c,nc,layout.relic,rich);route(a,rich,b);route(na,point(6,20),west,point(13,24),c,point(29,24),east,point(35,20),nb);
    }
    // Explicit clearings protect all economy and objective placements from authored walls.
    for(const start of layout.starts)circle(start.x,start.y,4.6,'ground');
    for(const expansion of layout.expansions)circle(expansion.x,expansion.y,2.9,'expansion');
    for(const objective of [...layout.conquestSites,layout.relic]){circle(objective.x,objective.y,2.3,'ruins');route(objective,layout.relic)}
    for(const objective of layout.neutralSites){circle(objective.x,objective.y,1.5,'ruins');const closest=[...layout.starts,...layout.expansions,layout.relic].sort((p,q)=>Math.hypot(p.x-objective.x,p.y-objective.y)-Math.hypot(q.x-objective.x,q.y-objective.y))[0];route(objective,closest)}
    // Close decorative pockets so every passable tile belongs to the playable battlefield.
    const seen=new Set(),queue=[layout.starts[0]];seen.add(queue[0].y*WIDTH+queue[0].x);
    for(let i=0;i<queue.length;i++){const p=queue[i];for(const [dx,dy] of [[1,0],[-1,0],[0,1],[0,-1]]){const t=tile(p.x+dx,p.y+dy),key=t&&t.y*WIDTH+t.x;if(!t||t.blocked||seen.has(key))continue;seen.add(key);queue.push(t)}}
    for(const t of terrain){if(!t.blocked&&!seen.has(t.y*WIDTH+t.x)){t.type='rock';t.blocked=true}else if(id==='orbital-foundry'&&t.type==='ruins'&&hash(s,t.x,t.y)>.65)t.type='dust'}
    result.terrain=terrain;return result;
  }
  root.TurncraftMaps={definitions,generate};
})(typeof window!=='undefined'?window:globalThis);
