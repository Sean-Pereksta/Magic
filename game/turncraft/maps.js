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
  function generate(id='classic',seed=1){
    if(!layouts[id])id='classic';
    const layout=clone(layouts[id]),result={id,width:WIDTH,height:HEIGHT,terrain:null,...layout};
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
