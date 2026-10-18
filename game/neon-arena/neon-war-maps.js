/* Authored Cats vs Mice battlefields. Horizontal coordinates are logical units;
 * heights remain world metres, matching the arena's existing geometry helpers. */
(function (root) {
  'use strict';
  const SCALE = 1.45, EYE = 1.86, BODY = 2.39;
  const CATALOG = [
    { id:'rail_yard_nine', name:'Rail Yard Nine', desc:'Board open passenger cars, infiltrate the signal depot and cross the elevated freight bridge.', tag:'Rail depot · Train interiors', tileset:'derelict_station', accent:'#d1b873' },
    { id:'whiskerhold', name:'Whiskerhold Fortress', desc:'Breach a fortified compound through four gates, sheltered service approaches and the upper ramparts.', tag:'Fortress · Siege and infiltration', tileset:'breach_ring', accent:'#d5a75c' },
    { id:'floodgate', name:'Floodgate Sewers', desc:'Fight beneath the central aqueduct or climb interconnected pump galleries above the drainage channels.', tag:'Sewer complex · Layered combat', tileset:'drowned_powerworks', accent:'#61c8c2' },
    { id:'cold_storage', name:'Cold Storage District', desc:'Four enterable refrigeration buildings surround a cargo avenue and overhead loading gantry.', tag:'Warehouse district · Indoor flanks', tileset:'black_ice_cryolab', accent:'#9adbe9' },
    { id:'clockwork', name:'Clockwork Foundry', desc:'Capture production workshops, flank the turbine line and contest an elevated maintenance crossing.', tag:'Industrial works · Destructible cover', tileset:'drowned_powerworks', accent:'#ee9c62' },
    { id:'night_market', name:'Night Market Ruins', desc:'Thread connected shops and alleys beneath two rooftop routes overlooking a shattered market.', tag:'Urban ruins · Rooftop routes', tileset:'breach_ring', accent:'#bd98ee' }
  ].map(m=>Object.freeze({...m, preview:'./assets/campaign/maps/'+m.id+'.svg'}));
  const IDS=Object.freeze(CATALOG.map(m=>m.id));
  const MISSIONS={cold_storage:['mice_broken_convoy','cats_silent_outpost'],rail_yard_nine:['mice_behind_enemy_lines'],floodgate:['mice_operation_floodgate','cats_beneath_city','cats_underground_citadel'],whiskerhold:['mice_whiskerhold_rescue','mice_dominions_end'],clockwork:['cats_foundry_offensive'],night_market:['cats_night_market_hunt']};
  const get=id=>CATALOG.find(m=>m.id===id)||null;
  const point=(x,z,label='',y=0)=>({x,y,z,label});

  function createLayout(id) {
    const meta=get(id); if(!meta)throw new RangeError('Unknown battlefield: '+id);
    const l={...meta,version:2,walls:[],platforms:[],decks:[],ramps:[],props:[],ambient:[],buildings:[],windows:[],doors:[],cover:[],landmarks:[],overwatch:[],navPoints:[],patrolRoutes:[],missionSites:{},features:[],combatSpaces:[]};
    const wall=(x,z,w,d,h=5.4,tile='wall_01',y=0,extra={})=>{const p={x,z,w,d,h,tile,y,...extra};l.walls.push(p);return p;};
    const slab=(x,z,w,d,top,tile='floor_04',roof=false,accessible=false)=>l.platforms.push({x,z,w,d,y:top-.4,h:.4,tile,architecturalRoof:roof,authoredDeck:accessible});
    const deck=(x,z,w,d,top=5.4)=>l.decks.push({x,z,w,d,top,tile:'floor_04'});
    const ramp=(x,z,w,d,axis,fromHeight,toHeight)=>l.ramps.push({x,z,w,d,axis,fromHeight,toHeight,tile:'floor_03'});
    const prop=(prefab,x,z,w,d,h,tile='prop_02',y=0)=>l.props.push({prefab,x,z,w,d,h,tile,y,material:'metal'});
    const cover=(x,z,w,d,h=1.2,tile='wall_04')=>{wall(x,z,w,d,h,tile,0,{cover:true});l.cover.push(point(x,z,'Field barricade'));};
    const detail=(x,z,w,d,h,y=0,tile='trim_00')=>wall(x,z,w,d,h,tile,y,{detail:true});
    const feature=(label,x,z)=>l.features.push({label,x,z});
    const clearSpace=(label,x,z,w,d)=>l.combatSpaces.push({label,x,z,w,d});
    const light=(label,x,z,color,radius=9,intensity=.5,y=4.2)=>({label,x,y,z,color,radius,intensity});
    function lighting(ambient,sky,sunColor,zones,exposure=1.08){
      l.theme.sky=sky;l.theme.lighting={ambient,exposure,sunColor,sunDirection:[.45,.82,-.35],fogDensity:.0014,zones};
    }
    // Details use the same physical/render batches as architecture. Furniture
    // never substitutes an invisible collider, and light fixtures sit overhead.
    function ceilingStrip(x,z,w=4,y=4.8,tile='trim_01'){detail(x,z,w,.24,.12,y,tile);}
    function vent(x,z,w=3,d=2,y=5.8){detail(x,z,w,d,1.05,y,'prop_02');for(const dz of [-.6,0,.6])detail(x,z+dz,w+.12,.1,.12,y+1.05,'trim_01');}
    function rack(x,z){
      for(const dx of [-.6,.6])for(const dz of [-1.35,1.35])detail(x+dx,z+dz,.12,.12,3.7,0,'trim_00');
      for(const y of [.15,1.65,3.15])detail(x,z,1.4,3,.14,y,'prop_01');
      detail(x,z,1.05,2.3,1.15,.29,'prop_03');
    }
    // Each opening is assembled from real jambs, a sill and a lintel. There is
    // deliberately no invisible wall across a window or doorway.
    function face(x,z,length,axis,height,tile,openings,y=0){
      const sorted=openings.slice().sort((a,b)=>a.at-b.at),thickness=.6;
      const segment=(at,size,bottom,top)=>{if(size<.01||top-bottom<.01)return;wall(x+(axis==='x'?at:0),z+(axis==='z'?at:0),axis==='x'?size:thickness,axis==='z'?size:thickness,top-bottom,tile,y+bottom);};
      let cursor=-length/2;
      for(const o of sorted){const left=o.at-o.width/2;segment((cursor+left)/2,left-cursor,0,height);segment(o.at,o.width,0,o.bottom);segment(o.at,o.width,o.top,height);cursor=o.at+o.width/2;
        const p={x:x+(axis==='x'?o.at:0),z:z+(axis==='z'?o.at:0),y:y+o.bottom,top:y+o.top,width:o.width,axis};
        if(o.bottom===0)l.doors.push(p);else l.windows.push(p);
      }segment((cursor+length/2)/2,length/2-cursor,0,height);
    }
    function room(name,x,z,w,d,{height=5.4,tile='wall_01',roof=true,roofAccess=false,doors='z',windows=true,base=0,blindFaces=[]}={}){
      const door={at:0,width:Math.min(5,w-2,d-2),bottom:0,top:3.4};
      const win={at:0,width:Math.min(5.2,w-2,d-2),bottom:1.05,top:2.8};
      const openings=(length,hasDoor,allowWindow)=>{if(!hasDoor)return allowWindow?[win]:[];const ports=[door],width=Math.min(3.2,(length-door.width)/2-1.3);if(allowWindow&&width>=1.7)for(const side of [-1,1])ports.push({...win,at:side*(door.width/2+width/2+.6),width});return ports;};
      for(const side of [-1,1]){face(x,z+side*d/2,w,'x',height,tile,openings(w,doors.includes('z'),windows&&!blindFaces.includes(side<0?'north':'south')),base);face(x+side*w/2,z,d,'z',height,tile,openings(d,doors.includes('x'),windows&&!blindFaces.includes(side<0?'west':'east')),base);}
      if(roof)slab(x,z,w,d,base+height+.4,'upper_00',true,roofAccess);
      l.buildings.push({name,x,z,w,d,height,base,roofAccess});l.landmarks.push(point(x,z,name,base));
      l.navPoints.push(point(x,z,name,base));
      for(const side of [-1,1]){if(doors.includes('z')){l.navPoints.push(point(x,z+side*(d/2-1.4),'',base),point(x,z+side*(d/2+1.4),'',base));}if(doors.includes('x'))l.navPoints.push(point(x+side*(w/2-1.4),z,'',base),point(x+side*(w/2+1.4),z,'',base));}
      if(windows)for(const side of [-1,1])l.overwatch.push(point(x+side*(w/2-1.2),z,name+' firing window',base));
    }
    function sectors(names,coords){l.sectors=names.map((name,i)=>({id:'ABCDE'[i],name,...point(...coords[i]),radius:4.5,neighbors:[['B','C'],['A','C','D'],['A','B','D','E'],['B','C','E'],['C','D']][i]}));}
    function anchors(values){l.anchors=Object.fromEntries(Object.entries(values).map(([k,p])=>[k,point(p[0],p[1],p[2]||k,p[3]||0)]));}
    // Opposing headquarters are outside the central fighting positions. The
    // boundary is the existing 130m-wide arena, so old physics limits hold.
    l.headquarters={cats:{...point(-38,-37,'Cat Forward Headquarters'),spawns:[point(-38,-37),point(-32,-39),point(-40,-30)]},mice:{...point(38,37,'Mouse Field Headquarters'),spawns:[point(38,37),point(32,39),point(40,30)]}};
    l.spawns=[...l.headquarters.cats.spawns,...l.headquarters.mice.spawns,point(-40,0),point(40,0)];
    l.theme={sky:[.10,.16,.22],floorA:'#34424c',floorB:'#3d505c',grid:meta.accent,gridSoft:'#455967'};
    if(id==='rail_yard_nine'){
      l.theme={sky:[.20,.23,.29],floorA:'#42494d',floorB:'#535a5e',grid:meta.accent,gridSoft:'#68737b'};
      room('Signal Depot',-25,-24,18,14,{tile:'wall_03',doors:'xz'});
      room('Freight Dispatch',25,28,18,14,{tile:'wall_04',doors:'xz'});
      room('Communications Annex',28,-24,15,12,{tile:'wall_02'});
      room('Passenger Car 09',-9,-9,7,22,{height:4.8,tile:'wall_04'});
      room('Passenger Car 17',9,28,7,22,{height:4.8,tile:'wall_04'});
      room('Maintenance Underpass',-28,0,8,14,{height:3.6,tile:'wall_02',windows:false});
      deck(0,12,40,6,5.2);ramp(-28,12,16,6,'x',0,5.2);ramp(28,12,16,6,'x',5.2,0);
      // Rails are low physical strips; rolling stock has real enterable shells.
      for(const x of [-13.2,-4.8,4.8,13.2])wall(x,0,.12,79,.07,'trim_00',0,{safety:true});
      room('Crossing Signal Tower',0,12,8,6,{height:3.4,tile:'wall_03',doors:'x',base:5.2});
      prop('station_sign',0,12,5,.35,1.2,'prop_05',9.4);
      prop('vending_machine',-31,-24,1.4,1.3,2.6);prop('ticket_gate',29,-34,5,1.4,1.1);
      for(const c of [[-20,29,7,2.4],[22,-6,7,2.4],[0,-29,6,2.2],[29,3,5,2],[-19,-5,2,5]])cover(...c);
      l.ambient.push({kind:'flicker',x:0,z:12,y:7},{kind:'steam',x:-9,z:-20,y:4.8},{kind:'warning',x:28,z:-24,y:5.8});
      sectors(['West Signal','Platform Nine','Crossing','Freight Office','East Loading'],[[-25,-24],[-1,-15],[0,4],[25,28],[29,36]]);
      anchors({entry:[-38,-37],intel:[28,-24,'Classified communications'],security:[-25,-24,'Railway switchboard'],prison:[25,28],escort:[20,34],defend:[0,4],vault:[28,-24],sabotage:[-27,0],extract:[38,37],alarm:[28,-28],communications:[28,-24],reinforce:[39,-35],command:[25,28]});
    }else if(id==='whiskerhold'){
      l.theme={sky:[.25,.29,.33],floorA:'#625d51',floorB:'#746d5c',grid:meta.accent,gridSoft:'#6c7a79'};
      for(const s of [-1,1]){face(0,s*27,52,'x',4.8,'wall_02',[{at:0,width:7,bottom:0,top:3.5},{at:-16,width:5,bottom:1.05,top:2.8},{at:16,width:5,bottom:1.05,top:2.8}]);face(s*26,0,54,'z',5.4,'wall_03',[{at:0,width:7,bottom:0,top:3.5},{at:-17,width:4,bottom:1.05,top:2.8},{at:17,width:4,bottom:1.05,top:2.8}]);}
      room('Dominion Barracks',-14,-14,13,12,{tile:'wall_01',doors:'xz'});room('Command Headquarters',14,-14,13,12,{tile:'wall_03',doors:'xz'});
      room('Prison Block',-14,14,13,12,{tile:'wall_04',doors:'xz'});room('Security Armoury',14,14,13,12,{tile:'wall_02',doors:'xz'});
      for(const z of [-27,27]){deck(0,z,52,6,5.4);ramp(-34,z,16,6,'x',0,5.4);ramp(34,z,16,6,'x',5.4,0);}
      room('North Rampart Watch',-17,-27,7,6,{height:3.4,tile:'wall_03',doors:'x',base:5.4});
      room('South Rampart Watch',17,27,7,6,{height:3.4,tile:'wall_03',doors:'x',base:5.4});
      for(const x of [-8,8]){cover(x,0,1.1,7,1.25);cover(x,-34,6,1,1.25);cover(x,34,6,1,1.25);}
      prop('antenna',35,-13,3,3,9);prop('generator',-35,14,5,5,3.5);prop('floodlight',35,15,1.8,1.8,7);
      l.ambient.push({kind:'warning',x:14,z:-14,y:5.8},{kind:'pulse',x:35,z:-13,y:8.5});
      sectors(['West Approach','Barracks','Parade Court','Armoury','East Approach'],[[-33,-8],[-14,-14],[0,0],[14,14],[33,8]]);
      anchors({entry:[-38,-37],intel:[14,-14,'Dominion command records'],security:[14,14,'Security power control'],prison:[-14,14,'Captured Resistance soldiers'],escort:[-33,0,'Service-gate rendezvous'],defend:[0,0,'Parade courtyard'],vault:[14,-14],sabotage:[-35,9],extract:[-38,37],alarm:[14,18],communications:[14,-14],reinforce:[38,-37],command:[14,-14]});
    }else if(id==='floodgate'){
      l.theme={sky:[.025,.06,.08],floorA:'#16363a',floorB:'#21444a',grid:meta.accent,gridSoft:'#2f535b'};
      room('West Pump House',-26,-27,16,12,{tile:'wall_02',doors:'xz'});room('East Flood Control',26,27,16,12,{tile:'wall_03',doors:'xz'});
      room('Resistance Service Hub',-27,27,15,12,{tile:'wall_01',doors:'xz'});room('Restricted Valve Office',27,-27,15,12,{tile:'wall_04',doors:'xz'});
      deck(0,0,8,50,4.8);ramp(0,-34,8,18,'z',0,4.8);ramp(0,34,8,18,'z',4.8,0);
      for(const z of [-12,12])deck(0,z,38,6,4.8);
      ramp(-28,-12,18,6,'x',0,4.8);ramp(28,12,18,6,'x',4.8,0);
      // Drainage walls stop short of three cross passages at each level.
      for(const x of [-7,7])for(const z of [-28,-11,11,28])wall(x,z,.65,9,1.25,'wall_03');
      prop('pump',-24,0,5,5,4);prop('turbine',24,0,6,6,4.4);prop('industrial_pipe',-38,11,2,2,9);prop('conduit_cluster',38,-12,2,2,7);
      for(const c of [[-16,-2,1,6],[16,2,1,6],[-15,22,6,1],[15,-22,6,1]])cover(...c);
      l.ambient.push({kind:'steam',x:-24,z:0,y:4.5},{kind:'fan',x:24,z:-2.8,y:2.8,r:1.5},{kind:'flicker',x:0,z:0,y:4.1});
      sectors(['Pump Station','Western Sluice','Aqueduct','Eastern Sluice','Flood Control'],[[-26,-27],[-15,-20],[0,0],[15,20],[26,27]]);
      anchors({entry:[-38,-37],intel:[27,-27,'Resistance supply records'],security:[26,27,'Surveillance relays'],prison:[-27,27],escort:[-17,27],defend:[0,0],vault:[-27,27,'Underground data vault'],sabotage:[-26,-27,'Main pump controls'],extract:[38,37],alarm:[26,23],communications:[27,-27],reinforce:[39,0],command:[-27,27]});
    }else if(id==='cold_storage'){
      l.theme={sky:[.26,.35,.42],floorA:'#6c8791',floorB:'#8ca4ad',grid:meta.accent,gridSoft:'#5c7c8d'};
      room('Frozen Cargo A',-23,-22,22,18,{tile:'wall_01',doors:'xz'});room('Frozen Cargo B',22,22,22,18,{tile:'wall_02',doors:'xz'});
      room('Dispatch Offices',24,-23,18,16,{tile:'wall_03',doors:'xz'});room('Cold Chain Annex',-24,23,18,16,{tile:'wall_04',doors:'xz'});
      deck(0,0,45,7,5.6);ramp(-31,0,17,7,'x',0,5.6);ramp(31,0,17,7,'x',5.6,0);
      for(const p of [[-39,-22],[39,22]])prop('refrigeration_tank',...p,2.8,2.8,3.9);
      prop('research_console',28,-27,2.5,1.2,1.3);prop('coolant_pipe',-31,28,1.6,1.6,4.4);
      for(const c of [[-9,-24,2,7],[9,24,2,7],[-3,-15,6,1.5],[3,15,6,1.5],[-20,9,7,2],[20,-9,7,2]])cover(...c);
      l.ambient.push({kind:'steam',x:-29,z:-27,y:4.2},{kind:'spore',x:22,z:22,y:2},{kind:'flicker',x:24,z:-23,y:4.8});
      sectors(['North Loading','Cold Chamber A','Cargo Avenue','Cold Chamber B','South Loading'],[[-30,-34],[-23,-22],[0,0],[22,22],[30,34]]);
      anchors({entry:[-38,-37],intel:[24,-23,'Convoy manifest'],security:[-24,23,'Outpost communications'],prison:[22,22],escort:[0,26],defend:[-23,-22,'Silent outpost'],vault:[24,-23],sabotage:[-23,-22],extract:[38,37],alarm:[-24,27],communications:[-24,23],reinforce:[39,-36],command:[24,-23]});
    }else if(id==='clockwork'){
      l.theme={sky:[.18,.10,.075],floorA:'#493b32',floorB:'#655044',grid:meta.accent,gridSoft:'#705e50'};
      room('Machine Workshop',-26,-27,18,14,{tile:'wall_02',doors:'xz'});room('Production Data Vault',26,27,18,14,{tile:'wall_03',doors:'xz'});room('Assembly Control',0,28,16,12,{tile:'wall_01',doors:'xz'});
      deck(-25,5,8,26,5.4);deck(25,-5,8,26,5.4);deck(0,-8,42,6,5.4);
      ramp(-25,27,8,18,'z',5.4,0);ramp(25,-27,8,18,'z',0,5.4);
      prop('reactor_core',0,4,8,8,9);prop('turbine',-10,-24,6,6,5);prop('transformer',13,17,5,4,3.4);prop('industrial_pipe',-38,5,2.5,2.5,10);prop('pump',37,-4,4,5,4);
      for(const z of [-3,10])for(const x of [-14,14])cover(x,z,5,1.4,1.25,'wall_05');
      // Assembly benches are shootable, destructible cover with real collision.
      for(const x of [-8,8])cover(x,18,4,2,1.05,'prop_01');
      l.ambient.push({kind:'steam',x:0,z:4,y:9.2},{kind:'spark',x:13,z:17,y:3.5},{kind:'fan',x:-10,z:-26.8,y:3,r:1.5},{kind:'warning',x:0,z:28,y:5.8});
      sectors(['Machine Shop','West Assembly','Furnace Yard','East Assembly','Production Vault'],[[-26,-27],[-17,5],[0,-15],[17,-5],[26,27]]);
      anchors({entry:[-38,-37],intel:[0,28],security:[-26,-27],prison:[0,28],escort:[0,35],defend:[0,-15,'Foundry counterattack line'],vault:[26,27,'Production data vault'],sabotage:[0,15],extract:[38,37],alarm:[-26,-23],communications:[0,28],reinforce:[40,-35],command:[26,27]});
    }else{
      l.theme={sky:[.045,.035,.095],floorA:'#343244',floorB:'#494357',grid:meta.accent,gridSoft:'#655a75'};
      for(const z of [-23,23])for(const x of [-25,0,25])room((z<0?'North':'South')+' '+({[-25]:'Tea House',0:'Exchange',25:'Arcade'})[x],x,z,16,14,{height:5,tile:x===0?'wall_04':'wall_02',doors:'xz',roofAccess:true,blindFaces:x===-25&&z<0?['south']:x===25&&z>0?['north']:[]});
      for(const z of [-23,23])for(const x of [-12.5,12.5])deck(x,z,9,5,5.4);
      // Roof access meets the corner of each shop, leaving its ground-floor
      // doorway and the central market street open at full standing height.
      ramp(-30.5,-7,4.8,18,'z',5.4,0);ramp(30.5,7,4.8,18,'z',0,5.4);
      // Broken storefronts and market stalls give alternating firing cover.
      for(const [x,z] of [[-10,-7],[10,7],[-10,7],[10,-7]]){cover(x,z,5,2,1.1);wall(x-2,z,.5,.5,3.7,'trim_00');wall(x+2,z,.5,.5,3.7,'trim_00');slab(x,z,5,3,4.1,'upper_00');}
      for(const c of [[-36,9,1.2,6],[36,-9,1.2,6],[0,0,3,3],[0,-36,7,1],[0,36,7,1]])cover(...c);
      prop('antenna',34,30,2,2,7);prop('cargo_container',-36,-19,4,6,2.8);prop('beacon',36,16,1.5,1.5,3.5);
      l.ambient.push({kind:'flicker',x:0,z:-23,y:4.8},{kind:'spark',x:36,z:16,y:3.6},{kind:'dust',x:6,z:0,y:.6});
      sectors(['West Arcade','North Exchange','Market Square','South Exchange','East Arcade'],[[-25,-23],[0,-23],[0,8],[0,23],[25,23]]);
      anchors({entry:[-38,-37],intel:[0,-23,'Recon operative trail'],security:[-25,23],prison:[25,-23],escort:[-25,23],defend:[0,8],vault:[25,23],sabotage:[0,23],extract:[38,37],alarm:[-25,27],communications:[0,-23],reinforce:[39,-34],command:[25,23]});
    }
    // Each battlefield has a different skyline, interior rhythm and light
    // palette. Reserved combat spaces are checked against production collision.
    if(id==='rail_yard_nine'){
      for(const x of [16,37]){detail(x,-8,.85,1.25,9.5);detail(x,-8,2,2.4,.5);}
      detail(26.5,-8,22,.95,.85,9.2,'wall_05');detail(26.5,-8,3,1.8,.7,8.5,'prop_03');detail(26.5,-8,.18,.18,2.3,6.2);detail(26.5,-8,1.1,.25,.25,6.1);
      for(const x of [-9,9])for(let z=-35;z<=35;z+=7)detail(x,z,10,.45,.045,0,'floor_04');
      for(const [x,z] of [[-9,-9],[9,28]])for(const side of [-1,1])for(const dz of [-7,0,7]){detail(x+side*1.7,z+dz,1.1,1.8,.55,0,'prop_03');detail(x+side*2.3,z+dz,.18,1.8,.85,.55,'wall_03');}
      for(const x of [-9,9])for(const z of [-40,40]){detail(x,z,5,.65,.9,0,'wall_05');detail(x,z,2,.8,.3,.9,'trim_01');}
      ceilingStrip(-25,-24,9);ceilingStrip(25,28,9);ceilingStrip(28,-24,7);ceilingStrip(-9,-9,2,4.45);ceilingStrip(9,28,2,4.45);
      feature('Freight gantry',26.5,-8);feature('Furnished passenger cars',-9,-9);feature('Signal crossing',0,12);
      clearSpace('Track crossing',0,4,6,6);clearSpace('Depot loading apron',-25,-35,8,4);clearSpace('Freight forecourt',25,-14,8,4);
      lighting(.70,[.30,.34,.39],[1,.92,.78],[light('Depot work lights',-25,-24,[1,.87,.62]),light('Freight floodlights',26,-9,[.75,.87,1],11,.6,5),light('Track crossing',0,4,[.80,.93,1],11,.52),light('Dispatch lights',25,28,[1,.89,.69]),light('Communications office',28,-24,[.77,.9,1])]);
    }else if(id==='whiskerhold'){
      for(const z of [-29.5,29.5])for(let x=-24;x<=24;x+=4)detail(x,z,1.8,.65,1,5.4,'wall_02');
      for(const x of [-26.9,26.9])for(const z of [-23,-9,9,23])detail(x,z,1.2,1.5,5.8,0,'wall_03');
      for(const [x,z] of [[-14,-14],[-14,14]])for(const side of [-1,1]){detail(x+side*3.7,z-3.3,2,2.5,.55,0,'prop_03');detail(x+side*3.7,z-3.3,2,2.5,.25,1.4,'prop_01');}
      for(const [x,z] of [[14,-14],[14,14]]){detail(x-3.7,z-3.2,2.2,2,1.05,0,'prop_02');detail(x+3.7,z+3.2,2.2,2,1.05,0,'prop_02');}
      for(const x of [-4.9,4.9])for(const z of [-27,27])detail(x,z,.5,.4,1.6,3.6,'trim_01');
      for(const [x,z] of [[-14,-14],[14,-14],[-14,14],[14,14]])ceilingStrip(x,z,6);
      feature('Crenellated ramparts',0,-27);feature('Command compound',14,-14);feature('Prison barracks',-14,14);
      clearSpace('Parade ground',0,0,10,12);clearSpace('West gate apron',-33,0,8,8);clearSpace('East gate apron',33,0,8,8);
      lighting(.73,[.46,.48,.47],[1,.91,.75],[light('Parade floodlights',0,0,[1,.93,.78],12,.46,5),light('Prison lighting',-14,14,[.8,.88,1],8,.55),light('Command lighting',14,-14,[1,.88,.61],8,.5),light('Barracks lighting',-14,-14,[1,.92,.77],8,.42),light('Armoury lighting',14,14,[.82,.95,1],8,.5)]);
    }else if(id==='floodgate'){
      for(const z of [-20,0,20]){for(const x of [-5.1,5.1])detail(x,z,.8,.9,3.8,0,'wall_02');detail(0,z,11,.9,.4,3.8,'trim_00');}
      for(const x of [-40.7,40.7]){detail(x,0,.8,53,.8,5.5,'prop_02');for(const z of [-18,18])detail(x,z,.6,.8,5.5,0,'trim_00');}
      prop('pump',-30,7,3,3,4.8);detail(12,28,4,.85,2.1,0,'wall_04');detail(-12,-28,4,.85,2.1,0,'wall_04');
      for(const z of [-20,0,20])ceilingStrip(0,z,4,3.62);
      for(const [x,z] of [[-26,-27],[26,27],[-27,27],[27,-27]])ceilingStrip(x,z,8);
      feature('Ribbed aqueduct',0,0);feature('Service pipe galleries',-40.7,4);feature('Paired sluice controls',12,28);
      clearSpace('Lower aqueduct passage',0,0,5,8);clearSpace('West sluice apron',-15,-20,4,4);clearSpace('East sluice apron',15,20,4,4);
      lighting(.70,[.10,.18,.20],[.71,.89,1],[light('Lower gallery lamps',0,0,[.76,.95,1],11,.72,3.5),light('West sluice lamps',-15,-20,[.74,1,.90],9,.55),light('East sluice lamps',15,20,[.74,1,.90],9,.55),light('Pump control',-26,-27,[1,.87,.66],8,.6),light('Flood control',26,27,[1,.87,.66],8,.6),light('Service hub',-27,27,[.76,.94,1],8,.6)],1.11);
    }else if(id==='cold_storage'){
      for(const [cx,cz] of [[-23,-22],[22,22]])for(const dx of [-5,5])for(const dz of [-4.5,4.5])rack(cx+dx,cz+dz);
      for(const [x,z] of [[-23,-22],[22,22],[24,-23],[-24,23]]){vent(x-4,z-3);vent(x+4,z+3);ceilingStrip(x,z,9,4.9);}
      for(const [x,z] of [[-23,-32.5],[22,32.5]]){detail(x,z,12,2.7,.3,4.5,'upper_00');for(const dx of [-5,5])detail(x+dx,z+(z<0?1:-1),.3,1.4,.5,4,'trim_00');}
      detail(19,-27,2.5,2,1.05,0,'prop_02');detail(-29,19,2.2,1.8,1.05,0,'prop_02');
      feature('Refrigerated rack aisles',-23,-22);feature('Condensing roof banks',24,-23);feature('Loading canopy',22,32.5);
      clearSpace('Cargo avenue',0,0,8,12);clearSpace('North loading apron',-30,-34,6,4);clearSpace('South loading apron',30,34,6,4);
      lighting(.73,[.42,.51,.59],[.85,.94,1],[light('Cargo aisle A',-23,-22,[.78,.93,1],11,.68),light('Cargo aisle B',22,22,[.78,.93,1],11,.68),light('Cargo avenue',0,0,[.88,.96,1],12,.4,5),light('Dispatch office',24,-23,[1,.92,.78],9,.52),light('Cold chain annex',-24,23,[1,.92,.78],9,.52)],1.06);
    }else if(id==='clockwork'){
      for(const x of [-19,19]){for(const z of [-14,14]){detail(x,z,.9,.9,12.5,0,'wall_04');detail(x,z,1.8,1.8,.5,0,'wall_05');}detail(x,0,.8,30,.8,12.5,'wall_05');}
      detail(0,14,40,1.3,1,12.5,'wall_05');detail(0,14,3,2.3,.7,11.8,'prop_03');detail(0,14,.25,.25,2.5,9.3);detail(0,14,2.3,2.3,.75,8.55,'prop_02');
      for(const [x,z] of [[-13,29],[14,-28]]){detail(x,z,4.6,10,1.1,0,'wall_05');for(const dz of [-3.5,0,3.5])detail(x,z+dz,3.6,1.2,.35,1.1,'prop_02');for(const dx of [-2.2,2.2])detail(x+dx,z,.15,10,.25,1.1,'trim_01');}
      for(const [x,z] of [[-26,-27],[26,27],[0,28]]){vent(x-4,z-3);ceilingStrip(x,z,8);}
      feature('Traveling furnace crane',0,14);feature('Assembly conveyors',14,-28);feature('Reactor chimney',0,4);
      clearSpace('Furnace counterattack yard',0,-15,10,8);clearSpace('West assembly lane',-17,5,4,5);
      lighting(.67,[.22,.16,.13],[1,.82,.65],[light('Furnace glow',0,4,[1,.49,.20],12,.58,5),light('Machine shop',-26,-27,[.79,.89,1],9,.63),light('Furnace yard work lamps',0,-15,[.80,.91,1],11,.6,5),light('Production vault',26,27,[.86,.94,1],9,.6),light('Assembly control',0,28,[1,.88,.65],8,.52),light('West work lights',-17,5,[.88,.95,1],8,.48)],1.10);
    }else{
      for(const z of [-12,12]){for(const x of [-5.5,5.5])detail(x,z,.5,.8,4.6,0,'wall_03');detail(0,z,11.5,.8,.35,4.4,'trim_01');for(const x of [-3,0,3])detail(x,z,.6,.65,.7,3.65,'prop_03');}
      detail(0,0,.8,.8,3.8,1.2,'wall_02');detail(0,0,2.2,1,.8,5,'trim_01');
      for(const z of [-23,23])for(const x of [-25,0,25]){
        detail(x+5,z+(z<0?-4:4),1.2,1.2,2.2,5.4,'wall_03');detail(x-5,z+(z<0?-4:4),2.6,1,.65,5.4,'prop_02');ceilingStrip(x,z,6,4.5);
        if(!(x===-25&&z<0)&&!(x===25&&z>0)){const faceZ=z+(z<0?8.3:-8.3);detail(x,faceZ,10,2,.25,3.5,'upper_00');detail(x-4,faceZ,.2,1.8,.2,3.25);detail(x+4,faceZ,.2,1.8,.2,3.25);}
      }
      prop('floodlight',-36,3,1.4,1.4,5.5);prop('floodlight',36,-3,1.4,1.4,5.5);
      feature('Lantern street gates',0,-12);feature('Shattered clock monument',0,0);feature('Arcade rooftops',25,23);
      clearSpace('Market square',0,8,7,6);clearSpace('North market street',0,-8,7,6);
      lighting(.67,[.10,.12,.20],[.65,.76,1],[light('South lantern gate',0,12,[1,.69,.42],10,.68),light('North lantern gate',0,-12,[.94,.56,1],10,.64),light('North exchange',0,-23,[.75,.89,1],8,.7),light('South exchange',0,23,[1,.89,.72],8,.65),light('West arcade',-25,-23,[.69,.94,1],9,.66),light('East arcade',25,23,[.85,.72,1],9,.65)],1.12);
    }
    l.pickups=[...l.sectors.map(s=>point(s.x,s.z)),...l.landmarks.map(p=>({...p})),point(-38,0),point(38,0),point(0,-38),point(0,38)].slice(0,13);
    const common=[l.headquarters.cats,l.sectors[0],l.sectors[1],l.sectors[2],l.sectors[3],l.sectors[4],l.headquarters.mice];
    // Routes are waypoints for the cached physical pathfinder, not straight-line
    // instructions through architecture. Side routes keep patrols varied.
    l.patrolRoutes=[common.slice(1,4).map(p=>point(p.x,p.z)),common.slice(3,6).map(p=>point(p.x,p.z)),[point(-39,0),point(-39,32),point(0,39),point(39,32),point(39,0)]];
    for(const missionId of MISSIONS[id])l.missionSites[missionId]={anchors:JSON.parse(JSON.stringify(l.anchors)),patrolRoutes:JSON.parse(JSON.stringify(l.patrolRoutes)),reinforcements:[l.anchors.reinforce],optional:l.anchors.security};
    l.navPoints.push(...Object.values(l.anchors),...l.sectors,...l.spawns,...l.patrolRoutes.flat());
    return l;
  }

  function build(id,a){
    const l=createLayout(id);a.setTheme?.(l.theme);
    a.addPlat(0,0,89.8,89.8,-.8,.8,l.theme.floorA,true);a.addArenaBounds();
    for(const p of l.walls)a.arenaWall(p.x,p.z,p.w,p.d,p.h,p.tile,p.y,!!p.safety,p.material||'metal');
    for(const p of l.platforms){const v=a.addPlat(p.x,p.z,p.w,p.d,p.y,p.h,'#566c7d',true);if(v){a.arenaSurface?.(v,p.tile);v.architecturalRoof=!!p.architecturalRoof;v.authoredDeck=!!p.authoredDeck;}else if(a.addPlatformMetadata)a.addPlatformMetadata(p);}
    for(const p of l.decks)a.arenaDeck(p.x,p.z,p.w,p.d,p.top,p.tile);
    for(const p of l.ramps)a.arenaRamp(p.x,p.z,p.w,p.d,p.axis,p.fromHeight,p.toHeight,p.tile);
    for(const p of l.props)a.arenaProp(p.prefab,p.x,p.z,p.w,p.d,p.h,p.tile,p.y,p.material);
    for(const p of l.ambient)a.addAmbient?.(p);
    a.arenaNavAndSpawns(l.spawns.map(p=>[p.x,p.z]));
    for(const p of l.navPoints)a.addNavPoint?.({...p,y:p.y+EYE,mapHeight:true});
    a.arenaLoadoutPickups(l.pickups.map(p=>[p.x,p.z]));
    return l;
  }

  function worldMetadata(id,scale=SCALE){
    const l=createLayout(id),p=v=>({...v,x:v.x*scale,z:v.z*scale,y:(v.y||0)+EYE});
    const anchors=Object.fromEntries(Object.entries(l.anchors).map(([k,v])=>[k,p(v)]));
    const patrolRoutes=l.patrolRoutes.map(route=>route.map(p));
    const headquarters=Object.fromEntries(Object.entries(l.headquarters).map(([k,v])=>[k,{...p(v),spawns:v.spawns.map(p)}]));
    const sectors=l.sectors.map((v,i)=>({...p(v),radius:v.radius*scale,owner:0,links:[...v.neighbors,...(i===0?['cat_hq']:i===4?['mouse_hq']:[])],spawn:p(v)}));
    const missionSites=Object.fromEntries(Object.keys(l.missionSites).map(k=>[k,{anchors:JSON.parse(JSON.stringify(anchors)),patrolRoutes:JSON.parse(JSON.stringify(patrolRoutes)),reinforcements:[p(l.anchors.reinforce)],optional:p(l.anchors.security)}]));
    const lighting={...l.theme.lighting,zones:l.theme.lighting.zones.map(v=>({...v,x:v.x*scale,z:v.z*scale,radius:v.radius*scale}))};
    const footprint=v=>({x:v.x*scale,z:v.z*scale,w:v.w*scale,d:v.d*scale,...(v.name?{label:v.name}:{})});
    const buildings=l.buildings.filter(v=>v.base<1),outsideBuildings=v=>!buildings.some(b=>Math.abs(v.x-b.x)<b.w/2+.35&&Math.abs(v.z-b.z)<b.d/2+.35);
    const minimap=[...buildings,...l.props.filter(v=>v.y<1),...l.walls.filter(v=>v.y<1&&v.h>=.75&&v.w*v.d>=1.5&&outsideBuildings(v))].slice(0,140).map(footprint);
    return {id,headquarters,hq:{1:headquarters.cats,2:headquarters.mice},factionSpawns:{1:headquarters.cats.spawns,2:headquarters.mice.spawns},sectors,anchors,patrolRoutes,missionSites,cover:l.cover.map(p),overwatch:l.overwatch.map(p),lighting,minimap,features:l.features.map(p),combatSpaces:l.combatSpaces.map(v=>({...footprint(v),label:v.label,y:EYE})),bounds:44.9*scale};
  }
  const api={VERSION:2,SCALE,EYE,BODY,IDS,catalog:()=>CATALOG.slice(),get,createLayout,build,worldMetadata};
  if(typeof module==='object'&&module.exports)module.exports=api;
  root.NeonWarMaps=api;
})(typeof globalThis!=='undefined'?globalThis:this);
