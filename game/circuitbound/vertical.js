/* Grid lift constraints. Power/rotation are supplied by the existing winch network. */
(function(root,factory){
  if(typeof module==='object'&&module.exports)module.exports=factory();
  else root.CircuitVertical=factory();
})(typeof globalThis==='object'?globalThis:this,function(){
  'use strict';
  const EPS=.01;
  const overlap=(a,b)=>a.x<b.x+b.w-EPS&&a.x+a.w>b.x+EPS&&a.y<b.y+b.h-EPS&&a.y+a.h>b.y+EPS;
  function guideContacts(group,get,T){
    if(T.RAIL===undefined)return [];
    const rows=new Map(),contacts=[];
    for(const [x,y] of group){const row=rows.get(y)||{left:x,right:x};row.left=Math.min(row.left,x);row.right=Math.max(row.right,x);rows.set(y,row);}
    for(const [y,row] of rows)for(const x of [row.left-1,row.right+1])if(get(x,y)===T.RAIL)contacts.push([x,y]);
    return contacts;
  }
  function hasGuide(group,get,T){return guideContacts(group,get,T).length>0;}
  function findCounterweight(engine,group,x,y){
    engine.rebuild();const own=new Set(group.map(p=>engine.key(...p))),candidates=[];
    const source=engine.key(x,y),metadata=engine.getMeta(),claimed=new Set(engine.active.filter(k=>k!==source&&engine.id(k)===engine.T.WINCH).map(k=>metadata.get(k)?.counterweightAssembly).filter(Boolean));
    for(const [assembly,keys] of engine.assemblies){
      if(claimed.has(assembly)||!keys.length||keys.some(k=>own.has(k)))continue;
      const coordinates=keys.map(k=>engine.xy(k));
      if(!coordinates.every(([gx,gy])=>Math.abs(gx-x)<=12&&gy>y&&gy<=y+50))continue;
      if(!keys.some(k=>engine.defs[engine.id(k)]?.counterweight))continue;
      const distance=Math.min(...coordinates.map(([gx,gy])=>Math.abs(gx-x)+Math.abs(gy-y)));
      candidates.push({assembly,coordinates,distance});
    }
    const attached=metadata.get(source)?.counterweightAssembly;
    candidates.sort((a,b)=>Number(b.assembly===attached)-Number(a.assembly===attached)||a.distance-b.distance||a.assembly-b.assembly);
    return candidates[0]?.coordinates||[];
  }
  function moveLift(options){
    const {group=[],counterGroup=[],x,y,m={},dy,torque=0,engine,get,set,defs,T,player,tile=24,width,height}=options;
    const metadata=typeof options.meta==='function'?options.meta():options.meta;
    const fail=message=>{m.fault=message;return false;};
    if(!engine||!metadata||typeof set!=='function'||typeof get!=='function')return fail('LIFT CONNECTION UNAVAILABLE');
    if(!group.length||![-1,1].includes(dy))return fail('LIFT REQUIRES A LOAD AND ONE GRID STEP');
    if(m.running===false)return fail('LIFT REQUIRES MECHANICAL DRIVE');
    engine.rebuild();
    const worldWidth=width||engine.width,worldHeight=height||engine.height,key=(gx,gy)=>gx+gy*worldWidth;
    const inside=(gx,gy)=>Number.isInteger(gx)&&Number.isInteger(gy)&&gx>=0&&gy>=0&&gx<worldWidth&&gy<worldHeight;
    const allCoordinates=[...group,...counterGroup];
    if(allCoordinates.some(p=>!Array.isArray(p)||p.length<2||!inside(...p)||!get(...p)))return fail('LIFT LOAD IS DETACHED OR OUTSIDE THE WORLD');
    const own=new Set(allCoordinates.map(p=>key(...p)));
    if(own.size!==allCoordinates.length)return fail('LOAD AND COUNTERWEIGHT MUST BE SEPARATE');
    if(own.has(key(x,y)))return fail('WINCH CANNOT BE LINKED TO ITS OWN LOAD');
    if(allCoordinates.some(p=>get(...p)===T.CORE))return fail('FOUNDRY CORE IS ANCHORED');
    if(counterGroup.length&&!counterGroup.some(p=>defs[get(...p)]?.counterweight))return fail('ATTACH A REAL COUNTERWEIGHT ASSEMBLY');
    const mainAssemblies=new Set(group.map(p=>metadata.get(key(...p))?.assembly).filter(Boolean));
    const counterAssemblies=new Set(counterGroup.map(p=>metadata.get(key(...p))?.assembly).filter(Boolean));
    const counterAssembly=[...counterAssemblies][0];
    if(counterGroup.length&&(counterAssemblies.size!==1||counterGroup.some(p=>metadata.get(key(...p))?.assembly!==counterAssembly)||mainAssemblies.has(counterAssembly)))return fail('LINK THE COUNTERWEIGHT SEPARATELY FROM THE LOAD');
    if(counterGroup.length&&engine.active.some(k=>k!==key(x,y)&&engine.id(k)===T.WINCH&&metadata.get(k)?.counterweightAssembly===counterAssembly))return fail('COUNTERWEIGHT ALREADY ATTACHED TO ANOTHER WINCH');
    for(const assembly of [...mainAssemblies,...counterAssemblies])if(engine.assemblies.get(assembly)?.some(k=>!own.has(k)))return fail('MOVE THE COMPLETE LINKED LIFT ASSEMBLY');
    const bodies=new Set([...mainAssemblies,...counterAssemblies]);
    if(engine.active.some(k=>bodies.has(metadata.get(k)?.jointAssembly))||allCoordinates.some(p=>[T.BEARING,T.HINGE].includes(get(...p))))return fail('LIFT LOAD IS ATTACHED TO AN ANCHORED JOINT');

    const moving=[...group.map(([gx,gy])=>({x:gx,y:gy,dy,side:0})),...counterGroup.map(([gx,gy])=>({x:gx,y:gy,dy:-dy,side:1}))];
    const mainGuides=guideContacts(group,get,T),counterGuides=guideContacts(counterGroup,get,T);
    m.guided=mainGuides.length+counterGuides.length>0;
    for(const [gx,gy] of mainGuides)if(get(gx,gy+dy)!==T.RAIL)return fail('GUIDE RAIL ENDS BEFORE THE NEXT LOAD POSITION');
    for(const [gx,gy] of counterGuides)if(get(gx,gy-dy)!==T.RAIL)return fail('COUNTERWEIGHT GUIDE RAIL ENDS');

    const riderOn=coordinates=>!!player&&coordinates.some(([gx,gy])=>Math.abs(player.y+player.h-gy*tile)<5&&player.x+player.w>gx*tile&&player.x<(gx+1)*tile);
    const mainRider=riderOn(group),counterRider=riderOn(counterGroup);
    if(mainRider&&counterRider)return fail('RIDER SPANS TWO OPPOSING PLATFORMS');
    const riding=mainRider?0:counterRider?1:-1,riderDy=mainRider?dy:counterRider?-dy:0;
    const mass=coordinates=>coordinates.reduce((total,p)=>{
      const data=metadata.get(key(...p)),cargo=[...Object.values(data?.contents||{}),...Object.values(data?.outputs||{})].reduce((n,count)=>n+(Number.isFinite(count)?Math.max(0,count):0),0);
      return total+(defs[get(...p)]?.weight??1)+cargo*.1;
    },0);
    const loadMass=mass(group)+(mainRider?3:0),counterMass=mass(counterGroup)+(counterRider?3:0),friction=(loadMass+counterMass)*.1;
    // Raising the heavier side needs net gravitational work; either direction still needs friction torque.
    const required=Math.max(1,Math.max(0,-dy*(loadMass-counterMass))+friction);
    Object.assign(m,{loadMass,counterweightMass:counterMass,frictionTorque:friction,requiredTorque:required,availableTorque:Number.isFinite(torque)?Math.max(0,torque):0});
    if(required>m.availableTorque+1e-8)return fail(`LIFT STALLED: need ${required.toFixed(1)} / have ${m.availableTorque.toFixed(1)} torque`);

    const destinations=new Set();
    for(const part of moving){const nx=part.x,ny=part.y+part.dy,k=key(nx,ny);
      if(!inside(nx,ny))return fail('LIFT REACHES WORLD BOUNDARY');
      if(ny<=y||ny>y+50||Math.abs(nx-x)>12)return fail(part.side?'COUNTERWEIGHT REACHES WINCH CABLE LIMIT':'LIFT REACHES WINCH CABLE LIMIT');
      // Even non-solid wire/ladder construction must not be overwritten and silently lost.
      if(!own.has(k)&&get(nx,ny)!==T.AIR)return fail(part.side?'COUNTERWEIGHT PATH BLOCKED':'LIFT PATH BLOCKED');
      if(destinations.has(k))return fail('LOAD AND COUNTERWEIGHT DESTINATIONS INTERSECT');destinations.add(k);
    }
    const playerBlocked=rectangle=>{
      for(let gy=Math.floor(rectangle.y/tile);gy<=Math.floor((rectangle.y+rectangle.h-EPS)/tile);gy++)for(let gx=Math.floor(rectangle.x/tile);gx<=Math.floor((rectangle.x+rectangle.w-EPS)/tile);gx++){
        if(!inside(gx,gy))return true;if(own.has(key(gx,gy)))continue;
        const d=defs[get(gx,gy)];if(d?.solid&&!(d.door&&metadata.get(key(gx,gy))?.open))return true;
      }return false;
    };
    // Sample opposing load sweeps and the rider's entire path, including headroom.
    for(let step=1;step<=8;step++){
      const fraction=step/8,rectangles=moving.map(part=>({x:part.x*tile,y:(part.y+part.dy*fraction)*tile,w:tile,h:tile,side:part.side,solid:!!defs[get(part.x,part.y)]?.solid}));
      for(const a of rectangles.filter(r=>r.side===0))for(const b of rectangles.filter(r=>r.side===1))if(overlap(a,b))return fail('LOAD AND COUNTERWEIGHT PATHS INTERSECT');
      if(player){const body={x:player.x,y:player.y+riderDy*tile*fraction,w:player.w,h:player.h};
        if(riding>=0&&playerBlocked(body))return fail('RIDER PATH BLOCKED');
        if(rectangles.some(r=>r.solid&&r.side!==riding&&overlap(r,body)))return fail(riding>=0?'COUNTERWEIGHT WOULD STRIKE RIDER':'PLAYER IN LIFT PATH');
      }
    }
    if(engine.applyStress?.(group,loadMass*(dy<0?1.25:1))!=null)return fail('LIFT STRUCTURAL CONNECTION FAILED');
    if(counterGroup.length&&engine.applyStress?.(counterGroup,counterMass*(dy>0?1.25:1))!=null)return fail('COUNTERWEIGHT STRUCTURAL CONNECTION FAILED');

    const snapshot=moving.map(part=>({...part,id:get(part.x,part.y),data:metadata.get(key(part.x,part.y))||{}}));
    try{
      for(const part of snapshot){set(part.x,part.y,T.AIR);if(get(part.x,part.y)!==T.AIR)throw new Error('edit refused');}
      for(const part of snapshot){set(part.x,part.y+part.dy,part.id,part.data);if(get(part.x,part.y+part.dy)!==part.id)throw new Error('edit refused');}
    }catch{
      // A host may reject an edit; restore the complete prior arrangement before reporting failure.
      for(const part of snapshot)try{set(part.x,part.y+part.dy,T.AIR);}catch{}
      for(const part of snapshot)try{set(part.x,part.y,part.id,part.data);}catch{}
      return fail('WORLD REFUSED LIFT MOVEMENT');
    }
    for(const part of snapshot){engine.changed?.(part.x,part.y);engine.changed?.(part.x,part.y+part.dy);}
    if(player&&riding>=0)player.y+=riderDy*tile;
    m.fault='';m.counterbalanced=counterGroup.length>0;return true;
  }
  return {moveLift,hasGuide,findCounterweight};
});
