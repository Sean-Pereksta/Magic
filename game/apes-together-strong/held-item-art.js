/* Authored held equipment. Pure visual selections; never changes combat or orders. */
(() => {
  'use strict';
  const P=ATSRenderer.prototype,TAU=Math.PI*2,EMPTY=Object.freeze([]),working=/build|chop|clear|repair|garden|cook|breach/;
  const art=()=>window.ATSVisualAssets,meta=()=>art()?.manifest?.['held-items'];
  const championItems=Object.freeze({wallbreaker:'maul',bulwark:'logShield',tankBuster:'chainWrap',jailerCrusher:'shoulderPlate',masterBuilder:'toolRig',rescueBearer:'ropePack',siegeHauler:'ropePack',greatForager:'food',saboteur:'cutters',climberCaptain:'cuffs',shockRaider:'baton',gateRunner:'satchel',skyRunner:'hook',scoutMaster:'scoutSash',silentInfiltrator:'cuffs',signalHunter:'sling',warcaller:'drum',guardCaptain:'staff',pursuitLeader:'commandSash',settlementWarden:'warMantle',slingerAce:'sling',alarmSpoiler:'jammer',skirmisher:'satchel',supplyThief:'utilityBelt'});
  const handItems=new Set(['pistol','rifle','shotgun','sniper','assault','machine','rotary','radio','grenade','flare','hammer','spear','torch','maul','cutters','baton','hook','staff','sling','jammer','binoculars']);
  const guns=new Set(['pistol','rifle','shotgun','sniper','assault','machine','rotary']);
  const backWorn=new Set(['ropePack','toolRig','warMantle']);
  function ready(){return !!meta()&&!!art()?.get('held-combat')&&!!art()?.get('held-utility')&&!!art()?.get('held-stone')}
  function hasEquipment(a,p={}){return a.hp>0&&(p.human||a.type==='human'||!!a.kind||a.equipment?.cuffs||a.equipment?.spear||a.equipment?.torch||a.shield?.hp>0||!!a.champion||!!a.carrying||!!a._carryingWood||a.state==='scout'||p.state==='throw'||working.test(a.activity||''))}
  function resolve(a={},p={}){
    if(!hasEquipment(a,p))return EMPTY;
    const result=[],human=p.human||a.type==='human',rear=!!p.rear;
    const add=(id,socket='right',extra={})=>{
      if(result.some(v=>v.id===id&&v.socket===socket))return;
      let layer=rear?'rear':'front';
      if(socket==='body')layer='front'; // The visible front OR back fabric belongs over that surface.
      if(backWorn.has(id))layer=rear?'front':'rear';
      result.push({id,socket,layer,...extra});
    };
    const sash=id=>add(id+(rear?'Rear':'Front'),'body');
    if(human){
      const busy=p.state==='radio'||p.state==='throw'||a.engineerJob||a.constructing;
      if(p.state==='radio')add('radio');
      else if(p.state==='throw'){if(p.phase<.525)add(a.role==='spotter'?'flare':'grenade')}
      else if(a.engineerJob||a.constructing)add('hammer');
      else add(a.role==='rotary'?'rotary':guns.has(a.kind)?a.kind:'rifle');
      if(a.role==='shield')add(rear?'riotShieldRear':'riotShieldFront','left');
      if(a.role==='medic')add('medkit','hip');
      if(a.role==='spotter'&&!busy)add('binoculars','hip');
      if(a.role==='mortar')add('mortar',a.moving?'back':'ground');
      return result;
    }
    const gear=a.equipment||{};
    if(gear.cuffs){add('cuffs','left');add('cuffs','right')}
    if(a.shield?.hp>0)add('logShield','both',{damage:a.shield.hp/Math.max(1,a.shield.maxHp)});
    const cargo=a._carryingWood||a.carrying==='wood'?'wood':a.carrying?'food':null;
    if(cargo)add(cargo,'both');
    if(p.state==='throw'&&p.phase<.525&&!gear.spear)add('stone');
    if(gear.spear&&(p.state!=='throw'||p.phase<.525))add('spear');
    if(gear.torch)add('torch',gear.spear?'left':'right');
    if(working.test(a.activity||'')&&!gear.spear&&!gear.torch&&!cargo)add('hammer');
    if(a.state==='scout')sash('scoutSash');
    const item=championItems[a.champion?.archetype];
    if(item){
      if(item==='scoutSash'||item==='commandSash')sash(item);
      else if(item==='logShield'){if(!(a.shield?.hp>0))add(item,'both',{damage:1})}
      else if(item==='chainWrap'||item==='cuffs'){add(item,'left');add(item,'right')}
      else if(item==='food'||item==='drum')add(item,'both');
      else add(item,handItems.has(item)?'right':backWorn.has(item)?'back':'body');
    }
    // A tool and an equipped weapon cannot occupy the same palm. Retained kit
    // moves to the free hand or to its carrying strap without changing loadout.
    const occupied=new Set();
    for(const item of result){
      if(['cuffs','chainWrap'].includes(item.id)||!['left','right','both'].includes(item.socket))continue;
      const needed=item.socket==='both'?['left','right']:[item.socket];
      if(needed.some(v=>occupied.has(v))){
        const free=['left','right'].find(v=>!occupied.has(v));
        if(item.socket!=='both'&&free){item.socket=free;occupied.add(free)}
        else {item.socket='back';item.layer=rear?'front':'rear'}
      }else for(const hand of needed)occupied.add(hand);
    }
    return result;
  }

  // Hand landmarks are measured in each painted crop, not guessed in world space.
  // Frame dimensions and planted-foot anchors convert them to actor coordinates.
  const apeHands=[[[.58,.96],[.87,.93]],[[.43,.96],[.88,.91]],[[.12,.96],[.86,.96]],[[.25,.95],[.87,.90]],[[.53,.95],[.90,.91]],[[.67,.92],[.95,.87]]];
  // Pixel landmarks on the weapon-free source sheets. These avoid anchoring
  // a new gun to the old baked gun's crop boundary instead of the empty palm.
  const humanHands=[
    [[58,124,146,108],[291,124,377,108],[513,127,600,110],[817,62,836,47],[1023,63,1042,46],[1182,112,1273,121],[1404,112,1498,125]],
    [[100,335,151,330],[333,339,382,329],[551,341,600,330],[815,275,834,262],[1018,278,1036,257],[1180,342,1277,348],[1404,347,1500,354]],
    [[90,552,144,556],[332,561,379,551],[550,557,604,554],[816,506,835,499],[1029,504,1045,490],[1173,554,1282,567],[1393,554,1500,570]],
    [[110,773,145,778],[336,773,374,776],[560,774,601,777],[808,721,822,711],[1007,721,1019,708],[1176,788,1264,797],[1401,785,1495,790]]
  ];
  const humanActionHands=[
    [[229,67,249,47],[532,65,550,48],[715,179,811,63],[1207,125,1025,24]],
    [[202,376,218,355],[506,374,526,353],[727,484,810,368],[1214,430,1006,331]],
    [[194,685,212,668],[511,691,530,675],[714,782,839,663],[1213,729,1018,631]],
    [[209,1000,214,980],[515,999,528,978],[729,1096,819,983],[1212,1058,1024,949]]
  ];
  function sockets(a,p,slot){
    const cm=art()?.manifest?.characters,table=cm?.frames?.[p.atlas],f=table?.[p.row+':'+p.column],sheet=cm?.atlases?.find(v=>v.id==='characters-'+p.atlas);
    let points=apeHands[Math.max(0,p.row)]||apeHands[0];
    if(p.human){
      if(p.atlas==='humanactions')points=p.column===2?[[.27,.45],[.32,.31]]:p.column===3?[[.45,.61],[.70,.14]]:[[.48,.35],[.68,.34]];
      else if(p.atlas==='humanwalk')points=p.column>=2?[[.19,.57],[.82,.60]]:p.row===0?[[.20,.62],[.86,.56]]:[[.46,.50],[.67,.54]];
      else if(p.column===3||p.column===4)points=p.row===0?[[.67,.28],[.76,.27]]:[[.48,.29],[.68,.30]];
      else if(p.column>=5)points=[[.16,.59],[.84,.61]];
      else points=p.row===0?[[.15,.59],[.87,.53]]:[[.42,.48],[.64,.54]];
    }else if(p.atlas==='actions'){
      points=[[[.35,.08],[.69,.10]],[[.56,.93],[.88,.92]],[[.11,.67],[.95,.26]],[[.36,.65],[.82,.065]],[[.36,.68],[.69,.055]]][p.column]||points;
    }else if(p.king){
      if(p.atlas==='kingactions')points=p.column===2?[[.19,.31],[.83,.25]]:p.column===0?[[.28,.08],[.79,.31]]:[[.21,.33],[.71,.08]];
      else if(p.column===3)points=[[.34,.07],[.70,.07]];
      else if(p.column===4)points=[[.40,.75],[.88,.92]];
      else points=p.row>=3?[[.15,.88],[.84,.89]]:p.row===0?[[.20,.90],[.79,.91]]:[[.43,.95],[.90,.88]];
    }else if(p.column===1){points=[[.65,.95],[.89,.81]]}
    else if(p.column===2){points=[[.65,.94],[.90,.82]]}
    else if(p.column>=3){points=[[.60,.88],[.91,.88]]}
    if(!f||!sheet)return {left:{x:-13,y:-15},right:{x:18,y:-15},body:{x:0,y:-32},hip:{x:-11,y:-22},back:{x:-10,y:-31},ground:{x:23,y:1},both:{x:2,y:-16}};
    const unit=slot/(sheet.width/sheet.columns)*(p.atlas==='kingactions'&&p.row===0?.68:1);
    const point=(v)=>({x:(f.w*v[0]-f.anchorX)*unit,y:(f.h*v[1]-f.anchorY)*unit});
    const pixels=p.atlas==='humans'?humanHands[p.row]?.[p.column]:p.atlas==='humanactions'?humanActionHands[p.row]?.[p.column]:null;
    const measured=f.hands,convert=k=>{const i=k==='left'?0:2;return measured?.[k]?{x:(measured[k].x-f.anchorX)*unit,y:(measured[k].y-f.anchorY)*unit}:pixels?{x:(pixels[i]-f.x-f.anchorX)*unit,y:(pixels[i+1]-f.y-f.anchorY)*unit}:point(points[k==='left'?0:1])};
    const left=convert('left'),right=convert('right');
    return {left,right,both:{x:(left.x+right.x)/2,y:(left.y+right.y)/2},body:point([.51,.48]),hip:point([.21,.63]),back:point([.28,.45]),ground:{x:22,y:0},frame:f,unit};
  }
  function placement(a,p,item,anchors){
    const s=anchors[item.socket]||anchors.right,move=['run','sprint'].includes(p.state),attack=['attack','heavyAttack','chargeAttack','prepare'].includes(p.state);
    let rotation=0;
    if(guns.has(item.id))rotation=['aim','fire'].includes(p.state)?(p.rear?-.28:-.09):(p.rear?1.2:.75);
    else if(['spear','torch','hammer','maul','staff','baton','hook','cutters'].includes(item.id))rotation=p.state==='throw'&&p.phase>=.525?1.1:attack?(p.state==='prepare'?-.5:.9):move?Math.sin(p.phase*TAU)*.08:.12;
    if(p.human&&(a.engineerJob||a.constructing)&&item.id==='hammer')rotation=Math.sin(p.phase*TAU)*.45;
    return {x:s.x,y:s.y,rotation};
  }
  function draw(r,c,a,p,slot,pass,items){
    if(!ready()||!hasEquipment(a,p))return false;
    items=items||resolve(a,p);const anchors=sockets(a,p,slot),data=meta();let drawn=false;
    for(const item of items){
      if(item.layer!==pass)continue;
      let f=data.frames[item.id],image=f&&art().get(f.atlas);
      if(item.id==='logShield'){
        const stage=item.damage<.3?2:item.damage<.65?1:0,q=art().manifest.equipment?.frames?.[stage];image=art().get('equipment-logs');
        if(q)f={...q,gripX:q.w*.5,gripY:q.h*.62,drawWidth:48,drawHeight:48};
      }
      if(!f||!image)continue;
      const at=placement(a,p,item,anchors),w=f.drawWidth,h=f.drawHeight;
      c.save();c.translate(at.x,at.y);c.rotate(r.reducedMotion&&['run','sprint'].includes(p.state)?0:at.rotation);
      c.drawImage(image,f.x,f.y,f.w,f.h,-f.gripX/f.w*w,-f.gripY/f.h*h,w,h);c.restore();
      r.heldItemArtworkDraws=(r.heldItemArtworkDraws||0)+1;drawn=true;
    }
    return drawn;
  }
  function restoreHands(r,c,a,p,slot,items,image){
    if(!image||!ready()||!items?.length)return;
    const anchors=sockets(a,p,slot),f=anchors.frame,u=anchors.unit;if(!f)return;
    const hands=new Set();for(const item of items)if(item.layer==='front'&&item.socket!=='body'&&item.socket!=='back'&&item.socket!=='hip'&&item.socket!=='ground'&&!['cuffs','chainWrap'].includes(item.id)){if(item.socket==='both'){hands.add('left');hands.add('right')}else hands.add(item.socket);if(guns.has(item.id)&&item.id!=='pistol')hands.add('left')}
    for(const name of hands){const h=anchors[name];if(!h)continue;const radius=p.human?1.65:p.king?3:2.4;c.save();c.beginPath();c.arc(h.x,h.y,radius,0,TAU);c.clip();c.drawImage(image,f.x,f.y,f.w,f.h,-f.anchorX*u,-f.anchorY*u,f.w*u,f.h*u);c.restore()}
  }
  const log=P.drawLogShield;if(log)P.drawLogShield=function(c,a){if(ready()&&a.hp>0&&a.shield?.hp>0)return true;return log.call(this,c,a)};
  const worker=P.drawWorkerDetail;if(worker)P.drawWorkerDetail=function(c,a){if(!ready())return worker.call(this,c,a);if(a.activity==='grooming'||a.activity==='socializing')worker.call(this,c,{...a,carrying:false})};
  const champion=P.drawChampionGear;if(champion)P.drawChampionGear=function(c,a){
    if(!ready())return champion.call(this,c,a);if(!a.champion||a._atlas||a.hp<=0)return;
    const color=window.ATSDivisions?.find(d=>d.id===a.divisionId)?.color||'#e9c56d';c.save();c.strokeStyle=color;c.lineWidth=1.7;c.beginPath();c.ellipse(0,5,20,7,0,0,TAU);c.stroke();c.translate(0,-(a.elevation||a.wallClimbHeight||0));c.fillStyle=color;c.beginPath();c.moveTo(0,-83);c.lineTo(4,-78);c.lineTo(0,-73);c.lineTo(-4,-78);c.closePath();c.fill();c.restore();
  };
  window.ATSHeldItemArt={version:1,ready,hasEquipment,resolve,sockets,placement,draw,restoreHands,championItems,
    status(){return {ready:ready(),atlases:meta()?.atlases?.length||0,frames:Object.keys(meta()?.frames||{}).length,perActorCache:0}}};
})();
