// Trash Tactics: deterministic, browser-local RTS simulation.
export const TYPES = {
  twig: {label:'Twig',cost:25,hp:40,r:13,speed:150,trainTime:4,supply:1,worker:true},
  rock: {label:'Rock',cost:50,hp:100,r:18,speed:123,trainTime:5,supply:1,beats:'scissors',range:43},
  paper: {label:'Paper',cost:50,hp:100,r:17,speed:132,trainTime:5,supply:1,beats:'rock',range:48},
  scissors: {label:'Scissors',cost:50,hp:100,r:18,speed:138,trainTime:5,supply:1,beats:'paper',range:46},
  desk: {label:'Home desk',cost:200,hp:850,r:55,buildTime:12,supplyCap:12,building:true},
  cabinet: {label:'Cabinet',cost:75,hp:400,r:37,buildTime:7,supplyCap:8,building:true},
  printer: {label:'Printer',cost:150,hp:550,r:44,buildTime:10,supplyCap:0,building:true},
  trash: {label:'Trash can',hp:1,r:27,building:false,resource:true}
};
export const MAPS = {
  desktop: {id:'desktop',name:'Desktop Meadow',color:'#e9ddba',obstacles:[
    {x:1040,y:470,w:320,h:250,type:'book'},
    {x:1040,y:880,w:320,h:250,type:'book'},
    {x:720,y:180,w:350,h:110,type:'ruler'},
    {x:1140,y:120,w:140,h:190,type:'tape'},
    {x:1330,y:1310,w:350,h:110,type:'ruler'},
    {x:1120,y:1290,w:160,h:170,type:'eraser'}
  ],lanes:[[[330,800],[1200,800],[2070,800]],[[330,800],[790,390],[1200,390],[1630,390],[2070,800]],[[330,800],[790,1210],[1200,1210],[1630,1210],[2070,800]]]},
  breakroom: {id:'breakroom',name:'Breakroom Blitz',color:'#dce5d3',obstacles:[
    {x:1100,y:390,w:210,h:300,type:'cup'},
    {x:960,y:900,w:480,h:220,type:'sponge'},
    {x:720,y:990,w:190,h:300,type:'book'},
    {x:1490,y:310,w:190,h:300,type:'book'},
    {x:720,y:180,w:340,h:110,type:'eraser'},
    {x:1340,y:1310,w:340,h:110,type:'eraser'}
  ],lanes:[[[330,800],[1200,800],[2070,800]],[[330,800],[790,350],[1200,335],[1420,260],[1800,260],[2070,800]],[[330,800],[600,1330],[1200,1220],[1800,1200],[2070,800]]]}
};
function layoutMap(id,players){
  const map=structuredClone(MAPS[id]||MAPS.desktop),deskTheme=map.id==='desktop';
  map.expansions=[{x:790,y:800},{x:1610,y:800},{x:1200,y:800},{x:1200,y:390},{x:1200,y:1210}];
  map.camps=players===4?[{x:650,y:250},{x:1750,y:250},{x:1750,y:1350},{x:650,y:1350}]:players===3?[{x:350,y:1220},{x:2050,y:1220},{x:1200,y:260}]:[{x:330,y:800},{x:2070,y:800}];
  if(players===3){
    map.obstacles=[{x:800,y:650,w:320,h:180,type:deskTheme?'book':'cup'},{x:1280,y:650,w:320,h:180,type:'book'},{x:1090,y:1050,w:220,h:130,type:deskTheme?'eraser':'sponge'}];
    map.lanes=[[[350,1220],[650,950],[1200,930],[1750,950],[2050,1220]],[[350,1220],[650,550],[1050,480],[1200,260]],[[2050,1220],[1750,550],[1350,480],[1200,260]],[[1200,260],[1200,950],[1200,1350]],[[350,1220],[1200,1350],[2050,1220]]];
    map.expansions=[{x:650,y:950},{x:1750,y:950},{x:1000,y:480},{x:1400,y:480},{x:1200,y:930},{x:500,y:650},{x:1900,y:650},{x:1200,y:1350}];
  }else if(players===4){
    map.obstacles=[{x:850,y:510,w:270,h:180,type:deskTheme?'book':'cup'},{x:1280,y:510,w:270,h:180,type:'book'},{x:850,y:910,w:270,h:180,type:deskTheme?'eraser':'sponge'},{x:1280,y:910,w:270,h:180,type:deskTheme?'book':'sponge'}];
    map.lanes=[[[650,250],[1200,390],[1750,250]],[[650,1350],[1200,1210],[1750,1350]],[[650,250],[650,800],[650,1350]],[[1750,250],[1750,800],[1750,1350]],[[1200,390],[1200,800],[1200,1210]],[[440,800],[1200,800],[1960,800]]];
    map.expansions=[{x:440,y:800},{x:1960,y:800},{x:1200,y:380},{x:1200,y:1220},{x:1200,y:800}];
  }
  map.players=players;return map;
}
const COMBAT=['rock','paper','scissors'];
const clamp=(v,min,max)=>Math.max(min,Math.min(max,v));
const d2=(a,b)=>(a.x-b.x)**2+(a.y-b.y)**2;
const dist=(a,b)=>Math.hypot(a.x-b.x,a.y-b.y);
const ok=()=>({ok:true});
const fail=reason=>({ok:false,reason});
const configs={
  easy:{think:2.0,attack:95,group:7,workers:8,expand:150,cap:34},
  normal:{think:1.05,attack:65,group:9,workers:11,expand:90,cap:50},
  hard:{think:.55,attack:42,group:7,workers:15,expand:65,cap:70}
};

export class Game {
  constructor({map='desktop',difficulty='normal',players=2,onEvent=()=>{}}={}) {
    this.width=2400; this.height=1600;
    this.players=clamp(Math.floor(Number(players)||2),2,4);
    this.map=layoutMap(map,this.players);this.starts=this.map.camps.map(c=>({...c}));
    this.difficulty=difficulty in configs?difficulty:'normal';
    this.onEvent=onEvent; this.entities=[]; this.effects=[]; this.time=0;
    this.winner=null; this._id=1; this._think=0;
    this._victoryCheck=0; this._separation=0;
    this.stats={trained:Array(this.players).fill(0),destroyed:Array(this.players).fill(0),collected:Array(this.players).fill(0)};
    this.teams=this.starts.map(start=>({trash:300,upgrades:{rock:false,paper:false,scissors:false},eliminated:false,start:{...start}}));
    this._bots=this.starts.map((_,team)=>({nextAttack:configs[this.difficulty].attack+team*2,lastExpansion:0}));
    for(let team=0;team<this.players;team++){
      const start=this.starts[team],length=Math.hypot(1200-start.x,800-start.y),fx=(1200-start.x)/length,fy=(800-start.y)/length,px=-fy,py=fx;
      const position=(forward,side)=>({x:start.x+fx*forward+px*side,y:start.y+fy*forward+py*side});
      this._spawn('desk',team,start.x,start.y);
      const printer=position(145,85),cabinet=position(-20,160);
      this._spawn('printer',team,printer.x,printer.y);
      this._spawn('cabinet',team,cabinet.x,cabinet.y);
      const ownCans=[];
      for(let i=0;i<6;i++){
        const angle=(-62.5+i*25)*Math.PI/180;
        const x=clamp(start.x-fx*Math.cos(angle)*205+px*Math.sin(angle)*205,48,this.width-48);
        const y=clamp(start.y-fy*Math.cos(angle)*205+py*Math.sin(angle)*205,48,this.height-48);
        ownCans.push(this._spawn('trash',-1,x,y,{amount:1500,collectorId:null,homeTeam:team}));
      }
      for(let i=0;i<5;i++){
        const p=position(-75,(i-2)*32),w=this._spawn('twig',team,p.x,p.y);
        w.order={type:'gather',targetId:ownCans[i].id};w.resourceId=ownCans[i].id;w.workState='toCan';
      }
      COMBAT.forEach((type,i)=>{const p=position(225,(i-1)*48);this._spawn(type,team,p.x,p.y);});
    }
    const neutralCans=this.players===2?(this.map.id==='breakroom'?[[780,530],[960,1210],[1070,790],[1330,790],[1440,300],[1620,1060]]:[[780,530],[780,1060],[1070,790],[1330,790],[1620,530],[1620,1060]]):this.map.expansions.flatMap(p=>[[p.x-45,p.y-135],[p.x+45,p.y-135]]);
    for(const [x,y] of neutralCans){
      if(!this.map.obstacles.some(o=>x>o.x-30&&x<o.x+o.w+30&&y>o.y-30&&y<o.y+o.h+30))this._spawn('trash',-1,x,y,{amount:1800,collectorId:null});
    }
  }
  getEntity(id){return this.entities.find(e=>e.id===id&&e.hp>0);}
  _spawn(type,team,x,y,extra={}){
    const t=TYPES[type];
    const e={id:this._id++,type,team,x,y,hp:t.hp,maxHp:t.hp,r:t.r,
      facing:team?Math.PI:0,cooldown:0,abilityCooldown:0,paperInvulnerable:0,
      order:{type:'idle'},queue:[],constructing:0,carrying:0,workTimer:0,...extra};
    if(type==='trash')e.maxAmount=e.amount||1500;
    this.entities.push(e);if(t.building)this._collisionBuildings=null;return e;
  }
  _event(type,details={}){this.onEvent({type,...details});}
  _effect(type,x,y,extra={}){const life=type==='death'?.7:type==='sweep'?.45:.5;this.effects.push({type,x,y,life,total:life,...extra});}
  getSupply(team){
    const own=this.entities.filter(e=>e.team===team&&e.hp>0);
    let cap=0,units=0,reserved=0;
    for(const e of own){if(TYPES[e.type].building){if(!e.constructing)cap+=TYPES[e.type].supplyCap||0;for(const q of e.queue)if(q.kind==='train')reserved++;}else units++;}
    return {used:units+reserved,cap:Math.min(100,cap),reserved,units};
  }
  train(buildingId,type){
    const b=this.getEntity(buildingId),t=TYPES[type];
    if(!b||!t)return fail('Choose a production building.');
    if((b.type==='desk'&&type!=='twig')||(b.type==='printer'&&!COMBAT.includes(type))||!['desk','printer'].includes(b.type))return fail('Choose a desk for twigs or a printer for troops.');
    if(b.constructing)return fail('This building is still being built.');
    if(b.queue.length>=6)return fail('The production queue is full.');
    const supply=this.getSupply(b.team);
    if(supply.used>=supply.cap)return fail('Supply full. Build a cabinet.');
    if(this.teams[b.team].trash<t.cost)return fail('Not enough trash.');
    this.teams[b.team].trash-=t.cost;
    b.queue.push({kind:'train',type,total:t.trainTime,remaining:t.trainTime});
    return ok();
  }
  research(printerId,type){
    const b=this.getEntity(printerId);
    if(!b||b.type!=='printer')return fail('Select a printer first.');
    if(!COMBAT.includes(type))return fail('Unknown upgrade.');
    if(b.constructing)return fail('This printer is still being built.');
    if(this.teams[b.team].upgrades[type])return fail('Already researched.');
    if(this.entities.some(e=>e.team===b.team&&e.queue.some(q=>q.kind==='research'&&q.type===type)))return fail('That upgrade is already in the queue.');
    if(b.queue.length>=6)return fail('The production queue is full.');
    if(this.teams[b.team].trash<150)return fail('This upgrade costs 150 trash.');
    this.teams[b.team].trash-=150;b.queue.push({kind:'research',type,total:14,remaining:14});return ok();
  }
  canBuild(type,x,y){
    if(!TYPES[type]?.building)return false;
    const r=TYPES[type].r+10;
    if(x<r||y<r||x>this.width-r||y>this.height-r)return false;
    if(this.map.obstacles.some(o=>x>o.x-r&&x<o.x+o.w+r&&y>o.y-r&&y<o.y+o.h+r))return false;
    return !this.entities.some(e=>e.hp>0&&(TYPES[e.type].building||e.type==='trash')&&Math.hypot(e.x-x,e.y-y)<e.r+r+8);
  }
  build(workerIds,type,x,y){
    const workers=workerIds.map(id=>this.getEntity(id)).filter(e=>e?.type==='twig');
    if(!workers.length)return fail('Select a twig to build.');
    const team=workers[0].team,t=TYPES[type];
    if(!t?.building)return fail('Choose a building.');
    if(!this.canBuild(type,x,y))return fail('Leave space around buildings and trash cans.');
    if(this.teams[team].trash<t.cost)return fail('Not enough trash.');
    this.teams[team].trash-=t.cost;
    const b=this._spawn(type,team,x,y,{constructing:t.buildTime,buildTotal:t.buildTime,hp:Math.round(t.hp*.2)});
    const moved=[];
    for(const u of this.entities)if(u.id!==b.id&&!TYPES[u.type].building&&u.type!=='trash'&&!u.leap&&dist(u,b)<u.r+b.r+3){const free=this._freePoint(u.x,u.y,u.r,moved);u.x=free.x;u.y=free.y;u.path=null;moved.push({...free,r:u.r});}
    const w=workers.sort((a,b)=>Math.hypot(a.x-x,a.y-y)-Math.hypot(b.x-x,b.y-y))[0];
    this._releaseCan(w);w.order={type:'build',targetId:b.id};w.path=null;
    b.builderId=w.id;this._effect('build',x,y,{team});return {...ok(),entityId:b.id};
  }
  command(ids,order){
    const units=ids.map(id=>this.getEntity(id)).filter(e=>e&&!TYPES[e.type].building&&e.type!=='trash');
    const moving=order.type==='move'||order.type==='attackMove';
    const cols=Math.ceil(Math.sqrt(units.length));
    const destinations=[];
    units.forEach((e,i)=>{
      this._releaseCan(e);e.path=null;e.detour=null;e.moveSample=null;e.goalProgress=null;e.combatTarget=null;
      const o={...order};
      if(moving){const desiredX=clamp(order.x+(i%cols-(cols-1)/2)*44,22,this.width-22),desiredY=clamp(order.y+(Math.floor(i/cols)-(Math.ceil(units.length/cols)-1)/2)*44,22,this.height-22);const free=this._freePoint(desiredX,desiredY,e.r,destinations);o.x=free.x;o.y=free.y;o.arrivalRadius=units.length>1?Math.min(60,10+Math.sqrt(units.length)*6):10;destinations.push({...free,r:e.r});}
      if(o.type==='gather'&&e.type==='twig'){
        const can=this.getEntity(o.targetId);
        if(can?.type==='trash'&&can.amount>0){e.resourceId=can.id;e.workState=e.carrying?'toDesk':'toCan';e.order=o;}else e.order={type:'idle'};
      }else if(o.type==='gather')e.order={type:'move',x:o.x??e.x,y:o.y??e.y};
      else if(o.type==='stop')e.order={type:'idle'};
      else e.order=o;
    });
    return ok();
  }
  ability(ids,x,y){
    const units=ids.map(id=>this.getEntity(id)).filter(e=>e&&COMBAT.includes(e.type));
    const available=units.filter(e=>this.teams[e.team].upgrades[e.type]&&e.abilityCooldown<=0&&!e.leap);
    if(!available.length)return fail('Research an ability at a printer, or wait for its cooldown.');
    let rockIndex=0;const rocks=available.filter(e=>e.type==='rock').length,landings=[];
    for(const e of available){
      if(e.type==='paper'){
        e.paperInvulnerable=3;e.abilityCooldown=12;this._effect('crumple',e.x,e.y,{team:e.team});
      } else if(e.type==='rock'){
        let dx=x-e.x,dy=y-e.y,len=Math.hypot(dx,dy);const max=420;
        if(len>max){dx=dx/len*max;dy=dy/len*max;}
        const cols=Math.ceil(Math.sqrt(rocks));let tx=clamp(e.x+dx+(rockIndex%cols-(cols-1)/2)*40,25,this.width-25);
        let ty=clamp(e.y+dy+(Math.floor(rockIndex/cols)-(Math.ceil(rocks/cols)-1)/2)*40,25,this.height-25);rockIndex++;
        const free=this._freePoint(tx,ty,e.r,landings);tx=free.x;ty=free.y;landings.push({...free,r:e.r});
        e.leap={fromX:e.x,fromY:e.y,toX:tx,toY:ty,remaining:.65,total:.65};e.abilityCooldown=11;e.order={type:'attackMove',x:tx,y:ty};
      } else {
        const angle=Math.atan2(y-e.y,x-e.x);e.facing=angle;e.abilityCooldown=10;
        this._effect('sweep',e.x,e.y,{angle,radius:125,team:e.team});
        for(const target of this.entities){if(target.team<0||target.team===e.team||target.type!=='paper'||target.hp<=0||target.paperInvulnerable>0)continue;
          const len=dist(e,target),a=Math.atan2(target.y-e.y,target.x-e.x),diff=Math.atan2(Math.sin(a-angle),Math.cos(a-angle));
          if(len<125&&Math.abs(diff)<Math.PI*.34)this._damage(target,target.hp,e);
        }
      }
    }
    return ok();
  }
  update(dt){
    if(this.winner!==null)return;
    dt=clamp(dt,0,.08);this.time+=dt;
    this.effects=this.effects.filter(e=>(e.life-=dt)>0);
    for(const e of [...this.entities]){
      if(e.hp<=0||e.type==='trash')continue;
      e.cooldown=Math.max(0,e.cooldown-dt);e.attackAnim=Math.max(0,(e.attackAnim||0)-dt);e.abilityCooldown=Math.max(0,e.abilityCooldown-dt);e.paperInvulnerable=Math.max(0,e.paperInvulnerable-dt);
      if(TYPES[e.type].building){this._building(e,dt);continue;}
      e.desiredVX=0;e.desiredVY=0;
      if(e.leap){const l=e.leap;l.remaining-=dt;const t=1-Math.max(0,l.remaining)/l.total;e.x=l.fromX+(l.toX-l.fromX)*t;e.y=l.fromY+(l.toY-l.fromY)*t;if(l.remaining<=0){e.leap=null;this._effect('leap',e.x,e.y,{team:e.team,radius:60});}continue;}
      if(e.type==='twig')this._worker(e,dt);else this._fighter(e,dt);
    }
    this._separate(dt);
    for(const e of this.entities)if(e.hp<=0&&!e.deadHandled){e.deadHandled=true;this._releaseCan(e);this._effect('death',e.x,e.y,{team:e.team});this._event('death',{entity:e});}
    this.entities=this.entities.filter(e=>e.hp>0);
    this._think-=dt;if(this._think<=0){this._think=configs[this.difficulty].think;for(let team=1;team<this.players;team++)if(!this.teams[team].eliminated)this._bot(team);}
    this._victoryCheck-=dt;if(this._victoryCheck<=0){
      this._victoryCheck=1;
      for(let team=0;team<this.players;team++)if(!this.teams[team].eliminated&&!this.entities.some(e=>e.team===team&&e.type==='desk')){
        this.teams[team].eliminated=true;for(const e of this.entities)if(e.team===team)e.hp=0;
        this._event('eliminated',{team});
      }
      const survivors=this.teams.map((t,i)=>t.eliminated?-1:i).filter(i=>i>=0);
      if(survivors.length===1){this.winner=survivors[0];this._event('victory',{winner:this.winner});}
    }
  }
  _building(b,dt){
    if(b.constructing)return;
    const q=b.queue[0];if(!q)return;
    q.remaining-=dt;if(q.remaining>0)return;
    b.queue.shift();
    if(q.kind==='research'){
      this.teams[b.team].upgrades[q.type]=true;
      if(b.team===0)this._event('toast',{message:`${TYPES[q.type].label} ability ready! Select troops and use Special.`});
    }else{
      const p=this._freePoint(b.x+(b.team?-72:72),b.y+65,TYPES[q.type].r);
      const unit=this._spawn(q.type,b.team,p.x,p.y);this.stats.trained[b.team]++;
      if(q.type==='twig'){
        const rallyCan=this.getEntity(b.rally?.targetId);
        const can=rallyCan?.type==='trash'&&rallyCan.amount>0?rallyCan:!b.rally?this._chooseCan(unit):null;
        if(can){unit.order={type:'gather',targetId:can.id};unit.resourceId=can.id;unit.workState='toCan';}
        else if(b.rally&&Number.isFinite(b.rally.x)&&Number.isFinite(b.rally.y))unit.order={type:'move',x:b.rally.x,y:b.rally.y};
      }
      else if(b.rally){unit.order={type:'attackMove',...b.rally,arrivalRadius:65};}
      this._event('trained',{entity:unit});
    }
  }
  _worker(e,dt){
    const order=e.order;
    if(order.type==='build'){
      const b=this.getEntity(order.targetId);
      if(!b||!b.constructing){e.order={type:'idle'};return;}
      if(dist(e,b)>b.r+e.r+14){this._move(e,b.x,b.y,dt,b.r+e.r+10,b.id);return;}
      const before=b.constructing;b.constructing=Math.max(0,b.constructing-dt);b.hp=Math.min(b.maxHp,b.hp+b.maxHp*.8*dt/b.buildTotal);
      if(before>0&&b.constructing===0){b.hp=Math.round(Math.min(b.maxHp,b.hp));this._effect('build',b.x,b.y,{team:b.team});if(b.team===0)this._event('toast',{message:`${TYPES[b.type].label} complete.`});const can=this._chooseCan(e);if(can){e.order={type:'gather',targetId:can.id};e.resourceId=can.id;e.workState='toCan';}else e.order={type:'idle'};
        if(b.type==='desk'&&b.team>0){const nearbyCans=this.entities.filter(c=>c.type==='trash'&&c.amount>0&&dist(c,b)<440).sort((a,c)=>dist(a,b)-dist(c,b));const transfer=this.entities.filter(w=>w.type==='twig'&&w.team===b.team&&w.order.type==='gather').sort((a,c)=>dist(a,b)-dist(c,b)).slice(0,3);transfer.forEach((w,i)=>{const c=nearbyCans[i%nearbyCans.length];if(c)this.command([w.id],{type:'gather',targetId:c.id});});}
      }
      return;
    }
    if(order.type==='gather'){
      let can=this.getEntity(e.resourceId||order.targetId);
      if(!can||can.amount<=0){this._releaseCan(e);can=this._chooseCan(e);if(can){e.resourceId=can.id;e.order.targetId=can.id;}else{e.order={type:'idle'};return;}}
      if(e.carrying||e.workState==='toDesk'){
        const desk=this._nearest(e,t=>t.team===e.team&&t.type==='desk'&&!t.constructing);
        if(!desk){e.order={type:'idle'};return;}
        if(dist(e,desk)>desk.r+e.r+10){this._move(e,desk.x,desk.y,dt,desk.r+e.r+8,desk.id);return;}
        this.teams[e.team].trash+=e.carrying;this.stats.collected[e.team]+=e.carrying;
        this._effect('deposit',e.x,e.y,{team:e.team,value:e.carrying});this._event('deposit',{team:e.team,amount:e.carrying,x:e.x,y:e.y});e.carrying=0;e.workState='toCan';e.path=null;
      }else{
        if(can.collectorId&&!this.getEntity(can.collectorId))can.collectorId=null;
        if(dist(e,can)>can.r+e.r+8){this._move(e,can.x,can.y,dt,can.r+e.r+6,can.id);return;}
        if(can.collectorId&&can.collectorId!==e.id){e.workState='waiting';return;}
        can.collectorId=e.id;e.workState='collecting';e.workTimer+=dt;
        if(e.workTimer>=1.25){const amount=Math.min(5,can.amount);can.amount-=amount;e.carrying=amount;e.workTimer=0;can.collectorId=null;e.workState='toDesk';e.path=null;}
      }
      return;
    }
    if(order.type==='move'||order.type==='attackMove'){if(this._move(e,order.x,order.y,dt,order.arrivalRadius||8))e.order={type:'idle'};}
    else if(order.type==='attack'){
      const t=this.getEntity(order.targetId);if(!t){e.order={type:'idle'};return;}
      if(dist(e,t)>e.r+t.r+12)this._move(e,t.x,t.y,dt,e.r+t.r+8,t.id);
      else if(e.cooldown<=0){this._damage(t,5,e);e.cooldown=1.2;}
    }
  }
  _fighter(e,dt){
    let target=null;const o=e.order;
    if(o.type==='attack'){target=this.getEntity(o.targetId);if(!target)e.order={type:'idle'};}
    const fighting=o.type!=='move'&&e.paperInvulnerable<=0;
    if(!target&&fighting){target=e.combatTarget?this.getEntity(e.combatTarget):null;if(target&&(dist(e,target)>260||!this._canHurt(e,target)))target=null;
      if(!target)target=this._nearest(e,t=>t.team>=0&&t.team!==e.team&&this._canHurt(e,t)&&dist(e,t)<(o.type==='attackMove'?235:170));
      e.combatTarget=target?.id;
    }
    if(target&&e.paperInvulnerable<=0){
      const range=TYPES[e.type].range+target.r;
      e.facing=Math.atan2(target.y-e.y,target.x-e.x);
      if(dist(e,target)>range)this._move(e,target.x,target.y,dt,range-3,target.id);
      else if(e.cooldown<=0){
        if(this._canHurt(e,target))this._damage(target,TYPES[target.type].building?24:target.type==='twig'?30:target.type===e.type?25:55,e);
        e.cooldown=.85;
      }
      return;
    }
    if(o.type==='move'||o.type==='attackMove'){
      if(this._move(e,o.x,o.y,dt,o.arrivalRadius||10))e.order={type:'idle'};
    }
  }
  _canHurt(e,t){return t.hp>0&&t.team>=0&&t.team!==e.team&&(!COMBAT.includes(t.type)||t.type===e.type||TYPES[e.type].beats===t.type);}
  _damage(target,amount,source){
    source.attackAnim=.25;source.attackTarget={x:target.x,y:target.y};
    if(target.paperInvulnerable>0||target.leap||target.hp<=0)return;
    target.hp-=amount;target.lastHit=this.time;this._effect('hit',target.x,target.y,{team:source.team});
    this._event('hit',{source,target,amount});
    if(target.hp<=0&&source)this.stats.destroyed[source.team]++;
  }
  _releaseCan(e){
    if(e.type!=='twig')return;
    const can=this.getEntity(e.resourceId||e.order?.targetId);if(can?.collectorId===e.id)can.collectorId=null;
    e.workTimer=0;
  }
  _chooseCan(worker){
    const cans=this.entities.filter(e=>e.type==='trash'&&e.amount>0);
    let best=null,bestScore=Infinity;
    for(const can of cans){
      const desk=this._nearest(can,e=>e.team===worker.team&&e.type==='desk'&&!e.constructing);
      if(!desk)continue;
      const deskDistance=dist(can,desk);if(deskDistance>640)continue;
      const assigned=this.entities.filter(e=>e.type==='twig'&&e.team===worker.team&&e.id!==worker.id&&e.order.type==='gather'&&e.resourceId===can.id).length;
      const score=dist(worker,can)*.3+deskDistance+assigned*250;
      if(score<bestScore){best=can;bestScore=score;}
    }
    return best;
  }
  _nearest(point,predicate){let best=null,score=Infinity;for(const e of this.entities)if(e.hp>0&&predicate(e)){const d=d2(point,e);if(d<score){score=d;best=e;}}return best;}
  _blocked(x,y,r=17,ignoreId=null){
    if(x<r||y<r||x>this.width-r||y>this.height-r)return true;
    if(this.map.obstacles.some(o=>x>o.x-r&&x<o.x+o.w+r&&y>o.y-r&&y<o.y+o.h+r))return true;
    if(!this._collisionBuildings||this._collisionSource!==this.entities){this._collisionBuildings=this.entities.filter(e=>TYPES[e.type].building);this._collisionSource=this.entities;}
    return this._collisionBuildings.some(e=>e.id!==ignoreId&&e.hp>0&&Math.hypot(x-e.x,y-e.y)<r+e.r+2);
  }
  _freePoint(x,y,r,reserved=[]){
    const available=(px,py)=>!this._blocked(px,py,r)&&!reserved.some(p=>Math.hypot(px-p.x,py-p.y)<r+p.r+5);
    if(available(x,y))return {x,y};
    for(let ring=1;ring<25;ring++)for(let i=0;i<24;i++){const a=i/24*Math.PI*2,px=clamp(x+Math.cos(a)*ring*24,r,this.width-r),py=clamp(y+Math.sin(a)*ring*24,r,this.height-r);if(available(px,py))return {x:px,y:py};}
    return {x:clamp(x,r,this.width-r),y:clamp(y,r,this.height-r)};
  }
  _lineFree(ax,ay,bx,by,r,ignoreId){
    if(bx<r||by<r||bx>this.width-r||by>this.height-r)return false;
    const dx=bx-ax,dy=by-ay;
    for(const o of this.map.obstacles){
      let enter=0,leave=1;
      for(const [origin,delta,low,high] of [[ax,dx,o.x-r,o.x+o.w+r],[ay,dy,o.y-r,o.y+o.h+r]]){
        if(Math.abs(delta)<1e-9){if(origin<=low||origin>=high){enter=2;break;}}
        else {let t0=(low-origin)/delta,t1=(high-origin)/delta;if(t0>t1)[t0,t1]=[t1,t0];enter=Math.max(enter,t0);leave=Math.min(leave,t1);}
      }
      if(enter<leave-1e-8&&leave>1e-8&&enter<1)return false;
    }
    if(!this._collisionBuildings||this._collisionSource!==this.entities){this._collisionBuildings=this.entities.filter(e=>TYPES[e.type].building);this._collisionSource=this.entities;}
    const length2=dx*dx+dy*dy;
    for(const e of this._collisionBuildings){
      if(e.id===ignoreId||e.hp<=0)continue;
      const t=length2?clamp(((e.x-ax)*dx+(e.y-ay)*dy)/length2,0,1):0;
      if(Math.hypot(ax+dx*t-e.x,ay+dy*t-e.y)<r+e.r+2-1e-6)return false;
    }
    return true;
  }
  _path(e,tx,ty,ignoreId){
    const cell=64,cols=Math.ceil(this.width/cell),rows=Math.ceil(this.height/cell);
    const sx=clamp(Math.floor(e.x/cell),0,cols-1),sy=clamp(Math.floor(e.y/cell),0,rows-1),gx=clamp(Math.floor(tx/cell),0,cols-1),gy=clamp(Math.floor(ty/cell),0,rows-1);
    const key=(x,y)=>y*cols+x,start=key(sx,sy),goal=key(gx,gy);const open=[start],came=new Map(),g=new Map([[start,0]]),closed=new Set();
    let found=start,best=Infinity,connected=false;
    for(let count=0;open.length&&count<1100;count++){
      let index=0,score=Infinity;
      for(let i=0;i<open.length;i++){const id=open[i],x=id%cols,y=Math.floor(id/cols),f=g.get(id)+Math.hypot(gx-x,gy-y);if(f<score){score=f;index=i;}}
      const current=open.splice(index,1)[0],cx=current%cols,cy=Math.floor(current/cols);const h=Math.hypot(gx-cx,gy-cy);
      if(h<best){found=current;best=h;}
      const currentX=current===start?e.x:current===goal?tx:cx*cell+cell/2,currentY=current===start?e.y:current===goal?ty:cy*cell+cell/2;
      if(h<2.5&&this._lineFree(currentX,currentY,tx,ty,e.r,ignoreId)){found=current;connected=true;break;}
      closed.add(current);
      for(const [dx,dy] of [[1,0],[-1,0],[0,1],[0,-1],[1,1],[1,-1],[-1,1],[-1,-1]]){
        const nx=cx+dx,ny=cy+dy,id=key(nx,ny);if(nx<0||ny<0||nx>=cols||ny>=rows||closed.has(id))continue;
        const nodeX=id===goal?tx:nx*cell+cell/2,nodeY=id===goal?ty:ny*cell+cell/2;
        if(this._blocked(nodeX,nodeY,e.r,ignoreId))continue;
        if(current===start&&!this._lineFree(e.x,e.y,nodeX,nodeY,e.r,ignoreId))continue;
        if(id===goal&&!this._lineFree(current===start?e.x:cx*cell+cell/2,current===start?e.y:cy*cell+cell/2,tx,ty,e.r,ignoreId))continue;
        if(dx&&dy&&(this._blocked((cx+dx)*cell+cell/2,cy*cell+cell/2,e.r,ignoreId)||this._blocked(cx*cell+cell/2,(cy+dy)*cell+cell/2,e.r,ignoreId)))continue;
        const ng=g.get(current)+(dx&&dy?1.414:1);if(ng<(g.get(id)??Infinity)){came.set(id,current);g.set(id,ng);if(!open.includes(id))open.push(id);}
      }
    }
    const path=[];let c=found;while(c!==start&&came.has(c)){path.push(c===goal?{x:tx,y:ty}:{x:(c%cols)*cell+cell/2,y:Math.floor(c/cols)*cell+cell/2});c=came.get(c);}path.reverse();
    if(connected&&(path.length===0||path.at(-1).x!==tx||path.at(-1).y!==ty))path.push({x:tx,y:ty});return path;
  }
  _move(e,tx,ty,dt,stop=8,ignoreId=null){
    if(!Number.isFinite(tx)||!Number.isFinite(ty))return true;
    const distance=Math.hypot(e.x-tx,e.y-ty),directClear=this._lineFree(e.x,e.y,tx,ty,e.r,ignoreId);
    if(distance<=stop&&directClear){e.path=null;return true;}
    if((e.order.arrivalRadius||0)>10){
      if(!e.goalProgress||e.goalProgress.tx!==tx||e.goalProgress.ty!==ty||distance<e.goalProgress.best-12)e.goalProgress={tx,ty,best:distance,time:this.time};
      if(directClear&&this.time-e.goalProgress.time>4&&distance<Math.min(180,stop*3)&&this.entities.some(u=>u.id!==e.id&&u.hp>0&&!TYPES[u.type].building&&u.type!=='trash'&&!u.leap&&Math.hypot(u.x-tx,u.y-ty)<u.r+e.r+25&&Math.hypot(u.desiredVX||0,u.desiredVY||0)<1)){
        // Occupied formation slots are soft goals. Join the nearby squad rather
        // than endlessly circling troops already fighting at the destination.
        e.path=null;e.detour=null;return true;
      }
    }
    let px=tx,py=ty;
    if(!directClear){
      if(!e.path||!e.path.length||!e.pathGoal||Math.hypot(e.pathGoal.x-tx,e.pathGoal.y-ty)>75||this.time>(e.pathRefresh||0)){
        e.path=this._path(e,tx,ty,ignoreId);e.pathGoal={x:tx,y:ty};e.pathRefresh=this.time+2;
      }
      while(e.path?.length&&Math.hypot(e.x-e.path[0].x,e.y-e.path[0].y)<18&&(!e.path[1]||this._lineFree(e.x,e.y,e.path[1].x,e.path[1].y,e.r,ignoreId)))e.path.shift();
      for(let i=(e.path?.length||0)-1;i>0;i--)if(this._lineFree(e.x,e.y,e.path[i].x,e.path[i].y,e.r,ignoreId)){e.path=e.path.slice(i);break;}
      if(e.path?.length){px=e.path[0].x;py=e.path[0].y;}
    }else e.path=null;
    // A brief local detour lets late arrivals walk around a resting squad.
    // Terrain remains the authority: every candidate must have a clear segment.
    if(!e.moveSample||this.time-e.moveSample.time>.7){
      if(e.moveSample&&Math.hypot(e.x-e.moveSample.x,e.y-e.moveSample.y)<10&&distance>stop+4){
        const heading=Math.atan2(py-e.y,px-e.x),candidates=[];
        for(const offset of [Math.PI/2,-Math.PI/2,Math.PI/3,-Math.PI/3,Math.PI*.75,-Math.PI*.75])for(const radius of [90,135]){
          const cx=e.x+Math.cos(heading+offset)*radius,cy=e.y+Math.sin(heading+offset)*radius;
          if(!this._lineFree(e.x,e.y,cx,cy,e.r,ignoreId))continue;
          let crowd=0;for(const u of this.entities)if(u.id!==e.id&&u.hp>0&&!TYPES[u.type].building&&u.type!=='trash'&&!u.leap)crowd+=Math.max(0,80-Math.hypot(u.x-cx,u.y-cy));
          candidates.push({x:cx,y:cy,score:crowd*3+Math.hypot(cx-tx,cy-ty)*.1});
        }
        candidates.sort((a,b)=>a.score-b.score);if(candidates.length)e.detour={...candidates[0],until:this.time+2.2};
      }
      e.moveSample={x:e.x,y:e.y,time:this.time};
    }
    if(e.detour){if(this.time>e.detour.until||Math.hypot(e.x-e.detour.x,e.y-e.detour.y)<12)e.detour=null;else if(this._lineFree(e.x,e.y,e.detour.x,e.detour.y,e.r,ignoreId)){px=e.detour.x;py=e.detour.y;}else e.detour=null;}
    const dx=px-e.x,dy=py-e.y,len=Math.hypot(dx,dy);if(len<.1)return false;
    e.facing=Math.atan2(dy,dx);const speed=TYPES[e.type].speed*(e.paperInvulnerable>0?1.85:1),step=Math.min(speed*dt,len,directClear?Math.max(0,distance-stop+1):len);
    e.desiredVX=dx/len*speed;e.desiredVY=dy/len*speed;
    const vx=dx/len*step,vy=dy/len*step;
    if(!this._blocked(e.x+vx,e.y+vy,e.r,ignoreId)){e.x+=vx;e.y+=vy;}
    else if(!this._blocked(e.x+vx,e.y,e.r,ignoreId))e.x+=vx;
    else if(!this._blocked(e.x,e.y+vy,e.r,ignoreId))e.y+=vy;
    else {e.path=null;e.pathRefresh=0;}
    return false;
  }
  _separate(dt){
    const units=this.entities.filter(e=>e.hp>0&&!TYPES[e.type].building&&e.type!=='trash'&&!e.leap);
    // A positional contact solver makes moving and resting squads take up space.
    // All teams participate equally; airborne rocks do not block the ground.
    // Rebuilding nearby buckets each pass avoids checking every distant pair.
    const cell=88;
    for(let pass=0;pass<12;pass++){
      let contacts=false;
      const buckets=new Map();
      for(let i=0;i<units.length;i++){
        const e=units[i],key=`${Math.floor(e.x/cell)},${Math.floor(e.y/cell)}`;
        if(!buckets.has(key))buckets.set(key,[]);buckets.get(key).push(i);
      }
      for(let i=0;i<units.length;i++){
        const a=units[i],cx=Math.floor(a.x/cell),cy=Math.floor(a.y/cell);
        for(let ox=-1;ox<=1;ox++)for(let oy=-1;oy<=1;oy++)for(const j of buckets.get(`${cx+ox},${cy+oy}`)||[]){
          if(j<=i)continue;const b=units[j];
          let dx=b.x-a.x,dy=b.y-a.y,len=Math.hypot(dx,dy);const min=a.r+b.r+5;
          if(len>=min)continue;
          contacts=true;
          if(len<.0001){const angle=((a.id*97+b.id*31)%360)*Math.PI/180;dx=Math.cos(angle);dy=Math.sin(angle);len=1;}
          const penetration=min-len+.01,nx=dx/len,ny=dy/len,half=penetration*.5;
          const ax=a.x-nx*half,ay=a.y-ny*half,bx=b.x+nx*half,by=b.y+ny*half;
          const aFree=!this._blocked(ax,ay,a.r),bFree=!this._blocked(bx,by,b.r);
          if(aFree&&bFree){a.x=ax;a.y=ay;b.x=bx;b.y=by;}
          else if(aFree&&!this._blocked(a.x-nx*penetration,a.y-ny*penetration,a.r)){a.x-=nx*penetration;a.y-=ny*penetration;}
          else if(bFree&&!this._blocked(b.x+nx*penetration,b.y+ny*penetration,b.r)){b.x+=nx*penetration;b.y+=ny*penetration;}
          else {if(aFree){a.x=ax;a.y=ay;}if(bFree){b.x=bx;b.y=by;}}
          // Head-on traffic takes opposite sides instead of forming a deadlock.
          if(pass===0){
            const aForward=(a.desiredVX*nx+a.desiredVY*ny)>10,bForward=(b.desiredVX*nx+b.desiredVY*ny)<-10;
            const aRest=Math.hypot(a.desiredVX||0,a.desiredVY||0)<1,bRest=Math.hypot(b.desiredVX||0,b.desiredVY||0)<1;
            const side=dt*45,tx=-ny*side,ty=nx*side;
            if(aForward&&(bForward||bRest)&&!this._blocked(a.x+tx,a.y+ty,a.r)){a.x+=tx;a.y+=ty;}
            if(bForward&&(aForward||aRest)&&!this._blocked(b.x-tx,b.y-ty,b.r)){b.x-=tx;b.y-=ty;}
          }
        }
      }
      if(!contacts)break;
    }
  }
  _bot(team=1){
    const cfg=configs[this.difficulty],bank=this.teams[team],state=this._bots[team],start=this.starts[team];
    const own=this.entities.filter(e=>e.team===team&&e.hp>0),workers=own.filter(e=>e.type==='twig'),desks=own.filter(e=>e.type==='desk'&&!e.constructing),printers=own.filter(e=>e.type==='printer'&&!e.constructing),army=own.filter(e=>COMBAT.includes(e.type));
    const planningExpansion=this.time>cfg.expand&&this.time-state.lastExpansion>70&&desks.length<3&&!own.some(e=>e.type==='desk'&&e.constructing);
    const planningResearch=!planningExpansion&&this.time>100&&!COMBAT.every(t=>bank.upgrades[t])&&!own.some(e=>e.queue.some(q=>q.kind==='research'));
    const reserve=planningExpansion?200:planningResearch?150:0;
    for(const w of workers)if(w.order.type==='idle'){
      const unfinished=own.find(b=>b.constructing&&!this.getEntity(b.builderId));
      if(unfinished){unfinished.builderId=w.id;w.order={type:'build',targetId:unfinished.id};continue;}
      const can=this._chooseCan(w);if(can){w.resourceId=can.id;w.order={type:'gather',targetId:can.id};w.workState=w.carrying?'toDesk':'toCan';}
    }
    const queuedWorkers=desks.reduce((n,d)=>n+d.queue.filter(q=>q.type==='twig').length,0);
    if(workers.length+queuedWorkers<cfg.workers)for(const d of desks)if(d.queue.length<1)this.train(d.id,'twig');
    const supply=this.getSupply(team);
    const builders=workers.filter(w=>w.order.type!=='build');
    const pending=own.filter(e=>e.constructing);
    const constructNear=(type,x,y)=>{
      if(!builders.length)return false;
      for(let ring=0;ring<5;ring++)for(let i=0;i<10;i++){
        const a=i/10*Math.PI*2,px=x+Math.cos(a)*ring*80,py=y+Math.sin(a)*ring*80;
        if(this.canBuild(type,px,py)){const result=this.build(builders.map(w=>w.id),type,px,py);if(result.ok)return true;}
      }return false;
    };
    const forwardLength=Math.hypot(1200-start.x,800-start.y),fx=(1200-start.x)/forwardLength,fy=(800-start.y)/forwardLength,px=-fy,py=fx;
    if(supply.cap-supply.used<4&&supply.cap<cfg.cap&&!pending.some(e=>e.type==='cabinet')&&bank.trash>=75)constructNear('cabinet',start.x+px*245-fx*40,start.y+py*245-fy*40);
    if(printers.length<(this.time>120?3:2)&&this.time>35&&bank.trash>220+reserve&&!pending.some(e=>e.type==='printer'))constructNear('printer',start.x+fx*190+px*155,start.y+fy*190+py*155);
    for(const p of printers)if(p.queue.length<2&&army.length+this.getSupply(team).reserved<cfg.cap-8&&bank.trash>=50+reserve){
      const counts=COMBAT.map(t=>army.filter(e=>e.type===t).length);
      const observed=this.entities.filter(e=>e.team>=0&&e.team!==team&&COMBAT.includes(e.type)&&dist(e,start)<1300);
      const desired=COMBAT.map(t=>observed.filter(e=>e.type===TYPES[t].beats).length*1.6-counts[COMBAT.indexOf(t)]);
      const index=desired.indexOf(Math.max(...desired));this.train(p.id,COMBAT[index]);
    }
    if(planningExpansion&&bank.trash>=200&&!pending.some(e=>e.type==='desk')){
      const candidates=this.map.expansions.filter(p=>desks.every(d=>dist(p,d)>320)).sort((a,b)=>dist(a,start)-dist(b,start));
      for(const target of candidates)if(constructNear('desk',target.x,target.y)){state.lastExpansion=this.time;break;}
    }
    if(!planningExpansion&&this.time>80&&bank.trash>=150)for(const type of COMBAT){
      if(!bank.upgrades[type]&&!own.some(e=>e.queue.some(q=>q.kind==='research'&&q.type===type))){const p=printers.find(e=>e.queue.length<2);if(p){this.research(p.id,type);break;}}
    }
    // React to raids and then send reinforced waves toward the nearest enemy desk.
    const intruder=this.entities.find(e=>e.team>=0&&e.team!==team&&COMBAT.includes(e.type)&&own.some(b=>TYPES[b.type].building&&dist(e,b)<340));
    if(intruder){for(const e of army.filter(a=>dist(a,start)<650))e.order={type:'attackMove',x:intruder.x,y:intruder.y};}
    else if(this.time>=state.nextAttack&&army.length>=cfg.group){
      const target=this._nearest(start,e=>e.team>=0&&e.team!==team&&!this.teams[e.team].eliminated&&e.type==='desk');
      if(target){this.command(army.map(e=>e.id),{type:'attackMove',x:target.x,y:target.y});state.nextAttack=this.time+(this.difficulty==='hard'?25:this.difficulty==='normal'?38:55);}
    }
    if(this.difficulty!=='easy')for(const e of army){
      if(!bank.upgrades[e.type]||e.abilityCooldown>0)continue;
      const enemy=this._nearest(e,t=>t.team>=0&&t.team!==team&&COMBAT.includes(t.type));
      if(!enemy)continue;
      const d=dist(e,enemy);
      if(e.type==='paper'&&e.hp<60&&d<150)this.ability([e.id],e.x,e.y);
      else if(e.type==='scissors'&&enemy.type==='paper'&&d<115)this.ability([e.id],enemy.x,enemy.y);
      else if(e.type==='rock'&&enemy.type==='scissors'&&d>160&&d<400)this.ability([e.id],enemy.x,enemy.y);
    }
  }
}
