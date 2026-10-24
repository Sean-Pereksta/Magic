/* Circuitbound survival. DOM-free; coordinates are pixels except tile-facing APIs. */
(function(root,factory){
  if(typeof module==='object'&&module.exports)module.exports=factory();
  else root.CircuitSurvival=factory();
})(typeof globalThis==='object'?globalThis:this,function(){
  'use strict';
  const clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
  const finite=(n,f=0)=>Number.isFinite(n)?n:f;
  const DIFFICULTIES={
    casual:{damage:.65,health:.8,spawn:1.65,raid:1.45,loss:0,grace:180,max:22},
    standard:{damage:1,health:1,spawn:1,raid:1,loss:.15,grace:110,max:34},
    hardcore:{damage:1.35,health:1.25,spawn:.7,raid:.75,loss:.3,grace:75,max:46}
  };
  const REGIONS=[
    {id:'scrapyard',name:'Surface Scrapyards',color:'#9bbd8b',depth:0,tier:0},
    {id:'iron',name:'Iron Caverns',color:'#a4b6c4',depth:42/84,tier:1},
    {id:'crystal',name:'Crystal Hollows',color:'#78e6e8',depth:54/84,tier:2},
    {id:'industrial',name:'Forgotten Industrial Depths',color:'#d5a271',depth:65/84,tier:3},
    {id:'abyss',name:'Reactor Abyss',color:'#dc83a7',depth:74/84,tier:4}
  ];
  const ENEMIES={
    beetle:{name:'Scrap Beetle',hp:28,speed:36,damage:7,reach:25,wind:.62,cool:1.5,w:21,h:13,color:'#b89058',loot:{scrap:2,copper:1},move:'crawl'},
    crawler:{name:'Rust Crawler',hp:43,speed:64,damage:10,reach:28,wind:.52,cool:1.25,w:23,h:19,color:'#c96c51',loot:{scrap:3,iron:1},move:'ground'},
    wisp:{name:'Spark Wisp',hp:30,speed:61,damage:10,reach:35,wind:.8,cool:2,w:16,h:18,color:'#85edff',loot:{crystal:1,scrap:1},move:'fly'},
    burrower:{name:'Tunnel Burrower',hp:68,speed:50,damage:16,reach:34,wind:1,cool:2.1,w:25,h:19,color:'#c5a880',loot:{iron:3,crystal:1},move:'dig'},
    drone:{name:'Salvage Drone',hp:52,speed:48,damage:11,reach:240,wind:.9,cool:2.3,w:27,h:18,color:'#d9bd65',loot:{scrap:4,copper:2},move:'hover',ranged:true},
    leech:{name:'Circuit Leech',hp:35,speed:58,damage:5,reach:27,wind:.9,cool:2,w:20,h:13,color:'#c592ef',loot:{crystal:2,copper:1},move:'leech'},
    siege:{name:'Siege Automaton',hp:190,speed:25,damage:20,reach:39,wind:1.25,cool:2.2,w:29,h:38,color:'#cc7970',loot:{iron:7,scrap:6},move:'siege',structureDamage:22},
    sentinel:{name:'Core Sentinel',hp:155,speed:44,damage:15,reach:265,wind:1.05,cool:2.6,w:28,h:34,color:'#7ce2c0',loot:{crystal:4,steel:2},move:'guard',ranged:true},
    guardian:{name:'Reactor Guardian',hp:1300,speed:38,damage:25,reach:290,wind:1.5,cool:2.8,w:55,h:56,color:'#ef8b76',loot:{reactorCell:1,steel:12,crystal:15},move:'boss',ranged:true,structureDamage:14}
  };
  const WEAPONS={
    tool:{name:'Field Tool',damage:15,range:58,cool:.45,kind:'melee'},
    hammer:{name:'Engineering Hammer',damage:29,range:70,cool:.62,kind:'melee'},
    scrapLauncher:{name:'Scrap Launcher',damage:24,range:440,cool:.6,kind:'projectile',ammo:'ammo'},
    stunTool:{name:'Electrical Stun Tool',damage:18,range:118,cool:.85,kind:'stun',energy:8},
    energyRifle:{name:'Energy Projector',damage:48,range:570,cool:.48,kind:'energy',energy:7}
  };
  const EQUIPMENT=[
    ['reinforcedPickaxe','Reinforced Pickaxe','⛏',{iron:5,copper:3,wood:2},'Mine stone and copper 1.7× faster.','mining',1],
    ['steelDrillTool','Steel Drill Tool','⛏',{steel:4,iron:8,copper:5},'Mine all ore 2.6× faster.','mining',2],
    ['poweredMiningTool','Powered Mining Tool','⚒',{steel:8,crystal:8,copper:8,advancedComponent:2},'Uses stored energy to mine 4× faster; works without energy at normal speed.','mining',4],
    ['hammer','Engineering Hammer','⚒',{iron:6,wood:3,copper:2},'A heavy melee weapon. Attack with Q or the combat button.','weapons',1],
    ['scrapLauncher','Scrap Launcher','➶',{iron:6,copper:4,scrap:4},'Fires manufactured ammunition; cycle weapons with Tab.','weapons',2],
    ['stunTool','Electrical Stun Tool','ϟ',{crystal:5,copper:6,iron:3},'Stuns enemies at short range; uses stored energy.','weapons',2],
    ['energyRifle','Energy Projector','✦',{steel:8,crystal:10,copper:10,advancedComponent:2},'Fires powerful energy bolts; recharge near a live generator.','weapons',4],
    ['miningArmor','Mining Armor','♜',{iron:8,wood:3},'Reduces incoming damage by 20%. Equipped automatically.','weapons',1],
    ['insulatedArmor','Insulated Armor','♜',{iron:10,crystal:6,copper:4},'Reduces damage by 32%, with extra electrical resistance.','weapons',2],
    ['reinforcedArmor','Mechanical Armor','♜',{steel:10,iron:12},'Reduces damage by 45%. Equipped automatically.','weapons',4],
    ['shieldModule','Protective Shield Module','⬡',{steel:4,crystal:6,copper:5},'Stored energy absorbs up to half of an incoming hit.','weapons',3],
    ['medkit','Repair Dressing','✚',{wood:2,crystal:1},'Heal 45 health with Z or the heal button.','weapons',1]
  ];
  const CHAPTERS=[
    {id:1,title:'Wake the Foundry',unlocks:['Reinforced tools','Connected mining','Iron Caverns'],reward:'8 iron + 2 repair dressings',loot:{iron:8,medkit:2}},
    {id:2,title:'Into the Depths',unlocks:['Ranged weapons','Crystal Hollows','Advanced smelting'],reward:'6 crystal + 8 ammunition',loot:{crystal:6,ammo:8}},
    {id:3,title:'Under Siege',unlocks:['Shield module','Industrial Depths','Industrial power'],reward:'4 steel + 3 repair dressings',loot:{steel:4,medkit:3}},
    {id:4,title:'The Industrial Network',unlocks:['Powered equipment','Reactor Abyss','Reactor schematics'],reward:'10 crystal + 6 steel',loot:{crystal:10,steel:6}},
    {id:5,title:'The Reactor Below',unlocks:['Continued sandbox','Escalating optional raids'],reward:'Reactor engineering mastery',loot:{medkit:5}}
  ];
  function install(T,defs,items,recipes){
    for(const [key,name,icon,cost,desc,category,chapter] of EQUIPMENT){
      if(!recipes.some(r=>r.key===key))recipes.push({key,name,icon,cost,desc,category,out:1,chapter,equipment:true});
    }
    if(!recipes.some(r=>r.key==='ammo'))recipes.push({key:'ammo',name:'Scrap Ammunition',icon:'•',cost:{iron:1,copper:1},out:6,desc:'Launcher and turret ammunition. Automated manufacture is more efficient.',category:'weapons',chapter:1});
  }
  class SpatialGrid{
    constructor(cell=192){this.cell=cell;this.buckets=new Map();this.maxRadius=0;}
    rebuild(enemies){this.buckets.clear();this.maxRadius=0;for(const e of enemies){if(e.hp<=0)continue;this.maxRadius=Math.max(this.maxRadius,Math.max(e.w,e.h)/2);const k=Math.floor(e.x/this.cell)+','+Math.floor(e.y/this.cell);if(!this.buckets.has(k))this.buckets.set(k,[]);this.buckets.get(k).push(e);}}
    query(x,y,r){const found=[],extent=r+this.maxRadius;for(let cy=Math.floor((y-extent)/this.cell);cy<=Math.floor((y+extent)/this.cell);cy++)for(let cx=Math.floor((x-extent)/this.cell);cx<=Math.floor((x+extent)/this.cell);cx++)for(const e of this.buckets.get(cx+','+cy)||[])if(e.hp>0&&Math.hypot(e.x-x,e.y-y)<=r+Math.max(e.w,e.h)/2)found.push(e);return found;}
  }
  class Survival{
    constructor(options){
      Object.assign(this,{tile:24,width:220,height:84,notify:()=>{},random:Math.random},options);
      this.getMeta=typeof options.meta==='function'?options.meta:()=>options.meta||new Map();
      this.grid=new SpatialGrid(this.tile*8);this.reset();
    }
    reset(){
      this.enemies=[];this.projectiles=[];this.effects=[];this.caches=[];this.nextEnemy=1;this.nextCache=1;this.elapsed=0;
      this.difficulty='standard';this.energy=100;this.maxEnergy=100;this.weapon=this.inventory?.hammer?'hammer':'tool';this.attackCooldown=0;this.attackAnimation=0;
      this.invulnerable=4;this.lastDamage=-100;this.spawnClock=8;this.worldClock=0;this.spawnFailures=0;this.nextRaid=260;
      this.raid={state:'quiet',remaining:0,count:0,id:0};this.completed=[];this.discoveredRegions=['scrapyard'];this.chapter=1;this.powered=[];this.machineKeys=[];
      this.maxDepth=0;this.verticalBuilt=0;this.powerCapacity=0;this.runningKinds=0;this.poweredLamp=false;this.runningMachine=false;this.bossSpawned=false;
      this.checkpoint={x:finite(this.player?.x,9*this.tile),y:finite(this.player?.y,25*this.tile)};
      if(this.player){this.player.maxHealth=100;this.player.health=clamp(finite(this.player.health,100),1,100);}
      this.grid.rebuild(this.enemies);
    }
    regionAt(x,y){let region=REGIONS[0];for(const r of REGIONS)if(y/this.height>=r.depth)region=r;return region;}
    config(){return DIFFICULTIES[this.difficulty];}
    setDifficulty(name){const value=String(name).toLowerCase();if(!DIFFICULTIES[value])return false;this.difficulty=value;if(this.elapsed<10&&this.raid.state==='quiet')this.nextRaid=260*this.config().raid;return true;}
    center(){return{x:this.player.x+(this.player.w||15)/2,y:this.player.y+(this.player.h||34)/2};}
    inside(x,y){return x>=1&&y>=1&&x<this.width-1&&y<this.height-1;}
    tileKey(x,y){return x+y*this.width;}
    solid(x,y,dig=false){
      if(!this.inside(x,y))return true;
      const id=this.get(x,y),d=this.defs[id],m=this.getMeta().get(this.tileKey(x,y));
      if(d?.door&&m?.open)return false;
      if(dig&&[this.T.DIRT,this.T.STONE,this.T.GRASS].includes(id))return false;
      return !!d?.solid;
    }
    blocked(x,y,w,h,dig=false,spawn=false){
      for(let ty=Math.floor((y-h/2)/this.tile);ty<=Math.floor((y+h/2-.01)/this.tile);ty++)for(let tx=Math.floor((x-w/2)/this.tile);tx<=Math.floor((x+w/2-.01)/this.tile);tx++){
        if(!this.inside(tx,ty)||this.solid(tx,ty,dig))return true;
        if(spawn&&this.get(tx,ty)!==this.T.AIR)return true;
      }return false;
    }
    lineOfSight(x,y,tx,ty,allowTarget=false){const steps=Math.max(1,Math.ceil(Math.hypot(tx-x,ty-y)/(this.tile*.35)));for(let i=1;i<steps;i++){const sx=Math.floor((x+(tx-x)*i/steps)/this.tile),sy=Math.floor((y+(ty-y)*i/steps)/this.tile);if(allowTarget&&sx===Math.floor(tx/this.tile)&&sy===Math.floor(ty/this.tile))continue;if(this.solid(sx,sy))return false;}return true;}
    validSpawn(type,x,y,ignoreDistance=false,raid=false){
      const d=ENEMIES[type];if(!d||this.blocked(x,y,d.w,d.h,false,true))return false;
      const p=this.center(),distance=Math.hypot(p.x-x,p.y-y),startDistance=Math.hypot(this.checkpoint.x-x,this.checkpoint.y-y);
      if(!ignoreDistance&&(distance<this.tile*10||(!raid&&distance>this.tile*35)))return false;
      if(!ignoreDistance&&startDistance<this.tile*(this.elapsed<this.config().grace?22:10))return false;
      // No spawning through a machine, including non-solid power/signal components.
      for(const k of this.machineKeys){const mx=k%this.width,my=Math.floor(k/this.width);if(Math.hypot(mx*this.tile+this.tile/2-x,my*this.tile+this.tile/2-y)<this.tile*2.2)return false;}
      if(['crawl','ground','siege','guard','boss','dig'].includes(d.move)&&!this.solid(Math.floor(x/this.tile),Math.floor((y+d.h/2+3)/this.tile)))return false;
      return !this.enemies.some(e=>e.hp>0&&Math.hypot(e.x-x,e.y-y)<this.tile*2);
    }
    spawn(type,x,y,options={}){
      if(this.enemies.length>=this.config().max||!this.validSpawn(type,x,y,!!options.ignoreDistance,!!options.raid))return null;
      const d=ENEMIES[type],health=Math.round(d.hp*this.config().health*(1+Math.max(0,this.chapter-2)*.08));
      const e={id:this.nextEnemy++,type,x,y,w:d.w,h:d.h,hp:health,maxHealth:health,vx:0,vy:0,facing:1,cool:.5+this.random(),wind:0,stun:0,flash:0,jump:0,stuck:0,age:0,anchorX:x,anchorY:y,raid:options.raid||0};
      this.enemies.push(e);this.grid.rebuild(this.enemies);return e;
    }
    findSpawn(type,raid=0){
      const p=raid?(this.nearestMachine(this.checkpoint.x,this.checkpoint.y,true)||this.center()):this.center(),d=ENEMIES[type];
      for(let tries=0;tries<75;tries++){
        const direction=this.random()<.5?-1:1,tx=clamp(Math.floor(p.x/this.tile)+direction*(12+Math.floor(this.random()*17)),2,this.width-3);
        const py=Math.floor(p.y/this.tile),flying=['fly','hover','leech'].includes(d.move);
        let ty=clamp(py+Math.floor(this.random()*15)-7,2,this.height-3);
        if(!flying){
          // Search nearby surfaces, including cavern floors; never teleport into terrain.
          const start=clamp(py-9,2,this.height-3),end=clamp(py+15,2,this.height-2),surfaces=[];
          for(let y=start;y<end;y++)if(!this.solid(tx,y)&&this.solid(tx,y+1))surfaces.push(y);
          if(!surfaces.length)continue;ty=surfaces[Math.floor(this.random()*surfaces.length)];
        }
        const x=(tx+.5)*this.tile,y=flying?(ty+.5)*this.tile:(ty+1)*this.tile-d.h/2-1;
        const e=this.spawn(type,x,y,{raid});if(e)return e;
      }return null;
    }
    chooseType(){
      const p=this.center(),r=this.regionAt(p.x/this.tile,p.y/this.tile),night=(Math.sin(this.elapsed*.012)+1)/2<.35;
      let pool=r.tier===0?['beetle','beetle','crawler']:r.tier===1?['crawler','beetle','burrower']:r.tier===2?['wisp','burrower','drone']:r.tier===3?['drone','sentinel','siege']:['sentinel','siege','wisp'];
      if(night&&r.tier<2)pool=pool.concat(['wisp']);if(this.powered.length>=4&&this.chapter>=2)pool=pool.concat(['leech']);
      return pool[Math.floor(this.random()*pool.length)];
    }
    query(x,y,r){return this.grid.query(x,y,r);}
    availableWeapons(){return Object.keys(WEAPONS).filter(k=>k==='tool'||this.inventory[k]>0);}
    cycleWeapon(){const list=this.availableWeapons();this.weapon=list[(list.indexOf(this.weapon)+1)%list.length];this.notify('Equipped '+WEAPONS[this.weapon].name);return this.weapon;}
    selectWeapon(key){if(!WEAPONS[key]||(key!=='tool'&&!this.inventory[key]))return false;this.weapon=key;return true;}
    armor(){if(this.inventory.reinforcedArmor)return{name:'Mechanical Armor',reduction:.45};if(this.inventory.insulatedArmor)return{name:'Insulated Armor',reduction:.32};if(this.inventory.miningArmor)return{name:'Mining Armor',reduction:.2};return{name:'Field Clothing',reduction:0};}
    miningMultiplier(id){if(this.inventory.poweredMiningTool&&this.energy>1)return 4;if(this.inventory.steelDrillTool)return 2.6;if(this.inventory.reinforcedPickaxe)return [this.T.STONE,this.T.COPPER,this.T.DIRT,this.T.GRASS].includes(id)?1.7:1.35;return 1;}
    spendToolEnergy(dt){if(this.inventory.poweredMiningTool&&this.energy>0)this.energy=Math.max(0,this.energy-dt*4);}
    attack(targetX,targetY){
      if(this.attackCooldown>0||this.player.health<=0)return false;
      if(!this.availableWeapons().includes(this.weapon))this.weapon='tool';
      const w=WEAPONS[this.weapon],p=this.center();
      if(w.ammo&&!(this.inventory[w.ammo]>0)){this.notify('No ammunition — craft ammo or switch weapon with Tab');return false;}
      if(w.energy&&this.energy<w.energy){this.notify('Energy low — recharge near a live generator or battery');return false;}
      if(w.ammo)this.inventory[w.ammo]--;if(w.energy)this.energy-=w.energy;
      const dx=finite(targetX,p.x+this.player.facing*80)-p.x,dy=finite(targetY,p.y)-p.y,angle=Math.atan2(dy,dx);
      this.player.facing=Math.cos(angle)>=0?1:-1;this.attackCooldown=w.cool;this.attackAnimation=.19;this.attackAngle=angle;
      if(w.kind==='projectile'||w.kind==='energy'){
        this.projectiles.push({x:p.x,y:p.y,vx:Math.cos(angle)*460,vy:Math.sin(angle)*460,life:w.range/460,damage:w.damage,owner:'player',kind:w.kind,color:w.kind==='energy'?'#85fff0':'#ffdc8a'});
      }else{
        let hits=0;for(const e of this.query(p.x,p.y,w.range)){
          const direction=Math.atan2(e.y-p.y,e.x-p.x),delta=Math.atan2(Math.sin(direction-angle),Math.cos(direction-angle));
          if(Math.abs(delta)>1.5||!this.lineOfSight(p.x,p.y,e.x,e.y))continue;
          this.hit(e,w.damage,'player');if(w.kind==='stun')e.stun=Math.max(e.stun,1.7);else{e.vx+=Math.cos(angle)*110;e.vy-=65;}if(++hits>=3)break;
        }
        this.effects.push({x:p.x+Math.cos(angle)*w.range*.6,y:p.y+Math.sin(angle)*w.range*.6,life:.18,max:.18,kind:'slash',angle,color:w.kind==='stun'?'#8cffff':'#ffdc8a'});
      }return true;
    }
    hit(enemy,amount,source='player'){
      if(!enemy||enemy.hp<=0||!Number.isFinite(amount)||amount<=0)return false;
      // Burrowers are vulnerable when exposed; the wind-up always emerges into clear air.
      if(enemy.type==='burrower'&&this.blocked(enemy.x,enemy.y,enemy.w,enemy.h)&&enemy.wind<=0)return false;
      enemy.hp-=amount;enemy.flash=.14;this.effects.push({x:enemy.x,y:enemy.y,life:.23,max:.23,kind:'hit',color:'#ffeaa0',amount:Math.ceil(amount)});
      if(enemy.hp<=0){
        this.stats.kills=(this.stats.kills||0)+1;if(['turret','arc','tesla','trap','defense','electric'].includes(source))this.stats.defenseKills=(this.stats.defenseKills||0)+1;
        if(enemy.type==='guardian'){this.stats.guardianDefeated=true;this.notify('Reactor Guardian defeated — the abyss is yours');}
        const loot=ENEMIES[enemy.type].loot;
        for(const [key,count] of Object.entries(loot))this.rewardDrop(key,count,enemy.x,enemy.y);
        this.effects.push({x:enemy.x,y:enemy.y,life:.5,max:.5,kind:'burst',color:ENEMIES[enemy.type].color});
      }return true;
    }
    rewardDrop(key,count,x,y){
      // Enemy rewards remain recoverable even when the physical-item budget is full.
      if(!this.engine?.spawn?.(key,count,x/this.tile,y/this.tile)){
        const cache=this.caches.find(c=>Math.hypot(c.x-x,c.y-y)<this.tile*2);
        if(cache)cache.items[key]=(cache.items[key]||0)+count;else this.caches.push({id:this.nextCache++,x,y,items:{[key]:count},kind:'salvage'});
      }
    }
    inventoryRoom(){return Math.max(0,(this.inventoryCapacity||2048)-Object.values(this.inventory).reduce((n,v)=>n+(Number.isFinite(v)?Math.max(0,v):0),0));}
    grantReward(key,count){const n=Math.min(count,this.inventoryRoom());if(n)this.inventory[key]=(this.inventory[key]||0)+n;if(n<count){const p=this.center();this.rewardDrop(key,count-n,p.x,p.y);}}
    damagePlayer(amount,source='impact'){
      if(this.invulnerable>0||this.player.health<=0||!Number.isFinite(amount)||amount<=0)return false;
      let damage=amount*this.config().damage*(1-this.armor().reduction);
      if(this.inventory.insulatedArmor&&['wisp','energy','electric','leech'].includes(source))damage*=.65;
      if(this.inventory.shieldModule&&this.energy>0){const absorbed=Math.min(damage*.5,this.energy/1.5);damage-=absorbed;this.energy-=absorbed*1.5;}
      this.player.health=Math.max(0,this.player.health-damage);this.lastDamage=this.elapsed;this.invulnerable=.55;this.player.hurt=.2;
      if(this.player.health<=0)this.respawn();return true;
    }
    heal(){if(this.player.health>=100){this.notify('Health is full');return false;}const item=this.inventory.medkit>0?'medkit':this.inventory.repairKit>0?'repairKit':null;if(!item){this.notify('Craft a repair dressing or repair kit, or recover near your powered base');return false;}this.inventory[item]--;this.player.health=Math.min(100,this.player.health+45);this.notify('Repairs applied: +45 health');return true;}
    safeCheckpoint(){
      const p=this.checkpoint,w=this.player.w||15,h=this.player.h||34;
      for(let radius=0;radius<14;radius++)for(let dy=-radius;dy<=radius;dy++)for(let dx=-radius;dx<=radius;dx++){
        if(radius&&Math.max(Math.abs(dx),Math.abs(dy))!==radius)continue;
        const x=p.x+dx*this.tile,y=p.y+dy*this.tile;
        if(!this.blocked(x+w/2,y+h/2,w,h)&&this.solid(Math.floor((x+w/2)/this.tile),Math.floor((y+h+2)/this.tile)))return{x,y};
      }
      // Search the entire spawn column before considering neighboring columns.
      for(let x=2;x<this.width-2;x++)for(let y=2;y<this.height-2;y++)if(!this.blocked((x+.5)*this.tile,(y+1)*this.tile-h/2,w,h)&&this.solid(x,y+1))return{x:(x+.5)*this.tile-w/2,y:(y+1)*this.tile-h};
      return{x:p.x,y:p.y};
    }
    setCheckpoint(){
      const p=this.center(),live=this.powered.some(k=>{const x=(k%this.width+.5)*this.tile,y=(Math.floor(k/this.width)+.5)*this.tile;return Math.hypot(x-p.x,y-p.y)<this.tile*7;});
      if(!live){this.notify('Establish a checkpoint within 7 blocks of live machinery');return false;}
      if(this.blocked(p.x,p.y,this.player.w||15,this.player.h||34)){this.notify('Stand in a clear space to set the checkpoint');return false;}
      this.checkpoint={x:this.player.x,y:this.player.y};this.notify('Base checkpoint established');return true;
    }
    respawn(){
      const p=this.center(),items={},loss=this.config().loss;
      for(const key of ['dirt','stone','wood','copper','iron','crystal','obsidian','scrap','steel','ammo']){const n=Math.floor((this.inventory[key]||0)*loss);if(n){this.inventory[key]-=n;items[key]=n;}}
      if(Object.keys(items).length)this.caches.push({id:this.nextCache++,x:p.x,y:p.y,items,kind:'recovery'});
      this.stats.deaths=(this.stats.deaths||0)+1;const safe=this.safeCheckpoint();Object.assign(this.player,safe,{vx:0,vy:0,health:100,onGround:false});this.energy=Math.max(this.energy,30);this.invulnerable=7;this.lastDamage=this.elapsed;
      this.notify(Object.keys(items).length?'Recovered at base — reclaim the marked resource cache. Equipment is safe.':'Recovered at base — equipment and resources retained.');
    }
    damageStructure(tx,ty,amount){
      const id=this.get(tx,ty),d=this.defs[id];if(!d||id<9||id===this.T.CORE)return false;
      const meta=this.getMeta(),key=this.tileKey(tx,ty),m=meta.get(key)||{};m.maxHp=m.maxHp||d.hp||Math.max(45,(d.strength||8)*12);m.hp=finite(m.hp,m.maxHp)-amount;m.underAttack=this.elapsed+1;meta.set(key,m);
      if(m.hp>0)return true;
      const spill={...m.contents};for(const [key,n] of Object.entries(m.outputs||{}))spill[key]=(spill[key]||0)+n;if(d.drop)spill[d.drop]=(spill[d.drop]||0)+1;
      // Reserve room for every stack before removing a structure; a full world cannot erase cargo.
      const needed=Object.entries(spill).filter(([key,n])=>n>0&&!this.engine?.drops?.some(v=>v.key===key&&Math.abs(v.x-(tx+.5))<.4&&Math.abs(v.y-(ty+.5))<.4)).length;
      if((this.engine?.drops?.length||0)+needed>512){m.hp=1;m.fault='Damage: clear loose items before recovering this machine';return true;}
      if(!this.set)return false;this.set(tx,ty,this.T.AIR);for(const [key,n] of Object.entries(spill))if(n>0)this.rewardDrop(key,n,(tx+.5)*this.tile,(ty+.5)*this.tile);
      this.effects.push({x:(tx+.5)*this.tile,y:(ty+.5)*this.tile,life:.45,max:.45,kind:'burst',color:'#d59b6a'});return true;
    }
    scanWorld(){
      this.powered=[];this.machineKeys=[];this.verticalBuilt=0;this.powerCapacity=0;this.runningMachine=false;this.poweredLamp=false;const kinds=new Set();
      for(const [key,m] of this.getMeta()){
        const x=key%this.width,y=Math.floor(key/this.width),id=this.get(x,y),d=this.defs[id];if(!d||id===this.T.AIR)continue;
        if(m.leechUntil&&m.leechUntil<=(this.engine?.elapsed||this.elapsed)){m.leechUntil=0;m.leechDrain=0;if(m.fault?.startsWith('Circuit leech'))m.fault='';}
        if(d.kind||d.electric||d.mechanical||d.transport)this.machineKeys.push(key);
        if((m.powered||d.source>0)&&!m.disabled)this.powered.push(key);
        if(id===this.T.LAMP&&m.powered&&m.signalOK!==false)this.poweredLamp=true;
        if((m.running||m.working)&&id!==this.T.LAMP){this.runningMachine=true;kinds.add(id);}
      }
      // Structures without machine metadata are deliberately counted too.
      for(let y=Math.floor(this.height*.44);y<this.height-1;y++)for(let x=0;x<this.width;x++)if(this.defs[this.get(x,y)]?.ladder||this.defs[this.get(x,y)]?.rope)this.verticalBuilt++;
      this.engine?.rebuild?.();
      // Generation milestones require one actual connected grid with a consumer.
      this.powerCapacity=Math.max(0,...(this.engine?.powerNets||[]).map(net=>net.some(k=>this.defs[this.get(k%this.width,Math.floor(k/this.width))]?.load>0&&!this.getMeta().get(k)?.disabled)?net.reduce((total,k)=>total+(this.getMeta().get(k)?.disabled?0:this.defs[this.get(k%this.width,Math.floor(k/this.width))]?.source||0),0):0));
      this.runningKinds=kinds.size;
    }
    objectiveData(){
      const metrics=this.engine?.metrics||{},s=this.stats,tool=!!(this.inventory.reinforcedPickaxe||this.inventory.hammer||this.inventory.steelDrillTool),depth=Math.floor(this.maxDepth);
      const obj=(id,label,current,target)=>({id,label,current:typeof current==='boolean'?Number(current):finite(current),target,complete:current>=target});
      return[
        [obj('copper','Mine 8 copper ore',s.copperMined||0,8),obj('tool','Craft a reinforced pickaxe or engineering hammer',tool,1),obj('machine','Operate a powered machine',this.runningMachine||s.machineOperated,1),obj('core','Restore the outpost Foundry Core',s.coreReached,1)],
        [obj('shaft','Build a 12-section route into the mine',this.verticalBuilt,12),obj('depth','Descend into the Iron Caverns',depth,Math.ceil(this.height*.53)),obj('mine','Extract 8 real ore with automated mining',Math.max(metrics.mined||0,s.autoMined||0,s.automined||0),8)],
        [obj('fight','Defeat 6 mechanical enemies',s.kills||0,6),obj('defense','Defeat 3 enemies using connected defenses',s.defenseKills||0,3),obj('raid','Survive a warned attack on the base',s.raidsSurvived||0,1)],
        [obj('production','Manufacture 12 outputs in production machines',Math.max(metrics.produced||0,s.produced||0),12),obj('factory','Operate 3 kinds of machinery together',this.runningKinds,3),obj('power','Build at least 36 connected generation capacity',this.powerCapacity,36)],
        [obj('abyss','Reach the Reactor Abyss',depth,Math.ceil(this.height*74/84)),obj('equipment','Craft powered mining or energy equipment',!!(this.inventory.poweredMiningTool||this.inventory.energyRifle),1),obj('guardian','Defeat the Reactor Guardian',!!s.guardianDefeated,1)]
      ];
    }
    progress(){
      const data=this.objectiveData();for(const c of CHAPTERS){if(c.id!==this.chapter||this.completed.includes(c.id))continue;if(!data[c.id-1].every(o=>o.complete))break;
        this.completed.push(c.id);for(const [key,n] of Object.entries(c.loot))this.grantReward(key,n);this.chapter=Math.min(6,c.id+1);this.notify('Chapter complete: '+c.title+' — '+c.reward);
      }
      if(this.runningMachine)this.stats.machineOperated=true;
    }
    getJournal(){const data=this.objectiveData(),chapters=CHAPTERS.map(c=>({...c,loot:undefined,complete:this.completed.includes(c.id),active:c.id===this.chapter,objectives:data[c.id-1]}));return{chapters,chapter:this.chapter,current:chapters.find(c=>c.active)?.objectives.find(o=>!o.complete)||null,unlocks:[...CHAPTERS.filter(c=>this.completed.includes(c.id)).flatMap(c=>c.unlocks),...(Array.isArray(this.stats.schematics)?this.stats.schematics.map(key=>'Schematic: '+key):[])],discoveredRegions:this.discoveredRegions.slice(),difficulty:this.difficulty,raidsSurvived:this.stats.raidsSurvived||0,kills:this.stats.kills||0};}
    missions(){return this.getJournal();}
    canCraft(recipe){const chapter=typeof recipe==='object'?(recipe.chapter||1):(EQUIPMENT.find(e=>e[0]===recipe)?.[6]||1);return chapter<=this.chapter||!!(typeof recipe==='object'&&recipe.schematic&&Array.isArray(this.stats.schematics)&&this.stats.schematics.includes(recipe.schematic));}
    getHud(){const p=this.center(),j=this.getJournal();return{chapter:this.chapter,mission:j.current,region:this.regionAt(p.x/this.tile,p.y/this.tile),weapon:WEAPONS[this.weapon]?.name||WEAPONS.tool.name,armor:this.armor().name,energy:this.energy,maxEnergy:this.maxEnergy,attackCooldown:this.attackCooldown,raid:{...this.raid},difficulty:this.difficulty,checkpoint:{...this.checkpoint},caches:this.caches.length};}
    nearestMachine(x,y,poweredOnly=false){let nearest=null,best=Infinity;for(const k of poweredOnly?this.powered:this.machineKeys){const mx=(k%this.width+.5)*this.tile,my=(Math.floor(k/this.width)+.5)*this.tile,d=Math.hypot(mx-x,my-y);if(d<best){best=d;nearest={x:mx,y:my,key:k,distance:d};}}return nearest;}
    raidMachine(x,y){const anchor=this.nearestMachine(this.checkpoint.x,this.checkpoint.y,true);if(!anchor)return null;let nearest=null,best=Infinity;for(const key of this.powered){const mx=(key%this.width+.5)*this.tile,my=(Math.floor(key/this.width)+.5)*this.tile;if(!this.get(key%this.width,Math.floor(key/this.width))||this.getMeta().get(key)?.disabled||Math.hypot(mx-anchor.x,my-anchor.y)>this.tile*12)continue;const distance=Math.hypot(mx-x,my-y);if(distance<best){best=distance;nearest={x:mx,y:my,key,distance};}}return nearest;}
    move(e,dx,dy,dig=false){
      const steps=Math.max(1,Math.ceil(Math.max(Math.abs(dx),Math.abs(dy))/(this.tile*.3)));let blockedX=false,ground=false;
      for(let i=0;i<steps;i++){
        if(!this.blocked(e.x+dx/steps,e.y,e.w,e.h,dig))e.x+=dx/steps;else{blockedX=true;e.vx=0;}
        if(!this.blocked(e.x,e.y+dy/steps,e.w,e.h,dig))e.y+=dy/steps;else{if(dy>0)ground=true;e.vy=0;}
      }e.ground=ground;return blockedX;
    }
    startAttack(e,target,machine){const d=ENEMIES[e.type];e.wind=d.wind;e.attackX=target.x;e.attackY=target.y;e.attackMachine=machine?.key??null;e.vx=0;}
    finishAttack(e){
      const d=ENEMIES[e.type],p=this.center(),distance=Math.hypot(p.x-e.x,p.y-e.y);e.cool=d.cool;e.wind=0;
      if(e.attackMachine!==null&&e.attackMachine!==undefined){const tx=e.attackMachine%this.width,ty=Math.floor(e.attackMachine/this.width);if(this.get(tx,ty)!==this.T.AIR&&Math.hypot((tx+.5)*this.tile-e.x,(ty+.5)*this.tile-e.y)<d.reach+this.tile&&this.lineOfSight(e.x,e.y,(tx+.5)*this.tile,(ty+.5)*this.tile,true)){
        if(e.type==='leech'){const m=this.getMeta().get(e.attackMachine)||{};m.leechUntil=(this.engine?.elapsed||this.elapsed)+3;m.leechDrain=5;m.fault='Circuit leech attached — destroy the leech';this.getMeta().set(e.attackMachine,m);}
        this.damageStructure(tx,ty,d.structureDamage||d.damage*.45);
      }return;}
      if(d.ranged){
        const angle=Math.atan2(e.attackY-e.y,e.attackX-e.x),count=e.type==='guardian'?5:e.type==='sentinel'?3:1;
        for(let i=0;i<count;i++){const a=angle+(i-(count-1)/2)*(e.type==='guardian'?.2:.14);this.projectiles.push({x:e.x,y:e.y,vx:Math.cos(a)*(e.type==='guardian'?150:180),vy:Math.sin(a)*(e.type==='guardian'?150:180),life:3,damage:d.damage,owner:'enemy',kind:'electric',color:e.type==='guardian'?'#ff837c':'#f9bf6b'});}
        this.effects.push({x:e.x,y:e.y,life:.25,max:.25,kind:'burst',color:d.color});
      }else if(distance<d.reach+Math.max(this.player.w||15,this.player.h||34)/2&&this.lineOfSight(e.x,e.y,p.x,p.y)){
        if(this.damagePlayer(d.damage,e.type)){const push=Math.sign(p.x-e.x)*80;if(!this.blocked(p.x+push*.08,p.y,this.player.w||15,this.player.h||34))this.player.vx=push;}
      }
    }
    updateEnemy(e,dt){
      const d=ENEMIES[e.type],p=this.center();e.age+=dt;e.cool=Math.max(0,e.cool-dt);e.flash=Math.max(0,e.flash-dt);e.jump=Math.max(0,e.jump-dt);e.stun=Math.max(0,e.stun-dt);
      if(e.stun>0){e.wind=0;return;}
      if(e.wind>0){e.wind-=dt;if(e.wind<=0)this.finishAttack(e);return;}
      let target={x:p.x,y:p.y},machine=null;
      if(e.type==='leech'||e.raid||d.move==='siege'){
        machine=e.raid?this.raidMachine(e.x,e.y):this.nearestMachine(e.x,e.y,e.type==='leech');if(machine&&((e.type==='leech'||e.raid)||machine.distance<Math.hypot(p.x-e.x,p.y-e.y)))target=machine;else machine=null;
      }
      const distance=Math.hypot(target.x-e.x,target.y-e.y),dir=Math.sign(target.x-e.x)||1;e.facing=dir;
      if(distance<(machine?d.reach+this.tile:d.reach)&&e.cool<=0&&this.lineOfSight(e.x,e.y,target.x,target.y,!!machine)){
        if(e.type!=='burrower'||!this.blocked(e.x,e.y,e.w,e.h)){this.startAttack(e,target,machine);return;}
      }
      const oldX=e.x,oldY=e.y;
      if(['fly','hover','leech'].includes(d.move)){
        let tx=target.x,ty=target.y;
        if(d.move==='fly'){tx+=Math.sin(e.age*2.3+e.id)*42;ty-=34+Math.cos(e.age*2)*30;}
        if(d.move==='hover'){const desired=150;tx=target.x+Math.sign(e.x-target.x||1)*desired;ty=target.y-45+Math.sin(e.age)*30;}
        const a=Math.atan2(ty-e.y,tx-e.x),speed=distance>this.tile?d.speed:d.speed*.5;
        const blocked=this.move(e,Math.cos(a)*speed*dt,Math.sin(a)*speed*dt);
        if(blocked)this.move(e,0,-d.speed*dt);if(this.blocked(e.x,e.y+Math.sin(a)*speed*dt,e.w,e.h))this.move(e,dir*d.speed*dt,0);
      }else if(d.move==='dig'){
        const a=Math.atan2(target.y-e.y,target.x-e.x),canDig=distance>this.tile*3;
        this.move(e,Math.cos(a)*d.speed*dt,Math.sin(a)*d.speed*dt,canDig);
        if(!canDig&&this.blocked(e.x,e.y,e.w,e.h)){
          // Emerge into a real adjacent opening; the player always gets the full wind-up.
          let emerged=false;for(let radius=1;radius<=4&&!emerged;radius++)for(let dy=-radius;dy<=radius&&!emerged;dy++)for(let dx=-radius;dx<=radius;dx++){
            if(Math.max(Math.abs(dx),Math.abs(dy))!==radius)continue;
            if(!this.blocked(e.x+dx*this.tile,e.y+dy*this.tile,e.w,e.h)){e.x+=dx*this.tile;e.y+=dy*this.tile;e.cool=Math.max(e.cool,.5);emerged=true;break;}
          }
        }
      }else{
        let moveDir=dir;if(d.move==='guard'&&Math.abs(p.x-e.anchorX)>this.tile*16)moveDir=Math.sign(e.anchorX-e.x);if(d.ranged&&distance<120)moveDir=-dir;
        const speed=d.speed*(d.move==='crawl'?(.75+.25*Math.sin(e.age*4)):1),drive=moveDir*speed;
        if(target.y<e.y-this.tile*.8&&Math.abs(target.x-e.x)<this.tile*3&&e.ground&&e.jump<=0){e.vy=-285;e.jump=.9;}
        e.vx=e.vx*.8+drive*.2;e.vy=Math.min(440,e.vy+700*dt);const obstacle=this.move(e,e.vx*dt,e.vy*dt);
        if(obstacle&&e.jump<=0){e.vy=-245;e.jump=.85;if((e.type==='siege'||e.type==='guardian'||e.raid)&&e.cool<=0){const tx=Math.floor((e.x+dir*(e.w/2+3))/this.tile),ty=Math.floor(e.y/this.tile),id=this.get(tx,ty);if(id>=9&&id!==this.T.CORE)this.startAttack(e,{x:(tx+.5)*this.tile,y:(ty+.5)*this.tile},{key:this.tileKey(tx,ty)});}}
        if(d.move==='boss'&&e.age%9<dt&&e.ground)e.vy=-280;
      }
      if(Math.hypot(e.x-oldX,e.y-oldY)<.12&&distance>d.reach)e.stuck+=dt;else e.stuck=Math.max(0,e.stuck-dt*.5);
      if(e.stuck>7){e.stuck=0;e.vy=-290;e.jump=1;e.cool=1;if(Math.hypot(e.x-p.x,e.y-p.y)>this.tile*14&&!e.raid&&e.type!=='guardian')e.hp=-1;}
    }
    updateProjectiles(dt){
      const p=this.center();for(const shot of this.projectiles){
        shot.life-=dt;const steps=Math.max(1,Math.ceil(Math.hypot(shot.vx,shot.vy)*dt/(this.tile*.25)));
        for(let i=0;i<steps&&shot.life>0;i++){
          shot.x+=shot.vx*dt/steps;shot.y+=shot.vy*dt/steps;
          if(this.solid(Math.floor(shot.x/this.tile),Math.floor(shot.y/this.tile))){shot.life=0;break;}
          if(shot.owner==='enemy'){
            if(Math.abs(p.x-shot.x)<(this.player.w||15)/2+4&&Math.abs(p.y-shot.y)<(this.player.h||34)/2+4){this.damagePlayer(shot.damage,shot.kind);shot.life=0;}
          }else{const enemy=this.query(shot.x,shot.y,10).find(e=>Math.abs(e.x-shot.x)<e.w/2+4&&Math.abs(e.y-shot.y)<e.h/2+4);if(enemy){this.hit(enemy,shot.damage,'player');shot.life=0;}}
        }
      }this.projectiles=this.projectiles.filter(s=>s.life>0).slice(-160);
    }
    updateRaid(dt){
      const active=this.enemies.filter(e=>e.raid===this.raid.id&&e.hp>0);
      if(this.raid.state==='warning'){
        this.raid.remaining-=dt;
        if(this.raid.remaining<=0){
          const total=clamp(2+Math.floor(this.chapter/2)+Math.floor(this.powerCapacity/60),3,8);let spawned=0;
          for(let i=0;i<total;i++)if(this.findSpawn(this.chapter>=3&&i===0?'siege':i%3===2?'leech':'crawler',this.raid.id))spawned++;
          if(spawned){this.raid.state='attack';this.raid.remaining=105;this.raid.count=spawned;this.notify('Base attack: '+spawned+' hostiles approaching');}
          else{this.raid.state='quiet';this.nextRaid=this.elapsed+35;}
        }
      }else if(this.raid.state==='attack'){
        this.raid.remaining-=dt;this.raid.count=active.length;
        if(!active.length){this.stats.raidsSurvived=(this.stats.raidsSurvived||0)+1;this.raid.state='quiet';this.nextRaid=this.elapsed+220*this.config().raid;this.notify('Base defended — raid survived');}
        else if(this.raid.remaining<=0){for(const e of active){e.raid=0;e.cool=3;}this.raid.state='quiet';this.nextRaid=this.elapsed+200*this.config().raid;this.notify('The attack is withdrawing. Repair and regroup.');}
      }else if(this.elapsed>=this.nextRaid&&this.elapsed>this.config().grace&&this.powered.length>=2){
        this.raid={state:'warning',remaining:24,count:0,id:this.raid.id+1};this.notify('Warning: machine activity has drawn a base attack in 24 seconds');
      }
    }
    tick(dt,time){
      dt=clamp(finite(dt),0,.1);if(!dt)return;this.elapsed+=dt;
      this.attackCooldown=Math.max(0,this.attackCooldown-dt);this.attackAnimation=Math.max(0,this.attackAnimation-dt);this.invulnerable=Math.max(0,this.invulnerable-dt);this.player.hurt=Math.max(0,(this.player.hurt||0)-dt);
      if(this.player.health<=0)this.respawn();const p=this.center();this.maxDepth=Math.max(this.maxDepth,p.y/this.tile);
      const region=this.regionAt(p.x/this.tile,p.y/this.tile);if(!this.discoveredRegions.includes(region.id)){this.discoveredRegions.push(region.id);this.notify('Discovered '+region.name);}
      this.worldClock+=dt;if(this.worldClock>=1){this.worldClock%=1;this.scanWorld();this.progress();}
      const live=this.nearestMachine(p.x,p.y,true),nearBase=Math.hypot(this.player.x-this.checkpoint.x,this.player.y-this.checkpoint.y)<this.tile*10;
      if(live&&live.distance<this.tile*7)this.energy=Math.min(this.maxEnergy,this.energy+dt*5);
      if(this.elapsed-this.lastDamage>12&&nearBase&&(live?.distance<this.tile*10||this.difficulty==='casual'))this.player.health=Math.min(100,this.player.health+dt*(this.difficulty==='casual'?2:1));
      for(const cache of this.caches)if(Math.hypot(cache.x-p.x,cache.y-p.y)<this.tile*1.8&&this.lineOfSight(cache.x,cache.y,p.x,p.y)){
        let recovered=0;for(const [key,count] of Object.entries(cache.items)){const n=Math.min(count,this.inventoryRoom());if(n){this.inventory[key]=(this.inventory[key]||0)+n;cache.items[key]-=n;recovered+=n;if(!cache.items[key])delete cache.items[key];}}
        if(recovered)this.notify(Object.keys(cache.items).length?'Recovered some resources — make inventory space to take the rest':'Recovered resource cache');
      }
      this.caches=this.caches.filter(c=>Object.keys(c.items).length);
      this.grid.rebuild(this.enemies);
      for(const e of this.enemies){
        if(e.hp<=0)continue;const distance=Math.hypot(p.x-e.x,p.y-e.y);
        if(distance>this.tile*55&&!e.raid&&e.type!=='guardian'){e.hp=-1;continue;}
        if(distance>this.tile*35&&!e.raid){e.sleep=(e.sleep||0)+dt;if(e.sleep<.5)continue;const slept=e.sleep;e.sleep=0;this.updateEnemy(e,Math.min(slept,.1));}else this.updateEnemy(e,dt);
      }
      this.enemies=this.enemies.filter(e=>e.hp>0);this.grid.rebuild(this.enemies);this.updateProjectiles(dt);
      this.spawnClock-=dt;if(this.spawnClock<=0&&this.elapsed>this.config().grace){
        const target=clamp(2+region.tier*2+this.chapter+Math.floor(this.powered.length/8),3,this.config().max);
        if(this.enemies.length<target){const e=this.findSpawn(this.chooseType());this.spawnFailures=e?0:this.spawnFailures+1;}
        this.spawnClock=(7+this.random()*5)*this.config().spawn/(1+region.tier*.13);
      }
      this.updateRaid(dt);
      if(this.chapter>=5&&region.tier>=4&&!this.stats.guardianDefeated&&!this.bossSpawned){const e=this.findSpawn('guardian');if(e){this.bossSpawned=true;this.notify('Reactor Guardian awakened — watch its attack indicators');}}
      for(const f of this.effects)f.life-=dt;this.effects=this.effects.filter(f=>f.life>0).slice(-110);
    }
    export(){return{version:1,difficulty:this.difficulty,elapsed:this.elapsed,energy:this.energy,weapon:this.weapon,enemies:this.enemies.map(e=>({...e})),projectiles:this.projectiles.map(p=>({...p})),caches:this.caches.map(c=>({...c,items:{...c.items}})),nextEnemy:this.nextEnemy,nextCache:this.nextCache,checkpoint:{...this.checkpoint},completed:this.completed.slice(),chapter:this.chapter,discoveredRegions:this.discoveredRegions.slice(),maxDepth:this.maxDepth,nextRaid:this.nextRaid,raid:{...this.raid},bossSpawned:this.bossSpawned};}
    restore(data){
      if(!data||typeof data!=='object'){this.scanWorld();return;}
      this.setDifficulty(data.difficulty||'standard');this.elapsed=Math.max(0,finite(data.elapsed));this.energy=clamp(finite(data.energy,100),0,this.maxEnergy);this.weapon=WEAPONS[data.weapon]?data.weapon:'tool';
      const claimed=new Set(Array.isArray(data.completed)?data.completed.filter(n=>Number.isInteger(n)&&n>=1&&n<=5):[]);
      // Keep a contiguous campaign history: a later malformed completion must not
      // prevent its chapter from being earned after the missing earlier chapter.
      this.completed=[];let next=1;while(claimed.has(next)){this.completed.push(next);next++;}this.chapter=Math.min(next,6);
      this.discoveredRegions=Array.isArray(data.discoveredRegions)?[...new Set(['scrapyard',...data.discoveredRegions.filter(id=>REGIONS.some(r=>r.id===id))])]:['scrapyard'];
      this.maxDepth=clamp(finite(data.maxDepth),0,this.height);this.nextRaid=Math.max(this.elapsed+15,finite(data.nextRaid,this.elapsed+200));
      if(data.checkpoint&&Number.isFinite(data.checkpoint.x)&&Number.isFinite(data.checkpoint.y))this.checkpoint={x:clamp(data.checkpoint.x,this.tile,(this.width-2)*this.tile),y:clamp(data.checkpoint.y,this.tile,(this.height-2)*this.tile)};
      this.enemies=(Array.isArray(data.enemies)?data.enemies:[]).filter(e=>ENEMIES[e.type]&&Number.isFinite(e.x)&&Number.isFinite(e.y)&&Number.isFinite(e.hp)&&e.hp>0&&e.x>0&&e.x<this.width*this.tile&&e.y>0&&e.y<this.height*this.tile).slice(0,this.config().max).map(e=>{const d=ENEMIES[e.type];return{...e,w:d.w,h:d.h,maxHealth:clamp(finite(e.maxHealth,d.hp),1,10000),hp:clamp(e.hp,1,10000),vx:clamp(finite(e.vx),-500,500),vy:clamp(finite(e.vy),-500,500),wind:clamp(finite(e.wind),0,d.wind),cool:clamp(finite(e.cool,1),0,10),stun:0,flash:0,jump:0,stuck:0,age:finite(e.age),facing:e.facing===-1?-1:1,anchorX:finite(e.anchorX,e.x),anchorY:finite(e.anchorY,e.y)};}).filter(e=>e.type==='burrower'||!this.blocked(e.x,e.y,e.w,e.h));
      this.projectiles=[]; // Flight is transient; avoid a hit immediately after loading.
      this.caches=(Array.isArray(data.caches)?data.caches:[]).filter(c=>Number.isFinite(c.x)&&Number.isFinite(c.y)&&c.items&&typeof c.items==='object').map(c=>({...c,x:clamp(c.x,this.tile,(this.width-1)*this.tile),y:clamp(c.y,this.tile,(this.height-1)*this.tile),items:Object.fromEntries(Object.entries(c.items).filter(([key,n])=>/^[a-zA-Z][a-zA-Z0-9]*$/.test(key)&&Number.isFinite(n)&&n>0).map(([key,n])=>[key,Math.floor(n)]))})).filter(c=>Object.keys(c.items).length);
      this.nextEnemy=Math.max(1,finite(data.nextEnemy,1),...this.enemies.map(e=>finite(e.id)+1));this.nextCache=Math.max(1,finite(data.nextCache,1),...this.caches.map(c=>finite(c.id)+1));
      if(data.raid&&['quiet','warning','attack'].includes(data.raid.state))this.raid={state:data.raid.state,remaining:clamp(finite(data.raid.remaining),0,120),count:clamp(finite(data.raid.count),0,12),id:Math.max(0,finite(data.raid.id))};
      this.bossSpawned=this.enemies.some(e=>e.type==='guardian')||!!this.stats.guardianDefeated;this.invulnerable=3;this.scanWorld();this.grid.rebuild(this.enemies);
    }
    draw(ctx,camera){
      const cx=camera.x||0,cy=camera.y||0;
      for(const cache of this.caches){const x=cache.x-cx,y=cache.y-cy;ctx.save();ctx.translate(x,y);ctx.fillStyle=cache.kind==='recovery'?'#8ff3d8':'#e9c179';ctx.fillRect(-9,-8,18,16);ctx.fillStyle='#294c4c';ctx.fillRect(-6,-5,12,10);ctx.fillStyle='#b5ffde';ctx.font='bold 15px monospace';ctx.textAlign='center';ctx.fillText('+',0,5);ctx.font='10px monospace';ctx.fillText(cache.kind==='recovery'?'RECOVER':'SALVAGE',0,-15);ctx.restore();}
      for(const e of this.enemies){
        const d=ENEMIES[e.type],x=e.x-cx,y=e.y-cy;if(x< -80||y< -80||x>ctx.canvas.width+80||y>ctx.canvas.height+80)continue;
        ctx.save();ctx.translate(Math.round(x),Math.round(y));
        if(e.type==='burrower'&&this.blocked(e.x,e.y,e.w,e.h)){ctx.globalAlpha=.45;ctx.fillStyle=d.color;ctx.fillRect(-10,2,20,3);ctx.restore();continue;}
        if(e.wind>0){const ratio=1-e.wind/d.wind;ctx.strokeStyle=ratio>.65?'#ff7d6e':'#ffc67b';ctx.lineWidth=2;ctx.beginPath();ctx.arc(0,0,Math.max(e.w,e.h)/2+5+ratio*5,0,Math.PI*2);ctx.stroke();if(d.ranged){ctx.globalAlpha=.35;ctx.beginPath();ctx.moveTo(0,0);ctx.lineTo(e.attackX-e.x,e.attackY-e.y);ctx.stroke();ctx.globalAlpha=1;}ctx.fillStyle='#ffe7a8';ctx.font='bold 14px monospace';ctx.textAlign='center';ctx.fillText('!',0,-e.h/2-8);}
        ctx.fillStyle=e.flash>0?'#fff8e0':d.color;const walk=Math.sin(e.age*12)*3;
        if(e.type==='beetle'||e.type==='crawler'){
          ctx.fillRect(-e.w/2,-e.h/2,e.w,e.h-3);ctx.fillStyle='#3b4547';ctx.fillRect(-3,-e.h/2,3,e.h-3);ctx.fillRect(-e.w/2-3,-3,5,3);ctx.fillRect(e.w/2-2,-3,5,3);for(const side of [-1,1]){ctx.fillRect(-8,e.h/2-3+side*walk*.3,4,5);ctx.fillRect(5,e.h/2-3-side*walk*.3,4,5);}
        }else if(e.type==='wisp'){
          ctx.globalAlpha=.25;ctx.fillRect(-17,-17,34,34);ctx.globalAlpha=1;ctx.fillRect(-7,-9,14,18);ctx.fillStyle='#e4ffff';ctx.fillRect(-3,-4,6,9);ctx.strokeStyle=d.color;ctx.lineWidth=2;ctx.beginPath();ctx.moveTo(-15,-8);ctx.lineTo(-10,0);ctx.lineTo(-16,8);ctx.moveTo(15,-7);ctx.lineTo(10,0);ctx.lineTo(16,7);ctx.stroke();
        }else if(e.type==='drone'||e.type==='leech'){
          ctx.fillRect(-e.w/2,-6,e.w,13);ctx.fillStyle='#304652';ctx.fillRect(-6,-3,12,7);ctx.fillStyle='#fff3a8';ctx.fillRect(e.facing>0?4:-8,-2,4,3);ctx.fillStyle=d.color;ctx.fillRect(-18,-10+walk*.4,12,3);ctx.fillRect(6,-10-walk*.4,12,3);if(e.type==='leech'){ctx.strokeStyle='#d9acff';ctx.beginPath();ctx.moveTo(-8,6);ctx.lineTo(-13,12);ctx.moveTo(8,6);ctx.lineTo(13,12);ctx.stroke();}
        }else if(e.type==='burrower'){
          ctx.fillRect(-12,-8,20,16);ctx.fillStyle='#6b5b50';for(let i=-7;i<8;i+=5)ctx.fillRect(i,-7,2,14);ctx.fillStyle='#f2d5a1';ctx.beginPath();ctx.moveTo(e.facing*10,-8);ctx.lineTo(e.facing*19,0);ctx.lineTo(e.facing*10,8);ctx.fill();
        }else{
          ctx.fillRect(-e.w/2+3,-e.h/2+5,e.w-6,e.h-13);ctx.fillStyle='#33464d';ctx.fillRect(-e.w/2,-e.h/2+12,6,e.h-20);ctx.fillRect(e.w/2-6,-e.h/2+12,6,e.h-20);ctx.fillRect(-e.w/2+3,e.h/2-9,8,9+walk*.3);ctx.fillRect(e.w/2-11,e.h/2-9,8,9-walk*.3);ctx.fillStyle=e.type==='guardian'?'#fff1a2':'#edfff1';ctx.fillRect(-e.w/3,-e.h/2+9,e.w*2/3,4);if(e.type==='guardian'){ctx.fillStyle='#a43d43';ctx.fillRect(-9,-7,18,18);ctx.fillStyle='#ffe49f';ctx.fillRect(-3,-4,6,12);}
        }
        if(!['wisp','drone','leech'].includes(e.type)){ctx.fillStyle='#19292f';ctx.fillRect(e.facing>0?e.w/2-6:-e.w/2+3,-e.h/2+4,3,3);}
        if(e.hp<e.maxHealth||e.type==='guardian'){ctx.fillStyle='#283337';ctx.fillRect(-e.w/2,-e.h/2-7,e.w,3);ctx.fillStyle='#ef8d79';ctx.fillRect(-e.w/2,-e.h/2-7,e.w*e.hp/e.maxHealth,3);}
        if(e.stun>0){ctx.fillStyle='#a6ffff';ctx.fillRect(-4,-e.h/2-13,8,2);}ctx.restore();
      }
      for(const shot of this.projectiles){ctx.fillStyle=shot.color;ctx.fillRect(shot.x-cx-3,shot.y-cy-2,6,4);ctx.strokeStyle=shot.color;ctx.globalAlpha=.35;ctx.beginPath();ctx.moveTo(shot.x-cx,shot.y-cy);ctx.lineTo(shot.x-cx-shot.vx*.025,shot.y-cy-shot.vy*.025);ctx.stroke();ctx.globalAlpha=1;}
      for(const f of this.effects){ctx.save();ctx.globalAlpha=clamp(f.life/f.max,0,1);ctx.translate(f.x-cx,f.y-cy);ctx.strokeStyle=ctx.fillStyle=f.color;ctx.lineWidth=2;if(f.kind==='hit'){ctx.font='bold 12px monospace';ctx.fillText(f.amount||'',0,-8-(1-f.life/f.max)*20);}else if(f.kind==='slash'){ctx.rotate(f.angle);ctx.beginPath();ctx.arc(-20,0,30,-1.1,1.1);ctx.stroke();}else for(let i=0;i<8;i++){const a=i*Math.PI/4,r=(1-f.life/f.max)*24;ctx.fillRect(Math.cos(a)*r-2,Math.sin(a)*r-2,4,4);}ctx.restore();}
      if(this.attackAnimation>0){const p=this.center();ctx.save();ctx.translate(p.x-cx,p.y-cy);ctx.rotate(this.attackAngle||0);ctx.strokeStyle='#ffe2a1';ctx.lineWidth=4;ctx.beginPath();ctx.moveTo(8,5);ctx.lineTo(34,-8);ctx.stroke();ctx.restore();}
    }
  }
  return{Survival,SpatialGrid,install,DIFFICULTIES,REGIONS,ENEMIES,WEAPONS,EQUIPMENT,CHAPTERS};
});
