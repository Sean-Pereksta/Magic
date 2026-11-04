/* IRONFIELD — original canvas artwork and battlefield renderer. No external assets. */
(function (global) {
  'use strict';

  const TAU = Math.PI * 2;
  const ATTACK_FRAMES = 10;
  const BODY_LIFETIME = 22;
  const BODY_FADE_START = 14;
  const CORPSE_TILE = 512, CORPSE_PAD = 80, CORPSE_CACHE_PIXELS = 8000000;
  const LONG_BODY_LIFETIME = 120, LONG_BODY_FADE_START = 96;
  const EXTRA_POSE_PIXEL_ZOOM = .25;
  const WEAPON_ART = Object.freeze({
    musketeer: { w: 104, h: 94, allFacings: true, healthY: 53 },
    berserker: { w: 104, h: 94, allFacings: true, healthY: 51 },
    cannon: { w: 116, h: 94, allFacings: true, healthY: 41 },
    trebuchet: { w: 182, h: 166, allFacings: false, healthY: 103 }
  });
  function spriteFacing(type,dir){
    if(WEAPON_ART[type]?.allFacings)return dir;
    const troop=global.GameData?.TYPES[type];
    if(troop?.mounted||troop?.siege||type==='elephant')return Math.cos(dir*Math.PI/4)<-.2?4:0;
    return [0,0,0,4,4,5,6,6][dir];
  }
  const TEAM = [
    { base: '#3f739e', light: '#90bddc', dark: '#24415e', flag: '#72b9ed', trim: '#d6c498' },
    { base: '#aa4a43', light: '#e39a81', dark: '#602d30', flag: '#f3836b', trim: '#e2bd83' }
  ];
  const THEMES = {
    grassland: { ground: '#66745a', patch: ['#6b785e','#778064','#5e6c53','#85876a'], grass:'#9ba17a', road:'#9a9176', hill:'#9a9d78', tree:'#354d35', treeTop:'#617647', water:'#395e69', stone:'#8d9181', shadow:'#354332' },
    wetland: { ground: '#64745d', patch: ['#6c7c62','#63765b','#7a8569','#556c57'], grass:'#a3a681', road:'#a29577', hill:'#9ca17b', tree:'#304f3d', treeTop:'#5c7d55', water:'#3a7380', stone:'#93998a', shadow:'#354b3f' },
    desert: { ground: '#aa9470', patch: ['#b4a17c','#a48b66','#b8a27d','#a38f6d'], grass:'#d1bc91', road:'#c4ac83', hill:'#d1b88b', tree:'#6c7045', treeTop:'#8a8a51', water:'#467879', stone:'#8e816c', shadow:'#786b52' },
    tundra: { ground: '#a8b4ac', patch: ['#b3bfb5','#a3afa6','#bdc6bb','#96a79e'], grass:'#d0d8c9', road:'#7c8a80', hill:'#d1d8c9', tree:'#394f47', treeTop:'#6f8c7b', water:'#537c87', stone:'#858f89', shadow:'#687b73' },
    woodland: { ground:'#566f54',patch:['#627b57','#526b4f','#6e805b','#4d654d'],grass:'#a3b27b',road:'#9b9971',hill:'#91a16f',tree:'#243f31',treeTop:'#739054',water:'#436d69',stone:'#879582',shadow:'#2f4837' },
    highland: { ground:'#7c8270',patch:['#858c79','#737e6b','#929781','#777d70'],grass:'#bfc3a0',road:'#aaa78c',hill:'#b0ad92',tree:'#45584a',treeTop:'#7f906b',water:'#547780',stone:'#8a8d81',shadow:'#505d50' },
    ashland: { ground:'#77736c',patch:['#827c70','#6d6d65','#8a8173','#767466'],grass:'#c2b197',road:'#9b8c75',hill:'#b9a27f',tree:'#4c4a3b',treeTop:'#817258',water:'#5b7474',stone:'#969083',shadow:'#49483d' }
  };
  const clamp = (n,a,b) => Math.max(a,Math.min(b,n));
  function rng(seed) { let a=(Number(seed)||1)>>>0; return function(){a+=0x6D2B79F5;let t=a;t=Math.imul(t^(t>>>15),t|1);t^=t+Math.imul(t^(t>>>7),t|61);return ((t^(t>>>14))>>>0)/4294967296;}; }
  function newCanvas(w,h) { const c=document.createElement('canvas'); c.width=Math.max(1,Math.ceil(w));c.height=Math.max(1,Math.ceil(h));return c; }
  function ellipse(ctx,x,y,rx,ry,fill,stroke,lw) { ctx.beginPath();ctx.ellipse(x,y,Math.max(.01,rx),Math.max(.01,ry),0,0,TAU);if(fill){ctx.fillStyle=fill;ctx.fill();}if(stroke){ctx.strokeStyle=stroke;ctx.lineWidth=lw||1;ctx.stroke();} }
  function poly(ctx,points,fill,stroke,lw) { ctx.beginPath();ctx.moveTo(points[0][0],points[0][1]);for(let i=1;i<points.length;i++)ctx.lineTo(points[i][0],points[i][1]);ctx.closePath();if(fill){ctx.fillStyle=fill;ctx.fill();}if(stroke){ctx.strokeStyle=stroke;ctx.lineWidth=lw||1;ctx.stroke();} }
  function line(ctx,x1,y1,x2,y2,color,width) {ctx.strokeStyle=color;ctx.lineWidth=width||1;ctx.beginPath();ctx.moveTo(x1,y1);ctx.lineTo(x2,y2);ctx.stroke();}

  class BattleRenderer {
    constructor(canvas,engine,options) {
      this.canvas=canvas;
      this.ctx=canvas.getContext('2d',{alpha:false});
      this.options=Object.assign({quality:'auto',showHealth:false,showLabels:true,showOrders:true,showBodies:true,corpseMode:'brief',animations:'full',cinematic:false},options||{});
      this.camera={x:3200,y:2200,zoom:.18};
      this.width=canvas.clientWidth||1280;this.height=canvas.clientHeight||720;
      this.dpr=1;this.fps=60;this._lastFrame=0;this._frame=0;this._visible=[];
      this._atlas=new Map();this._miniFrame=0;this._terrain=null;this._terrainScale=.4;this._now=0;
      this._unitAtlases=[];this._siegeTowers=[];this._depthCounts=new Int32Array(0);this._depthKeys=new Uint32Array(0);this._depthSorted=[];
      this._adaptiveLow=false;this._qualityChangedAt=0;
      this._formationEnvelopes=new Map();
      this._visibleBodies=[];this._bodyBuckets=Array.from({length:6},()=>[]);
      this._bodyWarmQueue=[];this._bodyBuildBudget=0;this._poseBuildBudget=0;
      this._corpseCache=null;this._corpseStats=null;this._hitPoses=new Map();this._minimapBase=null;
      this.pendingFacing=null;
      this._renderStats={visible:0,drawCalls:0};
      this.resize(this.width,this.height);
      this.setEngine(engine);
      this.fit();
    }

    setEngine(engine) {
      this._resetCorpseCache();
      if(this._minimapBase){this._minimapBase.canvas.width=1;this._minimapBase.canvas.height=1;this._minimapBase=null;}
      this.engine=engine;
      this.pendingFacing=null;
      this.map=engine.map||{width:engine.width,height:engine.height,regions:[],biome:'grassland'};
      this.theme=THEMES[this.map.biome]||THEMES.grassland;
      this._buildTerrain();
      this._unitAtlases=(engine.units||[]).map(u=>this._getAtlas(u.type,u.team||0));
      this._siegeTowers=(engine.units||[]).filter(u=>u.type==='siegeTower');
      this._visible.length=0;this._miniFrame=0;
      this._depthSorted.length=0;this._adaptiveLow=false;this._qualityChangedAt=0;
      this._visibleBodies.length=0;for(const bucket of this._bodyBuckets)bucket.length=0;
      const occupied=new Set((engine.regiments||[]).filter(r=>r.alive>0).map(r=>r.type+':'+r.team));
      this._bodyWarmQueue=Array.from(occupied,key=>{const parts=key.split(':');return {type:parts[0],team:Number(parts[1]),dir:0};});
      this._formationEnvelopes.clear();this._hitPoses.clear();
    }

    resize(width,height,dpr) {
      this.width=Math.max(1,Number(width)||1);this.height=Math.max(1,Number(height)||1);
      this.dpr=clamp(Number(dpr)||(typeof devicePixelRatio==='number'?devicePixelRatio:1),1,2);
      this.canvas.width=Math.round(this.width*this.dpr);this.canvas.height=Math.round(this.height*this.dpr);
      if(this.canvas.style){this.canvas.style.width=this.width+'px';this.canvas.style.height=this.height+'px';}
      this.ctx.imageSmoothingEnabled=true;
    }

    worldToScreen(x,y) { return {x:(x-this.camera.x)*this.camera.zoom+this.width/2,y:(y-this.camera.y)*this.camera.zoom+this.height/2}; }
    screenToWorld(x,y) { return {x:(x-this.width/2)/this.camera.zoom+this.camera.x,y:(y-this.height/2)/this.camera.zoom+this.camera.y}; }
    _minZoom() {return Math.max(.012,Math.min(.10,Math.min(this.width/this.engine.width,this.height/this.engine.height)*.82));}
    fit() { if(!this.engine)return;this.camera.x=this.engine.width/2;this.camera.y=this.engine.height/2;this.camera.zoom=clamp(Math.min(this.width/this.engine.width,this.height/this.engine.height)*.94,this._minZoom(),3); }
    focus(x,y,zoom) {this.camera.x=Number.isFinite(x)?x:this.camera.x;this.camera.y=Number.isFinite(y)?y:this.camera.y;if(Number.isFinite(zoom))this.camera.zoom=clamp(zoom,this._minZoom(),3);this._clampCamera();}
    pan(dx,dy) {this.camera.x-=dx/this.camera.zoom;this.camera.y-=dy/this.camera.zoom;this._clampCamera();}
    zoomAt(factor,x,y) {const p=this.screenToWorld(x,y);this.camera.zoom=clamp(this.camera.zoom*factor,this._minZoom(),3);const q=this.screenToWorld(x,y);this.camera.x+=p.x-q.x;this.camera.y+=p.y-q.y;this._clampCamera();}
    _clampCamera() {if(!this.engine)return;this.camera.x=clamp(this.camera.x,-this.engine.width*.1,this.engine.width*1.1);this.camera.y=clamp(this.camera.y,-this.engine.height*.1,this.engine.height*1.1);}

    _buildTerrain() {
      const w=this.engine.width||6400,h=this.engine.height||4400;
      // Keep the backdrop near the original map's memory footprint even on
      // a 16000×11000 battlefield. Terrain never allocates a world-sized bitmap.
      const s=Math.min(this._terrainScale||.4,3072/w,3072/h,Math.sqrt(4600000/(w*h)));
      this._terrainRasterScale=s;
      if(this._minimapBase){this._minimapBase.canvas.width=1;this._minimapBase.canvas.height=1;this._minimapBase=null;}
      if(this._terrain){this._terrain.width=1;this._terrain.height=1;}
      this._terrain=newCanvas(w*s,h*s);
      const c=this._terrain.getContext('2d');c.scale(s,s);
      const t=this.theme,rand=rng(this.map.seed||this.engine.seed||9317);
      c.fillStyle=t.ground;c.fillRect(0,0,w,h);
      // Long-wavelength land patches and field grain are baked once.
      for(let i=0;i<650;i++){
        const x=rand()*w,y=rand()*h,rx=40+rand()*240,ry=30+rand()*150;
        c.globalAlpha=.13+rand()*.16;ellipse(c,x,y,rx,ry,t.patch[(rand()*t.patch.length)|0]);
      }
      c.globalAlpha=1;
      for(let i=0;i<18000;i++){
        const x=rand()*w,y=rand()*h;c.globalAlpha=.12+rand()*.18;
        if(i%3===0){line(c,x,y,x+2+rand()*9,y-rand()*7,t.grass,1.5);}
        else {c.fillStyle=t.patch[i%t.patch.length];c.fillRect(x,y,2+rand()*7,2+rand()*3);}
      }
      c.globalAlpha=1;
      if(this.map.id!=='custom')this._drawRoads(c,rand,w,h);
      const regs=this.map.regions||[];
      for(const r of regs)if(r.type==='hill')this._paintHill(c,r,rand);
      for(const r of regs)if(r.type==='forest')this._paintForest(c,r,rand);
      for(const r of regs)if(r.type==='rock')this._paintRockRegion(c,r,rand);
      for(const r of regs)if(r.type==='water')this._paintWater(c,r,rand);
      for(const r of regs)if(r.type==='bridge')this._paintBridge(c,r);
      // Quiet peripheral camps give the giant landscape a human scale.
      if(this.map.id!=='custom'){this._paintCamp(c,130,h*.46,0);this._paintCamp(c,w-130,h*.54,1);}
      c.strokeStyle='rgba(33,41,32,.22)';c.lineWidth=22;c.strokeRect(11,11,w-22,h-22);
      c.strokeStyle='rgba(222,212,171,.22)';c.lineWidth=4;c.strokeRect(28,28,w-56,h-56);
      c.globalAlpha=1;
    }

    _drawRoads(c,rand,w,h) {
      c.lineCap='round';
      const t=this.theme,river=(this.map.regions||[]).some(r=>r.type==='water');
      const ys=river?(this.map.regions||[]).filter(r=>r.type==='bridge').map(r=>r.y+r.h/2):this.map.id==='highland'?[910,2160,3390]:this.map.id==='greenwood'?[1040,2200,3310]:this.map.id==='basin'?[1100,2200,3300]:[h*.51];
      for(const y of ys){
        c.strokeStyle=t.road;c.globalAlpha=.29;c.lineWidth=45;
        c.beginPath();c.moveTo(90,y-50);c.bezierCurveTo(w*.23,y+75,w*.37,y-45,w*.5,y);c.bezierCurveTo(w*.67,y+35,w*.79,y-65,w-90,y+30);c.stroke();
        c.strokeStyle='#d8c7a0';c.globalAlpha=.08;c.lineWidth=9;c.stroke();
      }
      c.globalAlpha=1;c.lineCap='butt';
    }

    _paintHill(c,r,rand) {
      const t=this.theme,x=r.x+r.w/2,y=r.y+r.h/2;
      c.save();
      for(let i=0;i<5;i++){
        const f=1-i*.13;c.globalAlpha=.07+i*.016;
        ellipse(c,x-i*9,y-i*12,r.w*f/2,r.h*f/2,t.hill);
      }
      c.globalAlpha=.22;c.strokeStyle=t.shadow;c.lineWidth=3;
      for(let i=0;i<4;i++){
        const rx=r.w*(.45-i*.07),ry=r.h*(.42-i*.067);
        c.beginPath();c.ellipse(x,y-10*i,rx,ry,0,.05,Math.PI*1.03);c.stroke();
      }
      c.globalAlpha=.22;
      for(let i=0;i<28;i++){const px=r.x+rand()*r.w,py=r.y+rand()*r.h;line(c,px,py,px+8+rand()*18,py-3,t.grass,2);}
      c.restore();
    }

    _paintForest(c,r,rand) {
      const t=this.theme;
      c.save();c.globalAlpha=.19;c.fillStyle=t.shadow;c.fillRect(r.x,r.y,r.w,r.h);c.globalAlpha=1;
      const trees=[];const n=Math.max(8,Math.min(2200,Math.floor(r.w*r.h/4200*Math.min(1,(this._terrainRasterScale||.4)/.4))));
      for(let i=0;i<n;i++)trees.push({x:r.x+12+rand()*Math.max(1,r.w-24),y:r.y+12+rand()*Math.max(1,r.h-24),s:.7+rand()*.8});
      trees.sort((a,b)=>a.y-b.y);
      for(const p of trees)this._tree(c,p.x,p.y,p.s,rand);
      c.restore();
    }

    _tree(c,x,y,s,rand) {
      const t=this.theme,snow=this.map.biome==='tundra',arid=this.map.biome==='desert',ash=this.map.biome==='ashland';
      c.save();c.translate(x,y);c.scale(s,s);
      c.globalAlpha=.19;ellipse(c,10,8,30,15,'#1b2922');c.globalAlpha=.87;
      line(c,0,4,0,-38,arid?'#74624c':'#5f5640',5);
      if(ash){
        line(c,0,3,-2,-48,'#514c40',6);line(c,-1,-19,-23,-36,'#514c40',4);line(c,-2,-31,18,-49,'#514c40',3.5);line(c,-15,-30,-18,-49,'#625c49',2.2);line(c,11,-43,25,-44,'#625c49',2);
        ellipse(c,-18,-39,13,7,t.tree);ellipse(c,14,-47,17,8,t.treeTop);ellipse(c,-3,-53,14,8,'#95825c');line(c,0,-4,-1,-34,'#9a896a',1.3);
      }else if(snow){
        for(let i=0;i<3;i++){const yy=-19-i*15;poly(c,[[-24+i*5,yy+15],[0,yy-33],[24-i*5,yy+15]],t.tree);poly(c,[[-16+i*4,yy+4],[0,yy-33],[17-i*4,yy+7]],i%2?t.treeTop:'#aabbb0');}
      }else if(arid){
        line(c,0,-13,-15,-29,'#74624c',4);line(c,0,-23,17,-38,'#74624c',3);
        ellipse(c,-13,-31,22,9,t.tree);ellipse(c,11,-40,29,12,t.treeTop);ellipse(c,2,-46,20,8,'#96965b');
      }else{
        ellipse(c,-13,-26,22,18,t.tree);ellipse(c,12,-31,25,22,t.tree);ellipse(c,-3,-47,25,24,t.treeTop);
        c.globalAlpha=.55;ellipse(c,-10,-49,16,15,'#88915a');ellipse(c,12,-33,14,12,t.treeTop);c.globalAlpha=.87;
        line(c,-9,-36,-4,-20,'rgba(23,38,26,.35)',2);
      }
      c.restore();
    }

    _paintRockRegion(c,r,rand) {
      const t=this.theme;
      c.save();c.globalAlpha=.18;c.fillStyle=t.shadow;c.fillRect(r.x,r.y,r.w,r.h);c.globalAlpha=1;
      const count=Math.max(4,Math.min(1400,Math.floor(r.w*r.h/2600*Math.min(1,(this._terrainRasterScale||.4)/.4))));
      for(let i=0;i<count;i++){
        const rx=Math.min(r.w*.45,12+rand()*31),ry=Math.min(r.h*.45,12+rand()*25);
        const x=r.x+rx+rand()*Math.max(1,r.w-rx*2),y=r.y+ry+rand()*Math.max(1,r.h-ry*2);
        c.globalAlpha=.21;ellipse(c,x+5,y+ry*.65,rx*1.1,ry*.5,'#27322c');c.globalAlpha=1;
        const pts=[[x-rx,y],[x-rx*.7,y-ry*.7],[x+rx*.15,y-ry],[x+rx*.8,y-ry*.4],[x+rx,y+ry*.25],[x,y+ry*.5]];
        poly(c,pts,t.stone,t.shadow,2);
        poly(c,[pts[1],pts[2],pts[3],[x,y]],this.map.biome==='tundra'?'#c9d0c5':'#b1af94');
        line(c,x,y,x+rx*.7,y-ry*.4,t.shadow,2);
      }
      c.restore();
    }

    _paintWater(c,r,rand) {
      const t=this.theme;
      c.save();c.fillStyle=t.shadow;c.globalAlpha=.32;c.fillRect(r.x-18,r.y,r.w+36,r.h);c.globalAlpha=1;
      c.fillStyle=t.water;c.fillRect(r.x,r.y,r.w,r.h);
      const grad=c.createLinearGradient(r.x,0,r.x+r.w,0);grad.addColorStop(0,'rgba(163,183,155,.24)');grad.addColorStop(.25,'rgba(35,72,83,.14)');grad.addColorStop(.65,'rgba(33,68,83,.2)');grad.addColorStop(1,'rgba(173,188,163,.25)');c.fillStyle=grad;c.fillRect(r.x,r.y,r.w,r.h);
      c.beginPath();c.rect(r.x,r.y,r.w,r.h);c.clip();
      for(let i=0,n=Math.min(9000,r.w*r.h/1800);i<n;i++){
        const x=r.x+rand()*r.w,y=r.y+rand()*r.h;
        c.globalAlpha=.08+rand()*.15;line(c,x,y,x+8+rand()*42,y-4,'#b2d3cb',2);
      }
      c.globalAlpha=.45;line(c,r.x+3,r.y,r.x+3,r.y+r.h,'#b6bd98',4);line(c,r.x+r.w-3,r.y,r.x+r.w-3,r.y+r.h,'#b6bd98',4);c.restore();
    }

    _paintBridge(c,r) {
      c.save();
      c.fillStyle='rgba(29,42,38,.38)';c.fillRect(r.x+10,r.y+14,r.w,r.h);
      c.fillStyle='#887a60';c.fillRect(r.x,r.y,r.w,r.h);
      c.fillStyle='#b0a081';c.fillRect(r.x,r.y+10,r.w,r.h-20);
      for(let x=r.x+8;x<r.x+r.w;x+=27){line(c,x,r.y+12,x,r.y+r.h-12,'#7b705c',2);line(c,x+3,r.y+12,x+3,r.y+r.h-12,'rgba(236,217,175,.22)',2);}
      c.fillStyle='#c1b28f';c.fillRect(r.x-7,r.y-7,r.w+14,14);c.fillRect(r.x-7,r.y+r.h-7,r.w+14,14);
      c.fillStyle='#80755e';c.fillRect(r.x-7,r.y+7,r.w+14,5);c.fillRect(r.x-7,r.y+r.h+7,r.w+14,5);
      for(let x=r.x;x<=r.x+r.w;x+=70){c.fillStyle='#d0c09c';c.fillRect(x-8,r.y-13,17,21);c.fillRect(x-8,r.y+r.h-13,17,21);c.fillStyle='#766a54';c.fillRect(x-6,r.y+8,13,4);c.fillRect(x-6,r.y+r.h+8,13,4);}
      c.restore();
    }

    _paintCamp(c,x,y,team) {
      const p=TEAM[team];c.save();c.globalAlpha=.75;
      for(let i=0;i<5;i++){
        const yy=y+(i-2)*95,xx=x+(i%2)*23;
        ellipse(c,xx+6,yy+11,40,17,'rgba(22,27,22,.25)');
        poly(c,[[xx-32,yy+13],[xx-22,yy-20],[xx+8,yy-35],[xx+36,yy+8]],'#bdb496','#5c5c49',2);
        poly(c,[[xx-22,yy-20],[xx+8,yy-35],[xx+17,yy+7],[xx-3,yy+15]],p.base);
        poly(c,[[xx-3,yy+15],[xx+6,yy-9],[xx+17,yy+7]],'#3b4138');
      }
      line(c,x+48,y-80,x+48,y-185,'#514937',5);poly(c,[[x+48,y-183],[x+110,y-177],[x+99,y-141],[x+48,y-146]],p.base,'#cfb786',2);
      c.restore();
    }

    _getAtlas(type,team) {
      const key=type+':'+team;
      if(this._atlas.has(key))return this._atlas.get(key);
      const mounted=type==='cavalry'||type==='lancer'||type==='horseArcher'||type==='commander';
      const art=WEAPON_ART[type];
      const cw=art?art.w:type==='siegeTower'?200:type==='batteringRam'?180:type==='hoplite'?144:type==='ballista'?116:type==='elephant'?116:type==='catapult'?100:type==='pikeman'||type==='halberdier'?136:type==='militia'?108:type==='javelin'?96:type==='slinger'?88:mounted?96:type==='swordsman'||type==='guardian'?82:68;
      const ch=art?art.h:type==='siegeTower'?200:type==='batteringRam'?116:type==='commander'?112:type==='pikeman'||type==='halberdier'||type==='elephant'?108:type==='hoplite'?100:type==='lancer'?96:type==='militia'||type==='ballista'?92:type==='catapult'?84:mounted?80:72;
      const scale=2,scratch=newCanvas(cw*scale,ch*scale),ctx=scratch.getContext('2d',{willReadFrequently:true});
      const frames=new Array(ATTACK_FRAMES*8),corpses=new Array(8),unique=new Map();
      // Compact, immutable frame sources avoid sampling an entire large atlas for
      // every soldier. Identical facings/gait frames share the same source object.
      for(let frame=0;frame<ATTACK_FRAMES;frame++)for(let dir=0;dir<8;dir++){
        const direction=spriteFacing(type,dir);
        const gait=frame===3?0:frame;
        const frameKey=direction+':'+gait;
        if(unique.has(frameKey)){frames[frame*8+dir]=unique.get(frameKey);continue;}
        ctx.setTransform(1,0,0,1,0,0);ctx.clearRect(0,0,scratch.width,scratch.height);
        const anchorY=ch*.72;
        ctx.save();ctx.scale(scale,scale);ctx.translate(cw/2,anchorY);ctx.lineCap='round';ctx.lineJoin='round';
        this._drawSprite(ctx,type,team,direction,gait);ctx.restore();
        const sprite=this._cropSprite(scratch,ctx,cw,anchorY,scale);
        unique.set(frameKey,sprite);frames[frame*8+dir]=sprite;
      }
      scratch.width=1;scratch.height=1;
      const atlas={frames,corpses,actions:new Map(),airborne:new Array(8),unique,cw,ch,scale,anchorX:cw/2,anchorY:ch*.72};
      this._atlas.set(key,atlas);return atlas;
    }

    _cropSprite(scratch,ctx,cw,anchorY,scale) {
      const pixels=ctx.getImageData(0,0,scratch.width,scratch.height),rgba=pixels.data;
      let minX=scratch.width,minY=scratch.height,maxX=0,maxY=0;
      for(let y=0;y<scratch.height;y++)for(let x=0;x<scratch.width;x++)if(rgba[(y*scratch.width+x)*4+3]>0){
        if(x<minX)minX=x;if(x>maxX)maxX=x;if(y<minY)minY=y;if(y>maxY)maxY=y;
      }
      minX=Math.max(0,minX-2);minY=Math.max(0,minY-2);maxX=Math.min(scratch.width-1,maxX+2);maxY=Math.min(scratch.height-1,maxY+2);
      const pw=Math.max(1,maxX-minX+1),ph=Math.max(1,maxY-minY+1),full=newCanvas(pw,ph);
      full.getContext('2d').putImageData(ctx.getImageData(minX,minY,pw,ph),0,0);
      const w=pw/scale,h=ph/scale,mid=newCanvas(w,h),low=newCanvas(w*.25,h*.25);
      mid.getContext('2d').drawImage(full,0,0,mid.width,mid.height);
      low.getContext('2d').drawImage(full,0,0,low.width,low.height);
      return {full,mid,low,w,h,ox:minX/scale-cw/2,oy:minY/scale-anchorY};
    }

    _corpseSprite(type,team,dir) {
      const atlas=this._getAtlas(type,team);
      if(atlas.corpses[dir])return atlas.corpses[dir];
      if(this._bodyBuildBudget>0){
        this._bodyBuildBudget--;
        const {cw,ch,scale}=atlas,scratch=newCanvas(cw*scale,ch*scale),ctx=scratch.getContext('2d',{willReadFrequently:true}),anchorY=ch*.5;
        ctx.scale(scale,scale);ctx.translate(cw/2,anchorY);ctx.lineCap='round';ctx.lineJoin='round';
        this._drawFallen(ctx,type,team,dir);
        const sprite=this._cropSprite(scratch,ctx,cw,anchorY,scale);scratch.width=1;scratch.height=1;
        atlas.corpses[dir]=sprite;atlas.unique.set('fallen:'+dir,sprite);return sprite;
      }
      // At most two new fallen variants are rasterized in one render. Until
      // its exact facing is ready, use an already cached pose of the same class.
      for(let offset=1;offset<8;offset++)if(atlas.corpses[(dir+offset)%8])return atlas.corpses[(dir+offset)%8];
      return null;
    }

    _warmBodies() {
      while(this._bodyBuildBudget>0&&this._bodyWarmQueue.length){
        const next=this._bodyWarmQueue.shift();this._corpseSprite(next.type,next.team,next.dir);
      }
    }

    _drawSprite(c,type,team,dir,frame,noShadow,cycle,action) {
      const p=TEAM[team],angle=dir*Math.PI/4,face=Math.cos(angle)<-.2?-1:1;
      const front=Math.sin(angle)>=-.2,walk=Number.isFinite(cycle)?Math.sin(cycle):frame>0&&frame<4?Math.sin(frame*Math.PI*2/3):0,attack=frame>=4;
      if(type==='elephant'){this._drawElephant(c,p,face,front,walk,attack,frame,cycle);return;}
      if(type==='cannon'){this._drawCannon(c,p,angle,walk,attack,frame,cycle,noShadow);return;}
      if(type==='trebuchet'){this._drawTrebuchet(c,p,face,walk,attack,frame,cycle,noShadow);return;}
      if(type==='catapult'){this._drawCatapult(c,p,face,walk,attack,frame,cycle);return;}
      if(type==='ballista'){this._drawBallista(c,p,face,walk,attack,frame,cycle);return;}
      if(type==='siegeTower'){this._drawSiegeTower(c,p,face,walk,attack,frame,cycle);return;}
      if(type==='batteringRam'){this._drawBatteringRam(c,p,face,walk,attack,frame,cycle);return;}
      if(type==='cavalry'||type==='lancer'||type==='horseArcher'||type==='commander'){this._drawMounted(c,type,p,face,front,walk,attack,frame,angle,cycle,action);return;}
      if(!noShadow)ellipse(c,0,1,8.5,3,'rgba(15,22,18,.27)');
      if(type==='hoplite'){this._drawHoplite(c,p,face,front,walk,attack,frame,cycle,action);return;}
      this._drawHuman(c,type,p,face,front,walk,attack,frame,angle,cycle,action);
    }

    _drawSiegeTower(c,p,face,walk,attack,frame,cycle) {
      const ink='#34372b',wood='#8f6b40',edge='#c4a16a',steel='#8d9383',pose=attack?frame-4:0;
      c.save();c.scale(face,1);
      ellipse(c,0,5,36,11,'rgba(17,25,18,.31)');
      // A broad wheeled base, three braced timber stories and a protected bow
      // platform distinguish the tower from both field artillery and rams.
      poly(c,[[-29,-8],[27,-8],[34,4],[-24,7]],'#5e4932',ink,2);
      for(const x of [-24,23]){
        ellipse(c,x,0,8,11,'#5b4d37',ink,2);ellipse(c,x,0,5.7,8,edge,ink,1);
        for(let j=0;j<3;j++){const a=j*Math.PI/3+(Number.isFinite(cycle)?cycle:walk*.35);line(c,x-Math.cos(a)*5,-Math.sin(a)*7,x+Math.cos(a)*5,Math.sin(a)*7,'#665337',1.8);}
        if(Number.isFinite(cycle))line(c,x,0,x+Math.cos(cycle)*5,Math.sin(cycle)*7,'#e0c486',1.4);
        ellipse(c,x,0,1.8,2.2,steel);
      }
      poly(c,[[22,-14],[33,-21],[29,-99],[18,-106]],'#664d35',ink,1.5);
      poly(c,[[-27,-15],[22,-15],[18,-106],[-21,-106]],wood,ink,1.8);
      for(let y=-22;y>=-99;y-=12)line(c,-25,y,20,y,edge,1.5);
      for(const x of [-24,20])line(c,x,-14,x*.82,-108,'#d0aa6c',4.5);
      for(const y of [-18,-50,-82]){line(c,-22,y,18,y-26,'#5e4932',4);line(c,19,y,-20,y-26,edge,2.2);}
      for(const y of [-48,-80]){line(c,-25,y,23,y,'#d4b57a',4);poly(c,[[-9,y-4],[8,y-4],[7,y-19],[-8,y-19]],'#2d3428',edge,1);}
      // Shielded upper deck and its working archer are drawn in every frame;
      // six release/reload poses match the platform's real attack cycle.
      poly(c,[[-25,-107],[20,-107],[31,-101],[-15,-100]],'#c4a16a',ink,1.3);
      for(const x of [-23,-8,8])poly(c,[[x,-103],[x+10,-103],[x+9,-118],[x,-118]],p.dark,edge,1.2);
      line(c,-24,-105,23,-105,edge,2.5);
      const draw=attack?[0,-2,3,5,1,-1][pose]:0;
      line(c,4,-107,5+draw*.25,-121,p.base,7);ellipse(c,6+draw*.25,-125,3.2,3.5,'#caaa80',ink,.8);
      poly(c,[[2+draw*.25,-127],[8+draw*.25,-129],[10+draw*.25,-123],[2+draw*.25,-123]],steel,ink,.8);
      line(c,4,-117,13+draw,-116,'#c9a47b',2.8);
      c.strokeStyle=edge;c.lineWidth=1.9;c.beginPath();c.moveTo(16+draw,-126);c.quadraticCurveTo(27+draw,-116,16+draw,-107);c.stroke();
      line(c,16+draw,-126,16+draw-(attack&&pose<2?6:0),-116,p.trim,.65);line(c,16+draw-(attack&&pose<2?6:0),-116,16+draw,-107,p.trim,.65);
      if(!attack||pose<3)line(c,12+draw,-116,34+draw,-116,'#e0c68b',1);
      line(c,-22,-107,-22,-139,'#d4b782',2);poly(c,[[-22,-138],[0,-136],[-3,-124],[-22,-127]],p.base,p.trim,1);
      for(const x of [-20,17]){ellipse(c,x,-9,2.8,3,'#c7a277',ink,.7);line(c,x,-6,x+walk,-1,p.base,4);}
      c.restore();
    }

    _drawBatteringRam(c,p,face,walk,attack,frame,cycle) {
      const ink='#34372b',edge='#c0a06b',pose=attack?frame-4:0,stroke=attack?[-9,-17,15,24,5,-5][pose]:0;
      c.save();c.scale(face,1);ellipse(c,0,3,54,13,'rgba(17,25,18,.28)');
      poly(c,[[-43,-8],[39,-8],[46,0],[-37,4]],'#695237',ink,1.7);
      for(const x of [-34,32]){
        ellipse(c,x,0,7.4,9.2,'#584a35',ink,1.5);ellipse(c,x,0,5.2,6.8,edge,ink,1);
        line(c,x-4,walk*2,x+4,-walk*2,'#695237',1.8);line(c,x+walk*2,-6,x-walk*2,6,'#695237',1.8);
        if(Number.isFinite(cycle))line(c,x,0,x+Math.cos(cycle)*5,Math.sin(cycle)*6,'#e0c486',1.5);
      }
      for(const x of [-35,31]){line(c,x,-7,x,-42,edge,4);line(c,x,-7,x+10,-38,'#695237',3);}
      // The log visibly retracts and plunges beneath its hide-covered roof.
      line(c,-20,-43,-18+stroke,-21,'#b4ad93',1.6);line(c,19,-43,21+stroke,-21,'#b4ad93',1.6);
      line(c,-40+stroke,-20,40+stroke,-20,'#89623b',10);line(c,-38+stroke,-23,39+stroke,-23,edge,2);
      poly(c,[[36+stroke,-27],[47+stroke,-26],[52+stroke,-19],[46+stroke,-13],[36+stroke,-13]],'#9fa69a',ink,1.4);
      line(c,42+stroke,-25,47+stroke,-19,'#d1cbb2',2);
      for(const x of [-25,-5,16]){
        line(c,x,-10,x-3+walk*1.5,-1,'#484734',3);line(c,x+4,-10,x+6-walk*1.5,-1,'#484734',3);
        line(c,x,-10,x,-22,p.dark,6);ellipse(c,x,-26,3,3.2,'#c9a67d',ink,.7);
        line(c,x,-18,x+6+stroke*.2,-22,'#c9a67d',2.5);
      }
      poly(c,[[-48,-39],[-32,-55],[38,-55],[51,-40],[45,-34],[-45,-34]],'#75674d',ink,1.8);
      poly(c,[[-48,-39],[-32,-55],[38,-55],[22,-39]],'#a58d62',ink,1.3);
      for(let x=-30;x<40;x+=13)line(c,x,-53,x-13,-39,'#c1ab7b',1.2);
      line(c,-46,-36,46,-36,edge,3);
      poly(c,[[-12,-49],[6,-49],[2,-38],[-16,-38]],p.base,p.trim,1);
      c.restore();
    }

    _drawHoplite(c,p,face,front,walk,attack,frame,cycle,action) {
      const pose=attack?frame-4:-1,ink='#30352b',skin='#d5a77b',bronze='#b78d49',bright='#efce82',dark='#735532';
      const shuffling=action==='backstep'||action==='sidestep',sideStep=action==='sidestep';
      const lean=attack?[-.8,-1.6,2.6,4.1,.6,0][pose]:0;
      const spread=attack?[1.7,2.5,3.7,4.5,2.3,1.4][pose]:shuffling?2:1.3;
      const rear=-4-spread+(sideStep?Math.min(0,walk)*2:-walk*(shuffling?.85:1.6)),lead=4+spread*.65+(sideStep?Math.max(0,walk)*2:walk*(shuffling?.85:1.6));
      const lift=shuffling?.75:1.8;
      const footA=Number.isFinite(cycle)?-Math.max(0,Math.cos(cycle))*lift:0,footB=Number.isFinite(cycle)?-Math.max(0,-Math.cos(cycle))*lift:0;
      c.save();c.scale(face,1);
      // A planted rear foot and forward knee carry the shield's weight. The
      // feet keep their real-distance gait; the torso alone leans into a shove.
      line(c,-3,-8,rear,-1+footA,'#544735',3.3);line(c,3,-8,lead,-1+footB,'#544735',3.3);
      line(c,-3.3,-6.5,rear,-2+footA,bronze,2.7);line(c,3.3,-6.5,lead,-2+footB,bright,2.6);
      line(c,rear-1.5,footA,rear+2,footA,'#332f25',2.1);line(c,lead-1,footB,lead+3,footB,'#332f25',2.1);
      c.translate(lean,-Math.abs(walk)*.34+(frame===7?1:0));
      poly(c,[[-6,-21],[4,-22],[7,-8],[4,-4],[-6,-5],[-8,-11]],p.dark,ink,.8);
      // Bronze cuirass over a team-colored skirt, with separate leather tabs.
      poly(c,[[-5,-22],[4,-22],[5.5,-13],[3,-9],[-5,-9],[-6,-14]],bronze,ink,.8);
      line(c,-4,-20,3,-20,bright,1.3);line(c,-3,-18,-3,-12,'#d7b26a',1.1);line(c,1,-19,2,-12,dark,.8);
      poly(c,[[-6,-11],[5,-11],[7,-5],[2,-3],[-7,-5]],p.base,ink,.65);
      for(let i=0;i<4;i++)line(c,-5+i*3,-10,-5+i*3,-5,bright,.85);
      line(c,-5,-11,5,-11,dark,1.9);
      // Closed cheek guards and a long, comb-like crest make the helmet read
      // differently from the steel pike cap and Guardian's small plume.
      ellipse(c,.6,-26,3.7,4.1,skin,ink,.65);
      poly(c,[[-4.5,-25],[-4.3,-29],[-1.8,-32],[2.5,-31.5],[4.8,-28],[4.7,-24],[2.2,-23],[1.8,-27],[-1.6,-27],[-2,-23]],front?bronze:dark,ink,.8);
      line(c,-3,-29,2,-30,bright,1.3);line(c,3.7,-27,4,-22,bronze,1.8);
      if(front){line(c,.7,-26,2.3,-26,'#2b3025',.8);line(c,2.8,-27,3.1,-24,bright,1);}
      else line(c,-2,-30,-2.5,-25,'#c09a53',1.4);
      poly(c,[[-7,-30],[-7,-35],[-3,-39],[2,-39],[6,-35],[6,-32],[2,-34],[-3,-33]],p.base,ink,.8);
      line(c,-5,-34,-2,-37,p.light,1.4);line(c,-1,-35,1,-37,p.light,1.3);line(c,3,-34,4,-35,p.light,1.2);
      line(c,-4,-32,4,-32,bronze,1.7);
      // Overhand spear: draw back, settle behind the shield, thrust, press,
      // withdraw, and reset. The weapon stays in the hand throughout.
      const handX=attack?[5,1,12,8,2,5][pose]:6,handY=attack?[-27,-29,-25,-24,-27,-26][pose]:-24;
      const tipX=attack?[33,24,57,47,32,38][pose]:31,tipY=attack?[-39,-38,-26,-24,-33,-39][pose]:-48;
      const dx=tipX-handX,dy=tipY-handY,length=Math.hypot(dx,dy),nx=dx/length,ny=dy/length;
      line(c,2,-21,handX-3,handY+3,skin,2.6);line(c,handX-3,handY+3,handX,handY,skin,2.5);
      const buttX=tipX-nx*72,buttY=tipY-ny*72;
      line(c,buttX,buttY,tipX,tipY,'#6c5131',1.9);
      line(c,buttX,buttY-.5,tipX,tipY-.5,'#b8a16b',.7);
      poly(c,[[tipX+nx*6,tipY+ny*6],[tipX-ny*2.1,tipY+nx*2.1],[tipX-nx*3,tipY-ny*3],[tipX+ny*2.1,tipY-nx*2.1]],'#e1dfc2',dark,.65);
      line(c,buttX,buttY,buttX-nx*4,buttY-ny*4,bronze,1.9);
      ellipse(c,handX,handY,1.5,1.5,skin,ink,.4);
      // A broad, round bronze shield is always the dominant silhouette. The
      // follow-through moves its rim forward while the spear draws back.
      const sx=attack?[5.2,6.2,7.6,9.7,6.7,5.7][pose]:5.2,sy=frame===7?-14:-15;
      line(c,-3,-19,sx-2,sy,skin,3.2);
      ellipse(c,sx,sy,10.1,10.5,front?bronze:dark,ink,1.2);
      ellipse(c,sx,sy,8.5,8.9,front?'#c19a56':'#917244',bright,1.15);
      ellipse(c,sx,sy,3,3.2,front?'#dfbb72':'#b89555',dark,.8);
      c.strokeStyle=front?'#f2d99a':'#c3a365';c.lineWidth=.8;c.beginPath();c.ellipse(sx,sy,9.2,9.6,0,3.55,5.35);c.stroke();
      poly(c,[[sx-5.5,sy+4],[sx-3.6,sy-3.7],[sx-1.6,sy+4]],null,p.dark,1.2);
      line(c,sx+5.3,sy-4.7,sx+6.5,sy-2.4,p.base,2);
      c.restore();
    }

    _drawHuman(c,type,p,face,front,walk,attack,frame,angle,cycle,action) {
      const berserk=type==='berserker',heavy=type==='guardian',halberd=type==='halberdier',archer=type==='archer',crossbow=type==='crossbow',javelin=type==='javelin',slinger=type==='slinger';
      const shuffling=action==='backstep'||action==='sidestep',sideStep=action==='sidestep';
      const bob=Math.abs(walk)*(shuffling?.22:.65),skin='#d5a77b',steel='#b0bbb5',steelDark='#5e7277',ink='#283330',gold='#d7bc77';
      const pose=attack?frame-4:-1,reach=attack?[-1,-2,3,1.5,-.8,0][pose]:0;
      const loading=crossbow&&frame>=8;
      c.save();c.translate(face*reach,-bob+(loading?2.5:0));
      // Cloaks, trailing quivers, and distinct armor silhouettes.
      if(archer||heavy){poly(c,[[-5,-20],[-8-walk,-4],[1,-1],[7,-5],[5,-20]],archer?p.dark:p.base,ink,.7);}
      if(archer){line(c,-face*4,-22,-face*8,-12,'#71543b',4);for(let i=0;i<3;i++){line(c,-face*(4+i*.8),-26,-face*(6+i*.8),-18,'#c8bda1',.8);line(c,-face*(4+i*.8)-1,-26,-face*(4+i*.8)+1,-24,'#e2dac1',.8);}}
      if(javelin){for(let i=0;i<3;i++){const xx=-face*(5+i*2);line(c,xx,-8,xx-face*5,-33-i*2,'#bda877',1.2);poly(c,[[xx-face*5,-38-i*2],[xx-face*7,-33-i*2],[xx-face*3,-34-i*2]],'#d4d9c5');}line(c,-face*9,-24,-face*4,-11,'#665035',2.5);}
      const stance=attack?[1,2,3.5,2.5,.7,1][pose]:shuffling?1.3:0;
      const legA=(sideStep?Math.min(0,walk)*2:walk*(shuffling?1.25:2.5))+face*stance,legB=(sideStep?Math.max(0,walk)*2:-walk*(shuffling?1.25:2.5))-face*stance*.45;
      const lift=shuffling?.8:2.8;
      const footA=Number.isFinite(cycle)?-Math.max(0,Math.cos(cycle))*lift:0,footB=Number.isFinite(cycle)?-Math.max(0,-Math.cos(cycle))*lift:0;
      line(c,-3,-7,-3+legA,footA,heavy?steelDark:'#3b4540',3.2);line(c,3,-7,3+legB,footB,heavy?steelDark:'#3b4540',3.2);
      line(c,-3+legA,footA-.2,-1+legA,footA+1,'#222e2a',2);line(c,3+legB,footB-.2,5+legB,footB+1,'#222e2a',2);
      if(berserk){
        poly(c,[[-6,-20],[5,-20],[6,-10],[3,-6],[-4,-6],[-7,-11]],skin,ink,.7);
        line(c,-5,-20,4,-9,'#6f4a32',2.6);line(c,5,-20,-4,-9,'#6f4a32',2.2);
        poly(c,[[-5,-10],[5,-10],[6,-5],[-6,-5]],p.base);
        line(c,-5,-9,5,-9,'#775332',1.8);line(c,-6,-19,-6,-16,p.light,2.2);
      }else{
        poly(c,[[-5,-21],[5,-21],[6,-9],[4,-5],[-5,-5],[-6,-10]],heavy||halberd?steel:archer?p.dark:type==='militia'?'#8b7650':slinger?'#c5bb9d':p.base,ink,.85);
        if(type==='swordsman'||heavy||crossbow||halberd){poly(c,[[-4,-20],[4,-20],[4,-12],[-4,-12]],heavy||halberd?'#98aaa9':steelDark);line(c,-3,-19,3,-19,'#d6ded0',1);line(c,0,-19,0,-12,'#cad1c3',.65);}
        if(type==='militia')line(c,-4,-19,4,-8,p.base,3);
        if(type==='pikeman'){line(c,-4,-18,4,-18,'#aab6ad',2);c.fillStyle=p.light;c.fillRect(-2,-12,4,7);}
        if(heavy||halberd){line(c,-6,-19,-7,-12,steelDark,3);line(c,6,-19,7,-12,steelDark,3);line(c,-5,-17,-5,-12,steel,1);}
        if(halberd){poly(c,[[-3,-16],[3,-16],[4,-4],[0,-6],[-4,-4]],p.base);line(c,0,-15,0,-6,p.trim,1);line(c,-7,-20,-4,-20,'#e2dfc4',1.5);line(c,4,-20,7,-20,'#e2dfc4',1.5);}
        if(javelin||slinger){line(c,-4,-20,4,-9,slinger?p.base:'#7b5d3d',3);ellipse(c,-face*6,-10,3,4,'#826642','#4f4934',.8);}
        line(c,-5,-9,5,-9,'#6b4d35',1.8);ellipse(c,0,-9,1.1,1.1,gold);
      }
      // Head, face and class-specific headwear.
      ellipse(c,face*.4,-24,3.5,4.2,skin,ink,.65);
      if(!front){ellipse(c,0,-25,3.9,3.2,berserk?'#b6733e':archer?p.dark:steelDark);}
      if(berserk){poly(c,[[-4,-25],[-3,-29],[0,-30],[3,-28],[4,-24]],'#b86a36');poly(c,[[face*3,-24],[face*4,-19],[face*1,-20],[0,-22]],'#a85c2f');}
      else if(type==='musketeer'){
        poly(c,[[-5,-26],[-3,-32],[2,-33],[5,-27]],'#434749',ink,1);
        line(c,-6,-26,7,-27,'#363c3b',2.1);line(c,-3,-28,4,-28,p.trim,1.2);
        line(c,-face*3,-28,-face*7,-35,p.light,1.7);
      }
      else if(archer){poly(c,[[-4,-22],[-5,-26],[-2,-30],[3,-29],[5,-24],[3,-21],[face*2,-25]],p.dark,ink,.6);line(c,-3,-27,1,-29,p.light,.8);}
      else if(slinger){poly(c,[[-4,-24],[-4,-28],[-1,-30],[4,-28],[4,-24]],'#e1d5b6',ink,.7);line(c,-4,-25,4,-25,p.base,2);poly(c,[[-face*4,-25],[-face*6,-18],[-face*2,-22]],p.base);}
      else if(javelin){poly(c,[[-4,-24],[-3,-28],[1,-30],[4,-27],[4,-24]],'#bca56d',ink,.8);line(c,-4,-24,4,-24,p.base,1.7);line(c,-1,-29,-3,-32,p.light,1.6);}
      else if(type==='militia'){poly(c,[[-4,-24],[-3,-28],[2,-29],[4,-25]],'#71684f',ink,.8);line(c,-4,-24,4,-24,'#b2a181',1);}
      else{
        poly(c,[[-4,-23],[-4,-27],[-2,-29],[2,-29],[4,-27],[4,-23]],heavy?'#c0c9c0':steel,ink,.75);
        line(c,-4,-24,4,-24,'#4c6062',1.4);
        if(crossbow)line(c,-6,-24,6,-24,steel,1.8);
        if(heavy){line(c,0,-29,0,-34,p.base,3);line(c,0,-33,4,-32,p.light,2);line(c,face*2,-24,face*2,-20,steel,1.2);}
        else if(halberd){line(c,-3,-29,3,-29,p.base,2);line(c,-2,-30,3,-32,p.light,1.5);line(c,face*2,-24,face*2,-20,steelDark,1.7);}
        else if(type==='swordsman'){line(c,-1,-29,2,-31,p.light,1.5);}
      }
      if(front){line(c,face*1,-24,face*2.6,-24,'#383c32',.65);}
      // The near arm and weapon are deliberately oversized for battlefield readability.
      const handX=face*(attack?[5,2,13,11,6,7][pose]:7),handY=attack?[-23,-29,-18,-12,-15,-17][pose]:-14;
      if(javelin){
        const hx=face*(attack?[-5,-11,16,13,-7,-3][pose]:8),hy=attack?[-25,-30,-22,-17,-17,-25][pose]:-19;
        line(c,face*4,-19,hx,hy,skin,2.7);ellipse(c,hx,hy,1.4,1.4,skin);
        if(frame!==6&&frame!==7&&frame!==8){
          const ex=attack?hx+face*([22,27,17,0,0,13][pose]):face*19,ey=attack?hy+([-6,-3,-1,0,0,-13][pose]):-40;
          line(c,hx-face*10,hy+4,ex,ey,'#b49a64',1.6);poly(c,[[ex+face*5,ey-3],[ex-face*2,ey-3],[ex+face,ey+2]],'#dce0ca',ink,.45);
        }
        const sx=-face*8;line(c,-face*4,-19,sx,-15,skin,2.5);ellipse(c,sx,-15,5.4,6.3,p.base,'#d0b376',1.2);ellipse(c,sx,-15,2,2.5,'#dfcca0');
      }else if(slinger){
        const hx=face*(attack?[4,7,13,15,4,6][pose]:6),hy=attack?[-22,-27,-24,-17,-11,-18][pose]:-17;line(c,face*4,-18,hx,hy,skin,2.5);line(c,-face*4,-18,-face*7,-12,skin,2.4);
        if(attack){const sx=face*[-13,22,27,24,10,15][pose],sy=[-38,-42,-30,-9,-6,-22][pose];line(c,hx,hy,sx,sy,'#b9a27b',.9);line(c,hx+face,hy,sx+face*2,sy+1,'#e5d5ad',.7);ellipse(c,sx,sy,2.6,1.6,'#795b3d');if(frame<6||frame>=8)ellipse(c,sx+face*.8,sy-.4,1.4,1.2,'#c5c8b5');if(frame<6){c.strokeStyle='rgba(223,211,175,.4)';c.lineWidth=.8;c.beginPath();c.ellipse(face*3,-34,15,8,-.3,frame===4?.5:3,frame===4?Math.PI*1.7:TAU);c.stroke();}}
        else{line(c,hx,hy,face*15,-22,'#c5ad81',.8);line(c,hx+face,hy+1,face*17,-22,'#e6d7b4',.7);ellipse(c,face*16,-22,2.8,1.7,'#82653e');}
      }else if(archer){
        const bx=face*(frame===8?7:11),by=frame===8?-12:-18;
        line(c,face*4,-18,bx,by,p.base,3);ellipse(c,bx,by,1.3,1.3,skin);
        c.strokeStyle='#ad8651';c.lineWidth=1.7;c.beginPath();c.moveTo(bx,by-12);c.quadraticCurveTo(bx+face*12,by,bx,by+12);c.stroke();
        const pull=face*(attack?[3,-3,10,9,-6,5][pose]:6),py=frame===8?-24:by;
        line(c,bx,by-12,frame===8?bx:pull,by,'#e0d6ac',.65);line(c,frame===8?bx:pull,by,bx,by+12,'#e0d6ac',.65);
        line(c,-face*2,-17,pull,py,skin,2);
        if(frame!==6&&frame!==7){line(c,pull,py,frame===8?pull-face*3:bx+face*13,frame===8?py-8:by,'#d4c6a1',.8);if(frame!==8)poly(c,[[bx+face*14,by],[bx+face*11,by-1.5],[bx+face*11,by+1.5]],'#ccd2c3');}
      }else if(crossbow){
        const bx=frame===6?5:frame===7?6:loading?6:9,by=loading?-9:frame===4?-16:-20;
        const tip=loading?{x:bx+5,y:by-12}:{x:bx+14,y:by-1};
        line(c,-4,-16,face*bx,by,p.base,3);line(c,face*4,-18,face*(loading?bx-2:bx+5),loading?by+4:by,skin,2);
        line(c,face*(bx-4),by,face*tip.x,tip.y,'#85603b',3);
        line(c,face*(tip.x-4),tip.y-7,face*(tip.x+2),tip.y+7,'#bdc4b2',1.8);
        const draw=frame===6||frame===7?tip.x:bx;
        line(c,face*(tip.x-4),tip.y-7,face*draw,by,'#e0d0a6',.6);line(c,face*(tip.x+2),tip.y+7,face*draw,by,'#e0d0a6',.6);
        if(frame!==6&&frame!==7&&frame!==8)line(c,face*bx,by,face*(tip.x+3),tip.y,'#ded8ba',.9);
        if(loading){line(c,face*(bx-4),by+3,face*(bx+1),by+4,'#c7b782',1.4);line(c,face*(bx-3),by+3,face*(frame===8?bx-5:bx+4),by+6,skin,2.2);if(frame===9)line(c,face*(bx+2),by-6,face*(tip.x+1),tip.y,'#e3d7af',1);}
        ellipse(c,face*bx,by,1.1,1.1,gold);
      }else if(type==='musketeer'){
        // The barrel is raised for loading, shouldered to aim, and drawn back
        // on the shot. The brass powder bandolier remains visible at rest.
        line(c,-face*4,-21,face*4,-10,'#574332',2);
        for(let k=0;k<3;k++)line(c,face*(-2+k*2),-18+k*2,face*(-2+k*2),-15.5+k*2,'#dbb976',1.2);
        const tilt=attack?[.24,.07,.12,.06,1.34,.96][pose]:.72;
        const grip=attack&&pose===2?-2:1,dx=Math.cos(angle)*Math.cos(tilt);
        const dy=Math.sin(angle)*.58*Math.cos(tilt)-Math.sin(tilt);
        const gx=face*grip,gy=attack&&pose<4?-19:-15;
        line(c,gx-8*dx,gy-8*dy+1,gx+12*dx,gy+12*dy+1,'#805834',3.6);
        line(c,gx+6*dx,gy+6*dy,gx+32*dx,gy+32*dy,'#2f3939',2.7);
        line(c,gx+7*dx,gy+7*dy-.6,gx+31*dx,gy+31*dy-.6,'#bdc9c4',1);
        line(c,-face*4,-18,gx+2*dx,gy+2*dy,p.light,2.8);
        line(c,face*3,-18,gx+11*dx,gy+11*dy,skin,2.3);
        if(frame===8)line(c,gx+17*dx+face*3,gy+17*dy-6,gx+32*dx,gy+32*dy-6,'#c3b79b',1);
      }else if(berserk){
        const bearing=angle+(attack?[-1.15,-1.70,-.42,.98,1.32,.48][pose]:-.65);
        const tilt=attack?[.85,.98,.12,.06,.4,.65][pose]:.78;
        const dx=Math.cos(bearing)*Math.cos(tilt),dy=Math.sin(bearing)*Math.cos(tilt)*.70-Math.sin(tilt);
        const hx=face*3,hy=-18,tipX=hx+32*dx,tipY=hy+32*dy;
        line(c,hx-5*dx,hy-5*dy,tipX,tipY,'#563922',3.5);
        line(c,hx+19*dx,hy+19*dy,tipX,tipY,'#97683e',8);
        line(c,hx+23*dx,hy+23*dy,hx+27*dx,hy+27*dy,'#61665c',9);
        line(c,hx+21*dx-1,hy+21*dy-1,tipX-1,tipY-1,'#c89d65',1.3);
        line(c,-face*5,-18,hx,hy,skin,3.6);line(c,face*4,-17,hx+7*dx,hy+7*dy,skin,3);
      }else{
        line(c,face*4,-18,handX,handY,berserk?skin:heavy?steel:p.base,3);ellipse(c,handX,handY,1.3,1.3,skin);
        if(type==='militia'||type==='pikeman'||halberd){
          const length=type==='pikeman'?43:halberd?36:30;
          const ex=handX+face*(attack?length*[.35,.15,1,.85,.45,.55][pose]:length*.54),ey=handY-(attack?[length*.7,length*.9,3,-4,length*.35,length*.55][pose]:length*.72);
          line(c,handX-face*7,handY+8,ex,ey,'#655036',1.8);line(c,handX-face*7+.4,handY+8,ex+.4,ey,'#b09c6a',.6);
          const dx=ex-handX,dy=ey-handY,dd=Math.hypot(dx,dy)||1,ux=dx/dd,uy=dy/dd;
          poly(c,[[ex+ux*6,ey+uy*6],[ex-uy*2,ey+ux*2],[ex-ux*4,ey-uy*4],[ex+uy*2,ey-ux*2]],'#d7ded0',ink,.5);
          if(halberd){
            const px=-uy,py=ux;
            poly(c,[[ex+ux*2+px*2,ey+uy*2+py*2],[ex+px*9,ey+py*9],[ex-ux*8+px*11,ey-uy*8+py*11],[ex-ux*10+px*5,ey-uy*10+py*5],[ex-ux*4+px*2,ey-uy*4+py*2]],'#dce4d4',steelDark,.8);
            line(c,ex-ux*3,ey-uy*3,ex-ux*5-px*6,ey-uy*5-py*6,steelDark,2);line(c,-face*4,-18,face*4,-12,p.base,3);ellipse(c,face*4,-12,1.3,1.3,skin);
          }
        }else{
          const ex=handX+face*(attack?[1,-6,18,16,4,6][pose]:5),ey=handY-(attack?[18,16,1,-9,14,17][pose]:15);
          line(c,handX,handY,ex,ey,steelDark,3);line(c,handX+.6,handY,ex+.6,ey,'#e0e5d6',1.2);line(c,handX-face*3,handY-1,handX+face*3,handY+1,gold,1.5);
        }
        if(type==='guardian'){
          const sx=-face*7;
          poly(c,[[sx-6,-22],[sx+5,-22],[sx+6,-4],[sx,-1],[sx-6,-5]],p.dark,gold,1.2);
          poly(c,[[sx-4,-20],[sx+3,-20],[sx+4,-6],[sx,-4],[sx-4,-6]],p.base);
          line(c,sx,-19,sx,-5,p.trim,1.4);line(c,sx-4,-14,sx+4,-14,p.trim,1.4);
        }else if(type==='swordsman'){
          const sx=-face*7;poly(c,[[sx-5,-21],[sx+5,-21],[sx+5,-11],[sx,-6],[sx-5,-11]],p.base,gold,.9);line(c,sx,-19,sx,-9,p.light,1.2);ellipse(c,sx,-15,1.5,1.5,'#c8c9b0');
        }else if(type==='militia'){
          const sx=-face*6;ellipse(c,sx,-15,4.5,5.6,'#8b7249','#c6ad75',1);line(c,sx-3,-18,sx+3,-12,p.base,2);ellipse(c,sx,-15,1.2,1.4,'#9ca89f');
        }
      }
      c.restore();
    }

    _drawMounted(c,type,p,face,front,walk,attack,frame,angle,cycle,action) {
      const commander=type==='commander',armored=type==='lancer'||commander;
      const pose=attack?frame-4:-1;
      const horse=commander?'#bbc5bd':armored?'#777e78':type==='horseArcher'?'#8c6848':'#695546',horseLight=commander?'#f0eee0':armored?'#a5ada2':type==='horseArcher'?'#b18b63':'#8c7158';
      ellipse(c,0,1,19,6,'rgba(16,24,19,.27)');c.save();c.scale(face,1);
      const shuffling=action==='backstep'||action==='sidestep';
      const step=walk*(action==='gallop'?6.2:shuffling?1.8:4),footLift=action==='gallop'?5:shuffling?1:2.8;
      const fore=Number.isFinite(cycle)?-Math.max(0,Math.cos(cycle))*footLift:0,rear=Number.isFinite(cycle)?-Math.max(0,-Math.cos(cycle))*footLift:0;
      // Four articulated legs, coat, mane, and a recognizable head.
      line(c,-10,-10,-14-step,-1+rear,horse,3.3);line(c,9,-10,13+step,-1+fore,horse,3.3);
      line(c,-7,-9,-6+step,2+fore,horseLight,3.2);line(c,12,-9,10-step,2+rear,horseLight,3.2);
      for(const [x,y] of [[-14-step,rear],[13+step,fore],[-6+step,fore+2],[10-step,rear+2]])line(c,x,y,x+2,y+1,'#27312c',2.5);
      ellipse(c,0,-14,16,9,horse,'#39443a',.6);ellipse(c,-1,-18,13,6,horseLight);
      poly(c,[[9,-15],[13,-32],[20,-33],[23,-24],[17,-16]],horseLight,'#39443a',.7);
      poly(c,[[15,-30],[19,-37],[21,-30]],horse);
      ellipse(c,21,-28,7,4.6,horseLight);ellipse(c,25,-26,2.8,2.2,horse);
      ellipse(c,22,-30,1,1,'#263029');line(c,15,-32,12,-20,commander?'#fbf7e2':'#3d3d31',3.3);
      line(c,-14,-17,-23,-12,commander?'#e0e1d4':'#3d3b30',3);line(c,-23,-12,-24,-5,commander?'#e0e1d4':'#3d3b30',2);
      if(armored){
        poly(c,[[-14,-18],[11,-18],[15,-8],[-10,-6]],p.base,commander?'#e9c86e':'#53666a',commander?1.4:.7);line(c,-11,-8,13,-10,commander?'#f2d37c':p.trim,1.6);
        for(let i=0;i<5;i++)line(c,-10+i*5,-17,-9+i*5,-9,'#8b9d9c',.7);
        poly(c,[[14,-31],[22,-33],[27,-27],[21,-23],[15,-25]],commander?'#e1c578':'#b5c0b6',commander?'#877041':'#657878',.7);
      }else{poly(c,[[-9,-20],[6,-20],[8,-9],[-8,-10]],p.base,p.trim,1);}
      line(c,22,-27,5,-23,'#483d30',.8);line(c,22,-27,3,-26,'#483d30',.8);
      if(commander){
        // The white charger, long gilded cape, and tall standard identify a
        // commander even when he is surrounded by ordinary mounted troops.
        poly(c,[[-6,-39],[-18-walk*2,-35],[-31-walk*2,-15],[-12,-18],[1,-30]],p.dark,'#efd17d',1.1);
        poly(c,[[-6,-39],[-15,-34],[-25-walk*2,-18],[-13,-21]],p.base);
        line(c,-17,-18,-17,-72,'#d7b86b',1.8);
        poly(c,[[-17,-72],[8,-69],[5,-56],[-17,-59]],p.base,'#f4d887',1);
        poly(c,[[-10,-64],[-10,-69],[-6,-66],[-3,-70],[0,-66],[3,-68],[2,-62]],'#f5df95');
        line(c,-10,-62,2,-61,'#fff3c7',1);ellipse(c,-17,-73,2.2,2.2,'#f3d67f');
      }
      // Rider remains upright while the horse gait changes beneath him.
      const lift=Math.abs(walk)*.8;c.translate(-1+(attack?[-1,-2,2,1,0,0][pose]:0),-18-lift);
      poly(c,[[-5,-15],[4,-15],[5,-4],[1,0],[-5,-1]],commander?'#dbc781':armored?'#9fafa8':p.base,'#34433c',.8);
      if(commander){line(c,-4,-13,3,-13,'#fff0b7',1.3);line(c,0,-14,0,-5,p.base,2.1);}
      line(c,-3,-3,-6,6,'#46514a',3);line(c,-6,6,-3,7,'#252f2b',2);
      line(c,2,-13,8,-8,armored?'#9fafa8':p.base,2.7);line(c,8,-8,10,-7,'#d5ac82',2);
      ellipse(c,0,-19,3.5,4,'#cfa47b');
      if(type==='horseArcher'){
        poly(c,[[-4,-18],[-5,-23],[-2,-26],[3,-24],[4,-19],[2,-17]],p.dark,'#293b32',.7);
        line(c,-4,-19,-8,-10,'#78593c',3);for(let i=0;i<3;i++)line(c,-6+i*.8,-23,-8+i*.8,-15,'#ded8b7',.7);
        const pull=attack?[5,0,11,10,7,6][pose]:10;
        c.strokeStyle='#bf975e';c.lineWidth=1.6;c.beginPath();c.moveTo(10,-24);c.quadraticCurveTo(24,-13,11,-3);c.stroke();line(c,10,-24,pull,-13,'#ddd4b0',.7);line(c,pull,-13,11,-3,'#ddd4b0',.7);line(c,-2,-14,frame===8?-5:pull,frame===8?-23:-13,'#d5ac82',2);if(frame!==6&&frame!==7)line(c,pull,-13,23,-14,'#e0d7b4',.8);
      }else{
        poly(c,[[-4,-18],[-4,-22],[-1,-25],[3,-24],[4,-18]],commander?'#e6ce86':'#bdc7bb',commander?'#887446':'#485d5e',.6);line(c,-3,-19,4,-19,'#4a6165',1);
        line(c,0,-25,-2,-30,commander?'#f1ebce':p.base,commander?4.3:3.4);line(c,-2,-29,-7,-27,commander?'#fcf6dd':p.light,commander?3.8:2.6);
        const sx=-5;poly(c,[[sx-5,-16],[sx+4,-16],[sx+4,-7],[sx,-2],[sx-5,-6]],p.base,p.trim,1);line(c,sx,-14,sx,-5,p.light,1.1);
        if(commander){line(c,sx-3,-13,sx+2,-13,'#efd27d',1.3);line(c,sx,-15,sx,-7,'#efd27d',1.4);}
        if(type==='lancer'){
          const end=attack?[[28,-31],[30,-23],[41,-13],[36,-10],[24,-20],[29,-30]][pose]:[32,-37];line(c,4,-11,end[0],end[1],'#82633d',1.6);line(c,3,-10,-5,-4,'#82633d',1.6);poly(c,[[end[0]+4,end[1]-2],[end[0]-3,end[1]-3],[end[0]-1,end[1]+2]],'#dfe5d4');
          poly(c,[[end[0]-6,end[1]+3],[end[0]-14,end[1]+9],[end[0]-3,end[1]+12]],p.base,p.light,.5);
        }else{
          const end=attack?[[9,-31],[0,-34],[27,-14],[24,-4],[12,-23],[14,-29]][pose]:[15,-30];line(c,7,-12,end[0],end[1],'#e0e5d4',2.1);line(c,5,-14,10,-11,'#d3b56b',1.3);
        }
      }
      c.restore();
    }

    _artilleryWheel(c,x,y,r,phase,flatten) {
      const rx=r*flatten,ry=r;
      ellipse(c,x,y,rx,ry,'#705435','#363b33',2);
      ellipse(c,x,y,rx*.78,ry*.78,'#b58c59','#57452e',1);
      for(let i=0;i<4;i++){
        const a=phase+i*Math.PI/4,dx=Math.cos(a)*rx*.73,dy=Math.sin(a)*ry*.73;
        line(c,x-dx,y-dy,x+dx,y+dy,'#584832',1.5);
      }
      ellipse(c,x,y,rx*.22,ry*.22,'#c8b782','#494840',.8);
    }

    _drawCannon(c,p,angle,walk,attack,frame,cycle,noShadow) {
      const co=Math.cos(angle),si=Math.sin(angle),pose=frame-4;
      const point=(along,across,h)=>[co*along-si*across,(si*along+co*across)*.54-h];
      const phase=Number.isFinite(cycle)?cycle:walk*.5;
      if(!noShadow)ellipse(c,0,4,36,12,'rgba(19,25,18,.30)');
      const hub=point(2,co<0?-13:13,6);
      const far=point(2,co<0?13:-13,6);
      const wheelWidth=.40+.54*Math.abs(co);
      this._artilleryWheel(c,far[0],far[1],11,phase,wheelWidth);
      for(const side of [-1,1]){
        line(c,...point(-32,side*3,2),...point(9,side*8,10),'#504231',6);
        line(c,...point(-30,side*3,3),...point(10,side*8,12),'#a37c4a',3.5);
      }
      line(c,...point(-29,-4,3),...point(-29,4,3),p.base,4);
      const recoil=attack?[0,1,-6,-3,-1,0][pose]:0;
      const breech=point(-9+recoil,0,16),muzzle=point(30+recoil,0,20);
      line(c,...breech,...muzzle,'#313a37',11);
      line(c,breech[0],breech[1]-1.2,muzzle[0],muzzle[1]-1.2,'#7e8272',7.6);
      for(const distance of [-6,5,23]){
        const at=point(distance+recoil,0,16+(distance+9)*.1);
        line(c,at[0]-si*3,at[1]-3,at[0]+si*3,at[1]+3,'#b09b68',2.5);
      }
      ellipse(c,muzzle[0],muzzle[1],2.6+3*Math.abs(si),5.3,'#29302d','#a59369',1.6);
      this._artilleryWheel(c,hub[0],hub[1],11,phase,wheelWidth);
      // Gunner primes the breech, then swabs and reloads the muzzle.
      const gunner=point(attack&&pose>=4?17:-17,-16,2),gx=gunner[0],gy=gunner[1];
      line(c,gx-2,gy-7,gx-3-walk,gy,'#353d37',3);
      line(c,gx+2,gy-7,gx+4+walk,gy,'#353d37',3);
      line(c,gx,gy-15,gx,gy-7,p.base,6);ellipse(c,gx,gy-18,3.3,3.6,'#d0ac83');
      line(c,gx-4,gy-20,gx+4,gy-20,'#3e4641',2.4);
      const hand=point(attack?[-14,-9,-15,-18,29,22][pose]:-8,-8,13);
      line(c,gx,gy-13,...hand,'#d4ae86',2.3);
      if(frame>=8)line(c,...hand,...point(frame===8?42:30,-4,23),'#c6a978',1.6);
    }

    _drawTrebuchet(c,p,face,walk,attack,frame,cycle,noShadow) {
      const ink='#40382c',wood='#976d42',edge='#c4a171',pose=frame-4;
      c.save();c.scale(face,1);
      if(!noShadow)ellipse(c,0,4,43,13,'rgba(19,25,18,.3)');
      const phase=Number.isFinite(cycle)?cycle:walk*.5;
      for(const x of [-27,25])this._artilleryWheel(c,x+6,-3,7,phase,.85);
      poly(c,[[-34,-11],[28,-14],[38,-6],[-28,-2]],wood,ink,1.7);
      const tip=attack?[-48,-51,38,45,18,-28][pose]:-46;
      const lift=attack?[19,12,87,76,94,60][pose]:26;
      const pivot=-49,dy=-lift-pivot,cx=-tip*.33,cy=pivot-dy*.33;
      line(c,-23,-8,3,pivot-2,'#6d5137',6);line(c,31,-11,3,pivot-2,'#b28d59',6);
      line(c,cx,cy,cx+1,cy+13,'#514331',3.3);
      poly(c,[[cx-8,cy+11],[cx+6,cy+9],[cx+10,cy+14],[cx+8,cy+24],[cx-5,cy+25],[cx-9,cy+18]],'#8f9180',ink,1.3);
      line(c,cx-5,cy+12,cx-2,cy+23,'#6d5941',2);line(c,cx+4,cy+12,cx+5,cy+23,'#6d5941',2);
      line(c,cx,cy,tip,-lift,wood,5.5);line(c,cx,cy-1,tip,-lift-1,edge,1.5);
      const pouchX=tip+(attack?[13,18,32,23,8,6][pose]:10);
      const pouchY=attack?[-5,-4,-94,-64,-66,-38][pose]:-6;
      line(c,tip,-lift,pouchX,pouchY,'#dad0ad',1.2);line(c,tip+2,-lift+1,pouchX+5,pouchY-1,'#a69a7f',1);
      if(!attack||pose<2||pose===5)ellipse(c,pouchX+2,pouchY-2,5.4,4.3,'#899286','#48534b',1.1);
      poly(c,[[pouchX-5,pouchY-1],[pouchX+7,pouchY-2],[pouchX+4,pouchY+2],[pouchX-2,pouchY+3]],'#705238',ink,.8);
      line(c,-30,-3,-1,pivot,'#c09b66',5.5);line(c,24,-5,-1,pivot,'#a37d4c',5.5);
      line(c,-16,-25,12,-25,'#7d5c39',3);ellipse(c,-1,pivot,3.7,3.7,'#b2ada0',ink,1.2);
      line(c,-23,-5,tip,-lift,'#ad9a74',1);
      for(const x of [-27,25])this._artilleryWheel(c,x,1,7,phase,.85);
      const crewX=-29,crewY=-9;
      line(c,crewX,crewY-6,crewX-2-walk,crewY+4,'#3d443d',3);
      line(c,crewX+2,crewY-5,crewX+4+walk,crewY+4,'#3d443d',3);
      line(c,crewX,crewY-13,crewX,crewY-5,p.base,6);ellipse(c,crewX,crewY-17,3.2,3.5,'#cfac84',ink,.6);
      line(c,crewX-4,crewY-19,crewX+4,crewY-19,'#656d60',2);
      line(c,crewX+1,crewY-11,-21,-11+(frame===8?3:frame===9?-3:0),'#d4b28b',2);
      poly(c,[[5,-32],[13,-32],[16,-20],[10,-24],[7,-21]],p.base,p.trim,.8);
      c.restore();
    }

    _drawCatapult(c,p,face,walk,attack,frame,cycle) {
      ellipse(c,0,3,24,10,'rgba(19,25,18,.3)');c.save();c.scale(face,1);
      // Heavy wheeled carriage, winding rope, counterweight and throwing spoon.
      poly(c,[[-24,-15],[12,-19],[27,-6],[-10,-1]],'#86613c','#463b2b',1.2);
      poly(c,[[-24,-15],[-10,-1],[-10,5],[-24,-8]],'#5e482f');poly(c,[[-10,-1],[27,-6],[27,0],[-10,5]],'#624a31');
      for(const q of [[-18,-1],[18,-4]]){ellipse(c,q[0],q[1],6.5,8,'#5d4934','#2b342b',1.4);ellipse(c,q[0],q[1],4.7,6,'#9f8053');for(let a=0;a<Math.PI;a+=Math.PI/3){const aa=a+(Number.isFinite(cycle)?cycle:0);line(c,q[0]+Math.cos(aa)*5,q[1]+Math.sin(aa)*6,q[0]-Math.cos(aa)*5,q[1]-Math.sin(aa)*6,'#54432e',1);}if(Number.isFinite(cycle))line(c,q[0],q[1],q[0]+Math.cos(cycle)*4,q[1]+Math.sin(cycle)*5,'#d8ba7f',1.3);ellipse(c,q[0],q[1],1.8,2.2,'#b8b9a1');}
      line(c,-10,-9,-1,-29,'#58482e',5);line(c,12,-11,-1,-29,'#ac8651',5);line(c,-1,-29,-2,-10,'#7a5b37',4);
      const ex=attack?[-18,-21,13,23,4,-10][frame-4]:19,ey=attack?[-35,-32,-49,-39,-47,-41][frame-4]:-35;
      line(c,-10,-18,ex,ey,'#c09a60',4.2);line(c,-10,-18,ex,ey,'#d0ae72',1.2);
      ellipse(c,ex,ey,6,3,'#776344','#3a382b',1);
      if(!attack||frame<6||frame===9)ellipse(c,ex,ey-3,4.5,4,'#87958a','#3e5046',.6);
      line(c,-16,-12,ex,ey,'#d4bf8c',.8);line(c,-17,-12,-10,-18,'#e1d0a0',.8);
      c.fillStyle=p.base;c.fillRect(3,-19,9,6);line(c,5,-18,5,-14,p.trim,1);line(c,9,-18,9,-14,p.trim,1);
      // Crew make the scale unambiguous.
      ellipse(c,-23,-16,3,3.3,'#c7a077');poly(c,[[-26,-13],[-20,-13],[-19,-6],[-25,-5]],p.base);ellipse(c,-23,-18,3.5,2,'#7a8c82');
      c.restore();
    }

    _drawBallista(c,p,face,walk,attack,frame,cycle) {
      ellipse(c,1,4,31,12,'rgba(18,26,20,.30)');c.save();c.scale(face,1);
      const recoil=attack?[0,1,-4,-2,0,0][frame-4]:0;
      // A broad steel bow on a wheeled timber carriage, with a central bolt
      // rail, winding ropes, and crew. Its silhouette is unlike the catapult.
      poly(c,[[-31,-13],[12,-21],[31,-7],[-13,3]],'#8b6743','#4d432f',1.2);
      poly(c,[[-31,-13],[-13,3],[-13,8],[-31,-6]],'#5e4a32');poly(c,[[-13,3],[31,-7],[31,-1],[-13,8]],'#665037');
      for(const q of [[-23,1],[20,-2]]){
        ellipse(c,q[0],q[1],7.4,8.7,'#514a35','#293b30',1.2);ellipse(c,q[0],q[1],5.3,6.5,'#aa8856');
        for(let a=0;a<Math.PI;a+=Math.PI/3){const aa=a+(Number.isFinite(cycle)?cycle:0);line(c,q[0]+Math.cos(aa)*5,q[1]+Math.sin(aa)*6,q[0]-Math.cos(aa)*5,q[1]-Math.sin(aa)*6,'#544732',1.2);}if(Number.isFinite(cycle))line(c,q[0],q[1],q[0]+Math.cos(cycle)*4,q[1]+Math.sin(cycle)*5,'#d8ba7f',1.3);
        ellipse(c,q[0],q[1],2,2.5,'#bfc8b7');
      }
      line(c,-7,-5,4,-27,'#765b3a',5);line(c,12,-7,4,-27,'#ba955d',4.5);
      poly(c,[[-27+recoil,-20],[24+recoil,-31],[29+recoil,-24],[-24+recoil,-13]],'#bea16b','#5d583e',1);
      line(c,-23+recoil,-18,31+recoil,-28,'#e0d0a0',1.3);
      const bx=18+recoil;
      c.strokeStyle='#54676b';c.lineWidth=5;c.beginPath();c.moveTo(bx+10,-48);c.quadraticCurveTo(bx-9,-28,bx+17,-5);c.stroke();
      c.strokeStyle='#bdc9bd';c.lineWidth=2.2;c.beginPath();c.moveTo(bx+10,-48);c.quadraticCurveTo(bx-9,-28,bx+17,-5);c.stroke();
      ellipse(c,bx+10,-48,2.4,2.4,'#d4bd80');ellipse(c,bx+17,-5,2.4,2.4,'#d4bd80');
      const drawn=attack?(frame===6?bx+9:frame===7?bx+3:frame===8?-3:-16+recoil):-16+recoil;
      line(c,bx+10,-48,drawn,-20,'#e3d7af',.9);line(c,drawn,-20,bx+17,-5,'#e3d7af',.9);
      if(!attack||frame<6||frame===9){line(c,-24+recoil,-19,42+recoil,-31,'#d4ba7d',2.2);poly(c,[[49+recoil,-32],[39+recoil,-35],[41+recoil,-27]],'#e1e8d8','#64736b',.5);line(c,-21+recoil,-20,-27+recoil,-25,'#d7dfc9',1.7);line(c,-21+recoil,-18,-25+recoil,-12,'#d7dfc9',1.7);}
      line(c,-27,-16,-27,-49,'#b7955d',1.6);poly(c,[[-27,-49],[-10,-46],[-12,-37],[-27,-40]],p.base,p.trim,.7);
      line(c,-23,-45,-15,-43,p.light,1.2);line(c,-19,-47,-19,-41,p.trim,1);
      ellipse(c,-35,-21,3.3,3.6,'#d0a77e');poly(c,[[-38,-18],[-31,-18],[-29,-9],[-36,-8]],p.base,'#384939',.6);line(c,-36,-23,-31,-23,'#b9c5b7',2.3);
      line(c,-32,-15,frame===8?-29:frame===9?-17:-23,frame===8?-8:frame===9?-23:-17,'#d0a77e',2);
      if(frame===8||frame===9){ellipse(c,-25,-10,3,3,'#766646','#c4b383',.8);line(c,-25,-10,frame===8?-30:-20,frame===8?-7:-13,'#c7b37b',1.5);}
      c.restore();
    }

    _drawElephant(c,p,face,front,walk,attack,frame,cycle) {
      ellipse(c,0,5,32,13,'rgba(19,25,18,.28)');c.save();c.scale(face,1);
      const pose=attack?frame-4:-1,thrust=attack?[-1,-2,3,2,0,0][pose]:0;c.translate(thrust,0);
      const s=walk*2.8,skin='#7f8881',light='#a7aaa0',dark='#626f69';
      const liftA=Number.isFinite(cycle)?-Math.max(0,Math.cos(cycle))*3:0,liftB=Number.isFinite(cycle)?-Math.max(0,-Math.cos(cycle))*3:0;
      line(c,-17,-13,-19-s,6+liftA,dark,8);line(c,17,-14,19+s,5+liftB,dark,8);line(c,-9,-13,-8+s,9+liftB,light,8);line(c,11,-12,10-s,10+liftA,light,8);
      for(const [x,y] of [[-19-s,8+liftA],[19+s,8+liftB],[-8+s,8+liftB],[10-s,8+liftA]])line(c,x-2,y,x+3,y,'#c1c0ac',2);
      ellipse(c,-1,-24,28,20,skin,'#4c6158',1);ellipse(c,-8,-30,22,14,light);
      line(c,-27,-24,-35,-10,dark,2.5);line(c,-35,-10,-34,-5,'#3c4840',3);
      ellipse(c,23,-29,15,16,light,'#4c6158',1);
      ellipse(c,15,-25,10,13,skin,'#6b7c71',1);ellipse(c,15,-27,7,9,'#969e91');
      c.strokeStyle=skin;c.lineWidth=8;c.beginPath();c.moveTo(33,-26);c.quadraticCurveTo(45,-5,attack?[32,35,43,40,33,31][pose]:31,attack?[-3,-9,-13,-6,1,4][pose]:4);c.stroke();
      c.strokeStyle='#b7baaa';c.lineWidth=2;c.beginPath();c.moveTo(34,-26);c.quadraticCurveTo(42,-6,33,1);c.stroke();
      c.strokeStyle='#e7dec0';c.lineWidth=3.2;c.beginPath();c.moveTo(29,-19);c.quadraticCurveTo(39,-9,44,-20);c.stroke();
      ellipse(c,30,-31,1.4,1.5,'#263e32');line(c,24,-39,34,-37,'#ccd2bb',2);
      poly(c,[[-23,-36],[12,-39],[17,-16],[-18,-12]],p.dark,p.trim,1.4);
      poly(c,[[-20,-34],[11,-36],[13,-20],[-17,-17]],p.base);line(c,-17,-20,11,-23,p.trim,2);line(c,-3,-34,-3,-20,p.light,2);
      // Armored howdah with two visibly distinct crew and a battle standard.
      poly(c,[[-15,-44],[8,-46],[13,-37],[-11,-34]],'#8f6e43','#4b4c36',1.2);poly(c,[[-15,-44],[-11,-34],[-11,-27],[-15,-35]],'#65573b');poly(c,[[-11,-34],[13,-37],[13,-30],[-11,-27]],p.base,p.trim,.8);
      for(const q of [[-8,-44],[5,-46]]){ellipse(c,q[0],q[1]-6,3,3.7,'#d1a479');poly(c,[[q[0]-4,q[1]-3],[q[0]+4,q[1]-3],[q[0]+4,q[1]+4],[q[0]-3,q[1]+4]],p.light);line(c,q[0]-3,q[1]-8,q[0]+3,q[1]-8,'#b9c5b8',2);}
      line(c,-13,-37,-13,-66,'#bd9a60',1.5);poly(c,[[-13,-66],[5,-63],[2,-53],[-13,-55]],p.base,p.trim,.6);line(c,-9,-62,-9,-57,p.light,1);
      const spearX=attack?[19,15,37,31,21,22][pose]:22,spearY=attack?[-63,-66,-47,-48,-59,-61][pose]:-61;
      line(c,9,-44,spearX,spearY,'#c9b47b',1.4);poly(c,[[spearX,spearY],[spearX+3,spearY-6],[spearX+3,spearY+2]],'#d8dfcd');
      c.restore();
    }

    _drawFallen(c,type,team,dir) {
      const p=TEAM[team],mounted=['cavalry','lancer','horseArcher','commander'].includes(type),siege=type==='catapult'||type==='ballista'||type==='cannon'||type==='trebuchet';
      const ink='#343e32',steel='#9ba59b',skin='#b99570';
      c.save();c.scale(1,.62);c.rotate(dir*Math.PI/4);
      if(type==='siegeTower'||type==='batteringRam'){
        const tower=type==='siegeTower';
        ellipse(c,0,2,tower?44:52,23,'rgba(24,29,22,.24)');
        poly(c,[[-36,-14],[32,-17],[42,12],[-29,21]],'#695239',ink,1.6);
        for(let j=0;j<6;j++)line(c,-36+j*12,-18+(j%2)*8,-29+j*11,18-(j%3)*6,j%2?'#b08d5c':'#7c613e',3.3);
        for(const q of [[-34,17],[29,-19]]){ellipse(c,q[0],q[1],8,8,'#997c52',ink,1.5);line(c,q[0]-6,q[1],q[0]+6,q[1],ink,1.4);}
        if(tower){
          poly(c,[[-25,-9],[8,-17],[28,8],[-10,16]],'#9e8157',ink,1.2);
          line(c,-32,-22,28,18,'#ceac73',5);line(c,-23,24,32,-18,'#574b36',4);
          for(let j=0;j<3;j++)poly(c,[[-20+j*11,-17],[-12+j*11,-20],[-8+j*11,-10],[-16+j*11,-7]],p.dark,p.trim,1);
          line(c,-34,6,-8,29,'#b69b68',2);poly(c,[[-29,11],[-15,10],[-10,21],[-19,20]],p.base,p.trim,1);
        }else{
          line(c,-43,-4,42,4,'#a37c4b',11);poly(c,[[36,-2],[49,-1],[53,5],[46,12],[36,10]],steel,ink,1.3);
          poly(c,[[-20,-18],[21,-21],[31,-8],[-13,-4]],'#8d7a57',ink,1.3);line(c,-19,-14,26,-11,p.base,5);
        }
      }else if(siege){
        ellipse(c,0,2,35,17,'rgba(24,29,22,.21)');
        poly(c,[[-25,-8],[21,-7],[26,9],[-19,12]],'#765b3d',ink,1.2);
        for(const q of [[-24,-9],[22,11]]){ellipse(c,q[0],q[1],6.5,6.5,'#9a7b51',ink,1.3);line(c,q[0]-5,q[1],q[0]+5,q[1],ink,1);line(c,q[0],q[1]-5,q[0],q[1]+5,ink,1);}
        line(c,-24,8,18,-10,'#b18d59',4);line(c,-20,-12,16,12,'#574935',3);
        if(type==='catapult'){line(c,-5,0,28,-18,'#b69863',4);ellipse(c,29,-19,6,4,'#776042',ink,1);}
        else if(type==='cannon'){
          line(c,-19,13,23,-9,'#454d48',10);line(c,-16,10,21,-10,'#99895e',2);
          ellipse(c,24,-9,3.5,5,'#283136','#a18c63',1.6);
        }else if(type==='trebuchet'){
          line(c,-37,14,35,-22,'#b48c59',5);line(c,21,-16,30,8,'#c5b692',1.3);
          poly(c,[[-30,-13],[-16,-16],[-11,-5],[-25,1]],'#6e756c',ink,1.5);
        }else{line(c,-26,3,32,-5,'#b9a16e',4);c.strokeStyle=steel;c.lineWidth=4;c.beginPath();c.moveTo(22,-18);c.quadraticCurveTo(8,-3,29,16);c.stroke();line(c,22,-18,-8,0,'#cbbb8e',.8);line(c,-8,0,29,16,'#cbbb8e',.8);}
        poly(c,[[-14,-5],[-3,-6],[-2,1],[-12,4]],p.base,p.trim,.7);
      }else if(type==='elephant'){
        ellipse(c,-2,1,32,19,'#6e7d73',ink,1);ellipse(c,0,-3,27,13,'#929c8d');
        for(const q of [[-18,9],[-8,-12],[12,-12],[16,11]])line(c,q[0],q[1],q[0]-5,q[1]*1.6,'#818e81',7);
        ellipse(c,29,1,14,13,'#9aa592',ink,.8);ellipse(c,20,0,8,11,'#748576');
        c.strokeStyle='#8c9989';c.lineWidth=6;c.beginPath();c.moveTo(39,4);c.quadraticCurveTo(49,10,40,18);c.stroke();
        line(c,35,7,45,10,'#d9d1b3',3);line(c,34,-5,45,-4,'#ded6b8',3);
        poly(c,[[-19,-9],[10,-10],[15,9],[-20,13]],p.dark,p.trim,1.3);poly(c,[[-12,-10],[8,-10],[10,5],[-9,8]],'#94794f',ink,1);
        line(c,-24,-18,13,-19,'#b7a168',1.5);poly(c,[[-22,-18],[-22,-28],[-10,-25],[-9,-18]],p.base,p.trim,.8);
      }else if(mounted){
        const horse=type==='commander'?'#c0c8b8':type==='horseArcher'?'#936f4d':'#7c786b';
        ellipse(c,-2,1,22,12,horse,ink,.8);
        line(c,-12,8,-22,17,horse,4);line(c,-6,-7,-11,-18,horse,4);line(c,11,7,19,17,horse,4);line(c,9,-8,19,-16,horse,4);
        line(c,-22,1,-31,7,type==='commander'?'#dddcc9':'#4c483a',3);
        poly(c,[[12,-4],[19,-12],[30,-8],[30,-1],[19,5]],horse,ink,.8);ellipse(c,30,-4,6,4,horse,ink,.7);
        poly(c,[[-12,-9],[6,-10],[10,6],[-9,8]],p.base,p.trim,1);
        poly(c,[[-7,9],[3,9],[7,18],[-4,21]],type==='commander'?'#cbb977':p.dark,ink,.8);ellipse(c,7,19,3.4,3,skin,ink,.5);
        line(c,-6,17,-14,23,'#48534a',3);line(c,-2,20,-8,26,'#48534a',3);
        if(type==='lancer'||type==='commander'){line(c,-31,-19,31,-15,'#a08b5b',1.5);poly(c,[[17,-16],[18,-26],[31,-23],[29,-15]],p.base,p.trim,.7);}
        else if(type==='horseArcher'){c.strokeStyle='#b5915b';c.lineWidth=1.7;c.beginPath();c.moveTo(-28,-10);c.quadraticCurveTo(-39,0,-28,11);c.stroke();line(c,-28,-10,-28,11,'#d8c89c',.6);}
        else line(c,13,18,28,24,steel,2.2);
      }else if(type==='hoplite'){
        ellipse(c,0,2,21,11,'rgba(24,30,22,.18)');
        line(c,-6,-2,-18,-6,'#ad8343',3.5);line(c,-6,3,-18,8,'#c5a261',3.5);
        line(c,-19,-6,-22,-5,ink,2);line(c,-19,8,-22,7,ink,2);
        poly(c,[[-9,-6],[4,-7],[10,-1],[5,6],[-8,6]],'#b78d49',ink,.8);
        poly(c,[[-10,-5],[-5,-5],[-4,5],[-11,6]],p.base,ink,.6);
        line(c,-1,-5,3,4,'#efce82',1.4);line(c,4,-5,10,-11,skin,2.7);line(c,3,4,13,9,skin,2.7);
        ellipse(c,13,0,4.2,3.6,skin,ink,.6);
        poly(c,[[9,-4],[14,-5],[18,-1],[16,3],[13,2],[12,0],[9,1]],'#c29a55',ink,.7);
        poly(c,[[10,-5],[13,-9],[18,-8],[21,-3],[18,-2],[15,-5]],p.base,ink,.65);line(c,13,-7,17,-6,p.light,1.1);
        // Discarded round shield and a complete long spear remain readable
        // after the man falls; neither leaves a permanent battlefield mark.
        line(c,-40,-12,38,-15,'#a18b5d',1.7);line(c,-40,-12,-44,-12,'#c39b55',2);
        poly(c,[[44,-15],[36,-18],[36,-12]],'#dcdcc2',ink,.5);
        ellipse(c,-2,13,10.5,8.5,'#b78d49',ink,1.1);ellipse(c,-2,13,8.7,6.9,'#c6a261','#efce82',1.1);ellipse(c,-2,13,2.8,2.3,'#e3c17b','#735532',.7);
        poly(c,[[-7,16],[-5,10],[-3,16]],null,p.dark,1.1);
      }else{
        const heavy=type==='guardian'||type==='halberdier',bow=type==='archer',sling=type==='slinger';
        ellipse(c,0,1,18,8,'rgba(24,30,22,.18)');
        const cloth=type==='berserker'?skin:heavy?'#8b9b92':bow?p.dark:sling?'#bfb297':type==='militia'?'#887957':p.base;
        line(c,-5,-2,-14,-5,'#4a5144',3.5);line(c,-6,3,-15,7,'#414a40',3.5);line(c,-15,7,-18,6,ink,2);
        poly(c,[[-8,-5],[4,-6],[9,-1],[5,6],[-6,5]],cloth,ink,.8);
        line(c,4,-4,9,-10,skin,2.6);line(c,4,4,12,8,skin,2.6);
        ellipse(c,12,0,4.1,3.5,skin,ink,.6);
        if(type==='berserker')ellipse(c,13,-1,3.7,2.2,'#ad713e');
        else if(bow)poly(c,[[9,-4],[15,-4],[17,0],[11,3]],p.dark,ink,.6);
        else ellipse(c,13,-2,4.2,2.7,sling?'#d7c8a4':type==='javelin'?'#b6a16d':steel,ink,.6);
        line(c,-3,-5,-3,5,p.trim,1.2);
        if(bow){c.strokeStyle='#b48e57';c.lineWidth=1.5;c.beginPath();c.moveTo(-17,-12);c.quadraticCurveTo(0,-23,16,-12);c.stroke();line(c,-17,-12,16,-12,'#d3c49c',.6);line(c,-10,10,17,11,'#c8b684',.9);}
        else if(type==='crossbow'){line(c,-10,12,13,11,'#927345',2.5);line(c,6,5,9,19,steel,1.5);line(c,6,5,-1,12,'#cbbb96',.6);line(c,-1,12,9,19,'#cbbb96',.6);}
        else if(type==='pikeman'||type==='militia'||type==='halberdier'||type==='javelin'){
          const length=type==='pikeman'?41:type==='halberdier'?32:27;line(c,-length,-10,length*.8,-12,'#a18b5d',1.5);poly(c,[[length*.8+5,-12],[length*.8-2,-15],[length*.8-2,-10]],'#c4ccbc');
          if(type==='halberdier')poly(c,[[length*.7,-13],[length*.7-4,-21],[length*.7-11,-19],[length*.7-8,-12]],steel,ink,.7);
        }else if(type==='berserker'){
          line(c,-19,17,16,10,'#8a603b',3);line(c,6,12,16,10,'#986f42',7);line(c,10,11,12,10.6,'#5b6058',8);
        }else if(type==='musketeer'){
          line(c,-21,15,17,10,'#6c4e35',3);line(c,-7,13,23,9,'#929f98',2);
          ellipse(c,17,-5,5,3,'#3d4542',ink,.7);
        }
        else if(sling){line(c,-8,11,6,16,'#b6a274',.8);ellipse(c,7,16,3,2,'#7d653e');ellipse(c,-11,11,3,3,'#9a8050');}
        else line(c,-13,13,12,17,steel,2);
        if(type==='guardian')poly(c,[[-8,8],[5,8],[8,15],[0,20],[-9,15]],p.dark,p.trim,1);
        else if(type==='swordsman')poly(c,[[-8,8],[3,7],[6,13],[-2,18],[-9,13]],p.base,p.trim,1);
        else if(type==='militia'||type==='javelin')ellipse(c,-1,13,6,4.5,p.base,'#bca66e',1.1);
      }
      c.restore();
    }

    _drawActionSprite(c,type,team,dir,kind,phase) {
      const cycle=phase*Math.PI/4*(kind==='backstep'?-1:1),face=Math.cos(dir*Math.PI/4)<-.2?-1:1;
      if(kind==='march'||kind==='gallop'||kind==='brace'||kind==='backstep'||kind==='sidestep'){
        const ready=kind==='brace'||kind==='gallop'&&(type==='lancer'||type==='cavalry');
        c.save();
        if(kind==='gallop')c.translate(0,-Math.abs(Math.sin(cycle))*1.6);
        // Yielding reverses or shortens the feet, not the soldier's facing.
        // The upper body keeps its ordinary guard until a real strike takes over.
        this._drawSprite(c,type,team,dir,ready?(type==='hoplite'?7:6):0,false,cycle,kind);
        c.restore();return;
      }
      if(kind==='collapse'){
        c.save();c.translate(face*(3+phase*3),0);c.rotate(face*(.30+phase*.34));c.scale(1,1-phase*.12);
        this._drawSprite(c,type,team,dir,0,true);c.restore();return;
      }
      // These reactions are dispatched only from a real, sampled hit event.
      // A guard presents its shield; an unguarded hit recoils the upper body.
      c.save();
      if(kind==='block'){c.translate(face*(phase?1:3),1);this._drawSprite(c,type,team,dir,type==='hoplite'?7:8,false);}
      else{c.translate(-face*(phase?1.5:4),1);c.rotate(-face*(phase?.035:.085));this._drawSprite(c,type,team,dir,0,false);}
      c.restore();
    }

    _actionSprite(u,atlas,dir,kind,phase) {
      if(!atlas.cw||!atlas.ch)return null;
      if(!atlas.actions)atlas.actions=new Map();
      const direction=spriteFacing(u.type,dir);
      const actionId=kind==='march'?0:kind==='gallop'?1:kind==='brace'?2:kind==='collapse'?3:kind==='block'?4:kind==='backstep'?6:kind==='sidestep'?7:5;
      const key=actionId*64+phase*8+direction;
      if(atlas.actions.has(key))return atlas.actions.get(key);
      if(!(this._poseBuildBudget>0))return null;
      this._poseBuildBudget--;
      const scale=atlas.scale||2,cw=atlas.cw+(kind==='collapse'?96:48),ch=atlas.ch+48,anchorY=ch*.72;
      const scratch=newCanvas(cw*scale,ch*scale),ctx=scratch.getContext('2d',{willReadFrequently:true});
      ctx.scale(scale,scale);ctx.translate(cw/2,anchorY);ctx.lineCap='round';ctx.lineJoin='round';
      this._drawActionSprite(ctx,u.type,u.team||0,direction,kind,phase);
      const sprite=this._cropSprite(scratch,ctx,cw,anchorY,scale);scratch.width=1;scratch.height=1;
      atlas.actions.set(key,sprite);if(atlas.unique)atlas.unique.set('action:'+key,sprite);return sprite;
    }

    _collectHitPoses() {
      if(!this._hitPoses)this._hitPoses=new Map();else this._hitPoses.clear();
      if(this.options&&this.options.animations==='reduced')return;
      for(const effect of this.engine.effects||[]){
        if(effect.kind!=='hit'||!Number.isInteger(effect.unitId)||effect.age<0||effect.age>.25)continue;
        this._hitPoses.set(effect.unitId,{kind:effect.blocked?'block':'recoil',phase:effect.age>.12?1:0});
      }
    }

    _motionAction(u,frame) {
      if(this.options&&this.options.animations==='reduced'||u.state==='turning'||u.state==='fallBack'||u.routed)return null;
      const e=this.engine,r=e.regiments&&e.regiments[u.regiment];
      if(r&&(r.order==='face'||r.order==='retreat'||r.fallingBack))return null;
      if(frame>=4)return null;
      const hit=this._hitPoses&&this._hitPoses.get(u.id);
      const moving=(u.state==='move'||u.state==='rout')&&((u._vx||0)*(u._vx||0)+(u._vy||0)*(u._vy||0)>1);
      let dx=u._pressureDX||0,dy=u._pressureDY||0;
      const yielding=r&&u._pressureTick===e._tick&&r._formationPressureUntil>e.time&&r._formationPressureSpeed>0&&dx*dx+dy*dy>.000001&&
        r.order!=='move'&&!(r.order==='attack'&&r._hasDestination&&!r._attackBlocked)&&!(r._rallyTime>0)&&u._impactUntil<=e.time;
      const fx=u._friendlyDX||0,fy=u._friendlyDY||0;
      const courtesy=r&&u._friendlyTick===e._tick&&u._friendlyDistance>0&&fx*fx+fy*fy>.000001&&u._impactUntil<=e.time&&
        (!(u._vx*u._vx+u._vy*u._vy>1)||Math.hypot(fx,fy)>Math.hypot(u._vx,u._vy)*(e.impacts?e.impacts.dt:1/30)*.2);
      const shuffling=yielding||courtesy;
      if(hit&&!moving&&!shuffling)return hit;
      if(!moving&&!shuffling)return null;
      const type=global.GameData&&global.GameData.TYPES[u.type];
      const stride=u.type==='elephant'?48:type&&type.mounted?shuffling?20:34:shuffling?12:22;
      const phase=Math.floor(((Number(u.travelDistance)||0)/stride*8+u.id*.37)%8);
      const action=this._motionState||(this._motionState={kind:'march',phase:0});action.phase=phase;
      if(shuffling){
        if(!yielding){dx=fx;dy=fy;}
        const angle=Number.isFinite(u.angle)?u.angle:0,nx=Math.cos(angle),ny=Math.sin(angle),forward=dx*nx+dy*ny,side=dy*nx-dx*ny;
        action.kind=Math.abs(side)>Math.abs(forward)?'sidestep':forward<0?'backstep':'march';
        if(action.kind==='sidestep'&&side<0)action.phase=(8-phase)%8;
        return action;
      }
      action.kind=r&&r._steadyBraceScale>0&&r.order==='auto'&&r._tactic&&r._tactic.movingBrace&&(!this.engine._steadyMotion||this.engine._steadyMotion(u,r))?'brace':type&&type.mounted&&(u._mountSpeed||0)>type.speed*.65&&r&&(r.order==='charge'||r._tactic&&r._tactic.charge||u._plungeUntil>this.engine.time)?'gallop':'march';
      return action;
    }

    _resetCorpseCache() {
      const cache=this._corpseCache;
      if(cache)for(const tile of cache.tiles.values())if(tile.canvas){tile.canvas.width=1;tile.canvas.height=1;}
      this._corpseCache=null;this._corpseStats=null;
    }

    _bodyMode() {const mode=this.options&&this.options.corpseMode;return mode==='long'||mode==='battle'?mode:'brief';}

    _syncCorpseCache() {
      const e=this.engine,mode=this._bodyMode();let cache=this._corpseCache;
      if(!cache||cache.engine!==e||cache.mode!==mode||e.time<cache.time){
        this._resetCorpseCache();
        cache=this._corpseCache={engine:e,mode,time:e.time,cursor:0,seen:new Set(),pending:[],tiles:new Map(),pixels:0,settled:0,frame:0,scanAt:-Infinity,scanCount:0};
      }
      cache.time=e.time;cache.frame++;
      const ids=e.deadUnitIds;
      if(Array.isArray(ids)){
        const stop=Math.min(ids.length,cache.cursor+1200);
        while(cache.cursor<stop){const id=ids[cache.cursor++];if(!cache.seen.has(id)){cache.seen.add(id);cache.pending.push(id);}}
      }else if(e.time-cache.scanAt>=.2||cache.scanAt<0){
        // Compatibility for imported engines and small isolated renderer tests.
        // Normal battles append to deadUnitIds and never rescan the whole army.
        cache.scanAt=e.time;cache.scanCount++;
        for(const u of e.units||[])if(!u.alive&&!u.routed&&u._deathTime>=0&&!cache.seen.has(u.id)){cache.seen.add(u.id);cache.pending.push(u.id);}
      }
      let keep=0,accepted=0;
      for(let i=0;i<cache.pending.length;i++){
        const id=cache.pending[i],u=e.units[id];if(!u||u.alive||u.routed)continue;
        if(!this._corpseOpacity(u))continue;
        const settle=Math.max(u._deathTime,u._impactUntil||0,(u._impactStart||0)+(u._impactDuration||0))+.34;
        if(e.time<settle||accepted>=1200){cache.pending[keep++]=id;continue;}
        const col=Math.floor(u.x/CORPSE_TILE),row=Math.floor(u.y/CORPSE_TILE),key=col+':'+row;
        let tile=cache.tiles.get(key);
        if(!tile){tile={key,x:col*CORPSE_TILE,y:row*CORPSE_TILE,entries:[],piles:new Map(),canvas:null,scale:0,baked:0,lastUsed:0,refreshAt:0,nextRefresh:Infinity,dirty:false,visibleCount:0};cache.tiles.set(key,tile);}
        const cell=Math.floor((u.x-tile.x)/26)+':'+Math.floor((u.y-tile.y)/26),count=tile.piles.get(cell)||0;
        tile.piles.set(cell,count+1);
        const layer=Math.min(8,Math.floor(count/3)),dir=((Math.round((u.angle||0)/(Math.PI/4))+u.id%3-1)%8+8)%8;
        tile.entries.push({id,x:u.x,y:u.y,elevation:this._elevation(u),layer,dir});
        cache.settled++;accepted++;
      }
      cache.pending.length=keep;return cache;
    }

    _corpseTileCanvas(cache,tile,scale) {
      if(tile.canvas&&tile.scale===scale)return;
      if(tile.canvas){cache.pixels-=tile.canvas.width*tile.canvas.height;tile.canvas.width=1;tile.canvas.height=1;}
      const edge=Math.ceil((CORPSE_TILE+CORPSE_PAD*2)*scale);
      tile.canvas=newCanvas(edge,edge);tile.scale=scale;tile.baked=0;tile.dirty=false;tile.refreshAt=0;tile.nextRefresh=Infinity;tile.visibleCount=0;
      cache.pixels+=edge*edge;
      // Only raster tiles consume the budget. All original casualty records
      // survive eviction and are reconstructed when the camera returns.
      if(cache.pixels>CORPSE_CACHE_PIXELS){
        const candidates=Array.from(cache.tiles.values()).filter(other=>other!==tile&&other.canvas&&other.lastUsed!==cache.frame).sort((a,b)=>a.lastUsed-b.lastUsed);
        for(const other of candidates){
          cache.pixels-=other.canvas.width*other.canvas.height;other.canvas.width=1;other.canvas.height=1;other.canvas=null;other.baked=0;other.visibleCount=0;
          if(cache.pixels<=CORPSE_CACHE_PIXELS)break;
        }
      }
    }

    _bakeCorpseTile(cache,tile,scale,budget) {
      this._corpseTileCanvas(cache,tile,scale);
      const c=tile.canvas.getContext('2d'),long=cache.mode==='long';
      if(tile.dirty||(long&&this.engine.time>=tile.refreshAt)){
        c.setTransform(1,0,0,1,0,0);c.clearRect(0,0,tile.canvas.width,tile.canvas.height);tile.baked=0;tile.dirty=false;tile.visibleCount=0;tile.nextRefresh=Infinity;tile.refreshAt=Infinity;
      }
      c.setTransform(scale,0,0,scale,(CORPSE_PAD-tile.x)*scale,(CORPSE_PAD-tile.y)*scale);
      const level=scale<.3?'low':scale<.8?'mid':'full',before=tile.baked;let count=0;
      while(tile.baked<tile.entries.length&&budget>0){
        const body=tile.entries[tile.baked],u=this.engine.units[body.id],opacity=u?this._corpseOpacity(u):0;
        if(opacity>0){
          const sprite=this._corpseSprite(u.type,u.team||0,body.dir);
          if(!sprite)break;
          c.globalAlpha=opacity;
          // Later casualties occupy progressively raised layers in their
          // local pile; no displacement is applied to simulation bodies.
          c.drawImage(sprite[level],body.x+sprite.ox,body.y-body.elevation-body.layer*1.35+sprite.oy,sprite.w,sprite.h);
          count++;tile.visibleCount++;
          if(long)tile.nextRefresh=Math.min(tile.nextRefresh,u._deathTime+LONG_BODY_FADE_START>this.engine.time?u._deathTime+LONG_BODY_FADE_START:this.engine.time+2);
        }
        tile.baked++;budget--;
      }
      c.globalAlpha=1;
      if(tile.baked===tile.entries.length&&(tile.baked!==before||!tile.refreshAt))tile.refreshAt=long?tile.nextRefresh:Infinity;
      return {budget,count};
    }

    _drawAccumulatedBodies(c,bounds,low,spriteLevel) {
      c.globalAlpha=1;
      const cache=this._syncCorpseCache(),pixelZoom=this.camera?this.camera.zoom*(this.dpr||1):spriteLevel==='full'?1:spriteLevel==='low'?.15:.5;
      let scale=pixelZoom<.10?.125:pixelZoom<.25?.25:pixelZoom<.70?.5:1;
      let budget=1200,draws=0,bodies=0,tiles=0;
      const visible=[];
      for(const tile of cache.tiles.values()){
        if(tile.x+CORPSE_TILE+CORPSE_PAD<bounds.x1||tile.x-CORPSE_PAD>bounds.x2||tile.y+CORPSE_TILE+CORPSE_PAD<bounds.y1||tile.y-CORPSE_PAD>bounds.y2)continue;
        tile.lastUsed=cache.frame;visible.push(tile);
      }
      // Lower chunks first; layers within a chunk keep death order. Padding
      // preserves long fallen pikes and elephants across a chunk boundary.
      visible.sort((a,b)=>a.y-b.y||a.x-b.x);
      while(visible.length*Math.ceil((CORPSE_TILE+CORPSE_PAD*2)*scale)**2>CORPSE_CACHE_PIXELS&&scale>.0625)scale*=.5;
      for(const tile of visible)if(tile.canvas&&tile.scale!==scale){cache.pixels-=tile.canvas.width*tile.canvas.height;tile.canvas.width=1;tile.canvas.height=1;tile.canvas=null;tile.baked=0;tile.visibleCount=0;}
      for(const tile of visible){
        if(budget>0){const result=this._bakeCorpseTile(cache,tile,scale,budget);budget=result.budget;}
        if(tile.canvas){c.drawImage(tile.canvas,tile.x-CORPSE_PAD,tile.y-CORPSE_PAD,CORPSE_TILE+CORPSE_PAD*2,CORPSE_TILE+CORPSE_PAD*2);draws++;tiles++;}
        bodies+=tile.visibleCount;
      }
      const pending=this._visibleBodies;pending.length=0;
      for(const id of cache.pending){
        const u=this.engine.units[id];if(u&&this._corpseOpacity(u)&&u.x>=bounds.x1&&u.x<=bounds.x2&&u.y>=bounds.y1&&u.y<=bounds.y2)pending.push(u);
      }
      pending.sort((a,b)=>a.y-b.y||a.id-b.id);
      const limit=low?320:900;
      for(let i=0;i<Math.min(limit,pending.length);i++){
        const u=pending[i],dir=((Math.round((u.angle||0)/(Math.PI/4))%8)+8)%8;
        if(this._drawFallenUnit(c,u,dir,spriteLevel)){draws++;bodies++;}
      }
      c.globalAlpha=1;this._bodyDrawCalls=draws;
      this._corpseStats={mode:cache.mode,registered:cache.seen.size,settled:cache.settled,pending:cache.pending.length,tiles:cache.tiles.size,drawnTiles:tiles,drawCalls:draws,canvasPixels:cache.pixels,fullArmyScans:cache.scanCount};
      return bodies;
    }

    _drawFallenUnit(c,u,dir,spriteLevel) {
      const elevation=this._elevation(u),atlas=this._unitAtlases[u.id]||this._getAtlas(u.type,u.team||0);
      if(this._impactPose(u)){c.globalAlpha=1;return this._drawImpactUnit(c,u,atlas,dir,spriteLevel,elevation);}
      const landed=u._impactLift>0?Math.max(u._deathTime,(u._impactStart||0)+(u._impactDuration||0)):u._deathTime;
      const settle=clamp((this.engine.time-landed)/.32,0,1);
      const pixelZoom=this.camera?this.camera.zoom*(this.dpr||1):spriteLevel==='low'?.15:1;
      c.globalAlpha=this._corpseOpacity(u);
      if(settle<.75&&pixelZoom>=EXTRA_POSE_PIXEL_ZOOM&&!(global.GameData&&global.GameData.TYPES[u.type]&&global.GameData.TYPES[u.type].equipment)&&!(this.options&&this.options.animations==='reduced')){
        const falling=this._actionSprite(u,atlas,dir,'collapse',Math.min(2,Math.floor(settle*4)));
        if(falling){c.drawImage(falling[spriteLevel],u.x+falling.ox,u.y-elevation+falling.oy,falling.w,falling.h);return true;}
      }
      const sprite=this._corpseSprite(u.type,u.team||0,dir);if(!sprite)return false;
      if(settle<1){c.save();c.translate(u.x,u.y-elevation);c.scale(.7+settle*.3,.8+settle*.2);c.drawImage(sprite[spriteLevel],sprite.ox,sprite.oy,sprite.w,sprite.h);c.restore();}
      else c.drawImage(sprite[spriteLevel],u.x+sprite.ox,u.y-elevation+sprite.oy,sprite.w,sprite.h);
      return true;
    }

    _attackFrame(u) {
      if(!u.alive||u.routed||u.state==='turning'||u.state==='fallBack')return 0;
      const e=this.engine,t=global.GameData&&global.GameData.TYPES[u.type];
      if(!t)return 0;
      const age=e.time-u._lastAttack,travel=u.state==='move'&&Math.hypot(u._vx||0,u._vy||0)>1,hoplite=u.type==='hoplite';
      const release=t.siege?.22:hoplite?.16:.12,follow=t.siege?.50:hoplite?.34:t.ranged?.29:.24;
      if(age>=0&&age<release)return 6;
      if(age>=release&&age<follow)return 7;
      // A shield-forward pose represents a successful physical shove, never
      // inferred proximity. Explicit travel and turning retain precedence.
      if(hoplite&&!travel&&Number.isFinite(u._lastPush)&&e.time-u._lastPush>=0&&e.time-u._lastPush<.30)return 7;
      // An active march retains its distance-based gait after the brief strike.
      // Stationary shooters visibly retrieve, crank, and nock their next round.
      if(travel)return 0;
      const longReload=t.siege||u.type==='crossbow'||u.type==='musketeer';
      const reload=longReload?t.cooldown*.50:u.type==='javelin'?.64:hoplite?.52:t.ranged?.63:.32;
      const ready=longReload?t.cooldown*.78:u.type==='javelin'?.94:hoplite?.72:t.ranged?.90:.42;
      if(age>=follow&&age<reload)return 8;
      if(age>=reload&&age<ready)return 9;
      const target=e.units&&e.units[u.target],reach=t.range*(u._terrain&&u._terrain.elevation?1.12:1)+(t.ranged?0:target&&target._radius||0);
      const wind=t.siege?.55:u.type==='archer'||u.type==='horseArcher'?.42:u.type==='javelin'?.30:u.type==='crossbow'?.34:hoplite?.32:.24;
      if((u._siegeStage==='battering'||u._siegeStage==='bombarding')&&u._cooldown>0&&u._cooldown<=wind)return u._cooldown>wind*.48?4:5;
      if(target&&target.alive&&!target.routed&&Number.isFinite(u._cooldown)&&u._cooldown>0&&u._cooldown<=wind&&Math.hypot(target.x-u.x,target.y-u.y)<=reach){
        return u._cooldown>wind*.48?4:5;
      }
      return 0;
    }

    _corpseOpacity(u) {
      if(u.alive||u.routed||!Number.isFinite(u._deathTime)||u._deathTime<0)return 0;
      const age=this.engine.time-u._deathTime,mode=this._bodyMode();
      if(age<0)return 0;
      if(mode==='battle')return .86;
      const lifetime=mode==='long'?LONG_BODY_LIFETIME:BODY_LIFETIME,fade=mode==='long'?LONG_BODY_FADE_START:BODY_FADE_START;
      if(age>=lifetime)return 0;
      return .86*(age<=fade?1:(lifetime-age)/(lifetime-fade));
    }

    _impactPose(u) {
      if(!(u._impactLift>0)||!(u._impactDuration>0)||!Number.isFinite(u._impactStart))return null;
      const age=this.engine.time-u._impactStart;
      if(age<0||age>=u._impactDuration)return null;
      const progress=age/u._impactDuration,arc=Math.sin(progress*Math.PI);
      return {progress,lift:arc*u._impactLift,tilt:arc*(u._impactNX<0?-1:1)*(.6+(u.id%3)*.12)};
    }

    _airborneSprite(u,atlas,dir) {
      if(!atlas.airborne)atlas.airborne=new Array(8);
      if(atlas.airborne[dir])return atlas.airborne[dir];
      if(this._bodyBuildBudget<=0)return null;
      this._bodyBuildBudget--;
      const {cw,ch,scale}=atlas,scratch=newCanvas(cw*scale,ch*scale),ctx=scratch.getContext('2d',{willReadFrequently:true});
      ctx.scale(scale,scale);ctx.translate(cw/2,atlas.anchorY);ctx.lineCap='round';ctx.lineJoin='round';
      this._drawSprite(ctx,u.type,u.team||0,dir,0,true);
      const sprite=this._cropSprite(scratch,ctx,cw,atlas.anchorY,scale);scratch.width=1;scratch.height=1;
      atlas.airborne[dir]=sprite;atlas.unique.set('airborne:'+dir,sprite);return sprite;
    }

    _drawImpactUnit(c,u,atlas,dir,spriteLevel,elevation) {
      const pose=this._impactPose(u);if(!pose)return false;
      const sprite=this._airborneSprite(u,atlas,dir),fallback=sprite||atlas.frames[dir],source=fallback[spriteLevel];
      c.save();c.globalAlpha*=.55+pose.progress*.2;
      ellipse(c,u.x,u.y-(elevation||0),8+pose.lift*.07,3+pose.lift*.025,'rgba(17,25,18,.4)');c.restore();
      c.save();c.translate(u.x,u.y-(elevation||0)-pose.lift);c.rotate(pose.tilt);
      if(sprite)c.drawImage(source,sprite.ox,sprite.oy,sprite.w,sprite.h);
      else{
        // A bounded cache build never leaves a second shadow floating in the
        // air: the temporary source excludes the idle sprite's ground strip.
        const h=Math.max(1,Math.min(fallback.h,-3-fallback.oy));
        c.drawImage(source,0,0,source.width,source.height*h/fallback.h,fallback.ox,fallback.oy,fallback.w,h);
      }
      c.restore();return true;
    }

    _elevation(u) {
      const siege=this.engine.siege;
      return siege&&typeof siege.heightAt==='function'?siege.heightAt(u.x,u.y):0;
    }

    _drawBodies(c,bounds,low,spriteLevel) {
      if(this._bodyMode()!=='brief')return this._drawAccumulatedBodies(c,bounds,low,spriteLevel);
      if(this._corpseCache)this._resetCorpseCache();
      const bodies=this._visibleBodies,buckets=this._bodyBuckets,limit=low?320:900;
      bodies.length=0;for(const bucket of buckets)bucket.length=0;
      for(const u of this.engine.units||[]){
        if(!this._corpseOpacity(u)||u.x<bounds.x1||u.x>bounds.x2||u.y<bounds.y1||u.y>bounds.y2)continue;
        const age=this.engine.time-u._deathTime;buckets[Math.min(5,Math.floor(age/4))].push(u);
      }
      // Recent falls get priority. Sampling within a bucket distributes a
      // bounded number of sprites across formations instead of drawing one team.
      for(const bucket of buckets){
        const count=Math.min(bucket.length,limit-bodies.length);
        for(let i=0;i<count;i++)bodies.push(bucket[Math.floor(i*bucket.length/count)]);
        if(bodies.length>=limit)break;
      }
      bodies.sort((a,b)=>a.y-b.y||a.id-b.id);
      let drawn=0;
      for(const u of bodies){
        const angle=Number.isFinite(u.angle)?u.angle:0,dir=((Math.round(angle/(Math.PI/4))%8)+8)%8;
        if(this._drawFallenUnit(c,u,dir,spriteLevel))drawn++;
      }
      c.globalAlpha=1;this._bodyDrawCalls=drawn;return drawn;
    }

    _drawFortifications(c,bounds,z) {
      const structures=this.engine.structures||[];
      for(const s of structures){
        const gate=s.type==='gate',tower=s.castleTower===true,height=tower?68:gate?29:38;
        if(s.x+s.w+10<bounds.x1||s.x-8>bounds.x2||s.y+s.h+10<bounds.y1||s.y-height-32>bounds.y2)continue;
        if(s.breached){
          c.fillStyle='rgba(57,51,36,.22)';c.fillRect(s.x-8,s.y-5,s.w+16,s.h+10);
          for(let i=0;i<18;i++){
            const x=s.x+((i*73+17)%101)/101*s.w,y=s.y+((i*47+11)%97)/97*s.h,r=6+(i%4)*3;
            poly(c,[[x-r,y],[x-r*.45,y-r*.6],[x+r*.7,y-r*.5],[x+r,y+r*.5],[x,y+r]],s.type==='gate'?'#8c704c':'#96917b','#5d6252',1);
          }
          continue;
        }
        if(gate&&s.open){
          // Keep the passable threshold flush with the ground. The retracted
          // timber leaves sit at the ends of the gate, next to the masonry.
          const vertical=s.h>s.w;
          c.fillStyle='rgba(196,170,113,.30)';c.fillRect(s.x,s.y,s.w,s.h);
          for(const end of [0,1]){
            const width=vertical?s.w:8,depth=vertical?8:s.h;
            const x=s.x+end*(s.w-width),y=s.y+end*(s.h-depth);
            c.fillStyle='#795e40';c.fillRect(x,y-6,width,depth);
            c.strokeStyle='#514a36';c.lineWidth=1.5;c.strokeRect(x,y-6,width,depth);
          }
          continue;
        }
        let pieces=[{x:s.x,y:s.y,w:s.w,h:s.h}];
        for(const gap of s.passages||[]){
          if(![gap.x,gap.y,gap.w,gap.h].every(Number.isFinite))continue;
          const next=[];
          for(const r of pieces){
            const x1=Math.max(r.x,gap.x),x2=Math.min(r.x+r.w,gap.x+gap.w),y1=Math.max(r.y,gap.y),y2=Math.min(r.y+r.h,gap.y+gap.h);
            if(x1>=x2||y1>=y2){next.push(r);continue;}
            if(y1>r.y)next.push({x:r.x,y:r.y,w:r.w,h:y1-r.y});
            if(y2<r.y+r.h)next.push({x:r.x,y:y2,w:r.w,h:r.y+r.h-y2});
            if(x1>r.x)next.push({x:r.x,y:y1,w:x1-r.x,h:y2-y1});
            if(x2<r.x+r.w)next.push({x:x2,y:y1,w:r.x+r.w-x2,h:y2-y1});
          }
          pieces=next;
        }
        const p=TEAM[s.team===0?0:1];
        for(const r of pieces){
          c.fillStyle='rgba(28,35,24,.25)';c.fillRect(r.x+9,r.y+9,r.w+12,r.h+9);
          poly(c,[[r.x,r.y+r.h-height],[r.x+r.w,r.y+r.h-height],[r.x+r.w,r.y+r.h],[r.x,r.y+r.h]],gate?'#655139':'#777968','#454b3d',2);
          poly(c,[[r.x+r.w,r.y-height],[r.x+r.w+8,r.y-height+8],[r.x+r.w+8,r.y+r.h],[r.x+r.w,r.y+r.h-height]],gate?'#544a33':'#626955','#414b3b',1.5);
          c.fillStyle=gate?'#9a7950':'#a7a58d';c.fillRect(r.x,r.y-height,r.w,r.h);
          c.strokeStyle=gate?'#5b4934':'#686f5a';c.lineWidth=1.6;c.strokeRect(r.x,r.y-height,r.w,r.h);
          const vertical=r.h>r.w,span=vertical?r.h:r.w,step=gate?18:40;
          for(let n=step;n<span;n+=step){
            if(vertical)line(c,r.x,r.y-height+n,r.x+r.w,r.y-height+n,gate?'#c0a16d':'#7d826c',1.4);
            else line(c,r.x+n,r.y-height,r.x+n,r.y+r.h-height,gate?'#c0a16d':'#7d826c',1.4);
          }
          if(gate){
            if(vertical){line(c,r.x+r.w*.28,r.y-height+4,r.x+r.w*.28,r.y+r.h-height-4,'#465045',5);line(c,r.x+r.w*.72,r.y-height+4,r.x+r.w*.72,r.y+r.h-height-4,'#465045',5);}
            else{line(c,r.x+4,r.y+r.h*.28-height,r.x+r.w-4,r.y+r.h*.28-height,'#465045',5);line(c,r.x+4,r.y+r.h*.72-height,r.x+r.w-4,r.y+r.h*.72-height,'#465045',5);}
          }else if(tower){
            const lip=Math.min(12,r.w/6,r.h/6),y=r.y-height;
            c.fillStyle='#747562';c.fillRect(r.x+lip,y+lip,r.w-2*lip,r.h-2*lip);
            c.strokeStyle='#5c604e';c.strokeRect(r.x+lip,y+lip,r.w-2*lip,r.h-2*lip);
            c.fillStyle='#c8c1a2';
            for(let n=0;n<r.w;n+=34){
              const width=Math.min(18,r.w-n);
              c.fillRect(r.x+n,y,width,lip);c.fillRect(r.x+n,y+r.h-lip,width,lip);
            }
            for(let n=20;n<r.h-lip;n+=34){
              const depth=Math.min(18,r.h-lip-n);
              c.fillRect(r.x,y+n,lip,depth);c.fillRect(r.x+r.w-lip,y+n,lip,depth);
            }
            c.fillStyle='#393e32';
            for(const fraction of [.24,.76])c.fillRect(r.x+r.w*fraction,r.y+r.h-height*.64,4,height*.22);
          }else{
            for(let n=0;n<span;n+=54){
              const x=vertical?r.x:r.x+n,y=vertical?r.y+n-height:r.y-height;
              c.fillStyle='#c1b99b';c.fillRect(x,y,vertical?12:Math.min(25,span-n),vertical?Math.min(25,span-n):12);
              c.strokeStyle='#6e775f';c.strokeRect(x,y,vertical?12:Math.min(25,span-n),vertical?Math.min(25,span-n):12);
            }
          }
          if(s.hp/s.maxHp<.7){
            const mx=r.x+r.w*.47,my=r.y+r.h*.5-height;
            line(c,mx-10,my-18,mx+5,my-3,'#4b5143',2.7);line(c,mx+5,my-3,mx-3,my+9,'#4b5143',2.7);line(c,mx-3,my+9,mx+14,my+21,'#4b5143',2.7);
          }
        }
        const mx=s.x+s.w/2,my=s.y+s.h/2-height;
        if(pieces.length&&z>.17){
          line(c,mx,my,mx,my-31,'#d5bf85',2);poly(c,[[mx,my-31],[mx+21,my-28],[mx+17,my-15],[mx,my-18]],p.base,p.trim,1);
          if(s.hp<s.maxHp){const width=Math.min(100,Math.max(48,70/z*.4));c.fillStyle='#273b31';c.fillRect(mx-width/2,my+7,width,6);c.fillStyle=s.hp/s.maxHp>.35?p.light:'#d98c62';c.fillRect(mx-width/2,my+7,width*clamp(s.hp/s.maxHp,0,1),6);}
        }
      }
    }

    _drawSiegeRamps(c,bounds,z) {
      const siege=this.engine.siege;if(!siege)return;
      for(const ramp of siege.ramps||[]){
        if(ramp.x+ramp.w<bounds.x1||ramp.x>bounds.x2||ramp.y+ramp.h<bounds.y1||ramp.y-50>bounds.y2)continue;
        const alongX=ramp.axis==='x',length=alongX?ramp.w:ramp.h,steps=Math.max(4,Math.min(28,Math.ceil(length/16))),p=TEAM[ramp.team===1?1:0];
        const point=(a,b)=>{const x=ramp.x+(alongX?a*length:b*ramp.w),y=ramp.y+(alongX?b*ramp.h:a*length);return [x,y-(typeof siege.heightAt==='function'?siege.heightAt(x,y):Math.sin(a*Math.PI)*(ramp.height||38))];};
        c.fillStyle='rgba(24,31,21,.3)';c.fillRect(ramp.x+7,ramp.y+6,ramp.w,ramp.h);
        for(let n=0;n<steps;n++){
          const a=n/steps,b=(n+1)/steps;
          poly(c,[point(a,0),point(b,0),point(b,1),point(a,1)],n%2?'#ac8a55':'#c1a06a','#675539',1);
        }
        for(const side of [0,1])for(let n=0;n<=steps;n+=3){
          const q=point(n/steps,side);line(c,q[0],q[1],q[0],q[1]-11,'#6e5538',2.5);
          if(n+3<=steps){const next=point((n+3)/steps,side);line(c,q[0],q[1]-10,next[0],next[1]-10,p.trim,2);}
        }
        if(z>.6){const q=point(.5,.5);c.font='600 '+10/z+'px system-ui, sans-serif';c.textAlign='center';c.fillStyle='#fae5ac';c.fillText('ASSAULT RAMP',q[0],q[1]-18);}
      }
    }

    _drawDockedTowers(c,bounds,spriteLevel) {
      let count=0;
      for(const u of this._siegeTowers||this.engine.units||[]){
        if(!u.alive||u.type!=='siegeTower'||!u._siegeDeployed||u.x<bounds.x1||u.x>bounds.x2||u.y<bounds.y1||u.y>bounds.y2)continue;
        const atlas=this._unitAtlases[u.id]||this._getAtlas(u.type,u.team||0),dir=Math.cos(u.angle)<-.2?4:0;
        const sprite=atlas.frames[this._attackFrame(u)*8+dir];
        c.drawImage(sprite[spriteLevel],u.x+sprite.ox,u.y+sprite.oy,sprite.w,sprite.h);count++;
      }
      return count;
    }

    _drawDeployingBridge(c,u) {
      if(u.type!=='siegeTower'||u._siegeStage!=='deploying')return;
      const p=clamp(u._siegeProgress||0,0,1),face=Math.cos(u.angle)<0?-1:1;
      const reach=12+p*32,hingeX=u.x+face*22,hingeY=u.y-38,endX=hingeX+face*reach,endY=hingeY-(1-p)*43;
      poly(c,[[hingeX,hingeY-9],[endX,endY-9],[endX,endY+9],[hingeX,hingeY+9]],'#c1a06a','#5b4c34',2);
      for(let n=1;n<6;n++){const a=n/6,x=hingeX+(endX-hingeX)*a,y=hingeY+(endY-hingeY)*a;line(c,x,y-9,x,y+9,'#78603d',1.2);}
      line(c,u.x+face*19,u.y-85,endX,endY-8,'#d8c594',1.2);
    }

    _sortVisibleRows(visible,bounds,pixelZoom) {
      // At overview scale, subpixel Y differences cannot change a visible
      // silhouette. Stable counting by screen row keeps depth in linear time
      // without comparison-sorting thousands of soldiers every frame.
      const rows=Math.max(1,Math.ceil((bounds.y2-bounds.y1)*pixelZoom)+1);
      if(this._depthCounts.length<rows)this._depthCounts=new Int32Array(rows+256);
      if(this._depthKeys.length<visible.length)this._depthKeys=new Uint32Array(Math.max(256,visible.length*2));
      const counts=this._depthCounts,keys=this._depthKeys,sorted=this._depthSorted;
      counts.fill(0,0,rows);sorted.length=visible.length;
      for(let i=0;i<visible.length;i++){
        const row=clamp(Math.floor((visible[i].y-bounds.y1)*pixelZoom),0,rows-1);
        keys[i]=row;counts[row]++;
      }
      let offset=0;
      for(let row=0;row<rows;row++){const count=counts[row];counts[row]=offset;offset+=count;}
      for(let i=0;i<visible.length;i++)sorted[counts[keys[i]]++]=visible[i];
      return sorted;
    }

    render(nowSeconds,selectedIds) {
      if(!this.engine||!this.ctx)return;
      const now=Number.isFinite(nowSeconds)?nowSeconds:0;
      if(this._lastFrame&&now>this._lastFrame){const instant=1/(now-this._lastFrame);this.fps=this.fps*.92+Math.min(240,instant)*.08;}
      this._lastFrame=now;this._now=now;this._frame++;
      const selected=selectedIds instanceof Set?selectedIds:new Set(selectedIds||[]),e=this.engine,c=this.ctx,z=this.camera.zoom;
      const offsetX=this.width/2-this.camera.x*z,offsetY=this.height/2-this.camera.y*z;
      c.setTransform(this.dpr,0,0,this.dpr,0,0);c.globalAlpha=1;c.fillStyle='#252d28';c.fillRect(0,0,this.width,this.height);
      c.setTransform(z*this.dpr,0,0,z*this.dpr,offsetX*this.dpr,offsetY*this.dpr);
      c.imageSmoothingEnabled=true;
      c.drawImage(this._terrain,0,0,e.width,e.height);
      const bounds={x1:this.camera.x-this.width/z/2-180,x2:this.camera.x+this.width/z/2+180,y1:this.camera.y-this.height/z/2-180,y2:this.camera.y+this.height/z/2+180};
      this._drawFortifications(c,bounds,z);
      this._drawSiegeRamps(c,bounds,z);
      if(e.phase==='deployment')this._drawDeployment(c,bounds,z);
      if(this.options.showOrders&&!this.options.cinematic)this._drawOrders(c,selected,z);
      this._drawWaterMotion(c,bounds,now,z);
      const units=e.units||[],visible=this._visible;visible.length=0;
      if(this.options.quality==='auto'&&now-this._qualityChangedAt>2){
        if(!this._adaptiveLow&&this.fps<38&&units.length>1800){this._adaptiveLow=true;this._qualityChangedAt=now;}
        else if(this._adaptiveLow&&(this.fps>48||units.length<1200)){this._adaptiveLow=false;this._qualityChangedAt=now;}
      }
      const low=this.options.quality==='low'||(this.options.quality==='auto'&&this._adaptiveLow);
      const pixelZoom=z*this.dpr,spriteLevel=pixelZoom<.38?'low':pixelZoom<1.05?'mid':'full';
      // Extra gait, guard and recoil detail is subpixel in a whole-world view.
      // Keep the base march/strike frames without a second lookup per soldier.
      const extraActions=pixelZoom>=EXTRA_POSE_PIXEL_ZOOM&&this.options.animations!=='reduced'&&(!low||z>=.25);
      let bodies=0,drawCalls=this._drawDockedTowers(c,bounds,spriteLevel);this._bodyBuildBudget=2;this._poseBuildBudget=2;
      if(extraActions)this._collectHitPoses();else if(this._hitPoses&&this._hitPoses.size)this._hitPoses.clear();
      if(this.options.showBodies){
        bodies=this._drawBodies(c,bounds,low,spriteLevel);drawCalls+=this._bodyDrawCalls||0;
        this._warmBodies();
      }
      for(let i=0;i<units.length;i++){const u=units[i];if((!u.alive&&!u.routed)||u.x<bounds.x1||u.x>bounds.x2||u.y<bounds.y1||u.y>bounds.y2)continue;visible.push(u);}
      // Keep exact depth at close zoom/high detail; overview ordering uses
      // reusable screen-row buckets. Neither path touches simulation order.
      const ordered=this.options.quality!=='high'&&pixelZoom<.8&&visible.length>1200?
        this._sortVisibleRows(visible,bounds,pixelZoom):visible.sort((a,b)=>a.y-b.y||a.id-b.id);
      for(let i=0;i<ordered.length;i++){
        const u=ordered[i],atlas=this._unitAtlases[u.id]||this._getAtlas(u.type,u.team||0),angle=Number.isFinite(u.angle)?u.angle:(u.team?Math.PI:0);
        if(u.type==='siegeTower'&&u._siegeDeployed)continue;
        const dir=((Math.round(angle/(Math.PI/4))%8)+8)%8;
        const elevation=this._elevation(u);
        if(this._drawImpactUnit(c,u,atlas,dir,spriteLevel,elevation)){drawCalls++;continue;}
        let frame=this._attackFrame(u);
        if(u.state==='turning')frame=0;
        else if(!frame&&(u.state==='move'||u.state==='rout'||u.state==='fallBack')){
          // Gait follows distance actually covered, so troops that slow, yield,
          // or stop in a gap do not keep marching in place at full speed.
          const type=global.GameData&&global.GameData.TYPES[u.type];
          const stride=u.type==='elephant'?48:type&&type.mounted?34:22;
          const phase=Number.isFinite(u.travelDistance)?u.travelDistance/stride*3:(u.anim||e.time||now)*7;
          frame=1+((phase+u.id*.37)%3|0);
        }
        // Cached elephant and hoplite strikes remain readable at overview scale.
        // Keep only their real release/follow-through; marching and other low
        // detail poses still use the inexpensive idle silhouette.
        if(low&&z<.2&&!((u.type==='elephant'||u.type==='hoplite'||u.type==='cannon'||u.type==='trebuchet'||u.type==='berserker')&&(frame===6||frame===7)))frame=0;
        if(u.routed)c.globalAlpha=.58;
        const action=extraActions?this._motionAction(u,frame):null;
        const fallback=action&&(action.kind==='backstep'||action.kind==='sidestep')?1+(action.phase*3/8|0):frame;
        const sprite=action?this._actionSprite(u,atlas,dir,action.kind,action.phase)||atlas.frames[fallback*8+dir]:atlas.frames[frame*8+dir];
        c.drawImage(sprite[spriteLevel],u.x+sprite.ox,u.y-elevation+sprite.oy,sprite.w,sprite.h);drawCalls++;
        if(u._siegeStage==='deploying')this._drawDeployingBridge(c,u);
        if(u.routed)c.globalAlpha=1;
        if((frame===6||frame===7)&&u.attackFlash>.1&&z>.45&&u.type!=='berserker'&&!(global.GameData&&global.GameData.TYPES[u.type]&&global.GameData.TYPES[u.type].ranged)){
          const thrust=['militia','pikeman','hoplite','halberdier','lancer'].includes(u.type);
          c.globalAlpha=u.attackFlash*.52;
          if(thrust)line(c,u.x+Math.cos(angle)*10,u.y-elevation-15+Math.sin(angle)*7,u.x+Math.cos(angle)*27,u.y-elevation-15+Math.sin(angle)*16,'#fff0bf',1.5);
          else{c.save();c.translate(u.x,u.y-elevation-18);c.scale(1,.62);c.strokeStyle='#f5e4b1';c.lineWidth=1.8;c.beginPath();c.arc(0,0,19,angle-(frame===6?1.1:.25),angle+(frame===6?.25:1.1));c.stroke();c.restore();}
          c.globalAlpha=1;
        }
        if(this.options.showHealth&&z>.45&&u.alive&&u.hp<u.maxHp){const width=u.type==='siegeTower'||u.type==='batteringRam'?44:u.type==='elephant'?32:u.type==='ballista'?28:16,yy=u.y-elevation-(WEAPON_ART[u.type]?.healthY??(u.type==='siegeTower'?148:u.type==='batteringRam'?64:u.type==='commander'?77:u.type==='elephant'?65:u.type==='ballista'?57:35));c.fillStyle='rgba(22,30,25,.7)';c.fillRect(u.x-width/2,yy,width,2.2);c.fillStyle=u.hp/u.maxHp>.4?TEAM[u.team||0].light:'#efb56a';c.fillRect(u.x-width/2,yy,width*clamp(u.hp/u.maxHp,0,1),2.2);}
      }
      this._drawProjectiles(c,bounds,z);
      this._drawEffects(c,bounds,z,low);
      this._drawStandards(c,selected,z,low);
      if(!this.options.cinematic&&(this.options.showOrders||this.pendingFacing))this._drawFacingIndicators(c,selected,z);
      // Small wind-streamers and the border stay readable at any zoom.
      c.strokeStyle='rgba(231,217,174,.24)';c.lineWidth=1/z;c.strokeRect(18,18,e.width-36,e.height-36);
      c.setTransform(this.dpr,0,0,this.dpr,0,0);c.globalAlpha=1;
      if(this.options.cinematic){
        const grd=c.createRadialGradient(this.width/2,this.height/2,this.height*.23,this.width/2,this.height/2,Math.max(this.width,this.height)*.7);grd.addColorStop(0,'rgba(8,17,13,0)');grd.addColorStop(1,'rgba(8,17,13,.42)');c.fillStyle=grd;c.fillRect(0,0,this.width,this.height);
      }
      this._renderStats={visible:visible.length,bodies,drawCalls,corpseTiles:this._corpseStats?this._corpseStats.drawnTiles:0};
    }

    _drawDeployment(c,bounds,z) {
      const e=this.engine;
      c.save();c.fillStyle='rgba(58,126,180,.055)';c.fillRect(0,0,e.width*.43,e.height);c.fillStyle='rgba(184,78,66,.055)';c.fillRect(e.width*.57,0,e.width*.43,e.height);
      c.lineWidth=1/z;c.setLineDash([8/z,10/z]);c.strokeStyle='rgba(142,193,221,.23)';line(c,e.width*.43,0,e.width*.43,e.height,'rgba(142,193,221,.23)',1/z);line(c,e.width*.57,0,e.width*.57,e.height,'rgba(224,148,123,.23)',1/z);c.setLineDash([]);
      if(z>.45){c.globalAlpha=.08;c.lineWidth=.7/z;c.strokeStyle='#e2ddbd';const step=160;for(let x=Math.max(0,Math.floor(bounds.x1/step)*step);x<Math.min(e.width,bounds.x2);x+=step){c.beginPath();c.moveTo(x,Math.max(0,bounds.y1));c.lineTo(x,Math.min(e.height,bounds.y2));c.stroke();}for(let y=Math.max(0,Math.floor(bounds.y1/step)*step);y<Math.min(e.height,bounds.y2);y+=step){c.beginPath();c.moveTo(Math.max(0,bounds.x1),y);c.lineTo(Math.min(e.width,bounds.x2),y);c.stroke();}}
      c.restore();
    }

    _drawOrders(c,selected,z) {
      c.save();
      for(const r of this.engine.regiments||[]){
        if(!selected.has(r.id)||!r.alive)continue;
        const p=TEAM[r.team||0],rw=Math.max(12,Number(r.effectiveWidth)||Number(r.width)||80),rh=Math.max(12,Number(r.effectiveDepth)||Number(r.depth)||50);
        const fallingBack=r.status==='fallingBack'||r.fallingBack,reforming=r.status==='rallying';
        const maneuver=r.maneuver||'formed',column=maneuver==='narrowing'||maneuver==='threading';
        const adapting=column||maneuver==='reforming'||Number(r.deformation)>12;
        const cohesion=clamp(Number.isFinite(r.cohesion)?r.cohesion/100:1,0,1);
        const color=fallingBack?'#efbd6e':reforming||maneuver==='reforming'?'#b8dcc4':p.light;
        c.strokeStyle=color;c.fillStyle=r.team?'rgba(215,115,89,.09)':'rgba(110,185,229,.10)';c.lineWidth=1.5/z;
        const ww=rw,hh=rh;
        // The solid rim follows actual soldiers throughout battle. The faint
        // guide shows their chosen shape while elastic ranks bend and recover.
        const measured=this.engine.phase!=='deployment'&&z>=.30&&r.alive>=4&&this._drawFormationEnvelope(c,r,z,color);
        c.save();c.translate(r.x,r.y);c.rotate(Number.isFinite(r.footprintFacing)?r.footprintFacing:Number(r.facing)||0);
        if(measured){c.globalAlpha=.25;c.setLineDash([4/z,5/z]);}
        else if(cohesion<.45||adapting)c.setLineDash([5/z,4/z]);
        if(r.formation==='wedge'&&!column){
          poly(c,[[hh*.65,0],[-hh*.5,-ww*.5],[-hh*.5,ww*.5]],c.fillStyle,color,1.5/z);
          line(c,hh*.45,0,hh*.12,-ww*.13,color,1.5/z);line(c,hh*.45,0,hh*.12,ww*.13,color,1.5/z);
        }else{
          if(r.formation==='loose'&&!column&&!measured)c.setLineDash([3/z,6/z]);
          c.fillRect(-hh*.5,-ww*.5,hh,ww);c.strokeRect(-hh*.5,-ww*.5,hh,ww);
          c.setLineDash([]);
          if(column){
            const tip=hh*.5,arrow=Math.min(11/z,ww*.30);
            line(c,tip-arrow,-arrow,tip,0,color,1.4/z);line(c,tip,0,tip-arrow,arrow,color,1.4/z);
          }else if(r.formation==='shieldwall'){
            for(let i=0;i<6;i++){const yy=-ww*.5+i*ww/6;line(c,hh*.5,yy+ww*.02,hh*.5,yy+ww*.145,color,3/z);}
          }else if(r.formation==='loose'){
            for(const side of [-1,1])for(const end of [-1,1]){const x=side*hh*.25,y=end*ww*.25;line(c,x-3/z,y,x+3/z,y,color,1/z);line(c,x,y-3/z,x,y+3/z,color,1/z);}
          }else{line(c,hh*.5,-ww*.5,hh*.5,ww*.5,color,2.5/z);}
        }
        c.restore();
        if(r.type==='commander'){
          const t=global.GameData&&global.GameData.TYPES.commander;
          const radius=Number(r.rallyRadius)||(t&&Number(t.rallyRadius))||620;
          const support=t&&Number(t.supportRadius)||360,cooldown=Math.max(0,Number(r.rallyCooldown)||0);
          c.save();c.globalAlpha=cooldown>0?.44:.8;c.setLineDash([7/z,6/z]);
          ellipse(c,r.x,r.y,radius,radius,'rgba(230,193,105,.025)','#e9cb84',1.25/z);
          c.setLineDash([2/z,6/z]);ellipse(c,r.x,r.y,support,support,null,'#d2c18b',1/z);c.setLineDash([]);
          c.font='600 '+10/z+'px system-ui, sans-serif';c.textAlign='center';c.textBaseline='bottom';
          c.fillStyle='#f3ddaa';c.fillText(cooldown>0?'RALLY IN '+Math.ceil(cooldown)+'s':'RALLY READY',r.x,r.y-radius-7/z);c.restore();
        }
        const orderX=Number.isFinite(r.commandX)?r.commandX:r.targetX,orderY=Number.isFinite(r.commandY)?r.commandY:r.targetY;
        if((r.order==='move'||r.order==='attack'||r.order==='charge'||r.order==='retreat')&&Number.isFinite(orderX)&&Number.isFinite(orderY)){
          const tx=orderX,ty=orderY,d=Math.hypot(tx-r.x,ty-r.y);
          if(d>20){c.globalAlpha=.75;c.setLineDash([7/z,6/z]);line(c,r.x,r.y,tx,ty,color,1.3/z);c.setLineDash([]);c.globalAlpha=1;const a=Math.atan2(ty-r.y,tx-r.x),l=9/z;line(c,tx,ty,tx-Math.cos(a-.55)*l,ty-Math.sin(a-.55)*l,color,1.5/z);line(c,tx,ty,tx-Math.cos(a+.55)*l,ty-Math.sin(a+.55)*l,color,1.5/z);ellipse(c,tx,ty,5/z,5/z,null,color,1/z);
            if(fallingBack){c.font='600 '+9/z+'px system-ui, sans-serif';c.textAlign='center';c.fillStyle=color;c.fillText(r.commandStatus==='turning'?'TURN / WITHDRAW':'WITHDRAW',tx,ty-11/z);}
            else if(r.order==='charge'){ellipse(c,tx,ty,12/z,12/z,null,color,1.2/z);c.font='700 '+9/z+'px system-ui, sans-serif';c.textAlign='center';c.fillStyle=color;c.fillText('CHARGE',tx,ty-16/z);}
          }
        }
        if(r.order==='auto'&&!fallingBack&&!reforming&&r._rallyTime<=0&&r._tactic)this._drawTacticRoute(c,r,z);
      }
      c.restore();
    }

    _drawTacticRoute(c,r,z) {
      const tactic=r._tactic;
      if(!tactic||!(tactic.transit||tactic.charge))return;
      const first=Math.max(0,tactic.waypointIndex||0);
      const points=(tactic.charge||!Array.isArray(tactic.waypoints)||!tactic.waypoints.length?[{x:tactic.goalX,y:tactic.goalY}]:tactic.waypoints.slice(first))
        .filter(p=>p&&Number.isFinite(p.x)&&Number.isFinite(p.y));
      if(!points.length)return;
      const color=TEAM[r.team||0].light;
      c.save();c.globalAlpha=.7;c.setLineDash([6/z,6/z]);c.lineWidth=1.5/z;c.strokeStyle=color;
      c.beginPath();c.moveTo(r.x,r.y);for(const p of points)c.lineTo(p.x,p.y);c.stroke();c.setLineDash([]);
      for(let i=0;i<points.length;i++){
        const p=points[i];c.globalAlpha=i===0?.95:.55;
        ellipse(c,p.x,p.y,(i===0?5:3.5)/z,(i===0?5:3.5)/z,'#1c2b26',i===0?'#f2d18a':color,1.3/z);
      }
      const p=points[0];c.globalAlpha=1;c.font='600 '+9/z+'px system-ui, sans-serif';c.textAlign='center';c.textBaseline='bottom';c.fillStyle='#efdcaa';
      c.fillText(tactic.phaseName||'BATTLE PLAN',p.x,p.y-10/z);c.restore();
    }

    _drawFacingIndicators(c,selected,z) {
      const pending=this.pendingFacing;
      const preview=!!(pending&&Number.isFinite(pending.x)&&Number.isFinite(pending.y));
      // Orientation is shown beside the formation. A distant pointer chooses
      // the angle only; no route, destination ring, or travel line is drawn.
      for(const r of this.engine.regiments||[]){
        if(!selected.has(r.id)||!r.alive||!Number.isFinite(r.x)||!Number.isFinite(r.y))continue;
        const actual=Number.isFinite(r.facing)?r.facing:r.team?Math.PI:0;
        const turning=r.order==='face'||r.commandStatus==='facing';
        const desired=preview&&Math.hypot(pending.x-r.x,pending.y-r.y)>1?Math.atan2(pending.y-r.y,pending.x-r.x):turning&&Number.isFinite(r.commandFacing)?r.commandFacing:null;
        const depth=Math.max(12,Number(r.effectiveDepth)||Number(r.depth)||50);
        const radius=clamp(depth*.5+8/z,16/z,70/z),color=TEAM[r.team||0].light;
        if(this.camera){
          const screen=this.worldToScreen(r.x,r.y),margin=radius*z+62;
          if(screen.x<-margin||screen.x>this.width+margin||screen.y<-margin||screen.y>this.height+margin)continue;
        }
        const arrow=(angle,length,head,tint,alpha,dashed)=>{
          c.save();c.translate(r.x,r.y);c.rotate(angle);c.globalAlpha=alpha;c.lineCap='round';
          c.setLineDash(dashed?[4/z,3/z]:[]);
          c.beginPath();c.moveTo(radius,0);c.lineTo(radius+length,0);
          c.strokeStyle='rgba(18,28,23,.88)';c.lineWidth=4/z;c.stroke();
          c.strokeStyle=tint;c.lineWidth=2/z;c.stroke();c.setLineDash([]);
          c.beginPath();c.moveTo(radius+length-head,-head*.65);c.lineTo(radius+length,0);c.lineTo(radius+length-head,head*.65);
          c.strokeStyle='rgba(18,28,23,.88)';c.lineWidth=4/z;c.stroke();
          c.strokeStyle=tint;c.lineWidth=2/z;c.stroke();c.restore();
        };
        arrow(actual,18/z,5/z,color,desired===null?.90:.48,false);
        if(desired===null)continue;
        const delta=Math.atan2(Math.sin(desired-actual),Math.cos(desired-actual));
        c.save();c.globalAlpha=.68;c.strokeStyle='#f1d58e';c.lineWidth=1.4/z;
        c.setLineDash(preview?[4/z,4/z]:[]);
        if(Math.abs(delta)>.08){c.beginPath();c.arc(r.x,r.y,radius+3/z,actual,actual+delta,delta<0);c.stroke();}
        c.restore();
        arrow(desired,26/z,6.5/z,'#f1d58e',1,preview);
        c.save();c.font='700 '+9/z+'px system-ui, sans-serif';c.textAlign='center';c.textBaseline='bottom';c.fillStyle='#f9e8b6';
        c.fillText(preview?'FACE':'TURN IN PLACE',r.x+Math.cos(desired)*(radius+26/z),r.y+Math.sin(desired)*(radius+26/z)-8/z);c.restore();
      }
    }

    _drawFormationEnvelope(c,r,z,color) {
      const time=Number(this.engine.time)||0;
      const footprintFacing=Number.isFinite(r.footprintFacing)?r.footprintFacing:Number(r.facing)||0;
      let entry=this._formationEnvelopes.get(r.id);
      const editMoved=this.engine.phase!=='running'&&entry&&(entry.x!==r.x||entry.y!==r.y||entry.facing!==footprintFacing||entry.alive!==r.alive);
      if(!entry||time<entry.time||time-entry.time>=.16||editMoved){
        const co=Math.cos(footprintFacing),si=Math.sin(footprintFacing);
        const type=global.GameData&&global.GameData.TYPES[r.type],padding=Math.max(5,(type&&type.radius)||6)+3;
        const points=[];let minF=Infinity,maxF=-Infinity,minX=Infinity,maxX=-Infinity,minY=Infinity;
        for(const id of r.unitIds||[]){
          const u=this.engine.units[id];if(!u||!u.alive||u.routed)continue;
          const dx=u.x-r.x,dy=u.y-r.y,f=dx*co+dy*si,s=-dx*si+dy*co;
          points.push([f,s]);if(f<minF)minF=f;if(f>maxF)maxF=f;minX=Math.min(minX,u.x);maxX=Math.max(maxX,u.x);minY=Math.min(minY,u.y);
        }
        const paths=[];
        if(points.length>=4){
          const span=Math.max(padding*2,maxF-minF),count=clamp(Math.ceil(span/Math.max(28,padding*3)),3,14),binWidth=span/count;
          const bins=Array.from({length:count},()=>({minS:Infinity,maxS:-Infinity,minF:Infinity,maxF:-Infinity,count:0}));
          for(const point of points){const b=bins[clamp(Math.floor((point[0]-minF)/binWidth),0,count-1)];b.minS=Math.min(b.minS,point[1]);b.maxS=Math.max(b.maxS,point[1]);b.minF=Math.min(b.minF,point[0]);b.maxF=Math.max(b.maxF,point[0]);b.count++;}
          let run=[];
          const commit=()=>{
            if(!run.length)return;
            const first=run[0],last=run[run.length-1];
            const lower=[[first.minF-padding,first.minS-padding]],upper=[[first.minF-padding,first.maxS+padding]];
            for(const b of run){const x=(b.minF+b.maxF)/2;lower.push([x,b.minS-padding]);upper.push([x,b.maxS+padding]);}
            lower.push([last.maxF+padding,last.minS-padding]);upper.push([last.maxF+padding,last.maxS+padding]);
            const local=lower.concat(upper.reverse());
            paths.push(local.map(point=>[r.x+point[0]*co-point[1]*si,r.y+point[0]*si+point[1]*co]));run=[];
          };
          for(const b of bins){if(b.count)run.push(b);else commit();}commit();
        }
        const bodyHeight=r.type==='elephant'||r.type==='commander'?79:type&&type.mounted?55:45;
        entry={time,x:r.x,y:r.y,facing:footprintFacing,alive:r.alive,paths,labelX:Number.isFinite(minX)?(minX+maxX)/2:r.x,labelY:Number.isFinite(minY)?minY-bodyHeight:r.y};this._formationEnvelopes.set(r.id,entry);
      }
      if(!entry.paths.length)return false;
      c.save();c.globalAlpha=.70;c.setLineDash([]);
      // No fill: a rim must not paint over a rock or imply that an empty space
      // between separate soldiers is physically occupied.
      for(const path of entry.paths)if(path.length>=3)poly(c,path,null,color,1.05/z);
      c.restore();return true;
    }

    _drawWaterMotion(c,bounds,now,z) {
      if(z<.19||this.options.quality==='low')return;
      c.save();c.globalAlpha=.16;c.strokeStyle='#c1dfd7';c.lineWidth=1.3;
      for(const r of this.map.regions||[]){if(r.type!=='water'||r.x>bounds.x2||r.x+r.w<bounds.x1)continue;
        const minY=Math.max(r.y,bounds.y1),maxY=Math.min(r.y+r.h,bounds.y2),step=86;
        for(let y=Math.floor(minY/step)*step;y<maxY;y+=step){const yy=y+(now*14)%step,x=r.x+25+((y*17.13)%Math.max(1,r.w-65));c.beginPath();c.moveTo(x,yy);c.quadraticCurveTo(x+12,yy-3,x+29,yy-1);c.stroke();}
      }c.restore();
    }

    _drawProjectiles(c,bounds,z) {
      const ps=this.engine.projectiles||[];c.save();
      for(let i=0;i<ps.length;i++){
        const p=ps[i];if(p.x<bounds.x1-45||p.x>bounds.x2+45||p.y<bounds.y1-24||p.y-(p.z||0)-125>bounds.y2)continue;
        let altitude=Number(p.z)||0;
        const siege=this.engine.siege;
        if(p.type==='siegeTower'||siege&&siege.ramps&&siege.ramps.length){
          const progress=clamp((Number(p.age)||0)/Math.max(.01,Number(p.duration)||1),0,1),attacker=this.engine.units&&this.engine.units[p.attacker];
          const start=p.type==='siegeTower'?116+(attacker&&attacker._siegeDeployed?0:siege&&siege.heightAt?siege.heightAt(p.sx,p.sy):0):siege&&siege.heightAt?siege.heightAt(p.sx,p.sy):0;
          const end=siege&&siege.heightAt?siege.heightAt(p.tx,p.ty):0;
          altitude+=start*(1-progress)+end*progress;
        }
        let x=p.x,y=p.y-altitude;
        if(p.type==='musketeer'||p.type==='cannon'||p.type==='trebuchet'){
          const fraction=clamp(p.age/Math.max(.001,p.duration),0,1);
          const dx=p.tx-p.sx,dy=p.ty-p.sy,length=Math.hypot(dx,dy)||1;
          const nx=dx/length,ny=dy/length;
          // Connect the shot to the muzzle or sling while it leaves the
          // sprite, then converge on the exact simulated impact point.
          const start=1-fraction;
          if(p.type==='trebuchet'){
            y-=87*start;
            c.globalAlpha=.23;ellipse(c,p.x,p.y,12,5,'#1c241d');c.globalAlpha=1;
            poly(c,[[x-10,y-4],[x-4,y-11],[x+6,y-10],[x+12,y-2],[x+8,y+7],[x-1,y+9],[x-10,y+5]],'#868e7f','#39423c',1.7);
            poly(c,[[x-8,y-3],[x-3,y-9],[x+5,y-8],[x+2,y-3]],'#bbc1ad');
          }else{
            x+=nx*27*start;y+=ny*16*start-19*start;
            if(p.type==='cannon'){
              c.globalAlpha=.26;ellipse(c,p.x,p.y,6,3,'#252922');c.globalAlpha=1;
              line(c,x-nx*20,y-ny*20,x,y,'rgba(219,202,159,.68)',3);
              ellipse(c,x,y,5.7,5.4,'#414940','#242b29',1.5);ellipse(c,x-1.8,y-2,1.8,1.3,'#bac1a9');
            }else{
              line(c,x-nx*21,y-ny*21,x,y,'#e5c88a',clamp(1/z,1.2,3));
              ellipse(c,x,y,1.9,1.6,'#362f26');
            }
          }
          continue;
        }
        const a=Math.atan2((p.ty||p.y)-p.y,(p.tx||p.x)-p.x),stone=p.kind==='stone';
        if(stone){c.globalAlpha=.16;ellipse(c,p.x,p.y,7,3.3,'#152c22');c.globalAlpha=1;ellipse(c,x,y,5.3,4.5,'#a3a997','#4b5b4e',1);line(c,x-2,y-2,x+1,y-3,'#d7d7bb',1);}
        else if(p.kind==='slingStone'){
          c.globalAlpha=.35;line(c,x-Math.cos(a)*9,y-Math.sin(a)*5,x,y,'#d3cbb3',1/z);c.globalAlpha=1;ellipse(c,x,y,2.2,1.8,'#d2d3bb','#667a67',.6);
        }else if(p.kind==='javelin'||p.kind==='ballistaBolt'){
          const heavy=p.kind==='ballistaBolt',len=heavy?36:26,co=Math.cos(a),si=Math.sin(a)*.65,px=-Math.sin(a),py=Math.cos(a)*.8;
          line(c,x-co*len,y-si*len,x,y,heavy?'#a5834c':'#b29a6a',heavy?2.5:1.6);
          line(c,x-co*len+.5,y-si*len-.5,x+.5,y-.5,heavy?'#e4d3a5':'#e4d1a2',heavy?1.1:.7);
          const h=heavy?8:5,w=heavy?3:2;poly(c,[[x+co*h,y+si*h],[x-co*2+px*w,y-si*2+py*w],[x-co*2-px*w,y-si*2-py*w]],'#e0e8d9','#74816c',.45);
          if(heavy){const tx=x-co*(len-4),ty=y-si*(len-4);line(c,tx,ty,tx-co*5+px*5,ty-si*5+py*5,'#dce1cc',1.2);line(c,tx,ty,tx-co*5-px*5,ty-si*5-py*5,'#dce1cc',1.2);}
        }
        else{const len=p.kind==='bolt'?10:14;c.strokeStyle=p.kind==='bolt'?'#d8e6dc':'#ddc8a0';c.lineWidth=clamp(1.15,1/z,3);c.beginPath();c.moveTo(x-Math.cos(a)*len,y-Math.sin(a)*len*.6);c.lineTo(x,y);c.stroke();if(z>.6){line(c,x-Math.cos(a)*len,y-Math.sin(a)*len*.6,x-Math.cos(a)*len-2,y-Math.sin(a)*len*.6-3,TEAM[p.team||0].light,.7);}}
      }c.restore();
    }

    _drawEffects(c,bounds,z,low) {
      const effects=this.engine.effects||[];c.save();
      for(let i=0;i<effects.length;i++){
        const e=effects[i];if(e.x<bounds.x1||e.x>bounds.x2||e.y<bounds.y1||e.y>bounds.y2||(low&&i%2))continue;
        const life=Number(e.life)||.5,t=clamp((Number(e.age)||0)/life,0,1),r=(Number(e.size)||12)*(1+t*.6),fade=1-t;
        if(e.kind==='musketMuzzle'||e.kind==='cannonMuzzle'){
          const cannon=e.kind==='cannonMuzzle',angle=Number.isFinite(e.angle)?e.angle:(this.engine.units[e.unitId]?.angle||0);
          const nx=Math.cos(angle),ny=Math.sin(angle),x=e.x+nx*27,y=e.y+ny*16-19;
          const length=cannon?30:16;
          if(t<.46){
            c.globalAlpha=fade;poly(c,[[x-ny*3,y+nx*3],[x+nx*length,y+ny*length],[x+ny*3,y-nx*3]],'#fff1af','#e7a148',1.1);
          }
          for(let j=0;j<(cannon?3:2);j++){
            const size=(cannon?4:2.5)*(1+t*2.5),drift=(j+1)*(2+t*5);
            c.globalAlpha=fade*.48;ellipse(c,x+nx*drift+j*2,y+ny*drift-t*(10+j*3),size,size*.8,'#e1dfc9');
          }
        }else if(e.kind==='clubSweep'){
          const half=e.halfAngle,lead=e.angle-half+2*half*Math.min(1,t*2+.22);
          const tail=Math.max(e.angle-half,lead-1.15*half);
          c.globalAlpha=fade*.7;c.strokeStyle='#f4ce87';c.lineWidth=4-2*t;c.beginPath();
          c.arc(e.x,e.y-10,e.radius*.9,tail,lead);c.stroke();
        }else if(e.kind==='musketHit'){
          c.globalAlpha=fade;for(let j=0;j<3;j++){const a=j*TAU/3+.6,len=4+7*t;line(c,e.x+Math.cos(a)*3,e.y-7+Math.sin(a)*3,e.x+Math.cos(a)*len,e.y-7+Math.sin(a)*len,'#efda9e',1.5);}
        }else if(e.kind==='cannonImpact'||e.kind==='trebuchetImpact'){
          const stone=e.kind==='trebuchetImpact',radius=e.size;
          const ring=radius*(.12+.82*t);
          c.globalAlpha=fade*.35;ellipse(c,e.x,e.y,ring,ring*.73,null,stone?'#d4bd99':'#e9c88a',stone?4:2.6);
          const debris=low?4:stone?10:7;
          for(let j=0;j<debris;j++){
            const a=j*TAU/debris+e.x*.09,dist=radius*(.14+t*.58)*(j%2?.7:1);
            const dx=e.x+Math.cos(a)*dist,dy=e.y+Math.sin(a)*dist*.78;
            const size=(stone?6:4)*(1+t);
            c.globalAlpha=fade*.46;ellipse(c,dx,dy-7*t,size*1.65,size,'#baaa8b');
            const lift=Math.sin(t*Math.PI)*(stone?36:20);
            c.globalAlpha=fade*.88;poly(c,[[dx-3,dy-lift],[dx,dy-lift-4],[dx+3,dy-lift+1]],stone?'#737c6d':'#786c50');
          }
        }else if(e.kind==='explosion'){
          c.globalAlpha=fade*.28;ellipse(c,e.x,e.y,r,r*.6,'#5e5540');
          for(let j=0;j<6;j++){const a=j*TAU/6+e.x*.1,rr=r*(.3+t*.7);c.globalAlpha=fade*.45;ellipse(c,e.x+Math.cos(a)*rr,e.y+Math.sin(a)*rr*.6-t*20,r*.27,r*.2,'#d1b985');}
          c.globalAlpha=fade*.8;for(let j=0;j<5;j++){const a=j*TAU/5;line(c,e.x+Math.cos(a)*r*.2,e.y+Math.sin(a)*r*.2,e.x+Math.cos(a)*r*.65,e.y+Math.sin(a)*r*.4,'#e5c683',2);}
        }else if(e.kind==='cavalryImpact'){
          c.globalAlpha=fade*.25;ellipse(c,e.x,e.y,r*(.7+t),r*(.3+t*.3),'#d4c096');
          for(let j=0;j<5;j++){
            const a=j*TAU/5+e.x*.07,rr=r*(.2+t*.8),x=e.x+Math.cos(a)*rr,y=e.y+Math.sin(a)*rr*.55-t*13;
            c.globalAlpha=fade*.5;ellipse(c,x,y,4+5*t,3+2*t,'#c7b38a');
            if(z>.28){c.globalAlpha=fade*.8;line(c,x,y,x+Math.cos(a)*5,y+Math.sin(a)*4,'#ead7a5',1.5);}
          }
        }else if(e.kind==='siegeHit'||e.kind==='breach'||e.kind==='siegeBreach'){
          c.globalAlpha=fade*.32;ellipse(c,e.x,e.y-t*12,r*(1+t),r*.7,'#c5b493');
          for(let j=0;j<7;j++){const a=j*TAU/7,rr=r*(.3+t);c.globalAlpha=fade*.85;c.fillStyle=j%2?'#a9a28c':'#7f816d';c.fillRect(e.x+Math.cos(a)*rr,e.y+Math.sin(a)*rr*.65-t*14,4+j%3,3+j%2);}
        }else if(e.kind==='hit'){
          c.globalAlpha=fade;const yy=e.y-13;line(c,e.x-r*.35,yy-r*.25,e.x+r*.35,yy+r*.25,'#f0dfaa',clamp(1.2,1/z,3));line(c,e.x-r*.2,yy+r*.35,e.x+r*.2,yy-r*.35,'#f6e8c0',clamp(1,1/z,3));
        }else if(e.kind==='rally'){
          c.globalAlpha=fade*.8;ellipse(c,e.x,e.y,r,r,null,'#f0d495',2/z);
        }else{
          c.globalAlpha=fade*.16;ellipse(c,e.x,e.y-t*6,r,r*.45,this.theme.grass);
          if(e.kind==='charge'){c.globalAlpha=fade*.35;ellipse(c,e.x,e.y,r*1.2,r*.65,null,'#dcc48d',1.5/z);}
        }
      }c.restore();
    }

    _drawStandards(c,selected,z,low) {
      const regs=this.engine.regiments||[],drawLabels=this.options.showLabels&&!this.options.cinematic;
      c.save();
      for(const r of regs){
        if(!r.alive)continue;
        const p=TEAM[r.team||0],isSel=selected.has(r.id);
        const fallingBack=r.status==='fallingBack'||r.fallingBack,reforming=r.status==='rallying',commander=r.type==='commander';
        const maneuver=r.maneuver||'formed',passage=maneuver==='narrowing'||maneuver==='threading';
        const maneuverStatus=maneuver==='narrowing'?'NARROWING':maneuver==='threading'?(Number(r.trafficPressure)>.45?'YIELDING / PASSAGE':'PASSAGE'):maneuver==='reforming'?'REFORMING RANKS':'';
        const pushing=r.type==='hoplite'&&Number(r.pushingSoldiers)>0;
        const yielding=r._formationPressureSpeed>.01&&r._formationPressureUntil>this.engine.time&&r._pressureSourceId>=0&&r.order!=='move'&&r.order!=='face'&&r.order!=='retreat'&&!(r._rallyTime>0)&&!(r.order==='attack'&&r._hasDestination&&!r._attackBlocked);
        const status=r.commandStatus==='facing'?'TURNING IN PLACE':r.commandStatus==='turning'?'TURNING TO WITHDRAW':fallingBack?'FALLING BACK':reforming?'REFORMING':r.status==='shaken'?'SHAKEN':r.status==='routing'?'ROUTING':((isSel||z>.65)?r.siegeStatus?r.siegeStatus.toUpperCase():yielding?'GIVING GROUND':pushing?'SHIELD PRESS':maneuverStatus:'');
        const statusColor=r.commandStatus==='facing'?'#f1d58e':fallingBack||r.status==='shaken'?'#efbd6e':status==='GIVING GROUND'?'#d8bd89':reforming||maneuver==='reforming'?'#b8dcc4':pushing?'#e9c67a':passage?'#d9c696':'#df8f77';
        const flagColor=fallingBack?'#b78a46':reforming?'#6e9d87':p.base,trim=commander?'#f4d78a':p.trim;
        const screen=this.worldToScreen(r.x,r.y);if(screen.x<-90||screen.x>this.width+90||screen.y<-90||screen.y>this.height+90)continue;
        const scale=clamp(1/z,.8,3.8)*(commander?1.25:1),fx=r.x,fy=r.y-18*scale;
        if(drawLabels||isSel){
          line(c,fx,fy+12*scale,fx,fy-15*scale,commander?'#dfc37b':'#4a4c36',1.6*scale);
          const wind=Math.sin(this._now*2.4+r.id)*1.5;
          poly(c,[[fx,fy-15*scale],[fx+17*scale,fy-(13+wind)*scale],[fx+14*scale,fy-3*scale],[fx,fy-5*scale]],flagColor,trim,(commander?1:.55)*scale);
          if(fallingBack||reforming)line(c,fx+1*scale,fy-14*scale,fx+1*scale,fy-6*scale,p.light,2*scale);
          else if(passage)line(c,fx+3*scale,fy-5.5*scale,fx+12*scale,fy-4.5*scale,'#e4cb90',1.2*scale);
          const t=r.type;
          c.strokeStyle=p.trim;c.lineWidth=.9*scale;
          if(commander){poly(c,[[fx+4*scale,fy-7*scale],[fx+4*scale,fy-12*scale],[fx+7*scale,fy-9*scale],[fx+9*scale,fy-13*scale],[fx+11*scale,fy-9*scale],[fx+14*scale,fy-11*scale],[fx+13*scale,fy-6*scale]],'#f5dfa1');}
          else if(t==='siegeTower'){c.fillStyle=p.trim;c.fillRect(fx+6*scale,fy-12*scale,7*scale,6*scale);for(let n=0;n<3;n++)c.fillRect(fx+(6+n*2.5)*scale,fy-14*scale,1.8*scale,3*scale);}
          else if(t==='batteringRam'){line(c,fx+5*scale,fy-8*scale,fx+13*scale,fy-8*scale,p.trim,2*scale);poly(c,[[fx+4*scale,fy-10*scale],[fx+8*scale,fy-14*scale],[fx+14*scale,fy-10*scale]],p.trim);}
          else if(t==='hoplite'){line(c,fx+5*scale,fy-5.8*scale,fx+12*scale,fy-14*scale,p.trim,1);ellipse(c,fx+8*scale,fy-9.8*scale,3.1*scale,3.1*scale,'#b78d49',p.trim,.9*scale);ellipse(c,fx+8*scale,fy-9.8*scale,.8*scale,.8*scale,p.trim);}
          else if(t==='halberdier'){line(c,fx+6*scale,fy-7*scale,fx+11*scale,fy-14*scale,p.trim,1.1*scale);poly(c,[[fx+9*scale,fy-13*scale],[fx+14*scale,fy-13*scale],[fx+12*scale,fy-9*scale]],p.trim);}
          else if(t==='javelin'){for(let i=0;i<2;i++)line(c,fx+(5+i*4)*scale,fy-6*scale,fx+(8+i*4)*scale,fy-13*scale,p.trim,.9*scale);}
          else if(t==='slinger'){ellipse(c,fx+9*scale,fy-10*scale,3*scale,2*scale,null,p.trim,.9*scale);line(c,fx+6*scale,fy-9*scale,fx+6*scale,fy-6*scale,p.trim,.9*scale);}
          else if(t==='musketeer'||t==='berserker'){
            line(c,fx+5*scale,fy-6*scale,fx+13*scale,fy-14*scale,p.trim,1.2*scale);
            if(t==='berserker')line(c,fx+10*scale,fy-11*scale,fx+13*scale,fy-14*scale,p.trim,3*scale);
            else line(c,fx+5*scale,fy-6*scale,fx+7*scale,fy-8*scale,p.trim,2.8*scale);
          }
          else if(t==='cannon'){
            ellipse(c,fx+8*scale,fy-8*scale,2*scale,2*scale,null,p.trim,.9*scale);
            line(c,fx+7*scale,fy-10*scale,fx+14*scale,fy-13*scale,p.trim,2.1*scale);
          }
          else if(t==='trebuchet'){
            poly(c,[[fx+6*scale,fy-6*scale],[fx+10*scale,fy-12*scale],[fx+12*scale,fy-6*scale]],null,p.trim,.9*scale);
            line(c,fx+5*scale,fy-9*scale,fx+14*scale,fy-15*scale,p.trim,1.3*scale);
          }
          else if(t==='ballista'){line(c,fx+5*scale,fy-7*scale,fx+12*scale,fy-13*scale,p.trim,1.2*scale);line(c,fx+6*scale,fy-13*scale,fx+13*scale,fy-6*scale,p.trim,1.3*scale);line(c,fx+6*scale,fy-13*scale,fx+5*scale,fy-7*scale,p.trim,.6*scale);line(c,fx+13*scale,fy-6*scale,fx+5*scale,fy-7*scale,p.trim,.6*scale);}
          else if(t==='archer'||t==='horseArcher'||t==='crossbow'){c.beginPath();c.arc(fx+5*scale,fy-10*scale,4*scale,-Math.PI/2,Math.PI/2);c.stroke();line(c,fx+5*scale,fy-14*scale,fx+5*scale,fy-6*scale,p.trim,.6*scale);}
          else if(t==='cavalry'||t==='lancer'){poly(c,[[fx+5*scale,fy-7*scale],[fx+7*scale,fy-13*scale],[fx+11*scale,fy-11*scale],[fx+9*scale,fy-7*scale]],p.trim);}
          else{line(c,fx+8*scale,fy-13*scale,fx+8*scale,fy-7*scale,p.trim,1.2*scale);line(c,fx+5*scale,fy-10*scale,fx+11*scale,fy-10*scale,p.trim,1.2*scale);}
        }
        if((isSel||z>.55)&&drawLabels){
          const name=(global.GameData&&global.GameData.TYPES[r.type]?global.GameData.TYPES[r.type].shortName:r.type)||'Regiment';
          const label=name.toUpperCase()+'  '+Math.max(0,r.alive).toLocaleString(),fontSize=10/z;
          c.font='600 '+fontSize+'px system-ui, sans-serif';c.textAlign='center';c.textBaseline='bottom';
          const formName=({line:'LINE',shieldwall:'SHIELD WALL',wedge:'WEDGE',loose:'LOOSE'})[r.formation]||'LINE';
          const cohesion=Math.round(clamp(Number.isFinite(r.cohesion)?r.cohesion:100,0,100));
          const role=({line:'LINE',flank:'FLANK',support:'SUPPORT',reserve:'RESERVE'})[r.tacticalRole];
          const details=isSel?(commander?'COMMAND AURA':passage?'PASSAGE COLUMN':formName)+'  ·  COH '+cohesion+(role&&z>.8?'  ·  '+role:''):'';
          const envelope=isSel&&z>=.36&&this.options.showOrders&&(maneuver!=='formed'||Number(r.deformation)>12||Number(r.engagedSoldiers)>0)?this._formationEnvelopes.get(r.id):null;
          const tw=Math.max(c.measureText(label).width,details?c.measureText(details).width:0,status?c.measureText(status).width:0),x=envelope?envelope.labelX:r.x,y=envelope?envelope.labelY:fy-19*scale;
          const extra=(details?13:0)+(status?13:0);
          c.fillStyle='rgba(17,28,23,.87)';c.fillRect(x-tw/2-6/z,y-(12+extra)/z,tw+12/z,(16+extra)/z);
          c.fillStyle=commander?'#f3d58a':isSel?'#f4e8c4':p.light;c.fillText(label,x,y-extra/z);
          if(details){c.font='600 '+9/z+'px system-ui, sans-serif';c.fillStyle='#bfcdb7';c.fillText(details,x,y-(status?13:0)/z);}
          if(status){c.font='700 '+9/z+'px system-ui, sans-serif';c.fillStyle=statusColor;c.fillText(status,x,y);}
          if(isSel){
            const morale=clamp((Number(r.morale)||0)/100,0,1),barW=tw+12/z;
            c.fillStyle='#273b31';c.fillRect(x-tw/2-6/z,y+4/z,barW,5/z);
            c.fillStyle=morale>.35?p.light:'#e3b36c';c.fillRect(x-tw/2-6/z,y+4/z,barW*morale,2/z);
            c.fillStyle=cohesion>40?'#d6ca91':'#da9a62';c.fillRect(x-tw/2-6/z,y+7/z,barW*cohesion/100,2/z);
          }
        }else if(status&&drawLabels&&z>.2){
          c.font='700 '+9/z+'px system-ui, sans-serif';c.textAlign='center';c.textBaseline='bottom';
          const tw=c.measureText(status).width,y=fy-19*scale;c.fillStyle='rgba(24,29,22,.84)';c.fillRect(r.x-tw/2-4/z,y-10/z,tw+8/z,13/z);c.fillStyle=statusColor;c.fillText(status,r.x,y);
        }
      }
      c.restore();
    }

    renderMinimap(canvas,selectedIds) {
      if(!canvas||!this.engine)return;
      const c=canvas.getContext('2d'),w=canvas.width,h=canvas.height,e=this.engine;
      const sx=w/e.width,sy=h/e.height,selected=selectedIds instanceof Set?selectedIds:new Set(selectedIds||[]);
      const ramps=e.siege&&e.siege.ramps||[],revision=(e.terrainRevision||0)+':'+ramps.length;
      let base=this._minimapBase;
      if(!base||base.w!==w||base.h!==h||base.revision!==revision){
        if(base){base.canvas.width=1;base.canvas.height=1;}
        const image=newCanvas(w,h),b=image.getContext('2d');
        b.fillStyle='#37453a';b.fillRect(0,0,w,h);b.drawImage(this._terrain,0,0,w,h);b.fillStyle='rgba(14,24,20,.16)';b.fillRect(0,0,w,h);
        for(const s of e.structures||[]){b.fillStyle=s.open?'#598969':s.breached?'#bba675':s.type==='gate'?'#dbc08b':'#d0cbb2';b.fillRect(s.x*sx,s.y*sy,Math.max(1,s.w*sx),Math.max(1,s.h*sy));}
        for(const ramp of ramps){b.fillStyle='#efd18b';b.fillRect(ramp.x*sx,ramp.y*sy,Math.max(1,ramp.w*sx),Math.max(1,ramp.h*sy));}
        base=this._minimapBase={w,h,revision,canvas:image};
      }
      c.setTransform(1,0,0,1,0,0);c.globalAlpha=1;c.drawImage(base.canvas,0,0);
      const stride=Math.max(1,Math.floor(e.units.length/5000));
      for(let team=0;team<2;team++){
        c.fillStyle=TEAM[team].flag;
        for(let i=0;i<e.units.length;i+=stride){const u=e.units[i];if(u.alive&&u.team===team)c.fillRect(u.x*sx,u.y*sy,1.2,1.2);}
      }
      for(const r of e.regiments||[]){if(!r.alive)continue;c.fillStyle=selected.has(r.id)?'#fff0b5':r.type==='commander'?'#f0d17f':r.status==='fallingBack'?'#e0ae64':TEAM[r.team||0].flag;const size=r.type==='commander'?4.2:selected.has(r.id)?3.2:2;c.fillRect(r.x*sx-size/2,r.y*sy-size/2,size,size);}
      const halfW=this.width/this.camera.zoom/2,halfH=this.height/this.camera.zoom/2;
      c.strokeStyle='#eee4c5';c.lineWidth=1;c.strokeRect((this.camera.x-halfW)*sx,(this.camera.y-halfH)*sy,halfW*2*sx,halfH*2*sy);
      c.strokeStyle='rgba(227,219,184,.34)';c.strokeRect(.5,.5,w-1,h-1);
    }

    drawUnitIcon(canvas,typeId,team) {
      if(!canvas)return;const c=canvas.getContext('2d'),w=canvas.width,h=canvas.height;
      const a=this._getAtlas(typeId,team===1?1:0);c.clearRect(0,0,w,h);c.imageSmoothingEnabled=true;
      const p=TEAM[team===1?1:0],grad=c.createLinearGradient(0,0,w,h);grad.addColorStop(0,p.dark);grad.addColorStop(1,'#17241f');c.fillStyle=grad;c.fillRect(0,0,w,h);
      c.fillStyle='rgba(242,224,180,.07)';c.beginPath();c.arc(w*.55,h*.44,w*.40,0,TAU);c.fill();
      const mounted=typeId==='cavalry'||typeId==='lancer'||typeId==='horseArcher'||typeId==='commander',large=mounted||typeId==='catapult'||typeId==='ballista'||typeId==='elephant';
      const polearm=typeId==='pikeman'||typeId==='militia'||typeId==='halberdier';
      const desired=typeId==='elephant'?h*.91:large?h*.79:polearm?h*.70:h*.87;
      const bodyHeight=typeId==='commander'?79:typeId==='elephant'?77:typeId==='ballista'?61:typeId==='catapult'?53:mounted?55:typeId==='javelin'?49:typeId==='slinger'?47:39;
      const bodyWidth=typeId==='elephant'?88:typeId==='ballista'?94:typeId==='catapult'?64:typeId==='lancer'?76:typeId==='commander'?66:mounted?60:polearm?52:typeId==='javelin'?48:typeId==='slinger'?46:typeId==='berserker'?37:33;
      const sprite=a.frames[0],fit=!!WEAPON_ART[typeId]||typeId==='hoplite'||typeId==='siegeTower'||typeId==='batteringRam';
      const scale=fit?Math.min((w-10)/sprite.w,(h-10)/sprite.h):Math.min(desired/bodyHeight,(w-12)/bodyWidth);
      const center=typeId==='elephant'?4:typeId==='ballista'?6:typeId==='lancer'?9:mounted?2:polearm?11:typeId==='slinger'?5:0;
      const dx=fit?(w-sprite.w*scale)/2:w*.50-center*scale+sprite.ox*scale,dy=fit?(h-sprite.h*scale)/2:h*.88+sprite.oy*scale;
      c.drawImage(sprite.full,dx,dy,sprite.w*scale,sprite.h*scale);
      c.strokeStyle=typeId==='commander'?'rgba(240,205,119,.65)':'rgba(221,206,159,.20)';c.lineWidth=1;c.strokeRect(.5,.5,w-1,h-1);
    }

    destroy() {this._resetCorpseCache();if(this._minimapBase){this._minimapBase.canvas.width=1;this._minimapBase.canvas.height=1;this._minimapBase=null;}if(this._hitPoses)this._hitPoses.clear();if(this._terrain){this._terrain.width=1;this._terrain.height=1;}for(const a of this._atlas.values())for(const frame of a.unique.values())for(const level of ['full','mid','low']){frame[level].width=1;frame[level].height=1;}this._atlas.clear();this._unitAtlases.length=0;this._siegeTowers.length=0;this._depthSorted.length=0;this._depthCounts=new Int32Array(0);this._depthKeys=new Uint32Array(0);this._formationEnvelopes.clear();this._visible.length=0;this._visibleBodies.length=0;for(const bucket of this._bodyBuckets)bucket.length=0;this._bodyWarmQueue.length=0;this.pendingFacing=null;}
  }

  global.BattleRenderer=BattleRenderer;
})(typeof window!=='undefined'?window:globalThis);
