function init(){
  scene = new THREE.Scene();
  scene.background = new THREE.Color(0x08111c);
  scene.fog = new THREE.Fog(0x08111c,35,95);

  clock = new THREE.Clock();

  renderer = new THREE.WebGLRenderer({antialias:true});
  renderer.setSize(window.innerWidth, window.innerHeight);
  renderer.shadowMap.enabled = true;
  renderer.shadowMap.type = THREE.PCFSoftShadowMap;
  renderer.autoClear = false;
  document.body.appendChild(renderer.domElement);

  camMain = new THREE.PerspectiveCamera(70, window.innerWidth/window.innerHeight, .1, 500);
  camP1 = new THREE.PerspectiveCamera(70, window.innerWidth/window.innerHeight, .1, 500);
  camP2 = new THREE.PerspectiveCamera(70, window.innerWidth/window.innerHeight, .1, 500);

  scene.add(new THREE.HemisphereLight(0xffffff,0x162234,1.25));
  arenaMoodLight = new THREE.PointLight(0x65d9ff,0.18,60);
  arenaMoodLight.position.set(0,7,0);
  scene.add(arenaMoodLight);

  const sun = new THREE.DirectionalLight(0xffffff,1.9);
  sun.position.set(7,16,8);
  sun.castShadow = true;
  sun.shadow.mapSize.set(2048,2048);
  scene.add(sun);

  addSpotLight(-6,10,-2);
  addSpotLight(6,10,-2);
  addSpotLight(-6,10,7);
  addSpotLight(6,10,7);

  buildArena();
  applyCourtTheme(selectedCourt);
  buildHoop();
  buildBall();
  createArcLine();
  createShotMeter();
  createParticlePool();
  createConfettiPool();
  setGraphicsQuality(graphicsQuality);

  window.addEventListener("resize", onResize);
  window.addEventListener("keydown", onKeyDown);
  window.addEventListener("keyup", onKeyUp);
}

function createShotMeter(){
  const group = new THREE.Group();
  const back = new THREE.Mesh(new THREE.PlaneGeometry(1.28,.16),new THREE.MeshBasicMaterial({color:0x101418,transparent:true,opacity:.88,side:THREE.DoubleSide}));
  group.add(back);
  const stripCanvas = document.createElement("canvas");
  stripCanvas.width = 384; stripCanvas.height = 36;
  const sctx = stripCanvas.getContext("2d");
  const grad = sctx.createLinearGradient(0,0,stripCanvas.width,0);
  grad.addColorStop(0,"#ff3333");
  grad.addColorStop(0.2,"#ffcc33");
  grad.addColorStop(0.55,"#35ff6b");
  grad.addColorStop(0.78,"#ffb13b");
  grad.addColorStop(1,"#ff3333");
  sctx.fillStyle = grad;
  sctx.fillRect(0,0,stripCanvas.width,stripCanvas.height);
  const strip = new THREE.Mesh(new THREE.PlaneGeometry(1.2,.07),new THREE.MeshBasicMaterial({map:new THREE.CanvasTexture(stripCanvas),transparent:true,opacity:.85,side:THREE.DoubleSide}));
  strip.position.z = .002; strip.position.y = .016;
  group.add(strip);
  const fill = new THREE.Mesh(new THREE.PlaneGeometry(.01,.095),new THREE.MeshBasicMaterial({color:0xffffff,transparent:true,opacity:.95,side:THREE.DoubleSide}));
  fill.position.z = .004;
  group.add(fill);
  const glow = new THREE.Mesh(new THREE.PlaneGeometry(.12,.12),new THREE.MeshBasicMaterial({color:0x35ff6b,transparent:true,opacity:0,side:THREE.DoubleSide}));
  glow.position.z = .006;
  group.add(glow);
  group.visible = false;
  const greenRegion = new THREE.Mesh(new THREE.PlaneGeometry(.2,.082),new THREE.MeshBasicMaterial({color:0x35ff6b,transparent:true,opacity:.38,side:THREE.DoubleSide}));
  greenRegion.position.z = .003;
  group.add(greenRegion);
  shotMeter = {group,fill,glow,stripCanvas,sctx,greenRegion,lastProfile:null};
  scene.add(group);
}

function addSpotLight(x,y,z){
  const s = new THREE.SpotLight(0xfff0d8,1.22,62,Math.PI/5.8,.4);
  s.position.set(x,y,z);
  s.target.position.set(0,0,-3);
  s.castShadow = true;
  const shadowSize=graphicsQuality === "ultra" ? 1536 : graphicsQuality === "high" ? 1024 : 512;
  s.shadow.mapSize.set(shadowSize,shadowSize);
  s.shadow.bias = -0.00018;
  s.shadow.radius = graphicsQuality === "ultra" ? 5 : graphicsQuality === "high" ? 4 : 2;
  scene.add(s);
  scene.add(s.target);
}

function buildArena(){
  crowdMembers.length = 0;
  arenaWalls.length = 0;
  arenaLightStrips.length = 0;
  ledBoardMesh = null;
  ledBoardTexture = null;
  ledBoardCtx = null;
  arenaThemeRefs = {court:null,lineMats:[],trimMats:[],logoCanvases:[],crowdMats:[]};
  const theme = courts[selectedCourt] || courts.classicArena;
  const court = new THREE.Mesh(
    new THREE.BoxGeometry(COURT_W,.18,COURT_L),
    new THREE.MeshStandardMaterial({map:makeImprovedCourtTexture(theme),roughness:.28,metalness:.2,emissive:0x0f0a04,emissiveIntensity:.08})
  );
  court.position.y = -.09;
  court.receiveShadow = true;
  scene.add(court);
  arenaThemeRefs.court = court;

  const outer = new THREE.Mesh(
    new THREE.BoxGeometry(COURT_W+3,.16,COURT_L+3),
    new THREE.MeshStandardMaterial({color:0x18243a,roughness:.75})
  );
  outer.position.y = -.18;
  outer.receiveShadow = true;
  scene.add(outer);

  function line(x,z,w,l,color=0xffffff){
    const lineMat = new THREE.MeshBasicMaterial({color});
    arenaThemeRefs.lineMats.push(lineMat);
    const m = new THREE.Mesh(new THREE.BoxGeometry(w,.035,l),lineMat);
    m.position.set(x,.035,z);
    scene.add(m);
  }

  // Court boundary and markings
  line(0,-COURT_L/2+.15,COURT_W,.08);
  line(0,COURT_L/2-.15,COURT_W,.08);
  line(-COURT_W/2+.15,0,.08,COURT_L);
  line(COURT_W/2-.15,0,.08,COURT_L);
  line(0,0,COURT_W,.06);
  line(0,-6.7,12,.08);
  line(0,-10.8,4.2,.08,0xffd56a);
  line(0,8.2,4.2,.055,0xffd56a);

  const center = new THREE.Mesh(
    new THREE.TorusGeometry(1.6,.035,8,80),
    new THREE.MeshBasicMaterial({color:0xffffff})
  );
  center.rotation.x = Math.PI/2;
  center.position.y = .07;
  scene.add(center);

  const key = new THREE.Mesh(
    new THREE.BoxGeometry(5.2,.025,5.7),
    new THREE.MeshBasicMaterial({color:theme.lineColor,transparent:true,opacity:.2})
  );
  key.position.set(0,.04,-9.6);
  scene.add(key);

  buildThreePointLine(line);
  addCourtLogos(theme);
  addFloorScuffs();
  addGlowingCourtLines();
  buildArenaWalls();

  // Back wall / crowd end zone behind the hoop
  const wallMat = new THREE.MeshStandardMaterial({color:0x101923,roughness:.8});
  const backWall = new THREE.Mesh(new THREE.BoxGeometry(27,8,.5),wallMat);
  backWall.position.set(0,4,-15.1);
  backWall.castShadow = true;
  scene.add(backWall);

  // Arena trim and court glow strips
  const trimMat = new THREE.MeshBasicMaterial({color:theme.trimColor});
  arenaThemeRefs.trimMats.push(trimMat);
  const trims = [
    [0,.1,-COURT_L/2-.18,COURT_W+.6,.045],
    [0,.1,COURT_L/2+.18,COURT_W+.6,.045],
    [-COURT_W/2-.18,.1,0,.045,COURT_L+.6],
    [COURT_W/2+.18,.1,0,.045,COURT_L+.6]
  ];
  trims.forEach(t=>{
    const m = new THREE.Mesh(new THREE.BoxGeometry(t[3],.04,t[4]),trimMat);
    m.position.set(t[0],t[1],t[2]);
    scene.add(m);
  });

  if(graphicsQuality !== "low"){
    buildCrowdSection("back",theme);
    buildCrowdSection("left",theme);
    buildCrowdSection("right",theme);
  }
  buildArenaBanners();

  // Side bleacher hints so the world feels less empty.
  for(let side of [-1,1]){
    for(let r=0;r<3;r++){
      const bench = new THREE.Mesh(
        new THREE.BoxGeometry(1.1,.16,COURT_L*.72),
        new THREE.MeshStandardMaterial({color:r%2?0x293955:0x1b2b42,roughness:.7})
      );
      bench.position.set(side*(COURT_W/2+1.3+r*.62),.35+r*.35,0);
      scene.add(bench);
    }
  }

  for(let i=0;i<20;i++){
    const stripMat = new THREE.MeshBasicMaterial({color:theme.lineColor});
    arenaThemeRefs.trimMats.push(stripMat);
    const strip = new THREE.Mesh(
      new THREE.BoxGeometry((COURT_W+1.2)/20,.08,.16),
      stripMat
    );
    strip.position.set(-COURT_W/2-.6+(i+.5)*((COURT_W+1.2)/20),.15,COURT_L/2+.45);
    scene.add(strip);
    arenaLightStrips.push(strip);
  }

  const benchMat = new THREE.MeshStandardMaterial({color:0x1e2735,roughness:.72});
  const railMat = new THREE.MeshStandardMaterial({color:0x9fb0c4,metalness:.45,roughness:.35});
  for(let side of [-1,1]){
    const sidelineX = side*(COURT_W/2+1.65);
    const bench = new THREE.Mesh(new THREE.BoxGeometry(1.1,.35,5.2),benchMat);
    bench.position.set(sidelineX,.22,2.8); bench.castShadow = true; scene.add(bench);
    const cooler = new THREE.Mesh(new THREE.BoxGeometry(.45,.58,.45),new THREE.MeshStandardMaterial({color:0x3f93ff,roughness:.5}));
    cooler.position.set(sidelineX+side*0.05,.31,5.9); cooler.castShadow = true; scene.add(cooler);
    const camBox = new THREE.Mesh(new THREE.BoxGeometry(.5,.36,.4),new THREE.MeshStandardMaterial({color:0x141922,roughness:.62}));
    camBox.position.set(sidelineX,.2,-5.7); scene.add(camBox);
    if(graphicsQuality !== "low"){
      const tripod = new THREE.Mesh(new THREE.CylinderGeometry(.03,.03,.9,7),new THREE.MeshStandardMaterial({color:0x404958,roughness:.5}));
      tripod.position.set(sidelineX+side*0.18,.45,-6.3); tripod.castShadow = true; scene.add(tripod);
    }
    const rail = new THREE.Mesh(new THREE.BoxGeometry(.1,1.1,COURT_L*.92),railMat);
    rail.position.set(side*(COURT_W/2+3.35),.74,0); rail.castShadow = true; scene.add(rail);
  }
}

function buildArenaWalls(){
  const glassMat = new THREE.MeshStandardMaterial({
    color:0x8fc8ff,
    transparent:true,
    opacity:.22,
    roughness:.08,
    metalness:.05,
    side:THREE.DoubleSide
  });
  const railMat = new THREE.MeshStandardMaterial({color:0xd9e6ff,metalness:.35,roughness:.28});
  const postMat = new THREE.MeshStandardMaterial({color:0x36485f,metalness:.25,roughness:.45});

  function wall(x,y,z,w,h,d){
    const panel = new THREE.Mesh(new THREE.BoxGeometry(w,h,d),glassMat);
    panel.position.set(x,y,z);
    panel.castShadow = true;
    panel.receiveShadow = true;
    scene.add(panel);
    arenaWalls.push(panel);

    const rail = new THREE.Mesh(new THREE.BoxGeometry(w+.05,.08,d+.05),railMat);
    rail.position.set(x,y+h/2+.05,z);
    rail.castShadow = true;
    scene.add(rail);
  }

  wall(0,.95,-COURT_L/2-.55,COURT_W+1.25,1.55,.18);
  wall(0,.95, COURT_L/2+.55,COURT_W+1.25,1.55,.18);
  wall(-COURT_W/2-.55,.95,0,.18,1.55,COURT_L+1.25);
  wall( COURT_W/2+.55,.95,0,.18,1.55,COURT_L+1.25);

  const posts = [
    [-COURT_W/2-.55,-COURT_L/2-.55],[COURT_W/2+.55,-COURT_L/2-.55],
    [-COURT_W/2-.55,COURT_L/2+.55],[COURT_W/2+.55,COURT_L/2+.55]
  ];
  posts.forEach(([x,z])=>{
    const p = new THREE.Mesh(new THREE.CylinderGeometry(.12,.12,1.85,16),postMat);
    p.position.set(x,.95,z);
    p.castShadow = true;
    scene.add(p);
  });
}

function createCrowdPerson(type,palette={}){
  const archetype = crowdArchetypes[type] || crowdArchetypes.tshirtFan;
  const g = new THREE.Group();
  const scale = (archetype.scale || 1) * THREE.MathUtils.randFloat(0.88,1.15);
  const skinTones = [0xf4c79a,0xd49a6a,0xb37a4b,0x8b5a37,0x5d3425,0x3d2418];
  const topColors = palette.top || [0xffd56a,0x65f0ff,0xff5f8f,0xffffff,0x7de35f,0x9d6cff,0x3f7bff];
  const pantColors = palette.pants || [0x263248,0x1a1a1a,0x334258,0x505f72];
  const skinMat = new THREE.MeshStandardMaterial({color:skinTones[Math.floor(Math.random()*skinTones.length)],roughness:.6});
  const topMat = new THREE.MeshStandardMaterial({color:topColors[Math.floor(Math.random()*topColors.length)],roughness:.55});
  const pantMat = new THREE.MeshStandardMaterial({color:pantColors[Math.floor(Math.random()*pantColors.length)],roughness:.68});
  const head = new THREE.Mesh(new THREE.SphereGeometry(.11,10,10),skinMat); head.position.y=.46; g.add(head);
  const torsoH = archetype.pose === "seated" ? .18 : .24;
  const torso = new THREE.Mesh(new THREE.CapsuleGeometry(.1,torsoH,6,10),topMat); torso.position.y=.29; g.add(torso);
  const armL = new THREE.Mesh(new THREE.CapsuleGeometry(.04,.16,5,8),skinMat); armL.position.set(-.13,.3,0); g.add(armL);
  const armR = armL.clone(); armR.position.x=.13; g.add(armR);
  if(archetype.pose === "seated"){
    const lower = new THREE.Mesh(new THREE.BoxGeometry(.22,.1,.12),pantMat); lower.position.set(0,.12,.05); g.add(lower);
  }else{
    const legL = new THREE.Mesh(new THREE.CapsuleGeometry(.045,.15,5,8),pantMat); legL.position.set(-.06,.1,0); g.add(legL);
    const legR = legL.clone(); legR.position.x=.06; g.add(legR);
  }
  const hairMat = new THREE.MeshStandardMaterial({color:palette.hair || [0x0d0d0d,0x2e1d12,0x4a2d18,0x6a4028][Math.floor(Math.random()*4)],roughness:.7});
  if(archetype.hat === "cap"){
    const cap = new THREE.Mesh(new THREE.CylinderGeometry(.1,.09,.05,10),new THREE.MeshStandardMaterial({color:0x223b77})); cap.position.set(0,.55,.01); g.add(cap);
  }else if(archetype.hat === "beanie"){
    const beanie = new THREE.Mesh(new THREE.SphereGeometry(.1,10,10),new THREE.MeshStandardMaterial({color:0x4b2c7a})); beanie.position.y=.54; beanie.scale.y=.7; g.add(beanie);
  }else if(archetype.hair === "long"){
    const hair = new THREE.Mesh(new THREE.SphereGeometry(.105,10,10),hairMat); hair.position.set(0,.5,-.02); hair.scale.set(1,1.3,1.1); g.add(hair);
  }else{
    const hair = new THREE.Mesh(new THREE.SphereGeometry(.095,8,8),hairMat); hair.position.y=.53; hair.scale.y=.5; g.add(hair);
  }
  g.scale.setScalar(scale);
  if(archetype.top === "hoodie"){
    const hood = new THREE.Mesh(new THREE.TorusGeometry(.09,.024,8,16,Math.PI),topMat);
    hood.position.set(0,.37,-.03); hood.rotation.x = Math.PI/2; g.add(hood);
  }else if(archetype.top === "jersey"){
    const trim = new THREE.Mesh(new THREE.TorusGeometry(.078,.012,8,14),new THREE.MeshStandardMaterial({color:0xffffff,roughness:.35}));
    trim.position.set(0,.39,.01); trim.rotation.x = Math.PI/2; g.add(trim);
  }
  g.userData = {baseY:0,phase:Math.random()*Math.PI*2,pose:archetype.pose,type,torso,armL,armR,reactionBias:THREE.MathUtils.randFloat(0.75,1.28),standMix:Math.random()};
  g.traverse(o=>{ if(o.material) arenaThemeRefs.crowdMats.push(o.material); });
  return g;
}


function createCrowdSection(side,rowCount,colCount){
  const theme = courts[selectedCourt] || courts.classicArena;
  const sideSign = side === "left" ? -1 : side === "right" ? 1 : 0;
  const types = Object.keys(crowdArchetypes);
  for(let row=0;row<rowCount;row++){
    for(let i=0;i<colCount;i++){
      const type = types[(i+row*2)%types.length];
      const fan = createCrowdPerson(type,{top:[theme.trimColor,theme.lineColor,0xffffff],pants:[0x1a1f2c,0x2a3142]});
      if(side === "back") fan.position.set(-8.2+i*.62,1.0+row*.39,-14.45-row*.24);
      else fan.position.set(sideSign*(COURT_W/2+2.8+row*.52),1+row*.38,-7.6+i*.72);
      fan.userData.baseY = fan.position.y; fan.userData.row=row; fan.userData.col=i;
      crowdMembers.push(fan); scene.add(fan);
    }
  }
}

function buildCrowdSection(side,theme=courts[selectedCourt]){
  const density = graphicsQuality === "high" ? 1 : graphicsQuality === "medium" ? 0.72 : 0;
  const baseRows = side === "back" ? 6 : 4;
  const baseCols = side === "back" ? 28 : 22;
  const rows = Math.max(1,Math.floor(baseRows * density));
  const cols = Math.max(6,Math.floor(baseCols * density));
  createCrowdSection(side,rows,cols);
}

function animateCrowdPerson(person,dt,excitement){
  const t = performance.now()*.001 + person.userData.phase;
  const poseMul = person.userData.pose === "hype" ? 1.35 : person.userData.pose === "standing" ? 1.05 : .85;
  let stateBoost = 1;
  if(crowdReactionState === "hype") stateBoost = 1.35;
  else if(crowdReactionState === "stunned") stateBoost = 0.72;
  else if(crowdReactionState === "gamePoint") stateBoost = 1.5;
  const react = excitement * person.userData.reactionBias * stateBoost;
  const bob = Math.sin(t*(2.1+react*1.6))*0.02*poseMul;
  const sway = Math.sin(t*1.7)*0.05 + excitement*0.1*Math.sin(t*5 + person.userData.col*.3);
  const standLift = react > .48 && person.userData.pose === "seated" && person.userData.standMix > 0.62 ? THREE.MathUtils.clamp((react-0.48)*0.3,0,0.11) : 0;
  const reactionLift = crowdReactionTimer > 0 ? Math.max(0,crowdReactionTimer) * 0.055 * crowdReactionStrength : 0;
  person.position.y = person.userData.baseY + bob + crowdWave*0.04 + standLift + reactionLift;
  person.rotation.y = sway*0.4;
  if(person.userData.armL && person.userData.armR){
    const clap = Math.sin(t*8 + person.userData.row)*0.5 + 0.5;
    const hype = react > .4 || person.userData.pose === "hype";
    person.userData.armL.rotation.z = hype ? -0.45 - clap*(0.7+react*0.5) : -0.2 - clap*0.2;
    person.userData.armR.rotation.z = hype ? 0.45 + clap*(0.7+react*0.5) : 0.2 + clap*0.2;
  }
  person.traverse(o=>{ if(o.material && o.material.emissive){ o.material.emissive.setHex(0x000000); o.material.emissiveIntensity = .03 + react*.18; } });
}

function updateCrowd(dt){
  if(!qualitySettings[graphicsQuality].crowd) return;
  crowdReactionTimer = Math.max(0,crowdReactionTimer - dt);
  crowdReactionStrength = Math.max(0,crowdReactionStrength - dt*0.9);
  if(crowdReactionTimer <= 0 && crowdReactionState !== "normal") crowdReactionState = "normal";
  crowdWave = Math.max(0,crowdWave - dt*.62);
  arenaExcitement = Math.max(0,arenaExcitement - dt*.22);
  const excit = THREE.MathUtils.clamp(arenaExcitement + crowdWave*.35,0,1);
  crowdMembers.forEach((fan,i)=>{
    if(graphicsQuality === "low" && i%3!==0) return;
    animateCrowdPerson(fan,dt,excit);
  });
}

function buildArenaBanners(){
  const theme = courts[selectedCourt] || courts.classicArena;
  const banners = [
    {text:"1V1 ARENA",x:0,y:5.3,z:-14.72,w:6.1,h:1.15,ry:0},
    {text:"HOME COURT",x:-10.2,y:4.5,z:-3.2,w:4.6,h:1.0,ry:Math.PI/2},
    {text:"DEFENSE",x:10.2,y:4.5,z:2.6,w:4.2,h:1.0,ry:-Math.PI/2},
    {text:"DUNK CAM",x:0,y:4.2,z:14.6,w:4.8,h:0.95,ry:Math.PI},
    {text:theme.name.toUpperCase(),x:0,y:6.6,z:-13.9,w:7.2,h:1.05,ry:0}
  ];
  banners.forEach(({text,x,y,z,w,h,ry})=>{
    const banner = new THREE.Mesh(
      new THREE.PlaneGeometry(w,h),
      new THREE.MeshStandardMaterial({map:makeBannerTexture(text),transparent:true,emissive:0x102034,emissiveIntensity:.5,side:THREE.DoubleSide})
    );
    banner.position.set(x,y,z);
    banner.rotation.y = ry;
    scene.add(banner);
  });

  const ledCanvas = document.createElement("canvas");
  ledCanvas.width = 1024; ledCanvas.height = 128;
  ledBoardCtx = ledCanvas.getContext("2d");
  ledBoardTexture = new THREE.CanvasTexture(ledCanvas);
  ledBoardMesh = new THREE.Mesh(
    new THREE.PlaneGeometry(COURT_W+1,.9),
    new THREE.MeshBasicMaterial({map:ledBoardTexture,transparent:true})
  );
  ledBoardMesh.position.set(0,2.2,COURT_L/2+.62);
  scene.add(ledBoardMesh);
  updateLEDBoards("FIRST TO 11");
}

function makeBannerTexture(text){
  const c = document.createElement("canvas");
  c.width = 1024; c.height = 128;
  const g = c.getContext("2d");
  g.fillStyle = "#09111a"; g.fillRect(0,0,c.width,c.height);
  g.strokeStyle = "#ffd56a"; g.lineWidth = 8; g.strokeRect(8,8,c.width-16,c.height-16);
  g.fillStyle = "#d9efff"; g.font = "bold 48px Arial"; g.textAlign = "center"; g.textBaseline = "middle";
  g.fillText(text,c.width/2,c.height/2);
  return new THREE.CanvasTexture(c);
}

function updateLEDBoards(text){
  if(!ledBoardCtx) return;

  const fx = getCourtEffectTheme();
  ledBoardCtx.fillStyle = "#03050a";
  ledBoardCtx.fillRect(0,0,1024,128);

  const accent =
    fx === "urban" ? "#ff7a45" :
    fx === "gold" ? "#ffd56a" :
    fx === "ice" ? "#9fe9ff" :
    "#38ff9a";

  const pulse = 0.3 + arenaExcitement * 0.45 + (crowdReactionTimer > 0 ? 0.3 : 0);

  ledBoardCtx.fillStyle = `rgba(255,255,255,${Math.min(0.34,pulse).toFixed(3)})`;
  ledBoardCtx.fillRect(0,0,1024,6);

  ledBoardCtx.fillStyle = accent;
  ledBoardCtx.font = "bold 58px 'Courier New', monospace";
  ledBoardCtx.textAlign = "center";
  ledBoardCtx.textBaseline = "middle";
  ledBoardCtx.fillText(text || "1V1 ARENA",512,44);

  ledBoardCtx.font = "bold 28px 'Courier New', monospace";
  ledBoardCtx.fillStyle = "#dce9ff";

  // FIX: use existing game state instead of undefined possessionOwner
  const currentPossession = possession || ballOwner || "p1";
  const posLabel = currentPossession === "p2" ? "P2" : "P1";

  ledBoardCtx.fillText(`${scoreP1} - ${scoreP2}  •  POS: ${posLabel}`,512,95);

  ledBoardTexture.needsUpdate = true;
}

function pulseArenaLights(colorIntensity){
  const mood = courts[selectedCourt] || courts.classicArena;
  const moodColor = new THREE.Color(mood.trimColor || 0x65d9ff);
  courtThemePulse = Math.max(0,courtThemePulse - 0.9/60);
  const themeColor = new THREE.Color(getThemeColorForEvent(crowdReactionState === "hype" ? "green" : "make"));
  arenaLightStrips.forEach((s,i)=>{
    const pulse = (.5+.5*Math.sin(performance.now()*.004 + i*.4))*(colorIntensity + arenaExcitement*0.4 + courtThemePulse*0.45);
    const c = moodColor.clone().lerp(themeColor,0.08 + courtThemePulse*0.35).lerp(new THREE.Color(0xffffff),0.12 + pulse*0.2);
    s.material.color.copy(c);
  });
  if(arenaMoodLight){
    arenaMoodLight.intensity = 0.15 + arenaExcitement*0.45 + colorIntensity*0.08 + courtThemePulse*0.14;
  }
}

function buildThreePointLine(lineFn){
  const mat = new THREE.LineBasicMaterial({color:0xffffff});
  const pts = [];
  const centerZ = HOOP_POS.z;

  for(let i=0;i<=120;i++){
    const angle = THREE.MathUtils.degToRad(200 - 220 * i/120);
    const x = Math.cos(angle) * THREE_RADIUS;
    const z = centerZ + Math.sin(angle) * THREE_RADIUS;
    if(Math.abs(x) <= COURT_W/2 - 1.35 && z > -12.35){
      pts.push(new THREE.Vector3(x,.09,z));
    }
  }

  const geo = new THREE.BufferGeometry().setFromPoints(pts);
  scene.add(new THREE.Line(geo,mat));

  lineFn(-COURT_W/2+1.35,-10.2,.08,4.25);
  lineFn(COURT_W/2-1.35,-10.2,.08,4.25);
}

function makeImprovedCourtTexture(theme=courts[selectedCourt]){
  const canvas = document.createElement("canvas");
  canvas.width = 1024;
  canvas.height = 1024;
  const ctx = canvas.getContext("2d");
  const grad = ctx.createLinearGradient(0,0,1024,1024);
  grad.addColorStop(0,theme.woodA);
  grad.addColorStop(.5,theme.woodB);
  grad.addColorStop(1,theme.woodA);
  ctx.fillStyle = grad;
  ctx.fillRect(0,0,1024,1024);
  for(let i=0;i<28;i++){
    ctx.fillStyle = i%2 ? "rgba(255,255,255,.055)" : "rgba(0,0,0,.065)";
    ctx.fillRect(i*37,0,37,1024);
    ctx.fillStyle = i%2 ? "rgba(255,240,210,.025)" : "rgba(75,42,18,.03)";
    for(let y=0;y<1024;y+=24){
      ctx.fillRect(i*37,y+Math.random()*6,37,2);
    }
  }
  ctx.strokeStyle = "rgba(70,35,10,.35)";
  ctx.lineWidth = 2;
  for(let x=0;x<1024;x+=37){
    ctx.beginPath();
    ctx.moveTo(x,0);
    ctx.lineTo(x,1024);
    ctx.stroke();
  }
  ctx.fillStyle = "rgba(255,213,106,.11)";
  ctx.fillRect(0,0,1024,18);
  ctx.fillRect(0,1006,1024,18);
  const varnish = ctx.createRadialGradient(512,560,220,512,560,700);
  varnish.addColorStop(0,"rgba(255,255,255,.08)");
  varnish.addColorStop(1,"rgba(255,255,255,0)");
  ctx.fillStyle = varnish;
  ctx.fillRect(0,0,1024,1024);
  const traffic = [[510,695,240,120],[510,420,280,130],[510,530,150,90]];
  traffic.forEach(([x,y,w,h])=>{
    ctx.fillStyle = "rgba(44,27,15,.08)";
    ctx.fillRect(x-w/2,y-h/2,w,h);
    for(let i=0;i<44;i++){
      ctx.strokeStyle = `rgba(25,18,14,${0.02+Math.random()*0.05})`;
      ctx.lineWidth = 1+Math.random()*2;
      ctx.beginPath();
      ctx.moveTo(x-w/2+Math.random()*w,y-h/2+Math.random()*h);
      ctx.lineTo(x-w/2+Math.random()*w,y-h/2+Math.random()*h);
      ctx.stroke();
    }
  });
  drawPattern(ctx,theme.theme || "wave",{primary:parseInt(theme.woodA.replace('#',''),16),secondary:parseInt(theme.woodB.replace('#',''),16),trim:theme.trimColor},{x:190,y:188,w:644,h:644});
  const tex = new THREE.CanvasTexture(canvas);
  tex.wrapS = tex.wrapT = THREE.RepeatWrapping;
  tex.repeat.set(1.1,1.6);
  tex.anisotropy = 8;
  return tex;
}

function addCourtLogos(theme=courts[selectedCourt]){
  const logoCanvas = document.createElement("canvas");
  logoCanvas.width = 512; logoCanvas.height = 512;
  const ctx = logoCanvas.getContext("2d");
  ctx.clearRect(0,0,512,512);
  ctx.fillStyle = "rgba(16,34,64,.9)";
  ctx.beginPath(); ctx.arc(256,256,160,0,Math.PI*2); ctx.fill();
  drawPattern(ctx,theme.theme,{primary:parseInt(theme.woodA.replace('#',''),16),secondary:parseInt(theme.woodB.replace('#',''),16),trim:theme.trimColor},{x:120,y:120,w:272,h:272});
  ctx.strokeStyle = toHex(theme.trimColor);
  ctx.lineWidth = 12;
  ctx.beginPath(); ctx.arc(256,256,150,0,Math.PI*2); ctx.stroke();
  ctx.fillStyle = "#f2f6ff";
  ctx.font = "bold 46px Arial";
  ctx.textAlign = "center";
  ctx.fillText(theme.logoText,256,248);
  ctx.font = "bold 22px Arial";
  ctx.fillText((theme.style || theme.theme || "ARENA").toUpperCase(),256,292);
  const logoTex = new THREE.CanvasTexture(logoCanvas);
  const logo = new THREE.Mesh(
    new THREE.PlaneGeometry(3.8,3.8),
    new THREE.MeshStandardMaterial({map:logoTex,transparent:true,opacity:.96,emissive:theme.trimColor,emissiveIntensity:.18})
  );
  logo.rotation.x = -Math.PI/2;
  logo.position.set(0,.03,.2);
  scene.add(logo);
}

function addFloorScuffs(){
  const scuffGeo = new THREE.PlaneGeometry(1.2,1.2);
  for(let i=0;i<26;i++){
    const dark = i%3===0;
    const m = new THREE.Mesh(
      scuffGeo,
      new THREE.MeshBasicMaterial({color:dark?0x2a1808:0x4b2d14,transparent:true,opacity:dark?.11:.08})
    );
    m.rotation.x = -Math.PI/2;
    m.rotation.z = Math.random()*Math.PI;
    const nearRimBias = i < 12 ? -9.9 + Math.random()*3.1 : i<20 ? -6.8 + Math.random()*4.8 : -1.5 + Math.random()*3.8;
    m.position.set((Math.random()-.5)*10.4,.026,nearRimBias);
    scene.add(m);
  }
}

function addGlowingCourtLines(){
  const mat = new THREE.MeshBasicMaterial({color:0xbbe7ff,transparent:true,opacity:.42});
  const lines = [
    new THREE.Mesh(new THREE.BoxGeometry(12,.02,.06),mat),
    new THREE.Mesh(new THREE.BoxGeometry(5.2,.02,.06),mat),
    new THREE.Mesh(new THREE.BoxGeometry(.06,.02,4.25),mat),
    new THREE.Mesh(new THREE.BoxGeometry(.06,.02,4.25),mat)
  ];
  lines[0].position.set(0,.08,-6.7);
  lines[1].position.set(0,.08,-10.8);
  lines[2].position.set(-COURT_W/2+1.35,.08,-10.2);
  lines[3].position.set(COURT_W/2-1.35,.08,-10.2);
  lines.forEach(l=>scene.add(l));
}

function applyCourtTheme(courtKey){
  const theme = courts[courtKey] || courts.classicArena;
  if(arenaThemeRefs.court && arenaThemeRefs.court.material){
    arenaThemeRefs.court.material.map = makeImprovedCourtTexture(theme);
    const glossy = graphicsQuality === "high" ? {roughness:.2,metalness:.24} : graphicsQuality === "medium" ? {roughness:.26,metalness:.18} : {roughness:.4,metalness:.1};
    arenaThemeRefs.court.material.roughness = glossy.roughness;
    arenaThemeRefs.court.material.metalness = glossy.metalness;
    if(theme.theme === "neon" || theme.theme === "cyber" || theme.theme === "galaxy"){
      arenaThemeRefs.court.material.emissiveIntensity = .14;
    }else if(theme.theme === "blacktop"){
      arenaThemeRefs.court.material.roughness = Math.min(.45,arenaThemeRefs.court.material.roughness+.1);
    }else if(theme.theme === "beach"){
      arenaThemeRefs.court.material.color = new THREE.Color(0xfff1d3);
    }else{
      arenaThemeRefs.court.material.color = new THREE.Color(0xffffff);
      arenaThemeRefs.court.material.emissiveIntensity = .08;
    }
    arenaThemeRefs.court.material.needsUpdate = true;
  }
  arenaThemeRefs.lineMats.forEach(m=>m.color.setHex(theme.lineColor));
  arenaThemeRefs.trimMats.forEach(m=>m.color.setHex(theme.trimColor));
  arenaThemeRefs.crowdMats.forEach((m,i)=>{
    if(m.color && i%7===0) m.color.setHex(theme.trimColor);
  });
  if(scene && scene.fog) scene.fog.color.setHex(theme.fog || 0x08111c);
  if(scene && scene.background) scene.background.setHex(theme.fog || 0x08111c);
  if(arenaMoodLight){
    arenaMoodLight.color.setHex(theme.mood || theme.trimColor || 0x65d9ff);
    arenaMoodLight.intensity = .22 + arenaExcitement*.38;
  }
  updateLEDBoards(`${theme.logoText} • ${theme.style || theme.theme || "ARENA"}`);
}

function buildHoop(){
  const poleMat = new THREE.MeshStandardMaterial({color:0x555b63,metalness:.55,roughness:.28});
  const hoopBaseZ = HOOP_POS.z - 1.65;

  const pole = new THREE.Mesh(new THREE.CylinderGeometry(.13,.145,4.4,24),poleMat);
  pole.position.set(HOOP_POS.x,2.2,hoopBaseZ);
  pole.castShadow = true;
  scene.add(pole);

  const arm = new THREE.Mesh(new THREE.BoxGeometry(.24,.22,1.5),poleMat);
  arm.position.set(HOOP_POS.x,HOOP_POS.y + 0.6,HOOP_POS.z - 1.05);
  arm.castShadow = true;
  scene.add(arm);

  backboard = new THREE.Mesh(
    new THREE.BoxGeometry(3.4,2.05,.1),
    new THREE.MeshPhysicalMaterial({
      color:0xdff6ff,transparent:true,opacity:.34,roughness:.12,metalness:0,
      transmission:.15,clearcoat:.75,clearcoatRoughness:.08
    })
  );
  backboard.position.set(HOOP_POS.x,HOOP_POS.y + 0.22,HOOP_POS.z - 0.43);
  backboard.castShadow = true;
  backboardBasePosition.copy(backboard.position);
  scene.add(backboard);

  rim = new THREE.Mesh(
    new THREE.TorusGeometry(RIM_RADIUS,.06,16,72),
    new THREE.MeshStandardMaterial({color:0xff6f1a,metalness:.72,roughness:.22})
  );
  rim.rotation.x = Math.PI/2;
  rim.position.copy(HOOP_POS);
  rimBasePosition = rim.position.clone();
  scene.add(rim);

  const rimBracket = new THREE.Mesh(new THREE.BoxGeometry(.34,.14,.5),new THREE.MeshStandardMaterial({color:0x3f4650,metalness:.74,roughness:.3}));
  rimBracket.position.set(HOOP_POS.x,HOOP_POS.y-0.02,HOOP_POS.z-0.24); scene.add(rimBracket);
  const boardFrame = new THREE.Mesh(new THREE.BoxGeometry(3.56,2.2,.08),new THREE.MeshStandardMaterial({color:0xf8fbff,metalness:.28,roughness:.32}));
  boardFrame.position.copy(backboard.position); boardFrame.position.z -= 0.08; scene.add(boardFrame);

  const squareMat = new THREE.MeshBasicMaterial({color:0xffffff});
  const barThickness = 0.035;
  const squareW = 1.15;
  const squareH = 0.85;
  const squareZOffset = 0.055;
  const bars = [
    [squareW,barThickness,0,squareH*0.5,0],
    [squareW,barThickness,0,-squareH*0.5,0],
    [barThickness,squareH,-squareW*0.5,0,0],
    [barThickness,squareH,squareW*0.5,0,0]
  ];
  bars.forEach(([w,h,x,y,z])=>{
    const bar = new THREE.Mesh(new THREE.BoxGeometry(w,h,0.01),squareMat);
    bar.position.set(x,y,squareZOffset + z);
    backboard.add(bar);
  });
  const rimMount = new THREE.Mesh(
    new THREE.BoxGeometry(0.55,0.35,0.08),
    new THREE.MeshStandardMaterial({color:0x222222,roughness:0.45,metalness:0.25})
  );
  rimMount.position.set(0,-0.24,0.09);
  backboard.add(rimMount);
  const polePad = new THREE.Mesh(new THREE.BoxGeometry(.48,1.05,.44),new THREE.MeshStandardMaterial({color:0x263d5f,roughness:.58}));
  polePad.position.set(HOOP_POS.x,.56,hoopBaseZ + 0.02); scene.add(polePad);
  // Clean single-piece hoop base; replaces old stacked step blocks.
  const basePad = new THREE.Mesh(new THREE.BoxGeometry(.8,.34,.62),new THREE.MeshStandardMaterial({color:0x16263d,roughness:.64}));
  basePad.position.set(HOOP_POS.x,.17,hoopBaseZ - 0.06); scene.add(basePad);

  netLines = [];
  const netMat = new THREE.LineBasicMaterial({color:0xffffff,transparent:true,opacity:.75});
  const netSegments = graphicsQuality === "high" ? 4 : graphicsQuality === "medium" ? 3 : 2;
  const netStrands = graphicsQuality === "low" ? 14 : 20;
  for(let i=0;i<netStrands;i++){
    const a = (i/netStrands)*Math.PI*2;
    const topR = RIM_RADIUS*1.01;
    const midR = RIM_RADIUS*0.72;
    const botR = RIM_RADIUS*0.5;
    const points = [];
    for(let j=0;j<=netSegments;j++){
      const t = j/netSegments;
      const rr = THREE.MathUtils.lerp(topR,botR,t);
      const y = HOOP_POS.y - 0.05 - t*(0.9 + 0.05*Math.sin(a*2));
      const x = Math.cos(a)*THREE.MathUtils.lerp(topR,midR,Math.min(1,t*1.3));
      const z = HOOP_POS.z + Math.sin(a)*rr;
      points.push(new THREE.Vector3(x,y,z));
    }
    const line = new THREE.Line(new THREE.BufferGeometry().setFromPoints(points),netMat);
    line.userData.netBasePoints = points.map(p=>p.clone());
    netLines.push(line);
    scene.add(line);
  }

  buildScoreboard();
}

function buildScoreboard(){
  const canvas = document.createElement("canvas");
  canvas.width = 512;
  canvas.height = 192;
  scoreboardCtx = canvas.getContext("2d");
  scoreboardTexture = new THREE.CanvasTexture(canvas);
  scoreboardPlane = new THREE.Mesh(
    new THREE.PlaneGeometry(4.2,1.55),
    new THREE.MeshBasicMaterial({map:scoreboardTexture,transparent:true})
  );
  scoreboardPlane.position.set(HOOP_POS.x,HOOP_POS.y + 2.92,HOOP_POS.z - 1.15);
  scene.add(scoreboardPlane);
  updateScoreboard();
}

function updateScoreboard(){
  if(!scoreboardCtx) return;
  const ctx = scoreboardCtx;
  ctx.clearRect(0,0,512,192);
  ctx.fillStyle = "#050505";
  ctx.fillRect(0,0,512,192);
  ctx.strokeStyle = "#ffd56a";
  ctx.lineWidth = 8;
  ctx.strokeRect(10,10,492,172);
  ctx.fillStyle = "#151515";
  ctx.fillRect(24,26,464,140);

  ctx.fillStyle = "#ff3333";
  ctx.font = "900 72px 'Courier New', monospace";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.shadowColor = "#ff3333";
  ctx.shadowBlur = 18;
  ctx.fillText(`${scoreP1}  -  ${scoreP2}`,256,92);

  ctx.shadowBlur = 0;
  ctx.fillStyle = "#ffd56a";
  ctx.font = "bold 24px 'Courier New', monospace";
  ctx.fillText("P1      P2",256,145);
  ctx.font = "bold 18px Arial";
  ctx.fillStyle = "#8ce1ff";
  ctx.fillText(`First to ${targetScore}, win by ${winBy}`,256,170);
  const gamePoint = (scoreP1 >= targetScore-1 || scoreP2 >= targetScore-1) && Math.abs(scoreP1-scoreP2) >= 1;
  if(gamePoint){
    ctx.fillStyle = "#ffcf44";
    ctx.font = "bold 20px Arial";
    ctx.fillText(scoreP1 > scoreP2 ? "P1 GAME POINT" : "P2 GAME POINT",256,28);
    triggerCrowdReaction("game_point",0.75);
  }

  scoreboardTexture.needsUpdate = true;
}

function buildBall(){
  const ballMat = new THREE.MeshStandardMaterial({
    color:0xc96522,
    roughness:0.78,
    metalness:0.03,
    emissive:0x2a1308,
    emissiveIntensity:0.06
  });
  const pebbleMat = new THREE.MeshStandardMaterial({color:0x8b4920,roughness:.9,metalness:0,transparent:true,opacity:.2});
  ball = new THREE.Mesh(
    new THREE.SphereGeometry(BALL_RADIUS,40,40),
    ballMat
  );
  ball.castShadow = true;
  ball.receiveShadow = true;
  scene.add(ball);

// Ground shadow under the ball
ballShadow = new THREE.Mesh(
  new THREE.CircleGeometry(BALL_RADIUS * 2.15, 32),
  new THREE.MeshBasicMaterial({
    color: 0x000000,
    transparent: true,
    opacity: 0.36,
    depthWrite: false
  })
);
ballShadow.rotation.x = -Math.PI / 2;
ballShadow.position.set(0, 0.018, 0);
scene.add(ballShadow);

  const pebble = new THREE.Mesh(new THREE.SphereGeometry(BALL_RADIUS*1.004,28,28),pebbleMat);
  ball.add(pebble);

  addBasketballSeams(ball);
  lastBallVisualPos.copy(ball.position);
}

function addBasketballSeams(ballMesh){
  const seamMat = new THREE.MeshStandardMaterial({color:0x1a120d,roughness:0.68,metalness:0.02});
  const tubeRadius = Math.max(0.0055, BALL_RADIUS * 0.028);
  const ringRadius = BALL_RADIUS * 1.004;

  const makeRing = (rx, ry, rz) => {
    const ring = new THREE.Mesh(
      new THREE.TorusGeometry(ringRadius, tubeRadius, 10, 128),
      seamMat
    );
    ring.rotation.set(rx, ry, rz);
    ring.castShadow = false;
    ring.receiveShadow = true;
    ballMesh.add(ring);
    return ring;
  };

  makeRing(Math.PI / 2, 0, 0);
  makeRing(0, Math.PI / 2, 0);
  makeRing(Math.PI / 2, Math.PI / 4, 0);
  makeRing(Math.PI / 2, -Math.PI / 4, 0);
}

function makeCirclePoints(r,count){
  const pts = [];
  for(let i=0;i<=count;i++){
    const a = i/count * Math.PI*2;
    pts.push(new THREE.Vector3(Math.cos(a)*r,Math.sin(a)*r,0));
  }
  return pts;
}

function createArcLine(){
  arcLine = new THREE.Line(
    new THREE.BufferGeometry(),
    new THREE.LineBasicMaterial({color:0x00ffdd,transparent:true,opacity:.8})
  );
  arcLine.visible = false;
  scene.add(arcLine);
}

function createCharacter(profile,pos,id){
  const group = new THREE.Group();
  group.position.copy(pos);

  const jerseyKey = id === "p1" ? selectedP1Jersey : selectedP2Jersey;
  const jerseyStyle = jerseys[jerseyKey] || jerseys.classicBlue;
  const skin = new THREE.MeshStandardMaterial({color:profile.skin,roughness:.62});
  const jerseyTex = makeJerseyTexture(jerseyStyle,profile.number || "23");
  const shortsTex = makeShortsTexture(jerseyStyle);
  const jersey = new THREE.MeshStandardMaterial({color:jerseyStyle.primary,map:jerseyTex,roughness:.55});
  const shorts = new THREE.MeshStandardMaterial({color:jerseyStyle.secondary,map:shortsTex,roughness:.65});
  const sneakerKey = id === "p1" ? selectedP1Sneaker : selectedP2Sneaker;
  const sneaker = sneakers[sneakerKey] || sneakers.classic;
  const shoeTex = makeShoeTexture(sneaker);
  const shoeMat = new THREE.MeshStandardMaterial({color:sneaker.primary,map:shoeTex,roughness:.35});

  const h = profile.height;
  const w = profile.width;
  const nameTag = (profile.name || "").toLowerCase();
  const isGuard = nameTag.includes("guard") || nameTag.includes("flash");
  const isShooter = nameTag.includes("range") || nameTag.includes("shooter");
  const isBruiser = nameTag.includes("bruiser") || nameTag.includes("paint");
  const isTower = nameTag.includes("tower") || profile.height > 1.15;
  const isDunker = nameTag.includes("reaper") || nameTag.includes("dunker");
  const isClaw = nameTag.includes("claw");
  const shoulderScale = isBruiser ? 1.15 : isDunker ? 1.08 : isGuard ? 0.9 : isShooter ? 0.94 : isTower ? 1.06 : 1;
  const torsoLean = isShooter ? 1.03 : isGuard ? 0.96 : 1;
  const limbLong = isTower ? 1.12 : isClaw ? 1.08 : 1;
  const legSpring = isDunker ? 1.08 : isGuard ? 0.96 : 1;

  const body = new THREE.Mesh(new THREE.CapsuleGeometry(.32*w*shoulderScale,.74*h*torsoLean,8,18),jersey);
  body.position.y = 1.3*h;
  body.castShadow = true;
  body.scale.set(1,1.02,isBruiser ? 1.08 : .98);
  group.add(body);
  const torso = new THREE.Mesh(new THREE.CylinderGeometry(.27*w*shoulderScale,.38*w*(isBruiser?1.05:1),.62*h*torsoLean,16),jersey);
  torso.position.y = 1.15*h;
  torso.scale.set(1,.95,isBruiser?1.1:1);
  group.add(torso);
  const neck = new THREE.Mesh(new THREE.CylinderGeometry(.08*w,.09*w,.12*h,12),skin);
  neck.position.y = 1.88*h;
  group.add(neck);

  const head = new THREE.Mesh(new THREE.SphereGeometry(.27*w,20,20),skin);
  head.position.y = 2.12*h;
  const headWidth = isBruiser || isTower ? 1.02 : isGuard ? 0.86 : 0.92;
  head.scale.set(headWidth,1.06,isGuard ? 0.86 : 0.92);
  head.castShadow = true;
  group.add(head);
  const earGeom = new THREE.SphereGeometry(.05*w,10,10);
  const leftEar = new THREE.Mesh(earGeom,skin); leftEar.scale.set(.7,1,.55); leftEar.position.set(-.23*w,2.12*h,.01); group.add(leftEar);
  const rightEar = leftEar.clone(); rightEar.position.x = .23*w; group.add(rightEar);
  const nose = new THREE.Mesh(new THREE.CylinderGeometry(.022*w,.03*w,.08*w,8),new THREE.MeshStandardMaterial({color:profile.skin,roughness:.5}));
  nose.rotation.x = Math.PI/2; nose.position.set(0,2.09*h,.24*w); group.add(nose);
  const eyeMat = new THREE.MeshStandardMaterial({color:0x101217,roughness:.2});
  const browMat = new THREE.MeshStandardMaterial({color:0x231a16,roughness:.45});
  const eyeL = new THREE.Mesh(new THREE.SphereGeometry(.022*w,8,8),eyeMat); eyeL.scale.set(1,.8,.7); eyeL.position.set(-.08*w,2.12*h,.23*w); group.add(eyeL);
  const eyeR = eyeL.clone(); eyeR.position.x = .08*w; group.add(eyeR);
  const browL = new THREE.Mesh(new THREE.BoxGeometry(.09*w,.012*h,.03*w),browMat); browL.position.set(-.08*w,2.16*h,.23*w); browL.rotation.x = -.15; group.add(browL);
  const browR = browL.clone(); browR.position.x = .08*w; group.add(browR);

  const hair = createHair(profile.hairStyle || "shortFade",profile.hair,w,h);
  hair.position.y = 2.06*h;
  group.add(hair);

  const num = makeTextSprite(profile.number,"white",80);
  num.position.set(0,1.43*h,-.38*w);
  num.scale.set(.45,.22,.22);
  group.add(num);

  const leftArm = makeLimb(.118*w,.52*h*limbLong,skin,.095*w*shoulderScale);
  leftArm.position.set(-.42*w*shoulderScale,1.52*h,-.01);
  leftArm.rotation.z = -.35;
  group.add(leftArm);
  const leftShoulder = new THREE.Mesh(new THREE.SphereGeometry(.09*w,10,10),jersey);
  leftShoulder.position.set(-.35*w*shoulderScale,1.57*h,0); leftShoulder.scale.set(1.2,.9,1); group.add(leftShoulder);

  const rightArm = makeLimb(.118*w,.52*h*limbLong,skin,.095*w*shoulderScale);
  rightArm.position.set(.42*w*shoulderScale,1.52*h,-.01);
  rightArm.rotation.z = .35;
  group.add(rightArm);
  const rightShoulder = leftShoulder.clone(); rightShoulder.position.x = .35*w*shoulderScale; group.add(rightShoulder);

  const leftForearm = makeLimb(.088*w,.46*h*limbLong,skin,.07*w);
  leftForearm.position.set(-.56*w,1.12*h,.02);
  group.add(leftForearm);

  const rightForearm = makeLimb(.088*w,.46*h*limbLong,skin,.07*w);
  rightForearm.position.set(.56*w,1.12*h,.02);
  group.add(rightForearm);

  const leftHand = new THREE.Mesh(new THREE.SphereGeometry(.11*w,14,14),skin);
  leftHand.position.set(-.65*w,.72*h,.07);
  leftHand.scale.set(1.2,.72,.9);
  group.add(leftHand);
  const leftThumb = new THREE.Mesh(new THREE.CapsuleGeometry(.018*w,.04*w,6,8),skin);
  leftThumb.position.set(-.71*w,.72*h,.11*w); leftThumb.rotation.z = .7; group.add(leftThumb);

  const rightHand = new THREE.Mesh(new THREE.SphereGeometry(.11*w,14,14),skin);
  rightHand.position.set(.65*w,.72*h,.07);
  rightHand.scale.set(1.2,.72,.9);
  group.add(rightHand);
  const rightThumb = leftThumb.clone(); rightThumb.position.x = .71*w; rightThumb.rotation.z = -.7; group.add(rightThumb);

  const leftLeg = makeLimb(.14*w,.42*h*legSpring,shorts,.102*w);
  leftLeg.position.set(-.19*w,.66*h,0);
  group.add(leftLeg);
  const leftKnee = new THREE.Mesh(new THREE.SphereGeometry(.075*w,10,10),new THREE.MeshStandardMaterial({color:profile.skin,roughness:.62}));
  leftKnee.position.set(-.18*w,.41*h,.02); leftKnee.scale.set(1,.75,1); group.add(leftKnee);
  const leftCalf = makeLimb(.102*w,.4*h*legSpring,new THREE.MeshStandardMaterial({color:profile.skin,roughness:.62}),.08*w);
  leftCalf.position.set(-.19*w,.29*h,.02); group.add(leftCalf);

  const rightLeg = makeLimb(.14*w,.42*h*legSpring,shorts,.102*w);
  rightLeg.position.set(.19*w,.66*h,0);
  group.add(rightLeg);
  const rightKnee = leftKnee.clone(); rightKnee.position.x = .19*w; group.add(rightKnee);
  const rightCalf = leftCalf.clone(); rightCalf.position.x = .19*w; group.add(rightCalf);

  const leftShoe = new THREE.Mesh(new THREE.BoxGeometry(.36*w,.14,.54*w),shoeMat);
  leftShoe.position.set(-.19*w,.085,.14*w);
  group.add(leftShoe);

  const rightShoe = leftShoe.clone();
  rightShoe.position.x = .19*w;
  group.add(rightShoe);
  const hookIndicator = createHookIndicator();
  group.add(hookIndicator);

  addShoeDetails({profile,group,leftShoe,rightShoe,sneaker});
  addJerseyTrim({profile,group,w,h,jerseyStyle});
  addPlayerAccessories({profile,group,w,h,leftArm,rightArm,leftLeg,rightLeg});

  const shadow = new THREE.Mesh(
    new THREE.CircleGeometry(.82*w,36),
    new THREE.MeshBasicMaterial({color:0x000000,transparent:true,opacity:.34,depthWrite:false})
  );
  shadow.rotation.x = -Math.PI/2;
  shadow.position.y = .012;
  group.add(shadow);

  const player = {
    id, group, profile, sneaker, jerseyStyle, hookIndicator,
    velocity:new THREE.Vector3(),
    angle:Math.PI,
    yVel:0,
    onGround:true,
    jumpCooldown:0,
    sprint:1,
    bodyRadius:.55*w,
    leftArm,rightArm,leftForearm,rightForearm,leftHand,rightHand,leftLeg,rightLeg,leftShoe,rightShoe,torso,head,shadow,
    hasBall:id==="p1",
    dribbleBounce:0,
    swipeSide:null,
    swipeTime:0,
    swipeReachScale:1,
    blockTime:0,
    handsUpTime:0,
    shotMotionTime:0,
    blockBoost:false,
    moveState:"normal",
    moveTimer:0,
    moveDuration:0,
    moveMeta:{},
    stepBackQuality:0,
    pumpFakeCooldown:0,
    sideHopCooldown:0,
    stumbledTimer:0,
    confidence:0,
    hotStreak:0,
    lastLeftTapTime:-99,
    lastRightTapTime:-99,
    tapTimes:{KeyQ:-99,KeyE:-99,KeyU:-99,KeyO:-99,Numpad4:-99,Numpad6:-99},
    lastPumpFakeTime:-99,
    debugLeftInputTimer:0,
    debugRightInputTimer:0,
    defensiveShadeSide:null,
    defensiveShadeTime:0,
    defensiveContainBonus:0,
    defensiveContainLabelTime:0,
    pendingStealTap:null,
    pendingStealSide:null,
    pendingStealScreenSide:null,
    pendingStealCode:null,
    contactType:null,
    contactSlide:0,
    contactTimer:0,
    contactStrength:0,
    contactSide:0,
    contactFinishBonusOrPenalty:0,
    bodyUpTimer:0,
    lastContestLabel:"Open",
    anim:{runCycle:0,lean:0,targetLean:0,armBlend:0,legBlend:0,walkBlend:0,sprintBlend:0,backpedalBlend:0,turnLean:0,verticalBob:0,landingTimer:0,strideStrength:0,torsoTwist:0,landingSquash:0,contactReact:0,shotFollowThrough:0,sideLean:0,legKick:0,fadeLean:0,hookWindup:0,hookRelease:0,defenseStanceBlend:0,shuffleCycle:0,cutoffLean:0,cutPlant:0,cutDir:0,lookYaw:0,headYaw:0},
    takeover:0,takeoverActive:false,takeoverTimer:0,stamina:1,dribbleStyle:"normal",stats:{fga:0,fgm:0,tpa:0,tpm:0,blocks:0,steals:0,turnovers:0,ankle:0,perfect:0},
    movement:{
      acceleration:5.2 * (1 + (profile.speed-1)*.7),
      lateralSpeed:3.4 * (1 + (profile.speed-1)*.52 + (profile.handle-1)*.23),
      turnSpeed:2.6 * (1 + (profile.handle-1)*.2),
      contactStrength:1 + (profile.width-1)*1.3 + (profile.block-1)*.2,
      defensiveWidth:.95 + (profile.width-1)*1.5,
      stopSpeed:1 + (profile.shot-1)*.4 + (profile.handle-1)*.2,
      recoverySpeed:1 + (profile.steal-1)*.3 + (profile.speed-1)*.25
    }
  };
  player.shotIndicator = createFloatingIndicator(player,"SHOT","#ffffff",.42);
  player.defenseIndicator = createFloatingIndicator(player,"CONTAIN","#a5e4ff",.4);
  player.staminaIndicator = createStaminaIndicator();
  player.group.add(player.staminaIndicator);
  if(sneaker.sprintRecover) player.movement.recoverySpeed *= (1 + sneaker.sprintRecover);
  if(sneaker.defense) player.movement.defensiveWidth *= (1 + sneaker.defense);
  if(sneaker.jump) player.profile.jump *= (1 + sneaker.jump);
  return player;
}

function makeLimb(radius,length,mat){
  const taper = arguments[3];
  const mesh = new THREE.Mesh(new THREE.CapsuleGeometry(radius,length,8,16),mat);
  if(taper){
    const taperBand = new THREE.Mesh(new THREE.CylinderGeometry(taper,radius,length*.72,10),mat);
    taperBand.position.y = -length*.06;
    mesh.add(taperBand);
  }
  mesh.castShadow = true;
  return mesh;
}

function createHair(style,color,w,h){
  const group = new THREE.Group();
  const mat = new THREE.MeshStandardMaterial({color,roughness:.7});
  if(style === "afro"){
    for(let i=0;i<5;i++){
      const puff = new THREE.Mesh(new THREE.SphereGeometry((.16 + (i===0?.05:0))*w,12,12),mat);
      const ring = i===0 ? 0 : .16*w;
      puff.position.set(i===0?0:Math.cos(i*1.25)*ring,.01*h + (i===0?.06*h:0),i===0?0:Math.sin(i*1.25)*ring);
      puff.scale.set(1,.88,1);
      group.add(puff);
    }
  }else if(style === "braids"){
    const cap = new THREE.Mesh(new THREE.SphereGeometry(.24*w,14,14),mat);
    cap.scale.set(.98,.52,.98); group.add(cap);
    for(let i=0;i<7;i++){
      const braid = new THREE.Mesh(new THREE.CylinderGeometry(.022*w,.016*w,.3*h,7),mat);
      braid.position.set((i-3)*.06*w,-.1*h,-.14*w + Math.sin(i*.8)*.03*w);
      braid.rotation.x = 0.5;
      group.add(braid);
    }
  }else if(style === "spiky"){
    const cap = new THREE.Mesh(new THREE.SphereGeometry(.22*w,12,12),mat); cap.scale.set(1,.42,1); group.add(cap);
    for(let i=0;i<10;i++){
      const spike = new THREE.Mesh(new THREE.ConeGeometry(.045*w,.17*h,7),mat);
      spike.position.set(Math.sin(i*.64)*.16*w,.07*h + (i%2?.015*h:0),Math.cos(i*.64)*.13*w);
      spike.rotation.x = (i%3-1)*0.12;
      group.add(spike);
    }
  }else if(style === "buzz"){
    const cap = new THREE.Mesh(new THREE.SphereGeometry(.25*w,14,14),mat);
    cap.scale.set(.95,.2,.95); group.add(cap);
  }else if(style === "curlyTop"){
    for(let i=0;i<14;i++){
      const curl = new THREE.Mesh(new THREE.SphereGeometry(.06*w,8,8),mat);
      curl.position.set((Math.random()-.5)*.38*w,Math.random()*.16*h,(Math.random()-.5)*.36*w);
      group.add(curl);
    }
  }else if(style === "headband"){
    const cap = new THREE.Mesh(new THREE.SphereGeometry(.23*w,14,14),mat);
    cap.scale.set(.98,.45,.98); group.add(cap);
    const band = new THREE.Mesh(new THREE.TorusGeometry(.23*w,.022*w,8,30),new THREE.MeshStandardMaterial({color:0xffffff,roughness:.28}));
    band.rotation.x = Math.PI/2; band.position.y = -.025*h; group.add(band);
  }else{
    const cap = new THREE.Mesh(new THREE.SphereGeometry(.27*w,16,16),mat);
    cap.scale.set(.94,.42,.94); group.add(cap);
  }
  return group;
}

function makeTextSprite(text,color,size){
  const canvas = document.createElement("canvas");
  canvas.width = 256;
  canvas.height = 128;
  const ctx = canvas.getContext("2d");
  drawTextSprite(canvas,ctx,text,color,size);
  const tex = new THREE.CanvasTexture(canvas);
  const sprite = new THREE.Sprite(new THREE.SpriteMaterial({map:tex,transparent:true}));
  sprite.userData.canvas = canvas;
  sprite.userData.ctx = ctx;
  sprite.userData.fontSize = size;
  sprite.userData.color = color;
  return sprite;
}

function drawTextSprite(canvas,ctx,text,color,size){
  ctx.clearRect(0,0,canvas.width,canvas.height);
  ctx.font = `900 ${size}px Arial`;
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillStyle = "rgba(7,13,24,.75)";
  ctx.fillText(text,128,72);
  ctx.fillStyle = color;
  ctx.fillText(text,128,68);
}

function setSpriteText(sprite,text,color,size){
  if(!sprite || !sprite.userData || !sprite.userData.ctx) return;
  const canvas = sprite.userData.canvas;
  const ctx = sprite.userData.ctx;
  const finalColor = color || sprite.userData.color || "#ffffff";
  const finalSize = size || sprite.userData.fontSize || 82;
  drawTextSprite(canvas,ctx,text,finalColor,finalSize);
  sprite.userData.color = finalColor;
  sprite.userData.fontSize = finalSize;
  if(sprite.material && sprite.material.map) sprite.material.map.needsUpdate = true;
}

function createFloatingIndicator(player,type="SHOT",color="#ffffff",scale=.44){
  const sprite = makeTextSprite(type,color,82);
  sprite.scale.set(scale,scale*.52,scale*.52);
  sprite.material.opacity = 0;
  sprite.material.depthTest = false;
  sprite.visible = false;
  player.group.add(sprite);
  return sprite;
}

function createStaminaIndicator(){
  const c = document.createElement("canvas"); c.width = 128; c.height = 128;
  const ctx = c.getContext("2d");
  const tex = new THREE.CanvasTexture(c);
  const sprite = new THREE.Sprite(new THREE.SpriteMaterial({map:tex,transparent:true,opacity:.7,depthWrite:false}));
  sprite.userData.canvas = c;
  sprite.userData.ctx = ctx;
  sprite.scale.set(.28,.28,.28);
  return sprite;
}

function drawStaminaIndicator(char){
  if(!char || !char.staminaIndicator) return;
  const c = char.staminaIndicator.userData.canvas;
  const ctx = char.staminaIndicator.userData.ctx;
  const st = THREE.MathUtils.clamp(char.stamina,0,1);
  const hue = 120 * st;
  ctx.clearRect(0,0,c.width,c.height);
  ctx.strokeStyle = "rgba(15,24,36,.8)";
  ctx.lineWidth = 14;
  ctx.beginPath(); ctx.arc(64,64,42,0,Math.PI*2); ctx.stroke();
  ctx.strokeStyle = `hsl(${hue},95%,56%)`;
  ctx.lineWidth = 12;
  ctx.beginPath(); ctx.arc(64,64,42,-Math.PI/2,-Math.PI/2 + Math.PI*2*st); ctx.stroke();
  ctx.fillStyle = `hsl(${hue},95%,62%)`;
  ctx.beginPath(); ctx.arc(64,64,7,0,Math.PI*2); ctx.fill();
  char.staminaIndicator.material.map.needsUpdate = true;
}

function addPlayerAccessories(char){
  const {profile,group,w,h,leftArm,rightArm,leftLeg,rightLeg} = char;
  const bandMat = new THREE.MeshStandardMaterial({color:0xffffff,roughness:.35});
  const leftBand = new THREE.Mesh(new THREE.TorusGeometry(.12*w,.03*w,8,24),bandMat);
  leftBand.position.set(-.48*w,1.2*h,0); leftBand.rotation.y = Math.PI/2; group.add(leftBand);
  const rightBand = leftBand.clone(); rightBand.position.x = .48*w; group.add(rightBand);
  const sockMat = new THREE.MeshStandardMaterial({color:profile.name.includes("Sky")||profile.name.includes("Tower")?0xe8f6ff:0xdadada,roughness:.55});
  const lSock = new THREE.Mesh(new THREE.CylinderGeometry(.11*w,.11*w,.22*h,10),sockMat);
  lSock.position.set(-.18*w,.24*h,.02); group.add(lSock);
  const rSock = lSock.clone(); rSock.position.x=.18*w; group.add(rSock);
  if(profile.name.includes("Flash")){
    const headband = new THREE.Mesh(new THREE.TorusGeometry(.24*w,.03*w,8,28),new THREE.MeshStandardMaterial({color:0xffffff}));
    headband.position.y = 2.14*h; headband.rotation.x = Math.PI/2; group.add(headband);
  }
  if(profile.name.includes("Claw") || profile.name.includes("Crafty") || profile.name.includes("Smooth")){
    const sleeveMat = new THREE.MeshStandardMaterial({color:0x111820,roughness:.48});
    const sleeve = new THREE.Mesh(new THREE.CylinderGeometry(.1*w,.1*w,.35*h,10),sleeveMat);
    sleeve.position.set(profile.name.includes("Crafty")?- .46*w:.46*w,1.18*h,0); group.add(sleeve);
  }
  addPlayerRimLight({group});
}

function addShoeDetails(char){
  const accent = char.sneaker && char.sneaker.accent ? char.sneaker.accent : 0xffffff;
  const soleColor = char.sneaker && char.sneaker.sole ? char.sneaker.sole : 0x111111;
  const highTop = char.sneaker && (char.sneaker.type === "high" || char.sneaker.type === "boost");
  [char.leftShoe,char.rightShoe].forEach((s)=>{
    const sole = new THREE.Mesh(new THREE.BoxGeometry(.32*char.profile.width,.04 + (char.sneaker.type==="boost"?0.02:0),.48*char.profile.width),new THREE.MeshStandardMaterial({color:soleColor}));
    sole.position.y = -.07; s.add(sole);
    const stripe = new THREE.Mesh(new THREE.BoxGeometry(.3*char.profile.width,.03,.08),new THREE.MeshStandardMaterial({color:accent,emissive:accent,emissiveIntensity:.2}));
    stripe.position.set(0,.05,.1); s.add(stripe);
    const sideStripe = new THREE.Mesh(new THREE.BoxGeometry(.03,.08,.3*char.profile.width),new THREE.MeshStandardMaterial({color:accent,roughness:.25}));
    sideStripe.position.set(.12*char.profile.width,.03,.02); s.add(sideStripe);
    const sideStripe2 = sideStripe.clone(); sideStripe2.position.x = -.12*char.profile.width; s.add(sideStripe2);
    const toe = new THREE.Mesh(new THREE.SphereGeometry(.1*char.profile.width,10,10),new THREE.MeshStandardMaterial({color:0xffffff,roughness:.25}));
    toe.position.set(0,.03,.22*char.profile.width); toe.scale.set(1,.65,1.2); s.add(toe);
    const laces = new THREE.Mesh(new THREE.BoxGeometry(.18*char.profile.width,.01,.16*char.profile.width),new THREE.MeshStandardMaterial({color:0xf5f5f5}));
    laces.position.set(0,.07,.02); s.add(laces);
    const heel = new THREE.Mesh(new THREE.BoxGeometry(.08*char.profile.width,.07,.05),new THREE.MeshStandardMaterial({color:accent,emissive:accent,emissiveIntensity:char.sneaker.type==="boost"?.35:.14}));
    heel.position.set(0,.04,-.18); s.add(heel);
    if(highTop){
      const collar = new THREE.Mesh(new THREE.CylinderGeometry(.14*char.profile.width,.14*char.profile.width,.09,10),new THREE.MeshStandardMaterial({color:accent,roughness:.4}));
      collar.position.y = .12;
      s.add(collar);
    }
    if(char.sneaker.type === "glow"){
      const glow = new THREE.Mesh(new THREE.PlaneGeometry(.18*char.profile.width,.22*char.profile.width),new THREE.MeshBasicMaterial({color:accent,transparent:true,opacity:.6,side:THREE.DoubleSide}));
      glow.position.set(0,.09,.05);
      s.add(glow);
    }
    if(char.sneaker.type === "boost"){
      const pod = new THREE.Mesh(new THREE.BoxGeometry(.2*char.profile.width,.03,.08),new THREE.MeshStandardMaterial({color:0x9ffcff,emissive:0x6fefff,emissiveIntensity:.4}));
      pod.position.set(0,-.055,.12);
      s.add(pod);
    }
  });
}

function addJerseyTrim(char){
  const accent = (char.jerseyStyle && char.jerseyStyle.trim) || char.profile.accent || 0xfff1b5;
  const trimMat = new THREE.MeshStandardMaterial({color:accent,emissive:accent,emissiveIntensity:.1});
  const secondary = (char.jerseyStyle && char.jerseyStyle.secondary) || 0xffffff;
  const pattern = char.jerseyStyle && char.jerseyStyle.pattern;
  const ring = new THREE.Mesh(new THREE.TorusGeometry(.35*char.w,.03,8,26),trimMat);
  ring.position.set(0,1.63*char.h,.01); ring.rotation.x = Math.PI/2; char.group.add(ring);
  const shoulderL = new THREE.Mesh(new THREE.BoxGeometry(.14*char.w,.08*char.h,.24*char.w),trimMat);
  shoulderL.position.set(-.34*char.w,1.58*char.h,0); char.group.add(shoulderL);
  const shoulderR = shoulderL.clone(); shoulderR.position.x = .34*char.w; char.group.add(shoulderR);
  const sideStripeL = new THREE.Mesh(new THREE.BoxGeometry(.04,.56*char.h,.04),trimMat);
  sideStripeL.position.set(-.36*char.w,1.14*char.h,.02); char.group.add(sideStripeL);
  const sideStripeR = sideStripeL.clone(); sideStripeR.position.x = .36*char.w; char.group.add(sideStripeR);
  const chestStripe = new THREE.Mesh(new THREE.BoxGeometry(.58*char.w,.05*char.h,.05),new THREE.MeshStandardMaterial({color:secondary,roughness:.3}));
  chestStripe.position.set(0,1.34*char.h,.38*char.w); char.group.add(chestStripe);
  const shortsTrim = new THREE.Mesh(new THREE.TorusGeometry(.26*char.w,.018,8,24),trimMat);
  shortsTrim.position.set(0,.84*char.h,.01); shortsTrim.rotation.x = Math.PI/2; char.group.add(shortsTrim);
  if(pattern === "neonLines" || pattern === "stars"){
    for(let i=0;i<3;i++){
      const p = new THREE.Mesh(new THREE.BoxGeometry(.03,.38*char.h,.02),new THREE.MeshStandardMaterial({color:secondary,emissive:secondary,emissiveIntensity:.25}));
      p.position.set(-.16*char.w + i*.16*char.w,1.2*char.h,.37*char.w); char.group.add(p);
    }
  }
  const chestLogo = makeTextSprite("★","#ffffff",74);
  chestLogo.position.set(0,1.3*char.h,.39*char.w);
  chestLogo.scale.set(.22,.12,.12);
  char.group.add(chestLogo);
  const backNum = makeTextSprite(char.profile.number || "0","#ffffff",84);
  backNum.position.set(0,1.33*char.h,-.41*char.w);
  backNum.scale.set(.28,.14,.14);
  char.group.add(backNum);
}

function addPlayerRimLight(char){
  const rim = new THREE.PointLight(0x8fd1ff,.45,4.4);
  rim.position.set(0,1.8,1.1);
  char.group.add(rim);
}

function createDebugArrow(color){
  return new THREE.ArrowHelper(new THREE.Vector3(0,0,1),new THREE.Vector3(),1,color,.28,.16);
}

function ensureDebugDirectionMeshes(){
  if(debugDirHelpers || !scene || !p1 || !p2) return;
  const group = new THREE.Group();
  const mk = ()=>({
    forward:createDebugArrow(0x34ff66),
    left:createDebugArrow(0x2d8cff),
    right:createDebugArrow(0xffdd44),
    reach:new THREE.Mesh(new THREE.SphereGeometry(.08,10,10),new THREE.MeshBasicMaterial({color:0xff3344}))
  });
  const p1d = mk();
  const p2d = mk();
  [p1d,p2d].forEach((d)=>{
    group.add(d.forward); group.add(d.left); group.add(d.right); group.add(d.reach);
  });
  group.visible = debugDirections;
  scene.add(group);
  debugDirHelpers = {group,p1:p1d,p2:p2d};
}

function updateDebugDirections(dt){
  if(!p1 || !p2) return;
  ensureDebugDirectionMeshes();
  p1.debugLeftInputTimer = Math.max(0,(p1.debugLeftInputTimer || 0) - dt);
  p1.debugRightInputTimer = Math.max(0,(p1.debugRightInputTimer || 0) - dt);
  p2.debugLeftInputTimer = Math.max(0,(p2.debugLeftInputTimer || 0) - dt);
  p2.debugRightInputTimer = Math.max(0,(p2.debugRightInputTimer || 0) - dt);
  if(!debugDirHelpers || !debugDirections) return;
  const updateChar = (char,dbg)=>{
    const chest = char.group.position.clone().add(new THREE.Vector3(0,1.2*char.profile.height,0));
    const forward = getForward(char.angle).clone().normalize();
    dbg.forward.position.copy(chest);
    dbg.forward.setDirection(forward);
    dbg.forward.setLength(1.3);
    const leftVec = getScreenSideVectorForChar(char,"left");
    const rightVec = getScreenSideVectorForChar(char,"right");
    dbg.left.position.copy(chest);
    dbg.left.setDirection(leftVec);
    dbg.left.setLength(char.debugLeftInputTimer > 0 ? 1.15 : .7);
    dbg.right.position.copy(chest);
    dbg.right.setDirection(rightVec);
    dbg.right.setLength(char.debugRightInputTimer > 0 ? 1.15 : .7);
    const reachPoint = getReachHitPoint(char,char.swipeSide || "right");
    dbg.reach.position.copy(reachPoint);
  };
  updateChar(p1,debugDirHelpers.p1);
  updateChar(p2,debugDirHelpers.p2);
}


function getActiveCameraForChar(char){
  if(twoPlayerMode && gameStarted) return char === p2 ? camP2 : camP1;
  return camMain;
}

function getScreenSideVectorForChar(char,screenSide){
  const cam = getActiveCameraForChar(char);
  const camDir = new THREE.Vector3();
  cam.getWorldDirection(camDir);
  camDir.y = 0;
  if(camDir.lengthSq() < .0001) camDir.set(0,0,-1);
  camDir.normalize();
  const worldUp = new THREE.Vector3(0,1,0);
  const screenRight = new THREE.Vector3().crossVectors(camDir,worldUp).normalize();
  return screenSide === "right" ? screenRight : screenRight.clone().multiplyScalar(-1);
}

function getRequestedShotType(char){
  if(char.id === "p1"){
    if(keys["KeyQ"] || keys["KeyE"] || keys["q"] || keys["e"]) return "jumper";
  }
  if(char.id === "p2"){
    if(keys["KeyU"] || keys["KeyO"] || keys["Numpad4"] || keys["Numpad6"] || keys["u"] || keys["o"]) return "jumper";
  }
  return "setshot";
}
function playerHasBall(char){
  if(!char) return false;
  return ballOwner === char.id && ballState === "dribble";
}
function canLayup(player){
  if(!player.hasBall || ballState !== "dribble") return false;
  const dist = flatDistance(player.group.position,HOOP_POS);
  const defender = player.id === "p1" ? p2 : p1;
  const hasBodyUp = !!(defender && defender.bodyUpTimer > 0.05 && flatDistance(defender.group.position,player.group.position) < 1.28);
  if(hasBodyUp) player.contactFinishBonusOrPenalty = THREE.MathUtils.clamp((player.profile.layup-1)*0.18 + (player.profile.width-defender.profile.width)*0.2,-0.22,0.12);
  return dist < 3.6 && player.velocity.length() > (hasBodyUp ? 0.85 : 1.2);
}
function chooseShotType(player){
  if(!playerHasBall(player) && player.moveMeta){
    player.moveMeta.fadeawayArmed = false;
  }
  if(player.moveState === "backjump" || (player.moveMeta && player.moveMeta.fadeawayArmed)) return "fadeaway";
  if(canDunk(player)) return "dunk";
  if(canHookShot(player)) return "hook";
  if(canLayup(player)) return "layup";
  return getRequestedShotType(player);
}

function getShotMeterProfile(shotType,shooter,contest){
  const movePenalty = THREE.MathUtils.clamp((shooter.velocity ? shooter.velocity.length() : 0) * 0.028,0,0.16);
  const contestPenalty = THREE.MathUtils.clamp((contest ? contest.value : 0) * 0.14,0,0.2);
  const base = {
    setshot:{greenStart:.5,greenEnd:.78,slightlyEarlyStart:.28,slightlyLateEnd:.94,greenAccuracyMultiplier:1.22,baseMakeBonus:.1,meterFillSpeed:1},
    jumper:{greenStart:.54,greenEnd:.74,slightlyEarlyStart:.32,slightlyLateEnd:.92,greenAccuracyMultiplier:1.2,baseMakeBonus:.06,meterFillSpeed:1.06},
    hook:{greenStart:.57,greenEnd:.71,slightlyEarlyStart:.35,slightlyLateEnd:.9,greenAccuracyMultiplier:1.15,baseMakeBonus:.05,meterFillSpeed:1.02},
    layup:{greenStart:.44,greenEnd:.8,slightlyEarlyStart:.24,slightlyLateEnd:.96,greenAccuracyMultiplier:1.08,baseMakeBonus:.08,meterFillSpeed:.9},
    dunk:{greenStart:.38,greenEnd:.86,slightlyEarlyStart:.18,slightlyLateEnd:.98,greenAccuracyMultiplier:1.05,baseMakeBonus:.18,meterFillSpeed:.85},
    stepback:{greenStart:.56,greenEnd:.72,slightlyEarlyStart:.34,slightlyLateEnd:.9,greenAccuracyMultiplier:1.18,baseMakeBonus:.05,meterFillSpeed:1.1},
    pullup:{greenStart:.55,greenEnd:.73,slightlyEarlyStart:.33,slightlyLateEnd:.9,greenAccuracyMultiplier:1.16,baseMakeBonus:.05,meterFillSpeed:1.08},
    floater:{greenStart:.5,greenEnd:.76,slightlyEarlyStart:.28,slightlyLateEnd:.94,greenAccuracyMultiplier:1.14,baseMakeBonus:.05,meterFillSpeed:.95},
    runner:{greenStart:.5,greenEnd:.76,slightlyEarlyStart:.28,slightlyLateEnd:.94,greenAccuracyMultiplier:1.12,baseMakeBonus:.05,meterFillSpeed:.95}
    ,fadeaway:{greenStart:.58,greenEnd:.70,slightlyEarlyStart:.36,slightlyLateEnd:.88,greenAccuracyMultiplier:1.15,baseMakeBonus:.035,meterFillSpeed:1.12}
  }[shotType] || {
    greenStart:.5,greenEnd:.76,slightlyEarlyStart:.3,slightlyLateEnd:.92,greenAccuracyMultiplier:1.16,baseMakeBonus:.05,meterFillSpeed:1
  };
  const shrink = movePenalty + contestPenalty;
  const greenCenter = (base.greenStart + base.greenEnd) * .5;
  const half = Math.max(.05,(base.greenEnd - base.greenStart)*.5 - shrink*.45);
  return {
    greenStart:THREE.MathUtils.clamp(greenCenter - half,0.08,0.9),
    greenEnd:THREE.MathUtils.clamp(greenCenter + half,0.1,0.95),
    slightlyEarlyStart:Math.max(0.04,base.slightlyEarlyStart - shrink*.3),
    slightlyLateEnd:Math.min(0.99,base.slightlyLateEnd + shrink*.15),
    greenAccuracyMultiplier:base.greenAccuracyMultiplier,
    baseMakeBonus:base.baseMakeBonus,
    meterFillSpeed:base.meterFillSpeed
  };
}

function calculateShotTiming(held,shotType="setshot",shooter=null,contest=null){
  const profile = getShotMeterProfile(shotType,shooter || {velocity:new THREE.Vector3()},contest || {value:0});
  const maxMeterTime = 1.35 / profile.meterFillSpeed;
  const pct = THREE.MathUtils.clamp(held / maxMeterTime, 0, 1);
  let grade = "Early";
  let accuracyMultiplier = .68;
  let color = 0xff3333;
  if(pct >= profile.slightlyEarlyStart && pct < profile.greenStart){
    grade = "Slightly Early"; accuracyMultiplier = .9; color = 0xffcc33;
  }else if(pct >= profile.greenStart && pct <= profile.greenEnd){
    grade = "Green Release"; accuracyMultiplier = profile.greenAccuracyMultiplier; color = 0x35ff6b;
  }else if(pct > profile.greenEnd && pct <= profile.slightlyLateEnd){
    grade = "Slightly Late"; accuracyMultiplier = .9; color = 0xffa533;
  }else if(pct > profile.slightlyLateEnd){
    grade = "Overheld"; accuracyMultiplier = .52; color = 0xff3333;
  }
  return {pct, grade, accuracyMultiplier, color, profile};
}

function getArcBonus(char,shotType){
  const held = char.moveMeta.chargeHeld || 0;
  const arcPct = THREE.MathUtils.clamp(held / 1.0, 0, 1);
  const maxArcBonus = shotType === "jumper" ? 3.4 : 1.8;
  const maxTotalArcBonus = 4.0;
  const rawArcBonus = THREE.MathUtils.lerp(0, maxArcBonus, smoothstep(arcPct));
  const heightBonus = Math.max(0, char.profile.height - 1) * 0.35;
  return Math.min(rawArcBonus + heightBonus, maxTotalArcBonus);
}

function getShotReleasePoint(char,shotType){
  if(shotType === "hook") return getHookReleasePoint(char);
  const forward = getForward(char.angle).normalize();
  const side = ballSideValue < 0 ? "left" : "right";
  const handWorld = getWorldHand(char,side);
  const jumpLift = shotType === "jumper" ? 0.26 + THREE.MathUtils.clamp((char.moveMeta.chargeHeld || 0) * 0.2,0,0.34) : 0.11;
  const releaseHeight = 0.26 + char.profile.height * 0.22 + Math.max(0,char.group.position.y) + jumpLift;
  return handWorld.clone()
    .add(new THREE.Vector3(0,releaseHeight,0))
    .add(forward.multiplyScalar(0.18 + (shotType === "jumper" ? 0.18 : 0.1)));
}

function isCloseDunkRange(char){
  return canStartDunk(char);
}

function getBetweenPlayerAndRimTarget(defender, ballHandler, spacing){
  const toHoop = HOOP_POS.clone().sub(ballHandler.group.position);
  toHoop.y = 0;
  if(toHoop.lengthSq() < .0001) return ballHandler.group.position.clone();
  toHoop.normalize();
  return ballHandler.group.position.clone().add(toHoop.multiplyScalar(spacing));
}

function getBodyRightVector(char){
  const f = getForward(char.angle);
  return new THREE.Vector3(f.z,0,-f.x).normalize();
}
function getPlayerForward(char){ return getForward(char.angle).clone().normalize(); }
function getPlayerRight(char){ return getBodyRightVector(char).clone().normalize(); }

function getBestHandForScreenSide(char,screenSide){
  const screenVec = getScreenSideVectorForChar(char,screenSide);
  const bodyRight = getBodyRightVector(char);
  return bodyRight.dot(screenVec) >= 0 ? "right" : "left";
}

function smoothAnimValue(value,target,speed,dt){
  return value + (target - value) * (1 - Math.exp(-speed * dt));
}
function clampLean(value,min,max){
  return THREE.MathUtils.clamp(value,min,max);
}

function getShotTimingQuality(held,shotType="setshot",shooter=null,contest=null){
  return THREE.MathUtils.clamp(calculateShotTiming(held,shotType,shooter,contest).accuracyMultiplier,0.45,1.28);
}
function getContestPenalty(shooter){
  return calculateShotContest(shooter).value * 0.34;
}
function getDistancePenalty(shooter){
  const dist = flatDistance(shooter.group.position,HOOP_POS);
  return THREE.MathUtils.clamp(Math.max(0,dist - 3.8) * 0.024,0,0.36);
}
function isInsideArc(player){
  const dx = player.group.position.x - HOOP_POS.x;
  const dz = player.group.position.z - HOOP_POS.z;
  return Math.hypot(dx,dz) < THREE_RADIUS;
}
function canHookShot(player){
  if(!player.hasBall || layupActive || dunkActive) return false;
  if(!isInsideArc(player)) return false;
  const flatPlayerPos = player.group.position.clone().setY(0);
  const flatRimPos = HOOP_POS.clone().setY(0);
  const toRim = flatRimPos.sub(flatPlayerPos).normalize();
  const forward = getPlayerForward(player).setY(0).normalize();
  const right = getPlayerRight(player).setY(0).normalize();
  const distToRim = player.group.position.distanceTo(HOOP_POS);
  const facingDot = forward.dot(toRim);
  const rimSide = Math.sign(right.dot(toRim));
  const inHookRange = distToRim <= 4.8 && distToRim > 1.45;
  const sidewaysDot = Math.abs(getPlayerRight(player).dot(toRim));
  const validSidewaysAngle = sidewaysDot >= Math.cos(THREE.MathUtils.degToRad(50));
  const ballOnOppositeArm =
    (rimSide > 0 && ballSideValue < 0) ||
    (rimSide < 0 && ballSideValue > 0);
  return inHookRange && validSidewaysAngle && ballOnOppositeArm;
}
function canStartHookShot(player){ return canHookShot(player); }
function createHookIndicator(){
  const c = document.createElement("canvas");
  c.width = 128; c.height = 128;
  const ctx = c.getContext("2d");
  ctx.clearRect(0,0,128,128);
  ctx.strokeStyle = "rgba(95,225,255,0.95)";
  ctx.lineWidth = 10;
  ctx.lineCap = "round";
  ctx.beginPath();
  ctx.arc(58,58,30,Math.PI*0.25,Math.PI*1.72);
  ctx.stroke();
  ctx.fillStyle = "rgba(255,235,140,0.95)";
  ctx.beginPath();
  ctx.moveTo(74,24); ctx.lineTo(104,36); ctx.lineTo(82,52);
  ctx.closePath();
  ctx.fill();
  const tex = new THREE.CanvasTexture(c);
  const mat = new THREE.SpriteMaterial({
    map:tex,transparent:true,opacity:0,depthWrite:false,blending:THREE.AdditiveBlending,color:0x8fe9ff
  });
  const sprite = new THREE.Sprite(mat);
  sprite.scale.set(.7,.7,.7);
  sprite.visible = false;
  return sprite;
}
function updateHookIndicator(player,dt){
  if(!player || !player.hookIndicator) return;
  const indicator = player.hookIndicator;
  const eligible = canHookShot(player) && possession === player.id && ballState === "dribble";
  if(!eligible){
    indicator.material.opacity = Math.max(0,indicator.material.opacity - dt*8);
    if(indicator.material.opacity <= .01) indicator.visible = false;
    return;
  }
  const toRim = HOOP_POS.clone().sub(player.group.position).setY(0).normalize();
  const right = getPlayerRight(player).setY(0).normalize();
  const rimSide = Math.sign(right.dot(toRim));
  const hookSide = rimSide > 0 ? -1 : 1;
  const pulse = .65 + .35*Math.sin(performance.now()*.009 + (player.id === "p1" ? 0 : 1.3));
  indicator.visible = true;
  indicator.position.set(hookSide * .58 * player.profile.width,1.76*player.profile.height,0.26);
  indicator.scale.setScalar(0.72 + pulse*0.2);
  indicator.material.opacity = Math.min(1,0.52 + pulse*0.45);
}
function getHookReleasePoint(player){
  const toRim = HOOP_POS.clone().sub(player.group.position).setY(0).normalize();
  const right = getPlayerRight(player).setY(0).normalize();
  const rimSide = Math.sign(right.dot(toRim));
  const hookSide = rimSide > 0 ? -1 : 1;
  const sideOffset = right.multiplyScalar(hookSide * 0.95 * player.profile.width);
  const releaseHeight = 2.15 + player.profile.height * 1.02 + Math.max(0,player.group.position.y);
  const forwardOffset = getPlayerForward(player).multiplyScalar(0.24);
  return player.group.position.clone().add(sideOffset).add(forwardOffset).add(new THREE.Vector3(0,releaseHeight,0));
}
function canDunk(player){
  if(!player.hasBall) return false;
  const defender = player.id === "p1" ? p2 : p1;
  const toRim = HOOP_POS.clone().sub(player.group.position).setY(0);
  const dist = toRim.length();
  const dirToRim = toRim.normalize();
  const forward = getPlayerForward(player).setY(0).normalize();
  const movingTowardRim = player.velocity.dot(dirToRim) > 0.65;
  const facingRim = forward.dot(dirToRim) > 0.35;
  const sizeBonus = (player.profile.height - 1) * 0.35 + player.profile.layup * 0.18;
  const dunkRange = 1.15 + sizeBonus;
  let resistance = 1;
  if(defender){
    const dd = flatDistance(defender.group.position,player.group.position);
    const inLane = isDefenderBetweenBallAndRim(defender,player.group.position);
    if(inLane && dd < 1.4){
      const early = Math.max(0,0.58 - (dunkTimer||0));
      resistance -= THREE.MathUtils.clamp((defender.profile.block*0.18 + defender.profile.width*0.16 - player.profile.width*0.1) * (0.65 + early),0,0.38);
    }
  }
  return dist <= dunkRange && facingRim && movingTowardRim && player.profile.jump > 0.86 && resistance > 0.72;
}
function canStartDunk(player){ return canDunk(player); }

function setAnimationState(char,state){
  char.anim.targetLean = state === "backpedal" ? -.12 : state === "sprint" ? .2 : state === "run" ? .12 : state === "defense" ? -.02 : 0;
  char.anim.state = state;
}
const maxForwardLean = 0.22;
const maxSideLean = 0.18;
const maxBackLean = 0.12;

function updateAnimationBlend(char,dt){
  const speed = char.velocity.length();
  const forward = getForward(char.angle);
  const moving = speed > .12;
  const moveDir = moving ? char.velocity.clone().normalize() : forward;
  const forwardDot = moving ? moveDir.dot(forward) : 0;
  const lateralDot = moving ? moveDir.dot(new THREE.Vector3(forward.z,0,-forward.x)) : 0;
  const inputState = inputManager.getState(char.id);
  const sprintKey = inputState.sprint;
  const sprinting = moving && (sprintKey || speed > 5.45);
  const backpedal = moving && forwardDot < -0.2;
  const walkTarget = moving ? THREE.MathUtils.clamp((speed - .1) / 3.2,0,1) : 0;
  const sprintTarget = sprinting ? THREE.MathUtils.clamp((speed - 3.8) / 2.4,0,1) : 0;
  const backpedalTarget = backpedal ? THREE.MathUtils.clamp((-forwardDot - .2) / .8,0,1) : 0;
  const idleTarget = moving ? 0 : 1;
  const turnPressed = inputState.moveX || 0;

  char.anim.walkBlend = smoothAnimValue(char.anim.walkBlend || 0,walkTarget,8,dt);
  char.anim.sprintBlend = smoothAnimValue(char.anim.sprintBlend || 0,sprintTarget,10,dt);
  char.anim.backpedalBlend = smoothAnimValue(char.anim.backpedalBlend || 0,backpedalTarget,9,dt);
  char.anim.legBlend = smoothAnimValue(char.anim.legBlend,idleTarget < .95 ? 1 : 0,7,dt);
  char.anim.armBlend = smoothAnimValue(char.anim.armBlend,char.hasBall?1:0,10,dt);
  char.anim.turnLean = smoothAnimValue(char.anim.turnLean || 0,turnPressed * THREE.MathUtils.clamp(speed/5.5,0,1),8,dt);
  char.anim.strideStrength = smoothAnimValue(char.anim.strideStrength || 0,THREE.MathUtils.clamp(speed/5.8,0,1.2),8,dt);
  char.anim.verticalBob = smoothAnimValue(char.anim.verticalBob || 0,moving ? THREE.MathUtils.clamp(speed/6.5,0,1) : 0,7,dt);
  const defending = possession !== char.id && ballState === "dribble" && flatDistance(char.group.position,(char.id==="p1"?p2:p1).group.position) < (2 + char.movement.defensiveWidth*.45);
  const strafeDot = Math.abs(lateralDot);
  const stanceTarget = defending ? THREE.MathUtils.clamp((1.95-flatDistance(char.group.position,(char.id==="p1"?p2:p1).group.position))/1.95,0,1) : 0;
  char.anim.defenseStanceBlend = smoothAnimValue(char.anim.defenseStanceBlend||0,stanceTarget,10,dt);
  const shuffleTarget = defending ? THREE.MathUtils.clamp(strafeDot * (1-sprintTarget) * walkTarget,0,1) : 0;
  char.anim.shuffleCycle = (char.anim.shuffleCycle||0) + dt * THREE.MathUtils.lerp(2.2,7.2,shuffleTarget);
  char.anim.cutoffLean = smoothAnimValue(char.anim.cutoffLean||0,defending ? (char.defensiveShadeSide==="left"?-1:char.defensiveShadeSide==="right"?1:0) : 0,7,dt);
  char.anim.targetLean = THREE.MathUtils.clamp(char.anim.sprintBlend * 0.2 + char.anim.walkBlend * 0.06 - char.anim.backpedalBlend * 0.06 - char.anim.defenseStanceBlend*0.04,-maxBackLean,maxForwardLean);
  char.anim.lean = smoothAnimValue(char.anim.lean,char.anim.targetLean,8,dt);
  const sharpTurn = moving ? THREE.MathUtils.clamp((strafeDot - 0.35) / 0.5,0,1) * THREE.MathUtils.clamp(speed/5,0,1) : 0;
  const cutDir = sharpTurn > 0.08 ? (lateralDot < 0 ? -1 : 1) : 0;
  char.anim.cutPlant = smoothAnimValue(char.anim.cutPlant || 0,sharpTurn,12,dt);
  char.anim.cutDir = cutDir || char.anim.cutDir || 0;
  if(sharpTurn > 0.2) char.velocity.multiplyScalar(1 - Math.min(0.11,dt*2.8));
  const cycleRate = THREE.MathUtils.lerp(2.1,8.6,char.anim.walkBlend * 0.58 + char.anim.sprintBlend * 0.9 + char.anim.backpedalBlend * 0.3);
  char.anim.runCycle = (char.anim.runCycle || 0) + dt * cycleRate * (moving ? 1 : .65);
  char.anim.contactReact = smoothAnimValue(char.anim.contactReact,0,6,dt);
}

function animateTorsoLean(char,dt){
  const fadeLean = char.anim.fadeLean || 0;
  const cutPlant = char.anim.cutPlant || 0;
  const sideLeanRaw = (char.anim.sideLean || 0) * -0.1 + fadeLean * -0.04 + (char.anim.turnLean || 0) * -0.04 + (char.anim.cutDir || 0) * cutPlant * -0.03;
  const forwardLeanRaw = -char.anim.lean + char.anim.contactReact*.05 + cutPlant*0.02;
  const clampedForwardLean = THREE.MathUtils.clamp(forwardLeanRaw,-maxBackLean,maxForwardLean);
  const clampedSideLean = THREE.MathUtils.clamp(sideLeanRaw,-maxSideLean,maxSideLean);
  char.group.rotation.x = smoothAnimValue(char.group.rotation.x,clampedForwardLean,11,dt);
  char.group.rotation.z = smoothAnimValue(char.group.rotation.z,clampedSideLean,12,dt);
  if(char.torso){
    const torsoSide = THREE.MathUtils.clamp((char.anim.turnLean || 0) * -0.04 + (char.anim.cutDir || 0) * cutPlant * -0.03,-0.12,0.12);
    const torsoForward = THREE.MathUtils.clamp((char.anim.sprintBlend || 0)*0.04 - (char.anim.backpedalBlend || 0)*0.02,-0.06,0.08);
    char.torso.rotation.z = smoothAnimValue(char.torso.rotation.z,torsoSide,10,dt);
    char.torso.rotation.x = smoothAnimValue(char.torso.rotation.x,torsoForward,9,dt);
  }
}

function animateLanding(char,dt){
  const wasGrounded = !!char.anim.wasGrounded;
  if(char.onGround && !wasGrounded){ char.anim.landingTimer = 0.16; }
  char.anim.wasGrounded = char.onGround;
  char.anim.landingTimer = Math.max(0,(char.anim.landingTimer || 0) - dt);
  const landPct = char.anim.landingTimer > 0 ? Math.sin((char.anim.landingTimer/0.16)*Math.PI) : 0;
  const airborneSquash = char.onGround ? 0 : 0.08;
  const landingSquash = landPct * 0.1;
  char.anim.landingSquash = smoothAnimValue(char.anim.landingSquash,airborneSquash + landingSquash,10,dt);
  char.group.scale.y = 1 - char.anim.landingSquash;
  if(landPct > 0){
    char.leftLeg.rotation.x += 0.22 * landPct;
    char.rightLeg.rotation.x += 0.22 * landPct;
    char.group.position.y -= landPct * 0.004;
  }
}

function animateContactReaction(char,dt){
  const react = char.anim.contactReact;
  char.group.rotation.z = clampLean(char.group.rotation.z + react * dt * 0.05,-0.12,0.12);
  if(char.contactSlide > 0){
    const slideVec = getPlayerForward(char).multiplyScalar(-0.18 * char.contactSlide * dt);
    char.group.position.add(slideVec);
    char.contactSlide = Math.max(0,char.contactSlide - dt*3.8);
  }
  if(char.contactType === "stonewall"){
    char.group.rotation.x = smoothAnimValue(char.group.rotation.x,0.09,8,dt);
  }else if(char.contactType === "blow_by"){
    char.group.rotation.x = smoothAnimValue(char.group.rotation.x,-0.06,8,dt);
  }
}

function resetArmDefaults(char){
  const w = char.profile.width;
  const h = char.profile.height;
  char.leftArm.position.set(-.42*w,1.52*h,-.01);
  char.rightArm.position.set(.42*w,1.52*h,-.01);
  char.leftForearm.position.set(-.56*w,1.12*h,.02);
  char.rightForearm.position.set(.56*w,1.12*h,.02);
  char.leftHand.position.set(-.57*w,.84*h,.06);
  char.rightHand.position.set(.57*w,.84*h,.06);
  char.leftArm.rotation.set(0,0,-.35);
  char.rightArm.rotation.set(0,0,.35);
  char.leftForearm.rotation.set(0,0,-.1);
  char.rightForearm.rotation.set(0,0,.1);
}

function poseSwipeArm(char,side,reach){
  const w = char.profile.width;
  const h = char.profile.height;
  const sign = side === "left" ? -1 : 1;
  const upper = side === "left" ? char.leftArm : char.rightArm;
  const fore = side === "left" ? char.leftForearm : char.rightForearm;
  const hand = side === "left" ? char.leftHand : char.rightHand;
  const reachScale = char.swipeReachScale || 1;
  const reachForward = -Math.max(0.14,0.24 + reach * 0.48 * reachScale);
  const crossBody = sign * (0.42 - reach * 0.1) * w;
  upper.position.set(sign * 0.42 * w, 1.48 * h, -0.08);
  fore.position.set(crossBody, 1.18 * h, reachForward);
  hand.position.set(crossBody + sign * 0.06*w, 1.02 * h, reachForward - (0.08 + reach * 0.22 * reachScale));
  upper.rotation.set(-0.65 - reach * 0.35, 0, sign * 0.12);
  fore.rotation.set(-0.92 - reach * 0.4, 0, sign * 0.06);
}

function poseBlockArms(char,reach){
  const w = char.profile.width;
  const h = char.profile.height;
  char.leftArm.position.set(-.4*w,1.55*h,-.08);
  char.rightArm.position.set(.4*w,1.55*h,-.08);
  char.leftForearm.position.set(-.34*w,1.9*h,-.24-reach*.22);
  char.rightForearm.position.set(.34*w,1.9*h,-.24-reach*.22);
  char.leftHand.position.set(-.31*w,2.2*h,-.34-reach*.3);
  char.rightHand.position.set(.31*w,2.2*h,-.34-reach*.3);
  char.leftArm.rotation.set(-1.66-reach*.32,0,-.16);
  char.rightArm.rotation.set(-1.66-reach*.32,0,.16);
  char.leftForearm.rotation.set(-.9-reach*.32,0,-.06);
  char.rightForearm.rotation.set(-.9-reach*.32,0,.06);
}

function poseShotArms(char,pct){
  const w = char.profile.width;
  const h = char.profile.height;
  const gather = smoothstep(Math.min(pct/.38,1));
  const lift = smoothstep(Math.min(Math.max(0,pct-.16)*1.35,1));
  const follow = Math.max(0,(pct-.55)/.45);
  const chestDip = (1-gather) * 0.22;
  char.leftArm.position.set(-.24*w,1.48*h,-.12);
  char.rightArm.position.set(.24*w,1.48*h,-.12);
  char.leftForearm.position.set(-.18*w,1.55*h + lift*.42*h - chestDip*h,-.28-lift*.20-follow*.15);
  char.rightForearm.position.set(.18*w,1.55*h + lift*.42*h - chestDip*h,-.28-lift*.20-follow*.15);
  char.leftHand.position.set(-.13*w,1.45*h + lift*.82*h - chestDip*h,-.38-lift*.24-follow*.18);
  char.rightHand.position.set(.13*w,1.45*h + lift*.82*h - chestDip*h,-.38-lift*.24-follow*.18);
  char.leftArm.rotation.set(-.95-lift*.9,0,-.05);
  char.rightArm.rotation.set(-.95-lift*.9,0,.05);
  char.leftForearm.rotation.set(-.65-lift*.9-follow*.35,0,-.05);
  char.rightForearm.rotation.set(-.65-lift*.9-follow*.35,0,.05);
}


function poseDunkArms(char,progress){
  const w = char.profile.width;
  const h = char.profile.height;
  const meta = (char.moveMeta && char.moveMeta.dunk) || {};
  const side = meta.side === "left" ? -1 : 1;
  const gather = THREE.MathUtils.clamp(progress / 0.2,0,1);
  const lift = THREE.MathUtils.clamp((progress - 0.2) / 0.34,0,1);
  const rimPhase = THREE.MathUtils.clamp((progress - 0.54) / 0.22,0,1);
  const finish = THREE.MathUtils.clamp((progress - 0.76) / 0.24,0,1);
  const gatherDip = Math.sin(gather*Math.PI) * (1-lift);
  char.leftArm.position.set(-.28*w,1.5*h,-.12);
  char.rightArm.position.set(.28*w,1.5*h,-.12);
  if(meta.style === "twohand"){
    const gatherCurl = smoothstep(gather);
    const rise = smoothstep(lift);
    const punch = smoothstep(finish);
    char.leftArm.rotation.set(-0.42 - gatherCurl*0.4 - rise*0.98 - punch*0.36,0,-0.1);
    char.rightArm.rotation.set(-0.42 - gatherCurl*0.4 - rise*0.98 - punch*0.36,0,0.1);
    char.leftForearm.rotation.set(-0.55 - gatherCurl*0.52 - rise*0.84 - punch*0.5,0,-0.08);
    char.rightForearm.rotation.set(-0.55 - gatherCurl*0.52 - rise*0.84 - punch*0.5,0,0.08);
  }else{
    const attackArm = side > 0 ? char.rightArm : char.leftArm;
    const attackForearm = side > 0 ? char.rightForearm : char.leftForearm;
    const guideArm = side > 0 ? char.leftArm : char.rightArm;
    const guideForearm = side > 0 ? char.leftForearm : char.rightForearm;
    const tomahawk = meta.style === "tomahawk" ? 1 : 0;
    const quick = meta.style === "quick" ? 1 : 0;
    attackArm.rotation.set(-0.52 - smoothstep(gather)*0.42 - smoothstep(lift)*(1.08 + tomahawk*0.16 - quick*0.1) - smoothstep(finish)*(0.3 + tomahawk*0.12),0,side*(0.06 + tomahawk*0.04));
    attackForearm.rotation.set(-0.62 - smoothstep(gather)*0.46 - smoothstep(rimPhase)*(0.92 + quick*0.12) - smoothstep(finish)*(0.52 + tomahawk*0.12),0,side*0.04);
    guideArm.rotation.set(-0.4 - gatherDip*0.45 - smoothstep(lift)*(0.38 + quick*0.12),0,-side*(0.22 + tomahawk*0.04));
    guideForearm.rotation.set(-0.35 - smoothstep(lift)*0.4,0,-side*0.08);
  }
}
