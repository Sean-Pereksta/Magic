// Cheesehold late-game overrun expansion
// Generalizes the fortress expansion from 3 waves into escalating late-game sieges.

CHEESEHOLD_LEVELS.push(
 {name:'Crossfire Depot',sub:'interlocking fire lanes with four long breach approaches',layout:'crossfire'},
 {name:'Four Keeps',sub:'four hardpoints split by a wide central kill cross',layout:'quadrants'},
 {name:'Serpent Works',sub:'alternating blast walls force a winding defense line',layout:'serpent'},
 {name:'Shattered Boardwalk',sub:'broken islands and open bridges create exposed approaches',layout:'bridges'},
 {name:'Ratchet Maze',sub:'staggered teeth create repeating fallback pockets',layout:'ratchet'},
 {name:'Siege Courtyard',sub:'a breached outer court surrounds a contested inner square',layout:'courtyard'},
 {name:'Iron Gauntlet',sub:'layered siege lines turn every retreat into another stand',layout:'gauntlet'},
 {name:'Split Cathedral',sub:'twin halls and a broad transept produce brutal crossfire',layout:'cathedral'},
 {name:'Last Redoubt',sub:'nested defensive rings face eight relentless assault waves',layout:'redoubt'},
 {name:'Overrun Foundry',sub:'wide open invasion lanes are built for enormous enemy floods',layout:'overrun'}
);

Object.assign(ENEMIES,{
 direRat:{unlock:8,hp:115,damage:19,move:.23,attack:.42,bounty:4,xp:6,color:0xc46f68,wall:1.15,family:'rat',lateTier:1,visualScale:1.08},
 ironMole:{unlock:10,hp:210,damage:30,move:.49,attack:.50,bounty:5,xp:8,color:0x6f7379,wall:4.5,family:'mole',lateTier:1,visualScale:1.13},
 warWeasel:{unlock:11,hp:150,damage:26,move:.21,attack:.36,bounty:5,xp:8,color:0xe08358,wall:1.2,family:'weasel',lateTier:2,visualScale:1.10},
 ramOx:{unlock:12,hp:480,damage:46,move:.65,attack:.60,bounty:8,xp:12,color:0xc49557,wall:2.6,family:'ox',lateTier:2,visualScale:1.18},
 blackBadger:{unlock:13,hp:620,damage:52,move:.60,attack:.54,bounty:9,xp:14,color:0x4e5158,wall:5.4,family:'badger',lateTier:3,visualScale:1.20},
 razorShrew:{unlock:14,hp:125,damage:28,move:.16,attack:.29,bounty:6,xp:10,color:0x9c4ec7,wall:1.0,family:'shrew',lateTier:3,visualScale:1.08},
 siegeBrute:{unlock:15,hp:850,damage:68,move:.68,attack:.50,bounty:12,xp:18,color:0x713e34,wall:6.5,family:'brute',lateTier:4,visualScale:1.26},
 dreadFerret:{unlock:16,hp:360,damage:45,move:.20,attack:.32,bounty:9,xp:14,color:0xad5e86,wall:1.6,family:'ferret',lateTier:4,visualScale:1.16},
 crusherOx:{unlock:18,hp:1100,damage:82,move:.63,attack:.48,bounty:15,xp:22,color:0x8e623c,wall:7.2,family:'ox',lateTier:5,visualScale:1.32},
 apexBrute:{unlock:20,hp:1450,damage:96,move:.60,attack:.42,bounty:18,xp:26,color:0x4f2026,wall:8.5,family:'brute',lateTier:6,visualScale:1.38}
});

function wavesForRound(round=game.round){
 if(round>=15)return 8;
 if(round>=10)return 5;
 if(round>=5)return 4;
 return 3;
}

function enemyCap(){
 const r=Math.max(1,game.round);
 const tier=r>=15?14:r>=10?8:r>=5?4:0;
 return Math.min(48,10+Math.floor(r*1.25)+tier);
}

function enemyLevelHealthScale(round=game.round){
 const level=Math.max(0,round-1);
 return Math.pow(1.075,level)*(1+Math.pow(level,1.35)*.018);
}

function catLevelHealthScale(round=game.round){
 const level=Math.max(0,round-1);
 return Math.pow(1.105,level)*(1+Math.pow(level,1.42)*.025);
}

function waveQuota(wave=game.wave?.number||1){
 const r=Math.max(1,game.round),total=wavesForRound(r);
 const tier=r>=15?20:r>=10?11:r>=5?4:0;
 const base=5+Math.floor(r*1.15)+tier;
 const ramp=1+(wave-1)*(r>=15?.095:r>=10?.10:.12);
 const finalBonus=wave===total?(r>=15?8:r>=10?5:3):0;
 return Math.max(6,Math.round(base*ramp+finalBonus));
}

function waveLabel(){
 const w=game.wave?.number||1,total=game.wave?.total||wavesForRound();
 return w===total?'WAVE '+w+' / '+total+' · CAT':'WAVE '+w+' / '+total;
}

function setWave(number){
 const total=wavesForRound();
 game.wave={number,total,spawned:0,quota:waveQuota(number),clearAt:null,nextAt:null,announced:false};
 game.nextMinion=game.time+(number===1?1.6:.82);
 if(number===total&&!cat)spawnCat();
 const final=number===total;
 showMsg(final?'FINAL WAVE — CAT':'WAVE '+number+' / '+total,final?'BOSS + MASS ESCORTS':'Enemy force '+number+' of '+total+' · '+game.wave.quota+' hostiles');
 updateWaveHud();
}

function updateWaveHud(){
 if(!game.running||!game.wave)return;
 const w=game.wave,total=w.total||wavesForRound();
 let status='Level '+game.round+' · '+waveLabel()+' · '+Math.max(0,w.quota-w.spawned)+' reinforcements';
 if(w.clearAt!=null&&w.number<total){
   const left=Math.max(0,Math.ceil(WAVE_REPRIEVE-(game.time-w.clearAt)));
   status='Level '+game.round+' · Wave '+w.number+' cleared · '+left+'s reprieve';
 }
 if(w.number===total&&cat&&!cat.dead)status='Level '+game.round+' · FINAL WAVE '+w.number+'/'+total+' · '+game.boss.name+' · '+Math.ceil(cat.hp)+' HP';
 ui.themeSub.textContent=status;
 if(!cat){
   ui.catHp.textContent='W'+total;
   ui.catBar.style.width='0%';
   ui.catAbility.textContent='Final wave';
   ui.catAbilityTime.textContent='W'+total;
 }
}

function waveController(){
 if(game.running&&!game.paused&&game.wave){
   const w=game.wave,total=w.total||wavesForRound();
   const live=enemies.filter(e=>!e.dead).length;
   if(w.spawned>=w.quota&&live===0&&w.number<total){
     if(w.clearAt==null){
       w.clearAt=game.time;
       showMsg('WAVE '+w.number+' CLEAR','15 SECOND REPRIEVE');
       toast('Rebuild fast — wave '+(w.number+1)+' of '+total+' incoming');
     }else if(game.time-w.clearAt>=WAVE_REPRIEVE){
       setWave(w.number+1);
     }
   }
   updateWaveHud();
 }
 requestAnimationFrame(waveController);
}

function farEdgeCandidates(){
 const px=player?.gx??Math.floor(COLS/2),py=player?.gy??Math.floor(ROWS/2),out=[];
 for(let x=1;x<COLS-1;x++){
   out.push([x,1],[x,ROWS-2]);
 }
 for(let y=2;y<ROWS-2;y++){
   out.push([1,y],[COLS-2,y]);
 }
 return out
   .filter(p=>!terrain.has(K(p[0],p[1]))&&!buildAt(p[0],p[1])&&!occupiedEnemy(p[0],p[1]))
   .map(p=>({p,d:Math.abs(p[0]-px)+Math.abs(p[1]-py)}))
   .sort((a,b)=>b.d-a.d);
}

function safeOppositeEdgeSpot(offset=0){
 const ranked=farEdgeCandidates();
 if(!ranked.length)return safeEdgeSpot(offset);
 const r=Math.max(1,game.round),w=game.wave?.number||1;
 const farBand=Math.max(1,Math.ceil(ranked.length*(r>=15?.42:r>=10?.34:.26)));
 const idx=Math.abs(offset+w*5+r*3)%farBand;
 return ranked[idx].p;
}

function spawnCat(){
 const total=wavesForRound();
 if(game.wave&&game.wave.number<total)return false;
 const s=safeOppositeEdgeSpot(11),boss=BOSS_VARIANTS[(game.round-1)%BOSS_VARIANTS.length],r=Math.max(1,game.round);
 const terror=1+(r-1)*.24+Math.pow(Math.max(0,r-1),1.28)*.075;
 const tierHp=r>=15?1.46:r>=10?1.26:r>=5?1.12:1;
 const tierDmg=r>=15?1.34:r>=10?1.20:r>=5?1.10:1;
 const hp=(350+r*145)*terror*tierHp*catLevelHealthScale(r)*(game.mutator?.catHp||1)*boss.hp;
 const dmg=(18+r*2.25)*(1+(r-1)*.13)*tierDmg*boss.damage;
 const move=Math.max(.095,(.49-r*.013)*boss.move);
 const mesh=catMesh();styleBossMesh(mesh,boss);
 mesh.scale.multiplyScalar(1+Math.min(.58,(r-1)*.038));
 game.boss=boss;
 cat={type:'cat',boss,gx:s[0],gy:s[1],mesh,hp,maxHp:hp,damage:dmg,baseMove:move,attackEvery:Math.max(.38,.7-(r-1)*.018),nextMove:game.time+.5,nextAttack:0,moving:null,dead:false,bounty:14+r*3,slow:0,poisonUntil:0,poisonTick:0,eaten:new Set(),wall:1.3*boss.wall,attackAnim:null,baseScale:mesh.scale.clone(),special:null,nextAbility:game.time+(r>=2?5.8:8.5),abilityCursor:0};
 cat.bar=healthBar(1.6);cat.bar.position.y=1.78;cat.mesh.add(cat.bar);cat.mesh.position.copy(worldPos(cat.gx,cat.gy));entityGroup.add(cat.mesh);
 flashDanger('rgba(255,25,42,.34)');
 toast(game.boss.name+' enters the final wave — '+Math.round(cat.maxHp)+' HP');
 return true;
}

function pickEnemyDefinition(){
 const r=Math.max(1,game.round),unlocked=Object.entries(ENEMIES).filter(([,d])=>r>=d.unlock);
 if(!unlocked.length)return null;
 const weighted=unlocked.map(([type,d])=>{
   let weight=1+Math.max(0,d.unlock-1)*.055;
   if(d.lateTier)weight*=1+Math.min(2.4,(r-d.unlock+1)*.32+d.lateTier*.12);
   if(d.unlock>=Math.max(1,r-4))weight*=2.1;
   return {type,d,weight};
 });
 const total=weighted.reduce((s,x)=>s+x.weight,0);
 let roll=Math.random()*total;
 for(const item of weighted){roll-=item.weight;if(roll<=0)return item;}
 return weighted[weighted.length-1];
}

function decorateLateEnemy(mesh,d){
 const tier=d.lateTier||0;
 if(!tier)return;
 mesh.scale.multiplyScalar(d.visualScale||1);
 mesh.traverse(n=>{if(n.material?.emissive){n.material.emissive.setHex(d.color);n.material.emissiveIntensity=Math.min(.8,.16+tier*.08)}});
 const ring=new THREE.Mesh(new THREE.TorusGeometry(.27+Math.min(.18,tier*.025),.025+Math.min(.025,tier*.004),6,18),new THREE.MeshBasicMaterial({color:d.color,transparent:true,opacity:.72}));
 ring.rotation.x=Math.PI/2;ring.position.y=.88+Math.min(.35,tier*.045);mesh.add(ring);
}

function spawnEnemy(){
 const w=game.wave;
 if(w&&(w.clearAt!=null||w.spawned>=w.quota))return false;
 if(enemies.filter(e=>!e.dead).length>=enemyCap())return false;
 const picked=pickEnemyDefinition();if(!picked)return false;
 const {type,d}=picked,baseType=d.family||type;
 const spot=safeOppositeEdgeSpot((w?.spawned||0)*3+enemies.length*2);if(!spot)return false;
 const r=Math.max(1,game.round),waveNumber=w?.number||1,total=w?.total||wavesForRound(r);
 const waveScale=1+(waveNumber-1)*(r>=15?.065:.08)+(waveNumber===total?.10:0);
 const hpTier=r>=15?1.62:r>=10?1.35:r>=5?1.18:1;
 const dmgTier=r>=15?1.48:r>=10?1.28:r>=5?1.12:1;
 const scale=(1+(r-1)*.13)*waveScale*hpTier*enemyLevelHealthScale(r);
 const eliteChance=Math.min(.66,.06+r*.021+(waveNumber===total?.10:0)+(game.mutator?.eliteBonus||0)+(d.lateTier||0)*.012);
 const elite=r>=3&&Math.random()<eliteChance;
 const mesh=minionMesh(baseType,d.color);decorateLateEnemy(mesh,d);
 const eliteHp=elite?1.75:1,eliteDmg=elite?1.32:1;
 const e={type:baseType,variant:type,gx:spot[0],gy:spot[1],mesh,hp:d.hp*scale*eliteHp,maxHp:d.hp*scale*eliteHp,damage:d.damage*(1+(r-1)*.09)*waveScale*dmgTier*eliteDmg,baseMove:Math.max(.105,d.move*(1-(r-1)*.011)*(elite?.86:1)*(game.mutator?.enemySpeed||1)),attackEvery:Math.max(.22,d.attack*(r>=15?.90:r>=10?.95:1)),bounty:d.bounty+(elite?2:0)+(game.mutator?.bountyBonus||0),xp:d.xp*(elite?1.75:1),wall:d.wall*(elite?1.15:1),elite,nextMove:game.time+.18,nextAttack:0,moving:null,dead:false,slow:0,poisonUntil:0,poisonTick:0,eaten:new Set(),attackAnim:null,baseScale:mesh.scale.clone()};
 if(elite){
   mesh.scale.multiplyScalar(1.18);e.baseScale=mesh.scale.clone();
   mesh.traverse(n=>{if(n.material?.emissive){n.material.emissive.setHex(0xffb52e);n.material.emissiveIntensity=.65}});
   const crown=new THREE.Mesh(new THREE.TorusGeometry(.32,.035,6,18),new THREE.MeshBasicMaterial({color:0xffcf55}));
   crown.rotation.x=Math.PI/2;crown.position.y=1.04;mesh.add(crown);
 }
 const heavy=baseType==='ox'||baseType==='brute'||baseType==='badger';
 e.bar=healthBar(heavy?1.12:.84);e.bar.position.y=heavy?1.24:.9;e.mesh.add(e.bar);
 e.mesh.position.copy(worldPos(e.gx,e.gy));entityGroup.add(e.mesh);enemies.push(e);
 if(w)w.spawned++;
 return true;
}

function minionUpdate(){
 const w=game.wave;
 if(!w||w.clearAt!=null||w.spawned>=w.quota||game.time<game.nextMinion)return;
 const r=Math.max(1,game.round),room=Math.max(0,enemyCap()-enemies.filter(e=>!e.dead).length),remaining=Math.max(0,w.quota-w.spawned);
 let burst=r>=15?4+Math.floor((w.number-1)/3):r>=10?3:r>=5?2:1;
 burst=Math.min(burst,room,remaining);
 for(let i=0;i<burst;i++)spawnEnemy();
 const interval=r>=15?.70:r>=10?.88:r>=5?1.05:Math.max(.9,2.4-r*.08);
 game.nextMinion=game.time+Math.max(.54,interval-(w.number-1)*(r>=15?.025:.04));
}

function beginRound(first=false){
 for(const b of buildings)disposeObject(b.mesh);for(const c of cheeses)disposeObject(c.mesh);for(const e of enemies)disposeObject(e.mesh);for(const b of bullets)disposeObject(b.mesh);
 if(cat)disposeObject(cat.mesh);if(player)disposeObject(player.mesh);
 buildings=[];cheeses=[];enemies=[];bullets=[];cat=null;player=null;
 game.mutator=ROUND_MUTATORS[(game.round-1)%ROUND_MUTATORS.length];
 game.cheese+=first?0:game.mods.roundCheese;
 buildArena(game.round);spawnPlayer();
 game.boss=BOSS_VARIANTS[(game.round-1)%BOSS_VARIANTS.length];
 for(let i=0;i<10;i++)spawnCheese();
 game.nextCheese=game.time+2.2;game.pendingRound=false;
 setWave(1);refreshEnclosures(true);updateSelected();
 const level=CHEESEHOLD_LEVELS[(game.round-1)%CHEESEHOLD_LEVELS.length],waves=wavesForRound();
 showMsg(first?'LEVEL 1':'LEVEL '+game.round,level.name+' · '+waves+' WAVES · ENEMY CAP '+enemyCap());
}

function customLateLayout(layout,cx,cy){
 if(layout==='crossfire'){
   for(const y of [4,7,ROWS-8,ROWS-5])lineTerrain(3,y,COLS-4,y,[6,cx,COLS-7]);
   for(const x of [6,COLS-7])lineTerrain(x,3,x,ROWS-4,[5,cy,ROWS-6]);
 }else if(layout==='quadrants'){
   for(const [x0,y0] of [[3,3],[COLS-9,3],[3,ROWS-9],[COLS-9,ROWS-9]]){
     lineTerrain(x0,y0,x0+5,y0,[x0+2]);lineTerrain(x0,y0,x0,y0+5,[y0+3]);
     lineTerrain(x0+5,y0,x0+5,y0+5,[y0+2]);lineTerrain(x0,y0+5,x0+5,y0+5,[x0+3]);
   }
 }else if(layout==='serpent'){
   const xs=[4,8,12,16,COLS-5];
   xs.forEach((x,i)=>lineTerrain(x,2,x,ROWS-3,i%2?[4,5,cy]:[cy,ROWS-6,ROWS-5]));
 }else if(layout==='bridges'){
   for(const [x,y,w,h] of [[3,3,5,4],[COLS-8,3,5,4],[3,ROWS-7,5,4],[COLS-8,ROWS-7,5,4],[cx-3,cy-2,7,5]])for(let yy=y;yy<y+h;yy++)for(let xx=x;xx<x+w;xx++)addLevelTerrain(xx,yy);
   for(const x of [cx-5,cx+5])for(let y=4;y<ROWS-4;y+=4)addLevelTerrain(x,y);
 }else if(layout==='ratchet'){
   for(let y=3;y<ROWS-3;y+=4){lineTerrain(3,y,COLS-4,y,[5+(y%3),cx,COLS-7]);for(let x=5;x<COLS-5;x+=6)addLevelTerrain(x,y+1);}
 }else if(layout==='courtyard'){
   lineTerrain(3,3,COLS-4,3,[cx]);lineTerrain(3,ROWS-4,COLS-4,ROWS-4,[cx]);
   lineTerrain(3,3,3,ROWS-4,[cy]);lineTerrain(COLS-4,3,COLS-4,ROWS-4,[cy]);
   for(const [x,y] of [[cx-5,cy-4],[cx+5,cy-4],[cx-5,cy+4],[cx+5,cy+4]])for(let yy=y-1;yy<=y+1;yy++)for(let xx=x-1;xx<=x+1;xx++)addLevelTerrain(xx,yy);
 }else if(layout==='gauntlet'){
   for(const y of [4,8,12,ROWS-5])lineTerrain(2,y,COLS-3,y,[4+(y%5),cx-3+(y%3)*3,COLS-6]);
   lineTerrain(5,2,5,ROWS-3,[6,cy,ROWS-7]);lineTerrain(COLS-6,2,COLS-6,ROWS-3,[5,cy,ROWS-6]);
 }else if(layout==='cathedral'){
   lineTerrain(cx-5,2,cx-5,ROWS-3,[5,cy,ROWS-6]);lineTerrain(cx+5,2,cx+5,ROWS-3,[5,cy,ROWS-6]);
   lineTerrain(2,cy-3,COLS-3,cy-3,[5,cx,COLS-6]);lineTerrain(2,cy+3,COLS-3,cy+3,[5,cx,COLS-6]);
   for(const x of [cx-8,cx+8])for(const y of [cy-6,cy+6]){addLevelTerrain(x,y);addLevelTerrain(x+1,y);addLevelTerrain(x,y+1);}
 }else if(layout==='redoubt'){
   for(const inset of [3,7]){
     lineTerrain(inset,inset,COLS-1-inset,inset,[cx-4,cx,cx+4]);
     lineTerrain(inset,ROWS-1-inset,COLS-1-inset,ROWS-1-inset,[cx-4,cx,cx+4]);
     lineTerrain(inset,inset,inset,ROWS-1-inset,[cy-3,cy,cy+3]);
     lineTerrain(COLS-1-inset,inset,COLS-1-inset,ROWS-1-inset,[cy-3,cy,cy+3]);
   }
 }else if(layout==='overrun'){
   for(const x of [5,10,COLS-11,COLS-6])for(const y of [4,cy,ROWS-5]){addLevelTerrain(x,y);addLevelTerrain(x+1,y);if(y!==cy)addLevelTerrain(x,y+1);}
   lineTerrain(3,5,9,5,[6]);lineTerrain(COLS-10,ROWS-6,COLS-4,ROWS-6,[COLS-7]);
 }
}

function buildArena(round){
 clearGroup(tileGroup);clearGroup(terrainGroup);clearGroup(decorGroup);terrain.clear();
 const t=THEMES[(round-1)%THEMES.length],level=CHEESEHOLD_LEVELS[(round-1)%CHEESEHOLD_LEVELS.length],rng=seeded(round*37+11);
 scene.background=new THREE.Color(t.sky);scene.fog=new THREE.FogExp2(t.fog,.029);
 ui.themeName.textContent='L'+round+' · '+level.name;ui.themeSub.textContent=level.sub;pointLight.color.setHex(t.accent);
 for(let y=0;y<ROWS;y++)for(let x=0;x<COLS;x++){const tile=box(CELL*.99,.14,CELL*.99,((x+y)&1)?t.tileA:t.tileB,.94,.01);tile.position.copy(worldPos(x,y,-.11));tileGroup.add(tile);}
 const bw=box(COLS*CELL+.5,1.9,.34,t.terrain,.62,.1);bw.position.set(0,.84,-ROWS*CELL/2-.14);terrainGroup.add(bw);
 const bs=bw.clone();bs.position.z=ROWS*CELL/2+.14;terrainGroup.add(bs);
 const bl=box(.34,1.9,ROWS*CELL+.5,t.terrain,.62,.1);bl.position.set(-COLS*CELL/2-.14,.84,0);terrainGroup.add(bl);
 const br=bl.clone();br.position.x=COLS*CELL/2+.14;terrainGroup.add(br);
 const cx=Math.floor(COLS/2),cy=Math.floor(ROWS/2);
 if(level.layout==='lanes'){
   for(const x of [5,9,17,21])lineTerrain(x,2,x,ROWS-3,[5,cy,15]);lineTerrain(3,5,COLS-4,5,[7,cx,19]);lineTerrain(3,15,COLS-4,15,[7,cx,19]);
 }else if(level.layout==='foundry'){
   for(const [x,y,w,h] of [[3,3,5,4],[COLS-8,3,5,4],[3,ROWS-7,5,4],[COLS-8,ROWS-7,5,4]])for(let yy=y;yy<y+h;yy++)for(let xx=x;xx<x+w;xx++)addLevelTerrain(xx,yy);lineTerrain(cx-4,4,cx+4,4,[cx]);lineTerrain(cx-4,ROWS-5,cx+4,ROWS-5,[cx]);
 }else if(level.layout==='spiral'){
   lineTerrain(4,4,COLS-5,4,[COLS-7]);lineTerrain(4,4,4,ROWS-5,[6]);lineTerrain(4,ROWS-5,COLS-5,ROWS-5,[6]);lineTerrain(COLS-5,7,COLS-5,ROWS-5,[ROWS-7]);lineTerrain(8,8,COLS-9,8,[10]);lineTerrain(8,8,8,ROWS-9,[ROWS-11]);lineTerrain(8,ROWS-9,COLS-9,ROWS-9,[COLS-11]);
 }else if(level.layout==='freezer'){
   for(let i=2;i<ROWS-2;i++){const x=3+((i*2)%8);addLevelTerrain(x,i);if(i%2)addLevelTerrain(x+1,i);}for(let i=2;i<ROWS-2;i++){const x=COLS-4-((i*3)%8);addLevelTerrain(x,i);if(i%3===0)addLevelTerrain(x-1,i);}lineTerrain(4,cy,COLS-5,cy,[7,cx,COLS-8]);
 }else if(level.layout==='vaults'){
   for(const x0 of [4,COLS-10]){lineTerrain(x0,4,x0+5,4,[x0+2]);lineTerrain(x0,4,x0,ROWS-5,[cy]);lineTerrain(x0,ROWS-5,x0+5,ROWS-5,[x0+3]);lineTerrain(x0+5,4,x0+5,ROWS-5,[cy]);}lineTerrain(cx,2,cx,ROWS-3,[5,cy,15]);
 }else if(level.layout==='rings'){
   lineTerrain(3,3,COLS-4,3,[cx]);lineTerrain(3,ROWS-4,COLS-4,ROWS-4,[cx]);lineTerrain(3,3,3,ROWS-4,[cy]);lineTerrain(COLS-4,3,COLS-4,ROWS-4,[cy]);lineTerrain(7,7,COLS-8,7,[9,cx,COLS-10]);lineTerrain(7,ROWS-8,COLS-8,ROWS-8,[9,cx,COLS-10]);lineTerrain(7,7,7,ROWS-8,[cy]);lineTerrain(COLS-8,7,COLS-8,ROWS-8,[cy]);
 }else customLateLayout(level.layout,cx,cy);
 for(let x=1;x<COLS-1;x++){carveLevel(x,cy);if(x%5===0)carveLevel(x,cy-1);}
 for(let y=1;y<ROWS-1;y++){carveLevel(cx,y);if(y%5===0)carveLevel(cx+1,y);}
 for(let y=cy-2;y<=cy+2;y++)for(let x=cx-2;x<=cx+2;x++)carveLevel(x,y);
 renderTerrain(t,rng);
 for(let i=0;i<34;i++){
   const x=1+Math.floor(rng()*(COLS-2)),y=1+Math.floor(rng()*(ROWS-2));if(terrain.has(K(x,y))||Math.abs(x-cx)+Math.abs(y-cy)<5)continue;const p=worldPos(x,y,0);
   if(i%3===0){const post=box(.10,1.55,.10,t.trim,.45,.55);post.position.set(p.x,.78,p.z);decorGroup.add(post);const lamp=sphere(.10,t.accent,9);lamp.material.emissive.setHex(t.accent);lamp.material.emissiveIntensity=1.8;lamp.position.set(p.x,1.62,p.z);decorGroup.add(lamp);}
 }
}
