/* Verify equipment on the actual skinned assets and the playable fallback.
 * THREE_BUNDLE and BASKETBALL_CHROME can provide offline test dependencies. */
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const {installLegacyGraphicsFixture}=require('./legacy-graphics-fixture.cjs');
const root=path.resolve(__dirname,'../../..');

(async()=>{
  const options={headless:true,args:['--use-gl=angle','--use-angle=swiftshader','--enable-webgl']};
  if(process.env.BASKETBALL_CHROME) options.executablePath=process.env.BASKETBALL_CHROME;
  const browser=await chromium.launch(options);
  try{
    const page=await browser.newPage({viewport:{width:1280,height:800}}),errors=[];
    page.on('pageerror',error=>errors.push(error.stack));
    await page.route('http://basketball.local/**',route=>{
      let file=path.join(root,new URL(route.request().url()).pathname);
      if(fs.existsSync(file)&&fs.statSync(file).isDirectory()) file=path.join(file,'index.html');
      if(!fs.existsSync(file)) return route.fulfill({status:404,body:'Not found'});
      if(file.endsWith('bootstrap.js')) return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
      const types={'.html':'text/html','.css':'text/css','.js':'text/javascript','.glb':'model/gltf-binary','.webp':'image/webp','.json':'application/json'};
      return route.fulfill({path:file,contentType:types[path.extname(file)]||'application/octet-stream'});
    });
    if(process.env.THREE_BUNDLE) await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
    await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}}));
    await installLegacyGraphicsFixture(page);

    await page.goto('http://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);

    const models=await page.evaluate(()=>{
      teamSize=3;twoPlayerMode=true;localTeamMode='competitive';
      selectedP1Equipment={jersey:{color:'#be241a'},shorts:{color:'#1351bb'},shoes:{color:'#7623ce',accent:'#ccab32'},socks:{color:'#f0a123'},headband:{color:'#11cf76',enabled:true},sleeve:{color:'#1977ba',enabled:true,side:'left'},wristband:{color:'#bb379c',enabled:true,side:'left'},accessory:{color:'#acfa12',enabled:true,side:'both'}};
      selectedP2Equipment={jersey:{color:'#31a267'},shorts:{color:'#e7bd31'},shoes:{color:'#dd362a'}};
      selectedP1Sneaker='tower';selectedP2Sneaker='classic';startGame();
      const colors=player=>Object.fromEntries(player.playerModel.materials.filter(material=>['jersey','shorts','sneaker','skin','sock','trim','shorts_trim','sneaker_accent'].includes(material.name)).map(material=>[material.name,material.color.getHex()]));
      const first=colors(p1),second=colors(p2),skin=p1.profile.skin;
      const otherColors=matchPlayers.filter(player=>player!==p1).map(player=>JSON.stringify(colors(player)));
      const shared=matchPlayers.find(player=>player!==p1&&getBasketballPlayerModelKind(player)===getBasketballPlayerModelKind(p1));
      const sameGeometry=shared ? p1.playerModel.nodes.find(node=>node.name==='Athlete').children[0].geometry===shared.playerModel.nodes.find(node=>node.name==='Athlete').children[0].geometry : true;
      updateBasketballPlayerEquipment(p1,{...selectedP1Equipment,shoes:{color:'#ed380e',accent:'#0beecf',sole:'#312a11'},shorts:{color:'#678901'}});
      const updated=colors(p1),othersUnchanged=matchPlayers.filter(player=>player!==p1).every((player,index)=>JSON.stringify(colors(player))===otherColors[index]);
      const accessories=p1.playerModel.equipmentNodes.map(entry=>({region:entry.region,side:entry.side,visible:entry.node.visible,color:entry.node.material.color.getHex()}));
      const wristbands=p1.playerModel.materials.filter(material=>material.name.startsWith('wristband_')).map(material=>({name:material.name,visible:material.visible,color:material.color.getHex()}));
      const teammate=matchPlayers.find(player=>player.teamId===0&&player.id!=='p1');
      const teammateSneaker={actual:teammate.sneaker.type,expected:sneakers.tower.type,primary:teammate.appearance.regions.shoes.color};
      updateBasketballPlayerEquipment(p1,{socks:{enabled:false},headband:{enabled:false},sleeve:{enabled:false},wristband:{enabled:false},accessory:{enabled:false}});
      const hidden=p1.playerModel.equipmentNodes.every(entry=>!entry.node.visible)&&p1.playerModel.materials.filter(material=>['sock','sock_trim','wristband_left','wristband_right'].includes(material.name)).every(material=>!material.visible);
      updateBasketballPlayerEquipment(p1,{});
      const reset=colors(p1);
      onResize();renderScene();
      return {first,second,updated,reset,skin,othersUnchanged,sameGeometry,accessories,wristbands,teammateSneaker,hidden,baseJersey:p1.baseJerseyStyle.primary};
    });
    assert.equal(models.first.jersey,0xbe241a);assert.equal(models.first.shorts,0x1351bb);assert.equal(models.first.sneaker,0x7623ce);assert.equal(models.first.sock,0xf0a123);
    assert.equal(models.second.jersey,0x31a267);assert.equal(models.second.shorts,0xe7bd31);assert.equal(models.second.sneaker,0xdd362a);
    assert.equal(models.updated.sneaker,0xed380e);assert.equal(models.updated.sneaker_accent,0x0beecf);assert.equal(models.updated.shorts,0x678901);
    assert.equal(models.updated.jersey,models.first.jersey);assert.equal(models.updated.skin,models.skin);assert.ok(models.othersUnchanged);assert.ok(models.sameGeometry);
    assert.equal(models.reset.jersey,models.baseJersey);assert.ok(models.hidden);
    assert.equal(models.teammateSneaker.actual,models.teammateSneaker.expected);assert.equal(models.teammateSneaker.primary,0x7623ce);
    assert.ok(models.accessories.find(item=>item.region==='headband').visible);
    assert.ok(models.accessories.filter(item=>item.region==='sleeve').every(item=>item.visible===(item.side==='left')));
    assert.ok(models.accessories.filter(item=>item.region==='accessory').every(item=>item.visible));
    assert.ok(models.wristbands.every(item=>item.visible===item.name.endsWith('_left')&&item.color===0xbb379c));
    console.log('PASS: real GLB independent clothing, shoe accents, accessories, team sneakers, live reset and shared immutable geometry');

    const fallback=await page.evaluate(()=>{
      globalThis.savedEquipmentAssets=new Map(basketballPlayerModels.assets);basketballPlayerModels.assets.clear();
      teamSize=2;twoPlayerMode=true;localTeamMode='cooperative';selectedP1='flash';selectedP1Equipment={};selectedP2Equipment={};startGame();
      const material=(player,name)=>player.equipmentMaterials.find(item=>item.name===name);
      const snapshot=player=>player.group.children.filter(node=>node.material?.color&&![player.shadow,player.hookIndicator].includes(node)).map(node=>({uuid:node.material.uuid,color:node.material.color.getHex()}));
      const oldJersey=material(p1,'jersey').map,oldShorts=material(p1,'shorts').map,oldShoes=material(p1,'sneaker').map;
      const jerseyImage=oldJersey.image.toDataURL(),other=JSON.stringify(snapshot(p2)),skin=p1.head.material.color.getHex();
      let jerseyDisposed=0,shortsDisposed=0,shoesDisposed=0;
      oldJersey.addEventListener('dispose',()=>jerseyDisposed++);oldShorts.addEventListener('dispose',()=>shortsDisposed++);oldShoes.addEventListener('dispose',()=>shoesDisposed++);
      updateBasketballPlayerEquipment(p1,{shorts:{color:'#153dbb'},shoes:{color:'#ed240b',accent:'#bc51de'},headband:{color:'#10eea1',enabled:true},sleeve:{enabled:true,color:'#2468ac',side:'left'},wristband:{enabled:true,color:'#dfa923',side:'right'},socks:{color:'#34bb62'},accessory:{enabled:true,color:'#cffa12',side:'both'}});
      const sameJerseyImage=jerseyImage===material(p1,'jersey').map.image.toDataURL();
      const pixel=texture=>Array.from(texture.image.getContext('2d').getImageData(2,35,1,1).data);
      const shortsPixel=pixel(material(p1,'shorts').map),headband=p1.equipmentNodes.find(entry=>entry.region==='headband');
      const selected=p1.equipmentNodes.map(entry=>({region:entry.region,side:entry.side,visible:entry.node.visible,color:entry.node.material.color.getHex()}));
      const allHeadbands=[];p1.group.traverse(node=>{if(node.material?.name==='headband')allHeadbands.push(node);});
      const accentMaterials=p1.equipmentMaterials.filter(item=>item.name==='sneaker_accent');
      const accentUpdated=accentMaterials.every(item=>item.color.getHex()===0xbc51de&&(!item.emissiveIntensity||item.emissive.getHex()===0xbc51de));
      const result={fallback:matchPlayers.every(player=>!player.playerModel),sameJerseyImage,shortsPixel,skinBefore:skin,skinAfter:p1.head.material.color.getHex(),otherUnchanged:other===JSON.stringify(snapshot(p2)),selected,headbandColor:headband.node.material.color.getHex(),headbands:allHeadbands.length,disposed:[jerseyDisposed,shortsDisposed,shoesDisposed],accentUpdated};
      updateBasketballPlayerEquipment(p1,{headband:{enabled:false},sleeve:{enabled:false},wristband:{enabled:false},socks:{enabled:false},accessory:{enabled:false}});
      result.hidden=p1.equipmentNodes.every(entry=>!entry.node.visible);
      for(const [kind,asset] of savedEquipmentAssets)basketballPlayerModels.assets.set(kind,asset);
      return result;
    });
    assert.ok(fallback.fallback);assert.ok(fallback.sameJerseyImage,'shorts changes must preserve every jersey texture pixel');
    assert.deepEqual(fallback.shortsPixel,[0x15,0x3d,0xbb,255]);assert.equal(fallback.skinAfter,fallback.skinBefore);assert.ok(fallback.otherUnchanged);
    assert.deepEqual(fallback.disposed,[1,1,1]);assert.ok(fallback.accentUpdated);assert.equal(fallback.headbands,1);assert.equal(fallback.headbandColor,0x10eea1);assert.ok(fallback.hidden);
    assert.ok(fallback.selected.filter(item=>item.region==='sleeve').every(item=>item.visible===(item.side==='left')));
    assert.ok(fallback.selected.filter(item=>item.region==='wristband').every(item=>item.visible===(item.side==='right')));
    console.log('PASS: procedural fallback texture pixels, no skin/player color bleed, per-side accessories and replaced-texture disposal');

    const saved=await page.evaluate(()=>{
      selectedP1Equipment={jersey:{color:'#c12423'},shorts:{color:'#2145ca'},shoes:{color:'#456def'},sleeve:{color:'#17be65',enabled:true,side:'both'}};
      selectedP2Equipment={headband:{color:'#ed1a32',enabled:true},socks:{enabled:false}};saveMatchSetup();
      const expected=[selectedP1Equipment,selectedP2Equipment];selectedP1Equipment={};selectedP2Equipment={};loadMatchSetup();
      const restored=[selectedP1Equipment,selectedP2Equipment];
      startGame();
      return {restored,expected,color:p1.playerModel.materials.find(material=>material.name==='jersey').color.getHex(),sleeves:p1.playerModel.equipmentNodes.filter(entry=>entry.region==='sleeve').every(entry=>entry.node.visible)};
    });
    assert.deepEqual(saved.restored,saved.expected);assert.equal(saved.color,0xc12423);assert.ok(saved.sleeves);
    const cues=await page.evaluate(()=>{
      takePossession(p1,{silent:true});const player=getTeammates(p1).find(char=>char!==p1);player.hasBall=false;player.velocity.set(0,0,0);player.aiScreen=true;player.aiBrain={role:'screen'};
      for(let i=0;i<20;i++)updatePlayerModel(player,1/60);
      const screen=player.playerModel.mode,screenHands=['left','right'].map(side=>getModelHandWorld(player,side).distanceTo(player.group.position));
      player.aiScreen=false;player.aiBrain.role='cut';player.velocity.set(4,0,0);updatePlayerModel(player,1/60);const cut=player.playerModel.mode;
      takePossession(getOpponents(player)[0],{silent:true});player.aiBrain.role='closeout';player.velocity.set(5,0,0);updatePlayerModel(player,1/60);const closeout=player.playerModel.mode;
      takePossession(player,{silent:true});player.catchAnimation=.2;updatePlayerModel(player,1/60);const receive=player.playerModel.mode;
      const head=player.playerModel.bones.get('head');player.catchAnimation=0;player.velocity.set(0,0,0);player.anim.headYaw=.4;
      for(let i=0;i<240;i++)updatePlayerModel(player,1/60);
      const headYaw=head.rotation.y;
      ballState='loose';player.hasBall=false;player.aiBrain.role='box_out';updatePlayerModel(player,1/60);const boxOut=player.playerModel.mode;
      player.aiBrain.role='rebound';player.blockTime=.3;updatePlayerModel(player,1/60);const rebound=player.playerModel.mode;
      return {screen,screenHands,cut,closeout,receive,boxOut,rebound,finite:[...player.playerModel.bones.values()].every(bone=>bone.quaternion.toArray().every(Number.isFinite)),headYaw};
    });
    assert.equal(cues.screen,'screen');assert.ok(cues.screenHands.every(distance=>distance>0&&distance<2));assert.equal(cues.cut,'cut');assert.equal(cues.closeout,'closeout');assert.equal(cues.receive,'receive');assert.equal(cues.boxOut,'box_out');assert.equal(cues.rebound,'rebound');assert.ok(cues.finite);assert.ok(Math.abs(cues.headYaw)<.8);
    assert.deepEqual(errors,[]);
    console.log('PASS: saved appearances restore on new match; screen, cut, closeout, receive, box-out, rebound and bounded head-look cues');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
