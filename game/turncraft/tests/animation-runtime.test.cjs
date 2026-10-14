const {test}=require('node:test');
const assert=require('node:assert/strict');
const {SpriteRuntime,TextureCache,assetTypeFor,selectionRenderContext,frameForState,sampleSegment,directionForVector,eventProgress}=require('../animation.js');
const manifest={assets:{rifleman:{anchor:[.5,.78],states:{idle:{sheet:'idle.webp',frames:4,fps:4,frameWidth:96,frameHeight:128,directions:['S'],rows:{S:0}},move:{sheet:'move.webp',frames:8,fps:8},attack:{sheet:'attack.webp',frames:6,fps:10,contactMs:200,loop:false},hit:{frames:3,fps:15},death:{sheet:'death.webp',frames:8,fps:10,loop:false}}}}};
const unit=()=>({id:1,type:'rifleman',kind:'unit',faction:0,x:2,y:3,px:2,py:3,moveReadyAt:0,attackReadyAt:0,abilityReadyAt:0});
const options={moveDuration:()=>1000,isVisible:()=>true};

test('timer-based interpolation preserves logical tiles and cooldowns at every frame rate',()=>{
  for(const step of [8,16,33,80]){const runtime=new SpriteRuntime({manifest}),entity=unit();runtime.beginFrame([entity],1000,options);entity.x=5;entity.moveReadyAt=2000;runtime.move(entity,null,1000,1000);for(let t=1000;t<1500;t+=step)runtime.beginFrame([entity],t,options);runtime.beginFrame([entity],1500,options);assert.equal(entity.x,5);assert.equal(entity.px,3.5);assert.equal(entity.moveReadyAt,2000);assert.equal(runtime.states.get(1).state,'move');runtime.beginFrame([entity],2000,options);assert.equal(entity.px,5);assert.equal(runtime.states.get(1).state,'idle');assert.equal(entity.animation,undefined)}
});

test('existing planned corner route is sampled by arc length without cutting through obstacles',()=>{
  const runtime=new SpriteRuntime({manifest}),entity=unit();runtime.beginFrame([entity],1000,options);runtime.route(entity,[{x:3,y:3},{x:3,y:4},{x:3,y:5}]);entity.x=3;entity.y=5;entity.moveReadyAt=2000;runtime.move(entity,null,1000,1000);runtime.beginFrame([entity],1500,options);assert.equal(entity.px,3);assert.equal(entity.py,3.5);assert.equal(runtime.states.get(1).direction,'S');assert.equal(sampleSegment(runtime.states.get(1).segment,2000).moving,false)
});

test('connected segments keep phase; corrections start at displayed position and do not restart unchanged snapshots',()=>{
  const runtime=new SpriteRuntime({manifest}),entity=unit();runtime.beginFrame([entity],1000,options);entity.x=4;entity.moveReadyAt=2000;runtime.move(entity,null,1000,1000);runtime.beginFrame([entity],1500,options);const phase=runtime.states.get(1).phaseAt;entity.x=6;entity.moveReadyAt=2500;runtime.beginFrame([entity],1500,{...options,remote:true});assert.equal(entity.px,3);runtime.beginFrame([entity],1750,{...options,remote:true});assert.equal(entity.px,3.75);assert.equal(runtime.states.get(1).segment.start,1500);assert.equal(runtime.states.get(1).phaseAt,phase);runtime.move(entity,null,1800,1000,{teleport:true});assert.equal(runtime.states.get(1).x,6);assert.equal(runtime.states.get(1).segment,null)
});

test('direction hysteresis prevents jitter around sector edges without mirroring asymmetric art',()=>{
  assert.equal(directionForVector(1,-2.1,'N'),'N');assert.equal(directionForVector(1,-1,'N'),'NE');assert.equal(directionForVector(0,0,'W'),'W');assert.equal(directionForVector(-1,0,'S'),'W');const runtime=new SpriteRuntime({manifest}),entity=unit();runtime.ensure(entity,0);runtime.signal(entity,'attack',100,{target:{x:8,y:3}});const state=runtime.states.get(1);assert.equal(state.direction,'E');assert.equal(state.previousDirection,'S');assert.equal(state.turnAt,100);runtime.signal(entity,'attack',300,{origin:{x:9,y:3},target:{x:8,y:3}});assert.equal(state.direction,'W','facing follows the rendered firing position without changing logical coordinates');assert.equal(entity.x,2)
});

test('turning draws one facing body at every quality without changing movement or attack timing',()=>{
  for(const quality of ['low','medium','high']){
    const runtime=new SpriteRuntime({manifest,quality}),entity=unit(),draws=[];
    runtime.drawState=(ctx,type,state,options)=>{draws.push({type,state,...options});return true};
    runtime.beginFrame([entity],1000,options);entity.x=4;entity.moveReadyAt=2000;runtime.move(entity,null,1000,1000);
    runtime.beginFrame([entity],1055,options);runtime.draw({},entity,1055,{zoom:1});
    assert.equal(draws.length,1,quality+': one body during a movement turn');
    assert.equal(draws[0].direction,'E');assert.equal(draws[0].alpha,1);assert.equal(draws[0].rate,2);
    assert.equal(entity.px,2.11);assert.equal(entity.x,4);assert.equal(entity.moveReadyAt,2000);assert.equal(runtime.states.get(1).segment.end,2000);
    draws.length=0;runtime.signal(entity,'attack',1100,{target:{x:0,y:3},contactAt:1100});
    runtime.beginFrame([entity],1155,options);runtime.draw({},entity,1155,{zoom:1});
    assert.equal(draws.length,1,quality+': one body during an attack turn');
    assert.equal(draws[0].direction,'W');assert.equal(draws[0].alpha,1);
    assert.equal(draws[0].progress,eventProgress(manifest.assets.rifleman.states.attack,runtime.states.get(1).event,1155));
    assert.equal(runtime.states.get(1).event.end,1620);assert.equal(entity.moveReadyAt,2000);
  }
});

test('large selected armies build visual selection and target sets once using indexed lookups',()=>{
  let lookups=0;class CountedMap extends Map{get(id){lookups++;return super.get(id)}}
  const entities=Array.from({length:1200},(_,id)=>({id:id+1,kind:'unit',alive:true,targetId:id<400?id+801:null})),index=new CountedMap(entities.map(e=>[e.id,e])),ids=entities.slice(0,400).map(e=>e.id),context=selectionRenderContext(entities,ids,ids[0],index);
  assert.equal(lookups,400);assert.equal(context.selected.size,400);assert.equal(context.targets.size,400);for(let frame=0;frame<5;frame++)for(const entity of entities){context.selected.has(entity.id);context.targets.has(entity.id)}assert.equal(lookups,400,'per-sprite checks must not perform entity lookups');
  const fallback=selectionRenderContext(entities,[],1);assert.equal(fallback.selected.has(1),true);assert.equal(fallback.targets.has(801),true);entities[0].alive=false;assert.equal(selectionRenderContext(entities,[1],1).targets.has(801),false)
});

test('attack contact frames align to existing launch and melee impact clocks',()=>{
  const definition=manifest.assets.rifleman.states.attack;
  const shot={kind:'attack',start:1000,end:1500,contactAt:1000};assert.equal(eventProgress(definition,shot,1000),1/3);assert.equal(frameForState(definition,'S',0,{progress:eventProgress(definition,shot,1000)}).index,2);
  const melee={kind:'attack',start:1000,end:1520,contactAt:1270};assert.equal(eventProgress(definition,melee,1000),0);assert.equal(eventProgress(definition,melee,1270),1/3);assert.equal(eventProgress(definition,melee,1520),1)
  const frameMarked={frames:5,fps:10,contactFrame:2,contactMs:0,loop:false};assert.equal(frameForState(frameMarked,'S',0,{progress:eventProgress(frameMarked,shot,1000)}).index,2);assert.equal(frameForState(frameMarked,'S',0,{progress:eventProgress(frameMarked,melee,1270)}).index,2)
});

test('atlas coordinates, directional rows and nonlooping final frames remain inside their frame',()=>{
  const state={sheet:'troop.webp',directions:['N','E','S','W'],frames:6,frameWidth:96,frameHeight:128,rows:{N:0,E:1,S:2,W:3},firstColumn:4,fps:10,loop:false};assert.deepEqual({...frameForState(state,'E',500),cycle:0},{x:864,y:128,w:96,h:128,index:5,direction:'E',cycle:0});assert.equal(frameForState(state,'NE',100).direction,'S');assert.equal(frameForState(state,'N',10000).index,5);assert.equal(frameForState({...state,loop:true},'N',600).index,0);assert.equal(frameForState({coordinates:{S:[{x:5,y:7,w:20,h:25}]}},'S',0).x,5)
});

test('visible local deaths are bounded cosmetic objects; hidden deaths and garrison losses create none',()=>{
  const runtime=new SpriteRuntime({manifest,quality:'low'});for(let id=1;id<=100;id++){const entity={...unit(),id};assert.equal(runtime.kill(entity,1000,true),true)}assert.equal(runtime.ghosts.length,24);assert.equal(runtime.kill({...unit(),id:101},1000,false),false);assert.equal(runtime.kill({...unit(),id:102,garrisonedIn:7},1000,true),false);const draws=[],ctx={save(){},restore(){},globalAlpha:1};runtime.drawGhosts(ctx,1100,{visible:()=>false,worldToScreen:(x,y)=>({x,y}),fallback:()=>draws.push(1)});assert.equal(draws.length,0);runtime.beginFrame([],3000);assert.equal(runtime.ghosts.length,0);assert.equal(runtime.states.size,0)
});

test('authoritative revival cancels a same-ID death ghost without modifying the restored unit',()=>{
  const runtime=new SpriteRuntime({manifest}),entity=unit();runtime.beginFrame([entity],1000);runtime.kill(entity,1100,true);entity.alive=false;runtime.beginFrame([entity],1150);assert.equal(runtime.ghosts.length,1);const restored={...entity,alive:true,hp:75,x:8,y:9,px:8,py:9};runtime.beginFrame([restored],1200);assert.equal(runtime.ghosts.length,0);assert.equal(runtime.ghostIds.has(entity.id),false);assert.equal(restored.hp,75);assert.equal(restored.x,8);assert.equal(runtime.states.get(entity.id).x,8)
});

test('remote damage/ability and persistent work/production/construction/deploy states observe existing data',()=>{
  const runtime=new SpriteRuntime({manifest}),entity=unit();runtime.beginFrame([entity],1000,options);entity.lastDamagedAt=1100;runtime.beginFrame([entity],1100,options);assert.equal(runtime.states.get(1).state,'hit');entity.abilityReadyAt=5000;runtime.beginFrame([entity],1300,options);assert.equal(runtime.states.get(1).state,'ability');runtime.beginFrame([entity],2500,{...options,workTarget:()=>({x:1,y:3})});assert.equal(runtime.states.get(1).state,'work');assert.equal(runtime.states.get(1).direction,'W');entity.queue=[{}];runtime.beginFrame([entity],2600,options);assert.equal(runtime.states.get(1).state,'production');entity.fabricatorDeployAt=4000;runtime.beginFrame([entity],2700,options);assert.equal(runtime.states.get(1).state,'deploy');entity.complete=false;runtime.beginFrame([entity],2800,options);assert.equal(runtime.states.get(1).state,'construction')
});

test('larvae and cocoons use distinct cosmetic mappings and morph by timer without rewriting the logical type',()=>{
  const assets={larva:{states:{idle:{frames:4,fps:4},hit:{frames:2,fps:12},death:{frames:6,fps:10}}},cocoon:{states:{idle:{frames:4,fps:4},hit:{frames:2,fps:12},death:{frames:6,fps:10},morph:{frames:6,fps:6,loop:false}}},rifleman:manifest.assets.rifleman},runtime=new SpriteRuntime({manifest:{assets}}),entity={id:7,type:'larva',kind:'larva',faction:2,x:3,y:4,hp:35};
  runtime.beginFrame([entity],1000);const larva=runtime.states.get(7);assert.equal(larva.assetType,'larva');assert.equal(assetTypeFor(entity),'larva');
  entity.kind='cocoon';entity.morphType='rifleman';entity.morphEnd=6000;runtime.beginFrame([entity],2000);const cocoon=runtime.states.get(7);assert.notEqual(cocoon,larva);assert.equal(cocoon.kind,'cocoon');assert.equal(cocoon.assetType,'cocoon');assert.equal(cocoon.state,'morph');assert.equal(entity.type,'larva');assert.equal(entity.morphEnd,6000);
  let drawn;runtime.drawState=(ctx,type,state,options)=>{drawn={type,state,progress:options.progress};return true};runtime.draw({},entity,3500,{morphProgress:.5});assert.deepEqual(drawn,{type:'cocoon',state:'morph',progress:.5});
  runtime.kill(entity,4000,true);assert.equal(runtime.ghosts[0].type,'cocoon');assert.equal(runtime.ghosts[0].kind,'cocoon');
  entity.kind='unit';entity.type='rifleman';delete entity.morphType;delete entity.morphEnd;runtime.beginFrame([entity],6000);assert.equal(runtime.states.get(7).assetType,'rifleman');assert.equal(runtime.states.get(7).state,'idle');assert.equal(entity.animation,undefined)
});

test('missing assets fall back and an atlas is decoded once; LRU eviction enforces byte budget with load concurrency',async()=>{
  let created=0;const pending=[];const cache=new TextureCache({baseUrl:'http://game.test/turncraft.html',maxBytes:4096,concurrency:2,createImage:()=>{created++;const image={naturalWidth:32,naturalHeight:32};Object.defineProperty(image,'src',{set(){pending.push(image)}});return image}});
  cache.get('one.webp');cache.get('one.webp');cache.get('two.webp');cache.get('three.webp');assert.equal(created,2);assert.equal(cache.active,2);pending.shift().onload();assert.equal(created,3);pending.shift().onload();pending.shift().onload();assert.ok(cache.bytes<=4096);assert.equal(cache.active,0);assert.equal(cache.get('one.webp'),null);assert.equal(created,3);const failed=new TextureCache({createImage:()=>{const image={};Object.defineProperty(image,'src',{set(){queueMicrotask(()=>image.onerror())}});return image}});assert.equal(failed.get('missing.webp'),null);await new Promise(resolve=>setImmediate(resolve));assert.equal(failed.get('missing.webp'),null);assert.equal(failed.entries.get('missing.webp').status,'failed');assert.equal(frameForState(null,'S',0),null)
});

test('quality downsamples decoded atlases once and scales source rectangles without changing metadata or destination size',()=>{
  const pending=[],canvases=[],cache=new TextureCache({createImage:()=>{const image={naturalWidth:320,naturalHeight:256};Object.defineProperty(image,'src',{set(value){if(value)pending.push(image)}});return image},createCanvas:()=>{const context={drawImage(...args){this.arguments=args}},canvas={width:0,height:0,getContext:()=>context};canvases.push(canvas);return canvas}}),metadata={assets:{troop:{anchor:[.5,.5],states:{idle:{sheet:'troop.webp',frameWidth:80,frameHeight:64,frames:3,firstColumn:1,rows:{S:2},directions:['S'],fps:3,loop:false}}}}},runtime=new SpriteRuntime({manifest:metadata,cache,quality:'low'}),calls=[],context={globalAlpha:1,save(){},restore(){},drawImage(...args){calls.push(args)}};
  assert.equal(runtime.drawState(context,'troop','idle',{elapsedMs:1000,size:48}),false);pending.shift().onload();assert.equal(cache.sourceScale('troop.webp'),.5);assert.equal(canvases[0].width,160);assert.equal(canvases[0].height,128);assert.equal(cache.bytes,160*128*4);assert.equal(runtime.drawState(context,'troop','idle',{elapsedMs:1000,size:48}),true);assert.deepEqual(calls[0].slice(1,5),[120,64,40,32]);assert.deepEqual(calls[0].slice(7),[48,38.4]);assert.equal(metadata.assets.troop.states.idle.frameWidth,80);
  runtime.setQuality('medium');assert.equal(cache.resolution,.75);assert.equal(cache.bytes,0);assert.equal(cache.entries.size,0);runtime.drawState(context,'troop','idle',{elapsedMs:1000,size:48});pending.shift().onload();assert.equal(cache.sourceScale('troop.webp'),.75);assert.equal(cache.get('troop.webp').width,240);runtime.drawState(context,'troop','idle',{elapsedMs:1000,size:48});assert.deepEqual(calls[1].slice(1,5),[180,96,60,48]);runtime.setQuality('high');assert.equal(cache.resolution,1);assert.equal(cache.bytes,0)
});

test('quality changes discard stale decode results without exceeding two simultaneous image decodes',()=>{
  const pending=[],cache=new TextureCache({createImage:()=>{const image={naturalWidth:320,naturalHeight:256};Object.defineProperty(image,'src',{set(value){if(value)pending.push(image)}});return image},createCanvas:()=>({width:0,height:0,getContext:()=>({drawImage(){}})})});
  cache.get('old-one.webp');cache.get('old-two.webp');assert.equal(cache.active,2);cache.setResolution(.5);cache.get('new.webp');assert.equal(cache.active,2);assert.equal(pending.length,2);pending.shift().onload();assert.equal(cache.active,2);assert.equal(pending.length,2);pending.shift().onload();assert.equal(cache.active,1);pending.shift().onload();assert.equal(cache.active,0);assert.equal(cache.entries.has('old-one.webp'),false);assert.equal(cache.entries.has('old-two.webp'),false);assert.equal(cache.entries.size,1);assert.equal(cache.bytes,160*128*4);assert.equal(cache.sourceScale('new.webp'),.5)
});

test('explicit symmetric directional flips preserve scaled source frames and destination bounds',()=>{
  const sheet={width:160,height:128},cache={setBudget(){},setResolution(){},get:()=>sheet,sourceScale:()=>.5},definition={anchor:[.3,.7],states:{idle:{sheet:'brood.webp',frameWidth:80,frameHeight:64,frames:2,fps:4,firstColumn:1,directions:['E','S'],rows:{E:1,S:0},directionFlipX:{E:true}}}},runtime=new SpriteRuntime({manifest:{assets:{drone:definition}},cache}),operations=[],context={globalAlpha:1,save(){},restore(){},translate(x,y){operations.push(['translate',x,y])},scale(x,y){operations.push(['scale',x,y])},drawImage(...args){operations.push(['draw',...args.slice(1)])}};
  assert.equal(runtime.drawState(context,'drone','idle',{direction:'E',elapsedMs:0,size:40,x:12,y:20}),true);assert.deepEqual(operations[0],['translate',12,20]);assert.deepEqual(operations[1],['scale',-1,1]);const draw=operations[2];assert.deepEqual(draw.slice(1,5),[40,32,40,32]);assert.equal(12-(draw[5]+draw[7]),0);assert.equal(12-draw[5],40);assert.ok(Math.abs(20+draw[6]+2.4)<1e-9);
  operations.length=0;runtime.drawState(context,'drone','idle',{direction:'S',elapsedMs:0,size:40,x:12,y:20});assert.equal(operations.length,1,'directions without an explicit flag remain unmirrored');assert.equal(operations[0][5],0);assert.ok(Math.abs(operations[0][6]+2.4)<1e-9);const bounds=runtime.bounds({id:99,kind:'unit',type:'drone',x:0,y:0},{size:40});assert.equal(bounds.width,40);assert.equal(bounds.height,32)
});

test('measured atlas crops are repacked before downsampling and keep original drawing scale and hit bounds',()=>{
  const images=[],blits=[],canvases=[],repair={width:384,height:144,frameWidth:192,frameHeight:144,sourceFrameWidth:96,crops:[[[12,20,80,100],[110,18,75,103]]]},cache=new TextureCache({createImage:()=>{const im={width:192,height:144};images.push(im);return im},createCanvas:()=>{const c={getContext:()=>({drawImage(...args){blits.push(args)}})};canvases.push(c);return c}});
  const definition={anchor:[.5,.78],states:{idle:{sheet:'buggy.webp',frameWidth:96,frameHeight:144,frames:2,fps:4,directions:['S'],rows:{S:0}}}},runtime=new SpriteRuntime({manifest:{cropRepairs:{'buggy.webp':repair},assets:{hellion:definition}},cache,quality:'low'});
  cache.get('buggy.webp');images[0].onload();assert.equal(blits.length,3,'two isolated crops, then one downsample');
  assert.deepEqual(blits[0].slice(1),[12,20,80,100,56,20,80,100]);assert.deepEqual(blits[2].slice(1),[0,0,192,72]);assert.equal(cache.frameExpansion('buggy.webp'),2);assert.equal(cache.bytes,192*72*4);
  const draws=[],context={globalAlpha:1,save(){},restore(){},drawImage(...args){draws.push(args.slice(1))}};
  assert.equal(runtime.drawState(context,'hellion','idle',{size:48,progress:1}),true);assert.deepEqual(draws[0].slice(0,4),[96,0,96,72]);assert.equal(draws[0][6],96,'wider transparent cell does not squash the unit');assert.equal(draws[0][7],72,'unit vertical scale remains unchanged');
  const bounds=runtime.bounds({id:1,type:'hellion',kind:'unit',x:0,y:0},{size:48});assert.equal(bounds.width,48);assert.equal(bounds.height,72);
  cache.setCropRepairs({});assert.equal(cache.entries.size,0,'manifest changes invalidate old repaired textures');assert.equal(cache.bytes,0);
});

test('a bad crop or missing repair canvas falls back instead of drawing the contaminated sheet',()=>{
  for(const createCanvas of [()=>null,()=>({getContext:()=>({drawImage(){}})})]){
    const images=[],cache=new TextureCache({createImage:()=>{const im={width:96,height:144};images.push(im);return im},createCanvas});
    cache.setCropRepairs({'broken.webp':{width:192,height:144,frameWidth:192,frameHeight:144,sourceFrameWidth:96,crops:[[[90,0,20,144]]]}});
    cache.get('broken.webp');images[0].onload();assert.equal(cache.get('broken.webp'),null);assert.equal(cache.entries.get('broken.webp').status,'failed');assert.equal(cache.bytes,0);assert.equal(cache.active,0);
  }
});

test('crops crossing a nominal row repack with vertical padding before quality scaling and retain the original pivot',()=>{
  const images=[],blits=[],repair={width:192,height:384,frameWidth:192,frameHeight:192,sourceFrameWidth:96,sourceFrameHeight:144,
    crops:[[[12,120,80,50]],[[24,130,70,80]]],destinationOffsets:[[[56,120]],[[61,10]]]},
    cache=new TextureCache({createImage:()=>{const image={width:192,height:288};images.push(image);return image},
      createCanvas:()=>({getContext:()=>({drawImage(...args){blits.push(args)}})})}),
    definition={anchor:[.5,.78],states:{idle:{sheet:'drifting.webp',frameWidth:96,frameHeight:144,frames:1,fps:4,directions:['N','S'],rows:{N:0,S:1},directionFlipX:{N:true}}}},
    runtime=new SpriteRuntime({manifest:{cropRepairs:{'drifting.webp':repair},assets:{broodling:definition}},cache,quality:'low'});
  cache.get('drifting.webp');images[0].onload();
  assert.deepEqual(blits[0].slice(1),[12,120,80,50,56,120,80,50],'the source body may cross the old row boundary');
  assert.deepEqual(blits[1].slice(1),[24,130,70,80,61,202,70,80]);
  assert.deepEqual(blits[2].slice(1),[0,0,96,192],'repair happens before downsampling');
  assert.equal(cache.frameExpansion('drifting.webp'),2);assert.equal(cache.frameExpansionY('drifting.webp'),4/3);assert.equal(cache.bytes,96*192*4);
  const draws=[],context={globalAlpha:1,save(){},restore(){},translate(){},scale(){},drawImage(...args){draws.push(args.slice(1))}};
  assert.equal(runtime.drawState(context,'broodling','idle',{direction:'S',size:48,x:12,y:100}),true);
  assert.deepEqual(draws[0].slice(0,4),[0,96,96,96]);assert.deepEqual(draws[0].slice(6),[96,96]);
  assert.equal(draws[0][4],-36);assert.ok(Math.abs(draws[0][5]-(100-72*.78-12))<1e-9,'extra vertical padding is centered around the old cell');
  const bounds=runtime.bounds({id:1,type:'broodling',kind:'unit',x:0,y:0},{size:48});
  assert.equal(bounds.width,48);assert.equal(bounds.height,72,'transparent padding must not enlarge hit bounds');
  draws.length=0;runtime.drawState(context,'broodling','idle',{direction:'N',size:48,x:12,y:100});
  assert.deepEqual(draws[0].slice(0,4),[0,0,96,96]);assert.ok(Math.abs(draws[0][5]-(-72*.78-12))<1e-9,'mirroring retains the same vertical pivot');
});
