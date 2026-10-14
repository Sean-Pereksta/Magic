const {test}=require('node:test');
const assert=require('node:assert/strict');
const {createHash}=require('node:crypto');
const {SpriteRuntime,frameForState}=require('../animation.js');
const manifest=require('../assets/animation-manifest.json');
const structures=require('../assets/structure-animation-manifest.json').structures;
const terrain=require('../assets/terrain-manifest.json');

test('all structure loops and destruction play at half speed in both shipped manifests',()=>{
  assert.equal(Object.keys(structures).length,34);
  for(const [type,asset]of Object.entries(structures)){
    for(const name of ['idle','production','death']){
      const state=asset.states[name];assert.deepEqual(manifest.assets[type].states[name],state,type+' '+name+' must use the shipped timing');
      assert.equal(state.fps,name==='death'?3.5:3);
      const interval=1000/state.fps;
      assert.equal(frameForState(state,'S',interval-1).index,0);
      assert.equal(frameForState(state,'S',interval+1).index,1);
      if(name==='death')assert.equal(state.durationMs,Math.round(state.frames*interval));
    }
    const construction=asset.states.construction;assert.equal(construction.progressDriven,true);assert.equal(construction.fps,6);
    assert.equal(frameForState(construction,'S',100000,{progress:.5}).index,2,'construction still follows real build progress');
    if(asset.states.attack){assert.equal(asset.states.attack.fps,8);assert.equal(asset.states.attack.contactMs,125)}
  }
  for(const map of Object.values(terrain.maps))for(const name of ['mineral','gas','ambient']){
    const interval=name==='ambient'?520:480;
    assert.ok(Math.abs(map.props.states[name].fps-1000/interval)<.001,'terrain metadata matches runtime playback');
  }
});

test('the slower structure destruction extends only cosmetic ghosts and leaves construction timers intact',()=>{
  const runtime=new SpriteRuntime({manifest}),structure={id:1,type:'commandHub',kind:'structure',faction:0,x:2,y:3,alive:true,complete:false,buildStart:1000,buildEnd:7000};
  runtime.beginFrame([structure],1000);let drawn;
  runtime.drawState=(ctx,type,state,options)=>{drawn={type,state,progress:options.progress};return true};
  runtime.draw({},structure,4000,{constructionProgress:.5});assert.deepEqual(drawn,{type:'commandHub',state:'construction',progress:.5});
  assert.equal(structure.buildStart,1000);assert.equal(structure.buildEnd,7000);assert.equal(structure.complete,false);
  assert.equal(runtime.kill(structure,4000,true),true);
  assert.ok(Math.abs(runtime.ghosts[0].end-4000-4000/3.5)<.001);
  runtime.beginFrame([],4600);assert.equal(runtime.ghosts.length,1,'destruction remains visible past the former 571 ms duration');
  runtime.beginFrame([],5200);assert.equal(runtime.ghosts.length,0);
});

test('all troop and developmental animation timing retains the pre-slowdown values',()=>{
  // Only temporal fields are pinned: crop rectangles and direction repairs can change freely.
  const timing=Object.entries(manifest.assets).filter(([,asset])=>asset.kind!=='structure').map(([type,asset])=>[type,Object.entries(asset.states).map(([state,definition])=>[state,Object.fromEntries(['frames','fps','loop','contactMs','contactFrame','progressDriven','durationMs'].filter(field=>definition[field]!==undefined).map(field=>[field,definition[field]]))])]);
  assert.equal(createHash('sha256').update(JSON.stringify(timing)).digest('hex'),'1337319aa508666f308d565113f50000b8607ada7e9632d4617a1e4cfc3bab80');
});
