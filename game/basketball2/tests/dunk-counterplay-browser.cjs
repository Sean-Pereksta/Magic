/* Real rigs and normal simulation: compare earned dunks with planted rim
 * defense and timed hand blocks, including an elite finisher's abilities. */
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {chromium}=require('playwright');
const root=path.resolve(__dirname,'../../..');
(async()=>{
  const options={headless:true,args:['--use-gl=angle','--use-angle=swiftshader','--enable-webgl']};
  if(process.env.BASKETBALL_CHROME)options.executablePath=process.env.BASKETBALL_CHROME;
  const browser=await chromium.launch(options);
  try{
    const page=await browser.newPage({viewport:{width:1280,height:800}}),errors=[];
    page.on('pageerror',error=>errors.push(error.stack));
    await page.route('http://basketball.local/**',route=>{
      let file=path.join(root,new URL(route.request().url()).pathname);
      if(fs.existsSync(file)&&fs.statSync(file).isDirectory())file=path.join(file,'index.html');
      if(!fs.existsSync(file))return route.fulfill({status:404,body:'Not found'});
      if(file.endsWith('bootstrap.js'))return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
      const types={'.html':'text/html','.css':'text/css','.js':'text/javascript','.glb':'model/gltf-binary','.webp':'image/webp','.json':'application/json'};
      return route.fulfill({path:file,contentType:types[path.extname(file)]||'application/octet-stream'});
    });
    if(process.env.THREE_BUNDLE)await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
    await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}}));
    await page.goto('http://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);
    const results=await page.evaluate(()=>{
      updateTeamAI=()=>{};const results=[];
      for(const elite of [false,true])for(const condition of ['open','wall','hands','timed','late','trailing','badGather'])for(const stamina of [1,.35]){
        teamSize=1;twoPlayerMode=false;selectedP1='smooth';selectedP2='tower';startGame();countdownTimer=0;possessionFrozen=false;
        p1.profile.ratings=elite?{dunking:92,jumping:94,strength:90,standingDunk:90,contactFinishing:92,aerialControl:92}:
          {dunking:70,jumping:75,strength:65,standingDunk:65,contactFinishing:65,aerialControl:70};
        p1.profile.abilities=elite?{posterizer:3,powerFinisher:3,contactSpecialist:3,highFlyer:3}:{};
        p1.stamina=stamina;p1.group.position.set(0,0,HOOP_POS.z+1.3);p1.velocity.set(0,0,-4.2);p1.angle=Math.PI;p1.group.rotation.set(0,Math.PI,0);
        p2.profile.ratings={interiorDefense:92,blocking:92,strength:90};
        p2.group.position.set(condition==='open'?8:0,0,condition==='open'?8:HOOP_POS.z+(condition==='trailing'?2.4:.68));
        p2.velocity.set(0,0,0);p2.jumpCooldown=0;p2.angle=condition==='trailing'?Math.PI:0;p2.group.rotation.set(0,p2.angle,0);
        takePossession(p1,{silent:true});p1.moveMeta.chargeHeld=condition==='badGather'?1.3:0;updateCharacters(0);startDunk(p1);
        const launched=dunkActive,style=p1.moveMeta.dunk?.style,profile=p1.moveMeta.dunk?.finishProfile;
        let jumped=false,maxContest=0,contactDistance=null,finite=true;
        const apply=applyBlockDeflection;
        applyBlockDeflection=(defender,shooter,contact)=>{contactDistance=contact?.distance;return apply(defender,shooter,contact);};
        try{
          for(let frame=0;frame<100&&(dunkActive||layupActive);frame++){
            if(condition==='hands')raiseHands(p2);
            if(!jumped&&(condition==='timed'&&dunkTimer>=.12||condition==='late'&&dunkTimer>=.46)){
              tryJumpBlock(p2,false);jumped=true;
            }
            if(dunkActive)maxContest=Math.max(maxContest,calculateShotContest(p1).value);
            simulateBasketball(1/60);
            finite=finite&&p1.group.position.toArray().every(Number.isFinite)&&ball.position.toArray().every(Number.isFinite);
          }
        }finally{applyBlockDeflection=apply;}
        results.push({elite,condition,stamina,launched,style,finish:profile?.finish,resistance:profile?.resistance,
          maxContest,made:scoreP1===2,blocks:p2.stats.blocks,attempts:p1.stats.fga,contactDistance,state:ballState,finite});
      }
      return results;
    });
    results.forEach(result=>console.log('DUNK '+JSON.stringify(result)));
    assert.deepEqual(errors,[]);
    assert.ok(results.every(result=>result.attempts===1&&result.finite),'each gather counts one finite real attempt');
    const find=(elite,condition,stamina=1)=>results.find(result=>result.elite===elite&&result.condition===condition&&result.stamina===stamina);
    for(const elite of [false,true]){
      for(const condition of ['open','late','trailing'])assert.equal(find(elite,condition).made,true,'an earned '+condition+' finish survives');
      const timed=find(elite,'timed');assert.equal(timed.made,false);assert.equal(timed.blocks,1);
      assert.ok(timed.contactDistance>=0&&timed.contactDistance<=.335,'a timed visible palm touches the actual ball');
    }
    for(const condition of ['wall','hands']){
      const average=find(false,condition);assert.equal(average.made,false);assert.equal(average.blocks,0,'body pressure cannot invent a block statistic');
    }
    assert.equal(find(true,'wall').made,true,'an elite fresh finisher can still earn a body-contact poster');
    for(const stamina of [1,.35]){
      const elite=find(true,'hands',stamina);assert.equal(elite.made,false);assert.equal(elite.blocks,1);
      assert.ok(elite.contactDistance<=.335,'stacked finishing abilities never ignore a real palm');
    }
    assert.equal(find(false,'open',.35).launched,false,'an ordinary exhausted gather falls back to the layup');
    assert.equal(find(true,'open',.35).made,true,'an elite retained open gather still works');
    assert.equal(find(true,'badGather').made,true,'strong fresh finishing retains some forgiveness');
    assert.equal(find(true,'badGather',.35).made,false,'exhaustion plus a mistimed gather cannot bully a planted wall');
    console.log('PASS: 28 real-rig average/elite finishing scenarios preserve earned dunks and visible block counterplay');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
