/* OVR picking, budget rolls and the real fielded roster. Optional environment:
 * THREE_BUNDLE, BASKETBALL_CHROME, BASKETBALL_SCREENSHOTS. */
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {chromium}=require('playwright');
const {installLegacyGraphicsFixture}=require('./legacy-graphics-fixture.cjs');
const root=path.resolve(__dirname,'../../..');
(async()=>{
  const options={headless:true,args:['--use-gl=angle','--use-angle=swiftshader','--enable-webgl']};
  if(process.env.BASKETBALL_CHROME)options.executablePath=process.env.BASKETBALL_CHROME;
  const browser=await chromium.launch(options);
  try{
    const page=await browser.newPage({viewport:{width:1440,height:1100}}),errors=[];
    page.on('pageerror',error=>errors.push(error.stack));
    await page.route('http://basketball.local/**',route=>{
      let file=path.join(root,new URL(route.request().url()).pathname);
      if(fs.existsSync(file)&&fs.statSync(file).isDirectory())file=path.join(file,'index.html');
      if(!fs.existsSync(file))return route.fulfill({status:404,body:'Not found'});
      if(file.endsWith('bootstrap.js'))return route.fulfill({contentType:'text/javascript',body:fs.readFileSync(file,'utf8').replace('animate();','')});
      const types={'.html':'text/html','.css':'text/css','.js':'text/javascript','.glb':'model/gltf-binary','.webp':'image/webp'};
      return route.fulfill({path:file,contentType:types[path.extname(file)]||'application/octet-stream'});
    });
    if(process.env.THREE_BUNDLE)await page.route('https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js',route=>route.fulfill({path:process.env.THREE_BUNDLE,contentType:'text/javascript'}));
    await page.route('https://pub-b956b47cfcfd467d9f678959c08a5781.r2.dev/*.glb',route=>route.fulfill({
      path:path.join(root,'game/basketball2/assets/models',new URL(route.request().url()).pathname.split('/').pop()),
      contentType:'model/gltf-binary',headers:{'Access-Control-Allow-Origin':'*'}
    }));
    await installLegacyGraphicsFixture(page);
    await page.goto('http://basketball.local/game/basketball2/');
    await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);
    await page.evaluate(()=>{
      riseState=RiseToGlory.create({seed:288,name:'Untouched Franchise',captainName:'Saved Captain'});
      RiseToGlory.player(riseState,riseState.captainId).equipment={
        jersey:{color:'#123456'},shorts:{color:'#345678'},shoes:{color:'#654321'},
        headband:{enabled:true,color:'#40cc88'}
      };
      RiseToGlory.save(riseState);
      showQuickPlaySetup(3,false);
    });
    const campaignBefore=await page.evaluate(()=>({memory:JSON.stringify(riseState),saved:localStorage.getItem(RiseToGlory.SAVE_KEY)}));
    assert.equal(await page.locator('#customRosterEnabled').isChecked(),true,'Quick Play opens the team builder');
    assert.equal(await page.locator('#customRosterEnabled').isVisible(),false,'the compatibility toggle does not duplicate the setup flow');
    assert.equal(await page.locator('#customRosterBuilder').isVisible(),true);
    assert.equal(await page.locator('#p1Cards').isVisible(),false);
    assert.equal(await page.locator('#p2Cards').isVisible(),false);
    assert.equal(await page.locator('#quickPlayUniforms').getAttribute('open'),null,'Uniforms starts collapsed');
    assert.equal(await page.locator('#quickPlayMatchSettings').getAttribute('open'),null,'secondary match settings start collapsed');
    await page.locator('#quickPlayUniforms > summary').click();
    for(const id of ['p1JerseyNext','p2JerseyNext','p1SneakerNext','p2SneakerNext'])assert.equal(await page.locator('#'+id).isVisible(),true,'existing team uniform control remains available');
    await page.locator('#p1JerseyNext').click();
    await page.locator('#p2SneakerNext').click();
    const expectedUniforms=await page.evaluate(()=>[
      {jersey:jerseys[selectedP1Jersey].primary,shorts:jerseys[selectedP1Jersey].secondary,shoes:sneakers[selectedP1Sneaker].primary},
      {jersey:jerseys[selectedP2Jersey].primary,shorts:jerseys[selectedP2Jersey].secondary,shoes:sneakers[selectedP2Sneaker].primary}
    ]);
    await page.locator('#quickPlayUniforms > summary').click();
    for(const [size,cap] of [[3,250],[2,160],[1,80],[3,250]]){
      await page.locator(`[data-team-size="${size}"]`).click();
      assert.equal(await page.locator('#customRosterCap').inputValue(),String(cap),'fresh automatic cap follows team size');
    }
    const catalog=await page.evaluate(()=>customRosterCatalog().filter(entry=>entry.id.startsWith('quick:')).map(entry=>({
      id:entry.id,name:entry.name,overall:entry.overall,role:entry.role,
      calculated:RiseToGlory.overall({ratings:entry.profile.ratings,archetype:entry.profile.campaignArchetype})
    })));
    assert.equal(catalog.length,108);
    assert.equal(new Set(catalog.map(entry=>entry.name)).size,108,'the catalog contains distinct athlete identities across tiers');
    assert.equal(new Set(catalog.map(entry=>entry.role)).size,9);
    assert.ok(catalog.every(entry=>entry.overall===entry.calculated),'displayed OVR derives from real player ratings');

    await page.locator('[data-roster-browse][data-team="0"][data-slot="0"]').click();
    await page.locator('#customRosterRoleFilter').selectOption('sharpshooter');
    for(const tier of [60,70,80,90]){
      await page.locator('#customRosterOverallFilter').selectOption(String(tier));
      const cards=await page.locator('#customRosterPicker [data-roster-player]').evaluateAll(nodes=>nodes.map(node=>({
        role:node.dataset.role,overall:Number(node.dataset.overall)
      })));
      assert.ok(cards.length>0&&cards.every(card=>card.role==='sharpshooter'&&card.overall===tier),'picker filters exact real OVR and role');
    }
    await page.locator('#customRosterOverallFilter').selectOption('80');
    if(process.env.BASKETBALL_SCREENSHOTS){
      fs.mkdirSync(process.env.BASKETBALL_SCREENSHOTS,{recursive:true});
      await page.locator('#customRosterPicker').scrollIntoViewIfNeeded();
      await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'Basketball2-QuickPlay-OVR-Picker.png')});
    }
    const chosen=page.locator('#customRosterPicker [data-roster-player]:not(:disabled)').first();
    const chosenId=await chosen.getAttribute('data-roster-player');
    await chosen.click();
    assert.equal(await page.locator('#customRosterTeam0Slot0').inputValue(),chosenId,'browsing selects that specific athlete');
    console.log('PASS: 108 real-rated players, nine roles and exact 60/70/80/90 OVR picker choices.');

    await page.locator('#customRosterCap').fill('237');
    for(const size of [1,2,3]){
      await page.locator(`[data-team-size="${size}"]`).click();
      assert.equal(await page.locator('#customRosterCap').inputValue(),'237','team-size changes retain a user-entered cap');
    }
    await page.locator('[data-team-size="1"]').click();
    const returningFromCampaign=await page.evaluate(()=>{
      showPrimaryMenu();
      // Campaign modes can temporarily change this global without changing
      // the stored Quick Play format. Returning must compare against the save.
      teamSize=3;showQuickPlaySetup(3,false);
      return {cap:customRosterDraft.cap,size:teamSize,
        result:BasketballCustomRosters.validate(customRosterDraft,teamSize,customRosterCatalog()),
        counts:customRosterDraft.teams.map(ids=>ids.slice(0,teamSize).filter(Boolean).length)};
    });
    assert.equal(returningFromCampaign.cap,237);
    assert.equal(returningFromCampaign.size,3);
    assert.equal(returningFromCampaign.result.ok,true,'returning from another mode fills the new Quick Play format');
    assert.deepEqual(returningFromCampaign.counts,[3,3]);
    for(const [size,cap] of [[3,250],[2,160],[1,80]]){
      await page.locator(`[data-team-size="${size}"]`).click();
      await page.locator('#customRosterCap').fill(String(cap));
      const outcomes=new Set();
      for(let repeat=0;repeat<6;repeat++){
        await page.locator('#customRosterFill').click();
        const rolled=await page.evaluate(()=>({
          result:BasketballCustomRosters.validate(customRosterDraft,teamSize,customRosterCatalog()),
          teams:customRosterDraft.teams.map(ids=>ids.slice(0,teamSize))
        }));
        assert.equal(rolled.result.ok,true);
        assert.deepEqual(rolled.result.totals,[cap,cap],`${size}v${size} fills both available budgets exactly`);
        assert.equal(new Set(rolled.teams.flat()).size,size*2,'one athlete cannot occupy two matchup slots');
        outcomes.add(JSON.stringify(rolled.teams.map(team=>team.slice().sort())));
        assert.equal(await page.locator('#startBtn').isEnabled(),true);
      }
      assert.ok(outcomes.size>=3,'repeated rolls produce varied complete lineups');
    }
    console.log('PASS: randomized unique lineups hit 250/160/80 OVR exactly for 3v3/2v2/1v1.');

    await page.locator('[data-team-size="3"]').click();
    await page.locator('#customRosterCap').fill('297');
    await page.evaluate(()=>{customRosterDraft.teams=[[],[]];saveCustomRosterDraft();refreshCustomRosterSetup();});
    const ninety=catalog.filter(entry=>entry.overall===90).slice(0,6);
    for(let index=0;index<6;index++){
      await page.locator(`[data-roster-browse][data-team="${Math.floor(index/3)}"][data-slot="${index%3}"]`).click();
      await page.locator('#customRosterRoleFilter').selectOption(ninety[index].role);
      await page.locator('#customRosterOverallFilter').selectOption('90');
      await page.locator('#customRosterPlayerSearch').fill(ninety[index].name);
      await page.locator(`[data-roster-player="${ninety[index].id}"]`).click();
    }
    await page.locator('#customRosterCap').fill('250');
    assert.equal(await page.locator('#startBtn').isDisabled(),true,'six 90 OVR picks exceed a 250 OVR cap');
    assert.match(await page.locator('#customRosterStatus').textContent(),/over/i);
    assert.equal(await page.evaluate(()=>{const roster=matchPlayers;return startGame()===false&&roster===matchPlayers&&!gameStarted;}),true,'direct starts cannot bypass the budget or dispose an existing roster');
    await page.locator('#customRosterCap').fill('270');
    assert.equal(await page.locator('#startBtn').isEnabled(),true,'the cap is inclusive');
    await page.locator('#customRosterCap').fill('250.5');
    assert.equal(await page.locator('#startBtn').isDisabled(),true,'fractional caps are rejected');
    await page.locator('#customRosterCap').fill('25');
    await page.locator('#customRosterFill').click();
    assert.equal(await page.locator('#startBtn').isDisabled(),true,'an impossible low cap is not silently exceeded');
    await page.locator('#customRosterCap').fill('237');
    await page.locator('#customRosterFill').click();
    const arbitrary=await page.evaluate(()=>BasketballCustomRosters.validate(customRosterDraft,teamSize,customRosterCatalog()));
    assert.ok(arbitrary.ok&&arbitrary.totals.every(total=>total<=237),'arbitrary legal caps remain supported');
    console.log('PASS: inclusive totals, arbitrary caps, impossible budgets and direct-start enforcement.');

    for(const [size,local,mode] of [[1,false,'competitive'],[2,false,'competitive'],[3,false,'competitive'],[2,true,'cooperative'],[3,true,'cooperative']]){
      await page.evaluate(({size,local,mode})=>{showQuickPlaySetup(size,local);localTeamMode=mode;refreshMatchSetup();}, {size,local,mode});
      await page.locator('#customRosterCap').fill(String(size===3?250:size*80));
      await page.locator('#customRosterFill').click();
      const expected=await page.evaluate(()=>BasketballCustomRosters.snapshot(customRosterDraft,teamSize,customRosterCatalog()));
      await page.locator('#startBtn').click();
      const actual=await page.evaluate(()=>({
        players:matchPlayers.map(player=>({team:player.teamId,controller:player.controllerSlot,profile:player.profile,model:!!player.playerModel,
          uniform:Object.fromEntries(['jersey','shorts','shoes'].map(region=>[region,player.appearance.regions[region].color]))})),
        started:gameStarted,riseMatch
      }));
      assert.equal(actual.started,true);assert.equal(actual.riseMatch,null);
      assert.equal(actual.players.length,2*size);assert.ok(actual.players.every(player=>player.model));
      for(const player of actual.players){
        const selected=expected.teams[player.team].find(profile=>profile.customRosterId===player.profile.customRosterId);
        assert.ok(selected,'the selected athlete appears on the intended team');
        for(const key of ['customOverall','ratings','abilities','role','skin','hair','speed','sprint','shot','layup','block']){
          assert.deepEqual(player.profile[key],selected[key],`fielded ${key} comes from selected real athlete`);
        }
        assert.deepEqual(player.uniform,expectedUniforms[player.team],'all teammates wear the chosen jersey, shorts and shoes');
      }
      const humans=actual.players.filter(player=>player.controller);
      assert.equal(humans.length,local?2:1);
      if(local){
        const second=humans.find(player=>player.controller==='p2');
        assert.equal(second.team,0);
        assert.equal(second.profile.customRosterId,expected.teams[0][1].customRosterId,'co-op P2 controls Blue slot 2');
      }
      await page.evaluate(()=>{countdownTimer=0;possessionFrozen=false;for(let frame=0;frame<60;frame++)simulateBasketball(1/60);showPrimaryMenu();});
    }
    const importedOutfit=await page.evaluate(()=>{
      showQuickPlaySetup(3,true);localTeamMode='cooperative';
      const choices=customRosterCatalog().filter(entry=>entry.id.startsWith('quick:')&&entry.overall===80).slice(0,5);
      customRosterDraft.cap=250;customRosterDraft.teams=[
        [`rise:${riseState.captainId}`,choices[0].id,choices[1].id],
        [choices[2].id,choices[3].id,choices[4].id]
      ];
      saveCustomRosterDraft();refreshMatchSetup();startGame();
      const captain=matchPlayers.find(player=>player.profile.customRosterId===`rise:${riseState.captainId}`);
      return {
        outfit:Object.fromEntries(['jersey','shorts','shoes'].map(region=>[region,captain.appearance.regions[region].color])),
        headband:captain.appearance.regions.headband,
        blue:matchPlayers.filter(player=>player.teamId===0).map(player=>Object.fromEntries(['jersey','shorts','shoes'].map(region=>[region,player.appearance.regions[region].color])))
      };
    });
    assert.deepEqual(importedOutfit.outfit,expectedUniforms[0],'selected team kit overrides an imported club outfit');
    assert.ok(importedOutfit.headband.enabled&&importedOutfit.headband.color===0x40cc88,'imported individual accessories remain available');
    assert.ok(importedOutfit.blue.every(outfit=>JSON.stringify(outfit)===JSON.stringify(expectedUniforms[0])),'co-op P2 and Blue AI match the same team kit');
    await page.evaluate(()=>showPrimaryMenu());
    assert.deepEqual(await page.evaluate(()=>({memory:JSON.stringify(riseState),saved:localStorage.getItem(RiseToGlory.SAVE_KEY)})),campaignBefore,'Quick Play leaves campaign state and save unchanged');
    console.log('PASS: real selected ratings, matching team kits, imported accessories and co-op slots without changing the franchise.');

    await page.evaluate(()=>showQuickPlaySetup(3,false));
    await page.locator('#customRosterCap').fill('250');
    await page.locator('#customRosterFill').click();
    const stored=await page.evaluate(()=>JSON.parse(localStorage.getItem(BasketballCustomRosters.SAVE_KEY)));
    await page.reload();await page.waitForFunction(()=>basketballPlayerModels.assets.size===3);
    await page.evaluate(()=>showQuickPlaySetup(3,false));
    assert.deepEqual(await page.evaluate(()=>JSON.parse(localStorage.getItem(BasketballCustomRosters.SAVE_KEY))),stored,'reload preserves chosen IDs and cap');
    assert.equal(await page.locator('#customRosterEnabled').isChecked(),true);
    for(let team=0;team<2;team++)for(let slot=0;slot<3;slot++)assert.equal(await page.locator(`#customRosterTeam${team}Slot${slot}`).inputValue(),stored.teams[team][slot]);
    if(process.env.BASKETBALL_SCREENSHOTS){
      fs.mkdirSync(process.env.BASKETBALL_SCREENSHOTS,{recursive:true});
      await page.setViewportSize({width:1440,height:1500});
      await page.evaluate(()=>{document.getElementById('menuBox').scrollTop=0;document.activeElement?.blur();});
      await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'Basketball2-QuickPlay-OVR.png')});
    }
    for(const [width,height] of [[390,844],[320,640]]){
      await page.setViewportSize({width,height});
      const dimensions=await page.evaluate(()=>({viewport:innerWidth,document:document.documentElement.scrollWidth,menu:document.getElementById('menuBox').clientWidth,content:document.getElementById('menuBox').scrollWidth}));
      assert.ok(dimensions.document<=width+1&&dimensions.content<=dimensions.menu+1,'no mobile horizontal overflow '+JSON.stringify(dimensions));
      for(const selector of ['[data-team-size="1"]','#onePlayerBtn','#twoPlayerBtn','#customRosterCap','#customRosterFill','#startBtn','[data-roster-browse][data-team="0"][data-slot="0"]','[data-roster-browse][data-team="1"][data-slot="2"]','#quickPlayUniforms > summary','#quickPlayMatchSettings > summary']){
        await page.locator(selector).scrollIntoViewIfNeeded();
        const bounds=await page.locator(selector).boundingBox();
        assert.ok(bounds&&bounds.x>=0&&bounds.x+bounds.width<=width+1&&bounds.height>=44,'usable mobile control '+selector+' '+JSON.stringify(bounds));
      }
      await page.locator('[data-roster-browse][data-team="0"][data-slot="0"]').click();
      await page.locator('#customRosterRoleFilter').selectOption('all');
      await page.locator('#customRosterOverallFilter').selectOption('all');
      await page.locator('#customRosterPlayerSearch').fill('');
      for(const selector of ['#customRosterRoleFilter','#customRosterOverallFilter','#customRosterPlayerSearch']){
        await page.locator(selector).scrollIntoViewIfNeeded();const bounds=await page.locator(selector).boundingBox();
        assert.ok(bounds&&bounds.x>=0&&bounds.x+bounds.width<=width+1&&bounds.height>=44,'usable mobile picker '+selector);
      }
      const overflow=await page.evaluate(()=>({viewport:innerWidth,document:document.documentElement.scrollWidth,menu:document.getElementById('menuBox').clientWidth,content:document.getElementById('menuBox').scrollWidth}));
      assert.ok(overflow.document<=width+1&&overflow.content<=overflow.menu+1,'open picker stays within mobile viewport');
      const card=page.locator('#customRosterPicker [data-roster-player]:not(:disabled)').first();
      await card.click();
      if(width===390&&process.env.BASKETBALL_SCREENSHOTS){
        await page.locator('[data-roster-browse][data-team="0"][data-slot="0"]').scrollIntoViewIfNeeded();
        await page.screenshot({path:path.join(process.env.BASKETBALL_SCREENSHOTS,'Basketball2-QuickPlay-OVR-Mobile.png')});
      }
    }
    assert.deepEqual(errors,[]);
    console.log('PASS: saved picks, 320/390px touch controls and one clear team builder.');
  }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
