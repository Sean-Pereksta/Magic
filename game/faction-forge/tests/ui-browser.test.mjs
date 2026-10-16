import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {join,resolve,extname} from 'node:path';
import {createRequire} from 'node:module';

const require=createRequire(import.meta.url);
let chromium;try{({chromium}=require('playwright'));}catch{}
const directory=resolve(fileURLToPath(new URL('..',import.meta.url)));

test('collection, deck chooser, draft info, reward reveal and responsive Forge are usable', {skip:!chromium}, async()=>{
  const server=createServer(async(req,res)=>{try{const path=resolve(directory,'.'+new URL(req.url,'http://localhost').pathname);if(!path.startsWith(directory))throw new Error();const body=await readFile(path);res.setHeader('Content-Type',extname(path)==='.css'?'text/css':'text/javascript');res.end(body);}catch{res.writeHead(404);res.end();}});
  await new Promise(r=>server.listen(0,'127.0.0.1',r));const port=server.address().port;
  let browser;
  try{
    browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH||undefined,args:['--no-sandbox','--disable-dev-shm-usage']});
    const page=await browser.newPage({viewport:{width:1150,height:800}});const errors=[];page.on('pageerror',e=>errors.push(e.message));
    await page.goto(`http://127.0.0.1:${port}/assets.js`);
    await page.setContent(`<link rel="stylesheet" href="http://127.0.0.1:${port}/expansion.css"><body><div id="draft"><div class="card"><button class="cardInfoBtn">?</button><div class="icon troopIcon"></div><div class="title">Riflemen</div></div></div></body>`);
    await page.evaluate(async(base)=>{
      const {createExpansionUI}=await import(base+'/ui.js');
      const {CAMPAIGN_MISSIONS}=await import(base+'/campaign.js');
      window.profile={uid:'real-user',username:'Commander',crowncouncilElo:1178,forgeDust:450,troops:{rifleman:{forgeTier:1,cards:10,unlocked:true},golem:{forgeTier:2,cards:31,unlocked:true},railbreaker:{forgeTier:1,cards:0,blueprints:3,unlocked:false}},campaign:{missions:{'forge-01':{stars:3,completed:true}}}};
      window.active=false;window.upgrades=[];window.picks=0;window.infoClicks=0;window.mission=null;
      const troops=window.testTroops={rifleman:{name:'Riflemen',faction:'Vanir',emoji:'🔫',role:'back'},golem:{name:'Sentinel Golem',faction:'Aesir',emoji:'🗿',role:'front',size:2},railbreaker:{name:'Railbreaker',faction:'Vanir',emoji:'⚡',role:'back'}};
      window.ui=createExpansionUI({assets:{enabled:()=>false},troops,getProfile:()=>window.profile,isActiveMatch:()=>window.active,missionDefinitions:CAMPAIGN_MISSIONS,onCampaignMission:async(id,faction)=>{window.mission={id,faction};return {ok:true};},onUpgrade:async(key,options)=>{window.upgrades.push({key,options});window.profile.troops[key].forgeTier++;window.profile.troops[key].cards-=10;window.profile.forgeDust-=100;return {profile:window.profile};},onSignIn:async(email,password)=>{window.signin={email,password};return {ok:true};}});
      const card=document.querySelector('.card');card.onclick=()=>window.picks++;card.querySelector('.cardInfoBtn').onclick=event=>{event.stopPropagation();window.infoClicks++;};ui.decorateDraftCard(card,{key:'rifleman',tier:2,recommended:'Strong Counter',copies:2});ui.showMenu();
    },`http://127.0.0.1:${port}`);
    await page.getByRole('button',{name:'The Forge',exact:false}).first().click();
    assert.equal(await page.locator('.ff-collection-card').count(),3);
    assert.equal(await page.locator('.ff-locked-card').count(),1);
    const badge=await page.locator('.ff-collection-card').first().locator('.ff-tier-badge').evaluate(e=>({text:e.textContent,position:getComputedStyle(e).position,bottom:getComputedStyle(e).bottom,right:getComputedStyle(e).right}));assert.equal(badge.text,'I');assert.equal(badge.position,'absolute');assert.notEqual(badge.bottom,'auto');assert.notEqual(badge.right,'auto');
    await page.getByRole('button',{name:'Forge Veteran',exact:true}).click();
    await page.getByRole('button',{name:'Confirm upgrade',exact:true}).click();
    await page.waitForFunction(()=>window.upgrades.length===1 && !document.querySelector('.ff-confirm'));
    const upgraded=await page.evaluate(()=>({tier:profile.troops.rifleman.forgeTier,dust:profile.forgeDust,id:upgrades[0].options.requestId}));assert.equal(upgraded.tier,2);assert.equal(upgraded.dust,350);assert.ok(upgraded.id);
    await page.getByRole('button',{name:'Aesir',exact:true}).click();assert.equal(await page.locator('.ff-collection-card').count(),1);
    await page.evaluate(()=>{window.active=true;ui.showForge('golem');});assert.equal(await page.locator('.ff-upgrade-button').isDisabled(),true);
    await page.evaluate(async(base)=>{const {FACTION_TROOPS}=await import(base+'/decks.js');for(const [faction,keys] of Object.entries(FACTION_TROOPS))for(const key of keys){if(!testTroops[key])testTroops[key]={name:key,faction,role:'back'};if(!profile.troops[key])profile.troops[key]={unlocked:true,forgeTier:1,cards:0};}window.active=false;ui.showCampaign();},`http://127.0.0.1:${port}`);assert.equal(await page.locator('.ff-mission').count(),30);assert.equal(await page.locator('.ff-mission:not(:disabled)').count(),2);
    await page.locator('.ff-mission').nth(1).click();await page.getByRole('button',{name:'Select and continue with Arcane Tempest',exact:true}).click();assert.deepEqual(await page.evaluate(()=>({id:mission.id,faction:mission.faction.faction,troops:mission.faction.troops})),{id:'forge-02',faction:'Aesir',troops:['guardian','phaser','storm','runebinder','wardmaker']});
    await page.locator('.cardInfoBtn').click();assert.equal(await page.evaluate(()=>window.picks),0);assert.equal(await page.evaluate(()=>window.infoClicks),1);
    await page.locator('#draft .card').click();assert.equal(await page.evaluate(()=>window.picks),1);assert.equal(await page.locator('#draft button').count(),1,'draft has only the info button, no Forge upgrade control');
    await page.evaluate(()=>{ui.decorateDraftCard(document.querySelector('#draft .card'),{key:'rifleman',type:'upgrade',faction:'Vanir',tier:1,card:{type:'special',special:'upgrade',for:'rifleman'},roster:[{type:'rifleman',count:2,lvl:2}],evolutions:{rifleman:'b'}});});
    assert.equal(await page.locator('#draft .ff-battle-rank').textContent(),'II → III');assert.equal(await page.locator('#draft .ff-tier-badge').count(),0,'battle card does not mislabel battle rank as permanent tier');
    assert.match(await page.locator('#draft .ff-mechanical-improvement').textContent(),/bypass two armor/);assert.match(await page.locator('#draft .ff-draft-tradeoff').textContent(),/Slower sustained fire/);
    const rankPosition=await page.locator('#draft .ff-battle-rank').evaluate(node=>({position:getComputedStyle(node).position,bottom:getComputedStyle(node).bottom,right:getComputedStyle(node).right}));assert.equal(rankPosition.position,'absolute');assert.notEqual(rankPosition.bottom,'auto');assert.notEqual(rankPosition.right,'auto');
    await page.setViewportSize({width:390,height:780});
    await page.evaluate(()=>{const target=document.createElement('div');target.id='combo-fixture';target.className='viewport';document.body.appendChild(target);ui.updateComboTracker({host:target,faction:'Vanir',roster:[{type:'vrocket',count:1,lvl:1},{type:'suppressor',count:1,lvl:1}],availableTypes:['vrocket','suppressor','vguard']});});
    const comboPanel=page.locator('#combo-fixture .ff-combo-tracker');assert.equal(await comboPanel.evaluate(node=>node.open),false,'mobile tracker starts collapsed');await comboPanel.locator('summary').click();assert.equal(await comboPanel.evaluate(node=>node.open),true);
    assert.match(await comboPanel.textContent(),/Suppression Bombardment/);assert.match(await comboPanel.textContent(),/BASIC · ACTIVE/);assert.equal(await comboPanel.locator('.ff-combo-row').count(),1,'only available faction partners are shown');
    assert.equal(await comboPanel.evaluate(node=>getComputedStyle(node).position),'absolute');assert.ok(await comboPanel.evaluate(node=>node.getBoundingClientRect().right<=innerWidth));
    await page.evaluate(()=>{ui.announceBattleUpgrade({type:'special',special:'upgrade',for:'rifleman'},[{type:'rifleman',count:2,lvl:1}],{faction:'Vanir'});document.getElementById('combo-fixture').remove();});
    assert.match(await page.locator('.ff-battle-upgrade-toast').textContent(),/Rank I → II/);assert.equal(await page.locator('.ff-battle-upgrade-toast .ff-battle-rank').textContent(),'II');await page.setViewportSize({width:1150,height:800});
    await page.evaluate(()=>ui.showResults({won:true,opponent:'IronWarden',estimatedMMR:true,mmr:1150,stars:3,receipt:{forgeDust:20,cards:[{troopKey:'rifleman',quantity:3},{troopKey:'railbreaker',quantity:1,blueprint:true}],unlocks:['railbreaker']}}));
    assert.equal(await page.locator('.ff-reward-card').count(),2);assert.match(await page.locator('.ff-reward-card').nth(1).textContent(),/blueprints/);assert.equal(await page.locator('.ff-unlocked-notice').count(),1);
    await page.setViewportSize({width:390,height:780});await page.evaluate(()=>ui.showForge());const width=await page.evaluate(()=>document.querySelector('.ff-panel').scrollWidth);assert.ok(width<=390,`mobile content exceeded viewport: ${width}`);
    const graphics=await page.evaluate(async(base)=>{
      const {createBattleRenderer}=await import(base+'/renderer.js');
      const sheet=document.createElement('canvas');sheet.width=768;sheet.height=768;const ctx=sheet.getContext('2d');for(let f=0;f<36;f++){ctx.fillStyle=f===1?'#00ff00':'#ff0000';ctx.fillRect(f%6*128,Math.floor(f/6)*128,128,128);}
      const spec={frameWidth:128,frameHeight:128,columns:6,rows:6,pivot:[.5,.9375],animations:{idle:{frames:[0,1,2,3,4,5],duration:600},move:{frames:[6,7,8,9,10,11],duration:600}}};
      const layer=document.createElement('div');document.body.appendChild(layer);const host=document.createElement('div');host.innerHTML='<div class="troop-art"><img alt="fallback"></div>';layer.appendChild(host);
      let clock=0;const renderer=createBattleRenderer({layer,now:()=>clock,assets:{enabled:()=>true,unit:async()=>({image:sheet,spec}),asset:async()=>({image:sheet,spec:{frameWidth:128,frameHeight:128,columns:6,rows:6}})}});
      const unit={id:'sprite-crop',type:'rifleman',p:0,x:0,y:0,hp:10,targetHp:10,forgeTier:1};renderer.renderUnit(host,unit,0);await Promise.resolve();await Promise.resolve();renderer.renderUnit(host,unit,120);const canvas=host.querySelector('canvas'),pixel=[...canvas.getContext('2d').getImageData(127,64,1,1).data];
      // step() must not overwrite animations controlled by the existing game loop.
      renderer.renderUnit(host,unit,180,{moving:true});renderer.step(180);const movePixel=[...canvas.getContext('2d').getImageData(64,64,1,1).data];
      renderer.event({id:'fx',type:'status',kind:'temporal-field',x:2,y:3,ttl:650});clock=225;renderer.step(clock);const effect=layer.parentElement.querySelector('.ff-battle-effects-ground');const result={width:canvas.width,height:canvas.height,pixel,movePixel,effectWidth:effect.width,effectCount:renderer.stats.active,surfaces:layer.parentElement.querySelectorAll('.ff-battle-effects').length};clock=800;renderer.step(clock);result.remainingEffects=renderer.stats.active;renderer.destroy();result.remainingSurfaces=layer.parentElement.querySelectorAll('.ff-battle-effects').length;layer.remove();return result;
    },`http://127.0.0.1:${port}`);
    assert.deepEqual(graphics.pixel,[0,255,0,255],'a cell edge contains only the selected frame');assert.equal(graphics.width,128);assert.equal(graphics.height,128);assert.deepEqual(graphics.movePixel,[255,0,0,255],'renderer.step does not reset external movement animation');assert.equal(graphics.effectWidth,540);assert.equal(graphics.effectCount,1);assert.equal(graphics.surfaces,2);assert.equal(graphics.remainingEffects,0);assert.equal(graphics.remainingSurfaces,0);
    // Exercise loaded frame layers: fallback-only checks cannot catch frames
    // obscuring faction captions or turning absolute tier badges into flow text.
    await page.evaluate(async(base)=>{
      ui.destroy();const {createExpansionUI}=await import(base+'/ui.js');
      const image=document.createElement('canvas');image.width=image.height=256;
      const ctx=image.getContext('2d');ctx.strokeStyle='#c9b77e';ctx.lineWidth=20;ctx.strokeRect(10,10,236,236);
      const url=image.toDataURL();
      const {FACTION_TROOPS}=await import(base+'/decks.js');const troops=Object.fromEntries(Object.entries(FACTION_TROOPS).flatMap(([faction,keys])=>keys.map(key=>[key,{name:key,faction,role:'back'}])));
      window.ui=createExpansionUI({assets:{enabled:()=>true,unit:async()=>null,asset:async(group,key)=>key.startsWith('card-frame-')?{image,url,spec:{}}:null},troops});
    },`http://127.0.0.1:${port}`);
    for(const width of [1150,390]){
      await page.setViewportSize({width,height:800});
      for(const view of ['menu','decks']){
        await page.evaluate(view=>view==='menu'?ui.showMenu():ui.chooseDeck(()=>{}),view);
        if(view==='menu')await page.waitForFunction(()=>document.querySelectorAll('.ff-faction-artwork .ff-card-frame').length===3);
        const cards=view==='menu'?await page.locator('.ff-faction-banner').evaluateAll(cards=>cards.map(card=>{
          const art=card.querySelector('.ff-faction-artwork').getBoundingClientRect(),name=card.querySelector('.ff-faction-name').getBoundingClientRect(),description=card.querySelector('.ff-faction-description')?.getBoundingClientRect();
          const overlaps=rect=>rect&&art.left<rect.right&&art.right>rect.left&&art.top<rect.bottom&&art.bottom>rect.top;
          return {square:Math.abs(art.width-art.height)<1,overlap:!!(overlaps(name)||overlaps(description)),badge:getComputedStyle(card.querySelector('.ff-tier-badge')).position};
        })):await page.locator('.ff-deck-card').evaluateAll(cards=>cards.map(card=>{const art=card.querySelector('.ff-deck-portraits').getBoundingClientRect(),name=card.querySelector('h2').getBoundingClientRect();return {square:true,overlap:art.top<name.bottom,badge:getComputedStyle(card.querySelector('.ff-tier-badge')).position};}));
        assert.equal(cards.length,3);for(const card of cards){assert.ok(card.square);assert.equal(card.overlap,false);assert.equal(card.badge,'absolute');}
        assert.ok(await page.evaluate(()=>document.querySelector('.ff-expansion').scrollWidth<=innerWidth));
      }
    }
    assert.deepEqual(errors,[]);
  }finally{await browser?.close();await new Promise(r=>server.close(r));}
});


