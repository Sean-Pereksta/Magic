import test from 'node:test';
import assert from 'node:assert/strict';
import {runtime} from './runtime-test-helper.mjs';

function start(touch=false){const h=runtime(touch);h.run('startNewGame();game.player.invuln=1000');h.step(2);return h;}
const buttons=(h,id)=>[...h.w.document.querySelectorAll(`#${id} button`)];
const buildButton=h=>buttons(h,'awHomePanel').find(b=>b.textContent==='BUILD COTTAGE');
function foundation(h,kind='house'){
  h.run(`AWCampaign.enter(AWHome.HOME);var target=game.interactables.find(o=>o.homeKind==='${kind}');game.player.x=target.x;game.player.y=target.y;interact();`);
}

test('every campaign village has exactly one supplier; badges and actual service panels agree',()=>{
  const h=start();try{
    h.run(`game.campaign.unlocked=['verdant','meridian','gloam'];game.gold=10000;`);
    const towns=h.run('Object.values(AWCampaignData.nodes).filter(n=>n.town).map(n=>n.id)');
    for(const id of towns){
      h.run(`AWCampaign.enter(${JSON.stringify(id)});`);
      assert.equal(h.run(`game.interactables.filter(n=>n.npc&&AWServices.capabilities(AWCampaignData.towns[n.campaignTown],n.role).supplies).length`),1,id);
      const count=h.run('game.interactables.length');
      for(let i=0;i<count;i++){
        if(!h.run(`!!game.interactables[${i}].npc`))continue;
        h.run(`var npc=game.interactables[${i}];var cap=AWServices.capabilities(AWCampaignData.towns[npc.campaignTown],npc.role);AWCampaignUI.openTown(npc);`);
        const text=h.w.document.getElementById('npcBody').textContent;
        assert.equal(text.includes('Home Supplies & Seeds'),h.run('cap.supplies'),id);
        assert.equal(text.includes('Material forge'),h.run('cap.forge'),id);
        for(const item of h.run('cap.stock.map(id=>AWCampaignData.items[id].name)'))assert.ok(text.includes(item),item);
        if(h.run("npc.role==='Villager'")){
          assert.equal(h.run('AWServices.forNPC(npc)[0].id'),'talk');
          assert.equal(h.w.document.querySelectorAll('#npcBody button').length,0);
        }
        h.run("closeOverlay('npcPanel')");
      }
    }
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('service resolution uses available inventory and supports declared actions for a new role',()=>{
  const h=start();try{
    h.run(`var testTown={roles:['Merchant','Warden'],stock:[],spells:[],mounts:[],services:{Warden:{forge:true}}};`);
    assert.equal(h.run("AWServices.capabilities(testTown,'Merchant').badges.some(b=>b.id==='shop')"),false);
    assert.equal(h.run("AWServices.capabilities(testTown,'Warden').forge"),true);
    assert.equal(h.run("AWServices.capabilities(testTown,'Warden').badges[0].id"),'forge');
  }finally{h.close();}
});

test('supplier stays available after a purchase and resource totals refresh immediately',()=>{
  const h=start();try{
    h.run(`game.gold=1000;AWCampaignUI.openTown(game.interactables.find(n=>n.role==='Merchant'));`);
    const buy=buttons(h,'npcBody').find(b=>b.textContent.includes('gold')&&!b.disabled);buy.click();
    const supplies=buttons(h,'npcBody').find(b=>b.textContent.includes('Home Supplies & Seeds'));assert.ok(supplies);supplies.click();
    assert.match(h.w.document.getElementById('awHomePanel').textContent,new RegExp(h.run('game.gold')+' gold'));
  }finally{h.close();}
});

for(const kind of ['house','board'])test(`${kind} opens the same cottage screen with exact shortages and prevents unaffordable builds`,()=>{
  const h=start();try{
    h.run('AWHome.state().deed=true;game.gold=75;game.materials.timber=23;game.materials.stone=2;');foundation(h,kind);
    const text=h.w.document.getElementById('awHomePanel').textContent;
    assert.match(text,/Build Your Cottage/);assert.match(text,/Stone\s+2 \/ 10 ✕ — Need 8 more Stone/);
    assert.match(text,/Timber\s+23 \/ 20 ✓/);assert.match(text,/Gold\s+75 \/ 60 ✓/);
    assert.equal(buildButton(h).disabled,true);buildButton(h).click();assert.equal(h.run('AWHome.state().tier'),0);
    assert.equal(h.run('game.gold'),75);assert.equal(h.run('AWHome.guiding()'),false);
    h.run('game.materials.stone=10;AWHomeUI.refresh()');assert.equal(buildButton(h).disabled,false);
  }finally{h.close();}
});

test('building consumes only existing costs, updates the prompt, and preserves save identity',async()=>{
  const h=start();try{
    h.run('AWHome.state().deed=true;game.gold=60;game.materials.timber=20;game.materials.stone=10;');foundation(h);
    const id=h.run('AWHome.state().journeyId');assert.equal(await h.run("AWHome.action('buildHouse')"),true);
    assert.equal(h.run('game.gold'),0);assert.equal(h.run('game.materials.timber'),0);assert.equal(h.run('game.materials.stone'),0);
    h.run('AWHomeUI.close();saveGame();loadGame();beginWorld()');assert.equal(h.run('AWHome.state().journeyId'),id);
    assert.equal(h.run("AWUsability.actionName(game.interactables.find(o=>o.homeKind==='house'))"),'Enter Cottage');
    assert.equal(h.run('AWHome.state().tier'),1);
  }finally{h.close();}
});

test('first arrival guidance expires, acknowledgement persists, and later visits stay quiet',()=>{
  const h=start();try{
    h.run('AWCampaign.enter(AWHome.HOME)');assert.equal(h.run('AWHome.guiding()'),true);
    h.run('elapsed+=13');assert.equal(h.run('AWHome.guiding()'),false);
    h.run("AWCampaign.enter('verdant-city');AWCampaign.enter(AWHome.HOME)");assert.equal(h.run('AWHome.guiding()'),false);
    h.run("AWHomeUI.open('build');AWHomeUI.close();saveGame();loadGame();beginWorld()");assert.equal(h.run('AWHome.state().foundationUnderstood'),true);
  }finally{h.close();}
});

test('map protects unknown services and reveals the home and quest objective after the deed',()=>{
  const h=start();try{
    h.run(`game.campaign.scouted=[];AWCampaignUI.open('Map');`);
    let home=h.w.document.querySelector('[data-node="verdant-hearthglade"]');assert.ok(!home||!home.textContent.includes('Hearthglade'));
    const unknown=h.w.document.querySelector('.aw-map-node.unknown');assert.ok(unknown);unknown.click();assert.doesNotMatch(h.w.document.getElementById('awNodeCard').textContent,/Services:/);
    h.run(`AWCampaignUI.close();AWHome.state().deed=true;AWCampaignUI.open('Map')`);
    home=h.w.document.querySelector('[data-node="verdant-hearthglade"]');assert.match(home.textContent,/⌂.*Hearthglade.*❗/);home.click();assert.match(h.w.document.getElementById('awNodeCard').textContent,/western road from Sunmere/);
    h.w.document.querySelector('[data-node="verdant-city"]').click();assert.match(h.w.document.getElementById('awNodeCard').textContent,/Services:.*Home Supplies & Seeds/);
    h.run(`AWCampaignUI.open('Character')`);assert.match(h.w.document.getElementById('inventoryContent').textContent,/Build Your Cottage/);
  }finally{h.close();}
});

test('quest badges distinguish offered, in-progress and turn-in work',()=>{
  const h=start();try{
    h.run("var keeper=game.interactables.find(n=>n.role==='Quest Keeper');var q=questForVillage(game.roomData)");
    assert.equal(h.run('AWServices.forNPC(keeper)[0].id'),'quest');
    h.run('acceptQuest(q)');assert.equal(h.run('AWServices.forNPC(keeper)[0].id'),'talk');
    h.run('game.quests.active[0].ready=true');assert.equal(h.run('AWServices.forNPC(keeper)[0].id'),'turnin');
  }finally{h.close();}
});

test('touch and remapped keyboard prompts describe the actual nearest target; markers reuse DOM',()=>{
  const h=start(true);try{
    h.run(`AWPresentation.settings.keys.interact='KeyF';var merchant=game.interactables.find(n=>n.role==='Merchant');game.player.x=merchant.x;game.player.y=merchant.y;updateHUD(true);`);
    const touch=h.w.document.getElementById('mobileInteract');assert.equal(touch.textContent,'Shop Supplies');
    assert.match(touch.getAttribute('aria-label'),/Shop Supplies/);
    const count=h.w.document.querySelectorAll('*').length;h.step(30);assert.equal(h.w.document.querySelectorAll('*').length,count);
    assert.equal(h.run('currentInteraction()===merchant'),true);
    assert.match(h.w.document.getElementById('awPrompt').textContent,/Shop Supplies/);
    h.run("AWInput.useDevice('keyboard')");h.step(6);
    assert.match(h.w.document.getElementById('awPrompt').textContent,/^F — Shop Supplies/);
    h.run("AWInput.useDevice('gamepad')");h.step(6);
    assert.ok(h.w.document.getElementById('awPrompt').textContent.startsWith(h.run("AWInput.prompt('interact')")+' — '));
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('crowded moving NPCs keep distinct canvas badges and emphasize only the nearest target',()=>{
  const h=start(true);try{
    h.run(`game.player.x=9;game.player.y=7;for(const n of game.interactables.filter(n=>n.npc)){n.x=9;n.y=7;}
      var badgeRects=[];var oldRoundRect=ctx.roundRect;ctx.roundRect=function(x,y,w,h,r){if(this.fillStyle==='#102327'||this.fillStyle==='#294e49')badgeRects.push({x,y,w,h,selected:this.fillStyle==='#294e49'});return oldRoundRect.call(this,x,y,w,h,r);};render();`);
    const rects=h.run('badgeRects');assert.ok(rects.length>=7);assert.equal(rects.filter(r=>r.selected).length,1);
    for(let i=0;i<rects.length;i++){
      const a=rects[i];assert.ok(a.y>=0&&a.y+a.h<=844);
      for(const b of rects.slice(i+1))assert.equal(a.x<b.x+b.w&&a.x+a.w>b.x&&a.y<b.y+b.h&&a.y+a.h>b.y,false);
    }
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('expansion keeps its continent gate and a completed purchase releases the build button',async()=>{
  const h=start();try{
    h.run(`AWHome.state().deed=true;game.gold=10000;for(const k of Object.keys(MATERIALS))game.materials[k]=1000;`);foundation(h);
    assert.equal(await h.run("AWHome.action('buildHouse')"),true);
    let expand=buttons(h,'awHomePanel').find(b=>b.textContent==='EXPAND HOUSE');assert.equal(expand.disabled,false);
    assert.equal(await h.run("AWHome.action('buildHouse')"),true);
    expand=buttons(h,'awHomePanel').find(b=>b.textContent==='EXPAND HOUSE');assert.equal(expand.disabled,true);
    assert.match(h.w.document.getElementById('awBuildReason').textContent,/Explore .* before this expansion/);
    h.run("AWCampaignUI.open('Map')");assert.equal(h.w.document.querySelectorAll('.overlay:not(.hidden)').length,1);
  }finally{h.close();}
});

test('Escape and controller Back close home and NPC menus without stacking pause',()=>{
  const h=start();try{
    foundation(h);h.run('AWModernUI.menuGamepad([false,true],[])');assert.ok(h.w.document.getElementById('awHomePanel').classList.contains('hidden'));
    h.run("AWHomeUI.open('build')");h.w.document.dispatchEvent(new h.w.KeyboardEvent('keydown',{key:'Escape',code:'Escape',bubbles:true,cancelable:true}));
    assert.equal(h.run('modalPause'),false);
    h.run("AWCampaign.enter('verdant-city');AWCampaignUI.openTown(game.interactables.find(n=>n.npc))");
    h.w.document.dispatchEvent(new h.w.KeyboardEvent('keydown',{key:'Escape',code:'Escape',bubbles:true,cancelable:true}));
    assert.equal(h.run('modalPause'),false);assert.ok(h.w.document.getElementById('pauseOverlay').classList.contains('hidden'));
  }finally{h.close();}
});
