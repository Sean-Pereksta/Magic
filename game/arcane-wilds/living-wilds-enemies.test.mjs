import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {runtime} from './runtime-test-helper.mjs';

const source=readFileSync(new URL('./living-wilds-enemies.js',import.meta.url),'utf8');
function start(){const h=runtime();if(!h.w.AWLivingWildsEnemies)h.run(source);h.run('startNewGame()');return h;}
const value=(h,code)=>JSON.parse(h.run(`JSON.stringify(${code})`));
function visit(h,node,level=25){
  h.run(`game.level=${level};game.campaign.current=${JSON.stringify(node)};if(!game.campaign.visited.includes(game.campaign.current))game.campaign.visited.push(game.campaign.current);game.campaign.room=0;game.room=AWCampaign.coordinates(game.campaign.current);loadRoom();`);
}

test('twelve artwork identities register once with valid regional biomes and implemented combat profiles',()=>{
  const h=start();try{
    const ids=value(h,'AWLivingWildsEnemies.ids');
    assert.equal(ids.length,12);assert.equal(new Set(ids).size,12);
    assert.deepEqual(value(h,'Object.values(AWLivingWildsEnemies.regions).map(a=>a.length)'),[4,4,4]);
    const supported=['regional_beast','regional_spore','regional_moss','regional_wasp','bladeDancer','stormknight','executioner','shield','regional_stalker','dangerAssassin','regional_shepherd'];
    for(const id of ids){
      const row=value(h,`ENEMY_TYPES[${JSON.stringify(id)}]`);
      assert.ok(supported.includes(row.ai),id+' dispatches existing AI');
      assert.ok(Number.isInteger(row.min)&&row.min>=1&&row.min<=30,id+' level gate');
      assert.ok(row.hp>0&&row.speed>0&&row.damage>0&&row.xp>0,id+' finite combat stats');
      assert.equal(h.run(`ENEMY_TYPES[${JSON.stringify(id)}].biomes.every(b=>AWCampaignData.continent(ENEMY_TYPES[${JSON.stringify(id)}].region).biomes.includes(b))`),true,id+' biome exists on its continent');
      h.run(`game.enemies=[];var subject=spawnEnemy(${JSON.stringify(id)},{x:4,y:7});`);
      const profile=value(h,'AWEnemyCombat.profile(subject)');
      assert.ok(profile.length>0,id+' has a signature');
      assert.equal(h.run('AWEnemyCombat.profile(subject).every(a=>!!AWEnemyCombat.definitions[a])'),true,id+' ability implemented');
      h.run('updateEnemyAI(subject,enemyDir(subject),5,subject.speed,.016);drawEnemy(subject);');
    }
    h.run(source);assert.equal(h.run('AWLivingWildsEnemies.ids.length'),12);
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('campaign initial squads and later waves include regional art creatures without crossing continents',()=>{
  const h=start();try{
    const seen=new Set();
    const nodes=value(h,"Object.values(AWCampaignData.nodes).filter(n=>!n.shadow&&['wildland','danger'].includes(n.type)).map(n=>n.id)");
    for(const node of nodes){
      visit(h,node);
      const spawned=value(h,'game.enemies.filter(e=>e.type.startsWith("lw_")).map(e=>({type:e.type,region:ENEMY_TYPES[e.type].region}))');
      const continent=h.run('AWCampaign.current().continent');
      for(const e of spawned){seen.add(e.type);assert.equal(e.region,continent,node+' region');}
      assert.equal(h.run('AWLivingWildsEnemies.eligible(AWCampaign.current(),game.roomData).every(id=>AWCampaign.pool(AWCampaign.current()).includes(id))'),true);
      assert.equal(h.run('!AWLivingWildsEnemies.eligible(AWCampaign.current(),game.roomData).length||AWLivingWildsEnemies.eligible(AWCampaign.current(),game.roomData).every(id=>intensityState().encounter?.pool?.includes(id))'),true,node+' reinforcement pool');
    }
    for(const id of value(h,'AWLivingWildsEnemies.ids'))assert.ok(seen.has(id),id+' appears in initial campaign squads');
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('rulers retain identities and phases, direct summons retain their requested type, and saves contain no content state',()=>{
  const h=start();try{
    h.run('saveGame()');const keys=value(h,'Object.keys(JSON.parse(localStorage.getItem(SAVE_KEY))).sort()');
    for(const c of ['verdant','meridian','gloam']){
      visit(h,c+'-ruler');
      assert.equal(h.run('game.enemies.filter(e=>e.boss).length'),1);
      assert.equal(h.run('game.enemies.some(e=>e.type.startsWith("lw_"))'),false);
      h.run('var boss=game.enemies.find(e=>e.boss);boss.hp=boss.maxHp*.3;updateEnemyAI(boss,enemyDir(boss),8,boss.speed,.1);intensityTickBosses(.1);');
      assert.equal(h.run('boss.campaignPhase'),3);
    }
    visit(h,'verdant-road',1);
    assert.equal(h.run('game.enemies.filter(e=>e.type.startsWith("lw_")).every(e=>ENEMY_TYPES[e.type].min<=4)'),true);
    assert.equal(h.run('spawnEnemy("skeleton",{x:4,y:7},false,.68).type'),'skeleton');
    h.run('saveGame()');
    assert.deepEqual(value(h,'Object.keys(JSON.parse(localStorage.getItem(SAVE_KEY))).sort()'),keys);
    assert.equal(h.run('/livingWilds|spawning|_combatProfile/.test(localStorage.getItem(SAVE_KEY))'),false);
    assert.equal(h.run('loadGame()'),true);h.run('beginWorld()');
    assert.equal(h.run('game.campaign.current'),'verdant-road');
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});
