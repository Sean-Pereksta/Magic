import test,{after} from 'node:test';
import assert from 'node:assert/strict';
import {runtime} from './runtime-test-helper.mjs';

// Read the final script order so registrations added by later content packs are covered.
const h=runtime();
after(()=>h.close());
const value=code=>JSON.parse(h.run(`JSON.stringify(${code})`));
const catalog=value('AWArtCatalog');
const types=value('ENEMY_TYPES');
const continents=value('AWCampaignData.continents.filter(c=>!c.endless).map(c=>({id:c.id,biomes:c.biomes}))');
const pursuits=value('Object.keys(AWDangerContent.pursuits)');
const normalize=name=>String(name||'').trim().replace(/^the\s+/i,'').toLowerCase().replace(/[^a-z0-9]/g,'');
const memberships=id=>Object.entries(catalog.regions).filter(([,ids])=>ids.includes(id)).map(([region])=>region).sort();
const ordinary=Object.values(catalog.entries).filter(entry=>entry.category==='enemy'&&!entry.boss);
const bosses=Object.values(catalog.entries).filter(entry=>entry.boss);

test('catalog covers every live enemy registration and keeps one sheet per canonical appearance',()=>{
  assert.equal(Object.keys(types).length,124,'112 packaged registrations and 12 living wilds registrations');
  assert.equal(ordinary.length,124);
  assert.equal(bosses.length,26);
  assert.equal(Object.keys(catalog.entries).length,170);
  assert.deepEqual(ordinary.map(entry=>entry.id).sort(),Object.keys(types).sort());
  assert.equal(new Set(Object.values(catalog.entries).map(entry=>entry.image)).size,170,'aliases must not duplicate sheets');
  assert.equal(Object.values(catalog.entries).filter(entry=>entry.category==='armor').length,14);
  assert.equal(Object.values(catalog.entries).filter(entry=>entry.category==='terrain').length,4);
  assert.equal(catalog.weapons.length,16);
  assert.equal(catalog.spells.length,20);
  for(const id of value('AWLivingWildsEnemies.ids'))assert(catalog.entries[id],id);
  for(const [id,entry] of Object.entries(catalog.entries)){
    assert.equal(entry.id,id);
    assert.equal(entry.meta.id,id);
    assert.equal(entry.meta.image,entry.image);
    assert.match(entry.image,/^assets\/[a-zA-Z0-9_./-]+\.png$/,id+' canonical portable filename');
    assert(!entry.image.includes('..'),id+' safe relative path');
    if(entry.boss){assert.equal(entry.artId,id);assert.equal(entry.kind,'boss');}
  }
});

test('all registered, campaign, dungeon and legendary boss identities resolve to canonical dedicated art',()=>{
  const encounters=[];
  value('BOSSES').forEach((boss,index)=>encounters.push({id:index<4?'legacy-'+index:'expansion-'+(index-4),name:boss.name}));
  const nodes=value("Object.values(AWCampaignData.nodes).filter(n=>!n.shadow&&['boss','ruler','dungeon'].includes(n.type)).map(n=>({id:n.id,type:n.type,name:n.type==='ruler'?AWCampaignData.continent(n.continent).ruler:n.name}))");
  for(const node of nodes)encounters.push({id:node.id+(node.type==='dungeon'?':4':''),name:node.name+(node.type==='dungeon'?' Guardian':'')});
  for(const boss of value('AWDangerContent.legendary'))encounters.push({id:boss.id,name:boss.name});
  assert.equal(encounters.length,40);
  assert.equal(Object.keys(catalog.bossNames).length,36);
  assert.deepEqual(Object.keys(catalog.bossAliases).sort(),encounters.map(encounter=>encounter.id).sort());
  for(const encounter of encounters){
    const id=catalog.bossAliases[encounter.id],entry=catalog.entries[id];
    assert(entry?.boss,encounter.id+' dedicated boss sheet');
    assert.equal(catalog.bossNames[normalize(encounter.name)],id,encounter.id+' exact runtime display name');
    assert.equal(catalog.bossNames[normalize('The '+encounter.name.replace(/^The\s+/i,''))],id,encounter.id+' leading article normalization');
  }
  assert.equal(new Set(Object.values(catalog.bossAliases)).size,26);
  assert.equal(catalog.bossAliases['legacy-0'],catalog.bossAliases['verdant-boss2']);
  assert.equal(catalog.bossAliases['legacy-1'],catalog.bossAliases['gloam-boss1']);
  assert.equal(catalog.bossAliases['legacy-2'],catalog.bossAliases['meridian-boss3']);
  assert.equal(catalog.bossAliases['legacy-3'],catalog.bossAliases['gloam-ruler']);
  for(const [alias,id] of Object.entries(catalog.bossAliases)){
    assert(catalog.entries[id]?.boss,alias);
    if('boss_'+alias!==id)assert.equal(catalog.entries['boss_'+alias],undefined,alias+' shares canonical entry');
  }
});

test('actual spawns select hosted art for every fixed boss encounter, including all landmark dungeon guardians',()=>{
  h.run('startNewGame();game.level=30');
  const spawned=value(`(()=>{
    const rows=[];
    BOSSES.forEach((definition,index)=>{
      game.enemies=[];
      const room={...game.roomData,campaignNode:null,campaignRoom:0},enemy=spawnBoss(definition,room);
      rows.push({id:index<4?'legacy-'+index:'expansion-'+(index-4),name:enemy.name,art:AWArtRenderer.enemyAssetId(enemy,room)});
    });
    for(const node of Object.values(AWCampaignData.nodes).filter(node=>!node.shadow&&['boss','ruler','dungeon'].includes(node.type))){
      const room=node.type==='dungeon'?4:0;
      game.campaign.current=node.id;game.campaign.room=room;game.room=AWCampaign.coordinates(node.id,room);loadRoom();
      const enemy=game.enemies.find(enemy=>enemy.boss);
      rows.push({id:node.id+(node.type==='dungeon'?':4':''),name:enemy?.name,art:AWArtRenderer.enemyAssetId(enemy,game.roomData)});
    }
    for(const definition of AWDangerContent.legendary){
      game.enemies=[];
      const room={...game.roomData,campaignNode:null,campaignRoom:0},enemy=spawnBoss({...definition,hp:6,damage:1.2,color:'#abc'},room);
      enemy.dangerLegend=definition.id;
      rows.push({id:definition.id,name:enemy.name,art:AWArtRenderer.enemyAssetId(enemy,room)});
    }
    return rows;
  })()`);
  assert.equal(spawned.length,40);
  assert.equal(new Set(spawned.map(row=>row.id)).size,40);
  for(const row of spawned){
    assert(row.name,row.id+' actually spawned');
    assert.equal(row.art,catalog.bossAliases[row.id],row.id+' '+row.name+' exact encounter selection');
  }
  assert.deepEqual(h.errors,[]);
});

test('regional membership respects live region declarations, shadows, pursuit migration and summon dependencies',()=>{
  for(const [region,ids] of Object.entries(catalog.regions)){
    assert.equal(new Set(ids).size,ids.length,region+' unique references');
    for(const id of ids)assert(catalog.entries[id],region+':'+id);
  }
  const shadows=Object.entries(types).filter(([,type])=>type.shadowOnly);
  assert.equal(shadows.length,12);
  for(const [id] of shadows)assert.deepEqual(memberships(id),['shadow'],id+' shadow only');
  for(const id of pursuits)assert.deepEqual(memberships(id),continents.map(continent=>continent.id).sort(),id+' may migrate between continents');
  for(const [id,type] of Object.entries(types)){
    if(type.shadowOnly||pursuits.includes(id))continue;
    if(type.region)assert.deepEqual(memberships(id),[type.region],id+' explicit region');
    else if(type.biomes.length)assert.deepEqual(memberships(id),continents.filter(continent=>type.biomes.some(biome=>continent.biomes.includes(biome))).map(continent=>continent.id).sort(),id+' biome regions');
  }
  assert.deepEqual(catalog.summonDependencies,{
    groveShaman:['regionalHealPlant'],prismMimic:['regionalPrismEcho'],shardcaster:['regionalCrystal'],
    shadowMimic:['regionalPrismEcho'],fallenArchmage:['regionalCrystal']
  });
  for(const [source,dependencies] of Object.entries(catalog.summonDependencies))for(const id of dependencies){
    assert(types[source]&&types[id],source+' live summon dependency '+id);
    for(const region of memberships(source))assert(catalog.regions[region].includes(id),source+' preloads '+id+' in '+region);
  }
  assert.deepEqual(memberships('regionalHealPlant'),['verdant']);
  assert.deepEqual(memberships('regionalPrismEcho'),['meridian','shadow']);
  assert.deepEqual(memberships('regionalCrystal'),['meridian','shadow']);
  assert.deepEqual(memberships('boss_sandWorm'),['verdant'],'legendary continent overrides base biome');
  assert.deepEqual(memberships('boss_roc'),['gloam'],'legendary continent overrides base biome');
});

test('all authored frame rectangles, pivots and animation references remain valid without source provenance',()=>{
  const dirs=['SE','SW','NW','NE'],states=['idle','move','attack','death'];
  function checkAnimations(animations,ids,label){
    if(!animations||typeof animations!=='object')return;
    const frames=animations.frames||animations.frameIds;
    if(frames){
      assert(animations.fps>0&&Number.isFinite(animations.fps),label+' fps');
      assert(frames.length>0,label+' frames');
      for(const id of frames)assert(ids.has(id),label+':'+id);
    }else for(const [key,value] of Object.entries(animations))checkAnimations(value,ids,label+'/'+key);
  }
  for(const entry of Object.values(catalog.entries)){
    const meta=entry.meta,ids=new Set();
    assert(Number.isFinite(meta.width)&&meta.width>0,entry.id+' width');
    assert(Number.isFinite(meta.height)&&meta.height>0,entry.id+' height');
    assert(!Object.hasOwn(meta,'source'),entry.id+' runtime metadata omits provenance');
    for(const frame of meta.frames){
      assert(!ids.has(frame.id),entry.id+' unique frame '+frame.id);ids.add(frame.id);
      const {rect,pivot}=frame;
      assert([rect.x,rect.y,rect.w,rect.h,pivot.x,pivot.y].every(Number.isFinite),entry.id+'/'+frame.id+' finite bounds');
      assert(rect.x>=0&&rect.y>=0&&rect.w>0&&rect.h>0&&rect.x+rect.w<=meta.width&&rect.y+rect.h<=meta.height,entry.id+'/'+frame.id+' atlas bounds');
      assert(pivot.x>=0&&pivot.x<=rect.w&&pivot.y>=0&&pivot.y<=rect.h,entry.id+'/'+frame.id+' local pivot');
      assert(!Object.hasOwn(frame,'sourceBounds'),entry.id+'/'+frame.id+' compact runtime metadata');
      if(entry.category==='enemy')assert.deepEqual(Object.keys(frame).sort(),['id','pivot','rect'],entry.id+'/'+frame.id+' compact creature frame');
    }
    checkAnimations(meta.animations,ids,entry.id);
    if(entry.category==='enemy'||entry.category==='armor'){
      assert.equal(meta.frames.length,32,entry.id+' 32 authored poses');
      assert.deepEqual(Object.keys(meta.animations).sort(),dirs.slice().sort(),entry.id+' four facings');
      for(const dir of dirs){
        assert.deepEqual(Object.keys(meta.animations[dir]).sort(),states.slice().sort(),entry.id+'/'+dir+' four states');
        for(const state of states){
          const animation=meta.animations[dir][state];
          assert.equal(animation.frames.length,2,entry.id+'/'+dir+'/'+state+' two poses');
          assert.equal(animation.loop,state==='idle'||state==='move',entry.id+'/'+dir+'/'+state+' loop semantics');
          for(const id of animation.frames)assert(id.startsWith(dir+'-'),entry.id+'/'+dir+' authored direction');
        }
      }
    }
  }
});
