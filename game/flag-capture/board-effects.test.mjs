import test from 'node:test';
import assert from 'node:assert/strict';
import {FACTIONS,GROUND_EFFECTS,formationStatus} from './engine.mjs';
import {groundArt,GROUND_ART_KINDS,groundLayerHTML,groundDescription,formationOverlayHTML} from './board-effects.mjs';

const field=(kind,id,side='player',expiresAt=12)=>({kind,id,side,x:2,y:3,sourceId:'u1',createdAt:0,expiresAt,nextTickAt:3});
const troop=(id,x,y,faction='bloom',side='player',hp=6)=>({id,x,y,faction,side,hp,maxHp:6,role:'scout'});
const state=units=>({width:9,height:7,time:0,units});
const deepFreeze=value=>{if(value&&typeof value==='object'){Object.values(value).forEach(deepFreeze);Object.freeze(value);}return value;};
const segments=html=>[...html.matchAll(/<path d="M([\d.]+),([\d.]+)L([\d.]+),([\d.]+)"/g)].map(match=>match.slice(1).map(Number));
const rings=html=>[...html.matchAll(/<circle cx="([\d.]+)" cy="([\d.]+)"/g)].map(match=>match.slice(1).map(Number));
const normalized=values=>values.map(value=>JSON.stringify(value)).sort();

test('local field ownership stays explicit for both players instead of depending on the active dock',()=>{
  const fields=deepFreeze([field('mycelium','g1','player'),field('rift','g2','ai')]);
  const local=groundLayerHTML(fields,2,GROUND_EFFECTS,'local');
  assert.match(local,/ground-clock ground-friendly[^>]*data-ground-id="g1"[^>]*>P1 <span>10s/);
  assert.match(local,/ground-clock ground-hostile[^>]*data-ground-id="g2"[^>]*>P2 <span>10s/);
  const labels=groundDescription(fields,2,GROUND_EFFECTS,'local');
  assert.match(labels,/Living Mycelium, Player 1 field/);assert.match(labels,/Rift Ink, Player 2 field/);
  assert.doesNotMatch(local,/[◆◇]/);assert.match(groundLayerHTML(fields,2,GROUND_EFFECTS,'player'),/>◆ <span>/);
});

test('every rules-defined persistent ground effect has distinct nonempty board art',()=>{
  assert.equal(Object.keys(GROUND_EFFECTS).length,6);
  assert.deepEqual([...GROUND_ART_KINDS].sort(),Object.keys(GROUND_EFFECTS).sort());
  const paints=Object.keys(GROUND_EFFECTS).map(kind=>groundArt(kind));
  assert.equal(new Set(paints).size,6);
  for(const paint of paints){assert.match(paint,/^<svg[^>]+aria-hidden="true"/);assert.match(paint,/<path /);assert.ok(paint.length>100);}
  const castKinds=new Set(Object.values(FACTIONS).flatMap(f=>Object.values(f.units).flatMap(u=>u.abilities.map(skill=>skill.groundKind).filter(Boolean))));
  assert.deepEqual([...castKinds].sort(),Object.keys(GROUND_EFFECTS).sort());
});

test('unknown and inherited registry keys cannot become accidental art, clocks or descriptions',()=>{
  for(const kind of ['unknown','constructor','toString','__proto__']){
    assert.equal(groundArt(kind),'');
    assert.equal(groundLayerHTML([field(kind,'g1')],0,GROUND_EFFECTS),'');
    assert.equal(groundDescription([field(kind,'g1')],0,GROUND_EFFECTS),'');
  }
  const inherited=Object.create({mycelium:GROUND_EFFECTS.mycelium});
  assert.equal(groundLayerHTML([field('mycelium','g1')],0,inherited),'');
  assert.equal(groundDescription([field('mycelium','g1')],0,inherited),'');
});

test('fields stay visible until their exact expiry and remaining time rounds upward',()=>{
  const effects=[field('mycelium','expired','player',4),field('sanctum','active','player',9.1)];
  const html=groundLayerHTML(effects,4,GROUND_EFFECTS);
  assert.doesNotMatch(html,/expired/);assert.match(html,/data-ground-id="active"/);
  assert.match(html,/6 seconds remaining/);assert.match(html,/>6s</);
  assert.match(groundLayerHTML(effects,9.099,GROUND_EFFECTS),/>1s</);
  assert.equal(groundLayerHTML(effects,9.1,GROUND_EFFECTS),'');
  assert.equal(groundDescription(effects,9.1,GROUND_EFFECTS),'');
});

test('friendly and hostile indicators follow the viewing army rather than faction color',()=>{
  const effects=[field('mycelium','player-field'),field('mycelium','ai-field','ai')];
  const player=groundLayerHTML(effects,2,GROUND_EFFECTS,'player'),other=groundLayerHTML(effects,2,GROUND_EFFECTS,'ai');
  assert.match(player,/ground-friendly" data-ground-art="player-field"/);
  assert.match(player,/ground-hostile" data-ground-art="ai-field"/);
  assert.match(other,/ground-hostile" data-ground-art="player-field"/);
  assert.match(other,/ground-friendly" data-ground-art="ai-field"/);
  assert.equal((player.match(/ground-mycelium /g)||[]).length,2);
});

test('overlapping fields cap visible paint at two while announcing every active field',()=>{
  const effects=[field('mycelium','old'),field('thornfield','middle','ai'),field('sanctum','new'),field('cinders','gone','ai',1)];
  const html=groundLayerHTML(effects,2,GROUND_EFFECTS);
  assert.doesNotMatch(html,/data-ground-art="old"|data-ground-art="gone"/);
  assert.match(html,/data-ground-art="middle"/);assert.match(html,/data-ground-art="new"/);assert.match(html,/ground-more">\+1</);
  const description=groundDescription(effects,2,GROUND_EFFECTS);
  for(const kind of ['mycelium','thornfield','sanctum'])assert.ok(description.includes(GROUND_EFFECTS[kind].name));
  assert.ok(!description.includes(GROUND_EFFECTS.cinders.name));
});

test('ground tooltips retain exact rules, ownership and timers without leaking HTML attributes',()=>{
  const id='g1" onpointerdown="<img src=x>&',name='Mycelium "<b>" & garden',description='Allies <heal> & enemies "slow".';
  const definitions={mycelium:{...GROUND_EFFECTS.mycelium,name,description}};
  const effects=[field('mycelium',id,'ai',9.4)];
  const html=groundLayerHTML(effects,2,definitions);
  assert.doesNotMatch(html,/<img|<b>| onpointerdown="/);
  assert.match(html,/data-ground-id="g1&quot; onpointerdown=&quot;&lt;img src=x&gt;&amp;"/);
  assert.match(html,/Mycelium &quot;&lt;b&gt;&quot; &amp; garden/);
  assert.match(html,/Allies &lt;heal&gt; &amp; enemies &quot;slow&quot;\./);
  assert.equal(groundDescription(effects,2,definitions),`${name}, enemy field, 8 seconds: ${description}`);
  assert.equal(groundDescription(effects,2,definitions,'ai'),`${name}, allied field, 8 seconds: ${description}`);
});

test('Brood links connect only live orthogonal members of the selected formation',()=>{
  const a=troop('a',1,1),b=troop('b',2,1),c=troop('c',2,2),outside=troop('outside',6,4),fallen=troop('fallen',1,2,'bloom','player',0),enemy=troop('enemy',3,1,'chaos','ai');
  const game=state([a,b,c,outside,fallen,enemy]),status=formationStatus(game,a),html=formationOverlayHTML(game,a,status);
  assert.equal(status.active,true);
  assert.deepEqual(normalized(segments(html)),normalized([[1.5,1.5,2.5,1.5],[2.5,1.5,2.5,2.5]]));
  assert.deepEqual(normalized(rings(html)),normalized([[1.5,1.5],[2.5,1.5],[2.5,2.5]]));
  assert.match(html,/viewBox="0 0 9 7"/);
});

test('Choir procession draws a consecutive firing line without diagonal or remote connections',()=>{
  const line=[1,2,3,4].map((x,i)=>troop(`c${i}`,x,3,'choir')),outside=troop('outside',5,4,'choir');
  const game=state([...line,outside]),status=formationStatus(game,line[0]),html=formationOverlayHTML(game,line[0],status);
  assert.deepEqual(normalized(segments(html)),normalized([[1.5,3.5,2.5,3.5],[2.5,3.5,3.5,3.5],[3.5,3.5,4.5,3.5]]));
  assert.equal(rings(html).length,4);
  line[2].hp=0;
  assert.equal(formationOverlayHTML(game,line[0],formationStatus(game,line[0])),'');
});

test('Chaos pincer links point from opposite hunters to their enemy, never hunter to hunter',()=>{
  const a=troop('left',1,2,'chaos'),b=troop('right',3,2,'chaos'),target=troop('target',2,2,'bloom','ai'),unrelated=troop('corner',2,1,'chaos');
  const game=state([a,b,target,unrelated]),status=formationStatus(game,a),html=formationOverlayHTML(game,a,status);
  assert.equal(status.active,true);
  assert.deepEqual(normalized(segments(html)),normalized([[1.5,2.5,2.5,2.5],[3.5,2.5,2.5,2.5]]));
  assert.deepEqual(normalized(rings(html)),normalized([[1.5,2.5],[3.5,2.5]]));
  target.hp=0;
  assert.equal(formationOverlayHTML(game,a,status),'');
});

test('a stale pincer disappears when its partner moves to a corner or no longer surrounds the target',()=>{
  const a=troop('left',1,2,'chaos'),b=troop('right',3,2,'chaos'),target=troop('target',2,2,'bloom','ai');
  const game=state([a,b,target]),status=formationStatus(game,a);
  Object.assign(b,{x:2,y:1});assert.equal(formationOverlayHTML(game,a,status),'');
  Object.assign(b,{x:4,y:2});assert.equal(formationOverlayHTML(game,a,status),'');
});

test('malformed formation membership cannot highlight opponents, fallen troops, unrelated factions or duplicates',()=>{
  const a=troop('a',1,1),b=troop('b',2,1),dead=troop('dead',2,2,'bloom','player',0),enemy=troop('enemy',1,2,'chaos','ai'),different=troop('other-faction',2,0,'choir');
  const game=state([a,b,dead,enemy,different]),status={active:true,unitIds:['a','b','b','dead','enemy','other-faction','missing'],targetIds:[]};
  const html=formationOverlayHTML(game,a,status);
  assert.deepEqual(rings(html),[[1.5,1.5],[2.5,1.5]]);
  assert.equal(segments(html).length,1);
  assert.equal(formationOverlayHTML(game,a,{...status,unitIds:['a','dead','enemy','missing']}),'');
  assert.equal(formationOverlayHTML(game,a,{...status,unitIds:['b','different']}),'');
  a.hp=0;assert.equal(formationOverlayHTML(game,a,status),'');
});

test('inactive, empty and missing selections draw nothing, and rendering never mutates rules state',()=>{
  const a=troop('a',1,1),b=troop('b',2,1),c=troop('c',2,2),game=state([a,b,c]),status=formationStatus(game,a),effects=[field('mycelium','g1'),field('cinders','g2','ai')];
  const before=structuredClone({game,status,effects});deepFreeze(game);deepFreeze(status);deepFreeze(effects);
  groundLayerHTML(effects,2,GROUND_EFFECTS);groundDescription(effects,2,GROUND_EFFECTS);formationOverlayHTML(game,a,status);
  assert.deepEqual({game,status,effects},before);
  assert.equal(formationOverlayHTML(game,null,status),'');
  assert.equal(formationOverlayHTML(game,a,{...status,active:false}),'');
  assert.equal(formationOverlayHTML(game,a,{active:true,unitIds:[]}),'');
});
