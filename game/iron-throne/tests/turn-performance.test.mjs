import test from 'node:test';
import assert from 'node:assert/strict';
import { settlements } from '../core.mjs';
import { knowledgeView, refreshKnowledge, visionTiles } from '../fog.mjs';
import { createGame as legacyGame } from './fixtures/legacy-game.mjs';

function field() {
  const s={width:40,height:30,turn:1,tiles:{},armies:[],fleets:[],treaties:[],kingdoms:[
    {id:'ashen',relations:{wintermere:{trust:60}}},
    {id:'wintermere',relations:{ashen:{trust:60}}}
  ]};
  for(let r=0;r<s.height;r++)for(let q=0;q<s.width;q++){
    const id=`${q},${r}`;
    s.tiles[id]={id,q,r,owner:null,building:null,levels:{}};
  }
  return s;
}

test('settlement lookups see construction, capture, destruction and replaced tiles immediately',()=>{
  const s=field(),first=s.tiles['5,5'],second=s.tiles['10,10'];
  assert.deepEqual(settlements(s),[]);
  Object.assign(first,{owner:'ashen',building:'city'});
  assert.deepEqual(settlements(s,'ashen'),[first]);
  Object.assign(second,{owner:'wintermere',building:'town'});
  assert.deepEqual(settlements(s),[first,second]);
  first.owner='wintermere';
  assert.deepEqual(settlements(s,'ashen'),[]);
  assert.deepEqual(settlements(s,'wintermere'),[first,second]);
  first.building=null;
  s.tiles[second.id]={...second,owner:'ashen'};
  assert.deepEqual(settlements(s,'ashen'),[s.tiles[second.id]]);
  const result=settlements(s);result.pop();
  assert.equal(settlements(s).length,1,'callers may sort or edit their result array');
});

test('cached vision follows movement, scouting, construction, captures, fleets and shared sight',()=>{
  const s=field();
  const scout={id:'scout',owner:'ashen',tile:'5,5',units:{levy:20}};
  s.armies.push(scout);
  const initial=visionTiles(s,'ashen');initial.clear();
  assert.ok(visionTiles(s,'ashen').size>0,'the first returned set is also detached');
  const check=()=>{
    for(const viewer of ['ashen','wintermere'])for(const allied of [false,true]){
      const actual=visionTiles(s,viewer,{allied});
      assert.deepEqual(actual,visionTiles(structuredClone(s),viewer,{allied}));
      assert.deepEqual(visionTiles(s,viewer,{allied}),actual);
    }
  };
  check();assert.equal(visionTiles(s,'ashen').has('9,5'),false);
  scout.units.scout=1;check();assert.equal(visionTiles(s,'ashen').has('9,5'),true);
  scout.tile='15,5';check();assert.equal(visionTiles(s,'ashen').has('5,5'),false);
  scout.owner='wintermere';check();assert.equal(visionTiles(s,'ashen').size,0);
  const tower=s.tiles['25,15'];Object.assign(tower,{owner:'ashen',building:'watchtower',levels:{watchtower:1}});
  check();assert.equal(visionTiles(s,'ashen').has('31,15'),true);
  tower.levels.watchtower=3;check();assert.equal(visionTiles(s,'ashen').has('33,15'),true);
  tower.owner='wintermere';check();assert.equal(visionTiles(s,'ashen').size,0);
  const alliance={type:'alliance',parties:['ashen','wintermere'],expires:20};s.treaties.push(alliance);
  check();assert.equal(visionTiles(s,'ashen').has(scout.tile),true);
  assert.equal(visionTiles(s,'ashen',{allied:false}).has(scout.tile),false);
  scout.units={levy:19};check();assert.equal(visionTiles(s,'ashen').has(scout.tile),false);
  scout.units.levy=20;check();assert.equal(visionTiles(s,'ashen').has(scout.tile),true);
  s.kingdoms[1].relations.ashen.trust=49;check();assert.equal(visionTiles(s,'ashen').size,0);
  s.kingdoms[1].relations.ashen.trust=60;check();
  s.turn=20;check();assert.equal(visionTiles(s,'ashen').size,0);
  s.fleets.push({id:'fleet',owner:'ashen',tile:'10,20',ships:[{type:'transport'}]});
  check();assert.equal(visionTiles(s,'ashen').has('14,20'),false);
  s.fleets[0].ships.push({type:'warship'});check();assert.equal(visionTiles(s,'ashen').has('14,20'),true);
  s.fleets[0].tile='20,20';check();assert.equal(visionTiles(s,'ashen').has('10,20'),false);
  const returned=visionTiles(s,'ashen');returned.clear();
  assert.ok(visionTiles(s,'ashen').size>0,'editing a returned set cannot corrupt cached sight');
});

test('unchanged visibility still refreshes visible construction and observation records',()=>{
  const s=legacyGame(),capital=settlements(s,'ashen')[0];
  refreshKnowledge(s);
  const before=knowledgeView(s,'ashen'),seen=visionTiles(s,'ashen');
  capital.project={type:'wall',remaining:2,total:2,level:1,owner:'ashen'};
  capital.road=false;capital.name='Renamed capital';
  refreshKnowledge(s);
  const after=knowledgeView(s,'ashen');
  assert.deepEqual(visionTiles(s,'ashen'),seen);
  assert.equal(after.tiles[capital.id].name,'Renamed capital');
  assert.equal(after.tiles[capital.id].project.type,'wall');
  assert.equal(s.fog.houses.ashen.tiles[capital.id].tile.road,false);
  assert.equal(before.tiles[capital.id].project,null,'previous detached views stay unchanged');
});
