import test from 'node:test';
import assert from 'node:assert/strict';
import {newSave} from './core.mjs';
import {teamPower} from './power.mjs';
import {renderSocialView,renderTribeRankings} from './social-ui.mjs';

const monkey=(id='companion',overrides={})=>({...structuredClone(newSave('UI tests','capuchin','gps',123).run.monkey),id,...overrides});
function model(team,extra={}){
 const me={uid:'subject',name:'Canopy crew',team,wallet:{fruit:0,items:{}},...extra};
 return{status:'ready',now:1700000000000,me,members:[me],nearby:[],challenges:[],offers:[],tribe:{id:'CANOPY',name:'Canopy tribe',ownerId:'subject',members:['subject'],boss:{level:1,hp:1500,maxHp:1500}}};
}
function crew(html){
 const match=html.match(/<li\b[^>]*data-crew-id="subject"[\s\S]*?<\/li>/);
 assert.ok(match,'The shared crew must remain visible');return match[0];
}
const slots=html=>[...html.matchAll(/class="social-crew-slot(?: |")/g)].length;

test('untrusted crew names and portrait fields cannot inject event attributes or markup',()=>{
 const nickname='"><img src=x onerror="globalThis.crewInjected=1">';
 const named=crew(renderSocialView(model([monkey('named',{nickname})])));
 assert.match(named,/&lt;img/);assert.doesNotMatch(named,/<(?:img|script)\b/i);
 assert.doesNotMatch(named,/<[^>]+\bon(?:load|error)\s*=/i);
 const malformed=crew(renderSocialView(model([monkey('malformed',{stage:'" onload="globalThis.crewInjected=2',types:'leaf'})])));
 assert.equal(slots(malformed),3);assert.doesNotMatch(malformed,/<[^>]+\bon(?:load|error)\s*=/i);
});

test('inherited catalogue names are unknown slots rather than shared monkey portraits',()=>{
 const valid=monkey('real'),html=crew(renderSocialView(model([monkey('fake-one',{species:'constructor'}),monkey('fake-two',{species:'toString'}),valid])));
 assert.equal(slots(html),3);assert.equal((html.match(/social-crew-slot is-empty/g)||[]).length,2);
 assert.doesNotMatch(html,/data-monkey-id="fake-(?:one|two)"/);
 assert.match(html,/data-monkey-id="real"/);assert.match(html,new RegExp(`data-crew-power="${teamPower([valid])}"`));
});

test('missing and partial shared teams render three stable slots without throwing',()=>{
 for(const team of [undefined,null,'not-an-array',[],[null],[monkey()],[monkey(),null]]){
  const html=crew(renderSocialView(model(team)));
  assert.equal(slots(html),3);
  assert.equal((html.match(/data-monkey-id=/g)||[]).length,Array.isArray(team)&&team.some(p=>p?.species==='capuchin')?1:0);
 }
});

test('public crew ratings recompute from permanent team data instead of trusting saved ratings',()=>{
 const team=[monkey('one',{level:4,gear:{fang:2},hp:0,combatStats:{atk:999999}}),monkey('two',{level:2})];
 const m=model(team,{power:999999,teamPower:999999}),first=crew(renderSocialView(m)),expected=teamPower(team);
 assert.match(first,new RegExp(`data-crew-power="${expected}"`));assert.doesNotMatch(first,/data-crew-power="999999"/);
 m.me.team[0].level=8;
 assert.match(crew(renderSocialView(m)),new RegExp(`data-crew-power="${teamPower(team)}"`));
 assert.ok(teamPower(team)>expected);
});

test('top tribes show progress, shared ranks and the current tribe without exposing member identities',()=>{
 const m=model([monkey()]);m.tribeRankings=[{id:'HIGH',name:'High canopy',rank:1,bossLevel:9,bossesDefeated:8,memberCount:10},{id:'CANOPY',name:'Our canopy',rank:1,bossLevel:9,bossesDefeated:8,memberCount:3},{id:'LOW',name:'Lower canopy',rank:3,bossLevel:5,bossesDefeated:4,memberCount:2}];m.rankingsStatus='ready';m.rankingsUpdatedAt=m.now;
 const html=renderTribeRankings(m);assert.equal((html.match(/data-tribe-rank="1"/g)||[]).length,2);assert.match(html,/data-tribe-rank="3"/);assert.match(html,/class="social-tribe-ranking is-yours" data-ranked-tribe="CANOPY"/);assert.match(html,/Your tribe/);assert.match(html,/8 bosses defeated/);assert.match(html,/3 \/ 10 members/);assert.match(html,/Highest shared boss level first/);assert.match(html,/Loaded just now/);assert.match(renderSocialView(m),/aria-label="Top tribe rankings"/);
});

test('rankings preserve escaped names and explain loading, stale and empty states',()=>{
 const m={rankingsStatus:'loading',tribeRankings:[]};let html=renderTribeRankings(m);assert.match(html,/Loading tribe rankings/);assert.match(html,/data-social-action="refresh-rankings" disabled/);
 m.rankingsStatus='ready';html=renderTribeRankings(m);assert.match(html,/No tribes have started/);
 m.tribeRankings=[{id:'"><script>',name:'<img src=x onerror=bad>',rank:1,bossLevel:2,bossesDefeated:1,memberCount:1}];m.rankingsStatus='error';m.rankingsError='Offline <script>';
 html=renderTribeRankings(m);assert.match(html,/Showing the last loaded rankings/);assert.match(html,/&lt;img/);assert.match(html,/Offline &lt;script&gt;/);assert.doesNotMatch(html,/<(?:img|script)\b/);assert.doesNotMatch(html,/<[^>]+\bon(?:load|error)\s*=/i);
});
