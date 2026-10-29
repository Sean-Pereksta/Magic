import test from 'node:test';
import assert from 'node:assert/strict';
import {newSave} from './core.mjs';
import {chooseRoute} from './trail-routes.mjs';
import {trackJournalGoal,placeDecoration,setSanctuaryArea} from './adventure-life.mjs';
import {ADVENTURE_ART,adventureArt,decorationArt} from './adventure-art.mjs';
import * as UI from './adventure-ui.mjs';
const save=()=>newSave('World visitor','capuchin','practice',1979);
const bloom=Date.UTC(2026,9,31,12);

test('each initial destination and event has distinct self-contained, labelled artwork',()=>{
 const keys=['berry-grove','ancient-ruins','misty-canopy','fruit-bloom','monkey-migration','essence-winds','guardian-stirring','traveling-merchant'];
 const drawings=keys.map(key=>{const svg=adventureArt(key,{uid:'same-id'});assert.match(svg,/role="img" aria-labelledby="adventure-same-id-title"/);assert.match(svg,/<title[^>]*>[^<]+<\/title>/);assert.doesNotMatch(svg,/<image|<script|(?:href|src)="https?:\/\//);return svg.replace(/<title[^>]*>.*?<\/title>/,'');});
 assert.equal(new Set(drawings).size,keys.length);
 for(const key of ['camp-meal','camp-study','camp-pack','camp-merchant','journal-explorer','journal-friends','journal-guardian','sanctuary-clearing','sanctuary-river','technique-canopy-relay','technique-dewfall-chorus'])assert.ok(ADVENTURE_ART[key],key);
 assert.notEqual(decorationArt('welcome-stone',{uid:'same'}),decorationArt('bloom-planter',{uid:'same'}));
});
test('route choices disclose saved duration, benefit and tradeoff and become a current-trail card',()=>{
 const s=save(),choice=UI.renderRouteChoices(s,{now:bloom});
 assert.equal((choice.match(/data-action="adventure-route"/g)||[]).length,3);
 assert.equal((choice.match(/<b>Consider:<\/b>/g)||[]).length,3);
 assert.match(choice,/Fruit Bloom/);assert.match(choice,/next camp/);assert.match(choice,/choice is saved/);
 chooseRoute(s,'ancient-ruins');const selected=UI.renderRouteChoices(JSON.parse(JSON.stringify(s)),{now:bloom});
 assert.match(selected,/YOUR CHOSEN TRAIL/);assert.match(selected,/Ancient Ruins/);assert.doesNotMatch(selected,/data-action="adventure-route"/);
});
test('forecast includes exact event bounds, quiet stretches, local zone and offline policy',()=>{
 const markup=UI.renderForecast(save(),{now:Date.UTC(2026,9,29,12)});
 assert.match(markup,/datetime="2026-10-31T00:00:00.000Z"/);assert.match(markup,/datetime="2026-11-01T00:00:00.000Z"/);
 for(const phrase of ['Fruit Bloom','Monkey Migration','Essence Winds','Guardian Stirring','Traveling Merchant','A quiet stretch','one extra fruit','Bonuses apply when','no refresh required'])assert.ok(markup.includes(phrase),phrase);
 assert.ok(markup.includes(Intl.DateTimeFormat().resolvedOptions().timeZone));
 const unavailable=UI.renderForecast(save(),{now:NaN});assert.match(unavailable,/timing cannot be confirmed/);assert.doesNotMatch(unavailable,/ACTIVE NOW/);
});
test('journal tracking, owned sanctuary displays, and player text remain understandable and escaped',()=>{
 const s=save();s.run.monkey.nickname='<img src=x onerror=alert(1)>';s.log=['<script>bad()</script>'];
 trackJournalGoal(s,'first-paths');const journal=UI.renderAdventureJournal(s),tracked=UI.renderTrackedJournalGoal(s);
 assert.match(journal,/FOLLOWING THIS GOAL/);assert.match(tracked,/A thousand little steps/);assert.match(journal,/&lt;script&gt;/);assert.doesNotMatch(journal,/<script>/);
 placeDecoration(s,'welcome-stone','left');const home=UI.renderSanctuaryHome(s);
 assert.match(home,/Welcome Stone/);assert.match(home,/left · placed/);assert.match(home,/&lt;img/);assert.doesNotMatch(home,/<img/);
 s.world.totalDistance=1000;setSanctuaryArea(s,'river');assert.match(UI.renderSanctuaryHome(s),/River Rest/);
});
test('camp visitor agrees with event calendar and technique feedback never leaks into a new battle',()=>{
 const s=save();s.run.phase='camp';s.run.camp={id:'camp-test'};
 const merchant=Date.UTC(2026,10,12,12),open=UI.renderCampMoments(s,{now:merchant}),quiet=UI.renderCampMoments(s,{now:merchant+86400000});
 assert.match(open,/A visitor’s small collection/);assert.match(open,/data-action="adventure-merchant-trade"/);assert.doesNotMatch(quiet,/data-action="adventure-merchant-trade"/);
 s.run.lastTeamTechnique={id:'canopy-relay',name:'Canopy Relay',artKey:'technique-canopy-relay',effect:'A prepared strike.',round:3,battleId:'old'};
 assert.match(UI.renderTeamTechniqueFeedback(s),/Canopy Relay/);s.run.battle={id:'new'};assert.equal(UI.renderTeamTechniqueFeedback(s),'');
});
