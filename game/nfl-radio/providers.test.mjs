import test from 'node:test';
import assert from 'node:assert/strict';
import {TEAMS,playable} from './core.mjs';
import {sirius} from './provider-data.mjs';
import {widgets,widgetFor,validWidget,safeListeningUrl,listeningLink,siriusEvent} from './providers.mjs';
const game={id:'test',away:'DAL',home:'NYG',date:'2026-09-13T20:20:00-04:00'};
test('every NFL team has a provider-published SiriusXM channel link',()=>{
 for(const t of TEAMS){const link=listeningLink('sirius',game,t.id);assert.ok(link,t.id);assert.match(link.url,/^https:\/\/sxm\.app\.link\//);assert.ok(sirius.teams[t.name].sourceUrl.startsWith('https://www.siriusxm.com/sports/nfl/'));}
});
test('national and Spanish feeds require the exact published matchup and kickoff',()=>{
 const event=siriusEvent(game);assert.ok(event);assert.equal(listeningLink('sirius',game,'DAL','spanish').url,event.spanish);
 assert.equal(listeningLink('sirius',{...game,date:'2027-09-13T20:20:00-04:00'},'DAL','national'),null);
 assert.equal(listeningLink('sirius',{...game,home:'DET'},'DAL','spanish'),null);
});
test('provider links never become direct audio authorization',()=>{
 for(const w of widgets){assert.ok(validWidget(w));assert.equal(playable(w,game),false);}
 assert.equal(playable(listeningLink('sirius',game,'DAL'),game),false);
 assert.equal(widgetFor('BUF'),undefined);assert.equal(widgetFor('DET').id,'iheart-det');
});
test('only fixed official destinations and explicit embed routes are allowed',()=>{
 for(const url of ['javascript:alert(1)','https://www.siriusxm.com.evil.example/','http://www.nfl.com/','https://me:secret@www.nfl.com/'])assert.equal(safeListeningUrl(url),false);
 assert.ok(safeListeningUrl('https://www.nfl.com/plus/'));
 for(const embedUrl of ['https://evil.example/live/test-1/?embed=true','https://www.iheart.com/?embed=true','https://www.iheart.com/live/test-1/'])assert.equal(validWidget({embedUrl}),false);
});
test('NFL+ handoff is explicitly the provider portal, not a fabricated game deep link',()=>{assert.equal(listeningLink('nfl',game,'DAL').url,'https://www.nfl.com/plus/');assert.match(listeningLink('nfl',game,'DAL').detail,/Choose this game/);});
