const test=require('node:test'),assert=require('node:assert/strict'),F=require('./franchise.js'),P=require('./progression.js');
const receiver=id=>P.migratePlayer({id,name:'Receiver '+id,speed:75,cutting:80,turning:80,evasion:77,catching:83,strength:72});
const franchise=()=>F.normalize({cash:450,round:23,wins:22,team:['x','h','y','z'].map(receiver),market:['a','b','c','d','e'].map(receiver)});
test('legacy migration preserves roster, progress and separate sanitized home / away uniforms',()=>{
 const f=franchise();assert.equal(f.cash,450);assert.equal(f.round,23);assert.equal(f.team[0].speed,75);assert.equal(f.currentGame.qb.attempts,0);assert.notEqual(f.identity.home.jersey,f.identity.away.jersey);
 f.identity.home.jersey='#112233';const restored=F.normalize(P.validateSave(JSON.stringify(f)));assert.equal(restored.identity.home.jersey,'#112233');assert.notEqual(restored.identity.away.jersey,'#112233');
 assert.equal(F.identity({home:{jersey:'url(x)'},logo:'<script>🔥</script>'}).logo,'🐺');assert.equal(F.identity({home:{jersey:'url(x)'}}).home.jersey,F.defaultKit.jersey);assert.equal(F.emoji('🐦‍🔥'),'🐦‍🔥');
});
test('all 520 opponent identities are repeatable and span all six stadium templates',()=>{
 const venues=new Set(),identities=new Set();for(let round=1;round<=520;round++){const a=F.opponent(round);assert.deepEqual(a,F.opponent(round));venues.add(a.venue);identities.add(a.primary+a.logo+a.pants+a.helmet);for(const k of F.colors)assert.match(a[k],/^#[a-f0-9]{6}$/i);}assert.equal(venues.size,6);assert.ok(identities.size>150);
});
test('one completed pass logs targets, signed yards, YAC, moves and records exactly once',()=>{
 const f=franchise(),play=F.newPlay('Mesh');play.attempt=true;play.target='x';play.receiver='x';play.catchSpot=12;play.contested=true;F.event(play,f.team[0],'stiffArms');F.event(play,f.team[0],'brokenTackles');F.event(play,f.team[1],'blocks');
 assert.equal(F.commitPlay(f,play,{snap:10,spot:30}),true);assert.equal(F.commitPlay(f,play,{snap:10,spot:50,touchdown:true}),false);assert.equal(f.currentGame.qb.attempts,1);assert.equal(f.career.qb.completions,1);assert.equal(f.career.qb.yards,20);assert.equal(f.career.receivers.x.yac,18);assert.equal(f.career.receivers.x.contested,1);assert.equal(f.career.receivers.h.blocks,1);assert.equal(f.records.mostYac.value,18);assert.ok(f.team[0].chemistry.xp>f.team[1].chemistry.xp);
});
test('incompletions, dropped balls, interceptions and clock expiry have distinct counting',()=>{
 const f=franchise();let p=F.newPlay('Stick');p.attempt=true;p.target='h';p.dropped='h';F.commitPlay(f,p);p=F.newPlay('Go');p.attempt=true;p.target='x';p.interception=true;F.commitPlay(f,p);F.commitPlay(f,F.newPlay('Mesh'));
 assert.equal(f.career.qb.attempts,2);assert.equal(f.career.qb.completions,0);assert.equal(f.career.qb.interceptions,1);assert.equal(f.career.receivers.h.drops,1);assert.equal(f.career.receivers.x.targets,1);
});
test('game summaries and career ledgers survive replacement, reload and current-game reset',()=>{
 const f=franchise(),p=F.newPlay('Fade');p.attempt=true;p.receiver='x';p.target='x';p.catchSpot=45;F.commitPlay(f,p,{snap:20,spot:50,touchdown:true});F.finishGame(f,{won:true,opponent:'Dragons',round:23,score:[3,1]});f.team[0]=receiver('new');F.normalize(f);
 assert.equal(f.currentGame.qb.attempts,0);assert.equal(f.lastGame.qb.yards,30);assert.equal(f.records.longestTouchdown.value,30);assert.equal(f.records.receivingGame.name,'Receiver x');assert.equal(f.career.receivers.x.games,1);assert.equal(f.winStreak,1);assert.equal(F.normalize(P.validateSave(f)).lastGame.score[0],3);
});
test('chemistry stays capped and suggestions adapt without removing any category',()=>{
 const f=franchise(),p=f.team[0];p.chemistry={xp:1e10,concepts:{Mesh:1e10}};F.normalize(f);assert.equal(F.chemistry(p,'Mesh'),.02);assert.ok(F.chemistry(p,'Other')<=.015);assert.ok(F.awareness(p)<=100);
 const quick={name:'Slant / Flat',category:'Quick Game',routes:['Slant','Flat','Stick','Fade']},deep={name:'Dagger',category:'Deep Shots',routes:['Go','Dig','Post','Go']};
 assert.ok(F.suggestion(quick,{down:3,toGo:3}).score>F.suggestion(deep,{down:3,toGo:3}).score);assert.ok(F.suggestion(deep,{down:3,toGo:22}).score>F.suggestion(quick,{down:3,toGo:22}).score);
});
