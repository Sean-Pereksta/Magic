/* Shared, deterministic passing-football rules. No rendering or legacy-save mutation. */
(function(root){
'use strict';
const clamp=(v,a=0,b=1e9)=>Math.max(a,Math.min(b,Number.isFinite(Number(v))?Number(v):a));
const integer=(v,a=0,b=1e9)=>Math.floor(clamp(v,a,b));
const clone=v=>JSON.parse(JSON.stringify(v));
const text=(v,f='',n=80)=>typeof v==='string'?v.slice(0,n):f;
const hash=value=>{let n=2166136261;for(const c of String(value)){n^=c.charCodeAt(0);n=Math.imul(n,16777619);}return n>>>0;};
const definitions=[
 ['academy_precision','QB Precision Academy','academy','qb','precision','Place ten throws in moving targets. Accuracy and timely glove contact earn medals.'],
 ['academy_windows','Passing Window Challenge','academy','qb','windows','Read leverage and release while a visible passing window is open.'],
 ['academy_contested','Contested Catch Academy','academy','receiver','contested','Control the receiver through high points, back shoulders, one-hand and toe-tap catches.'],
 ['academy_defense','Defensive Skills Academy','academy','defender','defense','Control a defensive back. Real glove contact is required for swat and interception credit.'],
 ['academy_routes','Route Running Academy','academy','receiver','routes','Control releases and cuts, create separation, then meet the ball.'],
 ['academy_pressure','Pressure Passing','academy','qb','pressure','Beat a real pass clock and pressure assignments without sacrificing placement.'],
 ['receiver_battles','1v1 Receiver Battles','duel','receiver',null,'Ten alternating receiver and defender rounds, with paired overtime if tied.'],
 ['red_zone','Red Zone Challenge','challenge','qb',null,'Three possessions from the opponent 20. Score on at least two drives.'],
 ['two_minute','Two-Minute Drill','challenge','qb',null,'Down four with two timeouts. Drive 40 yards before the final live play ends.'],
 ['seven_seven','7v7 Passing League','match','qb',null,'Seven actual players per side, four timed periods, alternating possessions and equal-possession overtime.'],
 ['survival','Survival Gauntlet','challenge','qb',null,'Complete drives against stronger units. A failed series ends the run.'],
 ['season','Season Mode','season','qb',null,'Eight-team 7v7 passing-football league: seven weeks, standings, semifinals and championship.'],
 ['career','Franchise Career','season','qb',null,'Carry a separate 7v7 career through seasons, championships and progressively stronger leagues.']
];
const catalog=definitions.map(([id,name,kind,role,challengeId,description])=>Object.freeze({id,name,kind,role,challengeId,description}));
const modes=Object.fromEntries(catalog.map(d=>[d.id,d]));
const academyTechniques=['high_point','back_shoulder','one_hand','toe_tap'];
const outcomes=new Set(['complete','incomplete','touchdown','interception','sack','fumble']);
const conferenceNames=['Window Wolves','Harbor Sharks','Canyon Hawks','Metro Lightning','Iron Bears','Coastal Vipers','Summit Kings','Desert Fire'];
const identities=['disciplined_zone','ball_hawks','physical_press','explosive_athletes'];
function normalizeRecord(raw={}){return {best:clamp(raw.best,0,100),medal:['bronze','silver','gold'].includes(raw.medal)?raw.medal:null,runs:integer(raw.runs),wins:integer(raw.wins),bestStage:integer(raw.bestStage,0,1000)};}
function normalizeProfile(raw={}){
 const p={version:1,runCounter:integer(raw?.runCounter),records:{},active:null,leagues:{},career:{year:integer(raw?.career?.year,1,10000),championships:integer(raw?.career?.championships),history:[]},rewardLedger:[]};
 for(const d of catalog)p.records[d.id]=normalizeRecord(raw?.records?.[d.id]);
 p.rewardLedger=(Array.isArray(raw?.rewardLedger)?raw.rewardLedger:[]).filter(x=>typeof x==='string'&&x.length<180).slice(-512);
 p.career.history=(Array.isArray(raw?.career?.history)?raw.career.history:[]).filter(x=>x&&Number.isFinite(x.year)).slice(-20).map(x=>({year:integer(x.year,1),champion:text(x.champion),playerWins:integer(x.playerWins),playerLosses:integer(x.playerLosses)}));
 for(const [key,l] of Object.entries(raw?.leagues||{}).slice(-5))if(!['__proto__','constructor','prototype'].includes(key)&&validLeague(l))p.leagues[key]=clone(l);
 const a=raw?.active;
 if(a&&modes[a.mode]&&typeof a.id==='string'&&['ready','live','break','finished'].includes(a.phase)&&Number.isFinite(a.spot)&&Number.isFinite(a.down)&&a.down>=1&&a.down<=4&&a.spot>=0&&a.spot<=50&&a.score&&Number.isFinite(a.score.player)&&Number.isFinite(a.score.opponent)&&(!a.leagueId||p.leagues[a.leagueId])){
  p.active=clone(a);const s=p.active;s.clockSeconds=s.clockSeconds==null?null:clamp(s.clockSeconds,0,120);s.spot=clamp(s.spot,0,50);s.marker=clamp(s.marker,s.spot,50);s.score={player:integer(s.score.player,0,10000),opponent:integer(s.score.opponent,0,10000)};s.seen=(Array.isArray(s.seen)?s.seen:[]).filter(x=>typeof x==='string').slice(-256);s.playElapsed=clamp(s.playElapsed,0,600);s.possession=s.possession==='opponent'?'opponent':'player';s.pendingForce=!!s.pendingForce;s.points=clamp(s.points,0,1000);s.rep=integer(s.rep,0,100);s.period=integer(s.period,1,1000);s.timeouts={player:integer(s.timeouts?.player,0,3),opponent:integer(s.timeouts?.opponent,0,3)};
 }
 return p;
}
// The renderer does not serialize an airborne football/actor pose. Resume an
// interrupted snap at its original LOS, retaining already-consumed game time.
function recoverInterrupted(profile){const s=profile.active;if(s?.phase==='live'){s.phase='ready';s.playElapsed=0;s.released=false;s.pendingForce=false;s.contacts=[];s.clockRunning=false;return true;}return false;}
function validLeague(l){return l&&Array.isArray(l.teams)&&l.teams.length===8&&l.teams.every(t=>typeof t.id==='string'&&typeof t.name==='string')&&Array.isArray(l.fixtures)&&l.fixtures.length<=40&&l.fixtures.every(f=>typeof f.id==='string'&&l.teams.some(t=>t.id===f.home)&&l.teams.some(t=>t.id===f.away)&&f.home!==f.away)&&Number.isFinite(l.week);}
function createLeague({seed=1,year=1,teamName='Window Wolves',career=false}={}){
 const teams=conferenceNames.map((name,i)=>({id:'t'+i,name:i?name:text(teamName,'Window Wolves',32),rating:clamp(54+(hash(seed+':team:'+i)%27)+Math.min(15,(year-1)*2),30,99),identity:identities[hash(seed+':id:'+i)%4],wins:0,losses:0,ties:0,pointsFor:0,pointsAgainst:0}));
 const fixtures=[],rotation=teams.map(t=>t.id);
 for(let week=1;week<=7;week++){
  for(let i=0;i<4;i++){const a=rotation[i],b=rotation[7-i];fixtures.push({id:`y${year}-w${week}-g${i}`,week,round:'regular',home:week%2?a:b,away:week%2?b:a,played:false,score:null});}
  rotation.splice(1,0,rotation.pop());
 }
 return {id:(career?'career-':'season-')+year+'-'+hash(seed),seed,year,career,teams,fixtures,week:1,phase:'regular',champion:null,completed:false,historyWritten:false};
}
function standings(league){return league.teams.map(t=>({...t})).sort((a,b)=>(b.wins*2+b.ties)-(a.wins*2+a.ties)||(b.pointsFor-b.pointsAgainst)-(a.pointsFor-a.pointsAgainst)||b.pointsFor-a.pointsFor||a.id.localeCompare(b.id));}
function fixturePending(l){return l.fixtures.find(f=>f.week===l.week&&!f.played&&(f.home==='t0'||f.away==='t0'));}
function recordFixture(l,f,home,away){
 if(f.played)return false;f.played=true;f.score=[integer(home,0,200),integer(away,0,200)];
 const a=l.teams.find(t=>t.id===f.home),b=l.teams.find(t=>t.id===f.away);a.pointsFor+=f.score[0];a.pointsAgainst+=f.score[1];b.pointsFor+=f.score[1];b.pointsAgainst+=f.score[0];if(home===away){a.ties++;b.ties++;}else{(home>away?a:b).wins++;(home>away?b:a).losses++;}return true;
}
function simulateFixture(l,f){
 const a=l.teams.find(t=>t.id===f.home),b=l.teams.find(t=>t.id===f.away),seed=hash(l.seed+':'+f.id),pa=7*(1+(seed%5)+(a.rating>b.rating+8?1:0)),pb=7*(1+((seed>>>5)%5)+(b.rating>a.rating+8?1:0));
 // Simulated other-team fixtures have declared deterministic outcomes; live user games never do.
 recordFixture(l,f,pa,pa===pb?pb+7:pb);
}
function advanceLeague(l){
 for(const f of l.fixtures.filter(f=>f.week===l.week&&!f.played))simulateFixture(l,f);
 if(l.phase==='regular'&&l.week<7){l.week++;return;}
 if(l.phase==='regular'){
  const rank=standings(l);l.phase='semifinal';l.week=8;l.fixtures.push({id:`y${l.year}-semi-1`,week:8,round:'semifinal',home:rank[0].id,away:rank[3].id,played:false,score:null},{id:`y${l.year}-semi-2`,week:8,round:'semifinal',home:rank[1].id,away:rank[2].id,played:false,score:null});
 }else if(l.phase==='semifinal'){
  const winners=l.fixtures.filter(f=>f.round==='semifinal').map(f=>f.score[0]>f.score[1]?f.home:f.away);l.phase='championship';l.week=9;l.fixtures.push({id:`y${l.year}-final`,week:9,round:'championship',home:winners[0],away:winners[1],played:false,score:null});
 }else{const final=l.fixtures.find(f=>f.round==='championship');l.champion=final.score[0]>final.score[1]?final.home:final.away;l.phase='complete';l.completed=true;}
}
function seekUserFixture(l){for(let guard=0;guard<10&&!l.completed&&!fixturePending(l);guard++)advanceLeague(l);return fixturePending(l);}
function sessionBase(id,mode,seed,dayKey){return {version:1,id,mode,seed,dayKey:text(dayKey,'',10),phase:'ready',possession:'player',score:{player:0,opponent:0},spot:10,marker:20,down:1,period:1,clockSeconds:null,clockRunning:false,playElapsed:0,playNumber:0,released:false,periodExpired:false,pendingForce:false,timeouts:{player:3,opponent:3},rep:0,points:0,stage:1,driveNumber:1,touchdowns:0,overtime:false,otDrives:0,seen:[],result:null,leagueId:null,fixtureId:null,contacts:[]};}
function start(profile,id,options={}){
 if(!modes[id])throw new Error('Unknown football mode: '+id);
 if(options.resume!==false&&profile.active?.mode===id&&profile.active.phase!=='finished')return profile.active;
 const seed=integer(options.seed??hash(id+':'+profile.runCounter),0,0xffffffff),s=sessionBase('mode-'+hash(id+':'+seed+':'+(++profile.runCounter)).toString(36),id,seed,options.dayKey);
 if(id==='red_zone'){s.spot=30;s.marker=40;}if(id==='two_minute'){s.score.opponent=4;s.clockSeconds=120;s.timeouts={player:2,opponent:2};}
 if(['seven_seven','season','career'].includes(id)){s.clockSeconds=120;s.period=1;}
 if(modes[id].kind==='academy'){s.spot=id==='academy_contested'?30:15;s.marker=Math.min(50,s.spot+10);s.timeouts={player:0,opponent:0};}
 if(id==='receiver_battles'){s.spot=25;s.marker=35;}
 if(['season','career'].includes(id)){
  let l=Object.values(profile.leagues).reverse().find(l=>l.career===(id==='career')&&!l.completed);
  if(!l||options.newSeason){l=createLeague({seed,year:id==='career'?profile.career.year:1,teamName:options.teamName,career:id==='career'});profile.leagues[l.id]=l;while(Object.keys(profile.leagues).length>5)delete profile.leagues[Object.keys(profile.leagues)[0]];}
  const fixture=seekUserFixture(l);s.leagueId=l.id;
  if(fixture)s.fixtureId=fixture.id;else{s.phase='finished';s.result={won:l.champion==='t0',reason:'Season complete',leagueComplete:true};}
 }
 profile.active=s;return s;
}
function medal(score){return score>=95?'gold':score>=80?'silver':score>=60?'bronze':null;}
function resetSeries(s,spot=10){s.spot=spot;s.marker=Math.min(50,spot+10);s.down=1;s.phase='ready';s.clockRunning=false;s.released=false;s.pendingForce=false;s.playElapsed=0;s.contacts=[];}
function controlled(s){if(modes[s.mode].kind==='academy')return modes[s.mode].role;if(s.mode==='receiver_battles')return s.possession==='player'?'receiver':'defender';if(['red_zone','two_minute','survival'].includes(s.mode))return 'qb';return s.possession==='player'?'qb':'defender';}
function setup(s){
 const d=modes[s.mode];if(!d)throw new Error('Unknown football mode');const full=['seven_seven','season','career'].includes(s.mode),pressure=s.mode==='academy_pressure',duel=s.mode==='receiver_battles'||['academy_contested','academy_defense','academy_routes'].includes(s.mode);
 const count=full?6:duel||s.mode==='academy_precision'?1:s.mode==='academy_windows'?2:pressure?3:4;
 const routes=['Slant','Drag','Corner','Go','Choice','Out'],labels=['X','H','Y','Z','R','C'],positions=['WR','SL','TE','WR','RB','C'];
 const offense=[{id:'QB',position:'QB',eligible:false,assignment:'passer',startX:0,depth:-5}];
 for(let i=0;i<count;i++)offense.push({id:labels[i],slot:i,position:positions[i],eligible:true,route:routes[(i+integer(s.rep))%routes.length],assignment:i===5?'eligible_snapper':'route',startX:full?[-20,-11,-3,7,15,0][i]:[-16,-5,6,17][i]??0,depth:i===5?0:(i%2)*.35});
 const defense=[];const n=full?7:s.mode==='academy_precision'?0:duel?1:s.mode==='academy_windows'?3:pressure?5:6;
 for(let i=0;i<n;i++)defense.push({id:'D'+i,position:i<count?'CB':i===n-1?'FS':'LB',assignment:pressure&&i===n-1?'rush':full&&s.playNumber%3===1?'zone':i<count?'man':'zone',target:i<count?i:null,zoneX:[-20,-10,0,10,20,0,12][i],zoneDepth:i<count?3:10+(i%2)*8});
 const role=controlled(s),technique=s.mode==='academy_contested'?academyTechniques[s.rep%4]:s.mode==='academy_defense'?['standing_swat','jump_swat','dive_pbu','interception'][s.rep%4]:null;
 return {mode:s.mode,name:d.name,fieldLengthYards:50,ruleset:full?'7v7_passing':duel?'receiver_duel':'passing_challenge',offense,defense,controlledRole:role,controlledSlot:0,userOnOffense:s.possession==='player',autoQB:role!=='qb',spot:s.spot,down:s.down,toGo:Math.max(0,s.marker-s.spot),marker:s.marker,clockSeconds:s.clockSeconds,period:s.period,throwClockSeconds:pressure?Math.max(2.2,3.5-s.rep*.10):full?4:8,difficulty:clamp(.28+(s.stage-1)*.075+s.rep*.018,.2,.95),coverage:full?['man','zone','bracket'][s.playNumber%3]:'man',technique,targetZone:s.mode==='academy_precision'?['outside','inside','high_point','low_away','back_shoulder'][s.rep%5]:null,rep:s.rep+1,totalReps:d.kind==='academy'||s.mode==='receiver_battles'?10:null,stage:s.stage,phase:s.phase,score:{...s.score},passOnly:full,centerEligible:full,scoring:full?'Touchdown 6 + automatic conversion 1; no field goals or punts.':''};
}
function emitSetup(s,commands){commands.push({type:'setup',setup:setup(s)});}
function award(profile,s,amount,reason,commands,key=s.id){const id='modes:'+key;if(profile.rewardLedger.includes(id))return 0;profile.rewardLedger.push(id);profile.rewardLedger=profile.rewardLedger.slice(-512);commands.push({type:'award',id,cash:integer(amount,0,1000),reason});return integer(amount,0,1000);}
function recordCareer(profile,l){if(l.historyWritten||!l.completed)return;l.historyWritten=true;const p=l.teams[0],champ=l.teams.find(t=>t.id===l.champion);if(l.career){profile.career.history.push({year:l.year,champion:champ.name,playerWins:p.wins,playerLosses:p.losses});profile.career.history=profile.career.history.slice(-20);if(l.champion==='t0')profile.career.championships++;profile.career.year=Math.max(profile.career.year,l.year+1);}}
function finish(profile,s,won,reason,commands,extra={}){
 if(s.phase==='finished')return;s.phase='finished';s.clockRunning=false;const record=profile.records[s.mode];record.runs++;if(won)record.wins++;record.bestStage=Math.max(record.bestStage,s.stage);
 let value=clamp(s.points/10,0,100),personalBest=value>record.best;
 if(modes[s.mode].kind==='academy'){
  const previous=record.best;record.best=Math.max(record.best,value);record.medal=medal(record.best);commands.push({type:'academy_result',challengeId:modes[s.mode].challengeId,score:value,medal:medal(value),personalBest,previousBest:previous,runId:s.id});
 }
 s.result={won:!!won,reason,score:{...s.score},academyScore:modes[s.mode].kind==='academy'?value:null,medal:modes[s.mode].kind==='academy'?medal(value):null,personalBest,stage:s.stage,...extra};
 if(s.leagueId){const l=profile.leagues[s.leagueId],f=l.fixtures.find(f=>f.id===s.fixtureId);if(f&&!f.played){const playerHome=f.home==='t0';recordFixture(l,f,playerHome?s.score.player:s.score.opponent,playerHome?s.score.opponent:s.score.player);award(profile,s,won?75:25,won?'League victory':'League participation',commands,l.id+':'+f.id);advanceLeague(l);seekUserFixture(l);recordCareer(profile,l);s.result.leagueComplete=l.completed;s.result.champion=l.completed?l.teams.find(t=>t.id===l.champion).name:null;if(l.completed&&l.champion==='t0')award(profile,s,350,'League championship',commands,l.id+':champion');}}
 else if(modes[s.mode].kind!=='academy')award(profile,s,s.mode==='survival'?Math.min(150,(s.stage-1)*15):won?60:15,reason,commands);
 commands.push({type:'result',result:clone(s.result),terminal:true});
}
function periodEnd(profile,s,commands){
 s.periodExpired=false;
 if(s.mode==='two_minute'){finish(profile,s,s.score.player>s.score.opponent,'Time expired',commands);return;}
 if(s.period<4){s.period++;s.clockSeconds=120;s.clockRunning=false;if(s.period===3)s.timeouts={player:3,opponent:3};commands.push({type:'period',period:s.period});return;}
 if(s.score.player!==s.score.opponent){finish(profile,s,s.score.player>s.score.opponent,'Regulation complete',commands);return;}
 s.overtime=true;s.otDrives=0;s.period=5;s.clockSeconds=null;s.possession='player';resetSeries(s,30);commands.push({type:'period',period:5,overtime:true});
}
function clockAdvance(profile,s,seconds,commands){
 seconds=clamp(seconds,0,600);if(s.phase==='live')s.playElapsed+=seconds;
 if(s.clockSeconds!==null&&(s.clockRunning||s.phase==='live')){s.clockSeconds=Math.max(0,s.clockSeconds-seconds);if(!s.clockSeconds){if(s.phase==='live')s.periodExpired=true;else{periodEnd(profile,s,commands);if(s.phase==='ready')emitSetup(s,commands);}}}
 if(s.phase==='live'&&!s.released&&!s.pendingForce&&s.playElapsed>=setup(s).throwClockSeconds){s.pendingForce=true;commands.push({type:'force_end',outcome:'sack',yards:-5,reason:'Pass clock expired'});}
}
function academyPoints(s,e,outcome){
 const caught=['complete','touchdown'].includes(outcome)&&e.physicalContact===true,placement=1-clamp(e.placementError??2,0,3)/3,timing=1-clamp(Math.abs(e.timingError??.5),0,1.5)/1.5;
 switch(s.mode){
 case'academy_precision':return caught?35+40*placement+25*timing:0;
 case'academy_windows':return caught?40+35*(e.window==='clean'?1:e.window==='tight'?.45:0)+25*timing:0;
 case'academy_contested':return caught?60+20*(e.technique===academyTechniques[s.rep%4]?1:0)+20*timing:0;
 case'academy_defense':return e.physicalContact===true?(outcome==='interception'?100:e.passBreakup===true?80:e.tip===true?50:0):0;
 case'academy_routes':return 45*(1-clamp(e.routeError??3,0,5)/5)+25*clamp(e.separation??0,0,3)/3+(caught?30:0);
 case'academy_pressure':return caught?40+35*placement+25*(1-clamp(e.releaseTime??s.playElapsed,0,4)/4):0;
 default:return 0;
 }
}
function endDrive(profile,s,success,commands,reason){
 s.driveNumber++;s.down=Math.min(4,s.down);s.clockRunning=false;
 if(s.mode==='two_minute'){finish(profile,s,success,success?'Winning touchdown':reason,commands);return;}
 if(s.mode==='survival'){
  if(!success){finish(profile,s,false,reason,commands);return;}s.touchdowns++;s.phase='break';commands.push({type:'result',terminal:false,result:{won:true,reason:'Stage '+s.stage+' cleared',next:'Stronger defense',stage:s.stage}});return;
 }
 if(s.mode==='red_zone'){
  if(success)s.touchdowns++;if(s.driveNumber>3){finish(profile,s,s.touchdowns>=2,s.touchdowns+' touchdowns in three red-zone possessions',commands);return;}s.phase='break';commands.push({type:'result',terminal:false,result:{won:success,reason,next:'Possession '+s.driveNumber+' of 3'}});return;
 }
 if(s.overtime){s.otDrives++;if(s.otDrives%2===0&&s.score.player!==s.score.opponent){finish(profile,s,s.score.player>s.score.opponent,'Overtime complete',commands);return;}s.possession=s.possession==='player'?'opponent':'player';resetSeries(s,30);return;}
 s.possession=s.possession==='player'?'opponent':'player';resetSeries(s,10);
}
function dispatch(profile,event={}){
 const s=profile.active,commands=[];if(!s)return {session:null,commands,reward:0,rejected:'No active mode'};let rejected=null;
 const reject=reason=>{rejected=reason;};
 if(event.type==='continue'){
  if(s.phase==='break'){if(s.mode==='survival')s.stage++;resetSeries(s,s.mode==='red_zone'?30:10);emitSetup(s,commands);}
  else if(s.phase==='finished'&&s.leagueId){const l=profile.leagues[s.leagueId];if(!l.completed||s.mode==='career'){start(profile,s.mode,{seed:hash(s.seed+':'+profile.career.year),resume:false,teamName:l.teams[0].name});emitSetup(profile.active,commands);}}
 }else if(s.phase==='finished'||s.phase==='break')reject('Continue or choose a mode before the next snap.');
 else if(event.type==='snap'){
  if(s.phase!=='ready')reject('A play is already live.');else{s.phase='live';s.playNumber++;s.playElapsed=0;s.released=false;s.pendingForce=false;s.contacts=[];s.clockRunning=s.clockSeconds!==null;commands.push({type:'snap',playId:s.id+':'+s.playNumber,setup:setup(s)});}
 }else if(event.type==='tick')clockAdvance(profile,s,event.seconds,commands);
 else if(event.type==='release'){if(s.phase==='live')s.released=true;else reject('The ball is not live.');}
 else if(event.type==='timeout'){
  const side=event.side==='opponent'?'opponent':'player';if(s.phase!=='ready'||s.clockSeconds===null||!s.timeouts[side])reject('Timeout unavailable.');else{s.timeouts[side]--;s.clockRunning=false;commands.push({type:'timeout',side,remaining:s.timeouts[side]});}
 }else if(event.type==='ball_contact'){
  if(s.phase==='live'&&event.physicalContact===true)s.contacts.push({side:event.side==='defense'?'defense':'offense',technique:text(event.technique),time:s.playElapsed});s.contacts=s.contacts.slice(-16);
 }else if(event.type==='play_end'){
  const playId=text(event.id,s.id+':'+s.playNumber,180);
  if(s.seen.includes(playId))reject('Duplicate play result.');else if(s.phase!=='live')reject('Snap the ball before ending a play.');else if(!outcomes.has(event.outcome))reject('Unknown play outcome.');else if(event.outcome==='interception'&&event.physicalContact!==true)reject('An interception requires actual defensive glove contact.');else{
   if(Number.isFinite(event.elapsed))clockAdvance(profile,s,Math.max(0,event.elapsed-s.playElapsed),commands);
   s.seen.push(playId);s.seen=s.seen.slice(-256);const outcome=['complete','touchdown'].includes(event.outcome)&&event.physicalContact!==true?'incomplete':event.outcome;
   s.phase='ready';s.pendingForce=false;const def=modes[s.mode];
   if(def.kind==='academy'){
    const points=Math.round(academyPoints(s,event,outcome));s.points+=points;s.rep++;commands.push({type:'rep_result',points,rep:s.rep,total:10});if(s.rep>=10)finish(profile,s,s.points>=600,'Academy complete',commands);else{resetSeries(s,s.mode==='academy_contested'?30:15);emitSetup(s,commands);}
   }else if(s.mode==='receiver_battles'){
    const completed=['complete','touchdown'].includes(outcome),offensePoints=outcome==='touchdown'?2:completed&&clamp(event.yards??0,-50,50)>=8?1:0,defensePoints=outcome==='interception'?2:offensePoints?0:1,own=s.possession==='player';s.score[own?'player':'opponent']+=offensePoints;s.score[own?'opponent':'player']+=defensePoints;s.rep++;s.possession=own?'opponent':'player';
    if(s.rep>=10&&s.rep%2===0&&s.score.player!==s.score.opponent)finish(profile,s,s.score.player>s.score.opponent,'Alternating receiver battle complete',commands);else{resetSeries(s,25);emitSetup(s,commands);}
   }else{
    const completed=['complete','touchdown'].includes(outcome),old=s.spot,gain=completed||outcome==='sack'?clamp(Number.isFinite(Number(event.yards))?Number(event.yards):0,-50,50):0;s.spot=clamp(old+gain,0,50);s.clockRunning=completed&&!event.outOfBounds;
    if((outcome==='touchdown'||completed&&s.spot>=50)){s.score[s.possession]+=7;endDrive(profile,s,true,commands,'Touchdown');}
    else if((outcome==='sack'||completed)&&old+gain<=0){s.score[s.possession==='player'?'opponent':'player']+=2;endDrive(profile,s,false,commands,'Safety');}
    else if(outcome==='interception'||outcome==='fumble')endDrive(profile,s,false,commands,'Turnover');
    else if(s.spot>=s.marker){s.down=1;s.marker=Math.min(50,s.spot+10);}
    else if(++s.down>4)endDrive(profile,s,false,commands,'Turnover on downs');
    if(s.phase!=='finished'&&s.periodExpired)periodEnd(profile,s,commands);
    if(s.phase==='ready')emitSetup(s,commands);
   }
  }
 }else reject('Unknown mode event.');
 return {session:profile.active,commands,reward:commands.filter(c=>c.type==='award').reduce((n,c)=>n+c.cash,0),rejected};
}
function summary(profile){const s=profile.active;if(!s)return null;const config=setup(s),league=s.leagueId?profile.leagues[s.leagueId]:null,fixture=league?.fixtures.find(f=>f.id===s.fixtureId),other=fixture?league.teams.find(t=>t.id===(fixture.home==='t0'?fixture.away:fixture.home)):null;return {session:s,setup:config,league,standings:league?standings(league):null,opponent:other,result:s.result};}
const api={catalog,modes,normalizeProfile,recoverInterrupted,start,setup,dispatch,summary,medal,createLeague,standings,hash,rules:{fieldLengthYards:50,firstDownYards:10,periodSeconds:120,periods:4,touchdownPoints:6,automaticConversionPoints:1,sevenPerSide:true,passOnly:true,eligibleCenter:true}};
if(typeof module!=='undefined')module.exports=api;root.QBModes=api;
})(typeof globalThis!=='undefined'?globalThis:this);
