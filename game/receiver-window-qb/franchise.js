/* Persistent identity and box-score rules. No rendering or gameplay randomness. */
(function(root){
'use strict';
const number=(v,min=0,max=1e9)=>Math.max(min,Math.min(max,Number.isFinite(Number(v))?Number(v):0));
const count=v=>Math.floor(number(v));
const text=(v,fallback,max=80)=>typeof v==='string'&&v.trim()?v.trim().slice(0,max):fallback;
const colors=['helmet','jersey','secondary','pants','socks','shoes','number','accent'];
const logos=['🐺','🔥','⚡','👑','🦈','🐉','🦅','💀','🛡️','🌪️','🦁','🐅','🐻','🦬','🦊','🦇','🦂','🐍','🦖','🦍','🐏','🦌','🚀','☄️','🌋','❄️','🌊','🌙','☀️','💎','⚔️','🏈'];
const moreLogos=['🐯','🐆','🐘','🦏','🦛','🐊','🐙','🦑','🐝','🦋','🦉','🐦‍🔥','🐲','🦄','🐎','🦓','🦣','🐧','🦀','🦞','🐢','🦚','🪽','⭐','🌟','💫','🌀','🌩️','🪐','🌎','🎯','🔱','⚙️','⚓','🏔️','🏰','🎲','♠️','♣️','♥️','♦️','🥷','🤖','👽','👻','🎃','🏴‍☠️','🧊'];
const defaultKit={helmet:'#183659',jersey:'#235fba',secondary:'#eff7ff',pants:'#f1f4f7',socks:'#183659',shoes:'#181b20',number:'#ffffff',accent:'#5ee3ff'};
const color=(v,fallback)=>typeof v==='string'&&/^#[0-9a-f]{6}$/i.test(v)?v:fallback;
function emoji(v,fallback='🐺'){
  if(typeof v!=='string')return fallback;
  const s=v.trim();if(!s||s.length>24||/[<>"'&]/.test(s)||!/[\p{Extended_Pictographic}\p{Regional_Indicator}]/u.test(s))return fallback;
  if(typeof Intl.Segmenter==='function')return [...new Intl.Segmenter('en',{granularity:'grapheme'}).segment(s)][0].segment;
  return s;
}
function kit(raw={},defaults=defaultKit){return Object.fromEntries(colors.map(k=>[k,color(raw?.[k],defaults[k])]))}
function identity(raw={}){return {name:text(raw?.name,'Window Wolves',32),logo:emoji(raw?.logo),active:raw?.active==='away'?'away':'home',home:kit(raw?.home),away:kit(raw?.away,{...defaultKit,jersey:'#f1f4f7',pants:'#183659',number:'#183659',secondary:'#235fba'})};}
function hash(value){let n=2166136261;for(const c of String(value)){n^=c.charCodeAt(0);n=Math.imul(n,16777619);}return n>>>0;}
function opponent(round){
  const seed=hash('qb-team-'+Math.max(1,Math.floor(round))),palette=[['#a71f31','#f1c76a'],['#12724e','#d7ffe7'],['#5d39a4','#d1c7ff'],['#cf6b14','#fff0cf'],['#203b6b','#eaedf5'],['#21252e','#4be2d5'],['#7e2136','#ff9966'],['#315f22','#c9ff58'],['#075e78','#faf4cb'],['#36313e','#fa7392']][seed%10];
  const primary=palette[0],secondary=palette[1],helmet=(seed>>>4)%2?primary:secondary;
  return {logo:logos[(seed>>>8)%logos.length],venue:(seed>>>14)%6,endzoneStyle:(seed>>>20)%3,...kit({helmet,jersey:primary,secondary,pants:(seed>>>6)%2?'#f0f2f5':primary,socks:primary,shoes:'#181b20',number:secondary,accent:secondary}),primary,name:'Team '+round,accent:secondary};
}
const qbKeys=['attempts','completions','yards','touchdowns','interceptions'];
const receiverKeys=['targets','catches','yards','touchdowns','yac','drops','contested','brokenTackles','stiffArms','hurdles','jukes','blocks'];
function line(raw={},keys=receiverKeys){return Object.fromEntries(keys.map(k=>[k,number(raw?.[k],k==='yards'?-1e6:0)]));}
function box(raw={}){
  const receivers=Object.create(null);
  for(const [id,r] of Object.entries(raw?.receivers||{}).slice(0,2000)){
    if(!r||typeof r!=='object')continue;
    receivers[id]={...line(r),name:text(r.name,'Receiver'),games:count(r.games)};
  }
  return {qb:line(raw?.qb,qbKeys),receivers,longest:number(raw?.longest),plays:count(raw?.plays)};
}
const recordKeys=['longestCompletion','longestTouchdown','mostYac','receivingGame','touchdownGame','brokenTacklesGame','winStreak','highestScore'];
function normalize(f){
  f.identity=identity(f.identity);
  f.career=box(f.career);f.currentGame=box(f.currentGame);
  const records={};for(const key of recordKeys){const r=f.records?.[key];records[key]={value:number(r?.value),name:text(r?.name,'—')};}f.records=records;
  f.winStreak=count(f.winStreak);
  for(const p of [...(f.team||[]),...(f.market||[])]){
    const concepts=Object.create(null);for(const [key,v] of Object.entries(p.chemistry?.concepts||{}).slice(-64))concepts[key.slice(0,80)]=number(v,0,1000);
    p.chemistry={xp:number(p.chemistry?.xp,0,10000),concepts};
  }
  if(f.lastGame&&typeof f.lastGame==='object')f.lastGame={...box(f.lastGame),won:!!f.lastGame.won,opponent:text(f.lastGame.opponent,'Opponent'),round:count(f.lastGame.round),score:[count(f.lastGame.score?.[0]),count(f.lastGame.score?.[1])]};
  return f;
}
function awareness(p){return Math.round(number((number(p.cutting)+number(p.turning)+number(p.catching))*.23+number(p.evasion)*.16+number(p.tricks)*.15,1,100));}
function chemistry(p,concept){const c=p.chemistry||{};return number(Math.sqrt(number(c.xp)/10000)*.015+Math.sqrt(number(c.concepts?.[concept],0,1000)/1000)*.005,0,.02);}
function chemistryLevel(p){return Math.round(Math.sqrt(number(p.chemistry?.xp)/10000)*100);}
function primary(play){if(Number.isInteger(play.screen))return play.screen;const option=play.routes.indexOf('Option');if(option>=0)return option;return Math.max(0,play.routes.findIndex(r=>['Slant','Dig','Stick','Drag','Corner'].includes(r)));}
function suggestion(play,{down=1,toGo=25,spot=0}={}){
  const name=play.name.toLowerCase(),routes=play.routes||[];
  if(spot>=38)return {score:(/fade|bunch|rub|motion|stick|spacing|slant/.test(name)?12:0)+(routes.includes('Option')?3:0),reason:'Red zone: quick windows and traffic releases'};
  if(toGo<=7)return {score:(/mesh|stick|slant|spacing|screen|bubble/.test(name)?12:0)+(play.category==='Quick Game'?4:0),reason:`${down>=3?'Money down':'Short yardage'}: get beyond the marker`};
  if(down>=3&&toGo>=15)return {score:(/dagger|levels|flood|sail|cross|drive|post/.test(name)?12:0)+(routes.includes('Dig')?3:0),reason:'Long yardage: layer routes beyond the sticks'};
  return {score:(/mesh|slant|flood|stick|bubble|levels/.test(name)?8:0),reason:'Balanced downs: create a clear first read'};
}
function newPlay(concept){return {concept,attempt:false,target:null,receiver:null,catchSpot:0,contested:false,dropped:null,interception:false,events:Object.create(null),finalized:false};}
function event(play,p,key){if(!play||play.finalized||!p||!receiverKeys.includes(key))return;const e=play.events[p.id]||(play.events[p.id]={});e[key]=(e[key]||0)+1;}
function bumpRecord(f,key,value,name){if(value>f.records[key].value)f.records[key]={value,name};}
function commitPlay(f,play,{spot=0,snap=0,touchdown=false}={}){
  if(!play||play.finalized)return false;play.finalized=true;
  const roster=new Map(f.team.map(p=>[p.id,p])),receiver=roster.get(play.receiver),target=roster.get(play.target),yards=receiver?Math.round((spot-snap)*10)/10:0,yac=receiver?Math.round(Math.max(0,spot-play.catchSpot)*10)/10:0;
  if(target)eventBeforeFinal(target,'targets');
  if(receiver){for(const [key,value] of Object.entries({catches:1,yards,touchdowns:touchdown?1:0,yac,contested:play.contested?1:0}))eventBeforeFinal(receiver,key,value);}
  if(play.dropped&&roster.has(play.dropped))eventBeforeFinal(roster.get(play.dropped),'drops');
  function eventBeforeFinal(p,key,value=1){const e=play.events[p.id]||(play.events[p.id]={});e[key]=(e[key]||0)+value;}
  for(const book of [f.currentGame,f.career]){
    book.plays++;book.qb.attempts+=play.attempt?1:0;book.qb.completions+=receiver?1:0;book.qb.yards+=yards;book.qb.touchdowns+=receiver&&touchdown?1:0;book.qb.interceptions+=play.interception?1:0;book.longest=Math.max(book.longest,yards);
    for(const [id,events] of Object.entries(play.events)){const p=roster.get(id);if(!p)continue;const row=book.receivers[id]||(book.receivers[id]={...line(),name:p.name,games:0});for(const k of receiverKeys)row[k]+=events[k]||0;}
  }
  for(const p of f.team){const c=p.chemistry;c.concepts[play.concept]=Math.min(1000,(c.concepts[play.concept]||0)+1);c.xp=Math.min(10000,c.xp+1+(p===target?2:0)+(p===receiver?4+(play.contested?3:0)+(touchdown?5:0):0));}
  if(receiver){bumpRecord(f,'longestCompletion',yards,receiver.name);bumpRecord(f,'mostYac',yac,receiver.name);if(touchdown)bumpRecord(f,'longestTouchdown',yards,receiver.name);}
  return true;
}
function finishGame(f,{won,opponent,round,score}){
  f.lastGame={...JSON.parse(JSON.stringify(f.currentGame)),won,opponent,round,score};
  for(const [id,r] of Object.entries(f.currentGame.receivers)){if(f.career.receivers[id])f.career.receivers[id].games++;bumpRecord(f,'receivingGame',r.yards,r.name);bumpRecord(f,'touchdownGame',r.touchdowns,r.name);bumpRecord(f,'brokenTacklesGame',r.brokenTackles,r.name);}
  f.winStreak=won?f.winStreak+1:0;bumpRecord(f,'winStreak',f.winStreak,f.identity.name);bumpRecord(f,'highestScore',score[0]*7,f.identity.name);f.currentGame=box();
}
const api={colors,logos,moreLogos,defaultKit,kit,emoji,identity,opponent,hash,normalize,box,line,qbKeys,receiverKeys,recordKeys,awareness,chemistry,chemistryLevel,primary,suggestion,newPlay,event,commitPlay,finishGame};
if(typeof module!=='undefined')module.exports=api;root.QBFranchise=api;
})(typeof globalThis!=='undefined'?globalThis:this);
