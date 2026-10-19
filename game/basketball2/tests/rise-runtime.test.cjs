const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const Rise=require('../js/campaign/rise-to-glory.js');

function fixture(){
  const state=Rise.create();
  const ctx=vm.createContext({RiseToGlory:Rise,riseState:state,riseMatch:null,riseTraining:null,
    profiles:{},console,Math,JSON,Number,Object,String,
    matchPlayers:[],gameStarted:true,gamePaused:false,gameOver:false,possessionFrozen:true,
    dunkActive:false,layupActive:false,ballState:'dribble',passFlight:{active:false},
    simulationTime:10,lastShotShooter:'p1',scoreP1:15,scoreP2:7,
    document:{getElementById(){return null;}},
    getBallHandler(){return null;},getOpponents(){return [];},flatDistance(){return 1;},
    getNearestOpponent(){return {profile:{ratings:{dunking:40,closeShot:40,threePoint:75,midrange:70}}};},
    getPlayerById(id){return ctx.matchPlayers.find(player=>player.id===id);},
    __launches:0,__substitutions:[],__messages:[],__leaves:0,
    showRiseCampaign(tab,message){ctx.__messages.push({tab,message});},hideWinnerOverlay(){}});
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../js/campaign/rise-runtime.js'),'utf8'),ctx);
  return ctx;
}

test('team and passing practice use three CPU athletes even before a 1v1 rivalry',()=>{
  for(const type of ['passing','team']){
    const ctx=fixture();
    for(let count=0;count<2;count++){
      const game=Rise.nextGame(ctx.riseState);
      Rise.recordGame(ctx.riseState,{gameId:game.id,won:true});
    }
    assert.equal(Rise.nextGame(ctx.riseState).teamSize,1);
    vm.runInContext('riseSnapshotSetup=()=>{};launchRiseCourt=()=>{__launches++;};',ctx);
    ctx.startRiseTraining(type,ctx.riseState.captainId);
    assert.equal(ctx.__launches,1);assert.equal(ctx.riseMatch.teamSize,3);
    assert.equal(ctx.riseMatch.home.length,3);assert.equal(ctx.riseMatch.away.length,3);
    assert.ok(ctx.riseMatch.away.every(player=>player.id&&player.ratings));
  }
});

test('substitutions cannot replace a scorer or receiver while a ball is in flight',()=>{
  const ctx=fixture();ctx.riseMatch=Rise.prepareMatch(ctx.riseState);ctx.gamePaused=true;
  let lookedUp=false;ctx.getPlayerById=()=>{lookedUp=true;throw Error('must reject before replacing a slot');};
  for(const state of ['shot','pass','dunk','layup']){
    ctx.ballState=state;
    assert.equal(ctx.riseSubstitute('p1',ctx.riseState.captainId),false);
  }
  ctx.ballState='dribble';ctx.passFlight.active=true;
  assert.equal(ctx.riseSubstitute('p1',ctx.riseState.captainId),false);
  assert.equal(lookedUp,false);
});

test('live substitutions follow starters, fresh legs and matchup priorities',()=>{
  const ctx=fixture(),state=ctx.riseState;
  state.currency=10000;
  const guards=Rise.market(state).filter(player=>player.position!=='C').slice(0,2);
  guards.forEach(player=>Rise.sign(state,player.id));
  const a=guards[0],b=guards[1];
  for(const player of [a,b]) for(const key of ['perimeterDefense','lateralMovement','steals']) player.ratings[key]=player===a?35:85;
  const captain=state.players[state.captainId];
  const tired={id:'p1',campaignPlayerId:captain.id,teamId:0,stamina:.35,stats:{},profile:Rise.toProfile(captain)};
  ctx.matchPlayers=[tired,...state.starters.slice(1).map((id,index)=>({id:'home'+index,campaignPlayerId:id,teamId:0,stamina:.9,stats:{},profile:Rise.toProfile(state.players[id])}))];
  ctx.riseMatch=Rise.prepareMatch(state);
  vm.runInContext('riseSubstitute=(slot,id)=>{__substitutions.push({slot,id});return true;};',ctx);
  state.team.substitution='starters';ctx.updateRiseRuntime(17);
  assert.equal(ctx.__substitutions.length,0);
  state.team.substitution='fresh_legs';ctx.updateRiseRuntime(.01);
  assert.equal(ctx.__substitutions.length,1,'fresh legs rotates a player at 35% stamina');
  ctx.__substitutions.length=0;tired.stamina=.2;state.team.substitution='matchup';
  ctx.updateRiseRuntime(17);
  assert.equal(ctx.__substitutions[0].id,b.id,'perimeter defender is chosen against a shooting threat');
  ctx.__substitutions.length=0;a.role='sixth_man';a.overall=b.overall;state.team.substitution='balanced';
  ctx.updateRiseRuntime(17);
  assert.equal(ctx.__substitutions[0].id,a.id,'designated sixth man leads an otherwise equally fresh bench rotation');
});

test('finish passes actual accumulated contributions once with the current game ID',()=>{
  const ctx=fixture(),state=ctx.riseState,game=Rise.nextGame(state);
  ctx.riseMatch=Rise.prepareMatch(state);
  const id=state.captainId;
  ctx.matchPlayers=[{campaignPlayerId:id,teamId:0,stats:{points:6,fgm:3,fga:5,tpm:1,tpa:2,blocks:2,rebounds:4,assists:3,perfect:2}}];
  vm.runInContext('leaveRiseMatch=()=>{__leaves++;riseMatch=null;};',ctx);
  assert.equal(ctx.finishRiseMatch('p1'),true);
  assert.equal(state.season.completed[0].id,game.id);
  assert.equal(state.players[id].stats.threeMade,1);assert.equal(state.players[id].stats.threeAttempts,2);
  assert.equal(state.players[id].stats.points,6);assert.equal(state.players[id].stats.blocks,2);
  assert.equal(state.players[id].stats.assists,3);assert.equal(state.players[id].stats.goodShots,2);
  assert.equal(ctx.finishRiseMatch('p1'),false);assert.equal(state.record.wins,1);assert.equal(ctx.__leaves,1);
});

test('practice completion only awards successful live metrics and never changes the campaign fixture',()=>{
  const ctx=fixture(),state=ctx.riseState,id=state.captainId,game=Rise.nextGame(state).id;
  ctx.riseMatch=Rise.prepareMatch(state);
  ctx.riseTraining={type:'midrange',athleteId:id,tier:1,attempts:8,progress:4};
  vm.runInContext('leaveRiseMatch=()=>{riseTraining=null;riseMatch=null;};',ctx);
  ctx.finishRiseTraining(true);
  assert.equal(state.players[id].training.midrange.tier,1);
  assert.equal(state.players[id].xp,35);
  assert.equal(Rise.nextGame(state).id,game);assert.equal(state.record.wins,0);
  ctx.riseMatch=Rise.prepareMatch(state);ctx.riseTraining={type:'midrange',athleteId:id,tier:2,attempts:8,progress:1};
  ctx.finishRiseTraining(false);
  assert.equal(state.players[id].xp,35);assert.equal(state.players[id].training.midrange.tier,1);
});
