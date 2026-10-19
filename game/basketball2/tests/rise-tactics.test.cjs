const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');

class Vector3{
  constructor(x=0,y=0,z=0){this.set(x,y,z);}
  set(x,y,z){this.x=x;this.y=y;this.z=z;return this;}
  setY(y){this.y=y;return this;}
  clone(){return new Vector3(this.x,this.y,this.z);}
  copy(v){return this.set(v.x,v.y,v.z);}
  addScaledVector(v,s){this.x+=v.x*s;this.y+=v.y*s;this.z+=v.z*s;return this;}
  sub(v){return this.addScaledVector(v,-1);}
  length(){return Math.hypot(this.x,this.y,this.z);}
  normalize(){const n=this.length()||1;this.x/=n;this.y/=n;this.z/=n;return this;}
  dot(v){return this.x*v.x+this.y*v.y+this.z*v.z;}
  lerp(v,t){this.x+=(v.x-this.x)*t;this.y+=(v.y-this.y)*t;this.z+=(v.z-this.z)*t;return this;}
}
function fixture(size=3){
  const ctx=vm.createContext({Math,THREE:{Vector3,MathUtils:{clamp:(n,a,b)=>Math.max(a,Math.min(b,n))}},
    HOOP_POS:new Vector3(),teamAITime:1,aiDefensePlans:{},ballState:'dribble',shotCharging:false,
    flatDistance:(a,b)=>Math.hypot(a.x-b.x,a.z-b.z),keepInCourt(){},
    aiIsHuman:p=>!!p.controllerSlot,getForward:a=>new Vector3(Math.sin(a),0,Math.cos(a)),
    riseMatch:{defense:'balanced',awayDefense:'balanced'},riseState:{team:{matchups:{}}}});
  const make=(id,teamId,x,z)=>({id,campaignPlayerId:'campaign-'+id,teamId,group:{position:new Vector3(x,0,z)},velocity:new Vector3(),
    angle:0,bodyRadius:.55,profile:{speed:1,width:1,shot:1,block:1,ratings:{threePoint:50,midrange:50,dribbling:50}},
    aiBrain:{role:'guard',target:new Vector3(),assignedId:null}});
  ctx.handler=make('handler',1,0,6);
  ctx.offense=[ctx.handler,make('wing',1,-5,5),make('shooter',1,5,5)].slice(0,size);
  ctx.defense=[make('onball',0,0,4.7),make('helper',0,-3,3),make('denier',0,4,4)].slice(0,size);
  ctx.roster=[...ctx.offense,...ctx.defense];ctx.aiRoster=()=>ctx.roster;
  ctx.getPlayerById=id=>ctx.roster.find(p=>p.id===id);ctx.getBallHandler=()=>ctx.handler;
  ctx.getOpponents=p=>ctx.roster.filter(other=>other.teamId!==p.teamId);
  ctx.settings={defensiveIQ:1,courtVision:1,lookAhead:.4,readError:0};
  for(const file of ['ai-reads.js','ai-coordination.js','ai-defense.js'])
    vm.runInContext(fs.readFileSync(path.join(__dirname,'../js',file),'utf8'),ctx);
  ctx.read=()=>ctx.aiGetCoordinatedDefensePlan(0,ctx.handler,ctx.settings).coverage;
  return ctx;
}

test('one manual matchup reserves its target and automatic defenders cover the remaining opponents',()=>{
  const c=fixture();
  c.riseState.team.matchups[c.defense[0].campaignPlayerId]=c.offense[1].campaignPlayerId;
  const coverage=c.read();
  assert.equal(coverage.assignments.onball,'wing');
  assert.equal(new Set(Object.values(coverage.assignments)).size,3);
  assert.deepEqual(new Set(Object.values(coverage.assignments)),new Set(c.offense.map(p=>p.id)));
});

test('stopper role automatically guards the strongest perimeter threat while explicit assignments win',()=>{
  const c=fixture();
  c.defense[1].profile.role='stopper';c.offense[2].profile.ratings.threePoint=95;
  assert.equal(c.read().assignments.helper,'shooter');
  c.teamAITime+=1;c.riseState.team.matchups[c.defense[1].campaignPlayerId]=c.offense[0].campaignPlayerId;
  assert.equal(c.read().assignments.helper,'handler');
  assert.equal(new Set(Object.values(c.read().assignments)).size,3);
});

test('switch philosophy changes real screen coverage and retains the exchanged manual assignments',()=>{
  for(const focus of ['balanced','switch']){
    const c=fixture(2),screener=c.offense[1],partner=c.defense[1];
    c.riseMatch.defense=focus;
    c.riseState.team.matchups[c.defense[0].campaignPlayerId]=c.handler.campaignPlayerId;
    screener.aiScreen=true;screener.group.position.set(0,0,5.35);partner.group.position.set(1.1,0,5.4);
    partner.profile.speed=.9;partner.profile.width=1.23;
    const first=c.read();
    assert.equal(first.primaryId,focus==='switch'?'helper':'onball');
    if(focus==='switch'){
      c.teamAITime+=.3;
      assert.equal(c.read().assignments.onball,screener.id,'the original manual target must not undo a valid screen exchange');
      assert.equal(c.read().assignments.helper,c.handler.id);
    }
  }
});

test('coordinated assignments take precedence over the original manual matchup during a switch',()=>{
  const c=fixture(2),defender=c.defense[0],switched=c.offense[1];
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../js/ai.js'),'utf8'),c);
  c.riseState.team.matchups[defender.campaignPlayerId]=c.handler.campaignPlayerId;
  c.aiDefenseAssignment=()=>switched;
  assert.equal(c.aiAssignedOpponent(defender),switched);
});
