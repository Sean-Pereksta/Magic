'use strict';
// Optional full authored-scenario audit. This intentionally stays outside the
// default suite: 600 simulated seconds with every nearby body checked is costly.
const assert=require('node:assert/strict');
global.window=global;
for(const name of ['data','tactics','impacts','siege','ramparts','engine'])require('../src/'+name+'.js');
const DT=1/30,SECONDS=600,round=n=>Math.round(n*1000)/1000;
const preset=GameData.EXPANSION_PRESETS.find(p=>p.map==='fourwinds'),e=new BattleEngine(p),companies=e.regiments.filter(r=>r._rampartJob);
const start=performance.now(),everClimbed=new Set();let minimumRatio=Infinity,lastMilestone=0,lastValue=0,reportedStagnation=false;
const counts=()=>companies.map(r=>({type:r.type,soldiers:r.alive,slots:r._rampartSlots.length,cohesion:round(r.cohesion),status:r.status,stages:r.unitIds.reduce((out,id)=>{const u=e.units[id],stage=u._rampartStage+(u._onRampart&&u._rampartPath.length?'-walking':'');out[stage]=(out[stage]||0)+1;return out;},{})}));
function progress(reason){console.log('PROGRESS '+round(e.time)+'s · '+reason+' · '+counts().map(c=>c.type+': '+JSON.stringify(c.stages)).join(' · '));}
function audit(){
  assert.equal(e.units.length,3258,'The audit must retain the entire authored army, including stationary ground defenders');assert.equal(companies.length,2);assert.deepEqual(companies.map(r=>r.type).sort(),['archer','crossbow']);
  for(const r of companies){assert.equal(r.alive,160);assert.equal(r._rampartSlots.length,160,'The authored wall lacks real capacity for its full company');assert(r.unitIds.every(id=>!e.units[id]._onRampart&&!e.units[id]._rampartZ),'Guards spawned on the deck instead of using its stairs');}
  // Keep all real ground defenders and assault formations in their authored
  // places, while removing combat as a confounding cause of a failed ascent.
  e.command(e.regiments.map(r=>r.id),'hold');for(const u of e.units){u._cooldown=1000000;u._meleeCooldown=1000000;}
  const ids=companies.flatMap(r=>r.unitIds);progress('all 320 guards start on courtyard ground');e.start();let complete=false;
  for(let tick=0;tick<SECONDS/DT&&e.phase==='running';tick++){
    e.step(DT);
    for(const r of companies){
      assert(r.cohesion>=70&&!r._routed,'An authored wall company lost cohesion while forming');
      for(const id of r.unitIds){
        const u=e.units[id];assert(u.alive&&[u.x,u.y,u._rampartZ].every(Number.isFinite),'An authored guard died or became nonfinite');assert(e.ramparts.isSupported(u),'An authored guard lost its physical ground, stair, or deck support');if(u._onRampart)everClimbed.add(id);
        const padding=u._radius+e._maxRadius+4,x0=Math.max(0,Math.floor((u.x-padding)/32)),x1=Math.min(e._bodyCols-1,Math.floor((u.x+padding)/32)),y0=Math.max(0,Math.floor((u.y-padding)/32)),y1=Math.min(e._bodyRows-1,Math.floor((u.y+padding)/32));
        for(let team=0;team<2;team++)for(let y=y0;y<=y1;y++)for(let x=x0;x<=x1;x++)for(let other=e._bodyHeads[team][y*e._bodyCols+x];other!==-1;other=e._bodyNext[other]){
          if(other===id)continue;const v=e.units[other];if(!v.alive||v.routed||Math.abs(u._rampartZ-v._rampartZ)>18)continue;
          const ratio=Math.hypot(u.x-v.x,u.y-v.y)/(u._radius+v._radius);minimumRatio=Math.min(minimumRatio,ratio);assert(ratio>=.88,'An authored guard passed through another physical body: '+JSON.stringify({time:e.time,id,other,ratio}));
        }
      }
    }
    const formed=ids.filter(id=>e.units[id]._onRampart&&!e.units[id]._rampartPath.length).length,value=everClimbed.size+formed;
    if(value>lastValue){lastMilestone=e.time;lastValue=value;reportedStagnation=false;}
    else if(e.time-lastMilestone>=40&&!reportedStagnation){progress('no new climber or occupied post for '+round(e.time-lastMilestone)+'s');reportedStagnation=true;}
    if((tick+1)%1500===0)progress('last new climber/post '+round(e.time-lastMilestone)+'s ago');
    complete=formed===320;if(complete)break;
  }
  assert(complete,'The full garrison did not occupy all 320 real wall slots within '+SECONDS+'s; final companies: '+JSON.stringify(counts()));
  return{passed:true,scenario:preset.id,authoredBodies:e.units.length,guards:320,simulatedSeconds:round(e.time),wallMilliseconds:round(performance.now()-start),minimumBodyRatio:round(minimumRatio),companies:counts()};
}
try{console.log(JSON.stringify(audit()));}
catch(error){console.error(error.stack);console.log(JSON.stringify({passed:false,scenario:preset.id,simulatedSeconds:round(e.time),wallMilliseconds:round(performance.now()-start),minimumBodyRatio:Number.isFinite(minimumRatio)?round(minimumRatio):null,error:error.message,companies:counts()}));process.exitCode=1;}
