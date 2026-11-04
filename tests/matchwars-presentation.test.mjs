import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { CLASSES, getTroop } from '../game/matchwars/classes.mjs';
import { ELEMENTS } from '../game/matchwars/board.mjs';
import { getStage } from '../game/matchwars/campaign.mjs';
import { SPECIALIZATIONS, normalizeMastery, masteryProgress } from '../game/matchwars/mastery.mjs';
import { createDaily, recordDailyResult } from '../game/matchwars/daily.mjs';

const source=readFileSync(new URL('../game/matchwars/app.mjs',import.meta.url),'utf8');
function host(name,next,context){
  const start=source.indexOf(`function ${name}(`),nextIndex=source.indexOf(`function ${next}(`,start);
  const end=source.lastIndexOf('\n',nextIndex)+1;
  assert.ok(start>=0&&end>start);
  const sandbox=vm.createContext(context);
  vm.runInContext(`${source.slice(start,end)}\nglobalThis.run=${name};`,sandbox);
  return sandbox;
}

test('the final two-encounter chapter shows its real denominator and boss ward',()=>{
  let html;
  // Reuse campaign exports without constructing the renderer or battle timer.
  return import('../game/matchwars/campaign.mjs').then(({TOTAL_STAGES,CHAPTERS,STAGES,getChapter})=>{
    const h=host('showCampaign','showDaily',{
      progress:{stage:134,unlocked:134,completed:[133]},TOTAL_STAGES,CHAPTERS,STAGES,getChapter,getStage,
      clamp:(v,a,b)=>Math.max(a,Math.min(b,v)),roman:String,difficultyName:()=> 'Cataclysm',escape:String,
      difficultyForStage:()=>({totalWaves:8}),openModal:(title,body)=>{html=body;},
    });
    h.run();assert.match(html,/1 \/ 2 CLEARED/);
    assert.deepEqual([...html.matchAll(/<button class="stage-card[^>]*data-stage="(\d+)"/g)].map(m=>+m[1]),[133,134]);
    assert.match(html,/The Eternal Unmaker/);assert.match(html,/Boss ward/);
  });
});

test('specializations become selectable at mastery three and show the actual saved choice',()=>{
  const progress={mastery:normalizeMastery({})};
  const h=host('masteryPanel','showArmory',{progress,masteryProgress,SPECIALIZATIONS,escape:String});
  const def=CLASSES.necromancer;
  let html=h.run(def,true);assert.equal([...html.matchAll(/ disabled/g)].length,2);assert.match(html,/MASTERY 1/);
  progress.mastery.necromancer={xp:180,specialization:SPECIALIZATIONS.necromancer[1].id};
  html=h.run(def,true);assert.doesNotMatch(html,/ disabled/);assert.match(html,/MASTERY 3/);
  assert.match(html,/aria-pressed="true"/);assert.match(html,new RegExp(SPECIALIZATIONS.necromancer[1].name));
  assert.equal([...h.run(def,false).matchAll(/ disabled/g)].length,2,'campaign-locked classes remain locked');
});

test('a completed daily result is routed once without granting campaign rewards or mastery',()=>{
  let results=0;
  const progress={stage:84,unlocked:85,completed:[84],gold:100,mastery:{}};
  const h=host('finishBattle','cast',{progress,battle:{status:'won'},dailyConfig:{date:'2026-11-04'},
    resolved:false,busy:false,finishDaily(){results++;},});
  const before=JSON.stringify(progress);h.run();h.run();
  assert.equal(results,1);assert.equal(JSON.stringify(progress),before);
});

test('daily result presentation records personal scores and grants only the once-per-day reward',()=>{
  const config=createDaily('2026-11-04','2026-11-04');let html;
  const progress={daily:{records:{}},gold:50,stage:84,completed:[84],mastery:{}};
  const h=host('finishDaily','shareDaily',{progress,dailyConfig:config,
    battle:{status:'won',time:75,kills:30,playerHp:120,playerMax:180},matches:40,bestChain:4,eventCount:1,
    recordDailyResult:(...args)=>recordDailyResult(...args,'2026-11-04'),
    persist(){},sound(){},openModal:(title,body)=>{html=body;},});
  h.run();assert.equal(progress.gold,250);assert.match(html,/SCORE/);assert.match(html,/MEDAL/);
  h.run();assert.equal(progress.gold,250);assert.equal(progress.stage,84);assert.deepEqual(progress.completed,[84]);
  assert.deepEqual(progress.mastery,{});assert.equal(progress.daily.records[config.date].attempts,2);
});

test('daily briefing includes shared-army medal targets and labels personal bests accurately',()=>{
  let html;const config=createDaily('2026-11-04','2026-11-04');
  const h=host('showDaily','finishDaily',{
    createDaily:()=>config,todayKey:()=>config.date,progress:{daily:{records:{}}},getStage,ELEMENTS,getTroop,
    classPortrait:def=>`<canvas data-class="${def.id}"></canvas>`,escape:String,
    openModal:(title,body)=>{html=body;},toast(){},Date,
  });h.run(config.date);
  assert.match(html,/YOUR BEST/);assert.match(html,/GOLD/);assert.match(html,/Personal scores are saved on this device/);
  assert.equal([...html.matchAll(/data-class=/g)].length,6);assert.match(html,/Daily runs do not change your campaign or class mastery/);
});

test('rare board events grant their distinct benefits and one curse reinforcement',()=>{
  const notes=[],battle={gold:0,reinforce:(key,options)=>notes.push([key,options.lane])};
  const h=host('applyBoardEvents','renderBoard',{battle,charge:0,overdrive:0,eventCount:0,
    dailyConfig:null,banner(){},burst(){},});
  h.run([{type:'supply',action:'collected',gold:20},{type:'curse',action:'purified'},
    {type:'frost',action:'thawed'},{type:'curse',action:'reinforcement',key:'void'}],2);
  assert.equal(battle.gold,20);assert.equal(h.charge,16);assert.equal(h.overdrive,5);assert.equal(h.eventCount,3);
  assert.deepEqual(notes,[['void',2]]);
});

test('opening classes during a daily shows the fixed army without campaign editing controls',()=>{
  let shown=0;
  const h=host('showArmory','equipClass',{dailyConfig:{stage:24},showDailyArmy(){shown++;}});
  h.run();assert.equal(shown,1);
});
