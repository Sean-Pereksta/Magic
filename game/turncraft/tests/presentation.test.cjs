const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const {simulation,GAME_FILE}=require('./helpers/expansion-harness.cjs');

function presentation(){
  const run=simulation();
  run('globalThis.testSceneImages=[];Image=class{constructor(){testSceneImages.push(this)}}');
  return run;
}

test('menu artwork, identity, and SVG insignia follow the selected faction',()=>{
  const run=presentation();
  for(const faction of ['frontier','ascendant','brood']){
    run(`chosenFaction=${JSON.stringify(faction)};renderMenuPresentation()`);
    const result=run.plain(`({theme:overlay.dataset.faction,card:document.getElementById('startCard').dataset.faction,name:document.getElementById('menuFactionName').textContent,motto:document.getElementById('menuFactionMotto').textContent,svg:document.getElementById('menuInsignia').innerHTML,art:document.getElementById('menuScene').src,request:testSceneImages.at(-1).src})`);
    assert.equal(result.theme,faction);assert.equal(result.card,faction);
    assert.ok(result.name.length>5);assert.ok(result.motto.length>10);
    assert.match(result.svg,/<svg.*viewBox="0 0 64 64"/);assert.match(result.svg,/aria-hidden="true"/);
    assert.equal(result.art,`turncraft/assets/factions/${faction}.webp`);
    assert.equal(result.request,`https://pub-cb55798f1eeb4f80a85463da3612ed61.r2.dev/effects/ui/${faction}-command.webp`);
  }
  const requests=run('testSceneImages.length');
  run('renderMenuPresentation();renderMenuPresentation()');
  assert.equal(run('testSceneImages.length'),requests,'background UI refreshes do not request artwork again');
});

test('rapid faction switches cannot let old artwork replace the current faction',()=>{
  const run=presentation();
  run("chosenFaction='frontier';renderMenuPresentation();globalThis.obsoleteSceneError=document.getElementById('menuScene').onerror;chosenFaction='brood';renderMenuPresentation();testSceneImages[0].onload();obsoleteSceneError()");
  assert.equal(run("document.getElementById('menuScene').src"),'turncraft/assets/factions/brood.webp');
  run('testSceneImages[1].onload()');
  assert.match(run("document.getElementById('menuScene').src"),/effects\/ui\/brood-command\.webp$/);
  assert.equal(run("document.getElementById('menuScene').hidden"),false);
  run("chosenFaction='ascendant';renderMenuPresentation();testSceneImages[2].onerror();testSceneImages[1].onload()");
  assert.equal(run("document.getElementById('menuScene').src"),'turncraft/assets/factions/ascendant.webp');
  assert.equal(run("document.getElementById('menuScene').dataset.fallback"),'true');
});

test('scene failures preserve the shipped illustration and never loop failing requests',()=>{
  const run=presentation();
  run("chosenFaction='frontier';renderMenuPresentation();testSceneImages[0].onerror()");
  assert.equal(run("document.getElementById('menuScene').src"),'turncraft/assets/factions/frontier.webp');
  assert.equal(run("document.getElementById('menuScene').hidden"),false);
  run("testSceneImages[0].onload();document.getElementById('menuScene').onerror()");
  assert.equal(run("document.getElementById('menuScene').src"),'turncraft/assets/factions/frontier.webp');
  run("document.getElementById('menuScene').onerror()");
  assert.equal(run("document.getElementById('menuScene').hidden"),true,'unavailable fallback leaves the themed gradient behind');
  assert.equal(run('testSceneImages.length'),1);
});

test('result presentation uses the local faction and distinguishes victory, defeat, draw, and early elimination',()=>{
  const run=presentation();
  for(const faction of ['frontier','ascendant','brood']){
    for(const outcome of ['victory','defeat','draw']){
      run(`renderMatchPresentation({winnerIdx:1,localFactionId:${JSON.stringify(faction)},outcome:${JSON.stringify(outcome)}})`);
      assert.equal(run('endOverlay.dataset.faction'),faction);assert.equal(run('endOverlay.dataset.outcome'),outcome);
      assert.equal(run("document.getElementById('matchFactionName').textContent"),run(`FACTIONS[${JSON.stringify(faction)}].name`));
      assert.match(run("document.getElementById('matchScene').src"),new RegExp(`factions/${faction}\\.webp$`));
    }
  }
  run("renderMatchPresentation({localFactionId:'frontier',outcome:'defeat',playerEliminated:true})");
  assert.equal(run("document.getElementById('matchEyebrow').textContent"),'COMMAND BASE LOST');
  run("renderMatchPresentation({localFactionId:'missing',outcome:'victory'})");
  assert.equal(run('endOverlay.dataset.faction'),'frontier');
  assert.equal(run("document.getElementById('matchEyebrow').textContent"),'COMMAND SECURED');
});

test('graphical result preserves report/actions and accessible dialog semantics',()=>{
  const html=fs.readFileSync(GAME_FILE,'utf8');
  assert.match(html,/<div id="endOverlay"[^>]*role="dialog"[^>]*aria-modal="true"[^>]*aria-labelledby="endTitle"[^>]*aria-describedby="endText"/);
  assert.match(html,/<div id="endCard"[^>]*tabindex="-1"/);
  for(const id of ['endTitle','endText','postMatchReport','restartBtn','spectateBtn','endExitBtn','startCard','startBtn','factionGrid','mapSelectBtn','selectedMapPreview','mapPopout','difficultyChoices','objectiveSelect'])assert.equal(html.match(new RegExp(`id="${id}"`,'g'))?.length,1,id);
  assert.doesNotMatch(html,/src="turncraft\/assets\/ui\//,'new graphic binaries are externally hosted');
});
