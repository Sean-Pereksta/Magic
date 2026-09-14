const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');

const dir=__dirname;
const source=fs.readFileSync(path.join(dir,'mobile-split-controls.js'),'utf8');
const html=fs.readFileSync(path.join(dir,'..','receiver-window-qb.html'),'utf8');

test('mobile split controls compile and are loaded after the game runtime',()=>{
  assert.doesNotThrow(()=>new Function(source));
  const gameIndex=html.indexOf('receiver-window-qb/game.js');
  const splitIndex=html.indexOf('receiver-window-qb/mobile-split-controls.js');
  assert.ok(gameIndex>=0,'game runtime script is missing');
  assert.ok(splitIndex>gameIndex,'split controls must load after game.js so capture listeners can gate legacy touch input');
});

test('left and right mobile zones have separate responsibilities',()=>{
  assert.match(source,/e\.clientX<innerWidth\*\.5/,'left/right screen split is missing');
  assert.match(source,/syntheticPointer\('pointermove'/,'left look should continue through the existing camera path');
  assert.match(source,/key\('keydown','Space'\)/,'right hold should start throw charge');
  assert.match(source,/key\('keyup','Space'\)/,'right release should throw');
  assert.match(source,/setLoftDirection\('KeyW'\)/,'upward drag should request loft');
  assert.match(source,/setLoftDirection\('KeyS'\)/,'downward drag should request bullet');
  assert.match(source,/capture:true/,'mobile splitter must intercept trusted touch events before legacy bubble listeners');
});

test('mobile instructions describe the split controls',()=>{
  assert.match(html,/drag the LEFT half to look around/);
  assert.match(html,/Hold the RIGHT half to charge the throw/);
  assert.match(html,/The two sides work independently/);
});
