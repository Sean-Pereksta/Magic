'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const S=require('./player-scale.js'),THREE=require('./vendor/three.min.js');
const source=fs.readFileSync(__dirname+'/game.js','utf8');
function extract(name){const start=source.indexOf('function '+name+'(');assert.ok(start>=0);let i=source.indexOf('{',start),depth=1;for(i++;depth;i++){if(source[i]==='{')depth++;if(source[i]==='}')depth--;}return source.slice(start,i);}
test('one immutable scale enlarges physical athletes by forty percent and raises the QB eye equally',()=>{
  assert.equal(S.actorScale,1.4);assert.equal(S.referenceHeightM,1.88);assert.equal(S.referenceModelHeight,2.27);
  assert.ok(Math.abs(S.worldUnitsPerMetre/(2.27/1.88)-1.4)<1e-12);
  assert.ok(Math.abs(S.eyeHeight-3.15)<1e-12);assert.ok(Math.abs(S.eyeHeight/2.25-S.actorScale)<1e-12);
  assert.ok(Object.isFrozen(S));assert.ok(Object.values(S).every(v=>typeof v==='number'&&Number.isFinite(v)&&v>0));
});
test('classic browser scripts and CommonJS receive the same scale contract',()=>{
  const context={};vm.runInNewContext(fs.readFileSync(require.resolve('./player-scale.js'),'utf8'),context);
  assert.deepEqual(JSON.parse(JSON.stringify(context.QBPlayerScale)),{...S});assert.ok(Object.isFrozen(context.QBPlayerScale));
});
test('the actual fallback constructor enlarges offense and defense once while preserving their physique ratios',()=>{
  const look={scaleX:.93,scaleY:1.08,scaleZ:1.04,jerseyPrimary:'#ffffff',jerseyAccent:'#1188ff',skinColor:'#80563d',helmetColor:'#2299ff',cleatColor:'#111111'};
  const context={THREE,PLAYER_SCALE:S.actorScale,R:null,normalizeAppearance:a=>a,playerGeometry:(_key,create)=>create()};
  for(const name of ['offenseMat','offenseAccent','defenseMat','skinMat','blackMat','whiteMat','shadowMat'])context[name]=new THREE.MeshStandardMaterial();
  vm.createContext(context);vm.runInContext(extract('makePlayer'),context);
  for(const offense of [true,false]){
    const actor=context.makePlayer(offense,look);actor.updateMatrixWorld(true);
    assert.ok(Math.abs(actor.scale.x-look.scaleX*1.4)<1e-12);assert.ok(Math.abs(actor.scale.y-look.scaleY*1.4)<1e-12);assert.ok(Math.abs(actor.scale.z-look.scaleZ*1.4)<1e-12);
    const enlarged=new THREE.Box3().setFromObject(actor,true).getSize(new THREE.Vector3());actor.scale.divideScalar(1.4);actor.updateMatrixWorld(true);
    const baseline=new THREE.Box3().setFromObject(actor,true).getSize(new THREE.Vector3());for(const key of ['x','y','z'])assert.ok(Math.abs(enlarged[key]/baseline[key]-1.4)<1e-6);
    assert.equal(actor.userData.hands.length,2);const hand=actor.userData.hands[0];assert.equal(hand.userData.contactRadius,.105);
    actor.traverse(o=>{o.geometry?.dispose();if(o.material?.dispose)o.material.dispose();});
  }
});
