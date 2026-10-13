const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'../game/neon-arena/neon-arena.js'),'utf8');
const body=source.slice(source.indexOf('function importedGunPose('),source.indexOf('function importedGunMatrix('));
test('first-person gun receivers stay in the lower right of portrait, landscape and split viewports',()=>{
  for(const [width,height]of [[390,844],[844,390],[422,390],[390,422],[1280,800],[640,800]]){
    const sandbox={WEAPONS:{pistol:{cool:.1}},reloadVisualState:()=>({active:false}),performance:{now:()=>0},viewRect:{w:width,h:height}};
    vm.createContext(sandbox);vm.runInContext(body,sandbox);
    const pose=sandbox.importedGunPose({weapon:'pistol',vx:0,vz:0,onGround:true,cool:0,reload:0});
    const x=width/2+pose.bx*.72*height/pose.bz,y=height/2-pose.by*.72*height/pose.bz;
    assert.ok(x>width/2&&x<width*.9,`${width}x${height} receiver x=${x}`);
    assert.ok(y>height/2&&y<height,`${width}x${height} receiver y=${y}`);
  }
});
