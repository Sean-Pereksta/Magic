const fs = require('node:fs');
const path = require('node:path');
const {writeJson} = require('./storage.cjs');
const {markRejectedUpdate,isRejectedUpdate} = require('./updates.cjs');

const hash = value => typeof value==='string' && /^[a-f0-9]{64}$/.test(value);
function read(file) {
  try {return JSON.parse(fs.readFileSync(file,'utf8')) || {};}
  catch(error) {
    if(error.code==='ENOENT' || error instanceof SyntaxError) return {};
    throw error;
  }
}
function recover(data,state) {
  if(!hash(state.active)) return {active:null,previous:null};
  markRejectedUpdate(data,'hub',state.active);
  const prior=hash(state.previous) && state.previous!==state.active && !isRejectedUpdate(data,{id:'hub',sha256:state.previous});
  const next={active:prior?state.previous:null,previous:null};
  writeJson(path.join(data,'hub-version.json'),next);
  return next;
}
function selectHub(data) {
  const state=read(path.join(data,'hub-version.json'));
  if(!hash(state.active)) return {active:null,previous:null};
  const lock=read(path.join(data,'startup.json'));
  if(lock.active===state.active || isRejectedUpdate(data,{id:'hub',sha256:state.active})) return recover(data,state);
  return state;
}
function boot(app) {
  app.setName('Catnmice Game Hub');
  if(process.env.CATNMICE_TEST_DATA && !app.isPackaged) app.setPath('userData',path.resolve(process.env.CATNMICE_TEST_DATA));
  if(!app.requestSingleInstanceLock()){app.quit();return;}
  const data=app.getPath('userData'),state=selectHub(data);
  const lock=path.join(data,'startup.json'),bundled=path.join(__dirname,'..');
  const root=state.active ? path.join(data,'versions','hub',state.active) : bundled;
  let timer,settled=false;
  function startupFailed() {
    if(!state.active || settled) return false;
    settled=true;clearTimeout(timer);
    recover(data,state);
    app.relaunch();app.exit(1);
    return true;
  }
  function ready() {
    if(settled) return;
    settled=true;clearTimeout(timer);fs.rmSync(lock,{force:true});
  }
  if(state.active) {
    writeJson(lock,{active:state.active});
    timer=setTimeout(startupFailed,90000);
    timer.unref();
  }
  try {
    require(path.join(root,'desktop','main.cjs')).start({root,bundled,data,ready,startupFailed});
  } catch(error) {if(!startupFailed()) throw error;}
}
if(process.type==='browser' && process.versions.electron) boot(require('electron').app);
module.exports={boot,recover,selectHub};
