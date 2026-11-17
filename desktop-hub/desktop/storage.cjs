const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
function writeJson(file,value) {
  fs.mkdirSync(path.dirname(file),{recursive:true});
  const temp = file + '.' + crypto.randomUUID() + '.tmp';
  fs.writeFileSync(temp,JSON.stringify(value),{mode:0o600});
  fs.renameSync(temp,file);
}
function newProfile(name='Guest',guest=true) {
  const id=crypto.randomUUID();
  return {id,name,username:'',guest,partition:'persist:catnmice-'+id,guestToken:crypto.randomBytes(32).toString('base64url'),favorites:[],recent:[],pending:{}};
}
function readState(dir) {
  const file=path.join(dir,'library.json');
  if(fs.existsSync(file)) return JSON.parse(fs.readFileSync(file,'utf8'));
  const guest=newProfile();
  const state={active:guest.id,profiles:[guest],settings:{autoUpdate:true,reducedMotion:false},downloads:[],packs:{},counts:{}};
  writeJson(file,state);
  return state;
}
function inDir(root,relative) {
  if(typeof relative!=='string' || relative.includes('\0') || relative.includes('\\') || relative.split('/').some(s=>s==='..'||s.includes(':'))) throw Error('Invalid pack path.');
  const target=path.resolve(root,'.'+ '/' +relative);
  if(target!==path.resolve(root) && !target.startsWith(path.resolve(root)+path.sep)) throw Error('Invalid pack path.');
  return target;
}
module.exports={writeJson,newProfile,readState,inDir};
