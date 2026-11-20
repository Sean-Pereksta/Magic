const fs=require('node:fs');
const path=require('node:path');
const {execFileSync}=require('node:child_process');
const {getMakeNsisPath,getRceditBundle}=require('app-builder-lib/out/toolsets/windows');
const {usesLocalHost,hasLocalHostPack}=require('../desktop/local-host.cjs');
const {supportsServerManager,hasServerManagerPack}=require('../desktop/server-manager.cjs');
const root=path.join(__dirname,'..');
const output=path.resolve(process.env.HUB_DIST||path.join(root,'dist'));
const stage=path.join(output,'app'),app=path.join(stage,'resources/app');
const content=path.join(root,'content'),catalog=require('../content/catalog.json');
const meta=require('../package.json');
const cp=(src,dest)=>fs.cpSync(src,dest,{recursive:true});
function nsis(s){return s.replaceAll('$','$$').replaceAll('"','$\\"').replace(/[\r\n]/g,' ');}
const sign=process.env.CATNMICE_SIGN_THUMBPRINT;
function signFile(file){
  if(!sign)return;
  execFileSync(process.env.SIGNTOOL_PATH||'signtool.exe',['sign','/sha1',sign,'/fd','SHA256','/td','SHA256','/tr','http://timestamp.digicert.com',file],{stdio:'inherit'});
  execFileSync(process.env.SIGNTOOL_PATH||'signtool.exe',['verify','/pa',file],{stdio:'inherit'});
}
async function main(){
  if(process.platform!=='win32'||process.arch!=='x64')throw Error('Use a Windows x64 build worker.');
  if(fs.existsSync(stage))throw Error('Use a fresh HUB_DIST directory for each build.');
  fs.mkdirSync(output,{recursive:true});
  const electron=path.dirname(require('electron'));cp(electron,stage);
  const exe=path.join(stage,'Catnmice Game Hub.exe');fs.renameSync(path.join(stage,'electron.exe'),exe);
  fs.rmSync(path.join(stage,'resources/default_app.asar'),{force:true});
  fs.mkdirSync(app,{recursive:true});
  for(const dir of ['ui','desktop'])cp(path.join(root,dir),path.join(app,dir));
  for(const file of ['services/read-model.mjs','services/web/activity.mjs','build/icon.ico','content/catalog.json','package.json']){
    const dest=path.join(app,file);fs.mkdirSync(path.dirname(dest),{recursive:true});fs.copyFileSync(path.join(root,file),dest);
  }
  cp(path.join(content,'art'),path.join(app,'content/art'));
  const release=process.env.HUB_RELEASE_CONFIG;
  if(release)fs.copyFileSync(release,path.join(app,'release.json'));
  const version=meta.version.split('-')[0],filever=version+'.0';
  const icon=path.join(root,'build/icon.ico'),edit=await getRceditBundle('1.0.0');
  execFileSync(edit.x64,[exe,'--set-icon',icon,'--set-version-string','ProductName','Catnmice Game Hub','--set-version-string','FileDescription','Catnmice Game Hub','--set-version-string','CompanyName','Catnmice','--set-file-version',filever,'--set-product-version',filever]);
  signFile(exe);
  const sections=[];
  for(const game of catalog.games.filter(g=>g.offline)){
    const src=path.join(content,'packs',game.id);
    if(!fs.existsSync(path.join(src,game.offline.entry.split(/[?#]/)[0])))throw Error('Missing '+game.id+' pack entry');
    const localHost=usesLocalHost(game.id);
    if(localHost && !hasLocalHostPack(game.id,game.offline,src))throw Error('Incomplete '+game.id+' Windows hosting pack');
    if(supportsServerManager(game.id) && !hasServerManagerPack(game.id,game.offline,src))throw Error('Incomplete '+game.id+' Server Manager pack');
    if(game.id==='lastsignal' && (game.offline.type!=='native-windows'||
        !['Hosting/cloudflared.exe','Assets/models.bin','Mobile/index.html'].every(file=>fs.existsSync(path.join(src,file)))))throw Error('Incomplete Last Signal Windows hosting pack');
    sections.push('Section "'+nsis(game.title)+'"');
    if(localHost || game.id==='lastsignal')sections.push('  SectionIn RO');
    sections.push('  ${If} $OnePack == ""','  ${OrIf} $OnePack == "'+game.id+'"','    SetOutPath "$INSTDIR\\resources\\app\\content\\packs\\'+game.id+'"','    ClearErrors','    File /r "'+nsis(src)+'\\*.*"','    IfErrors 0 +2','      Abort "Could not install the game files."','  ${EndIf}','SectionEnd');
  }
  const nsi=path.join(output,'packs.nsh');fs.writeFileSync(nsi,'\uFEFF'+sections.join('\r\n'));
  const compiler=await getMakeNsisPath();
  execFileSync(compiler.path,['/V2','/DVERSION='+version,'/DFILEVERSION='+filever,'/DOUTDIR='+output,'/DAPPDIR='+stage,'/DICON='+icon,'/DWIZARD='+path.join(root,'build/wizard.bmp'),'/DPACKSECTIONS='+nsi,path.join(root,'build/installer.nsi')],{env:{...process.env,...compiler.env},stdio:'inherit'});
  signFile(path.join(output,'Catnmice-Game-Hub-Setup.exe'));
  console.log('Built installer with '+catalog.games.filter(g=>g.offline).length+' selectable offline packs.');
}
main().catch(e=>{console.error(e);process.exitCode=1;});
