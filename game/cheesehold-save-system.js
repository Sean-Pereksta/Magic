// Cheesehold save/load checkpoint system
// Saves only at level boundaries so restored runs always restart the next level cleanly at Wave 1.
const CHEESEHOLD_SAVE_KEY='cheesehold.save.v1';
const CHEESEHOLD_SAVE_FORMAT='cheesehold-between-levels';
const CHEESEHOLD_SAVE_VERSION=1;
let cheeseholdBoundaryOverlay=null;
let cheeseholdFileInput=null;
let cheeseholdContinueButton=null;
let cheeseholdSaveStatus=null;

function cheeseholdRankMap(raw,defs){
 const out={},allowed=new Map(defs.map(d=>[d.id,d.max||1]));
 if(!raw||typeof raw!=='object'||Array.isArray(raw))return out;
 for(const [id,value] of Object.entries(raw)){
   if(!allowed.has(id))continue;
   const rank=Math.max(0,Math.min(allowed.get(id),Math.floor(Number(value)||0)));
   if(rank>0)out[id]=rank;
 }
 return out;
}
function cheeseholdFinite(value,fallback=0,min=0,max=1e9){
 const n=Number(value);return Number.isFinite(n)?Math.max(min,Math.min(max,n)):fallback;
}
function cheeseholdSanitizeSave(raw){
 if(!raw||typeof raw!=='object'||Array.isArray(raw))throw new Error('Save file is not a Cheesehold save.');
 if(raw.format!==CHEESEHOLD_SAVE_FORMAT)throw new Error('Unsupported Cheesehold save format.');
 if(Number(raw.version)!==CHEESEHOLD_SAVE_VERSION)throw new Error('Unsupported Cheesehold save version.');
 const round=Math.max(1,Math.min(100000,Math.floor(Number(raw.round)||1)));
 const xpLevel=Math.max(1,Math.min(100000,Math.floor(Number(raw.xpLevel)||1)));
 const xpNext=Math.round(10+(xpLevel-1)*7.5);
 return {
   format:CHEESEHOLD_SAVE_FORMAT,
   version:CHEESEHOLD_SAVE_VERSION,
   checkpoint:'between-levels',
   round,
   kills:Math.floor(cheeseholdFinite(raw.kills,0,0,1e9)),
   cheese:cheeseholdFinite(raw.cheese,20,0,1e9),
   xp:cheeseholdFinite(raw.xp,0,0,Math.max(0,xpNext-.001)),
   xpLevel,
   xpNext,
   ranks:cheeseholdRankMap(raw.ranks,XP_UPGRADES),
   victoryRanks:cheeseholdRankMap(raw.victoryRanks,VICTORY_UPGRADES),
   artifactRanks:cheeseholdRankMap(raw.artifactRanks,ARTIFACTS),
   activeBuild:BUILD_ORDER.includes(raw.activeBuild)?raw.activeBuild:'wall',
   savedAt:typeof raw.savedAt==='string'?raw.savedAt:new Date().toISOString()
 };
}
function cheeseholdRebuildMods(save){
 const mods=freshMods();
 const applyRanks=(defs,ranks)=>{
   for(const def of defs){
     const count=ranks[def.id]||0;
     for(let i=0;i<count;i++)def.apply(mods);
   }
 };
 applyRanks(XP_UPGRADES,save.ranks);
 applyRanks(VICTORY_UPGRADES,save.victoryRanks);
 applyRanks(ARTIFACTS,save.artifactRanks);
 return mods;
}
function cheeseholdCheckpoint(){
 return cheeseholdSanitizeSave({
   format:CHEESEHOLD_SAVE_FORMAT,
   version:CHEESEHOLD_SAVE_VERSION,
   checkpoint:'between-levels',
   round:game.round,
   kills:game.kills,
   cheese:game.cheese,
   xp:game.xp,
   xpLevel:game.xpLevel,
   ranks:game.ranks,
   victoryRanks:game.victoryRanks,
   artifactRanks:game.artifactRanks,
   activeBuild,
   savedAt:new Date().toISOString()
 });
}
function cheeseholdReadLocalSave(){
 try{
   const raw=localStorage.getItem(CHEESEHOLD_SAVE_KEY);
   if(!raw)return null;
   return cheeseholdSanitizeSave(JSON.parse(raw));
 }catch(err){
   console.warn('[Cheesehold] local save ignored:',err);
   return null;
 }
}
function cheeseholdWriteLocalSave(save=cheeseholdCheckpoint()){
 const clean=cheeseholdSanitizeSave(save);
 try{
   localStorage.setItem(CHEESEHOLD_SAVE_KEY,JSON.stringify(clean));
   cheeseholdRefreshMenuSaveUi();
   return true;
 }catch(err){
   console.warn('[Cheesehold] local save failed:',err);
   return false;
 }
}
function cheeseholdExportSave(save=cheeseholdCheckpoint()){
 const clean=cheeseholdSanitizeSave(save);
 const blob=new Blob([JSON.stringify(clean,null,2)],{type:'application/json'}),url=URL.createObjectURL(blob),a=document.createElement('a');
 a.href=url;a.download='cheesehold-level-'+clean.round+'-save.json';document.body.appendChild(a);a.click();a.remove();
 setTimeout(()=>URL.revokeObjectURL(url),1000);
}
function cheeseholdRestoreSave(raw){
 const save=cheeseholdSanitizeSave(raw);
 cheeseholdHideBoundary();
 ui.upgrade.classList.remove('open');
 clearEntities();
 const mods=cheeseholdRebuildMods(save);
 game={running:true,paused:false,time:0,round:save.round,kills:save.kills,cheese:save.cheese,xp:save.xp,xpLevel:save.xpLevel,xpNext:save.xpNext,nextCheese:.7,nextMinion:2.0,catFrenzyUntil:0,mods,ranks:{...save.ranks},victoryRanks:{...save.victoryRanks},artifactRanks:{...save.artifactRanks},pendingRound:false,mutator:ROUND_MUTATORS[(save.round-1)%ROUND_MUTATORS.length],boss:null,wave:null};
 activeBuild=save.activeBuild;
 ui.menu.classList.add('hidden');
 beginRound(false);
 updateSelected();updateUI();
 toast('Loaded save · Level '+save.round);
}
async function cheeseholdLoadFile(file){
 if(!file)return;
 try{
   const raw=JSON.parse(await file.text()),save=cheeseholdSanitizeSave(raw);
   cheeseholdWriteLocalSave(save);
   cheeseholdRestoreSave(save);
 }catch(err){
   console.error('[Cheesehold] save import failed',err);
   alert('Could not load that Cheesehold save file. '+String(err?.message||err));
 }
}
function cheeseholdRefreshMenuSaveUi(){
 if(!cheeseholdContinueButton||!cheeseholdSaveStatus)return;
 const save=cheeseholdReadLocalSave();
 cheeseholdContinueButton.hidden=!save;
 cheeseholdContinueButton.textContent=save?'Continue Local Save · Level '+save.round:'Continue Local Save';
 cheeseholdSaveStatus.textContent=save?'Local checkpoint ready: Level '+save.round+' · '+save.kills+' kills':'No local checkpoint yet. Saves are created between levels.';
 if(game.running===false&&ui.menu&&!ui.menu.classList.contains('hidden'))ui.start.textContent=save?'New Run':'Start New Run';
}
function cheeseholdInstallMenu(){
 const card=ui.menu?.querySelector('.card');
 if(!card||card.querySelector('.cheeseholdSaveActions'))return;
 const style=document.createElement('style');
 style.textContent=`
 .cheeseholdSaveActions{display:flex;flex-wrap:wrap;justify-content:center;gap:9px;margin:0 auto 12px}.cheeseholdSaveActions button{font-size:13px!important;padding:11px 15px!important}.cheeseholdSaveActions .continueSave{background:#ffd45a!important;color:#191407!important}.cheeseholdSaveActions .secondarySave{background:rgba(255,255,255,.09)!important;color:#f3f6fa!important;border:1px solid rgba(255,255,255,.16)!important}.cheeseholdSaveStatus{font-size:9px;color:#8f9aae;margin:-4px auto 11px;line-height:1.4}.cheeseholdBoundary{position:absolute;z-index:75;inset:0;display:none;align-items:center;justify-content:center;padding:18px;background:rgba(3,5,9,.82);backdrop-filter:blur(11px)}.cheeseholdBoundary.open{display:flex}.cheeseholdBoundaryCard{width:min(620px,94vw);padding:25px;border-radius:23px;border:1px solid rgba(255,255,255,.13);background:linear-gradient(145deg,rgba(18,24,35,.99),rgba(8,11,17,.99));box-shadow:0 34px 110px rgba(0,0,0,.62);text-align:center}.cheeseholdBoundaryCard h2{font-size:clamp(30px,6vw,50px);margin:5px 0 7px}.cheeseholdBoundaryCard p{color:#aeb7c6;font-size:12px;line-height:1.45;margin:0 auto 16px}.cheeseholdBoundaryActions{display:flex;flex-wrap:wrap;justify-content:center;gap:9px}.cheeseholdBoundaryActions button{border:0;border-radius:13px;padding:12px 16px;font:inherit;font-size:13px;font-weight:1000;cursor:pointer}.cheeseholdBoundaryActions .continueLevel{background:#ffd45a;color:#191407}.cheeseholdBoundaryActions .saveAlt{background:rgba(255,255,255,.09);color:#fff;border:1px solid rgba(255,255,255,.14)}.cheeseholdBoundaryNote{font-size:9px!important;color:#8390a2!important;margin-top:12px!important}`;
 document.head.appendChild(style);
 const actions=document.createElement('div');actions.className='cheeseholdSaveActions';
 cheeseholdContinueButton=document.createElement('button');cheeseholdContinueButton.type='button';cheeseholdContinueButton.className='continueSave';cheeseholdContinueButton.hidden=true;
 cheeseholdContinueButton.addEventListener('click',()=>{const save=cheeseholdReadLocalSave();if(save)cheeseholdRestoreSave(save)});
 const load=document.createElement('button');load.type='button';load.className='secondarySave';load.textContent='Load Save File';load.addEventListener('click',()=>cheeseholdFileInput?.click());
 actions.append(cheeseholdContinueButton,load);
 card.insertBefore(actions,ui.start);
 cheeseholdSaveStatus=document.createElement('div');cheeseholdSaveStatus.className='cheeseholdSaveStatus';ui.start.insertAdjacentElement('afterend',cheeseholdSaveStatus);
 cheeseholdFileInput=document.createElement('input');cheeseholdFileInput.type='file';cheeseholdFileInput.accept='.json,.cheesehold,application/json';cheeseholdFileInput.hidden=true;cheeseholdFileInput.addEventListener('change',async()=>{const file=cheeseholdFileInput.files?.[0];cheeseholdFileInput.value='';await cheeseholdLoadFile(file)});document.body.appendChild(cheeseholdFileInput);
 cheeseholdRefreshMenuSaveUi();
}
function cheeseholdInstallBoundary(){
 if(cheeseholdBoundaryOverlay)return;
 const overlay=document.createElement('div');overlay.className='cheeseholdBoundary';overlay.innerHTML='<div class="cheeseholdBoundaryCard"><div class="menuTag">LEVEL CLEARED · CHECKPOINT</div><h2 id="cheeseholdBoundaryTitle">Level Complete</h2><p id="cheeseholdBoundaryText"></p><div class="cheeseholdBoundaryActions"><button type="button" class="continueLevel" id="cheeseholdBoundaryContinue">Continue</button><button type="button" class="saveAlt" id="cheeseholdBoundaryBrowser">Save to Browser</button><button type="button" class="saveAlt" id="cheeseholdBoundaryExport">Export Save File</button></div><p class="cheeseholdBoundaryNote" id="cheeseholdBoundaryNote">Your browser checkpoint is also updated automatically at every completed level.</p></div>';
 ui.stage.parentElement.appendChild(overlay);cheeseholdBoundaryOverlay=overlay;
 overlay.querySelector('#cheeseholdBoundaryContinue').addEventListener('click',()=>{cheeseholdHideBoundary();game.paused=false;beginRound(false)});
 overlay.querySelector('#cheeseholdBoundaryBrowser').addEventListener('click',()=>{const ok=cheeseholdWriteLocalSave();overlay.querySelector('#cheeseholdBoundaryNote').textContent=ok?'Browser save updated. You can safely close the game here.':'Browser storage is unavailable; use Export Save File instead.'});
 overlay.querySelector('#cheeseholdBoundaryExport').addEventListener('click',()=>{cheeseholdExportSave();overlay.querySelector('#cheeseholdBoundaryNote').textContent='Save file downloaded. Load it from the opening menu later.'});
}
function cheeseholdShowBoundary(save){
 cheeseholdInstallBoundary();
 const level=CHEESEHOLD_LEVELS[(save.round-1)%CHEESEHOLD_LEVELS.length];
 cheeseholdBoundaryOverlay.querySelector('#cheeseholdBoundaryTitle').textContent='Level '+(save.round-1)+' Cleared';
 cheeseholdBoundaryOverlay.querySelector('#cheeseholdBoundaryText').textContent='Level '+save.round+' — '+level.name+' is ready. Your run checkpoint includes cheese, XP, upgrades, victory evolutions, artifacts, and total kills.';
 cheeseholdBoundaryOverlay.querySelector('#cheeseholdBoundaryContinue').textContent='Continue to Level '+save.round;
 cheeseholdBoundaryOverlay.querySelector('#cheeseholdBoundaryNote').textContent='Autosaved locally. Export a physical save file if you want a portable backup.';
 cheeseholdBoundaryOverlay.classList.add('open');
}
function cheeseholdHideBoundary(){cheeseholdBoundaryOverlay?.classList.remove('open');}

advanceRound=function(){
 game.round++;
 game.cheese=Math.max(10,Math.floor(game.cheese*.48)+8);
 game.pendingRound=false;
 game.paused=true;
 const save=cheeseholdCheckpoint(),stored=cheeseholdWriteLocalSave(save);
 cheeseholdShowBoundary(save);
 if(!stored)cheeseholdBoundaryOverlay.querySelector('#cheeseholdBoundaryNote').textContent='Browser storage is unavailable. Export a save file before continuing if you want a checkpoint.';
};

cheeseholdInstallMenu();
