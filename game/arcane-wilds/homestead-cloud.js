'use strict';
/* Extend the existing encrypted journey, not a second independently spendable wallet.
   This is concurrency protection for the client-trusted single-player save system,
   NOT server-authoritative anti-cheat. See HOMESTEAD.md before changing auth/rules. */
(() => {
  let expected=null,expectedName='',sample=0,sampleAt=0,uncertain=false,busy=false,lastStatus='Local save';
  const token=record=>record?`${Number(record.homeRevision)||0}:${awCloudHex(awCloudBytes(record.iv))}`:null;
  const normalize=awCloudNormalizeName;
  function loaded(name,record){expected=token(record);expectedName=normalize(name);uncertain=false;lastStatus='Firebase loaded';sample=record.updatedAt?.toMillis?.()||record.updatedAtMs||Date.now();sampleAt=performance.now();}
  function now(){return sample?sample+Math.max(0,performance.now()-sampleAt):Date.now();}
  async function trustedTime(firebase,name){
    const id=await awCloudDocumentId(name),ref=firebase.doc(firebase.db,AW_CLOUD_COLLECTION,id,'homeClocks',firebase.auth.currentUser.uid);
    await firebase.setDoc(ref,{at:firebase.serverTimestamp()});
    const snap=await firebase.getDocFromServer(ref),millis=snap.data()?.at?.toMillis?.();
    if(!Number.isFinite(millis))throw new Error('Firebase could not confirm the crop clock. Reconnect and try again.');
    sample=millis;sampleAt=performance.now();return millis;
  }
  function assertOnline(){if(navigator.onLine===false)throw new Error('Offline: cloud home actions need a connection. Existing crops retain their last confirmed watering.');if(uncertain)throw new Error('Reopen your cloud journey to reconcile its latest confirmed save.');}
  async function actionTime(){
    const h=AWHome.state();if(h.mode!=='cloud')return Date.now();assertOnline();
    if(!awCloudBoundName||!awCloudSecret||expectedName!==normalize(awCloudBoundName)||!expected)throw new Error('Reopen the named Firebase journey with its password before changing this cloud home.');
    return trustedTime(await awCloudFirebase(),awCloudBoundName);
  }
  function serialize(result){
    const bundle=JSON.parse(awCloudCurrentBundle()),base=JSON.parse(bundle.base),expansion=bundle.expansion?JSON.parse(bundle.expansion):{};
    if(result){base.homestead=result.home;base.gold=result.wallet.gold;expansion.materials=result.wallet.materials;
      const heal=(result.effects||[]).filter(e=>e.type==='heal').reduce((sum,e)=>sum+e.fraction*game.player.maxHp,0);
      if(heal)base.player.hp=Math.min(game.player.maxHp,game.player.hp+heal);
    }
    bundle.base=JSON.stringify(base);bundle.expansion=JSON.stringify(expansion);return JSON.stringify(bundle);
  }
  function rebaseLocal(home,serverNow){
    const h=AWHomeCore.settle(AWHomeCore.clone(home),Date.now()),offset=serverNow-h.lastEvaluatedAt;
    h.lastEvaluatedAt=serverNow;h.mode='cloud';
    for(const i of h.items)if(Number.isFinite(i.evaluatedAt))i.evaluatedAt+=offset;
    for(const p of h.plots)if(p.plant)for(const key of ['plantedAt','cycleStartedAt','lastWateredAt','wateredUntil','evaluatedAt'])if(Number.isFinite(p.plant[key]))p.plant[key]+=offset;
    return h;
  }
  async function write(name,password,raw){
    assertOnline();if(busy)throw new Error('Another Firebase save is still pending.');busy=true;lastStatus='Pending Firebase confirmation';AWHomeUI.refreshStatus();
    let attempted=false;
    try{
      const clean=awCloudValidateCredentials(name,password),firebase=await awCloudFirebase(),id=await awCloudDocumentId(clean),ref=firebase.doc(firebase.db,AW_CLOUD_COLLECTION,id),existing=await firebase.getDocFromServer(ref),prior=existing.exists()?existing.data():null;
      if(prior){
        if(prior.gameType!==AW_CLOUD_GAME_TYPE)throw new Error('That save name is already reserved.');
        try{await awCloudDecrypt(prior,password);}catch{throw new Error('The cloud save password does not match.');}
        if(expectedName!==normalize(clean)||expected!==token(prior))throw new Error('This cloud journey changed, or has not been reopened in this session. Reopen it before saving; use a new name to keep a separate journey.');
      }
      const checked=awCloudInspectBundle(raw),priorToken=token(prior),encrypted=await awCloudEncrypt(raw,password),revision=(Number(prior?.homeRevision)||0)+1;
      const record={kind:'arcaneWildsCloudSave',gameType:AW_CLOUD_GAME_TYPE,status:'active',name:`Arcane Wilds Save: ${clean}`,saveName:clean,host:'arcane-wilds',players:[],format:AW_CLOUD_FORMAT,cipher:'AES-GCM-256',kdf:'PBKDF2-SHA256',iterations:encrypted.iterations,salt:firebase.Bytes.fromUint8Array(encrypted.salt),iv:firebase.Bytes.fromUint8Array(encrypted.iv),payload:firebase.Bytes.fromUint8Array(encrypted.payload),compression:encrypted.compression,level:Number(checked.base.level)||1,gold:Number(checked.base.gold)||0,roomX:Number(checked.base.room?.x)||0,roomY:Number(checked.base.room?.y)||0,createdAtMs:Number(prior?.createdAtMs)||now(),updatedAtMs:now(),updatedAt:firebase.serverTimestamp(),homeRevision:revision};
      attempted=true;
      await firebase.runTransaction(firebase.db,async tx=>{
        const latest=await tx.get(ref),current=latest.exists()?latest.data():null;
        if(token(current)!==priorToken)throw new Error('Another device saved first. Reopen your cloud journey; no local reward was granted.');
        tx.set(ref,record);
      });
      expected=token(record);expectedName=normalize(clean);awCloudBind(clean,password);awCloudLastSavedAt=now();lastStatus='Saved to Firebase';awCloudRenderStatus();return clean;
    }catch(error){if(attempted)uncertain=true;lastStatus=uncertain?'Reopen cloud to reconcile':'Cloud save not confirmed';throw error;}
    finally{busy=false;AWHomeUI.refreshStatus();}
  }
  async function commit(result){
    if(result.home.mode!=='cloud'){lastStatus='Saved locally · use Official Firebase Save to sync';return;}
    if(awCloudBusy)throw new Error('Wait for the current official save to finish before changing the home.');
    await write(awCloudBoundName,awCloudSecret,serialize(result));
  }
  awCloudSaveNamed=async function(name,password){
    if(AWHome.pending())throw new Error('Finish the current home action before saving.');
    const h=AWHome.state(),firebase=await awCloudFirebase(),serverNow=await trustedTime(firebase,name),home=h.mode==='cloud'?AWHomeCore.clone(h):rebaseLocal(h,serverNow),result={home,wallet:AWHome.wallet(),effects:[]};
    const clean=await write(name,password,serialize(result));game.homestead=home;saveGame();return clean;
  };
  const previousLoad=awCloudLoadNamed;
  awCloudLoadNamed=async function(name,password){
    if(AWHome.pending()||busy)throw new Error('Finish the pending home save before reopening another journey.');
    const clean=await previousLoad(name,password);
    try{const t=await trustedTime(await awCloudFirebase(),clean);if(AWHome.state().mode!=='cloud')game.homestead=rebaseLocal(AWHome.state(),t);}
    catch{AWHome.state().mode='cloud';lastStatus='Cloud reopened · crop time is estimated until reconnection';sample=0;}
    saveGame();
    AWHomeUI.refresh();return clean;
  };
  function newJourney(){expected=null;expectedName='';uncertain=false;sample=0;awCloudSecret='';awCloudBoundName='';localStorage.removeItem(AW_CLOUD_BINDING_KEY);lastStatus='New local journey';}
  function status(){if(busy||AWHome.pending())return 'Pending… do not close this view';if(AWHome.state()?.mode==='cloud'&&navigator.onLine===false)return 'Offline · viewing cached home';return lastStatus;}
  window.AWHomeCloud={loaded,now,actionTime,commit,status,newJourney,token,rebaseLocal};
})();
