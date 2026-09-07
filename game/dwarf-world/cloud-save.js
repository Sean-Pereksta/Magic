/* Firebase-backed encrypted cloud saves for Dwarf World.
   Reuses the same Firebase project, anonymous auth flow, and Firestore `lobbies`
   surface as Arcane Wilds and Space Tyrants. The game's existing DWGZ2 local
   serializer remains authoritative; this layer encrypts and syncs that payload. */
(() => {
  "use strict";

  const shell=window.DwarfWorldShell;
  if(!shell)throw new Error("Dwarf World cloud save loaded before the game shell.");

  const FIREBASE_CONFIG={
    apiKey:"AIzaSyB7twY7z31ucB6pGA8JC_HrVMZhA8lNaJA",
    authDomain:"bible-game-246c0.firebaseapp.com",
    projectId:"bible-game-246c0",
    storageBucket:"bible-game-246c0.appspot.com",
    messagingSenderId:"959619818996",
    appId:"1:959619818996:web:5a9fbf492e23c765e445a1"
  };
  const APP_NAME="dwarf-world-cloud";
  const COLLECTION="lobbies";
  const GAME_TYPE="dwarf-world-cloud-save";
  const FORMAT=1;
  const KDF_ITERATIONS=210000;
  const CHUNK_BYTES=640*1024;
  const encoder=new TextEncoder();
  const decoder=new TextDecoder();

  let firebaseCache=null;
  let boundName=localStorage.getItem(shell.BINDING_KEY)||"";
  let secret="";
  let busy=false;
  let lastSavedAt=0;
  let lastError="";

  const cleanName=value=>String(value||"").trim().replace(/\s+/g," ").slice(0,48);
  const normalizeName=value=>cleanName(value).toLowerCase();
  const hex=bytes=>Array.from(bytes,b=>b.toString(16).padStart(2,"0")).join("");

  async function documentId(name){
    const digest=await crypto.subtle.digest("SHA-256",encoder.encode(`dwarf-world:${normalizeName(name)}`));
    return `dwarf-world-save--${hex(new Uint8Array(digest))}`;
  }
  function chunkId(base,index){return `${base}--chunk-${String(index).padStart(4,"0")}`}
  function randomBytes(length){const out=new Uint8Array(length);crypto.getRandomValues(out);return out}
  async function keyFor(password,salt,iterations=KDF_ITERATIONS){
    const base=await crypto.subtle.importKey("raw",encoder.encode(password),"PBKDF2",false,["deriveKey"]);
    return crypto.subtle.deriveKey({name:"PBKDF2",salt,iterations,hash:"SHA-256"},base,{name:"AES-GCM",length:256},false,["encrypt","decrypt"]);
  }
  async function pack(raw){
    const plain=encoder.encode(raw);
    if(typeof CompressionStream!=="function")return{bytes:plain,compression:"none"};
    try{
      const stream=new Blob([plain]).stream().pipeThrough(new CompressionStream("gzip"));
      const zipped=new Uint8Array(await new Response(stream).arrayBuffer());
      return zipped.length+32<plain.length?{bytes:zipped,compression:"gzip"}:{bytes:plain,compression:"none"};
    }catch{return{bytes:plain,compression:"none"}}
  }
  async function unpack(bytes,compression){
    if(compression!=="gzip")return decoder.decode(bytes);
    if(typeof DecompressionStream!=="function")throw new Error("This browser cannot decompress this cloud save.");
    const stream=new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"));
    return decoder.decode(await new Response(stream).arrayBuffer());
  }
  async function encryptRaw(raw,password){
    const packed=await pack(raw),salt=randomBytes(16),iv=randomBytes(12),key=await keyFor(password,salt);
    const payload=new Uint8Array(await crypto.subtle.encrypt({name:"AES-GCM",iv},key,packed.bytes));
    return{salt,iv,payload,compression:packed.compression,iterations:KDF_ITERATIONS};
  }
  function bytes(value){
    if(value instanceof Uint8Array)return value;
    if(value?.toUint8Array)return value.toUint8Array();
    if(Array.isArray(value))return new Uint8Array(value);
    throw new Error("Cloud save data is malformed.");
  }
  async function decryptRaw(record,payload,password){
    const salt=bytes(record.salt),iv=bytes(record.iv),iterations=Number(record.iterations)||KDF_ITERATIONS;
    const key=await keyFor(password,salt,iterations);
    const plain=new Uint8Array(await crypto.subtle.decrypt({name:"AES-GCM",iv},key,payload));
    return unpack(plain,record.compression||"none");
  }
  function validateCredentials(name,password){
    const clean=cleanName(name);
    if(clean.length<2)throw new Error("Use at least 2 characters for the save name.");
    if(String(password||"").length<4)throw new Error("Use at least 4 characters for the save password.");
    return clean;
  }
  function validateLocalRaw(raw){
    if(typeof raw!=="string"||raw.length<16)throw new Error("The Dwarf World save is empty or damaged.");
    if(!raw.startsWith("DWGZ2:")&&!raw.trim().startsWith("{")){
      throw new Error("The Dwarf World save format is not recognized.");
    }
    return raw;
  }
  async function firebase(){
    if(firebaseCache)return firebaseCache;
    firebaseCache=(async()=>{
      const [appMod,fireMod,authMod]=await Promise.all([
        import("https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js"),
        import("https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js"),
        import("https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js")
      ]);
      let app=appMod.getApps().find(candidate=>candidate.name===APP_NAME);
      if(!app)app=appMod.initializeApp(FIREBASE_CONFIG,APP_NAME);
      const auth=authMod.getAuth(app);
      if(!auth.currentUser)await authMod.signInAnonymously(auth);
      return{
        db:fireMod.getFirestore(app),
        doc:fireMod.doc,
        getDoc:fireMod.getDoc,
        setDoc:fireMod.setDoc,
        deleteDoc:fireMod.deleteDoc,
        serverTimestamp:fireMod.serverTimestamp,
        Bytes:fireMod.Bytes
      };
    })().catch(err=>{firebaseCache=null;throw err});
    return firebaseCache;
  }
  async function readEncryptedPayload(fb,id,record){
    const count=Math.max(0,Number(record.chunkCount)||0);
    if(!count)return bytes(record.payload);
    if(count>128)throw new Error("Cloud save has an invalid chunk count.");
    const snaps=await Promise.all(Array.from({length:count},(_,index)=>fb.getDoc(fb.doc(fb.db,COLLECTION,chunkId(id,index)))));
    const parts=snaps.map((snap,index)=>{
      if(!snap.exists())throw new Error(`Cloud save chunk ${index+1} is missing.`);
      const data=snap.data();
      if(data.gameType!==GAME_TYPE||data.parentSaveId!==id||Number(data.chunkIndex)!==index)throw new Error("Cloud save chunks do not match the save manifest.");
      return bytes(data.payload);
    });
    const total=parts.reduce((sum,part)=>sum+part.length,0),out=new Uint8Array(total);
    let offset=0;
    for(const part of parts){out.set(part,offset);offset+=part.length}
    return out;
  }
  function bind(name,password){
    boundName=cleanName(name);
    secret=String(password||"");
    localStorage.setItem(shell.BINDING_KEY,boundName);
    lastError="";
    render();
  }
  function forgetBinding(){
    boundName="";secret="";lastSavedAt=0;lastError="";
    localStorage.removeItem(shell.BINDING_KEY);
    shell.$("cloudPassword").value="";
    render();
    closeManager();
    shell.toast("Firebase binding cleared. Local saves are unchanged.");
  }

  async function saveNamed(name,password,{skipFlush=false}={}){
    const clean=validateCredentials(name,password);
    let raw=skipFlush?shell.readLocalSave():await shell.flushLocalSave();
    if(!raw&&skipFlush)raw=await shell.flushLocalSave();
    raw=validateLocalRaw(raw);

    const fb=await firebase(),id=await documentId(clean),ref=fb.doc(fb.db,COLLECTION,id),existing=await fb.getDoc(ref);
    let prior=null;
    if(existing.exists()){
      prior=existing.data();
      if(prior.gameType!==GAME_TYPE)throw new Error("That save name is already reserved.");
      try{
        const priorPayload=await readEncryptedPayload(fb,id,prior);
        const priorRaw=await decryptRaw(prior,priorPayload,password);
        validateLocalRaw(priorRaw);
      }catch(err){
        if(/missing|malformed|invalid chunk|damaged/i.test(String(err?.message||"")))throw err;
        throw new Error("That save name already exists, but the password does not match.");
      }
    }

    const encrypted=await encryptRaw(raw,password),now=Date.now();
    const chunks=[];
    for(let offset=0;offset<encrypted.payload.length;offset+=CHUNK_BYTES)chunks.push(encrypted.payload.slice(offset,offset+CHUNK_BYTES));
    const useChunks=chunks.length>1;

    if(useChunks){
      await Promise.all(chunks.map((part,index)=>fb.setDoc(fb.doc(fb.db,COLLECTION,chunkId(id,index)),{
        kind:"dwarfWorldCloudSaveChunk",
        gameType:GAME_TYPE,
        status:"cloud-save-chunk",
        parentSaveId:id,
        saveName:clean,
        chunkIndex:index,
        chunkCount:chunks.length,
        payload:fb.Bytes.fromUint8Array(part),
        updatedAtMs:now
      })));
    }

    const manifest={
      kind:"dwarfWorldCloudSave",
      gameType:GAME_TYPE,
      status:"active",
      name:`Dwarf World Save: ${clean}`,
      saveName:clean,
      host:"dwarf-world",
      players:[],
      format:FORMAT,
      localFormat:raw.startsWith("DWGZ2:")?"DWGZ2":"legacy-json",
      cipher:"AES-GCM-256",
      kdf:"PBKDF2-SHA256",
      iterations:encrypted.iterations,
      salt:fb.Bytes.fromUint8Array(encrypted.salt),
      iv:fb.Bytes.fromUint8Array(encrypted.iv),
      compression:encrypted.compression,
      chunkCount:useChunks?chunks.length:0,
      encryptedBytes:encrypted.payload.length,
      localBytes:new Blob([raw]).size,
      createdAtMs:Number(prior?.createdAtMs)||now,
      updatedAtMs:now,
      updatedAt:fb.serverTimestamp()
    };
    if(!useChunks)manifest.payload=fb.Bytes.fromUint8Array(encrypted.payload);
    await fb.setDoc(ref,manifest);

    const oldCount=Math.max(0,Number(prior?.chunkCount)||0);
    if(oldCount>(useChunks?chunks.length:0)){
      const start=useChunks?chunks.length:0;
      Promise.allSettled(Array.from({length:oldCount-start},(_,n)=>fb.deleteDoc(fb.doc(fb.db,COLLECTION,chunkId(id,start+n)))));
    }

    bind(clean,password);
    lastSavedAt=now;
    render();
    return clean;
  }

  async function loadNamed(name,password){
    const clean=validateCredentials(name,password),fb=await firebase(),id=await documentId(clean),ref=fb.doc(fb.db,COLLECTION,id),snap=await fb.getDoc(ref);
    if(!snap.exists())throw new Error("No Dwarf World Firebase save exists with that name.");
    const record=snap.data();
    if(record.gameType!==GAME_TYPE)throw new Error("That name does not belong to a Dwarf World cloud save.");
    let raw;
    try{
      const payload=await readEncryptedPayload(fb,id,record);
      raw=await decryptRaw(record,payload,password);
      validateLocalRaw(raw);
    }catch(err){
      console.warn("[Dwarf World cloud decrypt]",err);
      throw new Error("The password is incorrect, or the cloud save is damaged.");
    }
    shell.writeLocalSave(raw);
    bind(clean,password);
    lastSavedAt=Number(record.updatedAtMs)||Date.now();
    render();
    return clean;
  }

  function status(){
    if(lastError)return{title:"Firebase save needs attention",detail:lastError,on:false};
    if(boundName&&secret)return{
      title:`Firebase: ${boundName}`,
      detail:lastSavedAt?`Last save ${new Date(lastSavedAt).toLocaleTimeString()}`:"Ready to sync when you save.",
      on:true
    };
    if(boundName)return{title:`Cloud slot: ${boundName}`,detail:"Enter the password once this session to resume cloud syncing.",on:false};
    return{title:"Firebase save not linked",detail:"Local saves still work. Click Save to assign a cloud slot.",on:false};
  }
  function render(){
    const state=status();
    shell.$("cloudDot").classList.toggle("on",state.on);
    shell.$("cloudTitle").textContent=state.title;
    shell.$("cloudDetail").textContent=state.detail;
    shell.$("cloudSaveNow").textContent=boundName&&secret?"☁ Save":"☁ Link";
    shell.$("cloudForget").style.display=boundName?"":"none";
  }
  function setManagerMessage(message,bad=false){
    const el=shell.$("cloudMessage");el.textContent=message||"";el.classList.toggle("bad",!!bad);
  }
  function openManager(){
    shell.$("cloudName").value=boundName||"";
    shell.$("cloudPassword").value="";
    setManagerMessage(boundName?(secret?`Linked to “${boundName}” for this session.`:`Enter the password for “${boundName}” to resume syncing.`):"");
    shell.$("cloudModal").classList.add("open");
    setTimeout(()=>((shell.$("cloudName").value? shell.$("cloudPassword"):shell.$("cloudName")).focus()),0);
  }
  function closeManager(){if(!busy)shell.$("cloudModal").classList.remove("open")}
  async function assignFromManager(){
    if(busy)return;
    const name=shell.$("cloudName").value,password=shell.$("cloudPassword").value||secret,button=shell.$("cloudAssignSave");
    busy=true;button.disabled=true;button.textContent="Encrypting & Saving…";setManagerMessage("Preparing the current world and contacting Firebase…");
    try{
      const clean=await saveNamed(name,password);
      setManagerMessage(`Saved “${clean}” to Firebase.`);
      closeManager();
      shell.toast(`Firebase save uploaded: ${clean}`);
    }catch(err){
      console.warn("[Dwarf World cloud save]",err);
      lastError=err.message||"Firebase save failed.";
      setManagerMessage(lastError,true);
      render();
    }finally{busy=false;button.disabled=false;button.textContent="Assign & Save Now"}
  }
  async function saveBound({quiet=false,skipFlush=false}={}){
    if(busy||!boundName||!secret)return false;
    busy=true;lastError="";render();
    try{
      await saveNamed(boundName,secret,{skipFlush});
      if(!quiet)shell.toast(`Saved to Firebase: ${boundName}`);
      return true;
    }catch(err){
      console.warn("[Dwarf World cloud save]",err);
      lastError=err.message||"Firebase save failed.";
      render();
      if(!quiet)shell.toast("Firebase save failed — the local save remains safe.");
      return false;
    }finally{busy=false;render()}
  }
  function officialSave(){
    if(!shell.isRunning())return shell.toast("Start or load a world before saving.");
    if(!boundName||!secret)return openManager();
    saveBound({quiet:false});
  }
  function canAutoSave(){return !!(boundName&&secret&&!busy&&shell.isRunning())}

  window.DwarfWorldCloud={
    loadNamed,saveNamed,saveBound,officialSave,canAutoSave,
    openManager,closeManager,assignFromManager,forgetBinding,render,
    get boundName(){return boundName}
  };
  render();
})();
