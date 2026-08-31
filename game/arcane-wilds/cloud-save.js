/* Firebase-backed official cloud saves for Arcane Wilds.
   Mirrors Space Tyrants' proven Firebase project/auth/rules surface. The base
   journey and Arcane Wilds expansion save are bundled and encrypted client-side,
   so the password is never written to Firebase. */

const AW_CLOUD_FIREBASE_CONFIG={
  apiKey:"AIzaSyB7twY7z31ucB6pGA8JC_HrVMZhA8lNaJA",
  authDomain:"bible-game-246c0.firebaseapp.com",
  projectId:"bible-game-246c0",
  storageBucket:"bible-game-246c0.appspot.com",
  messagingSenderId:"959619818996",
  appId:"1:959619818996:web:5a9fbf492e23c765e445a1"
};
const AW_CLOUD_APP_NAME="arcane-wilds-cloud";
const AW_CLOUD_COLLECTION="lobbies";
const AW_CLOUD_GAME_TYPE="arcane-wilds-cloud-save";
const AW_CLOUD_BINDING_KEY="arcaneWilds.cloudSave.name.v1";
const AW_CLOUD_FORMAT=1;
const AW_CLOUD_KDF_ITERATIONS=210000;
const AW_CLOUD_ENCODER=new TextEncoder();
const AW_CLOUD_DECODER=new TextDecoder();
let awCloudFirebaseCache=null;
let awCloudSecret="";
let awCloudBoundName=localStorage.getItem(AW_CLOUD_BINDING_KEY)||"";
let awCloudBusy=false;
let awCloudMode="save";
let awCloudLastSavedAt=0;

function awCloudCleanName(value){return String(value||"").trim().replace(/\s+/g," ").slice(0,48)}
function awCloudNormalizeName(value){return awCloudCleanName(value).toLowerCase()}
function awCloudHex(bytes){return Array.from(bytes,b=>b.toString(16).padStart(2,"0")).join("")}
async function awCloudDocumentId(name){
  const digest=await crypto.subtle.digest("SHA-256",AW_CLOUD_ENCODER.encode(`arcane-wilds:${awCloudNormalizeName(name)}`));
  return `arcane-wilds-save--${awCloudHex(new Uint8Array(digest))}`;
}
function awCloudRandomBytes(length){const bytes=new Uint8Array(length);crypto.getRandomValues(bytes);return bytes}
async function awCloudKey(password,salt,iterations=AW_CLOUD_KDF_ITERATIONS){
  const base=await crypto.subtle.importKey("raw",AW_CLOUD_ENCODER.encode(password),"PBKDF2",false,["deriveKey"]);
  return crypto.subtle.deriveKey({name:"PBKDF2",salt,iterations,hash:"SHA-256"},base,{name:"AES-GCM",length:256},false,["encrypt","decrypt"]);
}
async function awCloudPack(raw){
  const plain=AW_CLOUD_ENCODER.encode(raw);
  if(typeof CompressionStream!=="function")return{bytes:plain,compression:"none"};
  try{
    const stream=new Blob([plain]).stream().pipeThrough(new CompressionStream("gzip"));
    const zipped=new Uint8Array(await new Response(stream).arrayBuffer());
    return zipped.length+32<plain.length?{bytes:zipped,compression:"gzip"}:{bytes:plain,compression:"none"};
  }catch{return{bytes:plain,compression:"none"}}
}
async function awCloudUnpack(bytes,compression){
  if(compression!=="gzip")return AW_CLOUD_DECODER.decode(bytes);
  if(typeof DecompressionStream!=="function")throw new Error("This browser cannot decompress this cloud save.");
  const stream=new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"));
  return AW_CLOUD_DECODER.decode(await new Response(stream).arrayBuffer());
}
async function awCloudEncrypt(raw,password){
  const packed=await awCloudPack(raw),salt=awCloudRandomBytes(16),iv=awCloudRandomBytes(12),key=await awCloudKey(password,salt);
  const encrypted=new Uint8Array(await crypto.subtle.encrypt({name:"AES-GCM",iv},key,packed.bytes));
  return{salt,iv,payload:encrypted,compression:packed.compression,iterations:AW_CLOUD_KDF_ITERATIONS};
}
function awCloudBytes(value){
  if(value instanceof Uint8Array)return value;
  if(value?.toUint8Array)return value.toUint8Array();
  if(Array.isArray(value))return new Uint8Array(value);
  throw new Error("Cloud save data is malformed.");
}
async function awCloudDecrypt(record,password){
  const salt=awCloudBytes(record.salt),iv=awCloudBytes(record.iv),payload=awCloudBytes(record.payload),iterations=Number(record.iterations)||AW_CLOUD_KDF_ITERATIONS;
  const key=await awCloudKey(password,salt,iterations);
  const plain=new Uint8Array(await crypto.subtle.decrypt({name:"AES-GCM",iv},key,payload));
  return awCloudUnpack(plain,record.compression||"none");
}
async function awCloudFirebase(){
  if(awCloudFirebaseCache)return awCloudFirebaseCache;
  awCloudFirebaseCache=(async()=>{
    const [appMod,fireMod,authMod]=await Promise.all([
      import("https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js"),
      import("https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js"),
      import("https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js")
    ]);
    let app=appMod.getApps().find(candidate=>candidate.name===AW_CLOUD_APP_NAME);
    if(!app)app=appMod.initializeApp(AW_CLOUD_FIREBASE_CONFIG,AW_CLOUD_APP_NAME);
    const auth=authMod.getAuth(app);
    if(!auth.currentUser)await authMod.signInAnonymously(auth);
    return{db:fireMod.getFirestore(app),doc:fireMod.doc,getDoc:fireMod.getDoc,setDoc:fireMod.setDoc,serverTimestamp:fireMod.serverTimestamp,Bytes:fireMod.Bytes};
  })().catch(err=>{awCloudFirebaseCache=null;throw err});
  return awCloudFirebaseCache;
}
function awCloudValidateCredentials(name,password){
  const clean=awCloudCleanName(name);
  if(clean.length<2)throw new Error("Use at least 2 characters for the save name.");
  if(String(password||"").length<4)throw new Error("Use at least 4 characters for the save password.");
  return clean;
}
function awCloudBind(name,password){
  awCloudBoundName=awCloudCleanName(name);awCloudSecret=String(password||"");
  localStorage.setItem(AW_CLOUD_BINDING_KEY,awCloudBoundName);awCloudRenderStatus();
}
function awCloudForgetBinding(){
  awCloudBoundName="";awCloudSecret="";localStorage.removeItem(AW_CLOUD_BINDING_KEY);awCloudRenderStatus();toastMsg("Cloud save binding cleared.")
}
function awCloudCurrentBundle(){
  if(!game?.player)throw new Error("Start or reopen a journey before saving it.");
  saveGame();
  if(typeof saveExpansion==="function")saveExpansion();
  const base=localStorage.getItem(SAVE_KEY);
  if(!base)throw new Error("The local Arcane Wilds journey could not be prepared for upload.");
  const expansion=typeof EXPANSION_SAVE_KEY!=="undefined"?localStorage.getItem(EXPANSION_SAVE_KEY):null;
  return JSON.stringify({format:AW_CLOUD_FORMAT,base,expansion});
}
function awCloudInspectBundle(raw){
  let bundle,base;
  try{bundle=JSON.parse(raw)}catch{throw new Error("The cloud save could not be decoded.")}
  if(!bundle||typeof bundle.base!=="string")throw new Error("The cloud save is missing the Arcane Wilds journey.");
  try{base=JSON.parse(bundle.base)}catch{throw new Error("The saved journey data is damaged.")}
  if(!base||!base.player||!Number.isFinite(Number(base.seed)))throw new Error("The cloud save is missing required journey data.");
  return{bundle,base};
}
async function awCloudSaveNamed(name,password){
  const clean=awCloudValidateCredentials(name,password),raw=awCloudCurrentBundle(),checked=awCloudInspectBundle(raw),firebase=await awCloudFirebase(),id=await awCloudDocumentId(clean),ref=firebase.doc(firebase.db,AW_CLOUD_COLLECTION,id),existing=await firebase.getDoc(ref);
  if(existing.exists()){
    const prior=existing.data();
    if(prior.gameType!==AW_CLOUD_GAME_TYPE)throw new Error("That save name is already reserved.");
    try{await awCloudDecrypt(prior,password)}catch{throw new Error("That save name already exists, but the password does not match.")}
  }
  const encrypted=await awCloudEncrypt(raw,password),now=Date.now(),prior=existing.exists()?existing.data():null;
  await firebase.setDoc(ref,{
    kind:"arcaneWildsCloudSave",
    gameType:AW_CLOUD_GAME_TYPE,
    status:"active",
    name:`Arcane Wilds Save: ${clean}`,
    saveName:clean,
    host:"arcane-wilds",
    players:[],
    format:AW_CLOUD_FORMAT,
    cipher:"AES-GCM-256",
    kdf:"PBKDF2-SHA256",
    iterations:encrypted.iterations,
    salt:firebase.Bytes.fromUint8Array(encrypted.salt),
    iv:firebase.Bytes.fromUint8Array(encrypted.iv),
    payload:firebase.Bytes.fromUint8Array(encrypted.payload),
    compression:encrypted.compression,
    level:Number(checked.base.level)||1,
    gold:Number(checked.base.gold)||0,
    roomX:Number(checked.base.room?.x)||0,
    roomY:Number(checked.base.room?.y)||0,
    createdAtMs:Number(prior?.createdAtMs)||now,
    updatedAtMs:now,
    updatedAt:firebase.serverTimestamp()
  });
  awCloudBind(clean,password);awCloudLastSavedAt=now;awCloudRenderStatus();return clean;
}
async function awCloudLoadNamed(name,password){
  const clean=awCloudValidateCredentials(name,password),firebase=await awCloudFirebase(),id=await awCloudDocumentId(clean),ref=firebase.doc(firebase.db,AW_CLOUD_COLLECTION,id),snap=await firebase.getDoc(ref);
  if(!snap.exists())throw new Error("No Arcane Wilds cloud save exists with that name.");
  const record=snap.data();
  if(record.gameType!==AW_CLOUD_GAME_TYPE)throw new Error("That name does not belong to an Arcane Wilds cloud save.");
  let raw;
  try{raw=await awCloudDecrypt(record,password)}catch{throw new Error("The password is incorrect, or the cloud save is damaged.")}
  const {bundle}=awCloudInspectBundle(raw);
  localStorage.setItem(SAVE_KEY,bundle.base);
  if(typeof EXPANSION_SAVE_KEY!=="undefined"){
    if(typeof bundle.expansion==="string")localStorage.setItem(EXPANSION_SAVE_KEY,bundle.expansion);
    else localStorage.removeItem(EXPANSION_SAVE_KEY);
  }
  if(!loadGame())throw new Error("The restored journey could not be opened.");
  if(typeof loadExpansion==="function")loadExpansion();
  awCloudBind(clean,password);awCloudLastSavedAt=Number(record.updatedAtMs)||Date.now();
  document.querySelectorAll(".overlay").forEach(o=>o.classList.add("hidden"));
  awCloudCloseModal(true);beginWorld();awCloudRenderStatus();return clean;
}
function awCloudStatusText(){
  if(!awCloudBoundName)return"No Firebase journey is assigned yet.";
  if(awCloudSecret)return`Bound to “${awCloudBoundName}”. Official saves overwrite this encrypted Firebase slot.`;
  return`Bound name: “${awCloudBoundName}”. Enter its password once this session to resume official cloud saving.`;
}
function awCloudRenderStatus(){
  const status=$("awCloudStatus"),save=$("awCloudSaveNow"),forget=$("awCloudForget"),stamp=$("awCloudStamp");
  if(status)status.textContent=awCloudStatusText();
  if(save)save.textContent=awCloudBoundName&&awCloudSecret?"Save to Firebase Now":"Assign Name & Password";
  if(forget)forget.classList.toggle("hidden",!awCloudBoundName);
  if(stamp)stamp.textContent=awCloudLastSavedAt?`Last Firebase save: ${new Date(awCloudLastSavedAt).toLocaleString()}`:"The journey is encrypted before it leaves this device.";
}
function awCloudSetMessage(message,bad=false){const el=$("awCloudMessage");if(!el)return;el.textContent=message||"";el.classList.toggle("bad",bad)}
function awCloudOpenModal(mode){
  awCloudMode=mode;const modal=$("awCloudModal"),name=$("awCloudName"),password=$("awCloudPassword"),title=$("awCloudModalTitle"),copy=$("awCloudModalCopy"),action=$("awCloudModalAction");
  if(!modal)return;name.value=awCloudBoundName||"";password.value="";title.textContent=mode==="load"?"Reopen a Cloud Journey":"Assign the Official Save";copy.textContent=mode==="load"?"Enter the save name and password from any device to restore the complete journey, including expansion materials, quests and trinkets.":"Choose a save name and password. Future official saves in this session overwrite the same encrypted Firebase slot.";action.textContent=mode==="load"?"Reopen Journey":"Assign & Save";awCloudSetMessage("");modal.classList.remove("hidden");modalPause=true;setTimeout(()=>{(name.value?password:name).focus()},0)
}
function awCloudCloseModal(force=false){if(awCloudBusy&&!force)return;const modal=$("awCloudModal");if(modal)modal.classList.add("hidden");if(!paused)modalPause=false}
async function awCloudSubmitModal(){
  if(awCloudBusy)return;const name=$("awCloudName").value,password=$("awCloudPassword").value,action=$("awCloudModalAction"),old=action.textContent;awCloudBusy=true;action.disabled=true;action.textContent=awCloudMode==="load"?"Reopening…":"Saving…";awCloudSetMessage(awCloudMode==="load"?"Contacting Firebase…":"Encrypting and uploading…");
  try{
    if(awCloudMode==="load"){
      if(running&&game?.player&&!confirm("Reopen this cloud journey and replace the journey currently in memory?"))return;
      const clean=await awCloudLoadNamed(name,password);toastMsg(`Reopened ${clean} from Firebase.`);
    }else{
      const clean=await awCloudSaveNamed(name,password);awCloudCloseModal(true);toastMsg(`Official save uploaded: ${clean}`);
    }
  }catch(err){console.warn("[Arcane Wilds cloud save]",err);awCloudSetMessage(err?.message||"Firebase save failed.",true)}
  finally{awCloudBusy=false;action.disabled=false;action.textContent=old;awCloudRenderStatus()}
}
async function awOfficialSave(){
  if(!game?.player)return toastMsg("Start a journey before saving.");
  if(!awCloudBoundName||!awCloudSecret)return awCloudOpenModal("save");
  if(awCloudBusy)return;awCloudBusy=true;
  try{await awCloudSaveNamed(awCloudBoundName,awCloudSecret);toastMsg(`Official save uploaded: ${awCloudBoundName}`)}catch(err){console.warn("[Arcane Wilds cloud save]",err);toastMsg("Firebase save failed — local saves remain safe.");awCloudOpenModal("save");awCloudSetMessage(err?.message||"Firebase save failed.",true)}finally{awCloudBusy=false;awCloudRenderStatus()}
}
function awInstallCloudSaveUI(){
  if($("awCloudModal"))return;
  const style=document.createElement("style");style.id="aw-cloud-save-style";style.textContent=`
    .aw-cloud-box{margin:14px auto 0;padding:13px;border:1px solid rgba(110,190,255,.22);border-radius:14px;background:rgba(19,35,52,.72);text-align:left}.aw-cloud-box h3{margin:0 0 7px;font-size:15px}.aw-cloud-status{font-size:11px;line-height:1.5;color:#d8e7f5}.aw-cloud-stamp{font-size:10px;color:#8298ad;margin-top:6px}.aw-cloud-actions{display:flex;flex-wrap:wrap;gap:7px;margin-top:10px}.aw-cloud-actions .btn{padding:8px 11px;font-size:11px}.aw-cloud-round{font-size:15px}.aw-cloud-modal-panel{max-width:560px}.aw-cloud-fields{display:grid;gap:10px;margin:16px 0}.aw-cloud-fields label{display:grid;gap:5px;text-align:left;font-size:10px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:#9eb0c4}.aw-cloud-fields input{width:100%;box-sizing:border-box;border:1px solid rgba(130,173,210,.3);border-radius:10px;background:#0d1722;color:#eef6ff;padding:11px 12px;outline:none}.aw-cloud-fields input:focus{border-color:#79d9ff;box-shadow:0 0 0 3px rgba(121,217,255,.1)}.aw-cloud-message{min-height:18px;font-size:11px;color:#7fe5a5}.aw-cloud-message.bad{color:#ff8995}.aw-cloud-note{font-size:10px;line-height:1.5;color:#8fa5ba;margin-top:9px}
  `;document.head.appendChild(style);
  const startActions=$("startOverlay")?.querySelector(".menu-actions");
  if(startActions){const reopen=document.createElement("button");reopen.className="btn secondary";reopen.id="awCloudReopenStart";reopen.textContent="☁ Reopen Cloud Journey";reopen.onclick=()=>awCloudOpenModal("load");startActions.insertBefore(reopen,$("howBtn"));}
  const hudButtons=$("buttons");
  if(hudButtons){const save=document.createElement("button");save.className="round-btn aw-cloud-round";save.id="awCloudHudSave";save.title="Official Firebase Save";save.textContent="☁";save.onclick=awOfficialSave;hudButtons.insertBefore(save,$("pauseBtn"));}
  const pauseActions=$("pauseOverlay")?.querySelector(".menu-actions");
  if(pauseActions){const save=document.createElement("button");save.className="btn secondary";save.textContent="Official Firebase Save";save.onclick=awOfficialSave;const reopen=document.createElement("button");reopen.className="btn secondary";reopen.textContent="Reopen Cloud Journey";reopen.onclick=()=>awCloudOpenModal("load");pauseActions.insertBefore(save,$("restartBtn"));pauseActions.insertBefore(reopen,$("restartBtn"));const box=document.createElement("div");box.className="aw-cloud-box";box.innerHTML=`<h3>☁ Official Cloud Journey</h3><div class="aw-cloud-status" id="awCloudStatus"></div><div class="aw-cloud-stamp" id="awCloudStamp"></div><div class="aw-cloud-actions"><button class="btn secondary" id="awCloudSaveNow"></button><button class="btn secondary" id="awCloudForget">Forget Binding</button></div>`;pauseActions.parentElement.appendChild(box);$("awCloudSaveNow").onclick=awOfficialSave;$("awCloudForget").onclick=awCloudForgetBinding;}
  const modal=document.createElement("div");modal.className="overlay hidden";modal.id="awCloudModal";modal.innerHTML=`<div class="panel aw-cloud-modal-panel"><div class="overlay-head"><div><h2 id="awCloudModalTitle">Official Cloud Save</h2><p id="awCloudModalCopy"></p></div><button class="btn secondary" id="awCloudModalClose">Close</button></div><div class="aw-cloud-fields"><label>Save name<input id="awCloudName" maxlength="48" autocomplete="username" placeholder="My Arcane Journey"></label><label>Password<input id="awCloudPassword" type="password" autocomplete="current-password" placeholder="Save password"></label></div><div class="aw-cloud-message" id="awCloudMessage"></div><div class="aw-cloud-note">Your password is not uploaded. The complete journey is encrypted in this browser before Firebase receives it.</div><div class="menu-actions" style="justify-content:flex-end;margin-top:15px"><button class="btn" id="awCloudModalAction">Assign & Save</button></div></div>`;document.body.appendChild(modal);$("awCloudModalClose").onclick=()=>awCloudCloseModal();$("awCloudModalAction").onclick=awCloudSubmitModal;modal.addEventListener("click",e=>{if(e.target===modal)awCloudCloseModal()});$("awCloudPassword").addEventListener("keydown",e=>{if(e.key==="Enter")awCloudSubmitModal()});awCloudRenderStatus();
}

awInstallCloudSaveUI();
